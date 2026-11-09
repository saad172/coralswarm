//! Gmail ingest — `users.watch`, `history.list` incremental sync, opt-in
//! per-thread fetch, and deletion propagation (gsuite Train G2 PR 3,
//! `.scratch/gsuite-enterprise/g2-design.md`, PR G2.3). Structural analogue
//! of `google_calendar.rs` — see that module's doc first; this one only
//! documents where Gmail's shape actually differs.
//!
//! # Mailbox archetype, not Calendar's (contract §7)
//! Unlike Calendar's `sync()` (self-owned, non-sensitive, bulk-pulled),
//! Gmail is the Mailbox archetype: `supports_fetch_items()` only — every
//! message enters via explicit per-thread opt-in
//! (`GoogleConnector::fetch_items`, ported here as [`fetch_items`]), never a
//! bulk mailbox pull (Train G2.4's "everything mode" is the founder-gated
//! exception, deliberately NOT built here). Two ingest paths follow from
//! that:
//!
//! 1. **Opt-in** ([`fetch_items`] + [`after_ingest`]): the user opens a
//!    message, we fetch it, expand to the whole thread
//!    (`threads.get(format=full)`, which — unlike Outlook's paginated
//!    conversation expand — returns every message in one response, no
//!    resumable-backfill scenario exists for this path), and register the
//!    thread in `gmail_watched_threads` for live follow.
//! 2. **Live follow** ([`fetch_pending`]/[`commit_pending`], driven by
//!    `routes::google_push`): a Pub/Sub push notification is a TRIGGER only
//!    (the notification's own `historyId` is never trusted as a cursor — the
//!    STORED `google_watch_channels.sync_token` is authoritative); on
//!    receipt we diff `history.list` from the stored cursor and, per
//!    ADR-0008 storage-scoping, discard every `messagesAdded` entry whose
//!    `threadId` is not in `gmail_watched_threads` for this account —
//!    exactly mirroring `microsoft.rs`'s inbox-wide-subscription-but-
//!    storage-scoped-to-opt-in shape.
//!
//! # Per-member token isolation (no fallback, ever)
//! Every function here takes an explicit `owner_user_id: Uuid` (the
//! connecting member) alongside `account_id`, because Gmail's OAuth token —
//! and therefore its `google_watch_channels`/`gmail_watched_threads` rows —
//! is per-member (`connectors::google::Surface::Gmail.token_owner_for`,
//! gsuite Train G2 PR 1), never `TokenOwner::AccountWide`. There is
//! deliberately no code path here that falls back to "the account's Google
//! connection" the way Calendar's account-wide model allows — every ingest/
//! watch/deletion call is anchored on the SPECIFIC member whose mailbox it
//! is, matching `TokenOwner`'s own "no fallback, ever" contract
//! (`connectors/mod.rs`).
//!
//! # `google_watch_channels` column overload (documented, not new schema)
//! Reuses `migrations_control/0180_google_watch_channels.sql` +
//! `0182_google_calendar_sync_state.sql`'s columns, keyed
//! `(account_id, surface='gmail', resource_id=<member's email>)` — ONE row
//! per connected mailbox (unlike Calendar, which has exactly one row per
//! account since `resource_id` is always `'primary'`), plus the
//! `owner_user_id` column `0186_google_gmail_watch_owner_and_threads.sql`
//! adds:
//!
//! * `sync_token` — Gmail's `historyId` (a decimal-string cursor, same shape
//!   Calendar's `syncToken` occupies). Empty means "no watch has ever
//!   succeeded" — [`ensure_watch`] seeds a baseline the FIRST time it
//!   creates a row (from the `users.watch` response's own `historyId`); it
//!   never overwrites an existing cursor on a later renewal, mirroring
//!   Calendar's `ensure_watch`/`upsert_sync_columns` split (channel
//!   bootstrap vs. cursor state are separate concerns, separate writers).
//! * `backfill_page_token` / `backfill_complete` — resume a `history.list`
//!   PAGINATION pass that hit [`SYNC_PASS_BUDGET`] before reaching its
//!   terminal page, NOT a mailbox backfill (Gmail's `history.list` has no
//!   "list everything" mode — it is always incremental from a
//!   `startHistoryId`, so there is no from-scratch listing to resume). The
//!   overload is intentional: same columns, same "resume an in-flight
//!   paginated fetch" meaning, applied to a diff-only API instead of a
//!   full-listing one.
//!
//! # Watch rows are CONTROL-plane, always (gsuite Train G2 PR 7 — org-plane
//! # split settlement)
//! `google_watch_channels` (surface `'gmail'`) rows live on the CONTROL
//! plane ONLY — for BOTH personal and org tenancy — never the content/tenant
//! plane. This is a deliberate, single-plane decision, not an oversight left
//! over from G2 PR 6's org-gate lift (that PR's own residual note flagged
//! exactly this split as unsettled; see `.scratch/gsuite-enterprise/
//! plan.md`'s "Train G2 PR 6" entry). The reasoning:
//!
//! * Push routing (`routes::google_push::run_gmail_sync_for_email` via
//!   [`resolve_watch_owners_for_push`]) resolves the notified mailbox through
//!   `resolve_gmail_watch_owners`, a CONTROL-only `SECURITY DEFINER` function
//!   (migration `0275`, no tenant twin) — a watch row that only ever existed
//!   on a tenant DB would be structurally invisible to push dispatch.
//! * Renewal discovery for the cross-account (Consumer) sweep goes through
//!   `resolve_google_watch_channels_due` (migration `0181`, also control-only,
//!   no tenant twin) — the SAME shape.
//! * Every function in this module that touches `google_watch_channels`
//!   ([`watch_is_live`], [`upsert_watch`], [`ensure_watch`],
//!   [`load_sync_state`], [`upsert_sync_columns`] and its three thin
//!   wrappers) therefore takes an explicit `control_pool: &PgPool` — the
//!   SAME pool `routes::oauth::callback` already resolves via
//!   `state.control_db()` for `post_store`'s watch bootstrap (unchanged by
//!   this PR — it was already correct, being the callback's own `state.db`).
//!   Under Personal tenancy `control_pool` and the content pool are the same
//!   physical database (`tenancy::extractor::Tenant`'s own doc), so this is
//!   byte-identical there; under Org tenancy they genuinely diverge, and
//!   every self-heal write ([`after_ingest`], [`fetch_pending`]) now targets
//!   `control_pool` explicitly rather than whatever content pool the caller
//!   happened to be holding — the same class of fix gsuite Train G2 PR 6
//!   made for `load_creds_owner_aware`/`finish_ingest`'s token-refresh
//!   persist (`oauth_tokens` is control-plane-only for the identical reason).
//!
//! `gmail_watched_threads` is NOT part of this decision — it STAYS
//! content-plane, always (the tenant pool under Org, the consumer pool under
//! Personal). It has no push-routing or cross-account-discovery reader, it
//! is read during ordinary sync (`watched_thread_ids_among`,
//! `watched_thread_graph_ids`) on the SAME pool `ingest_atom`/`delete_source`
//! themselves run against, and `after_ingest`/[`propagate_deletions`] both
//! need it colocated with the atoms/graphs it references. So every function
//! here that touches it ([`watched_thread_ids_among`],
//! [`watched_thread_graph_ids`], [`upsert_watched_thread`],
//! [`propagate_deletions`]) takes an explicit `content_pool: &PgPool`
//! instead — a plane-role name, not a rename of "the pool this module always
//! had", so a future reader cannot mix the two up by accident.
//!
//! `connectors::google_disconnect::purge_google_connection` (the disconnect
//! purge) mirrors the same split: the Gmail-surface `google_watch_channels`
//! delete now runs against a `control_pool`, while `gmail_watched_threads`
//! (and Calendar's own `google_watch_channels` rows, unaffected by this
//! decision — Calendar stays account-wide, Personal-plane-only, so its
//! tenant pool and control pool are the same DB today regardless) keep
//! running against the tenant/content pool. `workers::google_watch_renewal`
//! mirrors it a third time: `channels_due`'s org-tenant branch now discovers
//! Gmail rows from `control_pool` (scoped to that org's `account_id`) rather
//! than the tenant-loop's own `pool`, and `renew_one`/`mark_failed` persist a
//! Gmail row's renewal outcome back to `control_pool` — see that module's own
//! doc for the full mechanics.
//!
//! # Trash and Spam are reversible, graph-confined retractions
//! `history.list` reports a move to Trash/Spam as `labelsAdded` (never as
//! `messagesDeleted`, which Gmail reserves for permanent deletion — see
//! [`HIDING_LABELS`]). [`process_history_page`] folds those label events
//! into the same vocabulary as `messagesAdded`/`messagesDeleted`,
//! [`net_history`] lets a message's last event in the pass win, and every
//! non-explicit ingest path skips a message whose CURRENT labels hide it
//! ([`message_is_hidden`]). A hide ([`propagate_hides`]) retracts only the
//! connector's own copy, inside the ocean(s) it put it in, on both planes —
//! never a copy a human promoted elsewhere — and tombstones exactly what it
//! took out (`gmail_hidden_messages`, migrations control 0277 / tenant
//! 0116). Un-trash / "Not spam" re-ingests ONLY tombstoned messages, back
//! into the same ocean, then clears the tombstone; a restored message no
//! hide tombstoned is gated like a new arrival. A permanent
//! `messagesDeleted` keeps [`propagate_deletions`]'s semantics and clears
//! any tombstone. The request itself is unfiltered (no `historyTypes`), so
//! nothing upstream needs to change.
//!
//! # 404 on a stale `startHistoryId` — Gmail's `410 GONE` analogue
//! Google's Gmail API returns `404 NOT_FOUND` (not `410 GONE`, which
//! Calendar uses) when `startHistoryId` refers to a point further back than
//! Gmail retained history for. [`fetch_pending`] treats this exactly like
//! Calendar's 410: re-seed a fresh baseline (via `users.getProfile`, which
//! reports the mailbox's CURRENT `historyId` without re-registering the
//! watch or disturbing its expiry) and restart, once, guarded by the same
//! `restarted_once` flag Calendar's loop uses.
//!
//! # Deletion destination (decision 9)
//! Unlike Calendar (always the ACCOUNT OWNER's ocean,
//! `google_calendar::account_owner_user`), every create this module ingests
//! lands in the CONNECTING MEMBER's own private ocean (`owner_user_id`,
//! resolved via `routes::slack::resolve_primary_graph(Personal,
//! Attributable(owner_user_id))` — the member IS the mailbox owner for a
//! per-member connector). [`propagate_deletions`] does NOT re-derive that
//! same resolution for deletes: it reads the graph back from
//! `gmail_watched_threads.graph_id`, the row [`after_ingest`] itself wrote
//! at ingest time — see that function's own doc for the full reasoning
//! (a narrower one than "avoids data loss": `services::deletion::
//! delete_source` deletes by `(source_type, source_id)` across the WHOLE
//! account regardless of which of the account's own graphs is passed, so
//! re-deriving via `resolve_primary_graph` would not actually have lost
//! anything — reading the row's own graph avoids a redundant resolution
//! call and an unnecessary dependency, not a correctness bug). Since
//! gsuite Train G2 PR 4 (everything mode), a thread absent from
//! `gmail_watched_threads` is no longer automatically a correct no-op:
//! [`everything_mode_graph_id`] supplies a fallback graph when the owner
//! currently has everything mode enabled, because that path never
//! registers a `gmail_watched_threads` row for what it pulls — see
//! [`propagate_deletions`]'s own doc for the full mechanics.

use std::collections::HashSet;

use chrono::{DateTime, Utc};
use serde::Deserialize;
use sqlx::PgPool;
use uuid::Uuid;

use super::email_body::{gmail_message_to_item, GmailMessage};
use super::google::{google_api_error, google_send};
use super::{ConnectorError, IngestScope, NormalizedItem};
use crate::routes::slack::AccountPlane;

/// `atoms.source_type` for a Gmail message — the chunker already maps
/// `"gmail"` to the email chunker (`packages/core/src/chunker.rs`, gsuite
/// Train G2 PR 2), so no core change is needed.
pub(crate) const SOURCE_TYPE: &str = "gmail";

/// The outbound rate-limit key (`graph_rate_limit::scoped`) for Gmail calls
/// on one mailbox. Gmail's quota is per Google USER, and one Google user can
/// be connected from two CoralSwarm accounts (a personal account and an org
/// member), so the key is the mailbox, not the CoralSwarm account: both
/// connections then draw from one bucket. `email` is the mailbox address
/// (`oauth_tokens.provider_workspace_id` for Gmail); it is normalized and
/// hashed, so no address sits in a key. With no email (a malformed legacy
/// row), the key falls back to the account, as it was before.
pub(crate) fn rate_key(account_id: Uuid, email: &str) -> String {
    use sha2::{Digest, Sha256};
    let email = email.trim().to_lowercase();
    if email.is_empty() {
        return format!("google_gmail:{account_id}");
    }
    let digest = Sha256::digest(email.as_bytes());
    format!("google_gmail:user:{}", hex::encode(&digest[..12]))
}


/// Gmail's binding per-user limit (measured, GCP Cloud Monitoring for the
/// prod project, 2026-10-07): `gmail.googleapis.com/total_query_cost`, 6,000
/// units a minute per Google user. Not the 15,000/min `default` limit the
/// Gmail docs lead with. That limit trips first.
pub(crate) const GMAIL_QUERY_COST_PER_USER_PER_MINUTE: u32 = 6_000;

/// Measured `total_query_cost` of one `messages.get(format=full)`: in one
/// minute, 82 successful gets cost 4,060 units, about 50 each (the
/// `default` metric charged about 5).
pub(crate) const GMAIL_GET_QUERY_COST: u32 = 50;

/// So one Google user can take about 120 gets a minute, all callers together.
pub(crate) const GMAIL_GETS_PER_MINUTE_CEILING: u32 = GMAIL_QUERY_COST_PER_USER_PER_MINUTE / GMAIL_GET_QUERY_COST;

/// Mailbox get slots target three quarters of the ceiling, 90 a minute.
/// Pushes reserve one slot per HTTP attempt. Walks reserve one per logical
/// fetch; their internal retries still use only the shared rate limiter.
/// The remaining quota covers lists, other machines, and measurement error.
pub(crate) const TARGET_GETS_PER_MINUTE: u32 = GMAIL_GETS_PER_MINUTE_CEILING * 3 / 4;

/// Spacing between reserved get slots on one mailbox, about 667 ms.
pub(crate) const GET_INTERVAL: std::time::Duration =
    std::time::Duration::from_millis(60_000_u64.div_ceil(TARGET_GETS_PER_MINUTE as u64));

/// Next free get slot per mailbox ([`rate_key`]), process-wide.
fn mailbox_pacers() -> &'static std::sync::Mutex<std::collections::HashMap<String, tokio::time::Instant>> {
    static PACERS: std::sync::OnceLock<std::sync::Mutex<std::collections::HashMap<String, tokio::time::Instant>>> =
        std::sync::OnceLock::new();
    PACERS.get_or_init(Default::default)
}

/// Wait for this mailbox's next get slot. Slots are `interval` apart per
/// mailbox, so every caller getting messages for one Google user (a
/// personal and an org member everything-mode row, and push passes for
/// either) shares its rate instead of each pacing as if it had the quota to
/// itself. A caller reserves one slot per call, right before its get, so
/// nobody queues ahead: a new caller waits about one slot per caller
/// already pacing. Process-wide only: gets for one mailbox on two machines
/// at once are not coordinated (the claim keeps one walk ROW to one
/// machine; a push can land on any machine). The shared limiter's
/// rate-limit pause covers that overflow.
pub(crate) async fn pace_mailbox(key: &str, interval: std::time::Duration) {
    if interval.is_zero() {
        return;
    }
    let slot = {
        let mut pacers = mailbox_pacers().lock().unwrap_or_else(|e| e.into_inner());
        let now = tokio::time::Instant::now();
        // Forget mailboxes idle for a minute: their next slot is now anyway.
        pacers.retain(|_, next| *next + std::time::Duration::from_secs(60) > now);
        let next = pacers.entry(key.to_string()).or_insert(now);
        let slot = (*next).max(now);
        *next = slot + interval;
        slot
    };
    tokio::time::sleep_until(slot).await;
}

const WATCH_URL: &str = "https://gmail.googleapis.com/gmail/v1/users/me/watch";
const HISTORY_URL: &str = "https://gmail.googleapis.com/gmail/v1/users/me/history";
const MESSAGES_URL: &str = "https://gmail.googleapis.com/gmail/v1/users/me/messages";

#[cfg(test)]
tokio::task_local! {
    /// Test-only seam: points `users.messages.list`/`users.messages.get`
    /// made inside the scope at a local mock server (same shape as
    /// `google_meet::TEST_API_BASE`). Task-local, so it is parallel-test-safe
    /// and does not exist outside `cfg(test)`.
    pub(crate) static TEST_MESSAGES_URL: String;
}

/// [`MESSAGES_URL`], or the test seam's mock URL inside a
/// [`TEST_MESSAGES_URL`] scope.
fn messages_url() -> String {
    #[cfg(test)]
    if let Ok(url) = TEST_MESSAGES_URL.try_with(|u| u.clone()) {
        return url;
    }
    MESSAGES_URL.to_string()
}
const THREADS_URL: &str = "https://gmail.googleapis.com/gmail/v1/users/me/threads";
const PROFILE_URL: &str = "https://gmail.googleapis.com/gmail/v1/users/me/profile";

/// Page size for `history.list`. Mirrors `google_calendar::LIST_PAGE_SIZE`'s
/// own reasoning: a per-REQUEST size, well under Gmail's own max (500), not
/// a cap on total history processed in one [`fetch_pending`] call (see
/// [`SYNC_PASS_BUDGET`] for that).
const HISTORY_PAGE_SIZE: usize = 250;

/// Max messages (added + deleted refs) processed in one [`fetch_pending`]
/// call before persisting a resume cursor and returning — byte-identical
/// reasoning to `google_calendar::SYNC_PASS_BUDGET`.
const SYNC_PASS_BUDGET: usize = 2000;

/// Re-create the watch if it expires within this margin. Same value/
/// reasoning as `google_calendar::CHANNEL_LIVE_MARGIN_MINUTES`.
const CHANNEL_LIVE_MARGIN_MINUTES: i64 = 30;

// ---------------------------------------------------------------------------
// RLS scoping — mirrors `google_calendar::scoped_tx` exactly.
// ---------------------------------------------------------------------------

async fn scoped_tx(
    pool: &PgPool,
    account_id: Uuid,
    what: &str,
) -> Result<sqlx::Transaction<'static, sqlx::Postgres>, ConnectorError> {
    crate::tenancy::scope::begin_tenant_system_scoped(pool, account_id)
        .await
        .map_err(|e| ConnectorError::Internal(format!("{what} scoped tx: {e}")))
}

fn parse_epoch_millis(s: &str) -> Result<DateTime<Utc>, ConnectorError> {
    let millis: i64 = s
        .parse()
        .map_err(|e| ConnectorError::Exchange(format!("epoch millis parse {s:?}: {e}")))?;
    DateTime::from_timestamp_millis(millis)
        .ok_or_else(|| ConnectorError::Exchange(format!("epoch millis out of range: {millis}")))
}

// ---------------------------------------------------------------------------
// Watch (users.watch) bootstrap
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct GmailWatchResponse {
    #[serde(rename = "historyId")]
    history_id: String,
    /// Milliseconds-since-epoch, as a decimal string — same convention
    /// Calendar's `WatchChannelResponse::expiration` uses.
    expiration: String,
}

#[derive(Debug, Deserialize)]
struct ProfileResponse {
    #[serde(rename = "historyId")]
    history_id: String,
}

/// Whether a live (not imminently expiring) Gmail watch already exists for
/// this mailbox — cheap pre-check mirroring `google_calendar::
/// channel_is_live`.
async fn watch_is_live(control_pool: &PgPool, account_id: Uuid, email: &str) -> Result<bool, ConnectorError> {
    let mut tx = scoped_tx(control_pool, account_id, "watch_is_live").await?;
    let row: Option<(DateTime<Utc>,)> = sqlx::query_as(
        "SELECT expires_at FROM google_watch_channels \
         WHERE account_id = $1 AND surface = 'gmail' AND resource_id = $2",
    )
    .bind(account_id)
    .bind(email)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("watch_is_live: {e}")))?;
    tx.rollback().await.ok();
    Ok(row
        .map(|(expires_at,)| {
            expires_at > Utc::now() + chrono::Duration::minutes(CHANNEL_LIVE_MARGIN_MINUTES)
        })
        .unwrap_or(false))
}

/// Insert or refresh this mailbox's `google_watch_channels` row.
/// `baseline_history_id` seeds `sync_token` ONLY on a fresh INSERT — the
/// `ON CONFLICT` arm deliberately does not touch `sync_token`, so a renewal
/// call (this function re-run once the previous watch is nearing expiry)
/// never clobbers a still-valid, possibly-far-advanced cursor with a stale
/// baseline. Mirrors `google_calendar::upsert_channel`'s "channel bootstrap
/// and cursor state are separate writers" split, collapsed into one function
/// here because Gmail's watch response usefully hands back a cursor value
/// Calendar's channel response never does.
async fn upsert_watch(
    control_pool: &PgPool,
    account_id: Uuid,
    owner_user_id: Uuid,
    email: &str,
    topic_name: &str,
    expires_at: DateTime<Utc>,
    baseline_history_id: &str,
) -> Result<(), ConnectorError> {
    let mut tx = scoped_tx(control_pool, account_id, "upsert_watch").await?;
    sqlx::query(
        r#"
        INSERT INTO google_watch_channels
            (account_id, surface, resource_id, owner_user_id, topic_name, expires_at, sync_token, updated_at)
        VALUES ($1, 'gmail', $2, $3, $4, $5, $6, now())
        ON CONFLICT (account_id, surface, resource_id)
        DO UPDATE SET topic_name = EXCLUDED.topic_name,
                      owner_user_id = EXCLUDED.owner_user_id,
                      expires_at = EXCLUDED.expires_at,
                      last_error = NULL,
                      last_error_at = NULL,
                      updated_at = now()
        "#,
    )
    .bind(account_id)
    .bind(email)
    .bind(owner_user_id)
    .bind(topic_name)
    .bind(expires_at)
    .bind(baseline_history_id)
    .execute(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("upsert_watch: {e}")))?;
    tx.commit()
        .await
        .map_err(|e| ConnectorError::Internal(format!("upsert_watch commit: {e}")))?;
    Ok(())
}

/// Idempotently ensure a live Gmail `users.watch` exists for this mailbox,
/// (re)creating it if absent or imminently expiring. Called from TWO places
/// (mirrors `google_calendar::ensure_watch`'s "Watch bootstrap" doc exactly,
/// applied to Gmail's two entry points instead of Calendar's post_store +
/// every sync): `GoogleConnector::post_store` on a fresh Gmail connect, and
/// [`after_ingest`]/[`fetch_pending`] — i.e. every opt-in ingest and every
/// push-triggered pass, self-healing a member who connected before this PR
/// (or whose watch lapsed) without a dedicated backfill job.
///
/// `control_pool` (gsuite Train G2 PR 7): ALWAYS the control-plane pool,
/// never a content/tenant pool — see the module doc's "Watch rows are
/// CONTROL-plane, always" section. `post_store`'s call site already passed
/// the callback's own `state.db` (control), unchanged by this PR;
/// `after_ingest`/`fetch_pending`'s self-heal calls are the ones this PR
/// redirects.
pub(crate) async fn ensure_watch(
    http: &reqwest::Client,
    control_pool: &PgPool,
    account_id: Uuid,
    owner_user_id: Uuid,
    email: &str,
    access_token: &str,
) -> Result<(), ConnectorError> {
    if watch_is_live(control_pool, account_id, email).await? {
        return Ok(());
    }
    let topic_name = std::env::var("GOOGLE_GMAIL_PUBSUB_TOPIC").map_err(|_| {
        ConnectorError::NotConfigured("GOOGLE_GMAIL_PUBSUB_TOPIC not configured".into())
    })?;
    let resp = google_send(|| {
        http.post(super::google_transport::endpoint(WATCH_URL))
            .bearer_auth(access_token)
            .json(&serde_json::json!({ "topicName": topic_name }))
    })
    .await?;
    if !resp.status().is_success() {
        return Err(google_api_error("gmail users.watch", resp).await);
    }
    let parsed: GmailWatchResponse = resp
        .json()
        .await
        .map_err(|e| ConnectorError::Exchange(format!("users.watch response parse: {e}")))?;
    let expires_at = parse_epoch_millis(&parsed.expiration)?;
    upsert_watch(
        control_pool,
        account_id,
        owner_user_id,
        email,
        &topic_name,
        expires_at,
        &parsed.history_id,
    )
    .await
}

/// The mailbox's CURRENT `historyId`, per `users.getProfile` — used to
/// re-seed a fresh baseline after a 404 (stale `startHistoryId`), without
/// re-registering the watch itself (a plain read, no side effect on
/// `expires_at`/`topic_name`).
async fn fetch_current_history_id(
    http: &reqwest::Client,
    access_token: &str,
) -> Result<String, ConnectorError> {
    let resp = google_send(|| http.get(super::google_transport::endpoint(PROFILE_URL)).bearer_auth(access_token)).await?;
    if !resp.status().is_success() {
        return Err(google_api_error("gmail users.getProfile", resp).await);
    }
    let parsed: ProfileResponse = resp
        .json()
        .await
        .map_err(|e| ConnectorError::Exchange(format!("getProfile response parse: {e}")))?;
    Ok(parsed.history_id)
}

/// Best-effort recovery of the `oauth_tokens.owner_user_id` for a just-
/// stored Gmail connection, keyed by the account+email `GoogleConnector::
/// post_store` has in hand (no `user_id` in that trait method's signature —
/// see `ConnectorProvider::post_store`'s own doc). The row it reads back was
/// written by the SAME request, moments earlier, by `routes::oauth::callback`
/// (`Surface::Gmail.token_owner_for` — always `Member`, never `AccountWide`,
/// for this provider literal), so this is a lookback, not a guess.
pub(crate) async fn resolve_owner_user_id(
    pool: &PgPool,
    account_id: Uuid,
    email: &str,
) -> Result<Option<Uuid>, ConnectorError> {
    let mut tx = crate::tenancy::scope::begin_account_scoped(pool, account_id)
        .await
        .map_err(|e| ConnectorError::Internal(format!("resolve_owner_user_id scoped tx: {e}")))?;
    let row: Option<(Uuid,)> = sqlx::query_as(
        "SELECT owner_user_id FROM oauth_tokens \
         WHERE account_id = $1 AND provider = 'google_gmail' AND provider_workspace_id = $2 \
           AND owner_user_id IS NOT NULL \
         ORDER BY updated_at DESC LIMIT 1",
    )
    .bind(account_id)
    .bind(email)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("resolve_owner_user_id: {e}")))?;
    tx.rollback().await.ok();
    Ok(row.map(|(u,)| u))
}

/// Resolve every `(account_id, owner_user_id)` watching an inbound Gmail
/// Pub/Sub notification's decoded `emailAddress`, via the control-only
/// SECURITY DEFINER resolver (migration `0275`). Empty for an unknown
/// mailbox or one with no `owner_user_id` (structurally impossible for a
/// row this module writes — [`upsert_watch`] always binds one — but the SQL
/// guard is belt-and-suspenders, same posture as `google_calendar::
/// resolve_channel_for_push`'s own empty-placeholder defenses).
///
/// One mailbox, several accounts, is a supported shape, not an ambiguity:
/// the same Gmail address connected under a personal account AND an org
/// account holds its own token, cursor and atoms under each, and the push
/// is a bare "this mailbox changed" trigger for all of them. Migration
/// `0187`'s predecessor resolver returned zero rows on that overlap (fail
/// closed, CodeRabbit round 1 finding #1 on PR #686), which silently
/// starved every such mailbox of live follow and deletion propagation on
/// BOTH accounts; `routes::google_push::run_gmail_sync_for_email` now runs
/// one pass per row instead.
pub(crate) async fn resolve_watch_owners_for_push(
    pool: &PgPool,
    email: &str,
) -> Result<Vec<(Uuid, Uuid)>, ConnectorError> {
    if email.is_empty() {
        return Ok(Vec::new());
    }
    sqlx::query_as("SELECT * FROM resolve_gmail_watch_owners($1)")
        .bind(email)
        .fetch_all(pool)
        .await
        .map_err(|e| ConnectorError::Internal(format!("resolve_watch_owners_for_push: {e}")))
}

// ---------------------------------------------------------------------------
// Sync-cursor persistence (per (account_id, email) — see module doc's
// column-overload section)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
struct SyncState {
    history_id: String,
    backfill_page_token: Option<String>,
}

async fn load_sync_state(control_pool: &PgPool, account_id: Uuid, email: &str) -> Result<SyncState, ConnectorError> {
    let mut tx = scoped_tx(control_pool, account_id, "load_sync_state").await?;
    let row: Option<(String, Option<String>)> = sqlx::query_as(
        "SELECT sync_token, backfill_page_token FROM google_watch_channels \
         WHERE account_id = $1 AND surface = 'gmail' AND resource_id = $2",
    )
    .bind(account_id)
    .bind(email)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("load_sync_state: {e}")))?;
    tx.rollback().await.ok();
    Ok(row
        .map(|(history_id, backfill_page_token)| SyncState {
            history_id,
            backfill_page_token,
        })
        .unwrap_or_default())
}

/// Update the sync-cursor columns on an EXISTING row (never inserts — see
/// module doc: this is only ever called after a successful [`ensure_watch`]
/// already created the row, since [`fetch_pending`] returns early when no
/// baseline `historyId` exists). `history_id: None` preserves the existing
/// value (a mid-pass page-token-only write); `Some("")` is never passed —
/// unlike Calendar's `410`-restart clear, a fresh baseline is always a real
/// value ([`fetch_current_history_id`]), never an empty placeholder.
async fn upsert_sync_columns(
    control_pool: &PgPool,
    account_id: Uuid,
    email: &str,
    history_id: Option<&str>,
    backfill_page_token: Option<&str>,
    backfill_complete: bool,
) -> Result<(), ConnectorError> {
    let mut tx = scoped_tx(control_pool, account_id, "upsert_sync_columns").await?;
    sqlx::query(
        "UPDATE google_watch_channels \
         SET sync_token = COALESCE($3, sync_token), \
             backfill_page_token = $4, \
             backfill_complete = $5, \
             updated_at = now() \
         WHERE account_id = $1 AND surface = 'gmail' AND resource_id = $2",
    )
    .bind(account_id)
    .bind(email)
    .bind(history_id)
    .bind(backfill_page_token)
    .bind(backfill_complete)
    .execute(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("upsert_sync_columns: {e}")))?;
    tx.commit()
        .await
        .map_err(|e| ConnectorError::Internal(format!("upsert_sync_columns commit: {e}")))?;
    Ok(())
}

async fn persist_history_progress(
    control_pool: &PgPool,
    account_id: Uuid,
    email: &str,
    resume_page_token: Option<&str>,
) -> Result<(), ConnectorError> {
    upsert_sync_columns(control_pool, account_id, email, None, resume_page_token, false).await
}

async fn persist_history_complete(
    control_pool: &PgPool,
    account_id: Uuid,
    email: &str,
    new_history_id: &str,
) -> Result<(), ConnectorError> {
    upsert_sync_columns(control_pool, account_id, email, Some(new_history_id), None, true).await
}

async fn restart_history_cursor(
    control_pool: &PgPool,
    account_id: Uuid,
    email: &str,
    fresh_baseline: &str,
) -> Result<(), ConnectorError> {
    upsert_sync_columns(control_pool, account_id, email, Some(fresh_baseline), None, true).await
}


/// Record that a push for this mailbox found its pass lock held (by a pass
/// on another machine). Keeps the earliest time if already set.
pub(crate) async fn mark_push_pending(control_pool: &PgPool, account_id: Uuid, email: &str) -> Result<(), ConnectorError> {
    let mut tx = scoped_tx(control_pool, account_id, "mark_push_pending").await?;
    sqlx::query(
        "UPDATE google_watch_channels SET push_pending_at = COALESCE(push_pending_at, now()) \
         WHERE account_id = $1 AND surface = 'gmail' AND resource_id = $2",
    )
    .bind(account_id)
    .bind(email)
    .execute(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("mark_push_pending: {e}")))?;
    tx.commit()
        .await
        .map_err(|e| ConnectorError::Internal(format!("mark_push_pending commit: {e}")))
}

/// Clear the mark. Called by the lock holder before its pass reads the
/// cursor, so that pass covers every push that marked before this.
pub(crate) async fn clear_push_pending(control_pool: &PgPool, account_id: Uuid, email: &str) -> Result<(), ConnectorError> {
    let mut tx = scoped_tx(control_pool, account_id, "clear_push_pending").await?;
    sqlx::query(
        "UPDATE google_watch_channels SET push_pending_at = NULL \
         WHERE account_id = $1 AND surface = 'gmail' AND resource_id = $2 AND push_pending_at IS NOT NULL",
    )
    .bind(account_id)
    .bind(email)
    .execute(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("clear_push_pending: {e}")))?;
    tx.commit()
        .await
        .map_err(|e| ConnectorError::Internal(format!("clear_push_pending commit: {e}")))
}

/// Whether a push has marked the mailbox since the last clear.
pub(crate) async fn push_pending(control_pool: &PgPool, account_id: Uuid, email: &str) -> Result<bool, ConnectorError> {
    let mut tx = scoped_tx(control_pool, account_id, "push_pending").await?;
    let pending: Option<bool> = sqlx::query_scalar(
        "SELECT push_pending_at IS NOT NULL FROM google_watch_channels \
         WHERE account_id = $1 AND surface = 'gmail' AND resource_id = $2",
    )
    .bind(account_id)
    .bind(email)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("push_pending: {e}")))?;
    tx.rollback().await.ok();
    Ok(pending.unwrap_or(false))
}

// ---------------------------------------------------------------------------
// history.list fetch + pagination
// ---------------------------------------------------------------------------

enum ListOutcome {
    Page(GHistoryListResponse),
    /// `404 NOT_FOUND` — Gmail's analogue of Calendar's `410 GONE`: the
    /// stored `startHistoryId` refers further back than Gmail retained
    /// history for.
    TooOld,
}

#[derive(Debug, Deserialize)]
struct GHistoryListResponse {
    #[serde(default)]
    history: Vec<GHistoryRecord>,
    #[serde(default, rename = "nextPageToken")]
    next_page_token: Option<String>,
    /// Present on every `history.list` response (unlike Calendar's
    /// `nextSyncToken`, which is terminal-page-only) — see [`decide_after_page`]'s
    /// doc for why this module still only banks it once the pass reaches its
    /// terminal page.
    #[serde(default, rename = "historyId")]
    history_id: Option<String>,
}

/// One `history.list` record. `labelsAdded`/`labelsRemoved` are read ONLY
/// for the two labels that move a message out of (or back into) the visible
/// mailbox — see [`HIDING_LABELS`]; every other label-only change (UNREAD,
/// STARRED, a user label) is still neither a create nor a delete.
#[derive(Debug, Deserialize)]
struct GHistoryRecord {
    #[serde(default, rename = "messagesAdded")]
    messages_added: Vec<GHistoryMessageRef>,
    #[serde(default, rename = "messagesDeleted")]
    messages_deleted: Vec<GHistoryMessageRef>,
    #[serde(default, rename = "labelsAdded")]
    labels_added: Vec<GHistoryLabelRef>,
    #[serde(default, rename = "labelsRemoved")]
    labels_removed: Vec<GHistoryLabelRef>,
}

#[derive(Debug, Deserialize)]
struct GHistoryMessageRef {
    message: GHistoryMessage,
}

/// A `labelsAdded`/`labelsRemoved` entry: the message plus the label ids
/// that changed in this record (NOT the message's full current label set,
/// which Gmail also echoes under `message.labelIds`).
#[derive(Debug, Deserialize)]
struct GHistoryLabelRef {
    message: GHistoryMessage,
    #[serde(default, rename = "labelIds")]
    label_ids: Vec<String>,
}

/// System labels whose presence removes a message from the visible mailbox.
/// Gmail's own `users.history.list` reference says `messagesDeleted` is
/// "messages deleted (not Trashed)": moving a message to Trash (or marking
/// it Spam) only shows up as `labelsAdded` with one of these ids, and the
/// `messagesDeleted` entry arrives only on permanent deletion — including
/// the automatic 30-day Trash/Spam purge. Both labels are treated alike:
/// `users.messages.list` hides both by default (`includeSpamTrash=false`),
/// both auto-purge after 30 days, and founder decision Q3 (`.scratch/
/// gsuite-enterprise/plan.md`) already names "Spam/Trash purges" as
/// deletions this connector must propagate — the retraction merely happens
/// when the user acts instead of 30 days later. Un-trashing / "Not spam" is
/// the inverse: `labelsRemoved` naming one of these ids restores the
/// message to the mailbox, so it is re-fetched and re-ingested.
const HIDING_LABELS: [&str; 2] = ["TRASH", "SPAM"];

fn hides_message(label_ids: &[String]) -> bool {
    label_ids.iter().any(|l| HIDING_LABELS.contains(&l.as_str()))
}

/// Whether a fetched message is CURRENTLY in Trash or Spam — the gate every
/// non-explicit ingest path applies before shaping a message (the live-follow
/// pass, a `threads.get` expansion's siblings, the everything-mode walk). An
/// explicitly opened message ([`fetch_items`]'s own `item_ids`) is never
/// gated: a user saving a message they are looking at gets it regardless.
fn message_is_hidden(msg: &GmailMessage) -> bool {
    hides_message(&msg.label_ids)
}

/// What one history entry did to one message, in the vocabulary this
/// connector acts on: `Added`/`Restored` are fetched and ingested
/// (`Restored` back into the ocean a hide tombstoned it out of — see
/// [`propagate_hides`]), `Hidden` is a reversible graph-confined
/// retraction, `Deleted` a permanent one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HistoryChange {
    Added,
    Restored,
    Hidden,
    Deleted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct HistoryEvent {
    thread_id: String,
    message_id: String,
    change: HistoryChange,
}

#[derive(Debug, Deserialize)]
struct GHistoryMessage {
    #[serde(default)]
    id: String,
    #[serde(default, rename = "threadId")]
    thread_id: String,
}

async fn list_history_page(
    http: &reqwest::Client,
    access_token: &str,
    start_history_id: &str,
    page_token: Option<&str>,
) -> Result<ListOutcome, ConnectorError> {
    let mut url = format!(
        "{HISTORY_URL}?startHistoryId={}&maxResults={HISTORY_PAGE_SIZE}",
        urlencoding::encode(start_history_id)
    );
    if let Some(pt) = page_token {
        url.push_str("&pageToken=");
        url.push_str(&urlencoding::encode(pt));
    }
    let resp = google_send(|| http.get(super::google_transport::endpoint(&url)).bearer_auth(access_token)).await?;
    if resp.status() == reqwest::StatusCode::NOT_FOUND {
        return Ok(ListOutcome::TooOld);
    }
    if !resp.status().is_success() {
        return Err(google_api_error("gmail history.list", resp).await);
    }
    let page: GHistoryListResponse = resp
        .json()
        .await
        .map_err(|e| ConnectorError::Exchange(format!("history.list parse: {e}")))?;
    Ok(ListOutcome::Page(page))
}

/// Whether accumulating one more page would push a [`run_pass`] call past
/// its per-call `budget` — pure, identical shape to
/// `google_calendar::budget_exceeded`.
fn budget_exceeded(added: usize, deleted: usize, budget: usize) -> bool {
    added + deleted >= budget
}

/// Flatten one `history.list` page into [`HistoryEvent`]s, in Gmail's own
/// (chronological) record order. `threadId` is kept on every event — Gmail
/// carries it on a deletion entry exactly like on an addition
/// (`backend/tests/fixtures/gmail/history_list_page.json`'s own
/// `messagesDeleted` entry has one), and [`propagate_deletions`] needs it to
/// resolve the SAME graph a thread was actually ingested into
/// (`gmail_watched_threads.graph_id`) rather than re-derive one via
/// `resolve_primary_graph` (CodeRabbit round 1 finding #2 on PR #686; see
/// that function's own doc). Within one record the hiding/deleting entries
/// are emitted AFTER the adding/restoring ones, so [`net_history`]'s
/// last-wins rule resolves "spam → trash in one record" (`labelsRemoved
/// SPAM` + `labelsAdded TRASH`) to hidden. Pure — no I/O. Empty-id entries
/// are filtered at this funnel (the empty-id poison-pill lesson,
/// `google_calendar::process_events_page`'s own doc).
fn process_history_page(page: GHistoryListResponse) -> Vec<HistoryEvent> {
    fn push(out: &mut Vec<HistoryEvent>, m: GHistoryMessage, change: HistoryChange, what: &str) {
        let id = m.id.trim();
        if id.is_empty() {
            tracing::warn!("gmail: {what} entry with missing/empty id skipped");
            return;
        }
        out.push(HistoryEvent { thread_id: m.thread_id, message_id: id.to_string(), change });
    }
    let mut events = Vec::new();
    for record in page.history {
        for r in record.messages_added {
            push(&mut events, r.message, HistoryChange::Added, "messagesAdded");
        }
        for r in record.labels_removed.into_iter().filter(|r| hides_message(&r.label_ids)) {
            push(&mut events, r.message, HistoryChange::Restored, "labelsRemoved");
        }
        for r in record.labels_added.into_iter().filter(|r| hides_message(&r.label_ids)) {
            push(&mut events, r.message, HistoryChange::Hidden, "labelsAdded");
        }
        for r in record.messages_deleted {
            push(&mut events, r.message, HistoryChange::Deleted, "messagesDeleted");
        }
    }
    events
}

/// The net effect of one pass on each message, `(thread_id, message_id)`
/// per bucket: `added` and `restored` are to be fetched, `hidden` is to be
/// retracted reversibly ([`propagate_hides`]), `deleted` permanently
/// ([`propagate_deletions`]). One message can appear several times in a
/// single pass (new → trashed, trashed → untrashed → trashed again, …); its
/// LAST event decides, because [`fetch_pending`] retracts eagerly and hands
/// creates back to the caller to ingest afterwards — acting on every event
/// in order would resurrect a message trashed in the same pass, and
/// `messages.get` on one deleted in the same pass would 404. Bucket order
/// is first-seen order, so a message's position is stable across retries
/// of the same range.
fn net_history(events: Vec<HistoryEvent>) -> NetHistory {
    let mut order: Vec<String> = Vec::new();
    let mut last: std::collections::HashMap<String, (String, HistoryChange)> = std::collections::HashMap::new();
    for ev in events {
        if !last.contains_key(&ev.message_id) {
            order.push(ev.message_id.clone());
        }
        last.insert(ev.message_id, (ev.thread_id, ev.change));
    }
    let mut net = NetHistory::default();
    for message_id in order {
        let (thread_id, change) = last.remove(&message_id).expect("every ordered id was inserted");
        let bucket = match change {
            HistoryChange::Added => &mut net.added,
            HistoryChange::Restored => &mut net.restored,
            HistoryChange::Hidden => &mut net.hidden,
            HistoryChange::Deleted => &mut net.deleted,
        };
        bucket.push((thread_id, message_id));
    }
    net
}

#[derive(Debug, Default, PartialEq, Eq)]
struct NetHistory {
    added: Vec<(String, String)>,
    restored: Vec<(String, String)>,
    /// Moved to Trash/Spam ([`HIDING_LABELS`]) — reversible.
    hidden: Vec<(String, String)>,
    /// `messagesDeleted` — permanent.
    deleted: Vec<(String, String)>,
}

struct PassResult {
    /// See [`net_history`].
    net: NetHistory,
    resume_page_token: Option<String>,
    new_history_id: Option<String>,
}

enum PassOutcome {
    Progress(PassResult),
    TooOld,
}

/// What to do after accumulating one more `history.list` page — pure,
/// structurally identical to `google_calendar::decide_after_page` (same 4
/// boundary tests port directly): a TERMINAL page (`next_page_token: None`)
/// always finishes the pass and hands back the current `historyId`,
/// regardless of budget. Gmail's `historyId` is present on EVERY page
/// (unlike Calendar's `nextSyncToken`, terminal-page-only) — this module
/// deliberately still only banks it on the terminal page: there is no
/// correctness benefit to advancing early (a resumed pass re-derives the
/// same eventual terminal value), and doing so uniformly keeps the
/// commit-after-ingest contract identical to Calendar's, rather than a
/// Gmail-specific carve-out.
fn decide_after_page(
    next_page_token: Option<String>,
    next_history_id: Option<String>,
    accumulated_added: usize,
    accumulated_deleted: usize,
    budget: usize,
) -> PageDecision {
    match next_page_token {
        None => PageDecision::Done {
            resume_page_token: None,
            new_history_id: next_history_id,
        },
        Some(next) => {
            if budget_exceeded(accumulated_added, accumulated_deleted, budget) {
                PageDecision::Done {
                    resume_page_token: Some(next),
                    new_history_id: None,
                }
            } else {
                PageDecision::Continue(next)
            }
        }
    }
}

enum PageDecision {
    Continue(String),
    Done {
        resume_page_token: Option<String>,
        new_history_id: Option<String>,
    },
}

/// Page `history.list` starting at `start_history_id` (resuming at
/// `resume_page_token` if `Some`), accumulating added/deleted refs up to
/// `budget`. Mirrors `google_calendar::run_pass` exactly.
async fn run_pass(
    http: &reqwest::Client,
    access_token: &str,
    start_history_id: &str,
    resume_page_token: Option<String>,
    budget: usize,
) -> Result<PassOutcome, ConnectorError> {
    let mut page_token = resume_page_token;
    let mut events: Vec<HistoryEvent> = Vec::new();
    let (mut creates, mut retractions) = (0usize, 0usize);
    loop {
        match list_history_page(http, access_token, start_history_id, page_token.as_deref()).await? {
            ListOutcome::TooOld => return Ok(PassOutcome::TooOld),
            ListOutcome::Page(page) => {
                let next_page_token = page.next_page_token.clone();
                let next_history_id = page.history_id.clone();
                for ev in process_history_page(page) {
                    match ev.change {
                        HistoryChange::Added | HistoryChange::Restored => creates += 1,
                        HistoryChange::Hidden | HistoryChange::Deleted => retractions += 1,
                    }
                    events.push(ev);
                }

                match decide_after_page(next_page_token, next_history_id, creates, retractions, budget) {
                    PageDecision::Continue(next) => page_token = Some(next),
                    PageDecision::Done { resume_page_token, new_history_id } => {
                        return Ok(PassOutcome::Progress(PassResult {
                            net: net_history(events),
                            resume_page_token,
                            new_history_id,
                        }));
                    }
                }
            }
        }
    }
}

/// Discard every `(thread_id, message_id)` whose thread is not in `watched`
/// — the ADR-0008 storage-scoping decision (module doc's "Live follow").
/// Pure — no I/O — so the discard rule itself is directly testable without a
/// live/mocked `gmail_watched_threads` lookup.
fn filter_watched(added: Vec<(String, String)>, watched: &HashSet<String>) -> Vec<(String, String)> {
    added.into_iter().filter(|(thread_id, _)| watched.contains(thread_id)).collect()
}

// ---------------------------------------------------------------------------
// gmail_watched_threads persistence
// ---------------------------------------------------------------------------

/// Which of `thread_ids` are currently watched for `account_id` AND still
/// followed — the live-follow filter (gmail-options backend train, PR 1 of
/// 4): [`fetch_pending`] is this function's only caller, and it is the
/// `history.list` diff's "should this newly-added message be auto-captured
/// as a reply" gate, so `follow_replies = TRUE` belongs here directly rather
/// than as a second filter downstream — a thread with `follow_replies =
/// FALSE` is a real, intentional row (deletion propagation still needs it,
/// see [`watched_thread_graph_ids`] just below, which deliberately does NOT
/// carry this filter) that simply should not resurrect live-follow ingest.
async fn watched_thread_ids_among(
    content_pool: &PgPool,
    account_id: Uuid,
    thread_ids: &[String],
) -> Result<HashSet<String>, ConnectorError> {
    if thread_ids.is_empty() {
        return Ok(HashSet::new());
    }
    let mut tx = scoped_tx(content_pool, account_id, "watched_thread_ids_among").await?;
    let rows: Vec<(String,)> = sqlx::query_as(
        "SELECT thread_id FROM gmail_watched_threads \
         WHERE account_id = $1 AND thread_id = ANY($2) AND follow_replies = TRUE",
    )
    .bind(account_id)
    .bind(thread_ids)
    .fetch_all(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("watched_thread_ids_among: {e}")))?;
    tx.rollback().await.ok();
    Ok(rows.into_iter().map(|(t,)| t).collect())
}

/// Follow boundaries for the incremental pass. Existing thread saves have
/// NULL; message saves reject pre-save additions that an older history
/// cursor can still report. Deletion mapping remains a separate flag-blind read.
async fn watched_thread_follow_since(
    pool: &PgPool, account_id: Uuid, thread_ids: &[String],
) -> Result<std::collections::HashMap<String, Option<DateTime<Utc>>>, ConnectorError> {
    let mut tx = scoped_tx(pool, account_id, "watched_thread_follow_since").await?;
    let rows: Vec<(String, Option<DateTime<Utc>>)> = sqlx::query_as(
        "SELECT thread_id, follow_since FROM gmail_watched_threads WHERE account_id=$1 AND thread_id=ANY($2) AND follow_replies=TRUE",
    ).bind(account_id).bind(thread_ids).fetch_all(&mut *tx).await
        .map_err(|e| ConnectorError::Internal(format!("watched_thread_follow_since: {e}")))?;
    tx.rollback().await.ok();
    Ok(rows.into_iter().collect())
}

fn gmail_message_is_future_reply(msg: &GmailMessage, follow_since: Option<DateTime<Utc>>) -> bool {
    follow_since.is_none_or(|since| msg.internal_date.as_deref()
        .and_then(|s| s.parse::<i64>().ok())
        .and_then(DateTime::<Utc>::from_timestamp_millis)
        .is_some_and(|timestamp| timestamp >= since))
}

/// Every saved and recorded reply destination, including unfollowed threads.
/// Member graphs belong to the mailbox owner even when an account shares a thread.
async fn watched_thread_graph_ids(
    content_pool: &PgPool,
    account_id: Uuid,
    owner_user_id: Uuid,
    thread_ids: &[String],
) -> Result<std::collections::HashMap<String, Vec<Uuid>>, ConnectorError> {
    if thread_ids.is_empty() {
        return Ok(std::collections::HashMap::new());
    }
    let mut tx = scoped_tx(content_pool, account_id, "watched_thread_graph_ids").await?;
    let rows: Vec<(String, Uuid)> = sqlx::query_as(
        "SELECT DISTINCT w.thread_id, g.id FROM gmail_watched_threads w \
         JOIN graphs g ON g.id = ANY(array_prepend(w.graph_id, w.landing_graph_ids)) \
         WHERE w.account_id = $1 AND g.account_id = $1 AND w.thread_id = ANY($3) \
           AND (g.kind = 'personal' OR (g.kind = 'member' AND g.owner_user_id = $2))",
    )
    .bind(account_id)
    .bind(owner_user_id)
    .bind(thread_ids)
    .fetch_all(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("watched_thread_graph_ids: {e}")))?;
    tx.rollback().await.ok();
    let mut graphs = std::collections::HashMap::<String, Vec<Uuid>>::new();
    for (thread_id, graph_id) in rows {
        graphs.entry(thread_id).or_default().push(graph_id);
    }
    Ok(graphs)
}

/// Record before ingest. An extra empty destination is safe after an ingest failure.
pub(crate) async fn record_reply_landing_graph(
    content_pool: &PgPool,
    account_id: Uuid,
    owner_user_id: Uuid,
    thread_id: &str,
    graph_id: Uuid,
) -> Result<(), ConnectorError> {
    let mut tx = scoped_tx(content_pool, account_id, "record_reply_landing_graph").await?;
    sqlx::query(
        "UPDATE gmail_watched_threads w SET landing_graph_ids = array_append(w.landing_graph_ids, $4) \
         WHERE w.account_id = $1 AND w.thread_id = $3 AND w.follow_replies \
           AND NOT ($4 = ANY(array_prepend(w.graph_id, w.landing_graph_ids))) \
           AND EXISTS (SELECT 1 FROM graphs g WHERE g.id = $4 AND g.account_id = $1 \
                       AND (g.kind = 'personal' OR (g.kind = 'member' AND g.owner_user_id = $2)))",
    )
    .bind(account_id)
    .bind(owner_user_id)
    .bind(thread_id)
    .bind(graph_id)
    .execute(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("record_reply_landing_graph: {e}")))?;
    tx.commit()
        .await
        .map_err(|e| ConnectorError::Internal(format!("record_reply_landing_graph commit: {e}")))?;
    Ok(())
}

/// Recover pre-release imports from their source identity and attribution.
/// Promotions retain attribution, so their explicit copy marker must be excluded.
async fn original_message_graph_ids(
    content_pool: &PgPool,
    account_id: Uuid,
    owner_user_id: Uuid,
    message_id: &str,
) -> Result<Vec<Uuid>, ConnectorError> {
    let mut tx = scoped_tx(content_pool, account_id, "original_message_graph_ids").await?;
    let graphs = sqlx::query_scalar(
        "SELECT DISTINCT a.graph_id FROM atoms a JOIN graphs g ON g.id = a.graph_id \
         WHERE g.account_id = $1 AND a.ingested_by = $2 AND a.source_type = 'gmail' \
           AND (a.source_id = $3 OR a.source_id LIKE $4) AND a.promoted_from_atom_id IS NULL \
           AND (g.kind = 'personal' OR (g.kind = 'member' AND g.owner_user_id = $2)) \
         UNION SELECT s.graph_id FROM source_items s JOIN graphs g ON g.id=s.graph_id \
         JOIN source_versions v ON v.version_id=s.current_version_id AND v.graph_id=s.graph_id \
         WHERE g.account_id=$1 AND v.ingested_by=$2 AND s.source_type='gmail' AND s.source_id=$3 \
           AND (g.kind='personal' OR (g.kind='member' AND g.owner_user_id=$2))",
    )
    .bind(account_id)
    .bind(owner_user_id)
    .bind(message_id)
    .bind(crate::services::deletion::escaped_chunk_pattern(message_id))
    .fetch_all(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("original_message_graph_ids: {e}")))?;
    tx.rollback().await.ok();
    Ok(graphs)
}

// ---------------------------------------------------------------------------
// gmail_hidden_messages — what a reversible hide retracted (migrations
// control 0277 / tenant 0116)
// ---------------------------------------------------------------------------

/// `message_id -> every graph a hide retracted it from`, for `message_ids`
/// — the ONLY re-ingest gate for [`HistoryChange::Restored`]. A restored
/// message without a tombstone was never taken out of the ocean by a hide,
/// so [`fetch_pending`] treats it as a new arrival and applies the same
/// live-follow gates as to any other (`follow_replies`, `follow_since`,
/// [`message_is_hidden`]). The earlier coverage rule ("any
/// `gmail_watched_threads` row, or everything mode on") re-ingested mail
/// the user never opted into: every sibling of a `scope=message` save once
/// the whole conversation was trashed and restored, and an unfollowed
/// thread's new reply the moment "Not spam" was clicked.
///
/// Keyed on the owner, not just the account: on the org plane every member
/// shares one `account_id`, and two members can hold the same message id
/// (the same mail delivered to both), so a member's restore must only ever
/// see the tombstones their own hide wrote.
async fn hidden_messages_for(
    content_pool: &PgPool,
    account_id: Uuid,
    owner_user_id: Uuid,
    message_ids: &[String],
) -> Result<std::collections::HashMap<String, Vec<Uuid>>, ConnectorError> {
    if message_ids.is_empty() {
        return Ok(std::collections::HashMap::new());
    }
    let mut tx = scoped_tx(content_pool, account_id, "hidden_messages_for").await?;
    let rows: Vec<(String, Uuid)> = sqlx::query_as(
        "SELECT message_id, graph_id FROM gmail_hidden_messages \
         WHERE account_id = $1 AND owner_user_id = $2 AND message_id = ANY($3) ORDER BY hidden_at, graph_id",
    )
    .bind(account_id)
    .bind(owner_user_id)
    .bind(message_ids)
    .fetch_all(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("hidden_messages_for: {e}")))?;
    tx.rollback().await.ok();
    let mut out: std::collections::HashMap<String, Vec<Uuid>> = std::collections::HashMap::new();
    for (message_id, graph_id) in rows {
        out.entry(message_id).or_default().push(graph_id);
    }
    Ok(out)
}

/// Tombstone one coral a hide is about to retract from `graph_id`. Written
/// BEFORE the retraction: a failure in between leaves a tombstone over a
/// coral that is still there (harmless — the retried hide retracts it, and
/// a restore's re-ingest dedupes on it), never a retracted coral with no
/// way back. `ON CONFLICT DO NOTHING`: replayed history re-hides mail that
/// is already hidden.
async fn upsert_hidden_message(
    content_pool: &PgPool,
    account_id: Uuid,
    owner_user_id: Uuid,
    graph_id: Uuid,
    thread_id: &str,
    message_id: &str,
) -> Result<(), ConnectorError> {
    let mut tx = scoped_tx(content_pool, account_id, "upsert_hidden_message").await?;
    sqlx::query(
        "INSERT INTO gmail_hidden_messages (account_id, graph_id, owner_user_id, thread_id, message_id) \
         VALUES ($1, $2, $3, $4, $5) \
         ON CONFLICT (account_id, graph_id, message_id) DO NOTHING",
    )
    .bind(account_id)
    .bind(graph_id)
    .bind(owner_user_id)
    .bind(thread_id)
    .bind(message_id)
    .execute(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("upsert_hidden_message: {e}")))?;
    tx.commit()
        .await
        .map_err(|e| ConnectorError::Internal(format!("upsert_hidden_message commit: {e}")))?;
    Ok(())
}

/// Clear one tombstone once its restore has re-ingested the message into
/// `graph_id` — called by `routes::google_push::ingest_and_commit` AFTER the
/// pass's cursor commit succeeded, never before: the cursor only moves when
/// every item of the batch landed, so a tombstone cleared any earlier would
/// let the replay of a partly failed batch find no tombstone and route the
/// message through live-follow into the primary ocean instead — a duplicate
/// whenever that differs from the ocean the hide took it out of. Also the
/// exit for a restore whose ocean its owner can no longer write to (an
/// archived member ocean): nothing can go back, so the tombstone goes with
/// the commit rather than parking the cursor on that message forever.
pub(crate) async fn clear_hidden_message(
    content_pool: &PgPool,
    account_id: Uuid,
    graph_id: Uuid,
    message_id: &str,
) -> Result<(), ConnectorError> {
    let mut tx = scoped_tx(content_pool, account_id, "clear_hidden_message").await?;
    sqlx::query("DELETE FROM gmail_hidden_messages WHERE account_id = $1 AND graph_id = $2 AND message_id = $3")
        .bind(account_id)
        .bind(graph_id)
        .bind(message_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| ConnectorError::Internal(format!("clear_hidden_message: {e}")))?;
    tx.commit()
        .await
        .map_err(|e| ConnectorError::Internal(format!("clear_hidden_message commit: {e}")))?;
    Ok(())
}

/// Drop every tombstone for `message_ids`, whichever ocean it names — a
/// permanent `messagesDeleted` has nothing left to restore.
async fn clear_hidden_messages_for(
    content_pool: &PgPool,
    account_id: Uuid,
    message_ids: &[String],
) -> Result<(), ConnectorError> {
    if message_ids.is_empty() {
        return Ok(());
    }
    let mut tx = scoped_tx(content_pool, account_id, "clear_hidden_messages_for").await?;
    sqlx::query("DELETE FROM gmail_hidden_messages WHERE account_id = $1 AND message_id = ANY($2)")
        .bind(account_id)
        .bind(message_ids)
        .execute(&mut *tx)
        .await
        .map_err(|e| ConnectorError::Internal(format!("clear_hidden_messages_for: {e}")))?;
    tx.commit()
        .await
        .map_err(|e| ConnectorError::Internal(format!("clear_hidden_messages_for commit: {e}")))?;
    Ok(())
}

/// Record a thread as watched (opt-in ingest just captured it) — written for
/// EVERY opt-in save, always, including `scope=message` and `follow=false`
/// (locked decision — see `routes::connectors::ingest_selected`'s own doc:
/// deletion propagation resolves its target ocean from this row's
/// `graph_id`, so no row means an upstream deletion can never be propagated
/// again). Mirrors `microsoft::upsert_watched_conversation`, with
/// `backfill_complete = TRUE` always — see module doc's "gmail_watched_threads"
/// migration section for why G2.3's synchronous, non-paginated
/// `threads.get(format=full)` opt-in path never leaves this incomplete.
///
/// `scope=message` (gmail-options backend train, PR 1 of 4) does not change
/// that: nothing in this codebase ever reads `gmail_watched_threads.
/// backfill_complete`/`backfill_cursor` today (`idx_gmail_watched_threads_
/// backfill_pending`'s own migration header — `0186` — says they are
/// "reserved for Train G2.4's everything-mode resumable backfill... unused
/// by G2.3's own opt-in path"), so inserting a message-scope row with
/// `backfill_complete = TRUE` (same as thread-scope, unconditionally) is not
/// a special case — it is simply staying consistent with the value every
/// row has always carried, and it means a message-scope save can never
/// later be silently expanded into a full-thread backfill by some future
/// worker that starts consulting this flag, since there is nothing pending
/// to find. If Train G2.4 ever wires a real resumable-backfill consumer for
/// this table, IT will own deciding whether a message-scope opt-in should
/// arm it — this function makes no such promise either way.
///
/// `follow_replies` (gmail-options backend train, PR 1 of 4): set from the
/// caller's `follow` knob on insert, and RE-SET on every re-save via
/// `ON CONFLICT ... DO UPDATE SET follow_replies = EXCLUDED.follow_replies`
/// — a later save of an already-watched thread adopts THAT save's `follow`
/// value (re-saving with `follow=true` re-follows an unfollowed thread;
/// re-saving with `follow=false` un-follows a followed one). Never deletes
/// the row either way.
async fn upsert_watched_thread(
    content_pool: &PgPool,
    account_id: Uuid,
    graph_id: Uuid,
    ingested_by: Uuid,
    thread_id: &str,
    follow_replies: bool,
    follow_since: Option<DateTime<Utc>>,
) -> Result<(), ConnectorError> {
    let mut tx = scoped_tx(content_pool, account_id, "upsert_watched_thread").await?;
    sqlx::query(
        r#"
        INSERT INTO gmail_watched_threads
            (account_id, graph_id, ingested_by, thread_id, last_ingested_at, backfill_complete, follow_replies, follow_since)
        VALUES ($1, $2, $3, $4, now(), TRUE, $5, $6)
        ON CONFLICT (account_id, thread_id)
        DO UPDATE SET last_ingested_at = now(), follow_replies = EXCLUDED.follow_replies,
                      follow_since = EXCLUDED.follow_since, backfill_complete = TRUE, backfill_cursor = NULL,
                      landing_graph_ids = CASE
                          WHEN EXCLUDED.graph_id = ANY(array_prepend(gmail_watched_threads.graph_id, gmail_watched_threads.landing_graph_ids))
                          THEN gmail_watched_threads.landing_graph_ids
                          ELSE array_append(gmail_watched_threads.landing_graph_ids, EXCLUDED.graph_id)
                      END
        "#,
    )
    .bind(account_id)
    .bind(graph_id)
    .bind(ingested_by)
    .bind(thread_id)
    .bind(follow_replies)
    .bind(follow_since)
    .execute(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("upsert_watched_thread: {e}")))?;
    tx.commit()
        .await
        .map_err(|e| ConnectorError::Internal(format!("upsert_watched_thread commit: {e}")))?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Provider-generic watched resource (gmail-options backend train, PR 1 of
// 4): status / toggle / list — backs `routes::connectors`'s
// `GET|PUT|DELETE /v1/connectors/{provider}/watched/{thread_id}` and
// `GET /v1/connectors/{provider}/watched`, reused (via that route module's
// own per-provider dispatch) by the Gmail add-on, the Outlook add-in and the
// web app. See `connectors::microsoft`'s equivalent trio for the Outlook
// twin.
// ---------------------------------------------------------------------------

/// `Some(following)` for an existing row, `None` if this account has never
/// opted this thread in. Never writes anything.
pub(crate) async fn watched_status(
    content_pool: &PgPool,
    account_id: Uuid,
    thread_id: &str,
) -> Result<Option<bool>, ConnectorError> {
    let mut tx = crate::tenancy::scope::begin_request_system_read(content_pool, account_id).await.map_err(|e| ConnectorError::Internal(format!("watched_status scoped tx: {e}")))?;
    let row: Option<(bool,)> = sqlx::query_as(
        "SELECT follow_replies FROM gmail_watched_threads WHERE account_id = $1 AND thread_id = $2",
    )
    .bind(account_id)
    .bind(thread_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("watched_status: {e}")))?;
    tx.rollback().await.ok();
    Ok(row.map(|(f,)| f))
}

/// Flip `follow_replies` for an already-watched thread. `Some(new_value)` on
/// success, `None` if no row exists for this account + thread — the PUT/
/// DELETE watched-resource routes turn that into a `404`. NEVER inserts a
/// row and NEVER deletes one, regardless of `follow` — unfollowing (whether
/// via `PUT {"follow": false}` or the `DELETE` alias for it) only flips the
/// flag, for the same reason [`upsert_watched_thread`] always writes a row:
/// deletion propagation resolves its target ocean from this row's
/// `graph_id`, so removing the row would silently stop upstream deletions
/// from ever propagating again.
pub(crate) async fn set_follow(
    content_pool: &PgPool,
    account_id: Uuid,
    thread_id: &str,
    follow: bool,
) -> Result<Option<bool>, ConnectorError> {
    let mut tx = scoped_tx(content_pool, account_id, "set_follow").await?;
    let row: Option<(bool,)> = sqlx::query_as(
        r#"
        UPDATE gmail_watched_threads
        SET follow_replies = $1
        WHERE account_id = $2 AND thread_id = $3
        RETURNING follow_replies
        "#,
    )
    .bind(follow)
    .bind(account_id)
    .bind(thread_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("set_follow: {e}")))?;
    tx.commit()
        .await
        .map_err(|e| ConnectorError::Internal(format!("set_follow commit: {e}")))?;
    Ok(row.map(|(f,)| f))
}

/// One row of the watched-thread list (`GET /v1/connectors/{provider}/watched`).
pub(crate) struct WatchedRow {
    /// Kept for pg-test assertions and any future caller — the route
    /// handler itself only needs the cursor tuple's own copy (extracted
    /// before this conversion), not this field.
    #[allow(dead_code)]
    pub(crate) id: Uuid,
    pub(crate) thread_id: String,
    pub(crate) graph_id: Uuid,
    pub(crate) following: bool,
    pub(crate) saved_at: DateTime<Utc>,
}

/// Newest-first (by `saved_at` — see field doc) page of every watched thread
/// for `account_id`, following or not (the web list filters/toggles
/// client-side — this function has no opinion). `cursor` is the previous
/// page's last row's own `id` — the same keyset-over-the-row's-own-primary-
/// key technique `routes::source_connections::list` already uses in this
/// codebase (no separate encode/decode step needed: the id itself carries no
/// information an authenticated, account-scoped caller doesn't already have
/// the right to see). Returns up to `limit` rows plus the next cursor
/// (`None` once the account's last page has been reached).
///
/// `saved_at` = `last_ingested_at`, not `created_at`: this table has no
/// `updated_at` column (`0186`'s own schema), and `last_ingested_at` is the
/// one of the two that actually moves on a re-save (`upsert_watched_thread`'s
/// `ON CONFLICT` arm bumps it) — `created_at` would leave a re-followed
/// thread stranded wherever it first appeared instead of surfacing near the
/// top of a "newest first" list, which is the wrong UX for a page whose
/// whole point is "what did I just save/re-save". [`set_follow`] does NOT
/// bump `last_ingested_at` (it only flips the flag), so this timestamp keeps
/// meaning "last content touch", not "last flag toggle".
pub(crate) async fn list_watched(
    content_pool: &PgPool,
    account_id: Uuid,
    limit: i64,
    cursor: Option<Uuid>,
) -> Result<(Vec<WatchedRow>, Option<Uuid>), ConnectorError> {
    let mut tx = scoped_tx(content_pool, account_id, "list_watched").await?;
    let mut rows: Vec<(Uuid, String, Uuid, bool, DateTime<Utc>)> = sqlx::query_as(
        r#"
        SELECT id, thread_id, graph_id, follow_replies, last_ingested_at
        FROM gmail_watched_threads
        WHERE account_id = $1
          AND (
            $3::uuid IS NULL
            OR (last_ingested_at, id) < (
                SELECT last_ingested_at, id FROM gmail_watched_threads
                WHERE account_id = $1 AND id = $3
            )
          )
        ORDER BY last_ingested_at DESC, id DESC
        LIMIT $2
        "#,
    )
    .bind(account_id)
    .bind(limit + 1)
    .bind(cursor)
    .fetch_all(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("list_watched: {e}")))?;
    tx.rollback().await.ok();
    // One lookahead row distinguishes a full final page from a page
    // that actually has more content; clients need no empty sentinel page.
    let has_more = rows.len() as i64 > limit;
    rows.truncate(limit as usize);
    let next_cursor = rows.last().map(|(id, ..)| *id).filter(|_| has_more);
    let out = rows
        .into_iter()
        .map(|(id, thread_id, graph_id, following, saved_at)| WatchedRow {
            id,
            thread_id,
            graph_id,
            following,
            saved_at,
        })
        .collect();
    Ok((out, next_cursor))
}

/// Best-effort display title for a watched thread — `atoms.title`
/// (`0097`/`0212`) of the atom whose `source_id` equals the thread's OWN id.
/// Cheap and index-backed (`UNIQUE (graph_id, source_type, source_id)`,
/// `0003`), never a live Gmail API round trip, because Gmail's own
/// documented id scheme makes this join meaningful without one: a thread's
/// `threadId` is the `id` of the FIRST message in it. So the message that
/// established the thread — if it was ever ingested by this account,
/// meaning `scope=thread` on the original save, or a `scope=message` save
/// that happened to target that specific message — has `atoms.source_id ==
/// thread_id`.
///
/// `None` (not an error) whenever that atom doesn't exist: a `scope=message`
/// save of a REPLY (the thread's first message was never fetched at all), or
/// the first message was later deleted/superseded. This is the documented
/// "best-effort... else null" case, not a bug — there is no other cheap
/// source for a title (atoms carry no `thread_id` column at all, so there is
/// no general thread → atom join; widening the schema for one is explicitly
/// left to a later PR if the founder wants a stronger guarantee than this
/// best-effort heuristic).
pub(crate) async fn best_effort_title(
    content_pool: &PgPool,
    account_id: Uuid,
    graph_id: Uuid,
    thread_id: &str,
) -> Option<String> {
    let mut tx = scoped_tx(content_pool, account_id, "best_effort_title").await.ok()?;
    let row: Option<(Option<String>,)> = sqlx::query_as(
        "SELECT title FROM atoms WHERE graph_id = $1 AND source_type = $2 AND source_id = $3",
    )
    .bind(graph_id)
    .bind(SOURCE_TYPE)
    .bind(thread_id)
    .fetch_optional(&mut *tx)
    .await
    .ok()?;
    tx.rollback().await.ok();
    row.and_then(|(t,)| t)
}

// ---------------------------------------------------------------------------
// Deletion propagation (decision 9)
// ---------------------------------------------------------------------------

/// Everything-mode fallback destination for [`propagate_deletions`] (gsuite
/// Train G2 PR 4): the owner's own `gmail_everything_mode.graph_id`, if a
/// row exists AT ALL for them — regardless of the row's current `enabled`
/// value.
///
/// **`enabled` deliberately does NOT gate this lookup** (adversarial review
/// fix, 2026-08-25 — an earlier version of this query added `AND enabled =
/// true`, reintroducing the exact retention gap this whole mechanism exists
/// to close): disabling everything mode (`routes::gmail_everything_mode::
/// set_everything_mode`) only flips the flag — it never purges the atoms
/// already ingested while it was on. If this lookup required `enabled =
/// true`, then enable → ingest 50k messages → disable → later delete one of
/// those messages upstream would silently retain it forever: `history.list`
/// reports a given deletion EXACTLY ONCE, `fetch_pending` would still commit
/// the advanced cursor regardless, and there would be no second chance to
/// ever propagate that specific deletion. `services::deletion::
/// delete_source`'s `origin_graph_id` argument only AUTHORIZES the account
/// (see this file's own `propagate_deletions` doc) — it never scopes which
/// atom gets deleted — so a stale (no-longer-enabled) row's `graph_id` is
/// exactly as sound an authorization anchor as a live one; there is no
/// safety reason to gate on `enabled` here, only a (wrong) intuition that
/// disabled should mean invisible.
async fn everything_mode_graph_id(
    content_pool: &PgPool,
    account_id: Uuid,
    owner_user_id: Uuid,
) -> Result<Option<Uuid>, ConnectorError> {
    let mut tx = scoped_tx(content_pool, account_id, "everything_mode_graph_id").await?;
    let row: Option<(Uuid,)> = sqlx::query_as(
        "SELECT graph_id FROM gmail_everything_mode WHERE account_id = $1 AND owner_user_id = $2",
    )
    .bind(account_id)
    .bind(owner_user_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("everything_mode_graph_id: {e}")))?;
    tx.rollback().await.ok();
    Ok(row.map(|(g,)| g))
}

/// The owner's CURRENT primary ocean — where `routes::google_push` lands a
/// live-follow arrival (`resolve_primary_graph`), which is not necessarily
/// the ocean the thread was saved into: a reply that arrived after the user
/// changed their primary sits in the new primary while the watched row still
/// names the old one. A read-only twin of `resolve_primary_graph`'s two arms
/// (the account's personal primary; the member's own non-archived `member`
/// primary) because a hide must never create an ocean. `None` when there is
/// no primary yet, in which case no live-follow arrival can exist either.
async fn owner_primary_graph_id(
    content_pool: &PgPool,
    account_id: Uuid,
    owner_user_id: Uuid,
) -> Result<Option<Uuid>, ConnectorError> {
    let mut tx = scoped_tx(content_pool, account_id, "owner_primary_graph_id").await?;
    let row: Option<(Uuid,)> = sqlx::query_as(
        "SELECT id FROM graphs \
         WHERE account_id = $1 AND is_primary = true AND archived_at IS NULL \
           AND (kind = 'personal' OR (kind = 'member' AND owner_user_id = $2)) \
         LIMIT 1",
    )
    .bind(account_id)
    .bind(owner_user_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("owner_primary_graph_id: {e}")))?;
    tx.rollback().await.ok();
    Ok(row.map(|(g,)| g))
}

/// Propagate every deleted `(thread_id, message_id)` to
/// `services::deletion::delete_source`, resolving each id's ORIGIN graph
/// (the `origin_graph_id` argument `delete_source` uses to resolve/
/// authorize the account — see the note below on what that argument does
/// and does NOT do) from `gmail_watched_threads.graph_id` — the ocean the
/// thread was actually ingested into — rather than re-deriving a graph via
/// `resolve_primary_graph`. Best-effort per id, same reasoning as
/// `google_calendar::propagate_deletions`.
///
/// # Why read `gmail_watched_threads.graph_id` instead of re-deriving
/// (CodeRabbit round 1 finding #2 on PR #686 — corrected in place; see the
/// pg test right below this function for how the original framing was
/// found to overstate the practical risk)
/// The original version resolved the deletion's origin graph the same way
/// [`after_ingest`] resolves a CREATE's destination
/// (`resolve_primary_graph(Personal, Attributable(owner_user_id))`).
/// CodeRabbit's original claim was that a re-derived graph diverging from
/// the thread's real one would make `delete_source` silently no-op — that
/// specific claim does NOT hold: `delete_source`
/// (`services::deletion::delete_source`, see its own doc, "Entry point 2:
/// source") deletes every atom ANYWHERE IN THE SAME ACCOUNT matching
/// `(source_type, source_id)` — its `origin_graph_id` argument only
/// resolves/authorizes WHICH ACCOUNT the call may touch
/// (`resolve_account_id_for_graph`), it never scopes which atom gets
/// deleted. Since `resolve_primary_graph`'s result is always a graph
/// belonging to the SAME account as `gmail_watched_threads.graph_id`, the
/// old code would have authorized the identical account and therefore
/// deleted the identical set of atoms — no data loss, contrary to the
/// original finding's framing.
///
/// Reading the row's own graph is still the right call, on narrower
/// grounds: it avoids a redundant `resolve_primary_graph` round trip per
/// deletion batch, it ties the deletion's authorization anchor to a row
/// already proven to exist and be reachable (rather than depending on a
/// SEPARATE resolution succeeding), and it stays correct without
/// re-verification if `delete_source`'s or `resolve_primary_graph`'s own
/// semantics ever diverge in the future (e.g. a later org-plane Gmail PR).
/// None of that is "fixes a silent retention failure" — it is "removes an
/// unnecessary dependency", and this doc says so accurately rather than
/// repeating the stronger claim.
///
/// # A thread absent from `gmail_watched_threads` is a correct no-op —
/// # UNLESS everything mode has EVER covered it (gsuite Train G2 PR 4)
/// A `messagesDeleted` entry whose thread was never opted into ingest (no
/// `gmail_watched_threads` row) has nothing to delete — there is no atom
/// anywhere in this account for a message CoralSwarm never ingested in the
/// first place — UNLESS this owner has a `gmail_everything_mode` row AT
/// ALL (regardless of its CURRENT `enabled` value — see
/// [`everything_mode_graph_id`]'s own doc for why gating on `enabled` here
/// would silently reopen the exact retention gap this mechanism exists to
/// close), in which case [`everything_mode_graph_id`] supplies the fallback
/// graph (an everything-mode create never registers a `gmail_watched_
/// threads` row, so this map alone cannot tell "never ingested" apart from
/// "ingested by the whole-mailbox pull instead"). Only once BOTH lookups
/// come up empty is this logged at `debug!` (routine, not a failure) and
/// skipped, exactly the outcome an unwatched thread's live-follow discard
/// ([`filter_watched`], ADR-0008) already produces on the create side.
/// Founder decision Q3 (`.scratch/gsuite-enterprise/plan.md`) requires ALL
/// deletions to propagate, including Gmail's own automatic Spam/Trash
/// 30-day purges — which report through this SAME mailbox-wide
/// `history.list` diff regardless of opt-in state, since the underlying
/// watch is mailbox-wide, not thread-scoped.
///
/// # Both planes are reachable; the org plane retracts graph-confined
/// This function's only production caller is [`fetch_pending`], whose only
/// production caller is `routes::google_push::run_gmail_sync_for_email`.
/// Until the org push lift that route was hardcoded Personal with the
/// consumer pool as both planes, so for an org member's mailbox the
/// `gmail_watched_threads`/`gmail_everything_mode` lookups here ran against
/// the wrong database, found nothing, and every deletion was a silent
/// "nothing to delete" while the cursor advanced. It now passes the
/// account's real content pool and `plane`. On `AccountPlane::Org` the
/// retraction is confined to the resolved graph ([`retract_in_graph_only`],
/// the same `services::deletion::delete_atoms_in_graph_only` half
/// `google_calendar::propagate_org_member_deletions` uses) rather than
/// `delete_source`'s account-wide natural-key expansion: an org tenant
/// holds every member's oceans, and the rule for org connectors is that
/// one member's upstream deletion never reaches beyond that member's own
/// ocean (a copy a human promoted into a shared ocean is an explicit
/// decision of its own, retracted through the ordinary routes — the same
/// residual Calendar's org path accepts). Gmail message ids are per
/// mailbox, so unlike Calendar there is no same-id collision between
/// members to guard; the confinement is about scope discipline, not
/// collision. The personal plane keeps `delete_source` (every copy,
/// promoted ones included) exactly as before.
///
/// # Returns `Result`, blocks the cursor on ANY failure (CodeRabbit round 2
/// finding #1 on PR #686, Major)
/// A round 1 review pass made this function correct about WHICH graph to
/// delete from, but left it swallowing every failure and always returning
/// `()` — so [`fetch_pending`]'s caller had no way to know a deletion never
/// actually landed. That is a real silent-retention bug, unlike round 1's
/// finding #2: if [`watched_thread_graph_ids`] hit a transient DB error, or
/// an individual `delete_source` call failed, the function logged a warning
/// and returned successfully anyway — [`fetch_pending`] would still commit
/// the ADVANCED `historyId`, and Gmail's `history.list` reports a given
/// deletion EXACTLY ONCE (unlike a re-listable full backfill), so that
/// specific deleted message would never be reported again and would stay
/// indexed in CoralSwarm forever, with no error anywhere. Now: the batch
/// lookup failure aborts immediately (nothing in this batch could be
/// resolved, so nothing is attempted); an individual `delete_source`
/// failure is logged and does NOT stop the rest of the batch from being
/// attempted (still best-effort per id, matching `google_calendar::
/// propagate_deletions`'s posture for the *attempt* side), but the function
/// remembers it and returns `Err` at the end regardless. Either way,
/// [`fetch_pending`] propagates the error via `?` — see its own call site —
/// so the cursor is left unchanged and the SAME `history.list` range is
/// re-diffed (and this deletion re-attempted) on the next pass, exactly the
/// "never advance past something not durably captured" discipline the
/// create side's commit-after-ingest split already enforces. `422`-style
/// consumed semantics deliberately do NOT apply here: unlike `ingest_atom`'s
/// typed validation rejection, `delete_source`'s failure carries no signal
/// that distinguishes "deterministic, retrying is pointless" from
/// "transient, must retry" — treating every failure as retriable is the
/// safe default, and `delete_source` is itself idempotent
/// (`google_calendar::propagate_deletions`'s own doc), so a spurious retry
/// of an already-successful deletion is harmless.
#[cfg(test)]
async fn propagate_deletions(
    content_pool: &PgPool,
    account_id: Uuid,
    owner_user_id: Uuid,
    plane: AccountPlane,
    deleted: &[(String, String)],
) -> Result<(), ConnectorError> {
    propagate_deletions_with_namespace(content_pool,account_id,owner_user_id,plane,deleted,None).await
}

async fn propagate_deletions_with_namespace(
    content_pool:&PgPool,account_id:Uuid,owner_user_id:Uuid,plane:AccountPlane,
    deleted:&[(String,String)],namespace:Option<&str>,
) -> Result<(),ConnectorError> {
    if deleted.is_empty() {
        return Ok(());
    }
    let thread_ids: Vec<String> = deleted.iter().map(|(t, _)| t.clone()).collect();
    let graphs = watched_thread_graph_ids(content_pool, account_id, owner_user_id, &thread_ids).await.map_err(|e| {
        tracing::warn!(error = %e, %account_id, "gmail: deletion propagation aborted — watched-thread graph lookup failed");
        e
    })?;

    // gsuite Train G2 PR 4 (everything mode): a thread absent from
    // `gmail_watched_threads` is ONLY a correct no-op when this owner has
    // never had everything mode ingest their mailbox either. `workers::
    // gmail_everything_mode` never registers a `gmail_watched_threads` row
    // for what it pulls (it does not track threads at all — see that
    // module's own doc), so an atom CAN exist for a thread this map does
    // not cover. Silently skipping such a deletion would violate founder
    // decision Q3 ("propagate ALL deletions", `.scratch/gsuite-enterprise/
    // plan.md`) — including Gmail's own automatic Spam/Trash 30-day
    // purges, which report through this SAME mailbox-wide `history.list`
    // diff regardless of opt-in state (the watch itself is mailbox-wide,
    // not thread-scoped — only WHICH creates get ingested is opt-in
    // scoped, not what the diff reports). Resolved ONCE per batch, not per
    // id: every everything-mode create for this owner lands in the SAME
    // graph.
    let everything_mode_fallback = everything_mode_graph_id(content_pool, account_id, owner_user_id)
        .await
        .map_err(|e| {
            tracing::warn!(error = %e, %account_id, %owner_user_id, "gmail: deletion propagation aborted — everything-mode graph lookup failed");
            e
        })?;

    let mut any_failed = false;
    for (thread_id, message_id) in deleted {
        let mut candidates = graphs.get(thread_id).cloned().unwrap_or_default();
        candidates.extend(everything_mode_fallback);
        match original_message_graph_ids(content_pool, account_id, owner_user_id, message_id).await {
            Ok(originals) => candidates.extend(originals),
            Err(e) => {
                tracing::warn!(error = %e, %account_id, %message_id, "gmail: original message destination lookup failed");
                any_failed = true;
                continue;
            }
        }
        candidates.sort_unstable();
        candidates.dedup();
        let Some(&graph_id) = candidates.first() else {
            tracing::debug!(
                %account_id, %thread_id, %message_id,
                "gmail: deletion for a never-opted-in, never-everything-mode-enabled thread — no \
                 atom exists to delete, skipping"
            );
            continue;
        };
        if let Some(namespace)=namespace {
            let destinations = if plane==AccountPlane::Org {candidates.clone()} else {vec![graph_id]};
            for graph_id in destinations {
                let result=if plane==AccountPlane::Org {
                    crate::services::deletion::delete_source_identity_in_graph(
                        content_pool,None,crate::store::CLOUD_DEVICE_ID,graph_id,SOURCE_TYPE,message_id,
                        Some(namespace),crate::services::connector_audit::Actor::System,None,
                    ).await
                } else {
                    crate::services::deletion::delete_chunked_source_in_namespace(
                        content_pool,None,crate::store::CLOUD_DEVICE_ID,graph_id,SOURCE_TYPE,message_id,
                        namespace,crate::services::connector_audit::Actor::System,None,
                    ).await
                };
                if let Err(e)=result {
                    tracing::warn!(error=%e,%graph_id,%message_id,"gmail: source evidence deletion failed");
                    any_failed=true;
                }
            }
            continue;
        }
        if plane == AccountPlane::Org {
            for graph_id in candidates {
                if let Err(e) = retract_in_graph_only(content_pool, account_id, graph_id, message_id).await {
                    tracing::warn!(error = %e, %account_id, %message_id, %graph_id, "gmail: org member message deletion propagation failed");
                    any_failed = true;
                }
            }
            continue;
        }
        // fix/gmail-personal-delete: `delete_source` matches `source_id`
        // EXACTLY, but every coral `routes::ingest` stores for a Gmail
        // message is a `"{message id}:chunk:N"` row (`ingest_atom`'s chunk
        // loop; `email_body::gmail_message_to_item` sets `source_id` to the
        // bare Gmail message id, never the thread id) — so passing the bare
        // id straight in, as this loop did until now, matched NOTHING in
        // production and the deleted message's corals stayed while this
        // function logged "propagated". The identical gap was closed for
        // Calendar in PR #1157; resolve the concrete stored ids first,
        // exactly as `google_calendar`/`google_chat::propagate_deletions`
        // do, then retract each one account-wide as before. Resolved per
        // message (not once per batch) so a lookup failure is attributed to
        // the one id it belongs to and the rest of the batch is still
        // attempted — the same best-effort-per-id posture as the delete
        // itself; the function still returns `Err` at the end so the
        // cursor is not advanced past it.
        let concrete = match crate::services::deletion::concrete_source_ids(
            content_pool,
            account_id,
            SOURCE_TYPE,
            std::slice::from_ref(message_id),
        )
        .await
        {
            Ok(ids) => ids,
            Err(e) => {
                tracing::warn!(error = %e, %account_id, %message_id, "gmail: concrete source id lookup failed for a deleted message");
                any_failed = true;
                continue;
            }
        };
        if concrete.is_empty() {
            // The thread was watched (or everything mode covered it) but
            // THIS message was never turned into a coral — e.g. its body
            // was empty and `gmail_message_to_item` skipped it, or it
            // arrived in a followed thread before `follow_since`. Nothing
            // to retract; not a failure.
            tracing::debug!(%account_id, %thread_id, %message_id, "gmail: deleted message has no stored corals, skipping");
            continue;
        }
        for source_id in &concrete {
            match crate::services::deletion::delete_source(
                content_pool,
                None,
                crate::store::CLOUD_DEVICE_ID,
                graph_id,
                SOURCE_TYPE,
                source_id,
                // Sync/push-driven propagation, no CoralSwarm user session —
                // system actor (m365 enterprise train PR 4.2), same reasoning as
                // `google_calendar::propagate_deletions`'s identical call site.
                crate::services::connector_audit::Actor::System,
                // FM-5 (B2 unit 02): no `retraction_audit_log` row for this
                // connector-propagated delete — scoped out of this PR
                // deliberately (see `services::deletion::delete_coral`'s
                // `control_pool: None` doc note). `connector_audit_log`'s own
                // `deletion_propagated` row above already records this action;
                // wiring a second, distinct control-plane pool through every
                // connector sync worker is real plumbing D5's four fixes did
                // not scope FM-5 to include.
                None,
            )
            .await
            {
                Ok(outcome) => tracing::info!(%account_id, %message_id, %source_id, %graph_id, ?outcome, "gmail: message deletion propagated"),
                Err(e) => {
                    tracing::warn!(error = %e, %account_id, %message_id, %source_id, %graph_id, "gmail: message deletion propagation failed");
                    any_failed = true;
                }
            }
        }
    }
    // A permanent deletion has nothing to restore — drop any tombstone a
    // hide left for these messages, whichever ocean it named.
    let message_ids: Vec<String> = deleted.iter().map(|(_, m)| m.clone()).collect();
    clear_hidden_messages_for(content_pool, account_id, &message_ids).await.map_err(|e| {
        tracing::warn!(error = %e, %account_id, "gmail: deletion propagation aborted — tombstone clear failed");
        e
    })?;
    if any_failed {
        return Err(ConnectorError::Internal(format!(
            "gmail: one or more deletions failed to propagate for account {account_id}"
        )));
    }
    Ok(())
}

/// The stored corals of `message_id` inside `graph_id` and nowhere else —
/// every `"{message id}:chunk:N"` row `routes::ingest` wrote there.
async fn message_atoms_in_graph(
    content_pool: &PgPool,
    account_id: Uuid,
    graph_id: Uuid,
    message_id: &str,
) -> Result<Vec<Uuid>, ConnectorError> {
    let pattern = crate::services::deletion::escaped_chunk_pattern(message_id);
    let mut tx = scoped_tx(content_pool, account_id, "message_atoms_in_graph").await?;
    let atom_ids: Vec<Uuid> = sqlx::query_scalar(
        "SELECT atom_id FROM atoms \
         WHERE graph_id = $1 AND source_type = $2 AND promoted_from_atom_id IS NULL \
           AND (source_id = $3 OR source_id LIKE $4)",
    )
    .bind(graph_id)
    .bind(SOURCE_TYPE)
    .bind(message_id)
    .bind(&pattern)
    .fetch_all(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("message_atoms_in_graph: {e}")))?;
    tx.rollback().await.ok();
    Ok(atom_ids)
}

/// Retract `atom_ids` — the corals [`message_atoms_in_graph`] found for
/// `message_id` in `graph_id` — and nothing else: `services::deletion::
/// delete_atoms_in_graph_only`, no account-wide source-identity expansion to
/// promoted copies, with the same `deletion_propagated` compliance row
/// `delete_source` writes. Mirrors `google_calendar::
/// propagate_org_member_deletions`.
async fn retract_atoms_in_graph_only(
    content_pool: &PgPool,
    account_id: Uuid,
    graph_id: Uuid,
    message_id: &str,
    atom_ids: &[Uuid],
) -> Result<(), ConnectorError> {
    let outcome = crate::services::deletion::delete_atoms_in_graph_only(
        content_pool,
        None,
        crate::store::CLOUD_DEVICE_ID,
        graph_id,
        atom_ids,
        crate::services::connector_audit::Actor::System,
        None,
    )
    .await
    .map_err(|e| ConnectorError::Internal(format!("retract_atoms_in_graph_only: {e}")))?;
    crate::services::connector_audit::record_or_queue(
        content_pool,
        crate::services::connector_audit::AuditEntry {
            account_id,
            actor: crate::services::connector_audit::Actor::System,
            action: "deletion_propagated",
            provider: Some(crate::services::connector_audit::provider_from_source_type(SOURCE_TYPE).to_string()),
            subject: Some(message_id.to_string()),
            detail: Some(serde_json::json!({
                "atoms_deleted": outcome.atoms_deleted.len(),
                "reefs_deleted": outcome.deleted_reef_ids.len(),
                "graphs_touched": outcome.propagated_graph_ids.len(),
            })),
        },
    )
    .await;
    tracing::info!(%account_id, %message_id, %graph_id, ?outcome, "gmail: message retracted from its ocean");
    Ok(())
}

/// The org-plane half of [`propagate_deletions`]: retract every chunk of
/// `message_id` that lives in `graph_id` (one member's ocean on the org
/// tenant pool) and nothing else — [`message_atoms_in_graph`] +
/// [`retract_atoms_in_graph_only`], the same two steps a hide takes minus
/// the tombstone. No stored chunk is a `debug!` no-op, same as the personal
/// arm's empty `concrete_source_ids`.
async fn retract_in_graph_only(
    content_pool: &PgPool,
    account_id: Uuid,
    graph_id: Uuid,
    message_id: &str,
) -> Result<(), ConnectorError> {
    let atom_ids = message_atoms_in_graph(content_pool, account_id, graph_id, message_id).await?;
    if atom_ids.is_empty() {
        tracing::debug!(%account_id, %graph_id, %message_id, "gmail: deleted message has no stored corals in the member's ocean, skipping");
        return Ok(());
    }
    retract_atoms_in_graph_only(content_pool, account_id, graph_id, message_id, &atom_ids).await
}

/// Retract every message moved to Trash/Spam — reversibly. Unlike
/// [`propagate_deletions`] this is graph-confined on BOTH planes: a hide
/// takes the connector's own copy out of each ocean it put it in (the
/// watched thread's `graph_id`, the everything-mode ocean, the owner's
/// current primary where live-follow replies land — see
/// [`owner_primary_graph_id`] — or several of those when they differ) and
/// never a copy a human promoted into another ocean. On the personal plane
/// `delete_source`'s account-wide expansion destroyed those promoted copies
/// on trash, and the restore — which only re-ingests what was retracted —
/// could never bring them back; an undo must not be lossy.
///
/// Every coral actually retracted is tombstoned first
/// ([`upsert_hidden_message`]), and only those: a message with no stored
/// coral in a candidate ocean (an unfollowed thread's reply, a
/// pre-`follow_since` sibling of a message-scope save, an empty body) leaves
/// no tombstone, so the restore cannot ingest it either. Same per-id
/// best-effort / `Err` at the end contract as [`propagate_deletions`], for
/// the same cursor reasons; a replayed hide finds nothing to retract and is
/// a no-op.
#[cfg(test)]
pub(crate) async fn propagate_hides(
    content_pool: &PgPool,
    account_id: Uuid,
    owner_user_id: Uuid,
    hidden: &[(String, String)],
) -> Result<(), ConnectorError> {
    propagate_hides_with_namespace(content_pool,account_id,owner_user_id,hidden,None).await
}

async fn propagate_hides_with_namespace(
    content_pool:&PgPool,account_id:Uuid,owner_user_id:Uuid,
    hidden:&[(String,String)],namespace:Option<&str>,
) -> Result<(),ConnectorError> {
    if hidden.is_empty() {
        return Ok(());
    }
    let thread_ids: Vec<String> = hidden.iter().map(|(t, _)| t.clone()).collect();
    let graphs = watched_thread_graph_ids(content_pool, account_id, owner_user_id, &thread_ids).await.map_err(|e| {
        tracing::warn!(error = %e, %account_id, "gmail: hide propagation aborted — watched-thread graph lookup failed");
        e
    })?;
    let everything_mode_graph = everything_mode_graph_id(content_pool, account_id, owner_user_id)
        .await
        .map_err(|e| {
            tracing::warn!(error = %e, %account_id, %owner_user_id, "gmail: hide propagation aborted — everything-mode graph lookup failed");
            e
        })?;
    let primary_graph = owner_primary_graph_id(content_pool, account_id, owner_user_id).await.map_err(|e| {
        tracing::warn!(error = %e, %account_id, %owner_user_id, "gmail: hide propagation aborted — primary graph lookup failed");
        e
    })?;

    let mut any_failed = false;
    for (thread_id, message_id) in hidden {
        let mut candidates = graphs.get(thread_id).cloned().unwrap_or_default();
        candidates.extend(everything_mode_graph);
        candidates.extend(primary_graph);
        match original_message_graph_ids(content_pool, account_id, owner_user_id, message_id).await {
            Ok(originals) => candidates.extend(originals),
            Err(e) => {
                tracing::warn!(error = %e, %account_id, %message_id, "gmail: original message destination lookup failed");
                any_failed = true;
                continue;
            }
        }
        candidates.sort_unstable();
        candidates.dedup();
        if candidates.is_empty() {
            tracing::debug!(%account_id, %thread_id, %message_id, "gmail: hide for a never-opted-in thread — nothing to retract, skipping");
            continue;
        }
        for graph_id in candidates {
            if let Some(namespace)=namespace {
                let mut tx=scoped_tx(content_pool,account_id,"hidden source evidence").await?;
                let present:bool=sqlx::query_scalar(
                    "SELECT EXISTS(SELECT 1 FROM source_items WHERE graph_id=$1 AND source_type=$2 AND source_id=$3 AND provider_namespace=$4) \
                     OR (EXISTS(SELECT 1 FROM atoms WHERE graph_id=$1 AND source_type=$2 AND (source_id=$3 OR source_id LIKE $5)) \
                         AND NOT EXISTS(SELECT 1 FROM source_items WHERE graph_id=$1 AND source_type=$2 AND source_id=$3 AND provider_namespace<>$4))",
                ).bind(graph_id).bind(SOURCE_TYPE).bind(message_id).bind(namespace)
                    .bind(crate::services::deletion::escaped_chunk_pattern(message_id)).fetch_one(&mut *tx).await
                    .map_err(|e|ConnectorError::Internal(e.to_string()))?;
                tx.rollback().await.ok();
                if !present {continue;}
                upsert_hidden_message(content_pool,account_id,owner_user_id,graph_id,thread_id,message_id).await?;
                if let Err(e)=crate::services::deletion::delete_source_identity_in_graph(
                    content_pool,None,crate::store::CLOUD_DEVICE_ID,graph_id,SOURCE_TYPE,message_id,
                    Some(namespace),crate::services::connector_audit::Actor::System,None,
                ).await {
                    tracing::warn!(error=%e,%graph_id,%message_id,"gmail: hidden source evidence retraction failed");
                    any_failed=true;
                }
                continue;
            }
            let atom_ids = match message_atoms_in_graph(content_pool, account_id, graph_id, message_id).await {
                Ok(ids) => ids,
                Err(e) => {
                    tracing::warn!(error = %e, %account_id, %message_id, %graph_id, "gmail: coral lookup failed for a hidden message");
                    any_failed = true;
                    continue;
                }
            };
            if atom_ids.is_empty() {
                tracing::debug!(%account_id, %thread_id, %message_id, %graph_id, "gmail: hidden message has no stored corals in this ocean, skipping");
                continue;
            }
            if let Err(e) =
                upsert_hidden_message(content_pool, account_id, owner_user_id, graph_id, thread_id, message_id).await
            {
                tracing::warn!(error = %e, %account_id, %message_id, %graph_id, "gmail: tombstone write failed — hidden message left in place");
                any_failed = true;
                continue;
            }
            if let Err(e) = retract_atoms_in_graph_only(content_pool, account_id, graph_id, message_id, &atom_ids).await {
                tracing::warn!(error = %e, %account_id, %message_id, %graph_id, "gmail: hidden message retraction failed");
                any_failed = true;
            }
        }
    }
    if any_failed {
        return Err(ConnectorError::Internal(format!(
            "gmail: one or more hides failed to propagate for account {account_id}"
        )));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Opt-in per-thread fetch (item 3) — `GoogleConnector::fetch_items`
// ---------------------------------------------------------------------------

/// `messages.get(format=full)`, with `404` as `Ok(None)`: a message named by
/// `history.list` can be permanently gone by the time it is fetched (deleted
/// after the pass's history range ended, or an add-then-delete split across
/// a budget boundary). The live-follow pass must treat that as "nothing to
/// ingest" — propagating it as an error would leave the cursor parked on
/// that range forever, since the next pass re-diffs the same range and 404s
/// the same way.
async fn fetch_message_if_present(
    http: &reqwest::Client,
    access_token: &str,
    id: &str,
    pace_key: Option<&str>,
) -> Result<Option<GmailMessage>, ConnectorError> {
    let url = format!("{}/{}?format=full", messages_url(), urlencoding::encode(id));
    let resp = super::google::google_send_with_pacing(
        || http.get(super::google_transport::endpoint(&url)).bearer_auth(access_token),
        || async {
            if let Some(key) = pace_key {
                pace_mailbox(key, GET_INTERVAL).await;
            }
        },
    ).await?;
    if resp.status() == reqwest::StatusCode::NOT_FOUND {
        return Ok(None);
    }
    if !resp.status().is_success() {
        return Err(google_api_error("gmail messages.get", resp).await);
    }
    resp.json()
        .await
        .map(Some)
        .map_err(|e| ConnectorError::Exchange(format!("messages.get parse: {e}")))
}

async fn fetch_push_message(
    http: &reqwest::Client,
    access_token: &str,
    pace_key: &str,
    id: &str,
) -> Result<Option<GmailMessage>, ConnectorError> {
    fetch_message_if_present(http, access_token, id, Some(pace_key)).await
}

/// Fetch and gate one live-follow arrival: nothing when it is gone (404),
/// currently in Trash/Spam (it arrived straight into Spam, or was hidden
/// after this pass's range — a later `labelsRemoved` brings it back through
/// these same gates), or before its thread's `follow_since` boundary.
async fn live_follow_item(
    http: &reqwest::Client,
    access_token: &str,
    pace_key: &str,
    thread_id: &str,
    message_id: &str,
    boundaries: &std::collections::HashMap<String, Option<DateTime<Utc>>>,
) -> Result<Option<NormalizedItem>, ConnectorError> {
    let Some(msg) = fetch_push_message(http, access_token, pace_key, message_id).await? else {
        return Ok(None);
    };
    if message_is_hidden(&msg) {
        return Ok(None);
    }
    let Some(boundary) = boundaries.get(thread_id) else {
        return Ok(None);
    };
    if !gmail_message_is_future_reply(&msg, *boundary) {
        return Ok(None);
    }
    Ok(gmail_message_to_item(&msg))
}

/// [`fetch_message_if_present`] for callers that named the message
/// themselves (an explicit opt-in save, the everything-mode listing) — there
/// a `404` is a real failure to surface, not a benign race.
async fn fetch_message_full(
    http: &reqwest::Client,
    access_token: &str,
    id: &str,
) -> Result<GmailMessage, ConnectorError> {
    fetch_message_if_present(http, access_token, id, None)
        .await?
        .ok_or_else(|| ConnectorError::upstream(reqwest::StatusCode::NOT_FOUND, "gmail messages.get failed: 404 Not Found"))
}

#[derive(Debug, Deserialize)]
struct GThreadResponse {
    #[serde(default)]
    messages: Vec<GmailMessage>,
}

async fn fetch_thread_full(
    http: &reqwest::Client,
    access_token: &str,
    id: &str,
) -> Result<GThreadResponse, ConnectorError> {
    let url = format!("{THREADS_URL}/{}?format=full", urlencoding::encode(id));
    let resp = google_send(|| http.get(super::google_transport::endpoint(&url)).bearer_auth(access_token)).await?;
    if !resp.status().is_success() {
        return Err(google_api_error("gmail threads.get", resp).await);
    }
    resp.json()
        .await
        .map_err(|e| ConnectorError::Exchange(format!("threads.get parse: {e}")))
}

/// Fetch each opened message, then — unless `scope` says otherwise — expand
/// to its whole thread (`threads.get(format=full)`) — the Gmail analogue of
/// `microsoft::MicrosoftConnector::fetch_items`, ported to this module so
/// `GoogleConnector::fetch_items` stays a thin delegation. Dedup by message
/// id AND thread id (mirrors Microsoft's `seen_messages`/
/// `seen_conversations` exactly).
///
/// `scope` (gmail-options backend train, PR 1 of 4): [`IngestScope::Thread`]
/// (the default) is this function's original, only behavior — fetch the
/// opened message, then expand. [`IngestScope::Message`] skips the
/// `threads.get` expansion entirely — only the named item id(s) are fetched
/// and returned. Either way each returned [`NormalizedItem`] still carries
/// its own `thread_id` (`gmail_message_to_item` reads it straight off the
/// message, not off the expansion) — the caller's `after_ingest` needs it to
/// register the watched-thread row regardless of scope; see that function's
/// own doc for why a message-scope row is still marked backfill-complete.
pub(crate) async fn fetch_items(
    http: &reqwest::Client,
    access_token: &str,
    item_ids: &[String],
    scope: IngestScope,
) -> Result<Vec<NormalizedItem>, ConnectorError> {
    let mut seen_threads: HashSet<String> = HashSet::new();
    let mut seen_messages: HashSet<String> = HashSet::new();
    let mut items = Vec::new();

    for id in item_ids {
        let msg = fetch_message_full(http, access_token, id).await?;
        let thread_id = msg.thread_id.clone();
        if seen_messages.insert(msg.id.clone()) {
            if let Some(item) = gmail_message_to_item(&msg) {
                items.push(item);
            }
        }
        if scope == IngestScope::Thread
            && !thread_id.trim().is_empty()
            && seen_threads.insert(thread_id.clone())
        {
            let thread = fetch_thread_full(http, access_token, &thread_id).await?;
            for m in thread.messages {
                // `threads.get` returns the thread's trashed/spam siblings
                // too; only the explicitly opened message (handled above)
                // may enter the ocean while hidden.
                if message_is_hidden(&m) {
                    continue;
                }
                if seen_messages.insert(m.id.clone()) {
                    if let Some(item) = gmail_message_to_item(&m) {
                        items.push(item);
                    }
                }
            }
        }
    }
    Ok(items)
}

/// Post-ingest side effect for the opt-in path: register every distinct
/// thread just ingested in `gmail_watched_threads` (a REAL failure here
/// propagates — losing the watched-thread row is a correctness problem, not
/// an infra hiccup), then best-effort ensure a live watch exists for this
/// mailbox (self-heal — see [`ensure_watch`]'s doc).
///
/// `ensure_watch`'s own failure is caught and logged HERE, not propagated
/// (CodeRabbit round 1 finding #9 on PR #686, docs correction that surfaced
/// a real inconsistency): [`fetch_pending`]'s own best-effort `ensure_watch`
/// call already treats it as non-fatal, and `microsoft::
/// MicrosoftConnector::after_ingest`'s own subscription-bootstrap call
/// (`ensure_mail_subscriptions`) does too — it swallows every per-resource
/// failure internally and always returns `Ok(())`. An earlier version of
/// this function let `ensure_watch`'s error propagate all the way out,
/// which the generic caller (`routes::connectors::finish_ingest`) still
/// caught and logged as non-fatal — so the END-TO-END behavior was correct
/// — but that made THIS function's own posture inconsistent with its two
/// direct precedents for no reason. Matching them here, not documenting the
/// old propagate-and-rely-on-the-caller behavior, is the fix.
/// `control_pool: None` (gsuite Train G2 PR 7): the caller couldn't resolve
/// a control-plane pool (`state.control_db()` returned `None` — a
/// misconfigured/no-DB deploy) — the watched-thread registration below still
/// runs (it's content-plane, unaffected), but the watch self-heal is skipped
/// entirely rather than attempted against the wrong pool. Logged, non-fatal,
/// same posture `routes::connectors::finish_ingest`'s own "no control pool
/// configured" token-refresh warning already takes.
///
/// `follow` (gmail-options backend train, PR 1 of 4): written onto every
/// upserted row's `follow_replies` column, unconditionally — a watched row
/// is written for EVERY opt-in save regardless of `follow`'s value (locked
/// decision, see `routes::connectors::ingest_selected`'s own doc); only
/// whether it goes on to drive live-follow depends on this value.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn after_ingest(
    http: &reqwest::Client,
    control_pool: Option<&PgPool>,
    content_pool: &PgPool,
    account_id: Uuid,
    owner_user_id: Uuid,
    email: &str,
    access_token: &str,
    graph_id: Uuid,
    items: &[NormalizedItem],
    follow: bool,
    follow_since: Option<DateTime<Utc>>,
) -> Result<(), ConnectorError> {
    let thread_ids: HashSet<&str> = items.iter().filter_map(|i| i.thread_id.as_deref()).collect();
    if thread_ids.is_empty() {
        return Ok(());
    }
    for thread_id in thread_ids {
        upsert_watched_thread(content_pool, account_id, graph_id, owner_user_id, thread_id, follow, follow_since).await?;
    }
    match control_pool {
        Some(control_pool) => {
            if let Err(e) = ensure_watch(http, control_pool, account_id, owner_user_id, email, access_token).await {
                tracing::warn!(
                    error = %e, %account_id, %owner_user_id,
                    "gmail: ensure_watch failed during after_ingest (non-fatal — the next opt-in \
                     ingest, push, or renewal tick retries)"
                );
            }
        }
        None => tracing::warn!(
            %account_id, %owner_user_id,
            "gmail: after_ingest has no control-plane pool available — skipping watch self-heal \
             (non-fatal — the next opt-in ingest, push, or renewal tick retries)"
        ),
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Everything mode (item 4, gsuite Train G2 PR 4) — whole-mailbox pull
// ---------------------------------------------------------------------------

/// Ids per `users.messages.list` request during everything mode's walk.
/// Gmail's documented max. A list call costs the same quota (5 units)
/// whatever its size, and returns only bare `{id, threadId}` pairs, so the
/// biggest page is the cheapest way to cover the mailbox: the expensive
/// part is the per-id `messages.get`, which the worker
/// (`workers::gmail_everything_mode`) skips for ids that already have
/// corals and caps per tick.
pub(crate) const EVERYTHING_MODE_LIST_PAGE_SIZE: usize = 500;

#[derive(Debug, Deserialize)]
struct GMessageListEntry {
    #[serde(default)]
    id: String,
}

#[derive(Debug, Deserialize)]
struct GMessageListResponse {
    #[serde(default)]
    messages: Vec<GMessageListEntry>,
    #[serde(default, rename = "nextPageToken")]
    next_page_token: Option<String>,
}

/// One `users.messages.list` call of everything mode's walk.
pub(crate) enum EverythingListOutcome {
    /// Message ids on this page, newest first, and the token for the next
    /// page (`None` = the listing is exhausted).
    Page {
        ids: Vec<String>,
        next_page_token: Option<String>,
    },
    /// The `pageToken` we sent was rejected as invalid/expired — Gmail
    /// answers `400` (`invalid`/`INVALID_ARGUMENT`) or `404`. Only ever
    /// returned when a token WAS sent. The caller restarts the walk. Carries
    /// Google's bounded error detail.
    StaleToken(String),
    /// Google's per-user/per-project rate limit (`429`, or `403` with a
    /// rate-limit reason — `google::google_error_is_rate_limit`). Retry
    /// later from the SAME place; never a restart. Before 2026-10 this was
    /// folded into `StaleToken` (any 4xx with a pageToken), so a quota 403
    /// restarted the walk from the newest message and threw away the tick.
    RateLimited(String),
}

/// One page of the everything-mode listing. `after`: list only messages
/// Gmail dates after this instant (`q=after:<epoch seconds>`, the
/// documented second-precision form of the search operator) — the walk's
/// high-water mark, so a steady-state walk lists only recent mail rather
/// than the whole mailbox. `includeSpamTrash` stays at its default (false).
pub(crate) async fn list_everything_mode_page(
    http: &reqwest::Client,
    access_token: &str,
    page_token: Option<&str>,
    after: Option<DateTime<Utc>>,
) -> Result<EverythingListOutcome, ConnectorError> {
    let mut url = format!("{}?maxResults={EVERYTHING_MODE_LIST_PAGE_SIZE}", messages_url());
    if let Some(after) = after {
        url.push_str("&q=");
        url.push_str(&urlencoding::encode(&format!("after:{}", after.timestamp())));
    }
    if let Some(pt) = page_token {
        url.push_str("&pageToken=");
        url.push_str(&urlencoding::encode(pt));
    }
    let resp = google_send(|| http.get(super::google_transport::endpoint(&url)).bearer_auth(access_token)).await?;
    if resp.status().is_success() {
        let page: GMessageListResponse = resp
            .json()
            .await
            .map_err(|e| ConnectorError::Exchange(format!("messages.list parse: {e}")))?;
        let mut ids = Vec::with_capacity(page.messages.len());
        for entry in page.messages {
            let id = entry.id.trim();
            if id.is_empty() {
                tracing::warn!("gmail everything-mode: messages.list entry with missing/empty id skipped");
                continue;
            }
            ids.push(id.to_string());
        }
        return Ok(EverythingListOutcome::Page {
            ids,
            next_page_token: page.next_page_token.filter(|t| !t.is_empty()),
        });
    }
    let failure = super::google::google_api_failure("gmail messages.list", resp).await;
    if failure.rate_limited {
        return Ok(EverythingListOutcome::RateLimited(failure.message));
    }
    // A stale/invalid pageToken is only meaningful when we SENT one, and
    // only for the codes Gmail uses for it. A token-less 4xx, a 401, or a
    // non-rate-limit 403 (scope, disabled API, policy) is a real failure.
    if page_token.is_some()
        && matches!(
            failure.status,
            reqwest::StatusCode::BAD_REQUEST | reqwest::StatusCode::NOT_FOUND
        )
    {
        return Ok(EverythingListOutcome::StaleToken(failure.message));
    }
    Err(ConnectorError::upstream(failure.status, failure.message))
}

/// One `users.messages.get(format=full)` of everything mode's walk.
pub(crate) enum EverythingGetOutcome {
    Item(Box<NormalizedItem>),
    /// Nothing to ingest NOW: gone between list and get (`404`), or hidden
    /// (Trash/Spam). Hidden mail can be restored and rejoin the listing, so
    /// this is not permanent.
    Gone,
    /// Nothing ingestable in the message (empty body once normalized, no
    /// usable date or id). A message's content never changes, so this is
    /// permanent for it under the current code.
    NoBody,
    /// Same as [`EverythingListOutcome::RateLimited`].
    RateLimited(String),
    /// Gmail answered `200` with a body that does not parse as a message —
    /// permanent for this message, so the caller skips it (and says so)
    /// rather than retrying the page forever.
    Unreadable(String),
}

pub(crate) async fn get_everything_mode_message(
    http: &reqwest::Client,
    access_token: &str,
    id: &str,
) -> Result<EverythingGetOutcome, ConnectorError> {
    let url = format!("{}/{}?format=full", messages_url(), urlencoding::encode(id));
    let resp = google_send(|| http.get(super::google_transport::endpoint(&url)).bearer_auth(access_token)).await?;
    if resp.status() == reqwest::StatusCode::NOT_FOUND {
        return Ok(EverythingGetOutcome::Gone);
    }
    if !resp.status().is_success() {
        let failure = super::google::google_api_failure("gmail messages.get", resp).await;
        if failure.rate_limited {
            return Ok(EverythingGetOutcome::RateLimited(failure.message));
        }
        return Err(ConnectorError::upstream(failure.status, failure.message));
    }
    // Read the body first: a failure READING it (connection dropped) is
    // transient and must retry; a body that will not PARSE is permanent.
    let bytes = resp
        .bytes()
        .await
        .map_err(|e| ConnectorError::Exchange(format!("messages.get body: {e}")))?;
    let msg: GmailMessage = match serde_json::from_slice(&bytes) {
        Ok(msg) => msg,
        Err(e) => return Ok(EverythingGetOutcome::Unreadable(format!("messages.get parse: {e}"))),
    };
    // The listing already omits Trash/Spam; this only catches a message
    // hidden between the list and the get.
    if message_is_hidden(&msg) {
        return Ok(EverythingGetOutcome::Gone);
    }
    Ok(match gmail_message_to_item(&msg) {
        Some(item) => EverythingGetOutcome::Item(Box::new(item)),
        None => EverythingGetOutcome::NoBody,
    })
}

// ---------------------------------------------------------------------------
// Live-follow: fetch/commit (push-triggered, item 2)
// ---------------------------------------------------------------------------

/// What [`fetch_pending`] found, deferred rather than persisted immediately
/// — see `google_calendar::PendingSync`'s doc for the full commit-after-
/// ingest rationale this mirrors byte-for-byte.
pub(crate) struct PendingSync {
    pub(crate) items: Vec<PendingItem>,
    pub(crate) commit: PendingCommit,
}

/// One item to ingest. `restore_graph_id` is `Some` for a message a hide
/// tombstoned ([`hidden_messages_for`]): it goes back into THAT ocean, not
/// the caller's primary one, and the caller clears the tombstone
/// ([`clear_hidden_message`]) once the ingest succeeded. `None` for a
/// live-follow arrival.
pub(crate) struct PendingItem {
    pub(crate) item: NormalizedItem,
    pub(crate) restore_graph_id: Option<Uuid>,
}

pub(crate) enum PendingCommit {
    Complete(String),
    Progress(Option<String>),
}

pub(crate) async fn commit_pending(
    control_pool: &PgPool,
    account_id: Uuid,
    email: &str,
    commit: PendingCommit,
) -> Result<(), ConnectorError> {
    match commit {
        PendingCommit::Complete(history_id) => {
            persist_history_complete(control_pool, account_id, email, &history_id).await
        }
        PendingCommit::Progress(resume) => {
            persist_history_progress(control_pool, account_id, email, resume.as_deref()).await
        }
    }
}

/// Fetch one incremental pass (restarting once on a 404-stale cursor)
/// WITHOUT persisting the resulting cursor — see [`PendingSync`]/
/// [`commit_pending`]. Deletions are applied eagerly (idempotent, safe to
/// retry — same reasoning `google_calendar::fetch_pending`'s doc gives);
/// creates are returned as [`PendingSync::items`] for the caller to ingest
/// and only then commit.
///
/// Per-message content fetch errors (network/parse — NOT a deterministic
/// poison pill) propagate via `?`, leaving the cursor unchanged so the next
/// pass re-diffs the same range: a Gmail message, once reported by
/// `history.list`, is reported EXACTLY ONCE by a future incremental diff
/// (unlike a re-listable full backfill), so silently dropping one on a
/// transient fetch failure would lose it forever — the same "never advance
/// past something not durably captured" discipline the commit-after-ingest
/// split protects for the create path generally.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn fetch_pending(
    http: &reqwest::Client,
    control_pool: &PgPool,
    content_pool: &PgPool,
    account_id: Uuid,
    owner_user_id: Uuid,
    plane: AccountPlane,
    email: &str,
    access_token: &str,
) -> Result<PendingSync, ConnectorError> {
    // Best-effort watch bootstrap — see module doc / ensure_watch's own doc.
    if let Err(e) = ensure_watch(http, control_pool, account_id, owner_user_id, email, access_token).await {
        tracing::warn!(error = %e, %account_id, %owner_user_id, "gmail: ensure_watch failed during fetch (non-fatal)");
    }

    let state = load_sync_state(control_pool, account_id, email).await?;
    if state.history_id.is_empty() {
        // No baseline yet — ensure_watch above has never succeeded for this
        // mailbox. Nothing to diff against; the next pass (after a
        // successful watch bootstrap) picks up from whatever baseline that
        // watch call seeds.
        tracing::info!(%account_id, %owner_user_id, "gmail: no history baseline yet — skipping this pass");
        return Ok(PendingSync {
            items: Vec::new(),
            commit: PendingCommit::Progress(None),
        });
    }

    // Gets share the Google user's pacer with everything mode's walk.
    let pace_key = rate_key(account_id, email);
    let mut start_history_id = state.history_id;
    let mut resume_page_token = state.backfill_page_token;
    let mut restarted_once = false;

    loop {
        match run_pass(http, access_token, &start_history_id, resume_page_token.clone(), SYNC_PASS_BUDGET).await? {
            PassOutcome::TooOld => {
                if restarted_once {
                    return Err(ConnectorError::Exchange(
                        "gmail: history cursor stale again immediately after a full restart".into(),
                    ));
                }
                tracing::info!(%account_id, %owner_user_id, "gmail: startHistoryId too old (404) — re-seeding baseline");
                let fresh = fetch_current_history_id(http, access_token).await?;
                restart_history_cursor(control_pool, account_id, email, &fresh).await?;
                start_history_id = fresh;
                resume_page_token = None;
                restarted_once = true;
                continue;
            }
            PassOutcome::Progress(result) => {
                let NetHistory { added, restored, hidden, deleted } = result.net;
                // A restored message no hide tombstoned is a new arrival and
                // takes the live-follow gates like one, so the watched and
                // boundary lookups cover both buckets.
                let thread_ids: Vec<String> =
                    added.iter().chain(restored.iter()).map(|(t, _)| t.clone()).collect();
                let watched = watched_thread_ids_among(content_pool, account_id, &thread_ids).await?;
                let boundaries = watched_thread_follow_since(content_pool, account_id, &thread_ids).await?;
                let kept = filter_watched(added, &watched);
                let restored_ids: Vec<String> = restored.iter().map(|(_, m)| m.clone()).collect();
                let tombstones = hidden_messages_for(content_pool, account_id, owner_user_id, &restored_ids).await?;

                let mut seen = HashSet::new();
                let mut items = Vec::new();
                for (thread_id, message_id) in kept {
                    if !seen.insert(message_id.clone()) {
                        continue;
                    }
                    if let Some(item) = live_follow_item(http, access_token, &pace_key, &thread_id, &message_id, &boundaries).await? {
                        items.push(PendingItem { item, restore_graph_id: None });
                    }
                }
                for (thread_id, message_id) in restored {
                    if !seen.insert(message_id.clone()) {
                        continue;
                    }
                    let Some(graph_ids) = tombstones.get(&message_id) else {
                        // Never retracted by a hide — it landed straight in
                        // Spam, or was trashed before anything admitted it —
                        // so it is gated exactly like a `messagesAdded`.
                        if !watched.contains(&thread_id) {
                            continue;
                        }
                        if let Some(item) = live_follow_item(http, access_token, &pace_key, &thread_id, &message_id, &boundaries).await? {
                            items.push(PendingItem { item, restore_graph_id: None });
                        }
                        continue;
                    };
                    let Some(msg) = fetch_push_message(http, access_token, &pace_key, &message_id).await? else { continue; };
                    // Hidden again after this pass's range: the tombstone
                    // stays for the next `labelsRemoved`.
                    if message_is_hidden(&msg) { continue; }
                    if let Some(item) = gmail_message_to_item(&msg) {
                        for graph_id in graph_ids {
                            items.push(PendingItem { item: item.clone(), restore_graph_id: Some(*graph_id) });
                        }
                    }
                }

                // `?` — a retraction failure must block the cursor exactly
                // like a failed create does (CodeRabbit round 2 finding #1
                // on PR #686); see `propagate_deletions`' own doc for the
                // full reasoning.
                propagate_hides_with_namespace(content_pool, account_id, owner_user_id, &hidden,Some(&pace_key)).await?;
                propagate_deletions_with_namespace(content_pool, account_id, owner_user_id, plane, &deleted,Some(&pace_key)).await?;

                let commit = match result.new_history_id {
                    Some(new_id) => PendingCommit::Complete(new_id),
                    None => PendingCommit::Progress(result.resume_page_token),
                };
                return Ok(PendingSync { items, commit });
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests — pure shaping/pagination logic (no network, no DB). DB-touching
// paths are pg-gated below, mirroring `google_calendar`'s own
// `ORGT_TEST_BASE_URL` convention.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// A local `messages.get` that answers every id `404` (the push path's
    /// "gone" case, so no body has to parse), counting requests.
    async fn spawn_gone_messages_server(hits: std::sync::Arc<std::sync::atomic::AtomicUsize>) -> String {
        let app = axum::Router::new().fallback(move || {
            let hits = hits.clone();
            async move {
                hits.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                axum::http::StatusCode::NOT_FOUND
            }
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("http://{addr}/messages")
    }

    #[tokio::test]
    async fn push_gets_share_the_google_users_pacer_with_a_walk_and_wait_one_slot() {
        use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
        use std::time::Duration;
        use tokio::time::Instant;
        let hits = std::sync::Arc::new(AtomicUsize::new(0));
        let url = spawn_gone_messages_server(hits.clone()).await;
        let http = reqwest::Client::new();
        let key = rate_key(Uuid::new_v4(), &format!("{}@pace.test", Uuid::new_v4()));
        let slack = Duration::from_millis(100);

        pace_mailbox(&key, GET_INTERVAL).await;
        let t = Instant::now();
        let got = TEST_MESSAGES_URL.scope(url.clone(), fetch_push_message(&http, "tok", &key, "m-1")).await.unwrap();
        assert!(got.is_none());
        let waited = t.elapsed();
        assert!(waited + slack >= GET_INTERVAL, "the push get waited for the walk's next slot: {waited:?}");
        assert_eq!(hits.load(SeqCst), 1);

        let t = Instant::now();
        pace_mailbox(&key, GET_INTERVAL).await;
        assert!(t.elapsed() + slack * 3 >= GET_INTERVAL, "the push took a slot from the walk: {:?}", t.elapsed());

        // The HTTP checks above prove push GETs use the walk's shared pacer.
        // Check contention using virtual time: host scheduling and HTTP latency
        // cannot bound when a correct reserved slot finishes on a busy runner.
        tokio::time::pause();
        mailbox_pacers().lock().unwrap().remove(&key);
        pace_mailbox(&key, GET_INTERVAL).await;
        let walk_slot = mailbox_pacers().lock().unwrap()[&key];
        let mut walk = Box::pin(async {
            for _ in 0..20 {
                pace_mailbox(&key, GET_INTERVAL).await;
            }
        });
        assert!(futures::poll!(walk.as_mut()).is_pending());
        assert_eq!(mailbox_pacers().lock().unwrap()[&key], walk_slot + GET_INTERVAL,
            "the walk reserves only its next GET, not twenty future slots");
        let push_slot = walk_slot + GET_INTERVAL;
        let mut push = Box::pin(pace_mailbox(&key, GET_INTERVAL));
        assert!(futures::poll!(push.as_mut()).is_pending());
        assert_eq!(mailbox_pacers().lock().unwrap()[&key], push_slot + GET_INTERVAL,
            "the push reserves exactly one slot after the waiting walk");

        // Tokio timers have millisecond precision. The fixed 1 ms below is
        // timer rounding, not a wall-clock allowance for runner scheduling.
        tokio::time::advance(walk_slot.saturating_duration_since(Instant::now())
            + Duration::from_millis(1)).await;
        assert!(futures::poll!(walk.as_mut()).is_pending());
        assert!(futures::poll!(push.as_mut()).is_pending(),
            "the push cannot run in the walk's slot");
        assert_eq!(mailbox_pacers().lock().unwrap()[&key], push_slot + GET_INTERVAL * 2,
            "the resumed walk queues behind the already reserved push");
        tokio::time::advance(push_slot.saturating_duration_since(Instant::now())
            + Duration::from_millis(1)).await;
        assert!(futures::poll!(push.as_mut()).is_ready(),
            "the push runs at its own slot while the long walk remains pending");
        assert!(futures::poll!(walk.as_mut()).is_pending());

    }

    #[tokio::test]
    async fn push_get_retry_with_zero_retry_after_consumes_another_pacer_slot() {
        use std::sync::Mutex;
        use std::time::Duration;
        use tokio::time::Instant;
        let attempts = std::sync::Arc::new(Mutex::new(Vec::new()));
        let app = axum::Router::new().fallback({
            let attempts = attempts.clone();
            move || {
                let attempts = attempts.clone();
                async move {
                    let mut attempts = attempts.lock().unwrap();
                    attempts.push(Instant::now());
                    let status = if attempts.len() == 1 {
                        axum::http::StatusCode::SERVICE_UNAVAILABLE
                    } else {
                        axum::http::StatusCode::NOT_FOUND
                    };
                    (status, [(axum::http::header::RETRY_AFTER, "0")])
                }
            }
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/messages", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let http = reqwest::Client::new();
        let key = rate_key(Uuid::new_v4(), &format!("{}@retry.test", Uuid::new_v4()));
        let message = crate::graph_rate_limit::scoped(key.clone(),
            TEST_MESSAGES_URL.scope(url, fetch_push_message(&http, "tok", &key, "m-retry")),
        ).await.unwrap();
        assert!(message.is_none());
        let attempts = attempts.lock().unwrap();
        assert_eq!(attempts.len(), 2);
        let spacing = attempts[1].duration_since(attempts[0]);
        assert!(spacing + Duration::from_millis(100) >= GET_INTERVAL,
            "the HTTP retry must consume the next get slot even with Retry-After 0: {spacing:?}");
        server.abort();
    }

    fn history_record(json: serde_json::Value) -> GHistoryRecord {
        serde_json::from_value(json).unwrap()
    }

    /// What `run_pass` does to one page: flatten, then net.
    fn split(page: GHistoryListResponse) -> NetHistory {
        net_history(process_history_page(page))
    }

    fn pair(t: &str, m: &str) -> (String, String) {
        (t.to_string(), m.to_string())
    }

    #[test]
    fn process_history_page_splits_added_from_deleted_and_ignores_label_only() {
        let page: GHistoryListResponse = serde_json::from_value(serde_json::json!({
            "history": [
                {
                    "id": "1",
                    "messagesAdded": [{"message": {"id": "m1", "threadId": "t1"}}]
                },
                {
                    "id": "2",
                    "labelsAdded": [{"message": {"id": "m1", "threadId": "t1"}, "labelIds": ["UNREAD"]}]
                },
                {
                    "id": "3",
                    "messagesDeleted": [{"message": {"id": "m2", "threadId": "t2"}}]
                }
            ],
            "nextPageToken": null,
            "historyId": "999"
        }))
        .unwrap();
        let net = split(page);
        assert_eq!(net.added, vec![pair("t1", "m1")], "labelsAdded-only record must not add a second entry");
        assert!(net.restored.is_empty());
        assert!(net.hidden.is_empty());
        assert_eq!(
            net.deleted,
            vec![pair("t2", "m2")],
            "a deletion's threadId must be kept, not dropped — propagate_deletions needs it \
             to resolve the thread's real graph_id"
        );
    }

    #[test]
    fn process_history_page_treats_trash_and_spam_label_adds_as_hides_not_deletions() {
        let page: GHistoryListResponse = serde_json::from_value(serde_json::json!({
            "history": [
                { "id": "1", "labelsAdded": [{"message": {"id": "m-trash", "threadId": "t1"}, "labelIds": ["TRASH"]}] },
                { "id": "2", "labelsAdded": [{"message": {"id": "m-spam", "threadId": "t2"}, "labelIds": ["SPAM"]}] },
                { "id": "3", "labelsAdded": [{"message": {"id": "m-read", "threadId": "t3"}, "labelIds": ["UNREAD"]}] }
            ]
        }))
        .unwrap();
        let net = split(page);
        assert!(net.added.is_empty());
        assert!(net.restored.is_empty());
        assert_eq!(
            net.hidden,
            vec![pair("t1", "m-trash"), pair("t2", "m-spam")],
            "a message gaining TRASH or SPAM leaves the visible mailbox and must be retracted — reversibly"
        );
        assert!(net.deleted.is_empty(), "a hide is not a permanent deletion: it must keep its tombstone path");
    }

    #[test]
    fn process_history_page_treats_untrash_and_not_spam_as_restores_not_adds() {
        let page: GHistoryListResponse = serde_json::from_value(serde_json::json!({
            "history": [
                { "id": "1", "labelsRemoved": [{"message": {"id": "m-untrash", "threadId": "t1"}, "labelIds": ["TRASH"]}] },
                { "id": "2", "labelsRemoved": [{"message": {"id": "m-ham", "threadId": "t2"}, "labelIds": ["SPAM"]}] },
                { "id": "3", "labelsRemoved": [{"message": {"id": "m-read", "threadId": "t3"}, "labelIds": ["UNREAD"]}] }
            ]
        }))
        .unwrap();
        let net = split(page);
        assert!(net.added.is_empty(), "a restore is not a plain add — it goes back where its hide took it from");
        assert_eq!(net.restored, vec![pair("t1", "m-untrash"), pair("t2", "m-ham")]);
        assert!(net.hidden.is_empty());
        assert!(net.deleted.is_empty());
    }

    #[test]
    fn net_history_lets_a_messages_last_event_in_the_pass_win() {
        let page: GHistoryListResponse = serde_json::from_value(serde_json::json!({
            "history": [
                // new → trashed within one pass: never ingest, retract (no-op).
                { "id": "1", "messagesAdded": [{"message": {"id": "m-a", "threadId": "t"}}] },
                { "id": "2", "labelsAdded": [{"message": {"id": "m-a", "threadId": "t"}, "labelIds": ["TRASH"]}] },
                // trashed → untrashed: restore, do not retract.
                { "id": "3", "labelsAdded": [{"message": {"id": "m-b", "threadId": "t"}, "labelIds": ["TRASH"]}] },
                { "id": "4", "labelsRemoved": [{"message": {"id": "m-b", "threadId": "t"}, "labelIds": ["TRASH"]}] },
                // new → permanently deleted: never fetch (messages.get would 404).
                { "id": "5", "messagesAdded": [{"message": {"id": "m-c", "threadId": "t"}}] },
                { "id": "6", "messagesDeleted": [{"message": {"id": "m-c", "threadId": "t"}}] },
                // spam → trash in ONE record: hidden wins over restored.
                { "id": "7",
                  "labelsAdded": [{"message": {"id": "m-d", "threadId": "t"}, "labelIds": ["TRASH"]}],
                  "labelsRemoved": [{"message": {"id": "m-d", "threadId": "t"}, "labelIds": ["SPAM"]}] }
            ]
        }))
        .unwrap();
        let net = split(page);
        assert!(net.added.is_empty());
        assert_eq!(net.restored, vec![pair("t", "m-b")]);
        assert_eq!(net.hidden, vec![pair("t", "m-a"), pair("t", "m-d")]);
        assert_eq!(net.deleted, vec![pair("t", "m-c")]);
    }

    #[test]
    fn process_history_page_skips_empty_or_missing_ids() {
        let page: GHistoryListResponse = serde_json::from_value(serde_json::json!({
            "history": [
                { "id": "1", "messagesAdded": [{"message": {"id": "", "threadId": "t1"}}] },
                { "id": "2", "messagesAdded": [{"message": {"threadId": "t1"}}] },
                { "id": "3", "messagesDeleted": [{"message": {"id": "  ", "threadId": "t2"}}] },
                { "id": "4", "labelsAdded": [{"message": {"id": "", "threadId": "t3"}, "labelIds": ["TRASH"]}] }
            ]
        }))
        .unwrap();
        assert_eq!(split(page), NetHistory::default());
    }

    #[test]
    fn history_record_reads_label_events_and_ignores_unknown_fields() {
        let r = history_record(serde_json::json!({
            "id": "1",
            "labelsRemoved": [{"message": {"id": "m1", "threadId": "t1"}, "labelIds": ["UNREAD"]}],
            "someFutureField": {"x": 1}
        }));
        assert!(r.messages_added.is_empty());
        assert!(r.messages_deleted.is_empty());
        assert!(r.labels_added.is_empty());
        assert_eq!(r.labels_removed.len(), 1);
        assert_eq!(r.labels_removed[0].label_ids, vec!["UNREAD".to_string()]);
    }

    #[test]
    fn hidden_labels_are_exactly_trash_and_spam() {
        let labels = |v: &[&str]| v.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        assert!(hides_message(&labels(&["INBOX", "TRASH"])));
        assert!(hides_message(&labels(&["SPAM"])));
        assert!(!hides_message(&labels(&["INBOX", "UNREAD", "IMPORTANT", "CATEGORY_PROMOTIONS"])));
        assert!(!hides_message(&labels(&[])));
        // Gmail label ids are case-sensitive system ids; a user label that
        // merely spells "trash" is not the system Trash.
        assert!(!hides_message(&labels(&["Label_12", "trash"])));
    }

    #[test]
    fn fetched_message_label_ids_drive_the_hidden_gate() {
        let trashed: GmailMessage = serde_json::from_value(serde_json::json!({
            "id": "m1", "threadId": "t1", "internalDate": "1735689600000", "labelIds": ["TRASH"]
        }))
        .unwrap();
        let visible: GmailMessage = serde_json::from_value(serde_json::json!({
            "id": "m2", "threadId": "t1", "internalDate": "1735689600000", "labelIds": ["INBOX"]
        }))
        .unwrap();
        let unlabelled: GmailMessage =
            serde_json::from_value(serde_json::json!({ "id": "m3", "threadId": "t1" })).unwrap();
        assert!(message_is_hidden(&trashed));
        assert!(!message_is_hidden(&visible));
        assert!(!message_is_hidden(&unlabelled), "a response without labelIds must not be treated as hidden");
    }

    #[test]
    fn budget_exceeded_is_the_exact_boundary_run_pass_uses() {
        assert!(!budget_exceeded(1998, 0, 2000));
        assert!(budget_exceeded(1999, 1, 2000), "exactly at budget must stop");
        assert!(budget_exceeded(2000, 0, 2000));
        assert!(budget_exceeded(0, 2000, 2000), "deletions count toward the budget too");
        assert!(!budget_exceeded(0, 0, 2000));
    }

    // ── decide_after_page: terminal page always wins over budget (ported
    // from google_calendar's CRITICAL #677 fix — see this module's own
    // decide_after_page doc for why Gmail's always-present historyId does
    // not change this) ──────────────────────────────────────────────────

    #[test]
    fn decide_after_page_terminal_page_at_exactly_the_budget_still_completes() {
        let decision = decide_after_page(None, Some("999".to_string()), 2000, 0, 2000);
        match decision {
            PageDecision::Done { resume_page_token, new_history_id } => {
                assert!(resume_page_token.is_none());
                assert_eq!(new_history_id.as_deref(), Some("999"));
            }
            PageDecision::Continue(_) => panic!("terminal page must never continue"),
        }
    }

    #[test]
    fn decide_after_page_terminal_page_well_under_budget_also_completes() {
        let decision = decide_after_page(None, Some("42".to_string()), 5, 0, 2000);
        match decision {
            PageDecision::Done { resume_page_token, new_history_id } => {
                assert!(resume_page_token.is_none());
                assert_eq!(new_history_id.as_deref(), Some("42"));
            }
            PageDecision::Continue(_) => panic!("terminal page must never continue"),
        }
    }

    #[test]
    fn decide_after_page_non_terminal_page_over_budget_stops_with_resume_cursor() {
        let decision = decide_after_page(Some("next-tok".to_string()), Some("ignored".to_string()), 2000, 0, 2000);
        match decision {
            PageDecision::Done { resume_page_token, new_history_id } => {
                assert_eq!(resume_page_token.as_deref(), Some("next-tok"));
                assert!(new_history_id.is_none(), "budget cutoff never yields a history id, even though Gmail sent one");
            }
            PageDecision::Continue(_) => panic!("must stop once over budget"),
        }
    }

    #[test]
    fn decide_after_page_non_terminal_page_under_budget_continues() {
        let decision = decide_after_page(Some("next-tok".to_string()), Some("ignored".to_string()), 5, 0, 2000);
        match decision {
            PageDecision::Continue(next) => assert_eq!(next, "next-tok"),
            PageDecision::Done { .. } => panic!("must keep paging while under budget"),
        }
    }

    // ── ADR-0008 storage-scoping: unwatched-thread discard (pure) ────────

    #[test]
    fn filter_watched_discards_unwatched_threads() {
        let added = vec![
            ("t1".to_string(), "m1".to_string()),
            ("t2".to_string(), "m2".to_string()),
            ("t1".to_string(), "m3".to_string()),
        ];
        let mut watched = HashSet::new();
        watched.insert("t1".to_string());
        let kept = filter_watched(added, &watched);
        assert_eq!(kept, vec![("t1".to_string(), "m1".to_string()), ("t1".to_string(), "m3".to_string())]);
    }

    #[test]
    fn filter_watched_empty_watch_set_discards_everything() {
        let added = vec![("t1".to_string(), "m1".to_string())];
        let kept = filter_watched(added, &HashSet::new());
        assert!(kept.is_empty());
    }

    #[test]
    fn parse_epoch_millis_round_trips() {
        let dt = parse_epoch_millis("1700000000000").unwrap();
        assert_eq!(dt.timestamp_millis(), 1700000000000);
    }

    #[test]
    fn parse_epoch_millis_rejects_garbage() {
        assert!(parse_epoch_millis("not-a-number").is_err());
    }

    // ── Fixture-driven (backend/tests/fixtures/gmail/ — see its README) ──

    const HISTORY_LIST_PAGE: &str = include_str!("../../tests/fixtures/gmail/history_list_page.json");
    const USERS_WATCH_RESPONSE: &str = include_str!("../../tests/fixtures/gmail/users_watch_response.json");
    const PUBSUB_PUSH_GMAIL: &str = include_str!("../../tests/fixtures/gmail/pubsub_push_gmail.json");

    #[test]
    fn fixture_history_page_shapes_added_and_deleted_ignoring_label_only() {
        let page: GHistoryListResponse = serde_json::from_str(HISTORY_LIST_PAGE).unwrap();
        assert!(page.next_page_token.is_some(), "fixture is a non-terminal page");
        let net = split(page);
        assert_eq!(net.added, vec![pair("18c9f1a0b2c3d4e5", "18c9f2a1b3d4e5f6")]);
        assert!(net.restored.is_empty());
        assert_eq!(net.deleted, vec![pair("18c8a0d2c4e6f8a1", "18c8a0d2c4e6f8a1")]);
    }

    #[test]
    fn fixture_users_watch_response_parses() {
        let parsed: GmailWatchResponse = serde_json::from_str(USERS_WATCH_RESPONSE).unwrap();
        assert_eq!(parsed.history_id, "9876500");
        assert!(parse_epoch_millis(&parsed.expiration).is_ok());
    }

    #[test]
    fn fixture_pubsub_push_envelope_carries_expected_subscription() {
        let v: serde_json::Value = serde_json::from_str(PUBSUB_PUSH_GMAIL).unwrap();
        assert!(v["subscription"].as_str().unwrap().ends_with("gmail-push-sub"));
        assert!(!v["message"]["data"].as_str().unwrap().is_empty());
    }

    // ── DB-level round trips (pg-gated) ─────────────────────────────────────
    //
    //   ORGT_TEST_BASE_URL=postgres://postgres:postgres@localhost:15544/postgres \
    //     cargo test -p coralswarm-backend --lib connectors::google_gmail::tests

    use sqlx::postgres::PgPoolOptions;
    use sqlx::{Connection, Executor, PgConnection};

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
    async fn seed_user(pool: &PgPool) -> Uuid {
        sqlx::query_scalar("INSERT INTO users(id, email) VALUES (gen_random_uuid(), $1) RETURNING id")
            .bind(format!("user-{}@test.local", Uuid::new_v4()))
            .fetch_one(pool)
            .await
            .unwrap()
    }
    async fn seed_account(pool: &PgPool, owner: Uuid) -> Uuid {
        sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES ($1) RETURNING id")
            .bind(owner)
            .fetch_one(pool)
            .await
            .unwrap()
    }
    /// Matches `routes::slack::resolve_primary_graph`'s own auto-create
    /// shape for the Personal-plane primary ocean — see
    /// `google_calendar::tests`' identical fixture note for the CHECK
    /// constraint this must satisfy.
    async fn seed_graph(pool: &PgPool, account_id: Uuid) -> Uuid {
        sqlx::query_scalar(
            "INSERT INTO graphs (account_id, name, kind, is_primary) \
             VALUES ($1, 'Primary', 'personal', true) RETURNING id",
        )
        .bind(account_id)
        .fetch_one(pool)
        .await
        .unwrap()
    }
    /// A second, non-primary ocean of the same account — where a thread
    /// saved "into ocean X" or a promoted copy lives.
    async fn seed_secondary_graph(pool: &PgPool, account_id: Uuid, name: &str) -> Uuid {
        sqlx::query_scalar(
            "INSERT INTO graphs (account_id, name, kind, is_primary) \
             VALUES ($1, $2, 'personal', false) RETURNING id",
        )
        .bind(account_id)
        .bind(name)
        .fetch_one(pool)
        .await
        .unwrap()
    }
    /// A Gmail message's corals exactly as `routes::ingest` stores them —
    /// one `"{message id}:chunk:N"` row per chunk.
    async fn seed_message_chunks(pool: &PgPool, graph_id: Uuid, message_id: &str, chunks: usize) {
        for n in 0..chunks {
            sqlx::query(
                "INSERT INTO atoms (graph_id, source_type, source_id, content_blob_key, timestamp) \
                 VALUES ($1, 'gmail', $2, $3, now())",
            )
            .bind(graph_id)
            .bind(format!("{message_id}:chunk:{n}"))
            .bind(format!("blob-{graph_id}-{message_id}-{n}"))
            .execute(pool)
            .await
            .unwrap();
        }
    }
    async fn gmail_atoms_in(pool: &PgPool, graph_id: Uuid) -> Vec<String> {
        sqlx::query_scalar("SELECT source_id FROM atoms WHERE graph_id = $1 AND source_type = 'gmail' ORDER BY source_id")
            .bind(graph_id)
            .fetch_all(pool)
            .await
            .unwrap()
    }
    async fn tombstones(pool: &PgPool, account_id: Uuid) -> Vec<(Uuid, String)> {
        sqlx::query_as(
            "SELECT graph_id, message_id FROM gmail_hidden_messages WHERE account_id = $1 ORDER BY message_id, graph_id",
        )
        .bind(account_id)
        .fetch_all(pool)
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn watch_row_round_trip_seeds_baseline_only_on_insert_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping watch_row_round_trip_seeds_baseline_only_on_insert_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "gws_g23_watch_round_trip";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let owner = seed_user(&pool).await;
        let account_id = seed_account(&pool, owner).await;
        let email = "member@example.com";
        let far_future = Utc::now() + chrono::Duration::days(7);

        // Fresh insert: baseline history id is seeded.
        upsert_watch(&pool, account_id, owner, email, "projects/p/topics/gmail-push", far_future, "1000")
            .await
            .unwrap();
        let row: (String, Uuid) = sqlx::query_as(
            "SELECT sync_token, owner_user_id FROM google_watch_channels \
             WHERE account_id = $1 AND surface = 'gmail' AND resource_id = $2",
        )
        .bind(account_id)
        .bind(email)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(row.0, "1000");
        assert_eq!(row.1, owner);

        // A later "renewal" upsert (different baseline, as if a fresh watch
        // call happened) must NOT clobber the already-advanced cursor.
        upsert_watch(&pool, account_id, owner, email, "projects/p/topics/gmail-push", far_future, "9999")
            .await
            .unwrap();
        let sync_token_after: String = sqlx::query_scalar(
            "SELECT sync_token FROM google_watch_channels WHERE account_id = $1 AND surface = 'gmail' AND resource_id = $2",
        )
        .bind(account_id)
        .bind(email)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(sync_token_after, "1000", "a renewal upsert must never overwrite an existing cursor");

        pool.close().await;
        let mut admin_conn = PgConnection::connect(&base).await.unwrap();
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    #[tokio::test]
    async fn empty_email_never_resolves_via_the_push_owner_function_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping empty_email_never_resolves_via_the_push_owner_function_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "gws_g23_empty_email";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let owner = seed_user(&pool).await;
        let account_id = seed_account(&pool, owner).await;
        let far_future = Utc::now() + chrono::Duration::days(7);
        upsert_watch(&pool, account_id, owner, "real@example.com", "projects/p/topics/gmail-push", far_future, "1000")
            .await
            .unwrap();

        assert!(resolve_watch_owners_for_push(&pool, "").await.unwrap().is_empty());
        // Belt-and-suspenders: the raw SQL resolver itself, independent of
        // the Rust-level short circuit.
        let via_sql: Vec<(Uuid, Uuid)> = sqlx::query_as("SELECT * FROM resolve_gmail_watch_owners($1)")
            .bind("")
            .fetch_all(&pool)
            .await
            .unwrap();
        assert!(via_sql.is_empty());

        let resolved = resolve_watch_owners_for_push(&pool, "real@example.com").await.unwrap();
        assert_eq!(resolved, vec![(account_id, owner)]);

        pool.close().await;
        let mut admin_conn = PgConnection::connect(&base).await.unwrap();
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    /// The SAME email address connected under TWO different accounts (the
    /// personal + org shape) resolves to BOTH, each with its own owner —
    /// never an arbitrary pick (CodeRabbit round 1 finding #1 on PR #686) and
    /// no longer nothing (migration `0187`'s fail-closed resolver, which
    /// starved both accounts; see `0275`'s header).
    #[tokio::test]
    async fn same_email_across_two_accounts_resolves_to_every_account_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping same_email_across_two_accounts_resolves_to_every_account_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "gws_g23_email_collision";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let owner_a = seed_user(&pool).await;
        let account_a = seed_account(&pool, owner_a).await;
        let owner_b = seed_user(&pool).await;
        let account_b = seed_account(&pool, owner_b).await;
        let far_future = Utc::now() + chrono::Duration::days(7);
        let shared_email = "shared@example.com";

        // The SAME Gmail address, watched under two DIFFERENT accounts —
        // `google_watch_channels`' own `UNIQUE (account_id, surface,
        // resource_id)` permits exactly this (it is scoped per account, not
        // globally).
        upsert_watch(&pool, account_a, owner_a, shared_email, "projects/p/topics/gmail-push", far_future, "1000")
            .await
            .unwrap();
        upsert_watch(&pool, account_b, owner_b, shared_email, "projects/p/topics/gmail-push", far_future, "2000")
            .await
            .unwrap();

        let mut expected = vec![(account_a, owner_a), (account_b, owner_b)];
        expected.sort();
        let resolved = resolve_watch_owners_for_push(&pool, shared_email).await.unwrap();
        assert_eq!(
            resolved, expected,
            "a mailbox watched under two accounts must dispatch to BOTH, each with its own owner"
        );
        // Belt-and-suspenders: the raw SQL resolver itself, independent of
        // the Rust-level wrapper.
        let via_sql: Vec<(Uuid, Uuid)> = sqlx::query_as("SELECT * FROM resolve_gmail_watch_owners($1)")
            .bind(shared_email)
            .fetch_all(&pool)
            .await
            .unwrap();
        assert_eq!(via_sql, expected);

        // A DIFFERENT email on one of the same two accounts resolves to that
        // account alone — the fan-out is per mailbox, not per account.
        upsert_watch(&pool, account_a, owner_a, "unique@example.com", "projects/p/topics/gmail-push", far_future, "3000")
            .await
            .unwrap();
        let resolved = resolve_watch_owners_for_push(&pool, "unique@example.com").await.unwrap();
        assert_eq!(resolved, vec![(account_a, owner_a)]);

        pool.close().await;
        let mut admin_conn = PgConnection::connect(&base).await.unwrap();
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    #[tokio::test]
    async fn gmail_watched_threads_upsert_and_backfill_state_round_trip_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping gmail_watched_threads_upsert_and_backfill_state_round_trip_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "gws_g23_watched_threads";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let owner = seed_user(&pool).await;
        let account_id = seed_account(&pool, owner).await;
        let graph_id = seed_graph(&pool, account_id).await;

        upsert_watched_thread(&pool, account_id, graph_id, owner, "thread-1", true, None).await.unwrap();
        let row: (bool, Option<String>) =
            sqlx::query_as("SELECT backfill_complete, backfill_cursor FROM gmail_watched_threads WHERE account_id = $1 AND thread_id = $2")
                .bind(account_id)
                .bind("thread-1")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert!(row.0, "opt-in ingest always writes backfill_complete = TRUE");
        assert!(row.1.is_none());

        let watched = watched_thread_ids_among(&pool, account_id, &["thread-1".to_string(), "thread-2".to_string()])
            .await
            .unwrap();
        assert_eq!(watched.len(), 1);
        assert!(watched.contains("thread-1"));

        // Re-upsert (a later reply's opt-in) must not clobber the row.
        upsert_watched_thread(&pool, account_id, graph_id, owner, "thread-1", true, None).await.unwrap();
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM gmail_watched_threads WHERE account_id = $1")
            .bind(account_id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count, 1, "ON CONFLICT must update, not duplicate");

        pool.close().await;
        let mut admin_conn = PgConnection::connect(&base).await.unwrap();
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    // ── gmail-options backend train, PR 1 of 4: `follow_replies` ────────────
    //
    //   ORGT_TEST_BASE_URL=postgres://postgres:postgres@localhost:15544/postgres \
    //     cargo test -p coralswarm-backend --lib connectors::google_gmail::tests

    /// A save with `follow=false` writes the row with `follow_replies=false`
    /// — never inserts nothing, never omits the row.
    #[tokio::test]
    async fn follow_false_save_writes_follow_replies_false_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping follow_false_save_writes_follow_replies_false_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "gws_gopt_follow_false_save";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let owner = seed_user(&pool).await;
        let account_id = seed_account(&pool, owner).await;
        let graph_id = seed_graph(&pool, account_id).await;

        upsert_watched_thread(&pool, account_id, graph_id, owner, "thread-unfollowed", false, None)
            .await
            .unwrap();
        let follow_replies: bool = sqlx::query_scalar(
            "SELECT follow_replies FROM gmail_watched_threads WHERE account_id = $1 AND thread_id = $2",
        )
        .bind(account_id)
        .bind("thread-unfollowed")
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(!follow_replies, "follow=false must write follow_replies=false, never skip the row");

        pool.close().await;
        let mut admin_conn = PgConnection::connect(&base).await.unwrap();
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    /// `watched_thread_ids_among` (the live-follow gate `fetch_pending`
    /// consults) excludes unfollowed rows; `watched_thread_graph_ids` (the
    /// deletion-propagation lookup) still returns them — the two functions'
    /// deliberately different postures, proven side by side against the same
    /// data.
    #[tokio::test]
    async fn unfollowed_thread_excluded_from_live_follow_but_kept_for_deletion_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping unfollowed_thread_excluded_from_live_follow_but_kept_for_deletion_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "gws_gopt_unfollow_split";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let owner = seed_user(&pool).await;
        let account_id = seed_account(&pool, owner).await;
        let graph_id = seed_graph(&pool, account_id).await;

        upsert_watched_thread(&pool, account_id, graph_id, owner, "thread-followed", true, None)
            .await
            .unwrap();
        upsert_watched_thread(&pool, account_id, graph_id, owner, "thread-unfollowed", false, None)
            .await
            .unwrap();

        let ids = vec!["thread-followed".to_string(), "thread-unfollowed".to_string()];
        let live_follow = watched_thread_ids_among(&pool, account_id, &ids).await.unwrap();
        assert_eq!(live_follow.len(), 1, "only the followed thread should pass the live-follow gate");
        assert!(live_follow.contains("thread-followed"));
        assert!(
            !live_follow.contains("thread-unfollowed"),
            "an unfollowed thread must not be treated as live-follow-eligible"
        );

        let deletion_targets = watched_thread_graph_ids(&pool, account_id, owner, &ids).await.unwrap();
        assert_eq!(
            deletion_targets.len(),
            2,
            "deletion propagation must stay flag-blind — both rows must resolve a graph_id"
        );
        assert_eq!(deletion_targets.get("thread-unfollowed"), Some(&vec![graph_id]));

        pool.close().await;
        let mut admin_conn = PgConnection::connect(&base).await.unwrap();
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    /// `watched_status`/`set_follow` — the watched-resource routes' store
    /// layer: GET on a missing row is `None` (route maps this to
    /// `following=false`, never `404`); PUT/DELETE (`set_follow`) flip the
    /// flag and NEVER delete the row, including across repeated toggles.
    #[tokio::test]
    async fn watched_status_and_set_follow_never_delete_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping watched_status_and_set_follow_never_delete_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "gws_gopt_status_set_follow";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let owner = seed_user(&pool).await;
        let account_id = seed_account(&pool, owner).await;
        let graph_id = seed_graph(&pool, account_id).await;

        // GET on a missing row → `None` (never an error, never a row).
        assert_eq!(watched_status(&pool, account_id, "never-saved").await.unwrap(), None);
        // PUT/DELETE on a missing row → `None` — the route's 404 signal.
        assert_eq!(set_follow(&pool, account_id, "never-saved", false).await.unwrap(), None);

        upsert_watched_thread(&pool, account_id, graph_id, owner, "thread-toggle", true, None)
            .await
            .unwrap();
        assert_eq!(watched_status(&pool, account_id, "thread-toggle").await.unwrap(), Some(true));

        // DELETE (PUT {"follow": false} under the hood) flips, never deletes.
        assert_eq!(set_follow(&pool, account_id, "thread-toggle", false).await.unwrap(), Some(false));
        assert_eq!(watched_status(&pool, account_id, "thread-toggle").await.unwrap(), Some(false));

        // PUT {"follow": true} re-follows — still the same row.
        assert_eq!(set_follow(&pool, account_id, "thread-toggle", true).await.unwrap(), Some(true));

        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM gmail_watched_threads WHERE account_id = $1")
            .bind(account_id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count, 1, "toggling follow must never insert or delete a row");

        pool.close().await;
        let mut admin_conn = PgConnection::connect(&base).await.unwrap();
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    #[tokio::test]
    async fn message_scope_boundary_rejects_pre_save_history_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping message_scope_boundary_rejects_pre_save_history_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "gws_gopt_message_boundary";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        recreate(&base, db).await.close().await.unwrap();
        let pool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();
        let owner = seed_user(&pool).await;
        let account = seed_account(&pool, owner).await;
        let graph = seed_graph(&pool, account).await;
        let since = Utc::now();
        upsert_watched_thread(&pool, account, graph, owner, "message-only", true, Some(since)).await.unwrap();
        let boundaries = watched_thread_follow_since(&pool, account, &["message-only".into()]).await.unwrap();
        let boundary = boundaries["message-only"];
        assert!(boundary.is_some());
        let message = |date: DateTime<Utc>| serde_json::from_value::<GmailMessage>(serde_json::json!({
            "id":"m","threadId":"message-only","internalDate":date.timestamp_millis().to_string()
        })).unwrap();
        assert!(!gmail_message_is_future_reply(&message(since - chrono::Duration::days(1)), boundary));
        assert!(gmail_message_is_future_reply(&message(since + chrono::Duration::seconds(1)), boundary));
        assert!(gmail_message_is_future_reply(&message(since - chrono::Duration::days(1)), None));
        assert_eq!(watched_thread_graph_ids(&pool, account, owner, &["message-only".into()]).await.unwrap()["message-only"], vec![graph]);
        upsert_watched_thread(&pool, account, graph, owner, "message-only", true, None).await.unwrap();
        assert!(watched_thread_follow_since(&pool, account, &["message-only".into()]).await.unwrap()["message-only"].is_none());
        pool.close().await;
        let mut admin = PgConnection::connect(&base).await.unwrap();
        admin.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    /// A later save of an already-watched thread adopts THAT save's `follow`
    /// value — re-saving with `follow=true` re-follows a thread the caller
    /// had previously unfollowed.
    #[tokio::test]
    async fn resave_with_follow_true_re_follows_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping resave_with_follow_true_re_follows_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "gws_gopt_resave_refollow";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let owner = seed_user(&pool).await;
        let account_id = seed_account(&pool, owner).await;
        let graph_id = seed_graph(&pool, account_id).await;

        upsert_watched_thread(&pool, account_id, graph_id, owner, "thread-resave", true, None)
            .await
            .unwrap();
        set_follow(&pool, account_id, "thread-resave", false).await.unwrap();
        assert_eq!(watched_status(&pool, account_id, "thread-resave").await.unwrap(), Some(false));

        // The user re-opens the same thread and saves again (e.g. a later
        // reply's opt-in) with `follow=true` — the same `after_ingest` call
        // this connector always makes on a save.
        upsert_watched_thread(&pool, account_id, graph_id, owner, "thread-resave", true, None)
            .await
            .unwrap();
        assert_eq!(
            watched_status(&pool, account_id, "thread-resave").await.unwrap(),
            Some(true),
            "re-saving with follow=true must re-follow"
        );

        pool.close().await;
        let mut admin_conn = PgConnection::connect(&base).await.unwrap();
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    #[tokio::test]
    async fn sync_columns_unchanged_on_progress_advances_on_complete_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping sync_columns_unchanged_on_progress_advances_on_complete_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "gws_g23_sync_columns";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let owner = seed_user(&pool).await;
        let account_id = seed_account(&pool, owner).await;
        let email = "member@example.com";
        let far_future = Utc::now() + chrono::Duration::days(7);
        upsert_watch(&pool, account_id, owner, email, "projects/p/topics/gmail-push", far_future, "1000")
            .await
            .unwrap();

        // A mid-pass progress write leaves the cursor (sync_token) unchanged
        // — only the resume page token advances.
        persist_history_progress(&pool, account_id, email, Some("page-tok-1")).await.unwrap();
        let row: (String, Option<String>, bool) = sqlx::query_as(
            "SELECT sync_token, backfill_page_token, backfill_complete FROM google_watch_channels \
             WHERE account_id = $1 AND surface = 'gmail' AND resource_id = $2",
        )
        .bind(account_id)
        .bind(email)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(row.0, "1000", "a Progress commit must not advance the cursor");
        assert_eq!(row.1.as_deref(), Some("page-tok-1"));
        assert!(!row.2);

        // A Complete commit advances the cursor and clears the resume token.
        persist_history_complete(&pool, account_id, email, "2000").await.unwrap();
        let row2: (String, Option<String>, bool) = sqlx::query_as(
            "SELECT sync_token, backfill_page_token, backfill_complete FROM google_watch_channels \
             WHERE account_id = $1 AND surface = 'gmail' AND resource_id = $2",
        )
        .bind(account_id)
        .bind(email)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(row2.0, "2000");
        assert!(row2.1.is_none());
        assert!(row2.2);

        pool.close().await;
        let mut admin_conn = PgConnection::connect(&base).await.unwrap();
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    #[tokio::test]
    async fn deletion_propagation_removes_the_matching_atom_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping deletion_propagation_removes_the_matching_atom_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "gws_g23_deletion";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let owner = seed_user(&pool).await;
        let account_id = seed_account(&pool, owner).await;
        let graph_id = seed_graph(&pool, account_id).await;
        upsert_watched_thread(&pool, account_id, graph_id, owner, "thread-del-1", true, None).await.unwrap();
        // `content_blob_key`/`timestamp` NOT NULL with no default — mirrors
        // `routes::ingest`'s `insert_atom` shape exactly (see
        // `google_calendar::tests`' identical note).
        let atom_id: Uuid = sqlx::query_scalar(
            "INSERT INTO atoms (graph_id, source_type, source_id, content_blob_key, timestamp) \
             VALUES ($1, 'gmail', 'msg-del-1', 'blob-msg-del-1', now()) RETURNING atom_id",
        )
        .bind(graph_id)
        .fetch_one(&pool)
        .await
        .unwrap();

        propagate_deletions(&pool, account_id, owner, AccountPlane::Personal, &[("thread-del-1".to_string(), "msg-del-1".to_string())])
            .await
            .unwrap();

        let still_there: i64 = sqlx::query_scalar("SELECT count(*) FROM atoms WHERE atom_id = $1")
            .bind(atom_id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(still_there, 0, "a deleted gmail message must delete its atom");

        pool.close().await;
        let mut admin_conn = PgConnection::connect(&base).await.unwrap();
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    /// CodeRabbit round 1 finding #2 on PR #686 — corrected in place, see
    /// `propagate_deletions`'s own doc for the full story. The finding's
    /// claimed MECHANISM ("wrong graph = `delete_source` no-ops = silent
    /// retention failure") does not hold: `services::deletion::
    /// delete_source` is documented, and implemented
    /// (`services::deletion::run_cascade`), to delete every atom ANYWHERE IN
    /// THE SAME ACCOUNT sharing `(source_type, source_id)` — the
    /// `origin_graph_id` argument only resolves/authorizes the ACCOUNT, it
    /// never scopes WHICH atom is deleted. A first version of this test
    /// asserted the opposite (a same-`source_id` atom in a DIFFERENT graph
    /// of the same account must survive) and failed against the real
    /// implementation — confirming the mechanism claim was wrong, not the
    /// test. What this test proves instead, and what remains genuinely true
    /// and worth keeping: `propagate_deletions` succeeds using ONLY the
    /// watched thread's own `gmail_watched_threads.graph_id` as its
    /// authorization anchor — no primary ocean is seeded at all here, so a
    /// dependency on `resolve_primary_graph` (removed by this PR) would make
    /// this test fail outright, not just resolve a different graph.
    #[tokio::test]
    async fn deletion_authorizes_via_the_watched_threads_own_graph_no_primary_ocean_needed_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping deletion_authorizes_via_the_watched_threads_own_graph_no_primary_ocean_needed_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "gws_g23_deletion_no_primary";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let owner = seed_user(&pool).await;
        let account_id = seed_account(&pool, owner).await;
        // Deliberately NOT the account's primary ocean — a non-primary
        // graph, seeded directly (not via `resolve_primary_graph`'s
        // auto-create path), to prove this function has no dependency on
        // that resolution at all. `graphs_member_requires_owner`'s CHECK
        // only constrains `kind='member'`; `kind='personal', is_primary
        // =false` is a valid, distinct row a member can be watching a
        // thread through some other route (e.g. Train G2.4).
        let other_graph_id: Uuid = sqlx::query_scalar(
            "INSERT INTO graphs (account_id, name, kind, is_primary) \
             VALUES ($1, 'Other', 'personal', false) RETURNING id",
        )
        .bind(account_id)
        .fetch_one(&pool)
        .await
        .unwrap();

        upsert_watched_thread(&pool, account_id, other_graph_id, owner, "thread-del-2", true, None).await.unwrap();

        let other_atom_id: Uuid = sqlx::query_scalar(
            "INSERT INTO atoms (graph_id, source_type, source_id, content_blob_key, timestamp) \
             VALUES ($1, 'gmail', 'msg-del-2', 'blob-other', now()) RETURNING atom_id",
        )
        .bind(other_graph_id)
        .fetch_one(&pool)
        .await
        .unwrap();

        propagate_deletions(&pool, account_id, owner, AccountPlane::Personal, &[("thread-del-2".to_string(), "msg-del-2".to_string())])
            .await
            .unwrap();

        let other_count: i64 = sqlx::query_scalar("SELECT count(*) FROM atoms WHERE atom_id = $1")
            .bind(other_atom_id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(
            other_count, 0,
            "deletion must succeed via the watched thread's own graph, with no account primary \
             ocean involved at all"
        );

        pool.close().await;
        let mut admin_conn = PgConnection::connect(&base).await.unwrap();
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    /// A deletion for a thread never opted into ingest is a correct no-op
    /// (module doc's "Deletion destination" / `propagate_deletions`'s own
    /// doc) — there is no `gmail_watched_threads` row to resolve a graph
    /// from, so there is structurally nothing to delete. Must not error and
    /// must not touch any unrelated atom.
    #[tokio::test]
    async fn deletion_for_a_never_watched_thread_is_a_no_op_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping deletion_for_a_never_watched_thread_is_a_no_op_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "gws_g23_deletion_unwatched";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let owner = seed_user(&pool).await;
        let account_id = seed_account(&pool, owner).await;
        let graph_id = seed_graph(&pool, account_id).await;
        // An unrelated, otherwise-real atom in this account — proves the
        // no-op doesn't accidentally touch anything.
        let unrelated_atom_id: Uuid = sqlx::query_scalar(
            "INSERT INTO atoms (graph_id, source_type, source_id, content_blob_key, timestamp) \
             VALUES ($1, 'gmail', 'unrelated-msg', 'blob-unrelated', now()) RETURNING atom_id",
        )
        .bind(graph_id)
        .fetch_one(&pool)
        .await
        .unwrap();

        // No gmail_watched_threads row for "thread-never-watched" exists,
        // and this owner has no `gmail_everything_mode` row either — a
        // genuine no-op on both counts.
        propagate_deletions(&pool, account_id, owner, AccountPlane::Personal, &[("thread-never-watched".to_string(), "msg-x".to_string())])
            .await
            .unwrap();

        let still_there: i64 = sqlx::query_scalar("SELECT count(*) FROM atoms WHERE atom_id = $1")
            .bind(unrelated_atom_id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(still_there, 1, "an unwatched-thread deletion must be a no-op, not touch anything");

        pool.close().await;
        let mut admin_conn = PgConnection::connect(&base).await.unwrap();
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    /// gsuite Train G2 PR 4 (everything mode): a deletion for a thread NEVER
    /// registered in `gmail_watched_threads` must still propagate when this
    /// owner currently has everything mode enabled — proving
    /// [`everything_mode_graph_id`]'s fallback closes the gap the previous
    /// test (`deletion_for_a_never_watched_thread_is_a_no_op_pg`) documents
    /// for the genuinely-never-ingested case. Founder decision Q3 requires
    /// ALL deletions to propagate under everything mode, including Gmail's
    /// own automatic Spam/Trash 30-day purges — this is the mechanism that
    /// makes that hold for content the whole-mailbox pull ingested (which
    /// never writes a `gmail_watched_threads` row for what it pulls).
    #[tokio::test]
    async fn deletion_for_an_everything_mode_ingested_thread_still_propagates_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping deletion_for_an_everything_mode_ingested_thread_still_propagates_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "gws_g24_deletion_everything_mode";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let owner = seed_user(&pool).await;
        let account_id = seed_account(&pool, owner).await;
        let graph_id = seed_graph(&pool, account_id).await;

        // The owner has everything mode ENABLED against this graph — no
        // `gmail_watched_threads` row anywhere, exactly what `workers::
        // gmail_everything_mode` leaves behind (it never registers one).
        sqlx::query(
            "INSERT INTO gmail_everything_mode (account_id, owner_user_id, graph_id, enabled) \
             VALUES ($1, $2, $3, TRUE)",
        )
        .bind(account_id)
        .bind(owner)
        .bind(graph_id)
        .execute(&pool)
        .await
        .unwrap();

        let atom_id: Uuid = sqlx::query_scalar(
            "INSERT INTO atoms (graph_id, source_type, source_id, content_blob_key, timestamp) \
             VALUES ($1, 'gmail', 'msg-everything-1', 'blob-everything-1', now()) RETURNING atom_id",
        )
        .bind(graph_id)
        .fetch_one(&pool)
        .await
        .unwrap();

        propagate_deletions(
            &pool,
            account_id,
            owner,
            AccountPlane::Personal,
            &[("thread-never-watched-but-everything-mode".to_string(), "msg-everything-1".to_string())],
        )
        .await
        .unwrap();

        let still_there: i64 = sqlx::query_scalar("SELECT count(*) FROM atoms WHERE atom_id = $1")
            .bind(atom_id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(
            still_there, 0,
            "a deletion for a thread never in gmail_watched_threads must still propagate when \
             the owner has everything mode enabled"
        );

        pool.close().await;
        let mut admin_conn = PgConnection::connect(&base).await.unwrap();
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    /// gsuite Train G2 PR 4: the everything-mode fallback must NOT fire once
    /// the owner has DISABLED everything mode again — a stale `enabled =
    /// FALSE` row must behave identically to no row at all (the same
    /// no-op outcome `deletion_for_a_never_watched_thread_is_a_no_op_pg`
    /// proves for a fresh mailbox).
    #[tokio::test]
    async fn deletion_fallback_still_fires_after_everything_mode_is_disabled_pg() {
        // R3 (adversarial review, 2026-08-25) — this test used to assert the
        // OPPOSITE (`deletion_fallback_does_not_fire_once_everything_mode_
        // is_disabled_pg`), which locked in exactly the retention gap this
        // whole mechanism exists to close: disabling only flips a flag, it
        // never purges already-ingested atoms, so a deletion for one of
        // those atoms must STILL propagate after disable — otherwise
        // enable → ingest → disable → delete upstream retains the atom
        // forever (Gmail reports a given deletion exactly once). The
        // earlier version of this test also had a latent bug independent of
        // the assertion direction: it deleted a NON-matching source_id
        // (`msg-y`) against an atom seeded with a DIFFERENT source_id
        // (`unrelated-msg-2`), so it was vacuous regardless of `enabled` —
        // fixed here by using the SAME source_id for both.
        let Some(base) = base_url() else {
            eprintln!("skipping deletion_fallback_still_fires_after_everything_mode_is_disabled_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "gws_g24_deletion_everything_mode_disabled";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let owner = seed_user(&pool).await;
        let account_id = seed_account(&pool, owner).await;
        let graph_id = seed_graph(&pool, account_id).await;

        sqlx::query(
            "INSERT INTO gmail_everything_mode (account_id, owner_user_id, graph_id, enabled) \
             VALUES ($1, $2, $3, FALSE)",
        )
        .bind(account_id)
        .bind(owner)
        .bind(graph_id)
        .execute(&pool)
        .await
        .unwrap();

        let atom_id: Uuid = sqlx::query_scalar(
            "INSERT INTO atoms (graph_id, source_type, source_id, content_blob_key, timestamp) \
             VALUES ($1, 'gmail', 'msg-ingested-before-disable', 'blob-msg-y', now()) RETURNING atom_id",
        )
        .bind(graph_id)
        .fetch_one(&pool)
        .await
        .unwrap();

        propagate_deletions(
            &pool,
            account_id,
            owner,
            AccountPlane::Personal,
            &[(
                "thread-never-watched-mode-off".to_string(),
                "msg-ingested-before-disable".to_string(),
            )],
        )
        .await
        .unwrap();

        let still_there: i64 = sqlx::query_scalar("SELECT count(*) FROM atoms WHERE atom_id = $1")
            .bind(atom_id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(
            still_there, 0,
            "a message ingested while everything mode was on must still have its deletion \
             propagate even after the owner has since disabled it — disabling only stops NEW \
             ingest, it does not purge what was already ingested"
        );

        pool.close().await;
        let mut admin_conn = PgConnection::connect(&base).await.unwrap();
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    /// fix/gmail-personal-delete: the REAL stored shape. Every test above
    /// seeds a bare `source_id` equal to the Gmail message id, which is not
    /// what `routes::ingest` writes — `ingest_atom` stores one
    /// `"{message id}:chunk:N"` row per chunk (`email_body::
    /// gmail_message_to_item` sets `source_id` to the bare message id, the
    /// chunk loop appends the suffix), so those tests passed for the wrong
    /// reason while production retained every deleted message. This test
    /// seeds the chunked shape and proves `propagate_deletions` retracts
    /// every chunk of the deleted message — and ONLY that message: a
    /// sibling message in the same watched thread, and a message whose id
    /// merely has the deleted id as a prefix (`msg-chunked-10` vs
    /// `msg-chunked-1`), must both survive. Fails before the fix (bare id
    /// matched nothing; both chunks stayed).
    #[tokio::test]
    async fn deletion_propagation_removes_every_chunk_of_the_deleted_message_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping deletion_propagation_removes_every_chunk_of_the_deleted_message_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "gws_g23_deletion_chunked";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let owner = seed_user(&pool).await;
        let account_id = seed_account(&pool, owner).await;
        let graph_id = seed_graph(&pool, account_id).await;
        upsert_watched_thread(&pool, account_id, graph_id, owner, "thread-chunked", true, None).await.unwrap();

        // Exactly `routes::ingest`'s shape: `"{message id}:chunk:{position}"`.
        for (source_id, blob) in [
            ("msg-chunked-1:chunk:0", "blob-c1-0"),
            ("msg-chunked-1:chunk:1", "blob-c1-1"),
            ("msg-chunked-keep:chunk:0", "blob-keep-0"),
            ("msg-chunked-10:chunk:0", "blob-c10-0"),
        ] {
            sqlx::query(
                "INSERT INTO atoms (graph_id, source_type, source_id, content_blob_key, timestamp) \
                 VALUES ($1, 'gmail', $2, $3, now())",
            )
            .bind(graph_id)
            .bind(source_id)
            .bind(blob)
            .execute(&pool)
            .await
            .unwrap();
        }

        propagate_deletions(
            &pool,
            account_id,
            owner,
            AccountPlane::Personal,
            &[("thread-chunked".to_string(), "msg-chunked-1".to_string())],
        )
        .await
        .unwrap();

        let remaining: Vec<String> = sqlx::query_scalar(
            "SELECT source_id FROM atoms WHERE graph_id = $1 AND source_type = 'gmail' ORDER BY source_id",
        )
        .bind(graph_id)
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(
            remaining,
            vec!["msg-chunked-10:chunk:0".to_string(), "msg-chunked-keep:chunk:0".to_string()],
            "every `:chunk:N` row of the deleted message must be gone; the sibling message and \
             the prefix-sharing message must both survive"
        );

        pool.close().await;
        let mut admin_conn = PgConnection::connect(&base).await.unwrap();
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    /// fix/gmail-personal-delete, everything-mode arm of the same gap: the
    /// whole-mailbox pull (`workers::gmail_everything_mode`) also goes
    /// through `ingest_atom`, so its corals are `"{message id}:chunk:N"`
    /// rows too, reached here via the `everything_mode_graph_id` fallback
    /// (no `gmail_watched_threads` row). Fails before the fix for the same
    /// reason as the watched-thread test above.
    #[tokio::test]
    async fn deletion_for_an_everything_mode_ingested_chunked_message_propagates_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping deletion_for_an_everything_mode_ingested_chunked_message_propagates_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "gws_g24_deletion_everything_mode_chunked";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let owner = seed_user(&pool).await;
        let account_id = seed_account(&pool, owner).await;
        let graph_id = seed_graph(&pool, account_id).await;
        sqlx::query(
            "INSERT INTO gmail_everything_mode (account_id, owner_user_id, graph_id, enabled) \
             VALUES ($1, $2, $3, TRUE)",
        )
        .bind(account_id)
        .bind(owner)
        .bind(graph_id)
        .execute(&pool)
        .await
        .unwrap();

        for (source_id, blob) in [
            ("msg-everything-chunked:chunk:0", "blob-ec-0"),
            ("msg-everything-chunked:chunk:1", "blob-ec-1"),
            ("msg-everything-other:chunk:0", "blob-eo-0"),
        ] {
            sqlx::query(
                "INSERT INTO atoms (graph_id, source_type, source_id, content_blob_key, timestamp) \
                 VALUES ($1, 'gmail', $2, $3, now())",
            )
            .bind(graph_id)
            .bind(source_id)
            .bind(blob)
            .execute(&pool)
            .await
            .unwrap();
        }

        propagate_deletions(
            &pool,
            account_id,
            owner,
            AccountPlane::Personal,
            &[("thread-never-watched-everything-chunked".to_string(), "msg-everything-chunked".to_string())],
        )
        .await
        .unwrap();

        let remaining: Vec<String> = sqlx::query_scalar(
            "SELECT source_id FROM atoms WHERE graph_id = $1 AND source_type = 'gmail' ORDER BY source_id",
        )
        .bind(graph_id)
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(
            remaining,
            vec!["msg-everything-other:chunk:0".to_string()],
            "every `:chunk:N` row of the everything-mode-ingested deleted message must be gone; \
             the other message must survive"
        );

        pool.close().await;
        let mut admin_conn = PgConnection::connect(&base).await.unwrap();
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    /// CodeRabbit round 2 finding #1 on PR #686 (Major): a deletion-batch
    /// graph lookup failure must make `propagate_deletions` return `Err`,
    /// never swallow the failure and return `Ok(())`. Forces a REAL SQL-level
    /// failure (dropping `gmail_watched_threads` out from under the pending
    /// `SELECT` — deterministic, no mocking infra needed) rather than
    /// asserting against a synthetic error.
    ///
    /// This is deliberately a UNIT-level proof, not a `fetch_pending`-level
    /// one: `fetch_pending`'s own call site is `propagate_deletions(content_pool,
    /// account_id, &result.deleted).await?` (see that function, a few lines
    /// above this test module) — a bare `?` — and `routes::google_push::
    /// run_gmail_sync_for_email`'s own call to `fetch_pending` is ALSO a bare
    /// `?`. Both are structural, compiler-enforced guarantees: neither
    /// `fetch_pending` nor its caller can reach a `commit_pending` call on
    /// this path once `propagate_deletions` returns `Err`, so "the cursor is
    /// left unchanged" does not need its own separate runtime proof once
    /// THIS function is proven to return `Err` on a real failure — an
    /// end-to-end version would need a live (or mocked) Gmail `history.list`/
    /// `messages.get` HTTP round trip, infrastructure this crate does not
    /// have (see `backend/tests/fixtures/gmail/README.md`).
    #[tokio::test]
    async fn deletion_propagation_returns_err_when_the_graph_lookup_fails_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping deletion_propagation_returns_err_when_the_graph_lookup_fails_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "gws_g23_deletion_lookup_fails";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let owner = seed_user(&pool).await;
        let account_id = seed_account(&pool, owner).await;
        let graph_id = seed_graph(&pool, account_id).await;
        upsert_watched_thread(&pool, account_id, graph_id, owner, "thread-del-3", true, None).await.unwrap();
        sqlx::query(
            "INSERT INTO atoms (graph_id, source_type, source_id, content_blob_key, timestamp) \
             VALUES ($1, 'gmail', 'msg-del-3', 'blob-msg-del-3', now())",
        )
        .bind(graph_id)
        .execute(&pool)
        .await
        .unwrap();

        // Force a genuine SQL-level failure: the table `watched_thread_
        // graph_ids`' SELECT depends on is simply gone.
        sqlx::query("DROP TABLE gmail_watched_threads")
            .execute(&pool)
            .await
            .unwrap();

        let result =
            propagate_deletions(&pool, account_id, owner, AccountPlane::Personal, &[("thread-del-3".to_string(), "msg-del-3".to_string())]).await;
        assert!(
            result.is_err(),
            "a genuine graph-lookup failure must return Err, never swallow it as Ok(())"
        );

        // Belt-and-suspenders: the atom that WOULD have been deleted is
        // still there — the failure aborted before any delete_source call.
        let still_there: i64 =
            sqlx::query_scalar("SELECT count(*) FROM atoms WHERE source_type = 'gmail' AND source_id = 'msg-del-3'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(still_there, 1, "no deletion may have been attempted once the batch lookup itself failed");

        pool.close().await;
        let mut admin_conn = PgConnection::connect(&base).await.unwrap();
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    // ── gsuite Train G2 PR 7: the single-plane decision, proven across two
    // REAL physical databases ────────────────────────────────────────────
    //
    // Every test above runs `upsert_watch`/`upsert_watched_thread`/etc.
    // against ONE database — correct for what they prove, but it cannot
    // catch a regression that writes a Gmail watch row to the wrong plane
    // under Org, where content and control are genuinely different
    // databases. This test builds a real control database and a real tenant
    // database and proves, end to end: (1) the self-heal path's watch write
    // lands on CONTROL, never the content pool; (2) the push resolver
    // (`resolve_gmail_watch_owner`, control-only) finds it there — and the
    // identical query against the tenant pool structurally cannot (the
    // function doesn't exist there at all); (3) the watched-thread write
    // lands on CONTENT (tenant), never control; (4) `purge_google_connection`
    // purges each row from its own correct plane. The control-plane half
    // runs under the least-privilege `coralswarm_app` role — `0180`'s
    // `account_isolation` policy on `google_watch_channels` is real there
    // (unlike the tenant tree's twin, `0062`, which the migration's own
    // header says is "deliberately NOT covered") — matching the same
    // real-RLS-role discipline `workers::google_watch_renewal`'s sibling
    // cross-plane test uses, for the same reason.

    /// Org plane (`AccountPlane::Org`, a tenant tree): a member's deletion
    /// retracts every chunk of the message inside the graph the thread was
    /// ingested into — and nothing else in the tenant, even an atom with
    /// the same `source_id` in another member's ocean (the shape a promoted
    /// copy, or a test double, produces). The everything-mode fallback
    /// (tenant `gmail_everything_mode` row, no watched thread) is confined
    /// the same way, and the compliance audit row is written. Fails with
    /// `AccountPlane::Personal`: `delete_source` is account-wide and takes
    /// bob's copy too.
    #[tokio::test]
    async fn org_plane_deletion_propagation_retracts_only_inside_the_members_own_ocean_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping org_plane_deletion_propagation_retracts_only_inside_the_members_own_ocean_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "gws_gmail_org_plane_deletion_ten";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let tenant: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_tenant").run(&tenant).await.unwrap();
        sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, 'org_gmail_del')")
            .execute(&tenant)
            .await
            .unwrap();

        let account_id = Uuid::new_v4();
        let (alice, bob) = (Uuid::new_v4(), Uuid::new_v4());
        let mut graphs = Vec::new();
        for u in [alice, bob] {
            let g: Uuid = sqlx::query_scalar(
                "INSERT INTO graphs(account_id, name, kind, is_primary, owner_user_id) \
                 VALUES ($1, 'member ocean', 'member', true, $2) RETURNING id",
            )
            .bind(account_id)
            .bind(u)
            .fetch_one(&tenant)
            .await
            .unwrap();
            graphs.push(g);
        }
        let (alice_graph, bob_graph) = (graphs[0], graphs[1]);
        // Alice: a watched thread plus everything mode; both messages are
        // chunked exactly like `routes::ingest` stores them. Bob holds a
        // same-id row that must survive either retraction.
        upsert_watched_thread(&tenant, account_id, alice_graph, alice, "thread-org", true, None).await.unwrap();
        sqlx::query(
            "INSERT INTO gmail_everything_mode (account_id, owner_user_id, graph_id, enabled) VALUES ($1, $2, $3, TRUE)",
        )
        .bind(account_id)
        .bind(alice)
        .bind(alice_graph)
        .execute(&tenant)
        .await
        .unwrap();
        for (g, source_id) in [
            (alice_graph, "msg-watched:chunk:0"),
            (alice_graph, "msg-watched:chunk:1"),
            (alice_graph, "msg-everything:chunk:0"),
            (alice_graph, "msg-keep:chunk:0"),
            (bob_graph, "msg-watched:chunk:0"),
            (bob_graph, "msg-everything:chunk:0"),
        ] {
            sqlx::query(
                "INSERT INTO atoms (graph_id, source_type, source_id, content_blob_key, timestamp) \
                 VALUES ($1, 'gmail', $2, $3, now())",
            )
            .bind(g)
            .bind(source_id)
            .bind(format!("blob-{g}-{source_id}"))
            .execute(&tenant)
            .await
            .unwrap();
        }

        propagate_deletions(
            &tenant,
            account_id,
            alice,
            AccountPlane::Org,
            &[
                ("thread-org".to_string(), "msg-watched".to_string()),
                ("thread-unwatched".to_string(), "msg-everything".to_string()),
            ],
        )
        .await
        .unwrap();

        let alice_left: Vec<String> =
            sqlx::query_scalar("SELECT source_id FROM atoms WHERE graph_id = $1 ORDER BY source_id")
                .bind(alice_graph)
                .fetch_all(&tenant)
                .await
                .unwrap();
        assert_eq!(alice_left, vec!["msg-keep:chunk:0".to_string()], "every chunk of both deleted messages is gone from alice's ocean");
        let bob_left: i64 = sqlx::query_scalar("SELECT count(*) FROM atoms WHERE graph_id = $1")
            .bind(bob_graph)
            .fetch_one(&tenant)
            .await
            .unwrap();
        assert_eq!(bob_left, 2, "another member's same-id atoms must survive on the org plane");
        let audit: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM connector_audit_log WHERE account_id = $1 AND action = 'deletion_propagated'",
        )
        .bind(account_id)
        .fetch_one(&tenant)
        .await
        .unwrap();
        assert_eq!(audit, 2, "one compliance row per retracted message");

        tenant.close().await;
        let mut admin_conn = PgConnection::connect(&base).await.unwrap();
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    /// Scenario (a) of the review: the user saves ONE message of a
    /// conversation (`scope=message`, `follow=false` — the watched row
    /// covers the whole thread regardless), trashes the whole conversation,
    /// then restores it. The hide retracts the one saved message's corals
    /// and tombstones exactly that `(graph, message)`; the siblings that
    /// were never ingested leave no tombstone, so the restore has nothing to
    /// re-ingest for them and they fall through to the live-follow gate an
    /// unfollowed thread fails. The old coverage rule ("any watched row")
    /// re-ingested every sibling.
    #[tokio::test]
    async fn hide_tombstones_only_the_corals_it_retracted_and_restore_targets_exactly_those_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping hide_tombstones_only_the_corals_it_retracted_and_restore_targets_exactly_those_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "gws_gmail_hide_message_scope";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let owner = seed_user(&pool).await;
        let account_id = seed_account(&pool, owner).await;
        let graph_id = seed_graph(&pool, account_id).await;
        upsert_watched_thread(&pool, account_id, graph_id, owner, "thread-conv", false, Some(Utc::now())).await.unwrap();
        seed_message_chunks(&pool, graph_id, "msg-saved", 2).await;

        let conversation = vec![
            ("thread-conv".to_string(), "msg-saved".to_string()),
            ("thread-conv".to_string(), "msg-sib-1".to_string()),
            ("thread-conv".to_string(), "msg-sib-2".to_string()),
        ];
        propagate_hides(&pool, account_id, owner, &conversation).await.unwrap();

        assert!(gmail_atoms_in(&pool, graph_id).await.is_empty(), "the saved message's chunks are retracted");
        assert_eq!(
            tombstones(&pool, account_id).await,
            vec![(graph_id, "msg-saved".to_string())],
            "only the coral that was actually retracted is tombstoned — never-ingested siblings are not"
        );

        let restored_ids: Vec<String> = conversation.iter().map(|(_, m)| m.clone()).collect();
        let targets = hidden_messages_for(&pool, account_id, owner, &restored_ids).await.unwrap();
        assert_eq!(targets.len(), 1);
        assert_eq!(targets["msg-saved"], vec![graph_id], "the restore goes back into the ocean it came out of");
        assert!(
            watched_thread_ids_among(&pool, account_id, &["thread-conv".to_string()]).await.unwrap().is_empty(),
            "an un-tombstoned sibling takes the live-follow gate, which an unfollowed thread fails"
        );

        clear_hidden_message(&pool, account_id, graph_id, "msg-saved").await.unwrap();
        assert!(tombstones(&pool, account_id).await.is_empty());
        assert!(hidden_messages_for(&pool, account_id, owner, &restored_ids).await.unwrap().is_empty());
        assert!(hidden_messages_for(&pool, account_id, owner, &[]).await.unwrap().is_empty(), "no restores, no lookup");

        pool.close().await;
        let mut admin_conn = PgConnection::connect(&base).await.unwrap();
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    /// Scenario (b) of the review: a thread saved with `follow=false`, a
    /// new reply lands in Spam (never ingested), the user clicks "Not spam".
    /// Even with everything mode ON for the owner there is no tombstone, so
    /// the restore path has no route to ingest the reply other than the
    /// ordinary live-follow gate — which the unfollowed thread fails. The old
    /// rule covered it twice over (watched row, everything mode).
    #[tokio::test]
    async fn a_restored_reply_never_ingested_in_an_unfollowed_thread_has_no_tombstone_to_restore_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping a_restored_reply_never_ingested_in_an_unfollowed_thread_has_no_tombstone_to_restore_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "gws_gmail_restore_unfollowed_spam";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let owner = seed_user(&pool).await;
        let account_id = seed_account(&pool, owner).await;
        let graph_id = seed_graph(&pool, account_id).await;
        upsert_watched_thread(&pool, account_id, graph_id, owner, "thread-unfollowed", false, None).await.unwrap();
        sqlx::query(
            "INSERT INTO gmail_everything_mode (account_id, owner_user_id, graph_id, enabled) VALUES ($1, $2, $3, TRUE)",
        )
        .bind(account_id)
        .bind(owner)
        .bind(graph_id)
        .execute(&pool)
        .await
        .unwrap();
        seed_message_chunks(&pool, graph_id, "msg-older", 1).await;

        // The reply was never a coral; the pass that saw it in Spam dropped
        // it. A hide for it (Gmail reports SPAM both ways) retracts nothing
        // and must leave no tombstone behind.
        let reply = vec![("thread-unfollowed".to_string(), "msg-spam-reply".to_string())];
        propagate_hides(&pool, account_id, owner, &reply).await.unwrap();
        assert!(tombstones(&pool, account_id).await.is_empty());
        assert_eq!(gmail_atoms_in(&pool, graph_id).await, vec!["msg-older:chunk:0".to_string()]);

        let targets = hidden_messages_for(&pool, account_id, owner, &["msg-spam-reply".to_string()]).await.unwrap();
        assert!(targets.is_empty(), "nothing was retracted, so nothing is restored");
        assert!(
            watched_thread_ids_among(&pool, account_id, &["thread-unfollowed".to_string()]).await.unwrap().is_empty(),
            "and the live-follow gate the restore falls back to refuses an unfollowed thread"
        );

        pool.close().await;
        let mut admin_conn = PgConnection::connect(&base).await.unwrap();
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    /// Item 2 of the review, personal plane: a hide is graph-confined and
    /// leaves a copy a human promoted into another ocean alone (going through
    /// account-wide `delete_source`, as hides did, destroyed it — and the
    /// restore could never bring it back). The tombstone names the ocean the
    /// thread was saved into, not the primary one. A permanent
    /// `messagesDeleted` keeps today's semantics — every copy goes — and
    /// clears the tombstone.
    #[tokio::test]
    async fn hide_spares_promoted_copies_and_a_permanent_delete_still_takes_them_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping hide_spares_promoted_copies_and_a_permanent_delete_still_takes_them_pg: set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "gws_gmail_hide_promoted_copy";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let owner = seed_user(&pool).await;
        let account_id = seed_account(&pool, owner).await;
        let _primary = seed_graph(&pool, account_id).await;
        let saved_into = seed_secondary_graph(&pool, account_id, "Project ocean").await;
        let promoted_into = seed_secondary_graph(&pool, account_id, "Shared ocean").await;
        upsert_watched_thread(&pool, account_id, saved_into, owner, "thread-promo", true, None).await.unwrap();
        seed_message_chunks(&pool, saved_into, "msg-p", 2).await;
        seed_message_chunks(&pool, promoted_into, "msg-p", 1).await;

        let hide = vec![("thread-promo".to_string(), "msg-p".to_string())];
        propagate_hides(&pool, account_id, owner, &hide).await.unwrap();

        assert!(gmail_atoms_in(&pool, saved_into).await.is_empty(), "the connector's own copy is retracted");
        assert_eq!(
            gmail_atoms_in(&pool, promoted_into).await,
            vec!["msg-p:chunk:0".to_string()],
            "a copy a human promoted into another ocean survives a reversible hide"
        );
        assert_eq!(tombstones(&pool, account_id).await, vec![(saved_into, "msg-p".to_string())]);
        let targets = hidden_messages_for(&pool, account_id, owner, &["msg-p".to_string()]).await.unwrap();
        assert_eq!(targets["msg-p"], vec![saved_into], "the restore lands in the saved-into ocean, not the primary");

        propagate_deletions(&pool, account_id, owner, AccountPlane::Personal, &hide).await.unwrap();
        assert!(gmail_atoms_in(&pool, promoted_into).await.is_empty(), "a permanent delete still takes every copy");
        assert!(tombstones(&pool, account_id).await.is_empty(), "a permanent delete has nothing left to restore");

        pool.close().await;
        let mut admin_conn = PgConnection::connect(&base).await.unwrap();
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    /// A thread saved into one ocean while everything mode fills another:
    /// the hide takes the message out of BOTH (each is the connector's own
    /// copy), one tombstone per ocean, and the restore targets both. Replayed
    /// history is harmless: a second hide finds no coral and writes nothing
    /// new, clearing a tombstone twice is a no-op, and a hide for a thread
    /// nothing ever covered retracts nothing.
    #[tokio::test]
    async fn hide_covers_both_the_saved_into_and_the_everything_mode_ocean_and_replays_are_no_ops_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping hide_covers_both_the_saved_into_and_the_everything_mode_ocean_and_replays_are_no_ops_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "gws_gmail_hide_two_oceans";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let owner = seed_user(&pool).await;
        let account_id = seed_account(&pool, owner).await;
        let everything = seed_graph(&pool, account_id).await;
        let saved_into = seed_secondary_graph(&pool, account_id, "Project ocean").await;
        upsert_watched_thread(&pool, account_id, saved_into, owner, "thread-both", true, None).await.unwrap();
        sqlx::query(
            "INSERT INTO gmail_everything_mode (account_id, owner_user_id, graph_id, enabled) VALUES ($1, $2, $3, FALSE)",
        )
        .bind(account_id)
        .bind(owner)
        .bind(everything)
        .execute(&pool)
        .await
        .unwrap();
        seed_message_chunks(&pool, saved_into, "msg-both", 1).await;
        seed_message_chunks(&pool, everything, "msg-both", 1).await;
        seed_message_chunks(&pool, everything, "msg-keep", 1).await;

        let hide = vec![("thread-both".to_string(), "msg-both".to_string())];
        propagate_hides(&pool, account_id, owner, &hide).await.unwrap();
        assert!(gmail_atoms_in(&pool, saved_into).await.is_empty());
        assert_eq!(gmail_atoms_in(&pool, everything).await, vec!["msg-keep:chunk:0".to_string()]);
        let mut expected = vec![(saved_into, "msg-both".to_string()), (everything, "msg-both".to_string())];
        expected.sort();
        assert_eq!(tombstones(&pool, account_id).await, expected, "one tombstone per ocean the hide reached");
        let mut graphs = hidden_messages_for(&pool, account_id, owner, &["msg-both".to_string()]).await.unwrap()["msg-both"].clone();
        graphs.sort();
        let mut both = vec![saved_into, everything];
        both.sort();
        assert_eq!(graphs, both);

        propagate_hides(&pool, account_id, owner, &hide).await.unwrap();
        assert_eq!(tombstones(&pool, account_id).await.len(), 2, "a replayed hide adds nothing");
        let audit: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM connector_audit_log WHERE account_id = $1 AND action = 'deletion_propagated'",
        )
        .bind(account_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(audit, 2, "one compliance row per retraction that actually happened, none for the replay");

        let never = vec![("thread-never".to_string(), "msg-never".to_string())];
        propagate_hides(&pool, account_id, owner, &never).await.unwrap();
        assert_eq!(tombstones(&pool, account_id).await.len(), 2);

        clear_hidden_message(&pool, account_id, saved_into, "msg-both").await.unwrap();
        clear_hidden_message(&pool, account_id, saved_into, "msg-both").await.unwrap();
        assert_eq!(tombstones(&pool, account_id).await, vec![(everything, "msg-both".to_string())]);

        pool.close().await;
        let mut admin_conn = PgConnection::connect(&base).await.unwrap();
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    /// Org plane (a tenant tree): the hide is confined to the member's own
    /// ocean — another member's same-id atoms survive — and the tombstone
    /// lives on the tenant database next to the atoms (tenant migration
    /// 0116), so the restore can find it there.
    #[tokio::test]
    async fn org_plane_hide_tombstones_on_the_tenant_and_spares_another_members_ocean_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping org_plane_hide_tombstones_on_the_tenant_and_spares_another_members_ocean_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "gws_gmail_org_plane_hide_ten";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let tenant: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_tenant").run(&tenant).await.unwrap();
        sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, 'org_gmail_hide')")
            .execute(&tenant)
            .await
            .unwrap();

        let account_id = Uuid::new_v4();
        let (alice, bob) = (Uuid::new_v4(), Uuid::new_v4());
        let mut graphs = Vec::new();
        for u in [alice, bob] {
            let g: Uuid = sqlx::query_scalar(
                "INSERT INTO graphs(account_id, name, kind, is_primary, owner_user_id) \
                 VALUES ($1, 'member ocean', 'member', true, $2) RETURNING id",
            )
            .bind(account_id)
            .bind(u)
            .fetch_one(&tenant)
            .await
            .unwrap();
            graphs.push(g);
        }
        let (alice_graph, bob_graph) = (graphs[0], graphs[1]);
        upsert_watched_thread(&tenant, account_id, alice_graph, alice, "thread-org", true, None).await.unwrap();
        seed_message_chunks(&tenant, alice_graph, "msg-org", 2).await;
        seed_message_chunks(&tenant, bob_graph, "msg-org", 1).await;

        propagate_hides(&tenant, account_id, alice, &[("thread-org".to_string(), "msg-org".to_string())])
            .await
            .unwrap();

        assert!(gmail_atoms_in(&tenant, alice_graph).await.is_empty());
        assert_eq!(gmail_atoms_in(&tenant, bob_graph).await, vec!["msg-org:chunk:0".to_string()]);
        assert_eq!(tombstones(&tenant, account_id).await, vec![(alice_graph, "msg-org".to_string())]);
        let targets = hidden_messages_for(&tenant, account_id, alice, &["msg-org".to_string()]).await.unwrap();
        assert_eq!(targets["msg-org"], vec![alice_graph]);

        tenant.close().await;
        let mut admin_conn = PgConnection::connect(&base).await.unwrap();
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    /// Org plane: every member shares the org's one `account_id`, and the
    /// same mail (same Gmail message id) can sit in two members' oceans. A
    /// restore must only find the tombstones its own owner's hide wrote —
    /// keyed on `(account_id, message_id)` alone, bob's "untrash" would have
    /// targeted alice's ocean.
    #[tokio::test]
    async fn a_members_tombstone_is_invisible_to_another_members_restore_on_the_shared_org_account_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping a_members_tombstone_is_invisible_to_another_members_restore_on_the_shared_org_account_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "gws_gmail_org_restore_per_owner";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let tenant: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_tenant").run(&tenant).await.unwrap();
        sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, 'org_gmail_restore_owner')")
            .execute(&tenant)
            .await
            .unwrap();

        let account_id = Uuid::new_v4();
        let (alice, bob) = (Uuid::new_v4(), Uuid::new_v4());
        let mut graphs = Vec::new();
        for u in [alice, bob] {
            let g: Uuid = sqlx::query_scalar(
                "INSERT INTO graphs(account_id, name, kind, is_primary, owner_user_id) \
                 VALUES ($1, 'member ocean', 'member', true, $2) RETURNING id",
            )
            .bind(account_id)
            .bind(u)
            .fetch_one(&tenant)
            .await
            .unwrap();
            graphs.push(g);
        }
        let (alice_graph, bob_graph) = (graphs[0], graphs[1]);
        upsert_watched_thread(&tenant, account_id, alice_graph, alice, "thread-shared", true, None).await.unwrap();
        upsert_watched_thread(&tenant, account_id, bob_graph, bob, "thread-shared", true, None).await.unwrap();
        seed_message_chunks(&tenant, alice_graph, "msg-shared", 1).await;
        seed_message_chunks(&tenant, bob_graph, "msg-shared", 1).await;

        let hide = vec![("thread-shared".to_string(), "msg-shared".to_string())];
        propagate_hides(&tenant, account_id, alice, &hide).await.unwrap();
        let ids = vec!["msg-shared".to_string()];
        assert!(
            hidden_messages_for(&tenant, account_id, bob, &ids).await.unwrap().is_empty(),
            "bob never hid anything — alice's tombstone is not his restore target"
        );
        assert_eq!(hidden_messages_for(&tenant, account_id, alice, &ids).await.unwrap()["msg-shared"], vec![alice_graph]);

        propagate_hides(&tenant, account_id, bob, &hide).await.unwrap();
        assert_eq!(hidden_messages_for(&tenant, account_id, bob, &ids).await.unwrap()["msg-shared"], vec![bob_graph]);
        assert_eq!(
            hidden_messages_for(&tenant, account_id, alice, &ids).await.unwrap()["msg-shared"],
            vec![alice_graph],
            "each member restores into their own ocean only"
        );

        tenant.close().await;
        let mut admin_conn = PgConnection::connect(&base).await.unwrap();
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    /// A thread saved while ocean A was the primary, then the user made ocean
    /// B their primary: live-follow replies land in B
    /// (`resolve_primary_graph`), while the watched row still names A. The
    /// hide has to reach the reply in B — and still spares a promoted copy.
    #[tokio::test]
    async fn hide_reaches_a_reply_in_the_owners_current_primary_when_the_primary_moved_after_the_save_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping hide_reaches_a_reply_in_the_owners_current_primary_when_the_primary_moved_after_the_save_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "gws_gmail_hide_moved_primary";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let owner = seed_user(&pool).await;
        let account_id = seed_account(&pool, owner).await;
        let old_primary = seed_graph(&pool, account_id).await;
        upsert_watched_thread(&pool, account_id, old_primary, owner, "thread-moved", true, None).await.unwrap();
        seed_message_chunks(&pool, old_primary, "msg-first", 1).await;

        let new_primary = seed_secondary_graph(&pool, account_id, "New primary").await;
        sqlx::query("UPDATE graphs SET is_primary = false WHERE id = $1").bind(old_primary).execute(&pool).await.unwrap();
        sqlx::query("UPDATE graphs SET is_primary = true WHERE id = $1").bind(new_primary).execute(&pool).await.unwrap();
        seed_message_chunks(&pool, new_primary, "msg-reply", 2).await;
        let promoted_into = seed_secondary_graph(&pool, account_id, "Shared ocean").await;
        seed_message_chunks(&pool, promoted_into, "msg-reply", 1).await;

        propagate_hides(&pool, account_id, owner, &[("thread-moved".to_string(), "msg-reply".to_string())])
            .await
            .unwrap();

        assert!(gmail_atoms_in(&pool, new_primary).await.is_empty(), "the reply in the current primary is retracted");
        assert_eq!(gmail_atoms_in(&pool, old_primary).await, vec!["msg-first:chunk:0".to_string()]);
        assert_eq!(gmail_atoms_in(&pool, promoted_into).await, vec!["msg-reply:chunk:0".to_string()]);
        assert_eq!(tombstones(&pool, account_id).await, vec![(new_primary, "msg-reply".to_string())]);

        pool.close().await;
        let mut admin_conn = PgConnection::connect(&base).await.unwrap();
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    /// alice's three member oceans on a fresh tenant tree: `[primary, saved
    /// into, other]`, the first one primary.
    async fn seed_org_member_oceans(base: &str, db: &str, org_id: &str) -> (PgPool, Uuid, Uuid, [Uuid; 3]) {
        let conn = recreate(base, db).await;
        conn.close().await.ok();
        let tenant: PgPool = PgPoolOptions::new().connect(&with_db(base, db)).await.unwrap();
        sqlx::migrate!("./migrations_tenant").run(&tenant).await.unwrap();
        sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, $1)").bind(org_id).execute(&tenant).await.unwrap();
        let (account_id, alice) = (Uuid::new_v4(), Uuid::new_v4());
        let mut oceans = [Uuid::nil(); 3];
        for (i, name) in ["alice's primary", "alice's project ocean", "alice's other ocean"].into_iter().enumerate() {
            oceans[i] = sqlx::query_scalar(
                "INSERT INTO graphs(account_id, name, kind, is_primary, owner_user_id) \
                 VALUES ($1, $2, 'member', $3, $4) RETURNING id",
            )
            .bind(account_id)
            .bind(name)
            .bind(i == 0)
            .bind(alice)
            .fetch_one(&tenant)
            .await
            .unwrap();
        }
        (tenant, account_id, alice, oceans)
    }

    async fn switch_primary(pool: &PgPool, account_id: Uuid, graph_id: Uuid) {
        sqlx::query("UPDATE graphs SET is_primary = (id = $2) WHERE account_id = $1")
            .bind(account_id)
            .bind(graph_id)
            .execute(pool)
            .await
            .unwrap();
    }

    // Historical imports have no destination registry. Their attribution
    // must still confine retraction to the importing member.
    #[tokio::test]
    async fn org_plane_permanent_delete_reaches_a_reply_in_the_members_former_primary_pg() {
        org_legacy_reply_retraction_pg(true).await;
    }

    #[tokio::test]
    async fn org_plane_hide_reaches_a_legacy_reply_and_spares_other_members_and_promoted_copies_pg() {
        org_legacy_reply_retraction_pg(false).await;
    }

    async fn org_legacy_reply_retraction_pg(permanent: bool) {
        let Some(base) = base_url() else {
            eprintln!("skipping org_plane_permanent_delete_reaches_a_reply_in_the_members_former_primary_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = if permanent { "gws_gmail_org_delete_primary_reply" } else { "gws_gmail_org_hide_primary_reply" };
        let (tenant, account_id, alice, [primary, saved_into, next_primary]) =
            seed_org_member_oceans(&base, db, "org_gmail_delete_primary_reply").await;
        let bob = Uuid::new_v4();
        let bob_graph: Uuid = sqlx::query_scalar(
            "INSERT INTO graphs(account_id, name, kind, is_primary, owner_user_id) \
             VALUES ($1, 'bob', 'member', true, $2) RETURNING id",
        )
        .bind(account_id)
        .bind(bob)
        .fetch_one(&tenant)
        .await
        .unwrap();
        upsert_watched_thread(&tenant, account_id, saved_into, alice, "thread-o", true, None).await.unwrap();
        seed_message_chunks(&tenant, saved_into, "msg-first", 1).await;
        seed_message_chunks(&tenant, primary, "msg-reply", 2).await;
        seed_message_chunks(&tenant, bob_graph, "msg-reply", 1).await;
        let team: Uuid = sqlx::query_scalar(
            "INSERT INTO graphs(account_id, name, kind, is_primary) VALUES ($1, 'team copy', 'team', false) RETURNING id",
        ).bind(account_id).fetch_one(&tenant).await.unwrap();
        seed_message_chunks(&tenant, team, "msg-reply", 1).await;
        let promoted: Uuid = sqlx::query_scalar(
            "INSERT INTO graphs(account_id, name, kind, is_primary, owner_user_id) VALUES ($1, 'alice copy', 'member', false, $2) RETURNING id",
        ).bind(account_id).bind(alice).fetch_one(&tenant).await.unwrap();
        seed_message_chunks(&tenant, promoted, "msg-reply", 1).await;
        sqlx::query("UPDATE atoms SET ingested_by = $1 WHERE source_type = 'gmail'")
            .bind(alice).execute(&tenant).await.unwrap();
        sqlx::query("UPDATE atoms SET promoted_from_atom_id = (SELECT atom_id FROM atoms WHERE graph_id = $1 LIMIT 1) WHERE graph_id = $2")
            .bind(primary).bind(promoted).execute(&tenant).await.unwrap();
        switch_primary(&tenant, account_id, next_primary).await;
        let changes = [("thread-o".to_string(), "msg-reply".to_string())];
        if permanent {
            propagate_deletions(&tenant, account_id, alice, AccountPlane::Org, &changes).await.unwrap();
        } else {
            propagate_hides(&tenant, account_id, alice, &changes).await.unwrap();
            assert_eq!(tombstones(&tenant, account_id).await, vec![(primary, "msg-reply".into())]);
        }

        assert!(gmail_atoms_in(&tenant, primary).await.is_empty(), "the deleted reply leaves alice's primary");
        assert_eq!(gmail_atoms_in(&tenant, saved_into).await, vec!["msg-first:chunk:0".to_string()]);
        assert_eq!(gmail_atoms_in(&tenant, bob_graph).await, vec!["msg-reply:chunk:0".to_string()], "bob's copy survives");
        assert_eq!(gmail_atoms_in(&tenant, team).await, vec!["msg-reply:chunk:0".to_string()], "a legacy team copy with no marker survives");
        assert_eq!(gmail_atoms_in(&tenant, promoted).await, vec!["msg-reply:chunk:0".to_string()], "a marked copy in another owned ocean survives");

        tenant.close().await;
        let mut admin_conn = PgConnection::connect(&base).await.unwrap();
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    #[tokio::test]
    async fn a_resaved_thread_retracts_originals_from_every_saved_ocean_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping a_resaved_thread_retracts_originals_from_every_saved_ocean_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "gws_gmail_resaved_destinations";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let (pool, account_id, owner, [first, second, _]) = seed_org_member_oceans(&base, db, "org_gmail_resaved").await;
        for graph in [first, second, second] {
            upsert_watched_thread(&pool, account_id, graph, owner, "thread-resave", false, None).await.unwrap();
        }
        seed_message_chunks(&pool, first, "msg-resaved", 1).await;
        seed_message_chunks(&pool, second, "msg-resaved", 2).await;
        sqlx::raw_sql(include_str!("../../migrations_tenant/0121_gmail_watched_threads_landing_graphs.sql"))
            .execute(&pool).await.unwrap();
        propagate_hides(&pool, account_id, owner, &[("thread-resave".into(), "msg-resaved".into())]).await.unwrap();
        assert!(gmail_atoms_in(&pool, first).await.is_empty());
        assert!(gmail_atoms_in(&pool, second).await.is_empty());
        let targets = hidden_messages_for(&pool, account_id, owner, &["msg-resaved".into()]).await.unwrap();
        let mut actual = targets["msg-resaved"].clone();
        actual.sort_unstable();
        let mut expected = vec![first, second];
        expected.sort_unstable();
        assert_eq!(actual, expected, "both explicit saves remain restore destinations after unfollow");
        pool.close().await;
        let mut admin_conn = PgConnection::connect(&base).await.unwrap();
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.unwrap();
    }

    #[tokio::test]
    async fn personal_delete_reaches_a_former_primary_reply_after_the_saved_ocean_is_deleted_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping personal_delete_reaches_a_former_primary_reply_after_the_saved_ocean_is_deleted_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "gws_gmail_personal_orphan_reply";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.unwrap();
        let pool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();
        let owner = seed_user(&pool).await;
        let account_id = seed_account(&pool, owner).await;
        let former_primary = seed_graph(&pool, account_id).await;
        let saved_into = seed_secondary_graph(&pool, account_id, "Saved ocean").await;
        let next_primary = seed_secondary_graph(&pool, account_id, "Next primary").await;
        let promoted = seed_secondary_graph(&pool, account_id, "Promoted copy").await;
        upsert_watched_thread(&pool, account_id, saved_into, owner, "thread-orphan", true, None).await.unwrap();
        record_reply_landing_graph(&pool, account_id, owner, "thread-orphan", former_primary).await.unwrap();
        seed_message_chunks(&pool, former_primary, "msg-orphan", 2).await;
        seed_message_chunks(&pool, promoted, "msg-orphan", 1).await;
        seed_message_chunks(&pool, next_primary, "msg-keep", 1).await;
        sqlx::query("UPDATE atoms SET ingested_by = $1 WHERE source_type = 'gmail'")
            .bind(owner).execute(&pool).await.unwrap();
        sqlx::query("UPDATE atoms SET promoted_from_atom_id = (SELECT atom_id FROM atoms WHERE graph_id = $1 LIMIT 1) WHERE graph_id = $2")
            .bind(former_primary).bind(promoted).execute(&pool).await.unwrap();
        switch_primary(&pool, account_id, next_primary).await;
        sqlx::query("DELETE FROM graphs WHERE id = $1").bind(saved_into).execute(&pool).await.unwrap();
        let watched: i64 = sqlx::query_scalar("SELECT count(*) FROM gmail_watched_threads WHERE account_id = $1")
            .bind(account_id).fetch_one(&pool).await.unwrap();
        assert_eq!(watched, 0, "deleting the saved ocean cascades its watched registry");
        propagate_deletions(&pool, account_id, owner, AccountPlane::Personal, &[("thread-orphan".into(), "msg-orphan".into())]).await.unwrap();
        assert!(gmail_atoms_in(&pool, former_primary).await.is_empty(), "personal permanent delete reaches the original reply");
        assert!(gmail_atoms_in(&pool, promoted).await.is_empty(), "personal permanent deletion still removes every copy");
        assert_eq!(gmail_atoms_in(&pool, next_primary).await, vec!["msg-keep:chunk:0".to_string()]);
        pool.close().await;
        let mut admin_conn = PgConnection::connect(&base).await.unwrap();
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.unwrap();
    }

    // `app_role_pool` — shared with `workers::google_watch_renewal`'s own
    // cross-plane test, `crate::app_role_test_support` (gsuite Train G2 PR 7,
    // CodeRabbit round 1: was a byte-identical local copy in each file).
    use crate::app_role_test_support::app_role_pool;

    #[tokio::test]
    async fn org_plane_self_heal_lands_on_control_push_resolver_finds_it_disconnect_purges_it_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping org_plane_self_heal_lands_on_control_push_resolver_finds_it_disconnect_purges_it_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let control_db = "gws_g27_gmail_org_control";
        let _cleanup_control_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", control_db);
        let tenant_db = "gws_g27_gmail_org_tenant";
        let _cleanup_tenant_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", tenant_db);
        let _database_cleanup_0 = crate::test_db::TestDatabase::adopt_from_url(&base, control_db);
        let control_conn = recreate(&base, control_db).await;
        control_conn.close().await.ok();
        let _database_cleanup_1 = crate::test_db::TestDatabase::adopt_from_url(&base, tenant_db);
        let tenant_conn = recreate(&base, tenant_db).await;
        tenant_conn.close().await.ok();

        let control_url = with_db(&base, control_db);
        let control_owner: PgPool = PgPoolOptions::new().connect(&control_url).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&control_owner).await.unwrap();

        let tenant_url = with_db(&base, tenant_db);
        let tenant_pool: PgPool = PgPoolOptions::new().connect(&tenant_url).await.unwrap();
        sqlx::migrate!("./migrations_tenant").run(&tenant_pool).await.unwrap();
        sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, 'org_g27_gmail_watch_plane')")
            .execute(&tenant_pool)
            .await
            .unwrap();

        let owner = seed_user(&control_owner).await;
        let account_id = seed_account(&control_owner, owner).await;
        let email = "org-member@example.com";
        let far_future = Utc::now() + chrono::Duration::days(7);

        let control_app_pool = app_role_pool(&control_owner, &control_url).await;

        // (1) Self-heal write (`upsert_watch`, what `ensure_watch` calls)
        // lands on CONTROL, under the real coralswarm_app role.
        upsert_watch(
            &control_app_pool,
            account_id,
            owner,
            email,
            "projects/p/topics/gmail-push",
            far_future,
            "1000",
        )
        .await
        .expect("self-heal watch write under the coralswarm_app role");

        let on_control: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM google_watch_channels WHERE account_id = $1 AND surface = 'gmail'",
        )
        .bind(account_id)
        .fetch_one(&control_owner)
        .await
        .unwrap();
        assert_eq!(on_control, 1, "the watch row must land on control");

        let on_tenant: i64 =
            sqlx::query_scalar("SELECT count(*) FROM google_watch_channels WHERE account_id = $1")
                .bind(account_id)
                .fetch_one(&tenant_pool)
                .await
                .unwrap();
        assert_eq!(on_tenant, 0, "the watch row must NEVER land on the content/tenant plane");

        // (2) Push resolver finds it on control...
        let resolved = resolve_watch_owners_for_push(&control_app_pool, email).await.unwrap();
        assert_eq!(resolved, vec![(account_id, owner)], "push resolver must find the row on control");

        // ...and the identical query structurally cannot succeed against the
        // tenant pool: `resolve_gmail_watch_owners` (migration 0275) has no
        // tenant-tree twin at all.
        let via_tenant: Result<Vec<(Uuid, Uuid)>, sqlx::Error> =
            sqlx::query_as("SELECT * FROM resolve_gmail_watch_owners($1)")
                .bind(email)
                .fetch_all(&tenant_pool)
                .await;
        assert!(
            via_tenant.is_err(),
            "the push-routing function must not even exist on the tenant plane"
        );

        // (3) Watched-thread write (`upsert_watched_thread`, what
        // `after_ingest` calls) lands on CONTENT (tenant), never control.
        let graph_id = seed_graph(&tenant_pool, account_id).await;
        upsert_watched_thread(&tenant_pool, account_id, graph_id, owner, "thread-org-1", true, None)
            .await
            .expect("watched-thread write on the content/tenant plane");

        let thread_on_tenant: i64 = sqlx::query_scalar("SELECT count(*) FROM gmail_watched_threads WHERE account_id = $1")
            .bind(account_id)
            .fetch_one(&tenant_pool)
            .await
            .unwrap();
        assert_eq!(thread_on_tenant, 1, "the watched-thread row must land on content/tenant");

        let thread_on_control: i64 =
            sqlx::query_scalar("SELECT count(*) FROM gmail_watched_threads WHERE account_id = $1")
                .bind(account_id)
                .fetch_one(&control_owner)
                .await
                .unwrap();
        assert_eq!(thread_on_control, 0, "the watched-thread row must NEVER land on control");

        // (4) Disconnect purges each row from its own correct plane.
        let control_now: DateTime<Utc> = sqlx::query_scalar("SELECT now()").fetch_one(&control_owner).await.unwrap();
        let tenant_now: DateTime<Utc> = sqlx::query_scalar("SELECT now()").fetch_one(&tenant_pool).await.unwrap();
        let outcome = crate::connectors::google_disconnect::purge_google_connection(
            &control_owner,
            &tenant_pool,
            account_id,
            crate::connectors::google::Surface::Gmail,
            crate::connectors::TokenOwner::Member(owner),
            control_now,
            tenant_now,
        )
        .await;
        assert_eq!(outcome.watch_channels_deleted, 1, "the control-plane watch row must be purged");
        assert_eq!(outcome.watched_threads_deleted, 1, "the content-plane thread row must be purged");
        assert!(!outcome.purge_had_a_failure());

        let control_after: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM google_watch_channels WHERE account_id = $1 AND surface = 'gmail'",
        )
        .bind(account_id)
        .fetch_one(&control_owner)
        .await
        .unwrap();
        assert_eq!(control_after, 0);
        let tenant_after: i64 = sqlx::query_scalar("SELECT count(*) FROM gmail_watched_threads WHERE account_id = $1")
            .bind(account_id)
            .fetch_one(&tenant_pool)
            .await
            .unwrap();
        assert_eq!(tenant_after, 0);

        control_owner.close().await;
        control_app_pool.close().await;
        tenant_pool.close().await;
        let mut admin_conn = PgConnection::connect(&base).await.unwrap();
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {control_db} WITH (FORCE)").as_str())
            .await
            .ok();
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {tenant_db} WITH (FORCE)").as_str())
            .await
            .ok();
    }

    /// Personal plane, byte-identical: the SAME sequence — self-heal write,
    /// push resolver, watched-thread write, disconnect purge — against ONE
    /// database standing in for both planes (control_pool == content_pool,
    /// exactly how Personal tenancy resolves them —
    /// `tenancy::extractor::Tenant`'s own doc). Proves this PR's split is
    /// genuinely transparent for the only tenancy Google Workspace runs
    /// under in production today.
    #[tokio::test]
    async fn personal_plane_self_heal_push_resolve_and_disconnect_purge_are_byte_identical_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping personal_plane_self_heal_push_resolve_and_disconnect_purge_are_byte_identical_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "gws_g27_gmail_personal_plane";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let owner = seed_user(&pool).await;
        let account_id = seed_account(&pool, owner).await;
        let graph_id = seed_graph(&pool, account_id).await;
        let email = "solo@example.com";
        let far_future = Utc::now() + chrono::Duration::days(7);

        upsert_watch(&pool, account_id, owner, email, "projects/p/topics/gmail-push", far_future, "1000")
            .await
            .unwrap();
        let resolved = resolve_watch_owners_for_push(&pool, email).await.unwrap();
        assert_eq!(resolved, vec![(account_id, owner)]);

        upsert_watched_thread(&pool, account_id, graph_id, owner, "thread-personal-1", true, None).await.unwrap();

        let now: DateTime<Utc> = sqlx::query_scalar("SELECT now()").fetch_one(&pool).await.unwrap();
        let outcome = crate::connectors::google_disconnect::purge_google_connection(
            &pool,
            &pool,
            account_id,
            crate::connectors::google::Surface::Gmail,
            crate::connectors::TokenOwner::Member(owner),
            now,
            now,
        )
        .await;
        assert_eq!(outcome.watch_channels_deleted, 1);
        assert_eq!(outcome.watched_threads_deleted, 1);
        assert!(!outcome.purge_had_a_failure());

        let remaining_channels: i64 =
            sqlx::query_scalar("SELECT count(*) FROM google_watch_channels WHERE account_id = $1")
                .bind(account_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(remaining_channels, 0);
        let remaining_threads: i64 =
            sqlx::query_scalar("SELECT count(*) FROM gmail_watched_threads WHERE account_id = $1")
                .bind(account_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(remaining_threads, 0);

        pool.close().await;
        let mut admin_conn = PgConnection::connect(&base).await.unwrap();
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }
}
