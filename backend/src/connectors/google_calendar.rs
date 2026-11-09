//! Google Calendar ingest — events as atoms (gsuite Train G3 PR 1,
//! `.scratch/gsuite-enterprise/plan.md`).
//!
//! # Founder decision (2026-08-23)
//! Calendar events ARE ingested as atoms, in the owner's private ocean, via
//! `ConnectorProvider::sync()`. Contract §7 Calendar archetype: C1 (container)
//! and C3 (roster) are absent, C4 (author) is trivial — the account owner is
//! the only "author" a self-owned calendar has — so this is self-owned,
//! non-sensitive data the connecting member implicitly consents to by
//! connecting, exactly the case `sync()` exists for (contract §4's own
//! framing of the rule). See `docs/connectors/CONNECTOR_CONTRACT.md` §7.
//!
//! # What this module owns
//! - [`fetch_pending`] + [`commit_pending`] — the actual Calendar ingest:
//!   incremental sync via Google's `syncToken`, with a resumable,
//!   budget-capped full backfill on first run (or after a `410 GONE` token
//!   expiry), split so the caller commits the cursor only after every
//!   fetched event ingested. Called from the per-plane passes in
//!   `workers::google_calendar_sync` (sweep, push webhook and the manual
//!   route alike).
//! - [`ensure_watch`] — creates (idempotently) the Calendar `events.watch`
//!   push channel and persists it to `google_watch_channels`
//!   (`migrations_control/0180_google_watch_channels.sql` +
//!   `0182_google_calendar_sync_state.sql`). Called from
//!   `GoogleConnector::post_store` on connect, and opportunistically from
//!   every [`sync`] call (see "Watch bootstrap" below).
//! - Deletion propagation for cancelled events (decision 9), called inline
//!   from [`sync`] — see "Deletions" below.
//!
//! `routes::google_calendar_push` (the Calendar webhook, a separate route
//! from the Pub/Sub `routes::google_push`) calls [`sync`] and
//! [`account_owner_user`] directly to run a synchronous, personal-plane
//! ingest pass when a push notification arrives — see that module's own doc.
//!
//! # Org lift (Google Calendar org workspaces): per-member on the org plane
//! The section below records the ORIGINAL (G3 PR 1) decision, which still
//! holds for the PERSONAL plane. As of the Google Calendar org lift, an ORG
//! account stores `TokenOwner::Member(user_id)` under the same `"google"`
//! provider literal (`Surface::calendar_token_owner`) — exactly Microsoft
//! Calendar's plane-split precedent (`resolve_microsoft_token_target`). Every
//! function here therefore takes a [`CalendarTarget`]: `owner: None` is the
//! unchanged personal shape; `owner: Some(uid)` is one org member, whose
//! sync cursor/watch channel is their OWN `google_watch_channels` row
//! (`resource_id = "primary:<uid>"`, `owner_user_id = <uid>`), whose events
//! land in THEIR member-private ocean on the org TENANT pool, and whose
//! deletions propagate against that same ocean. **Watch rows stay on the
//! CONTROL plane for org members too** (the gsuite Train G2 PR 7 settlement,
//! `connectors::google_gmail`'s "Watch rows are CONTROL-plane, always"): the
//! Calendar push notification carries only a bare channel id, and the only
//! way to route it to an org account is the control-only `SECURITY DEFINER`
//! resolver `resolve_google_watch_channel_account_id` (migration `0183`) — a
//! row that lived only on a tenant database would be structurally invisible
//! to push dispatch. Hence [`CalendarTarget`] carries TWO pools: `control`
//! (watch rows) and `content` (atoms/graphs/deletions); on the personal
//! plane they are the same database.
//!
//! # Design decision (original, personal plane): `TokenOwner::AccountWide`
//! The task that kicked off this PR asks for Calendar tokens to route to
//! "the owner's private ocean" per member. This module honors that
//! DESTINATION requirement (every event lands in the connecting account's
//! own ocean, never a shared one) without introducing per-member OAuth
//! STORAGE, because that storage model does not exist for Google anywhere in
//! this codebase yet: `docs/connectors/google-workspace.md` §2/§5 already
//! record that `TokenOwner::Member` isolation for Google is Train G2+ work
//! (unbuilt), and both `routes::calendar` and `routes::connectors` already
//! refuse every Google/connector route outright under an org
//! `TenancyDecision` (`reject_org_calendar_access` /
//! `reject_org_connector_access` — "personal-workspace-only in v1"). A
//! Personal account has exactly one owner, so `TokenOwner::AccountWide` and
//! "the owner's own token" are the same thing today. Building genuine
//! per-member Google token storage in this PR — with no migration/backfill
//! plan for it, and without lifting the org gate anywhere — would be
//! unrequested scope expansion that contradicts the task's own instruction
//! to keep the OAuth/authorize paths byte-identical. This is documented
//! again at the PR-body level as an explicit, deliberate deviation.
//!
//! # Recurring events: master + exceptions, not expanded instances
//! Ingest lists with `singleEvents=false` (deliberately DIFFERENT from
//! `routes::calendar::upcoming`'s `singleEvents=true`, which stays
//! unchanged — see that route's own module doc; it needs literal upcoming
//! occurrence timestamps for the pre-meeting nudge, a different consumer
//! with a different requirement). With `singleEvents=false`, a recurring
//! series appears ONCE as its master event (stable `source_id` = the series
//! id, `start` = the first occurrence), and only individually modified or
//! cancelled occurrences appear as their own separate "exception" entries
//! (`recurringEventId` set, per [`super::google::GEvent::recurring_event_id`]).
//! This mirrors `connectors::microsoft::shape_graph_event`'s shape exactly: Graph's
//! `/me/events` (which Microsoft's calendar `sync()` already uses) likewise
//! returns unexpanded series, one atom per master. Choosing the SAME model
//! for Google keeps "one calendar event → how many atoms" consistent across
//! both connectors instead of one exploding a recurring standup into
//! hundreds of near-duplicate atoms while the other doesn't.
//!
//! # Incremental sync + resumable backfill
//! Google's Calendar `events.list` supports two cursor styles, both handled
//! uniformly here via [`Cursor`]/[`run_pass`]:
//!
//! * **`syncToken`** — an opaque incremental-diff cursor. Once a full listing
//!   has been paginated to its terminal page, that page carries a
//!   `nextSyncToken`; every future call passes it back as `syncToken=...` and
//!   receives only what changed since, INCLUDING cancellations
//!   (`status: "cancelled"`, requires `showDeleted=true`, which this module
//!   always sets).
//! * **`pageToken`** — Google's ordinary pagination cursor. It is
//!   self-describing: once you have one (whether the listing that produced it
//!   started `syncToken`-based or from scratch), passing it alone continues
//!   exactly that listing — you never need to re-supply the original
//!   `syncToken`. This is why [`google_watch_channels.backfill_page_token`]
//!   doubles as the resume cursor for BOTH a from-scratch full backfill AND
//!   an incremental (`syncToken`) pass that hit its own per-call budget
//!   before reaching the terminal page — one column, one meaning ("resume
//!   the in-flight `events.list` pagination"), regardless of how that
//!   pagination was originally started.
//!
//! Each `sync()` call caps itself at [`SYNC_PASS_BUDGET`] events (mirrors
//! `connectors::microsoft::BACKFILL_SCAN_CAP`'s identical reasoning: an
//! unbounded single HTTP request is an operational hazard, not a contract
//! requirement — contract §6 test #6 ("no silent truncation") is about never
//! PERMANENTLY dropping data, which a persisted, explicit resume cursor
//! already satisfies). Hitting the budget mid-pass persists
//! `backfill_page_token` and returns what was fetched so far;
//! `backfill_complete` stays `false` until a pass actually reaches Google's
//! terminal page and receives a `nextSyncToken`.
//!
//! On a `410 GONE` (the stored `syncToken` — or a stale in-flight
//! `pageToken` — has expired upstream, e.g. after 7+ days or if Calendar
//! purges old sync sessions), [`sync`] clears the stored cursor state and
//! restarts a full backfill from scratch, once, in the same call — this is
//! the "on 410 GONE sync-token expiry, restart full sync" requirement.
//!
//! # Watch bootstrap ("backfill-safe path" for already-connected members)
//! [`ensure_watch`] is idempotent (a no-op once a live channel already
//! exists, checked via [`channel_is_live`]) and called from TWO places:
//! `GoogleConnector::post_store` on a fresh connect (the normal case), and
//! the START of every [`sync`] call (best-effort — a failure here never
//! blocks event ingestion). The second call site is what makes this
//! "backfill-safe" for a connection whose connect-time bootstrap failed
//! (e.g. `GOOGLE_CALENDAR_WEBHOOK_URL` unset at the time): the next
//! [`sync`]/[`fetch_pending`] call creates the missing channel — and that
//! next call is GUARANTEED, not hoped for, because `workers::
//! google_calendar_sync` discovers every connection on both planes from its
//! `oauth_tokens` row (never from `google_watch_channels`, which such a
//! connection has no row in) and runs a pass every 30 minutes. The FIRST
//! such pass also writes the connection's `google_watch_channels` row for
//! its sync-token state with `expires_at = now()` (see
//! [`upsert_sync_columns`]'s doc), after which `workers::google_watch_
//! renewal` keeps the channel alive via its own `renew_calendar` (the SAME
//! `events.watch` POST as a "renewal" — Calendar has no separate create-vs-
//! renew distinction upstream). The renewal sweep alone was never a retry
//! path for a missing channel: it can only see rows that already exist.
//!
//! # Deletions (decision 9)
//! An event with `status: "cancelled"` in an incremental or backfill page is
//! never turned into an atom; its id is instead collected and, once the pass
//! completes, handed to `services::deletion::delete_source` — the SAME
//! cascade the platform's compliance story already uses (retention-policy
//! bypass argument, `.scratch/m365-enterprise/plan.md` decision 9). This
//! module is the first CONNECTOR to call `delete_source` directly rather
//! than through the authenticated HTTP route (`routes::deletion::
//! delete_source_route`, which gates on ordinary graph reachability, checked
//! by the route layer before it is called) — that gate does not apply here
//! because this call never crosses account boundaries: the event being
//! deleted, and the event's own origin graph (the SAME account's private
//! ocean resolved via [`account_owner_user`] + `resolve_primary_graph`), are
//! always the account that owns the Calendar connection that produced the
//! cancellation notice in the first place.

use chrono::{DateTime, Utc};
use serde::Deserialize;
use sqlx::PgPool;
use uuid::Uuid;

use super::google::{extract_meeting_url, parse_g_time, GEvent, GEventsList};
use super::{ConnectorError, CredentialDeathReason, NormalizedItem};

/// `atoms.source_type` for a Google Calendar event — drives the generic
/// chunker (no `ChunkStrategy` special-case exists for it, same as
/// `"microsoft_calendar"`) and `GoogleConnector::source_types`.
pub(crate) const SOURCE_TYPE: &str = "google_calendar";

/// The user's primary calendar — the only calendar this connector ingests
/// (mirrors `google.rs::EVENTS_ENDPOINT`'s own scope: `calendar.readonly`
/// covers it without a separate calendar-list call).
const CALENDAR_ID: &str = "primary";

const EVENTS_URL: &str = "https://www.googleapis.com/calendar/v3/calendars/primary/events";
const WATCH_URL: &str = "https://www.googleapis.com/calendar/v3/calendars/primary/events/watch";

/// Page size for `events.list`. Well under Google's own max (2500) — this is
/// a per-REQUEST size, not a cap on total events fetched (see
/// [`SYNC_PASS_BUDGET`] for the per-`sync()`-call cap, which spans multiple
/// pages).
const LIST_PAGE_SIZE: usize = 250;

/// Max events (created/updated + cancelled) processed in one [`sync`] call
/// before persisting a resume cursor and returning — mirrors
/// `connectors::microsoft::BACKFILL_SCAN_CAP`'s exact reasoning (module doc,
/// "Incremental sync + resumable backfill").
const SYNC_PASS_BUDGET: usize = 2000;

/// Re-create the watch channel if it expires within this margin — small and
/// deliberately independent of `workers::google_watch_renewal::
/// RENEW_WINDOW_HOURS` (24h): this is just "is there obviously nothing to do
/// right now" so a hot `sync()` path doesn't pay for a Google API round trip
/// on every call once a channel is freshly created; the renewal worker still
/// owns the real keep-alive schedule.
const CHANNEL_LIVE_MARGIN_MINUTES: i64 = 30;

// ---------------------------------------------------------------------------
// Target — which account/owner/plane a Calendar pass is for
// ---------------------------------------------------------------------------

/// Who a Calendar sync/watch operation is for, and which databases it
/// touches. See the module doc's "Org lift" section.
///
/// * `control` — owns `google_watch_channels` (cursor + push channel) and
///   `oauth_tokens`. Always the control-plane pool.
/// * `content` — owns the ingested atoms/graphs (deletion propagation).
///   The consumer pool on the personal plane (same database as `control`);
///   the org's TENANT pool on the org plane.
/// * `owner` — `None` = the personal account-wide connection (unchanged
///   behavior); `Some(user_id)` = one org member's own connection.
#[derive(Clone, Copy)]
pub(crate) struct CalendarTarget<'a> {
    pub(crate) control: &'a PgPool,
    pub(crate) content: &'a PgPool,
    pub(crate) account_id: Uuid,
    pub(crate) owner: Option<Uuid>,
}

impl<'a> CalendarTarget<'a> {
    /// The unchanged personal-plane shape: one pool, account-wide.
    pub(crate) fn personal(pool: &'a PgPool, account_id: Uuid) -> Self {
        Self { control: pool, content: pool, account_id, owner: None }
    }

    /// One org member's own connection.
    pub(crate) fn org_member(
        control: &'a PgPool,
        content: &'a PgPool,
        account_id: Uuid,
        owner: Uuid,
    ) -> Self {
        Self { control, content, account_id, owner: Some(owner) }
    }

    /// The `google_watch_channels.resource_id` this target's row is keyed on.
    /// `"primary"` for the personal account-wide connection (byte-identical
    /// to every existing row); `"primary:<user_id>"` for an org member, so N
    /// members of one account get N rows under the table's
    /// `UNIQUE (account_id, surface, resource_id)`.
    pub(crate) fn resource_id(&self) -> String {
        match self.owner {
            None => CALENDAR_ID.to_string(),
            Some(uid) => format!("{CALENDAR_ID}:{uid}"),
        }
    }
}

/// The Google calendar id a stored Calendar `resource_id` refers to — the
/// part before the `:<user_id>` suffix an org member's row carries
/// ([`CalendarTarget::resource_id`]). `workers::google_watch_renewal` posts
/// `events.watch` against this, so a member suffix must never reach Google's
/// URL path.
pub(crate) fn calendar_id_of(resource_id: &str) -> &str {
    resource_id.split(':').next().unwrap_or(resource_id)
}

// ---------------------------------------------------------------------------
// RLS scoping — mirrors `connectors::microsoft::scoped_tx` /
// `workers::google_watch_renewal::scoped_tx` exactly (see either's own doc
// comment): background/webhook-driven work has no single acting end user to
// present, so every `google_watch_channels` read/write here goes through
// `begin_tenant_system_scoped`, never a bare pool query — `0180`'s
// `account_isolation` RLS policy would otherwise silently zero-row every
// call once connected as the least-privilege `coralswarm_app` role.
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

// ---------------------------------------------------------------------------
// Watch (events.watch channel) bootstrap
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct WatchChannelResponse {
    id: String,
    #[serde(rename = "resourceId")]
    resource_id: String,
    /// Milliseconds-since-epoch, as a decimal string (same convention Gmail's
    /// `users.watch` and `workers::google_watch_renewal`'s Calendar renewal
    /// already use).
    expiration: String,
}

fn parse_epoch_millis(s: &str) -> Result<DateTime<Utc>, ConnectorError> {
    let millis: i64 = s
        .parse()
        .map_err(|e| ConnectorError::Exchange(format!("epoch millis parse {s:?}: {e}")))?;
    DateTime::from_timestamp_millis(millis)
        .ok_or_else(|| ConnectorError::Exchange(format!("epoch millis out of range: {millis}")))
}

/// 256 bits of opaque secret Google echoes back verbatim on every channel
/// notification (`X-Goog-Channel-Token`) — constant-time-verified by
/// `routes::google_calendar_push`. Same shape as
/// `connectors::microsoft::generate_client_state`.
pub(crate) fn generate_channel_token() -> String {
    format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple())
}

/// Pure liveness rule for a Calendar watch row. A channel is only "working"
/// if Google can deliver to it AND our push webhook can authenticate what it
/// delivers: that needs a non-empty `channel_id` AND a non-empty
/// `channel_token` (Google echoes the token in `X-Goog-Channel-Token`; an
/// empty one means the header is absent and `routes::google_calendar_push`
/// 401s every notification). A row with an empty token (e.g. one a pre-fix
/// renewal wrote with `"token": ""`) must NOT count as live, otherwise
/// [`ensure_watch`] would never replace it.
fn watch_row_is_live(
    channel_id: &str,
    channel_token: &str,
    expires_at: DateTime<Utc>,
    now: DateTime<Utc>,
) -> bool {
    !channel_id.is_empty()
        && !channel_token.is_empty()
        && expires_at > now + chrono::Duration::minutes(CHANNEL_LIVE_MARGIN_MINUTES)
}

/// Read this target's stored channel: `(channel_id, channel_resource_id,
/// channel_token, expires_at)`, if a row exists.
async fn load_channel_row(
    t: &CalendarTarget<'_>,
    what: &str,
) -> Result<Option<(String, String, String, DateTime<Utc>)>, ConnectorError> {
    let (pool, account_id) = (t.control, t.account_id);
    let mut tx = scoped_tx(pool, account_id, what).await?;
    let row = sqlx::query_as(
        "SELECT channel_id, channel_resource_id, channel_token, expires_at FROM google_watch_channels \
         WHERE account_id = $1 AND surface = 'calendar' AND resource_id = $2",
    )
    .bind(account_id)
    .bind(t.resource_id())
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("{what}: {e}")))?;
    tx.rollback().await.ok();
    Ok(row)
}

/// Whether a live (not imminently expiring, authenticatable) Calendar watch
/// channel already exists for this account — cheap pre-check so
/// [`ensure_watch`] doesn't pay for a Google API round trip when there's
/// obviously nothing to do. See [`watch_row_is_live`].
async fn channel_is_live(t: &CalendarTarget<'_>) -> Result<bool, ConnectorError> {
    Ok(load_channel_row(t, "channel_is_live")
        .await?
        .map(|(id, _, token, expires_at)| watch_row_is_live(&id, &token, expires_at, Utc::now()))
        .unwrap_or(false))
}

/// Resolve `(account_id, stored channel_token)` for an inbound Calendar push
/// notification's `X-Goog-Channel-ID`, for `routes::google_calendar_push` to
/// authenticate against. `None` for an unknown/stale channel.
///
/// Bootstrap problem (same shape as `connectors::microsoft::subscription_
/// by_id`): the notification carries ONLY a bare `channel_id`, so the owning
/// account has to be DISCOVERED before any scope can be opened. That first
/// hop goes through `resolve_google_watch_channel_account_id()`, the narrow
/// `SECURITY DEFINER` resolver from migration `0183` (bare `account_id`
/// only — never `channel_token`, the notification-validation secret). The
/// token is then read back under a proper scoped transaction for that
/// account.
///
/// Empty `channel_id`/`channel_token` are refused at every layer (CodeRabbit
/// review finding on PR #677, Major/security): a placeholder row created by
/// a sync-only write before `ensure_watch` has ever succeeded
/// (`upsert_sync_columns`'s own doc) defaults both to `''`, and without
/// this guard an empty inbound `channel_id` would resolve to WHICHEVER
/// account currently has such a row. `routes::google_calendar_push` already
/// rejects an empty header before calling this function at all; this is the
/// second, independent layer — the SQL resolver (migration `0183`) is the
/// third.
pub(crate) async fn resolve_channel_for_push(
    pool: &PgPool,
    channel_id: &str,
) -> Result<Option<(Uuid, Option<Uuid>, String)>, ConnectorError> {
    if channel_id.is_empty() {
        return Ok(None);
    }
    let account_id: Option<Uuid> =
        sqlx::query_scalar("SELECT resolve_google_watch_channel_account_id($1)")
            .bind(channel_id)
            .fetch_one(pool)
            .await
            .map_err(|e| ConnectorError::Internal(format!("resolve_channel_for_push resolve: {e}")))?;
    let Some(account_id) = account_id else {
        return Ok(None);
    };

    let mut tx = scoped_tx(pool, account_id, "resolve_channel_for_push").await?;
    // `owner_user_id` rides along (Google Calendar org lift): `Some` = an org
    // member's own channel, so the push handler syncs THAT member's calendar
    // into THEIR ocean; `None` = the unchanged personal account-wide channel.
    let row: Option<(String, Option<Uuid>)> = sqlx::query_as(
        "SELECT channel_token, owner_user_id FROM google_watch_channels \
         WHERE account_id = $1 AND surface = 'calendar' AND channel_id = $2 \
           AND channel_id <> '' AND channel_token <> ''",
    )
    .bind(account_id)
    .bind(channel_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("resolve_channel_for_push: {e}")))?;
    tx.rollback().await.ok();
    Ok(row.map(|(token, owner)| (account_id, owner, token)))
}

async fn upsert_channel(
    t: &CalendarTarget<'_>,
    channel_id: &str,
    channel_resource_id: &str,
    channel_token: &str,
    expires_at: DateTime<Utc>,
) -> Result<(), ConnectorError> {
    let (pool, account_id) = (t.control, t.account_id);
    let mut tx = scoped_tx(pool, account_id, "upsert_channel").await?;
    sqlx::query(
        r#"
        INSERT INTO google_watch_channels
            (account_id, surface, resource_id, channel_id, channel_resource_id,
             channel_token, expires_at, owner_user_id, updated_at)
        VALUES ($1, 'calendar', $2, $3, $4, $5, $6, $7, now())
        ON CONFLICT (account_id, surface, resource_id)
        DO UPDATE SET channel_id = EXCLUDED.channel_id,
                      channel_resource_id = EXCLUDED.channel_resource_id,
                      channel_token = EXCLUDED.channel_token,
                      expires_at = EXCLUDED.expires_at,
                      last_error = NULL,
                      last_error_at = NULL,
                      updated_at = now()
        "#,
    )
    .bind(account_id)
    .bind(t.resource_id())
    .bind(channel_id)
    .bind(channel_resource_id)
    .bind(channel_token)
    .bind(expires_at)
    .bind(t.owner)
    .execute(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("upsert_channel: {e}")))?;
    tx.commit()
        .await
        .map_err(|e| ConnectorError::Internal(format!("upsert_channel commit: {e}")))?;
    Ok(())
}

/// Test-only handle on [`upsert_channel`] for the org-plane pg suites in
/// `workers::google_calendar_sync` (which cannot reach Google to create a
/// real channel).
#[cfg(test)]
pub(crate) async fn upsert_channel_for_test(
    t: &CalendarTarget<'_>,
    channel_id: &str,
    channel_resource_id: &str,
    channel_token: &str,
    expires_at: DateTime<Utc>,
) -> Result<(), ConnectorError> {
    upsert_channel(t, channel_id, channel_resource_id, channel_token, expires_at).await
}

/// Idempotently ensure a live Calendar `events.watch` channel exists for this
/// account, creating one if absent or imminently expiring. See the module
/// doc's "Watch bootstrap" section for where this is called from and why a
/// failure here never blocks event ingestion.
pub(crate) async fn ensure_watch(
    http: &reqwest::Client,
    t: &CalendarTarget<'_>,
    access_token: &str,
) -> Result<(), ConnectorError> {
    let existing = load_channel_row(t, "ensure_watch").await?;
    if let Some((id, _, token, expires_at)) = &existing {
        if watch_row_is_live(id, token, *expires_at, Utc::now()) {
            return Ok(());
        }
    }
    let webhook_url = std::env::var("GOOGLE_CALENDAR_WEBHOOK_URL").map_err(|_| {
        ConnectorError::NotConfigured("GOOGLE_CALENDAR_WEBHOOK_URL not configured".into())
    })?;
    let channel_id = Uuid::new_v4().to_string();
    let channel_token = generate_channel_token();
    let resp = super::google::google_send(|| {
        http.post(super::google_transport::endpoint(WATCH_URL)).bearer_auth(access_token).json(&serde_json::json!({
            "id": channel_id,
            "type": "web_hook",
            "address": webhook_url,
            "token": channel_token,
        }))
    })
    .await?;
    if !resp.status().is_success() {
        let status = resp.status();
        return Err(ConnectorError::Exchange(format!(
            "calendar events.watch create failed: {status}"
        )));
    }
    let parsed: WatchChannelResponse = resp
        .json()
        .await
        .map_err(|e| ConnectorError::Exchange(format!("events.watch response parse: {e}")))?;
    let expires_at = parse_epoch_millis(&parsed.expiration)?;
    if parsed.id != channel_id {
        tracing::warn!(
            requested = %channel_id, returned = %parsed.id,
            "google calendar: events.watch returned a different channel id than requested"
        );
    }
    let stop_target = replace_channel(
        t,
        existing,
        &parsed.id,
        &parsed.resource_id,
        &channel_token,
        expires_at,
    )
    .await?;
    tracing::info!(
        account_id = %t.account_id, new_channel_id = %parsed.id, new_resource_id = %parsed.resource_id,
        %expires_at, replaced = stop_target.is_some(),
        "google calendar: watch channel (re)created"
    );
    // Best-effort; never fails the ensure — the new channel is already live.
    if let Some((old_id, old_res)) = stop_target {
        let stopped = super::google::google_send(|| {
            http.post(super::google_transport::endpoint("https://www.googleapis.com/calendar/v3/channels/stop"))
                .bearer_auth(access_token)
                .json(&serde_json::json!({ "id": old_id, "resourceId": old_res }))
        })
        .await
        .and_then(|r| r.error_for_status().map_err(|e| ConnectorError::Exchange(e.to_string())));
        if let Err(e) = stopped {
            tracing::warn!(error = %e, old_channel_id = %old_id, "replaced calendar channel stop failed — it will expire on its own");
        }
    }
    Ok(())
}

/// Which previously stored channel (if any) should be stopped at Google once
/// `new_channel_id`/`new_resource_id` has replaced it. `None` when there was no
/// real old channel, when it IS the new channel, or when it shares the new
/// channel's `resourceId`: Google's `resourceId` identifies the watched
/// resource, and we must never risk a `channels.stop` that could take the
/// freshly created channel down with the old one. Skipping the stop only
/// leaves the old (already unauthenticatable) channel to expire on its own.
fn old_channel_to_stop(
    existing: &Option<(String, String, String, DateTime<Utc>)>,
    new_channel_id: &str,
    new_resource_id: &str,
) -> Option<(String, String)> {
    let (old_id, old_res, _, _) = existing.as_ref()?;
    if old_id.is_empty() || old_res.is_empty() || old_id == new_channel_id || old_res == new_resource_id {
        return None;
    }
    Some((old_id.clone(), old_res.clone()))
}

/// Persist the NEW channel over whatever row exists and return the OLD
/// `(channel_id, resource_id)` to stop at Google, if any. All DB-side state of
/// the heal path, separated from HTTP so it is testable.
async fn replace_channel(
    t: &CalendarTarget<'_>,
    existing: Option<(String, String, String, DateTime<Utc>)>,
    new_channel_id: &str,
    new_resource_id: &str,
    new_token: &str,
    expires_at: DateTime<Utc>,
) -> Result<Option<(String, String)>, ConnectorError> {
    upsert_channel(t, new_channel_id, new_resource_id, new_token, expires_at).await?;
    Ok(old_channel_to_stop(&existing, new_channel_id, new_resource_id))
}

// ---------------------------------------------------------------------------
// Sync-cursor persistence
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
struct SyncState {
    sync_token: String,
    backfill_page_token: Option<String>,
}

async fn load_sync_state(t: &CalendarTarget<'_>) -> Result<SyncState, ConnectorError> {
    let (pool, account_id) = (t.control, t.account_id);
    let mut tx = scoped_tx(pool, account_id, "load_sync_state").await?;
    let row: Option<(String, Option<String>)> = sqlx::query_as(
        "SELECT sync_token, backfill_page_token FROM google_watch_channels \
         WHERE account_id = $1 AND surface = 'calendar' AND resource_id = $2",
    )
    .bind(account_id)
    .bind(t.resource_id())
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("load_sync_state: {e}")))?;
    tx.rollback().await.ok();
    Ok(row
        .map(|(sync_token, backfill_page_token)| SyncState {
            sync_token,
            backfill_page_token,
        })
        .unwrap_or_default())
}

/// Upsert the three sync-state columns. `sync_token: None` PRESERVES the
/// existing stored value (via `COALESCE`) — callers that are only updating
/// the resume page cursor mid-pass pass `None` here so they don't clobber a
/// still-valid `syncToken` with an empty string. `Some("")` explicitly
/// clears it (the `410 GONE` restart path). On a genuinely fresh row (no
/// watch channel and no prior sync attempt for this account), `expires_at`
/// defaults to `now()` — already "due" the moment `workers::
/// google_watch_renewal`'s sweep next looks at it, which is what makes the
/// module doc's "Watch bootstrap" self-healing path work without a
/// dedicated backfill job.
async fn upsert_sync_columns(
    t: &CalendarTarget<'_>,
    sync_token: Option<&str>,
    backfill_page_token: Option<&str>,
    backfill_complete: bool,
) -> Result<(), ConnectorError> {
    let (pool, account_id) = (t.control, t.account_id);
    let mut tx = scoped_tx(pool, account_id, "upsert_sync_columns").await?;
    sqlx::query(
        r#"
        INSERT INTO google_watch_channels
            (account_id, surface, resource_id, expires_at, sync_token, backfill_page_token,
             backfill_complete, owner_user_id, updated_at)
        VALUES ($1, 'calendar', $2, now(), COALESCE($3, ''), $4, $5, $6, now())
        ON CONFLICT (account_id, surface, resource_id)
        DO UPDATE SET sync_token = COALESCE($3, google_watch_channels.sync_token),
                      backfill_page_token = $4,
                      backfill_complete = $5,
                      updated_at = now()
        "#,
    )
    .bind(account_id)
    .bind(t.resource_id())
    .bind(sync_token)
    .bind(backfill_page_token)
    .bind(backfill_complete)
    .bind(t.owner)
    .execute(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("upsert_sync_columns: {e}")))?;
    tx.commit()
        .await
        .map_err(|e| ConnectorError::Internal(format!("upsert_sync_columns commit: {e}")))?;
    Ok(())
}

async fn persist_backfill_progress(
    t: &CalendarTarget<'_>,
    resume_page_token: Option<&str>,
) -> Result<(), ConnectorError> {
    upsert_sync_columns(t, None, resume_page_token, false).await
}

async fn persist_sync_complete(
    t: &CalendarTarget<'_>,
    new_sync_token: &str,
) -> Result<(), ConnectorError> {
    upsert_sync_columns(t, Some(new_sync_token), None, true).await
}

async fn clear_sync_token(t: &CalendarTarget<'_>) -> Result<(), ConnectorError> {
    upsert_sync_columns(t, Some(""), None, false).await
}

// ---------------------------------------------------------------------------
// events.list fetch + pagination
// ---------------------------------------------------------------------------

/// Where an `events.list` pagination pass starts (or resumes) from. See the
/// module doc's "Incremental sync + resumable backfill" section for why a
/// bare [`Cursor::PageToken`] is sufficient to resume EITHER a from-scratch
/// backfill or a `syncToken`-started incremental pass.
enum Cursor {
    SyncToken(String),
    PageToken(String),
    FromScratch,
}

enum ListOutcome {
    Page(GEventsList),
    /// `410 GONE` — the `syncToken` (or a stale in-flight `pageToken`) is no
    /// longer valid upstream.
    TokenExpired,
    /// `401` — Google rejected the access token itself. Whether that is a
    /// dead grant or an early-expired token is decided by [`fetch_pending`],
    /// which knows whether the token was minted by a refresh in this pass.
    Unauthorized,
}

/// The `events.list` URL for one page. `showDeleted=true` on EVERY cursor
/// shape — the incremental `syncToken` pass, a resumed `pageToken` page,
/// and the from-scratch listing a `410` restart falls back to — is what
/// makes a cancelled event reach [`process_events_page`] on every path;
/// without it a full resync after a token expiry would silently miss the
/// deletions that happened during the gap.
fn events_list_url(cursor: &Cursor) -> String {
    let mut url = format!(
        "{EVENTS_URL}?singleEvents=false&showDeleted=true&maxResults={LIST_PAGE_SIZE}"
    );
    match cursor {
        Cursor::SyncToken(t) => {
            url.push_str("&syncToken=");
            url.push_str(&urlencoding::encode(t));
        }
        Cursor::PageToken(t) => {
            url.push_str("&pageToken=");
            url.push_str(&urlencoding::encode(t));
        }
        Cursor::FromScratch => {}
    }
    url
}

async fn list_events_page(
    http: &reqwest::Client,
    access_token: &str,
    cursor: &Cursor,
) -> Result<ListOutcome, ConnectorError> {
    let url = events_list_url(cursor);
    let resp = super::google::google_send(|| http.get(super::google_transport::endpoint(&url)).bearer_auth(access_token)).await?;
    if resp.status() == reqwest::StatusCode::GONE {
        return Ok(ListOutcome::TokenExpired);
    }
    if resp.status() == reqwest::StatusCode::UNAUTHORIZED {
        return Ok(ListOutcome::Unauthorized);
    }
    if !resp.status().is_success() {
        let status = resp.status();
        // Do NOT include the body — a Google error body can echo event data
        // (same discipline `google.rs::fetch_upcoming` already follows).
        return Err(ConnectorError::Exchange(format!(
            "calendar events.list failed: {status}"
        )));
    }
    let value: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| ConnectorError::Exchange(format!("events.list parse: {e}")))?;
    let page: GEventsList = serde_json::from_value(value)
        .map_err(|e| ConnectorError::Exchange(format!("events.list decode: {e}")))?;
    Ok(ListOutcome::Page(page))
}

/// Whether accumulating one more page would push a [`run_pass`] call past
/// its per-call `budget` — the boundary that decides "return everything
/// fetched so far plus an explicit resume cursor" vs. "keep paging". Pure —
/// no I/O — so this module's tests exercise the exact boundary condition
/// `run_pass` uses directly, without needing a live/mocked HTTP round trip
/// (contract §6 test #6, "no silent truncation": this is the decision that
/// guarantees a capped call always returns EITHER everything, OR an
/// explicit signal to resume by, never a silent drop of the remainder).
fn budget_exceeded(items: usize, deleted: usize, budget: usize) -> bool {
    items + deleted >= budget
}

/// Split one `events.list` page into normalized items (created/updated
/// events) and deleted-event source ids (`status: "cancelled"`). Pure — no
/// I/O — so this is the piece [`run_pass`]'s network loop delegates to and
/// this module's tests exercise directly.
fn process_events_page(page: GEventsList) -> (Vec<NormalizedItem>, Vec<String>) {
    let mut items = Vec::new();
    let mut deleted_ids = Vec::new();
    for ev in page.items {
        // Empty (not just missing) id is treated as malformed upstream data
        // and skipped entirely — CodeRabbit review finding on PR #677,
        // round 2: `shape_calendar_item` used to accept `Some("")` (only
        // `?` on `None`), producing a `NormalizedItem` with `source_id: ""`
        // that `ingest_atom` itself deterministically 422s on
        // (`req.source_id.is_empty()`). Before this fix that turned into a
        // POISON PILL on the commit-after-ingest push path (round 1's own
        // fix, `PendingCommit`): the failing item blocked the whole batch's
        // cursor forever, and since the SAME malformed event would be
        // re-fetched by every retry, the pass would never succeed again.
        // Filtering here — the one place every code path (create AND
        // delete) funnels through — closes it structurally rather than
        // only at the ingest boundary.
        let has_id = ev.id.as_deref().map(|s| !s.trim().is_empty()).unwrap_or(false);
        if !has_id {
            tracing::warn!(
                "google calendar: event with missing/empty id skipped (malformed upstream data)"
            );
            continue;
        }
        let id = ev.id.clone().expect("has_id just proved this is Some");
        if ev.status.as_deref() == Some("cancelled") {
            deleted_ids.push(id);
            continue;
        }
        if let Some(item) = shape_calendar_item(&ev) {
            items.push(item);
        }
    }
    (items, deleted_ids)
}

/// Shape one (non-cancelled) Google Calendar event into a
/// [`NormalizedItem`]. `None` when the event carries no id (or an EMPTY
/// one — see [`process_events_page`]'s doc on the poison-pill this closes)
/// or no parseable start time (mirrors `google.rs::events_from_json`'s own
/// drop rules for the read-only `/upcoming` route). This check is also
/// applied independently of `process_events_page`'s own (so a future
/// caller of this function directly can't reintroduce the bug).
fn shape_calendar_item(ev: &GEvent) -> Option<NormalizedItem> {
    let id = ev.id.as_deref().filter(|s| !s.trim().is_empty())?.to_string();
    let start = parse_g_time(ev.start.as_ref())?;

    let title = ev
        .summary
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or("(no title)");

    let mut body = String::new();
    if let Some(desc) = ev.description.as_deref() {
        let d = desc.trim();
        if !d.is_empty() {
            body.push_str(d);
            body.push('\n');
        }
    }
    if let Some(loc) = ev.location.as_deref() {
        let l = loc.trim();
        if !l.is_empty() {
            body.push_str(&format!("Location: {l}\n"));
        }
    }
    if let Some(url) = extract_meeting_url(ev) {
        body.push_str(&format!("Join: {url}\n"));
    }
    let content = if body.trim().is_empty() {
        title.to_string()
    } else {
        format!("{title}\n\n{}", body.trim())
    };

    let participants: Vec<String> = ev
        .attendees
        .iter()
        .filter(|a| !a.resource)
        .filter_map(|a| a.display_name.clone().or_else(|| a.email.clone()))
        .collect();

    Some(NormalizedItem {
        source_type: SOURCE_TYPE.to_string(),
        source_id: id,
        content,
        participants,
        timestamp: start,
        scope: None,
        thread_id: None,
        channel_id: None,
        author_id: None,
        author_name: None,
    })
}

struct PassResult {
    items: Vec<NormalizedItem>,
    deleted_ids: Vec<String>,
    /// `Some` — the pass hit [`SYNC_PASS_BUDGET`] before reaching Google's
    /// terminal page; this is where the NEXT call must resume.
    resume_page_token: Option<String>,
    /// `Some` — the pass reached the terminal page; this is the new
    /// incremental-sync cursor to persist.
    new_sync_token: Option<String>,
}

enum PassOutcome {
    Progress(PassResult),
    TokenExpired,
    Unauthorized,
}

/// What to do after accumulating one more `events.list` page — pure (no
/// I/O), so this is the exact decision [`run_pass`]'s loop makes and this
/// module's tests exercise directly, INCLUDING the boundary CodeRabbit
/// review flagged on PR #677: a TERMINAL page (`next_page_token: None`)
/// must always finish the pass and hand back `next_sync_token`, regardless
/// of budget — Google's own pagination ending is not something a per-call
/// budget may override, or a `nextSyncToken` handed to us on exactly the
/// page that also happens to trip the budget would be silently discarded
/// (the caller would persist neither a resume cursor NOR the sync token,
/// and the next call would restart a full backfill from scratch forever).
/// The budget only ever applies when there IS another page to defer to.
enum PageDecision {
    /// More pages remain and the pass is still under budget — keep paging
    /// with this token.
    Continue(String),
    /// The pass is finished, one way or the other.
    Done {
        resume_page_token: Option<String>,
        new_sync_token: Option<String>,
    },
}

fn decide_after_page(
    next_page_token: Option<String>,
    next_sync_token: Option<String>,
    accumulated_items: usize,
    accumulated_deleted: usize,
    budget: usize,
) -> PageDecision {
    match next_page_token {
        None => PageDecision::Done {
            resume_page_token: None,
            new_sync_token: next_sync_token,
        },
        Some(next) => {
            if budget_exceeded(accumulated_items, accumulated_deleted, budget) {
                PageDecision::Done {
                    resume_page_token: Some(next),
                    new_sync_token: None,
                }
            } else {
                PageDecision::Continue(next)
            }
        }
    }
}

/// Capture policy only: never turn an aged event into a deletion or alter
/// Google's complete-resource sync protocol. Applies to every cursor shape.
fn retain_capture_window(items: &mut Vec<NormalizedItem>, floor: DateTime<Utc>) {
    items.retain(|item| item.timestamp >= floor);
}

/// Page `events.list` starting at `start`, accumulating items/deletions up to
/// `budget`. Returns as soon as EITHER the budget is hit on a NON-terminal
/// page (persisting a resume cursor — see [`PassResult::resume_page_token`])
/// OR Google's own pagination is exhausted (returning the new
/// `nextSyncToken` — see [`decide_after_page`]'s own doc for why the
/// terminal-page check always wins over the budget check).
async fn run_pass(
    http: &reqwest::Client,
    access_token: &str,
    start: Cursor,
    budget: usize,
) -> Result<PassOutcome, ConnectorError> {
    let history_floor = Utc::now() - chrono::Duration::days(365);
    let mut cursor = start;
    let mut items = Vec::new();
    let mut deleted_ids = Vec::new();
    loop {
        match list_events_page(http, access_token, &cursor).await? {
            ListOutcome::TokenExpired => return Ok(PassOutcome::TokenExpired),
            ListOutcome::Unauthorized => return Ok(PassOutcome::Unauthorized),
            ListOutcome::Page(page) => {
                let next_page_token = page.next_page_token.clone();
                let next_sync_token = page.next_sync_token.clone();
                let (mut page_items, mut page_deleted) = process_events_page(page);
                items.append(&mut page_items);
                deleted_ids.append(&mut page_deleted);

                match decide_after_page(next_page_token, next_sync_token, items.len(), deleted_ids.len(), budget) {
                    PageDecision::Continue(next) => cursor = Cursor::PageToken(next),
                    PageDecision::Done { resume_page_token, new_sync_token } => {
                        // Apply admission after scan-budget accounting. Sync/page tokens must
                        // still advance across old resources, and cancellations must still
                        // propagate to copies retained from earlier capture windows.
                        retain_capture_window(&mut items, history_floor);
                        return Ok(PassOutcome::Progress(PassResult {
                            items,
                            deleted_ids,
                            resume_page_token,
                            new_sync_token,
                        }));
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Deletion propagation (decision 9)
// ---------------------------------------------------------------------------

/// An account's owner user id — used both to resolve the destination/origin
/// ocean ([`propagate_deletions`]) and by `routes::google_calendar_push` to
/// build the synthetic `UserContext` a push-triggered ingest needs. Mirrors
/// `connectors::microsoft::account_owner_user` exactly (kept as its own
/// small copy rather than a shared helper — the same "duplicating one
/// two-line query is cheaper than a shared cross-connector dependency" call
/// `workers::google_watch_renewal::resolve_tenant_account`'s own comment
/// already makes for an identical situation).
pub(crate) async fn account_owner_user(
    pool: &PgPool,
    account_id: Uuid,
) -> Result<Option<Uuid>, ConnectorError> {
    let row: Option<(Uuid,)> = sqlx::query_as("SELECT owner_user_id FROM accounts WHERE id = $1")
        .bind(account_id)
        .fetch_optional(pool)
        .await
        .map_err(|e| ConnectorError::Internal(format!("account_owner_user: {e}")))?;
    Ok(row.map(|(id,)| id))
}

/// Recover the `oauth_tokens.owner_user_id` of the Calendar connection
/// `routes::oauth::callback` just stored for `(account_id, workspace/email)` —
/// `Some(uid)` for an org member's own row, `None` for the personal
/// account-wide row (or when no row is found). Same lookback shape as
/// `google_gmail::resolve_owner_user_id` (`ConnectorProvider::post_store`
/// carries no user id), against the control pool.
pub(crate) async fn resolve_connection_owner(
    control_pool: &PgPool,
    account_id: Uuid,
    email: &str,
) -> Result<Option<Uuid>, ConnectorError> {
    let mut tx = scoped_tx(control_pool, account_id, "resolve_connection_owner").await?;
    let row: Option<Option<Uuid>> = sqlx::query_scalar(
        "SELECT owner_user_id FROM oauth_tokens \
         WHERE account_id = $1 AND provider = 'google' AND provider_workspace_id = $2 \
         ORDER BY updated_at DESC LIMIT 1",
    )
    .bind(account_id)
    .bind(email)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("resolve_connection_owner: {e}")))?;
    tx.rollback().await.ok();
    Ok(row.flatten())
}

/// Propagate every cancelled event in `deleted_ids` to
/// `services::deletion::delete_source` — see the module doc's "Deletions"
/// section for the authorization reasoning. Best-effort on the *attempt*
/// side: one individual `delete_source` failure does not stop the rest of
/// the batch, since a stuck/duplicate delete_source call is safe to retry
/// on the next incremental pass (deleting an already-gone atom is a no-op
/// cascade, not an error) but losing an already-fetched batch of OTHER
/// deletions to one bad id would not be.
///
/// # Returns `Result`, blocks the cursor on ANY failure (identical gap to
/// the one CodeRabbit round 2 finding #1 on PR #686 found and fixed in
/// `google_gmail::propagate_deletions` — ported here verbatim; see that
/// function's own doc for the full reasoning)
/// This function used to swallow every failure (owner lookup, graph
/// resolution, or an individual `delete_source` call) and always return
/// `()` — so [`fetch_pending`]'s caller had no way to know a deletion never
/// actually landed. A transient DB error here let the sync cursor advance
/// past a `syncToken`/`historyId` range that reports a given deletion
/// EXACTLY ONCE, so that specific cancelled event would never be reported
/// again and would stay indexed in CoralSwarm forever, with no error
/// anywhere — permanent silent retention of a user-deleted event. Now: an
/// owner-lookup or graph-resolution failure aborts immediately (nothing in
/// this batch could be resolved, so nothing is attempted); an individual
/// `delete_source` failure is logged and does NOT stop the rest of the
/// batch from being attempted (still best-effort per id, per the doc
/// above), but the function remembers it and returns `Err` at the end
/// regardless. [`fetch_pending`] propagates the error via `?`, leaving the
/// cursor unchanged so the next pass re-diffs the same range and
/// re-attempts this deletion — `delete_source` is itself idempotent, so a
/// spurious retry of an already-successful deletion is harmless.
async fn propagate_deletions(
    t: &CalendarTarget<'_>,
    deleted_ids: &[String],
) -> Result<(), ConnectorError> {
    if deleted_ids.is_empty() {
        return Ok(());
    }
    let account_id = t.account_id;
    // Org lift: an org member's deletions resolve against THEIR own
    // member-private ocean on the org tenant pool (`AccountPlane::Org`) —
    // never `account_owner_user`, which on an org account is just whoever
    // created it and would make member A's cancellation retract (or miss)
    // member B's atom. Personal plane: unchanged.
    let (owner, plane, pool) = match t.owner {
        Some(member) => (member, crate::routes::slack::AccountPlane::Org, t.content),
        None => {
            let owner = account_owner_user(t.content, account_id)
                .await
                .map_err(|e| {
                    tracing::warn!(error = %e, %account_id, "google calendar: deletion propagation aborted — owner lookup failed");
                    e
                })?
                .ok_or_else(|| {
                    tracing::warn!(
                        %account_id,
                        "google calendar: deletion propagation aborted — account has no owner_user_id"
                    );
                    ConnectorError::Internal(format!(
                        "google calendar: account {account_id} has no owner_user_id"
                    ))
                })?;
            (owner, crate::routes::slack::AccountPlane::Personal, t.content)
        }
    };
    let graph_id = crate::routes::slack::resolve_primary_graph(
        pool,
        account_id,
        plane,
        crate::routes::slack::IngestCaller::Attributable(owner),
    )
    .await
    .map_err(|e| {
        tracing::warn!(error = %e, %account_id, "google calendar: deletion propagation aborted — graph resolution failed");
        ConnectorError::Internal(format!("google calendar: graph resolution failed: {e}"))
    })?;
    let namespace=format!("google_calendar:connection:{account_id}:{owner}");
    let mut any_failed=false;
    for source_id in deleted_ids {
        let result=if t.owner.is_some() {
            crate::services::deletion::delete_source_identity_in_graph(
                pool,None,crate::store::CLOUD_DEVICE_ID,graph_id,SOURCE_TYPE,source_id,
                Some(&namespace),crate::services::connector_audit::Actor::System,None,
            ).await
        } else {
            crate::services::deletion::delete_chunked_source_in_namespace(
                pool,None,crate::store::CLOUD_DEVICE_ID,graph_id,SOURCE_TYPE,source_id,
                &namespace,crate::services::connector_audit::Actor::System,None,
            ).await
        };
        if let Err(e)=result {
            tracing::warn!(error=%e,%account_id,%source_id,"google calendar: source deletion failed");
            any_failed=true;
        }
    }
    if any_failed {return Err(ConnectorError::Internal(format!(
        "google calendar: one or more deletions failed to propagate for account {account_id}"
    )));}
    Ok(())
}



// ---------------------------------------------------------------------------
// Top-level entry point
// ---------------------------------------------------------------------------

/// Incremental sync (via `syncToken`) once established, else a resumable
/// full backfill. See the module doc for the complete design. Called from
/// `GoogleConnector::sync` (which does the token refresh first) and, for a
/// push-triggered pass, `routes::google_calendar_push` (which loads/refreshes
/// the token itself the same way `routes::calendar::upcoming` does).
/// What [`fetch_pending`] found, deferred rather than persisted immediately.
/// `items`/`commit` are `pub(crate)` so `routes::google_calendar_push` can
/// hold onto both across its own ingest loop and only pass `commit` to
/// [`commit_pending`] once every item has actually landed as an atom — see
/// [`fetch_pending`]'s own doc, "commit-after-ingest" (CodeRabbit review
/// finding on PR #677, Major).
pub(crate) struct PendingSync {
    pub(crate) items: Vec<NormalizedItem>,
    pub(crate) commit: PendingCommit,
}

/// The sync-cursor write [`fetch_pending`] computed but did not yet make —
/// opaque to callers outside this module; just hand it back to
/// [`commit_pending`] once it is safe to advance the cursor.
pub(crate) enum PendingCommit {
    Complete(String),
    Progress(Option<String>),
}

/// Persist a [`PendingCommit`] — the ONLY place `sync_token`/
/// `backfill_page_token`/`backfill_complete` actually change as a result of
/// a fetch pass. Callers MUST NOT call this until every item in the
/// corresponding [`PendingSync::items`] has been durably ingested (or there
/// were none) — see [`fetch_pending`]'s doc.
pub(crate) async fn commit_pending(
    t: &CalendarTarget<'_>,
    commit: PendingCommit,
) -> Result<(), ConnectorError> {
    match commit {
        PendingCommit::Complete(token) => persist_sync_complete(t, &token).await,
        PendingCommit::Progress(resume) => persist_backfill_progress(t, resume.as_deref()).await,
    }
}

/// Fetch one sync pass (incremental or resumed/fresh backfill, restarting
/// once on a `410`) WITHOUT persisting the resulting cursor — see
/// [`PendingSync`]/[`commit_pending`]. Deletions (`propagate_deletions`) ARE
/// applied eagerly here, not deferred: `services::deletion::delete_source`
/// is idempotent (deleting an atom that's already gone, or was never
/// ingested, is a no-op cascade), so re-running it on a retry after a later
/// failure is harmless — unlike the create side, there is no cursor gating
/// re-delivery of a deletion the way there is for a create/update.
///
/// # Commit-after-ingest (CodeRabbit review finding on PR #677, Major)
/// An earlier version of this module persisted the new cursor INSIDE what
/// is now this function, before the caller had ingested anything — so a
/// transient `ingest_atom` failure on the push-webhook path (unattended, no
/// human retry) would advance past a changed event that was never actually
/// stored, losing it FOREVER (an incremental `syncToken` pass only ever
/// reports a given change once). Splitting fetch from commit lets the two
/// real callers make the right call for their own risk profile:
/// [`sync`] (used by the generic, human-driven "sync now" HTTP route,
/// `routes::connectors::trigger_sync`) commits unconditionally, matching
/// every other connector's existing best-effort ingest semantics (a stuck
/// item there is visible to the user, who can just click sync again).
/// `workers::google_calendar_sync`'s passes (push- or sweep-driven) commit
/// ONLY after every item in the batch ingests successfully — on ANY
/// failure it deliberately does NOT commit, so the NEXT pass re-fetches the
/// exact same page/token and gets another chance. Re-ingesting an
/// already-succeeded item on that retry is a harmless no-op:
/// `routes::ingest::ingest_atom` already dedupes on `(graph_id,
/// source_type, source_id)`.
///
/// # Dead credential
/// `freshly_refreshed` is whether `access_token` was minted by a successful
/// `/token` refresh in THIS pass (`creds.refreshed.is_some()` after
/// `GoogleConnector::refresh_if_needed_for`). A `401` from `events.list` on
/// such a token is a DEFINITE dead grant — Google just vouched for the
/// refresh token seconds ago, so the only thing that rejects its product is
/// a revocation in between — and is raised as `CredentialRevoked { "google",
/// GoogleUnauthorizedAfterRefresh }` for `credential_health::record_if_revoked`.
/// A `401` on a STORED token that `expires_at` still called live is left as
/// `Exchange`: an early-expired or clock-skewed access token is repaired by
/// the next pass's refresh, and if the grant really is dead that refresh
/// answers `invalid_grant` and classifies itself. `ensure_watch`'s own 401
/// stays non-fatal here like every other watch failure.
pub(crate) async fn fetch_pending(
    http: &reqwest::Client,
    t: &CalendarTarget<'_>,
    access_token: &str,
    freshly_refreshed: bool,
) -> Result<PendingSync, ConnectorError> {
    let account_id = t.account_id;
    // Best-effort watch bootstrap — see the module doc's "Watch bootstrap"
    // section. Never blocks event ingestion.
    if let Err(e) = ensure_watch(http, t, access_token).await {
        tracing::warn!(
            error = %e, %account_id,
            "google calendar: ensure_watch failed during sync (non-fatal)"
        );
    }

    let mut state = load_sync_state(t).await?;
    let mut restarted_once = false;

    loop {
        let cursor = match &state.backfill_page_token {
            Some(t) => Cursor::PageToken(t.clone()),
            None if !state.sync_token.is_empty() => Cursor::SyncToken(state.sync_token.clone()),
            None => Cursor::FromScratch,
        };

        match run_pass(http, access_token, cursor, SYNC_PASS_BUDGET).await? {
            PassOutcome::Unauthorized => {
                return Err(if freshly_refreshed {
                    ConnectorError::CredentialRevoked {
                        provider: "google",
                        reason: CredentialDeathReason::GoogleUnauthorizedAfterRefresh,
                    }
                } else {
                    ConnectorError::Exchange("calendar events.list failed: 401 Unauthorized".into())
                });
            }
            PassOutcome::TokenExpired => {
                if restarted_once {
                    // Defensive: a genuinely from-scratch listing should
                    // never itself 410. Surface rather than loop forever.
                    return Err(ConnectorError::Exchange(
                        "google calendar: sync token expired again immediately \
                         after a full restart"
                            .into(),
                    ));
                }
                tracing::info!(
                    %account_id,
                    "google calendar: sync token expired (410) — restarting full sync"
                );
                clear_sync_token(t).await?;
                state = SyncState::default();
                restarted_once = true;
                continue;
            }
            PassOutcome::Progress(result) => {
                // `?` — a deletion-propagation failure must block the
                // cursor exactly like a failed create does (ported from
                // CodeRabbit round 2 finding #1 on PR #686 / `google_gmail::
                // fetch_pending`'s identical call site); see
                // `propagate_deletions`'s own doc for the full reasoning.
                propagate_deletions(t, &result.deleted_ids).await?;
                let commit = match result.new_sync_token {
                    Some(new_token) => PendingCommit::Complete(new_token),
                    None => PendingCommit::Progress(result.resume_page_token),
                };
                return Ok(PendingSync {
                    items: result.items,
                    commit,
                });
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests — pure shaping/pagination logic (no network, no DB). DB-touching
// paths (upsert_sync_columns round trip, ensure_watch idempotency, real
// delete_source propagation) are pg-gated below, mirroring
// `workers::google_watch_renewal`'s own `ORGT_TEST_BASE_URL` convention —
// no `*_pg.rs` file, so no CI pg-suite wiring is needed (in-module, same
// class as that worker's suite).
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capture_window_keeps_boundary_and_recent_events_without_deleting_old_copies() {
        let now = DateTime::parse_from_rfc3339("2026-10-30T12:00:00Z").unwrap().with_timezone(&Utc);
        let floor = now - chrono::Duration::days(365);
        let event = |id: &str, at: DateTime<Utc>| serde_json::json!({"id":id,"start":{"dateTime":at.to_rfc3339()},"summary":"meeting"});
        let page: GEventsList = serde_json::from_value(serde_json::json!({"items":[
            event("old",floor-chrono::Duration::seconds(1)),event("boundary",floor),
            event("recent",now-chrono::Duration::days(120)),event("future",now+chrono::Duration::days(1)),
            {"id":"cancelled-old","status":"cancelled"}
        ]})).unwrap();
        let (mut items,deleted)=process_events_page(page);
        // Raw scan accounting remains bounded even when every old item is rejected.
        assert!(matches!(decide_after_page(Some("next".into()),None,items.len(),deleted.len(),2),PageDecision::Done {resume_page_token:Some(_),..}));
        retain_capture_window(&mut items,floor);
        assert_eq!(items.iter().map(|i|i.source_id.as_str()).collect::<Vec<_>>(),vec!["boundary","recent","future"]);
        assert_eq!(deleted,vec!["cancelled-old"]); // Aging is never a deletion.
        for cursor in [Cursor::FromScratch,Cursor::PageToken("next".into()),Cursor::SyncToken("sync".into())] {
            let url=events_list_url(&cursor);
            assert!(!url.contains("timeMin")); assert!(!url.contains("timeMax"));
        }
    }


    fn event(json: serde_json::Value) -> GEvent {
        serde_json::from_value(json).unwrap()
    }

    #[test]
    fn shape_calendar_item_composes_title_description_location_join() {
        let ev = event(serde_json::json!({
            "id": "evt-1",
            "summary": "Weekly Sync",
            "description": "Discuss roadmap",
            "location": "Room 4",
            "hangoutLink": "https://meet.google.com/abc-defg-hij",
            "start": { "dateTime": "2026-07-15T10:00:00Z" },
            "attendees": [
                { "email": "alice@ex.com", "displayName": "Alice Ng" },
                { "email": "bob@ex.com" },
                { "email": "room-4@resource.calendar.google.com", "resource": true }
            ]
        }));
        let item = shape_calendar_item(&ev).expect("must shape");
        assert_eq!(item.source_type, "google_calendar");
        assert_eq!(item.source_id, "evt-1");
        assert!(item.content.starts_with("Weekly Sync\n\n"));
        assert!(item.content.contains("Discuss roadmap"));
        assert!(item.content.contains("Location: Room 4"));
        assert!(item.content.contains("Join: https://meet.google.com/abc-defg-hij"));
        // resource room filtered; two humans remain
        assert_eq!(item.participants, vec!["Alice Ng".to_string(), "bob@ex.com".to_string()]);
        assert!(item.thread_id.is_none());
        assert!(item.channel_id.is_none());
    }

    #[test]
    fn shape_calendar_item_falls_back_to_no_title_and_bare_content() {
        let ev = event(serde_json::json!({
            "id": "evt-2",
            "start": { "date": "2026-07-16" }
        }));
        let item = shape_calendar_item(&ev).expect("must shape");
        assert_eq!(item.content, "(no title)");
        assert!(item.participants.is_empty());
    }

    #[test]
    fn shape_calendar_item_none_without_id_or_start() {
        assert!(shape_calendar_item(&event(serde_json::json!({ "start": { "dateTime": "2026-07-15T10:00:00Z" } }))).is_none());
        assert!(shape_calendar_item(&event(serde_json::json!({ "id": "evt-3" }))).is_none());
    }

    #[test]
    fn shape_calendar_item_none_for_empty_string_id_not_just_missing() {
        // The exact poison-pill case (CodeRabbit review finding on PR #677,
        // round 2): `Some("")` must be rejected exactly like `None`, not
        // pass through and later fail deterministically at ingest_atom.
        assert!(shape_calendar_item(&event(serde_json::json!({
            "id": "",
            "start": { "dateTime": "2026-07-15T10:00:00Z" }
        })))
        .is_none());
        assert!(shape_calendar_item(&event(serde_json::json!({
            "id": "   ",
            "start": { "dateTime": "2026-07-15T10:00:00Z" }
        })))
        .is_none());
    }

    #[test]
    fn process_events_page_skips_events_with_empty_or_missing_id_entirely() {
        let page: GEventsList = serde_json::from_value(serde_json::json!({
            "items": [
                { "id": "evt-ok", "summary": "Fine", "start": { "dateTime": "2026-07-15T10:00:00Z" } },
                { "id": "", "summary": "Malformed empty id", "start": { "dateTime": "2026-07-15T11:00:00Z" } },
                { "summary": "Malformed missing id", "start": { "dateTime": "2026-07-15T12:00:00Z" } },
                { "id": "", "status": "cancelled" }
            ]
        }))
        .unwrap();
        let (items, deleted) = process_events_page(page);
        assert_eq!(items.len(), 1, "only the well-formed event ingests");
        assert_eq!(items[0].source_id, "evt-ok");
        assert!(deleted.is_empty(), "an empty-id cancellation must not even reach the deletion list");
    }

    /// Every cursor shape — including the from-scratch listing the `410`
    /// full-resync path uses — asks Google for cancelled events, so
    /// deletions are reported on the restart path too, not only on the
    /// incremental one.
    #[test]
    fn every_events_list_cursor_shape_requests_deleted_events() {
        for cursor in [Cursor::FromScratch, Cursor::SyncToken("tok a".into()), Cursor::PageToken("pg/b".into())] {
            let url = events_list_url(&cursor);
            assert!(url.contains("showDeleted=true"), "{url}");
            assert!(url.contains("singleEvents=false"), "{url}");
        }
        assert!(events_list_url(&Cursor::SyncToken("tok a".into())).ends_with("&syncToken=tok%20a"));
        assert!(events_list_url(&Cursor::PageToken("pg/b".into())).ends_with("&pageToken=pg%2Fb"));
        assert!(!events_list_url(&Cursor::FromScratch).contains("Token="));
    }

    #[test]
    fn process_events_page_splits_cancelled_from_live() {
        let page: GEventsList = serde_json::from_value(serde_json::json!({
            "items": [
                { "id": "evt-1", "summary": "Alive", "start": { "dateTime": "2026-07-15T10:00:00Z" } },
                { "id": "evt-2", "summary": "Dead", "status": "cancelled", "start": { "dateTime": "2026-07-15T11:00:00Z" } },
                { "id": "evt-3", "status": "cancelled" }
            ],
            "nextPageToken": null,
            "nextSyncToken": "sync-tok-1"
        }))
        .unwrap();
        let (items, deleted) = process_events_page(page);
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].source_id, "evt-1");
        assert_eq!(deleted, vec!["evt-2".to_string(), "evt-3".to_string()]);
    }

    #[test]
    fn process_events_page_recurring_master_and_exception_both_ingest_as_events() {
        // singleEvents=false: a series appears once as its master, and an
        // individually-edited occurrence appears separately with
        // recurringEventId set — both are ordinary (non-cancelled) items
        // here; only the exception's OWN cancellation (a separate case)
        // would route it to deletions instead.
        let page: GEventsList = serde_json::from_value(serde_json::json!({
            "items": [
                { "id": "series-1", "summary": "Standup", "start": { "dateTime": "2026-07-13T09:00:00Z" } },
                {
                    "id": "series-1_20260715T090000Z",
                    "summary": "Standup (moved)",
                    "recurringEventId": "series-1",
                    "start": { "dateTime": "2026-07-15T10:00:00Z" }
                }
            ]
        }))
        .unwrap();
        let (items, deleted) = process_events_page(page);
        assert!(deleted.is_empty());
        assert_eq!(items.len(), 2, "master + exception both ingest as their own atom");
        assert_eq!(items[0].source_id, "series-1");
        assert_eq!(items[1].source_id, "series-1_20260715T090000Z");
    }

    // ── Fixture-driven (backend/tests/fixtures/google_calendar/ — see its
    // README for provenance: hand-authored against Google's documented
    // schema, not a live capture) ───────────────────────────────────────

    const EVENTS_LIST_PAGE: &str =
        include_str!("../../tests/fixtures/google_calendar/events_list_page.json");
    const EVENTS_LIST_INCREMENTAL_WITH_CANCELLATION: &str = include_str!(
        "../../tests/fixtures/google_calendar/events_list_incremental_with_cancellation.json"
    );

    #[test]
    fn fixture_full_page_shapes_master_and_exception_ingests_no_deletions() {
        let page: GEventsList = serde_json::from_str(EVENTS_LIST_PAGE).unwrap();
        assert!(page.next_page_token.is_none(), "fixture is a terminal page");
        assert!(page.next_sync_token.is_some());
        let (items, deleted) = process_events_page(page);
        assert!(deleted.is_empty());
        // timed 1:1 + all-day offsite + recurring master + recurring exception
        assert_eq!(items.len(), 4);
        let ids: Vec<&str> = items.iter().map(|i| i.source_id.as_str()).collect();
        assert!(ids.contains(&"series-standup"), "recurring master ingests once");
        assert!(
            ids.contains(&"series-standup_20260825T130000Z"),
            "recurring exception ingests as its own atom"
        );
        let offsite = items
            .iter()
            .find(|i| i.source_id == "2w4l0d3c9i7g5e3b1r9x7f5s3u1z9v7j")
            .unwrap();
        assert_eq!(offsite.content, "Company Offsite");
        let standup = items.iter().find(|i| i.source_id == "series-standup").unwrap();
        // resource room (room-alpha) filtered out of participants
        assert_eq!(standup.participants.len(), 2);
        assert!(!standup.participants.iter().any(|p| p.contains("room-alpha")));
    }

    #[test]
    fn fixture_incremental_page_splits_update_from_cancellation() {
        let page: GEventsList =
            serde_json::from_str(EVENTS_LIST_INCREMENTAL_WITH_CANCELLATION).unwrap();
        assert!(page.next_sync_token.is_some());
        let (items, deleted) = process_events_page(page);
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].source_id, "1v3k9c2b8h6f4d2a0q8w6e4r2t0y8u6i");
        assert!(items[0].content.contains("rescheduled"));
        assert_eq!(deleted, vec!["2w4l0d3c9i7g5e3b1r9x7f5s3u1z9v7j".to_string()]);
    }

    // ── No-silent-truncation: the per-call budget boundary (contract §6
    // test #6) ───────────────────────────────────────────────────────────

    #[test]
    fn budget_exceeded_is_the_exact_boundary_run_pass_uses() {
        assert!(!budget_exceeded(1998, 0, 2000));
        assert!(budget_exceeded(1999, 1, 2000), "exactly at budget must stop");
        assert!(budget_exceeded(2000, 0, 2000));
        assert!(budget_exceeded(0, 2000, 2000), "deletions count toward the budget too");
        assert!(!budget_exceeded(0, 0, 2000));
    }

    // ── decide_after_page: terminal page always wins over budget
    // (CodeRabbit review finding on PR #677, CRITICAL, fixed before founder
    // review) ────────────────────────────────────────────────────────────

    #[test]
    fn decide_after_page_terminal_page_at_exactly_the_budget_still_completes() {
        // The exact bug: a page with NO next_page_token (terminal) that ALSO
        // happens to push the running total to/over budget must still
        // finish the pass and hand back the sync token — never discard it.
        let decision = decide_after_page(None, Some("new-sync-tok".to_string()), 2000, 0, 2000);
        match decision {
            PageDecision::Done { resume_page_token, new_sync_token } => {
                assert!(resume_page_token.is_none());
                assert_eq!(new_sync_token.as_deref(), Some("new-sync-tok"));
            }
            PageDecision::Continue(_) => panic!("terminal page must never continue"),
        }
    }

    #[test]
    fn decide_after_page_terminal_page_well_under_budget_also_completes() {
        let decision = decide_after_page(None, Some("tok".to_string()), 5, 0, 2000);
        match decision {
            PageDecision::Done { resume_page_token, new_sync_token } => {
                assert!(resume_page_token.is_none());
                assert_eq!(new_sync_token.as_deref(), Some("tok"));
            }
            PageDecision::Continue(_) => panic!("terminal page must never continue"),
        }
    }

    #[test]
    fn decide_after_page_non_terminal_page_over_budget_stops_with_resume_cursor() {
        let decision =
            decide_after_page(Some("next-page-tok".to_string()), None, 2000, 0, 2000);
        match decision {
            PageDecision::Done { resume_page_token, new_sync_token } => {
                assert_eq!(resume_page_token.as_deref(), Some("next-page-tok"));
                assert!(new_sync_token.is_none(), "budget cutoff never yields a sync token");
            }
            PageDecision::Continue(_) => panic!("must stop once over budget"),
        }
    }

    #[test]
    fn decide_after_page_non_terminal_page_under_budget_continues() {
        let decision = decide_after_page(Some("next-page-tok".to_string()), None, 5, 0, 2000);
        match decision {
            PageDecision::Continue(next) => assert_eq!(next, "next-page-tok"),
            PageDecision::Done { .. } => panic!("must keep paging while under budget"),
        }
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

    #[test]
    fn generate_channel_token_is_nonempty_and_varies() {
        let a = generate_channel_token();
        let b = generate_channel_token();
        assert!(!a.is_empty());
        assert_ne!(a, b);
    }

    // ── DB-level round trips (pg-gated) ─────────────────────────────────────
    //
    //   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
    //     cargo test -p coralswarm-backend --lib connectors::google_calendar::tests

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
        conn.execute(format!("CREATE DATABASE {db}").as_str())
            .await
            .unwrap();
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

    #[test]
    fn watch_row_is_live_requires_channel_id_and_token_and_future_expiry() {
        let now = Utc::now();
        let far = now + chrono::Duration::days(3);
        assert!(watch_row_is_live("chan", "tok", far, now));
        assert!(!watch_row_is_live("chan", "", far, now), "empty token can never authenticate a push");
        assert!(!watch_row_is_live("", "tok", far, now), "empty channel id is not a channel");
        assert!(!watch_row_is_live("chan", "tok", now - chrono::Duration::hours(1), now), "expired");
    }

    /// Prod's broken rows: non-empty `channel_id` (written by a renewal that
    /// sent `"token": ""`), EMPTY `channel_token`, future `expires_at`. They
    /// must read as not-live so `ensure_watch` replaces them.
    #[tokio::test]
    async fn channel_is_live_is_false_for_an_empty_token_row_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping channel_is_live_is_false_for_an_empty_token_row_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "gcal_tok_is_live";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        recreate(&base, db).await.close().await.ok();
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();
        let owner = seed_user(&pool).await;
        let account_id = seed_account(&pool, owner).await;
        let t = CalendarTarget::personal(&pool, account_id);
        let far = Utc::now() + chrono::Duration::days(6);

        assert!(!channel_is_live(&t).await.unwrap(), "no row => not live");

        upsert_channel(&t, "chan-broken", "res-broken", "", far).await.unwrap();
        assert!(
            !channel_is_live(&t).await.unwrap(),
            "non-empty channel_id + EMPTY channel_token + future expiry must NOT be live"
        );

        upsert_channel(&t, "chan-good", "res-good", "secret-token", far).await.unwrap();
        assert!(channel_is_live(&t).await.unwrap(), "a tokened, unexpired channel is live");

        pool.close().await;
        let mut admin_conn = PgConnection::connect(&base).await.unwrap();
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    /// The heal path at DB level: tokenless row (non-empty channel_id) is
    /// replaced by a NEW channel; every stored field reflects the new channel
    /// and the stop target is the OLD ids.
    #[tokio::test]
    async fn heal_replaces_tokenless_channel_and_targets_old_ids_for_stop_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping heal_replaces_tokenless_channel_and_targets_old_ids_for_stop_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "gcal_heal_path";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        recreate(&base, db).await.close().await.ok();
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();
        let owner = seed_user(&pool).await;
        let account_id = seed_account(&pool, owner).await;
        let member = seed_user(&pool).await;
        let t = CalendarTarget::org_member(&pool, &pool, account_id, member);
        let old_expiry = Utc::now() + chrono::Duration::days(6);
        let new_expiry = old_expiry + chrono::Duration::hours(3);

        upsert_channel(&t, "old-chan", "old-res", "", old_expiry).await.unwrap();
        let existing = load_channel_row(&t, "test").await.unwrap();
        let (id, _, tok, exp) = existing.clone().unwrap();
        assert!(!watch_row_is_live(&id, &tok, exp, Utc::now()), "precondition: broken row is not live");

        let stop = replace_channel(&t, existing, "new-chan", "new-res", "new-token", new_expiry).await.unwrap();
        assert_eq!(stop, Some(("old-chan".to_string(), "old-res".to_string())), "stop must target the OLD ids");

        let row = load_channel_row(&t, "test").await.unwrap().unwrap();
        assert_eq!(row.0, "new-chan");
        assert_eq!(row.1, "new-res");
        assert_eq!(row.2, "new-token");
        assert_eq!(row.3.timestamp(), new_expiry.timestamp(), "expires_at must be the NEW channel's");
        assert!(watch_row_is_live(&row.0, &row.2, row.3, Utc::now()));
        let n: i64 = sqlx::query_scalar("SELECT count(*) FROM google_watch_channels WHERE account_id = $1")
            .bind(account_id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(n, 1, "replaced in place, no duplicate row");

        pool.close().await;
        let mut admin_conn = PgConnection::connect(&base).await.unwrap();
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    #[test]
    fn old_channel_to_stop_never_targets_the_new_channel() {
        let ex = |i: &str, r: &str| Some((i.to_string(), r.to_string(), String::new(), Utc::now()));
        assert_eq!(old_channel_to_stop(&ex("o", "ro"), "n", "rn"), Some(("o".into(), "ro".into())));
        assert_eq!(old_channel_to_stop(&None, "n", "rn"), None);
        assert_eq!(old_channel_to_stop(&ex("", ""), "n", "rn"), None, "no real old channel");
        assert_eq!(old_channel_to_stop(&ex("n", "ro"), "n", "rn"), None, "same channel id");
        assert_eq!(old_channel_to_stop(&ex("o", "rn"), "n", "rn"), None, "shared resourceId: stop could hit the new channel");
    }

    #[tokio::test]
    async fn sync_columns_round_trip_preserves_channel_fields_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping sync_columns_round_trip_preserves_channel_fields_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "gws_g31_sync_columns";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let owner = seed_user(&pool).await;
        let account_id = seed_account(&pool, owner).await;

        // Establish a real channel first (channel_id/expires_at set).
        let far_future = Utc::now() + chrono::Duration::days(7);
        upsert_channel(&CalendarTarget::personal(&pool, account_id), "chan-1", "res-1", "tok-1", far_future)
            .await
            .unwrap();

        // A sync-columns write must NOT clobber the channel fields.
        persist_sync_complete(&CalendarTarget::personal(&pool, account_id), "sync-tok-a").await.unwrap();
        let row: (String, Option<String>, bool, String, DateTime<Utc>) = sqlx::query_as(
            "SELECT sync_token, backfill_page_token, backfill_complete, channel_id, expires_at \
             FROM google_watch_channels WHERE account_id = $1",
        )
        .bind(account_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(row.0, "sync-tok-a");
        assert!(row.1.is_none());
        assert!(row.2, "backfill_complete must be true after persist_sync_complete");
        assert_eq!(row.3, "chan-1", "channel_id must survive a sync-columns-only write");
        assert_eq!(row.4.timestamp(), far_future.timestamp(), "expires_at must survive too");

        // A mid-pass progress write must preserve the still-valid sync_token
        // (None means "leave it alone").
        persist_backfill_progress(&CalendarTarget::personal(&pool, account_id), Some("page-tok-1"))
            .await
            .unwrap();
        let row2: (String, Option<String>, bool) = sqlx::query_as(
            "SELECT sync_token, backfill_page_token, backfill_complete \
             FROM google_watch_channels WHERE account_id = $1",
        )
        .bind(account_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(row2.0, "sync-tok-a", "sync_token preserved across a page-token-only update");
        assert_eq!(row2.1.as_deref(), Some("page-tok-1"));
        assert!(!row2.2);

        // 410 restart clears the token explicitly.
        clear_sync_token(&CalendarTarget::personal(&pool, account_id)).await.unwrap();
        let row3: (String, Option<String>, bool) = sqlx::query_as(
            "SELECT sync_token, backfill_page_token, backfill_complete \
             FROM google_watch_channels WHERE account_id = $1",
        )
        .bind(account_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(row3.0, "");
        assert!(row3.1.is_none());
        assert!(!row3.2);

        pool.close().await;
        let mut admin_conn = PgConnection::connect(&base).await.unwrap();
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }

    #[tokio::test]
    async fn fresh_sync_columns_write_defaults_expires_at_to_now_pg() {
        // The self-healing bootstrap invariant the module doc's "Watch
        // bootstrap" section describes: a sync-only write on a row that has
        // never had a real channel leaves expires_at already "due", so the
        // renewal sweep picks it up.
        let Some(base) = base_url() else {
            eprintln!("skipping fresh_sync_columns_write_defaults_expires_at_to_now_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "gws_g31_fresh_row";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let owner = seed_user(&pool).await;
        let account_id = seed_account(&pool, owner).await;
        let before = Utc::now();

        persist_backfill_progress(&CalendarTarget::personal(&pool, account_id), None).await.unwrap();

        let row: (String, DateTime<Utc>) = sqlx::query_as(
            "SELECT channel_id, expires_at FROM google_watch_channels WHERE account_id = $1",
        )
        .bind(account_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(row.0, "", "no channel exists yet");
        assert!(
            row.1 <= before + chrono::Duration::seconds(5),
            "fresh row's expires_at must already be ~now (due), not far in the future"
        );

        pool.close().await;
        let mut admin_conn = PgConnection::connect(&base).await.unwrap();
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }

    #[tokio::test]
    async fn deletion_propagation_removes_the_matching_atom_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping deletion_propagation_removes_the_matching_atom_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "gws_g31_deletion";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let owner = seed_user(&pool).await;
        let account_id = seed_account(&pool, owner).await;
        // Matches `routes::slack::resolve_primary_graph`'s OWN auto-create
        // shape for the Personal-plane primary ocean exactly (`name`,
        // `kind='personal'`, `is_primary=true`, `owner_user_id` left NULL —
        // required: `graphs_member_requires_owner` CHECKs `(kind = 'member')
        // = (owner_user_id IS NOT NULL)`, so a non-'member' kind with a
        // non-NULL `owner_user_id` violates it). Found via a real pg run
        // (CodeRabbit-directed, 2026-08-23): the previous version of this
        // INSERT omitted the NOT NULL `name` column and set `owner_user_id`
        // on a `kind='personal'` row, violating that CHECK too — this test
        // had never actually executed against Postgres before that run.
        let graph_id: Uuid = sqlx::query_scalar(
            "INSERT INTO graphs (account_id, name, kind, is_primary) \
             VALUES ($1, 'Primary', 'personal', true) RETURNING id",
        )
        .bind(account_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        // `content_blob_key`/`timestamp` are NOT NULL with no default
        // (`migrations_control/0003_atoms.sql`) — `content` is not even a
        // real column (`content_inline`, added by `0014_content_inline.sql`,
        // is optional and unused here). Mirrors `routes::ingest`'s own
        // `insert_atom` pg-test helper exactly — INCLUDING the `:chunk:N`
        // source id every real ingested coral carries (fix/cal-deletions):
        // this test used to seed the bare event id, which is the one shape
        // production never stores, and so passed while the real path
        // matched nothing. Two chunks, to prove the whole coral goes.
        let atom_id: Uuid = sqlx::query_scalar(
            "INSERT INTO atoms (graph_id, source_type, source_id, content_blob_key, timestamp) \
             VALUES ($1, 'google_calendar', 'evt-del-1:chunk:0', 'blob-evt-del-1-0', now()) RETURNING atom_id",
        )
        .bind(graph_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        let atom_id_2: Uuid = sqlx::query_scalar(
            "INSERT INTO atoms (graph_id, source_type, source_id, content_blob_key, timestamp) \
             VALUES ($1, 'google_calendar', 'evt-del-1:chunk:1', 'blob-evt-del-1-1', now()) RETURNING atom_id",
        )
        .bind(graph_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        // A bystander whose id merely shares a prefix must survive (the
        // LIKE pattern is anchored on `:chunk:`).
        let bystander: Uuid = sqlx::query_scalar(
            "INSERT INTO atoms (graph_id, source_type, source_id, content_blob_key, timestamp) \
             VALUES ($1, 'google_calendar', 'evt-del-10:chunk:0', 'blob-evt-del-10', now()) RETURNING atom_id",
        )
        .bind(graph_id)
        .fetch_one(&pool)
        .await
        .unwrap();

        propagate_deletions(&CalendarTarget::personal(&pool, account_id), &["evt-del-1".to_string()]).await.unwrap();

        let still_there: i64 = sqlx::query_scalar("SELECT count(*) FROM atoms WHERE atom_id = ANY($1)")
            .bind(vec![atom_id, atom_id_2])
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(still_there, 0, "cancelled calendar event must delete every chunk atom of its coral");
        let bystander_left: i64 = sqlx::query_scalar("SELECT count(*) FROM atoms WHERE atom_id = $1")
            .bind(bystander)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(bystander_left, 1, "a prefix-sharing event id is not the cancelled event");

        pool.close().await;
        let mut admin_conn = PgConnection::connect(&base).await.unwrap();
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }

    /// Ported from `google_gmail::tests::
    /// deletion_propagation_returns_err_when_the_graph_lookup_fails_pg`
    /// (CodeRabbit round 2 finding #1 on PR #686, Major) — the identical
    /// latent gap this PR fixes for calendar. Forces a REAL SQL-level
    /// failure (dropping `graphs`, the table `resolve_primary_graph`'s
    /// Personal-plane branch reads, out from under the pending `SELECT` —
    /// deterministic, no mocking infra needed) rather than asserting
    /// against a synthetic error. `CASCADE` is required because `atoms.
    /// graph_id` carries a `REFERENCES graphs(id)` foreign key; it drops
    /// that constraint, not the `atoms` table itself, so the
    /// belt-and-suspenders atom-survives assertion below still works.
    ///
    /// This is deliberately a UNIT-level proof, not a `fetch_pending`-level
    /// one: `fetch_pending`'s own call site is `propagate_deletions(pool,
    /// account_id, &result.deleted_ids).await?` (see that function, a few
    /// lines above this test module) — a bare `?` — so "the cursor is left
    /// unchanged" is a structural, compiler-enforced guarantee once THIS
    /// function is proven to return `Err` on a real failure (identical
    /// reasoning to the Gmail test this was ported from; an end-to-end
    /// version would need a live/mocked Google Calendar `events.list` HTTP
    /// round trip this crate does not have).
    #[tokio::test]
    async fn deletion_propagation_returns_err_when_the_graph_lookup_fails_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping deletion_propagation_returns_err_when_the_graph_lookup_fails_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "gws_g31_deletion_lookup_fails";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let owner = seed_user(&pool).await;
        let account_id = seed_account(&pool, owner).await;
        let graph_id: Uuid = sqlx::query_scalar(
            "INSERT INTO graphs (account_id, name, kind, is_primary) \
             VALUES ($1, 'Primary', 'personal', true) RETURNING id",
        )
        .bind(account_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        let atom_id: Uuid = sqlx::query_scalar(
            "INSERT INTO atoms (graph_id, source_type, source_id, content_blob_key, timestamp) \
             VALUES ($1, 'google_calendar', 'evt-del-fail', 'blob-evt-del-fail', now()) RETURNING atom_id",
        )
        .bind(graph_id)
        .fetch_one(&pool)
        .await
        .unwrap();

        // Force a genuine SQL-level failure: the table
        // `resolve_primary_graph`'s Personal-plane branch depends on is
        // simply gone. CASCADE drops the dependent `atoms.graph_id` FK
        // constraint, not the `atoms` table.
        sqlx::query("DROP TABLE graphs CASCADE").execute(&pool).await.unwrap();

        let result = propagate_deletions(&CalendarTarget::personal(&pool, account_id), &["evt-del-fail".to_string()]).await;
        assert!(
            result.is_err(),
            "a genuine graph-resolution failure must return Err, never swallow it as Ok(())"
        );

        // Belt-and-suspenders: the atom that WOULD have been deleted is
        // still there — the failure aborted before any delete_source call.
        let still_there: i64 = sqlx::query_scalar("SELECT count(*) FROM atoms WHERE atom_id = $1")
            .bind(atom_id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(still_there, 1, "no deletion may have been attempted once graph resolution itself failed");

        pool.close().await;
        let mut admin_conn = PgConnection::connect(&base).await.unwrap();
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }

    // ── resolve_channel_for_push: empty channel_id/channel_token rows must
    // never authenticate (CodeRabbit review finding on PR #677, Major/
    // security, fixed before founder review) ───────────────────────────────

    #[tokio::test]
    async fn resolve_channel_for_push_never_matches_an_empty_placeholder_row_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping resolve_channel_for_push_never_matches_an_empty_placeholder_row_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "gws_g31_empty_channel";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let owner = seed_user(&pool).await;
        let account_id = seed_account(&pool, owner).await;

        // A sync-only write (no channel created yet) — exactly the shape
        // `upsert_sync_columns`'s doc describes: channel_id/channel_token
        // default to '' on a fresh row.
        persist_backfill_progress(&CalendarTarget::personal(&pool, account_id), None).await.unwrap();
        let (placeholder_channel_id, placeholder_token): (String, String) = sqlx::query_as(
            "SELECT channel_id, channel_token FROM google_watch_channels WHERE account_id = $1",
        )
        .bind(account_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(placeholder_channel_id, "", "precondition: fresh row has an empty channel_id");
        assert_eq!(placeholder_token, "", "precondition: fresh row has an empty channel_token");

        // The empty-string query path (Rust-level short circuit).
        assert!(
            resolve_channel_for_push(&pool, "").await.unwrap().is_none(),
            "an empty channel_id must never resolve to any account"
        );

        // Belt-and-suspenders: even calling the raw SQL resolver directly
        // with an empty channel_id must return NULL, not this account's id —
        // proves the DB-level guard (migration 0183), independent of the
        // Rust-level short circuit above.
        let via_sql: Option<Uuid> =
            sqlx::query_scalar("SELECT resolve_google_watch_channel_account_id($1)")
                .bind("")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert!(via_sql.is_none(), "the SQL resolver itself must exclude empty channel_id rows");

        // A real, non-empty channel on the SAME account is unaffected —
        // this isn't a blanket "channels never resolve" regression.
        let far_future = Utc::now() + chrono::Duration::days(7);
        upsert_channel(&CalendarTarget::personal(&pool, account_id), "real-chan-1", "res-1", "real-secret-tok", far_future)
            .await
            .unwrap();
        let resolved = resolve_channel_for_push(&pool, "real-chan-1").await.unwrap();
        assert_eq!(resolved, Some((account_id, None, "real-secret-tok".to_string())));

        pool.close().await;
        let mut admin_conn = PgConnection::connect(&base).await.unwrap();
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }

    // ── idx_google_watch_channels_channel_id_unique: structurally prevents
    // the cross-account collision `resolve_channel_for_push`'s `LIMIT 1`
    // (no `ORDER BY`) can't otherwise defend against (CodeRabbit review
    // finding on PR #677, round 2, residual on the round-1 fix; migration
    // 0182, edited in place — unmerged) ─────────────────────────────────

    #[tokio::test]
    async fn duplicate_non_empty_channel_id_across_accounts_is_rejected_by_the_db_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping duplicate_non_empty_channel_id_across_accounts_is_rejected_by_the_db_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "gws_g31_channel_id_unique";
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

        upsert_channel(&CalendarTarget::personal(&pool, account_a), "shared-chan-id", "res-a", "tok-a", far_future)
            .await
            .unwrap();

        // Same channel_id, a DIFFERENT account — must be rejected at the DB
        // level, not silently accepted (which would let
        // resolve_channel_for_push's LIMIT-1-no-ORDER-BY lookup pick either
        // account arbitrarily for a real inbound notification).
        let err = upsert_channel(&CalendarTarget::personal(&pool, account_b), "shared-chan-id", "res-b", "tok-b", far_future)
            .await
            .expect_err("a duplicate non-empty channel_id across accounts must violate the unique index");
        assert!(
            err.to_string().to_lowercase().contains("duplicate")
                || err.to_string().to_lowercase().contains("unique")
                || err.to_string().contains("idx_google_watch_channels_channel_id_unique"),
            "expected a unique-constraint violation, got: {err}"
        );

        // Two DIFFERENT empty-channel (placeholder) rows on two accounts are
        // explicitly fine — the partial index excludes `channel_id = ''`.
        persist_backfill_progress(&CalendarTarget::personal(&pool, account_a), None).await.unwrap();
        persist_backfill_progress(&CalendarTarget::personal(&pool, account_b), None).await.unwrap();

        pool.close().await;
        let mut admin_conn = PgConnection::connect(&base).await.unwrap();
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }

    // ── Google Calendar org lift: pure helpers + org deletion propagation ──

    // `#[tokio::test]`: `connect_lazy` needs a Tokio context to construct.
    #[tokio::test]
    async fn resource_id_is_primary_for_personal_and_per_member_for_an_org_member() {
        let pool = PgPoolOptions::new().connect_lazy("postgres://localhost/unused").expect("lazy");
        let account = Uuid::new_v4();
        let member = Uuid::new_v4();
        assert_eq!(CalendarTarget::personal(&pool, account).resource_id(), "primary");
        let t = CalendarTarget::org_member(&pool, &pool, account, member);
        assert_eq!(t.resource_id(), format!("primary:{member}"));
        assert_ne!(
            t.resource_id(),
            CalendarTarget::org_member(&pool, &pool, account, Uuid::new_v4()).resource_id(),
            "two members of one account must key two distinct watch rows"
        );
    }

    #[test]
    fn calendar_id_of_strips_the_member_suffix_so_it_never_reaches_googles_url() {
        let member = Uuid::new_v4();
        assert_eq!(calendar_id_of("primary"), "primary");
        assert_eq!(calendar_id_of(&format!("primary:{member}")), "primary");
    }

    /// Org deletion propagation: a cancelled event retracts ONLY the
    /// cancelling member's own atom in THEIR ocean on the org tenant pool —
    /// another member's atom with the SAME source id (two people invited to
    /// one event) survives, and the account owner's ocean is never consulted.
    #[tokio::test]
    async fn org_deletion_propagation_targets_only_the_members_own_ocean_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping org_deletion_propagation_targets_only_the_members_own_ocean_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "gcal_org_deletion_ten";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let tenant: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_tenant").run(&tenant).await.unwrap();
        sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, 'org_gcal_del')")
            .execute(&tenant)
            .await
            .unwrap();

        let account_id = Uuid::new_v4();
        let alice = Uuid::new_v4();
        let bob = Uuid::new_v4();
        let mut graph_ids = Vec::new();
        let mut atom_ids = Vec::new();
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
            let a: Uuid = sqlx::query_scalar(
                "INSERT INTO atoms (graph_id, source_type, source_id, content_blob_key, timestamp) \
                 VALUES ($1, 'google_calendar', 'shared-evt:chunk:0', $2, now()) RETURNING atom_id",
            )
            .bind(g)
            .bind(format!("blob-{u}"))
            .fetch_one(&tenant)
            .await
            .unwrap();
            graph_ids.push(g);
            atom_ids.push(a);
        }

        // Alice's cancellation. `control` is unused by deletion propagation;
        // the tenant pool stands in so the target is well-formed.
        let t = CalendarTarget::org_member(&tenant, &tenant, account_id, alice);
        propagate_deletions(&t, &["shared-evt".to_string()]).await.unwrap();

        let alice_left: i64 = sqlx::query_scalar("SELECT count(*) FROM atoms WHERE atom_id = $1")
            .bind(atom_ids[0])
            .fetch_one(&tenant)
            .await
            .unwrap();
        let bob_left: i64 = sqlx::query_scalar("SELECT count(*) FROM atoms WHERE atom_id = $1")
            .bind(atom_ids[1])
            .fetch_one(&tenant)
            .await
            .unwrap();
        assert_eq!(alice_left, 0, "the cancelling member's own atom is retracted");
        assert_eq!(bob_left, 1, "another member's atom for the same event id must survive");

        tenant.close().await;
        let mut admin_conn = PgConnection::connect(&base).await.unwrap();
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }

    /// The SAME confinement, but as the real NOBYPASSRLS `coralswarm_tenant_app`
    /// role (CodeRabbit): an account-scoped transaction with no tenant actor
    /// cannot see a member's private graph, so the cascade must run
    /// system-scoped end to end or it would find nothing and "succeed".
    /// Also proves the connector compliance audit row is written.
    #[tokio::test]
    async fn org_deletion_propagation_works_as_the_real_tenant_app_role_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping org_deletion_propagation_works_as_the_real_tenant_app_role_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "gcal_org_deletion_role_ten";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let owner_pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_tenant").run(&owner_pool).await.unwrap();
        sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, 'org_gcal_del_role')")
            .execute(&owner_pool)
            .await
            .unwrap();
        let pw = "gcal_org_deletion_tenant_role_test_only_pw";
        let stmt = format!("ALTER ROLE coralswarm_tenant_app WITH PASSWORD '{pw}'");
        let mut ok = false;
        for attempt in 0..10u32 {
            if sqlx::query(&stmt).execute(&owner_pool).await.is_ok() {
                ok = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50 * u64::from(attempt + 1))).await;
        }
        assert!(ok, "set test password on coralswarm_tenant_app");
        let opts = <sqlx::postgres::PgConnectOptions as std::str::FromStr>::from_str(&with_db(&base, db))
            .unwrap()
            .username("coralswarm_tenant_app")
            .password(pw);
        let app = PgPoolOptions::new().max_connections(2).connect_with(opts).await.expect("connect AS coralswarm_tenant_app");

        let account_id = Uuid::new_v4();
        let (alice, bob) = (Uuid::new_v4(), Uuid::new_v4());
        let mut atoms = Vec::new();
        for u in [alice, bob] {
            let g: Uuid = sqlx::query_scalar(
                "INSERT INTO graphs(account_id, name, kind, is_primary, owner_user_id) \
                 VALUES ($1, 'member ocean', 'member', true, $2) RETURNING id",
            )
            .bind(account_id)
            .bind(u)
            .fetch_one(&owner_pool)
            .await
            .unwrap();
            let a: Uuid = sqlx::query_scalar(
                "INSERT INTO atoms (graph_id, source_type, source_id, content_blob_key, timestamp) \
                 VALUES ($1, 'google_calendar', 'evt:chunk:0', $2, now()) RETURNING atom_id",
            )
            .bind(g)
            .bind(format!("blob-{u}"))
            .fetch_one(&owner_pool)
            .await
            .unwrap();
            atoms.push(a);
        }

        let t = CalendarTarget::org_member(&app, &app, account_id, alice);
        propagate_deletions(&t, &["evt".to_string()]).await.unwrap();

        let left: Vec<Uuid> = sqlx::query_scalar("SELECT atom_id FROM atoms ORDER BY atom_id")
            .fetch_all(&owner_pool)
            .await
            .unwrap();
        assert_eq!(left, vec![atoms[1]], "alice's atom retracted as the app role; bob's survives");
        let audit: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM connector_audit_log WHERE account_id = $1 AND action = 'deletion_propagated'",
        )
        .bind(account_id)
        .fetch_one(&owner_pool)
        .await
        .unwrap();
        assert_eq!(audit, 1, "the compliance audit row is written on the org path");

        app.close().await;
        owner_pool.close().await;
        let mut admin_conn = PgConnection::connect(&base).await.unwrap();
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }
}
