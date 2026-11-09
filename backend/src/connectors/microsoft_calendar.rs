//! Microsoft Calendar ingest via Graph `calendarView/delta` — the Microsoft
//! twin of [`super::google_calendar`].
//!
//! # The bug this replaces (verified live in prod 2026-10-07)
//!
//! `connectors::microsoft::fetch_events` was a cursorless
//! `GET /me/events?$top=20`: a list of what currently exists. An Outlook
//! event the user deleted — or a meeting the organiser cancelled — simply
//! stopped appearing, and nothing ever told the ocean, so its coral stayed
//! forever (an event created, renamed, then deleted on an org member's
//! calendar left the renamed coral behind). Both callers had the gap:
//! `workers::microsoft_calendar_sync::sync_member` (org members, 30-minute
//! tick) and `routes::connectors::finish_ingest` (manual
//! `POST /v1/connectors/microsoft/sync`, personal plane and org member).
//! Google Calendar never did — its `syncToken` pass reports
//! `status: cancelled` and `google_calendar::propagate_deletions` retracts.
//!
//! # How it works now
//!
//! `GET /me/calendarView/delta?startDateTime=..&endDateTime=..` (delegated
//! `Calendars.Read`, already in [`super::microsoft`]'s default scopes — no
//! new consent). The first round lists every event in the window
//! ([`WINDOW_PAST_DAYS`] back → [`WINDOW_FUTURE_DAYS`] ahead), paging
//! `@odata.nextLink`, and ends with an `@odata.deltaLink`. Every later
//! round GETs that stored link and receives only what changed since:
//! created/updated events as full `event` resources, deleted ones as
//! `{"id": .., "@removed": {"reason": "deleted"}}`. The link is persisted
//! on `microsoft_calendar_sync_state` (`delta_link`, control `0272` /
//! tenant `0113`) exactly as Google's `syncToken` is persisted on
//! `google_watch_channels`; a round the per-pass page budget cuts off
//! persists its `nextLink` as `delta_resume_link` and continues next pass.
//!
//! What a pass does with the delta ([`fetch_pending`]):
//!
//! * a `singleInstance` event → an item for the caller's ingest loop, which
//!   runs `services::source_reingest::retract_if_changed_item` first, so an
//!   EDIT replaces the coral and an unchanged re-report is a no-op;
//! * `isCancelled: true` → retracted, same as deleted;
//! * an `occurrence`/`exception` (what `calendarView` expands a recurring
//!   series into) → its `seriesMasterId` is fetched once per pass and
//!   handled as one event — see "Recurring series";
//! * `@removed` → **verified before retracting**: Graph documents that
//!   `@removed` also lists events OUTSIDE the window that were added,
//!   edited or deleted, so a removed id is only trusted once
//!   `GET /me/events/{id}` says 404 (or `isCancelled`). A removed id that
//!   is still live is re-shaped and handed to the ingest loop (which keeps
//!   an out-of-window coral's content in step through the edit path);
//! * a `@removed` id with no stored coral is "unexplained" — the signature
//!   of a deleted recurring series (its occurrences are removed, the stored
//!   series-master id never is) — and triggers a [`reconcile`].
//!
//! Deletions are applied eagerly inside [`fetch_pending`] (idempotent, like
//! Google's); the cursor is handed back as a [`PendingCommit`] for the
//! caller to persist once its ingest loop succeeded ([`commit_pending`]) —
//! `workers::microsoft_calendar_sync::ingest_and_commit`, the one ingest
//! loop both the scheduled sweep and the manual route run, commits only on
//! a clean batch. A retraction failure is an `Err`, so the cursor is NOT
//! advanced and the next pass re-diffs the same range.
//!
//! # Content hash stability
//!
//! `calendarView/delta` does not support `$select`: Graph documents that it
//! "return[s] the same properties you'd normally get from a
//! `GET /calendarView` request" (`event: delta`, v1.0 reference) — the full
//! resource, `body` included, HTML by default. The per-event fetch here
//! `$select`s [`super::microsoft::EVENT_SELECT`], `body` among them, and
//! [`super::microsoft::shape_graph_event`] renders only those fields, so
//! the two sources produce byte-identical content for an unchanged event
//! and `retract_if_changed` sees `Unchanged` — no churn, and no extra Graph
//! call per event to get the body.
//!
//! The description rendered is the whole `body`, not `bodyPreview`
//! (Graph's ~255-character excerpt — an agenda edit past that point used to
//! leave the hash unchanged and the coral stale). A coral ingested from the
//! preview is therefore replaced ONCE, on the first pass that reports its
//! event, unless its body text equals its preview (short descriptions),
//! in which case it shapes identically and is not touched. Pre-`0271`
//! corals (NULL hash) are replaced once regardless, as #1156 documents.
//!
//! # Recurring series: one coral per series, never per occurrence
//!
//! The legacy list returned series MASTERS (one coral for "Standup, weekly"),
//! the same model `google_calendar` chose with `singleEvents=false`. The
//! calendar view expands series into occurrences, which would turn a daily
//! standup into ~90 corals per window; this module keeps the master model
//! by mapping every occurrence back to its `seriesMasterId` and fetching the
//! master (`GET /me/events/{masterId}`), so the stored coral is still keyed
//! by the master id — existing series corals are recognised, not
//! duplicated. The cost is one Graph GET per DISTINCT series per pass
//! (occurrences of one series are collapsed first).
//!
//! # Reconciliation: how a deleted series — and the pre-deploy leftovers —
//! get retracted
//!
//! The reconcile sweep verifies the distinct `microsoft_calendar` source
//! ids stored in the target graph against Graph (`GET /me/events/{id}` →
//! 404 or `isCancelled` → retract; live → kept, and re-shaped for the edit
//! path). A deleted event is NOT in a full listing, so "absent from the
//! window" alone is never evidence — only Graph's 404 is — which is why
//! corals for events legitimately outside the window are never touched.
//!
//! It is a resumable, circular walk, not a per-pass set difference
//! ([`plan_verification`], state on `microsoft_calendar_sync_state`:
//! `delta_verify_cursor` + `delta_verify_lap_end`, control `0276` / tenant
//! `0115`). The stored ids are a sorted circle; a lap starts at the
//! cursor's current position ([`VerifySweep::start_lap`]) and each
//! reconcile-eligible pass verifies the next [`VERIFY_CAP`] ids after the
//! cursor — wrapping to the smallest — minus whatever the pass itself just
//! handled, until the lap closes. A lap is started (or, if one is in
//! progress, extended to a full lap from the current cursor — never
//! restarted from a fixed position) by:
//!
//! * the completion of a FULL round (first sync after deploy, a
//!   `410`/`syncStateNotFound` restart, or a window re-anchor) — the
//!   one-time cleanup after deploy: every coral the old cursorless fetch
//!   left behind is verified and the ones whose event no longer exists are
//!   retracted;
//! * a pass carrying unexplained `@removed` ids — the signature of a
//!   deleted series.
//!
//! The guarantee this buys: every stored id is verified within one lap of
//! a trigger — at most ⌈N / VERIFY_CAP⌉ passes for N stored events — no
//! matter how many passes the round took or how the id sorts. The
//! previous shape ("every stored id minus what THIS pass saw, first 200 in
//! sorted order") lost that on any multi-pass full round: a full round is
//! cut every [`PAGES_PER_PASS`] pages and `calendarView` expands series
//! (a daily standup is ~325 occurrences over the window), so busy
//! calendars routinely take 2–3 passes, only the last of which counted as
//! "seen"; the cap then always hit and the same 200 lowest-sorting ids
//! were verified every time, so a deleted series whose master sorted
//! after them was never retracted — reversing the deletion guarantee.
//!
//! Cost is bounded by construction: ≤ [`VERIFY_CAP`] GETs per pass while
//! a lap is open, nothing otherwise, and a trigger during a lap extends it
//! rather than restarting it, so repeated triggers cost one lap, not one
//! lap each. `delta_reconciled_at` records when the last lap started
//! (observability only; it no longer throttles). Every call rides the
//! shared `graph_rate_limit` scope the callers establish.
//!
//! A verify `GET` that Graph refuses for THAT id — a `4xx` other than `401`
//! (the token, not the id), `404` (gone), `408` (timed out) and `429`
//! (throttled) — is skipped
//! and the lap moves on ([`EventLookup::Rejected`]): the coral is neither
//! retracted nor re-shaped, and the next lap asks again. Aborting the pass
//! on it, as the first revision did, let one stored id Graph answers `400`
//! for (a malformed legacy id, an id from a calendar the user lost access
//! to) stall the connection's sync for good. `5xx`/`408`/`429`/transport errors
//! still abort: they say nothing about the id and the next pass retries.
//!
//! # History window
//!
//! Google Calendar's listing has no `timeMin`: the whole calendar is
//! ingested. `calendarView/delta` requires a range (the range-free
//! `events/delta` is beta-only), and the Exchange calendar view behind it
//! refuses a range over two years (EWS `ErrorCalendarViewRangeTooBig`), so
//! the whole history is not an option here. The window is
//! [`WINDOW_PAST_DAYS`] back → [`WINDOW_FUTURE_DAYS`] ahead: a year of
//! past meetings — the first version's 30 days meant anything older was
//! never ingested and an edit or deletion of it never propagated — plus the
//! lookahead, 455 days in all. A full listing is `calendarView`-expanded
//! occurrences, so a busy calendar is a few thousand entries per round, a
//! handful of passes at [`PAGES_PER_PASS`] × 50.
//!
//! # Window re-anchoring
//!
//! The window is frozen into the delta token the moment the first request
//! is made, so an event created beyond `endDateTime` would never be
//! reported. [`plan_round`] starts a fresh full round (new window, new
//! token, reconcile at the end) once fewer than [`REANCHOR_MARGIN_DAYS`] of
//! lookahead remain — every ~30 days with the defaults — and whenever the
//! frozen window is NARROWER than the configured one (`delta_window_start`
//! + `delta_window_end`, control `0273` / tenant `0114`; a row with no
//! recorded start counts as narrower): a cursor anchored on the old 30-day
//! window, or on any later-widened constants, backfills on its next pass
//! instead of waiting for the scheduled re-anchor. That round is
//! idempotent — an event already stored is `Unchanged` (or replaced once
//! if its content changed), never duplicated.
//!
//! # Scope: ONE graph, never another member's
//!
//! Every lookup and retraction is confined to [`CalendarTarget::graph_id`]
//! — the member's own private ocean on the org plane
//! (`services::deletion::delete_atoms_in_graph_only`, no source-identity
//! expansion: Graph gives every attendee the same event id and each member
//! holds their OWN coral for it), the account's primary ocean on the
//! personal plane (`delete_source`, account-wide like Google's personal
//! path, resolved to the CONCRETE `:chunk:N` ids first — see
//! `services::deletion::concrete_source_ids`).

use std::collections::BTreeSet;

use chrono::{DateTime, Duration, Utc};
use sqlx::PgPool;
use uuid::Uuid;

use super::microsoft::{graph_send, shape_graph_event, GraphEvent, CALENDAR_SOURCE_TYPE, EVENT_SELECT, GRAPH};
use super::{ConnectorError, NormalizedItem};
use crate::routes::slack::{resolve_primary_graph, AccountPlane, IngestCaller};
use crate::services::blob_store::PromotionBlobStore;

/// How far back the delta window starts on a full round. Past events are
/// what the ocean mostly cares about (meetings that happened); a year, with
/// [`WINDOW_FUTURE_DAYS`] on top, stays well inside the two-year ceiling
/// the Exchange calendar view enforces — see the module doc, "History
/// window". Widening this later is safe: a cursor frozen on the narrower
/// window re-anchors on its next pass ([`plan_round`]).
pub(crate) const WINDOW_PAST_DAYS: i64 = 365;
/// How far ahead the window reaches. Together with [`REANCHOR_MARGIN_DAYS`]
/// this sets the re-anchor cadence (90 − 60 = every 30 days).
pub(crate) const WINDOW_FUTURE_DAYS: i64 = 90;
/// Start a fresh full round once fewer than this many days of lookahead
/// remain in the frozen window.
pub(crate) const REANCHOR_MARGIN_DAYS: i64 = 60;
/// `Prefer: odata.maxpagesize` for every delta request.
const PAGE_SIZE: usize = 50;
/// Pages fetched per pass before persisting `delta_resume_link` and
/// returning — 1000 events; the same "no silent truncation, explicit resume"
/// posture as `google_calendar::SYNC_PASS_BUDGET`.
pub(crate) const PAGES_PER_PASS: usize = 20;
/// Max `GET /me/events/{id}` verifications one pass performs — the pace of
/// the reconcile sweep, not a bound on what it covers (the module doc,
/// "Reconciliation").
pub(crate) const VERIFY_CAP: usize = 200;

// ---------------------------------------------------------------------------
// Target — which account/owner/plane a Calendar pass is for
// ---------------------------------------------------------------------------

/// Who a Microsoft Calendar pass is for and where its state lives. Unlike
/// [`super::google_calendar::CalendarTarget`] there is ONE pool: Microsoft
/// Calendar has no push channel to route from the control plane, so the
/// cursor row (`microsoft_calendar_sync_state`) lives next to the atoms —
/// the org's tenant pool for a member, the consumer pool for a personal
/// account.
#[derive(Clone, Copy)]
pub(crate) struct CalendarTarget<'a> {
    /// Content pool: owns `graphs`, `atoms`, `microsoft_calendar_sync_state`.
    pub(crate) pool: &'a PgPool,
    pub(crate) account_id: Uuid,
    /// The member (org) or the account owner (personal) — the cursor row's
    /// key together with `account_id`.
    pub(crate) owner_user_id: Uuid,
    /// The ONE graph events land in and deletions are confined to.
    pub(crate) graph_id: Uuid,
    pub(crate) plane: AccountPlane,
}

impl<'a> CalendarTarget<'a> {
    /// The personal account-wide connection: keyed on the account's owner,
    /// landing in the account's primary ocean (resolved like Google's
    /// `propagate_deletions` personal arm).
    pub(crate) async fn personal(pool: &'a PgPool, account_id: Uuid) -> Result<Self, ConnectorError> {
        let owner_user_id = super::microsoft::account_owner_user(pool, account_id)
            .await?
            .ok_or_else(|| {
                ConnectorError::Internal(format!("microsoft calendar: account {account_id} has no owner_user_id"))
            })?;
        let graph_id = resolve_primary_graph(
            pool,
            account_id,
            AccountPlane::Personal,
            IngestCaller::Attributable(owner_user_id),
        )
        .await
        .map_err(|e| ConnectorError::Internal(format!("microsoft calendar: graph resolution failed: {e}")))?;
        Ok(Self { pool, account_id, owner_user_id, graph_id, plane: AccountPlane::Personal })
    }

    /// One org member's own connection, for a caller that already resolved
    /// the member's private ocean (`workers::microsoft_calendar_sync::
    /// sync_member` — which never auto-creates one: an unprovisioned member
    /// is skipped there, not failed).
    pub(crate) fn org_member_resolved(
        pool: &'a PgPool,
        account_id: Uuid,
        owner_user_id: Uuid,
        graph_id: Uuid,
    ) -> Self {
        Self { pool, account_id, owner_user_id, graph_id, plane: AccountPlane::Org }
    }
}

/// Same scoping every background connector write uses — see
/// `google_calendar::scoped_tx`.
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
// Sync-cursor persistence (`microsoft_calendar_sync_state`, 0272 / 0113)
// ---------------------------------------------------------------------------

/// The `[startDateTime, endDateTime]` range a delta token is frozen on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Window {
    pub(crate) start: DateTime<Utc>,
    pub(crate) end: DateTime<Utc>,
}

impl Window {
    /// The window a full round anchors at `now`.
    pub(crate) fn configured(now: DateTime<Utc>) -> Self {
        Self { start: now - Duration::days(WINDOW_PAST_DAYS), end: now + Duration::days(WINDOW_FUTURE_DAYS) }
    }

    fn span(&self) -> Duration {
        self.end - self.start
    }
}

/// Where the reconcile sweep stands — see the module doc, "Reconciliation",
/// and [`plan_verification`] for the walk itself.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct VerifySweep {
    /// The last stored id verified; the next batch starts after it in
    /// sorted order, wrapping to the smallest id. `None` reads as "before
    /// the smallest id". Kept between laps so they rotate.
    pub(crate) cursor: Option<String>,
    /// The id the lap in progress ends at (inclusive, in circular order
    /// from the cursor). `None` = no lap in progress.
    pub(crate) lap_end: Option<String>,
}

impl VerifySweep {
    /// Begin (or, if a lap is in progress, extend to) one full lap from the
    /// current cursor.
    pub(crate) fn start_lap(&mut self) {
        self.lap_end = Some(self.cursor.clone().unwrap_or_default());
    }

    pub(crate) fn lap_in_progress(&self) -> bool {
        self.lap_end.is_some()
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct SyncState {
    /// `@odata.deltaLink` of the last completed round.
    pub(crate) delta_link: Option<String>,
    /// `@odata.nextLink` of a round in progress (page budget hit).
    pub(crate) resume_link: Option<String>,
    /// The window the current round is anchored at. `None` when either
    /// bound is unrecorded — a row anchored before `0273`/`0114` stored the
    /// start — and [`plan_round`] then re-anchors.
    pub(crate) window: Option<Window>,
    /// When the last reconcile lap started.
    pub(crate) reconciled_at: Option<DateTime<Utc>>,
    pub(crate) sweep: VerifySweep,
}

pub(crate) async fn load_sync_state(t: &CalendarTarget<'_>) -> Result<SyncState, ConnectorError> {
    // `(delta_link, delta_resume_link, delta_window_start, delta_window_end,
    // delta_reconciled_at, delta_verify_cursor, delta_verify_lap_end)`.
    type Row = (
        Option<String>,
        Option<String>,
        Option<DateTime<Utc>>,
        Option<DateTime<Utc>>,
        Option<DateTime<Utc>>,
        Option<String>,
        Option<String>,
    );
    let mut tx = scoped_tx(t.pool, t.account_id, "load_sync_state").await?;
    let row: Option<Row> = sqlx::query_as(
        "SELECT delta_link, delta_resume_link, delta_window_start, delta_window_end, delta_reconciled_at, \
                delta_verify_cursor, delta_verify_lap_end \
         FROM microsoft_calendar_sync_state WHERE account_id = $1 AND owner_user_id = $2",
    )
    .bind(t.account_id)
    .bind(t.owner_user_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("load_sync_state: {e}")))?;
    tx.rollback().await.ok();
    Ok(row
        .map(|(delta_link, resume_link, window_start, window_end, reconciled_at, cursor, lap_end)| SyncState {
            delta_link,
            resume_link,
            window: window_start.zip(window_end).map(|(start, end)| Window { start, end }),
            reconciled_at,
            sweep: VerifySweep { cursor, lap_end },
        })
        .unwrap_or_default())
}

/// Upsert the cursor columns. `delta_link`/`resume_link` are written as
/// given (`None` clears); `window: None` PRESERVES the stored bounds (an
/// incremental round never re-anchors). `graph_id` is refreshed on
/// conflict like `workers::microsoft_calendar_sync::sync_enabled` does;
/// `sync_enabled` is left to its default/existing value.
async fn upsert_cursor(
    t: &CalendarTarget<'_>,
    delta_link: Option<&str>,
    resume_link: Option<&str>,
    window: Option<Window>,
) -> Result<(), ConnectorError> {
    let mut tx = scoped_tx(t.pool, t.account_id, "upsert_cursor").await?;
    sqlx::query(
        r#"
        INSERT INTO microsoft_calendar_sync_state
            (account_id, owner_user_id, graph_id, delta_link, delta_resume_link,
             delta_window_start, delta_window_end, updated_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, now())
        ON CONFLICT (account_id, owner_user_id)
        DO UPDATE SET graph_id = EXCLUDED.graph_id,
                      delta_link = $4,
                      delta_resume_link = $5,
                      delta_window_start = COALESCE($6, microsoft_calendar_sync_state.delta_window_start),
                      delta_window_end = COALESCE($7, microsoft_calendar_sync_state.delta_window_end),
                      updated_at = now()
        "#,
    )
    .bind(t.account_id)
    .bind(t.owner_user_id)
    .bind(t.graph_id)
    .bind(delta_link)
    .bind(resume_link)
    .bind(window.map(|w| w.start))
    .bind(window.map(|w| w.end))
    .execute(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("upsert_cursor: {e}")))?;
    tx.commit()
        .await
        .map_err(|e| ConnectorError::Internal(format!("upsert_cursor commit: {e}")))?;
    Ok(())
}

async fn persist_round_complete(
    t: &CalendarTarget<'_>,
    delta_link: &str,
    window: Option<Window>,
) -> Result<(), ConnectorError> {
    upsert_cursor(t, Some(delta_link), None, window).await
}

/// A round cut off by the page budget: the resume link replaces the
/// completed round's delta link (the resumed round will mint a new one),
/// which also marks the round in progress as "full" for [`plan_round`]'s
/// reconcile decision — see [`fetch_pending`].
async fn persist_progress(
    t: &CalendarTarget<'_>,
    resume_link: &str,
    window: Option<Window>,
) -> Result<(), ConnectorError> {
    upsert_cursor(t, None, Some(resume_link), window).await
}

/// The `410 Gone` / `syncStateNotFound` restart path: every cursor column
/// back to NULL, INCLUDING the anchored window (the next round anchors a
/// fresh one) — which is why this is not `upsert_cursor(None, None, None)`,
/// whose `COALESCE` would keep the old window. The reconcile lap is
/// abandoned too (the restarted full round starts a fresh one on
/// completion); the sweep cursor stays, it is a rotation point, not round
/// state. A missing row needs no clearing.
async fn clear_cursor(t: &CalendarTarget<'_>) -> Result<(), ConnectorError> {
    let mut tx = scoped_tx(t.pool, t.account_id, "clear_cursor").await?;
    sqlx::query(
        "UPDATE microsoft_calendar_sync_state \
         SET delta_link = NULL, delta_resume_link = NULL, \
             delta_window_start = NULL, delta_window_end = NULL, \
             delta_verify_lap_end = NULL, updated_at = now() \
         WHERE account_id = $1 AND owner_user_id = $2",
    )
    .bind(t.account_id)
    .bind(t.owner_user_id)
    .execute(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("clear_cursor: {e}")))?;
    tx.commit()
        .await
        .map_err(|e| ConnectorError::Internal(format!("clear_cursor commit: {e}")))?;
    Ok(())
}

/// Persist the reconcile sweep's position, stamping `delta_reconciled_at`
/// when this pass started a lap. An upsert, not an `UPDATE`: the very first
/// pass of a connection can complete a full round — and start its lap —
/// before any cursor commit has created the row, and a lap recorded on no
/// row would be a lap forgotten. Cursor columns are untouched.
pub(crate) async fn persist_sweep(
    t: &CalendarTarget<'_>,
    sweep: &VerifySweep,
    lap_started: bool,
) -> Result<(), ConnectorError> {
    let mut tx = scoped_tx(t.pool, t.account_id, "persist_sweep").await?;
    sqlx::query(
        r#"
        INSERT INTO microsoft_calendar_sync_state
            (account_id, owner_user_id, graph_id, delta_verify_cursor, delta_verify_lap_end,
             delta_reconciled_at, updated_at)
        VALUES ($1, $2, $3, $4, $5, CASE WHEN $6 THEN now() END, now())
        ON CONFLICT (account_id, owner_user_id)
        DO UPDATE SET graph_id = EXCLUDED.graph_id,
                      delta_verify_cursor = $4,
                      delta_verify_lap_end = $5,
                      delta_reconciled_at = CASE WHEN $6 THEN now()
                                                 ELSE microsoft_calendar_sync_state.delta_reconciled_at END,
                      updated_at = now()
        "#,
    )
    .bind(t.account_id)
    .bind(t.owner_user_id)
    .bind(t.graph_id)
    .bind(sweep.cursor.as_deref())
    .bind(sweep.lap_end.as_deref())
    .bind(lap_started)
    .execute(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("persist_sweep: {e}")))?;
    tx.commit()
        .await
        .map_err(|e| ConnectorError::Internal(format!("persist_sweep commit: {e}")))?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Pure decisions
// ---------------------------------------------------------------------------

/// Where a pass starts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RoundStart {
    /// Continue the round the page budget cut off.
    Resume(String),
    /// Incremental: GET the stored delta link.
    Incremental(String),
    /// A fresh full listing of this window.
    Full(Window),
}

/// A resume link always wins (a round in progress is finished first); a
/// stored delta link is used while its window still has
/// [`REANCHOR_MARGIN_DAYS`] of lookahead AND is at least as wide as the
/// configured window (a token frozen on a narrower one — the pre-`0273`
/// 30-day window, or a later widening of the constants — must re-anchor
/// to list the history it never covered); otherwise a fresh full round.
pub(crate) fn plan_round(state: &SyncState, now: DateTime<Utc>) -> RoundStart {
    if let Some(r) = state.resume_link.as_deref().filter(|s| !s.is_empty()) {
        return RoundStart::Resume(r.to_string());
    }
    let configured = Window::configured(now);
    if let (Some(d), Some(w)) = (state.delta_link.as_deref().filter(|s| !s.is_empty()), state.window) {
        if w.end - now >= Duration::days(REANCHOR_MARGIN_DAYS) && w.span() >= configured.span() {
            return RoundStart::Incremental(d.to_string());
        }
    }
    RoundStart::Full(configured)
}

/// The initial request of a full round.
pub(crate) fn initial_url(w: Window) -> String {
    let fmt = |d: DateTime<Utc>| urlencoding::encode(&d.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)).into_owned();
    format!("{GRAPH}/me/calendarView/delta?startDateTime={}&endDateTime={}", fmt(w.start), fmt(w.end))
}

/// Graph's "your delta/skip token is no longer valid, start over" reply:
/// `410 Gone` (documented), or a 4xx whose error code names the sync state
/// (`SyncStateNotFound`, `ErrorInvalidSyncStateData`, `resyncRequired`).
pub(crate) fn is_sync_state_expired(status: u16, body: &str) -> bool {
    if status == 410 {
        return true;
    }
    if (400..500).contains(&status) {
        let b = body.to_ascii_lowercase();
        return b.contains("syncstate") || b.contains("resyncrequired");
    }
    false
}

/// What this pass does about the reconcile sweep — see the module doc.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReconcileAction {
    /// Start a lap (extend the one in progress) and verify the first batch.
    StartLap,
    /// A lap is in progress: verify the next batch.
    Continue,
    Skip,
}

pub(crate) fn reconcile_action(
    full_round_completed: bool,
    unexplained_removals: usize,
    sweep: &VerifySweep,
) -> ReconcileAction {
    if full_round_completed || unexplained_removals > 0 {
        ReconcileAction::StartLap
    } else if sweep.lap_in_progress() {
        ReconcileAction::Continue
    } else {
        ReconcileAction::Skip
    }
}

/// Position of `id` on the circle that starts just after `after`: ids
/// greater than `after` come first (ascending), then the rest from the
/// smallest up to and including `after`.
fn circular_key<'a>(after: &str, id: &'a str) -> (bool, &'a str) {
    (id <= after, id)
}

/// The next batch of the sweep: up to `cap` stored ids after the cursor in
/// circular order, within the lap in progress (`(cursor, lap_end]`), minus
/// `touched` (what this pass already handled), and the sweep state after
/// verifying them — the lap closes when the batch exhausts it. The returned
/// ids borrow from `stored`.
pub(crate) fn plan_verification<'a>(
    stored: &'a BTreeSet<String>,
    touched: &BTreeSet<String>,
    sweep: &VerifySweep,
    cap: usize,
) -> (Vec<&'a String>, VerifySweep) {
    let Some(lap_end) = sweep.lap_end.as_deref() else {
        return (Vec::new(), sweep.clone());
    };
    let after = sweep.cursor.as_deref().unwrap_or("");
    let end_key = circular_key(after, lap_end);
    let mut remaining: Vec<&'a String> = stored
        .iter()
        .filter(|id| !touched.contains(*id) && circular_key(after, id) <= end_key)
        .collect();
    remaining.sort_by_key(|id| circular_key(after, id));
    let lap_closes = remaining.len() <= cap;
    let batch: Vec<&'a String> = remaining.into_iter().take(cap).collect();
    let next = VerifySweep {
        cursor: batch.last().map(|id| (*id).clone()).or_else(|| sweep.cursor.clone()),
        lap_end: if lap_closes { None } else { sweep.lap_end.clone() },
    };
    (batch, next)
}

/// One decoded delta page.
#[derive(Debug, Default)]
pub(crate) struct DeltaPage {
    pub(crate) changed: Vec<GraphEvent>,
    pub(crate) removed_ids: Vec<String>,
    pub(crate) next_link: Option<String>,
    pub(crate) delta_link: Option<String>,
}

/// Split a `calendarView/delta` response into changed events and removed
/// ids. An entry with an empty id is skipped (malformed upstream data, same
/// posture as `google_calendar::process_events_page`).
pub(crate) fn parse_delta_page(value: serde_json::Value) -> Result<DeltaPage, ConnectorError> {
    let next_link = value.get("@odata.nextLink").and_then(|v| v.as_str()).map(str::to_string);
    let delta_link = value.get("@odata.deltaLink").and_then(|v| v.as_str()).map(str::to_string);
    let mut page = DeltaPage { next_link, delta_link, ..Default::default() };
    let entries = match value.get("value") {
        Some(serde_json::Value::Array(a)) => a.clone(),
        _ => Vec::new(),
    };
    for entry in entries {
        let id = entry.get("id").and_then(|v| v.as_str()).map(str::trim).unwrap_or("");
        if id.is_empty() {
            tracing::warn!("microsoft calendar: delta entry with missing/empty id skipped");
            continue;
        }
        if entry.get("@removed").is_some() {
            page.removed_ids.push(id.to_string());
            continue;
        }
        let ev: GraphEvent = serde_json::from_value(entry)
            .map_err(|e| ConnectorError::Exchange(format!("calendarView/delta event decode: {e}")))?;
        page.changed.push(ev);
    }
    Ok(page)
}

/// Changed events, sorted by what to do with them.
#[derive(Debug, Default)]
pub(crate) struct Classified {
    /// Live single-instance events, shaped for ingest.
    pub(crate) items: Vec<NormalizedItem>,
    /// Single-instance events Graph reports `isCancelled` — retract.
    pub(crate) cancelled_ids: Vec<String>,
    /// Distinct series masters whose occurrences changed — fetch once each.
    pub(crate) master_ids: Vec<String>,
}

pub(crate) fn classify_changed(events: Vec<GraphEvent>) -> Classified {
    let mut out = Classified::default();
    let mut seen_masters = BTreeSet::new();
    for ev in events {
        if let Some(master) = ev.series_master_id.as_deref().map(str::trim).filter(|m| !m.is_empty()) {
            if seen_masters.insert(master.to_string()) {
                out.master_ids.push(master.to_string());
            }
            continue;
        }
        if ev.is_cancelled == Some(true) {
            out.cancelled_ids.push(ev.id);
            continue;
        }
        out.items.push(shape_graph_event(ev));
    }
    out
}

// ---------------------------------------------------------------------------
// Graph calls
// ---------------------------------------------------------------------------

enum PageOutcome {
    Page(DeltaPage),
    SyncStateExpired,
}

async fn fetch_delta_page(
    http: &reqwest::Client,
    access_token: &str,
    url: &str,
) -> Result<PageOutcome, ConnectorError> {
    let resp = graph_send(|| {
        http.get(url)
            .bearer_auth(access_token)
            .header("Prefer", format!("odata.maxpagesize={PAGE_SIZE}"))
    })
    .await?;
    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        if is_sync_state_expired(status.as_u16(), &body) {
            return Ok(PageOutcome::SyncStateExpired);
        }
        return Err(ConnectorError::Exchange(format!(
            "graph calendarView/delta failed: {status}: {}",
            body.chars().take(300).collect::<String>()
        )));
    }
    let value: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| ConnectorError::Exchange(format!("calendarView/delta parse: {e}")))?;
    Ok(PageOutcome::Page(parse_delta_page(value)?))
}

/// What `GET /me/events/{id}` said about one stored or removed id.
pub(crate) enum EventLookup {
    Live(GraphEvent),
    /// `404` — the event no longer exists on this calendar.
    Gone,
    /// Graph refused the request for THIS id (a `4xx` other than `401`,
    /// `404`, `408`, `429`): nothing can be concluded about the event, so the
    /// caller leaves its coral alone and moves on — see the module doc,
    /// "Reconciliation".
    Rejected { status: u16 },
    MalformedId,
}

fn event_url(id: &str) -> Option<String> {
    if matches!(id, "" | "." | "..") {
        return None;
    }
    let mut url = reqwest::Url::parse(&format!("{GRAPH}/me/events/")).expect("static Graph URL");
    url.path_segments_mut().expect("Graph URL has a path").pop_if_empty().push(id);
    url.set_query(Some(&format!("$select={EVENT_SELECT}")));
    Some(url.into())
}

/// How one `GET /me/events/{id}` status is read. `Err` is the status text
/// for the abort-the-pass case ([`fetch_event`] attaches the body).
fn event_lookup_for_response(status: reqwest::StatusCode, body: &str) -> Result<Option<EventLookup>, ()> {
    use reqwest::StatusCode as S;
    if status.is_success() {
        return Ok(None);
    }
    if status == S::BAD_REQUEST
        && serde_json::from_str::<serde_json::Value>(body)
            .ok()
            .is_some_and(|value| value["error"]["code"].as_str() == Some("ErrorInvalidIdMalformed"))
    {
        return Ok(Some(EventLookup::MalformedId));
    }
    match status {
        S::NOT_FOUND => Ok(Some(EventLookup::Gone)),
        S::UNAUTHORIZED | S::REQUEST_TIMEOUT | S::TOO_MANY_REQUESTS => Err(()),
        s if s.is_client_error() => Ok(Some(EventLookup::Rejected { status: s.as_u16() })),
        _ => Err(()),
    }
}

async fn fetch_event(
    http: &reqwest::Client,
    access_token: &str,
    id: &str,
) -> Result<EventLookup, ConnectorError> {
    let Some(url) = event_url(id) else {
        return Ok(EventLookup::Gone);
    };
    let resp = graph_send(|| http.get(&url).bearer_auth(access_token)).await?;
    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        match event_lookup_for_response(status, &body) {
            Ok(Some(lookup)) => return Ok(lookup),
            Ok(None) => unreachable!("non-success response has a lookup verdict"),
            Err(()) => {
                return Err(ConnectorError::Exchange(format!(
                    "graph GET /me/events/{{id}} failed: {status}: {}",
                    body.chars().take(300).collect::<String>()
                )));
            }
        }
    }
    let ev: GraphEvent = resp
        .json()
        .await
        .map_err(|e| ConnectorError::Exchange(format!("event decode: {e}")))?;
    Ok(EventLookup::Live(ev))
}

/// Fold one verified id into the pass: cancelled or gone → retract, live →
/// the edit path, rejected → remembered for one summary log line and
/// otherwise left exactly as stored.
fn settle_lookup(
    id: String,
    lookup: EventLookup,
    items: &mut Vec<NormalizedItem>,
    to_retract: &mut Vec<String>,
    rejected: &mut Vec<(String, u16)>,
) {
    match lookup {
        EventLookup::Live(ev) if ev.is_cancelled == Some(true) => to_retract.push(id),
        EventLookup::Gone | EventLookup::MalformedId => to_retract.push(id),
        EventLookup::Live(ev) => items.push(shape_graph_event(ev)),
        EventLookup::Rejected { status } => rejected.push((id, status)),
    }
}

// ---------------------------------------------------------------------------
// Stored corals: lookup + retraction (ONE graph)
// ---------------------------------------------------------------------------

/// Distinct base source ids (chunk suffix stripped) of every
/// `microsoft_calendar` coral in the target graph.
pub(crate) async fn stored_event_ids(t: &CalendarTarget<'_>) -> Result<BTreeSet<String>, ConnectorError> {
    let mut tx = scoped_tx(t.pool, t.account_id, "stored_event_ids").await?;
    let rows: Vec<(String,)> = sqlx::query_as(
        "SELECT DISTINCT regexp_replace(source_id, ':chunk:[0-9]+$', '') FROM atoms \
         WHERE graph_id = $1 AND source_type = $2",
    )
    .bind(t.graph_id)
    .bind(CALENDAR_SOURCE_TYPE)
    .fetch_all(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("stored_event_ids: {e}")))?;
    tx.rollback().await.ok();
    Ok(rows.into_iter().map(|(s,)| s).collect())
}

/// Retract every coral the target graph holds for `source_ids` (bare event
/// ids; the `:chunk:N` rows are resolved here). Org plane: graph-confined
/// (`delete_atoms_in_graph_only`), another member's copy of the same event
/// id is never read or deleted. Personal plane: `delete_source` per concrete
/// chunk id (account-wide, like Google's personal arm). Returns the number
/// of atoms deleted. Any failure is remembered and returned as `Err` after
/// the whole batch was attempted, so the caller leaves the cursor unchanged
/// (both cascades are idempotent — a retry of an already-gone atom is a
/// no-op).
pub(crate) async fn retract_source_ids(
    t: &CalendarTarget<'_>,
    blobs: Option<&dyn PromotionBlobStore>,
    source_ids: &[String],
) -> Result<usize, ConnectorError> {
    if source_ids.is_empty() {
        return Ok(0);
    }
    let account_id = t.account_id;
    let namespace=format!("microsoft:connection:{account_id}:{}",t.owner_user_id);
    let mut deleted=0;
    let mut any_failed=false;
    for source_id in source_ids {
        let result=match t.plane {
            AccountPlane::Org=>crate::services::deletion::delete_source_identity_in_graph(
                t.pool,blobs,crate::store::CLOUD_DEVICE_ID,t.graph_id,CALENDAR_SOURCE_TYPE,source_id,
                Some(&namespace),crate::services::connector_audit::Actor::System,None,
            ).await,
            AccountPlane::Personal=>crate::services::deletion::delete_chunked_source_in_namespace(
                t.pool,blobs,crate::store::CLOUD_DEVICE_ID,t.graph_id,CALENDAR_SOURCE_TYPE,source_id,
                &namespace,crate::services::connector_audit::Actor::System,None,
            ).await,
        };
        match result {
            Ok(outcome)=>deleted+=outcome.atoms_deleted.len(),
            Err(e)=>{tracing::warn!(error=%e,%source_id,"microsoft calendar: source deletion failed");any_failed=true;}
        }
    }
    if any_failed {return Err(ConnectorError::Internal(format!(
        "microsoft calendar: one or more deletions failed to propagate for account {account_id}"
    )));}
    Ok(deleted)
}

// ---------------------------------------------------------------------------
// The pass
// ---------------------------------------------------------------------------

/// What [`fetch_pending`] found, deferred rather than persisted — see
/// `google_calendar::PendingSync`.
pub(crate) struct PendingSync {
    pub(crate) items: Vec<NormalizedItem>,
    pub(crate) commit: PendingCommit,
    /// Atoms retracted during this pass (deleted/cancelled events) — already
    /// applied; informational.
    pub(crate) retracted: usize,
}

/// The cursor write [`fetch_pending`] computed but did not make. `window`
/// is `Some` only for a round that anchored a new one.
pub(crate) enum PendingCommit {
    Complete { delta_link: String, window: Option<Window> },
    Progress { resume_link: String, window: Option<Window> },
}

/// Persist a [`PendingCommit`] — the ONLY place the cursor advances. Call
/// once every item in [`PendingSync::items`] has been ingested (or there
/// were none).
pub(crate) async fn commit_pending(t: &CalendarTarget<'_>, commit: PendingCommit) -> Result<(), ConnectorError> {
    match commit {
        PendingCommit::Complete { delta_link, window } => persist_round_complete(t, &delta_link, window).await,
        PendingCommit::Progress { resume_link, window } => persist_progress(t, &resume_link, window).await,
    }
}

/// One delta pass WITHOUT persisting the cursor; deletions applied eagerly.
/// See the module doc for every rule.
pub(crate) async fn fetch_pending(
    http: &reqwest::Client,
    t: &CalendarTarget<'_>,
    access_token: &str,
    blobs: Option<&dyn PromotionBlobStore>,
) -> Result<PendingSync, ConnectorError> {
    let account_id = t.account_id;
    let now = Utc::now();
    let mut state = load_sync_state(t).await?;
    let mut restarted_once = false;

    loop {
        let start = plan_round(&state, now);
        // A round with no completed delta link behind it is a FULL listing
        // (first sync, restart, re-anchor, or the resumed tail of one —
        // `persist_progress` clears `delta_link`), so its completion
        // reconciles the stored corals.
        let full_round = matches!(start, RoundStart::Full(_)) || state.delta_link.is_none();
        let (mut url, window) = match start {
            RoundStart::Resume(u) | RoundStart::Incremental(u) => (u, None),
            RoundStart::Full(w) => (initial_url(w), Some(w)),
        };

        let mut changed: Vec<GraphEvent> = Vec::new();
        let mut removed: Vec<String> = Vec::new();
        let mut pages = 0usize;
        let commit = loop {
            match fetch_delta_page(http, access_token, &url).await? {
                PageOutcome::SyncStateExpired => break None,
                PageOutcome::Page(page) => {
                    pages += 1;
                    changed.extend(page.changed);
                    removed.extend(page.removed_ids);
                    if let Some(delta_link) = page.delta_link {
                        break Some(PendingCommit::Complete { delta_link, window });
                    }
                    match page.next_link {
                        Some(next) if pages < PAGES_PER_PASS => url = next,
                        Some(next) => break Some(PendingCommit::Progress { resume_link: next, window }),
                        None => {
                            return Err(ConnectorError::Exchange(
                                "calendarView/delta page carried neither @odata.nextLink nor @odata.deltaLink".into(),
                            ))
                        }
                    }
                }
            }
        };

        let Some(commit) = commit else {
            if restarted_once {
                return Err(ConnectorError::Exchange(
                    "microsoft calendar: sync state expired again immediately after a full restart".into(),
                ));
            }
            tracing::info!(%account_id, "microsoft calendar: delta sync state expired — restarting full sync");
            clear_cursor(t).await?;
            state = SyncState::default();
            restarted_once = true;
            continue;
        };

        let full_round_completed = full_round && matches!(commit, PendingCommit::Complete { .. });
        return apply_round(http, t, access_token, blobs, changed, removed, full_round_completed, &state, commit).await;
    }
}

/// Everything after the pages are in hand: classify, resolve series
/// masters, verify removals, reconcile, retract. Pure Graph/DB work with no
/// cursor write.
#[allow(clippy::too_many_arguments)]
async fn apply_round(
    http: &reqwest::Client,
    t: &CalendarTarget<'_>,
    access_token: &str,
    blobs: Option<&dyn PromotionBlobStore>,
    changed: Vec<GraphEvent>,
    removed: Vec<String>,
    full_round_completed: bool,
    state: &SyncState,
    commit: PendingCommit,
) -> Result<PendingSync, ConnectorError> {
    let account_id = t.account_id;
    let Classified { mut items, cancelled_ids, master_ids } = classify_changed(changed);
    let mut to_retract: Vec<String> = cancelled_ids;
    let mut rejected: Vec<(String, u16)> = Vec::new();
    let mut touched: BTreeSet<String> = items.iter().map(|i| i.source_id.clone()).collect();
    touched.extend(to_retract.iter().cloned());

    // Series masters: one fetch per distinct series this pass touched.
    for master in master_ids {
        touched.insert(master.clone());
        let lookup = fetch_event(http, access_token, &master).await?;
        settle_lookup(master, lookup, &mut items, &mut to_retract, &mut rejected);
    }

    // Removed ids: trust only what Graph confirms gone. A live answer is an
    // edit outside the window (Graph reports that as removed) and goes to
    // the edit path so the coral stays in step.
    let stored = stored_event_ids(t).await?;
    let mut unexplained = 0usize;
    let removed: BTreeSet<String> = removed.into_iter().collect();
    for id in removed {
        if touched.contains(&id) {
            continue;
        }
        touched.insert(id.clone());
        if !stored.contains(&id) {
            unexplained += 1;
            continue;
        }
        let lookup = fetch_event(http, access_token, &id).await?;
        settle_lookup(id, lookup, &mut items, &mut to_retract, &mut rejected);
    }

    // The reconcile sweep: a batch of the lap in progress, or of the lap
    // this pass starts (module doc, "Reconciliation").
    let action = reconcile_action(full_round_completed, unexplained, &state.sweep);
    let mut sweep = state.sweep.clone();
    let lap_started = action == ReconcileAction::StartLap;
    if lap_started {
        sweep.start_lap();
    }
    if action != ReconcileAction::Skip {
        let (batch, next) = plan_verification(&stored, &touched, &sweep, VERIFY_CAP);
        for id in &batch {
            let lookup = fetch_event(http, access_token, id).await?;
            settle_lookup((*id).clone(), lookup, &mut items, &mut to_retract, &mut rejected);
        }
        tracing::info!(
            %account_id, graph_id = %t.graph_id, verified = batch.len(), stored = stored.len(),
            lap_started, lap_closed = !next.lap_in_progress(), unexplained, full_round_completed,
            "microsoft calendar: reconcile sweep verified a batch of stored events against Graph"
        );
        sweep = next;
    }
    if !rejected.is_empty() {
        // One line per pass, not one per id: a token that lost a calendar
        // can make Graph refuse every id in a 200-id batch.
        let sample: Vec<String> = rejected.iter().take(5).map(|(id, s)| format!("{id}={s}")).collect();
        tracing::warn!(
            %account_id, graph_id = %t.graph_id, rejected = rejected.len(), ?sample,
            "microsoft calendar: Graph refused the verify fetch for stored event ids — left as stored, sweep advanced"
        );
    }

    // `?` — a retraction failure blocks the cursor exactly like a failed
    // ingest does (the next pass re-diffs the same range; idempotent), and
    // the sweep position is not advanced past a batch whose retractions
    // did not apply.
    let retracted = retract_source_ids(t, blobs, &to_retract).await?;
    if action != ReconcileAction::Skip {
        persist_sweep(t, &sweep, lap_started).await?;
    }
    Ok(PendingSync { items, commit, retracted })
}

// ---------------------------------------------------------------------------
// Tests — pure decisions (no network, no DB), then pg-gated DB paths.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn ts(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    fn event(json: serde_json::Value) -> GraphEvent {
        serde_json::from_value(json).unwrap()
    }

    /// A `4xx` Graph answers for ONE stored id must not abort the pass (the
    /// review case: a persistent `400`/`403` on one id stalled the whole
    /// calendar sync for good). `404` is the deletion signal, `401` is the
    /// token and `429` the throttle — those, and every `5xx`, still abort
    /// so the next pass retries them with a refreshed token / after the
    /// backoff.
    #[test]
    fn verify_fetch_skips_the_id_on_a_per_id_4xx_and_aborts_on_everything_else() {
        use reqwest::StatusCode as S;
        assert!(matches!(event_lookup_for_response(S::OK, ""), Ok(None)));
        assert!(matches!(event_lookup_for_response(S::NOT_FOUND, ""), Ok(Some(EventLookup::Gone))));
        for s in [S::BAD_REQUEST, S::FORBIDDEN, S::GONE, S::PRECONDITION_FAILED, S::UNPROCESSABLE_ENTITY] {
            assert!(
                matches!(event_lookup_for_response(s, ""), Ok(Some(EventLookup::Rejected { status })) if status == s.as_u16()),
                "{s} is about this id: skip it"
            );
        }
        for s in [S::UNAUTHORIZED, S::REQUEST_TIMEOUT, S::TOO_MANY_REQUESTS, S::INTERNAL_SERVER_ERROR, S::BAD_GATEWAY, S::SERVICE_UNAVAILABLE] {
            assert!(event_lookup_for_response(s, "").is_err(), "{s} says nothing about the id: abort the pass");
        }
    }

    #[test]
    fn only_graphs_malformed_id_verdict_settles_like_a_deletion() {
        use reqwest::StatusCode as S;
        let malformed = r#"{"error":{"code":"ErrorInvalidIdMalformed","message":"Id is malformed."}}"#;
        assert!(matches!(event_lookup_for_response(S::BAD_REQUEST, malformed), Ok(Some(EventLookup::MalformedId))));
        for body in [
            r#"{"error":{"code":"BadRequest","message":"Resource not found for the segment 'x'."}}"#,
            r#"{"error":{"code":"ErrorAccessDenied","message":"Access is denied."}}"#,
            r#"{"error":{"message":"ErrorInvalidIdMalformed"}}"#,
            "ErrorInvalidIdMalformed",
            "",
        ] {
            assert!(
                matches!(event_lookup_for_response(S::BAD_REQUEST, body), Ok(Some(EventLookup::Rejected { status: 400 }))),
                "{body:?} is not the malformed-id verdict"
            );
        }
        for s in [S::FORBIDDEN, S::UNPROCESSABLE_ENTITY] {
            assert!(
                matches!(event_lookup_for_response(s, malformed), Ok(Some(EventLookup::Rejected { .. }))),
                "{s}: the verdict counts only on a 400"
            );
        }
        assert!(event_lookup_for_response(S::SERVICE_UNAVAILABLE, malformed).is_err(), "a 5xx still aborts");

        let mut items = Vec::new();
        let mut to_retract = Vec::new();
        let mut rejected = Vec::new();
        settle_lookup("bad-id".into(), EventLookup::MalformedId, &mut items, &mut to_retract, &mut rejected);
        assert_eq!(to_retract, vec!["bad-id".to_string()]);
        assert!(items.is_empty() && rejected.is_empty());
    }

    #[tokio::test]
    async fn empty_or_dot_event_ids_are_gone_without_any_http_request() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http = reqwest::Client::builder()
            .proxy(reqwest::Proxy::all(format!("http://{}", listener.local_addr().unwrap())).unwrap())
            .build().unwrap();
        let mut items = Vec::new();
        let mut to_retract = Vec::new();
        let mut rejected = Vec::new();
        for id in ["", ".", ".."] {
            let result = tokio::time::timeout(std::time::Duration::from_secs(1), async {
                tokio::select! {
                    biased;
                    result = fetch_event(&http, "unused", id) => result,
                    request = listener.accept() => panic!("{id:?} attempted an HTTP request: {request:?}"),
                }
            }).await.expect("a non-event id settles without waiting for HTTP").unwrap();
            assert!(matches!(result, EventLookup::Gone), "{id:?} cannot name an event");
            settle_lookup(id.into(), result, &mut items, &mut to_retract, &mut rejected);
        }
        assert_eq!(to_retract, vec!["", ".", ".."]);
        assert!(items.is_empty() && rejected.is_empty());
    }

    #[test]
    fn the_verify_url_names_the_stored_id_as_one_path_segment() {
        let id = "AAMkAGI2TG93AAA-_x=";
        assert_eq!(event_url(id).unwrap(), format!("{GRAPH}/me/events/{id}?$select={EVENT_SELECT}"));
        for id in ["AAMk/abc", "AAMk?x=1", "AAMk#frag", "AAMk%2Fq", "AA Mk", "AAMk\\..\\x", "...", "A..Mk"] {
            let url = reqwest::Url::parse(&event_url(id).expect("an opaque event id")).expect("a valid url");
            let segs: Vec<String> = url
                .path_segments()
                .unwrap()
                .map(|s| urlencoding::decode(s).unwrap().into_owned())
                .collect();
            assert_eq!(segs, vec!["v1.0".to_string(), "me".into(), "events".into(), id.to_string()], "{id:?}");
            assert_eq!(url.query(), Some(format!("$select={EVENT_SELECT}").as_str()), "{id:?}");
            assert_eq!(url.fragment(), None, "{id:?}");
        }
    }

    /// A rejected id is left exactly as stored — not retracted, not
    /// re-shaped — while the ids around it are settled as before, and the
    /// sweep position computed for the batch is unaffected (it never
    /// depended on the fetch outcomes).
    #[test]
    fn a_rejected_lookup_leaves_the_coral_alone_and_the_batch_continues() {
        let live = event(serde_json::json!({
            "id": "live", "subject": "Kept", "start": {"dateTime": "2026-10-08T09:00:00.0000000", "timeZone": "UTC"},
            "end": {"dateTime": "2026-10-08T10:00:00.0000000", "timeZone": "UTC"}
        }));
        let mut items = Vec::new();
        let mut to_retract = Vec::new();
        let mut rejected = Vec::new();
        settle_lookup("bad".into(), EventLookup::Rejected { status: 400 }, &mut items, &mut to_retract, &mut rejected);
        settle_lookup("gone".into(), EventLookup::Gone, &mut items, &mut to_retract, &mut rejected);
        settle_lookup("live".into(), EventLookup::Live(live), &mut items, &mut to_retract, &mut rejected);
        assert_eq!(to_retract, vec!["gone".to_string()], "the rejected id is not retracted");
        assert_eq!(items.iter().map(|i| i.source_id.as_str()).collect::<Vec<_>>(), vec!["live"], "nor re-ingested");
        assert_eq!(rejected, vec![("bad".to_string(), 400)]);

        let stored: BTreeSet<String> = ["bad", "gone", "live", "z"].iter().map(|s| s.to_string()).collect();
        let mut sweep = VerifySweep::default();
        sweep.start_lap();
        let (batch, next) = plan_verification(&stored, &BTreeSet::new(), &sweep, 2);
        assert_eq!(
            batch.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
            vec!["bad", "gone"],
            "the rejected id is part of the batch like any other"
        );
        assert_eq!(next.cursor.as_deref(), Some("gone"), "the sweep advanced past it");
    }

    #[test]
    fn plan_round_prefers_resume_then_fresh_delta_then_full() {
        let now = ts("2026-10-07T12:00:00Z");
        // Nothing stored → a full round over [now-365d, now+90d].
        let anchored = Window { start: ts("2025-10-07T12:00:00Z"), end: ts("2027-01-05T12:00:00Z") };
        assert_eq!(plan_round(&SyncState::default(), now), RoundStart::Full(anchored));
        // A resume link wins over everything.
        let s = SyncState {
            delta_link: Some("https://g/delta?$deltatoken=a".into()),
            resume_link: Some("https://g/delta?$skiptoken=b".into()),
            window: Some(anchored),
            reconciled_at: None,
            sweep: VerifySweep::default(),
        };
        assert_eq!(plan_round(&s, now), RoundStart::Resume("https://g/delta?$skiptoken=b".into()));
        // A delta link with ≥ 60 days of lookahead → incremental.
        let s = SyncState { resume_link: None, ..s };
        assert_eq!(plan_round(&s, now), RoundStart::Incremental("https://g/delta?$deltatoken=a".into()));
        // Exactly at the margin still counts; one second past it re-anchors
        // (whole windows of the configured width, anchored earlier — not a
        // window with its end moved, which would be narrower).
        let at_margin = Window::configured(now - Duration::days(WINDOW_FUTURE_DAYS - REANCHOR_MARGIN_DAYS));
        assert_eq!(at_margin.end, now + Duration::days(REANCHOR_MARGIN_DAYS));
        assert!(matches!(plan_round(&SyncState { window: Some(at_margin), ..s.clone() }, now), RoundStart::Incremental(_)));
        let past_margin = Window::configured(now - Duration::days(WINDOW_FUTURE_DAYS - REANCHOR_MARGIN_DAYS) - Duration::seconds(1));
        assert!(
            matches!(plan_round(&SyncState { window: Some(past_margin), ..s.clone() }, now), RoundStart::Full(_)),
            "fewer than 60 days of lookahead left → re-anchor"
        );
        // A delta link with no window recorded is not trusted.
        assert!(matches!(plan_round(&SyncState { window: None, ..s.clone() }, now), RoundStart::Full(_)));
        // Empty strings read as absent.
        let s5 = SyncState { delta_link: Some(String::new()), resume_link: Some(String::new()), window: Some(anchored), reconciled_at: None, sweep: VerifySweep::default() };
        assert!(matches!(plan_round(&s5, now), RoundStart::Full(_)));
    }

    /// A cursor frozen on a window NARROWER than the configured one — the
    /// pre-`0273` 30-day window of every connection that existed when this
    /// shipped, or any later widening of the constants — re-anchors on its
    /// next pass even with plenty of lookahead left, so the history it
    /// never covered is listed now rather than at the next scheduled
    /// re-anchor. A window at least as wide (anchored earlier, so its end is
    /// nearer, but still past the margin) stays incremental.
    #[test]
    fn plan_round_reanchors_a_cursor_frozen_on_a_narrower_window() {
        let now = ts("2026-10-07T12:00:00Z");
        let delta = Some("https://g/delta?$deltatoken=a".to_string());
        // Anchored yesterday on 30 back / 90 ahead: 89 days of lookahead.
        let narrow = Window { start: ts("2026-09-06T12:00:00Z"), end: ts("2027-01-04T12:00:00Z") };
        assert!(narrow.end - now >= Duration::days(REANCHOR_MARGIN_DAYS), "fixture: lookahead alone would keep it incremental");
        let s = SyncState { delta_link: delta.clone(), resume_link: None, window: Some(narrow), reconciled_at: None, sweep: VerifySweep::default() };
        assert_eq!(plan_round(&s, now), RoundStart::Full(Window::configured(now)));

        // Anchored 20 days ago on the configured width: incremental.
        let wide = Window::configured(now - Duration::days(20));
        let s = SyncState { window: Some(wide), ..s };
        assert_eq!(plan_round(&s, now), RoundStart::Incremental("https://g/delta?$deltatoken=a".into()));

        // Wider than configured (a future narrowing of the constants) is
        // not a reason to relist.
        let wider = Window { start: wide.start - Duration::days(100), ..wide };
        let s = SyncState { window: Some(wider), ..s };
        assert!(matches!(plan_round(&s, now), RoundStart::Incremental(_)));
    }

    #[test]
    fn initial_url_targets_calendar_view_delta_with_the_window() {
        let url = initial_url(Window { start: ts("2025-10-07T12:00:00Z"), end: ts("2027-01-05T12:00:00Z") });
        assert_eq!(
            url,
            "https://graph.microsoft.com/v1.0/me/calendarView/delta?startDateTime=2025-10-07T12%3A00%3A00Z&endDateTime=2027-01-05T12%3A00%3A00Z"
        );
    }

    /// The window stays inside the two-year range the Exchange calendar
    /// view behind `calendarView` enforces (EWS `ErrorCalendarViewRangeTooBig`).
    #[test]
    fn configured_window_is_under_the_two_year_calendar_view_ceiling() {
        let now = ts("2026-10-07T12:00:00Z");
        let w = Window::configured(now);
        assert_eq!(w.span(), Duration::days(WINDOW_PAST_DAYS + WINDOW_FUTURE_DAYS));
        assert!(w.span() < Duration::days(730), "{:?}", w.span());
        assert!(WINDOW_FUTURE_DAYS > REANCHOR_MARGIN_DAYS, "a window must have lookahead to spend before it re-anchors");
    }

    #[test]
    fn sync_state_expiry_is_410_or_a_4xx_naming_the_sync_state() {
        assert!(is_sync_state_expired(410, ""));
        assert!(is_sync_state_expired(400, r#"{"error":{"code":"SyncStateNotFound","message":"..."}}"#));
        assert!(is_sync_state_expired(400, r#"{"error":{"code":"ErrorInvalidSyncStateData"}}"#));
        assert!(is_sync_state_expired(404, r#"{"error":{"code":"ResyncRequired"}}"#));
        assert!(!is_sync_state_expired(400, r#"{"error":{"code":"ErrorInvalidRequest"}}"#));
        assert!(!is_sync_state_expired(401, r#"{"error":{"code":"InvalidAuthenticationToken"}}"#));
        assert!(!is_sync_state_expired(503, "SyncStateNotFound"), "a 5xx is an outage, never a restart");
        assert!(!is_sync_state_expired(200, "SyncStateNotFound"));
    }

    #[test]
    fn reconcile_starts_a_lap_on_a_trigger_and_otherwise_only_continues_one_in_progress() {
        let idle = VerifySweep::default();
        let open = VerifySweep { cursor: Some("m".into()), lap_end: Some("m".into()) };
        assert_eq!(reconcile_action(true, 0, &idle), ReconcileAction::StartLap, "full round completed");
        assert_eq!(reconcile_action(true, 0, &open), ReconcileAction::StartLap, "full round extends an open lap");
        assert_eq!(reconcile_action(false, 1, &idle), ReconcileAction::StartLap, "unexplained removal");
        assert_eq!(reconcile_action(false, 2, &open), ReconcileAction::StartLap, "unexplained removal mid-lap extends it");
        assert_eq!(reconcile_action(false, 0, &open), ReconcileAction::Continue, "nothing new, lap open: keep walking");
        assert_eq!(reconcile_action(false, 0, &idle), ReconcileAction::Skip, "nothing new, no lap: nothing to do");

        let mut s = VerifySweep { cursor: Some("k".into()), lap_end: None };
        s.start_lap();
        assert_eq!(s.lap_end.as_deref(), Some("k"), "a lap begins where the cursor is");
        let mut fresh = VerifySweep::default();
        fresh.start_lap();
        assert_eq!(fresh.lap_end.as_deref(), Some(""), "never swept: the lap begins before the smallest id");
    }

    fn ids(prefix: &str, n: usize) -> Vec<String> {
        (0..n).map(|i| format!("{prefix}-{i:04}")).collect()
    }

    /// The review scenario (#1160): a FULL round that takes more than one
    /// pass. Pass 1 (`PAGES_PER_PASS` hit) reports 1000 occurrences of
    /// live events; pass 2 reports the rest and gets the deltaLink. The
    /// user deleted a weekly series: its occurrences came back `@removed`
    /// (unexplained — the stored id is the MASTER's), and the master id
    /// sorts after more than `VERIFY_CAP` live ids. Reconcile must verify
    /// that master — in some pass — and so retract it. The previous shape
    /// ("stored minus what THIS pass saw, first `VERIFY_CAP` sorted")
    /// counted only pass 2's ids as seen, so the first 200 of pass 1's
    /// live ids were verified on every reconcile and the master never.
    #[test]
    fn verification_sweep_reaches_a_deleted_master_sorting_after_the_cap_across_passes() {
        let pass1: BTreeSet<String> = ids("live-a", 1000).into_iter().collect();
        let pass2: BTreeSet<String> = ids("live-b", 300).into_iter().collect();
        let master = "zz-deleted-series-master".to_string();
        let mut stored: BTreeSet<String> = pass1.union(&pass2).cloned().collect();
        stored.insert(master.clone());
        assert!(stored.iter().filter(|id| **id < master).count() > VERIFY_CAP, "fixture: the master sorts after the cap");

        // Pass 1: Progress, no trigger yet, nothing swept (no lap open).
        let mut sweep = VerifySweep::default();
        assert_eq!(reconcile_action(false, 0, &sweep), ReconcileAction::Skip);

        // Pass 2: the round completes (and carries the unexplained removals).
        assert_eq!(reconcile_action(true, 3, &sweep), ReconcileAction::StartLap);
        sweep.start_lap();
        let mut verified: BTreeSet<String> = BTreeSet::new();
        let mut passes = 0;
        let mut touched = pass2.clone();
        loop {
            passes += 1;
            let (batch, next) = plan_verification(&stored, &touched, &sweep, VERIFY_CAP);
            assert!(batch.len() <= VERIFY_CAP);
            verified.extend(batch.iter().map(|s| (*s).clone()));
            sweep = next;
            if !sweep.lap_in_progress() {
                break;
            }
            assert!(passes < 20, "the lap must close");
            // Later passes: incremental, nothing changed, the lap continues.
            assert_eq!(reconcile_action(false, 0, &sweep), ReconcileAction::Continue);
            touched = BTreeSet::new();
        }
        assert!(verified.contains(&master), "the deleted series master was verified (and would be retracted)");
        // Pass 2's own ids were skipped by the batch it ran (just handled)
        // and reached by a later one, so the whole calendar is covered,
        // paced at VERIFY_CAP per pass and nothing verified twice.
        assert_eq!(verified, stored);
        assert_eq!(passes, stored.len().div_ceil(VERIFY_CAP));
    }

    /// The lap is circular and rotates: it begins at the cursor, wraps past
    /// the largest id to the smallest, closes when it is back at its start,
    /// and the next lap begins where this one ended. A trigger during a
    /// lap EXTENDS it to a full lap from the current cursor — it never
    /// restarts from a fixed position, which is what keeps every id within
    /// one lap of the cursor whatever happens.
    #[test]
    fn verification_lap_wraps_rotates_and_extends_on_a_mid_lap_trigger() {
        let stored: BTreeSet<String> = ids("e", 10).into_iter().collect();
        let none = BTreeSet::new();
        let mut sweep = VerifySweep { cursor: Some("e-0006".into()), lap_end: None };
        sweep.start_lap();

        let (b1, s1) = plan_verification(&stored, &none, &sweep, 4);
        assert_eq!(b1.iter().map(|s| s.as_str()).collect::<Vec<_>>(), ["e-0007", "e-0008", "e-0009", "e-0000"], "after the cursor, then wrapped");
        assert!(s1.lap_in_progress());
        assert_eq!(s1.cursor.as_deref(), Some("e-0000"));

        let (b2, s2) = plan_verification(&stored, &none, &s1, 4);
        assert_eq!(b2.iter().map(|s| s.as_str()).collect::<Vec<_>>(), ["e-0001", "e-0002", "e-0003", "e-0004"]);
        let (b3, s3) = plan_verification(&stored, &none, &s2, 4);
        assert_eq!(b3.iter().map(|s| s.as_str()).collect::<Vec<_>>(), ["e-0005", "e-0006"], "up to and including the lap's start");
        assert!(!s3.lap_in_progress(), "lap closed");
        assert_eq!(s3.cursor.as_deref(), Some("e-0006"));
        assert!(plan_verification(&stored, &none, &s3, 4).0.is_empty(), "no lap: nothing to verify");

        // Mid-lap trigger: extend. Verified 7,8,9,0 so far; the trigger at
        // cursor e-0000 means one full lap from there — 1..=9 then 0.
        let mut extended = s1.clone();
        extended.start_lap();
        assert_eq!(extended.lap_end.as_deref(), Some("e-0000"));
        let mut seen = Vec::new();
        let mut s = extended;
        while s.lap_in_progress() {
            let (b, n) = plan_verification(&stored, &none, &s, 4);
            seen.extend(b.iter().map(|x| x.to_string()));
            s = n;
        }
        assert_eq!(seen, ids("e", 10).into_iter().cycle().skip(1).take(10).collect::<Vec<_>>());

        // What the pass itself handled is skipped and never blocks closing;
        // an id the cursor points at that no longer exists is fine too.
        let touched: BTreeSet<String> = ["e-0003", "e-0004"].iter().map(|s| s.to_string()).collect();
        let mut s = VerifySweep { cursor: Some("e-0002x".into()), lap_end: None };
        s.start_lap();
        let (b, n) = plan_verification(&stored, &touched, &s, 100);
        assert_eq!(b.iter().map(|x| x.as_str()).collect::<Vec<_>>(), ["e-0005", "e-0006", "e-0007", "e-0008", "e-0009", "e-0000", "e-0001", "e-0002"]);
        assert!(!n.lap_in_progress());

        // An empty calendar closes the lap immediately and leaves the
        // cursor alone.
        let mut s = VerifySweep { cursor: Some("k".into()), lap_end: None };
        s.start_lap();
        let empty = BTreeSet::new();
        let (b, n) = plan_verification(&empty, &none, &s, 4);
        assert!(b.is_empty());
        assert_eq!(n, VerifySweep { cursor: Some("k".into()), lap_end: None });
    }

    #[test]
    fn parse_delta_page_splits_removed_from_changed_and_keeps_links() {
        let page = parse_delta_page(serde_json::json!({
            "@odata.nextLink": "https://g/next",
            "value": [
                { "id": "AAMk-live", "subject": "Live", "type": "singleInstance",
                  "start": { "dateTime": "2026-10-07T09:00:00.0000000" } },
                { "id": "AAMk-gone", "@removed": { "reason": "deleted" } },
                { "id": "   ", "subject": "malformed" },
                { "id": "AAMk-occ", "subject": "Standup", "type": "occurrence", "seriesMasterId": "AAMk-master",
                  "start": { "dateTime": "2026-10-07T09:00:00.0000000" } }
            ]
        }))
        .unwrap();
        assert_eq!(page.next_link.as_deref(), Some("https://g/next"));
        assert!(page.delta_link.is_none());
        assert_eq!(page.removed_ids, vec!["AAMk-gone".to_string()]);
        let ids: Vec<&str> = page.changed.iter().map(|e| e.id.as_str()).collect();
        assert_eq!(ids, vec!["AAMk-live", "AAMk-occ"]);

        let last = parse_delta_page(serde_json::json!({
            "@odata.deltaLink": "https://g/delta?$deltatoken=z",
            "value": []
        }))
        .unwrap();
        assert_eq!(last.delta_link.as_deref(), Some("https://g/delta?$deltatoken=z"));
        assert!(last.changed.is_empty() && last.removed_ids.is_empty());
    }

    #[test]
    fn classify_changed_routes_singles_cancellations_and_series_masters() {
        let out = classify_changed(vec![
            event(serde_json::json!({ "id": "s1", "subject": "One", "type": "singleInstance",
                "start": { "dateTime": "2026-10-07T09:00:00.0000000" } })),
            event(serde_json::json!({ "id": "s2", "subject": "Cancelled", "type": "singleInstance", "isCancelled": true,
                "start": { "dateTime": "2026-10-07T09:00:00.0000000" } })),
            event(serde_json::json!({ "id": "o1", "type": "occurrence", "seriesMasterId": "m1",
                "start": { "dateTime": "2026-10-07T09:00:00.0000000" } })),
            event(serde_json::json!({ "id": "o2", "type": "exception", "seriesMasterId": "m1",
                "start": { "dateTime": "2026-10-08T09:00:00.0000000" } })),
            event(serde_json::json!({ "id": "o3", "type": "occurrence", "seriesMasterId": "m2",
                "start": { "dateTime": "2026-10-09T09:00:00.0000000" } })),
        ]);
        assert_eq!(out.items.iter().map(|i| i.source_id.as_str()).collect::<Vec<_>>(), vec!["s1"]);
        assert_eq!(out.cancelled_ids, vec!["s2".to_string()]);
        assert_eq!(out.master_ids, vec!["m1".to_string(), "m2".to_string()], "one fetch per DISTINCT series, in first-seen order");
    }

    /// The reason an unchanged event never churns: the delta reply carries
    /// the FULL resource (no `$select` there), the per-event fetch only the
    /// [`EVENT_SELECT`] fields — `body` among them, so the description is
    /// the same whole body from both — and the two must shape to
    /// byte-identical content/participants/timestamp.
    #[test]
    fn delta_full_resource_shapes_identically_to_the_selected_fetch() {
        assert!(EVENT_SELECT.split(',').any(|f| f == "body"), "the per-event fetch must carry the body the delta carries: {EVENT_SELECT}");
        let selected = event(serde_json::json!({
            "id": "AAMkAG-1",
            "subject": "Sprint review",
            "bodyPreview": "Agenda: demo, retro",
            "body": { "contentType": "html", "content": "<html><body>Agenda: demo, retro</body></html>" },
            "start": { "dateTime": "2026-10-07T09:00:00.0000000" },
            "end": { "dateTime": "2026-10-07T09:30:00.0000000" },
            "location": { "displayName": "Room 4" },
            "organizer": { "emailAddress": { "name": "Alice Ng", "address": "alice@ex.com" } },
            "attendees": [ { "emailAddress": { "name": "Bob", "address": "bob@ex.com" } } ],
            "isCancelled": false, "type": "singleInstance"
        }));
        let full = event(serde_json::json!({
            "@odata.etag": "W/\"abc\"",
            "id": "AAMkAG-1",
            "createdDateTime": "2026-10-01T10:00:00Z",
            "lastModifiedDateTime": "2026-10-06T10:00:00Z",
            "changeKey": "xyz",
            "categories": [],
            "subject": "Sprint review",
            "bodyPreview": "Agenda: demo, retro",
            "body": { "contentType": "html", "content": "<html><body>Agenda: demo, retro</body></html>" },
            "importance": "normal", "sensitivity": "normal", "showAs": "busy",
            "isAllDay": false, "isCancelled": false, "isOrganizer": true, "responseRequested": true,
            "seriesMasterId": null, "type": "singleInstance",
            "webLink": "https://outlook.office365.com/owa/?itemid=AAMkAG-1",
            "onlineMeetingUrl": null, "isOnlineMeeting": true, "onlineMeetingProvider": "teamsForBusiness",
            "responseStatus": { "response": "organizer", "time": "0001-01-01T00:00:00Z" },
            "start": { "dateTime": "2026-10-07T09:00:00.0000000", "timeZone": "UTC" },
            "end": { "dateTime": "2026-10-07T09:30:00.0000000", "timeZone": "UTC" },
            "location": { "displayName": "Room 4", "locationType": "default", "uniqueIdType": "unknown", "address": {}, "coordinates": {} },
            "locations": [ { "displayName": "Room 4" } ],
            "recurrence": null,
            "attendees": [ { "type": "required", "status": { "response": "accepted" },
                             "emailAddress": { "name": "Bob", "address": "bob@ex.com" } } ],
            "organizer": { "emailAddress": { "name": "Alice Ng", "address": "alice@ex.com" } },
            "onlineMeeting": { "joinUrl": "https://teams.microsoft.com/l/meetup-join/x" }
        }));
        let a = shape_graph_event(selected);
        let b = shape_graph_event(full);
        assert_eq!(a.content, b.content);
        assert_eq!(a.participants, b.participants);
        assert_eq!(a.timestamp, b.timestamp);
        assert_eq!(
            crate::services::source_reingest::content_hash_hex(&a.content),
            crate::services::source_reingest::content_hash_hex(&b.content)
        );

        // And a Teams meeting — the join-block trim runs on the body text
        // both endpoints deliver, so the two still agree byte for byte.
        let full: serde_json::Value =
            serde_json::from_str(include_str!("../../tests/fixtures/graph/calendar_event_teams_invite.json")).unwrap();
        let mut selected = serde_json::Map::new();
        for field in EVENT_SELECT.split(',') {
            selected.insert(field.to_string(), full[field].clone());
        }
        let a = shape_graph_event(event(serde_json::Value::Object(selected)));
        let b = shape_graph_event(event(full));
        assert!(a.content.contains("Agenda for the pipeline review:") && !a.content.contains("Passcode"), "{}", a.content);
        assert_eq!(a.content, b.content);
        assert_eq!(a.participants, b.participants);
        assert_eq!(a.timestamp, b.timestamp);
    }

    // ── pg-gated ───────────────────────────────────────────────────────────
    //
    //   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
    //     cargo test -p coralswarm-backend --lib connectors::microsoft_calendar::tests
    //
    // Absent → "skipping", vacuous pass. No test makes a Graph call: the
    // HTTP legs have no mock infrastructure in this crate, so the DB halves
    // (`retract_source_ids`, `stored_event_ids`, cursor persistence, the
    // no-churn comparison) are exercised directly — as the tenant app role
    // where RLS is what could silently break them.

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
    async fn recreate(base: &str, db: &str) {
        let mut conn = PgConnection::connect(base).await.expect("connect admin");
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.unwrap();
        conn.execute(format!("CREATE DATABASE {db}").as_str()).await.unwrap();
        conn.close().await.ok();
    }
    async fn drop_db(base: &str, db: &str) {
        let mut admin_conn = PgConnection::connect(base).await.unwrap();
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }

    /// A tenant-tree database plus a pool connected AS the real NOBYPASSRLS
    /// `coralswarm_tenant_app` role — same fixture as
    /// `services::source_reingest::tests::tenant_tree_as_app_role`.
    async fn tenant_tree_as_app_role(base: &str, db: &str, org_id: &str) -> (PgPool, PgPool) {
        recreate(base, db).await;
        let owner_pool: PgPool = PgPoolOptions::new().connect(&with_db(base, db)).await.unwrap();
        sqlx::migrate!("./migrations_tenant").run(&owner_pool).await.unwrap();
        sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, $1)")
            .bind(org_id)
            .execute(&owner_pool)
            .await
            .unwrap();
        let pw = "ms_calendar_tenant_role_test_only_pw";
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
        let opts = <sqlx::postgres::PgConnectOptions as std::str::FromStr>::from_str(&with_db(base, db))
            .unwrap()
            .username("coralswarm_tenant_app")
            .password(pw);
        let app = PgPoolOptions::new().max_connections(2).connect_with(opts).await.expect("connect AS coralswarm_tenant_app");
        (owner_pool, app)
    }

    async fn seed_member_graph(pool: &PgPool, account_id: Uuid, owner_user_id: Uuid) -> Uuid {
        sqlx::query_scalar(
            "INSERT INTO graphs(account_id, name, kind, is_primary, owner_user_id) \
             VALUES ($1, 'member ocean', 'member', true, $2) RETURNING id",
        )
        .bind(account_id)
        .bind(owner_user_id)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    /// A coral exactly as `routes::ingest` stores one: through the real
    /// `Store::insert_atoms_scoped`, chunked source id, content hash set.
    #[allow(clippy::too_many_arguments)]
    async fn seed_coral(
        pool: &PgPool,
        account_id: Uuid,
        graph_id: Uuid,
        user_id: Uuid,
        source_type: &str,
        source_id: &str,
        content: &str,
        timestamp: DateTime<Utc>,
        participants: &[String],
    ) -> Uuid {
        use crate::store::Store;
        let store = crate::store::PostgresStore::new(pool.clone());
        let atom_id = Uuid::new_v4();
        let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(pool, account_id).await.unwrap();
        let hash = crate::services::source_reingest::content_hash_hex(content);
        let inserted = store
            .insert_atoms_scoped(
                &mut tx,
                &[atom_id],
                graph_id,
                source_type,
                &[format!("{source_id}:chunk:0")],
                &[format!("blob-{atom_id}")],
                participants,
                timestamp,
                user_id,
                "private",
                "english",
                &[Some(content.to_string())],
                &[crate::services::atom_title::fallback_title_for_source(source_type, content)],
                &[Some("fallback".to_string())],
                &[None],
                Some(&hash),
            )
            .await
            .unwrap();
        tx.commit().await.unwrap();
        assert_eq!(inserted, vec![atom_id]);
        atom_id
    }

    async fn atom_count(pool: &PgPool, atom_id: Uuid) -> i64 {
        sqlx::query_scalar("SELECT count(*) FROM atoms WHERE atom_id = $1")
            .bind(atom_id)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    /// Org plane, as the real tenant app role: a `@removed` (or
    /// `isCancelled`) event retracts ONLY the syncing member's coral —
    /// another member's copy of the SAME event id survives; a transcript
    /// atom sharing the id is untouched (source_type gate); a second
    /// retraction of the same id is an idempotent no-op; and
    /// `stored_event_ids` reports bare ids with the chunk suffix stripped.
    #[tokio::test]
    async fn removed_event_retracts_only_that_members_coral_as_tenant_app_role_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping removed_event_retracts_only_that_members_coral_as_tenant_app_role_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "ms_cal_removed_org_role_ten";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let (owner_pool, app) = tenant_tree_as_app_role(&base, db, "org_ms_cal_removed").await;

        let account_id = Uuid::new_v4();
        let (alice, bob) = (Uuid::new_v4(), Uuid::new_v4());
        let start = ts("2026-10-07T09:00:00Z");
        let graph_a = seed_member_graph(&owner_pool, account_id, alice).await;
        let graph_b = seed_member_graph(&owner_pool, account_id, bob).await;
        let a_evt = seed_coral(&owner_pool, account_id, graph_a, alice, CALENDAR_SOURCE_TYPE, "AAMk-evt", "Planning", start, &[]).await;
        let b_evt = seed_coral(&owner_pool, account_id, graph_b, bob, CALENDAR_SOURCE_TYPE, "AAMk-evt", "Planning", start, &[]).await;
        let a_other = seed_coral(&owner_pool, account_id, graph_a, alice, CALENDAR_SOURCE_TYPE, "AAMk-other", "Standup", start, &[]).await;
        let a_transcript = seed_coral(&owner_pool, account_id, graph_a, alice, "teams_transcript", "AAMk-evt", "Alice: hi", start, &[]).await;

        let t_alice = CalendarTarget::org_member_resolved(&app, account_id, alice, graph_a);
        let stored = stored_event_ids(&t_alice).await.unwrap();
        assert_eq!(stored, ["AAMk-evt", "AAMk-other"].into_iter().map(String::from).collect::<BTreeSet<_>>(), "bare ids, chunk suffix stripped, transcript excluded");

        let deleted = retract_source_ids(&t_alice, None, &["AAMk-evt".to_string()]).await.unwrap();
        assert_eq!(deleted, 1);
        assert_eq!(atom_count(&owner_pool, a_evt).await, 0, "alice's coral for the deleted event is gone");
        assert_eq!(atom_count(&owner_pool, b_evt).await, 1, "bob's copy of the same event id survives");
        assert_eq!(atom_count(&owner_pool, a_other).await, 1, "alice's other event untouched");
        assert_eq!(atom_count(&owner_pool, a_transcript).await, 1, "a transcript sharing the id is not a calendar coral");

        // Idempotent: the next pass re-reporting the same removal is a no-op.
        let again = retract_source_ids(&t_alice, None, &["AAMk-evt".to_string()]).await.unwrap();
        assert_eq!(again, 0);
        assert_eq!(atom_count(&owner_pool, b_evt).await, 1);

        // Bob's own target never sees alice's graph.
        let t_bob = CalendarTarget::org_member_resolved(&app, account_id, bob, graph_b);
        assert_eq!(stored_event_ids(&t_bob).await.unwrap(), ["AAMk-evt"].into_iter().map(String::from).collect::<BTreeSet<_>>());

        // Compliance row landed for alice's retraction.
        let audits: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM connector_audit_log WHERE account_id = $1 AND action = 'deletion_propagated' AND provider = 'microsoft'",
        )
        .bind(account_id)
        .fetch_one(&owner_pool)
        .await
        .unwrap();
        assert_eq!(audits, 1);

        app.close().await;
        owner_pool.close().await;
        drop_db(&base, db).await;
    }

    /// The cursor round-trips on the tenant tree as the app role: a cut-off
    /// round stores the resume link and CLEARS the delta link; completing
    /// stores the delta link and clears the resume link, preserving the
    /// window; a restart clears everything but the sweep's rotation point;
    /// the reconcile sweep's lap round-trips and stamps on start.
    #[tokio::test]
    async fn cursor_round_trips_and_progress_clears_the_delta_link_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping cursor_round_trips_and_progress_clears_the_delta_link_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "ms_cal_cursor_org_role_ten";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let (owner_pool, app) = tenant_tree_as_app_role(&base, db, "org_ms_cal_cursor").await;
        let account_id = Uuid::new_v4();
        let alice = Uuid::new_v4();
        let graph_a = seed_member_graph(&owner_pool, account_id, alice).await;
        let t = CalendarTarget::org_member_resolved(&app, account_id, alice, graph_a);

        assert_eq!(load_sync_state(&t).await.unwrap(), SyncState::default(), "no row yet reads as a fresh state");

        let now = ts("2026-10-07T12:00:00Z");
        let window = Window::configured(now);
        commit_pending(&t, PendingCommit::Progress { resume_link: "https://g/next1".into(), window: Some(window) }).await.unwrap();
        let s = load_sync_state(&t).await.unwrap();
        assert_eq!(s.resume_link.as_deref(), Some("https://g/next1"));
        assert_eq!(s.window, Some(window));
        assert!(s.delta_link.is_none());

        commit_pending(&t, PendingCommit::Complete { delta_link: "https://g/delta1".into(), window: None }).await.unwrap();
        let s = load_sync_state(&t).await.unwrap();
        assert_eq!(s.delta_link.as_deref(), Some("https://g/delta1"));
        assert!(s.resume_link.is_none());
        assert_eq!(s.window, Some(window), "an incremental completion preserves the anchored window");
        assert!(matches!(plan_round(&s, now), RoundStart::Incremental(_)));

        // A later incremental round cut off mid-way: resume stored, delta cleared.
        commit_pending(&t, PendingCommit::Progress { resume_link: "https://g/next2".into(), window: None }).await.unwrap();
        let s = load_sync_state(&t).await.unwrap();
        assert_eq!(s.resume_link.as_deref(), Some("https://g/next2"));
        assert!(s.delta_link.is_none());
        assert_eq!(s.window, Some(window));

        // The row every connection had when `0273`/`0114` shipped: a delta
        // link and an end, but no recorded start (the old 30-day window).
        // It reads as "no window" and re-anchors on the configured one —
        // the deploy-time backfill — rather than staying incremental on the
        // narrow token for up to 30 more days.
        commit_pending(&t, PendingCommit::Complete { delta_link: "https://g/delta-legacy".into(), window: None }).await.unwrap();
        sqlx::query("UPDATE microsoft_calendar_sync_state SET delta_window_start = NULL WHERE account_id = $1 AND owner_user_id = $2")
            .bind(account_id)
            .bind(alice)
            .execute(&owner_pool)
            .await
            .unwrap();
        let s = load_sync_state(&t).await.unwrap();
        assert_eq!(s.delta_link.as_deref(), Some("https://g/delta-legacy"));
        assert_eq!(s.window, None, "an end without a start is not a window");
        assert_eq!(plan_round(&s, now), RoundStart::Full(Window::configured(now)));

        clear_cursor(&t).await.unwrap();
        let s = load_sync_state(&t).await.unwrap();
        assert!(s.delta_link.is_none() && s.resume_link.is_none() && s.window.is_none());
        let (start_col, end_col): (Option<DateTime<Utc>>, Option<DateTime<Utc>>) = sqlx::query_as(
            "SELECT delta_window_start, delta_window_end FROM microsoft_calendar_sync_state WHERE account_id = $1 AND owner_user_id = $2",
        )
        .bind(account_id)
        .bind(alice)
        .fetch_one(&owner_pool)
        .await
        .unwrap();
        assert!(start_col.is_none() && end_col.is_none(), "a restart clears both bounds");
        assert!(matches!(plan_round(&s, now), RoundStart::Full(_)));

        // The reconcile sweep: a lap's position round-trips, starting one
        // stamps `delta_reconciled_at`, continuing one does not, and a
        // restart abandons the lap but keeps the rotation point.
        assert!(s.reconciled_at.is_none());
        assert_eq!(s.sweep, VerifySweep::default());
        let open = VerifySweep { cursor: Some("AAMk-0200".into()), lap_end: Some(String::new()) };
        persist_sweep(&t, &open, true).await.unwrap();
        let s = load_sync_state(&t).await.unwrap();
        assert_eq!(s.sweep, open);
        let started_at = s.reconciled_at.expect("starting a lap stamps delta_reconciled_at");
        let further = VerifySweep { cursor: Some("AAMk-0400".into()), lap_end: Some(String::new()) };
        persist_sweep(&t, &further, false).await.unwrap();
        let s = load_sync_state(&t).await.unwrap();
        assert_eq!(s.sweep, further);
        assert_eq!(s.reconciled_at, Some(started_at), "continuing a lap does not re-stamp");
        clear_cursor(&t).await.unwrap();
        let s = load_sync_state(&t).await.unwrap();
        assert_eq!(s.sweep, VerifySweep { cursor: Some("AAMk-0400".into()), lap_end: None });

        // The row the worker's off switch lives on is the same row — and
        // the cursor writes never flipped it.
        let enabled: bool = sqlx::query_scalar(
            "SELECT sync_enabled FROM microsoft_calendar_sync_state WHERE account_id = $1 AND owner_user_id = $2",
        )
        .bind(account_id)
        .bind(alice)
        .fetch_one(&owner_pool)
        .await
        .unwrap();
        assert!(enabled);

        app.close().await;
        owner_pool.close().await;
        drop_db(&base, db).await;
    }

    /// An unchanged event re-reported by a delta pass (full resource) must
    /// not churn a coral that was ingested from the legacy `$select` fetch:
    /// same hash, same participants, same start → `Unchanged`. The legacy
    /// fetch had no `body`; its preview equals the body's text, so the
    /// coral shapes identically and is kept (the long-body case is
    /// `preview_ingested_coral_is_replaced_once_by_the_full_body_then_stable_pg`).
    #[tokio::test]
    async fn unchanged_delta_item_does_not_churn_the_stored_coral_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping unchanged_delta_item_does_not_churn_the_stored_coral_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "ms_cal_nochurn_org_role_ten";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let (owner_pool, app) = tenant_tree_as_app_role(&base, db, "org_ms_cal_nochurn").await;
        let account_id = Uuid::new_v4();
        let alice = Uuid::new_v4();
        let graph_a = seed_member_graph(&owner_pool, account_id, alice).await;

        // What the legacy fetch stored.
        let legacy = shape_graph_event(event(serde_json::json!({
            "id": "AAMkAG-1", "subject": "Sprint review", "bodyPreview": "Agenda",
            "start": { "dateTime": "2026-10-07T09:00:00.0000000" },
            "end": { "dateTime": "2026-10-07T09:30:00.0000000" },
            "location": { "displayName": "Room 4" },
            "attendees": [ { "emailAddress": { "name": "Bob" } } ]
        })));
        let atom = seed_coral(&owner_pool, account_id, graph_a, alice, CALENDAR_SOURCE_TYPE, &legacy.source_id, &legacy.content, legacy.timestamp, &legacy.participants).await;

        // What a delta pass carries for the same, unchanged event.
        let delta = shape_graph_event(event(serde_json::json!({
            "@odata.etag": "W/\"x\"", "id": "AAMkAG-1", "subject": "Sprint review", "bodyPreview": "Agenda",
            "body": { "contentType": "html", "content": "<p>Agenda</p>" },
            "start": { "dateTime": "2026-10-07T09:00:00.0000000", "timeZone": "UTC" },
            "end": { "dateTime": "2026-10-07T09:30:00.0000000", "timeZone": "UTC" },
            "location": { "displayName": "Room 4", "locationType": "default" },
            "attendees": [ { "type": "required", "emailAddress": { "name": "Bob", "address": "bob@ex.com" } } ],
            "isCancelled": false, "type": "singleInstance", "webLink": "https://outlook/x"
        })));
        let out = crate::services::source_reingest::retract_if_changed_item(&app, None, account_id, graph_a, &delta).await.unwrap();
        assert_eq!(out, Some(crate::services::source_reingest::ReingestOutcome::Unchanged));
        assert_eq!(atom_count(&owner_pool, atom).await, 1, "no churn");

        // And a genuine edit reported by delta still replaces it.
        let edited = shape_graph_event(event(serde_json::json!({
            "id": "AAMkAG-1", "subject": "Sprint review (moved)", "bodyPreview": "Agenda",
            "start": { "dateTime": "2026-10-07T09:00:00.0000000" },
            "end": { "dateTime": "2026-10-07T09:30:00.0000000" },
            "location": { "displayName": "Room 4" },
            "attendees": [ { "emailAddress": { "name": "Bob" } } ], "type": "singleInstance"
        })));
        let out = crate::services::source_reingest::retract_if_changed_item(&app, None, account_id, graph_a, &edited).await.unwrap();
        assert!(matches!(out, Some(crate::services::source_reingest::ReingestOutcome::Replaced { atoms_deleted: 1, .. })));
        assert_eq!(atom_count(&owner_pool, atom).await, 0);

        app.close().await;
        owner_pool.close().await;
        drop_db(&base, db).await;
    }

    /// The churn bound for the body change: a coral ingested from
    /// `bodyPreview` (a description longer than the preview) is replaced
    /// ONCE when a delta pass first carries the full body, and the next
    /// pass reporting the same event is `Unchanged` — never a replacement
    /// per pass. Drives the real `retract_if_changed_item` as the tenant
    /// app role, like the test above.
    #[tokio::test]
    async fn preview_ingested_coral_is_replaced_once_by_the_full_body_then_stable_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping preview_ingested_coral_is_replaced_once_by_the_full_body_then_stable_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "ms_cal_body_once_org_role_ten";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let (owner_pool, app) = tenant_tree_as_app_role(&base, db, "org_ms_cal_body_once").await;
        let account_id = Uuid::new_v4();
        let alice = Uuid::new_v4();
        let graph_a = seed_member_graph(&owner_pool, account_id, alice).await;

        let p1 = "Agenda for the quarterly review: revenue, pipeline, hiring plan and the platform roadmap through Q2.";
        let p2 = "Please read the pre-read deck before the call; we will not walk through it slide by slide, only the open questions.";
        let p3 = "Decision needed: freeze the API surface on March 1st.";
        let preview: String = format!("{p1} {p2} {p3}").chars().take(255).collect();
        assert!(!preview.contains("March 1st"), "fixture: the tail lies beyond the preview");
        let base_json = serde_json::json!({
            "id": "AAMkAG-long", "subject": "Quarterly review", "bodyPreview": preview,
            "start": { "dateTime": "2026-10-07T09:00:00.0000000" },
            "end": { "dateTime": "2026-10-07T10:00:00.0000000" },
            "attendees": [ { "emailAddress": { "name": "Bob", "address": "bob@ex.com" } } ]
        });

        // What the pre-body fetch stored: the preview only.
        let legacy = shape_graph_event(event(base_json.clone()));
        let atom = seed_coral(&owner_pool, account_id, graph_a, alice, CALENDAR_SOURCE_TYPE, &legacy.source_id, &legacy.content, legacy.timestamp, &legacy.participants).await;

        // First delta pass carrying the body: replaced once.
        let mut with_body = base_json.clone();
        with_body["body"] = serde_json::json!({ "contentType": "html", "content": format!("<p>{p1}</p><p>{p2}</p><p>{p3}</p>") });
        let delta = shape_graph_event(event(with_body.clone()));
        assert!(delta.content.contains("March 1st"), "{}", delta.content);
        let out = crate::services::source_reingest::retract_if_changed_item(&app, None, account_id, graph_a, &delta).await.unwrap();
        assert!(matches!(out, Some(crate::services::source_reingest::ReingestOutcome::Replaced { atoms_deleted: 1, .. })), "{out:?}");
        assert_eq!(atom_count(&owner_pool, atom).await, 0);
        let fresh = seed_coral(&owner_pool, account_id, graph_a, alice, CALENDAR_SOURCE_TYPE, &delta.source_id, &delta.content, delta.timestamp, &delta.participants).await;

        // Every later pass reporting the same event: unchanged, no churn.
        let again = shape_graph_event(event(with_body));
        let out = crate::services::source_reingest::retract_if_changed_item(&app, None, account_id, graph_a, &again).await.unwrap();
        assert_eq!(out, Some(crate::services::source_reingest::ReingestOutcome::Unchanged));
        assert_eq!(atom_count(&owner_pool, fresh).await, 1);

        // And an edit past the preview is now seen.
        let mut edited = base_json;
        edited["body"] = serde_json::json!({ "contentType": "html", "content": format!("<p>{p1}</p><p>{p2}</p><p>Decision needed: freeze the API surface on April 15th.</p>") });
        let edited = shape_graph_event(event(edited));
        let out = crate::services::source_reingest::retract_if_changed_item(&app, None, account_id, graph_a, &edited).await.unwrap();
        assert!(matches!(out, Some(crate::services::source_reingest::ReingestOutcome::Replaced { atoms_deleted: 1, .. })), "{out:?}");
        assert_eq!(atom_count(&owner_pool, fresh).await, 0);

        app.close().await;
        owner_pool.close().await;
        drop_db(&base, db).await;
    }

    /// Personal plane on the control tree: the account-wide connection's
    /// target resolves (owner + primary ocean), its cursor row is accepted
    /// by the widened `0272` policy, and a removed event's CHUNKED coral is
    /// retracted through `delete_source` (resolved to concrete ids first —
    /// the bare-id call that masked Google's personal gap would match
    /// nothing here).
    #[tokio::test]
    async fn personal_plane_cursor_row_and_chunked_retraction_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping personal_plane_cursor_row_and_chunked_retraction_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "ms_cal_personal_ctl";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let user_id: Uuid = sqlx::query_scalar("INSERT INTO users(id, email) VALUES (gen_random_uuid(), $1) RETURNING id")
            .bind(format!("user-{}@test.local", Uuid::new_v4()))
            .fetch_one(&pool)
            .await
            .unwrap();
        let account_id: Uuid = sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES ($1) RETURNING id")
            .bind(user_id)
            .fetch_one(&pool)
            .await
            .unwrap();
        let graph_id: Uuid = sqlx::query_scalar(
            "INSERT INTO graphs (account_id, name, kind, is_primary) VALUES ($1, 'Primary', 'personal', true) RETURNING id",
        )
        .bind(account_id)
        .fetch_one(&pool)
        .await
        .unwrap();

        let t = CalendarTarget::personal(&pool, account_id).await.unwrap();
        assert_eq!(t.owner_user_id, user_id);
        assert_eq!(t.graph_id, graph_id);
        assert!(matches!(t.plane, AccountPlane::Personal));

        // A reconcile lap recorded BEFORE any cursor commit — a first full
        // round that completes in one pass — creates the row itself, under
        // the widened `0272` policy; the cursor commit that follows keeps it.
        let lap = VerifySweep { cursor: Some("AAMk-0100".into()), lap_end: Some(String::new()) };
        persist_sweep(&t, &lap, true).await.unwrap();
        let s = load_sync_state(&t).await.unwrap();
        assert_eq!(s.sweep, lap);
        assert!(s.delta_link.is_none() && s.window.is_none());

        let window = Window::configured(ts("2026-10-07T12:00:00Z"));
        commit_pending(&t, PendingCommit::Complete { delta_link: "https://g/delta-p".into(), window: Some(window) }).await.unwrap();
        let s = load_sync_state(&t).await.unwrap();
        assert_eq!(s.delta_link.as_deref(), Some("https://g/delta-p"));
        assert_eq!(s.window, Some(window));
        assert_eq!(s.sweep, lap, "a cursor commit never touches the sweep");

        let start = ts("2026-10-07T09:00:00Z");
        let gone = seed_coral(&pool, account_id, graph_id, user_id, CALENDAR_SOURCE_TYPE, "AAMk-gone", "Deleted later", start, &[]).await;
        let kept = seed_coral(&pool, account_id, graph_id, user_id, CALENDAR_SOURCE_TYPE, "AAMk-kept", "Still there", start, &[]).await;
        assert_eq!(stored_event_ids(&t).await.unwrap(), ["AAMk-gone", "AAMk-kept"].into_iter().map(String::from).collect::<BTreeSet<_>>());

        let deleted = retract_source_ids(&t, None, &["AAMk-gone".to_string()]).await.unwrap();
        assert_eq!(deleted, 1);
        assert_eq!(atom_count(&pool, gone).await, 0, "the chunked coral is really gone");
        assert_eq!(atom_count(&pool, kept).await, 1);

        pool.close().await;
        drop_db(&base, db).await;
    }
}
