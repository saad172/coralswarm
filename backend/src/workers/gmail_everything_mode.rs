//! Gmail everything-mode sync worker (gsuite enterprise train PR G2.4).
//!
//! The whole-mailbox pull half of the contract's everything-mode carve-out
//! (`docs/connectors/CONNECTOR_CONTRACT.md` §4/§7, amended conformance test
//! #9, §6). `routes::gmail_everything_mode` owns the opt-in toggle
//! (`gmail_everything_mode`, migration `0197`/`0074`); this worker is the
//! ONLY code that actually performs the pull, driven entirely by that
//! table's `enabled` rows — never a default, never lazily created here.
//!
//! # Consumes, does not write, the everything-mode precedent
//!
//! Founder decision (`.scratch/gsuite-enterprise/plan.md`, "FOUNDER ANSWERS
//! Q1–Q3"): "Q2 = NO — G2.4 waits for Outlook 3.4." m365 enterprise train PR
//! 3.4 (`workers::outlook_everything_mode`, merged #700) is therefore the
//! FIRST implementation of this carve-out and the contract's amended
//! conformance test #9; this module is a structural + mechanical port of
//! that one, not an independent design. Every shape decision below —
//! per-(account, owner) table, atomic claim, release-on-success/backoff-
//! on-failure, `claim_id` fencing — is `workers::outlook_everything_mode`'s
//! own, carried over rather than re-derived, because PR #700's four
//! CodeRabbit rounds already found and fixed every race this shape is
//! vulnerable to. See that module's own doc for the full incident-by-
//! incident history (F1 atomic claim, F2 backoff correction, F3 fencing
//! token); this doc only calls out where Gmail's shape genuinely differs.
//!
//! # Genuine difference #1: NOT a sibling-of-calendar coupling concern —
//! # Gmail already keeps everything mode structurally separate
//!
//! Outlook's own doc explains why its everything-mode worker is a SIBLING of
//! `workers::microsoft_calendar_sync`, not a merge into it: `sync()` is
//! shared with an unrelated, default-on, non-sensitive feature (calendar),
//! and coupling them would make every calendar tick also attempt a mail
//! pull. Gmail has no such default-on `sync()` caller to protect — Gmail's
//! `ConnectorProvider::sync()` (`connectors::google_gmail::sync`, the opt-in
//! thread + live-follow path) is itself already opt-in, never default-on.
//! The SAME reasoning still applies in a different shape: `connectors::
//! google_gmail::list_everything_mode_page`/`get_everything_mode_message`
//! are dedicated, non-trait functions — deliberately NOT threaded through
//! `history.list`/`google_watch_channels`/`gmail_watched_threads`, so
//! enabling everything mode can never change what the opt-in/live-follow
//! path's OWN sync tick or push handler does.
//!
//! # Genuine difference #2: Gmail tokens are ALWAYS per-member — no
//! # `TokenOwner::AccountWide` arm exists on either plane
//!
//! `workers::outlook_everything_mode`'s consumer-plane tick uses
//! `TokenOwner::AccountWide` (Microsoft's personal-plane Outlook tokens are
//! account-wide) while its org-plane tick uses `TokenOwner::Member`. Gmail
//! has no such split: `connectors::google::Surface::Gmail.token_owner_for`
//! (gsuite Train G2 PR 1) returns `TokenOwner::Member(user_id)`
//! UNCONDITIONALLY, on BOTH planes — the per-member token isolation
//! `google_gmail.rs`'s own module doc documents ("no fallback, ever").
//! [`sync_owner`] therefore takes `owner_user_id` directly (no separate
//! `token_owner` parameter to select between two shapes — there is only
//! ever one), and BOTH [`tick_org`] and [`tick_consumer`] load credentials
//! as `TokenOwner::Member(owner_user_id)`.
//!
//! # Genuine difference #3: rate-limit key granularity
//!
//! Gmail's quota is per Google USER, and one Google user can be connected
//! from two CoralSwarm accounts (a personal account and an org member, e.g.
//! the founder's own mailbox). So this worker keys both the shared limiter
//! (`graph_rate_limit::scoped`) and its own get pacer ([`pace_mailbox`]) by
//! the mailbox (`connectors::google_gmail::rate_key`, from the token row's
//! mailbox email), not by `account_id`. Until 2026-10 it used
//! `"google_gmail:{account_id}"`, which gave each connection of one mailbox
//! its own bucket.
//!
//! # Both planes are genuinely reachable — same as `workers::
//! outlook_everything_mode`, unlike `microsoft_calendar_sync`'s consumer
//! no-op
//!
//! * **`TenantId::Org(org_id)`** ([`tick_org`]): resolves the tenant's one
//!   account (`resolve_tenant_account`, the same control-plane
//!   `tenant_databases` lookup every sibling worker uses), then reads
//!   `gmail_everything_mode` directly off the TENANT pool — a
//!   single-account, already-scoped query (ADR-0007).
//! * **`TenantId::Consumer`** ([`tick_consumer`]): a genuine cross-account
//!   sweep across every personal account on the shared consumer database,
//!   closed the identical way `workers::outlook_everything_mode`'s consumer
//!   tick (and `workers::google_watch_renewal`'s) already closes it: a
//!   `SECURITY DEFINER` bootstrap function (`resolve_gmail_everything_
//!   mode_due()`, migration `0197`) returns bare `(account_id,
//!   owner_user_id, graph_id, claim_id)` ids for every enabled row, and each
//!   is RE-READ (never trusted as-is — TOCTOU/staleness) inside its own
//!   `begin_tenant_system_scoped` transaction by [`owner_state`].
//!
//! # Ingest destination
//!
//! `graph_id` is never re-resolved here — it is read verbatim from the
//! `gmail_everything_mode` row, which `set_everything_mode` populated (and
//! keeps current on every re-enable) via `resolve_primary_graph(..,
//! IngestCaller::Attributable(owner))` at opt-in time. That is the entire
//! enforcement of contract condition (b) ("content lands exclusively in that
//! owner's private ocean") on the ingest side: there is no OTHER graph_id
//! this worker could reach for.
//!
//! # The walk
//!
//! Each tick walks `users.messages.list` newest-first
//! (`connectors::google_gmail::list_everything_mode_page`, 500 ids per
//! page) and, per page, asks Postgres in one query which ids already have
//! corals ([`already_ingested`]). Only the rest are `messages.get`-ed and
//! ingested. After each fully processed page the next `pageToken` is saved
//! ([`save_walk_position`]), so an interruption loses at most the page in
//! hand, and resuming that page skips what it already stored.
//!
//! A walk records when it started (`walk_started_at`); when the listing
//! runs out, that becomes `high_water_at`, and the next walk lists only
//! `q=after:` it, minus [`HIGH_WATER_SLACK_SECS`]. So a tick with no new
//! mail costs one list call and no gets. The walk's floor is stored with it
//! (`walk_after`): a `pageToken` is only valid for the query it came from.
//!
//! Mail can join the listing with an old date, below any floor: moved out
//! of Spam days later, restored from Trash, imported. So once every
//! [`FULL_WALK_EVERY_SECS`] (`last_full_walk_at`) a new walk is a FULL one,
//! with no floor. That costs list calls only (500 ids each), since known
//! ids are skipped. Rows from before migration `0279`/`0117` have no mark,
//! so their next walk is a full one.
//!
//! A tick lists at most [`TICK_BUDGET`]`.max_list_pages` pages (50,000 ids),
//! so a full walk of a mailbox over about 50k messages takes several ticks.
//! That is expected: each tick resumes from the saved `pageToken`.
//!
//! Before 2026-10 a completed walk restarted from the newest message and
//! re-fetched every id on every tick. From 2026-09-24 that blew Gmail's
//! per-user quota mid-walk on every tick, the tick kept none of its work,
//! and personal Gmail corals went stale.
//!
//! # Failures never lose a message
//!
//! A message the walk moves past is never listed again (until the next full
//! walk), so a page is only moved past once each of its messages is stored,
//! hidden, gone (`404`), or permanently unusable (`422` from `ingest_atom`,
//! an unparseable body). Any other `messages.get` failure (`5xx`, timeout,
//! `401`, network) or ingest failure ends the tick on that page, and the
//! next tick retries it. `page_failures` counts consecutive failed ticks on
//! one page that got nowhere on it, all on the same message
//! (`page_failing_id`); a tick that stored something there resets it, as
//! does a failure on a different message. After [`MAX_PAGE_ATTEMPTS`], the
//! next tick gives up on THAT message if it fails again, names it on
//! `last_error` (kept for a day), and records it in `given_up_ids`
//! (migration `0280`/`0118`), so one bad message cannot pin the walk
//! forever. Bounded walks skip given-up messages; each daily full walk tries
//! each once more (it may have recovered, e.g. after an ingest outage) and,
//! if it still fails, moves on without stalling. A `422` from ingest is
//! given up on the same way: it comes from a validation rule we wrote, so a
//! bad deploy must not lose mail for good. A message that can never be
//! ingested (a body that does not parse, or nothing ingestable in it) is
//! permanent (`unreadable_ids`): no walk fetches it again. A body-parser
//! regression that wrongly empties messages needs that column cleared by
//! hand. A message gone or hidden by the time of the get
//! is skipped for the rest of that walk only (`walk_skip_ids`), since hidden
//! mail can be restored; so is a given-up message still failing on its
//! retry. That way a tick paused at its budget never spends the next tick's
//! budget fetching the same messages again.
//!
//! The access token is refreshed [`WORKER_TOKEN_REFRESH_MARGIN`] ahead of
//! expiry, well over a tick's run time, so it cannot expire mid-walk.
//!
//! # Claims
//!
//! Discovery claims rows 15 minutes ahead and walks them one after another,
//! so a claim can run out before its row's turn. The walk renews its claim
//! before its first Gmail call ([`renew_claim`]: if the claim is no longer
//! ours, it does nothing), after every page ([`save_walk_position`]), and
//! every [`WalkBudget::renew_claim_every_gets`] gets within a page, so a
//! second machine cannot walk the same mailbox at the same time.
//!
//! # Quota
//!
//! The limit that trips is `total_query_cost`: 6,000 units a minute per
//! Google user, and a `messages.get(format=full)` costs about 50 of them
//! (measured in prod, 2026-10-07; see
//! `connectors::google_gmail::GMAIL_QUERY_COST_PER_USER_PER_MINUTE`).
//! That is about 120 gets a minute per Google user, shared with the push
//! path. Gets are paced per mailbox to about 90 a minute ([`GET_INTERVAL`]),
//! and [`TICK_BUDGET`] caps a tick's gets and list pages. A `429`, or a `403` naming a rate-limit reason
//! (`connectors::google::google_error_is_rate_limit`), ends the tick with
//! progress kept and the claim released so the next tick resumes. After real
//! progress that is routine pacing, not an error; only a tick rate-limited
//! before doing anything puts the reason on `last_error`. It is never read
//! as a stale pageToken. That restart is only for `400`/`404` sent with a
//! token, and costs list calls only, since known ids are skipped.
//!
//! A deterministic `422` rejection from `ingest_atom` is treated as
//! CONSUMED, not a retriable failure. See [`ingest_item`], which mirrors
//! `routes::google_push::run_gmail_sync_for_email`'s identical
//! 422-as-consumed arm.
//!
//! # Discovery ordering: `last_attempted_at`, not `last_synced_at` (fixed
//! # before merge — adversarial review, 2026-08-25)
//!
//! Both discovery queries (`enabled_owners_for_account`'s org-plane claim,
//! `resolve_gmail_everything_mode_due()`'s consumer-plane bootstrap,
//! migration `0197`) order by `last_attempted_at ASC NULLS FIRST`, and BOTH
//! [`record_tick_end`] and [`mark_failed_no_progress`] advance it on EVERY
//! outcome — success AND failure alike. An earlier draft of this worker
//! (ported byte-for-byte from `workers::outlook_everything_mode`, which has
//! the IDENTICAL gap — not something this PR is responsible for fixing on
//! the Outlook side, but not silently copied forward here either) ordered by
//! `last_synced_at` instead, which only success ever advances: a
//! permanently-failing owner (revoked Gmail access ⇒ `NotConfigured`, e.g.)
//! would sort to the FRONT of every claim query FOREVER, since its sort key
//! never moves forward while every healthy owner's does. A handful of such
//! owners could occupy the ENTIRE per-tick budget on every tick, starving
//! every healthy owner out indefinitely — reproduced against real Postgres
//! under the adversarial review before merge. `last_synced_at` keeps its
//! narrower "last SUCCESSFUL sync" meaning (only [`record_tick_end`]'s
//! clean-page branch advances it, preserved for any future status surface);
//! `last_attempted_at` is the pure, always-advancing rotation key.
//!
//! # Consumer-plane throughput (fixed before merge — adversarial review)
//!
//! `resolve_gmail_everything_mode_due()`'s `LIMIT` was raised from 5 (a
//! direct, unexamined port of Outlook's own `OWNERS_PER_TICK`) to 25 — see
//! migration `0197`'s own header, "LIMIT 25", for why a fleet-wide consumer-
//! plane bootstrap needs a materially higher cap than an org-plane claim
//! that is naturally scoped to one tenant's own membership.
//!
//! # Cadence
//!
//! [`POLL_INTERVAL`] mirrors `workers::outlook_everything_mode::
//! POLL_INTERVAL` (30 minutes) — the established cadence for background
//! whole-mailbox sync in this codebase.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};

use sqlx::PgPool;
use uuid::Uuid;

use crate::connectors::google_gmail::{pace_mailbox, GET_INTERVAL};
use crate::connectors::{self, ConnectorError, GoogleConnector, TokenOwner};
use crate::middleware::auth::UserContext;
use crate::routes::connectors::load_creds_bg;
use crate::ingest::atom::IngestAtomRequest;
use crate::routes::oauth::store_token_bundle;
use crate::store::{PostgresStore, Store};
use crate::tenancy::extractor::Tenant;
use crate::tenancy::runtime::{TenancyDecision, TenantContext};
use crate::tenancy::scope::begin_tenant_system_scoped;
use crate::tenancy::TenantId;
use crate::workers::tenant_harness::{Tick, WorkerTenant};
use crate::AppState;

/// See this module's own "Cadence" doc section.
pub const POLL_INTERVAL: Duration = Duration::from_secs(30 * 60);

// ── Account resolution (org tenant) ───────────────────────────────────────────

/// Identical resolution to `workers::outlook_everything_mode::
/// resolve_tenant_account` / `workers::microsoft_calendar_sync`'s own copy
/// (kept as its own copy per those modules' own doc comments: duplicating
/// one two-line query is cheaper than a shared dependency between
/// independent connector workers).
async fn resolve_tenant_account(control_pool: &PgPool, org_id: &str) -> Result<Uuid, String> {
    sqlx::query_scalar::<_, Uuid>("SELECT account_id FROM tenant_databases WHERE clerk_org_id = $1")
        .bind(org_id)
        .fetch_optional(control_pool)
        .await
        .map_err(|e| e.to_string())?
        .ok_or_else(|| format!("no tenant_databases row for org {org_id}"))
}

// ── Discovery ──────────────────────────────────────────────────────────────────

/// Per-tick owner budget — same reasoning and same value as `workers::
/// outlook_everything_mode::OWNERS_PER_TICK`: a tick must have a bounded
/// worst case, and ordering by `last_attempted_at ASC NULLS FIRST` makes the
/// budget self-rotating with no cursor state.
const OWNERS_PER_TICK: i64 = 5;

/// Up to [`OWNERS_PER_TICK`] `owner_user_id`s with `enabled = true` for
/// `account_id`, least-recently-ATTEMPTED first (fixed before merge — see
/// this module's own doc, "Discovery ordering" — NOT least-recently-
/// SYNCED, which only success advances and would let a permanently-failing
/// owner starve every healthy one out), CLAIMED atomically off `pool` (the
/// TENANT pool for an org tick — a single-account, already-scoped query, no
/// cross-account discovery needed: ADR-0007).
///
/// Byte-identical claim + fencing-token mechanics to `workers::
/// outlook_everything_mode::enabled_owners_for_account` — see that
/// function's own doc for the full incident history. Callers must thread
/// the returned `claim_id` through to [`record_tick_end`]/[`save_walk_position`]/
/// [`mark_failed_no_progress`], which fence their writes on it.
async fn enabled_owners_for_account(pool: &PgPool, account_id: Uuid) -> Result<Vec<(Uuid, Uuid)>, ConnectorError> {
    let mut tx = begin_tenant_system_scoped(pool, account_id)
        .await
        .map_err(|e| ConnectorError::Internal(format!("enabled_owners_for_account scoped tx: {e}")))?;
    let rows: Vec<(Uuid, Uuid)> = sqlx::query_as(
        "UPDATE gmail_everything_mode gem \
         SET claimed_until = now() + interval '15 minutes', claim_id = gen_random_uuid() \
         FROM ( \
             SELECT id FROM gmail_everything_mode \
             WHERE account_id = $1 AND enabled = true \
               AND (claimed_until IS NULL OR claimed_until < now()) \
             ORDER BY last_attempted_at ASC NULLS FIRST \
             LIMIT $2 \
             FOR UPDATE SKIP LOCKED \
         ) claimable \
         WHERE gem.id = claimable.id \
         RETURNING gem.owner_user_id, gem.claim_id",
    )
    .bind(account_id)
    .bind(OWNERS_PER_TICK)
    .fetch_all(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("enabled_owners_for_account claim: {e}")))?;
    tx.commit()
        .await
        .map_err(|e| ConnectorError::Internal(format!("enabled_owners_for_account commit: {e}")))?;
    Ok(rows)
}

/// CONSUMER-plane cross-account discovery: every `(account_id,
/// owner_user_id)` pair the `SECURITY DEFINER` bootstrap
/// (`resolve_gmail_everything_mode_due()`, migration `0197`) reports enabled
/// AND CLAIMS. Bare ids only — the row's `graph_id`/`resume_cursor` are
/// re-derived by [`owner_state`] inside a properly scoped transaction before
/// use, never trusted from this bypass read. Byte-identical shape to
/// `workers::outlook_everything_mode::due_accounts`.
async fn due_accounts(control_pool: &PgPool) -> Result<Vec<(Uuid, Uuid, Uuid)>, ConnectorError> {
    let rows: Vec<(Uuid, Uuid, Uuid, Uuid)> = sqlx::query_as(
        "SELECT account_id, owner_user_id, graph_id, claim_id FROM resolve_gmail_everything_mode_due()",
    )
    .fetch_all(control_pool)
    .await
    .map_err(|e| ConnectorError::Internal(format!("due_accounts: {e}")))?;
    Ok(rows.into_iter().map(|(a, o, _g, c)| (a, o, c)).collect())
}

/// This owner's LIVE walk state, re-read (and re-verified `enabled = true`)
/// inside a properly scoped transaction — the single TOCTOU-safe read both
/// [`tick_org`] (after its own bulk listing) and [`tick_consumer`] (after the
/// bootstrap bypass) route through before doing anything with Gmail. `None`
/// covers disabled, no row, or a race where the row changed between
/// discovery and here — never an error, just nothing to do this tick.
/// Byte-identical shape and staleness posture to `workers::
/// outlook_everything_mode::owner_state` — see that function's own doc for
/// why leaving the discovery claim held until its TTL here is deliberate,
/// not a bug.
async fn owner_state(pool: &PgPool, account_id: Uuid, owner_user_id: Uuid) -> Result<Option<OwnerState>, ConnectorError> {
    let mut tx = begin_tenant_system_scoped(pool, account_id)
        .await
        .map_err(|e| ConnectorError::Internal(format!("owner_state scoped tx: {e}")))?;
    let row: Option<OwnerStateRow> = sqlx::query_as(
        "SELECT graph_id, resume_cursor, walk_started_at, walk_after, high_water_at, \
                last_full_walk_at, page_failures, page_failing_id, unreadable_ids, given_up_ids, walk_skip_ids \
         FROM gmail_everything_mode \
         WHERE account_id = $1 AND owner_user_id = $2 AND enabled = true",
    )
    .bind(account_id)
    .bind(owner_user_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("owner_state query: {e}")))?;
    tx.rollback().await.ok();
    Ok(row.map(|row| {
        let walk = WalkPos { cursor: row.1, started_at: row.2, after: row.3 };
        // The walk's own list only means anything while that walk runs.
        let (walk_list, dirty) = if walk.in_progress() { (row.10, false) } else { (Vec::new(), !row.10.is_empty()) };
        OwnerState {
            graph_id: row.0,
            walk,
            high_water_at: row.4,
            last_full_walk_at: row.5,
            page_failures: row.6,
            page_failing_id: row.7,
            skips: SkipLists { unreadable: row.8, given_up: row.9, walk: walk_list, dirty },
        }
    }))
}

/// `owner_state`'s row, in column order.
type OwnerStateRow = (
    Uuid,
    Option<String>,
    Option<DateTime<Utc>>,
    Option<DateTime<Utc>>,
    Option<DateTime<Utc>>,
    Option<DateTime<Utc>>,
    i32,
    Option<String>,
    Vec<String>,
    Vec<String>,
    Vec<String>,
);

/// [`SkipLists::to_write`]'s `(unreadable_ids, given_up_ids, walk_skip_ids)`.
type ListsToWrite = (Option<Vec<String>>, Option<Vec<String>>, Option<Vec<String>>);

/// Message ids the walk skips (migration `0280`/`0118`), oldest first.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct SkipLists {
    /// Never ingestable: the body does not parse, or has nothing ingestable
    /// once normalized. A Gmail message's content never changes, so no walk
    /// fetches these again. A body-parser regression that wrongly empties
    /// messages needs this column cleared by hand to recover them.
    unreadable: Vec<String>,
    /// Given up on after they kept failing (a Gmail 5xx on that message, or
    /// our own ingest failing), or rejected by ingest (422, a rule we wrote).
    /// That can clear up, so a bounded walk skips them but each full walk
    /// tries each once more (S2 and fix round 3, review of #1168).
    given_up: Vec<String>,
    /// Fetched by the walk in progress and not to be fetched again by it:
    /// gone or hidden by the time of the get, or a given-up message still
    /// failing on its retry (fix round 2, review of #1168). Without it, a
    /// tick that pauses at its budget re-fetches them on the next tick, and
    /// can spend every tick's budget that way. Cleared when the walk ends.
    walk: Vec<String>,
    /// Changed since loaded: only then are the lists written back.
    dirty: bool,
}

impl SkipLists {
    fn add_unreadable(&mut self, id: &str) {
        self.forget_given_up(id);
        if !self.unreadable.iter().any(|u| u == id) {
            self.unreadable.push(id.to_string());
            self.dirty = true;
        }
    }
    fn add_given_up(&mut self, id: &str) {
        if !self.given_up.iter().any(|g| g == id) {
            self.given_up.push(id.to_string());
            self.dirty = true;
        }
    }
    /// It was stored, or is gone: no longer given up on.
    fn forget_given_up(&mut self, id: &str) {
        let before = self.given_up.len();
        self.given_up.retain(|g| g != id);
        self.dirty |= self.given_up.len() != before;
    }
    fn add_walk(&mut self, id: &str) {
        if !self.walk.iter().any(|w| w == id) {
            self.walk.push(id.to_string());
            self.dirty = true;
        }
    }
    /// `(unreadable_ids, given_up_ids, walk_skip_ids)` to write, capped, or
    /// all `None` (keep what is stored) when nothing changed.
    fn to_write(&self) -> ListsToWrite {
        if !self.dirty {
            return (None, None, None);
        }
        (
            Some(capped(&self.unreadable, MAX_UNREADABLE_IDS)),
            Some(capped(&self.given_up, MAX_GIVEN_UP_IDS)),
            Some(capped(&self.walk, MAX_WALK_SKIP_IDS)),
        )
    }
}

/// Where a walk is. Persisted as `resume_cursor`/`walk_started_at`/
/// `walk_after`. See this module's own doc, "The walk".
#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct WalkPos {
    /// `pageToken` of the page the walk is on. `None` = its first page.
    cursor: Option<String>,
    /// When the walk started; becomes `high_water_at` when it completes.
    /// `None` on a walk begun before migration `0279`/`0117` (start unknown).
    started_at: Option<DateTime<Utc>>,
    /// The walk's `after:` floor. `None` = a full walk. A `pageToken` is only
    /// valid for the query it came from, so a resumed walk reuses this.
    after: Option<DateTime<Utc>>,
}

impl WalkPos {
    fn in_progress(&self) -> bool {
        self.cursor.is_some() || self.started_at.is_some()
    }
}

/// One owner's row, as the walk reads it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct OwnerState {
    graph_id: Uuid,
    walk: WalkPos,
    /// When the last COMPLETED walk started. `None` = never completed.
    high_water_at: Option<DateTime<Utc>>,
    /// When the last completed FULL walk started.
    last_full_walk_at: Option<DateTime<Utc>>,
    /// Consecutive ticks that failed on the page at `walk.cursor` without
    /// getting anywhere on it (see [`TickEnd::Failed`]), all on
    /// `page_failing_id`.
    page_failures: i32,
    /// The message those ticks failed on. The walk gives up only on it.
    page_failing_id: Option<String>,
    skips: SkipLists,
}

impl OwnerState {
    /// The walk this tick continues, or the one it starts. A new walk is a
    /// full one if no walk has completed yet or the last full walk is more
    /// than [`FULL_WALK_EVERY_SECS`] old (mail can join the listing with an
    /// old date, below any `after:` floor); otherwise it lists only `after:`
    /// the high-water mark minus [`HIGH_WATER_SLACK_SECS`].
    fn walk_for_tick(&self, now: DateTime<Utc>) -> WalkPos {
        if self.walk.in_progress() {
            return self.walk.clone();
        }
        let full_due = match (self.high_water_at, self.last_full_walk_at) {
            (Some(_), Some(full)) => full < now - chrono::Duration::seconds(FULL_WALK_EVERY_SECS),
            _ => true,
        };
        WalkPos {
            cursor: None,
            started_at: Some(now),
            after: if full_due {
                None
            } else {
                self.high_water_at.map(|h| h - chrono::Duration::seconds(HIGH_WATER_SLACK_SECS))
            },
        }
    }
}

// ── Progress / error bookkeeping ────────────────────────────────────────────────

/// How long a walk's claim is extended each time it renews it — the same
/// 15 minutes discovery claims for. A tick renews before it starts and after
/// every page, so a claim taken at the head of a long serial sweep cannot
/// lapse mid-walk and let a second machine walk the same mailbox.
const CLAIM_RENEWAL: &str = "now() + interval '15 minutes'";

/// Renew this tick's claim: before touching Gmail, and every
/// [`WalkBudget::renew_claim_every_gets`] gets within a page. `false` = the
/// claim is no longer ours (it lapsed and another worker claimed the row):
/// stop, and write nothing.
async fn renew_claim(pool: &PgPool, account_id: Uuid, owner_user_id: Uuid, claim_id: Uuid) -> bool {
    let mut tx = match begin_tenant_system_scoped(pool, account_id).await {
        Ok(tx) => tx,
        Err(e) => {
            tracing::warn!(%account_id, %owner_user_id, %claim_id, error = %e, "gmail_everything_mode: renew_claim could not open a transaction");
            return false;
        }
    };
    let result = sqlx::query(&format!(
        "UPDATE gmail_everything_mode SET claimed_until = {CLAIM_RENEWAL} \
         WHERE account_id = $1 AND owner_user_id = $2 AND claim_id = $3"
    ))
    .bind(account_id)
    .bind(owner_user_id)
    .bind(claim_id)
    .execute(&mut *tx)
    .await;
    if let Err(e) = &result {
        tracing::warn!(%account_id, %owner_user_id, %claim_id, error = %e, "gmail_everything_mode: renew_claim write failed");
    }
    let held = matches!(result, Ok(ref res) if res.rows_affected() > 0);
    if !held {
        tracing::warn!(
            %account_id, %owner_user_id, %claim_id,
            "gmail_everything_mode: claim no longer held — stopping this owner's walk"
        );
    }
    tx.commit().await.is_ok() && held
}

/// Bank the walk's position mid-tick, after each fully processed page, so a
/// later interruption (rate limit, crash, deploy) never loses more than the
/// page in hand. Renews the claim, resets `page_failures` (a new page), and
/// saves the given-up ids (a page can only be moved past once they are
/// recorded, or the next walk would stall on them again). `false` = the
/// claim was lost (another worker owns the row now) and the caller must stop.
async fn save_walk_position(
    pool: &PgPool,
    account_id: Uuid,
    owner_user_id: Uuid,
    claim_id: Uuid,
    pos: &WalkPos,
    skips: &SkipLists,
) -> bool {
    let lists = skips.to_write();
    let Ok(mut tx) = begin_tenant_system_scoped(pool, account_id).await else {
        return false;
    };
    let result = sqlx::query(&format!(
        "UPDATE gmail_everything_mode \
         SET resume_cursor = $3, walk_started_at = $4, walk_after = $5, \
             page_failures = 0, page_failing_id = NULL, \
             unreadable_ids = COALESCE($7::text[], unreadable_ids), \
             given_up_ids = COALESCE($8::text[], given_up_ids), \
             walk_skip_ids = COALESCE($9::text[], walk_skip_ids), \
             claimed_until = {CLAIM_RENEWAL}, updated_at = now() \
         WHERE account_id = $1 AND owner_user_id = $2 AND claim_id = $6"
    ))
    .bind(account_id)
    .bind(owner_user_id)
    .bind(pos.cursor.as_deref())
    .bind(pos.started_at)
    .bind(pos.after)
    .bind(claim_id)
    .bind(lists.0)
    .bind(lists.1)
    .bind(lists.2)
    .execute(&mut *tx)
    .await;
    let held = match result {
        Ok(res) => res.rows_affected() > 0,
        Err(e) => {
            tracing::warn!(%account_id, %owner_user_id, %claim_id, error = %e, "gmail_everything_mode: save_walk_position write failed");
            false
        }
    };
    if !held {
        tracing::warn!(
            %account_id, %owner_user_id, %claim_id,
            "gmail_everything_mode: lost the claim mid-walk — stopping this tick"
        );
    }
    tx.commit().await.is_ok() && held
}

/// Prefix of the `last_error` note naming messages the walk gave up on.
const GIVEN_UP_PREFIX: &str = "skipped ";

/// `last_error`/`last_error_at` on a clean tick end, `$5` being the tick's
/// note. A clean tick normally clears `last_error`, but a note naming
/// messages the walk gave up on is kept for a day — those messages will not
/// be retried, so one 30-minute window is too short to notice them.
const CLEAN_LAST_ERROR: &str = "last_error = CASE \
         WHEN $5::text IS NOT NULL THEN $5::text \
         WHEN last_error LIKE 'skipped %' AND last_error_at > now() - interval '1 day' THEN last_error \
         ELSE NULL END, \
     last_error_at = CASE \
         WHEN $5::text IS NOT NULL THEN now() \
         WHEN last_error LIKE 'skipped %' AND last_error_at > now() - interval '1 day' THEN last_error_at \
         ELSE NULL END,";

/// How a tick that reached Gmail ended — what [`record_tick_end`] persists.
/// `note`: messages this tick gave up on (see [`MAX_PAGE_ATTEMPTS`]), written
/// to `last_error` so they stay visible even though the tick itself is clean.
#[derive(Debug, Clone, PartialEq, Eq)]
enum TickEnd {
    /// The listing ran out: the walk is done. Its start becomes
    /// `high_water_at` (and `last_full_walk_at`, if it was a full walk); the
    /// walk position clears.
    Completed { walk: WalkPos, note: Option<String> },
    /// Stopped at this tick's budget, or at a rate limit after real progress
    /// (routine pacing during a backlog, not an error). Claim released.
    /// `page_progress` as for `Failed`: it resets `page_failures` too (S3,
    /// review of #1168).
    Paused { walk: WalkPos, note: Option<String>, page_progress: bool },
    /// Google rate-limited the tick before it got anything done. Surfaced on
    /// `last_error`; the claim is released so the next tick tries again.
    RateLimited { walk: WalkPos, error: String },
    /// A real failure on a page (a failed `messages.get` or ingest, a token
    /// that went stale twice). Progress up to that page is kept and the claim
    /// backs off. `page_progress`: this tick stored (or gave up on) at least
    /// one message on the failing page. Then the next tick starts further
    /// along it, so `page_failures` resets to 0 instead of counting this
    /// tick (S-a, re-review of #1166): only ticks that got nowhere on the
    /// page count toward giving up. `failed_id`: the message it failed on,
    /// if one. Consecutive no-progress ticks count only while they fail on
    /// the SAME message (`page_failing_id`; B1, review of #1168).
    Failed { walk: WalkPos, error: String, page_progress: bool, failed_id: Option<String> },
}

impl TickEnd {
    /// A failure that is not about any one message (a list call, the
    /// known-id query, a stale token).
    fn failed(walk: WalkPos, error: String) -> Self {
        TickEnd::Failed { walk, error, page_progress: false, failed_id: None }
    }
}

/// Persist a tick's end state. `claim_id`-fenced exactly like the
/// `record_progress` it replaces (byte-identical mechanics to `workers::
/// outlook_everything_mode::record_progress` — see that function's own doc
/// for the incident history this shape closes). Every branch stamps
/// `last_attempted_at` (this module's own doc, "Discovery ordering"); only the
/// clean branches advance `last_synced_at`.
async fn record_tick_end(
    pool: &PgPool,
    account_id: Uuid,
    owner_user_id: Uuid,
    claim_id: Uuid,
    end: &TickEnd,
    skips: &SkipLists,
) {
    let Ok(mut tx) = begin_tenant_system_scoped(pool, account_id).await else {
        return;
    };
    let cap = |s: &str| s.chars().take(500).collect::<String>();
    let (unreadable_ids, given_up_ids, walk_skip_ids) = skips.to_write();
    let result = match end {
        TickEnd::Completed { walk, note } => {
            let full = walk.after.is_none();
            sqlx::query(&format!(
                "UPDATE gmail_everything_mode \
                 SET resume_cursor = NULL, walk_started_at = NULL, walk_after = NULL, \
                     page_failures = 0, page_failing_id = NULL, \
                     high_water_at = $3, \
                     last_full_walk_at = CASE WHEN $4 THEN $3 ELSE last_full_walk_at END, \
                     claimed_until = NULL, last_synced_at = now(), last_attempted_at = now(), \
                     {CLEAN_LAST_ERROR} \
                     unreadable_ids = COALESCE($7::text[], unreadable_ids), \
                     given_up_ids = COALESCE($8::text[], given_up_ids), \
                     walk_skip_ids = '{{}}', updated_at = now() \
                 WHERE account_id = $1 AND owner_user_id = $2 AND claim_id = $6"
            ))
            .bind(account_id)
            .bind(owner_user_id)
            .bind(walk.started_at)
            .bind(full)
            .bind(note.as_deref().map(cap))
            .bind(claim_id)
            .bind(&unreadable_ids)
            .bind(&given_up_ids)
            .execute(&mut *tx)
            .await
        }
        TickEnd::Paused { walk, note, page_progress } => {
            sqlx::query(&format!(
                "UPDATE gmail_everything_mode \
                 SET resume_cursor = $3, walk_started_at = $4, walk_after = $5, \
                     page_failures = CASE WHEN $10 THEN 0 ELSE page_failures END, \
                     page_failing_id = CASE WHEN $10 THEN NULL ELSE page_failing_id END, \
                     claimed_until = NULL, last_synced_at = now(), last_attempted_at = now(), \
                     {} \
                     unreadable_ids = COALESCE($8::text[], unreadable_ids), \
                     given_up_ids = COALESCE($9::text[], given_up_ids), \
                     walk_skip_ids = COALESCE($11::text[], walk_skip_ids), updated_at = now() \
                 WHERE account_id = $1 AND owner_user_id = $2 AND claim_id = $7",
                CLEAN_LAST_ERROR.replace("$5", "$6")
            ))
            .bind(account_id)
            .bind(owner_user_id)
            .bind(walk.cursor.as_deref())
            .bind(walk.started_at)
            .bind(walk.after)
            .bind(note.as_deref().map(cap))
            .bind(claim_id)
            .bind(&unreadable_ids)
            .bind(&given_up_ids)
            .bind(page_progress)
            .bind(&walk_skip_ids)
            .execute(&mut *tx)
            .await
        }
        TickEnd::RateLimited { walk, error } | TickEnd::Failed { walk, error, .. } => {
            // Rate limited: release the claim so the next tick resumes on
            // schedule (Gmail's limit is per minute; the poll is 30), and do
            // not count it against the page. A real failure backs off one
            // poll interval, as before. It resets the page's count if it got
            // somewhere on the page; otherwise it counts, but only while the
            // ticks keep failing on the same message (`page_failing_id`).
            // A failure not tied to any message (a list call, the known-id
            // query, a stale token) does not count: giving up on a message
            // would not fix it.
            let (backoff, counts, page_progress, failed_id) = match end {
                TickEnd::Failed { page_progress, failed_id, .. } => {
                    ("now() + interval '30 minutes'", failed_id.is_some(), *page_progress, failed_id.as_deref())
                }
                _ => ("NULL", false, false, None),
            };
            sqlx::query(&format!(
                "UPDATE gmail_everything_mode \
                 SET resume_cursor = $3, walk_started_at = $4, walk_after = $5, \
                     claimed_until = {backoff}, \
                     page_failures = CASE WHEN $10 THEN 0 \
                                          WHEN NOT $11 THEN page_failures \
                                          WHEN page_failing_id IS NOT DISTINCT FROM $12::text THEN page_failures + 1 \
                                          ELSE 1 END, \
                     page_failing_id = CASE WHEN $10 THEN NULL \
                                            WHEN NOT $11 OR $12::text IS NULL THEN page_failing_id \
                                            ELSE $12::text END, \
                     last_attempted_at = now(), last_error = $6, last_error_at = now(), \
                     unreadable_ids = COALESCE($8::text[], unreadable_ids), \
                     given_up_ids = COALESCE($9::text[], given_up_ids), \
                     walk_skip_ids = COALESCE($13::text[], walk_skip_ids), updated_at = now() \
                 WHERE account_id = $1 AND owner_user_id = $2 AND claim_id = $7"
            ))
            .bind(account_id)
            .bind(owner_user_id)
            .bind(walk.cursor.as_deref())
            .bind(walk.started_at)
            .bind(walk.after)
            .bind(cap(error))
            .bind(claim_id)
            .bind(&unreadable_ids)
            .bind(&given_up_ids)
            .bind(page_progress)
            .bind(counts)
            .bind(failed_id)
            .bind(&walk_skip_ids)
            .execute(&mut *tx)
            .await
        }
    };
    match result {
        Ok(res) if res.rows_affected() == 0 => {
            tracing::warn!(
                %account_id, %owner_user_id, %claim_id,
                "gmail_everything_mode: record_tick_end found a stale claim_id — another worker owns this row now, dropping write"
            );
        }
        Ok(_) => {}
        Err(e) => {
            tracing::warn!(%account_id, %owner_user_id, %claim_id, error = %e, "gmail_everything_mode: record_tick_end write failed");
        }
    }
    let _ = tx.commit().await;
}

/// A failure BEFORE any page was fetched (creds/token/Gmail-request error) —
/// records the error without touching `resume_cursor`. Byte-identical
/// mechanics to `workers::outlook_everything_mode::
/// mark_failed_no_progress`, PLUS `last_attempted_at = now()` (fixed before
/// merge — see this module's own doc, "Discovery ordering": this is the
/// branch a permanently-missing-credentials owner hits on EVERY tick, so it
/// is the one most responsible for the starvation incident this stamp
/// closes).
async fn mark_failed_no_progress(pool: &PgPool, account_id: Uuid, owner_user_id: Uuid, claim_id: Uuid, error: &str) {
    let Ok(mut tx) = begin_tenant_system_scoped(pool, account_id).await else {
        return;
    };
    let result = sqlx::query(
        "UPDATE gmail_everything_mode \
         SET claimed_until = now() + interval '30 minutes', last_attempted_at = now(), \
             last_error = $3, last_error_at = now(), updated_at = now() \
         WHERE account_id = $1 AND owner_user_id = $2 AND claim_id = $4",
    )
    .bind(account_id)
    .bind(owner_user_id)
    .bind(error.chars().take(500).collect::<String>())
    .bind(claim_id)
    .execute(&mut *tx)
    .await;
    match result {
        Ok(res) if res.rows_affected() == 0 => {
            tracing::warn!(
                %account_id, %owner_user_id, %claim_id,
                "gmail_everything_mode: mark_failed_no_progress found a stale claim_id — another worker owns this row now, dropping write"
            );
        }
        Ok(_) => {}
        Err(e) => {
            tracing::warn!(%account_id, %owner_user_id, %claim_id, error = %e, "gmail_everything_mode: mark_failed_no_progress write failed");
        }
    }
    let _ = tx.commit().await;
}

/// Pure decision logic, identical shape to `workers::
/// outlook_everything_mode::batch_ingest_outcome` (kept as its own copy —
/// see this module's own doc on why a sibling file, not a shared
/// dependency, is the deliberate choice here).
fn batch_ingest_outcome(total: usize, failed_ids: &[String]) -> Option<String> {
    if failed_ids.is_empty() {
        return None;
    }
    let failed = failed_ids.len();
    let sample = failed_ids.iter().take(5).cloned().collect::<Vec<_>>().join(", ");
    let ellipsis = if failed > 5 { ", …" } else { "" };
    Some(format!(
        "{failed} of {total} messages failed to ingest (source_ids: {sample}{ellipsis})"
    ))
}

// ── Sync ─────────────────────────────────────────────────────────────────────

/// What one owner's tick may spend against Gmail. See this module's own doc,
/// "Quota".
#[derive(Debug, Clone, Copy)]
struct WalkBudget {
    /// `messages.get` calls (5 quota units each) per tick.
    max_gets: usize,
    /// `messages.list` calls (5 quota units each, up to
    /// `EVERYTHING_MODE_LIST_PAGE_SIZE` ids) per tick.
    max_list_pages: usize,
    /// Minimum spacing between `messages.get` calls on one mailbox, across
    /// every row walking it ([`pace_mailbox`]).
    get_interval: Duration,
    /// Renew the claim every this many gets, as well as after every page,
    /// so one long page cannot outlast it.
    renew_claim_every_gets: usize,
}

/// Production per-tick budget. Gets are paced per MAILBOX at
/// [`GET_INTERVAL`] ([`pace_mailbox`]), shared by every row walking it, so
/// one mailbox stays at about 90 gets (about 4,500 query-cost units) a
/// minute. 120 gets is about 80 s of paced gets, or about 160 s if a second
/// row on the same mailbox is interleaved. 100 list pages is 50,000 ids, so the daily full walk
/// ([`FULL_WALK_EVERY_SECS`]) of a typical mailbox fits in one tick (list
/// calls only, known ids are never fetched). A mailbox over about 50k
/// messages needs several ticks per walk; that is expected, and each tick
/// resumes from the saved `pageToken`. A steady-state tick spends one list
/// call and no gets. Renewing the claim every 100 gets (about 67 s, or 133 s
/// shared) keeps a 15-minute claim far from lapsing even on one long page.
const TICK_BUDGET: WalkBudget = WalkBudget {
    max_gets: 120,
    max_list_pages: 100,
    get_interval: GET_INTERVAL,
    renew_claim_every_gets: 100,
};

/// How long before expiry this worker refreshes the access token: well over
/// a tick's run time (Google access tokens last an hour).
const WORKER_TOKEN_REFRESH_MARGIN: Duration = Duration::from_secs(10 * 60);

/// Consecutive failed ticks on one page, each getting nowhere on it, before
/// the walk gives up on the message that keeps failing there (S3, review of
/// #1166). Each failed tick backs off a poll interval, so this is roughly 1.5
/// to 3 hours of retries. The give-up tick gives up on its FIRST failing
/// message only: in ticks that got nowhere, that is the message that failed
/// every time. Any other message that fails in that tick gets its own
/// retries (S-a, re-review of #1166).
const MAX_PAGE_ATTEMPTS: i32 = 3;

/// Most given-up ids kept per mailbox (`given_up_ids`, oldest dropped
/// first). Give-ups are rare; this only bounds the row if something goes
/// badly wrong. A dropped id is just retried by a later full walk.
const MAX_GIVEN_UP_IDS: usize = 1000;

/// Most never-ingestable ids kept per mailbox (`unreadable_ids`). Empty
/// messages are more common than give-ups. A dropped id is fetched once
/// more by a later full walk and lands back in the list.
const MAX_UNREADABLE_IDS: usize = 5000;

/// Most ids one walk remembers having fetched (`walk_skip_ids`). A
/// dropped id is fetched once more by that walk.
const MAX_WALK_SKIP_IDS: usize = 2000;

/// The newest `max` of `ids` (oldest first), as persisted.
fn capped(ids: &[String], max: usize) -> Vec<String> {
    ids[ids.len().saturating_sub(max)..].to_vec()
}

/// How often a new walk is a FULL one (no `after:` floor), to pick up mail
/// that joins the listing with an old date. List calls only: known ids are
/// never re-fetched.
const FULL_WALK_EVERY_SECS: i64 = 24 * 60 * 60;

/// How far before the high-water mark a new walk starts listing. Covers
/// mail Gmail dates slightly before it surfaces in the listing, and clock
/// skew. Overlap costs list calls only: ids already ingested are never
/// re-fetched.
const HIGH_WATER_SLACK_SECS: i64 = 24 * 60 * 60;

/// Of `ids`, the Gmail message ids that already have at least one coral in
/// `graph_id`. One query per list page. `routes::ingest` stores every Gmail
/// message as `"{id}:chunk:{n}"` rows, so this is a byte-order prefix range
/// (`~>=~`/`~<~`, served by `idx_atoms_graph_source_id_pattern`'s
/// `text_pattern_ops`) rather than an equality match. `':chunk;'` is the
/// first string after every `':chunk:'`-prefixed one (`;` follows `:`).
async fn already_ingested(
    pool: &PgPool,
    account_id: Uuid,
    graph_id: Uuid,
    ids: &[String],
) -> Result<HashSet<String>, ConnectorError> {
    if ids.is_empty() {
        return Ok(HashSet::new());
    }
    let mut tx = begin_tenant_system_scoped(pool, account_id)
        .await
        .map_err(|e| ConnectorError::Internal(format!("already_ingested scoped tx: {e}")))?;
    let known: Vec<String> = sqlx::query_scalar(
        "SELECT ids.id FROM unnest($2::text[]) AS ids(id) \
         WHERE EXISTS ( \
             SELECT 1 FROM atoms a \
             WHERE a.graph_id = $1 AND a.source_type = $3 \
               AND a.source_id ~>=~ (ids.id || ':chunk:') \
               AND a.source_id ~<~ (ids.id || ':chunk;'))",
    )
    .bind(graph_id)
    .bind(ids)
    .bind(connectors::google_gmail::SOURCE_TYPE)
    .fetch_all(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("already_ingested query: {e}")))?;
    tx.rollback().await.ok();
    Ok(known.into_iter().collect())
}

#[cfg(test)]
tokio::task_local! {
    /// Test-only seam: source ids [`ingest_item`] answers as a `422`
    /// (`IngestResult::Rejected`) without calling ingest. No Gmail message
    /// the mock can serve reaches a real `ingest_atom` validation 422
    /// (`gmail_message_to_item` filters those shapes first).
    static TEST_INGEST_REJECTS: HashSet<String>;
}

enum IngestResult {
    Stored,
    /// A deterministic `422`: consumed, never retried — see [`ingest_item`].
    Rejected,
    Failed,
}

/// Ingest one message into the owner's ocean through the real `ingest_atom`
/// route handler.
async fn ingest_item(
    state: &Arc<AppState>,
    tenant: &Tenant,
    user_ctx: &UserContext,
    graph_id: Uuid,
    item: connectors::NormalizedItem,
    mailbox_namespace: &str,
) -> IngestResult {
    let source_id = item.source_id.clone();
    #[cfg(test)]
    if TEST_INGEST_REJECTS.try_with(|r| r.contains(&source_id)).unwrap_or(false) {
        return IngestResult::Rejected;
    }
    let req = IngestAtomRequest {
        graph_id,
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
    let source_namespace = mailbox_namespace.to_string();
    let origin = crate::source_origin::SourceOrigin::native("gmail","message","gmail_everything_mode").expect("static origin identifiers");
    let refused = crate::ingest::submit(crate::ingest::IngestRequest {
        origin: Some(&origin),
        ..crate::ingest::IngestRequest::private(crate::ingest::Member {
            provider_namespace: Some(&source_namespace),
            ..crate::ingest::Member::new(state, &tenant, &user_ctx, req)
        })
    }).await.err();
    match refused {
        None => IngestResult::Stored,
        // F4 (adversarial review): a deterministic validation rejection
        // must NOT block the walk — retrying it would never turn it into
        // success. Matches `routes::google_push::run_gmail_sync_for_email`'s
        // identical invalid-as-consumed arm.
        Some(refused) if refused.is_invalid() => {
            tracing::warn!(
                account_id = %user_ctx.account_id, owner_user_id = %user_ctx.user_id, source_id = %source_id,
                error = %refused,
                "gmail_everything_mode: the ingest door rejected this message as invalid — \
                 skipping permanently, not blocking the walk"
            );
            IngestResult::Rejected
        }
        Some(refused) => {
            tracing::warn!(
                account_id = %user_ctx.account_id, owner_user_id = %user_ctx.user_id,
                error = %refused, source_id = %source_id,
                "gmail_everything_mode: ingest_atom failed for a message"
            );
            IngestResult::Failed
        }
    }
}

/// Sync one owner's mailbox. `Ok(true)` = Gmail was reached and the tick's
/// end state recorded, `Ok(false)` = skipped (disabled, no row, or a race
/// since discovery — retried next tick), `Err` = a failure BEFORE any page
/// was fetched (token/Gmail error), recorded via [`mark_failed_no_progress`]
/// by the caller.
///
/// `claim_id` is the fencing token the caller ([`tick_org`]/[`tick_consumer`])
/// received from [`enabled_owners_for_account`]/
/// `resolve_gmail_everything_mode_due()` at discovery time. `owner_user_id`
/// doubles as this connector's token owner — see this module's own doc,
/// "Genuine difference #2": Gmail tokens are ALWAYS `TokenOwner::Member`, on
/// both planes, so there is no separate `token_owner` parameter to choose
/// between (contrast `workers::outlook_everything_mode::sync_owner`, which
/// takes one).
async fn sync_owner(
    pool: &PgPool,
    control_pool: &PgPool,
    state: &Arc<AppState>,
    account_id: Uuid,
    owner_user_id: Uuid,
    claim_id: Uuid,
    org_id: Option<&str>,
) -> Result<bool, ConnectorError> {
    let Some(owner) = owner_state(pool, account_id, owner_user_id).await? else {
        return Ok(false);
    };
    // The member's pause or off switch for Gmail, before any token or mailbox
    // read. The walk's saved position stays, so a resumed source goes on from it.
    if crate::connectors::catalog::pause::is_paused(
        control_pool, account_id, owner_user_id, crate::connectors::catalog::pause::GOOGLE_GMAIL,
    ).await {
        return Ok(false);
    }

    let token_owner = TokenOwner::Member(owner_user_id);
    // The account's DEK plane, from the `org_id` this tick ALREADY resolved —
    // `resolve_for_org` is THE decision table (`crypto::DekPlane`), and
    // passing the known `org_id` means zero extra `accounts` round trips on
    // this per-owner path. `None` (Personal) resolves to the consumer plane
    // exactly as before; an org owner now reads its key from the org's own
    // tenant `account_keys`, which is the only place it exists.
    let dek_plane =
        crate::crypto::DekPlane::resolve_for_org(state, control_pool, account_id, org_id)
            .await
            .map_err(|e| ConnectorError::Internal(format!("resolve DEK plane: {e}")))?;
    // A whole-mailbox walk for someone who has LEFT the org never runs
    // (settings launch PR 2.8): the gate offboards their capture instead.
    // An `Err`, like missing credentials below, so the row backs off.
    if org_id.is_some()
        && !crate::connectors::member_offboarding::admit_capture_or_offboard(
            &state.http_client,
            control_pool,
            Some(&dek_plane),
            account_id,
            owner_user_id,
            "gmail_everything_mode",
        )
        .await
    {
        return Err(ConnectorError::NotConfigured(format!(
            "owner {owner_user_id} is no longer a member of this account"
        )));
    }
    // See `workers::outlook_everything_mode::sync_owner`'s own doc, F1: a
    // missing-credentials row is still `enabled = true` and will be
    // rediscovered, so this counts as a failure for backoff purposes (routes
    // through the caller's `mark_failed_no_progress`) rather than a silent
    // `Ok(false)` that would leave `claimed_until` held with no error
    // recorded and no backoff.
    let Some(mut creds) = load_creds_bg(control_pool, &dek_plane, account_id, "google_gmail", token_owner).await? else {
        return Err(ConnectorError::NotConfigured(format!(
            "no gmail credentials for owner {owner_user_id}"
        )));
    };

    let connector = GoogleConnector;
    // Refresh well ahead of expiry: a walk runs for a minute or more, and a
    // token that expires mid-walk 401s every remaining call (review of #1166).
    connector
        .refresh_if_expiring_within(&state.http_client, &mut creds, WORKER_TOKEN_REFRESH_MARGIN)
        .await?;

    // Persist a refreshed token BEFORE the fetch — same discipline `workers::
    // outlook_everything_mode::sync_owner` takes (CodeRabbit round 1 on PR
    // #700): fetching first meant a fetch failure discarded the newer token.
    if let Some(refreshed) = creds.refreshed.as_ref() {
        if let Err(e) = store_token_bundle(control_pool, &dek_plane, account_id, "google_gmail", token_owner, refreshed).await {
            tracing::warn!(
                %account_id, %owner_user_id, status = ?e.status(),
                "gmail_everything_mode: failed to persist refreshed token"
            );
        }
    }

    // Per Google user, not per CoralSwarm account: see `google_gmail::rate_key`.
    let key = connectors::google_gmail::rate_key(account_id, &creds.provider_workspace_id);
    crate::graph_rate_limit::scoped(
        key.clone(),
        sync_owner_with_token(
            pool,
            state,
            account_id,
            owner_user_id,
            claim_id,
            org_id,
            &creds.access_token,
            &key,
            owner,
            TICK_BUDGET,
        ),
    )
    .await
}

/// The walk itself, once a usable access token is in hand. See this module's
/// own doc, "The walk".
#[allow(clippy::too_many_arguments)] // one call site plus tests; every argument is a distinct identity or budget
async fn sync_owner_with_token(
    pool: &PgPool,
    state: &Arc<AppState>,
    account_id: Uuid,
    owner_user_id: Uuid,
    claim_id: Uuid,
    org_id: Option<&str>,
    access_token: &str,
    mailbox_key: &str,
    owner: OwnerState,
    budget: WalkBudget,
) -> Result<bool, ConnectorError> {
    use connectors::google_gmail::{
        get_everything_mode_message, list_everything_mode_page, EverythingGetOutcome, EverythingListOutcome,
    };

    // Discovery may have claimed this row a while ago (a serial sweep); make
    // sure it is still ours, and keep it ours, before any Gmail call.
    if !renew_claim(pool, account_id, owner_user_id, claim_id).await {
        return Ok(false);
    }

    let http = &state.http_client;
    let graph_id = owner.graph_id;
    let mut walk = owner.walk_for_tick(Utc::now());
    // The last few ticks all failed on this page, each on the same message
    // and without getting anywhere: this time, give up on THAT message if it
    // fails again (B1, review of #1168: never on whichever message happens
    // to fail first), naming it on `last_error` and remembering it in
    // `given_up_ids`, instead of pinning the walk here forever.
    let mut lenient_id: Option<String> = if owner.walk.in_progress() && owner.page_failures >= MAX_PAGE_ATTEMPTS {
        owner.page_failing_id.clone()
    } else {
        None
    };
    // A full walk tries each kept-failing message once more (S2, review of
    // #1168); a bounded walk skips them. Unreadable ones are always skipped.
    let full_walk = walk.after.is_none();
    let mut skips = owner.skips.clone();

    let store: Arc<dyn Store> = Arc::new(PostgresStore::new(pool.clone()));
    let tenant = match org_id {
        None => Tenant::legacy(state),
        Some(org_id) => Tenant {
            store: Some(store),
            pool: Some(pool.clone()),
            decision: Some(TenancyDecision::Org(TenantContext {
                org_id: org_id.to_string(),
                account_id,
                user_id: owner_user_id,
                pool: pool.clone(),
                page_pool: None,
            })),
            class: crate::route_class::RouteClass::Other,
        },
    };
    let user_ctx = UserContext {
        user_id: owner_user_id,
        account_id,
        role: "member".into(),
        device_id: String::new(),
    };

    let mut restarted = false;
    let mut gets = 0usize;
    let mut list_pages = 0usize;
    // Anything stored or any page advanced this tick.
    let mut progressed = false;
    // Messages given up on this tick, with why.
    let mut given_up: Vec<String> = Vec::new();

    let end = loop {
        if list_pages >= budget.max_list_pages {
            break TickEnd::Paused { walk, note: given_up_note(&given_up), page_progress: false };
        }
        list_pages += 1;
        let outcome = match list_everything_mode_page(http, access_token, walk.cursor.as_deref(), walk.after).await {
            Ok(outcome) => outcome,
            // Nothing reached Gmail successfully yet: the caller records it
            // without touching the walk state, as before.
            Err(e) if list_pages == 1 => return Err(e),
            Err(e) => break TickEnd::failed(walk, with_note(e.to_string(), &given_up)),
        };
        let (ids, next_page_token) = match outcome {
            EverythingListOutcome::Page { ids, next_page_token } => (ids, next_page_token),
            EverythingListOutcome::RateLimited(error) => {
                break rate_limited_end(walk, error, progressed, false, &given_up, account_id, owner_user_id);
            }
            EverythingListOutcome::StaleToken(detail) => {
                if restarted {
                    // Google's detail first so `last_error`'s 500-char cap
                    // cuts the fixed suffix, never the reasons.
                    walk.cursor = None;
                    break TickEnd::failed(
                        walk,
                        format!("{detail} (gmail everything-mode: pageToken stale again immediately after a restart)"),
                    );
                }
                // Restart this walk (same query) from its first page. Nothing
                // already ingested is fetched again, so this costs list calls.
                tracing::warn!(
                    %account_id, %owner_user_id, google_error = %detail,
                    "gmail everything-mode: messages.list rejected the stored pageToken — \
                     restarting the walk from its first page"
                );
                restarted = true;
                lenient_id = None;
                walk.cursor = None;
                if !save_walk_position(pool, account_id, owner_user_id, claim_id, &walk, &skips).await {
                    return Ok(true);
                }
                continue;
            }
        };

        let known = match already_ingested(pool, account_id, graph_id, &ids).await {
            Ok(known) => known,
            Err(e) => break TickEnd::failed(walk, with_note(e.to_string(), &given_up)),
        };
        let mut failed_ids: Vec<String> = Vec::new();
        let mut attempted = 0usize;
        let mut stop: Option<TickEnd> = None;
        // Anything stored or given up on THIS page this tick: the next tick
        // starts further along it. See `TickEnd::Failed`.
        let mut page_progress = false;
        let unreadable: HashSet<String> = skips.unreadable.iter().cloned().collect();
        let kept_failing: HashSet<String> = skips.given_up.iter().cloned().collect();
        let done_this_walk: HashSet<String> = skips.walk.iter().cloned().collect();
        let unseen: Vec<String> = ids
            .iter()
            .filter(|id| {
                !known.contains(*id)
                    && !unreadable.contains(*id)
                    && !done_this_walk.contains(*id)
                    && (full_walk || !kept_failing.contains(*id))
            })
            .cloned()
            .collect();
        for id in &unseen {
            if gets >= budget.max_gets {
                stop = Some(TickEnd::Paused { walk: walk.clone(), note: given_up_note(&given_up), page_progress });
                break;
            }
            // A full walk's one more try at a message given up on before.
            let retry = kept_failing.contains(id);
            pace_mailbox(mailbox_key, budget.get_interval).await;
            gets += 1;
            // `Some((why, ingest))`: this message failed; `ingest` = our own
            // ingest failed, rather than Gmail's get.
            let failure: Option<(String, bool)> = match get_everything_mode_message(http, access_token, id).await {
                Ok(EverythingGetOutcome::Item(item)) => {
                    attempted += 1;
                    match ingest_item(state, &tenant, &user_ctx, graph_id, *item, mailbox_key).await {
                        IngestResult::Stored => {
                            progressed = true;
                            page_progress = true;
                            skips.forget_given_up(id);
                            None
                        }
                        // A 422 comes from a validation rule we wrote, and a bad
                        // deploy can 422 wrongly: give up on it (bounded walks
                        // skip it) and let the next full walk retry it, never
                        // drop it for good (fix round 3, review of #1168).
                        IngestResult::Rejected => {
                            skips.add_given_up(id);
                            skips.add_walk(id);
                            page_progress = true;
                            None
                        }
                        IngestResult::Failed => Some(("ingest kept failing".to_string(), true)),
                    }
                }
                // Gone or hidden now. Hidden mail can be restored and rejoin
                // the listing, so only this walk skips it from here on.
                Ok(EverythingGetOutcome::Gone) => {
                    skips.forget_given_up(id);
                    skips.add_walk(id);
                    page_progress = true;
                    None
                }
                // Nothing ingestable, and the message never changes.
                Ok(EverythingGetOutcome::NoBody) => {
                    skips.add_unreadable(id);
                    page_progress = true;
                    None
                }
                // Permanent for this message: no walk will ever fetch it again.
                Ok(EverythingGetOutcome::Unreadable(detail)) => {
                    give_up(&mut given_up, id, &detail);
                    skips.add_unreadable(id);
                    page_progress = true;
                    None
                }
                Ok(EverythingGetOutcome::RateLimited(error)) => {
                    stop = Some(rate_limited_end(
                        walk.clone(),
                        error,
                        progressed,
                        page_progress,
                        &given_up,
                        account_id,
                        owner_user_id,
                    ));
                    break;
                }
                Err(e) => Some((e.to_string(), false)),
            };
            if let Some((why, ingest)) = failure {
                if retry {
                    // Still failing on its full-walk retry: it stays given up,
                    // and the walk moves on rather than stalling on it again.
                    // Remembered for the rest of this walk, so a paused tick
                    // does not spend its budget on it again.
                    skips.add_walk(id);
                    page_progress = true;
                    tracing::info!(
                        %account_id, %owner_user_id, source_id = %id, reason = %why,
                        "gmail_everything_mode: a given-up message still fails on its full-walk retry — still skipping it"
                    );
                } else if lenient_id.as_deref() == Some(id.as_str()) {
                    give_up(&mut given_up, id, &why);
                    skips.add_given_up(id);
                    skips.add_walk(id);
                    lenient_id = None;
                    page_progress = true;
                } else if ingest {
                    failed_ids.push(id.clone());
                } else {
                    // B1 (review of #1166): a 5xx, timeout, 401, or network
                    // error is not this message's fault and is usually
                    // transient. Never skip it: with the high-water mark, a
                    // skipped message would never be listed again. End the
                    // tick on this page; the next tick retries it (skipping
                    // what this page already stored).
                    tracing::warn!(
                        %account_id, %owner_user_id, error = %why, source_id = %id,
                        "gmail everything-mode: messages.get failed — ending the tick on this page, \
                         the next tick retries it"
                    );
                    stop = Some(TickEnd::Failed {
                        walk: walk.clone(),
                        error: with_note(why, &given_up),
                        page_progress,
                        failed_id: Some(id.clone()),
                    });
                    break;
                }
            }
            // A page can take minutes of paced gets: keep the claim alive
            // between pages too, and stop if another machine has taken it.
            if gets.is_multiple_of(budget.renew_claim_every_gets.max(1))
                && !renew_claim(pool, account_id, owner_user_id, claim_id).await
            {
                return Ok(true);
            }
        }
        // A page with an ingest failure is retried from the same page next
        // tick — never moved past (the train's `progress_cursor` invariant,
        // #673/#676). Its already-stored messages are skipped then.
        if let Some(error) = batch_ingest_outcome(attempted, &failed_ids) {
            break TickEnd::Failed {
                walk,
                error: with_note(error, &given_up),
                page_progress,
                failed_id: failed_ids.first().cloned(),
            };
        }
        if let Some(stop) = stop {
            break stop;
        }
        lenient_id = None;
        match next_page_token {
            None => break TickEnd::Completed { walk, note: given_up_note(&given_up) },
            Some(next) => {
                walk.cursor = Some(next);
                progressed = true;
                if !save_walk_position(pool, account_id, owner_user_id, claim_id, &walk, &skips).await {
                    return Ok(true);
                }
            }
        }
    };

    record_tick_end(pool, account_id, owner_user_id, claim_id, &end, &skips).await;
    Ok(true)
}

/// A rate limit ends the tick either way. After real progress it is routine
/// pacing during a backlog — a clean pause, logged, not an error on the row
/// (N1, review of #1166). With no progress at all, it is surfaced.
fn rate_limited_end(
    walk: WalkPos,
    error: String,
    progressed: bool,
    page_progress: bool,
    given_up: &[String],
    account_id: Uuid,
    owner_user_id: Uuid,
) -> TickEnd {
    tracing::info!(
        %account_id, %owner_user_id, progressed, google_error = %error,
        "gmail_everything_mode: Gmail rate limit — keeping progress, resuming next tick"
    );
    if progressed {
        TickEnd::Paused { walk, note: given_up_note(given_up), page_progress }
    } else {
        TickEnd::RateLimited { walk, error: with_note(error, given_up) }
    }
}

fn give_up(given_up: &mut Vec<String>, id: &str, why: &str) {
    tracing::warn!(
        source_id = %id, reason = %why,
        "gmail_everything_mode: giving up on a message that keeps failing — skipping it so the walk can move on"
    );
    let why: String = why.chars().take(120).collect();
    given_up.push(format!("{id} ({why})"));
}

fn given_up_note(given_up: &[String]) -> Option<String> {
    if given_up.is_empty() {
        return None;
    }
    Some(format!(
        "{GIVEN_UP_PREFIX}{} message(s) that kept failing: {}",
        given_up.len(),
        given_up.join("; ")
    ))
}

fn with_note(error: String, given_up: &[String]) -> String {
    match given_up_note(given_up) {
        Some(note) => format!("{error}; also {note}"),
        None => error,
    }
}

// ── Ticks ────────────────────────────────────────────────────────────────────

async fn tick_org(
    t: &WorkerTenant,
    control_pool: &PgPool,
    state: &Arc<AppState>,
    org_id: &str,
) -> Result<Tick, String> {
    let account_id = resolve_tenant_account(control_pool, org_id).await?;
    let owners = enabled_owners_for_account(&t.pool, account_id)
        .await
        .map_err(|e| e.to_string())?;
    if owners.is_empty() {
        return Ok(Tick::idle());
    }
    let mut processed = 0usize;
    for (owner_user_id, claim_id) in owners {
        let outcome = sync_owner(&t.pool, control_pool, state, account_id, owner_user_id, claim_id, Some(org_id)).await;
        match outcome {
            Ok(true) => processed += 1,
            Ok(false) => {}
            Err(e) => {
                tracing::warn!(
                    %account_id, %owner_user_id, error = %e,
                    "gmail_everything_mode: org member sync failed"
                );
                mark_failed_no_progress(&t.pool, account_id, owner_user_id, claim_id, &e.to_string()).await;
            }
        }
    }
    Ok(if processed > 0 { Tick::worked(processed) } else { Tick::idle() })
}

async fn tick_consumer(t: &WorkerTenant, control_pool: &PgPool, state: &Arc<AppState>) -> Result<Tick, String> {
    let due = due_accounts(control_pool).await.map_err(|e| e.to_string())?;
    if due.is_empty() {
        return Ok(Tick::idle());
    }
    let mut processed = 0usize;
    for (account_id, owner_user_id, claim_id) in due {
        let outcome = sync_owner(&t.pool, control_pool, state, account_id, owner_user_id, claim_id, None).await;
        match outcome {
            Ok(true) => processed += 1,
            Ok(false) => {}
            Err(e) => {
                tracing::warn!(
                    %account_id, %owner_user_id, error = %e,
                    "gmail_everything_mode: personal-plane sync failed"
                );
                mark_failed_no_progress(&t.pool, account_id, owner_user_id, claim_id, &e.to_string()).await;
            }
        }
    }
    Ok(if processed > 0 { Tick::worked(processed) } else { Tick::idle() })
}

/// One everything-mode sync pass. Driven by `workers::tenant_harness::
/// run_multi_tenant` as a sweep worker (`HarnessConfig::sweep_worker(_,
/// POLL_INTERVAL)`) — both arms do real work, same as `workers::
/// outlook_everything_mode::tick` (contrast `microsoft_calendar_sync::
/// tick`'s consumer no-op).
pub async fn tick(t: &WorkerTenant, control_pool: &PgPool, state: &Arc<AppState>) -> Result<Tick, String> {
    match &t.tenant {
        TenantId::Consumer => tick_consumer(t, control_pool, state).await,
        TenantId::Org(org_id) => tick_org(t, control_pool, state, org_id).await,
    }
}

/// pg-gated integration tests: proves discovery + progress bookkeeping
/// against a REAL Postgres database, split control-plane (bootstrap
/// discovery) / tenant-plane (per-owner state + progress bookkeeping) —
/// same split `workers::outlook_everything_mode::
/// outlook_everything_mode_pg_tests` uses. The "end-to-end walk" group also
/// runs the walk itself, `sync_owner_with_token`, end to end: a local mock of
/// `users.messages.list`/`users.messages.get` (the `google_gmail::
/// TEST_MESSAGES_URL` seam) and the real `ingest_atom` into a personal-plane
/// ocean. Only `sync_owner`'s credential load and token refresh are not
/// exercised; those need a live Google credential.
///
///   ORGT_TEST_BASE_URL=postgres://postgres:postgres@localhost:15544/postgres \
///     cargo test -p coralswarm-backend --lib "workers::gmail_everything_mode::gmail_everything_mode_pg_tests" \
///     -- --test-threads=1
#[cfg(test)]
mod gmail_everything_mode_pg_tests {
    use super::*;
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
    async fn teardown(base: &str, db: &str, pool: PgPool) {
        pool.close().await;
        let mut admin_conn = PgConnection::connect(base).await.unwrap();
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }

    // ── Control-plane group: cross-account bootstrap discovery ─────────────

    async fn control_pool(base: &str, db: &str) -> PgPool {
        let pool = PgPoolOptions::new().connect(&with_db(base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();
        pool
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
    async fn seed_graph(pool: &PgPool, account_id: Uuid, owner_user_id: Uuid) -> Uuid {
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
    async fn seed_everything_mode(pool: &PgPool, account_id: Uuid, owner: Uuid, graph_id: Uuid, enabled: bool) {
        sqlx::query(
            "INSERT INTO gmail_everything_mode(account_id, owner_user_id, graph_id, enabled) \
             VALUES ($1, $2, $3, $4)",
        )
        .bind(account_id)
        .bind(owner)
        .bind(graph_id)
        .bind(enabled)
        .execute(pool)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn due_accounts_finds_only_enabled_rows_across_accounts_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping due_accounts_finds_only_enabled_rows_across_accounts_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "g24_due_accounts";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool = control_pool(&base, db).await;

        let founder_a = seed_user(&pool).await;
        let account_a = seed_account(&pool, founder_a).await;
        let owner_enabled = seed_user(&pool).await;
        let owner_disabled = seed_user(&pool).await;
        let graph_enabled = seed_graph(&pool, account_a, owner_enabled).await;
        let graph_disabled = seed_graph(&pool, account_a, owner_disabled).await;
        seed_everything_mode(&pool, account_a, owner_enabled, graph_enabled, true).await;
        seed_everything_mode(&pool, account_a, owner_disabled, graph_disabled, false).await;

        let founder_b = seed_user(&pool).await;
        let account_b = seed_account(&pool, founder_b).await;
        let owner_b = seed_user(&pool).await;
        let graph_b = seed_graph(&pool, account_b, owner_b).await;
        seed_everything_mode(&pool, account_b, owner_b, graph_b, true).await;

        let due = due_accounts(&pool).await.unwrap();
        let mut due_pairs: Vec<(Uuid, Uuid)> = due.iter().map(|(a, o, _c)| (*a, *o)).collect();
        due_pairs.sort();
        let mut expected = vec![(account_a, owner_enabled), (account_b, owner_b)];
        expected.sort();
        assert_eq!(
            due_pairs, expected,
            "only ENABLED rows across every account must be discovered — a disabled row, and a \
             fresh connection with no row at all, must never appear"
        );
        assert!(
            due.iter().all(|(_, _, claim_id)| *claim_id != Uuid::nil()),
            "the claiming UPDATE must stamp a real (non-nil) fencing token on every claimed row"
        );

        teardown(&base, db, pool).await;
    }

    #[tokio::test]
    async fn due_accounts_claims_rows_so_a_second_call_excludes_them_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping due_accounts_claims_rows_so_a_second_call_excludes_them_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "g24_due_accounts_claim";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool = control_pool(&base, db).await;

        let founder = seed_user(&pool).await;
        let account_id = seed_account(&pool, founder).await;
        let owner = seed_user(&pool).await;
        let graph_id = seed_graph(&pool, account_id, owner).await;
        seed_everything_mode(&pool, account_id, owner, graph_id, true).await;

        let first = due_accounts(&pool).await.unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!((first[0].0, first[0].1), (account_id, owner));
        assert_ne!(first[0].2, Uuid::nil(), "the claim must stamp a real fencing token");

        let second = due_accounts(&pool).await.unwrap();
        assert!(
            second.is_empty(),
            "a row claimed by the first call must not be returned again by a second call inside the claim window"
        );

        teardown(&base, db, pool).await;
    }

    #[tokio::test]
    async fn due_accounts_empty_when_nothing_is_enabled_anywhere_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping due_accounts_empty_when_nothing_is_enabled_anywhere_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "g24_due_accounts_empty";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool = control_pool(&base, db).await;

        let founder = seed_user(&pool).await;
        let account_id = seed_account(&pool, founder).await;
        let owner = seed_user(&pool).await;
        let graph_id = seed_graph(&pool, account_id, owner).await;
        seed_everything_mode(&pool, account_id, owner, graph_id, false).await;

        let due = due_accounts(&pool).await.unwrap();
        assert!(due.is_empty(), "a disabled-only universe must report nothing due");

        teardown(&base, db, pool).await;
    }

    // ── Tenant-plane group: per-owner state + progress bookkeeping ──────────

    async fn tenant_pool(base: &str, db: &str, org_id: &str) -> PgPool {
        let pool = PgPoolOptions::new().connect(&with_db(base, db)).await.unwrap();
        sqlx::migrate!("./migrations_tenant").run(&pool).await.unwrap();
        sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, $1)")
            .bind(org_id)
            .execute(&pool)
            .await
            .unwrap();
        pool
    }

    /// A member who paused (or turned off) the Gmail source: the whole-mailbox
    /// walk does nothing for them. Unpaused, the same call gets as far as the
    /// credentials, which a test has none of.
    #[tokio::test]
    async fn a_paused_gmail_source_is_not_walked_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping a_paused_gmail_source_is_not_walked_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "g24_paused_source";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool = control_pool(&base, db).await;
        let owner = seed_user(&pool).await;
        let account_id = seed_account(&pool, owner).await;
        let graph_id = seed_graph(&pool, account_id, owner).await;
        seed_everything_mode(&pool, account_id, owner, graph_id, true).await;
        let state = Arc::new(crate::test_app_state_owned_with_db(pool.clone(), None));

        let unpaused = sync_owner(&pool, &pool, &state, account_id, owner, Uuid::new_v4(), None).await;
        assert!(unpaused.is_err(), "an unpaused member gets past the gate, to the missing credentials: {unpaused:?}");

        sqlx::query(
            "INSERT INTO connector_sources (account_id, owner_user_id, provider, source, enabled, paused_at) \
             VALUES ($1, $2, 'google', 'gmail', true, now())",
        )
        .bind(account_id)
        .bind(owner)
        .execute(&pool)
        .await
        .unwrap();
        let paused = sync_owner(&pool, &pool, &state, account_id, owner, Uuid::new_v4(), None).await;
        assert!(matches!(paused, Ok(false)), "{paused:?}");

        teardown(&base, db, pool).await;
    }

    #[tokio::test]
    async fn owner_state_is_none_for_a_fresh_mailbox_and_some_once_enabled_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping owner_state_is_none_for_a_fresh_mailbox_and_some_once_enabled_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "g24_owner_state";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool = tenant_pool(&base, db, "org_owner_state").await;

        let account_id = Uuid::new_v4();
        let owner = Uuid::new_v4();
        let graph_id = seed_graph(&pool, account_id, owner).await;

        assert!(owner_state(&pool, account_id, owner).await.unwrap().is_none());

        seed_everything_mode(&pool, account_id, owner, graph_id, true).await;
        let state = owner_state(&pool, account_id, owner).await.unwrap();
        assert_eq!(
            state,
            Some(OwnerState {
                graph_id,
                walk: WalkPos::default(),
                high_water_at: None,
                last_full_walk_at: None,
                page_failures: 0,
                page_failing_id: None,
                skips: SkipLists::default(),
            })
        );

        sqlx::query("UPDATE gmail_everything_mode SET enabled = false WHERE account_id = $1 AND owner_user_id = $2")
            .bind(account_id)
            .bind(owner)
            .execute(&pool)
            .await
            .unwrap();
        assert!(owner_state(&pool, account_id, owner).await.unwrap().is_none());

        teardown(&base, db, pool).await;
    }

    #[tokio::test]
    async fn record_progress_persists_cursor_on_incomplete_page_and_clears_it_on_complete_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping record_progress_persists_cursor_on_incomplete_page_and_clears_it_on_complete_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "g24_record_progress";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool = tenant_pool(&base, db, "org_record_progress").await;

        let account_id = Uuid::new_v4();
        let owner = Uuid::new_v4();
        let graph_id = seed_graph(&pool, account_id, owner).await;
        seed_everything_mode(&pool, account_id, owner, graph_id, true).await;

        let claimed = enabled_owners_for_account(&pool, account_id).await.unwrap();
        assert_eq!(claimed.len(), 1);
        let claim_id = claimed[0].1;

        let started = Utc::now() - chrono::Duration::minutes(5);
        let walk = WalkPos { cursor: Some("page-2".into()), started_at: Some(started), after: None };
        let paused = TickEnd::Paused { walk: walk.clone(), note: None, page_progress: false };
        record_tick_end(&pool, account_id, owner, claim_id, &paused, &SkipLists::default()).await;
        let state = owner_state(&pool, account_id, owner).await.unwrap().unwrap();
        assert_eq!(state.walk.cursor.as_deref(), Some("page-2"));
        assert_eq!(state.walk.started_at.map(|t| t.timestamp_micros()), Some(started.timestamp_micros()));
        assert_eq!(state.high_water_at, None);

        // Re-claim (the previous claim was released by the success branch
        // above) before the next `record_tick_end` call, so its fence still
        // matches.
        let claimed2 = enabled_owners_for_account(&pool, account_id).await.unwrap();
        assert_eq!(claimed2.len(), 1);
        let claim_id2 = claimed2[0].1;

        record_tick_end(&pool, account_id, owner, claim_id2, &TickEnd::Completed { walk, note: None }, &SkipLists::default()).await;
        let state = owner_state(&pool, account_id, owner).await.unwrap().unwrap();
        assert_eq!(state.walk, WalkPos::default(), "a completed walk clears its position");
        assert_eq!(
            state.high_water_at.map(|t| t.timestamp_micros()),
            Some(started.timestamp_micros()),
            "a completed walk's start becomes the next walk's high-water mark"
        );
        assert_eq!(
            state.last_full_walk_at.map(|t| t.timestamp_micros()),
            Some(started.timestamp_micros()),
            "a completed FULL walk (no after: floor) also records the full-walk time"
        );

        teardown(&base, db, pool).await;
    }

    #[tokio::test]
    async fn record_progress_with_a_stale_claim_id_is_dropped_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping record_progress_with_a_stale_claim_id_is_dropped_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "g24_stale_claim";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool = tenant_pool(&base, db, "org_stale_claim").await;

        let account_id = Uuid::new_v4();
        let owner = Uuid::new_v4();
        let graph_id = seed_graph(&pool, account_id, owner).await;
        seed_everything_mode(&pool, account_id, owner, graph_id, true).await;

        let claimed = enabled_owners_for_account(&pool, account_id).await.unwrap();
        let real_claim_id = claimed[0].1;
        let stale_claim_id = Uuid::new_v4();

        // A stale (never-really-issued) claim_id must not clobber state —
        // proves the fencing predicate rejects it (zero rows affected,
        // logged, not treated as success).
        let at = |c: &str| WalkPos { cursor: Some(c.into()), ..WalkPos::default() };
        let stale = TickEnd::Paused { walk: at("stale-page"), note: None, page_progress: false };
        record_tick_end(&pool, account_id, owner, stale_claim_id, &stale, &SkipLists::default()).await;
        assert!(
            !save_walk_position(&pool, account_id, owner, stale_claim_id, &at("stale-mid-walk"), &SkipLists::default()).await,
            "a mid-walk save under a stale claim_id reports the lost claim"
        );
        assert!(
            !renew_claim(&pool, account_id, owner, stale_claim_id).await,
            "a stale claim_id cannot renew"
        );
        let state = owner_state(&pool, account_id, owner).await.unwrap().unwrap();
        assert_eq!(state.graph_id, graph_id);
        assert_eq!(state.walk.cursor, None, "a stale claim_id must not persist its cursor");

        let real = TickEnd::Paused { walk: at("real-page"), note: None, page_progress: false };
        record_tick_end(&pool, account_id, owner, real_claim_id, &real, &SkipLists::default()).await;
        let state = owner_state(&pool, account_id, owner).await.unwrap().unwrap();
        assert_eq!(
            state.walk.cursor.as_deref(),
            Some("real-page"),
            "the real claim_id must still be able to persist"
        );

        teardown(&base, db, pool).await;
    }

    #[tokio::test]
    async fn mark_failed_no_progress_backs_off_the_claim_without_touching_last_synced_at_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping mark_failed_no_progress_backs_off_the_claim_without_touching_last_synced_at_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "g24_mark_failed";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool = tenant_pool(&base, db, "org_mark_failed").await;

        let account_id = Uuid::new_v4();
        let owner = Uuid::new_v4();
        let graph_id = seed_graph(&pool, account_id, owner).await;
        seed_everything_mode(&pool, account_id, owner, graph_id, true).await;

        let claimed = enabled_owners_for_account(&pool, account_id).await.unwrap();
        let claim_id = claimed[0].1;

        mark_failed_no_progress(&pool, account_id, owner, claim_id, "boom").await;

        // Backed off (30-minute claim), not released — a second discovery
        // call inside that window must find nothing claimable.
        let redue = enabled_owners_for_account(&pool, account_id).await.unwrap();
        assert!(
            redue.is_empty(),
            "a failed owner must be backed off, not immediately reclaimable"
        );

        let (last_error,): (Option<String>,) =
            sqlx::query_as("SELECT last_error FROM gmail_everything_mode WHERE account_id = $1 AND owner_user_id = $2")
                .bind(account_id)
                .bind(owner)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(last_error.as_deref(), Some("boom"));

        teardown(&base, db, pool).await;
    }

    /// F2 (adversarial review, 2026-08-25), REPRODUCED before this fix: a
    /// handful of permanently-failing owners must not permanently occupy
    /// every slot in [`OWNERS_PER_TICK`] and starve a healthy owner out.
    /// Seeds ONE more owner than the budget (6, budget 5), claims round 1
    /// (5 of the 6 — which 5 is nondeterministic, all tie on `NULL
    /// last_attempted_at`), fails all 5 via [`mark_failed_no_progress`]
    /// (which now advances `last_attempted_at`), force-expires their
    /// backoff (simulating 30 minutes passing), then proves round 2 still
    /// includes the 6th (never-attempted, still `NULL`) owner — its `NULL`
    /// sort key beats the 5 owners' now-recent `last_attempted_at`
    /// unconditionally. Before this fix (ordering by `last_synced_at`,
    /// which NEITHER failure path ever advanced), all 6 owners tied on
    /// `NULL` forever, and the 6th could be excluded from every single round
    /// indefinitely — exactly the starvation the review reproduced with a
    /// simulated 10-tick run.
    #[tokio::test]
    async fn enabled_owners_for_account_does_not_starve_a_never_attempted_owner_behind_recently_failed_ones_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping enabled_owners_for_account_does_not_starve_a_never_attempted_owner_behind_recently_failed_ones_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "g24_no_starvation";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool = tenant_pool(&base, db, "org_no_starvation").await;

        let account_id = Uuid::new_v4();
        let mut owners = Vec::new();
        for _ in 0..(OWNERS_PER_TICK as usize + 1) {
            let owner = Uuid::new_v4();
            let graph_id = seed_graph(&pool, account_id, owner).await;
            seed_everything_mode(&pool, account_id, owner, graph_id, true).await;
            owners.push(owner);
        }

        // Round 1: claims exactly OWNERS_PER_TICK of the 6.
        let round1 = enabled_owners_for_account(&pool, account_id).await.unwrap();
        assert_eq!(round1.len(), OWNERS_PER_TICK as usize);
        let claimed_owners: std::collections::HashSet<Uuid> = round1.iter().map(|(o, _)| *o).collect();
        let never_claimed_owner = *owners.iter().find(|o| !claimed_owners.contains(o)).unwrap();

        // Fail every round-1 owner — advances their last_attempted_at.
        for (owner, claim_id) in &round1 {
            mark_failed_no_progress(&pool, account_id, *owner, *claim_id, "simulated permanent failure").await;
        }

        // Simulate the 30-minute backoff having already elapsed for all of
        // them (real time is too slow for a test).
        sqlx::query("UPDATE gmail_everything_mode SET claimed_until = now() - interval '1 minute' WHERE account_id = $1")
            .bind(account_id)
            .execute(&pool)
            .await
            .unwrap();

        // Round 2: the never-claimed owner (still NULL last_attempted_at)
        // must be included — its sort key beats the 5 recently-failed
        // owners' now-recent last_attempted_at.
        let round2 = enabled_owners_for_account(&pool, account_id).await.unwrap();
        let round2_owners: std::collections::HashSet<Uuid> = round2.iter().map(|(o, _)| *o).collect();
        assert!(
            round2_owners.contains(&never_claimed_owner),
            "a never-attempted owner must not be starved behind 5 recently-failed ones — got {round2_owners:?}, \
             expected to contain {never_claimed_owner}"
        );

        teardown(&base, db, pool).await;
    }

    /// Set only on this test's ephemeral scratch database's cluster.
    /// `coralswarm_tenant_app` is created with no password by migration
    /// `0009` §8; not a real credential, never used against any deploy —
    /// same posture and same const name as `backend/tests/
    /// tenant_rls_pg.rs`'s own (duplicated here rather than shared, since
    /// there is no crate-level tenant-plane equivalent to `crate::
    /// app_role_test_support`, which covers the CONTROL-plane
    /// `coralswarm_app` role only).
    const TENANT_APP_ROLE_TEST_PASSWORD: &str = "g24_tenant_rls_test_only_pw";

    /// `ALTER ROLE coralswarm_tenant_app WITH PASSWORD ...`, retried against
    /// the SAME cluster-wide `pg_authid` race `tenant_rls_pg.rs`'s own
    /// helper documents and retries.
    async fn set_tenant_app_role_password(owner: &PgPool) {
        let stmt = format!("ALTER ROLE coralswarm_tenant_app WITH PASSWORD '{TENANT_APP_ROLE_TEST_PASSWORD}'");
        let mut last: Option<sqlx::Error> = None;
        for attempt in 0..10u32 {
            match sqlx::query(&stmt).execute(owner).await {
                Ok(_) => return,
                Err(e) => {
                    last = Some(e);
                    let base = 50 * u64::from(attempt + 1);
                    let jitter = u64::from(attempt * 17 % 40);
                    tokio::time::sleep(std::time::Duration::from_millis(base + jitter)).await;
                }
            }
        }
        panic!("set test-only password on coralswarm_tenant_app after 10 attempts: {last:?}");
    }

    /// A pool connected AS `coralswarm_tenant_app` — the real, NOBYPASSRLS
    /// tenant-plane production role.
    async fn tenant_app_pool(owner: &PgPool, db_url: &str) -> PgPool {
        set_tenant_app_role_password(owner).await;
        let opts = <sqlx::postgres::PgConnectOptions as std::str::FromStr>::from_str(db_url)
            .expect("parse tenant scratch db url")
            .username("coralswarm_tenant_app")
            .password(TENANT_APP_ROLE_TEST_PASSWORD);
        sqlx::postgres::PgPoolOptions::new()
            .max_connections(2)
            .connect_with(opts)
            .await
            .expect("connect AS coralswarm_tenant_app")
    }

    /// R4 (adversarial review, 2026-08-25), REPRODUCED before this fix,
    /// against the REAL NOBYPASSRLS `coralswarm_tenant_app` role: an
    /// owner's per-tick claim UPDATE must succeed even after their member
    /// ocean has been archived. Migration `0074`'s `WITH CHECK` used to
    /// re-validate `graphs.archived_at IS NULL` on EVERY write, including
    /// this claim UPDATE (which never touches `graph_id`) — so ONE archived
    /// member ocean made `enabled_owners_for_account`'s multi-row claim fail
    /// for the WHOLE batch, which on the tenant tree means the ENTIRE org's
    /// tick (`tick_org` returns `Err`) — reproduced against real Postgres
    /// before this fix (`archived_at IS NULL` dropped from the policy).
    #[tokio::test]
    async fn enabled_owners_for_account_claims_the_row_even_after_the_owners_graph_is_archived_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping enabled_owners_for_account_claims_the_row_even_after_the_owners_graph_is_archived_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "g24_claim_after_archive";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let owner_pool = tenant_pool(&base, db, "org_claim_after_archive").await;
        let app = tenant_app_pool(&owner_pool, &with_db(&base, db)).await;

        let account_id = Uuid::new_v4();
        let user_id = Uuid::new_v4();
        let graph_id = seed_graph(&owner_pool, account_id, user_id).await;
        seed_everything_mode(&owner_pool, account_id, user_id, graph_id, true).await;

        sqlx::query("UPDATE graphs SET archived_at = now() WHERE id = $1")
            .bind(graph_id)
            .execute(&owner_pool)
            .await
            .unwrap();

        // The claim UPDATE runs against the REAL app role — under the
        // pre-fix policy this returns Err (RLS violation) for the whole
        // batch. `tenant_rls_visible_graph_ids()` (the USING side) is
        // deliberately not part of this claim UPDATE's own scoping — it
        // runs inside `begin_tenant_system_scoped`, the reviewed
        // `app.tenant_actor='system'` bypass every worker uses — so this
        // test exercises WITH CHECK specifically, the half that bypass does
        // NOT skip.
        let claimed = enabled_owners_for_account(&app, account_id)
            .await
            .expect("the claim UPDATE must succeed even though the owner's graph is now archived");
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].0, user_id);

        app.close().await;
        teardown(&base, db, owner_pool).await;
    }

    // ── End-to-end walk group: a mock Gmail + real ingest (personal plane) ──
    //
    // The 2026-09-24 prod incident: after the backfill completed, every tick
    // re-walked the mailbox from the newest message and `messages.get`-ed
    // every id again, blew Gmail's per-user "Total Query Cost" quota partway
    // through, kept none of the progress, and (because a 403 sent WITH a
    // pageToken was read as "stale token") restarted from scratch. These
    // tests drive `sync_owner_with_token` against a local mock of
    // `users.messages.list`/`users.messages.get` (the
    // `google_gmail::TEST_MESSAGES_URL` seam) and the real `ingest_atom`.

    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    use axum::extract::{Path as AxPath, Query as AxQuery, State as AxState};
    use axum::response::IntoResponse;
    use base64::Engine as _;
    use chrono::Utc;
    use coralswarm_core::kek::{generate_dek, load_kek, wrap_dek};

    /// A mock mailbox. `messages` is newest-first, the order `users.messages.
    /// list` returns. `rate_limit_after_gets = Some(n)`: once `n` gets have
    /// succeeded in the current window, EVERY request (list and get) answers
    /// Gmail's real per-user quota 403 until the test opens a new window.
    struct MockGmail {
        messages: Mutex<Vec<(String, i64)>>,
        page_size: usize,
        stale_status: u16,
        rate_limit_after_gets: Mutex<Option<usize>>,
        gets_in_window: AtomicUsize,
        list_calls: AtomicUsize,
        get_calls: AtomicUsize,
        list_page_tokens: Mutex<Vec<Option<String>>>,
        /// id -> how many more `messages.get` calls answer a Google `500`
        /// (`usize::MAX` = always).
        fail_gets: Mutex<std::collections::HashMap<String, usize>>,
        /// When set, every list call records the row's `claimed_until` as
        /// Postgres sees it at that moment.
        claim_probe: Mutex<Option<(PgPool, Uuid, Uuid)>>,
        claimed_until_seen: Mutex<Vec<Option<chrono::DateTime<Utc>>>>,
        /// When set, every get records the row's `claimed_until` (into
        /// `claimed_until_at_gets`), and the hooks below may act on the row.
        get_probe: Mutex<Option<(PgPool, Uuid, Uuid)>>,
        claimed_until_at_gets: Mutex<Vec<Option<chrono::DateTime<Utc>>>>,
        /// On this get (1-based), cut the claim to a few seconds left — a
        /// long page eating into the claim.
        shorten_claim_at_get: Mutex<Option<usize>>,
        /// On this get (1-based), another machine takes the claim.
        steal_claim_at_get: Mutex<Option<usize>>,
        /// Ids whose `messages.get` body never parses (`Unreadable`).
        unreadable: Mutex<HashSet<String>>,
        /// Ids the get finds in Trash (hidden between the list and the get).
        hidden: Mutex<HashSet<String>>,
        /// Ids whose body is empty: nothing to ingest.
        no_body: Mutex<HashSet<String>>,
        /// From this list call on (1-based), list calls answer a Google 500.
        fail_lists_from: Mutex<Option<usize>>,
    }

    impl MockGmail {
        fn new(page_size: usize) -> Arc<Self> {
            Arc::new(Self {
                messages: Mutex::new(Vec::new()),
                page_size,
                stale_status: 400,
                rate_limit_after_gets: Mutex::new(None),
                gets_in_window: AtomicUsize::new(0),
                list_calls: AtomicUsize::new(0),
                get_calls: AtomicUsize::new(0),
                list_page_tokens: Mutex::new(Vec::new()),
                fail_gets: Mutex::new(std::collections::HashMap::new()),
                claim_probe: Mutex::new(None),
                claimed_until_seen: Mutex::new(Vec::new()),
                get_probe: Mutex::new(None),
                claimed_until_at_gets: Mutex::new(Vec::new()),
                shorten_claim_at_get: Mutex::new(None),
                steal_claim_at_get: Mutex::new(None),
                unreadable: Mutex::new(HashSet::new()),
                hidden: Mutex::new(HashSet::new()),
                no_body: Mutex::new(HashSet::new()),
                fail_lists_from: Mutex::new(None),
            })
        }
        /// Add one message dated `ago` before now, in date order (newest
        /// first) — e.g. mail that joins the listing with an old date.
        fn add_dated(&self, id: &str, ago: chrono::Duration) {
            let ms = (Utc::now() - ago).timestamp_millis();
            let mut msgs = self.messages.lock().unwrap();
            msgs.push((id.to_string(), ms));
            msgs.sort_by_key(|m| std::cmp::Reverse(m.1));
        }
        fn with_stale_status(page_size: usize, stale_status: u16) -> Arc<Self> {
            let mut m = Arc::try_unwrap(Self::new(page_size)).ok().unwrap();
            m.stale_status = stale_status;
            Arc::new(m)
        }
        /// Push `n` messages, each newer than everything already present.
        /// Ids are `{prefix}{i}`; internal dates step a minute apart, ending
        /// `ends_ago` before now.
        fn add_newest(&self, prefix: &str, n: usize, ends_ago: chrono::Duration) {
            let end = (Utc::now() - ends_ago).timestamp_millis();
            let mut msgs = self.messages.lock().unwrap();
            for i in 0..n {
                let ms = end - ((n - 1 - i) as i64) * 60_000;
                msgs.insert(0, (format!("{prefix}{i}"), ms));
            }
        }
        fn reset_counters(&self) {
            self.list_calls.store(0, Ordering::SeqCst);
            self.get_calls.store(0, Ordering::SeqCst);
            self.gets_in_window.store(0, Ordering::SeqCst);
            self.list_page_tokens.lock().unwrap().clear();
        }
        fn rate_limited(&self) -> bool {
            matches!(*self.rate_limit_after_gets.lock().unwrap(), Some(n) if self.gets_in_window.load(Ordering::SeqCst) >= n)
        }
    }

    fn quota_403() -> axum::response::Response {
        let body = serde_json::json!({"error": {
            "code": 403,
            "message": "Quota exceeded for quota metric 'Total Query Cost' and limit 'Units per minute per user' of service 'gmail.googleapis.com' for consumer 'project_number:1'.",
            "errors": [{"message": "Quota exceeded", "domain": "usageLimits", "reason": "rateLimitExceeded"}],
            "status": "PERMISSION_DENIED",
            "details": [{"@type": "type.googleapis.com/google.rpc.ErrorInfo", "reason": "RATE_LIMIT_EXCEEDED"}]
        }});
        (axum::http::StatusCode::FORBIDDEN, axum::Json(body)).into_response()
    }

    async fn spawn_mock_gmail(mock: Arc<MockGmail>) -> String {
        async fn list(
            AxState(m): AxState<Arc<MockGmail>>,
            AxQuery(q): AxQuery<std::collections::HashMap<String, String>>,
        ) -> axum::response::Response {
            let nth_list = m.list_calls.fetch_add(1, Ordering::SeqCst) + 1;
            m.list_page_tokens.lock().unwrap().push(q.get("pageToken").cloned());
            if matches!(*m.fail_lists_from.lock().unwrap(), Some(from) if nth_list >= from) {
                let body = serde_json::json!({"error": {
                    "code": 500, "message": "Internal error encountered.",
                    "errors": [{"reason": "backendError", "message": "Internal error encountered."}],
                    "status": "INTERNAL"
                }});
                return (axum::http::StatusCode::INTERNAL_SERVER_ERROR, axum::Json(body)).into_response();
            }
            let probe = m.claim_probe.lock().unwrap().clone();
            if let Some((pool, account_id, owner)) = probe {
                let seen: Option<chrono::DateTime<Utc>> = sqlx::query_scalar(
                    "SELECT claimed_until FROM gmail_everything_mode WHERE account_id = $1 AND owner_user_id = $2",
                )
                .bind(account_id)
                .bind(owner)
                .fetch_one(&pool)
                .await
                .unwrap();
                m.claimed_until_seen.lock().unwrap().push(seen);
            }
            if m.rate_limited() {
                return quota_403();
            }
            let after_secs: Option<i64> = q
                .get("q")
                .and_then(|q| q.strip_prefix("after:"))
                .map(|s| s.trim().parse().expect("after: takes epoch seconds"));
            let offset = match q.get("pageToken") {
                None => 0usize,
                Some(t) => match t.strip_prefix("tok-").and_then(|n| n.parse().ok()) {
                    Some(n) => n,
                    None => {
                        let status = axum::http::StatusCode::from_u16(m.stale_status).unwrap();
                        let body = serde_json::json!({"error": {
                            "code": m.stale_status, "message": "Invalid pageToken",
                            "errors": [{"reason": "invalid", "message": "Invalid pageToken"}],
                            "status": "INVALID_ARGUMENT"
                        }});
                        return (status, axum::Json(body)).into_response();
                    }
                },
            };
            let page = q
                .get("maxResults")
                .and_then(|s| s.parse::<usize>().ok())
                .unwrap_or(100)
                .min(m.page_size);
            let msgs: Vec<(String, i64)> = m
                .messages
                .lock()
                .unwrap()
                .iter()
                .filter(|(_, ms)| after_secs.is_none_or(|a| ms / 1000 > a))
                .cloned()
                .collect();
            let slice: Vec<serde_json::Value> = msgs
                .iter()
                .skip(offset)
                .take(page)
                .map(|(id, _)| serde_json::json!({"id": id, "threadId": format!("t-{id}")}))
                .collect();
            let mut body = serde_json::json!({"messages": slice, "resultSizeEstimate": msgs.len()});
            if offset + page < msgs.len() {
                body["nextPageToken"] = serde_json::json!(format!("tok-{}", offset + page));
            }
            axum::Json(body).into_response()
        }
        async fn get_one(AxState(m): AxState<Arc<MockGmail>>, AxPath(id): AxPath<String>) -> axum::response::Response {
            let nth = m.get_calls.fetch_add(1, Ordering::SeqCst) + 1;
            let probe = m.get_probe.lock().unwrap().clone();
            if let Some((pool, account_id, owner)) = probe {
                let shorten = *m.shorten_claim_at_get.lock().unwrap() == Some(nth);
                let steal = *m.steal_claim_at_get.lock().unwrap() == Some(nth);
                if shorten {
                    sqlx::query(
                        "UPDATE gmail_everything_mode SET claimed_until = now() + interval '3 seconds' \
                         WHERE account_id = $1 AND owner_user_id = $2",
                    )
                    .bind(account_id)
                    .bind(owner)
                    .execute(&pool)
                    .await
                    .unwrap();
                }
                if steal {
                    sqlx::query(
                        "UPDATE gmail_everything_mode SET claim_id = gen_random_uuid(), \
                         claimed_until = now() + interval '15 minutes' \
                         WHERE account_id = $1 AND owner_user_id = $2",
                    )
                    .bind(account_id)
                    .bind(owner)
                    .execute(&pool)
                    .await
                    .unwrap();
                }
                let seen: Option<chrono::DateTime<Utc>> = sqlx::query_scalar(
                    "SELECT claimed_until FROM gmail_everything_mode WHERE account_id = $1 AND owner_user_id = $2",
                )
                .bind(account_id)
                .bind(owner)
                .fetch_one(&pool)
                .await
                .unwrap();
                m.claimed_until_at_gets.lock().unwrap().push(seen);
            }
            if m.rate_limited() {
                return quota_403();
            }
            m.gets_in_window.fetch_add(1, Ordering::SeqCst);
            {
                let mut fails = m.fail_gets.lock().unwrap();
                if let Some(left) = fails.get_mut(&id) {
                    if *left > 0 {
                        if *left != usize::MAX {
                            *left -= 1;
                        }
                        let body = serde_json::json!({"error": {
                            "code": 500, "message": "Internal error encountered.",
                            "errors": [{"reason": "backendError", "message": "Internal error encountered."}],
                            "status": "INTERNAL"
                        }});
                        return (axum::http::StatusCode::INTERNAL_SERVER_ERROR, axum::Json(body)).into_response();
                    }
                }
            }
            if m.unreadable.lock().unwrap().contains(&id) {
                return (axum::http::StatusCode::OK, "this is not a Gmail message").into_response();
            }
            let found = m.messages.lock().unwrap().iter().find(|(i, _)| *i == id).cloned();
            let Some((id, ms)) = found else {
                return axum::http::StatusCode::NOT_FOUND.into_response();
            };
            let text = if m.no_body.lock().unwrap().contains(&id) {
                String::new()
            } else {
                format!("Body of message {id}: the quarterly plan, revision notes.")
            };
            let data = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(text);
            let label = if m.hidden.lock().unwrap().contains(&id) { "TRASH" } else { "INBOX" };
            axum::Json(serde_json::json!({
                "id": id,
                "threadId": format!("t-{id}"),
                "internalDate": ms.to_string(),
                "labelIds": [label],
                "payload": {
                    "mimeType": "text/plain",
                    "headers": [
                        {"name": "Subject", "value": format!("Subject {id}")},
                        {"name": "From", "value": "jane@example.com"},
                        {"name": "To", "value": "owner@example.com"}
                    ],
                    "body": {"data": data}
                }
            }))
            .into_response()
        }
        let app = axum::Router::new()
            .route("/messages", axum::routing::get(list))
            .route("/messages/{id}", axum::routing::get(get_one))
            .with_state(mock);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });
        format!("http://{addr}/messages")
    }

    fn set_test_kek() {
        // SAFETY: every test in this process sets the SAME constant.
        unsafe { std::env::set_var("MASTER_KEK_HEX", "cc".repeat(32)) };
    }

    struct PersonalMailbox {
        pool: PgPool,
        state: Arc<AppState>,
        account_id: Uuid,
        owner: Uuid,
        graph_id: Uuid,
        budget: Mutex<WalkBudget>,
        /// Source ids `ingest_item` answers as a `422` ([`TEST_INGEST_REJECTS`]).
        rejects: Mutex<HashSet<String>>,
    }

    /// Unpaced and roomy: the walk tests are about what is fetched, not when.
    const TEST_BUDGET: WalkBudget =
        WalkBudget { max_gets: 1000, max_list_pages: 100, get_interval: Duration::ZERO, renew_claim_every_gets: 1000 };

    async fn personal_mailbox(base: &str, db: &str) -> PersonalMailbox {
        set_test_kek();
        let conn = recreate(base, db).await;
        conn.close().await.ok();
        let pool = control_pool(base, db).await;
        let owner = seed_user(&pool).await;
        let kek = load_kek().expect("test KEK loads");
        let wrapped = wrap_dek(&kek, &generate_dek()).expect("wrap test dek");
        let account_id: Uuid =
            sqlx::query_scalar("INSERT INTO accounts(owner_user_id, dek_wrapped) VALUES ($1, $2) RETURNING id")
                .bind(owner)
                .bind(&wrapped)
                .fetch_one(&pool)
                .await
                .unwrap();
        sqlx::query("INSERT INTO account_members(account_id, user_id, role) VALUES ($1, $2, 'owner')")
            .bind(account_id)
            .bind(owner)
            .execute(&pool)
            .await
            .unwrap();
        let graph_id: Uuid = sqlx::query_scalar(
            "INSERT INTO graphs(account_id, name, kind, is_primary) \
             VALUES ($1, 'personal ocean', 'personal', true) RETURNING id",
        )
        .bind(account_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        seed_everything_mode(&pool, account_id, owner, graph_id, true).await;
        let state = Arc::new(crate::test_app_state_owned_with_db_and_store(pool.clone(), None));
        PersonalMailbox {
            pool,
            state,
            account_id,
            owner,
            graph_id,
            budget: Mutex::new(TEST_BUDGET),
            rejects: Mutex::new(HashSet::new()),
        }
    }

    impl PersonalMailbox {
        /// One worker tick for this owner, exactly as `tick_consumer` runs it
        /// after discovery (claim → sync → `mark_failed_no_progress` on Err),
        /// minus credential loading.
        async fn tick(&self, url: &str) -> Result<bool, String> {
            let claim_id = self.claim(15 * 60).await;
            self.tick_with_claim(url, claim_id).await
        }

        /// Claim the row as discovery does, with a claim that lasts `ttl_secs`.
        async fn claim(&self, ttl_secs: i64) -> Uuid {
            sqlx::query_scalar(
                "UPDATE gmail_everything_mode SET claim_id = gen_random_uuid(), \
                 claimed_until = now() + make_interval(secs => $3) \
                 WHERE account_id = $1 AND owner_user_id = $2 RETURNING claim_id",
            )
            .bind(self.account_id)
            .bind(self.owner)
            .bind(ttl_secs as f64)
            .fetch_one(&self.pool)
            .await
            .unwrap()
        }

        async fn exec(&self, sql: &str) {
            sqlx::query(sql)
                .bind(self.account_id)
                .bind(self.owner)
                .execute(&self.pool)
                .await
                .unwrap();
        }

        async fn tick_with_claim(&self, url: &str, claim_id: Uuid) -> Result<bool, String> {
            let rejects = self.rejects.lock().unwrap().clone();
            let outcome = crate::connectors::google_gmail::TEST_MESSAGES_URL
                .scope(url.to_string(), TEST_INGEST_REJECTS.scope(rejects, self.sync(claim_id)))
                .await;
            match outcome {
                Ok(worked) => Ok(worked),
                Err(e) => {
                    mark_failed_no_progress(&self.pool, self.account_id, self.owner, claim_id, &e.to_string()).await;
                    Err(e.to_string())
                }
            }
        }

        async fn sync(&self, claim_id: Uuid) -> Result<bool, ConnectorError> {
            let Some(owner) = owner_state(&self.pool, self.account_id, self.owner).await? else {
                return Ok(false);
            };
            let budget = *self.budget.lock().unwrap();
            sync_owner_with_token(
                &self.pool,
                &self.state,
                self.account_id,
                self.owner,
                claim_id,
                None,
                "test-access-token",
                "test-mailbox",
                owner,
                budget,
            )
            .await
        }

        /// Distinct Gmail message ids with at least one stored coral.
        async fn ingested_ids(&self) -> Vec<String> {
            sqlx::query_scalar(
                "SELECT DISTINCT split_part(source_id, ':chunk:', 1) AS id FROM atoms \
                 WHERE graph_id = $1 AND source_type = 'gmail' ORDER BY id",
            )
            .bind(self.graph_id)
            .fetch_all(&self.pool)
            .await
            .unwrap()
        }

        async fn row(&self) -> (Option<String>, Option<String>) {
            sqlx::query_as(
                "SELECT resume_cursor, last_error FROM gmail_everything_mode \
                 WHERE account_id = $1 AND owner_user_id = $2",
            )
            .bind(self.account_id)
            .bind(self.owner)
            .fetch_one(&self.pool)
            .await
            .unwrap()
        }

        async fn given_up_ids(&self) -> Vec<String> {
            self.ids_column("given_up_ids").await
        }

        async fn unreadable_ids(&self) -> Vec<String> {
            self.ids_column("unreadable_ids").await
        }

        async fn ids_column(&self, column: &str) -> Vec<String> {
            sqlx::query_scalar(&format!(
                "SELECT {column} FROM gmail_everything_mode WHERE account_id = $1 AND owner_user_id = $2"
            ))
            .bind(self.account_id)
            .bind(self.owner)
            .fetch_one(&self.pool)
            .await
            .unwrap()
        }

        async fn page_failures(&self) -> i32 {
            sqlx::query_scalar(
                "SELECT page_failures FROM gmail_everything_mode WHERE account_id = $1 AND owner_user_id = $2",
            )
            .bind(self.account_id)
            .bind(self.owner)
            .fetch_one(&self.pool)
            .await
            .unwrap()
        }

        async fn make_full_walk_due(&self) {
            self.exec(
                "UPDATE gmail_everything_mode SET last_full_walk_at = now() - interval '25 hours' \
                 WHERE account_id = $1 AND owner_user_id = $2",
            )
            .await;
        }

        async fn set_resume_cursor(&self, cursor: &str) {
            sqlx::query("UPDATE gmail_everything_mode SET resume_cursor = $3 WHERE account_id = $1 AND owner_user_id = $2")
                .bind(self.account_id)
                .bind(self.owner)
                .bind(cursor)
                .execute(&self.pool)
                .await
                .unwrap();
        }
    }

    fn ids(prefix: &str, n: usize) -> Vec<String> {
        let mut v: Vec<String> = (0..n).map(|i| format!("{prefix}{i}")).collect();
        v.sort();
        v
    }

    /// (a) Once the walk has completed, a tick with no new mail must not
    /// `messages.get` anything it already ingested, and must cost O(1) list
    /// pages — the prod bug re-fetched the whole mailbox every 30 minutes.
    #[tokio::test]
    async fn steady_state_tick_with_no_new_mail_makes_no_message_gets_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping steady_state_tick_with_no_new_mail_makes_no_message_gets_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let mb = personal_mailbox(&base, "g24_walk_steady_state").await;
        let mock = MockGmail::new(3);
        mock.add_newest("old-", 7, chrono::Duration::days(3));
        // Two messages from an hour ago: inside the high-water slack, so a
        // steady-state walk LISTS them again — only the known-id skip keeps
        // them from being re-fetched (N3: proves the skip, not just `after:`).
        mock.add_newest("hour-", 2, chrono::Duration::hours(1));
        let url = spawn_mock_gmail(mock.clone()).await;

        assert_eq!(mb.tick(&url).await, Ok(true));
        let mut first = ids("old-", 7);
        first.extend(ids("hour-", 2));
        first.sort();
        assert_eq!(mb.ingested_ids().await, first, "the first walk ingests the whole mailbox");
        assert_eq!(mb.row().await, (None, None), "the first walk completes cleanly");

        mock.reset_counters();
        assert_eq!(mb.tick(&url).await, Ok(true));
        assert_eq!(
            mock.get_calls.load(Ordering::SeqCst),
            0,
            "a steady-state tick with no new mail must not re-get already-ingested messages, \
             including the ones its after: floor still lists"
        );
        assert!(
            mock.list_calls.load(Ordering::SeqCst) <= 1,
            "a steady-state tick with no new mail must cost at most one list page, got {}",
            mock.list_calls.load(Ordering::SeqCst)
        );

        // New mail after the high-water mark is still picked up — and ONLY it.
        mock.add_newest("new-", 2, chrono::Duration::minutes(1));
        mock.reset_counters();
        assert_eq!(mb.tick(&url).await, Ok(true));
        assert_eq!(mock.get_calls.load(Ordering::SeqCst), 2, "only the two new messages are fetched");
        let mut want = first.clone();
        want.extend(ids("new-", 2));
        want.sort();
        assert_eq!(mb.ingested_ids().await, want);

        teardown(&base, "g24_walk_steady_state", mb.pool).await;
    }

    /// (b) A per-user quota 403 partway through a page-tokened walk keeps
    /// everything ingested so far, is NOT read as a stale pageToken (no
    /// restart from the newest message), and the next tick resumes where
    /// this one stopped.
    #[tokio::test]
    async fn rate_limit_mid_walk_preserves_progress_and_does_not_restart_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping rate_limit_mid_walk_preserves_progress_and_does_not_restart_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let mb = personal_mailbox(&base, "g24_walk_rate_limit").await;
        let mock = MockGmail::new(2);
        mock.add_newest("m-", 6, chrono::Duration::days(2));
        *mock.rate_limit_after_gets.lock().unwrap() = Some(3);
        let url = spawn_mock_gmail(mock.clone()).await;

        let _ = mb.tick(&url).await;
        let after_first = mb.ingested_ids().await;
        assert!(
            after_first.len() >= 2,
            "everything fetched before the quota 403 must be kept, got {after_first:?}"
        );
        let tokens = mock.list_page_tokens.lock().unwrap().clone();
        assert!(
            !tokens.iter().skip(1).any(Option::is_none),
            "a quota 403 must never restart the walk from the newest message, list calls: {tokens:?}"
        );
        let (cursor, error) = mb.row().await;
        assert!(cursor.is_some(), "the walk position is persisted for the next tick");
        assert_eq!(
            error, None,
            "N1: a rate limit after real progress is routine pacing, not an error on the row"
        );

        // Next window: the quota has reset.
        mock.reset_counters();
        *mock.rate_limit_after_gets.lock().unwrap() = None;
        assert_eq!(mb.tick(&url).await, Ok(true));
        assert_eq!(mb.ingested_ids().await, ids("m-", 6));
        let tokens = mock.list_page_tokens.lock().unwrap().clone();
        assert!(tokens[0].is_some(), "the next tick resumes from the persisted pageToken, list calls: {tokens:?}");
        assert!(
            mock.get_calls.load(Ordering::SeqCst) <= 6 - after_first.len(),
            "the resumed tick must not re-get what the interrupted tick already ingested"
        );
        assert_eq!(mb.row().await, (None, None));

        // A tick the quota stops before it does ANYTHING is worth surfacing.
        mock.reset_counters();
        *mock.rate_limit_after_gets.lock().unwrap() = Some(0);
        let _ = mb.tick(&url).await;
        let (_, error) = mb.row().await;
        let error = error.expect("a tick rate-limited with no progress is surfaced");
        assert!(error.contains("rateLimitExceeded"), "last_error keeps Google's reason: {error}");

        teardown(&base, "g24_walk_rate_limit", mb.pool).await;
    }

    /// (c) A genuinely stale pageToken (Gmail answers 400 or 404) still
    /// restarts the walk from the newest message, and the walk completes.
    async fn stale_page_token_restarts(base: &str, db: &str, status: u16) {
        let mb = personal_mailbox(base, db).await;
        let mock = MockGmail::with_stale_status(2, status);
        mock.add_newest("s-", 5, chrono::Duration::days(1));
        let url = spawn_mock_gmail(mock.clone()).await;
        mb.set_resume_cursor("expired-token-from-last-month").await;

        assert_eq!(mb.tick(&url).await, Ok(true), "a stale token self-heals within the tick ({status})");
        let tokens = mock.list_page_tokens.lock().unwrap().clone();
        assert_eq!(tokens[0].as_deref(), Some("expired-token-from-last-month"));
        assert_eq!(tokens[1], None, "the stale token ({status}) restarts from the newest message: {tokens:?}");
        assert_eq!(mb.ingested_ids().await, ids("s-", 5));
        assert_eq!(mb.row().await, (None, None));
        teardown(base, db, mb.pool).await;
    }

    #[tokio::test]
    async fn stale_page_token_400_still_restarts_the_walk_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping stale_page_token_400_still_restarts_the_walk_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        stale_page_token_restarts(&base, "g24_walk_stale_400", 400).await;
    }

    #[tokio::test]
    async fn stale_page_token_404_still_restarts_the_walk_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping stale_page_token_404_still_restarts_the_walk_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        stale_page_token_restarts(&base, "g24_walk_stale_404", 404).await;
    }

    /// The stale-token branch is narrowed to 400/404: any OTHER 4xx sent with
    /// a pageToken (here a 403 that is not a rate limit) is a real failure —
    /// surfaced, no restart, the stored position untouched.
    #[tokio::test]
    async fn a_non_rate_limit_403_with_a_page_token_fails_without_restarting_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping a_non_rate_limit_403_with_a_page_token_fails_without_restarting_pg: set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "g24_walk_403_not_stale";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mb = personal_mailbox(&base, db).await;
        let mock = MockGmail::with_stale_status(2, 403);
        mock.add_newest("p-", 3, chrono::Duration::days(1));
        let url = spawn_mock_gmail(mock.clone()).await;
        mb.set_resume_cursor("token-the-api-refuses").await;

        let err = mb.tick(&url).await.expect_err("a policy/permission 403 is a failure");
        assert!(err.contains("403"), "{err}");
        assert_eq!(
            mock.list_page_tokens.lock().unwrap().clone(),
            vec![Some("token-the-api-refuses".to_string())],
            "no restart from the newest message"
        );
        assert_eq!(mock.get_calls.load(Ordering::SeqCst), 0);
        let (cursor, error) = mb.row().await;
        assert_eq!(cursor.as_deref(), Some("token-the-api-refuses"), "the stored position is untouched");
        assert!(error.is_some());
        teardown(&base, db, mb.pool).await;
    }

    /// B1 (review of #1166): a transient `messages.get` failure (here a
    /// Google 500, once) must not lose that message. Before, it was logged,
    /// skipped, and the page marked done — and with the high-water mark no
    /// later walk would ever list it again.
    #[tokio::test]
    async fn a_transient_get_failure_does_not_lose_the_message_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping a_transient_get_failure_does_not_lose_the_message_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "g24_walk_transient_get";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mb = personal_mailbox(&base, db).await;
        let mock = MockGmail::new(3);
        mock.add_newest("x-", 5, chrono::Duration::days(3));
        mock.fail_gets.lock().unwrap().insert("x-2".into(), 1);
        let url = spawn_mock_gmail(mock.clone()).await;

        let _ = mb.tick(&url).await;
        let (_, error) = mb.row().await;
        let error = error.expect("the failed get is surfaced");
        assert!(error.contains("500"), "{error}");
        for _ in 0..3 {
            let _ = mb.tick(&url).await;
        }
        assert_eq!(mb.ingested_ids().await, ids("x-", 5), "the message that failed once is ingested later");
        assert_eq!(mb.row().await, (None, None));
        teardown(&base, db, mb.pool).await;
    }

    /// S3 (review of #1166): a message that ALWAYS fails must not pin the
    /// walk on its page forever. After a few failed ticks on that page, it
    /// is skipped, named on `last_error`, and the walk completes.
    #[tokio::test]
    async fn a_message_that_always_fails_is_skipped_after_a_few_ticks_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping a_message_that_always_fails_is_skipped_after_a_few_ticks_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "g24_walk_poison";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mb = personal_mailbox(&base, db).await;
        let mock = MockGmail::new(3);
        mock.add_newest("x-", 5, chrono::Duration::days(3));
        mock.fail_gets.lock().unwrap().insert("x-2".into(), usize::MAX);
        let url = spawn_mock_gmail(mock.clone()).await;

        for _ in 0..6 {
            let _ = mb.tick(&url).await;
        }
        let mut want = ids("x-", 5);
        want.retain(|id| id != "x-2");
        assert_eq!(mb.ingested_ids().await, want);
        let (cursor, error) = mb.row().await;
        assert_eq!(cursor, None, "the walk completed past the poisoned message");
        let error = error.expect("the skipped message is surfaced");
        assert!(error.contains("x-2"), "last_error names the skipped message: {error}");
        let mark: Option<chrono::DateTime<Utc>> = sqlx::query_scalar(
            "SELECT high_water_at FROM gmail_everything_mode WHERE account_id = $1 AND owner_user_id = $2",
        )
        .bind(mb.account_id)
        .bind(mb.owner)
        .fetch_one(&mb.pool)
        .await
        .unwrap();
        assert!(mark.is_some(), "the walk completed and moved the mark");
        teardown(&base, db, mb.pool).await;
    }

    /// S1 (review of #1166): mail that joins the listing with an OLD date
    /// (moved out of Spam days later, restored from Trash, imported) sits
    /// below the `after:` floor. A periodic full walk still picks it up.
    #[tokio::test]
    async fn a_daily_full_walk_picks_up_mail_that_arrives_with_an_old_date_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping a_daily_full_walk_picks_up_mail_that_arrives_with_an_old_date_pg: set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "g24_walk_full_walk";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mb = personal_mailbox(&base, db).await;
        let mock = MockGmail::new(3);
        mock.add_newest("m-", 4, chrono::Duration::days(1));
        let url = spawn_mock_gmail(mock.clone()).await;
        assert_eq!(mb.tick(&url).await, Ok(true));
        assert_eq!(mb.ingested_ids().await, ids("m-", 4));

        // Out of Spam, dated five days ago — below the floor.
        mock.add_dated("late-0", chrono::Duration::days(5));
        mock.reset_counters();
        assert_eq!(mb.tick(&url).await, Ok(true));
        assert_eq!(mock.get_calls.load(Ordering::SeqCst), 0, "a bounded walk does not see it");

        // A day on, the next walk is a full one, and it costs one get.
        mb.exec(
            "UPDATE gmail_everything_mode SET last_full_walk_at = now() - interval '25 hours' \
             WHERE account_id = $1 AND owner_user_id = $2",
        )
        .await;
        mock.reset_counters();
        assert_eq!(mb.tick(&url).await, Ok(true));
        assert_eq!(mock.get_calls.load(Ordering::SeqCst), 1, "only the unknown message is fetched");
        let mut want = ids("m-", 4);
        want.push("late-0".into());
        want.sort();
        assert_eq!(mb.ingested_ids().await, want);
        teardown(&base, db, mb.pool).await;
    }

    /// S2 (review of #1166): discovery claims rows 15 minutes ahead and
    /// walks them one after another, so a claim can run out mid-walk and a
    /// second machine can claim the same mailbox. The walk must renew its
    /// claim before it starts and after every page.
    #[tokio::test]
    async fn the_walk_keeps_its_claim_alive_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping the_walk_keeps_its_claim_alive_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "g24_walk_claim_alive";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mb = personal_mailbox(&base, db).await;
        let mock = MockGmail::new(2);
        mock.add_newest("c-", 6, chrono::Duration::days(1));
        *mock.claim_probe.lock().unwrap() = Some((mb.pool.clone(), mb.account_id, mb.owner));
        let url = spawn_mock_gmail(mock.clone()).await;

        // A claim with seconds left — the tail of a serial sweep.
        let claim_id = mb.claim(5).await;
        assert_eq!(mb.tick_with_claim(&url, claim_id).await, Ok(true));
        let seen = mock.claimed_until_seen.lock().unwrap().clone();
        assert_eq!(seen.len(), 3, "three list pages");
        let floor = Utc::now() + chrono::Duration::minutes(10);
        for (i, until) in seen.iter().enumerate() {
            let until = until.expect("claimed while walking");
            assert!(until > floor, "list call {i}: the claim must be renewed, was {until}");
        }
        assert_eq!(mb.ingested_ids().await, ids("c-", 6));
        teardown(&base, db, mb.pool).await;
    }

    /// S2: a walk whose claim another machine has since taken must not touch
    /// Gmail at all.
    #[tokio::test]
    async fn a_walk_whose_claim_was_taken_over_does_not_call_gmail_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping a_walk_whose_claim_was_taken_over_does_not_call_gmail_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "g24_walk_claim_lost";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mb = personal_mailbox(&base, db).await;
        let mock = MockGmail::new(2);
        mock.add_newest("c-", 4, chrono::Duration::days(1));
        let url = spawn_mock_gmail(mock.clone()).await;

        let stale_claim = mb.claim(15 * 60).await;
        let _other_machine = mb.claim(15 * 60).await;
        let _ = mb.tick_with_claim(&url, stale_claim).await;
        assert_eq!(mock.list_calls.load(Ordering::SeqCst), 0, "no Gmail call under a lost claim");
        assert_eq!(mock.get_calls.load(Ordering::SeqCst), 0);
        assert!(mb.ingested_ids().await.is_empty());
        teardown(&base, db, mb.pool).await;
    }

    /// (d) The prod backlog: two weeks of mail behind a per-user quota that
    /// trips every tick. Each tick must bank what it got, so the backlog
    /// drains across ticks instead of failing forever, and no message is
    /// fetched twice.
    #[tokio::test]
    async fn backlog_drains_across_rate_limited_ticks_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping backlog_drains_across_rate_limited_ticks_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let mb = personal_mailbox(&base, "g24_walk_backlog").await;
        let mock = MockGmail::new(3);
        mock.add_newest("b-", 10, chrono::Duration::days(14));
        *mock.rate_limit_after_gets.lock().unwrap() = Some(4);
        let url = spawn_mock_gmail(mock.clone()).await;

        let mut total_gets = 0usize;
        let mut ticks = 0;
        while mb.ingested_ids().await.len() < 10 && ticks < 6 {
            mock.reset_counters();
            let _ = mb.tick(&url).await;
            total_gets += mock.get_calls.load(Ordering::SeqCst);
            ticks += 1;
        }
        assert_eq!(mb.ingested_ids().await, ids("b-", 10), "the backlog drains within {ticks} ticks");
        assert!(ticks <= 3, "4 gets per window over 10 messages needs 3 ticks, took {ticks}");
        // Each window's rejected get counts as a call too — at most one per tick.
        assert!(
            total_gets <= 10 + ticks,
            "no message is fetched twice across ticks: {total_gets} gets over {ticks} ticks"
        );

        // And once drained, the walk completes and goes quiet.
        mock.reset_counters();
        *mock.rate_limit_after_gets.lock().unwrap() = None;
        assert_eq!(mb.tick(&url).await, Ok(true));
        assert_eq!(mb.row().await, (None, None));
        mock.reset_counters();
        assert_eq!(mb.tick(&url).await, Ok(true));
        assert_eq!(mock.get_calls.load(Ordering::SeqCst), 0);

        teardown(&base, "g24_walk_backlog", mb.pool).await;
    }

    /// The per-tick budget: a tick stops at `max_gets`, cleanly (no error,
    /// claim released), and the next tick picks up where it stopped without
    /// re-fetching anything.
    #[tokio::test]
    async fn get_budget_pauses_the_tick_and_the_next_tick_resumes_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping get_budget_pauses_the_tick_and_the_next_tick_resumes_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let mb = personal_mailbox(&base, "g24_walk_budget").await;
        *mb.budget.lock().unwrap() = WalkBudget { max_gets: 4, ..TEST_BUDGET };
        let mock = MockGmail::new(3);
        mock.add_newest("g-", 10, chrono::Duration::days(5));
        let url = spawn_mock_gmail(mock.clone()).await;

        assert_eq!(mb.tick(&url).await, Ok(true));
        assert_eq!(mock.get_calls.load(Ordering::SeqCst), 4, "the tick stops at its get budget");
        assert_eq!(mb.ingested_ids().await.len(), 4);
        let (cursor, error) = mb.row().await;
        assert!(cursor.is_some(), "a paused walk keeps its place");
        assert_eq!(error, None, "running out of budget is not an error");

        let mut total_gets = 4;
        for _ in 0..2 {
            mock.reset_counters();
            assert_eq!(mb.tick(&url).await, Ok(true));
            total_gets += mock.get_calls.load(Ordering::SeqCst);
        }
        assert_eq!(mb.ingested_ids().await, ids("g-", 10));
        assert_eq!(total_gets, 10, "every message is fetched exactly once across the paused ticks");
        assert_eq!(mb.row().await, (None, None), "the walk completes on the third tick");

        teardown(&base, "g24_walk_budget", mb.pool).await;
    }

    /// S-a (re-review of #1166): `page_failures` counts ticks that failed on
    /// a page WITHOUT getting anywhere. Here every tick stores a message and
    /// then hits a different, one-off 500 on the same page. Before, the
    /// counter climbed anyway, and the 4th tick gave up on a message that
    /// had failed exactly once.
    #[tokio::test]
    async fn a_page_that_keeps_making_progress_never_gives_up_on_a_message_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping a_page_that_keeps_making_progress_never_gives_up_on_a_message_pg: set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "g24_walk_progress_resets";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mb = personal_mailbox(&base, db).await;
        let mock = MockGmail::new(5);
        // Listed newest first: x-4, x-3, x-2, x-1, x-0 — one page.
        mock.add_newest("x-", 5, chrono::Duration::days(3));
        for id in ["x-3", "x-2", "x-1", "x-0"] {
            mock.fail_gets.lock().unwrap().insert(id.into(), 1);
        }
        let url = spawn_mock_gmail(mock.clone()).await;

        for _ in 0..6 {
            let _ = mb.tick(&url).await;
        }
        assert_eq!(mb.ingested_ids().await, ids("x-", 5), "every message that failed only once is stored");
        assert!(mb.given_up_ids().await.is_empty(), "nothing was given up");
        assert_eq!(mb.row().await, (None, None), "the walk completed with no give-up note");
        teardown(&base, db, mb.pool).await;
    }

    /// S-a: the tick that finally gives up on the message that kept failing
    /// gives up on THAT message only. Another message on the page that fails
    /// for the first time in that tick is retried, not skipped.
    #[tokio::test]
    async fn a_give_up_tick_skips_only_the_message_that_kept_failing_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping a_give_up_tick_skips_only_the_message_that_kept_failing_pg: set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "g24_walk_give_up_one";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mb = personal_mailbox(&base, db).await;
        let mock = MockGmail::new(5);
        mock.add_newest("x-", 5, chrono::Duration::days(3));
        mock.fail_gets.lock().unwrap().insert("x-3".into(), usize::MAX);
        // Never reached until x-3 is given up; then fails once.
        mock.fail_gets.lock().unwrap().insert("x-1".into(), 1);
        let url = spawn_mock_gmail(mock.clone()).await;

        for _ in 0..7 {
            let _ = mb.tick(&url).await;
        }
        let mut want = ids("x-", 5);
        want.retain(|id| id != "x-3");
        assert_eq!(mb.ingested_ids().await, want, "x-1 failed once and is stored on the retry");
        assert_eq!(mb.given_up_ids().await, vec!["x-3".to_string()]);
        let (cursor, error) = mb.row().await;
        assert_eq!(cursor, None, "the walk completed");
        assert!(!error.unwrap_or_default().contains("x-1"), "x-1 was never given up");
        teardown(&base, db, mb.pool).await;
    }

    /// S-b (re-review of #1166): a message the walk gave up on is remembered.
    /// The next daily FULL walk (no `after:` floor) lists it again. It tries
    /// it ONCE (S2, review of #1168: it may have recovered) and, if it still
    /// fails, moves on in the same tick instead of failing for another
    /// 1.5–3 hours while new mail waits.
    #[tokio::test]
    async fn a_full_walk_retries_a_kept_failing_message_once_without_stalling_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping a_full_walk_retries_a_kept_failing_message_once_without_stalling_pg: set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "g24_walk_given_up_persists";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mb = personal_mailbox(&base, db).await;
        let mock = MockGmail::new(3);
        mock.add_newest("x-", 5, chrono::Duration::days(3));
        mock.fail_gets.lock().unwrap().insert("x-2".into(), usize::MAX);
        let url = spawn_mock_gmail(mock.clone()).await;
        for _ in 0..6 {
            let _ = mb.tick(&url).await;
        }
        assert_eq!(mb.row().await.0, None, "the first walk completed past x-2");
        assert_eq!(mb.given_up_ids().await, vec!["x-2".to_string()], "the give-up is persisted");
        assert!(mb.unreadable_ids().await.is_empty(), "a kept-failing message is not marked permanent");

        // A day later: a full walk, and one new message.
        mb.make_full_walk_due().await;
        mock.add_newest("new-", 1, chrono::Duration::minutes(1));
        mock.reset_counters();
        assert_eq!(mb.tick(&url).await, Ok(true));
        assert_eq!(mock.get_calls.load(Ordering::SeqCst), 2, "the new message, and x-2 tried exactly once");
        assert!(mb.ingested_ids().await.contains(&"new-0".to_string()));
        let (cursor, _) = mb.row().await;
        assert_eq!(cursor, None, "the full walk completes in one tick, not stalled on x-2");
        assert_eq!(mb.page_failures().await, 0);
        assert_eq!(mb.given_up_ids().await, vec!["x-2".to_string()], "still failing: still skipped");
        let full: Option<chrono::DateTime<Utc>> = sqlx::query_scalar(
            "SELECT last_full_walk_at FROM gmail_everything_mode WHERE account_id = $1 AND owner_user_id = $2",
        )
        .bind(mb.account_id)
        .bind(mb.owner)
        .fetch_one(&mb.pool)
        .await
        .unwrap();
        assert!(full.unwrap() > Utc::now() - chrono::Duration::minutes(5), "the full walk completed");
        teardown(&base, db, mb.pool).await;
    }

    /// S2 (review of #1168): a message given up on because it kept failing
    /// may recover (our ingest was down for a few hours, say). Bounded walks
    /// skip it, but the next full walk retries it, and once stored it leaves
    /// the list.
    #[tokio::test]
    async fn a_kept_failing_message_is_retried_on_the_next_full_walk_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping a_kept_failing_message_is_retried_on_the_next_full_walk_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "g24_walk_given_up_retried";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mb = personal_mailbox(&base, db).await;
        let mock = MockGmail::new(3);
        mock.add_newest("x-", 5, chrono::Duration::hours(2));
        mock.fail_gets.lock().unwrap().insert("x-2".into(), usize::MAX);
        let url = spawn_mock_gmail(mock.clone()).await;
        for _ in 0..6 {
            let _ = mb.tick(&url).await;
        }
        assert_eq!(mb.given_up_ids().await, vec!["x-2".to_string()]);

        // It recovers. A bounded walk (x-2 is inside its slack, so listed)
        // still skips it.
        mock.fail_gets.lock().unwrap().clear();
        mock.reset_counters();
        assert_eq!(mb.tick(&url).await, Ok(true));
        assert_eq!(mock.get_calls.load(Ordering::SeqCst), 0, "a bounded walk does not retry it");

        // The next full walk does, and stores it.
        mb.make_full_walk_due().await;
        mock.reset_counters();
        assert_eq!(mb.tick(&url).await, Ok(true));
        assert_eq!(mock.get_calls.load(Ordering::SeqCst), 1);
        assert_eq!(mb.ingested_ids().await, ids("x-", 5), "x-2 is stored on the full walk");
        assert!(mb.given_up_ids().await.is_empty(), "and leaves the list");
        teardown(&base, db, mb.pool).await;
    }

    /// S2: only a message whose body never parses is skipped for good: no
    /// later walk, full or not, fetches it again.
    #[tokio::test]
    async fn an_unreadable_message_is_never_fetched_again_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping an_unreadable_message_is_never_fetched_again_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "g24_walk_unreadable";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mb = personal_mailbox(&base, db).await;
        let mock = MockGmail::new(3);
        mock.add_newest("u-", 3, chrono::Duration::days(2));
        mock.unreadable.lock().unwrap().insert("u-1".into());
        let url = spawn_mock_gmail(mock.clone()).await;
        assert_eq!(mb.tick(&url).await, Ok(true));
        assert_eq!(mb.row().await.0, None, "an unreadable message does not hold the walk");
        assert_eq!(mb.unreadable_ids().await, vec!["u-1".to_string()]);
        assert!(mb.given_up_ids().await.is_empty());

        mb.make_full_walk_due().await;
        mock.reset_counters();
        assert_eq!(mb.tick(&url).await, Ok(true));
        assert_eq!(mock.get_calls.load(Ordering::SeqCst), 0, "a full walk does not fetch it again");
        teardown(&base, db, mb.pool).await;
    }

    /// B1 (review of #1168): the walk gives up only on the message the
    /// failed ticks were counted against. Here x-4 fails three ticks and then
    /// recovers in the give-up tick; x-3 then fails for the first time. x-3
    /// must get its own retries, not inherit x-4's count.
    #[tokio::test]
    async fn a_message_recovering_in_the_give_up_tick_leaves_the_next_one_its_retries_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping a_message_recovering_in_the_give_up_tick_leaves_the_next_one_its_retries_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "g24_walk_give_up_by_id";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mb = personal_mailbox(&base, db).await;
        let mock = MockGmail::new(5);
        // Listed newest first: x-4, x-3, x-2, x-1, x-0 — one page.
        mock.add_newest("x-", 5, chrono::Duration::days(3));
        mock.fail_gets.lock().unwrap().insert("x-4".into(), 3);
        mock.fail_gets.lock().unwrap().insert("x-3".into(), 1);
        let url = spawn_mock_gmail(mock.clone()).await;

        for _ in 0..8 {
            let _ = mb.tick(&url).await;
        }
        assert!(mb.given_up_ids().await.is_empty(), "nothing was given up");
        assert_eq!(mb.ingested_ids().await, ids("x-", 5), "x-3, which failed once, is stored on its retry");
        assert_eq!(mb.row().await, (None, None));
        teardown(&base, db, mb.pool).await;
    }

    /// S3 (review of #1168): a tick that stores messages on a page and then
    /// stops at its budget got somewhere on that page, so the failed-tick
    /// count for it starts over.
    #[tokio::test]
    async fn a_paused_tick_that_stored_on_its_page_resets_page_failures_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping a_paused_tick_that_stored_on_its_page_resets_page_failures_pg: set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "g24_walk_pause_resets";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mb = personal_mailbox(&base, db).await;
        *mb.budget.lock().unwrap() = WalkBudget { max_gets: 2, ..TEST_BUDGET };
        let mock = MockGmail::new(5);
        mock.add_newest("p-", 5, chrono::Duration::days(3));
        let url = spawn_mock_gmail(mock.clone()).await;
        mb.exec(
            "UPDATE gmail_everything_mode SET page_failures = 2, page_failing_id = 'p-0' \
             WHERE account_id = $1 AND owner_user_id = $2",
        )
        .await;

        assert_eq!(mb.tick(&url).await, Ok(true));
        assert_eq!(mb.ingested_ids().await.len(), 2, "paused at its budget");
        assert_eq!(mb.page_failures().await, 0, "progress on the page resets the count");
        teardown(&base, db, mb.pool).await;
    }

    /// Fix round 2 (review of #1168): a full walk retries each given-up
    /// message once, and a still-failing retry is not fetched again in that
    /// walk. Before, a tick that paused at its get budget left nothing to say
    /// the retries were done, so the next tick of the same walk fetched them
    /// again: with the budget no bigger than the retries at the head of a
    /// page, every tick spent it all on them, stored nothing, and the walk
    /// never moved (the reviewer's repro: `max_gets: 2`, x-4 and x-3 given
    /// up and still failing, 12 gets in 6 ticks, nothing stored).
    #[tokio::test]
    async fn full_walk_retries_cannot_eat_every_ticks_budget_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping full_walk_retries_cannot_eat_every_ticks_budget_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "g24_walk_retry_budget";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mb = personal_mailbox(&base, db).await;
        *mb.budget.lock().unwrap() = WalkBudget { max_gets: 2, ..TEST_BUDGET };
        let mock = MockGmail::new(5);
        // Listed newest first: x-4, x-3, x-2, x-1, x-0 — one page.
        mock.add_newest("x-", 5, chrono::Duration::days(3));
        for id in ["x-4", "x-3"] {
            mock.fail_gets.lock().unwrap().insert(id.into(), usize::MAX);
        }
        let url = spawn_mock_gmail(mock.clone()).await;
        // Given up on before; no walk completed yet, so this one is full.
        mb.exec(
            "UPDATE gmail_everything_mode SET given_up_ids = ARRAY['x-4', 'x-3'] \
             WHERE account_id = $1 AND owner_user_id = $2",
        )
        .await;

        // The reviewer's repro: six ticks.
        for _ in 0..6 {
            let _ = mb.tick(&url).await;
        }
        let mut want = ids("x-", 5);
        want.retain(|id| id != "x-4" && id != "x-3");
        assert_eq!(mb.ingested_ids().await, want, "the rest of the page is stored");
        assert_eq!(mb.row().await.0, None, "the walk completed");
        assert_eq!(
            mock.get_calls.load(Ordering::SeqCst),
            5,
            "each retry fetched once in the full walk, each new message once (later walks are bounded)"
        );
        assert_eq!(mb.given_up_ids().await, vec!["x-4".to_string(), "x-3".to_string()], "still given up");
        assert!(mb.ids_column("walk_skip_ids").await.is_empty(), "the walk's own list ends with it");
        teardown(&base, db, mb.pool).await;
    }

    /// Fix round 2: a message hidden by the time of the get, or with nothing
    /// ingestable in it, is not fetched again on every tick. With a small
    /// budget and both at the head of a page, that churn used to stall the
    /// walk the same way. Hidden mail can come back (restored from Trash), so
    /// it is skipped only for the rest of the walk; a later walk fetches it.
    /// An empty message never changes, so it is skipped for good.
    #[tokio::test]
    async fn hidden_and_empty_messages_are_not_refetched_every_tick_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping hidden_and_empty_messages_are_not_refetched_every_tick_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "g24_walk_skip_churn";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mb = personal_mailbox(&base, db).await;
        *mb.budget.lock().unwrap() = WalkBudget { max_gets: 2, ..TEST_BUDGET };
        let mock = MockGmail::new(5);
        mock.add_newest("m-", 3, chrono::Duration::days(3));
        // Newest: z-1 (in Trash by the time of the get), z-0 (empty body).
        mock.add_newest("z-", 2, chrono::Duration::days(2));
        mock.hidden.lock().unwrap().insert("z-1".into());
        mock.no_body.lock().unwrap().insert("z-0".into());
        let url = spawn_mock_gmail(mock.clone()).await;

        for _ in 0..3 {
            let _ = mb.tick(&url).await;
        }
        assert_eq!(mb.ingested_ids().await, ids("m-", 3), "the walk gets past them");
        assert_eq!(mb.row().await.0, None, "and completes");
        assert_eq!(mock.get_calls.load(Ordering::SeqCst), 5, "each fetched once");
        assert_eq!(mb.unreadable_ids().await, vec!["z-0".to_string()], "an empty message is skipped for good");

        // z-1 is restored from Trash. The next full walk fetches it; z-0 is
        // never fetched again.
        mock.hidden.lock().unwrap().clear();
        mb.make_full_walk_due().await;
        mock.reset_counters();
        assert_eq!(mb.tick(&url).await, Ok(true));
        assert_eq!(mock.get_calls.load(Ordering::SeqCst), 1, "only the restored message");
        assert!(mb.ingested_ids().await.contains(&"z-1".to_string()), "a restored message is not lost");
        teardown(&base, db, mb.pool).await;
    }

    /// Fix round 3 (review of #1168): a `422` from ingest comes from a
    /// validation rule we wrote, so a bad deploy can 422 messages wrongly.
    /// They must not be dropped for good: a 422 is given up on (bounded walks
    /// skip it) and the next full walk retries it.
    #[tokio::test]
    async fn a_rejected_message_is_retried_on_the_next_full_walk_not_dropped_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping a_rejected_message_is_retried_on_the_next_full_walk_not_dropped_pg: set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "g24_walk_422_retried";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mb = personal_mailbox(&base, db).await;
        let mock = MockGmail::new(5);
        mock.add_newest("r-", 3, chrono::Duration::hours(2));
        mb.rejects.lock().unwrap().insert("r-1".into());
        let url = spawn_mock_gmail(mock.clone()).await;

        assert_eq!(mb.tick(&url).await, Ok(true));
        assert_eq!(mb.row().await.0, None, "a 422 does not hold the walk");
        assert!(mb.unreadable_ids().await.is_empty(), "a 422 is never skipped for good");
        assert_eq!(mb.given_up_ids().await, vec!["r-1".to_string()]);

        // The deploy is fixed. A bounded walk (r-1 is inside its slack, so
        // listed) still skips it; the next full walk retries and stores it.
        mb.rejects.lock().unwrap().clear();
        mock.reset_counters();
        assert_eq!(mb.tick(&url).await, Ok(true));
        assert_eq!(mock.get_calls.load(Ordering::SeqCst), 0, "a bounded walk does not retry it");
        mb.make_full_walk_due().await;
        mock.reset_counters();
        assert_eq!(mb.tick(&url).await, Ok(true));
        assert_eq!(mock.get_calls.load(Ordering::SeqCst), 1);
        assert_eq!(mb.ingested_ids().await, ids("r-", 3), "the wrongly rejected message is recovered");
        assert!(mb.given_up_ids().await.is_empty());
        teardown(&base, db, mb.pool).await;
    }

    /// Fix round 2 nit: a failure not tied to any message (here a list call
    /// on a later page) does not count toward `page_failures`. Giving up on a
    /// message would not fix it.
    #[tokio::test]
    async fn a_failure_not_tied_to_a_message_does_not_count_against_the_page_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping a_failure_not_tied_to_a_message_does_not_count_against_the_page_pg: set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "g24_walk_list_fail_no_count";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mb = personal_mailbox(&base, db).await;
        let mock = MockGmail::new(2);
        mock.add_newest("l-", 4, chrono::Duration::days(1));
        *mock.fail_lists_from.lock().unwrap() = Some(2);
        let url = spawn_mock_gmail(mock.clone()).await;

        for _ in 0..3 {
            let _ = mb.tick(&url).await;
            *mock.fail_lists_from.lock().unwrap() = Some(1);
        }
        let (cursor, error) = mb.row().await;
        assert!(cursor.is_some(), "the walk stopped on its second page");
        assert!(error.unwrap_or_default().contains("500"));
        assert_eq!(mb.page_failures().await, 0, "a list failure never counts against the page");
        teardown(&base, db, mb.pool).await;
    }

    /// Nit (re-review of #1166): a single long page can take minutes of gets.
    /// The claim is renewed every `renew_claim_every_gets` gets too, not only
    /// between pages, so it cannot run out mid-page.
    #[tokio::test]
    async fn a_long_page_renews_its_claim_between_gets_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping a_long_page_renews_its_claim_between_gets_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "g24_walk_claim_mid_page";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mb = personal_mailbox(&base, db).await;
        *mb.budget.lock().unwrap() = WalkBudget { renew_claim_every_gets: 2, ..TEST_BUDGET };
        let mock = MockGmail::new(10);
        mock.add_newest("c-", 6, chrono::Duration::days(1));
        *mock.get_probe.lock().unwrap() = Some((mb.pool.clone(), mb.account_id, mb.owner));
        *mock.shorten_claim_at_get.lock().unwrap() = Some(1);
        let url = spawn_mock_gmail(mock.clone()).await;

        assert_eq!(mb.tick(&url).await, Ok(true));
        let seen = mock.claimed_until_at_gets.lock().unwrap().clone();
        assert_eq!(seen.len(), 6, "one page, six gets");
        let floor = Utc::now() + chrono::Duration::minutes(10);
        let last = seen[5].expect("claimed while walking");
        assert!(last > floor, "the claim is renewed mid-page after it ran low: {seen:?}");
        assert_eq!(mb.ingested_ids().await, ids("c-", 6));
        teardown(&base, db, mb.pool).await;
    }

    /// Nit: if another machine takes the claim mid-page, the walk notices at
    /// the next mid-page renewal and stops fetching.
    #[tokio::test]
    async fn a_claim_lost_mid_page_stops_the_gets_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping a_claim_lost_mid_page_stops_the_gets_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "g24_walk_claim_lost_mid_page";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mb = personal_mailbox(&base, db).await;
        *mb.budget.lock().unwrap() = WalkBudget { renew_claim_every_gets: 2, ..TEST_BUDGET };
        let mock = MockGmail::new(10);
        mock.add_newest("c-", 8, chrono::Duration::days(1));
        *mock.get_probe.lock().unwrap() = Some((mb.pool.clone(), mb.account_id, mb.owner));
        *mock.steal_claim_at_get.lock().unwrap() = Some(3);
        let url = spawn_mock_gmail(mock.clone()).await;

        let _ = mb.tick(&url).await;
        assert_eq!(
            mock.get_calls.load(Ordering::SeqCst),
            4,
            "the renewal after the 4th get finds the claim gone and stops the page"
        );
        teardown(&base, db, mb.pool).await;
    }
}

/// The production budget's quota arithmetic, pinned so a retune has to face
/// it. Gmail: `messages.get`/`messages.list` cost 5 units each; the per-user
/// limit is 15,000 units per minute, shared with the push path.
///
/// One mailbox can have TWO rows walking it — a personal row and an org
/// member row for the same Google account — each with its own budget. The
/// quota test sums them, per mailbox.
#[cfg(test)]
mod walk_budget_tests {
    use super::*;
    use crate::connectors::google_gmail::{
        GMAIL_GETS_PER_MINUTE_CEILING, GMAIL_GET_QUERY_COST, GMAIL_QUERY_COST_PER_USER_PER_MINUTE,
        TARGET_GETS_PER_MINUTE,
    };
    use tokio::time::Instant;

    /// The SQL that keeps a give-up note visible matches the note's prefix.
    #[test]
    fn clean_tick_sql_recognises_the_give_up_note() {
        assert!(CLEAN_LAST_ERROR.contains(&format!("'{GIVEN_UP_PREFIX}%'")));
        assert!(given_up_note(&["x-2 (boom)".into()]).unwrap().starts_with(GIVEN_UP_PREFIX));
    }

    /// B1 (review of #1166): the access token is refreshed only when it is
    /// close to expiry. If that margin is shorter than a tick can run, the
    /// token expires mid-walk and every remaining get 401s. The worker's
    /// margin must outlast a whole tick with room to spare.
    #[test]
    fn token_refresh_margin_outlasts_a_tick() {
        // Two rows on one mailbox share its pacer, so a tick's gets can take
        // twice their own paced time.
        let paced_gets = TICK_BUDGET.get_interval * TICK_BUDGET.max_gets as u32 * 2;
        // The shared limiter paces list calls at 5/s; ingest time is extra.
        let lists = Duration::from_millis(200) * TICK_BUDGET.max_list_pages as u32;
        let tick = paced_gets + lists;
        assert!(
            WORKER_TOKEN_REFRESH_MARGIN >= tick * 3,
            "refresh margin {WORKER_TOKEN_REFRESH_MARGIN:?} must be well over a tick's ~{tick:?}"
        );
    }

    /// The binding limit is `total_query_cost`: 6,000 units a minute per
    /// Google user, about 50 units per `messages.get(format=full)`. Pacing is
    /// per MAILBOX (one shared slot sequence per Google user, see
    /// [`pace_mailbox`]), so this is the rate for every row walking one
    /// mailbox together, not per row.
    #[test]
    fn production_budget_stays_well_under_the_per_user_quota() {
        let gets_per_minute_per_mailbox = 60_000.0 / TICK_BUDGET.get_interval.as_millis() as f64;
        assert!(
            gets_per_minute_per_mailbox <= 90.0,
            "paced gets per mailbox: {gets_per_minute_per_mailbox}/min"
        );
        let units = gets_per_minute_per_mailbox * f64::from(GMAIL_GET_QUERY_COST);
        assert!(
            units <= f64::from(GMAIL_QUERY_COST_PER_USER_PER_MINUTE) * 0.75,
            "one mailbox's paced gets must leave a quarter of total_query_cost for the push path: {units}/min"
        );
    }

    /// Two rows can walk one mailbox: a personal row and an org member row
    /// connecting the same Google user. They must share one pacer and one
    /// limiter key, or each paces as if it had the quota to itself.
    #[test]
    fn the_rate_key_is_per_google_user_not_per_account() {
        use crate::connectors::google_gmail::rate_key;
        let personal = Uuid::new_v4();
        let org = Uuid::new_v4();
        assert_eq!(rate_key(personal, "alex@Example.com"), rate_key(org, " alex@example.com "));
        assert_ne!(rate_key(personal, "a@example.com"), rate_key(personal, "b@example.com"));
        assert!(!rate_key(personal, "alex@example.com").contains("alex"), "no address in the key");
        assert_eq!(rate_key(personal, ""), format!("google_gmail:{personal}"), "no email: per account, as before");
    }

    /// Two walks on one mailbox at once draw from one sequence of slots.
    #[tokio::test(start_paused = true)]
    async fn one_mailbox_paces_its_gets_across_concurrent_walks() {
        let interval = Duration::from_millis(700);
        let key = format!("pacer-test-{}", Uuid::new_v4());
        let other = format!("pacer-test-{}", Uuid::new_v4());
        let start = Instant::now();
        let walk = |k: String| async move {
            let mut at = Vec::new();
            for _ in 0..3 {
                pace_mailbox(&k, interval).await;
                at.push(Instant::now() - start);
            }
            at
        };
        let (a, b, c) = tokio::join!(walk(key.clone()), walk(key.clone()), walk(other));
        let mut shared: Vec<Duration> = a.into_iter().chain(b).collect();
        shared.sort();
        for pair in shared.windows(2) {
            assert!(pair[1] - pair[0] >= interval, "same mailbox, gets {pair:?} closer than {interval:?}");
        }
        assert!(shared[5] >= interval * 5, "six gets on one mailbox take five intervals: {shared:?}");
        assert!(c[2] < interval * 3, "another mailbox is not held up: {c:?}");
    }

    /// The pacing interval is derived from the measured quota, not tuned by
    /// hand.
    #[test]
    fn get_interval_is_derived_from_the_quota_constants() {
        assert_eq!(GMAIL_GETS_PER_MINUTE_CEILING, 120);
        assert_eq!(TARGET_GETS_PER_MINUTE, 90);
        assert_eq!(GET_INTERVAL, Duration::from_millis(667));
        assert_eq!(TICK_BUDGET.get_interval, GET_INTERVAL);
    }

    /// The claim is renewed more often than a tick's gets run out, and the
    /// time between renewals is far inside the claim's 15 minutes, so a
    /// single long page cannot outlast it.
    #[test]
    fn the_claim_renewal_interval_is_far_inside_the_claim_ttl() {
        let every = TICK_BUDGET.renew_claim_every_gets;
        assert!(every > 0 && every < TICK_BUDGET.max_gets, "renew every {every} gets");
        let between_renewals = TICK_BUDGET.get_interval * every as u32;
        assert!(between_renewals < Duration::from_secs(15 * 60) / 10, "{between_renewals:?}");
    }

    #[test]
    fn given_up_ids_keep_the_newest() {
        let ids: Vec<String> = (0..MAX_GIVEN_UP_IDS + 5).map(|i| format!("m{i}")).collect();
        let kept = capped(&ids, MAX_GIVEN_UP_IDS);
        assert_eq!(kept.len(), MAX_GIVEN_UP_IDS);
        assert_eq!(kept.first().map(String::as_str), Some("m5"));
        assert_eq!(kept.last(), ids.last());
        assert_eq!(capped(&ids[..3], MAX_GIVEN_UP_IDS), ids[..3].to_vec());
    }
}
