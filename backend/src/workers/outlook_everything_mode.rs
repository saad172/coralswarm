//! Outlook everything-mode sync worker (m365 enterprise train PR 3.4).
//!
//! The whole-mailbox pull half of the contract's everything-mode carve-out
//! (`docs/connectors/CONNECTOR_CONTRACT.md` §4/§7, amended conformance test
//! #9, §6). `routes::outlook_everything_mode` owns the opt-in toggle
//! (`outlook_everything_mode`, migration `0192`/`0070`); this worker is the
//! ONLY code that actually performs the pull, driven entirely by that
//! table's `enabled` rows — never a default, never lazily created here.
//!
//! # Deliberately a SIBLING of `workers::microsoft_calendar_sync`, not a
//! merge into it
//!
//! Both workers ride the same tenant-loop harness and follow the same
//! per-owner sweep shape, but they stay two files for a real reason, not
//! just PR-scope hygiene: `microsoft_calendar_sync`'s passes
//! (`connectors::microsoft_calendar::fetch_pending`) are calendar-only and
//! STAY calendar-only — this worker calls
//! `connectors::microsoft::fetch_everything_mode_mail` instead, a dedicated
//! (non-trait) function, specifically so enabling everything mode can never
//! change what the EXISTING calendar worker's tick does. Merging mail into
//! `sync()` would couple two independently-cadenced, independently-gated
//! features (default-on calendar vs. opt-in mail) behind one trait call.
//!
//! # Opt-in discovery, not default-on discovery — the load-bearing
//! difference from `microsoft_calendar_sync`
//!
//! `microsoft_calendar_sync` discovers its work set by enumerating EVERY
//! member holding a Microsoft token (control-plane `oauth_tokens`) and
//! lazily upserting a `sync_enabled = TRUE` row the first time it sees one —
//! correct for a default-on feature. Everything mode is the opposite shape:
//! a row exists ONLY once `routes::outlook_everything_mode::
//! set_everything_mode` has been called by the mailbox's own owner, so this
//! worker's discovery is a direct read of `outlook_everything_mode WHERE
//! enabled = true` — no `oauth_tokens` cross-reference, no lazy upsert, and
//! (this is the part conformance test #9(b) pins) a mailbox with no row here
//! is never swept, full stop.
//!
//! # Both planes are genuinely reachable — unlike `microsoft_calendar_sync`'s
//! consumer no-op
//!
//! `microsoft_calendar_sync`'s `TenantId::Consumer` arm is a deliberate
//! structural no-op (personal-plane Microsoft tokens are always
//! `TokenOwner::AccountWide`, so its `Member`-token discovery query can never
//! find anything there — see that module's own doc). Everything mode has no
//! such structural emptiness: a personal-plane user can enable it for their
//! own (`AccountWide`-token) mailbox exactly as an org member can for their
//! own (`Member`-token) one, and the plan's brief requires this worker
//! "reachable on the personal plane" — not merely via a manual HTTP trigger,
//! but swept by this very tick. So:
//!
//! * **`TenantId::Org(org_id)`** ([`tick_org`]): resolves the tenant's one
//!   account (`resolve_tenant_account`, the same control-plane
//!   `tenant_databases` lookup `microsoft_calendar_sync`/`graph_subscriptions`
//!   use), then reads `outlook_everything_mode` directly off the TENANT pool
//!   — a single-account, already-scoped query, no cross-account discovery
//!   problem to solve (ADR-0007: one org tenant DB, one account).
//! * **`TenantId::Consumer`** ([`tick_consumer`]): a genuine cross-account
//!   sweep across every personal account on the shared consumer database —
//!   the SAME problem `workers::google_watch_renewal`'s consumer tick solves
//!   for `google_watch_channels`, closed here the identical way: a
//!   `SECURITY DEFINER` bootstrap function (`resolve_outlook_everything_
//!   mode_due()`, migration `0192`) returns bare `(account_id,
//!   owner_user_id, graph_id, claim_id)` ids for every enabled row, and each
//!   is RE-READ (never trusted as-is — TOCTOU/staleness) inside its own
//!   `begin_tenant_system_scoped` transaction by [`owner_state`], the same
//!   function [`tick_org`]'s per-owner sync also calls (`claim_id` is the
//!   one exception re-read does NOT re-derive: it is this row's fencing
//!   token, threaded through unchanged — see this module's own "Fencing
//!   token" doc section). See migration `0192`'s own header and
//!   `0181_google_watch_channels_bootstrap_function.sql`'s header for the
//!   full incident this pattern avoids.
//!
//! # Ingest destination
//!
//! `graph_id` is never re-resolved here — it is read verbatim from the
//! `outlook_everything_mode` row, which `set_everything_mode` populated (and
//! keeps current on every re-enable) via `resolve_primary_graph(..,
//! IngestCaller::Attributable(owner))` at opt-in time. That is the entire
//! enforcement of contract condition (b) ("content lands exclusively in that
//! owner's private ocean") on the ingest side: there is no OTHER graph_id
//! this worker could reach for. Org ticks build a genuine
//! `TenancyDecision::Org(TenantContext)` (never `Tenant::legacy` with a
//! swapped pool — see `workers::graph_ingest`'s own doc for why that
//! distinction is load-bearing for `routes::ingest::ingest_atom`'s
//! reachability check); consumer ticks build `Tenant::legacy(state)`
//! directly, the same pattern `workers::graph_ingest`'s own `TenantId::
//! Consumer` arm uses.
//!
//! # Resumable, bounded pages — contract §6 test #6 discipline
//!
//! `connectors::microsoft::fetch_everything_mode_mail` wraps `/me/messages`
//! pagination bounded to `BACKFILL_SCAN_CAP` (2000) per call, exactly the
//! CS-118 workflow-backfill helper. `outlook_everything_mode.resume_cursor`
//! persists Graph's `@odata.nextLink` across ticks when a page is capped
//! (`complete = false`); a `complete = true` page clears the cursor, so the
//! NEXT tick starts back at the newest message — `atoms`' own `(graph_id,
//! source_type, source_id)` uniqueness makes re-ingesting an
//! already-captured message a cheap no-op, exactly the reasoning `docs/
//! connectors/outlook.md`'s calendar-cursor-gap paragraph already gives for
//! the same re-fetch shape.
//!
//! **A page with a per-item ingest failure never moves this cursor**
//! (F2 fix, CodeRabbit round 2 on PR #700 — the train's established
//! invariant, first fixed for this exact class of bug in Train 2 #673/#676).
//! [`sync_owner`] routes `record_progress`'s `(next_cursor, complete)`
//! arguments through [`progress_cursor`]: any failed item in the batch keeps
//! the ORIGINAL `resume_cursor` and forces `complete = false`, so the next
//! tick retries the SAME page rather than silently skipping the failed
//! message.
//!
//! # Atomic claim — discovery is also a lock (F1 fix, CodeRabbit round 2 on
//! # PR #700)
//!
//! Both discovery paths — [`enabled_owners_for_account`] (org plane) and
//! `resolve_outlook_everything_mode_due()` (migration `0192`, consumer
//! plane) — CLAIM the rows they return, not just read them:
//! `outlook_everything_mode.claimed_until` is stamped `now() + interval '15
//! minutes'` in the same `UPDATE ... FOR UPDATE SKIP LOCKED ... RETURNING`
//! that selects eligible rows (`enabled = true`, `claimed_until` null or
//! expired). Without this, two backend processes sweeping concurrently could
//! both discover the same due owner, and the later of their two
//! [`record_progress`] writes would silently overwrite a NEWER
//! `resume_cursor` with a stale one — a correctness bug, not merely wasted
//! work (contrast `microsoft_calendar_sync_state`, whose sibling worker
//! deliberately has NO claim column: that table carries no cursor to
//! corrupt, so a duplicate discovery there is only a wasted re-fetch of the
//! same cursorless `$top=20` window — see that migration's own header;
//! closed by fix/cal-deletions, `connectors::microsoft_calendar`).
//! [`record_progress`]/[`mark_failed_no_progress`] release the claim
//! (`claimed_until = NULL`) once a tick's work on the row ends in SUCCESS,
//! and back it off (`claimed_until = now() + interval '30 minutes'`) once it
//! ends in FAILURE (release-on-success / backoff-on-failure — corrected in
//! CodeRabbit round 3, F2: an unconditional `NULL` clear on failure let a
//! permanently-failing owner, whose `last_synced_at` never advances, sort to
//! the front of discovery's `ORDER BY last_synced_at ASC NULLS FIRST` and be
//! reclaimed on literally the next tick — a handful of such owners could
//! monopolize every `OWNERS_PER_TICK` batch and starve every healthy owner
//! behind them. See [`record_progress`]'s own doc for the full incident and
//! why backoff, not a `GREATEST`-based reordering, was the fix). An
//! abandoned claim (crashed process, mid-tick, before either function runs)
//! simply expires after its original 15-minute claim TTL either way.
//!
//! # Fencing token — claim IDENTITY, not just claim DEADLINE (F3 fix,
//! # CodeRabbit round 4 on PR #700)
//!
//! `claimed_until` alone is a deadline, not an identity: a worker that
//! claims a row, stalls past its own 15-minute TTL, and only THEN calls
//! [`record_progress`]/[`mark_failed_no_progress`] was, before this fix,
//! indistinguishable from the CURRENT rightful claimant to those two
//! functions' `WHERE account_id = $1 AND owner_user_id = $2` — so a stale
//! worker A's write could silently overwrite a newer worker B's cursor/error
//! state after B had already reclaimed the same row past A's expired TTL,
//! and A's failure-path 30-minute backoff could even stomp B's still-ACTIVE
//! claim. Migration `0192`'s `claim_id UUID` column closes this: both
//! claiming queries — [`enabled_owners_for_account`] and
//! `resolve_outlook_everything_mode_due()` — stamp a fresh
//! `gen_random_uuid()` into `claim_id` in the SAME statement that sets
//! `claimed_until`, and return it. That token is threaded through
//! [`sync_owner`] to [`record_progress`]/[`mark_failed_no_progress`], both
//! of which now add `AND claim_id = $n` to their `WHERE` clause. A stale
//! worker's token no longer matches once the row has been reclaimed (a
//! fresh random UUID every claim), so its write affects zero rows instead of
//! clobbering state — the caller logs a warn (`"stale claim, another worker
//! owns this row"`) and moves on; a zero-row update is never treated as
//! success. See migration `0192`'s own header, "claim_id", for the full
//! incident.
//!
//! # Cadence
//!
//! [`POLL_INTERVAL`] mirrors `workers::microsoft_calendar_sync::
//! POLL_INTERVAL` (30 minutes) — the established cadence for Microsoft Graph
//! background sync in this codebase. Like calendar's `calendarView/delta`
//! cursor (fix/cal-deletions), this worker's cursor makes each tick forward
//! progress through a large mailbox rather than re-seeing the same window.

use std::sync::Arc;
use std::time::Duration;

use sqlx::PgPool;
use uuid::Uuid;

use crate::connectors::{self, ConnectorError, MicrosoftConnector, TokenOwner};
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
#[cfg(not(feature = "e2e"))]
pub const POLL_INTERVAL: Duration = Duration::from_secs(30 * 60);
#[cfg(feature = "e2e")]
pub const POLL_INTERVAL: Duration = Duration::from_secs(5);

// ── Account resolution (org tenant) ───────────────────────────────────────────

/// Identical resolution to `workers::microsoft_calendar_sync::
/// resolve_tenant_account` (kept as its own copy per that function's own doc
/// comment: duplicating one two-line query is cheaper than a shared
/// dependency between independent connector workers).
async fn resolve_tenant_account(control_pool: &PgPool, org_id: &str) -> Result<Uuid, String> {
    sqlx::query_scalar::<_, Uuid>("SELECT account_id FROM tenant_databases WHERE clerk_org_id = $1")
        .bind(org_id)
        .fetch_optional(control_pool)
        .await
        .map_err(|e| e.to_string())?
        .ok_or_else(|| format!("no tenant_databases row for org {org_id}"))
}

// ── Discovery ──────────────────────────────────────────────────────────────────

/// Per-tick owner budget (CodeRabbit round 1): a tick must have a bounded
/// worst case — each owner can cost up to a full `BACKFILL_SCAN_CAP` page of
/// serialized ingests, so an unbounded owner sweep would hold the harness's
/// sweep slot for O(N × page) and starve every tenant behind it. Ordering by
/// `last_synced_at ASC NULLS FIRST` makes the budget self-rotating with no
/// cursor state: the least-recently-synced owners go first, and owners the
/// budget skipped this tick sort to the front of the next one.
const OWNERS_PER_TICK: i64 = 5;

/// Up to [`OWNERS_PER_TICK`] `owner_user_id`s with `enabled = true` for
/// `account_id`, least-recently-synced first, CLAIMED atomically off `pool`
/// (the TENANT pool for an org tick — a single-account, already-scoped
/// query, no cross-account discovery needed: ADR-0007).
///
/// **Atomic claim (CodeRabbit round 2 on PR #700, F1).** This used to be a
/// plain `SELECT ... LIMIT $2` — with more than one backend process, two
/// concurrent org ticks could both discover the SAME owner, and the later of
/// their two [`record_progress`] writes would silently overwrite a NEWER
/// `resume_cursor` with a stale one. The mirror image of migration `0192`'s
/// `resolve_outlook_everything_mode_due()` claim (see that migration's own
/// header for the full incident), applied inline here rather than via a
/// `SECURITY DEFINER` function: this is already a single-account,
/// already-scoped query, so there is no cross-account boundary for a bypass
/// function to cross — the `UPDATE ... FROM (SELECT ... FOR UPDATE SKIP
/// LOCKED) RETURNING` claims up to [`OWNERS_PER_TICK`] eligible rows
/// (`enabled = true`, `claimed_until` null or expired) and stamps
/// `claimed_until = now() + interval '15 minutes'` in the SAME statement.
/// [`record_progress`]/[`mark_failed_no_progress`] release the claim on
/// success or back it off 30 minutes on failure once this tick's work on the
/// row is done (release-on-success / backoff-on-failure, F2 — see
/// [`record_progress`]'s own doc); an abandoned claim (crashed process)
/// simply expires after its original 15 minutes either way.
///
/// **Fencing token (F3 fix, CodeRabbit round 4 on PR #700).** The same
/// claiming `UPDATE` also stamps a fresh `claim_id = gen_random_uuid()` and
/// returns it alongside `owner_user_id` — see this module's own "Fencing
/// token" doc section and migration `0192`'s header, "claim_id", for the
/// full stale-worker-overwrite incident this closes. Callers must thread the
/// returned `claim_id` through to [`record_progress`]/
/// [`mark_failed_no_progress`], which fence their writes on it.
async fn enabled_owners_for_account(pool: &PgPool, account_id: Uuid) -> Result<Vec<(Uuid, Uuid)>, ConnectorError> {
    let mut tx = begin_tenant_system_scoped(pool, account_id)
        .await
        .map_err(|e| ConnectorError::Internal(format!("enabled_owners_for_account scoped tx: {e}")))?;
    let rows: Vec<(Uuid, Uuid)> = sqlx::query_as(
        "UPDATE outlook_everything_mode oem \
         SET claimed_until = now() + interval '15 minutes', claim_id = gen_random_uuid() \
         FROM ( \
             SELECT id FROM outlook_everything_mode \
             WHERE account_id = $1 AND enabled = true \
               AND (claimed_until IS NULL OR claimed_until < now()) \
             ORDER BY last_synced_at ASC NULLS FIRST \
             LIMIT $2 \
             FOR UPDATE SKIP LOCKED \
         ) claimable \
         WHERE oem.id = claimable.id \
         RETURNING oem.owner_user_id, oem.claim_id",
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
/// (`resolve_outlook_everything_mode_due()`, migration `0192`) reports
/// enabled AND CLAIMS (F1 fix, CodeRabbit round 2 on PR #700 — the function
/// is now an atomic `UPDATE ... RETURNING` claim, not a read-only `SELECT`;
/// see that migration's own header). Bare ids only — the row's
/// `graph_id`/`resume_cursor` are re-derived by [`owner_state`] inside a
/// properly scoped transaction before use, never trusted from this bypass
/// read (see this module's own doc and migration `0192`'s header).
/// [`record_progress`]/[`mark_failed_no_progress`] release or back off the
/// claim this function takes once the tick's work on the row is done (see
/// [`record_progress`]'s own doc, F2).
///
/// **Fencing token (F3 fix, CodeRabbit round 4 on PR #700).** The bootstrap
/// function's claiming `UPDATE` also stamps a fresh `claim_id =
/// gen_random_uuid()` and returns it — see this module's own "Fencing
/// token" doc section and migration `0192`'s header, "claim_id", for the
/// full stale-worker-overwrite incident this closes. Callers must thread the
/// returned `claim_id` through to [`record_progress`]/
/// [`mark_failed_no_progress`], which fence their writes on it.
async fn due_accounts(control_pool: &PgPool) -> Result<Vec<(Uuid, Uuid, Uuid)>, ConnectorError> {
    let rows: Vec<(Uuid, Uuid, Uuid, Uuid)> = sqlx::query_as(
        "SELECT account_id, owner_user_id, graph_id, claim_id FROM resolve_outlook_everything_mode_due()",
    )
    .fetch_all(control_pool)
    .await
    .map_err(|e| ConnectorError::Internal(format!("due_accounts: {e}")))?;
    Ok(rows.into_iter().map(|(a, o, _g, c)| (a, o, c)).collect())
}

/// This owner's LIVE `(graph_id, resume_cursor)`, re-read (and re-verified
/// `enabled = true`) inside a properly scoped transaction — the single
/// TOCTOU-safe read both [`tick_org`] (after its own bulk listing) and
/// [`tick_consumer`] (after the bootstrap bypass) route through before doing
/// anything with Graph. `None` covers disabled, no row, or a race where the
/// row changed between discovery and here — never an error, just nothing to
/// do this tick.
///
/// **`None` leaves the discovery claim held until its 15-minute TTL — this
/// is deliberately NOT the F1 gap (CodeRabbit round 3 on PR #700).** Every
/// path that lands here (row deleted, or `enabled` flipped false between
/// discovery and this re-read) already fails discovery's own `WHERE
/// enabled = true` predicate, so the row cannot be reclaimed on the NEXT
/// tick regardless of whether `claimed_until` is cleared — clearing it here
/// would be dead code, not a fix. (A user who disables and immediately
/// re-enables everything mode inside that same 15-minute window is the one
/// edge case that pays for this: the re-enabled row briefly reads as
/// claimed. That is a bounded, self-healing staleness, not the unbounded
/// "credentials permanently missing" monopolization F1/F2 target — see
/// [`sync_owner`]'s credentials-missing branch, which does NOT return
/// `None`/`Ok(false)` for exactly this reason.)
async fn owner_state(
    pool: &PgPool,
    account_id: Uuid,
    owner_user_id: Uuid,
) -> Result<Option<(Uuid, Option<String>)>, ConnectorError> {
    let mut tx = begin_tenant_system_scoped(pool, account_id)
        .await
        .map_err(|e| ConnectorError::Internal(format!("owner_state scoped tx: {e}")))?;
    let row: Option<(Uuid, Option<String>)> = sqlx::query_as(
        "SELECT graph_id, resume_cursor FROM outlook_everything_mode \
         WHERE account_id = $1 AND owner_user_id = $2 AND enabled = true",
    )
    .bind(account_id)
    .bind(owner_user_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("owner_state query: {e}")))?;
    tx.rollback().await.ok();
    Ok(row)
}

// ── Progress / error bookkeeping ────────────────────────────────────────────────

/// A page was fetched (regardless of whether every item ingested cleanly) —
/// persist the resumable cursor and either clear or set the error state.
/// `complete = true` clears the cursor (next tick restarts from the newest
/// message — see this module's doc on why that's cheap and correct).
///
/// **Callers, not this function, decide what `next_cursor`/`complete` to
/// pass when a page had per-item ingest failures (F2 fix, CodeRabbit round 2
/// on PR #700)** — see [`progress_cursor`] and its call site in
/// [`sync_owner`]. This function only ever writes what it is told.
///
/// **Release-on-success, backoff-on-failure (F1 fix, CodeRabbit round 2;
/// backoff shape corrected in round 3, F2, on PR #700).** A clean page
/// (`ingest_error: None`) releases the atomic claim [`enabled_owners_for_
/// account`]/`resolve_outlook_everything_mode_due()` took immediately —
/// `claimed_until = NULL` — rather than leaving it to expire after its
/// 15-minute window.
///
/// A page recorded WITH an error does the opposite: it stamps `claimed_until
/// = now() + interval '30 minutes'` instead of clearing it. Round 2 cleared
/// it to `NULL` here too, which was itself a bug (F2): with discovery's
/// `ORDER BY last_synced_at ASC NULLS FIRST`, a permanently-failing owner
/// never advances `last_synced_at` (only the `None` arm above does), so it
/// sorts to the FRONT of every batch forever — clearing its claim let it be
/// reclaimed on literally the next tick, meaning a handful of permanently-
/// failing owners could monopolize every `OWNERS_PER_TICK`/`LIMIT 5` batch
/// and starve healthy owners from ever syncing. Backing off `claimed_until`
/// instead fixes this WITHOUT touching `last_synced_at` or discovery's
/// ordering/index at all: the claim predicate (`claimed_until IS NULL OR
/// claimed_until < now()`) is a `WHERE` filter that runs before the `ORDER
/// BY`/`LIMIT`, so a backed-off row is excluded from the claimable set for
/// 30 minutes regardless of where it would have sorted — success semantics
/// (`last_synced_at`, ordering, index usage) are completely untouched. This
/// was chosen over reordering by `GREATEST(last_synced_at, last_error_at)`
/// or similar precisely because it changes NOTHING about the success path's
/// proven `ORDER BY last_synced_at ASC NULLS FIRST` behavior — only the
/// failure path's claim predicate.
///
/// **Fenced on `claim_id` (F3 fix, CodeRabbit round 4 on PR #700).** Both
/// branches below add `AND claim_id = $n` to their `WHERE` clause, using the
/// `claim_id` the caller received from [`enabled_owners_for_account`]/
/// `resolve_outlook_everything_mode_due()` at discovery time — see this
/// module's own "Fencing token" doc section and migration `0192`'s header,
/// "claim_id", for the full incident: without this, a worker that stalls
/// past its 15-minute `claimed_until` and only reaches this function AFTER
/// another worker has reclaimed the same row would silently overwrite that
/// newer worker's cursor/error state (or, on the failure branch, even stomp
/// its still-ACTIVE claim). A write that matches zero rows means this claim
/// is stale; it is logged as a warn and dropped, never treated as success.
async fn record_progress(
    pool: &PgPool,
    account_id: Uuid,
    owner_user_id: Uuid,
    claim_id: Uuid,
    next_cursor: Option<&str>,
    complete: bool,
    ingest_error: Option<&str>,
) {
    let Ok(mut tx) = begin_tenant_system_scoped(pool, account_id).await else {
        return;
    };
    let cursor = if complete { None } else { next_cursor };
    let result = match ingest_error {
        None => {
            sqlx::query(
                "UPDATE outlook_everything_mode \
                 SET resume_cursor = $3, claimed_until = NULL, last_synced_at = now(), \
                     last_error = NULL, last_error_at = NULL, updated_at = now() \
                 WHERE account_id = $1 AND owner_user_id = $2 AND claim_id = $4",
            )
            .bind(account_id)
            .bind(owner_user_id)
            .bind(cursor)
            .bind(claim_id)
            .execute(&mut *tx)
            .await
        }
        Some(message) => {
            // F2 fix: back off the claim 30 minutes instead of clearing it —
            // see this function's own doc above for why.
            sqlx::query(
                "UPDATE outlook_everything_mode \
                 SET resume_cursor = $3, claimed_until = now() + interval '30 minutes', last_error = $4, \
                     last_error_at = now(), updated_at = now() \
                 WHERE account_id = $1 AND owner_user_id = $2 AND claim_id = $5",
            )
            .bind(account_id)
            .bind(owner_user_id)
            .bind(cursor)
            .bind(message.chars().take(500).collect::<String>())
            .bind(claim_id)
            .execute(&mut *tx)
            .await
        }
    };
    // F3 fix (CodeRabbit round 4 on PR #700): a zero-row update means this
    // claim is stale — another worker has already reclaimed the row — and
    // must NOT be treated as success. See this function's own doc, "Fenced
    // on claim_id", for the full incident.
    match result {
        Ok(res) if res.rows_affected() == 0 => {
            tracing::warn!(
                %account_id, %owner_user_id, %claim_id,
                "outlook_everything_mode: record_progress found a stale claim_id — another worker owns this row now, dropping write"
            );
        }
        Ok(_) => {}
        Err(e) => {
            tracing::warn!(%account_id, %owner_user_id, %claim_id, error = %e, "outlook_everything_mode: record_progress write failed");
        }
    }
    let _ = tx.commit().await;
}

/// A failure BEFORE any page was fetched (creds/token/Graph-request error) —
/// records the error without touching `resume_cursor`, so an interrupted
/// tick resumes exactly where it left off rather than restarting.
///
/// Backs `claimed_until` off 30 minutes (F1 fix, CodeRabbit round 2;
/// corrected from an unconditional `NULL` clear to this backoff in round 3,
/// F2, on PR #700 — see [`record_progress`]'s own doc for the full
/// monopolization incident this avoids): a pre-fetch failure still ends this
/// tick's ownership of the row, but immediately clearing the claim let a
/// permanently-failing owner (whose `last_synced_at` never advances, since
/// this function never sets it) be reclaimed on literally the next tick,
/// sorting to the front of `ORDER BY last_synced_at ASC NULLS FIRST` forever
/// and starving healthy owners out of the `OWNERS_PER_TICK` batch. The
/// 30-minute backoff still resumes promptly relative to `POLL_INTERVAL` (30
/// minutes) — this is not a punitive delay, just enough to stop a hot-loop
/// failure from crowding out every other owner.
///
/// **Fenced on `claim_id` (F3 fix, CodeRabbit round 4 on PR #700).** Same
/// fencing as [`record_progress`] — see that function's own doc and this
/// module's "Fencing token" doc section. This is the branch the finding's
/// worst case runs through: without the `AND claim_id = $n` fence, a stalled
/// worker's 30-minute backoff write could stamp OVER a different worker's
/// still-ACTIVE claim on the same row, effectively hijacking it. A
/// zero-row-affected write means the claim is stale and is dropped with a
/// warn, never treated as success.
async fn mark_failed_no_progress(pool: &PgPool, account_id: Uuid, owner_user_id: Uuid, claim_id: Uuid, error: &str) {
    let Ok(mut tx) = begin_tenant_system_scoped(pool, account_id).await else {
        return;
    };
    let result = sqlx::query(
        "UPDATE outlook_everything_mode \
         SET claimed_until = now() + interval '30 minutes', last_error = $3, last_error_at = now(), updated_at = now() \
         WHERE account_id = $1 AND owner_user_id = $2 AND claim_id = $4",
    )
    .bind(account_id)
    .bind(owner_user_id)
    .bind(error.chars().take(500).collect::<String>())
    .bind(claim_id)
    .execute(&mut *tx)
    .await;
    // F3 fix (CodeRabbit round 4 on PR #700): a zero-row update means this
    // claim is stale — another worker has already reclaimed the row — and
    // must NOT stamp a backoff over that worker's still-ACTIVE claim. See
    // this function's own doc, "Fenced on claim_id", for the full incident.
    match result {
        Ok(res) if res.rows_affected() == 0 => {
            tracing::warn!(
                %account_id, %owner_user_id, %claim_id,
                "outlook_everything_mode: mark_failed_no_progress found a stale claim_id — another worker owns this row now, dropping write"
            );
        }
        Ok(_) => {}
        Err(e) => {
            tracing::warn!(%account_id, %owner_user_id, %claim_id, error = %e, "outlook_everything_mode: mark_failed_no_progress write failed");
        }
    }
    let _ = tx.commit().await;
}

/// Pure decision logic, identical shape and same tests-worth-having as
/// `workers::microsoft_calendar_sync::batch_ingest_outcome` (kept as its own
/// copy — see this module's own doc on why a sibling file, not a shared
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

/// What to persist as this tick's `(next_cursor, complete)` for
/// [`record_progress`], given whether ANY item in the just-fetched page
/// failed to ingest (F2 fix, CodeRabbit round 2 on PR #700).
///
/// A page with one or more failed items must retry from the SAME resume
/// point next tick — `original_resume_cursor` unchanged, `complete = false`
/// — never `page_next_cursor`/`page_complete`. The train's established
/// invariant (Train 2 #673/#676 fixed this exact class of bug for the
/// analogous Slack/Outlook thread loops): advancing (or, on a `complete`
/// page, CLEARING) the cursor past a batch with a failure permanently skips
/// the failed message — the next tick would start from a resume point that
/// is already past it, and `atoms`' `(graph_id, source_type, source_id)`
/// dedup only protects re-ingesting something already seen, not recovering
/// something never ingested at all. Only a fully clean page is allowed to
/// move the cursor forward (or clear it, on Graph's own last page).
///
/// Extracted as a pure function — rather than tested only via [`sync_owner`]
/// end-to-end — for the same reason [`batch_ingest_outcome`] is: forcing a
/// REAL `ingest_atom` failure needs infrastructure this test tree does not
/// have, but the cursor decision itself needs neither Postgres nor Graph to
/// verify.
fn progress_cursor(
    original_resume_cursor: Option<&str>,
    page_next_cursor: Option<&str>,
    page_complete: bool,
    any_item_failed: bool,
) -> (Option<String>, bool) {
    if any_item_failed {
        (original_resume_cursor.map(str::to_string), false)
    } else {
        (page_next_cursor.map(str::to_string), page_complete)
    }
}

// ── Sync ─────────────────────────────────────────────────────────────────────

/// Sync one owner's mailbox. `Ok(true)` = a page was attempted (whether or
/// not it was empty, or had per-item ingest failures — see
/// [`batch_ingest_outcome`]), `Ok(false)` = skipped (disabled, no row, or a
/// race since discovery — retried next tick), `Err` = a failure BEFORE any
/// page was fetched (token/Graph error), recorded via
/// [`mark_failed_no_progress`] by the caller.
///
/// `claim_id` is the fencing token the caller ([`tick_org`]/[`tick_consumer`])
/// received from [`enabled_owners_for_account`]/
/// `resolve_outlook_everything_mode_due()` at discovery time — threaded
/// through, unchanged, to this call's [`record_progress`] write (and to the
/// caller's own [`mark_failed_no_progress`] call on `Err`). See this
/// module's own "Fencing token" doc section.
#[allow(clippy::too_many_arguments)]
async fn sync_owner(
    pool: &PgPool,
    control_pool: &PgPool,
    state: &Arc<AppState>,
    account_id: Uuid,
    owner_user_id: Uuid,
    claim_id: Uuid,
    token_owner: TokenOwner,
    org_id: Option<&str>,
) -> Result<bool, ConnectorError> {
    let Some((graph_id, resume_cursor)) = owner_state(pool, account_id, owner_user_id).await? else {
        return Ok(false);
    };
    // The member's pause or off switch for Outlook mail, before any token or
    // mailbox read. The walk's saved position stays, so a resumed source goes on
    // from it.
    if crate::connectors::catalog::pause::is_paused(
        control_pool, account_id, owner_user_id, crate::connectors::catalog::pause::MS_MAIL,
    ).await {
        return Ok(false);
    }

    // F1 fix (CodeRabbit round 3 on PR #700): this USED TO be `return
    // Ok(false)` — a silent no-work skip. Unlike `owner_state` returning
    // `None` just above (moot to backoff: a disabled/deleted row is already
    // excluded from the NEXT discovery by its own `WHERE enabled = true`,
    // claim or no claim), this row is still `enabled = true` and WILL be
    // rediscovered — so a silent `Ok(false)` left `claimed_until` held from
    // [`enabled_owners_for_account`]/`resolve_outlook_everything_mode_due()`'s
    // claim with no error recorded and no backoff, relying on nothing but
    // the raw 15-minute claim TTL to eventually free the slot. Missing
    // credentials counts as a failure for backoff purposes (see
    // [`mark_failed_no_progress`]'s own doc, F2): returning `Err` here routes
    // it through the SAME caller-side `mark_failed_no_progress` call every
    // other pre-fetch failure already takes, so it gets both `last_error`
    // visibility and the 30-minute retry backoff instead of silently sitting
    // on the claim.
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
            "outlook_everything_mode",
        )
        .await
    {
        return Err(ConnectorError::NotConfigured(format!(
            "owner {owner_user_id} is no longer a member of this account"
        )));
    }

    let Some(mut creds) = load_creds_bg(control_pool, &dek_plane, account_id, "microsoft", token_owner).await? else {
        return Err(ConnectorError::NotConfigured(format!(
            "no microsoft credentials for owner {owner_user_id}"
        )));
    };

    let connector = MicrosoftConnector;
    connector.refresh_if_needed(&state.http_client, &mut creds).await?;

    // Persist a refreshed token BEFORE the Graph fetch (CodeRabbit round 1):
    // fetching first meant a fetch failure discarded the newer token, and
    // the stored one could expire before a later successful sync ever got
    // around to persisting a replacement.
    if let Some(refreshed) = creds.refreshed.as_ref() {
        if let Err(e) = store_token_bundle(control_pool, &dek_plane, account_id, "microsoft", token_owner, refreshed).await {
            tracing::warn!(
                %account_id, %owner_user_id, status = ?e.status(),
                "outlook_everything_mode: failed to persist refreshed token"
            );
        }
    }

    let page =
        connectors::microsoft::fetch_everything_mode_mail(&state.http_client, &creds.access_token, resume_cursor.as_deref())
            .await?;

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

    let total = page.items.len();
    let mut failed_ids: Vec<String> = Vec::new();
    for item in page.items {
        let source_id = item.source_id.clone();
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
        let source_namespace = format!("microsoft:connection:{account_id}:{owner_user_id}");
        let origin = crate::source_origin::SourceOrigin::native("outlook","message","outlook_everything_mode").expect("static origin identifiers");
        let resp = crate::ingest::submit(crate::ingest::IngestRequest {
            origin: Some(&origin),
            ..crate::ingest::IngestRequest::private(crate::ingest::Member {
                provider_namespace: Some(&source_namespace),
                ..crate::ingest::Member::new(state, &tenant, &user_ctx, req)
            })
        }).await;
        if let Err(refused) = resp {
            tracing::warn!(
                %account_id, %owner_user_id, error = %refused, source_id = %source_id,
                "outlook_everything_mode: ingest_atom failed for a message"
            );
            failed_ids.push(source_id);
        }
    }

    // F2 fix (CodeRabbit round 2 on PR #700): a page with any per-item
    // ingest failure must retry from the SAME resume point next tick, never
    // advance (or, on a `complete` page, clear) past it — see
    // `progress_cursor`'s own doc for why advancing would permanently skip
    // the failed message.
    let (cursor_to_persist, complete_to_persist) = progress_cursor(
        resume_cursor.as_deref(),
        page.next_cursor.as_deref(),
        page.complete,
        !failed_ids.is_empty(),
    );
    record_progress(
        pool,
        account_id,
        owner_user_id,
        claim_id,
        cursor_to_persist.as_deref(),
        complete_to_persist,
        batch_ingest_outcome(total, &failed_ids).as_deref(),
    )
    .await;
    Ok(true)
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
        let key = format!("microsoft:{account_id}:{owner_user_id}");
        let outcome = crate::graph_rate_limit::scoped(
            key,
            sync_owner(
                &t.pool,
                control_pool,
                state,
                account_id,
                owner_user_id,
                claim_id,
                TokenOwner::Member(owner_user_id),
                Some(org_id),
            ),
        )
        .await;
        match outcome {
            Ok(true) => processed += 1,
            Ok(false) => {}
            Err(e) => {
                tracing::warn!(
                    %account_id, %owner_user_id, error = %e,
                    "outlook_everything_mode: org member sync failed"
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
        let key = format!("microsoft:{account_id}:{owner_user_id}");
        let outcome = crate::graph_rate_limit::scoped(
            key,
            sync_owner(
                &t.pool,
                control_pool,
                state,
                account_id,
                owner_user_id,
                claim_id,
                TokenOwner::AccountWide,
                None,
            ),
        )
        .await;
        match outcome {
            Ok(true) => processed += 1,
            Ok(false) => {}
            Err(e) => {
                tracing::warn!(
                    %account_id, %owner_user_id, error = %e,
                    "outlook_everything_mode: personal-plane sync failed"
                );
                mark_failed_no_progress(&t.pool, account_id, owner_user_id, claim_id, &e.to_string()).await;
            }
        }
    }
    Ok(if processed > 0 { Tick::worked(processed) } else { Tick::idle() })
}

/// One everything-mode sync pass. Driven by `workers::tenant_harness::
/// run_multi_tenant` as a sweep worker (`HarnessConfig::sweep_worker(_,
/// POLL_INTERVAL)`) — see this module's own doc for why BOTH arms below do
/// real work, unlike `microsoft_calendar_sync::tick`'s consumer no-op.
pub async fn tick(t: &WorkerTenant, control_pool: &PgPool, state: &Arc<AppState>) -> Result<Tick, String> {
    match &t.tenant {
        TenantId::Consumer => tick_consumer(t, control_pool, state).await,
        TenantId::Org(org_id) => tick_org(t, control_pool, state, org_id).await,
    }
}

/// Pure unit tests for [`progress_cursor`] (F2 fix, CodeRabbit round 2 on PR
/// #700) — no Postgres needed, same rationale as [`batch_ingest_outcome`]'s
/// own doc comment for why the decision itself is worth testing in
/// isolation from `sync_owner`'s live-Graph-dependent surroundings.
#[cfg(test)]
mod progress_cursor_tests {
    use super::progress_cursor;

    #[test]
    fn clean_incomplete_page_advances_to_the_pages_own_next_cursor() {
        assert_eq!(
            progress_cursor(Some("old"), Some("new"), false, false),
            (Some("new".to_string()), false)
        );
    }

    #[test]
    fn clean_complete_page_clears_by_reporting_no_cursor() {
        // `complete = true` is what makes `record_progress` clear the
        // cursor; a clean complete page passes the page's own values
        // through unchanged.
        assert_eq!(progress_cursor(Some("old"), None, true, false), (None, true));
    }

    #[test]
    fn any_failure_on_an_incomplete_page_keeps_the_original_cursor_and_forces_incomplete() {
        assert_eq!(
            progress_cursor(Some("old"), Some("new"), false, true),
            (Some("old".to_string()), false),
            "a failed item must retry the SAME page next tick, never advance to the page's next_cursor"
        );
    }

    #[test]
    fn any_failure_on_a_complete_page_keeps_the_original_cursor_and_forces_incomplete() {
        // The page Graph itself reported as its last one — `page_complete =
        // true` — must NOT clear the cursor when the batch had a failure;
        // clearing would restart from the newest message and permanently
        // skip the failed item once newer messages push it out of range.
        assert_eq!(
            progress_cursor(Some("old"), None, true, true),
            (Some("old".to_string()), false)
        );
    }

    #[test]
    fn any_failure_with_no_original_cursor_retries_from_the_top_not_forward() {
        // First-ever tick (resume_cursor was None) whose page had a
        // failure: must stay None/incomplete, never adopt the page's own
        // next_cursor.
        assert_eq!(progress_cursor(None, Some("new"), false, true), (None, false));
    }
}

/// pg-gated integration tests: proves discovery + progress bookkeeping
/// against a REAL Postgres database, split the same way `workers::
/// microsoft_calendar_sync::microsoft_calendar_sync_pg_tests` splits — a
/// control-plane group (the `resolve_outlook_everything_mode_due()`
/// bootstrap) and a tenant-plane group (`owner_state`/`enabled_owners_for_
/// account`/`record_progress`/`mark_failed_no_progress`). Deliberately does
/// NOT exercise `tick`/`sync_owner` end-to-end — that would need a live
/// Microsoft Graph credential, the same posture `microsoft_calendar_sync`'s
/// own pg-test module doc states for the identical reason.
///
///   ORGT_TEST_BASE_URL=postgres://postgres:postgres@localhost:15544/postgres \
///     cargo test -p coralswarm-backend --lib "workers::outlook_everything_mode::outlook_everything_mode_pg_tests" \
///     -- --test-threads=1
#[cfg(test)]
mod outlook_everything_mode_pg_tests {
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
            "INSERT INTO outlook_everything_mode(account_id, owner_user_id, graph_id, enabled) \
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
        let db = "m365_34_due_accounts";
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
        let db = "m365_34_due_accounts_claim";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool = control_pool(&base, db).await;

        let founder = seed_user(&pool).await;
        let account_id = seed_account(&pool, founder).await;
        let owner = seed_user(&pool).await;
        let graph_id = seed_graph(&pool, account_id, owner).await;
        seed_everything_mode(&pool, account_id, owner, graph_id, true).await;

        // F1 fix (CodeRabbit round 2 on PR #700): `resolve_outlook_everything_
        // mode_due()` is now an atomic CLAIM, not a read-only SELECT. A
        // sequential second call within the 15-minute claim window is an
        // acceptable approximation of two concurrent processes racing the
        // same row — true overlapping-transaction concurrency is awkward to
        // force deterministically from a single test task.
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
        let db = "m365_34_due_accounts_empty";
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

    /// A member who paused (or turned off) the Outlook source: the whole-mailbox
    /// walk does nothing for them. Unpaused, the same call gets as far as the
    /// credentials, which a test has none of.
    #[tokio::test]
    async fn a_paused_outlook_source_is_not_walked_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping a_paused_outlook_source_is_not_walked_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "m365_paused_source";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool = control_pool(&base, db).await;
        let owner = seed_user(&pool).await;
        let account_id = seed_account(&pool, owner).await;
        let graph_id = seed_graph(&pool, account_id, owner).await;
        seed_everything_mode(&pool, account_id, owner, graph_id, true).await;
        let state = Arc::new(crate::test_app_state_owned_with_db(pool.clone(), None));

        let unpaused = sync_owner(&pool, &pool, &state, account_id, owner, Uuid::new_v4(), TokenOwner::Member(owner), None).await;
        assert!(unpaused.is_err(), "an unpaused member gets past the gate, to the missing credentials: {unpaused:?}");

        sqlx::query(
            "INSERT INTO connector_sources (account_id, owner_user_id, provider, source, enabled, paused_at) \
             VALUES ($1, $2, 'microsoft', 'mail', true, now())",
        )
        .bind(account_id)
        .bind(owner)
        .execute(&pool)
        .await
        .unwrap();
        let paused = sync_owner(&pool, &pool, &state, account_id, owner, Uuid::new_v4(), TokenOwner::Member(owner), None).await;
        assert!(matches!(paused, Ok(false)), "{paused:?}");

        teardown(&base, db, pool).await;
    }

    #[tokio::test]
    async fn owner_state_is_none_for_a_fresh_mailbox_and_some_once_enabled_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping owner_state_is_none_for_a_fresh_mailbox_and_some_once_enabled_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "m365_34_owner_state";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool = tenant_pool(&base, db, "org_owner_state").await;

        let account_id = Uuid::new_v4();
        let owner = Uuid::new_v4();
        let graph_id = seed_graph(&pool, account_id, owner).await;

        // No row at all yet (fresh connection): `None`.
        assert!(owner_state(&pool, account_id, owner).await.unwrap().is_none());

        seed_everything_mode(&pool, account_id, owner, graph_id, true).await;
        let state = owner_state(&pool, account_id, owner).await.unwrap();
        assert_eq!(state, Some((graph_id, None)));

        // Disabling makes it invisible to discovery again, without deleting
        // the row (the toggle route itself is the only writer of `enabled`).
        sqlx::query("UPDATE outlook_everything_mode SET enabled = false WHERE account_id = $1 AND owner_user_id = $2")
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
        let db = "m365_34_record_progress";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool = tenant_pool(&base, db, "org_record_progress").await;

        let account_id = Uuid::new_v4();
        let owner = Uuid::new_v4();
        let graph_id = seed_graph(&pool, account_id, owner).await;
        seed_everything_mode(&pool, account_id, owner, graph_id, true).await;

        // Claim the row first (F3 fix, CodeRabbit round 4 on PR #700):
        // record_progress is fenced on claim_id, so callers must have a real
        // claim before it will write.
        let claimed = enabled_owners_for_account(&pool, account_id).await.unwrap();
        let claim_id = claimed[0].1;

        // Incomplete page: cursor persists so the next tick resumes.
        record_progress(&pool, account_id, owner, claim_id, Some("https://graph/next"), false, None).await;
        let (cursor, synced): (Option<String>, Option<chrono::DateTime<chrono::Utc>>) = sqlx::query_as(
            "SELECT resume_cursor, last_synced_at FROM outlook_everything_mode \
             WHERE account_id = $1 AND owner_user_id = $2",
        )
        .bind(account_id)
        .bind(owner)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(cursor.as_deref(), Some("https://graph/next"));
        assert!(synced.is_some());

        // Complete page: cursor clears, even though this call also passes a
        // (now-irrelevant) next_cursor value — `complete` wins. Same
        // claim_id: a successful record_progress does not itself change
        // claim_id, only claimed_until.
        record_progress(&pool, account_id, owner, claim_id, Some("https://graph/stale"), true, None).await;
        let (cursor,): (Option<String>,) =
            sqlx::query_as("SELECT resume_cursor FROM outlook_everything_mode WHERE account_id = $1 AND owner_user_id = $2")
                .bind(account_id)
                .bind(owner)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert!(cursor.is_none(), "a complete page must clear the cursor, not carry a stale one forward");

        teardown(&base, db, pool).await;
    }

    #[tokio::test]
    async fn record_progress_with_a_failed_item_keeps_the_original_cursor_on_an_incomplete_page_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping record_progress_with_a_failed_item_keeps_the_original_cursor_on_an_incomplete_page_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "m365_34_f2_incomplete";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool = tenant_pool(&base, db, "org_f2_incomplete").await;

        let account_id = Uuid::new_v4();
        let owner = Uuid::new_v4();
        let graph_id = seed_graph(&pool, account_id, owner).await;
        seed_everything_mode(&pool, account_id, owner, graph_id, true).await;

        // Claim the row first (F3 fix, CodeRabbit round 4 on PR #700):
        // record_progress is fenced on claim_id.
        let claimed = enabled_owners_for_account(&pool, account_id).await.unwrap();
        let claim_id = claimed[0].1;

        // A prior, clean tick established this resume point.
        record_progress(&pool, account_id, owner, claim_id, Some("https://graph/checkpoint-1"), false, None).await;

        // F2 fix (CodeRabbit round 2 on PR #700): this tick fetched an
        // INCOMPLETE page starting from that checkpoint, with a next_cursor
        // of its own — but one item in it failed to ingest. `sync_owner`
        // routes record_progress's arguments through `progress_cursor`,
        // exactly as exercised here. Same claim_id: the prior clean call's
        // success branch doesn't touch the claim_id column, only
        // claimed_until.
        let (cursor, complete) = progress_cursor(
            Some("https://graph/checkpoint-1"),
            Some("https://graph/checkpoint-2"),
            false,
            true,
        );
        record_progress(
            &pool,
            account_id,
            owner,
            claim_id,
            cursor.as_deref(),
            complete,
            Some("1 of 3 messages failed to ingest"),
        )
        .await;

        let (resume_cursor, last_error, claimed_until): (Option<String>, Option<String>, Option<chrono::DateTime<chrono::Utc>>) =
            sqlx::query_as(
                "SELECT resume_cursor, last_error, claimed_until FROM outlook_everything_mode \
                 WHERE account_id = $1 AND owner_user_id = $2",
            )
            .bind(account_id)
            .bind(owner)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(
            resume_cursor.as_deref(),
            Some("https://graph/checkpoint-1"),
            "a failed item must retry the SAME page next tick, not advance to the page's own next_cursor"
        );
        assert_eq!(last_error.as_deref(), Some("1 of 3 messages failed to ingest"));
        // F2 fix (CodeRabbit round 3 on PR #700): `record_progress`'s error
        // branch must back the claim OFF, not clear it — see this module's
        // `record_progress` doc for the monopolization incident this avoids.
        assert!(
            claimed_until.is_some_and(|t| t > chrono::Utc::now()),
            "record_progress's error branch must back claimed_until off into the future, not clear it"
        );

        teardown(&base, db, pool).await;
    }

    #[tokio::test]
    async fn record_progress_with_a_failed_item_keeps_the_original_cursor_on_a_complete_page_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping record_progress_with_a_failed_item_keeps_the_original_cursor_on_a_complete_page_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "m365_34_f2_complete";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool = tenant_pool(&base, db, "org_f2_complete").await;

        let account_id = Uuid::new_v4();
        let owner = Uuid::new_v4();
        let graph_id = seed_graph(&pool, account_id, owner).await;
        seed_everything_mode(&pool, account_id, owner, graph_id, true).await;

        // Claim the row first (F3 fix, CodeRabbit round 4 on PR #700):
        // record_progress is fenced on claim_id.
        let claimed = enabled_owners_for_account(&pool, account_id).await.unwrap();
        let claim_id = claimed[0].1;

        record_progress(&pool, account_id, owner, claim_id, Some("https://graph/checkpoint-1"), false, None).await;

        // Graph itself reports this as its LAST page (`page_complete =
        // true`), but a message in it still failed to ingest — the cursor
        // must NOT clear, or the failed message is permanently skipped once
        // newer messages push it out of Graph's default ordering window.
        let (cursor, complete) = progress_cursor(Some("https://graph/checkpoint-1"), None, true, true);
        record_progress(
            &pool,
            account_id,
            owner,
            claim_id,
            cursor.as_deref(),
            complete,
            Some("1 of 1 messages failed to ingest"),
        )
        .await;

        let (resume_cursor,): (Option<String>,) = sqlx::query_as(
            "SELECT resume_cursor FROM outlook_everything_mode WHERE account_id = $1 AND owner_user_id = $2",
        )
        .bind(account_id)
        .bind(owner)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            resume_cursor.as_deref(),
            Some("https://graph/checkpoint-1"),
            "a complete page with a failure must NOT clear the cursor — clearing would restart from the \
             newest message and permanently skip the failed item"
        );

        teardown(&base, db, pool).await;
    }

    #[tokio::test]
    async fn enabled_owners_for_account_claims_rows_so_a_second_call_excludes_them_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping enabled_owners_for_account_claims_rows_so_a_second_call_excludes_them_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "m365_34_owners_claim";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool = tenant_pool(&base, db, "org_owners_claim").await;

        let account_id = Uuid::new_v4();
        let owner_a = Uuid::new_v4();
        let owner_b = Uuid::new_v4();
        let graph_a = seed_graph(&pool, account_id, owner_a).await;
        let graph_b = seed_graph(&pool, account_id, owner_b).await;
        seed_everything_mode(&pool, account_id, owner_a, graph_a, true).await;
        seed_everything_mode(&pool, account_id, owner_b, graph_b, true).await;

        // F1 fix (CodeRabbit round 2 on PR #700), tenant-plane discovery —
        // same sequential-call approximation as `due_accounts`' own claim
        // test above.
        let first = enabled_owners_for_account(&pool, account_id).await.unwrap();
        let mut first_owners: Vec<Uuid> = first.iter().map(|(o, _)| *o).collect();
        first_owners.sort();
        let mut expected = vec![owner_a, owner_b];
        expected.sort();
        assert_eq!(first_owners, expected);
        assert!(
            first.iter().all(|(_, claim_id)| *claim_id != Uuid::nil()),
            "the claiming UPDATE must stamp a real (non-nil) fencing token on every claimed row"
        );

        let second = enabled_owners_for_account(&pool, account_id).await.unwrap();
        assert!(
            second.is_empty(),
            "rows claimed by the first call must not be returned again by a second call inside the claim window"
        );

        teardown(&base, db, pool).await;
    }

    #[tokio::test]
    async fn record_progress_clears_the_claim_so_the_row_is_claimable_again_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping record_progress_clears_the_claim_so_the_row_is_claimable_again_pg: set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "m365_34_claim_release";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool = tenant_pool(&base, db, "org_claim_release").await;

        let account_id = Uuid::new_v4();
        let owner = Uuid::new_v4();
        let graph_id = seed_graph(&pool, account_id, owner).await;
        seed_everything_mode(&pool, account_id, owner, graph_id, true).await;

        let claimed = enabled_owners_for_account(&pool, account_id).await.unwrap();
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].0, owner);
        let claim_id = claimed[0].1;
        // Claimed now — a second call finds nothing.
        assert!(enabled_owners_for_account(&pool, account_id).await.unwrap().is_empty());

        // Finishing the tick (success or failure) releases the claim
        // immediately rather than waiting out the full 15-minute window.
        record_progress(&pool, account_id, owner, claim_id, None, true, None).await;
        let reclaimed = enabled_owners_for_account(&pool, account_id).await.unwrap();
        assert_eq!(reclaimed.len(), 1);
        assert_eq!(
            reclaimed[0].0, owner,
            "record_progress must clear claimed_until so the row is claimable again"
        );
        assert_ne!(
            reclaimed[0].1, claim_id,
            "a fresh claim must mint a NEW fencing token, never reuse the previous holder's"
        );

        teardown(&base, db, pool).await;
    }

    #[tokio::test]
    async fn mark_failed_no_progress_leaves_the_cursor_untouched_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping mark_failed_no_progress_leaves_the_cursor_untouched_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "m365_34_mark_failed";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool = tenant_pool(&base, db, "org_mark_failed").await;

        let account_id = Uuid::new_v4();
        let owner = Uuid::new_v4();
        let graph_id = seed_graph(&pool, account_id, owner).await;
        seed_everything_mode(&pool, account_id, owner, graph_id, true).await;

        // Claim the row first (F3 fix, CodeRabbit round 4 on PR #700): both
        // record_progress and mark_failed_no_progress are fenced on
        // claim_id. A successful record_progress doesn't touch the claim_id
        // column, only claimed_until, so the same token stays valid for the
        // mark_failed_no_progress call below.
        let claimed = enabled_owners_for_account(&pool, account_id).await.unwrap();
        let claim_id = claimed[0].1;
        record_progress(&pool, account_id, owner, claim_id, Some("https://graph/checkpoint"), false, None).await;

        // A pre-fetch failure (token/Graph error) must not disturb the
        // already-persisted resume point — an interrupted tick resumes
        // exactly where it left off.
        mark_failed_no_progress(&pool, account_id, owner, claim_id, "graph 503").await;
        let (cursor, err): (Option<String>, Option<String>) = sqlx::query_as(
            "SELECT resume_cursor, last_error FROM outlook_everything_mode \
             WHERE account_id = $1 AND owner_user_id = $2",
        )
        .bind(account_id)
        .bind(owner)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(cursor.as_deref(), Some("https://graph/checkpoint"));
        assert_eq!(err.as_deref(), Some("graph 503"));

        teardown(&base, db, pool).await;
    }

    #[tokio::test]
    async fn mark_failed_no_progress_backs_off_the_claim_until_the_backoff_window_expires_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping mark_failed_no_progress_backs_off_the_claim_until_the_backoff_window_expires_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "m365_34_mark_failed_backoff";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool = tenant_pool(&base, db, "org_mark_failed_backoff").await;

        let account_id = Uuid::new_v4();
        let owner = Uuid::new_v4();
        let graph_id = seed_graph(&pool, account_id, owner).await;
        seed_everything_mode(&pool, account_id, owner, graph_id, true).await;

        let claimed = enabled_owners_for_account(&pool, account_id).await.unwrap();
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].0, owner);
        let claim_id = claimed[0].1;

        // F2 fix (CodeRabbit round 3 on PR #700, corrected from round 2's
        // unconditional release): a pre-fetch failure (token/Graph error)
        // still ends this tick's ownership of the row, but must NOT make it
        // immediately reclaimable — that let a permanently-failing owner
        // monopolize every batch (see this function's own doc). It backs
        // `claimed_until` off 30 minutes instead.
        mark_failed_no_progress(&pool, account_id, owner, claim_id, "graph 503").await;
        let immediately_reclaimed = enabled_owners_for_account(&pool, account_id).await.unwrap();
        assert!(
            immediately_reclaimed.is_empty(),
            "a failed owner must NOT be re-claimable immediately after mark_failed_no_progress — \
             that would let a permanently-failing owner monopolize every batch"
        );

        // Simulate the 30-minute backoff window having elapsed (inject a
        // past `claimed_until` directly — the same sequential-call
        // approximation the other claim tests in this module already use
        // for "time has passed").
        sqlx::query(
            "UPDATE outlook_everything_mode SET claimed_until = now() - interval '1 minute' \
             WHERE account_id = $1 AND owner_user_id = $2",
        )
        .bind(account_id)
        .bind(owner)
        .execute(&pool)
        .await
        .unwrap();

        let reclaimed_after_backoff = enabled_owners_for_account(&pool, account_id).await.unwrap();
        assert_eq!(reclaimed_after_backoff.len(), 1);
        assert_eq!(
            reclaimed_after_backoff[0].0, owner,
            "once the backoff window has elapsed, the row must be claimable again"
        );
        assert_ne!(
            reclaimed_after_backoff[0].1, claim_id,
            "a fresh claim must mint a NEW fencing token, never reuse the previous holder's"
        );

        teardown(&base, db, pool).await;
    }

    #[tokio::test]
    async fn failing_owners_do_not_monopolize_the_batch_and_a_healthy_owner_still_gets_claimed_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping failing_owners_do_not_monopolize_the_batch_and_a_healthy_owner_still_gets_claimed_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "m365_34_no_monopolize";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool = tenant_pool(&base, db, "org_no_monopolize").await;

        let account_id = Uuid::new_v4();

        // OWNERS_PER_TICK failing owners — each has `last_synced_at` still
        // NULL (mark_failed_no_progress never sets it), so under round 2's
        // unconditional NULL-on-failure release, EVERY one of these would
        // re-satisfy the claim predicate immediately and sort to the front
        // of `ORDER BY last_synced_at ASC NULLS FIRST` forever, filling
        // every `OWNERS_PER_TICK`-sized batch on their own.
        let mut failing_owners = Vec::new();
        for _ in 0..OWNERS_PER_TICK {
            let owner = Uuid::new_v4();
            let graph_id = seed_graph(&pool, account_id, owner).await;
            seed_everything_mode(&pool, account_id, owner, graph_id, true).await;
            failing_owners.push(owner);
        }
        let claimed = enabled_owners_for_account(&pool, account_id).await.unwrap();
        assert_eq!(claimed.len(), failing_owners.len(), "sanity: all seeded owners claimed on the first sweep");
        for (owner, claim_id) in &claimed {
            mark_failed_no_progress(&pool, account_id, *owner, *claim_id, "graph 503").await;
        }

        // A healthy owner, never synced, seeded AFTER the failures — the
        // scenario the fix targets: a fresh/healthy owner must not be
        // crowded out by owners that keep failing forever.
        let healthy_owner = Uuid::new_v4();
        let healthy_graph = seed_graph(&pool, account_id, healthy_owner).await;
        seed_everything_mode(&pool, account_id, healthy_owner, healthy_graph, true).await;

        let next_batch = enabled_owners_for_account(&pool, account_id).await.unwrap();
        assert_eq!(next_batch.len(), 1);
        assert_eq!(
            next_batch[0].0, healthy_owner,
            "F2 fix: the OWNERS_PER_TICK failing owners are all still inside their 30-minute backoff \
             window, so the healthy owner must be the only one claimed — none of the failing owners \
             should still be eligible to fill (let alone monopolize) the batch"
        );

        teardown(&base, db, pool).await;
    }

    #[tokio::test]
    async fn one_owners_progress_never_touches_a_different_owners_row_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping one_owners_progress_never_touches_a_different_owners_row_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "m365_34_cross_owner_progress";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool = tenant_pool(&base, db, "org_cross_owner_progress").await;

        let account_id = Uuid::new_v4();
        let owner_a = Uuid::new_v4();
        let owner_b = Uuid::new_v4();
        let graph_a = seed_graph(&pool, account_id, owner_a).await;
        seed_everything_mode(&pool, account_id, owner_a, graph_a, true).await;

        // Claim and finish owner_a BEFORE owner_b exists, so the claiming
        // UPDATE below can only ever touch owner_a's row — isolating this
        // from the "which owners does a batch claim" concern the other
        // claim tests already cover, and keeping this test's focus purely
        // on "does one owner's fenced write leak onto another owner's row".
        let claimed = enabled_owners_for_account(&pool, account_id).await.unwrap();
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].0, owner_a);
        let claim_id_a = claimed[0].1;
        record_progress(&pool, account_id, owner_a, claim_id_a, Some("https://graph/a"), false, None).await;

        let graph_b = seed_graph(&pool, account_id, owner_b).await;
        seed_everything_mode(&pool, account_id, owner_b, graph_b, true).await;

        // owner_a's release put its row back in the claimable pool, and
        // owner_b is fresh — both claimable now.
        let owners = enabled_owners_for_account(&pool, account_id).await.unwrap();
        assert_eq!(owners.len(), 2, "owner_a's progress write must not disable/remove owner_b");

        let (cursor_b,): (Option<String>,) =
            sqlx::query_as("SELECT resume_cursor FROM outlook_everything_mode WHERE account_id = $1 AND owner_user_id = $2")
                .bind(account_id)
                .bind(owner_b)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert!(cursor_b.is_none(), "owner_a's cursor write must not leak onto owner_b's row");

        teardown(&base, db, pool).await;
    }

    // ── Fencing token: stale-worker overwrite (F3 fix, CodeRabbit round 4 on
    // PR #700) ────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn record_progress_with_a_stale_claim_id_updates_zero_rows_and_leaves_the_newer_claims_state_intact_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping record_progress_with_a_stale_claim_id_updates_zero_rows_and_leaves_the_newer_claims_state_intact_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "m365_34_f3_record_progress_stale";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool = tenant_pool(&base, db, "org_f3_record_progress_stale").await;

        let account_id = Uuid::new_v4();
        let owner = Uuid::new_v4();
        let graph_id = seed_graph(&pool, account_id, owner).await;
        seed_everything_mode(&pool, account_id, owner, graph_id, true).await;

        // Worker A claims the row (this is the token it will — wrongly, if
        // unfenced — try to write with after it stalls).
        let claimed_a = enabled_owners_for_account(&pool, account_id).await.unwrap();
        assert_eq!(claimed_a.len(), 1);
        let stale_claim_id = claimed_a[0].1;

        // Simulate worker A stalling past its 15-minute claim TTL (the same
        // "inject a past claimed_until" approximation the other claim tests
        // in this module already use for "time has passed").
        sqlx::query(
            "UPDATE outlook_everything_mode SET claimed_until = now() - interval '1 minute' \
             WHERE account_id = $1 AND owner_user_id = $2",
        )
        .bind(account_id)
        .bind(owner)
        .execute(&pool)
        .await
        .unwrap();

        // Worker B reclaims the now-expired row with a NEW fencing token —
        // this is the "newer claim" the finding says a stale worker A must
        // not be able to clobber.
        let claimed_b = enabled_owners_for_account(&pool, account_id).await.unwrap();
        assert_eq!(claimed_b.len(), 1);
        let fresh_claim_id = claimed_b[0].1;
        assert_ne!(fresh_claim_id, stale_claim_id, "sanity: B's claim must mint a different token than A's");

        let before: (Option<String>, Option<chrono::DateTime<chrono::Utc>>, Uuid) = sqlx::query_as(
            "SELECT resume_cursor, claimed_until, claim_id FROM outlook_everything_mode \
             WHERE account_id = $1 AND owner_user_id = $2",
        )
        .bind(account_id)
        .bind(owner)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(before.2, fresh_claim_id, "sanity: the row now carries B's claim_id");

        // Worker A, unaware it was ever reclaimed, finally calls
        // record_progress with its STALE token — the exact sequence the
        // finding describes (F3, CodeRabbit round 4 on PR #700).
        record_progress(&pool, account_id, owner, stale_claim_id, Some("https://graph/stale-A-cursor"), false, None).await;

        let after: (Option<String>, Option<chrono::DateTime<chrono::Utc>>, Uuid) = sqlx::query_as(
            "SELECT resume_cursor, claimed_until, claim_id FROM outlook_everything_mode \
             WHERE account_id = $1 AND owner_user_id = $2",
        )
        .bind(account_id)
        .bind(owner)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            after, before,
            "a record_progress call fenced on a STALE claim_id must update zero rows — B's cursor/ \
             claimed_until/claim_id must be completely untouched by A's stale write"
        );

        teardown(&base, db, pool).await;
    }

    #[tokio::test]
    async fn mark_failed_no_progress_with_a_stale_claim_id_does_not_stamp_a_backoff_over_an_active_foreign_claim_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping mark_failed_no_progress_with_a_stale_claim_id_does_not_stamp_a_backoff_over_an_active_foreign_claim_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "m365_34_f3_mark_failed_stale";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool = tenant_pool(&base, db, "org_f3_mark_failed_stale").await;

        let account_id = Uuid::new_v4();
        let owner = Uuid::new_v4();
        let graph_id = seed_graph(&pool, account_id, owner).await;
        seed_everything_mode(&pool, account_id, owner, graph_id, true).await;

        // Worker A claims, then stalls past its TTL — same setup as the
        // record_progress stale-claim test above.
        let claimed_a = enabled_owners_for_account(&pool, account_id).await.unwrap();
        assert_eq!(claimed_a.len(), 1);
        let stale_claim_id = claimed_a[0].1;
        sqlx::query(
            "UPDATE outlook_everything_mode SET claimed_until = now() - interval '1 minute' \
             WHERE account_id = $1 AND owner_user_id = $2",
        )
        .bind(account_id)
        .bind(owner)
        .execute(&pool)
        .await
        .unwrap();

        // Worker B reclaims — its claim is genuinely ACTIVE (claimed_until
        // ~15 minutes in the future) when A's stale write below happens.
        let claimed_b = enabled_owners_for_account(&pool, account_id).await.unwrap();
        assert_eq!(claimed_b.len(), 1);
        let fresh_claim_id = claimed_b[0].1;
        assert_ne!(fresh_claim_id, stale_claim_id);

        let before: (Option<String>, chrono::DateTime<chrono::Utc>, Uuid) = sqlx::query_as(
            "SELECT last_error, claimed_until, claim_id FROM outlook_everything_mode \
             WHERE account_id = $1 AND owner_user_id = $2",
        )
        .bind(account_id)
        .bind(owner)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(before.2, fresh_claim_id);
        assert!(before.1 > chrono::Utc::now(), "sanity: B's claim must be genuinely active (in the future)");

        // Worker A's pre-fetch failure finally lands with its STALE token —
        // the worst case in the finding: an unfenced write here would stamp
        // a 30-minute backoff RIGHT OVER B's still-active claim, effectively
        // hijacking a row B currently owns.
        mark_failed_no_progress(&pool, account_id, owner, stale_claim_id, "worker A: graph 503 (stale)").await;

        let after: (Option<String>, chrono::DateTime<chrono::Utc>, Uuid) = sqlx::query_as(
            "SELECT last_error, claimed_until, claim_id FROM outlook_everything_mode \
             WHERE account_id = $1 AND owner_user_id = $2",
        )
        .bind(account_id)
        .bind(owner)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            after, before,
            "mark_failed_no_progress fenced on a STALE claim_id must update zero rows — it must NOT stamp \
             a backoff (or an error message) over another worker's still-ACTIVE claim"
        );

        teardown(&base, db, pool).await;
    }
}
