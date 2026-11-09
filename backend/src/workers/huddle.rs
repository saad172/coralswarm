//! Slack huddle notes capture worker.
//!
//! Consumes `jobs::JobKind::SlackHuddleCapture` jobs enqueued by
//! `routes::slack_events::slack_events` the moment
//! `routes::slack_events::huddle_capture_trigger` fires — MEASURED ground
//! truth, see `backend/tests/fixtures/slack_huddle/README.md`:
//!
//!   * Trigger: `subtype == "message_changed"` AND
//!     `event.message.room.transcript_file_id` (the AI-notes CANVAS file id —
//!     a Slack naming trap, NOT the transcript) is present. It appears on
//!     exactly one event per huddle.
//!   * The transcript itself (`files[0].huddle_transcript_file_id`) is
//!     UNFETCHABLE — measured 302 redirect to the workspace web app for both
//!     the bot token and the user token. There is no code anywhere in this
//!     module that tries to fetch it.
//!   * The canvas IS fetchable: `files.info` on the canvas id resolves a
//!     fresh `url_private`, and `GET` on it with the BOT token returns 200
//!     `text/html`.
//!
//! # Why a job queue, not a bare `tokio::spawn`
//!
//! The trigger fires EXACTLY ONCE per huddle — there is no natural Slack
//! retry for it (contrast the old reaction-based opt-in, which a user could
//! always re-react to). If ingest failed inside a spawned task, the huddle's
//! content was lost forever. `jobs::queue` already gives every other worker
//! at-least-once delivery with bounded exponential backoff
//! ([`jobs::queue::fail`]) — this worker is that same generic retry plumbing,
//! not a resurrection of the deleted `slack_huddle_captures` QUEUE (which was
//! a user-facing opt-in lifecycle with cancellation and a 48h timeout; see
//! this module's git history for that design).
//!
//! # `slack_huddle_captures` is now a terminal-outcome LEDGER, not a queue
//!
//! Migration `0102_slack_huddle_captures_ledger` (tenant `0014`) dropped
//! `status`/`error`/`opted_by` and added `skip_reason`. One row per huddle
//! SEEN, recording what happened — there are no lifecycle transitions:
//!
//!   * **Ingested**: `processed_at` is set, `skip_reason` is NULL.
//!   * **Skipped, with a reason** (`skip_reason` set, `processed_at` NULL):
//!     the founder's "skip, but record why" decision — a skip must never be
//!     silent. See [`CaptureOutcome::Skipped`]'s call sites in
//!     [`capture_huddle`] for the full set of reasons.
//!
//! A row is never allowed to satisfy neither predicate — migration 0102/0014
//! backfills every pre-existing row and then enforces
//! `CHECK (processed_at IS NOT NULL OR skip_reason IS NOT NULL)` at the
//! database level, so this is not just an invariant asserted by this doc
//! comment. A skip that is really a TEMPORARY, ours-to-fix condition (e.g.
//! the org fan-out having no mappable destination yet — capability C5 —
//! see [`CaptureError::Retryable`] below) must never be recorded as a
//! `Skipped` ledger row at all: `ON CONFLICT DO NOTHING` means such a row can
//! never be overwritten once the underlying gap is closed, so it would
//! permanently and silently suppress that huddle forever.
//!
//! The pre-existing `UNIQUE (account_id, channel_id, huddle_thread_ts)` is
//! exactly the idempotency key this still needs, even with no natural Slack
//! retry: `message_changed` fires repeatedly for one huddle (3 observed in
//! the fixtures), and a later edit can re-present the same
//! `transcript_file_id`. [`capture_already_recorded`] checks this BEFORE any
//! work starts; a row for a SKIPPED huddle suppresses re-fetching exactly
//! like an ingested one.
//!
//! **A row is written ONLY on a terminal outcome.** A genuinely retryable
//! failure (Slack API blip, a transient DB error) does NOT write a ledger
//! row at all — [`process_job`] routes it through [`jobs::queue::fail`]
//! instead, so the job's own bounded backoff gets a chance to self-heal
//! without ever risking a permanent, un-recorded "we tried once and gave up"
//! row that a later legitimate retry could never get past.
//!
//! # Two pools, tenant loop (org-slack step 4)
//!
//! This worker joins `workers::tenant_harness::run_multi_tenant`, exactly like
//! `workers::slack_ingest`: `JobKind::SlackHuddleCapture` jobs land on
//! WHICHEVER plane the enqueue resolved, so this worker's `dequeue` must run
//! on that SAME tenant's `jobs` table (`t.pool`), never a single shared pool.
//!
//! **This paragraph used to claim the enqueue side "moved planes in the SAME
//! commit". It had not.** Step 4 moved this worker onto the tenant loop and
//! left `routes::slack_events::enqueue_huddle_capture` writing to the control
//! pool, so an org account's job was claimed by the harness's CONSUMER entry
//! (`queue::dequeue` filters by KIND only) and captured with `t.tenant =
//! Consumer` while `account_plane()` said `Org` — the org fan-out then read
//! `connector_identities` off control, found none, and filed a
//! legitimate-looking `Skipped`. Every huddle in an org workspace was silently
//! lost, and this doc asserted the opposite, so a reader had no reason to look.
//! The enqueue is plane-explicit and fans out as of the fix; the claim above is
//! true now. Kept as a note because a doc that lies about an invariant is worse
//! than no doc — see `enqueue_huddle_capture` for the full account.
//!
//! The Slack bot token is a deliberate exception to "content lives on the
//! tenant" — `oauth_tokens` stays CONTROL-ONLY (beside `connector_installs`;
//! founder decision), so [`get_slack_bot_token`]/[`get_slack_bot_token_pub`]
//! always read `control_pool`, while every content write (`atoms`,
//! `slack_huddle_captures`) goes to `t.pool`. `crypto::resolve_dek_for_tenant`
//! (not the Consumer-only `resolve_dek`) resolves the atom-blob DEK on
//! whichever plane `t.tenant` names, and `coralswarm_core::r2::blob_key_for`
//! (not the 2-segment-only `personal_blob_key`) picks the blob key scheme —
//! see [`ingest_huddle_notes`].
//!
//! # Tenant RLS scoping (org-slack step 4, carried forward from Phase W.2)
//!
//! Every table this worker writes on the content pool — `slack_huddle_captures`,
//! `atoms` — sits on either migration `0083_consumer_rls`'s (consumer plane)
//! or `migrations_tenant/0009_tenant_rls`'s (org plane) policy roster, so
//! every content-pool DB statement below runs inside
//! `tenancy::scope::begin_tenant_system_scoped`, not the plain
//! `begin_account_scoped` this file used before this worker had an org-tenant
//! plane to serve: `atoms` is on 0009's roster (`member_ocean_isolation`),
//! and `begin_account_scoped` alone sets only `app.account_id`, which that
//! policy's `tenant_rls_visible_graph_ids()` never treats as sufficient for a
//! tenant-wide background sweep with no acting end user — a plain scope would
//! silently write zero rows on an org tenant (the exact "passes as the owner
//! in tests, fails in prod" failure class this connector has hit three times
//! before). `begin_tenant_system_scoped` additionally sets
//! `app.tenant_actor = 'system'`, which is harmless on the consumer plane
//! (0083's policies never read that GUC) — so this is one code path for both
//! planes, not a per-plane branch. Every statement is scoped per-statement
//! (not per-job): processing one capture interleaves DB statements with Slack
//! API calls and R2 uploads, and holding a pooled connection across those
//! would change both pool pressure and this worker's per-statement error
//! handling — same reasoning as [`crate::workers::embed::JobScope`]'s doc.
//! `jobs` itself carries no such policy (every job kind's account boundary is
//! enforced inside its own processing, not by `jobs` row visibility) —
//! `jobs::queue::dequeue` reads across every account ON ONE PLANE in one
//! statement by design, exactly like every other job-queue worker; the plane
//! boundary itself is what the tenant harness now enforces.

use chrono::Utc;
use coralswarm_core::chunker::{chunk, ChunkStrategy};
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row};
use std::time::Duration;
use uuid::Uuid;

use coralswarm_core::oauth_token::decrypt_token;

use crate::jobs::queue::{self, JobRow};
use crate::jobs::JobKind;
use crate::tenancy::scope::{begin_account_scoped, begin_tenant_system_scoped};
use crate::tenancy::TenantId;
use crate::workers::tenant_harness::{Tick, WorkerTenant};

// ── Tuning constants ──────────────────────────────────────────────────────────

/// How many `SlackHuddleCapture` jobs to dequeue per poll cycle.
const JOB_BATCH_SIZE: usize = 4;
/// How long to sleep between polls when the queue is empty — the tenant
/// harness's `base_poll` / idle-backoff floor for this worker.
pub const EMPTY_POLL_SLEEP: Duration = Duration::from_millis(500);

// ── Worker entry point ────────────────────────────────────────────────────────

/// One `SlackHuddleCapture` pass on one tenant's `(pool, store)` pair:
/// dequeue up to [`JOB_BATCH_SIZE`] jobs and process them. Driven by
/// `workers::tenant_harness::run_multi_tenant` as a queue worker
/// (`HarnessConfig::queue_worker`), which owns the loop, the empty-poll
/// sleep, per-tenant error backoff, and shutdown (see that module's doc for
/// the full contract — this worker no longer owns its own loop or shutdown
/// check, exactly like `workers::slack_ingest`).
///
/// `control_pool` is the shared control/consumer pool — needed for the Slack
/// bot token (`oauth_tokens`, control-only) and the account's
/// personal-vs-org plane (`accounts.clerk_org_id`); every other read/write
/// runs on `t.pool`/`t.tenant` (see this module's doc comment).
pub async fn tick(
    t: &WorkerTenant,
    control_pool: &PgPool,
    http_client: &reqwest::Client,
    r2: Option<&coralswarm_core::r2::R2Client>,
) -> Result<Tick, String> {
    let jobs = queue::dequeue(&t.pool, &["SlackHuddleCapture"], JOB_BATCH_SIZE)
        .await
        .map_err(|e| format!("dequeue failed: {e}"))?;

    if jobs.is_empty() {
        return Ok(Tick::idle());
    }

    let n = jobs.len();
    for job in jobs {
        process_job(&t.pool, &t.tenant, control_pool, http_client, r2, job).await;
    }
    Ok(Tick::worked(n))
}

// ── Job payload ────────────────────────────────────────────────────────────────

use crate::connectors::slack_huddle_consent::CaptureRequest as CapturePayload;

#[derive(Debug, thiserror::Error)]
enum HuddleCaptureError {
    #[error("cannot deserialise JobKind from job payload: {0}")]
    InvalidPayload(#[source] serde_json::Error),
    #[error("expected SlackHuddleCapture job kind, got {0:?}")]
    WrongJobKind(JobKind),
}

fn parse_capture_payload(job: &JobRow) -> Result<CapturePayload, HuddleCaptureError> {
    let tagged = serde_json::json!({ "kind": job.kind, "payload": job.payload });
    let kind: JobKind = serde_json::from_value(tagged).map_err(HuddleCaptureError::InvalidPayload)?;

    match kind {
        JobKind::SlackHuddleCapture {
            account_id,
            workspace_id,
            channel_id,
            huddle_thread_ts,
            canvas_file_id,
            participant_history,
            date_start,
            date_end,
        } => Ok(CapturePayload {
            account_id,
            workspace_id,
            channel_id,
            huddle_thread_ts,
            canvas_file_id,
            participant_history,
            date_start,
            date_end,
        }),
        other => Err(HuddleCaptureError::WrongJobKind(other)),
    }
}

// ── Job processing ────────────────────────────────────────────────────────────

/// The terminal outcome of [`capture_huddle`] — either variant is recorded
/// into the `slack_huddle_captures` ledger (see this module's doc comment).
enum CaptureOutcome {
    Ingested { atom_count: usize },
    Skipped(String),
}

/// The failure outcome of [`capture_huddle`] (and the helpers it calls) —
/// makes the retryable-vs-terminal distinction load-bearing (a compile-time
/// choice) instead of a rule that lives only in a doc comment. Every
/// fallible step in the capture path must declare which side it is on:
///
/// * [`CaptureError::Retryable`] — a transient condition (Slack API blip,
///   DB hiccup) that may heal on its own. [`process_job`] routes this
///   through [`queue::fail`] and writes NO ledger row, so the job's own
///   bounded backoff gets a chance to retry.
/// * [`CaptureError::Terminal`] — a PERMANENT condition (revoked Slack app,
///   missing account DEK, a canvas that deterministically converts to empty
///   text) that will observe the exact same failure on every retry.
///   [`process_job`] records this as a `Skipped` outcome in the ledger
///   (`skip_reason` set) — exactly like [`CaptureOutcome::Skipped`] — so the
///   job stops burning its backoff schedule and the failure is never
///   silently lost.
#[derive(Debug, Clone, PartialEq, Eq)]
enum CaptureError {
    Retryable(String),
    Terminal(String),
}

impl CaptureError {
    fn retryable(msg: impl Into<String>) -> Self {
        CaptureError::Retryable(msg.into())
    }

    fn terminal(msg: impl Into<String>) -> Self {
        CaptureError::Terminal(msg.into())
    }

    /// Prepend context to the message while preserving the retryable/terminal
    /// classification — used at call sites that want to describe WHERE a
    /// lower-level error happened without collapsing the type back to a bare
    /// `String` (which is exactly the anti-pattern this enum exists to
    /// prevent).
    fn context(self, prefix: &str) -> Self {
        match self {
            CaptureError::Retryable(m) => CaptureError::Retryable(format!("{prefix}: {m}")),
            CaptureError::Terminal(m) => CaptureError::Terminal(format!("{prefix}: {m}")),
        }
    }

    fn message(&self) -> &str {
        match self {
            CaptureError::Retryable(m) | CaptureError::Terminal(m) => m,
        }
    }
}

impl std::fmt::Display for CaptureError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message())
    }
}

impl From<sqlx::Error> for CaptureError {
    /// Every raw `sqlx::Error` reaching this boundary is a transient DB
    /// condition (connection blip, statement timeout) — never a permanent
    /// business decision, which is always expressed explicitly as
    /// `CaptureError::Terminal` at its own call site, not inferred from an
    /// error type.
    fn from(e: sqlx::Error) -> Self {
        CaptureError::Retryable(e.to_string())
    }
}

/// `pub` (like `workers::slack_name_backfill::process_job`) so integration
/// tests can drive one job against a real Postgres without going through the
/// worker loop.
pub async fn process_job(
    pool: &PgPool,
    tenant: &TenantId,
    control_pool: &PgPool,
    http_client: &reqwest::Client,
    r2: Option<&coralswarm_core::r2::R2Client>,
    job: JobRow,
) {
    let payload = match parse_capture_payload(&job) {
        Ok(p) => p,
        Err(e) => {
            tracing::error!(job_id = %job.id, error = %e, "huddle capture: malformed job payload");
            if let Err(qe) = queue::fail(pool, job.id, &e.to_string()).await {
                tracing::error!(error = %qe, job_id = %job.id, "failed to mark malformed huddle job as failed");
            }
            return;
        }
    };

    // ── Idempotency: a huddle already recorded (ingested OR skipped) is a
    // no-op — see this module's doc comment for why `message_changed`
    // redelivery/re-edits make this necessary despite there being no
    // natural Slack retry for the trigger itself.
    match capture_already_recorded(
        pool,
        payload.account_id,
        &payload.channel_id,
        &payload.huddle_thread_ts,
    )
    .await
    {
        Ok(true) => {
            tracing::debug!(
                account_id = %payload.account_id,
                channel_id = %payload.channel_id,
                huddle_thread_ts = %payload.huddle_thread_ts,
                "huddle capture: already recorded — no-op"
            );
            if let Err(e) = queue::complete(pool, job.id, queue::JobOutcome::NothingToDo).await {
                tracing::warn!(error = %e, job_id = %job.id, "failed to complete already-recorded huddle job");
            }
            return;
        }
        Ok(false) => {}
        Err(e) => {
            tracing::warn!(error = %e, job_id = %job.id, "huddle capture: idempotency check failed — retrying");
            if let Err(qe) = queue::fail(pool, job.id, &e.to_string()).await {
                tracing::error!(error = %qe, job_id = %job.id, "failed to fail huddle job after idempotency-check error");
            }
            return;
        }
    }

    match capture_huddle(pool, tenant, control_pool, http_client, r2, &payload, job.attempts == 0).await {
        Ok(CaptureOutcome::Ingested { atom_count }) => {
            if let Err(e) = record_capture_outcome(
                pool,
                payload.account_id,
                &payload.channel_id,
                &payload.huddle_thread_ts,
                None,
            )
            .await
            {
                // Non-fatal: the atoms are already committed (idempotent via
                // the (graph_id, source_type, source_id) key), so a failure
                // here only risks a harmless re-attempt later, not
                // duplicated content.
                tracing::warn!(error = %e, "huddle capture: failed to record ingested outcome in the ledger");
            }
            tracing::info!(
                account_id = %payload.account_id,
                channel_id = %payload.channel_id,
                huddle_thread_ts = %payload.huddle_thread_ts,
                atom_count,
                "huddle capture: ingested"
            );
            if let Err(e) = queue::complete(pool, job.id, queue::JobOutcome::DidWork).await {
                tracing::warn!(error = %e, job_id = %job.id, "failed to complete huddle capture job");
            }
        }
        Ok(CaptureOutcome::Skipped(reason)) => {
            tracing::info!(
                account_id = %payload.account_id,
                channel_id = %payload.channel_id,
                huddle_thread_ts = %payload.huddle_thread_ts,
                reason = %reason,
                "huddle capture: skipped"
            );
            finish_as_skipped(pool, job.id, &payload, &reason).await;
        }
        // A permanent business condition surfaced as an `Err` rather than an
        // `Ok(CaptureOutcome::Skipped)` — see `CaptureError`'s doc comment.
        // Track B: THIS is a `FailureClass::Structural` CouldNotDo, not a
        // `NothingToDo` — "revoked Slack app" / "missing account DEK" / "file
        // deleted" mean this worker cannot capture the huddle, not that there
        // was nothing to capture. Dead-letters (never burning the retry
        // backoff schedule on something retrying cannot fix — the same
        // operational intent the original BLOCKER 3 fix had, now expressed as
        // `status='dead'` instead of a misleadingly green `status='done'`)
        // while still writing the SAME ledger row `finish_as_skipped` does,
        // so `skip_reason` observability is unchanged.
        Err(CaptureError::Terminal(reason)) => {
            tracing::info!(
                account_id = %payload.account_id,
                channel_id = %payload.channel_id,
                huddle_thread_ts = %payload.huddle_thread_ts,
                reason = %reason,
                "huddle capture: terminal failure — recording as skipped, dead-lettering (not retrying)"
            );
            finish_as_terminal_failure(pool, job.id, &payload, &reason).await;
        }
        Err(CaptureError::Retryable(msg)) => {
            // Retryable — deliberately does NOT write a ledger row (see this
            // module's doc comment). `queue::fail`'s bounded backoff gets a
            // chance to self-heal.
            tracing::warn!(
                account_id = %payload.account_id,
                channel_id = %payload.channel_id,
                huddle_thread_ts = %payload.huddle_thread_ts,
                error = %msg,
                "huddle capture: retryable failure"
            );
            if let Err(qe) = queue::fail(pool, job.id, &msg).await {
                tracing::error!(error = %qe, job_id = %job.id, "failed to fail huddle capture job");
            }
        }
    }
}

/// Tail for `Ok(CaptureOutcome::Skipped)`: a deliberate BUSINESS no-op (e.g.
/// "canvas has no Summary section") — the worker COULD read its input and
/// correctly determined there was nothing to capture. Writes the ledger row
/// and completes the job with `NothingToDo`.
async fn finish_as_skipped(pool: &PgPool, job_id: Uuid, payload: &CapturePayload, reason: &str) {
    if let Err(e) = record_capture_outcome(
        pool,
        payload.account_id,
        &payload.channel_id,
        &payload.huddle_thread_ts,
        Some(reason),
    )
    .await
    {
        tracing::warn!(error = %e, reason = %reason, "huddle capture: failed to record skip outcome in the ledger");
    }
    if let Err(e) = queue::complete(pool, job_id, queue::JobOutcome::NothingToDo).await {
        tracing::warn!(error = %e, job_id = %job_id, "failed to complete skipped huddle capture job");
    }
}

/// Tail for `Err(CaptureError::Terminal)`: this worker COULD NOT capture the
/// huddle — a permanent condition (revoked Slack app, missing account DEK, a
/// canvas that deterministically converts to empty text) that recurs on
/// every retry. Writes the SAME ledger row [`finish_as_skipped`] does (the
/// `skip_reason` observability contract is unchanged) but dead-letters the
/// job (`FailureClass::Structural`) instead of completing it — see the
/// `Err(CaptureError::Terminal)` match arm's comment for why this is a
/// Track B `CouldNotDo`, not a `NothingToDo`.
async fn finish_as_terminal_failure(pool: &PgPool, job_id: Uuid, payload: &CapturePayload, reason: &str) {
    if let Err(e) = record_capture_outcome(
        pool,
        payload.account_id,
        &payload.channel_id,
        &payload.huddle_thread_ts,
        Some(reason),
    )
    .await
    {
        tracing::warn!(error = %e, reason = %reason, "huddle capture: failed to record terminal-failure outcome in the ledger");
    }
    if let Err(e) = queue::could_not_do(pool, job_id, queue::FailureClass::Structural, reason).await {
        tracing::warn!(error = %e, job_id = %job_id, "failed to dead-letter terminal-failure huddle capture job");
    }
}

// ── Ledger (slack_huddle_captures) ────────────────────────────────────────────

/// `true` if this huddle already has a terminal row (ingested or skipped).
async fn capture_already_recorded(
    pool: &PgPool,
    account_id: Uuid,
    channel_id: &str,
    huddle_thread_ts: &str,
) -> Result<bool, sqlx::Error> {
    // `slack_huddle_captures` carries no tenant-RLS policy of its own (not on
    // `0009_tenant_rls`'s roster), so `begin_account_scoped` alone would be
    // sufficient here — `begin_tenant_system_scoped` used anyway for the same
    // "one code path for both planes" reason as every other content-pool
    // statement in this module (see the module doc).
    let mut tx = begin_tenant_system_scoped(pool, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
    let exists: bool = sqlx::query_scalar(
        r#"
        SELECT EXISTS(
            SELECT 1 FROM slack_huddle_captures
            WHERE account_id = $1 AND channel_id = $2 AND huddle_thread_ts = $3
        )
        "#,
    )
    .bind(account_id)
    .bind(channel_id)
    .bind(huddle_thread_ts)
    .fetch_one(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(exists)
}

/// Write the terminal outcome. `skip_reason: None` records an ingested
/// huddle (`processed_at` set); `Some(reason)` records a skip (`processed_at`
/// stays NULL). `ON CONFLICT DO NOTHING` on the pre-existing
/// `(account_id, channel_id, huddle_thread_ts)` unique key: a race between
/// two workers processing a redelivered trigger both reaching this point is
/// harmless (atoms are idempotently deduped already) — whichever writes
/// first wins the ledger row, and the second is a silent no-op.
async fn record_capture_outcome(
    pool: &PgPool,
    account_id: Uuid,
    channel_id: &str,
    huddle_thread_ts: &str,
    skip_reason: Option<&str>,
) -> Result<(), sqlx::Error> {
    let mut tx = begin_tenant_system_scoped(pool, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
    sqlx::query(
        r#"
        INSERT INTO slack_huddle_captures
            (account_id, channel_id, huddle_thread_ts, skip_reason, processed_at, created_at)
        VALUES ($1, $2, $3, $4, CASE WHEN $4 IS NULL THEN now() ELSE NULL END, now())
        ON CONFLICT (account_id, channel_id, huddle_thread_ts) DO NOTHING
        "#,
    )
    .bind(account_id)
    .bind(channel_id)
    .bind(huddle_thread_ts)
    .bind(skip_reason)
    .execute(&mut *tx)
    .await?;
    tx.commit().await
}

// ── Capture ────────────────────────────────────────────────────────────────────

/// Do the actual work for one huddle: resolve the destination(s), fetch and
/// convert the canvas, decide ingest-vs-skip, resolve attendees, and ingest.
///
/// `Err` is always [`CaptureError`] — see that type's doc comment for the
/// retryable-vs-terminal contract every fallible step below must declare a
/// side for. Every terminal business decision (no destination, no Summary
/// section, a placeholder summary, insubstantial prose) is still an
/// `Ok(CaptureOutcome::Skipped(reason))`, exactly as before — `CaptureError`
/// only replaces the OTHER path, where a permanent condition used to be
/// mis-reported as a bare retryable `Err`.
async fn capture_huddle(
    pool: &PgPool,
    tenant: &TenantId,
    control_pool: &PgPool,
    http_client: &reqwest::Client,
    r2: Option<&coralswarm_core::r2::R2Client>,
    payload: &CapturePayload,
    // True only on the job's first run. The unmatched-content notice is
    // gated on this so a channel sees ONE "not saved" message per huddle
    // rather than one per retry — see the call site.
    first_attempt: bool,
) -> Result<CaptureOutcome, CaptureError> {
    // The Slack bot token is control-only (`oauth_tokens`, beside
    // `connector_installs` — founder decision), regardless of which plane
    // this huddle's content lands on (org-slack step 4).
    // The ROW is control-only; the KEY is not. `tenant`/`pool` are the plane
    // this job already resolved for its CONTENT (org-slack step 4) — reusing
    // them for the DEK is what keeps the two from disagreeing, and for an org
    // account `pool` IS the tenant DB where `account_keys` lives.
    let dek_plane = crate::crypto::DekPlane::from_tenant(tenant, pool.clone(), payload.account_id);
    let bot_token = get_slack_bot_token_for_workspace(control_pool, &dek_plane, payload.account_id, &payload.workspace_id)
        .await
        .map_err(|e| CaptureError::retryable(format!("bot token lookup failed: {e}")))?
        .ok_or_else(|| {
            // BLOCKER 3: an uninstalled/revoked Slack app will not heal on
            // retry — this is a permanent condition, not a transient one.
            CaptureError::terminal("no Slack oauth token for account")
        })?;

    // ── Destination — resolved BEFORE any Slack canvas fetch, both because
    // it is the cheaper check and because an empty destination must never
    // fabricate a seat regardless of how good the canvas prose is. Mirrors
    // `routes::slack_events::ingest_slack_message`'s FanOut handling
    // (`backend/src/routes/slack_events.rs`).
    //
    // Plane-explicit (org-slack step 4, mirroring `ingest_slack_message`'s
    // own split from step 2): `control_pool` resolves the account's plane
    // (`accounts.clerk_org_id`) and `resolve_channel_destinations`'
    // `account_members` read; `pool` (this huddle's own content pool — `t.pool`
    // from the caller) resolves the destination graph(s) themselves. On the
    // consumer plane the two are the SAME pool (byte-identical to before this
    // split); an org tenant is the case that actually needs them to differ.
    let plane = crate::routes::slack::account_plane(control_pool, payload.account_id)
        .await
        .map_err(|e| CaptureError::retryable(format!("account plane lookup failed: {e}")))?;
    let (grant, unmapped_members) = crate::ingest::OrgSharedGrant::slack_channel(
        control_pool,
        pool,
        tenant,
        payload.account_id,
        plane,
        &payload.workspace_id,
        &payload.channel_id,
    )
    .await
    .map_err(|e| CaptureError::retryable(format!("destination resolution failed: {e}")))?;

    if unmapped_members > 0 {
        tracing::warn!(
            account_id = %payload.account_id,
            channel_id = %payload.channel_id,
            unmapped_members,
            connector_health = true,
            "huddle capture: org fan-out could not map {unmapped_members} member(s) to a \
             Slack identity yet (capability C5 unwired) — their oceans are skipped, never \
             guessed"
        );
    }
    if grant.oceans().is_empty() {
        // BLOCKER 4: this is an INFRASTRUCTURE gap — capability C5's
        // identity mapping simply is not wired yet — not a permanent,
        // content-derived property of THIS huddle (contrast "canvas
        // has no Summary section" / the placeholder-summary skip
        // below, which really can never succeed on any retry).
        // Recording it as `Ok(CaptureOutcome::Skipped(..))` would
        // write a PERMANENT `slack_huddle_captures` row via
        // `record_capture_outcome`'s `ON CONFLICT DO NOTHING` — which
        // can never be overwritten once C5 ships — silently
        // suppressing every org huddle seen between today and that
        // PR, forever. Returning `CaptureError::Retryable` instead
        // writes NO ledger row (see `process_job`'s retryable arm),
        // so the job's own bounded backoff gets a chance to self-heal
        // if C5 ships before it's exhausted, and — the part that
        // actually matters — a huddle that outlives that backoff is
        // left un-recorded and therefore still reprocessable later,
        // rather than permanently and silently lost.
        //
        // C5's "unmatched content drops and notifies in-channel"
        // requirement (docs/connectors/slack.md): best-effort, and
        // posted ONCE per huddle, on the job's first attempt only.
        // The retryable/no-ledger-row design above means this branch
        // is re-entered on every backoff tick, and without the gate a
        // channel would see the same notice `max_attempts` times for
        // one huddle. Accepted trade: if a later retry succeeds
        // (identity landed mid-backoff), the channel was told it
        // wasn't saved when it eventually was — rare, and quieter
        // than repeating.
        if first_attempt {
            // Same alertable signal the ingest path now emits. Without
            // it this state reaches the operator only as `process_job`'s
            // generic "huddle capture: retryable failure" — no
            // `connector_health` marker, and indistinguishable from a
            // Slack API blip or a DB hiccup, which are the failures
            // that heal on their own. This one does not: it stays
            // broken until somebody links an identity, and the huddle
            // is dropped every time.
            //
            // Gated on `first_attempt`, matching the notice beside it,
            // for the reason spelled out above: the retryable design
            // re-enters this branch on every backoff tick.
            tracing::warn!(
                account_id = %payload.account_id,
                channel_id = %payload.channel_id,
                huddle_thread_ts = %payload.huddle_thread_ts,
                unmapped_members,
                connector_health = true,
                // Cause-neutral, same reasoning as the ingest path's
                // twin: an identity can be confirmed and the member
                // ocean still be absent or unprovisioned.
                "huddle capture: org fan-out resolved to ZERO destinations — this huddle \
                 is being dropped. No member ocean could be resolved for this account. \
                 Check Slack identity links and member-ocean provisioning; the \
                 `unmapped_members` field distinguishes them (>0 = identities missing, \
                 0 = identities resolved but no ocean)."
            );
            crate::connectors::slack::notify_unmatched_content_not_saved(
                http_client,
                &bot_token,
                &payload.channel_id,
                "This huddle",
            )
            .await;
        }
        return Err(CaptureError::retryable(
            "no mappable destination ocean for this org account (identity mapping \
             capability C5 not wired) — never fabricating a seat",
        ));
    }
    let raw_ids = crate::connectors::slack_huddle_consent::require_consent(
        control_pool, pool, http_client, &bot_token, payload, grant.oceans(),
    ).await.map_err(CaptureError::retryable)?;

    // ── Resolve a fresh url_private via files.info on the canvas id, rather
    // than trusting any embedded copy from the trigger event.
    let (canvas_url, _filetype) =
        slack_files_info(http_client, &bot_token, &payload.canvas_file_id)
            .await
            .map_err(|e| e.context(&format!("files.info failed for canvas {}", payload.canvas_file_id)))?;

    let html = download_canvas(http_client, &bot_token, &canvas_url)
        .await
        .map_err(|e| CaptureError::retryable(format!("canvas download failed: {e}")))?;
    if html.is_empty() {
        return Err(CaptureError::retryable("canvas download returned an empty body"));
    }

    let raw_text = html_to_text(&html);
    let clean_text = strip_pointer_lines(&raw_text);

    // ── Extract + judge the Summary section — the founder's "skip, but
    // record why" decision (never a silent skip).
    let Some(summary) = extract_section(&clean_text, "Summary") else {
        return Ok(CaptureOutcome::Skipped("canvas has no Summary section".to_string()));
    };
    if is_placeholder_summary(&summary) {
        return Ok(CaptureOutcome::Skipped(
            "Slack's placeholder summary (\"not enough to summarize\") — huddle too short for \
             AI notes"
                .to_string(),
        ));
    }
    if is_pointer_junk(&summary) {
        return Ok(CaptureOutcome::Skipped(
            "summary text too short/insubstantial to ingest".to_string(),
        ));
    }

    // ── `ingested_by` attribution (journal completeness gap 2) ────────────────
    // A huddle transcript is one prose blob with no per-speaker breakdown — it
    // has attendees, not a single author the way a Slack message does — and
    // `atoms.ingested_by` can only ever hold ONE user per row. See
    // `resolve_huddle_attribution`'s doc comment for the "first seated
    // attendee wins" design and why per-attendee duplicate atoms were
    // rejected. Resolved from the SAME `raw_ids` list this function already
    // computed above, before it's filtered down to display names below.
    let ingested_by = resolve_huddle_attribution(pool, &payload.workspace_id, &raw_ids)
        .await
        .map_err(|e| CaptureError::retryable(format!("attribution lookup failed: {e}")))?;

    let mut participants: Vec<String> = Vec::new();
    for raw_id in &raw_ids {
        if is_bot_user(http_client, &bot_token, raw_id).await {
            continue;
        }
        let name = crate::routes::slack::resolve_user_name(
            http_client,
            payload.account_id,
            raw_id,
            &bot_token,
        )
        .await;
        if !participants.contains(&name) {
            participants.push(name);
        }
    }

    let channel_name = crate::routes::slack::resolve_channel_name(
        http_client,
        payload.account_id,
        &payload.channel_id,
        &bot_token,
    )
    .await;

    // Canvas download and name lookups can take time. Re-check the audience
    // and actor immediately before content writes as well as before fetch.
    crate::connectors::slack_huddle_consent::require_consent(
        control_pool, pool, http_client, &bot_token, payload, grant.oceans(),
    ).await.map_err(CaptureError::retryable)?;

    let atom_count = ingest_huddle_notes(
        pool,
        tenant,
        r2,
        payload,
        &grant,
        &clean_text,
        &participants,
        &channel_name,
        ingested_by,
    )
    .await
    .map_err(|e| e.context("atom ingest failed"))?;

    // BLOCKER 1: never report a zero-atom run as `Ingested` — that would
    // write a permanent `Ingested` ledger row for a huddle that has nothing
    // to show for it (e.g. every destination's R2 upload failed — BLOCKER
    // 2 — leaving nothing surviving anywhere). Treated as retryable: unlike
    // the three permanent conditions above, there is no reason to believe
    // the underlying cause (an R2 outage, most plausibly) won't clear.
    let atom_count = require_nonzero_atoms(atom_count)?;

    Ok(CaptureOutcome::Ingested { atom_count })
}

/// Refuse to call a zero-atom ingest a success (BLOCKER 1). Pure and
/// unit-testable without a database — see the `require_nonzero_atoms_*`
/// tests below.
fn require_nonzero_atoms(atom_count: usize) -> Result<usize, CaptureError> {
    if atom_count == 0 {
        Err(CaptureError::retryable(
            "ingest produced zero atoms across all destination graphs",
        ))
    } else {
        Ok(atom_count)
    }
}

/// Resolve which (if any) huddle attendee the transcript atom(s) should be
/// attributed to, given `attendee_slack_ids` in `room.participant_history`
/// order (or the canvas-parsed fallback — see [`capture_huddle`]'s "Attendees"
/// step).
///
/// ── Design: first seated attendee wins, not one-atom-per-attendee ──
/// A Slack MESSAGE has exactly one author, so `routes::slack_events::
/// ingest_slack_message` can stamp `ingested_by` unambiguously. A huddle
/// transcript is different: it is ONE prose blob (Slack AI's canvas summary)
/// with no per-speaker breakdown, contributed by potentially several
/// attendees, and `atoms.ingested_by` is a single column — it can hold
/// exactly one user per row.
///
/// Two honest options were considered:
///   1. **Duplicate the atom once per seated attendee** (this destination
///      graph's fan-out loop already does something structurally similar for
///      CHANNEL membership — see `ChannelDestination::FanOut` — but that
///      fans out to *different oceans*; this would fan out to *N rows in the
///      SAME ocean*). Rejected: it multiplies storage, embedding cost, and
///      search/journal results N-for-1 for every multi-attendee huddle, and
///      still doesn't produce a per-speaker attribution (the content itself
///      has no speaker breakdown to split on) — just N identical copies each
///      claimed by a different attendee.
///   2. **Attribute to the first seated attendee** (this function). Simple,
///      honest about the limitation (documented here and in the PR that
///      introduced it), and consistent with the existing "never guess, only
///      ever act on a CONFIRMED identity" posture the connector already
///      applies everywhere else (`docs/connectors/CONNECTOR_CONTRACT.md` §4,
///      C5 row). Flagged for founder review if per-participant huddle
///      attribution becomes a real product requirement later.
///
/// "Seated" means the SAME confirmed link `routes::slack_events::
/// ingest_slack_message` and `resolve_org_fanout_destinations` already read:
/// a `connector_identities` row with `provider = 'slack'`, this
/// `workspace_id`, and a non-NULL `user_id` — directory-only, no live Slack
/// API call. An attendee with no confirmed link (never signed up for
/// CoralSwarm, or an unlinked guest) is skipped, never guessed. A huddle with
/// no resolvable attendee returns `None` — the atom(s) keep
/// `ingested_by = NULL`, the same accepted "invisible to journals"
/// degradation an unmappable Slack message author already has.
async fn resolve_huddle_attribution(
    pool: &PgPool,
    workspace_id: &str,
    attendee_slack_ids: &[String],
) -> Result<Option<Uuid>, sqlx::Error> {
    for slack_id in attendee_slack_ids {
        if let Some(user_id) =
            crate::seats::resolve_linked_user_directory_only(pool, workspace_id, slack_id).await?
        {
            return Ok(Some(user_id));
        }
    }
    Ok(None)
}

// ── Canvas section extraction (pure, unit-testable) ───────────────────────────

/// True for a line shaped like a canvas section heading — `:<emoji-shortcode>:
/// <Title>` (e.g. `:star: Summary`, `:handshake: Attendees`), per the
/// MEASURED canvas shape (`backend/tests/fixtures/slack_huddle/README.md`).
/// Used both to find a target section and to know where it ENDS (the next
/// heading, of any kind).
fn looks_like_canvas_heading(line: &str) -> bool {
    let t = line.trim();
    t.starts_with(':') && t[1..].find(':').is_some()
}

/// Extract the body of a canvas section whose heading line ends with
/// `heading_word` (e.g. `"Summary"`, `"Attendees"`) — text after that
/// heading, up to (not including) the next heading-shaped line or the end of
/// the text. Returns `None` when no such heading is found. Operates on
/// `html_to_text` + `strip_pointer_lines` output (never raw HTML).
pub(crate) fn extract_section(text: &str, heading_word: &str) -> Option<String> {
    let lines: Vec<&str> = text.lines().collect();
    let start = lines
        .iter()
        .position(|l| looks_like_canvas_heading(l) && l.trim().ends_with(heading_word))?;

    let mut body: Vec<&str> = Vec::new();
    for line in &lines[start + 1..] {
        let t = line.trim();
        if t.is_empty() {
            continue;
        }
        if looks_like_canvas_heading(t) {
            break;
        }
        body.push(t);
    }

    if body.is_empty() {
        None
    } else {
        Some(body.join(" "))
    }
}

/// True when `summary` matches Slack's placeholder wording for a huddle too
/// short to summarize. Matches on a NORMALISED SUBSTRING (lowercase,
/// containment) — deliberately NOT an exact full-sentence equality — because
/// Slack's exact wording may vary; the measured placeholder is *"Not enough
/// to summarize — this was a huddle of few words."*
/// (`backend/tests/fixtures/slack_huddle/canvas_placeholder.html`).
pub(crate) fn is_placeholder_summary(summary: &str) -> bool {
    summary.to_ascii_lowercase().contains("not enough to summarize")
}

/// Extract raw `@U…`/`@W…` Slack user ids out of a canvas's Attendees
/// section — retained only for historical parser tests, never consent.
/// Never used to invent a name: callers still resolve each id through
/// `routes::slack::resolve_user_name`.
#[cfg(test)]
pub(crate) fn extract_attendee_ids(text: &str) -> Vec<String> {
    let Some(section) = extract_section(text, "Attendees") else {
        return Vec::new();
    };
    section
        .split_whitespace()
        .filter_map(|tok| tok.strip_prefix('@'))
        .map(|id| id.trim_end_matches(|c: char| !c.is_ascii_alphanumeric()))
        .filter(|id| id.starts_with('U') || id.starts_with('W'))
        .map(str::to_owned)
        .collect()
}

/// `true` if Slack's `users.info` reports this user id as a bot (our own bot
/// included) — attendees are filtered to humans only. Best-effort: any
/// network/parse failure is treated as "not a bot" (never silently drops a
/// genuine attendee because of a transient API hiccup).
async fn is_bot_user(http_client: &reqwest::Client, bot_token: &str, user_id: &str) -> bool {
    let fetched = async {
        let resp = http_client
            .get(format!("{}/users.info", crate::connectors::slack::slack_api_base()))
            .query(&[("user", user_id)])
            .bearer_auth(bot_token)
            .send()
            .await
            .ok()?
            .json::<serde_json::Value>()
            .await
            .ok()?;
        resp.get("user")?.get("is_bot")?.as_bool()
    }
    .await;
    fetched.unwrap_or(false)
}

// ── Token retrieval ───────────────────────────────────────────────────────────

/// Fetch and decrypt the Slack bot token for the given account from `oauth_tokens`.
///
/// Foundation PR 1.2 (CS-031): resolves the account DEK via
/// `crate::crypto::resolve_dek` — the SAME wrapped DEK content encryption
/// (blob/atom) uses (`MASTER_KEK_HEX` + `accounts.dek_wrapped`). Before this
/// PR, oauth tokens used a SEPARATE scheme (`ACCOUNT_MASTER_KEK` XOR the
/// account UUID, `routes::oauth::derive_account_dek`); that derivation is now
/// retired from every request/worker path and survives only as
/// `crate::crypto::derive_legacy_oauth_account_dek`, read solely by the
/// one-time cutover backfill.
///
/// Uncached (unlike `routes::connectors::load_creds`'s `dek_cache` param):
/// this function takes a bare `&PgPool`, with no `AppState`/`DekCache` handle
/// available to it — same "no `AppState` access, call the resolver directly"
/// convention `crate::crypto::resolve_dek_cached`'s own doc comment states
/// for the embed worker.
///
/// Org-tenancy Phase W.2: `oauth_tokens` is RLS-covered, so the lookup runs
/// inside a `begin_account_scoped` transaction (the `account_id = $1`
/// predicate stays, as the primary scoping mechanism). The decrypt happens
/// AFTER the transaction closes — it is pure CPU work with no reason to hold
/// a pooled connection.
///
/// Delegates to [`resolve_account_wide_oauth_token`] (unscoped by workspace —
/// see that function's own doc comment for why a bare account+provider
/// `LIMIT 1` is the existing, accepted shape here); see
/// [`get_slack_bot_token_for_workspace`] for the workspace-scoped sibling
/// Foundation PR 1.5 introduces for the sites that DO have a specific
/// `provider_workspace_id` on hand.
async fn get_slack_bot_token(
    control_pool: &PgPool,
    dek_plane: &crate::crypto::DekPlane,
    account_id: Uuid,
) -> Result<Option<String>, String> {
    resolve_account_wide_oauth_token(control_pool, dek_plane, account_id, "slack", None).await
}

/// Foundation PR 1.5 — the workspace-scoped sibling of [`get_slack_bot_token`],
/// for callers migrating a `slack_integrations.bot_token` read that has a
/// specific `workspace_id` in hand (a disconnect, a webhook's `team_id`, a
/// per-workspace resolver) rather than an account with an assumed single
/// Slack workspace. Same DEK resolution and decrypt as `get_slack_bot_token`,
/// with an extra `provider_workspace_id = $2` predicate so an account with
/// more than one connected workspace resolves the RIGHT one instead of an
/// arbitrary `LIMIT 1` row.
///
/// `pub(crate)`, like [`get_slack_bot_token_pub`]: exposed for
/// `routes::slack`, `routes::slack_events`, `routes::oauth`,
/// `connectors::slack_disconnect`, and `workers::slack_name_backfill` — every
/// one of this PR's six migrated `slack_integrations.bot_token` read sites
/// that has a workspace id on hand.
pub(crate) async fn get_slack_bot_token_for_workspace(
    control_pool: &PgPool,
    dek_plane: &crate::crypto::DekPlane,
    account_id: Uuid,
    workspace_id: &str,
) -> Result<Option<String>, String> {
    resolve_account_wide_oauth_token(
        control_pool,
        dek_plane,
        account_id,
        "slack",
        Some(workspace_id),
    )
    .await
}

/// Shared DEK-resolve + decrypt for ONE account-wide `oauth_tokens` row
/// (`owner_user_id IS NULL` — see `connectors::TokenOwner`'s own doc comment;
/// no provider this module resolves has per-member OAuth). Factored out of
/// [`get_slack_bot_token`] so [`get_slack_bot_token_for_workspace`] shares the
/// identical DEK derivation, transaction scoping, and decrypt — the two
/// functions differ ONLY in whether `provider_workspace_id` is part of the
/// `WHERE` clause.
///
/// # Two pools, two questions (org-plane DEK fix)
/// `control_pool` is where the `oauth_tokens` ROW lives — control-plane-only
/// for every account, personal or org. `dek_plane` is where that row's KEY
/// lives, and for an ORG account that is a DIFFERENT DATABASE: its own tenant
/// DB's `account_keys` (see `crate::crypto`'s module doc).
///
/// This used to call `crypto::resolve_dek(pool, ..)`, which hardcodes
/// `TenantId::Consumer`. That was survivable only for as long as an org
/// account could never get an `oauth_tokens` row at all — the OAuth callback
/// 503'd first. The moment that callback was fixed to write under the tenant
/// DEK, this function became the SECOND half of the same bug and a strictly
/// worse one: the row now exists, so the deferred `dek.ok_or_else(...)`
/// verdict below fires, and every one of this function's callers swallows
/// that `Err` into a `warn!`/`error!` (`slack_interactivity` answers Slack
/// `200 OK`). An org Slack connect would have succeeded and then silently
/// ingested nothing — invisible in the product, unlike the honest 503.
/// Callers now pass the plane they already resolved for content, or resolve
/// it through the one shared `crypto::DekPlane::resolve_for_org` table.
async fn resolve_account_wide_oauth_token(
    control_pool: &PgPool,
    dek_plane: &crate::crypto::DekPlane,
    account_id: Uuid,
    provider: &str,
    workspace_id: Option<&str>,
) -> Result<Option<String>, String> {
    // Resolve the DEK up front, but do NOT yet treat a missing one as fatal.
    //
    // `resolve_dek`'s two negative outcomes mean different things and must be
    // handled at different points (#741):
    //
    // * `Err` — the KEK itself is unavailable. That is an INFRASTRUCTURE
    //   condition, it says nothing about this account, and it must surface
    //   immediately so callers retry rather than record a permanent outcome.
    //   `workers::huddle`'s own `missing_master_kek_hex_is_retryable_and_
    //   writes_no_ledger_row_pg` pins exactly that, so this arm stays where
    //   it was.
    // * `Ok(None)` — this account has no `dek_wrapped` provisioned. That is
    //   only a problem if there turns out to be something to decrypt, so the
    //   verdict is deferred to below.
    //
    // Conflating the two is what made "this account has no Slack install" —
    // an ordinary, permanent product state every caller handles with an
    // `Ok(None)` arm — return `Err("account has no dek_wrapped
    // provisioned")`, which both callers then swallow (one of them answering
    // Slack `200 OK`). Pinned by `slack_token_rls_pg::
    // slack_bot_token_lookup_succeeds_under_rls`, whose own comment states
    // that the `Ok(None)` state "must stay distinguishable from a failure".
    let dek = dek_plane
        .resolve(account_id)
        .await
        .map_err(|e| format!("failed to resolve account DEK: {e}"))?;
    let row = async {
        let mut tx = begin_account_scoped(control_pool, account_id)
            .await
            .map_err(|e| e.to_string())?;
        // Foundation PR 1.1: `owner_user_id IS NULL` — the Slack bot token is
        // always account-wide (see `docs/plans/org-connectors-and-channel-
        // ingest.md` §14.1: Slack has no per-member OAuth).
        let row = match workspace_id {
            Some(ws) => {
                sqlx::query(
                    r#"
                    SELECT access_token
                    FROM   oauth_tokens
                    WHERE  account_id = $1
                      AND  provider   = $2
                      AND  provider_workspace_id = $3
                      AND  owner_user_id IS NULL
                    LIMIT  1
                    "#,
                )
                .bind(account_id)
                .bind(provider)
                .bind(ws)
                .fetch_optional(&mut *tx)
                .await
                .map_err(|e| e.to_string())?
            }
            None => {
                sqlx::query(
                    r#"
                    SELECT access_token
                    FROM   oauth_tokens
                    WHERE  account_id = $1
                      AND  provider   = $2
                      AND  owner_user_id IS NULL
                    LIMIT  1
                    "#,
                )
                .bind(account_id)
                .bind(provider)
                .fetch_optional(&mut *tx)
                .await
                .map_err(|e| e.to_string())?
            }
        };
        tx.commit().await.map_err(|e| e.to_string())?;
        Ok::<_, String>(row)
    }
    .await?;

    let Some(row) = row else {
        return Ok(None);
    };

    let encrypted: Vec<u8> = row.get("access_token");

    // A row exists, so there IS something to decrypt — only now is a missing
    // `dek_wrapped` genuinely an error rather than a fact about an account
    // that has installed nothing.
    let dek = dek.ok_or_else(|| "account has no dek_wrapped provisioned".to_string())?;

    let token = decrypt_token(&dek, &encrypted)
        .map_err(|e| format!("token decryption failed: {e}"))?;

    Ok(Some(token))
}

/// Public(crate) wrapper so `routes::slack_interactivity` / `routes::slack_events`
/// can reuse the token fetch without duplicating the DEK derivation logic.
///
/// # Scoping (org-tenancy OPS-4 — the F4 fix)
///
/// This used to run its own AMBIENT copy of [`get_slack_bot_token`]'s query,
/// on the stated reasoning that "its callers are route handlers … that lane
/// owns picking the scoping seam for its own request context". That reasoning
/// was **structurally impossible** and is gone: this function takes a
/// `&PgPool` and so checks out its OWN connection, and `SET LOCAL
/// app.account_id` is per-transaction on ONE connection. A scope a caller
/// opened would have lived on a different connection entirely and could never
/// have reached this query — see `tenancy::scope`'s module doc ("`PostgresStore`
/// has no per-request held connection"). Post-OPS-4 the read simply
/// hard-errored on the strict `current_setting` predicate, and BOTH callers
/// swallow that into a `warn!` (`slack_interactivity` even answers Slack
/// `200 OK`), so huddle capture and interactivity backfill would have died
/// silently.
///
/// The seam chosen is **resolve-then-scope-here**, not "take a scoped
/// executor", because there is nothing left for a caller to contribute:
///
/// * The account is ALREADY discovered before this is called. These are
///   signature-authenticated webhooks with no JWT and no session — both
///   callers bootstrap `account_id` from the Slack `team_id` via
///   `routes::slack::resolve_slack_account_id`, i.e. migration 0084's
///   `resolve_oauth_account_id` `SECURITY DEFINER` resolver, built for exactly
///   this webhook shape. Passing that id in is the whole contract already.
/// * No caller holds a transaction to hand down. Both call this in the middle
///   of a sequence interleaved with Slack HTTP calls (`auth.test`,
///   `conversations.history`, `response_url` posts), so threading a scoped
///   executor through would pin a pooled connection across that network I/O —
///   the thing PR #291's `JobScope` rationale rules out.
///
/// So it delegates verbatim to the already-audited, already-scoped
/// [`get_slack_bot_token`]: identical DEK derivation, identical query,
/// identical `Ok(None)`/`Err` semantics, with the scope opened and closed
/// around the read and the decrypt happening after the transaction closes.
/// The wrapper is retained (rather than pointing the callers straight at
/// `get_slack_bot_token`) purely because that one is private to this module.
pub(crate) async fn get_slack_bot_token_pub(
    control_pool: &PgPool,
    dek_plane: &crate::crypto::DekPlane,
    account_id: Uuid,
) -> Result<Option<String>, String> {
    get_slack_bot_token(control_pool, dek_plane, account_id).await
}

/// Test-only public alias for [`get_slack_bot_token_pub`] (which is
/// `pub(crate)`, so `backend/tests/*.rs` — which link this crate as an
/// ordinary external dependency — cannot see it). Same idiom, and same
/// `test-support` feature, as `lib::test_support_app`; never present in a
/// normal build. Used by `backend/tests/slack_token_rls_pg.rs` to prove the
/// lookup above really is scoped, against a real `coralswarm_app` login.
#[cfg(feature = "test-support")]
pub async fn test_support_slack_bot_token(
    pool: &PgPool,
    account_id: Uuid,
) -> Result<Option<String>, String> {
    // Consumer plane explicitly: these aliases exist to pin the CONSUMER-plane
    // scoping/`Ok(None)` behaviour (`slack_token_rls_pg`), so they keep the
    // pre-DekPlane shape rather than growing a parameter their callers have no
    // opinion about. The org-plane counterpart is proven in-crate, by
    // `routes::connectors::org_plane_dek_pg_tests`, which can see the
    // `pub(crate)` functions directly.
    get_slack_bot_token_pub(pool, &crate::crypto::DekPlane::consumer(pool.clone()), account_id).await
}

/// Test-only public alias for [`get_slack_bot_token_for_workspace`] (Foundation
/// PR 1.5), for the identical `backend/tests/*.rs`-cannot-see-`pub(crate)`
/// reason [`test_support_slack_bot_token`] exists. Used by
/// `backend/tests/slack_oauth_backfill_pg.rs` to prove a backfilled row
/// resolves through the real scoped path, and that a subsequently deleted
/// row resolves to `Ok(None)` (the "disconnect actually stops ingest" proof).
#[cfg(feature = "test-support")]
pub async fn test_support_slack_bot_token_for_workspace(
    pool: &PgPool,
    account_id: Uuid,
    workspace_id: &str,
) -> Result<Option<String>, String> {
    // Consumer plane explicitly — see `test_support_slack_bot_token`.
    get_slack_bot_token_for_workspace(
        pool,
        &crate::crypto::DekPlane::consumer(pool.clone()),
        account_id,
        workspace_id,
    )
    .await
}

// ── Slack API helpers ─────────────────────────────────────────────────────────

/// Scan conversation replies for a canvas / quip file and return its
/// `url_private`.
///
/// Retained (KEPT, not deleted — MEASURED-shape parity with the reference
/// test suite) but unused in the current production path: the new capture
/// flow resolves the canvas via `files.info` on the trigger's own
/// `canvas_file_id` (preferring a freshly-resolved `url_private` over any
/// embedded copy — see `capture_huddle`), rather than scanning
/// `conversations.replies`. Kept because it is already-tested, small, and a
/// natural fit if a future change needs to locate a canvas by scanning a
/// thread instead of by a known file id.
#[cfg_attr(not(test), allow(dead_code))]
fn find_canvas_url(replies: &serde_json::Value) -> Option<String> {
    let messages = replies.get("messages")?.as_array()?;

    for msg in messages {
        let files = match msg.get("files").and_then(|f| f.as_array()) {
            Some(f) => f,
            None => continue,
        };
        for file in files {
            let filetype = file.get("filetype").and_then(|v| v.as_str()).unwrap_or("");
            if filetype == "canvas" || filetype == "quip" {
                if let Some(url) = file.get("url_private").and_then(|v| v.as_str()) {
                    return Some(url.to_owned());
                }
            }
        }
    }

    None
}

/// Detect pointer-only junk text produced when the canvas HTML body contains
/// only a Slack file pointer (e.g., `sf:F0B9R1KQH42, File URL: …`).
///
/// Returns `true` when the text is not worth ingesting — it is either too short
/// or matches the pointer-only pattern with no substantive prose around it.
pub(crate) fn is_pointer_junk(text: &str) -> bool {
    let trimmed = text.trim();
    if trimmed.len() < 40 {
        return true;
    }
    // Count only substantive prose: URLs, `sf:F…` file pointers, and the
    // "File URL:" label do not qualify. A canvas whose body is just a pointer
    // to the (bot-inaccessible) transcript file must not be ingested.
    let non_pointer: String = trimmed
        .split_whitespace()
        .filter(|t| {
            let t = t.trim_start_matches(['(', '[', '<']);
            !(t.starts_with("http://")
                || t.starts_with("https://")
                || t.starts_with("sf:F")
                || t == "File"
                || t == "URL:")
        })
        .collect::<Vec<_>>()
        .join(" ");
    non_pointer.trim().len() < 40
}

/// Strip Slack file-pointer lines from canvas-derived plain text.
///
/// Removes any line whose trimmed form starts with `sf:F` (a Slack file
/// pointer) or contains `File URL:` (the URL label that accompanies the
/// pointer).  Trailing blank lines that result from the removal are also
/// collapsed so the output is clean prose.
///
/// # Behaviour
/// - A canvas that contained *only* pointer lines becomes an empty string.
/// - A canvas with real notes followed by a stray pointer line keeps only
///   the notes.
pub(crate) fn strip_pointer_lines(text: &str) -> String {
    let filtered: Vec<&str> = text
        .lines()
        .filter(|line| {
            let t = line.trim();
            !t.starts_with("sf:F") && !t.contains("File URL:")
        })
        .collect();
    // Re-join and collapse resulting excessive blank lines.
    let joined = filtered.join("\n");
    collapse_blank_lines(&joined)
}

/// Fetch a file's metadata from Slack `files.info` and return its `url_private`
/// and `filetype`.
///
/// Prefers plain `url_private` — MEASURED (`backend/tests/fixtures/
/// slack_huddle/README.md`) to return 200 `text/html` for the canvas with the
/// bot token — falling back to `url_private_download` only if `url_private`
/// is absent.
async fn slack_files_info(
    http_client: &reqwest::Client,
    bot_token: &str,
    file_id: &str,
) -> Result<(String, String), CaptureError> {
    let resp = http_client
        .get(format!("{}/files.info", crate::connectors::slack::slack_api_base()))
        .query(&[("file", file_id)])
        .bearer_auth(bot_token)
        .send()
        .await
        .map_err(|e| CaptureError::retryable(format!("HTTP error: {e}")))?;

    let json: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| CaptureError::retryable(format!("JSON parse error: {e}")))?;

    if json.get("ok").and_then(|v| v.as_bool()) != Some(true) {
        let err = json
            .get("error")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");
        return Err(classify_slack_api_error(err));
    }

    let file = json
        .get("file")
        .ok_or_else(|| CaptureError::retryable("files.info response missing 'file'"))?;

    let url = file
        .get("url_private")
        .or_else(|| file.get("url_private_download"))
        .and_then(|v| v.as_str())
        .ok_or_else(|| CaptureError::retryable("files.info: no url_private(_download)"))?
        .to_owned();

    let filetype = file
        .get("filetype")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_owned();

    Ok((url, filetype))
}

/// Classify a Slack `files.info` `error` code as [`CaptureError::Terminal`]
/// (BLOCKER 3's `huddle.rs:861` audit) or [`CaptureError::Retryable`]. Pure
/// so it is unit-testable without a live Slack API call — see the
/// `classify_slack_api_error_*` tests below.
///
/// **Terminal** — the file (canvas) is permanently gone, or the app's OAuth
/// scope will never grant this on its own:
///   * `file_not_found` / `file_deleted` — the canvas has been deleted (or
///     never existed under this id); retrying observes the identical error
///     forever.
///   * `missing_scope` — the installed Slack app lacks a required OAuth
///     scope; nothing about retrying the SAME request changes that (a scope
///     grant requires a NEW OAuth install, which arrives as a fresh
///     `oauth_tokens` row, not a retry of this job).
///
/// **Retryable** — everything else, deliberately including codes NOT
/// explicitly recognized here: `ratelimited`, `internal_error`, and any
/// other transport/server-side hiccup Slack may return are transient by
/// nature, and an unrecognized code is safer treated as "might heal" than
/// silently added to the permanent list by omission.
fn classify_slack_api_error(err_code: &str) -> CaptureError {
    match err_code {
        "file_not_found" | "file_deleted" | "missing_scope" => {
            CaptureError::terminal(format!("Slack API error: {err_code}"))
        }
        _ => CaptureError::retryable(format!("Slack API error: {err_code}")),
    }
}

async fn download_canvas(
    http_client: &reqwest::Client,
    bot_token: &str,
    url: &str,
) -> Result<String, String> {
    let resp = http_client
        .get(url)
        .bearer_auth(bot_token)
        .send()
        .await
        .map_err(|e| format!("HTTP error: {e}"))?;

    let status = resp.status();
    let body = resp.text().await.map_err(|e| format!("body read error: {e}"))?;

    if !status.is_success() {
        return Err(format!("canvas download returned HTTP {status}"));
    }

    Ok(body)
}

// ── HTML → plain text conversion ──────────────────────────────────────────────

/// Convert an HTML string to plain text.
///
/// - Drops `<script>` and `<style>` blocks entirely (including content).
/// - Converts block-level elements (`<br>`, `<p>`, `<div>`, `<li>`,
///   `<h1>`–`<h6>`) to newlines.
/// - Strips remaining HTML tags (including non-standard ones, like the
///   canvas's own `<lnk>`).
/// - Decodes common HTML entities: `&amp;`, `&lt;`, `&gt;`, `&quot;`,
///   `&#39;`, `&nbsp;`.
/// - Collapses runs of more than two consecutive newlines.
pub(crate) fn html_to_text(html: &str) -> String {
    // Step 1: Drop <script>…</script> and <style>…</style> blocks.
    let without_scripts = remove_tagged_block(html, "script");
    let without_styles = remove_tagged_block(&without_scripts, "style");

    // Step 2: Replace block-level tags with newlines.
    let with_newlines = replace_block_tags(&without_styles);

    // Step 3: Strip remaining tags.
    let stripped = strip_tags(&with_newlines);

    // Step 4: Decode HTML entities.
    let decoded = decode_entities(&stripped);

    // Step 5: Collapse excessive blank lines (> 2 consecutive newlines → 2).
    collapse_blank_lines(&decoded)
}

/// Remove all occurrences of `<tag …>…</tag>` (case-insensitive, non-greedy).
///
/// Uses `to_ascii_lowercase()` for tag matching instead of `to_lowercase()`.
/// `to_ascii_lowercase()` is length-preserving for all Unicode code points
/// (non-ASCII bytes are left unchanged), so byte offsets from the lowercased
/// copy are always valid offsets into the original string. HTML tags are
/// always ASCII, so ASCII-case folding is sufficient.
fn remove_tagged_block(input: &str, tag: &str) -> String {
    // We do a simple state-machine scan rather than pulling in a regex crate.
    let mut result = String::with_capacity(input.len());
    let lower = input.to_ascii_lowercase();
    let open_pat = format!("<{}", tag);
    let close_pat = format!("</{}>", tag);
    let mut pos = 0;

    while pos < input.len() {
        if let Some(start) = lower[pos..].find(&open_pat) {
            let abs_start = pos + start;
            // Emit everything up to the opening tag.
            result.push_str(&input[pos..abs_start]);
            // Find the matching close tag.
            if let Some(end_rel) = lower[abs_start..].find(&close_pat) {
                let abs_end = abs_start + end_rel + close_pat.len();
                pos = abs_end;
            } else {
                // No closing tag found — skip the rest of input.
                break;
            }
        } else {
            // No more occurrences — emit the rest.
            result.push_str(&input[pos..]);
            break;
        }
    }

    result
}

/// Replace block-level HTML tags with newline characters.
fn replace_block_tags(input: &str) -> String {
    // Tags that should become newlines.
    const BLOCK_TAGS: &[&str] = &["br", "p", "div", "li", "h1", "h2", "h3", "h4", "h5", "h6", "tr"];

    let mut result = String::with_capacity(input.len());
    let bytes = input.as_bytes();
    let len = input.len();
    let lower = input.to_ascii_lowercase();
    let lower_bytes = lower.as_bytes();
    let mut pos = 0;

    while pos < len {
        if bytes[pos] == b'<' {
            // Find the end of this tag.
            let tag_end = find_tag_end(lower_bytes, pos);

            // Check if this is one of our block tags.
            let tag_content = &lower[pos + 1..tag_end.min(len)];
            // Strip leading '/' for closing tags.
            let tag_name = tag_content.trim_start_matches('/').trim();
            // Tag name ends at first space or '>'.
            let name_end = tag_name
                .find(|c: char| c.is_whitespace() || c == '>')
                .unwrap_or(tag_name.len());
            let name = &tag_name[..name_end];

            if BLOCK_TAGS.contains(&name) {
                result.push('\n');
            }
            // Skip past the tag.
            pos = tag_end.min(len);
        } else {
            // Regular character — advance by one Unicode scalar value.
            let ch_len = input[pos..].chars().next().map(|c| c.len_utf8()).unwrap_or(1);
            result.push_str(&input[pos..pos + ch_len]);
            pos += ch_len;
        }
    }

    result
}

/// Find the index just after the closing `>` of an HTML tag starting at `pos`.
fn find_tag_end(lower_bytes: &[u8], pos: usize) -> usize {
    let mut i = pos;
    while i < lower_bytes.len() {
        if lower_bytes[i] == b'>' {
            return i + 1;
        }
        i += 1;
    }
    lower_bytes.len()
}

/// Strip all remaining HTML tags.
fn strip_tags(input: &str) -> String {
    let mut result = String::with_capacity(input.len());
    let bytes = input.as_bytes();
    let len = input.len();
    let lower = input.to_ascii_lowercase();
    let lower_bytes = lower.as_bytes();
    let mut pos = 0;

    while pos < len {
        if bytes[pos] == b'<' {
            pos = find_tag_end(lower_bytes, pos);
        } else {
            let ch_len = input[pos..].chars().next().map(|c| c.len_utf8()).unwrap_or(1);
            result.push_str(&input[pos..pos + ch_len]);
            pos += ch_len;
        }
    }

    result
}

/// Decode common HTML character entities.
fn decode_entities(input: &str) -> String {
    input
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
        .replace("&apos;", "'")
        .replace("&nbsp;", " ")
}

/// Collapse runs of more than 2 consecutive newlines into exactly 2.
fn collapse_blank_lines(input: &str) -> String {
    let mut result = String::with_capacity(input.len());
    let mut newline_count = 0usize;

    for ch in input.chars() {
        if ch == '\n' {
            newline_count += 1;
            if newline_count <= 2 {
                result.push('\n');
            }
        } else {
            newline_count = 0;
            result.push(ch);
        }
    }

    result.trim().to_string()
}

/// Parse a Slack timestamp ("epochsecs.seq", e.g. `"1717000000.000100"`) into a
/// `DateTime<Utc>`. Mirrors the parse used for regular Slack message ingest
/// (`routes/slack.rs`), but returns `None` on failure instead of silently
/// falling back to `now()` so the caller can decide how to handle it (and log).
fn parse_slack_ts(ts: &str) -> Option<chrono::DateTime<Utc>> {
    let secs: i64 = ts.split('.').next()?.parse().ok()?;
    chrono::DateTime::from_timestamp(secs, 0)
}

// ── Atom ingestion ────────────────────────────────────────────────────────────

/// Ingest a huddle's canvas notes as atoms, once per destination graph.
///
/// Always `ChunkStrategy::Generic` — canvas prose is Slack AI's summary of
/// the huddle, not verbatim speech, so the speaker-aware `Transcript`
/// strategy the old (deleted) VTT path used does not apply.
///
/// `source_type = 'transcript'` is KEPT (not `'huddle_notes'` or similar) to
/// preserve the `(graph_id, source_type, source_id)` deduplication key and
/// so `source_type = 'transcript'` retrieval filters continue to surface
/// huddle content — an ingest-time chunking-strategy change, not a
/// classification change. Per the founder's 2026-07-27 decision, this does
/// NOT write a `meeting_transcripts` row.
async fn ingest_huddle_notes(
    pool: &PgPool,
    tenant: &TenantId,
    r2: Option<&coralswarm_core::r2::R2Client>,
    payload: &CapturePayload,
    grant: &crate::ingest::OrgSharedGrant,
    text: &str,
    participants: &[String],
    channel_name: &str,
    // Journal completeness gap 2 — see `resolve_huddle_attribution`'s doc
    // comment for the "first seated attendee wins" design. Graph-independent
    // (huddle attendance doesn't change per destination) — the SAME value is
    // stamped on every destination graph's copy, exactly like every other
    // graph-independent value this function already binds per-iteration
    // (`participants`, `channel_name`, ...).
    ingested_by: Option<Uuid>,
) -> Result<usize, CaptureError> {
    // Resolve account DEK (`resolve_dek` scheme — for blob encryption, same
    // as ingest.rs), tenant-aware (org-slack step 4): a Consumer tenant reads
    // `accounts.dek_wrapped` (byte-identical to the old `resolve_dek` call —
    // see `crypto::resolve_dek`'s own doc comment for why the two are the
    // same thing), an Org tenant reads its own `account_keys.dek_wrapped` on
    // `pool` — never the control plane's key, wrong-key encryption being
    // unrecoverable.
    let dek: [u8; 32] = crate::crypto::resolve_dek_for_tenant(tenant, pool, payload.account_id)
        .await
        .map_err(|e| CaptureError::retryable(e.to_string()))?
        .ok_or_else(|| {
            // BLOCKER 3: a missing DEK means this account was never (or is
            // no longer) provisioned with one — retrying resolves the exact
            // same absence every time.
            CaptureError::terminal("account has no DEK")
        })?;

    // The huddle's true start time lives in `huddle_thread_ts` ("epochsecs.seq" —
    // same shape as a regular Slack message ts). Atoms' `timestamp` column is
    // documented as EVENT time, not ingest time.
    let event_timestamp = parse_slack_ts(&payload.huddle_thread_ts).unwrap_or_else(|| {
        tracing::warn!(
            huddle_thread_ts = %payload.huddle_thread_ts,
            "could not parse huddle start time from thread ts — falling back to ingest time"
        );
        Utc::now()
    });

    // Contextual chunk header (cs-phase0-chunk-headers): date + participants,
    // plus a duration clause built from room.date_start/date_end (unix
    // seconds) when Slack sent both. `header_len` (byte length including the
    // "\n\n" separator) is stamped on every inserted row so citation code can
    // strip it back out.
    let meeting_header =
        coralswarm_core::chunker::build_meeting_header(None, Some(event_timestamp), participants);
    let duration_clause = match (payload.date_start, payload.date_end) {
        (Some(start), Some(end)) if end > start => {
            let minutes = (end - start + 30) / 60; // round to nearest minute
            Some(format!("Duration: {minutes} min."))
        }
        _ => None,
    };
    let header = match (meeting_header, duration_clause) {
        (Some(m), Some(d)) => Some(format!("{m} {d}")),
        (Some(m), None) => Some(m),
        (None, Some(d)) => Some(d),
        (None, None) => None,
    };

    // Canvas prose is Slack AI's summary, not verbatim speech: Generic
    // chunking, not the Transcript strategy `transcript` would select. This
    // is the huddle's own capture hint (and `chunk_reconcile::backfill::
    // strategy_for` reproduces it for these sources).
    let chunks = chunk(
        text,
        crate::ingest::capture::chunk_strategy("transcript", None, Some(&ChunkStrategy::Generic)),
    );
    if chunks.is_empty() {
        // BLOCKER 3: this text will chunk to empty again on any retry — a
        // permanent property of the canvas content, not a transient failure.
        return Err(CaptureError::terminal("canvas converted to empty text"));
    }

    let content_hash = hex::encode(Sha256::digest(text.as_bytes()));

    let source_label = format!("slack-huddle {}", payload.channel_id);
    let source_id = format!("{source_label}:{}", payload.huddle_thread_ts);

    // Graph-independent: identical chunk ids (`{source_id}:chunk:{n}`), text
    // and columns for every destination graph. The meeting header is
    // recorded in `header_len`, so titles, excerpts and chunk hashes are the
    // body's.
    let prepared = crate::ingest::capture::Prepared::new(
        &source_id,
        "transcript",
        text,
        &chunks,
        &match header {
            Some(h) => crate::ingest::capture::Header::Recorded(h),
            None => crate::ingest::capture::Header::None,
        },
        None,
    );

    let mut total_atoms = 0usize;

    for &graph_id in grant.oceans() {
        let metadata=serde_json::json!({"channel_id":payload.channel_id,
            "channel_name":channel_name,"huddle_thread_ts":payload.huddle_thread_ts});
        // Bodies, verified links and both jobs commit with the snapshot
        // through the ingest door, under this channel's routing grant.
        // Existing positions are adopted by hash or replaced in place; a
        // position whose body is missing (a crashed earlier run) is replaced,
        // which is the crash repair this writer used to hand-roll.
        let origin=crate::source_origin::SourceOrigin::native("slack","huddle_notes","slack_huddle_artifacts")
            .and_then(|o|o.with_container("channel",&payload.channel_id,grant.namespace(),Some(channel_name)))
            .map_err(CaptureError::terminal)?;
        let ingested=crate::ingest::submit(crate::ingest::IngestRequest {
            origin:Some(&origin),
            ..crate::ingest::IngestRequest::new(crate::ingest::Destination::OrgShared(crate::ingest::Routed {
                ocean:graph_id, grant,
                writer:crate::ingest::SystemWriter {pool,tenant,dek:&dek,r2},
                source:crate::source_evidence::CaptureSource {
                    source_type:"transcript",provider_namespace:grant.namespace(),source_id:&source_id,
                    normalized_text:text,representation:"provider_summary",metadata:&metadata,
                    occurred_at:parse_slack_ts(&payload.huddle_thread_ts),participants,
                    ingested_by,scope:"account",duration_ms:None,segments:None,
                },
                chunks:&prepared,
                atoms:crate::ingest::capture::AtomFields {
                    scope:"account",
                    channel_id:Some(&payload.channel_id),channel_name:Some(channel_name),
                    workspace_id:Some(&payload.workspace_id),
                    ..crate::ingest::capture::AtomFields::new("transcript",participants,event_timestamp,ingested_by)
                },
                content_hash:&content_hash,
                sync_ops:None,
            }))
        }).await;
        match ingested {
            Ok(ingested)=>total_atoms+=ingested.linked,
            // A malformed identity, or a capture the core calls permanent,
            // fails the same way on every retry. A routing change does not:
            // the next attempt resolves the grant afresh.
            Err(refused) if refused.is_permanent()=>return Err(CaptureError::terminal(refused.to_string())),
            Err(refused)=>return Err(CaptureError::retryable(refused.to_string())),
        }
    }

    Ok(total_atoms)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── html_to_text: tags stripped ───────────────────────────────────────────

    #[test]
    fn html_to_text_strips_simple_tags() {
        let html = "<b>Hello</b> <i>world</i>";
        let result = html_to_text(html);
        assert_eq!(result, "Hello world");
    }

    #[test]
    fn html_to_text_decodes_entities() {
        let html = "AT&amp;T &lt;rocks&gt; &quot;yes&quot; it&#39;s&nbsp;great";
        let result = html_to_text(html);
        assert_eq!(result, "AT&T <rocks> \"yes\" it's great");
    }

    #[test]
    fn html_to_text_block_elements_become_newlines() {
        let html = "<p>First paragraph</p><p>Second paragraph</p>";
        let result = html_to_text(html);
        assert!(result.contains("First paragraph"));
        assert!(result.contains("Second paragraph"));
        let first_pos = result.find("First").unwrap();
        let second_pos = result.find("Second").unwrap();
        let between = &result[first_pos + "First paragraph".len()..second_pos];
        assert!(between.contains('\n'), "paragraphs should be separated by newlines");
    }

    #[test]
    fn html_to_text_br_becomes_newline() {
        let html = "line one<br>line two<br/>line three";
        let result = html_to_text(html);
        assert!(result.contains('\n'));
        assert!(result.contains("line one"));
        assert!(result.contains("line two"));
        assert!(result.contains("line three"));
    }

    #[test]
    fn html_to_text_list_items_become_newlines() {
        let html = "<ul><li>item one</li><li>item two</li></ul>";
        let result = html_to_text(html);
        assert!(result.contains("item one"));
        assert!(result.contains("item two"));
        let item_one_end = result.find("item one").unwrap() + "item one".len();
        let item_two_start = result.find("item two").unwrap();
        let between = &result[item_one_end..item_two_start];
        assert!(between.contains('\n'));
    }

    #[test]
    fn html_to_text_headings_become_newlines() {
        let html = "<h1>Title</h1><p>Body</p>";
        let result = html_to_text(html);
        assert!(result.contains("Title"));
        assert!(result.contains("Body"));
        assert!(result.contains('\n'));
    }

    #[test]
    fn html_to_text_script_content_dropped() {
        let html = "<p>visible</p><script>alert('xss')</script><p>also visible</p>";
        let result = html_to_text(html);
        assert!(result.contains("visible"));
        assert!(result.contains("also visible"));
        assert!(!result.contains("alert"), "script content must be dropped");
        assert!(!result.contains("xss"), "script content must be dropped");
    }

    #[test]
    fn html_to_text_style_content_dropped() {
        let html = "<p>text</p><style>body { color: red; }</style><p>more text</p>";
        let result = html_to_text(html);
        assert!(result.contains("text"));
        assert!(result.contains("more text"));
        assert!(!result.contains("color"), "style content must be dropped");
    }

    #[test]
    fn html_to_text_collapses_blank_lines() {
        let html = "<p>a</p><p></p><p></p><p></p><p>b</p>";
        let result = html_to_text(html);
        assert!(!result.contains("\n\n\n"), "more than 2 consecutive newlines found");
    }

    #[test]
    fn html_to_text_empty_input() {
        assert_eq!(html_to_text(""), "");
    }

    #[test]
    fn html_to_text_plain_text_unchanged() {
        let text = "Hello, world!";
        assert_eq!(html_to_text(text), text);
    }

    #[test]
    fn html_to_text_multibyte_unicode_no_panic() {
        let html = "<p>İstanbul toplantısı</p><b>résumé</b><br/>中文内容";
        let result = html_to_text(html);
        assert!(result.contains("İstanbul"), "Turkish text must be preserved");
        assert!(result.contains("résumé"), "accented text must be preserved");
        assert!(result.contains("中文内容"), "CJK text must be preserved");
    }

    #[test]
    fn html_to_text_multibyte_inside_tag_attr_no_panic() {
        let html = r#"<div class="résumé">content İ here</div>"#;
        let result = html_to_text(html);
        assert!(result.contains("content"), "content must survive");
        assert!(!result.contains("<div"), "tags must be stripped");
    }

    #[test]
    fn html_to_text_script_with_multibyte_no_panic() {
        let html = "<p>résumé</p><script>İ alert('xss') İ</script><p>Üniversite</p>";
        let result = html_to_text(html);
        assert!(!result.contains("alert"), "script content must be dropped");
        assert!(result.contains("résumé"), "text before script must survive");
        assert!(result.contains("Üniversite"), "text after script must survive");
    }

    // ── parse_slack_ts ─────────────────────────────────────────────────────────

    #[test]
    fn parse_slack_ts_extracts_event_time() {
        let dt = parse_slack_ts("1717000000.000100").expect("valid slack ts should parse");
        assert_eq!(dt.timestamp(), 1717000000);
    }

    #[test]
    fn parse_slack_ts_handles_missing_fraction() {
        let dt = parse_slack_ts("1717000000").expect("bare seconds should still parse");
        assert_eq!(dt.timestamp(), 1717000000);
    }

    #[test]
    fn parse_slack_ts_rejects_garbage() {
        assert!(parse_slack_ts("not-a-timestamp").is_none());
        assert!(parse_slack_ts("").is_none());
    }

    // ── find_canvas_url (kept, see its own doc comment) ───────────────────────

    #[test]
    fn find_canvas_url_in_replies() {
        let replies = serde_json::json!({
            "ok": true,
            "messages": [
                { "type": "message", "text": "No files here" },
                {
                    "type": "message",
                    "text": "Notes summary",
                    "files": [{
                        "filetype": "canvas",
                        "url_private": "https://files.slack.com/files-pri/T123/canvas/notes.html"
                    }]
                }
            ]
        });

        let url = find_canvas_url(&replies);
        assert_eq!(
            url.as_deref(),
            Some("https://files.slack.com/files-pri/T123/canvas/notes.html")
        );
    }

    #[test]
    fn find_canvas_url_quip_type() {
        let replies = serde_json::json!({
            "ok": true,
            "messages": [{
                "files": [{ "filetype": "quip", "url_private": "https://files.slack.com/quip.html" }]
            }]
        });

        let url = find_canvas_url(&replies);
        assert_eq!(url.as_deref(), Some("https://files.slack.com/quip.html"));
    }

    #[test]
    fn find_canvas_url_not_present() {
        let replies = serde_json::json!({
            "ok": true,
            "messages": [{ "text": "no files" }]
        });

        assert!(find_canvas_url(&replies).is_none());
    }

    #[test]
    fn find_canvas_url_pdf_not_matched() {
        let replies = serde_json::json!({
            "ok": true,
            "messages": [{
                "files": [{ "filetype": "pdf", "url_private": "https://files.slack.com/doc.pdf" }]
            }]
        });

        assert!(find_canvas_url(&replies).is_none(), "PDF should not match");
    }

    // ── Pointer-junk detection ────────────────────────────────────────────────

    #[test]
    fn is_pointer_junk_detects_observed_pointer_text() {
        let pointer = "sf:F0B9R1KQH42, File URL: https://example.slack.com/files/USLACKBOT/F0B9R1KQH42/huddle_transcript";
        assert!(is_pointer_junk(pointer), "observed pointer text must be classified as junk: {pointer:?}");
    }

    #[test]
    fn is_pointer_junk_detects_short_text() {
        assert!(is_pointer_junk("Too short"), "very short text must be junk");
        assert!(is_pointer_junk(""), "empty string must be junk");
        assert!(is_pointer_junk("   "), "whitespace-only must be junk");
    }

    #[test]
    fn is_pointer_junk_accepts_real_prose() {
        let prose = "In today's huddle we discussed the Q3 roadmap and decided to prioritise the transcript ingestion feature.";
        assert!(!is_pointer_junk(prose), "real prose must NOT be classified as junk");
    }

    #[test]
    fn is_pointer_junk_pointer_with_real_prose_not_junk() {
        let mixed = "sf:F0B9R1KQH42, File URL: https://example.slack.com/files/USLACKBOT/F0B9R1KQH42/huddle_transcript\nIn today's huddle we discussed the Q3 roadmap and decided to prioritise the transcript ingestion feature.";
        assert!(!is_pointer_junk(mixed), "pointer followed by real prose must not be classified as junk");
    }

    // ── strip_pointer_lines ───────────────────────────────────────────────────

    #[test]
    fn strip_pointer_lines_pointer_only_becomes_empty() {
        let pointer = "sf:F0B9R1KQH42, File URL: https://example.slack.com/files/USLACKBOT/F0B9R1KQH42/huddle_transcript";
        let result = strip_pointer_lines(pointer);
        assert!(result.trim().is_empty(), "pointer-only text must become empty after stripping, got: {result:?}");
    }

    #[test]
    fn strip_pointer_lines_notes_plus_pointer_keeps_notes_only() {
        let notes_with_pointer = "\
:headphones: Huddle notes: 6/10/26

AI took notes for this huddle from 4:17\u{2013}4:19 PM

:handshake: Attendees
Alice, Bob

:spiral_note_pad: Discussion
Agreed to ship the connector page by end of sprint.

sf:F0B9R1KQH42, File URL: https://example.slack.com/files/USLACKBOT/F0B9R1KQH42/huddle_transcript";

        let result = strip_pointer_lines(notes_with_pointer);

        assert!(result.contains("Huddle notes"), "notes heading must be present: {result:?}");
        assert!(result.contains("Attendees"), "attendees section must be present: {result:?}");
        assert!(result.contains("Agreed to ship"), "discussion prose must be present: {result:?}");
        assert!(!result.contains("sf:F0B9R1KQH42"), "sf: pointer must be stripped: {result:?}");
        assert!(!result.contains("File URL:"), "'File URL:' label must be stripped: {result:?}");
    }

    #[test]
    fn strip_pointer_lines_empty_input_stays_empty() {
        assert_eq!(strip_pointer_lines(""), "");
    }

    #[test]
    fn strip_pointer_lines_plain_prose_unchanged() {
        let prose = "In today's huddle we discussed the Q3 roadmap.";
        let result = strip_pointer_lines(prose);
        assert_eq!(result, prose);
    }

    // ── Notes-document chunk path ────────────────────────────────────────────

    #[test]
    fn notes_document_generic_chunk_contains_prose_not_pointer() {
        let notes_clean = "\
:headphones: Huddle notes: 6/10/26

AI took notes for this huddle from 4:17\u{2013}4:19 PM

:handshake: Attendees
Alice, Bob

:spiral_note_pad: Discussion
Agreed to ship the connector page by end of sprint.";

        let chunks = chunk(notes_clean, ChunkStrategy::Generic);
        assert!(!chunks.is_empty(), "generic chunking of notes prose must produce at least one chunk");

        let all_text: String = chunks.iter().map(|c| c.text.as_str()).collect::<Vec<_>>().join("\n");

        assert!(all_text.contains("Huddle notes"), "chunk text must contain 'Huddle notes': {all_text:?}");
        assert!(all_text.contains("Attendees"), "chunk text must contain 'Attendees': {all_text:?}");
        assert!(!all_text.contains("sf:F"), "chunk text must NOT contain 'sf:F' pointer: {all_text:?}");
        assert!(!all_text.contains("File URL:"), "chunk text must NOT contain 'File URL:': {all_text:?}");

        for c in &chunks {
            assert!(c.speaker.is_none(), "Generic-strategy chunks must not have a speaker label");
        }
    }

    // ── C1: Token round-trip (RELOCATED, Foundation PR 1.2 / CS-031) ─────────
    //
    // This used to prove a token round-trips through `derive_account_dek`
    // (the legacy ACCOUNT_MASTER_KEK-XOR scheme) directly. `get_slack_bot_token`
    // no longer uses that derivation at all — see its own doc comment — so
    // the formula-level coverage moved to `crypto::tests::
    // derive_legacy_dek_end_to_end_via_env_var` (the function now lives at
    // `crate::crypto::derive_legacy_oauth_account_dek`, used only by the
    // wrapped-DEK cutover backfill), and the REAL end-to-end round trip
    // through `get_slack_bot_token`'s new `crate::crypto::resolve_dek` path
    // is proven by the pg-gated tests in `backend/tests/
    // slack_token_rls_pg.rs` (needs a real Postgres pool, unlike this
    // formula, which needed none).

    // ── extract_section / is_placeholder_summary / extract_attendee_ids ──────
    // Against the two MEASURED canvas fixtures (verbatim Slack bodies —
    // backend/tests/fixtures/slack_huddle/README.md).

    const CANVAS_PLACEHOLDER: &str =
        include_str!("../../tests/fixtures/slack_huddle/canvas_placeholder.html");
    const CANVAS_SUBSTANTIVE: &str =
        include_str!("../../tests/fixtures/slack_huddle/canvas_substantive.html");

    fn canvas_to_clean_text(html: &str) -> String {
        strip_pointer_lines(&html_to_text(html))
    }

    #[test]
    fn canvas_placeholder_pointer_stripped_and_emoji_not_doubled() {
        let text = canvas_to_clean_text(CANVAS_PLACEHOLDER);

        // The trailing `sf:F…` pointer line must be gone.
        assert!(!text.contains("sf:F"), "sf: pointer must be stripped: {text:?}");
        assert!(!text.contains("File ID:"), "the pointer's 'File ID:' label must be stripped: {text:?}");

        // `:headphones:` appears exactly once — the emoji `alt` attribute
        // text ("headphones", no colons) is stripped as tag-internal markup,
        // and only the literal `:headphones:` text node survives.
        assert_eq!(
            text.matches(":headphones:").count(),
            1,
            "the literal :headphones: text must not be doubled: {text:?}"
        );

        // `<lnk>` is not a real HTML tag but must still be stripped along
        // with its content preserved.
        assert!(!text.contains("<lnk"), "the non-standard <lnk> tag must not leak: {text:?}");
        assert!(!text.contains("</lnk>"), "the non-standard </lnk> close tag must not leak: {text:?}");
        assert!(text.contains("View huddle in channel"), "the <lnk> element's text content must survive: {text:?}");
    }

    #[test]
    fn canvas_placeholder_summary_section_is_the_placeholder() {
        let text = canvas_to_clean_text(CANVAS_PLACEHOLDER);
        let summary = extract_section(&text, "Summary").expect("Summary section must be found");
        assert!(
            is_placeholder_summary(&summary),
            "the placeholder canvas's Summary section must be recognised as Slack's placeholder: {summary:?}"
        );
    }

    #[test]
    fn canvas_substantive_prose_survives_and_both_attendees_found() {
        let text = canvas_to_clean_text(CANVAS_SUBSTANTIVE);

        let summary = extract_section(&text, "Summary").expect("Summary section must be found");
        assert!(
            !is_placeholder_summary(&summary),
            "substantive prose must NOT be misidentified as the placeholder: {summary:?}"
        );
        assert!(!is_pointer_junk(&summary), "substantive prose must pass the substantive-length check: {summary:?}");
        assert!(
            summary.contains("schedule the next design review"),
            "the real summary prose must survive intact: {summary:?}"
        );

        let attendees = extract_attendee_ids(&text);
        assert_eq!(
            attendees,
            vec!["U57C3A9EF09".to_string(), "U232D57D034".to_string()],
            "both attendee ids must be found: {attendees:?}"
        );
    }

    #[test]
    fn placeholder_detection_matches_normalised_substring_not_exact_sentence() {
        // Slightly different wording/casing/punctuation must still match —
        // deliberately a substring match, not full-sentence equality.
        assert!(is_placeholder_summary("Not enough to summarize — this was a huddle of few words."));
        assert!(is_placeholder_summary("NOT ENOUGH TO SUMMARIZE."));
        assert!(is_placeholder_summary("  not enough to summarize  "));
        assert!(!is_placeholder_summary(
            "The team agreed to schedule the next design review from the Slack connector."
        ));
    }

    #[test]
    fn extract_section_returns_none_when_heading_absent() {
        let text = ":handshake: Attendees\n@U123\n";
        assert!(extract_section(text, "Summary").is_none());
    }

    #[test]
    fn extract_attendee_ids_empty_when_no_attendees_section() {
        assert!(extract_attendee_ids("just some text").is_empty());
    }

    // ── BLOCKER 3: CaptureError classification (pure, unit-testable) ──────────

    #[test]
    fn classify_slack_api_error_permanent_codes_are_terminal() {
        assert!(matches!(classify_slack_api_error("file_not_found"), CaptureError::Terminal(_)));
        assert!(matches!(classify_slack_api_error("file_deleted"), CaptureError::Terminal(_)));
        assert!(matches!(classify_slack_api_error("missing_scope"), CaptureError::Terminal(_)));
    }

    #[test]
    fn classify_slack_api_error_transient_and_unknown_codes_are_retryable() {
        // Genuine transport/rate-limit conditions must stay retryable.
        assert!(matches!(classify_slack_api_error("ratelimited"), CaptureError::Retryable(_)));
        assert!(matches!(classify_slack_api_error("internal_error"), CaptureError::Retryable(_)));
        // An error code this function has never seen before must default to
        // retryable, not be silently treated as permanent by omission.
        assert!(matches!(classify_slack_api_error("some_future_error_code"), CaptureError::Retryable(_)));
    }

    #[test]
    fn capture_error_context_preserves_variant() {
        let terminal = CaptureError::terminal("boom").context("prefix");
        assert!(matches!(terminal, CaptureError::Terminal(ref m) if m == "prefix: boom"));

        let retryable = CaptureError::retryable("boom").context("prefix");
        assert!(matches!(retryable, CaptureError::Retryable(ref m) if m == "prefix: boom"));
    }

    // ── BLOCKER 1: zero-atom ingest must never be reported a success ─────────
    //
    // Before this fix existed, `capture_huddle` built `Ok(CaptureOutcome::
    // Ingested { atom_count })` directly off whatever `ingest_huddle_notes`
    // returned, with no check at all — a atom_count of 0 (e.g. every
    // destination's R2 upload failed, BLOCKER 2) would have been reported as
    // a genuine `Ingested` outcome and `process_job` would have written a
    // permanent `Ingested` ledger row for a huddle with NOTHING behind it.
    // `require_nonzero_atoms` is the guard that closes that gap; these two
    // assertions are exactly what fails without it (a bare `Ok(atom_count)`
    // pass-through has no way to reject 0).

    #[test]
    fn require_nonzero_atoms_rejects_zero() {
        let result = require_nonzero_atoms(0);
        assert!(
            matches!(result, Err(CaptureError::Retryable(_))),
            "a zero-atom ingest must be rejected as a retryable failure, not treated as success: {result:?}"
        );
    }

    #[test]
    fn require_nonzero_atoms_accepts_nonzero() {
        assert_eq!(require_nonzero_atoms(3), Ok(3));
    }
}

// Foundation PR 1.2 (CS-031): `get_slack_bot_token` no longer mutates/reads
// `ACCOUNT_MASTER_KEK` on any live path (see its own doc comment), so the
// tests below that used to serialize against a file-local
// `ACCOUNT_MASTER_KEK_ENV_LOCK` now serialize against the SAME
// `MASTER_KEK_HEX` mutations `mod huddle_capture_idempotency_tests` already
// shares crate-wide via `workers::reembed::MASTER_KEK_HEX_ENV_LOCK` — one
// lock, not two, since there is now only one KEK scheme in play here.

// ── Idempotency (pg-gated) ────────────────────────────────────────────────────
//
// A second trigger for the SAME `(account_id, channel_id, huddle_thread_ts)`
// must be a no-op — for BOTH the ingested and the skipped case (this
// module's doc comment). In-crate (not `backend/tests/*_pg.rs`) because
// `process_job`/`record_capture_outcome` are private/`pub(crate)` and this
// suite wants direct access to the ledger-write helper to seed a prior
// terminal outcome without re-running the whole capture flow.
//
// Gated on `ORGT_TEST_BASE_URL`, migrating the CONTROL tree only — same
// convention as `routes::slack_events::slack_watched_channel_destination_tests`.
// Wired into CI as an explicit step (`.github/workflows/ci.yml`, "Huddle
// capture idempotency tests (in-crate)") — the workspace `cargo test` job has
// no Postgres service and silently skips every test here.
//
//   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//     cargo test -p coralswarm-backend --lib huddle_capture_idempotency_tests
#[cfg(test)]
mod huddle_capture_idempotency_tests {
    use super::*;
    use coralswarm_core::oauth_token::encrypt_token;
    use sqlx::postgres::PgPoolOptions;
    use sqlx::{Connection, Executor, PgConnection, PgPool};

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
    async fn seed_account(pool: &PgPool) -> Uuid {
        let owner: Uuid = sqlx::query_scalar("INSERT INTO users(id, email) VALUES (gen_random_uuid(), $1) RETURNING id")
            .bind(format!("user-{}@test.local", Uuid::new_v4()))
            .fetch_one(pool)
            .await
            .unwrap();
        sqlx::query_scalar("INSERT INTO accounts(owner_user_id, clerk_org_id) VALUES ($1, NULL) RETURNING id")
            .bind(owner)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    /// An ORG account (`clerk_org_id` set) with exactly one `account_members`
    /// row — the shape `connectors::slack::resolve_org_fanout_destinations`
    /// reads directly. With no identity mapping wired (capability C5) and no
    /// explicit `slack_watched_channels` row, this makes that member the
    /// unmapped count, producing the empty-`graphs` FanOut BLOCKER 4's fix
    /// targets.
    async fn seed_org_account(pool: &PgPool) -> Uuid {
        let owner: Uuid = sqlx::query_scalar(
            "INSERT INTO users(id, email) VALUES (gen_random_uuid(), $1) RETURNING id",
        )
        .bind(format!("user-{}@test.local", Uuid::new_v4()))
        .fetch_one(pool)
        .await
        .unwrap();
        let account_id: Uuid = sqlx::query_scalar(
            "INSERT INTO accounts(owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id",
        )
        .bind(owner)
        .bind(format!("org-{}", Uuid::new_v4()))
        .fetch_one(pool)
        .await
        .unwrap();
        sqlx::query("INSERT INTO account_members(account_id, user_id, role, seat_tier_id) VALUES ($1, $2, 'owner', (SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true))")
            .bind(account_id)
            .bind(owner)
            .execute(pool)
            .await
            .unwrap();
        account_id
    }

    /// Seed a decryptable `oauth_tokens` row for `account_id` so
    /// `get_slack_bot_token` (the FIRST thing `capture_huddle` calls) succeeds
    /// and execution reaches the destination-resolution step under test.
    ///
    /// Foundation PR 1.2 (CS-031): `get_slack_bot_token` resolves the account
    /// DEK via `crate::crypto::resolve_dek` now, not the retired
    /// `ACCOUNT_MASTER_KEK`-XOR derivation — so this provisions a REAL
    /// wrapped DEK on `accounts.dek_wrapped` (mirroring
    /// `clerk_provision::provision_account_dek`) and encrypts the fake token
    /// under that SAME DEK, rather than a formula derived purely from
    /// `account_id`. Requires `MASTER_KEK_HEX` to already be set in the
    /// process environment.
    async fn seed_slack_oauth_token(pool: &PgPool, account_id: Uuid) {
        use coralswarm_core::kek::{generate_dek, load_kek, wrap_dek};
        let kek = load_kek().expect("MASTER_KEK_HEX must be set before calling seed_slack_oauth_token");
        let dek = generate_dek();
        let wrapped = wrap_dek(&kek, &dek).expect("wrap_dek must succeed with a valid KEK");
        sqlx::query("UPDATE accounts SET dek_wrapped = $1 WHERE id = $2")
            .bind(&wrapped)
            .bind(account_id)
            .execute(pool)
            .await
            .expect("seed dek_wrapped");

        let encrypted =
            encrypt_token(&dek, "xoxb-test-token").expect("encrypt_token must succeed");
        sqlx::query(
            "INSERT INTO oauth_tokens(account_id, provider, scope, access_token,provider_workspace_id) \
             VALUES ($1, 'slack', 'channels:history', $2,'T1')",
        )
        .bind(account_id)
        .bind(encrypted)
        .execute(pool)
        .await
        .expect("seed slack oauth token");
    }

    async fn job_status(pool: &PgPool, job_id: Uuid) -> String {
        sqlx::query_scalar("SELECT status::text FROM jobs WHERE id = $1")
            .bind(job_id)
            .fetch_one(pool)
            .await
            .expect("read job status")
    }

    fn capture_kind(account_id: Uuid, channel_id: &str, huddle_thread_ts: &str) -> JobKind {
        JobKind::SlackHuddleCapture {
            account_id,
            workspace_id: "T1".to_string(),
            channel_id: channel_id.to_string(),
            huddle_thread_ts: huddle_thread_ts.to_string(),
            canvas_file_id: "F000TEST".to_string(),
            participant_history: vec![],
            date_start: None,
            date_end: None,
        }
    }

    #[tokio::test]
    async fn second_trigger_for_an_already_ingested_huddle_is_a_no_op_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping second_trigger_for_an_already_ingested_huddle_is_a_no_op_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "orgt_huddle_idem_ingested";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let account_id = seed_account(&pool).await;
        let channel_id = "C-idem-ingested";
        let huddle_thread_ts = "1.1";

        // Simulate a prior successful capture — a terminal ledger row with
        // processed_at set, skip_reason NULL.
        record_capture_outcome(&pool, account_id, channel_id, huddle_thread_ts, None)
            .await
            .expect("seed ingested ledger row");

        let kind = capture_kind(account_id, channel_id, huddle_thread_ts);
        let job_id = queue::enqueue(&pool, &kind, Some(account_id), None, 0)
            .await
            .expect("enqueue")
            .expect("not deduped");

        let jobs = queue::dequeue(&pool, &["SlackHuddleCapture"], 10).await.expect("dequeue");
        assert_eq!(jobs.len(), 1);
        process_job(
            &pool,
            &TenantId::Consumer,
            &pool,
            &reqwest::Client::new(),
            None,
            jobs.into_iter().next().unwrap(),
        )
        .await;

        assert_eq!(
            job_status(&pool, job_id).await,
            "done",
            "a redundant trigger for an already-ingested huddle must complete as a no-op, not fail/retry"
        );

        let row_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM slack_huddle_captures WHERE account_id = $1 AND channel_id = $2 AND huddle_thread_ts = $3",
        )
        .bind(account_id)
        .bind(channel_id)
        .bind(huddle_thread_ts)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(row_count, 1, "no duplicate ledger row must be written");

        pool.close().await;
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    #[tokio::test]
    async fn second_trigger_for_an_already_skipped_huddle_is_a_no_op_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping second_trigger_for_an_already_skipped_huddle_is_a_no_op_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "orgt_huddle_idem_skipped";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let account_id = seed_account(&pool).await;
        let channel_id = "C-idem-skipped";
        let huddle_thread_ts = "2.2";

        // Simulate a prior decided skip — a terminal ledger row with
        // skip_reason set, processed_at NULL.
        record_capture_outcome(
            &pool,
            account_id,
            channel_id,
            huddle_thread_ts,
            Some("canvas has no Summary section"),
        )
        .await
        .expect("seed skipped ledger row");

        let kind = capture_kind(account_id, channel_id, huddle_thread_ts);
        let job_id = queue::enqueue(&pool, &kind, Some(account_id), None, 0)
            .await
            .expect("enqueue")
            .expect("not deduped");

        let jobs = queue::dequeue(&pool, &["SlackHuddleCapture"], 10).await.expect("dequeue");
        assert_eq!(jobs.len(), 1);
        process_job(
            &pool,
            &TenantId::Consumer,
            &pool,
            &reqwest::Client::new(),
            None,
            jobs.into_iter().next().unwrap(),
        )
        .await;

        assert_eq!(
            job_status(&pool, job_id).await,
            "done",
            "a redundant trigger for an already-skipped huddle must complete as a no-op, not \
             re-attempt the fetch"
        );

        let (skip_reason, processed_at): (Option<String>, Option<chrono::DateTime<Utc>>) = sqlx::query_as(
            "SELECT skip_reason, processed_at FROM slack_huddle_captures \
             WHERE account_id = $1 AND channel_id = $2 AND huddle_thread_ts = $3",
        )
        .bind(account_id)
        .bind(channel_id)
        .bind(huddle_thread_ts)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            skip_reason.as_deref(),
            Some("canvas has no Summary section"),
            "the original skip reason must survive untouched"
        );
        assert!(processed_at.is_none(), "a skipped huddle must never gain a processed_at retroactively");

        pool.close().await;
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    /// Route `channel_id` in workspace `T1` to `graph_id` (a
    /// `slack_watched_channels` `single` row) and return its ingest grant.
    async fn route_channel(pool: &PgPool, account_id: Uuid, channel_id: &str, graph_id: Uuid) -> crate::ingest::OrgSharedGrant {
        sqlx::query(
            "INSERT INTO slack_watched_channels (account_id, user_id, mode, graph_id, workspace_id, channel_id, display_name) \
             SELECT id, owner_user_id, 'single', $2, 'T1', $3, 'general' FROM accounts WHERE id = $1",
        )
        .bind(account_id)
        .bind(graph_id)
        .bind(channel_id)
        .execute(pool)
        .await
        .unwrap();
        let (grant, _) = crate::ingest::OrgSharedGrant::slack_channel(
            pool, pool, &crate::tenancy::TenantId::Consumer, account_id, crate::routes::slack::AccountPlane::Personal, "T1", channel_id,
        )
        .await
        .unwrap();
        grant
    }

    async fn seed_graph(pool: &PgPool, account_id: Uuid) -> Uuid {
        sqlx::query_scalar("INSERT INTO graphs(account_id, name) VALUES ($1, 'home') RETURNING id")
            .bind(account_id)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    /// Provision `accounts.dek_wrapped` exactly like
    /// `clerk_provision::provision_account_dek` does, so `crypto::resolve_dek`
    /// (used by `ingest_huddle_notes` for blob encryption, AND — as of
    /// Foundation PR 1.2 / CS-031 — by `get_slack_bot_token` for oauth-token
    /// decryption too; the two used to be separate KEK schemes, see
    /// `crypto::derive_legacy_oauth_account_dek`'s doc comment for the
    /// retired one) succeeds. Requires `MASTER_KEK_HEX` to already be set in
    /// the process environment.
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

    // ── BLOCKER 3: terminal vs. retryable capture failures ────────────────────
    //
    // Both tests below are reachable via DB/env state alone, with NO real
    // Slack API or network call — `get_slack_bot_token` runs BEFORE anything
    // else in `capture_huddle`, so its two failure modes (missing row vs.
    // missing/invalid KEK) isolate the Terminal/Retryable distinction (Blocker
    // 3, `huddle.rs:404`) cheaply and deterministically.

    #[tokio::test]
    async fn no_oauth_token_is_terminal_and_recorded_as_skipped_not_retried_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping no_oauth_token_is_terminal_and_recorded_as_skipped_not_retried_pg: set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_huddle_terminal_no_token";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        // Serialized against every other test in the crate that touches
        // `MASTER_KEK_HEX` (`workers::reembed::MASTER_KEK_HEX_ENV_LOCK`'s doc
        // comment) — held across the `.await`s below, not just around the
        // `set_var`, so no concurrent test can flip the value out from under
        // `process_job`'s read of it.
        let _guard = crate::workers::reembed::MASTER_KEK_HEX_ENV_LOCK.lock().await;
        let prior_kek = std::env::var("MASTER_KEK_HEX").ok();

        // Valid KEK, so the ONLY missing ingredient is the oauth_tokens row
        // itself — isolates the "no Slack oauth token" terminal condition
        // from the KEK-missing retryable condition tested below.
        // SAFETY: `_guard` (held for this test's entire body) serializes
        // every `MASTER_KEK_HEX` mutation against any other test in the
        // crate that might touch it.
        unsafe {
            std::env::set_var("MASTER_KEK_HEX", hex::encode([0x11u8; 32]));
        }

        let account_id = seed_account(&pool).await;
        // A real wrapped DEK is required for `get_slack_bot_token` to reach
        // its row lookup at all (`crate::crypto::resolve_dek` runs BEFORE the
        // `oauth_tokens` SELECT) — see this test module's own doc comment.
        seed_dek(&pool, account_id).await;
        let channel_id = "C-no-token";
        let huddle_thread_ts = "5.5";

        // Deliberately NO oauth_tokens row for this account — an
        // uninstalled/revoked Slack app, which will never heal on retry.
        let kind = capture_kind(account_id, channel_id, huddle_thread_ts);
        let job_id = queue::enqueue(&pool, &kind, Some(account_id), None, 0)
            .await
            .expect("enqueue")
            .expect("not deduped");

        let jobs = queue::dequeue(&pool, &["SlackHuddleCapture"], 10).await.expect("dequeue");
        assert_eq!(jobs.len(), 1);
        process_job(
            &pool,
            &TenantId::Consumer,
            &pool,
            &reqwest::Client::new(),
            None,
            jobs.into_iter().next().unwrap(),
        )
        .await;

        // Track B: a Terminal capture failure is a `FailureClass::Structural`
        // CouldNotDo (this worker cannot capture the huddle — a revoked
        // Slack app will not heal on retry), so it now dead-letters instead
        // of completing — see `finish_as_terminal_failure`'s doc comment.
        // Still NOT `pending` (never retried) — only the terminal status
        // changed, from a misleadingly green `done` to `dead`.
        assert_eq!(
            job_status(&pool, job_id).await,
            "dead",
            "a Terminal capture failure must dead-letter immediately, not retry (pending) or \
             report success (done)"
        );

        let (skip_reason, processed_at): (Option<String>, Option<chrono::DateTime<Utc>>) = sqlx::query_as(
            "SELECT skip_reason, processed_at FROM slack_huddle_captures \
             WHERE account_id = $1 AND channel_id = $2 AND huddle_thread_ts = $3",
        )
        .bind(account_id)
        .bind(channel_id)
        .bind(huddle_thread_ts)
        .fetch_one(&pool)
        .await
        .expect("a Terminal condition must write a ledger row");

        assert!(
            skip_reason.as_deref().is_some_and(|r| r.contains("no Slack oauth token")),
            "skip_reason must carry the terminal reason: {skip_reason:?}"
        );
        assert!(processed_at.is_none(), "a Terminal skip must never be recorded as Ingested");

        // Restore the prior value so this test cannot leak state into
        // another — still under `_guard`.
        match prior_kek {
            Some(v) => unsafe { std::env::set_var("MASTER_KEK_HEX", v) },
            None => unsafe { std::env::remove_var("MASTER_KEK_HEX") },
        }
        drop(_guard);

        pool.close().await;
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    #[tokio::test]
    async fn missing_master_kek_hex_is_retryable_and_writes_no_ledger_row_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping missing_master_kek_hex_is_retryable_and_writes_no_ledger_row_pg: set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_huddle_retryable_no_kek";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        // Serialized against every other test in the crate that touches
        // `MASTER_KEK_HEX` (`workers::reembed::MASTER_KEK_HEX_ENV_LOCK`'s doc
        // comment) — held across the `.await`s below so no concurrent test
        // can set it back while this one is relying on it being absent.
        let _guard = crate::workers::reembed::MASTER_KEK_HEX_ENV_LOCK.lock().await;
        let prior_kek = std::env::var("MASTER_KEK_HEX").ok();

        // No `MASTER_KEK_HEX` at all — a transient config condition from
        // this job's point of view (it says nothing about whether the Slack
        // app itself is installed), so it must be retried, not recorded as a
        // permanent skip. `crate::crypto::resolve_dek` checks the KEK env
        // var BEFORE it ever reads `accounts.dek_wrapped`, so this account
        // needs no `seed_dek` call to prove the point — the failure happens
        // earlier than that read either way.
        // SAFETY: `_guard` (held for this test's entire body) serializes
        // every `MASTER_KEK_HEX` mutation against any other test in the
        // crate that might touch it.
        unsafe {
            std::env::remove_var("MASTER_KEK_HEX");
        }

        let account_id = seed_account(&pool).await;
        let channel_id = "C-no-kek";
        let huddle_thread_ts = "6.6";

        let kind = capture_kind(account_id, channel_id, huddle_thread_ts);
        let job_id = queue::enqueue(&pool, &kind, Some(account_id), None, 0)
            .await
            .expect("enqueue")
            .expect("not deduped");

        let jobs = queue::dequeue(&pool, &["SlackHuddleCapture"], 10).await.expect("dequeue");
        assert_eq!(jobs.len(), 1);
        process_job(
            &pool,
            &TenantId::Consumer,
            &pool,
            &reqwest::Client::new(),
            None,
            jobs.into_iter().next().unwrap(),
        )
        .await;

        let status = job_status(&pool, job_id).await;
        assert_ne!(status, "done", "a Retryable capture failure must NOT complete the job as done");

        let row_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM slack_huddle_captures WHERE account_id = $1 AND channel_id = $2 AND huddle_thread_ts = $3",
        )
        .bind(account_id)
        .bind(channel_id)
        .bind(huddle_thread_ts)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(row_count, 0, "a Retryable failure must write NO ledger row");

        // Restore the prior value so this test cannot leak state into
        // another — still under `_guard`.
        match prior_kek {
            Some(v) => unsafe { std::env::set_var("MASTER_KEK_HEX", v) },
            None => unsafe { std::env::remove_var("MASTER_KEK_HEX") },
        }
        drop(_guard);

        pool.close().await;
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    // ── BLOCKER 4: an org fan-out with no mappable member must be retryable,
    // never a permanent skip ────────────────────────────────────────────────
    //
    // `resolve_org_fanout_destinations` unconditionally returns an empty
    // `graphs` list today (capability C5's identity mapping is unwired) —
    // every org-account huddle with no explicit `slack_watched_channels` row
    // takes this branch. Before this fix, `capture_huddle` reported that as
    // `Ok(CaptureOutcome::Skipped(..))`, which `process_job` records as a
    // PERMANENT ledger row (`record_capture_outcome`'s `ON CONFLICT DO
    // NOTHING` means it can never be overwritten) and marks the job `done` —
    // silently and irrecoverably suppressing every org huddle seen between
    // today and whenever capability C5 ships.
    #[tokio::test]
    async fn org_fanout_empty_is_retryable_not_permanently_skipped_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping org_fanout_empty_is_retryable_not_permanently_skipped_pg: set \
                 ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_huddle_org_fanout_retryable";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        // Serialized against every other test in the crate that touches
        // `MASTER_KEK_HEX` (`workers::reembed::MASTER_KEK_HEX_ENV_LOCK`'s doc
        // comment) — held across the `.await`s below, not just around the
        // `set_var`.
        let _guard = crate::workers::reembed::MASTER_KEK_HEX_ENV_LOCK.lock().await;
        let prior_kek = std::env::var("MASTER_KEK_HEX").ok();

        // SAFETY: `_guard` (held for this test's entire body) serializes
        // every `MASTER_KEK_HEX` mutation against any other test in the
        // crate that might touch it.
        unsafe {
            std::env::set_var("MASTER_KEK_HEX", hex::encode([0x55u8; 32]));
        }

        let account_id = seed_org_account(&pool).await;
        seed_slack_oauth_token(&pool, account_id).await;

        let channel_id = "C-org-fanout";
        let huddle_thread_ts = "9.9";

        // No `slack_watched_channels` row, no identity mapping wired — this
        // account's one member is unmapped, so
        // `resolve_org_fanout_destinations` returns an EMPTY graph list.
        let kind = capture_kind(account_id, channel_id, huddle_thread_ts);
        let job_id = queue::enqueue(&pool, &kind, Some(account_id), None, 0)
            .await
            .expect("enqueue")
            .expect("not deduped");

        let jobs = queue::dequeue(&pool, &["SlackHuddleCapture"], 10).await.expect("dequeue");
        assert_eq!(jobs.len(), 1);
        process_job(
            &pool,
            &TenantId::Consumer,
            &pool,
            &reqwest::Client::new(),
            None,
            jobs.into_iter().next().unwrap(),
        )
        .await;

        let status = job_status(&pool, job_id).await;
        assert_ne!(
            status,
            "done",
            "BLOCKER 4: an org account with no mappable fan-out destination (capability C5 \
             unwired) must be treated as a retryable, temporary condition, not completed/\
             permanently skipped — got status {status:?}"
        );

        let row_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM slack_huddle_captures WHERE account_id = $1 AND channel_id = $2 \
             AND huddle_thread_ts = $3",
        )
        .bind(account_id)
        .bind(channel_id)
        .bind(huddle_thread_ts)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            row_count, 0,
            "BLOCKER 4: an infrastructure-gap skip must never write a PERMANENT ledger row — \
             ON CONFLICT DO NOTHING would make it unrecoverable once capability C5 ships"
        );
        let atoms: i64 = sqlx::query_scalar("SELECT count(*) FROM atoms").fetch_one(&pool).await.unwrap();
        assert_eq!(atoms, 0, "no routing, nothing stored");

        // Restore the prior value so this test cannot leak state into
        // another — still under `_guard`.
        match prior_kek {
            Some(v) => unsafe { std::env::set_var("MASTER_KEK_HEX", v) },
            None => unsafe { std::env::remove_var("MASTER_KEK_HEX") },
        }
        drop(_guard);

        pool.close().await;
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    // ── BLOCKER 1: a mid-flight crash's atoms must be re-driven to completion ──

    #[tokio::test]
    async fn crash_then_retry_redrives_full_atom_set_to_completion_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping crash_then_retry_redrives_full_atom_set_to_completion_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "orgt_huddle_crash_retry";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        // Serialized against every other test in the crate that touches
        // `MASTER_KEK_HEX` — this variable is also mutated by
        // `workers::reembed`'s own tests, and both link into the same
        // `--lib` test binary, so they must share one lock
        // (`MASTER_KEK_HEX_ENV_LOCK`'s doc comment). Held across the
        // `.await`s below, not just around the `set_var`.
        let _guard = crate::workers::reembed::MASTER_KEK_HEX_ENV_LOCK.lock().await;
        let prior_kek = std::env::var("MASTER_KEK_HEX").ok();

        // SAFETY: `_guard` (held for this test's entire body) serializes
        // every `MASTER_KEK_HEX` mutation against any other test in the
        // crate that might touch it.
        unsafe {
            std::env::set_var("MASTER_KEK_HEX", hex::encode([0x22u8; 32]));
        }

        let account_id = seed_account(&pool).await;
        seed_dek(&pool, account_id).await;
        let graph_id = seed_graph(&pool, account_id).await;
        let channel_id = "C-crash-retry";
        let huddle_thread_ts = "7.7";
        let text = "Alice and Bob agreed to ship the Slack huddle connector fix by Friday, with tests \
                    covering the crash-then-retry regression path end to end.";
        let participants = vec!["Alice".to_string(), "Bob".to_string()];
        let channel_name = "general";

        let payload = CapturePayload {
            account_id,
            workspace_id: "T1".to_string(),
            channel_id: channel_id.to_string(),
            huddle_thread_ts: huddle_thread_ts.to_string(),
            canvas_file_id: "F000TEST".to_string(),
            participant_history: vec![],
            date_start: None,
            date_end: None,
        };

        // Manually seed the atom rows exactly as a crashed prior run would
        // have left them: committed (so a retry's INSERT ... ON CONFLICT DO
        // NOTHING treats them as pre-existing, and the OLD code's
        // `RETURNING`-only check would see zero "new" rows), content_inline
        // still NULL (the blob/inline write never ran), and crucially NO
        // ledger row and NO EmbedAtoms job — exactly the shape
        // `jobs::queue::reap` resetting a job killed mid-flight produces
        // (this module's doc comment).
        let chunks = chunk(text, ChunkStrategy::Generic);
        assert!(!chunks.is_empty(), "fixture text must actually chunk");
        let source_label = format!("slack-huddle {channel_id}");
        let source_ids: Vec<String> = chunks
            .iter()
            .map(|ci| format!("{source_label}:{huddle_thread_ts}:chunk:{}", ci.position))
            .collect();
        let pre_atom_ids: Vec<Uuid> = chunks.iter().map(|_| Uuid::new_v4()).collect();
        let blob_keys: Vec<String> = pre_atom_ids
            .iter()
            .map(|id| coralswarm_core::r2::personal_blob_key(graph_id, *id))
            .collect();

        sqlx::query(
            r#"
            INSERT INTO atoms
                (atom_id, graph_id, source_type, source_id, content_blob_key,
                 participants, timestamp, ingested_by, scope, created_at,
                 channel_id, channel_name, workspace_id, header_len)
            SELECT unnest($1::uuid[]), $2, 'transcript', unnest($3::text[]), unnest($4::text[]),
                   $5, now(), NULL, 'account', now(), $6, $7, $8, 0
            "#,
        )
        .bind(&pre_atom_ids)
        .bind(graph_id)
        .bind(&source_ids)
        .bind(&blob_keys)
        .bind(&participants)
        .bind(channel_id)
        .bind(channel_name)
        .bind("T1")
        .execute(&pool)
        .await
        .expect("seed pre-existing atoms simulating a mid-flight crash");

        // Confirm the crash signature: no ledger row exists yet.
        let recorded = capture_already_recorded(&pool, account_id, channel_id, huddle_thread_ts)
            .await
            .unwrap();
        assert!(!recorded, "no ledger row should exist yet for the crashed run");

        // Re-drive — exactly what `process_job` does on the retry after
        // `reap` resets the job to `pending`.
        let atom_count = ingest_huddle_notes(
            &pool,
            &TenantId::Consumer,
            None, // dev-mode / no-R2 path — exercises the content_inline branch
            &payload,
            &route_channel(&pool, account_id, channel_id, graph_id).await,
            text,
            &participants,
            channel_name,
            None, // no attribution under test here — see the dedicated ingested_by tests
        )
        .await
        .expect("re-driving ingest for a crashed run must succeed, not error");

        assert_eq!(
            atom_count,
            pre_atom_ids.len(),
            "the full pre-existing atom set must be counted, not zero (BLOCKER 1)"
        );

        // The blob/inline write must have happened for the PRE-EXISTING
        // atoms this time.
        let inline_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM atoms WHERE atom_id = ANY($1) AND content_inline IS NOT NULL",
        )
        .bind(&pre_atom_ids)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            inline_count as usize,
            pre_atom_ids.len(),
            "content_inline must be (re)written for every pre-existing atom, not skipped"
        );

        // An EmbedAtoms job must have been enqueued covering the pre-existing
        // atom ids.
        let embed_atom_ids: Vec<Uuid> = sqlx::query_scalar(
            "SELECT jsonb_array_elements_text(payload->'atom_ids')::uuid FROM jobs WHERE kind = 'EmbedAtoms'",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        let mut expected = pre_atom_ids.clone();
        expected.sort();
        let mut got = embed_atom_ids;
        got.sort();
        assert_eq!(
            got, expected,
            "EmbedAtoms must be enqueued for the FULL pre-existing atom set, not skipped (BLOCKER 1)"
        );

        // Restore the prior value so this test cannot leak state into
        // another — still under `_guard`.
        match prior_kek {
            Some(v) => unsafe { std::env::set_var("MASTER_KEK_HEX", v) },
            None => unsafe { std::env::remove_var("MASTER_KEK_HEX") },
        }
        drop(_guard);

        pool.close().await;
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    // ── BLOCKER 2: an R2 upload failure must orphan nothing ───────────────────

    #[tokio::test]
    async fn r2_upload_failure_deletes_the_atom_and_excludes_it_from_embed_and_count_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping r2_upload_failure_deletes_the_atom_and_excludes_it_from_embed_and_count_pg: set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_huddle_r2_orphan";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        // Serialized against every other test in the crate that touches
        // `MASTER_KEK_HEX` — shared with `workers::reembed`'s own tests
        // (`MASTER_KEK_HEX_ENV_LOCK`'s doc comment). Held across the
        // `.await`s below, not just around the `set_var`.
        let _guard = crate::workers::reembed::MASTER_KEK_HEX_ENV_LOCK.lock().await;
        let prior_kek = std::env::var("MASTER_KEK_HEX").ok();

        // SAFETY: `_guard` (held for this test's entire body) serializes
        // every `MASTER_KEK_HEX` mutation against any other test in the
        // crate that might touch it.
        unsafe {
            std::env::set_var("MASTER_KEK_HEX", hex::encode([0x33u8; 32]));
        }

        let account_id = seed_account(&pool).await;
        seed_dek(&pool, account_id).await;
        let graph_id = seed_graph(&pool, account_id).await;
        let channel_id = "C-r2-fail";
        let huddle_thread_ts = "8.8";
        let text = "This is a substantive huddle summary long enough to survive the pointer-junk \
                    check, used only to exercise the R2-upload-failure cleanup path end to end.";
        let participants = vec!["Alice".to_string()];
        let channel_name = "general";

        let payload = CapturePayload {
            account_id,
            workspace_id: "T1".to_string(),
            channel_id: channel_id.to_string(),
            huddle_thread_ts: huddle_thread_ts.to_string(),
            canvas_file_id: "F000TEST".to_string(),
            participant_history: vec![],
            date_start: None,
            date_end: None,
        };

        // An R2 client that reliably fails EVERY upload: the account id
        // segment routes to a real Cloudflare edge whose TLS handshake
        // rejects credentials for an account that doesn't exist, which the
        // AWS SDK surfaces as an ordinary `SdkError` — `R2Client`'s blanket
        // `From<SdkError<..>>` maps that to `R2Error::S3`. No real R2 bucket
        // is read or written; this is here purely to make `put_atom_blob`
        // return `Err` deterministically so BLOCKER 2's cleanup wiring (not
        // the AWS SDK itself) is what's under test.
        let r2 = coralswarm_core::r2::R2Client::new(
            "huddle-blocker2-test-nonexistent-account",
            "fake-key",
            "fake-secret",
            "fake-bucket",
        )
        .await;

        let error = ingest_huddle_notes(
            &pool,
            &TenantId::Consumer,
            Some(&r2),
            &payload,
            &route_channel(&pool, account_id, channel_id, graph_id).await,
            text,
            &participants,
            channel_name,
            None, // no attribution under test here — see the dedicated ingested_by tests
        )
        .await
        .expect_err("failed source upload must be retried without committing partial rows");

        assert!(matches!(error,CaptureError::Retryable(_)));

        let surviving_atoms: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM atoms WHERE graph_id = $1")
            .bind(graph_id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(
            surviving_atoms, 0,
            "an R2 upload failure must leave NO atom row behind (BLOCKER 2)"
        );

        let embed_jobs: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM jobs WHERE kind = 'EmbedAtoms'")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(
            embed_jobs, 0,
            "a failed R2 upload must never be handed to EmbedAtoms (BLOCKER 2)"
        );

        // Restore the prior value so this test cannot leak state into
        // another — still under `_guard`.
        match prior_kek {
            Some(v) => unsafe { std::env::set_var("MASTER_KEK_HEX", v) },
            None => unsafe { std::env::remove_var("MASTER_KEK_HEX") },
        }
        drop(_guard);

        pool.close().await;
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    // ── Journal completeness gap 2: huddle `ingested_by` attribution ──────────
    //
    // Same lesson as the sibling Slack-message fixtures
    // (`routes::slack_events::slack_events`'s own gap-1 tests): the
    // pre-existing pg fixtures in this file (and in
    // `backend/tests/journal_rollups_pg.rs`) insert huddle atoms WITH
    // `ingested_by` set by hand, which is exactly why the real WRITER
    // (`ingest_huddle_notes`, always binding a literal `NULL`) survived
    // unnoticed. These drive the real writer + the real attribution resolver.

    async fn seed_user(pool: &PgPool) -> Uuid {
        sqlx::query_scalar("INSERT INTO users(id, email) VALUES (gen_random_uuid(), $1) RETURNING id")
            .bind(format!("user-{}@test.local", Uuid::new_v4()))
            .fetch_one(pool)
            .await
            .unwrap()
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

    /// No attendee resolves ⇒ `None` — never guessed, matches the "unmapped
    /// author leaves `ingested_by` NULL" degradation the Slack-message path
    /// accepts too.
    #[tokio::test]
    async fn resolve_huddle_attribution_returns_none_when_nobody_maps_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping resolve_huddle_attribution_returns_none_when_nobody_maps_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "orgt_huddle_attrib_none";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let attendees = vec!["U-GHOST-1".to_string(), "U-GHOST-2".to_string()];
        let resolved = resolve_huddle_attribution(&pool, "T1", &attendees).await.unwrap();
        assert_eq!(resolved, None, "no confirmed identity ⇒ no guessed attribution");

        pool.close().await;
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    /// An unmapped attendee ahead of a mapped one in `participant_history`
    /// order is skipped, not treated as a wall — the first attendee that
    /// actually resolves wins ("first SEATED attendee", not "first
    /// attendee").
    #[tokio::test]
    async fn resolve_huddle_attribution_skips_unmapped_and_uses_first_mapped_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping resolve_huddle_attribution_skips_unmapped_and_uses_first_mapped_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_huddle_attrib_skip_unmapped";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let mapped_user = seed_user(&pool).await;
        seed_confirmed_identity(&pool, "T1", "U-MAPPED", mapped_user).await;

        let attendees = vec!["U-UNMAPPED".to_string(), "U-MAPPED".to_string()];
        let resolved = resolve_huddle_attribution(&pool, "T1", &attendees).await.unwrap();
        assert_eq!(
            resolved,
            Some(mapped_user),
            "the unmapped attendee ahead of the mapped one must be skipped, not block resolution"
        );

        pool.close().await;
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    /// Two mapped attendees ⇒ the FIRST one (participant-history order) wins
    /// — the documented "first seated attendee, not one-atom-per-attendee"
    /// design in `resolve_huddle_attribution`'s doc comment.
    #[tokio::test]
    async fn resolve_huddle_attribution_prefers_earlier_attendee_over_a_later_mapped_one_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping resolve_huddle_attribution_prefers_earlier_attendee_over_a_later_mapped_one_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_huddle_attrib_first_wins";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let first = seed_user(&pool).await;
        seed_confirmed_identity(&pool, "T1", "U-FIRST", first).await;
        let second = seed_user(&pool).await;
        seed_confirmed_identity(&pool, "T1", "U-SECOND", second).await;

        let attendees = vec!["U-FIRST".to_string(), "U-SECOND".to_string()];
        let resolved = resolve_huddle_attribution(&pool, "T1", &attendees).await.unwrap();
        assert_eq!(
            resolved,
            Some(first),
            "with two mapped attendees, the earlier one in participant-history order must win"
        );

        pool.close().await;
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    /// `ingest_huddle_notes` actually BINDS the resolved `ingested_by` into
    /// the atom row — the writer half of the fix, exercised the same way
    /// BLOCKER 1/2's tests already exercise this function directly.
    #[tokio::test]
    async fn ingest_huddle_notes_stamps_the_resolved_ingested_by_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping ingest_huddle_notes_stamps_the_resolved_ingested_by_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "orgt_huddle_ingested_by_stamped";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let _guard = crate::workers::reembed::MASTER_KEK_HEX_ENV_LOCK.lock().await;
        let prior_kek = std::env::var("MASTER_KEK_HEX").ok();
        unsafe {
            std::env::set_var("MASTER_KEK_HEX", hex::encode([0x77u8; 32]));
        }

        let account_id = seed_account(&pool).await;
        seed_dek(&pool, account_id).await;
        let graph_id = seed_graph(&pool, account_id).await;
        let attendee = seed_user(&pool).await;

        let channel_id = "C-ingested-by";
        let huddle_thread_ts = "9.9";
        let text = "This substantive huddle summary is long enough to survive the pointer-junk \
                    check, used only to exercise the ingested_by attribution stamp end to end.";
        let participants = vec!["Attendee".to_string()];
        let channel_name = "general";

        let payload = CapturePayload {
            account_id,
            workspace_id: "T1".to_string(),
            channel_id: channel_id.to_string(),
            huddle_thread_ts: huddle_thread_ts.to_string(),
            canvas_file_id: "F000TEST".to_string(),
            participant_history: vec![],
            date_start: None,
            date_end: None,
        };

        let atom_count = ingest_huddle_notes(
            &pool,
            &TenantId::Consumer,
            None, // dev-mode / no-R2 path
            &payload,
            &route_channel(&pool, account_id, channel_id, graph_id).await,
            text,
            &participants,
            channel_name,
            Some(attendee),
        )
        .await
        .expect("ingest with a resolved attribution must succeed");
        assert!(atom_count > 0, "at least one atom must be ingested");

        let version:(String,String,Option<Uuid>)=sqlx::query_as(
            "SELECT representation,raw_inline,ingested_by FROM source_versions WHERE graph_id=$1",
        ).bind(graph_id).fetch_one(&pool).await.unwrap();
        assert_eq!(version.0,"provider_summary");
        assert_eq!(version.1,text);
        assert_eq!(version.2,Some(attendee));
        let meetings:i64=sqlx::query_scalar("SELECT count(*) FROM meeting_transcripts WHERE graph_id=$1")
            .bind(graph_id).fetch_one(&pool).await.unwrap();
        assert_eq!(meetings,0,"generated huddle notes must not become a verbatim transcript");
        // So they are not meeting-kind either: no summary is queued.
        let summaries: i64 = sqlx::query_scalar("SELECT count(*) FROM jobs WHERE kind = 'GenerateMeetingSummary'")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(summaries, 0);
        // The embed job dedupes on the shared `embed_key`, like every source.
        let embed_keys: Vec<Option<String>> =
            sqlx::query_scalar("SELECT idempotency_key FROM jobs WHERE kind = 'EmbedAtoms'")
                .fetch_all(&pool)
                .await
                .unwrap();
        let source_id = format!("slack-huddle {channel_id}:{huddle_thread_ts}");
        let content_hash = hex::encode(Sha256::digest(text.as_bytes()));
        assert_eq!(
            embed_keys,
            vec![Some(crate::ingest::capture::embed_key(graph_id, "transcript", &source_id, &content_hash))]
        );

        let ingested_by: Vec<Option<Uuid>> =
            sqlx::query_scalar("SELECT ingested_by FROM atoms WHERE graph_id = $1")
                .bind(graph_id)
                .fetch_all(&pool)
                .await
                .unwrap();
        assert!(!ingested_by.is_empty(), "at least one atom row must exist");
        assert!(
            ingested_by.iter().all(|v| *v == Some(attendee)),
            "every huddle atom for this destination must carry the resolved ingested_by, got \
             {ingested_by:?}"
        );

        match prior_kek {
            Some(v) => unsafe { std::env::set_var("MASTER_KEK_HEX", v) },
            None => unsafe { std::env::remove_var("MASTER_KEK_HEX") },
        }
        drop(_guard);

        pool.close().await;
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }
}
