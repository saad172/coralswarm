//! Post-meeting summary generation worker (cs-uxp3a-meeting-summary).
//!
//! Polls for `GenerateMeetingSummary` jobs and produces a structured summary for
//! one ingested meeting — `{ overview, decisions:[{text,ts_ms}],
//! action_items:[{text,owner,ts_ms}] }` — via a map-reduce over the transcript:
//! Haiku extracts decisions/action items per chunk (the "map"), then Sonnet
//! merges the partials into the final deduped structure (the "reduce"), matching
//! the repo's "Haiku for extraction, Sonnet for synthesis" idiom.
//!
//! **Owner check**: "Local only" no longer exists, so no account policy blocks
//! a summary. A job whose owning account cannot be resolved still fails closed
//! (the summary is left `blocked`); the per-meeting `allow_cloud_override`
//! bypasses that block and is kept for rows blocked before the setting was
//! removed.
//!
//! **Timestamps**: when the meeting was ingested with timestamped `segments`,
//! each map line is prefixed with an `@<start_ms>` marker so the model can anchor
//! every decision/action-item to a transcript moment; the result is snapped to
//! the nearest real segment start so hallucinated values can't leak through. A
//! meeting with no segments (older ingests / sources without timing) still gets a
//! summary — with `ts_ms: null`.
//!
//! **Privacy**: transcript text and generated summary text are NEVER logged, and
//! `summary_error` (surfaced to the client) is a short category, never model
//! output. Only ids and counts are logged.
//!
//! ## Per-job RLS scope (org-tenancy Phase W.2)
//!
//! `meeting_transcripts` (transcript body, summary status/result columns) and
//! `graphs` are both on migration `0083_consumer_rls`'s policy roster, so every
//! `Store` call below runs under the job's [`JobScope`]: on the consumer plane
//! each statement gets a fresh short `tenancy::scope::begin_account_scoped`
//! transaction for `job.account_id` (Phase W.1's per-job scoping input); on an
//! org tenant each gets a `begin_tenant_system_scoped_without_account`
//! transaction instead. Whole-database isolation does NOT stand in for that
//! scope. The isolation is real and covers the CROSS-ORG boundary only, while
//! `migrations_tenant/0009_tenant_rls` puts member-ocean policies on
//! `meeting_transcripts` and `graphs` inside a tenant DB. Unscoped, once
//! OPS-4-T moves the tenant DSN to the NOBYPASSRLS role, this worker would
//! summarise meetings in the org and personal oceans and find no transcript
//! row for any member or team ocean, which reads as a job with nothing to do.
//! Per statement rather
//! than per job: this worker interleaves DB statements with several LLM
//! round-trips and an optional R2 fetch, and holding a pooled connection (in a
//! transaction Postgres would abort on the first statement error) across those
//! would change both pool pressure and the worker's long-standing
//! per-statement error handling — see [`JobScope`]'s own doc.
//!
//! **This worker does NOT use the shared [`crate::workers::verify_job_scope`]
//! guard**, on purpose. That helper treats "graph resolves to no account" as
//! benign (the caller's pass then finds nothing). Here that same condition is
//! load-bearing: an unresolvable owner must fail CLOSED (block the summary),
//! never be treated as "no work to do". So the resolver is called once,
//! inline, and its result feeds both the ownership check and the block.
//!
//! ## Managed enrichment (source-agnostic plan PR 10)
//!
//! Every map and reduce call is its own `managed_enrichment` logical call
//! (`crate::managed::enrichment`), charged to the graph's executor under the
//! shared daily cap (`MANAGED_DAILY_CAP_USD_ENRICHMENT`) and a per-graph daily
//! call bound ([`SUMMARY_DAILY_CALLS_ENV`]). Each call binds every input it
//! depends on (COR-35): the transcript snapshot it read (`meeting_transcript`,
//! sha256 over the version id, the raw locator/inline text and the segments),
//! the source version that snapshot projects (`source_version`), every chunk
//! atom of that version (`atom`, revision + sha256 over its stored content
//! identity) and the exact prompt it sent (`summary_map_prompt` /
//! `summary_reduce_prompt`). There is no raw shortcut: the prompt is built
//! from the present, unretracted chunk atoms of the version (stored segments
//! keep their timing only where their text is still in that evidence), and a
//! transcript with no source version or no usable chunk is refused
//! (`summary_error='evidence unavailable'`) without a model call.
//! The summary is published in one transaction that re-locks the transcript
//! row on that snapshot, re-reads the version's chunk atoms, re-verifies the
//! reduce call's receipt inputs and marks them written. Deleting or
//! retracting any bound atom later clears the published summary and queues
//! a regeneration from the evidence that is left.
//!
//! Nothing is claimed unless `ENRICHMENT_PROCESSING_ENABLED=true`, the global
//! `MANAGED_PROCESSING_DISABLED` kill switch is off and
//! [`SUMMARIES_SWITCH_ENV`] is not off. A hold (switch flipped after claim,
//! credits, access, the shared cap, the daily bound, managed storage) never
//! dead-letters: the meeting goes back to `summary_status='pending'` and the
//! job is re-enqueued on the same `summary:{graph}:{source}` key after a
//! delay (the next UTC day for the cap and the bound).

use std::collections::HashSet;
use std::time::Duration;

use coralswarm_core::r2::R2Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::PgPool;
use uuid::Uuid;

use crate::jobs::{
    queue::{self, JobRow},
    JobKind,
};
use crate::llm::model_config;
use crate::managed::enrichment::{self, EnrichmentInput, EnrichmentJob, EnrichmentWork, InputCheck};
use crate::llm::{CompletionRequest, LlmError, LlmProvider, Message, Role, ToolSchema};
use crate::store::{MeetingTranscript, Store, StoreError};
use crate::tenancy::scope::{begin_account_scoped, begin_tenant_system_scoped_without_account};
use crate::tenancy::TenantId;
use crate::workers::embed::{JobScope, UNSCOPED_JOB_ERR};
use crate::workers::tenant_harness::{Tick, WorkerTenant};

// ── Prompts (compiled in) ─────────────────────────────────────────────────────

const MAP_SYSTEM_PROMPT: &str = include_str!("../../prompts/summary_map_v1.txt");
const REDUCE_SYSTEM_PROMPT: &str = include_str!("../../prompts/summary_reduce_v1.txt");

// ── Tuning constants ──────────────────────────────────────────────────────────

/// Jobs dequeued per poll cycle. Kept small: each job is several LLM round-trips.
const JOB_BATCH_SIZE: usize = 4;
/// Idle poll interval when the queue is empty. `pub(crate)`: fed to the
/// tenant harness as this worker's `base_poll` (see `workers::tenant_harness`).
pub(crate) const EMPTY_POLL_SLEEP: Duration = Duration::from_millis(500);
/// Character budget for one map chunk (~3-4k tokens). Whole transcript lines are
/// never split across chunks.
const CHUNK_CHAR_BUDGET: usize = 12_000;
/// Cap on map chunks processed for one meeting (bounds cost on pathological
/// inputs; a normal meeting is a handful of chunks).
const MAX_CHUNKS: usize = 40;
/// Max characters kept for one decision / action-item sentence.
const MAX_ITEM_CHARS: usize = 400;
/// Max characters kept for the overview paragraph.
const MAX_OVERVIEW_CHARS: usize = 2000;
/// Ceiling on decisions / action items in the stored summary.
const MAX_ITEMS: usize = 60;
/// Attempts (including the first) before a transient failure becomes terminal
/// `failed`. Kept in step with the `jobs` table's default `max_attempts` (3) so
/// `summary_status` converges to a terminal state rather than sitting `running`.
const SUMMARY_MAX_ATTEMPTS: i32 = 3;

/// `MEETING_SUMMARIES_ENABLED`: the summaries' own kill switch, on unless
/// `false`/`0`/`off`/`no`. It only narrows the shared
/// `ENRICHMENT_PROCESSING_ENABLED` switch.
pub(crate) const SUMMARIES_SWITCH_ENV: &str = "MEETING_SUMMARIES_ENABLED";
/// `MEETING_SUMMARY_DAILY_CALLS_PER_GRAPH`: map + reduce calls one graph may
/// open per UTC day. A meeting needs one call per chunk plus the reduce
/// (at most [`MAX_CHUNKS`] + 1 = 41); a one-hour meeting is about six.
pub(crate) const SUMMARY_DAILY_CALLS_ENV: &str = "MEETING_SUMMARY_DAILY_CALLS_PER_GRAPH";
pub(crate) const DEFAULT_SUMMARY_DAILY_CALLS_PER_GRAPH: i64 = 200;
/// The marker input every summary call binds (and the daily bound counts):
/// the transcript row and the snapshot the call read.
pub(crate) const INPUT_TRANSCRIPT: &str = "meeting_transcript";
/// The source version the transcript projects (`current_version_id`).
pub(crate) const INPUT_VERSION: &str = "source_version";
/// One chunk atom of that version (the evidence the transcript is captured
/// as). Deleting or retracting any of them retracts a published summary
/// (`managed_enrichment_invalidate_atoms`, tenant 0179 / control 0347).
pub(crate) const INPUT_ATOM: &str = "atom";
/// The exact user prompt of one map call.
const INPUT_MAP_PROMPT: &str = "summary_map_prompt";
/// The exact user prompt of the reduce call.
const INPUT_REDUCE_PROMPT: &str = "summary_reduce_prompt";

/// Whether the summary consumer may claim and spend.
pub(crate) fn summaries_claim_allowed() -> bool {
    enrichment::consumer_claim_allowed(SUMMARIES_SWITCH_ENV)
}

// ── Wire types ────────────────────────────────────────────────────────────────

/// A transcript segment as passed at ingest and stored on `meeting_transcripts`.
#[derive(Debug, Clone, Deserialize)]
struct Segment {
    #[serde(default)]
    speaker: String,
    #[serde(default)]
    start_ms: i64,
    #[serde(default)]
    text: String,
}

/// One extracted decision or action item (owner only meaningful for actions).
#[derive(Debug, Clone, Serialize)]
struct Item {
    text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    owner: Option<String>,
    ts_ms: Option<i64>,
}

/// The map/reduce shape: decisions + action items.
#[derive(Debug, Clone, Default, Serialize)]
struct Extraction {
    decisions: Vec<Item>,
    action_items: Vec<Item>,
}

// ── Worker entry point ────────────────────────────────────────────────────────

/// One summary pass on one tenant's (pool, store) pair: dequeue up to
/// [`JOB_BATCH_SIZE`] `GenerateMeetingSummary` jobs and process them. Driven
/// by `workers::tenant_harness::run_multi_tenant` (org-tenancy PR 2.1), which
/// owns the loop, sleeps, shutdown (stop claiming between ticks; the in-flight
/// batch finishes inside the caller's drain grace window), and per-org idle
/// backoff.
///
/// `control_pool` is the shared control/consumer pool. Jobs/transcripts ride `t.pool`; the DEK rides `t.tenant` (org DEKs
/// live in tenant `account_keys`).
pub async fn tick(
    t: &WorkerTenant,
    control_pool: &PgPool,
    llm: &dyn LlmProvider,
    r2: Option<&R2Client>,
) -> Result<Tick, String> {
    // Switched off: claim nothing, so every job stays pending.
    if !summaries_claim_allowed() {
        return Ok(Tick::idle());
    }
    let jobs = queue::dequeue(&t.pool, &["GenerateMeetingSummary"], JOB_BATCH_SIZE)
        .await
        .map_err(|e| format!("dequeue failed: {e}"))?;

    if jobs.is_empty() {
        return Ok(Tick::idle());
    }

    let n = jobs.len();
    for job in jobs {
        process_job(&t.tenant, &t.pool, control_pool, t.store.as_ref(), llm, r2, job).await;
    }
    Ok(Tick::worked(n))
}

// ── Job processing ────────────────────────────────────────────────────────────

/// `pub` (org-tenancy Phase W.2) so integration tests can drive ONE dequeued
/// job end-to-end against a real Postgres — including the scoping guards —
/// without going through the worker loop.
pub async fn process_job(
    tenant: &TenantId,
    pool: &PgPool,
    control_pool: &PgPool,
    store: &dyn crate::store::Store,
    llm: &dyn LlmProvider,
    r2: Option<&R2Client>,
    job: JobRow,
) {
    let (graph_id, source_id, allow_override) = match parse_payload(&job) {
        Ok(v) => v,
        Err(e) => {
            tracing::error!(job_id = %job.id, error = %e, "summary worker: malformed job payload");
            let _ = queue::fail(pool, job.id, &e).await;
            return;
        }
    };

    // ── Per-job RLS scope (org-tenancy Phase W.2) ─────────────────────────────
    // Established BEFORE the first status write, so no statement in this
    // function ever runs unscoped. Post-W.1 a `GenerateMeetingSummary` job
    // always carries an `account_id` (`queue::dequeue` backfills or
    // dead-letters NULL legacy rows), so `None` can only be an upstream bug.
    let Some(account_id) = job.account_id else {
        tracing::error!(job_id = %job.id, kind = %job.kind, error = UNSCOPED_JOB_ERR, "summary: refusing unscoped job");
        if let Err(qe) = queue::fail(pool, job.id, UNSCOPED_JOB_ERR).await {
            tracing::error!(error = %qe, job_id = %job.id, "failed to mark unscoped job as failed");
        }
        return;
    };

    // 0084's SECURITY DEFINER resolver — the ONE sanctioned pre-scope lookup,
    // correct on either plane's pool. Its result is used TWICE below: to
    // refuse a cross-account payload, and (further down) as the egress
    // ceiling's owner lookup, replacing the pre-conversion
    // `store.graph_account_id` call. See the module doc's RLS section for why
    // this worker doesn't use the shared `workers::verify_job_scope` helper.
    let owner: Result<Option<Uuid>, String> =
        sqlx::query_scalar::<_, Option<Uuid>>("SELECT resolve_graph_account_id($1)")
            .bind(graph_id)
            .fetch_one(pool)
            .await
            .map_err(|e| e.to_string());
    if let Ok(Some(o)) = owner {
        if o != account_id {
            let msg = format!(
                "graph {graph_id} belongs to account {o} but the job is stamped for account {account_id} — refusing cross-account processing"
            );
            tracing::error!(job_id = %job.id, %msg, "summary: cross-account payload refused");
            if let Err(qe) = queue::fail(pool, job.id, &msg).await {
                tracing::error!(error = %qe, job_id = %job.id, "failed to mark cross-account job as failed");
            }
            return;
        }
    }
    let scope = JobScope::for_job(tenant, pool, account_id);

    // Switched off between claim and processing: spend nothing, write
    // nothing, hand the meeting back for a later pass.
    if !summaries_claim_allowed() {
        tracing::info!(job_id = %job.id, %graph_id, "summary: managed enrichment switched off; meeting held");
        requeue(pool, &job, Stop::Held("switched_off")).await;
        return;
    }
    // Settle any receipt an earlier crashed attempt of this job left behind
    // before opening a new call (managed::enrichment step 1).
    if let Err(deny) = enrichment::recover_job(&scope, control_pool, tenant, account_id, job.id).await {
        tracing::warn!(job_id = %job.id, error = %deny, "summary: managed receipt recovery unavailable; meeting held");
        requeue(pool, &job, Stop::Held(crate::jobs::queue::MANAGED_UNAVAILABLE)).await;
        return;
    }

    // Capture the source identity and transcript under the same short row
    // lock. No connection or lock is held across R2/model calls. Every later
    // status/result write must still match this snapshot.
    let (transcript, snapshot) =
        match scoped_get_meeting_transcript(&scope, store, graph_id, &source_id).await {
            Ok(Some(value)) => value,
            Ok(None) => {
                let _ = queue::complete(pool, job.id, queue::JobOutcome::NothingToDo).await;
                return;
            }
            Err(_) => {
                // No source identity was read: never mark a possibly newer row.
                let _ = queue::fail(pool, job.id, "transcript load failed").await;
                return;
            }
        };

    // No raw shortcut (COR-35): summarise only from the transcript's source
    // version and its present, unretracted chunk atoms, and bind them all.
    // With none left it is refused without a model call; not a hold, since
    // the evidence will not come back for this version (a new capture brings
    // a new version and its own pass).
    let Some(evidence) = snapshot.evidence.clone() else {
        match scoped_set_summary_failed(&scope, store, graph_id, &source_id, "evidence unavailable", &snapshot).await {
            Ok(_) => {
                tracing::warn!(job_id = %job.id, %graph_id, "summary: no source version or no usable chunk evidence; refused without a model call");
                let _ = queue::complete(pool, job.id, queue::JobOutcome::NothingToDo).await;
            }
            Err(e) => {
                tracing::warn!(error = %e, %graph_id, "summary: could not record the evidence refusal; retrying");
                let _ = queue::fail(pool, job.id, "status write error").await;
            }
        }
        return;
    };

    // Mark running (best-effort — a lost status write just delays the client's
    // "running" view; the row still ends up done/failed/blocked).
    match scoped_set_summary_status(&scope, store, graph_id, &source_id, "running", &snapshot).await
    {
        Ok(false) => {
            let _ = queue::complete(pool, job.id, queue::JobOutcome::NothingToDo).await;
            return;
        }
        Ok(true) => {}
        Err(e) => {
            tracing::warn!(error = %e, graph_id = %graph_id, "summary: could not set running status")
        }
    }

    // ── Egress ceiling ────────────────────────────────────────────────────────
    let egress_forbidden = if allow_override {
        false
    } else {
        match owner {
            Ok(Some(_)) => false,
            // Ownership unresolved ⇒ fail closed: block rather than send the
            // transcript for an inconsistent/stale job.
            Ok(None) => true,
            Err(e) => {
                // Fail closed on a read fault.
                // (Pre-W.2 this was the `store.graph_account_id` error branch;
                // the outcome — block, then complete — is unchanged. Reaching
                // the blocked path with an unverified owner is safe: it writes
                // only this job's own account-scoped status row and never
                // sends transcript content to a model.)
                tracing::error!(error = %e, graph_id = %graph_id, "summary: account lookup failed — failing closed");
                true
            }
        }
    };
    if summary_blocked(allow_override, egress_forbidden) {
        // Only take the job off the queue once the terminal `blocked` state is
        // durably persisted. If that write fails, requeue instead of completing —
        // otherwise the meeting is left stuck `running` with no path to recovery.
        match scoped_set_summary_status(&scope, store, graph_id, &source_id, "blocked", &snapshot)
            .await
        {
            Ok(_) => {
                // A policy decision, not an inability to process — this job
                // legitimately has nothing to do until the account opts in.
                let _ = queue::complete(pool, job.id, queue::JobOutcome::NothingToDo).await;
                tracing::info!(graph_id = %graph_id, "meeting summary blocked: owning account unresolved (awaiting opt-in)");
            }
            Err(e) => {
                tracing::error!(error = %e, graph_id = %graph_id, "summary: could not persist blocked status; requeueing");
                retry_or_fail(
                    &scope,
                    pool,
                    store,
                    &job,
                    graph_id,
                    &source_id,
                    "status write error",
                    &snapshot,
                )
                .await;
            }
        }
        return;
    }

    // ── Assemble the transcript text + timing + speaker set ───────────────────
    // From the evidence only (COR-35, no raw shortcut): the text of the
    // version's present, unretracted chunk atoms, in chunk order. Stored
    // segments keep their timing, but only a segment whose text is still in
    // that evidence reaches the model; a deleted or retracted chunk's lines
    // never do.
    let atom_ids: Vec<Uuid> = evidence.iter().filter(|i| i.kind == INPUT_ATOM).map(|i| i.id).collect();
    let mut content = match crate::workers::digest::load_selected_coral_content(&scope, tenant, pool, r2, account_id, &atom_ids).await {
        Ok(content) => content,
        Err(e) => {
            tracing::warn!(error = %e, %graph_id, "summary: chunk evidence could not be read");
            retry_or_fail(&scope, pool, store, &job, graph_id, &source_id, "transcript text unavailable", &snapshot).await;
            return;
        }
    };
    let evidence_text: String = atom_ids
        .iter()
        .filter_map(|id| content.remove(id))
        .collect::<Vec<_>>()
        .join("\n");
    let segments: Vec<Segment> = parse_segments(transcript.segments.as_ref())
        .into_iter()
        .filter(|seg| evidence_text.contains(seg.text.trim()))
        .collect();
    let (lines, seg_starts, speakers): (Vec<String>, Vec<i64>, Vec<String>) =
        if !segments.is_empty() {
            build_segment_lines(&segments)
        } else if !evidence_text.trim().is_empty() {
            // No per-segment timing that the evidence still carries: the
            // chunk text itself (ts_ms null).
            let lines = evidence_text.lines().map(str::to_string).collect::<Vec<_>>();
            let speakers = speakers_from_plain_lines(&lines);
            (lines, Vec::new(), speakers)
        } else {
            mark_failed(
                &scope,
                pool,
                store,
                &job,
                graph_id,
                &source_id,
                "transcript text unavailable",
                &snapshot,
            )
            .await;
            return;
        };

    let mut chunks = chunk_lines(&lines, CHUNK_CHAR_BUDGET);
    if chunks.len() > MAX_CHUNKS {
        tracing::warn!(
            graph_id = %graph_id,
            chunks = chunks.len(),
            "summary: transcript exceeds MAX_CHUNKS; summarizing the first {MAX_CHUNKS}"
        );
        chunks.truncate(MAX_CHUNKS);
    }

    // Every call opens a managed logical call; a pass only starts with enough
    // of the graph's daily bound left to finish, so it never stops half-way
    // on the bound.
    let needed = chunks.len() as i64 + 1;
    match calls_left(&scope, account_id, graph_id).await {
        Ok(left) if left >= needed => {}
        Ok(left) => {
            tracing::info!(job_id = %job.id, %graph_id, calls_left = left, needed,
                "summary: graph's daily call bound reached; meeting rescheduled for the next UTC day");
            hold(&scope, pool, store, &job, graph_id, &source_id, &snapshot, Stop::NextDay).await;
            return;
        }
        Err(e) => {
            tracing::warn!(error = %e, job_id = %job.id, "summary: daily call bound unreadable; meeting rescheduled");
            hold(&scope, pool, store, &job, graph_id, &source_id, &snapshot, Stop::Held(crate::jobs::queue::MANAGED_UNAVAILABLE)).await;
            return;
        }
    }
    let mut managed = Managed {
        control: control_pool,
        tenant,
        account_id,
        graph_id,
        job: EnrichmentJob { id: job.id, kind: job.kind.clone(), attempts: job.attempts },
        principal: None,
        transcript: transcript_input(&snapshot),
        evidence,
    };

    // Billing Phase 2a: resolved ONCE for this job, shared by every MAP call
    // and the REDUCE call below — see `resolve_meeting_attribution`'s doc.
    // The managed receipts charge the graph's executor; this stays the
    // usage row's attribution.
    let attributed_user_id = resolve_meeting_attribution(&scope, graph_id, &source_id).await;
    let usage = usage_context(&scope, graph_id, job.id, attributed_user_id);

    // ── Map: per-chunk extraction (Haiku) ─────────────────────────────────────
    let map_model = model_config::summary_map_model();
    let map_max = model_config::summary_map_max_tokens();
    let mut partials: Vec<Extraction> = Vec::new();
    for chunk in &chunks {
        match run_pass(
            llm,
            MAP_SYSTEM_PROMPT,
            chunk,
            &map_model,
            map_max,
            map_tool_schema(),
            &scope,
            &mut managed,
            &usage,
            "summary_map",
            INPUT_MAP_PROMPT,
        )
        .await
        {
            Ok((v, _)) => partials.push(parse_extraction(&v)),
            Err(GenError::Held(stop)) => {
                hold(&scope, pool, store, &job, graph_id, &source_id, &snapshot, stop).await;
                return;
            }
            Err(GenError::Transient(reason)) => {
                tracing::warn!(graph_id = %graph_id, reason = %reason, "summary map pass: transient error");
                retry_or_fail(
                    &scope,
                    pool,
                    store,
                    &job,
                    graph_id,
                    &source_id,
                    "model error",
                    &snapshot,
                )
                .await;
                return;
            }
            // A single unparsable chunk shouldn't sink the whole meeting — skip it.
            Err(GenError::InvalidOutput(reason)) => {
                tracing::warn!(graph_id = %graph_id, reason = %reason, "summary map pass: skipping chunk (malformed model output)");
            }
            // Auth / missing-model / bad-request faults recur for every chunk and
            // would otherwise silently yield empty partials → a meaningless summary
            // persisted as `done`. Fail the whole job so the error surfaces.
            Err(GenError::Config(reason)) => {
                tracing::error!(graph_id = %graph_id, reason = %reason, "summary map pass: model/config error — failing job");
                mark_failed(
                    &scope,
                    pool,
                    store,
                    &job,
                    graph_id,
                    &source_id,
                    "model configuration error",
                    &snapshot,
                )
                .await;
                return;
            }
        }
    }

    // ── Reduce: merge partials into the final structure (Sonnet) ──────────────
    let reduce_model = model_config::summary_reduce_model();
    let reduce_max = model_config::summary_reduce_max_tokens();
    let reduce_input = build_reduce_input(&speakers, &partials);
    let (reduced, reduce_work) = match run_pass(
        llm,
        REDUCE_SYSTEM_PROMPT,
        &reduce_input,
        &reduce_model,
        reduce_max,
        reduce_tool_schema(),
        &scope,
        &mut managed,
        &usage,
        "summary_reduce",
        INPUT_REDUCE_PROMPT,
    )
    .await
    {
        Ok(v) => v,
        Err(GenError::Held(stop)) => {
            hold(&scope, pool, store, &job, graph_id, &source_id, &snapshot, stop).await;
            return;
        }
        Err(GenError::Transient(reason)) => {
            tracing::warn!(graph_id = %graph_id, reason = %reason, "summary reduce pass: transient error");
            retry_or_fail(
                &scope,
                pool,
                store,
                &job,
                graph_id,
                &source_id,
                "model error",
                &snapshot,
            )
            .await;
            return;
        }
        Err(GenError::InvalidOutput(reason)) => {
            tracing::warn!(graph_id = %graph_id, reason = %reason, "summary reduce pass: invalid response");
            mark_failed(
                &scope,
                pool,
                store,
                &job,
                graph_id,
                &source_id,
                "invalid model response",
                &snapshot,
            )
            .await;
            return;
        }
        Err(GenError::Config(reason)) => {
            tracing::error!(graph_id = %graph_id, reason = %reason, "summary reduce pass: model/config error");
            mark_failed(
                &scope,
                pool,
                store,
                &job,
                graph_id,
                &source_id,
                "model configuration error",
                &snapshot,
            )
            .await;
            return;
        }
    };

    // ── Shape + persist ───────────────────────────────────────────────────────
    let speaker_set: HashSet<String> = speakers.into_iter().collect();
    let overview = reduced
        .get("overview")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let reduced_ex = parse_extraction(&reduced);
    let summary = sanitize_summary(overview, &reduced_ex, &speaker_set, &seg_starts);

    let dcount = summary["decisions"].as_array().map_or(0, Vec::len);
    let acount = summary["action_items"].as_array().map_or(0, Vec::len);

    match scoped_set_summary_done(
        &scope,
        store,
        graph_id,
        &source_id,
        &summary,
        &reduce_model,
        &snapshot,
        &reduce_work,
    )
    .await
    {
        Ok(Publish::Stale) => {
            tracing::info!(graph_id = %graph_id, job_id = %job.id,
                "summary: chunk evidence changed during the call; published nothing (discarded_stale)");
            hold(&scope, pool, store, &job, graph_id, &source_id, &snapshot, Stop::Held("inputs_changed")).await;
        }
        Ok(Publish::Unavailable) => {
            tracing::warn!(graph_id = %graph_id, job_id = %job.id,
                "summary: managed publish re-verify unavailable; meeting rescheduled");
            hold(&scope, pool, store, &job, graph_id, &source_id, &snapshot, Stop::Held(crate::jobs::queue::MANAGED_UNAVAILABLE)).await;
        }
        Ok(published) => {
            let wrote = published == Publish::Written;
            let _ = queue::complete(
                pool,
                job.id,
                if wrote {
                    queue::JobOutcome::DidWork
                } else {
                    queue::JobOutcome::NothingToDo
                },
            )
            .await;
            tracing::info!(
                graph_id = %graph_id,
                model = %reduce_model,
                decisions = dcount,
                action_items = acount,
                wrote,
                "meeting summary generated (managed_enrichment)"
            );
        }
        Err(e) => {
            tracing::error!(error = %e, graph_id = %graph_id, "summary: store write failed");
            retry_or_fail(
                &scope,
                pool,
                store,
                &job,
                graph_id,
                &source_id,
                "storage error",
                &snapshot,
            )
            .await;
        }
    }
}

/// Terminal failure: record `failed` + a short (content-free) category and take
/// the job off the queue so it isn't retried automatically. Recovery is the
/// client's explicit Retry (`POST /v1/meetings/summary/generate`).
///
/// Reached two ways: directly, for a reason that will recur on every retry
/// (transcript not found/unavailable, model misconfigured, an unparseable
/// final model response), and via [`retry_or_fail`] once its OWN bounded
/// attempts are exhausted on a reason that stayed transient too long. Either
/// way, by the time this runs the summary permanently will not be produced —
/// Track B: this is `FailureClass::Structural` `could_not_do`, not a
/// completion. `summary_status` keeps its own richer `failed` + `category`
/// (unchanged, still the client-facing signal `POST …/generate` reads to
/// offer Retry); `jobs.status` now agrees with it instead of misleadingly
/// reporting `done`.
#[allow(clippy::too_many_arguments)]
async fn mark_failed(
    scope: &JobScope<'_>,
    pool: &PgPool,
    store: &dyn crate::store::Store,
    job: &JobRow,
    graph_id: Uuid,
    source_id: &str,
    category: &str,
    snapshot: &SummarySnapshot,
) {
    // Dead-letter (never retried further) only after the terminal `failed`
    // state is stored. If the status write fails, requeue the job so a later
    // attempt (or, once attempts are exhausted, the queue's dead-letter) can
    // converge it — dead-lettering here before the write lands would strand
    // the meeting at `running` indefinitely.
    match scoped_set_summary_failed(scope, store, graph_id, source_id, category, snapshot).await {
        Ok(false) => {
            let _ = queue::complete(pool, job.id, queue::JobOutcome::NothingToDo).await;
        }
        Ok(true) => {
            let _ =
                queue::could_not_do(pool, job.id, queue::FailureClass::Structural, category).await;
            tracing::info!(graph_id = %graph_id, category = %category, "meeting summary failed");
        }
        Err(e) => {
            tracing::warn!(error = %e, graph_id = %graph_id, "summary: could not persist failed status; requeueing");
            let _ = queue::fail(pool, job.id, "status write error").await;
        }
    }
}

/// Transient failure: re-enqueue with back-off while attempts remain; otherwise
/// converge to terminal `failed` so `summary_status` doesn't sit `running`.
#[allow(clippy::too_many_arguments)]
async fn retry_or_fail(
    scope: &JobScope<'_>,
    pool: &PgPool,
    store: &dyn crate::store::Store,
    job: &JobRow,
    graph_id: Uuid,
    source_id: &str,
    category: &str,
    snapshot: &SummarySnapshot,
) {
    if job.attempts + 1 >= SUMMARY_MAX_ATTEMPTS {
        mark_failed(
            scope, pool, store, job, graph_id, source_id, category, snapshot,
        )
        .await;
    } else if let Err(e) = queue::fail(pool, job.id, category).await {
        tracing::error!(error = %e, job_id = %job.id, "summary: could not requeue job");
    }
}

// ── Managed enrichment: holds, the daily bound, inputs ────────────────────────

/// Why a pass stopped before publishing. Never a failure: the meeting is
/// handed back under the shared retry rule (`jobs::queue::hold`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Stop {
    /// The graph's daily call bound or the shared daily cap.
    NextDay,
    /// A managed hold, refusal, or managed storage unavailable.
    Held(&'static str),
}

impl Stop {
    fn reason(self) -> &'static str {
        match self {
            Stop::NextDay => "daily_bound_or_cap",
            Stop::Held(code) => code,
        }
    }
}

/// The job's managed context: one principal for every call of the pass, and
/// the transcript snapshot every call binds.
struct Managed<'a> {
    control: &'a PgPool,
    tenant: &'a TenantId,
    account_id: Uuid,
    graph_id: Uuid,
    job: EnrichmentJob,
    principal: Option<Uuid>,
    transcript: EnrichmentInput,
    evidence: Vec<EnrichmentInput>,
}

impl Managed<'_> {
    fn work(&self, prompt_kind: &str, prompt: &str) -> Result<EnrichmentWork, GenError> {
        let mut inputs = Vec::with_capacity(self.evidence.len() + 2);
        inputs.push(self.transcript.clone());
        inputs.extend(self.evidence.iter().cloned());
        inputs.push(EnrichmentInput::new(prompt_kind, self.transcript.id, 0, prompt.as_bytes()));
        EnrichmentWork::new(self.account_id, self.graph_id, Some(self.job.clone()), None, inputs)
            .map_err(|e| GenError::Config(e.to_string()))
    }

    async fn principal(&mut self, scope: &JobScope<'_>, work: &EnrichmentWork) -> Result<Uuid, GenError> {
        if let Some(principal) = self.principal {
            return Ok(principal);
        }
        match enrichment::select_principal(scope, self.control, self.tenant, work).await {
            Ok(principal) => {
                self.principal = Some(principal);
                Ok(principal)
            }
            Err(coralswarm_core::send_gate::Deny::Hold(code)) => Err(GenError::Held(Stop::Held(code.code()))),
            // Storage, not a missing principal: a hold that can clear.
            Err(_) => Err(GenError::Held(Stop::Held(crate::jobs::queue::MANAGED_UNAVAILABLE))),
        }
    }
}

/// The transcript identity a call read: the row and the exact snapshot the
/// publish lock re-checks.
fn transcript_input(snapshot: &SummarySnapshot) -> EnrichmentInput {
    let identity = json!({
        "version_id": snapshot.version_id,
        "raw_blob_key": snapshot.raw_blob_key,
        "raw_inline": snapshot.raw_inline,
        "segments": snapshot.segments,
    });
    EnrichmentInput::new(INPUT_TRANSCRIPT, snapshot.transcript_id, 0, identity.to_string().as_bytes())
}

/// The version's evidence inputs, in a stable order: the version, then its
/// chunk atoms by id. `None` unless every chunk is present and unretracted.
async fn load_evidence(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    graph_id: Uuid,
    version_id: Uuid,
) -> Result<Option<Vec<EnrichmentInput>>, StoreError> {
    let version: Option<Uuid> = sqlx::query_scalar(
        "SELECT version_id FROM source_versions WHERE graph_id=$1 AND version_id=$2 FOR SHARE",
    )
    .bind(graph_id)
    .bind(version_id)
    .fetch_optional(&mut **tx)
    .await?;
    if version.is_none() {
        return Ok(None);
    }
    type Row = (Uuid, i64, Option<String>, Option<String>, Option<String>);
    let rows: Vec<Row> = sqlx::query_as(
        "SELECT a.atom_id,a.content_revision,a.content_hash,a.content_blob_key,a.content_inline \
           FROM source_version_chunks c JOIN atoms a ON a.atom_id=c.atom_id AND a.graph_id=c.graph_id \
          WHERE c.graph_id=$1 AND c.source_version_id=$2 AND a.evidence_retracted IS NOT TRUE \
          ORDER BY substring(a.source_id from ':chunk:([0-9]+)$')::int NULLS LAST, a.atom_id FOR SHARE OF a",
    )
    .bind(graph_id)
    .bind(version_id)
    .fetch_all(&mut **tx)
    .await?;
    // Leave room for the transcript, the version and the prompt marker.
    if rows.is_empty() || rows.len() + 3 > enrichment::MAX_INPUTS {
        return Ok(None);
    }
    let mut inputs = vec![EnrichmentInput::new(INPUT_VERSION, version_id, 0, version_id.as_bytes())];
    for (atom_id, revision, hash, blob, inline) in rows {
        let identity = json!({ "content_hash": hash, "content_blob_key": blob, "content_inline": inline });
        inputs.push(EnrichmentInput::new(INPUT_ATOM, atom_id, revision.max(0), identity.to_string().as_bytes()));
    }
    Ok(Some(inputs))
}

/// Summary calls the graph may still open today.
async fn calls_left(scope: &JobScope<'_>, account_id: Uuid, graph_id: Uuid) -> Result<i64, String> {
    let bound = enrichment::daily_bound(SUMMARY_DAILY_CALLS_ENV, DEFAULT_SUMMARY_DAILY_CALLS_PER_GRAPH);
    let mut tx = summary_tx(scope).await.map_err(|e| e.to_string())?;
    let used = enrichment::calls_today(&mut tx, account_id, Some(graph_id), INPUT_TRANSCRIPT)
        .await
        .map_err(|e| e.to_string())?;
    tx.rollback().await.ok();
    Ok((bound - used).max(0))
}

/// Hand the held meeting's job back to the queue in place: a daily bound
/// waits for the next UTC day, any other hold follows the shared retry rule
/// (`jobs::queue::hold`): a clearable hold backs off to a cap, one that
/// cannot clear dead-letters after a few tries. The row keeps its
/// `summary:{graph}:{source}` key, so a meeting never has two pending
/// passes, and a hold never charges an attempt.
async fn requeue(pool: &PgPool, job: &JobRow, stop: Stop) {
    let held = match stop {
        Stop::NextDay => {
            queue::hold_until(pool, job.id, "daily_bound", enrichment::secs_until_next_utc_day(chrono::Utc::now())).await
        }
        Stop::Held(code) => queue::hold(pool, job.id, code).await,
    };
    if let Err(e) = held {
        tracing::error!(error = %e, job_id = %job.id, "summary: could not hold the job; the lease reaper retries it");
    }
}

/// A managed hold: the meeting goes back to `pending` (unless a newer capture
/// already replaced the snapshot, which queued its own pass) and the job is
/// held ([`requeue`]). Never a failure.
#[allow(clippy::too_many_arguments)]
async fn hold(
    scope: &JobScope<'_>,
    pool: &PgPool,
    store: &dyn crate::store::Store,
    job: &JobRow,
    graph_id: Uuid,
    source_id: &str,
    snapshot: &SummarySnapshot,
    stop: Stop,
) {
    tracing::info!(job_id = %job.id, %graph_id, reason = stop.reason(),
        "summary: held by managed admission; meeting back on the queue under the shared retry rule");
    match scoped_set_summary_status(scope, store, graph_id, source_id, "pending", snapshot).await {
        Ok(false) => {
            let _ = queue::complete(pool, job.id, queue::JobOutcome::NothingToDo).await;
        }
        Ok(true) => requeue(pool, job, stop).await,
        Err(e) => {
            tracing::warn!(error = %e, %graph_id, "summary: could not reset held meeting to pending; holding the job anyway");
            requeue(pool, job, stop).await;
        }
    }
}

// ── Scope-aware Store wrappers (org-tenancy Phase W.2) ────────────────────────
//
// Each wrapper runs ONE of this worker's RLS-covered `Store` calls under the
// job's [`JobScope`]: consumer plane ⇒ the `_scoped` variant inside a fresh
// `begin_account_scoped` transaction; org tenant ⇒ the same `_scoped`
// variant inside a fresh `begin_tenant_system_scoped_without_account`
// transaction. NEITHER arm is ambient — a new wrapper that runs the plain
// method on the org arm re-opens the gap this module's doc describes. Per
// statement rather than per job — see [`JobScope`]'s doc.

/// The exact input a summary observed. Version identity catches A→B→A;
/// raw fields/segments additionally protect legacy writers without versions.
struct SummarySnapshot {
    transcript_id: Uuid,
    /// The source version and every chunk atom of it, read in the same
    /// transaction as the snapshot. `None` when no version or no present,
    /// unretracted chunk is left.
    evidence: Option<Vec<EnrichmentInput>>,
    version_id: Option<Uuid>,
    raw_blob_key: Option<String>,
    raw_inline: Option<String>,
    segments: Option<Value>,
}

async fn summary_tx<'a>(
    scope: &JobScope<'a>,
) -> Result<sqlx::Transaction<'a, sqlx::Postgres>, StoreError> {
    match scope {
        JobScope::Consumer { pool, account_id } => begin_account_scoped(pool, *account_id).await,
        JobScope::Org(pool) => begin_tenant_system_scoped_without_account(pool).await,
    }
}

async fn lock_summary_snapshot(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    graph_id: Uuid,
    source_id: &str,
    snapshot: &SummarySnapshot,
) -> Result<bool, StoreError> {
    // The predicate and row lock are one statement. A concurrent capture that
    // held this row first is rechecked after waiting; a capture that follows
    // this write clears the summary in its own transaction.
    Ok(sqlx::query_scalar::<_, i32>(
        "SELECT 1 FROM meeting_transcripts WHERE graph_id=$1 AND source_id=$2 \
         AND current_version_id IS NOT DISTINCT FROM $3 \
         AND raw_blob_key IS NOT DISTINCT FROM $4 \
         AND raw_inline IS NOT DISTINCT FROM $5 \
         AND segments IS NOT DISTINCT FROM $6 AND id=$7 FOR UPDATE",
    )
    .bind(graph_id)
    .bind(source_id)
    .bind(snapshot.version_id)
    .bind(&snapshot.raw_blob_key)
    .bind(&snapshot.raw_inline)
    .bind(&snapshot.segments)
    .bind(snapshot.transcript_id)
    .fetch_optional(&mut **tx)
    .await?
    .is_some())
}

async fn scoped_set_summary_status(
    scope: &JobScope<'_>,
    store: &dyn Store,
    graph_id: Uuid,
    source_id: &str,
    status: &str,
    snapshot: &SummarySnapshot,
) -> Result<bool, StoreError> {
    let mut tx = summary_tx(scope).await?;
    if !lock_summary_snapshot(&mut tx, graph_id, source_id, snapshot).await? {
        return Ok(false);
    }
    store
        .set_meeting_summary_status_scoped(&mut tx, graph_id, source_id, status)
        .await?;
    tx.commit().await?;
    Ok(true)
}

async fn scoped_set_summary_failed(
    scope: &JobScope<'_>,
    store: &dyn Store,
    graph_id: Uuid,
    source_id: &str,
    category: &str,
    snapshot: &SummarySnapshot,
) -> Result<bool, StoreError> {
    let mut tx = summary_tx(scope).await?;
    if !lock_summary_snapshot(&mut tx, graph_id, source_id, snapshot).await? {
        return Ok(false);
    }
    store
        .set_meeting_summary_failed_scoped(&mut tx, graph_id, source_id, category)
        .await?;
    tx.commit().await?;
    Ok(true)
}

/// What the publish transaction did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Publish {
    /// The summary and its receipt's `written` mark committed together.
    Written,
    /// A newer capture replaced the transcript (it queued its own pass), or
    /// an earlier publish of this call already wrote it. Nothing written.
    Skipped,
    /// The same transcript, but a chunk atom changed, was deleted or was
    /// retracted during the call (inputs `discarded_stale`). Nothing written;
    /// the meeting is held and summarised again from the current evidence.
    Stale,
    /// Managed storage could not re-verify the call. Nothing written; held.
    Unavailable,
}

/// Publish (managed::enrichment step 5): re-lock the transcript on the
/// snapshot the calls read, re-verify the reduce call's receipt inputs, and
/// write the summary and the `written` mark in one transaction.
#[allow(clippy::too_many_arguments)]
async fn scoped_set_summary_done(
    scope: &JobScope<'_>,
    store: &dyn Store,
    graph_id: Uuid,
    source_id: &str,
    summary: &Value,
    model: &str,
    snapshot: &SummarySnapshot,
    work: &EnrichmentWork,
) -> Result<Publish, StoreError> {
    let mut tx = summary_tx(scope).await?;
    if !lock_summary_snapshot(&mut tx, graph_id, source_id, snapshot).await? {
        // A newer capture replaced the transcript: mark this call's inputs
        // stale (best effort) and write nothing.
        let _ = enrichment::verify_inputs_locked(&mut tx, work, &[]).await;
        tx.commit().await.ok();
        return Ok(Publish::Skipped);
    }
    // The lock matched the exact snapshot, so the transcript input is
    // unchanged. Re-read the version and its chunk atoms under share locks; a
    // deleted, retracted or edited chunk drops out or changes, so the
    // re-verify reports Stale. The prompt input is this call's own.
    let mut current = vec![transcript_input(snapshot)];
    if let Some(version) = snapshot.version_id {
        if let Some(evidence) = load_evidence(&mut tx, graph_id, version).await? {
            current.extend(evidence);
        }
    }
    current.extend(
        work.inputs()
            .iter()
            .filter(|i| i.kind == INPUT_MAP_PROMPT || i.kind == INPUT_REDUCE_PROMPT)
            .cloned(),
    );
    let verified = match enrichment::verify_inputs_locked(&mut tx, work, &current).await {
        Ok(InputCheck::Fresh(verified)) => verified,
        Ok(InputCheck::Stale) => {
            tx.commit().await?;
            return Ok(Publish::Stale);
        }
        Ok(InputCheck::AlreadyWritten) => {
            tx.rollback().await.ok();
            return Ok(Publish::Skipped);
        }
        Err(_) => {
            tx.rollback().await.ok();
            return Ok(Publish::Unavailable);
        }
    };
    store
        .set_meeting_summary_done_scoped(&mut tx, graph_id, source_id, summary, model)
        .await?;
    if verified.mark_written(&mut tx).await.is_err() {
        tx.rollback().await.ok();
        return Ok(Publish::Unavailable);
    }
    tx.commit().await?;
    Ok(Publish::Written)
}

async fn scoped_get_meeting_transcript(
    scope: &JobScope<'_>,
    store: &dyn Store,
    graph_id: Uuid,
    source_id: &str,
) -> Result<Option<(MeetingTranscript, SummarySnapshot)>, StoreError> {
    let mut tx = summary_tx(scope).await?;
    let row = sqlx::query_as::<_, (Uuid, Option<Uuid>)>(
        "SELECT id, current_version_id FROM meeting_transcripts WHERE graph_id=$1 AND source_id=$2 FOR SHARE",
    ).bind(graph_id).bind(source_id).fetch_optional(&mut *tx).await?;
    let Some((transcript_id, version_id)) = row else {
        return Ok(None);
    };
    let transcript = store
        .get_meeting_transcript_scoped(&mut tx, graph_id, source_id)
        .await?;
    let evidence = match version_id {
        Some(version) => load_evidence(&mut tx, graph_id, version).await?,
        None => None,
    };
    tx.commit().await?;
    Ok(transcript.map(|transcript| {
        let snapshot = SummarySnapshot {
            transcript_id,
            evidence,
            version_id,
            raw_blob_key: transcript.raw_blob_key.clone(),
            raw_inline: transcript.raw_inline.clone(),
            segments: transcript.segments.clone(),
        };
        (transcript, snapshot)
    }))
}

/// Billing Phase 2a: resolve a SINGLE attributing member for this meeting's
/// `summary_map`/`summary_reduce` calls.
///
/// `meeting_transcripts` itself has no `ingested_by` column — it is an
/// `atoms` SIBLING table (migration `0056`'s own header), not a row derived
/// from one. The real per-member signal lives on this meeting's CHUNK
/// atoms, which carry `ingested_by` and whose `source_id` follows the
/// `"<source_id>:chunk:<N>"` convention `store::mod.rs`'s `list_meeting_
/// merges`/`list_meeting_new_entities` already join on via a `LIKE`
/// prefix match — this function reuses that exact join shape (same
/// metacharacter-escaping discipline as `store::like_prefix`, duplicated
/// here rather than exposed `pub(crate)` from `store` purely to avoid
/// widening that module's already-large public surface for one caller).
///
/// Applies `workers::billing_derivation::resolve_debit_attribution`'s
/// disagreement rule verbatim — not a re-implementation, the SAME function
/// — so this meeting's chunk atoms resolve to: the one member every chunk
/// agrees on, or `None` if zero chunks are attributed OR two-or-more
/// disagree (a meeting whose chunks were ingested under different
/// connector identities, e.g. a mid-meeting identity remap, must not have
/// its summary spend fabricated onto an arbitrary one of them).
///
/// A read failure (scope open, query) degrades to `None` rather than
/// failing the job — this is a billing ENRICHMENT, and this worker's own
/// design (egress/status writes already tolerate best-effort failures
/// throughout `process_job`) never lets an accounting lookup block or fail
/// the actual summarization work.
/// Build the `LIKE` pattern matching a meeting's CHUNK atoms only —
/// `"{source_id}:chunk:{n}"` (`routes::ingest`). Escapes LIKE metacharacters
/// (`\`, `%`, `_`) the same way `store::like_prefix` does, then appends the
/// literal `:chunk:` delimiter BEFORE the wildcard.
///
/// The delimiter is the whole point: a bare `"<escaped>%"` also matches an
/// unrelated sibling whose `source_id` merely starts with this one
/// (`"meeting-1%"` matches `"meeting-10:chunk:0"`), which would fold a
/// stranger's `ingested_by` into the attribution set and either unattribute
/// the summary or bill the wrong member. Extracted as its own function so the
/// regression test exercises the SAME code the query uses, rather than
/// re-typing the pattern and proving only itself.
fn chunk_like_pattern(source_id: &str) -> String {
    let mut out = String::with_capacity(source_id.len() + 8);
    for c in source_id.chars() {
        if c == '\\' || c == '%' || c == '_' {
            out.push('\\');
        }
        out.push(c);
    }
    out.push_str(":chunk:%");
    out
}

async fn resolve_meeting_attribution(scope: &JobScope<'_>, graph_id: Uuid, source_id: &str) -> Option<Uuid> {
    // Match the meeting's OWN atom exactly, plus its chunk atoms
    // (`"{source_id}:chunk:{n}"` — `routes::ingest:1286`/`:1852`).
    //
    // A bare `store::like_prefix`-style `"<escaped>%"` is NOT safe here. It
    // also matches an unrelated sibling whose `source_id` merely starts with
    // this one: `"meeting-1%"` matches `"meeting-10:chunk:0"`. That would fold
    // a stranger's `ingested_by` into `seen` and then either unattribute this
    // summary (two members "disagreeing") or bill the wrong one outright.
    //
    // `like_prefix`'s own doc (`store/mod.rs`) argues the bare form is fine
    // because "the meeting/atom ingest path guarantees" no such prefix
    // collision. `source_id` is caller-supplied, and this lookup decides who
    // gets BILLED, so it does not lean on that guarantee — the equality arm
    // keeps the base atom, and the `:chunk:` delimiter excludes siblings.
    // Same LIKE-metacharacter escaping as `like_prefix` (`\`, `%`, `_`).
    let chunk_pattern = chunk_like_pattern(source_id);

    const QUERY: &str =
        "SELECT DISTINCT ingested_by FROM atoms \
         WHERE graph_id = $1 AND (source_id = $2 OR source_id LIKE $3) \
           AND ingested_by IS NOT NULL";

    let rows: Result<Vec<(Uuid,)>, sqlx::Error> = match scope {
        JobScope::Consumer { pool, account_id } => match begin_account_scoped(pool, *account_id).await {
            Ok(mut tx) => {
                let out = sqlx::query_as(QUERY)
                    .bind(graph_id)
                    .bind(source_id)
                    .bind(&chunk_pattern)
                    .fetch_all(&mut *tx)
                    .await;
                tx.rollback().await.ok();
                out
            }
            Err(e) => {
                tracing::warn!(%graph_id, error = %e, "summary: attribution lookup: failed to open scope — leaving unattributed");
                return None;
            }
        },
        JobScope::Org(pool) => match begin_tenant_system_scoped_without_account(pool).await {
            Ok(mut tx) => {
                let out = sqlx::query_as(QUERY)
                    .bind(graph_id)
                    .bind(source_id)
                    .bind(&chunk_pattern)
                    .fetch_all(&mut *tx)
                    .await;
                tx.rollback().await.ok();
                out
            }
            Err(e) => {
                tracing::warn!(%graph_id, error = %e, "summary: attribution lookup: failed to open tenant system scope — leaving unattributed");
                return None;
            }
        },
    };

    let seen: HashSet<Uuid> = match rows {
        Ok(rows) => rows.into_iter().map(|(id,)| id).collect(),
        Err(e) => {
            tracing::warn!(%graph_id, error = %e, "summary: attribution lookup failed — leaving unattributed");
            return None;
        }
    };
    let (resolved, conflicted) = crate::workers::billing_derivation::resolve_debit_attribution(&seen);
    if conflicted {
        tracing::warn!(
            %graph_id, %source_id, seen = seen.len(),
            "summary: this meeting's chunk atoms disagree on ingested_by — recording summary spend \
             unattributed rather than guessing"
        );
    }
    resolved
}

// ── LLM pass ──────────────────────────────────────────────────────────────────

enum GenError {
    /// Retrying the whole job may help (rate limit / timeout / 5xx).
    Transient(String),
    /// Malformed / unusable model OUTPUT for this one call — a 200 body we could
    /// not parse into the expected tool shape. A single such MAP chunk is safe to
    /// skip; the rest of the meeting still summarizes.
    InvalidOutput(String),
    /// A configuration / provider fault (auth, missing model, bad request) that
    /// will recur for EVERY call — never a per-chunk quirk. These must fail the
    /// whole job: silently skipping them would let an invalid MAP model produce
    /// empty partials and persist a meaningless summary as `done`.
    Config(String),
    /// A managed hold before the send: reschedule, never fail.
    Held(Stop),
}

/// A managed denial stops the pass (never a second paid call, never a
/// failure); anything else follows [`classify_llm_error`].
fn classify_managed_error(e: &LlmError) -> GenError {
    use coralswarm_core::send_gate::{Deny, HoldCode};
    match e {
        LlmError::Managed(Deny::Hold(HoldCode::DailyBudgetExhausted)) => GenError::Held(Stop::NextDay),
        LlmError::Managed(Deny::Hold(code)) => GenError::Held(Stop::Held(code.code())),
        LlmError::Managed(Deny::Refuse(code)) => GenError::Held(Stop::Held(code.code())),
        LlmError::Managed(Deny::Unavailable) => GenError::Held(Stop::Held(crate::jobs::queue::MANAGED_UNAVAILABLE)),
        e => classify_llm_error(e),
    }
}

/// Classify an [`LlmError`] into the worker's retry policy. Transient errors
/// re-queue the job; `LlmError::Parse` (a 200 body we can't parse) is malformed
/// model output; everything else non-transient (auth, model-not-found, 4xx) is a
/// configuration/provider fault that must fail the job rather than be skipped.
fn classify_llm_error(e: &LlmError) -> GenError {
    if e.is_transient() {
        GenError::Transient(llm_error_kind(e).to_string())
    } else if matches!(e, LlmError::Parse(_)) {
        GenError::InvalidOutput(llm_error_kind(e).to_string())
    } else {
        GenError::Config(llm_error_kind(e).to_string())
    }
}

/// The usage context every call of one job records under. `pool`/
/// `account_id` follow the tenant-loop-worker convention every other
/// `JobScope` caller uses: `Some(account)` on consumer (RLS makes attribution
/// mandatory), `None` on org (one org per tenant DB; `llm_usage.account_id`
/// is nullable for exactly that), still inside the plane's own scope since
/// `llm_usage` carries a `graph_id`-anchored member-ocean policy inside a
/// tenant DB. `source_job_id` is this worker's own `jobs.id`. Billing Phase
/// 2a: `attributed_user_id` is resolved ONCE per job by
/// `resolve_meeting_attribution` and shared by the MAP and REDUCE calls. The
/// managed receipts, not this row, are the bill.
fn usage_context(
    scope: &JobScope<'_>,
    graph_id: Uuid,
    job_id: Uuid,
    attributed_user_id: Option<Uuid>,
) -> crate::llm::usage::UsageContext {
    let usage_pool: &PgPool = match scope {
        JobScope::Consumer { pool, .. } => pool,
        JobScope::Org(pool) => pool,
    };
    crate::llm::usage::UsageContext::new(usage_pool.clone(), scope.usage_account_id(), Some(graph_id))
        .with_source_job(job_id)
        .with_attributed_user_opt(attributed_user_id)
}

/// One forced-tool LLM call as its own `managed_enrichment` logical call:
/// returns the tool call's input as a parsed JSON `Value` and the work the
/// call bound (publish re-verifies the reduce call's). Errors are classified
/// for retry policy; a managed denial is [`GenError::Held`].
///
/// `task` is the `llm_usage.task` label this call records under — distinct
/// per pass (`"summary_map"` for the per-chunk extraction, `"summary_reduce"`
/// for the merge) so cost can be broken out per stage even though both share
/// this one function. `prompt_kind` names the prompt input the call binds.
#[allow(clippy::too_many_arguments)]
async fn run_pass(
    llm: &dyn LlmProvider,
    system: &str,
    user: &str,
    model: &str,
    max_tokens: u32,
    tool: ToolSchema,
    scope: &JobScope<'_>,
    managed: &mut Managed<'_>,
    usage: &crate::llm::usage::UsageContext,
    task: &str,
    prompt_kind: &str,
) -> Result<(Value, EnrichmentWork), GenError> {
    let work = managed.work(prompt_kind, user)?;
    let principal = managed.principal(scope, &work).await?;
    let request = CompletionRequest {
        messages: vec![Message {
            role: Role::User,
            content: user.to_string(),
        }],
        model: model.to_string(),
        max_tokens,
        system: Some(system.to_string()),
        tool_schema: Some(tool),
        effort: None,
        reasoning: crate::llm::ReasoningSetting::Off,
    };
    // Billed either way, regardless of what the parse below decides. A 200
    // body we can't parse into JSON is malformed model output, not a config
    // fault — skippable for a single MAP chunk.
    let (_, parsed) = enrichment::complete(
        llm,
        scope,
        managed.control,
        managed.tenant,
        &work,
        principal,
        request,
        usage,
        task,
        |response| serde_json::from_str::<Value>(&response.content).ok(),
    )
    .await
    .map_err(|e| classify_managed_error(&e))?;
    match parsed {
        Some(value) => Ok((value, work)),
        None => Err(GenError::InvalidOutput("unparsable tool output".to_string())),
    }
}

/// A short, content-free category for an `LlmError` (never echoes model output).
fn llm_error_kind(e: &LlmError) -> &'static str {
    match e {
        LlmError::RateLimit(_) => "rate limited",
        LlmError::Timeout => "timeout",
        LlmError::Upstream { .. } => "upstream error",
        LlmError::Auth(_) => "auth error",
        LlmError::ModelNotFound(_) => "model not found",
        LlmError::Parse(_) => "invalid response",
        LlmError::Managed(_) => "managed send denied",
    }
}

// ── Tool schemas ──────────────────────────────────────────────────────────────

fn item_props(with_owner: bool) -> Value {
    let mut props = json!({
        "text": { "type": "string" },
        "ts_ms": { "type": ["integer", "null"] }
    });
    if with_owner {
        props["owner"] = json!({ "type": ["string", "null"] });
    }
    json!({
        "type": "array",
        "items": {
            "type": "object",
            "properties": props,
            "required": ["text"]
        }
    })
}

fn map_tool_schema() -> ToolSchema {
    ToolSchema {
        strict: false,
        name: "emit_meeting_extraction".to_string(),
        schema: json!({
            "type": "object",
            "properties": {
                "decisions": item_props(false),
                "action_items": item_props(true),
            },
            "required": ["decisions", "action_items"]
        }),
    }
}

fn reduce_tool_schema() -> ToolSchema {
    ToolSchema {
        strict: false,
        name: "emit_meeting_summary".to_string(),
        schema: json!({
            "type": "object",
            "properties": {
                "overview": { "type": "string" },
                "decisions": item_props(false),
                "action_items": item_props(true),
            },
            "required": ["overview", "decisions", "action_items"]
        }),
    }
}

// ── Payload / segment parsing ─────────────────────────────────────────────────

fn parse_payload(job: &JobRow) -> Result<(Uuid, String, bool), String> {
    let kind: JobKind = serde_json::from_value(json!({
        "kind": job.kind,
        "payload": job.payload,
    }))
    .map_err(|e| format!("payload decode: {e}"))?;
    match kind {
        JobKind::GenerateMeetingSummary {
            graph_id,
            source_id,
            allow_cloud_override,
        } => Ok((graph_id, source_id, allow_cloud_override)),
        other => Err(format!("unexpected job kind: {other:?}")),
    }
}

/// Parse stored `segments` JSONB into a segment list, dropping empties. A
/// malformed value yields an empty list (⇒ raw-text fallback).
fn parse_segments(raw: Option<&Value>) -> Vec<Segment> {
    let Some(v) = raw else { return Vec::new() };
    serde_json::from_value::<Vec<Segment>>(v.clone())
        .unwrap_or_default()
        .into_iter()
        .filter(|s| !s.text.trim().is_empty())
        .collect()
}

// ── Value → Extraction (robust to model number/shape quirks) ──────────────────

/// Accept an int, float, or numeric string as an `i64`; anything else ⇒ `None`.
fn as_i64_opt(v: &Value) -> Option<i64> {
    match v {
        Value::Number(n) => n
            .as_i64()
            .or_else(|| n.as_f64().map(|f| f.round() as i64)),
        Value::String(s) => s.trim().parse::<i64>().ok(),
        _ => None,
    }
}

fn parse_items(v: &Value, key: &str, with_owner: bool) -> Vec<Item> {
    let Some(arr) = v.get(key).and_then(Value::as_array) else {
        return Vec::new();
    };
    arr.iter()
        .filter_map(|it| {
            let text = it.get("text").and_then(Value::as_str)?.trim().to_string();
            if text.is_empty() {
                return None;
            }
            let ts_ms = it.get("ts_ms").and_then(as_i64_opt);
            let owner = if with_owner {
                it.get("owner")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(str::to_string)
            } else {
                None
            };
            Some(Item { text, owner, ts_ms })
        })
        .collect()
}

fn parse_extraction(v: &Value) -> Extraction {
    Extraction {
        decisions: parse_items(v, "decisions", false),
        action_items: parse_items(v, "action_items", true),
    }
}

// ── Pure helpers (unit-tested) ────────────────────────────────────────────────

/// Whether generation must be blocked: cloud egress is forbidden and the user
/// did NOT opt this meeting in.
fn summary_blocked(allow_cloud_override: bool, egress_forbidden: bool) -> bool {
    !allow_cloud_override && egress_forbidden
}

/// Whether a speaker label is a generic diarization placeholder rather than a
/// real person — `Speaker`, `Speaker 1`, `You`, `Others`, `Unknown`,
/// `Participant`, `Attendee 2`, and the like. These must never become a
/// validated action-item owner: the MAP prompt requires them to resolve to
/// `null`, so we exclude them from the speaker set and re-check during
/// sanitization (defense in depth against a model that echoes one back).
fn is_generic_speaker_label(label: &str) -> bool {
    let t = label.trim();
    if t.is_empty() {
        return true;
    }
    let lower = t.to_ascii_lowercase();
    // Exact single-word placeholders.
    if matches!(
        lower.as_str(),
        "speaker" | "you" | "others" | "unknown" | "someone" | "participant" | "attendee" | "guest"
    ) {
        return true;
    }
    // `<word> <number>` voice-cluster labels (Speaker 1, Attendee 2, Participant 3…).
    for prefix in ["speaker ", "attendee ", "participant ", "guest "] {
        if let Some(rest) = lower.strip_prefix(prefix) {
            let rest = rest.trim();
            if !rest.is_empty() && rest.chars().all(|c| c.is_ascii_digit()) {
                return true;
            }
        }
    }
    false
}

/// Build the map input lines (`@<start_ms> Speaker: text`), the sorted-unique
/// list of segment start times (for ts_ms snapping), and the distinct speaker
/// labels in first-seen order.
fn build_segment_lines(segments: &[Segment]) -> (Vec<String>, Vec<i64>, Vec<String>) {
    let mut lines = Vec::with_capacity(segments.len());
    let mut speakers: Vec<String> = Vec::new();
    let mut starts: Vec<i64> = Vec::with_capacity(segments.len());
    for s in segments {
        let speaker = if s.speaker.trim().is_empty() {
            "Speaker"
        } else {
            s.speaker.trim()
        };
        lines.push(format!("@{} {}: {}", s.start_ms, speaker, s.text.trim()));
        starts.push(s.start_ms);
        // Keep the placeholder in the map INPUT (so a turn still reads
        // `@0 Speaker: …`), but exclude generic labels from the speaker set that
        // gates valid owners — a `Speaker`/`Speaker 1` is not a real person.
        if !is_generic_speaker_label(speaker) && !speakers.iter().any(|k| k == speaker) {
            speakers.push(speaker.to_string());
        }
    }
    starts.sort_unstable();
    starts.dedup();
    (lines, starts, speakers)
}

/// Best-effort speaker labels for the raw-text (no-segments) path: the token
/// before the first `:` on lines that look like `Name: said something`.
fn speakers_from_plain_lines(lines: &[String]) -> Vec<String> {
    let mut speakers: Vec<String> = Vec::new();
    for line in lines {
        if let Some(idx) = line.find(':') {
            let name = line[..idx].trim();
            if !name.is_empty()
                && idx <= 40
                && name.split_whitespace().count() <= 4
                && !is_generic_speaker_label(name)
                && !speakers.iter().any(|k| k == name)
            {
                speakers.push(name.to_string());
            }
        }
    }
    speakers
}

/// Greedily pack whole lines into chunks under `budget` bytes (joined by `\n`).
/// A line that on its own exceeds `budget` is split at UTF-8 character boundaries
/// into `<= budget`-byte pieces so NO emitted chunk exceeds the budget — a single
/// pathologically long line (e.g. an un-segmented paste with no newlines) must not
/// bypass the ceiling and blow the model's context window.
fn chunk_lines(lines: &[String], budget: usize) -> Vec<String> {
    let budget = budget.max(1);
    let mut chunks: Vec<String> = Vec::new();
    let mut cur = String::new();
    for line in lines {
        // Whole line fits the budget: greedily pack it into the running chunk.
        if line.len() <= budget {
            if !cur.is_empty() && cur.len() + 1 + line.len() > budget {
                chunks.push(std::mem::take(&mut cur));
            }
            if !cur.is_empty() {
                cur.push('\n');
            }
            cur.push_str(line);
            continue;
        }
        // Oversized single line: flush the running chunk, then split the line into
        // budget-sized pieces at char boundaries (never mid-codepoint).
        if !cur.is_empty() {
            chunks.push(std::mem::take(&mut cur));
        }
        let mut piece = String::new();
        for ch in line.chars() {
            if !piece.is_empty() && piece.len() + ch.len_utf8() > budget {
                chunks.push(std::mem::take(&mut piece));
            }
            piece.push(ch);
        }
        if !piece.is_empty() {
            chunks.push(piece);
        }
    }
    if !cur.is_empty() {
        chunks.push(cur);
    }
    chunks
}

/// Snap a model-supplied `ts_ms` to the nearest real segment start. Preserves
/// `None`. When there are NO segments there is no validated moment to cite, so
/// any model-supplied timestamp is dropped (`None`) rather than passed through —
/// this upholds the `ts_ms: null` contract for the raw-text fallback and prevents
/// a citation deep-link to an unvalidated (possibly hallucinated) offset.
fn snap_ts(ts: Option<i64>, seg_starts: &[i64]) -> Option<i64> {
    let ts = ts?;
    if seg_starts.is_empty() {
        return None;
    }
    seg_starts
        .iter()
        .copied()
        .min_by_key(|&s| (s - ts).abs())
}

/// Build the reduce prompt's user message: the real speaker list + the JSON
/// array of per-chunk partial extractions.
fn build_reduce_input(speakers: &[String], partials: &[Extraction]) -> String {
    let speaker_block = if speakers.is_empty() {
        "(none identified)".to_string()
    } else {
        speakers.join("\n")
    };
    let partials_json = serde_json::to_string(partials).unwrap_or_else(|_| "[]".to_string());
    format!("## Speakers\n{speaker_block}\n\n## Partial extractions\n{partials_json}\n")
}

/// Shape the reduce output into the stored/served summary JSON, enforcing the
/// invariants the model can't be trusted to: owners restricted to real speakers
/// (else null), ts_ms snapped to real segment starts, lengths/counts bounded.
fn sanitize_summary(
    overview: &str,
    reduced: &Extraction,
    speakers: &HashSet<String>,
    seg_starts: &[i64],
) -> Value {
    let overview = truncate_chars(overview.trim(), MAX_OVERVIEW_CHARS);

    let decisions: Vec<Value> = reduced
        .decisions
        .iter()
        .filter(|d| !d.text.trim().is_empty())
        .take(MAX_ITEMS)
        .map(|d| {
            json!({
                "text": truncate_chars(d.text.trim(), MAX_ITEM_CHARS),
                "ts_ms": snap_ts(d.ts_ms, seg_starts),
            })
        })
        .collect();

    let action_items: Vec<Value> = reduced
        .action_items
        .iter()
        .filter(|a| !a.text.trim().is_empty())
        .take(MAX_ITEMS)
        .map(|a| {
            // Owner must be a real speaker from THIS meeting, or null. Never
            // invented, and never a generic placeholder even if the model echoed
            // one back (the speaker set already excludes them; re-check anyway).
            let owner = a
                .owner
                .as_deref()
                .map(str::trim)
                .filter(|o| speakers.contains(*o) && !is_generic_speaker_label(o));
            json!({
                "text": truncate_chars(a.text.trim(), MAX_ITEM_CHARS),
                "owner": owner,
                "ts_ms": snap_ts(a.ts_ms, seg_starts),
            })
        })
        .collect();

    json!({
        "overview": overview,
        "decisions": decisions,
        "action_items": action_items,
    })
}

fn truncate_chars(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        return s.to_string();
    }
    let mut out: String = s.chars().take(max.saturating_sub(1)).collect();
    out.push('…');
    out
}

// ── Raw-text fallback (no segments) ───────────────────────────────────────────


// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    /// **The prefix-collision regression (CodeRabbit, PR #634).** Two source
    /// ids where one is a literal prefix of the other: `"meeting-1"` and
    /// `"meeting-10"`. The pattern for the shorter one must NOT match the
    /// longer one's chunks, or the summary debit picks up a stranger's
    /// `ingested_by` and is either unattributed (two members "disagreeing")
    /// or billed to the wrong member outright.
    #[test]
    fn chunk_pattern_does_not_match_a_longer_sibling_source_id() {
        let p = super::chunk_like_pattern("meeting-1");
        assert_eq!(p, "meeting-1:chunk:%");

        // Postgres LIKE semantics for the parts that matter here: the literal
        // prefix must match exactly up to the wildcard.
        let prefix = p.trim_end_matches('%');
        assert!(
            "meeting-1:chunk:0".starts_with(prefix),
            "the meeting's OWN chunks must still match"
        );
        assert!(
            !"meeting-10:chunk:0".starts_with(prefix),
            "a longer sibling's chunks must NOT match — this is the bug the \
             :chunk: delimiter closes; a bare '%' pattern matches them"
        );
        assert!(
            !"meeting-10".starts_with(prefix),
            "a longer sibling's base atom must NOT match either"
        );
    }

    /// LIKE metacharacters in a caller-supplied `source_id` must stay
    /// literal, exactly as `store::like_prefix` escapes them — otherwise a
    /// `source_id` containing `%` would match unrelated atoms wholesale.
    #[test]
    fn chunk_pattern_escapes_like_metacharacters() {
        assert_eq!(super::chunk_like_pattern("a%b"), "a\\%b:chunk:%");
        assert_eq!(super::chunk_like_pattern("a_b"), "a\\_b:chunk:%");
        assert_eq!(super::chunk_like_pattern("a\\b"), "a\\\\b:chunk:%");
    }

    use super::*;

    fn seg(speaker: &str, start_ms: i64, text: &str) -> Segment {
        Segment {
            speaker: speaker.to_string(),
            start_ms,
            text: text.to_string(),
        }
    }

    // ── Egress gating truth table ─────────────────────────────────────────────

    #[test]
    fn egress_gating_truth_table() {
        // allow_override, egress_forbidden → blocked?
        assert!(!summary_blocked(false, false), "cloud allowed → run");
        assert!(summary_blocked(false, true), "local-only, no opt-in → blocked");
        assert!(!summary_blocked(true, true), "local-only but opted in → run");
        assert!(!summary_blocked(true, false), "opted in, cloud allowed → run");
    }

    // ── LLM error classification (retry policy) ───────────────────────────────

    #[test]
    fn classify_llm_error_separates_transient_invalid_and_config() {
        use crate::llm::LlmError;
        // Transient → re-queue the job.
        assert!(matches!(
            classify_llm_error(&LlmError::RateLimit("429".into())),
            GenError::Transient(_)
        ));
        assert!(matches!(classify_llm_error(&LlmError::Timeout), GenError::Transient(_)));
        assert!(matches!(
            classify_llm_error(&LlmError::Upstream { status: 503, body: "x".into() }),
            GenError::Transient(_)
        ));
        // Parse error of a 200 body → malformed model output, skippable per-chunk.
        assert!(matches!(
            classify_llm_error(&LlmError::Parse("bad json".into())),
            GenError::InvalidOutput(_)
        ));
        // Auth / missing-model / 4xx → configuration fault, must fail the job.
        assert!(matches!(
            classify_llm_error(&LlmError::Auth("bad key".into())),
            GenError::Config(_)
        ));
        assert!(matches!(
            classify_llm_error(&LlmError::ModelNotFound("nope".into())),
            GenError::Config(_)
        ));
        assert!(matches!(
            classify_llm_error(&LlmError::Upstream { status: 400, body: "bad req".into() }),
            GenError::Config(_)
        ));
    }

    // ── Segment assembly + timing ─────────────────────────────────────────────

    #[test]
    fn build_segment_lines_marks_timestamps_and_speakers() {
        let segs = vec![
            seg("Alice", 0, "let's use Postgres"),
            seg("Bob", 5000, "I'll write the migration"),
            seg("Alice", 9000, "ship Friday"),
        ];
        let (lines, starts, speakers) = build_segment_lines(&segs);
        assert_eq!(lines[0], "@0 Alice: let's use Postgres");
        assert_eq!(lines[1], "@5000 Bob: I'll write the migration");
        assert_eq!(starts, vec![0, 5000, 9000]);
        assert_eq!(speakers, vec!["Alice".to_string(), "Bob".to_string()]);
    }

    #[test]
    fn empty_speaker_becomes_generic_label() {
        // The blank speaker still renders as `Speaker` in the map INPUT line, but
        // a generic placeholder is excluded from the speaker set (it can never be
        // a validated action-item owner).
        let (lines, _, speakers) = build_segment_lines(&[seg("  ", 100, "hi")]);
        assert_eq!(lines[0], "@100 Speaker: hi");
        assert!(speakers.is_empty(), "generic 'Speaker' label excluded from speaker set");
    }

    #[test]
    fn generic_speaker_labels_are_excluded_but_real_names_kept() {
        // Generic placeholders → excluded; real names → kept, in first-seen order.
        let segs = vec![
            seg("Speaker 1", 0, "hi"),
            seg("Alice", 1000, "let's ship"),
            seg("You", 2000, "sounds good"),
            seg("Unknown", 3000, "who's that"),
            seg("Bob", 4000, "I'll do it"),
            seg("Attendee 2", 5000, "ok"),
        ];
        let (_, _, speakers) = build_segment_lines(&segs);
        assert_eq!(speakers, vec!["Alice".to_string(), "Bob".to_string()]);
    }

    #[test]
    fn is_generic_speaker_label_matches_placeholders() {
        for g in [
            "Speaker", "speaker", "Speaker 1", "Speaker 12", "You", "you", "Others",
            "Unknown", "Participant", "Attendee 2", "Guest", "  ", "",
        ] {
            assert!(is_generic_speaker_label(g), "{g:?} should be generic");
        }
        for real in ["Alice", "Bob Smith", "Dr. Chen", "Speaker Kim", "Jordan 3rd"] {
            assert!(!is_generic_speaker_label(real), "{real:?} should be a real name");
        }
    }

    // ── ts_ms snapping ────────────────────────────────────────────────────────

    #[test]
    fn snap_ts_picks_nearest_segment_start() {
        let starts = vec![0, 5000, 9000];
        assert_eq!(snap_ts(Some(4800), &starts), Some(5000));
        assert_eq!(snap_ts(Some(100), &starts), Some(0));
        assert_eq!(snap_ts(Some(99999), &starts), Some(9000)); // hallucinated → clamp
        assert_eq!(snap_ts(None, &starts), None);
    }

    #[test]
    fn snap_ts_drops_timestamp_without_segments() {
        // No segments ⇒ no validated moment to cite ⇒ ts_ms is nulled, never
        // passed through (upholds the raw-text-fallback `ts_ms: null` contract).
        assert_eq!(snap_ts(Some(1234), &[]), None);
        assert_eq!(snap_ts(None, &[]), None);
    }

    // ── Chunking never splits a line ──────────────────────────────────────────

    #[test]
    fn chunk_lines_packs_and_never_splits() {
        let lines: Vec<String> = (0..10).map(|i| format!("line-{i}")).collect();
        let chunks = chunk_lines(&lines, 20);
        // Reconstruct: every original line appears intact, in order.
        let rejoined: Vec<&str> = chunks.iter().flat_map(|c| c.lines()).collect();
        assert_eq!(rejoined, lines.iter().map(String::as_str).collect::<Vec<_>>());
        assert!(chunks.len() > 1, "should split into multiple chunks");
    }

    #[test]
    fn chunk_lines_oversized_line_is_split_within_budget() {
        let big = "x".repeat(100);
        let chunks = chunk_lines(&[big.clone(), "small".to_string()], 20);
        // Every emitted chunk must respect the budget — the 100-char line is split.
        for c in &chunks {
            assert!(c.len() <= 20, "chunk exceeds budget: {} bytes", c.len());
        }
        // The oversized line is split into five 20-char pieces, then "small".
        assert_eq!(chunks.iter().filter(|c| c.chars().all(|ch| ch == 'x')).count(), 5);
        assert!(chunks.contains(&"small".to_string()));
        // No content is lost: concatenating the x-pieces reconstructs the line.
        let rebuilt: String = chunks.iter().filter(|c| c.starts_with('x')).flat_map(|c| c.chars()).collect();
        assert_eq!(rebuilt, big);
    }

    #[test]
    fn chunk_lines_splits_multibyte_line_on_char_boundaries() {
        // A line of multibyte chars (3 bytes each) must never split mid-codepoint.
        let line = "语".repeat(30); // 90 bytes
        let chunks = chunk_lines(&[line.clone()], 20);
        for c in &chunks {
            assert!(c.len() <= 20, "chunk exceeds budget: {} bytes", c.len());
            assert!(c.chars().all(|ch| ch == '语'), "no codepoint was split");
        }
        let rebuilt: String = chunks.concat();
        assert_eq!(rebuilt, line);
    }

    // ── Value → Extraction robustness ─────────────────────────────────────────

    #[test]
    fn parse_extraction_handles_number_shapes_and_missing_fields() {
        let v = json!({
            "decisions": [
                { "text": "Adopt Postgres", "ts_ms": 5000 },
                { "text": "Ship Friday", "ts_ms": 9000.0 },   // float
                { "text": "No ts", "ts_ms": null },
                { "text": "   " },                              // blank → dropped
            ],
            "action_items": [
                { "text": "Write migration", "owner": "Bob", "ts_ms": "5000" }, // string int
                { "text": "Follow up", "owner": null }
            ]
        });
        let ex = parse_extraction(&v);
        assert_eq!(ex.decisions.len(), 3, "blank decision dropped");
        assert_eq!(ex.decisions[1].ts_ms, Some(9000));
        assert_eq!(ex.decisions[2].ts_ms, None);
        assert_eq!(ex.action_items[0].owner.as_deref(), Some("Bob"));
        assert_eq!(ex.action_items[0].ts_ms, Some(5000));
        assert_eq!(ex.action_items[1].owner, None);
    }

    // ── sanitize_summary: the stored/served shape + invariants ────────────────

    #[test]
    fn sanitize_summary_enforces_shape_owners_and_timestamps() {
        let speakers: HashSet<String> =
            ["Alice".to_string(), "Bob".to_string()].into_iter().collect();
        let starts = vec![0, 5000, 9000];
        let reduced = Extraction {
            decisions: vec![Item {
                text: "  Adopt Postgres  ".to_string(),
                owner: None,
                ts_ms: Some(4900), // → snaps to 5000
            }],
            action_items: vec![
                Item {
                    text: "Write the migration".to_string(),
                    owner: Some("Bob".to_string()), // real speaker → kept
                    ts_ms: Some(5100),
                },
                Item {
                    text: "Email the client".to_string(),
                    owner: Some("Charlie".to_string()), // NOT a speaker → nulled
                    ts_ms: None,
                },
            ],
        };
        let out = sanitize_summary("  The team met.  ", &reduced, &speakers, &starts);

        assert_eq!(out["overview"], "The team met.");
        // decisions
        let d = &out["decisions"][0];
        assert_eq!(d["text"], "Adopt Postgres");
        assert_eq!(d["ts_ms"], 5000);
        // action items — owner validation + null timestamp preserved
        let a0 = &out["action_items"][0];
        assert_eq!(a0["owner"], "Bob");
        assert_eq!(a0["ts_ms"], 5000);
        let a1 = &out["action_items"][1];
        assert!(a1["owner"].is_null(), "invented owner must be nulled");
        assert!(a1["ts_ms"].is_null(), "null ts stays null");

        // Full contract shape.
        assert!(out["overview"].is_string());
        assert!(out["decisions"].is_array());
        assert!(out["action_items"].is_array());
    }

    #[test]
    fn sanitize_summary_empty_is_valid_shape() {
        let out = sanitize_summary("", &Extraction::default(), &HashSet::new(), &[]);
        assert_eq!(out["overview"], "");
        assert_eq!(out["decisions"].as_array().unwrap().len(), 0);
        assert_eq!(out["action_items"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn truncate_chars_adds_ellipsis_over_limit() {
        assert_eq!(truncate_chars("hello", 10), "hello");
        let t = truncate_chars(&"a".repeat(50), 10);
        assert_eq!(t.chars().count(), 10);
        assert!(t.ends_with('…'));
    }

    #[test]
    fn build_reduce_input_includes_speakers_and_partials() {
        let partials = vec![Extraction {
            decisions: vec![Item {
                text: "Adopt Postgres".to_string(),
                owner: None,
                ts_ms: Some(5000),
            }],
            action_items: vec![],
        }];
        let input = build_reduce_input(&["Alice".to_string()], &partials);
        assert!(input.contains("## Speakers\nAlice"));
        assert!(input.contains("## Partial extractions"));
        assert!(input.contains("Adopt Postgres"));
        // The Item serializer omits a None owner (skip_serializing_if).
        assert!(!input.contains("\"owner\":null"));
    }

    #[test]
    fn parse_segments_bad_json_falls_back_to_empty() {
        assert!(parse_segments(Some(&json!("not an array"))).is_empty());
        assert!(parse_segments(None).is_empty());
        let ok = parse_segments(Some(&json!([
            { "speaker": "A", "start_ms": 0, "end_ms": 10, "text": "hi" },
            { "speaker": "B", "start_ms": 5, "text": "  " } // blank text dropped
        ])));
        assert_eq!(ok.len(), 1);
    }
}

#[cfg(test)]
mod summary_snapshot_pg_tests {
    use super::*;
    use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
    use std::str::FromStr;

    #[tokio::test]
    async fn stale_summary_completion_cannot_overwrite_new_capture_pg() {
        let Ok(url) = std::env::var("RELINFER_TEST_DATABASE_URL") else {
            assert!(std::env::var_os("REQUIRE_PG_SUITE").is_none(), "local PostgreSQL fixture required");
            eprintln!("skipping stale summary test: set RELINFER_TEST_DATABASE_URL");
            return;
        };
        let options = PgConnectOptions::from_str(&url).unwrap();
        assert!(matches!(options.get_host(), "127.0.0.1" | "localhost" | "::1"));
        let owner = PgPoolOptions::new().max_connections(4).connect_with(options.clone()).await.unwrap();
        let app = PgPoolOptions::new().max_connections(4).after_connect(|connection, _| Box::pin(async move {
            sqlx::query("SET ROLE coralswarm_app").execute(connection).await?;
            Ok(())
        })).connect_with(options).await.unwrap();
        assert_eq!(sqlx::query_scalar::<_, String>("SELECT current_user::text").fetch_one(&app).await.unwrap(), "coralswarm_app");
        let user: Uuid = sqlx::query_scalar("INSERT INTO users(email,clerk_user_id) VALUES($1,$1) RETURNING id")
            .bind(format!("summary-stale-{}@test.local", Uuid::new_v4())).fetch_one(&owner).await.unwrap();
        let account: Uuid = sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES($1) RETURNING id")
            .bind(user).fetch_one(&owner).await.unwrap();
        sqlx::query("INSERT INTO account_members(account_id,user_id,role) VALUES($1,$2,'owner')")
            .bind(account).bind(user).execute(&owner).await.unwrap();
        let graph: Uuid = sqlx::query_scalar("INSERT INTO graphs(account_id,name) VALUES($1,'summary stale fixture') RETURNING id")
            .bind(account).fetch_one(&owner).await.unwrap();
        let state = std::sync::Arc::new(crate::test_app_state_owned_with_db_and_store(app.clone(), None));
        let store = crate::store::PostgresStore::new(app.clone());
        let scope = JobScope::for_job(&TenantId::Consumer, &app, account);
        let source = "stale-summary-source";
        let mut tx = begin_account_scoped(&app, account).await.unwrap();
        let first = crate::meeting_evidence::capture_version_tx(&mut tx, &state, graph, account, source,
            "Alice: Version A is the old plan.", None, None, None, &[42; 32]).await.unwrap();
        tx.commit().await.unwrap();
        let (_, snapshot) = scoped_get_meeting_transcript(&scope, &store, graph, source).await.unwrap().unwrap();
        assert_eq!(snapshot.version_id, Some(first.version_id));
        assert!(scoped_set_summary_status(&scope, &store, graph, source, "running", &snapshot).await.unwrap());
        let kind = crate::jobs::JobKind::GenerateMeetingSummary {
            graph_id: graph, source_id: source.into(), allow_cloud_override: false,
        };
        let summary_key = format!("summary:{graph}:{source}");
        let old = queue::enqueue(&app, &kind, Some(account), Some(&summary_key), 0)
            .await.unwrap().unwrap();
        let initial_old: (String, Option<String>) = sqlx::query_as("SELECT status::text,error_message FROM jobs WHERE id=$1")
            .bind(old).fetch_one(&owner).await.unwrap();
        assert_eq!(initial_old, ("pending".into(), None), "a managed_enrichment kind is admitted, never dead-lettered");
        // The worker claimed it; a newer capture's pass can now be queued.
        sqlx::query("UPDATE jobs SET status='running',started_at=now(),attempts=1 WHERE id=$1").bind(old).execute(&owner).await.unwrap();
        let mut tx = begin_account_scoped(&app, account).await.unwrap();
        let replacement = crate::meeting_evidence::capture_version_tx(&mut tx, &state, graph, account, source,
            "Bob: Version B changes the plan.", None, None, None, &[42; 32]).await.unwrap();
        tx.commit().await.unwrap();
        assert_ne!(first.version_id, replacement.version_id);
        let replacement_job = queue::enqueue(&app, &kind, Some(account), Some(&summary_key), 0)
            .await.unwrap().unwrap();
        async fn replacement_state(owner: &sqlx::PgPool, graph: Uuid, source: &str, job: Uuid) -> (Value, Value) {
            let transcript = sqlx::query_scalar("SELECT to_jsonb(t) FROM meeting_transcripts t WHERE graph_id=$1 AND source_id=$2")
                .bind(graph).bind(source).fetch_one(owner).await.unwrap();
            let job = sqlx::query_scalar("SELECT to_jsonb(j) FROM jobs j WHERE id=$1")
                .bind(job).fetch_one(owner).await.unwrap();
            (transcript, job)
        }
        let replacement_before = replacement_state(&owner, graph, source, replacement_job).await;
        assert_eq!(replacement_before.1["status"], "pending");
        assert_eq!(replacement_before.1["error_message"], Value::Null);
        assert_eq!(replacement_before.1["payload"], serde_json::to_value(&kind).unwrap()["payload"]);
        let work = EnrichmentWork::new(account, graph, Some(EnrichmentJob { id: old, kind: "GenerateMeetingSummary".into(), attempts: 1 }),
            None, vec![transcript_input(&snapshot)]).unwrap();
        assert_eq!(scoped_set_summary_done(&scope, &store, graph, source, &serde_json::json!({"overview":"Stale A"}), "synthetic-model", &snapshot, &work).await.unwrap(), Publish::Skipped);
        assert_eq!(replacement_state(&owner, graph, source, replacement_job).await, replacement_before);
        assert!(!scoped_set_summary_status(&scope, &store, graph, source, "running", &snapshot).await.unwrap());
        assert_eq!(replacement_state(&owner, graph, source, replacement_job).await, replacement_before);
        assert!(!scoped_set_summary_failed(&scope, &store, graph, source, "synthetic stale failure", &snapshot).await.unwrap());
        assert_eq!(replacement_state(&owner, graph, source, replacement_job).await, replacement_before);
        let job = JobRow { id: old, kind: "GenerateMeetingSummary".into(),
            payload: serde_json::to_value(kind).unwrap()["payload"].clone(), attempts: 1, account_id: Some(account) };
        mark_failed(&scope, &app, &store, &job, graph, source, "synthetic stale failure", &snapshot).await;
        assert_eq!(replacement_state(&owner, graph, source, replacement_job).await, replacement_before);
        let current: (Uuid, String, String, Option<Value>, Option<String>, Option<String>) = sqlx::query_as(
            "SELECT current_version_id,raw_inline,summary_status,summary,summary_error,summary_model \
             FROM meeting_transcripts WHERE graph_id=$1 AND source_id=$2",
        ).bind(graph).bind(source).fetch_one(&owner).await.unwrap();
        assert_eq!(current, (replacement.version_id, "Bob: Version B changes the plan.".into(),
            "pending".into(), None, None, None));
        let outcome: (String, Option<String>) = sqlx::query_as("SELECT status::text,outcome FROM jobs WHERE id=$1")
            .bind(old).fetch_one(&owner).await.unwrap();
        assert_eq!(outcome, ("done".into(), Some("nothing_to_do".into())));
        assert_eq!(sqlx::query_scalar::<_, i64>("SELECT count(*) FROM llm_usage WHERE source_job_id=ANY($1)")
            .bind(vec![old, replacement_job]).fetch_one(&owner).await.unwrap(), 0);
        app.close().await;
        owner.close().await;
    }
}
