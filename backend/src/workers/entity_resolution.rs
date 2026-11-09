//! Entity-resolution worker (ADR-0008, Phase 0).
//!
//! Polls `EntityResolution` jobs for a graph, finds candidate duplicate entity
//! reefs (normalized-name / alias / embedding blocking), adjudicates each pair
//! with the LLM using **attributes + relationships** (not name similarity), and
//! merges confirmed duplicates via the existing `store.merge_nodes`. Ambiguous
//! pairs are left for human review; "different" verdicts are recorded so they
//! are not re-proposed.
//!
//! ## Managed enrichment (source-agnostic plan PR 5)
//!
//! The worker runs on the shared `managed_enrichment` purpose. It claims
//! nothing unless `ENRICHMENT_PROCESSING_ENABLED=true`. Every judged pair is
//! one managed logical call per model request (Jev's Choice when
//! `JEV_JUDGE_MODE` is active, else the LLM verdict, plus the optional
//! UNSURE-band escalation), each reserving the shared daily cap and binding
//! its inputs: both reefs and a hash of the exact prompt the judge saw.
//! Merges, judgements and dismissals are published in one transaction only
//! when those inputs are unchanged. The identifier auto-merge and the
//! materialisation of an existing POSITIVE stay free (no model call, no
//! receipt). Candidates are the lexical page, the provisional seeds, pending
//! POSITIVEs and a bounded same-title sweep ordered by coral count; judged
//! calls are bounded per graph per UTC day (`ER_DAILY_CALLS_PER_GRAPH`) and
//! per account by a dollar share of the shared cap
//! (`ER_DAILY_SHARE_OF_ENRICHMENT`, 0.3), so the journal, coral titles and
//! the supersession judge always keep headroom.
//! Producers: standing extraction after it binds reefs
//! (`enqueue_after_binding`) and the worker itself while a judged backlog
//! remains. See `docs/connectors/CORPUS_ENRICHMENT_CONTRACT.md`.
//!
//! ## Per-job RLS scope (org-tenancy Phase W.2)
//!
//! Every RLS-covered statement (`nodes`/`node_atoms` inline queries and the
//! `Store` calls over `nodes`/`node_merges`/`relationship_edge`/`atoms`) runs
//! under the job's [`JobScope`]: on the consumer plane each statement gets a
//! fresh short `tenancy::scope::begin_account_scoped` transaction for
//! `job.account_id` (W.1's per-job scoping input); on an org tenant each gets
//! a `begin_tenant_system_scoped_without_account` transaction instead.
//! Whole-database isolation does NOT stand in for that scope. The isolation
//! is real and covers the CROSS-ORG boundary only, while
//! `migrations_tenant/0009_tenant_rls` puts member-ocean policies on
//! `nodes`/`node_atoms`/`node_merges`/`relationship_edge`/`atoms` inside a
//! tenant DB. Unscoped, once OPS-4-T moves the tenant DSN to the NOBYPASSRLS
//! role, this worker would adjudicate the org and personal oceans and leave
//! every member and team ocean unresolved, reporting a normal pass.
//! Per-statement, not per-job, because the adjudication loop
//! interleaves DB statements with LLM network calls — same reasoning as
//! [`JobScope`]'s own doc. A job whose payload `graph_id` resolves (via the
//! 0084 SECURITY DEFINER resolver) to a DIFFERENT account than its stamp is
//! refused before any content query runs — load-bearing pre-OPS-4, where the
//! scope GUC is inert against the BYPASSRLS owner role; defense in depth
//! after.

use std::collections::HashSet;
use std::time::Duration;

use serde::Deserialize;
use sqlx::PgPool;
use uuid::Uuid;

use coralswarm_core::send_gate::{Deny, HoldCode};

use crate::jobs::queue::{self, JobRow};
use crate::llm::model_config;
use crate::managed::enrichment::{self, EnrichmentInput, EnrichmentJob, EnrichmentWork, InputCheck};
use crate::llm::{CompletionRequest, LlmError, LlmProvider, Message, Role, ToolSchema};
use crate::review_policy;
use crate::store::{
    AnnulKind, JudgementSource, JudgementVerdict, NewJudgement, Node, PendingPositiveJudgements,
    ReefJudgement, Store, StoreError,
};
use crate::tenancy::scope::{begin_account_scoped, begin_tenant_system_scoped_without_account};
use crate::tenancy::TenantId;
use crate::workers::embed::JobScope;
use crate::workers::tenant_harness::{Tick, WorkerTenant};
use crate::workers::verify_job_scope;

/// System prompt instructing the LLM to adjudicate two candidate reefs.
pub(crate) const SYSTEM_PROMPT: &str = include_str!("../../prompts/resolve_entities_v1.txt");

const JOB_BATCH_SIZE: usize = 4;
/// Idle poll interval when the queue is empty. `pub(crate)`: fed to the
/// tenant harness as this worker's `base_poll` (see `workers::tenant_harness`).
pub(crate) const EMPTY_POLL_SLEEP: Duration = Duration::from_millis(500);

// The model id, response token cap, and merge-confidence threshold are resolved
// from `crate::llm::model_config` (audit S1 / H4); defaults equal the previous
// hardcoded values (`deepseek/deepseek-v4-flash-0731`, 256 tokens, 0.85).
/// How many entity reefs to scan for candidate pairs per job, in the default
/// Rust blocking path (blocking is O(n²) over titles, so keep `n` bounded).
/// This is now a PER-PAGE size, not a whole-ocean cap: `resolve_graph`'s
/// `BlockingMode::Rust` arm keyset-paginates by `(updated_at, node_id)` across
/// successive debounced job runs (persisted on `graphs.er_sweep_cursor_*`),
/// so an ocean with more than `MAX_ENTITY_NODES` entity reefs is still fully
/// swept — just over more than one job — rather than permanently truncated
/// ("lift the 500-node cap").
const MAX_ENTITY_NODES: i64 = 500;
/// Hard cap on LLM adjudications per job so one swarmy graph can't run up cost.
///
/// NOTE this is a budget on LLM SPEND, not on coverage. It used to be applied
/// by `considered.truncate(MAX_PAIRS_PER_JOB)` over pairs in nested-loop index
/// order, which silently made it a coverage cap too — and a viciously unfair
/// one, since index order is `(updated_at ASC, node_id ASC)` and therefore
/// ranks the ocean's most active reefs dead last. [`select_pairs`] now decides
/// WHICH pairs get the budget; this constant only decides HOW MANY.
///
/// 100 (was 50): a pass also stops at [`pass_budget`], so a larger budget
/// only lets a fast pass do more before it hands over.
const MAX_PAIRS_PER_JOB: usize = 100;
/// Pairs per round trip in the pre-cap batch gate lookup — see
/// [`scoped_list_active_judgements_for_pairs`] for why the input can't be
/// assumed small.
const JUDGEMENT_GATE_BATCH: usize = 1_000;
/// Design doc §5.3's per-graph UNSURE flood-control cap — at/over this many
/// active (`annulled_at IS NULL`) UNSURE rows, a new UNSURE write is skipped
/// (falls through to today's debug-log-only) rather than growing the review
/// queue unbounded among name-similar candidates.
const MAX_ACTIVE_UNSURE_PER_GRAPH: i64 = 25;
/// `reef_judgement.prompt_version` stamped on every LLM-sourced judgement
/// this worker writes, and the "current" value the §5.0 read gate compares a
/// NEGATIVE(llm) row's stored `prompt_version` against to decide whether a
/// prompt bump reopens re-adjudication (`gate_pair`'s `negative` arm).
/// Mirrors `conflict_detection.rs:137`'s `CONFLICT_PROMPT_VERSION` — bump
/// this alongside any change to `SYSTEM_PROMPT`'s adjudication instructions.
pub(crate) const RESOLVE_PROMPT_VERSION: &str = "resolve_entities_v1";
/// Default trigram similarity threshold for the opt-in `trgm` blocking path.
const DEFAULT_TRGM_THRESHOLD: f64 = 0.3;
/// Page size for the opt-in `trgm` candidate-pair query (NOT a 500-style cap on
/// total entities — paging continues until the DB returns a short page, so the
/// trgm path is not bounded by entity count the way the rust path is).
const TRGM_PAGE_SIZE: i64 = 1000;
/// Facts (atom snippets / edges) fed per candidate side.
const MAX_FACTS_PER_SIDE: usize = 6;
/// Atom-content snippet length per fact (chars).
const FACT_SNIPPET_LEN: usize = 240;

/// Identifier kinds (T2 "identifier awareness") near-decisive enough to
/// auto-merge a pair WITHOUT an LLM call when shared and non-conflicting —
/// see [`identifier_evidence`]. `git_author` is deliberately excluded: a
/// commit-author string is a much weaker signal (shared CI/bot accounts,
/// `Co-authored-by` trailers, pair-programming) than owning a specific email
/// address, Slack account, or Clerk login — it still informs the LLM prompt
/// as a fact, it just never triggers the no-LLM shortcut on its own.
pub(crate) const DECISIVE_IDENTIFIER_KINDS: &[&str] = &["email", "slack_user", "clerk_user"];

/// True when two of `node_ids` both carry a decisive identifier kind and
/// share no value of it (two different emails): they are two identities,
/// whatever their titles say. The title-only paths (exact-title binding,
/// `dedupe_reefs`) never fold such reefs together, and ER never asks a
/// model to.
pub(crate) async fn identities_conflict_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    node_ids: &[Uuid],
) -> Result<bool, sqlx::Error> {
    if node_ids.len() < 2 {
        return Ok(false);
    }
    sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM reef_identifiers x JOIN reef_identifiers y ON y.kind=x.kind AND y.node_id>x.node_id \
          WHERE x.node_id=ANY($1) AND y.node_id=ANY($1) AND x.kind=ANY($2) \
            AND NOT EXISTS(SELECT 1 FROM reef_identifiers x2 JOIN reef_identifiers y2 ON y2.kind=x2.kind AND y2.value_norm=x2.value_norm \
                            WHERE x2.node_id=x.node_id AND y2.node_id=y.node_id AND x2.kind=x.kind))",
    )
    .bind(node_ids)
    .bind(DECISIVE_IDENTIFIER_KINDS)
    .fetch_one(&mut **tx)
    .await
}

/// Identity-resolution-at-the-door design doc §10.5: cap on how many
/// provisional reefs (`body.provisional = true`) this sweep scans for seeded
/// pairs, newest-first. A runaway inflow of provisional mints must not turn
/// this scan into an unbounded table scan on every job tick — mirrors the
/// bounding discipline every other per-sweep query in this file already
/// applies ([`MAX_ENTITY_NODES`], [`TRGM_PAGE_SIZE`]).
const MAX_PROVISIONAL_SEEDS: i64 = 200;

/// `JUDGEMENT_GRAPH_WRITES` — the judgement-graph resolver's master rollout
/// switch (design doc §9), THREE states rather than a boolean. `pub(crate)`:
/// `routes/nodes.rs`'s `merge_node`/`dismiss_merge_suggestion` read it too
/// (founder-Q4 bypass coverage, PR2 §6) — one kill-switch governs every
/// judgement-graph writer, worker or route.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum JudgementWriteMode {
    /// Unset, blank, or any value not recognized below. Byte-identical to
    /// pre-PR2 behavior: no gate reads, no candidate-stream union, no
    /// `reef_judgement` writes anywhere (worker or routes).
    Off,
    /// `log` (case-insensitive). Today's merge/dismiss behavior fires
    /// UNCHANGED (same calls as `Off`) — this is the founder's pre-rollout
    /// volume estimate (design doc §5.3): a per-job `tracing::info!` counts
    /// what WOULD have been written (identifier/llm-same auto-merges,
    /// negatives, UNSURE writes, UNSURE cap hits), but nothing is ever
    /// written to `reef_judgement`, no gate is consulted, and the candidate
    /// stream is never unioned with pending positives.
    Log,
    /// `1`/`true`/`yes`/`on` (case-insensitive). The §5.0 read gate is
    /// consulted, the candidate stream is unioned with pending-positive
    /// judgements, judgements are written, and materialization runs via
    /// `merge_nodes_with_judgement`.
    On,
}

/// Parses `JUDGEMENT_GRAPH_WRITES` with the usual truthy-token discipline:
/// `1`/`true`/`yes`/`on` (case-insensitive) → `On`;
/// `log` (case-insensitive) → `Log`; anything else — unset, blank, or
/// garbage — → `Off`. Read at CALL TIME, not cached in a `OnceLock` — same
/// reason `blocking_mode()` below isn't cached (env-var unit tests flip this
/// mid-process).
///
/// Known ops corner (deliberately not engineered around): an `On` → `Log`
/// downgrade leaves any live judgement rows in place, which `Log` mode
/// ignores (no gate reads) — a pair with an active judgement can then see an
/// LLM re-spend under `Log` that `On` would have suppressed. Acceptable:
/// `Log` is a pre-rollout / rollback posture, not a steady state.
pub(crate) fn judgement_write_mode() -> JudgementWriteMode {
    match std::env::var("JUDGEMENT_GRAPH_WRITES") {
        Ok(v) => {
            let v = v.trim().to_ascii_lowercase();
            if v == "log" {
                JudgementWriteMode::Log
            } else if matches!(v.as_str(), "1" | "true" | "yes" | "on") {
                JudgementWriteMode::On
            } else {
                JudgementWriteMode::Off
            }
        }
        Err(_) => JudgementWriteMode::Off,
    }
}

/// One entity-resolution pass on one tenant's (pool, store) pair: dequeue up
/// to [`JOB_BATCH_SIZE`] `EntityResolution` jobs and process them. Driven by
/// `workers::tenant_harness::run_multi_tenant` (org-tenancy PR 2.1), which
/// owns the loop, sleeps, shutdown, and per-org idle backoff.
///
/// Managed (source-agnostic plan PR 5): nothing is claimed unless
/// [`enrichment::claim_allowed`] (`ENRICHMENT_PROCESSING_ENABLED=true` and the
/// global kill switch off). `control` is the control-plane pool the managed
/// gate reserves credits and the daily budget on.
pub async fn tick(t: &WorkerTenant, control: &PgPool, llm: &dyn LlmProvider) -> Result<Tick, String> {
    if !enrichment::claim_allowed() {
        return Ok(Tick::idle());
    }
    let jobs = queue::dequeue(&t.pool, &["EntityResolution"], JOB_BATCH_SIZE)
        .await
        .map_err(|e| format!("dequeue failed: {e}"))?;

    if jobs.is_empty() {
        return Ok(Tick::idle());
    }

    // Concurrently: each job is one graph's pass, and run one after another
    // the fourth job's claim aged by the first three's whole runtime, past
    // the reaper's 20-minute `stale_after` (2026-10-20: passes up to 91
    // minutes, reaped and retried while still running).
    let n = jobs.len();
    futures::future::join_all(
        jobs.into_iter().map(|job| process_job(&t.tenant, &t.pool, control, t.store.as_ref(), llm, job)),
    )
    .await;
    Ok(Tick::worked(n))
}

pub(crate) async fn process_job(
    tenant: &TenantId,
    pool: &PgPool,
    control: &PgPool,
    store: &dyn crate::store::Store,
    llm: &dyn LlmProvider,
    job: JobRow,
) {
    let graph_id = match job
        .payload
        .get("graph_id")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<Uuid>().ok())
    {
        Some(id) => id,
        None => {
            tracing::error!(job_id = %job.id, "entity-resolution: malformed payload (missing graph_id)");
            if let Err(e) = queue::fail(pool, job.id, "malformed payload: missing graph_id").await {
                tracing::error!(error = %e, job_id = %job.id, "failed to mark malformed job failed");
            }
            return;
        }
    };

    // ── Per-job RLS scope (org-tenancy Phase W.2) ─────────────────────────────
    // Fail-closed on a missing `account_id` stamp and on a cross-account
    // payload — see `workers::verify_job_scope` for both checks' rationale.
    let Some(account_id) = verify_job_scope(pool, &job, graph_id, "entity-resolution").await else {
        return;
    };

    let scope = JobScope::for_job(tenant, pool, account_id);

    // Switched off between claim and processing: spend nothing, write
    // nothing, hand the graph back for a later pass.
    if !enrichment::claim_allowed() {
        finish_and_follow_up(pool, &job, graph_id, account_id, queue::JobOutcome::NothingToDo,
            Some(FollowUp::Held(HoldCode::ProcessingDisabled))).await;
        return;
    }
    // Settle any receipt an earlier crashed attempt of this job left behind
    // before opening a new call (managed::enrichment step 1).
    if let Err(deny) = enrichment::recover_job(&scope, control, tenant, account_id, job.id).await {
        tracing::warn!(job_id = %job.id, %graph_id, error = %deny, "entity-resolution: managed receipt recovery unavailable — retrying the job");
        if let Err(e) = queue::fail(pool, job.id, "managed enrichment recovery unavailable").await {
            tracing::error!(error = %e, job_id = %job.id, "failed to mark entity-resolution job failed");
        }
        return;
    }
    let mut managed = Managed {
        control,
        tenant,
        account_id,
        job: EnrichmentJob { id: job.id, kind: job.kind.clone(), attempts: job.attempts },
        principal: None,
        pairs_left: 0,
    };
    managed.pairs_left = match judged_pairs_left(&scope, account_id, graph_id).await {
        Ok(left) => left,
        Err(e) => {
            tracing::error!(error = %e, job_id = %job.id, %graph_id, "entity-resolution: daily pair bound unreadable — failing job");
            if let Err(qe) = queue::fail(pool, job.id, &e).await {
                tracing::error!(error = %qe, job_id = %job.id, "failed to mark entity-resolution job failed");
            }
            return;
        }
    };

    let pass = match resolve_graph(&scope, store, llm, &mut managed, graph_id, job.id).await {
        Ok(pass) => pass,
        Err(e) => {
            tracing::error!(error = %e, job_id = %job.id, %graph_id, "entity-resolution: DB error — failing job");
            if let Err(qe) = queue::fail(pool, job.id, &e).await {
                tracing::error!(error = %qe, job_id = %job.id, "failed to mark entity-resolution job failed");
            }
            return;
        }
    };
    let outcome = if pass.merges > 0 || pass.adjudicated > 0 {
        queue::JobOutcome::DidWork
    } else {
        queue::JobOutcome::NothingToDo
    };
    let follow_up = match pass.stop {
        Some(Stop::DailyPairs | Stop::DailyShare) => Some(FollowUp::NextDay),
        Some(Stop::Hold(HoldCode::DailyBudgetExhausted)) => Some(FollowUp::NextDay),
        Some(Stop::Hold(code)) => Some(FollowUp::Held(code)),
        Some(Stop::Unavailable(_)) => Some(FollowUp::Retry),
        // Judged something and stopped only because the per-job budget ran
        // out: the backlog continues shortly (bounded per day above).
        None if (pass.adjudicated > 0 || pass.cut_short) && pass.deferred > 0 => Some(FollowUp::Continue),
        None => None,
    };
    if let Some(stop) = &pass.stop {
        tracing::info!(job_id = %job.id, %graph_id, stop = stop.code(), adjudicated = pass.adjudicated,
            merges = pass.merges, "entity-resolution: pass stopped; graph rescheduled");
    }
    finish_and_follow_up(pool, &job, graph_id, account_id, outcome, follow_up).await;
}

// ── Managed enrichment (source-agnostic plan PR 5) ────────────────────────────
//
// Every model call this worker makes (the Jev judge seat, the LLM verdict,
// and the UNSURE-band escalation) is its own `managed_enrichment` logical
// call: the gate reserves the shared daily budget
// (`MANAGED_DAILY_CAP_USD_ENRICHMENT`) and the payer's credits before each
// attempt and writes the call's inputs with its receipt. The inputs are the
// two candidate reefs and a hash of the exact prompt the judge saw. Merges,
// judgements and dismissals are written in ONE transaction that re-reads
// those inputs under row locks and publishes only when they are unchanged
// (`enrichment::verify_inputs_locked` -> `Fresh`). Free paths (identifier
// auto-merge, materialising an existing POSITIVE) never open a call.

/// Debounce for the per-graph `er:<graph>` key when a producer binds reefs.
pub(crate) const ER_DEBOUNCE_SECS: i64 = 60;
/// A pass that judged pairs and still had a backlog continues after this.
/// Short (was 120 s): the daily bound and the shared cap are what limit
/// spend, so idle minutes between passes only slow the backlog down.
const ER_CONTINUE_SECS: i64 = 15;
/// Wall-clock budget for one pass (`ER_PASS_BUDGET_SECS`). Once spent, the
/// pass judges no further pair, keeps its sweep cursor and continues in
/// [`ER_CONTINUE_SECS`], so a pass ends well inside the reaper's 20-minute
/// `stale_after` and is never reaped while still running.
const DEFAULT_PASS_BUDGET_SECS: u64 = 480;
/// How often one process emits the identity-door convergence gauges per
/// graph. Their `rebirths_24h` query scans every alias in the graph for
/// every reef minted in the last day: over 2 minutes on the 2026-10-20
/// org ocean, and it ran at the end of every pass.
const GAUGE_INTERVAL: Duration = Duration::from_secs(3600);
/// A pass held after it made progress (merges or verdicts) is retried after
/// this. A pass held with no progress goes back on the queue in place under
/// the shared retry rule (`jobs::queue::hold`) instead.
const ER_HOLD_RETRY_SECS: i64 = 900;
/// Managed storage unavailable after progress: retried after this.
const ER_UNAVAILABLE_RETRY_SECS: i64 = 300;
/// Default bound on judged calls per graph per UTC day
/// (`ER_DAILY_CALLS_PER_GRAPH`). At the measured ~$0.0001-0.0002 per call
/// this is at most ~$0.20 per graph per day; the account-wide
/// `managed_enrichment` cap ($2/day) is the hard bound across graphs.
///
/// 3,000 (was 1,000): the same-title backlog was ~10,800 pairs on 2026-10-20
/// and the bound alone made it more than a week. 3,000 calls is about $0.50.
const DEFAULT_DAILY_CALLS_PER_GRAPH: i64 = 3000;
/// `ER_DAILY_SHARE_OF_ENRICHMENT`: entity resolution's share of the account's
/// shared `managed_enrichment` cap (`MANAGED_DAILY_CAP_USD_ENRICHMENT`), as a
/// fraction of it, across all of its graphs, counted from the receipts
/// (settled, else reserved) that bind an [`INPUT_FACTS`] marker: every call
/// this worker opens (Jev's Choice, the LLM verdict, the escalation). Checked
/// before each call; over it, the pass stops and the graph comes back after
/// the UTC day turns, like [`Stop::DailyPairs`].
///
/// Before it the per-graph call bound was the only limit, and on 2026-10-27
/// the org account's judge calls (`jev_judge`, ~$0.75) and digests (~$1.00)
/// spent the whole $2 cap by 04:00-06:30 UTC every day, so the journal,
/// coral titles and the supersession judge were withheld for the rest of it.
/// 0.3 ($0.60 at $2, $3 at $10) with the digests' 0.5
/// (`workers::digest::DEFAULT_DIGEST_DAILY_SHARE`) leaves a fifth of the cap
/// to those three, which spent $0.25 together on 2026-10-20.
const ER_DAILY_SHARE_ENV: &str = "ER_DAILY_SHARE_OF_ENRICHMENT";
const DEFAULT_ER_DAILY_SHARE: f64 = 0.3;
/// Same-title candidate pairs fed into one pass (before the per-job
/// [`MAX_PAIRS_PER_JOB`] budget selects among them).
const SAME_TITLE_PAIRS_PER_JOB: i64 = 200;
/// Minimum `same` confidence to merge a pair when either side is a topic or
/// has no recorded type. The same word as a topic often names different
/// things in different sources (`main`, `slack`, `postgres`), so a topic
/// merge needs more certainty than a named entity's.
const TOPIC_MERGE_CONFIDENCE: f64 = 0.9;
/// Input kinds this worker binds into each receipt.
const INPUT_NODE: &str = "node";
const INPUT_FACTS: &str = "er_facts";

fn pass_budget() -> Duration {
    Duration::from_secs(
        std::env::var("ER_PASS_BUDGET_SECS")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(DEFAULT_PASS_BUDGET_SECS),
    )
}

fn daily_calls_per_graph() -> i64 {
    std::env::var("ER_DAILY_CALLS_PER_GRAPH")
        .ok()
        .and_then(|v| v.trim().parse::<i64>().ok())
        .filter(|n| *n >= 0)
        .unwrap_or(DEFAULT_DAILY_CALLS_PER_GRAPH)
}

/// The job's managed context, threaded through one pass.
struct Managed<'a> {
    control: &'a PgPool,
    tenant: &'a TenantId,
    account_id: Uuid,
    job: EnrichmentJob,
    /// The member every call of this pass is charged to, selected once. The
    /// gate's locked fence re-checks it before every attempt.
    principal: Option<Uuid>,
    /// Judged calls this graph may still open today.
    pairs_left: i64,
}

/// Why a pass stopped before its candidate list ran out.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Stop {
    DailyPairs,
    /// The account's entity-resolution dollar share for the UTC day
    /// ([`ER_DAILY_SHARE_ENV`]).
    DailyShare,
    Hold(HoldCode),
    Unavailable(String),
}

impl Stop {
    fn code(&self) -> &str {
        match self {
            Stop::DailyPairs => "daily_pair_bound",
            Stop::DailyShare => "daily_usd_share",
            Stop::Hold(code) => code.code(),
            Stop::Unavailable(reason) => reason.as_str(),
        }
    }
}

#[derive(Debug, Default)]
struct PassReport {
    merges: usize,
    adjudicated: usize,
    deferred: usize,
    /// Stopped at [`pass_budget`] with selected pairs left.
    cut_short: bool,
    stop: Option<Stop>,
}

/// When the graph's next pass runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FollowUp {
    Continue,
    NextDay,
    Held(HoldCode),
    Retry,
}

impl FollowUp {
    fn delay_secs(self, now: chrono::DateTime<chrono::Utc>) -> i64 {
        match self {
            FollowUp::Continue => ER_CONTINUE_SECS,
            FollowUp::Held(_) => ER_HOLD_RETRY_SECS,
            FollowUp::Retry => ER_UNAVAILABLE_RETRY_SECS,
            FollowUp::NextDay => seconds_until_next_utc_day(now) + 60,
        }
    }
}

fn seconds_until_next_utc_day(now: chrono::DateTime<chrono::Utc>) -> i64 {
    let tomorrow = (now.date_naive() + chrono::Days::new(1))
        .and_hms_opt(0, 0, 0)
        .expect("midnight exists")
        .and_utc();
    (tomorrow - now).num_seconds().max(1)
}

/// Producer hook: a standing-extraction window just bound reefs in
/// `graph_id`, so schedule one debounced resolution pass for the graph, and
/// after it the graph's deterministic edge pass (`InferRelationships`:
/// inference closure and statement co-occurrence, `crate::co_occurrence`),
/// so derived edges see the merges. Nothing is scheduled while enrichment is
/// switched off, so the off state leaves the queue exactly as before.
/// Detached from any source publication: a graph-level pass must never hold
/// a source open.
pub(crate) async fn enqueue_after_binding(
    conn: &mut sqlx::PgConnection,
    account_id: Uuid,
    graph_id: Uuid,
) -> Result<(), sqlx::Error> {
    if !enrichment::enabled() {
        return Ok(());
    }
    enqueue_pass(conn, graph_id, account_id, ER_DEBOUNCE_SECS).await?;
    queue::enqueue_detached(
        conn,
        &crate::jobs::JobKind::InferRelationships { graph_id },
        Some(account_id),
        Some(&format!("infer:{graph_id}")),
        ER_DEBOUNCE_SECS * 2,
    )
    .await
    .map(|_| ())
}

async fn enqueue_pass(
    conn: &mut sqlx::PgConnection,
    graph_id: Uuid,
    account_id: Uuid,
    delay_secs: i64,
) -> Result<(), sqlx::Error> {
    queue::enqueue_detached(
        conn,
        &crate::jobs::JobKind::EntityResolution { graph_id },
        Some(account_id),
        Some(&format!("er:{graph_id}")),
        delay_secs,
    )
    .await
    .map(|_| ())
}

/// Completes the job, then schedules the graph's next pass when one is due.
/// A held pass that made no progress is instead held in place under the
/// shared retry rule (`jobs::queue::hold`): a hold that can clear backs off
/// to a cap, one that cannot dead-letters after a few tries and the next
/// binding re-schedules the graph. A capped pass comes back the next day.
async fn finish_and_follow_up(
    pool: &PgPool,
    job: &JobRow,
    graph_id: Uuid,
    account_id: Uuid,
    outcome: queue::JobOutcome,
    follow_up: Option<FollowUp>,
) {
    let hold = match follow_up {
        Some(FollowUp::Held(code)) => Some(code.code()),
        Some(FollowUp::Retry) => Some(crate::jobs::queue::MANAGED_UNAVAILABLE),
        _ => None,
    };
    if let (Some(code), queue::JobOutcome::NothingToDo) = (hold, outcome) {
        match queue::hold(pool, job.id, code).await {
            Ok(held) => tracing::info!(job_id = %job.id, %graph_id, code, ?held,
                "entity-resolution: pass held; back on the queue under the shared retry rule"),
            Err(e) => tracing::warn!(job_id = %job.id, %graph_id, error = %e,
                "entity-resolution: failed to hold the pass; the lease reaper retries it"),
        }
        return;
    }
    if let Err(e) = queue::complete(pool, job.id, outcome).await {
        tracing::error!(error = %e, job_id = %job.id, "entity-resolution: failed to complete job");
        return;
    }
    let Some(follow_up) = follow_up else {
        return;
    };
    let delay = follow_up.delay_secs(chrono::Utc::now());
    let enqueued = async {
        let mut conn = pool.acquire().await?;
        enqueue_pass(&mut conn, graph_id, account_id, delay).await
    }
    .await;
    match enqueued {
        Ok(()) => tracing::info!(job_id = %job.id, %graph_id, ?follow_up, delay_secs = delay,
            "entity-resolution: next pass scheduled"),
        Err(e) => tracing::warn!(job_id = %job.id, %graph_id, error = %e,
            "entity-resolution: failed to schedule the next pass; the next binding re-schedules it"),
    }
}

/// Stop unless the account's entity-resolution spend this UTC day (every
/// graph, every call that bound an [`INPUT_FACTS`] marker) is under its
/// share of the shared cap ([`ER_DAILY_SHARE_ENV`]). Read before every call,
/// so the share overshoots by at most the one call in flight per pass.
async fn within_daily_share(scope: &JobScope<'_>, account_id: Uuid) -> Result<(), PairError> {
    let share = enrichment::daily_share_micro_usd(ER_DAILY_SHARE_ENV, DEFAULT_ER_DAILY_SHARE);
    let unavailable = |e: String| {
        tracing::warn!(error = %e, %account_id, "entity-resolution: daily dollar share unreadable");
        PairError::Stop(Stop::Unavailable(crate::jobs::queue::MANAGED_UNAVAILABLE.into()))
    };
    let mut tx = begin_scope(scope).await.map_err(|e| unavailable(e.to_string()))?;
    let spent = enrichment::spend_today_micro_usd(&mut tx, account_id, None, INPUT_FACTS)
        .await
        .map_err(|e| unavailable(e.to_string()))?;
    tx.rollback().await.ok();
    if spent >= share {
        tracing::info!(%account_id, spent_micro_usd = spent, share_micro_usd = share,
            "entity-resolution: daily dollar share reached; the graph waits for the next UTC day");
        return Err(PairError::Stop(Stop::DailyShare));
    }
    Ok(())
}

/// Judged calls the graph may still open today: the configured bound minus
/// the logical calls this UTC day whose receipts bound an `er_facts` input.
async fn judged_pairs_left(scope: &JobScope<'_>, account_id: Uuid, graph_id: Uuid) -> Result<i64, String> {
    let bound = daily_calls_per_graph();
    let mut tx = begin_scope(scope).await.map_err(|e| e.to_string())?;
    let used: i64 = sqlx::query_scalar(
        "SELECT count(DISTINCT r.logical_call) FROM managed_attempt_receipts r \
          WHERE r.account_id=$1 AND r.graph_id=$2 AND r.purpose='managed_enrichment' \
            AND r.period_start=(clock_timestamp() AT TIME ZONE 'UTC')::date \
            AND EXISTS(SELECT 1 FROM managed_enrichment_inputs i WHERE i.receipt_id=r.receipt_id AND i.input_kind=$3)",
    )
    .bind(account_id)
    .bind(graph_id)
    .bind(INPUT_FACTS)
    .fetch_one(&mut *tx)
    .await
    .map_err(|e| e.to_string())?;
    tx.commit().await.map_err(|e| e.to_string())?;
    Ok((bound - used).max(0))
}

/// One plane transaction for this job (account scope on the consumer plane,
/// tenant system scope on an org tenant), the same scope every wrapper in
/// this file opens.
async fn begin_scope<'a>(
    scope: &JobScope<'a>,
) -> Result<sqlx::Transaction<'a, sqlx::Postgres>, StoreError> {
    match scope {
        JobScope::Consumer { pool, account_id } => begin_account_scoped(pool, *account_id).await,
        JobScope::Org(pool) => begin_tenant_system_scoped_without_account(pool).await,
    }
}

/// Same-title candidate pairs for the bounded sweep: entity reefs in this
/// graph that share a case-insensitive title (the copies that differ only by
/// `identity_scope`), each paired with its group's keeper (most corals, then
/// oldest). Groups with the most corals first.
///
/// Every pair the pass would only skip is dropped here, before the `LIMIT`,
/// so the stream advances past it instead of re-reading it every pass:
///   * a live non-positive judgement (settled);
///   * a person's "keep separate" (`body.not_same`, the dismissals the pass
///     filters out);
///   * a guarded reef ([`guarded_nodes`]: identity correction or promotion
///     lineage);
///   * an unresolved identity that the context guard in [`resolve_graph`]
///     leaves unresolved: either side `identity_status = unresolved`, the
///     two do not carry the same known type, and they share no decisive
///     identifier. Ambiguous stays unresolved; it just stops taking a slot.
///
/// Before this, those pairs sat at the head of the mass order for good. On
/// 2026-10-20 they were 101, 93 and 134 of the first 200 in the three
/// oceans, and the rest of the 200 was the only budget the sweep had left.
async fn same_title_pairs(scope: &JobScope<'_>, graph_id: Uuid, limit: i64) -> Result<Vec<(Uuid, Uuid)>, String> {
    let mut tx = begin_scope(scope).await.map_err(|e| e.to_string())?;
    let rows: Vec<(Uuid, Uuid)> = sqlx::query_as(
        "WITH m AS MATERIALIZED ( \
           SELECT node_id, lower(title) AS t, coral_count::bigint AS c, created_at, \
                  COALESCE(body->>'identity_status','')='unresolved' AS unresolved, \
                  COALESCE(entity_type, body->>'entity_type') AS et, \
                  CASE WHEN jsonb_typeof(body->'not_same')='array' THEN body->'not_same' ELSE '[]'::jsonb END AS not_same, \
                  promoted_from_node_id IS NOT NULL AS promoted \
             FROM nodes WHERE graph_id=$1 AND type='entity' \
         ), g AS ( \
           SELECT t, sum(c) AS mass FROM m GROUP BY t HAVING count(*)>1 \
         ), k AS ( \
           SELECT DISTINCT ON (m.t) m.t, m.node_id AS keep, m.unresolved AS k_unresolved, m.et AS k_et, \
                  m.not_same AS k_not_same, m.promoted AS k_promoted \
             FROM m JOIN g USING (t) \
            ORDER BY m.t, m.c DESC, m.created_at, m.node_id \
         ) \
         SELECT k.keep, m.node_id FROM m JOIN k USING (t) JOIN g USING (t) \
          WHERE m.node_id<>k.keep \
            AND NOT EXISTS (SELECT 1 FROM reef_judgement j \
                 WHERE j.graph_id=$1 AND j.annulled_at IS NULL AND j.verdict<>'positive' \
                   AND j.node_a_id=LEAST(k.keep,m.node_id) AND j.node_b_id=GREATEST(k.keep,m.node_id) \
                   AND (j.expires_at IS NULL OR j.expires_at>now())) \
            AND NOT (k.k_not_same ? m.node_id::text OR m.not_same ? k.keep::text) \
            AND NOT (k.k_promoted OR m.promoted) \
            AND NOT EXISTS (SELECT 1 FROM reef_identity_corrections c WHERE c.node_id IN (k.keep, m.node_id)) \
            AND NOT EXISTS (SELECT 1 FROM node_promotions p \
                 WHERE p.source_node_id IN (k.keep, m.node_id) OR p.target_node_id IN (k.keep, m.node_id)) \
            AND NOT EXISTS (SELECT 1 FROM nodes x WHERE x.promoted_from_node_id IN (k.keep, m.node_id)) \
            AND NOT ((k.k_unresolved OR m.unresolved) \
                 AND NOT (k.k_et IS NOT NULL AND m.et IS NOT NULL AND k.k_et = m.et) \
                 AND NOT EXISTS (SELECT 1 FROM reef_identifiers x JOIN reef_identifiers y \
                      ON y.kind=x.kind AND y.value_norm=x.value_norm \
                      WHERE x.node_id=k.keep AND y.node_id=m.node_id AND x.kind=ANY($3))) \
          ORDER BY g.mass DESC, m.c DESC, k.t, m.node_id \
          LIMIT $2",
    )
    .bind(graph_id)
    .bind(limit)
    .bind(DECISIVE_IDENTIFIER_KINDS)
    .fetch_all(&mut *tx)
    .await
    .map_err(|e| e.to_string())?;
    tx.commit().await.map_err(|e| e.to_string())?;
    Ok(rows.into_iter().map(|(a, b)| canonical_pair(a, b)).collect())
}

/// Reefs a judged merge must never touch, the same guards `reef_dedupe`
/// applies: a human identity correction (it cascades with the reef's links)
/// or promotion lineage on either side.
async fn guarded_nodes(scope: &JobScope<'_>, ids: &[Uuid]) -> Result<HashSet<Uuid>, String> {
    if ids.is_empty() {
        return Ok(HashSet::new());
    }
    let mut tx = begin_scope(scope).await.map_err(|e| e.to_string())?;
    let rows: Vec<Uuid> = sqlx::query_scalar(
        "SELECT n.node_id FROM nodes n WHERE n.node_id=ANY($1) AND ( \
           EXISTS(SELECT 1 FROM reef_identity_corrections c WHERE c.node_id=n.node_id) \
           OR EXISTS(SELECT 1 FROM node_promotions p WHERE p.source_node_id=n.node_id OR p.target_node_id=n.node_id) \
           OR n.promoted_from_node_id IS NOT NULL \
           OR EXISTS(SELECT 1 FROM nodes x WHERE x.promoted_from_node_id=n.node_id))",
    )
    .bind(ids)
    .fetch_all(&mut *tx)
    .await
    .map_err(|e| e.to_string())?;
    tx.commit().await.map_err(|e| e.to_string())?;
    Ok(rows.into_iter().collect())
}

/// One candidate reef as the judge saw it.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
struct PairNode {
    node_id: Uuid,
    version: i32,
    r#type: String,
    title: String,
    scope: String,
    owner_id: Option<Uuid>,
    identity_scope: Uuid,
    entity_type: Option<String>,
}

impl PairNode {
    /// `(kind, id, revision, sha256)`: the reef's identity fields. A rename,
    /// rescope or deletion (absent at publish) makes the call stale.
    fn input(&self) -> EnrichmentInput {
        let material = serde_json::json!([
            self.node_id, self.r#type, self.title, self.scope, self.owner_id,
            self.identity_scope, self.entity_type,
        ]);
        EnrichmentInput::new(INPUT_NODE, self.node_id, i64::from(self.version.max(0)), material.to_string().as_bytes())
    }

    /// `topic`, or no recorded type: the conservative merge bar applies.
    fn is_topic_like(&self) -> bool {
        matches!(self.entity_type.as_deref().map(str::trim), None | Some("") | Some("topic") | Some("unknown"))
    }
}

/// The pair's prompt and inputs, read in one transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
struct PairSnapshot {
    a: PairNode,
    b: PairNode,
    prompt: String,
    inputs: Vec<EnrichmentInput>,
}

/// Stable id for a pair's `er_facts` input: the first 16 bytes of
/// `sha256("er_facts:" lo hi)`.
fn facts_input_id(lo: Uuid, hi: Uuid) -> Uuid {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(b"er_facts:");
    h.update(lo.as_bytes());
    h.update(hi.as_bytes());
    let digest = h.finalize();
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    Uuid::from_bytes(bytes)
}

/// Reads both reefs (locked when `lock`) and builds the exact prompt the
/// judge sees, deterministically, so publish can rebuild it and compare.
/// `None` when either reef is gone. Candidate A is the lower node id.
async fn pair_snapshot_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    store: &dyn Store,
    graph_id: Uuid,
    a_id: Uuid,
    b_id: Uuid,
    lock: bool,
) -> Result<Option<PairSnapshot>, StoreError> {
    let (lo, hi) = canonical_pair(a_id, b_id);
    let sql = if lock {
        "SELECT node_id,version,type,title,scope,owner_id,identity_scope,COALESCE(entity_type,body->>'entity_type') AS entity_type \
           FROM nodes WHERE graph_id=$1 AND node_id=ANY($2) ORDER BY node_id FOR UPDATE"
    } else {
        "SELECT node_id,version,type,title,scope,owner_id,identity_scope,COALESCE(entity_type,body->>'entity_type') AS entity_type \
           FROM nodes WHERE graph_id=$1 AND node_id=ANY($2) ORDER BY node_id"
    };
    let rows: Vec<PairNode> = sqlx::query_as(sql).bind(graph_id).bind([lo, hi].as_slice()).fetch_all(&mut **tx).await?;
    let [a, b]: [PairNode; 2] = match rows.try_into() {
        Ok(pair) => pair,
        Err(_) => return Ok(None),
    };
    let mut identifiers = store.list_reef_identifiers_for_nodes_scoped(tx, &[lo, hi]).await?;
    identifiers.sort_by(|x, y| (x.node_id, &x.kind, &x.value_norm).cmp(&(y.node_id, &y.kind, &y.value_norm)));
    let a_ids: Vec<&crate::store::ReefIdentifier> = identifiers.iter().filter(|i| i.node_id == lo).collect();
    let b_ids: Vec<&crate::store::ReefIdentifier> = identifiers.iter().filter(|i| i.node_id == hi).collect();
    let evidence = identifier_evidence(&a_ids, &b_ids);
    let a_block = candidate_block_tx(tx, graph_id, &a, "A").await?;
    let b_block = candidate_block_tx(tx, graph_id, &b, "B").await?;
    let mut prompt = format!("{a_block}\n{b_block}");
    if !evidence.facts.is_empty() {
        prompt.push_str("\n## Identifier evidence\n");
        for f in &evidence.facts {
            prompt.push_str("  - ");
            prompt.push_str(f);
            prompt.push('\n');
        }
    }
    let facts_material = format!("{RESOLVE_PROMPT_VERSION}\n{prompt}");
    let inputs = vec![
        a.input(),
        b.input(),
        EnrichmentInput::new(INPUT_FACTS, facts_input_id(lo, hi), 0, facts_material.as_bytes()),
    ];
    Ok(Some(PairSnapshot { a, b, prompt, inputs }))
}

/// `## Candidate <label>` for one reef: its recorded type, then up to
/// [`MAX_FACTS_PER_SIDE`] facts in a fixed order (relationship edges, the
/// reef's extracted statements from permitted sources, then permitted coral
/// snippets), each list ordered newest first with an id tie-break.
async fn candidate_block_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    graph_id: Uuid,
    node: &PairNode,
    label: &str,
) -> Result<String, StoreError> {
    let cap = MAX_FACTS_PER_SIDE as i64;
    let mut facts: Vec<String> = Vec::new();
    let edges: Vec<(String, bool, Option<String>)> = sqlx::query_as(
        "SELECT e.predicate, e.src_node_id=$2, o.title FROM relationship_edge e \
           LEFT JOIN nodes o ON o.node_id = CASE WHEN e.src_node_id=$2 THEN e.dst_node_id ELSE e.src_node_id END \
          WHERE e.graph_id=$1 AND (e.src_node_id=$2 OR e.dst_node_id=$2) AND e.predicate <> 'co_occurs_with' \
          ORDER BY e.created_at DESC, e.edge_id LIMIT 3",
    )
    .bind(graph_id)
    .bind(node.node_id)
    .fetch_all(&mut **tx)
    .await?;
    for (predicate, outgoing, other) in edges {
        let other = other.unwrap_or_else(|| "(unknown reef)".into());
        facts.push(if outgoing {
            format!("{predicate} {other} (relationship)")
        } else {
            format!("{other} {predicate} this (relationship)")
        });
    }
    let statements: Vec<String> = sqlx::query_scalar(
        "SELECT a.statement FROM node_semantic_atoms l \
           JOIN semantic_atoms a ON a.semantic_atom_id=l.semantic_atom_id AND a.graph_id=l.graph_id \
           JOIN source_versions v ON v.version_id=a.source_version_id AND v.graph_id=a.graph_id \
           JOIN source_items i ON i.source_item_id=v.source_item_id AND i.graph_id=v.graph_id AND i.access_state='permitted' \
          WHERE l.node_id=$1 AND l.graph_id=$2 \
          ORDER BY a.created_at DESC, a.semantic_atom_id LIMIT $3",
    )
    .bind(node.node_id)
    .bind(graph_id)
    .bind(cap - facts.len() as i64)
    .fetch_all(&mut **tx)
    .await?;
    facts.extend(statements.iter().map(|s| snippet(s)).filter(|s| !s.is_empty()));
    let remaining = cap - facts.len() as i64;
    if remaining > 0 {
        let corals: Vec<(String, i32)> = sqlx::query_as(
            "SELECT a.content_inline, COALESCE(a.header_len,0) FROM node_atoms na \
               JOIN atoms a ON a.atom_id=na.atom_id \
              WHERE na.node_id=$1 AND a.content_inline IS NOT NULL AND library_atom_permitted(a.graph_id,a.atom_id) \
              ORDER BY a.timestamp DESC NULLS LAST, a.atom_id LIMIT $2",
        )
        .bind(node.node_id)
        .bind(remaining)
        .fetch_all(&mut **tx)
        .await?;
        for (content, header_len) in corals {
            let text = snippet(crate::store::strip_header_prefix(&content, header_len));
            if !text.is_empty() {
                facts.push(text);
            }
        }
    }
    let entity_type = node.entity_type.as_deref().map(str::trim).filter(|t| !t.is_empty()).unwrap_or("unknown");
    Ok(format_candidate_block(label, &node.title, entity_type, None, &facts))
}

/// Pre-call read: the snapshot outside any lock (publish re-reads it locked).
async fn read_pair(
    scope: &JobScope<'_>,
    store: &dyn Store,
    graph_id: Uuid,
    a_id: Uuid,
    b_id: Uuid,
) -> Result<Option<PairSnapshot>, String> {
    let mut tx = begin_scope(scope).await.map_err(|e| e.to_string())?;
    let snapshot = pair_snapshot_tx(&mut tx, store, graph_id, a_id, b_id, false)
        .await
        .map_err(|e| e.to_string())?;
    tx.rollback().await.map_err(|e| e.to_string())?;
    Ok(snapshot)
}

/// What a judged pair produced.
#[derive(Debug, Clone, PartialEq)]
enum Decision {
    /// `same` above the merge bar: merge `dup` into `keep`.
    Merge { keep: Uuid, dup: Uuid },
    /// `different`: a NEGATIVE judgement (plus the legacy dismissal).
    Different,
    /// Unsure band, consequential and under the per-graph cap: a live UNSURE
    /// row for the human review queue.
    Review,
    /// Unsure, low confidence, or `same` below the bar: not merged, recorded
    /// as an llm NEGATIVE (`recorded_as: unsure`) so the pair is not re-spent
    /// on every pass; a prompt bump reopens it (`gate_pair`).
    NotMerged,
}

/// Why one pair produced nothing this pass.
enum PairError {
    /// Skip just this pair (bad model output, vanished reef).
    Skip(String),
    /// Stop the pass (hold, cap, refusal, managed storage unavailable).
    Stop(Stop),
    /// Fail the job so the queue retries with backoff (transient provider).
    Transient(String),
}

/// A judged verdict and the managed call that produced it.
struct Judged {
    verdict: Verdict,
    model_id: String,
    work: EnrichmentWork,
}

fn managed_error(e: LlmError) -> PairError {
    match e {
        LlmError::Managed(Deny::Hold(code)) => PairError::Stop(Stop::Hold(code)),
        LlmError::Managed(Deny::Refuse(code)) => PairError::Stop(Stop::Unavailable(code.code().to_owned())),
        LlmError::Managed(Deny::Unavailable) => PairError::Stop(Stop::Unavailable(crate::jobs::queue::MANAGED_UNAVAILABLE.into())),
        e if e.is_transient() => PairError::Transient(format!("transient LLM error during adjudication: {e}")),
        e => PairError::Skip(e.to_string()),
    }
}

/// One managed call per verdict. The Jev judge seat when `JEV_JUDGE_MODE` is
/// active and `allow_jev` (live: its answer is the verdict; shadow: logged,
/// then the LLM decides), else the LLM verdict on `model`. Every call is its
/// own `EnrichmentWork` (own logical call and receipts). A hold never falls
/// back into a second paid call.
#[allow(clippy::too_many_arguments)]
async fn adjudicate_managed(
    scope: &JobScope<'_>,
    managed: &mut Managed<'_>,
    llm: &dyn LlmProvider,
    graph_id: Uuid,
    job_id: Uuid,
    snapshot: &PairSnapshot,
    model: &str,
    allow_jev: bool,
) -> Result<Judged, PairError> {
    let pool: &PgPool = match scope {
        JobScope::Consumer { pool, .. } => pool,
        JobScope::Org(pool) => pool,
    };
    let usage = crate::llm::usage::UsageContext::new(pool.clone(), scope.usage_account_id(), Some(graph_id))
        .with_source_job(job_id);
    let judge_mode = crate::decision::jev_judge_mode();
    // Jev's seat on the managed wire speaks OpenRouter; a TypeSafe-configured
    // process has no priced model for it, so the LLM verdict decides there.
    let jev_seat = allow_jev
        && judge_mode.is_active()
        && crate::decision::jev_provider_kind() != Some(crate::decision::JevProviderKind::TypeSafe);
    if jev_seat {
        if managed.pairs_left <= 0 {
            return Err(PairError::Stop(Stop::DailyPairs));
        }
        within_daily_share(scope, managed.account_id).await?;
        let work = new_work(managed, graph_id, snapshot)?;
        let principal = principal(scope, managed, &work).await?;
        managed.pairs_left -= 1;
        let req = crate::decision::judge::resolve_pair_request(&snapshot.prompt);
        let call = managed_call(scope, managed, principal, &work);
        match crate::decision::openrouter::evaluate_managed_enrichment(llm, &req, &usage, call).await {
            Ok(resp) => {
                let (verdict, confidence, served) = crate::decision::judge::resolve_pair_verdict(&resp);
                tracing::info!(mode = judge_mode.as_str(), verdict = %verdict, confidence,
                    "entity_resolution: Jev Choice (managed_enrichment)");
                if judge_mode.is_live() {
                    return Ok(Judged { verdict: Verdict { verdict, confidence, reason: "jev".into() }, model_id: served, work });
                }
            }
            Err(LlmError::Managed(deny @ (Deny::Hold(_) | Deny::Refuse(_)))) => {
                return Err(managed_error(LlmError::Managed(deny)));
            }
            Err(e) => tracing::warn!(error = %e, "entity_resolution: Jev failed — metered LLM fallback"),
        }
    }
    if managed.pairs_left <= 0 {
        return Err(PairError::Stop(Stop::DailyPairs));
    }
    within_daily_share(scope, managed.account_id).await?;
    let work = new_work(managed, graph_id, snapshot)?;
    let principal = principal(scope, managed, &work).await?;
    managed.pairs_left -= 1;
    let request = CompletionRequest {
        messages: vec![Message { role: Role::User, content: snapshot.prompt.clone() }],
        model: model.to_string(),
        max_tokens: model_config::resolve_max_tokens(),
        system: Some(SYSTEM_PROMPT.to_string()),
        tool_schema: Some(verdict_tool_schema()),
        effort: None,
        reasoning: crate::llm::ReasoningSetting::Off,
    };
    let call = managed_call(scope, managed, principal, &work);
    match crate::llm::usage::metered_complete_managed(
        llm,
        request,
        &usage,
        "entity_resolution",
        call,
        false,
        |response| parse_verdict(&response.content),
    )
    .await
    {
        Ok((response, Ok(verdict))) => Ok(Judged { verdict, model_id: response.model, work }),
        // A parse failure of a 200 body is bad model output, not transient.
        Ok((_, Err(message))) => Err(PairError::Skip(message)),
        Err(e) => Err(managed_error(e)),
    }
}

fn new_work(managed: &Managed<'_>, graph_id: Uuid, snapshot: &PairSnapshot) -> Result<EnrichmentWork, PairError> {
    EnrichmentWork::new(managed.account_id, graph_id, Some(managed.job.clone()), None, snapshot.inputs.clone())
        .map_err(|e| PairError::Skip(e.to_string()))
}

fn managed_call<'a>(
    scope: &'a JobScope<'a>,
    managed: &Managed<'a>,
    principal: Uuid,
    work: &'a EnrichmentWork,
) -> crate::managed::ManagedCall<'a> {
    crate::managed::ManagedCall {
        plane: scope,
        control: managed.control,
        tenant: managed.tenant,
        account_id: managed.account_id,
        principal,
        fallback_model: None,
        subject: crate::managed::Subject::Enrichment { work },
    }
}

async fn principal(scope: &JobScope<'_>, managed: &mut Managed<'_>, work: &EnrichmentWork) -> Result<Uuid, PairError> {
    if let Some(principal) = managed.principal {
        return Ok(principal);
    }
    match enrichment::select_principal(scope, managed.control, managed.tenant, work).await {
        Ok(principal) => {
            managed.principal = Some(principal);
            Ok(principal)
        }
        Err(Deny::Hold(code)) => Err(PairError::Stop(Stop::Hold(code))),
        Err(_) => Err(PairError::Stop(Stop::Unavailable("principal_unavailable".into()))),
    }
}

/// The class a verdict falls in, before the UNSURE review gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Class {
    Merge,
    Different,
    /// `floor <= confidence < bar` and not `different`: the review band.
    UnsureBand,
    NotMerged,
}

/// Merges only on `same` at or above the merge bar ([`TOPIC_MERGE_CONFIDENCE`]
/// when either side is topic-like) and only when the two recorded types do
/// not contradict. `different` keeps the pair apart. Everything else is not
/// merged: the review band may become an UNSURE row, the rest an llm
/// NEGATIVE recorded as unsure.
fn classify(verdict: &Verdict, a: &PairNode, b: &PairNode, threshold: f64) -> Class {
    let bar = if a.is_topic_like() || b.is_topic_like() { threshold.max(TOPIC_MERGE_CONFIDENCE) } else { threshold };
    let types_conflict = !a.is_topic_like()
        && !b.is_topic_like()
        && a.entity_type.as_deref().map(str::trim) != b.entity_type.as_deref().map(str::trim);
    match verdict.verdict.as_str() {
        "same" if verdict.confidence >= bar && !types_conflict => Class::Merge,
        "different" => Class::Different,
        _ => match band_decision(verdict.confidence, review_policy::UNCERTAINTY_FLOOR, bar) {
            ConfidenceBand::Unsure => Class::UnsureBand,
            ConfidenceBand::Auto | ConfidenceBand::TooLowToAct => Class::NotMerged,
        },
    }
}

/// Publish one judged pair: re-read the inputs under row locks, verify them
/// against the call's receipt, and write the decision's outputs only when
/// they are unchanged, marking the inputs written in the same transaction.
#[allow(clippy::too_many_arguments)]
async fn publish(
    scope: &JobScope<'_>,
    store: &dyn Store,
    graph_id: Uuid,
    job_id: Uuid,
    judged: &Judged,
    decision: &Decision,
    mode: JudgementWriteMode,
    raw_verdict: &str,
) -> Result<Published, String> {
    let work = &judged.work;
    let (lo, hi) = (work.inputs()[0].id, work.inputs()[1].id);
    let mut tx = begin_scope(scope).await.map_err(|e| e.to_string())?;
    let current = pair_snapshot_tx(&mut tx, store, graph_id, lo, hi, true)
        .await
        .map_err(|e| e.to_string())?
        .map(|s| s.inputs)
        .unwrap_or_default();
    let verified = match enrichment::verify_inputs_locked(&mut tx, work, &current).await {
        Ok(InputCheck::Fresh(verified)) => verified,
        Ok(InputCheck::Stale) => {
            tx.commit().await.map_err(|e| e.to_string())?;
            tracing::info!(job_id = %job_id, %graph_id, a = %lo, b = %hi,
                "entity-resolution: inputs changed during the call — published nothing (discarded_stale)");
            return Ok(Published::Stale);
        }
        Ok(InputCheck::AlreadyWritten) => {
            tx.rollback().await.map_err(|e| e.to_string())?;
            return Ok(Published::AlreadyWritten);
        }
        Err(deny) => {
            tx.rollback().await.map_err(|e| e.to_string())?;
            return Ok(Published::Denied(deny));
        }
    };
    let model_id = Some(judged.model_id.clone());
    let prompt_version = Some(RESOLVE_PROMPT_VERSION.to_string());
    let receipt = verified.receipt();
    let judgement = |verdict, evidence: serde_json::Value, expires_at| NewJudgement {
        graph_id,
        node_a_id: lo,
        node_b_id: hi,
        verdict,
        source: JudgementSource::Llm,
        confidence: Some(judged.verdict.confidence),
        model_id: model_id.clone(),
        prompt_version: prompt_version.clone(),
        evidence,
        created_by: None,
        expires_at,
    };
    let reason = judged.verdict.reason.clone();
    let mut merged = None;
    let written: Result<(), StoreError> = async {
        match (decision, mode) {
            (Decision::Merge { keep, dup }, JudgementWriteMode::On) => {
                let new = judgement(JudgementVerdict::Positive,
                    serde_json::json!({"llm_verdict": "same", "reason": reason, "managed_receipt": receipt}), None);
                match store.write_judgement_scoped(&mut tx, new).await {
                    Ok(id) => {
                        let moved = store.merge_nodes_with_judgement_scoped(&mut tx, *keep, *dup, id).await?;
                        merged = Some((*keep, *dup, moved, Some(id)));
                    }
                    Err(StoreError::HumanJudgementExists { judgement_id }) => tracing::info!(job_id = %job_id,
                        %graph_id, %judgement_id, "entity-resolution: a human judgement already exists for this pair — not merged (human wins)"),
                    Err(e) => return Err(e),
                }
            }
            (Decision::Merge { keep, dup }, _) => {
                let moved = store.merge_nodes_scoped(&mut tx, *keep, *dup).await?;
                merged = Some((*keep, *dup, moved, None));
            }
            (Decision::Different, JudgementWriteMode::On) => {
                let new = judgement(JudgementVerdict::Negative,
                    serde_json::json!({"llm_verdict": "different", "reason": reason, "managed_receipt": receipt}), None);
                match store.write_judgement_scoped(&mut tx, new).await {
                    Ok(_) => store.insert_merge_dismissal_scoped(&mut tx, graph_id, lo, hi).await?,
                    Err(StoreError::HumanJudgementExists { .. }) => {}
                    Err(e) => return Err(e),
                }
            }
            (Decision::Different, _) => store.insert_merge_dismissal_scoped(&mut tx, graph_id, lo, hi).await?,
            (Decision::Review, JudgementWriteMode::On) => {
                let new = judgement(JudgementVerdict::Unsure,
                    serde_json::json!({"llm_verdict": raw_verdict, "reason": reason, "managed_receipt": receipt}),
                    Some(chrono::Utc::now() + unsure_judgement_ttl(model_config::unsure_dismissal_ttl_days())));
                match store.write_judgement_scoped(&mut tx, new).await {
                    Ok(_) | Err(StoreError::HumanJudgementExists { .. }) => {}
                    Err(e) => return Err(e),
                }
            }
            (Decision::NotMerged, JudgementWriteMode::On) => {
                let new = judgement(JudgementVerdict::Negative,
                    serde_json::json!({"llm_verdict": raw_verdict, "reason": reason, "recorded_as": "unsure",
                        "managed_receipt": receipt}), None);
                match store.write_judgement_scoped(&mut tx, new).await {
                    Ok(_) | Err(StoreError::HumanJudgementExists { .. }) => {}
                    Err(e) => return Err(e),
                }
            }
            // Off / Log never write judgements; an unsure pair writes nothing.
            (Decision::Review | Decision::NotMerged, _) => {}
        }
        Ok(())
    }
    .await;
    if let Err(e) = written {
        tx.rollback().await.ok();
        return Err(format!("entity-resolution publish failed: {e}"));
    }
    if let Err(deny) = verified.mark_written(&mut tx).await {
        tx.rollback().await.ok();
        return Ok(Published::Denied(deny));
    }
    tx.commit().await.map_err(|e| e.to_string())?;
    Ok(match merged {
        Some((keep, dup, moved, judgement_id)) => Published::Merged { keep, dup, moved, judgement_id },
        None => Published::Written,
    })
}

#[derive(Debug)]
enum Published {
    Merged { keep: Uuid, dup: Uuid, moved: u64, judgement_id: Option<Uuid> },
    Written,
    Stale,
    AlreadyWritten,
    Denied(Deny),
}

// ── Scope-aware Store wrappers (org-tenancy Phase W.2) ────────────────────────
//
// Each wrapper runs ONE of this worker's RLS-covered `Store` calls under the
// job's [`JobScope`]: consumer plane ⇒ the `_scoped` variant inside a fresh
// `begin_account_scoped` transaction (committed on success); org tenant ⇒
// the same `_scoped` variant inside a fresh
// `begin_tenant_system_scoped_without_account` transaction. NEITHER arm is
// ambient — a new wrapper that runs the plain method against the tenant pool
// re-opens the gap this module's doc describes. Per-statement rather
// than per-job — see [`JobScope`]'s doc (the adjudication loop interleaves
// DB statements with LLM calls).

async fn scoped_list_merge_dismissals(
    scope: &JobScope<'_>,
    store: &dyn Store,
    graph_id: Uuid,
) -> Result<Vec<(Uuid, Uuid)>, StoreError> {
    match scope {
        JobScope::Consumer { pool, account_id } => {
            let mut tx = begin_account_scoped(pool, *account_id).await?;
            let out = store.list_merge_dismissals_scoped(&mut tx, graph_id).await?;
            tx.commit().await?;
            Ok(out)
        }
        JobScope::Org(pool) => {
            let mut tx = begin_tenant_system_scoped_without_account(pool).await?;
            let out = store.list_merge_dismissals_scoped(&mut tx, graph_id).await?;
            tx.commit().await?;
            Ok(out)
        }
    }
}

/// Keyset-paginated page of entity reefs after `after` (audit "lift the
/// 500-node cap") — the cursor-aware sibling of [`scoped_list_nodes`].
async fn scoped_list_entity_nodes_after(
    scope: &JobScope<'_>,
    store: &dyn Store,
    graph_id: Uuid,
    after: Option<(chrono::DateTime<chrono::Utc>, Uuid)>,
    limit: i64,
) -> Result<Vec<Node>, StoreError> {
    match scope {
        JobScope::Consumer { pool, account_id } => {
            let mut tx = begin_account_scoped(pool, *account_id).await?;
            let out = store.list_entity_nodes_after_scoped(&mut tx, graph_id, after, limit).await?;
            tx.commit().await?;
            Ok(out)
        }
        JobScope::Org(pool) => {
            let mut tx = begin_tenant_system_scoped_without_account(pool).await?;
            let out = store.list_entity_nodes_after_scoped(&mut tx, graph_id, after, limit).await?;
            tx.commit().await?;
            Ok(out)
        }
    }
}

/// Fetch the persisted entity-resolution sweep cursor for a graph.
async fn scoped_get_er_sweep_cursor(
    scope: &JobScope<'_>,
    store: &dyn Store,
    graph_id: Uuid,
) -> Result<Option<(chrono::DateTime<chrono::Utc>, Uuid)>, StoreError> {
    match scope {
        JobScope::Consumer { pool, account_id } => {
            let mut tx = begin_account_scoped(pool, *account_id).await?;
            let out = store.get_er_sweep_cursor_scoped(&mut tx, graph_id).await?;
            tx.commit().await?;
            Ok(out)
        }
        JobScope::Org(pool) => {
            let mut tx = begin_tenant_system_scoped_without_account(pool).await?;
            let out = store.get_er_sweep_cursor_scoped(&mut tx, graph_id).await?;
            tx.commit().await?;
            Ok(out)
        }
    }
}

/// Persist the entity-resolution sweep cursor for a graph. Best-effort at the
/// call site (a failure to advance the cursor never fails the job — the next
/// debounced run simply re-sweeps the same page).
///
/// CAS-guarded (audit "cursor read-then-write race" — two concurrent
/// `EntityResolution` jobs for the same graph are legal under the queue's
/// debounce design, see `jobs::queue`): `expected` is the cursor value THIS
/// call read at sweep start, so the underlying `Store` write only applies if
/// no other concurrent job has already moved it. Returns `Ok(true)` if the
/// write applied, `Ok(false)` if a concurrent sweep won the race (caller logs
/// and does NOT retry).
async fn scoped_set_er_sweep_cursor(
    scope: &JobScope<'_>,
    store: &dyn Store,
    graph_id: Uuid,
    expected: Option<(chrono::DateTime<chrono::Utc>, Uuid)>,
    cursor: Option<(chrono::DateTime<chrono::Utc>, Uuid)>,
) -> Result<bool, StoreError> {
    match scope {
        JobScope::Consumer { pool, account_id } => {
            let mut tx = begin_account_scoped(pool, *account_id).await?;
            let applied = store
                .set_er_sweep_cursor_scoped(&mut tx, graph_id, expected, cursor)
                .await?;
            tx.commit().await?;
            Ok(applied)
        }
        JobScope::Org(pool) => {
            let mut tx = begin_tenant_system_scoped_without_account(pool).await?;
            let out = store.set_er_sweep_cursor_scoped(&mut tx, graph_id, expected, cursor).await?;
            tx.commit().await?;
            Ok(out)
        }
    }
}

async fn scoped_merge_nodes(
    scope: &JobScope<'_>,
    store: &dyn Store,
    keep_id: Uuid,
    dup_id: Uuid,
) -> Result<u64, StoreError> {
    match scope {
        JobScope::Consumer { pool, account_id } => {
            let mut tx = begin_account_scoped(pool, *account_id).await?;
            let moved = store.merge_nodes_scoped(&mut tx, keep_id, dup_id).await?;
            tx.commit().await?;
            Ok(moved)
        }
        JobScope::Org(pool) => {
            let mut tx = begin_tenant_system_scoped_without_account(pool).await?;
            let out = store.merge_nodes_scoped(&mut tx, keep_id, dup_id).await?;
            tx.commit().await?;
            Ok(out)
        }
    }
}

/// Scope-aware sibling of [`Store::list_reef_identifiers_for_nodes`] (T2
/// "identifier awareness"). Best-effort: a lookup failure degrades to "no
/// identifiers" (logged by the caller) rather than failing the job — the
/// worst case is missing near-decisive evidence, not a wrong merge.
async fn scoped_list_reef_identifiers_for_nodes(
    scope: &JobScope<'_>,
    store: &dyn Store,
    node_ids: &[Uuid],
) -> Result<Vec<crate::store::ReefIdentifier>, StoreError> {
    match scope {
        JobScope::Consumer { pool, account_id } => {
            let mut tx = begin_account_scoped(pool, *account_id).await?;
            let out = store.list_reef_identifiers_for_nodes_scoped(&mut tx, node_ids).await?;
            tx.commit().await?;
            Ok(out)
        }
        JobScope::Org(pool) => {
            let mut tx = begin_tenant_system_scoped_without_account(pool).await?;
            let out = store.list_reef_identifiers_for_nodes_scoped(&mut tx, node_ids).await?;
            tx.commit().await?;
            Ok(out)
        }
    }
}

/// Scope-aware sibling of [`Store::get_active_judgement`] — the §5.0 read
/// gate's own lookup, consulted for every candidate pair in On mode.
async fn scoped_get_active_judgement(
    scope: &JobScope<'_>,
    store: &dyn Store,
    graph_id: Uuid,
    a: Uuid,
    b: Uuid,
) -> Result<Option<ReefJudgement>, StoreError> {
    match scope {
        JobScope::Consumer { pool, account_id } => {
            let mut tx = begin_account_scoped(pool, *account_id).await?;
            let out = store.get_active_judgement_scoped(&mut tx, graph_id, a, b).await?;
            tx.commit().await?;
            Ok(out)
        }
        JobScope::Org(pool) => {
            let mut tx = begin_tenant_system_scoped_without_account(pool).await?;
            let out = store.get_active_judgement_scoped(&mut tx, graph_id, a, b).await?;
            tx.commit().await?;
            Ok(out)
        }
    }
}

/// Scope-aware sibling of [`Store::list_active_judgements_for_pairs`] — the
/// BATCH gate lookup that runs BEFORE this sweep applies its per-job cap,
/// so already-settled pairs never consume a budget slot (see
/// [`select_pairs`]).
///
/// CHUNKED at [`JUDGEMENT_GATE_BATCH`] pairs per round trip. The input is
/// this lap's whole candidate list, which is NOT bounded by
/// `MAX_PAIRS_PER_JOB` — that cap is applied after this call, which is the
/// entire point — and is not bounded by `MAX_ENTITY_NODES` either: a dense
/// 500-reef page reaches ~7k pairs, and `BlockingMode::Trgm` derives pairs
/// across the WHOLE graph with no page ceiling at all (~62k on the primary
/// ocean, measured 2026-08-13). Binding that many uuids into one statement
/// would be multi-megabyte parameter payloads, so it is deliberately split
/// into indexed lookups of a fixed size.
async fn scoped_list_active_judgements_for_pairs(
    scope: &JobScope<'_>,
    store: &dyn Store,
    graph_id: Uuid,
    pairs: &[(Uuid, Uuid)],
) -> Result<Vec<ReefJudgement>, StoreError> {
    let mut out: Vec<ReefJudgement> = Vec::new();
    for chunk in pairs.chunks(JUDGEMENT_GATE_BATCH) {
        let rows = match scope {
            JobScope::Consumer { pool, account_id } => {
                let mut tx = begin_account_scoped(pool, *account_id).await?;
                let rows =
                    store.list_active_judgements_for_pairs_scoped(&mut tx, graph_id, chunk).await?;
                tx.commit().await?;
                rows
            }
            JobScope::Org(pool) => {
                let mut tx = begin_tenant_system_scoped_without_account(pool).await?;
                let rows =
                    store.list_active_judgements_for_pairs_scoped(&mut tx, graph_id, chunk).await?;
                tx.commit().await?;
                rows
            }
        };
        out.extend(rows);
    }
    Ok(out)
}

/// Scope-aware sibling of [`Store::write_judgement`].
async fn scoped_write_judgement(
    scope: &JobScope<'_>,
    store: &dyn Store,
    new: NewJudgement,
) -> Result<Uuid, StoreError> {
    match scope {
        JobScope::Consumer { pool, account_id } => {
            let mut tx = begin_account_scoped(pool, *account_id).await?;
            let out = store.write_judgement_scoped(&mut tx, new).await?;
            tx.commit().await?;
            Ok(out)
        }
        JobScope::Org(pool) => {
            let mut tx = begin_tenant_system_scoped_without_account(pool).await?;
            let out = store.write_judgement_scoped(&mut tx, new).await?;
            tx.commit().await?;
            Ok(out)
        }
    }
}

/// Scope-aware sibling of [`Store::annul_judgement`].
async fn scoped_annul_judgement(
    scope: &JobScope<'_>,
    store: &dyn Store,
    judgement_id: Uuid,
    by: Option<Uuid>,
    kind: AnnulKind,
    reason: &str,
) -> Result<crate::store::AnnulOutcome, StoreError> {
    match scope {
        JobScope::Consumer { pool, account_id } => {
            let mut tx = begin_account_scoped(pool, *account_id).await?;
            let out = store.annul_judgement_scoped(&mut tx, judgement_id, by, kind, reason).await?;
            tx.commit().await?;
            Ok(out)
        }
        JobScope::Org(pool) => {
            let mut tx = begin_tenant_system_scoped_without_account(pool).await?;
            let out = store.annul_judgement_scoped(&mut tx, judgement_id, by, kind, reason).await?;
            tx.commit().await?;
            Ok(out)
        }
    }
}

/// Scope-aware sibling of [`Store::merge_nodes_with_judgement`].
async fn scoped_merge_nodes_with_judgement(
    scope: &JobScope<'_>,
    store: &dyn Store,
    keep_id: Uuid,
    dup_id: Uuid,
    judgement_id: Uuid,
) -> Result<u64, StoreError> {
    match scope {
        JobScope::Consumer { pool, account_id } => {
            let mut tx = begin_account_scoped(pool, *account_id).await?;
            let moved =
                store.merge_nodes_with_judgement_scoped(&mut tx, keep_id, dup_id, judgement_id).await?;
            tx.commit().await?;
            Ok(moved)
        }
        JobScope::Org(pool) => {
            let mut tx = begin_tenant_system_scoped_without_account(pool).await?;
            let out = store.merge_nodes_with_judgement_scoped(&mut tx, keep_id, dup_id, judgement_id).await?;
            tx.commit().await?;
            Ok(out)
        }
    }
}

/// Scope-aware sibling of [`Store::list_pending_positive_judgements`] (§5.0
/// candidate-stream union, On mode only).
async fn scoped_list_pending_positive_judgements(
    scope: &JobScope<'_>,
    store: &dyn Store,
    graph_id: Uuid,
) -> Result<PendingPositiveJudgements, StoreError> {
    match scope {
        JobScope::Consumer { pool, account_id } => {
            let mut tx = begin_account_scoped(pool, *account_id).await?;
            let out = store.list_pending_positive_judgements_scoped(&mut tx, graph_id).await?;
            tx.commit().await?;
            Ok(out)
        }
        JobScope::Org(pool) => {
            let mut tx = begin_tenant_system_scoped_without_account(pool).await?;
            let out = store.list_pending_positive_judgements_scoped(&mut tx, graph_id).await?;
            tx.commit().await?;
            Ok(out)
        }
    }
}

/// Scope-aware sibling of [`Store::count_active_unsure_judgements`] (§5.3
/// flood-control cap check).
async fn scoped_count_active_unsure_judgements(
    scope: &JobScope<'_>,
    store: &dyn Store,
    graph_id: Uuid,
) -> Result<i64, StoreError> {
    match scope {
        JobScope::Consumer { pool, account_id } => {
            let mut tx = begin_account_scoped(pool, *account_id).await?;
            let out = store.count_active_unsure_judgements_scoped(&mut tx, graph_id).await?;
            tx.commit().await?;
            Ok(out)
        }
        JobScope::Org(pool) => {
            let mut tx = begin_tenant_system_scoped_without_account(pool).await?;
            let out = store.count_active_unsure_judgements_scoped(&mut tx, graph_id).await?;
            tx.commit().await?;
            Ok(out)
        }
    }
}

/// Scope-aware sibling of [`Store::node_structural_impact`] (§5.3's impact
/// gate; also reused as the Materialize path's pre-merge liveness check —
/// see `resolve_graph`'s doc on that reuse). Best-effort at the CALL site
/// (not here): a lookup failure surfaces as `Err` so callers can apply their
/// own fail-direction rather than this wrapper silently picking one.
async fn scoped_node_structural_impact(
    scope: &JobScope<'_>,
    store: &dyn Store,
    graph_id: Uuid,
    node_ids: &[Uuid],
) -> Result<Vec<review_policy::NodeImpact>, StoreError> {
    match scope {
        JobScope::Consumer { pool, account_id } => {
            let mut tx = begin_account_scoped(pool, *account_id).await?;
            let out = store.node_structural_impact_scoped(&mut tx, graph_id, node_ids).await?;
            tx.commit().await?;
            Ok(out)
        }
        JobScope::Org(pool) => {
            let mut tx = begin_tenant_system_scoped_without_account(pool).await?;
            let out = store.node_structural_impact_scoped(&mut tx, graph_id, node_ids).await?;
            tx.commit().await?;
            Ok(out)
        }
    }
}

// ── Identity-resolution-at-the-door §10.5: provisional seeding, self reef,
// escalation, gauges — raw SQL (no new `Store` trait methods; `store/mod.rs`
// is B1's file, and the ONLY line B3 adds there is `pub mod identity_class;`)
// ─────────────────────────────────────────────────────────────────────────────

/// `graphs.self_node_id` for this graph, if bound. Best-effort: a lookup
/// failure degrades to `None` (no self-reef bump/seed this sweep) rather than
/// failing the job — the self reef is a prioritization/seeding signal here,
/// not a correctness-load-bearing one.
async fn scoped_self_node_id(scope: &JobScope<'_>, graph_id: Uuid) -> Option<Uuid> {
    const SQL: &str = "SELECT self_node_id FROM graphs WHERE id = $1";
    let result: Result<Option<Uuid>, StoreError> = match scope {
        JobScope::Consumer { pool, account_id } => async {
            let mut tx = begin_account_scoped(pool, *account_id).await?;
            let id: Option<Uuid> =
                sqlx::query_scalar::<_, Option<Uuid>>(SQL).bind(graph_id).fetch_optional(&mut *tx).await?.flatten();
            tx.commit().await?;
            Ok(id)
        }
        .await,
        JobScope::Org(pool) => async {
            let mut tx = begin_tenant_system_scoped_without_account(pool).await?;
            let id: Option<Uuid> =
                sqlx::query_scalar::<_, Option<Uuid>>(SQL).bind(graph_id).fetch_optional(&mut *tx).await?.flatten();
            tx.commit().await?;
            Ok(id)
        }
        .await,
    };
    result.unwrap_or_default()
}

/// Which of `ids` (a set of already-LOADED reef ids — see
/// [`resolve_graph`]'s `provisional_ids` local) currently carry
/// `body.provisional = true`. Used only for the [`pair_priority`] bump, so a
/// failure degrades to an empty set (no bump this lap) rather than erroring.
async fn scoped_provisional_node_ids(
    scope: &JobScope<'_>,
    graph_id: Uuid,
    ids: &[Uuid],
) -> Result<HashSet<Uuid>, String> {
    if ids.is_empty() {
        return Ok(HashSet::new());
    }
    const SQL: &str = "SELECT node_id FROM nodes \
                        WHERE graph_id = $1 AND node_id = ANY($2) \
                          AND body @> '{\"provisional\": true}'::jsonb";
    let rows: Vec<Uuid> = match scope {
        JobScope::Consumer { pool, account_id } => {
            let mut tx = begin_account_scoped(pool, *account_id).await.map_err(|e| e.to_string())?;
            let rows =
                sqlx::query_scalar(SQL).bind(graph_id).bind(ids).fetch_all(&mut *tx).await.map_err(|e| e.to_string())?;
            tx.commit().await.map_err(|e| e.to_string())?;
            rows
        }
        JobScope::Org(pool) => {
            let mut tx = begin_tenant_system_scoped_without_account(pool).await.map_err(|e| e.to_string())?;
            let rows =
                sqlx::query_scalar(SQL).bind(graph_id).bind(ids).fetch_all(&mut *tx).await.map_err(|e| e.to_string())?;
            tx.commit().await.map_err(|e| e.to_string())?;
            rows
        }
    };
    Ok(rows.into_iter().collect())
}

/// Row shape for the provisional-seed scan: `(node_id, near_candidates)`,
/// where `near_candidates` is the raw `body.born.near_candidates` jsonb array
/// of node-id STRINGS the door recorded at mint time (§4.4) — `'[]'::jsonb`
/// when absent, so callers never see a SQL NULL.
async fn scoped_context_identity_unresolved(scope:&JobScope<'_>,graph_id:Uuid,ids:&[Uuid])->Result<bool,String> {
    let mut tx=match scope {
        JobScope::Consumer{pool,account_id}=>begin_account_scoped(pool,*account_id).await.map_err(|e|e.to_string())?,
        JobScope::Org(pool)=>begin_tenant_system_scoped_without_account(pool).await.map_err(|e|e.to_string())?,
    };
    // Same-title copies of one known type are extraction's per-version
    // duplicates of one name (the same-title sweep's subject), so the judge
    // may decide them; every other unresolved pair keeps the guard.
    let unresolved:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM nodes WHERE graph_id=$1 AND node_id=ANY($2) AND body->>'identity_status'='unresolved') \
        AND NOT (SELECT count(*)=2 AND count(DISTINCT lower(title))=1 AND count(DISTINCT COALESCE(entity_type,body->>'entity_type'))=1 \
                 AND bool_and(COALESCE(entity_type,body->>'entity_type') IS NOT NULL) FROM nodes WHERE graph_id=$1 AND node_id=ANY($2))")
        .bind(graph_id).bind(ids).fetch_one(&mut *tx).await.map_err(|e|e.to_string())?;
    tx.commit().await.map_err(|e|e.to_string())?;
    Ok(unresolved)
}

/// The pairs of `pairs` (canonical) that [`scoped_context_identity_unresolved`]
/// would hold back and that share no decisive identifier value: one read for
/// the whole candidate list.
async fn scoped_context_unresolved_pairs(
    scope: &JobScope<'_>,
    graph_id: Uuid,
    pairs: &[(Uuid, Uuid)],
) -> Result<HashSet<(Uuid, Uuid)>, String> {
    if pairs.is_empty() {
        return Ok(HashSet::new());
    }
    let (a, b): (Vec<Uuid>, Vec<Uuid>) = pairs.iter().copied().unzip();
    let mut tx = begin_scope(scope).await.map_err(|e| e.to_string())?;
    let rows: Vec<(Uuid, Uuid)> = sqlx::query_as(
        "SELECT p.a, p.b FROM unnest($2::uuid[], $3::uuid[]) AS p(a, b) \
           JOIN nodes na ON na.node_id=p.a AND na.graph_id=$1 \
           JOIN nodes nb ON nb.node_id=p.b AND nb.graph_id=$1 \
          WHERE (COALESCE(na.body->>'identity_status','')='unresolved' OR COALESCE(nb.body->>'identity_status','')='unresolved') \
            AND NOT (lower(na.title)=lower(nb.title) \
                 AND COALESCE(na.entity_type, na.body->>'entity_type') IS NOT NULL \
                 AND COALESCE(na.entity_type, na.body->>'entity_type')=COALESCE(nb.entity_type, nb.body->>'entity_type')) \
            AND NOT EXISTS (SELECT 1 FROM reef_identifiers x JOIN reef_identifiers y ON y.kind=x.kind AND y.value_norm=x.value_norm \
                 WHERE x.node_id=p.a AND y.node_id=p.b AND x.kind=ANY($4))",
    )
    .bind(graph_id)
    .bind(&a)
    .bind(&b)
    .bind(DECISIVE_IDENTIFIER_KINDS)
    .fetch_all(&mut *tx)
    .await
    .map_err(|e| e.to_string())?;
    tx.commit().await.map_err(|e| e.to_string())?;
    Ok(rows.into_iter().map(|(a, b)| canonical_pair(a, b)).collect())
}

async fn scoped_provisional_seed_rows(
    scope: &JobScope<'_>,
    graph_id: Uuid,
) -> Result<Vec<(Uuid, serde_json::Value)>, String> {
    const SQL: &str = "SELECT node_id, COALESCE(body->'born'->'near_candidates', '[]'::jsonb) \
                        FROM nodes \
                        WHERE graph_id = $1 AND type = 'entity' \
                          AND body @> '{\"provisional\": true}'::jsonb \
                        ORDER BY created_at DESC \
                        LIMIT $2";
    match scope {
        JobScope::Consumer { pool, account_id } => {
            let mut tx = begin_account_scoped(pool, *account_id).await.map_err(|e| e.to_string())?;
            let rows = sqlx::query_as(SQL)
                .bind(graph_id)
                .bind(MAX_PROVISIONAL_SEEDS)
                .fetch_all(&mut *tx)
                .await
                .map_err(|e| e.to_string())?;
            tx.commit().await.map_err(|e| e.to_string())?;
            Ok(rows)
        }
        JobScope::Org(pool) => {
            let mut tx = begin_tenant_system_scoped_without_account(pool).await.map_err(|e| e.to_string())?;
            let rows = sqlx::query_as(SQL)
                .bind(graph_id)
                .bind(MAX_PROVISIONAL_SEEDS)
                .fetch_all(&mut *tx)
                .await
                .map_err(|e| e.to_string())?;
            tx.commit().await.map_err(|e| e.to_string())?;
            Ok(rows)
        }
    }
}

/// Parse `body.born.near_candidates` (an array of node-id strings — see the
/// seam's own doc, §10.2) into `Uuid`s, silently skipping anything that isn't
/// a parseable UUID string rather than failing the whole row: this is an
/// optional, best-effort cross-builder field (B1 writes it; B3 only reads
/// it), so a malformed or absent entry costs one fewer seeded pair, never a
/// worker error.
fn parse_near_candidates(v: &serde_json::Value) -> Vec<Uuid> {
    v.as_array()
        .into_iter()
        .flatten()
        .filter_map(|x| x.as_str())
        .filter_map(|s| s.parse::<Uuid>().ok())
        .collect()
}

/// §10.5: provisional reefs pair FIRST against every id in their own
/// `body.born.near_candidates` and against the graph's self reef (if bound),
/// deduplicated against each other (the caller further dedupes against the
/// normal candidate set — see `resolve_graph`'s provisional-seed union).
/// Bounded to [`MAX_PROVISIONAL_SEEDS`] provisional reefs per sweep,
/// newest-first. Best-effort: `Err` only on a hard DB error in the seed scan
/// itself; a malformed `near_candidates` entry is silently skipped (see
/// [`parse_near_candidates`]).
async fn provisional_seed_pairs(
    scope: &JobScope<'_>,
    graph_id: Uuid,
    self_node_id: Option<Uuid>,
) -> Result<Vec<(Uuid, Uuid)>, String> {
    let rows = scoped_provisional_seed_rows(scope, graph_id).await?;
    let mut seen: HashSet<(Uuid, Uuid)> = HashSet::new();
    let mut out = Vec::new();
    for (node_id, near) in rows {
        for cand in parse_near_candidates(&near) {
            if cand == node_id {
                continue;
            }
            let pair = canonical_pair(node_id, cand);
            if seen.insert(pair) {
                out.push(pair);
            }
        }
        if let Some(self_id) = self_node_id {
            if self_id != node_id {
                let pair = canonical_pair(node_id, self_id);
                if seen.insert(pair) {
                    out.push(pair);
                }
            }
        }
    }
    Ok(out)
}

/// §10.5: after a successful merge, the KEEP reef is no longer a
/// freshly-uncertain mint — whether or not it started out provisional — so
/// its `body.provisional` flag is retired. Best-effort: a failure here never
/// fails the job (the reef survives the merge regardless; a stale flag only
/// costs one extra sweep of prioritization noise, never a wrong merge or a
/// lost reef).
async fn clear_provisional_flag(scope: &JobScope<'_>, keep_id: Uuid) {
    const SQL: &str = "UPDATE nodes SET body = body - 'provisional' WHERE node_id = $1";
    let result: Result<(), StoreError> = match scope {
        JobScope::Consumer { pool, account_id } => async {
            let mut tx = begin_account_scoped(pool, *account_id).await?;
            sqlx::query(SQL).bind(keep_id).execute(&mut *tx).await?;
            tx.commit().await?;
            Ok(())
        }
        .await,
        JobScope::Org(pool) => async {
            let mut tx = begin_tenant_system_scoped_without_account(pool).await?;
            sqlx::query(SQL).bind(keep_id).execute(&mut *tx).await?;
            tx.commit().await?;
            Ok(())
        }
        .await,
    };
    if let Err(e) = result {
        tracing::debug!(
            %keep_id, error = %e,
            "entity-resolution: failed to clear body.provisional on a merge's keep reef"
        );
    }
}

/// §10.5 escalation gate: is `(a_id, b_id)` worth retrying on a stronger
/// model before parking it as UNSURE? True when the pair touches the self
/// reef, OR when BOTH endpoints are individually consequential (10.2
/// thresholds: `coral_count >= min_corals` or `degree >= min_degree`). Pure,
/// unit-testable — the caller supplies `impacts` (already fetched for this
/// pair) rather than this function reaching for the DB itself.
fn should_escalate_unsure(
    a_id: Uuid,
    b_id: Uuid,
    self_node_id: Option<Uuid>,
    impacts: &[review_policy::NodeImpact],
    min_corals: i64,
    min_degree: i64,
) -> bool {
    if self_node_id.is_some_and(|id| id == a_id || id == b_id) {
        return true;
    }
    let consequential = |id: Uuid| {
        impacts
            .iter()
            .find(|i| i.node_id == id)
            .is_some_and(|i| i.coral_count >= min_corals || i.degree >= min_degree)
    };
    consequential(a_id) && consequential(b_id)
}

/// §10.5 convergence gauges — computed once at the end of EVERY sweep,
/// including the "nothing to do" early returns (a quiet graph must still
/// report `rebirths_24h = 0`, not go silent), and logged as one structured
/// line. `backlog` is this sweep's own [`PairSelection::deferred`] (always 0
/// on an early-return path, since nothing was deferred). No metrics-emitter
/// crate exists anywhere in this codebase today (verified: no `metrics::`,
/// `counter!`, or `gauge!` call site in `backend/src`) — `tracing::info!` IS
/// the workers' existing observability surface (see this file's own "pair
/// selection" line above), so that is what this emits through; there is
/// nothing else to export to yet. Best-effort per gauge: an individual query
/// failure logs at debug and reports `-1` for JUST that gauge rather than
/// dropping the whole line.
async fn log_convergence_gauges(scope: &JobScope<'_>, graph_id: Uuid, job_id: Uuid, backlog: usize) {
    if !gauges_due(graph_id, std::time::Instant::now()) {
        tracing::debug!(job_id = %job_id, %graph_id, backlog, "entity-resolution: convergence gauges skipped (emitted within the hour)");
        return;
    }
    let rebirths_24h = scoped_rebirths_24h(scope, graph_id).await;
    let twins_24h = scoped_twins_24h(scope, graph_id).await;
    let stale_provisional = scoped_stale_provisional(scope, graph_id).await;
    tracing::info!(
        job_id = %job_id, %graph_id,
        rebirths_24h, twins_24h, backlog, stale_provisional,
        "entity-resolution: identity-door convergence gauges (§10.5)"
    );
}

/// Whether this process should emit the gauges for `graph_id` now (at most
/// once per [`GAUGE_INTERVAL`]); records the emission when it says yes.
fn gauges_due(graph_id: Uuid, now: std::time::Instant) -> bool {
    use std::collections::HashMap;
    use std::sync::{Mutex, OnceLock};
    static LAST: OnceLock<Mutex<HashMap<Uuid, std::time::Instant>>> = OnceLock::new();
    let mut last = LAST.get_or_init(Default::default).lock().unwrap_or_else(|e| e.into_inner());
    match last.get(&graph_id) {
        Some(at) if now.saturating_duration_since(*at) < GAUGE_INTERVAL => false,
        _ => {
            last.insert(graph_id, now);
            true
        }
    }
}

/// Scope-dispatched [`crate::store::identity_class::rebirths_24h`].
async fn scoped_rebirths_24h(scope: &JobScope<'_>, graph_id: Uuid) -> i64 {
    let result: Result<i64, StoreError> = match scope {
        JobScope::Consumer { pool, account_id } => async {
            let mut tx = begin_account_scoped(pool, *account_id).await?;
            let n = crate::store::identity_class::rebirths_24h(&mut tx, graph_id).await?;
            tx.commit().await?;
            Ok(n)
        }
        .await,
        JobScope::Org(pool) => async {
            let mut tx = begin_tenant_system_scoped_without_account(pool).await?;
            let n = crate::store::identity_class::rebirths_24h(&mut tx, graph_id).await?;
            tx.commit().await?;
            Ok(n)
        }
        .await,
    };
    match result {
        Ok(n) => n,
        Err(e) => {
            tracing::debug!(%graph_id, error = %e, "entity-resolution: rebirths_24h gauge query failed");
            -1
        }
    }
}

/// Scope-dispatched [`crate::store::identity_class::twins_24h`].
async fn scoped_twins_24h(scope: &JobScope<'_>, graph_id: Uuid) -> i64 {
    let result: Result<i64, StoreError> = match scope {
        JobScope::Consumer { pool, account_id } => async {
            let mut tx = begin_account_scoped(pool, *account_id).await?;
            let n = crate::store::identity_class::twins_24h(&mut tx, graph_id).await?;
            tx.commit().await?;
            Ok(n)
        }
        .await,
        JobScope::Org(pool) => async {
            let mut tx = begin_tenant_system_scoped_without_account(pool).await?;
            let n = crate::store::identity_class::twins_24h(&mut tx, graph_id).await?;
            tx.commit().await?;
            Ok(n)
        }
        .await,
    };
    match result {
        Ok(n) => n,
        Err(e) => {
            tracing::debug!(%graph_id, error = %e, "entity-resolution: twins_24h gauge query failed");
            -1
        }
    }
}

/// Scope-dispatched [`crate::store::identity_class::stale_provisional`].
async fn scoped_stale_provisional(scope: &JobScope<'_>, graph_id: Uuid) -> i64 {
    let result: Result<i64, StoreError> = match scope {
        JobScope::Consumer { pool, account_id } => async {
            let mut tx = begin_account_scoped(pool, *account_id).await?;
            let n = crate::store::identity_class::stale_provisional(&mut tx, graph_id).await?;
            tx.commit().await?;
            Ok(n)
        }
        .await,
        JobScope::Org(pool) => async {
            let mut tx = begin_tenant_system_scoped_without_account(pool).await?;
            let n = crate::store::identity_class::stale_provisional(&mut tx, graph_id).await?;
            tx.commit().await?;
            Ok(n)
        }
        .await,
    };
    match result {
        Ok(n) => n,
        Err(e) => {
            tracing::debug!(%graph_id, error = %e, "entity-resolution: stale_provisional gauge query failed");
            -1
        }
    }
}

// ── §5.0 read gate + §5.3 confidence banding — pure, unit-testable ──────────

/// Outcome of the §5.0 read gate for one candidate pair. Consulted in On
/// mode ONLY, at the same point the legacy `dismissals` set already is
/// (`resolve_graph`'s per-pair loop, before the identifier-evidence fetch).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GateOutcome {
    /// An active POSITIVE judgement exists — merge via
    /// `merge_nodes_with_judgement`, no LLM call.
    Materialize(Uuid),
    /// Don't re-propose this pair to the LLM this lap.
    Suppress,
    /// Fall through to identifier-evidence + LLM adjudication, unchanged.
    ReAdjudicate,
}

/// Design doc §5.0's decision table, exactly: `dismissed` is checked FIRST
/// (cheap, in-memory, mirrors the legacy dismissals-set precedence); `None`
/// re-adjudicates; POSITIVE materializes regardless of source; NEGATIVE from
/// a human suppresses permanently, from anything else suppresses only until
/// `current_prompt_version` moves past the row's stamped `prompt_version`
/// (a prompt bump reopens it — 0112 precedent); UNSURE suppresses while
/// unexpired.
///
/// The UNSURE arm is DEFENSIVE: [`crate::store::Store::get_active_judgement`]'s
/// own "live" filter already excludes an expired-but-unannulled UNSURE row
/// (returns `None` for it), so in the worker's real call path this function
/// never actually SEES an expired UNSURE via `active`. It is kept anyway —
/// this function takes any row a caller hands it and must get the defensive
/// case right on its own, which is exactly what its unit tests drive
/// directly (a hand-built expired row, bypassing the DB read entirely).
pub(crate) fn gate_pair(
    dismissed: bool,
    active: Option<&ReefJudgement>,
    current_prompt_version: &str,
) -> GateOutcome {
    if dismissed {
        return GateOutcome::Suppress;
    }
    let Some(j) = active else {
        return GateOutcome::ReAdjudicate;
    };
    match j.verdict.as_str() {
        "positive" => GateOutcome::Materialize(j.judgement_id),
        "negative" if j.source == "human" => GateOutcome::Suppress,
        "negative" => {
            if j.prompt_version.as_deref() == Some(current_prompt_version) {
                GateOutcome::Suppress
            } else {
                GateOutcome::ReAdjudicate
            }
        }
        "unsure" => {
            let unexpired = j.expires_at.is_none_or(|e| e > chrono::Utc::now());
            if unexpired {
                GateOutcome::Suppress
            } else {
                GateOutcome::ReAdjudicate
            }
        }
        _ => GateOutcome::ReAdjudicate,
    }
}

/// The three §5.3 confidence bands. `band_decision` never returns `Auto` in
/// practice — the caller only reaches it once the existing `"same" if
/// confidence >= threshold` arm has already been ruled out — but the
/// variant exists so the type names all three bands the doc describes, and
/// so the exact `>= threshold` boundary is independently unit-testable
/// without duplicating the match arm's own condition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ConfidenceBand {
    /// `>= threshold`.
    Auto,
    /// `floor <= confidence < threshold` — the review-queue band, further
    /// gated by structural impact + the per-graph UNSURE cap (see the
    /// caller in `resolve_graph`).
    Unsure,
    /// `< floor` — unchanged debug-log-only, no write, no gate.
    TooLowToAct,
}

/// Pure band lookup. `floor`/`threshold` are parameters rather than read
/// from env/config internally so exact-boundary unit tests can pin them
/// without env coupling; callers pass
/// [`review_policy::UNCERTAINTY_FLOOR`]/[`model_config::merge_confidence_threshold`].
pub(crate) fn band_decision(confidence: f64, floor: f32, threshold: f64) -> ConfidenceBand {
    if confidence >= threshold {
        ConfidenceBand::Auto
    } else if confidence >= f64::from(floor) {
        ConfidenceBand::Unsure
    } else {
        ConfidenceBand::TooLowToAct
    }
}

/// F10 (review round 2): guard the UNSURE judgement's `expires_at` TTL
/// conversion. The founder's Q2 decision STANDS — this deliberately reuses
/// [`model_config::unsure_dismissal_ttl_days`], i.e. the `CONFLICT_`-prefixed
/// `CONFLICT_UNSURE_DISMISSAL_TTL_DAYS` env knob written for the UNRELATED
/// conflict-detection subsystem, not a new dedicated judgement-graph knob
/// (the name mismatch is a known, accepted coupling — do not rename or add a
/// second env var). What this guards: `configured_days` reaching here
/// unguarded could (a) be `<= 0`, silently writing an already-expired
/// judgement — defeating the whole point of the UNSURE band ("park this pair
/// for ~30 days") without any error — or (b) be large enough that
/// `chrono::Duration`'s underlying `TimeDelta` can't represent it, which the
/// panicking `Duration::days` constructor would turn into a worker-task
/// panic on every UNSURE write until an operator noticed. `try_days` reports
/// both cases as `None` instead of panicking; either one falls back to
/// [`model_config::DEFAULT_UNSURE_DISMISSAL_TTL_DAYS`] with a `warn!`, never
/// a panic and never a dead-on-arrival row.
pub(crate) fn unsure_judgement_ttl(configured_days: i64) -> chrono::Duration {
    if configured_days > 0 {
        if let Some(d) = chrono::Duration::try_days(configured_days) {
            return d;
        }
    }
    tracing::warn!(
        configured_days,
        fallback_days = model_config::DEFAULT_UNSURE_DISMISSAL_TTL_DAYS,
        "entity-resolution: CONFLICT_UNSURE_DISMISSAL_TTL_DAYS is <= 0, or too large for \
         chrono::Duration, for the ER UNSURE judgement's expires_at — falling back to the \
         default TTL rather than writing an already-expired row or panicking"
    );
    chrono::Duration::days(model_config::DEFAULT_UNSURE_DISMISSAL_TTL_DAYS)
}

// ── On-mode write helpers (§5.2/§5.3) ────────────────────────────────────────

/// On-mode helper (§5.2): write a judgement then materialize it via
/// `merge_nodes_with_judgement` in the same tick — shared by the identifier
/// auto-merge branch and the "same"-verdict branch (the only difference
/// between the two call sites is `new`'s `source`/`confidence`/`model_id`/
/// `prompt_version`/`evidence`; `write_judgement` normalizes `node_a_id`/
/// `node_b_id` order itself, so callers don't need to canonicalize first).
/// If `write_judgement` fails with anything other than
/// `HumanJudgementExists`, this does NOT fall back to a judgement-less
/// plain merge — the pair is skipped (warn) this lap: a merge whose
/// WHY-record failed to write is the exact hole this slice closes.
#[allow(clippy::too_many_arguments)]
async fn write_and_merge(
    scope: &JobScope<'_>,
    store: &dyn Store,
    job_id: Uuid,
    graph_id: Uuid,
    keep_id: Uuid,
    dup_id: Uuid,
    new: NewJudgement,
    merged_away: &mut HashSet<Uuid>,
    merges: &mut usize,
) {
    match scoped_write_judgement(scope, store, new).await {
        Ok(judgement_id) => {
            match scoped_merge_nodes_with_judgement(scope, store, keep_id, dup_id, judgement_id).await
            {
                Ok(moved) => {
                    merged_away.insert(dup_id);
                    *merges += 1;
                    clear_provisional_flag(scope, keep_id).await;
                    tracing::info!(
                        job_id = %job_id, %graph_id,
                        keep = %keep_id, dup = %dup_id, corals_moved = moved, %judgement_id,
                        "entity-resolution: merged duplicate reef (judgement-graph)"
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        job_id = %job_id, %graph_id, keep = %keep_id, dup = %dup_id, %judgement_id, error = %e,
                        "entity-resolution: materialization failed after judgement write — skipping \
                         (the judgement stays active; the next sweep's pending-positive stream \
                         retries just the merge, no LLM re-spend)"
                    );
                }
            }
        }
        Err(StoreError::HumanJudgementExists { judgement_id }) => {
            tracing::info!(
                job_id = %job_id, %graph_id, keep = %keep_id, dup = %dup_id, %judgement_id,
                "entity-resolution: a human judgement already exists for this pair — skipping (human wins)"
            );
        }
        Err(e) => {
            tracing::warn!(
                job_id = %job_id, %graph_id, keep = %keep_id, dup = %dup_id, error = %e,
                "entity-resolution: judgement write failed — skipping (no judgement-less fallback merge)"
            );
        }
    }
}

/// §5.0 `GateOutcome::Materialize` handler — the crash-recovery / pending-
/// positive materialization path. Re-verifies BOTH endpoints are still live
/// immediately before merging (`scoped_node_structural_impact` reused as a
/// liveness probe: unknown ids are silently absent from its result, so
/// `rows.len() == 2` iff both `keep_id`/`dup_id` are live) — time separates
/// the pending-positive fetch (or an earlier pair in this same loop) from
/// this call, so a dup independently merged/deleted since then is a real,
/// reachable race ("the in-loop race"), not a theoretical one.
///
/// This is why the annul below is gated on an EXPLICIT liveness check,
/// never on `merge_nodes_with_judgement`'s `moved == 0` alone:  `moved == 0`
/// is genuinely ambiguous in the PG impl — it means EITHER "dup was already
/// gone before this call" (no merge performed, no `node_merges` row) OR
/// "dup existed and merged cleanly with zero atoms" (a REAL merge,
/// `node_merges.judgement_id` correctly set to this judgement) — and
/// post-call node liveness can't distinguish the two (dup is absent from
/// `nodes` in both cases, since a successful merge deletes it too).
/// Checking liveness BEFORE the call sidesteps the ambiguity entirely: once
/// confirmed live here, ANY `Ok(_)` from the merge call below — including
/// `Ok(0)` — is treated as a real attempt, same as every other merge branch
/// in this file. If a dup is independently deleted in the (now provably
/// narrow) window between this check and the merge call, the judgement is
/// simply left active; the NEXT sweep's pending-positive fetch
/// (`resolve_graph`'s dead-endpoint bucket) catches it then — self-healing,
/// not lost.
async fn materialize_positive(
    scope: &JobScope<'_>,
    store: &dyn Store,
    job_id: Uuid,
    graph_id: Uuid,
    a: &Node,
    b: &Node,
    judgement_id: Uuid,
    merged_away: &mut HashSet<Uuid>,
    merges: &mut usize,
) {
    let (keep_id, dup_id) = choose_keep_dup(a, b);
    match scoped_node_structural_impact(scope, store, graph_id, &[keep_id, dup_id]).await {
        Ok(rows) if rows.len() == 2 => {
            match scoped_merge_nodes_with_judgement(scope, store, keep_id, dup_id, judgement_id).await
            {
                Ok(moved) => {
                    merged_away.insert(dup_id);
                    *merges += 1;
                    clear_provisional_flag(scope, keep_id).await;
                    tracing::info!(
                        job_id = %job_id, %graph_id, keep = %keep_id, dup = %dup_id,
                        corals_moved = moved, %judgement_id,
                        "entity-resolution: materialized a pending POSITIVE judgement (no LLM call)"
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        job_id = %job_id, %graph_id, keep = %keep_id, dup = %dup_id, %judgement_id, error = %e,
                        "entity-resolution: materialization of a pending POSITIVE failed — skipping, \
                         judgement stays active"
                    );
                }
            }
        }
        Ok(_) => {
            // One (or both) endpoints no longer live — this POSITIVE can
            // never materialize through this pair again (blocking only ever
            // proposes live-live pairs). Annul it as already-materialized/
            // gone, same disposition as the top-of-sweep dead-endpoint sweep.
            match scoped_annul_judgement(
                scope,
                store,
                judgement_id,
                None,
                AnnulKind::System,
                "already-materialized",
            )
            .await
            {
                Ok(_) => {
                    tracing::info!(
                        job_id = %job_id, %graph_id, %judgement_id,
                        "entity-resolution: annulled a pending POSITIVE judgement whose endpoint no \
                         longer resolves (in-loop race — already materialized elsewhere, or \
                         independently deleted)"
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        job_id = %job_id, %graph_id, %judgement_id, error = %e,
                        "entity-resolution: failed to annul a stale POSITIVE judgement — will retry next sweep"
                    );
                }
            }
        }
        Err(e) => {
            // Can't confirm liveness — skip this lap rather than risk
            // annulling a judgement that's actually still pending, or
            // merging blind. Next sweep retries.
            tracing::warn!(
                job_id = %job_id, %graph_id, keep = %keep_id, dup = %dup_id, %judgement_id, error = %e,
                "entity-resolution: liveness check before materialization failed — skipping this lap"
            );
        }
    }
}

/// §5.3's impact + cap gate for the UNSURE band, shared by Log mode's
/// counting and On mode's actual write. On an impact-fetch OR cap-count
/// error, fails toward "don't queue noise" (`AtCap`): this gate guards
/// against WRITING a new review-queue row, so an infra hiccup must not
/// silently create noise nobody asked for. (CR3,
/// CodeRabbit: the impact-fetch error arm used to return `BelowImpact`,
/// contradicting this very doc and underCOUNTing `unsure_cap_hits` in Log
/// mode — fixed to `AtCap` so the doc, the fail direction, and the Log
/// counter all agree.)
enum UnsureGate {
    Eligible,
    BelowImpact,
    AtCap,
}

/// `extra_count` (F11, review round 2): the caller's lap-local count of
/// UNSURE writes already simulated (Log mode) or already committed (On
/// mode, where it's always `0` — see below) earlier in THIS SAME sweep, so
/// the cap check sees `db_count + extra_count` rather than a stale snapshot.
///
/// - `On` mode always passes `0`: each pair's UNSURE write is its own
///   committed `scoped_write_judgement` transaction, so by the time a LATER
///   pair in the same sweep calls this gate, `scoped_count_active_unsure_judgements`
///   already reflects every earlier pair's real write — no simulation
///   needed, the DB count IS live.
/// - `Log` mode never writes anything, so `scoped_count_active_unsure_judgements`
///   returns the SAME snapshot all lap — without `extra_count` the Log
///   volume estimate would report `unsure_cap_hits = 0` no matter how many
///   eligible pairs this lap found (this was review finding #11: the cap
///   simulation never advanced). The caller threads its own running
///   `simulated_unsure_this_lap` counter in as `extra_count`, incrementing
///   it on every gate-passing (`Eligible`) pair.
///
/// F7 cap-neutral supersession: when `db_count + extra_count` is at/over the
/// cap, this pair may still be `Eligible` if it already holds its OWN active
/// UNSURE row (`scoped_pair_has_active_unsure`, expiry ignored) — a fresh
/// write for that exact pair supersedes (annuls-then-inserts) its own row
/// rather than growing the cap, so it must not be dropped as noise. This
/// point read only runs on the at-cap path (one indexed lookup, not one per
/// pair unconditionally).
async fn unsure_impact_and_cap_gate(
    scope: &JobScope<'_>,
    store: &dyn Store,
    job_id: Uuid,
    graph_id: Uuid,
    a_id: Uuid,
    b_id: Uuid,
    extra_count: i64,
) -> UnsureGate {
    let impact_ok = match scoped_node_structural_impact(scope, store, graph_id, &[a_id, b_id]).await {
        Ok(rows) => review_policy::any_high_impact(&rows),
        Err(e) => {
            tracing::warn!(
                job_id = %job_id, %graph_id, a = %a_id, b = %b_id, error = %e,
                "entity-resolution: UNSURE impact lookup failed — skipping this write (fail toward \
                 not queuing noise)"
            );
            return UnsureGate::AtCap;
        }
    };
    if !impact_ok {
        return UnsureGate::BelowImpact;
    }
    let db_count = match scoped_count_active_unsure_judgements(scope, store, graph_id).await {
        Ok(n) => n,
        Err(e) => {
            tracing::warn!(
                job_id = %job_id, %graph_id, error = %e,
                "entity-resolution: UNSURE cap count failed — skipping this write (fail toward not \
                 queuing noise)"
            );
            return UnsureGate::AtCap;
        }
    };
    if db_count + extra_count < MAX_ACTIVE_UNSURE_PER_GRAPH {
        return UnsureGate::Eligible;
    }
    // At/over cap — F7 cap-neutral supersession check (see this fn's doc).
    match scoped_pair_has_active_unsure(scope, graph_id, a_id, b_id).await {
        Ok(true) => UnsureGate::Eligible,
        Ok(false) => UnsureGate::AtCap,
        Err(e) => {
            tracing::warn!(
                job_id = %job_id, %graph_id, a = %a_id, b = %b_id, error = %e,
                "entity-resolution: UNSURE own-row supersession check failed — treating as at-cap \
                 (fail toward not queuing noise)"
            );
            UnsureGate::AtCap
        }
    }
}

/// Point read for the §5.3 UNSURE cap's own-row supersession check (F7,
/// review round 2): does THIS canonical pair already have an active UNSURE
/// row (`annulled_at IS NULL`), regardless of `expires_at`? Deliberately NOT
/// expiry-filtered — `write_judgement_tx`'s own "live" row lookup (step 2,
/// see its doc) isn't either: an expired-but-unannulled row still occupies
/// the pair's one active slot and gets unconditionally annulled-then-
/// replaced, not double-counted, so a new UNSURE write for a pair that
/// already holds its own (possibly expired) UNSURE row is count-neutral —
/// it supersedes, it doesn't grow the cap. No `_scoped` trait variant exists
/// for this read (inline query on the job's scoped transaction):
/// the consumer branch runs the query inline on the scoped transaction; the
/// org-plane branch runs it directly on the tenant pool.
async fn scoped_pair_has_active_unsure(
    scope: &JobScope<'_>,
    graph_id: Uuid,
    a_id: Uuid,
    b_id: Uuid,
) -> Result<bool, StoreError> {
    let (lo, hi) = canonical_pair(a_id, b_id);
    const SQL: &str = "SELECT EXISTS(SELECT 1 FROM reef_judgement \
                        WHERE graph_id = $1 AND node_a_id = $2 AND node_b_id = $3 \
                        AND verdict = 'unsure' AND annulled_at IS NULL)";
    match scope {
        JobScope::Consumer { pool, account_id } => {
            let mut tx = begin_account_scoped(pool, *account_id).await?;
            let exists: bool =
                sqlx::query_scalar(SQL).bind(graph_id).bind(lo).bind(hi).fetch_one(&mut *tx).await?;
            tx.commit().await?;
            Ok(exists)
        }
        JobScope::Org(pool) => {
            let mut tx = begin_tenant_system_scoped_without_account(pool).await?;
            let exists: bool =
                sqlx::query_scalar(SQL).bind(graph_id).bind(lo).bind(hi).fetch_one(&mut *tx).await?;
            tx.commit().await?;
            Ok(exists)
        }
    }
}

/// Core resolution pass: block candidate pairs, adjudicate with the LLM, merge
/// confirmed duplicates, and record "different" verdicts. Returns `Err` only on
/// a hard DB error (which fails the job); LLM/parse errors are soft per-pair.
#[allow(clippy::too_many_arguments)]
async fn resolve_graph(
    scope: &JobScope<'_>,
    store: &dyn crate::store::Store,
    llm: &dyn LlmProvider,
    managed: &mut Managed<'_>,
    graph_id: Uuid,
    job_id: Uuid,
) -> Result<PassReport, String> {
    let mut report = PassReport::default();

    // JUDGEMENT_GRAPH_WRITES — read once per pass (design doc §9). `Off` is
    // byte-identical to pre-PR2 behavior: every block below gated on `mode`
    // is skipped entirely, so an `Off` run makes exactly the same calls in
    // exactly the same order as before this slice.
    let mode = judgement_write_mode();

    // The user's "keep separate" decisions are excluded by both blocking paths.
    let dismissals: HashSet<(Uuid, Uuid)> = scoped_list_merge_dismissals(scope, store, graph_id)
        .await
        .map_err(|e| e.to_string())?
        .into_iter()
        .map(|(a, b)| canonical_pair(a, b))
        .collect();

    // ── §5.0 pending-positive candidate stream (On mode only) ─────────────────
    // Fetched BEFORE blocking so the `BlockingMode::Rust` early-return paths
    // below can tell whether there's still pending-positive work to do on an
    // empty/short page. `pending_pairs` feeds the union below (added to
    // `considered` FIRST — no LLM call, and a review-confirm materializing
    // promptly is the §6 guarantee); `dead_endpoint`/`dead_endpoint_unsure`
    // rows are annulled right here, bounded per sweep by the fetch itself.
    let mut pending_pairs: Vec<(Uuid, Uuid)> = Vec::new();
    let mut pending_node_ids: HashSet<Uuid> = HashSet::new();
    // F6 (review round 2): carries each pending pair's already-fetched
    // `ReefJudgement`, keyed by canonical pair, so the per-pair loop below
    // NEVER re-reads `scoped_get_active_judgement` for a pair this sweep
    // already knows is an active POSITIVE — see the loop's own doc for why
    // a re-read's error path is dangerous here (F6/F8).
    let mut pending_judgements: std::collections::HashMap<(Uuid, Uuid), ReefJudgement> =
        std::collections::HashMap::new();
    if mode == JudgementWriteMode::On {
        match scoped_list_pending_positive_judgements(scope, store, graph_id).await {
            Ok(pending) => {
                // F7: UNSURE rows whose endpoint no longer resolves get the
                // exact same disposition as a dead-endpoint POSITIVE — see
                // `PendingPositiveJudgements::dead_endpoint_unsure`'s doc for
                // why UNSURE otherwise has NO other annul path at all.
                for row in pending.dead_endpoint.iter().chain(pending.dead_endpoint_unsure.iter()) {
                    let reason = if row.verdict == "unsure" { "endpoint-gone" } else { "already-materialized" };
                    match scoped_annul_judgement(
                        scope,
                        store,
                        row.judgement_id,
                        None,
                        AnnulKind::System,
                        reason,
                    )
                    .await
                    {
                        Ok(_) => {
                            tracing::info!(
                                job_id = %job_id, %graph_id, judgement_id = %row.judgement_id,
                                verdict = %row.verdict, reason,
                                "entity-resolution: annulled a stale judgement whose endpoint no \
                                 longer resolves (already materialized elsewhere, or independently \
                                 deleted)"
                            );
                        }
                        Err(e) => {
                            tracing::warn!(
                                job_id = %job_id, %graph_id, judgement_id = %row.judgement_id, error = %e,
                                "entity-resolution: failed to annul a stale judgement — will \
                                 retry next sweep"
                            );
                        }
                    }
                }
                for row in &pending.live {
                    let pair = (row.node_a_id, row.node_b_id);
                    pending_pairs.push(pair);
                    pending_node_ids.insert(row.node_a_id);
                    pending_node_ids.insert(row.node_b_id);
                    pending_judgements.insert(pair, row.clone());
                }
            }
            Err(e) => {
                tracing::warn!(
                    job_id = %job_id, %graph_id, error = %e,
                    "entity-resolution: pending-positive judgement fetch failed — this sweep's \
                     candidate stream is lexical-only"
                );
            }
        }
    }
    let has_pending_positives = !pending_pairs.is_empty();

    // ── Provisional-first seeding (identity-resolution-at-the-door §10.5) ─────
    // Mode-independent (unlike the pending-positive stream above): this is a
    // CANDIDATE-GENERATION improvement, not a judgement-graph rollout
    // feature, so it runs the same whether `JUDGEMENT_GRAPH_WRITES` is
    // Off/Log/On. `self_node_id` doubles as the [`pair_priority`] self-reef
    // bump input below, so it is read once here and threaded through.
    let self_node_id = scoped_self_node_id(scope, graph_id).await;
    let provisional_seeds: Vec<(Uuid, Uuid)> =
        match provisional_seed_pairs(scope, graph_id, self_node_id).await {
            Ok(pairs) => pairs.into_iter().filter(|p| !dismissals.contains(p)).collect(),
            Err(e) => {
                tracing::warn!(
                    job_id = %job_id, %graph_id, error = %e,
                    "entity-resolution: provisional-seed lookup failed — this sweep proceeds \
                     without seeded pairs"
                );
                Vec::new()
            }
        };
    let has_provisional_seeds = !provisional_seeds.is_empty();

    // ── Same-title sweep (PR 5) — MODE-INDEPENDENT ───────────────────────────
    // The per-version duplicate copies (same title, different
    // `identity_scope`) rarely land on the same keyset page, so the lexical
    // page alone would leave most of them unjudged. A bounded, coral-ordered
    // stream of keeper/copy pairs is unioned in every pass; settled pairs drop
    // out in SQL, so the stream advances to the next group.
    let same_title: Vec<(Uuid, Uuid)> = match same_title_pairs(scope, graph_id, SAME_TITLE_PAIRS_PER_JOB).await {
        Ok(pairs) => pairs.into_iter().filter(|p| !dismissals.contains(p)).collect(),
        Err(e) => {
            tracing::warn!(job_id = %job_id, %graph_id, error = %e,
                "entity-resolution: same-title candidate lookup failed — this pass proceeds without it");
            Vec::new()
        }
    };
    let has_same_title = !same_title.is_empty();

    // ── Block candidate pairs (opt-in DB-side trigram path vs default rust) ────
    // `nodes` carries the reefs referenced by `candidates` so the downstream
    // adjudication loop can resolve each id back to a `Node`.
    // Rust-path-only: the keyset cursor advance to persist once this pass
    // completes (see [`Store::list_entity_nodes_after`]'s doc comment for the
    // "lift the 500-node cap" rationale). `None` here means "trgm mode, or
    // nothing to persist"; `Some(next)` (where `next` may itself be `None` to
    // wrap back to the start) is written by [`persist_er_sweep_cursor`] at
    // every exit point of the `BlockingMode::Rust` arm below, INCLUDING its
    // early returns — advancing past a sparse/empty page is exactly how the
    // sweep makes forward progress across debounced job runs.
    let mut cursor_advance: Option<Option<(chrono::DateTime<chrono::Utc>, Uuid)>> = None;
    // The cursor value THIS pass read at sweep start (`after`, below) — CAS
    // guard for the write in `persist_er_sweep_cursor` (audit "cursor
    // read-then-write race": two concurrent `EntityResolution` jobs for the
    // same graph are legal under the queue's debounce design, and without
    // this the slower one's write could clobber the faster one's advance).
    let mut cursor_expected: Option<(chrono::DateTime<chrono::Utc>, Uuid)> = None;

    let (mut nodes, candidates) = match blocking_mode() {
        BlockingMode::Trgm => {
            // DB-side trigram blocking: NOT capped at MAX_ENTITY_NODES — the
            // similarity self-join is paginated until exhausted (audit H5).
            // F9 (review round 2): mirrors the Rust arm's restructure below
            // — an empty lexical candidate list no longer early-returns when
            // pending positives exist, so a crash-window or review-confirmed
            // POSITIVE on an otherwise-quiet trgm graph still materializes
            // promptly instead of waiting on a future lap's lexical
            // candidates to be non-empty. Trgm's own overflow/retry
            // semantics (the page-boundary/pagination logic inside
            // `trgm_candidate_pairs` itself) are untouched — this only
            // changes what happens AFTER that call returns an empty vec.
            let pairs = trgm_candidate_pairs(scope, graph_id, &dismissals).await?;
            if pairs.is_empty() && !has_pending_positives && !has_provisional_seeds && !has_same_title {
                tracing::debug!(job_id = %job_id, %graph_id, "entity-resolution: no candidate duplicate pairs (trgm)");
                log_convergence_gauges(scope, graph_id, job_id, 0).await;
                return Ok(report);
            }
            // Load only the reefs that actually appear in a candidate pair
            // (empty `pairs` + pending-only lap ⇒ empty here; the union
            // block below loads the pending-positive endpoints separately).
            let ids: HashSet<Uuid> = pairs.iter().flat_map(|(a, b)| [*a, *b]).collect();
            let nodes = load_nodes_by_ids(scope, graph_id, &ids).await?;
            (nodes, pairs)
        }
        BlockingMode::Rust => {
            // ── Default path: keyset-paginated sweep over entity reefs ────────
            // (replaces the old fixed-offset-0 / first-500-only page — "lift
            // the 500-node cap"). `after` resumes strictly past whatever this
            // graph's last run left off; a full page advances the cursor to
            // its last row, a short page means the sweep reached the end and
            // wraps back to the start so reefs updated since the last full
            // lap are eventually revisited.
            let after = scoped_get_er_sweep_cursor(scope, store, graph_id)
                .await
                .map_err(|e| e.to_string())?;
            cursor_expected = after;
            let page = scoped_list_entity_nodes_after(scope, store, graph_id, after, MAX_ENTITY_NODES)
                .await
                .map_err(|e| e.to_string())?;
            cursor_advance = Some(next_sweep_cursor(&page));

            if page.len() < 2 {
                if !has_pending_positives && !has_provisional_seeds && !has_same_title {
                    tracing::debug!(job_id = %job_id, %graph_id, count = page.len(), "entity-resolution: <2 entity reefs on this page, nothing to do");
                    persist_er_sweep_cursor(scope, store, graph_id, job_id, cursor_expected, cursor_advance).await;
                    log_convergence_gauges(scope, graph_id, job_id, 0).await;
                    return Ok(report);
                }
                // On mode with pending positives still to process even
                // though this page is too short to pair lexically — fall
                // through with an empty lexical candidate list; the union
                // below still has work. Cursor-persistence semantics are
                // untouched: the SAME `cursor_expected`/`cursor_advance`
                // reach the single `persist_er_sweep_cursor` call at the end
                // of this function either way.
                (page, Vec::new())
            } else {
                let candidates = candidate_pairs(&page, &dismissals);
                if candidates.is_empty() && !has_pending_positives && !has_provisional_seeds && !has_same_title {
                    tracing::debug!(job_id = %job_id, %graph_id, "entity-resolution: no candidate duplicate pairs on this page");
                    persist_er_sweep_cursor(scope, store, graph_id, job_id, cursor_expected, cursor_advance).await;
                    log_convergence_gauges(scope, graph_id, job_id, 0).await;
                    return Ok(report);
                }
                (page, candidates)
            }
        }
    };

    // ── Provisional-seed union (§10.5) — MODE-INDEPENDENT ─────────────────────
    // A provisional reef and its `body.born.near_candidates`/self-reef pair
    // often share NO title token at all (an email-titled mint beside "Alex
    // Alex"), so lexical blocking alone would never propose them. Same
    // missing-`Node`-load discipline as the pending-positive union below.
    let mut considered = candidates;
    if has_provisional_seeds {
        let missing_ids: HashSet<Uuid> = provisional_seeds
            .iter()
            .flat_map(|(a, b)| [*a, *b])
            .filter(|id| find_node(&nodes, *id).is_none())
            .collect();
        if !missing_ids.is_empty() {
            match load_nodes_by_ids(scope, graph_id, &missing_ids).await {
                Ok(extra) => nodes.extend(extra),
                Err(e) => {
                    tracing::warn!(
                        job_id = %job_id, %graph_id, error = %e,
                        "entity-resolution: failed to load Node rows for provisional-seed endpoints \
                         — those pairs will be silently skipped this lap by the loop's own missing-Node guard"
                    );
                }
            }
        }
        let mut seen: HashSet<(Uuid, Uuid)> = HashSet::new();
        let mut unioned = Vec::with_capacity(provisional_seeds.len() + considered.len());
        for pair in &provisional_seeds {
            if seen.insert(*pair) {
                unioned.push(*pair);
            }
        }
        for pair in considered {
            if seen.insert(pair) {
                unioned.push(pair);
            }
        }
        considered = unioned;
    }

    if has_same_title {
        let missing_ids: HashSet<Uuid> = same_title
            .iter()
            .flat_map(|(a, b)| [*a, *b])
            .filter(|id| find_node(&nodes, *id).is_none())
            .collect();
        if !missing_ids.is_empty() {
            match load_nodes_by_ids(scope, graph_id, &missing_ids).await {
                Ok(extra) => nodes.extend(extra),
                Err(e) => tracing::warn!(job_id = %job_id, %graph_id, error = %e,
                    "entity-resolution: failed to load same-title endpoints — those pairs are skipped this lap"),
            }
        }
        let mut seen: HashSet<(Uuid, Uuid)> = considered.iter().map(|(a, b)| canonical_pair(*a, *b)).collect();
        for pair in &same_title {
            if seen.insert(*pair) {
                considered.push(*pair);
            }
        }
    }

    // ── §5.0 union: pending positives FIRST, then lexical candidates ──────────
    // (On mode only.) Dedup via the same canonical_pair discipline the
    // dismissals set already uses; ensure every pending-positive endpoint has
    // a `Node` row loaded (the per-pair loop's `find_node` lookups below need
    // it — the loop continues silently past any pair whose `Node`s aren't in
    // `nodes`, `resolve_graph`'s own long-standing structural convention).
    if mode == JudgementWriteMode::On && has_pending_positives {
        let missing_ids: HashSet<Uuid> = pending_node_ids
            .iter()
            .copied()
            .filter(|id| find_node(&nodes, *id).is_none())
            .collect();
        if !missing_ids.is_empty() {
            match load_nodes_by_ids(scope, graph_id, &missing_ids).await {
                Ok(extra) => nodes.extend(extra),
                Err(e) => {
                    tracing::warn!(
                        job_id = %job_id, %graph_id, error = %e,
                        "entity-resolution: failed to load Node rows for pending-positive endpoints \
                         — those pairs will be silently skipped this lap by the loop's own missing-Node guard"
                    );
                }
            }
        }
        let mut seen: HashSet<(Uuid, Uuid)> = pending_pairs.iter().copied().collect();
        let mut unioned = pending_pairs;
        for pair in considered {
            if seen.insert(pair) {
                unioned.push(pair);
            }
        }
        considered = unioned;
    }

    // ── Convergent selection (replaces `considered.truncate(cap)`) ────────────
    // Consult the read gate in ONE batch read BEFORE the cap is applied, then
    // rank what survives. See [`select_pairs`] for the two defects this fixes
    // and the prod measurements behind them. The per-pair gate inside the loop
    // below is deliberately left in place: it is the correctness authority
    // (it also handles dismissals, the F6 carried-row path, and races against
    // concurrent sweeps), while this batch pass is a BUDGET-ALLOCATION filter.
    // If the batch read fails we simply don't filter — the loop still gates
    // every pair, so a failure costs coverage for one lap, never correctness.
    // reef_dedupe's guards: never judge (or auto-merge) a reef with a human
    // identity correction or promotion lineage. Pending POSITIVEs (a human
    // review confirm, or a crash-window materialization) are left to their
    // own path. A failed guard read fails the job rather than merging blind.
    {
        let ids: Vec<Uuid> = considered.iter().flat_map(|(a, b)| [*a, *b]).collect::<HashSet<_>>().into_iter().collect();
        let guarded = guarded_nodes(scope, &ids).await?;
        if !guarded.is_empty() {
            let pending: HashSet<(Uuid, Uuid)> = pending_judgements.keys().copied().collect();
            considered.retain(|(a, b)| {
                pending.contains(&canonical_pair(*a, *b)) || (!guarded.contains(a) && !guarded.contains(b))
            });
        }
    }
    let candidates_total = considered.len();
    let mut settled_pairs: HashSet<(Uuid, Uuid)> = HashSet::new();
    let mut materialize_first: HashSet<(Uuid, Uuid)> = HashSet::new();
    if mode == JudgementWriteMode::On && !considered.is_empty() {
        match scoped_list_active_judgements_for_pairs(scope, store, graph_id, &considered).await {
            Ok(rows) => {
                for row in &rows {
                    let canon = canonical_pair(row.node_a_id, row.node_b_id);
                    if row.verdict == "positive" {
                        // Active POSITIVEs must reach the loop even when the
                        // pair is ALSO dismissed: F8's conflicting-dismissal
                        // annul is what stops a dismissed pending POSITIVE
                        // from being re-fetched every sweep forever. Marking
                        // it settled here would silently reintroduce exactly
                        // the starvation F8 was written to fix.
                        materialize_first.insert(canon);
                        continue;
                    }
                    let dismissed = dismissals.contains(&canon);
                    if gate_pair(dismissed, Some(row), RESOLVE_PROMPT_VERSION)
                        == GateOutcome::Suppress
                    {
                        settled_pairs.insert(canon);
                    }
                }
            }
            Err(e) => {
                tracing::warn!(
                    job_id = %job_id, %graph_id, error = %e,
                    "entity-resolution: batch gate lookup failed — selecting this lap without \
                     the settled-pair filter (the per-pair gate in the loop still applies, so \
                     this costs coverage for one lap, not correctness)"
                );
            }
        }
    }

    // The context guard in the loop below skips an unresolved-identity pair
    // that is not two copies of one known type and has no decisive
    // identifier in common, and writes nothing, so the same pairs came back
    // every pass and took budget slots ahead of judgeable ones. Drop them
    // before selection. Ambiguous stays unresolved; the loop's own check
    // stays the authority. A failed read selects without the filter.
    {
        let exempt: HashSet<(Uuid, Uuid)> = pending_judgements.keys().copied().chain(materialize_first.iter().copied()).collect();
        let candidates: Vec<(Uuid, Uuid)> = considered
            .iter()
            .map(|(a, b)| canonical_pair(*a, *b))
            .filter(|p| !exempt.contains(p))
            .collect();
        match scoped_context_unresolved_pairs(scope, graph_id, &candidates).await {
            Ok(skip) if !skip.is_empty() => {
                tracing::info!(job_id = %job_id, %graph_id, skipped = skip.len(),
                    "entity-resolution: unresolved-identity pairs left unresolved without taking a slot");
                considered.retain(|(a, b)| !skip.contains(&canonical_pair(*a, *b)));
            }
            Ok(_) => {}
            Err(e) => tracing::warn!(job_id = %job_id, %graph_id, error = %e,
                "entity-resolution: context-guard batch read failed — selecting without it this lap"),
        }
    }

    // §10.5 `pair_priority` bumps: which of the LOADED nodes are provisional
    // right now (best-effort — a lookup failure just costs the bump, not
    // correctness, so it degrades to "none provisional" rather than failing
    // the job).
    let provisional_ids: HashSet<Uuid> = {
        let loaded_ids: Vec<Uuid> = nodes.iter().map(|n| n.node_id).collect();
        match scoped_provisional_node_ids(scope, graph_id, &loaded_ids).await {
            Ok(ids) => ids,
            Err(e) => {
                tracing::warn!(
                    job_id = %job_id, %graph_id, error = %e,
                    "entity-resolution: provisional-flag lookup failed — pair_priority proceeds \
                     without the provisional bump this lap"
                );
                HashSet::new()
            }
        }
    };

    let selection = select_pairs(
        &considered,
        &nodes,
        &settled_pairs,
        &materialize_first,
        &provisional_ids,
        self_node_id,
        MAX_PAIRS_PER_JOB,
    );
    let truncated = selection.deferred > 0;
    let backlog = selection.deferred;
    // Coverage counters, at `info` so a sweep's convergence is observable
    // without turning on debug. `deferred` is the number that matters: it is
    // the candidate backlog this lap could not reach, and it must trend to
    // zero. Nothing measured this before, which is why a sweep could cover
    // 1.3% of its own candidate space for months without anyone noticing.
    tracing::info!(
        job_id = %job_id, %graph_id,
        candidates = candidates_total,
        settled = selection.settled,
        selected = selection.selected.len(),
        deferred = selection.deferred,
        cap = MAX_PAIRS_PER_JOB,
        "entity-resolution: pair selection"
    );
    let considered = selection.selected;

    // `merged_away` tracks reefs deleted by a merge earlier in this run so we
    // don't try to adjudicate or merge against a tombstone.
    let mut merged_away: HashSet<Uuid> = HashSet::new();
    let mut merges = 0usize;
    let mut dismissed = 0usize;
    let merge_threshold = model_config::merge_confidence_threshold();

    // `Log` mode's per-job volume-estimate counters (design doc §5.3) — only
    // ever incremented when `mode == JudgementWriteMode::Log`; logged once at
    // the end of this pass. Meaningless (left at 0, never logged) in `Off`/`On`.
    //
    // F11 (review round 2): `would_be_auto`/`would_be_negative` are counted
    // at the DECISION point (the verdict/threshold match arm), not after a
    // successful merge/dismissal — `would_be_auto` still increments even
    // when the legacy merge attempt in the SAME branch later fails (`Err`
    // arm). This is intentional, not a bug: `On` mode writes the
    // `reef_judgement` row BEFORE attempting the merge (`write_and_merge`),
    // so a judgement row would exist regardless of whether the merge itself
    // succeeds — row volume IS decision-point volume in `On` too, so
    // counting at the same point in `Log` keeps the two comparable. Both are
    // therefore honest upper bounds on what `On` would write, same as
    // `would_be_unsure`'s impact-gate half (a would-be-eligible pair here
    // might still hit `HumanJudgementExists` in real `On` writes). The ONE
    // counter that is NOT an upper bound is `would_be_unsure`/
    // `unsure_cap_hits`: they run the exact same `unsure_impact_and_cap_gate`
    // `On` uses, fed a lap-local `simulated_unsure_this_lap` running count
    // (below) so the simulated cap actually advances as eligible pairs
    // accumulate within one sweep — before this fix, `Log` always read the
    // same never-changing DB count (nothing is ever written in `Log`), so
    // `unsure_cap_hits` stayed 0 and `would_be_unsure` over-reported by
    // however many pairs `On` would actually have capped.
    let started = std::time::Instant::now();
    let budget = pass_budget();
    let selected_total = considered.len();
    let mut cut_short = 0usize;
    let mut would_be_auto = 0usize;
    let mut would_be_negative = 0usize;
    let mut would_be_unsure = 0usize;
    let mut unsure_cap_hits = 0usize;
    let mut simulated_unsure_this_lap: i64 = 0;

    for (index, (a_id, b_id)) in considered.into_iter().enumerate() {
        if started.elapsed() >= budget {
            // Out of wall-clock budget: hand the rest to the next pass.
            cut_short = selected_total - index;
            tracing::info!(job_id = %job_id, %graph_id, remaining = cut_short, budget_secs = budget.as_secs(),
                "entity-resolution: pass budget spent; the rest continues in the next pass");
            break;
        }
        if merged_away.contains(&a_id) || merged_away.contains(&b_id) {
            continue;
        }
        let (Some(a), Some(b)) = (find_node(&nodes, a_id), find_node(&nodes, b_id)) else {
            continue;
        };

        // Contextual homonyms are not model-merge candidates. Only stable
        // identifiers or an explicit human positive review can release them.
        match scoped_context_identity_unresolved(scope,graph_id,&[a_id,b_id]).await {
            Ok(true)=> {
                let identifiers=match scoped_list_reef_identifiers_for_nodes(scope,store,&[a_id,b_id]).await {Ok(v)=>v,Err(_)=>continue};
                let ai:Vec<_>=identifiers.iter().filter(|i|i.node_id==a_id).collect();
                let bi:Vec<_>=identifiers.iter().filter(|i|i.node_id==b_id).collect();
                let stable=identifier_evidence(&ai,&bi).auto_merge;
                let human=matches!(scoped_get_active_judgement(scope,store,graph_id,a_id,b_id).await,Ok(Some(j)) if j.source=="human" && j.verdict=="positive");
                if !stable && !human {continue;}
            },
            Ok(false)=>{},
            Err(_)=>continue,
        }

        // ── §5.0 read gate (On mode only) ──────────────────────────────────
        if mode == JudgementWriteMode::On {
            let canon = canonical_pair(a_id, b_id);
            let dismissed_pair = dismissals.contains(&canon);
            // F6 (review round 2): if this pair came from the pending-
            // positive stream fetched once at the top of this sweep, we
            // ALREADY know it's an active POSITIVE — use that carried row
            // directly and never re-read `scoped_get_active_judgement` for
            // it. Re-reading per-pair (rather than trusting the fetch this
            // sweep already did) is exactly the N+1 surface where a
            // transient error previously degraded to `ReAdjudicate`,
            // sending an already-confirmed pair back to the LLM — a fresh
            // "different" verdict would then supersede the very POSITIVE
            // this sweep is supposed to be materializing.
            let outcome = if let Some(judgement) = pending_judgements.get(&canon) {
                if dismissed_pair {
                    // F8: dismissed-first is still the spec's precedence,
                    // but a pending POSITIVE that conflicts with an
                    // existing dismissal must not become an eternal
                    // Suppress that gets re-fetched into `pending_pairs`
                    // every sweep forever (starving the 50-pair budget —
                    // review finding #8's failure scenario). Annul it
                    // instead: dismissed-first (keep separate) is the
                    // conservative direction, this durably records the
                    // conflict's resolution instead of refetching the same
                    // row indefinitely, and F5 stops new instances of this
                    // state from forming (the "different" arm below only
                    // dismisses when its NEGATIVE write actually landed).
                    match scoped_annul_judgement(
                        scope, store, judgement.judgement_id, None, AnnulKind::System,
                        "conflicting-dismissal",
                    )
                    .await
                    {
                        Ok(_) => {
                            tracing::warn!(
                                job_id = %job_id, %graph_id, a = %a_id, b = %b_id,
                                judgement_id = %judgement.judgement_id,
                                "entity-resolution: annulled a pending POSITIVE judgement that \
                                 conflicts with an existing 'keep separate' dismissal \
                                 (conflicting-dismissal)"
                            );
                        }
                        Err(e) => {
                            tracing::warn!(
                                job_id = %job_id, %graph_id, a = %a_id, b = %b_id,
                                judgement_id = %judgement.judgement_id, error = %e,
                                "entity-resolution: failed to annul a dismissed-but-pending \
                                 POSITIVE judgement — will retry next sweep"
                            );
                        }
                    }
                    continue;
                }
                GateOutcome::Materialize(judgement.judgement_id)
            } else {
                match scoped_get_active_judgement(scope, store, graph_id, a_id, b_id).await {
                    Ok(active) => gate_pair(dismissed_pair, active.as_ref(), RESOLVE_PROMPT_VERSION),
                    Err(e) => {
                        // F6: a transient read error on a LEXICAL pair (one
                        // this sweep has no prior knowledge about) must
                        // never fall through to `ReAdjudicate` — that could
                        // send the pair to the LLM, and a fresh "different"
                        // verdict would then supersede a POSITIVE this
                        // failed read simply couldn't see. Skip the pair
                        // this lap instead; the next sweep re-reads cleanly.
                        tracing::warn!(
                            job_id = %job_id, %graph_id, a = %a_id, b = %b_id, error = %e,
                            "entity-resolution: active-judgement lookup failed — skipping this pair \
                             this lap (never degrade to re-adjudicate on a read error)"
                        );
                        continue;
                    }
                }
            };
            match outcome {
                GateOutcome::Suppress => continue,
                GateOutcome::Materialize(judgement_id) => {
                    materialize_positive(
                        scope, store, job_id, graph_id, a, b, judgement_id, &mut merged_away,
                        &mut merges,
                    )
                    .await;
                    continue;
                }
                GateOutcome::ReAdjudicate => {}
            }
        }

        // T2 "identifier awareness": gather shared/conflicting identifiers
        // BEFORE spending an LLM call — a lookup failure degrades to "no
        // evidence" (falls through to the normal LLM path below) rather than
        // failing the job, since this is a pure enrichment on top of the
        // pre-T2 behavior.
        let identifiers = match scoped_list_reef_identifiers_for_nodes(scope, store, &[a_id, b_id])
            .await
        {
            Ok(ids) => ids,
            Err(e) => {
                tracing::warn!(
                    job_id = %job_id,
                    %graph_id,
                    a = %a_id,
                    b = %b_id,
                    error = %e,
                    "entity-resolution: identifier lookup failed — degrading to no evidence"
                );
                Vec::new()
            }
        };
        let a_ids: Vec<&crate::store::ReefIdentifier> =
            identifiers.iter().filter(|i| i.node_id == a_id).collect();
        let b_ids: Vec<&crate::store::ReefIdentifier> =
            identifiers.iter().filter(|i| i.node_id == b_id).collect();
        let evidence = identifier_evidence(&a_ids, &b_ids);

        if evidence.auto_merge && crate::decision::identifier_auto_merge_enabled() {
            // Near-decisive: a shared email/slack_user/clerk_user identifier
            // with no conflicting identifier of that same kind — skip the
            // LLM call entirely and merge directly.
            // ADR-0013: paused while ingest-door is shadow or live.
            let (keep_id, dup_id) = choose_keep_dup(a, b);
            if mode == JudgementWriteMode::On {
                let new = NewJudgement {
                    graph_id,
                    node_a_id: a_id,
                    node_b_id: b_id,
                    verdict: JudgementVerdict::Positive,
                    source: JudgementSource::IdentifierAuto,
                    confidence: None,
                    model_id: None,
                    prompt_version: None,
                    evidence: serde_json::json!({"identifier": evidence.facts}),
                    created_by: None,
                    expires_at: None,
                };
                write_and_merge(
                    scope, store, job_id, graph_id, keep_id, dup_id, new, &mut merged_away,
                    &mut merges,
                )
                .await;
                continue;
            }
            // Off AND Log share this path unchanged — Log's "today's
            // behavior exactly" contract (design doc §5.3).
            match scoped_merge_nodes(scope, store, keep_id, dup_id).await {
                Ok(moved) => {
                    // Identifier union happens INSIDE `merge_nodes`/
                    // `merge_nodes_scoped` itself now (T2 FIX 1) — no separate
                    // post-commit call needed here.
                    merged_away.insert(dup_id);
                    merges += 1;
                    clear_provisional_flag(scope, keep_id).await;
                    tracing::info!(
                        job_id = %job_id, %graph_id,
                        keep = %keep_id, dup = %dup_id, corals_moved = moved,
                        "entity-resolution: auto-merged duplicate reef on a shared identifier (no LLM call)"
                    );
                }
                Err(e) => {
                    tracing::warn!(job_id = %job_id, %graph_id, keep = %keep_id, dup = %dup_id, error = %e, "entity-resolution: identifier auto-merge failed — skipping");
                }
            }
            if mode == JudgementWriteMode::Log {
                would_be_auto += 1;
            }
            continue;
        }

        // Conflicting decisive identifiers are two identities, whatever a
        // model would say about their titles. Recorded so the pair is not
        // picked again; no call is made.
        if evidence.conflict {
            if mode == JudgementWriteMode::On {
                let new = NewJudgement {
                    graph_id,
                    node_a_id: a_id,
                    node_b_id: b_id,
                    verdict: JudgementVerdict::Negative,
                    source: JudgementSource::IdentifierAuto,
                    confidence: None,
                    model_id: None,
                    prompt_version: None,
                    evidence: serde_json::json!({"identifier_conflict": evidence.facts}),
                    created_by: None,
                    expires_at: None,
                };
                match scoped_write_judgement(scope, store, new).await {
                    Ok(_) | Err(StoreError::HumanJudgementExists { .. }) => {}
                    Err(e) => tracing::warn!(job_id = %job_id, %graph_id, a = %a_id, b = %b_id, error = %e,
                        "entity-resolution: identifier-conflict judgement write failed"),
                }
            }
            continue;
        }

        // ── Judged path: managed_enrichment calls (PR 5) ─────────────────────
        let snapshot = match read_pair(scope, store, graph_id, a_id, b_id).await {
            Ok(Some(snapshot)) => snapshot,
            Ok(None) => continue,
            // DB error gathering evidence is hard — bail so the job retries.
            Err(e) => return Err(e),
        };
        let mut judged = match adjudicate_managed(
            scope, managed, llm, graph_id, job_id, &snapshot, &model_config::resolve_model(), true,
        )
        .await
        {
            Ok(judged) => judged,
            Err(PairError::Skip(e)) => {
                tracing::warn!(job_id = %job_id, %graph_id, a = %a_id, b = %b_id, error = %e,
                    "entity-resolution: adjudication failed — skipping pair");
                continue;
            }
            Err(PairError::Stop(stop)) => {
                report.stop = Some(stop);
                break;
            }
            // Audit S2: a transient provider failure fails the job so the
            // queue re-runs it with back-off.
            Err(PairError::Transient(e)) => return Err(e),
        };
        report.adjudicated += 1;
        let mut class = classify(&judged.verdict, &snapshot.a, &snapshot.b, merge_threshold);
        let mut stop_after: Option<Stop> = None;
        // §10.5: one retry on `RESOLVE_ESCALATE_MODEL` for a review-band
        // verdict on a pair touching the self reef or with two consequential
        // endpoints. Its own managed call; its verdict goes through the same
        // classification.
        if class == Class::UnsureBand {
            if let Some(escalate_model) = model_config::resolve_escalate_model() {
                let impacts = scoped_node_structural_impact(scope, store, graph_id, &[a_id, b_id])
                    .await
                    .unwrap_or_default();
                if should_escalate_unsure(
                    a_id, b_id, self_node_id, &impacts,
                    model_config::door_adjudicate_min_corals(), model_config::door_adjudicate_min_degree(),
                ) {
                    match adjudicate_managed(scope, managed, llm, graph_id, job_id, &snapshot, &escalate_model, false).await {
                        Ok(escalated) => {
                            judged = escalated;
                            report.adjudicated += 1;
                            class = classify(&judged.verdict, &snapshot.a, &snapshot.b, merge_threshold);
                        }
                        Err(PairError::Transient(e)) => return Err(e),
                        Err(PairError::Stop(stop)) => stop_after = Some(stop),
                        Err(PairError::Skip(e)) => tracing::warn!(job_id = %job_id, %graph_id, a = %a_id, b = %b_id,
                            error = %e, "entity-resolution: UNSURE-escalation failed — publishing the primary verdict"),
                    }
                }
            }
        }
        let decision = match class {
            Class::Merge => {
                let (keep, dup) = choose_keep_dup(a, b);
                Decision::Merge { keep, dup }
            }
            Class::Different => Decision::Different,
            Class::NotMerged => Decision::NotMerged,
            Class::UnsureBand => match mode {
                // `extra_count = 0` — see `unsure_impact_and_cap_gate`'s doc.
                JudgementWriteMode::On => match unsure_impact_and_cap_gate(scope, store, job_id, graph_id, a_id, b_id, 0).await {
                    UnsureGate::Eligible => Decision::Review,
                    UnsureGate::BelowImpact | UnsureGate::AtCap => Decision::NotMerged,
                },
                JudgementWriteMode::Log => {
                    match unsure_impact_and_cap_gate(scope, store, job_id, graph_id, a_id, b_id, simulated_unsure_this_lap).await {
                        UnsureGate::Eligible => {
                            would_be_unsure += 1;
                            simulated_unsure_this_lap += 1;
                        }
                        UnsureGate::AtCap => unsure_cap_hits += 1,
                        UnsureGate::BelowImpact => {}
                    }
                    Decision::NotMerged
                }
                JudgementWriteMode::Off => Decision::NotMerged,
            },
        };
        if mode == JudgementWriteMode::Log {
            match decision {
                Decision::Merge { .. } => would_be_auto += 1,
                Decision::Different => would_be_negative += 1,
                Decision::Review | Decision::NotMerged => {}
            }
        }
        let raw_verdict = judged.verdict.verdict.clone();
        match publish(scope, store, graph_id, job_id, &judged, &decision, mode, &raw_verdict).await {
            Ok(Published::Merged { keep, dup, moved, judgement_id }) => {
                merged_away.insert(dup);
                merges += 1;
                clear_provisional_flag(scope, keep).await;
                tracing::info!(
                    job_id = %job_id, %graph_id, keep = %keep, dup = %dup, corals_moved = moved,
                    judgement_id = ?judgement_id, confidence = judged.verdict.confidence,
                    "entity-resolution: merged duplicate reef (managed_enrichment)"
                );
            }
            Ok(Published::Written) => {
                if decision == Decision::Different {
                    dismissed += 1;
                }
            }
            Ok(Published::Stale | Published::AlreadyWritten) => {}
            Ok(Published::Denied(Deny::Hold(code))) => {
                report.stop = Some(Stop::Hold(code));
                break;
            }
            Ok(Published::Denied(deny)) => tracing::warn!(job_id = %job_id, %graph_id, a = %a_id, b = %b_id,
                error = %deny, "entity-resolution: publish verification unavailable — nothing written for this pair"),
            Err(e) => tracing::warn!(job_id = %job_id, %graph_id, a = %a_id, b = %b_id, error = %e,
                "entity-resolution: publish failed — nothing written for this pair"),
        }
        if let Some(stop) = stop_after {
            report.stop = Some(stop);
            break;
        }
    }

    if mode == JudgementWriteMode::Log {
        tracing::info!(
            job_id = %job_id, %graph_id,
            would_be_auto, would_be_negative, would_be_unsure, unsure_cap_hits,
            "entity-resolution[log]: judgement-graph volume estimate for this job \
             (JUDGEMENT_GRAPH_WRITES=log — no reef_judgement writes performed)"
        );
    }

    // The whole pass succeeded (no hard `Err` bailed out above) — safe to
    // advance the sweep cursor now. A hard `Err` earlier (DB error gathering
    // evidence, or a transient LLM failure) returns before this point, so a
    // retry of the same debounced job re-sweeps the identical page rather
    // than skipping past it. A pass stopped by a hold or the daily bound did
    // not reach the rest of its page either, so it keeps the cursor too.
    if report.stop.is_none() && cut_short == 0 {
        persist_er_sweep_cursor(scope, store, graph_id, job_id, cursor_expected, cursor_advance).await;
    }

    log_convergence_gauges(scope, graph_id, job_id, backlog).await;

    tracing::debug!(
        job_id = %job_id, %graph_id,
        merges, dismissed, truncated, adjudicated = report.adjudicated,
        "entity-resolution: pass complete"
    );
    report.merges = merges;
    report.deferred = backlog + cut_short;
    report.cut_short = cut_short > 0;
    Ok(report)
}

/// Best-effort, CAS-guarded persist of the Rust-path keyset sweep cursor
/// (`cursor_advance` is `None` for trgm mode, or `Some(next)` for the default
/// path — see `resolve_graph`'s local variable of the same name).
///
/// `expected` is the cursor value this pass read at sweep start
/// (`resolve_graph`'s `cursor_expected`). The underlying `Store` write is
/// conditioned on the row still holding `expected` (audit "cursor
/// read-then-write race" — two concurrent `EntityResolution` jobs for the
/// same graph are legal under the queue's debounce design, and up to
/// `MAX_PAIRS_PER_JOB` sequential LLM calls separate this pass's read from
/// its write, so a slower concurrent pass could otherwise clobber a faster
/// one's advance, or even roll the cursor backward). A lost CAS race is
/// logged and NOT retried: the "winning" concurrent pass already advanced the
/// cursor correctly, and the next debounced run re-reads whatever it left
/// behind. A hard DB error is also non-fatal here (same as before this
/// guard): the next debounced run simply re-sweeps the same page instead of
/// advancing, which is self-correcting rather than data-losing.
async fn persist_er_sweep_cursor(
    scope: &JobScope<'_>,
    store: &dyn crate::store::Store,
    graph_id: Uuid,
    job_id: Uuid,
    expected: Option<(chrono::DateTime<chrono::Utc>, Uuid)>,
    cursor_advance: Option<Option<(chrono::DateTime<chrono::Utc>, Uuid)>>,
) {
    let Some(next) = cursor_advance else {
        return;
    };
    match scoped_set_er_sweep_cursor(scope, store, graph_id, expected, next).await {
        Ok(true) => {}
        Ok(false) => {
            tracing::info!(
                job_id = %job_id, %graph_id,
                "entity-resolution: sweep cursor CAS lost — a concurrent EntityResolution job for \
                 this graph already advanced the cursor; skipping this pass's write (not retrying)"
            );
        }
        Err(e) => {
            tracing::warn!(
                job_id = %job_id, %graph_id, error = %e,
                "entity-resolution: failed to persist sweep cursor advance — next run will re-sweep this page"
            );
        }
    }
}

/// Pure decision: where should the Rust-path keyset sweep cursor land after
/// fetching one page of entity reefs? Extracted out of `resolve_graph`'s
/// `BlockingMode::Rust` arm so the "lift the 500-node cap" cursor logic is
/// unit-testable without a DB.
///
/// A page whose length equals `MAX_ENTITY_NODES` (a "full page" — the query
/// hit its `LIMIT`) means there may be more reefs after it, so the cursor
/// ADVANCES to `(updated_at, node_id)` of the page's last row, resuming
/// strictly after it on the next debounced run. Anything short of a full page
/// — including an empty page — means the sweep reached the end of the ocean,
/// so the cursor WRAPS back to the start (`None`) rather than advancing, so
/// reefs updated since the last full lap are eventually revisited.
fn next_sweep_cursor(page: &[Node]) -> Option<(chrono::DateTime<chrono::Utc>, Uuid)> {
    let full_page = page.len() as i64 == MAX_ENTITY_NODES;
    if full_page {
        page.last().map(|n| (n.updated_at, n.node_id))
    } else {
        None
    }
}

// ── Blocking: candidate-pair generation ─────────────────────────────────────────

/// Which candidate-blocking strategy the worker uses, selected by `ER_BLOCKING`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BlockingMode {
    /// Default: O(n²) in-Rust blocking over one keyset-paginated page (at most
    /// `MAX_ENTITY_NODES` reefs) per job run, sweeping the whole ocean over
    /// successive debounced jobs via the persisted `graphs.er_sweep_cursor_*`
    /// cursor ("lift the 500-node cap") rather than always re-scanning the
    /// same first page.
    Rust,
    /// Opt-in: DB-side pg_trgm similarity blocking (not capped at 500).
    Trgm,
}

/// Resolve the blocking mode from `ER_BLOCKING`. Unset, empty, or `rust`
/// (case-insensitive) → the default rust path; `trgm` → the trigram path. Any
/// other value falls back to the default so a typo never silently disables ER.
fn blocking_mode() -> BlockingMode {
    match std::env::var("ER_BLOCKING") {
        Ok(v) if v.trim().eq_ignore_ascii_case("trgm") => BlockingMode::Trgm,
        _ => BlockingMode::Rust,
    }
}

/// Trigram similarity threshold for the trgm path, from `ER_TRGM_THRESHOLD`
/// (clamped to (0.0, 1.0]); defaults to [`DEFAULT_TRGM_THRESHOLD`].
fn trgm_threshold() -> f64 {
    std::env::var("ER_TRGM_THRESHOLD")
        .ok()
        .and_then(|v| v.trim().parse::<f64>().ok())
        .filter(|t| *t > 0.0 && *t <= 1.0)
        .unwrap_or(DEFAULT_TRGM_THRESHOLD)
}

/// Opt-in DB-side blocking (audit H5): fetch candidate entity-reef pairs whose
/// titles are trigram-similar above the threshold, paginated by `node_id` so the
/// scan is NOT capped at `MAX_ENTITY_NODES`. Pairs are returned canonical
/// `(lo, hi)`, with `dismissals` ("keep separate") filtered out. Uses a runtime
/// `sqlx::query` (no macro); each page runs under the job's [`JobScope`]
/// (consumer plane: its own short `begin_account_scoped` transaction — same
/// pool-checkout-per-page pressure as the pre-conversion direct pool fetch).
async fn trgm_candidate_pairs(
    scope: &JobScope<'_>,
    graph_id: Uuid,
    dismissals: &HashSet<(Uuid, Uuid)>,
) -> Result<Vec<(Uuid, Uuid)>, String> {
    use sqlx::Row;

    let threshold = trgm_threshold();
    let mut out: Vec<(Uuid, Uuid)> = Vec::new();
    let mut seen: HashSet<(Uuid, Uuid)> = HashSet::new();
    // Keyset pagination over the "left" reef so a large graph is swept fully
    // without OFFSET drift or a 500-row ceiling.
    let mut after: Uuid = Uuid::nil();

    loop {
        // Self-join entity reefs whose titles are trigram-similar. The `%`
        // operator (set_limit defaults to ~0.3) prunes via the GIN trgm index;
        // the explicit `similarity() >= threshold` makes the cutoff
        // configurable. `a.node_id < b.node_id` yields each unordered pair once
        // and already canonicalizes to `(lo, hi)`.
        let page = sqlx::query(
            "SELECT a.node_id AS a_id, b.node_id AS b_id \
             FROM nodes a \
             JOIN nodes b \
               ON b.graph_id = a.graph_id \
              AND b.type = 'entity' \
              AND a.node_id < b.node_id \
              AND a.title % b.title \
              AND similarity(a.title, b.title) >= $2 \
             WHERE a.graph_id = $1 \
               AND a.type = 'entity' \
               AND a.node_id > $3 \
             ORDER BY a.node_id ASC \
             LIMIT $4",
        )
        .bind(graph_id)
        .bind(threshold)
        .bind(after)
        .bind(TRGM_PAGE_SIZE);
        let rows = match scope {
            JobScope::Consumer { pool, account_id } => {
                let mut tx = begin_account_scoped(pool, *account_id)
                    .await
                    .map_err(|e| e.to_string())?;
                let rows = page.fetch_all(&mut *tx).await.map_err(|e| e.to_string())?;
                tx.commit().await.map_err(|e| e.to_string())?;
                rows
            }
            JobScope::Org(pool) => {
                let mut tx = begin_tenant_system_scoped_without_account(pool)
                    .await
                    .map_err(|e| e.to_string())?;
                let rows = page.fetch_all(&mut *tx).await.map_err(|e| e.to_string())?;
                tx.commit().await.map_err(|e| e.to_string())?;
                rows
            }
        };

        if rows.is_empty() {
            break;
        }

        let mut max_a = after;
        let mut page_len = 0usize;
        for row in &rows {
            page_len += 1;
            let a_id: Uuid = row.try_get("a_id").map_err(|e| e.to_string())?;
            let b_id: Uuid = row.try_get("b_id").map_err(|e| e.to_string())?;
            if a_id > max_a {
                max_a = a_id;
            }
            let pair = canonical_pair(a_id, b_id);
            if dismissals.contains(&pair) {
                continue;
            }
            if seen.insert(pair) {
                out.push(pair);
            }
        }

        // A short page (or no advance) means the sweep is complete. Note pairs
        // sharing the same `a.node_id` may straddle a page boundary; advancing
        // by the page's max `a_id` only after a full page keeps every such pair.
        if page_len < TRGM_PAGE_SIZE as usize || max_a == after {
            break;
        }
        after = max_a;
    }

    Ok(out)
}

/// Load the `Node` rows for a specific set of reef ids (used by the trgm path so
/// the downstream loop can resolve each candidate id back to a `Node`). Mirrors
/// the column shape of `Store::list_nodes` and is NOT capped at 500. Runtime
/// `sqlx::query` (no macro), run under the job's [`JobScope`].
async fn load_nodes_by_ids(
    scope: &JobScope<'_>,
    graph_id: Uuid,
    ids: &HashSet<Uuid>,
) -> Result<Vec<Node>, String> {
    use sqlx::Row;

    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let id_vec: Vec<Uuid> = ids.iter().copied().collect();
    let query = sqlx::query(
        "SELECT n.node_id, n.type, n.title, n.scope, n.created_at, n.updated_at, \
                COUNT(na.atom_id) AS atom_count \
         FROM nodes n \
         LEFT JOIN node_atoms na ON na.node_id = n.node_id \
         WHERE n.graph_id = $1 \
           AND n.node_id = ANY($2) \
         GROUP BY n.node_id",
    )
    .bind(graph_id)
    .bind(&id_vec);
    let rows = match scope {
        JobScope::Consumer { pool, account_id } => {
            let mut tx = begin_account_scoped(pool, *account_id)
                .await
                .map_err(|e| e.to_string())?;
            let rows = query.fetch_all(&mut *tx).await.map_err(|e| e.to_string())?;
            tx.commit().await.map_err(|e| e.to_string())?;
            rows
        }
        JobScope::Org(pool) => {
            let mut tx = begin_tenant_system_scoped_without_account(pool)
                .await
                .map_err(|e| e.to_string())?;
            let rows = query.fetch_all(&mut *tx).await.map_err(|e| e.to_string())?;
            tx.commit().await.map_err(|e| e.to_string())?;
            rows
        }
    };

    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        out.push(Node {
            node_id: row.try_get("node_id").map_err(|e| e.to_string())?,
            node_type: row.try_get("type").map_err(|e| e.to_string())?,
            title: row.try_get("title").map_err(|e| e.to_string())?,
            scope: row.try_get("scope").map_err(|e| e.to_string())?,
            created_at: row.try_get("created_at").map_err(|e| e.to_string())?,
            updated_at: row.try_get("updated_at").map_err(|e| e.to_string())?,
            atom_count: row.try_get("atom_count").map_err(|e| e.to_string())?,
        });
    }
    Ok(out)
}

/// Canonicalize a pair to `(lo, hi)` so order-insensitive lookups work.
fn canonical_pair(a: Uuid, b: Uuid) -> (Uuid, Uuid) {
    if a <= b {
        (a, b)
    } else {
        (b, a)
    }
}

fn find_node(nodes: &[Node], id: Uuid) -> Option<&Node> {
    nodes.iter().find(|n| n.node_id == id)
}

/// Choose `(keep_id, dup_id)`: keep the reef with more corals; tie-break to the
/// older reef (smaller `created_at`). `merge_nodes` deletes `dup` into `keep`.
fn choose_keep_dup(a: &Node, b: &Node) -> (Uuid, Uuid) {
    let a_keeps = a.atom_count > b.atom_count
        || (a.atom_count == b.atom_count && a.created_at <= b.created_at);
    if a_keeps {
        (a.node_id, b.node_id)
    } else {
        (b.node_id, a.node_id)
    }
}

/// Lowercase + trim a title for blocking comparisons.
fn normalize_title(title: &str) -> String {
    title.trim().to_lowercase()
}

/// Tokenize a normalized title into alphanumeric word tokens.
fn tokens(norm: &str) -> Vec<String> {
    norm.split(|c: char| !c.is_alphanumeric())
        .filter(|t| !t.is_empty())
        .map(|t| t.to_string())
        .collect()
}

/// Levenshtein edit distance (small strings only — used as a blocking heuristic).
fn edit_distance(a: &str, b: &str) -> usize {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    if a.is_empty() {
        return b.len();
    }
    if b.is_empty() {
        return a.len();
    }
    let mut prev: Vec<usize> = (0..=b.len()).collect();
    let mut curr = vec![0usize; b.len() + 1];
    for (i, ca) in a.iter().enumerate() {
        curr[0] = i + 1;
        for (j, cb) in b.iter().enumerate() {
            let cost = if ca == cb { 0 } else { 1 };
            curr[j + 1] = (prev[j + 1] + 1).min(curr[j] + 1).min(prev[j] + cost);
        }
        std::mem::swap(&mut prev, &mut curr);
    }
    prev[b.len()]
}

/// True if two normalized titles are similar enough to be worth an LLM call:
/// exact match, a shared word token, or a small edit distance on short names.
fn titles_similar(an: &str, bn: &str) -> bool {
    if an == bn {
        return true;
    }
    let at = tokens(an);
    let bt = tokens(bn);
    // Shared significant token (length ≥ 3 to avoid blocking on "of"/"the").
    let bset: HashSet<&String> = bt.iter().collect();
    if at.iter().any(|t| t.len() >= 3 && bset.contains(t)) {
        return true;
    }
    // Small edit distance on short single-token names (e.g. "kacey" vs "Casey").
    if at.len() == 1 && bt.len() == 1 {
        let (sa, sb) = (&at[0], &bt[0]);
        let maxlen = sa.chars().count().max(sb.chars().count());
        if maxlen > 0 && maxlen <= 8 && edit_distance(sa, sb) <= 2 {
            return true;
        }
    }
    false
}

/// Order-insensitive token-set overlap (Jaccard) of two normalized titles,
/// with a containment floor. This is the half of [`pair_priority`] that
/// actually detects a duplicate, and it is deliberately NOT the same
/// predicate as [`titles_similar`]: that one is a recall-first BLOCKER
/// (any shared token ≥3 chars) whose job is to never miss a pair, which is
/// why it admits ~98 candidates per entity on a real ocean. This one RANKS
/// what the blocker admitted.
///
/// Order-insensitivity is the point: "Alex Example" and "Alex Alex"
/// have identical token sets and score 1.0 here, while `routes/nodes.rs`'s
/// `suggest_pair` can never flag them (equal token counts skip its
/// containment check, and the permutation is far past its edit-distance
/// bound). Containment ("Alex" ⊂ "Alex Alex") floors at 0.75 rather
/// than taking the raw Jaccard of 0.5, because a short name contained in a
/// longer one is a much stronger alias signal than the set arithmetic alone
/// suggests.
fn title_affinity(an: &str, bn: &str) -> f64 {
    if an == bn {
        return 1.0;
    }
    let at: HashSet<String> = tokens(an).into_iter().collect();
    let bt: HashSet<String> = tokens(bn).into_iter().collect();
    if at.is_empty() || bt.is_empty() {
        return 0.0;
    }
    let inter = at.intersection(&bt).count() as f64;
    let union = at.union(&bt).count() as f64;
    let jaccard = if union > 0.0 { inter / union } else { 0.0 };
    // Either token set fully contained in the other.
    if inter > 0.0 && (inter as usize == at.len() || inter as usize == bt.len()) {
        return jaccard.max(0.75);
    }
    jaccard
}

/// Structural mass of one reef — how much of the graph a merge involving it
/// would actually move. `degree` is approximated by `coral_count` here
/// because [`Node`] is what blocking already loaded; the worker does not
/// re-query `node_structural_impact` for every candidate just to rank it.
fn node_mass(n: &Node) -> f64 {
    n.atom_count.max(0) as f64
}

/// How much one candidate pair is worth spending one of this job's
/// [`MAX_PAIRS_PER_JOB`] LLM calls on. Higher is better; deterministic.
///
/// Two terms, in deliberate proportion:
///   * [`title_affinity`] (0..1) DOMINATES — it is the duplicate signal.
///   * structural mass contributes at most `MASS_WEIGHT`, breaking ties
///     toward pairs whose merge would actually change the graph.
///
/// Mass is aggregated with `max`, not `min`, and that is the whole point of
/// this function. The canonical duplicate shape is a fat hub beside a thin
/// twin ("Alex Example" at 3,198 corals beside "Alex Alex" at 0), so
/// `min` would rank exactly the pairs that matter most at the very bottom.
/// `max` mirrors `review_policy::any_high_impact`'s "either endpoint is
/// consequential" predicate, which is the right symmetry for a merge pair.
///
/// Identity-resolution-at-the-door §10.5 adds two more terms, both additive
/// bumps on top of the affinity+mass base: `provisional` (which of `a`/`b`'s
/// ids are freshly-minted provisional reefs — §4.4) and `self_node_id` (the
/// graph's bound self reef, if any). A provisional reef's own near-match
/// signal, or a twin of the account owner, is worth reaching within THIS
/// job's budget even when its title shares no token with its twin at all
/// (an email-titled mint beside "Alex Example" scores near-zero on
/// `title_affinity` alone) — this is what makes [`resolve_graph`]'s
/// provisional-seed union (§10.5) actually get PROCESSED, not merely
/// considered.
fn pair_priority(
    a: &Node,
    b: &Node,
    provisional: &HashSet<Uuid>,
    self_node_id: Option<Uuid>,
) -> f64 {
    /// Ceiling on the mass term's contribution, relative to affinity's 0..1.
    const MASS_WEIGHT: f64 = 0.15;
    /// Mass at which the term saturates (10^4 corals).
    const MASS_SATURATION_LOG: f64 = 4.0;
    /// §10.5: bump for a pair where either endpoint is a freshly-minted
    /// provisional reef.
    const PROVISIONAL_BUMP: f64 = 0.2;
    /// §10.5: bump for a pair touching the self reef — a duplicate of the
    /// account owner is the single most consequential twin in the graph.
    const SELF_BUMP: f64 = 0.3;

    let affinity = title_affinity(&normalize_title(&a.title), &normalize_title(&b.title));
    let mass = node_mass(a).max(node_mass(b));
    let mass_term = ((1.0 + mass).log10() / MASS_SATURATION_LOG).clamp(0.0, 1.0);
    let mut score = affinity + MASS_WEIGHT * mass_term;
    if provisional.contains(&a.node_id) || provisional.contains(&b.node_id) {
        score += PROVISIONAL_BUMP;
    }
    if self_node_id.is_some_and(|id| id == a.node_id || id == b.node_id) {
        score += SELF_BUMP;
    }
    score
}

/// What one sweep decided to do with its candidate pairs — returned by
/// [`select_pairs`] so the caller can both drive the loop and log honest
/// coverage counters.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct PairSelection {
    /// Pairs this lap will actually process, in processing order.
    pub selected: Vec<(Uuid, Uuid)>,
    /// Dropped because an active judgement already settles them. These used
    /// to consume budget slots and produce nothing.
    pub settled: usize,
    /// Eligible but over the cap this lap. THIS is the number that must
    /// trend to zero for the sweep to be converging.
    pub deferred: usize,
}

/// Choose which candidate pairs this job spends its budget on.
///
/// Replaces `considered.truncate(MAX_PAIRS_PER_JOB)`, which took the first
/// `cap` pairs in nested-loop `(i, j)` index order. Two defects, both
/// measured on the primary ocean 2026-08-13:
///
///   1. **Settled pairs consumed budget.** The read gate ran per-pair INSIDE
///      the adjudication loop, i.e. after truncation, so a slot could be
///      spent discovering the pair was already decided. Dropping `settled`
///      here instead is what makes the candidate backlog shrink
///      monotonically — each lap now retires `cap` PREVIOUSLY-UNDECIDED
///      pairs, which is the convergence property the sweep never had.
///   2. **Index order starved exactly the reefs that mattered.** Page index
///      IS `(updated_at ASC, node_id ASC)` order, so the head of every page
///      is its LEAST recently updated reefs — and an active hub is always
///      among the MOST recently updated. On a 500-reef page carrying 6,807
///      candidate pairs the cap was exhausted by left index 1: reefs at
///      index 2..499 were never compared to anything. Four reefs for one
///      person sat at ordinals 6,736–6,753 and were cut on every sweep.
///
/// `materialize_first` (active POSITIVEs awaiting merge) is placed ahead of
/// everything else because those cost no LLM call and §6 promises they
/// materialize promptly. They still consume cap, so one lap's total work
/// stays bounded.
pub(crate) fn select_pairs(
    considered: &[(Uuid, Uuid)],
    nodes: &[Node],
    settled: &HashSet<(Uuid, Uuid)>,
    materialize_first: &HashSet<(Uuid, Uuid)>,
    provisional: &HashSet<Uuid>,
    self_node_id: Option<Uuid>,
    cap: usize,
) -> PairSelection {
    // Score ONCE per pair, then sort on the precomputed key. Scoring inside
    // the comparator instead would re-derive it O(log n) times per pair and
    // do two linear `find_node` scans each time — ~10^8 operations on a real
    // 500-reef / 6,800-pair page, for a value that never changes during the
    // sort.
    let index: std::collections::HashMap<Uuid, &Node> =
        nodes.iter().map(|n| (n.node_id, n)).collect();
    let mut settled_count = 0usize;
    /// `(free, score, canonical_pair, original_pair)`.
    type Ranked = (bool, f64, (Uuid, Uuid), (Uuid, Uuid));
    let mut eligible: Vec<Ranked> = Vec::new();

    for pair in considered {
        let canon = canonical_pair(pair.0, pair.1);
        if settled.contains(&canon) {
            settled_count += 1;
            continue;
        }
        // A pair whose endpoints blocking didn't load can't be scored. Rank
        // it last rather than dropping it: the loop's own missing-`Node`
        // guard is what decides to skip it, and silently discarding it here
        // would hide that from the `deferred` counter.
        let score = match (index.get(&pair.0), index.get(&pair.1)) {
            (Some(a), Some(b)) => pair_priority(a, b, provisional, self_node_id),
            _ => f64::NEG_INFINITY,
        };
        eligible.push((materialize_first.contains(&canon), score, canon, *pair));
    }

    // Materialization first (costs no LLM call), then priority desc. The
    // canonical pair is the final tie-break so the order is TOTAL and
    // reproducible — score alone would leave equal-scoring pairs in whatever
    // order blocking happened to emit them, making selection differ between
    // otherwise identical laps and reintroducing a form of the positional
    // bias this function exists to remove.
    eligible.sort_by(|x, y| {
        y.0.cmp(&x.0)
            .then_with(|| y.1.partial_cmp(&x.1).unwrap_or(std::cmp::Ordering::Equal))
            .then_with(|| x.2.cmp(&y.2))
    });

    let deferred = eligible.len().saturating_sub(cap);
    eligible.truncate(cap);
    PairSelection {
        selected: eligible.into_iter().map(|(_, _, _, pair)| pair).collect(),
        settled: settled_count,
        deferred,
    }
}

/// Generate candidate duplicate pairs via cheap blocking, skipping any pair the
/// user already marked "keep separate". Returned in canonical `(lo, hi)` order.
fn candidate_pairs(nodes: &[Node], dismissals: &HashSet<(Uuid, Uuid)>) -> Vec<(Uuid, Uuid)> {
    let norms: Vec<String> = nodes.iter().map(|n| normalize_title(&n.title)).collect();
    let mut out = Vec::new();
    for i in 0..nodes.len() {
        for j in (i + 1)..nodes.len() {
            if nodes[i].node_id == nodes[j].node_id {
                continue;
            }
            if !titles_similar(&norms[i], &norms[j]) {
                continue;
            }
            let pair = canonical_pair(nodes[i].node_id, nodes[j].node_id);
            if dismissals.contains(&pair) {
                continue;
            }
            out.push(pair);
        }
    }
    out
}

// ── Evidence gathering + prompt construction ────────────────────────────────────

/// Result of comparing two candidate reefs' `reef_identifiers` rows (T2).
struct IdentifierEvidence {
    /// `true` only when at least one [`DECISIVE_IDENTIFIER_KINDS`] kind is
    /// SHARED (identical `value_norm` on both sides) and NO kind in that same
    /// decisive set CONFLICTS (both sides carry a value for it, but not the
    /// same one). A conflict on any decisive kind vetoes the shortcut even if
    /// a DIFFERENT decisive kind is shared — sharing an email while
    /// disagreeing on Slack user id is a real contradiction, not confirming
    /// evidence, so it is safer to fall through to the LLM (or a human) than
    /// to auto-merge.
    auto_merge: bool,
    /// Some decisive kind is carried by both sides with no value in common:
    /// two identities. Never merged and never sent to the model.
    conflict: bool,
    /// One human-readable line per identifier kind that overlaps at all
    /// (shared OR conflicting), across EVERY kind including `git_author` —
    /// richer evidence for the LLM even though `git_author` alone never
    /// drives `auto_merge`. Empty when the two reefs' identifiers don't
    /// overlap on any kind (nothing worth telling the LLM).
    facts: Vec<String>,
}

/// Compare two reefs' identifiers (already filtered to each `node_id` by the
/// caller — see `resolve_graph`'s `a_ids`/`b_ids`). Deterministic output
/// ordering (`facts` is sorted) so the same pair always produces the same
/// prompt text regardless of the identifiers' DB read order.
fn identifier_evidence(
    a: &[&crate::store::ReefIdentifier],
    b: &[&crate::store::ReefIdentifier],
) -> IdentifierEvidence {
    let kinds: HashSet<&str> =
        a.iter().map(|i| i.kind.as_str()).chain(b.iter().map(|i| i.kind.as_str())).collect();

    let mut facts = Vec::new();
    let mut decisive_shared = false;
    let mut decisive_conflict = false;

    for kind in kinds {
        let a_vals: HashSet<&str> =
            a.iter().filter(|i| i.kind == kind).map(|i| i.value_norm.as_str()).collect();
        let b_vals: HashSet<&str> =
            b.iter().filter(|i| i.kind == kind).map(|i| i.value_norm.as_str()).collect();
        // Nothing to compare unless BOTH sides carry at least one identifier
        // of this kind — one side having an identifier the other simply
        // never recorded is not evidence either way.
        if a_vals.is_empty() || b_vals.is_empty() {
            continue;
        }
        let is_decisive = DECISIVE_IDENTIFIER_KINDS.contains(&kind);
        if a_vals.iter().any(|v| b_vals.contains(v)) {
            let mut shared: Vec<&str> = a_vals.intersection(&b_vals).copied().collect();
            shared.sort_unstable();
            facts.push(format!("shared {kind} identifier: {}", shared.join(", ")));
            if is_decisive {
                decisive_shared = true;
            }
        } else {
            // Both sides have this kind, but no value in common → a genuine
            // conflict (e.g. two different Slack user ids).
            let mut a_sorted: Vec<&str> = a_vals.into_iter().collect();
            a_sorted.sort_unstable();
            let mut b_sorted: Vec<&str> = b_vals.into_iter().collect();
            b_sorted.sort_unstable();
            facts.push(format!(
                "conflicting {kind} identifiers: {} (A) vs {} (B)",
                a_sorted.join(", "),
                b_sorted.join(", ")
            ));
            if is_decisive {
                decisive_conflict = true;
            }
        }
    }
    facts.sort();

    IdentifierEvidence { auto_merge: decisive_shared && !decisive_conflict, conflict: decisive_conflict, facts }
}

/// Pure string-assembly step split out of [`candidate_block_tx`] (T1 review fix
/// — the async version does an LLM/DB fetch for `summary`/`facts`/
/// `entity_type` first, so it can't be unit-tested without a `Store`; this
/// half can). This is exactly the spot where a swapped `format!` argument
/// (e.g. `title`/`entity_type` transposed) would silently corrupt the
/// `## Candidate <label>` block the resolve-entities prompt parses — the
/// LLM would then read a person's NAME as its TYPE line and vice versa with
/// no error anywhere, only a degraded adjudication.
fn format_candidate_block(
    label: &str,
    title: &str,
    entity_type: &str,
    summary: Option<&str>,
    facts: &[String],
) -> String {
    let mut block = format!(
        "## Candidate {label}\ntitle: {title}\ntype: {entity_type}\naliases: \nsummary: {}\nfacts:\n",
        summary.unwrap_or_default()
    );
    if facts.is_empty() {
        block.push_str("  - (no facts available)\n");
    } else {
        for f in facts {
            block.push_str("  - ");
            block.push_str(f);
            block.push('\n');
        }
    }
    block
}

/// First non-empty line of an atom, truncated to a snippet length.
fn snippet(text: &str) -> String {
    let line = text
        .lines()
        .map(str::trim)
        .find(|l| !l.is_empty())
        .unwrap_or("")
        .to_string();
    if line.chars().count() > FACT_SNIPPET_LEN {
        let truncated: String = line.chars().take(FACT_SNIPPET_LEN).collect();
        format!("{truncated}…")
    } else {
        line
    }
}

// ── LLM adjudication ────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct Verdict {
    verdict: String,
    #[serde(default)]
    confidence: f64,
    /// Now read (PR2): folded into `NewJudgement.evidence` on every
    /// judgement-graph write this file makes, so the LLM's own justification
    /// survives alongside the verdict, not just alongside the log line.
    #[serde(default)]
    reason: String,
}

/// JSON Schema for the forced-tool resolution verdict (audit H3). Mirrors the
/// `Verdict` struct. When honored, `resp.content` is guaranteed-valid JSON;
/// otherwise `parse_verdict` (strip-fence + brace slice) is the fallback.
fn verdict_tool_schema() -> ToolSchema {
    ToolSchema {
        strict: false,
        name: "emit_resolution_verdict".to_string(),
        schema: serde_json::json!({
            "type": "object",
            "properties": {
                "verdict": { "type": "string", "enum": ["same", "different", "unsure"] },
                "confidence": { "type": "number" },
                "reason": { "type": "string" }
            },
            "required": ["verdict"]
        }),
    }
}

/// Robustly extract the verdict JSON object from a possibly-fenced / prose-wrapped
/// model response (strip fences, take the first `{` to the last `}`).
fn parse_verdict(raw: &str) -> Result<Verdict, String> {
    let cleaned = raw.trim().trim_start_matches("```json").trim_start_matches("```").trim();
    let start = cleaned.find('{').ok_or_else(|| format!("no JSON object in response: {cleaned}"))?;
    let end = cleaned.rfind('}').ok_or_else(|| format!("no closing brace in response: {cleaned}"))?;
    if end < start {
        return Err(format!("malformed JSON object in response: {cleaned}"));
    }
    let json_str = &cleaned[start..=end];
    serde_json::from_str(json_str).map_err(|e| format!("verdict parse error: {e} — raw: {json_str}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── identifier_evidence (T2 "identifier awareness") ─────────────────────────

    fn mk_ident(kind: &str, value_norm: &str) -> crate::store::ReefIdentifier {
        crate::store::ReefIdentifier {
            node_id: Uuid::new_v4(),
            kind: kind.to_string(),
            value_norm: value_norm.to_string(),
        }
    }

    #[test]
    fn identifier_evidence_shared_email_auto_merges() {
        let a = [mk_ident("email", "alice@example.com")];
        let b = [mk_ident("email", "alice@example.com")];
        let a_refs: Vec<&crate::store::ReefIdentifier> = a.iter().collect();
        let b_refs: Vec<&crate::store::ReefIdentifier> = b.iter().collect();
        let evidence = identifier_evidence(&a_refs, &b_refs);
        assert!(evidence.auto_merge);
        assert_eq!(evidence.facts, vec!["shared email identifier: alice@example.com"]);
    }

    #[test]
    fn identifier_evidence_conflicting_slack_user_blocks_auto_merge() {
        let a = [mk_ident("slack_user", "U111")];
        let b = [mk_ident("slack_user", "U222")];
        let a_refs: Vec<&crate::store::ReefIdentifier> = a.iter().collect();
        let b_refs: Vec<&crate::store::ReefIdentifier> = b.iter().collect();
        let evidence = identifier_evidence(&a_refs, &b_refs);
        assert!(!evidence.auto_merge);
        assert_eq!(evidence.facts, vec!["conflicting slack_user identifiers: U111 (A) vs U222 (B)"]);
    }

    #[test]
    fn identifier_evidence_shared_email_but_conflicting_slack_user_blocks_auto_merge() {
        // A DIFFERENT decisive kind conflicting must veto the shortcut even
        // though email agrees — see `IdentifierEvidence::auto_merge`'s doc.
        let a = [mk_ident("email", "alice@example.com"), mk_ident("slack_user", "U111")];
        let b = [mk_ident("email", "alice@example.com"), mk_ident("slack_user", "U222")];
        let a_refs: Vec<&crate::store::ReefIdentifier> = a.iter().collect();
        let b_refs: Vec<&crate::store::ReefIdentifier> = b.iter().collect();
        let evidence = identifier_evidence(&a_refs, &b_refs);
        assert!(!evidence.auto_merge);
        assert_eq!(evidence.facts.len(), 2);
    }

    #[test]
    fn identifier_evidence_git_author_never_auto_merges_alone() {
        let a = [mk_ident("git_author", "Alice <alice@example.com>")];
        let b = [mk_ident("git_author", "Alice <alice@example.com>")];
        let a_refs: Vec<&crate::store::ReefIdentifier> = a.iter().collect();
        let b_refs: Vec<&crate::store::ReefIdentifier> = b.iter().collect();
        let evidence = identifier_evidence(&a_refs, &b_refs);
        assert!(!evidence.auto_merge);
        assert_eq!(evidence.facts.len(), 1);
    }

    #[test]
    fn identifier_evidence_disjoint_kinds_produce_no_facts() {
        let a = [mk_ident("email", "alice@example.com")];
        let b = [mk_ident("slack_user", "U111")];
        let a_refs: Vec<&crate::store::ReefIdentifier> = a.iter().collect();
        let b_refs: Vec<&crate::store::ReefIdentifier> = b.iter().collect();
        let evidence = identifier_evidence(&a_refs, &b_refs);
        assert!(!evidence.auto_merge);
        assert!(evidence.facts.is_empty());
    }

    #[test]
    fn identifier_evidence_empty_both_sides_is_empty() {
        let evidence = identifier_evidence(&[], &[]);
        assert!(!evidence.auto_merge);
        assert!(evidence.facts.is_empty());
    }

    #[test]
    fn normalize_and_token_blocking() {
        assert!(titles_similar(&normalize_title("Sam Carter"), &normalize_title("sam carter")));
        // shared significant token "carter"
        assert!(titles_similar("sam carter", "samuel carter"));
        // no shared token, not short single names
        assert!(!titles_similar("anthropic", "openai"));
    }

    #[test]
    fn short_name_edit_distance_blocks() {
        // "kacey" vs "Casey" — distance 2 on short single-token names
        assert!(titles_similar("kacey", "Casey"));
        assert!(!titles_similar("kacey", "Alex"));
    }

    #[test]
    fn blocking_is_general_not_tuned_to_one_name_pair() {
        // Generality proof: the near-duplicate blocker is a general edit-distance /
        // shared-token rule, not something tuned to the "kacey/Casey" pair. It must
        // flag ARBITRARY near-miss names for LLM review and leave genuinely
        // distinct ones alone — on data that looks nothing like the source ocean.

        // Near-miss short single-token names (≤2 edits) → blocked for adjudication:
        assert!(titles_similar("sten", "sven")); // 1 substitution
        assert!(titles_similar("jon", "joan")); // 1 insertion
        assert!(titles_similar("lena", "lina")); // 1 substitution

        // Shared significant token across different multi-word names → blocked:
        assert!(titles_similar("acme corp", "acme inc"));
        assert!(titles_similar("northwind logistics", "northwind freight"));

        // Genuinely different names → NOT blocked (no spurious merge proposed):
        assert!(!titles_similar("microsoft", "spotify"));
        assert!(!titles_similar("kubernetes", "postgres"));
        assert!(!titles_similar("dana", "omar"));
    }

    #[test]
    fn candidate_pairs_skip_dismissals() {
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();
        let mk = |id, title: &str| Node {
            node_id: id,
            node_type: "entity".into(),
            title: title.into(),
            scope: "private".into(),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            atom_count: 1,
        };
        let nodes = vec![mk(a, "Kacey"), mk(b, "Kacey")];
        let empty = HashSet::new();
        assert_eq!(candidate_pairs(&nodes, &empty).len(), 1);
        let mut dis = HashSet::new();
        dis.insert(canonical_pair(a, b));
        assert!(candidate_pairs(&nodes, &dis).is_empty());
    }

    // ── `title_affinity` / `pair_priority` / `select_pairs` ───────────────────

    fn mk_node(id: Uuid, title: &str, atom_count: i64) -> Node {
        Node {
            node_id: id,
            node_type: "entity".into(),
            title: title.into(),
            scope: "private".into(),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            atom_count,
        }
    }

    #[test]
    fn title_affinity_scores_token_permutations_as_identical() {
        // The prod case `routes/nodes.rs::suggest_pair` can NEVER flag:
        // equal token counts skip its containment check and the permutation
        // is far past its edit-distance bound. Ranking is order-insensitive
        // precisely so this pair sorts to the top instead of never surfacing.
        assert_eq!(title_affinity("Alex Example", "Alex Alex"), 1.0);
    }

    #[test]
    fn title_affinity_floors_containment_above_raw_jaccard() {
        // {Alex} ⊂ {Alex, Alex} — raw Jaccard is 0.5, but a short name
        // fully contained in a longer one is a stronger alias signal than
        // the set arithmetic alone says.
        let a = title_affinity("Alex", "Alex Alex");
        assert!(a >= 0.75, "containment should floor at 0.75, got {a}");
        // A single shared token out of many is what the recall-first blocker
        // admits by the thousand; it must rank far below containment.
        let junk = title_affinity("weekly review session", "deploy review runbook");
        assert!(junk < 0.75, "one-token overlap should rank low, got {junk}");
        assert!(a > junk);
    }

    #[test]
    fn pair_priority_aggregates_mass_with_max_not_min() {
        // The canonical duplicate shape is a fat hub beside a thin twin.
        // Under `min` aggregation this pair would score at the very bottom,
        // which is exactly the starvation being fixed.
        let hub = mk_node(Uuid::new_v4(), "Alex Example", 3198);
        let twin = mk_node(Uuid::new_v4(), "Alex Alex", 0);
        let two_tiny = (
            mk_node(Uuid::new_v4(), "Alex Example", 0),
            mk_node(Uuid::new_v4(), "Alex Alex", 0),
        );
        assert!(
            pair_priority(&hub, &twin, &HashSet::new(), None)
                > pair_priority(&two_tiny.0, &two_tiny.1, &HashSet::new(), None)
        );
    }

    #[test]
    fn pair_priority_bumps_a_provisional_endpoint() {
        let a = mk_node(Uuid::new_v4(), "Weekly Review Session 1", 1);
        let b = mk_node(Uuid::new_v4(), "Weekly Review Session 2", 1);
        let base = pair_priority(&a, &b, &HashSet::new(), None);
        let mut provisional = HashSet::new();
        provisional.insert(a.node_id);
        let bumped = pair_priority(&a, &b, &provisional, None);
        assert!(bumped > base, "a provisional endpoint must raise the pair's priority");
    }

    #[test]
    fn pair_priority_bumps_a_self_reef_pair_higher_than_a_provisional_one() {
        let a = mk_node(Uuid::new_v4(), "Weekly Review Session 1", 1);
        let b = mk_node(Uuid::new_v4(), "Weekly Review Session 2", 1);
        let mut provisional = HashSet::new();
        provisional.insert(a.node_id);
        let provisional_score = pair_priority(&a, &b, &provisional, None);
        let self_score = pair_priority(&a, &b, &HashSet::new(), Some(a.node_id));
        assert!(
            self_score > provisional_score,
            "the self-reef bump (0.3) must outrank the provisional bump (0.2)"
        );
    }

    /// THE regression test for this whole change.
    ///
    /// Reproduces the measured prod shape: one dense page of near-identically
    /// titled reefs generating far more candidate pairs than the per-job cap,
    /// with the one TRUE duplicate sitting at a high page index — because
    /// page index is `(updated_at ASC, node_id ASC)` order and an active hub
    /// is always among the most recently updated reefs in its ocean.
    ///
    /// Under the old `considered.truncate(cap)` this pair was unreachable on
    /// every sweep forever. The test asserts BOTH halves: that positional
    /// truncation misses it (so the test can't quietly stop testing anything)
    /// and that `select_pairs` finds it.
    #[test]
    fn select_pairs_reaches_a_true_duplicate_buried_deep_in_the_page() {
        const CAP: usize = 50;
        let mut nodes: Vec<Node> = (0..60)
            .map(|i| mk_node(Uuid::new_v4(), &format!("Weekly Review Session {i}"), 1))
            .collect();
        // The real duplicate, late in the page, hub-shaped.
        let dup_a = Uuid::new_v4();
        let dup_b = Uuid::new_v4();
        nodes.push(mk_node(dup_a, "Alex Example", 3198));
        nodes.push(mk_node(dup_b, "Alex Alex", 0));
        let target = canonical_pair(dup_a, dup_b);

        let considered = candidate_pairs(&nodes, &HashSet::new());
        assert!(
            considered.len() > CAP * 10,
            "test needs a page far denser than the cap, got {}",
            considered.len()
        );

        // Old behavior: take the first `CAP` in emission order.
        let positional: HashSet<(Uuid, Uuid)> =
            considered.iter().take(CAP).map(|(a, b)| canonical_pair(*a, *b)).collect();
        assert!(
            !positional.contains(&target),
            "positional truncation was supposed to MISS the buried duplicate — if this fails \
             the fixture no longer reproduces the bug and the assertion below proves nothing"
        );

        // New behavior: score-ranked selection reaches it.
        let selection = select_pairs(
            &considered, &nodes, &HashSet::new(), &HashSet::new(), &HashSet::new(), None, CAP,
        );
        let selected: HashSet<(Uuid, Uuid)> =
            selection.selected.iter().map(|(a, b)| canonical_pair(*a, *b)).collect();
        assert!(selected.contains(&target), "score-ranked selection must reach the duplicate");
        assert_eq!(selection.selected.len(), CAP);
        assert_eq!(selection.deferred, considered.len() - CAP);
    }

    #[test]
    fn select_pairs_drops_settled_pairs_without_spending_budget() {
        // The convergence property: a settled pair must not occupy a slot.
        // Before this, the gate ran INSIDE the loop (after truncation), so a
        // lap could spend its whole allowance rediscovering settled pairs.
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();
        let c = Uuid::new_v4();
        let d = Uuid::new_v4();
        let nodes = vec![
            mk_node(a, "Kacey Lane", 5),
            mk_node(b, "Kacey Lane", 5),
            mk_node(c, "Omar Rashid", 5),
            mk_node(d, "Omar Rashid", 5),
        ];
        let considered = vec![canonical_pair(a, b), canonical_pair(c, d)];
        let mut settled = HashSet::new();
        settled.insert(canonical_pair(a, b));

        let selection =
            select_pairs(&considered, &nodes, &settled, &HashSet::new(), &HashSet::new(), None, 1);
        assert_eq!(selection.settled, 1);
        assert_eq!(selection.deferred, 0, "the settled pair must not count as deferred work");
        assert_eq!(selection.selected, vec![canonical_pair(c, d)]);
    }

    #[test]
    fn select_pairs_puts_free_materializations_first() {
        // Active POSITIVEs cost no LLM call and §6 promises they materialize
        // promptly, so they outrank even a perfect-affinity fresh pair.
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();
        let c = Uuid::new_v4();
        let d = Uuid::new_v4();
        let nodes = vec![
            mk_node(a, "Zeta", 0),
            mk_node(b, "Zeta Two", 0),
            mk_node(c, "Alpha Beta", 9000),
            mk_node(d, "Beta Alpha", 9000),
        ];
        let considered = vec![canonical_pair(c, d), canonical_pair(a, b)];
        let mut materialize = HashSet::new();
        materialize.insert(canonical_pair(a, b));

        let selection = select_pairs(
            &considered, &nodes, &HashSet::new(), &materialize, &HashSet::new(), None, 2,
        );
        assert_eq!(selection.selected[0], canonical_pair(a, b));
    }

    #[test]
    fn select_pairs_is_deterministic_across_input_order() {
        // Selection must not depend on the order blocking happened to emit
        // pairs in — otherwise equal-scoring pairs reintroduce positional
        // bias by the back door.
        let ids: Vec<Uuid> = (0..8).map(|_| Uuid::new_v4()).collect();
        let nodes: Vec<Node> =
            ids.iter().map(|id| mk_node(*id, "Shared Name Token", 3)).collect();
        let considered = candidate_pairs(&nodes, &HashSet::new());
        let mut reversed = considered.clone();
        reversed.reverse();

        let forward = select_pairs(
            &considered, &nodes, &HashSet::new(), &HashSet::new(), &HashSet::new(), None, 5,
        );
        let backward = select_pairs(
            &reversed, &nodes, &HashSet::new(), &HashSet::new(), &HashSet::new(), None, 5,
        );
        assert_eq!(forward.selected, backward.selected);
    }

    // ── `next_sweep_cursor` (keyset sweep cursor advance/wrap decision) ────────

    /// Build a synthetic page of `n` distinct entity reefs, each with a
    /// strictly increasing `updated_at` so "the last row of the page" is
    /// unambiguous (mirrors the `(updated_at, node_id)` ordering the real
    /// `list_entity_nodes_after` query returns rows in).
    fn synthetic_page(n: usize) -> Vec<Node> {
        let base = chrono::Utc::now();
        (0..n)
            .map(|i| Node {
                node_id: Uuid::new_v4(),
                node_type: "entity".into(),
                title: format!("Reef {i}"),
                scope: "private".into(),
                created_at: base,
                updated_at: base + chrono::Duration::seconds(i as i64),
                atom_count: 1,
            })
            .collect()
    }

    #[test]
    fn next_sweep_cursor_full_page_advances_not_wraps() {
        // A page whose length is EXACTLY MAX_ENTITY_NODES must ADVANCE the
        // cursor to the last row, not wrap to None — this is the "lift the
        // 500-node cap" forward-progress case. If someone flips the
        // `page.len() as i64 == MAX_ENTITY_NODES` condition (e.g. to `<`),
        // this assertion fails because the cursor would come back `None`.
        let page = synthetic_page(MAX_ENTITY_NODES as usize);
        let last = page.last().expect("non-empty page");
        let expected = (last.updated_at, last.node_id);
        assert_eq!(next_sweep_cursor(&page), Some(expected));
    }

    #[test]
    fn next_sweep_cursor_short_page_wraps_to_none() {
        // Fewer rows than MAX_ENTITY_NODES means the sweep reached the end of
        // the ocean on this page — the cursor must wrap back to the start
        // (`None`), never advance to a partial-page row.
        let page = synthetic_page(MAX_ENTITY_NODES as usize - 1);
        assert_eq!(next_sweep_cursor(&page), None);

        let small_page = synthetic_page(3);
        assert_eq!(next_sweep_cursor(&small_page), None);
    }

    #[test]
    fn next_sweep_cursor_empty_page_wraps_to_none_and_skips_pairing() {
        // An empty page both wraps the cursor to None (same as any short
        // page) AND yields zero candidate pairs, i.e. no adjudication work is
        // attempted — `resolve_graph`'s `page.len() < 2` early-return relies
        // on exactly this.
        let page: Vec<Node> = Vec::new();
        assert_eq!(next_sweep_cursor(&page), None);
        assert!(candidate_pairs(&page, &HashSet::new()).is_empty());
    }

    #[test]
    fn choose_keep_dup_prefers_more_atoms() {
        let older = Node {
            node_id: Uuid::new_v4(),
            node_type: "entity".into(),
            title: "A".into(),
            scope: "private".into(),
            created_at: chrono::Utc::now() - chrono::Duration::hours(1),
            updated_at: chrono::Utc::now(),
            atom_count: 2,
        };
        let newer = Node {
            node_id: Uuid::new_v4(),
            node_type: "entity".into(),
            title: "A".into(),
            scope: "private".into(),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            atom_count: 5,
        };
        // newer has more atoms → it is kept
        let (keep, dup) = choose_keep_dup(&older, &newer);
        assert_eq!(keep, newer.node_id);
        assert_eq!(dup, older.node_id);
    }

    #[test]
    fn parse_verdict_strips_fences_and_prose() {
        let raw = "```json\n{\"verdict\":\"same\",\"confidence\":0.9,\"reason\":\"ok\"}\n```";
        let v = parse_verdict(raw).unwrap();
        assert_eq!(v.verdict, "same");
        assert!((v.confidence - 0.9).abs() < 1e-9);

        let raw2 = "Here is my answer: {\"verdict\":\"different\",\"confidence\":0.95} done";
        let v2 = parse_verdict(raw2).unwrap();
        assert_eq!(v2.verdict, "different");
    }

    // ── candidate_block `type:` line (T1 review fix) ──────────────────────────

    #[test]
    fn candidate_block_renders_unknown_type_when_hint_absent() {
        let block = format_candidate_block("A", "Phoenix", "unknown", None, &[]);
        assert!(
            block.contains("title: Phoenix\ntype: unknown\n"),
            "expected an unknown type line right after the title, got:\n{block}"
        );
    }

    #[test]
    fn candidate_block_renders_recorded_type_when_hint_present() {
        let block = format_candidate_block("B", "Phoenix", "person", None, &[]);
        // Arg-order regression guard: `title` and `entity_type` are adjacent
        // positional values in the underlying `format!` call — a swap would
        // silently put the entity's NAME on the `type:` line (or vice versa)
        // with no compile error and no runtime error, only a corrupted
        // adjudication prompt. Assert both lines land on their own field,
        // not just that "person" appears somewhere in the block.
        assert!(
            block.contains("title: Phoenix\ntype: person\n"),
            "expected the recorded type on its own line right after the title, got:\n{block}"
        );
        assert!(!block.contains("title: person"), "title/type must not be transposed");
        assert!(!block.contains("type: Phoenix"), "title/type must not be transposed");
    }

    // `ER_BLOCKING` / `ER_TRGM_THRESHOLD` mutate process-global env, so the env
    // cases share one serialized test to avoid races with other parallel tests.
    #[test]
    fn blocking_mode_env_switch_defaults_to_rust() {
        // Save + restore so we don't leak state into other tests.
        let prev_mode = std::env::var("ER_BLOCKING").ok();
        let prev_thresh = std::env::var("ER_TRGM_THRESHOLD").ok();

        // Default: unset → rust path (default behavior MUST be unchanged).
        std::env::remove_var("ER_BLOCKING");
        assert_eq!(blocking_mode(), BlockingMode::Rust);

        // Explicit / case-insensitive / whitespace `rust` → rust.
        std::env::set_var("ER_BLOCKING", "rust");
        assert_eq!(blocking_mode(), BlockingMode::Rust);
        std::env::set_var("ER_BLOCKING", "  RuSt  ");
        assert_eq!(blocking_mode(), BlockingMode::Rust);

        // Unknown value falls back to the default rust path (typo-safe).
        std::env::set_var("ER_BLOCKING", "bogus");
        assert_eq!(blocking_mode(), BlockingMode::Rust);

        // Opt-in trgm.
        std::env::set_var("ER_BLOCKING", "trgm");
        assert_eq!(blocking_mode(), BlockingMode::Trgm);
        std::env::set_var("ER_BLOCKING", " TRGM ");
        assert_eq!(blocking_mode(), BlockingMode::Trgm);

        // Threshold: default when unset/invalid, parsed + clamped otherwise.
        std::env::remove_var("ER_TRGM_THRESHOLD");
        assert!((trgm_threshold() - DEFAULT_TRGM_THRESHOLD).abs() < 1e-9);
        std::env::set_var("ER_TRGM_THRESHOLD", "0.55");
        assert!((trgm_threshold() - 0.55).abs() < 1e-9);
        std::env::set_var("ER_TRGM_THRESHOLD", "not-a-number");
        assert!((trgm_threshold() - DEFAULT_TRGM_THRESHOLD).abs() < 1e-9);
        std::env::set_var("ER_TRGM_THRESHOLD", "1.5"); // out of range → default
        assert!((trgm_threshold() - DEFAULT_TRGM_THRESHOLD).abs() < 1e-9);

        // Restore.
        match prev_mode {
            Some(v) => std::env::set_var("ER_BLOCKING", v),
            None => std::env::remove_var("ER_BLOCKING"),
        }
        match prev_thresh {
            Some(v) => std::env::set_var("ER_TRGM_THRESHOLD", v),
            None => std::env::remove_var("ER_TRGM_THRESHOLD"),
        }
    }

    // ── JUDGEMENT_GRAPH_WRITES flag parsing ─────────────────────────────────────

    // `JUDGEMENT_GRAPH_WRITES` mutates process-global env — one serialized test,
    // same discipline as `blocking_mode_env_switch_defaults_to_rust` above.
    #[test]
    fn judgement_write_mode_env_switch() {
        let prev = std::env::var("JUDGEMENT_GRAPH_WRITES").ok();

        // Unset → Off (default behavior MUST be unchanged).
        std::env::remove_var("JUDGEMENT_GRAPH_WRITES");
        assert_eq!(judgement_write_mode(), JudgementWriteMode::Off);

        // Blank / garbage → Off.
        std::env::set_var("JUDGEMENT_GRAPH_WRITES", "");
        assert_eq!(judgement_write_mode(), JudgementWriteMode::Off);
        std::env::set_var("JUDGEMENT_GRAPH_WRITES", "bogus");
        assert_eq!(judgement_write_mode(), JudgementWriteMode::Off);
        std::env::set_var("JUDGEMENT_GRAPH_WRITES", "0");
        assert_eq!(judgement_write_mode(), JudgementWriteMode::Off);
        std::env::set_var("JUDGEMENT_GRAPH_WRITES", "false");
        assert_eq!(judgement_write_mode(), JudgementWriteMode::Off);

        // `log`, case-insensitive / whitespace-tolerant → Log.
        std::env::set_var("JUDGEMENT_GRAPH_WRITES", "log");
        assert_eq!(judgement_write_mode(), JudgementWriteMode::Log);
        std::env::set_var("JUDGEMENT_GRAPH_WRITES", "  LOG  ");
        assert_eq!(judgement_write_mode(), JudgementWriteMode::Log);

        // Truthy tokens, case-insensitive → On.
        for tok in ["1", "true", "yes", "on", "TRUE", "On", " yes "] {
            std::env::set_var("JUDGEMENT_GRAPH_WRITES", tok);
            assert_eq!(
                judgement_write_mode(),
                JudgementWriteMode::On,
                "expected {tok:?} to parse as On"
            );
        }

        // Restore.
        match prev {
            Some(v) => std::env::set_var("JUDGEMENT_GRAPH_WRITES", v),
            None => std::env::remove_var("JUDGEMENT_GRAPH_WRITES"),
        }
    }

    // ── §5.0 gate_pair — every decision-table row ────────────────────────────────

    /// Build a minimal `ReefJudgement` for `gate_pair` tests — only the four
    /// fields the function reads (`verdict`, `source`, `prompt_version`,
    /// `expires_at`) vary per test; everything else is a fixed placeholder.
    fn mk_judgement(
        verdict: &str,
        source: &str,
        prompt_version: Option<&str>,
        expires_at: Option<chrono::DateTime<chrono::Utc>>,
    ) -> ReefJudgement {
        ReefJudgement {
            judgement_id: Uuid::new_v4(),
            graph_id: Uuid::new_v4(),
            node_a_id: Uuid::new_v4(),
            node_b_id: Uuid::new_v4(),
            verdict: verdict.to_string(),
            source: source.to_string(),
            confidence: None,
            model_id: None,
            prompt_version: prompt_version.map(|s| s.to_string()),
            evidence: serde_json::json!({}),
            created_at: chrono::Utc::now(),
            created_by: None,
            expires_at,
            annulled_at: None,
            annulled_by: None,
            annulled_by_kind: None,
            annul_reason: None,
            superseded_by: None,
        }
    }

    #[test]
    fn gate_pair_dismissed_wins_even_over_an_active_positive() {
        // dismissed is checked FIRST, before `active` is even inspected.
        let j = mk_judgement("positive", "llm", None, None);
        assert_eq!(gate_pair(true, Some(&j), RESOLVE_PROMPT_VERSION), GateOutcome::Suppress);
    }

    #[test]
    fn gate_pair_no_active_judgement_readjudicates() {
        assert_eq!(gate_pair(false, None, RESOLVE_PROMPT_VERSION), GateOutcome::ReAdjudicate);
    }

    #[test]
    fn gate_pair_active_positive_materializes_regardless_of_source() {
        for source in ["identifier_auto", "llm", "human", "backfill"] {
            let j = mk_judgement("positive", source, None, None);
            let outcome = gate_pair(false, Some(&j), RESOLVE_PROMPT_VERSION);
            assert_eq!(outcome, GateOutcome::Materialize(j.judgement_id), "source={source}");
        }
    }

    #[test]
    fn gate_pair_human_negative_suppresses_permanently() {
        // Even with a prompt-version mismatch — human negatives never reopen
        // on a prompt bump, unlike llm-sourced ones.
        let j = mk_judgement("negative", "human", Some("some_old_version"), None);
        assert_eq!(gate_pair(false, Some(&j), RESOLVE_PROMPT_VERSION), GateOutcome::Suppress);
        let j2 = mk_judgement("negative", "human", None, None);
        assert_eq!(gate_pair(false, Some(&j2), RESOLVE_PROMPT_VERSION), GateOutcome::Suppress);
    }

    #[test]
    fn gate_pair_llm_negative_suppresses_while_prompt_version_matches() {
        let j = mk_judgement("negative", "llm", Some(RESOLVE_PROMPT_VERSION), None);
        assert_eq!(gate_pair(false, Some(&j), RESOLVE_PROMPT_VERSION), GateOutcome::Suppress);
    }

    #[test]
    fn gate_pair_llm_negative_reopens_on_prompt_bump() {
        // A prompt-version mismatch (including None, the pre-PR2 case where
        // nothing stamped a version) reopens re-adjudication — 0112 precedent.
        let j = mk_judgement("negative", "llm", Some("resolve_entities_v0"), None);
        assert_eq!(gate_pair(false, Some(&j), RESOLVE_PROMPT_VERSION), GateOutcome::ReAdjudicate);
        let j2 = mk_judgement("negative", "llm", None, None);
        assert_eq!(gate_pair(false, Some(&j2), RESOLVE_PROMPT_VERSION), GateOutcome::ReAdjudicate);
    }

    #[test]
    fn gate_pair_negative_identifier_auto_source_follows_the_non_human_arm() {
        // Any non-human source (not just "llm") follows the reopens-on-bump
        // arm — the match only special-cases source == "human".
        let j = mk_judgement("negative", "identifier_auto", Some(RESOLVE_PROMPT_VERSION), None);
        assert_eq!(gate_pair(false, Some(&j), RESOLVE_PROMPT_VERSION), GateOutcome::Suppress);
        let j2 = mk_judgement("negative", "identifier_auto", Some("stale"), None);
        assert_eq!(gate_pair(false, Some(&j2), RESOLVE_PROMPT_VERSION), GateOutcome::ReAdjudicate);
    }

    #[test]
    fn gate_pair_unsure_suppresses_while_unexpired() {
        let future = chrono::Utc::now() + chrono::Duration::days(1);
        let j = mk_judgement("unsure", "llm", None, Some(future));
        assert_eq!(gate_pair(false, Some(&j), RESOLVE_PROMPT_VERSION), GateOutcome::Suppress);
    }

    #[test]
    fn gate_pair_unsure_with_no_expiry_suppresses() {
        // `expires_at.is_none_or(...)` — `None` counts as "unexpired".
        let j = mk_judgement("unsure", "llm", None, None);
        assert_eq!(gate_pair(false, Some(&j), RESOLVE_PROMPT_VERSION), GateOutcome::Suppress);
    }

    #[test]
    fn gate_pair_expired_unsure_readjudicates_defensive_arm() {
        // DEFENSIVE: `get_active_judgement`'s own "live" filter already
        // excludes an expired-but-unannulled UNSURE row from ever reaching
        // `active` in the real call path (it returns `None` instead) — this
        // test drives `gate_pair` directly with a hand-built expired row to
        // prove the function itself still gets this case right, independent
        // of that upstream filter.
        let past = chrono::Utc::now() - chrono::Duration::days(1);
        let j = mk_judgement("unsure", "llm", None, Some(past));
        assert_eq!(gate_pair(false, Some(&j), RESOLVE_PROMPT_VERSION), GateOutcome::ReAdjudicate);
    }

    #[test]
    fn gate_pair_unknown_verdict_string_readjudicates() {
        // Defensive `_ =>` arm — shouldn't be reachable given the DB CHECK
        // constraint, but must not panic or silently suppress.
        let j = mk_judgement("something_else", "llm", None, None);
        assert_eq!(gate_pair(false, Some(&j), RESOLVE_PROMPT_VERSION), GateOutcome::ReAdjudicate);
    }

    // ── §5.3 band_decision — exact boundaries ────────────────────────────────────

    #[test]
    fn band_decision_at_and_above_threshold_is_auto() {
        assert_eq!(band_decision(0.85, 0.5, 0.85), ConfidenceBand::Auto);
        assert_eq!(band_decision(0.86, 0.5, 0.85), ConfidenceBand::Auto);
        assert_eq!(band_decision(1.0, 0.5, 0.85), ConfidenceBand::Auto);
    }

    #[test]
    fn band_decision_just_below_threshold_is_unsure() {
        assert_eq!(band_decision(0.8499999, 0.5, 0.85), ConfidenceBand::Unsure);
    }

    #[test]
    fn band_decision_at_floor_is_unsure() {
        // floor is INCLUSIVE — `confidence >= floor`.
        assert_eq!(band_decision(0.5, 0.5, 0.85), ConfidenceBand::Unsure);
    }

    #[test]
    fn band_decision_just_below_floor_is_too_low_to_act() {
        assert_eq!(band_decision(0.4999999, 0.5, 0.85), ConfidenceBand::TooLowToAct);
    }

    #[test]
    fn band_decision_well_below_floor_is_too_low_to_act() {
        assert_eq!(band_decision(0.0, 0.5, 0.85), ConfidenceBand::TooLowToAct);
    }

    #[test]
    fn band_decision_mid_band_is_unsure() {
        assert_eq!(band_decision(0.6, 0.5, 0.85), ConfidenceBand::Unsure);
        assert_eq!(band_decision(0.7, 0.5, 0.85), ConfidenceBand::Unsure);
    }

    #[test]
    fn band_decision_bands_purely_on_confidence_garbage_verdict_included() {
        // `band_decision` takes no verdict string at all — the raw
        // `"same"`-below-threshold / `"unsure"` / any-other-garbage string
        // all band identically on confidence alone (the caller preserves the
        // raw string separately, in `evidence.llm_verdict`). A "garbage"
        // verdict at auto-merge-level confidence still bands Unsure/Auto
        // exactly like "unsure" would — it can never auto-MERGE, because the
        // `"same"` arm that drives merging is string-matched upstream of
        // this function ever being called.
        assert_eq!(band_decision(0.9, 0.5, 0.85), ConfidenceBand::Auto);
        assert_eq!(band_decision(0.6, 0.5, 0.85), ConfidenceBand::Unsure);
        assert_eq!(band_decision(0.1, 0.5, 0.85), ConfidenceBand::TooLowToAct);
    }

    // ── F10: unsure_judgement_ttl guard ──────────────────────────────────────

    #[test]
    fn unsure_judgement_ttl_passes_through_a_sane_positive_value() {
        assert_eq!(unsure_judgement_ttl(45), chrono::Duration::days(45));
        assert_eq!(unsure_judgement_ttl(1), chrono::Duration::days(1));
    }

    #[test]
    fn unsure_judgement_ttl_falls_back_on_non_positive_values() {
        assert_eq!(
            unsure_judgement_ttl(0),
            chrono::Duration::days(model_config::DEFAULT_UNSURE_DISMISSAL_TTL_DAYS)
        );
        assert_eq!(
            unsure_judgement_ttl(-30),
            chrono::Duration::days(model_config::DEFAULT_UNSURE_DISMISSAL_TTL_DAYS)
        );
    }

    #[test]
    fn unsure_judgement_ttl_falls_back_when_chrono_duration_cannot_represent_it() {
        // The exact failure mode review finding #10 names: a garbage-large
        // configured value must fall back, not panic (`Duration::days` — the
        // non-`try_` constructor — panics on out-of-range input).
        assert_eq!(
            unsure_judgement_ttl(i64::MAX),
            chrono::Duration::days(model_config::DEFAULT_UNSURE_DISMISSAL_TTL_DAYS)
        );
    }
}
