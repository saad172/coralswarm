//! `atoms.title` generation sweep (migration 0097/0010, step 2 of per-coral
//! titles).
//!
//! Fills in [`crate::services::atom_title`]-generated headlines for every coral
//! that doesn't have one — or that only has the deterministic fallback
//! headline `routes::ingest` writes synchronously at insert time (see
//! [`TitleKind`] below) — so the read-side [`crate::services::atom_title::
//! display_title`] guarantee is backed by an actual LLM headline wherever
//! possible, not just a non-empty string. See that module for WHY titles
//! exist (date-range recall answers from ~10-word headlines rather than
//! bodies, so it can cover every coral in a range instead of a sampled
//! subset) and why they are per-coral rather than reusing
//! `coding_sessions.title` (notes, meeting transcripts, email, Slack and
//! Teams have no session).
//!
//! # Tenant loop (title-always-present PR, 2026-09-09)
//!
//! This worker used to own a single loop against the control-plane `pool`
//! ONLY (`workers::atom_title_backfill::run`, spawned unconditionally in
//! `lib.rs`) — so no org tenant DB ever got a `BackfillAtomTitles` job, ever:
//! verified against production 2026-09-09, zero rows in the tenant `jobs`
//! table for this kind, across the tenant's whole history. It now joins
//! `workers::tenant_harness::run_multi_tenant` as a queue worker
//! (`HarnessConfig::queue_worker`), the same conversion `workers::
//! slack_name_backfill` went through (see that module's doc for the seed →
//! per-account fan-out shape this worker copies almost verbatim) — the
//! harness owns the loop, the empty-poll sleep, per-tenant error backoff, and
//! shutdown; this file owns exactly one [`tick`] per (worker, tenant) pass.
//!
//! # R2-backed bodies (the second production gap)
//!
//! The candidate predicate used to be `title IS NULL AND content_inline IS
//! NOT NULL` — but R2 has been on in prod since 2026-08-04, so
//! `content_inline` is NULL for essentially every atom ingested since (0 of
//! 13,367 atoms in the verified tenant carry an inline body). The predicate
//! now also admits `content_blob_key IS NOT NULL`, and [`run_batch`] fetches
//! an R2-only body via the account's DEK before titling — the exact fetch
//! `workers::extract::fetch_r2_only_content` already implements for the
//! extraction pipeline, reused here rather than reimplemented, with one
//! addition: [`fetch_r2_body`] asserts the blob key's own org segment matches
//! this tick's tenant BEFORE ever calling it (`coralswarm_core::r2::
//! assert_blob_key_org_scope`), mirroring `routes::atoms::get_atom`'s
//! deferred-assert wiring — a worker sweep has no per-request org context to
//! lean on, so this check is this file's own responsibility, not inherited
//! for free the way a route's `Tenant` extractor gets it.
//!
//! # Permanently unfetchable R2 candidates (independent review finding M4)
//!
//! A `content_blob_key` that fails `assert_blob_key_org_scope` can never
//! resolve — the key's own org segment permanently disagrees with this
//! tenant, no matter how many passes retry it. The concrete production case:
//! 3,129 atoms `workers::promote`'s `copy_atoms_to_graph`/`_scoped` copied
//! with a 2-segment blob key before that copy path carried the org segment
//! through. Left undetected, every pass re-examines them, tries and fails
//! the same local (no-network) assertion, and reports the batch
//! `NothingToDo` even though real candidates were seen — indistinguishable
//! from a genuinely idle tenant on any dashboard reading `jobs.outcome`.
//!
//! [`run_batch`] now tells terminal apart from transient
//! ([`R2FetchOutcome`]) and: (1) logs ONE aggregated `connector_health =
//! true` line per BATCH (not two lines per atom, forever — see
//! [`BatchOutcome::fetch_failed`]/[`BatchOutcome::fetch_failed_terminal`]);
//! (2) stamps a terminal candidate with a deterministic placeholder title
//! and blanks its unusable `content_blob_key` in the same pass
//! ([`TERMINAL_UNFETCHABLE_SQL`]) — the write, not the log line, is what
//! actually removes it from [`CANDIDATE_SQL`]'s selection for good; and (3)
//! when a batch examines real candidates but titles NONE of them because
//! every fetch this pass was transient (a terminal one always titles at
//! least the placeholder rows), [`process_job`] reports the job through
//! `jobs::queue::could_not_do` instead of completing it `NothingToDo` — see
//! that call site's own comment for why the silent-`done` shape is exactly
//! the 2026-08-04 `ExtractRelationships` incident `jobs::queue`'s "Typed
//! completion outcomes" section documents.
//!
//! # `title_kind`: letting ingest title synchronously without freezing the sweep
//!
//! `routes::ingest` now writes a deterministic [`crate::services::atom_title::
//! fallback_title`] SYNCHRONOUSLY at insert time (the founder's "I need at
//! least a title always" requirement — no caller anywhere should ever have to
//! wait on this sweep for a non-empty title). That would have been a trap for
//! THIS sweep on its own: if the only candidate signal were `title IS NULL`,
//! every atom would satisfy it at ingest and never again, permanently
//! freezing every coral on its synchronous fallback title and never trying
//! the LLM. `atoms.title_kind` (migration `0212_atoms_title_kind` /
//! `0080_atoms_title_kind`) fixes that: the candidate predicate is now
//! `(title IS NULL OR title_kind = 'fallback')`, and every title this sweep
//! writes is stamped with which path produced it
//! ([`crate::services::atom_title::TitleKind`]) — so a fallback-titled atom
//! stays a candidate until it is upgraded, and an LLM-titled one never is
//! again.
//!
//! A row titled BEFORE this column existed has `title SET, title_kind
//! NULL` — that shape does NOT satisfy the predicate above (independent
//! review finding M1, proven by truth table on PG16: neither `title IS
//! NULL` nor `title_kind = 'fallback'` holds), so left alone every legacy
//! fallback-titled row would be silently excluded and frozen on its
//! pre-column title forever. `0212`/`0080`'s one-shot backfill (`UPDATE
//! atoms SET title_kind = 'fallback' WHERE title IS NOT NULL AND title_kind
//! IS NULL`) closes that at migration time by normalizing every such row to
//! `'fallback'`, rather than widening this predicate to re-derive "NULL
//! means fallback" on every read.
//!
//! # Not only a backfill
//!
//! The candidate predicate is `title IS NULL OR title_kind = 'fallback'`, not
//! a fixed historical window, so the same sweep covers the existing backlog
//! AND anything ingested since. That is deliberate: it means new corals need
//! no enqueue-on-ingest hook to get titled, and re-inserting the seed is
//! always safe — an already-LLM-titled coral is never a candidate, so a
//! redundant run costs one empty batch per account.
//!
//! **That only held while a chain was in flight, and it silently stopped
//! holding.** A child enqueues its continuation only `if outcome.batch_full`, so
//! the moment an account's candidates drop below one batch the chain terminates
//! by design — correctly, because a chain that re-enqueued forever would be a
//! busy loop. But nothing re-armed it. With the original operator-seed-only
//! model, every coral ingested after the last chain drained stayed untitled
//! indefinitely: observed in prod 2026-07-26 as 3875/3942 titled with all 67
//! stragglers created after the final job ran, and the whole of that day's
//! capture invisible to date-range recall, which answers from titles.
//!
//! [`maybe_auto_seed`] closes that: when a tick finds the queue EMPTY (i.e. no
//! chain is in flight FOR THIS TENANT) and [`AUTO_SEED_INTERVAL`] has elapsed
//! since this tenant was last seeded, it seeds itself. [`SeedClock`] is the
//! per-tenant clock this needs — a single global `next_seed_at` (the
//! pre-tenant-loop shape) would let one busy tenant's seeding cadence starve
//! every other tenant sharing the same deadline, or worse, let a fresh org
//! tenant inherit a consumer-plane deadline that has nothing to do with it.
//! The operator seed still works and is still the way to force an immediate
//! full pass on one tenant; it is just no longer the only thing that ever
//! starts one.
//!
//! # Shape: seed → per-account children → keyset continuations
//!
//! An operator (or the auto-seed) inserts ONE account-less seed onto a given
//! tenant's `jobs` table; the seed touches only the account roster for THAT
//! tenant (`accounts` on the consumer plane, the tenant's own singleton
//! account via `tenant_databases` on an org tenant — see
//! [`fan_out_per_account`]) and enqueues one child per account, onto the SAME
//! plane. Each child carries the RLS scope plus its own `after_atom_id`
//! keyset cursor, and enqueues its own continuation while its account still
//! has candidates. A continuation-enqueue failure FAILS the current job rather
//! than completing it, so the queue's bounded retry gets another chance —
//! dropping it silently would strand every coral beyond that cursor.
//!
//! # Why the LLM call is outside the transaction
//!
//! Each batch is read in one short scoped transaction, titled (and, for
//! R2-only atoms, fetched) with the transaction CLOSED, then written back in a
//! second short scoped transaction. Holding a pooled connection open across
//! per-atom network calls is the exact pattern `services::ask`'s RLS section
//! rejects (and PR #291 rejected for the workers): a batch of 25 titling
//! calls (plus, now, up to 25 R2 fetches) would pin a connection for seconds.
//! The cost of splitting is that a concurrent writer could title the same
//! coral between the read and the write — harmless, because the write is
//! `WHERE title IS NULL OR title_kind = 'fallback'`, so the loser of that race
//! simply updates nothing.
//!
//! # Egress
//!
//! Titling is a cloud LLM call. A missing LLM (an unconfigured
//! instance) is titled with `services::atom_title`'s deterministic fallback
//! (`TitleKind::Fallback`), so its corals still participate in date-range
//! recall and stay eligible for a later upgrade.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use sqlx::PgPool;
use uuid::Uuid;

use crate::jobs::queue::{self, JobRow};
use crate::jobs::JobKind;
use crate::llm::LlmProvider;
use crate::services::atom_title::{batch_request, fallback_for, parse_batch_titles, title_input, TitleKind};
use crate::store::derive_journal_excerpt;
use crate::tenancy::scope::begin_tenant_system_scoped_without_account;
use crate::tenancy::TenantId;
use crate::workers::embed::JobScope;
use crate::workers::tenant_harness::{Tick, WorkerTenant};
use coralswarm_core::r2::R2Client;

// ── Tuning constants ──────────────────────────────────────────────────────────

/// How many `BackfillAtomTitles` jobs to dequeue per poll cycle. One active
/// sweep per (tenant, account) at a time (each child enqueues its own single
/// continuation), so this stays small.
const JOB_BATCH_SIZE: usize = 1;

/// The tenant harness's empty-poll / idle-backoff floor for this worker —
/// fed to `HarnessConfig::queue_worker`, same role as every other queue
/// worker's `EMPTY_POLL_SLEEP`.
pub(crate) const EMPTY_POLL_SLEEP: Duration = Duration::from_millis(500);

/// How often an idle tenant re-seeds itself (see the module doc's "Not only a
/// backfill").
///
/// This is the worst-case delay between ingesting a coral and it becoming
/// eligible for an LLM upgrade attempt, for a tenant whose chain has fully
/// drained — it does NOT bound plain titling latency any more, now that
/// `routes::ingest` writes a synchronous fallback title on every insert. Cost
/// of a tighter interval: one candidate-less `SELECT` per (worker, account)
/// pair per tenant — no LLM call, since an empty batch titles nothing.
const AUTO_SEED_INTERVAL: Duration = Duration::from_secs(15 * 60);

/// How long to wait after a FAILED auto-seed enqueue before trying again, for
/// the SAME tenant. Without it a persistent insert-specific failure (a
/// constraint problem, a full disk) becomes a tight loop hammering that
/// tenant's Postgres and flooding logs on every idle tick.
const AUTO_SEED_RETRY_BACKOFF: Duration = Duration::from_secs(60);

/// Idempotency-key prefix for a tenant's self-seed. STABLE per tenant on
/// purpose: `queue::enqueue`'s `ON CONFLICT (idempotency_key) WHERE status =
/// 'pending' DO NOTHING` then permits at most ONE pending auto-seed at a time
/// per tenant, across every replica, while still allowing the next one once
/// the previous has completed.
const AUTO_SEED_IDEMPOTENCY_PREFIX: &str = "backfill_atom_titles:auto-seed";

/// Corals titled per job execution.
///
/// Far smaller than `content_tsv_backfill`'s 2000 because every row here costs
/// an LLM call (and, for an R2-only atom, a decrypt fetch too) — this bounds
/// the per-job API burst the way `slack_name_backfill` bounds its Slack burst.
const ATOM_BATCH_SIZE: i64 = 25;

/// How many titling calls (and, separately, R2 fetches) run concurrently
/// within one batch. Keeps a batch to roughly `ATOM_BATCH_SIZE /
/// TITLE_CONCURRENCY` sequential round-trips without letting a sweep
/// monopolise the provider's rate limit or R2's connection pool.
const TITLE_CONCURRENCY: usize = 5;

/// Delay before a continuation becomes eligible — spaces batches out so the
/// sweep neither monopolises the queue poller nor bursts the LLM provider.
const CONTINUATION_DELAY_SECS: i64 = 1;

// ── Managed enrichment (source-agnostic plan PR 7) ───────────────────────────

/// `ATOM_TITLES_ENABLED`: the titles' own kill switch (on unless `false`).
/// The shared `ENRICHMENT_PROCESSING_ENABLED` switch must also be on.
pub(crate) const ATOM_TITLES_SWITCH_ENV: &str = "ATOM_TITLES_ENABLED";
/// `ATOM_TITLES_DAILY_CALLS_PER_ACCOUNT`: titling calls (one per graph per
/// [`ATOM_BATCH_SIZE`]-coral batch) one account may open per UTC day. 100
/// calls title at most 2,500 corals a day; at ~$0.0008 a call that is at most
/// ~$0.08 per account per day, inside the shared $2 cap.
pub(crate) const ATOM_TITLES_DAILY_CALLS_ENV: &str = "ATOM_TITLES_DAILY_CALLS_PER_ACCOUNT";
pub(crate) const DEFAULT_ATOM_TITLES_DAILY_CALLS: i64 = 100;
/// Input kinds a titling call binds: every coral, plus one `title_batch`
/// marker whose hash is the exact prompt (and which the daily bound counts).
const INPUT_ATOM: &str = "atom";
pub(crate) const INPUT_BATCH: &str = "title_batch";
/// A batch held after it titled something resumes after this, on a fresh
/// row. A batch held with nothing titled goes back on the queue in place
/// under the shared retry rule (`jobs::queue::hold`) instead.
const HOLD_RETRY_SECS: i64 = 900;

// ── Per-tenant seed clock ───────────────────────────────────────────────────

/// Process-wide, per-tenant "when may this tenant next auto-seed" clock —
/// the tenant-loop analog of the pre-conversion worker's single global
/// `next_seed_at`. One instance is created in `lib.rs::run` and shared across
/// every [`tick`] call for this worker (all tenants, all polls) via the same
/// `Arc`-clone-into-closure pattern `workers::tenant_harness::TenantActivity`
/// uses.
///
/// A per-tenant map rather than one shared deadline: a single global clock
/// would let one tenant's seed (success or failure) reset the wait for every
/// OTHER tenant too, and a fresh org tenant added mid-run would inherit
/// whatever deadline the consumer plane happened to be on instead of seeding
/// immediately the way a genuinely fresh process does.
#[derive(Default)]
pub struct SeedClock {
    next_seed_at: DashMap<String, Instant>,
}

impl SeedClock {
    pub fn new() -> Arc<Self> {
        Arc::new(SeedClock::default())
    }

    fn get(&self, key: &str) -> Option<Instant> {
        self.next_seed_at.get(key).map(|v| *v.value())
    }

    fn set(&self, key: &str, at: Instant) {
        self.next_seed_at.insert(key.to_string(), at);
    }
}

/// Stable per-tenant key for [`SeedClock`] and the auto-seed idempotency key.
fn tenant_key(tenant: &TenantId) -> String {
    match tenant {
        TenantId::Consumer => "consumer".to_string(),
        TenantId::Org(org_id) => format!("org:{org_id}"),
    }
}

/// Has the clock-side gate opened for one tenant? `None` (nothing scheduled
/// yet — a fresh process, or a tenant this worker has never seeded) is always
/// yes.
///
/// A DEADLINE rather than a "last seeded" timestamp, so one mechanism carries
/// both cadences: a success schedules the next attempt an
/// [`AUTO_SEED_INTERVAL`] out, a failure only an [`AUTO_SEED_RETRY_BACKOFF`]
/// out. Split out from [`maybe_auto_seed`] so the gate is unit-testable
/// without a Postgres pool.
fn seed_gate_open(next_seed_at: Option<Instant>) -> bool {
    match next_seed_at {
        None => true,
        Some(at) => Instant::now() >= at,
    }
}

/// Is a titling chain already in flight FOR THIS TENANT'S pool?
///
/// An empty `dequeue` does NOT mean idle: it reports only what is CLAIMABLE,
/// and a job that has already been claimed is `running`, not `pending`.
/// Seeding on an empty dequeue alone would therefore fire straight into a
/// live chain — which the per-account `:seed` idempotency key would not stop,
/// because the running child is keyed by its CURSOR, not by `:seed`. On
/// error, reports `true` (assume busy): skipping a seed costs one interval of
/// staleness; issuing a duplicate one costs money.
async fn chain_in_flight(pool: &PgPool) -> bool {
    match sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (SELECT 1 FROM jobs \
          WHERE kind = 'BackfillAtomTitles' AND status IN ('pending', 'running'))",
    )
    .fetch_one(pool)
    .await
    {
        Ok(busy) => busy,
        Err(e) => {
            tracing::warn!(error = %e, "atom-title-backfill: in-flight check failed, assuming busy");
            true
        }
    }
}

/// Enqueue a fresh account-less seed onto `t.pool` if [`AUTO_SEED_INTERVAL`]
/// has elapsed for THIS tenant, updating [`SeedClock`] in place.
///
/// Failure is logged and swallowed rather than propagated: a seed that could
/// not be enqueued costs at most one interval of extra staleness for this
/// tenant, and the next idle tick retries. Contrast with the CONTINUATION
/// enqueue in [`process_job`], which FAILS its job on error — dropping a
/// continuation strands every coral past a cursor, whereas dropping a seed
/// strands nothing, because the following attempt starts from the beginning
/// again.
async fn maybe_auto_seed(t: &WorkerTenant, seed_clock: &SeedClock) {
    let key = tenant_key(&t.tenant);
    if !seed_gate_open(seed_clock.get(&key)) {
        return;
    }
    // Clock gate passed; now the cheap-but-not-free DB check, run at most
    // once per interval per tenant rather than on every idle tick.
    if chain_in_flight(&t.pool).await {
        return;
    }

    let idempotency = format!("{AUTO_SEED_IDEMPOTENCY_PREFIX}:{key}");
    match queue::enqueue(
        &t.pool,
        &JobKind::BackfillAtomTitles { after_atom_id: None },
        None,
        Some(&idempotency),
        0,
    )
    .await
    {
        // `None` = an auto-seed was already pending for this tenant (another
        // replica, or an operator, beat us to it). Still counts as seeded.
        Ok(id) => {
            tracing::info!(
                job_id = ?id,
                tenant = %key,
                "atom-title-backfill: auto-seed enqueued (no chain in flight)"
            );
            seed_clock.set(&key, Instant::now() + AUTO_SEED_INTERVAL);
        }
        Err(e) => {
            tracing::error!(
                error = %e,
                tenant = %key,
                backoff_secs = AUTO_SEED_RETRY_BACKOFF.as_secs(),
                "atom-title-backfill: auto-seed enqueue failed, backing off"
            );
            seed_clock.set(&key, Instant::now() + AUTO_SEED_RETRY_BACKOFF);
        }
    }
}

// ── Worker entry point ────────────────────────────────────────────────────────

/// One atom-title pass on one tenant's `(pool, store)` pair: dequeue up to
/// [`JOB_BATCH_SIZE`] `BackfillAtomTitles` jobs and process them, or —
/// finding none — maybe auto-seed this tenant (see the module doc). Driven by
/// `workers::tenant_harness::run_multi_tenant` as a queue worker
/// (`HarnessConfig::queue_worker`), which owns the loop, the empty-poll
/// sleep, per-tenant error backoff, and shutdown.
///
/// `control_pool` is the shared control/consumer pool — needed ONLY by the
/// egress-policy read (`account_prefs`, control-plane only) and the
/// account-less **seed** job's fan-out (see [`fan_out_per_account`]); every
/// other read/write in this worker runs on `t.pool`.
pub async fn tick(
    t: &WorkerTenant,
    control_pool: &PgPool,
    llm: Option<&dyn LlmProvider>,
    r2: Option<&R2Client>,
    seed_clock: &SeedClock,
) -> Result<Tick, String> {
    // Managed (source-agnostic plan PR 7): nothing is claimed or seeded unless
    // ENRICHMENT_PROCESSING_ENABLED=true, the global kill switch is off and
    // ATOM_TITLES_ENABLED is not off.
    if !crate::managed::enrichment::consumer_claim_allowed(ATOM_TITLES_SWITCH_ENV) {
        return Ok(Tick::idle());
    }

    let jobs = queue::dequeue(&t.pool, &["BackfillAtomTitles"], JOB_BATCH_SIZE)
        .await
        .map_err(|e| format!("dequeue failed: {e}"))?;

    if jobs.is_empty() {
        maybe_auto_seed(t, seed_clock).await;
        return Ok(Tick::idle());
    }

    let n = jobs.len();
    for job in jobs {
        process_job(&t.tenant, &t.pool, control_pool, llm, r2, job).await;
    }
    Ok(Tick::worked(n))
}

// ── Job processing ────────────────────────────────────────────────────────────

fn parse_after_atom_id(job: &JobRow) -> Result<Option<Uuid>, String> {
    let tagged = serde_json::json!({ "kind": job.kind, "payload": job.payload });
    let kind: JobKind = serde_json::from_value(tagged)
        .map_err(|e| format!("cannot deserialise JobKind from payload: {e}"))?;
    match kind {
        JobKind::BackfillAtomTitles { after_atom_id } => Ok(after_atom_id),
        other => Err(format!("unexpected job kind for atom-title sweep: {other:?}")),
    }
}

/// One coral awaiting a title (and possibly an excerpt).
struct Candidate {
    atom_id: Uuid,
    content_inline: Option<String>,
    content_blob_key: Option<String>,
    header_len: Option<i32>,
    /// Only computed for a candidate whose `excerpt` is currently NULL — see
    /// [`run_batch`]'s write step.
    needs_excerpt: bool,
    /// The ocean the coral is in: one managed call per graph in a batch.
    graph_id: Uuid,
    /// `atoms.content_revision` at read time, bound into the call's receipt.
    content_revision: i64,
}

/// [`CANDIDATE_SQL`]'s row.
type CandidateRow = (Uuid, Option<String>, Option<String>, Option<i32>, Option<String>, Uuid, i64);

/// What one candidate resolved to after content fetch — separate from
/// [`Candidate`] so a fetch failure (R2 unconfigured, DEK missing, blob
/// org-scope mismatch, network error) is representable without discarding the
/// atom_id: it stays a candidate for the next pass rather than silently
/// vanishing from the batch.
struct ResolvedCandidate {
    atom_id: Uuid,
    content: String,
    header_len: Option<i32>,
    needs_excerpt: bool,
    graph_id: Uuid,
    content_revision: i64,
}

/// Why a batch stopped before titling everything it could (nothing was sent
/// for the rest; they stay candidates).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Stop {
    /// The account's daily batch bound or the shared daily cap: resume after
    /// the UTC day turns.
    DailyBound,
    /// A managed hold (payer credits, access, switch): resume shortly.
    Hold(coralswarm_core::send_gate::HoldCode),
    /// Managed storage unavailable: resume shortly.
    Unavailable,
}

/// What a batch accomplished, for the cursor/continuation decision.
struct BatchOutcome {
    examined: usize,
    titled: usize,
    /// R2 fetch attempts that did not produce a body this pass — the SUM of
    /// [`Self::fetch_failed_terminal`] (permanently unfetchable) and
    /// transient ones (network/DEK/R2-config, which may succeed on a later
    /// pass). Independent review finding M4.
    fetch_failed: usize,
    /// Of `fetch_failed`, how many were `assert_blob_key_org_scope` failures
    /// — a blob key whose own org segment can never match this tenant, no
    /// matter how many times it is retried. [`run_batch`] stamps these with
    /// a deterministic terminal title and clears `content_blob_key` in the
    /// SAME pass, so they do not recur in `fetch_failed` on a later batch.
    fetch_failed_terminal: usize,
    /// The batch came back full, so more candidates probably remain.
    batch_full: bool,
    /// Highest `atom_id` examined — the next cursor. `None` for an empty batch.
    last_atom_id: Option<Uuid>,
    /// Set when managed admission stopped the batch early: the job resumes
    /// from the SAME cursor later instead of advancing past untitled corals.
    stop: Option<Stop>,
}

/// `pub` so integration tests can drive one dequeued job end-to-end against a
/// real Postgres, including the scoping guards, without the worker loop.
pub async fn process_job(
    tenant: &TenantId,
    pool: &PgPool,
    control_pool: &PgPool,
    llm: Option<&dyn LlmProvider>,
    r2: Option<&R2Client>,
    job: JobRow,
) {
    let after_atom_id = match parse_after_atom_id(&job) {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(job_id = %job.id, error = %e, "atom-title-backfill: malformed job payload");
            if let Err(qe) = queue::fail(pool, job.id, &e).await {
                tracing::error!(error = %qe, job_id = %job.id, "failed to mark malformed job as failed");
            }
            return;
        }
    };

    // Account-less job ⇒ SEED: fan out one child per account on THIS tenant's
    // plane. The seed itself touches no RLS-covered content table.
    // Switched off (or claimed just before a flip-off): spend nothing, write
    // nothing. A child hands its cursor back for a later pass; a seed fans
    // out nothing (the auto-seed starts a fresh sweep once switched on).
    if !crate::managed::enrichment::consumer_claim_allowed(ATOM_TITLES_SWITCH_ENV) {
        match job.account_id {
            Some(_) => hold_in_place(pool, &job, "switched_off", None).await,
            None => {
                if let Err(e) = queue::complete(pool, job.id, queue::JobOutcome::NothingToDo).await {
                    tracing::error!(error = %e, job_id = %job.id, "atom-title-backfill: failed to complete a switched-off seed");
                }
            }
        }
        return;
    }
    let Some(account_id) = job.account_id else {
        fan_out_per_account(pool, tenant, control_pool, job.id, after_atom_id).await;
        return;
    };
    // Settle any receipt an earlier crashed attempt of this job left behind
    // before opening a new call (managed::enrichment step 1).
    let scope = JobScope::for_job(tenant, pool, account_id);
    if let Err(deny) = crate::managed::enrichment::recover_job(&scope, control_pool, tenant, account_id, job.id).await {
        tracing::warn!(job_id = %job.id, error = %deny, "atom-title-backfill: managed receipt recovery unavailable — retrying the job");
        if let Err(e) = queue::fail(pool, job.id, "managed enrichment recovery unavailable").await {
            tracing::error!(error = %e, job_id = %job.id, "failed to mark atom-title-backfill job failed");
        }
        return;
    }
    let managed_job = crate::managed::enrichment::EnrichmentJob { id: job.id, kind: job.kind.clone(), attempts: job.attempts };

    match run_batch(pool, control_pool, tenant, llm, r2, account_id, Some(managed_job), after_atom_id).await {
        Ok(outcome) if outcome.stop.is_some() => {
            // Managed admission stopped the batch: resume from the SAME
            // cursor (the untitled corals stay candidates) once the bound or
            // hold can have cleared. With nothing titled the job is held in
            // place under the shared retry rule, so a hold that cannot clear
            // dead-letters after a few tries (the next auto-seed restarts the
            // sweep); with progress it resumes on a fresh row.
            let stop = outcome.stop.expect("checked");
            let next_day = crate::managed::enrichment::secs_until_next_utc_day(chrono::Utc::now());
            let (code, delay) = match stop {
                Stop::DailyBound => ("daily_bound", next_day),
                Stop::Hold(code) => (code.code(), HOLD_RETRY_SECS),
                Stop::Unavailable => (crate::jobs::queue::MANAGED_UNAVAILABLE, HOLD_RETRY_SECS),
            };
            tracing::info!(job_id = %job.id, %account_id, ?stop, titled = outcome.titled,
                "atom-title-backfill: batch stopped by managed admission; resuming from the same cursor later");
            if outcome.titled == 0 {
                hold_in_place(pool, &job, code, matches!(stop, Stop::DailyBound).then_some(next_day)).await;
            } else {
                resume_later(pool, &job, account_id, after_atom_id, delay, "managed admission").await;
            }
        }
        Ok(outcome) => {
            tracing::info!(
                job_id = %job.id,
                account_id = %account_id,
                examined = outcome.examined,
                titled = outcome.titled,
                fetch_failed = outcome.fetch_failed,
                fetch_failed_terminal = outcome.fetch_failed_terminal,
                batch_full = outcome.batch_full,
                "atom-title-backfill: batch complete"
            );

            if outcome.batch_full {
                if let Some(cursor) = outcome.last_atom_id {
                    let idempotency = format!("backfill_atom_titles:{account_id}:{cursor}");
                    if let Err(e) = queue::enqueue(
                        pool,
                        &JobKind::BackfillAtomTitles {
                            after_atom_id: Some(cursor),
                        },
                        Some(account_id),
                        Some(&idempotency),
                        CONTINUATION_DELAY_SECS,
                    )
                    .await
                    {
                        // Dropping this silently would strand every coral past
                        // the cursor even though this batch succeeded. Fail the
                        // job so its bounded retry re-attempts; re-running a
                        // batch is harmless because the write is
                        // `WHERE title IS NULL OR title_kind = 'fallback'`.
                        tracing::error!(job_id = %job.id, error = %e, "atom-title-backfill: failed to enqueue continuation — failing this job to retry");
                        if let Err(qe) = queue::fail(pool, job.id, &e.to_string()).await {
                            tracing::error!(error = %qe, job_id = %job.id, "failed to mark job failed after continuation enqueue failure");
                        }
                        return;
                    }
                }
            }

            // Independent review finding M4: `examined > 0 && titled == 0 &&
            // fetch_failed > 0` means this batch found real candidates but
            // titled NONE of them, and the reason is R2 fetch failures — a
            // TRANSIENT-only outcome (a terminal one always titles at least
            // the stamped-placeholder rows via `terminal_written`, so it
            // never reaches this branch with `titled == 0`). Completing that
            // as `NothingToDo` would be the exact silent-success shape
            // `jobs::queue`'s "Typed completion outcomes" section exists to
            // rule out: the row stays `done`, `chain_in_flight` sees no
            // pending/running job for this tenant, and
            // `maybe_auto_seed` re-walks the SAME still-failing candidates
            // on its next interval, forever, with nothing ever going on the
            // retry ladder or getting louder. Routing through
            // `could_not_do(..., Transient, ...)` instead keeps the job
            // `pending` on `fail`'s backoff ladder — which is what
            // `chain_in_flight` actually checks — so a new auto-seed does
            // NOT fire while this account's fetch problem is still live, and
            // three bumps without success promotes it to `dead` (visible on
            // an operator dashboard) rather than staying invisible behind a
            // green `done`/`nothing_to_do` row forever.
            if outcome.titled == 0 && outcome.examined > 0 && outcome.fetch_failed > 0 {
                let msg = format!(
                    "batch examined {} candidate(s) but titled none — {} R2 fetch failure(s) \
                     ({} terminal, already handled separately)",
                    outcome.examined, outcome.fetch_failed, outcome.fetch_failed_terminal
                );
                if let Err(e) =
                    queue::could_not_do(pool, job.id, queue::FailureClass::Transient, &msg).await
                {
                    tracing::error!(error = %e, job_id = %job.id, "atom-title-backfill: failed to mark job could-not-do after all-transient fetch failures");
                }
                return;
            }

            // `outcome.titled == 0` here means this batch's candidate window
            // came back genuinely empty — a legitimate no-op for a sweep
            // worker, not a failure to act on real candidates (the
            // fetch-failure case above already returned).
            let job_outcome =
                if outcome.titled > 0 { queue::JobOutcome::DidWork } else { queue::JobOutcome::NothingToDo };
            if let Err(e) = queue::complete(pool, job.id, job_outcome).await {
                tracing::error!(error = %e, job_id = %job.id, "atom-title-backfill: failed to complete job");
            }
        }
        Err(e) => {
            tracing::error!(job_id = %job.id, error = %e, "atom-title-backfill: batch failed — failing job for retry");
            if let Err(qe) = queue::fail(pool, job.id, &e).await {
                tracing::error!(error = %qe, job_id = %job.id, "failed to mark atom-title-backfill job failed");
            }
        }
    }
}

/// Complete `job` and enqueue the same cursor again after `delay_secs`, so a
/// held or bounded batch resumes where it stopped. A failed re-enqueue fails
/// the job instead, so the queue's bounded retry re-attempts it.
/// Hold the claimed job in place: until a known time (`until`, the next UTC
/// day), else under the shared retry rule (`jobs::queue::hold`). Same row,
/// same cursor, no attempt charged.
async fn hold_in_place(pool: &PgPool, job: &JobRow, code: &str, until: Option<i64>) {
    let held = match until {
        Some(secs) => queue::hold_until(pool, job.id, code, secs).await,
        None => queue::hold(pool, job.id, code).await,
    };
    match held {
        Ok(held) => tracing::info!(job_id = %job.id, code, ?held, "atom-title-backfill: batch held in place"),
        Err(e) => tracing::error!(job_id = %job.id, error = %e, code, "atom-title-backfill: failed to hold the batch; the lease reaper retries it"),
    }
}

async fn resume_later(pool: &PgPool, job: &JobRow, account_id: Uuid, cursor: Option<Uuid>, delay_secs: i64, why: &str) {
    let idempotency = match cursor {
        Some(cursor) => format!("backfill_atom_titles:{account_id}:{cursor}"),
        None => format!("backfill_atom_titles:{account_id}:start"),
    };
    if let Err(e) = queue::complete(pool, job.id, queue::JobOutcome::NothingToDo).await {
        tracing::error!(error = %e, job_id = %job.id, "atom-title-backfill: failed to complete held job");
        return;
    }
    if let Err(e) = queue::enqueue(
        pool,
        &JobKind::BackfillAtomTitles { after_atom_id: cursor },
        Some(account_id),
        Some(&idempotency),
        delay_secs,
    )
    .await
    {
        tracing::error!(job_id = %job.id, error = %e, why, "atom-title-backfill: failed to resume the held batch; the next auto-seed restarts the sweep");
    }
}

/// SEED handling: resolve which account(s) this tenant's sweep should fan out
/// to and enqueue one per-account child onto `pool` — the SAME plane the seed
/// was dequeued from. Any failure fails the seed for a bounded queue retry;
/// per-account idempotency keys make a retried seed re-enqueue only the
/// children that didn't make it. Same shape as
/// `workers::slack_name_backfill::fan_out_per_account`.
///
/// The account roster is plane-dependent: on the consumer plane it is every
/// row in `accounts` (control table, needs no scope); on an org tenant there
/// is exactly ONE account (that tenant's own), resolved from the control
/// plane's `tenant_databases` routing row.
async fn fan_out_per_account(
    pool: &PgPool,
    tenant: &TenantId,
    control_pool: &PgPool,
    job_id: Uuid,
    after_atom_id: Option<Uuid>,
) {
    let fail_seed = |msg: String| async move {
        tracing::error!(job_id = %job_id, error = %msg, "atom-title-backfill: seed fan-out failed — failing seed job to retry");
        if let Err(qe) = queue::fail(pool, job_id, &msg).await {
            tracing::error!(error = %qe, job_id = %job_id, "failed to mark seed job failed");
        }
    };

    let account_ids: Vec<Uuid> = match tenant {
        TenantId::Consumer => {
            match sqlx::query_scalar("SELECT id FROM accounts ORDER BY id")
                .fetch_all(control_pool)
                .await
            {
                Ok(ids) => ids,
                Err(e) => {
                    fail_seed(format!("account enumeration failed: {e}")).await;
                    return;
                }
            }
        }
        TenantId::Org(org_id) => {
            match sqlx::query_scalar::<_, Uuid>(
                "SELECT account_id FROM tenant_databases WHERE clerk_org_id = $1",
            )
            .bind(org_id.as_str())
            .fetch_optional(control_pool)
            .await
            {
                Ok(Some(id)) => vec![id],
                Ok(None) => {
                    // Defensive only: the harness never schedules a tenant
                    // without an active routing row.
                    fail_seed(format!(
                        "account resolution failed: no tenant_databases row for org {org_id}"
                    ))
                    .await;
                    return;
                }
                Err(e) => {
                    fail_seed(format!("account resolution failed: {e}")).await;
                    return;
                }
            }
        }
    };

    for account_id in &account_ids {
        let idempotency = format!("backfill_atom_titles:{account_id}:seed");
        if let Err(e) = queue::enqueue(
            pool,
            &JobKind::BackfillAtomTitles { after_atom_id },
            Some(*account_id),
            Some(&idempotency),
            0,
        )
        .await
        {
            fail_seed(format!("child enqueue failed for account {account_id}: {e}")).await;
            return;
        }
    }

    tracing::info!(job_id = %job_id, accounts = account_ids.len(), "atom-title-backfill: seed fanned out");
    if let Err(e) = queue::complete(pool, job_id, queue::JobOutcome::DidWork).await {
        tracing::error!(error = %e, job_id = %job_id, "atom-title-backfill: failed to complete seed job");
    }
}

/// A deterministic label stamped on a candidate whose `content_blob_key` can
/// never be resolved (independent review finding M4) — no body was ever
/// available to derive a real headline from, so this is a fixed string, not
/// a [`fallback_title`]-derived one. Kept intentionally generic: the atom's
/// own `source_type`/`timestamp` aren't in [`CANDIDATE_SQL`]'s projection
/// (widening it is exactly the "no schema/query-shape change" this fix
/// avoids), and a coral-detail page still has `source_type`/`occurred_at` of
/// its own to render alongside this.
const UNFETCHABLE_TERMINAL_TITLE: &str = "Archived coral (content unavailable)";

/// What one R2 fetch attempt produced — distinguishes a TERMINAL failure
/// (this exact blob key can never succeed, no matter how many passes retry
/// it) from a transient one (may succeed later), so [`run_batch`] can decide
/// per-atom rather than lump every fetch problem into one undifferentiated
/// "warn and move on" bucket (independent review finding M4).
enum R2FetchOutcome {
    /// Fetched and decrypted to a non-empty body.
    Body(String),
    /// Fetched, but decrypted to nothing — not a failure, same as an
    /// inline-empty atom.
    Empty,
    /// `assert_blob_key_org_scope` rejected this blob key BEFORE any network
    /// call — the key's own org segment can never match this tenant. Retrying
    /// hits the identical wall every time.
    Terminal(String),
    /// [`crate::workers::extract::fetch_r2_only_content`] failed for a
    /// reason that may not recur (R2 unconfigured, DEK missing, a network
    /// error) — worth retrying on a future pass.
    Transient(String),
}

/// Fetch an R2-only candidate's plaintext body: asserts the blob key's own
/// org segment matches `tenant` (org-tenancy PR 4.1's deferred-assert
/// pattern, mirroring `routes::atoms::get_atom` — see this module's doc
/// comment) BEFORE ever calling
/// [`crate::workers::extract::fetch_r2_only_content`], which does the actual
/// DEK resolution + R2 GET. A scope mismatch is refused loudly (never
/// fetched) rather than silently skipped, same "never crossing to another
/// org's/plane's object" contract the route enforces — and, unlike a
/// transient fetch error, reported back as [`R2FetchOutcome::Terminal`] so
/// the caller can stop retrying it instead of hitting the same local,
/// no-network assertion failure every pass forever (independent review
/// finding M4; the 3,129 `copy_atoms_to_graph`-promoted atoms with 2-segment
/// blob keys are the concrete production case).
async fn fetch_r2_body(
    tenant: &TenantId,
    pool: &PgPool,
    r2: Option<&R2Client>,
    account_id: Uuid,
    atom_id: Uuid,
    blob_key: &str,
) -> R2FetchOutcome {
    let expected_org = match tenant {
        TenantId::Org(org_id) => Some(org_id.as_str()),
        TenantId::Consumer => None,
    };
    if let Err(e) = coralswarm_core::r2::assert_blob_key_org_scope(blob_key, expected_org) {
        // Deliberately NOT logged per-atom at `error!` here any more — see
        // `run_batch`'s single aggregated `connector_health` line, which
        // replaces this file's old "two log lines per atom, every pass,
        // forever" shape (independent review finding M4). `debug!` keeps a
        // trace available without repeating at alert volume.
        tracing::debug!(
            %atom_id,
            blob_key = %blob_key,
            expected_org = ?expected_org,
            error = %e,
            "atom-title-backfill: blob_key org-scope mismatch — terminal, will not retry"
        );
        return R2FetchOutcome::Terminal(format!("blob_key org-scope mismatch: {e}"));
    }
    match crate::workers::extract::fetch_r2_only_content(tenant, pool, r2, account_id, blob_key).await
    {
        Ok(Some(text)) => R2FetchOutcome::Body(text),
        Ok(None) => R2FetchOutcome::Empty,
        Err(e) => R2FetchOutcome::Transient(e),
    }
}

/// One candidate's write: a title and its kind, and/or a missing excerpt.
struct Write {
    title: Option<String>,
    title_kind: Option<&'static str>,
    excerpt: Option<String>,
}

/// Run [`WRITE_SQL`] for `writes` inside `tx`. Returns rows updated.
async fn write_titles(
    tx: &mut sqlx::Transaction<'static, sqlx::Postgres>,
    writes: &[(Uuid, Write)],
) -> Result<u64, String> {
    if writes.is_empty() {
        return Ok(0);
    }
    let ids: Vec<Uuid> = writes.iter().map(|(id, _)| *id).collect();
    let titles: Vec<Option<String>> = writes.iter().map(|(_, w)| w.title.clone()).collect();
    let kinds: Vec<Option<String>> = writes.iter().map(|(_, w)| w.title_kind.map(str::to_string)).collect();
    let excerpts: Vec<Option<String>> = writes.iter().map(|(_, w)| w.excerpt.clone()).collect();
    Ok(sqlx::query(WRITE_SQL)
        .bind(&ids)
        .bind(&titles)
        .bind(&kinds)
        .bind(&excerpts)
        .execute(&mut **tx)
        .await
        .map_err(|e| format!("title write failed: {e}"))?
        .rows_affected())
}

/// One plane transaction for this account (account scope on the consumer
/// plane, the reviewed tenant system actor on an org tenant — `atoms` is
/// `member_ocean_isolation`-covered, so the transaction-local system actor is
/// what makes these reads and writes correct under the non-exempt role, not
/// merely under today's owner connection).
async fn begin_plane(scope: &JobScope<'_>) -> Result<sqlx::Transaction<'static, sqlx::Postgres>, String> {
    match scope {
        JobScope::Consumer { pool, account_id } => crate::tenancy::scope::begin_account_scoped(pool, *account_id)
            .await
            .map_err(|e| format!("scoped begin failed: {e}")),
        JobScope::Org(pool) => begin_tenant_system_scoped_without_account(pool)
            .await
            .map_err(|e| format!("scoped begin (org) failed: {e}")),
    }
}

/// The managed context of one title batch: the job's fence, the control pool
/// the gate reserves on, and the account's remaining batch calls today.
struct TitleManaged<'a> {
    control: &'a PgPool,
    job: Option<crate::managed::enrichment::EnrichmentJob>,
    calls_left: i64,
}

/// Title one graph's resolved candidates in ONE managed call and publish
/// only when every bound atom is unchanged. Returns rows written, or the hold
/// that stopped it (nothing sent, nothing written).
#[allow(clippy::too_many_arguments)]
async fn title_graph_batch(
    scope: &JobScope<'_>,
    tenant: &TenantId,
    managed: &mut TitleManaged<'_>,
    llm: &dyn LlmProvider,
    model: &str,
    account_id: Uuid,
    graph_id: Uuid,
    group: &[ResolvedCandidate],
) -> Result<Result<u64, Stop>, String> {
    use coralswarm_core::send_gate::{Deny, HoldCode};
    use crate::managed::enrichment::{EnrichmentInput, EnrichmentWork, InputCheck};
    // Corals with nothing titleable get only their missing excerpt.
    let mut titleable: Vec<(&ResolvedCandidate, String)> = Vec::new();
    let mut writes: Vec<(Uuid, Write)> = Vec::new();
    for c in group {
        match title_input(&c.content, c.header_len) {
            Some(body) => titleable.push((c, body)),
            None => {
                if let Some(excerpt) = c.needs_excerpt.then(|| derive_journal_excerpt(Some(&c.content), c.header_len)).flatten() {
                    writes.push((c.atom_id, Write { title: None, title_kind: None, excerpt: Some(excerpt) }));
                }
            }
        }
    }
    if titleable.is_empty() {
        let mut tx = begin_plane(scope).await?;
        let n = write_titles(&mut tx, &writes).await?;
        tx.commit().await.map_err(|e| format!("title write commit failed: {e}"))?;
        return Ok(Ok(n));
    }
    if managed.calls_left <= 0 {
        return Ok(Err(Stop::DailyBound));
    }
    let bodies: Vec<String> = titleable.iter().map(|(_, body)| body.clone()).collect();
    let request = batch_request(model, &bodies);
    let mut ids: Vec<Uuid> = titleable.iter().map(|(c, _)| c.atom_id).collect();
    ids.sort();
    let marker_id = {
        use sha2::Digest as _;
        let digest = sha2::Sha256::digest(format!("title_batch|{graph_id}|{ids:?}").as_bytes());
        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(&digest[..16]);
        Uuid::from_bytes(bytes)
    };
    let mut inputs: Vec<EnrichmentInput> = titleable
        .iter()
        .map(|(c, body)| EnrichmentInput::new(INPUT_ATOM, c.atom_id, c.content_revision.max(0), body.as_bytes()))
        .collect();
    inputs.push(EnrichmentInput::new(INPUT_BATCH, marker_id, 0, request.messages[0].content.as_bytes()));
    let work = EnrichmentWork::new(account_id, graph_id, managed.job.clone(), None, inputs).map_err(|e| e.to_string())?;
    let principal = match crate::managed::enrichment::select_principal(scope, managed.control, tenant, &work).await {
        Ok(principal) => principal,
        Err(Deny::Hold(code)) => return Ok(Err(Stop::Hold(code))),
        Err(_) => return Ok(Err(Stop::Unavailable)),
    };
    managed.calls_left -= 1;
    let mut usage = crate::llm::usage::UsageContext::new(scope_pool(scope).clone(), scope.usage_account_id(), Some(graph_id))
        .with_attributed_user(principal);
    if let Some(job) = &managed.job {
        usage = usage.with_source_job(job.id);
    }
    let n = titleable.len();
    let titles = match crate::managed::enrichment::complete(
        llm, scope, managed.control, tenant, &work, principal, request, &usage,
        crate::services::atom_title::usage_task(),
        |response| parse_batch_titles(&response.content, n),
    )
    .await
    {
        Ok((_, titles)) => titles,
        Err(crate::llm::LlmError::Managed(Deny::Hold(HoldCode::DailyBudgetExhausted))) => return Ok(Err(Stop::DailyBound)),
        Err(crate::llm::LlmError::Managed(Deny::Hold(code))) => return Ok(Err(Stop::Hold(code))),
        Err(crate::llm::LlmError::Managed(_)) => return Ok(Err(Stop::Unavailable)),
        // A provider failure leaves every coral a candidate for a later pass.
        Err(e) => return Err(format!("title batch call failed: {e}")),
    };
    for ((c, _), title) in titleable.iter().zip(titles) {
        let excerpt = c.needs_excerpt.then(|| derive_journal_excerpt(Some(&c.content), c.header_len)).flatten();
        let (title, kind) = match title {
            Some(title) => (Some(title), Some(TitleKind::Llm.as_db_str())),
            // Asked and declined: keep the deterministic headline and stop
            // re-paying for the same answer.
            None => match fallback_for(&c.content, c.header_len) {
                Some(title) => (Some(title), Some(TitleKind::Declined.as_db_str())),
                None => (None, None),
            },
        };
        if title.is_some() || excerpt.is_some() {
            writes.push((c.atom_id, Write { title, title_kind: kind, excerpt }));
        }
    }

    // Publish: lock the bound atoms, re-verify them against the receipt, and
    // write only when every one is unchanged (`content_revision` moves on any
    // content, timestamp or title change).
    let mut tx = begin_plane(scope).await?;
    let current_revisions: HashMap<Uuid, i64> = sqlx::query_as::<_, (Uuid, i64)>(
        "SELECT atom_id, content_revision FROM atoms WHERE atom_id = ANY($1) ORDER BY atom_id FOR UPDATE",
    )
    .bind(&ids)
    .fetch_all(&mut *tx)
    .await
    .map_err(|e| format!("title publish re-read failed: {e}"))?
    .into_iter()
    .collect();
    let current: Vec<EnrichmentInput> = work
        .inputs()
        .iter()
        .filter_map(|input| {
            if input.kind != INPUT_ATOM {
                return Some(input.clone());
            }
            let revision = *current_revisions.get(&input.id)?;
            let mut now = input.clone();
            now.content_revision = revision.max(0);
            Some(now)
        })
        .collect();
    match crate::managed::enrichment::verify_inputs_locked(&mut tx, &work, &current).await {
        Ok(InputCheck::Fresh(verified)) => {
            let n = write_titles(&mut tx, &writes).await?;
            verified.mark_written(&mut tx).await.map_err(|e| format!("managed publish unavailable: {e}"))?;
            tx.commit().await.map_err(|e| format!("title write commit failed: {e}"))?;
            Ok(Ok(n))
        }
        Ok(InputCheck::Stale) => {
            tx.commit().await.map_err(|e| e.to_string())?;
            tracing::info!(%graph_id, atoms = n, "atom-title-backfill: a coral changed during the call — published nothing (discarded_stale)");
            Ok(Ok(0))
        }
        Ok(InputCheck::AlreadyWritten) => {
            tx.rollback().await.ok();
            Ok(Ok(0))
        }
        Err(_) => {
            tx.rollback().await.ok();
            Ok(Err(Stop::Unavailable))
        }
    }
}

fn scope_pool<'a>(scope: &JobScope<'a>) -> &'a PgPool {
    match scope {
        JobScope::Consumer { pool, .. } => pool,
        JobScope::Org(pool) => pool,
    }
}

/// Read a batch of untitled-or-fallback-titled corals, resolve their bodies
/// (inline or R2), title them, write titles (+ `title_kind`, + `excerpt` when
/// missing) back.
///
/// Managed (source-agnostic plan PR 7): with a model and egress allowed, the
/// batch is titled with ONE `managed_enrichment` call per graph in it (a
/// batch is at most [`ATOM_BATCH_SIZE`] corals), each binding its corals and
/// the exact prompt, bounded per account per UTC day
/// ([`ATOM_TITLES_DAILY_CALLS_ENV`]) and published only when every bound coral
/// is unchanged. Without a model, or with egress forbidden, every coral gets
/// the free deterministic fallback as before.
#[allow(clippy::too_many_arguments)]
async fn run_batch(
    pool: &PgPool,
    control_pool: &PgPool,
    tenant: &TenantId,
    llm: Option<&dyn LlmProvider>,
    r2: Option<&R2Client>,
    account_id: Uuid,
    job: Option<crate::managed::enrichment::EnrichmentJob>,
    after_atom_id: Option<Uuid>,
) -> Result<BatchOutcome, String> {
    let job_id = job.as_ref().map(|j| j.id).unwrap_or_else(Uuid::nil);
    // ── 1. Read candidates (short scoped transaction) ────────────────────────
    // `atoms` is on `0083_consumer_rls`'s (and `0009_tenant_rls`'s) roster;
    // `JobScope` picks the right scoping regime for `tenant` (see
    // `workers::embed::JobScope`'s doc comment). Read-only: rolled back.
    let scope = JobScope::for_job(tenant, pool, account_id);
    let rows: Vec<CandidateRow> = {
        let mut tx = begin_plane(&scope).await?;
        let rows: Vec<CandidateRow> = sqlx::query_as(CANDIDATE_SQL)
            .bind(after_atom_id)
            .bind(ATOM_BATCH_SIZE)
            .fetch_all(&mut *tx)
            .await
            .map_err(|e| format!("candidate read failed: {e}"))?;
        tx.rollback().await.ok();
        rows
    };

    let examined = rows.len();
    let batch_full = examined as i64 >= ATOM_BATCH_SIZE;
    let last_atom_id = rows.last().map(|r| r.0);
    let mut outcome = BatchOutcome {
        examined,
        titled: 0,
        fetch_failed: 0,
        fetch_failed_terminal: 0,
        batch_full,
        last_atom_id,
        stop: None,
    };

    let candidates: Vec<Candidate> = rows
        .into_iter()
        .map(
            |(atom_id, content_inline, content_blob_key, header_len, excerpt, graph_id, content_revision)| Candidate {
                atom_id,
                content_inline,
                content_blob_key,
                header_len,
                needs_excerpt: excerpt.is_none(),
                graph_id,
                content_revision,
            },
        )
        .collect();

    if candidates.is_empty() {
        return Ok(outcome);
    }

    // ── 2. Model or fallback, and today's bound, BEFORE any R2 fetch: a
    //    bounded account must not re-fetch bodies it will not title ─────────
    let effective_llm = llm;
    let mut managed = TitleManaged { control: control_pool, job, calls_left: 0 };
    if effective_llm.is_some() {
        let bound = crate::managed::enrichment::daily_bound(ATOM_TITLES_DAILY_CALLS_ENV, DEFAULT_ATOM_TITLES_DAILY_CALLS);
        let mut tx = begin_plane(&scope).await?;
        let used = crate::managed::enrichment::calls_today(&mut tx, account_id, None, INPUT_BATCH)
            .await
            .map_err(|e| format!("title daily bound unreadable: {e}"))?;
        tx.rollback().await.ok();
        managed.calls_left = (bound - used).max(0);
        if managed.calls_left == 0 {
            outcome.stop = Some(Stop::DailyBound);
            return Ok(outcome);
        }
    }

    // ── 3. Resolve bodies: inline candidates are free; R2-only candidates
    //    need a fetch (bounded concurrency, transaction already closed) ──────
    let (inline_ready, r2_only): (Vec<Candidate>, Vec<Candidate>) = candidates
        .into_iter()
        .partition(|c| c.content_inline.as_deref().is_some_and(|s| !s.is_empty()));

    let mut resolved: Vec<ResolvedCandidate> = inline_ready
        .into_iter()
        .map(|c| ResolvedCandidate {
            atom_id: c.atom_id,
            content: c.content_inline.unwrap_or_default(),
            header_len: c.header_len,
            needs_excerpt: c.needs_excerpt,
            graph_id: c.graph_id,
            content_revision: c.content_revision,
        })
        .collect();

    // Independent review finding M4: `fetch_failed`/`fetch_failed_terminal`
    // feed ONE aggregated log line below instead of the old per-atom
    // `warn!`/`error!` pair, and `terminal_atom_ids` drives a dedicated
    // write (below) that actually removes an unfetchable candidate from
    // [`CANDIDATE_SQL`]'s selection — logging alone does not.
    let mut terminal_atom_ids: Vec<Uuid> = Vec::new();
    let mut last_fetch_error: Option<String> = None;

    for chunk in r2_only.chunks(TITLE_CONCURRENCY) {
        let fetched = futures::future::join_all(chunk.iter().filter_map(|c| {
            let blob_key = c.content_blob_key.as_deref()?;
            if blob_key.trim().is_empty() {
                return None;
            }
            let blob_key = blob_key.to_string();
            Some(async move {
                let fetched = fetch_r2_body(tenant, pool, r2, account_id, c.atom_id, &blob_key).await;
                (c, fetched)
            })
        }))
        .await;
        for (c, fetched) in fetched {
            match fetched {
                R2FetchOutcome::Body(content) => resolved.push(ResolvedCandidate {
                    atom_id: c.atom_id,
                    content,
                    header_len: c.header_len,
                    needs_excerpt: c.needs_excerpt,
                    graph_id: c.graph_id,
                    content_revision: c.content_revision,
                }),
                // Blob fetched but decrypted to nothing — nothing titleable,
                // same as an inline-empty atom (it stays a candidate).
                R2FetchOutcome::Empty => {
                    tracing::debug!(atom_id = %c.atom_id, "atom-title-backfill: R2-only atom blob is empty — nothing to title");
                }
                // Terminal: this exact blob key can never resolve. Recorded
                // for the dedicated write below rather than left to recur on
                // every future pass.
                R2FetchOutcome::Terminal(e) => {
                    outcome.fetch_failed += 1;
                    outcome.fetch_failed_terminal += 1;
                    terminal_atom_ids.push(c.atom_id);
                    last_fetch_error = Some(e);
                }
                // Transient (R2 unconfigured, DEK missing, network error):
                // the atom simply stays a candidate for the next pass rather
                // than failing the whole batch; `process_job` routes an
                // all-transient batch through `could_not_do`.
                R2FetchOutcome::Transient(e) => {
                    outcome.fetch_failed += 1;
                    last_fetch_error = Some(e);
                }
            }
        }
    }

    if outcome.fetch_failed > 0 {
        tracing::error!(
            connector_health = true,
            account_id = %account_id,
            job_id = %job_id,
            fetch_failed = outcome.fetch_failed,
            fetch_failed_terminal = outcome.fetch_failed_terminal,
            last_error = %last_fetch_error.unwrap_or_default(),
            "atom-title-backfill: R2 fetch failures this batch — terminal ones stamped with a \
             deterministic fallback title and their unusable blob_key cleared so they leave the \
             candidate set for good; transient ones remain candidates for a later pass"
        );
    }

    // ── 3b. Terminal write: stamp + blank the blob key (free, no model) ─────
    if !terminal_atom_ids.is_empty() {
        let mut tx = begin_plane(&scope).await?;
        let n = sqlx::query(TERMINAL_UNFETCHABLE_SQL)
            .bind(&terminal_atom_ids)
            .bind(UNFETCHABLE_TERMINAL_TITLE)
            .execute(&mut *tx)
            .await
            .map_err(|e| format!("terminal title write failed: {e}"))?
            .rows_affected();
        tx.commit().await.map_err(|e| format!("terminal title write commit failed: {e}"))?;
        outcome.titled += n as usize;
    }

    if resolved.is_empty() {
        return Ok(outcome);
    }

    // ── 4. Title and write ───────────────────────────────────────────────────
    let Some(llm) = effective_llm else {
        // No model may be asked: the free deterministic fallback, one write.
        let writes: Vec<(Uuid, Write)> = resolved
            .iter()
            .filter_map(|c| {
                let title = fallback_for(&c.content, c.header_len);
                let excerpt = c.needs_excerpt.then(|| derive_journal_excerpt(Some(&c.content), c.header_len)).flatten();
                (title.is_some() || excerpt.is_some()).then(|| {
                    let kind = title.as_ref().map(|_| TitleKind::Fallback.as_db_str());
                    (c.atom_id, Write { title, title_kind: kind, excerpt })
                })
            })
            .collect();
        let mut tx = begin_plane(&scope).await?;
        let n = write_titles(&mut tx, &writes).await?;
        tx.commit().await.map_err(|e| format!("title write commit failed: {e}"))?;
        outcome.titled += n as usize;
        return Ok(outcome);
    };
    let model = crate::llm::model_config::aux_model();
    let mut by_graph: std::collections::BTreeMap<Uuid, Vec<ResolvedCandidate>> = std::collections::BTreeMap::new();
    for c in resolved {
        by_graph.entry(c.graph_id).or_default().push(c);
    }
    for (graph_id, group) in &by_graph {
        match title_graph_batch(&scope, tenant, &mut managed, llm, &model, account_id, *graph_id, group).await? {
            Ok(n) => outcome.titled += n as usize,
            Err(stop) => {
                outcome.stop = Some(stop);
                break;
            }
        }
    }
    Ok(outcome)
}

/// Candidate predicate (title-always-present PR): admits both the historical
/// `title IS NULL` backlog AND rows `routes::ingest` already gave a
/// synchronous fallback title (`title_kind = 'fallback'`) so this sweep can
/// upgrade them to a real LLM headline. Does NOT admit a `title SET,
/// title_kind NULL` row on its own -- that legacy pre-column shape is
/// normalized to `'fallback'` by `0212_atoms_title_kind`/
/// `0080_atoms_title_kind`'s one-shot backfill at migration time
/// (independent review finding M1), not treated as equivalent here. Also
/// admits R2-only atoms (`content_blob_key IS NOT NULL`), not just
/// `content_inline` ones — see this module's doc comment for the prod finding
/// that made that necessary.
const CANDIDATE_SQL: &str = "SELECT atom_id, content_inline, content_blob_key, header_len, excerpt, graph_id, content_revision \
     FROM   atoms \
     WHERE  (title IS NULL OR title_kind = 'fallback') \
       AND  ((content_inline IS NOT NULL AND content_inline <> '') \
             OR (content_blob_key IS NOT NULL AND content_blob_key <> '')) \
       AND  ($1::uuid IS NULL OR atom_id > $1) \
     ORDER  BY atom_id \
     LIMIT  $2";

/// Write-back: `title`/`title_kind` are overwritten with the newly-generated
/// value when one was produced (`COALESCE(v.title, a.title)` — a `NULL` `v`
/// element, i.e. a candidate this pass only derived an excerpt for, leaves
/// the existing value alone); `excerpt` fills in only when currently NULL
/// (`COALESCE(a.excerpt, v.excerpt)`), never overwriting an existing one. The
/// `WHERE` guard is defence-in-depth against the read-then-write race
/// described in this module's doc comment — the candidate SELECT already
/// excludes rows that don't need a title, so the guard only ever matters
/// against a CONCURRENT titler landing between the two transactions.
const WRITE_SQL: &str = "UPDATE atoms AS a \
     SET    title      = COALESCE(v.title, a.title), \
            title_kind = COALESCE(v.title_kind, a.title_kind), \
            excerpt    = COALESCE(a.excerpt, v.excerpt) \
     FROM   (SELECT unnest($1::uuid[]) AS atom_id, \
                     unnest($2::text[]) AS title, \
                     unnest($3::text[]) AS title_kind, \
                     unnest($4::text[]) AS excerpt) AS v \
     WHERE  a.atom_id = v.atom_id \
       AND  (a.title IS NULL OR a.title_kind = 'fallback')";

/// Terminal write-back (independent review finding M4): stamps
/// [`UNFETCHABLE_TERMINAL_TITLE`] ONLY when `title` is still NULL
/// (`COALESCE(title, $2)` — a candidate that already carries a real fallback
/// title from before its blob key went bad keeps that title, never
/// downgraded to the generic placeholder), forces `title_kind = 'fallback'`
/// (normalizing a legacy NULL kind the same way the `0212`/`0080` one-shot
/// backfill does), and — the part that actually stops the retry — blanks
/// `content_blob_key` to `''` (`atoms.content_blob_key` is `NOT NULL`, so
/// `''` is the "no usable blob" value, same convention [`CANDIDATE_SQL`]'s
/// own `content_blob_key <> ''` check already relies on). [`CANDIDATE_SQL`]'s
/// second `AND` clause requires `content_inline` OR a non-empty
/// `content_blob_key`; blanking the one key that can never resolve (its own
/// org segment permanently disagrees with this tenant) is what removes the
/// row from that predicate for good, not the title/kind change alone —
/// `title_kind = 'fallback'` on its own would still leave it selectable
/// forever. The same `WHERE` guard as [`WRITE_SQL`] against the
/// read-then-write race.
const TERMINAL_UNFETCHABLE_SQL: &str = "UPDATE atoms \
     SET    title            = COALESCE(title, $2), \
            title_kind       = 'fallback', \
            content_blob_key = '' \
     WHERE  atom_id = ANY($1) \
       AND  (title IS NULL OR title_kind = 'fallback')";

/// Test-only entry point onto the real [`CANDIDATE_SQL`], for the same
/// reason `workers::extract::test_support_fetch_r2_only_content` exists — an
/// external `backend/tests/*.rs` integration binary links this crate as an
/// ordinary dependency and cannot see a private `const`, but the tenant-RLS
/// negative control needs to run this EXACT query (not a hand-copied
/// reimplementation that could silently drift from it) against a
/// caller-supplied executor: a `begin_tenant_system_scoped_without_account`
/// transaction for the positive proof, or a bare unscoped one for the
/// negative control ("with no `app.tenant_actor` set, this exact query sees
/// zero rows under the non-exempt role").
#[cfg(feature = "test-support")]
pub async fn test_support_candidate_atom_ids<'e, E>(exec: E) -> Result<Vec<Uuid>, sqlx::Error>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    let rows: Vec<CandidateRow> =
        sqlx::query_as(CANDIDATE_SQL)
            .bind(None::<Uuid>)
            .bind(ATOM_BATCH_SIZE)
            .fetch_all(exec)
            .await?;
    Ok(rows.into_iter().map(|(id, ..)| id).collect())
}


/// Drive the existing Org batch primitive with no model or blob client.
/// Test maintenance only: this does NOT admit/restore BackfillAtomTitles, touch
/// its queue lifecycle, or create a paid-purpose/provider dispatch entry point.
/// The real public process_job/tick guards above remain unchanged.
#[cfg(feature = "test-support")]
#[doc(hidden)]
pub async fn test_support_model_free_org_batch(
    pool: &PgPool,
    org_id: &str,
    account_id: Uuid,
) -> Result<(usize, usize, usize), String> {
    let outcome = run_batch(
        pool, pool, &TenantId::Org(org_id.to_owned()), None, None,
        account_id, None, None,
    ).await?;
    Ok((outcome.examined, outcome.titled, outcome.fetch_failed_terminal))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn job(kind: &str, payload: serde_json::Value, account_id: Option<Uuid>) -> JobRow {
        JobRow {
            id: Uuid::new_v4(),
            kind: kind.to_string(),
            payload,
            account_id,
            attempts: 0,
        }
    }

    #[test]
    fn seed_gate_opens_on_a_fresh_process_then_waits_out_its_deadline() {
        assert!(seed_gate_open(None), "a fresh process must seed at once");
        assert!(
            !seed_gate_open(Some(Instant::now() + AUTO_SEED_INTERVAL)),
            "must not seed before the scheduled deadline"
        );
        let due = Instant::now()
            .checked_sub(Duration::from_secs(1))
            .expect("test clock underflow");
        assert!(seed_gate_open(Some(due)), "must seed once the deadline passes");
    }

    #[test]
    fn failure_backoff_is_shorter_than_the_success_interval() {
        assert!(
            AUTO_SEED_RETRY_BACKOFF < AUTO_SEED_INTERVAL,
            "a failed seed should retry sooner than the steady-state interval"
        );
    }

    #[test]
    fn seed_clock_is_per_tenant() {
        // The whole point of the tenant-loop conversion: one tenant's
        // schedule must not leak into another's.
        let clock = SeedClock::default();
        let consumer = tenant_key(&TenantId::Consumer);
        let org_a = tenant_key(&TenantId::Org("org_a".to_string()));
        assert_ne!(consumer, org_a);

        clock.set(&consumer, Instant::now() + AUTO_SEED_INTERVAL);
        assert!(
            !seed_gate_open(clock.get(&consumer)),
            "consumer plane should now be gated"
        );
        assert!(
            seed_gate_open(clock.get(&org_a)),
            "a DIFFERENT tenant's clock must be untouched by the consumer's seed"
        );
    }

    #[test]
    fn tenant_key_is_stable_and_distinguishes_orgs() {
        assert_eq!(tenant_key(&TenantId::Consumer), tenant_key(&TenantId::Consumer));
        assert_ne!(
            tenant_key(&TenantId::Org("acme".to_string())),
            tenant_key(&TenantId::Org("globex".to_string()))
        );
    }

    #[test]
    fn parses_a_null_and_a_set_cursor() {
        let j = job("BackfillAtomTitles", serde_json::json!({"after_atom_id": null}), None);
        assert_eq!(parse_after_atom_id(&j).unwrap(), None);

        let id = Uuid::new_v4();
        let j = job(
            "BackfillAtomTitles",
            serde_json::json!({"after_atom_id": id}),
            None,
        );
        assert_eq!(parse_after_atom_id(&j).unwrap(), Some(id));
    }

    #[test]
    fn rejects_another_sweeps_kind() {
        let j = job(
            "ContentTsvRegconfigRepair",
            serde_json::json!({"after_atom_id": null}),
            None,
        );
        assert!(parse_after_atom_id(&j).is_err());
    }

    #[test]
    fn an_absent_cursor_field_means_start_from_the_beginning() {
        let j = job("BackfillAtomTitles", serde_json::json!({}), None);
        assert_eq!(parse_after_atom_id(&j).unwrap(), None);
    }

    #[test]
    fn rejects_a_cursor_that_is_not_a_uuid() {
        let j = job(
            "BackfillAtomTitles",
            serde_json::json!({"after_atom_id": "not-a-uuid"}),
            None,
        );
        assert!(parse_after_atom_id(&j).is_err());
    }

    #[test]
    fn candidate_sql_admits_both_null_and_fallback_titles_and_both_storage_backends() {
        // Not a DB-backed test (no Postgres here) — a text-shape pin so a
        // future edit can't silently narrow the predicate back to
        // `title IS NULL` (which would freeze every synchronously-fallback-
        // titled ingest row forever) or back to `content_inline`-only (which
        // is the R2 gap this PR fixes). The `*_pg` suite proves the query
        // actually SELECTS the right rows against real Postgres.
        assert!(CANDIDATE_SQL.contains("title IS NULL"));
        assert!(CANDIDATE_SQL.contains("title_kind = 'fallback'"));
        assert!(CANDIDATE_SQL.contains("content_inline IS NOT NULL"));
        assert!(CANDIDATE_SQL.contains("content_blob_key IS NOT NULL"));
    }

    #[test]
    fn terminal_unfetchable_sql_blanks_the_blob_key_and_preserves_an_existing_title() {
        // Independent review finding M4: the whole point of this statement
        // over the ordinary `WRITE_SQL` path is that it removes a candidate
        // from `CANDIDATE_SQL`'s selection FOR GOOD by blanking the one
        // column that can never resolve — `title_kind = 'fallback'` alone
        // (what the ordinary path sets) would leave it selectable forever.
        // Pinned as a text-shape check; the `*_pg` suite proves it against
        // real Postgres.
        assert!(TERMINAL_UNFETCHABLE_SQL.contains("content_blob_key = ''"));
        assert!(TERMINAL_UNFETCHABLE_SQL.contains("COALESCE(title, $2)"));
        assert!(TERMINAL_UNFETCHABLE_SQL.contains("title_kind       = 'fallback'"));
    }

    #[tokio::test]
    async fn fetch_r2_body_flags_a_two_segment_org_scoped_key_as_terminal_never_transient() {
        // Independent review finding M4's concrete production case: a
        // 2-segment blob_key ("{graph}/{atom}", `copy_atoms_to_graph`'s
        // pre-org-scope shape) can never satisfy an ORG tenant's
        // `assert_blob_key_org_scope` (which requires exactly 3 segments) —
        // proven here classifying as `Terminal`, never `Transient`, and
        // proven WITHOUT ever touching the network: `r2: None` and a lazy
        // (never-connecting) pool both prove the assertion runs and rejects
        // BEFORE `fetch_r2_only_content` is ever reached — if it were, this
        // test would panic on the `.expect`-free `None` R2 client instead of
        // returning `Terminal` cleanly.
        let org = crate::tenancy::TenantId::Org("acme".to_string());
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://unused:unused@localhost/unused")
            .expect("connect_lazy never opens a real connection");

        let outcome = fetch_r2_body(
            &org,
            &pool,
            None,
            Uuid::new_v4(),
            Uuid::new_v4(),
            "some-graph-id/some-atom-id",
        )
        .await;

        assert!(
            matches!(outcome, R2FetchOutcome::Terminal(_)),
            "a 2-segment key under an Org tenant must classify as Terminal"
        );

        // A second "pass" — literally the same call again, standing in for
        // a later batch re-examining the same row before the terminal write
        // has run — must classify the SAME way: this is a pure function of
        // the key's shape, not something that could flip on retry.
        let outcome2 = fetch_r2_body(
            &org,
            &pool,
            None,
            Uuid::new_v4(),
            Uuid::new_v4(),
            "some-graph-id/some-atom-id",
        )
        .await;
        assert!(matches!(outcome2, R2FetchOutcome::Terminal(_)));
    }

    #[tokio::test]
    async fn fetch_r2_body_is_never_terminal_on_the_consumer_plane() {
        // `assert_blob_key_org_scope` is a no-op when `expected_org` is
        // `None` (the consumer/personal plane never carries an org segment
        // to check against) — same 2-segment key that is Terminal under an
        // Org tenant above must NOT be Terminal here. `r2: None` still
        // proves no network call happens; the outcome is Transient (R2
        // unconfigured), the ordinary "retry later" bucket.
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://unused:unused@localhost/unused")
            .expect("connect_lazy never opens a real connection");

        let outcome = fetch_r2_body(
            &crate::tenancy::TenantId::Consumer,
            &pool,
            None,
            Uuid::new_v4(),
            Uuid::new_v4(),
            "some-graph-id/some-atom-id",
        )
        .await;

        assert!(
            matches!(outcome, R2FetchOutcome::Transient(_)),
            "the consumer plane has no org segment to check, so the SAME key must not be Terminal"
        );
    }
}
