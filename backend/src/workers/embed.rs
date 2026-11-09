//! Embedding job worker.
//!
//! Polls for `EmbedAtoms` jobs, loads atom text from R2 using the account DEK,
//! calls the cloud embedding provider in batches of up to [`EMBED_BATCH_SIZE`],
//! and writes the resulting vectors to `atoms.embedding_next`. The live
//! `atoms.embedding` column stays the bge vector and is not overwritten
//! here.
//!
//! Managed corpus admission: with a control pool and a gated (sealed) cloud
//! transport, every job goes through `managed::embedding::process`, which
//! reserves, receipts and settles each batch against the account's daily
//! `cloud_embedding` budget and stamps held/refused atoms `blocked:<reason>`
//! (keyword-indexed, retryable). Without both, nothing is sent: atoms are
//! keyword-indexed and marked failed on the ordinary bounded retry ladder.
//! Atoms the 2026-10-13 fence stamped [`BLOCKED_MANAGED_PURPOSE`] with no
//! vector are pending again and `workers::embed_retry` backfills them.

use std::time::{Duration, Instant};

use futures::future;
use sqlx::{PgPool, Row};
use uuid::Uuid;

use coralswarm_core::embed::{EmbedProvider, EMBED_MODEL_CLOUD};
use coralswarm_core::r2::{R2Client, R2Error};

use crate::jobs::{
    queue::{self, JobRow},
    JobKind,
};
use crate::store::StoreError;
use crate::tenancy::scope::{begin_account_scoped, begin_tenant_system_scoped_without_account};
use crate::tenancy::TenantId;
use crate::workers::tenant_harness::{Tick, WorkerTenant};

// ── Tuning constants ──────────────────────────────────────────────────────────

/// Fallback FTS language used for atoms that predate the `fts_language` column
/// (all such atoms were ingested as English, so this is always correct).
const DEFAULT_FTS_LANGUAGE: &str = "english";

/// Number of EmbedAtoms jobs to dequeue per poll cycle — higher reduces round-trips, lower reduces lock contention.
const JOB_BATCH_SIZE: usize = 8;
/// Number of atoms per embedding API call — higher reduces API round-trips, lower reduces memory pressure and timeout risk.
///
/// Was 64; dropped to 8 after a confirmed production OOM
/// (`packages/core/src/embed_ort.rs` — see the doc comment on
/// `DEFAULT_MAX_LENGTH` for the incident numbers). Memory for the ONNX
/// self-attention scores scales *linearly* with this batch size and
/// *quadratically* with the padded sequence length, so both levers matter:
/// `embed_ort.rs` now sizes tensors to the batch's actual content instead of
/// always padding to 512, and this constant caps how bad the worst case can
/// still be. At `EMBED_BATCH_SIZE = 8`, even worst-case-length content
/// (every atom truncated at the full 512-token cap) costs
/// `8 * 12 heads * 512^2 * 4 bytes ≈ 100 MB` for the attention-score tensor
/// **alone** — versus the 805 MB the old `64 * 512^2` combination produced.
/// That ~100 MB figure excludes the model weights, the runtime's other
/// allocations, any co-located workers sharing the same 1024 MB Fly machine,
/// and concurrent HTTP request handling; it is not a peak-RSS or headroom
/// estimate. Actual peak RSS and throughput under production concurrency
/// have not been validated post-fix and remain to be measured.
const EMBED_BATCH_SIZE: usize = 8;
/// Idle poll interval when the queue is empty — lower latency vs. higher DB
/// load. `pub(crate)`: fed to the tenant harness as this worker's `base_poll`
/// (consumer plane polls at exactly this cadence, as before; org tenants back
/// off from it — see `workers::tenant_harness`).
pub(crate) const EMPTY_POLL_SLEEP: Duration = Duration::from_millis(100);
/// How long to pause when the queue depth exceeds PAUSE_DEPTH — gives ingestion time to drain before resuming.
const BACKPRESSURE_SLEEP: Duration = Duration::from_secs(30);
/// Queue depth at which a warning is emitted — indicates the embed pipeline is falling behind ingestion.
const WARN_DEPTH: i64 = 100_000;
/// Queue depth at which the worker pauses entirely — prevents memory/CPU runaway under extreme backlog.
const PAUSE_DEPTH: i64 = 500_000;

/// The attempt cap and backoff ladder live with the embedding gateway, which
/// decides what a failure costs; re-exported for this module's queries and
/// `workers::embed_retry`, whose sweep must use the same threshold.
pub(crate) use crate::embedding::{EMBED_BACKOFF_BASE_SECS, MAX_EMBED_ATTEMPTS};
use crate::embedding::embed_backoff_secs;

/// Stamp for local-only accounts that must not send content to OpenRouter.
/// Search skips these rows; keyword (`content_tsv`) still runs.
pub(crate) const SKIPPED_LOCAL_ONLY: &str = "skipped:local-only";
/// Legacy stamp from the 2026-10-13 constant fence (#1234). No longer
/// written. Unlike [`SKIPPED_LOCAL_ONLY`] it is a missing cloud vector: these
/// atoms were left with `embed_failed_at` NULL, so [`load_pending_atoms`]
/// treats them as pending and `workers::embed_retry` claims them (clearing
/// the stamp), which backfills the backlog with no manual SQL.
pub(crate) const BLOCKED_MANAGED_PURPOSE: &str = "blocked:managed_purpose_unsupported";
/// Stamp for an atom the provider refused as longer than its per-input limit
/// (`managed::embedding`'s `input_too_long` hold). Permanent: the hold sets
/// `embed_attempts` to [`MAX_EMBED_ATTEMPTS`]. `backfill_chunk_links
/// --rechunk-oversized` finds atoms by this stamp and re-chunks their source.
pub const BLOCKED_INPUT_TOO_LONG: &str = "blocked:input_too_long";


// ── Per-job RLS scope (org-tenancy Phase W.2, group (a)) ─────────────────────

/// Error a job worker fails a job with when a dequeued job of an RLS-relevant
/// kind carries no `account_id`. Post-W.1 this is unreachable through the real
/// pipeline — `queue::dequeue` backfills or dead-letters every NULL legacy row
/// whose kind references a graph/atom — so hitting it means an upstream bug,
/// and the only safe response is to refuse to process the job unscoped.
/// `pub(crate)` so every ingest worker's fail-path message is greppable as
/// one string.
pub(crate) const UNSCOPED_JOB_ERR: &str =
    "job has no account_id — refusing to process unscoped (W.1 dequeue should have backfilled or dead-lettered this row)";

/// Which scoping regime one job's DB statements run under (org-tenancy Phase
/// W.2). Built once per job from the tick's [`TenantId`] + the job's own
/// `account_id` (org-tenancy Phase W.1):
///
/// * **Consumer plane** — every statement that touches an RLS-covered table
///   (migration `0083_consumer_rls`) runs inside a fresh
///   [`begin_account_scoped`] transaction for the job's account, so the
///   policies actually scope the queries once OPS-4 switches the app role to
///   the non-`BYPASSRLS` `coralswarm_app`. One SHORT transaction per DB
///   operation, not one long transaction per job: the workers interleave DB
///   statements with R2 fetches and embedding/LLM network calls, and holding
///   a pooled connection (and an open transaction Postgres would abort on the
///   first statement error) across those would change both pool pressure and
///   the workers' long-standing per-statement error semantics.
/// * **Org tenant plane** — the arm is NO LONGER ambient. The variant is a
///   bare `&'a PgPool` and still enforces nothing by itself, so this is a
///   property of the call sites, not of the enum. Every `JobScope::Org` arm
///   in the W.2 worker group now opens its own transaction with
///   `tenancy::scope::begin_tenant_system_scoped_without_account` before it
///   touches an RLS-covered table.
///
///   **Why it had to change.** The ambient arm was written on the rationale
///   that "tenant databases get whole-database isolation, not RLS". That is
///   still true of the CROSS-ORG boundary, and false of the member-ocean
///   boundary as of `migrations_tenant/0009_tenant_rls`, which adds 34
///   policies keyed on an `app.user_id` session GUC. Those policies resolve
///   through `tenant_rls_visible_graph_ids()`, which returns the EMPTY SET
///   with neither `app.tenant_actor` nor `app.user_id` set. Both are
///   `set_config(..,true)`, i.e. transaction-local, so a statement on a
///   pooled `&PgPool` could never carry either. Once the tenant DSNs are
///   repointed at the NOBYPASSRLS role an ambient tick would have seen no
///   member-private or team ocean at all, and an empty read is not an error
///   — it would have read as "nothing to do" and stopped embedding those
///   oceans in silence.
///
///   The conversion ran in two slices. PR #840 did the arms that issue their
///   own query. Slice 4 did the arms that delegate to the `Store` trait, by
///   calling the `_scoped` twin the Consumer arm already used. The shape of
///   the fix was constrained: worker pools are the SAME `TenantRegistry`
///   pools request traffic uses, so a session-level `SET` is not available.
///   Use `begin_tenant_scoped` where a job carries a genuine acting user, and
///   `begin_tenant_system_scoped`/`begin_tenant_system_scoped_without_account`
///   for a background sweep that has none. See
///   `docs/tenant-rls-cutover-runbook.md` §2.3.
///
/// `pub(crate)`: shared by the W.2 group (a) ingest workers; later worker
/// groups can move this somewhere more central when they convert.
pub(crate) enum JobScope<'a> {
    /// Shared consumer plane: scope every RLS-table statement to this account.
    Consumer { pool: &'a PgPool, account_id: Uuid },
    /// Org tenant plane. The variant enforces no scope itself — it is a bare
    /// pool. Every arm that matches it and then touches an RLS-covered tenant
    /// table must open `tenancy::scope::begin_tenant_system_scoped_without_account`
    /// first; see the enum's doc comment for why. A NEW arm that
    /// runs a statement straight on this pool re-opens the gap.
    Org(&'a PgPool),
}

impl<'a> JobScope<'a> {
    /// The scope for one dequeued job: `tenant` is the plane discriminator the
    /// tenant harness already threads into every tick ([`WorkerTenant::tenant`]),
    /// `account_id` is the job's own W.1 stamp.
    pub(crate) fn for_job(tenant: &TenantId, pool: &'a PgPool, account_id: Uuid) -> Self {
        match tenant {
            TenantId::Consumer => JobScope::Consumer { pool, account_id },
            TenantId::Org(_) => JobScope::Org(pool),
        }
    }

    /// The account to ATTRIBUTE this job's cost/usage bookkeeping to — i.e.
    /// the `account_id` a worker hands [`crate::llm::usage::record`].
    ///
    /// * **Consumer plane** — `Some(account)`. `llm_usage` carries an
    ///   `account_id`-keyed policy (migration `0083_consumer_rls`), so post-
    ///   OPS-4 an insert MUST be attributed and MUST run inside a
    ///   [`begin_account_scoped`] transaction or the strict
    ///   `current_setting('app.account_id')::uuid` predicate rejects it
    ///   outright. The job's own W.1 `account_id` stamp is already the right
    ///   answer here, so this needs no lookup of any kind — in particular it
    ///   does NOT need 0084's `resolve_graph_account_id` bootstrap.
    /// * **Org tenant plane** — `None`, and that stays correct, but the reason
    ///   this comment used to give no longer is. It said: "Tenant databases get
    ///   whole-database isolation, never RLS (`migrations_tenant` applies no
    ///   `0083`) ... an unattributed autocommit insert is both correct and
    ///   unchanged there." The first clause is now false —
    ///   `migrations_tenant/0009_tenant_rls` puts member-ocean policies on
    ///   tenant databases, `llm_usage` among them (anchored on `graph_id`, not
    ///   `account_id`) — and the second is a trap.
    ///
    ///   What is still true: `None` is the right ATTRIBUTION value. A tenant
    ///   database holds one org, so there is no second account to attribute
    ///   to, and `llm_usage.account_id` is nullable for exactly this case.
    ///
    ///   What is NOT true, and must not be read out of this comment: that a
    ///   tenant-plane insert can therefore skip transaction-local scoping.
    ///   Post-cutover, `llm_usage`'s policy resolves through
    ///   `tenant_rls_visible_graph_ids()`, so an autocommit insert carrying a
    ///   `graph_id` in a member-private ocean is REJECTED unless the
    ///   transaction has set `app.user_id` (or `app.tenant_actor`). Rows with a
    ///   NULL `graph_id` are hidden by the same policy for the same reason.
    ///
    ///   **The failure mode has TWO layers and they are worth separating**, so
    ///   an operator reading a post-cutover incident knows what to look for.
    ///   (An earlier version of this comment said "silently dropped rows",
    ///   which describes the observable outcome but names the wrong mechanism —
    ///   and the mechanism is the part that tells you where to look.)
    ///
    ///   1. **Postgres does NOT silently drop the row.** A `WITH CHECK`
    ///      violation raises `42501 new row violates row-level security policy
    ///      for table "llm_usage"` and aborts the `INSERT`. Nothing is written
    ///      and nothing is discarded quietly at the database layer.
    ///   2. **This specific caller then swallows that error.**
    ///      `crate::llm::usage::record` returns `()`, not a `Result`: it logs
    ///      `warn!("llm_usage: failed to record LLM usage (continuing —
    ///      accounting is best-effort)")` and carries on, by deliberate design,
    ///      because metering must never fail a user's request.
    ///
    ///   So the composed behaviour is: a real database error, downgraded by
    ///   this call site into a `WARN` log line and a missing metering row.
    ///   Never a 500, never a user-visible failure, and never anything in
    ///   `llm_usage` to notice the absence of. Detection is that log line and a
    ///   drop in row rate — NOT an error-rate alert. A different caller that
    ///   propagated the error would instead see a hard 42501, so do not
    ///   generalise "best-effort" from here to other tenant-plane writes.
    ///
    ///   Converting this write path is part of Phase 2 (see
    ///   `docs/tenant-rls-cutover-runbook.md` §2.3 — `llm/usage.rs` is one of
    ///   the Category-B call sites), and it is a Phase-3 prerequisite, not a
    ///   nice-to-have.
    ///
    /// Deliberately distinct from the scoping decision itself: this is the
    /// bookkeeping attribution, and reading it as "the job's account" at a
    /// site that must choose a *scoping* regime would be a category error.
    pub(crate) fn usage_account_id(&self) -> Option<Uuid> {
        match self {
            JobScope::Consumer { account_id, .. } => Some(*account_id),
            JobScope::Org(_) => None,
        }
    }
}

// ── Worker entry point ────────────────────────────────────────────────────────

/// How often the queue-depth backpressure check runs (consumer plane only —
/// see [`tick`]).
const DEPTH_CHECK_INTERVAL: Duration = Duration::from_secs(30);

/// One embed pass on one tenant's (pool, store) pair: dequeue up to
/// [`JOB_BATCH_SIZE`] jobs and process them. Driven by
/// `workers::tenant_harness::run_multi_tenant` (org-tenancy PR 2.1), which
/// owns the loop, the empty-poll sleep ([`EMPTY_POLL_SLEEP`]), the error
/// sleep (old `DB_ERROR_SLEEP` semantics), shutdown (stop claiming between
/// ticks; an in-flight `future::join_all` batch finishes inside the caller's
/// drain grace window, same contract as the old `run` loop), and per-org idle
/// backoff.
///
/// The queue-depth backpressure check runs on the CONSUMER plane only:
/// [`WARN_DEPTH`]/[`PAUSE_DEPTH`] are shared-plane-scale thresholds, and
/// running the depth query per org tenant would add a periodic query to every
/// tenant DB for a runaway-backlog mode org queues (promotion-fed, small)
/// can't realistically reach. `last_depth_check` is the caller-owned check
/// timestamp (`None` = never checked); a backpressure pause is returned as
/// [`Tick::deferred`] so the harness parks only this plane.
pub async fn tick(
    t: &WorkerTenant,
    embedder: &(dyn EmbedProvider + Send + Sync),
    cloud_embedder: Option<&(dyn EmbedProvider + Send + Sync)>,
    control_pool: Option<&PgPool>,
    r2: Option<&R2Client>,
    last_depth_check: &tokio::sync::Mutex<Option<Instant>>,
) -> Result<Tick, String> {
    if matches!(t.tenant, TenantId::Consumer) {
        let due = {
            let mut last = last_depth_check.lock().await;
            let due = last.is_none_or(|at| at.elapsed() >= DEPTH_CHECK_INTERVAL);
            if due {
                *last = Some(Instant::now());
            }
            due
        };
        if due {
            match pending_depth(&t.pool).await {
                Ok(depth) if depth > PAUSE_DEPTH => {
                    tracing::warn!(
                        depth,
                        threshold = PAUSE_DEPTH,
                        "embed queue critically deep — pausing {}s before next poll",
                        BACKPRESSURE_SLEEP.as_secs(),
                    );
                    return Ok(Tick::deferred(BACKPRESSURE_SLEEP));
                }
                Ok(depth) if depth > WARN_DEPTH => {
                    tracing::warn!(depth, threshold = WARN_DEPTH, "embed queue depth exceeds warn threshold");
                }
                Ok(_) => {}
                Err(e) => {
                    tracing::warn!(error = %e, "failed to query pending embed depth");
                }
            }
        }
    }

    // `EmbedAtomsRetry` is `workers::embed_retry`'s sweep output — same
    // processing pipeline (`process_job` dispatches both), the only
    // difference is its atom_ids bypass the backoff check in
    // `load_pending_atoms` (see `JobKind::EmbedAtomsRetry`'s doc comment).
    // `EmbedStatements` (statement vectors for scoped Ask) shares this queue
    // worker and the same managed `cloud_embedding` purpose
    // (`workers::statement_embed`).
    let jobs = queue::dequeue(&t.pool, &["EmbedAtoms", "EmbedAtomsRetry", crate::workers::statement_embed::JOB_KIND], JOB_BATCH_SIZE)
        .await
        .map_err(|e| format!("dequeue failed: {e}"))?;

    if jobs.is_empty() {
        return Ok(Tick::idle());
    }

    let n = jobs.len();
    future::join_all(jobs.into_iter().map(|job| async move {
        if job.kind == crate::workers::statement_embed::JOB_KIND {
            crate::workers::statement_embed::process_job(&t.tenant, &t.pool, cloud_embedder, control_pool, job).await;
            return;
        }
        process_job(
            &t.tenant,
            &t.pool,
            t.store.as_ref(),
            embedder,
            cloud_embedder,
            control_pool,
            r2,
            job,
        )
        .await
    }))
    .await;
    Ok(Tick::worked(n))
}

// ── Job processing ────────────────────────────────────────────────────────────

async fn process_managed_job(
    tenant: &TenantId,
    pool: &PgPool,
    store: &dyn crate::store::Store,
    cloud: Option<&(dyn EmbedProvider + Send + Sync)>,
    control: Option<&PgPool>,
    blobs: Option<&dyn crate::services::blob_store::PromotionBlobStore>,
    job: &JobRow,
    ids: &[Uuid],
) {
    let (Some(control), Some(cloud)) = (control, cloud.filter(|cloud| cloud.gated().is_some())) else {
        tracing::warn!(job=%job.id,"managed embedding requires control and sealed transport");
        return;
    };
    crate::managed::embedding::with_failure_cause(async {
        // A run that errors is settled here (`settle_unprocessed`), never left
        // `running` for the reaper: the atoms it could not embed are charged
        // an attempt when the job runs out of them.
        let (deny, atoms) = match crate::managed::embedding::process(
            tenant,
            pool,
            control,
            job,
            ids,
            cloud,
            blobs,
            store,
        )
        .await
        {
            Ok(report) => {
                if report.blocked > 0 {
                    // Held/refused atoms are keyword-indexed, stamped
                    // `blocked:<reason>` and retryable; they are not written.
                    tracing::warn!(
                        job = %job.id,
                        written = report.written,
                        held = report.blocked,
                        discarded_stale = report.discarded_stale,
                        "managed embedding held or refused part of the job; atoms left retryable"
                    );
                }
                match report.finish(tenant, pool, None).await {
                    Ok(()) => return,
                    // The work is done; only the job row is unsettled.
                    Err(deny) => (deny, &[][..]),
                }
            }
            Err(deny) => (deny, ids),
        };
        match crate::managed::embedding::settle_unprocessed(tenant, pool, job, deny, atoms).await {
            Ok(settled) => tracing::warn!(
                job = %job.id,
                %deny,
                cause = crate::managed::embedding::failure_cause().as_deref().unwrap_or(""),
                ?settled,
                "managed embedding job did not complete"
            ),
            Err(error) => tracing::warn!(
                %error,
                job = %job.id,
                %deny,
                "managed embedding job unsettled; the reaper will reset it"
            ),
        }
    })
    .await
}

/// Rebuild `content_tsv`, then mark the atoms embed-failed (attempts + 1,
/// backoff from now) in one transaction: keyword search does not wait for a
/// cloud vector, and the atoms stay on the bounded retry ladder.
async fn index_and_mark_failed(
    scope: &JobScope<'_>,
    ids: &[Uuid],
    texts: &[String],
    fts_languages: &[String],
) -> Result<(), StoreError> {
    if ids.is_empty() {
        return Ok(());
    }
    match scope {
        JobScope::Consumer { pool, account_id } => {
            let mut tx = begin_account_scoped(pool, *account_id).await?;
            update_content_tsv_and_clear_failure(&mut *tx, ids, texts, fts_languages).await?;
            bulk_mark_atoms_failed(&mut *tx, ids).await?;
            tx.commit().await?;
            Ok(())
        }
        JobScope::Org(pool) => {
            let mut tx = begin_tenant_system_scoped_without_account(pool).await?;
            update_content_tsv_and_clear_failure(&mut *tx, ids, texts, fts_languages).await?;
            bulk_mark_atoms_failed(&mut *tx, ids).await?;
            tx.commit().await?;
            Ok(())
        }
    }
}

pub(crate) async fn process_job(
    tenant: &TenantId,
    pool: &PgPool,
    store: &dyn crate::store::Store,
    _embedder: &dyn EmbedProvider,
    cloud_embedder: Option<&(dyn EmbedProvider + Send + Sync)>,
    control_pool: Option<&PgPool>,
    r2: Option<&R2Client>,
    job: JobRow,
) {
    let (atom_ids, bypass_backoff): (Vec<Uuid>, bool) = match parse_atom_ids(&job) {
        Ok(parsed) => parsed,
        Err(e) => {
            tracing::error!(job_id = %job.id, error = %e, "embed worker: malformed job payload");
            if let Err(qe) = queue::fail(pool, job.id, &e).await {
                tracing::error!(error = %qe, job_id = %job.id, "failed to mark malformed job as failed");
            }
            return;
        }
    };

    // ── Per-job RLS scope (org-tenancy Phase W.2) ─────────────────────────────
    // Every EmbedAtoms/EmbedAtomsRetry job carries the account its processing
    // must be scoped to (W.1 stamps it at enqueue; `queue::dequeue` backfills
    // or dead-letters NULL legacy rows), so `None` here can only be an
    // upstream bug — refuse to process unscoped. This guard runs before the
    // empty-atom_ids fast path deliberately: an RLS-relevant job with no
    // account stamp is malformed even when it happens to name zero atoms.
    let Some(account_id) = job.account_id else {
        tracing::error!(job_id = %job.id, kind = %job.kind, error = UNSCOPED_JOB_ERR, "embed worker: refusing unscoped job");
        if let Err(qe) = queue::fail(pool, job.id, UNSCOPED_JOB_ERR).await {
            tracing::error!(error = %qe, job_id = %job.id, "failed to mark unscoped job as failed");
        }
        return;
    };
    let scope = JobScope::for_job(tenant, pool, account_id);
    // Admission is real, not a constant: with a control pool and a sealed
    // transport, `managed::embedding::process` decides per batch (policy,
    // executor, credits, daily budget, per-attempt cap). Without both nothing
    // may be sent; the keyword-only path below leaves atoms retryable.
    let managed_ready = control_pool.is_some() && cloud_embedder.is_some_and(|c| c.gated().is_some());
    if managed_ready {
        let blobs = r2.map(|r2| r2 as &dyn crate::services::blob_store::PromotionBlobStore);
        process_managed_job(tenant,pool,store,cloud_embedder,control_pool,blobs,&job,&atom_ids).await;
        return;
    }

    if atom_ids.is_empty() {
        tracing::debug!(job_id = %job.id, "EmbedAtoms job has empty atom_ids, completing");
        if let Err(e) = queue::complete(pool, job.id, queue::JobOutcome::NothingToDo).await {
            tracing::error!(error = %e, job_id = %job.id, "failed to complete empty-atom job");
        }
        return;
    }

    // ── Resolve the account DEK for this batch of atoms ───────────────────────
    // `job.account_id` is the source of truth post-W.1; the resolver call
    // inside verifies the payload's atoms actually belong to that account
    // (single-account invariant + cross-account payload rejection) before the
    // key is unwrapped — see `resolve_dek_for_atoms`.
    let dek: [u8; 32] = match resolve_dek_for_atoms(tenant, pool, account_id, &atom_ids).await {
        Ok(d) => d,
        Err(e) => {
            tracing::warn!(job_id = %job.id, error = %e, "DEK resolution failed");
            if let Err(qe) = queue::fail(pool, job.id, &format!("DEK resolution failed: {e}")).await {
                tracing::error!(error = %qe, job_id = %job.id, "failed to mark job as failed after DEK error");
            }
            return;
        }
    };

    // ── Load atom blob keys ────────────────────────────────────────────────────
    let rows = match scoped_load_pending_atoms(&scope, &atom_ids, bypass_backoff).await {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(job_id = %job.id, error = %e, "embed worker: failed to load atoms from DB");
            if let Err(qe) = queue::fail(pool, job.id, &e.to_string()).await {
                tracing::error!(error = %qe, job_id = %job.id, "failed to mark job as failed after DB error");
            }
            return;
        }
    };

    if rows.is_empty() {
        tracing::debug!(job_id = %job.id, "all atoms in job already embedded or failed, completing");
        if let Err(e) = queue::complete(pool, job.id, queue::JobOutcome::NothingToDo).await {
            tracing::error!(error = %e, job_id = %job.id, "failed to complete already-embedded job");
        }
        return;
    }

    // ── Fetch text from R2 concurrently ───────────────────────────────────────
    // resolved carries (atom_id, text, fts_language) so the embed loop can
    // pass per-atom language to write_embeddings_bulk.
    let fetch_futures: Vec<_> = rows.iter().map(|row| {
        let lang = row.fts_language.clone();
        async move {
            if let Some(r2_client) = r2 {
                let r2_result = r2_client.get_atom_blob(&row.blob_key, &dek).await;
                let used_inline_fallback = matches!(&r2_result, Err(R2Error::NotFound(_)));
                match atom_text_from_r2_or_inline(r2_result, row.content_inline.as_deref()) {
                    Ok(text) => {
                        if used_inline_fallback {
                            tracing::warn!(
                                atom_id = %row.atom_id,
                                "embed worker: R2 blob missing, using content_inline"
                            );
                        }
                        Ok((row.atom_id, text, lang))
                    }
                    Err(msg) => Err((row.atom_id, msg)),
                }
            } else if let Some(inline) = &row.content_inline {
                Ok((row.atom_id, inline.clone(), lang))
            } else {
                Err((row.atom_id, "no R2 configured and content_inline is NULL".to_string()))
            }
        }
    }).collect();

    let fetch_results = future::join_all(fetch_futures).await;

    let mut resolved: Vec<(Uuid, String, String)> = Vec::with_capacity(rows.len());
    let mut r2_failed_ids: Vec<Uuid> = Vec::new();
    for result in fetch_results {
        match result {
            Ok(triple) => resolved.push(triple),
            Err((atom_id, e)) => {
                tracing::error!(
                    job_id = %job.id,
                    atom_id = %atom_id,
                    error = %e,
                    "embed worker: R2 fetch failed for atom"
                );
                r2_failed_ids.push(atom_id);
            }
        }
    }
    if !r2_failed_ids.is_empty() {
        if let Err(e) = scoped_mark_atoms_failed(&scope, &r2_failed_ids).await {
            tracing::warn!(error = %e, "failed to mark R2-failed atoms");
        }
    }

    if resolved.is_empty() {
        if let Err(e) = queue::fail(pool, job.id, "all atoms failed R2 fetch").await {
            tracing::error!(error = %e, job_id = %job.id, "failed to mark job as failed after all-R2-fail");
        }
        return;
    }

    // ── Process in sub-batches ────────────────────────────────────────────────
    let success_count: usize = 0;
    let mut failure_count: usize = 0;

    for chunk in resolved.chunks(EMBED_BATCH_SIZE) {
        let start = Instant::now();
        let ids: Vec<Uuid> = chunk.iter().map(|(id, _, _)| *id).collect();
        let text_strings: Vec<String> = chunk.iter().map(|(_, t, _)| t.clone()).collect();
        let lang_strings: Vec<String> = chunk.iter().map(|(_, _, l)| l.clone()).collect();
        // No managed transport: nothing is sent. Keyword search still gets
        // the text; the atoms are a bounded, retryable failure, not a success.
        failure_count += chunk.len();
        tracing::warn!(job_id=%job.id,atoms=chunk.len(),"embed worker: managed embedding unavailable (no control pool or sealed transport); keyword-indexed, left retryable");
        if let Err(error) = index_and_mark_failed(&scope,&ids,&text_strings,&lang_strings).await {
            tracing::error!(job_id=%job.id,atom_ids=?ids,%error,"embed worker: keyword indexing failed");
            if let Err(error)=scoped_mark_atoms_failed(&scope,&ids).await {tracing::warn!(%error,"failed to mark keyword indexing failure");}
        }
        let duration_ms = start.elapsed().as_millis();
        tracing::info!(job_id = %job.id, batch_size = chunk.len(), duration_ms, "embed batch completed");
    }

    if failure_count > 0 {
        tracing::warn!(
            job_id = %job.id,
            success_count,
            failure_count,
            "embed job completed with partial failures — failed atoms left in embed_failed state"
        );
    }

    let job_result = if success_count > 0 {
        queue::complete(pool, job.id, queue::JobOutcome::DidWork).await
    } else {
        queue::fail(pool, job.id, "all atoms in job failed to embed").await
    };
    if let Err(e) = job_result {
        tracing::error!(error = %e, job_id = %job.id, "failed to update final job state");
    }
}

// ── DEK resolution ────────────────────────────────────────────────────────────

/// Resolve the job account's DEK, first verifying the payload's atom_ids
/// actually belong to that account.
///
/// **W.1 simplification:** `job_account_id` (stamped at enqueue, backfilled at
/// dequeue for legacy rows) is now the source of truth for WHICH account's key
/// to unwrap — pre-W.1 this function had to DISCOVER the account from the
/// atom_ids. The `resolve_atoms_account_id` call is retained, repurposed from
/// discovery to VERIFICATION: it still enforces the single-account invariant
/// (a batch spanning accounts has no unambiguous DEK), and now additionally
/// rejects a job whose payload names another account's atoms — pre-OPS-4 the
/// `begin_account_scoped` GUC is inert against the BYPASSRLS owner role, so
/// this check is what stops a mis-stamped/forged payload from touching another
/// account's rows until the RLS backstop goes live (and stays as defense in
/// depth after it does).
///
/// The verification runs through the `resolve_atoms_account_id` SECURITY
/// DEFINER bootstrap function (migration 0084 / tenant
/// 0003_bootstrap_scope_functions, org-tenancy Phase S) — the ONE sanctioned
/// pre-scope exception: it must be able to see the atoms' true owner
/// regardless of (and before) any `app.account_id` scope, so it deliberately
/// runs OUTSIDE the job's scoped transactions. Present on both migration
/// trees with a byte-identical body, so it is correct on either plane's pool.
///
/// Key unwrapping then delegates to `crate::crypto::resolve_dek_for_tenant`.
/// The `tenant` dimension is load-bearing (org-tenancy PR 2.1 control-table
/// audit): on an org tenant the wrapped DEK lives in the tenant DB's
/// `account_keys`, NOT control `accounts.dek_wrapped` — resolving against
/// the control plane there would come back empty (or worse, find a stale
/// shadow DEK) and wrong-key encryption is unrecoverable.
async fn resolve_dek_for_atoms(
    tenant: &TenantId,
    pool: &PgPool,
    job_account_id: Uuid,
    atom_ids: &[Uuid],
) -> Result<[u8; 32], String> {
    let rows: Vec<Uuid> = sqlx::query_scalar("SELECT * FROM resolve_atoms_account_id($1)")
        .bind(atom_ids)
        .fetch_all(pool)
        .await
        .map_err(|e| format!("DB error looking up account for atoms: {e}"))?;

    if rows.is_empty() {
        return Err("no account found for given atom_ids".to_string());
    }
    if rows.len() > 1 {
        return Err(format!(
            "atom_ids span {} accounts — refusing to use ambiguous DEK",
            rows.len()
        ));
    }
    if rows[0] != job_account_id {
        return Err(format!(
            "atom_ids belong to account {} but the job is stamped for account {job_account_id} — refusing cross-account processing",
            rows[0]
        ));
    }

    crate::crypto::resolve_dek_for_tenant(tenant, pool, job_account_id)
        .await
        .map_err(|e| e.to_string())?
        .ok_or_else(|| "account has no DEK — run key rotation first".to_string())
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Resolve atom plaintext after an R2 fetch: use the blob when it exists,
/// otherwise `content_inline` if the object is missing. Decrypt/S3 errors
/// still fail — only [`R2Error::NotFound`] is eligible for the fallback, so
/// a corrupt blob cannot silently embed stale inline text.
///
/// The blob's text comes back with NUL replaced (`coralswarm_core::pg_text`).
/// A body sealed before the capture funnel cleaned chunk text can still hold
/// one, and every reader here binds the text into Postgres (`content_tsv`),
/// which refuses it (prod, 2026-10-25). The object store never passed through
/// Postgres' check, so this read is where those old bodies are cleaned.
pub(crate) fn atom_text_from_r2_or_inline(
    r2: Result<Vec<u8>, R2Error>,
    inline: Option<&str>,
) -> Result<String, String> {
    match r2 {
        Ok(bytes) => Ok(coralswarm_core::pg_text::sanitized(String::from_utf8_lossy(&bytes))),
        Err(e) => {
            if matches!(e, R2Error::NotFound(_)) {
                if let Some(text) = inline {
                    if !text.is_empty() {
                        return Ok(text.to_owned());
                    }
                }
            }
            Err(e.to_string())
        }
    }
}

/// Parse a claimed job's payload into its atom_ids plus whether
/// `load_pending_atoms` should bypass the backoff check for them.
///
/// Accepts both `EmbedAtoms` (bypass = `false`, ordinary backoff-checked
/// atom_ids) and `EmbedAtomsRetry` (bypass = `true` — `workers::embed_retry`'s
/// sweep already vetted these atom_ids as backoff-eligible before claiming
/// them; see that variant's doc comment for why re-checking backoff on them
/// here would reject every one).
///
/// `pub(crate)`: `workers::embed_retry`'s live test calls this directly (the
/// same function `process_job` uses) so its end-to-end coverage exercises the
/// real parsing path rather than reimplementing it.
pub(crate) fn parse_atom_ids(job: &JobRow) -> Result<(Vec<Uuid>, bool), String> {
    let tagged = serde_json::json!({
        "kind": job.kind,
        "payload": job.payload,
    });
    let kind: JobKind = serde_json::from_value(tagged)
        .map_err(|e| format!("cannot deserialise JobKind from payload: {e}"))?;

    match kind {
        JobKind::EmbedAtoms { atom_ids } => Ok((atom_ids, false)),
        JobKind::EmbedAtomsRetry { atom_ids } => Ok((atom_ids, true)),
        other => Err(format!("expected EmbedAtoms or EmbedAtomsRetry, got {:?}", other)),
    }
}

/// `pub(crate)`: `workers::embed_retry`'s live test reads `atom_id` off rows
/// returned by [`load_pending_atoms`] directly (see that function's doc
/// comment for why the test calls it at all).
pub(crate) struct AtomRow {
    pub(crate) atom_id: Uuid,
    blob_key: String,
    content_inline: Option<String>,
    fts_language: String,
}

/// `bypass_backoff`: when `true` (an `EmbedAtomsRetry` job — see that
/// variant's doc comment), admit `atom_ids` regardless of `embed_failed_at`'s
/// backoff window; still requires a missing cloud vector either way (an atom a
/// racing job already cloud-embedded is correctly excluded regardless of which job
/// kind is asking). `false` (an ordinary `EmbedAtoms` job) preserves the
/// original backoff-checked behaviour.
///
/// `pub(crate)`: `workers::embed_retry`'s live test calls this directly to
/// prove the atoms its sweep claims are actually loadable through the real
/// candidate query — the exact end-to-end assertion CodeRabbit's round-2
/// review asked for (a job that completes empty because its atom_ids fail
/// this query's backoff check is exactly the bug that review caught).
pub(crate) async fn load_pending_atoms<'e, E>(
    exec: E,
    atom_ids: &[Uuid],
    bypass_backoff: bool,
) -> Result<Vec<AtomRow>, sqlx::Error>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    // A failed atom is a candidate again once its backoff window has elapsed
    // AND it's still under the attempts cap — see `MAX_EMBED_ATTEMPTS` /
    // `embed_backoff_secs`. The `power(2, LEAST(embed_attempts, 30))` here
    // must stay in lockstep with `embed_backoff_secs`'s Rust formula (the
    // `embed_backoff_matches_sql_formula` test below cross-checks a handful of
    // attempt counts against this exact expression).
    let rows = sqlx::query(
        r#"
        SELECT atom_id, content_blob_key, content_inline,
               COALESCE(fts_language, $2) AS fts_language
        FROM   atoms
        WHERE  atom_id = ANY($1)
          AND  COALESCE(embed_model_next, '') <> $7
          AND  (embedding_next IS NULL OR embed_model_next IS DISTINCT FROM $6)
          -- The legacy fence stamp is pending only while it has no vector.
          AND  NOT (embed_model_next = $8 AND embedding_next IS NOT NULL)
          AND  (
                 $5
                 OR embed_failed_at IS NULL
                 OR (
                      embed_attempts < $3
                      AND embed_failed_at <= now() - (
                            power(2, LEAST(embed_attempts, 30)) * ($4 * INTERVAL '1 second')
                          )
                    )
               )
        "#,
    )
    .bind(atom_ids)
    .bind(DEFAULT_FTS_LANGUAGE)
    .bind(MAX_EMBED_ATTEMPTS)
    .bind(EMBED_BACKOFF_BASE_SECS)
    .bind(bypass_backoff)
    .bind(EMBED_MODEL_CLOUD)
    .bind(SKIPPED_LOCAL_ONLY)
    .bind(BLOCKED_MANAGED_PURPOSE)
    .fetch_all(exec)
    .await?;

    let atoms = rows
        .into_iter()
        .map(|r| AtomRow {
            atom_id: r.get("atom_id"),
            blob_key: r.get("content_blob_key"),
            content_inline: r.get("content_inline"),
            fts_language: r.get("fts_language"),
        })
        .collect();

    Ok(atoms)
}

/// Update the Postgres-internal `content_tsv` (BM25 keyword search, CS-041)
/// and clear `embed_failed_at` for a batch of just-embedded atoms.
///
/// Called after `Store::set_atom_embeddings` (review H12): that seam owns
/// the vector write + sync-op emission, this helper covers the columns
/// that are cloud-only and need no oplog presence.
///
/// `texts` is the decrypted atom content already in memory from the R2 fetch,
/// so we piggyback the tsvector update into the same bulk UPDATE at no extra
/// round-trip.
///
/// `fts_languages` is a parallel array to `atom_ids` containing each atom's
/// `fts_language` value (e.g. `"english"`, `"french"`). It is passed through
/// to the query (kept as a length-checked parallel array for callers that
/// already have it in hand) but is deliberately NOT used to tokenise
/// `content_tsv` — see the regconfig note below.
///
/// # Note on the embedding write split
///
/// Before review H12 this helper also wrote the embedding column directly via
/// pgvector's binary wire format (4 bytes/dim big-endian f32 via
/// `unnest($2::vector[])`). That bypassed the `Store` seam and never produced
/// an oplog op. The
/// embedding write now lives in `Store::set_atom_embeddings` (which records
/// an `atom_embedding_op` atomically); the remaining cloud-only columns
/// stay here.
///
/// `pub(crate)`: `workers::reembed`'s backfill sweep reuses this exact
/// function after re-embedding an atom (through the real embedding path,
/// possibly with a reconstructed contextual chunk header prepended) so the
/// regconfig-correctness reasoning above (`'simple'` on both the write and
/// `Store::keyword_search`'s query side) stays in exactly one place instead
/// of being duplicated.
pub(crate) async fn update_content_tsv_and_clear_failure<'e, E>(
    exec: E,
    atom_ids: &[Uuid],
    texts: &[String],
    fts_languages: &[String],
) -> Result<(), sqlx::Error>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    if atom_ids.len() != texts.len() || atom_ids.len() != fts_languages.len() {
        return Err(sqlx::Error::Protocol(format!(
            "update_content_tsv_and_clear_failure: {} atom_ids, {} texts, {} fts_languages — lengths must match",
            atom_ids.len(),
            texts.len(),
            fts_languages.len(),
        )));
    }
    if atom_ids.is_empty() {
        return Ok(());
    }

    // content_tsv is stored as plaintext; tsvector tokenisation requires unencrypted text.
    //
    // Always tokenise with the 'simple' regconfig, NOT the atom's per-language
    // `fts_language` (e.g. "english"). `Store::keyword_search` queries
    // content_tsv with `websearch_to_tsquery('simple', ...)` unconditionally
    // (store/mod.rs) so the tsquery is never stemmed. Building content_tsv with
    // a stemming regconfig (e.g. "english" turns "running" into the lexeme
    // "run") while querying with 'simple' (no stemming — the query stays
    // "running") means `content_tsv @@ tsquery` silently never matches on any
    // word whose stemmed form differs from its surface form. 'simple' on both
    // sides keeps them in lockstep — see migration 0060 for the one-time
    // repair of rows written by the old per-language code path.
    //
    // A NUL in any text fails the whole statement ("invalid byte sequence for
    // encoding UTF8: 0x00"), after the embedding was paid for. Every caller
    // reads its text through `atom_text_from_r2_or_inline`, which cleans it,
    // and this bind cleans again so no caller can bring one here.
    let texts: Vec<std::borrow::Cow<'_, str>> =
        texts.iter().map(|t| coralswarm_core::pg_text::sanitize(t)).collect();
    let texts: Vec<&str> = texts.iter().map(|t| t.as_ref()).collect();
    sqlx::query(
        r#"
        UPDATE atoms
        SET content_tsv = to_tsvector('simple', v.content),
            embed_failed_at = NULL
        FROM (
            SELECT unnest($1::uuid[])    AS atom_id,
                   unnest($2::text[])    AS content,
                   unnest($3::text[])    AS fts_language
        ) v
        WHERE atoms.atom_id = v.atom_id
        "#,
    )
    .bind(atom_ids)
    .bind(&texts)
    .bind(fts_languages)
    .execute(exec)
    .await?;

    Ok(())
}

/// Mark atoms as embed-failed, bumping `embed_attempts` on every call — including
/// retries — so [`load_pending_atoms`]'s backoff window and attempts cap
/// advance each time, not just once. `embed_failed_at` is always refreshed to
/// `now()` too: it's the anchor the backoff window is measured from, so a
/// retry that fails again must restart the clock from *this* failure, not the
/// original one.
///
/// `WHERE embedding_next is missing` guards against a race between overlapping
/// `EmbedAtoms` jobs for the same atom (possible via `workers::embed_retry`'s
/// sweep and/or horizontal scaling — see that module's doc comment): if a
/// racing job already wrote a successful cloud vector for one of `atom_ids`
/// before this call runs, that atom must NOT be stomped back into the failed
/// state — doing so would falsely bump `embed_attempts` toward
/// [`MAX_EMBED_ATTEMPTS`] (and log a spurious failure) for an atom that is
/// actually done and vector-searchable. Atoms this guard excludes are simply
/// dropped from the `RETURNING` set — that's correct, not a bug: there is
/// nothing to mark failed for them.
pub(crate) async fn bulk_mark_atoms_failed<'e, E>(exec: E, atom_ids: &[Uuid]) -> Result<(), sqlx::Error>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    if atom_ids.is_empty() {
        return Ok(());
    }
    let rows = sqlx::query(
        r#"
        UPDATE atoms
        SET    embed_failed_at = now(),
               embed_attempts  = embed_attempts + 1,
               -- A failure supersedes the legacy fence stamp.
               embed_model_next = CASE WHEN embed_model_next = $4
                                       THEN NULL ELSE embed_model_next END
        WHERE  atom_id = ANY($1)
          AND  COALESCE(embed_model_next, '') <> $3
          AND  (embedding_next IS NULL OR embed_model_next IS DISTINCT FROM $2)
        RETURNING atom_id, embed_attempts
        "#,
    )
    .bind(atom_ids)
    .bind(EMBED_MODEL_CLOUD)
    .bind(SKIPPED_LOCAL_ONLY)
    .bind(BLOCKED_MANAGED_PURPOSE)
    .fetch_all(exec)
    .await?;

    // Surface whether each atom will actually be retried (and when) or has
    // just gone permanent, so an operator scanning logs doesn't have to query
    // the DB to tell the two apart.
    for row in rows {
        let atom_id: Uuid = row.get("atom_id");
        let attempts: i32 = row.get("embed_attempts");
        if attempts >= MAX_EMBED_ATTEMPTS {
            tracing::warn!(
                %atom_id,
                attempts,
                max_attempts = MAX_EMBED_ATTEMPTS,
                "embed worker: atom embed permanently failed (attempts cap reached)"
            );
        } else {
            tracing::debug!(
                %atom_id,
                attempts,
                retry_in_secs = embed_backoff_secs(attempts),
                "embed worker: atom embed failed, will retry with backoff"
            );
        }
    }
    Ok(())
}

// ── Scope-aware statement wrappers (org-tenancy Phase W.2) ────────────────────
//
// Each wrapper runs ONE of the worker's `atoms`-touching statements under the
// job's [`JobScope`]: consumer plane ⇒ inside a fresh `begin_account_scoped`
// transaction (committed on success, rolled back on error); org tenant ⇒
// inside a fresh `begin_tenant_system_scoped_without_account` transaction.
// NEITHER arm runs directly against the pool — see [`JobScope`]'s doc for why
// the org arm had to stop doing that, and for why scoping is per-statement
// rather than per-job.

/// [`load_pending_atoms`] under the job's scope.
async fn scoped_load_pending_atoms(
    scope: &JobScope<'_>,
    atom_ids: &[Uuid],
    bypass_backoff: bool,
) -> Result<Vec<AtomRow>, StoreError> {
    match scope {
        JobScope::Consumer { pool, account_id } => {
            let mut tx = begin_account_scoped(pool, *account_id).await?;
            let rows = load_pending_atoms(&mut *tx, atom_ids, bypass_backoff).await?;
            tx.commit().await?;
            Ok(rows)
        }
        JobScope::Org(pool) => {
            let mut tx = begin_tenant_system_scoped_without_account(pool).await?;
            let rows = load_pending_atoms(&mut *tx, atom_ids, bypass_backoff).await?;
            tx.commit().await?;
            Ok(rows)
        }
    }
}

/// [`bulk_mark_atoms_failed`] under the job's scope.
async fn scoped_mark_atoms_failed(scope: &JobScope<'_>, atom_ids: &[Uuid]) -> Result<(), StoreError> {
    match scope {
        JobScope::Consumer { pool, account_id } => {
            let mut tx = begin_account_scoped(pool, *account_id).await?;
            bulk_mark_atoms_failed(&mut *tx, atom_ids).await?;
            tx.commit().await?;
            Ok(())
        }
        JobScope::Org(pool) => {
            let mut tx = begin_tenant_system_scoped_without_account(pool).await?;
            bulk_mark_atoms_failed(&mut *tx, atom_ids).await?;
            tx.commit().await?;
            Ok(())
        }
    }
}

async fn pending_depth(pool: &PgPool) -> Result<i64, sqlx::Error> {
    let row = sqlx::query(
        r#"
        SELECT COUNT(*) AS cnt
        FROM   jobs
        WHERE  kind   = 'EmbedAtoms'
          AND  status = 'pending'
        "#,
    )
    .fetch_one(pool)
    .await?;

    Ok(row.get::<i64, _>("cnt"))
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use coralswarm_core::embed::EmbedBatchFuture;
    use std::sync::Arc;

    struct StubEmbedder {
        dim: usize,
    }

    impl EmbedProvider for StubEmbedder {

        fn embed_batch<'a>(
        &'a self,
        texts: &'a [coralswarm_core::embed::EmbedInput],
        _: coralswarm_core::embed::ProviderPermit,
    ) -> EmbedBatchFuture<'a> {
            let dim = self.dim;
            let n = texts.len();
            Box::pin(async move { Ok(vec![vec![0.0f32; dim]; n]) })
        }
    }

    // ── embed_backoff_secs (pure logic) ───────────────────────────────────────

    #[test]
    fn embed_backoff_grows_exponentially() {
        // 30, 60, 120, 240 …
        let delays: Vec<i64> = (0..4).map(embed_backoff_secs).collect();
        assert_eq!(delays, vec![30, 60, 120, 240]);
    }

    #[test]
    fn embed_backoff_never_overflows_or_panics_at_large_attempts() {
        // Exponent is clamped at 30 regardless of how large `attempts` gets —
        // must not overflow/panic even at i32::MAX.
        let capped = embed_backoff_secs(30);
        assert_eq!(embed_backoff_secs(i32::MAX), capped);
        assert_eq!(embed_backoff_secs(1000), capped);
    }

    #[test]
    fn embed_backoff_matches_sql_formula() {
        // Cross-check the Rust formula against the exact SQL expression used
        // in `load_pending_atoms` / `embed_retry::find_retry_eligible_atoms`
        // (`power(2, LEAST(embed_attempts, 30)) * (EMBED_BACKOFF_BASE_SECS *
        // INTERVAL '1 second')`) — computed here in f64 to mirror Postgres's
        // `power()` (double precision), then compared as whole seconds.
        for attempts in [0, 1, 2, 5, 6, 10, 30, 31, 100] {
            let sql_equivalent =
                (2f64.powi(attempts.clamp(0, 30)) * EMBED_BACKOFF_BASE_SECS as f64).round() as i64;
            assert_eq!(
                embed_backoff_secs(attempts),
                sql_equivalent,
                "mismatch at attempts={attempts}"
            );
        }
    }

    // ── JobScope::usage_account_id (pure logic) ───────────────────────────────
    //
    // The `llm_usage` attribution every worker's `llm::usage::record` call now
    // reads from. The CONSUMER arm is proven end-to-end against a real
    // `coralswarm_app` login in `tests/llm_usage_attribution_rls_pg.rs`; the
    // ORG arm is not reachable from that suite (it drives the consumer plane
    // only), so it is pinned here. Getting the org arm wrong would be quiet
    // and bad in a specific way: `Some(account)` there would push a
    // `begin_account_scoped` transaction onto a tenant pool where the GUC
    // means nothing, contradicting `tenancy::scope`'s "consumer pool only"
    // contract.
    // `#[tokio::test]`, not `#[test]`: `connect_lazy` never opens a socket but
    // does spawn the pool's idle reaper, so it needs a runtime in scope. No
    // database is contacted — the URL is never dialed.
    #[tokio::test]
    async fn usage_account_id_attributes_on_the_consumer_plane_only() {
        let account = Uuid::new_v4();
        // A `PgPool` can be constructed lazily without ever connecting, which
        // is all these two variants need to exist.
        let pool = PgPool::connect_lazy("postgres://user@localhost/db").expect("lazy pool");

        let consumer = JobScope::Consumer { pool: &pool, account_id: account };
        assert_eq!(
            consumer.usage_account_id(),
            Some(account),
            "consumer-plane jobs must attribute usage to their own account — RLS rejects the \
             insert otherwise"
        );

        let org = JobScope::Org(&pool);
        assert_eq!(
            org.usage_account_id(),
            None,
            "org-tenant jobs must stay unattributed — the tenant DB has no RLS and the account \
             GUC is meaningless there"
        );
    }

    #[test]
    fn parse_atom_ids_roundtrip() {
        let ids = vec![Uuid::new_v4(), Uuid::new_v4()];
        let kind = JobKind::EmbedAtoms { atom_ids: ids.clone() };
        let tagged = serde_json::to_value(&kind).unwrap();

        let job = JobRow {
            id: Uuid::new_v4(),
            kind: tagged["kind"].as_str().unwrap().to_owned(),
            payload: tagged["payload"].clone(),
            attempts: 0,
            account_id: None,
        };

        let (parsed, bypass_backoff) = parse_atom_ids(&job).unwrap();
        assert_eq!(parsed, ids);
        assert!(!bypass_backoff, "an ordinary EmbedAtoms job must NOT bypass backoff");
    }

    #[test]
    fn atom_text_prefers_r2_blob() {
        let text = atom_text_from_r2_or_inline(Ok(b"from-r2".to_vec()), Some("from-inline"));
        assert_eq!(text.unwrap(), "from-r2");
    }

    #[test]
    fn atom_text_falls_back_to_inline_when_blob_missing() {
        let text = atom_text_from_r2_or_inline(
            Err(R2Error::NotFound("graph/atom".into())),
            Some("from-inline"),
        );
        assert_eq!(text.unwrap(), "from-inline");
    }

    #[test]
    fn atom_text_does_not_fall_back_on_decrypt_errors() {
        let err = atom_text_from_r2_or_inline(
            Err(R2Error::S3("storage operation failed".into())),
            Some("from-inline"),
        );
        assert!(err.is_err(), "non-NotFound R2 errors must not use inline text");
    }

    #[test]
    fn atom_text_not_found_without_inline_stays_an_error() {
        let err = atom_text_from_r2_or_inline(Err(R2Error::NotFound("graph/atom".into())), None);
        assert!(err.unwrap_err().contains("blob not found"));
    }

    /// `EmbedAtomsRetry` (the job kind `workers::embed_retry`'s sweep
    /// enqueues) must parse to the same atom_ids with `bypass_backoff = true`
    /// — CodeRabbit review, PR #194 round 2: this is the flag that makes the
    /// sweep's claimed atoms actually loadable by the job it just enqueued.
    #[test]
    fn parse_atom_ids_retry_variant_sets_bypass_backoff() {
        let ids = vec![Uuid::new_v4(), Uuid::new_v4()];
        let kind = JobKind::EmbedAtomsRetry { atom_ids: ids.clone() };
        let tagged = serde_json::to_value(&kind).unwrap();

        let job = JobRow {
            id: Uuid::new_v4(),
            kind: tagged["kind"].as_str().unwrap().to_owned(),
            payload: tagged["payload"].clone(),
            attempts: 0,
            account_id: None,
        };

        let (parsed, bypass_backoff) = parse_atom_ids(&job).unwrap();
        assert_eq!(parsed, ids);
        assert!(bypass_backoff, "an EmbedAtomsRetry job MUST bypass backoff");
    }

    #[test]
    fn parse_atom_ids_empty_list() {
        let kind = JobKind::EmbedAtoms { atom_ids: vec![] };
        let tagged = serde_json::to_value(&kind).unwrap();
        let job = JobRow {
            id: Uuid::new_v4(),
            kind: tagged["kind"].as_str().unwrap().to_owned(),
            payload: tagged["payload"].clone(),
            attempts: 0,
            account_id: None,
        };
        let (parsed, _) = parse_atom_ids(&job).unwrap();
        assert!(parsed.is_empty());
    }

    #[test]
    fn parse_atom_ids_wrong_kind_errors() {
        let kind = JobKind::RouteCopresence { session_id: Uuid::new_v4() };
        let tagged = serde_json::to_value(&kind).unwrap();
        let job = JobRow {
            id: Uuid::new_v4(),
            kind: tagged["kind"].as_str().unwrap().to_owned(),
            payload: tagged["payload"].clone(),
            attempts: 0,
            account_id: None,
        };
        assert!(parse_atom_ids(&job).is_err());
    }

    #[tokio::test]
    async fn stub_embedder_batch() {
        let e = StubEmbedder { dim: 384 };
        let texts = ["hello", "world"];
        let on_device = crate::embedding::Purpose::Unbilled(crate::embedding::Unbilled::OnDevice);
        let result = crate::embedding::submit(&e, on_device, &texts).await.unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].len(), 384);
    }

    #[allow(dead_code)]
    fn _assert_arc_dyn_send_sync(e: Arc<dyn EmbedProvider + Send + Sync>) {
        let _ = e;
    }

    /// Regression test for the `content_tsv` regconfig mismatch (silent BM25
    /// recall loss): `update_content_tsv_and_clear_failure` must tokenise
    /// with 'simple' so it matches `Store::keyword_search`'s unconditional
    /// `websearch_to_tsquery('simple', ...)` — see this function's doc
    /// comment for the full bug writeup.
    ///
    /// Gated on `RELINFER_TEST_DATABASE_URL`, same convention as
    /// `workers::slack_name_backfill`'s live tests: `update_content_tsv_and_clear_failure`
    /// is private to the crate, so there is nothing `pub` to call from a
    /// separate `backend/tests/*_pg.rs` integration test — the live test
    /// lives here instead.
    ///
    /// Ingests an atom whose content is "we are running late" (a word,
    /// "running", that a stemming regconfig like 'english' would fold to the
    /// lexeme "run"), builds `content_tsv` the same way the real embed
    /// pipeline does, then searches for the literal surface form "running"
    /// through `Store::keyword_search` and asserts it is found. Before the
    /// fix (write side used `fts_language::regconfig`, e.g. "english") this
    /// search would silently return zero rows.
    #[tokio::test]
    async fn keyword_search_finds_atom_after_content_tsv_uses_simple_regconfig() {
        use crate::retrieval::RetrievalFilter;
        use crate::store::{PostgresStore, Store};

        let url = match std::env::var("RELINFER_TEST_DATABASE_URL") {
            Ok(u) => u,
            Err(_) => {
                eprintln!(
                    "skipping: set RELINFER_TEST_DATABASE_URL to run the live content_tsv regconfig regression test"
                );
                return;
            }
        };
        // Bound the connect attempt: without a deadline, an unreachable
        // RELINFER_TEST_DATABASE_URL hangs on the OS TCP timeout instead of
        // failing fast (CodeRabbit review, PR #190).
        const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
        let pool = tokio::time::timeout(
            CONNECT_TIMEOUT,
            sqlx::postgres::PgPoolOptions::new().max_connections(4).connect(&url),
        )
        .await
        .unwrap_or_else(|_| panic!("connect to test Postgres timed out after {CONNECT_TIMEOUT:?}"))
        .expect("connect to test Postgres");

        // ── Seed tenancy chain: user → account → graph ────────────────────────
        let email = format!("{}@test.local", Uuid::new_v4());
        let user_id: Uuid = sqlx::query_scalar("INSERT INTO users (email) VALUES ($1) RETURNING id")
            .bind(&email)
            .fetch_one(&pool)
            .await
            .expect("insert user");
        let account_id: Uuid =
            sqlx::query_scalar("INSERT INTO accounts (owner_user_id) VALUES ($1) RETURNING id")
                .bind(user_id)
                .fetch_one(&pool)
                .await
                .expect("insert account");

        let store = PostgresStore::new(pool.clone());
        let graph = store
            .create_graph(account_id, "content-tsv-regconfig-e2e", "personal")
            .await
            .expect("create graph");
        let graph_id = graph.id;

        // ── Insert an atom with plaintext content (dev-mode content_inline
        // path — mirrors what the embed worker fetches before tokenising). ──
        let atom_id: Uuid = sqlx::query_scalar(
            "INSERT INTO atoms (graph_id, source_type, source_id, content_blob_key, \
                                 content_inline, timestamp, fts_language) \
             VALUES ($1, $2, $3, $4, $5, now(), $6) \
             RETURNING atom_id",
        )
        .bind(graph_id)
        .bind("test_source")
        .bind(format!("regconfig-e2e-{}", Uuid::new_v4()))
        .bind("test-blob-key")
        .bind("we are running late")
        .bind("english")
        .fetch_one(&pool)
        .await
        .expect("insert atom");

        // ── Build content_tsv the same way the real embed pipeline does ───────
        update_content_tsv_and_clear_failure(
            &pool,
            &[atom_id],
            &["we are running late".to_string()],
            &["english".to_string()],
        )
        .await
        .expect("update_content_tsv_and_clear_failure");

        // ── A search for the literal surface form "running" must find it ──────
        let hits = store
            .keyword_search(graph_id, "running", 10, &RetrievalFilter::default())
            .await
            .expect("keyword_search");
        assert!(
            hits.iter().any(|(id, _)| *id == atom_id),
            "keyword_search for \"running\" must find an atom indexed from \"we are running late\" \
             — content_tsv must be tokenised with the SAME regconfig ('simple') that \
             keyword_search queries with, or stemming silently drops the match"
        );

        // ── Cleanup (cascades through graph → atoms). `.expect(...)`, not
        // `.ok()`, so a cleanup failure fails the test loudly instead of
        // being silently swallowed (CodeRabbit review, PR #190). ─────────────
        sqlx::query("DELETE FROM accounts WHERE id = $1")
            .bind(account_id)
            .execute(&pool)
            .await
            .expect("cleanup: delete account");
        sqlx::query("DELETE FROM users WHERE id = $1")
            .bind(user_id)
            .execute(&pool)
            .await
            .expect("cleanup: delete user");

        println!(
            "content_tsv regconfig regression test passed: \"running\" found in \"we are running late\""
        );
    }

    /// W.2 defensive guard: a job of an RLS-relevant kind whose `account_id`
    /// is `None` must be FAILED with [`UNSCOPED_JOB_ERR`] — never processed
    /// unscoped. This state is unreachable through the real pipeline
    /// (`queue::dequeue` backfills or dead-letters NULL legacy rows before a
    /// worker ever sees them), which is exactly why the guard is exercised
    /// here by calling the private `process_job` directly with a forged
    /// `JobRow` — the tick path cannot produce one.
    ///
    /// Gated on `RELINFER_TEST_DATABASE_URL`, same convention as the other
    /// live tests in this module.
    #[tokio::test]
    async fn process_job_without_account_id_fails_defensively() {
        use crate::store::PostgresStore;

        let url = match std::env::var("RELINFER_TEST_DATABASE_URL") {
            Ok(u) => u,
            Err(_) => {
                eprintln!("skipping: set RELINFER_TEST_DATABASE_URL to run the unscoped-job guard test");
                return;
            }
        };
        const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
        let pool = tokio::time::timeout(
            CONNECT_TIMEOUT,
            sqlx::postgres::PgPoolOptions::new().max_connections(2).connect(&url),
        )
        .await
        .unwrap_or_else(|_| panic!("connect to test Postgres timed out after {CONNECT_TIMEOUT:?}"))
        .expect("connect to test Postgres");

        // A claimed ('running') job row for the guard's `queue::fail` to act
        // on — inserted raw so `account_id` stays NULL, mirroring the forged
        // JobRow below.
        let atom_id = Uuid::new_v4();
        let kind = JobKind::EmbedAtoms { atom_ids: vec![atom_id] };
        let tagged = serde_json::to_value(&kind).unwrap();
        let job_id: Uuid = sqlx::query_scalar(
            "INSERT INTO jobs (kind, payload, status, started_at) \
             VALUES ($1, $2, 'running', now()) RETURNING id",
        )
        .bind(tagged["kind"].as_str().unwrap())
        .bind(&tagged["payload"])
        .fetch_one(&pool)
        .await
        .expect("insert running job");

        let job = JobRow {
            id: job_id,
            kind: tagged["kind"].as_str().unwrap().to_owned(),
            payload: tagged["payload"].clone(),
            attempts: 0,
            account_id: None,
        };

        let store = PostgresStore::new(pool.clone());
        process_job(
            &TenantId::Consumer,
            &pool,
            &store,
            &StubEmbedder { dim: 4 },
            None,
            None,
            None,
            job,
        )
        .await;

        let (status, error_message): (String, Option<String>) = sqlx::query_as(
            "SELECT status::text, error_message FROM jobs WHERE id = $1",
        )
        .bind(job_id)
        .fetch_one(&pool)
        .await
        .expect("read job status");
        assert_ne!(status, "done", "an unscoped job must never complete");
        assert!(
            error_message.unwrap_or_default().contains("account_id"),
            "failure must carry the unscoped-job error"
        );

        sqlx::query("DELETE FROM jobs WHERE id = $1")
            .bind(job_id)
            .execute(&pool)
            .await
            .expect("cleanup: delete job");
    }
}

#[cfg(test)]
pub(crate) async fn managed_job_for_test(
    tenant: &TenantId,
    pool: &PgPool,
    store: &dyn crate::store::Store,
    cloud: &(dyn EmbedProvider + Send + Sync),
    control: &PgPool,
    job: &JobRow,
    ids: &[Uuid],
) {
    managed_job_with_blobs_for_test(tenant, pool, store, cloud, control, None, job, ids).await;
}

/// [`managed_job_for_test`] reading atom bodies from `blobs`, as prod reads
/// them from the object store.
#[cfg(test)]
pub(crate) async fn managed_job_with_blobs_for_test(
    tenant: &TenantId,
    pool: &PgPool,
    store: &dyn crate::store::Store,
    cloud: &(dyn EmbedProvider + Send + Sync),
    control: &PgPool,
    blobs: Option<&dyn crate::services::blob_store::PromotionBlobStore>,
    job: &JobRow,
    ids: &[Uuid],
) {
    process_managed_job(
        tenant,
        pool,
        store,
        Some(cloud),
        Some(control),
        blobs,
        job,
        ids,
    )
    .await;
}
