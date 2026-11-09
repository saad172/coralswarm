//! Re-embed backfill sweep for atoms whose stored vector predates the
//! currently-active `chunker_version` / `embed_model` (`ReembedOutdatedChunks`).
//!
//! # Why this exists
//!
//! Migration 0063 (`cs-phase0-embed-versioning`) added `atoms.embed_model` /
//! `atoms.chunker_version`, stamped on every write via
//! `Store::set_atom_embeddings`. Every atom embedded before PR #198
//! (`cs-phase0-chunk-headers`) landed carries `chunker_version = 1`: no
//! contextual chunk header, and chunked under the OLD `TOKEN_TARGET`.
//! `coralswarm_core::chunker::CHUNKER_VERSION` is now `2` — meaning the
//! WHOLE pre-#198 corpus is stuck on a stale chunking/embedding shape until
//! something re-embeds it. `Store::vector_search`'s model filter does not
//! exclude v1 atoms from retrieval (0063's backfill deliberately stamped
//! them as "current" so they kept participating in search — see that
//! migration's header), so this is a QUALITY backfill (header-aware,
//! current-TOKEN_TARGET retrieval on old meetings), not an availability one.
//!
//! # Candidate selection
//!
//! ```sql
//! SELECT a.atom_id, a.graph_id, g.account_id, a.source_type, a.source_id,
//!        a.content_blob_key, a.content_inline, a.participants, a.timestamp,
//!        COALESCE(a.header_len, 0)       AS header_len,
//!        COALESCE(a.fts_language, 'english') AS fts_language
//! FROM   atoms a
//! JOIN   graphs g ON g.id = a.graph_id
//! WHERE  a.embedding IS NOT NULL
//!   AND  (a.chunker_version IS DISTINCT FROM $1 OR a.embed_model IS DISTINCT FROM $2)
//!   AND  ($3::uuid IS NULL OR a.atom_id > $3)
//! ORDER BY a.atom_id
//! LIMIT  $4
//! ```
//! `$1` / `$2` are the currently-active `CHUNKER_VERSION` / `EMBED_MODEL_NAME`
//! constants — [`needs_reembed`] is the pure, unit-tested Rust mirror of the
//! `IS DISTINCT FROM ... OR ... IS DISTINCT FROM ...` clause (same
//! cross-check pattern `workers::embed::embed_backoff_secs` uses against its
//! own SQL formula). `embedding IS NOT NULL` scopes this to atoms that HAVE
//! a vector to refresh — an atom mid-first-embed (or permanently
//! embed-failed) is `workers::embed`'s / `workers::embed_retry`'s job, not
//! this one's; the two candidate sets are disjoint by construction, so there
//! is no double-claim risk between this sweep and the primary embed
//! pipeline.
//!
//! # Header reconstruction — the real payoff, and its actual coverage
//!
//! A pre-header atom's PERSISTED content never had a header prepended
//! (`header_len` is `0`/`NULL`) — naively re-embedding it just re-embeds the
//! exact same headerless text under a fresh `chunker_version` stamp, gaining
//! nothing but model/version uniformity. [`plan_reembed`] instead
//! reconstructs the contextual chunk header
//! (`coralswarm_core::chunker::build_meeting_header`) for every candidate
//! that is BOTH meeting-shaped (`ChunkStrategy::for_source_type(source_type)
//! == Transcript` — the exact scope `ingest::atom::build_contextual_header`
//! uses at ingest time) AND headerless (`header_len <= 0`):
//!
//!   - **Date** — `Meeting on {date}.` at minimum. `atoms.timestamp` is a
//!     `NOT NULL` column, so this clause is ALWAYS available. Combined with
//!     the meeting-shaped scope above, this means header reconstruction
//!     succeeds (produces `Some(header)`, never `None`) for effectively
//!     EVERY selected pre-header meeting/transcript atom — 100% of that
//!     subset gains at least a date-anchored header.
//!   - **Participants** — from `atoms.participants` (already stored per atom
//!     at ingest time, independent of any header — see
//!     `ingest::atom::merge_chunk_speakers`). Present whenever the
//!     original ingest request supplied participants or the transcript
//!     chunker parsed speaker turns; empty otherwise (e.g. an
//!     un-diarized transcript), in which case the clause is simply omitted
//!     (`build_meeting_header`'s graceful-omission rule).
//!   - **Title** — best-effort, and the one clause NOT guaranteed: resolved
//!     from `meeting_transcripts` (`(graph_id, base_source_id)`, the chunk
//!     atom's `source_id` with its `:chunk:<N>` suffix stripped via
//!     `workers::extract::base_source_id`) via the SAME first-line heuristic
//!     `routes::meetings::meeting_title_from_raw` uses at ingest time
//!     (`ingest::atom::build_contextual_header`), so a meeting's
//!     reconstructed header never disagrees with a freshly-ingested one.
//!     Absent when the original ingest request never carried `raw_content`
//!     (verbatim transcript storage — `POST /v1/ingest/atom`'s
//!     `raw_content` field — is optional and best-effort; some historical
//!     ingest call sites and/or dropped-on-R2-failure writes never
//!     populated `meeting_transcripts` for a given meeting) or the raw
//!     text's first non-blank line doesn't read as a plausible title. There
//!     is no way to determine ahead of time what fraction of the historical
//!     corpus this covers without querying production data — this PR
//!     therefore ships the mechanism and states the guarantee precisely:
//!     EVERY pre-header meeting-shaped atom gets AT LEAST a
//!     date-(+participants-)anchored header; SOME additionally get a title,
//!     exactly when `meeting_transcripts` has a usable row.
//!
//! Every OTHER candidate — chat sources (Slack, Teams channel/chat, which
//! carry their own intentionally user-visible "{sender} in #{channel} ·
//! {when}" header and were never in scope for this header at ingest time
//! either), email, generic docs, and any atom that ALREADY has a header
//! (`header_len > 0` — e.g. selected only because `embed_model` differs, a
//! hypothetical future model bump) — is re-embedded VERBATIM: no content
//! mutation, just the model/version uniformity gain plus (for meeting atoms
//! that already had a v2 header) nothing new to reconstruct.
//!
//! # Safety
//!
//! - **Idempotent.** Once an atom's `chunker_version`/`embed_model` reach
//!   current, the candidate query's `IS DISTINCT FROM` predicate excludes
//!   it from every future pass — re-running a full sweep after a partial
//!   one (or after hand-inserting it twice) just skips everything already
//!   converged and reprocesses only what's left.
//! - **Bounded batches.** [`ATOM_BATCH_SIZE`] atoms are claimed per job
//!   execution (see "Batching, continuation, termination" below);
//!   [`EMBED_SUBBATCH_SIZE`] mirrors `workers::embed::EMBED_BATCH_SIZE`'s
//!   memory reasoning for the embedder call itself.
//! - **Rejects zero vectors, fails fast on a broken embedder.**
//!   `Store::set_atom_embeddings` already refuses to persist an all-zero
//!   embedding (the signature of a stub/broken embedder) — this sweep
//!   inherits that guard for free by writing through the same seam, exactly
//!   like `workers::embed::process_job`.
//! - **No durable double-claim marker (unlike `workers::embed_retry`).**
//!   `workers::embed_retry` needs one because it's a PERIODIC sweep where
//!   overlapping passes are the normal case. This is a ONE-SHOT,
//!   hand-triggered sweep: the `jobs` table's own `FOR UPDATE SKIP LOCKED`
//!   dequeue (`jobs::queue::dequeue`) already guarantees only one worker
//!   replica ever processes a given job row, and each pass's continuation
//!   carries an idempotency key
//!   (`format!("reembed_outdated_chunks:{cursor}")`) so a retried enqueue
//!   can't duplicate the NEXT window either. The only residual risk is an
//!   operator hand-inserting two unscoped (`after_atom_id: None`) sweeps
//!   concurrently, which would let both claim an overlapping window and
//!   re-embed some atoms twice — wasted duplicate embed-API/R2 calls, not a
//!   correctness bug: both passes converge to the same correct
//!   current-version result (idempotent), so this is accepted rather than
//!   engineered around; the module doc for
//!   [`crate::jobs::JobKind::ReembedOutdatedChunks`] tells operators to
//!   hand-insert exactly one.
//!
//! # Batching, continuation, termination
//!
//! Same keyset-cursor + continuation SHAPE as `workers::content_tsv_backfill`
//! (see that module's doc for the general pagination argument this mirrors),
//! with one deliberate difference: `content_tsv_backfill`'s unit of work is
//! one all-or-nothing SQL statement, so its cursor always advances past a
//! full batch unconditionally. This worker's batches have genuine PARTIAL
//! failure modes (a DEK failure, an embedder timeout, an R2 fetch/put
//! failure), so the cursor only advances — via a continuation job carrying
//! it forward — when EVERY candidate this pass examined fully converged
//! (`BatchOutcome::all_succeeded`; see that field's doc comment for why
//! advancing past a partial failure would silently strand it). Any failure
//! instead fails the CURRENT job (unchanged cursor), letting `jobs::queue`'s
//! own bounded attempts/backoff retry the SAME window automatically. If
//! enqueueing the continuation itself fails (after a fully successful
//! batch), the CURRENT job is ALSO failed (not completed) so its own bounded
//! retry gets another chance — identical reasoning to
//! `workers::content_tsv_backfill::process_job`.
//!
//! # Triggering
//!
//! Not wired to any automatic trigger. Hand-insert a single unscoped sweep
//! job after deploying this PR:
//! ```sql
//! INSERT INTO jobs (kind, payload)
//! VALUES ('ReembedOutdatedChunks', '{"after_atom_id": null}'::jsonb);
//! ```
//!
//! # Consumer-plane RLS scoping (org-tenancy Phase W.2): seed → per-account
//!
//! `atoms`, `graphs`, `meeting_transcripts`, and `op_log` are all on
//! migration `0083_consumer_rls`'s policy roster, and this sweep was ONE
//! deliberate cross-account query surface — post-OPS-4 (app role switched to
//! the non-`BYPASSRLS` `coralswarm_app`) its candidate query would
//! hard-error on the strict `current_setting('app.account_id')` predicate.
//! Same queue-native loop-per-account conversion as
//! `workers::content_tsv_backfill` (see that module's RLS section for the
//! full flagged-decision rationale): the operator-inserted account-less job
//! above becomes a **seed** that enumerates the `accounts` roster (control
//! table, outside the RLS roster) and enqueues one stamped child job per
//! account; each child sweeps only its own account's candidates (explicit
//! `g.account_id = $n` predicate — app-layer scoping primary even pre-OPS-4,
//! RLS the backstop) with its own keyset cursor and per-account
//! continuations. Every RLS-covered statement runs inside its own short
//! `begin_account_scoped` transaction — per-statement, not per-job, because
//! batches interleave DB statements with R2/embedder network calls. R2 and
//! embedder calls themselves never run inside a transaction. The worker now
//! runs on the tenant harness: leftover missing-cloud rows on an org DB are
//! seeded onto that pool when its queue is empty. Consumer-plane statements
//! still use `begin_account_scoped`; org ticks open
//! `begin_tenant_system_scoped_without_account`. The account-less seed →
//! `accounts` fan-out is consumer-only (`accounts` is control-plane). Org
//! seeds carry the tenant's account_id from `tenant_databases`.

use std::collections::HashMap;
use std::time::Duration;

use chrono::{DateTime, Utc};
use sqlx::{PgPool, Row};
use uuid::Uuid;

use coralswarm_core::chunker::{build_meeting_header, ChunkStrategy, CHUNKER_VERSION};
use coralswarm_core::embed::{EmbedProvider, EMBED_MODEL_NAME};
use coralswarm_core::r2::R2Client;

use crate::jobs::queue::{self, JobRow};
use crate::jobs::JobKind;
use crate::services::blob_store::PromotionBlobStore;
use crate::services::content_replacement::{self, ContentScope, ContentSnapshot};
use crate::store::Store;
use crate::store::StoreError;
use crate::tenancy::scope::{begin_account_scoped, begin_tenant_system_scoped_without_account};
use crate::tenancy::TenantId;
use crate::workers::tenant_harness::{Tick, WorkerTenant};
use crate::workers::extract::base_source_id;

// ── Tuning constants ──────────────────────────────────────────────────────────

/// How many `ReembedOutdatedChunks` jobs to dequeue per poll cycle. Same
/// reasoning as `workers::content_tsv_backfill::JOB_BATCH_SIZE`: at most one
/// active unscoped sweep is normally in flight (each pass enqueues its own
/// single continuation).
const JOB_BATCH_SIZE: usize = 1;

/// How many candidate atoms a single job execution claims and processes.
/// Deliberately much smaller than
/// `workers::content_tsv_backfill::ATOM_BATCH_SIZE` (2,000): that worker
/// does one SQL-only `UPDATE` per batch, this one does a real embedder call
/// plus, for header-reconstructing atoms, an R2 GET (raw transcript) + R2
/// PUT (mutated content) per atom — network-bound work whose latency and
/// failure surface scale with batch size in a way a single `UPDATE`
/// statement's doesn't.
const ATOM_BATCH_SIZE: i64 = 100;

/// Sub-batch size for the embedder call itself, mirroring
/// `workers::embed::EMBED_BATCH_SIZE` exactly (same ONNX self-attention
/// memory-scaling reasoning documented on that constant — duplicated here
/// rather than made `pub(crate)` cross-module, since the two workers'
/// batching loops are independent and this value is small enough that
/// drift risk is low, unlike the regconfig-correctness-critical
/// `update_content_tsv_and_clear_failure`, which IS reused directly).
const EMBED_SUBBATCH_SIZE: usize = 8;

/// Delay before a continuation job becomes eligible to run — spaces out
/// batches so the sweep doesn't monopolise the queue poller or hammer the
/// embedder/R2/DB back-to-back. Longer than
/// `workers::content_tsv_backfill::CONTINUATION_DELAY_SECS` (1s) since this
/// sweep's batches are materially heavier (network-bound, not one `UPDATE`).
const CONTINUATION_DELAY_SECS: i64 = 2;

/// How long to sleep between polls when the queue is empty.
/// `pub(crate)`: fed to the tenant harness as this worker's `base_poll`.
pub(crate) const EMPTY_POLL_SLEEP: Duration = Duration::from_millis(500);

/// Bound on the candidate-selection query, mirroring
/// `workers::embed_retry::SWEEP_DB_TIMEOUT`'s reasoning: a stalled Postgres
/// must not block this worker indefinitely. Deliberately NOT applied to the
/// per-atom R2/embedder calls below (`workers::embed::process_job` doesn't
/// wrap those either) — those clients own their own request timeouts.
const CANDIDATE_QUERY_TIMEOUT: Duration = Duration::from_secs(30);

// ── Errors ────────────────────────────────────────────────────────────────────

/// Typed errors for this worker's JOB-LEVEL failure paths (malformed
/// payload, candidate-query timeout/DB error) — per this repo's Rust path
/// instructions ("library-style crates... define typed errors with
/// `thiserror`"), converted to text only at the `queue::fail` boundary in
/// [`process_job`], not threaded through as `String` the whole way down
/// (CodeRabbit review).
///
/// Deliberately scoped to the job-level chain
/// (`parse_after_atom_id` → `select_candidates` → `run_batch` →
/// `process_job`) and NOT extended to the per-atom helpers below
/// (`prepare_atom`/`persist_mutated_content`/`resolve_meeting_title`):
/// those already carry their failure per-`Uuid` as a logged, non-propagated
/// diagnostic (`(Uuid, String)`) rather than a job-terminating error, and
/// their messages are heterogeneous enough (R2, DEK/crypto, `Store`, plain
/// SQL) that forcing them through one shared enum would mostly just
/// duplicate `{e}` interpolation without adding a distinction any caller
/// actually branches on.
#[derive(Debug, thiserror::Error)]
enum ReembedError {
    #[error("cannot deserialise JobKind from payload: {0}")]
    MalformedJob(String),
    #[error("expected ReembedOutdatedChunks, got {0:?}")]
    WrongJobKind(JobKind),
    #[error("candidate query timed out after {0:?}")]
    QueryTimeout(Duration),
    #[error("database error: {0}")]
    Db(#[from] sqlx::Error),
}

// ── Worker entry point ────────────────────────────────────────────────────────

/// One re-embed pass on one tenant's (pool, store) pair. Driven by
/// `workers::tenant_harness::run_multi_tenant`. An empty queue on a pool that
/// still has leftover missing-cloud rows enqueues `reembed_cloud_seed` against
/// **that** pool so org leftovers are not stranded on the consumer plane.
pub async fn tick(
    t: &WorkerTenant,
    embedder: &(dyn EmbedProvider + Send + Sync),
    cloud_embedder: Option<&(dyn EmbedProvider + Send + Sync)>,
    r2: Option<&R2Client>,
    control_pool: Option<&PgPool>,
) -> Result<Tick, String> {
    // No cloud-gap private scan or automatic seed without managed admission.
    if crate::managed_history::automatic_sweep_blocked("ReembedOutdatedChunks") { return Ok(Tick::idle()); }
    let jobs = queue::dequeue(&t.pool, &["ReembedOutdatedChunks"], JOB_BATCH_SIZE)
        .await
        .map_err(|e| format!("dequeue failed: {e}"))?;

    if jobs.is_empty() {
        let seeded = maybe_seed_cloud_gap(t, control_pool, cloud_embedder.is_some()).await;
        return Ok(if seeded { Tick::worked(1) } else { Tick::idle() });
    }

    let n = jobs.len();
    for job in jobs {
        process_managed_job(
            &t.tenant,
            &t.pool,
            t.store.as_ref(),
            embedder,
            cloud_embedder,
            r2,
            control_pool,
            job,
        )
        .await;
    }
    Ok(Tick::worked(n))
}

/// Seed a sweep on this pool when bge-backed atoms are still missing cloud.
/// Skips when a `ReembedOutdatedChunks` job is already pending/running so a
/// second empty tick does not insert a duplicate while the first is claimed.
async fn maybe_seed_cloud_gap(
    t: &WorkerTenant,
    control_pool: Option<&PgPool>,
    cloud_embedder_present: bool,
) -> bool {
    if !cloud_embedder_present {
        return false;
    }
    let in_flight: i64 = match sqlx::query_scalar(
        "SELECT count(*)::bigint FROM jobs \
         WHERE kind = 'ReembedOutdatedChunks' \
           AND status IN ('pending', 'running')",
    )
    .fetch_one(&t.pool)
    .await
    {
        Ok(n) => n,
        Err(e) => {
            tracing::warn!(error = %e, "reembed backfill: in-flight count failed");
            return false;
        }
    };
    if in_flight > 0 {
        return false;
    }

    let missing = match count_missing_cloud(&t.tenant, &t.pool).await {
        Ok(n) => n,
        Err(e) => {
            tracing::warn!(error = %e, "reembed backfill: leftover cloud count failed");
            return false;
        }
    };
    if missing == 0 {
        return false;
    }

    let account_id = match &t.tenant {
        TenantId::Consumer => None,
        TenantId::Org(org) => {
            let Some(control) = control_pool else {
                tracing::warn!("reembed backfill: org leftover seed needs the control pool");
                return false;
            };
            match crate::workers::billing_derivation::resolve_org_account_id(control, org).await {
                Ok(id) => Some(id),
                Err(e) => {
                    tracing::warn!(error = %e, "reembed backfill: org account resolve failed");
                    return false;
                }
            }
        }
    };

    match queue::enqueue(
        &t.pool,
        &JobKind::ReembedOutdatedChunks { after_atom_id: None },
        account_id,
        Some("reembed_cloud_seed"),
        0,
    )
    .await
    {
        Ok(Some(_)) => {
            tracing::info!(missing, "reembed backfill: seeded leftover cloud gap");
            true
        }
        Ok(None) => false,
        Err(e) => {
            tracing::warn!(error = %e, "reembed backfill: leftover seed enqueue failed");
            false
        }
    }
}

async fn count_missing_cloud(tenant: &TenantId, pool: &PgPool) -> Result<i64, sqlx::Error> {
    let sql = "SELECT count(*)::bigint FROM atoms \
               WHERE embedding IS NOT NULL \
                 AND embed_model_next IS DISTINCT FROM $1 \
                 AND embed_model_next IS DISTINCT FROM $2";
    match tenant {
        TenantId::Consumer => {
            sqlx::query_scalar(sql)
                .bind(coralswarm_core::embed::EMBED_MODEL_CLOUD)
                .bind(crate::workers::embed::SKIPPED_LOCAL_ONLY)
                .fetch_one(pool)
                .await
        }
        TenantId::Org(_) => {
            let mut tx = begin_tenant_system_scoped_without_account(pool)
                .await
                .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
            let n = sqlx::query_scalar(sql)
                .bind(coralswarm_core::embed::EMBED_MODEL_CLOUD)
                .bind(crate::workers::embed::SKIPPED_LOCAL_ONLY)
                .fetch_one(&mut *tx)
                .await?;
            tx.commit().await?;
            Ok(n)
        }
    }
}

async fn begin_reembed_scope(
    tenant: &TenantId,
    pool: &PgPool,
    account_id: Uuid,
) -> Result<sqlx::Transaction<'static, sqlx::Postgres>, StoreError> {
    match tenant {
        TenantId::Consumer => begin_account_scoped(pool, account_id).await,
        TenantId::Org(_) => begin_tenant_system_scoped_without_account(pool).await,
    }
}

// ── Job processing ────────────────────────────────────────────────────────────

fn parse_after_atom_id(job: &JobRow) -> Result<Option<Uuid>, ReembedError> {
    let tagged = serde_json::json!({ "kind": job.kind, "payload": job.payload });
    let kind: JobKind = serde_json::from_value(tagged).map_err(|e| ReembedError::MalformedJob(e.to_string()))?;

    match kind {
        JobKind::ReembedOutdatedChunks { after_atom_id } => Ok(after_atom_id),
        other => Err(ReembedError::WrongJobKind(other)),
    }
}

async fn process_managed_job(
    tenant: &TenantId,
    pool: &PgPool,
    store: &dyn Store,
    _embedder: &dyn EmbedProvider,
    cloud: Option<&(dyn EmbedProvider + Send + Sync)>,
    r2: Option<&R2Client>,
    control: Option<&PgPool>,
    job: JobRow,
) {
    let Some(control) = control else {
        return;
    };
    let Some(cloud) = cloud.filter(|cloud| cloud.gated().is_some()) else {
        return;
    };
    let cursor = match parse_after_atom_id(&job) {
        Ok(cursor) => cursor,
        Err(error) => {
            tracing::warn!(%error,"managed reembed payload invalid");
            return;
        }
    };
    let Some(account) = job.account_id else {
        if matches!(tenant, TenantId::Consumer) {
            fan_out_per_account(pool, job.id, cursor).await;
        }
        return;
    };
    let selected = match select_candidates(tenant, pool, account, cursor, true).await {
        Ok(selected) => selected,
        Err(error) => {
            tracing::warn!(%error,"managed reembed candidates unavailable");
            return;
        }
    };
    let batch_full = selected.raw_count as i64 == ATOM_BATCH_SIZE;
    let last = selected.last_atom_id;
    let attempted = selected.candidates.len();
    let dek = match crate::crypto::resolve_dek_for_tenant(tenant, pool, account).await {
        Ok(Some(dek)) => dek,
        _ => return,
    };
    let prepared = futures::future::join_all(selected.candidates.into_iter().map(|candidate| {
        prepare_atom(
            tenant,
            pool,
            store,
            r2.map(|r2| r2 as &dyn PromotionBlobStore),
            &dek,
            account,
            candidate,
        )
    }))
    .await;
    let ids: Vec<_> = prepared
        .into_iter()
        .filter_map(|result| result.ok().map(|atom| atom.atom_id))
        .collect();
    let incomplete = ids.len() != attempted;
    crate::managed::embedding::with_failure_cause(async {
        let deny = match crate::managed::embedding::process(
            tenant,
            pool,
            control,
            &job,
            &ids,
            cloud,
            r2.map(|r2| r2 as &dyn PromotionBlobStore),
            store,
        )
        .await
        {
            Ok(report) => {
                let retry = incomplete || report.blocked > 0 || report.discarded_stale > 0;
                let next = if retry {
                    Some((
                        JobKind::ReembedOutdatedChunks {
                            after_atom_id: cursor,
                        },
                        600,
                    ))
                } else if batch_full {
                    last.map(|last| {
                        (
                            JobKind::ReembedOutdatedChunks {
                                after_atom_id: Some(last),
                            },
                            CONTINUATION_DELAY_SECS,
                        )
                    })
                } else {
                    None
                };
                match report
                    .finish(
                        tenant,
                        pool,
                        next.as_ref().map(|(kind, delay)| (kind, *delay)),
                    )
                    .await
                {
                    Ok(()) => return,
                    Err(deny) => deny,
                }
            }
            Err(deny) => deny,
        };
        // Settled, never left `running` for the reaper (`settle_unprocessed`).
        // These atoms already have a vector, so none is charged an attempt.
        match crate::managed::embedding::settle_unprocessed(tenant, pool, &job, deny, &[]).await {
            Ok(settled) => tracing::warn!(job=%job.id,%deny,?settled,"managed reembed job did not complete"),
            Err(error) => tracing::warn!(%error,job=%job.id,%deny,"managed reembed job unsettled; the reaper will reset it"),
        }
    })
    .await
}

/// `pub` (like `workers::infer_expertise::run_expertise`) so integration
/// tests can drive one seed/child job against a real Postgres without going
/// through the worker loop.
pub async fn process_job(
    tenant: &TenantId,
    pool: &PgPool,
    store: &dyn Store,
    embedder: &dyn EmbedProvider,
    cloud_embedder: Option<&(dyn EmbedProvider + Send + Sync)>,
    r2: Option<&R2Client>,
    job: JobRow,
) {
    // This implementation dispatches cloud vectors, never the local CPU model.
    if crate::managed_history::block_legacy_paid_job(pool, &job).await { return; }

    let after_atom_id = match parse_after_atom_id(&job) {
        Ok(c) => c,
        Err(e) => {
            // `e` is a typed `ReembedError` end to end up to here — stringified
            // only now, at the `queue::fail` boundary (CodeRabbit review).
            tracing::error!(job_id = %job.id, error = %e, "reembed backfill: malformed job payload");
            if let Err(qe) = queue::fail(pool, job.id, &e.to_string()).await {
                tracing::error!(error = %qe, job_id = %job.id, "failed to mark malformed job as failed");
            }
            return;
        }
    };

    // ── Account-less job ⇒ SEED: fan out one child per account (org-tenancy
    // Phase W.2 — see the module doc's RLS section). The seed itself touches
    // no RLS-covered table, only `accounts` (control) and `jobs`. Org leftover
    // seeds carry account_id; an account-less job on an org pool is a bug.
    let Some(account_id) = job.account_id else {
        if matches!(tenant, TenantId::Org(_)) {
            tracing::error!(job_id = %job.id, "reembed backfill: refusing unscoped org seed");
            if let Err(qe) = queue::fail(pool, job.id, "org reembed seed must carry account_id").await {
                tracing::error!(error = %qe, job_id = %job.id, "failed to mark unscoped org seed as failed");
            }
            return;
        }
        fan_out_per_account(pool, job.id, after_atom_id).await;
        return;
    };

    match run_batch(tenant, pool, store, embedder, cloud_embedder, r2, account_id, after_atom_id).await {
        Ok(outcome) => {
            tracing::info!(
                job_id = %job.id,
                examined = outcome.examined,
                reembedded = outcome.reembedded,
                batch_full = outcome.batch_full,
                all_succeeded = outcome.all_succeeded,
                "reembed backfill: batch complete"
            );

            if !outcome.all_succeeded {
                // At least one candidate this pass failed to fully converge
                // (DEK, prepare, embed, or write failure — see
                // `BatchOutcome::all_succeeded`'s doc comment). The cursor
                // must NOT advance past it: fail the job (unchanged
                // `after_atom_id`) so `jobs::queue`'s own bounded
                // attempts/backoff retries this SAME window automatically,
                // instead of silently stranding the failed atoms until an
                // operator notices and hand-inserts a fresh sweep.
                let msg = format!(
                    "{}/{} candidates failed to fully re-embed this pass",
                    outcome.examined.saturating_sub(outcome.reembedded),
                    outcome.examined
                );
                tracing::warn!(job_id = %job.id, %msg, "reembed backfill: batch had failures — failing job to retry the same window");
                if let Err(qe) = queue::fail(pool, job.id, &msg).await {
                    tracing::error!(error = %qe, job_id = %job.id, "failed to mark partially-failed reembed batch job as failed");
                }
                return;
            }

            if outcome.batch_full {
                if let Some(cursor) = outcome.last_atom_id {
                    // Per-account continuation: propagate this child's own
                    // account stamp (org-tenancy Phase W.2 — see the module
                    // doc's RLS section). The key includes the account so two
                    // accounts' independent sweeps can never dedup each other.
                    let idempotency = format!("reembed_outdated_chunks:{account_id}:{cursor}");
                    if let Err(e) = queue::enqueue(
                        pool,
                        &JobKind::ReembedOutdatedChunks { after_atom_id: Some(cursor) },
                        Some(account_id),
                        Some(&idempotency),
                        CONTINUATION_DELAY_SECS,
                    )
                    .await
                    {
                        // Same reasoning as `workers::content_tsv_backfill`:
                        // a silently-dropped continuation here would
                        // permanently strand every atom beyond this window —
                        // fail THIS job instead of completing it, so its own
                        // bounded retry gets another chance (cheap: this
                        // sweep is idempotent, so re-covering this window is
                        // wasted work, not wrong results).
                        tracing::error!(job_id = %job.id, error = %e, "reembed backfill: failed to enqueue continuation job — failing this job to retry");
                        if let Err(qe) = queue::fail(pool, job.id, &e.to_string()).await {
                            tracing::error!(error = %qe, job_id = %job.id, "failed to mark job failed after continuation enqueue failure");
                        }
                        return;
                    }
                }
            }

            // `outcome.reembedded == 0` means this pass's candidate window
            // came back empty — a legitimate no-op for a sweep worker.
            let job_outcome = if outcome.reembedded > 0 {
                queue::JobOutcome::DidWork
            } else {
                queue::JobOutcome::NothingToDo
            };
            if let Err(e) = queue::complete(pool, job.id, job_outcome).await {
                tracing::error!(error = %e, job_id = %job.id, "reembed backfill: failed to complete job");
            } else if should_enqueue_cloud_resweep(
                cloud_embedder.is_some(),
                outcome.batch_full,
                outcome.reembedded,
            ) {
                // The account is caught up. Ask again later so a laptop-synced
                // bge vector written after this pass still gets a cloud vector
                // beside it before search is allowed to flip.
                let idempotency = format!("reembed_outdated_chunks:{account_id}:resweep");
                if let Err(e) = queue::enqueue(
                    pool,
                    &JobKind::ReembedOutdatedChunks { after_atom_id: None },
                    Some(account_id),
                    Some(&idempotency),
                    600,
                )
                .await
                {
                    tracing::warn!(job_id = %job.id, error = %e, "reembed backfill: failed to schedule the next cloud pass");
                }
            }
        }
        Err(e) => {
            // Stringified only here, at the `queue::fail` boundary — see
            // `ReembedError`'s doc comment.
            tracing::error!(job_id = %job.id, error = %e, "reembed backfill: candidate query failed — failing job for retry");
            if let Err(qe) = queue::fail(pool, job.id, &e.to_string()).await {
                tracing::error!(error = %qe, job_id = %job.id, "failed to mark reembed backfill job failed");
            }
        }
    }
}

// ── Pure logic (unit-tested without a database) ────────────────────────────────

/// Rust mirror of the candidate query's
/// `chunker_version IS DISTINCT FROM $current OR embed_model IS DISTINCT
/// FROM $active` clause — `Option<T> != Option<T>` already has the same
/// NULL-safe "distinct" semantics `IS DISTINCT FROM` does (unlike `=`,
/// which in SQL is `NULL` — never `true` — when either side is `NULL`), so
/// this is a direct, not merely analogous, translation. Kept in lockstep
/// with the SQL by [`needs_reembed_matches_current_atoms_are_excluded`]
/// below — same cross-check pattern
/// `workers::embed::embed_backoff_matches_sql_formula` uses for its own
/// SQL-mirroring formula.
fn needs_reembed(chunker_version: Option<i16>, embed_model: Option<&str>) -> bool {
    chunker_version != Some(CHUNKER_VERSION) || embed_model != Some(EMBED_MODEL_NAME)
}

/// The side column is missing or stamped with something other than the cloud
/// model. NULL is missing: `IS DISTINCT FROM` treats it as not current.
/// Local-only skip stamps are finished, not missing.
fn needs_cloud_vector(embed_model_next: Option<&str>) -> bool {
    match embed_model_next {
        Some(coralswarm_core::embed::EMBED_MODEL_CLOUD) => false,
        Some(crate::workers::embed::SKIPPED_LOCAL_ONLY) => false,
        _ => true,
    }
}

/// Resweep only after a cloud-enabled pass that actually re-embedded
/// something and did not already schedule a continuation (`batch_full`).
/// An empty window must complete without enqueueing another delay, or
/// the worker loops on no-op `ReembedOutdatedChunks` jobs.
fn should_enqueue_cloud_resweep(
    cloud_embedder_present: bool,
    batch_full: bool,
    reembedded: usize,
) -> bool {
    cloud_embedder_present && !batch_full && reembedded > 0
}

/// The outcome of deciding how to re-embed one candidate atom, computed
/// entirely from in-memory inputs — no I/O, so this (and [`plan_reembed`],
/// which produces it) is directly unit-testable.
#[derive(Debug, PartialEq, Eq)]
struct ReembedPlan {
    /// The text to feed the embedder — always the atom's FULL content
    /// (existing header, if any, is NEVER stripped from this — see the
    /// CodeRabbit-flagged bug this replaced, in this struct's history).
    text_to_embed: String,
    /// `Some((new_content, new_header_len))` when [`plan_reembed`]
    /// reconstructed a NEW header that must be persisted (content write +
    /// `header_len` stamp) before the embed call — see this module's doc
    /// comment on the persist-before-stamp ordering. `None` means the
    /// atom's stored content is unchanged: either it already had a header
    /// (matched exactly, or a different one this function must never
    /// clobber) or the atom isn't meeting-shaped.
    content_mutation: Option<(String, i32)>,
    /// `Some(correct_header_len)` when `current_content` ALREADY starts with
    /// the exact header this function would have injected, but the
    /// `header_len` the caller passed in doesn't match — i.e. a PRIOR pass's
    /// content write landed but its `header_len` stamp didn't (a partial
    /// failure between the two — see [`persist_mutated_content`]'s doc
    /// comment). No content mutation is needed (the content is already
    /// correct), just a cheap metadata repair.
    header_len_repair: Option<i32>,
}

/// Decide what to (re-)embed for one candidate atom, and whether a
/// contextual chunk header needs to be reconstructed and persisted first.
///
/// `current_content` is the atom's FULL persisted content, taken VERBATIM —
/// never pre-stripped by the caller. That's deliberate, not an oversight:
/// this function detects for itself whether a header is already present by
/// checking whether `current_content` starts with the EXACT header it would
/// reconstruct right now, rather than trusting `header_len` alone. Two
/// atoms this makes correct that a `header_len`-only check would not:
///
///   1. **An atom that already has a v2 header** (`header_len > 0`, the
///      common "just needs model/version uniformity" case) — a caller that
///      pre-stripped the header before calling this would silently lose it
///      on every re-embed, re-embedding body-only forever (this was a real
///      bug in an earlier version of this function, caught in review).
///   2. **A partial prior failure that persisted the NEW header but never
///      reached the `header_len` UPDATE** (`header_len` still says `0`/old,
///      but the content, when re-fetched, already carries the header). Re-deriving
///      idempotency from CONTENT rather than from a separate counter means
///      a retry after exactly this failure mode reconstructs the identical
///      header string, recognises it's already there, and skips straight to
///      `header_len_repair` instead of double-prepending.
///
/// Reconstruction is attempted only for meeting-shaped sources
/// (`ChunkStrategy::Transcript` — the exact scope
/// `ingest::atom::build_contextual_header` uses at ingest time);
/// `build_meeting_header` is guaranteed to return `Some` (not `None`) for
/// every such candidate (the date clause always has `timestamp`, `NOT NULL`
/// on `atoms`) — see this module's doc comment ("Header reconstruction") for
/// the full coverage argument.
fn plan_reembed(
    source_type: &str,
    header_len: i32,
    current_content: &str,
    title: Option<&str>,
    timestamp: DateTime<Utc>,
    participants: &[String],
    project: Option<&str>,
) -> ReembedPlan {
    let no_op = || ReembedPlan {
        text_to_embed: current_content.to_string(),
        content_mutation: None,
        header_len_repair: None,
    };

    let is_meeting = matches!(ChunkStrategy::for_source_type(source_type), ChunkStrategy::Transcript);
    let header = if is_meeting {
        build_meeting_header(title, Some(timestamp), participants)
    } else if coralswarm_core::chunker::provenance_source(source_type) {
        coralswarm_core::chunker::build_provenance_header(title, Some(timestamp), project)
    } else {
        None
    };
    let Some(header) = header else {
        // Defensive only — see this function's doc comment on why this
        // branch is not expected to be reachable for any meeting-shaped
        // candidate (timestamp is always known), but a future change to
        // `build_meeting_header`'s omission rules must not panic or corrupt
        // content if it ever is.
        return no_op();
    };
    // `+ 2` accounts for the "\n\n" separator, matching `routes::ingest`'s
    // `header_len` computation exactly (see
    // `contextual_header_len_accounts_for_separator` there).
    let expected_prefix_len = header.len() + 2;
    let expected_prefix = format!("{header}\n\n");

    if current_content.starts_with(&expected_prefix) {
        // Already carries EXACTLY this header — see case 2 above.
        let correct_header_len = expected_prefix_len as i32;
        let header_len_repair = (header_len != correct_header_len).then_some(correct_header_len);
        return ReembedPlan { text_to_embed: current_content.to_string(), content_mutation: None, header_len_repair };
    }

    if header_len > 0 {
        // Already headered with something ELSE (title/participants changed
        // since ingest, or some other header entirely) — never clobber
        // existing content. See case 1 above.
        return no_op();
    }

    // Genuinely headerless: inject the reconstructed header.
    let new_content = format!("{expected_prefix}{current_content}");
    ReembedPlan {
        text_to_embed: new_content.clone(),
        content_mutation: Some((new_content, expected_prefix_len as i32)),
        header_len_repair: None,
    }
}

/// One candidate atom returned by [`select_candidates`].
struct Candidate {
    atom_id: Uuid,
    graph_id: Uuid,
    account_id: Uuid,
    source_type: String,
    source_id: String,
    content_blob_key: String,
    content_inline: Option<String>,
    content_revision: i64,
    stored_header_len: Option<i32>,
    participants: Vec<String>,
    timestamp: DateTime<Utc>,
    header_len: i32,
    fts_language: String,
    title: Option<String>,
    project: Option<String>,
    /// Live bge vector / chunker stamp is behind. Those atoms go through the
    /// ONNX embedder. Cloud-only rows do not.
    bge_stale: bool,
    needs_cloud: bool,
}

/// Partition candidates by owning account — same shape and purpose as
/// `workers::embed_retry::group_by_account`: each account's DEK resolution
/// is independent, so one account's DEK failure must not block another
/// account's atoms in the same batch. `HashMap` iteration order is
/// unspecified and irrelevant — [`run_batch`] processes every group
/// regardless of order.
fn group_by_account(candidates: Vec<Candidate>) -> HashMap<Uuid, Vec<Candidate>> {
    let mut by_account: HashMap<Uuid, Vec<Candidate>> = HashMap::new();
    for c in candidates {
        by_account.entry(c.account_id).or_default().push(c);
    }
    by_account
}

// ── Candidate selection ─────────────────────────────────────────────────────────

/// Result of one candidate-selection query: the (possibly defensively
/// filtered — see [`select_candidates`]) candidates to process, plus batch
/// bookkeeping derived from the RAW row count/max `atom_id` the query
/// returned — deliberately independent of the defensive filter below, so a
/// (never-expected) filtered-out row still counts toward `batch_full` and
/// still advances the continuation cursor past itself.
struct SelectResult {
    candidates: Vec<Candidate>,
    /// Row count as returned by the SQL query, before any defensive filter.
    raw_count: usize,
    /// Highest `atom_id` among ALL rows returned (filtered or not).
    last_atom_id: Option<Uuid>,
}

async fn select_candidates(
    tenant: &TenantId,
    pool: &PgPool,
    account_id: Uuid,
    after_atom_id: Option<Uuid>,
    cloud_enabled: bool,
) -> Result<SelectResult, ReembedError> {
    // Consumer-plane RLS scope (org-tenancy Phase W.2): the read runs inside
    // a `begin_account_scoped` transaction, and the explicit `$5` account
    // predicate is the primary scoping mechanism even pre-OPS-4 (see module
    // doc's RLS section). Org ticks use `begin_tenant_system_scoped_without_account`.
    let rows = tokio::time::timeout(CANDIDATE_QUERY_TIMEOUT, async {
        let mut tx = begin_reembed_scope(tenant, pool, account_id)
            .await
            .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
        let rows = sqlx::query(
            r#"
            SELECT a.atom_id, a.graph_id, g.account_id, a.source_type, a.source_id,
                   a.content_blob_key, a.content_inline, a.content_revision, a.header_len AS stored_header_len, a.participants, a.timestamp,
                   COALESCE(a.header_len, 0)            AS header_len,
                   COALESCE(a.fts_language, 'english')  AS fts_language,
                   a.chunker_version, a.embed_model, a.embed_model_next,
                   a.title, a.project
            FROM   atoms a
            JOIN   graphs g ON g.id = a.graph_id
            WHERE  a.embedding IS NOT NULL
              AND  g.account_id = $3
              AND  $4::text IS NOT NULL
              AND  a.embed_model_next IS DISTINCT FROM $4
              AND  a.embed_model_next IS DISTINCT FROM $5
              AND  ($1::uuid IS NULL OR a.atom_id > $1)
            ORDER BY a.atom_id
            LIMIT  $2
            "#,
        )
        .bind(after_atom_id)
        .bind(ATOM_BATCH_SIZE)
        .bind(account_id)
        .bind(cloud_enabled.then_some(coralswarm_core::embed::EMBED_MODEL_CLOUD))
        .bind(crate::workers::embed::SKIPPED_LOCAL_ONLY)
        .fetch_all(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok::<_, sqlx::Error>(rows)
    })
    .await
    .map_err(|_| ReembedError::QueryTimeout(CANDIDATE_QUERY_TIMEOUT))??;

    let raw_count = rows.len();
    let last_atom_id = rows.iter().map(|r| r.get::<Uuid, _>("atom_id")).max();

    let candidates: Vec<Candidate> = rows
        .into_iter()
        .filter_map(|r| {
            let atom_id: Uuid = r.get("atom_id");
            let chunker_version: Option<i16> = r.get("chunker_version");
            let embed_model: Option<String> = r.get("embed_model");
            let embed_model_next: Option<String> = r.get("embed_model_next");
            let bge_stale = needs_reembed(chunker_version, embed_model.as_deref());
            let needs_cloud = cloud_enabled && needs_cloud_vector(embed_model_next.as_deref());
            // Server search reads embedding_next only. Do not select rows just
            // because the live bge column is behind — rewriting it with the
            // cloud embedder would mix spaces on atoms.embedding.
            if !needs_cloud {
                tracing::error!(
                    %atom_id,
                    ?chunker_version,
                    ?embed_model,
                    ?embed_model_next,
                    "reembed backfill: candidate query returned an atom that is already current — \
                     SQL/Rust predicate drift, skipping this row defensively"
                );
                return None;
            }
            Some(Candidate {
                atom_id,
                graph_id: r.get("graph_id"),
                account_id: r.get("account_id"),
                source_type: r.get("source_type"),
                source_id: r.get("source_id"),
                content_blob_key: r.get("content_blob_key"),
                content_inline: r.get("content_inline"),
                content_revision: r.get("content_revision"),
                stored_header_len: r.get("stored_header_len"),
                participants: r.get("participants"),
                timestamp: r.get("timestamp"),
                header_len: r.get("header_len"),
                fts_language: r.get("fts_language"),
                title: r.get("title"),
                project: r.get("project"),
                bge_stale,
                needs_cloud,
            })
        })
        .collect();

    Ok(SelectResult { candidates, raw_count, last_atom_id })
}

// ── Batch processing ────────────────────────────────────────────────────────────

struct BatchOutcome {
    examined: usize,
    reembedded: usize,
    batch_full: bool,
    /// `atom_id` of the LAST (highest) row EXAMINED this pass. Only
    /// meaningful — and only ever used by [`process_job`] — when
    /// [`Self::all_succeeded`] is `true`: see that field's doc comment for
    /// why a batch containing ANY failure must not advance past it.
    last_atom_id: Option<Uuid>,
    /// `true` iff `reembedded == attempted` (every candidate this pass
    /// looked at fully converged: embedded, `content_tsv` rebuilt, and
    /// current `chunker_version`/`embed_model` stamped) — including the
    /// trivial case of zero candidates (sweep complete).
    ///
    /// [`process_job`] gates BOTH advancing the continuation cursor AND
    /// completing the job on this being `true`. Earlier versions of this
    /// worker advanced the cursor unconditionally, on the same reasoning
    /// `workers::content_tsv_backfill` uses — but that reasoning only holds
    /// for a batch whose unit of work is one all-or-nothing SQL statement.
    /// This worker's batches have genuine PARTIAL failure modes (a DEK
    /// failure, an embedder timeout, an R2 fetch/put failure — see
    /// `process_account_group`/`prepare_atom`), and advancing past a
    /// partially-failed window silently strands the failed atoms: nothing
    /// short of an operator noticing and hand-inserting a fresh unscoped
    /// sweep would ever revisit them (CodeRabbit review). Failing the job
    /// instead — cursor untouched — lets `jobs::queue`'s own bounded
    /// attempts/backoff retry the SAME window automatically first.
    all_succeeded: bool,
}

async fn run_batch(
    tenant: &TenantId,
    pool: &PgPool,
    store: &dyn Store,
    embedder: &dyn EmbedProvider,
    cloud_embedder: Option<&(dyn EmbedProvider + Send + Sync)>,
    r2: Option<&R2Client>,
    job_account_id: Uuid,
    after_atom_id: Option<Uuid>,
) -> Result<BatchOutcome, ReembedError> {
    let cloud_enabled = cloud_embedder.is_some();
    let SelectResult { candidates, raw_count, last_atom_id } =
        select_candidates(tenant, pool, job_account_id, after_atom_id, cloud_enabled).await?;

    // `batch_full` and the continuation cursor are derived from `raw_count`
    // / `last_atom_id` (the RAW row count/max atom_id, before the defensive
    // `needs_reembed` filter in `select_candidates`) — see [`SelectResult`]'s
    // doc comment for why: a (never-expected) filtered-out row must still
    // advance the cursor past itself and still count toward "more likely
    // remain", exactly as if it had been processed.
    let batch_full = raw_count as i64 == ATOM_BATCH_SIZE;
    // "Examined" mirrors `workers::content_tsv_backfill`'s `BatchOutcome`
    // semantics: how many rows this pass looked at (raw), not how many
    // survived the defensive filter above.
    let examined = raw_count;

    if raw_count == 0 {
        return Ok(BatchOutcome { examined: 0, reembedded: 0, batch_full: false, last_atom_id: None, all_succeeded: true });
    }

    // `attempted` — the post-defensive-filter set this pass actually tries
    // to process — is what `reembedded` is compared against for
    // `all_succeeded`, NOT `raw_count`/`examined` (see [`SelectResult`]'s
    // doc comment: a defensively-filtered row was never attempted, so it
    // can't count as a failure).
    let attempted = candidates.len();

    let mut reembedded = 0usize;
    // The account-filtered candidate query above makes this a single-group
    // loop (every candidate belongs to `job_account_id`); `group_by_account`
    // is kept as defense-in-depth, and a group for any OTHER account —
    // impossible unless the SQL predicate and this check drift apart — is
    // refused loudly rather than processed under the wrong scope.
    for (account_id, group) in group_by_account(candidates) {
        if account_id != job_account_id {
            tracing::error!(
                %account_id,
                %job_account_id,
                atoms = group.len(),
                "reembed backfill: candidate group belongs to a different account than the job's \
                 stamp — refusing to process it (SQL predicate drift?)"
            );
            continue;
        }
        reembedded +=
            process_account_group(tenant, pool, store, embedder, cloud_embedder, r2, account_id, group).await;
    }

    Ok(BatchOutcome { examined, reembedded, batch_full, last_atom_id, all_succeeded: reembedded == attempted })
}

/// SEED handling for the account-less operator-inserted job: enumerate the
/// account roster and enqueue one per-account child (same cursor payload,
/// `jobs.account_id` stamped). Any failure fails the seed for a bounded queue
/// retry; per-account idempotency keys make a retried seed re-enqueue only
/// the children that didn't make it. Same shape as
/// `workers::content_tsv_backfill::fan_out_per_account`.
async fn fan_out_per_account(pool: &PgPool, job_id: Uuid, after_atom_id: Option<Uuid>) {
    let fail_seed = |msg: String| async move {
        tracing::error!(job_id = %job_id, error = %msg, "reembed backfill: seed fan-out failed — failing seed job to retry");
        if let Err(qe) = queue::fail(pool, job_id, &msg).await {
            tracing::error!(error = %qe, job_id = %job_id, "failed to mark seed job as failed");
        }
    };

    // Account roster — control-table read, needs no scope (see module doc).
    let account_ids: Vec<Uuid> =
        match sqlx::query_scalar("SELECT id FROM accounts ORDER BY id").fetch_all(pool).await {
            Ok(ids) => ids,
            Err(e) => {
                fail_seed(format!("seed account enumeration failed: {e}")).await;
                return;
            }
        };

    for account_id in &account_ids {
        let cursor_label =
            after_atom_id.map(|c| c.to_string()).unwrap_or_else(|| "start".to_string());
        let idempotency = format!("reembed_outdated_chunks:{account_id}:{cursor_label}");
        if let Err(e) = queue::enqueue(
            pool,
            &JobKind::ReembedOutdatedChunks { after_atom_id },
            Some(*account_id),
            Some(&idempotency),
            CONTINUATION_DELAY_SECS,
        )
        .await
        {
            fail_seed(format!("seed child enqueue failed for account {account_id}: {e}")).await;
            return;
        }
    }

    tracing::info!(
        job_id = %job_id,
        accounts = account_ids.len(),
        "reembed backfill: seed fanned out one per-account child job per account"
    );
    if let Err(e) = queue::complete(pool, job_id, queue::JobOutcome::DidWork).await {
        tracing::error!(error = %e, job_id = %job_id, "reembed backfill: failed to complete seed job");
    }
}

/// Re-embed every candidate atom for ONE account: resolve its DEK once,
/// prepare (fetch content, reconstruct + persist a header where applicable)
/// every atom concurrently, then embed the survivors in
/// [`EMBED_SUBBATCH_SIZE`] chunks. A DEK failure (or an account with no DEK
/// provisioned) skips the WHOLE group — logged, non-fatal to the batch —
/// exactly like `workers::embed::resolve_dek_for_atoms`'s callers treat that
/// failure. Returns the number of atoms successfully re-embedded.
async fn process_account_group(
    tenant: &TenantId,
    pool: &PgPool,
    store: &dyn Store,
    _embedder: &dyn EmbedProvider,
    cloud_embedder: Option<&(dyn EmbedProvider + Send + Sync)>,
    r2: Option<&R2Client>,
    account_id: Uuid,
    group: Vec<Candidate>,
) -> usize {
    let dek: [u8; 32] = match crate::crypto::resolve_dek_for_tenant(tenant, pool, account_id).await {
        Ok(Some(d)) => d,
        Ok(None) => {
            tracing::warn!(%account_id, "reembed backfill: account has no DEK — skipping its atoms this pass");
            return 0;
        }
        Err(e) => {
            tracing::warn!(%account_id, error = %e, "reembed backfill: DEK resolution failed — skipping its atoms this pass");
            return 0;
        }
    };

    // ── Prepare phase: fetch current content, reconstruct + PERSIST a
    // header where applicable, all BEFORE any embedding is computed — see
    // this module's doc comment on why content must land before the
    // chunker_version bump. Concurrent across the account's whole group,
    // mirroring `routes::ingest`'s bulk R2 upload's `future::join_all` use.
    let prep_futures = group
        .into_iter()
        .map(|c| prepare_atom(tenant, pool, store, r2.map(|r2| r2 as &dyn PromotionBlobStore), &dek, account_id, c));
    let prepared: Vec<PreparedAtom> = futures::future::join_all(prep_futures)
        .await
        .into_iter()
        .filter_map(|r| match r {
            Ok(p) => Some(p),
            Err((atom_id, e)) => {
                tracing::warn!(%atom_id, error = %e, "reembed backfill: failed to prepare atom — this batch will be retried (see BatchOutcome::all_succeeded)");
                None
            }
        })
        .collect();

    // Search reads embedding_next. Do not write the live bge column — the
    // server embedder is the cloud model.
    let mut cloud_only = Vec::new();
    for atom in prepared {
        if atom.needs_cloud {
            cloud_only.push(atom);
        }
    }

    let mut reembedded = 0usize;
    if let Some(cloud) = cloud_embedder {
        for chunk in cloud_only.chunks(EMBED_SUBBATCH_SIZE) {
            let refs: Vec<&PreparedAtom> = chunk.iter().collect();
            #[cfg(test)]
            if write_cloud_side(tenant,pool,store,cloud,account_id,&refs).await {reembedded+=chunk.len();}
            #[cfg(not(test))]
            {let _=(cloud,refs);}

        }
    }

    reembedded
}

#[cfg(test)]
async fn write_cloud_side(
    tenant: &TenantId,
    pool: &PgPool,
    store: &dyn Store,
    cloud: &(dyn EmbedProvider + Send + Sync),
    account_id: Uuid,
    chunk: &[&PreparedAtom],
) -> bool {
    let texts: Vec<&str> = chunk.iter().map(|p| p.text_to_embed.as_str()).collect();
    let ids: Vec<Uuid> = chunk.iter().map(|p| p.atom_id).collect();
    // Test-only: the production pass sends nothing (see the caller), so no
    // account is attributed.
    let purpose = crate::embedding::Purpose::Metered { usage: None, task: "reembed_backfill" };
    let vectors = match crate::embedding::submit(cloud, purpose, &texts).await {
        Ok(v) if v.len() == ids.len() => v,
        Ok(v) => {
            tracing::warn!(got = v.len(), expected = ids.len(), "reembed backfill: cloud batch length mismatch");
            return false;
        }
        Err(e) => {
            tracing::warn!(error = %e, "reembed backfill: cloud embed failed — live bge vector kept");
            return false;
        }
    };
    let texts_owned: Vec<String> = chunk.iter().map(|p| p.text_to_embed.clone()).collect();
    let langs: Vec<String> = chunk.iter().map(|p| p.fts_language.clone()).collect();
    let tsv_result = async {
        let mut tx = begin_reembed_scope(tenant, pool, account_id).await.map_err(|e| e.to_string())?;
        crate::workers::embed::update_content_tsv_and_clear_failure(&mut *tx, &ids, &texts_owned, &langs)
            .await
            .map_err(|e| e.to_string())?;
        tx.commit().await.map_err(|e| e.to_string())
    }
    .await;
    if let Err(e) = tsv_result {
        tracing::warn!(error = %e, "reembed backfill: content_tsv rebuild before cloud write failed");
        return false;
    }
    let wrote = async {
        let mut tx = begin_reembed_scope(tenant, pool, account_id).await.map_err(|e| e.to_string())?;
        store.set_cloud_embeddings_scoped(&mut tx, &ids, &vectors).await.map_err(|e| e.to_string())?;
        tx.commit().await.map_err(|e| e.to_string())
    }
    .await;
    if let Err(e) = wrote {
        tracing::warn!(error = %e, "reembed backfill: set_cloud_embeddings failed");
        return false;
    }
    true
}

/// One atom's outcome from the prepare phase: the (possibly header-injected)
/// text to embed, ready for [`process_account_group`]'s embed sub-batches.
struct PreparedAtom {
    atom_id: Uuid,
    text_to_embed: String,
    fts_language: String,
    needs_cloud: bool,
}

/// Fetch `c`'s current content, decide + persist a header via
/// [`plan_reembed`] when applicable, and return the text to embed. On any
/// I/O failure (R2 fetch, R2 put, or the content_inline/header_len write),
/// returns `Err((atom_id, message))` — the atom is simply left outdated for
/// a future sweep pass (see this module's idempotency argument).
async fn prepare_atom(
    tenant: &TenantId,
    pool: &PgPool,
    store: &dyn Store,
    r2: Option<&dyn PromotionBlobStore>,
    dek: &[u8; 32],
    account_id: Uuid,
    c: Candidate,
) -> Result<PreparedAtom, (Uuid, String)> {
    let current_content = content_replacement::current_text(
        &c.content_blob_key, c.content_inline.as_deref(), r2, Some(dek),
    ).await.map_err(|e| (c.atom_id, e))?;

    // Title resolution is only worth attempting when there's a real chance
    // this atom is still headerless — an atom that already has SOME header
    // (`header_len > 0`) may still turn out to need a `header_len` repair
    // (see `plan_reembed`'s doc comment), but never a NEW title lookup.
    let is_meeting = matches!(ChunkStrategy::for_source_type(&c.source_type), ChunkStrategy::Transcript);
    let title = if c.header_len <= 0 && is_meeting {
        resolve_meeting_title(tenant, pool, store, r2, dek, account_id, c.graph_id, &c.source_id).await
    } else {
        c.title.clone()
    };

    // `current_content` is passed FULL, never pre-stripped — see
    // `plan_reembed`'s doc comment for why that matters.
    let plan = plan_reembed(
        &c.source_type,
        c.header_len,
        &current_content,
        title.as_deref(),
        c.timestamp,
        &c.participants,
        c.project.as_deref(),
    );

    if let Some((new_content, new_header_len)) = &plan.content_mutation {
        persist_mutated_content(tenant, pool, store, r2, dek, account_id, &c, new_content, *new_header_len)
            .await
            .map_err(|e| (c.atom_id, e))?;
    } else if let Some(correct_header_len) = plan.header_len_repair {
        persist_mutated_content(tenant, pool, store, r2,
            dek, account_id, &c, &plan.text_to_embed, correct_header_len)
            .await.map_err(|e| (c.atom_id, e))?;
    }

    Ok(PreparedAtom {
        atom_id: c.atom_id,
        text_to_embed: plan.text_to_embed,
        fts_language: c.fts_language,
        needs_cloud: c.needs_cloud,
    })
}

/// Best-effort meeting title, resolved the SAME way
/// `ingest::atom::build_contextual_header` does at ingest time: the first
/// non-blank line of the meeting's verbatim raw transcript
/// (`meeting_transcripts`, keyed by `(graph_id, base_source_id)`). Returns
/// `None` (not an error) on any miss — no `meeting_transcripts` row, no
/// resolvable raw text, or a first line that doesn't read as a title; the
/// caller degrades to a date-only header in every such case, per this
/// module's coverage argument.
///
/// `meeting_transcripts` is RLS-covered, so the lookup runs inside its own
/// short `begin_account_scoped` transaction (org-tenancy Phase W.2); the
/// trailing R2 fetch deliberately happens AFTER the transaction closes.
async fn resolve_meeting_title(
    tenant: &TenantId,
    pool: &PgPool,
    store: &dyn Store,
    r2: Option<&dyn PromotionBlobStore>,
    dek: &[u8; 32],
    account_id: Uuid,
    graph_id: Uuid,
    source_id: &str,
) -> Option<String> {
    let base = base_source_id(source_id);
    let lookup = async {
        let mut tx = begin_reembed_scope(tenant, pool, account_id).await?;
        let found = store.get_meeting_transcript_scoped(&mut tx, graph_id, &base).await?;
        tx.commit().await?;
        Ok::<_, crate::store::StoreError>(found)
    }
    .await;
    let transcript = match lookup {
        Ok(Some(t)) => t,
        Ok(None) => return None,
        Err(e) => {
            tracing::debug!(%graph_id, source_id = %base, error = %e, "reembed backfill: meeting_transcripts lookup failed; degrading to date-only header");
            return None;
        }
    };

    let raw_text = if let Some(inline) = transcript.raw_inline {
        Some(inline)
    } else if let (Some(blob_key), Some(r2_client)) = (transcript.raw_blob_key.as_deref(), r2) {
        match r2_client.get_blob(blob_key, dek).await {
            Ok(bytes) => Some(String::from_utf8_lossy(&bytes).into_owned()),
            Err(e) => {
                tracing::debug!(%graph_id, source_id = %base, error = %e, "reembed backfill: raw transcript R2 fetch failed; degrading to date-only header");
                None
            }
        }
    } else {
        None
    };

    raw_text.as_deref().and_then(crate::routes::meetings::meeting_title_from_raw)
}

async fn persist_mutated_content(
    tenant: &TenantId,
    pool: &PgPool,
    store: &dyn Store,
    blobs: Option<&dyn PromotionBlobStore>,
    dek: &[u8; 32],
    account_id: Uuid,
    c: &Candidate,
    new_content: &str,
    new_header_len: i32,
) -> Result<(), String> {
    let scope = match tenant {
        TenantId::Consumer => ContentScope::Account { pool, account_id },
        TenantId::Org(_) => ContentScope::Org { pool, account_id },
    };
    let replaced = content_replacement::replace(scope, &ContentSnapshot {
        graph_id: c.graph_id,
        atom_id: c.atom_id,
        revision: c.content_revision,
        blob_key: c.content_blob_key.clone(),
        inline: c.content_inline.clone(),
        header_len: c.stored_header_len,
        timestamp: c.timestamp,
    }, new_content, Some(new_header_len), store.device_id(), blobs, Some(dek)).await?;
    if !replaced {
        return Err("atom content changed during header preparation".into());
    }
    Ok(())
}

#[cfg(test)]
pub(crate) async fn prepare_current_content_for_test(
    tenant: &TenantId,
    pool: &PgPool,
    account_id: Uuid,
    atom_id: Uuid,
    blobs: Option<&dyn PromotionBlobStore>,
) -> Result<String, String> {
    let selected = select_candidates(tenant, pool, account_id, None, true)
        .await.map_err(|e| e.to_string())?;
    let candidate = selected.candidates.into_iter().find(|c| c.atom_id == atom_id)
        .ok_or_else(|| "preparation fixture atom was not selected".to_string())?;
    let store = crate::store::PostgresStore::new(pool.clone());
    prepare_atom(tenant, pool, &store, blobs, &[7;32], account_id, candidate)
        .await.map(|p| p.text_to_embed).map_err(|(_,e)|e)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

/// Serializes every test in the crate (this module's own tests AND
/// `workers::huddle`'s `huddle_capture_idempotency_tests` — which, as of
/// Foundation PR 1.2 / CS-031, uses `MASTER_KEK_HEX` for BOTH its
/// `ingest_huddle_notes`/blob-encryption paths AND its `get_slack_bot_token`
/// oauth-token-decryption path; the two used to be separate KEK schemes, see
/// `crypto::derive_legacy_oauth_account_dek`'s doc comment for the retired
/// one) that mutates the process-global `MASTER_KEK_HEX` env var (CodeRabbit
/// review): `cargo
/// test`'s default in-binary thread parallelism means two such tests running
/// concurrently — even across files, since they link into the same `--lib`
/// test binary — could otherwise read back the WRONG value mid-run.
/// `pub(crate)` (and declared at file scope, not inside `mod tests`) so
/// `workers::huddle`'s tests can share this exact lock rather than a second,
/// unsynchronized one — same pattern as
/// `llm::model_config::EXTRACT_ESCALATE_ENV_LOCK` (shared with
/// `workers::extract`). Same defensive shape as `mcp_pg_e2e.rs`'s own
/// `ENV_LOCK` (a separate static there since that one lives in a different
/// compilation unit and isn't reachable from this crate's own `#[cfg(test)]`
/// modules).
#[cfg(test)]
pub(crate) static MASTER_KEK_HEX_ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[cfg(test)]
mod tests {
    use super::*;
    use coralswarm_core::embed::EmbedBatchFuture;

    // ── needs_reembed (pure, mirrors the SQL `IS DISTINCT FROM` predicate) ──────

    #[test]
    fn needs_reembed_true_for_old_chunker_version() {
        assert!(needs_reembed(Some(1), Some(EMBED_MODEL_NAME)));
    }

    #[test]
    fn needs_reembed_true_for_old_embed_model() {
        assert!(needs_reembed(Some(CHUNKER_VERSION), Some("some-retired-model")));
    }

    #[test]
    fn needs_reembed_true_for_null_columns() {
        // Defensive: an atom with embedding IS NOT NULL should always have
        // both columns stamped in practice (migration 0064's backfill), but
        // the predicate treats NULL as "not current" (NULL is distinct from
        // any concrete value under IS DISTINCT FROM), never as "skip".
        assert!(needs_reembed(None, Some(EMBED_MODEL_NAME)));
        assert!(needs_reembed(Some(CHUNKER_VERSION), None));
        assert!(needs_reembed(None, None));
    }

    #[test]
    fn needs_reembed_false_when_fully_current() {
        assert!(!needs_reembed(Some(CHUNKER_VERSION), Some(EMBED_MODEL_NAME)));
    }

    #[test]
    fn needs_cloud_vector_true_when_missing_or_stale() {
        assert!(needs_cloud_vector(None));
        assert!(needs_cloud_vector(Some("bge-small-en-v1.5")));
        assert!(!needs_cloud_vector(Some(coralswarm_core::embed::EMBED_MODEL_CLOUD)));
        assert!(!needs_cloud_vector(Some(crate::workers::embed::SKIPPED_LOCAL_ONLY)));
    }

    #[test]
    fn cloud_resweep_only_after_a_pass_that_reembedded() {
        assert!(should_enqueue_cloud_resweep(true, false, 3));
        assert!(
            !should_enqueue_cloud_resweep(true, false, 0),
            "empty window must not re-enqueue a delayed no-op"
        );
        assert!(!should_enqueue_cloud_resweep(true, true, 8));
        assert!(!should_enqueue_cloud_resweep(false, false, 4));
    }

    // ── plan_reembed (pure header-reconstruction decision logic) ────────────────

    fn ts(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    #[test]
    fn plan_reembed_injects_header_for_headerless_meeting_atom() {
        let plan = plan_reembed(
            "transcript",
            0,
            "Alice: good morning everyone.",
            Some("Weekly Standup"),
            ts("2026-07-15T14:00:00Z"),
            &["Alice".to_string(), "Bob".to_string()],
            None,
        );
        let expected_header = "Meeting: Weekly Standup, 2026-07-15. Participants: Alice, Bob.";
        assert_eq!(
            plan.text_to_embed,
            format!("{expected_header}\n\nAlice: good morning everyone.")
        );
        let (new_content, new_header_len) =
            plan.content_mutation.expect("headerless meeting atom must get a content mutation");
        assert_eq!(new_content, plan.text_to_embed);
        assert_eq!(new_header_len as usize, expected_header.len() + 2);
    }

    #[test]
    fn plan_reembed_degrades_to_date_only_header_without_title_or_participants() {
        // Guarantees the "every meeting-shaped pre-header atom gets AT LEAST
        // a date header" coverage claim: no title, no participants, but
        // `timestamp` (NOT NULL on `atoms`) is always available.
        let plan = plan_reembed("zoom", 0, "some transcript text", None, ts("2026-07-15T14:00:00Z"), &[], None);
        assert_eq!(plan.text_to_embed, "Meeting on 2026-07-15.\n\nsome transcript text");
        assert!(plan.content_mutation.is_some());
    }

    #[test]
    fn plan_reembed_leaves_already_headered_atom_untouched() {
        // header_len > 0 (e.g. a hypothetical future selection on
        // embed_model alone, with chunker_version already current) — must
        // NOT double-prepend a second header AND must NOT strip the
        // existing one either (regression coverage for a real bug caught in
        // review: an earlier version of this function was handed the
        // ALREADY-STRIPPED body by its caller, so this branch silently
        // re-embedded header-less text on every model-only re-embed).
        let existing = "Meeting: Standup, 2026-07-10.\n\nAlice: hi";
        let plan = plan_reembed(
            "transcript",
            30, // arbitrary positive header_len
            existing,
            Some("Different Title"),
            ts("2026-07-15T14:00:00Z"),
            &[],
            None,
        );
        assert_eq!(
            plan.text_to_embed, existing,
            "the EXISTING header must survive a model-only re-embed, not be stripped"
        );
        assert_eq!(plan.content_mutation, None);
        assert_eq!(
            plan.header_len_repair, None,
            "header_len already correctly describes a DIFFERENT header — no repair signal, no clobbering"
        );
    }

    #[test]
    fn plan_reembed_repairs_header_len_when_content_already_has_the_exact_header() {
        // Simulates the partial-failure recovery case (CodeRabbit review):
        // a PRIOR pass's content write landed (content already starts with
        // the EXACT header this pass would reconstruct) but its header_len
        // stamp never did (still 0). Must NOT double-prepend; must signal a
        // metadata-only repair instead.
        let header = "Meeting: Weekly Standup, 2026-07-15. Participants: Alice, Bob.";
        let body = "Alice: good morning everyone.";
        let already_headered = format!("{header}\n\n{body}");
        let plan = plan_reembed(
            "transcript",
            0, // stale — a prior pass's header_len UPDATE never landed
            &already_headered,
            Some("Weekly Standup"),
            ts("2026-07-15T14:00:00Z"),
            &["Alice".to_string(), "Bob".to_string()],
            None,
        );
        assert_eq!(
            plan.text_to_embed, already_headered,
            "must re-embed the content as-is, not double-prepend a second copy of the header"
        );
        assert_eq!(plan.content_mutation, None, "content is already correct — no content mutation needed");
        assert_eq!(
            plan.header_len_repair,
            Some((header.len() + 2) as i32),
            "header_len must be repaired to the correct value"
        );
    }

    #[test]
    fn plan_reembed_no_repair_signal_when_header_len_already_correct() {
        let header = "Meeting on 2026-07-15.";
        let body = "some transcript text";
        let already_headered = format!("{header}\n\n{body}");
        let correct_header_len = (header.len() + 2) as i32;
        let plan = plan_reembed(
            "zoom",
            correct_header_len,
            &already_headered,
            None,
            ts("2026-07-15T14:00:00Z"),
            &[],
            None,
        );
        assert_eq!(plan.text_to_embed, already_headered);
        assert_eq!(plan.content_mutation, None);
        assert_eq!(plan.header_len_repair, None, "header_len already matches — nothing to repair");
    }

    #[test]
    fn plan_reembed_never_injects_header_for_chat_sources() {
        for source_type in ["slack", "teams_chat"] {
            let plan = plan_reembed(
                source_type,
                0,
                "some message text",
                Some("Should Never Be Used"),
                ts("2026-07-15T14:00:00Z"),
                &["Alice".to_string()],
                Some("proj"),
            );
            assert_eq!(plan.text_to_embed, "some message text", "source_type={source_type}");
            assert_eq!(plan.content_mutation, None, "source_type={source_type}");
        }
    }

    #[test]
    fn plan_reembed_adds_provenance_for_email_notes_and_docs() {
        let plan = plan_reembed(
            "email",
            0,
            "see you thursday",
            Some("Q3 plan"),
            ts("2026-07-15T14:00:00Z"),
            &[],
            Some("reef"),
        );
        assert_eq!(
            plan.text_to_embed,
            "Title: Q3 plan. Date: 2026-07-15. Project: reef.\n\nsee you thursday"
        );
        let note = plan_reembed("note", 0, "body", None, ts("2026-07-15T14:00:00Z"), &[], None);
        assert_eq!(note.text_to_embed, "Date: 2026-07-15.\n\nbody");
    }

    // ── group_by_account (pure grouping, mirrors workers::embed_retry) ──────────

    fn candidate(atom_id: Uuid, account_id: Uuid) -> Candidate {
        Candidate {
            atom_id,
            graph_id: Uuid::new_v4(),
            account_id,
            source_type: "transcript".to_string(),
            source_id: format!("{atom_id}:chunk:0"),
            content_blob_key: "blob-key".to_string(),
            content_inline: Some("text".to_string()),
            content_revision: 1,
            stored_header_len: Some(0),
            participants: vec![],
            timestamp: Utc::now(),
            header_len: 0,
            fts_language: "english".to_string(),
            title: None,
            project: None,
            bge_stale: true,
            needs_cloud: false,
        }
    }

    #[test]
    fn grouping_splits_candidates_by_account() {
        let account_a = Uuid::new_v4();
        let account_b = Uuid::new_v4();
        let atom_a1 = Uuid::new_v4();
        let atom_a2 = Uuid::new_v4();
        let atom_b1 = Uuid::new_v4();

        let candidates = vec![
            candidate(atom_a1, account_a),
            candidate(atom_b1, account_b),
            candidate(atom_a2, account_a),
        ];

        let mut by_account = group_by_account(candidates);
        assert_eq!(by_account.len(), 2);

        let mut group_a: Vec<Uuid> = by_account.remove(&account_a).unwrap().into_iter().map(|c| c.atom_id).collect();
        group_a.sort();
        let mut expected_a = vec![atom_a1, atom_a2];
        expected_a.sort();
        assert_eq!(group_a, expected_a);

        let group_b: Vec<Uuid> = by_account.remove(&account_b).unwrap().into_iter().map(|c| c.atom_id).collect();
        assert_eq!(group_b, vec![atom_b1]);
    }

    #[test]
    fn grouping_empty_input_yields_no_groups() {
        assert!(group_by_account(vec![]).is_empty());
    }

    // ── JobKind payload round-trip ───────────────────────────────────────────────

    #[test]
    fn reembed_outdated_chunks_roundtrip() {
        let cursor = Some(Uuid::new_v4());
        let kind = JobKind::ReembedOutdatedChunks { after_atom_id: cursor };
        let tagged = serde_json::to_value(&kind).unwrap();

        let job = JobRow {
            id: Uuid::new_v4(),
            kind: tagged["kind"].as_str().unwrap().to_owned(),
            payload: tagged["payload"].clone(),
            attempts: 0,
            account_id: None,
        };

        assert_eq!(parse_after_atom_id(&job).unwrap(), cursor);
    }

    #[test]
    fn reembed_outdated_chunks_none_cursor_roundtrip() {
        let kind = JobKind::ReembedOutdatedChunks { after_atom_id: None };
        let tagged = serde_json::to_value(&kind).unwrap();
        let job = JobRow {
            id: Uuid::new_v4(),
            kind: tagged["kind"].as_str().unwrap().to_owned(),
            payload: tagged["payload"].clone(),
            attempts: 0,
            account_id: None,
        };
        assert_eq!(parse_after_atom_id(&job).unwrap(), None);
    }

    #[test]
    fn parse_after_atom_id_wrong_kind_errors() {
        let kind = JobKind::RouteCopresence { session_id: Uuid::new_v4() };
        let tagged = serde_json::to_value(&kind).unwrap();
        let job = JobRow {
            id: Uuid::new_v4(),
            kind: tagged["kind"].as_str().unwrap().to_owned(),
            payload: tagged["payload"].clone(),
            attempts: 0,
            account_id: None,
        };
        assert!(parse_after_atom_id(&job).is_err());
    }

    // ── Live-Postgres end-to-end sweep ───────────────────────────────────────────

    /// A deterministic, non-zero stub embedder (`Store::set_atom_embeddings`
    /// rejects all-zero vectors — see that method's doc comment — so unlike
    /// some other workers' `StubEmbedder`s in this crate, this one must NOT
    /// return zeros).
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
            Box::pin(async move { Ok(vec![vec![0.1f32; dim]; n]) })
        }
    }

    const TEST_EMBED_DIM: usize = 384;

    /// Live-Postgres end-to-end regression covering this module's central
    /// claims: candidate selection, account-scoped DEK resolution, header
    /// reconstruction (with AND without a resolvable `meeting_transcripts`
    /// title — both degrade correctly per this module's coverage argument),
    /// non-meeting atoms re-embedded verbatim, the content →
    /// `content_tsv` → oplog ordering, and idempotency (a second sweep pass
    /// finds nothing left to do).
    ///
    /// Gated on `RELINFER_TEST_DATABASE_URL`, same convention as every other
    /// live test in this crate. Connect is bounded by `tokio::time::timeout`
    /// (an unreachable URL must fail fast, not hang on the OS TCP timeout).
    #[tokio::test]
    async fn reembed_sweep_reconstructs_headers_and_converges_against_real_postgres() {
        use crate::store::{PostgresStore, Store};
        use coralswarm_core::kek::{generate_dek, load_kek, wrap_dek};
        use pgvector::Vector;

        let url = match std::env::var("RELINFER_TEST_DATABASE_URL") {
            Ok(u) => u,
            Err(_) => {
                eprintln!(
                    "skipping: set RELINFER_TEST_DATABASE_URL to run the live reembed backfill regression test"
                );
                return;
            }
        };
        const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
        let pool = tokio::time::timeout(
            CONNECT_TIMEOUT,
            sqlx::postgres::PgPoolOptions::new().max_connections(4).connect(&url),
        )
        .await
        .unwrap_or_else(|_| panic!("connect to test Postgres timed out after {CONNECT_TIMEOUT:?}"))
        .expect("connect to test Postgres");

        // ── KEK + account DEK provisioning (mirrors
        // `clerk_provision::provision_account_dek` exactly) ────────────────────
        //
        // Holds `MASTER_KEK_HEX_ENV_LOCK` for the rest of this test's body
        // (CodeRabbit review): `MASTER_KEK_HEX` is a process-global env var,
        // and `cargo test`'s default in-binary thread parallelism means a
        // concurrently-running test that also touched it (including
        // `workers::huddle`'s tests, which share this exact lock) could
        // otherwise observe (or clobber) the wrong value mid-run.
        let env_guard = MASTER_KEK_HEX_ENV_LOCK.lock().await;
        // Save whatever was there before (almost certainly nothing, in a
        // normal `cargo test` run) so cleanup restores the PRIOR state
        // exactly, rather than assuming "absent" and deleting a real value
        // some other harness/environment had set.
        let prior_master_kek_hex = std::env::var("MASTER_KEK_HEX").ok();
        let kek_hex = "22".repeat(32);
        // SAFETY: `env_guard` (held for this test's entire body) serializes
        // every `MASTER_KEK_HEX` mutation against any other test in this
        // module that might touch it; no other thread can observe a
        // torn/partial write.
        unsafe {
            std::env::set_var("MASTER_KEK_HEX", &kek_hex);
        }

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

        let kek = load_kek().expect("load test KEK");
        let wrapped = wrap_dek(&kek, &generate_dek()).expect("wrap test DEK");
        sqlx::query("UPDATE accounts SET dek_wrapped = $1 WHERE id = $2")
            .bind(&wrapped)
            .bind(account_id)
            .execute(&pool)
            .await
            .expect("provision account DEK");

        let store = PostgresStore::new(pool.clone());
        let graph = store
            .create_graph(account_id, "reembed-backfill-e2e", "personal")
            .await
            .expect("create graph");
        let graph_id = graph.id;

        let timestamp = Utc::now();
        let date_str = timestamp.format("%Y-%m-%d").to_string();
        let old_vector = Vector::from(vec![0.01f32; TEST_EMBED_DIM]);

        // ── Atom A: meeting-shaped, v1, WITH a resolvable meeting_transcripts
        // title — expect the FULL "Meeting: {title}, {date}. Participants:
        // {names}." header reconstructed and persisted. ────────────────────────
        let base_a = format!("reembed-e2e-a-{}", Uuid::new_v4());
        let body_a = "Alice: good morning everyone.";
        let participants_a = vec!["Alice".to_string(), "Bob".to_string()];
        let atom_a: Uuid = sqlx::query_scalar(
            "INSERT INTO atoms (graph_id, source_type, source_id, content_blob_key, content_inline, \
                                 participants, timestamp, fts_language, embedding, embed_model, chunker_version) \
             VALUES ($1, 'transcript', $2, 'blob-a', $3, $4, $5, 'english', $6, 'bge-small-en-v1.5', 1) \
             RETURNING atom_id",
        )
        .bind(graph_id)
        .bind(format!("{base_a}:chunk:0"))
        .bind(body_a)
        .bind(&participants_a)
        .bind(timestamp)
        .bind(&old_vector)
        .fetch_one(&pool)
        .await
        .expect("insert atom A");

        store
            .upsert_meeting_transcript(
                graph_id,
                &base_a,
                None,
                Some("Weekly Standup\n\nAlice: good morning everyone."),
                None,
                None,
            )
            .await
            .expect("upsert meeting_transcripts for atom A");

        // ── Atom B: meeting-shaped, v1, NO meeting_transcripts row and no
        // participants — expect the minimal "Meeting on {date}." header
        // (the guaranteed-coverage floor). ──────────────────────────────────────
        let base_b = format!("reembed-e2e-b-{}", Uuid::new_v4());
        let body_b = "some unattributed transcript text";
        let atom_b: Uuid = sqlx::query_scalar(
            "INSERT INTO atoms (graph_id, source_type, source_id, content_blob_key, content_inline, \
                                 participants, timestamp, fts_language, embedding, embed_model, chunker_version) \
             VALUES ($1, 'zoom', $2, 'blob-b', $3, '{}', $4, 'english', $5, 'bge-small-en-v1.5', 1) \
             RETURNING atom_id",
        )
        .bind(graph_id)
        .bind(format!("{base_b}:chunk:0"))
        .bind(body_b)
        .bind(timestamp)
        .bind(&old_vector)
        .fetch_one(&pool)
        .await
        .expect("insert atom B");

        // ── Atom C: non-meeting (Slack), v1 — must be re-embedded VERBATIM,
        // no header ever injected. ──────────────────────────────────────────────
        let body_c = "hey team, standup moved to 10am";
        let atom_c: Uuid = sqlx::query_scalar(
            "INSERT INTO atoms (graph_id, source_type, source_id, content_blob_key, content_inline, \
                                 participants, timestamp, fts_language, embedding, embed_model, chunker_version) \
             VALUES ($1, 'slack', $2, 'blob-c', $3, '{}', $4, 'english', $5, 'bge-small-en-v1.5', 1) \
             RETURNING atom_id",
        )
        .bind(graph_id)
        .bind(format!("reembed-e2e-c-{}", Uuid::new_v4()))
        .bind(body_c)
        .bind(timestamp)
        .bind(&old_vector)
        .fetch_one(&pool)
        .await
        .expect("insert atom C");

        // ── Atom D: ALREADY current (chunker_version=2, current embed_model)
        // — must be excluded from selection entirely (idempotency control). ────
        let atom_d: Uuid = sqlx::query_scalar(
            "INSERT INTO atoms (graph_id, source_type, source_id, content_blob_key, content_inline, \
                                 participants, timestamp, fts_language, embedding, embed_model, chunker_version, \
                                 embedding_next, embed_model_next) \
             VALUES ($1, 'transcript', $2, 'blob-d', 'Meeting on 2020-01-01.\n\nalready current', '{}', $3, \
                     'english', $4, $5, $6, $4, $7) \
             RETURNING atom_id",
        )
        .bind(graph_id)
        .bind(format!("reembed-e2e-d-{}", Uuid::new_v4()))
        .bind(timestamp)
        .bind(&old_vector)
        .bind(coralswarm_core::embed::EMBED_MODEL_NAME)
        .bind(CHUNKER_VERSION)
        .bind(coralswarm_core::embed::EMBED_MODEL_CLOUD)
        .fetch_one(&pool)
        .await
        .expect("insert atom D");
        let _ = atom_d; // inserted for its side effect (candidate exclusion); id unused below.

        // ── Atom E: ALREADY headered (header_len > 0), but v1/stale
        // embed_model — a "model-only" re-embed. This is the exact scenario
        // that exposed a real bug in an earlier version of `plan_reembed`
        // (CodeRabbit review): the caller handed it the ALREADY-STRIPPED
        // body, so the "already headered" branch silently re-embedded
        // header-less text and stamped it current. Exercises `prepare_atom`
        // itself end to end (not just `plan_reembed` in isolation), per
        // that same review's ask. ─────────────────────────────────────────────
        let existing_header_e = "Meeting: Retro, 2025-01-01. Participants: Carol.";
        let body_e = "Carol: what went well this sprint?";
        let content_e = format!("{existing_header_e}\n\n{body_e}");
        let atom_e: Uuid = sqlx::query_scalar(
            "INSERT INTO atoms (graph_id, source_type, source_id, content_blob_key, content_inline, \
                                 participants, timestamp, fts_language, embedding, embed_model, chunker_version, header_len) \
             VALUES ($1, 'transcript', $2, 'blob-e', $3, $4, $5, 'english', $6, 'bge-small-en-v1.5', 1, $7) \
             RETURNING atom_id",
        )
        .bind(graph_id)
        .bind(format!("reembed-e2e-e-{}", Uuid::new_v4()))
        .bind(&content_e)
        .bind(vec!["Carol".to_string()])
        .bind(timestamp)
        .bind(&old_vector)
        .bind((existing_header_e.len() + 2) as i32)
        .fetch_one(&pool)
        .await
        .expect("insert atom E");

        // ── Atom F: partial-failure recovery — content ALREADY carries the
        // EXACT header this sweep would reconstruct (as if a prior pass's
        // content write landed), but `header_len` never got stamped (still
        // 0/stale). Must be repaired, NOT double-prepended. ─────────────────────
        let title_f = "Design Review";
        let body_f = "Dave: let's walk through the mocks.";
        let participants_f = vec!["Dave".to_string()];
        let header_f = format!(
            "Meeting: {title_f}, {date_str}. Participants: {}.",
            participants_f.join(", ")
        );
        let content_f = format!("{header_f}\n\n{body_f}");
        let base_f = format!("reembed-e2e-f-{}", Uuid::new_v4());
        let source_id_f = format!("{base_f}:chunk:0");
        let atom_f: Uuid = sqlx::query_scalar(
            "INSERT INTO atoms (graph_id, source_type, source_id, content_blob_key, content_inline, \
                                 participants, timestamp, fts_language, embedding, embed_model, chunker_version) \
             VALUES ($1, 'transcript', $2, 'blob-f', $3, $4, $5, 'english', $6, 'bge-small-en-v1.5', 1) \
             RETURNING atom_id",
        )
        .bind(graph_id)
        .bind(&source_id_f)
        .bind(&content_f)
        .bind(&participants_f)
        .bind(timestamp)
        .bind(&old_vector)
        .fetch_one(&pool)
        .await
        .expect("insert atom F");
        // Keyed by the SAME base_source_id `resolve_meeting_title` derives
        // from atom F's `source_id` (via `workers::extract::base_source_id`)
        // — so title resolution finds exactly "Design Review", matching
        // `header_f`'s title clause and making the content-based
        // already-headered check in `plan_reembed` actually match.
        let raw_f = format!("{title_f}\n\n{body_f}");
        store
            .upsert_meeting_transcript(graph_id, &base_f, None, Some(&raw_f), None, None)
            .await
            .expect("upsert meeting_transcripts for atom F");

        // ── Run the sweep (dev-mode: no R2, content lives in content_inline). ──
        let embedder = StubEmbedder { dim: TEST_EMBED_DIM };
        let r2: Option<&R2Client> = None;
        let outcome = run_batch(
            &TenantId::Consumer,
            &pool,
            &store,
            &embedder,
            Some(&embedder),
            r2,
            account_id,
            None,
        )
            .await
            .expect("run_batch");
        assert_eq!(outcome.examined, 5, "must select A, B, C, E, F — not D (already current)");
        assert_eq!(outcome.reembedded, 5, "all five candidates must re-embed successfully");
        assert!(outcome.all_succeeded, "a fully successful pass must report all_succeeded");
        assert!(!outcome.batch_full, "5 candidates is well under ATOM_BATCH_SIZE");

        let cloud_a: Option<String> = sqlx::query_scalar(
            "SELECT embed_model_next FROM atoms WHERE atom_id = $1",
        )
        .bind(atom_a)
        .fetch_one(&pool)
        .await
        .expect("fetch atom A cloud stamp");
        assert_eq!(cloud_a.as_deref(), Some(coralswarm_core::embed::EMBED_MODEL_CLOUD));

        // ── Assert atom A: full title+date+participants header ─────────────────
        let row_a: (Option<i16>, Option<String>, i32, Option<String>) = sqlx::query_as(
            "SELECT chunker_version, embed_model, COALESCE(header_len, 0), content_inline FROM atoms WHERE atom_id = $1",
        )
        .bind(atom_a)
        .fetch_one(&pool)
        .await
        .expect("fetch atom A after sweep");
        assert_eq!(row_a.0, Some(1), "live bge chunker stamp is not rewritten");
        assert_eq!(row_a.1.as_deref(), Some(coralswarm_core::embed::EMBED_MODEL_NAME));
        let expected_header_a = format!("Meeting: Weekly Standup, {date_str}. Participants: Alice, Bob.");
        assert_eq!(row_a.2 as usize, expected_header_a.len() + 2);
        assert_eq!(
            row_a.3.as_deref(),
            Some(format!("{expected_header_a}\n\n{body_a}").as_str())
        );

        // ── Assert atom B: date-only header (no title, no participants) ────────
        let row_b: (Option<i16>, i32, Option<String>) = sqlx::query_as(
            "SELECT chunker_version, COALESCE(header_len, 0), content_inline FROM atoms WHERE atom_id = $1",
        )
        .bind(atom_b)
        .fetch_one(&pool)
        .await
        .expect("fetch atom B after sweep");
        assert_eq!(row_b.0, Some(1), "live bge chunker stamp is not rewritten");
        let expected_header_b = format!("Meeting on {date_str}.");
        assert_eq!(row_b.1 as usize, expected_header_b.len() + 2);
        assert_eq!(
            row_b.2.as_deref(),
            Some(format!("{expected_header_b}\n\n{body_b}").as_str())
        );

        // ── Assert atom C: verbatim re-embed, no header ever injected ──────────
        let row_c: (Option<i16>, i32, Option<String>) = sqlx::query_as(
            "SELECT chunker_version, COALESCE(header_len, 0), content_inline FROM atoms WHERE atom_id = $1",
        )
        .bind(atom_c)
        .fetch_one(&pool)
        .await
        .expect("fetch atom C after sweep");
        assert_eq!(row_c.0, Some(1), "live bge chunker stamp is not rewritten");
        assert_eq!(row_c.1, 0, "non-meeting atom must never get a header");
        assert_eq!(row_c.2.as_deref(), Some(body_c), "non-meeting atom's content must be unchanged");

        // ── Assert atom E: model-only re-embed must PRESERVE its existing
        // header verbatim (the bug this test specifically guards against). ──────
        let row_e: (Option<i16>, Option<String>, i32, Option<String>) = sqlx::query_as(
            "SELECT chunker_version, embed_model, COALESCE(header_len, 0), content_inline FROM atoms WHERE atom_id = $1",
        )
        .bind(atom_e)
        .fetch_one(&pool)
        .await
        .expect("fetch atom E after sweep");
        assert_eq!(row_e.0, Some(1), "live bge chunker stamp is not rewritten");
        assert_eq!(row_e.1.as_deref(), Some(coralswarm_core::embed::EMBED_MODEL_NAME));
        assert_eq!(row_e.2 as usize, existing_header_e.len() + 2, "header_len must be unchanged");
        assert_eq!(
            row_e.3.as_deref(),
            Some(content_e.as_str()),
            "atom E's EXISTING header must survive the re-embed unchanged — not stripped, not duplicated"
        );

        // ── Assert atom F: partial-failure recovery repairs header_len
        // WITHOUT double-prepending the header. ─────────────────────────────────
        let row_f: (Option<i16>, i32, Option<String>) = sqlx::query_as(
            "SELECT chunker_version, COALESCE(header_len, 0), content_inline FROM atoms WHERE atom_id = $1",
        )
        .bind(atom_f)
        .fetch_one(&pool)
        .await
        .expect("fetch atom F after sweep");
        assert_eq!(row_f.0, Some(1), "live bge chunker stamp is not rewritten");
        assert_eq!(
            row_f.1 as usize,
            header_f.len() + 2,
            "header_len must be repaired to the correct value, not left at the stale 0"
        );
        assert_eq!(
            row_f.2.as_deref(),
            Some(content_f.as_str()),
            "content must be UNCHANGED — the header was already present, must not be duplicated"
        );

        // ── content_tsv actually reflects the injected header (BM25 recall on
        // header terms — the retrieval-quality payoff this backfill exists
        // for) ──────────────────────────────────────────────────────────────────
        let hits = store
            .keyword_search(graph_id, "Standup", 10, &crate::retrieval::RetrievalFilter::default())
            .await
            .expect("keyword_search for header term");
        assert!(
            hits.iter().any(|(id, _)| *id == atom_a),
            "content_tsv for atom A must be rebuilt from the header-injected text so \"Standup\" \
             (only present in the reconstructed header, not the body) is now findable via BM25"
        );

        // ── Idempotency: a second sweep pass finds nothing left to do ──────────
        let outcome_2 = run_batch(
            &TenantId::Consumer,
            &pool,
            &store,
            &embedder,
            Some(&embedder),
            r2,
            account_id,
            None,
        )
            .await
            .expect("run_batch (second pass)");
        assert_eq!(outcome_2.examined, 0, "every candidate already converged; nothing left to select");
        assert_eq!(outcome_2.reembedded, 0);

        // ── Cleanup ──────────────────────────────────────────────────────────────
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
        // Restore whatever `MASTER_KEK_HEX` held before this test started —
        // set it back if something was already there, remove it only if it
        // was genuinely absent (CodeRabbit review: unconditionally removing
        // it would clobber a real prior value in an environment that had
        // one set for some other reason).
        // SAFETY: still under `env_guard` — no other thread can observe a
        // torn/partial write while this restores the prior state.
        unsafe {
            match &prior_master_kek_hex {
                Some(prior) => std::env::set_var("MASTER_KEK_HEX", prior),
                None => std::env::remove_var("MASTER_KEK_HEX"),
            }
        }
        drop(env_guard);

        println!(
            "reembed backfill e2e passed: full/date-only/no header reconstruction, version \
             uniformity, content_tsv rebuild, and idempotent re-run all verified against real Postgres"
        );
    }
}

#[cfg(test)]
pub(crate) async fn managed_job_for_test(
    tenant: &TenantId,
    pool: &PgPool,
    store: &dyn Store,
    cloud: &(dyn EmbedProvider + Send + Sync),
    control: &PgPool,
    job: JobRow,
) {
    process_managed_job(
        tenant,
        pool,
        store,
        cloud,
        Some(cloud),
        None,
        Some(control),
        job,
    )
    .await;
}
