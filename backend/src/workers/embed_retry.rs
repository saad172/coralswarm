//! Periodic re-enqueue sweep for atoms whose embedding previously failed.
//!
//! `workers::embed`'s candidate query (`load_pending_atoms`) now admits a
//! failed atom again once its backoff window has elapsed and it's still
//! under `MAX_EMBED_ATTEMPTS` — but that query only ever runs against the
//! `atom_ids` of an `EmbedAtoms` job actually being processed. Two of the
//! three ways an atom ends up with `embed_failed_at` set never produce
//! another such job on their own:
//!
//!   1. **Partial failure.** If an `EmbedAtoms` job's batch has ANY
//!      successes, `workers::embed::process_job` marks the whole job
//!      `done` (`success_count > 0`) even though some atoms in it failed and
//!      got `embed_failed_at` set. The job is gone — nothing ever revisits
//!      those specific atom_ids again without a fresh job naming them.
//!   2. **Job dead-lettered.** If every atom in a job fails,
//!      `jobs::queue::fail` retries the JOB itself with its own backoff —
//!      but once the job's own `max_attempts` is exhausted it goes `dead`
//!      and is never claimed again, even if the atoms' own
//!      `embed_attempts`/backoff would still allow more tries.
//!
//! This worker closes both gaps: on an interval, it finds atoms that are
//! backoff-eligible per the same rule `load_pending_atoms` uses and
//! re-enqueues a fresh `EmbedAtomsRetry` job naming them.
//!
//! ## Orphans: no vector, no stamp, no failure, no live job
//!
//! A third population never reaches either arm above: atoms with
//! `embedding_next`, `embed_model_next` AND `embed_failed_at` all NULL. Both
//! arms key on a failure timestamp or a stamp, so these are invisible, and
//! nothing else ever names them again. On the coralswarm tenant (2026-10-19)
//! they came from three producers, none of which marks the atoms:
//!
//!   * a managed `EmbedAtoms` job that the queue reaper dead-letters ("worker
//!     died mid-job 3 times"): `jobs::queue::reap` touches only the job row;
//!   * a managed job finished `done` as `managed_embedding_held` or
//!     `managed_embedding_discarded_stale`, where the fence check failed
//!     before a `blocked:<reason>` stamp was written
//!     (`hold_inputs_locked`'s `DiscardedStale` branch);
//!   * a writer that published the atom and never enqueued an embed job.
//!
//! The orphan arm claims such an atom once it is past [`ORPHAN_GRACE_SECS`]
//! old and no `pending`/`running` embed job names it, so the normal backlog
//! (which can lag for hours) is never double-enqueued. It is floored at
//! [`ORPHAN_FLOOR`], which leaves alone the pre-gate set the founder deferred.
//! The claim then gives the atom an ordinary failure timestamp, so from then
//! on it is a normal retryable atom.
//!
//! Retry jobs are capped at [`RETRY_JOB_MAX_ATOMS`] atoms. The managed path
//! embeds a job serially in batches of 8, and two 500-atom retry jobs were
//! reaped dead at the 20-minute lease on 2026-10-18.
//!
//! ## Concurrency: avoiding overlapping sweeps double-claiming an atom
//!
//! An earlier version of this worker just `SELECT`ed eligible atoms and
//! reasoned that a duplicate/overlapping sweep enqueuing the same atom twice
//! was harmless. That's wrong under horizontal scaling (more than one backend
//! process running this loop) or a sweep whose enqueued job outlives
//! `SWEEP_INTERVAL`: two sweeps can both see the same backoff-elapsed atom,
//! both enqueue a job naming it, and both jobs get concurrently claimed and
//! processed by `workers::embed` — wasted work at best, and at worst a race
//! where one job's success is stomped by the other job's
//! `bulk_mark_atoms_failed` call (see that function's doc comment for the
//! other half of this fix).
//!
//! [`find_retry_eligible_atoms`] closes this by claiming atomically instead
//! of just reading: it's a `SELECT ... FOR UPDATE SKIP LOCKED` CTE feeding an
//! `UPDATE` that bumps `embed_failed_at` to `now()` for exactly the rows it's
//! about to return. `FOR UPDATE SKIP LOCKED` means a second, overlapping
//! sweep's identical query never sees a row the first sweep is mid-claiming;
//! and because the bump resets the row's own backoff-eligibility clock, once
//! the first sweep's transaction commits, the row is no longer eligible again
//! until ITS OWN new backoff window (measured from the claim, not the
//! original failure) elapses — so a second sweep that starts even a moment
//! later, after the first committed, still won't re-select it. `embed_attempts`
//! is deliberately left untouched by the claim (only a genuine failure via
//! `bulk_mark_atoms_failed` advances it) — this is a re-queue, not a failure.
//! If the resulting job is itself lost (crash before `enqueue`, or the job
//! never gets claimed) the atom simply becomes eligible again after its own
//! backoff window, same self-healing shape as everything else in this queue
//! (see `jobs::queue`'s module doc on the reaper).
//!
//! ## The claim's `embed_failed_at` bump would otherwise break retryability
//!
//! A prior version of the claim above bumped `embed_failed_at` and then
//! enqueued an ordinary `JobKind::EmbedAtoms` job naming the claimed
//! atom_ids. That's broken: `workers::embed::load_pending_atoms` (the query
//! `process_job` uses to decide what to actually embed) re-applies the SAME
//! backoff-eligibility check to whatever atom_ids a job names — and the claim
//! had JUST set `embed_failed_at = now()` on those exact rows, so they look
//! freshly-failed and fail that check immediately. The enqueued job would
//! find nothing to do, complete empty, and the atom would never actually be
//! retried — the next sweep re-claims it and repeats the cycle forever
//! (CodeRabbit review, PR #194 round 2).
//!
//! The fix: enqueue [`JobKind::EmbedAtomsRetry`] instead of `EmbedAtoms` — a
//! distinct job kind carrying the exact atom_ids this sweep already vetted
//! and claimed, whose `load_pending_atoms` call bypasses the backoff check
//! entirely for them (see that variant's doc comment). This keeps the claim's
//! `embed_failed_at` bump doing its job (stopping an overlapping sweep from
//! re-claiming the same atom) without that bump also blocking the very job
//! the claim exists to produce.
//!
//! ## One job per account, not one flat job for the whole claim batch
//!
//! `workers::embed::process_job` resolves a SINGLE DEK for a job's entire
//! `atom_ids` list (`resolve_dek_for_atoms`) and refuses to proceed if those
//! atom_ids span more than one `graphs.account_id` — "atom_ids span N
//! accounts — refusing to use ambiguous DEK". [`find_retry_eligible_atoms`]
//! claims across the WHOLE `atoms` table with no account scoping (by design —
//! a global sweep), so a single claim batch can easily contain atoms from
//! several accounts once there's more than one tenant with stale failures.
//! Enqueuing that flat claimed list as one `EmbedAtomsRetry` job would fail
//! the DEK check wholesale: every atom in the batch — including the ones
//! belonging to accounts that would have succeeded on their own — churns
//! through a doomed job and get marked failed again, then reclaimed and
//! reproduced the same broken job forever (CodeRabbit review, PR #194 round
//! 3).
//!
//! [`find_retry_eligible_atoms`] therefore returns each claimed atom paired
//! with its `account_id` (one extra joined column, same claiming statement —
//! no additional round-trip), and [`group_by_account`] partitions the claim
//! into one `Vec<Uuid>` per account. [`tick`] enqueues one `EmbedAtomsRetry`
//! job per account group instead of one job for the whole batch, so an
//! account with a healthy DEK still gets retried even when another account
//! in the same sweep's claim batch doesn't (e.g. mid key-rotation).

use std::collections::HashMap;
use std::time::Duration;

use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::jobs::{queue, JobKind};
use crate::shutdown::Shutdown;
use crate::tenancy::scope::{begin_account_scoped, begin_tenant_system_scoped_without_account};
use crate::tenancy::TenantId;
use crate::workers::tenant_harness::{Tick, WorkerTenant};

use super::embed::{EMBED_BACKOFF_BASE_SECS, MAX_EMBED_ATTEMPTS};

/// How often the sweep runs. The shortest possible backoff window is
/// `EMBED_BACKOFF_BASE_SECS` (attempt 0 → next retry in ~30s); running the
/// sweep every 10 minutes means a freshly-eligible atom waits at most one
/// sweep interval past its computed backoff before actually being retried —
/// ample for a background repair pass that (unlike the primary embed worker)
/// isn't on any user-facing latency path. `pub(crate)`: fed to the tenant
/// harness as this worker's sweep cadence (see `workers::tenant_harness`).
pub(crate) const SWEEP_INTERVAL: Duration = Duration::from_secs(10 * 60);

/// Cap on how many retry-eligible atoms a single sweep claims TOTAL, across
/// all accounts, mirroring `jobs::queue::reap`'s `limit` reasoning: bounds
/// one sweep's DB read/write, so a large backlog of stale failures drains
/// across successive sweeps instead of one arbitrarily large claim. Since
/// [`group_by_account`] splits the claim into one `EmbedAtomsRetry` job per
/// account, no single job's batch size is bounded by this constant alone —
/// each is at most this many atoms, but in practice far fewer once split
/// across however many accounts the claim happened to span.
const RETRY_BATCH_LIMIT: i64 = 500;

/// Most atoms one `EmbedAtomsRetry` job names. A claim batch for one account
/// is split into jobs of at most this many. On 2026-10-18 the 257-500 atom
/// retry jobs that finished took up to 361 s on the managed path, and two
/// 500-atom jobs were reaped dead at the 20-minute lease. Jobs of 65-128
/// atoms finished in about 190 s.
const RETRY_JOB_MAX_ATOMS: usize = 64;

/// The orphan arm only claims atoms created at or after this instant: the
/// managed embedding gate (2026-10-12). Older vectorless atoms are the
/// pre-gate set the founder deferred. Move this earlier, or drop it, to
/// backfill them.
const ORPHAN_FLOOR: &str = "2026-10-12T00:00:00Z";

/// The orphan arm skips atoms younger than this. A writer that publishes an
/// atom and enqueues its embed job in separate statements is never raced.
const ORPHAN_GRACE_SECS: i64 = 60 * 60;

/// Granularity at which the inter-sweep sleep polls [`Shutdown`], mirroring
/// `workers::reaper`'s `SHUTDOWN_POLL_INTERVAL` exactly (same "periodic
/// sweeper with a multi-minute interval" shape — see that constant's doc
/// comment for why a plain `is_triggered()` check at the top of the loop
/// alone isn't enough here).
const SHUTDOWN_POLL_INTERVAL: Duration = Duration::from_millis(500);

/// Bound on each of the sweep's two DB round-trips (the atomic claim and the
/// `enqueue` call) so a stalled Postgres can't block this worker indefinitely
/// — mirrors `workers::reaper::REAP_TIMEOUT`'s reasoning exactly (same
/// "periodic sweeper, one bounded DB call per iteration" shape). Applied to
/// each call separately, not the pair combined, so a slow claim doesn't eat
/// into the budget the enqueue needs (CodeRabbit review, PR #194 round 2).
const SWEEP_DB_TIMEOUT: Duration = Duration::from_secs(30);

/// One embed-retry sweep on one tenant's pool (the store is unused): claim
/// backoff-eligible failed atoms and re-enqueue them as `EmbedAtomsRetry`
/// jobs, per account. Driven by `workers::tenant_harness::run_multi_tenant`
/// (org-tenancy PR 2.1) as a sweep worker with cadence [`SWEEP_INTERVAL`];
/// the harness owns the loop and the (shutdown-aware) inter-sweep sleep. No
/// control-table reads: `atoms`/`graphs`/`jobs` are all tenant-tree tables.
///
/// The per-account enqueue loop is STILL raced against `shutdown` per
/// iteration (the harness only checks shutdown between ticks) — a sweep that
/// claimed atoms across many accounts would otherwise keep enqueueing (each
/// call separately capped at [`SWEEP_DB_TIMEOUT`]) through every remaining
/// account after shutdown fires, materially delaying process exit (CodeRabbit
/// review, PR #194 round 4). This worker is **not** given a grace window in
/// `lib.rs::run()`'s drain step: a sweep is one bounded DB round-trip (the
/// atomic claim) plus up to one `enqueue` call per claimed account, none of
/// it a multi-step job with an LLM call or R2 fetch in it, so there's nothing
/// worth draining — stopping promptly is enough, same reasoning as the reaper.
pub async fn tick(t: &WorkerTenant, shutdown: &Shutdown) -> Result<Tick, String> {
    let pool = &t.pool;
    let mut processed = 0;
    {
        // ── Claim (org-tenancy Phase W.2) ─────────────────────────────────────
        // The claim query touches `atoms`/`graphs` — RLS-covered consumer
        // tables (migration 0083) — so on the CONSUMER plane the old single
        // global claim would hard-error once OPS-4 switches the app role to
        // the non-BYPASSRLS `coralswarm_app` (the strict
        // `current_setting('app.account_id')` predicate fails without a
        // scope). The sweep therefore claims PER ACCOUNT there: enumerate
        // `accounts` (a control-plane identity table, deliberately outside
        // 0083's RLS roster) and run one `begin_account_scoped` claim per
        // account, each with an explicit `g.account_id = $x` predicate so
        // behaviour is identical pre-cutover (where the GUC is inert against
        // the BYPASSRLS owner role) and post-cutover. Same loop-per-account
        // shape as Phase B.4's admin backfills.
        //
        // Org tenant pools keep the SINGLE whole-table claim, but it is not
        // unscoped. Whole-database isolation is real and covers the CROSS-ORG
        // boundary only; inside a tenant DB `atoms` and `graphs` also carry
        // member-ocean policies (`migrations_tenant/0009_tenant_rls`),
        // resolved through `tenant_rls_visible_graph_ids()`. With neither
        // `app.user_id` nor `app.tenant_actor` set,
        // `tenant_rls_actor_user_id()` is NULL, so after OPS-4-T the claim
        // would match only the org and personal oceans: atoms stuck in every
        // member and team ocean would never be re-embedded, and the sweep
        // would log a clean "nothing eligible". `claim_org_scoped` therefore
        // takes the system principal for the claim query.
        //
        // Scale note: this puts one cheap claim probe per consumer account on
        // every sweep (10-minute cadence). Fine at current scale; the planned
        // enqueue-side "which accounts plausibly have work" signal (see
        // `workers::tenant_harness`'s Neon section for the analogous
        // discussion) is the right fix if the account roster grows enough for
        // this to matter.
        let claimed: Vec<ClaimedAtom> = match &t.tenant {
            TenantId::Consumer => claim_consumer_per_account(pool, shutdown).await,
            TenantId::Org(_) => {
                match tokio::time::timeout(
                    SWEEP_DB_TIMEOUT,
                    claim_org_scoped(pool, RETRY_BATCH_LIMIT),
                )
                .await
                {
                    Err(_) => {
                        tracing::warn!(
                            timeout_secs = SWEEP_DB_TIMEOUT.as_secs(),
                            "embed retry sweep: claim query timed out; will retry next cycle"
                        );
                        Vec::new()
                    }
                    Ok(Err(e)) => {
                        tracing::warn!(error = %e, "embed retry sweep: query for eligible atoms failed");
                        Vec::new()
                    }
                    Ok(Ok(claimed)) => claimed,
                }
            }
        };

        match claimed {
            claimed if claimed.is_empty() => {
                tracing::debug!("embed retry sweep: no backoff-eligible failed atoms");
            }
            claimed => {
                processed = claimed.len();
                let total = claimed.len();
                let by_account = group_by_account(claimed);
                let account_count = by_account.len();
                tracing::debug!(
                    total,
                    account_count,
                    "embed retry sweep: claimed atoms grouped by account"
                );

                // One EmbedAtomsRetry job per account (see this module's doc
                // comment): each account's DEK resolution is independent, so
                // one account's failure (e.g. mid key-rotation) must not
                // block another account's retry in the same sweep.
                //
                // Both the top-of-iteration check AND the enqueue call itself
                // are raced against `shutdown` (CodeRabbit review, PR #194
                // round 4): a sweep spanning many accounts would otherwise
                // keep enqueueing — each call separately capped at
                // `SWEEP_DB_TIMEOUT` — straight through every remaining
                // account after shutdown fires, materially delaying process
                // exit. Any account not reached before shutdown simply gets
                // re-claimed by a future sweep, same self-healing shape as
                // every other interrupted step in this worker.
                for (account_id, atom_ids) in split_into_jobs(by_account) {
                    if shutdown.is_triggered() {
                        tracing::info!(
                            "embed retry sweep worker: shutdown signalled mid per-account enqueue \
                             loop, stopping (remaining claimed accounts will be re-claimed by a \
                             future sweep)"
                        );
                        return Ok(Tick::worked(processed));
                    }

                    let count = atom_ids.len();
                    match enqueue_racing_shutdown(
                        pool,
                        &JobKind::EmbedAtomsRetry { atom_ids },
                        account_id,
                        shutdown,
                    )
                    .await
                    {
                        EnqueueRace::Done(Ok(_)) => tracing::info!(
                            %account_id,
                            count,
                            "embed retry sweep: re-enqueued failed atoms for account"
                        ),
                        EnqueueRace::Done(Err(e)) => tracing::warn!(
                            error = %e,
                            %account_id,
                            count,
                            "embed retry sweep: failed to enqueue retry job for account"
                        ),
                        EnqueueRace::TimedOut => tracing::warn!(
                            %account_id,
                            count,
                            timeout_secs = SWEEP_DB_TIMEOUT.as_secs(),
                            "embed retry sweep: enqueue timed out for account; claimed atoms will be \
                             re-claimed by a future sweep once their (already-bumped) backoff window \
                             elapses again"
                        ),
                        EnqueueRace::ShutdownWon => {
                            tracing::info!(
                                %account_id,
                                count,
                                "embed retry sweep worker: shutdown signalled during enqueue for \
                                 account, stopping (this and any remaining claimed accounts will be \
                                 re-claimed by a future sweep)"
                            );
                            return Ok(Tick::worked(processed));
                        }
                    }
                }
            }
        }
    }
    // The inter-sweep sleep (and its shutdown race) now lives in the tenant
    // harness: this worker is configured as a sweep with cadence
    // [`SWEEP_INTERVAL`], and the harness caps every scheduler sleep at its
    // own shutdown-poll interval — the same contract the old in-loop
    // `tokio::select!` race provided.
    Ok(Tick::worked(processed))
}

/// Resolve once `shutdown` is triggered, polling every
/// [`SHUTDOWN_POLL_INTERVAL`]. Never returns otherwise — used only as the
/// "shutdown" arm of [`enqueue_racing_shutdown`]'s `tokio::select!`. Mirrors
/// `workers::reaper`'s old shutdown poll.
async fn poll_until_triggered(shutdown: &Shutdown) {
    loop {
        if shutdown.is_triggered() {
            return;
        }
        tokio::time::sleep(SHUTDOWN_POLL_INTERVAL).await;
    }
}

/// Outcome of [`enqueue_racing_shutdown`] — lets [`tick`]'s per-account
/// enqueue loop tell "enqueue finished (ok or error)" apart from "timed out"
/// apart from "shutdown fired mid-enqueue", each of which the loop handles
/// differently (the last two are both non-fatal — the atoms involved simply
/// get re-claimed by a future sweep — but only `ShutdownWon` should stop the
/// loop from moving on to the next account).
enum EnqueueRace {
    /// The `enqueue` call completed (successfully or not) before either the
    /// timeout or shutdown fired.
    Done(Result<Option<Uuid>, sqlx::Error>),
    /// [`SWEEP_DB_TIMEOUT`] elapsed before the `enqueue` call completed.
    TimedOut,
    /// `shutdown` was signalled before the `enqueue` call completed (or
    /// timed out) — see this module's doc comment on why the per-account
    /// loop races EVERY enqueue against shutdown, not just the top of each
    /// iteration.
    ShutdownWon,
}

/// Race one `enqueue` call — itself bounded by [`SWEEP_DB_TIMEOUT`] — against
/// `shutdown`, so a shutdown signalled mid-enqueue is noticed within
/// [`SHUTDOWN_POLL_INTERVAL`] instead of riding out the full timeout (or the
/// enqueue's actual, possibly-fast completion) first. Used by [`tick`]'s
/// per-account enqueue loop; see [`EnqueueRace`] for how the caller
/// distinguishes the three outcomes.
async fn enqueue_racing_shutdown(
    pool: &PgPool,
    kind: &JobKind,
    account_id: Uuid,
    shutdown: &Shutdown,
) -> EnqueueRace {
    tokio::select! {
        result = tokio::time::timeout(SWEEP_DB_TIMEOUT, queue::enqueue(pool, kind, Some(account_id), None, 0)) => {
            match result {
                Ok(r) => EnqueueRace::Done(r),
                Err(_) => EnqueueRace::TimedOut,
            }
        }
        _ = poll_until_triggered(shutdown) => EnqueueRace::ShutdownWon,
    }
}

/// One atom this sweep just claimed, paired with the account that owns it —
/// [`group_by_account`] needs the pairing to split a claim batch into
/// per-account `EmbedAtomsRetry` jobs (see this module's doc comment for
/// why a single flat job across accounts is wrong).
struct ClaimedAtom {
    atom_id: Uuid,
    account_id: Uuid,
}

/// Atomically find AND claim up to `limit` atom ids that failed to embed, are
/// still under the attempts cap, and whose backoff window has elapsed — the
/// exact same eligibility rule `workers::embed::load_pending_atoms` applies
/// (see that query's comment); this is the "find them in the first place"
/// half, since that query only ever sees atom_ids it's handed. Each claimed
/// atom is paired with its owning `graphs.account_id` — the same column
/// `workers::embed::resolve_dek_for_atoms` keys its single-DEK-per-job
/// invariant on — so the caller can split the claim into per-account jobs.
///
/// "Claim" means: the eligible rows are locked (`FOR UPDATE OF a SKIP
/// LOCKED` — only the `atoms` row is locked, not the joined `graphs` row,
/// since only `atoms` is being claimed) and, in the same statement, have
/// `embed_failed_at` bumped to `now()` — this is what makes overlapping
/// sweeps (see this module's doc comment) not double-claim the same atom:
/// `SKIP LOCKED` means a concurrent sweep's identical query never sees a row
/// this one is mid-claiming, and the `embed_failed_at` bump means that once
/// this sweep's claim commits, the row isn't backoff-eligible again until its
/// OWN new window elapses — so a sweep that starts moments later, after this
/// one committed, still won't re-select it. `embed_attempts` is intentionally
/// untouched: this is a re-queue, not a failure, so it must not count against
/// [`MAX_EMBED_ATTEMPTS`].
/// `account_id` (org-tenancy Phase W.2): `Some` restricts the claim to one
/// account's atoms with an explicit `g.account_id = $4` predicate — used by
/// the consumer plane's per-account scoped claim loop, where the explicit
/// predicate keeps the claim correct even pre-OPS-4 (the RLS scope is inert
/// against the BYPASSRLS owner role until the role cutover). `None` keeps the
/// original whole-table claim — org tenant pools only.
async fn find_retry_eligible_atoms<'e, E>(
    exec: E,
    limit: i64,
    account_id: Option<Uuid>,
) -> Result<Vec<ClaimedAtom>, sqlx::Error>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    // Claimable: an ordinary failure whose backoff has elapsed, OR the legacy
    // fence backlog ($7, no vector, never failed — the fence cleared
    // embed_failed_at), OR an orphan (no vector, no stamp, no failure, at
    // least $9 seconds old, created at or after $8, not retracted, and named
    // by no pending/running embed job; see the module doc). The claim clears
    // $7 and stamps embed_failed_at, so a claimed backlog or orphan atom is an
    // ordinary retryable atom from then on.
    //
    // `live` is compared as text so a malformed historical payload cannot
    // fail the cast and stall the whole sweep.
    let floor: chrono::DateTime<chrono::Utc> =
        ORPHAN_FLOOR.parse().map_err(|e| sqlx::Error::Protocol(format!("ORPHAN_FLOOR: {e}")))?;
    let rows = sqlx::query(
        r#"
        WITH live AS (
            SELECT DISTINCT x.atom_id
            FROM   jobs j, jsonb_array_elements_text(j.payload->'atom_ids') AS x(atom_id)
            WHERE  j.kind IN ('EmbedAtoms','EmbedAtomsRetry')
              AND  j.status IN ('pending','running')
              AND  jsonb_typeof(j.payload->'atom_ids')='array'
              AND  ($4::uuid IS NULL OR j.account_id=$4)
        ), eligible AS (
            SELECT a.atom_id,row_number() OVER (PARTITION BY a.graph_id ORDER BY a.embed_failed_at,a.atom_id) AS graph_rank
            FROM atoms a JOIN graphs g ON g.id=a.graph_id
            WHERE (a.embedding_next IS NULL OR a.embed_model_next IS DISTINCT FROM $5)
              AND COALESCE(a.embed_model_next,'')<>$6
              AND (
                    (a.embed_failed_at IS NOT NULL AND a.embed_attempts<$1
                     AND a.embed_failed_at<=now()-(power(2,LEAST(a.embed_attempts,30))*($2*interval '1 second')))
                 OR (a.embed_model_next=$7 AND a.embedding_next IS NULL AND a.embed_failed_at IS NULL)
                 OR (a.embedding_next IS NULL AND a.embed_model_next IS NULL AND a.embed_failed_at IS NULL
                     AND NOT a.evidence_retracted
                     AND a.created_at>=$8 AND a.created_at<now()-($9*interval '1 second')
                     AND NOT EXISTS (SELECT 1 FROM live l WHERE l.atom_id=a.atom_id::text))
              )
              AND ($4::uuid IS NULL OR g.account_id=$4)
        ), claimed AS (
            SELECT a.atom_id, g.account_id
            FROM   atoms a
            JOIN   graphs g ON g.id = a.graph_id
            JOIN eligible e ON e.atom_id=a.atom_id AND e.graph_rank<=256
            WHERE  (a.embedding_next IS NULL OR a.embed_model_next IS DISTINCT FROM $5)
              AND  COALESCE(a.embed_model_next, '') <> $6
              AND  (
                     (    a.embed_failed_at IS NOT NULL
                      AND a.embed_attempts  < $1
                      AND a.embed_failed_at <= now() - (
                            power(2, LEAST(a.embed_attempts, 30)) * ($2 * INTERVAL '1 second')
                          ))
                  OR (    a.embed_model_next = $7
                      AND a.embedding_next IS NULL
                      AND a.embed_failed_at IS NULL)
                  OR (    a.embedding_next IS NULL
                      AND a.embed_model_next IS NULL
                      AND a.embed_failed_at IS NULL
                      AND NOT a.evidence_retracted
                      AND a.created_at >= $8
                      AND a.created_at < now() - ($9 * INTERVAL '1 second')
                      AND NOT EXISTS (SELECT 1 FROM live l WHERE l.atom_id = a.atom_id::text))
                   )
              AND  ($4::uuid IS NULL OR g.account_id = $4)
            ORDER BY a.embed_failed_at
            LIMIT  $3
            FOR UPDATE OF a SKIP LOCKED
        )
        UPDATE atoms a
        SET    embed_failed_at = now(),
               embed_model_next = CASE WHEN a.embed_model_next = $7
                                       THEN NULL ELSE a.embed_model_next END
        FROM   claimed c
        WHERE  a.atom_id = c.atom_id
        RETURNING a.atom_id, c.account_id
        "#,
    )
    .bind(MAX_EMBED_ATTEMPTS)
    .bind(EMBED_BACKOFF_BASE_SECS)
    .bind(limit)
    .bind(account_id)
    .bind(coralswarm_core::embed::EMBED_MODEL_CLOUD)
    .bind("skipped:local-only")
    .bind(super::embed::BLOCKED_MANAGED_PURPOSE)
    .bind(floor)
    .bind(ORPHAN_GRACE_SECS)
    .fetch_all(exec)
    .await?;

    Ok(rows
        .into_iter()
        .map(|r| ClaimedAtom { atom_id: r.get("atom_id"), account_id: r.get("account_id") })
        .collect())
}

/// Org-tenant-plane claim (tenant RLS Phase 2, Category C): the sweep-wide
/// [`find_retry_eligible_atoms`] claim, inside one short
/// [`begin_tenant_system_scoped_without_account`] transaction.
///
/// **Why a transaction and not a bare `find_retry_eligible_atoms(pool, ..)`.**
/// The claim reads `atoms` JOIN `graphs`, and both carry a
/// `member_ocean_isolation` policy (`migrations_tenant/0009_tenant_rls`)
/// resolving through `tenant_rls_visible_graph_ids()`, which returns the EMPTY
/// SET unless the transaction has set `app.tenant_actor` or `app.user_id`.
/// Both are `set_config(.., true)` — transaction-local — so a statement run
/// straight against a pooled `&PgPool` cannot carry either, whatever pool it
/// came from. The worker pool does not supply them at the connection level:
/// `TenantRegistry::get_or_build_worker_pool` used to append
/// `-c app.tenant_actor=system` to the connection STARTUP packet, and PR #388
/// removed that because a connection pooler rejects unknown startup
/// parameters. Until this arm was converted, its only cover was the owner
/// exemption (`0009` §7 does not FORCE row security), asserted at pool build
/// by `worker_pool_rls_exemption_breach`.
///
/// A retry sweep is genuinely tenant-wide — it claims every backoff-eligible
/// atom in the org, across every member's ocean — so there is no honest
/// acting user to thread, and the reviewed service-principal bypass is the
/// right primitive. Same shape as `workers::mcp_task_sweep::org_tick`.
/// Committed, not rolled back:
/// the claim is an `UPDATE ... RETURNING`.
///
/// The `FOR UPDATE OF a SKIP LOCKED` row locks now live for the transaction
/// rather than for the single autocommit statement, which is the same
/// exposure [`claim_consumer_per_account`] already accepts per account — the
/// transaction wraps one statement and commits immediately, with no LLM or
/// R2 call inside it.
async fn claim_org_scoped(pool: &PgPool, limit: i64) -> Result<Vec<ClaimedAtom>, sqlx::Error> {
    let mut tx = begin_tenant_system_scoped_without_account(pool)
        .await
        .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
    let claimed = find_retry_eligible_atoms(&mut *tx, limit, None).await?;
    tx.commit().await?;
    Ok(claimed)
}

/// Consumer-plane claim (org-tenancy Phase W.2): one `begin_account_scoped`
/// transaction + one account-filtered [`find_retry_eligible_atoms`] per
/// account, sharing the sweep-wide [`RETRY_BATCH_LIMIT`] budget. Per-account
/// failures (timeout, scope/claim error) are isolated — logged, that account
/// skipped, the loop continues — and the loop stops early on shutdown or an
/// exhausted budget; anything unclaimed stays backoff-eligible for the next
/// sweep, the module's usual self-healing shape.
///
/// A budget exhausted by an early account starves later accounts within ONE
/// sweep only: the bump-on-claim semantics push claimed rows out of
/// eligibility, so successive sweeps drain the backlog across accounts.
async fn claim_consumer_per_account(pool: &PgPool, shutdown: &Shutdown) -> Vec<ClaimedAtom> {
    // `accounts` is a control-plane identity table, deliberately outside
    // 0083's RLS roster — this roster read needs (and can have) no scope.
    let accounts: Vec<Uuid> = match tokio::time::timeout(
        SWEEP_DB_TIMEOUT,
        sqlx::query_scalar("SELECT id FROM accounts ORDER BY id").fetch_all(pool),
    )
    .await
    {
        Err(_) => {
            tracing::warn!(
                timeout_secs = SWEEP_DB_TIMEOUT.as_secs(),
                "embed retry sweep: account roster query timed out; will retry next cycle"
            );
            return Vec::new();
        }
        Ok(Err(e)) => {
            tracing::warn!(error = %e, "embed retry sweep: account roster query failed");
            return Vec::new();
        }
        Ok(Ok(a)) => a,
    };

    let mut out: Vec<ClaimedAtom> = Vec::new();
    let mut remaining = RETRY_BATCH_LIMIT;
    for account_id in accounts {
        if remaining <= 0 {
            break;
        }
        if shutdown.is_triggered() {
            tracing::info!(
                "embed retry sweep: shutdown signalled mid per-account claim loop, stopping \
                 (remaining accounts' atoms stay eligible for a future sweep)"
            );
            break;
        }
        let claim = tokio::time::timeout(SWEEP_DB_TIMEOUT, async {
            let mut tx = begin_account_scoped(pool, account_id)
                .await
                .map_err(|e| e.to_string())?;
            let claimed = find_retry_eligible_atoms(&mut *tx, remaining, Some(account_id))
                .await
                .map_err(|e| e.to_string())?;
            tx.commit().await.map_err(|e| e.to_string())?;
            Ok::<_, String>(claimed)
        })
        .await;
        match claim {
            Err(_) => tracing::warn!(
                %account_id,
                timeout_secs = SWEEP_DB_TIMEOUT.as_secs(),
                "embed retry sweep: per-account claim timed out; skipping this account"
            ),
            Ok(Err(e)) => tracing::warn!(
                %account_id,
                error = %e,
                "embed retry sweep: per-account claim failed; skipping this account"
            ),
            Ok(Ok(claimed)) => {
                remaining -= claimed.len() as i64;
                out.extend(claimed);
            }
        }
    }
    out
}

/// Partition a sweep's claimed atoms into one `Vec<Uuid>` per account —
/// pure, DB-free grouping logic so it's independently unit-testable (see
/// [`grouping_splits_atoms_by_account`]). `HashMap` iteration order is
/// unspecified, which is fine: [`run`] enqueues one job per group regardless
/// of order, and nothing downstream depends on group ordering.
fn group_by_account(claimed: Vec<ClaimedAtom>) -> HashMap<Uuid, Vec<Uuid>> {
    let mut by_account: HashMap<Uuid, Vec<Uuid>> = HashMap::new();
    for c in claimed {
        by_account.entry(c.account_id).or_default().push(c.atom_id);
    }
    by_account
}

/// Split each account's group into jobs of at most [`RETRY_JOB_MAX_ATOMS`]
/// atoms. Every job still names exactly one account's atoms.
fn split_into_jobs(by_account: HashMap<Uuid, Vec<Uuid>>) -> Vec<(Uuid, Vec<Uuid>)> {
    by_account
        .into_iter()
        .flat_map(|(account_id, atom_ids)| {
            atom_ids
                .chunks(RETRY_JOB_MAX_ATOMS)
                .map(|chunk| (account_id, chunk.to_vec()))
                .collect::<Vec<_>>()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pure, DB-free coverage for [`group_by_account`] (CodeRabbit review, PR
    /// #194 round 3): three claimed atoms across two accounts must split
    /// into exactly two groups, each containing only that account's atoms —
    /// this is the structural fix for `workers::embed::process_job` refusing
    /// a job whose atom_ids span more than one account's DEK.
    #[test]
    fn grouping_splits_atoms_by_account() {
        let account_a = Uuid::new_v4();
        let account_b = Uuid::new_v4();
        let atom_a1 = Uuid::new_v4();
        let atom_a2 = Uuid::new_v4();
        let atom_b1 = Uuid::new_v4();

        let claimed = vec![
            ClaimedAtom { atom_id: atom_a1, account_id: account_a },
            ClaimedAtom { atom_id: atom_b1, account_id: account_b },
            ClaimedAtom { atom_id: atom_a2, account_id: account_a },
        ];

        let mut by_account = group_by_account(claimed);
        assert_eq!(by_account.len(), 2, "two distinct accounts must yield two groups");

        let mut group_a = by_account.remove(&account_a).expect("account_a group present");
        group_a.sort();
        let mut expected_a = vec![atom_a1, atom_a2];
        expected_a.sort();
        assert_eq!(group_a, expected_a, "account_a's group must contain exactly its two atoms");

        let group_b = by_account.remove(&account_b).expect("account_b group present");
        assert_eq!(group_b, vec![atom_b1], "account_b's group must contain exactly its one atom");

        assert!(by_account.is_empty(), "no groups besides account_a/account_b");
    }

    #[test]
    fn grouping_empty_claim_yields_no_groups() {
        assert!(group_by_account(vec![]).is_empty());
    }

    #[test]
    fn jobs_are_capped_and_never_mix_accounts() {
        let (a, b) = (Uuid::new_v4(), Uuid::new_v4());
        let atoms_a: Vec<Uuid> = (0..RETRY_JOB_MAX_ATOMS * 2 + 1).map(|_| Uuid::new_v4()).collect();
        let atoms_b = vec![Uuid::new_v4()];
        let jobs = split_into_jobs(HashMap::from([(a, atoms_a.clone()), (b, atoms_b.clone())]));
        assert_eq!(jobs.len(), 4, "129 atoms split into 64+64+1, plus one job for the other account");
        assert!(jobs.iter().all(|(_, ids)| !ids.is_empty() && ids.len() <= RETRY_JOB_MAX_ATOMS));
        let mut got_a: Vec<Uuid> = jobs.iter().filter(|(acc, _)| *acc == a).flat_map(|(_, ids)| ids.clone()).collect();
        let mut want_a = atoms_a;
        got_a.sort();
        want_a.sort();
        assert_eq!(got_a, want_a, "every claimed atom lands in exactly one job of its own account");
        assert_eq!(jobs.iter().filter(|(acc, _)| *acc == b).map(|(_, ids)| ids.clone()).collect::<Vec<_>>(), vec![atoms_b]);
    }

    #[test]
    fn orphan_floor_parses() {
        let floor: chrono::DateTime<chrono::Utc> = ORPHAN_FLOOR.parse().unwrap();
        assert_eq!(floor.to_rfc3339(), "2026-10-12T00:00:00+00:00");
    }

    /// Live-Postgres regression: an atom whose backoff window has elapsed is
    /// found and re-enqueues into a fresh, claimable `EmbedAtomsRetry` job;
    /// one still inside its backoff window, or already at the attempts cap,
    /// is not. Also exercises the module's actual `run()`-level flow, the
    /// fix for CodeRabbit's round-2 finding, AND the round-3 account-scoping
    /// fix, end to end, seeding atoms under TWO accounts:
    ///
    ///   1. Claim eligible atoms across both accounts in one sweep.
    ///   2. [`group_by_account`] the claim and assert it produced exactly
    ///      two groups, one per account, each naming only that account's
    ///      eligible atom — the fix for CodeRabbit round 3 (a single flat
    ///      job spanning both accounts would make
    ///      `workers::embed::resolve_dek_for_atoms` refuse the WHOLE batch).
    ///   3. Enqueue one `EmbedAtomsRetry` job per group exactly like [`tick`]
    ///      does, dequeue each back, and assert each claimed job really is
    ///      `EmbedAtomsRetry` naming only that account's atom (not the
    ///      other account's).
    ///   4. **The assertion that would have caught the round-2 bug:** for
    ///      each job, parse it via the real `workers::embed::parse_atom_ids`
    ///      and feed the result straight into the real
    ///      `workers::embed::load_pending_atoms` — the exact function
    ///      `process_job` calls — and assert the claimed atom comes back.
    ///      Before the round-2 fix (enqueuing plain `EmbedAtoms` with no
    ///      bypass), this step would return an EMPTY result.
    ///
    /// Gated on `RELINFER_TEST_DATABASE_URL` (all migrations applied through
    /// at least 0065), same convention as `jobs::queue`'s live tests. Connect
    /// is bounded by `tokio::time::timeout` (CodeRabbit review, PR #194 round
    /// 2 — an unreachable `RELINFER_TEST_DATABASE_URL` would otherwise hang
    /// CI on the OS TCP timeout instead of failing fast), same pattern as
    /// `workers::embed`'s live test.
    #[tokio::test]
    async fn sweep_finds_backoff_eligible_atoms_and_ignores_ineligible_ones() {
        let url = match std::env::var("RELINFER_TEST_DATABASE_URL") {
            Ok(u) => u,
            Err(_) => {
                eprintln!(
                    "skipping: set RELINFER_TEST_DATABASE_URL to run the live embed-retry sweep test"
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

        // Minimal fixtures: TWO independent user -> account -> graph chains
        // (cheap to seed — mirrors the single-account shape other live tests
        // in this crate use, e.g. `workers::infer_expertise`'s live test,
        // just duplicated once so the sweep's claim batch spans two
        // accounts, exercising the round-3 account-scoping fix).
        let (user_a, account_a, graph_a) = seed_tenant(&pool, "embed retry test A").await;
        let (user_b, account_b, graph_b) = seed_tenant(&pool, "embed retry test B").await;

        // Eligible: 1 prior attempt, failed long enough ago that 2^1*30s=60s
        // has clearly elapsed. One per account.
        let eligible_a = insert_test_atom(&pool, graph_a, 1, 300).await;
        let eligible_b = insert_test_atom(&pool, graph_b, 1, 300).await;
        // Ineligible: failed 5 seconds ago — 2^0*30s=30s backoff not yet elapsed.
        let too_recent = insert_test_atom(&pool, graph_a, 0, 5).await;
        // Ineligible: at the attempts cap even though old enough.
        let exhausted = insert_test_atom(&pool, graph_a, MAX_EMBED_ATTEMPTS, 3600).await;

        let claimed = find_retry_eligible_atoms(&pool, 100, None)
            .await
            .expect("query eligible atoms");
        let found_ids: Vec<Uuid> = claimed.iter().map(|c| c.atom_id).collect();

        assert!(found_ids.contains(&eligible_a), "backoff-elapsed atom (account A) must be eligible");
        assert!(found_ids.contains(&eligible_b), "backoff-elapsed atom (account B) must be eligible");
        assert!(!found_ids.contains(&too_recent), "atom still inside its backoff window must not be eligible");
        assert!(!found_ids.contains(&exhausted), "atom at the attempts cap must not be eligible");

        // ── Round-3 fix: grouping must split the two-account claim into two
        // single-account groups, not one flat cross-account list.
        let by_account = group_by_account(claimed);
        assert_eq!(
            by_account.get(&account_a).cloned().unwrap_or_default(),
            vec![eligible_a],
            "account A's group must contain exactly its own eligible atom"
        );
        assert_eq!(
            by_account.get(&account_b).cloned().unwrap_or_default(),
            vec![eligible_b],
            "account B's group must contain exactly its own eligible atom, not account A's"
        );

        // ── Enqueue one EmbedAtomsRetry job per account group exactly like
        // `run()` does, then dequeue + verify each independently.
        let mut enqueued_ids = Vec::new();
        for (account_id, atom_ids) in &by_account {
            let enqueued_id =
                queue::enqueue(&pool, &JobKind::EmbedAtomsRetry { atom_ids: atom_ids.clone() }, Some(*account_id), None, 0)
                    .await
                    .expect("enqueue retry job")
                    .expect("enqueue must return a job id (no competing pending job holds this key)");
            enqueued_ids.push((*account_id, enqueued_id, atom_ids.clone()));
        }

        let dequeued = queue::dequeue(&pool, &["EmbedAtoms", "EmbedAtomsRetry"], 10)
            .await
            .expect("dequeue claimable jobs");

        for (account_id, enqueued_id, expected_atom_ids) in &enqueued_ids {
            let claimed_job = dequeued
                .iter()
                .find(|j| j.id == *enqueued_id)
                .expect("the job this test just enqueued must be claimable via queue::dequeue");
            assert_eq!(claimed_job.kind, "EmbedAtomsRetry", "claimed job must be an EmbedAtomsRetry job");

            // ── The assertion that would have caught the round-2 bug: parse
            // the claimed job through the REAL
            // `workers::embed::parse_atom_ids`, then feed the result
            // straight into the REAL `workers::embed::load_pending_atoms` —
            // exactly what `workers::embed::process_job` does with a
            // dequeued job. Before the round-2 fix (plain `EmbedAtoms`, no
            // bypass), this step would return an EMPTY result. Before the
            // round-3 fix, this job's atom_ids would instead be a flat mix
            // of BOTH accounts' atoms.
            let (parsed_ids, bypass_backoff) = crate::workers::embed::parse_atom_ids(claimed_job)
                .expect("claimed job payload must parse via the real parse_atom_ids");
            assert_eq!(
                &parsed_ids, expected_atom_ids,
                "parsed atom_ids for account {account_id} must exactly match what the sweep claimed for it, \
                 and only it"
            );
            assert!(bypass_backoff, "an EmbedAtomsRetry job must set bypass_backoff = true");

            let loadable = crate::workers::embed::load_pending_atoms(&pool, &parsed_ids, bypass_backoff)
                .await
                .expect("load_pending_atoms query");
            for expected_id in expected_atom_ids {
                assert!(
                    loadable.iter().any(|row| row.atom_id == *expected_id),
                    "atom {expected_id} claimed for account {account_id} MUST be loadable by the job \
                     enqueued for it — if this is empty, the claim's embed_failed_at bump is blocking \
                     its own retry job again"
                );
            }

            // Job is now 'running' after dequeue — complete it so it doesn't
            // linger as an orphaned claim past this test.
            queue::complete(&pool, *enqueued_id, queue::JobOutcome::DidWork)
                .await
                .expect("complete test job");
        }

        // Cleanup.
        for (_, enqueued_id, _) in &enqueued_ids {
            sqlx::query("DELETE FROM jobs WHERE id = $1").bind(enqueued_id).execute(&pool).await.ok();
        }
        for (user_id, account_id, graph_id) in [(user_a, account_a, graph_a), (user_b, account_b, graph_b)] {
            sqlx::query("DELETE FROM atoms WHERE graph_id = $1")
                .bind(graph_id)
                .execute(&pool)
                .await
                .ok();
            sqlx::query("DELETE FROM graphs WHERE id = $1").bind(graph_id).execute(&pool).await.ok();
            sqlx::query("DELETE FROM accounts WHERE id = $1").bind(account_id).execute(&pool).await.ok();
            sqlx::query("DELETE FROM users WHERE id = $1").bind(user_id).execute(&pool).await.ok();
        }
        eprintln!(
            "PG embed-retry sweep e2e passed: backoff eligibility matches load_pending_atoms's rule, \
             claimed atoms across two accounts split into one EmbedAtomsRetry job PER account, and \
             each job's own load_pending_atoms call actually finds its claimed atom (end-to-end \
             retryability + account scoping)"
        );
    }

    /// Seed a minimal user -> account -> graph chain, returning their ids.
    /// Factored out so the live test can cheaply seed TWO independent
    /// tenants (round-3 account-scoping coverage) without duplicating the
    /// insert boilerplate.
    async fn seed_tenant(pool: &PgPool, graph_name: &str) -> (Uuid, Uuid, Uuid) {
        let email = format!("{}@test.local", Uuid::new_v4());
        let user_id: Uuid = sqlx::query_scalar("INSERT INTO users (email) VALUES ($1) RETURNING id")
            .bind(&email)
            .fetch_one(pool)
            .await
            .expect("insert user");
        let account_id: Uuid =
            sqlx::query_scalar("INSERT INTO accounts (owner_user_id) VALUES ($1) RETURNING id")
                .bind(user_id)
                .fetch_one(pool)
                .await
                .expect("insert account");
        let graph_id: Uuid = sqlx::query_scalar(
            "INSERT INTO graphs (account_id, name) VALUES ($1, $2) RETURNING id",
        )
        .bind(account_id)
        .bind(graph_name)
        .fetch_one(pool)
        .await
        .expect("insert graph");
        (user_id, account_id, graph_id)
    }

    async fn insert_test_atom(
        pool: &PgPool,
        graph_id: Uuid,
        embed_attempts: i32,
        failed_secs_ago: i64,
    ) -> Uuid {
        sqlx::query_scalar(
            r#"
            INSERT INTO atoms
                (graph_id, source_type, source_id, content_blob_key, timestamp,
                 content_inline, embed_attempts, embed_failed_at)
            VALUES ($1, 'generic', $2, '', now(), 'test content', $3,
                    now() - ($4 * INTERVAL '1 second'))
            RETURNING atom_id
            "#,
        )
        .bind(graph_id)
        .bind(format!("embed-retry-test-{}", Uuid::new_v4()))
        .bind(embed_attempts)
        .bind(failed_secs_ago)
        .fetch_one(pool)
        .await
        .expect("insert test atom")
    }
}

/// Orphan arm on a fresh tenant database, as the table owner (how prod tenant
/// DBs connect). Requires `ORGT_TEST_BASE_URL` (a local PostgreSQL); with
/// `REQUIRE_PG_SUITE=1` a missing database fails instead of skipping.
#[cfg(test)]
mod orphan_pg_tests {
    use super::*;
    use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
    use std::str::FromStr;

    async fn fixture(tenant: bool) -> Option<(PgPool, PgPool, crate::test_db::TestDatabase, Uuid, Uuid)> {
        let Ok(base) = std::env::var("ORGT_TEST_BASE_URL") else {
            assert!(std::env::var_os("REQUIRE_PG_SUITE").is_none(), "ORGT_TEST_BASE_URL required");
            eprintln!("embed_retry orphan_pg_tests skipped: set ORGT_TEST_BASE_URL");
            return None;
        };
        let opts = PgConnectOptions::from_str(&base).unwrap();
        assert!(matches!(opts.get_host(), "localhost" | "127.0.0.1" | "::1"));
        let admin = PgPoolOptions::new().max_connections(2).connect_with(opts.clone()).await.unwrap();
        let db = crate::test_db::TestDatabase::new(&opts, "embed_retry_orphan");
        sqlx::query(&format!("CREATE DATABASE {db}")).execute(&admin).await.unwrap();
        let owner = PgPoolOptions::new().max_connections(4).connect_with(opts.database(&db)).await.unwrap();
        let account = Uuid::new_v4();
        if tenant {
            sqlx::migrate!("./migrations_tenant").run(&owner).await.unwrap();
        } else {
            sqlx::migrate!("./migrations_control").run(&owner).await.unwrap();
            let user = Uuid::new_v4();
            sqlx::query("INSERT INTO users(id,email) VALUES($1,$2)")
                .bind(user)
                .bind(format!("{user}@orphan.test"))
                .execute(&owner)
                .await
                .unwrap();
            sqlx::query("INSERT INTO accounts(id,owner_user_id) VALUES($1,$2)")
                .bind(account)
                .bind(user)
                .execute(&owner)
                .await
                .unwrap();
        }
        let graph: Uuid = sqlx::query_scalar("INSERT INTO graphs(account_id,name) VALUES($1,'orphans') RETURNING id")
            .bind(account)
            .fetch_one(&owner)
            .await
            .unwrap();
        Some((admin, owner, db, account, graph))
    }

    async fn atom(pool: &PgPool, graph: Uuid, created_at: &str) -> Uuid {
        sqlx::query_scalar(
            "INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,timestamp,content_inline,created_at)
             VALUES($1,'note',$2,'',now(),'orphan content',$3::timestamptz) RETURNING atom_id",
        )
        .bind(graph)
        .bind(format!("orphan-{}", Uuid::new_v4()))
        .bind(created_at)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    async fn job_naming(pool: &PgPool, account: Uuid, atom_id: Uuid, status: &str) {
        let id = queue::enqueue(pool, &JobKind::EmbedAtoms { atom_ids: vec![atom_id] }, Some(account), None, 0)
            .await
            .unwrap()
            .expect("job id");
        sqlx::query("UPDATE jobs SET status=$2::job_status, started_at=CASE WHEN $2<>'pending' THEN now() END WHERE id=$1")
            .bind(id)
            .bind(status)
            .execute(pool)
            .await
            .unwrap();
    }

    /// The claim each plane's sweep makes: the org plane's whole-tenant
    /// system-scoped claim, or the consumer plane's per-account claim.
    async fn claim(pool: &PgPool, tenant: bool, account: Uuid) -> Vec<ClaimedAtom> {
        if tenant {
            return claim_org_scoped(pool, RETRY_BATCH_LIMIT).await.unwrap();
        }
        let mut tx = begin_account_scoped(pool, account).await.unwrap();
        let claimed = find_retry_eligible_atoms(&mut *tx, RETRY_BATCH_LIMIT, Some(account)).await.unwrap();
        tx.commit().await.unwrap();
        claimed
    }

    #[tokio::test]
    async fn tenant_orphans_without_a_live_job_are_claimed_once() {
        orphans_without_a_live_job_are_claimed_once(true).await;
    }

    #[tokio::test]
    async fn consumer_orphans_without_a_live_job_are_claimed_once() {
        orphans_without_a_live_job_are_claimed_once(false).await;
    }

    async fn orphans_without_a_live_job_are_claimed_once(tenant: bool) {
        let Some((admin, owner, db, account, graph)) = fixture(tenant).await else { return };
        let old = "2026-10-15T00:00:00Z";
        let orphan = atom(&owner, graph, old).await;
        let dead_job = atom(&owner, graph, old).await;
        job_naming(&owner, account, dead_job, "dead").await;
        let done_job = atom(&owner, graph, old).await;
        job_naming(&owner, account, done_job, "done").await;
        let pending_job = atom(&owner, graph, old).await;
        job_naming(&owner, account, pending_job, "pending").await;
        let running_job = atom(&owner, graph, old).await;
        job_naming(&owner, account, running_job, "running").await;
        let young = atom(&owner, graph, &chrono::Utc::now().to_rfc3339()).await;
        let pre_floor = atom(&owner, graph, "2026-10-01T00:00:00Z").await;
        let retracted = atom(&owner, graph, old).await;
        sqlx::query("UPDATE atoms SET evidence_retracted=true WHERE atom_id=$1").bind(retracted).execute(&owner).await.unwrap();
        let stamped = atom(&owner, graph, old).await;
        sqlx::query("UPDATE atoms SET embed_model_next='skipped:local-only' WHERE atom_id=$1").bind(stamped).execute(&owner).await.unwrap();
        let embedded = atom(&owner, graph, old).await;
        sqlx::query("UPDATE atoms SET embedding_next=array_fill(0.1::real, ARRAY[384])::vector, embed_model_next=$2 WHERE atom_id=$1")
            .bind(embedded)
            .bind(coralswarm_core::embed::EMBED_MODEL_CLOUD)
            .execute(&owner)
            .await
            .unwrap();
        // A malformed historical payload must not fail the sweep.
        sqlx::query("INSERT INTO jobs(kind,payload,status,account_id) VALUES('EmbedAtoms','{\"atom_ids\":\"not-an-array\"}','pending',$1)")
            .bind(account)
            .execute(&owner)
            .await
            .unwrap();

        let claimed = claim(&owner, tenant, account).await;
        let mut got: Vec<Uuid> = claimed.iter().map(|c| c.atom_id).collect();
        got.sort();
        let mut want = vec![orphan, dead_job, done_job];
        want.sort();
        assert_eq!(got, want, "only orphans past the grace window, after the floor, with no live job are claimed");
        for (atom_id, why) in [
            (pending_job, "named by a pending embed job"),
            (running_job, "named by a running embed job"),
            (young, "inside the grace window"),
            (pre_floor, "created before the floor"),
            (retracted, "evidence retracted"),
            (stamped, "stamped local-only"),
            (embedded, "already has a current cloud vector"),
        ] {
            assert!(!got.contains(&atom_id), "an atom {why} must not be claimed");
        }
        assert!(claimed.iter().all(|c| c.account_id == account));

        let state: Vec<(Uuid, bool, i32, Option<String>)> = sqlx::query_as(
            "SELECT atom_id, embed_failed_at IS NOT NULL, embed_attempts, embed_model_next FROM atoms WHERE atom_id=ANY($1) ORDER BY atom_id",
        )
        .bind(&want)
        .fetch_all(&owner)
        .await
        .unwrap();
        assert!(
            state.iter().all(|(_, failed, attempts, model)| *failed && *attempts == 0 && model.is_none()),
            "a claimed orphan becomes an ordinary retryable atom without spending an attempt: {state:?}"
        );

        let again = claim(&owner, tenant, account).await;
        assert!(again.is_empty(), "the claim's failure timestamp keeps an overlapping sweep from re-claiming");

        // The retry job the sweep enqueues for them can actually load them.
        let ids: Vec<Uuid> = claimed.iter().map(|c| c.atom_id).collect();
        let loadable = crate::workers::embed::load_pending_atoms(&owner, &ids, true).await.unwrap();
        assert_eq!(loadable.len(), 3, "an EmbedAtomsRetry job bypasses the backoff the claim just set");

        owner.close().await;
        sqlx::query(&format!("DROP DATABASE {db} WITH (FORCE)")).execute(&admin).await.unwrap();
    }
}
