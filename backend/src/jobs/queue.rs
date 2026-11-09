//! Lightweight Postgres-backed job queue.
//!
//! Uses `SELECT … FOR UPDATE SKIP LOCKED` so multiple worker processes can
//! dequeue jobs concurrently without stepping on each other.  No external
//! broker required — Postgres is the only moving part.
//!
//! ## Delivery guarantees
//! At-least-once: a worker that crashes after claiming a job leaves it in
//! `running` state. [`reap`] (scheduled by `workers::reaper`) is the
//! previously-deferred "reaper" story: it periodically resets stale
//! `running` rows back to `pending` (or to the terminal `dead` state once
//! `max_attempts` would be exceeded), mirroring [`fail`]'s bookkeeping.
//! Callers should still use idempotent handlers — this closes the "orphaned
//! forever" failure mode, not at-least-once delivery itself.
//!
//! This gap was not theoretical: in production it silently orphaned 5
//! `EmbedAtoms` jobs over 3 days (Fly deploys / HTTP-idle autostop killing
//! machines mid-job, with no drain). Because `ExtractEntities` /
//! `ExtractRelationships` are only enqueued *after* embedding completes
//! (`workers::embed::process_job`), every orphaned embed batch silently
//! produced zero downstream entities/decisions/reefs — with no error logged
//! anywhere to alert on.

use chrono::{DateTime, Utc};
use sqlx::PgPool;
use uuid::Uuid;

use super::JobKind;

// ── Row type returned by `dequeue` ────────────────────────────────────────────

/// A claimed job row, ready for a worker to process.
#[derive(Debug)]
pub struct JobRow {
    pub id: Uuid,
    pub kind: String,
    pub payload: serde_json::Value,
    pub attempts: i32,
    /// The account this job's processing must be scoped to (org-tenancy
    /// Phase W.1): stamped at enqueue time by every call site (which already
    /// knows it), so post-OPS-4 a worker can open its per-job
    /// `tenancy::scope::begin_account_scoped` transaction without a
    /// bootstrap round-trip. `None` only for
    ///   * the deliberately account-less unscoped sweeps
    ///     (`SlackNameBackfill` with `graph_id: None`,
    ///     `ContentTsvRegconfigRepair`, `ReembedOutdatedChunks`) — those
    ///     span every account by design; and
    ///   * pre-W.1 legacy rows whose kind carries no resolvable
    ///     graph/atom reference ([`dequeue`] backfills every resolvable
    ///     legacy row via the 0084 SECURITY DEFINER resolvers, and
    ///     fail-paths the unresolvable ones — see
    ///     [`MISSING_ACCOUNT_ID_ERR`]).
    pub account_id: Option<Uuid>,
}

/// Greppable prefix of the error a [`dequeue`]d pre-W.1 job (NULL
/// `jobs.account_id`) is failed with when its kind references a graph/atom
/// that the 0084 SECURITY DEFINER resolvers cannot map to an owning account
/// (deleted graph, vanished atoms, or an atom batch spanning multiple
/// accounts). Such a job must never be handed to a worker unscoped
/// post-OPS-4, and must not retry forever: [`fail`]'s bounded
/// attempts/backoff converges it to `dead`.
pub const MISSING_ACCOUNT_ID_ERR: &str = "job missing account_id (pre-W.1 row)";

/// `jobs.max_attempts` for every kind that sets no budget of its own: the
/// column default of migration `0004_jobs`, written explicitly by
/// [`enqueue`] so the budget is decided in one place
/// (`JobKind::max_attempts`). Three attempts on the [`fail`] ladder (60 s,
/// then 120 s) dead-letter a job about three minutes after its first
/// failure.
pub const DEFAULT_MAX_ATTEMPTS: i32 = 3;

/// A payload redaction [`reap`] applies in the same `UPDATE` that moves a
/// job to `dead`, for a kind whose payload holds what must not outlive the
/// job (`workers::ingest_receive::REAP_REDACTION`). The queue stays generic:
/// which kinds, which fields and what replaces them are the caller's. Every
/// `paths` entry is a JSON path into `payload` (`["envelope", "message",
/// "data"]`); a path whose parents are missing leaves the payload unchanged
/// (`jsonb_set` with `create_if_missing = false`). A reaped job that goes
/// back to `pending` is never redacted: its retry runs from the payload.
#[derive(Debug, Clone, Copy)]
pub struct DeadPayloadRedaction {
    pub kinds: &'static [&'static str],
    pub paths: &'static [&'static [&'static str]],
    pub replacement: &'static str,
}

// ── Public API ────────────────────────────────────────────────────────────────

/// Insert a new job.
///
/// Dedup is scoped to **pending** jobs via the `idx_jobs_idem_pending` partial
/// unique index (migration 0050): `ON CONFLICT (idempotency_key) WHERE status =
/// 'pending' DO NOTHING`. If a job with the same key is still queued, the second
/// insert is dropped and `None` is returned; once the prior job is claimed
/// (leaves 'pending') or completes, a fresh enqueue with that key succeeds again.
/// This is what lets a debounced per-graph pass (a constant key like
/// `infer:<graph>`) re-run as new work arrives, instead of firing exactly once
/// per graph forever. When `idempotency_key` is `None` no dedup is applied.
///
/// `delay_secs` postpones the job's earliest execution time; pass `0` for
/// immediate scheduling.
///
/// `account_id` is the account this job's processing will be scoped to
/// (org-tenancy Phase W.1 — see [`JobRow::account_id`]). Pass `Some` whenever
/// the caller knows the owning account (every production call site does,
/// except the deliberately account-less unscoped sweeps, which pass `None`).
pub async fn enqueue(
    pool: &PgPool,
    kind: &JobKind,
    account_id: Option<Uuid>,
    idempotency_key: Option<&str>,
    delay_secs: i64,
) -> Result<Option<Uuid>, sqlx::Error> {
    let mut tx = pool.begin().await.inspect_err(|_| super::publication::enqueue_failed())?;
    let result = enqueue_tx(&mut tx, kind, account_id, idempotency_key, delay_secs).await;
    match result {
        Ok(id) => { tx.commit().await.inspect_err(|_| super::publication::enqueue_failed())?; Ok(id) }
        Err(e) => { super::publication::enqueue_failed(); Err(e) }
    }
}


/// Report (or initialise) a running job's progress — the read half of the
/// async-promote progress bar contract. Deliberately a bare `(current,
/// total)` overwrite, not an increment: the caller (`workers::promote`)
/// always knows its own running total for the chunk it just committed, so
/// there is no read-modify-write race to protect against, and an overwrite
/// means a worker that restarts mid-job and RE-derives its count from
/// scratch (this job kind's idempotent full-rescan-on-retry design — see
/// [`super::JobKind::PromoteNode`]'s doc comment) self-corrects instead of
/// compounding a stale increment.
///
/// Deliberately does NOT touch `status`/`started_at`/anything [`complete`]
/// or [`fail`] own — a progress update is a pure observability write, never
/// a state transition, so it can safely run any number of times against a
/// `running` job without interacting with the completion/retry machinery at
/// all. Safe to call from a job that is not `running` too (an operator
/// re-checking a `done` job's final progress, say); it just overwrites the
/// same two columns either way.
pub async fn update_progress(
    pool: &PgPool,
    job_id: Uuid,
    current: i64,
    total: i64,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        UPDATE jobs
        SET    progress_current = $2,
               progress_total   = $3
        WHERE  id = $1
        "#,
    )
    .bind(job_id)
    .bind(current)
    .bind(total)
    .execute(pool)
    .await?;

    Ok(())
}

/// Split a `JobKind` into the `(kind, payload)` pair the `jobs` table expects
/// — shared by [`enqueue`] and [`enqueue_tx`] so the two never drift.
fn tag_job(kind: &JobKind) -> (String, serde_json::Value) {
    // Serialise the enum: `kind` (tag) and `payload` (content) come out as a
    // flat JSON object — split them for the two separate DB columns.
    let tagged = serde_json::to_value(kind).expect("JobKind serialisation is infallible");

    let kind_str = tagged["kind"]
        .as_str()
        .expect("JobKind always produces a string `kind` tag")
        .to_owned();

    let payload = tagged["payload"].clone();
    (kind_str, payload)
}

/// Same contract as [`enqueue`], but runs against an existing transaction so
/// the caller can commit the enqueue atomically alongside other writes (e.g.
/// `routes::slack::ingest_slack_message` persisting a
/// `connector_identities` row and enqueuing its `SlackNameBackfill` repair
/// job together — see that call site's doc comment for why a non-atomic
/// two-step there is a durability gap: an enqueue failure after a successful
/// identity write would otherwise never be retried, since later ingests hit
/// the directory-hit branch and skip the enqueue entirely).
pub async fn enqueue_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    kind: &JobKind,
    account_id: Option<Uuid>,
    idempotency_key: Option<&str>,
    delay_secs: i64,
) -> Result<Option<Uuid>, sqlx::Error> {
    let (kind_str, payload) = tag_job(kind);

    // A tracked enqueue returns and locks the shared pending job on conflict.
    // The lock prevents a dequeuer from claiming it before its dependency commits.
    let query = if super::publication::parent().is_some() {
        "INSERT INTO jobs (kind,payload,account_id,idempotency_key,run_after,max_attempts)
         VALUES ($1,$2,$3,$4,now()+($5*INTERVAL '1 second'),$6)
         ON CONFLICT (idempotency_key) WHERE status='pending'
         DO UPDATE SET idempotency_key=EXCLUDED.idempotency_key RETURNING id"
    } else {
        "INSERT INTO jobs (kind,payload,account_id,idempotency_key,run_after,max_attempts)
         VALUES ($1,$2,$3,$4,now()+($5*INTERVAL '1 second'),$6)
         ON CONFLICT (idempotency_key) WHERE status='pending' DO NOTHING RETURNING id"
    };
    let id: Option<Uuid> = sqlx::query_scalar(query)
        .bind(&kind_str).bind(payload).bind(account_id).bind(idempotency_key).bind(delay_secs).bind(kind.max_attempts())
        .fetch_optional(&mut **tx).await?;
    if let Some(id)=id {
        super::publication::link(tx,id).await?;
    }
    Ok(id)
}

/// [`enqueue_tx`] without a publication link, on any connection. For
/// graph-level follow-up work (entity resolution) enqueued from inside a
/// source job: the follow-up is deduped per graph and may wait on a managed
/// switch or a daily cap, so it must never become a child that holds the
/// source's publication open.
pub async fn enqueue_detached(
    conn: &mut sqlx::PgConnection,
    kind: &JobKind,
    account_id: Option<Uuid>,
    idempotency_key: Option<&str>,
    delay_secs: i64,
) -> Result<Option<Uuid>, sqlx::Error> {
    let (kind_str, payload) = tag_job(kind);
    let id: Option<Uuid> = sqlx::query_scalar(
        "INSERT INTO jobs (kind,payload,account_id,idempotency_key,run_after,max_attempts)
         VALUES ($1,$2,$3,$4,now()+($5*INTERVAL '1 second'),$6)
         ON CONFLICT (idempotency_key) WHERE status='pending' DO NOTHING RETURNING id",
    )
    .bind(&kind_str).bind(payload).bind(account_id).bind(idempotency_key).bind(delay_secs).bind(kind.max_attempts())
    .fetch_optional(&mut *conn).await?;
    Ok(id)
}

/// Claim up to `concurrency_limit` pending jobs of the given kinds.
///
/// The SELECT … FOR UPDATE SKIP LOCKED pattern atomically locks the rows so
/// only one worker claims each job, even under high concurrency.
///
/// **Pre-W.1 legacy rows** (NULL `jobs.account_id` — enqueued before
/// migration 0085 deployed) get their `account_id` backfilled here via the
/// 0084 SECURITY DEFINER resolvers (`resolve_graph_account_id` /
/// `resolve_atoms_account_id`), keyed off the graph/atom reference the
/// payload already carries. A legacy row whose reference can no longer be
/// resolved (deleted graph, vanished atoms, multi-account atom batch) is
/// immediately routed through [`fail`] with a [`MISSING_ACCOUNT_ID_ERR`]
/// error and **not returned** — it must never reach a worker unscoped
/// post-OPS-4, and `fail`'s bounded attempts converge it to `dead` instead
/// of retrying forever. Kinds with no single-account reference by design
/// (the unscoped sweeps — see [`JobRow::account_id`]) pass through with
/// `None` untouched. New rows always carry `account_id`, so this fallback
/// costs zero extra round-trips on the steady-state path.
pub async fn dequeue(
    pool: &PgPool,
    kinds: &[&str],
    concurrency_limit: usize,
) -> Result<Vec<JobRow>, sqlx::Error> {
    // Build a CTE that selects candidate rows then immediately updates them to
    // `running`.  All done in a single round-trip.
    let rows = sqlx::query(
        r#"
        WITH claimed AS (
            SELECT id
            FROM   jobs
            WHERE  status    = 'pending'
              AND  run_after <= now()
              AND  kind       = ANY($1)
            ORDER BY run_after, id
            LIMIT  $2
            FOR UPDATE SKIP LOCKED
        )
        UPDATE jobs j
        SET    status     = 'running',
               started_at = now()
        FROM   claimed
        WHERE  j.id = claimed.id
        RETURNING j.id, j.kind, j.payload, j.attempts, j.account_id
        "#,
    )
    .bind(kinds) // $1  TEXT[]
    .bind(concurrency_limit as i64) // $2  BIGINT
    .fetch_all(pool)
    .await?;
    claimed(pool, rows).await
}

/// [`dequeue`] restricted to `ids`, claimed in the order given. A worker that
/// picks its own candidates (a priority lane) claims them through here, so
/// they get the same `SKIP LOCKED` race and legacy account handling.
pub async fn dequeue_ids(
    pool: &PgPool,
    kinds: &[&str],
    ids: &[Uuid],
) -> Result<Vec<JobRow>, sqlx::Error> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let rows = sqlx::query(
        r#"
        WITH claimed AS (
            SELECT id
            FROM   jobs
            WHERE  status    = 'pending'
              AND  run_after <= now()
              AND  kind       = ANY($1)
              AND  id         = ANY($2)
            ORDER BY array_position($2, id)
            FOR UPDATE SKIP LOCKED
        )
        UPDATE jobs j
        SET    status     = 'running',
               started_at = now()
        FROM   claimed
        WHERE  j.id = claimed.id
        RETURNING j.id, j.kind, j.payload, j.attempts, j.account_id
        "#,
    )
    .bind(kinds)
    .bind(ids)
    .fetch_all(pool)
    .await?;
    claimed(pool, rows).await
}

async fn claimed(pool: &PgPool, rows: Vec<sqlx::postgres::PgRow>) -> Result<Vec<JobRow>, sqlx::Error> {
    let mut jobs = Vec::with_capacity(rows.len());
    for r in rows {
        use sqlx::Row;
        let mut job = JobRow {
            id: r.get("id"),
            kind: r.get("kind"),
            payload: r.get("payload"),
            attempts: r.get("attempts"),
            account_id: r.get("account_id"),
        };

        // Pre-W.1 legacy row: backfill (or fail-path) before any worker sees it.
        if job.account_id.is_none() {
            match backfill_legacy_account_id(pool, &job).await {
                LegacyBackfill::Resolved(account_id) => {
                    job.account_id = Some(account_id);
                    // Best-effort persist so a later retry of this same row
                    // (via `fail`/`reap`) doesn't need to re-resolve.
                    if let Err(e) = sqlx::query("UPDATE jobs SET account_id = $1 WHERE id = $2")
                        .bind(account_id)
                        .bind(job.id)
                        .execute(pool)
                        .await
                    {
                        tracing::warn!(
                            job_id = %job.id,
                            error = %e,
                            "dequeue: failed to persist backfilled account_id (non-fatal)"
                        );
                    }
                }
                LegacyBackfill::Unscoped => {} // account-less by design; pass through.
                LegacyBackfill::Unresolvable(reason) => {
                    let msg = format!("{MISSING_ACCOUNT_ID_ERR}: {reason}");
                    tracing::error!(job_id = %job.id, kind = %job.kind, %msg, "dequeue: failing unresolvable pre-W.1 job");
                    if let Err(e) = fail(pool, job.id, &msg).await {
                        tracing::error!(
                            job_id = %job.id,
                            error = %e,
                            "dequeue: failed to fail-path unresolvable pre-W.1 job (it stays 'running' until the reaper resets it)"
                        );
                    }
                    continue; // never hand it to a worker unscoped.
                }
            }
        }

        jobs.push(job);
    }

    Ok(jobs)
}

// ── Pre-W.1 legacy-row account backfill ───────────────────────────────────────

/// Outcome of [`backfill_legacy_account_id`].
enum LegacyBackfill {
    /// The payload's graph/atom reference resolved to exactly one account.
    Resolved(Uuid),
    /// This kind carries no single-account reference by design (unscoped
    /// sweep or a dormant kind with no resolver) — pass through with `None`.
    Unscoped,
    /// The reference exists but could not be mapped to one owning account
    /// (or the resolver call itself errored) — the job must be fail-pathed.
    Unresolvable(String),
}

/// The single-account reference a job's payload carries, extracted by kind.
///
/// Reads the raw payload JSON (not a `JobKind` round-trip) so an unknown or
/// malformed payload degrades to `Unscoped`/worker-side malformed handling
/// instead of erroring here — the worker's own `parse_*` already owns the
/// malformed-payload fail path.
enum AccountRef {
    Graph(Uuid),
    Atoms(Vec<Uuid>),
    /// No single-account reference by design (unscoped sweeps, harness/test
    /// kinds, unknown kinds) — pass through with `None`.
    None,
    /// The kind is single-account in nature but its payload carries no
    /// reference the 0084 resolvers can map (`RouteCopresence`'s session_id /
    /// `SendOutreach`'s user_id have no bootstrap resolver). Dormant today —
    /// no production enqueue site or worker exists for either — but a legacy
    /// NULL row must dead-letter loudly rather than silently bypass tenant
    /// scoping if a worker ever appears (CodeRabbit review, PR #287).
    NoResolver,
}

fn job_account_ref(kind: &str, payload: &serde_json::Value) -> AccountRef {
    let graph_id = |v: &serde_json::Value| -> Option<Uuid> {
        v.get("graph_id")
            .and_then(|g| g.as_str())
            .and_then(|s| s.parse().ok())
    };
    match kind {
        "EmbedAtoms" | "EmbedAtomsRetry" => {
            let ids: Vec<Uuid> = payload
                .get("atom_ids")
                .and_then(|v| v.as_array())
                .map(|a| a.iter().filter_map(|x| x.as_str()?.parse().ok()).collect())
                .unwrap_or_default();
            if ids.is_empty() {
                AccountRef::None
            } else {
                AccountRef::Atoms(ids)
            }
        }
        "ComputeDrift"
        | "EntityResolution"
        | "InferRelationships"
        | "GenerateMeetingSummary"
        | "ExtractMeetingAtoms"
        | "ExtractSourceAtoms"
        | "ExtractStandingSourceAtoms"
        | "ExtractStandingMeetingAtoms"
        | "EmbedStatements"
        | "RegenerateDigest"
        | "PropagateCorrections"
        | "OceanHealth"
        | "HealthViolation" => graph_id(payload)
            .map(AccountRef::Graph)
            .unwrap_or(AccountRef::None),
        // `SlackNameBackfill` is per-graph when scoped, a deliberate
        // all-accounts sweep when `graph_id` is null.
        "SlackNameBackfill" => graph_id(payload)
            .map(AccountRef::Graph)
            .unwrap_or(AccountRef::None),
        // Single-account kinds whose payload reference (session_id / user_id)
        // has no 0084 bootstrap resolver — must dead-letter, never pass
        // through unscoped (see `AccountRef::NoResolver`).
        "RouteCopresence" | "SendOutreach" | "DeleteSourceBlob" => AccountRef::NoResolver,
        // Deliberately account-less unscoped sweeps
        // ("ContentTsvRegconfigRepair", "ReembedOutdatedChunks") and anything
        // unknown (e.g. integration-test harness kinds).
        _ => AccountRef::None,
    }
}

/// Resolve a pre-W.1 job's owning account via the 0084 SECURITY DEFINER
/// bootstrap functions — the ONE sanctioned way to answer "which account owns
/// this row?" before a scoped connection exists (see that migration's
/// header). SECURITY DEFINER means this works identically pre- and
/// post-OPS-4, regardless of the caller role's RLS visibility.
async fn backfill_legacy_account_id(pool: &PgPool, job: &JobRow) -> LegacyBackfill {
    match job_account_ref(&job.kind, &job.payload) {
        AccountRef::None => LegacyBackfill::Unscoped,
        AccountRef::NoResolver => LegacyBackfill::Unresolvable(format!(
            "kind {} is single-account but has no bootstrap account resolver",
            job.kind
        )),
        AccountRef::Graph(graph_id) => {
            match sqlx::query_scalar::<_, Option<Uuid>>("SELECT resolve_graph_account_id($1)")
                .bind(graph_id)
                .fetch_one(pool)
                .await
            {
                Ok(Some(account_id)) => LegacyBackfill::Resolved(account_id),
                Ok(None) => {
                    LegacyBackfill::Unresolvable(format!("graph {graph_id} resolves to no account"))
                }
                Err(e) => LegacyBackfill::Unresolvable(format!(
                    "resolve_graph_account_id({graph_id}) errored: {e}"
                )),
            }
        }
        AccountRef::Atoms(atom_ids) => {
            match sqlx::query_scalar::<_, Uuid>("SELECT resolve_atoms_account_id($1)")
                .bind(&atom_ids)
                .fetch_all(pool)
                .await
            {
                Ok(accounts) => match accounts.as_slice() {
                    [one] => LegacyBackfill::Resolved(*one),
                    [] => LegacyBackfill::Unresolvable(format!(
                        "{} atom_id(s) resolve to no account",
                        atom_ids.len()
                    )),
                    many => LegacyBackfill::Unresolvable(format!(
                        "{} atom_id(s) span {} accounts — cannot single-scope",
                        atom_ids.len(),
                        many.len()
                    )),
                },
                Err(e) => {
                    LegacyBackfill::Unresolvable(format!("resolve_atoms_account_id errored: {e}"))
                }
            }
        }
    }
}

// ── Typed completion outcomes (Track B step 1) ─────────────────────────────────
//
// The defect class this register exists to make impossible to express again:
// on 2026-08-04 `ExtractRelationships` dequeued an atom whose content lived
// only in R2 (`content_blob_key` set, `content_inline` NULL) — a shape that
// worker had no code path to read. It treated "I cannot read my own input" as
// "there is nothing to do", called the OLD bare `complete(pool, job_id)`, and
// the row landed `status='done'`. Every dashboard reading `jobs.status`
// stayed green while all graph-relationship inference silently stopped for
// three days — nothing distinguished that `done` row from one where the
// worker actually did its job.
//
// [`complete`]/[`complete_with_payload`] now require a declared [`JobOutcome`]
// instead of a bare job id. The fix is in the SHAPE of that type, not a
// runtime check: [`JobOutcome`] has exactly two variants, `DidWork` and
// `NothingToDo` — there is no third "I couldn't do it" variant for a worker to
// reach for. A worker that cannot process its own input has NOTHING of type
// `JobOutcome` to construct and hand to `complete`; the only calls available
// to it are [`could_not_do`] (transient — stays on the existing [`fail`]
// backoff ladder) or a structural dead-letter (bypasses the ladder entirely —
// see [`FailureClass`]). Misrouting a "couldn't do it" job through `complete`
// is therefore a compile error at every one of the ~30 call sites across the
// worker fleet, not a pattern a human reviewer has to keep re-catching.

/// The declared outcome of a job a worker is marking [`complete`] —
/// deliberately NOT the same enum as [`FailureClass`]. See this module's
/// "Typed completion outcomes" section header for why the absence of a third
/// variant here is the whole point.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobOutcome {
    /// The worker performed the job: wrote the entities/edges/summary/whatever
    /// it was dequeued to produce.
    DidWork,
    /// The worker correctly determined there was legitimately nothing to do —
    /// e.g. the atom was already processed by an earlier pass, the input was
    /// empty by design, or a sweep's candidate window came back empty. NOT for
    /// "the input existed but this worker has no way to read it" — that is
    /// [`FailureClass::Structural`], which must never reach `complete`.
    NothingToDo,
}

impl JobOutcome {
    fn as_db_str(self) -> &'static str {
        match self {
            JobOutcome::DidWork => "did_work",
            JobOutcome::NothingToDo => "nothing_to_do",
        }
    }
}

/// Why a [`could_not_do`] job could not be processed — routes to a DIFFERENT
/// place depending on which: see [`could_not_do`]'s doc comment for the full
/// transient-vs-structural split and why the two must alert differently.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureClass {
    /// A retryable environmental failure (R2 timeout, pool exhaustion, a 5xx,
    /// a flaky LLM response the SAME worker would likely parse fine on a
    /// second attempt). Stays on [`fail`]'s existing exponential-backoff
    /// ladder, exactly like any other transient error.
    Transient,
    /// This worker cannot process its own input BY DESIGN — the 2026-08-04
    /// shape: retrying the identical job against the identical worker would
    /// hit the identical wall every time, so the attempts ladder buys nothing
    /// but three wasted retries and a slower alert. Dead-letters immediately.
    Structural,
}

impl FailureClass {
    fn as_db_str(self) -> &'static str {
        match self {
            FailureClass::Transient => "transient",
            FailureClass::Structural => "structural",
        }
    }
}

/// Mark a job as successfully completed, recording which of [`JobOutcome`]'s
/// two shapes this was. See this module's "Typed completion outcomes" section
/// header for why a worker that could not do its job has no `JobOutcome` to
/// pass here.
///
/// # A failed CHILD enqueue does not un-do this job
///
/// This used to `return fail(...)` whenever `publication::has_enqueue_failure`
/// was set — i.e. whenever a downstream job this one tried to schedule could
/// not be written. That is the wrong ledger to charge (CodeRabbit round 1 on
/// #871). `publication::scope` wraps EVERY `ExtractEntities` /
/// `ExtractRelationships` job, and those workers enqueue their
/// `EntityResolution` / `InferRelationships` follow-ups
/// AFTER their own extraction results are already durably written. Routing
/// that through [`fail`] incremented `attempts`, re-queued a job whose work
/// was complete (re-running a paid LLM extraction to re-derive results that
/// were already in the database), and on a persistent scheduling failure
/// exhausted `max_attempts` and DEAD-LETTERED it — which
/// `publication::status` counts as a failed stage, pinning the parent source
/// publication in `attention` forever over work that actually succeeded.
///
/// So the job lands `done` — it did what it was dequeued to do — and the
/// scheduling failure is recorded on the same row as a stable
/// `error_message` ([`super::publication::ENQUEUE_FAILURE_MESSAGE`]).
/// `publication::status` counts a `done` row carrying an `error_message` as a
/// failed stage, so the publication still refuses to call itself `ready`,
/// and the connection's existing **Retry** action re-queues exactly these
/// rows. No retry budget is consumed and no extraction is repeated on its
/// own.
///
/// That contract is why this statement also CLEARS `error_message` on an
/// ordinary success: [`fail`]'s ladder writes an error and re-queues, so
/// without this a job that failed once and then succeeded would keep the
/// stale text and read, to `publication::status`, as permanently failed.
pub async fn complete(pool: &PgPool, job_id: Uuid, outcome: JobOutcome) -> Result<(), sqlx::Error> {
    complete_on(pool, job_id, outcome).await
}

/// [`complete`] on any executor, so a worker can close the job in the same
/// transaction as the state it settles.
pub async fn complete_on<'e>(
    executor: impl sqlx::PgExecutor<'e>,
    job_id: Uuid,
    outcome: JobOutcome,
) -> Result<(), sqlx::Error> {
    let scheduling_failure = super::publication::has_enqueue_failure(job_id)
        .then_some(super::publication::ENQUEUE_FAILURE_MESSAGE);
    if scheduling_failure.is_some() {
        tracing::warn!(
            %job_id,
            "job completed its own work but could not schedule its downstream jobs; \
             recorded on the job row rather than retried"
        );
    }
    sqlx::query(
        r#"
        UPDATE jobs
        SET    status        = 'done',
               finished_at   = now(),
               outcome       = $2,
               error_message = $3
        WHERE  id = $1
        "#,
    )
    .bind(job_id)
    .bind(outcome.as_db_str())
    .bind(scheduling_failure)
    .execute(executor)
    .await?;

    Ok(())
}

/// [`complete`], plus OVERWRITING `payload` with `redacted_payload` in the
/// same statement.
///
/// `done` rows are retained indefinitely (migration `0050`'s own header: "are
/// retained (status='done')") — there is no purge worker for `jobs`. For most
/// job kinds that is fine (a `graph_id`/`atom_id` reference, never raw
/// content). [`JobKind::SlackMessageIngest`] is the first kind whose payload
/// carries actual message TEXT, so completing it through the plain
/// [`complete`] would leave that plaintext sitting at rest in `jobs.payload`
/// forever — a new, unbounded-retention location for content that previously
/// went straight into an encrypted atom blob. This variant exists so a
/// worker can scrub the sensitive field(s) at the exact moment they stop
/// being needed (the job succeeded; the atom is already durably written)
/// while still keeping the row itself, its `kind`, and whatever
/// non-sensitive payload fields the caller chooses to keep, for the same
/// observability `complete` gives every other job kind.
pub async fn complete_with_payload(
    pool: &PgPool,
    job_id: Uuid,
    outcome: JobOutcome,
    redacted_payload: serde_json::Value,
) -> Result<(), sqlx::Error> {
    // `error_message = NULL` for the same reason as [`complete`]: a job that
    // failed once, was re-queued by `fail`'s ladder and then succeeded must
    // not keep the stale error text, which `publication::status` reads as a
    // failed stage on a `done` row.
    sqlx::query(
        r#"
        UPDATE jobs
        SET    status        = 'done',
               finished_at   = now(),
               outcome       = $2,
               payload       = $3,
               error_message = NULL
        WHERE  id = $1
        "#,
    )
    .bind(job_id)
    .bind(outcome.as_db_str())
    .bind(redacted_payload)
    .execute(pool)
    .await?;

    Ok(())
}

/// Record a job failure, applying exponential back-off or marking it dead.
///
/// Back-off formula: `now() + 2^attempts * 30 seconds`.
/// Once `attempts >= max_attempts` the job is moved to `dead` so it will
/// never be retried automatically.
///
/// For an ordinary error (malformed payload, an unscoped job, a plain DB
/// error) — anything that was never run through [`could_not_do`]'s explicit
/// classification — `failure_class` stays untouched at NULL, exactly as
/// before Track B. Use [`could_not_do`] instead of calling this directly when
/// a worker HAS made the "I cannot process this input" judgment call; it
/// stamps `failure_class` and, for [`FailureClass::Structural`], skips this
/// ladder entirely.
pub async fn fail(pool: &PgPool, job_id: Uuid, error: &str) -> Result<(), sqlx::Error> {
    fail_ladder(pool, job_id, error, None).await
}

/// What [`fail_keeping_key`] did with the job.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FailedKeepingKey {
    /// Back to `pending` on the backoff ladder, its idempotency key kept, so
    /// a new enqueue under that key coalesces into it while it waits.
    Pending,
    /// The ladder is exhausted: `dead`, with `settled_payload`.
    Dead,
    /// A newer pending job already holds this job's key (it was enqueued
    /// while this one ran), so it carries the retry: this one completed
    /// `nothing_to_do` with `settled_payload`.
    Superseded,
    /// The job was no longer `running` (reaped): nothing written.
    NotRunning,
}

/// [`fail`] for a job whose retry covers everything a new enqueue under the
/// same key would do (`workers::ingest_receive`: a
/// mailbox pass diffs from its cursor, a Graph subscription's change is
/// re-fetched by id). The key is KEPT while the job waits on backoff, so
/// every push for that mailbox or resource during an outage coalesces into
/// the one waiting job instead of adding a row each; the pending-only unique
/// index (migration `0050`) covers a job on backoff, since it is `pending`
/// with a future `run_after`. If a newer pending job took the key while this
/// one ran, this one completes as superseded and the newer one carries the
/// work, as [`hold`] does. The terminal row (`dead`, or superseded) takes
/// `settled_payload`; a row that will retry keeps its payload.
///
/// [`fail`] itself nulls the key on every retry because other kinds rely
/// on it: a re-queued pass that must run regardless of a newer pending one
/// (the `infer:` debounce) would otherwise collide with it on the index.
pub async fn fail_keeping_key(
    pool: &PgPool,
    job_id: Uuid,
    error: &str,
    settled_payload: serde_json::Value,
) -> Result<FailedKeepingKey, sqlx::Error> {
    let error = coralswarm_core::pg_text::sanitize(error);
    let laddered = sqlx::query_scalar::<_, bool>(
        r#"
        UPDATE jobs j
        SET    attempts        = j.attempts + 1,
               error_message   = $2,
               payload         = CASE WHEN j.attempts + 1 >= j.max_attempts THEN $3 ELSE j.payload END,
               status          = CASE WHEN j.attempts + 1 >= j.max_attempts THEN 'dead'::job_status ELSE 'pending'::job_status END,
               idempotency_key = CASE WHEN j.attempts + 1 >= j.max_attempts THEN NULL ELSE j.idempotency_key END,
               run_after       = CASE WHEN j.attempts + 1 >= j.max_attempts THEN j.run_after
                                      ELSE now() + (power(2, j.attempts + 1) * 30 || ' seconds')::interval END,
               finished_at     = CASE WHEN j.attempts + 1 >= j.max_attempts THEN now() ELSE NULL END
        WHERE  j.id = $1 AND j.status = 'running'
          AND (j.idempotency_key IS NULL
               OR j.attempts + 1 >= j.max_attempts
               OR NOT EXISTS (SELECT 1 FROM jobs p
                              WHERE p.idempotency_key = j.idempotency_key AND p.status = 'pending' AND p.id <> j.id))
        RETURNING j.status = 'dead'::job_status
        "#,
    )
    .bind(job_id)
    .bind(error.as_ref())
    .bind(&settled_payload)
    .fetch_optional(pool)
    .await;
    match laddered {
        Ok(Some(true)) => return Ok(FailedKeepingKey::Dead),
        Ok(Some(false)) => return Ok(FailedKeepingKey::Pending),
        Ok(None) => {}
        // A sibling took the key between the check and the write.
        Err(sqlx::Error::Database(e)) if e.is_unique_violation() => {}
        Err(e) => return Err(e),
    }
    let superseded = sqlx::query(
        "UPDATE jobs SET status='done', finished_at=now(), outcome=$2, payload=$3, error_message=NULL \
          WHERE id=$1 AND status='running'",
    )
    .bind(job_id)
    .bind(JobOutcome::NothingToDo.as_db_str())
    .bind(&settled_payload)
    .execute(pool)
    .await?;
    Ok(if superseded.rows_affected() == 1 { FailedKeepingKey::Superseded } else { FailedKeepingKey::NotRunning })
}

/// [`fail`] on any executor. See [`complete_on`].
pub async fn fail_on<'e>(executor: impl sqlx::PgExecutor<'e>, job_id: Uuid, error: &str) -> Result<(), sqlx::Error> {
    fail_ladder(executor, job_id, error, None).await
}

/// Hand a claimed job back to `pending` untouched: no attempt is charged and
/// no outcome is recorded. For a worker that is switched off, not failing.
pub async fn release(pool: &PgPool, job_id: Uuid, after: std::time::Duration) -> Result<(), sqlx::Error> {
    sqlx::query("UPDATE jobs SET status='pending',started_at=NULL,idempotency_key=NULL,run_after=now()+$2::double precision*interval '1 second' WHERE id=$1 AND status='running'")
        .bind(job_id)
        .bind(after.as_secs_f64())
        .execute(pool)
        .await?;
    Ok(())
}

// ── Managed holds: the shared retry rule ─────────────────────────────────────
//
// A worker whose managed call is held (`coralswarm_core::send_gate::Deny`)
// did not fail: nothing ran. It hands the job back through [`hold`] (or
// [`hold_until`] for a known clear time), which keeps the SAME row: no attempt
// is charged, no fresh row is written, and `jobs.hold_streak` counts the
// holds in a row. The streak and the hold's class set the delay
// ([`hold_retry`]):
//
// * a hold that can clear (money, a switch or pause, managed storage, a lost
//   race) backs off from 15 minutes, doubling, to a 6-hour cap;
// * a hold that cannot clear on its own (the requester lost access, the
//   source is gone, no member can be charged, the provider refused the
//   request) waits 1 h, 4 h, 16 h, then dead-letters on the fourth hold in a
//   row, about 21 hours after the first, with `error_message` `held: <code>`.
//   `failure_class` stays NULL, as for an exhausted [`fail`] ladder: the
//   worker is not broken, so the chain-health structural invariant
//   (`routes::admin`), whose threshold is zero, must not count it.
//
// Before COR-114 every worker completed the held job and enqueued a fresh
// one 15 minutes out, forever: one personal-org account refused every
// managed purpose wrote ~43,000 `RegenerateDigest` rows a day.

/// Whether a managed hold can clear without this job doing anything.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HoldClass {
    /// Credits, a daily cap, a switch, a pause, managed storage, the
    /// lifecycle contract, a fence lost to a concurrent writer.
    Clears,
    /// The requester lost access, the source was removed, no member can be
    /// charged (an unresolvable liability plane included), or the request
    /// itself is refused (unpriced, over a cost cap, no gated transport).
    Never,
}

/// The class of a hold code (`HoldCode::code`, `RefuseCode::code`, or a
/// worker's own reason). Unknown codes can clear: a misfiled clearable hold
/// only costs capped retries, a misfiled permanent one would dead-letter
/// work that could still run.
pub fn hold_class(code: &str) -> HoldClass {
    match code {
        "requester_access_lost" | "source_removed_or_access_lost" | "principal_unavailable"
        | "unpriced_model" | "call_cost_cap" | "version_cost_cap" | "provider_not_gated" => HoldClass::Never,
        _ => HoldClass::Clears,
    }
}

/// The hold code for a managed call denied `Deny::Unavailable`: managed
/// storage, or the account's billing lock or entry-queue turn not coming
/// within its bound (#1497). Nothing ran and nothing is wrong with the job,
/// so it can clear ([`HoldClass::Clears`]) and must never charge an attempt.
pub const MANAGED_UNAVAILABLE: &str = "managed_unavailable";

/// First retry of a clearable hold, doubling per hold in a row.
pub const HOLD_CLEARS_BASE_SECS: i64 = 900;
/// Longest wait between retries of a clearable hold.
pub const HOLD_CLEARS_CAP_SECS: i64 = 6 * 3600;
/// First retry of a hold that cannot clear, times four per hold in a row.
pub const HOLD_NEVER_BASE_SECS: i64 = 3600;
pub const HOLD_NEVER_CAP_SECS: i64 = 24 * 3600;
/// The hold in a row at which a hold that cannot clear dead-letters.
pub const HOLD_NEVER_DEAD_AT: i32 = 4;

/// When a held job runs again.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HoldRetry {
    After(i64),
    DeadLetter,
}

/// The retry for a hold of `class` on a job already held `streak` times in
/// a row.
pub fn hold_retry(class: HoldClass, streak: i32) -> HoldRetry {
    let streak = streak.clamp(0, 30);
    let pow = |base: i64| base.saturating_pow(streak as u32);
    match class {
        HoldClass::Clears => HoldRetry::After(HOLD_CLEARS_BASE_SECS.saturating_mul(pow(2)).min(HOLD_CLEARS_CAP_SECS)),
        HoldClass::Never if streak + 1 >= HOLD_NEVER_DEAD_AT => HoldRetry::DeadLetter,
        HoldClass::Never => HoldRetry::After(HOLD_NEVER_BASE_SECS.saturating_mul(pow(4)).min(HOLD_NEVER_CAP_SECS)),
    }
}

/// What [`hold`] did with the job.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Held {
    /// Back to `pending`, due after this many seconds.
    Pending(i64),
    /// A hold that cannot clear, held [`HOLD_NEVER_DEAD_AT`] times in a row.
    DeadLettered,
    /// A newer pending job already holds this job's idempotency key, so it
    /// carries the work: this one completed `nothing_to_do`.
    Superseded,
    /// The job was no longer `running` (reaped): nothing written.
    NotRunning,
}

/// Hold a claimed job for `code`, per the shared rule ([`hold_retry`]).
pub async fn hold(pool: &PgPool, job_id: Uuid, code: &str) -> Result<Held, sqlx::Error> {
    let streak: Option<i32> =
        sqlx::query_scalar("SELECT hold_streak FROM jobs WHERE id=$1 AND status='running'")
            .bind(job_id)
            .fetch_optional(pool)
            .await?;
    let Some(streak) = streak else { return Ok(Held::NotRunning) };
    match hold_retry(hold_class(code), streak) {
        HoldRetry::After(secs) => hold_pending(pool, job_id, code, secs, 1).await,
        HoldRetry::DeadLetter => {
            let dead = sqlx::query(
                "UPDATE jobs SET status='dead'::job_status, error_message='held: '||$2, \
                        idempotency_key=NULL, finished_at=now(), hold_streak=hold_streak+1 \
                  WHERE id=$1 AND status='running'",
            )
            .bind(job_id)
            .bind(code)
            .execute(pool)
            .await?;
            Ok(if dead.rows_affected() == 1 { Held::DeadLettered } else { Held::NotRunning })
        }
    }
}

/// Hold a claimed job until a known time (the next UTC day for a daily
/// bound). The streak is left as it is and the job never dead-letters: the
/// wait ends by itself.
pub async fn hold_until(pool: &PgPool, job_id: Uuid, code: &str, after_secs: i64) -> Result<Held, sqlx::Error> {
    hold_pending(pool, job_id, code, after_secs.max(0), 0).await
}

/// Back to `pending` in place, keeping the idempotency key, unless a newer
/// pending job already holds that key: then this one completes and the newer
/// one carries the work.
async fn hold_pending(pool: &PgPool, job_id: Uuid, code: &str, secs: i64, count: i32) -> Result<Held, sqlx::Error> {
    let held = sqlx::query(
        "UPDATE jobs j SET status='pending'::job_status, started_at=NULL, finished_at=NULL, outcome=NULL, \
                run_after=now()+make_interval(secs => $3), error_message='held: '||$2, hold_streak=j.hold_streak+$4 \
          WHERE j.id=$1 AND j.status='running' \
            AND (j.idempotency_key IS NULL OR NOT EXISTS(SELECT 1 FROM jobs p \
                 WHERE p.idempotency_key=j.idempotency_key AND p.status='pending' AND p.id<>j.id))",
    )
    .bind(job_id)
    .bind(code)
    .bind(secs as f64)
    .bind(count)
    .execute(pool)
    .await;
    match held {
        Ok(done) if done.rows_affected() == 1 => return Ok(Held::Pending(secs)),
        Ok(_) => {}
        // A sibling took the key between the check and the write.
        Err(sqlx::Error::Database(e)) if e.is_unique_violation() => {}
        Err(e) => return Err(e),
    }
    let superseded = sqlx::query(
        "UPDATE jobs SET status='done', finished_at=now(), outcome=$2, error_message=NULL \
          WHERE id=$1 AND status='running'",
    )
    .bind(job_id)
    .bind(JobOutcome::NothingToDo.as_db_str())
    .execute(pool)
    .await?;
    Ok(if superseded.rows_affected() == 1 { Held::Superseded } else { Held::NotRunning })
}

/// Shared SQL behind [`fail`] and [`could_not_do`]'s `Transient` arm — kept as
/// ONE statement so the two can never drift on the backoff formula or the
/// dead-letter threshold. `failure_class` is `None` for an ordinary [`fail`]
/// call (column stays NULL) and `Some(FailureClass::Transient)` when reached
/// via [`could_not_do`] (see that function's doc comment for why `Structural`
/// never calls this — it skips the ladder via [`dead_letter`] instead).
async fn fail_ladder<'e>(
    executor: impl sqlx::PgExecutor<'e>,
    job_id: Uuid,
    error: &str,
    failure_class: Option<FailureClass>,
) -> Result<(), sqlx::Error> {
    // Error text often quotes a provider's response body or the input that
    // failed; a NUL in it would fail this very statement and leave the job
    // `running` for the reaper (`pg_text`).
    let error = coralswarm_core::pg_text::sanitize(error);
    sqlx::query(
        r#"
        UPDATE jobs
        SET    attempts      = attempts + 1,
               error_message = $2,
               failure_class = $3,
               -- Drop the dedup key when leaving 'pending' on retry: a re-queued
               -- job must run regardless, and must not collide with a newer
               -- pending pass that already holds this key (partial unique index,
               -- migration 0050). Harmless on the 'dead' branch (terminal).
               idempotency_key = NULL,
               status        = CASE
                                   WHEN attempts + 1 >= max_attempts THEN 'dead'::job_status
                                   ELSE 'pending'::job_status
                               END,
               run_after     = CASE
                                   WHEN attempts + 1 >= max_attempts THEN run_after
                                   ELSE now() + (power(2, attempts + 1) * 30 || ' seconds')::interval
                               END,
               finished_at   = CASE
                                   WHEN attempts + 1 >= max_attempts THEN now()
                                   ELSE NULL
                               END
        WHERE  id = $1
        "#,
    )
    .bind(job_id)
    .bind(error.as_ref())
    .bind(failure_class.map(FailureClass::as_db_str))
    .execute(executor)
    .await?;

    Ok(())
}

/// A worker's declaration that it CANNOT process this job — Track B step 2.
/// NEVER a completion (see [`JobOutcome`]'s doc comment: this is precisely
/// the call a worker in this state has no `complete`-shaped alternative to).
/// Routes to one of two different places depending on `class`, because the
/// two must ALERT differently:
///
/// * [`FailureClass::Transient`] stays on [`fail`]'s existing bounded
///   backoff ladder (same formula, same `max_attempts` convergence to
///   `dead`) — three transient bumps on one atom is noise a human should
///   never see.
/// * [`FailureClass::Structural`] skips the ladder and dead-letters
///   IMMEDIATELY (see [`dead_letter`]): this worker will hit the exact same
///   wall on every future attempt, so three wasted retries only cost time —
///   in the 2026-08-04 incident, the difference between an alert firing
///   within minutes and the failure staying invisible for three days.
pub async fn could_not_do(
    pool: &PgPool,
    job_id: Uuid,
    class: FailureClass,
    error: &str,
) -> Result<(), sqlx::Error> {
    match class {
        FailureClass::Transient => fail_ladder(pool, job_id, error, Some(class)).await,
        FailureClass::Structural => dead_letter(pool, job_id, error, None).await,
    }
}

/// [`could_not_do`] with [`FailureClass::Structural`], the `dead` row
/// keeping `dead_payload` instead of its payload (see
/// [`fail_with_dead_payload`]).
pub async fn dead_letter_with_payload(
    pool: &PgPool,
    job_id: Uuid,
    error: &str,
    dead_payload: serde_json::Value,
) -> Result<(), sqlx::Error> {
    dead_letter(pool, job_id, error, Some(dead_payload)).await
}

/// Immediately dead-letter a job — the [`FailureClass::Structural`] arm of
/// [`could_not_do`], factored out so its bypass of the attempts ladder is
/// visible as its own statement rather than buried in a `CASE`. Increments
/// `attempts` (for observability parity with [`fail_ladder`]'s bookkeeping —
/// "how many times did we actually try this") but does NOT consult it: this
/// always lands on `status = 'dead'` on the FIRST call, regardless of how far
/// through `max_attempts` the job was. Same `idempotency_key = NULL` on the
/// way out as [`fail_ladder`], for the same reason (harmless on a terminal
/// row; load-bearing if anything upstream ever re-derives a job from a dead
/// one).
async fn dead_letter(
    pool: &PgPool,
    job_id: Uuid,
    error: &str,
    dead_payload: Option<serde_json::Value>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        UPDATE jobs
        SET    attempts        = attempts + 1,
               error_message   = $2,
               failure_class   = 'structural',
               idempotency_key = NULL,
               payload         = COALESCE($3, payload),
               status          = 'dead'::job_status,
               finished_at     = now()
        WHERE  id = $1
        "#,
    )
    .bind(job_id)
    .bind(coralswarm_core::pg_text::sanitize(error).as_ref())
    .bind(dead_payload)
    .execute(pool)
    .await?;

    Ok(())
}

// ── Reaper ────────────────────────────────────────────────────────────────────

/// A `running` job the reaper just reset (or permanently failed) because its
/// worker went silent mid-processing. Returned so the caller
/// (`workers::reaper::run`) can log loudly with job id/kind/age — this class
/// of incident is otherwise invisible until someone notices a downstream
/// effect (e.g. atoms that never got embedded) days later.
#[derive(Debug)]
pub struct ReapedJob {
    pub id: Uuid,
    pub kind: String,
    /// The job's `started_at` from *before* this reap (i.e. when it was
    /// originally claimed) — use this to compute how long it sat orphaned.
    pub claimed_at: DateTime<Utc>,
    /// `attempts` *after* the increment this reap applied.
    pub attempts: i32,
    /// `true` if this reap moved the job to the terminal `dead` state
    /// (attempts would have exceeded `max_attempts`) rather than requeueing
    /// it to `pending`.
    pub permanently_failed: bool,
}

/// Reset (or permanently fail) `running` jobs whose worker appears to have
/// died mid-processing: `started_at` older than `stale_after_secs` ago and
/// never finished (`finished_at IS NULL`).
///
/// Mirrors [`fail`]'s dead-letter threshold (see [`reap_should_dead_letter`]
/// for the pure logic it encodes here via `CASE`): once
/// `attempts + 1 >= max_attempts` the job moves to `dead` with a clear
/// `error_message` instead of being requeued yet again, so a job that
/// reliably kills its worker (not just one that got unlucky with a Fly
/// autostop) still converges to a terminal state rather than looping
/// forever between "reaped" and "claimed, killed again".
///
/// Unlike [`fail`]'s exponential back-off, a reaped-but-still-retryable job
/// is requeued with `run_after = now()` (immediate re-claim) rather than a
/// delay — the job was never actually *attempted* to fail (no error was
/// ever recorded by its handler), so there is no reason to believe the next
/// worker to claim it will fare any worse than the first; back-off exists to
/// avoid hammering a failing dependency, and reaping is orthogonal to that.
///
/// Uses the same `SELECT … FOR UPDATE SKIP LOCKED` pattern as [`dequeue`] so
/// two reaper instances overlapping during a rolling deploy never double-reap
/// the same row.
///
/// `limit` bounds how many stale rows a single sweep locks and rewrites in
/// one transaction, mirroring [`dequeue`]'s `LIMIT $2`. Without it, a pile-up
/// of stale `running` jobs (e.g. after a bad deploy that kills many workers
/// mid-batch) would get reaped in one unbounded statement/transaction; with
/// it, a spike drains across successive sweeps (`workers::reaper` runs one
/// every `REAP_INTERVAL`, currently 5 minutes) instead of blocking the hot
/// path on an arbitrarily large write.
///
/// `redaction`, when given, is applied to `payload` in this same statement
/// on the `dead` branch for the kinds it names ([`DeadPayloadRedaction`]):
/// a job that dies with its worker never runs its own completion path, and
/// a second statement afterwards could be lost to a crash in between. The
/// expression is `jsonb_set(... jsonb_set(payload, $p1, $r, false) ..., $pn,
/// $r, false)`, one nesting per path, every path and the replacement bound
/// as parameters; a job that goes back to `pending` keeps its payload.
pub async fn reap(
    pool: &PgPool,
    stale_after_secs: i64,
    limit: i64,
    redaction: Option<&DeadPayloadRedaction>,
) -> Result<Vec<ReapedJob>, sqlx::Error> {
    // $1 stale_after_secs, $2 limit, $3 the redacted kinds, $4 the
    // replacement, $5.. one text[] per path.
    let (dead_payload, redacted_kinds, replacement, paths) = match redaction {
        Some(r) if !r.paths.is_empty() => {
            let expr = r.paths.iter().enumerate().fold("j.payload".to_string(), |inner, (i, _)| {
                format!("jsonb_set({inner}, ${}::text[], to_jsonb($4::text), false)", i + 5)
            });
            (
                format!("CASE WHEN stale.attempts + 1 >= j.max_attempts AND j.kind = ANY($3) THEN {expr} ELSE j.payload END"),
                r.kinds.to_vec(),
                r.replacement,
                r.paths.iter().map(|p| p.to_vec()).collect::<Vec<Vec<&str>>>(),
            )
        }
        _ => ("j.payload".to_string(), Vec::new(), "", Vec::new()),
    };
    let sql = format!(
        r#"
        WITH stale AS (
            SELECT id, kind, started_at, attempts
            FROM   jobs
            WHERE  status      = 'running'
              AND  finished_at IS NULL
              AND  started_at  < now() - ($1 * INTERVAL '1 second')
            ORDER BY started_at
            LIMIT  $2
            FOR UPDATE SKIP LOCKED
        )
        UPDATE jobs j
        SET    attempts        = stale.attempts + 1,
               -- Drop the dedup key on requeue, same reasoning as `fail`:
               -- a re-queued job must run regardless of a newer pending pass
               -- holding this idempotency key. Harmless on the 'dead'
               -- branch (terminal).
               idempotency_key = NULL,
               -- The caller's redaction, on the 'dead' branch only.
               payload         = {dead_payload},
               status          = CASE
                                     WHEN stale.attempts + 1 >= j.max_attempts THEN 'dead'::job_status
                                     ELSE 'pending'::job_status
                                 END,
               run_after       = CASE
                                     WHEN stale.attempts + 1 >= j.max_attempts THEN j.run_after
                                     ELSE now()
                                 END,
               started_at      = CASE
                                     WHEN stale.attempts + 1 >= j.max_attempts THEN j.started_at
                                     ELSE NULL
                                 END,
               finished_at     = CASE
                                     WHEN stale.attempts + 1 >= j.max_attempts THEN now()
                                     ELSE NULL
                                 END,
               error_message   = CASE
                                     WHEN stale.attempts + 1 >= j.max_attempts
                                         THEN 'reaped: worker died mid-job ' || (stale.attempts + 1) || ' times'
                                     ELSE 'reaped: worker died mid-job (retrying, attempt ' || (stale.attempts + 1) || ')'
                                 END
        FROM   stale
        WHERE  j.id = stale.id
        RETURNING j.id,
                  stale.kind,
                  stale.started_at              AS claimed_at,
                  (stale.attempts + 1)           AS new_attempts,
                  (j.status = 'dead'::job_status) AS permanently_failed
        "#
    );
    let mut query = sqlx::query(&sql)
        .bind(stale_after_secs) // $1
        .bind(limit); // $2
    if !paths.is_empty() {
        // Bound only when the statement references them: Postgres refuses a
        // bind that supplies more parameters than the statement uses.
        query = query.bind(redacted_kinds).bind(replacement); // $3, $4
        for path in &paths {
            query = query.bind(path); // $5..
        }
    }
    let rows = query.fetch_all(pool).await?;

    use sqlx::Row;
    let reaped = rows
        .into_iter()
        .map(|r| ReapedJob {
            id: r.get("id"),
            kind: r.get("kind"),
            claimed_at: r.get("claimed_at"),
            attempts: r.get("new_attempts"),
            permanently_failed: r.get("permanently_failed"),
        })
        .collect();

    Ok(reaped)
}

// ── Pure-logic helpers (used by unit tests) ───────────────────────────────────

/// Compute the back-off delay in seconds for a given attempt count.
///
/// `attempt` is the value **before** incrementing (i.e. the current
/// `jobs.attempts` column value).  Returns `None` when the job should be
/// declared dead (`attempt + 1 >= max_attempts`).
pub fn backoff_secs(attempt: i32, max_attempts: i32) -> Option<i64> {
    let next_attempt = attempt + 1;
    if next_attempt >= max_attempts {
        None // dead
    } else {
        let exp = next_attempt.min(30) as u32;
        Some(2_i64.saturating_pow(exp) * 30)
    }
}

/// Decide whether a stale job the reaper is about to reset should instead be
/// permanently failed (moved to `dead`). Factored out of the `CASE` in
/// [`reap`]'s SQL so the threshold — identical to [`fail`]'s dead-letter
/// threshold — is unit-testable without a database, mirroring
/// [`backoff_secs`] above.
///
/// `attempts` is the job's CURRENT `attempts` column value (before this
/// reap's increment).
pub fn reap_should_dead_letter(attempts: i32, max_attempts: i32) -> bool {
    attempts + 1 >= max_attempts
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    // ── Managed holds (COR-114) ───────────────────────────────────────────────

    #[test]
    fn a_clearable_hold_backs_off_to_its_cap_and_never_dead_letters() {
        let delays: Vec<_> = (0..8).map(|n| hold_retry(HoldClass::Clears, n)).collect();
        assert_eq!(delays, [900, 1800, 3600, 7200, 14400, 21600, 21600, 21600].map(HoldRetry::After).to_vec());
        assert_eq!(hold_retry(HoldClass::Clears, i32::MAX), HoldRetry::After(HOLD_CLEARS_CAP_SECS));
    }

    #[test]
    fn a_hold_that_cannot_clear_dead_letters_on_the_fourth_in_a_row() {
        assert_eq!(hold_retry(HoldClass::Never, 0), HoldRetry::After(3600));
        assert_eq!(hold_retry(HoldClass::Never, 1), HoldRetry::After(4 * 3600));
        assert_eq!(hold_retry(HoldClass::Never, 2), HoldRetry::After(16 * 3600));
        assert_eq!(hold_retry(HoldClass::Never, 3), HoldRetry::DeadLetter);
        assert_eq!(hold_retry(HoldClass::Never, 40), HoldRetry::DeadLetter);
    }

    #[test]
    fn no_hold_retries_on_a_fixed_interval() {
        for class in [HoldClass::Clears, HoldClass::Never] {
            assert_ne!(hold_retry(class, 0), hold_retry(class, 1), "{class:?}");
        }
    }

    #[test]
    fn hold_codes_are_classed_by_whether_they_can_clear() {
        use coralswarm_core::send_gate::{HoldCode, RefuseCode};
        for code in [HoldCode::RequesterAccessLost, HoldCode::SourceRemovedOrAccessLost, HoldCode::PrincipalUnavailable] {
            assert_eq!(hold_class(code.code()), HoldClass::Never, "{code:?}");
        }
        for code in [
            HoldCode::ProcessingDisabled, HoldCode::CreditsExhausted, HoldCode::LegacyManagedLiabilitiesPending,
            HoldCode::DailyBudgetExhausted, HoldCode::Fenced, HoldCode::CloudProcessingDisabled,
            HoldCode::ManagedRunPaused, HoldCode::LifecycleContractNotReady,
        ] {
            assert_eq!(hold_class(code.code()), HoldClass::Clears, "{code:?}");
        }
        for code in [RefuseCode::UnpricedModel, RefuseCode::CallCostCap, RefuseCode::VersionCostCap, RefuseCode::Ungated] {
            assert_eq!(hold_class(code.code()), HoldClass::Never, "{code:?}");
        }
        for reason in [MANAGED_UNAVAILABLE, "inputs_changed", "switched_off"] {
            assert_eq!(hold_class(reason), HoldClass::Clears, "{reason}");
        }
    }

    // ── JobKind serialisation / round-trip ────────────────────────────────────

    #[test]
    fn embed_atoms_roundtrip() {
        let ids = vec![Uuid::new_v4(), Uuid::new_v4()];
        let kind = JobKind::EmbedAtoms {
            atom_ids: ids.clone(),
        };

        let json = serde_json::to_value(&kind).unwrap();

        // The internally-tagged format must produce "kind" and "payload" keys.
        assert_eq!(json["kind"], "EmbedAtoms");
        assert!(json["payload"]["atom_ids"].is_array());

        // Full round-trip back to enum.
        let decoded: JobKind = serde_json::from_value(json).unwrap();
        if let JobKind::EmbedAtoms { atom_ids } = decoded {
            assert_eq!(atom_ids, ids);
        } else {
            panic!("unexpected variant");
        }
    }

    #[test]
    fn all_variants_serialise() {
        let graph_id = Uuid::new_v4();
        let user_id = Uuid::new_v4();
        let session_id = Uuid::new_v4();
        let atom_id = Uuid::new_v4();

        let variants = [
            JobKind::EmbedAtoms {
                atom_ids: vec![atom_id],
            },
            JobKind::RouteCopresence { session_id },
            JobKind::SendOutreach {
                user_id,
                template: "welcome".into(),
            },
            JobKind::ComputeDrift { graph_id },
            JobKind::SlackNameBackfill {
                graph_id: Some(graph_id),
                workspace_id: Some("T12345678".into()),
                author_id: Some("U57C3A9EF09".into()),
                after_atom_id: None,
            },
        ];

        for v in &variants {
            let json = serde_json::to_value(v).unwrap();
            assert!(json["kind"].is_string(), "kind field missing for {v:?}");
        }
    }

    // ── Idempotency-key reasoning (pure logic) ────────────────────────────────

    #[test]
    fn idempotency_key_serialisation() {
        // Ensure that two jobs with the same key would produce the same key
        // string, making it straightforward to detect duplicates at the DB layer.
        let key1 = format!("embed:{}", Uuid::nil());
        let key2 = format!("embed:{}", Uuid::nil());
        assert_eq!(key1, key2, "idempotency keys must be deterministic");
    }

    // ── Backoff calculation ───────────────────────────────────────────────────

    #[test]
    fn backoff_first_retry() {
        // attempt=0 means the job has never been tried; next attempt is 1.
        // expected delay: 2^1 * 30 = 60 seconds
        assert_eq!(backoff_secs(0, 3), Some(60));
    }

    #[test]
    fn backoff_second_retry() {
        // attempt=1 → next=2; 2^2 * 30 = 120 seconds
        assert_eq!(backoff_secs(1, 3), Some(120));
    }

    #[test]
    fn backoff_dead_on_max_attempts() {
        // attempt=2, max_attempts=3 → next=3 == max_attempts → dead
        assert_eq!(backoff_secs(2, 3), None);
    }

    #[test]
    fn backoff_dead_when_exceeded() {
        // attempt already at max should also be dead
        assert_eq!(backoff_secs(3, 3), None);
    }

    #[test]
    fn backoff_grows_exponentially() {
        // delays: 60, 120, 240, 480 …
        let delays: Vec<i64> = (0..4).filter_map(|a| backoff_secs(a, 10)).collect();
        assert_eq!(delays, vec![60, 120, 240, 480]);
    }

    // ── Reaper: dead-letter threshold (pure logic) ────────────────────────────

    #[test]
    fn reap_requeues_when_under_max_attempts() {
        // attempts=0 → next=1, well under max_attempts=3 → requeue.
        assert!(!reap_should_dead_letter(0, 3));
    }

    #[test]
    fn reap_dead_letters_at_max_attempts() {
        // attempts=2 → next=3 == max_attempts=3 → dead.
        assert!(reap_should_dead_letter(2, 3));
    }

    #[test]
    fn reap_dead_letters_when_exceeded() {
        // attempts already at max should also dead-letter.
        assert!(reap_should_dead_letter(3, 3));
    }

    #[test]
    fn reap_threshold_matches_fail_threshold() {
        // The reaper and `fail()` must agree on exactly where a job goes
        // terminal — otherwise a job could be requeued forever by one path
        // while the other considers it dead-letter eligible. Cross-check
        // reap_should_dead_letter's boundary against backoff_secs' (the same
        // `attempts + 1 >= max_attempts` condition, expressed the other way:
        // backoff_secs returns None exactly when the job is dead).
        for attempts in 0..6 {
            for max_attempts in 1..6 {
                assert_eq!(
                    reap_should_dead_letter(attempts, max_attempts),
                    backoff_secs(attempts, max_attempts).is_none(),
                    "disagreement at attempts={attempts}, max_attempts={max_attempts}"
                );
            }
        }
    }

    // ── Live Postgres: enqueue_tx is atomic with sibling writes ───────────────
    //
    // Proves the durability fix behind `routes::slack::persist_identity_and_
    // enqueue_backfill`: `enqueue_tx` participates in the CALLER's transaction,
    // so rolling that transaction back leaves NO job row (not a half-applied
    // state where a sibling write committed but the enqueue didn't survive),
    // and committing it leaves exactly one. Gated on RELINFER_TEST_DATABASE_URL.
    #[tokio::test]
    async fn enqueue_tx_rolls_back_and_commits_atomically_against_real_postgres() {
        let url = match std::env::var("RELINFER_TEST_DATABASE_URL") {
            Ok(u) => u,
            Err(_) => {
                eprintln!(
                    "skipping: set RELINFER_TEST_DATABASE_URL to run the live enqueue_tx test"
                );
                return;
            }
        };
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(4)
            .connect(&url)
            .await
            .expect("connect to test Postgres");

        let graph_id = Uuid::new_v4();
        let account_id = Uuid::new_v4();
        let kind = JobKind::ComputeDrift { graph_id };
        let key = format!("enqueue_tx_test:{graph_id}");

        // 1. Enqueue inside a transaction, then ROLL BACK — mirrors an enqueue
        //    that logically "failed" alongside a sibling write in the same tx.
        {
            let mut tx = pool.begin().await.expect("begin tx");
            let enqueued = enqueue_tx(&mut tx, &kind, Some(account_id), Some(&key), 0)
                .await
                .expect("enqueue_tx inside tx");
            assert!(enqueued.is_some(), "enqueue_tx returns an id before commit");
            tx.rollback().await.expect("rollback tx");
        }

        let count_after_rollback: i64 =
            sqlx::query_scalar("SELECT count(*) FROM jobs WHERE idempotency_key = $1")
                .bind(&key)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(
            count_after_rollback, 0,
            "a rolled-back enqueue_tx must leave no job row behind"
        );

        // 2. Same key, this time COMMITTED — must produce exactly one row.
        let job_id = {
            let mut tx = pool.begin().await.expect("begin tx");
            let enqueued = enqueue_tx(&mut tx, &kind, Some(account_id), Some(&key), 0)
                .await
                .expect("enqueue_tx inside tx");
            tx.commit().await.expect("commit tx");
            enqueued
        };
        assert!(job_id.is_some(), "committed enqueue_tx creates a job");

        let count_after_commit: i64 =
            sqlx::query_scalar("SELECT count(*) FROM jobs WHERE idempotency_key = $1")
                .bind(&key)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(
            count_after_commit, 1,
            "a committed enqueue_tx leaves exactly one job row"
        );

        // Cleanup.
        sqlx::query("DELETE FROM jobs WHERE idempotency_key = $1")
            .bind(&key)
            .execute(&pool)
            .await
            .ok();
        eprintln!(
            "PG enqueue_tx e2e passed: rollback leaves nothing, commit leaves exactly one row"
        );
    }

    // ── Live Postgres: pending-scoped dedup re-opens (migration 0050) ─────────
    //
    // Proves the debounce no longer fires once-per-graph-forever: a constant-key
    // pass can be re-enqueued as soon as the prior one leaves 'pending'.
    // Gated on RELINFER_TEST_DATABASE_URL (a Postgres with all migrations applied,
    // through 0050); a normal `cargo test` with no DB skips it.
    #[tokio::test]
    async fn debounce_dedup_reopens_after_claim_against_real_postgres() {
        let url = match std::env::var("RELINFER_TEST_DATABASE_URL") {
            Ok(u) => u,
            Err(_) => {
                eprintln!("skipping: set RELINFER_TEST_DATABASE_URL to run the live debounce test");
                return;
            }
        };
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(4)
            .connect(&url)
            .await
            .expect("connect to test Postgres");

        let g = Uuid::new_v4();
        // A synthetic account_id: enqueueing WITH one keeps `dequeue`'s
        // pre-W.1 legacy backfill (which would try — and fail — to resolve
        // this random, row-less graph_id) entirely out of this test's way.
        let account = Some(Uuid::new_v4());
        let key = format!("infer:{g}"); // a CONSTANT per-graph debounce key
        let kind = JobKind::InferRelationships { graph_id: g };

        // 1. First enqueue creates the job.
        let j1 = enqueue(&pool, &kind, account, Some(&key), 0).await.unwrap();
        assert!(j1.is_some(), "first enqueue creates a job");

        // 2. While it is still pending, a second enqueue is deduped (debounce holds).
        let dup = enqueue(&pool, &kind, account, Some(&key), 0).await.unwrap();
        assert!(
            dup.is_none(),
            "second enqueue while pending must be deduped"
        );

        // 3. Claim it (pending -> running) — frees the partial-index slot.
        let claimed = dequeue(&pool, &["InferRelationships"], 50).await.unwrap();
        let claimed_j1 = claimed.iter().find(|jr| Some(jr.id) == j1);
        assert!(claimed_j1.is_some(), "the job must be claimable");
        assert_eq!(
            claimed_j1.unwrap().account_id,
            account,
            "enqueue -> dequeue must round-trip the account_id"
        );

        // 4. THE FIX: a fresh enqueue now succeeds (old behaviour: blocked forever).
        let j2 = enqueue(&pool, &kind, account, Some(&key), 0).await.unwrap();
        assert!(
            j2.is_some(),
            "enqueue must succeed again once the prior pass left 'pending'"
        );
        assert_ne!(j2, j1, "a distinct new job row");

        // 5. Completing the first does NOT unblock dedup against the still-pending j2.
        complete(&pool, j1.unwrap(), JobOutcome::DidWork)
            .await
            .unwrap();
        let dup2 = enqueue(&pool, &kind, account, Some(&key), 0).await.unwrap();
        assert!(dup2.is_none(), "still deduped against the pending j2");

        // 6. fail() clears the retried job's key so it can't collide with the
        //    partial unique index, and frees the slot for a fresh pass.
        let claimed2 = dequeue(&pool, &["InferRelationships"], 50).await.unwrap();
        assert!(claimed2.iter().any(|jr| Some(jr.id) == j2), "j2 claimable");
        fail(&pool, j2.unwrap(), "boom").await.unwrap();
        let j3 = enqueue(&pool, &kind, account, Some(&key), 0).await.unwrap();
        assert!(
            j3.is_some(),
            "fail() cleared j2's key, so a fresh pass can queue"
        );

        // Cleanup.
        let ids: Vec<Uuid> = [j1, j2, j3].into_iter().flatten().collect();
        sqlx::query("DELETE FROM jobs WHERE idempotency_key = $1 OR id = ANY($2)")
            .bind(&key)
            .bind(&ids)
            .execute(&pool)
            .await
            .ok();
        eprintln!("PG debounce e2e passed: pending-scoped dedup re-opens after claim/fail");
    }

    // ── Live Postgres: reaper ───────────────────────────────────────────────────
    //
    // Gated on RELINFER_TEST_DATABASE_URL, same as the debounce test above; a
    // normal `cargo test` with no DB skips these.
    //
    // All three reaper scenarios (reset-and-retry, leave-fresh-alone,
    // dead-letter-at-max-attempts) are exercised inside ONE test function
    // sharing ONE `reap()` call, rather than three separate `#[tokio::test]`
    // functions each calling `reap()` independently. `reap()`'s stale-job CTE
    // has no `kind`/owner scoping — it sweeps every stale `running` row in
    // the shared `jobs` table — so if these scenarios lived in separate test
    // functions, cargo's default parallel test execution could let one
    // test's `reap()` call race in and reap another test's still-unclaimed
    // row before that test got a chance to call `reap()` itself, failing its
    // `hit.is_some()` assertion intermittently (see PR #171 review). Driving
    // all three rows through a single `reap()` call removes the race
    // entirely: nothing else in this crate calls `reap()` or inserts
    // `status = 'running'` rows directly (`enqueue`/`dequeue` always stamp
    // `started_at = now()`, so they can never look stale), so there is no
    // other concurrent writer to race against. Each row still gets a unique
    // id (and a `payload` marker for anyone eyeballing `SELECT * FROM jobs`
    // mid-failure) and every assertion below filters `reaped` by that row's
    // own id — never by `reaped.len()` — so this stays robust even if a
    // future test adds more stale rows to the same sweep.

    /// Shared setup: connect (or skip, printing why) so the test doesn't
    /// repeat the env-var dance inline.
    async fn reaper_test_pool() -> Option<PgPool> {
        let url = match std::env::var("RELINFER_TEST_DATABASE_URL") {
            Ok(u) => u,
            Err(_) => {
                eprintln!("skipping: set RELINFER_TEST_DATABASE_URL to run live reaper tests");
                return None;
            }
        };
        Some(
            sqlx::postgres::PgPoolOptions::new()
                .max_connections(4)
                .connect(&url)
                .await
                .expect("connect to test Postgres"),
        )
    }

    /// Insert a `running` job directly (bypassing `enqueue`/`dequeue`, which
    /// always stamp `started_at = now()`) so the test can control its
    /// apparent age. `label` is embedded in the payload purely as a
    /// human-readable discriminator (e.g. for `SELECT * FROM jobs` while
    /// debugging a failure) — assertions always key off the returned `Uuid`,
    /// never the label.
    async fn insert_running_test_job(
        pool: &PgPool,
        label: &str,
        age_secs: i64,
        attempts: i32,
        max_attempts: i32,
    ) -> Uuid {
        use sqlx::Row;
        let payload = serde_json::json!({ "test_label": label });
        let row = sqlx::query(
            r#"
            INSERT INTO jobs (kind, payload, status, attempts, max_attempts, started_at, run_after)
            VALUES ('EmbedAtoms', $1, 'running'::job_status, $2, $3,
                    now() - ($4 * INTERVAL '1 second'), now())
            RETURNING id
            "#,
        )
        .bind(payload)
        .bind(attempts)
        .bind(max_attempts)
        .bind(age_secs)
        .fetch_one(pool)
        .await
        .expect("insert running test job");

        row.get("id")
    }

    async fn cleanup_test_job(pool: &PgPool, id: Uuid) {
        sqlx::query("DELETE FROM jobs WHERE id = $1")
            .bind(id)
            .execute(pool)
            .await
            .ok();
    }

    #[tokio::test]
    async fn reaper_handles_reset_fresh_and_dead_letter_scenarios_against_real_postgres() {
        let Some(pool) = reaper_test_pool().await else {
            return;
        };

        // Scenario A: 25 minutes old, well past a 20-minute stale threshold,
        // well under max_attempts — must be reset to 'pending' and retried.
        let reset_id = insert_running_test_job(&pool, "reset", 25 * 60, 0, 5).await;

        // Scenario B: 1 minute old — well within a 20-minute stale
        // threshold, i.e. a worker could plausibly still be actively
        // processing it — must be left completely untouched.
        let fresh_id = insert_running_test_job(&pool, "fresh", 60, 0, 5).await;

        // Scenario C: attempts=2, max_attempts=3 → next attempt (3) would
        // meet max_attempts, so this reap must dead-letter instead of
        // requeueing.
        let dead_letter_id = insert_running_test_job(&pool, "dead_letter", 25 * 60, 2, 3).await;

        // One sweep, all three rows in scope; a limit of 10 comfortably
        // covers this test's 3 rows.
        let reaped = reap(&pool, 20 * 60, 10, None).await.unwrap();

        // ── Scenario A assertions ──────────────────────────────────────────
        let hit = reaped.iter().find(|j| j.id == reset_id);
        assert!(hit.is_some(), "stale job must be reaped");
        let hit = hit.unwrap();
        assert_eq!(hit.attempts, 1, "attempts must increment from 0 to 1");
        assert!(
            !hit.permanently_failed,
            "1 attempt is well under max_attempts=5"
        );

        use sqlx::Row;
        let row = sqlx::query(
            "SELECT status::text, started_at, run_after, attempts, error_message FROM jobs WHERE id = $1",
        )
        .bind(reset_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            row.get::<String, _>("status"),
            "pending",
            "reset back to pending"
        );
        assert!(
            row.get::<Option<DateTime<Utc>>, _>("started_at").is_none(),
            "started_at cleared"
        );
        assert!(
            row.get::<DateTime<Utc>, _>("run_after") <= Utc::now(),
            "immediately re-claimable"
        );
        assert_eq!(row.get::<i32, _>("attempts"), 1);
        let msg: String = row.get("error_message");
        assert!(
            msg.contains("reaped"),
            "error_message must explain the reap: {msg}"
        );

        // ── Scenario B assertions ──────────────────────────────────────────
        assert!(
            reaped.iter().all(|j| j.id != fresh_id),
            "fresh running job must not be reaped"
        );
        let row = sqlx::query("SELECT status::text, attempts FROM jobs WHERE id = $1")
            .bind(fresh_id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(
            row.get::<String, _>("status"),
            "running",
            "still running, untouched"
        );
        assert_eq!(row.get::<i32, _>("attempts"), 0, "attempts unchanged");

        // ── Scenario C assertions ──────────────────────────────────────────
        let hit = reaped.iter().find(|j| j.id == dead_letter_id);
        assert!(
            hit.is_some(),
            "stale job at the max_attempts boundary must still be reaped"
        );
        let hit = hit.unwrap();
        assert_eq!(hit.attempts, 3);
        assert!(
            hit.permanently_failed,
            "3rd attempt hits max_attempts=3 — must dead-letter"
        );

        let row =
            sqlx::query("SELECT status::text, finished_at, error_message FROM jobs WHERE id = $1")
                .bind(dead_letter_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(
            row.get::<String, _>("status"),
            "dead",
            "permanently failed, not requeued"
        );
        assert!(
            row.get::<Option<DateTime<Utc>>, _>("finished_at").is_some(),
            "terminal jobs get finished_at"
        );
        let msg: String = row.get("error_message");
        assert!(
            msg.contains("reaped: worker died mid-job 3 times"),
            "error_message must record the repeated-death count: {msg}"
        );

        cleanup_test_job(&pool, reset_id).await;
        cleanup_test_job(&pool, fresh_id).await;
        cleanup_test_job(&pool, dead_letter_id).await;
        eprintln!(
            "PG reaper e2e passed: reset+retry, leave-fresh-alone, and dead-letter-at-max-attempts scenarios"
        );
    }

    // ── Live Postgres: Track B typed outcomes ──────────────────────────────────
    //
    // Gated on RELINFER_TEST_DATABASE_URL, same convention as every other live
    // test in this module. These are the tests the 2026-08-04 incident's own
    // fix (this file) shipped WITHOUT — added after review flagged that a
    // diff this size, touching brand-new terminal-state SQL, had zero new
    // tests. A silently-skipping test here would be this exact defect class
    // in test form, so every test below prints an `eprintln!` marker on
    // success and this is verified from the actual run output, not assumed.

    /// Connect to `RELINFER_TEST_DATABASE_URL`, or print why and return `None`
    /// — factored out of the four tests below (previously each inlined this;
    /// duplicating it four more times was the wrong tradeoff against a
    /// shared helper any one of them can be read next to without following a
    /// jump).
    async fn track_b_test_pool(test_name: &str) -> Option<PgPool> {
        let url = match std::env::var("RELINFER_TEST_DATABASE_URL") {
            Ok(u) => u,
            Err(_) => {
                eprintln!("skipping {test_name}: set RELINFER_TEST_DATABASE_URL to run it");
                return None;
            }
        };
        Some(
            sqlx::postgres::PgPoolOptions::new()
                .max_connections(4)
                .connect(&url)
                .await
                .expect("connect to test Postgres"),
        )
    }

    #[tokio::test]
    async fn complete_writes_the_declared_outcome_against_real_postgres() {
        let Some(pool) =
            track_b_test_pool("complete_writes_the_declared_outcome_against_real_postgres").await
        else {
            return;
        };

        let did_work_id = insert_running_test_job(&pool, "did_work", 0, 0, 3).await;
        let nothing_id = insert_running_test_job(&pool, "nothing_to_do", 0, 0, 3).await;

        complete(&pool, did_work_id, JobOutcome::DidWork)
            .await
            .expect("complete DidWork");
        complete(&pool, nothing_id, JobOutcome::NothingToDo)
            .await
            .expect("complete NothingToDo");

        use sqlx::Row;
        let row = sqlx::query("SELECT status::text, outcome, finished_at FROM jobs WHERE id = $1")
            .bind(did_work_id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(row.get::<String, _>("status"), "done");
        assert_eq!(
            row.get::<Option<String>, _>("outcome").as_deref(),
            Some("did_work"),
            "JobOutcome::DidWork must persist as the DB literal 'did_work' — this is exactly the \
             column the parallel outcome-distribution alarm (PR #571) reads; a wrong literal here \
             means that alarm silently never fires"
        );
        assert!(row.get::<Option<DateTime<Utc>>, _>("finished_at").is_some());

        let row = sqlx::query("SELECT status::text, outcome FROM jobs WHERE id = $1")
            .bind(nothing_id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(row.get::<String, _>("status"), "done");
        assert_eq!(
            row.get::<Option<String>, _>("outcome").as_deref(),
            Some("nothing_to_do"),
            "JobOutcome::NothingToDo must persist as the DB literal 'nothing_to_do'"
        );

        cleanup_test_job(&pool, did_work_id).await;
        cleanup_test_job(&pool, nothing_id).await;
        eprintln!(
            "PG complete/outcome e2e passed: DidWork -> 'did_work', NothingToDo -> 'nothing_to_do'"
        );
    }

    #[tokio::test]
    async fn dead_letter_bypasses_the_attempts_ladder_on_first_call_against_real_postgres() {
        let Some(pool) = track_b_test_pool(
            "dead_letter_bypasses_the_attempts_ladder_on_first_call_against_real_postgres",
        )
        .await
        else {
            return;
        };

        // attempts=0, max_attempts=3: under the ORDINARY `fail()` ladder this
        // would take THREE calls to reach 'dead'. A structural `could_not_do`
        // must reach it on the FIRST.
        let job_id = insert_running_test_job(&pool, "structural", 0, 0, 3).await;
        sqlx::query("UPDATE jobs SET idempotency_key = $1 WHERE id = $2")
            .bind(format!("track_b_dead_letter_test:{job_id}"))
            .bind(job_id)
            .execute(&pool)
            .await
            .expect("stamp idempotency_key");

        could_not_do(
            &pool,
            job_id,
            FailureClass::Structural,
            "R2-only atom: content_inline is empty and this worker has no R2 fetch path",
        )
        .await
        .expect("could_not_do(Structural)");

        use sqlx::Row;
        let row = sqlx::query(
            "SELECT status::text, attempts, failure_class, outcome, idempotency_key, finished_at \
             FROM jobs WHERE id = $1",
        )
        .bind(job_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            row.get::<String, _>("status"),
            "dead",
            "Structural must dead-letter on the FIRST call, not after exhausting max_attempts"
        );
        assert_eq!(
            row.get::<i32, _>("attempts"),
            1,
            "one attempt recorded, not looped through the ladder"
        );
        assert_eq!(
            row.get::<Option<String>, _>("failure_class").as_deref(),
            Some("structural")
        );
        assert!(
            row.get::<Option<String>, _>("outcome").is_none(),
            "a dead-lettered job is never a completion"
        );
        assert!(
            row.get::<Option<String>, _>("idempotency_key").is_none(),
            "dead_letter must NULL idempotency_key on the way out, same as fail()"
        );
        assert!(
            row.get::<Option<DateTime<Utc>>, _>("finished_at").is_some(),
            "terminal jobs get finished_at"
        );

        cleanup_test_job(&pool, job_id).await;
        eprintln!("PG dead_letter e2e passed: Structural CouldNotDo dead-letters immediately, bypassing the ladder");
    }

    #[tokio::test]
    async fn could_not_do_transient_shares_fail_ladder_thresholds_against_real_postgres() {
        let Some(pool) = track_b_test_pool(
            "could_not_do_transient_shares_fail_ladder_thresholds_against_real_postgres",
        )
        .await
        else {
            return;
        };

        use sqlx::Row;

        // ── Scenario A: well under max_attempts — both must requeue to
        // 'pending' with the SAME backoff formula. `fail_ladder`'s SQL is
        // shared between `fail()` and `could_not_do`'s Transient arm
        // specifically so these two can never drift apart; this is the test
        // that would fail if they ever did.
        let fail_id = insert_running_test_job(&pool, "fail_retry", 0, 0, 5).await;
        let transient_id = insert_running_test_job(&pool, "transient_retry", 0, 0, 5).await;
        sqlx::query("UPDATE jobs SET idempotency_key = $1 WHERE id = $2")
            .bind(format!("track_b_transient_test:{transient_id}"))
            .bind(transient_id)
            .execute(&pool)
            .await
            .expect("stamp idempotency_key");

        fail(&pool, fail_id, "boom").await.expect("fail");
        could_not_do(&pool, transient_id, FailureClass::Transient, "boom")
            .await
            .expect("could_not_do(Transient)");

        let fail_row = sqlx::query(
            "SELECT status::text, attempts, run_after, failure_class FROM jobs WHERE id = $1",
        )
        .bind(fail_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        let transient_row = sqlx::query(
            "SELECT status::text, attempts, run_after, failure_class, idempotency_key FROM jobs WHERE id = $1",
        )
        .bind(transient_id)
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(fail_row.get::<String, _>("status"), "pending");
        assert_eq!(
            transient_row.get::<String, _>("status"),
            "pending",
            "Transient CouldNotDo must stay on the ladder, not dead-letter"
        );
        assert_eq!(fail_row.get::<i32, _>("attempts"), 1);
        assert_eq!(transient_row.get::<i32, _>("attempts"), 1);
        assert!(
            fail_row.get::<Option<String>, _>("failure_class").is_none(),
            "plain fail() must leave failure_class NULL — unrelated errors are never classified"
        );
        assert_eq!(
            transient_row
                .get::<Option<String>, _>("failure_class")
                .as_deref(),
            Some("transient")
        );
        assert!(
            transient_row
                .get::<Option<String>, _>("idempotency_key")
                .is_none(),
            "the Transient path must NULL idempotency_key on retry, same as fail()"
        );
        // Same backoff formula (`backoff_secs(0, 5)` == 60s): both `run_after`
        // values must land within a few seconds of each other — they were
        // computed by the IDENTICAL SQL expression, evaluated moments apart.
        let fail_run_after: DateTime<Utc> = fail_row.get("run_after");
        let transient_run_after: DateTime<Utc> = transient_row.get("run_after");
        let drift = (fail_run_after - transient_run_after)
            .num_milliseconds()
            .abs();
        assert!(
            drift < 5_000,
            "fail() and could_not_do(Transient) must compute run_after via the identical formula \
             (observed drift {drift}ms) — a real divergence here means `fail_ladder` stopped being \
             shared"
        );

        cleanup_test_job(&pool, fail_id).await;
        cleanup_test_job(&pool, transient_id).await;

        // ── Scenario B: one call short of max_attempts — both must converge
        // to 'dead' at the IDENTICAL threshold.
        let fail_exhaust_id = insert_running_test_job(&pool, "fail_exhaust", 0, 2, 3).await;
        let transient_exhaust_id =
            insert_running_test_job(&pool, "transient_exhaust", 0, 2, 3).await;

        fail(&pool, fail_exhaust_id, "boom")
            .await
            .expect("fail (exhausting)");
        could_not_do(&pool, transient_exhaust_id, FailureClass::Transient, "boom")
            .await
            .expect("could_not_do(Transient) (exhausting)");

        let fail_status: String = sqlx::query_scalar("SELECT status::text FROM jobs WHERE id = $1")
            .bind(fail_exhaust_id)
            .fetch_one(&pool)
            .await
            .unwrap();
        let (transient_status, transient_failure_class): (String, Option<String>) =
            sqlx::query_as("SELECT status::text, failure_class FROM jobs WHERE id = $1")
                .bind(transient_exhaust_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(fail_status, "dead");
        assert_eq!(
            transient_status, "dead",
            "both must exhaust to 'dead' at the SAME max_attempts threshold"
        );
        assert_eq!(
            transient_failure_class.as_deref(),
            Some("transient"),
            "exhausting the ladder does not reclassify a Transient failure as Structural"
        );

        cleanup_test_job(&pool, fail_exhaust_id).await;
        cleanup_test_job(&pool, transient_exhaust_id).await;
        eprintln!(
            "PG could_not_do(Transient)/fail parity e2e passed: identical backoff timing, identical \
             dead-letter threshold, failure_class correctly split"
        );
    }


    /// `fail_keeping_key`: a receive job back on backoff keeps its key, so a
    /// new enqueue under it coalesces; a newer pending holder supersedes it;
    /// the exhausted ladder dead-letters it with the settled payload.
    /// Gated on RELINFER_TEST_DATABASE_URL like the other live queue tests.
    #[tokio::test]
    async fn fail_keeping_key_coalesces_pushes_and_defers_to_a_newer_holder_pg() {
        let url = match std::env::var("RELINFER_TEST_DATABASE_URL") {
            Ok(u) => u,
            Err(_) => {
                eprintln!("skipping: set RELINFER_TEST_DATABASE_URL to run the live fail_keeping_key test");
                return;
            }
        };
        let pool = sqlx::postgres::PgPoolOptions::new().max_connections(4).connect(&url).await.expect("connect");
        let key = format!("ingest_receive:test:{}", Uuid::new_v4());
        let kind = JobKind::IngestReceive(super::super::ReceivedEvent::GmailPush { email_address: "keep@example.com".into() });
        let settled = serde_json::json!({"provider": "gmail_push", "email_address": "[redacted on completion]"});
        let claim = |id: Uuid| {
            let pool = pool.clone();
            async move {
                sqlx::query("UPDATE jobs SET status = 'running', started_at = now() WHERE id = $1")
                    .bind(id)
                    .execute(&pool)
                    .await
                    .unwrap();
            }
        };
        let row = |id: Uuid| {
            let pool = pool.clone();
            async move {
                sqlx::query_as::<_, (String, Option<String>, i32, bool, serde_json::Value)>(
                    "SELECT status::text, idempotency_key, attempts, run_after > now(), payload FROM jobs WHERE id = $1",
                )
                .bind(id)
                .fetch_one(&pool)
                .await
                .unwrap()
            }
        };

        let j1 = enqueue(&pool, &kind, None, Some(&key), 0).await.unwrap().expect("first enqueue");
        claim(j1).await;
        assert_eq!(fail_keeping_key(&pool, j1, "boom", settled.clone()).await.unwrap(), FailedKeepingKey::Pending);
        let (status, kept, attempts, waits, payload) = row(j1).await;
        assert_eq!((status.as_str(), kept.as_deref(), attempts, waits), ("pending", Some(key.as_str()), 1, true));
        assert_eq!(payload["email_address"], "keep@example.com", "a retrying row keeps its payload");
        assert!(enqueue(&pool, &kind, None, Some(&key), 0).await.unwrap().is_none(), "a push during the backoff coalesces");

        // Claimed again, a push arrives meanwhile: the newer row carries it.
        claim(j1).await;
        let j2 = enqueue(&pool, &kind, None, Some(&key), 0).await.unwrap().expect("a push while running adds a row");
        assert_eq!(fail_keeping_key(&pool, j1, "boom again", settled.clone()).await.unwrap(), FailedKeepingKey::Superseded);
        let (status, kept, attempts, _, payload) = row(j1).await;
        assert_eq!((status.as_str(), kept, attempts), ("done", Some(key.clone()), 1), "superseded: done, no attempt charged");
        assert_eq!(payload, settled, "a settled row is scrubbed");
        assert_eq!(row(j2).await.0, "pending");

        // The ladder's end.
        claim(j2).await;
        sqlx::query("UPDATE jobs SET max_attempts = 1 WHERE id = $1").bind(j2).execute(&pool).await.unwrap();
        assert_eq!(fail_keeping_key(&pool, j2, "still down", settled.clone()).await.unwrap(), FailedKeepingKey::Dead);
        let (status, kept, attempts, _, payload) = row(j2).await;
        assert_eq!((status.as_str(), kept, attempts), ("dead", None, 1));
        assert_eq!(payload, settled);
        assert_eq!(fail_keeping_key(&pool, j2, "again", settled.clone()).await.unwrap(), FailedKeepingKey::NotRunning);

        sqlx::query("DELETE FROM jobs WHERE id = ANY($1)").bind(vec![j1, j2]).execute(&pool).await.unwrap();
    }

    /// A receive job the reaper dead-letters (its worker died mid-job,
    /// again and again) is redacted by the dead-lettering `UPDATE` itself
    /// (`workers::ingest_receive::REAP_REDACTION`), so the row is never
    /// `dead` with its payload, even for an instant; one the reaper puts
    /// back to `pending` keeps its payload for the retry; a dead job of
    /// another kind, and a receive payload with none of the redacted
    /// fields, are left as they were. The receive job is also enqueued with
    /// its own attempt budget.
    #[tokio::test]
    async fn the_reaper_redacts_a_receive_job_in_the_statement_that_dead_letters_it_pg() {
        let Some(pool) = reaper_test_pool().await else { return };
        let kind = JobKind::IngestReceive(super::super::ReceivedEvent::GmailPush { email_address: "reaped@example.com".into() });
        let dies = enqueue(&pool, &kind, None, None, 0).await.unwrap().unwrap();
        let retries = enqueue(&pool, &kind, None, None, 0).await.unwrap().unwrap();
        let graph = JobKind::IngestReceive(super::super::ReceivedEvent::GraphNotification {
            notification: serde_json::json!({"subscriptionId": "s", "clientState": "secret", "resourceData": {"id": "m"}}),
        });
        let graph_dies = enqueue(&pool, &graph, None, None, 0).await.unwrap().unwrap();
        let bare = JobKind::IngestReceive(super::super::ReceivedEvent::GoogleWorkspaceEvent {
            envelope: serde_json::json!({"subscription": "projects/p/subscriptions/meet-events-sub", "message": {"messageId": "m-1"}}),
        });
        let bare_dies = enqueue(&pool, &bare, None, None, 0).await.unwrap().unwrap();
        let other_kind = JobKind::GenerateMeetingSummary {
            graph_id: Uuid::new_v4(),
            source_id: "reaped@example.com".into(),
            allow_cloud_override: false,
        };
        let other_dies = enqueue(&pool, &other_kind, None, None, 0).await.unwrap().unwrap();
        let budget: i32 = sqlx::query_scalar("SELECT max_attempts FROM jobs WHERE id = $1").bind(dies).fetch_one(&pool).await.unwrap();
        assert_eq!(budget, kind.max_attempts(), "a receive job carries its own attempt budget");
        assert_eq!(budget, 9);
        let other_budget: i32 =
            sqlx::query_scalar("SELECT max_attempts FROM jobs WHERE id = $1").bind(other_dies).fetch_one(&pool).await.unwrap();
        assert_eq!(other_budget, DEFAULT_MAX_ATTEMPTS, "every other kind keeps the table default");
        for id in [dies, graph_dies, bare_dies, other_dies] {
            sqlx::query("UPDATE jobs SET status = 'running', started_at = now() - interval '3 hours', max_attempts = 1 WHERE id = $1")
                .bind(id)
                .execute(&pool)
                .await
                .unwrap();
        }
        sqlx::query("UPDATE jobs SET status = 'running', started_at = now() - interval '3 hours' WHERE id = $1")
            .bind(retries)
            .execute(&pool)
            .await
            .unwrap();

        let reaped = reap(&pool, 2 * 3600, 500, Some(&crate::workers::ingest_receive::REAP_REDACTION)).await.unwrap();
        for id in [dies, graph_dies, bare_dies, other_dies] {
            assert!(reaped.iter().any(|j| j.id == id && j.permanently_failed), "{id} dead-lettered");
        }
        assert!(reaped.iter().any(|j| j.id == retries && !j.permanently_failed));

        let row = |id: Uuid| {
            let pool = pool.clone();
            async move {
                sqlx::query_as::<_, (String, serde_json::Value)>("SELECT status::text, payload FROM jobs WHERE id = $1")
                    .bind(id)
                    .fetch_one(&pool)
                    .await
                    .unwrap()
            }
        };
        let (status, dead) = row(dies).await;
        assert_eq!(status, "dead");
        assert_eq!(dead["email_address"], "[redacted on completion]", "the dead row is redacted by the reap itself");
        assert_eq!(dead["provider"], "gmail_push", "nothing else is touched");
        let (status, dead_graph) = row(graph_dies).await;
        assert_eq!(status, "dead");
        assert_eq!(dead_graph["notification"]["clientState"], "[redacted on completion]");
        assert_eq!(dead_graph["notification"]["subscriptionId"], "s");
        let (status, dead_bare) = row(bare_dies).await;
        assert_eq!(status, "dead");
        assert!(dead_bare["envelope"]["message"].get("data").is_none(), "a missing field is not added");
        assert_eq!(dead_bare["envelope"]["message"]["messageId"], "m-1");
        let (status, other) = row(other_dies).await;
        assert_eq!(status, "dead");
        assert_eq!(other["source_id"], "reaped@example.com", "another kind's payload is not the receive rule's business");
        let (status, pending) = row(retries).await;
        assert_eq!(status, "pending");
        assert_eq!(pending["email_address"], "reaped@example.com", "the retrying row keeps its payload");

        sqlx::query("DELETE FROM jobs WHERE id = ANY($1)")
            .bind(vec![dies, retries, graph_dies, bare_dies, other_dies])
            .execute(&pool)
            .await
            .unwrap();
    }
}
