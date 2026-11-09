//! `content_tsv` regconfig repair sweep worker (PR #190 follow-up).
//!
//! `atoms.content_tsv` rows written before the write-side fix in
//! `workers::embed::update_content_tsv_and_clear_failure` may have been
//! tokenised with a per-atom STEMMING regconfig (e.g. "english") instead of
//! `'simple'`, silently breaking `Store::keyword_search`'s BM25 matches —
//! see migration 0062's header for the full bug writeup. There is no way to
//! detect after the fact which specific rows were affected (Postgres does
//! not record which regconfig produced a stored tsvector), so — exactly
//! like migration 0062's original single-`UPDATE` attempt — this worker
//! unconditionally rebuilds `content_tsv` for every atom with
//! `content_inline` set, tokenising with `'simple'`.
//!
//! # Why a worker instead of the migration doing this directly
//! A single `UPDATE atoms SET content_tsv = ... WHERE content_inline IS NOT
//! NULL` inside a migration takes row-level locks as it goes and holds every
//! one of them until the migration's single transaction commits, plus one
//! large WAL burst — lock/bloat risk on `atoms`, a live, read/write-heavy
//! production table (CodeRabbit review, PR #190 round 1, "Major"). A
//! `plpgsql` `DO`-block loop does NOT fix this: `COMMIT` is only usable
//! inside a `PROCEDURE` called at the top level, never inside a `DO` block,
//! so every row lock a loop like that takes is still held until the SAME
//! final transaction commit a single unbatched `UPDATE` would have needed —
//! see migration 0062's header for the fuller reasoning, including why a
//! stored-procedure-based real per-batch `COMMIT` was rejected (no
//! precedent anywhere else in this migration history, more machinery than
//! reusing the sweep pattern this module implements).
//!
//! This worker runs the SAME shape as `workers::slack_name_backfill`:
//! [`ATOM_BATCH_SIZE`] atoms are read and rewritten in ONE short,
//! auto-committing statement per job execution — deliberately bounded so no
//! single statement holds row locks for longer than one small batch takes,
//! and no single statement generates more than one small batch's worth of
//! WAL.
//!
//! # Triggering
//! Not wired to any automatic trigger — this is a one-time repair, not an
//! ongoing reconciliation, exactly like `workers::slack_name_backfill`'s
//! standalone one-shot mode. Hand-insert a single unscoped sweep job after
//! deploying this PR's write-side fix:
//! ```sql
//! INSERT INTO jobs (kind, payload)
//! VALUES ('ContentTsvRegconfigRepair', '{"after_atom_id": null}'::jsonb);
//! ```
//! (Mirrors the `{"atom_ids": [...]}` / `{"graph_id": null, ...}` shapes the
//! team has hand-inserted before for `EmbedAtoms` / `SlackNameBackfill` —
//! same `jobs` table, same "paste JSON, INSERT, done" workflow.)
//!
//! # Batching, continuation, and termination
//! Every batch carries an `after_atom_id` keyset cursor (ordered by
//! `atom_id`). Whenever a batch is full (`ATOM_BATCH_SIZE` rows returned —
//! more rows likely remain beyond it), a continuation job is enqueued with
//! the cursor advanced past the LAST atom_id examined this pass, and the
//! CURRENT job completes regardless. Unlike `workers::slack_name_backfill`,
//! there is no "some atoms still need repair, retry with backoff" outcome:
//! a batch's `UPDATE` either rebuilds every row it selected (in which case
//! those rows are correct, full stop — this repair does not depend on an
//! external API that can partially fail per-row) or the whole statement
//! errors (handled as a hard failure below, which fails the job for a
//! bounded queue retry — safe to retry since a successful partial pass
//! isn't possible with a single-statement batch: it's all-or-nothing).
//!
//! If enqueueing the continuation itself fails, the CURRENT job is failed
//! (`queue::fail`, not `queue::complete`) instead — otherwise a full batch
//! that itself repaired correctly would complete successfully with the
//! continuation silently lost, permanently stranding every atom beyond this
//! window. Retrying the current job gives the enqueue another chance (cheap
//! to retry: this repair is idempotent, so re-running the same window is
//! wasted work, not wrong results — see below).
//!
//! Since every full batch unconditionally advances the cursor, a full sweep
//! visits every candidate atom exactly once per pass and always terminates
//! — same termination argument as `workers::slack_name_backfill`'s module
//! doc.
//!
//! # Idempotency
//! Idempotent: re-running against atoms that are already correctly
//! tokenised with `'simple'` just rebuilds the same tsvector value again
//! (wasted work, not wrong results). Safe to hand-insert more than once, or
//! to re-run after a partial sweep was interrupted (e.g. a deploy killed
//! every worker mid-sweep) — the reaper (`workers::reaper`) will requeue any
//! orphaned `running` job, and the next attempt simply re-covers whatever
//! window it was working on.
//!
//! # Consumer-plane RLS scoping (org-tenancy Phase W.2): seed → per-account
//!
//! `atoms` is on migration `0083_consumer_rls`'s policy roster, and this
//! sweep was ONE deliberate cross-account statement — post-OPS-4 (app role
//! switched to the non-`BYPASSRLS` `coralswarm_app`) it would hard-error on
//! the strict `current_setting('app.account_id')` predicate. The sweep is
//! therefore converted to **loop-per-account**, expressed as a queue-native
//! fan-out so the batching/continuation/retry semantics above survive
//! per-account:
//!
//!   * The operator-inserted account-less job (`jobs.account_id IS NULL` —
//!     the documented hand-insert above) becomes a **seed**: it enumerates
//!     account ids from `accounts` (a control/identity table deliberately
//!     OUTSIDE the RLS roster, so the read needs no scope), enqueues one
//!     `ContentTsvRegconfigRepair` child job PER ACCOUNT (same
//!     `after_atom_id` payload, `jobs.account_id` stamped — org-tenancy
//!     Phase W.1's per-job scoping input), and completes. A child-enqueue
//!     failure fails the seed for a bounded queue retry; already-enqueued
//!     children are deduped by their per-account idempotency keys.
//!   * Each **per-account child** runs the same one-statement batch, inside
//!     a `tenancy::scope::begin_account_scoped` transaction AND with an
//!     explicit `graph_id IN (SELECT id FROM graphs WHERE account_id = $n)`
//!     predicate — the app-layer WHERE clause is the primary scoping
//!     mechanism (it keeps each child's work per-account deterministic even
//!     pre-OPS-4, where the owner role's BYPASSRLS would otherwise let the
//!     first child sweep every account); RLS is the backstop, exactly per
//!     0083's design. Children carry their own keyset cursors and enqueue
//!     their own per-account continuations, so the termination argument
//!     above holds independently per account.
//!
//! Same flagged-decision rationale as Phase B.4's admin backfill: a separate
//! elevated (BYPASSRLS) ops connection for cross-account sweeps was rejected
//! to avoid widening the elevated-credential surface; looping is slower but
//! keeps every consumer-plane content query inside one RLS contract.

use std::time::Duration;

use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::jobs::queue::{self, JobRow};
use crate::jobs::JobKind;
use crate::shutdown::Shutdown;
use crate::tenancy::scope::begin_account_scoped;

// ── Tuning constants ──────────────────────────────────────────────────────────

/// How many `ContentTsvRegconfigRepair` jobs to dequeue per poll cycle.
/// There is normally at most one active unscoped sweep in flight at a time
/// (each pass enqueues its own single continuation), so this stays small —
/// unlike `workers::slack_name_backfill::JOB_BATCH_SIZE`, which is sized for
/// several independently-scoped sweeps running concurrently.
const JOB_BATCH_SIZE: usize = 1;
/// How many atoms a single job execution reads AND rewrites, in one short
/// auto-committing `UPDATE ... RETURNING` statement. This repair has no
/// external API calls (unlike `workers::slack_name_backfill`, which caps
/// its batch at 20 partly to bound Slack API burst), so the batch can be
/// larger while still keeping any single statement's row-lock duration and
/// WAL burst small relative to the whole table.
const ATOM_BATCH_SIZE: i64 = 2_000;
/// Delay before a continuation job (more likely remains) becomes eligible to
/// run — spaces out batches so the sweep doesn't monopolise the queue poller
/// or hammer the DB back-to-back.
const CONTINUATION_DELAY_SECS: i64 = 1;
/// How long to sleep between polls when the queue is empty.
const EMPTY_POLL_SLEEP: Duration = Duration::from_millis(500);
/// How long to sleep after a DB error before retrying.
const DB_ERROR_SLEEP: Duration = Duration::from_secs(5);

// ── Worker entry point ────────────────────────────────────────────────────────

/// Run the content-tsv-backfill worker loop, or until `shutdown` is
/// signalled.
///
/// Wired to the shared `crate::shutdown::Shutdown` signal exactly like
/// `workers::embed` / `workers::slack_name_backfill` (see those modules'
/// doc comments for the full contract this mirrors): on shutdown, stop
/// claiming new jobs (checked at the top of every loop iteration, before
/// `dequeue`) but let anything already claimed in the current iteration's
/// `for` loop finish — `lib.rs::run()` then gives this task a bounded grace
/// window to actually return before the process exits anyway.
pub async fn run(pool: PgPool, shutdown: Shutdown) {
    tracing::info!("content-tsv-backfill worker started");

    loop {
        if shutdown.is_triggered() {
            tracing::info!(
                "content-tsv-backfill worker: shutdown signalled, stopping (no new jobs claimed)"
            );
            return;
        }

        let jobs = match queue::dequeue(&pool, &["ContentTsvRegconfigRepair"], JOB_BATCH_SIZE).await
        {
            Ok(j) => j,
            Err(e) => {
                tracing::error!(error = %e, "content-tsv-backfill worker: dequeue failed, sleeping");
                tokio::time::sleep(DB_ERROR_SLEEP).await;
                continue;
            }
        };

        if jobs.is_empty() {
            tokio::time::sleep(EMPTY_POLL_SLEEP).await;
            continue;
        }

        for job in jobs {
            process_job(&pool, job).await;
        }
    }
}

// ── Job processing ────────────────────────────────────────────────────────────

fn parse_after_atom_id(job: &JobRow) -> Result<Option<Uuid>, String> {
    let tagged = serde_json::json!({ "kind": job.kind, "payload": job.payload });
    let kind: JobKind =
        serde_json::from_value(tagged).map_err(|e| format!("cannot deserialise JobKind from payload: {e}"))?;

    match kind {
        JobKind::ContentTsvRegconfigRepair { after_atom_id } => Ok(after_atom_id),
        other => Err(format!("expected ContentTsvRegconfigRepair, got {other:?}")),
    }
}

/// `pub` (like `workers::infer_expertise::run_expertise`) so integration
/// tests can drive one seed/child job against a real Postgres without going
/// through the worker loop.
pub async fn process_job(pool: &PgPool, job: JobRow) {
    let after_atom_id = match parse_after_atom_id(&job) {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(job_id = %job.id, error = %e, "content-tsv-backfill: malformed job payload");
            if let Err(qe) = queue::fail(pool, job.id, &e).await {
                tracing::error!(error = %qe, job_id = %job.id, "failed to mark malformed job as failed");
            }
            return;
        }
    };

    // ── Account-less job ⇒ SEED: fan out one child per account (org-tenancy
    // Phase W.2 — see the module doc's RLS section). The seed itself touches
    // no RLS-covered table, only `accounts` (control) and `jobs`.
    let Some(account_id) = job.account_id else {
        fan_out_per_account(pool, job.id, after_atom_id).await;
        return;
    };

    match run_batch(pool, account_id, after_atom_id).await {
        Ok(outcome) => {
            tracing::info!(
                job_id = %job.id,
                examined = outcome.examined,
                batch_full = outcome.batch_full,
                "content-tsv-backfill: batch complete"
            );

            // ALWAYS advance the cursor and continue the sweep when more rows
            // are likely to remain (batch was full) — see module docs.
            if outcome.batch_full {
                if let Some(cursor) = outcome.last_atom_id {
                    // Per-account continuation: propagate this child's own
                    // account stamp (org-tenancy Phase W.2 — see the module
                    // doc's RLS section). The key includes the account so two
                    // accounts' independent sweeps can never dedup each other.
                    let idempotency = format!("content_tsv_regconfig_repair:{account_id}:{cursor}");
                    if let Err(e) = queue::enqueue(
                        pool,
                        &JobKind::ContentTsvRegconfigRepair { after_atom_id: Some(cursor) },
                        Some(account_id),
                        Some(&idempotency),
                        CONTINUATION_DELAY_SECS,
                    )
                    .await
                    {
                        // A silently-dropped continuation here would
                        // permanently strand every atom beyond this window
                        // even though this batch itself repaired correctly —
                        // fail THIS job instead of completing it, so its own
                        // bounded retry gets another chance to enqueue the
                        // continuation (cheap to retry: this repair is
                        // idempotent).
                        tracing::error!(job_id = %job.id, error = %e, "content-tsv-backfill: failed to enqueue continuation job — failing this job to retry");
                        if let Err(qe) = queue::fail(pool, job.id, &e.to_string()).await {
                            tracing::error!(error = %qe, job_id = %job.id, "failed to mark job failed after continuation enqueue failure");
                        }
                        return;
                    }
                }
            }

            // `outcome.examined == 0` means this pass's candidate window came
            // back empty — a legitimate no-op for a sweep worker.
            let job_outcome = if outcome.examined > 0 {
                queue::JobOutcome::DidWork
            } else {
                queue::JobOutcome::NothingToDo
            };
            if let Err(e) = queue::complete(pool, job.id, job_outcome).await {
                tracing::error!(error = %e, job_id = %job.id, "content-tsv-backfill: failed to complete job");
            }
        }
        Err(e) => {
            tracing::error!(job_id = %job.id, error = %e, "content-tsv-backfill: batch UPDATE failed — failing job for retry");
            if let Err(qe) = queue::fail(pool, job.id, &e.to_string()).await {
                tracing::error!(error = %qe, job_id = %job.id, "failed to mark content-tsv-backfill job failed");
            }
        }
    }
}

/// SEED handling for the account-less operator-inserted job: enumerate the
/// account roster and enqueue one per-account child (same cursor payload,
/// `jobs.account_id` stamped). Any failure — enumeration or a child enqueue —
/// fails the seed job for a bounded queue retry: the per-account idempotency
/// keys make a retried seed re-enqueue only the children that didn't make it
/// (still-pending ones dedup away), so a partial fan-out can never strand an
/// account.
async fn fan_out_per_account(pool: &PgPool, job_id: Uuid, after_atom_id: Option<Uuid>) {
    let fail_seed = |msg: String| async move {
        tracing::error!(job_id = %job_id, error = %msg, "content-tsv-backfill: seed fan-out failed — failing seed job to retry");
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
        let idempotency = format!("content_tsv_regconfig_repair:{account_id}:{cursor_label}");
        if let Err(e) = queue::enqueue(
            pool,
            &JobKind::ContentTsvRegconfigRepair { after_atom_id },
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
        "content-tsv-backfill: seed fanned out one per-account child job per account"
    );
    if let Err(e) = queue::complete(pool, job_id, queue::JobOutcome::DidWork).await {
        tracing::error!(error = %e, job_id = %job_id, "content-tsv-backfill: failed to complete seed job");
    }
}

struct BatchOutcome {
    examined: usize,
    batch_full: bool,
    /// `atom_id` of the LAST (highest) row rewritten this pass, used as the
    /// next continuation's `after_atom_id` cursor. `None` when the batch was
    /// empty (sweep complete).
    last_atom_id: Option<Uuid>,
}

/// Select and rebuild the next [`ATOM_BATCH_SIZE`] candidate atoms'
/// `content_tsv` for ONE account, in ONE short statement inside a
/// `begin_account_scoped` transaction (org-tenancy Phase W.2 — see the
/// module doc's RLS section; the explicit `graph_id IN (SELECT id FROM
/// graphs WHERE account_id = $3)` predicate is the primary scoping
/// mechanism, RLS the backstop). Still one statement's worth of row locks —
/// the transaction wraps exactly that statement, no network I/O inside it.
///
/// The `batch` CTE picks the next `ATOM_BATCH_SIZE` atoms with
/// `content_inline IS NOT NULL` and `atom_id > after_atom_id` (keyset
/// pagination, ordered by `atom_id`); the outer `UPDATE ... RETURNING`
/// rewrites exactly those rows and reports back which ones it touched.
/// Unconditional (no `content_tsv IS DISTINCT FROM ...` guard) — per
/// CodeRabbit's review comment on the original migration, such a guard only
/// trims unnecessary rewrites, it does not by itself establish which rows
/// are correct (there is no way to detect after the fact which regconfig
/// produced an existing tsvector), so it would not be a safe substitute for
/// visiting every candidate row at least once.
async fn run_batch(
    pool: &PgPool,
    account_id: Uuid,
    after_atom_id: Option<Uuid>,
) -> Result<BatchOutcome, sqlx::Error> {
    let mut tx = begin_account_scoped(pool, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
    let rows = sqlx::query(
        r#"
        WITH batch AS (
            SELECT atom_id
            FROM   atoms
            WHERE  content_inline IS NOT NULL
              AND  ($1::uuid IS NULL OR atom_id > $1)
              AND  graph_id IN (SELECT id FROM graphs WHERE account_id = $3)
            ORDER BY atom_id
            LIMIT  $2
        )
        UPDATE atoms a
        SET    content_tsv = to_tsvector('simple', a.content_inline)
        FROM   batch b
        WHERE  a.atom_id = b.atom_id
        RETURNING a.atom_id
        "#,
    )
    .bind(after_atom_id)
    .bind(ATOM_BATCH_SIZE)
    .bind(account_id)
    .fetch_all(&mut *tx)
    .await?;
    tx.commit().await?;

    let examined = rows.len();
    let batch_full = examined as i64 == ATOM_BATCH_SIZE;
    let last_atom_id = rows.iter().map(|r| r.get::<Uuid, _>("atom_id")).max();

    Ok(BatchOutcome { examined, batch_full, last_atom_id })
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn content_tsv_regconfig_repair_roundtrip() {
        let cursor = Some(Uuid::new_v4());
        let kind = JobKind::ContentTsvRegconfigRepair { after_atom_id: cursor };
        let tagged = serde_json::to_value(&kind).unwrap();

        let job = JobRow {
            id: Uuid::new_v4(),
            kind: tagged["kind"].as_str().unwrap().to_owned(),
            payload: tagged["payload"].clone(),
            attempts: 0,
            account_id: None,
        };

        let parsed = parse_after_atom_id(&job).unwrap();
        assert_eq!(parsed, cursor);
    }

    #[test]
    fn content_tsv_regconfig_repair_none_cursor_roundtrip() {
        let kind = JobKind::ContentTsvRegconfigRepair { after_atom_id: None };
        let tagged = serde_json::to_value(&kind).unwrap();

        let job = JobRow {
            id: Uuid::new_v4(),
            kind: tagged["kind"].as_str().unwrap().to_owned(),
            payload: tagged["payload"].clone(),
            attempts: 0,
            account_id: None,
        };

        let parsed = parse_after_atom_id(&job).unwrap();
        assert_eq!(parsed, None);
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

    /// Live-Postgres regression test: seeds an atom whose `content_tsv` was
    /// built with a stemming regconfig (reproducing the historical bug),
    /// runs one `run_batch` pass, and asserts the row comes out tokenised
    /// with `'simple'` and searchable via `Store::keyword_search`'s
    /// unstemmed query — exactly the same assertion shape as
    /// `workers::embed`'s live regconfig regression test, but exercising
    /// this worker's batched sweep path instead of the embed pipeline's
    /// inline update.
    ///
    /// Gated on `RELINFER_TEST_DATABASE_URL`, same convention as
    /// `workers::slack_name_backfill`'s and `workers::embed`'s live tests.
    /// Connect is bounded by `tokio::time::timeout` (PR #190 CodeRabbit
    /// review: an unreachable URL must fail fast, not hang on the OS TCP
    /// timeout) and cleanup uses `.expect(...)` rather than `.ok()` so a
    /// cleanup failure is visible instead of silently swallowed.
    #[tokio::test]
    async fn batch_repairs_stemmed_content_tsv_against_real_postgres() {
        use crate::retrieval::RetrievalFilter;
        use crate::store::{PostgresStore, Store};

        let url = match std::env::var("RELINFER_TEST_DATABASE_URL") {
            Ok(u) => u,
            Err(_) => {
                eprintln!(
                    "skipping: set RELINFER_TEST_DATABASE_URL to run the live content-tsv-backfill regression test"
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
            .create_graph(account_id, "content-tsv-backfill-e2e", "personal")
            .await
            .expect("create graph");
        let graph_id = graph.id;

        // ── Insert an atom whose content_tsv is seeded with the OLD buggy
        // stemming regconfig ("english"), reproducing a pre-fix row. ────────
        let atom_id: Uuid = sqlx::query_scalar(
            "INSERT INTO atoms (graph_id, source_type, source_id, content_blob_key, \
                                 content_inline, timestamp, fts_language, content_tsv) \
             VALUES ($1, $2, $3, $4, $5, now(), $6, to_tsvector('english', $5)) \
             RETURNING atom_id",
        )
        .bind(graph_id)
        .bind("test_source")
        .bind(format!("content-tsv-backfill-e2e-{}", Uuid::new_v4()))
        .bind("test-blob-key")
        .bind("we are running late")
        .bind("english")
        .fetch_one(&pool)
        .await
        .expect("insert atom with stemmed content_tsv");

        // ── Before repair: the stemmed tsvector must NOT match the literal
        // surface form "running" via keyword_search's unstemmed query. ──────
        let hits_before = store
            .keyword_search(graph_id, "running", 10, &RetrievalFilter::default())
            .await
            .expect("keyword_search before repair");
        assert!(
            !hits_before.iter().any(|(id, _)| *id == atom_id),
            "test setup invariant: the seeded row must reproduce the bug (stemmed content_tsv \
             must NOT match an unstemmed \"running\" query) before the repair runs"
        );

        // ── Run one batch of the sweep (per-account child shape — org-tenancy
        // Phase W.2: the batch is account-scoped now) ────────────────────────
        let outcome = run_batch(&pool, account_id, None).await.expect("run_batch");
        assert!(
            outcome.examined >= 1,
            "batch must have examined at least the seeded atom"
        );

        // ── After repair: "running" must now be found ───────────────────────
        let hits_after = store
            .keyword_search(graph_id, "running", 10, &RetrievalFilter::default())
            .await
            .expect("keyword_search after repair");
        assert!(
            hits_after.iter().any(|(id, _)| *id == atom_id),
            "keyword_search for \"running\" must find the atom after run_batch repairs its \
             content_tsv to use the 'simple' regconfig"
        );

        // ── Cleanup (cascades through graph → atoms). Uses `.expect(...)`,
        // not `.ok()`, so a cleanup failure fails the test loudly instead of
        // being silently swallowed (PR #190 CodeRabbit review). ─────────────
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
            "content-tsv-backfill regression test passed: run_batch repaired a stemmed row and \
             \"running\" became searchable"
        );
    }
}
