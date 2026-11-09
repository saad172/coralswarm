-- Migration: 0134_job_outcomes
-- Description: Track B step 1 — the schema half of typed job-completion
--              outcomes (`jobs::queue`, Rust side landed in the same PR).
--
--              The defect this exists to make impossible to express again:
--              on 2026-08-04 the `ExtractRelationships` worker dequeued an
--              atom whose content lived only in R2 (`content_blob_key` set,
--              `content_inline` NULL) — a shape this worker has no code path
--              to read. It treated "I cannot read my own input" as "there is
--              nothing to do", called `queue::complete()`, and the row landed
--              `status='done'`. Every dashboard reading `jobs.status` stayed
--              green while all graph-relationship inference silently stopped
--              for three days — nothing distinguished that `done` row from
--              one where the worker actually did its job.
--
--              `outcome` and `failure_class` give a completed/failed `jobs`
--              row a machine-readable answer to "what actually happened
--              here", so a monitor can alert on the shape that mattered —
--              `status='dead' AND failure_class='structural'` — instead of
--              only ever being able to ask "how many rows are `done`", which
--              is exactly the signal that stayed silent for three days.
--
--   * `outcome` — written ONLY by `queue::complete`/`complete_with_payload`
--     (i.e. only on `status='done'`): `'did_work'` when the worker actually
--     performed the job, `'nothing_to_do'` when it correctly determined
--     there was legitimately nothing to do (already processed, empty input
--     by design). NULL for every other status (pending/running/failed/dead)
--     — a job that never reached `done` has no completion outcome to record.
--     There is deliberately NO THIRD VALUE here for "the worker couldn't
--     process its input" — see `jobs::queue::JobOutcome`'s doc comment for
--     why that is a type-level impossibility on the Rust side, not a value
--     this column's CHECK needs to reject at runtime.
--   * `failure_class` — written ONLY by the `CouldNotDo` path (`queue::
--     could_not_do`), never by a job's ordinary `fail()` for an unrelated
--     error (malformed payload, a plain DB error, an unscoped job — those
--     leave this NULL, exactly as before this migration): `'transient'` for
--     a retryable environmental failure (R2 timeout, pool exhaustion, a
--     5xx) that stays on the existing `fail()` backoff ladder, `'structural'`
--     for "this worker cannot process its own input by design" — which
--     dead-letters immediately, bypassing the attempts ladder entirely (see
--     `queue::could_not_do`'s doc comment). NULL for `pending`/`running`, for
--     `done`, and for `failed`/`dead` rows that failed for a reason that was
--     never classified through the `CouldNotDo` path.
--
-- Depends on: 0004_jobs (creates `jobs`).
-- Down:
--   ALTER TABLE jobs DROP CONSTRAINT IF EXISTS jobs_outcome_check;
--   ALTER TABLE jobs DROP CONSTRAINT IF EXISTS jobs_failure_class_check;
--   ALTER TABLE jobs DROP COLUMN IF EXISTS outcome;
--   ALTER TABLE jobs DROP COLUMN IF EXISTS failure_class;

SET LOCAL lock_timeout = '3s';

ALTER TABLE jobs ADD COLUMN IF NOT EXISTS outcome TEXT;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS failure_class TEXT;

ALTER TABLE jobs ADD CONSTRAINT jobs_outcome_check
    CHECK (outcome IS NULL OR outcome IN ('did_work', 'nothing_to_do'));

ALTER TABLE jobs ADD CONSTRAINT jobs_failure_class_check
    CHECK (failure_class IS NULL OR failure_class IN ('transient', 'structural'));
