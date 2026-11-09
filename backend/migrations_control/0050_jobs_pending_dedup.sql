-- Migration: 0050_jobs_pending_dedup
-- Description: Scope the jobs idempotency-key dedup to PENDING jobs only.
--
--   The queue debounces a burst of per-atom enqueues into a single delayed
--   aggregate pass by giving it a CONSTANT per-graph idempotency key
--   (`infer:<graph>`, `er:<graph>`, `expertise:<graph>`) + `ON CONFLICT
--   (idempotency_key) DO NOTHING`. But the original table-wide
--   `UNIQUE (idempotency_key)` keeps that key occupied FOREVER: completed jobs
--   are retained (status='done') and the key is never cleared, so once the first
--   pass finishes, every later enqueue is silently dropped. Result: the
--   inference-closure, entity-resolution, and expertise passes each ran exactly
--   ONCE per graph and never again — the multi-hop "who is X's mom" inference and
--   duplicate-reef merging went dark after the first run.
--
--   Fix: replace the table-wide unique constraint with a PARTIAL unique index over
--   pending jobs only. The dedup slot reopens the instant a pass leaves 'pending'
--   (claimed → running), so the next extraction can enqueue a fresh pass.
--   `queue::enqueue` is updated to `ON CONFLICT (idempotency_key) WHERE status =
--   'pending'`, and `queue::fail` now clears the key when re-queuing so a retried
--   job can't collide with a newer pending pass that holds the same key.
--
-- Depends on: 0004_jobs. NOTE: numbered 0050 to sit AFTER the unrelated
--   0048_node_merges / 0049_connector_identity_self_reef (PR #111). Merge that PR
--   first; if this lands first instead, renumber to 0048.
-- Down:
--   DROP INDEX IF EXISTS idx_jobs_idem_pending;
--   ALTER TABLE jobs ADD CONSTRAINT jobs_idempotency_key_key UNIQUE (idempotency_key);

-- Drop the table-wide unique constraint (Postgres auto-names a column UNIQUE
-- constraint `<table>_<column>_key`).
ALTER TABLE jobs DROP CONSTRAINT IF EXISTS jobs_idempotency_key_key;

-- Dedup only among queued (not-yet-claimed) jobs. NULL keys remain un-deduped
-- (a unique index treats NULLs as distinct), so keyless jobs are unaffected.
CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_idem_pending
    ON jobs (idempotency_key)
    WHERE status = 'pending';
