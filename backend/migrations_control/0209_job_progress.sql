-- Migration: 0209_job_progress
-- Description: `jobs.progress_current`/`jobs.progress_total` — the schema
--              half of pollable job progress (async-promote PR, Rust side
--              landed in the same PR).
--
--              WHY THIS EXISTS. `promote_node_scoped`'s coral-copy tail
--              (`store::mod.rs`'s `copy_atoms_to_graph_scoped` +
--              `link_node_atom_scoped`) used to run per-atom, synchronously,
--              inside the HTTP request that called `/v1/nodes/{id}/promote` —
--              for a reef with thousands of attached corals this is
--              thousands of sequential round trips inside ONE request/
--              transaction (measured against production: a 3,129-coral
--              promote never completed at all, timing out with no durable
--              effect). The fix moves that tail into a `PromoteNode`
--              background job (`workers::promote`) — but a job an operator
--              or a UI progress bar cannot see the state of is just a slower
--              version of the same UX problem. `jobs` had no notion of
--              partial progress at all before this migration: `status` is a
--              five-value enum (`pending`/`running`/`done`/`failed`/`dead`)
--              with nothing between "just started" and "finished".
--
--              `progress_current`/`progress_total` are deliberately BOTH
--              nullable, independent of `status`, and updated in place by
--              the running worker (never reset on read) — see
--              `jobs::queue::update_progress`'s doc comment for the update
--              discipline (monotonic, never decreases, safe to poll from any
--              other transaction without taking any lock this table doesn't
--              already have). NULL on both columns (the default, and the
--              value for every OTHER job kind that never calls
--              `update_progress`) means "this job kind does not report
--              granular progress" — a poller must treat NULL as "no
--              progress signal available", not as zero.
--
-- Depends on: 0004_jobs (creates `jobs`).
-- Down:
--   ALTER TABLE jobs DROP COLUMN IF EXISTS progress_current;
--   ALTER TABLE jobs DROP COLUMN IF EXISTS progress_total;

SET LOCAL lock_timeout = '3s';

ALTER TABLE jobs ADD COLUMN IF NOT EXISTS progress_current INTEGER;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS progress_total INTEGER;

ALTER TABLE jobs ADD CONSTRAINT jobs_progress_current_nonnegative
    CHECK (progress_current IS NULL OR progress_current >= 0);
ALTER TABLE jobs ADD CONSTRAINT jobs_progress_total_nonnegative
    CHECK (progress_total IS NULL OR progress_total >= 0);
