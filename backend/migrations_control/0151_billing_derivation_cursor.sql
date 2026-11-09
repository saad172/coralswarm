-- Migration: 0151_billing_derivation_cursor
-- Description: usage-metering PR9 of the 13-PR train
--   (`.scratch/usage-metering/plan.md` §1 PR9) — six nullable cursor columns
--   on `credit_balances`, one keyset-pagination pair per costing axis (job /
--   query / journal). CONTROL-ONLY (no tenant-tree change): `credit_balances`
--   already lives control-only (migration `0150_credit_ledger.sql`, in
--   `scripts/check_schema_parity.sh`'s `CONTROL_TABLES`), so these six
--   columns need no new registration there.
--
--   ── Numbering note ──
--   Live heads at authoring time: control `0150` (PR8, merged), tenant `0055`
--   (PR7's tenant-side twin, merged). Sibling PR5 (#612) and PR6 are both
--   in flight with NO migrations of their own, so `0151`/`0152` (control) and
--   `0056` (tenant, for `0152`'s paired twin) are free per explicit
--   instruction. Per plan §0's "Numbering reality" — assigned at merge time,
--   not authoring time — these may get renumbered if another migration-
--   bearing PR lands first; do not treat the literal numbers as load-bearing,
--   only the ordering and the DDL.
--
--   ── Why `credit_balances`, not a new cursor table ──
--   The task brief for this PR names `graphs.er_sweep_cursor_*`
--   (`0136_er_sweep_cursor.sql`) as the precedent: store a sweep cursor as
--   columns on the row that already owns the relevant state, not a generic
--   `(worker, key) -> cursor` table. `graphs` is the right owner for the
--   entity-resolution sweep because that sweep is genuinely PER OCEAN. This
--   worker's three cursors are genuinely PER ACCOUNT instead (see the Rust
--   module doc in `backend/src/workers/billing_derivation.rs` for the full
--   per-account-loop design this follows, mirroring
--   `workers::platform_rollup::consumer_tick`'s existing account-enumeration
--   shape) — so the matching "owning row" is `credit_balances`
--   (`account_id` PRIMARY KEY), which this worker already reads and writes
--   every tick to materialize the running balance. Adding the cursor here
--   means the balance update and the cursor advance land in the SAME
--   transaction and the SAME row, so a crash between them is impossible by
--   construction, and it needs NO new RLS policy: `credit_balances` already
--   carries `account_isolation` (0150), and a cursor is exactly the same
--   kind of per-account operational bookkeeping the ER-sweep-cursor
--   migration's own header used to justify landing on an already-RLS-covered
--   table instead of a bespoke one.
--
--   ── The three axes, briefly (full detail: the Rust module doc) ──
--   * `job_axis_cursor_(finished_at, job_id)` — keyset cursor over this
--     account's completed `ExtractEntities` jobs (`jobs.finished_at, jobs.id`
--     — the composite the plan's own PR9 section specifies, "`finished_at`
--     is `now()` at commit and therefore not strictly monotonic across
--     concurrent transactions").
--   * `query_axis_cursor_(created_at, usage_id)` — keyset cursor over this
--     account's request-path `llm_usage` rows (`WHERE source_query_id IS NOT
--     NULL`), ordered by `(created_at, id)` — the same non-monotonic-commit
--     hazard applies to `llm_usage.created_at` as to `jobs.finished_at`, so
--     it gets the identical composite-cursor treatment. Needs its own index
--     — see `0152_llm_usage_source_query_id_cursor_index_concurrently.sql`.
--   * `journal_axis_cursor_(period, usage_id)` — keyset cursor over this
--     account's journal-compile `llm_usage` rows (`WHERE
--     source_journal_period IS NOT NULL`), ordered by
--     `(source_journal_period, id)` — already indexed by merged PR7
--     (`0147_llm_usage_journal_period_cursor_index_concurrently.sql`).
--
--   All six columns are NULLABLE with no default: NULL means "no cursor yet
--   for this account / this axis", which the Rust-side keyset query treats
--   as "start from the beginning" (same convention as
--   `0136_er_sweep_cursor.sql`'s own NULL contract).
--
--   `credit_balances` is not yet a hot table (nothing writes to it before
--   this PR), but `SET LOCAL lock_timeout` is applied anyway on the same
--   "cheap insurance on any ALTER of a table other code touches concurrently"
--   basis `0141`'s header gives for `llm_usage`.
--
-- Depends on: 0150_credit_ledger.sql (creates `credit_balances`).
-- Down:
--   ALTER TABLE credit_balances
--       DROP COLUMN IF EXISTS job_axis_cursor_finished_at,
--       DROP COLUMN IF EXISTS job_axis_cursor_job_id,
--       DROP COLUMN IF EXISTS query_axis_cursor_created_at,
--       DROP COLUMN IF EXISTS query_axis_cursor_usage_id,
--       DROP COLUMN IF EXISTS journal_axis_cursor_period,
--       DROP COLUMN IF EXISTS journal_axis_cursor_usage_id;

SET LOCAL lock_timeout = '3s';

ALTER TABLE credit_balances
    ADD COLUMN IF NOT EXISTS job_axis_cursor_finished_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS job_axis_cursor_job_id UUID,
    ADD COLUMN IF NOT EXISTS query_axis_cursor_created_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS query_axis_cursor_usage_id UUID,
    ADD COLUMN IF NOT EXISTS journal_axis_cursor_period TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS journal_axis_cursor_usage_id UUID;
