-- Migration: 0014_slack_huddle_captures_ledger (tenant tree)
-- Description: Tenant-tree half of the paired
--   `migrations_control/0102_slack_huddle_captures_ledger.sql` — see that
--   file for the full rationale (queue → terminal-outcome ledger, why
--   idempotency still matters with no natural Slack retry, why
--   `status`/`error`/`opted_by` are dropped in favour of `skip_reason`, and
--   why this file's DDL is ordered ADD -> BACKFILL -> DROP -> CHECK instead
--   of destroying `error`/`status` before `skip_reason` exists).
--
--   `slack_huddle_captures` is one of the shared content tables baked into
--   `migrations_tenant/0001_tenant_baseline.sql`, so this file's `ALTER
--   TABLE` must stay byte-equivalent to the control side's or
--   `scripts/check_schema_parity.sh` fails. The table carries no RLS policy
--   on this tree today (`DELIBERATELY_UNCOVERED` in
--   `backend/tests/tenant_rls_pg.rs`) and this migration does not change
--   that — huddle capture is CONSUMER-only
--   (`workers::tenant_harness`'s decision table), so this tree's copy of the
--   table stays inert exactly as before.
--
-- Depends on: 0001_tenant_baseline (slack_huddle_captures shape).
--
-- Paired with: migrations_control/0102_slack_huddle_captures_ledger.sql.
--
-- Down: see the control-tree migration's Down block (byte-identical DDL,
--   same "not a true inverse" caveats apply here too).
--   ALTER TABLE slack_huddle_captures DROP CONSTRAINT IF EXISTS slack_huddle_captures_terminal_state_check;
--   ALTER TABLE slack_huddle_captures ADD COLUMN error TEXT;
--   UPDATE slack_huddle_captures SET error = skip_reason WHERE skip_reason IS NOT NULL;
--   ALTER TABLE slack_huddle_captures ADD COLUMN status TEXT;
--   UPDATE slack_huddle_captures SET status = CASE WHEN processed_at IS NOT NULL THEN 'processed' ELSE 'failed' END;
--   ALTER TABLE slack_huddle_captures ALTER COLUMN status SET NOT NULL;
--   ALTER TABLE slack_huddle_captures ALTER COLUMN status SET DEFAULT 'pending';
--   ALTER TABLE slack_huddle_captures ADD CONSTRAINT slack_huddle_captures_status_check
--       CHECK (status IN ('pending', 'processing', 'processed', 'failed', 'cancelled'));
--   ALTER TABLE slack_huddle_captures ADD COLUMN opted_by TEXT NOT NULL DEFAULT '';
--   ALTER TABLE slack_huddle_captures DROP COLUMN IF EXISTS skip_reason;

-- Step 1: add the new column FIRST — nothing destroyed yet.
ALTER TABLE slack_huddle_captures ADD COLUMN IF NOT EXISTS skip_reason TEXT;

-- Step 2: backfill BEFORE dropping anything (BLOCKER 2/3 — see the control
-- migration's header for the full incident this guards against).
UPDATE slack_huddle_captures
SET    skip_reason = COALESCE(NULLIF(error, ''), 'legacy pre-ledger row (status=' || status || ')')
WHERE  processed_at IS NULL
  AND  skip_reason IS NULL;

-- Step 3: now safe to drop the old lifecycle columns.
ALTER TABLE slack_huddle_captures DROP COLUMN IF EXISTS status;
ALTER TABLE slack_huddle_captures DROP COLUMN IF EXISTS error;
ALTER TABLE slack_huddle_captures DROP COLUMN IF EXISTS opted_by;

-- Step 4: enforce the ledger's terminal-state invariant at the database
-- level.
ALTER TABLE slack_huddle_captures
    ADD CONSTRAINT slack_huddle_captures_terminal_state_check
    CHECK (processed_at IS NOT NULL OR skip_reason IS NOT NULL);

DROP INDEX IF EXISTS idx_huddle_captures_pending;
