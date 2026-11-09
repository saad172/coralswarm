-- Migration: 0057_escrow_recovery_log_identity
-- Description: Capture member identity AT RECOVERY TIME on escrow_recovery_log
--              (follow-up to PR #176, which made raw ids non-user-visible). The
--              recovery log previously stored only `recovered_user_id` /
--              `initiated_by` (raw UUIDs) and the frontend resolved them
--              against the *current* org roster, falling back to "a former
--              member" once someone left — losing their identity forever.
--
--              These columns snapshot the email known at the moment the
--              recovery is logged, so departed members can still be named in
--              the audit trail. Nullable and additive: existing rows are left
--              untouched (NULL) and the frontend keeps its client-side roster
--              lookup / "a former member" fallback for those.
--
--              `users` has no name column (see 0001_init) — only email is
--              captured server-side; there is no `recovered_user_name` to add.
--
-- Depends on: 0030_org_escrow
--
-- Down:
--   ALTER TABLE escrow_recovery_log DROP COLUMN IF EXISTS initiated_by_email;
--   ALTER TABLE escrow_recovery_log DROP COLUMN IF EXISTS recovered_user_email;

ALTER TABLE escrow_recovery_log
    ADD COLUMN IF NOT EXISTS recovered_user_email TEXT,
    ADD COLUMN IF NOT EXISTS initiated_by_email    TEXT;
