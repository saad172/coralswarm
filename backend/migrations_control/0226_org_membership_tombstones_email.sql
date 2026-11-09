-- Migration: 0226_org_membership_tombstones_email
-- Description: CS-430 (org-onboarding-from-the-invite plan, D12, item 33).
--
--              `org_membership_tombstones` (0081) is keyed `(clerk_org_id,
--              user_id)` — right for blocking a token belonging to the SAME
--              account from resurrecting a membership, wrong for the invite
--              path (CS-428, item 3/9), which only ever holds an *email
--              address* at invite time. A member who is removed, then
--              deletes and recreates their CoralSwarm account, gets a brand
--              new `user_id` and slips straight past the existing key —
--              D12's own "Build note". Founder review (tree node 07): record
--              the address on the tombstone at removal time and check BOTH
--              keys at invite time.
--
--              Nullable TEXT, no CHECK — the value is written best-effort at
--              removal time from `users.email` (NOT NULL on that table today,
--              so this is expected to be populated in the overwhelming
--              majority of cases) and is never fabricated when unknown; a
--              null here is a real, load-bearing "we don't know" answered by
--              `clerk_provision::is_org_membership_tombstoned`'s email arm as
--              `false`, not an oversight. The one column, not a normalized-
--              vs-raw pair: the value stored here IS already
--              `lower(trim(...))`-normalized by the writer (same "exact,
--              normalised, no provider canonicalisation" rule
--              `connectors::slack::normalize_email` and every other
--              `users.email` comparison in this codebase already use — see
--              e.g. `connectors/slack.rs:1797`,
--              `connectors/microsoft.rs:3077`), so a reader need only
--              normalize its OWN input the same way before comparing, never
--              the stored column.
--
--              Partial index on `(clerk_org_id, email)`, mirroring 0220's
--              `coding_sessions.branch` shape: this table is control-only
--              (never has a tenant twin — see `CONTROL_TABLES` in
--              `scripts/check_schema_parity.sh`), so there is no paired
--              tenant migration to keep in step.
--
-- Depends on: 0081_org_membership_tombstones (the table itself)
--
-- Down:
--   DROP INDEX IF EXISTS idx_org_membership_tombstones_org_email;
--   ALTER TABLE org_membership_tombstones DROP COLUMN IF EXISTS email;

ALTER TABLE org_membership_tombstones
    ADD COLUMN IF NOT EXISTS email TEXT;

CREATE INDEX IF NOT EXISTS idx_org_membership_tombstones_org_email
    ON org_membership_tombstones (clerk_org_id, email)
    WHERE email IS NOT NULL;
