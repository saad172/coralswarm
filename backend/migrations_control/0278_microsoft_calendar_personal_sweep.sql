-- Migration: 0278_microsoft_calendar_personal_sweep
-- Description: consumer-plane Microsoft Calendar sweep — the discovery
--   resolver (SECURITY DEFINER) and its rotation clock. The Microsoft twin
--   of 0274 (`resolve_google_calendar_personal_accounts`).
--
--   ── The gap ──
--   `workers::microsoft_calendar_sync`'s consumer entry was a deliberate
--   no-op (0189's header: "the personal plane's refresh is user-triggered
--   only"), so a personal account's calendar synced ONLY when the user
--   clicked "sync now" — no scheduled deletions, no scheduled edits, no
--   scheduled backfill of the year of history 0273 opened. Org members got
--   all of that every 30 minutes. Founder rule 2026-10-06: nothing is
--   org-only or personal-only.
--
--   ── Discovery: `resolve_microsoft_calendar_personal_accounts(p_limit)` ──
--   Every personal account holding an account-wide Microsoft connection
--   (`oauth_tokens.provider = 'microsoft'`, `owner_user_id IS NULL`) is a
--   cross-account read on the RLS-bound consumer pool: under the production
--   `coralswarm_app` role a bare `SELECT ... FROM oauth_tokens` compares
--   `account_id = NULL`, matches nothing, and SUCCEEDS — the inert-but-green
--   shape 0181/0216/0274 exist to avoid. Same discipline: account ids only,
--   never a token column; the token is then read inside the account's own
--   `begin_account_scoped` transaction (`routes::connectors::load_creds_bg`).
--
--   Consumer plane = no `tenant_databases` row AND (no `clerk_org_id`, or a
--   `personal_orgs` one). "No routing row" alone would also match a real
--   org that was never tenant-onboarded, whose members' calendars are the
--   org entry's territory once it is.
--
--   Excluded: a credential already marked dead (`credential_invalid_at`,
--   0251 — set by `credential_health::record_if_revoked` when the Microsoft
--   token endpoint answers `invalid_grant`, cleared by the reconnect upsert
--   in `routes::oauth::store_token_bundle_tx`), so a revoked grant leaves
--   the queue instead of warning every tick forever; and a connection whose
--   `microsoft_calendar_sync_state.sync_enabled` is false (0189's off
--   switch, respected on both planes).
--
--   ── Rotation: `microsoft_calendar_sync_attempts` ──
--   Ordered least-recently-ATTEMPTED first, `NULLS FIRST` (never attempted
--   sorts ahead), bounded by `p_limit`. The sweep stamps `last_attempt_at`
--   BEFORE a pass runs, success or failure, so a connection that fails
--   every pass (no owner, Graph unreachable) moves to the back each tick
--   instead of pinning the head of a bounded sweep — ordering by a
--   last-SUCCESS stamp would let the always-failing accounts starve the
--   never-synced ones forever. Keyed by `account_id` alone (the personal
--   sweep's unit is the account-wide connection; the org pass enumerates
--   every member each tick and needs no clock) and NOT a column on
--   `microsoft_calendar_sync_state`: that row needs a resolved `graph_id`,
--   and a connection whose ocean cannot be resolved would then never be
--   stamped — the starvation again, one step later.
--
--   Account-scoped bookkeeping on the shared control DB: `account_isolation`
--   policy (same predicate as 0180/0274, `WITH CHECK` like 0243), written
--   inside `begin_account_scoped`; the resolver reads it as definer.
--   Control-plane only, no tenant-tree twin — in
--   `scripts/check_schema_parity.sh`'s `CONTROL_TABLES`,
--   `scripts/check_rls_coverage.sh`'s `EXTRA_RLS_TABLES` and
--   `tests/consumer_rls_pg.rs`'s `RLS_TABLES`; the function is in
--   `check_schema_parity.sh`'s `CONTROL_FUNCTIONS`.
--
-- Depends on: 0175_oauth_tokens_owner_user_id.sql, 0189_microsoft_calendar_sync_state.sql,
--   0208_personal_orgs.sql, 0251_oauth_tokens_credential_invalid.sql,
--   0078_tenant_databases.sql.
-- Down:
--   REVOKE EXECUTE ON FUNCTION resolve_microsoft_calendar_personal_accounts(int) FROM coralswarm_app;
--   REVOKE EXECUTE ON FUNCTION resolve_microsoft_calendar_personal_accounts(int) FROM PUBLIC;
--   DROP FUNCTION IF EXISTS resolve_microsoft_calendar_personal_accounts(int);
--   DROP POLICY IF EXISTS account_isolation ON microsoft_calendar_sync_attempts;
--   DROP TABLE IF EXISTS microsoft_calendar_sync_attempts;

CREATE TABLE IF NOT EXISTS microsoft_calendar_sync_attempts (
    account_id      UUID        PRIMARY KEY REFERENCES accounts(id) ON DELETE CASCADE,
    last_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE microsoft_calendar_sync_attempts ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON microsoft_calendar_sync_attempts;
CREATE POLICY account_isolation ON microsoft_calendar_sync_attempts
    USING (account_id = current_setting('app.account_id')::uuid)
    WITH CHECK (account_id = current_setting('app.account_id')::uuid);

CREATE OR REPLACE FUNCTION resolve_microsoft_calendar_personal_accounts(p_limit int)
RETURNS SETOF uuid
LANGUAGE sql
SECURITY DEFINER
STABLE
SET search_path = public, pg_temp
AS $$
    SELECT account_id FROM (
        SELECT DISTINCT ot.account_id, att.last_attempt_at
        FROM oauth_tokens ot
        JOIN accounts a ON a.id = ot.account_id
        LEFT JOIN microsoft_calendar_sync_attempts att ON att.account_id = ot.account_id
        LEFT JOIN microsoft_calendar_sync_state st
               ON st.account_id = ot.account_id AND st.owner_user_id = a.owner_user_id
        WHERE ot.provider = 'microsoft'
          AND ot.owner_user_id IS NULL
          AND ot.credential_invalid_at IS NULL
          AND COALESCE(st.sync_enabled, true)
          AND NOT EXISTS (
              SELECT 1 FROM tenant_databases td WHERE td.account_id = ot.account_id
          )
          AND (
              a.clerk_org_id IS NULL
              OR EXISTS (SELECT 1 FROM personal_orgs po WHERE po.clerk_org_id = a.clerk_org_id)
          )
    ) d
    ORDER BY d.last_attempt_at ASC NULLS FIRST, d.account_id
    LIMIT p_limit
$$;

REVOKE EXECUTE ON FUNCTION resolve_microsoft_calendar_personal_accounts(int) FROM PUBLIC;

DO $$ BEGIN
  IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname='coralswarm_app') THEN
    GRANT EXECUTE ON FUNCTION resolve_microsoft_calendar_personal_accounts(int) TO coralswarm_app;
  END IF;
END $$;

GRANT EXECUTE ON FUNCTION resolve_microsoft_calendar_personal_accounts(int) TO CURRENT_USER;
