-- Migration 0274: consumer-plane Google Calendar sweep discovery
-- (SECURITY DEFINER bootstrap function) + its rotation clock.
--
--   `workers::google_calendar_sync`'s CONSUMER-plane tick sweeps every
--   personal account holding an account-wide Google Calendar connection
--   (`oauth_tokens.provider = 'google'`, `owner_user_id IS NULL`) — cross-
--   account by definition, so there is no single account to scope a plain
--   `SELECT ... FROM oauth_tokens` against. Left as a raw table read under
--   the production `coralswarm_app` role, `0083`'s `account_isolation`
--   policy would compare `account_id = NULL`, match nothing, and SUCCEED —
--   the inert-but-green shape `0181`/`0216`'s headers describe. Same
--   pattern, same discipline: account ids only, never a token column; each
--   id is then read inside its own `begin_account_scoped` transaction
--   (`routes::connectors::load_creds_bg`).
--
--   Why this exists at all (prod, 2026-10-07): a personal account connected
--   Calendar while `GOOGLE_CALENDAR_WEBHOOK_URL` was unset, the connect-time
--   `events.watch` failed, and nothing ever retried — the renewal sweep only
--   enumerates `google_watch_channels` rows (there was none) and the
--   calendar sync worker's consumer entry was a structural no-op. The token
--   was never loaded again. This function is what lets the sweep find such
--   a connection from the TOKEN, which is the one row a connect always
--   leaves behind.
--
--   Plane rule: an account with NO `tenant_databases` row AND either no
--   `clerk_org_id` or a `personal_orgs` one (an adopted personal org — one
--   person's own ocean, `0208`) lives on the consumer plane. A REAL org that
--   was never tenant-onboarded also has no routing row, but its primary
--   ocean is org-visible: sweeping its account-wide row would backfill one
--   member's calendar into everyone's view, so it is excluded — the
--   `clerk_org_id` arm is what tells the two no-routing-row shapes apart.
--   An account with a routing row is the org entry's territory, whatever
--   token shapes it holds.
--
--   Only LIVE credentials: a row whose `credential_invalid_at` is set (`0251`
--   — a definite revocation, for Google `invalid_grant` on refresh or a 401
--   on a just-refreshed token, recorded by `credential_health::
--   record_if_revoked`) is skipped until a reconnect clears the mark
--   (`routes::oauth::store_token_bundle_tx`'s upsert). Nothing a retry can
--   fix, so nothing to spend a sweep slot — or a warn line every 30 minutes
--   — on.
--
--   `p_limit` is mandatory and the order is least-recently-ATTEMPTED first:
--   `google_calendar_sync_attempts` (below) is stamped by the sweep for each
--   account BEFORE its pass runs, success or failure, so an account that
--   fails every time (no owner, Google unreachable) rotates to the back and
--   a never-attempted connection (no row, `NULLS FIRST`) is picked up on the
--   very next tick. Ordering by a success-only timestamp
--   (`google_watch_channels.updated_at`) would let the permanently-failing
--   accounts hold the head of a bounded sweep forever — exactly the
--   starvation this whole migration exists to end.
--
--   `google_calendar_sync_attempts` is account-scoped bookkeeping on the
--   shared control DB: `account_isolation` policy (same predicate + shape as
--   `0180_google_watch_channels`, with `WITH CHECK` like `0243`), written
--   inside `begin_account_scoped`; the resolver reads it as definer. Keyed
--   by `account_id` alone because the personal sweep's unit is the
--   account-wide connection; the org pass is not bounded and needs no clock.
--   Control-plane only, no tenant-tree twin (`oauth_tokens`'s stamp sibling
--   lives where the resolver can see it) — in `check_schema_parity.sh`'s
--   `CONTROL_TABLES`, `check_rls_coverage.sh`'s `EXTRA_RLS_TABLES` and
--   `tests/consumer_rls_pg.rs`'s `RLS_TABLES`. The function is registered in
--   `check_schema_parity.sh`'s `CONTROL_FUNCTIONS` list.
--
-- Depends on: 0175_oauth_tokens_owner_user_id.sql, 0208_personal_orgs.sql,
--   0251_oauth_tokens_credential_invalid.sql, 0078_tenant_databases.sql.
-- Down:
--   REVOKE EXECUTE ON FUNCTION resolve_google_calendar_personal_accounts(int) FROM coralswarm_app;
--   REVOKE EXECUTE ON FUNCTION resolve_google_calendar_personal_accounts(int) FROM PUBLIC;
--   DROP FUNCTION IF EXISTS resolve_google_calendar_personal_accounts(int);
--   DROP POLICY IF EXISTS account_isolation ON google_calendar_sync_attempts;
--   DROP TABLE IF EXISTS google_calendar_sync_attempts;

CREATE TABLE IF NOT EXISTS google_calendar_sync_attempts (
    account_id      UUID        PRIMARY KEY REFERENCES accounts(id) ON DELETE CASCADE,
    last_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE google_calendar_sync_attempts ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON google_calendar_sync_attempts;
CREATE POLICY account_isolation ON google_calendar_sync_attempts
    USING (account_id = current_setting('app.account_id')::uuid)
    WITH CHECK (account_id = current_setting('app.account_id')::uuid);

CREATE OR REPLACE FUNCTION resolve_google_calendar_personal_accounts(p_limit int)
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
        LEFT JOIN google_calendar_sync_attempts att ON att.account_id = ot.account_id
        WHERE ot.provider = 'google'
          AND ot.owner_user_id IS NULL
          AND ot.credential_invalid_at IS NULL
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

REVOKE EXECUTE ON FUNCTION resolve_google_calendar_personal_accounts(int) FROM PUBLIC;

DO $$ BEGIN
  IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname='coralswarm_app') THEN
    GRANT EXECUTE ON FUNCTION resolve_google_calendar_personal_accounts(int) TO coralswarm_app;
  END IF;
END $$;

GRANT EXECUTE ON FUNCTION resolve_google_calendar_personal_accounts(int) TO CURRENT_USER;
