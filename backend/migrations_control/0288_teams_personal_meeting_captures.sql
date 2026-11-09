-- Migration: 0288_teams_personal_meeting_captures
-- Description: explicit per-meeting Teams transcript capture for a PERSONAL workspace
--   connected to a work/school Microsoft identity. Graph supports delegated
--   work/school identities for transcript reads (consumer Microsoft accounts are
--   excluded; the application refuses those before this table is touched).
--
--   The capture ledger (`teams_meeting_captures`, 0260) already keys consent on
--   (account_id, user_id, tenant_id, meeting_id). For a personal account `user_id` is
--   the account's real owner (`accounts.owner_user_id`) — never the ownerless
--   (`oauth_tokens.owner_user_id IS NULL`) credential row — and `graph_id` is that
--   account's own ownerless `kind='personal'` graph.
--
--   ── 1. Capture-owner WITH CHECK ──
--   Only the WITH CHECK widens. USING is unchanged (account + acting user), so no
--   existing row becomes visible to anyone new. A write may now also pin a personal
--   graph, and only when ALL of these hold: the graph belongs to the row's account,
--   is unarchived, is `kind='personal'` with no owner, and the account is a personal
--   account (`clerk_org_id IS NULL`) whose owner is the row's `user_id`. The member
--   arm is byte-identical to 0260. The TENANT twin (tenant 0105) is deliberately not
--   widened: a personal account has no tenant database.
--
--   ── 2. Due-capture discovery for the consumer worker ──
--   `workers::teams_meeting_sync` previously returned idle on the consumer plane. The
--   ledger is FORCE ROW LEVEL SECURITY and owner-keyed, so a cross-account "what is
--   due" read cannot run under the application role. `resolve_teams_personal_capture_due`
--   is a SECURITY DEFINER resolver (same discipline as 0278): it returns account/user
--   ids and a due instant only — never a meeting id, transcript, token or content — for
--   unrevoked captures of a personal, non-tenant-routed account whose owner is live and
--   whose account-wide Microsoft credential is not marked dead. Each row is then
--   authorized again, per account, inside the owner's own scoped transaction
--   (identity, graph, installation, generation), and the credential is read only after
--   that finds due work. FORCE RLS applies to the definer too, so the resolver's role
--   gets a SELECT-only policy gated on `app.tenant_actor='system'`, which the resolver
--   sets itself; it is `TO <owner>`, so the application role gains nothing.
--
-- Depends on: 0260_teams_meeting_captures.sql, 0264 (personal owner mapping),
--   0251_oauth_tokens_credential_invalid.sql, 0078_tenant_databases.sql.
-- Down:
--   REVOKE EXECUTE ON FUNCTION resolve_teams_personal_capture_due(int) FROM PUBLIC;
--   DROP FUNCTION IF EXISTS resolve_teams_personal_capture_due(int);
--   DROP POLICY IF EXISTS teams_personal_capture_scheduler ON teams_meeting_captures;
--   ALTER POLICY meeting_capture_owner ON teams_meeting_captures WITH CHECK (<0260 member-only form>);

ALTER POLICY meeting_capture_owner ON teams_meeting_captures
 WITH CHECK (account_id = nullif(current_setting('app.account_id',true),'')::uuid
 AND user_id = nullif(current_setting('app.user_id',true),'')::uuid
 AND (EXISTS (SELECT 1 FROM graphs g WHERE g.id=graph_id AND g.account_id=teams_meeting_captures.account_id
       AND g.owner_user_id=teams_meeting_captures.user_id AND g.kind='member' AND g.archived_at IS NULL)
   OR EXISTS (SELECT 1 FROM graphs g JOIN accounts a ON a.id=g.account_id
       WHERE g.id=graph_id AND g.account_id=teams_meeting_captures.account_id
       AND g.owner_user_id IS NULL AND g.kind='personal' AND g.archived_at IS NULL
       AND a.clerk_org_id IS NULL AND a.owner_user_id=teams_meeting_captures.user_id)));

DO $$ BEGIN
  EXECUTE format(
    'CREATE POLICY teams_personal_capture_scheduler ON teams_meeting_captures FOR SELECT TO %I USING (current_setting(''app.tenant_actor'',true)=''system'')',
    current_user);
END $$;

CREATE OR REPLACE FUNCTION resolve_teams_personal_capture_due(p_limit int)
RETURNS TABLE(account_id uuid, user_id uuid, due_at timestamptz)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_temp
AS $$
DECLARE
    prior text := current_setting('app.tenant_actor', true);
BEGIN
    PERFORM set_config('app.tenant_actor', 'system', true);
    RETURN QUERY
    SELECT c.account_id, c.user_id, min(greatest(c.next_poll_at, c.occurrence_start)) AS due_at
    FROM teams_meeting_captures c
    JOIN accounts a ON a.id = c.account_id AND a.clerk_org_id IS NULL AND a.owner_user_id = c.user_id
    JOIN users u ON u.id = c.user_id AND u.deleted_at IS NULL
    JOIN graphs g ON g.id = c.graph_id AND g.account_id = c.account_id
         AND g.kind = 'personal' AND g.owner_user_id IS NULL AND g.archived_at IS NULL
    WHERE c.revoked_at IS NULL
      AND c.occurrence_end + interval '24 hours' >= now()
      AND NOT EXISTS (SELECT 1 FROM tenant_databases td WHERE td.account_id = c.account_id)
      AND EXISTS (
          SELECT 1 FROM oauth_tokens ot
          WHERE ot.account_id = c.account_id AND ot.provider = 'microsoft'
            AND ot.owner_user_id IS NULL AND ot.credential_invalid_at IS NULL)
    GROUP BY c.account_id, c.user_id
    ORDER BY 3, 1
    LIMIT p_limit;
    PERFORM set_config('app.tenant_actor', coalesce(prior, ''), true);
END
$$;

REVOKE EXECUTE ON FUNCTION resolve_teams_personal_capture_due(int) FROM PUBLIC;

DO $$ BEGIN
  IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname='coralswarm_app') THEN
    GRANT EXECUTE ON FUNCTION resolve_teams_personal_capture_due(int) TO coralswarm_app;
  END IF;
END $$;

GRANT EXECUTE ON FUNCTION resolve_teams_personal_capture_due(int) TO CURRENT_USER;
