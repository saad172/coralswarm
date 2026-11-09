-- Migration: 0289_teams_personal_capture_due_grant_fairness
-- Description: bind the due-capture resolver (0288) to the exact grant the consumer
--   worker would accept, so captures that can never be pulled stop occupying the
--   bounded `LIMIT p_limit` page ahead of healthy ones.
--
--   0288 only required that SOME valid ownerless Microsoft row exist for the account.
--   `workers::teams_meeting_sync::tick_personal` then re-derives the identity with
--   `routes::teams_onboarding::capture_identity` and claims rows with
--   `c.tenant_id = tenant AND c.aad_id = aad`. A capture whose grant was replaced
--   (different tenant or object id), duplicated (a second ownerless Microsoft row),
--   moved to the consumer directory, or stored with a malformed provider id is never
--   claimed, so its past `next_poll_at` is never advanced. It sorted first under
--   `ORDER BY 3, 1 LIMIT p_limit` for as long as its 24-hour window stayed open, and
--   enough of them (PERSONAL_ACCOUNTS_PER_TICK = 100) hid every healthy due account.
--
--   A capture is now enumerated only if, for its account, the ownerless Microsoft
--   rows are exactly one and that row is what `capture_identity` accepts:
--     * both `provider_workspace_id` and `provider_user_id` are present and parse as
--       UUIDs (the forms `Uuid::parse_str` accepts: simple, hyphenated, braced, urn);
--     * the tenant is not the consumer directory (case-insensitive, as the worker);
--     * `credential_invalid_at IS NULL`;
--     * and it equals the capture's own `tenant_id` / `aad_id`, compared as TEXT with
--       the same exact equality the worker's claim uses (no UUID cast of provider
--       strings, so a malformed row cannot make the resolver raise).
--   The account/owner/graph/tenant-routing predicates are unchanged from 0288. The
--   access token's expiry is deliberately NOT consulted: an expired but refreshable
--   token is the normal state before the worker refreshes it, and the resolver never
--   reads or decrypts a credential.
--
--   Signature, return shape, SECURITY DEFINER, search_path and the actor
--   set/restore are as in 0288. The scheduler policy and every grant are untouched
--   (re-stated below only so the file is self-contained; they are idempotent).
--
-- Depends on: 0288_teams_personal_meeting_captures.sql.
-- Down:
--   Re-run the `CREATE OR REPLACE FUNCTION resolve_teams_personal_capture_due` body
--   from 0288_teams_personal_meeting_captures.sql.

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
      -- capture_identity: the account-wide Microsoft rows must be exactly one ...
      AND (SELECT count(*) FROM oauth_tokens one
           WHERE one.account_id = c.account_id AND one.provider = 'microsoft'
             AND one.owner_user_id IS NULL) = 1
      -- ... and that row must be the live work/school grant this capture was approved for.
      AND EXISTS (
          SELECT 1 FROM oauth_tokens ot
          WHERE ot.account_id = c.account_id AND ot.provider = 'microsoft'
            AND ot.owner_user_id IS NULL AND ot.credential_invalid_at IS NULL
            AND ot.provider_workspace_id = c.tenant_id
            AND ot.provider_user_id = c.aad_id
            AND lower(ot.provider_workspace_id) <> '9188040d-6c67-4c5b-b112-36a304b66dad'
            AND ot.provider_workspace_id ~ '^([0-9a-fA-F]{32}|[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}|\{[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\}|urn:uuid:[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})$'
            AND ot.provider_user_id ~ '^([0-9a-fA-F]{32}|[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}|\{[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\}|urn:uuid:[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})$')
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
