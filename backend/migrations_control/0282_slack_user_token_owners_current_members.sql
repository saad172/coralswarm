-- Migration 0282: resolve_slack_user_token_owners(text, text) — an ex-member's
--   live token no longer counts as usable (control-only, no tenant twin).
--
--   0281 returned every member-owned `slack_user` row for one Slack user in
--   one workspace, with `credential_live` = "not marked invalid". It never
--   checked that the row's owner still belongs to the row's account. A
--   member who leaves an org keeps a live `slack_user` row until something
--   revokes it, so `member_joined_channel` went on indexing channels under
--   that ex-member (and the doorbell / backfill paths then used their token).
--
--   `credential_live` now also requires the owner to be a current member:
--     * an `account_members` row for (account_id, owner_user_id), or
--     * a personal account (`clerk_org_id IS NULL`) owned by that user — for
--       a personal account the owner is the account itself.
--   The same predicate `connectors::slack_member_capture` applies to its own
--   `oauth_tokens` / `slack_member_channels` reads.
--
--   Same signature and columns as 0281, so this is a plain CREATE OR REPLACE
--   and either code version is correct against it: the join path filters on
--   `credential_live`; the leave path ignores it and still gets every row, so
--   a leave removes the index row whatever the owner's membership (removing a
--   row is always safe). The column name is kept for that compatibility; read
--   it as "this token may be used".
--
--   Lockdown unchanged: SECURITY DEFINER, pinned search_path, PUBLIC
--   revoked. EXECUTE is granted to coralswarm_app and CURRENT_USER, matching
--   0197 / 0275.
--
-- Depends on: 0281_resolve_slack_user_token_owners.sql (the function this replaces),
--             0001_init.sql (accounts, account_members),
--             0027_clerk_identity.sql (accounts.clerk_org_id; NULL = personal).
-- Down (restores 0281's body):
--   CREATE OR REPLACE FUNCTION resolve_slack_user_token_owners(p_workspace_id text, p_slack_user_id text)
--   RETURNS TABLE(account_id uuid, owner_user_id uuid, credential_live boolean)
--   LANGUAGE sql SECURITY DEFINER STABLE SET search_path = public, pg_temp
--   AS $$
--       SELECT ot.account_id, ot.owner_user_id, ot.credential_invalid_at IS NULL
--       FROM oauth_tokens ot
--       WHERE ot.provider = 'slack_user'
--         AND ot.provider_workspace_id = p_workspace_id
--         AND ot.provider_user_id = p_slack_user_id
--         AND p_workspace_id <> ''
--         AND p_slack_user_id <> ''
--         AND ot.owner_user_id IS NOT NULL
--       ORDER BY ot.account_id, ot.owner_user_id
--   $$;
--   REVOKE EXECUTE ON FUNCTION resolve_slack_user_token_owners(text, text) FROM CURRENT_USER;

CREATE OR REPLACE FUNCTION resolve_slack_user_token_owners(p_workspace_id text, p_slack_user_id text)
RETURNS TABLE(account_id uuid, owner_user_id uuid, credential_live boolean)
LANGUAGE sql
SECURITY DEFINER
STABLE
SET search_path = public, pg_temp
AS $$
    SELECT ot.account_id,
           ot.owner_user_id,
           ot.credential_invalid_at IS NULL
             AND (
               EXISTS (
                 SELECT 1 FROM account_members am
                 WHERE am.account_id = ot.account_id
                   AND am.user_id = ot.owner_user_id
               )
               OR EXISTS (
                 SELECT 1 FROM accounts a
                 WHERE a.id = ot.account_id
                   AND a.clerk_org_id IS NULL
                   AND a.owner_user_id = ot.owner_user_id
               )
             )
    FROM oauth_tokens ot
    WHERE ot.provider = 'slack_user'
      AND ot.provider_workspace_id = p_workspace_id
      AND ot.provider_user_id = p_slack_user_id
      AND p_workspace_id <> ''
      AND p_slack_user_id <> ''
      AND ot.owner_user_id IS NOT NULL
    ORDER BY ot.account_id, ot.owner_user_id
$$;

REVOKE EXECUTE ON FUNCTION resolve_slack_user_token_owners(text, text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION resolve_slack_user_token_owners(text, text) TO coralswarm_app, CURRENT_USER;
