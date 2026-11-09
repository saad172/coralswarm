-- Migration 0281: resolve_slack_user_token_owners(text, text) — the connected
--   members behind one Slack user id in one Slack workspace (control-only,
--   no tenant twin).
--
--   `connectors::slack_member_capture::apply_membership_event` handles
--   Slack's `member_joined_channel` / `member_left_channel` events, which
--   carry only the workspace id and the Slack user id. It used to join
--   `oauth_tokens` directly on the control pool. `oauth_tokens` is
--   RLS-covered (0083) with a strict `current_setting('app.account_id')`
--   predicate, so as `coralswarm_app` that statement hard-errored and the
--   member channel index (`slack_member_channels`, 0256, no RLS) never
--   followed a join or a leave. There is no account id to scope by before
--   the lookup, so this is the same bootstrap-resolver shape as 0275's
--   `resolve_gmail_watch_owners(text)`.
--
--   Narrow by design: one provider (`slack_user`), one workspace, one Slack
--   user id, member-owned rows only. It returns ids and a liveness flag,
--   never scopes or token material. `credential_live` is returned rather
--   than filtered on because a leave must still remove the index row after
--   the token was marked invalid (a join indexes live tokens only).
--
--   Control-only: `slack_member_channels` (its only consumer's write target)
--   is in `check_schema_parity.sh`'s CONTROL_TABLES, and Slack event routing
--   runs on the control plane.
--
-- Depends on: 0175_oauth_tokens_owner_user_id.sql (owner_user_id),
--             0251_oauth_tokens_credential_invalid.sql (credential_invalid_at).
-- Down:
--   REVOKE EXECUTE ON FUNCTION resolve_slack_user_token_owners(text, text) FROM coralswarm_app;
--   DROP FUNCTION IF EXISTS resolve_slack_user_token_owners(text, text);

CREATE OR REPLACE FUNCTION resolve_slack_user_token_owners(p_workspace_id text, p_slack_user_id text)
RETURNS TABLE(account_id uuid, owner_user_id uuid, credential_live boolean)
LANGUAGE sql
SECURITY DEFINER
STABLE
SET search_path = public, pg_temp
AS $$
    SELECT ot.account_id, ot.owner_user_id, ot.credential_invalid_at IS NULL
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
GRANT EXECUTE ON FUNCTION resolve_slack_user_token_owners(text, text) TO coralswarm_app;
