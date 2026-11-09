-- Migration 0368: oauth_grant_held_elsewhere(...) — control-only SECURITY
-- DEFINER predicate for member offboarding's upstream token revoke
-- (settings launch PR 2.8).
--
-- When a member leaves an org, `connectors::member_offboarding` deletes the
-- org account's copy of their per-member `oauth_tokens` rows and then asks the
-- provider to revoke the credential (Google `oauth2/revoke`, Slack
-- `auth.revoke`). Both of those revoke MORE than the one copy we held:
--
--   * Google revokes the whole grant for (Google user, OAuth client). The same
--     Google identity connected under the member's PERSONAL account (or under
--     a second org) shares that grant, so revoking the org's copy would
--     silently kill a connection the member still owns.
--   * A Slack user token is stable per (user, app); a second CoralSwarm row
--     for the same Slack user can hold the identical token.
--
-- So the revoke only runs when no OTHER row still holds the same provider
-- identity. That question spans accounts, and `oauth_tokens` is RLS-isolated
-- per account on the control plane (0083; enforced in production since
-- OPS-4), so it needs the same audited SECURITY DEFINER carve-out shape as
-- `resolve_google_identity_by_sub` (0198) and `resolve_gmail_watch_owners`.
-- It returns ONE boolean and never a row, an account id or a credential.
--
-- Answers TRUE ("held elsewhere — do not revoke") when ANY of:
--   * the departing row carries NEITHER identifier (rows written before
--     2026-07-16 can have an empty Google `sub` and email): with nothing to
--     match on, whether the grant is shared is unknowable, so never revoke;
--   * another row in `p_providers` has the same non-empty
--     `provider_user_id` (Google `sub`, Slack user id);
--   * when `p_match_workspace`, another row has the same non-empty
--     `provider_workspace_id` (Google stores the mailbox address there;
--     Slack stores the TEAM id, which many members share, so Slack callers
--     pass false);
--   * the SAME CoralSwarm user holds any other row in `p_providers` — as its
--     owner (`owner_user_id`), or as the owner of the account it sits on (a
--     personal account's account-wide Calendar row has no owner_user_id).
--     Their personal connection to the same provider may be the very grant
--     the org's copy came from, even when identifiers differ or are empty.
-- Each excludes only the departing (account, owner) pair. Deliberately
-- broad: a false TRUE only skips a best-effort upstream call; a false FALSE
-- would revoke a grant the person still uses.
--
-- Down:
--   REVOKE EXECUTE ON FUNCTION oauth_grant_held_elsewhere(text[], text, text, boolean, uuid, uuid) FROM coralswarm_app, CURRENT_USER;
--   DROP FUNCTION IF EXISTS oauth_grant_held_elsewhere(text[], text, text, boolean, uuid, uuid);

CREATE OR REPLACE FUNCTION oauth_grant_held_elsewhere(
    p_providers             text[],
    p_provider_user_id      text,
    p_provider_workspace_id text,
    p_match_workspace       boolean,
    p_account_id            uuid,
    p_owner_user_id         uuid
)
RETURNS boolean
LANGUAGE sql
SECURITY DEFINER
STABLE
SET search_path = public, pg_temp
AS $$
    SELECT (COALESCE(p_provider_user_id, '') = '' AND COALESCE(p_provider_workspace_id, '') = '')
        OR EXISTS (
        SELECT 1
        FROM oauth_tokens ot
        WHERE ot.provider = ANY (p_providers)
          AND NOT (ot.account_id = p_account_id
                   AND ot.owner_user_id IS NOT DISTINCT FROM p_owner_user_id)
          AND (
                (COALESCE(p_provider_user_id, '') <> ''
                 AND ot.provider_user_id = p_provider_user_id)
             OR (p_match_workspace
                 AND COALESCE(p_provider_workspace_id, '') <> ''
                 AND ot.provider_workspace_id = p_provider_workspace_id)
             OR ot.owner_user_id = p_owner_user_id
             OR EXISTS (SELECT 1 FROM accounts a
                        WHERE a.id = ot.account_id AND a.owner_user_id = p_owner_user_id)
          )
    )
$$;

REVOKE EXECUTE ON FUNCTION oauth_grant_held_elsewhere(text[], text, text, boolean, uuid, uuid) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION oauth_grant_held_elsewhere(text[], text, text, boolean, uuid, uuid) TO coralswarm_app, CURRENT_USER;
