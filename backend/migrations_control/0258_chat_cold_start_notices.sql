-- Migration: 0258_chat_cold_start_notices
-- Description: Phase G item 25 for Google Chat. One row per space for the
--   in-space connect notice. Email is the normal invite. This post runs
--   only when a member has added the app and nobody in the account can be
--   reached by email, at most once a day, and at most five times in a
--   rolling 30 days. The first directory-capable connect retires it.
--   No message content.
--
--   Control-only, no tenant twin — same bookkeeping posture as
--   slack_unmatched_email_notices. account_isolation so a scoped worker
--   can claim a slot. Two SECURITY DEFINER helpers exist only because
--   REMOVED_FROM_SPACE and the hourly repeat must see every account's
--   row for one space, and that discovery has no account_id yet.
--
-- Depends on: 0257_org_install_capture_grace.
-- Down:
--   DROP FUNCTION IF EXISTS retire_chat_cold_start_container(text);
--   DROP FUNCTION IF EXISTS chat_cold_start_due_accounts();
--   DROP TABLE IF EXISTS chat_cold_start_notices;

CREATE TABLE chat_cold_start_notices (
    provider          TEXT        NOT NULL,
    account_id        UUID        NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    container_id      TEXT        NOT NULL,
    last_posted_at    TIMESTAMPTZ,
    posts_in_window   INTEGER     NOT NULL DEFAULT 0,
    window_started_at TIMESTAMPTZ,
    PRIMARY KEY (provider, account_id, container_id)
);

ALTER TABLE chat_cold_start_notices ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON chat_cold_start_notices;
CREATE POLICY account_isolation ON chat_cold_start_notices
    USING (account_id = current_setting('app.account_id')::uuid)
    WITH CHECK (account_id = current_setting('app.account_id')::uuid);

CREATE OR REPLACE FUNCTION retire_chat_cold_start_container(p_container_id text)
RETURNS void
LANGUAGE sql
SECURITY DEFINER
SET search_path = public, pg_temp
AS $$
    DELETE FROM chat_cold_start_notices
    WHERE provider = 'google_chat'
      AND container_id = p_container_id
      AND p_container_id <> ''
$$;

REVOKE EXECUTE ON FUNCTION retire_chat_cold_start_container(text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION retire_chat_cold_start_container(text) TO coralswarm_app, CURRENT_USER;

CREATE OR REPLACE FUNCTION chat_cold_start_due_accounts()
RETURNS TABLE(account_id uuid)
LANGUAGE sql
SECURITY DEFINER
STABLE
SET search_path = public, pg_temp
AS $$
    SELECT DISTINCT n.account_id
    FROM chat_cold_start_notices n
    WHERE n.provider = 'google_chat'
      AND n.last_posted_at IS NOT NULL
      AND n.last_posted_at <= now() - interval '24 hours'
$$;

REVOKE EXECUTE ON FUNCTION chat_cold_start_due_accounts() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION chat_cold_start_due_accounts() TO coralswarm_app, CURRENT_USER;
