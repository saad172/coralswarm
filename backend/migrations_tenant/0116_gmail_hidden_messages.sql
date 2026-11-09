-- Migration: 0116_gmail_hidden_messages (tenant twin of control 0277)
-- Description: Gmail trash/spam tombstones for an org member's mailbox —
--   see `migrations_control/0277_gmail_hidden_messages.sql`'s header for
--   what a row means (one coral a reversible hide retracted, keyed by the
--   ocean it came out of) and why the restore needs it. Same columns, same
--   key. Lives on this tree because the atoms it describes do.
--
--   Tenant-tree conventions (`0001_tenant_baseline.sql`'s header):
--   `account_id`/`owner_user_id` are bare uuids (accounts/users are
--   control-only); `graph_id` is a real FK, `graphs` exists here.
--
--   RLS: `member_ocean_isolation` anchored on `graph_id` via
--   `tenant_rls_visible_graph_ids()`, the same shape as
--   `gmail_watched_threads` (0066) and `gmail_everything_mode` (0074).
--
-- Depends on: 0066_google_gmail_watch_owner_and_threads,
--             0074_gmail_everything_mode, 0009_tenant_rls
--             (tenant_rls_visible_graph_ids()).
-- Down:
--   DROP POLICY IF EXISTS member_ocean_isolation ON gmail_hidden_messages;
--   DROP TABLE IF EXISTS gmail_hidden_messages;

CREATE TABLE IF NOT EXISTS gmail_hidden_messages (
    account_id     UUID        NOT NULL,
    graph_id       UUID        NOT NULL REFERENCES graphs(id) ON DELETE CASCADE,
    owner_user_id  UUID        NOT NULL,
    thread_id      TEXT        NOT NULL,
    message_id     TEXT        NOT NULL,
    hidden_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (account_id, graph_id, message_id)
);

CREATE INDEX IF NOT EXISTS idx_gmail_hidden_messages_account_message
    ON gmail_hidden_messages (account_id, message_id);

ALTER TABLE gmail_hidden_messages ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS member_ocean_isolation ON gmail_hidden_messages;
CREATE POLICY member_ocean_isolation ON gmail_hidden_messages
    USING (graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids()));
