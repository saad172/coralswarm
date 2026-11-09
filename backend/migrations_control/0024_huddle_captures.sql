-- Migration 0024: slack_huddle_captures table
-- Tracks reaction-based opt-in for Slack huddle notes ingestion.
-- Default OFF: a huddle is only captured when a user reacts with :coral:
-- or :coralswarm: to the huddle message.
--
-- Down:
--   DROP INDEX IF EXISTS idx_huddle_captures_pending;
--   DROP INDEX IF EXISTS idx_huddle_captures_account;
--   DROP TABLE IF EXISTS slack_huddle_captures;

CREATE TABLE IF NOT EXISTS slack_huddle_captures (
    id                 UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id         UUID        NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    channel_id         TEXT        NOT NULL,
    huddle_thread_ts   TEXT        NOT NULL,  -- Slack message ts of the huddle-start message
    opted_by           TEXT        NOT NULL,  -- Slack user ID who reacted
    status             TEXT        NOT NULL DEFAULT 'pending'
                                   CHECK (status IN ('pending', 'processing', 'processed', 'failed', 'cancelled')),
    error              TEXT,                  -- set on status='failed'
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    processed_at       TIMESTAMPTZ,           -- set on status='processed'

    UNIQUE (account_id, channel_id, huddle_thread_ts)
);

-- Allow the worker to find pending rows efficiently.
CREATE INDEX IF NOT EXISTS idx_huddle_captures_pending
    ON slack_huddle_captures (created_at)
    WHERE status = 'pending';

-- Allow account-scoped lookups.
CREATE INDEX IF NOT EXISTS idx_huddle_captures_account
    ON slack_huddle_captures (account_id);
