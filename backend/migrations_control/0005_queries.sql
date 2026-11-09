-- Migration 0005: queries table
-- Stores every query submitted by users; populated at request time and
-- updated once the pipeline completes.

CREATE TABLE IF NOT EXISTS queries (
    id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id   UUID        NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    user_id      UUID        NOT NULL REFERENCES users(id)    ON DELETE CASCADE,
    text         TEXT        NOT NULL,
    scope        TEXT        NOT NULL DEFAULT 'personal',
    status       TEXT        NOT NULL DEFAULT 'pending',  -- 'pending' | 'complete' | 'failed'
    answer       TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at  TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_queries_account ON queries (account_id, created_at DESC);
