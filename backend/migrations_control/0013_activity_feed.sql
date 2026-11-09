-- Migration: 0013_activity_feed
-- Description: Add activity_events table for per-account audit/activity feed
--
-- Down:
--   DROP TABLE IF EXISTS activity_events;

CREATE TABLE IF NOT EXISTS activity_events (
    id          UUID        NOT NULL DEFAULT gen_random_uuid(),
    account_id  UUID        NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    user_id     UUID                 REFERENCES users(id)    ON DELETE SET NULL,
    graph_id    UUID                 REFERENCES graphs(id)   ON DELETE CASCADE,
    event_type  TEXT        NOT NULL CHECK (event_type IN ('atom_created', 'query_run', 'node_created', 'graph_created')),
    payload     JSONB       NOT NULL DEFAULT '{}',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (id)
);

-- Primary feed query: recent activity for an account
CREATE INDEX IF NOT EXISTS idx_activity_events_account_created
    ON activity_events (account_id, created_at DESC);

-- Filtered feed: activity for an account by event type
CREATE INDEX IF NOT EXISTS idx_activity_events_account_type_created
    ON activity_events (account_id, event_type, created_at DESC);

-- Graph-scoped feed (partial index — only rows with a graph_id)
CREATE INDEX IF NOT EXISTS idx_activity_events_graph_created
    ON activity_events (graph_id, created_at DESC)
    WHERE graph_id IS NOT NULL;
