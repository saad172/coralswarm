-- Migration: 0045_rate_limit
-- Description: Postgres-backed shared rate limiter (audit S4). The in-process
--              DashMap limiter only throttles per VM, so the effective limit on
--              multi-replica Fly is 10×replicas. This fixed-window counter table
--              gives all replicas a single shared view: one row per
--              (key, window_start), atomically upserted+incremented per request,
--              allow while count <= limit. Opt-in via RATE_LIMIT_BACKEND=pg;
--              the default (memory) path never touches this table.
-- Depends on: nothing (key is an opaque limiter key, e.g. the user UUID as text;
--              no FKs since this is throwaway throttling state, not durable data).
-- Down:
--   DROP TABLE IF EXISTS rate_limit_window;

CREATE TABLE IF NOT EXISTS rate_limit_window (
    key          TEXT        NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    count        INTEGER     NOT NULL DEFAULT 0,
    PRIMARY KEY (key, window_start)
);

-- Cleanup scans: "delete all windows older than now - WINDOW_SECS".
CREATE INDEX IF NOT EXISTS idx_rate_limit_window_start
    ON rate_limit_window (window_start);
