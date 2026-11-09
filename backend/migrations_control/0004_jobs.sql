-- Migration: 0004_jobs
-- Description: Background job queue backed by Postgres SELECT … FOR UPDATE SKIP LOCKED
--
-- Down:
--   DROP TABLE IF EXISTS jobs;
--   DROP TYPE IF EXISTS job_status;

CREATE TYPE job_status AS ENUM ('pending', 'running', 'done', 'failed', 'dead');

CREATE TABLE IF NOT EXISTS jobs (
    id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    kind            TEXT        NOT NULL,   -- job type: 'embed_atoms', 'extract_entities', etc.
    payload         JSONB       NOT NULL,
    status          job_status  NOT NULL DEFAULT 'pending',
    idempotency_key TEXT        UNIQUE,     -- NULL allowed; unique when set
    attempts        INT         NOT NULL DEFAULT 0,
    max_attempts    INT         NOT NULL DEFAULT 3,
    run_after       TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at      TIMESTAMPTZ,
    finished_at     TIMESTAMPTZ,
    error_message   TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_jobs_pending
    ON jobs (run_after, id)
    WHERE status = 'pending';
