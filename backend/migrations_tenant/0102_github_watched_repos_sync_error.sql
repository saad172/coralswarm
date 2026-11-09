-- Migration: 0102_github_watched_repos_sync_error (tenant tree)
-- Description: Tenant-tree half of the paired
--   `github_watched_repos.sync_error_code` / `sync_error_at` change. See
--   `migrations_control/0250_github_watched_repos_sync_error.sql` for the
--   full rationale — the dead-backfill-renders-as-`backfilling` incident,
--   why the column holds a CLOSED SET of classification codes rather than
--   the worker's error text (this value is serialized into an API
--   response), why the state lives on the watch row rather than being
--   joined out of `jobs`, and why both columns are cleared together by the
--   next progressing tick.
--
--   This is the tree that actually matters for the reported bug: both dead
--   installs are ORG accounts, so their `github_watched_repos` rows live on
--   a tenant database, not the control plane.
--
--   `github_watched_repos` is one of the content tables shared by both
--   trees, so this MUST stay byte-equivalent to the control side or
--   `scripts/check_schema_parity.sh` fails: it applies each tree to a
--   scratch DB and diffs a normalized `pg_dump --schema-only` of the shared
--   tables. Keep the column types, nullability, CHECK constraint and
--   `COMMENT` text identical to 0250.
--
--   Idempotent (`IF NOT EXISTS`) like every other tenant-tree column add,
--   since tenant databases are provisioned at different times and the
--   orchestrator may re-run a tree against a DB that is already partially
--   ahead.

ALTER TABLE github_watched_repos ADD COLUMN IF NOT EXISTS sync_error_code TEXT;
ALTER TABLE github_watched_repos ADD COLUMN IF NOT EXISTS sync_error_at TIMESTAMPTZ;

-- The closed set, enforced by the database rather than by convention: this
-- column is serialized into an API response, so "someone later stores the
-- raw error text here" has to be impossible, not merely discouraged.
-- `NOT VALID` is not used: the column was just added, so every existing row
-- is NULL and the constraint validates instantly with no scan cost.
ALTER TABLE github_watched_repos
    DROP CONSTRAINT IF EXISTS github_watched_repos_sync_error_code_check;
ALTER TABLE github_watched_repos
    ADD CONSTRAINT github_watched_repos_sync_error_code_check
    CHECK (sync_error_code IS NULL OR sync_error_code IN ('destination_unreachable', 'sync_failed'));

COMMENT ON COLUMN github_watched_repos.sync_error_code IS
    'Classification of the most recent GithubBackfill tick that failed, or '
    'NULL when this repo is not currently failing. A CLOSED SET (see the '
    'CHECK constraint): never the worker''s error text, because '
    'routes::github::list_repos serializes this value into its API response '
    'and internal error detail must not leak that way — the verbatim message '
    'lives in jobs.error_message and the worker''s own tracing line. Written '
    'by workers::github_backfill::process_job and CLEARED by the next tick '
    'that makes progress, so it answers "is this repo failing right now", '
    'never "has it ever failed".';

COMMENT ON COLUMN github_watched_repos.sync_error_at IS
    'When sync_error_code was recorded. Set and cleared together with it, '
    'never independently.';
