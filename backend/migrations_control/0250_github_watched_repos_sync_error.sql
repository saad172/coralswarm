-- Migration: 0250_github_watched_repos_sync_error
-- Description: `github_watched_repos.sync_error_code` / `sync_error_at` — the
--   per-repo "the last backfill tick failed" state the selection UI had no
--   way to render, so a dead job presented as healthy progress.
--
-- WHAT WENT WRONG WITHOUT IT
--   Both `GithubBackfill` jobs in production sat at `status = 'dead'` (three
--   attempts, `error_message = "ingest_atom failed: 400 Bad Request"` — the
--   D7 destination refusal this migration's PR fixes) while
--   `/connectors/github` rendered the repo rows as `backfilling` with a
--   healthy-looking counter line ("skipped as expected: not a file (272),
--   binary (55), …"). Every signal the page had was consistent with
--   progress and none of them was: those skip counts advance during the
--   code tier's TREE WALK, before any ingest is attempted, so they climb
--   whether or not a single atom ever lands; `code_cursor->>'done'` stays
--   `false` for the same reason; and `last_synced_at` staying NULL is
--   indistinguishable from "still working" on a first backfill. The job
--   row held the truth the whole time, on the wrong plane's `jobs` table and
--   behind no route.
--
-- WHY A CODE AND NOT THE ERROR TEXT
--   `routes::github::list_repos` serializes this column straight into its
--   API response, and the repo's guidance prohibits leaking internal error
--   detail that way (a worker error string can carry a Postgres detail line,
--   a GitHub error payload, a connection string fragment). So the column
--   holds a CLOSED SET of classification codes and the CHECK constraint
--   below is what makes that structural rather than a convention a future
--   writer can forget: a raw message cannot be stored here even by mistake,
--   because Postgres refuses the row.
--
--   The verbatim detail is not lost, it simply lives where operators already
--   look for it and end users never see it: `jobs.error_message` for the
--   failing job (the queue writes it on every `fail`), plus the worker's own
--   `tracing::warn!(error = %e, …)` line.
--
--   The codes, and why exactly these two:
--     'destination_unreachable' — the ocean this repo is routed into can no
--         longer be reached by the person who chose it (they left the team,
--         left the org, or the ocean was archived). ACTIONABLE by the admin
--         — re-route the repo — so it earns its own code. It is also the one
--         cause that can be established as a FACT rather than guessed from
--         an error string: `workers::github_backfill` re-runs the real
--         reachability predicate against the live row before recording it
--         (see `connectors::github_backfill::classify_tick_failure`). No
--         substring matching on messages anywhere.
--     'sync_failed'             — the honest catch-all. Everything else is
--         ours to fix, not the admin's, so a finer public taxonomy would be
--         precision the UI cannot act on and the classifier cannot justify.
--
-- WHY A COLUMN ON THE WATCH ROW RATHER THAN A JOIN TO `jobs`
--   `routes::github::list_repos` already reads `github_watched_repos` once
--   per installation and merges it with the live GitHub listing. Joining
--   `jobs` instead would mean re-deriving "which job is this repo's" from
--   `payload->>'repo_row_id'` (an unindexed JSONB predicate over a table
--   that is swept and pruned on its own schedule), and would make the
--   selection page's correctness depend on a dead job row still EXISTING —
--   which is exactly the kind of retention coupling that turns "no error
--   shown" back into "looks fine". The state belongs to the repo, is
--   written by the one worker that owns the repo's sync, and outlives any
--   individual job.
--
-- SEMANTICS
--   `sync_error_code` is the classification of the most recent tick that
--   failed, `sync_error_at` when it was recorded. BOTH are cleared (set
--   NULL, together) by the next tick that makes progress — a transient
--   rate-limit blip must not pin a permanent red badge on a repo that then
--   went on to sync. They are therefore "is this repo failing RIGHT NOW",
--   not an error history; the `jobs` table remains the audit trail.
--
--   Deliberately NOT paired with `active`/`paused_reason`: a failing repo is
--   still active and still scheduled (the job queue's own backoff ladder is
--   what retries it), so flipping `active = false` here would both lie and
--   stop the retries. `sync_stopped_at`/`paused_reason` keep their §3.8
--   meaning — "sync deliberately stopped, content kept" — and a paused repo
--   still wins the status badge ahead of a failing one, because pausing is
--   the state an admin chose.
--
--   Nullable, no default, no backfill: NULL means "no failure recorded",
--   which is the correct reading for every pre-existing row (the two dead
--   prod installs included — their next tick writes the real value, or
--   succeeds and leaves it NULL).
--
--   Paired with migrations_tenant/0102_github_watched_repos_sync_error.sql —
--   `github_watched_repos` is one of the shared content tables, so this must
--   stay byte-equivalent to that file's `ALTER`/`CHECK`/`COMMENT` or
--   `scripts/check_schema_parity.sh` fails.
--
-- Depends on: 0235_github_watched_repos.
-- Paired with: migrations_tenant/0102_github_watched_repos_sync_error.sql.
--
-- Down:
--   ALTER TABLE github_watched_repos DROP CONSTRAINT IF EXISTS github_watched_repos_sync_error_code_check;
--   ALTER TABLE github_watched_repos DROP COLUMN IF EXISTS sync_error_at;
--   ALTER TABLE github_watched_repos DROP COLUMN IF EXISTS sync_error_code;

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
