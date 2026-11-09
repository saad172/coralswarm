-- Migration 0179: Resumable Graph backfill cursors (m365 Train 2 PR 2.6)
--
-- Kills the silent-truncation debt in `connectors::microsoft`
-- (`MAX_CONVERSATION_MESSAGES`, `BACKFILL_SCAN_CAP`, `TEAMS_BACKFILL_CAP`; see
-- the plan's "Corrections to the decision record" entry) by giving each
-- backfillable resource somewhere to persist HOW FAR a capped Graph fetch got,
-- so the next pass resumes instead of the remainder being dropped with only a
-- log line. Two columns, same shape, on every table a backfill runs against:
--
--   * `backfill_cursor`   — the absolute Graph `@odata.nextLink` to resume
--                           from. NULL means "nothing to resume" (either never
--                           started, or finished).
--   * `backfill_complete` — explicit, queryable signal that the FULL upstream
--                           collection has been captured, not just "the last
--                           fetch didn't error". Existing rows default to
--                           `TRUE`: they were created before this migration
--                           and their backfill already ran to whatever the
--                           old hard cap allowed with no way to tell whether
--                           it was truncated, so we do not retroactively
--                           reopen them as "incomplete" (that would kick off
--                           surprise re-backfills across the estate). New
--                           rows opt into `FALSE` explicitly at creation time
--                           until their first fetch proves otherwise.
--
-- `outlook_workflows` gets the same two columns for its own bounded mailbox
-- scan (`BACKFILL_SCAN_CAP`, used at rule-creation time to find pre-existing
-- matches). Continuation is a control-plane resumable-fetch pass, not a
-- Graph-native cursor resume for that path specifically (search-folder
-- `nextLink`s are scoped to an ephemeral folder id) — see
-- `workers::graph_subscriptions::tick_backfill_continue`'s module doc for why
-- a full idempotent rescan is the safe continuation there.

ALTER TABLE outlook_watched_conversations
    ADD COLUMN IF NOT EXISTS backfill_cursor   TEXT,
    ADD COLUMN IF NOT EXISTS backfill_complete BOOLEAN NOT NULL DEFAULT TRUE;

ALTER TABLE teams_watched_resources
    ADD COLUMN IF NOT EXISTS backfill_cursor   TEXT,
    ADD COLUMN IF NOT EXISTS backfill_complete BOOLEAN NOT NULL DEFAULT TRUE;

ALTER TABLE outlook_workflows
    ADD COLUMN IF NOT EXISTS backfill_cursor   TEXT,
    ADD COLUMN IF NOT EXISTS backfill_complete BOOLEAN NOT NULL DEFAULT TRUE;

-- Continuation sweep work-set: "give me the incomplete ones for this account".
-- Partial (only rows actually pending) so the common steady-state case (nearly
-- everything complete) stays cheap to scan.
CREATE INDEX IF NOT EXISTS idx_outlook_watched_conversations_backfill_pending
    ON outlook_watched_conversations (account_id)
    WHERE NOT backfill_complete;

CREATE INDEX IF NOT EXISTS idx_teams_watched_resources_backfill_pending
    ON teams_watched_resources (account_id)
    WHERE NOT backfill_complete;

CREATE INDEX IF NOT EXISTS idx_outlook_workflows_backfill_pending
    ON outlook_workflows (account_id)
    WHERE NOT backfill_complete;
