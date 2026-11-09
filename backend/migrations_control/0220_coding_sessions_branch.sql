-- Migration: 0220_coding_sessions_branch
-- Description: Persist the current git branch on `coding_sessions` so the
--   library list can filter on it.
--
--   `branch` has always been accepted on ingest (`add_context` / HTTP) and
--   embedded in the contextual chunk header (migration 0066) so each
--   checkpoint's FTS document can name the branch at capture time. It was
--   deliberately LEFT OFF the session row because it is per-checkpoint and
--   may change across calls within the same session (0076's header).
--
--   A real list filter cannot join every coral's header, so the row now
--   stores the LAST REPORTED value: INSERT on the first checkpoint, then
--   `COALESCE(EXCLUDED.branch, coding_sessions.branch)` on every later one
--   (overwrite when a checkpoint sends a branch; never blank when it omits
--   one). The header path is unchanged — per-checkpoint history still rides
--   the atoms.
--
--   Nullable TEXT, no CHECK: branch names are free-form. Partial index on
--   `(graph_id, branch)` backs `GET /v1/sessions?branch=` / MCP
--   `list_sessions` the same way 0219 indexed `agent_name` / `platform`.
--
--   Paired tenant migration: `0088_coding_sessions_branch.sql`
--   (byte-identical DDL — `scripts/check_schema_parity.sh`).
--
-- Down:
--   DROP INDEX IF EXISTS idx_coding_sessions_graph_branch;
--   ALTER TABLE coding_sessions DROP COLUMN IF EXISTS branch;

ALTER TABLE coding_sessions
    ADD COLUMN IF NOT EXISTS branch TEXT;

CREATE INDEX IF NOT EXISTS idx_coding_sessions_graph_branch
    ON coding_sessions (graph_id, branch)
    WHERE branch IS NOT NULL;
