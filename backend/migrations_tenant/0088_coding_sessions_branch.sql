-- TENANT-TREE PAIR of control migration `0220_coding_sessions_branch`.
-- See that file's header for the rationale. DDL must stay byte-identical
-- (`scripts/check_schema_parity.sh`).
--
-- Down:
--   DROP INDEX IF EXISTS idx_coding_sessions_graph_branch;
--   ALTER TABLE coding_sessions DROP COLUMN IF EXISTS branch;

ALTER TABLE coding_sessions
    ADD COLUMN IF NOT EXISTS branch TEXT;

CREATE INDEX IF NOT EXISTS idx_coding_sessions_graph_branch
    ON coding_sessions (graph_id, branch)
    WHERE branch IS NOT NULL;
