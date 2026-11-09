-- TENANT-TREE PAIR of control migration `0219_coding_sessions_agent_metadata`.
-- See that file's header for the rationale. DDL must stay byte-identical
-- (`scripts/check_schema_parity.sh`).
--
-- Down:
--   DROP INDEX IF EXISTS idx_coding_sessions_graph_parent;
--   DROP INDEX IF EXISTS idx_coding_sessions_graph_root;
--   DROP INDEX IF EXISTS idx_coding_sessions_graph_platform;
--   DROP INDEX IF EXISTS idx_coding_sessions_graph_agent_name;
--   ALTER TABLE coding_sessions
--     DROP COLUMN IF EXISTS subagent_name,
--     DROP COLUMN IF EXISTS root_session_id,
--     DROP COLUMN IF EXISTS parent_session_id,
--     DROP COLUMN IF EXISTS task,
--     DROP COLUMN IF EXISTS platform_detected,
--     DROP COLUMN IF EXISTS platform,
--     DROP COLUMN IF EXISTS agent_name;

ALTER TABLE coding_sessions
    ADD COLUMN IF NOT EXISTS agent_name        TEXT,
    ADD COLUMN IF NOT EXISTS platform          TEXT,
    ADD COLUMN IF NOT EXISTS platform_detected TEXT,
    ADD COLUMN IF NOT EXISTS task              TEXT,
    ADD COLUMN IF NOT EXISTS parent_session_id TEXT,
    ADD COLUMN IF NOT EXISTS root_session_id   TEXT,
    ADD COLUMN IF NOT EXISTS subagent_name     TEXT;

CREATE INDEX IF NOT EXISTS idx_coding_sessions_graph_agent_name
    ON coding_sessions (graph_id, agent_name)
    WHERE agent_name IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_coding_sessions_graph_platform
    ON coding_sessions (graph_id, platform)
    WHERE platform IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_coding_sessions_graph_root
    ON coding_sessions (graph_id, root_session_id)
    WHERE root_session_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_coding_sessions_graph_parent
    ON coding_sessions (graph_id, parent_session_id)
    WHERE parent_session_id IS NOT NULL;
