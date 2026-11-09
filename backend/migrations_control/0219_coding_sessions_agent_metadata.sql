-- Migration: 0219_coding_sessions_agent_metadata
-- Description: Identity + roll-up metadata for AGENT sessions
--   (`routes::ingest::SessionKind::Agent`, wire `agent`, derived
--   `atoms.source_type = 'agent_session'`).
--
--   The kind itself needs no schema change (same rationale as `meeting` in
--   0185: `session_kind` is free TEXT closed over in Rust). What an
--   autonomous agent needs on top of a coding session is WHO it is, WHERE it
--   runs, WHAT it is doing, and — when it spawns helpers — which agent those
--   helpers belong to. All nullable, all TEXT, no CHECK constraints: the
--   vocabulary is open on purpose (a new platform must not need a deploy;
--   the server canonicalizes spellings in `services::platform`).
--
--   Columns:
--     agent_name         What the agent calls itself. COALESCE-refinable
--                        across checkpoints like `title`.
--     platform           Where it runs, canonical slug (`claude-code`,
--                        `cursor`, `codex`, ...). COALESCE-refinable.
--                        REQUIRED for kind=agent at ingest (declared or
--                        detected) — enforced in Rust, not here, so legacy
--                        rows and the other kinds stay NULL.
--     platform_detected  What the request's User-Agent said the harness was,
--                        recorded independently so a declared value is never
--                        silently rewritten; the API derives
--                        `platform_verified = platform_detected IS NULL OR
--                        platform_detected = platform`. INSERT-only.
--     task               What it is working on (free text / `CS-NNN`).
--                        COALESCE-refinable.
--     parent_session_id  The spawning agent session, when this is a
--                        subagent. INSERT-only (a link is fixed at the
--                        child's first checkpoint; re-parenting is out of
--                        scope).
--     root_session_id    The top of the tree this session belongs to. A root
--                        is its own root. INSERT-only. NULL on rows written
--                        before this migration; readers treat
--                        `COALESCE(root_session_id, session_id)` as the root.
--     subagent_name      The name a subagent declared for itself. Children
--                        INHERIT `agent_name`/`platform` from the root so
--                        every filter on the root matches the whole tree;
--                        the child's own declared name is kept here for
--                        display/tracing only. COALESCE-refinable.
--
--   Indexes back the new list/search filters (`agent_name`, `platform`) and
--   the roll-up reads (`root_session_id` for tree-wide counts and
--   `search_atoms root_session_id=`, `parent_session_id` for "direct
--   children of X"). Partial on NOT NULL: the overwhelming majority of rows
--   (every coding/work/meeting session) never sets them.
--
--   Paired tenant migration: `0087_coding_sessions_agent_metadata.sql`
--   (byte-identical DDL — `scripts/check_schema_parity.sh`).
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
