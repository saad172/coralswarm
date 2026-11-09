-- Migration: 0076_coding_sessions
-- Description: Coding-session capture, Phase 1 (audit amendment A1/A4).
--
--   `coding_sessions` is a `meeting_transcripts` sibling, not a graph node —
--   the architecture verdict rejected a `project` reef type outright (the
--   libsql `nodes.type` CHECK is frozen into every existing desktop SQLite
--   file with no retry-safe migration path, and an unguarded `REEF_META`
--   lookup on web/desktop would TypeError-crash the Reefs page the first
--   time a project node appeared). One row per `(graph_id, session_id)` —
--   an AI coding-agent session (Claude Code, etc.) captured via the MCP
--   `add_context` write tool. Lazily upserted on the FIRST `add_context` call
--   that carries a `session_id`; there is no explicit "close" — the row is
--   just as fresh as its `last_activity_at`.
--
--   Session-constant metadata (`project_key`, `title`, `repo_path`,
--   `remote_sanitized`, `hostname`, `harness_version`, `model`) lives here,
--   NOT on `atoms` — of the fields the original design would have added to
--   `atoms`, none survive there: they're either session-constant (belong on
--   this row) or per-checkpoint or (`branch`), which rides the existing 0066
--   contextual-header mechanism (embedded + FTS-indexed text, stripped on
--   display) instead of a new atom column. The atoms schema is UNCHANGED by
--   this migration. Per-coral linkage to a session is NOT a foreign key
--   either — it rides the `source_id` convention
--   (`"cc:{session_id}:{checkpoint_id}"`, chunked further to
--   `"...:chunk:{n}"` by the existing ingest chunking step), because
--   `source_id` — unlike any new provenance column — is already part of the
--   atom sync payload (reaches desktop for free) and already covered by the
--   0074 prefix-LIKE index.
--
--   `raw_blob_key` / `raw_inline` are RESERVED, UNUSED columns (audit A9):
--   raw coding-session log capture is explicitly deferred — a per-checkpoint
--   re-upsert of a single "whole session" blob would orphan R2 blobs (unlike
--   `meeting_transcripts`, a coding session has no single terminal ingest
--   event), and the summary-generation debounce logic
--   (`GenerateMeetingSummary`) assumes a one-shot terminal transcript. Do not
--   write to these columns until that's designed.
--
--   `projects` is a small alias/dictionary table, upserted lazily whenever a
--   session first references a `project_key` (never pre-populated) — the
--   audit's P1-4: because `project_key` is frozen forever once stamped (no
--   bulk rekey path, excluded from anything sync-based), every future key
--   split (repo rename, fork, basename → real remote, a v2 normalizer) needs
--   a cheap alias-merge path instead of a permanent fork. This table must
--   exist before any key is ever stamped — hence landing in the same
--   migration as `coding_sessions`, not a later one.
--
--   Cloud-only (Postgres), same tier restriction as `meeting_transcripts`:
--   the libsql (desktop) `Store` impl gets empty-stub methods (see
--   `store/libsql.rs`'s meeting-transcript stub block for the exact
--   precedent) — present only so the `Store` trait object stays complete.
--
-- Depends on: 0003_atoms (graphs).
-- Down:
--   DROP TABLE IF EXISTS coding_sessions;
--   DROP TABLE IF EXISTS projects;

-- ── Project alias/dictionary table (must exist before any key is stamped) ──
CREATE TABLE IF NOT EXISTS projects (
    graph_id     UUID        NOT NULL REFERENCES graphs(id) ON DELETE CASCADE,
    project_key  TEXT        NOT NULL,
    -- User-editable label for display (e.g. "CoralSwarm"). Defaults to the
    -- key itself on first upsert; never overwritten by a later upsert once
    -- set (COALESCE-preserve — see `Store::upsert_coding_session`), so an
    -- explicit user edit here is never silently clobbered by ingest.
    display_name TEXT,
    -- Other keys known to refer to the SAME project (repo renames, basename
    -- → real-remote splits, a future normalizer version). Phase 1 never
    -- writes to this — reserved for the future re-key/merge tooling the
    -- alias table exists to support.
    aliases      TEXT[]      NOT NULL DEFAULT '{}',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (graph_id, project_key)
);

-- ── One row per captured coding-agent session ───────────────────────────────
CREATE TABLE IF NOT EXISTS coding_sessions (
    id                UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    graph_id          UUID        NOT NULL REFERENCES graphs(id) ON DELETE CASCADE,
    -- Caller-supplied session identifier (e.g. a Claude Code session uuid).
    -- Unique per graph — see the UNIQUE constraint below, which is also what
    -- `Store::upsert_coding_session`'s single-statement `ON CONFLICT`
    -- upsert targets (audit P1-3: avoids a check-then-write race across the
    -- stateless multi-replica `/mcp` transport).
    session_id        TEXT        NOT NULL,
    project_key       TEXT,
    title             TEXT,
    repo_path         TEXT,
    -- The git remote AFTER `project_key::normalize_remote` (userinfo/
    -- credentials stripped) — never the raw caller-supplied remote. Reused
    -- for display so a raw credential-bearing remote is structurally unable
    -- to reach this column either.
    remote_sanitized  TEXT,
    hostname          TEXT,
    harness_version   TEXT,
    model             TEXT,
    started_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_activity_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- RESERVED, UNUSED (audit A9) — see header comment. Do not write here yet.
    raw_blob_key      TEXT,
    raw_inline        TEXT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (graph_id, session_id)
);

CREATE INDEX IF NOT EXISTS idx_coding_sessions_graph_project
    ON coding_sessions (graph_id, project_key);

CREATE INDEX IF NOT EXISTS idx_coding_sessions_graph_last_activity
    ON coding_sessions (graph_id, last_activity_at DESC);
