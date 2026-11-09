-- Migration: 0003_atoms
-- Description: Enable pgvector extension and create atoms table
--
-- Down:
--   DROP TABLE IF EXISTS atoms;
--   DROP INDEX IF EXISTS idx_atoms_graph_timestamp;
--   DROP INDEX IF EXISTS idx_atoms_graph_source;
--   -- Note: do not drop the pgvector extension as other tables may use it

-- Enable pgvector (idempotent)
CREATE EXTENSION IF NOT EXISTS vector;

-- ── Graphs ────────────────────────────────────────────────────────────────────
-- A graph is a named knowledge space owned by an account.
-- Atoms belong to exactly one graph.
CREATE TABLE IF NOT EXISTS graphs (
    id         UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id UUID        NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    name       TEXT        NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ── Atoms ─────────────────────────────────────────────────────────────────────
-- An atom is a single ingested unit of knowledge.
-- Content is stored encrypted in R2 (blob_key), NOT inline.
CREATE TABLE IF NOT EXISTS atoms (
    atom_id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    graph_id         UUID        NOT NULL REFERENCES graphs(id) ON DELETE CASCADE,
    source_type      TEXT        NOT NULL,   -- e.g. 'slack', 'notion', 'zoom'
    source_id        TEXT        NOT NULL,   -- provider-native message/doc ID
    content_blob_key TEXT        NOT NULL,   -- R2 object key — content NOT stored here
    embedding        vector(384),            -- nullable until embedding job runs
    participants     TEXT[]      NOT NULL DEFAULT '{}',
    timestamp        TIMESTAMPTZ NOT NULL,   -- when the source event occurred
    ingested_by      UUID        REFERENCES users(id) ON DELETE SET NULL,
    scope            TEXT        NOT NULL DEFAULT 'private',  -- 'private' | 'account'
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Prevent duplicate ingestion of the same source event in the same graph
    UNIQUE (graph_id, source_type, source_id)
);

-- ── Indexes ───────────────────────────────────────────────────────────────────
-- Primary query patterns: latest atoms in a graph, atoms by source type
CREATE INDEX IF NOT EXISTS idx_atoms_graph_timestamp
    ON atoms (graph_id, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_atoms_graph_source
    ON atoms (graph_id, source_type);

-- Vector similarity index (cosine distance, HNSW for low-latency ANN search)
-- Built once enough atoms exist; HNSW is preferred over ivfflat (no training needed)
-- Parameters: m=16, ef_construction=64 are solid defaults for 384-dim embeddings
CREATE INDEX IF NOT EXISTS idx_atoms_embedding_hnsw
    ON atoms
    USING hnsw (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 64);
