-- Migration: 0035_relationship_edges
-- Description: Typed relationship edges between entity reefs, with inference + review
--              support (ADR-0008). Edges carry an origin (stated vs inferred), a review
--              status (suggested/confirmed/rejected), a confidence, and provenance
--              (stated: source atom_ids; inferred: derivation chain + composition rule).
-- Depends on: 0011_nodes (nodes), 0003_atoms (graphs)
-- Down:
--   DROP TABLE IF EXISTS relationship_edge;

CREATE TABLE IF NOT EXISTS relationship_edge (
    edge_id      UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    graph_id     UUID        NOT NULL REFERENCES graphs(id)     ON DELETE CASCADE,
    src_node_id  UUID        NOT NULL REFERENCES nodes(node_id) ON DELETE CASCADE,
    predicate    TEXT        NOT NULL,
    dst_node_id  UUID        NOT NULL REFERENCES nodes(node_id) ON DELETE CASCADE,
    origin       TEXT        NOT NULL DEFAULT 'stated'
                 CHECK (origin IN ('stated', 'inferred')),
    status       TEXT        NOT NULL DEFAULT 'suggested'
                 CHECK (status IN ('suggested', 'confirmed', 'rejected')),
    confidence   REAL        NOT NULL DEFAULT 1.0,
    -- stated:   { "atom_ids": ["<uuid>", ...] }
    -- inferred: { "rule": "spouse_of∘parent_of→parent_of", "source_edge_ids": ["<uuid>", ...] }
    provenance   JSONB       NOT NULL DEFAULT '{}'::jsonb,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    version      INTEGER     NOT NULL DEFAULT 1,

    -- No self-loops; a single directed (predicate) edge per ordered pair per ocean.
    CONSTRAINT relationship_edge_no_self CHECK (src_node_id <> dst_node_id),
    CONSTRAINT relationship_edge_unique  UNIQUE (graph_id, src_node_id, predicate, dst_node_id)
);

-- Traversal from either endpoint (multi-hop subgraph expansion at query time).
CREATE INDEX IF NOT EXISTS idx_relationship_edge_src
    ON relationship_edge (src_node_id);
CREATE INDEX IF NOT EXISTS idx_relationship_edge_dst
    ON relationship_edge (dst_node_id);

-- Review-queue scans: "all suggested edges in this ocean".
CREATE INDEX IF NOT EXISTS idx_relationship_edge_graph_status
    ON relationship_edge (graph_id, status);
