-- Migration: 0011_nodes
-- Description: Knowledge-graph node tables — nodes, node_atoms, supersession_edge, non_supersession_pair
--
-- Down:
--   DROP TABLE IF EXISTS non_supersession_pair;
--   DROP TABLE IF EXISTS supersession_edge;
--   DROP TABLE IF EXISTS node_atoms;
--   DROP TABLE IF EXISTS nodes;

-- ── Node type enum ─────────────────────────────────────────────────────────────
-- Four types of knowledge-graph nodes.  Stored as TEXT to stay migration-friendly.
-- Valid values: 'entity' | 'decision' | 'thread' | 'reference'

-- ── Nodes ─────────────────────────────────────────────────────────────────────
-- A node is a higher-level knowledge-graph object.
-- Multiple atoms are grouped into a node.
-- Per-type JSONB schema is validated in application code.
-- Exception: decisions require a decided_at timestamp, enforced here.
CREATE TABLE IF NOT EXISTS nodes (
    node_id    UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    graph_id   UUID        NOT NULL REFERENCES graphs(id) ON DELETE CASCADE,
    type       TEXT        NOT NULL CHECK (type IN ('entity', 'decision', 'thread', 'reference')),
    title      TEXT        NOT NULL,
    body       JSONB       NOT NULL DEFAULT '{}',
    scope      TEXT        NOT NULL DEFAULT 'private',  -- 'private' | 'account'
    owner_id   UUID        REFERENCES users(id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    version    INTEGER     NOT NULL DEFAULT 1,

    -- Decisions must record when the decision was made
    CONSTRAINT decisions_require_decided_at
        CHECK (type <> 'decision' OR (body->>'decided_at') IS NOT NULL)
);

-- ── Indexes on nodes ──────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_nodes_graph_type
    ON nodes (graph_id, type);

CREATE INDEX IF NOT EXISTS idx_nodes_graph_updated_at
    ON nodes (graph_id, updated_at DESC);

-- ── Node ↔ Atom join ──────────────────────────────────────────────────────────
-- Many-to-many: one atom can belong to multiple nodes; one node can span many atoms.
CREATE TABLE IF NOT EXISTS node_atoms (
    node_id  UUID NOT NULL REFERENCES nodes(node_id) ON DELETE CASCADE,
    atom_id  UUID NOT NULL REFERENCES atoms(atom_id) ON DELETE CASCADE,

    PRIMARY KEY (node_id, atom_id)
);

-- ── Indexes on node_atoms ─────────────────────────────────────────────────────
-- PK already covers (node_id, atom_id); add reverse-lookup indexes.
CREATE INDEX IF NOT EXISTS idx_node_atoms_node_id
    ON node_atoms (node_id);

CREATE INDEX IF NOT EXISTS idx_node_atoms_atom_id
    ON node_atoms (atom_id);

-- ── Supersession edges ────────────────────────────────────────────────────────
-- Records that one node (superseder) replaces another (superseded).
-- confirmed_by / confirmed_at are nullable — not yet confirmed if NULL.
-- source is free-form text, e.g. 'auto' (model-detected) or 'user' (human-confirmed).
CREATE TABLE IF NOT EXISTS supersession_edge (
    superseded_id  UUID        NOT NULL REFERENCES nodes(node_id) ON DELETE CASCADE,
    superseder_id  UUID        NOT NULL REFERENCES nodes(node_id) ON DELETE CASCADE,
    confirmed_by   UUID        REFERENCES users(id) ON DELETE SET NULL,
    confirmed_at   TIMESTAMPTZ,
    source         TEXT        NOT NULL,

    PRIMARY KEY (superseded_id, superseder_id)
);

-- ── Indexes on supersession_edge ─────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_supersession_edge_superseded_id
    ON supersession_edge (superseded_id);

CREATE INDEX IF NOT EXISTS idx_supersession_edge_superseder_id
    ON supersession_edge (superseder_id);

-- ── Non-supersession pairs ────────────────────────────────────────────────────
-- Explicit markers that two nodes are NOT superseding each other.
-- Prevents the model from incorrectly flagging them as superseding.
-- Order-insensitive uniqueness: callers must always insert with node_a_id < node_b_id.
CREATE TABLE IF NOT EXISTS non_supersession_pair (
    node_a_id  UUID        NOT NULL REFERENCES nodes(node_id) ON DELETE CASCADE,
    node_b_id  UUID        NOT NULL REFERENCES nodes(node_id) ON DELETE CASCADE,
    marked_by  UUID        REFERENCES users(id) ON DELETE SET NULL,
    marked_at  TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Enforce canonical ordering so (A,B) and (B,A) cannot both exist
    CONSTRAINT non_supersession_pair_ordered CHECK (node_a_id < node_b_id),
    UNIQUE (node_a_id, node_b_id)
);
