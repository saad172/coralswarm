-- Migration: 0052_atoms_author_index
-- Description: Index atoms by (graph_id, author_id) so the "most active people"
--   aggregate dimension (see backend/src/store/dimensions.rs) rolls up authors
--   without an ocean-wide scan. Migration 0034 added author_id/author_name but
--   only indexed channel_id; this completes the symmetric provenance index set
--   ((graph_id, channel_id) already exists; (graph_id, source_type) since 0003).
--
-- Down:
--   DROP INDEX IF EXISTS idx_atoms_graph_author;

CREATE INDEX IF NOT EXISTS idx_atoms_graph_author
    ON atoms (graph_id, author_id);
