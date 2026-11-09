-- Migration: 0036_atoms_created_at_index
-- Description: Index atoms by (graph_id, created_at) so the temporal "intake"
--   query — "what was ingested this week" — can scan a graph's recent atoms by
--   ingestion time without a full table scan. Complements the existing
--   (graph_id, timestamp) index, which orders by source-event time, not intake.
--
-- Down:
--   DROP INDEX IF EXISTS idx_atoms_graph_created;

CREATE INDEX IF NOT EXISTS idx_atoms_graph_created
    ON atoms (graph_id, created_at DESC);
