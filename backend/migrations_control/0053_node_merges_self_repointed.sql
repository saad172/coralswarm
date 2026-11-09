-- Migration: 0053_node_merges_self_repointed
-- Description: Track whether a merge re-pointed the ocean's self reef (#135).
--              `graphs.self_node_id` (0049) is `ON DELETE SET NULL`, so when
--              `merge_nodes` hard-deletes a duplicate that happens to BE the
--              ocean's self reef, the pointer is silently nulled and the next
--              owner-authored ingest mints a SECOND self reef — reproducing the
--              owner-conflation bug CS-ER was built to fix. The fix re-points
--              `self_node_id` to the surviving reef during merge; this flag
--              records that so `unmerge_nodes` can restore it to the dup on undo.
-- Depends on: 0048_node_merges (node_merges), 0049_connector_identity_self_reef
-- Down:
--   ALTER TABLE node_merges DROP COLUMN IF EXISTS self_repointed;

ALTER TABLE node_merges
    ADD COLUMN IF NOT EXISTS self_repointed BOOLEAN NOT NULL DEFAULT FALSE;
