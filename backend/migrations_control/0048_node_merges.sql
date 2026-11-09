-- Migration: 0048_node_merges
-- Description: Undo-log for entity-resolution merges (CS-ER). `merge_nodes`
--              hard-deletes the duplicate reef (`keep` stays the single live
--              row for the hot read path), so a wrong auto-merge would be
--              irreversible. Each merge writes one `node_merges` row IN THE SAME
--              transaction capturing everything needed to reverse it exactly:
--              the dup node snapshot, the atom links actually moved dup->keep,
--              and the relationship/supersession/promotion edges re-pointed or
--              dropped (with their ORIGINAL endpoints). `unmerge_nodes(merge_id)`
--              replays this row to recreate the dup and restore its references,
--              then stamps `undone_at` (idempotent: a second undo is a no-op).
-- Depends on: 0011_nodes (nodes), 0003_atoms (graphs)
-- Down:
--   DROP TABLE IF EXISTS node_merges;

CREATE TABLE IF NOT EXISTS node_merges (
    merge_id     UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    graph_id     UUID        NOT NULL REFERENCES graphs(id) ON DELETE CASCADE,
    -- The surviving reef and the duplicate that was merged into it + deleted.
    -- Not FK-constrained on dup_id (it no longer exists once the merge commits);
    -- keep_id is left un-constrained too so a later delete of keep doesn't cascade
    -- away the audit row (undo would be moot, but the record is preserved).
    keep_id      UUID        NOT NULL,
    dup_id       UUID        NOT NULL,

    -- Full snapshot of the deleted dup `nodes` row, so undo can recreate it byte
    -- for byte: { "type", "title", "body", "scope", "owner_id", "created_at",
    -- "version" } (UUID/timestamps as strings; body as the JSON object).
    dup_snapshot         JSONB NOT NULL,
    -- The atom_ids this merge actually moved dup->keep (the INSERT succeeded;
    -- excludes atoms keep already had). Undo moves back exactly these.
    moved_atom_ids       JSONB NOT NULL DEFAULT '[]'::jsonb,
    -- Relationship edges touched, each carrying the edge's ORIGINAL full row plus
    -- an "action": "repointed_src" | "repointed_dst" | "dropped". Undo restores
    -- every recorded edge to these exact endpoints (re-inserting dropped ones).
    repointed_edges      JSONB NOT NULL DEFAULT '[]'::jsonb,
    -- Supersession edges touched (re-pointed dup->keep or dropped on collision/
    -- self-loop), each with its ORIGINAL { superseder_id, superseded_id, source }
    -- and an "action".
    repointed_supersessions JSONB NOT NULL DEFAULT '[]'::jsonb,
    -- Promotion provenance rows touched, each with its ORIGINAL full row and an
    -- "action" ("repointed_src" | "repointed_dst" | "dropped").
    repointed_promotions JSONB NOT NULL DEFAULT '[]'::jsonb,

    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- NULL while the merge stands; stamped when reversed (idempotency guard).
    undone_at    TIMESTAMPTZ
);

-- "Undo this merge" lookups by id are the PK; this index supports listing a
-- graph's reversible (not-yet-undone) merges newest-first.
CREATE INDEX IF NOT EXISTS idx_node_merges_graph_active
    ON node_merges (graph_id, created_at DESC) WHERE undone_at IS NULL;
