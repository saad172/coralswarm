-- Migration: 0126_evaluation_ledger
-- Description: `evaluation_ledger` — the burn guard behind high-signal
--   review's "stay blank" rule. When an inference worker evaluates a
--   candidate fact and decides it is LOW-IMPACT + uncertain, it now writes
--   NO relationship_edge row at all (the graph stays blank) — but the
--   incrementality gate in `workers/infer_expertise` (#475/#478) anchors
--   "already looked at this" on written rows, so a truly blank verdict would
--   be re-evaluated (re-burning an LLM call) every cycle forever. This table
--   is the anchor for blank verdicts: one row per (graph, subject, predicate)
--   recording when the last evaluation happened and what confidence it
--   produced. The gate takes MAX(edge.updated_at, ledger.evaluated_at) per
--   subject, so a blank topic re-evaluates only when its supporting evidence
--   is newer than the last look.
--
--   Rust-side policy constants live in `backend/src/review_policy.rs`.
--
-- Depends on: 0003_atoms (graphs), 0011_nodes (nodes)
-- Down:
--   DROP TABLE IF EXISTS evaluation_ledger;

CREATE TABLE IF NOT EXISTS evaluation_ledger (
    graph_id        UUID        NOT NULL REFERENCES graphs(id)      ON DELETE CASCADE,
    subject_node_id UUID        NOT NULL REFERENCES nodes(node_id)  ON DELETE CASCADE,
    predicate       TEXT        NOT NULL,
    evaluated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- The confidence the last evaluation produced (NULL when the verdict was
    -- "no clear answer" rather than a scored claim). Recorded for audit and
    -- threshold tuning; nothing gates on it today.
    confidence      REAL,

    PRIMARY KEY (graph_id, subject_node_id, predicate)
);

-- The gate reads one graph's rows for one predicate per worker pass.
CREATE INDEX IF NOT EXISTS idx_evaluation_ledger_graph_predicate
    ON evaluation_ledger (graph_id, predicate);

-- Consumer-plane RLS (OPS-4): graph-keyed isolation, same shape as
-- relationship_edge's policy in 0083_consumer_rls.
ALTER TABLE evaluation_ledger ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON evaluation_ledger;
CREATE POLICY account_isolation ON evaluation_ledger
    USING (graph_id IN (
        SELECT id FROM graphs WHERE account_id = current_setting('app.account_id')::uuid
    ));
