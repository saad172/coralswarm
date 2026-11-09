-- Migration: 0033_evaluation_ledger
-- Description: The TENANT half of control's `0126_evaluation_ledger` — see
--   that migration's header for the full rationale (the burn guard behind
--   high-signal review's "stay blank" rule: blank verdicts anchor the
--   incrementality gate here instead of in a relationship_edge row). Table
--   shape kept byte-equivalent minus RLS, which follows this tree's own
--   convention: graph-keyed `member_ocean_isolation` via
--   `tenant_rls_visible_graph_ids()` (0009_tenant_rls), not control's
--   account-keyed policy.
--
-- PAIRED WITH: migrations_control/0126_evaluation_ledger.sql
-- Depends on: 0001_tenant_baseline (graphs, nodes), 0009_tenant_rls
--   (tenant_rls_visible_graph_ids)
-- Down:
--   DROP TABLE IF EXISTS evaluation_ledger;

CREATE TABLE IF NOT EXISTS evaluation_ledger (
    graph_id        UUID        NOT NULL REFERENCES graphs(id)      ON DELETE CASCADE,
    subject_node_id UUID        NOT NULL REFERENCES nodes(node_id)  ON DELETE CASCADE,
    predicate       TEXT        NOT NULL,
    evaluated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    confidence      REAL,

    PRIMARY KEY (graph_id, subject_node_id, predicate)
);

CREATE INDEX IF NOT EXISTS idx_evaluation_ledger_graph_predicate
    ON evaluation_ledger (graph_id, predicate);

ALTER TABLE evaluation_ledger ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS member_ocean_isolation ON evaluation_ledger;
CREATE POLICY member_ocean_isolation ON evaluation_ledger
    USING (graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids()));
