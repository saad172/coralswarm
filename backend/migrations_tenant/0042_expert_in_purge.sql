-- Migration: 0042_expert_in_purge
-- Description: The TENANT half of control's `0135_expert_in_purge` — see that
--   migration's header for the full rationale (one-shot purge of the
--   machine-generated `expert_in` corpus: suggested rows plus the
--   machine-confirmed ones, ledger seeded FIRST and keyed on the TOPIC
--   (`dst_node_id`) because that is what `infer_expertise`'s gate reads;
--   `DISTINCT ON` is load-bearing against the ON CONFLICT second-touch abort;
--   human decisions and `rejected` rows untouched). DML kept byte-equivalent.
--
--   EXPECTED TO BE A NO-OP ON TODAY'S TENANTS. The entire measured corpus
--   (2,120 edges) lives in one consumer-plane account on the control DB; every
--   org tenant currently holds zero `expert_in` rows. This half exists so the
--   two trees stay in lockstep — `tenant_harness` refuses to poll a tenant
--   whose `schema_version` is below `head_version()`, so skipping it would
--   503 every org — and so a tenant that DOES accumulate this corpus later is
--   cleaned by the same rule rather than by a second, divergent one-shot.
--
-- PAIRED WITH: migrations_control/0135_expert_in_purge.sql
-- Depends on: 0033_evaluation_ledger, 0001_tenant_baseline
-- Down:
--   -- Irreversible (rows deleted); the ledger seed preserves the verdicts.

WITH purged AS (
    SELECT edge_id, graph_id, dst_node_id, predicate, confidence
      FROM relationship_edge
     WHERE predicate = 'expert_in'
       AND origin    = 'inferred'
       AND (
                status = 'suggested'
             OR (status = 'confirmed' AND provenance->>'auto_confirmed' = 'true')
           )
),
-- One row per (graph, topic) — see control 0135's "DISTINCT ON IS
-- LOAD-BEARING" note.
seed AS (
    SELECT DISTINCT ON (graph_id, dst_node_id)
           graph_id, dst_node_id, predicate, confidence
      FROM purged
     ORDER BY graph_id, dst_node_id, confidence DESC
),
seeded AS (
    INSERT INTO evaluation_ledger (graph_id, subject_node_id, predicate, evaluated_at, confidence)
    SELECT graph_id, dst_node_id, predicate, now(), confidence FROM seed
    ON CONFLICT (graph_id, subject_node_id, predicate)
    DO UPDATE SET evaluated_at = GREATEST(evaluation_ledger.evaluated_at, EXCLUDED.evaluated_at),
                  confidence   = EXCLUDED.confidence
    RETURNING 1
)
DELETE FROM relationship_edge
 WHERE edge_id IN (SELECT edge_id FROM purged);

-- Drop queued expertise work so the drained corpus is not immediately rebuilt
-- by jobs that were already enqueued against the pre-purge graph. Only
-- `pending` rows: in-flight and finished rows are history, not future writes.
DELETE FROM jobs
 WHERE kind = 'InferExpertise'
   AND status = 'pending';
