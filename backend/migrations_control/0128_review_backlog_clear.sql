-- Migration: 0128_review_backlog_clear
-- Description: One-shot backlog clear for high-signal review — applies the
--   0125/0126 policy to the queue that accumulated BEFORE the write gate
--   existed, so the rule covers the rows that caused the flood complaint,
--   not just future inferences:
--     (1) every low-impact suggested INFERRED edge is blanked: its verdict is
--         seeded into `evaluation_ledger` (so re-inference stays suppressed
--         until evidence is newer), then the row is deleted;
--     (2) low-impact pending decisions would auto-confirm — but every pending
--         decision is supersession-flipped by construction (extract.rs writes
--         decisions confirmed; only a conflict flips them), i.e.
--         conflict-involved ⇒ high-impact ⇒ retained. The statement exists
--         for completeness/defense against legacy rows only.
--
--   "Low-impact" mirrors `backend/src/review_policy.rs` AT TIME OF WRITING
--   (IMPACT_DEGREE = 5, IMPACT_ATOMS = 3, contradiction auto-qualifies) as
--   SQL literals — migrations cannot read Rust constants; if the constants
--   are retuned later, this one-shot is NOT re-run (deliberate: it describes
--   the policy at cutover).
--
--   Stated edges (`origin = 'stated'`) and `rejected` rows (durable human
--   negative constraints, ADR-0008) are untouched. High-confidence suggested
--   backlog was already promoted by 0124.
--
-- Depends on: 0126_evaluation_ledger, 0109_reef_digest_counters (coral_count),
--   0040_relationship_edges, 0011_nodes (supersession_edge)
-- Down:
--   -- Irreversible (rows deleted); the ledger seed preserves the verdicts.

WITH node_impact AS (
    SELECT n.node_id,
           (SELECT count(*) FROM relationship_edge e
             WHERE e.src_node_id = n.node_id OR e.dst_node_id = n.node_id) AS degree,
           n.coral_count,
           EXISTS (SELECT 1 FROM supersession_edge s
                    WHERE s.superseded_id = n.node_id OR s.superseder_id = n.node_id) AS conflict
    FROM nodes n
    WHERE n.node_id IN (
        SELECT src_node_id FROM relationship_edge WHERE status = 'suggested' AND origin = 'inferred'
        UNION
        SELECT dst_node_id FROM relationship_edge WHERE status = 'suggested' AND origin = 'inferred'
    )
),
high_impact AS (
    SELECT node_id FROM node_impact
    WHERE conflict OR degree >= 5 OR coral_count >= 3
),
low AS (
    SELECT e.edge_id, e.graph_id, e.src_node_id, e.predicate, e.confidence
    FROM relationship_edge e
    WHERE e.status = 'suggested'
      AND e.origin = 'inferred'
      AND e.src_node_id NOT IN (SELECT node_id FROM high_impact)
      AND e.dst_node_id NOT IN (SELECT node_id FROM high_impact)
),
seeded AS (
    INSERT INTO evaluation_ledger (graph_id, subject_node_id, predicate, evaluated_at, confidence)
    SELECT graph_id, src_node_id, predicate, now(), confidence FROM low
    ON CONFLICT (graph_id, subject_node_id, predicate)
    DO UPDATE SET evaluated_at = GREATEST(evaluation_ledger.evaluated_at, EXCLUDED.evaluated_at)
    RETURNING 1
)
DELETE FROM relationship_edge
 WHERE edge_id IN (SELECT edge_id FROM low);

-- (2) Defensive only — see header: pending decisions are conflict-involved by
-- construction, so this is expected to match zero rows on healthy data.
UPDATE nodes n
   SET body       = body || jsonb_build_object(
                        'review_status', 'confirmed',
                        'auto_resolved', true,
                        'auto_resolved_at', now()),
       updated_at = now(),
       version    = version + 1
 WHERE n.type = 'decision'
   AND n.body->>'review_status' = 'pending'
   AND NOT EXISTS (SELECT 1 FROM supersession_edge s
                    WHERE s.superseded_id = n.node_id OR s.superseder_id = n.node_id)
   AND (SELECT count(*) FROM relationship_edge e
         WHERE e.src_node_id = n.node_id OR e.dst_node_id = n.node_id) < 5
   AND n.coral_count < 3;
