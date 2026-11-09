-- Migration: 0037_review_backlog_fastforward
-- Description: The TENANT half of control's `0130_review_backlog_fastforward`
--   — see that header for the full rationale (pre-gate suggestion backlog gets
--   the sweep's resolution semantics now instead of in 7 days; ledger subject
--   matches the gate's read key). DML kept byte-equivalent.
--
-- PAIRED WITH: migrations_control/0130_review_backlog_fastforward.sql
-- Depends on: 0033_evaluation_ledger, 0034_review_deadlines
-- Down:
--   -- See the control header.

-- (1) Lean-confirm the strong half.
UPDATE relationship_edge
   SET status     = 'confirmed',
       provenance = provenance || jsonb_build_object(
                        'auto_confirmed', true,
                        'auto_resolved', true,
                        'auto_resolution', 'backlog_fastforward',
                        'auto_resolved_at', now()),
       updated_at = now(),
       version    = version + 1
 WHERE status = 'suggested'
   AND origin = 'inferred'
   AND confidence >= 0.70;

-- (2) Ledger-anchor then blank the weak half.
WITH weak AS (
    SELECT edge_id, graph_id, src_node_id, dst_node_id, predicate, confidence
    FROM relationship_edge
    WHERE status = 'suggested' AND origin = 'inferred' AND confidence < 0.70
),
seeded AS (
    INSERT INTO evaluation_ledger (graph_id, subject_node_id, predicate, evaluated_at, confidence)
    SELECT graph_id,
           CASE WHEN predicate = 'expert_in' THEN dst_node_id ELSE src_node_id END,
           predicate, now(), confidence
    FROM weak
    ON CONFLICT (graph_id, subject_node_id, predicate)
    DO UPDATE SET evaluated_at = GREATEST(evaluation_ledger.evaluated_at, EXCLUDED.evaluated_at),
                  confidence   = EXCLUDED.confidence
    RETURNING 1
)
DELETE FROM relationship_edge
 WHERE edge_id IN (SELECT edge_id FROM weak);
