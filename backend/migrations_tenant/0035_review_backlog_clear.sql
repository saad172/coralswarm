-- Migration: 0035_review_backlog_clear
-- Description: The TENANT half of control's `0128_review_backlog_clear` — see
--   that migration's header for the full rationale (one-shot blank of the
--   pre-gate low-impact suggested backlog, ledger seed first; threshold
--   literals mirror `review_policy.rs` at time of writing; stated and
--   rejected rows untouched; the decision statement is defensive only).
--   DML kept byte-equivalent.
--
-- PAIRED WITH: migrations_control/0128_review_backlog_clear.sql
-- Depends on: 0033_evaluation_ledger, 0020_reef_digest_counters (coral_count),
--   0001_tenant_baseline
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
