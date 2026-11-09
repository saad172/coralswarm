-- Migration: 0130_review_backlog_fastforward
-- Description: Fast-forward the review deadline for the PRE-GATE suggestion
--   backlog. 0126-0128 shipped the high-signal design assuming the queued
--   backlog was small (the July corpus); in reality the Aug 1-4 expertise
--   passes over the freshly-ingested Slack corpus queued ~333 suggestions
--   under the OLD no-gate code, and the 7-day deadline would leave them
--   staring at the founder until Aug 8-11. These rows PRE-DATE the deadline
--   promise — no human was told they had a week — so they get the sweep's
--   exact resolution semantics NOW:
--     * confidence >= 0.70 (the traversability floor / DEADLINE_LEAN_CONFIRM)
--       -> confirmed, stamped auto_confirmed + auto_resolved
--       ('backlog_fastforward'), machine-revisable like every deadline
--       confirm;
--     * below -> blanked into evaluation_ledger, then deleted.
--   Ledger subject matches the producing gate's read key: the TOPIC (dst)
--   for expert_in, src otherwise — same rule as the (fixed) sweep.
--   `rejected` is not written; pending DECISIONS are untouched (they are
--   conflict-flipped genuine clarifications).
--
--   Idempotent by construction: a re-run sees no 'suggested' rows older than
--   its own first application and matches nothing new.
--
-- Depends on: 0126_evaluation_ledger, 0127_review_deadlines (queued_at)
-- Down:
--   -- Confirm-arm reversal:
--   -- UPDATE relationship_edge SET status='suggested'
--   --  WHERE provenance->>'auto_resolution' = 'backlog_fastforward';
--   -- Blank-arm is irreversible (rows deleted; verdicts preserved in the ledger).

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
