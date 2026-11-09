-- Migration: 0131_low_structure_conflict_clear
-- Description: One-shot companion to the flip-site structure test (bloat RCA,
--   2026-08-05: https://claude.ai/code/artifact/f98f10a6-9f05-4959-bf64-c65df1ae6311).
--   Pending decisions that FAIL the structure test — under 3 citing corals AND
--   under degree 5, i.e. the status-log supersession trivia the flip sites no
--   longer queue — auto-resolve now instead of dribbling out over their
--   deadlines. The supersession edges and [SUPERSEDED] rendering are
--   untouched; newest simply wins, which for a status log is the only sane
--   outcome. Thresholds mirror `review_policy.rs` at time of writing.
--   Structurally significant pending decisions (if any) are left queued.
--   Idempotent: resolved rows no longer match.
--
-- Depends on: 0011_nodes, 0040_relationship_edges, 0109_reef_digest_counters
-- Down:
--   -- UPDATE nodes SET body = body || '{"review_status":"pending"}'
--   --  WHERE body->>'auto_resolution' = 'low_structure_conflict';

UPDATE nodes n
   SET body       = body || jsonb_build_object(
                        'review_status', 'confirmed',
                        'auto_resolved', true,
                        'auto_resolution', 'low_structure_conflict',
                        'auto_resolved_at', now()),
       updated_at = now(),
       version    = version + 1
 WHERE n.type = 'decision'
   AND n.body->>'review_status' = 'pending'
   AND n.coral_count < 3
   AND (SELECT count(*) FROM relationship_edge e
         WHERE e.src_node_id = n.node_id OR e.dst_node_id = n.node_id) < 5;
