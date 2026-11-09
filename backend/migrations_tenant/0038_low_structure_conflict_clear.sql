-- Migration: 0038_low_structure_conflict_clear
-- Description: The TENANT half of control's `0131_low_structure_conflict_clear`
--   — see that header (one-shot auto-resolve of low-structure conflict-flipped
--   decisions, per the 2026-08-05 bloat RCA). DML kept byte-equivalent.
--
-- PAIRED WITH: migrations_control/0131_low_structure_conflict_clear.sql
-- Depends on: 0001_tenant_baseline, 0020_reef_digest_counters
-- Down:
--   -- See the control header.

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
