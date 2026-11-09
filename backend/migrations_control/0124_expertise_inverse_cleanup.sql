-- Migration: 0124_expertise_inverse_cleanup
-- Description: Review-queue flood cleanup for expertise edges.
--              (1) Delete ALL materialized `expertise_of` inverse rows: nothing
--              reads the predicate (its meta is traversable:false — "outbound-only
--              traversal never needs it"), and each row only duplicated its
--              forward `expert_in` twin in the review queue. `expert_in` now sits
--              on `skip_inverse_materialization`'s list, so no new rows appear.
--              (2) Promote the suggested inferred `expert_in` backlog at/above the
--              shared AUTO_CONFIRM_CONFIDENCE threshold (0.85) to confirmed,
--              stamped `provenance.auto_confirmed = true` — the same status the
--              worker now writes for fresh inferences at that confidence, so the
--              rule applies to the backlog that caused the flood, not just to
--              future rows. Machine-confirmed rows stay distinguishable from
--              human confirmations (and stay machine-revisable) via that flag.
-- Depends on: 0040_relationship_edges (relationship_edge)
-- Down:
--   -- (1) is irreversible (rows deleted); regenerating would require re-running
--   --     expertise inference. (2):
--   UPDATE relationship_edge
--      SET status = 'suggested', provenance = provenance - 'auto_confirmed'
--    WHERE predicate = 'expert_in' AND status = 'confirmed'
--      AND provenance->>'auto_confirmed' = 'true';

DELETE FROM relationship_edge
 WHERE predicate = 'expertise_of';

UPDATE relationship_edge
   SET status     = 'confirmed',
       provenance = jsonb_set(provenance, '{auto_confirmed}', 'true'::jsonb),
       updated_at = now(),
       version    = version + 1
 WHERE predicate  = 'expert_in'
   AND status     = 'suggested'
   AND origin     = 'inferred'
   AND confidence >= 0.85;
