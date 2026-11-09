-- Migration: 0031_expertise_inverse_cleanup
-- Description: The TENANT half of control's `0124_expertise_inverse_cleanup` —
--   see that migration's header for the full rationale (why every
--   `expertise_of` row is dead data, why the suggested inferred `expert_in`
--   backlog at/above AUTO_CONFIRM_CONFIDENCE (0.85) is promoted to confirmed
--   with `provenance.auto_confirmed = true`, and the Down statement). The DML
--   is kept byte-equivalent with the control copy.
--
-- PAIRED WITH: migrations_control/0124_expertise_inverse_cleanup.sql
-- Depends on: 0001_tenant_baseline (relationship_edge)

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
