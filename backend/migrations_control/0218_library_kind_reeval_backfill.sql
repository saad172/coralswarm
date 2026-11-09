-- Enqueue an initial ReevaluateKind job for every non-sticky Library node
-- that already sits at or past a 100-coral mark.
--
-- PAIRED WITH: migrations_tenant/0086_library_kind_reeval_backfill.sql —
-- the DML in the two files is IDENTICAL and must stay that way.
--
-- 0217's fallback UPDATE assigned `entity_type = 'topic'` without touching
-- `coral_count`, so `nodes_maybe_enqueue_kind_reeval` never fired. A
-- non-sticky legacy node with 150 corals would stay `topic` until a later
-- threshold crossing. Nodes under 100 corals wait for that first crossing,
-- matching the trigger.
--
-- Down:
--   DELETE FROM jobs
--    WHERE kind = 'ReevaluateKind'
--      AND idempotency_key LIKE 'kind:%'
--      AND status = 'pending';

INSERT INTO jobs (kind, payload, idempotency_key, run_after)
SELECT 'ReevaluateKind',
       jsonb_build_object('node_id', node_id, 'graph_id', graph_id),
       'kind:' || node_id || ':' || (coral_count / 100),
       now()
  FROM nodes
 WHERE entity_type IS NOT NULL
   AND type = 'entity'
   AND coral_count >= 100
   AND NOT entity_type_sticky
ON CONFLICT (idempotency_key) WHERE status = 'pending' DO NOTHING;
