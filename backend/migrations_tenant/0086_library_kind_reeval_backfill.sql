-- TENANT-TREE PAIR of control migration `0218_library_kind_reeval_backfill`.
-- See that file's header for the rationale. DML must stay byte-identical.

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
