-- CONTROL 0297 / TENANT 0132 contract phase. NOT AN AUTOMATIC MIGRATION.
-- An operator sets app.identity_serving_contract='composite-v1' only after
-- observing every backend machine serves the new composite conflict target.
-- Include stopped standby/reembed-drain images that could restart as writers;
-- update or fence them before attesting, not only currently running machines.
-- Old binaries must never serve after this operation. Apply each plane alone.
BEGIN;
SELECT pg_advisory_xact_lock(2940129);
DO $$ BEGIN
 IF current_setting('app.identity_serving_contract',true) IS DISTINCT FROM 'composite-v1' THEN
  RAISE EXCEPTION 'all-serving composite-v1 binary verification required';
 END IF;
 IF to_regclass('nodes_graph_type_lower_title_identity_unique') IS NULL OR
  NOT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_name='nodes' AND column_name='identity_scope') THEN
  RAISE EXCEPTION 'identity expand migration must precede contract';
 END IF;
END $$;
-- Reconcile old automatic timestamp-direction guesses after old workers have
-- left service. Human source='user' supersessions remain untouched.
INSERT INTO assertion_disagreements(graph_id,left_node_id,right_node_id)
 SELECT graph_id,LEAST(superseder_id,superseded_id),GREATEST(superseder_id,superseded_id)
 FROM supersession_edge WHERE source='auto' ON CONFLICT DO NOTHING;
UPDATE nodes n SET body=body || jsonb_build_object('unresolved_disagreement',true),version=version+1,updated_at=now()
 WHERE EXISTS(SELECT 1 FROM supersession_edge s WHERE s.source='auto' AND s.graph_id=n.graph_id AND n.node_id IN (s.superseder_id,s.superseded_id));
DELETE FROM supersession_edge WHERE source='auto';
DROP INDEX IF EXISTS nodes_graph_type_lower_title_unique;
COMMIT;
