-- Explicit interpreted validity and immutable system knowledge. No historical
-- backfill from capture/event dates, and no latest-date supersession.
CREATE TABLE source_version_observations (
 observation_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
 graph_id UUID NOT NULL, source_item_id UUID NOT NULL, source_version_id UUID NOT NULL,
 known_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
 observation_kind TEXT NOT NULL CHECK(observation_kind IN('version_registered','trusted_recapture','annotation_recorded')),
 FOREIGN KEY(source_version_id,source_item_id,graph_id) REFERENCES source_versions(version_id,source_item_id,graph_id) ON DELETE CASCADE
);
CREATE INDEX source_observations_version ON source_version_observations(graph_id,source_version_id,known_at,observation_id);
CREATE FUNCTION temporal_observation_clock() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN
 IF TG_OP='UPDATE' THEN RAISE EXCEPTION 'knowledge observation is immutable' USING ERRCODE='23514'; END IF;
 NEW.known_at=clock_timestamp(); RETURN NEW;
END $$;
CREATE TRIGGER temporal_observation_clock BEFORE INSERT OR UPDATE ON source_version_observations FOR EACH ROW EXECUTE FUNCTION temporal_observation_clock();
CREATE FUNCTION temporal_version_registered() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN
 INSERT INTO source_version_observations(graph_id,source_item_id,source_version_id,observation_kind)
 VALUES(NEW.graph_id,NEW.source_item_id,NEW.version_id,'version_registered'); RETURN NEW;
END $$;
CREATE TRIGGER temporal_version_registered AFTER INSERT ON source_versions FOR EACH ROW EXECUTE FUNCTION temporal_version_registered();

ALTER TABLE semantic_atom_passages ADD CONSTRAINT temporal_passage_identity UNIQUE(passage_id,semantic_atom_id,source_version_id,graph_id);
CREATE TABLE temporal_evidence_records (
 event_id UUID PRIMARY KEY DEFAULT gen_random_uuid(), graph_id UUID NOT NULL,
 subject_node_id UUID NOT NULL, state_key TEXT NOT NULL CHECK(state_key ~ '^[a-z][a-z0-9_.-]{0,63}$'),
 source_item_id UUID NOT NULL, source_version_id UUID NOT NULL, semantic_atom_id UUID NOT NULL, passage_id UUID NOT NULL,
 observation_id UUID NOT NULL REFERENCES source_version_observations(observation_id) ON DELETE CASCADE,
 known_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
 operation TEXT NOT NULL CHECK(operation IN('assert','transition','correct','withdraw')),
 value TEXT CHECK(value IS NULL OR (length(value) BETWEEN 1 AND 512)),
 valid_from TIMESTAMPTZ, valid_until TIMESTAMPTZ,
 validity_quote TEXT CHECK(validity_quote IS NULL OR length(validity_quote) BETWEEN 1 AND 4000),
 predecessor_event_id UUID,
 recording_method TEXT NOT NULL CHECK(recording_method IN('human_curated_v1','temporal_extraction_v1')),
 recorded_by UUID NOT NULL,
 lifecycle TEXT NOT NULL DEFAULT 'supported' CHECK(lifecycle IN('supported','human_review','rejected','evidence_retracted')),
 UNIQUE(event_id,graph_id),
 CHECK(valid_until IS NULL OR (valid_from IS NOT NULL AND valid_until>valid_from)),
 CHECK((operation='assert' AND predecessor_event_id IS NULL) OR (operation<>'assert' AND predecessor_event_id IS NOT NULL)),
 -- A correction's value follows the operation it corrects (enforced by trigger).
 CHECK(lifecycle<>'supported' OR operation='correct' OR (operation='withdraw' AND value IS NULL) OR (operation IN('assert','transition') AND value IS NOT NULL)),
 FOREIGN KEY(subject_node_id,graph_id) REFERENCES nodes(node_id,graph_id) ON DELETE CASCADE,
 FOREIGN KEY(source_version_id,source_item_id,graph_id) REFERENCES source_versions(version_id,source_item_id,graph_id) ON DELETE CASCADE,
 FOREIGN KEY(passage_id,semantic_atom_id,source_version_id,graph_id) REFERENCES semantic_atom_passages(passage_id,semantic_atom_id,source_version_id,graph_id) ON DELETE CASCADE
 -- Predecessor intentionally has no FK: deletion must not be blocked or erase
 -- the fact that a surviving claim needs unavailable prior evidence.
);
CREATE INDEX temporal_subject_state ON temporal_evidence_records(graph_id,subject_node_id,state_key,known_at,event_id);
CREATE FUNCTION temporal_record_integrity() RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE prior temporal_evidence_records%ROWTYPE; base temporal_evidence_records%ROWTYPE; observed source_version_observations%ROWTYPE; depth INT:=0;
BEGIN
 IF TG_OP='UPDATE' THEN
  IF (to_jsonb(NEW)-'lifecycle') IS DISTINCT FROM (to_jsonb(OLD)-'lifecycle') OR (OLD.lifecycle<>'supported' AND NEW.lifecycle='supported') THEN
   RAISE EXCEPTION 'temporal evidence and knowledge are immutable; record a new interpretation' USING ERRCODE='23514';
  END IF;
  IF NEW.lifecycle<>'supported' THEN NEW.value=NULL; NEW.validity_quote=NULL; NEW.valid_from=NULL; NEW.valid_until=NULL; END IF;
  RETURN NEW;
 END IF;
 NEW.known_at=clock_timestamp();
 IF NULLIF(current_setting('app.user_id',true),'') IS NOT NULL AND NEW.recorded_by<>NULLIF(current_setting('app.user_id',true),'')::uuid THEN RAISE EXCEPTION 'temporal actor mismatched' USING ERRCODE='23514'; END IF;
 SELECT * INTO observed FROM source_version_observations WHERE observation_id=NEW.observation_id;
 IF NOT FOUND OR observed.graph_id<>NEW.graph_id OR observed.source_item_id<>NEW.source_item_id OR observed.source_version_id<>NEW.source_version_id OR observed.known_at>NEW.known_at THEN
  RAISE EXCEPTION 'temporal observation does not identify retained evidence' USING ERRCODE='23514';
 END IF;
 IF NOT EXISTS(SELECT 1 FROM node_semantic_atoms l WHERE l.graph_id=NEW.graph_id AND l.node_id=NEW.subject_node_id AND l.semantic_atom_id=NEW.semantic_atom_id) THEN
  RAISE EXCEPTION 'temporal subject lacks canonical semantic support' USING ERRCODE='23514';
 END IF;
 IF NEW.predecessor_event_id IS NOT NULL THEN
  SELECT * INTO prior FROM temporal_evidence_records WHERE event_id=NEW.predecessor_event_id;
  IF NOT FOUND OR prior.graph_id<>NEW.graph_id OR prior.subject_node_id<>NEW.subject_node_id OR prior.state_key<>NEW.state_key OR prior.known_at>=NEW.known_at OR prior.lifecycle<>'supported' THEN
   RAISE EXCEPTION 'temporal predecessor unavailable or mismatched' USING ERRCODE='23514';
  END IF;
  IF NEW.operation='correct' THEN
   base:=prior;
   WHILE base.operation='correct' LOOP
    depth:=depth+1;
    SELECT * INTO base FROM temporal_evidence_records WHERE event_id=base.predecessor_event_id;
    IF NOT FOUND OR depth>128 THEN RAISE EXCEPTION 'temporal correction chain unavailable' USING ERRCODE='23514'; END IF;
   END LOOP;
   IF (base.operation='withdraw') <> (NEW.value IS NULL) THEN
    RAISE EXCEPTION 'temporal correction value must match the corrected operation' USING ERRCODE='23514';
   END IF;
  END IF;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER temporal_record_integrity BEFORE INSERT OR UPDATE ON temporal_evidence_records FOR EACH ROW EXECUTE FUNCTION temporal_record_integrity();

ALTER TABLE conversations ADD COLUMN temporal_selection JSONB CHECK(temporal_selection IS NULL OR (jsonb_typeof(temporal_selection)='object' AND octet_length(temporal_selection::text)<=4096));
ALTER TABLE mcp_tasks ADD COLUMN temporal_selection JSONB CHECK(temporal_selection IS NULL OR (jsonb_typeof(temporal_selection)='object' AND octet_length(temporal_selection::text)<=4096));
CREATE FUNCTION preserve_task_temporal_selection() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN
 IF OLD.temporal_selection IS NOT NULL AND NEW.temporal_selection IS DISTINCT FROM OLD.temporal_selection THEN
  RAISE EXCEPTION 'server temporal task selection is immutable' USING ERRCODE='23514';
 END IF; RETURN NEW;
END $$;
CREATE TRIGGER task_temporal_selection_immutable BEFORE UPDATE OF temporal_selection ON mcp_tasks FOR EACH ROW EXECUTE FUNCTION preserve_task_temporal_selection();
ALTER TABLE conversation_messages ADD COLUMN temporal_selection JSONB CHECK(temporal_selection IS NULL OR (role='user' AND jsonb_typeof(temporal_selection)='object' AND octet_length(temporal_selection::text)<=4096));
CREATE FUNCTION preserve_question_temporal_selection() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN
 IF OLD.temporal_selection IS NOT NULL AND (NEW.temporal_selection IS DISTINCT FROM OLD.temporal_selection OR NEW.role IS DISTINCT FROM OLD.role OR NEW.conversation_id IS DISTINCT FROM OLD.conversation_id) THEN
  RAISE EXCEPTION 'server temporal question selection is immutable' USING ERRCODE='23514';
 END IF; RETURN NEW;
END $$;
CREATE TRIGGER question_temporal_selection_immutable BEFORE UPDATE OF temporal_selection,role,conversation_id ON conversation_messages FOR EACH ROW EXECUTE FUNCTION preserve_question_temporal_selection();

CREATE TABLE answer_temporal_receipts (
 receipt_id UUID PRIMARY KEY REFERENCES answer_support_receipts(receipt_id) ON DELETE CASCADE,
 graph_id UUID NOT NULL REFERENCES graphs(id) ON DELETE CASCADE, owner_user_id UUID NOT NULL,
 temporal_selection JSONB NOT NULL CHECK(jsonb_typeof(temporal_selection)='object' AND octet_length(temporal_selection::text)<=4096),
 event_ids UUID[] NOT NULL CHECK(cardinality(event_ids)<=128)
);
CREATE FUNCTION preserve_temporal_receipt() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN
 RAISE EXCEPTION 'temporal input receipt is immutable' USING ERRCODE='23514';
END $$;
CREATE TRIGGER temporal_receipt_immutable BEFORE UPDATE ON answer_temporal_receipts FOR EACH ROW EXECUTE FUNCTION preserve_temporal_receipt();

ALTER TABLE source_version_observations ENABLE ROW LEVEL SECURITY;
ALTER TABLE temporal_evidence_records ENABLE ROW LEVEL SECURITY;
ALTER TABLE answer_temporal_receipts ENABLE ROW LEVEL SECURITY;
CREATE POLICY member_ocean_isolation ON source_version_observations USING(graph_id IN(SELECT tenant_rls_visible_graph_ids()) AND EXISTS(SELECT 1 FROM source_items s WHERE s.source_item_id=source_version_observations.source_item_id AND s.graph_id=source_version_observations.graph_id AND s.access_state='permitted')) WITH CHECK(graph_id IN(SELECT tenant_rls_visible_graph_ids()));
CREATE POLICY member_ocean_isolation ON temporal_evidence_records USING(graph_id IN(SELECT tenant_rls_visible_graph_ids()) AND EXISTS(SELECT 1 FROM source_items s WHERE s.source_item_id=temporal_evidence_records.source_item_id AND s.graph_id=temporal_evidence_records.graph_id AND s.access_state='permitted')) WITH CHECK(graph_id IN(SELECT tenant_rls_visible_graph_ids()) AND EXISTS(SELECT 1 FROM source_items s WHERE s.source_item_id=temporal_evidence_records.source_item_id AND s.graph_id=temporal_evidence_records.graph_id AND s.access_state='permitted'));
CREATE POLICY member_ocean_isolation ON answer_temporal_receipts USING(graph_id IN(SELECT tenant_rls_visible_graph_ids())) WITH CHECK(graph_id IN(SELECT tenant_rls_visible_graph_ids()));

ALTER FUNCTION library_answer_permitted(UUID) RENAME TO library_answer_permitted_before_temporal;
CREATE FUNCTION library_answer_permitted(r_id UUID) RETURNS BOOLEAN LANGUAGE SQL STABLE AS $$
 SELECT library_answer_permitted_before_temporal(r_id) AND NOT EXISTS(
  SELECT 1 FROM answer_temporal_receipts r WHERE r.receipt_id=r_id AND
   (r.owner_user_id IS DISTINCT FROM NULLIF(current_setting('app.user_id',true),'')::uuid OR
    EXISTS(SELECT 1 FROM unnest(r.event_ids) requested(id) WHERE NOT EXISTS(
     SELECT 1 FROM temporal_evidence_records e JOIN source_items s ON s.source_item_id=e.source_item_id AND s.graph_id=e.graph_id
      JOIN node_semantic_atoms l ON l.graph_id=e.graph_id AND l.node_id=e.subject_node_id AND l.semantic_atom_id=e.semantic_atom_id
     WHERE e.event_id=requested.id AND e.lifecycle='supported' AND s.access_state='permitted' AND library_node_permitted(e.graph_id,e.subject_node_id)))))
$$;
REVOKE ALL ON FUNCTION temporal_observation_clock(),temporal_version_registered(),temporal_record_integrity(),preserve_question_temporal_selection(),preserve_task_temporal_selection(),preserve_temporal_receipt(),library_answer_permitted(UUID) FROM PUBLIC;
DO $$ BEGIN IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname='coralswarm_tenant_app') THEN
 GRANT SELECT,INSERT,UPDATE,DELETE ON source_version_observations,temporal_evidence_records,answer_temporal_receipts TO coralswarm_tenant_app;
 GRANT EXECUTE ON FUNCTION library_answer_permitted(UUID) TO coralswarm_tenant_app;
END IF; END $$;
