-- P8: non-content dependency delivery, selected evidence consent, physical purge.
-- Old source/version/graph rows may disappear before a drain. Never FK audit
-- identities to those rows. New queues contain UUIDs and validated blob locators.
-- A retained decision with lost date support must become explicitly unknown,
-- rather than keep an unsupported timestamp or invent one to satisfy legacy DDL.
ALTER TABLE nodes DROP CONSTRAINT decisions_require_decided_at;
ALTER TABLE nodes ADD CONSTRAINT decisions_require_decided_at CHECK(type<>'decision' OR body->>'decided_at' IS NOT NULL OR body->>'lifecycle'='evidence_retracted');
ALTER TABLE atoms ADD COLUMN derived_copy_bound BOOLEAN NOT NULL DEFAULT false;
UPDATE atoms a SET derived_copy_bound=true WHERE a.promoted_from_atom_id IS NOT NULL OR EXISTS(SELECT 1 FROM source_connection_atoms c WHERE c.target_atom_id=a.atom_id);
CREATE FUNCTION library_bind_derived_copy() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF TG_TABLE_NAME='source_connection_atoms' THEN UPDATE atoms SET derived_copy_bound=true WHERE atom_id=NEW.target_atom_id;RETURN NEW; END IF;
 IF NEW.promoted_from_atom_id IS NOT NULL OR (TG_OP='UPDATE' AND OLD.derived_copy_bound) THEN NEW.derived_copy_bound:=true;END IF;RETURN NEW;END $$;
CREATE TRIGGER derived_copy_monotonic BEFORE INSERT OR UPDATE ON atoms FOR EACH ROW EXECUTE FUNCTION library_bind_derived_copy();
CREATE TRIGGER source_copy_bound AFTER INSERT OR UPDATE OF target_atom_id ON source_connection_atoms FOR EACH ROW EXECUTE FUNCTION library_bind_derived_copy();
ALTER TABLE atoms ADD COLUMN evidence_retracted BOOLEAN NOT NULL DEFAULT false;
CREATE FUNCTION library_keep_retracted_atom() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF OLD.evidence_retracted THEN NEW.evidence_retracted:=true; END IF; RETURN NEW; END $$;
CREATE TRIGGER atom_retraction_monotonic BEFORE UPDATE OF evidence_retracted ON atoms FOR EACH ROW EXECUTE FUNCTION library_keep_retracted_atom();
CREATE TABLE source_purge_events (
 event_id UUID PRIMARY KEY DEFAULT gen_random_uuid(), account_id UUID NOT NULL,owner_user_id UUID,
 graph_id UUID NOT NULL,source_item_id UUID NOT NULL,event_kind TEXT NOT NULL
 CHECK(event_kind IN('deleted','restricted','unknown','retracted','graph_deleted')),
 created_at TIMESTAMPTZ NOT NULL DEFAULT now(),sql_processed_at TIMESTAMPTZ
);
CREATE UNIQUE INDEX source_purge_event_pending ON source_purge_events(graph_id,source_item_id,event_kind) WHERE sql_processed_at IS NULL;
CREATE TABLE source_purge_dependencies (
 dependency_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),event_id UUID NOT NULL REFERENCES source_purge_events ON DELETE CASCADE,
 kind TEXT NOT NULL CHECK(kind IN('atom','node','blob')),graph_id UUID NOT NULL,object_id UUID NOT NULL,
 blob_key TEXT,blob_owner_account_id UUID,blob_owner_org_id TEXT,error_category TEXT CHECK(error_category IS NULL OR error_category IN('invalid_locator','ownership_unavailable')),processed_at TIMESTAMPTZ,cleanup_job_id UUID,
 UNIQUE(event_id,kind,graph_id,object_id), CHECK((kind='blob')=(blob_key IS NOT NULL))
);
CREATE INDEX source_purge_dependency_pending ON source_purge_dependencies(event_id,kind,dependency_id) WHERE processed_at IS NULL;
ALTER TABLE source_purge_events ENABLE ROW LEVEL SECURITY;
CREATE POLICY purge_event_scope ON source_purge_events USING(account_id=NULLIF(current_setting('app.account_id',true),'')::uuid);
ALTER TABLE source_purge_dependencies ENABLE ROW LEVEL SECURITY;
CREATE POLICY purge_dependency_scope ON source_purge_dependencies USING(event_id IN(SELECT event_id FROM source_purge_events));

CREATE TABLE source_purge_reviews(
 account_id UUID NOT NULL,graph_id UUID NOT NULL,object_id UUID NOT NULL,kind TEXT NOT NULL CHECK(kind IN('node','edge')),
 review_status TEXT NOT NULL CHECK(review_status IN('confirmed','rejected','pending','held','reviewed','selected')),
 PRIMARY KEY(graph_id,kind,object_id));
ALTER TABLE source_purge_reviews ENABLE ROW LEVEL SECURITY;
CREATE POLICY purge_review_scope ON source_purge_reviews USING(account_id=NULLIF(current_setting('app.account_id',true),'')::uuid);
CREATE TABLE selected_evidence_publications (
 publication_id UUID PRIMARY KEY,account_id UUID NOT NULL,owner_user_id UUID NOT NULL,request_id UUID NOT NULL,
 source_graph_id UUID NOT NULL,source_item_id UUID NOT NULL,source_version_id UUID NOT NULL,
 selection JSONB NOT NULL CHECK(jsonb_typeof(selection)='array' AND octet_length(selection::text)<=16384),
 passage_ids UUID[] NOT NULL CHECK(cardinality(passage_ids) BETWEEN 1 AND 32),
 target_graph_id UUID NOT NULL,target_source_item_id UUID,target_source_version_id UUID,
 selected_sha256 TEXT NOT NULL CHECK(selected_sha256~'^[0-9a-f]{64}$'),
 status TEXT NOT NULL DEFAULT 'active' CHECK(status IN('active','retracted')),
 created_at TIMESTAMPTZ NOT NULL DEFAULT now(),retracted_at TIMESTAMPTZ,
 UNIQUE(account_id,owner_user_id,request_id),CHECK(source_graph_id<>target_graph_id)
);
CREATE FUNCTION library_seal_publication() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF (to_jsonb(NEW)-ARRAY['status','retracted_at']) IS DISTINCT FROM (to_jsonb(OLD)-ARRAY['status','retracted_at']) OR (OLD.status='retracted' AND NEW.status<>'retracted') THEN RAISE EXCEPTION 'publication consent is immutable';END IF;RETURN NEW;END $$;
CREATE TRIGGER publication_consent_immutable BEFORE UPDATE ON selected_evidence_publications FOR EACH ROW EXECUTE FUNCTION library_seal_publication();
CREATE INDEX selected_publication_source ON selected_evidence_publications(source_graph_id,source_item_id) WHERE status='active';
ALTER TABLE selected_evidence_publications ENABLE ROW LEVEL SECURITY;
CREATE POLICY selected_publication_scope ON selected_evidence_publications
 USING(account_id=NULLIF(current_setting('app.account_id',true),'')::uuid AND owner_user_id=NULLIF(current_setting('app.user_id',true),'')::uuid)
 WITH CHECK(account_id=NULLIF(current_setting('app.account_id',true),'')::uuid AND owner_user_id=NULLIF(current_setting('app.user_id',true),'')::uuid);

-- One central set predicate remains authoritative for Library/P3 and the
-- cheap scalar wrapper from 0307. Legacy whole-coral copies are unavailable;
-- exact selected sources use versioned semantic evidence, never raw copies.
CREATE OR REPLACE FUNCTION library_permitted_atoms(g UUID,atom_ids UUID[] DEFAULT NULL) RETURNS TABLE(atom_id UUID)
 LANGUAGE SQL STABLE AS $$
 WITH mapped AS MATERIALIZED (
  SELECT c.atom_id,bool_or(s.access_state='permitted') permitted,bool_or(s.access_state<>'permitted') denied
  FROM source_version_chunks c JOIN source_versions v ON v.graph_id=c.graph_id AND v.version_id=c.source_version_id
  JOIN source_items s ON s.graph_id=v.graph_id AND s.source_item_id=v.source_item_id
  WHERE c.graph_id=g AND (atom_ids IS NULL OR c.atom_id=ANY(atom_ids)) GROUP BY c.atom_id)
 SELECT a.atom_id FROM atoms a LEFT JOIN mapped m ON m.atom_id=a.atom_id
 WHERE a.graph_id=g AND (atom_ids IS NULL OR a.atom_id=ANY(atom_ids))
  AND NOT a.evidence_retracted AND NOT a.derived_copy_bound
  AND (NOT a.source_evidence_bound OR COALESCE(m.permitted,false)) AND NOT COALESCE(m.denied,false)
$$;
REVOKE ALL ON FUNCTION library_permitted_atoms(UUID,UUID[]) FROM PUBLIC;
CREATE OR REPLACE FUNCTION library_atom_permitted(g UUID,a_id UUID) RETURNS BOOLEAN LANGUAGE SQL STABLE AS $$ SELECT EXISTS(SELECT 1 FROM library_permitted_atoms(g,ARRAY[a_id])) $$;
CREATE INDEX answer_support_source_delivery ON answer_support_receipts USING GIN(support jsonb_path_ops);

CREATE FUNCTION library_capture_purge(g UUID,s_id UUID,k TEXT) RETURNS UUID
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public AS $$
DECLARE ev UUID;acc UUID;owner UUID;receipts UUID[];src TEXT;
BEGIN
 SELECT account_id,owner_user_id INTO acc,owner FROM public.graphs WHERE id=g;
 IF NOT FOUND THEN RETURN NULL; END IF;
 SELECT source_id INTO src FROM public.source_items WHERE graph_id=g AND source_item_id=s_id;
 INSERT INTO public.source_purge_events(account_id,owner_user_id,graph_id,source_item_id,event_kind) VALUES(acc,owner,g,s_id,k)
 ON CONFLICT(graph_id,source_item_id,event_kind) WHERE sql_processed_at IS NULL DO UPDATE SET account_id=EXCLUDED.account_id RETURNING event_id INTO ev;
 INSERT INTO public.source_purge_dependencies(event_id,kind,graph_id,object_id)
 SELECT DISTINCT ev,'atom',c.graph_id,c.atom_id FROM public.source_versions v JOIN public.source_version_chunks c ON c.graph_id=v.graph_id AND c.source_version_id=v.version_id WHERE v.graph_id=g AND v.source_item_id=s_id ON CONFLICT DO NOTHING;
 INSERT INTO public.source_purge_dependencies(event_id,kind,graph_id,object_id)
 SELECT DISTINCT ev,'node',l.graph_id,l.node_id FROM public.source_versions v JOIN public.semantic_atoms a ON a.graph_id=v.graph_id AND a.source_version_id=v.version_id JOIN public.node_semantic_atoms l ON l.graph_id=a.graph_id AND l.semantic_atom_id=a.semantic_atom_id WHERE v.graph_id=g AND v.source_item_id=s_id ON CONFLICT DO NOTHING;
 WITH RECURSIVE copies(graph_id,atom_id,depth,path) AS (
  SELECT d.graph_id,d.object_id,0,ARRAY[d.object_id] FROM public.source_purge_dependencies d WHERE d.event_id=ev AND d.kind='atom'
  UNION ALL SELECT a.graph_id,a.atom_id,c.depth+1,c.path||a.atom_id FROM copies c JOIN public.atoms a ON a.promoted_from_atom_id=c.atom_id OR EXISTS(SELECT 1 FROM public.source_connection_atoms r JOIN public.source_connections sc ON sc.id=r.connection_id WHERE r.atom_id=c.atom_id AND r.target_atom_id=a.atom_id AND sc.source_graph_id=c.graph_id AND sc.target_graph_id=a.graph_id AND sc.account_id=acc)
   JOIN public.graphs dg ON dg.id=a.graph_id AND dg.account_id=acc AND dg.kind IN('team','org') WHERE c.depth<16 AND NOT a.atom_id=ANY(c.path)
 ) INSERT INTO public.source_purge_dependencies(event_id,kind,graph_id,object_id) SELECT DISTINCT ev,'atom',graph_id,atom_id FROM copies ON CONFLICT DO NOTHING;
 INSERT INTO public.source_purge_dependencies(event_id,kind,graph_id,object_id)
 SELECT DISTINCT ev,'node',n.graph_id,n.node_id FROM public.node_atoms l JOIN public.nodes n ON n.node_id=l.node_id JOIN public.source_purge_dependencies d ON d.event_id=ev AND d.kind='atom' AND d.object_id=l.atom_id AND d.graph_id=n.graph_id ON CONFLICT DO NOTHING;
 INSERT INTO public.source_purge_reviews SELECT acc,n.graph_id,n.node_id,'node',n.body->>'human_review_status' FROM public.nodes n JOIN public.source_purge_dependencies d ON d.event_id=ev AND d.kind='node' AND d.graph_id=n.graph_id AND d.object_id=n.node_id WHERE n.body->>'human_review_status' IN('confirmed','rejected','pending','held','reviewed','selected') ON CONFLICT DO NOTHING;
 INSERT INTO public.source_purge_reviews SELECT acc,e.graph_id,e.edge_id,'edge',COALESCE(e.provenance->>'human_review_status',e.status) FROM public.relationship_edge e WHERE EXISTS(SELECT 1 FROM public.source_purge_dependencies d WHERE d.event_id=ev AND d.kind='node' AND d.graph_id=e.graph_id AND d.object_id IN(e.src_node_id,e.dst_node_id)) AND COALESCE(e.provenance->>'human_review_status',e.status) IN('confirmed','rejected','pending','held','reviewed','selected') ON CONFLICT DO NOTHING;
 -- Close the retained-header/readability gap before mapping FKs disappear.
 UPDATE public.atoms a SET evidence_retracted=true WHERE EXISTS(SELECT 1 FROM public.source_purge_dependencies d WHERE d.event_id=ev AND d.kind='atom' AND d.graph_id=a.graph_id AND d.object_id=a.atom_id);
 IF k='graph_deleted' THEN
  INSERT INTO public.source_purge_dependencies(event_id,kind,graph_id,object_id,blob_key)
  SELECT ev,'blob',a.graph_id,a.atom_id,a.content_blob_key FROM public.atoms a JOIN public.source_purge_dependencies d ON d.event_id=ev AND d.kind='atom' AND d.graph_id=a.graph_id AND d.object_id=a.atom_id WHERE a.content_blob_key IS NOT NULL AND (NOT a.derived_copy_bound OR split_part(a.content_blob_key,'/',GREATEST(1,array_length(string_to_array(a.content_blob_key,'/'),1)-1))=a.graph_id::text) ON CONFLICT DO NOTHING;
 END IF;
 -- Canonical originals survive permission uncertainty; deleted originals need
 -- durable ciphertext cleanup, including deletion via a graph FK cascade.
 IF k IN('deleted','graph_deleted') THEN
  INSERT INTO public.source_purge_dependencies(event_id,kind,graph_id,object_id,blob_key)
  SELECT ev,'blob',g,v.version_id,v.raw_blob_key FROM public.source_versions v WHERE v.graph_id=g AND v.source_item_id=s_id AND v.raw_blob_key IS NOT NULL ON CONFLICT DO NOTHING;
 END IF;
 SELECT array_agg(r.receipt_id) INTO receipts FROM public.answer_support_receipts r
 WHERE r.support @> jsonb_build_array(jsonb_build_object('kind','source_passage','graph_id',g,'source_item_id',s_id))
 OR EXISTS(SELECT 1 FROM jsonb_array_elements(r.support) e JOIN public.source_purge_dependencies d ON d.event_id=ev AND d.kind='atom' AND d.graph_id::text=e->>'graph_id' AND d.object_id::text=e->>'atom_id' WHERE e->>'kind'='legacy_chunk');
 UPDATE public.answer_support_receipts SET evidence_complete=false WHERE receipt_id=ANY(receipts);
 UPDATE public.conversations SET title='' WHERE id=ANY(receipts) OR id IN(SELECT conversation_id FROM public.conversation_messages WHERE id=ANY(receipts));
 UPDATE public.conversation_messages SET text='[Evidence no longer available]',citations='[]',sources='[]',timing='{}',scope_graph_ids='[]' WHERE id=ANY(receipts) AND role='assistant';
 DELETE FROM public.query_history WHERE id=ANY(receipts);
 UPDATE public.mcp_tasks SET status='failed',result=NULL,error='{"code":"evidence_retracted","message":"Evidence no longer available"}',result_sha256=NULL,updated_at=now() WHERE answer_receipt_id=ANY(receipts);
 -- Old answer caches have no complete input lineage. Conservative graph/account
 -- invalidation removes their content rather than pretending unrelated support
 -- independently authorizes previously generated prose.
 PERFORM public.invalidate_derived_read_surfaces(g);
 UPDATE public.conversation_messages m SET timing='{}' WHERE m.role='assistant' AND (m.scope_graph_ids @> to_jsonb(ARRAY[g::text]) OR EXISTS(SELECT 1 FROM public.conversations c WHERE c.id=m.conversation_id AND c.graph_id=g));
 -- Lineage-free legacy task RESULTS are invalidated conservatively; in-flight
 -- tasks of other graphs carry no prose yet and are left running.
 UPDATE public.mcp_tasks SET status='failed',result=NULL,error='{"code":"evidence_retracted","message":"Evidence no longer available"}',result_sha256=NULL,updated_at=now() WHERE graph_id=g OR (answer_receipt_id IS NULL AND account_id=acc AND status='completed');
 -- Source-owned state (summaries, queued ingestion, session labels) is scoped
 -- to the purged source; only a whole-graph event (nil item) widens to the graph.
 UPDATE public.meeting_transcripts SET summary=NULL,summary_status='pending',summary_error=NULL,summary_model=NULL,summary_updated_at=NULL WHERE graph_id=g AND (s_id='00000000-0000-0000-0000-000000000000' OR source_item_id=s_id);
 UPDATE public.jobs SET status='dead',payload=jsonb_build_object('graph_id',g,'evidence_retracted',true),error_message=NULL,finished_at=now(),idempotency_key=NULL WHERE payload->>'graph_id'=g::text AND (s_id='00000000-0000-0000-0000-000000000000' OR payload->>'source_item_id'=s_id::text OR payload->>'source_id'=src OR payload->>'source_version_id' IN(SELECT v.version_id::text FROM public.source_versions v WHERE v.graph_id=g AND v.source_item_id=s_id) OR EXISTS(SELECT 1 FROM public.source_purge_dependencies d WHERE d.event_id=ev AND d.kind='atom' AND (payload->>'atom_id'=d.object_id::text OR (jsonb_typeof(payload->'atom_ids')='array' AND payload->'atom_ids' ? d.object_id::text)))) AND kind IN('ExtractMeetingAtoms','ExtractSourceAtoms','ExtractEntities','ExtractRelationships','IngestDoor','GenerateMeetingSummary','ExtractEntityBatch','RegenerateDigest','EntityResolution','ConflictDetection','InferExpertise','ClassifyKind','ExtractMentions','GenerateTitle','GenerateJournal','PromoteNode','PublishSource');
 UPDATE public.coding_sessions cs SET platform_detected=NULL,project_key=NULL,title=NULL,repo_path=NULL,remote_sanitized=NULL,hostname=NULL,harness_version=NULL,model=NULL,branch=NULL,agent_name=NULL,platform=NULL,task=NULL,subagent_name=NULL WHERE cs.graph_id=g AND (s_id='00000000-0000-0000-0000-000000000000' OR EXISTS(SELECT 1 FROM public.source_purge_dependencies d JOIN public.atoms a ON a.graph_id=d.graph_id AND a.atom_id=d.object_id WHERE d.event_id=ev AND d.kind='atom' AND a.graph_id=g AND a.source_id LIKE ('cc:'||replace(replace(replace(cs.session_id,'\','\\'),'%','\%'),'_','\_')||':%')));
 DELETE FROM public.op_log WHERE graph_id=g AND entity IN('query','conversation','conversation_message');
 DELETE FROM public.op_log o WHERE o.graph_id=g AND o.entity='coding_session' AND (s_id='00000000-0000-0000-0000-000000000000' OR EXISTS(SELECT 1 FROM public.coding_sessions cs WHERE o.entity_id IN(cs.id::text,cs.session_id) AND cs.graph_id=g AND EXISTS(SELECT 1 FROM public.source_purge_dependencies d JOIN public.atoms a ON a.graph_id=d.graph_id AND a.atom_id=d.object_id WHERE d.event_id=ev AND d.kind='atom' AND a.graph_id=g AND a.source_id LIKE ('cc:'||replace(replace(replace(cs.session_id,'\','\\'),'%','\%'),'_','\_')||':%'))));
 -- Exact, sealed publication provenance is sufficient to revoke dependent
 -- destination support without widening the deleting actor's permissions.
 UPDATE public.source_items t SET access_state='restricted' FROM public.selected_evidence_publications p
 WHERE p.source_graph_id=g AND p.source_item_id=s_id AND t.graph_id=p.target_graph_id AND t.source_item_id=p.target_source_item_id
 AND EXISTS(SELECT 1 FROM public.source_versions v WHERE v.graph_id=t.graph_id AND v.source_item_id=t.source_item_id AND v.source_metadata->>'publication_receipt_id'=p.publication_id::text);
 DELETE FROM public.source_items t USING public.selected_evidence_publications p
 WHERE p.source_graph_id=g AND p.source_item_id=s_id AND t.graph_id=p.target_graph_id AND t.source_item_id=p.target_source_item_id
 AND EXISTS(SELECT 1 FROM public.source_versions v WHERE v.graph_id=t.graph_id AND v.source_item_id=t.source_item_id AND v.source_metadata->>'publication_receipt_id'=p.publication_id::text);
 UPDATE public.selected_evidence_publications SET status='retracted',retracted_at=now() WHERE status='active' AND ((source_graph_id=g AND source_item_id=s_id) OR (target_graph_id=g AND target_source_item_id=s_id));
 RETURN ev;
END $$;
CREATE FUNCTION library_source_purge_signal() RETURNS trigger
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public AS $$
BEGIN
 IF TG_OP='DELETE' THEN PERFORM public.library_capture_purge(OLD.graph_id,OLD.source_item_id,'deleted'); RETURN OLD; END IF;
 IF NEW.access_state<>'permitted' AND NEW.access_state IS DISTINCT FROM OLD.access_state THEN PERFORM public.library_capture_purge(OLD.graph_id,OLD.source_item_id,NEW.access_state); END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER source_purge_delete BEFORE DELETE ON source_items FOR EACH ROW EXECUTE FUNCTION library_source_purge_signal();
CREATE TRIGGER source_purge_access BEFORE UPDATE OF access_state ON source_items FOR EACH ROW EXECUTE FUNCTION library_source_purge_signal();
CREATE FUNCTION library_graph_purge_signal() RETURNS trigger
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public AS $$
DECLARE item UUID;ev UUID;
BEGIN
 FOR item IN SELECT source_item_id FROM public.source_items WHERE graph_id=OLD.id ORDER BY source_item_id LOOP
  PERFORM public.library_capture_purge(OLD.id,item,'graph_deleted');
 END LOOP;
 ev:=public.library_capture_purge(OLD.id,'00000000-0000-0000-0000-000000000000','graph_deleted');
 INSERT INTO public.source_purge_dependencies(event_id,kind,graph_id,object_id) SELECT ev,'atom',OLD.id,atom_id FROM public.atoms WHERE graph_id=OLD.id ON CONFLICT DO NOTHING;
 PERFORM public.library_capture_purge(OLD.id,'00000000-0000-0000-0000-000000000000','graph_deleted');
 RETURN OLD;
END $$;
CREATE TRIGGER graph_purge_delete BEFORE DELETE ON graphs FOR EACH ROW EXECUTE FUNCTION library_graph_purge_signal();

-- Delivery is bounded by dependency rows, not ocean size. Only trusted trigger
-- functions can create dependencies. Callers receive counts, never source data.
CREATE FUNCTION library_drain_purge(batch INTEGER DEFAULT 100) RETURNS INTEGER
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public AS $$
DECLARE initial_scope TEXT:=current_setting('app.account_id',true);d public.source_purge_dependencies%ROWTYPE;ev public.source_purge_events%ROWTYPE;key TEXT;parts TEXT[];owner_graph UUID;owner_account UUID;scope_account UUID;job UUID;done INTEGER:=0;
BEGIN
 IF NULLIF(initial_scope,'') IS NULL THEN RAISE EXCEPTION 'purge requires explicit retained account scope';END IF;
 IF batch<1 OR batch>200 THEN RAISE EXCEPTION 'invalid purge batch'; END IF;
 FOR d IN SELECT * FROM public.source_purge_dependencies WHERE processed_at IS NULL AND error_category IS NULL AND event_id IN(SELECT event_id FROM public.source_purge_events WHERE account_id=initial_scope::uuid) ORDER BY CASE kind WHEN 'atom' THEN 0 WHEN 'node' THEN 1 ELSE 2 END,dependency_id LIMIT batch FOR UPDATE SKIP LOCKED LOOP
  SELECT * INTO ev FROM public.source_purge_events WHERE event_id=d.event_id;
  SELECT account_id INTO scope_account FROM public.graphs WHERE id=d.graph_id;
  PERFORM set_config('app.account_id',COALESCE(scope_account,ev.account_id)::text,true);
  IF d.kind='atom' THEN
   -- Expand explicit copy receipts before deleting the original atom and its
   -- receipt FKs. No provider-title/source-ID wildcard is trusted.
   INSERT INTO public.source_purge_dependencies(event_id,kind,graph_id,object_id)
   SELECT d.event_id,'atom',a.graph_id,a.atom_id FROM public.atoms a JOIN public.graphs tg ON tg.id=a.graph_id AND tg.account_id=ev.account_id AND tg.kind IN('team','org') WHERE a.promoted_from_atom_id=d.object_id
    OR EXISTS(SELECT 1 FROM public.source_connection_atoms c JOIN public.source_connections sc ON sc.id=c.connection_id WHERE c.atom_id=d.object_id AND c.target_atom_id=a.atom_id AND sc.source_graph_id=d.graph_id AND sc.target_graph_id=a.graph_id AND sc.account_id=ev.account_id) ON CONFLICT DO NOTHING;
   INSERT INTO public.source_purge_dependencies(event_id,kind,graph_id,object_id)
   SELECT d.event_id,'node',n.graph_id,n.node_id FROM public.node_atoms l JOIN public.nodes n ON n.node_id=l.node_id WHERE l.atom_id=d.object_id ON CONFLICT DO NOTHING;
   SELECT content_blob_key INTO key FROM public.atoms WHERE atom_id=d.object_id AND graph_id=d.graph_id;
   IF key IS NOT NULL AND NOT EXISTS(SELECT 1 FROM public.atoms WHERE atom_id=d.object_id AND derived_copy_bound AND split_part(key,'/',GREATEST(1,array_length(string_to_array(key,'/'),1)-1))<>d.graph_id::text) THEN INSERT INTO public.source_purge_dependencies(event_id,kind,graph_id,object_id,blob_key) VALUES(d.event_id,'blob',d.graph_id,d.object_id,key) ON CONFLICT DO NOTHING; END IF;
   UPDATE public.answer_support_receipts SET evidence_complete=false WHERE support @> jsonb_build_array(jsonb_build_object('kind','legacy_chunk','graph_id',d.graph_id,'atom_id',d.object_id));
   UPDATE public.conversation_messages m SET text='[Evidence no longer available]',citations='[]',sources='[]',timing='{}',scope_graph_ids='[]' WHERE m.role='assistant' AND m.id IN(SELECT receipt_id FROM public.answer_support_receipts r WHERE r.support @> jsonb_build_array(jsonb_build_object('kind','legacy_chunk','graph_id',d.graph_id,'atom_id',d.object_id)));
   UPDATE public.mcp_tasks SET status='failed',result=NULL,error='{"code":"evidence_retracted","message":"Evidence no longer available"}',result_sha256=NULL WHERE answer_receipt_id IN(SELECT receipt_id FROM public.answer_support_receipts r WHERE r.support @> jsonb_build_array(jsonb_build_object('kind','legacy_chunk','graph_id',d.graph_id,'atom_id',d.object_id)));
   PERFORM public.invalidate_derived_read_surfaces(d.graph_id);
   DELETE FROM public.atoms WHERE atom_id=d.object_id AND graph_id=d.graph_id;
  ELSIF d.kind='node' THEN
   INSERT INTO public.source_purge_dependencies(event_id,kind,graph_id,object_id)
   SELECT d.event_id,'node',n.graph_id,n.node_id FROM public.nodes n JOIN public.graphs tg ON tg.id=n.graph_id AND tg.account_id=ev.account_id AND tg.kind IN('team','org') WHERE n.promoted_from_node_id=d.object_id AND n.promoted_from_graph_id=d.graph_id ON CONFLICT DO NOTHING;
   INSERT INTO public.source_purge_reviews SELECT ev.account_id,d.graph_id,n.node_id,'node',n.body->>'human_review_status' FROM public.nodes n WHERE n.graph_id=d.graph_id AND n.node_id=d.object_id AND n.body->>'human_review_status' IN('confirmed','rejected','pending','held','reviewed','selected') ON CONFLICT DO NOTHING;
   INSERT INTO public.source_purge_reviews SELECT ev.account_id,d.graph_id,e.edge_id,'edge',COALESCE(e.provenance->>'human_review_status',e.status) FROM public.relationship_edge e WHERE e.graph_id=d.graph_id AND (e.src_node_id=d.object_id OR e.dst_node_id=d.object_id) AND COALESCE(e.provenance->>'human_review_status',e.status) IN('confirmed','rejected','pending','held','reviewed','selected') ON CONFLICT DO NOTHING;
   -- Old free-text edge rationale/confidence has no complete input receipt.
   -- Keep typed independently permitted support and review only, never private
   -- prose merely because another source still justifies the endpoint identity.
   -- confidence is NOT NULL: 0 records 'no attested confidence', never support.
   UPDATE public.relationship_edge e SET confidence=0,provenance=jsonb_build_object(
    'lifecycle',CASE WHEN e.status='rejected' THEN 'unsupported' ELSE 'retained_support_needs_review' END,
    'human_review_status',CASE WHEN COALESCE(e.provenance->>'human_review_status',e.status) IN('confirmed','rejected','pending','held','reviewed','selected') THEN COALESCE(e.provenance->>'human_review_status',e.status) END,
    'atom_ids',COALESCE((SELECT jsonb_agg(x.id) FROM jsonb_array_elements_text(CASE WHEN jsonb_typeof(e.provenance->'atom_ids')='array' THEN e.provenance->'atom_ids' ELSE '[]'::jsonb END) x(id) JOIN public.atoms a ON a.graph_id=e.graph_id AND a.atom_id::text=x.id WHERE public.library_atom_permitted(a.graph_id,a.atom_id)),'[]'::jsonb),
    'semantic_atom_ids',COALESCE((SELECT jsonb_agg(x.id) FROM jsonb_array_elements_text(CASE WHEN jsonb_typeof(e.provenance->'semantic_atom_ids')='array' THEN e.provenance->'semantic_atom_ids' ELSE '[]'::jsonb END) x(id) JOIN public.semantic_atoms a ON a.graph_id=e.graph_id AND a.semantic_atom_id::text=x.id JOIN public.source_versions v ON v.graph_id=a.graph_id AND v.version_id=a.source_version_id JOIN public.source_items si ON si.graph_id=v.graph_id AND si.source_item_id=v.source_item_id WHERE si.access_state='permitted'),'[]'::jsonb),
    'source_edge_ids',COALESCE((SELECT jsonb_agg(x.id) FROM jsonb_array_elements_text(CASE WHEN jsonb_typeof(e.provenance->'source_edge_ids')='array' THEN e.provenance->'source_edge_ids' ELSE '[]'::jsonb END) x(id) JOIN public.relationship_edge parent ON parent.graph_id=e.graph_id AND parent.edge_id::text=x.id WHERE parent.status='confirmed' AND parent.provenance->>'lifecycle' IS DISTINCT FROM 'unsupported'),'[]'::jsonb))
    WHERE e.graph_id=d.graph_id AND d.object_id IN(e.src_node_id,e.dst_node_id);
   DELETE FROM public.node_merges WHERE graph_id=d.graph_id AND (keep_id=d.object_id OR dup_id=d.object_id);
   DELETE FROM public.reef_identifiers WHERE graph_id=d.graph_id AND node_id=d.object_id AND source NOT IN('account','human','user');
   DELETE FROM public.op_log WHERE graph_id=d.graph_id AND entity_id=d.object_id::text;
   UPDATE public.nodes SET body=CASE WHEN body->>'human_review_status' IN('confirmed','rejected','pending','held','reviewed','selected') THEN jsonb_build_object('human_review_status',body->>'human_review_status','lifecycle','evidence_retracted') ELSE '{"lifecycle":"evidence_retracted"}'::jsonb END,version=version+1,updated_at=now() WHERE graph_id=d.graph_id AND node_id=d.object_id;
   DELETE FROM public.nodes n WHERE n.graph_id=d.graph_id AND n.node_id=d.object_id AND public.library_node_title(n.graph_id,n.node_id) IS NULL
    AND NOT EXISTS(SELECT 1 FROM public.graphs sg WHERE sg.self_node_id=n.node_id) AND NOT (n.body ? 'human_review_status') AND NOT EXISTS(SELECT 1 FROM public.reef_identifiers i WHERE i.graph_id=n.graph_id AND i.node_id=n.node_id AND i.source IN('account','human','user'));
  ELSE
   -- Never delete an object still referenced by another retained source or
   -- independently readable coral. Object graph/account comes from persisted
   -- ownership, not the event actor or a client-supplied locator.
   IF EXISTS(SELECT 1 FROM public.source_purge_dependencies p JOIN public.atoms a ON a.atom_id=p.object_id AND a.graph_id=p.graph_id WHERE p.event_id=d.event_id AND p.kind='atom' AND p.processed_at IS NULL AND a.content_blob_key=d.blob_key) THEN CONTINUE;END IF;
   IF NOT EXISTS(SELECT 1 FROM public.source_versions WHERE raw_blob_key=d.blob_key)
    AND NOT EXISTS(SELECT 1 FROM public.atoms WHERE content_blob_key=d.blob_key)
    AND NOT EXISTS(SELECT 1 FROM public.meeting_transcripts WHERE raw_blob_key=d.blob_key)
    AND NOT EXISTS(SELECT 1 FROM public.coding_sessions WHERE raw_blob_key=d.blob_key) THEN
    parts:=string_to_array(d.blob_key,'/');
    IF cardinality(parts) NOT IN(2,3) OR parts[cardinality(parts)-1]!~*'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' OR parts[cardinality(parts)]!~*'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
     UPDATE public.source_purge_dependencies SET error_category='invalid_locator' WHERE dependency_id=d.dependency_id;CONTINUE;
    END IF;
    owner_graph:=parts[cardinality(parts)-1]::uuid;
    -- Only a root's own namespace is authorized. Promoted foreign locators
    -- are cleaned by the already captured root dependency, never adopted.
    IF owner_graph<>d.graph_id OR (d.blob_owner_org_id IS NULL AND cardinality(parts)<>2) OR (d.blob_owner_org_id IS NOT NULL AND (cardinality(parts)<>3 OR parts[1]<>d.blob_owner_org_id)) THEN UPDATE public.source_purge_dependencies SET error_category='invalid_locator' WHERE dependency_id=d.dependency_id;CONTINUE;END IF;
    SELECT account_id INTO owner_account FROM public.graphs WHERE id=owner_graph;
    IF owner_account IS NULL THEN owner_account:=d.blob_owner_account_id; END IF;
    IF owner_account IS NULL THEN UPDATE public.source_purge_dependencies SET error_category='ownership_unavailable' WHERE dependency_id=d.dependency_id;CONTINUE;END IF;
    INSERT INTO public.jobs(kind,payload,account_id,idempotency_key) VALUES('DeleteSourceBlob',jsonb_build_object('graph_id',owner_graph,'account_id',owner_account,'org_id',CASE WHEN cardinality(parts)=3 THEN parts[1] END,'blob_key',d.blob_key),owner_account,'purge-blob:'||d.dependency_id::text) RETURNING id INTO job;
    UPDATE public.source_purge_dependencies SET cleanup_job_id=job WHERE dependency_id=d.dependency_id;
   END IF;
  END IF;
  UPDATE public.source_purge_dependencies SET processed_at=now() WHERE dependency_id=d.dependency_id;
  done:=done+1;
 END LOOP;
 UPDATE public.source_purge_events e SET sql_processed_at=now() WHERE e.account_id=initial_scope::uuid AND sql_processed_at IS NULL AND NOT EXISTS(SELECT 1 FROM public.source_purge_dependencies pending WHERE pending.event_id=e.event_id AND pending.processed_at IS NULL);
 PERFORM set_config('app.account_id',COALESCE(initial_scope,''),true);
 RETURN done;
END $$;
REVOKE ALL ON FUNCTION library_capture_purge(UUID,UUID,TEXT),library_source_purge_signal(),library_graph_purge_signal(),library_drain_purge(INTEGER),library_keep_retracted_atom(),library_bind_derived_copy(),library_seal_publication() FROM PUBLIC;
DO $$ DECLARE r TEXT; BEGIN
 FOREACH r IN ARRAY ARRAY['coralswarm_app','coralswarm_tenant_app'] LOOP
  IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname=r) THEN
   EXECUTE format('GRANT SELECT ON source_purge_events,source_purge_dependencies,source_purge_reviews TO %I',r);
   EXECUTE format('GRANT SELECT,INSERT,UPDATE ON selected_evidence_publications TO %I',r);
   EXECUTE format('GRANT EXECUTE ON FUNCTION library_permitted_atoms(UUID,UUID[]),library_drain_purge(INTEGER) TO %I',r);
  END IF;
 END LOOP;
END $$;

-- Actor-specific loss is distinct from global provider uncertainty. Originals
-- and other actors' independently allowed inputs remain retained.
CREATE FUNCTION library_owner_outputs_retracted(acc UUID,u UUID) RETURNS VOID
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public AS $$
DECLARE ids UUID[];
BEGIN
 SELECT array_agg(receipt_id) INTO ids FROM public.answer_support_receipts WHERE owner_user_id=u AND graph_id IN(SELECT id FROM public.graphs WHERE account_id=acc);
 UPDATE public.answer_support_receipts SET evidence_complete=false WHERE receipt_id=ANY(ids);
 UPDATE public.conversation_messages m SET text='[Access no longer available]',citations='[]',sources='[]',timing='{}',scope_graph_ids='[]' FROM public.conversations c WHERE m.conversation_id=c.id AND m.role='assistant' AND c.user_id=u AND c.account_id=acc;
 UPDATE public.conversations SET title='' WHERE user_id=u AND account_id=acc;
 DELETE FROM public.query_history WHERE user_id=u AND account_id=acc;
 UPDATE public.mcp_tasks SET status='failed',result=NULL,error='{"code":"access_retracted","message":"Access no longer available"}',result_sha256=NULL,updated_at=now() WHERE owner_user_id=u AND (account_id=acc OR scope_owner_account_id=acc);
 DELETE FROM public.source_items t USING public.selected_evidence_publications p WHERE p.account_id=acc AND p.owner_user_id=u AND p.status='active' AND t.graph_id=p.target_graph_id AND t.source_item_id=p.target_source_item_id AND EXISTS(SELECT 1 FROM public.source_versions v WHERE v.graph_id=t.graph_id AND v.source_item_id=t.source_item_id AND v.source_metadata->>'publication_receipt_id'=p.publication_id::text);
 UPDATE public.selected_evidence_publications SET status='retracted',retracted_at=now() WHERE account_id=acc AND owner_user_id=u AND status='active';
END $$;
CREATE FUNCTION library_member_purge_signal() RETURNS trigger
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public AS $$ BEGIN
 PERFORM public.library_owner_outputs_retracted(OLD.account_id,OLD.user_id);RETURN OLD;END $$;
DO $$ BEGIN IF to_regclass('public.account_members') IS NOT NULL THEN
 CREATE TRIGGER owner_cache_membership_deleted AFTER DELETE ON public.account_members FOR EACH ROW EXECUTE FUNCTION library_member_purge_signal();
END IF;END $$;
CREATE FUNCTION library_graph_access_purge_signal() RETURNS trigger
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public AS $$ DECLARE ev UUID;BEGIN
 IF (NEW.kind IS DISTINCT FROM OLD.kind OR NEW.owner_user_id IS DISTINCT FROM OLD.owner_user_id OR (OLD.archived_at IS NULL AND NEW.archived_at IS NOT NULL)) THEN
  UPDATE public.source_items SET access_state='unknown' WHERE graph_id=OLD.id AND access_state='permitted';
  ev:=public.library_capture_purge(OLD.id,'00000000-0000-0000-0000-000000000000','unknown');
  INSERT INTO public.source_purge_dependencies(event_id,kind,graph_id,object_id) SELECT ev,'atom',OLD.id,atom_id FROM public.atoms WHERE graph_id=OLD.id ON CONFLICT DO NOTHING;
  PERFORM public.library_capture_purge(OLD.id,'00000000-0000-0000-0000-000000000000','unknown');
 END IF;
 RETURN NEW;END $$;
CREATE TRIGGER source_graph_permission_loss AFTER UPDATE OF kind,owner_user_id,archived_at ON graphs FOR EACH ROW EXECUTE FUNCTION library_graph_access_purge_signal();
REVOKE ALL ON FUNCTION library_owner_outputs_retracted(UUID,UUID),library_member_purge_signal(),library_graph_access_purge_signal() FROM PUBLIC;

CREATE FUNCTION library_stamp_blob_owner() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public AS $$ BEGIN
 IF NEW.kind='blob' THEN SELECT account_id INTO NEW.blob_owner_account_id FROM public.graphs WHERE id=NEW.graph_id;
 IF to_regclass('public.tenant_meta') IS NOT NULL THEN EXECUTE 'SELECT org_id FROM public.tenant_meta WHERE id=true' INTO NEW.blob_owner_org_id;END IF;
 END IF;RETURN NEW;END $$;
CREATE TRIGGER purge_blob_owner BEFORE INSERT ON source_purge_dependencies FOR EACH ROW EXECUTE FUNCTION library_stamp_blob_owner();
REVOKE ALL ON FUNCTION library_stamp_blob_owner() FROM PUBLIC;

CREATE FUNCTION library_validate_publication() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public AS $$ BEGIN
 IF NOT EXISTS(SELECT 1 FROM public.graphs sg JOIN public.source_items si ON si.graph_id=sg.id JOIN public.source_versions sv ON sv.graph_id=si.graph_id AND sv.source_item_id=si.source_item_id,
  public.graphs tg JOIN public.source_items ti ON ti.graph_id=tg.id JOIN public.source_versions tv ON tv.graph_id=ti.graph_id AND tv.source_item_id=ti.source_item_id
  WHERE sg.id=NEW.source_graph_id AND sg.account_id=NEW.account_id AND ((sg.kind='member' AND sg.owner_user_id=NEW.owner_user_id) OR sg.kind='personal') AND si.source_item_id=NEW.source_item_id AND sv.version_id=NEW.source_version_id AND si.access_state='permitted'
   AND tg.id=NEW.target_graph_id AND tg.account_id=NEW.account_id AND tg.kind IN('team','org') AND ti.source_item_id=NEW.target_source_item_id AND tv.version_id=NEW.target_source_version_id
   AND tv.extraction_version='selected-passages-v1' AND tv.source_metadata->>'publication_receipt_id'=NEW.publication_id::text AND tv.content_sha256=NEW.selected_sha256)
 THEN RAISE EXCEPTION 'publication ownership mismatch';END IF;RETURN NEW;END $$;
CREATE TRIGGER selected_publication_qualified BEFORE INSERT ON selected_evidence_publications FOR EACH ROW EXECUTE FUNCTION library_validate_publication();
REVOKE ALL ON FUNCTION library_validate_publication() FROM PUBLIC;

CREATE FUNCTION library_version_purge_signal() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public AS $$ BEGIN
 IF pg_trigger_depth()=1 THEN PERFORM public.library_capture_purge(OLD.graph_id,OLD.source_item_id,'deleted');END IF;RETURN OLD;END $$;
CREATE TRIGGER source_version_purge_delete BEFORE DELETE ON source_versions FOR EACH ROW EXECUTE FUNCTION library_version_purge_signal();
REVOKE ALL ON FUNCTION library_version_purge_signal() FROM PUBLIC;

-- Captured versions are insert-new on normal ingestion. Consent additionally
-- seals its exact byte/locator identity against in-place corrections/rebinding.
-- Progress/status updates remain allowed; no new source identity is invented.
CREATE FUNCTION library_preserve_published_version() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public AS $$ BEGIN
 IF (OLD.graph_id,OLD.source_item_id,OLD.version_id,OLD.raw_inline,OLD.raw_blob_key,OLD.content_sha256,OLD.extraction_version) IS DISTINCT FROM
    (NEW.graph_id,NEW.source_item_id,NEW.version_id,NEW.raw_inline,NEW.raw_blob_key,NEW.content_sha256,NEW.extraction_version)
  AND EXISTS(SELECT 1 FROM public.selected_evidence_publications p WHERE p.status='active' AND (p.source_version_id=OLD.version_id OR p.target_source_version_id=OLD.version_id)) THEN RAISE EXCEPTION 'consented version byte identity is immutable';END IF;
 IF (NEW.source_metadata,NEW.origin) IS DISTINCT FROM (OLD.source_metadata,OLD.origin)
  AND EXISTS(SELECT 1 FROM public.selected_evidence_publications p WHERE p.status='active' AND p.target_source_version_id=OLD.version_id) THEN RAISE EXCEPTION 'publication provenance is immutable';END IF;
 RETURN NEW;END $$;
CREATE TRIGGER selected_version_immutable BEFORE UPDATE ON source_versions FOR EACH ROW EXECUTE FUNCTION library_preserve_published_version();
REVOKE ALL ON FUNCTION library_preserve_published_version() FROM PUBLIC;

-- Genuine legacy coral removal has no surviving source_items row to signal.
-- Capture its exact UUID/body-free dependencies before atom/mapping cascades.
CREATE FUNCTION library_atom_purge_signal() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public AS $$
DECLARE ev UUID;
BEGIN
 IF pg_trigger_depth()>1 OR EXISTS(SELECT 1 FROM public.source_purge_dependencies WHERE kind='atom' AND graph_id=OLD.graph_id AND object_id=OLD.atom_id AND processed_at IS NULL) THEN RETURN OLD;END IF;
 ev:=public.library_capture_purge(OLD.graph_id,'00000000-0000-0000-0000-000000000000','retracted');
 IF ev IS NULL THEN RETURN OLD;END IF;
 INSERT INTO public.source_purge_dependencies(event_id,kind,graph_id,object_id) VALUES(ev,'atom',OLD.graph_id,OLD.atom_id) ON CONFLICT DO NOTHING;
 IF OLD.content_blob_key IS NOT NULL AND (NOT OLD.derived_copy_bound OR split_part(OLD.content_blob_key,'/',GREATEST(1,array_length(string_to_array(OLD.content_blob_key,'/'),1)-1))=OLD.graph_id::text) THEN
  INSERT INTO public.source_purge_dependencies(event_id,kind,graph_id,object_id,blob_key) VALUES(ev,'blob',OLD.graph_id,OLD.atom_id,OLD.content_blob_key) ON CONFLICT DO NOTHING;
 END IF;
 PERFORM public.library_capture_purge(OLD.graph_id,'00000000-0000-0000-0000-000000000000','retracted');
 RETURN OLD;
END $$;
CREATE TRIGGER atom_purge_delete BEFORE DELETE ON atoms FOR EACH ROW EXECUTE FUNCTION library_atom_purge_signal();
REVOKE ALL ON FUNCTION library_atom_purge_signal() FROM PUBLIC;

-- Queue-envelope bootstrap returns only retained account UUIDs. The worker must
-- acquire an explicit account/system-scoped transaction before calling drain.
-- No source UUID, locator, payload or arbitrary client dependency is accepted.
CREATE FUNCTION library_pending_purge_accounts(batch INTEGER DEFAULT 20) RETURNS SETOF UUID LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public AS $$ BEGIN
 IF batch<1 OR batch>20 THEN RAISE EXCEPTION 'invalid purge account batch';END IF;
 RETURN QUERY SELECT DISTINCT e.account_id FROM public.source_purge_events e JOIN public.source_purge_dependencies d ON d.event_id=e.event_id WHERE d.processed_at IS NULL AND d.error_category IS NULL ORDER BY e.account_id LIMIT batch;
END $$;
REVOKE ALL ON FUNCTION library_pending_purge_accounts(INTEGER) FROM PUBLIC;
DO $$ DECLARE r TEXT;BEGIN FOREACH r IN ARRAY ARRAY['coralswarm_app','coralswarm_tenant_app'] LOOP IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname=r) THEN EXECUTE format('GRANT EXECUTE ON FUNCTION library_pending_purge_accounts(INTEGER) TO %I',r);END IF;END LOOP;END $$;

-- Standing whole-body consent is incompatible with selected-version consent.
-- Stop discovery/retries before any old pending publication can read private
-- body/header data. Consent records remain retractable; labels are not evidence.
UPDATE source_connections SET paused=true,error_message='Selected retained passage consent is required',current_job_id=NULL;
-- Existing formerly-bound orphan and whole-coral copies must not wait for a
-- future provider event. Deliver cleanup through the same bounded durable path.
DO $$ DECLARE g UUID;ev UUID;BEGIN
 FOR g IN SELECT DISTINCT graph_id FROM atoms a WHERE a.derived_copy_bound OR (a.source_evidence_bound AND NOT library_atom_permitted(a.graph_id,a.atom_id)) ORDER BY graph_id LOOP
  ev:=library_capture_purge(g,'00000000-0000-0000-0000-000000000000','retracted');
  INSERT INTO source_purge_dependencies(event_id,kind,graph_id,object_id)
   SELECT ev,'atom',g,a.atom_id FROM atoms a WHERE a.graph_id=g AND (a.derived_copy_bound OR (a.source_evidence_bound AND NOT library_atom_permitted(a.graph_id,a.atom_id))) ON CONFLICT DO NOTHING;
  PERFORM library_capture_purge(g,'00000000-0000-0000-0000-000000000000','retracted');
 END LOOP;
END $$;
-- Narrow trusted annotation-retraction seam. It accepts only server-selected
-- receipt UUIDs, filters their primary graph/account, and mutates no source,
-- semantic evidence or publication. No application/PUBLIC execution grant.
CREATE FUNCTION library_retract_answer_receipts(g UUID,receipt_ids UUID[]) RETURNS INTEGER
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public AS $$
DECLARE acc UUID;ids UUID[];affected INTEGER;
BEGIN
 SELECT account_id INTO acc FROM public.graphs WHERE id=g;
 IF acc IS NULL THEN RETURN 0;END IF;
 SELECT array_agg(r.receipt_id) INTO ids FROM public.answer_support_receipts r WHERE r.graph_id=g AND r.receipt_id=ANY(receipt_ids);
 IF ids IS NULL THEN RETURN 0;END IF;
 UPDATE public.answer_support_receipts SET evidence_complete=false WHERE graph_id=g AND receipt_id=ANY(ids);
 GET DIAGNOSTICS affected=ROW_COUNT;
 UPDATE public.conversations c SET title='' WHERE c.account_id=acc AND (c.id=ANY(ids) OR c.id IN(SELECT m.conversation_id FROM public.conversation_messages m WHERE m.id=ANY(ids)));
 UPDATE public.conversation_messages m SET text='[Evidence no longer available]',citations='[]',sources='[]',timing='{}',scope_graph_ids='[]' WHERE m.role='assistant' AND m.id=ANY(ids) AND EXISTS(SELECT 1 FROM public.conversations c WHERE c.id=m.conversation_id AND c.account_id=acc);
 DELETE FROM public.query_history WHERE account_id=acc AND id=ANY(ids);
 UPDATE public.queries SET answer=NULL WHERE account_id=acc AND id=ANY(ids);
 UPDATE public.mcp_tasks SET status='failed',result=NULL,error='{"code":"evidence_retracted","message":"Evidence no longer available"}',result_sha256=NULL,updated_at=now() WHERE answer_receipt_id=ANY(ids) AND scope_owner_account_id=acc;
 RETURN affected;
END $$;
REVOKE ALL ON FUNCTION library_retract_answer_receipts(UUID,UUID[]) FROM PUBLIC;

-- Known-new uploads are different from arbitrary orphan discovery. The trusted
-- native writer records successful server-generated PUTs only AFTER releasing
-- its capture transaction. Full xid and database identity bind the outcome to
-- this plane; once verified aborted, the attestation survives xid aging.
CREATE TABLE unattached_capture_uploads (
 cleanup_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),account_id UUID NOT NULL,
 graph_id UUID NOT NULL,object_id UUID NOT NULL,org_id TEXT,blob_key TEXT NOT NULL,
 capture_database TEXT NOT NULL,capture_xid TEXT NOT NULL CHECK(capture_xid~'^[0-9]+$'),
 abort_verified BOOLEAN NOT NULL DEFAULT false,
 state TEXT NOT NULL DEFAULT 'pending' CHECK(state IN('pending','deleting','deleted','committed','retained_reference')),
 created_at TIMESTAMPTZ NOT NULL DEFAULT now(),attempted_at TIMESTAMPTZ,completed_at TIMESTAMPTZ,
 UNIQUE(blob_key),CHECK(blob_key=CASE WHEN org_id IS NULL THEN graph_id::text||'/'||object_id::text ELSE org_id||'/'||graph_id::text||'/'||object_id::text END),
 CHECK(org_id IS NULL OR org_id~'^org_[A-Za-z0-9_]+$')
);
ALTER TABLE unattached_capture_uploads ENABLE ROW LEVEL SECURITY;
CREATE POLICY unattached_capture_scope ON unattached_capture_uploads USING(account_id=NULLIF(current_setting('app.account_id',true),'')::uuid);
CREATE FUNCTION library_seal_upload_cleanup() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF (to_jsonb(NEW)-ARRAY['abort_verified','state','attempted_at','completed_at']) IS DISTINCT FROM (to_jsonb(OLD)-ARRAY['abort_verified','state','attempted_at','completed_at']) OR (OLD.abort_verified AND NOT NEW.abort_verified) OR (OLD.state IN('deleted','committed') AND NEW.state<>OLD.state) THEN RAISE EXCEPTION 'capture cleanup identity is immutable';END IF;RETURN NEW;END $$;
CREATE TRIGGER capture_cleanup_identity BEFORE UPDATE ON unattached_capture_uploads FOR EACH ROW EXECUTE FUNCTION library_seal_upload_cleanup();
-- Narrow service operation, qualified by an existing explicit account scope.
-- A live graph can never disagree with the retained ownership stamp. No source
-- existence, source access restoration or public content-read grant is involved.
CREATE FUNCTION library_record_unattached_upload(acc UUID,g UUID,obj UUID,org TEXT,key TEXT,db TEXT,xid TEXT) RETURNS UUID LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public AS $$
DECLARE result UUID;live_acc UUID;existing public.unattached_capture_uploads%ROWTYPE;
BEGIN
 IF acc IS DISTINCT FROM NULLIF(current_setting('app.account_id',true),'')::uuid OR db<>current_database() THEN RAISE EXCEPTION 'capture cleanup scope mismatch';END IF;
 SELECT account_id INTO live_acc FROM public.graphs WHERE id=g;
 IF live_acc IS NOT NULL AND live_acc<>acc THEN RAISE EXCEPTION 'capture cleanup owner mismatch';END IF;
 IF to_regclass('public.tenant_meta') IS NOT NULL THEN
  IF org IS DISTINCT FROM (SELECT org_id FROM public.tenant_meta WHERE id=true) THEN RAISE EXCEPTION 'capture cleanup namespace mismatch';END IF;
 ELSIF org IS NOT NULL THEN RAISE EXCEPTION 'capture cleanup namespace mismatch';END IF;
 PERFORM pg_advisory_xact_lock(hashtextextended('capture_cleanup:'||key,0));
 SELECT * INTO existing FROM public.unattached_capture_uploads WHERE blob_key=key;
 IF FOUND THEN
  IF existing.account_id<>acc OR existing.graph_id<>g OR existing.object_id<>obj OR existing.capture_xid<>xid OR existing.capture_database<>db OR existing.org_id IS DISTINCT FROM org THEN RAISE EXCEPTION 'capture cleanup identity mismatch';END IF;
  RETURN existing.cleanup_id;
 END IF;
 INSERT INTO public.unattached_capture_uploads(account_id,graph_id,object_id,org_id,blob_key,capture_database,capture_xid) VALUES(acc,g,obj,org,key,db,xid) RETURNING cleanup_id INTO result;
 RETURN result;
END $$;
CREATE FUNCTION library_stage_unattached_upload(id UUID) RETURNS BOOLEAN LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public AS $$
DECLARE item public.unattached_capture_uploads%ROWTYPE;status TEXT;
BEGIN
 SELECT * INTO item FROM public.unattached_capture_uploads WHERE cleanup_id=id AND account_id=NULLIF(current_setting('app.account_id',true),'')::uuid FOR UPDATE SKIP LOCKED;
 -- The app role holds SELECT only; this definer owns the row lock. A row a
 -- concurrent worker holds is skipped, never double-staged.
 IF NOT FOUND OR item.state IN('deleted','committed') THEN RETURN false;END IF;
 PERFORM pg_advisory_xact_lock(hashtextextended('capture_cleanup:'||item.blob_key,0));
 IF item.capture_database<>current_database() THEN RETURN false;END IF;
 IF NOT item.abort_verified THEN
  status:=pg_xact_status(item.capture_xid::xid8);
  IF status='committed' THEN UPDATE public.unattached_capture_uploads SET state='committed',attempted_at=now() WHERE cleanup_id=id;RETURN false;END IF;
  IF status IS DISTINCT FROM 'aborted' THEN UPDATE public.unattached_capture_uploads SET attempted_at=now() WHERE cleanup_id=id;RETURN false;END IF;
  UPDATE public.unattached_capture_uploads SET abort_verified=true WHERE cleanup_id=id;
 END IF;
 -- Check canonical AND legacy refs, including restricted/private surviving
 -- owners. This service-only predicate returns no content or foreign identity.
 IF EXISTS(SELECT 1 FROM public.source_versions WHERE raw_blob_key=item.blob_key)
 OR EXISTS(SELECT 1 FROM public.atoms WHERE content_blob_key=item.blob_key)
 OR EXISTS(SELECT 1 FROM public.meeting_transcripts WHERE raw_blob_key=item.blob_key)
 OR EXISTS(SELECT 1 FROM public.coding_sessions WHERE raw_blob_key=item.blob_key) THEN
  UPDATE public.unattached_capture_uploads SET state='retained_reference',attempted_at=now() WHERE cleanup_id=id;RETURN false;
 END IF;
 UPDATE public.unattached_capture_uploads SET state='deleting',attempted_at=now() WHERE cleanup_id=id;RETURN true;
END $$;
CREATE FUNCTION library_finish_unattached_upload(id UUID,deleted BOOLEAN) RETURNS VOID LANGUAGE SQL SECURITY DEFINER SET search_path=pg_catalog,public AS $$
 UPDATE public.unattached_capture_uploads SET state=CASE WHEN deleted THEN 'deleted' ELSE 'pending' END,completed_at=CASE WHEN deleted THEN now() ELSE NULL END WHERE cleanup_id=id AND account_id=NULLIF(current_setting('app.account_id',true),'')::uuid AND state='deleting' AND abort_verified
$$;
-- Normal reference writers serialize with deletion. A successful known-orphan
-- delete cannot race a new reference to the same server-generated identity.
CREATE FUNCTION library_fence_cleanup_reference() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public AS $$
DECLARE key TEXT;oldkey TEXT;
BEGIN
 IF TG_TABLE_NAME='atoms' THEN key:=NEW.content_blob_key;IF TG_OP='UPDATE' THEN oldkey:=OLD.content_blob_key;END IF;
 ELSE key:=NEW.raw_blob_key;IF TG_OP='UPDATE' THEN oldkey:=OLD.raw_blob_key;END IF;END IF;
 IF key IS NULL OR (TG_OP='UPDATE' AND key IS NOT DISTINCT FROM oldkey) THEN RETURN NEW;END IF;
 PERFORM pg_advisory_xact_lock(hashtextextended('capture_cleanup:'||key,0));
 IF EXISTS(SELECT 1 FROM public.unattached_capture_uploads WHERE blob_key=key AND state IN('deleting','deleted')) THEN RAISE EXCEPTION 'capture object no longer attachable';END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER cleanup_source_reference BEFORE INSERT OR UPDATE OF raw_blob_key ON source_versions FOR EACH ROW EXECUTE FUNCTION library_fence_cleanup_reference();
CREATE TRIGGER cleanup_atom_reference BEFORE INSERT OR UPDATE OF content_blob_key ON atoms FOR EACH ROW EXECUTE FUNCTION library_fence_cleanup_reference();
CREATE TRIGGER cleanup_meeting_reference BEFORE INSERT OR UPDATE OF raw_blob_key ON meeting_transcripts FOR EACH ROW EXECUTE FUNCTION library_fence_cleanup_reference();
CREATE TRIGGER cleanup_session_reference BEFORE INSERT OR UPDATE OF raw_blob_key ON coding_sessions FOR EACH ROW EXECUTE FUNCTION library_fence_cleanup_reference();
CREATE FUNCTION library_pending_upload_cleanup_accounts(batch INTEGER DEFAULT 20) RETURNS SETOF UUID LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public AS $$ BEGIN
 IF batch<1 OR batch>20 THEN RAISE EXCEPTION 'invalid cleanup batch';END IF;
 RETURN QUERY SELECT account_id FROM public.unattached_capture_uploads WHERE state IN('pending','retained_reference') AND (attempted_at IS NULL OR attempted_at<now()-interval '30 seconds') GROUP BY account_id ORDER BY min(attempted_at) NULLS FIRST,account_id LIMIT batch;
END $$;
REVOKE ALL ON FUNCTION library_record_unattached_upload(UUID,UUID,UUID,TEXT,TEXT,TEXT,TEXT),library_stage_unattached_upload(UUID),library_finish_unattached_upload(UUID,BOOLEAN),library_pending_upload_cleanup_accounts(INTEGER),library_fence_cleanup_reference(),library_seal_upload_cleanup() FROM PUBLIC;
DO $$ DECLARE r TEXT;BEGIN FOREACH r IN ARRAY ARRAY['coralswarm_app','coralswarm_tenant_app'] LOOP IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname=r) THEN
 EXECUTE format('REVOKE INSERT,UPDATE,DELETE ON unattached_capture_uploads FROM %I',r);
 EXECUTE format('GRANT SELECT ON unattached_capture_uploads TO %I',r);
 EXECUTE format('GRANT EXECUTE ON FUNCTION library_record_unattached_upload(UUID,UUID,UUID,TEXT,TEXT,TEXT,TEXT),library_stage_unattached_upload(UUID),library_finish_unattached_upload(UUID,BOOLEAN),library_pending_upload_cleanup_accounts(INTEGER) TO %I',r);
END IF;END LOOP;END $$;

-- Team removal is a specific user's permission loss on both planes (the tenant
-- plane has no account_members). Clear only that user's cached answers that
-- touched the team graph; originals and other members' outputs are retained.
CREATE FUNCTION library_team_member_purge_signal() RETURNS trigger
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public AS $$
DECLARE g UUID;ids UUID[];
BEGIN
 SELECT t.graph_id INTO g FROM public.teams t WHERE t.id=OLD.team_id;
 IF g IS NULL OR OLD.user_id IS NULL THEN RETURN OLD;END IF;
 SELECT array_agg(r.receipt_id) INTO ids FROM public.answer_support_receipts r WHERE r.owner_user_id=OLD.user_id AND (r.graph_id=g OR r.support @> jsonb_build_array(jsonb_build_object('graph_id',g)));
 UPDATE public.answer_support_receipts SET evidence_complete=false WHERE receipt_id=ANY(ids);
 UPDATE public.conversation_messages m SET text='[Access no longer available]',citations='[]',sources='[]',timing='{}',scope_graph_ids='[]' FROM public.conversations c
  WHERE m.conversation_id=c.id AND m.role='assistant' AND c.user_id=OLD.user_id AND (m.id=ANY(ids) OR c.graph_id=g OR m.scope_graph_ids @> to_jsonb(ARRAY[g::text]));
 DELETE FROM public.query_history WHERE user_id=OLD.user_id AND (graph_id=g OR id=ANY(ids));
 UPDATE public.mcp_tasks SET status='failed',result=NULL,error='{"code":"access_retracted","message":"Access no longer available"}',result_sha256=NULL,updated_at=now()
  WHERE owner_user_id=OLD.user_id AND status IN('working','completed') AND (graph_id=g OR answer_receipt_id=ANY(ids));
 RETURN OLD;
END $$;
DO $$ BEGIN IF to_regclass('public.team_members') IS NOT NULL THEN
 CREATE TRIGGER owner_cache_team_membership_deleted AFTER DELETE ON public.team_members FOR EACH ROW EXECUTE FUNCTION library_team_member_purge_signal();
END IF;END $$;
REVOKE ALL ON FUNCTION library_team_member_purge_signal() FROM PUBLIC;
