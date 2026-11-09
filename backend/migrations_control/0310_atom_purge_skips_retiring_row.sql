-- atom_purge_delete is a BEFORE DELETE trigger that registers the deleted atom
-- as a purge dependency and re-runs library_capture_purge, whose
-- "UPDATE atoms SET evidence_retracted=true" then touched the row being deleted.
-- Postgres refuses that. The retiring row needs no flag; surviving copies do.
DROP FUNCTION library_capture_purge(UUID,UUID,TEXT);
CREATE FUNCTION library_capture_purge(g UUID,s_id UUID,k TEXT,retiring_atom UUID DEFAULT NULL) RETURNS UUID
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
 UPDATE public.atoms a SET evidence_retracted=true WHERE a.atom_id IS DISTINCT FROM retiring_atom AND EXISTS(SELECT 1 FROM public.source_purge_dependencies d WHERE d.event_id=ev AND d.kind='atom' AND d.graph_id=a.graph_id AND d.object_id=a.atom_id);
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
REVOKE ALL ON FUNCTION library_capture_purge(UUID,UUID,TEXT,UUID) FROM PUBLIC;
CREATE OR REPLACE FUNCTION library_atom_purge_signal() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public AS $$
DECLARE ev UUID;
BEGIN
 IF pg_trigger_depth()>1 OR EXISTS(SELECT 1 FROM public.source_purge_dependencies WHERE kind='atom' AND graph_id=OLD.graph_id AND object_id=OLD.atom_id AND processed_at IS NULL) THEN RETURN OLD;END IF;
 ev:=public.library_capture_purge(OLD.graph_id,'00000000-0000-0000-0000-000000000000','retracted',OLD.atom_id);
 IF ev IS NULL THEN RETURN OLD;END IF;
 INSERT INTO public.source_purge_dependencies(event_id,kind,graph_id,object_id) VALUES(ev,'atom',OLD.graph_id,OLD.atom_id) ON CONFLICT DO NOTHING;
 IF OLD.content_blob_key IS NOT NULL AND (NOT OLD.derived_copy_bound OR split_part(OLD.content_blob_key,'/',GREATEST(1,array_length(string_to_array(OLD.content_blob_key,'/'),1)-1))=OLD.graph_id::text) THEN
  INSERT INTO public.source_purge_dependencies(event_id,kind,graph_id,object_id,blob_key) VALUES(ev,'blob',OLD.graph_id,OLD.atom_id,OLD.content_blob_key) ON CONFLICT DO NOTHING;
 END IF;
 PERFORM public.library_capture_purge(OLD.graph_id,'00000000-0000-0000-0000-000000000000','retracted',OLD.atom_id);
 RETURN OLD;
END $$;
