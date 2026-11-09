
CREATE OR REPLACE FUNCTION library_node_backed_only_by_event(ev UUID,g UUID,node UUID) RETURNS BOOLEAN LANGUAGE SQL STABLE SET search_path=pg_catalog,public AS $$
 SELECT NOT EXISTS(SELECT 1 FROM public.node_atoms l WHERE l.node_id=node AND NOT EXISTS(SELECT 1 FROM public.source_purge_dependencies d WHERE d.event_id=ev AND d.kind='atom' AND d.graph_id=g AND d.object_id=l.atom_id))
  AND NOT EXISTS(SELECT 1 FROM public.node_semantic_atoms s WHERE s.node_id=node)
$$;
REVOKE ALL ON FUNCTION library_node_backed_only_by_event(UUID,UUID,UUID) FROM PUBLIC;
CREATE OR REPLACE FUNCTION library_capture_purge(g UUID,s_id UUID,k TEXT,retiring_atom UUID DEFAULT NULL) RETURNS UUID
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public AS $$
DECLARE ev UUID;acc UUID;owner UUID;receipts UUID[];src TEXT;sessions UUID[];own_atoms UUID[];
 per_atom BOOLEAN:=s_id='00000000-0000-0000-0000-000000000000' AND k='retracted';
 whole_graph BOOLEAN:=s_id='00000000-0000-0000-0000-000000000000' AND k<>'retracted';
 purged_kinds TEXT[]:=ARRAY['ExtractMeetingAtoms','ExtractSourceAtoms','ExtractEntities','ExtractRelationships','IngestDoor','GenerateMeetingSummary','ExtractEntityBatch','RegenerateDigest','EntityResolution','ConflictDetection','InferExpertise','ClassifyKind','ExtractMentions','GenerateTitle','GenerateJournal','PromoteNode','PublishSource'];
BEGIN
 IF per_atom AND retiring_atom IS NULL THEN RAISE EXCEPTION 'a per-atom purge needs its retiring atom'; END IF;
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
  SELECT d.graph_id,d.object_id,0,ARRAY[d.object_id] FROM public.source_purge_dependencies d WHERE d.event_id=ev AND d.kind='atom' AND (NOT per_atom OR d.object_id=retiring_atom)
  UNION ALL SELECT a.graph_id,a.atom_id,c.depth+1,c.path||a.atom_id FROM copies c JOIN public.atoms a ON a.promoted_from_atom_id=c.atom_id OR EXISTS(SELECT 1 FROM public.source_connection_atoms r JOIN public.source_connections sc ON sc.id=r.connection_id WHERE r.atom_id=c.atom_id AND r.target_atom_id=a.atom_id AND sc.source_graph_id=c.graph_id AND sc.target_graph_id=a.graph_id AND sc.account_id=acc)
   JOIN public.graphs dg ON dg.id=a.graph_id AND dg.account_id=acc AND dg.kind IN('team','org') WHERE c.depth<16 AND NOT a.atom_id=ANY(c.path)
 ), recorded AS (INSERT INTO public.source_purge_dependencies(event_id,kind,graph_id,object_id) SELECT DISTINCT ev,'atom',graph_id,atom_id FROM copies ON CONFLICT DO NOTHING)
 SELECT array_agg(DISTINCT atom_id) INTO own_atoms FROM copies WHERE per_atom;
 INSERT INTO public.source_purge_dependencies(event_id,kind,graph_id,object_id)
 SELECT ev,'node',r.graph_id,r.node_id FROM (SELECT DISTINCT n.graph_id,n.node_id FROM public.node_atoms l JOIN public.nodes n ON n.node_id=l.node_id JOIN public.source_purge_dependencies d ON d.event_id=ev AND d.kind='atom' AND d.object_id=l.atom_id AND d.graph_id=n.graph_id WHERE NOT per_atom OR d.object_id=ANY(own_atoms)) r
 WHERE NOT per_atom OR public.library_node_backed_only_by_event(ev,r.graph_id,r.node_id) ON CONFLICT DO NOTHING;
 IF per_atom THEN
  WITH stripped AS (UPDATE public.nodes n SET body=n.body-'digest',digest_coral_count=0,version=n.version+1,updated_at=now()
   WHERE n.body ? 'digest' AND EXISTS(SELECT 1 FROM public.node_atoms l WHERE l.node_id=n.node_id AND l.atom_id=ANY(own_atoms)) RETURNING n.*),
  dropped AS (DELETE FROM public.op_log o USING stripped s WHERE o.graph_id=s.graph_id AND o.entity='node' AND o.entity_id=s.node_id::text)
  INSERT INTO public.op_log(op_id,entity,entity_id,op_type,payload,version,updated_at,device_id,graph_id)
  SELECT gen_random_uuid(),'node',s.node_id::text,'upsert',jsonb_build_object('node_id',s.node_id,'graph_id',s.graph_id,'type',s.type,'title',s.title,'body',s.body::text,'scope',s.scope,'owner_id',s.owner_id,'created_at',to_char(s.created_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"+00:00"'),'updated_at',to_char(s.updated_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"+00:00"'),'version',s.version),s.version,s.updated_at,'evidence-lifecycle',s.graph_id FROM stripped s;
 END IF;
 INSERT INTO public.source_purge_reviews SELECT acc,n.graph_id,n.node_id,'node',n.body->>'human_review_status' FROM public.nodes n JOIN public.source_purge_dependencies d ON d.event_id=ev AND d.kind='node' AND d.graph_id=n.graph_id AND d.object_id=n.node_id WHERE n.body->>'human_review_status' IN('confirmed','rejected','pending','held','reviewed','selected') ON CONFLICT DO NOTHING;
 INSERT INTO public.source_purge_reviews SELECT acc,e.graph_id,e.edge_id,'edge',COALESCE(e.provenance->>'human_review_status',e.status) FROM public.relationship_edge e WHERE EXISTS(SELECT 1 FROM public.source_purge_dependencies d WHERE d.event_id=ev AND d.kind='node' AND d.graph_id=e.graph_id AND d.object_id IN(e.src_node_id,e.dst_node_id)) AND COALESCE(e.provenance->>'human_review_status',e.status) IN('confirmed','rejected','pending','held','reviewed','selected') ON CONFLICT DO NOTHING;
 UPDATE public.atoms a SET evidence_retracted=true WHERE a.atom_id IS DISTINCT FROM retiring_atom AND EXISTS(SELECT 1 FROM public.source_purge_dependencies d WHERE d.event_id=ev AND d.kind='atom' AND d.graph_id=a.graph_id AND d.object_id=a.atom_id AND (NOT per_atom OR d.object_id=ANY(own_atoms)));
 IF k='graph_deleted' THEN
  INSERT INTO public.source_purge_dependencies(event_id,kind,graph_id,object_id,blob_key)
  SELECT ev,'blob',a.graph_id,a.atom_id,a.content_blob_key FROM public.atoms a JOIN public.source_purge_dependencies d ON d.event_id=ev AND d.kind='atom' AND d.graph_id=a.graph_id AND d.object_id=a.atom_id WHERE a.content_blob_key IS NOT NULL AND (NOT a.derived_copy_bound OR split_part(a.content_blob_key,'/',GREATEST(1,array_length(string_to_array(a.content_blob_key,'/'),1)-1))=a.graph_id::text) ON CONFLICT DO NOTHING;
 END IF;
 IF k IN('deleted','graph_deleted') THEN
  INSERT INTO public.source_purge_dependencies(event_id,kind,graph_id,object_id,blob_key)
  SELECT ev,'blob',g,v.version_id,v.raw_blob_key FROM public.source_versions v WHERE v.graph_id=g AND v.source_item_id=s_id AND v.raw_blob_key IS NOT NULL ON CONFLICT DO NOTHING;
 END IF;
 SELECT array_agg(r.receipt_id) INTO receipts FROM public.answer_support_receipts r
 WHERE r.support @> jsonb_build_array(jsonb_build_object('kind','source_passage','graph_id',g,'source_item_id',s_id))
 OR EXISTS(SELECT 1 FROM jsonb_array_elements(r.support) e JOIN public.source_purge_dependencies d ON d.event_id=ev AND d.kind='atom' AND d.graph_id::text=e->>'graph_id' AND d.object_id::text=e->>'atom_id' WHERE e->>'kind'='legacy_chunk' AND (NOT per_atom OR d.object_id=ANY(own_atoms)));
 UPDATE public.answer_support_receipts SET evidence_complete=false WHERE receipt_id=ANY(receipts);
 UPDATE public.conversations SET title='' WHERE id=ANY(receipts) OR id IN(SELECT conversation_id FROM public.conversation_messages WHERE id=ANY(receipts));
 UPDATE public.conversation_messages SET text='[Evidence no longer available]',citations='[]',sources='[]',timing='{}',scope_graph_ids='[]' WHERE id=ANY(receipts) AND role='assistant';
 DELETE FROM public.query_history WHERE id=ANY(receipts);
 UPDATE public.mcp_tasks SET status='failed',result=NULL,error='{"code":"evidence_retracted","message":"Evidence no longer available"}',result_sha256=NULL,updated_at=now() WHERE answer_receipt_id=ANY(receipts);
 IF NOT per_atom THEN
  PERFORM public.invalidate_derived_read_surfaces(g);
  UPDATE public.conversation_messages m SET timing='{}' WHERE m.role='assistant' AND (m.scope_graph_ids @> to_jsonb(ARRAY[g::text]) OR EXISTS(SELECT 1 FROM public.conversations c WHERE c.id=m.conversation_id AND c.graph_id=g));
  UPDATE public.mcp_tasks SET status='failed',result=NULL,error='{"code":"evidence_retracted","message":"Evidence no longer available"}',result_sha256=NULL,updated_at=now() WHERE graph_id=g OR (answer_receipt_id IS NULL AND account_id=acc AND status='completed');
 END IF;
 UPDATE public.meeting_transcripts SET summary=NULL,summary_status='pending',summary_error=NULL,summary_model=NULL,summary_updated_at=NULL WHERE graph_id=g AND (whole_graph OR source_item_id=s_id);
 IF per_atom THEN
  UPDATE public.jobs j SET payload=jsonb_set(j.payload,'{atom_ids}',(SELECT jsonb_agg(x) FROM jsonb_array_elements_text(j.payload->'atom_ids') x WHERE x<>ALL(own_atoms::text[])))
  WHERE j.payload->>'graph_id'=g::text AND j.status IN('pending','failed') AND j.kind=ANY(purged_kinds) AND jsonb_typeof(j.payload->'atom_ids')='array' AND j.payload->'atom_ids' ?| own_atoms::text[]
   AND EXISTS(SELECT 1 FROM jsonb_array_elements_text(j.payload->'atom_ids') x WHERE x<>ALL(own_atoms::text[]));
 END IF;
 UPDATE public.jobs SET status='dead',payload=jsonb_build_object('graph_id',g,'evidence_retracted',true),error_message=NULL,finished_at=now(),idempotency_key=NULL WHERE payload->>'graph_id'=g::text AND (CASE WHEN per_atom THEN status IN('pending','failed') ELSE true END) AND (whole_graph OR payload->>'source_item_id'=s_id::text OR payload->>'source_id'=src OR payload->>'source_version_id' IN(SELECT v.version_id::text FROM public.source_versions v WHERE v.graph_id=g AND v.source_item_id=s_id) OR (per_atom AND (payload->>'atom_id'=ANY(own_atoms::text[]) OR (jsonb_typeof(payload->'atom_ids')='array' AND payload->'atom_ids' ?| own_atoms::text[]))) OR (NOT per_atom AND EXISTS(SELECT 1 FROM public.source_purge_dependencies d WHERE d.event_id=ev AND d.kind='atom' AND (payload->>'atom_id'=d.object_id::text OR (jsonb_typeof(payload->'atom_ids')='array' AND payload->'atom_ids' ? d.object_id::text))))) AND kind=ANY(purged_kinds);
 SELECT array_agg(o.id) INTO sessions FROM (SELECT DISTINCT ON (a.atom_id) cs.id FROM public.source_purge_dependencies d JOIN public.atoms a ON a.graph_id=d.graph_id AND a.atom_id=d.object_id
  JOIN public.coding_sessions cs ON cs.graph_id=g AND a.source_id LIKE ('cc:'||replace(replace(replace(cs.session_id,'\','\\'),'%','\%'),'_','\_')||':%')
  WHERE d.event_id=ev AND d.kind='atom' AND a.graph_id=g AND (NOT per_atom OR d.object_id=ANY(own_atoms)) ORDER BY a.atom_id,length(cs.session_id) DESC) o;
 UPDATE public.coding_sessions cs SET platform_detected=NULL,project_key=NULL,title=NULL,repo_path=NULL,remote_sanitized=NULL,hostname=NULL,harness_version=NULL,model=NULL,branch=NULL,agent_name=NULL,platform=NULL,task=NULL,subagent_name=NULL WHERE cs.graph_id=g AND (whole_graph OR cs.id=ANY(sessions));
 DELETE FROM public.op_log WHERE graph_id=g AND entity IN('query','conversation','conversation_message')
 AND (NOT per_atom OR entity_id IN(SELECT unnest(receipts)::text UNION SELECT conversation_id::text FROM public.conversation_messages WHERE id=ANY(receipts)));
 DELETE FROM public.op_log o WHERE o.graph_id=g AND o.entity='coding_session' AND (whole_graph OR EXISTS(SELECT 1 FROM public.coding_sessions cs WHERE cs.id=ANY(sessions) AND o.entity_id IN(cs.id::text,cs.session_id)));
 UPDATE public.source_items t SET access_state='restricted' FROM public.selected_evidence_publications p
 WHERE p.source_graph_id=g AND p.source_item_id=s_id AND t.graph_id=p.target_graph_id AND t.source_item_id=p.target_source_item_id
 AND EXISTS(SELECT 1 FROM public.source_versions v WHERE v.graph_id=t.graph_id AND v.source_item_id=t.source_item_id AND v.source_metadata->>'publication_receipt_id'=p.publication_id::text);
 DELETE FROM public.source_items t USING public.selected_evidence_publications p
 WHERE p.source_graph_id=g AND p.source_item_id=s_id AND t.graph_id=p.target_graph_id AND t.source_item_id=p.target_source_item_id
 AND EXISTS(SELECT 1 FROM public.source_versions v WHERE v.graph_id=t.graph_id AND v.source_item_id=t.source_item_id AND v.source_metadata->>'publication_receipt_id'=p.publication_id::text);
 UPDATE public.selected_evidence_publications SET status='retracted',retracted_at=now() WHERE status='active' AND ((source_graph_id=g AND source_item_id=s_id) OR (target_graph_id=g AND target_source_item_id=s_id));
 RETURN ev;
END $$;
CREATE OR REPLACE FUNCTION library_drain_purge(batch INTEGER DEFAULT 100) RETURNS INTEGER
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public AS $$
DECLARE initial_scope TEXT:=current_setting('app.account_id',true);d public.source_purge_dependencies%ROWTYPE;ev public.source_purge_events%ROWTYPE;key TEXT;parts TEXT[];owner_graph UUID;owner_account UUID;scope_account UUID;job UUID;done INTEGER:=0;per_atom BOOLEAN;
BEGIN
 IF NULLIF(initial_scope,'') IS NULL THEN RAISE EXCEPTION 'purge requires explicit retained account scope';END IF;
 IF batch<1 OR batch>200 THEN RAISE EXCEPTION 'invalid purge batch'; END IF;
 FOR d IN SELECT * FROM public.source_purge_dependencies WHERE processed_at IS NULL AND error_category IS NULL AND event_id IN(SELECT event_id FROM public.source_purge_events WHERE account_id=initial_scope::uuid) ORDER BY CASE kind WHEN 'atom' THEN 0 WHEN 'node' THEN 1 ELSE 2 END,dependency_id LIMIT batch FOR UPDATE SKIP LOCKED LOOP
  SELECT * INTO ev FROM public.source_purge_events WHERE event_id=d.event_id;
  per_atom:=ev.event_kind='retracted' AND ev.source_item_id='00000000-0000-0000-0000-000000000000';
  SELECT account_id INTO scope_account FROM public.graphs WHERE id=d.graph_id;
  PERFORM set_config('app.account_id',COALESCE(scope_account,ev.account_id)::text,true);
  IF d.kind='atom' THEN
   INSERT INTO public.source_purge_dependencies(event_id,kind,graph_id,object_id)
   SELECT d.event_id,'atom',a.graph_id,a.atom_id FROM public.atoms a JOIN public.graphs tg ON tg.id=a.graph_id AND tg.account_id=ev.account_id AND tg.kind IN('team','org') WHERE a.promoted_from_atom_id=d.object_id
    OR EXISTS(SELECT 1 FROM public.source_connection_atoms c JOIN public.source_connections sc ON sc.id=c.connection_id WHERE c.atom_id=d.object_id AND c.target_atom_id=a.atom_id AND sc.source_graph_id=d.graph_id AND sc.target_graph_id=a.graph_id AND sc.account_id=ev.account_id) ON CONFLICT DO NOTHING;
   INSERT INTO public.source_purge_dependencies(event_id,kind,graph_id,object_id)
   SELECT d.event_id,'node',n.graph_id,n.node_id FROM public.node_atoms l JOIN public.nodes n ON n.node_id=l.node_id WHERE l.atom_id=d.object_id AND (NOT per_atom OR public.library_node_backed_only_by_event(d.event_id,n.graph_id,n.node_id)) ON CONFLICT DO NOTHING;
   IF per_atom THEN
    WITH stripped AS (UPDATE public.nodes n SET body=n.body-'digest',digest_coral_count=0,version=n.version+1,updated_at=now() FROM public.node_atoms l WHERE l.node_id=n.node_id AND l.atom_id=d.object_id AND n.body ? 'digest' RETURNING n.*),
    dropped AS (DELETE FROM public.op_log o USING stripped s WHERE o.graph_id=s.graph_id AND o.entity='node' AND o.entity_id=s.node_id::text)
    INSERT INTO public.op_log(op_id,entity,entity_id,op_type,payload,version,updated_at,device_id,graph_id)
    SELECT gen_random_uuid(),'node',s.node_id::text,'upsert',jsonb_build_object('node_id',s.node_id,'graph_id',s.graph_id,'type',s.type,'title',s.title,'body',s.body::text,'scope',s.scope,'owner_id',s.owner_id,'created_at',to_char(s.created_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"+00:00"'),'updated_at',to_char(s.updated_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"+00:00"'),'version',s.version),s.version,s.updated_at,'evidence-lifecycle',s.graph_id FROM stripped s;
   END IF;
   SELECT content_blob_key INTO key FROM public.atoms WHERE atom_id=d.object_id AND graph_id=d.graph_id;
   IF key IS NOT NULL AND NOT EXISTS(SELECT 1 FROM public.atoms WHERE atom_id=d.object_id AND derived_copy_bound AND split_part(key,'/',GREATEST(1,array_length(string_to_array(key,'/'),1)-1))<>d.graph_id::text) THEN INSERT INTO public.source_purge_dependencies(event_id,kind,graph_id,object_id,blob_key) VALUES(d.event_id,'blob',d.graph_id,d.object_id,key) ON CONFLICT DO NOTHING; END IF;
   UPDATE public.answer_support_receipts SET evidence_complete=false WHERE support @> jsonb_build_array(jsonb_build_object('kind','legacy_chunk','graph_id',d.graph_id,'atom_id',d.object_id));
   UPDATE public.conversation_messages m SET text='[Evidence no longer available]',citations='[]',sources='[]',timing='{}',scope_graph_ids='[]' WHERE m.role='assistant' AND m.id IN(SELECT receipt_id FROM public.answer_support_receipts r WHERE r.support @> jsonb_build_array(jsonb_build_object('kind','legacy_chunk','graph_id',d.graph_id,'atom_id',d.object_id)));
   UPDATE public.mcp_tasks SET status='failed',result=NULL,error='{"code":"evidence_retracted","message":"Evidence no longer available"}',result_sha256=NULL WHERE answer_receipt_id IN(SELECT receipt_id FROM public.answer_support_receipts r WHERE r.support @> jsonb_build_array(jsonb_build_object('kind','legacy_chunk','graph_id',d.graph_id,'atom_id',d.object_id)));
   IF NOT per_atom THEN PERFORM public.invalidate_derived_read_surfaces(d.graph_id);END IF;
   DELETE FROM public.atoms WHERE atom_id=d.object_id AND graph_id=d.graph_id;
  ELSIF d.kind='node' THEN
   INSERT INTO public.source_purge_dependencies(event_id,kind,graph_id,object_id)
   SELECT d.event_id,'node',n.graph_id,n.node_id FROM public.nodes n JOIN public.graphs tg ON tg.id=n.graph_id AND tg.account_id=ev.account_id AND tg.kind IN('team','org') WHERE n.promoted_from_node_id=d.object_id AND n.promoted_from_graph_id=d.graph_id ON CONFLICT DO NOTHING;
   INSERT INTO public.source_purge_reviews SELECT ev.account_id,d.graph_id,n.node_id,'node',n.body->>'human_review_status' FROM public.nodes n WHERE n.graph_id=d.graph_id AND n.node_id=d.object_id AND n.body->>'human_review_status' IN('confirmed','rejected','pending','held','reviewed','selected') ON CONFLICT DO NOTHING;
   INSERT INTO public.source_purge_reviews SELECT ev.account_id,d.graph_id,e.edge_id,'edge',COALESCE(e.provenance->>'human_review_status',e.status) FROM public.relationship_edge e WHERE e.graph_id=d.graph_id AND (e.src_node_id=d.object_id OR e.dst_node_id=d.object_id) AND COALESCE(e.provenance->>'human_review_status',e.status) IN('confirmed','rejected','pending','held','reviewed','selected') ON CONFLICT DO NOTHING;
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
CREATE OR REPLACE FUNCTION library_atom_purge_signal() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public AS $$
DECLARE ev UUID;acc UUID;owner UUID;
BEGIN
 IF pg_trigger_depth()>1 OR EXISTS(SELECT 1 FROM public.source_purge_dependencies WHERE kind='atom' AND graph_id=OLD.graph_id AND object_id=OLD.atom_id AND processed_at IS NULL) THEN RETURN OLD;END IF;
 SELECT account_id,owner_user_id INTO acc,owner FROM public.graphs WHERE id=OLD.graph_id;
 IF NOT FOUND THEN RETURN OLD;END IF;
 INSERT INTO public.source_purge_events(account_id,owner_user_id,graph_id,source_item_id,event_kind) VALUES(acc,owner,OLD.graph_id,'00000000-0000-0000-0000-000000000000','retracted')
 ON CONFLICT(graph_id,source_item_id,event_kind) WHERE sql_processed_at IS NULL DO UPDATE SET account_id=EXCLUDED.account_id RETURNING event_id INTO ev;
 INSERT INTO public.source_purge_dependencies(event_id,kind,graph_id,object_id) VALUES(ev,'atom',OLD.graph_id,OLD.atom_id) ON CONFLICT DO NOTHING;
 IF OLD.content_blob_key IS NOT NULL AND (NOT OLD.derived_copy_bound OR split_part(OLD.content_blob_key,'/',GREATEST(1,array_length(string_to_array(OLD.content_blob_key,'/'),1)-1))=OLD.graph_id::text) THEN
  INSERT INTO public.source_purge_dependencies(event_id,kind,graph_id,object_id,blob_key) VALUES(ev,'blob',OLD.graph_id,OLD.atom_id,OLD.content_blob_key) ON CONFLICT DO NOTHING;
 END IF;
 PERFORM public.library_capture_purge(OLD.graph_id,'00000000-0000-0000-0000-000000000000','retracted',OLD.atom_id);
 RETURN OLD;
END $$;
-- Journal zones are validated by chrono-tz, not Postgres. A zone Postgres
-- cannot read must not abort an atom delete (or the drain batch running it).
CREATE OR REPLACE FUNCTION journal_tz_ok(tz TEXT) RETURNS BOOLEAN LANGUAGE plpgsql STABLE SET search_path=pg_catalog AS $$
BEGIN
 PERFORM now() AT TIME ZONE tz;
 RETURN true;
EXCEPTION WHEN invalid_parameter_value THEN RETURN false;
END $$;
-- The app role deletes atoms. Tenant default privileges revoke PUBLIC
-- execution; the control plane has no such default.
REVOKE ALL ON FUNCTION journal_tz_ok(TEXT) FROM PUBLIC;
DO $$ DECLARE r TEXT;BEGIN FOREACH r IN ARRAY ARRAY['coralswarm_app','coralswarm_tenant_app'] LOOP IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname=r) THEN EXECUTE format('GRANT EXECUTE ON FUNCTION journal_tz_ok(TEXT) TO %I',r);END IF;END LOOP;END $$;
CREATE OR REPLACE FUNCTION atom_lifecycle_deleted() RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE h TIMESTAMPTZ:=date_trunc('hour',OLD."timestamp" AT TIME ZONE 'UTC') AT TIME ZONE 'UTC';bad_tz TEXT[];unsupported UUID[];
BEGIN
 WITH t AS (UPDATE relationship_edge e SET
  provenance=jsonb_set(e.provenance,'{atom_ids}',COALESCE((SELECT jsonb_agg(v) FROM jsonb_array_elements(e.provenance->'atom_ids') v WHERE v<>to_jsonb(OLD.atom_id::text)),'[]')),
  version=version+1,updated_at=now()
 WHERE e.graph_id=OLD.graph_id AND jsonb_typeof(e.provenance->'atom_ids')='array' AND e.provenance->'atom_ids' @> to_jsonb(ARRAY[OLD.atom_id::text])
 RETURNING e.edge_id,e.provenance)
 SELECT array_agg(t.edge_id) INTO unsupported FROM t WHERE t.provenance->'atom_ids'='[]'::jsonb
  AND NOT COALESCE(jsonb_typeof(t.provenance->'source_edge_ids')='array' AND jsonb_array_length(t.provenance->'source_edge_ids')>0,false)
  AND NOT COALESCE(jsonb_typeof(t.provenance->'semantic_atom_ids')='array' AND jsonb_array_length(t.provenance->'semantic_atom_ids')>0,false);
 WITH stripped AS (UPDATE relationship_edge e SET provenance=e.provenance-'rationale',version=version+1,updated_at=now()
  WHERE e.edge_id=ANY(unsupported) AND e.status='rejected' AND e.provenance ? 'rationale' RETURNING e.*),
 dropped AS (DELETE FROM op_log o USING stripped s WHERE o.graph_id=s.graph_id AND o.entity='relationship_edge' AND o.entity_id=s.edge_id::text)
 INSERT INTO op_log(op_id,entity,entity_id,op_type,payload,version,updated_at,device_id,graph_id)
 SELECT gen_random_uuid(),'relationship_edge',s.edge_id::text,'upsert',jsonb_build_object('edge_id',s.edge_id,'graph_id',s.graph_id,'src_node_id',s.src_node_id,'predicate',s.predicate,'dst_node_id',s.dst_node_id,'origin',s.origin,'status',s.status,'confidence',s.confidence,'provenance',s.provenance,'created_at',to_char(s.created_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"+00:00"'),'updated_at',to_char(s.updated_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"+00:00"'),'version',s.version),s.version,s.updated_at,'evidence-lifecycle',s.graph_id FROM stripped s;
 UPDATE relationship_edge e SET status='rejected',
  provenance=(e.provenance-'rationale') || jsonb_build_object('lifecycle','unsupported','human_review_status',COALESCE(e.provenance->>'human_review_status',e.status)),
  version=version+1,updated_at=now()
 WHERE e.edge_id=ANY(unsupported) AND e.status<>'rejected';
 PERFORM reconcile_relationship_lifecycle(OLD.graph_id);
 SELECT array_agg(z.tz) INTO bad_tz FROM (SELECT DISTINCT j.tz FROM journal_rollups j WHERE j.graph_id=OLD.graph_id AND j.tz IS NOT NULL) z WHERE NOT journal_tz_ok(z.tz);
 DELETE FROM journal_rollups j WHERE j.graph_id=OLD.graph_id AND j.user_id=OLD.ingested_by AND CASE
  WHEN j.granularity='hour' THEN j.period_start=h
  WHEN j.tz=ANY(COALESCE(bad_tz,'{}')) THEN true
  WHEN j.granularity IN('day','week') THEN date_trunc(j.granularity,h AT TIME ZONE j.tz)=date_trunc(j.granularity,j.period_start AT TIME ZONE j.tz)
  WHEN j.granularity='month' THEN date_trunc('month',date_trunc('week',h AT TIME ZONE j.tz))=date_trunc('month',j.period_start AT TIME ZONE j.tz)
  ELSE false END;
 DELETE FROM op_log WHERE graph_id=OLD.graph_id AND entity_id=OLD.atom_id::text;
 RETURN NULL;
END $$;
