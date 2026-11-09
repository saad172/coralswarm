ALTER TABLE corpus_enrichment_runs
 ADD COLUMN run_kind TEXT NOT NULL DEFAULT 'cohort' CHECK(run_kind IN ('cohort','standing')),
 ADD COLUMN execution_principal UUID,
 ADD COLUMN retired_at TIMESTAMPTZ,
 ADD COLUMN standing_pause_state TEXT,
 ADD CONSTRAINT corpus_standing_shape CHECK(run_kind<>'standing' OR
  (mode='execute' AND corpus_kind='real' AND execution_principal IS NOT NULL
   AND stage=0 AND cursor_time IS NULL AND cursor_id IS NULL
   AND admitted_items=0 AND attempted_characters=0 AND attempted_model_calls=0)),
 ADD CONSTRAINT corpus_standing_pause_shape CHECK(
  (run_kind='cohort' AND standing_pause_state IS NULL) OR
  (run_kind='standing' AND standing_pause_state IS NOT NULL AND
   ((standing_pause_state='none' AND status='running' AND pause_reason IS NULL) OR
    (standing_pause_state IN ('operator','both') AND status='paused' AND pause_reason IS NOT DISTINCT FROM 'operator_paused') OR
    (standing_pause_state='processing' AND status='paused' AND pause_reason IS NOT DISTINCT FROM 'managed_processing_disabled'))));
CREATE UNIQUE INDEX corpus_standing_run_live
 ON corpus_enrichment_runs(graph_id,extraction_version,corpus_kind)
 WHERE run_kind='standing' AND retired_at IS NULL;
ALTER TABLE corpus_enrichment_units ADD COLUMN retry_after TIMESTAMPTZ;
ALTER TABLE corpus_capture_admissions
 ADD COLUMN processed_at TIMESTAMPTZ,
 ADD COLUMN ever_held BOOLEAN NOT NULL DEFAULT true;
ALTER TABLE corpus_capture_admissions ALTER COLUMN ever_held SET DEFAULT false;

CREATE OR REPLACE FUNCTION corpus_managed_identity_immutable() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
 IF TG_TABLE_NAME='corpus_enrichment_runs' THEN
  IF (NEW.graph_id,NEW.account_id,NEW.requested_by,NEW.mode,NEW.corpus_kind,NEW.extraction_version,NEW.model,NEW.cohort_cutoff,NEW.cohort_snapshot,NEW.run_kind)
   IS DISTINCT FROM (OLD.graph_id,OLD.account_id,OLD.requested_by,OLD.mode,OLD.corpus_kind,OLD.extraction_version,OLD.model,OLD.cohort_cutoff,OLD.cohort_snapshot,OLD.run_kind)
   OR (OLD.retired_at IS NOT NULL AND NEW.retired_at IS DISTINCT FROM OLD.retired_at) THEN
   RAISE EXCEPTION 'managed run identity/model/cohort/retirement is immutable';
  END IF;
 ELSE
  IF (NEW.run_id,NEW.graph_id,NEW.kind,NEW.holding_id) IS DISTINCT FROM (OLD.run_id,OLD.graph_id,OLD.kind,OLD.holding_id)
   OR (OLD.promoted_version_id IS NOT NULL AND (NEW.promoted_version_id,NEW.promotion_fingerprint,NEW.source_item_id,NEW.extraction_version) IS DISTINCT FROM (OLD.promoted_version_id,OLD.promotion_fingerprint,OLD.source_item_id,OLD.extraction_version))
   OR (OLD.execution_owner AND (NOT NEW.execution_owner OR
    (NEW.source_item_id,NEW.extraction_version) IS DISTINCT FROM (OLD.source_item_id,OLD.extraction_version))) THEN
   RAISE EXCEPTION 'managed holding/promotion/owner is immutable';
  END IF;
 END IF;
 RETURN NEW;
END $$;

CREATE FUNCTION corpus_standing_budget_immutable() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
 IF OLD.run_kind='standing' THEN
  RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='standing runs do not accept logical budgets';
 END IF;
 RETURN NEW;
END $$;
REVOKE ALL ON FUNCTION corpus_standing_budget_immutable() FROM PUBLIC;
CREATE TRIGGER corpus_standing_budget_immutable BEFORE UPDATE OF max_items,max_characters,max_model_calls ON corpus_enrichment_runs
 FOR EACH ROW EXECUTE FUNCTION corpus_standing_budget_immutable();

CREATE FUNCTION corpus_standing_unit_shape() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE parent_kind TEXT; parent_graph UUID; parent_profile TEXT;
BEGIN
 SELECT r.run_kind,r.graph_id,r.extraction_version INTO parent_kind,parent_graph,parent_profile
  FROM public.corpus_enrichment_runs r WHERE r.run_id=NEW.run_id;
 IF NOT FOUND THEN
  RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='managed holding parent is unavailable';
 END IF;
 IF parent_kind='standing' AND
  (NEW.graph_id IS DISTINCT FROM parent_graph OR NEW.kind<>'retained_version'
   OR NEW.extraction_version IS DISTINCT FROM parent_profile
   OR NEW.promoted_version_id IS NOT NULL OR NEW.promotion_fingerprint IS NOT NULL) THEN
  RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='standing runs require retained version holdings';
 END IF;
 RETURN NEW;
END $$;
REVOKE ALL ON FUNCTION corpus_standing_unit_shape() FROM PUBLIC;
CREATE TRIGGER corpus_standing_unit_shape BEFORE INSERT OR UPDATE ON corpus_enrichment_units
 FOR EACH ROW EXECUTE FUNCTION corpus_standing_unit_shape();

CREATE FUNCTION corpus_admission_observation_immutable() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
 NEW.ever_held=OLD.ever_held OR NEW.ever_held;
 IF OLD.processed_at IS NOT NULL THEN NEW.processed_at=OLD.processed_at; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER corpus_admission_observation_immutable BEFORE UPDATE ON corpus_capture_admissions
 FOR EACH ROW EXECUTE FUNCTION corpus_admission_observation_immutable();

CREATE FUNCTION corpus_queue_uuid(value jsonb) RETURNS uuid
LANGUAGE plpgsql IMMUTABLE STRICT SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE text_value TEXT;
BEGIN
 IF jsonb_typeof(value)<>'string' THEN RETURN NULL; END IF;
 text_value=value#>>'{}';
 IF text_value COLLATE "C" ~ '^[0-9a-fA-F]{32}$'
  OR text_value COLLATE "C" ~ '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$' THEN
  RETURN text_value::uuid;
 ELSIF text_value COLLATE "C" ~ '^[{][0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}[}]$' THEN
  RETURN substring(text_value FROM 2 FOR 36)::uuid;
 ELSIF text_value COLLATE "C" ~ '^urn:uuid:[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$' THEN
  RETURN substring(text_value FROM 10)::uuid;
 END IF;
 RETURN NULL;
END $$;
REVOKE ALL ON FUNCTION corpus_queue_uuid(jsonb) FROM PUBLIC;

CREATE FUNCTION corpus_standing_queue_route() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE graph UUID; version UUID; profile TEXT; owner_account UUID; graph_account UUID; owner_profile TEXT;
BEGIN
 IF TG_OP='UPDATE' THEN
  IF OLD.kind IN ('ExtractSourceAtoms','ExtractMeetingAtoms','ExtractStandingSourceAtoms','ExtractStandingMeetingAtoms') THEN
   IF OLD.status::text='dead' AND OLD.payload->'evidence_retracted'='true'::jsonb THEN
    IF NEW.status::text IS DISTINCT FROM 'dead' OR
     (NEW.payload,NEW.kind,NEW.account_id) IS DISTINCT FROM (OLD.payload,OLD.kind,OLD.account_id) THEN
     RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='retracted extraction job is terminal';
    END IF;
    RETURN NEW;
   END IF;
   graph=public.corpus_queue_uuid(OLD.payload->'graph_id');
   IF graph IS NOT NULL AND NEW.status::text='dead'
    AND NEW.kind IS NOT DISTINCT FROM OLD.kind AND NEW.account_id IS NOT DISTINCT FROM OLD.account_id
    AND NEW.payload=jsonb_build_object('graph_id',graph,'evidence_retracted',true) THEN
    RETURN NEW;
   END IF;
  END IF;
  IF OLD.kind IN ('ExtractSourceAtoms','ExtractMeetingAtoms','ExtractStandingSourceAtoms','ExtractStandingMeetingAtoms')
   AND OLD.status::text IN ('pending','running') THEN
   IF (NEW.payload->'graph_id',NEW.payload->'source_version_id') IS DISTINCT FROM
     (OLD.payload->'graph_id',OLD.payload->'source_version_id')
    OR (OLD.account_id IS NOT NULL AND NEW.account_id IS DISTINCT FROM OLD.account_id)
    OR (NEW.kind IS DISTINCT FROM OLD.kind AND (
     OLD.status::text='pending' AND NEW.status::text='pending' AND
     ((OLD.kind='ExtractSourceAtoms' AND NEW.kind='ExtractStandingSourceAtoms') OR
      (OLD.kind='ExtractMeetingAtoms' AND NEW.kind='ExtractStandingMeetingAtoms'))) IS NOT TRUE) THEN
    RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='active extraction job identity is immutable';
   END IF;
  END IF;
 END IF;
 IF NEW.kind NOT IN ('ExtractSourceAtoms','ExtractMeetingAtoms','ExtractStandingSourceAtoms','ExtractStandingMeetingAtoms')
  OR NEW.status::text NOT IN ('pending','running') THEN RETURN NEW; END IF;
 graph=public.corpus_queue_uuid(NEW.payload->'graph_id');
 version=public.corpus_queue_uuid(NEW.payload->'source_version_id');
 IF graph IS NULL OR version IS NULL THEN RETURN NEW; END IF;
 profile=CASE WHEN NEW.kind IN ('ExtractSourceAtoms','ExtractStandingSourceAtoms') THEN 'source-atoms-v1' ELSE 'meeting-atoms-v1' END;
 IF TG_OP='INSERT' THEN
  PERFORM 1 FROM public.source_versions v WHERE v.graph_id=graph AND v.version_id=version FOR KEY SHARE;
 END IF;
 SELECT r.account_id,g.account_id,v.extraction_version INTO owner_account,graph_account,owner_profile
  FROM public.corpus_enrichment_units u
  JOIN public.corpus_enrichment_runs r ON r.run_id=u.run_id AND r.graph_id=u.graph_id
  JOIN public.source_versions v ON v.graph_id=u.graph_id AND v.version_id=version
  JOIN public.graphs g ON g.id=v.graph_id
  WHERE u.graph_id=graph AND COALESCE(u.promoted_version_id,u.holding_id)=version
   AND u.extraction_version=v.extraction_version
   AND u.execution_owner AND r.run_kind='standing';
 IF owner_account IS NOT NULL THEN
  IF NEW.account_id IS DISTINCT FROM owner_account OR owner_account IS DISTINCT FROM graph_account OR profile<>owner_profile THEN
   RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='standing extraction identity is incompatible';
  END IF;
  IF NEW.kind IN ('ExtractSourceAtoms','ExtractMeetingAtoms') AND NEW.status::text='running' THEN
   RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='standing extraction requires a compatible worker';
  END IF;
  NEW.kind=CASE NEW.kind WHEN 'ExtractSourceAtoms' THEN 'ExtractStandingSourceAtoms'
   WHEN 'ExtractMeetingAtoms' THEN 'ExtractStandingMeetingAtoms' ELSE NEW.kind END;
 END IF;
 RETURN NEW;
END $$;
REVOKE ALL ON FUNCTION corpus_standing_queue_route() FROM PUBLIC;
CREATE TRIGGER corpus_standing_queue_route BEFORE INSERT OR UPDATE OF kind,payload,status,account_id ON jobs
 FOR EACH ROW EXECUTE FUNCTION corpus_standing_queue_route();

CREATE OR REPLACE FUNCTION library_capture_purge(g UUID,s_id UUID,k TEXT,retiring_atom UUID DEFAULT NULL) RETURNS UUID
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public AS $$
DECLARE ev UUID;acc UUID;owner UUID;receipts UUID[];src TEXT;sessions UUID[];own_atoms UUID[];
 per_atom BOOLEAN:=s_id='00000000-0000-0000-0000-000000000000' AND k='retracted';
 whole_graph BOOLEAN:=s_id='00000000-0000-0000-0000-000000000000' AND k<>'retracted';
 purged_kinds TEXT[]:=ARRAY['ExtractMeetingAtoms','ExtractSourceAtoms','ExtractStandingMeetingAtoms','ExtractStandingSourceAtoms','ExtractEntities','ExtractRelationships','IngestDoor','GenerateMeetingSummary','ExtractEntityBatch','RegenerateDigest','EntityResolution','ConflictDetection','InferExpertise','ClassifyKind','ExtractMentions','GenerateTitle','GenerateJournal','PromoteNode','PublishSource'];
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
 UPDATE public.jobs SET status='dead',payload=jsonb_build_object('graph_id',g,'evidence_retracted',true),error_message=NULL,finished_at=now(),idempotency_key=NULL WHERE (payload->>'graph_id'=g::text OR (NOT per_atom AND kind IN ('ExtractSourceAtoms','ExtractMeetingAtoms','ExtractStandingSourceAtoms','ExtractStandingMeetingAtoms') AND public.corpus_queue_uuid(payload->'graph_id')=g)) AND (CASE WHEN per_atom THEN status IN('pending','failed') ELSE true END) AND (whole_graph OR payload->>'source_item_id'=s_id::text OR payload->>'source_id'=src OR payload->>'source_version_id' IN(SELECT v.version_id::text FROM public.source_versions v WHERE v.graph_id=g AND v.source_item_id=s_id) OR (NOT per_atom AND kind IN ('ExtractSourceAtoms','ExtractMeetingAtoms','ExtractStandingSourceAtoms','ExtractStandingMeetingAtoms') AND public.corpus_queue_uuid(payload->'source_version_id') IN(SELECT v.version_id FROM public.source_versions v WHERE v.graph_id=g AND v.source_item_id=s_id)) OR (per_atom AND (payload->>'atom_id'=ANY(own_atoms::text[]) OR (jsonb_typeof(payload->'atom_ids')='array' AND payload->'atom_ids' ?| own_atoms::text[]))) OR (NOT per_atom AND EXISTS(SELECT 1 FROM public.source_purge_dependencies d WHERE d.event_id=ev AND d.kind='atom' AND (payload->>'atom_id'=d.object_id::text OR (jsonb_typeof(payload->'atom_ids')='array' AND payload->'atom_ids' ? d.object_id::text))))) AND kind=ANY(purged_kinds);
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

ALTER TABLE public.corpus_capture_admissions ADD COLUMN hold_basis text NOT NULL DEFAULT 'legacy_unknown' CHECK(hold_basis IN ('legacy_unknown','never_held','unverified_transition','backlog_policy_excluded','operator_paused','managed_processing_disabled','processing_off','cloud_processing_disabled','source_removed_or_access_lost','requester_access_lost','daily_budget_exhausted','execution_budget_exhausted','lifecycle_contract_not_ready','credits_exhausted'));
ALTER TABLE public.corpus_capture_admissions ALTER COLUMN hold_basis SET DEFAULT 'unverified_transition';
REVOKE INSERT,UPDATE,DELETE ON public.corpus_capture_admissions FROM PUBLIC,coralswarm_app,coralswarm_tenant_app;
DO $$ DECLARE app_role text:=CASE WHEN to_regprocedure('public.tenant_rls_is_system()') IS NULL THEN 'coralswarm_app' ELSE 'coralswarm_tenant_app' END; BEGIN EXECUTE 'GRANT INSERT(source_version_id,graph_id,account_id,source_item_id,captured_by,extraction_version,run_id,unit_id,state,reason,observed_at,updated_at), UPDATE(run_id,unit_id,state,reason,updated_at) ON public.corpus_capture_admissions TO '||quote_ident(app_role); END $$;
CREATE OR REPLACE FUNCTION public.corpus_admission_observation_immutable() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE fresh_edge boolean; valid_source boolean;
BEGIN
 IF TG_OP='INSERT' THEN
  NEW.observed_at=clock_timestamp();NEW.processed_at=NULL;NEW.ever_held=false;NEW.hold_basis='unverified_transition';
  IF NOT EXISTS(SELECT 1 FROM public.source_versions v JOIN public.source_items s ON s.source_item_id=v.source_item_id AND s.graph_id=v.graph_id JOIN public.graphs g ON g.id=v.graph_id WHERE v.version_id=NEW.source_version_id AND v.graph_id=NEW.graph_id AND v.source_item_id=NEW.source_item_id AND g.account_id=NEW.account_id) THEN
   RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='receipt source identity unavailable';
  END IF;
  fresh_edge=NEW.state='processed';
 ELSE
  IF (NEW.source_version_id,NEW.graph_id,NEW.account_id,NEW.source_item_id,NEW.captured_by,NEW.extraction_version) IS DISTINCT FROM (OLD.source_version_id,OLD.graph_id,OLD.account_id,OLD.source_item_id,OLD.captured_by,OLD.extraction_version) THEN
   RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='receipt identity immutable';
  END IF;
  NEW.observed_at=OLD.observed_at;NEW.processed_at=OLD.processed_at;
  fresh_edge=NEW.state='processed' AND OLD.state<>'processed';
  NEW.hold_basis=OLD.hold_basis;
  NEW.ever_held=OLD.ever_held;
 END IF;
 IF fresh_edge THEN
  SELECT EXISTS(SELECT 1 FROM public.source_versions v JOIN public.source_items s ON s.source_item_id=v.source_item_id AND s.graph_id=v.graph_id JOIN public.graphs g ON g.id=v.graph_id WHERE v.version_id=NEW.source_version_id AND v.graph_id=NEW.graph_id AND v.source_item_id=NEW.source_item_id AND g.account_id=NEW.account_id AND v.extraction_version=NEW.extraction_version AND v.extraction_status='done' AND s.access_state='permitted' AND (NEW.extraction_version<>'selected-passages-v1' OR EXISTS(SELECT 1 FROM public.selected_evidence_publications p WHERE p.target_graph_id=NEW.graph_id AND p.target_source_item_id=NEW.source_item_id AND p.target_source_version_id=NEW.source_version_id AND p.account_id=NEW.account_id AND p.owner_user_id=NEW.captured_by AND p.status='active'))) INTO valid_source;
  IF NOT valid_source THEN RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='processed receipt requires matching done source';END IF;
  IF NEW.processed_at IS NULL THEN NEW.processed_at=clock_timestamp();END IF;
 END IF;
 RETURN NEW;
END $$;
DROP TRIGGER corpus_admission_observation_immutable ON public.corpus_capture_admissions;
CREATE TRIGGER corpus_admission_observation_immutable BEFORE INSERT OR UPDATE ON public.corpus_capture_admissions FOR EACH ROW EXECUTE FUNCTION public.corpus_admission_observation_immutable();
DO $$ DECLARE role_name text; protected text; BEGIN
 FOREACH role_name IN ARRAY ARRAY['coralswarm_app','coralswarm_tenant_app'] LOOP
  IF has_table_privilege(role_name,'public.corpus_capture_admissions','DELETE') OR pg_has_role(role_name,(SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid='public.corpus_capture_admissions'::regclass),'MEMBER') THEN RAISE EXCEPTION 'unexpected effective receipt DELETE/owner authority: %',role_name;END IF;
  FOREACH protected IN ARRAY ARRAY['processed_at','ever_held','hold_basis'] LOOP
   IF has_column_privilege(role_name,'public.corpus_capture_admissions',protected,'INSERT') OR has_column_privilege(role_name,'public.corpus_capture_admissions',protected,'UPDATE') THEN RAISE EXCEPTION 'unexpected protected receipt grant: % %',role_name,protected;END IF;
  END LOOP;
 END LOOP;
END $$;
