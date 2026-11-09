-- PAIRED WITH: tenant 0227 (page_model_operational_fences).
-- Correct only demonstrated operational over-invalidation.
-- CREATE OR REPLACE retains the existing trigger owners and ACLs; preserve
-- SECURITY DEFINER and the pinned search_path. Existing trigger wiring stays.
-- No upgrade fence scan: old over-invalidated rows remain withheld until an
-- ordinary certified recompute. Never release a previously unsafe snapshot.

CREATE OR REPLACE FUNCTION read_models_mark_dirty() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE gs uuid[]; reducing boolean; changed_rows jsonb; accounts_changed uuid[]; clear_prose boolean;
BEGIN
 -- Transition tables collapse bulk writes to one update. JSON supports both
 -- direct graph rows and link/membership rows without schema-specific triggers.
 IF TG_TABLE_NAME IN ('atoms','nodes','node_semantic_atoms','source_items','source_versions','source_version_chunks','coding_sessions','journal_rollups','journal_gists') THEN
  IF TG_OP='INSERT' THEN EXECUTE 'SELECT array_agg(DISTINCT graph_id) FROM new_rows' INTO gs;
  ELSIF TG_OP='DELETE' THEN EXECUTE 'SELECT array_agg(DISTINCT graph_id) FROM old_rows' INTO gs;
  ELSE EXECUTE 'SELECT array_agg(DISTINCT graph_id) FROM (SELECT graph_id FROM new_rows UNION SELECT graph_id FROM old_rows) s' INTO gs; END IF;
 ELSE
 IF TG_OP='INSERT' THEN
  EXECUTE 'SELECT jsonb_agg(to_jsonb(n)) FROM new_rows n' INTO changed_rows;
 ELSIF TG_OP='DELETE' THEN
  EXECUTE 'SELECT jsonb_agg(to_jsonb(o)) FROM old_rows o' INTO changed_rows;
 ELSE
  EXECUTE 'SELECT jsonb_agg(j) FROM (SELECT to_jsonb(n) j FROM new_rows n UNION ALL SELECT to_jsonb(o) FROM old_rows o) s' INTO changed_rows;
 END IF;
 -- Only managed-enrichment receipts can certify reef_digest_read output.
 -- Embedding/extraction dispatch and settlement are bookkeeping for other
 -- domains; they must not reduce every content model on the graph.
 -- Filtering BOTH transition sets retains old/new graph invalidation for
 -- every receipt which could have certified a digest before or after a write.
 IF TG_TABLE_NAME='managed_attempt_receipts' THEN
  SELECT jsonb_agg(j) INTO changed_rows
   FROM jsonb_array_elements(COALESCE(changed_rows,'[]'::jsonb)) AS evidence(j)
   WHERE j->>'purpose'='managed_enrichment';
  IF changed_rows IS NULL THEN RETURN NULL; END IF;
 END IF;
 WITH changed AS (SELECT value j FROM jsonb_array_elements(COALESCE(changed_rows,'[]'::jsonb)))
 SELECT array_agg(DISTINCT x.g) INTO gs FROM (
  SELECT NULLIF(j->>'graph_id','')::uuid g FROM changed
  UNION SELECT n.graph_id FROM changed c JOIN public.nodes n ON n.node_id=NULLIF(c.j->>'node_id','')::uuid
  UNION SELECT t.graph_id FROM changed c JOIN public.teams t ON t.id=CASE WHEN TG_TABLE_NAME IN ('team_members','team_seat_grants') THEN NULLIF(c.j->>'team_id','')::uuid END
  UNION SELECT o.graph_id FROM changed c JOIN public.conversation_oceans o ON o.conversation_id=COALESCE(NULLIF(c.j->>'conversation_id','')::uuid,NULLIF(c.j->>'id','')::uuid) WHERE TG_TABLE_NAME='conversations'
  UNION SELECT c.graph_id FROM changed x JOIN public.conversations c ON c.id=CASE WHEN TG_TABLE_NAME='conversation_messages' THEN NULLIF(x.j->>'conversation_id','')::uuid END
  UNION SELECT g.id FROM changed c JOIN public.graphs g ON g.id=NULLIF(c.j->>'id','')::uuid WHERE TG_TABLE_NAME='graphs'
  UNION SELECT c.graph_id FROM changed d JOIN public.conversation_messages m ON m.id=CASE WHEN TG_TABLE_NAME='message_feedback' THEN NULLIF(d.j->>'message_id','')::uuid END JOIN public.conversations c ON c.id=m.conversation_id
 ) x WHERE x.g IS NOT NULL;
 WITH changed AS (SELECT value j FROM jsonb_array_elements(COALESCE(changed_rows,'[]'::jsonb)))
 SELECT array_agg(DISTINCT account_id) INTO accounts_changed FROM (
  SELECT NULLIF(j->>'account_id','')::uuid account_id FROM changed
  UNION SELECT c.account_id FROM changed x JOIN public.conversations c
   ON c.id=CASE WHEN TG_TABLE_NAME IN ('conversation_messages','conversation_oceans') THEN NULLIF(x.j->>'conversation_id','')::uuid END
 ) a WHERE account_id IS NOT NULL;
 END IF;
 reducing := TG_TABLE_NAME IN ('source_version_chunks','team_members','team_seat_grants','graphs')
             OR (TG_OP<>'INSERT' AND TG_TABLE_NAME IN ('node_atoms','node_semantic_atoms','reef_identity_corrections','semantic_atoms','meeting_transcripts','conversations','conversation_messages','conversation_oceans','answer_support_receipts','connector_sources','github_watched_repos','gmail_watched_threads','outlook_watched_conversations','teams_watched_resources','teams_meeting_captures','teams_meeting_installations','google_chat_watched_spaces','google_meet_captures'))
             OR (TG_TABLE_NAME IN ('atoms','nodes','source_versions') AND TG_OP='DELETE');
 IF TG_OP<>'INSERT' AND TG_TABLE_NAME IN ('activity_events','semantic_atom_passages','managed_enrichment_inputs','managed_attempt_receipts','relationship_edge','message_feedback') THEN reducing:=true; END IF;
 IF TG_TABLE_NAME='atoms' AND TG_OP='UPDATE' THEN
  EXECUTE 'SELECT EXISTS(SELECT 1 FROM new_rows n FULL JOIN old_rows o USING(atom_id) WHERE ROW(n.evidence_retracted,n.derived_copy_bound,n.source_evidence_bound,n.graph_id,n.timestamp,n.content_inline,n.participants,n.source_type,n.source_id) IS DISTINCT FROM ROW(o.evidence_retracted,o.derived_copy_bound,o.source_evidence_bound,o.graph_id,o.timestamp,o.content_inline,o.participants,o.source_type,o.source_id))' INTO reducing;
 ELSIF TG_TABLE_NAME='nodes' AND TG_OP='UPDATE' THEN
  EXECUTE 'SELECT EXISTS(SELECT 1 FROM new_rows n FULL JOIN old_rows o USING(node_id) WHERE ROW(n.graph_id,n.title,n.type) IS DISTINCT FROM ROW(o.graph_id,o.title,o.type))' INTO reducing;
 ELSIF TG_TABLE_NAME='source_items' THEN
  IF TG_OP='INSERT' THEN EXECUTE 'SELECT EXISTS(SELECT 1 FROM new_rows WHERE access_state<>''permitted'')' INTO reducing;
  ELSIF TG_OP='UPDATE' THEN EXECUTE 'SELECT EXISTS(SELECT 1 FROM new_rows n JOIN old_rows o USING(source_item_id) WHERE ROW(n.access_state,n.source_id,n.source_type,n.graph_id,n.current_version_id) IS DISTINCT FROM ROW(o.access_state,o.source_id,o.source_type,o.graph_id,o.current_version_id))' INTO reducing;
  ELSE reducing := true; END IF;
 ELSIF TG_TABLE_NAME='source_versions' AND TG_OP='UPDATE' THEN
  -- A matched operational no-op still dirties subscribed status readers, but
  -- cannot remove support or justify retracting certified prose. Compare the
  -- complete version row: every real transition (including sealing/status,
  -- identity, graph, capture time and future columns) stays conservative.
  EXECUTE 'SELECT EXISTS(SELECT 1 FROM new_rows n FULL JOIN old_rows o USING(version_id) WHERE n IS DISTINCT FROM o)' INTO reducing;
 END IF;
 -- Serialize each graph fence BEFORE touching cached rows, even on first
 -- fill. Preserve the last permission fence on additive writes. Sorting gives
 -- multi-graph statements a common lock order. No FK: deletion must retain it.
 INSERT INTO public.read_model_graph_fences(account_id,graph_id,changed_xid,invalidated_xid)
 SELECT a,g,pg_current_xact_id(),CASE WHEN reducing THEN pg_current_xact_id() END FROM (
  SELECT account_id a,id g FROM public.graphs WHERE id=ANY(gs)
  UNION SELECT (j->>'account_id')::uuid,(j->>'id')::uuid
   FROM jsonb_array_elements(COALESCE(changed_rows,'[]'::jsonb)) j WHERE TG_TABLE_NAME='graphs'
 ) affected
 -- Nested source row triggers can issue thousands of node UPDATE statements
 -- in one transaction. Its XID certifies them all: do not rewrite the same
 -- fence, but always upgrade an additive stamp to a reducing stamp.
 WHERE NOT EXISTS(SELECT 1 FROM public.read_model_graph_fences f
  WHERE f.account_id=a AND f.graph_id=g AND f.changed_xid=pg_current_xact_id()
   AND (NOT reducing OR f.invalidated_xid=pg_current_xact_id()))
 ORDER BY a,g
 ON CONFLICT(account_id,graph_id) DO UPDATE SET changed_xid=EXCLUDED.changed_xid,
 invalidated_xid=COALESCE(EXCLUDED.invalidated_xid,read_model_graph_fences.invalidated_xid);
 -- Actor-wide mutation certificates cover owner data whose scope has no
 -- graphs. The reserved '*' subscription exists before registry sync or first
 -- enqueue. Named mark_user events retain their narrower model/actor semantics.
 IF TG_TABLE_NAME IN ('conversations','conversation_messages','conversation_oceans','query_history','activity_events','message_feedback') THEN
  WITH changed AS (SELECT value j FROM jsonb_array_elements(COALESCE(changed_rows,'[]'::jsonb))), actors AS (
   SELECT NULLIF(j->>'account_id','')::uuid a,CASE WHEN TG_TABLE_NAME='activity_events' AND NULLIF(j->>'graph_id','') IS NULL THEN COALESCE(NULLIF(j->>'user_id','')::uuid,'00000000-0000-0000-0000-000000000000'::uuid) ELSE NULLIF(j->>'user_id','')::uuid END u FROM changed
   UNION SELECT c.account_id,c.user_id FROM changed x JOIN public.conversations c
    ON c.id=CASE WHEN TG_TABLE_NAME IN ('conversation_messages','conversation_oceans') THEN NULLIF(x.j->>'conversation_id','')::uuid END
  )
  INSERT INTO public.read_model_user_fences(account_id,model,user_id,changed_xid,invalidated_xid)
  SELECT DISTINCT a,'*',u,pg_current_xact_id(),CASE WHEN reducing THEN pg_current_xact_id() END
  FROM actors WHERE a IS NOT NULL AND u IS NOT NULL AND (u<>'00000000-0000-0000-0000-000000000000'::uuid OR TG_TABLE_NAME='activity_events')
   AND NOT EXISTS(SELECT 1 FROM public.read_model_user_fences f WHERE f.account_id=a AND f.model='*' AND f.user_id=u
    AND f.changed_xid=pg_current_xact_id() AND (NOT reducing OR f.invalidated_xid=pg_current_xact_id()))
  ORDER BY a,u
  ON CONFLICT(account_id,model,user_id) DO UPDATE SET changed_xid=EXCLUDED.changed_xid,
   invalidated_xid=COALESCE(EXCLUDED.invalidated_xid,read_model_user_fences.invalidated_xid);
 END IF;
 -- Retained compiled prose has no exhaustive source lineage. Keep the existing
 -- output-table retraction machinery as a transactional permission backstop.
 clear_prose := reducing;
 IF TG_TABLE_NAME='source_version_chunks' AND TG_OP='INSERT' THEN
  EXECUTE 'SELECT EXISTS(SELECT 1 FROM new_rows c JOIN public.source_versions v ON v.version_id=c.source_version_id AND v.graph_id=c.graph_id JOIN public.source_items s ON s.source_item_id=v.source_item_id AND s.graph_id=v.graph_id WHERE s.access_state<>''permitted'')' INTO clear_prose;
 END IF;
 IF clear_prose AND TG_TABLE_NAME IN ('atoms','nodes','source_items','source_versions','source_version_chunks','semantic_atoms','node_atoms','node_semantic_atoms','reef_identity_corrections') THEN
  DELETE FROM public.journal_rollups WHERE graph_id=ANY(gs);
  DELETE FROM public.journal_gists WHERE graph_id=ANY(gs);
  UPDATE public.meeting_transcripts SET summary=NULL,summary_status='pending',summary_error=NULL,summary_updated_at=NULL
   WHERE graph_id=ANY(gs) AND summary IS NOT NULL;
 END IF;
 UPDATE public.read_models r SET dirty_at=COALESCE(r.dirty_at,statement_timestamp()),
  dirty_bumped_at=statement_timestamp(),dirty_generation=r.dirty_generation+1,
  retry_after=NULL,
  invalidated_xid=CASE WHEN reducing THEN pg_current_xact_id() ELSE r.invalidated_xid END
 WHERE (r.graph_ids && gs OR (r.account_id=ANY(accounts_changed) AND r.model IN (SELECT m.model FROM public.read_model_triggers m WHERE m.table_name=TG_TABLE_NAME))) AND (reducing OR r.model IN (SELECT m.model FROM public.read_model_triggers m WHERE m.table_name=TG_TABLE_NAME))
 AND NOT (r.model='usage_summary' AND TG_TABLE_NAME IN ('llm_usage','query_history','atoms','graphs'));
 RETURN NULL;
END $$;
REVOKE ALL ON FUNCTION read_models_mark_dirty() FROM PUBLIC;

CREATE OR REPLACE FUNCTION read_models_metering_dirty() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE r record; projection text; source_sql text; tenant boolean;
BEGIN
 tenant:=to_regclass('public.tenant_meta') IS NOT NULL;
 projection:=CASE TG_TABLE_NAME WHEN 'atoms' THEN 'NULL::uuid account_id,graph_id'
  WHEN 'graphs' THEN 'account_id,id graph_id' ELSE 'account_id,graph_id' END;
 IF TG_TABLE_NAME='atoms' AND TG_OP='UPDATE' THEN
  -- Both whole-account UsageFacts readers count atoms by graph/account and
  -- created_at. Embedding attempts/failure markers are not metering inputs.
  -- Filter per atom before projecting BOTH graph boundaries: a mixed bulk
  -- UPDATE must not revoke an unchanged graph because another atom moved.
  -- Do not compare only NEW, current-month membership, or visible graphs.
  source_sql:='WITH changed AS (
   SELECT n.graph_id new_graph,o.graph_id old_graph
    FROM new_rows n FULL JOIN old_rows o USING(atom_id)
    WHERE ROW(n.graph_id,n.created_at) IS DISTINCT FROM ROW(o.graph_id,o.created_at)
  ) SELECT NULL::uuid account_id,new_graph graph_id FROM changed WHERE new_graph IS NOT NULL
    UNION SELECT NULL::uuid account_id,old_graph graph_id FROM changed WHERE old_graph IS NOT NULL';
 ELSE
  source_sql:=CASE TG_OP WHEN 'INSERT' THEN format('SELECT %s FROM new_rows',projection)
   WHEN 'DELETE' THEN format('SELECT %s FROM old_rows',projection)
   ELSE format('SELECT %s FROM new_rows UNION SELECT %s FROM old_rows',projection,projection) END;
 END IF;
 FOR r IN EXECUTE format('SELECT DISTINCT a,g FROM (
  SELECT COALESCE(x.account_id,g.account_id,CASE WHEN $1 THEN ''00000000-0000-0000-0000-000000000000''::uuid END) a,
   CASE WHEN COALESCE(x.account_id,g.account_id) IS NULL THEN ''00000000-0000-0000-0000-000000000000''::uuid ELSE COALESCE(x.graph_id,''00000000-0000-0000-0000-000000000000''::uuid) END g
  FROM (%s) x LEFT JOIN public.graphs g ON g.id=x.graph_id) pairs WHERE a IS NOT NULL ORDER BY a,g',source_sql) USING tenant LOOP
  PERFORM public.read_models_stamp_account(r.a,r.g,TG_OP<>'INSERT');
 END LOOP;
 RETURN NULL;
END $$;
REVOKE ALL ON FUNCTION read_models_metering_dirty() FROM PUBLIC;
