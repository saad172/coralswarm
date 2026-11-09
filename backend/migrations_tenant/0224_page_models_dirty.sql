-- PAIRED WITH: control 0412. Additive L3 subscriptions and account/actor certification.
-- Preserve L2 durable graph fences, XID coalescing and timestamp reductions.
ALTER TABLE read_models ADD COLUMN control_revision bigint NOT NULL DEFAULT 0;
-- Additive control facts never change the address or invalidate a safe payload.
ALTER TABLE read_models ADD COLUMN control_freshness bigint NOT NULL DEFAULT 0;
ALTER TABLE read_models ADD COLUMN computed_control_freshness bigint NOT NULL DEFAULT 0;
ALTER TABLE read_model_user_fences ADD COLUMN changed_xid xid8;
ALTER TABLE read_model_user_fences ALTER COLUMN invalidated_xid DROP NOT NULL;

CREATE OR REPLACE FUNCTION read_model_fence_state(a uuid, gs uuid[], m text, u uuid, s pg_snapshot)
RETURNS int LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
BEGIN
 IF s IS NULL THEN RETURN 0; END IF;
 IF current_setting('app.tenant_actor',true) IS DISTINCT FROM 'system' AND
    (a IS DISTINCT FROM NULLIF(current_setting('app.account_id',true),'')::uuid OR
     (u IS NOT NULL AND u IS DISTINCT FROM NULLIF(current_setting('app.user_id',true),'')::uuid))
 THEN RETURN 0; END IF;
 IF EXISTS(SELECT 1 FROM public.read_model_graph_fences f WHERE f.account_id=a AND f.graph_id=ANY(gs)
           AND f.invalidated_xid IS NOT NULL AND NOT pg_visible_in_snapshot(f.invalidated_xid,s))
 OR EXISTS(SELECT 1 FROM public.read_model_user_fences f WHERE f.account_id=a AND f.model IN (m,'*')
           AND f.user_id=COALESCE(u,'00000000-0000-0000-0000-000000000000'::uuid)
           AND NOT pg_visible_in_snapshot(f.invalidated_xid,s))
 THEN RETURN 0; END IF;
 IF EXISTS(SELECT 1 FROM public.read_model_graph_fences f WHERE f.account_id=a AND f.graph_id=ANY(gs)
           AND NOT pg_visible_in_snapshot(f.changed_xid,s))
 OR EXISTS(SELECT 1 FROM public.read_model_user_fences f WHERE f.account_id=a AND f.model IN (m,'*')
           AND f.user_id=COALESCE(u,'00000000-0000-0000-0000-000000000000'::uuid)
           AND f.changed_xid IS NOT NULL AND NOT pg_visible_in_snapshot(f.changed_xid,s))
 THEN RETURN 1; END IF;
 RETURN 2;
END $$;

-- Account-wide mutations use the same durable user fences and serialized XID
-- contract. Rust supplies the authoritative control roster; cached private
-- actors are included so a removed member's old row cannot escape invalidation.
CREATE FUNCTION read_models_mark_account_dirty(a uuid, actors uuid[], models text[]) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
BEGIN
 IF current_setting('app.tenant_actor',true) IS DISTINCT FROM 'system' THEN
  RAISE EXCEPTION 'read-model event requires system scope' USING ERRCODE='42501';
 END IF;
 IF a IS NULL THEN RAISE EXCEPTION 'invalid read-model account' USING ERRCODE='22023'; END IF;
 INSERT INTO public.read_model_user_fences(account_id,model,user_id,invalidated_xid)
 SELECT DISTINCT a,m,u,pg_current_xact_id() FROM unnest(models) m CROSS JOIN (
  SELECT unnest(actors) u UNION SELECT '00000000-0000-0000-0000-000000000000'::uuid
  UNION SELECT viewer_user_id FROM public.read_models WHERE account_id=a AND model=ANY(models) AND viewer_user_id IS NOT NULL
 ) actor WHERE u IS NOT NULL ORDER BY m,u
 ON CONFLICT(account_id,model,user_id) DO UPDATE SET invalidated_xid=EXCLUDED.invalidated_xid;
 UPDATE public.read_models SET dirty_at=COALESCE(dirty_at,statement_timestamp()),dirty_bumped_at=statement_timestamp(),
  dirty_generation=dirty_generation+1,retry_after=NULL,invalidated_xid=pg_current_xact_id()
 WHERE account_id=a AND model=ANY(models);
END $$;
REVOKE ALL ON FUNCTION read_models_mark_account_dirty(uuid,uuid[],text[]) FROM PUBLIC;
DO $$ DECLARE r text; BEGIN FOREACH r IN ARRAY ARRAY['coralswarm_app','coralswarm_tenant_app'] LOOP
 IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname=r) THEN
  EXECUTE format('GRANT EXECUTE ON FUNCTION read_models_mark_account_dirty(uuid,uuid[],text[]) TO %I',r);
 END IF;
END LOOP; END $$;
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
 WITH changed AS (SELECT value j FROM jsonb_array_elements(COALESCE(changed_rows,'[]'::jsonb)))
 SELECT array_agg(DISTINCT x.g) INTO gs FROM (
  SELECT NULLIF(j->>'graph_id','')::uuid g FROM changed
  UNION SELECT n.graph_id FROM changed c JOIN public.nodes n ON n.node_id=NULLIF(c.j->>'node_id','')::uuid
  UNION SELECT t.graph_id FROM changed c JOIN public.teams t ON t.id=CASE WHEN TG_TABLE_NAME IN ('team_members','team_seat_grants') THEN NULLIF(c.j->>'team_id','')::uuid END
  UNION SELECT o.graph_id FROM changed c JOIN public.conversation_oceans o ON o.conversation_id=COALESCE(NULLIF(c.j->>'conversation_id','')::uuid,NULLIF(c.j->>'id','')::uuid) WHERE TG_TABLE_NAME='conversations'
  UNION SELECT c.graph_id FROM changed x JOIN public.conversations c ON c.id=CASE WHEN TG_TABLE_NAME='conversation_messages' THEN NULLIF(x.j->>'conversation_id','')::uuid END
  UNION SELECT g.id FROM changed c JOIN public.graphs g ON g.id=NULLIF(c.j->>'id','')::uuid WHERE TG_TABLE_NAME='graphs'
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
 IF TG_TABLE_NAME='atoms' AND TG_OP='UPDATE' THEN
  EXECUTE 'SELECT EXISTS(SELECT 1 FROM new_rows n FULL JOIN old_rows o USING(atom_id) WHERE ROW(n.evidence_retracted,n.derived_copy_bound,n.source_evidence_bound,n.graph_id,n.timestamp,n.content_inline,n.participants,n.source_type,n.source_id) IS DISTINCT FROM ROW(o.evidence_retracted,o.derived_copy_bound,o.source_evidence_bound,o.graph_id,o.timestamp,o.content_inline,o.participants,o.source_type,o.source_id))' INTO reducing;
 ELSIF TG_TABLE_NAME='nodes' AND TG_OP='UPDATE' THEN
  EXECUTE 'SELECT EXISTS(SELECT 1 FROM new_rows n FULL JOIN old_rows o USING(node_id) WHERE ROW(n.graph_id,n.title,n.type) IS DISTINCT FROM ROW(o.graph_id,o.title,o.type))' INTO reducing;
 ELSIF TG_TABLE_NAME='source_items' THEN
  IF TG_OP='INSERT' THEN EXECUTE 'SELECT EXISTS(SELECT 1 FROM new_rows WHERE access_state<>''permitted'')' INTO reducing;
  ELSIF TG_OP='UPDATE' THEN EXECUTE 'SELECT EXISTS(SELECT 1 FROM new_rows n JOIN old_rows o USING(source_item_id) WHERE ROW(n.access_state,n.source_id,n.source_type,n.graph_id,n.current_version_id) IS DISTINCT FROM ROW(o.access_state,o.source_id,o.source_type,o.graph_id,o.current_version_id))' INTO reducing;
  ELSE reducing := true; END IF;
 ELSIF TG_TABLE_NAME='source_versions' AND TG_OP='UPDATE' THEN
  -- Sealing/superseding can remove previously served semantic evidence.
  reducing := true;
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
 IF TG_TABLE_NAME IN ('conversations','conversation_messages','conversation_oceans','query_history','activity_events') THEN
  WITH changed AS (SELECT value j FROM jsonb_array_elements(COALESCE(changed_rows,'[]'::jsonb))), actors AS (
   SELECT NULLIF(j->>'account_id','')::uuid a,NULLIF(j->>'user_id','')::uuid u FROM changed
   UNION SELECT c.account_id,c.user_id FROM changed x JOIN public.conversations c
    ON c.id=CASE WHEN TG_TABLE_NAME IN ('conversation_messages','conversation_oceans') THEN NULLIF(x.j->>'conversation_id','')::uuid END
  )
  INSERT INTO public.read_model_user_fences(account_id,model,user_id,changed_xid,invalidated_xid)
  SELECT DISTINCT a,'*',u,pg_current_xact_id(),CASE WHEN reducing THEN pg_current_xact_id() END
  FROM actors WHERE a IS NOT NULL AND u IS NOT NULL AND u<>'00000000-0000-0000-0000-000000000000'::uuid
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
 WHERE (r.graph_ids && gs OR (r.account_id=ANY(accounts_changed) AND r.model IN (SELECT m.model FROM public.read_model_triggers m WHERE m.table_name=TG_TABLE_NAME))) AND (reducing OR r.model IN (SELECT m.model FROM public.read_model_triggers m WHERE m.table_name=TG_TABLE_NAME));
 RETURN NULL;
END $$;
CREATE TRIGGER read_models_conversation_messages_insert AFTER INSERT ON conversation_messages REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_conversation_messages_update AFTER UPDATE ON conversation_messages REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_conversation_messages_delete AFTER DELETE ON conversation_messages REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_answer_support_receipts_insert AFTER INSERT ON answer_support_receipts REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_answer_support_receipts_update AFTER UPDATE ON answer_support_receipts REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_answer_support_receipts_delete AFTER DELETE ON answer_support_receipts REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_activity_events_insert AFTER INSERT ON activity_events REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_activity_events_update AFTER UPDATE ON activity_events REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_activity_events_delete AFTER DELETE ON activity_events REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_connector_sources_insert AFTER INSERT ON connector_sources REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_connector_sources_update AFTER UPDATE ON connector_sources REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_connector_sources_delete AFTER DELETE ON connector_sources REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_github_watched_repos_insert AFTER INSERT ON github_watched_repos REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_github_watched_repos_update AFTER UPDATE ON github_watched_repos REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_github_watched_repos_delete AFTER DELETE ON github_watched_repos REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_gmail_watched_threads_insert AFTER INSERT ON gmail_watched_threads REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_gmail_watched_threads_update AFTER UPDATE ON gmail_watched_threads REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_gmail_watched_threads_delete AFTER DELETE ON gmail_watched_threads REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_outlook_watched_conversations_insert AFTER INSERT ON outlook_watched_conversations REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_outlook_watched_conversations_update AFTER UPDATE ON outlook_watched_conversations REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_outlook_watched_conversations_delete AFTER DELETE ON outlook_watched_conversations REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_teams_watched_resources_insert AFTER INSERT ON teams_watched_resources REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_teams_watched_resources_update AFTER UPDATE ON teams_watched_resources REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_teams_watched_resources_delete AFTER DELETE ON teams_watched_resources REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_teams_meeting_captures_insert AFTER INSERT ON teams_meeting_captures REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_teams_meeting_captures_update AFTER UPDATE ON teams_meeting_captures REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_teams_meeting_captures_delete AFTER DELETE ON teams_meeting_captures REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_teams_meeting_installations_insert AFTER INSERT ON teams_meeting_installations REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_teams_meeting_installations_update AFTER UPDATE ON teams_meeting_installations REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_teams_meeting_installations_delete AFTER DELETE ON teams_meeting_installations REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_google_chat_watched_spaces_insert AFTER INSERT ON google_chat_watched_spaces REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_google_chat_watched_spaces_update AFTER UPDATE ON google_chat_watched_spaces REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_google_chat_watched_spaces_delete AFTER DELETE ON google_chat_watched_spaces REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_google_meet_captures_insert AFTER INSERT ON google_meet_captures REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_google_meet_captures_update AFTER UPDATE ON google_meet_captures REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_google_meet_captures_delete AFTER DELETE ON google_meet_captures REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_slack_watched_channels_insert AFTER INSERT ON slack_watched_channels REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_slack_watched_channels_update AFTER UPDATE ON slack_watched_channels REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_slack_watched_channels_delete AFTER DELETE ON slack_watched_channels REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_semantic_atoms_insert AFTER INSERT ON semantic_atoms REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_semantic_atoms_update AFTER UPDATE ON semantic_atoms REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_semantic_atoms_delete AFTER DELETE ON semantic_atoms REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_meeting_transcripts_insert AFTER INSERT ON meeting_transcripts REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_meeting_transcripts_update AFTER UPDATE ON meeting_transcripts REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_meeting_transcripts_delete AFTER DELETE ON meeting_transcripts REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_composio_capture_scopes_insert AFTER INSERT ON composio_capture_scopes REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_composio_capture_scopes_update AFTER UPDATE ON composio_capture_scopes REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_composio_capture_scopes_delete AFTER DELETE ON composio_capture_scopes REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_reef_identity_corrections_insert AFTER INSERT ON reef_identity_corrections REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_reef_identity_corrections_update AFTER UPDATE ON reef_identity_corrections REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_reef_identity_corrections_delete AFTER DELETE ON reef_identity_corrections REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
