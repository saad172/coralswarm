-- L4 additive union over effective L3 control0412+0413 / tenant0224.
-- L2/L3 migration bytes remain unchanged.
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
CREATE OR REPLACE FUNCTION read_model_fence_state(a uuid, gs uuid[], m text, u uuid, s pg_snapshot)
RETURNS int LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
BEGIN
 IF s IS NULL THEN RETURN 0; END IF;
 IF current_setting('app.tenant_actor',true) IS DISTINCT FROM 'system' AND
    (a IS DISTINCT FROM NULLIF(current_setting('app.account_id',true),'')::uuid OR
     (u IS NOT NULL AND u IS DISTINCT FROM NULLIF(current_setting('app.user_id',true),'')::uuid))
 THEN RETURN 0; END IF;
 IF EXISTS(SELECT 1 FROM public.read_model_graph_fences f WHERE (f.account_id=a OR (m='usage_summary' AND f.account_id='00000000-0000-0000-0000-000000000000'::uuid AND f.graph_id='00000000-0000-0000-0000-000000000000'::uuid)) AND (f.graph_id=ANY(gs) OR (m='usage_summary' AND gs=ARRAY['00000000-0000-0000-0000-000000000000'::uuid]))
           AND f.invalidated_xid IS NOT NULL AND NOT pg_visible_in_snapshot(f.invalidated_xid,s))
 OR EXISTS(SELECT 1 FROM public.read_model_user_fences f WHERE f.account_id=a AND f.model IN (m,'*')
           AND (f.user_id=COALESCE(u,'00000000-0000-0000-0000-000000000000'::uuid) OR (f.model='*' AND f.user_id='00000000-0000-0000-0000-000000000000'::uuid))
           AND NOT pg_visible_in_snapshot(f.invalidated_xid,s))
 THEN RETURN 0; END IF;
 IF EXISTS(SELECT 1 FROM public.read_model_graph_fences f WHERE (f.account_id=a OR (m='usage_summary' AND f.account_id='00000000-0000-0000-0000-000000000000'::uuid AND f.graph_id='00000000-0000-0000-0000-000000000000'::uuid)) AND (f.graph_id=ANY(gs) OR (m='usage_summary' AND gs=ARRAY['00000000-0000-0000-0000-000000000000'::uuid]))
           AND NOT pg_visible_in_snapshot(f.changed_xid,s))
 OR EXISTS(SELECT 1 FROM public.read_model_user_fences f WHERE f.account_id=a AND f.model IN (m,'*')
           AND (f.user_id=COALESCE(u,'00000000-0000-0000-0000-000000000000'::uuid) OR (f.model='*' AND f.user_id='00000000-0000-0000-0000-000000000000'::uuid))
           AND f.changed_xid IS NOT NULL AND NOT pg_visible_in_snapshot(f.changed_xid,s))
 THEN RETURN 1; END IF;
 RETURN 2;
END $$;
CREATE FUNCTION read_model_org_usage_facts(a uuid, period timestamptz) RETURNS jsonb
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
BEGIN RAISE EXCEPTION 'org metering requires the bound tenant plane' USING ERRCODE='42501'; END $$;
REVOKE ALL ON FUNCTION read_model_org_usage_facts(uuid,timestamptz) FROM PUBLIC;
DO $$ BEGIN IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname='coralswarm_app') THEN GRANT EXECUTE ON FUNCTION read_model_org_usage_facts(uuid,timestamptz) TO coralswarm_app; END IF; END $$;
CREATE FUNCTION read_models_stamp_account(a uuid, g uuid, reducing boolean) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
BEGIN
 -- Distributed graph certificates do not lock a shared account/cache row.
 -- Graphless facts alone use the reserved nil graph. Stored GET observes the
 -- domain oracle and uses the canonical shared refresh helper when stale.
 INSERT INTO public.read_model_graph_fences(account_id,graph_id,changed_xid,invalidated_xid)
 SELECT a,g,pg_current_xact_id(),CASE WHEN reducing THEN pg_current_xact_id() END
 WHERE NOT EXISTS(SELECT 1 FROM public.read_model_graph_fences f WHERE f.account_id=a AND f.graph_id=g
  AND f.changed_xid=pg_current_xact_id() AND (NOT reducing OR f.invalidated_xid=pg_current_xact_id()))
 ON CONFLICT(account_id,graph_id) DO UPDATE SET changed_xid=EXCLUDED.changed_xid,
 invalidated_xid=COALESCE(EXCLUDED.invalidated_xid,read_model_graph_fences.invalidated_xid);
END $$;
REVOKE ALL ON FUNCTION read_models_stamp_account(uuid,uuid,boolean) FROM PUBLIC;

CREATE FUNCTION read_models_metering_dirty() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE r record; projection text; source_sql text; tenant boolean;
BEGIN
 tenant:=to_regclass('public.tenant_meta') IS NOT NULL;
 projection:=CASE TG_TABLE_NAME WHEN 'atoms' THEN 'NULL::uuid account_id,graph_id'
  WHEN 'graphs' THEN 'account_id,id graph_id' ELSE 'account_id,graph_id' END;
 source_sql:=CASE TG_OP WHEN 'INSERT' THEN format('SELECT %s FROM new_rows',projection)
  WHEN 'DELETE' THEN format('SELECT %s FROM old_rows',projection)
  ELSE format('SELECT %s FROM new_rows UNION SELECT %s FROM old_rows',projection,projection) END;
 FOR r IN EXECUTE format('SELECT DISTINCT a,g FROM (
  SELECT COALESCE(x.account_id,g.account_id,CASE WHEN $1 THEN ''00000000-0000-0000-0000-000000000000''::uuid END) a,
   CASE WHEN COALESCE(x.account_id,g.account_id) IS NULL THEN ''00000000-0000-0000-0000-000000000000''::uuid ELSE COALESCE(x.graph_id,''00000000-0000-0000-0000-000000000000''::uuid) END g
  FROM (%s) x LEFT JOIN public.graphs g ON g.id=x.graph_id) pairs WHERE a IS NOT NULL ORDER BY a,g',source_sql) USING tenant LOOP
  PERFORM public.read_models_stamp_account(r.a,r.g,TG_OP<>'INSERT');
 END LOOP;
 RETURN NULL;
END $$;
REVOKE ALL ON FUNCTION read_models_metering_dirty() FROM PUBLIC;
CREATE TRIGGER read_models_semantic_atom_passages_insert AFTER INSERT ON semantic_atom_passages REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_semantic_atom_passages_update AFTER UPDATE ON semantic_atom_passages REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_semantic_atom_passages_delete AFTER DELETE ON semantic_atom_passages REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_relationship_edge_insert AFTER INSERT ON relationship_edge REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_relationship_edge_update AFTER UPDATE ON relationship_edge REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_relationship_edge_delete AFTER DELETE ON relationship_edge REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_managed_enrichment_inputs_insert AFTER INSERT ON managed_enrichment_inputs REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_managed_enrichment_inputs_update AFTER UPDATE ON managed_enrichment_inputs REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_managed_enrichment_inputs_delete AFTER DELETE ON managed_enrichment_inputs REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_managed_attempt_receipts_insert AFTER INSERT ON managed_attempt_receipts REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_managed_attempt_receipts_update AFTER UPDATE ON managed_attempt_receipts REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_managed_attempt_receipts_delete AFTER DELETE ON managed_attempt_receipts REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_message_feedback_insert AFTER INSERT ON message_feedback REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_message_feedback_update AFTER UPDATE ON message_feedback REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_message_feedback_delete AFTER DELETE ON message_feedback REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_teams_insert AFTER INSERT ON teams REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_teams_update AFTER UPDATE ON teams REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_teams_delete AFTER DELETE ON teams REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_llm_usage_insert AFTER INSERT ON llm_usage REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_llm_usage_update AFTER UPDATE ON llm_usage REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_llm_usage_delete AFTER DELETE ON llm_usage REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_metering_llm_usage_insert AFTER INSERT ON llm_usage REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_metering_dirty();
CREATE TRIGGER read_models_metering_llm_usage_update AFTER UPDATE ON llm_usage REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_metering_dirty();
CREATE TRIGGER read_models_metering_llm_usage_delete AFTER DELETE ON llm_usage REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_metering_dirty();
CREATE TRIGGER read_models_metering_query_history_insert AFTER INSERT ON query_history REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_metering_dirty();
CREATE TRIGGER read_models_metering_query_history_update AFTER UPDATE ON query_history REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_metering_dirty();
CREATE TRIGGER read_models_metering_query_history_delete AFTER DELETE ON query_history REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_metering_dirty();
CREATE TRIGGER read_models_metering_atoms_insert AFTER INSERT ON atoms REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_metering_dirty();
CREATE TRIGGER read_models_metering_atoms_update AFTER UPDATE ON atoms REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_metering_dirty();
CREATE TRIGGER read_models_metering_atoms_delete AFTER DELETE ON atoms REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_metering_dirty();
CREATE TRIGGER read_models_metering_graphs_insert AFTER INSERT ON graphs REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_metering_dirty();
CREATE TRIGGER read_models_metering_graphs_update AFTER UPDATE ON graphs REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_metering_dirty();
CREATE TRIGGER read_models_metering_graphs_delete AFTER DELETE ON graphs REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_metering_dirty();

CREATE OR REPLACE FUNCTION read_model_control_fact_changed() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE changed_rows jsonb; affected uuid[]; unchanged boolean; authority boolean := true; ignored text[];
BEGIN
 -- Strip only known operational fields when comparing authority. Everything
 -- else (including future columns) conservatively remains authority-bearing.
 -- Ordinary telemetry is durable freshness even on INSERT/DELETE, never a key.
 ignored := ARRAY['updated_at'];
 -- L4 known ordinary billing observations do not supersede model addresses.
 -- Account/user binding and every unknown future field remain conservative.
 IF TG_TABLE_NAME='credit_ledger' THEN
  authority := TG_OP='UPDATE';
  ignored := ignored || ARRAY['id','entry_kind','credits','source_job_id','source_query_id','graph_id','journal_period','journal_granularity','cost_usd','created_at','attributed_user_id','stripe_checkout_session_id','source_receipt_id','query_revision'];
 ELSIF TG_TABLE_NAME='credit_balances' THEN
  authority := TG_OP='UPDATE';
  ignored := ignored || ARRAY['balance','job_axis_cursor_finished_at','job_axis_cursor_job_id','query_axis_cursor_created_at','query_axis_cursor_usage_id','journal_axis_cursor_period','journal_axis_cursor_usage_id','receipt_cursor_at','receipt_cursor_id','managed_debited_credits'];
 ELSIF TG_TABLE_NAME='member_allowance_periods' THEN
  ignored := ignored || ARRAY['consumed_credits','managed_consumed_credits'];
 ELSIF TG_TABLE_NAME='slack_unmatched_email_notices' THEN
  authority := TG_OP='UPDATE';
  ignored := ignored || ARRAY['last_notified_at'];
 ELSIF TG_TABLE_NAME='mcp_tool_calls' THEN
  authority := TG_OP='UPDATE';
  ignored := ignored || ARRAY['id','client_id','token_jti','tool','scope_used','graph_id','target_id','args_digest','args_summary','outcome','error_code','duration_ms','request_id','created_at'];
 ELSIF TG_TABLE_NAME='google_meet_sync_state' THEN
  authority := TG_OP='UPDATE';
  ignored := ignored || ARRAY['last_synced_end_time','claimed_until','claim_id','last_attempted_at','last_synced_at','last_error','last_error_at'];
 ELSIF TG_TABLE_NAME='google_watch_channels' THEN
  authority := TG_OP='UPDATE';
  ignored := ignored || ARRAY['resource_id','channel_id','channel_resource_id','topic_name','channel_token','expires_at','last_renewed_at','last_error','last_error_at','sync_token','backfill_page_token','backfill_complete','push_pending_at'];
 ELSIF TG_TABLE_NAME='oauth_tokens' THEN
  ignored := ignored || ARRAY['access_token','refresh_token','user_token_encrypted','token_type','expires_at','dek_migrated_at'];
 ELSIF TG_TABLE_NAME='addin_tokens' THEN
  ignored := ignored || ARRAY['last_used_at'];
 ELSIF TG_TABLE_NAME='composio_connections' THEN
  ignored := ignored || ARRAY['self_check','self_check_reason','self_checked_at','namespace_checked_at'];
 ELSIF TG_TABLE_NAME='gmail_everything_mode' THEN
  ignored := ignored || ARRAY['resume_cursor','claimed_until','claim_id','last_synced_at','last_attempted_at','last_error','last_error_at','high_water_at','walk_started_at','walk_after','last_full_walk_at','page_failures','unreadable_ids','given_up_ids','page_failing_id','walk_skip_ids'];
 ELSIF TG_TABLE_NAME='outlook_everything_mode' THEN
  ignored := ignored || ARRAY['resume_cursor','claimed_until','claim_id','last_synced_at','last_attempted_at','last_error','last_error_at'];
 ELSIF TG_TABLE_NAME='microsoft_calendar_sync_state' THEN
  ignored := ignored || ARRAY['last_synced_at','last_error','last_error_at','delta_link','delta_resume_link','delta_window_end','delta_reconciled_at','delta_window_start','delta_verify_cursor','delta_verify_lap_end'];
 END IF;
 IF TG_OP='UPDATE' THEN
  -- Exact no-ops (and timestamp-only heartbeats) need no notification at all.
  EXECUTE 'SELECT NOT EXISTS(SELECT 1 FROM ((SELECT to_jsonb(n)-''updated_at'' j FROM new_rows n EXCEPT ALL SELECT to_jsonb(o)-''updated_at'' FROM old_rows o) UNION ALL (SELECT to_jsonb(o)-''updated_at'' FROM old_rows o EXCEPT ALL SELECT to_jsonb(n)-''updated_at'' FROM new_rows n)) changed)' INTO unchanged;
  IF unchanged THEN RETURN NULL; END IF;
  IF authority THEN
   EXECUTE 'SELECT EXISTS(SELECT 1 FROM ((SELECT to_jsonb(n)-$1 j FROM new_rows n EXCEPT ALL SELECT to_jsonb(o)-$1 FROM old_rows o) UNION ALL (SELECT to_jsonb(o)-$1 FROM old_rows o EXCEPT ALL SELECT to_jsonb(n)-$1 FROM new_rows n)) changed)' INTO authority USING ignored;
  END IF;
 END IF;
 IF TG_OP='INSERT' THEN EXECUTE 'SELECT jsonb_agg(to_jsonb(n)) FROM new_rows n' INTO changed_rows;
 ELSIF TG_OP='DELETE' THEN EXECUTE 'SELECT jsonb_agg(to_jsonb(o)) FROM old_rows o' INTO changed_rows;
 ELSE EXECUTE 'SELECT jsonb_agg(j) FROM (SELECT to_jsonb(n) j FROM new_rows n UNION ALL SELECT to_jsonb(o) FROM old_rows o) s' INTO changed_rows;
 END IF;
 WITH changes AS (SELECT value j FROM jsonb_array_elements(COALESCE(changed_rows,'[]'::jsonb)))
 SELECT array_agg(DISTINCT account_id) INTO affected FROM (
  SELECT NULLIF(j->>'account_id','')::uuid account_id FROM changes
  UNION SELECT NULLIF(j->>'id','')::uuid FROM changes WHERE TG_TABLE_NAME='accounts'
  UNION SELECT a.id FROM changes c JOIN public.accounts a ON a.clerk_org_id=c.j->>'clerk_org_id' WHERE TG_TABLE_NAME IN ('org_connector_flags','org_revocations','org_membership_tombstones','personal_orgs')
  UNION SELECT m.account_id FROM changes c JOIN public.account_members m ON m.user_id=CASE WHEN TG_TABLE_NAME='users' THEN NULLIF(c.j->>'id','')::uuid ELSE NULLIF(c.j->>'user_id','')::uuid END
  UNION SELECT a.id FROM changes c JOIN public.accounts a ON a.owner_user_id=CASE WHEN TG_TABLE_NAME='users' THEN NULLIF(c.j->>'id','')::uuid ELSE NULLIF(c.j->>'user_id','')::uuid END
  -- Applicants need not be members yet: their identity still labels each target org's queue/history.
  UNION SELECT r.account_id FROM changes c JOIN public.org_join_requests r ON r.user_id=CASE WHEN TG_TABLE_NAME='users' THEN NULLIF(c.j->>'id','')::uuid END
  UNION SELECT g.account_id FROM changes c JOIN public.grants g ON g.id=NULLIF(c.j->>'grant_id','')::uuid WHERE TG_TABLE_NAME='grant_seat_tiers'
  UNION SELECT m.account_id FROM changes c JOIN public.users u ON u.email=c.j->>'email' JOIN public.account_members m ON m.user_id=u.id WHERE TG_TABLE_NAME='slack_unmatched_email_notices'
  UNION SELECT m.account_id FROM changes c JOIN public.users u ON u.clerk_user_id=c.j->>'clerk_user_id' JOIN public.account_members m ON m.user_id=u.id WHERE TG_TABLE_NAME='personal_orgs'
  UNION SELECT id FROM public.accounts WHERE TG_TABLE_NAME IN ('plans','seat_tiers','model_rates','credit_conversion')
 ) a WHERE account_id IS NOT NULL;
 INSERT INTO public.read_model_control_revisions(account_id,revision,freshness)
 SELECT a.id,CASE WHEN authority THEN 1 ELSE 0 END,1 FROM public.accounts a WHERE a.id=ANY(affected)
 ON CONFLICT(account_id) DO UPDATE SET revision=read_model_control_revisions.revision+CASE WHEN authority THEN 1 ELSE 0 END,
 freshness=read_model_control_revisions.freshness+1;
 RETURN NULL;
END $$;
CREATE TRIGGER read_model_control_credit_balances_insert AFTER INSERT ON credit_balances REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_credit_balances_update AFTER UPDATE ON credit_balances REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_credit_balances_delete AFTER DELETE ON credit_balances REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_credit_ledger_insert AFTER INSERT ON credit_ledger REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_credit_ledger_update AFTER UPDATE ON credit_ledger REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_credit_ledger_delete AFTER DELETE ON credit_ledger REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_grants_insert AFTER INSERT ON grants REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_grants_update AFTER UPDATE ON grants REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_grants_delete AFTER DELETE ON grants REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_grant_seat_tiers_insert AFTER INSERT ON grant_seat_tiers REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_grant_seat_tiers_update AFTER UPDATE ON grant_seat_tiers REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_grant_seat_tiers_delete AFTER DELETE ON grant_seat_tiers REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_plans_insert AFTER INSERT ON plans REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_plans_update AFTER UPDATE ON plans REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_plans_delete AFTER DELETE ON plans REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_seat_tiers_insert AFTER INSERT ON seat_tiers REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_seat_tiers_update AFTER UPDATE ON seat_tiers REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_seat_tiers_delete AFTER DELETE ON seat_tiers REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_model_rates_insert AFTER INSERT ON model_rates REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_model_rates_update AFTER UPDATE ON model_rates REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_model_rates_delete AFTER DELETE ON model_rates REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_credit_conversion_insert AFTER INSERT ON credit_conversion REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_credit_conversion_update AFTER UPDATE ON credit_conversion REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_credit_conversion_delete AFTER DELETE ON credit_conversion REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_org_domains_insert AFTER INSERT ON org_domains REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_org_domains_update AFTER UPDATE ON org_domains REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_org_domains_delete AFTER DELETE ON org_domains REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_org_join_requests_insert AFTER INSERT ON org_join_requests REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_org_join_requests_update AFTER UPDATE ON org_join_requests REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_org_join_requests_delete AFTER DELETE ON org_join_requests REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_org_auto_add_settings_insert AFTER INSERT ON org_auto_add_settings REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_org_auto_add_settings_update AFTER UPDATE ON org_auto_add_settings REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_org_auto_add_settings_delete AFTER DELETE ON org_auto_add_settings REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_slot_reservations_insert AFTER INSERT ON slot_reservations REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_slot_reservations_update AFTER UPDATE ON slot_reservations REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_slot_reservations_delete AFTER DELETE ON slot_reservations REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_stripe_subscriptions_insert AFTER INSERT ON stripe_subscriptions REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_stripe_subscriptions_update AFTER UPDATE ON stripe_subscriptions REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_stripe_subscriptions_delete AFTER DELETE ON stripe_subscriptions REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_member_allowance_periods_insert AFTER INSERT ON member_allowance_periods REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_member_allowance_periods_update AFTER UPDATE ON member_allowance_periods REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_member_allowance_periods_delete AFTER DELETE ON member_allowance_periods REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_slack_unmatched_email_notices_insert AFTER INSERT ON slack_unmatched_email_notices REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_slack_unmatched_email_notices_update AFTER UPDATE ON slack_unmatched_email_notices REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_slack_unmatched_email_notices_delete AFTER DELETE ON slack_unmatched_email_notices REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
-- Queue/history readers and identity invalidation include nonmember applicants.
CREATE INDEX read_models_join_requests_identity ON org_join_requests(user_id,account_id);
CREATE INDEX read_models_join_requests_decided ON org_join_requests(account_id,decided_at DESC) WHERE decision IS NOT NULL;
-- Personal-only indexed fan-in adapter. Each call independently verifies the
-- graph anchor and runs the ordinary stored-row RLS with that single account.
-- INVOKER is deliberate: this grants no content or cache-row bypass.
CREATE FUNCTION read_model_ocean_context(g uuid)
RETURNS TABLE(account_id uuid,anchor uuid,windows jsonb,revision bigint,freshness bigint,authority boolean)
LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE a uuid; prior text:=current_setting('app.account_id',true); actor uuid:=NULLIF(current_setting('app.user_id',true),'')::uuid;
BEGIN
 IF to_regclass('public.tenant_meta') IS NOT NULL THEN RAISE EXCEPTION 'personal fan-in unavailable on tenant plane' USING ERRCODE='42501'; END IF;
 a:=public.read_model_graph_account(g);
 IF a IS NULL OR EXISTS(SELECT 1 FROM public.tenant_databases t WHERE t.account_id=a) THEN RETURN; END IF;
 PERFORM set_config('app.account_id',a::text,true);
 RETURN QUERY SELECT a,g,
  COALESCE((SELECT jsonb_object_agg(w.graph_id::text,to_jsonb(w.history_floor)) FROM public.read_model_visible_windows() w),'{}'::jsonb),
  COALESCE((SELECT r.revision FROM public.read_model_control_revisions r WHERE r.account_id=a),0),
  COALESCE((SELECT r.freshness FROM public.read_model_control_revisions r WHERE r.account_id=a),0),
  EXISTS(SELECT 1 FROM public.account_members m WHERE m.account_id=a AND m.user_id=actor AND m.role IN ('owner','admin'));
 PERFORM set_config('app.account_id',COALESCE(prior,''),true);
EXCEPTION WHEN OTHERS THEN PERFORM set_config('app.account_id',COALESCE(prior,''),true); RAISE;
END $$;
CREATE FUNCTION read_model_ocean_stored(g uuid,k text,v int)
RETURNS TABLE(payload text,computed_at timestamptz,dirty_at timestamptz,computed_control_freshness bigint,fence_state int,eta timestamptz,retry_after timestamptz,valid boolean)
LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE a uuid; prior text:=current_setting('app.account_id',true);
BEGIN
 IF to_regclass('public.tenant_meta') IS NOT NULL THEN RAISE EXCEPTION 'personal fan-in unavailable on tenant plane' USING ERRCODE='42501'; END IF;
 a:=public.read_model_graph_account(g);
 IF a IS NULL OR EXISTS(SELECT 1 FROM public.tenant_databases t WHERE t.account_id=a) THEN RETURN; END IF;
 PERFORM set_config('app.account_id',a::text,true);
 RETURN QUERY SELECT r.payload::text,r.computed_at,r.dirty_at,r.computed_control_freshness,
  public.read_model_fence_state(r.account_id,r.graph_ids,r.model,r.viewer_user_id,r.computed_snapshot),
  LEAST(r.dirty_bumped_at+make_interval(secs=>r.debounce_seconds),r.dirty_at+make_interval(secs=>r.max_delay_seconds)),r.retry_after,
  (r.invalidated_xid IS NULL OR pg_visible_in_snapshot(r.invalidated_xid,r.computed_snapshot))
 FROM public.read_models r WHERE r.account_id=a AND r.model='oceans_index' AND r.scope_key=k AND r.version=v;
 PERFORM set_config('app.account_id',COALESCE(prior,''),true);
EXCEPTION WHEN OTHERS THEN PERFORM set_config('app.account_id',COALESCE(prior,''),true); RAISE;
END $$;
REVOKE ALL ON FUNCTION read_model_ocean_context(uuid),read_model_ocean_stored(uuid,text,int) FROM PUBLIC;
DO $$ BEGIN IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname='coralswarm_app') THEN GRANT EXECUTE ON FUNCTION read_model_ocean_context(uuid),read_model_ocean_stored(uuid,text,int) TO coralswarm_app; END IF; END $$;
-- Indexed object-account anchors on both planes. Existing control-only
-- resolve_node/conversation_account_id helpers do not exist on tenant DBs.
-- Return only an account after the established graph resolver verifies it.
CREATE FUNCTION read_model_node_account(n uuid) RETURNS uuid
LANGUAGE sql STABLE SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
 SELECT public.read_model_graph_account(x.graph_id) FROM public.nodes x WHERE x.node_id=n
$$;
CREATE FUNCTION read_model_conversation_account(c uuid) RETURNS uuid
LANGUAGE sql STABLE SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
 SELECT public.read_model_graph_account(x.graph_id) FROM public.conversations x WHERE x.id=c
$$;
REVOKE ALL ON FUNCTION read_model_node_account(uuid),read_model_conversation_account(uuid) FROM PUBLIC;
DO $$ DECLARE role_name text; BEGIN FOREACH role_name IN ARRAY ARRAY['coralswarm_app','coralswarm_tenant_app'] LOOP IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname=role_name) THEN EXECUTE format('GRANT EXECUTE ON FUNCTION read_model_node_account(uuid),read_model_conversation_account(uuid) TO %I',role_name); END IF; END LOOP; END $$;
