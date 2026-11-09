-- PAIRED WITH: control 0411 (read_models).
-- Additive read-model storage. Dirty generations and claim tokens fence concurrent workers.
-- Due time is LEAST(last bump + debounce, first dirty + max delay).
CREATE TABLE read_models (
 model text NOT NULL, scope_key text NOT NULL, version int NOT NULL,
 account_id uuid NOT NULL , graph_ids uuid[] NOT NULL,
 viewer_user_id uuid, compute_as uuid NOT NULL, params jsonb NOT NULL DEFAULT '{}',
 payload jsonb, computed_at timestamptz, computed_snapshot pg_snapshot,
 invalidated_xid xid8, dirty_at timestamptz, dirty_bumped_at timestamptz,
 dirty_generation bigint NOT NULL DEFAULT 1,
 refresh_claimed_at timestamptz, claim_token uuid,
 retry_after timestamptz, last_error text, failed_attempts int NOT NULL DEFAULT 0,
 debounce_seconds int NOT NULL DEFAULT 30, max_delay_seconds int NOT NULL DEFAULT 60,
 PRIMARY KEY(model,scope_key),
 CHECK ((payload IS NULL) = (computed_at IS NULL) AND (computed_at IS NULL) = (computed_snapshot IS NULL)),
 CHECK (debounce_seconds >= 0 AND max_delay_seconds >= debounce_seconds)
);
CREATE INDEX read_models_due_idx ON read_models(dirty_bumped_at) WHERE dirty_at IS NOT NULL;
CREATE INDEX read_models_graph_idx ON read_models USING gin(graph_ids);
CREATE TABLE read_model_triggers(table_name text NOT NULL,model text NOT NULL,PRIMARY KEY(table_name,model));


-- Durable metadata exists independently of any cached scope. Raw access is
-- system-only: members cannot enumerate graph ids or another actor's events.
CREATE TABLE read_model_graph_fences (
 account_id uuid NOT NULL, graph_id uuid NOT NULL,
 changed_xid xid8 NOT NULL, invalidated_xid xid8,
 PRIMARY KEY(account_id,graph_id)
);
CREATE TABLE read_model_user_fences (
 account_id uuid NOT NULL, model text NOT NULL, user_id uuid NOT NULL,
 invalidated_xid xid8 NOT NULL,
 PRIMARY KEY(account_id,model,user_id)
);
ALTER TABLE read_model_graph_fences ENABLE ROW LEVEL SECURITY;
ALTER TABLE read_model_user_fences ENABLE ROW LEVEL SECURITY;
CREATE POLICY read_model_graph_fence_system ON read_model_graph_fences
 USING(current_setting('app.tenant_actor',true)='system') WITH CHECK(current_setting('app.tenant_actor',true)='system');
CREATE POLICY read_model_user_fence_system ON read_model_user_fences
 USING(current_setting('app.tenant_actor',true)='system') WITH CHECK(current_setting('app.tenant_actor',true)='system');

-- 0 = withhold, 1 = valid but stale, 2 = current. Only indexed metadata probes;
-- no content or permission-heavy function. Scope authorization remains on
-- read_models RLS and the verified Rust viewer. This oracle returns no ids,
-- timestamps or content and refuses foreign account / private viewer probes.
CREATE FUNCTION read_model_fence_state(a uuid, gs uuid[], m text, u uuid, s pg_snapshot)
RETURNS int LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
BEGIN
 IF s IS NULL THEN RETURN 0; END IF;
 IF current_setting('app.tenant_actor',true) IS DISTINCT FROM 'system' AND
    (a IS DISTINCT FROM NULLIF(current_setting('app.account_id',true),'')::uuid OR
     (u IS NOT NULL AND u IS DISTINCT FROM NULLIF(current_setting('app.user_id',true),'')::uuid))
 THEN RETURN 0; END IF;
 IF EXISTS(SELECT 1 FROM public.read_model_graph_fences f WHERE f.account_id=a AND f.graph_id=ANY(gs)
           AND f.invalidated_xid IS NOT NULL AND NOT pg_visible_in_snapshot(f.invalidated_xid,s))
 OR EXISTS(SELECT 1 FROM public.read_model_user_fences f WHERE f.account_id=a AND f.model=m
           AND f.user_id=COALESCE(u,'00000000-0000-0000-0000-000000000000'::uuid)
           AND NOT pg_visible_in_snapshot(f.invalidated_xid,s))
 THEN RETURN 0; END IF;
 IF EXISTS(SELECT 1 FROM public.read_model_graph_fences f WHERE f.account_id=a AND f.graph_id=ANY(gs)
           AND NOT pg_visible_in_snapshot(f.changed_xid,s))
 THEN RETURN 1; END IF;
 RETURN 2;
END $$;

-- Called after the authorized control-fact commit, before acknowledgement.
-- The nil-user key certifies Shared rows; the actor key certifies Windowed
-- rows even with an empty graph set. Row-lock ordering, never GREATEST(xid),
-- ensures visibility of the last fence implies all preceding writers committed.
CREATE FUNCTION read_models_mark_user_dirty(a uuid,u uuid,models text[]) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
BEGIN
 IF current_setting('app.tenant_actor',true) IS DISTINCT FROM 'system' THEN
  RAISE EXCEPTION 'read-model event requires system scope' USING ERRCODE='42501';
 END IF;
 IF a IS NULL OR u IS NULL OR u='00000000-0000-0000-0000-000000000000'::uuid THEN
  RAISE EXCEPTION 'invalid read-model event identity' USING ERRCODE='22023';
 END IF;
 INSERT INTO public.read_model_user_fences(account_id,model,user_id,invalidated_xid)
 SELECT DISTINCT a,m,actor,pg_current_xact_id() FROM unnest(models) m
 CROSS JOIN unnest(ARRAY[u,'00000000-0000-0000-0000-000000000000'::uuid]) actor ORDER BY m,actor
 ON CONFLICT(account_id,model,user_id) DO UPDATE SET invalidated_xid=EXCLUDED.invalidated_xid;
 UPDATE public.read_models SET dirty_at=COALESCE(dirty_at,statement_timestamp()),dirty_bumped_at=statement_timestamp(),
 dirty_generation=dirty_generation+1,retry_after=NULL,invalidated_xid=pg_current_xact_id()
 WHERE account_id=a AND model=ANY(models) AND (viewer_user_id=u OR viewer_user_id IS NULL);
END $$;

-- One implementation on both planes. SECURITY DEFINER only resolves visibility;
-- no content is returned. The actor/account are scoped by verified auth in Rust.
CREATE FUNCTION read_model_visible_windows()
RETURNS TABLE(graph_id uuid, history_floor timestamptz)
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
BEGIN
 IF to_regprocedure('public.tenant_rls_visible_graph_windows()') IS NOT NULL THEN
  RETURN QUERY SELECT w.graph_id,w.history_floor FROM public.tenant_rls_visible_graph_windows() w JOIN public.graphs g ON g.id=w.graph_id WHERE g.archived_at IS NULL;
 ELSE
  RETURN QUERY SELECT g.id,NULL::timestamptz FROM public.resolve_reachable_graphs(NULLIF(current_setting('app.user_id',true),'')::uuid) r
   JOIN public.graphs g ON g.id=r.id WHERE g.account_id=NULLIF(current_setting('app.account_id',true),'')::uuid;
 END IF;
END $$;
-- Resolve the legacy cross-account personal-ocean stats scope only after
-- verifying the actor can reach it. No owner id can be supplied by HTTP.
CREATE FUNCTION read_model_graph_account(g uuid) RETURNS uuid
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE a uuid;
BEGIN
 IF to_regprocedure('public.tenant_rls_visible_graph_windows()') IS NOT NULL THEN
  SELECT x.account_id INTO a FROM public.graphs x JOIN public.tenant_rls_visible_graph_windows() w ON w.graph_id=x.id
  WHERE x.id=g AND x.archived_at IS NULL AND x.account_id=NULLIF(current_setting('app.account_id',true),'')::uuid;
 ELSE
  SELECT x.account_id INTO a FROM public.graphs x
  WHERE x.id=g AND x.archived_at IS NULL AND public.resolve_graph_reachable_by_user(g,NULLIF(current_setting('app.user_id',true),'')::uuid);
 END IF;
 RETURN a;
END $$;
ALTER TABLE read_models ENABLE ROW LEVEL SECURITY;
CREATE POLICY read_model_member ON read_models USING (
 account_id=NULLIF(current_setting('app.account_id',true),'')::uuid
 AND graph_ids <@ ARRAY(SELECT graph_id FROM read_model_visible_windows())
 AND (viewer_user_id=NULLIF(current_setting('app.user_id',true),'')::uuid
      OR (viewer_user_id IS NULL AND NOT EXISTS(SELECT 1 FROM read_model_visible_windows() w WHERE w.graph_id=ANY(graph_ids) AND w.history_floor IS NOT NULL)))
) WITH CHECK (
 account_id=NULLIF(current_setting('app.account_id',true),'')::uuid
 AND graph_ids <@ ARRAY(SELECT graph_id FROM read_model_visible_windows())
 AND (viewer_user_id=NULLIF(current_setting('app.user_id',true),'')::uuid
      OR (viewer_user_id IS NULL AND NOT EXISTS(SELECT 1 FROM read_model_visible_windows() w WHERE w.graph_id=ANY(graph_ids) AND w.history_floor IS NOT NULL)))
);
-- Service scope may touch bookkeeping, never used by compute. Reads still reverify
-- the stored actor's membership and exact class inside the compute snapshot.
CREATE POLICY read_model_system ON read_models USING (current_setting('app.tenant_actor',true)='system') WITH CHECK (current_setting('app.tenant_actor',true)='system');
ALTER TABLE read_model_triggers ENABLE ROW LEVEL SECURITY;
CREATE POLICY read_model_metadata ON read_model_triggers USING(true) WITH CHECK(true);

CREATE FUNCTION read_models_mark_dirty() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE gs uuid[]; reducing boolean; changed_rows jsonb;
BEGIN
 -- Transition tables collapse bulk writes to one update. JSON supports both
 -- direct graph rows and link/membership rows without schema-specific triggers.
 IF TG_TABLE_NAME IN ('atoms','nodes','node_semantic_atoms','source_items','source_versions','source_version_chunks','coding_sessions','query_history','journal_rollups','journal_gists') THEN
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
  UNION SELECT t.graph_id FROM changed c JOIN public.teams t ON t.id=NULLIF(c.j->>'team_id','')::uuid
  UNION SELECT o.graph_id FROM changed c JOIN public.conversation_oceans o ON o.conversation_id=COALESCE(NULLIF(c.j->>'conversation_id','')::uuid,NULLIF(c.j->>'id','')::uuid) WHERE TG_TABLE_NAME='conversations'
  UNION SELECT g.id FROM changed c JOIN public.graphs g ON g.id=NULLIF(c.j->>'id','')::uuid WHERE TG_TABLE_NAME='graphs'
 ) x WHERE x.g IS NOT NULL;
 END IF;
 reducing := TG_TABLE_NAME IN ('source_version_chunks','team_members','team_seat_grants','graphs')
             OR (TG_TABLE_NAME IN ('atoms','source_versions') AND TG_OP='DELETE');
 IF TG_TABLE_NAME='atoms' AND TG_OP='UPDATE' THEN
  EXECUTE 'SELECT EXISTS(SELECT 1 FROM new_rows n FULL JOIN old_rows o USING(atom_id) WHERE ROW(n.evidence_retracted,n.derived_copy_bound,n.source_evidence_bound,n.graph_id,n.timestamp) IS DISTINCT FROM ROW(o.evidence_retracted,o.derived_copy_bound,o.source_evidence_bound,o.graph_id,o.timestamp))' INTO reducing;
 ELSIF TG_TABLE_NAME='source_items' THEN
  IF TG_OP='INSERT' THEN EXECUTE 'SELECT EXISTS(SELECT 1 FROM new_rows WHERE access_state<>''permitted'')' INTO reducing;
  ELSIF TG_OP='UPDATE' THEN EXECUTE 'SELECT EXISTS(SELECT 1 FROM new_rows n JOIN old_rows o USING(source_item_id) WHERE ROW(n.access_state,n.source_id,n.source_type,n.graph_id) IS DISTINCT FROM ROW(o.access_state,o.source_id,o.source_type,o.graph_id))' INTO reducing;
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
 UPDATE public.read_models r SET dirty_at=COALESCE(r.dirty_at,statement_timestamp()),
  dirty_bumped_at=statement_timestamp(),dirty_generation=r.dirty_generation+1,
  retry_after=NULL,
  invalidated_xid=CASE WHEN reducing THEN pg_current_xact_id() ELSE r.invalidated_xid END
 WHERE r.graph_ids && gs AND (reducing OR r.model IN (SELECT m.model FROM public.read_model_triggers m WHERE m.table_name=TG_TABLE_NAME));
 RETURN NULL;
END $$;
CREATE TRIGGER read_models_atoms_insert AFTER INSERT ON atoms REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_atoms_update AFTER UPDATE ON atoms REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_atoms_delete AFTER DELETE ON atoms REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_nodes_insert AFTER INSERT ON nodes REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_nodes_update AFTER UPDATE ON nodes REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_nodes_delete AFTER DELETE ON nodes REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_node_atoms_insert AFTER INSERT ON node_atoms REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_node_atoms_update AFTER UPDATE ON node_atoms REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_node_atoms_delete AFTER DELETE ON node_atoms REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_node_semantic_atoms_insert AFTER INSERT ON node_semantic_atoms REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_node_semantic_atoms_update AFTER UPDATE ON node_semantic_atoms REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_node_semantic_atoms_delete AFTER DELETE ON node_semantic_atoms REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_source_items_insert AFTER INSERT ON source_items REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_source_items_update AFTER UPDATE ON source_items REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_source_items_delete AFTER DELETE ON source_items REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_source_versions_insert AFTER INSERT ON source_versions REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_source_versions_update AFTER UPDATE ON source_versions REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_source_versions_delete AFTER DELETE ON source_versions REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_source_version_chunks_insert AFTER INSERT ON source_version_chunks REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_source_version_chunks_update AFTER UPDATE ON source_version_chunks REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_source_version_chunks_delete AFTER DELETE ON source_version_chunks REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_coding_sessions_insert AFTER INSERT ON coding_sessions REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_coding_sessions_update AFTER UPDATE ON coding_sessions REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_coding_sessions_delete AFTER DELETE ON coding_sessions REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_query_history_insert AFTER INSERT ON query_history REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_query_history_update AFTER UPDATE ON query_history REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_query_history_delete AFTER DELETE ON query_history REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_journal_rollups_insert AFTER INSERT ON journal_rollups REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_journal_rollups_update AFTER UPDATE ON journal_rollups REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_journal_rollups_delete AFTER DELETE ON journal_rollups REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_journal_gists_insert AFTER INSERT ON journal_gists REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_journal_gists_update AFTER UPDATE ON journal_gists REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_journal_gists_delete AFTER DELETE ON journal_gists REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_team_members_insert AFTER INSERT ON team_members REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_team_members_update AFTER UPDATE ON team_members REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_team_members_delete AFTER DELETE ON team_members REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_team_seat_grants_insert AFTER INSERT ON team_seat_grants REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_team_seat_grants_update AFTER UPDATE ON team_seat_grants REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_team_seat_grants_delete AFTER DELETE ON team_seat_grants REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_graphs_insert AFTER INSERT ON graphs REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_graphs_update AFTER UPDATE ON graphs REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_graphs_delete AFTER DELETE ON graphs REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();

REVOKE ALL ON FUNCTION read_model_visible_windows(),read_model_graph_account(uuid),read_models_mark_dirty(),read_model_fence_state(uuid,uuid[],text,uuid,pg_snapshot),read_models_mark_user_dirty(uuid,uuid,text[]) FROM PUBLIC;
DO $$ DECLARE r text; BEGIN
 FOREACH r IN ARRAY ARRAY['coralswarm_app','coralswarm_tenant_app'] LOOP
  IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname=r) THEN
   EXECUTE format('GRANT SELECT,INSERT,UPDATE,DELETE ON read_models,read_model_triggers,read_model_graph_fences,read_model_user_fences TO %I',r);
   EXECUTE format('GRANT EXECUTE ON FUNCTION read_model_visible_windows(),read_model_graph_account(uuid),read_model_fence_state(uuid,uuid[],text,uuid,pg_snapshot),read_models_mark_user_dirty(uuid,uuid,text[]) TO %I',r);
  END IF;
 END LOOP;
END $$;
CREATE TRIGGER read_models_conversations_insert AFTER INSERT ON conversations REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_conversations_update AFTER UPDATE ON conversations REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_conversations_delete AFTER DELETE ON conversations REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_conversation_oceans_insert AFTER INSERT ON conversation_oceans REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_conversation_oceans_update AFTER UPDATE ON conversation_oceans REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
CREATE TRIGGER read_models_conversation_oceans_delete AFTER DELETE ON conversation_oceans REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty();
