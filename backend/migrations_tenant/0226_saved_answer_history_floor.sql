-- PAIRED WITH: control 0415 (saved_answer_history_floor).
-- Saved answers are live permission reads, including retained source passages.
-- Keep exact historical version identity: a newer current pointer cannot admit
-- an older excluded input, nor invalidate an older version still above the floor.
-- Legacy fingerprints/headlines, owned questions and the temporal wrapper retain
-- their existing contracts. No source/message/receipt row is rewritten.
-- Serialize NULL-to-bounded floor writes with helper replacement and the
-- upgrade fence scan. Hold through migration commit so an old helper cannot
-- certify a newly bounded graph omitted from the scan.
LOCK TABLE public.team_seat_grants IN SHARE MODE;

CREATE OR REPLACE FUNCTION library_answer_permitted_before_temporal(r_id UUID) RETURNS BOOLEAN LANGUAGE plpgsql STABLE AS $$
DECLARE r answer_support_receipts%ROWTYPE; e JSONB; floors JSONB;
BEGIN
 SELECT * INTO r FROM answer_support_receipts WHERE receipt_id=r_id;
 IF NOT FOUND OR NOT r.evidence_complete OR NOT COALESCE(resolve_tenant_graph_reachable_by_user(r.graph_id,current_setting('app.account_id')::uuid,NULLIF(current_setting('app.user_id',true),'')::uuid),false) THEN RETURN false; END IF;
 -- One actor-bound floor snapshot per receipt, not once per support entry.
 floors := public.reader_history_floors();
 FOR e IN SELECT value FROM jsonb_array_elements(r.support) LOOP
  IF e->>'kind'='source_passage' THEN
   IF NOT EXISTS(SELECT 1 FROM semantic_atom_passages p
    JOIN source_versions v ON v.version_id=p.source_version_id AND v.graph_id=p.graph_id
    JOIN source_items s ON s.source_item_id=v.source_item_id AND s.graph_id=v.graph_id
    WHERE p.graph_id=(e->>'graph_id')::uuid AND p.passage_id=(e->>'passage_id')::uuid
      AND p.semantic_atom_id=(e->>'semantic_atom_id')::uuid
      AND v.version_id=(e->>'source_version_id')::uuid AND s.source_item_id=(e->>'source_item_id')::uuid
      -- Authorize the exact retained version, not its item's current pointer.
      AND public.history_floor_admits(v.graph_id,COALESCE(v.occurred_at,v.captured_at),floors)
      AND s.access_state='permitted' AND COALESCE(resolve_tenant_graph_reachable_by_user(p.graph_id,current_setting('app.account_id')::uuid,NULLIF(current_setting('app.user_id',true),'')::uuid),false)) THEN RETURN false; END IF;
  ELSIF e->>'kind'='user_question' THEN
   IF NOT EXISTS(SELECT 1 FROM conversation_messages m JOIN conversations c ON c.id=m.conversation_id
    WHERE m.id=(e->>'message_id')::uuid AND c.id=(e->>'conversation_id')::uuid
     AND c.graph_id=(e->>'graph_id')::uuid AND c.graph_id=r.graph_id AND c.user_id=(e->>'owner_user_id')::uuid
     AND c.user_id=r.owner_user_id AND m.role='user' AND m.library_scope IS NOT NULL
     AND jsonb_typeof(m.scope_graph_ids)='array' AND jsonb_array_length(m.scope_graph_ids)>0
     AND NOT EXISTS(SELECT 1 FROM jsonb_array_elements(m.scope_graph_ids) g
      WHERE NOT COALESCE(resolve_tenant_graph_reachable_by_user((g.value#>>'{}')::uuid,current_setting('app.account_id')::uuid,NULLIF(current_setting('app.user_id',true),'')::uuid),false))) THEN RETURN false; END IF;
  ELSIF e->>'kind'='legacy_chunk' THEN
   IF e ? 'projection' THEN
    IF NOT library_support_shape(jsonb_build_array(e)) OR e->>'projection'<>'headline_v1' THEN RETURN false; END IF;
    IF library_legacy_headline_sha256((e->>'graph_id')::uuid,(e->>'atom_id')::uuid) IS DISTINCT FROM e->>'input_sha256' THEN RETURN false; END IF;
   ELSE
   IF NOT EXISTS(SELECT 1 FROM atoms a WHERE a.graph_id=(e->>'graph_id')::uuid
    AND a.atom_id=(e->>'atom_id')::uuid
    AND encode(sha256(convert_to(jsonb_build_array(a.content_inline,a.header_len,a.source_type,a.source_id,a.timestamp,a.created_at)::text,'UTF8')),'hex')=e->>'input_sha256'
    AND library_atom_permitted(a.graph_id,a.atom_id) AND COALESCE(resolve_tenant_graph_reachable_by_user(a.graph_id,current_setting('app.account_id')::uuid,NULLIF(current_setting('app.user_id',true),'')::uuid),false)) THEN RETURN false; END IF;
   END IF;
  ELSE RETURN false;
  END IF;
 END LOOP;
 RETURN true;
END $$;

-- Upgrade barrier: pre-fix workers may have published an unsafe answer AFTER a
-- floor reduction. Stamp durable fences even when no cache row exists, so both
-- existing payloads and racing pre-fix snapshots fail the shared GET/publish
-- certificate. Restrict the barrier to graphs with actual non-null seat floors.
INSERT INTO public.read_model_graph_fences(account_id,graph_id,changed_xid,invalidated_xid)
SELECT DISTINCT g.account_id,g.id,pg_current_xact_id(),pg_current_xact_id()
 FROM public.team_seat_grants s JOIN public.teams t ON t.id=s.team_id
 JOIN public.graphs g ON g.id=t.graph_id AND g.account_id=s.account_id
 WHERE s.history_from IS NOT NULL
 ORDER BY g.account_id,g.id
ON CONFLICT(account_id,graph_id) DO UPDATE
 SET changed_xid=EXCLUDED.changed_xid,invalidated_xid=EXCLUDED.invalidated_xid;
