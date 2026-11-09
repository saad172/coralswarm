-- The uuid casts of the support json rely on the library_support_shape CHECK
-- on answer_support_receipts, which rejects any id field that is not a uuid.
-- Unlike the text comparison they replace, the casts also match uppercase hex.
CREATE OR REPLACE FUNCTION library_answer_permitted_before_temporal(r_id UUID) RETURNS BOOLEAN LANGUAGE plpgsql STABLE AS $$
DECLARE r answer_support_receipts%ROWTYPE; e JSONB;
BEGIN
 SELECT * INTO r FROM answer_support_receipts WHERE receipt_id=r_id;
 IF NOT FOUND OR NOT r.evidence_complete OR NOT COALESCE(resolve_graph_reachable_by_user(r.graph_id,NULLIF(current_setting('app.user_id',true),'')::uuid),false) THEN RETURN false; END IF;
 FOR e IN SELECT value FROM jsonb_array_elements(r.support) LOOP
  IF e->>'kind'='source_passage' THEN
   IF NOT EXISTS(SELECT 1 FROM semantic_atom_passages p
    JOIN source_versions v ON v.version_id=p.source_version_id AND v.graph_id=p.graph_id
    JOIN source_items s ON s.source_item_id=v.source_item_id AND s.graph_id=v.graph_id
    WHERE p.graph_id=(e->>'graph_id')::uuid AND p.passage_id=(e->>'passage_id')::uuid
      AND p.semantic_atom_id=(e->>'semantic_atom_id')::uuid
      AND v.version_id=(e->>'source_version_id')::uuid AND s.source_item_id=(e->>'source_item_id')::uuid
      AND s.access_state='permitted' AND COALESCE(resolve_graph_reachable_by_user(p.graph_id,NULLIF(current_setting('app.user_id',true),'')::uuid),false)) THEN RETURN false; END IF;
  ELSIF e->>'kind'='user_question' THEN
   IF NOT EXISTS(SELECT 1 FROM conversation_messages m JOIN conversations c ON c.id=m.conversation_id
    WHERE m.id=(e->>'message_id')::uuid AND c.id=(e->>'conversation_id')::uuid
     AND c.graph_id=(e->>'graph_id')::uuid AND c.graph_id=r.graph_id AND c.user_id=(e->>'owner_user_id')::uuid
     AND c.user_id=r.owner_user_id AND m.role='user' AND m.library_scope IS NOT NULL
     AND jsonb_typeof(m.scope_graph_ids)='array' AND jsonb_array_length(m.scope_graph_ids)>0
     AND NOT EXISTS(SELECT 1 FROM jsonb_array_elements(m.scope_graph_ids) g
      WHERE NOT COALESCE(resolve_graph_reachable_by_user((g.value#>>'{}')::uuid,NULLIF(current_setting('app.user_id',true),'')::uuid),false))) THEN RETURN false; END IF;
  ELSIF e->>'kind'='legacy_chunk' THEN
   IF e ? 'projection' THEN
    IF NOT library_support_shape(jsonb_build_array(e)) OR e->>'projection'<>'headline_v1' THEN RETURN false; END IF;
    IF library_legacy_headline_sha256((e->>'graph_id')::uuid,(e->>'atom_id')::uuid) IS DISTINCT FROM e->>'input_sha256' THEN RETURN false; END IF;
   ELSE
   IF NOT EXISTS(SELECT 1 FROM atoms a WHERE a.graph_id=(e->>'graph_id')::uuid
    AND a.atom_id=(e->>'atom_id')::uuid
    AND encode(sha256(convert_to(jsonb_build_array(a.content_inline,a.header_len,a.source_type,a.source_id,a.timestamp,a.created_at)::text,'UTF8')),'hex')=e->>'input_sha256'
    AND library_atom_permitted(a.graph_id,a.atom_id) AND COALESCE(resolve_graph_reachable_by_user(a.graph_id,NULLIF(current_setting('app.user_id',true),'')::uuid),false)) THEN RETURN false; END IF;
   END IF;
  ELSE RETURN false;
  END IF;
 END LOOP;
 RETURN true;
END $$;
