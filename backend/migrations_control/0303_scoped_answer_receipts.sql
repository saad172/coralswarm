-- Server-generated support receipts. Client timing JSON is never authority.
ALTER TABLE conversations ADD COLUMN library_scope JSONB;
ALTER TABLE conversations ADD CONSTRAINT conversation_library_scope_bound CHECK(library_scope IS NULL OR (jsonb_typeof(library_scope)='object' AND octet_length(library_scope::text)<=16384));
-- Receipts retain identifiers only, never provider text or model prompts.
CREATE FUNCTION library_support_shape(s JSONB) RETURNS BOOLEAN LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE e JSONB; fields TEXT[]; k TEXT;
BEGIN
 IF jsonb_typeof(s) IS DISTINCT FROM 'array' OR jsonb_array_length(s)>200 OR octet_length(s::text)>262144 THEN RETURN false; END IF;
 FOR e IN SELECT value FROM jsonb_array_elements(s) LOOP
  IF jsonb_typeof(e) IS DISTINCT FROM 'object' THEN RETURN false; END IF;
  IF e->>'kind'='source_passage' THEN fields=ARRAY['graph_id','source_item_id','source_version_id','semantic_atom_id','passage_id'];
  ELSIF e->>'kind'='legacy_chunk' THEN fields=ARRAY['graph_id','atom_id'];
  ELSE RETURN false; END IF;
  IF (e-(fields||ARRAY['kind']))<>'{}'::jsonb THEN RETURN false; END IF;
  FOREACH k IN ARRAY fields LOOP
   IF jsonb_typeof(e->k) IS DISTINCT FROM 'string' OR NOT COALESCE((e->>k)~*'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$',false) THEN RETURN false; END IF;
  END LOOP;
 END LOOP;
 RETURN true;
END $$;
REVOKE ALL ON FUNCTION library_support_shape(JSONB) FROM PUBLIC;
CREATE TABLE answer_support_receipts (
 receipt_id UUID PRIMARY KEY,
 graph_id UUID NOT NULL REFERENCES graphs(id) ON DELETE CASCADE,
 owner_user_id UUID NOT NULL,
 support JSONB NOT NULL CHECK(library_support_shape(support)),
 evidence_complete BOOLEAN NOT NULL DEFAULT false,
 created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE answer_support_receipts ENABLE ROW LEVEL SECURITY;
CREATE POLICY account_isolation ON answer_support_receipts USING(graph_id IN(SELECT id FROM graphs WHERE account_id=current_setting('app.account_id')::uuid)) WITH CHECK(graph_id IN(SELECT id FROM graphs WHERE account_id=current_setting('app.account_id')::uuid));
CREATE FUNCTION library_answer_permitted(r_id UUID) RETURNS BOOLEAN LANGUAGE plpgsql STABLE AS $$
DECLARE r answer_support_receipts%ROWTYPE; e JSONB;
BEGIN
 SELECT * INTO r FROM answer_support_receipts WHERE receipt_id=r_id;
 IF NOT FOUND OR NOT r.evidence_complete OR NOT COALESCE(resolve_graph_reachable_by_user(r.graph_id,NULLIF(current_setting('app.user_id',true),'')::uuid),false) THEN RETURN false; END IF;
 FOR e IN SELECT value FROM jsonb_array_elements(r.support) LOOP
  IF e->>'kind'='source_passage' THEN
   IF NOT EXISTS(SELECT 1 FROM semantic_atom_passages p
    JOIN source_versions v ON v.version_id=p.source_version_id AND v.graph_id=p.graph_id
    JOIN source_items s ON s.source_item_id=v.source_item_id AND s.graph_id=v.graph_id
    WHERE p.graph_id::text=e->>'graph_id' AND p.passage_id::text=e->>'passage_id'
      AND p.semantic_atom_id::text=e->>'semantic_atom_id'
      AND v.version_id::text=e->>'source_version_id' AND s.source_item_id::text=e->>'source_item_id'
      AND s.access_state='permitted' AND COALESCE(resolve_graph_reachable_by_user(p.graph_id,NULLIF(current_setting('app.user_id',true),'')::uuid),false)) THEN RETURN false; END IF;
  ELSIF e->>'kind'='legacy_chunk' THEN
   IF NOT EXISTS(SELECT 1 FROM atoms a WHERE a.graph_id::text=e->>'graph_id'
    AND a.atom_id::text=e->>'atom_id' AND library_atom_permitted(a.graph_id,a.atom_id) AND COALESCE(resolve_graph_reachable_by_user(a.graph_id,NULLIF(current_setting('app.user_id',true),'')::uuid),false)) THEN RETURN false; END IF;
  ELSE RETURN false;
  END IF;
 END LOOP;
 RETURN true;
END $$;
REVOKE ALL ON FUNCTION library_answer_permitted(UUID) FROM PUBLIC;
-- Generated titles consume answer text too. Unproven titles fall back to the
-- first explicitly user-authored question, never an old model answer snippet.
CREATE FUNCTION library_conversation_title(c_id UUID) RETURNS TEXT LANGUAGE SQL STABLE AS $$
 SELECT CASE WHEN library_answer_permitted(c.id) THEN substring(c.title FOR 80)
 ELSE COALESCE((SELECT substring(m.text FOR 80) FROM conversation_messages m WHERE m.conversation_id=c.id AND m.role='user' ORDER BY m.seq LIMIT 1),'Conversation') END
 FROM conversations c WHERE c.id=c_id
$$;
REVOKE ALL ON FUNCTION library_conversation_title(UUID) FROM PUBLIC;

DO $$ BEGIN IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname='coralswarm_app') THEN GRANT EXECUTE ON FUNCTION library_answer_permitted(UUID),library_support_shape(JSONB),library_conversation_title(UUID) TO coralswarm_app; END IF; END $$;
