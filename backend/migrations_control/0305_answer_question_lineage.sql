-- Complete server-owned model inputs include exact owned user contributions.
-- Mutable legacy inputs retain only a SHA-256 identity fingerprint. Old UUID-only
-- receipts remain structurally readable but cannot attest to unchanged content.
-- Preserve invoker/RLS semantics and the existing narrow function grants.
-- Only the scoped server engine populates this normalized per-question snapshot.
-- Client timing and old append paths leave it NULL and cannot certify context.
ALTER TABLE conversation_messages ADD COLUMN library_scope JSONB;
ALTER TABLE conversation_messages ADD CONSTRAINT question_library_scope_bound CHECK(library_scope IS NULL OR (role='user' AND jsonb_typeof(library_scope)='object' AND octet_length(library_scope::text)<=16384));
CREATE FUNCTION preserve_question_library_scope() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN
 IF OLD.library_scope IS NOT NULL AND (NEW.library_scope IS DISTINCT FROM OLD.library_scope OR NEW.role IS DISTINCT FROM OLD.role OR NEW.conversation_id IS DISTINCT FROM OLD.conversation_id) THEN
  RAISE EXCEPTION 'server question scope is immutable' USING ERRCODE='check_violation';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER question_library_scope_immutable BEFORE UPDATE OF library_scope,role,conversation_id ON conversation_messages FOR EACH ROW EXECUTE FUNCTION preserve_question_library_scope();
REVOKE ALL ON FUNCTION preserve_question_library_scope() FROM PUBLIC;
DO $$ BEGIN IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname='coralswarm_app') THEN GRANT EXECUTE ON FUNCTION preserve_question_library_scope() TO coralswarm_app; END IF; END $$;
CREATE OR REPLACE FUNCTION library_support_shape(s JSONB) RETURNS BOOLEAN LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE e JSONB; fields TEXT[]; k TEXT;
BEGIN
 IF jsonb_typeof(s) IS DISTINCT FROM 'array' OR jsonb_array_length(s)>200 OR octet_length(s::text)>262144 THEN RETURN false; END IF;
 FOR e IN SELECT value FROM jsonb_array_elements(s) LOOP
  IF jsonb_typeof(e) IS DISTINCT FROM 'object' THEN RETURN false; END IF;
  IF e->>'kind'='source_passage' THEN fields=ARRAY['graph_id','source_item_id','source_version_id','semantic_atom_id','passage_id'];
  ELSIF e->>'kind'='user_question' THEN fields=ARRAY['graph_id','conversation_id','message_id','owner_user_id'];
  ELSIF e->>'kind'='legacy_chunk' THEN fields=ARRAY['graph_id','atom_id'];
  ELSE RETURN false; END IF;
  IF (e-(fields||ARRAY['kind']||CASE WHEN e->>'kind'='legacy_chunk' THEN ARRAY['input_sha256'] ELSE ARRAY[]::text[] END))<>'{}'::jsonb THEN RETURN false; END IF;
  IF e ? 'input_sha256' AND (jsonb_typeof(e->'input_sha256') IS DISTINCT FROM 'string' OR NOT COALESCE((e->>'input_sha256')~'^[0-9a-f]{64}$',false)) THEN RETURN false; END IF;
  FOREACH k IN ARRAY fields LOOP
   IF jsonb_typeof(e->k) IS DISTINCT FROM 'string' OR NOT COALESCE((e->>k)~*'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$',false) THEN RETURN false; END IF;
  END LOOP;
 END LOOP;
 RETURN true;
END $$;

CREATE OR REPLACE FUNCTION library_answer_permitted(r_id UUID) RETURNS BOOLEAN LANGUAGE plpgsql STABLE AS $$
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
  ELSIF e->>'kind'='user_question' THEN
   IF NOT EXISTS(SELECT 1 FROM conversation_messages m JOIN conversations c ON c.id=m.conversation_id
    WHERE m.id::text=e->>'message_id' AND c.id::text=e->>'conversation_id'
     AND c.graph_id::text=e->>'graph_id' AND c.graph_id=r.graph_id AND c.user_id::text=e->>'owner_user_id'
     AND c.user_id=r.owner_user_id AND m.role='user' AND m.library_scope IS NOT NULL
     AND jsonb_typeof(m.scope_graph_ids)='array' AND jsonb_array_length(m.scope_graph_ids)>0
     AND NOT EXISTS(SELECT 1 FROM jsonb_array_elements(m.scope_graph_ids) g
      WHERE NOT COALESCE(resolve_graph_reachable_by_user((g.value#>>'{}')::uuid,NULLIF(current_setting('app.user_id',true),'')::uuid),false))) THEN RETURN false; END IF;
  ELSIF e->>'kind'='legacy_chunk' THEN
   IF NOT EXISTS(SELECT 1 FROM atoms a WHERE a.graph_id::text=e->>'graph_id'
    AND a.atom_id::text=e->>'atom_id'
    AND encode(sha256(convert_to(jsonb_build_array(a.content_inline,a.header_len,a.source_type,a.source_id,a.timestamp,a.created_at)::text,'UTF8')),'hex')=e->>'input_sha256'
    AND library_atom_permitted(a.graph_id,a.atom_id) AND COALESCE(resolve_graph_reachable_by_user(a.graph_id,NULLIF(current_setting('app.user_id',true),'')::uuid),false)) THEN RETURN false; END IF;
  ELSE RETURN false;
  END IF;
 END LOOP;
 RETURN true;
END $$;
