-- Prospective reported capture context revision1. Existing versions stay unknown.
-- No policy change: source_versions already has graph/account/member RLS.
ALTER TABLE source_versions ADD COLUMN capture_context JSONB;
ALTER TABLE source_versions ADD CONSTRAINT capture_context_revision CHECK (
 capture_context IS NULL OR (jsonb_typeof(capture_context)='object' AND capture_context->>'revision'='1' AND octet_length(capture_context::text)<=65536));
CREATE FUNCTION library_capture_context_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='UPDATE' AND (NEW.capture_context IS DISTINCT FROM OLD.capture_context
  OR NEW.source_metadata->'capture_context' IS DISTINCT FROM OLD.source_metadata->'capture_context') THEN
  RAISE EXCEPTION 'captured context is immutable; preserve a new version instead';
 END IF;
 IF NEW.capture_context IS NOT NULL AND (NEW.capture_context-'parent_session_id'-'root_session_id') IS DISTINCT FROM NEW.source_metadata->'capture_context' THEN
  RAISE EXCEPTION 'captured context must match the fingerprinted metadata snapshot';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER library_capture_context_immutable BEFORE INSERT OR UPDATE OF capture_context,source_metadata ON source_versions
 FOR EACH ROW EXECUTE FUNCTION library_capture_context_immutable();

-- First-fixed session identity is enforced at the database seam too: later
-- mutable descriptor reports cannot reparent or move the canonical session.
CREATE FUNCTION library_session_lineage_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF (NEW.graph_id,NEW.session_id,NEW.session_kind,NEW.parent_session_id,NEW.root_session_id)
 IS DISTINCT FROM (OLD.graph_id,OLD.session_id,OLD.session_kind,OLD.parent_session_id,OLD.root_session_id) THEN
  RAISE EXCEPTION 'session identity and lineage are fixed at first capture';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER library_session_lineage_immutable BEFORE UPDATE OF graph_id,session_id,session_kind,parent_session_id,root_session_id ON coding_sessions
 FOR EACH ROW EXECUTE FUNCTION library_session_lineage_immutable();

-- The immutable source mapping supplies session identity. Current mutable session
-- descriptors, raw source-id prefix matching and title/time joins are excluded.
CREATE FUNCTION library_session_versions(g UUID) RETURNS TABLE(
 version_id UUID,source_item_id UUID,provider_namespace TEXT,session_id TEXT,captured_at TIMESTAMPTZ)
 LANGUAGE SQL STABLE AS $$
 SELECT v.version_id,v.source_item_id,v.provider_namespace,
 COALESCE(NULLIF(v.capture_context->>'session_id',''),NULLIF(v.source_metadata->>'session_id',''),
  (SELECT CASE WHEN count(DISTINCT cs.session_id)=1 THEN min(cs.session_id) END
   FROM source_version_chunks c JOIN atoms a ON a.graph_id=c.graph_id AND a.atom_id=c.atom_id JOIN coding_sessions cs ON cs.graph_id=a.graph_id AND a.source_id LIKE ('cc:'||replace(replace(replace(cs.session_id,'\','\\'),'%','\%'),'_','\_')||':%')
   WHERE c.graph_id=v.graph_id AND c.source_version_id=v.version_id AND a.source_type IN('coding_session','agent_session','work_session','meeting_session','mcp') AND library_atom_permitted(g,a.atom_id))),v.captured_at
 FROM source_versions v JOIN source_items s ON s.graph_id=v.graph_id AND s.source_item_id=v.source_item_id
 WHERE v.graph_id=g AND s.access_state='permitted'
$$;
CREATE INDEX library_capture_session_lookup ON source_versions(graph_id,provider_namespace,(COALESCE(capture_context->>'session_id',source_metadata->>'session_id')));
CREATE FUNCTION library_session_supported(g UUID,ns TEXT,s_id TEXT) RETURNS BOOLEAN LANGUAGE SQL STABLE AS $$
 -- Canonical version attribution also rejects ambiguous legacy prefixes and
 -- multiple conflicting chunk-derived session IDs within one version.
 SELECT s_id IS NOT NULL AND EXISTS (
  SELECT 1 FROM library_session_versions(g) sv
  WHERE sv.provider_namespace=ns AND sv.session_id=s_id)
$$;
CREATE FUNCTION library_capture_context(g UUID,v_id UUID) RETURNS JSONB LANGUAGE SQL STABLE AS $$
 WITH admitted AS MATERIALIZED (
  SELECT v.* FROM source_versions v JOIN source_items s ON s.graph_id=v.graph_id AND s.source_item_id=v.source_item_id
  WHERE v.graph_id=g AND v.version_id=v_id AND s.access_state='permitted'
 ), recorded AS (
  SELECT v.*,sv.session_id,
   CASE WHEN v.capture_context IS NOT NULL THEN v.capture_context
   ELSE jsonb_build_object('revision',NULL,'session_id',sv.session_id,
    'session_kind',COALESCE(v.source_metadata->>'session_kind',CASE v.source_type WHEN 'coding_session' THEN 'coding' WHEN 'agent_session' THEN 'agent' WHEN 'work_session' THEN 'work' WHEN 'meeting_session' THEN 'meeting' END),
    'session_title',v.source_metadata->>'session_title','project',v.source_metadata->>'project',
    'project_key',v.source_metadata->>'project_key','branch',v.source_metadata->>'branch') END ctx
  FROM admitted v CROSS JOIN LATERAL (SELECT COALESCE(NULLIF(v.capture_context->>'session_id',''),NULLIF(v.source_metadata->>'session_id',''),
   (SELECT CASE WHEN count(DISTINCT cs.session_id)=1 THEN min(cs.session_id) END FROM source_version_chunks c
    JOIN atoms a ON a.graph_id=c.graph_id AND a.atom_id=c.atom_id JOIN coding_sessions cs ON cs.graph_id=a.graph_id AND a.source_id LIKE ('cc:'||replace(replace(replace(cs.session_id,'\','\\'),'%','\%'),'_','\_')||':%')
    WHERE c.graph_id=g AND c.source_version_id=v.version_id AND a.source_type IN('coding_session','agent_session','work_session','meeting_session','mcp') AND library_atom_permitted(g,a.atom_id))) session_id) sv
 ), proved AS (
  SELECT r.*,
   library_session_supported(g,r.provider_namespace,r.ctx->>'parent_session_id') parent_readable,
   (r.ctx->>'root_session_id'=r.session_id OR library_session_supported(g,r.provider_namespace,r.ctx->>'root_session_id')) root_readable
  FROM recorded r
 ) SELECT jsonb_build_object(
  'revision',ctx->'revision','session_id',session_id,'session_kind',ctx->>'session_kind',
  'session_title',substring(ctx->>'session_title' FOR 512),'project',substring(ctx->>'project' FOR 512),
  'project_key',substring(ctx->>'project_key' FOR 1024),'repo_path',substring(ctx->>'repo_path' FOR 1024),
  'remote_sanitized',substring(ctx->>'remote_sanitized' FOR 1024),'branch',substring(ctx->>'branch' FOR 512),
  'hostname',substring(ctx->>'hostname' FOR 512),'harness_version',substring(ctx->>'harness_version' FOR 512),
  'model',substring(ctx->>'model' FOR 512),'agent_name',substring(ctx->>'agent_name' FOR 512),
  'platform',substring(ctx->>'platform' FOR 512),'platform_detected',substring(ctx->>'platform_detected' FOR 512),
  'task',substring(ctx->>'task' FOR 2000),'subagent_name',substring(ctx->>'subagent_name' FOR 512),
  'parent_session_id',CASE WHEN parent_readable THEN ctx->>'parent_session_id' END,
  'root_session_id',CASE WHEN root_readable THEN ctx->>'root_session_id' END,
  'lineage_status',CASE WHEN capture_context IS NULL OR session_id IS NULL THEN 'unknown'
   WHEN (ctx->>'parent_session_id' IS NULL OR parent_readable) AND root_readable THEN 'captured' ELSE 'partial' END)
 FROM proved
$$;

-- One permitted reported snapshot per existing session. All filters, counts and
-- keysets consume this projection, never coding_sessions descriptor columns.
CREATE FUNCTION library_sessions(g UUID) RETURNS TABLE(
 id UUID,graph_id UUID,provider_namespace TEXT,session_id TEXT,session_kind TEXT,project_key TEXT,title TEXT,
 repo_path TEXT,remote_sanitized TEXT,hostname TEXT,harness_version TEXT,model TEXT,branch TEXT,
 started_at TIMESTAMPTZ,last_activity_at TIMESTAMPTZ,agent_name TEXT,platform TEXT,
 platform_detected TEXT,task TEXT,parent_session_id TEXT,root_session_id TEXT,subagent_name TEXT)
 LANGUAGE SQL STABLE AS $$
 WITH recorded AS MATERIALIZED (
  SELECT sv.* FROM library_session_versions(g) sv WHERE sv.session_id IS NOT NULL
 ), chosen AS (
  SELECT DISTINCT ON(session_id) * FROM recorded ORDER BY session_id,captured_at DESC,version_id DESC
 ), masked AS MATERIALIZED (SELECT c.*,library_capture_context(g,c.version_id) ctx FROM chosen c),
 clocks AS (SELECT session_id,min(captured_at) first_capture,max(captured_at) last_capture FROM recorded GROUP BY session_id HAVING count(DISTINCT provider_namespace)=1)
 SELECT cs.id,cs.graph_id,c.provider_namespace,cs.session_id,COALESCE(c.ctx->>'session_kind','unknown'),
 c.ctx->>'project_key',c.ctx->>'session_title',c.ctx->>'repo_path',c.ctx->>'remote_sanitized',c.ctx->>'hostname',
 c.ctx->>'harness_version',c.ctx->>'model',c.ctx->>'branch',cl.first_capture,cl.last_capture,
 c.ctx->>'agent_name',c.ctx->>'platform',c.ctx->>'platform_detected',c.ctx->>'task',
 c.ctx->>'parent_session_id',c.ctx->>'root_session_id',c.ctx->>'subagent_name'
 FROM coding_sessions cs JOIN masked c ON c.session_id=cs.session_id JOIN clocks cl ON cl.session_id=cs.session_id
 WHERE cs.graph_id=g
$$;
REVOKE ALL ON FUNCTION library_capture_context_immutable(),library_session_lineage_immutable(),library_session_versions(UUID),library_session_supported(UUID,TEXT,TEXT),library_capture_context(UUID,UUID),library_sessions(UUID) FROM PUBLIC;
DO $$ BEGIN
 IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname='coralswarm_app') THEN
  GRANT EXECUTE ON FUNCTION library_session_versions(UUID),library_session_supported(UUID,TEXT,TEXT),library_capture_context(UUID,UUID),library_sessions(UUID) TO coralswarm_app;
 END IF;
 IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname='coralswarm_tenant_app') THEN
  GRANT EXECUTE ON FUNCTION library_session_versions(UUID),library_session_supported(UUID,TEXT,TEXT),library_capture_context(UUID,UUID),library_sessions(UUID) TO coralswarm_tenant_app;
 END IF;
END $$;
