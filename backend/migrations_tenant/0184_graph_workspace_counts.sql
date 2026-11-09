-- PAIRED WITH: control 0353 (graph_workspace_counts).
-- Workspace card counts are precomputed per graph instead of per request.
-- The bodies match the control twin except the RLS policy and
-- graph_workspace_counts_shareable(), which read each plane's own
-- visibility rules.
--
-- GET /v1/graphs/{id}/stats took 0.9-4.2 s in production: every call
-- materialised library_permitted_atoms and hashed the whole node_atoms table
-- (7.6M rows on the org tenant) to find admitted reefs. The route now reads one
-- row from graph_workspace_counts, refreshes it in the background when it is
-- older than 30 seconds, and computes it in the request only when there is no
-- usable row.
--
-- 1. graph_workspace_counts_compute(g): the counts, with the same admission as
--    library_node_permitted and library_permitted_atoms. A reef counts when it
--    has a legacy link to a permitted atom or a semantic link to a served
--    semantic atom. LANGUAGE sql without SECURITY DEFINER, so the caller's
--    RLS applies.
--
-- 2. graph_workspace_counts_shareable(g): whether the caller sees every row
--    of g that the counts read. Only then is a stored row, computed by
--    another reader, equal to what this caller would compute. On this plane
--    the one per-user cut inside a visible graph is the team seat window
--    (atoms before history_floor), so a windowed member, or a caller who
--    cannot see g, always gets a live count in their own scope and never
--    reads or writes the stored row.
--
-- 3. Permission-reducing writes invalidate synchronously. An access_state
--    change, an atom becoming retracted/derived/source-bound, a chunk mapping
--    an atom to a source that is not permitted, and a removed chunk of a
--    source-bound atom each record their transaction id in invalidated_xid. A
--    row is only served when that transaction is visible in computed_snapshot
--    (the snapshot its counts were computed from), so a refresh that raced a
--    revoke can never serve the pre-revoke numbers. Other writes
--    (captures, deletes, re-extraction) only make the row stale. It is then
--    served at most once more, and that read refreshes it in the background.
--    The trigger function is SECURITY DEFINER so a writer's RLS scope cannot
--    hide the row it must invalidate.
CREATE TABLE graph_workspace_counts (
 graph_id UUID PRIMARY KEY REFERENCES graphs(id) ON DELETE CASCADE,
 reef_counts JSONB,
 atom_count BIGINT,
 source_count BIGINT,
 computed_at TIMESTAMPTZ,
 computed_snapshot PG_SNAPSHOT,
 invalidated_xid XID8,
 refresh_claimed_at TIMESTAMPTZ,
 CHECK ((reef_counts IS NULL) = (computed_snapshot IS NULL)
    AND (atom_count IS NULL) = (computed_snapshot IS NULL)
    AND (source_count IS NULL) = (computed_snapshot IS NULL)
    AND (computed_at IS NULL) = (computed_snapshot IS NULL))
);

CREATE FUNCTION graph_workspace_counts_compute(g UUID)
RETURNS TABLE(reef_counts JSONB, atom_count BIGINT, source_count BIGINT)
LANGUAGE sql STABLE AS $$
 WITH permitted_atoms AS MATERIALIZED (
  SELECT atom_id FROM library_permitted_atoms(g,NULL::uuid[])
 ), semantic_nodes AS MATERIALIZED (
  SELECT DISTINCT l.node_id FROM node_semantic_atoms l
  JOIN served_semantic_atoms a ON a.semantic_atom_id=l.semantic_atom_id AND a.graph_id=l.graph_id
  WHERE l.graph_id=g
 ), reefs AS (
  SELECT n.type,count(*) reef_count FROM nodes n
  WHERE n.graph_id=g AND n.type IS NOT NULL
    AND (n.node_id IN (SELECT node_id FROM semantic_nodes)
      OR EXISTS (SELECT 1 FROM node_atoms l WHERE l.node_id=n.node_id
                 AND l.atom_id IN (SELECT atom_id FROM permitted_atoms)))
  GROUP BY n.type
 ), atom_sources AS (
  SELECT split_part(a.source_id,':chunk:',1) source_key,count(*) atoms
  FROM permitted_atoms p JOIN atoms a ON a.atom_id=p.atom_id WHERE a.graph_id=g
  GROUP BY 1
 )
 SELECT COALESCE((SELECT jsonb_object_agg(type,reef_count) FROM reefs),'{}'::jsonb),
        COALESCE((SELECT sum(atoms) FROM atom_sources),0)::bigint,
        (SELECT count(*) FROM atom_sources)
$$;

CREATE FUNCTION graph_workspace_counts_shareable(g UUID) RETURNS BOOLEAN
LANGUAGE sql STABLE AS $$
 SELECT EXISTS (SELECT 1 FROM tenant_rls_visible_graph_windows() w
                WHERE w.graph_id=g AND w.history_floor IS NULL)
$$;

CREATE FUNCTION graph_workspace_counts_invalidate() RETURNS TRIGGER
LANGUAGE plpgsql SECURITY DEFINER SET search_path = public, pg_temp AS $$
DECLARE g UUID;
BEGIN
 IF TG_TABLE_NAME = 'source_version_chunks' THEN
  IF TG_OP = 'INSERT' THEN
   g := NEW.graph_id;
   IF NOT EXISTS (SELECT 1 FROM source_versions v JOIN source_items s
                    ON s.graph_id=v.graph_id AND s.source_item_id=v.source_item_id
                  WHERE v.graph_id=NEW.graph_id AND v.version_id=NEW.source_version_id
                    AND s.access_state<>'permitted') THEN
    RETURN NULL;
   END IF;
  ELSE
   g := OLD.graph_id;
   IF NOT EXISTS (SELECT 1 FROM atoms a WHERE a.atom_id=OLD.atom_id AND a.source_evidence_bound) THEN
    RETURN NULL;
   END IF;
  END IF;
 ELSE
  g := NEW.graph_id;
 END IF;
 INSERT INTO graph_workspace_counts(graph_id,invalidated_xid) VALUES (g,pg_current_xact_id())
 ON CONFLICT (graph_id) DO UPDATE
  SET invalidated_xid=GREATEST(graph_workspace_counts.invalidated_xid,EXCLUDED.invalidated_xid);
 RETURN NULL;
END $$;

CREATE TRIGGER graph_workspace_counts_access_changed
 AFTER UPDATE OF access_state ON source_items FOR EACH ROW
 WHEN (OLD.access_state IS DISTINCT FROM NEW.access_state)
 EXECUTE FUNCTION graph_workspace_counts_invalidate();
CREATE TRIGGER graph_workspace_counts_atom_withheld
 AFTER UPDATE OF evidence_retracted,derived_copy_bound,source_evidence_bound ON atoms FOR EACH ROW
 WHEN (OLD.evidence_retracted IS DISTINCT FROM NEW.evidence_retracted
    OR OLD.derived_copy_bound IS DISTINCT FROM NEW.derived_copy_bound
    OR OLD.source_evidence_bound IS DISTINCT FROM NEW.source_evidence_bound)
 EXECUTE FUNCTION graph_workspace_counts_invalidate();
CREATE TRIGGER graph_workspace_counts_chunk_changed
 AFTER INSERT OR DELETE ON source_version_chunks FOR EACH ROW
 EXECUTE FUNCTION graph_workspace_counts_invalidate();

ALTER TABLE graph_workspace_counts ENABLE ROW LEVEL SECURITY;
DO $$ DECLARE r TEXT;BEGIN FOREACH r IN ARRAY ARRAY['coralswarm_app','coralswarm_tenant_app'] LOOP IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname=r) THEN EXECUTE format('GRANT EXECUTE ON FUNCTION graph_workspace_counts_compute(UUID),graph_workspace_counts_shareable(UUID) TO %I',r);END IF;END LOOP;END $$;
DO $$ BEGIN
 IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'coralswarm_app') THEN
  GRANT SELECT,INSERT,UPDATE ON graph_workspace_counts TO coralswarm_app;
 END IF;
 IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'coralswarm_tenant_app') THEN
  GRANT SELECT,INSERT,UPDATE ON graph_workspace_counts TO coralswarm_tenant_app;
 END IF;
END $$;

CREATE POLICY member_ocean_isolation ON graph_workspace_counts
  USING (graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids()))
  WITH CHECK (graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids()));
