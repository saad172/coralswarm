-- PAIRED WITH: tenant 0185 (library_evidence).
-- Additive Library read-model storage; the request path remains live.
-- The definition stays in Rust (library_read/evidence.rs); these tables do
-- not redefine evidence semantics. All writers only append invalidation keys.
-- Ordered inverse links let the resolver stop at a node cursor without
-- sorting an entire hub. A permission grant appends atom/outcome keys
-- because a denied item may have no cached item row.
CREATE TABLE library_node_evidence (
 graph_id UUID NOT NULL REFERENCES graphs(id) ON DELETE CASCADE ON UPDATE CASCADE,
 node_id UUID NOT NULL,
 source_count BIGINT NOT NULL CHECK(source_count>=0),
 unknown_independence_record_count BIGINT NOT NULL CHECK(unknown_independence_record_count>=0),
 atom_count_retained BIGINT NOT NULL CHECK(atom_count_retained>=0),
 atom_count_served BIGINT NOT NULL CHECK(atom_count_served>=0),
 unmapped_legacy_count BIGINT NOT NULL CHECK(unmapped_legacy_count>=0),
 admitted_unfiltered BOOLEAN NOT NULL,
 has_source_link BOOLEAN NOT NULL,
 title256 TEXT,
 title_visible BOOLEAN NOT NULL,
 project_mention BOOLEAN NOT NULL,
 work_eligible BOOLEAN NOT NULL,
 stats_admitted BOOLEAN NOT NULL,
 computed_version INTEGER NOT NULL CHECK(computed_version>0),
 computed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 PRIMARY KEY(graph_id,node_id),
 FOREIGN KEY(node_id,graph_id) REFERENCES nodes(node_id,graph_id) ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE INDEX library_node_evidence_retained_rank ON library_node_evidence(graph_id,source_count DESC,atom_count_retained DESC,node_id DESC) WHERE admitted_unfiltered;
CREATE INDEX library_node_evidence_served_rank ON library_node_evidence(graph_id,source_count DESC,atom_count_served DESC,node_id DESC) WHERE admitted_unfiltered;
CREATE INDEX library_node_evidence_work ON library_node_evidence(graph_id,source_count DESC,atom_count_retained DESC,node_id DESC) WHERE admitted_unfiltered AND work_eligible;

CREATE TABLE library_node_item_evidence (
 graph_id UUID NOT NULL REFERENCES graphs(id) ON DELETE CASCADE ON UPDATE CASCADE,
 node_id UUID NOT NULL,
 -- Deliberately no source_items FK: deleting an item must leave its old
 -- node links available to dirty resolution until the refresher removes them.
 source_item_id UUID NOT NULL,
 semantic_link BOOLEAN NOT NULL,
 legacy_link BOOLEAN NOT NULL,
 legacy_independent_version BOOLEAN NOT NULL,
 legacy_synthetic BOOLEAN NOT NULL,
 atom_count_retained BIGINT NOT NULL CHECK(atom_count_retained>=0),
 atom_count_served BIGINT NOT NULL CHECK(atom_count_served>=0),
 project_mention BOOLEAN NOT NULL,
 PRIMARY KEY(graph_id,node_id,source_item_id),
 FOREIGN KEY(node_id,graph_id) REFERENCES nodes(node_id,graph_id) ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE INDEX library_node_item_evidence_item ON library_node_item_evidence(graph_id,source_item_id,node_id);

CREATE TABLE library_evidence_changes (
 id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 graph_id UUID NOT NULL REFERENCES graphs(id) ON DELETE CASCADE ON UPDATE CASCADE,
 -- No object FKs: old keys must survive object/link deletion and movement.
 node_id UUID,
 source_item_id UUID,
 atom_id UUID,
 semantic_atom_id UUID,
 created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 -- Worker-only progress. Overlay resolution explicitly ignores this cursor.
 resolved_after_node_id UUID,
 CHECK(num_nonnulls(node_id,source_item_id,atom_id,semantic_atom_id)=1)
);
CREATE INDEX library_evidence_changes_graph ON library_evidence_changes(graph_id,id);

CREATE TABLE library_evidence_state (
 graph_id UUID PRIMARY KEY REFERENCES graphs(id) ON DELETE CASCADE ON UPDATE CASCADE,
 ready_version INTEGER CHECK(ready_version>0),
 backfilled_at TIMESTAMPTZ,
 computed_account_id UUID,
 computed_context JSONB,
 backfill_version INTEGER CHECK(backfill_version>0),
 backfill_account_id UUID,
 backfill_context JSONB,
 backfill_after_node_id UUID,
 backfill_started_at TIMESTAMPTZ,
 last_refreshed_at TIMESTAMPTZ NOT NULL DEFAULT '-infinity',
 CHECK(ready_version IS NULL OR (backfilled_at IS NOT NULL AND computed_account_id IS NOT NULL AND computed_context IS NOT NULL))
);

-- Capture every column on all three events, rather than a fragile UPDATE OF
-- list: representation/source identity/generation changes affect the shared
-- definition too. UPDATE includes both images; keys are DISTINCT per statement
-- but never conflict with a concurrent writer's keys (append-only, no upsert).
CREATE FUNCTION library_evidence_capture() RETURNS TRIGGER
LANGUAGE plpgsql SECURITY DEFINER SET search_path=public,pg_temp AS $$
DECLARE rows_sql TEXT; identities_sql TEXT;
BEGIN
 IF TG_OP='INSERT' THEN rows_sql:='SELECT * FROM library_new';
 ELSIF TG_OP='DELETE' THEN rows_sql:='SELECT * FROM library_old';
 ELSE rows_sql:='SELECT * FROM library_old UNION ALL SELECT * FROM library_new'; END IF;

 IF TG_TABLE_NAME='node_atoms' THEN
  EXECUTE format('INSERT INTO library_evidence_changes(graph_id,node_id)
   SELECT DISTINCT n.graph_id,t.node_id FROM (%s) t JOIN nodes n ON n.node_id=t.node_id
   JOIN graphs g ON g.id=n.graph_id',rows_sql);
 ELSIF TG_TABLE_NAME IN ('nodes','node_semantic_atoms','reef_identity_corrections') THEN
  EXECUTE format('INSERT INTO library_evidence_changes(graph_id,node_id)
   SELECT DISTINCT t.graph_id,t.node_id FROM (%s) t JOIN graphs g ON g.id=t.graph_id',rows_sql);
 ELSIF TG_TABLE_NAME='atoms' THEN
  EXECUTE format('INSERT INTO library_evidence_changes(graph_id,atom_id)
   SELECT DISTINCT t.graph_id,t.atom_id FROM (%s) t JOIN graphs g ON g.id=t.graph_id',rows_sql);
 ELSIF TG_TABLE_NAME='source_version_chunks' THEN
  EXECUTE format('INSERT INTO library_evidence_changes(graph_id,atom_id)
   SELECT DISTINCT t.graph_id,t.atom_id FROM (%s) t JOIN graphs g ON g.id=t.graph_id',rows_sql);
  EXECUTE format('INSERT INTO library_evidence_changes(graph_id,source_item_id)
   SELECT DISTINCT t.graph_id,v.source_item_id FROM (%s) t JOIN source_versions v
   ON v.graph_id=t.graph_id AND v.version_id=t.source_version_id JOIN graphs g ON g.id=t.graph_id',rows_sql);
 ELSIF TG_TABLE_NAME='semantic_atoms' THEN
  EXECUTE format('INSERT INTO library_evidence_changes(graph_id,semantic_atom_id)
   SELECT DISTINCT t.graph_id,t.semantic_atom_id FROM (%s) t JOIN graphs g ON g.id=t.graph_id',rows_sql);
  EXECUTE format('INSERT INTO library_evidence_changes(graph_id,source_item_id)
   SELECT DISTINCT t.graph_id,v.source_item_id FROM (%s) t JOIN source_versions v
   ON v.graph_id=t.graph_id AND v.version_id=t.source_version_id JOIN graphs g ON g.id=t.graph_id',rows_sql);
 ELSIF TG_TABLE_NAME IN ('source_versions','source_items') THEN
  EXECUTE format('INSERT INTO library_evidence_changes(graph_id,source_item_id)
   SELECT DISTINCT t.graph_id,t.source_item_id FROM (%s) t JOIN graphs g ON g.id=t.graph_id',rows_sql);
  IF TG_TABLE_NAME='source_versions' AND TG_OP='UPDATE' THEN
   -- Reparenting a version can grant previously denied evidence without a
   -- node-link write. Append object keys, never walk their node fan-out.
   identities_sql:='SELECT n.graph_id,n.version_id FROM library_new n JOIN library_old o USING(version_id)
    WHERE (n.graph_id,n.source_item_id) IS DISTINCT FROM (o.graph_id,o.source_item_id)';
   EXECUTE format('INSERT INTO library_evidence_changes(graph_id,atom_id)
    SELECT DISTINCT m.graph_id,m.atom_id FROM (%s) t JOIN source_version_chunks m
    ON m.source_version_id=t.version_id AND m.graph_id=t.graph_id JOIN graphs g ON g.id=m.graph_id',identities_sql);
   EXECUTE format('INSERT INTO library_evidence_changes(graph_id,semantic_atom_id)
    SELECT DISTINCT a.graph_id,a.semantic_atom_id FROM (%s) t JOIN semantic_atoms a
    ON a.source_version_id=t.version_id AND a.graph_id=t.graph_id JOIN graphs g ON g.id=a.graph_id',identities_sql);
  END IF;
  IF TG_TABLE_NAME='source_items' THEN
   IF TG_OP='UPDATE' THEN
    -- A denied item has no permitted stored item grain. On grant, capture
    -- its existing objects once; ordered object->node resolution remains
    -- bounded even for a hub. Ordinary pointer/metadata updates do not scan.
    identities_sql:='SELECT n.graph_id,n.source_item_id FROM library_new n JOIN library_old o USING(source_item_id)
     WHERE o.access_state<>''permitted'' AND n.access_state=''permitted''';
    EXECUTE format('INSERT INTO library_evidence_changes(graph_id,atom_id)
     SELECT DISTINCT m.graph_id,m.atom_id FROM (%s) t JOIN source_versions v
     ON v.source_item_id=t.source_item_id AND v.graph_id=t.graph_id JOIN source_version_chunks m
     ON m.source_version_id=v.version_id AND m.graph_id=v.graph_id JOIN graphs g ON g.id=m.graph_id',identities_sql);
    EXECUTE format('INSERT INTO library_evidence_changes(graph_id,semantic_atom_id)
     SELECT DISTINCT a.graph_id,a.semantic_atom_id FROM (%s) t JOIN source_versions v
     ON v.source_item_id=t.source_item_id AND v.graph_id=t.graph_id JOIN semantic_atoms a
     ON a.source_version_id=v.version_id AND a.graph_id=v.graph_id JOIN graphs g ON g.id=a.graph_id',identities_sql);
   END IF;
   -- A denied identity also governs unbound positional chunk atoms, even
   -- without a chunk mapping. INSERT/DELETE and old/new identity changes
   -- must reach those nodes. Ordinary current-pointer updates do not scan
   -- atoms: only a changed denied identity can change that permission gate.
   IF TG_OP='UPDATE' THEN
    identities_sql:='SELECT o.graph_id,o.source_type,o.source_id,o.access_state FROM library_old o
     FULL JOIN library_new n ON n.source_item_id=o.source_item_id
     WHERE (o.graph_id,o.source_type,o.source_id,o.access_state) IS DISTINCT FROM (n.graph_id,n.source_type,n.source_id,n.access_state)
     UNION ALL SELECT n.graph_id,n.source_type,n.source_id,n.access_state FROM library_new n
     FULL JOIN library_old o ON n.source_item_id=o.source_item_id
     WHERE (o.graph_id,o.source_type,o.source_id,o.access_state) IS DISTINCT FROM (n.graph_id,n.source_type,n.source_id,n.access_state)';
   ELSE identities_sql:=rows_sql; END IF;
   EXECUTE format('INSERT INTO library_evidence_changes(graph_id,atom_id)
    SELECT DISTINCT a.graph_id,a.atom_id FROM (%s) t JOIN atoms a ON a.graph_id=t.graph_id
     AND a.source_type=t.source_type AND NOT a.source_evidence_bound
     AND t.source_id=CASE WHEN right(rtrim(a.source_id,''0123456789''),7)='':chunk:'' AND rtrim(a.source_id,''0123456789'')<>a.source_id
      THEN left(rtrim(a.source_id,''0123456789''),-7) ELSE a.source_id END
    JOIN graphs g ON g.id=a.graph_id WHERE t.access_state<>''permitted''',identities_sql);
  END IF;
 END IF;
 RETURN NULL;
END $$;
REVOKE ALL ON FUNCTION library_evidence_capture() FROM PUBLIC;

CREATE TRIGGER library_evidence_node_atoms_insert AFTER INSERT ON node_atoms
 REFERENCING NEW TABLE AS library_new FOR EACH STATEMENT EXECUTE FUNCTION library_evidence_capture();
CREATE TRIGGER library_evidence_node_atoms_update AFTER UPDATE ON node_atoms
 REFERENCING OLD TABLE AS library_old NEW TABLE AS library_new FOR EACH STATEMENT EXECUTE FUNCTION library_evidence_capture();
CREATE TRIGGER library_evidence_node_atoms_delete AFTER DELETE ON node_atoms
 REFERENCING OLD TABLE AS library_old FOR EACH STATEMENT EXECUTE FUNCTION library_evidence_capture();
CREATE TRIGGER library_evidence_node_semantic_atoms_insert AFTER INSERT ON node_semantic_atoms
 REFERENCING NEW TABLE AS library_new FOR EACH STATEMENT EXECUTE FUNCTION library_evidence_capture();
CREATE TRIGGER library_evidence_node_semantic_atoms_update AFTER UPDATE ON node_semantic_atoms
 REFERENCING OLD TABLE AS library_old NEW TABLE AS library_new FOR EACH STATEMENT EXECUTE FUNCTION library_evidence_capture();
CREATE TRIGGER library_evidence_node_semantic_atoms_delete AFTER DELETE ON node_semantic_atoms
 REFERENCING OLD TABLE AS library_old FOR EACH STATEMENT EXECUTE FUNCTION library_evidence_capture();
CREATE TRIGGER library_evidence_nodes_insert AFTER INSERT ON nodes
 REFERENCING NEW TABLE AS library_new FOR EACH STATEMENT EXECUTE FUNCTION library_evidence_capture();
CREATE TRIGGER library_evidence_nodes_update AFTER UPDATE ON nodes
 REFERENCING OLD TABLE AS library_old NEW TABLE AS library_new FOR EACH STATEMENT EXECUTE FUNCTION library_evidence_capture();
CREATE TRIGGER library_evidence_nodes_delete AFTER DELETE ON nodes
 REFERENCING OLD TABLE AS library_old FOR EACH STATEMENT EXECUTE FUNCTION library_evidence_capture();
CREATE TRIGGER library_evidence_reef_identity_corrections_insert AFTER INSERT ON reef_identity_corrections
 REFERENCING NEW TABLE AS library_new FOR EACH STATEMENT EXECUTE FUNCTION library_evidence_capture();
CREATE TRIGGER library_evidence_reef_identity_corrections_update AFTER UPDATE ON reef_identity_corrections
 REFERENCING OLD TABLE AS library_old NEW TABLE AS library_new FOR EACH STATEMENT EXECUTE FUNCTION library_evidence_capture();
CREATE TRIGGER library_evidence_reef_identity_corrections_delete AFTER DELETE ON reef_identity_corrections
 REFERENCING OLD TABLE AS library_old FOR EACH STATEMENT EXECUTE FUNCTION library_evidence_capture();
CREATE TRIGGER library_evidence_atoms_insert AFTER INSERT ON atoms
 REFERENCING NEW TABLE AS library_new FOR EACH STATEMENT EXECUTE FUNCTION library_evidence_capture();
CREATE TRIGGER library_evidence_atoms_update AFTER UPDATE ON atoms
 REFERENCING OLD TABLE AS library_old NEW TABLE AS library_new FOR EACH STATEMENT EXECUTE FUNCTION library_evidence_capture();
CREATE TRIGGER library_evidence_atoms_delete AFTER DELETE ON atoms
 REFERENCING OLD TABLE AS library_old FOR EACH STATEMENT EXECUTE FUNCTION library_evidence_capture();
CREATE TRIGGER library_evidence_source_version_chunks_insert AFTER INSERT ON source_version_chunks
 REFERENCING NEW TABLE AS library_new FOR EACH STATEMENT EXECUTE FUNCTION library_evidence_capture();
CREATE TRIGGER library_evidence_source_version_chunks_update AFTER UPDATE ON source_version_chunks
 REFERENCING OLD TABLE AS library_old NEW TABLE AS library_new FOR EACH STATEMENT EXECUTE FUNCTION library_evidence_capture();
CREATE TRIGGER library_evidence_source_version_chunks_delete AFTER DELETE ON source_version_chunks
 REFERENCING OLD TABLE AS library_old FOR EACH STATEMENT EXECUTE FUNCTION library_evidence_capture();
CREATE TRIGGER library_evidence_semantic_atoms_insert AFTER INSERT ON semantic_atoms
 REFERENCING NEW TABLE AS library_new FOR EACH STATEMENT EXECUTE FUNCTION library_evidence_capture();
CREATE TRIGGER library_evidence_semantic_atoms_update AFTER UPDATE ON semantic_atoms
 REFERENCING OLD TABLE AS library_old NEW TABLE AS library_new FOR EACH STATEMENT EXECUTE FUNCTION library_evidence_capture();
CREATE TRIGGER library_evidence_semantic_atoms_delete AFTER DELETE ON semantic_atoms
 REFERENCING OLD TABLE AS library_old FOR EACH STATEMENT EXECUTE FUNCTION library_evidence_capture();
CREATE TRIGGER library_evidence_source_versions_insert AFTER INSERT ON source_versions
 REFERENCING NEW TABLE AS library_new FOR EACH STATEMENT EXECUTE FUNCTION library_evidence_capture();
CREATE TRIGGER library_evidence_source_versions_update AFTER UPDATE ON source_versions
 REFERENCING OLD TABLE AS library_old NEW TABLE AS library_new FOR EACH STATEMENT EXECUTE FUNCTION library_evidence_capture();
CREATE TRIGGER library_evidence_source_versions_delete AFTER DELETE ON source_versions
 REFERENCING OLD TABLE AS library_old FOR EACH STATEMENT EXECUTE FUNCTION library_evidence_capture();
CREATE TRIGGER library_evidence_source_items_insert AFTER INSERT ON source_items
 REFERENCING NEW TABLE AS library_new FOR EACH STATEMENT EXECUTE FUNCTION library_evidence_capture();
CREATE TRIGGER library_evidence_source_items_update AFTER UPDATE ON source_items
 REFERENCING OLD TABLE AS library_old NEW TABLE AS library_new FOR EACH STATEMENT EXECUTE FUNCTION library_evidence_capture();
CREATE TRIGGER library_evidence_source_items_delete AFTER DELETE ON source_items
 REFERENCING OLD TABLE AS library_old FOR EACH STATEMENT EXECUTE FUNCTION library_evidence_capture();

-- Resolve a SINGLE durable log key, in bounded node order. Each arm takes
-- its own first N before UNION, so high fan-out cannot materialize an
-- unbounded result set; the final first N is exactly the union's first N.
-- Deleted keys survive in the log/stored item grain. New links always append
-- a separate node or atom key, including links preceding a worker cursor.
-- Refresh passes resolved_after_node_id; overlay passes NULL deliberately.
CREATE FUNCTION library_evidence_change_nodes(g UUID,change_id BIGINT,after_node UUID DEFAULT NULL,node_limit INTEGER DEFAULT 64)
RETURNS TABLE(node_id UUID) LANGUAGE sql STABLE SET search_path=public,pg_temp AS $$
 WITH c AS MATERIALIZED (SELECT * FROM library_evidence_changes WHERE graph_id=g AND id=change_id),
 keys AS (
  (SELECT c.node_id FROM c WHERE c.node_id IS NOT NULL AND (after_node IS NULL OR c.node_id>after_node))
  UNION
  (SELECT l.node_id FROM c CROSS JOIN LATERAL (
   SELECT a.node_id FROM node_atoms a JOIN nodes n ON n.node_id=a.node_id AND n.graph_id=c.graph_id AND n.node_id>=COALESCE(after_node,'00000000-0000-0000-0000-000000000000'::uuid) AND (after_node IS NULL OR n.node_id>after_node)
   WHERE a.atom_id=c.atom_id AND a.node_id>=COALESCE(after_node,'00000000-0000-0000-0000-000000000000'::uuid) AND (after_node IS NULL OR a.node_id>after_node)
   ORDER BY a.node_id LIMIT LEAST(GREATEST(node_limit,0),4096)) l)
  UNION
  (SELECT l.node_id FROM c CROSS JOIN LATERAL (
   SELECT a.node_id FROM node_semantic_atoms a WHERE a.semantic_atom_id=c.semantic_atom_id AND a.graph_id=c.graph_id
   AND a.node_id>=COALESCE(after_node,'00000000-0000-0000-0000-000000000000'::uuid) AND (after_node IS NULL OR a.node_id>after_node) ORDER BY a.node_id LIMIT LEAST(GREATEST(node_limit,0),4096)) l)
  UNION
  (SELECT l.node_id FROM c CROSS JOIN LATERAL (
   SELECT a.node_id FROM library_node_item_evidence a WHERE a.source_item_id=c.source_item_id AND a.graph_id=c.graph_id
   AND a.node_id>=COALESCE(after_node,'00000000-0000-0000-0000-000000000000'::uuid) AND (after_node IS NULL OR a.node_id>after_node) ORDER BY a.node_id LIMIT LEAST(GREATEST(node_limit,0),4096)) l)
 ) SELECT keys.node_id FROM keys ORDER BY keys.node_id LIMIT LEAST(GREATEST(node_limit,0),4096)
$$;
REVOKE ALL ON FUNCTION library_evidence_change_nodes(UUID,BIGINT,UUID,INTEGER) FROM PUBLIC;

-- Correction-only titles depend on account context (0157/0324). A cache
-- computed for a graph owner is shareable only in that same account context.
CREATE FUNCTION library_evidence_context(g UUID) RETURNS JSONB LANGUAGE sql STABLE SET search_path=public,pg_temp AS $$
 SELECT jsonb_build_object('schema_version',1,'role',current_user,'account_id',NULLIF(current_setting('app.account_id',true),''),'row_security',jsonb_build_array(row_security_active('public.nodes'::regclass),row_security_active('public.atoms'::regclass),row_security_active('public.source_items'::regclass),row_security_active('public.source_versions'::regclass),row_security_active('public.source_version_chunks'::regclass),row_security_active('public.semantic_atoms'::regclass),row_security_active('public.node_semantic_atoms'::regclass),row_security_active('public.reef_identity_corrections'::regclass)),'correction_visible',CASE WHEN row_security_active('public.reef_identity_corrections'::regclass) THEN EXISTS(SELECT 1 FROM graphs g WHERE g.id=$1 AND g.account_id=NULLIF(current_setting('app.account_id',true),'')::uuid AND COALESCE(resolve_graph_reachable_by_user($1,NULLIF(current_setting('app.user_id',true),'')::uuid),false)) ELSE true END)
$$;
REVOKE ALL ON FUNCTION library_evidence_context(UUID) FROM PUBLIC;
CREATE FUNCTION library_evidence_shareable(g UUID) RETURNS BOOLEAN LANGUAGE sql STABLE SET search_path=public,pg_temp AS $$
 SELECT EXISTS(SELECT 1 FROM graphs g WHERE g.id=$1 AND g.account_id=NULLIF(current_setting('app.account_id',true),'')::uuid)
$$;
REVOKE ALL ON FUNCTION library_evidence_shareable(UUID) FROM PUBLIC;
ALTER TABLE library_node_evidence ENABLE ROW LEVEL SECURITY;
CREATE POLICY account_isolation ON library_node_evidence USING (graph_id IN (SELECT id FROM graphs WHERE account_id=current_setting('app.account_id')::uuid)) WITH CHECK (graph_id IN (SELECT id FROM graphs WHERE account_id=current_setting('app.account_id')::uuid));
GRANT SELECT,INSERT,UPDATE,DELETE ON library_node_evidence TO coralswarm_app;
ALTER TABLE library_node_item_evidence ENABLE ROW LEVEL SECURITY;
CREATE POLICY account_isolation ON library_node_item_evidence USING (graph_id IN (SELECT id FROM graphs WHERE account_id=current_setting('app.account_id')::uuid)) WITH CHECK (graph_id IN (SELECT id FROM graphs WHERE account_id=current_setting('app.account_id')::uuid));
GRANT SELECT,INSERT,UPDATE,DELETE ON library_node_item_evidence TO coralswarm_app;
ALTER TABLE library_evidence_changes ENABLE ROW LEVEL SECURITY;
CREATE POLICY account_isolation ON library_evidence_changes USING (graph_id IN (SELECT id FROM graphs WHERE account_id=current_setting('app.account_id')::uuid)) WITH CHECK (graph_id IN (SELECT id FROM graphs WHERE account_id=current_setting('app.account_id')::uuid));
GRANT SELECT,INSERT,UPDATE,DELETE ON library_evidence_changes TO coralswarm_app;
ALTER TABLE library_evidence_state ENABLE ROW LEVEL SECURITY;
CREATE POLICY account_isolation ON library_evidence_state USING (graph_id IN (SELECT id FROM graphs WHERE account_id=current_setting('app.account_id')::uuid)) WITH CHECK (graph_id IN (SELECT id FROM graphs WHERE account_id=current_setting('app.account_id')::uuid));
GRANT SELECT,INSERT,UPDATE,DELETE ON library_evidence_state TO coralswarm_app;
GRANT USAGE,SELECT ON SEQUENCE library_evidence_changes_id_seq TO coralswarm_app;
GRANT EXECUTE ON FUNCTION library_evidence_change_nodes(UUID,BIGINT,UUID,INTEGER),library_evidence_shareable(UUID),library_evidence_context(UUID) TO coralswarm_app;
