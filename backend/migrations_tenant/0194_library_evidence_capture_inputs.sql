-- PAIRED WITH: control 0363 (library_evidence_capture_inputs).
-- Library evidence change capture fires only when an evidence input changes.
-- The bodies match the control twin byte for byte apart from this header.
--
-- tenant 0185 / control 0354 captured every INSERT and UPDATE of atoms and
-- nodes with statement-level transition tables: each write copied the whole
-- old and new rows (an embedding write carries two vector(384) columns) and
-- logged a key whether or not anything the evidence reads changed. Measured
-- on a branch of the production org tenant (2,000 rows per batch, median of
-- five, capture on vs off):
--   atoms embedding UPDATE, one statement     4.0 s vs 3.1 s, 2,000 keys
--   atoms embedding UPDATE, row by row        4.3 s vs 3.3 s, 2,000 keys
--   nodes counter UPDATE, row by row          0.90 s vs 0.41 s, 2,000 keys
--   node_atoms INSERT (link + count triggers) 1.44 s vs 0.69 s, 6,000 keys
-- node_atoms' coral_count and last_happened_at triggers each UPDATE the node,
-- so every new link logged its node three times.
--
-- 1. atoms and nodes UPDATE capture is row level, restricted to the columns
--    the evidence definition (library_read/evidence.rs and the functions it
--    calls) reads, and fires only when one of them is distinct:
--    - nodes: NODE_INPUT_CTES' node row (title, owner_id, the synthetic_hub
--      key of body, type, entity_type_sticky, entity_type) and its identity.
--    - atoms: library_permitted_atoms (graph_id, source_type, source_id,
--      evidence_retracted, derived_copy_bound, source_evidence_bound), the
--      title text (content_inline, participants), the atoms SELECT policy's
--      history window (timestamp), and every column
--      reef_identity_atom_fingerprint hashes, because reef_identity_support_valid
--      decides whether a correction titles its node.
--    UPDATE OF names the statement's SET targets, not columns a BEFORE trigger
--    rewrites. The atoms BEFORE triggers that rewrite an input column derive it
--    from listed columns (content_revision) or keep it sticky
--    (evidence_retracted, source_evidence_bound). library_bind_derived_copy
--    also sets derived_copy_bound on an unrelated UPDATE of a row with
--    promoted_from_atom_id but derived_copy_bound false; its own BEFORE INSERT
--    arm never lets such a row exist (zero on the production org tenant).
--    Row-level triggers cannot carry transition tables with a column list,
--    and these need none: one key per changed row, plus the old key when the
--    identity moved.
-- 2. atoms and nodes INSERT capture is dropped. The definition reaches an atom
--    only through a node_atoms link (directly, or as a correction's support,
--    which references node_atoms), and a node only through its links. Those
--    foreign keys are not deferrable, so no link can exist before its atom or
--    node, and inserting the link logs the node. A node with no links computes
--    the constant zero row, which no reader admits.
-- 3. A source_items identity change probed every atom of the item's source
--    type in the graph (an expression join on source_id) inside the writer's
--    transaction. It now probes the exact id and the ':chunk:<n>' range by
--    index; the 0161 identity predicate is rechecked unchanged.
-- 4. The queue format is unchanged: one key column per row, so the
--    library_evidence_changes CHECK holds. Every capture INSERT names exactly
--    one key column, copied from a NOT NULL column (pinned by
--    library_evidence_changes_pg).
-- The new functions are trigger functions, which no role calls directly, so
-- nothing here needs a grant; PUBLIC's default EXECUTE is revoked.
SET LOCAL lock_timeout = '3s';

DROP TRIGGER library_evidence_atoms_insert ON atoms;
DROP TRIGGER library_evidence_atoms_update ON atoms;
DROP TRIGGER library_evidence_nodes_insert ON nodes;
DROP TRIGGER library_evidence_nodes_update ON nodes;

CREATE FUNCTION library_evidence_node_updated() RETURNS TRIGGER
LANGUAGE plpgsql SECURITY DEFINER SET search_path=public,pg_temp AS $$
BEGIN
 INSERT INTO library_evidence_changes(graph_id,node_id)
  SELECT g.id,NEW.node_id FROM graphs g WHERE g.id=NEW.graph_id;
 IF (OLD.graph_id,OLD.node_id) IS DISTINCT FROM (NEW.graph_id,NEW.node_id) THEN
  INSERT INTO library_evidence_changes(graph_id,node_id)
   SELECT g.id,OLD.node_id FROM graphs g WHERE g.id=OLD.graph_id;
 END IF;
 RETURN NULL;
END $$;
REVOKE ALL ON FUNCTION library_evidence_node_updated() FROM PUBLIC;
CREATE TRIGGER library_evidence_nodes_update
 AFTER UPDATE OF graph_id,node_id,title,owner_id,body,type,entity_type_sticky,entity_type ON nodes
 FOR EACH ROW WHEN ((OLD.graph_id,OLD.node_id,OLD.title,OLD.owner_id,OLD.body->>'synthetic_hub',OLD.type,OLD.entity_type_sticky,OLD.entity_type)
  IS DISTINCT FROM (NEW.graph_id,NEW.node_id,NEW.title,NEW.owner_id,NEW.body->>'synthetic_hub',NEW.type,NEW.entity_type_sticky,NEW.entity_type))
 EXECUTE FUNCTION library_evidence_node_updated();

CREATE FUNCTION library_evidence_atom_updated() RETURNS TRIGGER
LANGUAGE plpgsql SECURITY DEFINER SET search_path=public,pg_temp AS $$
BEGIN
 INSERT INTO library_evidence_changes(graph_id,atom_id)
  SELECT g.id,NEW.atom_id FROM graphs g WHERE g.id=NEW.graph_id;
 IF (OLD.graph_id,OLD.atom_id) IS DISTINCT FROM (NEW.graph_id,NEW.atom_id) THEN
  INSERT INTO library_evidence_changes(graph_id,atom_id)
   SELECT g.id,OLD.atom_id FROM graphs g WHERE g.id=OLD.graph_id;
 END IF;
 RETURN NULL;
END $$;
REVOKE ALL ON FUNCTION library_evidence_atom_updated() FROM PUBLIC;
CREATE TRIGGER library_evidence_atoms_update
 AFTER UPDATE OF graph_id,atom_id,source_type,source_id,content_inline,participants,evidence_retracted,derived_copy_bound,source_evidence_bound,
  timestamp,content_blob_key,created_at,header_len,scope,ingested_by,promoted_from_atom_id,promoted_from_revision,content_revision,title,title_kind ON atoms
 FOR EACH ROW WHEN ((OLD.graph_id,OLD.atom_id,OLD.source_type,OLD.source_id,OLD.content_inline,OLD.participants,OLD.evidence_retracted,OLD.derived_copy_bound,OLD.source_evidence_bound,
   OLD.timestamp,OLD.content_blob_key,OLD.created_at,OLD.header_len,OLD.scope,OLD.ingested_by,OLD.promoted_from_atom_id,OLD.promoted_from_revision,OLD.content_revision,OLD.title,OLD.title_kind)
  IS DISTINCT FROM (NEW.graph_id,NEW.atom_id,NEW.source_type,NEW.source_id,NEW.content_inline,NEW.participants,NEW.evidence_retracted,NEW.derived_copy_bound,NEW.source_evidence_bound,
   NEW.timestamp,NEW.content_blob_key,NEW.created_at,NEW.header_len,NEW.scope,NEW.ingested_by,NEW.promoted_from_atom_id,NEW.promoted_from_revision,NEW.content_revision,NEW.title,NEW.title_kind))
 EXECUTE FUNCTION library_evidence_atom_updated();

-- The shared statement-level capture, unchanged except the unbound-identity
-- probe at the end of the source_items arm. Its atoms/nodes arms still serve
-- their DELETE triggers.
CREATE OR REPLACE FUNCTION library_evidence_capture() RETURNS TRIGGER
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
   -- Probe the two source_id shapes the identity rule can match, each by
   -- index: the exact id (atoms' unique graph/type/id key) and the
   -- '<id>:chunk:<n>' range (byte order on idx_atoms_graph_source_id_pattern;
   -- ';' follows ':'). The CASE recheck below is the 0161 rule unchanged.
   EXECUTE format('INSERT INTO library_evidence_changes(graph_id,atom_id)
    SELECT DISTINCT a.graph_id,a.atom_id
    FROM (SELECT DISTINCT t.graph_id,t.source_type,t.source_id FROM (%s) t WHERE t.access_state<>''permitted'') t
    CROSS JOIN LATERAL (
     SELECT a.graph_id,a.atom_id,a.source_id,a.source_evidence_bound FROM atoms a
     WHERE a.graph_id=t.graph_id AND a.source_type=t.source_type AND a.source_id=t.source_id
     UNION ALL
     SELECT a.graph_id,a.atom_id,a.source_id,a.source_evidence_bound FROM atoms a
     WHERE a.graph_id=t.graph_id AND a.source_id ~>=~ (t.source_id||'':chunk:'') AND a.source_id ~<~ (t.source_id||'':chunk;'')
      AND a.source_type=t.source_type) a
    JOIN graphs g ON g.id=a.graph_id
    WHERE NOT a.source_evidence_bound
     AND t.source_id=CASE WHEN right(rtrim(a.source_id,''0123456789''),7)='':chunk:'' AND rtrim(a.source_id,''0123456789'')<>a.source_id
      THEN left(rtrim(a.source_id,''0123456789''),-7) ELSE a.source_id END',identities_sql);
  END IF;
 END IF;
 RETURN NULL;
END $$;
