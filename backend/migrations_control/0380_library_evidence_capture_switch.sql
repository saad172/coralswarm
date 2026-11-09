-- PAIRED WITH: tenant 0201 (library_evidence_capture_switch).
-- Library evidence capture honours the listing switch, and a completed
-- backfill can acknowledge the change rows it covered.
--
-- Measured read-only on the production org tenant on 2026-10-27, with
-- LIBRARY_LISTING_FACTS off since the switch shipped in 0199:
-- library_evidence_changes held 858,075 rows for the 105k-node Organization
-- graph, growing 222k rows a day in bursts behind extraction, with 16,686
-- rows ever deleted against 882k inserted. Three causes; the first two are
-- repaired here, the third in workers/library_evidence.rs:
--  1. 0185/0194's capture functions never read library_listing_switch, so an
--     Off plane (nothing reads the model; only the worker drains) logged every
--     evidence-input write. 0199's listing capture returns at the top when
--     the switch is off; the three evidence capture functions now do the
--     same. The probe is one primary-key read of a one-row table per
--     statement (per row for the two row-level UPDATE triggers, whose WHEN
--     clause already limits them to real input changes).
--  2. A backfill recomputed every node of a graph but acknowledged no change
--     row ("Do not consume ANY change ids during backfill"), so a rebuilt
--     graph re-walked its whole backlog afterwards. Two columns make a
--     bounded acknowledgement exact: capture_xid on the log (the logging
--     transaction; pg_current_xact_id() is the top-level id, so a
--     subtransaction's row carries its parent) and backfill_snapshot on the
--     state row (pg_current_snapshot(), taken when the backfill restarts,
--     before its first node page). A row is covered when
--     pg_visible_in_snapshot(capture_xid,backfill_snapshot): its data change
--     committed before every page's snapshot, so every page saw it. A row
--     whose transaction was open at the snapshot, or began after it, is
--     kept (the listing log's capture_xid/input_snapshot pair, 0199, is the
--     same technique). Rows logged before this migration have a NULL
--     capture_xid, which the worker treats as covered: ADD COLUMN waited for
--     every in-flight writer of the table, so every NULL row committed before
--     any snapshot a later backfill can take. Neither ALTER rewrites the
--     table: the column is added without a default, and the volatile default
--     attached afterwards applies to new rows only.
--  3. (worker) a change whose fan-out exceeded the node budget blocked the
--     head of the queue, and a key logged many times was resolved once per
--     row.
-- Capture skipped while Off is gone, so, like library_listing_state.capture_since
-- (0199), the node grain records the switch generation it was certified
-- under (capture_since; backfill_capture_since while a backfill runs), and
-- the worker rebuilds it when the generation changes. Nothing reads the
-- model while Off (the reader requires the switch's enabled_since), and a
-- grain certified while Off is rebuilt at the next On anyway, so while Off
-- the worker computes no node evidence at all: it deletes the log in
-- bounded batches instead (workers/library_evidence.rs, purge_while_off).
-- That is how an Off plane's existing backlog drains after this migration.
-- The first On, after the flip barrier below, rebuilds every graph once.
-- The three bodies below are 0194's, with the switch probe as their first
-- statement; the control twin is byte for byte the same apart from the header.
SET LOCAL lock_timeout = '3s';

CREATE OR REPLACE FUNCTION library_evidence_node_updated() RETURNS TRIGGER
LANGUAGE plpgsql SECURITY DEFINER SET search_path=public,pg_temp AS $$
BEGIN
 IF NOT EXISTS(SELECT 1 FROM library_listing_switch WHERE enabled) THEN RETURN NULL; END IF;
 INSERT INTO library_evidence_changes(graph_id,node_id)
  SELECT g.id,NEW.node_id FROM graphs g WHERE g.id=NEW.graph_id;
 IF (OLD.graph_id,OLD.node_id) IS DISTINCT FROM (NEW.graph_id,NEW.node_id) THEN
  INSERT INTO library_evidence_changes(graph_id,node_id)
   SELECT g.id,OLD.node_id FROM graphs g WHERE g.id=OLD.graph_id;
 END IF;
 RETURN NULL;
END $$;

CREATE OR REPLACE FUNCTION library_evidence_atom_updated() RETURNS TRIGGER
LANGUAGE plpgsql SECURITY DEFINER SET search_path=public,pg_temp AS $$
BEGIN
 IF NOT EXISTS(SELECT 1 FROM library_listing_switch WHERE enabled) THEN RETURN NULL; END IF;
 INSERT INTO library_evidence_changes(graph_id,atom_id)
  SELECT g.id,NEW.atom_id FROM graphs g WHERE g.id=NEW.graph_id;
 IF (OLD.graph_id,OLD.atom_id) IS DISTINCT FROM (NEW.graph_id,NEW.atom_id) THEN
  INSERT INTO library_evidence_changes(graph_id,atom_id)
   SELECT g.id,OLD.atom_id FROM graphs g WHERE g.id=OLD.graph_id;
 END IF;
 RETURN NULL;
END $$;

CREATE OR REPLACE FUNCTION library_evidence_capture() RETURNS TRIGGER
LANGUAGE plpgsql SECURITY DEFINER SET search_path=public,pg_temp AS $$
DECLARE rows_sql TEXT; identities_sql TEXT;
BEGIN
 -- Off: no reader admits the model and the worker alone drains the log, so
 -- capturing would only grow it (one primary-key probe per statement).
 IF NOT EXISTS(SELECT 1 FROM library_listing_switch WHERE enabled) THEN RETURN NULL; END IF;
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

-- Lock order. Every ALTER below takes ACCESS EXCLUSIVE, so they run in the
-- order every runtime path already takes these tables: the worker turn
-- writes library_evidence_state first (its ON CONFLICT insert), then reads
-- library_listing_switch (the gate), then reads and deletes from
-- library_evidence_changes; a capture trigger reads the switch, then inserts
-- into the log; the reader's admission statement names state, switch, log in
-- that order. Altering the log first would invert the worker's order (it
-- holds state, waits for the log; this would hold the log, wait for state).

-- capture_since: the switch generation the certified node grain was
-- maintained under (NULL: certified with the switch off).
-- backfill_capture_since: the generation the running backfill is for; a
-- different one restarts it, as a different account or context does.
-- backfill_snapshot: the running backfill's start snapshot; after it
-- completes, the snapshot stays until every change row it covers has been
-- acknowledged in bounded batches, then clears.
ALTER TABLE library_evidence_state
 ADD COLUMN capture_since TIMESTAMPTZ,
 ADD COLUMN backfill_capture_since TIMESTAMPTZ,
 ADD COLUMN backfill_snapshot pg_snapshot;

-- The flip barrier. The capture triggers read the switch once per statement,
-- after the statement's own writes (so after its transaction has an xid). A
-- statement that read Off logs nothing even if its transaction commits after
-- the flip; a rebuild whose start snapshot saw that transaction in progress
-- would miss its write with no log row to repair it. enabled_snapshot is a
-- snapshot taken by a statement that already saw the flip committed
-- (snapshot_since names the generation it belongs to, so a stale snapshot
-- from an earlier generation, or one left by a binary that predates this
-- column, never counts). Every transaction that read Off has an xid below
-- its xmax; once pg_snapshot_xmin(pg_current_snapshot()) reaches that xmax
-- they have all ended, and only then may the worker start either grain's
-- rebuild for the generation. Every writer of these tables runs READ
-- COMMITTED (the only REPEATABLE READ transactions in the backend are READ
-- ONLY); a REPEATABLE READ writer would read the switch from its first
-- snapshot and could get its xid after the barrier, which this does not
-- cover.
ALTER TABLE library_listing_switch
 ADD COLUMN enabled_snapshot pg_snapshot,
 ADD COLUMN snapshot_since TIMESTAMPTZ;

-- The logging transaction. NULL: logged before this migration (see header).
ALTER TABLE library_evidence_changes ADD COLUMN capture_xid xid8;
ALTER TABLE library_evidence_changes ALTER COLUMN capture_xid SET DEFAULT pg_current_xact_id();
