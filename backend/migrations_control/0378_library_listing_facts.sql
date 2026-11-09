-- PAIRED WITH: tenant 0199 (library_listing_facts).
-- Reserved definition 3 storage only. No ready row is inserted by this migration.
-- Maintained facts are the source heads, the outcomes and one node count per
-- graph; totals and facets are aggregated at read time over the heads.
-- Serving order contains one active row per outcome; staging is a separate table.
-- During indexed reconciliation the WHOLE item remains dirty, and its old
-- published header/count contribution remains unchanged until final publication.
CREATE TABLE library_listing_state (
 graph_id UUID PRIMARY KEY REFERENCES graphs(id) ON DELETE CASCADE ON UPDATE CASCADE,
 epoch UUID NOT NULL DEFAULT gen_random_uuid(),
 definition_version INTEGER NOT NULL DEFAULT 3 CHECK(definition_version=3),
 ready_version INTEGER CHECK(ready_version=3),
 computed_account_id UUID, computed_context JSONB,
 -- The switch generation (library_listing_switch.enabled_since) these facts
 -- were maintained under. Capture missed while the switch was off is gone,
 -- so facts certified under another generation, or none, are a rebuild.
 capture_since TIMESTAMPTZ,
 -- Listing work shares the refresh transaction with node evidence but never
 -- its fate: a failed listing step rolls back to its savepoint, is recorded
 -- here, and the graph's listing work waits out retry_after (exponential
 -- backoff) while node evidence keeps refreshing. A later success clears
 -- the count and the wait; the last failure stays readable.
 failure_count INTEGER NOT NULL DEFAULT 0 CHECK(failure_count>=0),
 last_failure TEXT, last_failed_at TIMESTAMPTZ, retry_after TIMESTAMPTZ,
 -- The one maintained aggregate: count(*) of this graph's library_node_evidence
 -- rows with admitted_unfiltered. Source, outcome and facet totals are
 -- aggregated at read time over source_heads; counting every node at read
 -- time cost 40-1,700 ms cold on a 66k-node ocean. nodes_complete says the
 -- count was taken from the certified node grain under this generation;
 -- library_listing_node_count() below keeps it exact from then on.
 node_count BIGINT NOT NULL DEFAULT 0 CHECK(node_count>=0),
 sources_complete BOOLEAN NOT NULL DEFAULT false,
 nodes_complete BOOLEAN NOT NULL DEFAULT false,
 aggregates_complete BOOLEAN NOT NULL DEFAULT false,
 backfill_after_source_item_id UUID,
 backfill_after_source_updated_at TIMESTAMPTZ,
 sources_old_complete BOOLEAN NOT NULL DEFAULT false,
 UNIQUE(graph_id,epoch),
 CHECK(ready_version IS NULL OR (computed_account_id IS NOT NULL AND computed_context IS NOT NULL AND capture_since IS NOT NULL
  AND sources_complete AND nodes_complete AND aggregates_complete AND sources_old_complete))
);
-- The one switch for the listing program on this plane. The worker mirrors
-- its LIBRARY_LISTING_FACTS setting into this row every tick, so a backend
-- set Off stops the capture triggers below as well as its own maintenance:
-- with nothing draining library_listing_changes, capturing would only grow
-- it. Off is the shipped default. Every flip to On is a new enabled_since.
-- Not graph-keyed, so no RLS: one row per database, readable by the
-- SECURITY DEFINER capture function and the app role alike.
CREATE TABLE library_listing_switch (
 singleton BOOLEAN PRIMARY KEY DEFAULT true CHECK(singleton),
 enabled BOOLEAN NOT NULL DEFAULT false,
 enabled_since TIMESTAMPTZ,
 changed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 CHECK(NOT enabled OR enabled_since IS NOT NULL)
);
INSERT INTO library_listing_switch DEFAULT VALUES;
CREATE TABLE library_listing_changes (
 id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 graph_id UUID NOT NULL REFERENCES graphs(id) ON DELETE CASCADE ON UPDATE CASCADE,
 source_item_id UUID, source_version_id UUID, semantic_atom_id UUID,
 created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 capture_xid xid8 NOT NULL DEFAULT pg_current_xact_id(),
 UNIQUE(graph_id,id),
 UNIQUE(graph_id,source_item_id,id),
 CHECK(num_nonnulls(source_item_id,source_version_id,semantic_atom_id)>0)
);
CREATE INDEX library_listing_changes_graph ON library_listing_changes(graph_id,id);
CREATE INDEX library_listing_changes_item ON library_listing_changes(graph_id,source_item_id,id);
CREATE INDEX library_listing_changes_input ON library_listing_changes(graph_id,source_item_id,capture_xid,id);
CREATE INDEX library_listing_changes_version ON library_listing_changes(graph_id,source_version_id,id);
CREATE INDEX library_listing_changes_outcome ON library_listing_changes(graph_id,semantic_atom_id,id);
-- No object FKs: the OLD identity must outlive delete/move/cascade.
CREATE TABLE library_listing_generations (
 graph_id UUID NOT NULL, source_item_id UUID NOT NULL, generation UUID NOT NULL,
 epoch UUID NOT NULL,
 phase TEXT NOT NULL DEFAULT 'build' CHECK(phase IN('build','upsert','prune','ack','publish','discard','complete','abandoned')),
 guard_change_id BIGINT,
 captured_change_ids BIGINT[] NOT NULL,
 -- Snapshot identity, not a numeric log-ID watermark: late commits are input changes.
 input_snapshot pg_snapshot NOT NULL DEFAULT pg_current_snapshot(),
 input_account_id UUID, input_context JSONB,
 backfill_inventory_updated_at TIMESTAMPTZ,
 build_after_version_captured_at TIMESTAMPTZ, build_after_version_id UUID,
 build_version_id UUID, build_version_captured_at TIMESTAMPTZ,
 build_after_extraction_version TEXT, build_after_ordinal INTEGER,
 build_version_count BIGINT NOT NULL DEFAULT 0 CHECK(build_version_count>=0),
 current_outcome_count BIGINT NOT NULL DEFAULT 0 CHECK(current_outcome_count>=0),
 winner_version_id UUID, winner_captured_at TIMESTAMPTZ, winner_done BOOLEAN,
 winner_outcome_count BIGINT NOT NULL DEFAULT 0 CHECK(winner_outcome_count>=0),
 source_present BOOLEAN NOT NULL DEFAULT false,
 retained_count BIGINT NOT NULL DEFAULT 0 CHECK(retained_count>=0),
 served_count BIGINT NOT NULL DEFAULT 0 CHECK(served_count>=0 AND served_count<=retained_count),
 build_after_outcome_id UUID, upsert_after_outcome_id UUID, prune_after_outcome_id UUID,
 build_complete BOOLEAN NOT NULL DEFAULT false,
 upsert_complete BOOLEAN NOT NULL DEFAULT false,
 prune_complete BOOLEAN NOT NULL DEFAULT false,
 PRIMARY KEY(graph_id,source_item_id,generation),
 UNIQUE(graph_id,source_item_id,generation,epoch),
 UNIQUE(graph_id,source_item_id,generation,phase),
 FOREIGN KEY(graph_id,epoch) REFERENCES library_listing_state(graph_id,epoch) ON DELETE CASCADE,
 FOREIGN KEY(graph_id,source_item_id,guard_change_id) REFERENCES library_listing_changes(graph_id,source_item_id,id)
  DEFERRABLE INITIALLY DEFERRED,
 CHECK(array_position(captured_change_ids,NULL) IS NULL AND 0<ALL(captured_change_ids)),
 CHECK(guard_change_id IS NULL OR guard_change_id=ANY(captured_change_ids)),
 CHECK(phase IN('complete','abandoned') OR guard_change_id IS NOT NULL),
 CHECK(phase<>'complete' OR (build_complete AND upsert_complete AND prune_complete)),
 CHECK(source_present OR retained_count=0)
);
CREATE UNIQUE INDEX library_listing_one_build ON library_listing_generations(graph_id)
 WHERE phase NOT IN('complete','abandoned');
-- This header is also the old counted contribution while an item is dirty.
CREATE TABLE library_listing_source_heads (
 graph_id UUID NOT NULL REFERENCES graphs(id) ON DELETE CASCADE ON UPDATE CASCADE, source_item_id UUID NOT NULL,
 epoch UUID NOT NULL, generation UUID NOT NULL,
 published_phase TEXT NOT NULL DEFAULT 'complete' CHECK(published_phase='complete'),
 source_type TEXT NOT NULL, provider_namespace TEXT NOT NULL, source_id TEXT NOT NULL,
 origin JSONB, discovery_content_kind TEXT,
 current_version_id UUID, selected_version_id UUID, served_version_id UUID,
 captured_at TIMESTAMPTZ, title256 TEXT,
 unknown_independence BOOLEAN NOT NULL,
 raw_extraction_status TEXT CHECK(raw_extraction_status IN('not_started','pending','running','done','failed','blocked')),
 raw_next_start_char BIGINT,
 raw_extraction_reason TEXT CHECK(raw_extraction_reason IN('managed_admission_required','capture_actor_unknown_managed_admission_required','managed_run_paused','execution_budget_exhausted','cloud_processing_disabled','requester_access_lost','source_removed_or_access_lost')),
 retained_count BIGINT NOT NULL CHECK(retained_count>=0),
 served_count BIGINT NOT NULL CHECK(served_count>=0 AND served_count<=retained_count),
 PRIMARY KEY(graph_id,source_item_id),
 FOREIGN KEY(graph_id,source_item_id,generation,epoch)
  REFERENCES library_listing_generations(graph_id,source_item_id,generation,epoch) DEFERRABLE INITIALLY DEFERRED,
 FOREIGN KEY(graph_id,source_item_id,generation,published_phase)
  REFERENCES library_listing_generations(graph_id,source_item_id,generation,phase)
  DEFERRABLE INITIALLY DEFERRED,
 CHECK(selected_version_id IS NULL OR selected_version_id=current_version_id),
 CHECK((selected_version_id IS NULL AND captured_at IS NULL AND title256 IS NULL
  AND raw_extraction_status IS NULL AND raw_next_start_char IS NULL AND raw_extraction_reason IS NULL)
  OR (selected_version_id IS NOT NULL AND captured_at IS NOT NULL AND raw_extraction_status IS NOT NULL AND raw_next_start_char IS NOT NULL)),
 CHECK(raw_extraction_reason IS NULL OR raw_extraction_status='blocked'),
 CHECK(served_count=0 OR served_version_id IS NOT NULL)
);
CREATE INDEX library_listing_source_order ON library_listing_source_heads(graph_id,captured_at DESC NULLS LAST,source_item_id DESC);
CREATE INDEX library_listing_source_current ON library_listing_source_heads(graph_id,current_version_id);
CREATE TABLE library_listing_staged_sources (LIKE library_listing_source_heads INCLUDING DEFAULTS INCLUDING CONSTRAINTS);
ALTER TABLE library_listing_staged_sources ADD PRIMARY KEY(graph_id,source_item_id,generation);
ALTER TABLE library_listing_staged_sources ADD FOREIGN KEY(graph_id,source_item_id,generation,epoch)
 REFERENCES library_listing_generations(graph_id,source_item_id,generation,epoch) ON DELETE CASCADE;
CREATE TABLE library_listing_outcomes (
 graph_id UUID NOT NULL REFERENCES graphs(id) ON DELETE CASCADE ON UPDATE CASCADE,
 semantic_atom_id UUID NOT NULL, source_item_id UUID NOT NULL,
 source_version_id UUID NOT NULL, generation UUID NOT NULL,
 occurred_at TIMESTAMPTZ, captured_at TIMESTAMPTZ NOT NULL,
 source_version_is_current BOOLEAN NOT NULL,
 PRIMARY KEY(graph_id,semantic_atom_id),
 FOREIGN KEY(graph_id,source_item_id,generation)
  REFERENCES library_listing_generations(graph_id,source_item_id,generation) DEFERRABLE INITIALLY DEFERRED
);
CREATE INDEX library_listing_outcome_item ON library_listing_outcomes(graph_id,source_item_id,semantic_atom_id);
CREATE INDEX library_listing_outcome_version ON library_listing_outcomes(graph_id,source_version_id,semantic_atom_id);
CREATE INDEX library_listing_outcome_retained_order ON library_listing_outcomes
 (graph_id,occurred_at DESC NULLS LAST,captured_at DESC,semantic_atom_id DESC) INCLUDE(source_item_id,source_version_id);
CREATE INDEX library_listing_outcome_served_order ON library_listing_outcomes
 (graph_id,occurred_at DESC NULLS LAST,captured_at DESC,semantic_atom_id DESC) INCLUDE(source_item_id,source_version_id)
 WHERE source_version_is_current;
CREATE TABLE library_listing_staged_outcomes (LIKE library_listing_outcomes INCLUDING DEFAULTS INCLUDING CONSTRAINTS);
ALTER TABLE library_listing_staged_outcomes ADD PRIMARY KEY(graph_id,source_item_id,generation,semantic_atom_id);
CREATE INDEX library_listing_staged_parent ON library_listing_staged_outcomes(graph_id,semantic_atom_id,source_item_id);
ALTER TABLE library_listing_staged_outcomes ADD FOREIGN KEY(graph_id,source_item_id,generation)
 REFERENCES library_listing_generations(graph_id,source_item_id,generation) ON DELETE CASCADE;

-- A rebuild needs a durable item guard too, BEFORE the first destructive batch.
-- Pending IDs cannot disappear while any unfinished generation still owns them.
CREATE FUNCTION library_listing_active_guard() RETURNS TRIGGER LANGUAGE plpgsql SET search_path=public,pg_temp AS $$
DECLARE g UUID; i UUID;
BEGIN
 -- A graph cascade removes the whole epoch; no per-item guard survives it.
 IF TG_OP='DELETE' AND NOT EXISTS(SELECT 1 FROM graphs WHERE id=OLD.graph_id) THEN RETURN OLD; END IF;
 IF TG_OP<>'INSERT' THEN
  g:=OLD.graph_id; i:=OLD.source_item_id;
  IF NOT EXISTS(SELECT 1 FROM library_listing_changes c WHERE c.graph_id=g AND c.source_item_id=i) THEN
   RAISE EXCEPTION 'listing active write without old item guard' USING ERRCODE='23514';
  END IF;
 END IF;
 IF TG_OP<>'DELETE' THEN
  IF NOT EXISTS(SELECT 1 FROM library_listing_generations x JOIN library_listing_changes c
   ON c.graph_id=x.graph_id AND c.id=x.guard_change_id
   WHERE x.graph_id=NEW.graph_id AND x.source_item_id=NEW.source_item_id AND x.generation=NEW.generation
    AND x.phase='upsert' AND c.source_item_id=NEW.source_item_id) THEN
   RAISE EXCEPTION 'listing active write without generation guard' USING ERRCODE='23514';
  END IF;
  RETURN NEW;
 END IF;
 RETURN OLD;
END $$;
REVOKE ALL ON FUNCTION library_listing_active_guard() FROM PUBLIC;
CREATE TRIGGER library_listing_active_guard BEFORE INSERT OR UPDATE OR DELETE ON library_listing_outcomes
 FOR EACH ROW EXECUTE FUNCTION library_listing_active_guard();
CREATE FUNCTION library_listing_generation_identity() RETURNS TRIGGER LANGUAGE plpgsql SET search_path=public,pg_temp AS $$
BEGIN
 IF TG_OP='INSERT' THEN
  IF NEW.phase<>'build' OR NEW.guard_change_id IS NULL OR NEW.build_complete OR NEW.upsert_complete OR NEW.prune_complete THEN
   RAISE EXCEPTION 'listing generation must start guarded and incomplete' USING ERRCODE='23514';
  END IF;
  RETURN NEW;
 END IF;
 IF OLD.phase IN('complete','abandoned') AND to_jsonb(NEW) IS DISTINCT FROM to_jsonb(OLD) THEN
  RAISE EXCEPTION 'closed listing generation is immutable' USING ERRCODE='23514';
 END IF;
 IF (NEW.graph_id,NEW.source_item_id,NEW.generation,NEW.epoch,NEW.captured_change_ids,NEW.input_snapshot::text,NEW.input_account_id,NEW.input_context,NEW.backfill_inventory_updated_at)
  IS DISTINCT FROM (OLD.graph_id,OLD.source_item_id,OLD.generation,OLD.epoch,OLD.captured_change_ids,OLD.input_snapshot::text,OLD.input_account_id,OLD.input_context,OLD.backfill_inventory_updated_at) THEN
  RAISE EXCEPTION 'listing generation identity/selected IDs are immutable' USING ERRCODE='23514';
 END IF;
 IF NEW.guard_change_id IS DISTINCT FROM OLD.guard_change_id
  AND NOT (NEW.guard_change_id IS NULL AND NEW.phase IN('complete','abandoned')) THEN
  RAISE EXCEPTION 'listing generation guard cannot be replaced' USING ERRCODE='23514';
 END IF;
 IF NEW.phase IS DISTINCT FROM OLD.phase AND NOT (
  (OLD.phase='build' AND NEW.phase='upsert' AND NEW.build_complete)
  OR (OLD.phase='upsert' AND NEW.phase='prune' AND NEW.upsert_complete)
  OR (OLD.phase='prune' AND NEW.phase='ack' AND NEW.prune_complete)
  OR (OLD.phase='ack' AND NEW.phase='publish')
  OR (OLD.phase='publish' AND NEW.phase='complete' AND NEW.guard_change_id IS NULL)
  OR (OLD.phase NOT IN('complete','abandoned','discard') AND NEW.phase='discard')
  OR (OLD.phase='discard' AND NEW.phase='abandoned' AND NEW.guard_change_id IS NULL)) THEN
  RAISE EXCEPTION 'invalid listing generation transition' USING ERRCODE='23514';
 END IF;
 RETURN NEW;
END $$;
REVOKE ALL ON FUNCTION library_listing_generation_identity() FROM PUBLIC;
CREATE TRIGGER library_listing_generation_identity BEFORE INSERT OR UPDATE ON library_listing_generations
 FOR EACH ROW EXECUTE FUNCTION library_listing_generation_identity();

-- Independent fact invalidations: node-only consumption must not ACK these.
-- All OLD/NEW images are captured. Parent mappings are hints plus deletion-safe
-- identities; an unresolved mapping is retained, never silently acknowledged.
CREATE FUNCTION library_listing_capture() RETURNS TRIGGER LANGUAGE plpgsql SECURITY DEFINER SET search_path=public,pg_temp AS $$
DECLARE rows_sql TEXT;
BEGIN
 -- Off: no worker drains the log, so capturing would only grow it (one
 -- primary-key probe per statement, not per row).
 IF NOT EXISTS(SELECT 1 FROM library_listing_switch WHERE enabled) THEN RETURN NULL; END IF;
 IF TG_OP='INSERT' THEN rows_sql:='SELECT * FROM listing_new';
 ELSIF TG_OP='DELETE' THEN rows_sql:='SELECT * FROM listing_old';
 ELSE rows_sql:='SELECT * FROM listing_old UNION ALL SELECT * FROM listing_new'; END IF;
 IF TG_TABLE_NAME='source_items' THEN
  EXECUTE format('INSERT INTO library_listing_changes(graph_id,source_item_id)
   SELECT DISTINCT t.graph_id,t.source_item_id FROM (%s) t JOIN graphs g ON g.id=t.graph_id',rows_sql);
 ELSIF TG_TABLE_NAME='source_versions' THEN
  EXECUTE format('INSERT INTO library_listing_changes(graph_id,source_item_id,source_version_id)
   SELECT DISTINCT t.graph_id,t.source_item_id,t.version_id FROM (%s) t JOIN graphs g ON g.id=t.graph_id',rows_sql);
 ELSE
  EXECUTE format('INSERT INTO library_listing_changes(graph_id,source_item_id,source_version_id,semantic_atom_id)
   SELECT DISTINCT t.graph_id,m.source_item_id,t.source_version_id,t.semantic_atom_id
   FROM (%s) t JOIN graphs g ON g.id=t.graph_id LEFT JOIN LATERAL (
    SELECT v.source_item_id FROM source_versions v WHERE v.graph_id=t.graph_id AND v.version_id=t.source_version_id
    UNION SELECT a.source_item_id FROM library_listing_outcomes a WHERE a.graph_id=t.graph_id AND a.semantic_atom_id=t.semantic_atom_id
    UNION SELECT a.source_item_id FROM library_listing_staged_outcomes a WHERE a.graph_id=t.graph_id AND a.semantic_atom_id=t.semantic_atom_id
   ) m ON true',rows_sql);
 END IF;
 RETURN NULL;
END $$;
REVOKE ALL ON FUNCTION library_listing_capture() FROM PUBLIC;
CREATE TRIGGER library_listing_source_items_insert AFTER INSERT ON source_items
 REFERENCING NEW TABLE AS listing_new FOR EACH STATEMENT EXECUTE FUNCTION library_listing_capture();
CREATE TRIGGER library_listing_source_items_update AFTER UPDATE ON source_items
 REFERENCING OLD TABLE AS listing_old NEW TABLE AS listing_new FOR EACH STATEMENT EXECUTE FUNCTION library_listing_capture();
CREATE TRIGGER library_listing_source_items_delete AFTER DELETE ON source_items
 REFERENCING OLD TABLE AS listing_old FOR EACH STATEMENT EXECUTE FUNCTION library_listing_capture();
CREATE TRIGGER library_listing_source_versions_insert AFTER INSERT ON source_versions
 REFERENCING NEW TABLE AS listing_new FOR EACH STATEMENT EXECUTE FUNCTION library_listing_capture();
CREATE TRIGGER library_listing_source_versions_update AFTER UPDATE ON source_versions
 REFERENCING OLD TABLE AS listing_old NEW TABLE AS listing_new FOR EACH STATEMENT EXECUTE FUNCTION library_listing_capture();
CREATE TRIGGER library_listing_source_versions_delete AFTER DELETE ON source_versions
 REFERENCING OLD TABLE AS listing_old FOR EACH STATEMENT EXECUTE FUNCTION library_listing_capture();
CREATE TRIGGER library_listing_semantic_atoms_insert AFTER INSERT ON semantic_atoms
 REFERENCING NEW TABLE AS listing_new FOR EACH STATEMENT EXECUTE FUNCTION library_listing_capture();
CREATE TRIGGER library_listing_semantic_atoms_update AFTER UPDATE ON semantic_atoms
 REFERENCING OLD TABLE AS listing_old NEW TABLE AS listing_new FOR EACH STATEMENT EXECUTE FUNCTION library_listing_capture();
CREATE TRIGGER library_listing_semantic_atoms_delete AFTER DELETE ON semantic_atoms
 REFERENCING OLD TABLE AS listing_old FOR EACH STATEMENT EXECUTE FUNCTION library_listing_capture();
-- The maintained node count follows the node grain by delta: each
-- library_node_evidence write (the worker's batches, a node cascade) adds the
-- admitted rows it created and subtracts those it removed. The listing worker
-- takes the baseline count from the certified grain when nodes_complete is
-- false; after that this keeps it exact. Switch off: nothing is maintained.
CREATE FUNCTION library_listing_node_count() RETURNS TRIGGER LANGUAGE plpgsql SECURITY DEFINER SET search_path=public,pg_temp AS $$
BEGIN
 IF NOT EXISTS(SELECT 1 FROM library_listing_switch WHERE enabled) THEN RETURN NULL; END IF;
 IF TG_OP='INSERT' THEN
  UPDATE library_listing_state s SET node_count=s.node_count+d.n FROM (SELECT graph_id,count(*) n FROM listing_new WHERE admitted_unfiltered GROUP BY 1) d WHERE s.graph_id=d.graph_id;
 ELSIF TG_OP='DELETE' THEN
  UPDATE library_listing_state s SET node_count=GREATEST(0,s.node_count-d.n) FROM (SELECT graph_id,count(*) n FROM listing_old WHERE admitted_unfiltered GROUP BY 1) d WHERE s.graph_id=d.graph_id;
 ELSE
  UPDATE library_listing_state s SET node_count=GREATEST(0,s.node_count+d.n) FROM (
   SELECT graph_id,sum(n) n FROM (
    SELECT graph_id,count(*) n FROM listing_new WHERE admitted_unfiltered GROUP BY 1
    UNION ALL SELECT graph_id,-count(*) FROM listing_old WHERE admitted_unfiltered GROUP BY 1) x GROUP BY 1) d
  WHERE s.graph_id=d.graph_id AND d.n<>0;
 END IF;
 RETURN NULL;
END $$;
REVOKE ALL ON FUNCTION library_listing_node_count() FROM PUBLIC;
CREATE TRIGGER library_listing_node_count_insert AFTER INSERT ON library_node_evidence
 REFERENCING NEW TABLE AS listing_new FOR EACH STATEMENT EXECUTE FUNCTION library_listing_node_count();
CREATE TRIGGER library_listing_node_count_update AFTER UPDATE ON library_node_evidence
 REFERENCING OLD TABLE AS listing_old NEW TABLE AS listing_new FOR EACH STATEMENT EXECUTE FUNCTION library_listing_node_count();
CREATE TRIGGER library_listing_node_count_delete AFTER DELETE ON library_node_evidence
 REFERENCING OLD TABLE AS listing_old FOR EACH STATEMENT EXECUTE FUNCTION library_listing_node_count();
-- A node page reads every item row of its nodes; covering them makes that an
-- index-only range read instead of one heap fetch per item (cold page detail
-- 4.5 s to 1.3 s on the measured org tenant).
CREATE INDEX library_node_item_evidence_node_covering ON library_node_item_evidence(graph_id,node_id)
 INCLUDE (source_item_id,semantic_link,legacy_link,legacy_independent_version,legacy_synthetic,atom_count_retained,atom_count_served,
  project_mention,semantic_independent_version,semantic_synthetic,semantic_latest_captured_at,entity_type_has_match,entity_type_value,
  entity_type_captured_at,entity_type_semantic_atom_id,entity_type_ambiguous);
ALTER TABLE library_listing_state ENABLE ROW LEVEL SECURITY;
CREATE POLICY account_isolation ON library_listing_state USING (graph_id IN (SELECT id FROM graphs WHERE account_id=current_setting('app.account_id')::uuid)) WITH CHECK (graph_id IN (SELECT id FROM graphs WHERE account_id=current_setting('app.account_id')::uuid));
GRANT SELECT,INSERT,UPDATE,DELETE ON library_listing_state TO coralswarm_app;
ALTER TABLE library_listing_changes ENABLE ROW LEVEL SECURITY;
CREATE POLICY account_isolation ON library_listing_changes USING (graph_id IN (SELECT id FROM graphs WHERE account_id=current_setting('app.account_id')::uuid)) WITH CHECK (graph_id IN (SELECT id FROM graphs WHERE account_id=current_setting('app.account_id')::uuid));
GRANT SELECT,INSERT,UPDATE,DELETE ON library_listing_changes TO coralswarm_app;
ALTER TABLE library_listing_generations ENABLE ROW LEVEL SECURITY;
CREATE POLICY account_isolation ON library_listing_generations USING (graph_id IN (SELECT id FROM graphs WHERE account_id=current_setting('app.account_id')::uuid)) WITH CHECK (graph_id IN (SELECT id FROM graphs WHERE account_id=current_setting('app.account_id')::uuid));
GRANT SELECT,INSERT,UPDATE,DELETE ON library_listing_generations TO coralswarm_app;
ALTER TABLE library_listing_source_heads ENABLE ROW LEVEL SECURITY;
CREATE POLICY account_isolation ON library_listing_source_heads USING (graph_id IN (SELECT id FROM graphs WHERE account_id=current_setting('app.account_id')::uuid)) WITH CHECK (graph_id IN (SELECT id FROM graphs WHERE account_id=current_setting('app.account_id')::uuid));
GRANT SELECT,INSERT,UPDATE,DELETE ON library_listing_source_heads TO coralswarm_app;
ALTER TABLE library_listing_staged_sources ENABLE ROW LEVEL SECURITY;
CREATE POLICY account_isolation ON library_listing_staged_sources USING (graph_id IN (SELECT id FROM graphs WHERE account_id=current_setting('app.account_id')::uuid)) WITH CHECK (graph_id IN (SELECT id FROM graphs WHERE account_id=current_setting('app.account_id')::uuid));
GRANT SELECT,INSERT,UPDATE,DELETE ON library_listing_staged_sources TO coralswarm_app;
ALTER TABLE library_listing_outcomes ENABLE ROW LEVEL SECURITY;
CREATE POLICY account_isolation ON library_listing_outcomes USING (graph_id IN (SELECT id FROM graphs WHERE account_id=current_setting('app.account_id')::uuid)) WITH CHECK (graph_id IN (SELECT id FROM graphs WHERE account_id=current_setting('app.account_id')::uuid));
GRANT SELECT,INSERT,UPDATE,DELETE ON library_listing_outcomes TO coralswarm_app;
ALTER TABLE library_listing_staged_outcomes ENABLE ROW LEVEL SECURITY;
CREATE POLICY account_isolation ON library_listing_staged_outcomes USING (graph_id IN (SELECT id FROM graphs WHERE account_id=current_setting('app.account_id')::uuid)) WITH CHECK (graph_id IN (SELECT id FROM graphs WHERE account_id=current_setting('app.account_id')::uuid));
GRANT SELECT,INSERT,UPDATE,DELETE ON library_listing_staged_outcomes TO coralswarm_app;
GRANT USAGE,SELECT ON SEQUENCE library_listing_changes_id_seq TO coralswarm_app;
GRANT SELECT,UPDATE ON library_listing_switch TO coralswarm_app;
