-- Identity is established by identifiers/context, never solely by a homonym.
ALTER TABLE nodes ADD COLUMN identity_scope UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000000';
-- Phase one retains the legacy title index for old serving binaries.
-- Phase two is separately gated in docs/implementation/p2-identity-contract.sql.
CREATE UNIQUE INDEX nodes_graph_type_lower_title_identity_unique ON nodes(graph_id,type,lower(title),identity_scope);

-- Non-content, durable delivery record remains after all source rows vanish.
ALTER TABLE source_items ADD COLUMN access_state TEXT NOT NULL DEFAULT 'permitted' CHECK(access_state IN ('permitted','restricted','unknown'));
CREATE TABLE source_retraction_outbox (
 event_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
 graph_id UUID NOT NULL REFERENCES graphs(id) ON DELETE CASCADE,
 source_item_id UUID NOT NULL,
 event_kind TEXT NOT NULL DEFAULT 'deleted' CHECK(event_kind IN ('deleted','restricted','unknown')),
 created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 processed_at TIMESTAMPTZ,
 CHECK(source_item_id IS NOT NULL)
);
CREATE UNIQUE INDEX source_retraction_outbox_pending ON source_retraction_outbox(graph_id,source_item_id,event_kind) WHERE processed_at IS NULL;
ALTER TABLE source_retraction_outbox ENABLE ROW LEVEL SECURITY;
CREATE POLICY graph_visibility ON source_retraction_outbox
 USING (EXISTS(SELECT 1 FROM graphs g WHERE g.id=source_retraction_outbox.graph_id))
 WITH CHECK (EXISTS(SELECT 1 FROM graphs g WHERE g.id=source_retraction_outbox.graph_id));

-- Direct support is permitted only when its mapped source is still readable.
CREATE FUNCTION atom_support_permitted(g UUID,support_id TEXT) RETURNS BOOLEAN LANGUAGE SQL STABLE AS $$
 SELECT EXISTS(SELECT 1 FROM atoms a WHERE a.graph_id=g AND a.atom_id::text=support_id
  AND NOT EXISTS(SELECT 1 FROM source_version_chunks c JOIN source_versions v ON v.version_id=c.source_version_id AND v.graph_id=c.graph_id JOIN source_items s ON s.source_item_id=v.source_item_id AND s.graph_id=v.graph_id WHERE c.graph_id=g AND c.atom_id=a.atom_id AND s.access_state<>'permitted'))
$$;

-- Existing caches/answers lack exhaustive source lineage. Remove affected
-- authorized derived prose conservatively instead of leaving stale content.
CREATE FUNCTION invalidate_derived_read_surfaces(g UUID) RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
 DELETE FROM journal_rollups WHERE graph_id=g;
 DELETE FROM query_history WHERE graph_id=g;
 -- Org extraction caches use the nil account sentinel and have no graph
 -- lineage; conservatively clear that tenant-wide cache as well.
 DELETE FROM extraction_cache WHERE account_id=(SELECT account_id FROM graphs WHERE id=g)
  OR account_id='00000000-0000-0000-0000-000000000000'::uuid;
 UPDATE queries SET answer=NULL WHERE account_id=(SELECT account_id FROM graphs WHERE id=g) AND answer IS NOT NULL;
 UPDATE conversation_messages m SET text='[Evidence no longer available]',citations='[]',sources='[]'
  WHERE m.role='assistant' AND (m.scope_graph_ids @> to_jsonb(ARRAY[g::text]) OR EXISTS(SELECT 1 FROM conversations c WHERE c.id=m.conversation_id AND c.graph_id=g));
 UPDATE conversations c SET title='' WHERE c.graph_id=g OR EXISTS(SELECT 1 FROM conversation_oceans o WHERE o.conversation_id=c.id AND o.graph_id=g);
END $$;

-- SECURITY INVOKER: graph and evidence visibility follow the deleting actor's
-- actual transaction scope on both planes. Independent readable support wins.
CREATE FUNCTION reconcile_relationship_lifecycle(g UUID) RETURNS VOID LANGUAGE plpgsql AS $$
DECLARE changed INTEGER;
BEGIN
 LOOP
  UPDATE relationship_edge e
   SET status='rejected',
       provenance=(e.provenance - 'rule' - 'source_edge_ids') || jsonb_build_object(
        'lifecycle','unsupported', 'human_review_status',COALESCE(e.provenance->>'human_review_status',e.status)),
       version=e.version+1,updated_at=now()
   WHERE e.graph_id=g AND e.status <> 'rejected'
    AND jsonb_typeof(e.provenance->'source_edge_ids')='array'
    AND EXISTS(SELECT 1 FROM jsonb_array_elements_text(e.provenance->'source_edge_ids') s(id)
     WHERE NOT EXISTS(SELECT 1 FROM relationship_edge p WHERE p.graph_id=g AND p.edge_id::text=s.id AND p.status='confirmed'))
    AND NOT EXISTS(SELECT 1 FROM jsonb_array_elements_text(
      CASE WHEN jsonb_typeof(e.provenance->'atom_ids')='array' THEN e.provenance->'atom_ids' ELSE '[]'::jsonb END) s(id)
      WHERE atom_support_permitted(g,s.id));
  GET DIAGNOSTICS changed=ROW_COUNT;
  EXIT WHEN changed=0;
 END LOOP;
 -- If a chain broke but direct, authorized support survives, discard only the
 -- failed derivation. Explicit review fields/status remain unchanged.
 UPDATE relationship_edge e SET provenance=e.provenance-'rule'-'source_edge_ids',version=version+1,updated_at=now()
 WHERE e.graph_id=g AND e.status<>'rejected' AND jsonb_typeof(e.provenance->'source_edge_ids')='array'
 AND EXISTS(SELECT 1 FROM jsonb_array_elements_text(e.provenance->'source_edge_ids') s(id)
  WHERE NOT EXISTS(SELECT 1 FROM relationship_edge p WHERE p.graph_id=g AND p.edge_id::text=s.id AND p.status='confirmed'));
END $$;
CREATE FUNCTION relationship_lifecycle_changed() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
 IF pg_trigger_depth()=1 THEN PERFORM reconcile_relationship_lifecycle(OLD.graph_id); END IF;
 RETURN NULL;
END $$;
CREATE TRIGGER relationship_lifecycle_rejected AFTER UPDATE OF status ON relationship_edge
 FOR EACH ROW WHEN (NEW.status='rejected' AND OLD.status<>'rejected') EXECUTE FUNCTION relationship_lifecycle_changed();
CREATE TRIGGER relationship_lifecycle_deleted AFTER DELETE ON relationship_edge
 FOR EACH ROW EXECUTE FUNCTION relationship_lifecycle_changed();

CREATE FUNCTION atom_lifecycle_deleted() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
 UPDATE relationship_edge e SET
  provenance=jsonb_set(e.provenance,'{atom_ids}',COALESCE((SELECT jsonb_agg(v) FROM jsonb_array_elements(e.provenance->'atom_ids') v WHERE v<>to_jsonb(OLD.atom_id::text)),'[]')),
  version=version+1,updated_at=now()
 WHERE e.graph_id=OLD.graph_id AND jsonb_typeof(e.provenance->'atom_ids')='array' AND e.provenance->'atom_ids' @> to_jsonb(ARRAY[OLD.atom_id::text]);
 UPDATE relationship_edge e SET status='rejected',
  provenance=e.provenance || jsonb_build_object('lifecycle','unsupported','human_review_status',COALESCE(e.provenance->>'human_review_status',e.status)),
  version=version+1,updated_at=now()
 WHERE e.graph_id=OLD.graph_id AND e.status<>'rejected' AND e.provenance->'atom_ids'='[]'::jsonb
 AND NOT COALESCE((jsonb_typeof(e.provenance->'source_edge_ids')='array' AND jsonb_array_length(e.provenance->'source_edge_ids')>0),false);
 PERFORM reconcile_relationship_lifecycle(OLD.graph_id);
 -- Existing summaries have no reliable per-passage lineage. Conservatively
 -- invalidate every authorized summary in this ocean, without an age limit.
 PERFORM invalidate_derived_read_surfaces(OLD.graph_id);
 -- Historical sync snapshots cannot retain the deleted content/provenance.
 DELETE FROM op_log WHERE graph_id=OLD.graph_id AND entity_id=OLD.atom_id::text;
 RETURN NULL;
END $$;
CREATE TRIGGER atom_lifecycle_deleted AFTER DELETE ON atoms FOR EACH ROW EXECUTE FUNCTION atom_lifecycle_deleted();
CREATE FUNCTION source_lifecycle_deleted() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
 IF EXISTS(SELECT 1 FROM graphs WHERE id=OLD.graph_id) THEN
  INSERT INTO source_retraction_outbox(graph_id,source_item_id) VALUES(OLD.graph_id,OLD.source_item_id) ON CONFLICT DO NOTHING;
 END IF;
 PERFORM invalidate_derived_read_surfaces(OLD.graph_id);
 RETURN NULL;
END $$;
CREATE TRIGGER source_lifecycle_deleted BEFORE DELETE ON source_items FOR EACH ROW EXECUTE FUNCTION source_lifecycle_deleted();
DO $$ BEGIN
 IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname='coralswarm_app') THEN GRANT SELECT,INSERT,UPDATE,DELETE ON source_retraction_outbox TO coralswarm_app; END IF;
 IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname='coralswarm_tenant_app') THEN GRANT SELECT,INSERT,UPDATE,DELETE ON source_retraction_outbox TO coralswarm_tenant_app; END IF;
END $$;

-- A symmetric conflict candidate is distinct from a reviewed supersession.
CREATE TABLE assertion_disagreements (
 graph_id UUID NOT NULL REFERENCES graphs(id) ON DELETE CASCADE,
 left_node_id UUID NOT NULL REFERENCES nodes(node_id) ON DELETE CASCADE,
 right_node_id UUID NOT NULL REFERENCES nodes(node_id) ON DELETE CASCADE,
 created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 resolved_at TIMESTAMPTZ,
 PRIMARY KEY(graph_id,left_node_id,right_node_id),
 CHECK(left_node_id<right_node_id)
);
ALTER TABLE assertion_disagreements ENABLE ROW LEVEL SECURITY;
CREATE POLICY graph_visibility ON assertion_disagreements
 USING (EXISTS(SELECT 1 FROM graphs g WHERE g.id=assertion_disagreements.graph_id))
 WITH CHECK (EXISTS(SELECT 1 FROM graphs g WHERE g.id=assertion_disagreements.graph_id));
DO $$ BEGIN
 IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname='coralswarm_app') THEN GRANT SELECT,INSERT,UPDATE,DELETE ON assertion_disagreements TO coralswarm_app; END IF;
 IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname='coralswarm_tenant_app') THEN GRANT SELECT,INSERT,UPDATE,DELETE ON assertion_disagreements TO coralswarm_tenant_app; END IF;
END $$;

-- The source-only orphan-pruning service removes node rows after their final
-- semantic/legacy support. Their historical sync snapshots must disappear too.
CREATE FUNCTION evidence_node_deleted() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
 DELETE FROM op_log WHERE graph_id=OLD.graph_id AND entity_id=OLD.node_id::text;
 RETURN NULL;
END $$;
CREATE TRIGGER evidence_node_deleted AFTER DELETE ON nodes FOR EACH ROW EXECUTE FUNCTION evidence_node_deleted();
-- Invalidate synced inferred snapshots immediately; a minimal deletion op
-- reaches existing devices without keeping sensitive derivation payloads.
CREATE FUNCTION lifecycle_edge_sync() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.provenance->>'lifecycle'='unsupported' THEN
  DELETE FROM op_log WHERE graph_id=NEW.graph_id AND entity_id=NEW.edge_id::text;
  INSERT INTO op_log(op_id,entity,entity_id,op_type,payload,version,updated_at,device_id,graph_id)
   VALUES(gen_random_uuid(),'relationship_edge',NEW.edge_id::text,'delete',jsonb_build_object('edge_id',NEW.edge_id,'graph_id',NEW.graph_id),NEW.version,now(),'evidence-lifecycle',NEW.graph_id);
 END IF;
 RETURN NULL;
END $$;
CREATE TRIGGER lifecycle_edge_sync AFTER UPDATE OF status ON relationship_edge
 FOR EACH ROW WHEN(NEW.status='rejected' AND OLD.status<>'rejected') EXECUTE FUNCTION lifecycle_edge_sync();

-- Trusted provider permission loss keeps retained content but invalidates claims
-- whose only current support is restricted. It never reaches another graph.
CREATE FUNCTION source_access_changed() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.access_state='permitted' THEN RETURN NULL; END IF;
 INSERT INTO source_retraction_outbox(graph_id,source_item_id,event_kind) VALUES(NEW.graph_id,NEW.source_item_id,NEW.access_state) ON CONFLICT DO NOTHING;
 UPDATE relationship_edge e SET status='rejected',
  provenance=e.provenance || jsonb_build_object('lifecycle','unsupported','human_review_status',COALESCE(e.provenance->>'human_review_status',e.status)),version=version+1,updated_at=now()
 WHERE e.graph_id=NEW.graph_id AND e.status<>'rejected' AND jsonb_typeof(e.provenance->'atom_ids')='array'
  AND EXISTS(SELECT 1 FROM jsonb_array_elements_text(e.provenance->'atom_ids') ids(id)
   JOIN source_version_chunks c ON c.graph_id=NEW.graph_id AND c.atom_id::text=ids.id
   JOIN source_versions v ON v.version_id=c.source_version_id AND v.graph_id=c.graph_id WHERE v.source_item_id=NEW.source_item_id)
  AND NOT EXISTS(SELECT 1 FROM jsonb_array_elements_text(e.provenance->'atom_ids') ids(id) WHERE atom_support_permitted(NEW.graph_id,ids.id))
  -- Independent confirmed derivation remains valid when direct support loses
  -- permission. Empty/forged/cross-graph dependency sets are not support.
  AND NOT CASE WHEN jsonb_typeof(e.provenance->'source_edge_ids')='array' THEN
   jsonb_array_length(e.provenance->'source_edge_ids')>0 AND NOT EXISTS(
    SELECT 1 FROM jsonb_array_elements_text(e.provenance->'source_edge_ids') ids(id)
    WHERE NOT EXISTS(SELECT 1 FROM relationship_edge p WHERE p.graph_id=NEW.graph_id AND p.edge_id::text=ids.id AND p.status='confirmed'))
   ELSE false END;
 PERFORM reconcile_relationship_lifecycle(NEW.graph_id);
 PERFORM invalidate_derived_read_surfaces(NEW.graph_id);
 RETURN NULL;
END $$;
CREATE TRIGGER source_access_changed AFTER UPDATE OF access_state ON source_items FOR EACH ROW
 WHEN(NEW.access_state IS DISTINCT FROM OLD.access_state) EXECUTE FUNCTION source_access_changed();

-- Tenant default privileges intentionally revoke PUBLIC function execution.
-- All helpers stay SECURITY INVOKER; grants do not bypass graph/member RLS.
DO $$ BEGIN
 IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname='coralswarm_app') THEN GRANT EXECUTE ON FUNCTION atom_support_permitted(UUID,TEXT),reconcile_relationship_lifecycle(UUID),invalidate_derived_read_surfaces(UUID) TO coralswarm_app; END IF;
 IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname='coralswarm_tenant_app') THEN GRANT EXECUTE ON FUNCTION atom_support_permitted(UUID,TEXT),reconcile_relationship_lifecycle(UUID),invalidate_derived_read_surfaces(UUID) TO coralswarm_tenant_app; END IF;
END $$;
