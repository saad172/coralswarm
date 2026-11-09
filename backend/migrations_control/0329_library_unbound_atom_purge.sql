-- Library restriction lifecycle: unbound atoms follow their source item.
--
-- 0161 (control 0328) maps an atom with source_evidence_bound=false to the
-- source item in the same graph with the same source_type whose source_id is
-- the atom's source_id with one trailing ':chunk:<digits>' removed, and hides
-- it from reads while any such item is not permitted. Two write paths still
-- considered only chunk-bound atoms (source_version_chunks rows):
--
-- 1. The restriction/deletion purge (0143, latest library_capture_purge in
--    0151). An item event retracted and purged only the item's bound atoms.
--    Unbound atoms of the item stayed in the table, and became readable again
--    when the item was re-permitted (or deleted: no item, no gate).
-- 2. atom_support_permitted (0129), which drives edge rejection in
--    source_access_changed and reconcile_relationship_lifecycle. An edge
--    supported only by a restricted item's unbound atoms stayed accepted.
--
-- Both now use the 0161 mapping, with the same semantics as bound atoms:
--
-- * An item event (deleted / restricted / unknown, from source_items or a
--   source_versions delete) records the item's unbound atoms as 'atom'
--   dependencies of the same pending purge event before library_capture_purge
--   runs. library_capture_purge then treats them exactly like bound atoms:
--   promoted/connection copies, node dependencies and review snapshots,
--   immediate evidence_retracted (monotonic), job/session/answer
--   invalidation; the drain deletes the atoms, expands node dependencies,
--   queues DeleteSourceBlob for their content blobs and rewrites edges.
--   Re-permitting the item therefore restores neither bound nor unbound
--   atoms: both are permanently retracted and come back only through a new
--   capture. (0161's read gate alone briefly made unbound atoms reappear on
--   re-permit; that inconsistency is removed.)
-- * The mapping is fail closed like 0161: an atom whose mapped base matches
--   the item (any provider namespace) is purged with it.
-- * library_capture_purge itself is not redefined. The trigger functions call
--   a new wrapper that upserts the same pending event (the unique pending
--   index guarantees library_capture_purge reuses it) and adds the unbound
--   atoms first, so one pass handles both kinds and later redefinitions of
--   library_capture_purge keep working unchanged.
-- * The item -> atoms direction (library_item_unbound_atoms) is an index
--   range scan on the existing idx_atoms_graph_source_id_pattern
--   (graph_id, source_id text_pattern_ops): the exact base plus the byte
--   range [base||':chunk:', base||':chunk;').
--   The atom -> item direction (atom_support_permitted) reuses 0161's partial
--   index source_items_denied_identity.
-- * atom_support_permitted now matches its id through the atoms primary key
--   instead of comparing atom_id::text per row. Only canonical lowercase UUID
--   text can equal atom_id::text, so accepting exactly that form and casting
--   it is equivalent, and malformed ids still return false without error.
-- * source_access_changed computes the item's atom ids once (chunk rows plus
--   the unbound helper) and selects edges with a single jsonb ?| membership
--   test, checking support only for those edges. The 0129 version let the
--   planner run atom_support_permitted (a per-row atom_id::text scan) for
--   every edge in the graph: about 6 s per restricted item with 5k edges.
--   The membership test is exactly the old per-element text equality.
--
-- Function attributes are preserved: the signal functions stay SECURITY
-- DEFINER with search_path pg_catalog,public; atom_support_permitted and
-- source_access_changed stay SECURITY INVOKER with no pinned search_path (their
-- bodies are now schema-qualified). CREATE OR REPLACE keeps owners and grants.

-- The item -> unbound atoms direction of the 0161 mapping, shared by the purge
-- and edge rejection. SECURITY INVOKER (RLS applies to app-role callers) and
-- no SET clause, so the planner can inline it with the caller's parameters.
CREATE FUNCTION public.library_item_unbound_atoms(g UUID,st TEXT,src TEXT) RETURNS TABLE(atom_id UUID) LANGUAGE SQL STABLE AS $$
 SELECT a.atom_id FROM public.atoms a
 WHERE a.graph_id=g AND a.source_type=st AND NOT a.source_evidence_bound
  -- An atom whose own id already carries a chunk suffix maps to its base,
  -- so the exact match applies only to a suffix-free item id (0161 CASE).
  AND ((a.source_id=src AND NOT (right(rtrim(src,'0123456789'),7)=':chunk:' AND rtrim(src,'0123456789')<>src))
   OR (a.source_id ~>=~ (src||':chunk:') AND a.source_id ~<~ (src||':chunk;')
    AND rtrim(a.source_id,'0123456789')=src||':chunk:' AND a.source_id<>src||':chunk:'))
$$;
REVOKE ALL ON FUNCTION public.library_item_unbound_atoms(UUID,TEXT,TEXT) FROM PUBLIC;
DO $$ DECLARE r TEXT;BEGIN FOREACH r IN ARRAY ARRAY['coralswarm_app','coralswarm_tenant_app'] LOOP IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname=r) THEN EXECUTE format('GRANT EXECUTE ON FUNCTION public.library_item_unbound_atoms(UUID,TEXT,TEXT) TO %I',r);END IF;END LOOP;END $$;

CREATE FUNCTION public.library_capture_item_purge(g UUID,s_id UUID,k TEXT) RETURNS UUID
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE ev UUID;acc UUID;owner UUID;st TEXT;src TEXT;
BEGIN
 SELECT account_id,owner_user_id INTO acc,owner FROM public.graphs WHERE id=g;
 IF FOUND AND s_id<>'00000000-0000-0000-0000-000000000000' THEN
  SELECT source_type,source_id INTO st,src FROM public.source_items WHERE graph_id=g AND source_item_id=s_id;
  IF FOUND THEN
   -- Same upsert as library_capture_purge, which then returns this event.
   INSERT INTO public.source_purge_events(account_id,owner_user_id,graph_id,source_item_id,event_kind) VALUES(acc,owner,g,s_id,k)
   ON CONFLICT(graph_id,source_item_id,event_kind) WHERE sql_processed_at IS NULL DO UPDATE SET account_id=EXCLUDED.account_id RETURNING event_id INTO ev;
   INSERT INTO public.source_purge_dependencies(event_id,kind,graph_id,object_id)
   SELECT ev,'atom',g,u.atom_id FROM public.library_item_unbound_atoms(g,st,src) u
   ON CONFLICT DO NOTHING;
  END IF;
 END IF;
 RETURN public.library_capture_purge(g,s_id,k);
END $$;
REVOKE ALL ON FUNCTION public.library_capture_item_purge(UUID,UUID,TEXT) FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.library_source_purge_signal() RETURNS trigger
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public AS $$
BEGIN
 IF TG_OP='DELETE' THEN PERFORM public.library_capture_item_purge(OLD.graph_id,OLD.source_item_id,'deleted'); RETURN OLD; END IF;
 IF NEW.access_state<>'permitted' AND NEW.access_state IS DISTINCT FROM OLD.access_state THEN PERFORM public.library_capture_item_purge(OLD.graph_id,OLD.source_item_id,NEW.access_state); END IF;
 RETURN NEW;
END $$;

CREATE OR REPLACE FUNCTION public.library_version_purge_signal() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public AS $$ BEGIN
 IF pg_trigger_depth()=1 THEN PERFORM public.library_capture_item_purge(OLD.graph_id,OLD.source_item_id,'deleted');END IF;RETURN OLD;END $$;

-- Direct support is permitted only when its mapped source is still readable:
-- the chunk-mapped items (bound) and, for an unbound atom, the 0161 item.
CREATE OR REPLACE FUNCTION public.atom_support_permitted(g UUID,support_id TEXT) RETURNS BOOLEAN LANGUAGE SQL STABLE AS $$
 SELECT EXISTS(SELECT 1 FROM public.atoms a
  WHERE a.atom_id=CASE WHEN support_id COLLATE "C" ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN support_id::uuid END
   AND a.graph_id=g
   AND NOT EXISTS(SELECT 1 FROM public.source_version_chunks c JOIN public.source_versions v ON v.version_id=c.source_version_id AND v.graph_id=c.graph_id JOIN public.source_items s ON s.source_item_id=v.source_item_id AND s.graph_id=v.graph_id WHERE c.graph_id=g AND c.atom_id=a.atom_id AND s.access_state<>'permitted')
   AND NOT EXISTS(SELECT 1 FROM public.source_items s
    WHERE NOT a.source_evidence_bound AND s.graph_id=g AND s.access_state<>'permitted' AND s.source_type=a.source_type
     AND s.source_id=CASE WHEN right(rtrim(a.source_id,'0123456789'),7)=':chunk:' AND rtrim(a.source_id,'0123456789')<>a.source_id
      THEN left(rtrim(a.source_id,'0123456789'),-7) ELSE a.source_id END))
$$;

-- Trusted provider permission loss keeps retained content but invalidates claims
-- whose only current support is restricted. It never reaches another graph.
-- An edge is in scope when one of its atoms belongs to the item, bound through
-- a chunk row or unbound through the 0161 mapping.
CREATE OR REPLACE FUNCTION public.source_access_changed() RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE item_atoms TEXT[];
BEGIN
 IF NEW.access_state='permitted' THEN RETURN NULL; END IF;
 INSERT INTO public.source_retraction_outbox(graph_id,source_item_id,event_kind) VALUES(NEW.graph_id,NEW.source_item_id,NEW.access_state) ON CONFLICT DO NOTHING;
 -- The item's atoms, once: bound through chunk rows, unbound through 0161.
 SELECT array_agg(x.atom_id::text) INTO item_atoms FROM (
  SELECT c.atom_id FROM public.source_versions v JOIN public.source_version_chunks c ON c.graph_id=v.graph_id AND c.source_version_id=v.version_id
  WHERE v.graph_id=NEW.graph_id AND v.source_item_id=NEW.source_item_id
  UNION SELECT u.atom_id FROM public.library_item_unbound_atoms(NEW.graph_id,NEW.source_type,NEW.source_id) u) x;
 IF item_atoms IS NOT NULL THEN
  UPDATE public.relationship_edge e SET status='rejected',
   provenance=e.provenance || jsonb_build_object('lifecycle','unsupported','human_review_status',COALESCE(e.provenance->>'human_review_status',e.status)),version=version+1,updated_at=now()
  WHERE e.graph_id=NEW.graph_id AND e.status<>'rejected' AND jsonb_typeof(e.provenance->'atom_ids')='array'
   -- CASE fixes the evaluation order: the cheap membership test runs for
   -- every edge, the support check only for edges that cite the item.
   AND CASE WHEN e.provenance->'atom_ids' ?| item_atoms
    THEN NOT EXISTS(SELECT 1 FROM jsonb_array_elements_text(e.provenance->'atom_ids') ids(id) WHERE public.atom_support_permitted(NEW.graph_id,ids.id))
    ELSE false END
   -- Independent confirmed derivation remains valid when direct support loses
   -- permission. Empty/forged/cross-graph dependency sets are not support.
   AND NOT CASE WHEN jsonb_typeof(e.provenance->'source_edge_ids')='array' THEN
    jsonb_array_length(e.provenance->'source_edge_ids')>0 AND NOT EXISTS(
     SELECT 1 FROM jsonb_array_elements_text(e.provenance->'source_edge_ids') ids(id)
     WHERE NOT EXISTS(SELECT 1 FROM public.relationship_edge p WHERE p.graph_id=NEW.graph_id AND p.edge_id::text=ids.id AND p.status='confirmed'))
    ELSE false END;
 END IF;
 PERFORM public.reconcile_relationship_lifecycle(NEW.graph_id);
 PERFORM public.invalidate_derived_read_surfaces(NEW.graph_id);
 RETURN NULL;
END $$;
