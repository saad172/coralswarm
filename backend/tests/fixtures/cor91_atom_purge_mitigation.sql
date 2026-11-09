BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';
-- Refuse to run on a plane that already has the COR-91 fix migration (it would regress it).
DO $$ BEGIN
 IF (SELECT max(version) FROM public._sqlx_migrations) > (CASE WHEN to_regclass('public.tenant_meta') IS NOT NULL THEN 146 ELSE 312 END)
 THEN RAISE EXCEPTION 'COR-91 fix migration already applied here; do not run the mitigation'; END IF;
END $$;
CREATE OR REPLACE FUNCTION public.library_atom_purge_signal() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public AS $$
DECLARE ev UUID;acc UUID;owner UUID;
BEGIN
 IF pg_trigger_depth()>1 OR EXISTS(SELECT 1 FROM public.source_purge_dependencies WHERE kind='atom' AND graph_id=OLD.graph_id AND object_id=OLD.atom_id AND processed_at IS NULL) THEN RETURN OLD;END IF;
 IF OLD.content_blob_key IS NULL OR NOT (NOT OLD.derived_copy_bound OR split_part(OLD.content_blob_key,'/',GREATEST(1,array_length(string_to_array(OLD.content_blob_key,'/'),1)-1))=OLD.graph_id::text) THEN RETURN OLD;END IF;
 SELECT account_id,owner_user_id INTO acc,owner FROM public.graphs WHERE id=OLD.graph_id;
 IF NOT FOUND THEN RETURN OLD;END IF;
 INSERT INTO public.source_purge_events(account_id,owner_user_id,graph_id,source_item_id,event_kind) VALUES(acc,owner,OLD.graph_id,'00000000-0000-0000-0000-000000000000','retracted')
 ON CONFLICT(graph_id,source_item_id,event_kind) WHERE sql_processed_at IS NULL DO UPDATE SET account_id=EXCLUDED.account_id RETURNING event_id INTO ev;
 INSERT INTO public.source_purge_dependencies(event_id,kind,graph_id,object_id,blob_key) VALUES(ev,'blob',OLD.graph_id,OLD.atom_id,OLD.content_blob_key) ON CONFLICT DO NOTHING;
 RETURN OLD;
END $$;
CREATE OR REPLACE FUNCTION public.atom_lifecycle_deleted() RETURNS trigger LANGUAGE plpgsql AS $$
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
 DELETE FROM op_log WHERE graph_id=OLD.graph_id AND entity_id=OLD.atom_id::text;
 RETURN NULL;
END $$;
SELECT 'atom_purge_signal_calls_capture' AS check, pg_get_functiondef('public.library_atom_purge_signal'::regproc) ~ 'library_capture_purge' AS value
UNION ALL SELECT 'atom_lifecycle_invalidates', pg_get_functiondef('public.atom_lifecycle_deleted'::regproc) ~ 'invalidate_derived_read_surfaces'
UNION ALL SELECT 'atom_purge_delete_enabled', (SELECT tgenabled='O' FROM pg_trigger WHERE tgname='atom_purge_delete' AND tgrelid='public.atoms'::regclass)
UNION ALL SELECT 'atom_lifecycle_deleted_enabled', (SELECT tgenabled='O' FROM pg_trigger WHERE tgname='atom_lifecycle_deleted' AND tgrelid='public.atoms'::regclass);
COMMIT;
