\set ON_ERROR_STOP on
BEGIN;
SET LOCAL lock_timeout = '5s';
-- This restores the pre-stop-gap bodies, which purge the whole graph on every
-- atom delete. Once the COR-91 fix (tenant 0148 / control 0317) is applied it
-- would undo that fix, so refuse.
DO $$
BEGIN
 IF to_regprocedure('public.journal_tz_ok(text)') IS NOT NULL THEN
  RAISE EXCEPTION 'COR-91 rollback refused: the fix migration (atom purge scoped to atom) is applied on this plane';
 END IF;
 IF to_regclass('public._sqlx_migrations') IS NOT NULL THEN
  IF EXISTS(SELECT 1 FROM public._sqlx_migrations WHERE description='atom purge scoped to atom') THEN
   RAISE EXCEPTION 'COR-91 rollback refused: the fix migration (atom purge scoped to atom) is recorded on this plane';
  END IF;
 END IF;
END $$;
CREATE OR REPLACE FUNCTION public.library_atom_purge_signal()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'public'
AS $function$
DECLARE ev UUID;
BEGIN
 IF pg_trigger_depth()>1 OR EXISTS(SELECT 1 FROM public.source_purge_dependencies WHERE kind='atom' AND graph_id=OLD.graph_id AND object_id=OLD.atom_id AND processed_at IS NULL) THEN RETURN OLD;END IF;
 ev:=public.library_capture_purge(OLD.graph_id,'00000000-0000-0000-0000-000000000000','retracted',OLD.atom_id);
 IF ev IS NULL THEN RETURN OLD;END IF;
 INSERT INTO public.source_purge_dependencies(event_id,kind,graph_id,object_id) VALUES(ev,'atom',OLD.graph_id,OLD.atom_id) ON CONFLICT DO NOTHING;
 IF OLD.content_blob_key IS NOT NULL AND (NOT OLD.derived_copy_bound OR split_part(OLD.content_blob_key,'/',GREATEST(1,array_length(string_to_array(OLD.content_blob_key,'/'),1)-1))=OLD.graph_id::text) THEN
  INSERT INTO public.source_purge_dependencies(event_id,kind,graph_id,object_id,blob_key) VALUES(ev,'blob',OLD.graph_id,OLD.atom_id,OLD.content_blob_key) ON CONFLICT DO NOTHING;
 END IF;
 PERFORM public.library_capture_purge(OLD.graph_id,'00000000-0000-0000-0000-000000000000','retracted',OLD.atom_id);
 RETURN OLD;
END $function$
;
CREATE OR REPLACE FUNCTION public.atom_lifecycle_deleted()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
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
END $function$
;
COMMIT;
