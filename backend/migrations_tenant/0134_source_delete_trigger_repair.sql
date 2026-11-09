-- Fix forward after 0129: a BEFORE DELETE trigger must return OLD to allow
-- deletion. RETURN NULL emits the audit event but suppresses the actual row
-- operation and its evidence FK cascades. Preserve applied migration checksums.
CREATE OR REPLACE FUNCTION source_lifecycle_deleted() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
 IF EXISTS(SELECT 1 FROM graphs WHERE id=OLD.graph_id) THEN
  INSERT INTO source_retraction_outbox(graph_id,source_item_id) VALUES(OLD.graph_id,OLD.source_item_id) ON CONFLICT DO NOTHING;
 END IF;
 PERFORM invalidate_derived_read_surfaces(OLD.graph_id);
 RETURN OLD;
END $$;
