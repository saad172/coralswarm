-- PAIRED WITH: tenant 0237 (read_model_immediate_due).
-- Earliest unresolved immediate enqueue, not the artificially backdated dirty
-- deadline. NULL for legacy/trigger-only work; never invent a queue duration.
ALTER TABLE read_models ADD COLUMN immediate_due_at timestamptz;

-- Also clear the episode for a rolling older binary, whose publication SQL
-- does not know this column. No query or privilege elevation in this trigger.
CREATE FUNCTION read_model_clear_immediate_due() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $$
BEGIN
 NEW.immediate_due_at := NULL;
 RETURN NEW;
END $$;
REVOKE ALL ON FUNCTION read_model_clear_immediate_due() FROM PUBLIC;
CREATE TRIGGER read_model_clear_immediate_due
 BEFORE UPDATE ON read_models FOR EACH ROW
 WHEN (NEW.dirty_at IS NULL AND NEW.immediate_due_at IS NOT NULL)
 EXECUTE FUNCTION read_model_clear_immediate_due();
