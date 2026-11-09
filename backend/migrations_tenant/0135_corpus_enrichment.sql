-- Durable bounded inventory; references to vanished evidence remain accounting,
-- never retained source text. Graph deletion still removes the whole ledger.
-- Immutable insertion transaction IDs preserve snapshot membership even when
-- mutable extraction state changes or a transaction commits after run creation.
ALTER TABLE source_versions ADD COLUMN inventory_created_xid xid8 NOT NULL DEFAULT pg_current_xact_id();
ALTER TABLE atoms ADD COLUMN inventory_created_xid xid8 NOT NULL DEFAULT pg_current_xact_id();
ALTER TABLE meeting_transcripts ADD COLUMN inventory_created_xid xid8 NOT NULL DEFAULT pg_current_xact_id();
CREATE FUNCTION corpus_inventory_insert_identity() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
 IF TG_OP='INSERT' THEN NEW.inventory_created_xid=pg_current_xact_id();
 ELSE NEW.inventory_created_xid=OLD.inventory_created_xid;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER corpus_inventory_insert_identity BEFORE INSERT OR UPDATE ON source_versions
 FOR EACH ROW EXECUTE FUNCTION corpus_inventory_insert_identity();
CREATE TRIGGER corpus_inventory_insert_identity BEFORE INSERT OR UPDATE ON atoms
 FOR EACH ROW EXECUTE FUNCTION corpus_inventory_insert_identity();
CREATE TRIGGER corpus_inventory_insert_identity BEFORE INSERT OR UPDATE ON meeting_transcripts
 FOR EACH ROW EXECUTE FUNCTION corpus_inventory_insert_identity();
CREATE TABLE corpus_enrichment_runs (
 run_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
 graph_id UUID NOT NULL REFERENCES graphs(id) ON DELETE CASCADE,
 account_id UUID NOT NULL,
 requested_by UUID NOT NULL,
 mode TEXT NOT NULL CHECK(mode IN ('dry_run','execute')),
 corpus_kind TEXT NOT NULL DEFAULT 'real' CHECK(corpus_kind IN ('real','synthetic_demo')),
 status TEXT NOT NULL CHECK(status IN ('inventory','ready','running','paused','complete')),
 extraction_version TEXT NOT NULL CHECK(extraction_version IN ('source-atoms-v1','meeting-atoms-v1')),
 model TEXT NOT NULL,
 cohort_cutoff TIMESTAMPTZ NOT NULL DEFAULT now(),
 cohort_snapshot TEXT NOT NULL DEFAULT pg_current_snapshot()::text,
 stage SMALLINT NOT NULL DEFAULT 0 CHECK(stage BETWEEN 0 AND 4),
 cursor_time TIMESTAMPTZ,
 cursor_id UUID,
 max_items BIGINT NOT NULL CHECK(max_items BETWEEN 1 AND 1000),
 max_characters BIGINT NOT NULL CHECK(max_characters BETWEEN 1 AND 8000000),
 max_model_calls BIGINT NOT NULL CHECK(max_model_calls BETWEEN 1 AND 10000),
 admitted_items BIGINT NOT NULL DEFAULT 0,
 attempted_characters BIGINT NOT NULL DEFAULT 0,
 attempted_model_calls BIGINT NOT NULL DEFAULT 0,
 pause_reason TEXT,
 local_holdings_declared BOOLEAN NOT NULL DEFAULT true,
 local_inventory_reason TEXT NOT NULL DEFAULT 'local_inventory_not_attested_to_graph'
   CHECK(local_inventory_reason IN ('local_inventory_not_attested_to_graph','device_key_read_timeout_15s_no_retry_or_upload')),
 created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 UNIQUE(run_id,graph_id)
);
CREATE TABLE corpus_enrichment_units (
 unit_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
 run_id UUID NOT NULL,
 graph_id UUID NOT NULL,
 kind TEXT NOT NULL CHECK(kind IN ('retained_version','legacy_chunk','retained_transcript','local_holdings')),
 holding_id UUID NOT NULL,
 source_item_id UUID,
 extraction_version TEXT,
 state TEXT NOT NULL CHECK(state IN ('pending','queued','processed','excluded','unavailable','failed')),
 original_availability TEXT NOT NULL DEFAULT 'unknown'
   CHECK(original_availability IN ('available','unavailable','unknown')),
 processing_path TEXT NOT NULL DEFAULT 'retained_version'
   CHECK(processing_path IN ('retained_version','legacy_normalized_baseline_pending','retained_transcript_capture_pending','covered_by_retained_version','local_unavailable')),
 reason TEXT,
 execution_owner BOOLEAN NOT NULL DEFAULT false,
 attempted_characters BIGINT NOT NULL DEFAULT 0,
 attempted_model_calls BIGINT NOT NULL DEFAULT 0,
 observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 FOREIGN KEY(run_id,graph_id) REFERENCES corpus_enrichment_runs(run_id,graph_id) ON DELETE CASCADE,
 UNIQUE(run_id,kind,holding_id),
 CHECK(NOT execution_owner OR (kind='retained_version' AND extraction_version IS NOT NULL))
);
-- One executor per immutable version/profile, including retries and continuations.
CREATE UNIQUE INDEX corpus_enrichment_version_owner
 ON corpus_enrichment_units(graph_id,holding_id,extraction_version) WHERE execution_owner;
CREATE INDEX corpus_enrichment_units_run ON corpus_enrichment_units(run_id,state,unit_id);
ALTER TABLE corpus_enrichment_runs ENABLE ROW LEVEL SECURITY;
ALTER TABLE corpus_enrichment_units ENABLE ROW LEVEL SECURITY;
CREATE POLICY member_ocean_isolation ON corpus_enrichment_runs USING (graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids())) WITH CHECK (graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids()));
CREATE POLICY member_ocean_isolation ON corpus_enrichment_units USING (graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids())) WITH CHECK (graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids()));

-- App roles deliberately cannot read SQLx bookkeeping. Expose only this
-- non-content readiness bit; never grant direct ledger access.
CREATE FUNCTION corpus_lifecycle_delete_repair_ready() RETURNS BOOLEAN
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog, public
AS $$ SELECT EXISTS(SELECT 1 FROM public._sqlx_migrations WHERE success AND version=CASE WHEN to_regclass('public.accounts') IS NULL THEN 134 ELSE 299 END) $$;
REVOKE ALL ON FUNCTION corpus_lifecycle_delete_repair_ready() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION corpus_lifecycle_delete_repair_ready() TO coralswarm_tenant_app;
