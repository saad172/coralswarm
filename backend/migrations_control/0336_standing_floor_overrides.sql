-- PAIRED WITH: migrations_tenant/0169_standing_floor_overrides.sql.
--
-- Explicit, per-version exceptions to standing extraction's backlog floor.
--
-- Standing processes captures from 2026-10-13 on (`STANDING_ELIGIBLE` in
-- managed/standing/admission.rs), plus explicit v1 -> v2 re-extraction of an
-- earlier capture that has v1 windows. A failed capture from before the floor
-- with no v1 window is never processed. The founder chose a contained one-off
-- for those (2026-10-19), not a lower floor: an operator lists the versions
-- (`retry_failed_extractions_v2 --before-floor`), and each listed version gets
-- one row here with the reason. `STANDING_ELIGIBLE` admits a version that has
-- a row for its current profile. Nothing else changes: every other pre-floor
-- capture stays outside standing, and an admitted version still goes through
-- the managed gate, its caps, receipts, egress policy and access checks.
--
-- Append-only audit: the runtime roles can only read it. Rows are written by
-- the operator tool as the table owner (tenant owner DSN /
-- MIGRATOR_DATABASE_URL), and an UPDATE is refused even for the owner. A row
-- carries no source text, provider identifier or blob locator. Rows go with
-- their graph (ON DELETE CASCADE); a purged version leaves an inert row that
-- matches nothing.
--
-- The shared block is byte-identical on both planes; only the RLS policy at
-- the end differs. Production tenant runtimes connect as the table OWNER
-- (owner bypass, no FORCE) and inherit coralswarm_tenant_app since 0159.

CREATE TABLE standing_floor_overrides (
  source_version_id UUID PRIMARY KEY,
  graph_id UUID NOT NULL REFERENCES graphs(id) ON DELETE CASCADE,
  extraction_version TEXT NOT NULL CHECK (extraction_version = 'source-atoms-v2'),
  reason TEXT NOT NULL CHECK (char_length(btrim(reason)) BETWEEN 8 AND 300),
  granted_by TEXT NOT NULL CHECK (char_length(btrim(granted_by)) BETWEEN 1 AND 100),
  granted_via TEXT NOT NULL DEFAULT 'retry_failed_extractions_v2'
    CHECK (granted_via = 'retry_failed_extractions_v2'),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX standing_floor_overrides_graph ON standing_floor_overrides(graph_id, source_version_id);

CREATE FUNCTION standing_floor_override_append_only() RETURNS trigger
 LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
 RAISE EXCEPTION USING ERRCODE='23514', MESSAGE='standing floor overrides are append-only';
END $$;
REVOKE ALL ON FUNCTION standing_floor_override_append_only() FROM PUBLIC;
CREATE TRIGGER standing_floor_overrides_append_only BEFORE UPDATE ON standing_floor_overrides
 FOR EACH ROW EXECUTE FUNCTION standing_floor_override_append_only();

ALTER TABLE standing_floor_overrides ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON standing_floor_overrides FROM PUBLIC;
DO $$ BEGIN
 IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'coralswarm_app') THEN
  GRANT SELECT ON standing_floor_overrides TO coralswarm_app;
 END IF;
 IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'coralswarm_tenant_app') THEN
  GRANT SELECT ON standing_floor_overrides TO coralswarm_tenant_app;
 END IF;
END $$;

-- Control plane: one account per transaction (app.account_id), the same
-- graph-anchored policy as corpus_capture_admissions (0307).
CREATE POLICY account_isolation ON standing_floor_overrides FOR SELECT
  USING (graph_id IN (SELECT id FROM graphs WHERE account_id = current_setting('app.account_id', true)::uuid));
