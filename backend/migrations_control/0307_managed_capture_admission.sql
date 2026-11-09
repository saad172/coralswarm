
-- Canonical set admission: one role/RLS-aware root join, not one permission
-- subquery per chunk. The optional exact IDs keep scalar checks cheap.
CREATE FUNCTION library_permitted_atoms(g UUID, atom_ids UUID[] DEFAULT NULL)
RETURNS TABLE(atom_id UUID) LANGUAGE SQL STABLE AS $$
 WITH mapped AS MATERIALIZED (
  SELECT c.atom_id,bool_or(s.access_state='permitted') permitted,
         bool_or(s.access_state<>'permitted') denied
  FROM source_version_chunks c
  JOIN source_versions v ON v.version_id=c.source_version_id AND v.graph_id=c.graph_id
  JOIN source_items s ON s.source_item_id=v.source_item_id AND s.graph_id=v.graph_id
  WHERE c.graph_id=g AND (atom_ids IS NULL OR c.atom_id=ANY(atom_ids))
  GROUP BY c.atom_id
 )
 SELECT a.atom_id FROM atoms a LEFT JOIN mapped m ON m.atom_id=a.atom_id
 WHERE a.graph_id=g AND (atom_ids IS NULL OR a.atom_id=ANY(atom_ids))
  AND (NOT a.source_evidence_bound OR COALESCE(m.permitted,false))
  AND NOT COALESCE(m.denied,false)
$$;
CREATE OR REPLACE FUNCTION library_atom_permitted(g UUID,a_id UUID) RETURNS BOOLEAN
 LANGUAGE SQL STABLE AS $$
 SELECT EXISTS(SELECT 1 FROM library_permitted_atoms(g,ARRAY[a_id]))
$$;
REVOKE ALL ON FUNCTION library_permitted_atoms(UUID,UUID[]) FROM PUBLIC;
DO $$ BEGIN
 IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname='coralswarm_app') THEN
  GRANT EXECUTE ON FUNCTION library_permitted_atoms(UUID,UUID[]) TO coralswarm_app;
 END IF;
 IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname='coralswarm_tenant_app') THEN
  GRANT EXECUTE ON FUNCTION library_permitted_atoms(UUID,UUID[]) TO coralswarm_tenant_app;
 END IF;
END $$;
-- P10: capture is retained evidence, not an automatic model authorization.
-- Minimal admission receipts survive source retraction for internal accounting;
-- no provider IDs/text/blob locators are retained here. Public reads gate source.
CREATE TABLE corpus_capture_admissions (
 source_version_id UUID PRIMARY KEY,
 graph_id UUID NOT NULL REFERENCES graphs(id) ON DELETE CASCADE,
 account_id UUID NOT NULL, source_item_id UUID NOT NULL,
 captured_by UUID, extraction_version TEXT NOT NULL,
 run_id UUID, unit_id UUID,
 state TEXT NOT NULL CHECK(state IN ('blocked','queued','processed','excluded','unavailable','failed')),
 reason TEXT, observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 CHECK((run_id IS NULL)=(unit_id IS NULL))
);
CREATE INDEX corpus_capture_admissions_graph ON corpus_capture_admissions(graph_id,state,source_version_id);
ALTER TABLE corpus_capture_admissions ENABLE ROW LEVEL SECURITY;
CREATE POLICY account_isolation ON corpus_capture_admissions USING(graph_id IN(SELECT id FROM graphs WHERE account_id=current_setting('app.account_id',true)::uuid)) WITH CHECK(graph_id IN(SELECT id FROM graphs WHERE account_id=current_setting('app.account_id',true)::uuid));
INSERT INTO corpus_capture_admissions(source_version_id,graph_id,account_id,source_item_id,captured_by,extraction_version,state,reason)
 SELECT v.version_id,v.graph_id,g.account_id,v.source_item_id,v.ingested_by,v.extraction_version,
 CASE WHEN v.extraction_status='done' THEN 'processed' ELSE 'blocked' END,
 CASE WHEN v.extraction_status='done' THEN 'pre_controller_completed_evidence' ELSE 'managed_admission_required' END
 FROM source_versions v JOIN graphs g ON g.id=v.graph_id;
ALTER TABLE corpus_enrichment_units ADD COLUMN promoted_version_id UUID;
ALTER TABLE corpus_enrichment_units ADD COLUMN promotion_fingerprint TEXT;
ALTER TABLE corpus_enrichment_units ADD CONSTRAINT corpus_promotion_identity CHECK((promoted_version_id IS NULL AND promotion_fingerprint IS NULL) OR (kind IN('legacy_chunk','retained_transcript') AND promoted_version_id IS NOT NULL AND promotion_fingerprint ~ '^[0-9a-f]{64}$'));
-- A promoted representation is an explicit derivative of a pinned inventoried
-- holding, not a newly discovered provider source or an invented original.
CREATE INDEX corpus_enrichment_promoted_version ON corpus_enrichment_units(graph_id,promoted_version_id) WHERE promoted_version_id IS NOT NULL;

DO $$ DECLARE c TEXT; BEGIN
 FOR c IN SELECT conname FROM pg_constraint WHERE conrelid='corpus_enrichment_units'::regclass AND contype='c' AND pg_get_constraintdef(oid) LIKE '%execution_owner%' LOOP
  EXECUTE format('ALTER TABLE corpus_enrichment_units DROP CONSTRAINT %I',c);
 END LOOP;
END $$;
ALTER TABLE corpus_enrichment_units ADD CONSTRAINT corpus_execution_owner_shape CHECK(NOT execution_owner OR (extraction_version IS NOT NULL AND (kind='retained_version' OR (kind IN('legacy_chunk','retained_transcript') AND promoted_version_id IS NOT NULL))));
DROP INDEX corpus_enrichment_version_owner;
CREATE UNIQUE INDEX corpus_enrichment_version_owner ON corpus_enrichment_units(graph_id,COALESCE(promoted_version_id,holding_id),extraction_version) WHERE execution_owner;

CREATE INDEX corpus_enrichment_units_page ON corpus_enrichment_units(run_id,graph_id,unit_id DESC);
CREATE INDEX corpus_atoms_snapshot_page ON atoms(graph_id,created_at,atom_id);

-- No legacy queue producer (native embedding/repair, direct enqueue or batch)
-- can create dispatchable paid work without a compatible managed purpose.
-- P8 must restore these consumers with typed receipts and a budget, explicitly.
CREATE FUNCTION corpus_legacy_purpose_admission() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
 IF NEW.kind IN ('ReembedOutdatedChunks','GenerateMeetingSummary','ExtractEntities','ExtractRelationships','IngestDoor','RegenerateDigest','EntityResolution','ConflictDetection','InferExpertise','ReevaluateKind','BackfillAtomTitles','InferRelationships','ExtractThreads','ExtractDecisions') AND NEW.status::text IN ('pending','running','failed') THEN
  NEW.status='dead'; NEW.finished_at=now();
  NEW.error_message='managed_purpose_unsupported';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER corpus_legacy_purpose_admission BEFORE INSERT OR UPDATE OF kind,status ON jobs
 FOR EACH ROW EXECUTE FUNCTION corpus_legacy_purpose_admission();
UPDATE jobs SET status='dead',finished_at=now(),error_message='managed_purpose_unsupported'
 WHERE kind IN ('ReembedOutdatedChunks','GenerateMeetingSummary','ExtractEntities','ExtractRelationships','IngestDoor','RegenerateDigest','EntityResolution','ConflictDetection','InferExpertise','ReevaluateKind','BackfillAtomTitles','InferRelationships','ExtractThreads','ExtractDecisions') AND status IN ('pending','running','failed');
UPDATE meeting_transcripts SET summary_status='failed',summary_error='managed_purpose_unsupported',summary_updated_at=now()
 WHERE summary_status IN ('pending','running');

CREATE FUNCTION corpus_managed_identity_immutable() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
 IF TG_TABLE_NAME='corpus_enrichment_runs' THEN
  IF (NEW.graph_id,NEW.account_id,NEW.requested_by,NEW.mode,NEW.corpus_kind,NEW.extraction_version,NEW.model,NEW.cohort_cutoff,NEW.cohort_snapshot)
   IS DISTINCT FROM (OLD.graph_id,OLD.account_id,OLD.requested_by,OLD.mode,OLD.corpus_kind,OLD.extraction_version,OLD.model,OLD.cohort_cutoff,OLD.cohort_snapshot) THEN
   RAISE EXCEPTION 'managed run identity/model/cohort is immutable';
  END IF;
 ELSE
  IF (NEW.run_id,NEW.graph_id,NEW.kind,NEW.holding_id) IS DISTINCT FROM (OLD.run_id,OLD.graph_id,OLD.kind,OLD.holding_id)
   OR (OLD.promoted_version_id IS NOT NULL AND (NEW.promoted_version_id,NEW.promotion_fingerprint,NEW.source_item_id,NEW.extraction_version) IS DISTINCT FROM (OLD.promoted_version_id,OLD.promotion_fingerprint,OLD.source_item_id,OLD.extraction_version))
   OR (OLD.execution_owner AND NOT NEW.execution_owner) THEN
   RAISE EXCEPTION 'managed holding/promotion/owner is immutable';
  END IF;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER corpus_managed_identity_immutable BEFORE UPDATE ON corpus_enrichment_runs
 FOR EACH ROW EXECUTE FUNCTION corpus_managed_identity_immutable();
CREATE TRIGGER corpus_managed_identity_immutable BEFORE UPDATE ON corpus_enrichment_units
 FOR EACH ROW EXECUTE FUNCTION corpus_managed_identity_immutable();
