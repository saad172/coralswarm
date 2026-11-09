-- A carried re-capture gets a receipt clock (capture_admission_health
-- unknown_clock, 2026-10-25).
--
-- source_evidence::carries_extraction (#1397) records a byte-identical
-- metadata-only re-capture as `done` at capture and never queues it. It
-- skipped queue_extraction_tx, the only capture path that writes a receipt,
-- so every carried version had none, and clock_coverage counted each one
-- `unknown` forever: 423 on the coralswarm org tenant from 2026-10-20 22:10
-- (403 Slack history re-captures in one hour, then live Composio polls).
-- Capture now inserts a `processed` receipt for a carried version.
--
-- * The observation trigger, not the caller, decides the basis: an INSERT
--   that is already `processed` for a `done` version, with no extraction
--   window of its own, whose bytes and representation match an earlier
--   `done` version of the same item under the same profile, records
--   `carried_forward`. Every other insert still records
--   `unverified_transition`. hold_basis stays write-protected; the app
--   cannot claim a carry the source facts do not show.
-- * `carried_forward` is a new hold_basis value. It is never an eligible lag
--   sample (capture_admission_health::lag reads never_held/standing_admitted
--   only) and is class `carried`, not `unknown`.
--
-- The function body is 0180's (tenant) / 0348's (control) with only the
-- INSERT branch's carried check added. The DDL is identical in both trees
-- (control 0362 is the twin).
ALTER TABLE corpus_capture_admissions DROP CONSTRAINT corpus_capture_admissions_hold_basis_check;
ALTER TABLE corpus_capture_admissions ADD CONSTRAINT corpus_capture_admissions_hold_basis_check CHECK(hold_basis IN
 ('legacy_unknown','never_held','unverified_transition','backlog_policy_excluded','standing_admitted','operator_paused','managed_processing_disabled','processing_off','cloud_processing_disabled','source_removed_or_access_lost','requester_access_lost','daily_budget_exhausted','execution_budget_exhausted','lifecycle_contract_not_ready','credits_exhausted','principal_unavailable','infrastructure_unavailable','managed_processing_killed','carried_forward'));

CREATE OR REPLACE FUNCTION public.corpus_admission_observation_immutable() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE fresh_edge boolean; valid_source boolean;
BEGIN
 IF TG_OP='INSERT' THEN
  NEW.observed_at=clock_timestamp();NEW.processed_at=NULL;NEW.ever_held=false;NEW.hold_basis='unverified_transition';
  IF NOT EXISTS(SELECT 1 FROM public.source_versions v JOIN public.source_items s ON s.source_item_id=v.source_item_id AND s.graph_id=v.graph_id JOIN public.graphs g ON g.id=v.graph_id WHERE v.version_id=NEW.source_version_id AND v.graph_id=NEW.graph_id AND v.source_item_id=NEW.source_item_id AND g.account_id=NEW.account_id) THEN
   RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='receipt source identity unavailable';
  END IF;
  fresh_edge=NEW.state='processed';
  IF fresh_edge AND EXISTS(SELECT 1 FROM public.source_versions v JOIN public.source_versions p ON p.graph_id=v.graph_id AND p.source_item_id=v.source_item_id AND p.version_id<>v.version_id AND (p.captured_at,p.version_id)<(v.captured_at,v.version_id) AND p.content_sha256=v.content_sha256 AND p.representation=v.representation AND p.extraction_version=v.extraction_version AND p.extraction_status='done' AND NOT p.is_legacy_baseline WHERE v.version_id=NEW.source_version_id AND v.graph_id=NEW.graph_id AND v.extraction_version=NEW.extraction_version AND v.extraction_status='done' AND NOT v.is_legacy_baseline AND NOT EXISTS(SELECT 1 FROM public.semantic_atom_windows w WHERE w.graph_id=v.graph_id AND w.source_version_id=v.version_id)) THEN
   NEW.hold_basis='carried_forward';
  END IF;
 ELSE
  IF (NEW.source_version_id,NEW.graph_id,NEW.account_id,NEW.source_item_id,NEW.captured_by) IS DISTINCT FROM (OLD.source_version_id,OLD.graph_id,OLD.account_id,OLD.source_item_id,OLD.captured_by)
   OR (NEW.extraction_version IS DISTINCT FROM OLD.extraction_version AND NOT ((OLD.extraction_version,NEW.extraction_version) IN (('source-atoms-v1','source-atoms-v2'),('source-atoms-v1','source-atoms-v3'),('source-atoms-v2','source-atoms-v3'))
    AND EXISTS(SELECT 1 FROM public.source_versions v WHERE v.version_id=NEW.source_version_id AND v.graph_id=NEW.graph_id AND v.extraction_version=NEW.extraction_version))) THEN
   RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='receipt identity immutable';
  END IF;
  NEW.observed_at=OLD.observed_at;NEW.processed_at=OLD.processed_at;
  fresh_edge=NEW.state='processed' AND OLD.state<>'processed';
  IF current_user='coralswarm_managed_clock' THEN
   NEW.ever_held=OLD.ever_held OR NEW.ever_held;
   IF OLD.hold_basis='legacy_unknown' THEN NEW.hold_basis=OLD.hold_basis; END IF;
  ELSE
   NEW.hold_basis=OLD.hold_basis;NEW.ever_held=OLD.ever_held;
  END IF;
 END IF;
 IF fresh_edge THEN
  SELECT EXISTS(SELECT 1 FROM public.source_versions v JOIN public.source_items s ON s.source_item_id=v.source_item_id AND s.graph_id=v.graph_id JOIN public.graphs g ON g.id=v.graph_id WHERE v.version_id=NEW.source_version_id AND v.graph_id=NEW.graph_id AND v.source_item_id=NEW.source_item_id AND g.account_id=NEW.account_id AND v.extraction_version=NEW.extraction_version AND v.extraction_status='done' AND s.access_state='permitted' AND (NEW.extraction_version<>'selected-passages-v1' OR EXISTS(SELECT 1 FROM public.selected_evidence_publications p WHERE p.target_graph_id=NEW.graph_id AND p.target_source_item_id=NEW.source_item_id AND p.target_source_version_id=NEW.source_version_id AND p.account_id=NEW.account_id AND p.owner_user_id=NEW.captured_by AND p.status='active'))) INTO valid_source;
  IF NOT valid_source THEN RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='processed receipt requires matching done source';END IF;
  IF NEW.processed_at IS NULL THEN NEW.processed_at=clock_timestamp();END IF;
 END IF;
 RETURN NEW;
END $$;
