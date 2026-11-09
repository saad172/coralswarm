-- PAIRED WITH: control 0348 (extraction_v3).
-- Extraction v3: change statements, resolved dates and single-valued
-- relationships (source-atoms-v3). Paired tenant/control migrations with
-- byte-identical DDL; only the header names the pair.
--
-- 1. A stated change ("X previously did A, now does B") on a semantic atom.
--    A JSON array of validated changes written by extraction v3
--    (`workers::source_atoms::v3`), each carrying its subject reef, the
--    supersession state key, the old and new values and the exact quote.
--    Nullable, so every existing row and reader is unchanged. Fact
--    supersession (`fact_supersession::changes`) is the only reader and
--    turns each into an explicit transition; it remains the only writer of
--    fact_supersession_edges.
ALTER TABLE semantic_atoms
 ADD COLUMN stated_changes JSONB,
 ADD CONSTRAINT semantic_atoms_stated_changes_shape
  CHECK(stated_changes IS NULL OR (jsonb_typeof(stated_changes)='array'
   AND jsonb_array_length(stated_changes) BETWEEN 1 AND 8
   AND octet_length(stated_changes::text)<=32768));
CREATE INDEX semantic_atoms_stated_changes ON semantic_atoms(graph_id,extraction_version)
 WHERE stated_changes IS NOT NULL;

-- 2. A decision that a stated change itself made.
DO $$ DECLARE c TEXT; BEGIN
 FOR c IN SELECT conname FROM pg_constraint WHERE conrelid='fact_supersession_edges'::regclass
   AND contype='c' AND pg_get_constraintdef(oid) LIKE '%replacement_language%' LOOP
  EXECUTE format('ALTER TABLE fact_supersession_edges DROP CONSTRAINT %I',c);
 END LOOP;
END $$;
ALTER TABLE fact_supersession_edges ADD CONSTRAINT fact_supersession_edges_rule_check
 CHECK(rule IN ('explicit_end','later_start','replacement_language','judge','stated_change'));

-- 3. The source-atoms-v3 profile. Runs may pin it.
DO $$ DECLARE c TEXT; BEGIN
 FOR c IN SELECT conname FROM pg_constraint WHERE conrelid='corpus_enrichment_runs'::regclass
   AND contype='c' AND pg_get_constraintdef(oid) LIKE '%source-atoms-v1%' LOOP
  EXECUTE format('ALTER TABLE corpus_enrichment_runs DROP CONSTRAINT %I',c);
 END LOOP;
END $$;
ALTER TABLE corpus_enrichment_runs ADD CONSTRAINT corpus_enrichment_runs_extraction_version_check
 CHECK(extraction_version IN ('source-atoms-v1','source-atoms-v2','source-atoms-v3','meeting-atoms-v1'));

-- 4. Standing queue routing accepts v3 for the source job kinds. Only the
--    profile list changes from 0164; every other guard is as before.
CREATE OR REPLACE FUNCTION corpus_standing_queue_route() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE graph UUID; version UUID; profile TEXT; owner_account UUID; graph_account UUID; owner_profile TEXT;
BEGIN
 IF TG_OP='UPDATE' THEN
  IF OLD.kind IN ('ExtractSourceAtoms','ExtractMeetingAtoms','ExtractStandingSourceAtoms','ExtractStandingMeetingAtoms') THEN
   IF OLD.status::text='dead' AND OLD.payload->'evidence_retracted'='true'::jsonb THEN
    IF NEW.status::text IS DISTINCT FROM 'dead' OR
     (NEW.payload,NEW.kind,NEW.account_id) IS DISTINCT FROM (OLD.payload,OLD.kind,OLD.account_id) THEN
     RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='retracted extraction job is terminal';
    END IF;
    RETURN NEW;
   END IF;
   graph=public.corpus_queue_uuid(OLD.payload->'graph_id');
   IF graph IS NOT NULL AND NEW.status::text='dead'
    AND NEW.kind IS NOT DISTINCT FROM OLD.kind AND NEW.account_id IS NOT DISTINCT FROM OLD.account_id
    AND NEW.payload=jsonb_build_object('graph_id',graph,'evidence_retracted',true) THEN
    RETURN NEW;
   END IF;
  END IF;
  IF OLD.kind IN ('ExtractSourceAtoms','ExtractMeetingAtoms','ExtractStandingSourceAtoms','ExtractStandingMeetingAtoms')
   AND OLD.status::text IN ('pending','running') THEN
   IF (NEW.payload->'graph_id',NEW.payload->'source_version_id') IS DISTINCT FROM
     (OLD.payload->'graph_id',OLD.payload->'source_version_id')
    OR (OLD.account_id IS NOT NULL AND NEW.account_id IS DISTINCT FROM OLD.account_id)
    OR (NEW.kind IS DISTINCT FROM OLD.kind AND (
     OLD.status::text='pending' AND NEW.status::text='pending' AND
     ((OLD.kind='ExtractSourceAtoms' AND NEW.kind='ExtractStandingSourceAtoms') OR
      (OLD.kind='ExtractMeetingAtoms' AND NEW.kind='ExtractStandingMeetingAtoms'))) IS NOT TRUE) THEN
    RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='active extraction job identity is immutable';
   END IF;
  END IF;
 END IF;
 IF NEW.kind NOT IN ('ExtractSourceAtoms','ExtractMeetingAtoms','ExtractStandingSourceAtoms','ExtractStandingMeetingAtoms')
  OR NEW.status::text NOT IN ('pending','running') THEN RETURN NEW; END IF;
 graph=public.corpus_queue_uuid(NEW.payload->'graph_id');
 version=public.corpus_queue_uuid(NEW.payload->'source_version_id');
 IF graph IS NULL OR version IS NULL THEN RETURN NEW; END IF;
 profile=CASE WHEN NEW.kind IN ('ExtractSourceAtoms','ExtractStandingSourceAtoms') THEN 'source-atoms' ELSE 'meeting-atoms' END;
 IF TG_OP='INSERT' THEN
  PERFORM 1 FROM public.source_versions v WHERE v.graph_id=graph AND v.version_id=version FOR KEY SHARE;
 END IF;
 SELECT r.account_id,g.account_id,v.extraction_version INTO owner_account,graph_account,owner_profile
  FROM public.corpus_enrichment_units u
  JOIN public.corpus_enrichment_runs r ON r.run_id=u.run_id AND r.graph_id=u.graph_id
  JOIN public.source_versions v ON v.graph_id=u.graph_id AND v.version_id=version
  JOIN public.graphs g ON g.id=v.graph_id
  WHERE u.graph_id=graph AND COALESCE(u.promoted_version_id,u.holding_id)=version
   AND u.extraction_version=v.extraction_version
   AND u.execution_owner AND r.run_kind='standing';
 IF owner_account IS NOT NULL THEN
  IF NEW.account_id IS DISTINCT FROM owner_account OR owner_account IS DISTINCT FROM graph_account
   OR NOT ((profile='source-atoms' AND owner_profile IN ('source-atoms-v1','source-atoms-v2','source-atoms-v3')) OR (profile='meeting-atoms' AND owner_profile='meeting-atoms-v1')) THEN
   RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='standing extraction identity is incompatible';
  END IF;
  IF NEW.kind IN ('ExtractSourceAtoms','ExtractMeetingAtoms') AND NEW.status::text='running' THEN
   RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='standing extraction requires a compatible worker';
  END IF;
  NEW.kind=CASE NEW.kind WHEN 'ExtractSourceAtoms' THEN 'ExtractStandingSourceAtoms'
   WHEN 'ExtractMeetingAtoms' THEN 'ExtractStandingMeetingAtoms' ELSE NEW.kind END;
 END IF;
 RETURN NEW;
END $$;

-- 5. A capture receipt may follow its source version's pin forward: v1 to
--    v2 (as 0164), v1 to v3, or v2 to v3, and only when the source version
--    already carries the new pin. Every other identity column stays
--    immutable. Only the transition list changes from 0164.
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

-- 6. Repinning a source version forward (standing adoption under the active
--    profile, or an explicit re-extraction request) moves its capture
--    receipt with it: to v2 from v1 (as 0164), or to v3 from v1 or v2. It
--    can only align a receipt with the pin its source version already has,
--    and it clears the old run/unit so the new owner is recorded on the
--    next enqueue.
CREATE OR REPLACE FUNCTION corpus_capture_admission_follow_pin(p_graph uuid,p_version uuid)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public AS $$
DECLARE pin text; status text; err text;
BEGIN
 SELECT v.extraction_version,v.extraction_status,v.extraction_error INTO pin,status,err
  FROM public.source_versions v WHERE v.graph_id=p_graph AND v.version_id=p_version;
 IF NOT FOUND OR pin IS NULL OR pin NOT IN ('source-atoms-v2','source-atoms-v3') THEN RETURN false; END IF;
 UPDATE public.corpus_capture_admissions
    SET extraction_version=pin,run_id=NULL,unit_id=NULL,
        state=CASE WHEN status='blocked' THEN 'blocked' ELSE state END,
        reason=CASE WHEN status='blocked' THEN err ELSE reason END,
        updated_at=now()
  WHERE graph_id=p_graph AND source_version_id=p_version
    AND extraction_version=ANY(CASE WHEN pin='source-atoms-v2' THEN ARRAY['source-atoms-v1']
                                    ELSE ARRAY['source-atoms-v1','source-atoms-v2'] END);
 RETURN FOUND;
END $$;
REVOKE ALL ON FUNCTION corpus_capture_admission_follow_pin(uuid,uuid) FROM PUBLIC;
DO $$ BEGIN
 IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname='coralswarm_tenant_app') THEN
  GRANT EXECUTE ON FUNCTION corpus_capture_admission_follow_pin(uuid,uuid) TO coralswarm_tenant_app;
 END IF;
 IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname='coralswarm_app') THEN
  GRANT EXECUTE ON FUNCTION corpus_capture_admission_follow_pin(uuid,uuid) TO coralswarm_app;
 END IF;
END $$;
