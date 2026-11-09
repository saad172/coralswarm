-- Draft only: root allocates the next actual migration pair after finance lands.
ALTER TABLE corpus_enrichment_units
 ADD COLUMN standing_generation bigint NOT NULL DEFAULT 0 CHECK(standing_generation>=0),
 ADD COLUMN standing_logical_call uuid,
 ADD COLUMN standing_start_cursor bigint CHECK(standing_start_cursor>=0),
 ADD COLUMN standing_claim_until timestamptz,
 ADD CONSTRAINT corpus_standing_claim_shape CHECK(
  (standing_claim_until IS NULL OR (standing_logical_call IS NOT NULL AND standing_start_cursor IS NOT NULL AND standing_generation>0))
 );
ALTER TABLE corpus_capture_admissions DROP CONSTRAINT corpus_capture_admissions_hold_basis_check;
ALTER TABLE corpus_capture_admissions ADD CONSTRAINT corpus_capture_admissions_hold_basis_check CHECK(hold_basis IN
 ('legacy_unknown','never_held','unverified_transition','backlog_policy_excluded','operator_paused','managed_processing_disabled','processing_off','cloud_processing_disabled','source_removed_or_access_lost','requester_access_lost','daily_budget_exhausted','execution_budget_exhausted','lifecycle_contract_not_ready','credits_exhausted','principal_unavailable','infrastructure_unavailable','managed_processing_killed'));
DO $$ BEGIN
 IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='coralswarm_managed_clock') THEN
  CREATE ROLE coralswarm_managed_clock NOLOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
 END IF;
 IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname='coralswarm_managed_clock' AND
   (rolcanlogin OR rolsuper OR rolcreatedb OR rolcreaterole OR rolreplication OR rolbypassrls)) THEN
  RAISE EXCEPTION 'unexpected managed clock role attributes';
 END IF;
 IF EXISTS(SELECT 1 FROM pg_roles r WHERE r.rolname IN ('coralswarm_app','coralswarm_tenant_app')
   AND pg_has_role(r.oid,'coralswarm_managed_clock','MEMBER')) THEN
  RAISE EXCEPTION 'managed clock owner must be non-assumable by app roles';
 END IF;
END $$;
GRANT USAGE ON SCHEMA public TO coralswarm_managed_clock;
GRANT SELECT ON graphs,source_items,source_versions,corpus_enrichment_units,corpus_enrichment_runs,jobs,corpus_capture_admissions TO coralswarm_managed_clock;
GRANT UPDATE(ever_held,hold_basis) ON corpus_capture_admissions TO coralswarm_managed_clock;
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
  IF (NEW.source_version_id,NEW.graph_id,NEW.account_id,NEW.source_item_id,NEW.captured_by,NEW.extraction_version) IS DISTINCT FROM (OLD.source_version_id,OLD.graph_id,OLD.account_id,OLD.source_item_id,OLD.captured_by,OLD.extraction_version) THEN
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

CREATE FUNCTION corpus_standing_receipt_event(p_graph uuid,p_version uuid,p_event text,p_reason text)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public AS $$
DECLARE u public.corpus_enrichment_units; r public.corpus_capture_admissions; basis text;
BEGIN
 IF p_event IS NULL OR p_event NOT IN ('adopt','hold','requeue') THEN RAISE EXCEPTION 'unsupported managed clock event'; END IF;
 SELECT cu.* INTO u FROM public.corpus_enrichment_units cu
  JOIN public.corpus_enrichment_runs cr ON cr.run_id=cu.run_id AND cr.graph_id=cu.graph_id
  JOIN public.graphs g ON g.id=cu.graph_id AND g.account_id=cr.account_id
  JOIN public.source_versions v ON v.graph_id=cu.graph_id AND v.version_id=cu.holding_id AND v.extraction_version=cu.extraction_version
  JOIN public.source_items s ON s.graph_id=v.graph_id AND s.source_item_id=v.source_item_id
  WHERE cu.graph_id=p_graph AND cu.holding_id=p_version AND cu.kind='retained_version' AND cu.execution_owner
   AND cr.run_kind='standing' AND cr.mode='execute' AND cr.corpus_kind='real';
 IF NOT FOUND THEN RETURN false; END IF;
 SELECT * INTO r FROM public.corpus_capture_admissions WHERE source_version_id=p_version AND graph_id=p_graph FOR UPDATE;
 IF NOT FOUND OR r.unit_id IS DISTINCT FROM u.unit_id OR r.run_id IS DISTINCT FROM u.run_id OR r.extraction_version IS DISTINCT FROM u.extraction_version THEN RETURN false; END IF;
 IF p_event='hold' THEN
  IF p_reason NOT IN ('operator_paused','managed_processing_disabled','cloud_processing_disabled','source_removed_or_access_lost','daily_budget_exhausted','lifecycle_contract_not_ready','credits_exhausted','principal_unavailable','infrastructure_unavailable','managed_processing_killed')
   OR p_reason IS NULL OR u.reason IS DISTINCT FROM p_reason OR u.state NOT IN ('pending','unavailable')
   OR r.state<>'blocked' OR r.reason IS DISTINCT FROM p_reason
   OR NOT EXISTS(SELECT 1 FROM public.source_versions WHERE graph_id=p_graph AND version_id=p_version AND extraction_status='blocked' AND extraction_error=p_reason) THEN
   RAISE EXCEPTION 'managed hold event lacks matching source owner facts';
  END IF;
  basis=p_reason;
 ELSE
  IF p_reason IS NOT NULL OR u.state<>'queued' OR r.state<>'queued'
    OR NOT EXISTS(SELECT 1 FROM public.jobs j WHERE j.kind IN ('ExtractStandingSourceAtoms','ExtractStandingMeetingAtoms') AND j.status::text IN ('pending','running') AND public.corpus_queue_uuid(j.payload->'graph_id')=p_graph AND public.corpus_queue_uuid(j.payload->'source_version_id')=p_version)
    OR NOT EXISTS(SELECT 1 FROM public.source_versions v JOIN public.source_items s ON s.source_item_id=v.source_item_id AND s.graph_id=v.graph_id WHERE v.graph_id=p_graph AND v.version_id=p_version AND s.access_state='permitted' AND v.extraction_status NOT IN ('done','failed') AND v.captured_at>='2026-10-13T00:00:00Z' AND NOT COALESCE(v.origin->>'transport'='synthetic',false) AND NOT COALESCE(v.source_metadata->'synthetic_demo'='true'::jsonb,false)) THEN
   RETURN false;
  END IF;
  basis='backlog_policy_excluded';
 END IF;
 UPDATE public.corpus_capture_admissions SET ever_held=true,hold_basis=CASE WHEN hold_basis IN ('legacy_unknown','backlog_policy_excluded') THEN hold_basis ELSE basis END WHERE source_version_id=p_version AND graph_id=p_graph;
 RETURN true;
END $$;
REVOKE ALL ON FUNCTION corpus_standing_receipt_event(uuid,uuid,text,text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.corpus_standing_receipt_event(uuid,uuid,text,text) TO coralswarm_tenant_app;
DO $$
DECLARE
 migrator oid := (SELECT oid FROM pg_roles WHERE rolname=current_user);
 prior_self_grant pg_auth_members;
 temporary_set boolean := NOT pg_has_role(current_user,'coralswarm_managed_clock','SET');
 temporary_create boolean := NOT has_schema_privilege('coralswarm_managed_clock','public','CREATE');
BEGIN
 IF temporary_set THEN
  IF NOT pg_has_role(current_user,'coralswarm_managed_clock','MEMBER WITH ADMIN OPTION') THEN
   RAISE EXCEPTION USING ERRCODE='42501',MESSAGE='managed clock ownership transfer requires this cluster''s trusted migration owner to hold SET or ADMIN on coralswarm_managed_clock; provision that owner edge out of band, never grant the clock role to an app role';
  END IF;
  SELECT * INTO prior_self_grant FROM pg_auth_members
   WHERE roleid='coralswarm_managed_clock'::regrole AND member=migrator AND grantor=migrator;
  EXECUTE format('GRANT coralswarm_managed_clock TO %I WITH SET TRUE, INHERIT FALSE',current_user);
 END IF;
 IF temporary_create THEN
  GRANT CREATE ON SCHEMA public TO coralswarm_managed_clock;
 END IF;
 ALTER FUNCTION corpus_standing_receipt_event(uuid,uuid,text,text) OWNER TO coralswarm_managed_clock;
 IF temporary_create THEN
  REVOKE CREATE ON SCHEMA public FROM coralswarm_managed_clock;
 END IF;
 IF temporary_set THEN
  IF prior_self_grant.member IS NULL THEN
   EXECUTE format('REVOKE coralswarm_managed_clock FROM %I GRANTED BY %I',current_user,current_user);
  ELSE
   EXECUTE format('GRANT coralswarm_managed_clock TO %I WITH ADMIN %s, INHERIT %s, SET %s',
    current_user,CASE WHEN prior_self_grant.admin_option THEN 'TRUE' ELSE 'FALSE' END,
    CASE WHEN prior_self_grant.inherit_option THEN 'TRUE' ELSE 'FALSE' END,
    CASE WHEN prior_self_grant.set_option THEN 'TRUE' ELSE 'FALSE' END);
  END IF;
 END IF;
END $$;
GRANT EXECUTE ON FUNCTION corpus_queue_uuid(jsonb) TO coralswarm_managed_clock;

DO $$ BEGIN
 IF to_regprocedure('public.tenant_rls_visible_graph_ids()') IS NOT NULL THEN
  GRANT EXECUTE ON FUNCTION tenant_rls_visible_graph_ids(),tenant_rls_actor_user_id(),tenant_rls_is_system() TO coralswarm_managed_clock;
 END IF;
END $$;
