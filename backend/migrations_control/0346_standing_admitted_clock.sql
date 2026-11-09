-- #1359: label adopted captures accurately on the managed receipt clock.
-- Every capture now reaches standing through adoption, and 0155's
-- corpus_standing_receipt_event labelled every 'adopt' and 'requeue'
-- backlog_policy_excluded, so the freshness SLO (capture_admission_health)
-- never had a never-held sample.
--
-- * 'adopt' of a receipt that has no clock history yet ('unverified_transition',
--   never held) and was observed within the last 24 hours (the fresh-capture
--   window, source_atoms::FRESH_CAPTURE_WINDOW) records 'standing_admitted'
--   and leaves ever_held false. The SLO counts it like 'never_held'. Any
--   other adopt (a repinned, retried or floor-override version, or an older
--   capture) stays backlog_policy_excluded, as before.
-- * 'requeue' may carry the hold reason the unit had. The receipt then keeps
--   that reason instead of being relabelled backlog_policy_excluded. A requeue
--   without a reason (a binary from before this migration) behaves as before.
--
-- * The live-job check now leads with payload->>'source_version_id', which
--   jobs_payload_source_version (tenant 0165 / control 0332) indexes. 0155's
--   corpus_queue_uuid(...) form scanned every job: 840 ms per event against
--   1.44M jobs in production, so a requeue batch of 25 outlasted the 8 s
--   sweep slot and rolled back on every sweep (2026-10-20, graph 95bba458).
--
-- The function is owned by coralswarm_managed_clock (0155/0323). The migrator
-- replaces it acting as that role, with the same temporary SET and CREATE
-- grants 0155 used to transfer ownership. The DDL is identical in both trees
-- (tenant 0178 is the twin).
ALTER TABLE corpus_capture_admissions DROP CONSTRAINT corpus_capture_admissions_hold_basis_check;
ALTER TABLE corpus_capture_admissions ADD CONSTRAINT corpus_capture_admissions_hold_basis_check CHECK(hold_basis IN
 ('legacy_unknown','never_held','unverified_transition','backlog_policy_excluded','standing_admitted','operator_paused','managed_processing_disabled','processing_off','cloud_processing_disabled','source_removed_or_access_lost','requester_access_lost','daily_budget_exhausted','execution_budget_exhausted','lifecycle_contract_not_ready','credits_exhausted','principal_unavailable','infrastructure_unavailable','managed_processing_killed'));

DO $migration$
DECLARE
 migrator oid := (SELECT oid FROM pg_roles WHERE rolname=current_user);
 prior_self_grant pg_auth_members;
 temporary_set boolean := NOT pg_has_role(current_user,'coralswarm_managed_clock','SET');
 temporary_create boolean := NOT has_schema_privilege('coralswarm_managed_clock','public','CREATE');
BEGIN
 IF temporary_set THEN
  IF NOT pg_has_role(current_user,'coralswarm_managed_clock','MEMBER WITH ADMIN OPTION') THEN
   RAISE EXCEPTION USING ERRCODE='42501',MESSAGE='replacing the managed clock requires this cluster''s trusted migration owner to hold SET or ADMIN on coralswarm_managed_clock; provision that owner edge out of band, never grant the clock role to an app role';
  END IF;
  SELECT * INTO prior_self_grant FROM pg_auth_members
   WHERE roleid='coralswarm_managed_clock'::regrole AND member=migrator AND grantor=migrator;
  EXECUTE format('GRANT coralswarm_managed_clock TO %I WITH SET TRUE, INHERIT FALSE',current_user);
 END IF;
 IF temporary_create THEN
  GRANT CREATE ON SCHEMA public TO coralswarm_managed_clock;
 END IF;
 SET LOCAL ROLE coralswarm_managed_clock;
 EXECUTE $create$
CREATE OR REPLACE FUNCTION public.corpus_standing_receipt_event(p_graph uuid,p_version uuid,p_event text,p_reason text)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public AS $body$
DECLARE u public.corpus_enrichment_units; r public.corpus_capture_admissions; basis text;
 holds constant text[] := ARRAY['operator_paused','managed_processing_disabled','cloud_processing_disabled','source_removed_or_access_lost','daily_budget_exhausted','lifecycle_contract_not_ready','credits_exhausted','principal_unavailable','infrastructure_unavailable','managed_processing_killed'];
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
  IF p_reason IS NULL OR NOT p_reason=ANY(holds) OR u.reason IS DISTINCT FROM p_reason OR u.state NOT IN ('pending','unavailable')
   OR r.state<>'blocked' OR r.reason IS DISTINCT FROM p_reason
   OR NOT EXISTS(SELECT 1 FROM public.source_versions WHERE graph_id=p_graph AND version_id=p_version AND extraction_status='blocked' AND extraction_error=p_reason) THEN
   RAISE EXCEPTION 'managed hold event lacks matching source owner facts';
  END IF;
  basis=p_reason;
 ELSE
  IF (p_reason IS NOT NULL AND (p_event='adopt' OR NOT p_reason=ANY(holds))) OR u.state<>'queued' OR r.state<>'queued'
    OR NOT EXISTS(SELECT 1 FROM public.jobs j WHERE j.payload->>'source_version_id'=p_version::text AND j.kind IN ('ExtractStandingSourceAtoms','ExtractStandingMeetingAtoms') AND j.status::text IN ('pending','running') AND public.corpus_queue_uuid(j.payload->'graph_id')=p_graph AND public.corpus_queue_uuid(j.payload->'source_version_id')=p_version)
    OR NOT EXISTS(SELECT 1 FROM public.source_versions v JOIN public.source_items s ON s.source_item_id=v.source_item_id AND s.graph_id=v.graph_id WHERE v.graph_id=p_graph AND v.version_id=p_version AND s.access_state='permitted' AND v.extraction_status NOT IN ('done','failed') AND v.captured_at>='2026-10-13T00:00:00Z' AND NOT COALESCE(v.origin->>'transport'='synthetic',false) AND NOT COALESCE(v.source_metadata->'synthetic_demo'='true'::jsonb,false)) THEN
   RETURN false;
  END IF;
  IF p_event='adopt' AND r.hold_basis='unverified_transition' AND NOT r.ever_held AND r.observed_at>=clock_timestamp()-interval '24 hours' THEN
   UPDATE public.corpus_capture_admissions SET hold_basis='standing_admitted' WHERE source_version_id=p_version AND graph_id=p_graph;
   RETURN true;
  END IF;
  basis=COALESCE(p_reason,'backlog_policy_excluded');
 END IF;
 UPDATE public.corpus_capture_admissions SET ever_held=true,hold_basis=CASE WHEN hold_basis IN ('legacy_unknown','backlog_policy_excluded') THEN hold_basis ELSE basis END WHERE source_version_id=p_version AND graph_id=p_graph;
 RETURN true;
END $body$
$create$;
 SET LOCAL ROLE NONE;
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
 IF pg_get_userbyid((SELECT proowner FROM pg_proc WHERE oid='public.corpus_standing_receipt_event(uuid,uuid,text,text)'::regprocedure))<>'coralswarm_managed_clock' THEN
  RAISE EXCEPTION 'managed clock must stay owned by coralswarm_managed_clock';
 END IF;
END $migration$;
