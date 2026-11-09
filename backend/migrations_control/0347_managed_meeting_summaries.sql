-- PAIRED WITH: migrations_tenant/0179_managed_meeting_summaries.sql.
--
-- Source-agnostic pipeline PR 10 (COR-35): GenerateMeetingSummary (the post-meeting
-- map/reduce summary) moves onto the managed_enrichment purpose. Its worker
-- claims only while ENRICHMENT_PROCESSING_ENABLED=true and
-- MEETING_SUMMARIES_ENABLED is not off, meters every model call through the
-- managed gate (shared daily cap per account), is bounded per graph per UTC
-- day, builds its prompt only from the version's present, unretracted chunk
-- atoms, and publishes only when the transcript, its source version and
-- every chunk atom it bound are unchanged. Holds
-- and the cap reschedule the meeting; they never dead-letter it.
--
-- The admission trigger stops dead-lettering the kind. Rows already
-- dead-lettered with managed_purpose_unsupported stay terminal: this
-- migration requeues nothing. A new capture or an explicit
-- POST /v1/meetings/summary/generate mints a fresh pending row.
--
-- The quoted list is pinned to managed_history::UNSUPPORTED_JOB_PURPOSES
-- followed by RETIRED_JOB_KINDS by a unit test. CREATE OR REPLACE keeps the
-- function's owner, search_path and ACL, exactly as before.
CREATE OR REPLACE FUNCTION corpus_legacy_purpose_admission() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
 IF NEW.kind IN ('ExtractEntities','ExtractRelationships','IngestDoor','ConflictDetection','InferExpertise','ReevaluateKind','ExtractThreads','ExtractDecisions') AND NEW.status::text IN ('pending','running','failed') THEN
  NEW.status='dead'; NEW.finished_at=now();
  NEW.error_message='managed_purpose_unsupported';
 END IF;
 RETURN NEW;
END $$;

-- Deletion retracts a published summary (COR-35). Every summary call binds
-- the transcript snapshot, its source version and every chunk atom of that
-- version. managed_enrichment_invalidate_atoms keeps its 0338 behaviour
-- (in-flight inputs become 'invalidated', digests built from a gone coral are
-- stripped and regenerated) and now also clears the current summary of any
-- meeting whose published reduce call read a deleted or retracted atom of
-- the version still current (summary NULL, summary_status='pending') and
-- queues one regeneration on the producers' summary:{graph}:{source} key.
-- The worker builds the prompt only from the version's present, unretracted
-- chunk atoms (no raw shortcut), so the regenerated summary never carries
-- the gone chunk; with no chunk left it is refused without a model call.
-- Whole-source and version deletion already clear the summary in
-- library_capture_purge and source_evidence. CREATE OR REPLACE keeps the
-- owner, SECURITY DEFINER, search_path and the revoked PUBLIC EXECUTE from
-- 0338.
CREATE OR REPLACE FUNCTION managed_enrichment_invalidate_atoms(gone UUID[]) RETURNS void
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public AS $$
BEGIN
 IF gone IS NULL OR cardinality(gone)=0 THEN RETURN; END IF;
 UPDATE public.managed_enrichment_inputs SET outcome='invalidated'
  WHERE input_kind='atom' AND input_id=ANY(gone) AND outcome='sent';
 WITH hit AS (
  SELECT DISTINCT m.input_id AS node_id, a.receipt_id
    FROM public.managed_enrichment_inputs a
    JOIN public.managed_enrichment_inputs m ON m.receipt_id=a.receipt_id AND m.input_kind='digest_node'
   WHERE a.input_kind='atom' AND a.input_id=ANY(gone) AND a.outcome='written'),
 stripped AS (
  UPDATE public.nodes n SET body=n.body-'digest', digest_coral_count=0, version=n.version+1, updated_at=now()
    FROM hit WHERE n.node_id=hit.node_id AND n.body->'digest'->>'managed_receipt'=hit.receipt_id::text
   RETURNING n.*),
 dropped AS (
  DELETE FROM public.op_log o USING stripped s
   WHERE o.graph_id=s.graph_id AND o.entity='node' AND o.entity_id=s.node_id::text),
 logged AS (
  INSERT INTO public.op_log(op_id,entity,entity_id,op_type,payload,version,updated_at,device_id,graph_id)
  SELECT gen_random_uuid(),'node',s.node_id::text,'upsert',jsonb_build_object('node_id',s.node_id,'graph_id',s.graph_id,'type',s.type,'title',s.title,'body',s.body::text,'scope',s.scope,'owner_id',s.owner_id,'created_at',to_char(s.created_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"+00:00"'),'updated_at',to_char(s.updated_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"+00:00"'),'version',s.version),s.version,s.updated_at,'evidence-lifecycle',s.graph_id
    FROM stripped s)
 INSERT INTO public.jobs(kind,payload,idempotency_key,run_after)
 SELECT 'RegenerateDigest', jsonb_build_object('node_id',r.node_id,'graph_id',r.graph_id), 'digest:'||r.node_id, now()
   FROM (SELECT DISTINCT n.node_id, n.graph_id FROM hit JOIN public.nodes n ON n.node_id=hit.node_id) r
 ON CONFLICT (idempotency_key) WHERE status='pending' DO NOTHING;
 WITH hit AS (
  SELECT DISTINCT t.input_id AS transcript_id, v.input_id AS version_id
    FROM public.managed_enrichment_inputs a
    JOIN public.managed_enrichment_inputs t ON t.receipt_id=a.receipt_id AND t.input_kind='meeting_transcript'
    JOIN public.managed_enrichment_inputs v ON v.receipt_id=a.receipt_id AND v.input_kind='source_version'
   WHERE a.input_kind='atom' AND a.input_id=ANY(gone) AND a.outcome='written'),
 cleared AS (
  UPDATE public.meeting_transcripts mt SET summary=NULL,summary_status='pending',summary_error=NULL,summary_model=NULL,summary_updated_at=now()
    FROM hit WHERE mt.id=hit.transcript_id AND mt.current_version_id=hit.version_id AND mt.summary_status='done'
   RETURNING mt.graph_id, mt.source_id)
 INSERT INTO public.jobs(kind,payload,idempotency_key,run_after)
 SELECT 'GenerateMeetingSummary', jsonb_build_object('graph_id',c.graph_id,'source_id',c.source_id,'allow_cloud_override',false),
        'summary:'||c.graph_id||':'||c.source_id, now()
   FROM cleared c
 ON CONFLICT (idempotency_key) WHERE status='pending' DO NOTHING;
END $$;
