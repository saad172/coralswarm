-- PAIRED WITH: migrations_control/0338_managed_enrichment_consumers.sql.
--
-- Source-agnostic pipeline PRs 6-8: the journal sweep (Today summaries),
-- coral titles (BackfillAtomTitles) and reef digests (RegenerateDigest) move
-- onto the shared managed_enrichment purpose. Each worker claims only while
-- ENRICHMENT_PROCESSING_ENABLED=true and its own switch (JOURNAL_SWEEP_ENABLED,
-- ATOM_TITLES_ENABLED, DIGESTS_ENABLED) is not off, meters every model call
-- through the managed gate (shared $2/day cap per account), is bounded per
-- graph per UTC day, and publishes only when the inputs it read are unchanged.
--
-- 1. The admission trigger stops dead-lettering RegenerateDigest and
--    BackfillAtomTitles. Rows already dead-lettered with
--    managed_purpose_unsupported stay terminal: this migration requeues
--    nothing. The digest trigger (trg_nodes_digest_regeneration) mints at
--    most one pending row per reef ('digest:' || node_id, ON CONFLICT ...
--    WHERE status='pending' DO NOTHING), and the worker bounds what it spends.
--
--    The quoted list is pinned to managed_history::UNSUPPORTED_JOB_PURPOSES
--    followed by RETIRED_JOB_KINDS by a unit test. CREATE OR REPLACE keeps
--    the function's owner, search_path and ACL, exactly as before.
CREATE OR REPLACE FUNCTION corpus_legacy_purpose_admission() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
 IF NEW.kind IN ('GenerateMeetingSummary','ExtractEntities','ExtractRelationships','IngestDoor','ConflictDetection','InferExpertise','ReevaluateKind','ExtractThreads','ExtractDecisions') AND NEW.status::text IN ('pending','running','failed') THEN
  NEW.status='dead'; NEW.finished_at=now();
  NEW.error_message='managed_purpose_unsupported';
 END IF;
 RETURN NEW;
END $$;

-- 2. Deletion invalidation goes through managed_enrichment_inputs. When a
--    coral is deleted or its evidence is retracted:
--    * every in-flight call that read it ('sent') becomes 'invalidated', so
--      its publish re-verify reports Stale and writes nothing;
--    * every reef with a digest published from a call that read it gets one
--      regeneration queued (deduped per reef like the digest trigger). If
--      that reef's CURRENT body.digest is still the one published from such
--      a call (body.digest.managed_receipt names the receipt), it is
--      stripped, its digest_coral_count reset and the change logged for
--      sync. A per-atom purge (library_capture_purge, which a raw DELETE
--      reaches through library_atom_purge_signal) usually strips it first;
--      the regeneration is queued either way.
--    SECURITY DEFINER under the migration owner (the table owner on every
--    plane), so a member- or account-scoped delete still reaches the
--    system-written inputs rows. The owner is not changed and the helper's
--    EXECUTE is revoked from PUBLIC; the two trigger functions run as the
--    owner, which keeps EXECUTE.
CREATE FUNCTION managed_enrichment_invalidate_atoms(gone UUID[]) RETURNS void
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
END $$;
REVOKE ALL ON FUNCTION managed_enrichment_invalidate_atoms(UUID[]) FROM PUBLIC;

CREATE FUNCTION managed_enrichment_atoms_deleted() RETURNS trigger
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public AS $$
BEGIN
 PERFORM public.managed_enrichment_invalidate_atoms(ARRAY(SELECT atom_id FROM gone_atoms));
 RETURN NULL;
END $$;
CREATE TRIGGER managed_enrichment_atoms_deleted AFTER DELETE ON atoms
 REFERENCING OLD TABLE AS gone_atoms FOR EACH STATEMENT
 EXECUTE FUNCTION managed_enrichment_atoms_deleted();

CREATE FUNCTION managed_enrichment_atom_retracted() RETURNS trigger
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public AS $$
BEGIN
 PERFORM public.managed_enrichment_invalidate_atoms(ARRAY[NEW.atom_id]);
 RETURN NULL;
END $$;
CREATE TRIGGER managed_enrichment_atom_retracted AFTER UPDATE OF evidence_retracted ON atoms
 FOR EACH ROW WHEN (NEW.evidence_retracted IS TRUE AND OLD.evidence_retracted IS NOT TRUE)
 EXECUTE FUNCTION managed_enrichment_atom_retracted();
