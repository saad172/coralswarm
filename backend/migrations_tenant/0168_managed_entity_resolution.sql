-- PAIRED WITH: migrations_control/0335_managed_entity_resolution.sql.
--
-- Source-agnostic pipeline PR 5: EntityResolution (judged duplicate-reef
-- merging) moves onto the managed_enrichment purpose. Its worker claims only
-- while ENRICHMENT_PROCESSING_ENABLED=true, meters every model call through
-- the managed gate (shared $2/day cap) and publishes merges and judgements
-- only when the inputs it judged are unchanged.
--
-- The admission trigger stops dead-lettering the kind. Rows already
-- dead-lettered with managed_purpose_unsupported stay terminal: this
-- migration requeues nothing. Producers mint fresh pending rows.
--
-- The quoted list is pinned to managed_history::UNSUPPORTED_JOB_PURPOSES
-- followed by RETIRED_JOB_KINDS by a unit test. CREATE OR REPLACE keeps the
-- function's owner, search_path and ACL, exactly as 0160.
CREATE OR REPLACE FUNCTION corpus_legacy_purpose_admission() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
 IF NEW.kind IN ('GenerateMeetingSummary','ExtractEntities','ExtractRelationships','RegenerateDigest','BackfillAtomTitles','IngestDoor','ConflictDetection','InferExpertise','ReevaluateKind','ExtractThreads','ExtractDecisions') AND NEW.status::text IN ('pending','running','failed') THEN
  NEW.status='dead'; NEW.finished_at=now();
  NEW.error_message='managed_purpose_unsupported';
 END IF;
 RETURN NEW;
END $$;
