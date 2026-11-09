-- PAIRED WITH: migrations_tenant/0160_retire_unsupported_enrichment.sql.
--
-- Founder decision 2026-10-18 (source-agnostic pipeline PR 0b):
-- * IngestDoor, ConflictDetection, InferExpertise, ReevaluateKind,
--   ExtractThreads and ExtractDecisions are retired. They lose their Rust
--   variant, worker and producers; the admission function below still
--   dead-letters a hand-inserted row so nothing waits on a missing worker.
-- * InferRelationships makes no model call. It leaves the list and runs again.
--   Rows already dead-lettered stay terminal: this migration requeues nothing.
--
-- The quoted list is pinned to managed_history::UNSUPPORTED_JOB_PURPOSES
-- followed by RETIRED_JOB_KINDS by a unit test. Same owner, same search_path
-- and no grant changes as 0307/0324: CREATE OR REPLACE keeps the function's
-- owner and ACL.
CREATE OR REPLACE FUNCTION corpus_legacy_purpose_admission() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
 IF NEW.kind IN ('GenerateMeetingSummary','ExtractEntities','ExtractRelationships','RegenerateDigest','EntityResolution','BackfillAtomTitles','IngestDoor','ConflictDetection','InferExpertise','ReevaluateKind','ExtractThreads','ExtractDecisions') AND NEW.status::text IN ('pending','running','failed') THEN
  NEW.status='dead'; NEW.finished_at=now();
  NEW.error_message='managed_purpose_unsupported';
 END IF;
 RETURN NEW;
END $$;

-- ReevaluateKind's only producer (0217): every 100-coral mark minted a job
-- that the admission trigger immediately dead-lettered.
DROP TRIGGER IF EXISTS trg_nodes_kind_reeval ON nodes;
DROP FUNCTION IF EXISTS nodes_maybe_enqueue_kind_reeval();
