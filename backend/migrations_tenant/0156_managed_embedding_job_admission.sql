CREATE OR REPLACE FUNCTION corpus_legacy_purpose_admission() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
 IF NEW.kind IN ('GenerateMeetingSummary','ExtractEntities','ExtractRelationships','IngestDoor','RegenerateDigest','EntityResolution','ConflictDetection','InferExpertise','ReevaluateKind','BackfillAtomTitles','InferRelationships','ExtractThreads','ExtractDecisions') AND NEW.status::text IN ('pending','running','failed') THEN
  NEW.status='dead'; NEW.finished_at=now();
  NEW.error_message='managed_purpose_unsupported';
 END IF;
 RETURN NEW;
END $$;
