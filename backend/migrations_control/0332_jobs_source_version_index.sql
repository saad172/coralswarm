-- no-transaction
-- PAIRED WITH: tenant 0165 (DDL byte-identical).
-- Standing adoption (`corpus_enrichment::admit_tx`) and the standing requeue
-- look up active extraction jobs by `payload->>'source_version_id'` with no
-- kind filter, so the kind-partial jobs_source_atom_version (0127) never
-- serves them and each adoption scanned the whole jobs table. The predicate is
-- implied by any equality on the same expression.
CREATE INDEX CONCURRENTLY IF NOT EXISTS jobs_payload_source_version
 ON jobs ((payload->>'source_version_id'))
 WHERE (payload->>'source_version_id') IS NOT NULL;
