-- PAIRED WITH: migrations_tenant/0167_fact_supersession.sql.
--
-- Source-agnostic pipeline PR 9: supersession over extraction v2's temporal
-- facts. The shared block below is byte-identical on both planes; only the
-- RLS policy at the end differs. Grants go to coralswarm_app and
-- coralswarm_tenant_app. The production tenant runtime connects as the table
-- OWNER, which owns this table (owner bypass, no FORCE) and inherits
-- coralswarm_tenant_app since tenant 0159.
--
-- 1. Claim-level supersession. `supersession_edge` relates two REEFS (the
--    legacy decision-reef model, node -> node, primary key on the node pair).
--    Extraction v2's facts are semantic atoms and stated relations about a
--    subject reef, so "Z owns Y from April" replacing "X owns Y until March"
--    is an edge between two FACTS about the same subject and state key. A
--    fact is (semantic atom, subject reef, state key, value); its stable key
--    is `fact_supersession::fact_key`, a 32-hex digest of those four.
--
--    status:
--      proposed   a deterministic decision recorded in dry run; readers ignore it
--      active     a live decision; readers treat the superseded fact as history
--      ambiguous  needs the metered judge (managed_enrichment); readers ignore it
--      rejected   the judge said the two facts are not a replacement
--      blocked    a human said the two values are not a supersession pair
--                 (`non_supersession_pair`)
--      curator    the judge could not decide; left for a human
--      unresolved the judge failed three times
--
--    Every row cascades from both semantic atoms and the subject reef, so
--    deleting a source retracts the supersession its facts produced.
CREATE TABLE fact_supersession_edges (
 edge_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
 graph_id UUID NOT NULL,
 subject_node_id UUID NOT NULL,
 state_key TEXT NOT NULL CHECK (state_key ~ '^[a-z][a-z0-9_.-]{0,63}$'),
 superseder_atom_id UUID NOT NULL,
 superseded_atom_id UUID NOT NULL,
 superseder_fact TEXT NOT NULL CHECK (superseder_fact ~ '^[0-9a-f]{32}$'),
 superseded_fact TEXT NOT NULL CHECK (superseded_fact ~ '^[0-9a-f]{32}$'),
 rule TEXT NOT NULL CHECK (rule IN ('explicit_end','later_start','replacement_language','judge')),
 kind TEXT NOT NULL DEFAULT 'change' CHECK (kind IN ('change','correction')),
 status TEXT NOT NULL CHECK (status IN ('proposed','active','ambiguous','rejected','blocked','curator','unresolved')),
 provenance JSONB NOT NULL DEFAULT '{}'::jsonb
  CHECK (jsonb_typeof(provenance)='object' AND octet_length(provenance::text)<=16384),
 judge_receipt_id UUID,
 judge_attempts SMALLINT NOT NULL DEFAULT 0 CHECK (judge_attempts BETWEEN 0 AND 16),
 judge_claimed_until TIMESTAMPTZ,
 created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 CHECK (superseder_fact<>superseded_fact),
 UNIQUE (graph_id,superseded_fact,superseder_fact),
 FOREIGN KEY (subject_node_id,graph_id) REFERENCES nodes(node_id,graph_id) ON DELETE CASCADE,
 FOREIGN KEY (superseder_atom_id,graph_id) REFERENCES semantic_atoms(semantic_atom_id,graph_id) ON DELETE CASCADE,
 FOREIGN KEY (superseded_atom_id,graph_id) REFERENCES semantic_atoms(semantic_atom_id,graph_id) ON DELETE CASCADE
);
CREATE INDEX fact_supersession_superseded ON fact_supersession_edges(graph_id,superseded_atom_id) WHERE status='active';
CREATE INDEX fact_supersession_superseder ON fact_supersession_edges(graph_id,superseder_atom_id);
CREATE INDEX fact_supersession_subject ON fact_supersession_edges(graph_id,subject_node_id,state_key);
CREATE INDEX fact_supersession_judge_queue ON fact_supersession_edges(created_at,edge_id) WHERE status='ambiguous';

-- 2. Idempotency for extraction-recorded temporal evidence: one record per
--    (atom, subject, key, operation, value, predecessor). A re-run of the
--    same version (the extraction hook, the backfill) inserts nothing new.
--    Human curation (`human_curated_v1`) is unaffected.
CREATE UNIQUE INDEX temporal_extraction_identity ON temporal_evidence_records
 (graph_id,semantic_atom_id,subject_node_id,state_key,operation,COALESCE(value,''),
  COALESCE(predecessor_event_id,'00000000-0000-0000-0000-000000000000'::uuid))
 WHERE recording_method='temporal_extraction_v1' AND lifecycle='supported';

ALTER TABLE fact_supersession_edges ENABLE ROW LEVEL SECURITY;
DO $$ BEGIN
 IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'coralswarm_app') THEN
  GRANT SELECT,INSERT,UPDATE,DELETE ON fact_supersession_edges TO coralswarm_app;
 END IF;
 IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'coralswarm_tenant_app') THEN
  GRANT SELECT,INSERT,UPDATE,DELETE ON fact_supersession_edges TO coralswarm_tenant_app;
 END IF;
END $$;

-- Control plane: one account per transaction (app.account_id), the same
-- policy as semantic_atoms (control 0291).
CREATE POLICY account_isolation ON fact_supersession_edges
  USING (graph_id IN (SELECT id FROM graphs WHERE account_id = current_setting('app.account_id', true)::uuid))
  WITH CHECK (graph_id IN (SELECT id FROM graphs WHERE account_id = current_setting('app.account_id', true)::uuid));
