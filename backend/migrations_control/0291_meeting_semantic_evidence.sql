-- Immutable meeting source snapshots and evidence-backed semantic memory.
-- Unicode scalar passage offsets are zero-based, end-exclusive, into raw text.
CREATE TABLE meeting_source_versions (
 version_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
 graph_id UUID NOT NULL REFERENCES graphs(id) ON DELETE CASCADE,
 source_id TEXT NOT NULL,
 captured_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 is_legacy_baseline BOOLEAN NOT NULL DEFAULT FALSE,
 raw_blob_key TEXT, raw_inline TEXT, content_sha256 TEXT,
 duration_ms BIGINT, segments JSONB,
 extraction_status TEXT NOT NULL DEFAULT 'not_started'
   CHECK (extraction_status IN ('not_started','pending','running','done','failed','blocked')),
 extraction_error TEXT, next_start_char BIGINT NOT NULL DEFAULT 0,
 UNIQUE(version_id,graph_id), UNIQUE(version_id,graph_id,source_id),
 FOREIGN KEY(graph_id,source_id) REFERENCES meeting_transcripts(graph_id,source_id) ON DELETE CASCADE
);
CREATE INDEX meeting_versions_source ON meeting_source_versions(graph_id,source_id,captured_at DESC,version_id DESC);
ALTER TABLE meeting_transcripts ADD COLUMN current_version_id UUID;
-- Keep the last permitted pre-feature snapshot without fetching historical blobs.
INSERT INTO meeting_source_versions(graph_id,source_id,captured_at,is_legacy_baseline,raw_blob_key,raw_inline,duration_ms,segments)
 SELECT graph_id,source_id,now(),TRUE,raw_blob_key,raw_inline,duration_ms,segments
 FROM meeting_transcripts WHERE raw_blob_key IS NOT NULL OR raw_inline IS NOT NULL;
UPDATE meeting_transcripts m SET current_version_id=v.version_id
 FROM meeting_source_versions v WHERE v.graph_id=m.graph_id AND v.source_id=m.source_id;
ALTER TABLE meeting_transcripts ADD CONSTRAINT meeting_current_version_source
 FOREIGN KEY(current_version_id,graph_id,source_id)
 REFERENCES meeting_source_versions(version_id,graph_id,source_id) ON DELETE SET NULL (current_version_id) DEFERRABLE INITIALLY DEFERRED;
CREATE TABLE semantic_atoms (
 semantic_atom_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
 graph_id UUID NOT NULL, source_version_id UUID NOT NULL,
 ordinal INTEGER NOT NULL CHECK(ordinal>=0),
 kind TEXT NOT NULL CHECK(kind IN ('fact','decision','action','observation')),
 statement TEXT NOT NULL CHECK(length(trim(statement))>0),
 confidence REAL CHECK(confidence>=0 AND confidence<=1),
 extraction_version TEXT NOT NULL, model TEXT NOT NULL, entities JSONB NOT NULL DEFAULT '[]',
 created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 UNIQUE(semantic_atom_id,source_version_id,graph_id), UNIQUE(semantic_atom_id,graph_id),
 UNIQUE(source_version_id,extraction_version,ordinal),
 FOREIGN KEY(source_version_id,graph_id) REFERENCES meeting_source_versions(version_id,graph_id) ON DELETE CASCADE
);
CREATE TABLE semantic_atom_passages (
 passage_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
 semantic_atom_id UUID NOT NULL, source_version_id UUID NOT NULL, graph_id UUID NOT NULL,
 start_char INTEGER NOT NULL CHECK(start_char>=0), end_char INTEGER NOT NULL CHECK(end_char>start_char),
 quote TEXT NOT NULL CHECK(length(quote)>0), start_ms BIGINT, end_ms BIGINT, speaker TEXT,
 UNIQUE(semantic_atom_id,start_char,end_char),
 FOREIGN KEY(semantic_atom_id,source_version_id,graph_id)
 REFERENCES semantic_atoms(semantic_atom_id,source_version_id,graph_id) ON DELETE CASCADE
);
CREATE UNIQUE INDEX nodes_id_graph_evidence ON nodes(node_id,graph_id);
CREATE TABLE node_semantic_atoms (
 node_id UUID NOT NULL, semantic_atom_id UUID NOT NULL, graph_id UUID NOT NULL,
 PRIMARY KEY(node_id,semantic_atom_id),
 FOREIGN KEY(node_id,graph_id) REFERENCES nodes(node_id,graph_id) ON DELETE CASCADE,
 FOREIGN KEY(semantic_atom_id,graph_id) REFERENCES semantic_atoms(semantic_atom_id,graph_id) ON DELETE CASCADE
);
CREATE INDEX node_semantic_atoms_atom ON node_semantic_atoms(semantic_atom_id);
CREATE TABLE semantic_atom_windows (
 source_version_id UUID NOT NULL, graph_id UUID NOT NULL, extraction_version TEXT NOT NULL,
 start_char INTEGER NOT NULL CHECK(start_char>=0), end_char INTEGER NOT NULL CHECK(end_char>start_char),
 completed_at TIMESTAMPTZ NOT NULL DEFAULT now(), model TEXT, job_id UUID,
 PRIMARY KEY(source_version_id,extraction_version,start_char),
 FOREIGN KEY(source_version_id,graph_id) REFERENCES meeting_source_versions(version_id,graph_id) ON DELETE CASCADE
);
ALTER TABLE meeting_source_versions ENABLE ROW LEVEL SECURITY;
CREATE POLICY account_isolation ON meeting_source_versions USING (graph_id IN (SELECT id FROM graphs WHERE account_id = current_setting('app.account_id', true)::uuid)) WITH CHECK (graph_id IN (SELECT id FROM graphs WHERE account_id = current_setting('app.account_id', true)::uuid));
ALTER TABLE semantic_atoms ENABLE ROW LEVEL SECURITY;
CREATE POLICY account_isolation ON semantic_atoms USING (graph_id IN (SELECT id FROM graphs WHERE account_id = current_setting('app.account_id', true)::uuid)) WITH CHECK (graph_id IN (SELECT id FROM graphs WHERE account_id = current_setting('app.account_id', true)::uuid));
ALTER TABLE semantic_atom_passages ENABLE ROW LEVEL SECURITY;
CREATE POLICY account_isolation ON semantic_atom_passages USING (graph_id IN (SELECT id FROM graphs WHERE account_id = current_setting('app.account_id', true)::uuid)) WITH CHECK (graph_id IN (SELECT id FROM graphs WHERE account_id = current_setting('app.account_id', true)::uuid));
ALTER TABLE node_semantic_atoms ENABLE ROW LEVEL SECURITY;
CREATE POLICY account_isolation ON node_semantic_atoms USING (graph_id IN (SELECT id FROM graphs WHERE account_id = current_setting('app.account_id', true)::uuid)) WITH CHECK (graph_id IN (SELECT id FROM graphs WHERE account_id = current_setting('app.account_id', true)::uuid));
ALTER TABLE semantic_atom_windows ENABLE ROW LEVEL SECURITY;
CREATE POLICY account_isolation ON semantic_atom_windows USING (graph_id IN (SELECT id FROM graphs WHERE account_id = current_setting('app.account_id', true)::uuid)) WITH CHECK (graph_id IN (SELECT id FROM graphs WHERE account_id = current_setting('app.account_id', true)::uuid));
-- Bounded job-state recovery for a worker that dies before updating the version.
CREATE INDEX jobs_meeting_atom_version ON jobs ((payload->>'source_version_id'),created_at DESC)
 WHERE kind='ExtractMeetingAtoms';
