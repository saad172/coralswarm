-- Generalize preserved meeting evidence without changing any existing UUID.
-- The final stack deploys together; the intermediate meeting-only binary is not
-- compatible with this renamed relation and is not a supported rollback target.
CREATE TABLE source_items (
 source_item_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
 graph_id UUID NOT NULL REFERENCES graphs(id) ON DELETE CASCADE,
 source_type TEXT NOT NULL CHECK(length(source_type) BETWEEN 1 AND 100),
 provider_namespace TEXT NOT NULL CHECK(length(provider_namespace) BETWEEN 1 AND 512),
 source_id TEXT NOT NULL CHECK(length(source_id)>0),
 current_version_id UUID,
 created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 UNIQUE(source_item_id,graph_id),
 UNIQUE(graph_id,source_type,provider_namespace,source_id)
);
CREATE INDEX source_items_inventory ON source_items(graph_id,updated_at DESC,source_item_id DESC);
ALTER TABLE meeting_transcripts ADD COLUMN source_item_id UUID;
INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id,current_version_id)
 SELECT graph_id,'transcript','legacy:meeting',source_id,current_version_id FROM meeting_transcripts;
UPDATE meeting_transcripts m SET source_item_id=s.source_item_id FROM source_items s
 WHERE s.graph_id=m.graph_id AND s.source_id=m.source_id
 AND s.source_type='transcript' AND s.provider_namespace='legacy:meeting';
ALTER TABLE meeting_transcripts ADD CONSTRAINT meeting_source_item
 FOREIGN KEY(source_item_id,graph_id) REFERENCES source_items(source_item_id,graph_id) ON DELETE CASCADE;
ALTER TABLE meeting_source_versions RENAME TO source_versions;
ALTER TABLE source_versions DROP CONSTRAINT meeting_source_versions_graph_id_source_id_fkey;
ALTER TABLE source_versions
 ADD COLUMN source_item_id UUID,
 ADD COLUMN source_type TEXT NOT NULL DEFAULT 'transcript',
 ADD COLUMN provider_namespace TEXT NOT NULL DEFAULT 'legacy:meeting',
 ADD COLUMN extraction_version TEXT NOT NULL DEFAULT 'meeting-atoms-v1',
 ADD COLUMN representation TEXT NOT NULL DEFAULT 'normalized_text'
   CHECK(representation IN ('original_text','normalized_text','provider_summary','generated_summary')),
 ADD COLUMN source_metadata JSONB NOT NULL DEFAULT '{}',
 ADD COLUMN occurred_at TIMESTAMPTZ,
 ADD COLUMN participants TEXT[] NOT NULL DEFAULT '{}',
 ADD COLUMN ingested_by UUID,
 ADD COLUMN scope TEXT NOT NULL DEFAULT 'private',
 ADD COLUMN fingerprint_sha256 TEXT;
UPDATE source_versions v SET source_item_id=m.source_item_id FROM meeting_transcripts m
 WHERE m.graph_id=v.graph_id AND m.source_id=v.source_id;
ALTER TABLE source_versions ALTER COLUMN source_item_id SET NOT NULL;
ALTER TABLE source_versions ALTER COLUMN extraction_version SET DEFAULT 'source-atoms-v1';
ALTER TABLE source_versions ADD CONSTRAINT source_version_item
 FOREIGN KEY(source_item_id,graph_id) REFERENCES source_items(source_item_id,graph_id) ON DELETE CASCADE;
ALTER TABLE source_versions ADD CONSTRAINT source_version_item_identity UNIQUE(version_id,source_item_id,graph_id);
ALTER TABLE source_items ADD CONSTRAINT source_item_current_version
 FOREIGN KEY(current_version_id,source_item_id,graph_id)
 REFERENCES source_versions(version_id,source_item_id,graph_id)
 ON DELETE SET NULL (current_version_id) DEFERRABLE INITIALLY DEFERRED;
CREATE INDEX source_versions_item_history ON source_versions(graph_id,source_item_id,captured_at DESC,version_id DESC);
CREATE UNIQUE INDEX atoms_id_graph_source_evidence ON atoms(atom_id,graph_id);
CREATE TABLE source_version_chunks (
 graph_id UUID NOT NULL,
 source_version_id UUID NOT NULL,
 atom_id UUID NOT NULL,
 PRIMARY KEY(source_version_id,atom_id),
 FOREIGN KEY(source_version_id,graph_id) REFERENCES source_versions(version_id,graph_id) ON DELETE CASCADE,
 FOREIGN KEY(atom_id,graph_id) REFERENCES atoms(atom_id,graph_id) ON DELETE CASCADE
);
CREATE INDEX source_version_chunks_atom ON source_version_chunks(atom_id,graph_id);
DROP INDEX jobs_meeting_atom_version;
CREATE INDEX jobs_source_atom_version ON jobs ((payload->>'source_version_id'),created_at DESC)
 WHERE kind IN ('ExtractMeetingAtoms','ExtractSourceAtoms');
ALTER TABLE source_items ENABLE ROW LEVEL SECURITY;
CREATE POLICY account_isolation ON source_items USING (graph_id IN (SELECT id FROM graphs WHERE account_id = current_setting('app.account_id', true)::uuid)) WITH CHECK (graph_id IN (SELECT id FROM graphs WHERE account_id = current_setting('app.account_id', true)::uuid));
ALTER TABLE source_version_chunks ENABLE ROW LEVEL SECURITY;
CREATE POLICY account_isolation ON source_version_chunks USING (graph_id IN (SELECT id FROM graphs WHERE account_id = current_setting('app.account_id', true)::uuid)) WITH CHECK (graph_id IN (SELECT id FROM graphs WHERE account_id = current_setting('app.account_id', true)::uuid));
