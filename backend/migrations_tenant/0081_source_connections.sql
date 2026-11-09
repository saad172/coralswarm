-- Durable publication consent, evidence revision receipts, and processing dependencies.
-- Same-database only. Owner UUID is deliberately bare on the tenant plane.
ALTER TABLE atoms ADD COLUMN content_revision BIGINT NOT NULL DEFAULT 1;
ALTER TABLE atoms ADD COLUMN promoted_from_atom_id UUID;
ALTER TABLE atoms ADD COLUMN promoted_from_revision BIGINT;
CREATE FUNCTION bump_atom_content_revision() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF (NEW.content_inline, NEW.content_blob_key, NEW.timestamp) IS DISTINCT FROM
     (OLD.content_inline, OLD.content_blob_key, OLD.timestamp) THEN
    NEW.content_revision := OLD.content_revision + 1;
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER atoms_content_revision BEFORE UPDATE ON atoms
FOR EACH ROW EXECUTE FUNCTION bump_atom_content_revision();

-- Queue metadata follows jobs' explicit account checks, not content RLS.
CREATE TABLE job_dependencies (
  parent_id UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
  child_id UUID NOT NULL REFERENCES jobs(id) ON DELETE RESTRICT,
  PRIMARY KEY(parent_id, child_id), CHECK(parent_id <> child_id)
);
CREATE INDEX job_dependencies_child ON job_dependencies(child_id);

CREATE TABLE source_connections (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  account_id UUID NOT NULL,
  owner_id UUID NOT NULL,
  source_graph_id UUID NOT NULL REFERENCES graphs(id) ON DELETE CASCADE,
  target_graph_id UUID NOT NULL REFERENCES graphs(id) ON DELETE CASCADE,
  source_type TEXT NOT NULL,
  source_id TEXT NOT NULL,
  match_prefix BOOLEAN NOT NULL DEFAULT false,
  workspace_id TEXT,
  label TEXT NOT NULL,
  mode TEXT NOT NULL CHECK(mode IN ('once','live')),
  paused BOOLEAN NOT NULL DEFAULT false,
  include_history BOOLEAN NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  next_check_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_checked_at TIMESTAMPTZ,
  completed_at TIMESTAMPTZ,
  current_job_id UUID REFERENCES jobs(id),
  error_message TEXT,
  CHECK(source_graph_id <> target_graph_id)
);
CREATE UNIQUE INDEX source_connections_identity ON source_connections
  (source_graph_id, target_graph_id, source_type, source_id, match_prefix, COALESCE(workspace_id, ''));
CREATE INDEX source_connections_due ON source_connections(next_check_at) WHERE NOT paused;
CREATE TABLE source_connection_atoms (
  connection_id UUID NOT NULL REFERENCES source_connections(id) ON DELETE CASCADE,
  atom_id UUID NOT NULL REFERENCES atoms(atom_id) ON DELETE CASCADE,
  revision BIGINT NOT NULL,
  target_atom_id UUID NOT NULL REFERENCES atoms(atom_id) ON DELETE CASCADE,
  PRIMARY KEY(connection_id, atom_id)
);
ALTER TABLE source_connections ENABLE ROW LEVEL SECURITY;
CREATE POLICY source_connection_scope ON source_connections USING (
  account_id = current_setting('app.account_id', true)::uuid
  AND source_graph_id IN (SELECT id FROM graphs)
  AND target_graph_id IN (SELECT id FROM graphs)
) WITH CHECK (
  account_id = current_setting('app.account_id', true)::uuid
  AND source_graph_id IN (SELECT id FROM graphs)
  AND target_graph_id IN (SELECT id FROM graphs)
);
ALTER TABLE source_connection_atoms ENABLE ROW LEVEL SECURITY;
CREATE POLICY source_connection_atom_scope ON source_connection_atoms USING (
  connection_id IN (SELECT id FROM source_connections)
) WITH CHECK (connection_id IN (SELECT id FROM source_connections));

DO $$ BEGIN
  IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname='coralswarm_tenant_app') THEN
    GRANT SELECT, INSERT, UPDATE, DELETE ON source_connections, source_connection_atoms, job_dependencies TO coralswarm_tenant_app;
  END IF;
  IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname='coralswarm_consumer_app') THEN
    GRANT SELECT, INSERT, UPDATE, DELETE ON source_connections, source_connection_atoms, job_dependencies TO coralswarm_consumer_app;
  END IF;
END $$;
