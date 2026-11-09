ALTER TABLE managed_attempt_receipts
  ADD CONSTRAINT managed_receipt_input_binding UNIQUE (receipt_id, account_id, graph_id, purpose);

CREATE TABLE managed_embedding_inputs (
  receipt_id UUID NOT NULL,
  account_id UUID NOT NULL,
  graph_id UUID NOT NULL,
  purpose TEXT NOT NULL DEFAULT 'cloud_embedding' CHECK (purpose = 'cloud_embedding'),
  atom_id UUID NOT NULL,
  ordinal SMALLINT NOT NULL CHECK (ordinal BETWEEN 0 AND 7),
  content_revision BIGINT NOT NULL CHECK (content_revision > 0),
  content_sha256 BYTEA NOT NULL CHECK (octet_length(content_sha256) = 32),
  input_bytes BIGINT NOT NULL CHECK (input_bytes > 0),
  outcome TEXT NOT NULL DEFAULT 'sent' CHECK (outcome IN ('sent','written','discarded_stale')),
  PRIMARY KEY (receipt_id, ordinal),
  UNIQUE (receipt_id, atom_id),
  FOREIGN KEY (receipt_id, account_id, graph_id, purpose)
    REFERENCES managed_attempt_receipts (receipt_id, account_id, graph_id, purpose)
);
ALTER TABLE managed_embedding_inputs ENABLE ROW LEVEL SECURITY;
DO $$ BEGIN
 IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'coralswarm_app') THEN
  GRANT SELECT,INSERT,UPDATE ON managed_embedding_inputs TO coralswarm_app;
 END IF;
 IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'coralswarm_tenant_app') THEN
  GRANT SELECT,INSERT,UPDATE ON managed_embedding_inputs TO coralswarm_tenant_app;
 END IF;
END $$;
CREATE POLICY member_ocean_isolation ON managed_embedding_inputs FOR ALL
  USING (tenant_rls_is_system()) WITH CHECK (tenant_rls_is_system());
CREATE POLICY member_ocean_read ON managed_embedding_inputs FOR SELECT
  USING (tenant_rls_is_system() OR graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids()));
