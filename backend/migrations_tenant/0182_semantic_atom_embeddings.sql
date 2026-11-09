-- PAIRED WITH: control 0351 (semantic_atom_embeddings).
-- Statement embeddings for scoped Ask's statement-vector leg. Paired
-- tenant/control migrations with byte-identical DDL except the header and
-- the plane's RLS policy.
--
-- 1. One cloud vector per semantic atom's statement, written only by the
--    managed `cloud_embedding` purpose (`managed::embedding`, job kind
--    `EmbedStatements`): every statement sent is receipted in
--    `managed_embedding_inputs` (subject 'statement') under the account's
--    daily cap. `embed_model` tags the vector
--    (`openai/text-embedding-3-small@384`, the chunk model); a row whose tag
--    or `statement_sha256` no longer matches is stale, never scored, and the
--    sweep re-embeds it. A held statement (daily cap, cloud processing off,
--    access lost) keeps `held_reason` and waits for `retry_after`.
--    The Ask reader joins on the primary key from the already-permitted
--    scope set and scores exactly (no approximate index): the scope is a
--    filtered subset, and Ask ranking must be deterministic.
CREATE TABLE semantic_atom_embeddings (
 graph_id UUID NOT NULL,
 semantic_atom_id UUID NOT NULL,
 embed_model TEXT NOT NULL CHECK (length(embed_model) BETWEEN 1 AND 128),
 statement_sha256 BYTEA NOT NULL CHECK (octet_length(statement_sha256) = 32),
 embedding vector(384),
 receipt_id UUID,
 held_reason TEXT CHECK (held_reason IS NULL OR length(held_reason) BETWEEN 1 AND 64),
 attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
 retry_after TIMESTAMPTZ,
 updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 PRIMARY KEY (graph_id, semantic_atom_id),
 CHECK (embedding IS NOT NULL OR held_reason IS NOT NULL),
 FOREIGN KEY (semantic_atom_id, graph_id)
  REFERENCES semantic_atoms (semantic_atom_id, graph_id) ON DELETE CASCADE
);
-- The backfill sweep skips held statements until their retry time.
CREATE INDEX semantic_atom_embeddings_held ON semantic_atom_embeddings (graph_id, retry_after)
 WHERE embedding IS NULL;

-- 2. Statement inputs share the embedding receipt table. `atom_id` carries
--    the semantic atom id for subject 'statement' (statements are immutable,
--    so their revision is 1). Statements are short, so a statement batch may
--    hold up to 64 inputs; chunk batches stay at 8.
ALTER TABLE managed_embedding_inputs
 ADD COLUMN subject TEXT NOT NULL DEFAULT 'chunk' CHECK (subject IN ('chunk','statement'));
DO $$ DECLARE c TEXT; BEGIN
 FOR c IN SELECT conname FROM pg_constraint WHERE conrelid='managed_embedding_inputs'::regclass
   AND contype='c' AND pg_get_constraintdef(oid) LIKE '%ordinal%' LOOP
  EXECUTE format('ALTER TABLE managed_embedding_inputs DROP CONSTRAINT %I',c);
 END LOOP;
END $$;
ALTER TABLE managed_embedding_inputs ADD CONSTRAINT managed_embedding_inputs_ordinal_check
 CHECK (ordinal BETWEEN 0 AND 63 AND (subject = 'statement' OR ordinal <= 7));

ALTER TABLE semantic_atom_embeddings ENABLE ROW LEVEL SECURITY;
DO $$ BEGIN
 IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'coralswarm_app') THEN
  GRANT SELECT,INSERT,UPDATE,DELETE ON semantic_atom_embeddings TO coralswarm_app;
 END IF;
 IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'coralswarm_tenant_app') THEN
  GRANT SELECT,INSERT,UPDATE,DELETE ON semantic_atom_embeddings TO coralswarm_tenant_app;
 END IF;
END $$;

-- Tenant plane: the same member-ocean visibility as semantic_atoms (0126).
CREATE POLICY member_ocean_isolation ON semantic_atom_embeddings
  USING (graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids()))
  WITH CHECK (graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids()));
