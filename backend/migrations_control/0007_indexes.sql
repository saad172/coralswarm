-- Add account_members user_id index and fix HNSW partial index

-- ── account_members(user_id) index ────────────────────────────────────────────
-- The auth middleware JOINs account_members by user_id on every authenticated
-- request. Without an index this is a sequential scan on every API call.
CREATE INDEX IF NOT EXISTS account_members_user_id_idx ON account_members(user_id);

-- ── HNSW partial index — exclude NULL embeddings ──────────────────────────────
-- The old index included rows where embedding IS NULL (atoms not yet embedded).
-- NULL rows cannot participate in ANN searches and waste HNSW graph space,
-- degrading query quality. Replace with a partial index that covers only atoms
-- that have been embedded.
DROP INDEX IF EXISTS idx_atoms_embedding_hnsw;
CREATE INDEX atoms_embedding_hnsw_idx
    ON atoms USING hnsw (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 64)
    WHERE embedding IS NOT NULL;
