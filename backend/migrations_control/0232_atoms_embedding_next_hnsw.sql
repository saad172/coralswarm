-- no-transaction
-- HNSW for the cloud side column added in 0231. CONCURRENTLY cannot run
-- inside the migration transaction, so this file is alone.
--
-- Partial: rows still on the bge-only hold have a NULL side vector and must
-- not sit in the graph. m / ef_construction match atoms_embedding_hnsw_idx.

CREATE INDEX CONCURRENTLY IF NOT EXISTS atoms_embedding_next_hnsw_idx
    ON atoms USING hnsw (embedding_next vector_cosine_ops)
    WITH (m = 16, ef_construction = 64)
    WHERE embedding_next IS NOT NULL;
