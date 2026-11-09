-- no-transaction
-- Paired half of migrations_control/0231_atoms_embedding_next_hnsw.sql.
-- The DDL below is byte-identical to the control migration.

CREATE INDEX CONCURRENTLY IF NOT EXISTS atoms_embedding_next_hnsw_idx
    ON atoms USING hnsw (embedding_next vector_cosine_ops)
    WITH (m = 16, ef_construction = 64)
    WHERE embedding_next IS NOT NULL;
