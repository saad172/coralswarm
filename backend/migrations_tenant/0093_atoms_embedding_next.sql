-- Cloud embedding hold: a second vector beside the live bge vector.
-- Byte-identical DDL to migrations_control/0230_atoms_embedding_next.sql.
-- `atoms` is shared across both trees; see that file for why the side
-- column exists and why search does not read it yet.

ALTER TABLE atoms ADD COLUMN IF NOT EXISTS embedding_next vector(384);
ALTER TABLE atoms ADD COLUMN IF NOT EXISTS embed_model_next text;
