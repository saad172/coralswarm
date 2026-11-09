-- Cloud embedding hold: a second vector beside the live bge vector.
--
-- Search keeps reading `embedding` / `embed_model` (bge-small-en-v1.5) until
-- every server row that should have a cloud vector has one. New writes and
-- the re-embed sweep fill `embedding_next` / `embed_model_next` without
-- replacing the live vector, so retrieval never drops to keyword-only during
-- the backfill.
--
-- The HNSW index is the next migration (`-- no-transaction`, CREATE INDEX
-- CONCURRENTLY). This file only adds the columns.

ALTER TABLE atoms ADD COLUMN IF NOT EXISTS embedding_next vector(384);
ALTER TABLE atoms ADD COLUMN IF NOT EXISTS embed_model_next text;
