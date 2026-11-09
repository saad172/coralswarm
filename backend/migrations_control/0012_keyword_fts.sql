-- Migration: 0012_keyword_fts
-- Description: Add tsvector column to atoms for BM25 full-text search
--
-- SECURITY: content_tsv stores plaintext for full-text search tokenisation.
-- tsvector requires unencrypted input, so this column is intentionally excluded
-- from the at-rest encryption scope applied to content_blob_key / embedding.
--
-- Down:
--   DROP INDEX IF EXISTS idx_atoms_content_tsv;
--   ALTER TABLE atoms DROP COLUMN IF EXISTS content_tsv;

ALTER TABLE atoms ADD COLUMN IF NOT EXISTS content_tsv tsvector;

CREATE INDEX IF NOT EXISTS idx_atoms_content_tsv
    ON atoms USING GIN (content_tsv);
