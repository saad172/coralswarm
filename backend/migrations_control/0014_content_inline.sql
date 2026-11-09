-- Migration: 0014_content_inline
-- Description: Add content_inline column to atoms for dev-mode operation without R2.
--
-- In production, atom content is encrypted and stored in R2 (content_blob_key).
-- When R2 is not configured (local development), content is stored here instead.
--
-- Down:
--   ALTER TABLE atoms DROP COLUMN IF EXISTS content_inline;

ALTER TABLE atoms ADD COLUMN IF NOT EXISTS content_inline TEXT;
