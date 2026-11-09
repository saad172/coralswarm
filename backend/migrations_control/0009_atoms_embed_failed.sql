-- Migration: 0009_atoms_embed_failed
-- Description: Add embed_failed_at column to atoms for tracking embedding failures
--
-- Down:
--   DROP INDEX IF EXISTS idx_atoms_embed_failed_at;
--   ALTER TABLE atoms DROP COLUMN IF EXISTS embed_failed_at;

ALTER TABLE atoms ADD COLUMN IF NOT EXISTS embed_failed_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_atoms_embed_failed_at
    ON atoms (embed_failed_at)
    WHERE embed_failed_at IS NOT NULL;
