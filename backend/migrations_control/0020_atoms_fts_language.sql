-- Migration 0020: per-atom FTS language
-- Adds fts_language column to atoms so each atom can be indexed with the
-- correct PostgreSQL text-search configuration (e.g. 'english', 'french').

ALTER TABLE atoms ADD COLUMN IF NOT EXISTS fts_language TEXT NOT NULL DEFAULT 'english';

-- Backfill existing rows (all 'english' since that was the only language).
UPDATE atoms SET fts_language = 'english' WHERE fts_language IS NULL;

-- Sparse index: most atoms will be 'english', so only index non-english rows
-- to keep the index small while still supporting language-specific queries.
CREATE INDEX IF NOT EXISTS idx_atoms_fts_language ON atoms(fts_language) WHERE fts_language != 'english';
