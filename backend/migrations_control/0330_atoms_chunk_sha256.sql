-- PAIRED WITH: migrations_tenant/0163_atoms_chunk_sha256.sql (byte-identical
-- body; `atoms` is a shared content table, see scripts/check_schema_parity.sh).
--
-- `atoms.chunk_sha256`: lowercase hex SHA-256 of one positional retrieval
-- chunk's canonical text, i.e. the persisted text with its first
-- `header_len` bytes (the contextual header) removed. Written by
-- `chunk_reconcile` when a capture inserts, adopts or replaces the chunk at
-- `{source_id}:chunk:{position}`; read by the same module to decide, per
-- position, whether a re-capture's chunk is the stored one (adopt and link it
-- to the new source version) or an edit (replace the content in place through
-- the `content_replacement` revision CAS).
--
-- NULL means "unknown": every atom written before this migration, and any
-- atom whose content changed through a writer that did not supply a new hash.
-- Readers fall back to hashing the stored content (inline or the decrypted
-- blob), so NULL is never treated as a match. Nullable with no default, so
-- the column add is catalog-only (no table rewrite, no backfill).
--
-- The trigger keeps the hash honest without trusting every writer: an UPDATE
-- that changes the content, blob locator or header length and does not set a
-- new hash in the SAME statement clears it. Not part of any unique key, and
-- not part of `reef_identity_atom_fingerprint` (that function lists its
-- columns explicitly), so stamping a hash never bumps `content_revision`.
--
-- Down:
--   DROP TRIGGER IF EXISTS atoms_chunk_sha256_invalidate ON atoms;
--   DROP FUNCTION IF EXISTS atoms_chunk_sha256_invalidate();
--   ALTER TABLE atoms DROP COLUMN IF EXISTS chunk_sha256;
ALTER TABLE atoms ADD COLUMN IF NOT EXISTS chunk_sha256 TEXT;
DO $$ BEGIN
 IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='atoms_chunk_sha256_hex' AND conrelid='atoms'::regclass) THEN
  ALTER TABLE atoms ADD CONSTRAINT atoms_chunk_sha256_hex CHECK (chunk_sha256 IS NULL OR chunk_sha256 ~ '^[0-9a-f]{64}$') NOT VALID;
 END IF;
END $$;
COMMENT ON COLUMN atoms.chunk_sha256 IS
    'Lowercase hex SHA-256 of this positional chunk''s text after its first '
    'header_len bytes. Written by chunk_reconcile; NULL = unknown (hash the '
    'stored content instead). Cleared by trigger when content changes without '
    'a new hash. Never an identity or conflict key.';
CREATE OR REPLACE FUNCTION atoms_chunk_sha256_invalidate() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
 IF (NEW.content_inline, NEW.content_blob_key, NEW.header_len) IS DISTINCT FROM
    (OLD.content_inline, OLD.content_blob_key, OLD.header_len)
    AND NEW.chunk_sha256 IS NOT DISTINCT FROM OLD.chunk_sha256 THEN
  NEW.chunk_sha256 := NULL;
 END IF;
 RETURN NEW;
END $$;
REVOKE ALL ON FUNCTION atoms_chunk_sha256_invalidate() FROM PUBLIC;
DROP TRIGGER IF EXISTS atoms_chunk_sha256_invalidate ON atoms;
CREATE TRIGGER atoms_chunk_sha256_invalidate
 BEFORE UPDATE OF content_inline, content_blob_key, header_len ON atoms
 FOR EACH ROW EXECUTE FUNCTION atoms_chunk_sha256_invalidate();
