-- PAIRED WITH: migrations_control/0360_embed_input_bounds.sql (byte-identical
-- body; `atoms` and `managed_embedding_inputs` are shared tables, see
-- scripts/check_schema_parity.sh).
--
-- The database backstop for the embedding gateway's size rule
-- (`coralswarm_core::embed_gateway`). The gateway splits any text over the
-- provider's per-input limit and pools the pieces' vectors, so the only
-- text it can never embed is one over `REQUEST_MAX_BYTES` (262144 bytes),
-- which no single provider request may carry. These constraints refuse such
-- text where it is stored and where it is receipted:
--
--   * `atoms.content_inline` (inline atom text; R2-backed atoms keep it
--     NULL). Chunks are at most `chunker::MAX_CHUNK_BYTES` (6000) plus a
--     contextual header, far below the bound; the bound is the gateway's
--     ceiling, not the chunk size, so a desktop sync push of a legacy
--     unchunked atom up to 256 KiB still applies.
--   * `managed_embedding_inputs.input_bytes`, the receipted size of every
--     managed embedding input whatever its storage. The gateway refuses an
--     over-limit text before the receipt is written, so a violation here is
--     a gateway bypass.
--
-- NOT VALID: legacy rows are not scanned and do not block the migration;
-- every new INSERT and UPDATE is checked. `lock_timeout` bounds the brief
-- ACCESS EXCLUSIVE lock the constraint add takes, so a busy table fails the
-- migration instead of queuing writers behind it.
--
-- Down:
--   ALTER TABLE atoms DROP CONSTRAINT IF EXISTS atoms_content_inline_embeddable;
--   ALTER TABLE managed_embedding_inputs DROP CONSTRAINT IF EXISTS managed_embedding_inputs_input_bytes_embeddable;
SET LOCAL lock_timeout = '3s';
DO $$ BEGIN
 IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='atoms_content_inline_embeddable' AND conrelid='atoms'::regclass) THEN
  ALTER TABLE atoms ADD CONSTRAINT atoms_content_inline_embeddable
    CHECK (content_inline IS NULL OR octet_length(content_inline) <= 262144) NOT VALID;
 END IF;
 IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='managed_embedding_inputs_input_bytes_embeddable' AND conrelid='managed_embedding_inputs'::regclass) THEN
  ALTER TABLE managed_embedding_inputs ADD CONSTRAINT managed_embedding_inputs_input_bytes_embeddable
    CHECK (input_bytes <= 262144) NOT VALID;
 END IF;
END $$;
