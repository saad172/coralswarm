-- Migration: 0063_atom_embed_versioning
-- Description: Track which embedding model + chunker version produced each
--   atom's vector, so retrieval never silently mixes incompatible embedding
--   spaces (vectors from different models don't share a coordinate space —
--   comparing them via cosine distance produces meaningless scores).
--
--   The backfill of existing rows is deliberately NOT done here: `atoms` is
--   a live, read/write-heavy production table (see 0059's header), and a
--   single unbounded `UPDATE atoms SET ...` over every historical row would
--   hold row locks for the whole statement's duration, competing with live
--   ingest writes for as long as the backfill takes. Instead this migration
--   only adds the columns (fast, no table rewrite) and defines a batched
--   backfill procedure; migration 0064 CALLs it as a standalone,
--   non-transactional statement (same `-- no-transaction` mechanism 0059
--   uses for `CREATE INDEX CONCURRENTLY`) so each batch commits on its own
--   and releases its locks before the next batch starts.
--
-- Down:
--   DROP PROCEDURE IF EXISTS backfill_atom_embed_versioning(INT);
--   ALTER TABLE atoms DROP COLUMN IF EXISTS embed_model;
--   ALTER TABLE atoms DROP COLUMN IF EXISTS chunker_version;

ALTER TABLE atoms ADD COLUMN IF NOT EXISTS embed_model TEXT;
ALTER TABLE atoms ADD COLUMN IF NOT EXISTS chunker_version SMALLINT;

-- Backfill: every atom embedded before this migration was produced by the
-- only embedding model + chunker CoralSwarm has ever run in production
-- (bge-small-en-v1.5 / chunker v1 — see `coralswarm_core::embed::EMBED_MODEL_NAME`
-- and `coralswarm_core::chunker::CHUNKER_VERSION`). Stamping them here rather
-- than leaving `embed_model` NULL means `vector_search`'s new model filter
-- (added alongside this migration) treats the entire pre-migration corpus as
-- current-model rows instead of excluding it from vector search overnight —
-- NULL is treated as "unknown, do not compare" (excluded), not as
-- "compatible with the current model", so this backfill is what keeps
-- existing atoms searchable.
--
-- Batched in `batch_size`-row chunks, each committed on its own (see
-- migration 0064, which calls this procedure), so no single transaction
-- holds locks across the entire historical corpus at once.
CREATE OR REPLACE PROCEDURE backfill_atom_embed_versioning(batch_size INT DEFAULT 5000)
LANGUAGE plpgsql
AS $$
DECLARE
    rows_updated INT;
BEGIN
    LOOP
        UPDATE atoms
        SET    embed_model     = 'bge-small-en-v1.5',
               chunker_version = 1
        WHERE  atom_id IN (
            SELECT atom_id
            FROM   atoms
            WHERE  embedding IS NOT NULL
              AND  embed_model IS NULL
            LIMIT  batch_size
        );
        GET DIAGNOSTICS rows_updated = ROW_COUNT;
        COMMIT;
        EXIT WHEN rows_updated = 0;
    END LOOP;
END;
$$;
