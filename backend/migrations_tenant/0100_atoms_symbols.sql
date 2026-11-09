-- Migration: 0100_atoms_symbols (tenant tree)
-- Description: Tenant-tree half of the paired `atoms.symbols` change. See
--   `migrations_control/0248_atoms_symbols.sql` for the full rationale —
--   the residual fix-3b left open (a code chunk's header truncates its
--   declaration list to three plus "+N more", and `atoms.excerpt` is
--   derived from that same truncated text), why this is a new column
--   rather than widening `excerpt`, why it is a plain nullable `TEXT`
--   column rather than a generated one, and why there is no backfill.
--
--   `atoms` is one of the content tables shared by both trees, so this MUST
--   stay byte-equivalent to the control side or
--   `scripts/check_schema_parity.sh` fails: it applies each tree to a
--   scratch DB and diffs a normalized `pg_dump --schema-only` of the shared
--   tables. Keep the column type, nullability, and `COMMENT` text identical
--   to 0247.
--
--   Idempotent (`IF NOT EXISTS`) like every other tenant-tree column add,
--   since tenant databases are provisioned at different times and the
--   orchestrator may re-run a tree against a DB that is already partially
--   ahead.

ALTER TABLE atoms ADD COLUMN IF NOT EXISTS symbols TEXT;

COMMENT ON COLUMN atoms.symbols IS
    'Newline-joined "{kind} {name}" spelling of every declaration this '
    'chunk contains (coralswarm_core::chunker::Chunk::symbols), UNCAPPED — '
    'unlike the items … header, which truncates to three plus "+N more" '
    'for display. Persisted at ingest time by routes::ingest via '
    'Store::insert_atoms_scoped, from the same in-memory Chunk excerpt/ '
    'title already derive from. NULL means no declarations (a non-code '
    'chunk, or an all-decorative code chunk), not "not yet backfilled" — '
    'existing atoms stay NULL until a real re-ingest, see this migration''s '
    'header. Read by store::keyword_search_pg''s symbol candidate leg via '
    'idx_atoms_symbols_tsv (migration 0248/0101).';
