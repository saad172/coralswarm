-- Migration: 0027_atoms_excerpt (tenant tree)
-- Description: Tenant-tree half of the paired `atoms.excerpt` change. See
--   `migrations_control/0116_atoms_excerpt.sql` for the full rationale (gap
--   9 of the today-zoomable-journal audit: cloud ingest used to populate a
--   journal excerpt only in the R2-off dev-mode branch, a landmine that
--   would silently gut the hourly journal the moment R2 was turned on;
--   fixed by persisting `atoms.excerpt` at ingest time via the shared
--   `store::derive_journal_excerpt`, regardless of storage backend), and
--   0116's header for the control-0115/tenant-0026 → control-0116/
--   tenant-0027 renumbering history (collision with #469's own already-
--   merged `0115_atoms_excerpt`-numbered work).
--
--   `atoms` is one of the content tables shared by both trees, so this MUST
--   stay byte-equivalent to the control side or
--   `scripts/check_schema_parity.sh` fails: it applies each tree to a
--   scratch DB and diffs a normalized `pg_dump --schema-only` of the shared
--   tables. Keep the column type, nullability, and `COMMENT` text identical
--   to 0116.
--
--   Idempotent (`IF NOT EXISTS`) like every other tenant-tree column add,
--   since tenant databases are provisioned at different times and the
--   orchestrator may re-run a tree against a DB that is already partially
--   ahead. The backfill `UPDATE` below is DML, not DDL — invisible to
--   `check_schema_parity.sh`'s schema-only diff — and safe to run
--   unconditionally: a freshly-provisioned tenant DB has no atoms yet, so it
--   no-ops; an existing one gets the same bounded headerless-row backfill
--   the control tree runs (see 0116's header for exactly what it does and
--   does not cover).

ALTER TABLE atoms ADD COLUMN IF NOT EXISTS excerpt TEXT;

COMMENT ON COLUMN atoms.excerpt IS
    'Short (<= 280 char) header-stripped excerpt of this coral''s body, '
    'persisted at ingest time via store::derive_journal_excerpt regardless '
    'of whether the full content lives in content_inline or R2. Used by the '
    'hourly work journal (journal_hour_inputs/_scoped) as compile-prompt '
    'material. NULL means not backfilled yet, or genuinely no body left '
    'after stripping the header_len provenance prefix (header-only '
    'capture); readers must COALESCE against a content_inline-derived '
    'fallback for pre-migration rows.';

UPDATE atoms
SET excerpt = CASE
    WHEN LENGTH(TRIM(BOTH FROM content_inline)) > 280
        THEN LEFT(TRIM(BOTH FROM content_inline), 280) || E'…'
    ELSE TRIM(BOTH FROM content_inline)
END
WHERE atom_id IN (
    SELECT atom_id FROM atoms
    WHERE excerpt IS NULL
      AND content_inline IS NOT NULL
      AND TRIM(BOTH FROM content_inline) <> ''
      AND COALESCE(header_len, 0) <= 0
    LIMIT 50000
);
