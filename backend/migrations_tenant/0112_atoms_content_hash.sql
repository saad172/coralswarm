-- Migration: 0112_atoms_content_hash (tenant tree)
-- Description: Tenant-tree half of the paired `atoms.content_hash` change.
--   See `migrations_control/0271_atoms_content_hash.sql` for the full
--   rationale — the Google Calendar event-edit bug (an edited event keeps
--   its id, `Store::insert_atoms_scoped` is `ON CONFLICT ... DO NOTHING`,
--   so the stored coral never changed), why the hash is of the whole
--   pre-chunk request content, why NULL means "assume changed", and why
--   this is NOT an identity/conflict key.
--
--   `atoms` is one of the content tables shared by both trees, so this MUST
--   stay byte-equivalent to the control side or
--   `scripts/check_schema_parity.sh` fails: it applies each tree to a
--   scratch DB and diffs a normalized `pg_dump --schema-only` of the shared
--   tables. Keep the column type, nullability, and `COMMENT` text identical
--   to 0271.
--
--   Idempotent (`IF NOT EXISTS`) like every other tenant-tree column add,
--   since tenant databases are provisioned at different times and the
--   orchestrator may re-run a tree against a DB that is already partially
--   ahead.

ALTER TABLE atoms ADD COLUMN IF NOT EXISTS content_hash TEXT;

COMMENT ON COLUMN atoms.content_hash IS
    'Lowercase hex SHA-256 of the FULL ingest request content this chunk '
    'was cut from (routes::ingest::IngestAtomRequest::content, before '
    'chunking and before the provenance header) — identical across every '
    'chunk of one request. Persisted at ingest time by routes::ingest via '
    'Store::insert_atoms_scoped. Read by services::source_reingest to tell '
    'a re-fetched, unchanged upstream item (skip) from an EDITED one '
    '(retract + re-ingest). NULL = ingested before migration 0271/0112, '
    'content unknown — readers treat NULL as "assume changed". Never a '
    'conflict/identity key.';
