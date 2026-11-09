-- Migration: 0083_atoms_superseded_blob_keys (tenant tree)
-- Description: Tenant-tree half of the paired `atoms.superseded_blob_keys`
--   change. See `migrations_control/0215_atoms_superseded_blob_keys.sql` for
--   the full rationale — why a publication REFRESH used to leave one
--   unreachable R2 object behind per revision, why the record lives on the
--   atom row rather than in a separate GC queue, and the "never reclaim
--   before the replacing transaction commits" ordering contract.
--
--   `atoms` is one of the content tables shared by both trees, so this MUST
--   stay byte-equivalent to the control side or
--   `scripts/check_schema_parity.sh` fails: it applies each tree to a scratch
--   database and diffs a normalized `pg_dump --schema-only` of the shared
--   tables. Keep the column type, nullability, default, `COMMENT`, and the
--   partial index (name, column, predicate) identical to 0215.
--
--   Idempotent (`IF NOT EXISTS`) like every other tenant-tree column add:
--   tenant databases are provisioned at different times and the orchestrator
--   may re-run a tree against a database that is already partially ahead.
--
-- Depends on: 0081_source_connections (adds the refresh path this records for).
-- Down:
--   DROP INDEX IF EXISTS atoms_superseded_blob_keys_pending;
--   ALTER TABLE atoms DROP COLUMN IF EXISTS superseded_blob_keys;

ALTER TABLE atoms ADD COLUMN IF NOT EXISTS superseded_blob_keys TEXT[] NOT NULL DEFAULT '{}';

COMMENT ON COLUMN atoms.superseded_blob_keys IS
    'R2 object keys this atom''s content_blob_key was replaced BY a publication '
    'refresh, awaiting post-commit reclamation. Appended inside the same '
    'transaction that replaces the key; drained (and only then cleared) after '
    'that transaction commits. Never delete a key listed here before the '
    'replacing transaction has committed.';

CREATE INDEX IF NOT EXISTS atoms_superseded_blob_keys_pending
    ON atoms(graph_id)
    WHERE superseded_blob_keys <> '{}';
