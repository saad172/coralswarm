-- Migration: 0082_source_connection_atom_indexes (tenant tree)
-- Description: Tenant-tree half of the paired `source_connection_atoms`
--   foreign-key index change. See
--   `migrations_control/0214_source_connection_atom_indexes.sql` for the full
--   rationale (Postgres indexes only the REFERENCED side of a foreign key, so
--   both `ON DELETE CASCADE` referencing columns here are answered by a
--   sequential scan on every `DELETE FROM atoms`).
--
--   `source_connection_atoms` is shared by both trees, so this MUST stay
--   schema-identical to the control side or `scripts/check_schema_parity.sh`
--   fails: it applies each tree to a scratch database and diffs a normalized
--   `pg_dump --schema-only` of the shared tables. Keep the index NAMES and
--   their column lists identical to 0214.
--
--   `IF NOT EXISTS` like every other tenant-tree index add: tenant databases
--   are provisioned at different times and the orchestrator may re-run a tree
--   against a database that is already partially ahead.
--
-- Depends on: 0081_source_connections (creates the table).
-- Down:
--   DROP INDEX IF EXISTS source_connection_atoms_atom;
--   DROP INDEX IF EXISTS source_connection_atoms_target_atom;

CREATE INDEX IF NOT EXISTS source_connection_atoms_atom
    ON source_connection_atoms(atom_id);
CREATE INDEX IF NOT EXISTS source_connection_atoms_target_atom
    ON source_connection_atoms(target_atom_id);
