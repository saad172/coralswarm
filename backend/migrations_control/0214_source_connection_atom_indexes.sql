-- Migration: 0214_source_connection_atom_indexes
-- Description: Index the two FOREIGN KEY *referencing* columns on
--   `source_connection_atoms` (CodeRabbit round 1 on #871).
--
--   Postgres creates an index on the REFERENCED side of a foreign key (that
--   side is a primary/unique key by definition) but never on the referencing
--   side. `0213_source_connections` gave this table two `REFERENCES
--   atoms(atom_id) ON DELETE CASCADE` columns and one composite primary key:
--
--     PRIMARY KEY(connection_id, atom_id)
--
--   `atom_id` is that key's TRAILING column, so the PK's btree cannot be used
--   to answer `WHERE atom_id = $1` on its own, and `target_atom_id` has no
--   index at all. Every `DELETE FROM atoms` — a single retraction, a coral
--   deletion, and the row-at-a-time cascade a graph delete fans out into —
--   therefore makes Postgres take TWO sequential scans of
--   `source_connection_atoms` to prove the cascades are satisfiable. The
--   table grows one row per published atom per connection, so that cost rises
--   with every publication while the deletes stay the same size.
--
--   This migration is separate from 0213 rather than an edit to it because
--   0213 is merged: sqlx fingerprints migration file CONTENT, so editing it
--   (even a comment) breaks `_sqlx_migrations` on every already-deployed
--   database. `scripts/check_migrations.sh` enforces that with a committed
--   checksum manifest.
--
--   Paired with `migrations_tenant/0082_source_connection_atom_indexes.sql`,
--   which must stay schema-identical — `source_connection_atoms` is one of
--   the tables `scripts/check_schema_parity.sh` diffs across both trees.
--
-- Depends on: 0213_source_connections (creates the table).
-- Down:
--   DROP INDEX IF EXISTS source_connection_atoms_atom;
--   DROP INDEX IF EXISTS source_connection_atoms_target_atom;

CREATE INDEX IF NOT EXISTS source_connection_atoms_atom
    ON source_connection_atoms(atom_id);
CREATE INDEX IF NOT EXISTS source_connection_atoms_target_atom
    ON source_connection_atoms(target_atom_id);
