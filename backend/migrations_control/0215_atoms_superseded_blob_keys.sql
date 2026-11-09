-- Migration: 0215_atoms_superseded_blob_keys
-- Description: A durable, per-atom record of R2 object keys a publication
--   REFRESH replaced, so they can be reclaimed after the replacing
--   transaction commits (CodeRabbit round 1 on #871).
--
--   The leak this closes. `Store::refresh_published_atoms_scoped` (the write
--   half of a live source connection) re-uploads the source coral's content
--   under a NEW random key every revision — `blob_key_for(org, target_graph,
--   Uuid::new_v4())` — and then overwrites `atoms.content_blob_key` with it.
--   Before this column the PREVIOUS key was simply overwritten: nothing in
--   Postgres still referenced it, so nothing could ever find it again. Every
--   revision of every connected source left exactly one unreachable object
--   behind, forever, and the only bound on the total was how often the
--   upstream source changed.
--
--   Why a column on `atoms` and not a separate GC queue table. The record
--   must not outlive the row it belongs to (deleting a coral must retire its
--   pending reclaims with it — `services::deletion` already deletes the atom's
--   CURRENT blob), it must be visible under exactly the same tenant/account
--   RLS as the key it supersedes (a separate table would need its own policy
--   roster entry and its own scoping rules for a payload that is one opaque
--   object key), and it must be written inside the SAME transaction as the
--   key replacement so a rolled-back refresh records nothing. A `TEXT[]` that
--   the refresh APPENDS to satisfies all three, and an array rather than a
--   single column because two refreshes can land before a reclaim does.
--
--   Ordering contract, deliberately: the reclaim NEVER runs before the
--   database transaction commits. `services::source_publication::copy_batch`
--   drains this array in a separate, post-commit, account-scoped pass, and
--   clears only the keys R2 actually accepted a delete for. A process that
--   dies between the commit and that pass leaves the keys RECORDED (not lost)
--   — the next refresh of the same atom drains them too.
--
--   Empty-array default, never NULL, so the drain predicate is a plain
--   `superseded_blob_keys <> '{}'` with no three-valued logic. The partial
--   index below is what makes that predicate a cheap lookup on a table where
--   the overwhelming majority of rows will always have nothing pending.
--
--   Paired with `migrations_tenant/0083_atoms_superseded_blob_keys.sql`:
--   `atoms` is a shared table, so both trees must stay schema-identical or
--   `scripts/check_schema_parity.sh` fails.
--
-- Depends on: 0213_source_connections (adds the refresh path this records for).
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
