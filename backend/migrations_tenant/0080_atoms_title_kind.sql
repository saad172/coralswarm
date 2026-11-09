-- Migration: 0080_atoms_title_kind (tenant tree)
-- Description: Tenant-tree half of the paired `atoms.title_kind` change. See
--   `migrations_control/0212_atoms_title_kind.sql` for the full rationale —
--   why this column exists (it's what lets the sweep upgrade an ingest-time
--   fallback title to a real LLM headline without re-scanning already-titled
--   rows forever) and why a legacy `title SET, title_kind NULL` row needs the
--   one-shot backfill below rather than being treated as equivalent to
--   `'fallback'` by the predicate itself (independent review finding M1: the
--   sweep's `(title IS NULL OR title_kind = 'fallback')` predicate is FALSE
--   for that shape, proven by truth table on PG16 — this file used to claim
--   otherwise).
--
--   `atoms` is one of the content tables shared by both trees, so this MUST
--   stay byte-equivalent to the control side or `scripts/check_schema_parity.sh`
--   fails: it applies each tree to a scratch DB and diffs a normalized
--   `pg_dump --schema-only` of the shared tables. Keep the column type,
--   nullability, `COMMENT`, index, and one-shot backfill identical to 0212.
--
--   Idempotent (`IF NOT EXISTS`) like every other tenant-tree column add
--   (0004 `jobs.account_id`, 0010 `atoms.title`), since tenant databases are
--   provisioned at different times and the orchestrator may re-run a tree
--   against a DB that is already partially ahead. The backfill `UPDATE` below
--   is separately idempotent (a re-run or a DB with no NULL-kind titled rows
--   matches nothing).

ALTER TABLE atoms ADD COLUMN IF NOT EXISTS title_kind TEXT;

COMMENT ON COLUMN atoms.title_kind IS
    'Provenance of atoms.title: ''llm'' (the model produced it) or '
    '''fallback'' (routes::ingest''s synchronous write, or '
    'services::atom_title::fallback_title via the sweep — no LLM configured, '
    'egress-blocked, or the model''s output was rejected). NULL is a legacy '
    'row titled before this column existed; the one-shot backfill below '
    'normalizes it to ''fallback'' at migration time so it is not silently '
    'excluded from the sweep''s candidate predicate.';

CREATE INDEX IF NOT EXISTS idx_atoms_title_kind_fallback
    ON atoms (title_kind)
    WHERE title_kind = 'fallback';

-- One-shot backfill (independent review finding M1): every row titled before
-- this column existed has `title IS NOT NULL AND title_kind IS NULL`. The
-- sweep's candidate predicate is `(title IS NULL OR title_kind = 'fallback')`
-- — that is FALSE for exactly this shape, so without this statement every
-- legacy fallback-titled row is silently excluded from ever being selected
-- and stays frozen on its pre-column title forever, never getting the LLM
-- upgrade this column exists to offer. Normalizing every such row to
-- `'fallback'` here (once, at migration apply time) makes it a real
-- candidate going forward, through the SAME predicate every other
-- fallback-titled row already relies on — no predicate widening needed.
-- Idempotent: a second application (or a DB with no such rows) matches
-- nothing and updates zero rows.
UPDATE atoms
SET    title_kind = 'fallback'
WHERE  title IS NOT NULL
  AND  title_kind IS NULL;
