-- Migration: 0010_atoms_title (tenant tree)
-- Description: Tenant-tree half of the paired `atoms.title` change. See
--   `migrations_control/0097_atoms_title.sql` for the full rationale — why a
--   per-coral headline exists at all, and why it is per-CORAL rather than
--   reusing `coding_sessions.title` (session-level recall silently omits
--   notes, meeting transcripts, email, Slack and Teams, which have no
--   session).
--
--   `atoms` is one of the 39 content tables shared by both trees, so this
--   MUST stay byte-equivalent to the control side or
--   `scripts/check_schema_parity.sh` fails: it applies each tree to a scratch
--   DB and diffs a normalized `pg_dump --schema-only` of the shared tables.
--   Keep the column type, nullability, and `COMMENT` text identical to 0097.
--
--   Idempotent (`IF NOT EXISTS`) like every other tenant-tree column add
--   (0004 `jobs.account_id`, 0005 `graphs.owner_user_id`/`archived_at`), since
--   tenant databases are provisioned at different times and the orchestrator
--   may re-run a tree against a DB that is already partially ahead.

ALTER TABLE atoms ADD COLUMN IF NOT EXISTS title TEXT;

COMMENT ON COLUMN atoms.title IS
    'Short human-readable headline (~10 words) for this coral, used by '
    'date-range recall so an answer can cover every coral in a range without '
    'reading every body. Generated from the coral BODY with the '
    '`header_len` provenance prefix stripped — never from the raw '
    '`content_inline`, which would yield the enclosing session title. NULL '
    'means not titled yet (backfill is asynchronous) or untitleable; all '
    'readers must tolerate NULL.';
