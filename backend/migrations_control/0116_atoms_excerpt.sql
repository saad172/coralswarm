-- Migration: 0116_atoms_excerpt
-- Description: `atoms.excerpt` — a short (<= JOURNAL_EXCERPT_MAX_CHARS = 280
--   character) header-stripped snippet of a coral's body, persisted at INGEST
--   TIME regardless of storage backend.
--
--   Why this exists (today-zoomable-journal audit, gap 9). The hourly work
--   journal's excerpts (`Store::journal_hour_inputs`/`_scoped`) used to read
--   ONLY `atoms.content_inline`. Cloud ingest (`routes::ingest`) populates
--   `content_inline` ONLY in its no-R2 dev-mode branch — when R2 is
--   configured, the request's text goes to an encrypted blob instead and
--   `content_inline` stays NULL. Prod runs with R2 off today, so this
--   happened to work, but flipping R2 on would silently gut the journal:
--   every NEW atom's excerpt would become `None`, and every hour would
--   compile down to source-type counts alone (the LLM prompt explicitly
--   degrades to "N items were captured with no further detail available"
--   when it has no excerpts — `prompts/journal_compile_v1.txt`). That is a
--   landmine, not a live bug: R2 is off in prod today, so it has not fired.
--
--   The fix moves excerpt derivation from READ time to WRITE time: every
--   insert path that has content in hand (currently `Store::
--   insert_atoms_scoped`, the sole write path `routes::ingest`'s
--   `ingest_atom_impl` funnel uses — MCP `add_context`, the direct HTTP
--   ingest route, and every connector that calls the real `ingest_atom`
--   handler all go through it) now derives the excerpt from the full
--   in-memory chunk text BEFORE deciding whether that text is stored inline
--   or in R2, via the single shared function `store::derive_journal_excerpt`
--   (same strip-then-truncate logic `journal_hour_inputs` used to run at
--   read time, factored out so write and read can never disagree). Reads
--   become `COALESCE`-shaped: prefer the persisted `excerpt`, fall back to
--   deriving it from `content_inline` for rows written before this column
--   existed.
--
--   Nullable with no default, same precedent as 0097 (`title`), 0072
--   (`project`/`mcp_client_name`), 0066 (`header_len`): a plain
--   `ADD COLUMN ... TEXT` with no default is a metadata-only change on
--   Postgres 11+, does not rewrite the table. NULL means "no excerpt" —
--   either not backfilled yet, or genuinely nothing left after stripping the
--   provenance header (a header-only capture); every reader must tolerate
--   NULL.
--
--   Bounded backfill below covers existing HEADERLESS rows (`header_len`
--   NULL/<=0) directly in SQL — the common case, and safe to express without
--   `header_len`'s byte-boundary care. Rows that DO carry a header
--   (`header_len > 0`, e.g. coding-session/meeting transcripts) are left
--   NULL here on purpose: an exact byte-boundary-safe strip needs Rust
--   (`services::atom_title::strip_provenance_header`'s UTF-8-boundary
--   walking), not a migration, and the read-side `COALESCE` fallback already
--   derives their excerpt correctly and identically to before this
--   migration — this backfill is a pure write-time optimization, never a
--   correctness dependency.
--
--   Paired with `migrations_tenant/0027_atoms_excerpt.sql`. `atoms` is one of
--   the shared content tables, so a column added to only one tree fails
--   `scripts/check_schema_parity.sh` (and its CI job) by design.
--
--   Renumbering history: this started life as control-0115 / tenant-0026 in
--   this PR's own branch, back when open PR #468 (`journal: fix dead
--   supersession events + add promote event type`) had already claimed
--   control-0114 / tenant-0025 and this was the next free pair. By the time
--   this PR was rebased onto main, #468, #469, and #470 had all merged —
--   and #469 (`journal_dirty_hours`/`JournalHourInputs` rework, incl. moving
--   the "asks" source to `query_history`) claimed control-0115
--   (`0115_query_history_created_at_index.sql`) / tenant-0026 for its OWN,
--   unrelated migration. This migration was renumbered to control-0116 /
--   tenant-0027 — the next free pair after #469 — to avoid colliding with an
--   already-merged migration on main.
--
--   No index. Excerpts are read alongside the atoms a date-range scan
--   already selected by `timestamp`, never searched on their own —
--   `content_tsv` remains the lexical search surface.
--
-- Depends on: 0003_atoms (atoms), 0066_atom_header_len (header_len, this
--   migration's backfill predicate).
--
-- Down:
--   ALTER TABLE atoms DROP COLUMN IF EXISTS excerpt;

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

-- Bounded backfill: populate `excerpt` for existing HEADERLESS rows
-- (`header_len` NULL/<=0) that have `content_inline` — see this migration's
-- header for why header-bearing rows are deliberately left to the Rust
-- read-side fallback instead. Mirrors `store::truncate_excerpt` exactly:
-- leading/trailing `TRIM` only (no internal whitespace collapsing), a
-- CHARACTER-counted (not byte-counted) 280-char cap via `LEFT`, with a
-- trailing ellipsis appended only when something was actually cut. `LIMIT
-- 50000` bounds a single run (prod holds ~6.3k atoms total as of this
-- migration, so one pass fully covers it with room to spare); re-running
-- this migration's UPDATE is safe and idempotent either way — `excerpt IS
-- NULL` narrows to whatever's left.
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
