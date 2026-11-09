-- Migration: 0097_atoms_title
-- Description: A short human-readable headline per coral, so date-range recall
--   ("what did we work on today and yesterday?") can answer from TITLES rather
--   than from raw bodies.
--
--   Why this exists. Event-time recall force-merges every atom in the window
--   into the synthesis set, which drives a per-atom content fetch AND the
--   synthesis prompt. That does not scale: on a real ocean a single day held
--   139 atoms, so any workable bound had to drop most of the range — and the
--   most-recent-first bound it used dropped whole DAYS (a dense newest day
--   evicted every older one). Titles change the unit: ~10 words per coral
--   instead of a full body, so a two-day range is a few thousand tokens and
--   the answer can cover EVERY coral in the range instead of a sampled subset.
--   This is the same shape chat products use for history recall — they answer
--   from per-conversation titles/summaries, never from raw transcripts.
--
--   Why per-CORAL and not per-session. `coding_sessions` already carries a
--   `title`, and reusing it was the obvious shortcut — but it only covers
--   corals captured under a coding session. Notes, meeting transcripts,
--   emails, Slack and Teams messages have no session at all, so session-level
--   recall would silently omit them from a date range. A column on `atoms`
--   makes every source type behave identically, which is the whole point.
--
--   Note for whoever generates these: the coding-session capture path prefixes
--   an atom's `content_inline` with a provenance header ("Session: <session
--   title>. Repo: … Branch: …", length recorded in `atoms.header_len`). A
--   title derived from the raw `content_inline` would therefore be the SESSION
--   title on every coral in that session — reproducing exactly the
--   session-granularity problem this column exists to avoid. Strip
--   `header_len` bytes first. Only 1235 of 3818 atoms on the reference ocean
--   carry a header, so the stripping must be conditional, not assumed.
--
--   Nullable with no default, following the precedent of 0072 (`project`,
--   `mcp_client_name`), 0058 (`workspace_id`) and 0034 (`channel_id` et al):
--   a plain `ADD COLUMN ... TEXT` with no default is a metadata-only change on
--   Postgres 11+, so it does not rewrite the table. NULL means "not titled
--   yet" — every reader must tolerate it, because backfill is asynchronous and
--   because a local-only account with no LLM and an untitleable body (empty or
--   whitespace-only) legitimately has no title.
--
--   Paired with `migrations_tenant/0010_atoms_title.sql`. `atoms` is one of the
--   39 shared content tables, so a column added to only one tree fails
--   `scripts/check_schema_parity.sh` (and its CI job) by design.
--
--   No index. Titles are read alongside the atoms a date-range scan already
--   selected by `timestamp`, never searched on their own — `content_tsv`
--   remains the lexical surface. Add one only if a title-specific query
--   appears.

ALTER TABLE atoms ADD COLUMN IF NOT EXISTS title TEXT;

COMMENT ON COLUMN atoms.title IS
    'Short human-readable headline (~10 words) for this coral, used by '
    'date-range recall so an answer can cover every coral in a range without '
    'reading every body. Generated from the coral BODY with the '
    '`header_len` provenance prefix stripped — never from the raw '
    '`content_inline`, which would yield the enclosing session title. NULL '
    'means not titled yet (backfill is asynchronous) or untitleable; all '
    'readers must tolerate NULL.';
