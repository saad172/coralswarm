-- Migration: 0248_atoms_symbols
-- Description: `atoms.symbols` — the UNCAPPED declaration list a code
--   chunk's own header truncates for display
--   (`packages/core/src/chunker.rs::combined_label`), persisted at INGEST
--   TIME so the atom keyword leg's symbol candidate (`store::
--   keyword_search_pg`, fix 3b / migrations `0245`/`0246`) can reach a
--   declaration that never made it into `atoms.excerpt`.
--
-- THE RESIDUAL FIX 3B LEFT OPEN
--   A merged code chunk's header lists only the first three declarations
--   (`items function foo, struct Bar, const C, +N more`) — the rest is
--   display truncation, not a retrieval limit. But `atoms.excerpt`
--   (migration `0116`, the column the symbol candidate leg's predicate
--   reads) is derived from that SAME truncated header text, so a
--   declaration named only inside "+N more" is invisible to the excerpt
--   leg too, no matter how the question is worded. Measured over a
--   16,569-chunk ingest of this repo, 2026-09-22: 2,975 chunks (18.0%)
--   carry a truncated header, hiding ~4.7 items each — roughly 14,000
--   declarations the symbol leg could never reach.
--
-- WHY A NEW COLUMN RATHER THAN WIDENING `excerpt`
--   `atoms.excerpt` is deliberately capped at `JOURNAL_EXCERPT_MAX_CHARS`
--   (280 chars) so it never toasts — 0245's whole argument for why it is
--   affordable where `content_tsv` is not. A chunk's declaration list is
--   not "more excerpt text": it is a short, bounded list of identifiers (a
--   handful of `"{kind} {name}"` strings per chunk — see
--   `coralswarm_core::chunker::Chunk::symbols`'s own doc comment for
--   exactly what populates it), never a second copy of the body. Raising
--   `excerpt`'s cap to fit it would reintroduce the same TOAST cost 0245
--   measured for `content_tsv` on any chunk whose body is long — this
--   column stays small on every chunk, by construction, because it holds
--   declarations, not prose.
--
-- WHY A PLAIN TEXT COLUMN, NOT A GENERATED/STORED ONE
--   Same reasoning as 0244/0245/0246: a stored generated column rewrites
--   `atoms` under an ACCESS EXCLUSIVE lock (379 MB on dogfood). This
--   migration is a plain nullable `ADD COLUMN ... TEXT` with NO DEFAULT —
--   catalog-only on Postgres 11+, no table rewrite. The tsvector
--   expression index that makes the column searchable is a SEPARATE
--   migration (`0248`/`0101`): `CREATE INDEX CONCURRENTLY` must be the
--   ONLY statement in its migration file, or sqlx sends this ADD COLUMN
--   and that CREATE INDEX as one wire-protocol Query message and Postgres
--   implicitly wraps the whole message in a transaction even when sqlx
--   itself is not — and Postgres refuses CONCURRENTLY inside any
--   transaction block. See `0059_atoms_workspace_index_concurrently.sql`'s
--   header for the fuller version of this same argument, first written
--   down for `atoms.workspace_id`/`idx_atoms_graph_workspace`.
--
-- NULLABLE, NO BACKFILL
--   Nullable with no default, same precedent as `excerpt` (0116), `title`
--   (0097), `project`/`mcp_client_name` (0072): NULL means "no declarations
--   persisted here" — either an atom this fix has not re-ingested yet, or
--   genuinely nothing declared (a note, a transcript chunk, an
--   all-decorative code chunk — see `Chunk::symbols`'s doc comment for
--   that boundary). Unlike 0116, there is no bounded backfill UPDATE here:
--   `Store::insert_atoms_scoped` is `ON CONFLICT (graph_id, source_type,
--   source_id) DO NOTHING`, so an existing atom is never rewritten by a
--   later insert — this column only ever populates on a chunk ingested
--   AFTER this PR's `routes::ingest` starts computing it. Backfilling
--   existing atoms needs a real re-ingest (which re-derives `content_tsv`,
--   `excerpt`, embeddings and everything else too), not a migration-time
--   SQL UPDATE with nothing to derive the declaration list FROM — the raw
--   source text a symbol was declared in is not itself stored in `atoms`.
--
-- WHAT THIS COLUMN DOES NOT CLOSE
--   A symbol that is only ever USED — called, referenced, imported — and
--   never DECLARED in any chunk's header stays unreachable through this
--   column. That is a deliberate narrowing (indexing every USE would mean
--   indexing chunk bodies, the unaffordable shape 0245's header measures:
--   `content_tsv @@ phraseto_tsquery('simple','user_id')` seq-scans 3,925
--   toasted rows at 241ms warm / 3.8s cold), not an oversight. See
--   `store::keyword_search_pg`'s doc comment for the symbol leg itself.
--
-- Paired with `migrations_tenant/0100_atoms_symbols.sql`. `atoms` is one of
-- the shared content tables, so a column added to only one tree fails
-- `scripts/check_schema_parity.sh` (and its CI job) by design.
--
-- Down:
--   ALTER TABLE atoms DROP COLUMN IF EXISTS symbols;

ALTER TABLE atoms ADD COLUMN IF NOT EXISTS symbols TEXT;

COMMENT ON COLUMN atoms.symbols IS
    'Newline-joined "{kind} {name}" spelling of every declaration this '
    'chunk contains (coralswarm_core::chunker::Chunk::symbols), UNCAPPED — '
    'unlike the items … header, which truncates to three plus "+N more" '
    'for display. Persisted at ingest time by routes::ingest via '
    'Store::insert_atoms_scoped, from the same in-memory Chunk excerpt/ '
    'title already derive from. NULL means no declarations (a non-code '
    'chunk, or an all-decorative code chunk), not "not yet backfilled" — '
    'existing atoms stay NULL until a real re-ingest, see this migration''s '
    'header. Read by store::keyword_search_pg''s symbol candidate leg via '
    'idx_atoms_symbols_tsv (migration 0248/0101).';
