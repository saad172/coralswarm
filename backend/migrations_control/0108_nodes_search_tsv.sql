-- Migration: 0108_nodes_search_tsv
-- Description: §5.6 reef matching — a weighted, GIN-indexed `tsvector` over
--   `nodes` so a reef can be found by name/alias/summary via an index lookup
--   instead of `Store::keyword_search_node_atoms`'s full `nodes JOIN
--   node_atoms` scan (D8). Landed ahead of §3.3's widened extraction (which
--   takes reef counts from ~5,000 toward ~25,000) per §8's sequencing note.
--
--   ── Weight classes ──
--   `A` entity title, `B` aliases, `C` non-entity title, `D` digest summary.
--   Postgres's `ts_rank_cd` default weight array is `{0.1, 0.2, 0.4, 1.0}` for
--   `{D, C, B, A}` — an A-class hit is ~5x a C-class one, so an entity reef
--   matching one query token outranks an assertion reef matching three,
--   without excluding the assertion reef outright. Verified directly against
--   a populated scratch DB (2026-07-28, `cs_reefmatch_control`, Postgres 16):
--     ts_rank_cd('pricing' vs entity title "Pricing")                = 1.0
--     ts_rank_cd('pricing' vs decision title "...pricing model...")  = 0.2
--   — exactly the 5x the design calls for.
--
--   ── 'simple' vs 'english' ──
--   `'simple'` on titles and aliases deliberately avoids stemming proper
--   nouns — "Marchetti" must not become "pecorar". The digest summary is prose
--   and takes the `'english'` config. `nodes` has no per-row language column,
--   so the summary config is a constant, not data-driven.
--
--   ── Aliases ride as raw JSON text ──
--   Generated columns forbid subqueries, so `body->>'aliases'` is indexed as
--   its raw JSON array text, e.g. `["chris p", "chris"]`. Confirmed against a
--   populated table (not just assumed) that `'simple'` tokenisation strips the
--   brackets/quotes/commas out and indexes the words: that example produced
--   `'chris':3B,5B 'p':4B` — i.e. both "chris" occurrences AND "p" are real,
--   separately-matchable lexemes, not garbage. Also confirmed the retrieval
--   gap this closes end-to-end: a reef titled "Jordan Rivera" (title contains
--   no "nomad" anywhere) with `body.aliases = ["nomad"]` produces a
--   `search_tsv` that DOES match `to_tsquery('simple', 'nomad')` — alias-only
--   retrieval, previously impossible (§5.6 Gap 1).
--
--   ── Generated, not triggered ──
--   `GENERATED ALWAYS ... STORED` means every one of the five write paths that
--   touch title/aliases/digest (node creation, rename, merge alias-learning,
--   digest write, promotion) stays correct with zero new call sites — there is
--   no trigger or worker to forget. Confirmed the STORED column backfills
--   automatically on `ALTER TABLE ADD COLUMN` against an ALREADY-POPULATED
--   table (505 rows, mixed entity/decision types, real alias JSON) rather than
--   assuming it from the fresh-table case — every existing row's `search_tsv`
--   was correctly populated by the ALTER itself, no follow-up backfill step
--   required.
--
--   ── Not CONCURRENTLY ──
--   `ADD COLUMN ... STORED` already requires a full table rewrite under an
--   ACCESS EXCLUSIVE lock (Postgres cannot avoid this for a stored generated
--   column — CONCURRENTLY only applies to CREATE INDEX). Since that rewrite
--   already blocks the table for its whole duration, building the index
--   CONCURRENTLY immediately after would not shrink the total blocking
--   window, and this tree's `-- no-transaction` + `CONCURRENTLY` idiom (see
--   0098/0099) requires the index build to be the ONLY statement in its own
--   migration file — a second file was not available (migration numbers are
--   assigned; this tree gets exactly this one slot). Plain `CREATE INDEX` in
--   the same transaction as the column add was the correct call given that
--   constraint, not an oversight.
--
-- Down:
--   DROP INDEX IF EXISTS idx_nodes_search_tsv;
--   ALTER TABLE nodes DROP COLUMN IF EXISTS search_tsv;

ALTER TABLE nodes ADD COLUMN search_tsv tsvector
  GENERATED ALWAYS AS (
      setweight(to_tsvector('simple',
        CASE WHEN type = 'entity' THEN title ELSE '' END), 'A')
   || setweight(to_tsvector('simple',
        coalesce(body->>'aliases', '')), 'B')
   || setweight(to_tsvector('simple',
        CASE WHEN type <> 'entity' THEN title ELSE '' END), 'C')
   || setweight(to_tsvector('english',
        coalesce(body->'digest'->>'summary', '')), 'D')
  ) STORED;

CREATE INDEX idx_nodes_search_tsv ON nodes USING GIN (search_tsv);
