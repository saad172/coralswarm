pub mod rerank;
pub mod traversal;

use chrono::{DateTime, Utc};

// ── Reef (node) lexical matching (§5.6) ────────────────────────────────────────
//
// Retrieval is atom-first: vector + keyword search run over corals (atoms), and
// reefs (nodes) are only ever reached by expanding *up* from atom hits. That
// means a curated reef — e.g. a person entity a user hand-renamed, or one only
// ever named by a taught alias (§5.6 Gap 1) — is invisible to search unless its
// atoms happen to contain the queried term verbatim. The helpers below let a
// store score a reef's `title` + `body.aliases` + digest `summary` against the
// query so the reef's atoms can be injected as additional keyword candidates.
//
// Scoring is deliberately lexical (token-overlap, not embeddings). On Postgres
// this is backed by `nodes.search_tsv`, a GENERATED tsvector column + GIN index
// (migration `0108`/`0018`) — no trigger/worker to keep it in sync, so a rename
// or a newly-taught alias takes effect on the very next query.
// [`node_match_score`] below is a Rust model of the same coverage +
// weight-class semantics (see its doc comment, and `store::select_reef_ids` /
// `store::flatten_reef_atoms` for the reservation/flatten logic).
//
// DELIBERATE DEVIATION FROM THE DESIGN-LOCKED SPEC: qualification is
// `REEF_COVERAGE_FLOOR OR full-title-match` for `type='entity'` reefs, not
// coverage alone as originally specified — a coverage floor normalised
// against QUERY length means a two-word person's name can only ever qualify
// on questions of 4 tokens or fewer, which measurably undercut §5.2's stated
// reason for sibling-pull existing at all. See [`node_match_score`]'s doc
// comment for the full reasoning, the measured 8/23 -> 16/23 recovery, why
// it is scoped to >= 2 title tokens (so it does NOT reopen the coverage-only
// `"Pricing"` case the spec's own worked example depends on staying
// excluded), and the two residual gaps left open on purpose. The Postgres
// query (`store::mod`) implements the identical `OR` condition in SQL.

/// A handful of high-frequency English tokens that carry no retrieval signal.
/// Dropping them keeps a query like "give me a breakdown of dorian" from
/// matching every reef whose summary happens to contain "team" or "of".
const NODE_MATCH_STOPWORDS: &[&str] = &[
    "a", "about", "an", "and", "any", "are", "as", "at", "be", "but", "by", "can", "could", "did",
    "do", "does", "explain", "for", "from", "give", "had", "has", "have", "he", "her", "him",
    "his", "how", "i", "in", "is", "it", "its", "me", "my", "of", "on", "or", "please", "she",
    "should", "show", "tell", "than", "that", "the", "their", "them", "they", "this", "to", "use",
    "used", "using", "vs", "was", "we", "were", "what", "when", "where", "which", "who", "whom",
    "why", "with", "would", "you", "your",
];

/// Minimum fraction of a query's distinct, non-stopword tokens ([`node_query_tokens`])
/// a reef must match — anywhere across title, aliases, or digest summary — to
/// qualify for ranking at all (§5.6). A raw score/rank floor is not comparable
/// across queries of different lengths (the same constant is strict for
/// "chris marchetti" and permissive for a ten-word question); coverage is
/// query-length-normalised by construction. Applied as a qualification GATE,
/// not part of the score: once a reef qualifies, [`node_match_score`] /
/// `ts_rank_cd` (the Postgres path, `idx_nodes_search_tsv`) rank it.
pub const REEF_COVERAGE_FLOOR: f64 = 0.5;

/// Weight applied when a query token matches a token in an **entity** reef's
/// title. Mirrors `idx_nodes_search_tsv`'s tsvector weight class `'A'`
/// (migration `0108`/`0018`) — Postgres's `ts_rank_cd` default weight array is
/// `{0.1, 0.2, 0.4, 1.0}` for `{D, C, B, A}`, so these four constants use the
/// same ratios the SQL path does, keeping [`node_match_score`]'s ranking in
/// step with Postgres's — same relative ordering, computed without a
/// `tsvector`.
const WEIGHT_ENTITY_TITLE: f64 = 1.0;
/// Weight class `'B'` — a query token matched in `body.aliases`.
const WEIGHT_ALIAS: f64 = 0.4;
/// Weight class `'C'` — a query token matched in a **non-entity** reef's title
/// (e.g. an assertion/decision reef's sentence-length title).
const WEIGHT_OTHER_TITLE: f64 = 0.2;
/// Weight class `'D'` — a query token matched in the reef's digest summary.
const WEIGHT_SUMMARY: f64 = 0.1;

/// Minimum distinct title-token count for the full-title-match escape hatch
/// (see [`node_match_score`]'s doc comment, "§5.6 deviation") to apply at
/// all. Guards against reopening the spec's own `"Pricing"`-shaped failure —
/// a ONE-token entity title matching its own single word is exactly the
/// `1/3`-coverage case §5.6's worked example says must stay excluded, so a
/// full-title match is only a free pass once it required at least two
/// distinct words of corroboration.
const FULL_TITLE_MATCH_MIN_TOKENS: usize = 2;

/// Tokenise text for reef lexical matching: lowercase, split on non-alphanumeric
/// boundaries, drop tokens shorter than two characters. Used for both the query
/// and the reef text so matching is on whole tokens (no substring false hits
/// like "man" inside "management").
pub fn node_match_tokens(text: &str) -> Vec<String> {
    text.split(|c: char| !c.is_alphanumeric())
        .filter(|t| t.len() >= 2)
        .map(|t| t.to_lowercase())
        .collect()
}

/// The distinct, non-stopword query tokens used to score reefs. Computed once
/// per query and reused across every candidate reef.
pub fn node_query_tokens(query: &str) -> Vec<String> {
    let mut seen = std::collections::HashSet::new();
    node_match_tokens(query)
        .into_iter()
        .filter(|t| !NODE_MATCH_STOPWORDS.contains(&t.as_str()))
        .filter(|t| seen.insert(t.clone()))
        .collect()
}

// ── Coral (atom) keyword matching — the `Store::keyword_search` leg ───────────
//
// The atom keyword leg has a HARDER stopword problem than the reef leg above.
// A reef's text is a short title/alias/summary, so a stray "before" costs
// little. An atom's `content_tsv` is a whole chunk, and the leg's tsquery is
// built from the user's VERBATIM question (`condense_query` only runs on
// follow-up turns), so every filler word in a fifteen-word question is a real
// term the query either demands or ORs in. Measured on the live dev ocean
// (~27k atoms): `code` appears in 31% of atoms, `before` in 29%, `file` in
// 24%. ORing those in reaches 17,281 of 27,161 rows and degenerates into a
// sequential scan of every toasted `content_tsv` — 6.5s WARM against an
// ask-path retrieve budget of ~130ms.
//
// So the keyword leg drops MORE words than the reef leg does. It reuses
// [`NODE_MATCH_STOPWORDS`] (one tokeniser, one shared base list) and adds
// [`KEYWORD_EXTRA_STOPWORDS`] on top; the extra list is deliberately NOT
// merged into `NODE_MATCH_STOPWORDS`, because that constant is the DENOMINATOR
// of the reef leg's [`REEF_COVERAGE_FLOOR`] — shrinking it would quietly
// loosen every reef qualification decision and move the §5.6 tests that pin
// them.

/// Query words the ATOM keyword leg drops on top of [`NODE_MATCH_STOPWORDS`]
/// (see the section comment above for why the two lists are separate).
///
/// Every entry is a closed-class English word — a preposition, conjunction,
/// determiner, auxiliary, quantifier or discourse filler — that a user's
/// phrasing supplies for free and that therefore says nothing about WHICH
/// atom they want. Nothing domain-specific belongs here: `code`, `file` and
/// `module` are high-frequency in this corpus and still carry real meaning,
/// so they stay, and the coverage sort is what keeps them from dominating.
const KEYWORD_EXTRA_STOPWORDS: &[&str] = &[
    "after", "again", "against", "all", "along", "already", "also", "although", "always", "am",
    "among", "another", "anything", "around", "been", "before", "being", "below", "beside",
    "besides", "between", "both", "cannot", "come", "comes", "coming", "during", "each", "either",
    "else", "even", "ever", "every", "few", "first", "get", "gets", "getting", "go", "goes",
    "going", "got", "if", "into", "itself", "just", "keep", "kept", "know", "known", "let", "like",
    "likely", "made", "make", "makes", "making", "many", "may", "maybe", "might", "more", "most",
    "much", "must", "near", "need", "needs", "never", "new", "next", "no", "none", "nor", "not",
    "now", "off", "often", "once", "one", "only", "onto", "other", "others", "ours", "out", "over",
    "own", "per", "put", "rather", "really", "right", "said", "same", "say", "says", "see", "seen",
    "since", "so", "some", "something", "soon", "still", "such", "sure", "take", "taken", "then",
    "there", "therefore", "these", "thing", "things", "those", "though", "through", "thus",
    "together", "too", "toward", "under", "until", "up", "upon", "us", "very", "want", "wants",
    "way", "well", "went", "whatever", "whenever", "whether", "while", "whose", "will", "within",
    "without", "yes", "yet",
];

/// Hard ceiling on the distinct tokens [`keyword_query_tokens`] returns.
///
/// Both halves of `Store::keyword_search`'s cost scale with the token count —
/// the OR tsquery widens the candidate set and the per-candidate coverage
/// count is `O(tokens)` — so a pasted stack trace or a whole email quoted into
/// the ask box must not be able to turn one retrieval into an unbounded scan.
/// 24 is comfortably above any real question (the twenty in
/// `backend/eval/github/coralswarm.questions.json` yield 5-11 tokens each)
/// and is applied by truncation in QUESTION ORDER, which keeps the earliest —
/// and in English, usually the most topical — words.
pub const KEYWORD_MAX_QUERY_TOKENS: usize = 24;

/// How many title-leg candidates `Store::keyword_search` materialises per
/// requested result, before the coverage sort picks the final `k`.
///
/// The title predicate is cheap and selective (an expression GIN index over
/// `to_tsvector('simple', coalesce(title,''))`, migration
/// `0244`/`0097` — `0242` in the original text, stale since that PR renumbered
/// around `0242_org_domains`), but every candidate it admits then costs one `content_tsv`
/// detoast in the scoring stage, so the cap is what converts "how much does a
/// keyword search cost" from a property of the OCEAN into a property of `k`.
///
/// 12 is measured, not guessed. Sweeping the cap over the twenty questions in
/// `backend/eval/github/coralswarm.questions.json` against the live dev ocean
/// (26,980 atoms), scoring "did the expected source document reach the top
/// 50": 4x = 15/20 at a 36ms median, 8x = 16/20 at 40ms, **12x = 17/20 at
/// 48ms**, 16x = 17/20 at 59ms. The pre-fix leg scored 1/20.
pub const KEYWORD_TITLE_CANDIDATE_MULTIPLE: usize = 12;

/// The distinct, non-stopword query tokens the ATOM keyword leg searches and
/// counts coverage over — [`node_query_tokens`]'s tokeniser and stopword list,
/// plus [`KEYWORD_EXTRA_STOPWORDS`], truncated to [`KEYWORD_MAX_QUERY_TOKENS`].
///
/// Every token is lowercase and `[0-9a-z]`-only by construction
/// ([`node_match_tokens`] splits on every non-alphanumeric character), which is
/// also what makes it safe to splice them into a `to_tsquery` string: there is
/// no character left in a token that `tsquery` could read as an operator, and
/// nothing here ever reaches Postgres as SQL text — the token array itself is a
/// bound `text[]` parameter.
///
/// An empty result means "this query has no content words at all" (e.g. "what
/// is it"), and `Store::keyword_search` returns no candidates rather than
/// matching the whole ocean.
pub fn keyword_query_tokens(query: &str) -> Vec<String> {
    let mut seen = std::collections::HashSet::new();
    node_match_tokens(query)
        .into_iter()
        .filter(|t| {
            !NODE_MATCH_STOPWORDS.contains(&t.as_str())
                && !KEYWORD_EXTRA_STOPWORDS.contains(&t.as_str())
        })
        .filter(|t| seen.insert(t.clone()))
        .take(KEYWORD_MAX_QUERY_TOKENS)
        .collect()
}

// ── Symbols: identifiers and paths (fix 3b) ──────────────────────────────────
//
// [`keyword_query_tokens`] above splits on every non-alphanumeric character, so
// a spelling a developer types as ONE word arrives at the leg as several
// ordinary ones. Measured against `to_tsvector('simple', …)` on PostgreSQL 17
// before any of this was written:
//
// | typed                                        | indexed lexemes                         |
// |----------------------------------------------|-----------------------------------------|
// | `intake_window`                              | `intake`, `window`          — SPLIT     |
// | `store::PostgresStore::keyword_search`       | `store`, `postgresstore`, `keyword`, `search` |
// | `queries.rs`                                 | `queries.rs`                — ONE lexeme |
// | `PostgresStore`                              | `postgresstore`             — ONE lexeme |
// | `intake-window`                              | `intake-window`, `intake`, `window`     |
// | `// backend/src/routes/queries.rs — function intake_window` | `backend/src/routes/queries.rs`, `function`, `intake`, `window` |
//
// Three consequences, and each one shapes a rule below.
//
// 1. `_` and `::` SPLIT, into ADJACENT positions. So the repair is a PHRASE
//    (`'intake' <-> 'window'`), which matched `function intake_window` and did
//    NOT match "the intake was wide open and the window closed" — precise, and
//    served by the `content_tsv`/`excerpt` GIN indexes without positions
//    because Postgres rechecks the phrase on the heap tuple.
//
// 2. `.` does NOT split — `queries.rs` and `self.window` are each ONE lexeme.
//    So a dotted spelling is not a phrase problem at all; it is a problem that
//    [`keyword_query_tokens`] THREW THE LEXEME AWAY (it yields `queries` + `rs`,
//    neither of which matches `queries.rs`). The repair is to search the
//    undivided string.
//
// 3. CamelCase does NOT split, on EITHER side — `PostgresStore` is one lexeme
//    and [`node_match_tokens`] lowercases without splitting on case, so the two
//    already agree. **A CamelCase identifier needs nothing.** Fix 3b's brief
//    lists it among the degenerate spellings; that is measurably wrong, and
//    splitting it here would compile `'postgres' <-> 'store'`, a phrase that
//    matches nothing Postgres indexes. `camel_case_is_not_a_symbol` below pins
//    it.
//
// [`keyword_query_symbols`] does not try to know any of this: it lifts the raw
// spelling out of the question and `store::keyword_search_pg` hands it to
// Postgres's own `phraseto_tsquery`, which applies the table above by
// construction and therefore cannot drift from what the index stores.

/// Characters that may appear INSIDE an identifier or a path, and therefore do
/// not end a symbol run. `-` is deliberately absent: Postgres indexes a
/// hyphenated compound as the whole lexeme AND its parts at adjacent positions
/// (`intake-window` → `intake-window`, `intake`, `window`), so the loose tokens
/// already match every atom containing it — nothing is out of reach — while
/// including it would spend symbol slots on ordinary prose hyphenation
/// ("re-invited", "member-of") in nearly every question.
const SYMBOL_INNER_CHARS: &[char] = &['_', '.', '/', ':'];

/// Minimum length of a symbol run, after trimming. Excludes the sentence
/// abbreviations a `.`-aware scan otherwise picks up (`e.g`, `i.e`, `a.m`)
/// without excluding anything real: the shortest identifiers that actually
/// carry signal here are four characters (`a/b/c`, `ok_id`, `db.rs`).
const SYMBOL_MIN_CHARS: usize = 4;

/// Hard ceiling on the symbols [`keyword_query_symbols`] returns, the sibling of
/// [`KEYWORD_MAX_QUERY_TOKENS`].
///
/// Each symbol costs one more tsquery in the candidate leg's `ARRAY(SELECT …)`,
/// one more `@@` per scored candidate, and — the part that is not about
/// latency — one more unit in the COVERAGE DENOMINATOR, which it shares with
/// the loose tokens. A pasted stack trace or struct definition is mostly
/// symbols; uncapped, it would dilute every real query word to nothing.
///
/// Four, from measurement. `Store::keyword_search(k=50)` end-to-end against the
/// live dev ocean (29,772 atoms), warm, median of five:
///
/// | question | median |
/// |---|---|
/// | no symbol (`#1035`'s own shape) | **29 ms** |
/// | one identifier (`intake_window`) | 27 ms |
/// | one path (`backend/src/routes/queries.rs`) | 34 ms |
/// | path + identifier | 48 ms |
/// | two common-word identifiers (`user_id`, `graph_id`) | 87 ms |
/// | six `*_id` identifiers, capped to four | **110 ms** |
///
/// against a ~130 ms ask-path retrieve budget. The cost saturates rather than
/// growing linearly — common `*_id` symbols match heavily OVERLAPPING excerpt
/// sets, so the `BitmapOr` widens much more slowly than the symbol count
/// (capping at 2 instead of 4 moved that last row only 110 ms → 110 ms). Four
/// is therefore generous rather than tight: no question in
/// `backend/eval/github/coralswarm.questions.json` or `backend/eval/golden/`
/// yields more than ONE symbol, and four covers a question that names several
/// files at once.
///
/// If this ever needs tightening, this constant is the lever — the expensive
/// half is the `excerpt` predicate's per-row recheck for a symbol made of
/// common words, and those symbols are exactly the ones with no discriminative
/// value.
pub const KEYWORD_MAX_QUERY_SYMBOLS: usize = 4;

/// How many symbol-leg candidates `Store::keyword_search` materialises per
/// requested result — the sibling of [`KEYWORD_TITLE_CANDIDATE_MULTIPLE`], and
/// deliberately six times smaller.
///
/// The symbol leg is far more selective than the title OR (a path match on the
/// dev ocean returned 286 rows — every chunk of ONE file — and an identifier
/// phrase 4-24), so it does not need the title leg's twelvefold width. The cap
/// is still what converts the leg's cost from a property of the FILE into a
/// property of `k`: `backend/src/routes/queries.rs` alone chunks into 286
/// atoms, and every admitted candidate costs one `content_tsv` detoast
/// downstream.
///
/// `1` rather than `2`, measured: on a path-plus-identifier question against
/// the dev ocean, `2 * k` cost 62ms and `1 * k` 56ms, with no change to which
/// atoms the coverage sort returned — 50 symbol candidates is already
/// proportionate when the caller asked for 50 results and the title leg is
/// contributing 600 of its own.
pub const KEYWORD_SYMBOL_CANDIDATE_MULTIPLE: usize = 1;

/// The identifier- and path-shaped spellings in a query, lifted from the RAW
/// text (not from [`keyword_query_tokens`], which has already destroyed them).
///
/// A run of `[alphanumeric]` plus [`SYMBOL_INNER_CHARS`] qualifies when, after
/// trimming stray leading/trailing separators (a sentence's full stop, a
/// trailing colon), it still has a separator with an alphanumeric on BOTH
/// sides — i.e. the user joined two things into one word. Pure prose never
/// does; `Note:` trims to `Note` and `module` has no separator at all.
///
/// Additional rules, each with a reason:
/// * at least one ASCII LETTER, so `10:30` and `1.5` are not symbols;
/// * at least [`SYMBOL_MIN_CHARS`] characters, so `e.g` is not;
/// * lowercased and de-duplicated, because the `simple` text-search
///   configuration lowercases anyway, so `Queries.rs` and `queries.rs` compile
///   to the same tsquery and must not occupy two coverage slots;
/// * truncated to [`KEYWORD_MAX_QUERY_SYMBOLS`] in question order.
///
/// The returned strings reach Postgres as a bound `text[]` and are compiled by
/// `phraseto_tsquery`, which reads its argument as TEXT — `&`, `|`, `!`, `:*`
/// and quotes are punctuation to it, not operators. Unlike
/// [`keyword_query_tokens`], whose output is spliced into a `to_tsquery` STRING,
/// nothing here has to be — and nothing here is — sanitised for tsquery syntax.
pub fn keyword_query_symbols(query: &str) -> Vec<String> {
    let mut seen = std::collections::HashSet::new();
    let mut out = Vec::new();
    for run in query.split(|c: char| !(c.is_alphanumeric() || SYMBOL_INNER_CHARS.contains(&c))) {
        let trimmed = run.trim_matches(|c: char| SYMBOL_INNER_CHARS.contains(&c));
        if trimmed.chars().count() < SYMBOL_MIN_CHARS {
            continue;
        }
        if !trimmed.chars().any(|c| c.is_ascii_alphabetic()) {
            continue;
        }
        // An INTERIOR separator: the trim above already guarantees the first and
        // last characters are alphanumeric, so any separator left is interior.
        if !trimmed.chars().any(|c| SYMBOL_INNER_CHARS.contains(&c)) {
            continue;
        }
        let lowered = trimmed.to_lowercase();
        if seen.insert(lowered.clone()) {
            out.push(lowered);
            if out.len() == KEYWORD_MAX_QUERY_SYMBOLS {
                break;
            }
        }
    }
    out
}

/// Split `text` into its lowercase alphanumeric runs, keeping EVERY run —
/// including one-character ones — because the symbol mirror below tests
/// for CONTIGUITY, and a dropped token would silently close a gap between two
/// that are not actually adjacent. Deliberately NOT [`node_match_tokens`],
/// which drops runs shorter than two characters for the opposite (and, for its
/// own job, correct) reason.
pub fn adjacency_tokens(text: &str) -> Vec<String> {
    text.split(|c: char| !c.is_alphanumeric())
        .filter(|t| !t.is_empty())
        .map(|t| t.to_lowercase())
        .collect()
}

/// True when `text` contains the symbol's tokens CONTIGUOUSLY and in order —
/// a Rust mirror of Postgres's `phraseto_tsquery` `<->`.
///
/// **Where it deviates, and why it cannot not.** Postgres's parser keeps a
/// DOTTED spelling whole (`queries.rs` and `backend/src/routes/queries.rs` are
/// each ONE lexeme), so on Postgres `queries.rs` matches a document's *path
/// segments* but not the undivided path lexeme in its body. This function
/// splits on every non-alphanumeric, so it also matches `queries.rs` inside
/// `backend/src/routes/queries.rs` in body text. That is strictly more
/// permissive, in the direction of recall, and it is not fixable without
/// reimplementing Postgres's text-search parser in Rust. The SEMANTIC decisions
/// — that a symbol is a contiguous, ordered, whole-token match and that it is
/// one extra coverage dimension beside the loose tokens, not a replacement for
/// them — are the same as Postgres's, exactly as [`node_match_score`] shares
/// the reef weighting with `nodes.search_tsv`.
pub fn symbol_matches_text(symbol: &str, text: &str) -> bool {
    let needle = adjacency_tokens(symbol);
    if needle.is_empty() {
        return false;
    }
    let hay = adjacency_tokens(text);
    hay.windows(needle.len().min(hay.len().max(1)))
        .any(|w| w == needle.as_slice())
}

/// True when `source_id`'s `/`-separated PATH SEGMENTS contain the symbol's
/// segments contiguously and in order — a Rust mirror of Postgres's
/// `to_tsvector('simple', translate(source_id, '/', ' '))` index
/// (migration `0246`/`0099`).
///
/// Whole-SEGMENT, not substring: `queries.rs` matches
/// `…:blob:backend/src/routes/queries.rs:chunk:7` and does NOT match
/// `…/roster_queries.rs`, the same precision property that chose a segment
/// tsvector over `pg_trgm` on the Postgres side.
pub fn symbol_matches_source_path(symbol: &str, source_id: &str) -> bool {
    let seg = |s: &str| -> Vec<String> {
        s.split(['/', ':'])
            .filter(|t| !t.is_empty())
            .map(|t| t.to_lowercase())
            .collect()
    };
    let needle = seg(symbol);
    if needle.is_empty() {
        return false;
    }
    let hay = seg(source_id);
    if needle.len() > hay.len() {
        return false;
    }
    hay.windows(needle.len()).any(|w| w == needle.as_slice())
}

/// The possessive/plural stem of a query token: `"Caseys"` → `"Casey"`, so a query
/// like "who is Caseys mom" still matches the reef titled "Casey" without the
/// apostrophe (with the apostrophe, `node_match_tokens` already splits `Casey's`
/// into `Casey` + `s`). Only strips a trailing `s` when a ≥2-char stem remains.
fn possessive_stem(tok: &str) -> Option<&str> {
    tok.strip_suffix('s').filter(|s| s.len() >= 2)
}

/// True if `tok` (or its possessive/plural stem) is a whole token in `set`.
fn token_in(set: &std::collections::HashSet<String>, tok: &str) -> bool {
    set.contains(tok) || possessive_stem(tok).is_some_and(|s| set.contains(s))
}

/// Score a reef against pre-tokenised query tokens, gated by [`REEF_COVERAGE_FLOOR`]
/// (§5.6) — a Rust model of the Postgres `search_tsv` + `ts_rank_cd` path
/// (`idx_nodes_search_tsv`, migration `0108`/`0018`). Coverage qualification,
/// type weighting and reserved entity slots match the SQL path and
/// `store::select_reef_ids`/`store::flatten_reef_atoms`.
///
/// `is_entity` selects between [`WEIGHT_ENTITY_TITLE`] (weight class `'A'`) and
/// [`WEIGHT_OTHER_TITLE`] (`'C'`) for a `title` hit — a reef is never scored
/// under both, mirroring `idx_nodes_search_tsv`'s generated column, which
/// routes a reef's title into exactly one of those two `CASE` branches by
/// `type`. `aliases_raw` is the reef's `body.aliases` value as raw JSON-array
/// text (e.g. `["chris p", "chris"]`, the same representation the tsvector
/// column indexes) — [`node_match_tokens`]' non-alphanumeric split strips the
/// brackets/quotes/commas out exactly like Postgres's `'simple'` tsvector
/// parser does, so `"chris"` and `"p"` come out as real, separately-matchable
/// tokens rather than JSON syntax noise.
///
/// A query token counts once, at the BEST (highest-weight) class it matches in
/// (title > alias > summary), never double-counted across classes — same
/// "title priority" rule the pre-§5.6 `node_text_score` used. Returns `None`
/// when the reef fails BOTH qualification paths below; `Some(score)`
/// otherwise. A trailing possessive/plural `s` on the query token is
/// tolerated (see [`possessive_stem`]) so "Caseys" matches the reef "Casey".
///
/// ## Qualification: `REEF_COVERAGE_FLOOR` OR full-title-match (entities only)
///
/// The primary gate is coverage — matched tokens ÷ `query_tokens.len()` at or
/// above [`REEF_COVERAGE_FLOOR`] — exactly as originally specified.
///
/// **§5.6 deviation, deliberate and NOT in the design-locked spec:** an
/// `is_entity` reef ALSO qualifies, regardless of query length, if 100% of
/// its OWN distinct title tokens are found among the query tokens, provided
/// the title has at least [`FULL_TITLE_MATCH_MIN_TOKENS`] distinct tokens.
/// This exists because coverage alone is normalised against the QUERY's
/// length, not the entity's: a `T`-token entity title can achieve at most
/// `T / query_len` coverage even when every one of its own tokens appears
/// verbatim, so it needs `query_len <= 2*T` to ever clear a 0.5 floor. For
/// the common case — a two-word person's name — that is queries of 4 tokens
/// or fewer; measured directly against the §7.1 fixture ocean's real entity
/// reefs (2026-07-28): a realistic battery of natural-language questions
/// about those people qualified in only 8 of 23 cases before this escape
/// hatch, all short/name-only queries — every multi-clause question ("what
/// did Priya Anand say about the on-call pager handoff rotation") failed
/// outright, not merely ranked lower, undercutting §5.2's stated reason for
/// sibling-pull existing at all ("the only way an entity's corals are
/// retrieved when they never spell the entity's name"). The escape hatch
/// recovers 16 of those 23 cases.
///
/// The spec's own §5.6 worked example is the reason this is scoped to full
/// matches of title length >= 2, not a blanket entity exemption: it marks a
/// ONE-token entity title (`"Pricing"`, matching its own single word against
/// a 3-token query) coverage `1/3 ✗` and says this reef must stay excluded —
/// while ALSO stating "a reserved slot goes unused when no entity reef
/// qualifies", which is only true if coverage really does gate reservation
/// (§5.6's own text is inconsistent about this: the worked example is
/// presented as reservation's motivating case, but reservation cannot act on
/// a reef that already failed coverage, so the example doesn't actually
/// demonstrate what it claims to). [`FULL_TITLE_MATCH_MIN_TOKENS`] = 2 keeps
/// `"Pricing"` excluded exactly as that worked example requires — a 1-token
/// title matching its own word is indistinguishable from the coverage-floor
/// case the spec is protecting against, so it gets NO escape hatch, full
/// stop. A blanket entity exemption (no floor at all for `is_entity`) was
/// considered and rejected: it would let a single incidental word — the
/// exact "a reef matching a single incidental summary word normalises to 1.0
/// whenever it is the only reef that matched" over-optimism the coverage
/// floor exists to prevent — hand a weak match one of the
/// `RESERVED_ENTITY_SLOTS`, ahead of a well-corroborated non-entity reef.
///
/// **Two residual gaps this does NOT fix, left as documented limitations
/// rather than silently accepted:**
/// - A first-name-only (or last-name-only) mention in a long question still
///   fails: `"what did Priya say about the rotation"` matches only 1 of
///   `"Priya Anand"`'s 2 title tokens, which is neither full coverage-floor
///   clearance at that length nor a full-title match. Only an EXACT full-name
///   mention is rescued.
/// - A multi-word title containing a function word (e.g. `"ExamplePortal onboarding
///   support via Zendesk"`) may never achieve a literal 100% token match in
///   natural phrasing, because a word like "via" is unlikely to be repeated
///   verbatim in a real question and is not in [`NODE_MATCH_STOPWORDS`]. The
///   fix would be stripping such words from the TITLE side of this check
///   too (e.g. reusing [`node_query_tokens`]'s stopword filter on the title),
///   but `NODE_MATCH_STOPWORDS` has other call sites. Question words (`how`,
///   `does`, `use`, `who`, …) were added 2026-09-13 so a one-word entity
///   title can seed Ask traversal on a natural question; `work` is
///   deliberately NOT a stopword (`what does X work on`).
///
/// Unlike the Postgres path's `'english'` config, the summary comparison here
/// is whole-token only (no stemming) — there is no text-search dictionary to
/// call, so a query for "decided" will not match a summary containing only
/// "decision" here even though the Postgres path's `ts_rank_cd`
/// would. Accepted divergence: coverage floor, weight ordering and the
/// reservation rule (this function's actual job) are unaffected; only summary
/// recall on unstemmed forms differs, and the summary class is already the
/// lowest-weighted signal.
pub fn node_match_score(
    query_tokens: &[String],
    is_entity: bool,
    title: &str,
    aliases_raw: &str,
    summary: &str,
) -> Option<f64> {
    if query_tokens.is_empty() {
        return None;
    }
    let title_tokens: std::collections::HashSet<String> =
        node_match_tokens(title).into_iter().collect();
    let alias_tokens: std::collections::HashSet<String> =
        node_match_tokens(aliases_raw).into_iter().collect();
    let summary_tokens: std::collections::HashSet<String> =
        node_match_tokens(summary).into_iter().collect();
    let title_weight = if is_entity {
        WEIGHT_ENTITY_TITLE
    } else {
        WEIGHT_OTHER_TITLE
    };

    let mut matched: usize = 0;
    let mut score = 0.0_f64;
    for tok in query_tokens {
        let hit = if token_in(&title_tokens, tok) {
            Some(title_weight)
        } else if token_in(&alias_tokens, tok) {
            Some(WEIGHT_ALIAS)
        } else if token_in(&summary_tokens, tok) {
            Some(WEIGHT_SUMMARY)
        } else {
            None
        };
        if let Some(w) = hit {
            matched += 1;
            score += w;
        }
    }

    // Integer "at least half, rounded up" comparison rather than a float
    // ratio — matches the Postgres path's `matched_tokens >= ceil(floor *
    // token_count)` exactly (see `Store::keyword_search_node_atoms`'s SQL),
    // so both draw the qualification line at the identical token
    // count for every query length, not just approximately.
    let required = (REEF_COVERAGE_FLOOR * query_tokens.len() as f64).ceil() as usize;
    let coverage_ok = matched >= required;

    // §5.6 deviation (see doc comment above): a full match of the entity's
    // own title, at least FULL_TITLE_MATCH_MIN_TOKENS words of it, qualifies
    // independently of query length. `title_token_found_in_query` allows the
    // same possessive/plural tolerance as `token_in` (checked from the
    // title-token side rather than the query-token side).
    let full_title_match_ok = is_entity
        && title_tokens.len() >= FULL_TITLE_MATCH_MIN_TOKENS
        && title_tokens
            .iter()
            .all(|tt| title_token_found_in_query(tt, query_tokens));

    if !coverage_ok && !full_title_match_ok {
        return None;
    }
    Some(score)
}

/// True if title token `tt` is present among `query_tokens`, either exactly
/// or via a query token's possessive/plural stem (`"Caseys"` -> `"Casey"`) —
/// the reverse direction of [`token_in`], used only by
/// [`node_match_score`]'s full-title-match escape hatch.
fn title_token_found_in_query(tt: &str, query_tokens: &[String]) -> bool {
    query_tokens
        .iter()
        .any(|qt| qt == tt || possessive_stem(qt) == Some(tt))
}

// ── Shared filter type ────────────────────────────────────────────────────────

/// Filters applied to both the vector and keyword retrieval paths.
///
/// Both fields are optional.  When `None`, the corresponding column is not
/// filtered and all atoms in the graph are eligible.
#[derive(Debug, Clone, Default)]
pub struct RetrievalFilter {
    /// Restrict to atoms whose `scope` column equals this value.
    /// Typical values: `"private"`, `"account"`.
    pub scope: Option<String>,
    /// Restrict to atoms whose `source_type` column equals this value.
    /// Typical values: `"slack"`, `"email"`, `"transcript"`.
    ///
    /// Superseded by [`RetrievalFilter::source_types`] whenever that list is
    /// present and non-empty — see [`RetrievalFilter::source_type_list`] for
    /// the exact precedence every store applies.
    pub source_type: Option<String>,
    /// Restrict to atoms whose `source_type` is ANY of these values (GitHub
    /// connector eval, `docs/connectors/github.md` §5): the multi-valued
    /// form of `source_type`, so one ask can span both code tiers
    /// (`["github_code", "github_code_summary"]`) or the four narrative
    /// types at once. When present and non-empty it WINS over `source_type`
    /// (the single value is ignored, not unioned in); `None` or `Some([])`
    /// falls back to `source_type` unchanged, so every pre-existing caller
    /// keeps its exact behaviour. Stores bind this as one `text[]` parameter
    /// (`source_type = ANY($n)`) — the same shape `atoms.source_type = ANY(…)`
    /// already takes for the coding-session type set — via
    /// [`RetrievalFilter::source_type_list`].
    pub source_types: Option<Vec<String>>,
    /// Restrict to atoms whose `timestamp` column is at or after this value.
    pub since: Option<DateTime<Utc>>,
    /// Restrict to atoms whose `timestamp` column is at or before this value.
    pub until: Option<DateTime<Utc>>,
    /// Restrict to atoms whose `project` column equals this value
    /// (coding-session capture Phase 3, D6). Mirrors `scope`/`source_type`'s
    /// exact-equality semantics — an unrecognized value simply matches
    /// nothing. `Store::expand`'s graph-expansion output must ALSO be
    /// post-filtered against this value (P1-6, audit) — see
    /// `routes::queries::expand_filtered` — since `Store::expand` itself
    /// takes no filter and would otherwise drag in out-of-project sibling
    /// atoms via a matched reef's OTHER members.
    pub project: Option<String>,
    /// Restrict to atoms whose `source_id` starts with the
    /// `"cc:{session_id}:"` coding-session convention (migration 0076;
    /// `add_context`'s `session_id` input). Applied as an indexed
    /// (`idx_atoms_graph_source_id_pattern`, migration 0074) prefix-LIKE
    /// match — see `store::session_source_id_prefix` for the exact pattern
    /// (escapes `%`/`_`/`\` so a session id containing those characters can
    /// never widen the match past its own atoms). This field holds the RAW,
    /// caller-supplied session id, not the escaped SQL pattern — callers
    /// must validate its shape (non-empty, capped, no NUL) before setting
    /// this field; `store::session_source_id_prefix` only handles LIKE
    /// metacharacter escaping, not shape validation.
    pub session_id: Option<String>,
    /// Restrict to corals captured under sessions whose effective
    /// `coding_sessions.agent_name` equals this value (agent sessions,
    /// migration 0219). Resolved through a `coding_sessions` subquery on the
    /// `cc:{session_id}:` prefix — no atom column involved. Because
    /// subagents inherit the root's `agent_name`, this matches the whole
    /// agent tree.
    pub agent_name: Option<String>,
    /// Restrict to corals captured under sessions whose CANONICAL EFFECTIVE
    /// platform (`COALESCE(coding_sessions.platform,
    /// coding_sessions.platform_detected)` — `SessionIdentity::
    /// effective_platform`'s SQL equivalent) equals this value: a session
    /// that declared no platform still matches on the one its User-Agent
    /// detected. Callers canonicalize first
    /// (`services::platform::canonicalize_platform`). Same resolution and
    /// inheritance as `agent_name`.
    pub platform: Option<String>,
    /// Restrict to corals captured under ANY session in the tree rooted at
    /// this session id (`COALESCE(root_session_id, session_id)`), the root
    /// itself included — a whole agent's output, subagents and all. Unlike
    /// `session_id` this is not a `source_id` prefix match; it resolves
    /// through `coding_sessions`.
    pub root_session_id: Option<String>,
}

impl RetrievalFilter {
    /// The effective `source_type` restriction as ONE list, for stores to
    /// bind as a single `text[]` / `IN (…)` parameter:
    ///
    /// * `source_types` present and non-empty → that list, verbatim
    ///   (`source_type` is ignored — the list wins outright, it is not
    ///   unioned with the single value);
    /// * otherwise `source_type` present → a one-element list;
    /// * otherwise `None` → no restriction.
    ///
    /// Centralised here so the precedence rule lives in exactly one place
    /// rather than being re-derived at each of the fourteen store call
    /// sites that apply it.
    pub fn source_type_list(&self) -> Option<Vec<String>> {
        match self.source_types.as_deref() {
            Some(list) if !list.is_empty() => Some(list.to_vec()),
            _ => self.source_type.clone().map(|s| vec![s]),
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_filter_has_no_scope() {
        let f = RetrievalFilter::default();
        assert!(f.scope.is_none(), "default scope should be None");
        assert!(f.source_type.is_none(), "default source_type should be None");
        assert!(f.since.is_none(), "default since should be None");
        assert!(f.until.is_none(), "default until should be None");
        assert!(f.project.is_none(), "default project should be None");
        assert!(f.session_id.is_none(), "default session_id should be None");
    }

    #[test]
    fn question_words_are_stopwords_so_one_word_reefs_can_seed() {
        // "work" stays: "what does CoralSwarm work on" must keep a topical token.
        let toks = node_query_tokens("How does CoralSwarm use CodeRabbit?");
        assert_eq!(
            toks,
            vec!["coralswarm".to_string(), "coderabbit".to_string()]
        );
        let work = node_query_tokens("What does CoralSwarm work on?");
        assert_eq!(work, vec!["coralswarm".to_string(), "work".to_string()]);
    }

    // ── `keyword_query_tokens` (the atom keyword leg) ────────────────────────

    #[test]
    fn keyword_tokens_keep_only_the_content_words_of_a_long_question() {
        // The exact question the keyword leg used to return 2 hits for on the
        // live dev ocean, because `websearch_to_tsquery` conjoined all fifteen
        // of its words — `a` and `it` included.
        let toks = keyword_query_tokens(
            "Which module decides whether a repository file is skipped before the code tier \
             fetches it?",
        );
        assert_eq!(
            toks,
            vec![
                "module".to_string(),
                "decides".to_string(),
                "repository".to_string(),
                "file".to_string(),
                "skipped".to_string(),
                "code".to_string(),
                "tier".to_string(),
                "fetches".to_string(),
            ],
            "only the eight content words should survive"
        );
    }

    #[test]
    fn keyword_tokens_drop_more_than_reef_tokens_do() {
        // `whether`/`before` are NOT in `NODE_MATCH_STOPWORDS` — that list is
        // the reef coverage floor's denominator and is deliberately left
        // alone. The atom leg needs them gone: `before` is in 29% of this
        // corpus's atoms.
        for filler in ["whether", "before", "between", "into", "will"] {
            assert!(
                node_query_tokens(filler).contains(&filler.to_string()),
                "{filler} must stay a reef token (changing that moves REEF_COVERAGE_FLOOR)"
            );
            assert!(
                keyword_query_tokens(filler).is_empty(),
                "{filler} must not be an atom keyword token"
            );
        }
    }

    #[test]
    fn keyword_tokens_keep_domain_words_that_happen_to_be_frequent() {
        // `code`/`file`/`module` are high-frequency in a code-heavy ocean and
        // still carry meaning. Stopwording them would be a domain assumption;
        // the coverage sort is what stops them dominating instead.
        let toks = keyword_query_tokens("where is the code that reads a file in this module");
        assert_eq!(
            toks,
            vec!["code".to_string(), "reads".to_string(), "file".to_string(), "module".to_string()]
        );
    }

    #[test]
    fn keyword_tokens_are_distinct_and_safe_to_splice_into_a_tsquery() {
        let toks = keyword_query_tokens("Fix fix FIX the a&b | c:* 'quoted' <-> thing!");
        assert!(
            toks.iter().all(|t| t.chars().all(|c| c.is_ascii_alphanumeric())),
            "tokens must be alphanumeric-only, so no tsquery operator can survive: {toks:?}"
        );
        assert_eq!(
            toks.iter().filter(|t| *t == "fix").count(),
            1,
            "a repeated word must be counted once — coverage is over DISTINCT tokens: {toks:?}"
        );
    }

    #[test]
    fn keyword_tokens_are_capped() {
        // A pasted wall of text must not turn one retrieval into an unbounded scan.
        let long: String = (0..200)
            .map(|i| format!("token{i} "))
            .collect::<Vec<_>>()
            .concat();
        let toks = keyword_query_tokens(&long);
        assert_eq!(toks.len(), KEYWORD_MAX_QUERY_TOKENS);
        assert_eq!(
            toks[0], "token0",
            "the cap must keep the EARLIEST words, not an arbitrary slice"
        );
    }

    #[test]
    fn a_signal_free_query_yields_no_keyword_tokens() {
        for q in ["", "  --  ", "what is it", "how do they do this", "a"] {
            assert!(
                keyword_query_tokens(q).is_empty(),
                "{q:?} carries no retrieval signal, so the keyword leg must have nothing to search"
            );
        }
    }

    // ── `keyword_query_symbols` (fix 3b) ─────────────────────────────────────

    #[test]
    fn an_underscored_identifier_is_a_symbol() {
        let syms = keyword_query_symbols("which function owns the intake_window logic");
        assert_eq!(syms, vec!["intake_window".to_string()], "got {syms:?}");
    }

    #[test]
    fn a_path_keeps_its_separators() {
        let syms = keyword_query_symbols("walk me through backend/src/routes/queries.rs, please");
        assert_eq!(syms, vec!["backend/src/routes/queries.rs".to_string()], "got {syms:?}");
    }

    #[test]
    fn a_trailing_full_stop_is_trimmed_but_a_file_extension_is_not() {
        // The same character, two meanings, decided by position.
        assert_eq!(keyword_query_symbols("look at queries.rs."), vec!["queries.rs".to_string()]);
        assert_eq!(keyword_query_symbols("look at the module."), Vec::<String>::new());
    }

    #[test]
    fn a_rust_path_is_one_symbol() {
        assert_eq!(
            keyword_query_symbols("where is store::PostgresStore::keyword_search called"),
            vec!["store::postgresstore::keyword_search".to_string()]
        );
    }

    /// `to_tsvector('simple','PostgresStore')` is `'postgresstore':1` and
    /// [`node_match_tokens`] lowercases without splitting on case, so both sides
    /// already produce the same single token. Splitting it here would compile
    /// `'postgres' <-> 'store'`, which matches NOTHING Postgres indexes — a
    /// silent recall loss. See the section comment above.
    #[test]
    fn camel_case_is_not_a_symbol() {
        assert!(keyword_query_symbols("where is PostgresStore built").is_empty());
        assert!(keyword_query_symbols("getUserById and MyStruct").is_empty());
        // ...but a CamelCase name joined by a real separator still is one.
        assert_eq!(
            keyword_query_symbols("MyStruct::new"),
            vec!["mystruct::new".to_string()]
        );
    }

    /// A hyphenated compound is indexed by Postgres as the whole lexeme AND its
    /// parts at adjacent positions, so the loose tokens already reach every atom
    /// containing it. Treating it as a symbol would spend a slot (and a coverage
    /// denominator unit) on ordinary prose hyphenation.
    #[test]
    fn a_hyphenated_word_is_not_a_symbol() {
        assert!(keyword_query_symbols("how is a removed member re-invited").is_empty());
        assert!(keyword_query_symbols("the member-of edge on 2026-09-22").is_empty());
    }

    #[test]
    fn prose_and_abbreviations_are_not_symbols() {
        for q in [
            "Which module decides whether a repository file is skipped?",
            "Note: the code tier fetches it",
            "e.g. the thing at 10:30 costs 1.5 credits",
            "what is it",
        ] {
            assert!(
                keyword_query_symbols(q).is_empty(),
                "{q:?} has no identifier or path in it — got {:?}",
                keyword_query_symbols(q)
            );
        }
    }

    #[test]
    fn symbols_are_deduplicated_case_insensitively_and_capped() {
        let syms = keyword_query_symbols("Queries.rs vs queries.rs and QUERIES.RS");
        assert_eq!(syms, vec!["queries.rs".to_string()], "got {syms:?}");

        let many = (0..40).map(|i| format!("sym_{i}")).collect::<Vec<_>>().join(" ");
        assert_eq!(keyword_query_symbols(&many).len(), KEYWORD_MAX_QUERY_SYMBOLS);
    }

    /// The symbols are bound as a `text[]` and compiled by `phraseto_tsquery`,
    /// which reads its argument as TEXT — so unlike `keyword_query_tokens`'
    /// output (spliced into a `to_tsquery` STRING) they need no sanitising. This
    /// pins that nothing in the extractor reintroduces a sanitisation
    /// assumption by, say, stripping quotes.
    #[test]
    fn tsquery_operators_survive_verbatim_because_they_are_never_spliced() {
        let syms = keyword_query_symbols("look at a&b|c:*'d");
        // `&`, `|`, `'` all end a run; `:` is an inner char, so `c:` trims to
        // nothing usable and no symbol survives.
        assert!(syms.is_empty(), "got {syms:?}");
    }

    #[test]
    fn filter_fields_round_trip() {
        let since: DateTime<Utc> = "2026-01-01T00:00:00Z".parse().unwrap();
        let until: DateTime<Utc> = "2026-06-01T00:00:00Z".parse().unwrap();
        let f = RetrievalFilter {
            scope: Some("account".to_string()),
            source_type: Some("slack".to_string()),
            source_types: None,
            since: Some(since),
            until: Some(until),
            project: Some("coralswarm".to_string()),
            session_id: Some("sess-abc".to_string()),
            agent_name: None,
            platform: None,
            root_session_id: None,
        };
        assert_eq!(f.scope.as_deref(), Some("account"));
        assert_eq!(f.source_type.as_deref(), Some("slack"));
        assert_eq!(f.since, Some(since));
        assert_eq!(f.until, Some(until));
        assert_eq!(f.project.as_deref(), Some("coralswarm"));
        assert_eq!(f.session_id.as_deref(), Some("sess-abc"));
    }

    // ── `source_types` precedence (GitHub connector eval, §5) ──────────────

    #[test]
    fn source_type_list_is_none_when_neither_field_is_set() {
        assert_eq!(RetrievalFilter::default().source_type_list(), None);
    }

    #[test]
    fn source_type_list_wraps_the_single_value_when_no_list_is_given() {
        let f = RetrievalFilter {
            source_type: Some("github_code".to_string()),
            ..Default::default()
        };
        assert_eq!(f.source_type_list(), Some(vec!["github_code".to_string()]));
    }

    #[test]
    fn source_type_list_prefers_the_list_over_the_single_value() {
        // The list WINS outright: the single value is not unioned in.
        let f = RetrievalFilter {
            source_type: Some("slack".to_string()),
            source_types: Some(vec![
                "github_code".to_string(),
                "github_code_summary".to_string(),
            ]),
            ..Default::default()
        };
        assert_eq!(
            f.source_type_list(),
            Some(vec!["github_code".to_string(), "github_code_summary".to_string()])
        );
    }

    #[test]
    fn source_type_list_treats_an_empty_list_as_absent() {
        // `"source_types": []` on the wire must not silently match NOTHING
        // — it falls through to the single value (or to no filter at all).
        let f = RetrievalFilter {
            source_type: Some("slack".to_string()),
            source_types: Some(Vec::new()),
            ..Default::default()
        };
        assert_eq!(f.source_type_list(), Some(vec!["slack".to_string()]));
        let g = RetrievalFilter {
            source_types: Some(Vec::new()),
            ..Default::default()
        };
        assert_eq!(g.source_type_list(), None);
    }

    #[test]
    fn query_tokens_drop_stopwords_and_short_and_dupes() {
        let toks = node_query_tokens("Give me a breakdown of Dorian dorian");
        // "give", "me", "a", "of" are stopwords; "dorian" deduped to one.
        assert_eq!(toks, vec!["breakdown".to_string(), "dorian".to_string()]);
    }

    #[test]
    fn possessive_query_matches_reef_title() {
        // Single-token query so coverage (1/1) is trivially satisfied either
        // way — this test is specifically about `possessive_stem`, not §5.6
        // coverage. "Caseys" (no apostrophe) must still hit the reef "Casey".
        let toks = node_query_tokens("Caseys");
        assert_eq!(toks, vec!["caseys".to_string()]);
        let stated = node_match_score(&toks, true, "Casey", "", "");
        assert_eq!(stated, Some(WEIGHT_ENTITY_TITLE), "Caseys should match reef Casey");
        // The apostrophe form already worked (splits to `Casey`).
        let ap = node_query_tokens("Casey's");
        assert_eq!(node_match_score(&ap, true, "Casey", "", ""), Some(WEIGHT_ENTITY_TITLE));
        // Don't over-strip: a 2-char token isn't reduced below 2 chars.
        assert!(possessive_stem("is").is_none());
    }

    #[test]
    fn title_match_outscores_summary_match() {
        let q = node_query_tokens("dorian");
        let title_hit = node_match_score(&q, true, "Dorian", "", "some unrelated summary");
        let summary_hit = node_match_score(&q, true, "Some Reef", "", "dorian leads the team");
        assert_eq!(title_hit, Some(WEIGHT_ENTITY_TITLE));
        assert_eq!(summary_hit, Some(WEIGHT_SUMMARY));
        assert!(title_hit > summary_hit);
    }

    #[test]
    fn no_overlap_qualifies_for_nothing() {
        let q = node_query_tokens("dorian");
        assert_eq!(
            node_match_score(&q, true, "Jason", "", "talks about the roadmap"),
            None
        );
    }

    #[test]
    fn token_match_is_whole_word_not_substring() {
        let q = node_query_tokens("man");
        // "man" must not match inside "management".
        assert_eq!(
            node_match_score(&q, false, "Management plan", "", "team management"),
            None
        );
    }

    #[test]
    fn each_query_token_counted_once_via_title_priority() {
        // A token present in BOTH title and summary counts as a title hit only.
        let q = node_query_tokens("dorian");
        let score = node_match_score(&q, true, "Dorian", "", "dorian dorian dorian");
        assert_eq!(score, Some(WEIGHT_ENTITY_TITLE));
    }

    // ── §5.6: coverage-floor qualification at the boundary ─────────────────────

    #[test]
    fn coverage_at_exactly_the_floor_qualifies() {
        // 4-token query, reef matches exactly 2 -> coverage == 0.5 == floor.
        // Inclusive: must qualify, not be excluded as "below" the floor.
        let q = node_query_tokens("alpha bravo charlie delta");
        assert_eq!(q.len(), 4);
        let score = node_match_score(&q, false, "alpha bravo", "", "");
        assert_eq!(score, Some(WEIGHT_OTHER_TITLE * 2.0));
    }

    #[test]
    fn coverage_just_under_the_floor_excludes() {
        // Same 4-token query, reef matches only 1 -> coverage == 0.25 < 0.5.
        let q = node_query_tokens("alpha bravo charlie delta");
        assert_eq!(node_match_score(&q, false, "alpha", "", ""), None);
    }

    #[test]
    fn coverage_floor_uses_ceiling_for_odd_token_counts() {
        // 3-token query: floor requires ceil(0.5*3) = 2 matches, not 1 (which
        // would be a mere 0.33 raw ratio, correctly excluded) and not 3.
        let q = node_query_tokens("alpha bravo charlie");
        assert_eq!(q.len(), 3);
        assert_eq!(node_match_score(&q, false, "alpha", "", ""), None);
        assert_eq!(
            node_match_score(&q, false, "alpha bravo", "", ""),
            Some(WEIGHT_OTHER_TITLE * 2.0)
        );
    }

    // ── §5.6: entity titles outrank assertion titles ────────────────────────────

    #[test]
    fn entity_title_partial_coverage_outranks_assertion_title_full_coverage() {
        // The spec's claim: an entity reef matching FEWER query tokens can
        // still outrank an assertion (non-entity) reef matching MORE, because
        // weight class 'A' (entity title) is ~5x class 'C' (other title). For
        // both reefs to even be in the ranking, both must first clear
        // `REEF_COVERAGE_FLOOR` on the SAME query — a 2-token query is the
        // shortest one where a 1-of-2 entity match (50%) still qualifies.
        let q = node_query_tokens("pricing model");
        assert_eq!(q.len(), 2);

        // The entity "Pricing" matches only "pricing" (1/2 = 50%, exactly the
        // floor) — qualifies, barely.
        let entity_score = node_match_score(&q, true, "Pricing", "", "").unwrap();
        // The widened decision reef matches BOTH "pricing" and "model"
        // (2/2 = 100%) — full coverage, strictly more tokens matched.
        let assertion_score = node_match_score(
            &q,
            false,
            "We decided to change the pricing model for enterprise customers",
            "",
            "",
        )
        .unwrap();
        assert_eq!(entity_score, WEIGHT_ENTITY_TITLE);
        assert_eq!(assertion_score, WEIGHT_OTHER_TITLE * 2.0);
        assert!(
            entity_score > assertion_score,
            "a partial-coverage entity-title hit ({entity_score}) must outrank a \
             full-coverage assertion-title hit ({assertion_score})"
        );
    }

    // ── §5.6 Gap 1: alias-only retrieval ─────────────────────────────────────────

    #[test]
    fn alias_only_match_qualifies_even_though_title_has_no_overlap() {
        // ER taught the survivor "nomad" as an alias; the reef's own title
        // ("Jordan Rivera") shares no tokens with the query at all.
        let q = node_query_tokens("nomad");
        let score = node_match_score(&q, true, "Jordan Rivera", "[\"nomad\"]", "");
        assert_eq!(score, Some(WEIGHT_ALIAS));
    }

    #[test]
    fn alias_json_punctuation_does_not_pollute_tokens() {
        // `["chris p", "chris", "cp"]` must tokenise to real words ("chris",
        // "cp"), not JSON syntax noise (brackets/quotes/commas), and a query
        // for a word that ONLY the alias list contains must still qualify.
        // ("p" alone is not used here: both `node_match_tokens` and Postgres's
        // `simple` tsvector parser tokenise it differently at length 1 — see
        // this module's `node_match_tokens`, which — like `node_query_tokens`
        // — drops sub-two-character tokens; a single-character alias word is
        // a known, narrow divergence between the two backends, not something
        // this test asserts either way.)
        let aliases = "[\"chris p\", \"chris\", \"cp\"]";
        let q_chris = node_query_tokens("chris");
        assert_eq!(
            node_match_score(&q_chris, true, "Chris Marchetti", aliases, ""),
            Some(WEIGHT_ENTITY_TITLE), // "chris" also matches the title itself
        );
        // "cp" only appears in the alias, not the title "Chris Marchetti".
        let q_cp = node_query_tokens("cp");
        assert_eq!(
            node_match_score(&q_cp, true, "Chris Marchetti", aliases, ""),
            Some(WEIGHT_ALIAS),
        );
    }

    // ── §5.6 deviation: full-title-match escape hatch for entity reefs ─────────
    //
    // Deliberate deviation from the design-locked spec — see this module's
    // header comment and `node_match_score`'s doc comment for the full
    // rationale (coverage alone undercuts §5.2's sibling-pull guarantee on
    // multi-clause questions; measured 8/23 -> 16/23 recovery on the §7.1
    // fixture ocean's realistic queries; scoped to >= 2 title tokens so it
    // does not reopen the spec's own coverage-only `"Pricing"` case).

    #[test]
    fn full_title_match_rescues_a_long_question_a_two_word_entity_name_would_otherwise_fail() {
        // The exact regression this escape hatch exists to fix: a multi-clause
        // question naming both words of a two-word entity's title fails the
        // ordinary coverage floor (2 matched of 6+ query tokens < 0.5), but
        // must still qualify because the FULL name was used.
        let q = node_query_tokens("Priya Anand on-call pager handoff rotation");
        assert!(q.len() >= 5, "this case only proves anything if the query is long enough to fail coverage on its own: got {q:?}");
        // Confirm it actually WOULD fail on coverage alone (title-only score,
        // no alias/summary help) — otherwise this test would not be isolating
        // the escape hatch at all.
        let required = (REEF_COVERAGE_FLOOR * q.len() as f64).ceil() as usize;
        assert!(2 < required, "test setup: expected the ordinary floor to fail this case (matched=2, required={required})");

        let score = node_match_score(&q, true, "Priya Anand", "", "");
        assert!(
            score.is_some(),
            "a full match of a 2-token entity title must qualify regardless of query length"
        );
    }

    #[test]
    fn full_title_match_requires_every_title_token_not_just_one() {
        // Only "Priya" present (not "Anand") on an otherwise-identical long
        // question — a PARTIAL name match must NOT trigger the escape hatch
        // (this is the documented residual gap: only a FULL name mention is
        // rescued).
        let q = node_query_tokens("what did Priya say about the on-call pager rotation");
        let required = (REEF_COVERAGE_FLOOR * q.len() as f64).ceil() as usize;
        assert!(1 < required, "test setup: expected the ordinary floor to fail on a single matched token");

        assert_eq!(
            node_match_score(&q, true, "Priya Anand", "", ""),
            None,
            "a partial (first-name-only) match must not be rescued by the full-title-match escape hatch"
        );
    }

    #[test]
    fn full_title_match_escape_hatch_is_entity_only() {
        // The SAME full-title-match condition against a non-entity (decision)
        // reef must NOT qualify via the escape hatch — it is scoped to
        // `is_entity` only, exactly as specified.
        let q = node_query_tokens("Priya Anand on-call pager handoff rotation");
        assert_eq!(
            node_match_score(&q, false, "Priya Anand", "", ""),
            None,
            "the full-title-match escape hatch must not apply to non-entity reefs"
        );
    }

    #[test]
    fn full_title_match_one_token_title_is_not_rescued_pricing_shaped_case() {
        // The spec's own §5.6 worked example: a ONE-token entity title
        // ("Pricing") matching its own single word against a 3-token query is
        // explicitly marked coverage `1/3 ✗` and must stay excluded.
        // FULL_TITLE_MATCH_MIN_TOKENS=2 exists specifically so this case is
        // NOT rescued by the escape hatch (a 1-token full match is
        // indistinguishable from the ordinary coverage-floor case the spec
        // protects against).
        let q = node_query_tokens("decide pricing model");
        assert_eq!(q.len(), 3);
        assert_eq!(
            node_match_score(&q, true, "Pricing", "", ""),
            None,
            "a 1-token entity title must NOT get a free pass from the full-title-match escape hatch"
        );
    }

    #[test]
    fn full_title_match_boundary_at_exactly_two_title_tokens() {
        // Exactly at FULL_TITLE_MATCH_MIN_TOKENS=2: a 2-token entity title,
        // fully matched, on a query long enough to fail the ordinary floor,
        // must qualify (this is the boundary the guard is defined at, not
        // merely "more than 1").
        assert_eq!(FULL_TITLE_MATCH_MIN_TOKENS, 2);
        let q = node_query_tokens("Kenji Sato annual performance review discussion notes");
        let required = (REEF_COVERAGE_FLOOR * q.len() as f64).ceil() as usize;
        assert!(2 < required, "test setup: expects the ordinary floor to fail");
        assert!(
            node_match_score(&q, true, "Kenji Sato", "", "").is_some(),
            "a full match of an exactly-2-token title must qualify"
        );
    }

    #[test]
    fn full_title_match_does_not_lower_the_score_itself() {
        // The escape hatch changes QUALIFICATION only, never the SCORE: a
        // rescued reef scores exactly what its matched tokens are worth under
        // the ordinary weighting, same as a reef that qualified via coverage.
        let q = node_query_tokens("Priya Anand on-call pager handoff rotation");
        let score = node_match_score(&q, true, "Priya Anand", "", "").unwrap();
        assert_eq!(
            score,
            WEIGHT_ENTITY_TITLE * 2.0,
            "score is still just 'two title-weight tokens matched', unaffected by which \
             qualification path let the reef through"
        );
    }
}
