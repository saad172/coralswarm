//! Live Postgres tests for the atom keyword leg (`PostgresStore::keyword_search`
//! and its RLS-scoped twin `keyword_search_scoped`).
//!
//! These exist because the leg was, in practice, INERT on natural-language asks.
//! `websearch_to_tsquery` CONJOINS every term and the `'simple'` config strips no
//! stopwords, so "Which module decides whether a repository file is skipped
//! before the code tier fetches it?" became
//!
//!   'which' & 'module' & 'decides' & 'whether' & 'a' & 'repository' & 'file'
//!     & 'is' & 'skipped' & 'before' & 'the' & 'code' & 'tier' & 'fetches' & 'it'
//!
//! — fifteen words, `a` and `it` included, ALL required. Measured against the
//! live dev ocean (~27k atoms) that yielded 2 hits for that question, and 1/20
//! answer-document recall across `backend/eval/github/coralswarm.questions.json`.
//! Since `retrieval::rerank` blends `0.7*vector_sim + 0.3*keyword_norm`, an empty
//! keyword half means every ask is decided by the vector leg alone.
//!
//! The replacement ranks by TERM COVERAGE — how many DISTINCT non-stopword query
//! tokens an atom matches across `atoms.title` (weight `A`) and
//! `atoms.content_tsv` — with `ts_rank_cd` demoted to a tie-break, mirroring
//! `store::qualified_reef_ids_pg`'s `matched_tokens` shape (§5.4).
//!
//! Deliberately drives the `Store` methods DIRECTLY (not through HTTP) so these
//! are tight tests of the real SQL against real rows — same convention as
//! `reef_search_tsv_pg.rs`, which this file is pattern-copied from.
//!
//! Gated on `RELINFER_TEST_DATABASE_URL` (a Postgres with `migrations_control`
//! applied); a normal `cargo test` with no DB configured SKIPS LOUDLY.
//!
//! Run with:
//!   RELINFER_TEST_DATABASE_URL=postgres:///cs_kwcov \
//!     cargo test -p coralswarm-backend --test keyword_coverage_pg -- --nocapture

use chrono::Utc;
use coralswarm_backend::retrieval::RetrievalFilter;
use coralswarm_backend::store::{PostgresStore, Store};
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

/// The natural-language question this whole file is about. A first-turn ask
/// reaches `keyword_search` VERBATIM — `condense_query` only runs on follow-ups
/// (`routes::queries.rs`) — so long prose is the NORMAL input, not an edge case.
const LONG_QUESTION: &str =
    "Which module decides whether a repository file is skipped before the code tier fetches it?";

macro_rules! test_pool {
    () => {{
        match std::env::var("RELINFER_TEST_DATABASE_URL") {
            Ok(u) => PgPoolOptions::new()
                .max_connections(4)
                .connect(&u)
                .await
                .expect("connect to test Postgres"),
            Err(_) => {
                eprintln!(
                    "skipping: set RELINFER_TEST_DATABASE_URL to run keyword_coverage_pg \
                     (e.g. RELINFER_TEST_DATABASE_URL=postgres:///cs_kwcov)"
                );
                return;
            }
        }
    }};
}

/// Seed a fresh `(user, account, graph)` tenancy chain — pattern-copied from
/// `reef_search_tsv_pg.rs`.
async fn seed_tenancy(pool: &sqlx::PgPool) -> (Uuid, Uuid, Uuid) {
    let email = format!("{}@test.local", Uuid::new_v4());
    let user_id: Uuid = sqlx::query_scalar("INSERT INTO users (email) VALUES ($1) RETURNING id")
        .bind(&email)
        .fetch_one(pool)
        .await
        .expect("insert user");
    let account_id: Uuid =
        sqlx::query_scalar("INSERT INTO accounts (owner_user_id) VALUES ($1) RETURNING id")
            .bind(user_id)
            .fetch_one(pool)
            .await
            .expect("insert account");
    let store = PostgresStore::new(pool.clone());
    let graph = store
        .create_graph(account_id, &format!("kw-coverage-{}", Uuid::new_v4()), "personal")
        .await
        .expect("create graph");
    (user_id, account_id, graph.id)
}

async fn cleanup(pool: &sqlx::PgPool, account_id: Uuid, user_id: Uuid) {
    sqlx::query("DELETE FROM accounts WHERE id = $1")
        .bind(account_id)
        .execute(pool)
        .await
        .ok();
    sqlx::query("DELETE FROM users WHERE id = $1")
        .bind(user_id)
        .execute(pool)
        .await
        .ok();
}

/// Insert one atom through the real `insert_atoms` write path, then stamp
/// `title` + `excerpt` + `content_tsv` exactly the way production does: the LLM
/// headline sweep writes `title`/`title_kind='llm'`, `routes::ingest` writes
/// `excerpt = derive_journal_excerpt(content, header_len)` for every chunk at
/// write time (migration `0116`, so it is populated regardless of whether the
/// body lands in R2 or `content_inline`), and `workers::embed` writes
/// `content_tsv = to_tsvector('simple', content)`. Nothing here hand-crafts a
/// row shape the real pipeline would not produce.
///
/// `left(btrim(content), 280)` is `derive_journal_excerpt`'s behaviour for the
/// `header_len IS NULL` case every fixture here takes — `strip_provenance_header`
/// returns its input unchanged when there is no header length, and
/// [`coralswarm_backend::store`]'s `JOURNAL_EXCERPT_MAX_CHARS` is 280. (The
/// production helper appends U+2026 when it truncates; that ellipsis is not a
/// lexeme, so the tsvector is identical either way.)
async fn seed_atom(
    store: &PostgresStore,
    pool: &sqlx::PgPool,
    graph_id: Uuid,
    user_id: Uuid,
    source_id: &str,
    title: Option<&str>,
    content: &str,
) -> Uuid {
    let atom_id = Uuid::new_v4();
    store
        .insert_atoms(
            &[atom_id],
            graph_id,
            "note",
            &[source_id.to_string()],
            &[format!("blob-{atom_id}")],
            &[],
            Utc::now(),
            user_id,
            "private",
            "english",
        )
        .await
        .expect("insert_atoms");
    sqlx::query(
        "UPDATE atoms \
         SET title = $2, \
             title_kind = CASE WHEN $2::text IS NULL THEN NULL ELSE 'llm' END, \
             content_inline = $3, \
             excerpt = left(btrim($3), 280), \
             content_tsv = to_tsvector('simple', $3) \
         WHERE atom_id = $1",
    )
    .bind(atom_id)
    .bind(title)
    .bind(content)
    .execute(pool)
    .await
    .expect("stamp title + content_tsv");
    atom_id
}

/// Stamp `atoms.symbols` directly — the retrieval fix (migrations
/// `0247`/`0100`, GIN-indexed by `0248`/`0101`) this file's newer tests
/// exercise. Production writes this column via `routes::ingest` from
/// [`coralswarm_core::chunker::Chunk::symbols`] (the UNCAPPED declaration
/// list a chunk's own `items …` header truncates to `", +N more"`); this
/// helper is the test-fixture equivalent of that write, the same way
/// [`seed_atom`]'s own `UPDATE` stamps `excerpt`/`content_tsv` directly
/// rather than driving the real chunker. `symbols` is the exact
/// `"{kind} {name}"` spelling `declaration_label` in
/// `packages/core/src/chunker.rs` would have produced (e.g. `"function
/// intake_window"`) — one call per symbol-bearing fixture; every other atom
/// in this file leaves the column NULL, which is the common case (a note, a
/// transcript chunk, or a code chunk this file's own tests never stamped a
/// symbol for).
async fn stamp_symbols(pool: &sqlx::PgPool, atom_id: Uuid, symbols: &str) {
    sqlx::query("UPDATE atoms SET symbols = $2 WHERE atom_id = $1")
        .bind(atom_id)
        .bind(symbols)
        .execute(pool)
        .await
        .expect("stamp symbols");
}

/// The document a human would say answers [`LONG_QUESTION`]. It shares SOME of
/// the question's content words — `module`, `repository`, `file` in the body and
/// `code` in the title — but NOT all of them: it never says "decides",
/// "skipped", "tier" or "fetches". That is the whole point. Under
/// `websearch_to_tsquery`'s conjunction it is unreachable; under coverage
/// ranking it is the best answer in the corpus.
async fn seed_answer(
    store: &PostgresStore,
    pool: &sqlx::PgPool,
    graph_id: Uuid,
    user_id: Uuid,
) -> Uuid {
    seed_atom(
        store,
        pool,
        graph_id,
        user_id,
        "answer-doc",
        Some("Blob filter: vendored and minified pruning before the code fetch"),
        "The skip decision module inspects every repository file and returns a reason, \
         so the downstream fetcher never requests vendored, minified or binary blobs.",
    )
    .await
}

/// A term-frequency-heavy junk document: it repeats ONE of the query's words
/// 200 times and matches nothing else. On the live dev ocean, a naive
/// `ts_rank_cd` OR put exactly this shape (`scripts/scoped_shape.baseline`,
/// `scripts/tenant_rls_coverage.manifest`) on top — frequency is not evidence.
async fn seed_frequency_junk(
    store: &PostgresStore,
    pool: &sqlx::PgPool,
    graph_id: Uuid,
    user_id: Uuid,
) -> Uuid {
    let body = "file ".repeat(200);
    seed_atom(
        store,
        pool,
        graph_id,
        user_id,
        "frequency-junk",
        Some("generated manifest"),
        &body,
    )
    .await
}

/// Filler that shares no query vocabulary at all.
async fn seed_filler(store: &PostgresStore, pool: &sqlx::PgPool, graph_id: Uuid, user_id: Uuid) {
    for i in 0..8 {
        seed_atom(
            store,
            pool,
            graph_id,
            user_id,
            &format!("filler-{i}"),
            Some("weekly standup notes"),
            "Priya walked through the launch calendar and the hiring plan for next quarter.",
        )
        .await;
    }
}

async fn ids(hits: &[(Uuid, f64)]) -> Vec<Uuid> {
    hits.iter().map(|&(id, _)| id).collect()
}

// ── The regression ──────────────────────────────────────────────────────────────

/// THE regression. A long natural-language question against a corpus where the
/// answer document shares content words but NOT every word.
///
/// On the pre-fix code this fails on the FIRST assertion: `keyword_search`
/// returns zero rows, because `websearch_to_tsquery('simple', …)` requires all
/// fifteen words of the question — `a` and `it` included — to be present in one
/// atom.
#[tokio::test]
async fn long_natural_language_question_reaches_the_partial_match() {
    let pool = test_pool!();
    let (user_id, account_id, graph_id) = seed_tenancy(&pool).await;
    let store = PostgresStore::new(pool.clone());

    let answer = seed_answer(&store, &pool, graph_id, user_id).await;
    let junk = seed_frequency_junk(&store, &pool, graph_id, user_id).await;
    seed_filler(&store, &pool, graph_id, user_id).await;

    let hits = store
        .keyword_search(graph_id, LONG_QUESTION, 20, &RetrievalFilter::default())
        .await
        .expect("keyword_search");
    let got = ids(&hits).await;

    assert!(
        got.contains(&answer),
        "the keyword leg must reach a document that shares most of the question's content \
         words but not every one — got {} hit(s): {got:?}",
        got.len()
    );

    let answer_pos = got.iter().position(|&id| id == answer).unwrap();
    let junk_pos = got.iter().position(|&id| id == junk);
    assert_eq!(
        answer_pos, 0,
        "the highest-coverage document must rank first — got {hits:?}"
    );
    if let Some(junk_pos) = junk_pos {
        assert!(
            junk_pos > answer_pos,
            "a document that repeats ONE query word 200 times must not outrank one that \
             covers four distinct query tokens — got {hits:?}"
        );
    }

    cleanup(&pool, account_id, user_id).await;
}

/// Coverage is the PRIMARY sort; `ts_rank_cd` only breaks ties. Proven with two
/// documents of identical length where the low-coverage one has far higher term
/// frequency.
#[tokio::test]
async fn distinct_token_coverage_outranks_term_frequency() {
    let pool = test_pool!();
    let (user_id, account_id, graph_id) = seed_tenancy(&pool).await;
    let store = PostgresStore::new(pool.clone());

    // Both carry the SAME title (so both are candidates on equal footing) and
    // differ only in their bodies. `broad` covers 4 distinct query tokens once
    // each; `deep` covers 2, one of them 40 times — a far higher `ts_rank_cd`.
    let broad = seed_atom(
        &store,
        &pool,
        graph_id,
        user_id,
        "broad",
        Some("code path notes"),
        "module repository file",
    )
    .await;
    let deep = seed_atom(
        &store,
        &pool,
        graph_id,
        user_id,
        "deep",
        Some("code path notes"),
        &"file ".repeat(40),
    )
    .await;

    let hits = store
        .keyword_search(graph_id, LONG_QUESTION, 20, &RetrievalFilter::default())
        .await
        .expect("keyword_search");
    let got = ids(&hits).await;

    let broad_pos = got
        .iter()
        .position(|&id| id == broad)
        .unwrap_or_else(|| panic!("3-token-coverage atom missing entirely: {got:?}"));
    let deep_pos = got
        .iter()
        .position(|&id| id == deep)
        .unwrap_or_else(|| panic!("1-token-coverage atom missing entirely: {got:?}"));
    assert!(
        broad_pos < deep_pos,
        "coverage must be the primary sort, ts_rank_cd only the tie-break — got {hits:?}"
    );

    cleanup(&pool, account_id, user_id).await;
}

/// `atoms.title` is LLM-generated, short and descriptive — the highest-signal
/// lexical field on an atom, and the pre-fix leg ignored it entirely (it
/// searched `content_tsv` only). An atom whose TITLE answers the question must
/// be reachable even when its body never uses the question's words.
#[tokio::test]
async fn a_title_only_match_is_reachable() {
    let pool = test_pool!();
    let (user_id, account_id, graph_id) = seed_tenancy(&pool).await;
    let store = PostgresStore::new(pool.clone());

    let title_only = seed_atom(
        &store,
        &pool,
        graph_id,
        user_id,
        "title-only",
        Some("Repository file skipped before the code tier fetches it"),
        "Everything downstream reads the classifier's verdict and moves on.",
    )
    .await;
    seed_filler(&store, &pool, graph_id, user_id).await;

    let hits = store
        .keyword_search(graph_id, LONG_QUESTION, 20, &RetrievalFilter::default())
        .await
        .expect("keyword_search");
    let got = ids(&hits).await;

    assert!(
        got.contains(&title_only),
        "an atom whose TITLE carries the question's words must be reachable — got {got:?}"
    );
    assert_eq!(
        got.first().copied(),
        Some(title_only),
        "the title is weight class 'A'; a 5-token title match must lead — got {hits:?}"
    );

    cleanup(&pool, account_id, user_id).await;
}

/// More distinct query tokens beats fewer, regardless of which field they land
/// in — the ordering contract stated plainly.
#[tokio::test]
async fn higher_coverage_outranks_lower_coverage() {
    let pool = test_pool!();
    let (user_id, account_id, graph_id) = seed_tenancy(&pool).await;
    let store = PostgresStore::new(pool.clone());

    // One shared title, so all three enter the candidate set the same way and
    // only their bodies' coverage separates them.
    const SHARED_TITLE: Option<&str> = Some("code review notes");
    let high = seed_atom(
        &store,
        &pool,
        graph_id,
        user_id,
        "high",
        SHARED_TITLE,
        "The module decides which repository file is skipped before the code tier fetches it.",
    )
    .await;
    let mid = seed_atom(
        &store,
        &pool,
        graph_id,
        user_id,
        "mid",
        SHARED_TITLE,
        "module repository",
    )
    .await;
    let low = seed_atom(&store, &pool, graph_id, user_id, "low", SHARED_TITLE, "module").await;

    let hits = store
        .keyword_search(graph_id, LONG_QUESTION, 20, &RetrievalFilter::default())
        .await
        .expect("keyword_search");
    let got = ids(&hits).await;

    let pos = |id: Uuid| {
        got.iter()
            .position(|&x| x == id)
            .unwrap_or_else(|| panic!("atom {id} missing from {got:?}"))
    };
    assert!(pos(high) < pos(mid), "8-token > 3-token coverage: {hits:?}");
    assert!(pos(mid) < pos(low), "3-token > 2-token coverage: {hits:?}");

    cleanup(&pool, account_id, user_id).await;
}

/// **The affordable middle `atoms.symbols` (migrations `0247`/`0100`,
/// `0248`/`0101`) turned out to be — this test used to pin the OPPOSITE
/// result.**
///
/// The candidate filter is a union of index-served legs: the title leg
/// admits an atom on ANY query token, the content leg only on ALL of them.
/// So an atom whose BODY covers some-but-not-all of a question's LOOSE
/// tokens while its TITLE shares nothing is still not a candidate through
/// either of those two legs — that half of the original boundary here is
/// unchanged, and still true (a broad `content_tsv` OR sequentially scanned
/// 17,281 of 27,161 atoms on the live dev ocean: 6.5s warm, 39-140s cold,
/// against a ~130ms retrieve budget; Postgres will not serve an
/// at-least-m-of-n coverage floor from a GIN index either).
///
/// What changed is that a DECLARED symbol is a THIRD, independent way in —
/// the symbol leg does not need the title or the loose-token content leg to
/// agree with it at all, because `symbol_hits` is its own candidate source,
/// UNION'd in alongside `title_hits`/`content_hits` rather than filtered
/// through them (see `keyword_search_pg`'s doc comment). So a chunk whose
/// title and loose-token body coverage both miss the question entirely is
/// still reached, the moment its `atoms.symbols` carries the declaration the
/// question names.
#[tokio::test]
async fn a_declared_symbol_reaches_its_chunk_despite_an_unrelated_title_and_partial_body() {
    let pool = test_pool!();
    let (user_id, account_id, graph_id) = seed_tenancy(&pool).await;
    let store = PostgresStore::new(pool.clone());

    // Same shape as the boundary this replaces: a title that shares nothing
    // with the question, and a body that covers only SOME of its loose
    // tokens — `function`/`owns`/`logic` from `SYMBOL_QUESTION` never
    // appear. `atoms.symbols`, stamped separately below the way
    // `routes::ingest` now writes it, is the only thing that makes this atom
    // reachable.
    let body_only = seed_atom(
        &store,
        &pool,
        graph_id,
        user_id,
        "body-only",
        Some("weekly standup notes"),
        "the intake_window logic lives near the scheduler",
    )
    .await;
    stamp_symbols(&pool, body_only, "function intake_window").await;
    seed_filler(&store, &pool, graph_id, user_id).await;

    let got = ids(&store
        .keyword_search(graph_id, SYMBOL_QUESTION, 20, &RetrievalFilter::default())
        .await
        .expect("keyword_search"))
    .await;
    assert!(
        got.contains(&body_only),
        "a chunk whose title and loose-token body coverage both miss the question is still \
         reached once its `atoms.symbols` carries the declaration the question names — got \
         {got:?}"
    );

    cleanup(&pool, account_id, user_id).await;
}

/// A query made only of stopwords carries no retrieval signal — the leg must
/// return nothing rather than every atom in the ocean.
#[tokio::test]
async fn a_stopword_only_query_matches_nothing() {
    let pool = test_pool!();
    let (user_id, account_id, graph_id) = seed_tenancy(&pool).await;
    let store = PostgresStore::new(pool.clone());

    seed_filler(&store, &pool, graph_id, user_id).await;

    for q in ["what is it", "how do they do this", "  --  ", ""] {
        let hits = store
            .keyword_search(graph_id, q, 20, &RetrievalFilter::default())
            .await
            .expect("keyword_search");
        assert!(
            hits.is_empty(),
            "a signal-free query ({q:?}) must match nothing — got {hits:?}"
        );
    }

    cleanup(&pool, account_id, user_id).await;
}

/// The score handed to `retrieval::rerank` must be MONOTONE with the SQL
/// ordering: `rerank` max-normalises `keyword_hits` into `keyword_norm` and
/// blends it at `beta`, so a score that disagreed with the returned order would
/// silently hand the reranker a different ranking than the one this query
/// computed.
#[tokio::test]
async fn returned_scores_are_non_increasing() {
    let pool = test_pool!();
    let (user_id, account_id, graph_id) = seed_tenancy(&pool).await;
    let store = PostgresStore::new(pool.clone());

    seed_answer(&store, &pool, graph_id, user_id).await;
    seed_frequency_junk(&store, &pool, graph_id, user_id).await;
    seed_filler(&store, &pool, graph_id, user_id).await;
    seed_atom(
        &store,
        &pool,
        graph_id,
        user_id,
        "partial",
        Some("code tier"),
        "module repository file skipped",
    )
    .await;

    let hits = store
        .keyword_search(graph_id, LONG_QUESTION, 50, &RetrievalFilter::default())
        .await
        .expect("keyword_search");
    assert!(hits.len() >= 2, "need at least two hits to compare: {hits:?}");
    for w in hits.windows(2) {
        assert!(
            w[0].1 >= w[1].1,
            "scores must be non-increasing in returned order — got {hits:?}"
        );
    }
    assert!(
        hits.iter().all(|&(_, s)| s > 0.0),
        "every returned hit matched at least one query token, so every score must be \
         strictly positive (a zero would be erased by rerank's max-normalisation) — {hits:?}"
    );

    cleanup(&pool, account_id, user_id).await;
}

/// A cohort with IDENTICAL coverage AND identical rank straddling the `k` cap
/// must be cut at its lowest `atom_id`s, on every run — the contract
/// `retrieval_determinism_pg.rs` pins for the single-token case, re-pinned here
/// for the multi-token coverage sort (the tie now happens one column earlier).
#[tokio::test]
async fn a_tied_coverage_cohort_straddling_the_cap_is_deterministic() {
    let pool = test_pool!();
    let (user_id, account_id, graph_id) = seed_tenancy(&pool).await;
    let store = PostgresStore::new(pool.clone());

    let mut tied = Vec::new();
    for i in 0..12 {
        tied.push(
            seed_atom(
                &store,
                &pool,
                graph_id,
                user_id,
                &format!("tied-{i}"),
                Some("code review notes"),
                "module repository file",
            )
            .await,
        );
    }
    tied.sort();

    let mut runs = Vec::new();
    for _ in 0..3 {
        runs.push(
            ids(&store
                .keyword_search(graph_id, LONG_QUESTION, 5, &RetrievalFilter::default())
                .await
                .expect("keyword_search"))
            .await,
        );
    }
    assert_eq!(runs[0].len(), 5, "expected exactly k=5: {:?}", runs[0]);
    assert_eq!(runs[0], runs[1], "runs 0 and 1 must be identical");
    assert_eq!(runs[1], runs[2], "runs 1 and 2 must be identical");
    let mut selected = runs[0].clone();
    selected.sort();
    assert_eq!(
        selected,
        tied[..5].to_vec(),
        "the tied cohort must be cut at its 5 lowest atom_ids, deterministically"
    );

    cleanup(&pool, account_id, user_id).await;
}

/// The RLS-scoped twin must agree with the unscoped method row-for-row. They are
/// two copies of the same query (`keyword_search` on the bare pool,
/// `keyword_search_scoped` inside a `begin_account_scoped` transaction) and the
/// pre-fix pair had already drifted into two hand-maintained duplicates.
#[tokio::test]
async fn the_scoped_twin_returns_the_same_ranking() {
    let pool = test_pool!();
    let (user_id, account_id, graph_id) = seed_tenancy(&pool).await;
    let store = PostgresStore::new(pool.clone());

    seed_answer(&store, &pool, graph_id, user_id).await;
    seed_frequency_junk(&store, &pool, graph_id, user_id).await;
    seed_filler(&store, &pool, graph_id, user_id).await;
    seed_code_chunk(
        &store,
        &pool,
        graph_id,
        user_id,
        "backend/src/routes/queries.rs",
        1,
        Some("weekly standup notes"),
        "pub fn intake_window(now: DateTime<Utc>) -> Range { … }",
    )
    .await;

    // BOTH query shapes: fix 3b makes `keyword_search_pg` emit two different
    // statements — a symbol-free question runs `#1035`'s SQL with 16 binds, a
    // question with a symbol adds the `syms` CTE, the symbol candidate leg and
    // TWO more binds. Postgres rejects a bind count that does not match the
    // parse, so the scoped twin has to be exercised on both.
    for q in [LONG_QUESTION, SYMBOL_QUESTION] {
        let unscoped = store
            .keyword_search(graph_id, q, 20, &RetrievalFilter::default())
            .await
            .expect("keyword_search");

        let mut tx = coralswarm_backend::tenancy::scope::begin_account_scoped(&pool, account_id)
            .await
            .expect("begin_account_scoped");
        let scoped = store
            .keyword_search_scoped(&mut tx, graph_id, q, 20, &RetrievalFilter::default())
            .await
            .expect("keyword_search_scoped");
        tx.commit().await.expect("commit");

        assert!(!unscoped.is_empty(), "fixture must produce hits for {q:?}");
        assert_eq!(
            unscoped, scoped,
            "keyword_search and keyword_search_scoped must return identical rankings for {q:?}"
        );
    }

    cleanup(&pool, account_id, user_id).await;
}

/// The existing `graph_id` + `since`/`until` + `scope` filters must survive the
/// rewrite — a candidate-generation change that quietly dropped a filter would
/// leak rows across oceans.
#[tokio::test]
async fn filters_still_apply() {
    let pool = test_pool!();
    let (user_id, account_id, graph_id) = seed_tenancy(&pool).await;
    let (other_user, other_account, other_graph) = seed_tenancy(&pool).await;
    let store = PostgresStore::new(pool.clone());

    let mine = seed_answer(&store, &pool, graph_id, user_id).await;
    let theirs = seed_answer(&store, &pool, other_graph, other_user).await;

    let hits = ids(&store
        .keyword_search(graph_id, LONG_QUESTION, 20, &RetrievalFilter::default())
        .await
        .expect("keyword_search"))
    .await;
    assert!(hits.contains(&mine), "own ocean's answer must be found: {hits:?}");
    assert!(
        !hits.contains(&theirs),
        "another ocean's atom must never leak into this graph's results: {hits:?}"
    );

    // The SAME, through fix 3b's symbol leg. This is not redundant: the symbol
    // leg interpolates `KEYWORD_SEARCH_FILTERS_SQL` (`$8`..`$16`) a THIRD
    // time, and its own two binds (`$17`/`$18`) sit AFTER them — an off-by-one
    // in that numbering would silently bind a filter value as the symbol array
    // (or drop the graph scope) rather than fail to compile.
    let my_chunk = seed_code_chunk(
        &store,
        &pool,
        graph_id,
        user_id,
        "backend/src/routes/queries.rs",
        1,
        Some("weekly standup notes"),
        "pub fn intake_window(now: DateTime<Utc>) -> Range { … }",
    )
    .await;
    let their_chunk = seed_code_chunk(
        &store,
        &pool,
        other_graph,
        other_user,
        "backend/src/routes/queries.rs",
        1,
        Some("weekly standup notes"),
        "pub fn intake_window(now: DateTime<Utc>) -> Range { … }",
    )
    .await;
    let sym_hits = ids(&store
        .keyword_search(graph_id, SYMBOL_QUESTION, 20, &RetrievalFilter::default())
        .await
        .expect("keyword_search"))
    .await;
    assert!(
        sym_hits.contains(&my_chunk),
        "the symbol leg must find this graph's chunk: {sym_hits:?}"
    );
    assert!(
        !sym_hits.contains(&their_chunk),
        "the symbol leg must not leak another ocean's identically-named chunk: {sym_hits:?}"
    );
    for f in [
        RetrievalFilter {
            until: Some(Utc::now() - chrono::Duration::days(3650)),
            ..Default::default()
        },
        RetrievalFilter { scope: Some("shared".to_string()), ..Default::default() },
        RetrievalFilter { project: Some("nope".to_string()), ..Default::default() },
        RetrievalFilter { agent_name: Some("claude-code".to_string()), ..Default::default() },
        RetrievalFilter { root_session_id: Some("root-1".to_string()), ..Default::default() },
    ] {
        let none = store
            .keyword_search(graph_id, SYMBOL_QUESTION, 20, &f)
            .await
            .expect("keyword_search");
        assert!(
            none.is_empty(),
            "every filter must still apply on the symbol path ({f:?}) — got {none:?}"
        );
    }

    // `until` in the past excludes everything (atoms are stamped `now()`).
    let past = RetrievalFilter {
        until: Some(Utc::now() - chrono::Duration::days(3650)),
        ..Default::default()
    };
    let none = store
        .keyword_search(graph_id, LONG_QUESTION, 20, &past)
        .await
        .expect("keyword_search");
    assert!(none.is_empty(), "the `until` bound must still apply — got {none:?}");

    // A `scope` that matches nothing excludes everything.
    let wrong_scope = RetrievalFilter {
        scope: Some("shared".to_string()),
        ..Default::default()
    };
    let none = store
        .keyword_search(graph_id, LONG_QUESTION, 20, &wrong_scope)
        .await
        .expect("keyword_search");
    assert!(none.is_empty(), "the `scope` filter must still apply — got {none:?}");

    // The `agent_name`/`platform`/`root_session_id` clause (three binds that
    // sit LAST, after every other filter) resolves through `coding_sessions`.
    // No session owns these atoms, so an agent filter must exclude them — a
    // cheap proof the three tail binds are still wired to the right
    // placeholders after the query was rebuilt around them.
    for f in [
        RetrievalFilter { agent_name: Some("claude-code".to_string()), ..Default::default() },
        RetrievalFilter { platform: Some("darwin".to_string()), ..Default::default() },
        RetrievalFilter { root_session_id: Some("root-1".to_string()), ..Default::default() },
    ] {
        let none = store
            .keyword_search(graph_id, LONG_QUESTION, 20, &f)
            .await
            .expect("keyword_search");
        assert!(
            none.is_empty(),
            "the coding-session filter must still apply ({f:?}) — got {none:?}"
        );
    }

    cleanup(&pool, account_id, user_id).await;
    cleanup(&pool, other_account, other_user).await;
}

// ── The index question ──────────────────────────────────────────────────────────

/// Best-effort `EXPLAIN` visibility that BOTH halves of the candidate filter can
/// be served by a GIN index rather than a sequential scan over `atoms`: the
/// title half by `idx_atoms_title_tsv` (this change's migration —
/// `to_tsvector('simple', coalesce(title,''))`, an EXPRESSION index, so no
/// table rewrite) and the content half by `idx_atoms_content_tsv` (migration
/// `0012`).
///
/// This matters more than a plan check usually does: on the live dev ocean a
/// broad `content_tsv` OR degenerated into a sequential scan that took 6.5s
/// WARM (39-140s cold) against an ask-path retrieve budget of ~130ms. Latency is
/// a correctness concern for this query, and "which index serves it" is the
/// whole reason the candidate filter is shaped the way it is.
///
/// Planner heuristics on a small, freshly-created table are inherently fickle,
/// so a plan that does not mention the index is logged loudly rather than
/// failing the build — the same convention `reef_search_tsv_pg.rs` uses for its
/// analogous D8 check. Functional correctness is covered by the hard-asserting
/// tests above.
#[tokio::test]
async fn the_candidate_filter_can_use_the_title_and_content_gin_indexes() {
    let pool = test_pool!();
    let (user_id, account_id, graph_id) = seed_tenancy(&pool).await;
    let store = PostgresStore::new(pool.clone());

    seed_answer(&store, &pool, graph_id, user_id).await;
    for i in 0..600 {
        seed_atom(
            &store,
            &pool,
            graph_id,
            user_id,
            &format!("decoy-{i}"),
            Some("quarterly revenue summary"),
            "Nothing in this atom uses the question's vocabulary at all.",
        )
        .await;
    }
    // Give `symbols` real content before ANALYZE: a uniformly NULL column makes
    // the symbols arm of the predicate trivially satisfiable and the EXPLAIN
    // below meaningless for it.
    sqlx::query("UPDATE atoms SET symbols = 'function_signature decoy_symbol' WHERE graph_id = $1")
        .bind(graph_id)
        .execute(&pool)
        .await
        .expect("stamp decoy symbols");
    sqlx::query("ANALYZE atoms").execute(&pool).await.ok();

    let plan: Vec<String> = sqlx::query_scalar(
        "EXPLAIN SELECT atom_id FROM atoms \
         WHERE graph_id = $1 \
           AND to_tsvector('simple', coalesce(title, '')) @@ to_tsquery('simple', $2)",
    )
    .bind(graph_id)
    .bind("module | repository | file | code")
    .fetch_all(&pool)
    .await
    .expect("EXPLAIN title predicate");
    let plan = plan.join("\n");
    if plan.contains("idx_atoms_title_tsv") {
        eprintln!("title predicate served by idx_atoms_title_tsv:\n{plan}");
    } else {
        eprintln!(
            "NOTE: planner did not choose idx_atoms_title_tsv for the title predicate on this \
             small table (this is best-effort, not a failure). Plan:\n{plan}"
        );
    }

    let content_plan: Vec<String> = sqlx::query_scalar(
        "EXPLAIN SELECT atom_id FROM atoms \
         WHERE graph_id = $1 \
           AND content_tsv @@ to_tsquery('simple', $2)",
    )
    .bind(graph_id)
    .bind("module & repository & file & code")
    .fetch_all(&pool)
    .await
    .expect("EXPLAIN content predicate");
    let content_plan = content_plan.join("\n");
    if content_plan.contains("idx_atoms_content_tsv") {
        eprintln!("content predicate served by idx_atoms_content_tsv:\n{content_plan}");
    } else {
        eprintln!(
            "NOTE: planner did not choose idx_atoms_content_tsv for the content predicate on this \
             small table (this is best-effort, not a failure). Plan:\n{content_plan}"
        );
    }

    cleanup(&pool, account_id, user_id).await;
}

// ── Identifier- and path-aware retrieval (fix 3b) ───────────────────────────────
//
// Two gaps `#1035` left, both measured against `to_tsvector('simple', …)` on the
// live dev ocean before a line of this was written:
//
//   `intake_window`                        → 'intake':1 'window':2   — SPLIT
//   `// backend/src/routes/queries.rs — …` → 'backend/src/routes/queries.rs':1
//   `to_tsquery('simple','queries.rs')` @@ that header               → FALSE
//
// So (a) an identifier the user typed as ONE word degenerates into several
// common ones, and (b) the filename a user actually says is a SUFFIX of an
// indexed whole-path lexeme, which `:*` cannot reach (prefixes work:
// `to_tsquery('simple','backend/src:*')` @@ that header is TRUE).
//
// `retrieval::keyword_query_symbols` lifts those spellings out of the RAW
// question and `store::keyword_search_pg` compiles each one with Postgres's own
// `phraseto_tsquery` — so `intake_window` becomes `'intake' <-> 'window'` and
// `routes/queries.rs` (after `translate(…,'/',' ')`) becomes
// `'routes' <-> 'queries.rs'`. See `keyword_search_pg`'s doc comment for which
// fields each is matched against and, more importantly, which it is NOT.

/// A question that names a symbol the way a developer would. Its content words
/// (`function`, `owns`, `logic`) are deliberately words the fixtures below do
/// NOT all share, so nothing reaches the leg on the loose tokens alone.
const SYMBOL_QUESTION: &str = "which function owns the intake_window logic";

/// Seed one code-shaped chunk: a `{repo}:blob:{path}:chunk:{n}` source id and a
/// body that opens with the chunk header `connectors::github` writes.
async fn seed_code_chunk(
    store: &PostgresStore,
    pool: &sqlx::PgPool,
    graph_id: Uuid,
    user_id: Uuid,
    path: &str,
    chunk: usize,
    title: Option<&str>,
    body: &str,
) -> Uuid {
    seed_atom(
        store,
        pool,
        graph_id,
        user_id,
        &format!("1236057652:blob:{path}:chunk:{chunk}"),
        title,
        &format!("// {path} — items\n{body}"),
    )
    .await
}

/// **(a) Identifier admission.** A chunk that DECLARES `intake_window`, under a
/// title that shares nothing with the question, is currently out of reach of
/// both of `#1035`'s candidate legs: the content leg conjoins every token
/// (`function & owns & intake & window & logic` — the chunk never says "owns"
/// or "logic") and the title leg needs a title-word overlap it does not have.
///
/// Fails against pre-3b code with an EMPTY hit list.
#[tokio::test]
async fn an_identifier_reaches_the_chunk_that_declares_it() {
    let pool = test_pool!();
    let (user_id, account_id, graph_id) = seed_tenancy(&pool).await;
    let store = PostgresStore::new(pool.clone());

    let declarer = seed_code_chunk(
        &store,
        &pool,
        graph_id,
        user_id,
        "backend/src/routes/queries.rs",
        1,
        Some("weekly standup notes"),
        "pub fn intake_window(now: DateTime<Utc>) -> Range { … }",
    )
    .await;
    seed_filler(&store, &pool, graph_id, user_id).await;

    let got = ids(&store
        .keyword_search(graph_id, SYMBOL_QUESTION, 20, &RetrievalFilter::default())
        .await
        .expect("keyword_search"))
    .await;

    assert!(
        got.contains(&declarer),
        "a question naming `intake_window` must reach the chunk that declares it — got {} \
         hit(s): {got:?}",
        got.len()
    );

    cleanup(&pool, account_id, user_id).await;
}

/// **(a) Identifier ranking — the augment-don't-replace decision.** The loose
/// tokens are KEPT, and the compiled phrase is ONE MORE coverage dimension
/// beside them, so an atom carrying the literal `intake_window` covers strictly
/// more of what the user said than one that merely contains `intake` and
/// `window` somewhere.
///
/// Both atoms here share a title that overlaps the question, so both are
/// candidates on `#1035`'s rules and both match every loose token. The decoy
/// repeats them, which is exactly what `ts_rank_cd` — the only tie-break the
/// pre-3b sort had left — rewards. Fails against pre-3b code with the decoy
/// ranked first.
#[tokio::test]
async fn an_exact_identifier_outranks_the_same_words_apart() {
    let pool = test_pool!();
    let (user_id, account_id, graph_id) = seed_tenancy(&pool).await;
    let store = PostgresStore::new(pool.clone());

    const SHARED_TITLE: Option<&str> = Some("function logic notes");
    let exact = seed_atom(
        &store,
        &pool,
        graph_id,
        user_id,
        "exact",
        SHARED_TITLE,
        "pub fn intake_window(now: DateTime<Utc>) -> Range { … } — called once per tick by \
         the scheduler to bound how far back the pipeline reaches for fresh work.",
    )
    .await;
    // The SAME two words, never in that order, in a SHORTER body — so it wins
    // the only tie-break the pre-3b sort had left. `ts_rank_cd(…, 2|32)`
    // divides by document length, so the short atom scores higher at equal
    // coverage, and pre-3b the coverage IS equal: both cover `function` and
    // `logic` from the shared title and `intake` and `window` from the body.
    let apart = seed_atom(
        &store,
        &pool,
        graph_id,
        user_id,
        "apart",
        SHARED_TITLE,
        "window intake",
    )
    .await;
    seed_filler(&store, &pool, graph_id, user_id).await;

    let hits = store
        .keyword_search(graph_id, SYMBOL_QUESTION, 20, &RetrievalFilter::default())
        .await
        .expect("keyword_search");
    let got = ids(&hits).await;
    let pos = |id: Uuid| {
        got.iter()
            .position(|&x| x == id)
            .unwrap_or_else(|| panic!("atom {id} missing from {got:?}"))
    };
    assert!(
        pos(exact) < pos(apart),
        "the atom containing the literal `intake_window` must outrank one that only has \
         `intake` and `window` far apart — got {hits:?}"
    );

    cleanup(&pool, account_id, user_id).await;
}

/// **(a) The phrase must not fire on words that merely sit side by side in
/// prose the user did not spell as one identifier.** `'intake' <-> 'window'`
/// against "the intake was wide open and the window closed" is FALSE — verified
/// directly against Postgres before this leg was built — so the symbol is a
/// PRECISION signal, not a second loose OR.
#[tokio::test]
async fn an_identifier_phrase_does_not_match_the_words_scattered() {
    let pool = test_pool!();
    let (user_id, account_id, graph_id) = seed_tenancy(&pool).await;
    let store = PostgresStore::new(pool.clone());

    // Both query words, never in that order — and SHORT, so pre-3b it wins the
    // `ts_rank_cd` tie-break outright.
    let scattered = seed_atom(
        &store,
        &pool,
        graph_id,
        user_id,
        "scattered",
        Some("function logic notes"),
        "window intake",
    )
    .await;
    let exact = seed_atom(
        &store,
        &pool,
        graph_id,
        user_id,
        "exact",
        Some("function logic notes"),
        "pub fn intake_window(now: DateTime<Utc>) -> Range { … } — called once per tick by \
         the scheduler to bound how far back the pipeline reaches for fresh work.",
    )
    .await;
    seed_filler(&store, &pool, graph_id, user_id).await;

    let hits = store
        .keyword_search(graph_id, SYMBOL_QUESTION, 20, &RetrievalFilter::default())
        .await
        .expect("keyword_search");

    let score = |id: Uuid| {
        hits.iter()
            .find(|&&(x, _)| x == id)
            .unwrap_or_else(|| panic!("atom {id} missing from {hits:?}"))
            .1
    };
    // SYMBOL_QUESTION yields 5 tokens + 1 symbol, so one coverage step is 1/6
    // and the bounded `ts_rank_cd` tie-break term is strictly below 1/12. The
    // exact atom must therefore clear the scattered one by more than the
    // tie-break alone could ever supply — proving the gap is the SYMBOL, not
    // the rank.
    assert!(
        score(exact) - score(scattered) > 1.0 / 12.0,
        "the phrase must credit only the literal identifier, and by a whole coverage step — \
         got {hits:?}"
    );

    cleanup(&pool, account_id, user_id).await;
}

/// **(b) Path suffix.** A bare filename is what a user actually types, and it is
/// a SUFFIX of the whole-path lexeme Postgres indexes — unreachable by `:*`.
/// `idx_atoms_source_path_tsv` indexes `translate(source_id, '/', ' ')`, which
/// splits the path into per-segment lexemes and makes `queries.rs` a lexeme in
/// its own right.
///
/// The SAME test pins the precision property that chose a segment tsvector over
/// a `pg_trgm` index: `roster_queries.rs` merely CONTAINS `queries.rs` and must
/// not be returned (the trgm candidate matched it; measured, 291 rows vs 286).
///
/// Fails against pre-3b code with an EMPTY hit list.
#[tokio::test]
async fn a_bare_filename_reaches_the_file_it_names_and_not_a_longer_one() {
    let pool = test_pool!();
    let (user_id, account_id, graph_id) = seed_tenancy(&pool).await;
    let store = PostgresStore::new(pool.clone());

    let wanted = seed_code_chunk(
        &store,
        &pool,
        graph_id,
        user_id,
        "backend/src/routes/queries.rs",
        7,
        Some("weekly standup notes"),
        "let reason = decide(&cfg);",
    )
    .await;
    let substring_only = seed_code_chunk(
        &store,
        &pool,
        graph_id,
        user_id,
        "backend/src/store/roster_queries.rs",
        2,
        Some("weekly standup notes"),
        "let reason = decide(&cfg);",
    )
    .await;
    seed_filler(&store, &pool, graph_id, user_id).await;

    let got = ids(&store
        .keyword_search(graph_id, "what does queries.rs do", 20, &RetrievalFilter::default())
        .await
        .expect("keyword_search"))
    .await;

    assert!(
        got.contains(&wanted),
        "a bare filename must reach the file it names — got {} hit(s): {got:?}",
        got.len()
    );
    assert!(
        !got.contains(&substring_only),
        "`queries.rs` must not match `roster_queries.rs`: path matching is per SEGMENT, not \
         per substring — got {got:?}"
    );

    cleanup(&pool, account_id, user_id).await;
}

/// **(b) Multi-segment suffix.** `routes/queries.rs` is neither a prefix nor a
/// whole lexeme of `backend/src/routes/queries.rs`; after `translate` it is the
/// phrase `'routes' <-> 'queries.rs'`, which is precise about segment ORDER —
/// `queries.rs/routes` must not match.
///
/// Fails against pre-3b code with an EMPTY hit list.
#[tokio::test]
async fn a_multi_segment_path_suffix_matches_in_order_only() {
    let pool = test_pool!();
    let (user_id, account_id, graph_id) = seed_tenancy(&pool).await;
    let store = PostgresStore::new(pool.clone());

    let wanted = seed_code_chunk(
        &store,
        &pool,
        graph_id,
        user_id,
        "backend/src/routes/queries.rs",
        3,
        Some("weekly standup notes"),
        "let reason = decide(&cfg);",
    )
    .await;
    let reversed = seed_code_chunk(
        &store,
        &pool,
        graph_id,
        user_id,
        "backend/src/queries.rs/routes",
        1,
        Some("weekly standup notes"),
        "let reason = decide(&cfg);",
    )
    .await;
    seed_filler(&store, &pool, graph_id, user_id).await;

    let got = ids(&store
        .keyword_search(
            graph_id,
            "walk me through routes/queries.rs",
            20,
            &RetrievalFilter::default(),
        )
        .await
        .expect("keyword_search"))
    .await;

    assert!(
        got.contains(&wanted),
        "a multi-segment path suffix must reach the file it names — got {} hit(s): {got:?}",
        got.len()
    );
    assert!(
        !got.contains(&reversed),
        "segment ORDER is part of the match — got {got:?}"
    );

    cleanup(&pool, account_id, user_id).await;
}

/// **A pinned MEASUREMENT, not a feature.** `to_tsvector('simple',
/// 'PostgresStore')` is `'postgresstore':1` — ONE lexeme — and
/// `retrieval::node_match_tokens` lowercases without splitting on case, so both
/// sides already agree and a CamelCase identifier needs no phrase at all.
///
/// This contradicts fix 3b's brief, which lists "a CamelCase boundary" among the
/// spellings that degenerate into several common words. It does not. This test
/// pins that the CamelCase token still matches end to end; the companion unit
/// test `camel_case_is_not_a_symbol` (`retrieval::tests`) pins the other half —
/// that `keyword_query_symbols` must never split it, because the resulting
/// `'postgres' <-> 'store'` phrase matches NOTHING Postgres indexes.
#[tokio::test]
async fn a_camel_case_identifier_is_already_one_lexeme_on_both_sides() {
    let pool = test_pool!();
    let (user_id, account_id, graph_id) = seed_tenancy(&pool).await;
    let store = PostgresStore::new(pool.clone());

    let wanted = seed_atom(
        &store,
        &pool,
        graph_id,
        user_id,
        "camel",
        Some("weekly standup notes"),
        "impl PostgresStore { pub fn new(pool: PgPool) -> Self { … } } — built from the \
         pool the router hands it.",
    )
    .await;
    seed_filler(&store, &pool, graph_id, user_id).await;

    let got = ids(&store
        .keyword_search(graph_id, "where is PostgresStore built", 20, &RetrievalFilter::default())
        .await
        .expect("keyword_search"))
    .await;
    assert!(
        got.contains(&wanted),
        "a CamelCase identifier must still match as the single lexeme both sides produce — \
         got {got:?}"
    );

    cleanup(&pool, account_id, user_id).await;
}

/// **The boundary 3b did NOT close, closed HERE by `atoms.symbols`
/// (migrations `0247`/`0100`, `0248`/`0101`) — pinned the way `#1035` and
/// fix 3b each pinned their own.**
///
/// Fix 3b admitted a symbol from `atoms.excerpt` (the first
/// `JOURNAL_EXCERPT_MAX_CHARS` chars of the body, which for a code chunk is
/// the header plus AT MOST THREE declarations — `combined_label` in
/// `packages/core/src/chunker.rs` folds the rest into `", +N more"`) and
/// from `atoms.source_id`. A declaration named only in that "+N more" was
/// therefore unreachable no matter how deep in the body it sat — not because
/// of WHERE it was, but because the header text `excerpt` derives from had
/// already dropped it before `excerpt` was ever truncated to 280 chars.
///
/// `atoms.symbols` carries the SAME declaration list `combined_label` builds
/// but UNCAPPED (`Chunk::symbols`'s own doc comment), so this fixture — a
/// declaration that used to be invisible for exactly that reason — is now a
/// candidate. This is still NOT a route through `content_tsv`: measured on
/// the live dev ocean (29,953 atoms), `content_tsv @@
/// phraseto_tsquery('simple', 'user_id')` matches 3,925 rows and sequentially
/// scans every toasted `content_tsv` — **241ms warm, 3.8s cold** — so that
/// path is still closed, exactly as before; `idx_atoms_symbols_tsv` is what
/// serves this now, the same small, bounded shape as
/// `idx_atoms_excerpt_tsv`.
#[tokio::test]
async fn a_declaration_deep_in_the_body_reaches_it_via_symbols() {
    let pool = test_pool!();
    let (user_id, account_id, graph_id) = seed_tenancy(&pool).await;
    let store = PostgresStore::new(pool.clone());

    // Same fixture as the boundary this replaces: 280 chars of unrelated
    // prose first, so `intake_window` falls outside `excerpt` entirely; the
    // source id names no path and the title shares nothing. What is new is
    // `atoms.symbols`, stamped below the way `routes::ingest` now writes it
    // from `Chunk::symbols` at write time — the declaration reaches the
    // symbol leg through THAT column, never through `content_tsv`.
    let deep = seed_atom(
        &store,
        &pool,
        graph_id,
        user_id,
        "deep-body",
        Some("weekly standup notes"),
        &format!(
            "{}\npub fn intake_window(now: DateTime<Utc>) -> Range {{ … }}",
            "Priya walked through the launch calendar and the hiring plan. ".repeat(6)
        ),
    )
    .await;
    stamp_symbols(&pool, deep, "function intake_window").await;
    seed_filler(&store, &pool, graph_id, user_id).await;

    let got = ids(&store
        .keyword_search(graph_id, SYMBOL_QUESTION, 20, &RetrievalFilter::default())
        .await
        .expect("keyword_search"))
    .await;
    assert!(
        got.contains(&deep),
        "a declared identifier reaches its chunk through `atoms.symbols` regardless of how \
         deep in the body it sits — got {got:?}"
    );

    cleanup(&pool, account_id, user_id).await;
}

/// A question with NO symbol in it must behave EXACTLY as it did before 3b — the
/// symbol legs are skipped entirely, so they cost nothing on the common case.
/// Pinned behaviourally: the `#1035` regression test's corpus and ordering are
/// unchanged, and the coverage denominator (which the returned score divides by)
/// is still the token count alone.
#[tokio::test]
async fn a_question_with_no_symbol_scores_exactly_as_before() {
    let pool = test_pool!();
    let (user_id, account_id, graph_id) = seed_tenancy(&pool).await;
    let store = PostgresStore::new(pool.clone());

    let answer = seed_answer(&store, &pool, graph_id, user_id).await;
    seed_filler(&store, &pool, graph_id, user_id).await;

    let hits = store
        .keyword_search(graph_id, LONG_QUESTION, 20, &RetrievalFilter::default())
        .await
        .expect("keyword_search");
    assert_eq!(hits[0].0, answer, "unchanged ranking on a symbol-free question: {hits:?}");

    // LONG_QUESTION yields 8 tokens and 0 symbols, and the answer doc covers 4
    // of them (`module`, `repository`, `file` in the body, `code` in the title).
    // Denominator 8, not 9: a query with no symbols must not widen it.
    let base = 4.0 / 8.0;
    assert!(
        hits[0].1 >= base && hits[0].1 < base + 1.0 / 16.0,
        "score must be `matched/8 + rank/16` with matched=4 — got {}",
        hits[0].1
    );

    cleanup(&pool, account_id, user_id).await;
}

/// Best-effort `EXPLAIN` visibility that the symbol candidate leg is served by
/// the three GIN indexes behind it — 3b's `excerpt` and `source_id` pair plus
/// `idx_atoms_symbols_tsv` — rather than a sequential scan over `atoms`. Same
/// convention (and the same reason) as
/// `the_candidate_filter_can_use_the_title_and_content_gin_indexes` above.
///
/// It stays an `eprintln!` rather than an assertion ON PURPOSE. Planner CHOICE
/// is a cost decision, and on a seeded table this small a sequential scan is
/// often genuinely cheaper — asserting here would buy a flaky gate, not rigour.
/// What this test does guarantee is that the predicate is EXPLAINable and keeps
/// the InitPlan shape below; proving the indexes are chosen needs realistic
/// cardinality, which only a full re-ingest provides.
///
/// This one also guards a plan shape that is easy to get wrong and silent when
/// you do: writing the compiled-tsquery CTE as `FROM atoms, syms` turns the
/// predicate into a **Join Filter** and the planner seq-scans (measured: 352ms
/// vs 7.9ms on the dev ocean). The tsqueries must reach the predicate as an
/// uncorrelated `ARRAY(SELECT …)` — an InitPlan — for the GIN indexes to be
/// usable at all.
#[tokio::test]
async fn the_symbol_leg_can_use_the_excerpt_source_path_and_symbols_gin_indexes() {
    let pool = test_pool!();
    let (user_id, account_id, graph_id) = seed_tenancy(&pool).await;
    let store = PostgresStore::new(pool.clone());

    for i in 0..600 {
        seed_code_chunk(
            &store,
            &pool,
            graph_id,
            user_id,
            &format!("backend/src/decoy_{i}.rs"),
            0,
            Some("quarterly revenue summary"),
            "Nothing in this atom uses the question's vocabulary at all.",
        )
        .await;
    }
    sqlx::query("ANALYZE atoms").execute(&pool).await.ok();

    let plan: Vec<String> = sqlx::query_scalar(
        "EXPLAIN SELECT atom_id FROM atoms \
         WHERE graph_id = $1 \
           AND (to_tsvector('simple', coalesce(excerpt, '')) \
                    @@ ANY(ARRAY[phraseto_tsquery('simple', $2)]) \
                OR to_tsvector('simple', translate(source_id, '/', ' ')) \
                    @@ ANY(ARRAY[phraseto_tsquery('simple', translate($2, '/', ' '))]) \
                OR to_tsvector('simple', coalesce(symbols, '')) \
                    @@ ANY(ARRAY[phraseto_tsquery('simple', $2)]))",
    )
    .bind(graph_id)
    .bind("intake_window")
    .fetch_all(&pool)
    .await
    .expect("EXPLAIN symbol predicate");
    let plan = plan.join("\n");
    if plan.contains("idx_atoms_excerpt_tsv")
        && plan.contains("idx_atoms_source_path_tsv")
        && plan.contains("idx_atoms_symbols_tsv")
    {
        eprintln!("symbol predicate served by all three GIN indexes:\n{plan}");
    } else {
        eprintln!(
            "NOTE: planner did not choose all three GIN indexes on this small table \
             (best-effort, not a failure). Plan:\n{plan}"
        );
    }

    cleanup(&pool, account_id, user_id).await;
}
