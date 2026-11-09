//! Integration tests for the org-tenancy OPS-4 "final seven" `_scoped` twins
//! (the last gap in the `Store` trait's RLS surface): `vector_search`,
//! `keyword_search`, `atom_content_refs`, `list_session_atoms`,
//! `count_session_atoms`, `relabel_summary_owner`, and
//! `list_shared_session_corals`.
//!
//! Same rigor as every prior chunk (`store_scoped_atoms_nodes_graphs_pg.rs`,
//! `store_scoped_chunk2_pg.rs`, `store_scoped_chunk3_pg.rs`): drives
//! `PostgresStore` directly as a real `coralswarm_app` login, via the shared
//! `tests/support/rls_probe.rs` two-account harness (PR #286). Three proofs
//! per method:
//!
//!   1. scoped to account A, the query finds exactly A's own seeded rows;
//!   2. scoped to account A but aimed at B's graph/atom ids, it returns
//!      nothing (never B's real data, never an error);
//!   3. with `app.account_id` never set, the same call HARD-ERRORS (the
//!      strict `current_setting` RLS predicate fails closed).
//!
//! `vector_search_scoped` additionally gets an `EXPLAIN`-adjacent smoke
//! check: the seeded row is found via a real `pgvector` nearest-neighbor
//! query, not just a `graph_id` filter coincidence (a zero-distance probe
//! vector identical to what was seeded).
//!
//! Negative control (this file's own honesty check, matching the discipline
//! PR #299 established after a suite went green for the wrong reason): see
//! `keyword_search_hard_error_is_caused_by_rls_not_a_broken_query` below —
//! it re-runs the EXACT same `keyword_search_scoped` SQL via the owner
//! (BYPASSRLS) pool, with no `app.account_id` GUC set at all, and asserts it
//! STILL SUCCEEDS. That proves the "hard error" asserted elsewhere in this
//! file is specifically an RLS-policy effect of the non-BYPASSRLS
//! `coralswarm_app` role — not a coincidentally-broken query that would
//! "hard error" for any caller regardless of scoping (which would make the
//! whole suite vacuously green).
//!
//! Beyond this file, the coordinator's literal negative-control instruction
//! (un-scope one real call site, confirm this suite goes red, restore it)
//! was run manually against `routes::queries::scoped_keyword_search` during
//! development — see this PR's description for the before/after run.
//!
//! Env-gated, like every other `*_pg.rs` test here:
//!
//!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test store_scoped_final_seven_pg

mod support;

use coralswarm_backend::retrieval::RetrievalFilter;
use coralswarm_backend::store::{PostgresStore, Store};
use pgvector::Vector;
use sqlx::PgPool;
use support::rls_probe::RlsProbe;
use uuid::Uuid;

const EMBED_MODEL_NAME: &str = coralswarm_core::embed::EMBED_MODEL_NAME;

fn probe_vector() -> Vec<f32> {
    // A fixed, arbitrary 384-dim vector — seeded verbatim onto the atom's
    // `embedding` column and reused as the query vector, so a correct HNSW
    // lookup finds it at (near-)zero cosine distance regardless of the
    // embedder actually used in production.
    (0..384).map(|i| (i as f32 * 0.01).sin()).collect()
}

/// Extra per-test fixture rows beyond `AccountFixture` — seeded via the
/// owner/BYPASSRLS pool so seeding itself is never subject to the policies
/// under test.
async fn seed_final_seven_fixture(owner: &PgPool, graph_id: Uuid, atom_id: Uuid, session_id: &str) {
    // ── vector_search_scoped: give the fixture atom a real embedding ───────
    let vec = Vector::from(probe_vector());
    sqlx::query("UPDATE atoms SET embedding = $1, embed_model = $2, embedding_next = $1, embed_model_next = $3 WHERE atom_id = $4")
        .bind(vec)
        .bind(EMBED_MODEL_NAME)
        .bind(coralswarm_core::embed::EMBED_MODEL_CLOUD)
        .bind(atom_id)
        .execute(owner)
        .await
        .expect("seed embedding onto fixture atom");

    // ── keyword_search_scoped: a distinctive tsvector token ─────────────────
    sqlx::query(
        "UPDATE atoms SET content_tsv = to_tsvector('simple', 'quokka expedition briefing') \
         WHERE atom_id = $1",
    )
    .bind(atom_id)
    .execute(owner)
    .await
    .expect("seed content_tsv onto fixture atom");

    // ── list_session_atoms_scoped / count_session_atoms_scoped /
    //    list_shared_session_corals_scoped: a session-shaped coral, matching
    //    the "cc:{session}:" source_id prefix all three match on. ──────────
    sqlx::query(
        "INSERT INTO atoms(graph_id, source_type, source_id, content_blob_key, content_inline, \
                           timestamp) \
         VALUES ($1, 'coding_session', $2, 'blob-key', 'session coral body', now())",
    )
    .bind(graph_id)
    .bind(format!("cc:{session_id}:1"))
    .execute(owner)
    .await
    .expect("insert session coral atom");

    // ── relabel_summary_owner_scoped: a meeting_transcripts row carrying a
    //    `summary.action_items` array with an owner to relabel. ─────────────
    sqlx::query(
        "INSERT INTO meeting_transcripts(graph_id, source_id, raw_inline, summary) \
         VALUES ($1, $2, 'raw transcript text', \
                 jsonb_build_object('action_items', jsonb_build_array( \
                     jsonb_build_object('owner', 'Speaker-1', 'text', 'ship the thing') \
                 )))",
    )
    .bind(graph_id)
    .bind(format!("meeting-{session_id}"))
    .execute(owner)
    .await
    .expect("insert meeting_transcripts row with a relabelable action item");
}

#[tokio::test]
async fn final_seven_scoped_methods_work_correctly_when_account_is_scoped() {
    let Some(probe) = RlsProbe::setup("orgt_final_seven_rw").await else {
        eprintln!(
            "skipping final_seven_scoped_methods_work_correctly_when_account_is_scoped: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };

    let session_id = "final7-sess-a";
    seed_final_seven_fixture(&probe.owner, probe.a.graph_id, probe.a.atom_id, session_id).await;
    // B gets its own independent fixture too, so the cross-account probes
    // below aim at REAL rows that must still not leak.
    seed_final_seven_fixture(&probe.owner, probe.b.graph_id, probe.b.atom_id, "final7-sess-b").await;

    let store = PostgresStore::new(probe.app_pool.clone());
    let filter = RetrievalFilter::default();

    // ── Positive path, scoped to A ──────────────────────────────────────────
    let mut tx = probe.scoped_tx(probe.a.account_id).await;

    let vhits = store
        .vector_search_scoped(&mut tx, probe.a.graph_id, &probe_vector(), 10, &filter)
        .await
        .expect("vector_search_scoped must succeed for A's own graph");
    assert!(
        vhits.iter().any(|(id, _)| *id == probe.a.atom_id),
        "vector_search_scoped must find A's seeded atom via real HNSW lookup — got {vhits:?}"
    );

    let khits = store
        .keyword_search_scoped(&mut tx, probe.a.graph_id, "quokka", 10, &filter)
        .await
        .expect("keyword_search_scoped must succeed for A's own graph");
    assert!(
        khits.iter().any(|(id, _)| *id == probe.a.atom_id),
        "keyword_search_scoped must find A's seeded atom — got {khits:?}"
    );

    let refs = store
        .atom_content_refs_scoped(&mut tx, &[probe.a.atom_id])
        .await
        .expect("atom_content_refs_scoped must succeed for A's own atom");
    assert_eq!(refs.len(), 1, "must resolve exactly A's own atom — got {refs:?}");
    assert_eq!(refs[0].0, probe.a.atom_id);
    assert_eq!(refs[0].3, probe.a.account_id, "resolved account_id must be A's, not leaked");

    let session_atoms = store
        .list_session_atoms_scoped(&mut tx, probe.a.graph_id, session_id, 100)
        .await
        .expect("list_session_atoms_scoped must succeed for A's own session");
    assert_eq!(session_atoms.len(), 1, "must find exactly the one seeded session coral");

    let session_count = store
        .count_session_atoms_scoped(&mut tx, probe.a.graph_id, session_id)
        .await
        .expect("count_session_atoms_scoped must succeed for A's own session");
    assert_eq!(session_count, 1);

    let shared_corals = store
        .list_shared_session_corals_scoped(&mut tx, probe.a.graph_id, session_id)
        .await
        .expect("list_shared_session_corals_scoped must succeed for A's own session");
    assert_eq!(shared_corals.len(), 1);
    assert_eq!(shared_corals[0].content_inline.as_deref(), Some("session coral body"));

    let relabeled = store
        .relabel_summary_owner_scoped(
            &mut tx,
            probe.a.graph_id,
            &format!("meeting-{session_id}"),
            "Speaker-1",
            "Ada Lovelace",
        )
        .await
        .expect("relabel_summary_owner_scoped must succeed for A's own transcript");
    assert_eq!(relabeled, 1, "must relabel exactly the one seeded action item");

    tx.commit().await.expect("commit A's scoped transaction");

    // ── Cross-account isolation, still scoped to A but aimed at B's ids ────
    let mut tx = probe.scoped_tx(probe.a.account_id).await;

    let vhits = store
        .vector_search_scoped(&mut tx, probe.b.graph_id, &probe_vector(), 10, &filter)
        .await
        .expect("vector_search_scoped must not error on a cross-account graph_id");
    assert!(vhits.is_empty(), "scoped to A, B's graph must yield zero vector hits — got {vhits:?}");

    let khits = store
        .keyword_search_scoped(&mut tx, probe.b.graph_id, "quokka", 10, &filter)
        .await
        .expect("keyword_search_scoped must not error on a cross-account graph_id");
    assert!(khits.is_empty(), "scoped to A, B's graph must yield zero keyword hits — got {khits:?}");

    let refs = store
        .atom_content_refs_scoped(&mut tx, &[probe.b.atom_id])
        .await
        .expect("atom_content_refs_scoped must not error on a cross-account atom_id");
    assert!(refs.is_empty(), "scoped to A, B's atom must resolve to nothing — got {refs:?}");

    let session_atoms = store
        .list_session_atoms_scoped(&mut tx, probe.b.graph_id, "final7-sess-b", 100)
        .await
        .expect("list_session_atoms_scoped must not error on a cross-account graph_id");
    assert!(session_atoms.is_empty(), "scoped to A, B's session must yield nothing");

    let session_count = store
        .count_session_atoms_scoped(&mut tx, probe.b.graph_id, "final7-sess-b")
        .await
        .expect("count_session_atoms_scoped must not error on a cross-account graph_id");
    assert_eq!(session_count, 0, "scoped to A, B's session must count zero");

    let shared_corals = store
        .list_shared_session_corals_scoped(&mut tx, probe.b.graph_id, "final7-sess-b")
        .await
        .expect("list_shared_session_corals_scoped must not error on a cross-account graph_id");
    assert!(shared_corals.is_empty(), "scoped to A, B's session corals must yield nothing");

    let relabeled = store
        .relabel_summary_owner_scoped(&mut tx, probe.b.graph_id, "meeting-final7-sess-b", "Speaker-1", "Nope")
        .await
        .expect("relabel_summary_owner_scoped must not error on a cross-account graph_id");
    assert_eq!(relabeled, 0, "scoped to A, B's transcript must relabel zero rows");

    tx.rollback().await.ok();

    probe.teardown().await;
}

/// With `app.account_id` never set on the transaction, all seven scoped
/// calls must HARD-ERROR — the strict `current_setting('app.account_id')`
/// predicate fails closed rather than returning an empty (or worse,
/// unfiltered) result. This is what catches a caller that runs one of these
/// seven RLS-table queries outside `begin_account_scoped` after the OPS-4
/// cutover.
#[tokio::test]
async fn final_seven_scoped_methods_hard_error_when_account_id_is_never_set() {
    let Some(probe) = RlsProbe::setup("orgt_final_seven_backstop").await else {
        eprintln!(
            "skipping final_seven_scoped_methods_hard_error_when_account_id_is_never_set: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };

    let session_id = "final7-backstop-sess";
    seed_final_seven_fixture(&probe.owner, probe.a.graph_id, probe.a.atom_id, session_id).await;

    let store = PostgresStore::new(probe.app_pool.clone());
    let filter = RetrievalFilter::default();

    {
        let mut tx = probe.app_pool.begin().await.expect("begin unscoped tx (app.account_id never set)");
        let result = store.vector_search_scoped(&mut tx, probe.a.graph_id, &probe_vector(), 10, &filter).await;
        assert!(result.is_err(), "vector_search_scoped must hard-error — got {result:?}");
        tx.rollback().await.ok();
    }
    {
        let mut tx = probe.app_pool.begin().await.expect("begin unscoped tx (app.account_id never set)");
        let result = store.keyword_search_scoped(&mut tx, probe.a.graph_id, "quokka", 10, &filter).await;
        assert!(result.is_err(), "keyword_search_scoped must hard-error — got {result:?}");
        tx.rollback().await.ok();
    }
    {
        let mut tx = probe.app_pool.begin().await.expect("begin unscoped tx (app.account_id never set)");
        let result = store.atom_content_refs_scoped(&mut tx, &[probe.a.atom_id]).await;
        assert!(result.is_err(), "atom_content_refs_scoped must hard-error — got {result:?}");
        tx.rollback().await.ok();
    }
    {
        let mut tx = probe.app_pool.begin().await.expect("begin unscoped tx (app.account_id never set)");
        let result = store.list_session_atoms_scoped(&mut tx, probe.a.graph_id, session_id, 100).await;
        assert!(result.is_err(), "list_session_atoms_scoped must hard-error — got {result:?}");
        tx.rollback().await.ok();
    }
    {
        let mut tx = probe.app_pool.begin().await.expect("begin unscoped tx (app.account_id never set)");
        let result = store.count_session_atoms_scoped(&mut tx, probe.a.graph_id, session_id).await;
        assert!(result.is_err(), "count_session_atoms_scoped must hard-error — got {result:?}");
        tx.rollback().await.ok();
    }
    {
        let mut tx = probe.app_pool.begin().await.expect("begin unscoped tx (app.account_id never set)");
        let result = store.list_shared_session_corals_scoped(&mut tx, probe.a.graph_id, session_id).await;
        assert!(result.is_err(), "list_shared_session_corals_scoped must hard-error — got {result:?}");
        tx.rollback().await.ok();
    }
    {
        let mut tx = probe.app_pool.begin().await.expect("begin unscoped tx (app.account_id never set)");
        let result = store
            .relabel_summary_owner_scoped(
                &mut tx,
                probe.a.graph_id,
                &format!("meeting-{session_id}"),
                "Speaker-1",
                "Ada Lovelace",
            )
            .await;
        assert!(result.is_err(), "relabel_summary_owner_scoped must hard-error — got {result:?}");
        tx.rollback().await.ok();
    }

    probe.teardown().await;
}

/// Negative control (PR #299's discipline): proves the "hard error when
/// `app.account_id` is never set" assertions above are catching a real RLS
/// effect, not a coincidentally-broken query that would fail for ANY caller
/// regardless of scoping (which would make this whole suite vacuously
/// green). Runs the exact same `keyword_search_scoped` call, unscoped (no
/// GUC ever set), against the OWNER — BYPASSRLS — pool instead of
/// `coralswarm_app`: it must SUCCEED, proving the SQL itself is fine and the
/// earlier hard-error is specifically the non-BYPASSRLS role's RLS policy
/// enforcement kicking in.
#[tokio::test]
async fn keyword_search_hard_error_is_caused_by_rls_not_a_broken_query() {
    let Some(probe) = RlsProbe::setup("orgt_final_seven_negctrl").await else {
        eprintln!(
            "skipping keyword_search_hard_error_is_caused_by_rls_not_a_broken_query: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };

    seed_final_seven_fixture(&probe.owner, probe.a.graph_id, probe.a.atom_id, "negctrl-sess-a").await;

    let filter = RetrievalFilter::default();

    // Contrast case (already proven above, re-asserted here inline): as
    // `coralswarm_app` with no scope set, the call hard-errors.
    let app_store = PostgresStore::new(probe.app_pool.clone());
    let mut unscoped_app_tx = probe.app_pool.begin().await.expect("begin unscoped app-role tx");
    let app_result = app_store
        .keyword_search_scoped(&mut unscoped_app_tx, probe.a.graph_id, "quokka", 10, &filter)
        .await;
    assert!(
        app_result.is_err(),
        "control contrast: unscoped coralswarm_app must still hard-error — got {app_result:?}"
    );
    unscoped_app_tx.rollback().await.ok();

    // The negative control itself: the SAME query, unscoped, via the owner
    // (BYPASSRLS) pool must SUCCEED and find A's real row — proving the SQL
    // shape is correct and the app-role failure above is an RLS effect, not
    // a broken statement that would "hard error" unconditionally and make
    // every assertion in this file meaningless.
    let owner_store = PostgresStore::new(probe.owner.clone());
    let mut unscoped_owner_tx = probe.owner.begin().await.expect("begin unscoped owner tx");
    let owner_hits = owner_store
        .keyword_search_scoped(&mut unscoped_owner_tx, probe.a.graph_id, "quokka", 10, &filter)
        .await
        .expect(
            "negative control: the SAME query via BYPASSRLS owner, unscoped, must succeed — if \
             it also errored, this file's hard-error assertions would be proving nothing about \
             RLS specifically",
        );
    assert!(
        owner_hits.iter().any(|(id, _)| *id == probe.a.atom_id),
        "negative control: BYPASSRLS owner must find A's real row unscoped — got {owner_hits:?}"
    );
    unscoped_owner_tx.rollback().await.ok();

    probe.teardown().await;
}
