//! Live Postgres tests for §5.4's NEW SQL: `Store::matched_reef_ids` and
//! `Store::reef_atoms_ranked_by_similarity` — the two Postgres-only query
//! paths introduced for currents traversal, which the in-crate
//! `#[cfg(test)]` suite in `src/routes/queries.rs` does not reach.
//!
//! Deliberately drives `PostgresStore` DIRECTLY (not through the HTTP layer
//! or `multi_graph_retrieve`), same pattern as `reef_search_tsv_pg.rs`: real
//! `tsvector`/`ts_rank_cd` for the reef match, real `pgvector <=> ` cosine
//! distance for the atom ranking — nothing here is mocked or re-implemented
//! in Rust. The traversability predicate (§5.3), hop-decay arithmetic, and
//! cap logic (§5.4) are pure Rust and already covered by the committed
//! `src/relationships/mod.rs` / `src/retrieval/traversal.rs` unit tests; this
//! file exists to prove the two new SQL queries those pure functions depend
//! on actually do what their doc comments say against a real database.
//!
//! Gated on `RELINFER_TEST_DATABASE_URL` (a Postgres with all migrations
//! applied); a normal `cargo test` with no DB configured skips every test.
//!
//! Run with:
//!   RELINFER_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:55987/graph_traversal_wt \
//!     cargo test -p coralswarm-backend --test currents_traversal_pg -- --nocapture

use chrono::Utc;
use coralswarm_backend::retrieval::RetrievalFilter;
use coralswarm_backend::store::{PostgresStore, Store, MAX_SIBLING_ATOMS_PER_NODE};
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

macro_rules! test_pool {
    () => {{
        match std::env::var("RELINFER_TEST_DATABASE_URL") {
            Ok(u) => PgPoolOptions::new()
                .max_connections(4)
                .connect(&u)
                .await
                .expect("connect to test Postgres"),
            Err(_) => {
                eprintln!("skipping: set RELINFER_TEST_DATABASE_URL to run currents_traversal_pg");
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
        .create_graph(
            account_id,
            &format!("currents-traversal-{}", Uuid::new_v4()),
            "personal",
        )
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

async fn seed_atom(store: &PostgresStore, graph_id: Uuid, user_id: Uuid, source_id: &str) -> Uuid {
    let atom_id = Uuid::new_v4();
    store
        .insert_atoms(
            &[atom_id],
            graph_id,
            "mcp",
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
    atom_id
}

/// A unit vector along dimension `dim` (out of `EMBED_DIM`) — cheap synthetic
/// embeddings whose cosine distances are exactly known ahead of time (0 for
/// identical dims, 1 for orthogonal dims), so ranking assertions don't depend
/// on a real embedder.
const EMBED_DIM: usize = 384;
fn unit_vec(dim: usize) -> Vec<f32> {
    let mut v = vec![0.0f32; EMBED_DIM];
    v[dim] = 1.0;
    v
}

async fn set_embedding(store: &PostgresStore, atom_id: Uuid, v: &[f32]) {
    store
        .set_atom_embeddings(&[atom_id], &[v.to_vec()])
        .await
        .expect("set_atom_embeddings");
    store
        .set_cloud_embeddings(&[atom_id], &[v.to_vec()])
        .await
        .expect("set_cloud_embeddings");
}

// ── `Store::matched_reef_ids` — the §5.4 hop-0 seed frontier ────────────────────

#[tokio::test]
async fn matched_reef_ids_finds_the_reef_the_query_names() {
    let pool = test_pool!();
    let (user_id, account_id, graph_id) = seed_tenancy(&pool).await;
    let store = PostgresStore::new(pool.clone());

    let reef = store
        .upsert_node(graph_id, "entity", "Zzqcurrents Traversal Seed")
        .await
        .expect("upsert_node");
    let atom = seed_atom(&store, graph_id, user_id, "s-seed").await;
    store.link_node_atoms(&[(reef, atom)]).await.unwrap();

    // An unrelated reef that must NOT qualify (shares no query tokens).
    let unrelated = store
        .upsert_node(graph_id, "entity", "Something Else Entirely")
        .await
        .unwrap();
    let unrelated_atom = seed_atom(&store, graph_id, user_id, "s-unrelated").await;
    store
        .link_node_atoms(&[(unrelated, unrelated_atom)])
        .await
        .unwrap();

    let matched = store
        .matched_reef_ids(
            graph_id,
            "zzqcurrents traversal seed",
            8,
            &RetrievalFilter::default(),
        )
        .await
        .expect("matched_reef_ids");
    let ids: Vec<Uuid> = matched.iter().map(|(id, _)| *id).collect();

    assert!(
        ids.contains(&reef),
        "the named reef must qualify: {matched:?}"
    );
    assert!(
        !ids.contains(&unrelated),
        "the unrelated reef must not qualify: {matched:?}"
    );
    // `matched_reef_ids` is step 1 ONLY — no atoms attached, node ids only.
    assert!(matched.iter().all(|(_, score)| *score > 0.0));

    cleanup(&pool, account_id, user_id).await;
}

#[tokio::test]
async fn matched_reef_ids_and_keyword_search_node_atoms_agree_on_which_reefs_qualify() {
    // §5.4's doc comment promises `matched_reef_ids` is EXACTLY step 1 of
    // `keyword_search_node_atoms` — same qualification, same ranking, same
    // reservation. Prove it by comparing the reef-id/score output of both
    // against the identical query/data.
    let pool = test_pool!();
    let (user_id, account_id, graph_id) = seed_tenancy(&pool).await;
    let store = PostgresStore::new(pool.clone());

    let reef_a = store
        .upsert_node(graph_id, "entity", "Zzqalpha Reef")
        .await
        .unwrap();
    let atom_a = seed_atom(&store, graph_id, user_id, "s-a").await;
    store.link_node_atoms(&[(reef_a, atom_a)]).await.unwrap();

    // A `decision`-typed reef would need `body.decided_at` at INSERT time
    // (the non-deferrable `decisions_require_decided_at` CHECK) — irrelevant
    // to what this test is proving, so a second `entity` reef with a
    // sentence-length title exercises the same coverage-floor qualification
    // path without that unrelated ceremony.
    let reef_b = store
        .upsert_node(
            graph_id,
            "entity",
            "We zzqalpha88 decided to change zzqreef99 policy",
        )
        .await
        .unwrap();
    let atom_b = seed_atom(&store, graph_id, user_id, "s-b").await;
    store.link_node_atoms(&[(reef_b, atom_b)]).await.unwrap();

    let filter = RetrievalFilter::default();
    let matched = store
        .matched_reef_ids(graph_id, "zzqalpha88 zzqreef99", 8, &filter)
        .await
        .unwrap();
    let kw = store
        .keyword_search_node_atoms(graph_id, "zzqalpha88 zzqreef99", 8, &filter)
        .await
        .unwrap();

    // Every atom `keyword_search_node_atoms` surfaced must trace back to a
    // reef `matched_reef_ids` also qualified.
    let matched_ids: std::collections::HashSet<Uuid> = matched.iter().map(|(id, _)| *id).collect();
    assert!(
        matched_ids.contains(&reef_b),
        "reef_b (2/2 coverage) must qualify: {matched:?}"
    );
    let kw_ids: Vec<Uuid> = kw.iter().map(|(id, _)| *id).collect();
    assert!(kw_ids.contains(&atom_b), "{kw:?}");

    cleanup(&pool, account_id, user_id).await;
}

// ── `Store::reef_atoms_ranked_by_similarity` — the D1 fix for hopped reefs ─────

#[tokio::test]
async fn reef_atoms_ranked_by_similarity_orders_by_real_pgvector_cosine_distance() {
    let pool = test_pool!();
    let (user_id, account_id, graph_id) = seed_tenancy(&pool).await;
    let store = PostgresStore::new(pool.clone());

    let reef = store
        .upsert_node(graph_id, "entity", "Zzqhopped Reef")
        .await
        .unwrap();
    let near = seed_atom(&store, graph_id, user_id, "s-near").await;
    let mid = seed_atom(&store, graph_id, user_id, "s-mid").await;
    let far = seed_atom(&store, graph_id, user_id, "s-far").await;
    store
        .link_node_atoms(&[(reef, near), (reef, mid), (reef, far)])
        .await
        .unwrap();

    // Query vector = unit_vec(0). `near` = unit_vec(0) (identical, distance 0);
    // `mid` = a 45°-ish blend of dims 0/1; `far` = unit_vec(1) (orthogonal,
    // distance 1 in cosine-similarity terms -> pgvector distance 1.0).
    let query_vec = unit_vec(0);
    set_embedding(&store, near, &query_vec).await;
    let mut mid_vec = vec![0.0f32; EMBED_DIM];
    mid_vec[0] = 1.0;
    mid_vec[1] = 0.5;
    set_embedding(&store, mid, &mid_vec).await;
    set_embedding(&store, far, &unit_vec(1)).await;

    let atoms_by_node = store
        .reef_atoms_ranked_by_similarity(graph_id, &[reef], Some(&query_vec))
        .await
        .expect("reef_atoms_ranked_by_similarity");
    let ranked = atoms_by_node.get(&reef).expect("reef present");

    assert_eq!(
        ranked,
        &vec![near, mid, far],
        "must rank nearest-to-query-vector first (D1 fix, not heap order): {ranked:?}"
    );

    cleanup(&pool, account_id, user_id).await;
}

#[tokio::test]
async fn reef_atoms_ranked_by_similarity_excludes_a_different_embed_model_from_ranking_but_keeps_the_atom(
) {
    let pool = test_pool!();
    let (user_id, account_id, graph_id) = seed_tenancy(&pool).await;
    let store = PostgresStore::new(pool.clone());

    let reef = store
        .upsert_node(graph_id, "entity", "Zzqmixed Model Reef")
        .await
        .unwrap();
    let matching_model = seed_atom(&store, graph_id, user_id, "s-mm").await;
    let stale_model = seed_atom(&store, graph_id, user_id, "s-sm").await;
    store
        .link_node_atoms(&[(reef, matching_model), (reef, stale_model)])
        .await
        .unwrap();

    let query_vec = unit_vec(0);
    // `matching_model` embedded under the CURRENT model — ranks by real distance.
    set_embedding(&store, matching_model, &unit_vec(1)).await; // far from query
                                                               // `stale_model` simulates a pre-migration / different-model embedding by
                                                               // writing `embed_model` directly (the public `set_atom_embeddings` API
                                                               // always stamps the current model, so this bypasses it deliberately to
                                                               // simulate the legacy-row case the `embed_model` filter guards against).
    sqlx::query(
        "UPDATE atoms SET embedding = $1, embed_model = 'some-other-model', \
                embedding_next = $1, embed_model_next = 'some-other-model' WHERE atom_id = $2",
    )
    .bind(pgvector::Vector::from(query_vec.clone())) // identical to the query — would rank FIRST if the filter were missing
    .bind(stale_model)
    .execute(&pool)
    .await
    .unwrap();

    let atoms_by_node = store
        .reef_atoms_ranked_by_similarity(graph_id, &[reef], Some(&query_vec))
        .await
        .expect("reef_atoms_ranked_by_similarity");
    let ranked = atoms_by_node.get(&reef).expect("reef present");

    assert_eq!(ranked.len(), 2, "both atoms still present: {ranked:?}");
    assert_eq!(
        ranked[0], matching_model,
        "the CURRENT-model atom must rank first even though the stale-model atom is a closer \
         RAW vector match — embed_model must gate ranking, matching `vector_search`'s own hard \
         filter: {ranked:?}"
    );

    cleanup(&pool, account_id, user_id).await;
}

#[tokio::test]
async fn reef_atoms_ranked_by_similarity_caps_at_max_sibling_atoms_per_node() {
    let pool = test_pool!();
    let (user_id, account_id, graph_id) = seed_tenancy(&pool).await;
    let store = PostgresStore::new(pool.clone());

    let reef = store
        .upsert_node(graph_id, "entity", "Zzqbig Reef")
        .await
        .unwrap();
    let mut atoms = Vec::new();
    for i in 0..(MAX_SIBLING_ATOMS_PER_NODE + 7) {
        let atom = seed_atom(&store, graph_id, user_id, &format!("s-big-{i}")).await;
        store.link_node_atoms(&[(reef, atom)]).await.unwrap();
        atoms.push(atom);
    }

    let atoms_by_node = store
        .reef_atoms_ranked_by_similarity(graph_id, &[reef], None)
        .await
        .expect("reef_atoms_ranked_by_similarity");
    let ranked = atoms_by_node.get(&reef).expect("reef present");

    assert_eq!(ranked.len(), MAX_SIBLING_ATOMS_PER_NODE);

    cleanup(&pool, account_id, user_id).await;
}

#[tokio::test]
async fn reef_atoms_ranked_by_similarity_none_query_embedding_falls_back_to_atom_id_order() {
    let pool = test_pool!();
    let (user_id, account_id, graph_id) = seed_tenancy(&pool).await;
    let store = PostgresStore::new(pool.clone());

    let reef = store
        .upsert_node(graph_id, "entity", "Zzqno Query Vec Reef")
        .await
        .unwrap();
    let mut atoms = Vec::new();
    for i in 0..3 {
        let atom = seed_atom(&store, graph_id, user_id, &format!("s-noqv-{i}")).await;
        store.link_node_atoms(&[(reef, atom)]).await.unwrap();
        // Even WITH an embedding, `query_embedding: None` must ignore it.
        set_embedding(&store, atom, &unit_vec(i % EMBED_DIM)).await;
        atoms.push(atom);
    }
    let mut expected = atoms.clone();
    expected.sort();

    let atoms_by_node = store
        .reef_atoms_ranked_by_similarity(graph_id, &[reef], None)
        .await
        .expect("reef_atoms_ranked_by_similarity");
    let mut ranked = atoms_by_node.get(&reef).expect("reef present").clone();
    ranked.sort();

    assert_eq!(
        ranked, expected,
        "None query embedding must still return every atom, deterministically ordered"
    );

    cleanup(&pool, account_id, user_id).await;
}
