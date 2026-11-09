//! Live regression suite for `Store::expand`'s D1 fix against a REAL Postgres
//! (docs/plans/graph-temporal-memory.md §1 D1).
//!
//! Before this fix, the per-node sibling cap in `PostgresStore::expand`'s SQL
//! ranked candidates with `ORDER BY na.atom_id` — a random UUID — so on a node
//! with more than `MAX_SIBLING_ATOMS_PER_NODE` members, the SEED atoms
//! retrieval had just selected were usually not among the kept siblings
//! (~3% hit rate on a 300-coral reef per the spec). This silently broke
//! supersession marking, `ENTITIES` reef selection (`build_synthesis_reefs`),
//! and `atom_reef_map` at once, with no error or metric.
//!
//! This file proves the fix directly against the Postgres SQL (the CTE +
//! `ROW_NUMBER() OVER (...)` window function in `PostgresStore::expand`):
//! seeds survive the cap whatever their UUID order, the cap floors at the
//! node's own seed count, every seed survives without a query embedding, and
//! filler ranking follows `embedding <=> query_vec`.
//!
//! Gated on `RELINFER_TEST_DATABASE_URL` (a Postgres with `migrations_control`
//! applied — this suite uses the single-database "personal" schema, the same
//! one `retrieval_eval_pg.rs` / `header_len_pg.rs` use, not the separate
//! `migrations_tenant` tree); a normal `cargo test` with no DB configured
//! SKIPS LOUDLY (prints exactly why on every test) rather than silently.
//!
//! Run with:
//!   RELINFER_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:55987/coralswarm \
//!     cargo test -p coralswarm-backend --test expand_d1_pg -- --nocapture

use chrono::Utc;
use coralswarm_backend::store::{PostgresStore, Store, MAX_SIBLING_ATOMS_PER_NODE};
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

/// `set RELINFER_TEST_DATABASE_URL to run the live expand() D1 regression suite`
/// printed by every test in this file when the env var is unset — a normal
/// `cargo test` run reports this loudly on every skipped test rather than
/// passing silently (this repo's own `*_pg` convention; see
/// `scripts/check_pg_suite_coverage.sh`).
const SKIP_REASON: &str =
    "skipping: set RELINFER_TEST_DATABASE_URL to run the live expand() D1 regression suite \
     (e.g. RELINFER_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:55987/coralswarm)";

/// Seed a user → account → ("personal") graph chain and return `(store, pool,
/// account_id, user_id, graph_id)`. Mirrors `header_len_pg.rs`/
/// `retrieval_eval_pg.rs`'s setup exactly — this suite is intentionally not
/// its own new convention.
async fn setup(url: &str) -> (PostgresStore, sqlx::PgPool, Uuid, Uuid, Uuid) {
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect(url)
        .await
        .expect("connect to test Postgres");

    let email = format!("expand-d1-{}@test.local", Uuid::new_v4());
    let user_id: Uuid = sqlx::query_scalar("INSERT INTO users (email) VALUES ($1) RETURNING id")
        .bind(&email)
        .fetch_one(&pool)
        .await
        .expect("insert user");
    let account_id: Uuid =
        sqlx::query_scalar("INSERT INTO accounts (owner_user_id) VALUES ($1) RETURNING id")
            .bind(user_id)
            .fetch_one(&pool)
            .await
            .expect("insert account");

    let store = PostgresStore::new(pool.clone());
    let graph = store
        .create_graph(
            account_id,
            &format!("expand-d1-{}", Uuid::new_v4()),
            "personal",
        )
        .await
        .expect("create graph");
    (store, pool, account_id, user_id, graph.id)
}

async fn insert_atom(
    store: &PostgresStore,
    graph_id: Uuid,
    user_id: Uuid,
    atom_id: Uuid,
    source_id: &str,
) {
    store
        .insert_atoms(
            &[atom_id],
            graph_id,
            "note",
            &[source_id.to_string()],
            &[format!("blob-{source_id}")],
            &["alice".to_string()],
            Utc::now(),
            user_id,
            "private",
            "english",
        )
        .await
        .expect("insert atom");
}

/// Best-effort cleanup for the handful of FIXED (non-random) atom ids this
/// suite relies on (the "sorts dead last/second-to-last by raw atom_id"
/// tests need deterministic constants, not `Uuid::new_v4()`). `atoms.atom_id`
/// is a global primary key, not scoped by graph, so a fixed id from a
/// previous local run (a fresh graph_id each run defeats `insert_atoms`'s own
/// `ON CONFLICT (graph_id, source_type, source_id)` idempotency) would
/// otherwise collide on rerun against a persistent dev database. `node_atoms`
/// rows cascade-delete with the atom (migration 0011's `ON DELETE CASCADE`).
async fn forget_fixed_atom(pool: &sqlx::PgPool, atom_id: Uuid) {
    sqlx::query("DELETE FROM atoms WHERE atom_id = $1")
        .bind(atom_id)
        .execute(pool)
        .await
        .expect("cleanup stale fixed-id atom from a previous run");
}

async fn link_node_atom(pool: &sqlx::PgPool, node_id: Uuid, atom_id: Uuid) {
    sqlx::query("INSERT INTO node_atoms (node_id, atom_id) VALUES ($1, $2)")
        .bind(node_id)
        .bind(atom_id)
        .execute(pool)
        .await
        .expect("link node_atoms");
}

/// Stamp an atom's embedding + `embed_model` directly (mirrors the embed
/// worker's write path closely enough for this SQL-level test — the point
/// here is exercising `PostgresStore::expand`'s own query, not the embed
/// worker).
async fn stamp_embedding(pool: &sqlx::PgPool, atom_id: Uuid, embedding: &[f32]) {
    sqlx::query("UPDATE atoms SET embedding = $1, embed_model = $2, embedding_next = $1, embed_model_next = $3 WHERE atom_id = $4")
        .bind(pgvector::Vector::from(embedding.to_vec()))
        .bind(coralswarm_core::embed::EMBED_MODEL_NAME)
        .bind(coralswarm_core::embed::EMBED_MODEL_CLOUD)
        .bind(atom_id)
        .execute(pool)
        .await
        .expect("stamp embedding");
}

/// Property 1 (D1): a seed atom whose UUID sorts OUTSIDE the lexicographic
/// top-`MAX_SIBLING_ATOMS_PER_NODE` must still survive the cap. This is the
/// exact defect: the old `ORDER BY na.atom_id` ranking silently dropped seeds
/// like this one on any node with more than the cap's worth of members.
#[tokio::test]
async fn expand_keeps_seed_atom_outside_lexicographic_top_cap_against_real_postgres() {
    let url = match std::env::var("RELINFER_TEST_DATABASE_URL") {
        Ok(u) => u,
        Err(_) => {
            eprintln!("{SKIP_REASON}");
            return;
        }
    };
    let (store, pool, _account, user_id, graph_id) = setup(&url).await;
    let node_id = store
        .upsert_node(graph_id, "entity", "Crowded reef")
        .await
        .expect("upsert_node");

    // The seed atom gets the LARGEST possible UUID (all-`f` bytes), so it
    // sorts dead last by `atom_id` — precisely the case the old
    // `ORDER BY na.atom_id` cap excluded.
    let seed_atom = Uuid::from_bytes([0xff; 16]);
    forget_fixed_atom(&pool, seed_atom).await;
    insert_atom(&store, graph_id, user_id, seed_atom, "seed-src").await;
    link_node_atom(&pool, node_id, seed_atom).await;

    // MAX_SIBLING_ATOMS_PER_NODE other siblings. Random v4 UUIDs, which are
    // guaranteed to sort below the seed's all-`0xff` maximum-possible UUID —
    // random rather than another fixed low constant (e.g. `from_u128(i)`) so
    // this doesn't collide with fixed atom ids other tests in this file use
    // when `cargo test` runs them concurrently in the same database.
    let mut sibling_ids = Vec::new();
    for i in 0..MAX_SIBLING_ATOMS_PER_NODE {
        let sibling = Uuid::new_v4();
        insert_atom(&store, graph_id, user_id, sibling, &format!("sib-src-{i}")).await;
        link_node_atom(&pool, node_id, sibling).await;
        sibling_ids.push(sibling);
    }

    // Sanity check the setup actually reproduces the old failure mode: the
    // seed atom's UUID is NOT in the lexicographic bottom-N.
    let mut all_ids = sibling_ids.clone();
    all_ids.push(seed_atom);
    all_ids.sort();
    assert_eq!(
        all_ids.last().copied(),
        Some(seed_atom),
        "test setup invariant broken: seed atom must sort dead last by raw atom_id"
    );

    let res = store
        .expand(graph_id, &[seed_atom], None)
        .await
        .expect("expand");
    assert_eq!(res.len(), 1);
    assert!(
        res[0].atom_ids.contains(&seed_atom),
        "D1 regression: the seed atom itself must never be dropped from its own node's \
         expansion, regardless of how its UUID sorts against its siblings: got {:?}",
        res[0].atom_ids
    );
}

/// Property 2 (D1): a node whose OWN seed count exceeds
/// `MAX_SIBLING_ATOMS_PER_NODE` must keep every seed — the effective cap
/// floors at `GREATEST(MAX_SIBLING_ATOMS_PER_NODE, seed_count)`, not a flat
/// constant.
#[tokio::test]
async fn expand_floors_the_cap_at_the_nodes_own_seed_count_against_real_postgres() {
    let url = match std::env::var("RELINFER_TEST_DATABASE_URL") {
        Ok(u) => u,
        Err(_) => {
            eprintln!("{SKIP_REASON}");
            return;
        }
    };
    let (store, pool, _account, user_id, graph_id) = setup(&url).await;
    let node_id = store
        .upsert_node(graph_id, "entity", "All-seed reef")
        .await
        .expect("upsert_node");

    // MAX_SIBLING_ATOMS_PER_NODE + 5 atoms in ONE node, ALL of them seeds. A
    // flat `MAX_SIBLING_ATOMS_PER_NODE` cap would truncate 5 of them.
    let total = MAX_SIBLING_ATOMS_PER_NODE + 5;
    let mut seed_ids = Vec::new();
    for i in 0..total {
        let atom = Uuid::new_v4();
        insert_atom(&store, graph_id, user_id, atom, &format!("allseed-src-{i}")).await;
        link_node_atom(&pool, node_id, atom).await;
        seed_ids.push(atom);
    }

    let res = store
        .expand(graph_id, &seed_ids, None)
        .await
        .expect("expand");
    assert_eq!(res.len(), 1);
    let mut got = res[0].atom_ids.clone();
    got.sort();
    let mut want = seed_ids.clone();
    want.sort();
    assert_eq!(
        got,
        want,
        "D1 regression: every seed must survive when a node's own seed count exceeds \
         MAX_SIBLING_ATOMS_PER_NODE ({MAX_SIBLING_ATOMS_PER_NODE}): got {} atoms, want {}",
        got.len(),
        want.len()
    );
}

/// Property 3 (D1): the `query_embedding: None` path (no query vector in
/// scope — e.g. a source listing / atlas render) must still keep every seed
/// via seeds-first ordering; only filler ranking degrades to the
/// deterministic fallback.
#[tokio::test]
async fn expand_with_no_query_embedding_still_keeps_every_seed_against_real_postgres() {
    let url = match std::env::var("RELINFER_TEST_DATABASE_URL") {
        Ok(u) => u,
        Err(_) => {
            eprintln!("{SKIP_REASON}");
            return;
        }
    };
    let (store, pool, _account, user_id, graph_id) = setup(&url).await;
    let node_id = store
        .upsert_node(graph_id, "entity", "No-embedding reef")
        .await
        .expect("upsert_node");

    // A distinct fixed constant from the other lexicographic-cap test in this
    // file (which uses all-`0xff`) — seeds-first ordering is unconditional on
    // the seed's UUID value, so a plain random seed would prove the same
    // property, but this constant also keeps this test's data from ever
    // colliding with `Uuid::new_v4()` collisions across concurrently-run
    // tests in the same database.
    let seed_atom = Uuid::from_bytes([0xfe; 16]);
    forget_fixed_atom(&pool, seed_atom).await;
    insert_atom(&store, graph_id, user_id, seed_atom, "no-embed-seed").await;
    link_node_atom(&pool, node_id, seed_atom).await;

    for i in 0..MAX_SIBLING_ATOMS_PER_NODE {
        let filler = Uuid::new_v4();
        insert_atom(
            &store,
            graph_id,
            user_id,
            filler,
            &format!("no-embed-filler-{i}"),
        )
        .await;
        link_node_atom(&pool, node_id, filler).await;
    }

    let res = store
        .expand(graph_id, &[seed_atom], None)
        .await
        .expect("expand");
    assert_eq!(res.len(), 1);
    assert!(
        res[0].atom_ids.contains(&seed_atom),
        "D1 regression: with no query_embedding, the seed must still survive via seeds-first \
         ordering: got {:?}",
        res[0].atom_ids
    );
}

/// Property 4 (pgvector): with a query vector present, filler ranking must
/// actually follow `embedding <=> query_vec`, not just "seeds survive". Two
/// non-seed siblings sit at deliberately different cosine distances from the
/// query vector, and the cap admits exactly one filler slot — the NEARER
/// sibling must be the one that survives.
#[tokio::test]
async fn expand_ranks_fillers_by_embedding_similarity_to_the_query_vector_against_real_postgres() {
    let url = match std::env::var("RELINFER_TEST_DATABASE_URL") {
        Ok(u) => u,
        Err(_) => {
            eprintln!("{SKIP_REASON}");
            return;
        }
    };
    let (store, pool, _account, user_id, graph_id) = setup(&url).await;
    let node_id = store
        .upsert_node(graph_id, "entity", "Similarity reef")
        .await
        .expect("upsert_node");

    // MAX_SIBLING_ATOMS_PER_NODE - 1 seeds, leaving exactly ONE filler slot
    // under the cap (cap = max(MAX_SIBLING_ATOMS_PER_NODE, seed_count) =
    // MAX_SIBLING_ATOMS_PER_NODE since seed_count < the base cap here).
    let mut seed_ids = Vec::new();
    for i in 0..(MAX_SIBLING_ATOMS_PER_NODE - 1) {
        let seed = Uuid::new_v4();
        insert_atom(&store, graph_id, user_id, seed, &format!("sim-seed-{i}")).await;
        link_node_atom(&pool, node_id, seed).await;
        seed_ids.push(seed);
    }

    let dim = coralswarm_core::embed_ort::EMBED_DIM;
    let mut query_vec = vec![0.0f32; dim];
    query_vec[0] = 1.0;

    // NEAR filler: embedding identical to the query vector (cosine distance
    // 0).
    let near = Uuid::new_v4();
    insert_atom(&store, graph_id, user_id, near, "sim-near").await;
    link_node_atom(&pool, node_id, near).await;
    stamp_embedding(&pool, near, &query_vec).await;

    // FAR filler: embedding orthogonal to the query vector (cosine distance
    // 1, strictly worse than `near`'s 0).
    let far = Uuid::new_v4();
    insert_atom(&store, graph_id, user_id, far, "sim-far").await;
    link_node_atom(&pool, node_id, far).await;
    let mut far_vec = vec![0.0f32; dim];
    far_vec[1] = 1.0;
    stamp_embedding(&pool, far, &far_vec).await;

    // Seeds + near + far = MAX_SIBLING_ATOMS_PER_NODE + 1 members, one over
    // the cap — exactly one of {near, far} must be dropped, and it must be
    // `far` (the one further from the query vector).
    let res = store
        .expand(graph_id, &seed_ids, Some(&query_vec))
        .await
        .expect("expand");
    assert_eq!(res.len(), 1);
    let atom_ids = &res[0].atom_ids;
    assert_eq!(
        atom_ids.len(),
        MAX_SIBLING_ATOMS_PER_NODE,
        "cap must still be MAX_SIBLING_ATOMS_PER_NODE with only {} seeds present: got {:?}",
        seed_ids.len(),
        atom_ids
    );
    for seed in &seed_ids {
        assert!(
            atom_ids.contains(seed),
            "every seed must survive: missing {seed}"
        );
    }
    assert!(
        atom_ids.contains(&near),
        "D1: the filler whose embedding matches the query vector (cosine distance 0) must \
         win the one remaining slot over a strictly-further filler: got {atom_ids:?}"
    );
    assert!(
        !atom_ids.contains(&far),
        "D1: the filler further from the query vector (cosine distance 1) must lose the one \
         remaining slot to the nearer filler: got {atom_ids:?}"
    );
}
