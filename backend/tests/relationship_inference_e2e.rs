//! Live end-to-end test for the relationship knowledge graph (ADR-0008).
//!
//! Drives the REAL `PostgresStore` against a real Postgres: seeds the
//! Alex/Harper/Casey/Jordan/Kacey family as stated edges, runs the inference
//! composition pass, persists the derived edges through the confidence gate, and
//! asserts the headline behaviour against the database:
//!
//!   * `parent_of(Harper, Casey)` is inferred and retrievable (status `suggested`),
//!   * `parent_of(Jordan, Casey)` is NEVER produced (the old conflation bug),
//!   * stated edges round-trip through `upsert`/`list` (incl. oplog emission).
//!
//! Gated on `RELINFER_TEST_DATABASE_URL` (a Postgres with all migrations applied);
//! a normal `cargo test` with no DB configured skips it.
//!
//! Run with:
//!   RELINFER_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:54322/cs_relinfer_test \
//!     cargo test -p coralswarm-backend --test relationship_inference_e2e -- --nocapture

use coralswarm_backend::relationships::inference::{self, EdgeInput};
use coralswarm_backend::relationships::{predicates, EdgeOrigin, EdgeStatus};
use coralswarm_backend::store::{PostgresStore, Store};
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

const AUTO_CONFIRM: f32 = 0.85;
const SOFT_FLOOR: f32 = 0.5;

#[tokio::test]
async fn harper_inferred_as_Caseys_mother_against_real_postgres() {
    let url = match std::env::var("RELINFER_TEST_DATABASE_URL") {
        Ok(u) => u,
        Err(_) => {
            eprintln!("skipping: set RELINFER_TEST_DATABASE_URL to run the live e2e test");
            return;
        }
    };

    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&url)
        .await
        .expect("connect to test Postgres");

    // ── Seed tenancy chain: user → account → graph ────────────────────────────
    let email = format!("{}@test.local", Uuid::new_v4());
    let user_id: Uuid =
        sqlx::query_scalar("INSERT INTO users (email) VALUES ($1) RETURNING id")
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
        .create_graph(account_id, "relinfer-e2e", "personal")
        .await
        .expect("create graph");
    let graph_id = graph.id;

    // ── Entity reefs ──────────────────────────────────────────────────────────
    let Alex = store.upsert_node(graph_id, "entity", "Alex").await.unwrap();
    let harper = store.upsert_node(graph_id, "entity", "Harper").await.unwrap();
    let Casey = store.upsert_node(graph_id, "entity", "Casey").await.unwrap();
    let Jordan = store.upsert_node(graph_id, "entity", "Jordan").await.unwrap();
    let kacey = store.upsert_node(graph_id, "entity", "Kacey").await.unwrap();

    // ── Stated edges (as the extraction worker would write them) ──────────────
    let stated = |src, pred: &'static str, dst| (src, pred, dst);
    let edges = [
        // "Harper is my wife" (author Alex) — spouse_of symmetric
        stated(Alex, predicates::SPOUSE_OF, harper),
        stated(harper, predicates::SPOUSE_OF, Alex),
        // "Casey is my daughter" — parent_of + child_of inverse
        stated(Alex, predicates::PARENT_OF, Casey),
        stated(Casey, predicates::CHILD_OF, Alex),
        // "Jordan is the mother of Kacey" — the COUSIN, not Casey
        stated(Jordan, predicates::PARENT_OF, kacey),
        stated(kacey, predicates::CHILD_OF, Jordan),
    ];
    for (src, pred, dst) in edges {
        store
            .upsert_relationship_edge(
                graph_id,
                src,
                pred,
                dst,
                EdgeOrigin::Stated,
                EdgeStatus::Confirmed,
                0.95,
                serde_json::json!({ "atom_ids": [] }),
            )
            .await
            .expect("upsert stated edge");
    }

    // ── Round-trip: stated edges persisted as Confirmed ───────────────────────
    let confirmed = store
        .list_relationship_edges_by_status(graph_id, EdgeStatus::Confirmed)
        .await
        .expect("list confirmed");
    assert_eq!(confirmed.len(), 6, "all six stated edges should round-trip");

    // ── Inference closure (mirrors the worker) ────────────────────────────────
    let inputs: Vec<EdgeInput> = confirmed
        .iter()
        .map(|e| EdgeInput {
            edge_id: e.edge_id,
            src: e.src_node_id,
            predicate: e.predicate.clone(),
            dst: e.dst_node_id,
            confidence: e.confidence,
        })
        .collect();

    for c in inference::infer(&inputs) {
        let status = if c.confidence >= AUTO_CONFIRM {
            EdgeStatus::Confirmed
        } else if c.confidence >= SOFT_FLOOR {
            EdgeStatus::Suggested
        } else {
            continue;
        };
        store
            .upsert_relationship_edge(
                graph_id,
                c.src,
                c.predicate,
                c.dst,
                EdgeOrigin::Inferred,
                status,
                c.confidence,
                serde_json::json!({ "rule": c.rule }),
            )
            .await
            .expect("upsert inferred edge");
    }

    // ── Assert the headline against the DB ────────────────────────────────────
    let around_Casey = store
        .list_relationship_edges_for_nodes(graph_id, &[Casey])
        .await
        .expect("list edges around Casey");

    let harper_is_mother = around_Casey.iter().find(|e| {
        e.predicate == predicates::PARENT_OF
            && e.src_node_id == harper
            && e.dst_node_id == Casey
    });
    let hit = harper_is_mother.expect("DB must contain inferred parent_of(Harper, Casey)");
    assert_eq!(hit.origin, EdgeOrigin::Inferred);
    assert_eq!(
        hit.status,
        EdgeStatus::Suggested,
        "defeasible inference should land in review (suggested), not auto-confirmed"
    );

    // The conflation bug must stay dead: nothing links Jordan to Casey.
    assert!(
        !around_Casey.iter().any(|e| {
            e.predicate == predicates::PARENT_OF
                && e.src_node_id == Jordan
                && e.dst_node_id == Casey
        }),
        "must NOT infer parent_of(Jordan, Casey) — Jordan is Kacey's mother"
    );

    // ── Cleanup (best-effort; account cascade drops graph + edges) ────────────
    let mut financial = coralswarm_backend::tenancy::scope::begin_account_scoped(&pool, account_id).await.unwrap();
    coralswarm_backend::billing::renewal::lock_period_rotation(&mut financial, account_id).await.unwrap();
    let _ = sqlx::query("DELETE FROM accounts WHERE id = $1")
        .bind(account_id)
        .execute(&mut *financial)
        .await;
    financial.commit().await.unwrap();
    let _ = sqlx::query("DELETE FROM users WHERE id = $1")
        .bind(user_id)
        .execute(&pool)
        .await;
}

/// H1 (ADR-0008): a Rejected edge is a durable negative constraint. Re-asserting
/// the same triple — as the extraction worker does, always with status=Confirmed —
/// must NOT flip it back to Confirmed.
#[tokio::test]
async fn rejected_edge_is_not_resurrected_by_reextraction() {
    let url = match std::env::var("RELINFER_TEST_DATABASE_URL") {
        Ok(u) => u,
        Err(_) => {
            eprintln!("skipping: set RELINFER_TEST_DATABASE_URL to run the live e2e test");
            return;
        }
    };
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&url)
        .await
        .expect("connect to test Postgres");

    let email = format!("{}@test.local", Uuid::new_v4());
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
        .create_graph(account_id, "relinfer-reject", "personal")
        .await
        .expect("create graph");
    let g = graph.id;
    let alice = store.upsert_node(g, "entity", "Alice").await.unwrap();
    let bob = store.upsert_node(g, "entity", "Bob").await.unwrap();

    // Stated edge, then the user REJECTS it.
    let edge = store
        .upsert_relationship_edge(
            g,
            alice,
            predicates::SPOUSE_OF,
            bob,
            EdgeOrigin::Stated,
            EdgeStatus::Confirmed,
            0.95,
            serde_json::json!({}),
        )
        .await
        .unwrap();
    store
        .set_relationship_edge_status(edge.edge_id, EdgeStatus::Rejected, None, "test")
        .await
        .unwrap();
    assert_eq!(
        store
            .get_relationship_edge(edge.edge_id)
            .await
            .unwrap()
            .unwrap()
            .status,
        EdgeStatus::Rejected
    );

    // Re-extraction re-asserts the SAME triple as Confirmed — the resurrection vector.
    store
        .upsert_relationship_edge(
            g,
            alice,
            predicates::SPOUSE_OF,
            bob,
            EdgeOrigin::Stated,
            EdgeStatus::Confirmed,
            0.99,
            serde_json::json!({}),
        )
        .await
        .unwrap();

    let after = store
        .get_relationship_edge(edge.edge_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        after.status,
        EdgeStatus::Rejected,
        "re-asserting a rejected triple must NOT resurrect it to Confirmed"
    );

    let mut financial = coralswarm_backend::tenancy::scope::begin_account_scoped(&pool, account_id).await.unwrap();
    coralswarm_backend::billing::renewal::lock_period_rotation(&mut financial, account_id).await.unwrap();
    let _ = sqlx::query("DELETE FROM accounts WHERE id = $1")
        .bind(account_id)
        .execute(&mut *financial)
        .await;
    financial.commit().await.unwrap();
    let _ = sqlx::query("DELETE FROM users WHERE id = $1")
        .bind(user_id)
        .execute(&pool)
        .await;
}

/// A confirm/reject must cover the WHOLE relationship: setting a directional
/// edge's status also flips its materialized inverse, so a rejected claim can't
/// leak back through the reverse direction (e.g. rejecting `expert_in` must also
/// reject `expertise_of`).
#[tokio::test]
async fn review_decision_propagates_to_materialized_inverse() {
    let url = match std::env::var("RELINFER_TEST_DATABASE_URL") {
        Ok(u) => u,
        Err(_) => {
            eprintln!("skipping: set RELINFER_TEST_DATABASE_URL to run the live e2e test");
            return;
        }
    };
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&url)
        .await
        .expect("connect to test Postgres");

    let email = format!("{}@test.local", Uuid::new_v4());
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
    let g = store
        .create_graph(account_id, "inverse-review", "personal")
        .await
        .expect("create graph")
        .id;
    let Taylor = store.upsert_node(g, "entity", "Taylor").await.unwrap();
    let topic = store.upsert_node(g, "entity", "Financial Projections").await.unwrap();

    // Inferred expert_in + its materialized expertise_of inverse, both Suggested.
    let fwd = store
        .upsert_relationship_edge(
            g, Taylor, predicates::EXPERT_IN, topic,
            EdgeOrigin::Inferred, EdgeStatus::Suggested, 0.9, serde_json::json!({}),
        )
        .await
        .unwrap();
    store
        .upsert_relationship_edge(
            g, topic, predicates::EXPERTISE_OF, Taylor,
            EdgeOrigin::Inferred, EdgeStatus::Suggested, 0.9, serde_json::json!({}),
        )
        .await
        .unwrap();

    // Reject the FORWARD edge only.
    store
        .set_relationship_edge_status(fwd.edge_id, EdgeStatus::Rejected, None, "test")
        .await
        .unwrap();

    // Both directions must now be Rejected; neither should remain Suggested.
    let rejected = store
        .list_relationship_edges_by_status(g, EdgeStatus::Rejected)
        .await
        .unwrap();
    let suggested = store
        .list_relationship_edges_by_status(g, EdgeStatus::Suggested)
        .await
        .unwrap();
    assert!(
        rejected.iter().any(|e| e.predicate == predicates::EXPERT_IN
            && e.src_node_id == Taylor
            && e.dst_node_id == topic),
        "forward expert_in must be Rejected"
    );
    assert!(
        rejected.iter().any(|e| e.predicate == predicates::EXPERTISE_OF
            && e.src_node_id == topic
            && e.dst_node_id == Taylor),
        "inverse expertise_of must ALSO be Rejected — the rejected claim must not leak back"
    );
    assert!(
        !suggested.iter().any(|e| e.predicate == predicates::EXPERTISE_OF),
        "inverse must not remain Suggested after the forward edge was rejected"
    );

    let mut financial = coralswarm_backend::tenancy::scope::begin_account_scoped(&pool, account_id).await.unwrap();
    coralswarm_backend::billing::renewal::lock_period_rotation(&mut financial, account_id).await.unwrap();
    let _ = sqlx::query("DELETE FROM accounts WHERE id = $1")
        .bind(account_id)
        .execute(&mut *financial)
        .await;
    financial.commit().await.unwrap();
    let _ = sqlx::query("DELETE FROM users WHERE id = $1")
        .bind(user_id)
        .execute(&pool)
        .await;
}
