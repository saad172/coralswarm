//! Persisted relationship contracts for the Postgres store.
//! PostgreSQL tests use an explicitly supplied, loopback-only scratch database:
//! MEMORY_PROVENANCE_TEST_DATABASE_URL=postgres://postgres:memory_test_only@127.0.0.1:15549/memory_provenance_test
//! Apply migrations_control first. No dotenv loading or remote database fallback.

use std::collections::{HashMap, HashSet};
use std::str::FromStr;

use coralswarm_backend::relationships::{predicates, EdgeOrigin, EdgeStatus, RelationshipEdge};
use coralswarm_backend::retrieval::traversal::{hop_scores, HopPolicy};
use coralswarm_backend::store::{PostgresStore, Store};
use coralswarm_backend::tenancy::scope::begin_account_scoped;
use serde_json::{json, Value};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::PgPool;
use uuid::Uuid;

struct Fixture<'a> {
    store: &'a dyn Store,
    scope: Option<(&'a PgPool, Uuid)>,
    graph: Uuid,
    src: Uuid,
    dst: Uuid,
    user: Uuid,
}

impl Fixture<'_> {
    async fn upsert(
        &self,
        inverse: bool,
        origin: EdgeOrigin,
        status: EdgeStatus,
        provenance: Value,
    ) -> RelationshipEdge {
        let (src, predicate, dst) = if inverse {
            (self.dst, predicates::REPORTS_TO, self.src)
        } else {
            (self.src, predicates::MANAGES, self.dst)
        };
        if let Some((pool, account)) = self.scope {
            let mut tx = begin_account_scoped(pool, account).await.unwrap();
            let edge = self
                .store
                .upsert_relationship_edge_scoped(
                    &mut tx, self.graph, src, predicate, dst, origin, status, 0.95, provenance,
                )
                .await
                .unwrap();
            tx.commit().await.unwrap();
            edge
        } else {
            self.store
                .upsert_relationship_edge(
                    self.graph, src, predicate, dst, origin, status, 0.95, provenance,
                )
                .await
                .unwrap()
        }
    }

    async fn review(&self, id: Uuid, status: EdgeStatus) {
        if let Some((pool, account)) = self.scope {
            let mut tx = begin_account_scoped(pool, account).await.unwrap();
            self.store
                .set_relationship_edge_status_scoped(
                    &mut tx,
                    id,
                    status,
                    Some(self.user),
                    "provenance-regression",
                )
                .await
                .unwrap();
            tx.commit().await.unwrap();
        } else {
            self.store
                .set_relationship_edge_status(id, status, Some(self.user), "provenance-regression")
                .await
                .unwrap();
        }
    }
}

async fn contract(
    store: &dyn Store,
    pool: &PgPool,
    account: Uuid,
    user: Uuid,
    scope: Option<(&PgPool, Uuid)>,
) {
    let graph = store
        .create_graph(account, "Relationship provenance", "personal")
        .await
        .unwrap()
        .id;
    let src = store
        .upsert_node(graph, "entity", "Project Lead")
        .await
        .unwrap();
    let dst = store
        .upsert_node(graph, "entity", "Engineer")
        .await
        .unwrap();
    let f = Fixture {
        store,
        scope,
        graph,
        src,
        dst,
        user,
    };
    let atom_a = Uuid::new_v4();
    let atom_b = Uuid::new_v4();
    let dependency = Uuid::new_v4();
    let start_seq: i64 = sqlx::query_scalar("SELECT COALESCE(MAX(seq), 0) FROM op_log")
        .fetch_one(pool)
        .await
        .unwrap();

    // Independently meaningful derivation metadata survives direct restatement;
    // a repeated extraction contributes support instead of replacing it.
    let initial = f.upsert(false, EdgeOrigin::Inferred, EdgeStatus::Confirmed,
        json!({"atom_ids": [atom_a], "rule": "existing-rule", "source_edge_ids": [dependency], "auto_confirmed": true})).await;
    let reverse = f
        .upsert(
            true,
            EdgeOrigin::Inferred,
            EdgeStatus::Confirmed,
            json!({"atom_ids": [atom_a], "derived": "inverse", "auto_confirmed": true}),
        )
        .await;
    let before_review = f
        .upsert(
            false,
            EdgeOrigin::Inferred,
            EdgeStatus::Confirmed,
            json!({"atom_ids": [atom_b], "auto_confirmed": true}),
        )
        .await;
    assert_eq!(
        before_review.provenance["atom_ids"],
        json!([atom_a, atom_b]),
        "independent atom support must accumulate before review"
    );
    let demoted = f
        .upsert(
            false,
            EdgeOrigin::Inferred,
            EdgeStatus::Suggested,
            json!({"rule": "existing-rule", "source_edge_ids": [dependency]}),
        )
        .await;
    assert_eq!(
        demoted.status,
        EdgeStatus::Suggested,
        "machine confirmations remain revisable"
    );
    assert!(demoted.provenance.get("auto_confirmed").is_none());
    f.review(initial.edge_id, EdgeStatus::Confirmed).await;
    let reviewed = store
        .get_relationship_edge(initial.edge_id)
        .await
        .unwrap()
        .unwrap();
    let reviewed_inverse = store
        .get_relationship_edge(reverse.edge_id)
        .await
        .unwrap()
        .unwrap();
    for key in ["reviewed_by", "reviewed_at", "via"] {
        assert_eq!(
            reviewed_inverse.provenance[key], reviewed.provenance[key],
            "inverse review must retain {key}"
        );
    }
    assert!(reviewed_inverse.provenance.get("auto_confirmed").is_none());

    for inverse in [false, true] {
        let edge = f
            .upsert(
                inverse,
                EdgeOrigin::Stated,
                EdgeStatus::Confirmed,
                json!({"atom_ids": [atom_b], "extractor": "second-pass"}),
            )
            .await;
        let replay = f
            .upsert(
                inverse,
                EdgeOrigin::Stated,
                EdgeStatus::Confirmed,
                json!({"atom_ids": [atom_b], "extractor": "second-pass"}),
            )
            .await;
        assert_eq!(
            edge.edge_id,
            if inverse {
                reverse.edge_id
            } else {
                initial.edge_id
            }
        );
        assert_eq!(
            edge.provenance, replay.provenance,
            "replay must not duplicate evidence"
        );
        assert_eq!(replay.provenance["atom_ids"], json!([atom_a, atom_b]));
        for key in ["reviewed_by", "reviewed_at", "via"] {
            assert_eq!(replay.provenance[key], reviewed.provenance[key]);
        }
        if inverse {
            assert_eq!(replay.provenance["derived"], "inverse");
        } else {
            assert_eq!(replay.provenance["source_edge_ids"], json!([dependency]));
            assert_eq!(replay.provenance["rule"], "existing-rule");
        }
        // Machine inference cannot reclassify a human decision as automatic.
        let reevaluated = f.upsert(inverse, EdgeOrigin::Inferred, EdgeStatus::Suggested,
            json!({"atom_ids": [atom_b], "auto_confirmed": true, "rule": "new-rule", "source_edge_ids": [Uuid::nil()]})).await;
        assert_eq!(reevaluated.status, EdgeStatus::Confirmed);
        assert!(reevaluated.provenance.get("auto_confirmed").is_none());
        assert_eq!(reevaluated.provenance["rule"], "new-rule");
        assert_eq!(
            reevaluated.provenance["source_edge_ids"],
            json!([Uuid::nil()])
        );
    }

    f.review(initial.edge_id, EdgeStatus::Rejected).await;
    let rejection = store
        .get_relationship_edge(initial.edge_id)
        .await
        .unwrap()
        .unwrap();
    for inverse in [false, true] {
        let edge = f
            .upsert(
                inverse,
                EdgeOrigin::Stated,
                EdgeStatus::Confirmed,
                json!({"atom_ids": [atom_a], "auto_confirmed": true}),
            )
            .await;
        assert_eq!(edge.status, EdgeStatus::Rejected);
        assert_eq!(edge.provenance["atom_ids"], json!([atom_a, atom_b]));
        assert_eq!(
            edge.provenance["reviewed_at"],
            rejection.provenance["reviewed_at"]
        );
        assert!(edge.provenance.get("auto_confirmed").is_none());
    }
    let edges = store
        .list_relationship_edges_for_nodes(graph, &[src, dst])
        .await
        .unwrap();
    assert_eq!(edges.len(), 2, "retain rejected edges for review/audit");
    for seed in [src, dst] {
        let outcome = hop_scores(
            &HashMap::from([(seed, 1.0)]),
            &edges,
            &HashSet::from([seed]),
            &HopPolicy::default(),
        );
        assert!(
            outcome.next.is_empty(),
            "a rejected assertion must never drive retrieval"
        );
        assert!(outcome.currents.is_empty());
    }
    let ops: Vec<(String, Value, i64)> = sqlx::query_as(
        "SELECT entity_id, payload, version FROM op_log WHERE seq > $1 ORDER BY seq",
    )
    .bind(start_seq)
    .fetch_all(pool)
    .await
    .unwrap();
    for edge in &edges {
        let (_, payload, version) = ops
            .iter()
            .rev()
            .find(|(entity_id, ..)| *entity_id == edge.edge_id.to_string())
            .unwrap();
        assert_eq!(
            payload["provenance"], edge.provenance,
            "the op log must record merged stored truth"
        );
        assert_eq!(payload["status"], "rejected");
        assert_eq!(*version, edge.version);
    }

    // Current extraction suppresses new expertise_of rows, but older rows
    // must still receive the same review on both the scoped and unscoped paths.
    let legacy_forward = store
        .upsert_relationship_edge(
            graph,
            src,
            predicates::EXPERT_IN,
            dst,
            EdgeOrigin::Inferred,
            EdgeStatus::Suggested,
            0.9,
            json!({}),
        )
        .await
        .unwrap();
    f.review(legacy_forward.edge_id, EdgeStatus::Confirmed)
        .await;
    let without_inverse = store
        .list_relationship_edges_for_nodes(graph, &[src, dst])
        .await
        .unwrap();
    assert!(
        !without_inverse
            .iter()
            .any(|edge| edge.predicate == predicates::EXPERTISE_OF),
        "review must never materialize a missing inverse"
    );
    let legacy_inverse = store
        .upsert_relationship_edge(
            graph,
            dst,
            predicates::EXPERTISE_OF,
            src,
            EdgeOrigin::Inferred,
            EdgeStatus::Confirmed,
            0.9,
            json!({"auto_confirmed": true, "derived": "inverse", "atom_ids": [atom_a]}),
        )
        .await
        .unwrap();
    f.review(legacy_forward.edge_id, EdgeStatus::Rejected).await;
    let inverse = store
        .get_relationship_edge(legacy_inverse.edge_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(inverse.status, EdgeStatus::Rejected);
    assert_eq!(inverse.provenance["reviewed_by"], user.to_string());
    assert_eq!(inverse.provenance["atom_ids"], json!([atom_a]));
    assert_eq!(inverse.provenance["derived"], "inverse");
    assert!(inverse.provenance.get("auto_confirmed").is_none());
}

async fn pg_fixture() -> Option<(PgPool, Uuid, Uuid)> {
    let Ok(url) = std::env::var("MEMORY_PROVENANCE_TEST_DATABASE_URL") else {
        eprintln!("skipping PG contract: set MEMORY_PROVENANCE_TEST_DATABASE_URL to a migrated local scratch DB");
        return None;
    };
    let options = PgConnectOptions::from_str(&url).unwrap();
    assert!(
        matches!(options.get_host(), "localhost" | "127.0.0.1" | "::1"),
        "only loopback PostgreSQL is allowed"
    );
    assert_eq!(
        options.get_database(),
        Some("memory_provenance_test"),
        "use the dedicated scratch database"
    );
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect_with(options)
        .await
        .unwrap();
    let user: Uuid = sqlx::query_scalar("INSERT INTO users(email) VALUES ($1) RETURNING id")
        .bind(format!("{}@provenance.test", Uuid::new_v4()))
        .fetch_one(&pool)
        .await
        .unwrap();
    let account: Uuid =
        sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES ($1) RETURNING id")
            .bind(user)
            .fetch_one(&pool)
            .await
            .unwrap();
    Some((pool, user, account))
}

#[tokio::test]
async fn postgres_relationship_provenance_contract() {
    let Some((pool, user, account)) = pg_fixture().await else {
        return;
    };
    let store = PostgresStore::new(pool.clone());
    contract(&store, &pool, account, user, None).await;
    contract(&store, &pool, account, user, Some((&pool, account))).await;
    pool.close().await;
}

#[tokio::test]
async fn postgres_concurrent_supports_are_not_lost() {
    let Some((pool, user, account)) = pg_fixture().await else {
        return;
    };
    let store = PostgresStore::new(pool.clone());
    let graph = store
        .create_graph(account, "Concurrent supports", "personal")
        .await
        .unwrap()
        .id;
    let src = store.upsert_node(graph, "entity", "Manager").await.unwrap();
    let dst = store.upsert_node(graph, "entity", "Report").await.unwrap();
    let f = Fixture {
        store: &store,
        scope: Some((&pool, account)),
        graph,
        src,
        dst,
        user,
    };
    let initial_atom = Uuid::new_v4();
    let edge = f
        .upsert(
            false,
            EdgeOrigin::Stated,
            EdgeStatus::Confirmed,
            json!({"atom_ids": [initial_atom]}),
        )
        .await;
    let mut expected = HashSet::from([initial_atom.to_string()]);
    let mut tasks = Vec::new();
    for _ in 0..12 {
        let atom = Uuid::new_v4();
        expected.insert(atom.to_string());
        let pool = pool.clone();
        tasks.push(tokio::spawn(async move {
            let store = PostgresStore::new(pool.clone());
            let f = Fixture {
                store: &store,
                scope: Some((&pool, account)),
                graph,
                src,
                dst,
                user,
            };
            f.upsert(
                false,
                EdgeOrigin::Stated,
                EdgeStatus::Confirmed,
                json!({"atom_ids": [atom]}),
            )
            .await;
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    let final_edge = store
        .get_relationship_edge(edge.edge_id)
        .await
        .unwrap()
        .unwrap();
    let actual: HashSet<String> = final_edge.provenance["atom_ids"]
        .as_array()
        .unwrap()
        .iter()
        .map(|id| id.as_str().unwrap().to_owned())
        .collect();
    assert_eq!(
        actual, expected,
        "row locking must protect concurrent evidence unions"
    );
    pool.close().await;
}
