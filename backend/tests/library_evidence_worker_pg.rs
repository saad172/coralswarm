//! Real PostgreSQL proofs. Fresh databases and uniquely named app roles avoid
//! mutating another worktree's fixture roles. No optional/quietly skipped tests.
use std::{str::FromStr, time::Duration};

use chrono::{DateTime, Utc};

use coralswarm_backend::{
    library_read::evidence,
    tenancy::scope::{
        begin_account_scoped, begin_tenant_scoped, begin_tenant_system_scoped_without_account,
    },
    workers::library_evidence::{self as worker, Batch, Limits},
};
use serde_json::{json, Value};
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    Connection, Executor, PgConnection, PgPool, Postgres, Transaction,
};
use uuid::Uuid;

#[path = "library_evidence_worker_pg/success_events.rs"]
mod success_events;

// Fixture-only exact dirty-key assertion. Production request admission must
// bound log count and raw fanout BEFORE expanding keys (PR4's overlay gate).
const FIXTURE_DIRTY_NODES_SQL: &str = r#"
SELECT DISTINCT c.graph_id,n.node_id FROM library_evidence_changes c
CROSS JOIN LATERAL library_evidence_change_nodes(c.graph_id,c.id,NULL,$2::bigint::int) n
WHERE c.graph_id=ANY($1::uuid[]) ORDER BY c.graph_id,n.node_id LIMIT $2
"#;

// Read the persisted contract directly. Expected values in the worker tests
// below come from fixture facts, not from the canonical implementation under test.
const ITEM_CONTEXT_SQL: &str = r#"SELECT jsonb_build_object(
 'independent',semantic_independent_version,'synthetic',semantic_synthetic,
 'latest',(semantic_latest_captured_at AT TIME ZONE 'UTC')::text,
 'has_match',entity_type_has_match,'type',entity_type_value,
 'captured',(entity_type_captured_at AT TIME ZONE 'UTC')::text,
 'outcome',entity_type_semantic_atom_id,'ambiguous',entity_type_ambiguous,
 'retained',atom_count_retained)
 FROM library_node_item_evidence WHERE graph_id=$1 AND node_id=$2 AND source_item_id=$3"#;

struct Fixture {
    owner: PgPool,
    app: PgPool,
    admin: PgConnection,
    db: coralswarm_backend::test_db::TestDatabase,
    role: String,
    tenant: bool,
    account: Uuid,
    actor: Uuid,
    graph: Uuid,
}

impl Fixture {
    async fn new(tenant: bool) -> Self {
        let base = std::env::var("ORGT_TEST_BASE_URL")
            .expect("ORGT_TEST_BASE_URL required for worker proof");
        let opts = PgConnectOptions::from_str(&base).unwrap();
        assert!(matches!(opts.get_host(), "localhost" | "127.0.0.1" | "::1"));
        let suffix = Uuid::new_v4().simple().to_string();
        let mut admin = PgConnection::connect_with(&opts).await.unwrap();
        let plane = if tenant {
            coralswarm_backend::test_db::Plane::Tenant
        } else {
            coralswarm_backend::test_db::Plane::Control
        };
        let db = coralswarm_backend::test_db::migrated_database(
            &opts,
            plane,
            if tenant { "codex_rm3_tenant" } else { "codex_rm3_control" },
        )
        .await;
        let role = db.role("codex_rm3_app");
        let owner = PgPoolOptions::new()
            .max_connections(8)
            .connect_with(opts.clone().database(&db))
            .await
            .unwrap();
        // Serialize shared role creation in the immutable migration trees with
        // the same lock used by RlsProbe, without altering/drop shared roles.
        sqlx::query("SELECT pg_advisory_lock($1)")
            .bind(0x0083_5C09_i64)
            .execute(&mut admin)
            .await
            .unwrap();
        // The migration ships the switch On; stamp its barrier, as the
        // worker's first tick does.
        assert!(worker::stamp_listing_barrier(&owner).await.unwrap());
        let parent = if tenant {
            "coralswarm_tenant_app"
        } else {
            "coralswarm_app"
        };
        owner.execute(format!("CREATE ROLE {role} LOGIN INHERIT NOBYPASSRLS PASSWORD 'codex-rm3-disposable-fixture'").as_str()).await.unwrap();
        owner
            .execute(format!("GRANT {parent} TO {role}").as_str())
            .await
            .unwrap();
        sqlx::query("SELECT pg_advisory_unlock($1)")
            .bind(0x0083_5C09_i64)
            .execute(&mut admin)
            .await
            .unwrap();
        // The barrier settles once every older transaction on the server has
        // ended, and the tests in this binary share the server.
        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        while worker::listing_switch_settled(&owner).await.unwrap() != Some(true) {
            assert!(std::time::Instant::now() < deadline, "the flip barrier never settled");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let app = PgPoolOptions::new()
            .max_connections(8)
            .connect_with(
                opts.database(&db)
                    .username(&role)
                    .password("codex-rm3-disposable-fixture"),
            )
            .await
            .unwrap();
        let (actor, account) = if tenant {
            sqlx::query("INSERT INTO tenant_meta(id,org_id) VALUES(true,'org_codex_rm3_fixture')")
                .execute(&owner)
                .await
                .unwrap();
            (Uuid::new_v4(), Uuid::new_v4())
        } else {
            let actor: Uuid =
                sqlx::query_scalar("INSERT INTO users(email) VALUES($1) RETURNING id")
                    .bind(format!("{suffix}@fixture.invalid"))
                    .fetch_one(&owner)
                    .await
                    .unwrap();
            let account: Uuid =
                sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES($1) RETURNING id")
                    .bind(actor)
                    .fetch_one(&owner)
                    .await
                    .unwrap();
            sqlx::query(
                "INSERT INTO account_members(account_id,user_id,role) VALUES($1,$2,'owner')",
            )
            .bind(account)
            .bind(actor)
            .execute(&owner)
            .await
            .unwrap();
            (actor, account)
        };
        let graph: Uuid = sqlx::query_scalar("INSERT INTO graphs(account_id,name,kind,owner_user_id) VALUES($1,'Worker proof','member',$2) RETURNING id").bind(account).bind(actor).fetch_one(&owner).await.unwrap();
        Self {
            owner,
            app,
            admin,
            db,
            role,
            tenant,
            account,
            actor,
            graph,
        }
    }

    async fn tx_on(&self, pool: &PgPool) -> Transaction<'static, Postgres> {
        let mut tx = if self.tenant {
            begin_tenant_system_scoped_without_account(pool)
                .await
                .unwrap()
        } else {
            begin_account_scoped(pool, self.account).await.unwrap()
        };
        sqlx::query(
            "SELECT set_config('app.account_id',$1,true),set_config('app.user_id','',true)",
        )
        .bind(self.account.to_string())
        .execute(&mut *tx)
        .await
        .unwrap();
        tx
    }

    async fn batch(&self, limit: usize) -> Batch {
        let mut tx = self.tx_on(&self.app).await;
        let batch = worker::refresh_graph_tx(
            &mut tx,
            self.graph,
            Limits {
                nodes: limit,
                changes: 32,
            },
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
        assert!(batch.nodes <= limit);
        batch
    }

    async fn ready(&self, limit: usize) {
        for _ in 0..100 {
            if self.batch(limit).await.backfill_completed {
                return;
            }
        }
        panic!("backfill did not finish");
    }

    async fn drain(&self, limit: usize) {
        for _ in 0..1000 {
            let left: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM library_evidence_changes WHERE graph_id=$1",
            )
            .bind(self.graph)
            .fetch_one(&self.owner)
            .await
            .unwrap();
            if left == 0 {
                return;
            }
            self.batch(limit).await;
        }
        panic!("change log did not drain");
    }

    async fn item_context(&self, node: Uuid, item: Uuid) -> Option<Value> {
        sqlx::query_scalar(ITEM_CONTEXT_SQL)
            .bind(self.graph)
            .bind(node)
            .bind(item)
            .fetch_optional(&self.owner)
            .await
            .unwrap()
    }

    async fn node(&self, title: &str) -> Uuid {
        sqlx::query_scalar("INSERT INTO nodes(graph_id,type,title,body) VALUES($1,'entity',$2,'{}') RETURNING node_id").bind(self.graph).bind(title).fetch_one(&self.owner).await.unwrap()
    }

    async fn atom(&self, node: Uuid, text: Option<&str>, participants: &[&str]) -> Uuid {
        let atom:Uuid=sqlx::query_scalar("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,participants,timestamp) VALUES($1,'note',$2,'fixture',$3,$4,now()) RETURNING atom_id")
            .bind(self.graph).bind(Uuid::new_v4().to_string()).bind(text).bind(participants).fetch_one(&self.owner).await.unwrap();
        sqlx::query("INSERT INTO node_atoms(node_id,atom_id) VALUES($1,$2)")
            .bind(node)
            .bind(atom)
            .execute(&self.owner)
            .await
            .unwrap();
        atom
    }

    async fn source(&self, nodes: &[Uuid], mention: Value) -> (Uuid, Uuid, Uuid) {
        let id = Uuid::new_v4().to_string();
        let item:Uuid=sqlx::query_scalar("INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id,access_state) VALUES($1,'note','codex:rm3',$2,'permitted') RETURNING source_item_id").bind(self.graph).bind(&id).fetch_one(&self.owner).await.unwrap();
        let version:Uuid=sqlx::query_scalar("INSERT INTO source_versions(graph_id,source_item_id,source_id,source_type,provider_namespace,raw_inline,source_metadata,content_sha256,extraction_status) VALUES($1,$2,$3,'note','codex:rm3','neutral text','{}',repeat('a',64),'done') RETURNING version_id").bind(self.graph).bind(item).bind(&id).fetch_one(&self.owner).await.unwrap();
        sqlx::query("UPDATE source_items SET current_version_id=$2 WHERE source_item_id=$1")
            .bind(item)
            .bind(version)
            .execute(&self.owner)
            .await
            .unwrap();
        let semantic:Uuid=sqlx::query_scalar("INSERT INTO semantic_atoms(graph_id,source_version_id,ordinal,kind,statement,extraction_version,model,entities) VALUES($1,$2,0,'fact','neutral outcome','source-atoms-v1','mock',$3) RETURNING semantic_atom_id").bind(self.graph).bind(version).bind(mention).fetch_one(&self.owner).await.unwrap();
        for node in nodes {
            sqlx::query("INSERT INTO node_semantic_atoms(graph_id,node_id,semantic_atom_id) VALUES($1,$2,$3)").bind(self.graph).bind(node).bind(semantic).execute(&self.owner).await.unwrap();
        }
        (item, version, semantic)
    }

    async fn correction(&self, node: Uuid, atom: Uuid) -> Uuid {
        let mut tx = self.owner.begin().await.unwrap();
        sqlx::query("SELECT set_config('app.account_id',$1,true),set_config('app.user_id',$2,true),set_config('app.tenant_actor','user',true)").bind(self.account.to_string()).bind(self.actor.to_string()).execute(&mut *tx).await.unwrap();
        let correction:Uuid=sqlx::query_scalar("INSERT INTO reef_identity_corrections(graph_id,node_id,label,recorded_by,support_atom_id,support_kind,content_revision,input_sha256) SELECT a.graph_id,n.node_id,n.title,$3,a.atom_id,'legacy',a.content_revision,reef_identity_atom_fingerprint(a) FROM atoms a JOIN nodes n ON n.node_id=$2 WHERE a.atom_id=$1 RETURNING correction_id").bind(atom).bind(node).bind(self.actor).fetch_one(&mut *tx).await.unwrap();
        tx.commit().await.unwrap();
        correction
    }

    async fn parity(&self, pool: &PgPool, nodes: &[Uuid]) -> Vec<(Uuid, bool)> {
        let mut tx = self.tx_on(pool).await;
        let sql = format!(
            "SELECT node_id,title_visible FROM ({}) e ORDER BY node_id",
            &*evidence::NODE_EVIDENCE_SQL
        );
        let rows: Vec<(Uuid, bool)> = sqlx::query_as(&sql)
            .bind(self.graph)
            .bind(nodes)
            .fetch_all(&mut *tx)
            .await
            .unwrap();
        for (node, visible) in &rows {
            let canonical: bool =
                sqlx::query_scalar("SELECT library_node_title($1,$2) IS NOT NULL")
                    .bind(self.graph)
                    .bind(node)
                    .fetch_one(&mut *tx)
                    .await
                    .unwrap();
            assert_eq!(
                *visible, canonical,
                "independent canonical title parity tenant={} node={node}",
                self.tenant
            );
        }
        tx.rollback().await.unwrap();
        rows
    }

    async fn close(mut self) {
        self.app.close().await;
        self.owner.close().await;
        self.admin
            .execute(format!("DROP DATABASE {} WITH (FORCE)", self.db).as_str())
            .await
            .unwrap();
        self.admin
            .execute(format!("DROP ROLE {}", self.role).as_str())
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn a_definition_bump_restarts_previous_version_rows_and_only_publishes_a_complete_resumed_rebuild(
) {
    // The worker keys every restart on "stamped version != current", so one
    // prior version exercises the whole contract. The immediate predecessor is
    // the cache a production bump actually finds, cursor parked at the end.
    let previous = evidence::READ_MODEL_VERSION - 1;
    assert!(previous >= 1, "a bump must leave a prior definition to restart");
    for tenant in [false, true] {
        let f = Fixture::new(tenant).await;
        let mut nodes = Vec::new();
        for i in 0..3 {
            nodes.push(f.node(&format!("Atlas{i}")).await);
        }
        nodes.sort_unstable();
        let (item, version, outcome) = f
            .source(
                &nodes,
                json!([
                    {"mention":"Atlas0","type":"project"},
                    {"mention":"Atlas1","type":"project"},
                    {"mention":"Atlas2","type":"project"}
                ]),
            )
            .await;
        sqlx::query("UPDATE source_versions SET captured_at='2026-01-01 00:00:00+00',origin='{\"transport\":\"synthetic\"}' WHERE version_id=$1")
            .bind(version).execute(&f.owner).await.unwrap();
        f.ready(3).await;
        f.drain(3).await;
        // Simulate a previously ready cache stamped with the prior definition.
        // Resetting the item columns to their migration defaults is a tracer:
        // a rebuild recomputes them, while relabelling computed_version in
        // place (observationally identical when the stored semantics did not
        // change between definitions) leaves them untouched. A stale cursor at
        // the end of the graph must not resume into the new definition either.
        sqlx::query("UPDATE library_node_item_evidence SET semantic_independent_version=false,semantic_synthetic=false,semantic_latest_captured_at=NULL,entity_type_has_match=false,entity_type_value=NULL,entity_type_captured_at=NULL,entity_type_semantic_atom_id=NULL,entity_type_ambiguous=false WHERE graph_id=$1")
            .bind(f.graph).execute(&f.owner).await.unwrap();
        sqlx::query("UPDATE library_node_evidence SET computed_version=$2 WHERE graph_id=$1")
            .bind(f.graph)
            .bind(previous)
            .execute(&f.owner)
            .await
            .unwrap();
        sqlx::query("UPDATE library_evidence_state SET ready_version=$2,backfill_version=$2,backfill_after_node_id=$3 WHERE graph_id=$1")
            .bind(f.graph).bind(previous).bind(nodes[2]).execute(&f.owner).await.unwrap();
        // A cache from before the listing grain existed has a zero node
        // count; only a recount from the recertified grain refills it.
        sqlx::query("UPDATE library_listing_state SET node_count=0 WHERE graph_id=$1")
            .bind(f.graph).execute(&f.owner).await.unwrap();
        let sentinel: i64 = sqlx::query_scalar(
            "INSERT INTO library_evidence_changes(graph_id,node_id) VALUES($1,$2) RETURNING id",
        )
        .bind(f.graph)
        .bind(nodes[0])
        .fetch_one(&f.owner)
        .await
        .unwrap();
        let expected = json!({"independent":true,"synthetic":true,"latest":"2026-01-01 00:00:00",
            "has_match":true,"type":"project","captured":"2026-01-01 00:00:00","outcome":outcome,"ambiguous":false,"retained":1});
        let defaults = json!({"independent":false,"synthetic":false,"latest":null,"has_match":false,
            "type":null,"captured":null,"outcome":null,"ambiguous":false,"retained":1});
        assert_eq!(f.item_context(nodes[2], item).await, Some(defaults.clone()));
        let first = f.batch(1).await;
        assert_eq!(first.nodes, 1);
        assert!(!first.backfill_completed);
        let state:(Option<i32>,Option<i32>,Option<Uuid>)=sqlx::query_as("SELECT ready_version,backfill_version,backfill_after_node_id FROM library_evidence_state WHERE graph_id=$1")
            .bind(f.graph).fetch_one(&f.owner).await.unwrap();
        assert_eq!(
            state,
            (None, Some(evidence::READ_MODEL_VERSION), Some(nodes[0]))
        );
        assert_eq!(f.item_context(nodes[0], item).await, Some(expected.clone()));
        assert_eq!(f.item_context(nodes[1], item).await, Some(defaults.clone()));
        let versions: Vec<i32> = sqlx::query_scalar(
            "SELECT computed_version FROM library_node_evidence WHERE graph_id=$1 ORDER BY node_id",
        )
        .bind(f.graph)
        .fetch_all(&f.owner)
        .await
        .unwrap();
        assert_eq!(
            versions,
            vec![evidence::READ_MODEL_VERSION, previous, previous],
            "mixed rows cannot publish readiness"
        );
        // A crash/rollback after writing the next page restores both item facts
        // and progress; the next committed tick must retry exactly that page.
        let mut tx = f.tx_on(&f.app).await;
        let retry = worker::refresh_graph_tx(
            &mut tx,
            f.graph,
            Limits {
                nodes: 1,
                changes: 32,
            },
        )
        .await
        .unwrap();
        assert_eq!(retry.nodes, 1);
        let in_tx: Value = sqlx::query_scalar(ITEM_CONTEXT_SQL)
            .bind(f.graph)
            .bind(nodes[1])
            .bind(item)
            .fetch_one(&mut *tx)
            .await
            .unwrap();
        assert_eq!(in_tx, expected);
        tx.rollback().await.unwrap();
        assert_eq!(f.item_context(nodes[1], item).await, Some(defaults));
        let after_rollback: Option<Uuid> = sqlx::query_scalar(
            "SELECT backfill_after_node_id FROM library_evidence_state WHERE graph_id=$1",
        )
        .bind(f.graph)
        .fetch_one(&f.owner)
        .await
        .unwrap();
        assert_eq!(after_rollback, Some(nodes[0]));
        assert!(!f.batch(1).await.backfill_completed);
        let still_unready: Option<i32> = sqlx::query_scalar(
            "SELECT ready_version FROM library_evidence_state WHERE graph_id=$1",
        )
        .bind(f.graph)
        .fetch_one(&f.owner)
        .await
        .unwrap();
        assert_eq!(still_unready, None);
        assert!(f.batch(1).await.backfill_completed);
        let mut tx = f.tx_on(&f.app).await;
        let complete:bool=sqlx::query_scalar("SELECT ready_version=$2 AND computed_account_id=$3 AND computed_context=library_evidence_class(graph_id) AND backfill_after_node_id IS NULL FROM library_evidence_state WHERE graph_id=$1")
            .bind(f.graph).bind(evidence::READ_MODEL_VERSION).bind(f.account).fetch_one(&mut *tx).await.unwrap();
        assert!(complete);
        tx.rollback().await.unwrap();
        for node in &nodes {
            assert_eq!(f.item_context(*node, item).await, Some(expected.clone()));
        }
        let all_current:bool=sqlx::query_scalar("SELECT count(*)=3 AND bool_and(computed_version=$2) FROM library_node_evidence WHERE graph_id=$1")
            .bind(f.graph).bind(evidence::READ_MODEL_VERSION).fetch_one(&f.owner).await.unwrap();
        assert!(all_current);
        let listing_grain: (i64, bool) = sqlx::query_as("SELECT node_count,nodes_complete FROM library_listing_state WHERE graph_id=$1")
            .bind(f.graph).fetch_one(&f.owner).await.unwrap();
        assert_eq!(
            listing_grain,
            (3, true),
            "a rebuild recertifies the node grain, so the listing recounts it; a relabel would leave the count empty"
        );
        let kept: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM library_evidence_changes WHERE id=$1)")
                .bind(sentinel)
                .fetch_one(&f.owner)
                .await
                .unwrap();
        assert!(
            !kept,
            "the sentinel was logged before the rebuild's start snapshot, so every page saw its write: the completed backfill acknowledges it"
        );
        f.drain(1).await;
        let remaining: i64 =
            sqlx::query_scalar("SELECT count(*) FROM library_evidence_changes WHERE graph_id=$1")
                .bind(f.graph)
                .fetch_one(&f.owner)
                .await
                .unwrap();
        assert_eq!(remaining, 0);
        f.close().await;
    }
}

#[tokio::test]
async fn worker_persists_historical_nullable_entity_context_and_replaces_it_atomically_after_mutations(
) {
    for tenant in [false, true] {
        let f = Fixture::new(tenant).await;
        let node = f.node("Atlas").await;
        let (item, old, outcome) = f
            .source(
                &[node],
                json!([{"mention":"Atlas"},{"mention":"ATLAS","type":null}]),
            )
            .await;
        sqlx::query("UPDATE source_versions SET captured_at='2026-01-01 00:00:00+00',origin='{\"transport\":\"synthetic\"}' WHERE version_id=$1")
            .bind(old).execute(&f.owner).await.unwrap();
        let current:Uuid=sqlx::query_scalar("INSERT INTO source_versions(graph_id,source_item_id,source_id,source_type,provider_namespace,raw_inline,source_metadata,content_sha256,extraction_status,captured_at) SELECT graph_id,source_item_id,source_id,source_type,provider_namespace,'unlinked current','{\"independence\":\"derived_copy\"}',repeat('b',64),'done','2026-02-01 00:00:00+00' FROM source_versions WHERE version_id=$1 RETURNING version_id")
            .bind(old).fetch_one(&f.owner).await.unwrap();
        sqlx::query("UPDATE source_items SET current_version_id=$2 WHERE source_item_id=$1")
            .bind(item)
            .bind(current)
            .execute(&f.owner)
            .await
            .unwrap();
        f.ready(1).await;
        f.drain(1).await;
        let mut expected = json!({"independent":true,"synthetic":true,"latest":"2026-01-01 00:00:00",
            "has_match":true,"type":null,"captured":"2026-01-01 00:00:00","outcome":outcome,"ambiguous":false,"retained":1});
        assert_eq!(f.item_context(node,item).await,Some(expected.clone()),"unlinked current metadata must not replace historical linked facts; duplicate NULL types are one possible type");
        sqlx::query("UPDATE source_versions SET captured_at='2026-03-01 00:00:00+00',source_metadata='{\"independence\":\"derived_copy\"}',origin=NULL WHERE version_id=$1")
            .bind(old).execute(&f.owner).await.unwrap();
        let before = expected.clone();
        expected["independent"] = json!(false);
        expected["synthetic"] = json!(false);
        expected["latest"] = json!("2026-03-01 00:00:00");
        expected["captured"] = expected["latest"].clone();
        assert_eq!(
            f.item_context(node, item).await,
            Some(before.clone()),
            "capture only appends dirty keys"
        );
        let mut tx = f.tx_on(&f.app).await;
        worker::refresh_graph_tx(
            &mut tx,
            f.graph,
            Limits {
                nodes: 1,
                changes: 32,
            },
        )
        .await
        .unwrap();
        let in_tx: Value = sqlx::query_scalar(ITEM_CONTEXT_SQL)
            .bind(f.graph)
            .bind(node)
            .bind(item)
            .fetch_one(&mut *tx)
            .await
            .unwrap();
        assert_eq!(in_tx, expected);
        tx.rollback().await.unwrap();
        assert_eq!(
            f.item_context(node, item).await,
            Some(before),
            "rollback restores every persisted v2 field"
        );
        f.drain(1).await;
        assert_eq!(f.item_context(node, item).await, Some(expected.clone()));
        // NULL plus text at the winning tuple is ambiguous. Repeated entries
        // of one text type are safe, and the stored NULL value is distinguished.
        sqlx::query("UPDATE semantic_atoms SET entities=$2 WHERE semantic_atom_id=$1")
            .bind(outcome)
            .bind(json!([{"mention":"Atlas"},{"mention":"Atlas","type":"project"}]))
            .execute(&f.owner)
            .await
            .unwrap();
        f.drain(1).await;
        expected["ambiguous"] = json!(true);
        assert_eq!(f.item_context(node, item).await, Some(expected.clone()));
        sqlx::query("UPDATE semantic_atoms SET entities=$2 WHERE semantic_atom_id=$1")
            .bind(outcome)
            .bind(
                json!([{"mention":"Atlas","type":"project"},{"mention":"ATLAS","type":"project"}]),
            )
            .execute(&f.owner)
            .await
            .unwrap();
        f.drain(1).await;
        expected["type"] = json!("project");
        expected["ambiguous"] = json!(false);
        assert_eq!(f.item_context(node, item).await, Some(expected.clone()));
        // A newer retained linked outcome wins the tuple. Its deletion must
        // promote the old winner without an outcome FK blocking the write.
        sqlx::query(
            "UPDATE source_versions SET captured_at='2026-04-01 00:00:00+00' WHERE version_id=$1",
        )
        .bind(current)
        .execute(&f.owner)
        .await
        .unwrap();
        let winner:Uuid=sqlx::query_scalar("INSERT INTO semantic_atoms(graph_id,source_version_id,ordinal,kind,statement,extraction_version,model,entities) VALUES($1,$2,0,'fact','newer linked outcome','source-atoms-v1','mock',$3) RETURNING semantic_atom_id")
            .bind(f.graph).bind(current).bind(json!([{"mention":"Atlas","type":"custom/type"}])).fetch_one(&f.owner).await.unwrap();
        sqlx::query(
            "INSERT INTO node_semantic_atoms(graph_id,node_id,semantic_atom_id) VALUES($1,$2,$3)",
        )
        .bind(f.graph)
        .bind(node)
        .bind(winner)
        .execute(&f.owner)
        .await
        .unwrap();
        f.drain(1).await;
        expected["latest"] = json!("2026-04-01 00:00:00");
        expected["captured"] = expected["latest"].clone();
        expected["type"] = json!("custom/type");
        expected["outcome"] = json!(winner);
        expected["retained"] = json!(2);
        assert_eq!(f.item_context(node, item).await, Some(expected.clone()));
        sqlx::query("DELETE FROM semantic_atoms WHERE semantic_atom_id=$1")
            .bind(winner)
            .execute(&f.owner)
            .await
            .unwrap();
        f.drain(1).await;
        expected["latest"] = json!("2026-03-01 00:00:00");
        expected["captured"] = expected["latest"].clone();
        expected["type"] = json!("project");
        expected["outcome"] = json!(outcome);
        expected["retained"] = json!(1);
        assert_eq!(f.item_context(node, item).await, Some(expected.clone()));
        sqlx::query("UPDATE nodes SET title='Renamed' WHERE node_id=$1")
            .bind(node)
            .execute(&f.owner)
            .await
            .unwrap();
        f.drain(1).await;
        expected["has_match"] = json!(false);
        expected["type"] = Value::Null;
        expected["captured"] = Value::Null;
        expected["outcome"] = Value::Null;
        assert_eq!(f.item_context(node, item).await, Some(expected.clone()));
        sqlx::query("UPDATE semantic_atoms SET entities=$2 WHERE semantic_atom_id=$1")
            .bind(outcome)
            .bind(json!([{"mention":"Renamed","type":""}]))
            .execute(&f.owner)
            .await
            .unwrap();
        f.drain(1).await;
        expected["has_match"] = json!(true);
        expected["type"] = json!("");
        expected["captured"] = expected["latest"].clone();
        expected["outcome"] = json!(outcome);
        assert_eq!(
            f.item_context(node, item).await,
            Some(expected.clone()),
            "empty text remains distinct from NULL and no match"
        );
        sqlx::query("UPDATE source_items SET access_state='restricted' WHERE source_item_id=$1")
            .bind(item)
            .execute(&f.owner)
            .await
            .unwrap();
        f.drain(1).await;
        assert_eq!(f.item_context(node, item).await, None);
        sqlx::query("UPDATE source_items SET access_state='permitted' WHERE source_item_id=$1")
            .bind(item)
            .execute(&f.owner)
            .await
            .unwrap();
        f.drain(1).await;
        assert_eq!(
            f.item_context(node, item).await,
            Some(expected),
            "grant restores complete v2 facts without a new node link"
        );
        f.close().await;
    }
}

#[tokio::test]
async fn bounded_backfill_and_high_fanout_drain_resume_without_losing_lower_uuid_writes() {
    for tenant in [false, true] {
        let f = Fixture::new(tenant).await;
        let mut nodes = Vec::new();
        for i in 0..9 {
            nodes.push(f.node(&format!("Node {i}")).await);
        }
        let (item, _, _) = f.source(&nodes, json!([])).await;
        // First page is durable, but a rolled-back second page advances nothing.
        let first = f.batch(3).await;
        assert_eq!(first.nodes, 3);
        assert!(!first.backfill_completed);
        let cursor: Option<Uuid> = sqlx::query_scalar(
            "SELECT backfill_after_node_id FROM library_evidence_state WHERE graph_id=$1",
        )
        .bind(f.graph)
        .fetch_one(&f.owner)
        .await
        .unwrap();
        let mut tx = f.tx_on(&f.app).await;
        worker::refresh_graph_tx(
            &mut tx,
            f.graph,
            Limits {
                nodes: 3,
                changes: 32,
            },
        )
        .await
        .unwrap();
        tx.rollback().await.unwrap();
        let retry_cursor: Option<Uuid> = sqlx::query_scalar(
            "SELECT backfill_after_node_id FROM library_evidence_state WHERE graph_id=$1",
        )
        .bind(f.graph)
        .fetch_one(&f.owner)
        .await
        .unwrap();
        assert_eq!(cursor, retry_cursor);
        // Give a node behind the backfill cursor evidence. Its link's trigger
        // log must survive readiness and supply the row the forward-only UUID
        // walk cannot see. (A node with no link is not logged: its evidence is
        // the constant row no reader admits.)
        let lower = Uuid::from_u128(1);
        sqlx::query("INSERT INTO nodes(node_id,graph_id,type,title,body) VALUES($1,$2,'entity','Behind cursor','{}')").bind(lower).bind(f.graph).execute(&f.owner).await.unwrap();
        f.atom(lower, Some("Behind cursor"), &[]).await;
        f.ready(3).await;
        let before: i64 =
            sqlx::query_scalar("SELECT count(*) FROM library_evidence_changes WHERE graph_id=$1")
                .bind(f.graph)
                .fetch_one(&f.owner)
                .await
                .unwrap();
        assert!(before > 0, "backfill must not acknowledge capture logs");
        f.drain(3).await;
        let found: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM library_node_evidence WHERE graph_id=$1 AND node_id=$2)",
        )
        .bind(f.graph)
        .bind(lower)
        .fetch_one(&f.owner)
        .await
        .unwrap();
        assert!(found);
        // Isolate a single high-fanout item key. The log is not consumed at the
        // first page and overlay dirtiness remains the WHOLE item, not its tail.
        let id:i64=sqlx::query_scalar("INSERT INTO library_evidence_changes(graph_id,source_item_id) VALUES($1,$2) RETURNING id").bind(f.graph).bind(item).fetch_one(&f.owner).await.unwrap();
        let partial = f.batch(3).await;
        assert_eq!(partial.nodes, 3);
        assert_eq!(partial.changes_consumed, 0);
        let after: Option<Uuid> = sqlx::query_scalar(
            "SELECT resolved_after_node_id FROM library_evidence_changes WHERE id=$1",
        )
        .bind(id)
        .fetch_one(&f.owner)
        .await
        .unwrap();
        assert!(after.is_some());
        let mut tx = f.tx_on(&f.app).await;
        let dirty: Vec<(Uuid, Uuid)> = sqlx::query_as(FIXTURE_DIRTY_NODES_SQL)
            .bind(&[f.graph][..])
            .bind(100_i64)
            .fetch_all(&mut *tx)
            .await
            .unwrap();
        assert_eq!(
            dirty.len(),
            nodes.len(),
            "partially refreshed item remains dirty in full"
        );
        tx.rollback().await.unwrap();
        // Retry a partially processed key after rollback; both stored grains
        // and its durable cursor must remain at the committed checkpoint.
        let mut tx = f.tx_on(&f.app).await;
        worker::refresh_graph_tx(
            &mut tx,
            f.graph,
            Limits {
                nodes: 3,
                changes: 32,
            },
        )
        .await
        .unwrap();
        tx.rollback().await.unwrap();
        let retried: Option<Uuid> = sqlx::query_scalar(
            "SELECT resolved_after_node_id FROM library_evidence_changes WHERE id=$1",
        )
        .bind(id)
        .fetch_one(&f.owner)
        .await
        .unwrap();
        assert_eq!(after, retried);
        // A new linked node below the item cursor gets its own independent key.
        let earlier = Uuid::from_u128(2);
        sqlx::query("INSERT INTO nodes(node_id,graph_id,type,title,body) VALUES($1,$2,'entity','New linked node','{}')").bind(earlier).bind(f.graph).execute(&f.owner).await.unwrap();
        let semantic: Uuid = sqlx::query_scalar(
            "SELECT semantic_atom_id FROM semantic_atoms WHERE graph_id=$1 LIMIT 1",
        )
        .bind(f.graph)
        .fetch_one(&f.owner)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO node_semantic_atoms(graph_id,node_id,semantic_atom_id) VALUES($1,$2,$3)",
        )
        .bind(f.graph)
        .bind(earlier)
        .bind(semantic)
        .execute(&f.owner)
        .await
        .unwrap();
        f.drain(3).await;
        let count: i64 = sqlx::query_scalar(
            "SELECT source_count FROM library_node_evidence WHERE graph_id=$1 AND node_id=$2",
        )
        .bind(f.graph)
        .bind(earlier)
        .fetch_one(&f.owner)
        .await
        .unwrap();
        assert_eq!(count, 1);
        // Removing the source must delete stale links, even though item lookup
        // now has no live item/version rows to follow.
        sqlx::query("DELETE FROM source_items WHERE source_item_id=$1")
            .bind(item)
            .execute(&f.owner)
            .await
            .unwrap();
        f.drain(3).await;
        let links: i64 =
            sqlx::query_scalar("SELECT count(*) FROM library_node_item_evidence WHERE graph_id=$1")
                .bind(f.graph)
                .fetch_one(&f.owner)
                .await
                .unwrap();
        assert_eq!(links, 0);
        assert_eq!(f.batch(3).await.nodes, 0, "drain is idempotent");
        f.close().await;
    }
}

#[tokio::test]
async fn a_writer_during_refresh_survives_and_a_second_refresher_retries_after_the_graph_lock() {
    for tenant in [false, true] {
        let f = Fixture::new(tenant).await;
        let node = f.node("Before").await;
        f.atom(node, Some("Before After"), &[]).await;
        f.ready(8).await;
        f.drain(8).await;
        // A real title change: a write that changes no evidence input logs nothing.
        sqlx::query("UPDATE nodes SET title='Before After' WHERE node_id=$1")
            .bind(node)
            .execute(&f.owner)
            .await
            .unwrap();
        let old_id: i64 =
            sqlx::query_scalar("SELECT min(id) FROM library_evidence_changes WHERE graph_id=$1")
                .bind(f.graph)
                .fetch_one(&f.owner)
                .await
                .unwrap();
        let gate = 0x4353_524D_3300_i64;
        f.owner.execute(format!("CREATE FUNCTION codex_rm3_gate() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock({gate}); RETURN NEW; END $$").as_str()).await.unwrap();
        f.owner.execute("CREATE TRIGGER codex_rm3_gate BEFORE INSERT ON library_node_evidence FOR EACH ROW EXECUTE FUNCTION codex_rm3_gate()").await.unwrap();
        let mut blocker = f.owner.begin().await.unwrap();
        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(gate)
            .execute(&mut *blocker)
            .await
            .unwrap();
        let mut refresh = f.tx_on(&f.app).await;
        let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut *refresh)
            .await
            .unwrap();
        let graph = f.graph;
        let first = tokio::spawn(async move {
            let report = worker::refresh_graph_tx(
                &mut refresh,
                graph,
                Limits {
                    nodes: 8,
                    changes: 32,
                },
            )
            .await
            .unwrap();
            refresh.commit().await.unwrap();
            report
        });
        tokio::time::timeout(Duration::from_secs(10),async {
            loop {
                let waiting:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND wait_event_type='Lock' AND wait_event='advisory')").bind(pid).fetch_one(&f.owner).await.unwrap();
                if waiting {break;} tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }).await.expect("first refresher reached the cache-write gate");
        sqlx::query("UPDATE nodes SET title='After' WHERE node_id=$1")
            .bind(node)
            .execute(&f.owner)
            .await
            .unwrap();
        let new_id: i64 =
            sqlx::query_scalar("SELECT max(id) FROM library_evidence_changes WHERE graph_id=$1")
                .bind(f.graph)
                .fetch_one(&f.owner)
                .await
                .unwrap();
        assert!(new_id > old_id);
        let second = f.batch(8).await;
        assert!(
            second.locked,
            "second refresher cannot overwrite the in-flight snapshot"
        );
        blocker.commit().await.unwrap();
        first.await.unwrap();
        let pending: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM library_evidence_changes WHERE id=$1)")
                .bind(new_id)
                .fetch_one(&f.owner)
                .await
                .unwrap();
        assert!(
            pending,
            "delete EXACT captured ids, never a watermark range"
        );
        f.drain(8).await;
        let title: String = sqlx::query_scalar(
            "SELECT title256 FROM library_node_evidence WHERE graph_id=$1 AND node_id=$2",
        )
        .bind(f.graph)
        .bind(node)
        .fetch_one(&f.owner)
        .await
        .unwrap();
        assert_eq!(title, "After");
        let mut rr = f.app.begin().await.unwrap();
        rr.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            .await
            .unwrap();
        assert!(
            worker::refresh_graph_tx(&mut rr, f.graph, Limits::default())
                .await
                .is_err(),
            "reject stale pre-lock snapshots"
        );
        rr.rollback().await.unwrap();
        f.close().await;
    }
}

#[tokio::test]
async fn bulk_titles_match_the_independent_canonical_function_and_context_changes_restart_backfill()
{
    for tenant in [false, true] {
        let f = Fixture::new(tenant).await;
        let correction = f.node("Corrected label").await;
        let atom = f
            .atom(
                correction,
                Some("This does not contain the node label."),
                &[],
            )
            .await;
        let correction_id = f.correction(correction, atom).await;
        let fingerprint = f.node("Fingerprint label").await;
        let fingerprint_atom = f
            .atom(fingerprint, Some("Neutral support only."), &[])
            .await;
        f.correction(fingerprint, fingerprint_atom).await;
        let long = "X".repeat(300);
        let full = f.node(&long).await;
        let truncated = f.node(&format!("{}suffix", "Y".repeat(256))).await;
        let (_, old, _) = f
            .source(&[full], json!([{"type":"person","mention":long}]))
            .await;
        f.source(&[truncated], json!([{"mention":"Y".repeat(256)}]))
            .await;
        let malformed = f.node("Malformed").await;
        f.source(&[malformed], json!({"mention":"Malformed"})).await;
        let null = f.node("Null entities").await;
        f.source(&[null], Value::Null).await;
        let legacy = f.node("Legacy title").await;
        f.atom(legacy, Some("Supported LEGACY TITLE substring"), &[])
            .await;
        let participant = f.node("Participant").await;
        f.atom(participant, None, &["PARTICIPANT"]).await;
        let denied = f.node("Denied").await;
        let (denied_item, denied_version, _) =
            f.source(&[denied], json!([{"mention":"Denied"}])).await;
        sqlx::query("UPDATE source_items SET access_state='restricted' WHERE source_item_id=$1")
            .bind(denied_item)
            .execute(&f.owner)
            .await
            .unwrap();
        let denied_unbound = f.node("Denied unbound").await;
        let unbound = f.atom(denied_unbound, Some("Denied unbound"), &[]).await;
        sqlx::query("UPDATE atoms SET source_id=(SELECT source_id||':chunk:9' FROM source_items WHERE source_item_id=$2) WHERE atom_id=$1").bind(unbound).bind(denied_item).execute(&f.owner).await.unwrap();
        let mixed = f.node("Mixed mapping").await;
        let mixed_atom = f.atom(mixed, Some("Mixed mapping"), &[]).await;
        sqlx::query("INSERT INTO source_version_chunks(graph_id,source_version_id,atom_id) VALUES($1,$2,$3),($1,$4,$3)").bind(f.graph).bind(old).bind(mixed_atom).bind(denied_version).execute(&f.owner).await.unwrap();
        let retracted = f.node("Retracted").await;
        let a = f.atom(retracted, Some("Retracted"), &[]).await;
        sqlx::query("UPDATE atoms SET evidence_retracted=true WHERE atom_id=$1")
            .bind(a)
            .execute(&f.owner)
            .await
            .unwrap();
        let derived = f.node("Derived").await;
        let a = f.atom(derived, Some("Derived"), &[]).await;
        sqlx::query("UPDATE atoms SET derived_copy_bound=true WHERE atom_id=$1")
            .bind(a)
            .execute(&f.owner)
            .await
            .unwrap();
        let empty = f.node("No links").await;
        // An older retained version remains valid direct title support even
        // when changed current content has no served outcomes.
        let current:Uuid=sqlx::query_scalar("INSERT INTO source_versions(graph_id,source_item_id,source_id,source_type,provider_namespace,raw_inline,source_metadata,content_sha256,extraction_status) SELECT graph_id,source_item_id,source_id,source_type,provider_namespace,'changed','{}',repeat('b',64),'pending' FROM source_versions WHERE version_id=$1 RETURNING version_id").bind(old).fetch_one(&f.owner).await.unwrap();
        sqlx::query("UPDATE source_items SET current_version_id=$2 WHERE source_item_id=(SELECT source_item_id FROM source_versions WHERE version_id=$1)").bind(old).bind(current).execute(&f.owner).await.unwrap();
        let nodes = [
            correction,
            fingerprint,
            denied_unbound,
            mixed,
            full,
            truncated,
            malformed,
            null,
            legacy,
            participant,
            denied,
            retracted,
            derived,
            empty,
        ];
        for pool in [&f.owner, &f.app] {
            let rows = f.parity(pool, &nodes).await;
            assert_eq!(rows.iter().find(|(id, _)| *id == full).unwrap().1, true);
            assert_eq!(
                rows.iter().find(|(id, _)| *id == truncated).unwrap().1,
                false,
                "never truncate title identity to256"
            );
        }
        // App tenant system + owner account sees correction-only support; on
        // control the actual app account-only context cannot read corrections.
        let app = f.parity(&f.app, &nodes).await;
        assert_eq!(
            app.iter().find(|(id, _)| *id == correction).unwrap().1,
            tenant
        );
        let mut first = f.tx_on(&f.owner).await;
        worker::refresh_graph_tx(
            &mut first,
            f.graph,
            Limits {
                nodes: 2,
                changes: 32,
            },
        )
        .await
        .unwrap();
        first.commit().await.unwrap();
        let owner_context: Value = sqlx::query_scalar(
            "SELECT backfill_context FROM library_evidence_state WHERE graph_id=$1",
        )
        .bind(f.graph)
        .fetch_one(&f.owner)
        .await
        .unwrap();
        f.batch(2).await;
        let app_context: Value = sqlx::query_scalar(
            "SELECT backfill_context FROM library_evidence_state WHERE graph_id=$1",
        )
        .bind(f.graph)
        .fetch_one(&f.owner)
        .await
        .unwrap();
        if tenant {
            // #1563: on the tenant plane the owner and the app role share one
            // access class, so the role switch is not a context change.
            assert_eq!(owner_context, app_context, "owner and app share one tenant access class");
        } else {
            assert_ne!(owner_context, app_context);
        }
        f.ready(2).await;
        f.drain(2).await;
        let mut tx = f.tx_on(&f.app).await;
        let state_ok:bool=sqlx::query_scalar("SELECT computed_context=library_evidence_class(graph_id) FROM library_evidence_state WHERE graph_id=$1").bind(f.graph).fetch_one(&mut *tx).await.unwrap();
        assert!(state_ok);
        tx.rollback().await.unwrap();
        // A correction retirement and a changed support fingerprint both lose
        // support according to the canonical function, without label matching.
        sqlx::query(
            "UPDATE reef_identity_corrections SET state='superseded' WHERE correction_id=$1",
        )
        .bind(correction_id)
        .execute(&f.owner)
        .await
        .unwrap();
        f.parity(&f.owner, &nodes).await;
        f.parity(&f.app, &nodes).await;
        sqlx::query("UPDATE atoms SET content_inline='Fingerprint changed' WHERE atom_id=$1")
            .bind(fingerprint_atom)
            .execute(&f.owner)
            .await
            .unwrap();
        sqlx::query("DELETE FROM node_atoms WHERE node_id=$1")
            .bind(legacy)
            .execute(&f.owner)
            .await
            .unwrap();
        f.parity(&f.owner, &nodes).await;
        f.parity(&f.app, &nodes).await;
        f.drain(2).await;
        f.close().await;
    }
}

#[tokio::test]
async fn a_history_floor_changes_canonical_titles_and_refuses_system_backfill_sharing() {
    let f = Fixture::new(true).await;
    sqlx::query("UPDATE graphs SET kind='team',owner_user_id=NULL WHERE id=$1")
        .bind(f.graph)
        .execute(&f.owner)
        .await
        .unwrap();
    let team:Uuid=sqlx::query_scalar("INSERT INTO teams(account_id,graph_id,name,created_by) VALUES($1,$2,'Window proof',$3) RETURNING id").bind(f.account).bind(f.graph).bind(f.actor).fetch_one(&f.owner).await.unwrap();
    sqlx::query("INSERT INTO team_members(team_id,user_id,role) VALUES($1,$2,'member')")
        .bind(team)
        .bind(f.actor)
        .execute(&f.owner)
        .await
        .unwrap();
    sqlx::query("INSERT INTO team_seat_grants(account_id,team_id,user_id,source,history_from) VALUES($1,$2,$3,'fixture','2026-01-01')").bind(f.account).bind(team).bind(f.actor).execute(&f.owner).await.unwrap();
    let node = f.node("Only old title").await;
    let old = f.atom(node, Some("Only old title"), &[]).await;
    sqlx::query("UPDATE atoms SET timestamp='2025-01-01' WHERE atom_id=$1")
        .bind(old)
        .execute(&f.owner)
        .await
        .unwrap();
    let recent = f.atom(node, Some("Neutral recent evidence."), &[]).await;
    sqlx::query("UPDATE atoms SET timestamp='2026-02-01' WHERE atom_id=$1")
        .bind(recent)
        .execute(&f.owner)
        .await
        .unwrap();
    f.ready(8).await;
    f.drain(8).await;
    let stored: bool = sqlx::query_scalar(
        "SELECT title_visible FROM library_node_evidence WHERE graph_id=$1 AND node_id=$2",
    )
    .bind(f.graph)
    .bind(node)
    .fetch_one(&f.owner)
    .await
    .unwrap();
    assert!(stored);
    let mut request = begin_tenant_scoped(&f.app, f.account, f.actor)
        .await
        .unwrap();
    let sql = format!(
        "SELECT title_visible,unmapped_legacy_count FROM ({}) e",
        &*evidence::NODE_EVIDENCE_SQL
    );
    let (bulk, count): (bool, i64) = sqlx::query_as(&sql)
        .bind(f.graph)
        .bind(&[node][..])
        .fetch_one(&mut *request)
        .await
        .unwrap();
    let canonical: bool = sqlx::query_scalar("SELECT library_node_title($1,$2) IS NOT NULL")
        .bind(f.graph)
        .bind(node)
        .fetch_one(&mut *request)
        .await
        .unwrap();
    assert_eq!(bulk, canonical);
    assert!(!bulk);
    assert_eq!(count, 1);
    let shareable: bool = sqlx::query_scalar("SELECT library_evidence_shareable($1)")
        .bind(f.graph)
        .fetch_one(&mut *request)
        .await
        .unwrap();
    assert!(
        !shareable,
        "a member history floor must use the live reader"
    );
    request.rollback().await.unwrap();
    f.close().await;
}

#[tokio::test]
async fn tenant_worker_backfills_every_member_graph_and_scopes_context_to_its_owner_account() {
    use coralswarm_backend::{
        store::PostgresStore, tenancy::TenantId, workers::tenant_harness::WorkerTenant,
    };
    use std::sync::Arc;
    let f = Fixture::new(true).await;
    let other:Uuid=sqlx::query_scalar("INSERT INTO graphs(account_id,name,kind,owner_user_id) VALUES($1,'Other member','member',$2) RETURNING id").bind(f.account).bind(Uuid::new_v4()).fetch_one(&f.owner).await.unwrap();
    f.node("First member node").await;
    sqlx::query(
        "INSERT INTO nodes(graph_id,type,title,body) VALUES($1,'entity','Other member node','{}')",
    )
    .bind(other)
    .execute(&f.owner)
    .await
    .unwrap();
    let t = WorkerTenant {
        tenant: TenantId::Org("org_codex_rm3_fixture".into()),
        pool: f.app.clone(),
        store: Arc::new(PostgresStore::new(f.app.clone())),
    };
    for _ in 0..8 {
        worker::tick(&t).await.unwrap();
    }
    let ready:i64=sqlx::query_scalar("SELECT count(*) FROM library_evidence_state WHERE ready_version=$1 AND computed_account_id=$2 AND computed_context IS NOT NULL").bind(evidence::READ_MODEL_VERSION).bind(f.account).fetch_one(&f.owner).await.unwrap();
    assert_eq!(
        ready, 2,
        "system worker includes the other member's private graph"
    );
    // An account mismatch fails the shareability gate even on an otherwise
    // authorized/system-visible graph; owning account is an explicit boundary.
    let mut tx = begin_tenant_system_scoped_without_account(&f.app)
        .await
        .unwrap();
    sqlx::query("SELECT set_config('app.account_id',$1,true)")
        .bind(Uuid::new_v4().to_string())
        .execute(&mut *tx)
        .await
        .unwrap();
    let shareable: bool = sqlx::query_scalar("SELECT library_evidence_shareable($1)")
        .bind(f.graph)
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    assert!(!shareable);
    tx.rollback().await.unwrap();
    f.close().await;
}

#[tokio::test]
async fn multigraph_batches_reject_corrupt_cross_graph_legacy_links_at_both_grains() {
    for tenant in [false, true] {
        let f = Fixture::new(tenant).await;
        let own = f.node("Own support").await;
        let (_, version, _) = f.source(&[own], json!([{"mention":"Own support"}])).await;
        let atom = f.atom(own, Some("Own support and Foreign"), &[]).await;
        sqlx::query("INSERT INTO source_version_chunks(graph_id,source_version_id,atom_id) VALUES($1,$2,$3)")
            .bind(f.graph).bind(version).bind(atom).execute(&f.owner).await.unwrap();
        let other:Uuid=sqlx::query_scalar("INSERT INTO graphs(account_id,name,kind,owner_user_id) VALUES($1,'Other graph','member',$2) RETURNING id")
            .bind(f.account).bind(f.actor).fetch_one(&f.owner).await.unwrap();
        let foreign:Uuid=sqlx::query_scalar("INSERT INTO nodes(graph_id,type,title,body) VALUES($1,'entity','Foreign','{}') RETURNING node_id")
            .bind(other).fetch_one(&f.owner).await.unwrap();
        sqlx::query("INSERT INTO node_atoms(node_id,atom_id) VALUES($1,$2)")
            .bind(foreign)
            .bind(atom)
            .execute(&f.owner)
            .await
            .unwrap();
        for pool in [&f.owner, &f.app] {
            let mut tx = f.tx_on(pool).await;
            worker::configure_node_query(&mut tx).await.unwrap();
            let ctes = evidence::node_evidence_ctes(
                "SELECT graph_id,node_id FROM nodes WHERE node_id=ANY($1::uuid[])",
            );
            let pairs: Vec<(Uuid, Uuid)> = sqlx::query_as(&format!(
                "{ctes} SELECT graph_id,node_id FROM node_item_evidence"
            ))
            .bind(vec![own, foreign])
            .fetch_all(&mut *tx)
            .await
            .unwrap();
            assert!(
                !pairs.is_empty(),
                "mapped support must actually be exercised"
            );
            assert!(
                pairs.iter().all(|p| *p == (f.graph, own)),
                "item grain cannot escape requested graph/node pairs: {pairs:?}"
            );
            let counts:Vec<(Uuid,i64,bool)>=sqlx::query_as(&format!("{ctes} SELECT node_id,source_count,admitted_unfiltered FROM node_evidence ORDER BY node_id"))
                .bind(vec![own,foreign]).fetch_all(&mut *tx).await.unwrap();
            assert_eq!(counts.len(), 2);
            assert_eq!(
                counts.iter().find(|r| r.0 == foreign).map(|r| (r.1, r.2)),
                Some((0, false)),
                "foreign atoms cannot support a node in another graph"
            );
            tx.rollback().await.unwrap();
        }
        f.close().await;
    }
}

#[tokio::test]
async fn a_real_write_cannot_turn_the_next_hub_resolution_into_a_full_scan() {
    fn scans<'a>(plan: &'a Value, out: &mut Vec<&'a Value>) {
        out.push(plan);
        if let Some(children) = plan["Plans"].as_array() {
            for child in children {
                scans(child, out);
            }
        }
    }
    // Explain the exact replacement body as well as the opaque actual function:
    // the latter includes function planning and the unchanged RLS visibility set.
    let migration = include_str!("../migrations_tenant/0190_library_evidence_permission_plan.sql");
    let body = migration
        .split("CREATE OR REPLACE FUNCTION public.library_evidence_change_nodes")
        .nth(1)
        .unwrap()
        .split(" AS $$")
        .nth(1)
        .unwrap()
        .split("$$;")
        .next()
        .unwrap()
        .replace("graph_id=g", "graph_id=$1")
        .replace("change_id", "$2")
        .replace("after_node", "$3")
        .replace("node_limit", "$4");
    let explain = format!("EXPLAIN(ANALYZE,BUFFERS,FORMAT JSON) {body}");
    for tenant in [false, true] {
        let f = Fixture::new(tenant).await;
        let direct = f.node("Direct first").await;
        let atom = f.atom(direct, Some("Direct first"), &[]).await;
        let prefix = Uuid::new_v4().to_string();
        let key: i64 = sqlx::query_scalar(
            "INSERT INTO library_evidence_changes(graph_id,atom_id) VALUES($1,$2) RETURNING id",
        )
        .bind(f.graph)
        .bind(atom)
        .fetch_one(&f.owner)
        .await
        .unwrap();
        for (first, last, fanout) in [(1_i32, 127_i32, 128_i32), (128, 20000, 20001)] {
            sqlx::query("WITH inserted AS (INSERT INTO nodes(graph_id,node_id,type,title,body) SELECT $1,md5($2::text||i::text)::uuid,'entity','Hub link '||i::text,'{}' FROM generate_series($4::int,$5::int) i RETURNING node_id) INSERT INTO node_atoms(node_id,atom_id) SELECT node_id,$3 FROM inserted")
                .bind(f.graph).bind(&prefix).bind(atom).bind(first).bind(last).execute(&f.owner).await.unwrap();
            for mode in ["force_custom_plan", "force_generic_plan"] {
                let mut tx = f.tx_on(&f.app).await;
                sqlx::query("SELECT set_config('plan_cache_mode',$1,true)")
                    .bind(mode)
                    .execute(&mut *tx)
                    .await
                    .unwrap();
                assert!(worker::lock_graph(&mut tx, f.graph).await.unwrap());
                assert_eq!(
                    worker::store_nodes(&mut tx, f.graph, &[direct])
                        .await
                        .unwrap(),
                    1
                );
                // Deliberately hostile caller settings must not escape into
                // the indexed resolver, and must be restored after it returns.
                sqlx::query("SELECT set_config('enable_indexscan','off',true),set_config('enable_indexonlyscan','off',true)").execute(&mut *tx).await.unwrap();
                let plan:Value=sqlx::query_scalar("EXPLAIN(ANALYZE,BUFFERS,FORMAT JSON) SELECT node_id FROM library_evidence_change_nodes($1,$2,NULL,65)")
                    .bind(f.graph).bind(key).fetch_one(&mut *tx).await.unwrap();
                assert_eq!(plan[0]["Plan"]["Actual Rows"].as_f64(), Some(65.));
                let blocks = plan[0]["Plan"]["Shared Hit Blocks"].as_u64().unwrap()
                    + plan[0]["Plan"]["Shared Read Blocks"].as_u64().unwrap();
                let elapsed = plan[0]["Execution Time"].as_f64().unwrap();
                assert!(
                    elapsed < 200.,
                    "bounded fixture probe milliseconds={elapsed}: {plan}"
                );
                assert_eq!(
                    worker::change_nodes(&mut tx, f.graph, key, None, 65)
                        .await
                        .unwrap()
                        .len(),
                    65
                );
                let settings: (String, String, String, String, String, String, String) = sqlx::query_as(
                    "SELECT current_setting('enable_nestloop'),current_setting('enable_mergejoin'),current_setting('enable_seqscan'),current_setting('enable_bitmapscan'),current_setting('enable_indexscan'),current_setting('enable_indexonlyscan'),current_setting('enable_sort')",
                )
                .fetch_one(&mut *tx)
                .await
                .unwrap();
                assert_eq!(
                    settings,
                    (
                        "off".into(),
                        "off".into(),
                        "on".into(),
                        "on".into(),
                        "off".into(),
                        "off".into(),
                        "off".into()
                    ),
                    "function-local policy restores all caller planner settings"
                );
                sqlx::query("SELECT set_config('enable_nestloop','on',true),set_config('enable_mergejoin','on',true),set_config('enable_seqscan','off',true),set_config('enable_bitmapscan','off',true),set_config('enable_indexscan','on',true),set_config('enable_indexonlyscan','on',true),set_config('enable_sort','on',true),set_config('search_path','public,pg_temp',true)").execute(&mut *tx).await.unwrap();
                let inner: Value = sqlx::query_scalar(&explain)
                    .bind(f.graph)
                    .bind(key)
                    .bind(None::<Uuid>)
                    .bind(65_i32)
                    .fetch_one(&mut *tx)
                    .await
                    .unwrap();
                let mut parts = Vec::new();
                scans(&inner[0]["Plan"], &mut parts);
                let inverse: Vec<_> = parts
                    .iter()
                    .filter(|p| p["Relation Name"] == "node_atoms")
                    .collect();
                assert!(!inverse.is_empty());
                let visited: f64 = inverse
                    .iter()
                    .map(|p| {
                        p["Actual Rows"].as_f64().unwrap() * p["Actual Loops"].as_f64().unwrap()
                    })
                    .sum();
                let bounded_input = if fanout == 20001 { 65. } else { 128. };
                if fanout == 20001 {
                    assert_eq!(
                        visited, 65.,
                        "ordered inverse scan cannot read the complete high-fanout hub: {inner}"
                    );
                } else {
                    assert!(
                        visited <= bounded_input,
                        "a tiny scope may choose its bounded128-row sequential plan"
                    );
                }
                let pk: Vec<_> = parts
                    .iter()
                    .filter(|p| {
                        p["Relation Name"] == "nodes"
                            && p["Index Cond"]
                                .as_str()
                                .is_some_and(|c| c.contains("a.node_id"))
                    })
                    .collect();
                if fanout == 20001 {
                    assert!(!pk.is_empty(), "high-fanout visibility uses indexed probes");
                }
                let probes: f64 = pk.iter().map(|p| p["Actual Loops"].as_f64().unwrap()).sum();
                if fanout == 20001 {
                    assert_eq!(
                        probes, 65.,
                        "one graph/visibility key probe per resolved high-fanout candidate"
                    );
                } else {
                    assert!(probes <= bounded_input);
                }
                for sort in parts.iter().filter(|p| p["Node Type"] == "Sort") {
                    assert!(sort["Actual Rows"].as_f64().unwrap() <= bounded_input);
                    for input in sort["Plans"].as_array().unwrap() {
                        assert!(
                            input["Actual Rows"].as_f64().unwrap() <= bounded_input,
                            "fanout cannot be sorted before LIMIT: {inner}"
                        );
                    }
                }
                let visible: f64 = parts
                    .iter()
                    .filter(|p| {
                        p["Relation Name"] == "nodes"
                            && !p["Index Cond"]
                                .as_str()
                                .is_some_and(|c| c.contains("a.node_id"))
                    })
                    .map(|p| {
                        p["Actual Rows"].as_f64().unwrap_or(0.)
                            * p["Actual Loops"].as_f64().unwrap_or(0.)
                    })
                    .sum();
                eprintln!("RM3_BOUNDED_RESOLVER tenant={tenant} mode={mode} links={fanout} returned=65 inverse_rows={visited} pk_probes={probes} policy_visible_nodes={visible} opaque_blocks={blocks} execution_ms={elapsed}");
                tx.rollback().await.unwrap();
            }
        }
        f.close().await;
    }
}

#[tokio::test]
async fn permission_wrapper_preserves_invoker_scope_and_restores_planner_settings() {
    for tenant in [false, true] {
        let f = Fixture::new(tenant).await;
        let node = f.node("Visible atom").await;
        let atom = f.atom(node, Some("Visible atom"), &[]).await;
        let denied = f.node("Retracted atom").await;
        let hidden = f.atom(denied, Some("Retracted atom"), &[]).await;
        sqlx::query("UPDATE atoms SET evidence_retracted=true WHERE atom_id=$1")
            .bind(hidden)
            .execute(&f.owner)
            .await
            .unwrap();
        let (_, _, outcome) = f
            .source(
                &[node],
                json!([{"type":"project","mention":"Visible atom"}]),
            )
            .await;
        for pool in [&f.owner, &f.app] {
            let mut tx = f.tx_on(pool).await;
            worker::configure_node_query(&mut tx).await.unwrap();
            for ids in [None, Some(vec![atom, hidden])] {
                let differs:bool=sqlx::query_scalar("SELECT EXISTS((SELECT atom_id FROM library_evidence_permitted_atoms($1,$2) EXCEPT SELECT atom_id FROM library_permitted_atoms($1,$2)) UNION ALL (SELECT atom_id FROM library_permitted_atoms($1,$2) EXCEPT SELECT atom_id FROM library_evidence_permitted_atoms($1,$2)))")
                    .bind(f.graph).bind(ids).fetch_one(&mut *tx).await.unwrap();
                assert!(
                    !differs,
                    "same canonical permission result under owner/app scope"
                );
            }
            let served_differs:bool=sqlx::query_scalar("SELECT EXISTS((SELECT semantic_atom_id FROM library_evidence_served_outcomes($1,ARRAY[$2]::uuid[]) EXCEPT SELECT semantic_atom_id FROM served_semantic_atoms WHERE graph_id=$1 AND semantic_atom_id=$2) UNION ALL (SELECT semantic_atom_id FROM served_semantic_atoms WHERE graph_id=$1 AND semantic_atom_id=$2 EXCEPT SELECT semantic_atom_id FROM library_evidence_served_outcomes($1,ARRAY[$2]::uuid[])))")
                .bind(f.graph).bind(outcome).fetch_one(&mut *tx).await.unwrap();
            assert!(!served_differs,"served-view wrapper preserves the independent canonical view under both invoker roles");
            let (nested, merge, sort): (String, String, String) = sqlx::query_as(
                "SELECT current_setting('enable_nestloop'),current_setting('enable_mergejoin'),current_setting('enable_sort')",
            )
            .fetch_one(&mut *tx)
            .await
            .unwrap();
            assert_eq!(
                (nested.as_str(), merge.as_str(), sort.as_str()),
                ("off", "off", "off"),
                "function-local settings restored to caller"
            );
            let blocked: Vec<Uuid> = sqlx::query_scalar(
                "SELECT atom_id FROM library_evidence_permitted_atoms($1,ARRAY[$2]::uuid[])",
            )
            .bind(f.graph)
            .bind(hidden)
            .fetch_all(&mut *tx)
            .await
            .unwrap();
            assert!(
                blocked.is_empty(),
                "wrapper cannot resurrect retracted evidence"
            );
            tx.commit().await.unwrap();
            let settings: (String, String, String) = sqlx::query_as(
                "SELECT current_setting('enable_nestloop'),current_setting('enable_mergejoin'),current_setting('enable_sort')",
            )
            .fetch_one(pool)
            .await
            .unwrap();
            assert_eq!(
                settings,
                ("on".into(), "on".into(), "on".into()),
                "transaction-local planner policy cannot leak to another borrower"
            );
        }
        f.close().await;
    }
}

#[tokio::test]
async fn failed_graph_and_account_do_not_starve_healthy_private_graphs() {
    use coralswarm_backend::{
        store::PostgresStore, tenancy::TenantId, workers::tenant_harness::WorkerTenant,
    };
    use std::sync::Arc;
    for tenant in [false, true] {
        let f = Fixture::new(tenant).await;
        f.node("Failing graph").await;
        let (account, actor) = if tenant {
            (f.account, f.actor)
        } else {
            let actor: Uuid =
                sqlx::query_scalar("INSERT INTO users(email) VALUES($1) RETURNING id")
                    .bind(format!("{}@fixture.invalid", Uuid::new_v4()))
                    .fetch_one(&f.owner)
                    .await
                    .unwrap();
            let account: Uuid =
                sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES($1) RETURNING id")
                    .bind(actor)
                    .fetch_one(&f.owner)
                    .await
                    .unwrap();
            sqlx::query(
                "INSERT INTO account_members(account_id,user_id,role) VALUES($1,$2,'owner')",
            )
            .bind(account)
            .bind(actor)
            .execute(&f.owner)
            .await
            .unwrap();
            (account, actor)
        };
        // A second graph in the failing account proves graph fairness; a
        // different Consumer account also proves attempt-based account rotation.
        let mut healthy = Vec::new();
        for (owner, graph_actor) in [(f.account, f.actor), (account, actor)] {
            let graph:Uuid=sqlx::query_scalar("INSERT INTO graphs(account_id,name,kind,owner_user_id) VALUES($1,'Healthy private graph','member',$2) RETURNING id")
                .bind(owner).bind(graph_actor).fetch_one(&f.owner).await.unwrap();
            sqlx::query(
                "INSERT INTO nodes(graph_id,type,title,body) VALUES($1,'entity','Healthy','{}')",
            )
            .bind(graph)
            .execute(&f.owner)
            .await
            .unwrap();
            healthy.push(graph);
        }
        f.owner.execute(format!("CREATE FUNCTION fixture_fail_one_graph() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN IF NEW.graph_id='{}'::uuid THEN RAISE EXCEPTION 'deliberate graph failure'; END IF; RETURN NEW; END$$; CREATE TRIGGER fixture_fail_one_graph BEFORE INSERT ON library_node_evidence FOR EACH ROW EXECUTE FUNCTION fixture_fail_one_graph()",f.graph).as_str()).await.unwrap();
        let t = WorkerTenant {
            tenant: if tenant {
                TenantId::Org(format!("org_fairness_{}", f.graph))
            } else {
                TenantId::Consumer
            },
            pool: f.app.clone(),
            store: Arc::new(PostgresStore::new(f.app.clone())),
        };
        let mut failures = 0;
        for _ in 0..16 {
            if worker::tick(&t).await.is_err() {
                failures += 1;
            }
        }
        assert!(
            failures > 0,
            "the permanently failing graph was actually attempted"
        );
        let ready:Vec<Uuid>=sqlx::query_scalar("SELECT graph_id FROM library_evidence_state WHERE ready_version=$1 AND graph_id=ANY($2)")
            .bind(evidence::READ_MODEL_VERSION).bind(&healthy).fetch_all(&f.owner).await.unwrap();
        assert_eq!(
            ready.len(),
            healthy.len(),
            "tenant={tenant}: failed work cannot starve healthy private graphs/accounts"
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM library_evidence_state WHERE graph_id=$1"
            )
            .bind(f.graph)
            .fetch_one(&f.owner)
            .await
            .unwrap(),
            0,
            "failed refresh rolled back its readiness"
        );
        f.close().await;
    }
}

#[tokio::test]
async fn granting_a_previously_denied_item_resolves_semantic_and_mapped_legacy_nodes() {
    for tenant in [false, true] {
        let f = Fixture::new(tenant).await;
        let semantic_node = f.node("Semantic grant").await;
        let legacy_node = f.node("Legacy grant").await;
        let (item, version, _) = f
            .source(&[semantic_node], json!([{"mention":"Semantic grant"}]))
            .await;
        sqlx::query("UPDATE source_items SET access_state='restricted' WHERE source_item_id=$1")
            .bind(item)
            .execute(&f.owner)
            .await
            .unwrap();
        // Start with denied evidence. Restricting an already permitted bound
        // chunk intentionally retracts it durably, which a grant must NEVER
        // resurrect; that is a different permission invariant.
        let atom = f.atom(legacy_node, Some("Legacy grant"), &[]).await;
        sqlx::query("INSERT INTO source_version_chunks(graph_id,source_version_id,atom_id) VALUES($1,$2,$3)").bind(f.graph).bind(version).bind(atom).execute(&f.owner).await.unwrap();
        let retracted: bool =
            sqlx::query_scalar("SELECT evidence_retracted FROM atoms WHERE atom_id=$1")
                .bind(atom)
                .fetch_one(&f.owner)
                .await
                .unwrap();
        assert!(
            !retracted,
            "initially denied fixture must not be durably purged evidence"
        );
        f.ready(8).await;
        f.drain(8).await;
        let before:i64=sqlx::query_scalar("SELECT count(*) FROM library_node_item_evidence WHERE graph_id=$1 AND source_item_id=$2").bind(f.graph).bind(item).fetch_one(&f.owner).await.unwrap();
        assert_eq!(before, 0);
        sqlx::query("UPDATE source_items SET access_state='permitted' WHERE source_item_id=$1")
            .bind(item)
            .execute(&f.owner)
            .await
            .unwrap();
        let mut tx = f.tx_on(&f.app).await;
        let dirty: Vec<(Uuid, Uuid)> = sqlx::query_as(FIXTURE_DIRTY_NODES_SQL)
            .bind(&[f.graph][..])
            .bind(100_i64)
            .fetch_all(&mut *tx)
            .await
            .unwrap();
        assert!(
            dirty.contains(&(f.graph, semantic_node)),
            "grant must resolve live semantic links absent from stored grain"
        );
        assert!(
            dirty.contains(&(f.graph, legacy_node)),
            "grant must resolve live chunk mappings absent from stored grain"
        );
        tx.rollback().await.unwrap();
        f.drain(1).await;
        let counts: Vec<i64> = sqlx::query_scalar(
            "SELECT source_count FROM library_node_evidence WHERE graph_id=$1 ORDER BY node_id",
        )
        .bind(f.graph)
        .fetch_all(&f.owner)
        .await
        .unwrap();
        assert_eq!(counts, vec![1, 1]);
        let links:i64=sqlx::query_scalar("SELECT count(*) FROM library_node_item_evidence WHERE graph_id=$1 AND source_item_id=$2").bind(f.graph).bind(item).fetch_one(&f.owner).await.unwrap();
        assert_eq!(links, 2);
        f.close().await;
    }
}

/// Explicitly selected operator-only mode for the preserved v1 cache. This
/// mode has no migration, reset, synthetic-log, benchmark or manual state SQL.
/// If initial128 stops after only one graph commits, retain every receipt and
/// stop for review. Do not reset progress, rerun initial128 over that graph, or
/// reconstruct the missing graph's initial evidence to force a resume.
struct PreservedTransition {
    host: String,
    phase: String,
    max_batches: usize,
}

impl PreservedTransition {
    fn from_env() -> Option<Self> {
        let host = match std::env::var("CODEX_RM3_NEON_TRANSITION_ONLY") {
            Ok(host) => host,
            Err(std::env::VarError::NotPresent) => {
                assert!(
                    [
                        "CODEX_RM3_NEON_TRANSITION_SOURCE",
                        "CODEX_RM3_NEON_TRANSITION_PHASE",
                        "CODEX_RM3_NEON_TRANSITION_MAX_BATCHES"
                    ]
                    .iter()
                    .all(|key| std::env::var_os(key).is_none()),
                    "partial transition arguments must not fall through to the legacy benchmark"
                );
                return None;
            }
            Err(std::env::VarError::NotUnicode(_)) => panic!("transition mode must be valid text"),
        };
        assert_eq!(
            host, "ep-wild-bonus-aw0nio2i.c-12.us-east-1.aws.neon.tech",
            "transition is restricted to the explicitly owned clone endpoint"
        );
        assert_eq!(
            std::env::var("CODEX_RM3_NEON_EXPECTED_HOST")
                .ok()
                .as_deref(),
            Some(host.as_str())
        );
        assert_eq!(
            std::env::var("CODEX_RM3_NEON_REUSE_OWNED").ok().as_deref(),
            Some(host.as_str())
        );
        assert!(
            std::env::var_os("CODEX_RM3_NEON_RUN_BACKFILL").is_none(),
            "preserved transition cannot select the reset benchmark"
        );
        let source = env!("CORALSWARM_GIT_SHA_FULL");
        assert!(
            source.len() == 40 && source.bytes().all(|b| b.is_ascii_hexdigit()),
            "compiled source must be a full git SHA"
        );
        assert_eq!(
            std::env::var("CODEX_RM3_NEON_TRANSITION_SOURCE")
                .ok()
                .as_deref(),
            Some(source),
            "exact compiled source must be explicitly selected"
        );
        assert_eq!(evidence::READ_MODEL_VERSION, 2);
        let phase =
            std::env::var("CODEX_RM3_NEON_TRANSITION_PHASE").expect("transition phase required");
        assert!(
            matches!(phase.as_str(), "initial128" | "resume128" | "resume2000"),
            "unknown transition phase"
        );
        let max_batches = std::env::var("CODEX_RM3_NEON_TRANSITION_MAX_BATCHES")
            .expect("explicit per-graph batch budget required")
            .parse::<usize>()
            .expect("batch budget must be an integer");
        assert!((1..=2000).contains(&max_batches));
        if phase == "initial128" {
            assert_eq!(
                max_batches, 1,
                "initial default128 evidence is one committed page per graph"
            );
        }
        Some(Self {
            host,
            phase,
            max_batches,
        })
    }
}

async fn preserved_transition_snapshot(pool: &PgPool, graph: Uuid, account: Uuid) -> Value {
    use sha2::{Digest, Sha256};
    let mut tx = begin_tenant_system_scoped_without_account(pool)
        .await
        .unwrap();
    sqlx::query("SELECT set_config('app.account_id',$1,true),set_config('app.user_id','',true),set_config('statement_timeout','10s',true)")
        .bind(account.to_string()).execute(&mut *tx).await.unwrap();
    let mut snapshot:Value=sqlx::query_scalar(r#"SELECT jsonb_build_object(
      'observed_at',clock_timestamp(),'postmaster_start_time',pg_postmaster_start_time(),'role',current_user,
      'graph_id',$1,'account_id',$3,'reader_context',library_evidence_context($1),
      'work_mem',current_setting('work_mem'),'node_count',(SELECT count(*) FROM nodes WHERE graph_id=$1),
      'node_rows',(SELECT count(*) FROM library_node_evidence WHERE graph_id=$1),
      'node_rows_v2',(SELECT count(*) FROM library_node_evidence WHERE graph_id=$1 AND computed_version=$2),
      'item_rows',(SELECT count(*) FROM library_node_item_evidence WHERE graph_id=$1),
      'semantic_context_rows',(SELECT count(*) FROM library_node_item_evidence WHERE graph_id=$1 AND semantic_latest_captured_at IS NOT NULL),
      'semantic_link_rows',(SELECT count(*) FROM library_node_item_evidence WHERE graph_id=$1 AND semantic_link),
      'entity_match_rows',(SELECT count(*) FROM library_node_item_evidence WHERE graph_id=$1 AND entity_type_has_match),
      'entity_ambiguous_rows',(SELECT count(*) FROM library_node_item_evidence WHERE graph_id=$1 AND entity_type_ambiguous),
      'state',(SELECT jsonb_build_object('ready_version',ready_version,'backfill_version',backfill_version,
        'cursor',backfill_after_node_id,'account_matches',computed_account_id=$3,
        'context_matches',computed_context=library_evidence_context($1),
        'backfill_account_matches',backfill_account_id=$3,
        'backfill_context_matches',backfill_context=library_evidence_context($1))
        FROM library_evidence_state WHERE graph_id=$1))"#)
        .bind(graph).bind(evidence::READ_MODEL_VERSION).bind(account).fetch_one(&mut *tx).await.unwrap();
    let ids: Vec<i64> = sqlx::query_scalar(
        "SELECT id FROM library_evidence_changes WHERE graph_id=$1 ORDER BY id LIMIT 10001",
    )
    .bind(graph)
    .fetch_all(&mut *tx)
    .await
    .unwrap();
    assert!(ids.len()<=10000,"pending-log inventory exceeds proof cap; report incomplete instead of claiming preservation");
    snapshot["pending_ids_sha256"] = json!(format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&ids).unwrap())
    ));
    snapshot["pending_ids"] = json!(ids);
    tx.rollback().await.unwrap();
    snapshot
}

fn write_transition_receipt(path: &std::path::Path, value: &Value) {
    use std::io::Write;
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options
        .open(path)
        .expect("preserve prior receipt; choose a fresh invocation directory");
    file.write_all(&serde_json::to_vec_pretty(value).unwrap())
        .unwrap();
}

async fn run_preserved_transition(pool: &PgPool, mode: &PreservedTransition) {
    use std::time::Instant;
    let dir = std::path::PathBuf::from(
        std::env::var("CODEX_RM3_NEON_ARTIFACT_DIR")
            .expect("private transition artifact directory required"),
    );
    assert!(dir.is_absolute());
    std::fs::create_dir_all(&dir).unwrap();
    let invocation = Uuid::new_v4();
    let mut invocation_work = 0usize;
    // These are the documented graph and the actual largest measured graph;
    // never substitute an easier graph or silently widen operator scope.
    for graph in [
        Uuid::parse_str("ac9befa1-28bc-45d4-bedd-c1497e3b1dc8").unwrap(),
        Uuid::parse_str("95bba458-fd11-4993-8c59-f17192d3338c").unwrap(),
    ] {
        let account: Uuid = sqlx::query_scalar("SELECT account_id FROM graphs WHERE id=$1")
            .bind(graph)
            .fetch_one(pool)
            .await
            .unwrap();
        let before = preserved_transition_snapshot(pool, graph, account).await;
        let total = before["node_count"].as_u64().unwrap();
        assert!(total > 0);
        assert_eq!(
            before["node_rows"].as_u64(),
            Some(total),
            "existing complete v1 cache must be retained"
        );
        assert!(before["item_rows"].as_u64().unwrap() > 0);
        let initial_path = dir.join(format!("{graph}-transition-initial128.json"));
        let original_ids: Vec<i64>;
        if mode.phase == "initial128" {
            original_ids = serde_json::from_value(before["pending_ids"].clone()).unwrap();
            assert_eq!(before["state"]["account_matches"], json!(true));
            assert_eq!(before["state"]["context_matches"], json!(true));
            assert!(
                !initial_path.exists(),
                "initial transition evidence already exists; resume rather than overwrite"
            );
            assert_eq!(
                before["state"]["ready_version"],
                json!(1),
                "initial transition requires actual ready v1 state"
            );
            assert_eq!(
                before["node_rows_v2"],
                json!(0),
                "do not label an already rebuilt cache as fresh transition"
            );
        } else {
            let initial: Value = serde_json::from_slice(
                &std::fs::read(&initial_path)
                    .expect("same-source default128 initial evidence required"),
            )
            .unwrap();
            assert_eq!(
                initial["source_sha"],
                json!(env!("CORALSWARM_GIT_SHA_FULL"))
            );
            assert_eq!(
                initial["definition_version"],
                json!(evidence::READ_MODEL_VERSION)
            );
            assert_eq!(
                initial["model_sql_sha256"],
                json!(coralswarm_backend::library_read::read_model::definition_sha256())
            );
            original_ids =
                serde_json::from_value(initial["before"]["pending_ids"].clone()).unwrap();
            assert_eq!(
                initial["kind"],
                json!("preserved_cache_actual_worker_transition")
            );
            assert_eq!(initial["graph_id"], json!(graph));
            assert_eq!(initial["account_id"], json!(account));
            assert_eq!(initial["node_limit"], json!(128));
            assert_eq!(initial["max_batches"], json!(1));
            for boundary in ["before", "after"] {
                assert_eq!(initial[boundary]["graph_id"], json!(graph));
                assert_eq!(initial[boundary]["account_id"], json!(account));
                assert_eq!(initial[boundary]["role"], before["role"]);
                assert_eq!(
                    initial[boundary]["reader_context"],
                    before["reader_context"]
                );
            }
            assert_eq!(initial["before"]["state"]["account_matches"], json!(true));
            assert_eq!(initial["before"]["state"]["context_matches"], json!(true));
            assert_eq!(
                initial["after"]["state"]["backfill_account_matches"],
                json!(true)
            );
            assert_eq!(
                initial["after"]["state"]["backfill_context_matches"],
                json!(true)
            );
            assert_eq!(initial["before"]["node_count"], before["node_count"]);
            assert_eq!(
                initial["before"]["item_rows"], before["item_rows"],
                "the additive transition cannot change the stored link grain"
            );
            assert_eq!(initial["host"], json!(mode.host));
            assert_eq!(initial["phase"], json!("initial128"));
            assert_eq!(initial["nodes_recomputed"], json!(128));
            assert!(initial["after"]["state"]["ready_version"].is_null());
            assert_eq!(
                initial["after"]["state"]["backfill_version"],
                json!(evidence::READ_MODEL_VERSION)
            );
            assert!(!initial["after"]["state"]["cursor"].is_null());
            assert!(
                before["state"]["ready_version"] == json!(evidence::READ_MODEL_VERSION)
                    || (before["state"]["ready_version"].is_null()
                        && before["state"]["backfill_version"]
                            == json!(evidence::READ_MODEL_VERSION)),
                "resume requires actual preserved v2 progress"
            );
        }
        write_transition_receipt(
            &dir.join(format!(
                "{graph}-transition-{}-{invocation}-before.json",
                mode.phase
            )),
            &json!({"source_sha":env!("CORALSWARM_GIT_SHA_FULL"),"phase":mode.phase,"host":mode.host,"before":before}),
        );
        let limit = if mode.phase == "resume2000" {
            2000
        } else {
            128
        };
        let mut after = before.clone();
        let mut batches = Vec::new();
        let mut work = 0usize;
        let already_ready = before["state"]["ready_version"] == json!(evidence::READ_MODEL_VERSION);
        let mut became_ready_before_batch = false;
        if !already_ready {
            for index in 0..mode.max_batches {
                let start = Instant::now();
                let report=tokio::time::timeout(Duration::from_secs(35),async {
                    let mut tx=begin_tenant_system_scoped_without_account(pool).await.unwrap();
                    // Reuse the real graph lock, not a parallel lock-key formula.
                    // READ COMMITTED observes any preceding worker's commit only
                    // after this lock is held. Never enter its steady-state drain.
                    assert!(worker::lock_graph(&mut tx,graph).await.unwrap(),"another worker owns the graph; stop without writes");
                    let ready:Option<i32>=sqlx::query_scalar("SELECT ready_version FROM library_evidence_state WHERE graph_id=$1")
                        .bind(graph).fetch_one(&mut *tx).await.unwrap();
                    if ready==Some(evidence::READ_MODEL_VERSION) {tx.rollback().await.unwrap();return None;}
                    let report=worker::refresh_graph_tx(&mut tx,graph,Limits{nodes:limit,changes:32}).await.unwrap();
                    assert_eq!(report.changes_consumed,0,"rollback rather than commit any steady-state drain");
                    assert!(!report.locked && report.nodes>0 && report.nodes<=limit,"rollback non-progress or out-of-bound work");
                    let settings:Option<Value>=if index==0 {Some(sqlx::query_scalar("SELECT jsonb_build_object('role',current_user,'postmaster_start_time',pg_postmaster_start_time(),'statement_timeout',current_setting('statement_timeout'),'work_mem',current_setting('work_mem'),'plan_cache_mode',current_setting('plan_cache_mode'),'jit',current_setting('jit'),'enable_nestloop',current_setting('enable_nestloop'),'enable_mergejoin',current_setting('enable_mergejoin'),'enable_sort',current_setting('enable_sort'))").fetch_one(&mut *tx).await.unwrap())} else {None};
                    tx.commit().await.unwrap();Some((report,settings))
                }).await.expect("actual worker batch exceeded35s; stop, retain durable progress");
                let Some((report, settings)) = report else {
                    became_ready_before_batch = true;
                    break;
                };
                work += report.nodes;
                batches.push(json!({"batch":index+1,"node_limit":limit,"nodes":report.nodes,"changes_consumed":report.changes_consumed,
                    "backfill_completed":report.backfill_completed,"wall_ms":start.elapsed().as_secs_f64()*1000.,
                    "wall_includes_guc_readback":index==0,"actual_tx_settings":settings}));
                write_transition_receipt(
                    &dir.join(format!(
                        "{graph}-transition-{}-{invocation}-batch{}.json",
                        mode.phase,
                        index + 1
                    )),
                    batches.last().unwrap(),
                );
                if report.backfill_completed {
                    break;
                }
            }
            after = preserved_transition_snapshot(pool, graph, account).await;
        }
        let before_ids: Vec<i64> = serde_json::from_value(before["pending_ids"].clone()).unwrap();
        let after_ids: Vec<i64> = serde_json::from_value(after["pending_ids"].clone()).unwrap();
        let completed = batches.iter().any(|b| b["backfill_completed"] == json!(true));
        if completed {
            // The completed backfill acknowledges the rows its start snapshot
            // covers, a bounded batch per refresh, and nothing else: every id
            // still pending was pending before, and the first batch went.
            assert!(
                after_ids.iter().all(|id| before_ids.binary_search(id).is_ok() || original_ids.contains(id)),
                "a completed backfill may only drop ids, never add them"
            );
            assert!(
                original_ids.is_empty() || original_ids.iter().any(|id| after_ids.binary_search(id).is_err()),
                "the completed backfill acknowledged none of the ids it covered"
            );
        } else {
            assert!(
                before_ids
                    .iter()
                    .chain(original_ids.iter())
                    .all(|id| after_ids.binary_search(id).is_ok()),
                "an unfinished backfill lost a prior dirty ID"
            );
        }
        assert_eq!(
            after["node_count"].as_u64(),
            Some(total),
            "unexpected source graph cardinality change needs a separate proof"
        );
        assert_eq!(after["node_rows"].as_u64(), Some(total));
        assert_eq!(
            after["item_rows"], before["item_rows"],
            "the version-only transition preserves the exact item link grain"
        );
        let complete = after["state"]["ready_version"] == json!(evidence::READ_MODEL_VERSION);
        if complete {
            assert!(
                after["item_rows"].as_u64().unwrap() > 0,
                "both stored grains must remain nonempty"
            );
            // Each semantic item link contains an actual retained outcome whose
            // source capture is NOT NULL. These two measured graphs have such
            // links; zero/default semantic context is not a completed v2 rebuild.
            assert!(after["semantic_link_rows"].as_u64().unwrap() > 0);
            assert_eq!(after["semantic_context_rows"], after["semantic_link_rows"]);
            assert_eq!(after["node_rows_v2"].as_u64(), Some(total));
            assert!(after["state"]["cursor"].is_null());
            assert_eq!(after["state"]["account_matches"], json!(true));
            assert_eq!(after["state"]["context_matches"], json!(true));
        } else {
            assert!(after["state"]["ready_version"].is_null());
            assert_eq!(
                after["state"]["backfill_version"],
                json!(evidence::READ_MODEL_VERSION)
            );
            assert_eq!(after["state"]["backfill_account_matches"], json!(true));
            assert_eq!(after["state"]["backfill_context_matches"], json!(true));
            assert!(!after["state"]["cursor"].is_null());
        }
        if mode.phase == "initial128" {
            assert_eq!(work, 128);
            assert!(!complete);
            assert_eq!(after["node_rows_v2"], json!(128));
        }
        invocation_work += work;
        let receipt = json!({"kind":"preserved_cache_actual_worker_transition","source_sha":env!("CORALSWARM_GIT_SHA_FULL"),
            "definition_version":evidence::READ_MODEL_VERSION,"model_sql_sha256":coralswarm_backend::library_read::read_model::definition_sha256(),
            "invocation":invocation,"host":mode.host,"phase":mode.phase,"graph_id":graph,"account_id":account,
            "node_limit":limit,"max_batches":mode.max_batches,"nodes_recomputed":work,"already_ready":already_ready,
            "became_ready_before_batch":became_ready_before_batch,
            "complete":complete,"before":before,"after":after,"batches":batches});
        let path = if mode.phase == "initial128" {
            initial_path
        } else {
            dir.join(format!(
                "{graph}-transition-{}-{invocation}.json",
                mode.phase
            ))
        };
        write_transition_receipt(&path, &receipt);
        eprintln!("RM3_NEON transition graph={graph} phase={} nodes={work} complete={complete} already_ready={already_ready}",mode.phase);
    }
    assert!(
        invocation_work > 0,
        "a no-op against already-ready rows is not a new transition proof"
    );
}

#[tokio::test]
#[ignore = "requires explicitly authorized disposable Neon branch"]
async fn isolated_neon_complete_evidence_and_resumable_backfill_benchmark() {
    use std::time::Instant;
    // Parse all transition selectors before connecting and before any legacy
    // schema/reset/benchmark action. Invalid explicit mode never falls through.
    let transition = PreservedTransition::from_env();
    let dsn = std::env::var("CODEX_RM3_NEON_DSN").expect("disposable branch DSN required");
    let expected = std::env::var("CODEX_RM3_NEON_EXPECTED_HOST")
        .expect("independently verified branch endpoint required");
    let opts = PgConnectOptions::from_str(&dsn).unwrap();
    assert_eq!(opts.get_host(), expected);
    assert!(expected.ends_with(".neon.tech") && !expected.contains("-pooler"));
    assert!(
        !expected.starts_with("ep-patient-band-aw7tllvy"),
        "production primary forbidden"
    );
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect_with(opts)
        .await
        .unwrap();
    let fingerprint: (String, i64) =
        sqlx::query_as("SELECT org_id,(SELECT count(*) FROM nodes) FROM tenant_meta WHERE id=true")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        fingerprint.0, "org_2ExampleTargetOrg0000000000",
        "independent tenant fingerprint"
    );
    assert!(
        fingerprint.1 > 60_000,
        "expected complete production-shaped tenant, not empty fixture"
    );
    let existing: bool =
        sqlx::query_scalar("SELECT to_regclass('library_node_evidence') IS NOT NULL")
            .fetch_one(&pool)
            .await
            .unwrap();
    if let Some(mode) = transition {
        assert_eq!(expected, mode.host);
        assert!(
            existing,
            "transition requires preserved cache; it never creates schema"
        );
        run_preserved_transition(&pool, &mode).await;
        pool.close().await;
        return;
    }
    if existing {
        assert_eq!(
            std::env::var("CODEX_RM3_NEON_REUSE_OWNED").ok().as_deref(),
            Some(expected.as_str()),
            "existing schema requires explicit verified owned-clone reuse"
        );
    } else {
        let mut migration = pool.begin().await.unwrap();
        sqlx::query("SELECT set_config('statement_timeout','120s',true)")
            .execute(&mut *migration)
            .await
            .unwrap();
        sqlx::raw_sql(include_str!(
            "../migrations_tenant/0185_library_evidence.sql"
        ))
        .execute(&mut *migration)
        .await
        .unwrap();
        migration.commit().await.unwrap();
        sqlx::raw_sql(include_str!(
            "../migrations_tenant/0186_library_node_atoms_inverse.sql"
        ))
        .execute(&pool)
        .await
        .unwrap();
        sqlx::raw_sql(include_str!(
            "../migrations_tenant/0187_library_node_semantic_inverse.sql"
        ))
        .execute(&pool)
        .await
        .unwrap();
    }
    // The snapshot predates PR5; apply its additive one-statement concurrent
    // indexes too, so the later query proof uses the actual main schema.
    for index in [
        include_str!("../migrations_tenant/0188_library_semantic_atoms_statement_trgm.sql"),
        include_str!("../migrations_tenant/0189_library_semantic_atom_passages_quote_trgm.sql"),
    ] {
        pool.execute(index).await.unwrap();
    }
    // Reapply our idempotent additive planner boundary on this verified owned
    // sandbox, including metadata added after an earlier diagnostic run.
    pool.execute(include_str!(
        "../migrations_tenant/0190_library_evidence_permission_plan.sql"
    ))
    .await
    .unwrap();
    let mut graphs: Vec<Uuid> =
        sqlx::query_scalar("SELECT id FROM graphs WHERE id::text LIKE 'ac9befa1%'")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(
        graphs.len(),
        1,
        "documented production-shape ocean must still be measured"
    );
    for sql in [
        "SELECT graph_id FROM nodes GROUP BY graph_id ORDER BY count(*) DESC LIMIT 1",
        "SELECT graph_id FROM semantic_atoms GROUP BY graph_id ORDER BY count(*) DESC LIMIT 1",
        "SELECT graph_id FROM node_semantic_atoms GROUP BY graph_id ORDER BY count(*) DESC LIMIT 1",
        "SELECT n.graph_id FROM node_atoms l JOIN nodes n ON n.node_id=l.node_id GROUP BY n.graph_id ORDER BY count(*) DESC LIMIT 1",
    ] {
        let graph:Uuid=sqlx::query_scalar(sql).fetch_one(&pool).await.unwrap();if !graphs.contains(&graph){graphs.push(graph);}
    }
    let plan_dir = std::path::PathBuf::from(
        std::env::var("CODEX_RM3_NEON_ARTIFACT_DIR").expect("plan artifact dir required"),
    );
    std::fs::create_dir_all(&plan_dir).unwrap();
    async fn cache_snapshot(pool: &PgPool, graph: Uuid, account: Uuid) -> Value {
        let mut tx = begin_tenant_system_scoped_without_account(pool)
            .await
            .unwrap();
        sqlx::query("SELECT set_config('app.account_id',$1,true)")
            .bind(account.to_string())
            .execute(&mut *tx)
            .await
            .unwrap();
        let result: Value = sqlx::query_scalar(r#"SELECT jsonb_build_object(
            'node_rows',(SELECT count(*) FROM library_node_evidence WHERE graph_id=$1),
            'item_rows',(SELECT count(*) FROM library_node_item_evidence WHERE graph_id=$1),
            'pending_changes',(SELECT count(*) FROM library_evidence_changes WHERE graph_id=$1),
            'state',(SELECT jsonb_build_object('ready_version',ready_version,'backfill_version',backfill_version,
                'backfill_after_node_id',backfill_after_node_id,'account_matches',computed_account_id=$2,
                'context_matches',computed_context=library_evidence_context($1)) FROM library_evidence_state WHERE graph_id=$1))"#)
            .bind(graph).bind(account).fetch_one(&mut *tx).await.unwrap();
        tx.rollback().await.unwrap();
        result
    }
    eprintln!(
        "RM3_NEON runtime_work_mem={}",
        sqlx::query_scalar::<_, String>("SHOW work_mem")
            .fetch_one(&pool)
            .await
            .unwrap()
    );
    eprintln!(
        "RM3_NEON tenant_nodes={} graph_targets={} server={}",
        fingerprint.1,
        graphs.len(),
        sqlx::query_scalar::<_, String>("SELECT current_setting('server_version')")
            .fetch_one(&pool)
            .await
            .unwrap()
    );
    for graph in graphs {
        let (account,total,outcomes):(Uuid,i64,i64)=sqlx::query_as("SELECT account_id,(SELECT count(*) FROM nodes WHERE graph_id=$1),(SELECT count(*) FROM semantic_atoms WHERE graph_id=$1) FROM graphs WHERE id=$1").bind(graph).fetch_one(&pool).await.unwrap();
        assert!(
            total >= 2000 && outcomes > 0,
            "no-data/zero comparisons are not performance proof"
        );
        let before = cache_snapshot(&pool, graph, account).await;
        std::fs::write(
            plan_dir.join(format!("{graph}-cache-before.json")),
            serde_json::to_vec_pretty(&before).unwrap(),
        )
        .unwrap();
        eprintln!("RM3_NEON cache_before graph={graph} metadata={before}");
        if std::env::var("CODEX_RM3_NEON_RUN_BACKFILL").ok().as_deref() == Some(expected.as_str()) {
            let mut tx = begin_tenant_system_scoped_without_account(&pool)
                .await
                .unwrap();
            let prior = sqlx::query("DELETE FROM library_node_item_evidence WHERE graph_id=$1")
                .bind(graph)
                .execute(&mut *tx)
                .await
                .unwrap()
                .rows_affected();
            sqlx::query("DELETE FROM library_node_evidence WHERE graph_id=$1")
                .bind(graph)
                .execute(&mut *tx)
                .await
                .unwrap();
            sqlx::query("DELETE FROM library_evidence_state WHERE graph_id=$1")
                .bind(graph)
                .execute(&mut *tx)
                .await
                .unwrap();
            tx.commit().await.unwrap();
            eprintln!("RM3_NEON reset_owned_test_cache graph={graph} prior_item_rows={prior}");
        }
        if std::env::var("CODEX_RM3_NEON_RUN_BACKFILL").ok().as_deref() == Some(expected.as_str()) {
            let reset = cache_snapshot(&pool, graph, account).await;
            assert_eq!(reset["node_rows"], json!(0));
            assert_eq!(reset["item_rows"], json!(0));
            assert!(reset["state"].is_null());
            assert_eq!(
                reset["pending_changes"],
                json!(0),
                "do not erase unexpected sandbox dirty work"
            );
            std::fs::write(
                plan_dir.join(format!("{graph}-cache-reset.json")),
                serde_json::to_vec_pretty(&reset).unwrap(),
            )
            .unwrap();
        }
        // Logged after the backfill's first page (so after its start
        // snapshot), this key must survive every later page and the
        // completed backfill's purge, then be acknowledged by a real,
        // nonzero dirty refresh.
        let mut sentinel: Option<i64> = None;
        eprintln!("RM3_NEON graph={} nodes={total} outcomes={outcomes}", graph);
        for (name,selection) in [
            ("uuid","SELECT node_id FROM nodes WHERE graph_id=$1 ORDER BY node_id LIMIT $2"),
            ("hub","WITH counts AS (SELECT node_id,count(*) c FROM node_semantic_atoms WHERE graph_id=$1 GROUP BY node_id UNION ALL SELECT l.node_id,count(*) FROM node_atoms l JOIN nodes n ON n.node_id=l.node_id WHERE n.graph_id=$1 GROUP BY l.node_id), totals AS(SELECT node_id,sum(c) c FROM counts GROUP BY node_id) SELECT n.node_id FROM nodes n LEFT JOIN totals t ON t.node_id=n.node_id WHERE n.graph_id=$1 ORDER BY COALESCE(t.c,0) DESC,n.node_id LIMIT $2"),
        ] {
            for size in [1_i64,128,2000] {
                let nodes:Vec<Uuid>=sqlx::query_scalar(selection).bind(graph).bind(size).fetch_all(&pool).await.unwrap();assert_eq!(nodes.len(),size as usize);
                let combined=format!("{} SELECT jsonb_build_object('nodes',(SELECT jsonb_agg(to_jsonb(n)) FROM node_evidence n),'items',(SELECT jsonb_agg(to_jsonb(i)) FROM node_item_evidence i))",evidence::node_evidence_ctes(evidence::NODE_BATCH_SQL));
                for mode in ["force_custom_plan","force_generic_plan"] {
                    for (kind,sql) in [("node",evidence::NODE_EVIDENCE_SQL.as_str()),("item",evidence::NODE_ITEM_EVIDENCE_SQL.as_str()),("combined",combined.as_str()),("write",worker::write_sql_for_explain())] {
                        // Repeat the exact node API for the documented/actual
                        // UUID 2k populations under both plan-cache modes. Keep
                        // first observations and repeat timings separately.
                        for repeat in 0..if kind=="node" && name=="uuid" && size==2000 {3} else {1} {
                        let mut tx=begin_tenant_system_scoped_without_account(&pool).await.unwrap();
                        sqlx::query("SELECT set_config('app.account_id',$1,true),set_config('statement_timeout','120s',true),set_config('jit','off',true),set_config('plan_cache_mode',$2,true)").bind(account.to_string()).bind(mode).execute(&mut *tx).await.unwrap();
                        assert!(worker::lock_graph(&mut tx,graph).await.unwrap());
                        worker::configure_node_query(&mut tx).await.unwrap();
                        let query=format!("EXPLAIN(ANALYZE,BUFFERS,SETTINGS,FORMAT JSON) {sql}");let start=Instant::now();
                        let mut q=sqlx::query_scalar(&query).bind(graph).bind(&nodes);
                        if kind=="write" {q=q.bind(evidence::READ_MODEL_VERSION);}
                        let plan:Value=q.fetch_one(&mut *tx).await.unwrap();
                        let wall=start.elapsed().as_secs_f64()*1000.;
                        if kind=="node" {assert_eq!(plan[0]["Plan"]["Actual Rows"].as_f64(),Some(size as f64),"every selected node and all flags must be evaluated");}
                        if kind=="item" && size==2000 {assert!(plan[0]["Plan"]["Actual Rows"].as_f64().unwrap()>0.,"zero item comparison is not proof");}
                        let stem=format!("{graph}-{name}-{size}-{mode}-{kind}-r{repeat}");
                        std::fs::write(plan_dir.join(format!("{stem}.sql")),sql).unwrap();
                        std::fs::write(plan_dir.join(format!("{stem}-plan.json")),serde_json::to_vec_pretty(&plan).unwrap()).unwrap();
                        eprintln!("RM3_NEON evidence graph={graph} selection={name} nodes={size} mode={mode} kind={kind} repeat={repeat} wall_ms={wall:.1} execution_ms={}",plan[0]["Execution Time"]);
                        tx.rollback().await.unwrap();
                        }
                    }
                }
            }
        }
        if std::env::var("CODEX_RM3_NEON_RUN_BACKFILL").ok().as_deref() != Some(expected.as_str()) {
            continue;
        }
        let previous: i64 =
            sqlx::query_scalar("SELECT count(*) FROM library_node_evidence WHERE graph_id=$1")
                .bind(graph)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(
            previous, 0,
            "fresh full-backfill timing requires empty owned cache"
        );
        let start = Instant::now();
        let mut batches = 0;
        let mut nodes = 0;
        let mut maximum = 0_f64;
        // Measure sixteen real default128 pages, then finish this same durable
        // cursor using operator2000. The scheduling projection is explicitly a
        // single-graph estimate, excluding other graph/account turns, discovery,
        // optional progress observation and harness/pool contention.
        let mut default_ms = 0_f64;
        let mut default_max_ms = 0_f64;
        loop {
            let limit = if batches < 16 { 128 } else { 2000 };
            let mut tx = begin_tenant_system_scoped_without_account(&pool)
                .await
                .unwrap();
            let page = Instant::now();
            let report = worker::refresh_graph_tx(
                &mut tx,
                graph,
                Limits {
                    nodes: limit,
                    changes: 32,
                },
            )
            .await
            .unwrap();
            tx.commit().await.unwrap();
            let elapsed = page.elapsed().as_secs_f64() * 1000.;
            maximum = maximum.max(elapsed);
            batches += 1;
            nodes += report.nodes;
            if batches == 1 {
                let id: i64 = sqlx::query_scalar("INSERT INTO library_evidence_changes(graph_id,node_id) SELECT $1,node_id FROM nodes WHERE graph_id=$1 ORDER BY node_id LIMIT 1 RETURNING id").bind(graph).fetch_one(&pool).await.unwrap();
                sentinel = Some(id);
            }
            if batches <= 16 {
                default_ms += elapsed;
                default_max_ms = default_max_ms.max(elapsed);
                eprintln!("RM3_NEON default_page graph={graph} batch={batches} nodes={} wall_ms={elapsed:.1}",report.nodes);
                if batches == 16 {
                    let estimated_ticks = (total + 127) / 128;
                    let projected_ms = estimated_ticks as f64
                        * (default_ms / 16. + worker::INTERVAL.as_secs_f64() * 1000.);
                    eprintln!("RM3_NEON default_sample graph={graph} pages=16 nodes=2048 total_wall_ms={default_ms:.1} max_page_ms={default_max_ms:.1} single_graph_estimated_ticks={estimated_ticks} estimate_plus_2s_cadence_ms={projected_ms:.1} excludes_other_graphs_and_harness_overhead=true");
                }
            }
            if batches % 10 == 0 {
                eprintln!(
                    "RM3_NEON backfill_progress graph={graph} nodes={nodes} batches={batches}"
                );
            }
            if report.backfill_completed {
                break;
            }
            assert!(
                batches < (total as usize / 128) + 3,
                "bounded backfill made no progress"
            );
        }
        let stored: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM library_node_evidence WHERE graph_id=$1 AND computed_version=$2",
        )
        .bind(graph)
        .bind(evidence::READ_MODEL_VERSION)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(stored, total);
        assert_eq!(nodes as i64, total, "every node was genuinely recomputed");
        let ready = cache_snapshot(&pool, graph, account).await;
        assert_eq!(
            ready["state"]["ready_version"],
            json!(evidence::READ_MODEL_VERSION)
        );
        assert_eq!(ready["state"]["account_matches"], json!(true));
        assert_eq!(ready["state"]["context_matches"], json!(true));
        assert!(ready["state"]["backfill_after_node_id"].is_null());
        assert!(
            ready["item_rows"].as_u64().unwrap() > 0,
            "nonzero second grain required"
        );
        assert_eq!(
            ready["pending_changes"],
            json!(1),
            "a key logged after the backfill's start snapshot survives its purge"
        );
        std::fs::write(
            plan_dir.join(format!("{graph}-cache-backfilled.json")),
            serde_json::to_vec_pretty(&ready).unwrap(),
        )
        .unwrap();
        eprintln!("RM3_NEON backfill graph={graph} nodes={nodes} batches={batches} total_ms={:.1} max_batch_ms={maximum:.1} limits=16x128_then2000",start.elapsed().as_secs_f64()*1000.);
        let sentinel = sentinel.unwrap();
        assert!(sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM library_evidence_changes WHERE graph_id=$1 AND id=$2)"
        )
        .bind(graph)
        .bind(sentinel)
        .fetch_one(&pool)
        .await
        .unwrap());
        let mut tx = begin_tenant_system_scoped_without_account(&pool)
            .await
            .unwrap();
        let drained = worker::refresh_graph_tx(&mut tx, graph, Limits::default())
            .await
            .unwrap();
        assert_eq!(drained.nodes, 1);
        assert_eq!(drained.changes_consumed, 1);
        tx.commit().await.unwrap();
        assert!(!sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM library_evidence_changes WHERE graph_id=$1 AND id=$2)"
        )
        .bind(graph)
        .bind(sentinel)
        .fetch_one(&pool)
        .await
        .unwrap());
        let complete = cache_snapshot(&pool, graph, account).await;
        assert_eq!(complete["pending_changes"], json!(0));
        assert_eq!(complete["node_rows"], json!(total));
        assert_eq!(complete["state"]["context_matches"], json!(true));
        std::fs::write(
            plan_dir.join(format!("{graph}-cache-drained.json")),
            serde_json::to_vec_pretty(&complete).unwrap(),
        )
        .unwrap();
        eprintln!(
            "RM3_NEON dirty_drain graph={graph} nodes={} consumed={} metadata={complete}",
            drained.nodes, drained.changes_consumed
        );
    }
    pool.close().await;
}

async fn pending(f: &Fixture) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM library_evidence_changes WHERE graph_id=$1")
        .bind(f.graph)
        .fetch_one(&f.owner)
        .await
        .unwrap()
}

async fn row_exists(f: &Fixture, id: i64) -> bool {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM library_evidence_changes WHERE graph_id=$1 AND id=$2)")
        .bind(f.graph)
        .bind(id)
        .fetch_one(&f.owner)
        .await
        .unwrap()
}

/// The node budget of one turn is shared: a key whose fan-out exceeds it
/// advances its cursor by its share and the keys logged behind it still get
/// the rest, instead of waiting for every page of the large key.
#[tokio::test]
async fn a_large_fanout_key_cannot_block_the_keys_logged_behind_it() {
    for tenant in [false, true] {
        let f = Fixture::new(tenant).await;
        let mut hub = Vec::new();
        for i in 0..9 {
            hub.push(f.node(&format!("Hub {i}")).await);
        }
        let (item, _, _) = f.source(&hub, json!([])).await;
        f.ready(16).await;
        f.drain(16).await;
        // The queue: the item key (nine nodes), then one node key behind it.
        let big: i64 = sqlx::query_scalar("INSERT INTO library_evidence_changes(graph_id,source_item_id) VALUES($1,$2) RETURNING id")
            .bind(f.graph).bind(item).fetch_one(&f.owner).await.unwrap();
        let small = f.node("Behind the hub").await;
        f.atom(small, Some("Behind the hub"), &[]).await;
        let small_row: i64 = sqlx::query_scalar("SELECT max(id) FROM library_evidence_changes WHERE graph_id=$1 AND node_id=$2")
            .bind(f.graph).bind(small).fetch_one(&f.owner).await.unwrap();
        assert!(small_row > big);
        let turn = f.batch(3).await;
        assert_eq!(turn.nodes, 3, "the whole node budget is spent");
        assert!(!row_exists(&f, small_row).await, "the small key behind the hub was resolved and acknowledged in the same turn");
        assert_eq!(turn.changes_consumed, 1);
        let stored: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM library_node_evidence WHERE graph_id=$1 AND node_id=$2 AND computed_version=$3)")
            .bind(f.graph).bind(small).bind(evidence::READ_MODEL_VERSION).fetch_one(&f.owner).await.unwrap();
        assert!(stored);
        let cursor: Option<Uuid> = sqlx::query_scalar("SELECT resolved_after_node_id FROM library_evidence_changes WHERE graph_id=$1 AND id=$2")
            .bind(f.graph).bind(big).fetch_one(&f.owner).await.unwrap();
        let mut walk = hub.clone();
        walk.sort();
        assert_eq!(cursor, Some(walk[1]), "the item key kept its row, its share of two nodes in");
        f.drain(3).await;
        f.close().await;
    }
}

/// A key logged many times (an item re-extracted logs its item key once per
/// statement) is walked once per turn, and every row read for it is
/// acknowledged by that one walk. Keys beyond the turn's key budget wait.
#[tokio::test]
async fn duplicate_keys_are_resolved_once_and_acknowledged_together() {
    for tenant in [false, true] {
        let f = Fixture::new(tenant).await;
        let mut nodes = Vec::new();
        for i in 0..3 {
            nodes.push(f.node(&format!("Item node {i}")).await);
        }
        let (item, _, _) = f.source(&nodes, json!([])).await;
        f.ready(16).await;
        f.drain(16).await;
        sqlx::query("INSERT INTO library_evidence_changes(graph_id,source_item_id) SELECT $1,$2 FROM generate_series(1,50)")
            .bind(f.graph).bind(item).execute(&f.owner).await.unwrap();
        let turn = f.batch(10).await;
        assert_eq!(turn.nodes, 3, "one walk of the item's three nodes, not fifty");
        assert_eq!(turn.changes_consumed, 50, "that one walk acknowledges every row of the key");
        assert_eq!(pending(&f).await, 0);
        // Forty distinct keys, a key budget of 32: the turn resolves the
        // first 32 in log order and leaves the other eight untouched.
        sqlx::query("INSERT INTO library_evidence_changes(graph_id,node_id) SELECT $1,gen_random_uuid() FROM generate_series(1,40)")
            .bind(f.graph).execute(&f.owner).await.unwrap();
        let turn = f.batch(100).await;
        assert_eq!(turn.changes_consumed, 32);
        assert_eq!(pending(&f).await, 8);
        let last_eight: Vec<i64> = sqlx::query_scalar("SELECT id FROM library_evidence_changes WHERE graph_id=$1 ORDER BY id")
            .bind(f.graph).fetch_all(&f.owner).await.unwrap();
        let turn = f.batch(100).await;
        assert_eq!(turn.changes_consumed, 8);
        assert_eq!(pending(&f).await, 0);
        assert!(!row_exists(&f, last_eight[0]).await);
        f.close().await;
    }
}

/// A completed backfill acknowledges exactly the change rows its start
/// snapshot covers (every page saw their writes), a bounded batch per
/// refresh, draining nothing until the purge is finished. A row whose
/// transaction was still open at the snapshot, or began after it, survives.
#[tokio::test]
async fn a_completed_backfill_acknowledges_covered_rows_in_bounded_batches_and_keeps_late_commits() {
    for tenant in [false, true] {
        let f = Fixture::new(tenant).await;
        let node = f.node("Covered").await;
        let atom = f.atom(node, Some("Covered"), &[]).await;
        let other = f.node("Other").await;
        f.atom(other, Some("Other"), &[]).await;
        // Logged before the backfill starts: covered.
        let early = (2 * worker::PURGE_BATCH + 5) as i64;
        sqlx::query("DELETE FROM library_evidence_changes WHERE graph_id=$1").bind(f.graph).execute(&f.owner).await.unwrap();
        sqlx::query("INSERT INTO library_evidence_changes(graph_id,node_id) SELECT $1,$2 FROM generate_series(1,$3)")
            .bind(f.graph).bind(node).bind(early).execute(&f.owner).await.unwrap();
        // A late commit: its write (and the row the trigger logs for it) took
        // an id and a transaction id before the backfill started, and commits
        // only after the start snapshot, which saw the transaction in progress.
        let last_early: i64 = sqlx::query_scalar("SELECT max(id) FROM library_evidence_changes WHERE graph_id=$1")
            .bind(f.graph).fetch_one(&f.owner).await.unwrap();
        let mut late_tx = f.owner.begin().await.unwrap();
        sqlx::query("UPDATE atoms SET participants=ARRAY['Late commit'] WHERE atom_id=$1")
            .bind(atom).execute(&mut *late_tx).await.unwrap();
        let late: i64 = sqlx::query_scalar("SELECT max(id) FROM library_evidence_changes WHERE graph_id=$1")
            .bind(f.graph).fetch_one(&mut *late_tx).await.unwrap();
        assert!(late > last_early, "the late commit's row was logged after every covered row");
        let first = f.batch(1).await;
        assert!(!first.backfill_completed && first.changes_purged == 0);
        let snapshot_taken: bool = sqlx::query_scalar("SELECT backfill_snapshot IS NOT NULL FROM library_evidence_state WHERE graph_id=$1")
            .bind(f.graph).fetch_one(&f.owner).await.unwrap();
        assert!(snapshot_taken, "the restart takes the start snapshot before its first page");
        late_tx.commit().await.unwrap();
        // Logged after the start snapshot.
        let after: i64 = sqlx::query_scalar("INSERT INTO library_evidence_changes(graph_id,node_id) VALUES($1,$2) RETURNING id")
            .bind(f.graph).bind(node).fetch_one(&f.owner).await.unwrap();
        let mut completion = None;
        for _ in 0..100 {
            let turn = f.batch(1).await;
            if turn.backfill_completed {
                completion = Some(turn);
                break;
            }
            assert_eq!(turn.changes_purged, 0, "nothing is acknowledged before the backfill completes");
        }
        let completion = completion.expect("the backfill completes");
        assert_eq!(completion.changes_purged, worker::PURGE_BATCH, "the completing refresh drops the first bounded batch");
        assert_eq!(completion.changes_consumed, 0);
        assert_eq!(pending(&f).await, early + 2 - worker::PURGE_BATCH as i64);
        let second = f.batch(1).await;
        assert_eq!((second.changes_purged, second.changes_consumed, second.nodes), (worker::PURGE_BATCH, 0, 0), "a full batch: the next refresh purges again and drains nothing");
        assert!(row_exists(&f, late).await, "the late commit was not visible in the start snapshot");
        assert!(row_exists(&f, after).await, "a row logged after the snapshot is kept");
        assert_eq!(pending(&f).await, 7);
        let still_pending: bool = sqlx::query_scalar("SELECT backfill_snapshot IS NOT NULL FROM library_evidence_state WHERE graph_id=$1")
            .bind(f.graph).fetch_one(&f.owner).await.unwrap();
        assert!(still_pending);
        // The last five covered rows: a short batch finishes the purge, and
        // the same refresh drains. One node of budget: the late atom key
        // resolves to `node`, and the row logged after the snapshot names
        // that same node, so the one recomputation covers both keys and both
        // rows are acknowledged (the budget counts distinct nodes).
        let third = f.batch(1).await;
        assert_eq!((third.changes_purged, third.nodes, third.changes_consumed), (5, 1, 2));
        let cleared: bool = sqlx::query_scalar("SELECT backfill_snapshot IS NULL FROM library_evidence_state WHERE graph_id=$1")
            .bind(f.graph).fetch_one(&f.owner).await.unwrap();
        assert!(cleared, "a finished purge clears the snapshot");
        assert!(!row_exists(&f, late).await);
        assert!(!row_exists(&f, after).await);
        f.drain(1).await;
        f.close().await;
    }
}

/// Capture is gated by the listing switch, so a grain certified under one
/// switch generation misses every write made while Off. Off keeps the grain
/// as it is (nothing reads it); the first refresh after a flip to On
/// withdraws the certificate and rebuilds under the new generation, which
/// covers the missed writes.
#[tokio::test]
async fn a_flip_to_on_rebuilds_the_node_grain_and_covers_writes_missed_while_off() {
    for tenant in [false, true] {
        let f = Fixture::new(tenant).await;
        let node = f.node("Flip").await;
        f.atom(node, Some("Flip"), &[]).await;
        f.ready(8).await;
        f.drain(8).await;
        async fn switch_since(f: &Fixture) -> Option<DateTime<Utc>> {
            sqlx::query_scalar("SELECT enabled_since FROM library_listing_switch WHERE enabled").fetch_optional(&f.owner).await.unwrap()
        }
        async fn state(f: &Fixture) -> (Option<i32>, Option<DateTime<Utc>>) {
            sqlx::query_as("SELECT ready_version,capture_since FROM library_evidence_state WHERE graph_id=$1")
                .bind(f.graph).fetch_one(&f.owner).await.unwrap()
        }
        async fn stored_sources(f: &Fixture, node: Uuid) -> i64 {
            sqlx::query_scalar("SELECT source_count FROM library_node_evidence WHERE graph_id=$1 AND node_id=$2")
                .bind(f.graph).bind(node).fetch_one(&f.owner).await.unwrap()
        }
        let first = switch_since(&f).await;
        assert!(first.is_some());
        assert_eq!(state(&f).await, (Some(evidence::READ_MODEL_VERSION), first), "the certified grain carries its switch generation");
        assert_eq!(stored_sources(&f, node).await, 0);
        // Off: a write that changes the node's evidence logs nothing, and the
        // grain is neither drained into nor rebuilt.
        assert_eq!(worker::flip_listing_switch(&f.owner, false).await.unwrap(), Some(false));
        f.source(&[node], json!([])).await;
        assert_eq!(pending(&f).await, 0, "Off logs no evidence change");
        let idle = f.batch(8).await;
        assert_eq!((idle.nodes, idle.backfill_completed), (0, false), "Off demands no generation: the grain is kept, not rebuilt");
        assert_eq!(state(&f).await, (Some(evidence::READ_MODEL_VERSION), first));
        assert_eq!(stored_sources(&f, node).await, 0, "stale while Off, which nothing reads");
        // On: a new generation. The first refresh withdraws the certificate
        // and restarts the backfill under it.
        assert_eq!(worker::flip_listing_switch(&f.owner, true).await.unwrap(), Some(true));
        let second = switch_since(&f).await;
        assert!(second > first);
        let rebuilt = f.batch(8).await;
        assert!(rebuilt.backfill_completed && rebuilt.nodes == 1);
        assert_eq!(state(&f).await, (Some(evidence::READ_MODEL_VERSION), second));
        assert_eq!(stored_sources(&f, node).await, 1, "the write missed while Off is covered by the rebuild");
        f.close().await;
    }
}

async fn switch_since(f: &Fixture) -> Option<DateTime<Utc>> {
    sqlx::query_scalar("SELECT enabled_since FROM library_listing_switch WHERE enabled")
        .fetch_optional(&f.owner)
        .await
        .unwrap()
}

/// Stamp every stored node as computed long ago, so `recomputed` names
/// exactly the nodes a later turn wrote (the canonical write sets
/// computed_at=now() on every node it stores).
async fn mark_computed_long_ago(f: &Fixture) {
    sqlx::query("UPDATE library_node_evidence SET computed_at='2000-01-01' WHERE graph_id=$1")
        .bind(f.graph)
        .execute(&f.owner)
        .await
        .unwrap();
}

async fn recomputed(f: &Fixture) -> Vec<Uuid> {
    sqlx::query_scalar("SELECT node_id FROM library_node_evidence WHERE graph_id=$1 AND computed_at>'2000-01-02' ORDER BY node_id")
        .bind(f.graph)
        .fetch_all(&f.owner)
        .await
        .unwrap()
}

async fn log_item(f: &Fixture, item: Uuid, cursor: Option<Uuid>) -> i64 {
    sqlx::query_scalar("INSERT INTO library_evidence_changes(graph_id,source_item_id,resolved_after_node_id) VALUES($1,$2,$3) RETURNING id")
        .bind(f.graph).bind(item).bind(cursor).fetch_one(&f.owner).await.unwrap()
}

async fn cursor_of(f: &Fixture, id: i64) -> Option<Uuid> {
    sqlx::query_scalar("SELECT resolved_after_node_id FROM library_evidence_changes WHERE graph_id=$1 AND id=$2")
        .bind(f.graph).bind(id).fetch_one(&f.owner).await.unwrap()
}

/// A nine-node item key on a certified grain with an empty log; the walk
/// order (node id order) is returned.
async fn hub(tenant: bool) -> (Fixture, Uuid, Vec<Uuid>) {
    let f = Fixture::new(tenant).await;
    let mut walk = Vec::new();
    for i in 0..9 {
        walk.push(f.node(&format!("Hub {i}")).await);
    }
    let (item, _, _) = f.source(&walk, json!([])).await;
    f.ready(16).await;
    f.drain(16).await;
    walk.sort();
    (f, item, walk)
}

/// Rows merged under one key resume from the lowest cursor among them, and
/// from the first node when any merged row has none, including a row with a
/// lower id that committed after the carrier's cursor was saved. Resuming
/// from the carrier's own cursor (or the highest) would skip nodes an
/// earlier row's write reached.
#[tokio::test]
async fn merged_rows_resume_from_the_lowest_cursor_and_restart_when_any_row_has_none() {
    for tenant in [false, true] {
        let (f, item, walk) = hub(tenant).await;
        // The lowest cursor wins: two rows walked to w6 and w2.
        let carrier = log_item(&f, item, Some(walk[6])).await;
        let merged = log_item(&f, item, Some(walk[2])).await;
        mark_computed_long_ago(&f).await;
        let turn = f.batch(3).await;
        assert_eq!(recomputed(&f).await, walk[3..6].to_vec(), "the walk resumes after the lowest cursor");
        assert_eq!(turn.changes_consumed, 1, "the merged row is acknowledged");
        assert!(!row_exists(&f, merged).await);
        assert_eq!(cursor_of(&f, carrier).await, Some(walk[5]), "the carrier keeps the advanced cursor");
        f.drain(16).await;

        // A merged row without a cursor restarts the walk from the first node.
        let carrier = log_item(&f, item, Some(walk[5])).await;
        let merged = log_item(&f, item, None).await;
        mark_computed_long_ago(&f).await;
        f.batch(3).await;
        assert_eq!(recomputed(&f).await, walk[0..3].to_vec(), "a row without a cursor restarts from the first node");
        assert!(!row_exists(&f, merged).await);
        assert_eq!(cursor_of(&f, carrier).await, Some(walk[2]));
        f.drain(16).await;

        // A late commit: its id is taken first, but the row becomes visible
        // only after a later row of the same key was walked to w7. The late
        // row is the lower id, so it is the carrier, and it has no cursor.
        let mut late = f.owner.begin().await.unwrap();
        let low: i64 = sqlx::query_scalar("INSERT INTO library_evidence_changes(graph_id,source_item_id) VALUES($1,$2) RETURNING id")
            .bind(f.graph).bind(item).fetch_one(&mut *late).await.unwrap();
        let high = log_item(&f, item, Some(walk[5])).await;
        assert!(high > low);
        f.batch(2).await;
        assert_eq!(cursor_of(&f, high).await, Some(walk[7]), "the visible row walked on alone");
        late.commit().await.unwrap();
        mark_computed_long_ago(&f).await;
        let turn = f.batch(3).await;
        assert_eq!(recomputed(&f).await, walk[0..3].to_vec(), "the late row's write may sit behind w7: the walk restarts");
        assert_eq!(turn.changes_consumed, 1);
        assert!(!row_exists(&f, high).await, "the merged row with the cursor is acknowledged");
        assert_eq!(cursor_of(&f, low).await, Some(walk[2]), "the lower id carries the restarted walk");
        f.drain(16).await;
        f.close().await;
    }
}

/// Rows logged before 0201/0380 have no capture_xid (all 858k production
/// rows). A completed backfill counts them as covered even where a row with
/// a capture_xid logged at the same moment is not.
#[tokio::test]
async fn rows_without_a_capture_xid_count_as_covered_by_a_completed_backfill() {
    for tenant in [false, true] {
        let f = Fixture::new(tenant).await;
        for i in 0..3 {
            let node = f.node(&format!("Legacy {i}")).await;
            f.atom(node, Some("Legacy"), &[]).await;
        }
        f.ready(8).await;
        f.drain(8).await;
        // A definition change: the next refresh restarts the backfill.
        sqlx::query("UPDATE library_evidence_state SET ready_version=NULL,backfill_version=NULL WHERE graph_id=$1")
            .bind(f.graph).execute(&f.owner).await.unwrap();
        let first = f.batch(1).await;
        assert!(!first.backfill_completed);
        // After the start snapshot: one legacy-shaped row set and one fresh row.
        sqlx::query("INSERT INTO library_evidence_changes(graph_id,node_id,capture_xid) SELECT $1,gen_random_uuid(),NULL FROM generate_series(1,5)")
            .bind(f.graph).execute(&f.owner).await.unwrap();
        let fresh: i64 = sqlx::query_scalar("INSERT INTO library_evidence_changes(graph_id,node_id) VALUES($1,gen_random_uuid()) RETURNING id")
            .bind(f.graph).fetch_one(&f.owner).await.unwrap();
        let mut completion = None;
        for _ in 0..10 {
            let turn = f.batch(1).await;
            if turn.backfill_completed {
                completion = Some(turn);
                break;
            }
        }
        let completion = completion.expect("the backfill completes");
        assert_eq!(completion.changes_purged, 5, "every NULL capture_xid row is covered");
        let legacy_left: i64 = sqlx::query_scalar("SELECT count(*) FROM library_evidence_changes WHERE graph_id=$1 AND capture_xid IS NULL")
            .bind(f.graph).fetch_one(&f.owner).await.unwrap();
        assert_eq!(legacy_left, 0);
        assert!(row_exists(&f, fresh).await, "a row logged after the start snapshot is kept");
        f.close().await;
    }
}

/// While Off nothing reads the model and the next On rebuilds it, so a
/// refresh computes no node evidence: it deletes the log, a bounded batch
/// per refresh, oldest first, rows without a capture_xid included (the
/// production backlog), and leaves the grain and its state row alone.
#[tokio::test]
async fn an_off_refresh_purges_the_log_in_bounded_batches_without_node_work() {
    for tenant in [false, true] {
        let f = Fixture::new(tenant).await;
        let node = f.node("Off purge").await;
        f.atom(node, Some("Off purge"), &[]).await;
        f.ready(8).await;
        f.drain(8).await;
        let backlog = (2 * worker::PURGE_BATCH + 5) as i64;
        sqlx::query("INSERT INTO library_evidence_changes(graph_id,node_id,capture_xid) SELECT $1,$2,NULL FROM generate_series(1,$3)")
            .bind(f.graph).bind(node).bind(backlog).execute(&f.owner).await.unwrap();
        let oldest: i64 = sqlx::query_scalar("SELECT min(id) FROM library_evidence_changes WHERE graph_id=$1")
            .bind(f.graph).fetch_one(&f.owner).await.unwrap();
        assert_eq!(worker::flip_listing_switch(&f.owner, false).await.unwrap(), Some(false));
        let state = |f: &Fixture| {
            let (owner, graph) = (f.owner.clone(), f.graph);
            async move {
                sqlx::query_scalar::<_, Value>("SELECT to_jsonb(s)-'last_refreshed_at' FROM library_evidence_state s WHERE graph_id=$1")
                    .bind(graph).fetch_one(&owner).await.unwrap()
            }
        };
        let before = state(&f).await;
        mark_computed_long_ago(&f).await;
        let first = f.batch(128).await;
        assert_eq!((first.changes_purged, first.nodes, first.changes_consumed, first.backfill_completed), (worker::PURGE_BATCH, 0, 0, false));
        assert!(!row_exists(&f, oldest).await, "oldest first");
        let second = f.batch(128).await;
        assert_eq!((second.changes_purged, second.nodes), (worker::PURGE_BATCH, 0));
        let third = f.batch(128).await;
        assert_eq!((third.changes_purged, third.nodes), (5, 0));
        let idle = f.batch(128).await;
        assert_eq!((idle.changes_purged, idle.nodes), (0, 0));
        assert_eq!(pending(&f).await, 0);
        assert!(recomputed(&f).await.is_empty(), "no node evidence computed while Off");
        assert_eq!(state(&f).await, before, "the state row is untouched");
        // On: the new generation rebuilds once.
        assert_eq!(worker::flip_listing_switch(&f.owner, true).await.unwrap(), Some(true));
        let rebuilt = f.batch(8).await;
        assert!(rebuilt.backfill_completed);
        let since: Option<DateTime<Utc>> = sqlx::query_scalar("SELECT capture_since FROM library_evidence_state WHERE graph_id=$1")
            .bind(f.graph).fetch_one(&f.owner).await.unwrap();
        assert_eq!(since, switch_since(&f).await);
        f.close().await;
    }
}

/// On, Off, On again in the middle of a backfill. Off does no node work, so
/// the backfill neither advances nor certifies anything while Off; the next
/// On is a new generation and restarts it from the first node with a fresh
/// start snapshot, which covers the write made while Off.
#[tokio::test]
async fn off_in_the_middle_of_a_backfill_certifies_nothing_and_the_next_on_rebuilds() {
    for tenant in [false, true] {
        let f = Fixture::new(tenant).await;
        let mut nodes = Vec::new();
        for i in 0..5 {
            let node = f.node(&format!("Mid {i}")).await;
            f.atom(node, Some("Mid"), &[]).await;
            nodes.push(node);
        }
        nodes.sort();
        async fn state(f: &Fixture) -> (Option<i32>, Option<DateTime<Utc>>, Option<DateTime<Utc>>, Option<Uuid>) {
            sqlx::query_as("SELECT ready_version,capture_since,backfill_capture_since,backfill_after_node_id FROM library_evidence_state WHERE graph_id=$1")
                .bind(f.graph).fetch_one(&f.owner).await.unwrap()
        }
        let first_on = switch_since(&f).await;
        let started = f.batch(2).await;
        assert!(!started.backfill_completed);
        assert_eq!(state(&f).await, (None, None, first_on, Some(nodes[1])), "a backfill two nodes in");
        assert_eq!(worker::flip_listing_switch(&f.owner, false).await.unwrap(), Some(false));
        // A write while Off: logs nothing, and only a rebuild can cover it.
        f.source(&[nodes[4]], json!([])).await;
        for _ in 0..5 {
            let off = f.batch(2).await;
            assert_eq!((off.nodes, off.backfill_completed), (0, false), "Off advances no backfill");
        }
        assert_eq!(state(&f).await, (None, None, first_on, Some(nodes[1])), "nothing certified while Off");
        assert_eq!(worker::flip_listing_switch(&f.owner, true).await.unwrap(), Some(true));
        let second_on = switch_since(&f).await;
        assert!(second_on > first_on);
        let restarted = f.batch(2).await;
        assert!(!restarted.backfill_completed);
        assert_eq!(state(&f).await, (None, None, second_on, Some(nodes[1])), "restarted from the first node under the new generation");
        f.ready(2).await;
        assert_eq!(state(&f).await, (Some(evidence::READ_MODEL_VERSION), second_on, second_on, None));
        let sources: i64 = sqlx::query_scalar("SELECT source_count FROM library_node_evidence WHERE graph_id=$1 AND node_id=$2")
            .bind(f.graph).bind(nodes[4]).fetch_one(&f.owner).await.unwrap();
        assert_eq!(sources, 1, "the write made while Off is covered by the rebuild");
        f.close().await;
    }
}

/// The flip barrier's xid order (tenant 0202 / control 0383). A writer can
/// take its xid AFTER the flip's and still read the switch as Off: its
/// statement runs while the flip transaction is open. At the stamp, that
/// writer's xid is above every completed xid, so it is neither below a
/// stamp snapshot's xmax (one past the highest completed xid) nor in its
/// in-progress list, and a barrier settled at xmin >= that xmax (0201/0380)
/// let the rebuild start with it open. The stamp transaction's own xid is
/// above the writer's, so the barrier holds until the writer ends.
#[tokio::test]
async fn a_writer_that_takes_its_xid_after_the_flip_and_reads_off_holds_the_barrier() {
    for tenant in [false, true] {
        let f = Fixture::new(tenant).await;
        let node = f.node("Late").await;
        f.atom(node, Some("Late"), &[]).await;
        f.ready(8).await;
        f.drain(8).await;
        assert_eq!(worker::flip_listing_switch(&f.owner, false).await.unwrap(), Some(false));
        // The flip, held open: it has its xid, and every other session still
        // reads the switch as Off. Same statement as the operator flip.
        let mut flip = f.owner.begin().await.unwrap();
        sqlx::query("SELECT set_config('coralswarm.library_listing_switch_flip','operator',true)")
            .execute(&mut *flip).await.unwrap();
        let flip_xid: i64 = sqlx::query_scalar("UPDATE library_listing_switch SET enabled=true,enabled_since=now(),enabled_snapshot=NULL,snapshot_since=NULL,barrier_xid=NULL,barrier_since=NULL,changed_at=now() RETURNING pg_current_xact_id()::text::bigint")
            .fetch_one(&mut *flip).await.unwrap();
        // The writer takes its xid after the flip's, then writes; its
        // trigger reads Off (the flip is uncommitted) and logs nothing.
        let mut writer = f.owner.begin().await.unwrap();
        let writer_xid: i64 = sqlx::query_scalar("SELECT pg_current_xact_id()::text::bigint")
            .fetch_one(&mut *writer).await.unwrap();
        assert!(writer_xid > flip_xid, "the writer's xid is after the flip's");
        let item: Uuid = sqlx::query_scalar("INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id,access_state) VALUES($1,'note','codex:rm3','late','permitted') RETURNING source_item_id")
            .bind(f.graph).fetch_one(&mut *writer).await.unwrap();
        let version: Uuid = sqlx::query_scalar("INSERT INTO source_versions(graph_id,source_item_id,source_id,source_type,provider_namespace,raw_inline,source_metadata,content_sha256,extraction_status) VALUES($1,$2,'late','note','codex:rm3','neutral text','{}',repeat('a',64),'done') RETURNING version_id")
            .bind(f.graph).bind(item).fetch_one(&mut *writer).await.unwrap();
        sqlx::query("UPDATE source_items SET current_version_id=$2 WHERE source_item_id=$1")
            .bind(item).bind(version).execute(&mut *writer).await.unwrap();
        let outcome: Uuid = sqlx::query_scalar("INSERT INTO semantic_atoms(graph_id,source_version_id,ordinal,kind,statement,extraction_version,model,entities) VALUES($1,$2,0,'fact','late','source-atoms-v1','mock','[]'::jsonb) RETURNING semantic_atom_id")
            .bind(f.graph).bind(version).fetch_one(&mut *writer).await.unwrap();
        sqlx::query("INSERT INTO node_semantic_atoms(node_id,semantic_atom_id,graph_id) VALUES($1,$2,$3)")
            .bind(node).bind(outcome).bind(f.graph).execute(&mut *writer).await.unwrap();
        let logged: i64 = sqlx::query_scalar("SELECT count(*) FROM library_evidence_changes WHERE graph_id=$1")
            .bind(f.graph).fetch_one(&mut *writer).await.unwrap();
        assert_eq!(logged, 0, "the writer read Off and logged nothing");
        flip.commit().await.unwrap();
        // The hole, observed: right after the flip commits, a snapshot's xmax
        // is one past the highest completed xid, and the open writer is at
        // or above it (so in no in-progress list either).
        let xmax: i64 = sqlx::query_scalar("SELECT pg_snapshot_xmax(pg_current_snapshot())::text::bigint")
            .fetch_one(&f.owner).await.unwrap();
        assert!(writer_xid >= xmax, "xmax {xmax} is not above the open writer {writer_xid}");
        // What the operator command (and any later worker tick) runs next.
        assert!(worker::stamp_listing_barrier(&f.owner).await.unwrap());
        assert_eq!(worker::listing_switch_settled(&f.owner).await.unwrap(), Some(false), "the writer that read Off is still open");
        let generation = switch_since(&f).await;
        mark_computed_long_ago(&f).await;
        let waiting = f.batch(8).await;
        assert_eq!((waiting.nodes, waiting.backfill_completed), (0, false), "no rebuild before the barrier");
        let (snapshot, backfill_since): (bool, Option<DateTime<Utc>>) = sqlx::query_as("SELECT backfill_snapshot IS NOT NULL,backfill_capture_since FROM library_evidence_state WHERE graph_id=$1")
            .bind(f.graph).fetch_one(&f.owner).await.unwrap();
        assert!(!snapshot && backfill_since != generation, "no start snapshot taken for the new generation");
        writer.commit().await.unwrap();
        assert_eq!(worker::listing_switch_settled(&f.owner).await.unwrap(), Some(true));
        f.ready(8).await;
        let sources: i64 = sqlx::query_scalar("SELECT source_count FROM library_node_evidence WHERE graph_id=$1 AND node_id=$2")
            .bind(f.graph).bind(node).fetch_one(&f.owner).await.unwrap();
        assert_eq!(sources, 1, "the rebuild started after the writer ended, so it saw the unlogged write");
        f.close().await;
    }
}

/// The Off-to-On race. A statement whose capture trigger read the switch as
/// Off logs nothing, even when its transaction commits after the flip. The
/// rebuild for the new generation must not take its start snapshot while
/// such a transaction is still open: that snapshot would see it in progress,
/// every page would miss its write, and no log row would repair it. The
/// worker waits for the flip barrier (every xid up to the stamp
/// transaction's own has ended) before either grain starts.
#[tokio::test]
async fn a_flip_to_on_waits_for_writers_that_read_the_switch_off() {
    for tenant in [false, true] {
        let f = Fixture::new(tenant).await;
        let node = f.node("Race").await;
        f.atom(node, Some("Race"), &[]).await;
        f.ready(8).await;
        f.drain(8).await;
        assert_eq!(worker::flip_listing_switch(&f.owner, false).await.unwrap(), Some(false));
        assert_eq!(worker::listing_switch_settled(&f.owner).await.unwrap(), None, "no barrier while Off");
        // The writer: its statement reads Off and logs nothing; it stays open.
        let mut writer = f.owner.begin().await.unwrap();
        let item: Uuid = sqlx::query_scalar("INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id,access_state) VALUES($1,'note','codex:rm3','race','permitted') RETURNING source_item_id")
            .bind(f.graph).fetch_one(&mut *writer).await.unwrap();
        let version: Uuid = sqlx::query_scalar("INSERT INTO source_versions(graph_id,source_item_id,source_id,source_type,provider_namespace,raw_inline,source_metadata,content_sha256,extraction_status) VALUES($1,$2,'race','note','codex:rm3','neutral text','{}',repeat('a',64),'done') RETURNING version_id")
            .bind(f.graph).bind(item).fetch_one(&mut *writer).await.unwrap();
        sqlx::query("UPDATE source_items SET current_version_id=$2 WHERE source_item_id=$1")
            .bind(item).bind(version).execute(&mut *writer).await.unwrap();
        let outcome: Uuid = sqlx::query_scalar("INSERT INTO semantic_atoms(graph_id,source_version_id,ordinal,kind,statement,extraction_version,model,entities) VALUES($1,$2,0,'fact','race','source-atoms-v1','mock','[]'::jsonb) RETURNING semantic_atom_id")
            .bind(f.graph).bind(version).fetch_one(&mut *writer).await.unwrap();
        sqlx::query("INSERT INTO node_semantic_atoms(node_id,semantic_atom_id,graph_id) VALUES($1,$2,$3)")
            .bind(node).bind(outcome).bind(f.graph).execute(&mut *writer).await.unwrap();
        // The flip commits while the writer is open.
        assert_eq!(worker::flip_listing_switch(&f.owner, true).await.unwrap(), Some(true));
        let generation = switch_since(&f).await;
        assert_eq!(worker::listing_switch_settled(&f.owner).await.unwrap(), Some(false), "a transaction that read Off is still open");
        mark_computed_long_ago(&f).await;
        let waiting = f.batch(8).await;
        assert_eq!((waiting.nodes, waiting.backfill_completed), (0, false), "no rebuild before the barrier");
        let (snapshot, backfill_since): (bool, Option<DateTime<Utc>>) = sqlx::query_as("SELECT backfill_snapshot IS NOT NULL,backfill_capture_since FROM library_evidence_state WHERE graph_id=$1")
            .bind(f.graph).fetch_one(&f.owner).await.unwrap();
        assert!(!snapshot && backfill_since != generation, "no start snapshot taken for the new generation");
        let listing_since: Option<DateTime<Utc>> = sqlx::query_scalar("SELECT capture_since FROM library_listing_state WHERE graph_id=$1")
            .bind(f.graph).fetch_optional(&f.owner).await.unwrap().flatten();
        assert_ne!(listing_since, generation, "the listing grain is not prepared for the new generation either");
        writer.commit().await.unwrap();
        assert_eq!(worker::listing_switch_settled(&f.owner).await.unwrap(), Some(true));
        f.ready(8).await;
        let sources: i64 = sqlx::query_scalar("SELECT source_count FROM library_node_evidence WHERE graph_id=$1 AND node_id=$2")
            .bind(f.graph).bind(node).fetch_one(&f.owner).await.unwrap();
        assert_eq!(sources, 1, "the rebuild started after the writer ended, so it saw the unlogged write");
        // A stamp from another generation never counts as settled, and the
        // next stamp (any worker tick) replaces it.
        sqlx::query("UPDATE library_listing_switch SET barrier_since=barrier_since-interval '1 second'")
            .execute(&f.owner).await.unwrap();
        assert_eq!(worker::listing_switch_settled(&f.owner).await.unwrap(), Some(false));
        assert!(worker::stamp_listing_barrier(&f.owner).await.unwrap());
        assert_eq!(worker::listing_switch_settled(&f.owner).await.unwrap(), Some(true), "the next stamp restamps it");
        assert!(!worker::stamp_listing_barrier(&f.owner).await.unwrap(), "a stamped generation is not restamped");
        f.close().await;
    }
}
