//! Definition-three maintenance proofs through the real graph worker.
//! Requires a task-private ORGT_TEST_BASE_URL; no quiet skips or fabricated ready rows.
//! Storage/worker integration must be frozen before compiling or running this target.
use std::{borrow::Cow, collections::BTreeSet, str::FromStr, sync::Arc, time::Duration};

use chrono::{DateTime, Utc};
use coralswarm_backend::{
    library_read::{self, evidence, LibraryScope},
    store::PostgresStore,
    tenancy::scope::{begin_account_scoped, begin_tenant_system_scoped_without_account},
    tenancy::TenantId,
    workers::library_evidence::{self as worker, Batch, Limits, ListingLimits, RefreshLimits},
    workers::tenant_harness::WorkerTenant,
};
use serde_json::{json, Value};
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    Connection, Executor, PgConnection, PgPool, Postgres, Transaction,
};
use coralswarm_backend::test_db::TestDatabase;
use uuid::Uuid;

struct Fixture {
    admin: PgConnection,
    owner: PgPool,
    app: PgPool,
    db: coralswarm_backend::test_db::TestDatabase,
    role: String,
    tenant: bool,
    account: Uuid,
    graph: Uuid,
}

fn tiny() -> RefreshLimits {
    RefreshLimits {
        evidence: Limits {
            nodes: 1,
            changes: 2,
        },
        listing: ListingLimits {
            outcomes: 2,
            changes: 2,
        },
    }
}

fn migrations(tenant: bool) -> sqlx::migrate::Migrator {
    if tenant { sqlx::migrate!("./migrations_tenant") }
    else { sqlx::migrate!("./migrations_control") }
}

fn listing_version(tenant: bool) -> i64 {
    if tenant { 199 } else { 378 }
}
fn default_on_version(tenant: bool) -> i64 {
    if tenant { 209 } else { 394 }
}

#[derive(Clone, Copy)]
enum Start {
    /// The fully migrated tree, untouched: the switch On, its barrier unstamped.
    Shipped,
    /// Shipped, then turned Off by the operator command.
    Off,
    /// Only the migrations below this version, for an upgrade to apply the rest.
    Below(i64),
}

impl Fixture {
    /// The shipped plane after the worker's first tick stamped its barrier.
    async fn new(tenant: bool) -> Self {
        let f = Self::build(tenant, Start::Shipped).await;
        assert!(worker::stamp_listing_barrier(&f.owner).await.unwrap());
        f.settled().await;
        f
    }

    /// The flip barrier settles once every transaction on the server older
    /// than its stamp has ended, and the tests in this binary share the
    /// server, so a test waits it out rather than assuming it already passed.
    async fn settled(&self) {
        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        while worker::listing_switch_settled(&self.owner).await.unwrap() != Some(true) {
            assert!(std::time::Instant::now() < deadline, "the flip barrier never settled");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    async fn switched_off(tenant: bool) -> Self {
        Self::build(tenant, Start::Off).await
    }

    async fn at_version(tenant: bool, below: i64) -> Self {
        Self::build(tenant, Start::Below(below)).await
    }

    async fn build(tenant: bool, start: Start) -> Self {
        let before_listing = matches!(start, Start::Below(_));
        let base = std::env::var("ORGT_TEST_BASE_URL").expect("real private PostgreSQL required");
        let opts = PgConnectOptions::from_str(&base).unwrap();
        assert!(matches!(opts.get_host(), "127.0.0.1" | "localhost" | "::1"));
        let nonce = Uuid::new_v4().simple().to_string();
        let mut admin = PgConnection::connect_with(&opts).await.unwrap();
        // A pre-listing schema is a partial migration run; everything else
        // copies the fully migrated template.
        let db = if before_listing {
            let db = TestDatabase::new(&opts, "listing_worker");
            admin
                .execute(format!("CREATE DATABASE {db}").as_str())
                .await
                .unwrap();
            db
        } else {
            let plane = if tenant {
                coralswarm_backend::test_db::Plane::Tenant
            } else {
                coralswarm_backend::test_db::Plane::Control
            };
            coralswarm_backend::test_db::migrated_database(&opts, plane, "listing_worker").await
        };
        let role = db.role("listing_app");
        let owner = PgPoolOptions::new()
            .max_connections(8)
            .connect_with(opts.clone().database(&db))
            .await
            .unwrap();
        sqlx::query("SELECT pg_advisory_lock($1)")
            .bind(0x0083_5C09_i64)
            .execute(&mut admin)
            .await
            .unwrap();
        match start {
            Start::Below(below) => {
                // Every migration below `below` is byte-identical to the
                // pinned main base. SQLx applies that real historical tree;
                // the caller upgrades it with the rest.
                let mut schema = migrations(tenant);
                assert!(schema.iter().any(|m| m.version == below));
                let old_len = schema.iter().count();
                schema.migrations = Cow::Owned(schema.iter()
                    .filter(|m| m.version < below).cloned().collect());
                assert!(schema.iter().count() < old_len);
                schema.run(&owner).await.unwrap();
            }
            Start::Shipped => {
                let (on, since, stamped): (bool, Option<DateTime<Utc>>, bool) = sqlx::query_as("SELECT enabled,enabled_since,barrier_xid IS NOT NULL FROM library_listing_switch")
                    .fetch_one(&owner).await.unwrap();
                assert!(on && since.is_some() && !stamped, "the migration ships the switch On with an unstamped generation");
            }
            Start::Off => {
                assert_eq!(worker::flip_listing_switch(&owner, false).await.unwrap(), Some(false));
            }
        }
        let parent = if tenant {
            "coralswarm_tenant_app"
        } else {
            "coralswarm_app"
        };
        owner
            .execute(
                format!(
                    "CREATE ROLE {role} LOGIN INHERIT NOBYPASSRLS PASSWORD 'listing-fixture-only'"
                )
                .as_str(),
            )
            .await
            .unwrap();
        owner
            .execute(format!("GRANT {parent} TO {role}").as_str())
            .await
            .unwrap();
        sqlx::query("SELECT pg_advisory_unlock($1)")
            .bind(0x0083_5C09_i64)
            .execute(&mut admin)
            .await
            .unwrap();
        let app = PgPoolOptions::new()
            .max_connections(8)
            .connect_with(
                opts.database(&db)
                    .username(&role)
                    .password("listing-fixture-only"),
            )
            .await
            .unwrap();
        let (actor, account) = if tenant {
            sqlx::query("INSERT INTO tenant_meta(id,org_id) VALUES(true,'org_listing_worker')")
                .execute(&owner)
                .await
                .unwrap();
            (Uuid::new_v4(), Uuid::new_v4())
        } else {
            let actor: Uuid =
                sqlx::query_scalar("INSERT INTO users(email) VALUES($1) RETURNING id")
                    .bind(format!("{nonce}@fixture.invalid"))
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
        let graph = sqlx::query_scalar("INSERT INTO graphs(account_id,name,kind,owner_user_id) VALUES($1,'Listing worker','member',$2) RETURNING id")
            .bind(account).bind(actor).fetch_one(&owner).await.unwrap();
        Self {
            admin,
            owner,
            app,
            db,
            role,
            tenant,
            account,
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

    async fn step(&self) -> Batch {
        self.step_on(&self.app).await
    }

    async fn step_on(&self, pool: &PgPool) -> Batch {
        let mut tx = self.tx_on(pool).await;
        let batch = worker::refresh_graph_tx(&mut tx, self.graph, tiny())
            .await
            .unwrap();
        assert!(!batch.locked);
        // One turn resolves at most two keys but acknowledges every row it
        // read for them (the dedupe window), so the row bound is wider.
        assert!(batch.nodes <= 1 && batch.changes_consumed <= 2 * worker::DEDUPE_WINDOW);
        assert!(batch.listing_outcomes <= 2 && batch.listing_changes_consumed <= 2);
        assert!(batch.listing_items_published <= 1);
        tx.commit().await.unwrap();
        batch
    }

    async fn clean(&self) -> bool {
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM library_listing_state WHERE graph_id=$1 AND ready_version=3) AND NOT EXISTS(SELECT 1 FROM library_listing_changes WHERE graph_id=$1) AND NOT EXISTS(SELECT 1 FROM library_evidence_changes WHERE graph_id=$1) AND NOT EXISTS(SELECT 1 FROM library_listing_generations WHERE graph_id=$1 AND phase NOT IN('complete','abandoned'))")
            .bind(self.graph).fetch_one(&self.owner).await.unwrap()
    }

    /// The node grain alone: certified at its own version with no log left.
    async fn node_evidence_clean(&self) -> bool {
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM library_evidence_state WHERE graph_id=$1 AND ready_version=$2) AND NOT EXISTS(SELECT 1 FROM library_evidence_changes WHERE graph_id=$1)")
            .bind(self.graph).bind(evidence::READ_MODEL_VERSION).fetch_one(&self.owner).await.unwrap()
    }

    async fn pending_listing_changes(&self) -> i64 {
        sqlx::query_scalar("SELECT count(*) FROM library_listing_changes WHERE graph_id=$1")
            .bind(self.graph).fetch_one(&self.owner).await.unwrap()
    }

    async fn switch(&self) -> (bool, Option<DateTime<Utc>>) {
        sqlx::query_as("SELECT enabled,enabled_since FROM library_listing_switch")
            .fetch_one(&self.owner).await.unwrap()
    }

    /// The real worker entry, as the harness drives it for this plane.
    fn worker_tenant(&self) -> WorkerTenant {
        WorkerTenant {
            tenant: if self.tenant {
                TenantId::Org(format!("org_listing_{}", self.db))
            } else {
                TenantId::Consumer
            },
            pool: self.app.clone(),
            store: Arc::new(PostgresStore::new(self.app.clone())),
        }
    }

    /// Which path the guarded reader takes for this graph, and why it fell
    /// back. Whatever the path, the candidate must equal the independent
    /// Live reader: this is the hybrid reader's equivalence oracle.
    async fn read_path(&self, current: bool) -> (String, Option<String>) {
        let scope: LibraryScope = serde_json::from_value(json!({"graph_ids":[self.graph],"outcome_versions":if current {"current"} else {"all_retained"}})).unwrap();
        let mut tx = self.app.begin_with("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY").await.unwrap();
        sqlx::query("SELECT set_config('app.account_id',$1,true),set_config('app.user_id','',true),set_config('app.tenant_actor',$2,true)")
            .bind(self.account.to_string()).bind(if self.tenant {"system"} else {""})
            .execute(&mut *tx).await.unwrap();
        library_read::configure_read_model_proof_tx(&mut tx).await.unwrap();
        let pair = library_read::compare_read_model_tx(&mut tx, &scope, "", 20, [None;3]).await.unwrap();
        assert_eq!(pair.live, pair.candidate, "the hybrid reader must equal the Live reader on every path (current={current})");
        tx.rollback().await.unwrap();
        (pair.path.to_string(), pair.fallback_reason.clone())
    }

    async fn drain(&self) -> usize {
        for tick in 1..=2000 {
            self.step().await;
            if self.clean().await {
                return tick;
            }
        }
        panic!("small known fixture did not converge under real bounded batches");
    }

    async fn unfinished(&self) -> Option<(Uuid, Uuid, String)> {
        let rows: Vec<(Uuid, Uuid, String)> = sqlx::query_as("SELECT source_item_id,generation,phase FROM library_listing_generations WHERE graph_id=$1 AND phase NOT IN('complete','abandoned')")
            .bind(self.graph).fetch_all(&self.owner).await.unwrap();
        assert!(
            rows.len() <= 1,
            "one unfinished generation PER GRAPH, not per item"
        );
        rows.into_iter().next()
    }

    async fn start_item(&self, item: Uuid) -> Uuid {
        for _ in 0..2000 {
            self.step().await;
            if let Some((active, generation, _)) = self.unfinished().await {
                assert_eq!(active, item, "fixture deliberately queues only this item");
                return generation;
            }
        }
        panic!("item never began a real generation");
    }

    async fn snapshot(&self, conn: &mut PgConnection) -> Value {
        sqlx::query_scalar("SELECT jsonb_build_object('state',(SELECT to_jsonb(s) FROM library_listing_state s WHERE graph_id=$1),'generations',(SELECT jsonb_agg(to_jsonb(g) ORDER BY source_item_id,generation) FROM library_listing_generations g WHERE graph_id=$1),'staged',(SELECT jsonb_agg(to_jsonb(o) ORDER BY source_item_id,generation,semantic_atom_id) FROM library_listing_staged_outcomes o WHERE graph_id=$1),'heads',(SELECT jsonb_agg(to_jsonb(h) ORDER BY source_item_id) FROM library_listing_source_heads h WHERE graph_id=$1),'pending',(SELECT jsonb_agg(id ORDER BY id) FROM library_listing_changes WHERE graph_id=$1))")
            .bind(self.graph).fetch_one(conn).await.unwrap()
    }

    async fn source(&self, label: &str, outcomes: i32) -> (Uuid, Uuid, Vec<Uuid>) {
        let id = Uuid::new_v4().to_string();
        let origin = json!({"service_id":"slack","content_kind":"message","provider_namespace":"worker:listing", "connection":{"kind":"oauth_grant","id":"fixture"},"container":{"kind":"channel","id":"shared","provider_namespace":"worker:listing","label":label}});
        let item = sqlx::query_scalar("INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id,origin,access_state) VALUES($1,'slack','worker:listing',$2,$3,'permitted') RETURNING source_item_id")
            .bind(self.graph).bind(&id).bind(origin).fetch_one(&self.owner).await.unwrap();
        let (version, ids) = self.version(item, outcomes).await;
        (item, version, ids)
    }

    async fn version(&self, item: Uuid, outcomes: i32) -> (Uuid, Vec<Uuid>) {
        let hash = format!("{:064x}", Uuid::new_v4().as_u128());
        let version = sqlx::query_scalar("INSERT INTO source_versions(graph_id,source_item_id,source_id,source_type,provider_namespace,origin,raw_inline,source_metadata,content_sha256,extraction_status) SELECT graph_id,source_item_id,source_id,source_type,provider_namespace,origin,$2,'{\"title\":\"Atlas\"}',$2,'done' FROM source_items WHERE source_item_id=$1 RETURNING version_id")
            .bind(item).bind(hash).fetch_one(&self.owner).await.unwrap();
        let mut ids = Vec::new();
        // Fixed finite raw fixture; no cache/staging/ready writes or bulk fake counts.
        for ordinal in 0..outcomes {
            ids.push(self.outcome(version, ordinal).await);
        }
        sqlx::query("UPDATE source_items SET current_version_id=$2 WHERE source_item_id=$1")
            .bind(item)
            .bind(version)
            .execute(&self.owner)
            .await
            .unwrap();
        (version, ids)
    }

    async fn outcome(&self, version: Uuid, ordinal: i32) -> Uuid {
        sqlx::query_scalar("INSERT INTO semantic_atoms(graph_id,source_version_id,ordinal,kind,statement,extraction_version,model,entities) VALUES($1,$2,$3,'fact','Atlas listing fact','source-atoms-v1','fixture','[{\"mention\":\"Atlas\",\"type\":\"project\"}]') RETURNING semantic_atom_id")
            .bind(self.graph).bind(version).bind(ordinal).fetch_one(&self.owner).await.unwrap()
    }

    async fn node(&self, title: &str) -> Uuid {
        sqlx::query_scalar("INSERT INTO nodes(graph_id,type,title,body) VALUES($1,'entity',$2,'{}') RETURNING node_id")
            .bind(self.graph).bind(title).fetch_one(&self.owner).await.unwrap()
    }

    async fn link(&self, node: Uuid, outcome: Uuid) {
        sqlx::query(
            "INSERT INTO node_semantic_atoms(graph_id,node_id,semantic_atom_id) VALUES($1,$2,$3)",
        )
        .bind(self.graph)
        .bind(node)
        .bind(outcome)
        .execute(&self.owner)
        .await
        .unwrap();
    }

    async fn live(&self, current: bool) -> Value {
        let mut scope: LibraryScope = serde_json::from_value(json!({"graph_ids":[self.graph],"outcome_versions":if current {"current"}else{"all_retained"}})).unwrap();
        scope.normalize().unwrap();
        let sql = library_read::search_sql_for_text_explain(&scope, "");
        let mut tx = self.tx_on(&self.app).await;
        let value = sqlx::query_scalar(&sql)
            .bind(&scope.graph_ids)
            .bind(json!({})) // Exact unfiltered scope.origin_filter() encoding; SQL NULL rejects every origin.
            .bind(None::<Uuid>)
            .bind(None::<Uuid>)
            .bind(None::<Uuid>)
            .bind(None::<String>)
            .bind(None::<String>)
            .bind(None::<DateTime<Utc>>)
            .bind(None::<Uuid>)
            .bind(None::<DateTime<Utc>>)
            .bind(None::<Uuid>)
            .bind(None::<i64>)
            .bind(None::<i64>)
            .bind(None::<Uuid>)
            .bind(1000_i64)
            .bind(None::<DateTime<Utc>>)
            .fetch_one(&mut *tx)
            .await
            .unwrap();
        tx.rollback().await.unwrap();
        value
    }

    async fn assert_live(&self) {
        assert!(self.clean().await);
        let retained = self.live(false).await;
        let served = self.live(true).await;
        // The one maintained aggregate: exactly the admitted node grain, and
        // exactly what Live counts.
        let (node_count, admitted): (i64, i64) = sqlx::query_as("SELECT (SELECT node_count FROM library_listing_state WHERE graph_id=$1),(SELECT count(*) FROM library_node_evidence WHERE graph_id=$1 AND admitted_unfiltered)")
            .bind(self.graph).fetch_one(&self.owner).await.unwrap();
        assert_eq!(node_count, admitted, "the maintained node count is the admitted node grain");
        assert_eq!(node_count, retained["nodes"]["total"].as_i64().unwrap());
        let (retained_total, served_total): (i64, i64) = sqlx::query_as("SELECT COALESCE(sum(retained_count),0)::bigint,COALESCE(sum(served_count),0)::bigint FROM library_listing_source_heads WHERE graph_id=$1")
            .bind(self.graph).fetch_one(&self.owner).await.unwrap();
        assert_eq!((retained_total, served_total), (retained["outcomes"]["total"].as_i64().unwrap(), served["outcomes"]["total"].as_i64().unwrap()), "heads carry the exact per-item counts the read-time totals sum");
        for (current, live) in [(false, &retained), (true, &served)] {
            let stored: Vec<Uuid> = sqlx::query_scalar("SELECT semantic_atom_id FROM library_listing_outcomes WHERE graph_id=$1 AND (NOT $2 OR source_version_is_current) ORDER BY semantic_atom_id")
                .bind(self.graph).bind(current).fetch_all(&self.owner).await.unwrap();
            let expected: BTreeSet<Uuid> = live["outcomes"]["items"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| serde_json::from_value(v["semantic_atom_id"].clone()).unwrap())
                .collect();
            assert_eq!(stored.into_iter().collect::<BTreeSet<_>>(), expected);
        }
        let heads: Vec<Uuid> = sqlx::query_scalar("SELECT source_item_id FROM library_listing_source_heads WHERE graph_id=$1 ORDER BY source_item_id")
            .bind(self.graph).fetch_all(&self.owner).await.unwrap();
        let expected: BTreeSet<Uuid> = retained["sources"]["items"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| serde_json::from_value(v["source_item_id"].clone()).unwrap())
            .collect();
        assert_eq!(heads.into_iter().collect::<BTreeSet<_>>(), expected);
        // Totals and facets are read-time aggregates over the heads: the
        // guarded reader's answer (model path) must equal Live in both modes.
        for current in [false, true] {
            assert_eq!(self.read_path(current).await.0, "model", "a clean certified graph serves the model (current={current})");
        }
    }

    async fn close(mut self) {
        self.app.close().await;
        self.owner.close().await;
        self.admin
            .execute(format!("DROP DATABASE {} WITH(FORCE)", self.db).as_str())
            .await
            .unwrap();
        self.admin
            .execute(format!("DROP ROLE {}", self.role).as_str())
            .await
            .unwrap();
    }
}

/// A real pre-listing database with a canonical definition-two cache must not
/// become eligible merely because the additive listing migration has run.
#[tokio::test]
async fn definition_two_database_upgrades_without_certifying_unbuilt_listing_facts() {
    async fn ledger(pool: &PgPool) -> Value {
        sqlx::query_scalar("SELECT jsonb_agg(to_jsonb(m) ORDER BY version) FROM _sqlx_migrations m")
            .fetch_one(pool).await.unwrap()
    }
    async fn data(f: &Fixture) -> Value {
        // The state row is compared without the three columns 0201/0380 adds
        // (NULL on an upgraded row), so the old certificate reads the same
        // before and after the upgrade.
        sqlx::query_scalar("SELECT jsonb_build_object('items',(SELECT jsonb_agg(to_jsonb(s) ORDER BY source_item_id) FROM source_items s WHERE graph_id=$1),'versions',(SELECT jsonb_agg(to_jsonb(v) ORDER BY version_id) FROM source_versions v WHERE graph_id=$1),'outcomes',(SELECT jsonb_agg(to_jsonb(a) ORDER BY semantic_atom_id) FROM semantic_atoms a WHERE graph_id=$1),'nodes',(SELECT jsonb_agg(to_jsonb(n) ORDER BY node_id) FROM nodes n WHERE graph_id=$1),'cache',(SELECT jsonb_agg(to_jsonb(e) ORDER BY node_id) FROM library_node_evidence e WHERE graph_id=$1),'item_cache',(SELECT jsonb_agg(to_jsonb(e) ORDER BY node_id,source_item_id) FROM library_node_item_evidence e WHERE graph_id=$1),'state',(SELECT to_jsonb(s)-'capture_since'-'backfill_capture_since'-'backfill_snapshot' FROM library_evidence_state s WHERE graph_id=$1))")
            .bind(f.graph).fetch_one(&f.owner).await.unwrap()
    }
    async fn compare(f: &Fixture, expected_path: &str) {
        let scope: LibraryScope = serde_json::from_value(json!({"graph_ids":[f.graph]})).unwrap();
        let mut tx = f.app.begin_with("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY").await.unwrap();
        sqlx::query("SELECT set_config('app.account_id',$1,true),set_config('app.user_id','',true),set_config('app.tenant_actor',$2,true)")
            .bind(f.account.to_string()).bind(if f.tenant {"system"} else {""})
            .execute(&mut *tx).await.unwrap();
        library_read::configure_read_model_proof_tx(&mut tx).await.unwrap();
        let pair = library_read::compare_read_model_tx(&mut tx, &scope, "", 20, [None;3]).await.unwrap();
        assert_eq!(pair.path, expected_path, "reason={:?}", pair.fallback_reason);
        if expected_path == "live" { assert_eq!(pair.fallback_reason.as_deref(), Some("not_ready")); }
        assert_eq!(pair.live, pair.candidate);
        assert_eq!(pair.live["sources"]["total"], json!(1));
        assert_eq!(pair.live["outcomes"]["total"], json!(3));
        assert_eq!(pair.live["nodes"]["total"], json!(1));
        tx.rollback().await.unwrap();
    }
    for tenant in [false, true] {
        let f = Fixture::at_version(tenant, listing_version(tenant)).await;
        assert_eq!(sqlx::query_scalar::<_, Option<String>>("SELECT to_regclass('library_listing_state')::text")
            .fetch_one(&f.owner).await.unwrap(), None);
        let (_, _, outcomes) = f.source("Upgrade", 3).await;
        let node = f.node("Atlas upgrade").await;
        for outcome in outcomes { f.link(node, outcome).await; }
        // Build the actual old grain with the canonical production write SQL,
        // explicitly version2. store_nodes now also writes the new listing ledger
        // and therefore cannot run before the additive migration exists.
        let mut tx = f.tx_on(&f.app).await;
        assert!(worker::lock_graph(&mut tx, f.graph).await.unwrap());
        worker::configure_node_query(&mut tx).await.unwrap();
        let written: i64 = sqlx::query_scalar(worker::write_sql_for_explain())
            .bind(f.graph).bind(vec![node]).bind(2_i32).fetch_one(&mut *tx).await.unwrap();
        assert_eq!(written, 1);
        sqlx::query("INSERT INTO library_evidence_state(graph_id,ready_version,computed_account_id,computed_context,backfilled_at) VALUES($1,2,$2,library_evidence_context($1),now())")
            .bind(f.graph).bind(f.account).execute(&mut *tx).await.unwrap();
        sqlx::query("DELETE FROM library_evidence_changes WHERE graph_id=$1")
            .bind(f.graph).execute(&mut *tx).await.unwrap();
        tx.commit().await.unwrap();
        let old_data = data(&f).await;
        assert_eq!(old_data["cache"][0]["computed_version"], json!(2));
        assert_eq!(old_data["cache"][0]["atom_count_retained"], json!(3));
        assert_eq!(old_data["state"]["ready_version"], json!(2));
        let old_ledger = ledger(&f.owner).await;
        let full = migrations(tenant);
        full.run(&f.owner).await.unwrap();
        assert_eq!(data(&f).await, old_data, "migration preserves raw data and old cache/certificate");
        let upgraded_ledger = ledger(&f.owner).await;
        let old_rows = old_ledger.as_array().unwrap();
        let rows = upgraded_ledger.as_array().unwrap();
        assert_eq!(&rows[..old_rows.len()], old_rows, "all old ledger fields/checksums remain unchanged");
        assert_eq!(rows.len(), old_rows.len() + full.iter().filter(|m| m.version >= listing_version(tenant)).count());
        let actual: Vec<(i64, Vec<u8>, bool)> = sqlx::query_as("SELECT version,checksum,success FROM _sqlx_migrations ORDER BY version")
            .fetch_all(&f.owner).await.unwrap();
        let expected: Vec<_> = full.iter().map(|m| (m.version, m.checksum.to_vec(), true)).collect();
        assert_eq!(actual, expected, "complete actual SQLx ledger matches source-owned migration bytes");
        assert_eq!(sqlx::query_scalar::<_, i64>("SELECT count(*) FROM library_listing_state")
            .fetch_one(&f.owner).await.unwrap(), 0, "DDL never fabricates ready3");
        compare(&f, "live").await;
        // The upgrade opened the first generation (tenant 0209 / control
        // 0394). Node evidence certified at its own version before the
        // switch existed carries no switch generation (0201/0380: evidence
        // capture is gated by the switch, so writes while Off never reach
        // the log); the first On re-certifies it once under that generation,
        // at the same version.
        let (on, since) = f.switch().await;
        assert!(on && since.is_some(), "the upgrade opened the first generation");
        assert!(worker::stamp_listing_barrier(&f.owner).await.unwrap());
        assert!(f.drain().await > 1, "the listing channel requires actual bounded work");
        let (ready, since): (i32, Option<DateTime<Utc>>) = sqlx::query_as("SELECT ready_version,capture_since FROM library_evidence_state WHERE graph_id=$1")
            .bind(f.graph).fetch_one(&f.owner).await.unwrap();
        assert_eq!(ready, evidence::READ_MODEL_VERSION);
        assert_eq!(since, f.switch().await.1, "re-certified under the first On generation");
        f.assert_live().await;
        compare(&f, "model").await;
        eprintln!("UPGRADE_PROVED plane={} old_ledger={} new_ledger={} raw_preserved=true node_ready={} listing_ready=3 live_then_model=true", if tenant {"tenant"}else{"control"}, old_rows.len(), rows.len(), evidence::READ_MODEL_VERSION);
        f.close().await;
    }
}

#[tokio::test]
async fn bounded_raw_build_resumes_rolls_back_and_preserves_unlinked_and_carried_facts() {
    for tenant in [false, true] {
        let f = Fixture::new(tenant).await;
        let (item, old, old_ids) = f.source("Alpha", 5).await;
        let (current, current_ids) = f.version(item, 9).await;
        assert_ne!(old, current);
        // Independently witness the real caller's canonical unfiltered oracle
        // BEFORE any listing publication: SQL-NULL bind2 must not pass vacuously.
        let raw_live = f.live(false).await;
        assert_eq!(raw_live["sources"]["total"], json!(1));
        assert_eq!(raw_live["sources"]["items"][0]["source_item_id"], json!(item));
        assert_eq!(raw_live["outcomes"]["total"], json!(14));
        let raw_ids: BTreeSet<Uuid> = raw_live["outcomes"]["items"].as_array().unwrap()
            .iter().map(|row| serde_json::from_value(row["semantic_atom_id"].clone()).unwrap()).collect();
        assert_eq!(raw_ids, old_ids.iter().chain(&current_ids).copied().collect());
        assert_eq!(f.live(true).await["outcomes"]["total"], json!(9));
        let generation = f.start_item(item).await;
        let staged: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM library_listing_staged_outcomes WHERE graph_id=$1",
        )
        .bind(f.graph)
        .fetch_one(&f.owner)
        .await
        .unwrap();
        assert!(
            staged < 14,
            "small budget cannot stage whole item in first generation tick"
        );
        let mut owner = f.owner.acquire().await.unwrap();
        let before = f.snapshot(&mut owner).await;
        let mut tx = f.tx_on(&f.app).await;
        worker::refresh_graph_tx(&mut tx, f.graph, tiny())
            .await
            .unwrap();
        let during = f.snapshot(&mut tx).await;
        assert_ne!(
            during, before,
            "real durable progress occurred inside rollback probe"
        );
        tx.rollback().await.unwrap();
        assert_eq!(
            f.snapshot(&mut owner).await,
            before,
            "rollback preserves generation/cursors/guards/old counted rows"
        );
        drop(owner);
        let ticks = f.drain().await;
        assert!(
            ticks > 7,
            "14 raw outcomes require multiple bounded build/reconcile batches"
        );
        let head: Uuid = sqlx::query_scalar("SELECT generation FROM library_listing_source_heads WHERE graph_id=$1 AND source_item_id=$2")
            .bind(f.graph).bind(item).fetch_one(&f.owner).await.unwrap();
        assert_eq!(
            head, generation,
            "no-change resume keeps the same generation"
        );
        f.assert_live().await;
        let live = f.live(false).await;
        assert_eq!(
            live["outcomes"]["total"],
            json!(old_ids.len() + current_ids.len())
        );
        assert_eq!(
            f.live(true).await["outcomes"]["total"],
            json!(current_ids.len())
        );
        // Canonical origins are SQL NULL or JSON objects. An empty object is
        // non-NULL and has no content-kind fallback; JSON null must be rejected.
        let (sql_null, _, _) = f.source("SQL NULL origin", 1).await;
        let (json_object, _, _) = f.source("JSON object origin", 1).await;
        sqlx::query("UPDATE source_items SET origin=NULL WHERE source_item_id=$1")
            .bind(sql_null).execute(&f.owner).await.unwrap();
        sqlx::query("UPDATE source_items SET origin='{}'::jsonb WHERE source_item_id=$1")
            .bind(json_object).execute(&f.owner).await.unwrap();
        let rejected = sqlx::query("UPDATE source_items SET origin='null'::jsonb WHERE source_item_id=$1")
            .bind(json_object).execute(&f.owner).await.expect_err("JSON null is not a legal origin");
        let database_error = rejected.as_database_error().expect("origin constraint must reject JSON null");
        assert_eq!(database_error.code().as_deref(), Some("23514"));
        assert_eq!(database_error.constraint(), Some("source_items_origin_check"));
        let unchanged: Value = sqlx::query_scalar("SELECT origin FROM source_items WHERE source_item_id=$1")
            .bind(json_object).fetch_one(&f.owner).await.unwrap();
        assert_eq!(unchanged, json!({}), "rejected JSON null must preserve the legal object");
        f.drain().await;
        f.assert_live().await;
        assert_eq!(f.live(false).await["facets"]["unknown_origin_count"], json!(1));
        let null_origin: (bool, Option<String>) = sqlx::query_as("SELECT origin IS NULL,discovery_content_kind FROM library_listing_source_heads WHERE graph_id=$1 AND source_item_id=$2")
            .bind(f.graph).bind(sql_null).fetch_one(&f.owner).await.unwrap();
        let json_origin: (bool, bool, Option<String>) = sqlx::query_as("SELECT origin IS NULL,COALESCE(origin='{}'::jsonb,false),discovery_content_kind FROM library_listing_source_heads WHERE graph_id=$1 AND source_item_id=$2")
            .bind(f.graph).bind(json_object).fetch_one(&f.owner).await.unwrap();
        assert_eq!(null_origin, (true, Some("slack".to_owned())));
        assert_eq!(json_origin, (false, true, None));
        f.close().await;
    }
}

#[tokio::test]
async fn raw_chunk_explain_seeks_the_actual_worker_tuple_and_reads_only_limit_plus_one() {
    fn semantic_scans<'a>(v: &'a Value, out: &mut Vec<&'a Value>) {
        if v.get("Relation Name").and_then(Value::as_str) == Some("semantic_atoms") {
            out.push(v);
        }
        if let Some(rows) = v.get("Plans").and_then(Value::as_array) {
            for row in rows {
                semantic_scans(row, out);
            }
        }
    }
    for tenant in [false, true] {
        let f = Fixture::new(tenant).await;
        let (_, version, _) = f.source("Range", 513).await;
        f.owner.execute("ANALYZE semantic_atoms").await.unwrap();
        let mut tx = f.tx_on(&f.app).await;
        let sql = format!(
            "EXPLAIN(ANALYZE,BUFFERS,FORMAT JSON,TIMING OFF) {}",
            worker::listing_build_sql_for_explain()
        );
        let plan: Value = sqlx::query_scalar(&sql)
            .bind(f.graph)
            .bind(version)
            .bind(Some("source-atoms-v1"))
            .bind(Some(400_i32))
            .bind(3_i64)
            .fetch_one(&mut *tx)
            .await
            .unwrap();
        let mut scans = Vec::new();
        semantic_scans(&plan[0]["Plan"], &mut scans);
        assert!(
            !scans.is_empty(),
            "the real raw input relation must execute"
        );
        for scan in scans {
            assert_eq!(scan["Actual Loops"], json!(1));
            assert!(scan["Actual Rows"].as_u64().unwrap() <= 3);
            assert!(
                scan.get("Rows Removed by Filter")
                    .and_then(Value::as_u64)
                    .unwrap_or(0)
                    <= 1,
                "a LIMIT on output cannot conceal scanning the prior400 inputs: {scan}"
            );
            let condition = scan["Index Cond"]
                .as_str()
                .expect("physical tuple seek, not a whole-version filter");
            assert!(condition.contains("extraction_version") && condition.contains("ordinal"));
        }
        tx.rollback().await.unwrap();
        f.close().await;
    }
}

#[tokio::test]
async fn independent_change_channel_tracks_unlinked_updates_reparent_and_deletion() {
    for tenant in [false, true] {
        let f = Fixture::new(tenant).await;
        let (a, va, ids) = f.source("Alpha", 4).await;
        let (b, vb, _) = f.source("Beta", 1).await;
        let node = f.node("Atlas").await;
        f.link(node, ids[0]).await;
        f.drain().await;
        f.assert_live().await;
        sqlx::query(
            "UPDATE semantic_atoms SET source_version_id=$2,ordinal=99 WHERE semantic_atom_id=$1",
        )
        .bind(ids[1])
        .bind(vb)
        .execute(&f.owner)
        .await
        .unwrap();
        sqlx::query("DELETE FROM semantic_atoms WHERE semantic_atom_id=$1")
            .bind(ids[2])
            .execute(&f.owner)
            .await
            .unwrap();
        f.outcome(va, 200).await; // No node link: must still produce an independent listing event.
        sqlx::query("UPDATE source_versions SET source_metadata='{\"title\":\"Changed current\"}' WHERE version_id=$1")
            .bind(va).execute(&f.owner).await.unwrap();
        f.drain().await;
        f.assert_live().await;
        let moved: Uuid = sqlx::query_scalar("SELECT source_item_id FROM library_listing_outcomes WHERE graph_id=$1 AND semantic_atom_id=$2")
            .bind(f.graph).bind(ids[1]).fetch_one(&f.owner).await.unwrap();
        assert_eq!(moved, b);
        sqlx::query("DELETE FROM nodes WHERE node_id=$1")
            .bind(node)
            .execute(&f.owner)
            .await
            .unwrap();
        let (node_count, admitted): (i64, i64) = sqlx::query_as("SELECT (SELECT node_count FROM library_listing_state WHERE graph_id=$1),(SELECT count(*) FROM library_node_evidence WHERE graph_id=$1 AND admitted_unfiltered)")
            .bind(f.graph).fetch_one(&f.owner).await.unwrap();
        assert_eq!(
            node_count, admitted,
            "the node cascade's trigger delta keeps the maintained count exact without a membership row"
        );
        sqlx::query("DELETE FROM source_items WHERE source_item_id=$1")
            .bind(a)
            .execute(&f.owner)
            .await
            .unwrap();
        f.drain().await;
        f.assert_live().await;
        let missing: i64 = sqlx::query_scalar("SELECT count(*) FROM library_listing_source_heads WHERE graph_id=$1 AND source_item_id=$2")
            .bind(f.graph).bind(a).fetch_one(&f.owner).await.unwrap();
        assert_eq!(missing, 0);
        assert_eq!(f.live(false).await["outcomes"]["total"], json!(2));
        f.close().await;
    }
}

#[tokio::test]
async fn a_lower_allocated_id_committing_after_input_snapshot_survives_another_items_ack() {
    for tenant in [false, true] {
        let f = Fixture::new(tenant).await;
        let (a, va, _) = f.source("Alpha", 2).await;
        let (b, vb, _) = f.source("Beta", 9).await;
        f.drain().await;
        let mut late = f.owner.begin().await.unwrap();
        sqlx::query("UPDATE source_versions SET source_metadata='{\"title\":\"Late A\"}' WHERE version_id=$1")
            .bind(va).execute(&mut *late).await.unwrap();
        let lower: Vec<i64> = sqlx::query_scalar("SELECT id FROM library_listing_changes WHERE graph_id=$1 AND source_item_id=$2 ORDER BY id")
            .bind(f.graph).bind(a).fetch_all(&mut *late).await.unwrap();
        assert!(!lower.is_empty());
        sqlx::query("UPDATE source_versions SET source_metadata='{\"title\":\"Before snapshot B\"}' WHERE version_id=$1")
            .bind(vb).execute(&f.owner).await.unwrap();
        let higher: Vec<i64> = sqlx::query_scalar("SELECT id FROM library_listing_changes WHERE graph_id=$1 AND source_item_id=$2 ORDER BY id")
            .bind(f.graph).bind(b).fetch_all(&f.owner).await.unwrap();
        assert!(lower.iter().max().unwrap() < higher.iter().min().unwrap());
        let generation = f.start_item(b).await;
        let input: String = sqlx::query_scalar("SELECT input_snapshot::text FROM library_listing_generations WHERE graph_id=$1 AND generation=$2")
            .bind(f.graph).bind(generation).fetch_one(&f.owner).await.unwrap();
        late.commit().await.unwrap();
        let mut published = false;
        for _ in 0..2000 {
            let batch = f.step().await;
            if batch.listing_items_published > 0 {
                published = true;
                break;
            }
        }
        assert!(published);
        let remaining: Vec<i64> = sqlx::query_scalar(
            "SELECT id FROM library_listing_changes WHERE graph_id=$1 AND id=ANY($2) ORDER BY id",
        )
        .bind(f.graph)
        .bind(&lower)
        .fetch_all(&f.owner)
        .await
        .unwrap();
        assert_eq!(
            remaining, lower,
            "another item's higher-ID ACK cannot use a graph watermark"
        );
        let old_high: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM library_listing_changes WHERE graph_id=$1 AND id=ANY($2)",
        )
        .bind(f.graph)
        .bind(&higher)
        .fetch_one(&f.owner)
        .await
        .unwrap();
        assert_eq!(old_high, 0);
        let same: String = sqlx::query_scalar("SELECT input_snapshot::text FROM library_listing_generations WHERE graph_id=$1 AND generation=$2")
            .bind(f.graph).bind(generation).fetch_one(&f.owner).await.unwrap();
        assert_eq!(same, input);
        f.drain().await;
        f.assert_live().await;
        f.close().await;
    }
}

#[tokio::test]
async fn a_same_item_post_snapshot_event_restarts_without_publishing_partial_counts() {
    for tenant in [false, true] {
        let f = Fixture::new(tenant).await;
        let (item, version, _) = f.source("Restart", 13).await;
        f.drain().await;
        sqlx::query("UPDATE source_versions SET source_metadata='{\"title\":\"Rebuild\"}' WHERE version_id=$1")
            .bind(version).execute(&f.owner).await.unwrap();
        let old = f.start_item(item).await;
        let previous: (Uuid,i64) = sqlx::query_as("SELECT generation,retained_count FROM library_listing_source_heads WHERE graph_id=$1 AND source_item_id=$2")
            .bind(f.graph).bind(item).fetch_one(&f.owner).await.unwrap();
        let added = f.outcome(version, 99).await;
        let mut restarted = false;
        for _ in 0..2000 {
            let batch = f.step().await;
            restarted |= batch.listing_restarted;
            if f.clean().await {
                break;
            }
            let head: (Uuid,i64) = sqlx::query_as("SELECT generation,retained_count FROM library_listing_source_heads WHERE graph_id=$1 AND source_item_id=$2")
                .bind(f.graph).bind(item).fetch_one(&f.owner).await.unwrap();
            if head.0 == previous.0 {
                assert_eq!(
                    head, previous,
                    "old counted header stays intact before publication"
                );
            } else {
                assert_ne!(head.0, old, "abandoned input generation cannot publish");
                assert_eq!(
                    head.1, 14,
                    "publication exposes the complete replacement at once"
                );
            }
        }
        assert!(restarted);
        assert!(f.clean().await);
        let new: Uuid = sqlx::query_scalar("SELECT generation FROM library_listing_source_heads WHERE graph_id=$1 AND source_item_id=$2")
            .bind(f.graph).bind(item).fetch_one(&f.owner).await.unwrap();
        assert_ne!(new, old);
        f.assert_live().await;
        assert!(f.live(false).await["outcomes"]["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v["semantic_atom_id"] == json!(added)));
        f.close().await;
    }
}

#[tokio::test]
async fn an_open_generation_serializes_other_item_publication_and_facets_follow_the_heads() {
    for tenant in [false, true] {
        let f = Fixture::new(tenant).await;
        // Distinct labels: the container facet's label is the read-time
        // maximum over the remaining heads once an item is relabelled and
        // another deleted.
        let mut sources = Vec::new();
        for label in ["Alpha", "Beta", "Delta", "Omega"] {
            sources.push(f.source(label, 1).await.0);
        }
        f.drain().await;
        f.assert_live().await;
        let index: (bool, i16, String, String) = sqlx::query_as("SELECT i.indisunique,i.indnkeyatts,pg_get_indexdef(i.indexrelid,1,true),pg_get_expr(i.indpred,i.indrelid) FROM pg_index i WHERE i.indexrelid='library_listing_one_build'::regclass")
            .fetch_one(&f.owner).await.unwrap();
        assert!(index.0);
        assert_eq!(index.1, 1);
        assert_eq!(index.2, "graph_id");
        assert!(index.3.contains("complete") && index.3.contains("abandoned"));
        let a = sources[3];
        let b = sources[2];
        sqlx::query("UPDATE source_items SET origin=jsonb_set(origin,'{container,label}','\"Aardvark\"') WHERE source_item_id=$1")
            .bind(a).execute(&f.owner).await.unwrap();
        // More outcomes than one batch's budget: the generation stays open
        // across steps while other items' events queue behind it.
        f.version(a, 5).await;
        let active = f.start_item(a).await;
        let mut open = false;
        for _ in 0..2000 {
            if let Some((item, generation, phase)) = f.unfinished().await {
                assert_eq!((item, generation), (a, active));
                if matches!(phase.as_str(), "build" | "upsert" | "prune") {
                    open = true;
                    break;
                }
            }
            f.step().await;
        }
        assert!(open, "fixture must actually stop inside a bounded reconciliation phase");
        sqlx::query("DELETE FROM source_items WHERE source_item_id=$1")
            .bind(b)
            .execute(&f.owner)
            .await
            .unwrap();
        let queued: Vec<i64>=sqlx::query_scalar("SELECT id FROM library_listing_changes WHERE graph_id=$1 AND source_item_id=$2 ORDER BY id")
            .bind(f.graph).bind(b).fetch_all(&f.owner).await.unwrap();
        assert!(!queued.is_empty());
        let b_head:Uuid=sqlx::query_scalar("SELECT generation FROM library_listing_source_heads WHERE graph_id=$1 AND source_item_id=$2")
            .bind(f.graph).bind(b).fetch_one(&f.owner).await.unwrap();
        let mut completed = false;
        for _ in 0..2000 {
            let batch = f.step().await;
            let still:Vec<i64>=sqlx::query_scalar("SELECT id FROM library_listing_changes WHERE graph_id=$1 AND id=ANY($2) ORDER BY id")
                .bind(f.graph).bind(&queued).fetch_all(&f.owner).await.unwrap();
            assert_eq!(still, queued);
            let untouched:Uuid=sqlx::query_scalar("SELECT generation FROM library_listing_source_heads WHERE graph_id=$1 AND source_item_id=$2")
                .bind(f.graph).bind(b).fetch_one(&f.owner).await.unwrap();
            assert_eq!(untouched, b_head);
            if batch.listing_items_published > 0 {
                completed = true;
                break;
            }
            if let Some((item, generation, _)) = f.unfinished().await {
                assert_eq!((item, generation), (a, active));
            }
        }
        assert!(
            completed,
            "current generation must finish before queued item publication"
        );
        f.drain().await;
        f.assert_live().await;
        let containers = f.live(false).await["facets"]["containers"].clone();
        assert_eq!(containers.as_array().unwrap().len(), 1);
        assert_eq!(containers[0]["label"], json!("Beta"));
        assert_eq!(containers[0]["source_count"], json!(3));
        f.close().await;
    }
}

#[tokio::test]
async fn canonical_node_version_and_scope_context_restart_listing_without_rotating_epoch() {
    for tenant in [false, true] {
        let f = Fixture::new(tenant).await;
        let (item, _, outcomes) = f.source("Context", 9).await;
        for title in ["Atlas", "Beacon", "Coral"] {
            let node = f.node(title).await;
            f.link(node, outcomes[0]).await;
        }
        f.drain().await;
        f.assert_live().await;
        let epoch: Uuid =
            sqlx::query_scalar("SELECT epoch FROM library_listing_state WHERE graph_id=$1")
                .bind(f.graph)
                .fetch_one(&f.owner)
                .await
                .unwrap();
        // Model an old canonical evidence certificate, not a fabricated listing ready3.
        // The only listing certificate in this test was produced by the actual worker.
        sqlx::query("UPDATE library_evidence_state SET ready_version=1,backfill_version=1 WHERE graph_id=$1")
            .bind(f.graph).execute(&f.owner).await.unwrap();
        let mut saw_unready = false;
        for _ in 0..2000 {
            let batch = f.step_on(&f.owner).await;
            let evidence_ready: Option<i32> = sqlx::query_scalar(
                "SELECT ready_version FROM library_evidence_state WHERE graph_id=$1",
            )
            .bind(f.graph)
            .fetch_one(&f.owner)
            .await
            .unwrap();
            let listing_ready: Option<i32> = sqlx::query_scalar(
                "SELECT ready_version FROM library_listing_state WHERE graph_id=$1",
            )
            .bind(f.graph)
            .fetch_one(&f.owner)
            .await
            .unwrap();
            if evidence_ready
                != Some(coralswarm_backend::library_read::evidence::READ_MODEL_VERSION)
            {
                assert_eq!(
                    listing_ready, None,
                    "listing cannot certify stale canonical node input"
                );
                let counted: bool = sqlx::query_scalar("SELECT nodes_complete FROM library_listing_state WHERE graph_id=$1")
                    .bind(f.graph).fetch_one(&f.owner).await.unwrap();
                assert!(!counted && !batch.listing_failed,
                    "the node count waits for canonical version/account/context readiness");
                saw_unready = true;
            }
            if f.unfinished().await.is_some() {
                break;
            }
        }
        assert!(
            saw_unready,
            "three nodes with one-node budgets exercise incomplete canonical rebuild"
        );
        if f.tenant {
            // #1563: on the tenant plane the owner and the app role share one
            // access class, so only the canonical node-version half of this
            // proof applies; the role switch below restarts nothing.
            let mut owner_tx = f.tx_on(&f.owner).await;
            let owner_class: Value = sqlx::query_scalar("SELECT library_evidence_class($1)")
                .bind(f.graph).fetch_one(&mut *owner_tx).await.unwrap();
            owner_tx.rollback().await.unwrap();
            let mut app_tx = f.tx_on(&f.app).await;
            let app_class: Value = sqlx::query_scalar("SELECT library_evidence_class($1)")
                .bind(f.graph).fetch_one(&mut *app_tx).await.unwrap();
            app_tx.rollback().await.unwrap();
            assert_eq!(owner_class, app_class, "owner and app share one tenant access class");
            f.drain().await;
            assert!(f.clean().await);
            f.assert_live().await;
            f.close().await;
            continue;
        }
        let (_, old, _) = f
            .unfinished()
            .await
            .expect("owner-context listing generation started");
        let old_context:Value=sqlx::query_scalar("SELECT input_context FROM library_listing_generations WHERE graph_id=$1 AND generation=$2")
            .bind(f.graph).bind(old).fetch_one(&f.owner).await.unwrap();
        let mut app_tx = f.tx_on(&f.app).await;
        let target: Value = sqlx::query_scalar("SELECT library_evidence_class($1)")
            .bind(f.graph)
            .fetch_one(&mut *app_tx)
            .await
            .unwrap();
        app_tx.rollback().await.unwrap();
        assert_ne!(
            old_context, target,
            "actual owner/app scope change supplies a distinct context"
        );
        let mut restarted = false;
        for _ in 0..2000 {
            let batch = f.step().await;
            restarted |= batch.listing_restarted;
            let current_epoch: Uuid =
                sqlx::query_scalar("SELECT epoch FROM library_listing_state WHERE graph_id=$1")
                    .bind(f.graph)
                    .fetch_one(&f.owner)
                    .await
                    .unwrap();
            assert_eq!(
                current_epoch, epoch,
                "bounded context rebuild keeps graph incarnation stable"
            );
            if f.clean().await {
                break;
            }
        }
        assert!(restarted);
        assert!(f.clean().await);
        let complete:(Value,Uuid)=sqlx::query_as("SELECT s.computed_context,h.generation FROM library_listing_state s JOIN library_listing_source_heads h USING(graph_id) WHERE s.graph_id=$1 AND h.source_item_id=$2")
            .bind(f.graph).bind(item).fetch_one(&f.owner).await.unwrap();
        assert_eq!(complete.0, target);
        assert_ne!(complete.1, old);
        f.assert_live().await;
        f.close().await;
    }
}

#[tokio::test]
async fn mutation_between_header_capture_and_generation_insert_cannot_enter_frozen_input() {
    for tenant in [false, true] {
        let f=Fixture::new(tenant).await;
        let (item,_,_)=f.source("Initial",1).await;
        f.drain().await;
        sqlx::query("UPDATE source_items SET origin=jsonb_set(origin,'{container,label}',to_jsonb('Before'::text)) WHERE source_item_id=$1")
            .bind(item).execute(&f.owner).await.unwrap();
        let (reached,resume)=worker::pause_listing_input_for_test(f.graph).unwrap();
        let pool=f.app.clone();let graph=f.graph;let account=f.account;
        let task=tokio::spawn(async move {
            let mut tx=if tenant {begin_tenant_system_scoped_without_account(&pool).await.unwrap()}
                else {begin_account_scoped(&pool,account).await.unwrap()};
            sqlx::query("SELECT set_config('app.account_id',$1,true),set_config('app.user_id','',true)")
                .bind(account.to_string()).execute(&mut *tx).await.unwrap();
            let batch=worker::refresh_graph_tx(&mut tx,graph,tiny()).await.unwrap();
            tx.commit().await.unwrap();batch
        });
        tokio::time::timeout(Duration::from_secs(30),reached).await
            .expect("worker must reach its actual frozen-header seam").unwrap();
        // Capture writers do NOT acquire the worker's advisory lock. Commit a
        // real input mutation in precisely the header-read -> INSERT gap.
        tokio::time::timeout(Duration::from_secs(10),sqlx::query("UPDATE source_items SET origin=jsonb_set(origin,'{container,label}',to_jsonb('After'::text)) WHERE source_item_id=$1")
            .bind(item).execute(&f.owner)).await.expect("capture writer is not excluded by graph advisory lock").unwrap();
        resume.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(30),task).await.unwrap().unwrap();
        // The batch that froze the header also re-checks its input before the
        // next phase, so the post-snapshot commit is detected in the same batch
        // and ends it with the staged input still observable.
        let (_,generation,phase)=f.unfinished().await.unwrap();
        assert_eq!(phase,"discard");
        let header:String=sqlx::query_scalar("SELECT origin->'container'->>'label' FROM library_listing_staged_sources WHERE graph_id=$1 AND source_item_id=$2 AND generation=$3")
            .bind(f.graph).bind(item).bind(generation).fetch_one(&f.owner).await.unwrap();
        assert_eq!(header,"Before","the staged row really came from the earlier header snapshot");
        let later:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM library_listing_changes c JOIN library_listing_generations g ON g.graph_id=c.graph_id AND g.source_item_id=c.source_item_id WHERE g.graph_id=$1 AND g.generation=$2 AND c.id<>g.guard_change_id AND NOT pg_visible_in_snapshot(c.capture_xid,g.input_snapshot))")
            .bind(f.graph).bind(generation).fetch_one(&f.owner).await.unwrap();
        assert!(later,"the later source commit must stay outside the frozen input snapshot");
        assert!(f.step().await.listing_restarted,"a future event cannot be ACKed as part of the old header");
        f.drain().await;f.assert_live().await;
        let published:(Uuid,String)=sqlx::query_as("SELECT generation,origin->'container'->>'label' FROM library_listing_source_heads WHERE graph_id=$1 AND source_item_id=$2")
            .bind(f.graph).bind(item).fetch_one(&f.owner).await.unwrap();
        assert_ne!(published.0,generation);
        assert_eq!(published.1,"After");
        f.close().await;
    }
}

/// Throughput lever. N items, each one version, one chunk atom, one node link and
/// one semantic atom, driven with the production limits until joint readiness.
/// LISTING_BENCH_N overrides N; LISTING_BENCH_OUT receives one JSON line per batch;
/// LISTING_BENCH_DISTINCT_LABELS=1 gives every item its own container label;
/// LISTING_BENCH_PRE_MIGRATION=1 drops the seeding's change events, as for
/// sources captured before the listing migration existed.
#[tokio::test]
async fn backfill_reaches_readiness_in_fewer_batches_than_items() {
    use std::io::Write;
    use std::time::Instant;
    let flag = |name: &str| std::env::var(name).map(|v| v == "1").unwrap_or(false);
    let n: usize = std::env::var("LISTING_BENCH_N").ok().and_then(|v| v.parse().ok()).unwrap_or(24);
    let distinct_labels = flag("LISTING_BENCH_DISTINCT_LABELS");
    let f = Fixture::new(true).await;
    let stats = sqlx::query("CREATE EXTENSION IF NOT EXISTS pg_stat_statements").execute(&f.owner).await.is_ok();
    async fn calls(f: &Fixture, enabled: bool) -> Option<i64> {
        if !enabled { return None; }
        sqlx::query_scalar("SELECT COALESCE(sum(calls),0)::bigint FROM pg_stat_statements WHERE dbid=(SELECT oid FROM pg_database WHERE datname=current_database())")
            .fetch_one(&f.owner).await.ok()
    }
    for i in 0..n {
        let label = if distinct_labels { format!("Item {i}") } else { "Shared".to_owned() };
        let (_, version, outcomes) = f.source(&label, 1).await;
        let node = f.node(&format!("Node {i}")).await;
        f.link(node, outcomes[0]).await;
        let atom: Uuid = sqlx::query_scalar("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,timestamp) VALUES($1,'slack',$2,'fixture','Atlas listing chunk.',now()) RETURNING atom_id")
            .bind(f.graph).bind(format!("bench:{i}:chunk:1")).fetch_one(&f.owner).await.unwrap();
        sqlx::query("INSERT INTO source_version_chunks(graph_id,source_version_id,atom_id) VALUES($1,$2,$3)")
            .bind(f.graph).bind(version).bind(atom).execute(&f.owner).await.unwrap();
        sqlx::query("INSERT INTO node_atoms(node_id,atom_id) VALUES($1,$2)")
            .bind(node).bind(atom).execute(&f.owner).await.unwrap();
    }
    if flag("LISTING_BENCH_PRE_MIGRATION") {
        sqlx::query("DELETE FROM library_listing_changes WHERE graph_id=$1")
            .bind(f.graph).execute(&f.owner).await.unwrap();
    }
    let mut out = std::env::var("LISTING_BENCH_OUT").ok().map(|p| std::fs::File::create(p).unwrap());
    let (mut batches, mut published, mut publishing_batches) = (0usize, 0usize, 0usize);
    let started = Instant::now();
    loop {
        let before = calls(&f, stats).await;
        let clock = Instant::now();
        let mut tx = f.tx_on(&f.app).await;
        let batch = worker::refresh_graph_tx(&mut tx, f.graph, Limits::default()).await.unwrap();
        tx.commit().await.unwrap();
        let wall = clock.elapsed();
        let after = calls(&f, stats).await;
        batches += 1;
        assert!(!batch.locked);
        let budget = worker::ListingLimits::default();
        assert!(batch.listing_outcomes <= budget.outcomes && batch.listing_changes_consumed <= budget.changes);
        published += batch.listing_items_published;
        publishing_batches += usize::from(batch.listing_items_published > 0);
        if let Some(out) = out.as_mut() {
            let statements = before.zip(after).map(|(b, a)| a - b - 1);
            writeln!(out, "{}", json!({"batch":batches,"wall_us":wall.as_micros() as u64,"statements":statements,
                "nodes":batch.nodes,"listing_outcomes":batch.listing_outcomes,
                "listing_changes_consumed":batch.listing_changes_consumed,
                "listing_items_published":batch.listing_items_published,"listing_restarted":batch.listing_restarted,
                "joint_ready":batch.listing_backfill_completed})).unwrap();
        }
        if f.clean().await { break; }
        assert!(batches < 40 * n + 200, "{n} items did not reach readiness");
    }
    let total = started.elapsed();
    eprintln!("LISTING_BACKFILL_THROUGHPUT items={n} batches={batches} publishing_batches={publishing_batches} published={published} wall_ms={} batches_per_item={:.2}",
        total.as_millis(), batches as f64 / n as f64);
    assert_eq!(published, n);
    f.assert_live().await;
    assert!(batches < n, "{n} items needed {batches} batches: a batch must carry a generation through every phase its row budgets allow");
    f.close().await;
}

/// An explicit Off. One Off setting means no listing maintenance AND no
/// capture: with nothing draining it, `library_listing_changes` must not grow.
/// Off also computes no node evidence (tenant 0201 / control 0380): nothing
/// reads the model and the next On rebuilds it, so the worker only purges
/// the evidence log.
#[tokio::test]
async fn switch_off_captures_nothing_and_runs_only_node_evidence() {
    for tenant in [false, true] {
        let f = Fixture::switched_off(tenant).await;
        assert_eq!(f.switch().await, (false, None), "an explicit Off");
        let (item, _, outcomes) = f.source("Dormant", 3).await;
        let node = f.node("Atlas dormant").await;
        for outcome in &outcomes {
            f.link(node, *outcome).await;
        }
        assert_eq!(f.pending_listing_changes().await, 0, "an Off switch fires no capture");
        for _ in 0..5 {
            let batch = f.step().await;
            assert_eq!(
                (batch.listing_outcomes, batch.listing_changes_consumed, batch.listing_items_published),
                (0, 0, 0),
                "Off runs no listing work"
            );
            assert!(!batch.listing_backfill_completed && !batch.listing_restarted);
            assert_eq!((batch.nodes, batch.backfill_completed), (0, false), "Off runs no node work");
        }
        let node_rows: i64 = sqlx::query_scalar("SELECT (SELECT count(*) FROM library_node_evidence WHERE graph_id=$1)+(SELECT count(*) FROM library_evidence_changes WHERE graph_id=$1)")
            .bind(f.graph).fetch_one(&f.owner).await.unwrap();
        assert_eq!(node_rows, 0, "Off computes no node evidence and logs no evidence change");
        assert!(!f.node_evidence_clean().await, "the node grain stays uncertified while Off");
        let listing_rows: i64 = sqlx::query_scalar("SELECT (SELECT count(*) FROM library_listing_state WHERE graph_id=$1)+(SELECT count(*) FROM library_listing_source_heads WHERE graph_id=$1)+(SELECT count(*) FROM library_listing_changes WHERE graph_id=$1)")
            .bind(f.graph).fetch_one(&f.owner).await.unwrap();
        assert_eq!(listing_rows, 0, "Off writes no listing row of any kind");
        assert_eq!(f.read_path(false).await.1.as_deref(), Some("not_ready"), "the reader never trusts an Off plane");
        // On from here: capture resumes and the whole graph certifies.
        assert_eq!(worker::flip_listing_switch(&f.owner, true).await.unwrap(), Some(true));
        f.version(item, 1).await;
        assert!(f.pending_listing_changes().await > 0, "an On switch captures");
        f.drain().await;
        f.assert_live().await;
        assert_eq!(f.read_path(false).await.0, "model");
        f.close().await;
    }
}

/// The release that ships the switch On (tenant 0209 / control 0394) runs on
/// every plane, and every production plane is already On. Re-flipping one
/// would start a new generation and rebuild every graph on it.
#[tokio::test]
async fn an_on_plane_keeps_its_generation_through_the_default_on_migration() {
    async fn generation(f: &Fixture) -> Value {
        sqlx::query_scalar("SELECT jsonb_build_object('switch',(SELECT to_jsonb(w) FROM library_listing_switch w),'listing',(SELECT jsonb_agg(to_jsonb(s) ORDER BY graph_id) FROM library_listing_state s),'evidence',(SELECT jsonb_agg(to_jsonb(s) ORDER BY graph_id) FROM library_evidence_state s))")
            .fetch_one(&f.owner).await.unwrap()
    }
    for tenant in [false, true] {
        let f = Fixture::at_version(tenant, default_on_version(tenant)).await;
        assert_eq!(f.switch().await, (false, None), "below the default-on migration a plane starts Off");
        assert_eq!(worker::flip_listing_switch(&f.owner, true).await.unwrap(), Some(true));
        let (_, _, outcomes) = f.source("Already on", 2).await;
        let node = f.node("Atlas already on").await;
        for outcome in &outcomes {
            f.link(node, *outcome).await;
        }
        f.drain().await;
        assert_eq!(f.read_path(false).await.0, "model");
        let before = generation(&f).await;
        assert_eq!(before["listing"][0]["capture_since"], before["switch"]["enabled_since"]);
        migrations(tenant).run(&f.owner).await.unwrap();
        assert!(sqlx::query_scalar::<_, bool>("SELECT EXISTS(SELECT 1 FROM _sqlx_migrations WHERE version=$1 AND success)")
            .bind(default_on_version(tenant)).fetch_one(&f.owner).await.unwrap());
        assert_eq!(generation(&f).await, before, "the switch row, its generation and every certificate are untouched");
        assert_eq!(f.read_path(false).await.0, "model", "the graph still serves the model without a rebuild");
        f.close().await;
    }
}

/// A freshly provisioned plane runs the whole tree and nothing else: no
/// operator flip. It must come up On and serve the model once the worker
/// ticks it.
#[tokio::test]
async fn a_fresh_plane_ships_the_switch_on_and_an_empty_graph_becomes_model_ready() {
    for tenant in [false, true] {
        let f = Fixture::build(tenant, Start::Shipped).await;
        let t = f.worker_tenant();
        worker::tick(&t).await.unwrap();
        let (on, since) = f.switch().await;
        let stamped: Option<DateTime<Utc>> = sqlx::query_scalar("SELECT barrier_since FROM library_listing_switch WHERE barrier_xid IS NOT NULL")
            .fetch_optional(&f.owner).await.unwrap().flatten();
        assert!(on && stamped.is_some() && stamped == since, "the first tick stamps the shipped generation");
        f.settled().await;
        worker::tick(&t).await.unwrap();
        assert_eq!(f.read_path(false).await.0, "model", "one settled tick certifies an empty graph");
        assert_eq!(f.read_path(true).await.0, "model");
        let since_state: Option<DateTime<Utc>> = sqlx::query_scalar("SELECT capture_since FROM library_listing_state WHERE graph_id=$1")
            .bind(f.graph).fetch_one(&f.owner).await.unwrap();
        assert_eq!(since_state, since, "certified under the shipped generation");
        f.close().await;
    }
}

/// The switch is sticky (tenant 0202 / control 0383): each plane's row is
/// the source of truth and only the operator command flips it. Two backend
/// instances whose LIBRARY_LISTING_FACTS disagree (a rolling deploy) tick
/// the same plane and leave the row, its generation and its barrier alone;
/// the per-tick env mirror a binary from before this change ran is refused;
/// an explicit flip changes the row, starts a generation and stamps it.
#[tokio::test]
async fn instances_with_different_env_values_never_flip_the_switch() {
    async fn barrier(f: &Fixture) -> (Option<String>, Option<DateTime<Utc>>) {
        sqlx::query_as("SELECT barrier_xid::text,barrier_since FROM library_listing_switch")
            .fetch_one(&f.owner).await.unwrap()
    }
    // The UPDATE every tick of the previous binary ran (0199..0201 sync_switch).
    const OLD_MIRROR: &str = "UPDATE library_listing_switch SET enabled=$1,enabled_since=CASE WHEN $1 THEN now() END,enabled_snapshot=NULL,snapshot_since=NULL,changed_at=now() WHERE enabled IS DISTINCT FROM $1 RETURNING enabled";
    for tenant in [false, true] {
        let f = Fixture::new(tenant).await;
        let (item, _, _) = f.source("Sticky", 1).await;
        let on = f.switch().await;
        assert!(on.0, "the plane ships On");
        let stamped = barrier(&f).await;
        assert!(stamped.0.is_some() && stamped.1 == on.1, "the first tick stamped the shipped generation's barrier");
        // Instance A runs with the setting off, instance B with it on; both
        // tick this plane in turn, as during a rolling deploy.
        let t = f.worker_tenant();
        for value in ["off", "on", "off", "on", ""] {
            std::env::set_var(worker::LISTING_LEGACY_SETTING, value);
            worker::tick(&t).await.unwrap();
            assert_eq!(f.switch().await, on, "an instance with {value:?} leaves the switch and its generation");
            assert_eq!(barrier(&f).await, stamped, "and its barrier");
        }
        std::env::remove_var(worker::LISTING_LEGACY_SETTING);
        let before = f.pending_listing_changes().await;
        f.version(item, 2).await;
        assert!(f.pending_listing_changes().await > before, "the plane still captures: nothing turned it off");
        // An older instance still running mid-deploy: its mirror is refused
        // when it disagrees, and matches no row when it agrees.
        let refused = sqlx::query_scalar::<_, bool>(OLD_MIRROR).bind(false).fetch_optional(&f.app).await.unwrap_err();
        assert_eq!(refused.as_database_error().and_then(|e| e.code()).as_deref(), Some("42501"), "{refused}");
        assert_eq!(sqlx::query_scalar::<_, bool>(OLD_MIRROR).bind(true).fetch_optional(&f.app).await.unwrap(), None);
        assert_eq!(f.switch().await, on);
        // The explicit flip is the only thing that changes the row.
        assert_eq!(worker::flip_listing_switch(&f.owner, false).await.unwrap(), Some(false));
        assert_eq!(f.switch().await, (false, None));
        assert_eq!(barrier(&f).await, (None, None), "Off has no barrier");
        assert_eq!(worker::flip_listing_switch(&f.owner, false).await.unwrap(), None, "a repeated flip is a no-op");
        for value in ["on", "on"] {
            std::env::set_var(worker::LISTING_LEGACY_SETTING, value);
            worker::tick(&t).await.unwrap();
            assert_eq!(f.switch().await, (false, None), "an instance set on does not turn it back on");
        }
        std::env::remove_var(worker::LISTING_LEGACY_SETTING);
        let before = f.pending_listing_changes().await;
        f.version(item, 3).await;
        assert_eq!(f.pending_listing_changes().await, before, "Off captures nothing");
        assert_eq!(worker::flip_listing_switch(&f.owner, true).await.unwrap(), Some(true));
        let again = f.switch().await;
        assert!(again.0 && again.1 > on.1, "an explicit On starts a new generation");
        let restamped = barrier(&f).await;
        assert!(restamped.0.is_some() && restamped.1 == again.1, "stamped for the new generation");
        f.settled().await;
        f.close().await;
    }
}

/// Capture missed while off never reaches the log, so facts certified under
/// the previous switch generation are a rebuild, not a resume: the reader
/// refuses them while off, and the next On re-walks every item.
#[tokio::test]
async fn facts_certified_under_an_older_switch_generation_rebuild_when_capture_resumes() {
    for tenant in [false, true] {
        let f = Fixture::new(tenant).await;
        let (item, _, outcomes) = f.source("Before", 2).await;
        let node = f.node("Atlas before").await;
        for outcome in &outcomes {
            f.link(node, *outcome).await;
        }
        f.drain().await;
        f.assert_live().await;
        assert_eq!(f.read_path(false).await.0, "model");
        let first: Option<DateTime<Utc>> = sqlx::query_scalar("SELECT capture_since FROM library_listing_state WHERE graph_id=$1")
            .bind(f.graph).fetch_one(&f.owner).await.unwrap();
        assert_eq!(first, f.switch().await.1, "certified facts carry their switch generation");
        assert_eq!(worker::flip_listing_switch(&f.owner, false).await.unwrap(), Some(false));
        // Missed while off: a new item and a new version of the old one.
        f.source("While off", 2).await;
        f.version(item, 3).await;
        assert_eq!(f.pending_listing_changes().await, 0);
        assert_eq!(f.read_path(false).await, ("live".to_string(), Some("not_ready".to_string())), "a stale certificate is not served while off");
        assert_eq!(worker::flip_listing_switch(&f.owner, true).await.unwrap(), Some(true));
        f.settled().await;
        f.step().await;
        let (ready, since): (Option<i32>, Option<DateTime<Utc>>) = sqlx::query_as("SELECT ready_version,capture_since FROM library_listing_state WHERE graph_id=$1")
            .bind(f.graph).fetch_one(&f.owner).await.unwrap();
        assert_eq!(ready, None, "the old certificate is withdrawn on the first refresh");
        assert!(since > first, "and the state moves to the new generation");
        f.drain().await;
        f.assert_live().await;
        assert_eq!(f.live(false).await["sources"]["total"], json!(2), "the item added while off is listed");
        assert_eq!(f.read_path(false).await.0, "model");
        f.close().await;
    }
}

/// A listing invariant violation costs the listing grain alone. Node
/// evidence in the same refresh transaction still commits, the failure is
/// recorded on the graph's listing state, the graph's listing work waits
/// out a backoff without failing again, and a repaired graph recovers on
/// its own with the last failure still readable.
#[tokio::test]
async fn a_listing_protocol_violation_never_fails_node_evidence() {
    for tenant in [false, true] {
        let f = Fixture::new(tenant).await;
        let (_, _, outcomes) = f.source("Guarded", 2).await;
        let node = f.node("Atlas guarded").await;
        for outcome in &outcomes {
            f.link(node, *outcome).await;
        }
        f.drain().await;
        f.assert_live().await;
        // Stamp the certified node grain with a foreign version. The listing
        // node count refuses to take it as a baseline ("node publication
        // contains wrong-version evidence"), a protocol error raised mid-refresh.
        sqlx::query("UPDATE library_node_evidence SET computed_version=$2 WHERE graph_id=$1")
            .bind(f.graph).bind(evidence::READ_MODEL_VERSION - 1).execute(&f.owner).await.unwrap();
        sqlx::query("UPDATE library_listing_state SET ready_version=NULL,nodes_complete=false WHERE graph_id=$1")
            .bind(f.graph).execute(&f.owner).await.unwrap();
        // Fresh node work, queued for the same refresh.
        let twin = f.node("Atlas guarded twin").await;
        f.link(twin, outcomes[0]).await;
        let queued: i64 = sqlx::query_scalar("SELECT count(*) FROM library_evidence_changes WHERE graph_id=$1")
            .bind(f.graph).fetch_one(&f.owner).await.unwrap();
        assert!(queued > 0, "the twin's link queues node work");
        let batch = f.step().await;
        assert!(batch.listing_failed, "the violation is reported on the batch");
        assert_eq!(
            (batch.listing_outcomes, batch.listing_changes_consumed, batch.listing_items_published),
            (0, 0, 0),
            "the failed listing step published nothing"
        );
        let twin_version: Option<i32> = sqlx::query_scalar("SELECT computed_version FROM library_node_evidence WHERE graph_id=$1 AND node_id=$2")
            .bind(f.graph).bind(twin).fetch_one(&f.owner).await.unwrap();
        assert_eq!(twin_version, Some(evidence::READ_MODEL_VERSION), "node evidence of the same refresh committed");
        let (failures, last, retry_after): (i32, Option<String>, Option<DateTime<Utc>>) = sqlx::query_as("SELECT failure_count,last_failure,retry_after FROM library_listing_state WHERE graph_id=$1")
            .bind(f.graph).fetch_one(&f.owner).await.unwrap();
        assert_eq!(failures, 1);
        assert!(last.as_deref().unwrap_or("").contains("wrong-version"), "the failure is recorded: {last:?}");
        assert!(retry_after.unwrap() > Utc::now(), "the graph's listing work is quarantined");
        // Quarantined: the next refresh runs no listing work and does not
        // fail again, while node evidence keeps going.
        let batch = f.step().await;
        assert!(!batch.listing_failed);
        assert_eq!((batch.listing_outcomes, batch.listing_changes_consumed), (0, 0));
        let failures: i32 = sqlx::query_scalar("SELECT failure_count FROM library_listing_state WHERE graph_id=$1")
            .bind(f.graph).fetch_one(&f.owner).await.unwrap();
        assert_eq!(failures, 1, "a quarantined graph is not re-attempted");
        assert!(f.node_evidence_clean().await, "node evidence converged regardless");
        // Repair the stamp, let the backoff lapse, and the graph recovers.
        sqlx::query("UPDATE library_node_evidence SET computed_version=$2 WHERE graph_id=$1")
            .bind(f.graph).bind(evidence::READ_MODEL_VERSION).execute(&f.owner).await.unwrap();
        sqlx::query("UPDATE library_listing_state SET retry_after=NULL WHERE graph_id=$1")
            .bind(f.graph).execute(&f.owner).await.unwrap();
        f.drain().await;
        f.assert_live().await;
        let (failures, last, retry_after): (i32, Option<String>, Option<DateTime<Utc>>) = sqlx::query_as("SELECT failure_count,last_failure,retry_after FROM library_listing_state WHERE graph_id=$1")
            .bind(f.graph).fetch_one(&f.owner).await.unwrap();
        assert_eq!((failures, retry_after), (0, None), "a completed pass lifts the quarantine");
        assert!(last.is_some(), "the last failure stays readable");
        assert_eq!(f.read_path(false).await.0, "model");
        f.close().await;
    }
}

/// The hybrid reader against the independent Live reader, section by section
/// (sources, outcomes, nodes, facets with every dimension and a NULL origin,
/// prominent work), in both outcome modes, on the clean path and on the
/// dirty path (an item changed after certification, not yet drained), and
/// again after the node count had to absorb a node cascade.
#[tokio::test]
async fn hybrid_reader_equals_live_reader_clean_dirty_and_after_a_node_cascade() {
    for tenant in [false, true] {
        let f = Fixture::new(tenant).await;
        let (alpha, _, alpha_ids) = f.source("Alpha", 3).await;
        let (_beta, _, beta_ids) = f.source("Beta", 2).await;
        let (gamma, _, gamma_ids) = f.source("Gamma", 1).await;
        // A second connection/container and an item with no origin at all.
        sqlx::query("UPDATE source_items SET origin=jsonb_set(jsonb_set(origin,'{connection,id}','\"other\"'),'{container,id}','\"private\"') WHERE source_item_id=$1")
            .bind(gamma).execute(&f.owner).await.unwrap();
        let (unknown, _, unknown_ids) = f.source("Unknown", 2).await;
        sqlx::query("UPDATE source_items SET origin=NULL WHERE source_item_id=$1")
            .bind(unknown).execute(&f.owner).await.unwrap();
        let atlas = f.node("Atlas").await;
        let beacon = f.node("Beacon").await;
        for outcome in alpha_ids.iter().chain(&beta_ids) {
            f.link(atlas, *outcome).await;
        }
        f.link(beacon, gamma_ids[0]).await;
        f.link(beacon, unknown_ids[0]).await;
        f.drain().await;
        f.assert_live().await;
        let live = f.live(false).await;
        assert_eq!(live["sources"]["total"], json!(4));
        assert_eq!(live["facets"]["unknown_origin_count"], json!(1));
        assert_eq!(live["facets"]["containers"].as_array().unwrap().len(), 2, "two container buckets");
        for current in [false, true] {
            assert_eq!(f.read_path(current).await, ("model".to_string(), None));
        }
        // Dirty: a changed item and a new node, captured but not drained.
        sqlx::query("UPDATE source_versions SET source_metadata='{\"title\":\"Alpha renamed\"}' WHERE source_item_id=$1")
            .bind(alpha).execute(&f.owner).await.unwrap();
        let coral = f.node("Coral").await;
        f.link(coral, beta_ids[1]).await;
        assert!(f.pending_listing_changes().await > 0);
        for current in [false, true] {
            let (path, reason) = f.read_path(current).await;
            assert_eq!((path.as_str(), reason), ("model", None), "the dirty path answers exactly (current={current})");
        }
        f.drain().await;
        f.assert_live().await;
        // A node cascade: the maintained count follows by trigger delta.
        sqlx::query("DELETE FROM nodes WHERE node_id=$1").bind(beacon).execute(&f.owner).await.unwrap();
        f.drain().await;
        f.assert_live().await;
        assert_eq!(f.live(false).await["nodes"]["total"], json!(2));
        f.close().await;
    }
}
