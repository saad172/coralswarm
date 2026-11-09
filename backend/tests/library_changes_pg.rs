//! Busy-graph proofs (#1524 blocker 2), against real PostgreSQL.
//!
//! * The worker drain: a captured item's pending rows are coalesced into one
//!   generation, every change row is consumed exactly once, and concurrent
//!   writers lose nothing (the drained model equals Live).
//! * The `library_changes` module: distinct-key extraction and the relevance
//!   classifier, at the module level.
//! * The reader wired to it: distinct-key admission, the relevant-only
//!   overlay (contribution swap, page threshold, filtered node relevance) and
//!   the hub's bounded stale-nodes window, each against the Live oracle.
//!
//! Requires a task-private ORGT_TEST_BASE_URL on localhost. The read and the
//! classifier run under a NOBYPASSRLS app-role login, never as `postgres`.
use std::{str::FromStr, time::{Duration, Instant}};

use coralswarm_backend::{
    library_read::{self, evidence, LibraryScope},
    tenancy::scope::{begin_account_scoped, begin_tenant_system_scoped_without_account},
    workers::library_evidence::{self as worker, Batch, Limits, RefreshLimits},
};
use serde_json::{json, Value};
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    Connection, Executor, PgConnection, PgPool, Postgres, Transaction,
};
use uuid::Uuid;

struct Fixture {
    admin: PgConnection,
    owner: PgPool,
    app: PgPool,
    db: coralswarm_backend::test_db::TestDatabase,
    role: String,
    tenant: bool,
    account: Uuid,
    actor: Uuid,
    graph: Uuid,
}

fn migrations(tenant: bool) -> sqlx::migrate::Migrator {
    if tenant { sqlx::migrate!("./migrations_tenant") } else { sqlx::migrate!("./migrations_control") }
}

impl Fixture {
    async fn new(tenant: bool) -> Self {
        let base = std::env::var("ORGT_TEST_BASE_URL").expect("real private PostgreSQL required");
        let opts = PgConnectOptions::from_str(&base).unwrap();
        assert!(matches!(opts.get_host(), "127.0.0.1" | "localhost" | "::1"));
        let nonce = Uuid::new_v4().simple().to_string();
        let mut admin = PgConnection::connect_with(&opts).await.unwrap();
        let plane = if tenant { coralswarm_backend::test_db::Plane::Tenant } else { coralswarm_backend::test_db::Plane::Control };
        let db = coralswarm_backend::test_db::migrated_database(&opts, plane, "library_changes").await;
        let role = db.role("library_changes_app");
        let owner = PgPoolOptions::new().max_connections(8).connect_with(opts.clone().database(&db)).await.unwrap();
        // Cluster-level role DDL in the migrations serializes with every other
        // fixture on this server (the other library suites take the same lock).
        sqlx::query("SELECT pg_advisory_lock($1)").bind(0x0083_5C09_i64).execute(&mut admin).await.unwrap();
        // The migration ships the switch On; stamp its barrier, as the worker's first tick does.
        assert!(worker::stamp_listing_barrier(&owner).await.unwrap());
        let parent = if tenant { "coralswarm_tenant_app" } else { "coralswarm_app" };
        owner.execute(format!("CREATE ROLE {role} LOGIN INHERIT NOBYPASSRLS PASSWORD 'library-changes-fixture-only'").as_str()).await.unwrap();
        owner.execute(format!("GRANT {parent} TO {role}").as_str()).await.unwrap();
        sqlx::query("SELECT pg_advisory_unlock($1)").bind(0x0083_5C09_i64).execute(&mut admin).await.unwrap();
        let app = PgPoolOptions::new().max_connections(8)
            .connect_with(opts.database(&db).username(&role).password("library-changes-fixture-only")).await.unwrap();
        let (actor, account) = if tenant {
            sqlx::query("INSERT INTO tenant_meta(id,org_id) VALUES(true,'org_library_changes')").execute(&owner).await.unwrap();
            (Uuid::new_v4(), Uuid::new_v4())
        } else {
            let actor: Uuid = sqlx::query_scalar("INSERT INTO users(email) VALUES($1) RETURNING id")
                .bind(format!("{nonce}@fixture.invalid")).fetch_one(&owner).await.unwrap();
            let account: Uuid = sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES($1) RETURNING id")
                .bind(actor).fetch_one(&owner).await.unwrap();
            sqlx::query("INSERT INTO account_members(account_id,user_id,role) VALUES($1,$2,'owner')")
                .bind(account).bind(actor).execute(&owner).await.unwrap();
            (actor, account)
        };
        let graph = sqlx::query_scalar("INSERT INTO graphs(account_id,name,kind,owner_user_id) VALUES($1,'Library changes','member',$2) RETURNING id")
            .bind(account).bind(actor).fetch_one(&owner).await.unwrap();
        Self { admin, owner, app, db, role, tenant, account, actor, graph }
    }

    /// The worker's own scoped transaction on the app-role pool.
    async fn worker_tx(&self) -> Transaction<'static, Postgres> {
        let mut tx = if self.tenant {
            begin_tenant_system_scoped_without_account(&self.app).await.unwrap()
        } else {
            begin_account_scoped(&self.app, self.account).await.unwrap()
        };
        sqlx::query("SELECT set_config('app.account_id',$1,true),set_config('app.user_id','',true)")
            .bind(self.account.to_string()).execute(&mut *tx).await.unwrap();
        tx
    }

    async fn tick(&self, limits: RefreshLimits) -> Batch {
        let mut tx = self.worker_tx().await;
        let batch = worker::refresh_graph_tx(&mut tx, self.graph, limits).await.unwrap();
        assert!(!batch.locked);
        tx.commit().await.unwrap();
        batch
    }

    async fn clean(&self) -> bool {
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM library_listing_state WHERE graph_id=$1 AND ready_version=3) AND EXISTS(SELECT 1 FROM library_evidence_state WHERE graph_id=$1 AND ready_version=$2) AND NOT EXISTS(SELECT 1 FROM library_listing_changes WHERE graph_id=$1) AND NOT EXISTS(SELECT 1 FROM library_evidence_changes WHERE graph_id=$1) AND NOT EXISTS(SELECT 1 FROM library_listing_generations WHERE graph_id=$1 AND phase NOT IN('complete','abandoned'))")
            .bind(self.graph).bind(evidence::READ_MODEL_VERSION).fetch_one(&self.owner).await.unwrap()
    }

    /// Ticks at production limits until both logs are empty and both grains
    /// certified. Returns the per-tick reports.
    async fn drain(&self) -> Vec<(Batch, Duration)> {
        let mut ticks = Vec::new();
        for _ in 0..5000 {
            let started = Instant::now();
            let batch = self.tick(RefreshLimits::from(Limits::default())).await;
            ticks.push((batch, started.elapsed()));
            if self.clean().await {
                return ticks;
            }
        }
        panic!("the fixture did not drain at production limits");
    }

    async fn pending(&self) -> (i64, i64) {
        sqlx::query_as("SELECT (SELECT count(*) FROM library_evidence_changes WHERE graph_id=$1),(SELECT count(*) FROM library_listing_changes WHERE graph_id=$1)")
            .bind(self.graph).fetch_one(&self.owner).await.unwrap()
    }

    async fn distinct_pending(&self) -> (i64, i64) {
        sqlx::query_as("SELECT (SELECT count(*) FROM (SELECT DISTINCT node_id,source_item_id,atom_id,semantic_atom_id FROM library_evidence_changes WHERE graph_id=$1) k),(SELECT count(*) FROM (SELECT DISTINCT source_item_id,source_version_id,semantic_atom_id FROM library_listing_changes WHERE graph_id=$1 AND source_item_id IS NOT NULL) k)")
            .bind(self.graph).fetch_one(&self.owner).await.unwrap()
    }

    async fn node(&self, title: &str) -> Uuid {
        sqlx::query_scalar("INSERT INTO nodes(graph_id,type,title,body) VALUES($1,'entity',$2,'{}') RETURNING node_id")
            .bind(self.graph).bind(title).fetch_one(&self.owner).await.unwrap()
    }

    /// One captured item the way capture writes it: the item, one done
    /// version, `outcomes` semantic atoms in one statement, the current
    /// pointer, and each atom linked to one of `nodes` (one statement).
    async fn capture_on(&self, pool: &PgPool, service: &str, kind: &str, outcomes: i32, nodes: &[Uuid]) -> Uuid {
        let mut tx = pool.begin().await.unwrap();
        let source_id = Uuid::new_v4().to_string();
        let origin = json!({"service_id":service,"content_kind":kind,"provider_namespace":"changes:busy","connection":{"kind":"oauth_grant","id":"fixture"},"container":{"kind":"channel","id":"busy","provider_namespace":"changes:busy","label":"Busy"}});
        let item: Uuid = sqlx::query_scalar("INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id,origin,access_state) VALUES($1,$2,'changes:busy',$3,$4,'permitted') RETURNING source_item_id")
            .bind(self.graph).bind(service).bind(&source_id).bind(origin).fetch_one(&mut *tx).await.unwrap();
        let hash = format!("{:064x}", Uuid::new_v4().as_u128());
        let version: Uuid = sqlx::query_scalar("INSERT INTO source_versions(graph_id,source_item_id,source_id,source_type,provider_namespace,origin,raw_inline,source_metadata,content_sha256,extraction_status) SELECT graph_id,source_item_id,source_id,source_type,provider_namespace,origin,$2,'{\"title\":\"Busy\"}',$2,'done' FROM source_items WHERE source_item_id=$1 RETURNING version_id")
            .bind(item).bind(hash).fetch_one(&mut *tx).await.unwrap();
        let atoms: Vec<Uuid> = sqlx::query_scalar("INSERT INTO semantic_atoms(graph_id,source_version_id,ordinal,kind,statement,extraction_version,model,entities) SELECT $1,$2,o,'fact','Busy listing fact','source-atoms-v1','fixture','[{\"mention\":\"Busy\",\"type\":\"project\"}]' FROM generate_series(0,$3-1) o RETURNING semantic_atom_id")
            .bind(self.graph).bind(version).bind(outcomes).fetch_all(&mut *tx).await.unwrap();
        sqlx::query("UPDATE source_items SET current_version_id=$2 WHERE source_item_id=$1")
            .bind(item).bind(version).execute(&mut *tx).await.unwrap();
        if !nodes.is_empty() {
            let linked: Vec<Uuid> = (0..atoms.len()).map(|i| nodes[i % nodes.len()]).collect();
            sqlx::query("INSERT INTO node_semantic_atoms(graph_id,node_id,semantic_atom_id) SELECT $1,n,a FROM unnest($2::uuid[],$3::uuid[]) x(n,a)")
                .bind(self.graph).bind(&linked).bind(&atoms).execute(&mut *tx).await.unwrap();
        }
        tx.commit().await.unwrap();
        item
    }

    async fn capture(&self, outcomes: i32, nodes: &[Uuid]) -> Uuid {
        self.capture_on(&self.owner, "slack", "message", outcomes, nodes).await
    }

    /// The hybrid reader's equivalence oracle, under the app role, for both
    /// outcome modes. Returns the path and fallback reason of each.
    async fn read_paths(&self) -> Vec<(String, Option<String>)> {
        let mut paths = Vec::new();
        for current in [false, true] {
            let scope: LibraryScope = serde_json::from_value(json!({"graph_ids":[self.graph],"outcome_versions":if current {"current"} else {"all_retained"}})).unwrap();
            let mut tx = self.app.begin_with("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY").await.unwrap();
            sqlx::query("SELECT set_config('app.account_id',$1,true),set_config('app.user_id','',true),set_config('app.tenant_actor',$2,true)")
                .bind(self.account.to_string()).bind(if self.tenant { "system" } else { "" })
                .execute(&mut *tx).await.unwrap();
            library_read::configure_read_model_proof_tx(&mut tx).await.unwrap();
            let pair = library_read::compare_read_model_tx(&mut tx, &scope, "", 20, [None; 3]).await.unwrap();
            assert_eq!(pair.live, pair.candidate, "the hybrid reader must equal the Live reader (current={current})");
            tx.rollback().await.unwrap();
            paths.push((pair.path.to_string(), pair.fallback_reason.clone()));
        }
        paths
    }

    /// The drained listing grain is exactly the raw inventory: every permitted
    /// item has one head whose counts are its outcomes, every outcome has one
    /// active row, and the maintained node count is the admitted node grain.
    async fn assert_exact(&self) {
        assert!(self.clean().await);
        let mismatch: Value = sqlx::query_scalar(r#"SELECT jsonb_build_object(
 'heads_missing',(SELECT count(*) FROM source_items s WHERE s.graph_id=$1 AND s.access_state='permitted' AND NOT EXISTS(SELECT 1 FROM library_listing_source_heads h WHERE h.graph_id=s.graph_id AND h.source_item_id=s.source_item_id)),
 'heads_extra',(SELECT count(*) FROM library_listing_source_heads h WHERE h.graph_id=$1 AND NOT EXISTS(SELECT 1 FROM source_items s WHERE s.graph_id=h.graph_id AND s.source_item_id=h.source_item_id AND s.access_state='permitted')),
 'count_wrong',(SELECT count(*) FROM library_listing_source_heads h WHERE h.graph_id=$1 AND h.retained_count<>(SELECT count(*) FROM semantic_atoms a JOIN source_versions v ON v.graph_id=a.graph_id AND v.version_id=a.source_version_id WHERE v.graph_id=h.graph_id AND v.source_item_id=h.source_item_id)),
 'outcomes_missing',(SELECT count(*) FROM semantic_atoms a JOIN source_versions v ON v.graph_id=a.graph_id AND v.version_id=a.source_version_id JOIN source_items s ON s.graph_id=v.graph_id AND s.source_item_id=v.source_item_id AND s.access_state='permitted' WHERE a.graph_id=$1 AND NOT EXISTS(SELECT 1 FROM library_listing_outcomes o WHERE o.graph_id=a.graph_id AND o.semantic_atom_id=a.semantic_atom_id)),
 'outcomes_extra',(SELECT count(*) FROM library_listing_outcomes o WHERE o.graph_id=$1 AND NOT EXISTS(SELECT 1 FROM semantic_atoms a WHERE a.graph_id=o.graph_id AND a.semantic_atom_id=o.semantic_atom_id)),
 'node_count',(SELECT node_count FROM library_listing_state WHERE graph_id=$1)-(SELECT count(*) FROM library_node_evidence WHERE graph_id=$1 AND admitted_unfiltered),
 'nodes_missing',(SELECT count(*) FROM nodes n WHERE n.graph_id=$1 AND NOT EXISTS(SELECT 1 FROM library_node_evidence e WHERE e.graph_id=n.graph_id AND e.node_id=n.node_id)))"#)
            .bind(self.graph).fetch_one(&self.owner).await.unwrap();
        assert_eq!(mismatch, json!({"heads_missing":0,"heads_extra":0,"count_wrong":0,"outcomes_missing":0,"outcomes_extra":0,"node_count":0,"nodes_missing":0}));
        for (path, reason) in self.read_paths().await {
            assert_eq!((path.as_str(), reason), ("model", None), "a drained certified graph serves the model");
        }
    }

    async fn close(mut self) {
        self.app.close().await;
        self.owner.close().await;
        self.admin.execute(format!("DROP DATABASE {} WITH(FORCE)", self.db).as_str()).await.unwrap();
        self.admin.execute(format!("DROP ROLE {}", self.role).as_str()).await.unwrap();
    }
}

fn summarize(label: &str, ticks: &[(Batch, Duration)], backlog: (i64, i64), distinct: (i64, i64), items: usize) {
    let wall: Duration = ticks.iter().map(|t| t.1).sum();
    let node_rows: usize = ticks.iter().map(|t| t.0.changes_consumed + t.0.changes_purged).sum();
    let listing_rows: usize = ticks.iter().map(|t| t.0.listing_changes_consumed).sum();
    let published: usize = ticks.iter().map(|t| t.0.listing_items_published).sum();
    let max_tick = ticks.iter().map(|t| t.1).max().unwrap_or_default();
    eprintln!(
        "LIBRARY_DRAIN {label} items={items} backlog_node_rows={} backlog_listing_rows={} distinct_node_keys={} distinct_listing_keys={} ticks={} wall_ms={} max_tick_ms={} node_rows={node_rows} listing_rows={listing_rows} published={published} node_rows_per_tick={:.1} listing_rows_per_tick={:.1} items_per_tick={:.2} distinct_keys_per_tick={:.1}",
        backlog.0, backlog.1, distinct.0, distinct.1, ticks.len(), wall.as_millis(), max_tick.as_millis(),
        node_rows as f64 / ticks.len() as f64, listing_rows as f64 / ticks.len() as f64,
        items as f64 / ticks.len() as f64, (distinct.0 + distinct.1) as f64 / ticks.len() as f64,
    );
}

/// The measured busy shape: one new item per write, ~32 semantic atoms per
/// item, each atom linked to one of a few nodes. A standing backlog drains at
/// production limits; the report is per tick (RTT-independent) and local
/// wall time. Exactness is checked after the drain.
#[tokio::test]
async fn busy_backlog_drains_at_production_limits_and_stays_exact() {
    let f = Fixture::new(true).await;
    let mut nodes = Vec::new();
    for i in 0..8 {
        nodes.push(f.node(&format!("Busy entity {i}")).await);
    }
    f.capture(4, &nodes[..2]).await;
    f.drain().await;
    const ITEMS: usize = 40;
    for i in 0..ITEMS {
        let at = (i * 3) % nodes.len();
        f.capture(32, &[nodes[at], nodes[(at + 1) % nodes.len()], nodes[(at + 2) % nodes.len()]]).await;
    }
    let backlog = f.pending().await;
    let distinct = f.distinct_pending().await;
    let ticks = f.drain().await;
    summarize("standing_backlog", &ticks, backlog, distinct, ITEMS);
    f.assert_exact().await;
    f.close().await;
}

/// Writers keep capturing while the worker drains, on separate connections
/// and in parallel. Every change committed is consumed exactly once: when
/// the writers stop and the logs empty, the model is exactly the inventory
/// and the hybrid reader equals Live on both planes.
#[tokio::test]
async fn concurrent_writers_lose_no_change_and_the_drained_model_equals_live() {
    for tenant in [true, false] {
        let f = Fixture::new(tenant).await;
        let mut nodes = Vec::new();
        for i in 0..6 {
            nodes.push(f.node(&format!("Concurrent entity {i}")).await);
        }
        f.capture(2, &nodes[..1]).await;
        f.drain().await;
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut writers = Vec::new();
        for w in 0..3 {
            let pool = f.owner.clone();
            let graph = f.graph;
            let nodes = nodes.clone();
            let stop = stop.clone();
            writers.push(tokio::spawn(async move {
                let mut written = 0usize;
                while !stop.load(std::sync::atomic::Ordering::Relaxed) && written < 25 {
                    // Mixed writes: a new item, a re-extraction of an existing
                    // item (new atoms on its current version), a node link.
                    let mut tx = pool.begin().await.unwrap();
                    let item: Option<Uuid> = sqlx::query_scalar("SELECT source_item_id FROM source_items WHERE graph_id=$1 ORDER BY random() LIMIT 1")
                        .bind(graph).fetch_optional(&mut *tx).await.unwrap();
                    if let (Some(item), true) = (item, written % 3 == 1) {
                        let atoms: Vec<Uuid> = sqlx::query_scalar("INSERT INTO semantic_atoms(graph_id,source_version_id,ordinal,kind,statement,extraction_version,model,entities) SELECT s.graph_id,s.current_version_id,$2+o,'fact','Re-extracted','source-atoms-v2','fixture','[]' FROM source_items s CROSS JOIN generate_series(0,4) o WHERE s.source_item_id=$1 AND s.current_version_id IS NOT NULL RETURNING semantic_atom_id")
                            .bind(item).bind(1000 + (w * 1000 + written) as i32 * 10).fetch_all(&mut *tx).await.unwrap();
                        if let Some(atom) = atoms.first() {
                            sqlx::query("INSERT INTO node_semantic_atoms(graph_id,node_id,semantic_atom_id) VALUES($1,$2,$3)")
                                .bind(graph).bind(nodes[(written + w) % nodes.len()]).bind(atom).execute(&mut *tx).await.unwrap();
                        }
                        tx.commit().await.unwrap();
                    } else {
                        tx.commit().await.unwrap();
                        let source_id = Uuid::new_v4().to_string();
                        let mut tx = pool.begin().await.unwrap();
                        let item: Uuid = sqlx::query_scalar("INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id,origin,access_state) VALUES($1,'slack','changes:busy',$2,'{\"service_id\":\"slack\",\"content_kind\":\"message\"}','permitted') RETURNING source_item_id")
                            .bind(graph).bind(&source_id).fetch_one(&mut *tx).await.unwrap();
                        let version: Uuid = sqlx::query_scalar("INSERT INTO source_versions(graph_id,source_item_id,source_id,source_type,provider_namespace,raw_inline,source_metadata,content_sha256,extraction_status) SELECT graph_id,source_item_id,source_id,source_type,provider_namespace,$2,'{}',$2,'done' FROM source_items WHERE source_item_id=$1 RETURNING version_id")
                            .bind(item).bind(format!("{:064x}", Uuid::new_v4().as_u128())).fetch_one(&mut *tx).await.unwrap();
                        let atoms: Vec<Uuid> = sqlx::query_scalar("INSERT INTO semantic_atoms(graph_id,source_version_id,ordinal,kind,statement,extraction_version,model,entities) SELECT $1,$2,o,'fact','Concurrent fact','source-atoms-v1','fixture','[]' FROM generate_series(0,11) o RETURNING semantic_atom_id")
                            .bind(graph).bind(version).fetch_all(&mut *tx).await.unwrap();
                        sqlx::query("UPDATE source_items SET current_version_id=$2 WHERE source_item_id=$1").bind(item).bind(version).execute(&mut *tx).await.unwrap();
                        sqlx::query("INSERT INTO node_semantic_atoms(graph_id,node_id,semantic_atom_id) SELECT $1,$2,a FROM unnest($3::uuid[]) a")
                            .bind(graph).bind(nodes[(written + w) % nodes.len()]).bind(&atoms).execute(&mut *tx).await.unwrap();
                        tx.commit().await.unwrap();
                    }
                    written += 1;
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                written
            }));
        }
        // The worker ticks while the writers run.
        let mut ticks = 0;
        while writers.iter().any(|w| !w.is_finished()) && ticks < 2000 {
            f.tick(RefreshLimits::from(Limits::default())).await;
            ticks += 1;
        }
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        let mut written = 0;
        for w in writers {
            written += w.await.unwrap();
        }
        assert!(written > 0);
        f.drain().await;
        f.assert_exact().await;
        f.close().await;
    }
}

// ---------------------------------------------------------------------------
// The library_changes module, at the module level.
// ---------------------------------------------------------------------------

use coralswarm_backend::library_changes::{self as changes, Budgets, ChangeSet, Fallback};
use std::collections::{BTreeMap, BTreeSet};

impl Fixture {
    /// A member reader's own read transaction: the app-role login,
    /// REPEATABLE READ, the reader's account and user, no system actor. Both
    /// logs and every input are RLS policed in it.
    async fn reader_tx(&self) -> Transaction<'static, Postgres> {
        let mut tx = self.app.begin_with("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY").await.unwrap();
        sqlx::query("SELECT set_config('app.account_id',$1,true),set_config('app.user_id',$2,true),set_config('app.tenant_actor','',true)")
            .bind(self.account.to_string()).bind(self.actor.to_string()).execute(&mut *tx).await.unwrap();
        library_read::configure_read_model_proof_tx(&mut tx).await.unwrap();
        tx
    }

    async fn change_set(&self, scope: &LibraryScope, budgets: Budgets) -> ChangeSet {
        let mut tx = self.reader_tx().await;
        let set = changes::evaluate(&mut tx, scope, budgets).await.unwrap();
        tx.rollback().await.unwrap();
        set
    }

    /// Live's source items for a scope, and the reader's equality with Live
    /// on the same snapshot.
    async fn live_items(&self, scope: &LibraryScope) -> BTreeSet<Uuid> {
        let mut tx = self.reader_tx().await;
        let pair = library_read::compare_read_model_tx(&mut tx, scope, "", 200, [None; 3]).await.unwrap();
        assert_eq!(pair.live, pair.candidate, "the hybrid reader must equal Live for {scope:?}");
        tx.rollback().await.unwrap();
        assert!(pair.live["sources"]["next_cursor"].is_null(), "the fixture fits one page");
        pair.live["sources"]["items"].as_array().unwrap().iter()
            .map(|v| serde_json::from_value(v["source_item_id"].clone()).unwrap()).collect()
    }
}

fn scope(graph: Uuid, filter: Value) -> LibraryScope {
    let mut shape = json!({"graph_ids":[graph],"outcome_versions":"all_retained"});
    for (k, v) in filter.as_object().unwrap() {
        shape[k] = v.clone();
    }
    let mut scope: LibraryScope = serde_json::from_value(shape).unwrap();
    scope.normalize().unwrap();
    scope
}

/// Budgets count distinct keys, never log rows: one captured item with 32
/// atoms is one dirty item however many rows it logged, a node logged forty
/// times is one dirty node, and each budget's fallback is explicit.
#[tokio::test]
async fn distinct_key_extraction_counts_keys_not_rows_with_explicit_budget_fallbacks() {
    for tenant in [true, false] {
        let f = Fixture::new(tenant).await;
        let mut hub = Vec::new();
        for i in 0..12 {
            hub.push(f.node(&format!("Hub {i}")).await);
        }
        // A published item whose 24 atoms link to twelve nodes: its item key
        // fans out to all twelve through the stored item grain.
        let published = f.capture(24, &hub).await;
        f.drain().await;
        let all = scope(f.graph, json!({}));
        let clean = f.change_set(&all, Budgets::default()).await;
        assert_eq!((clean.fallback, clean.distinct_items, clean.items.len(), clean.nodes.len(), clean.object_rows), (None, 0, 0, 0, 0));

        let captured = f.capture(32, &[]).await;
        let (listing_rows, distinct): (i64, i64) = sqlx::query_as("SELECT count(*),count(DISTINCT source_item_id) FROM library_listing_changes WHERE graph_id=$1")
            .bind(f.graph).fetch_one(&f.owner).await.unwrap();
        assert!(listing_rows >= 34, "one captured item logged {listing_rows} listing rows");
        assert_eq!(distinct, 1);
        let set = f.change_set(&all, Budgets::default()).await;
        assert_eq!(set.fallback, None, "{set:?}");
        assert_eq!(set.distinct_items, 1, "one item, not {listing_rows} rows");
        assert_eq!(set.items.len(), 1);
        assert_eq!((set.items[0].source_item_id, set.items[0].old_in_scope, set.items[0].new_in_scope), (captured, false, true), "no head yet; the fresh header is in scope");

        // Fifty more rows of the same item, and forty of one node.
        sqlx::query("INSERT INTO library_listing_changes(graph_id,source_item_id) SELECT $1,$2 FROM generate_series(1,50)")
            .bind(f.graph).bind(captured).execute(&f.owner).await.unwrap();
        sqlx::query("INSERT INTO library_evidence_changes(graph_id,node_id) SELECT $1,$2 FROM generate_series(1,40)")
            .bind(f.graph).bind(hub[0]).execute(&f.owner).await.unwrap();
        let (listing_rows, node_rows): (i64, i64) = sqlx::query_as("SELECT (SELECT count(*) FROM library_listing_changes WHERE graph_id=$1),(SELECT count(*) FROM library_evidence_changes WHERE graph_id=$1)")
            .bind(f.graph).fetch_one(&f.owner).await.unwrap();
        assert!(listing_rows + node_rows > 128, "the raw-row admission would overflow here");
        let set = f.change_set(&all, Budgets::default()).await;
        assert_eq!((set.fallback, set.distinct_items), (None, 1));
        assert!(set.nodes.contains(&(f.graph, hub[0])));
        let distinct_nodes: BTreeSet<(Uuid, Uuid)> = set.nodes.iter().copied().collect();
        assert_eq!(distinct_nodes.len(), set.nodes.len(), "nodes are distinct");

        // Item budget: two distinct items against a budget of one.
        f.capture(1, &[]).await;
        let set = f.change_set(&all, Budgets { items: 1, ..Budgets::default() }).await;
        assert_eq!(set.fallback, Some(Fallback::ItemOverflow));
        assert!(set.items.is_empty(), "nothing is classified over budget");
        assert_eq!(f.change_set(&all, Budgets { items: 2, ..Budgets::default() }).await.fallback, None);

        // Object-row walk and object-key budgets: the captured atoms exceed four.
        assert_eq!(f.change_set(&all, Budgets { object_rows: 4, ..Budgets::default() }).await.fallback, Some(Fallback::ChangeOverflow));
        assert_eq!(f.change_set(&all, Budgets { object_keys: 4, ..Budgets::default() }).await.fallback, Some(Fallback::ChangeOverflow));

        // Hub: the published item's key resolves to its twelve nodes. Over a
        // node budget of eight that is dirty_hub; at sixteen every node is in
        // the set exactly once.
        sqlx::query("INSERT INTO library_evidence_changes(graph_id,source_item_id) VALUES($1,$2)")
            .bind(f.graph).bind(published).execute(&f.owner).await.unwrap();
        let set = f.change_set(&all, Budgets { nodes: 8, ..Budgets::default() }).await;
        assert_eq!(set.fallback, Some(Fallback::DirtyHub));
        let set = f.change_set(&all, Budgets { nodes: 16, ..Budgets::default() }).await;
        assert_eq!(set.fallback, None, "{set:?}");
        for node in &hub {
            assert!(set.nodes.contains(&(f.graph, *node)), "hub node {node} resolved");
        }

        // An unresolved parent hint (a NULL item) is reported, never traversed.
        sqlx::query("INSERT INTO library_listing_changes(graph_id,source_version_id) VALUES($1,gen_random_uuid())")
            .bind(f.graph).execute(&f.owner).await.unwrap();
        let set = f.change_set(&all, Budgets::default()).await;
        assert_eq!(set.fallback, Some(Fallback::ItemUnresolved));
        assert!(set.items.is_empty());
        f.close().await;
    }
}

/// The change logs are graph-policed: a reader sees only the changes of the
/// graphs it can see. Proved under the NOBYPASSRLS app role; the same
/// statement as the table owner sees the foreign rows, so the assertion is
/// not vacuous.
#[tokio::test]
async fn a_reader_sees_only_the_changes_of_graphs_it_can_see() {
    for tenant in [true, false] {
        let f = Fixture::new(tenant).await;
        let foreign: Uuid = if tenant {
            sqlx::query_scalar("INSERT INTO graphs(account_id,name,kind,owner_user_id) VALUES($1,'Foreign member ocean','member',$2) RETURNING id")
                .bind(f.account).bind(Uuid::new_v4()).fetch_one(&f.owner).await.unwrap()
        } else {
            let other: Uuid = sqlx::query_scalar("INSERT INTO users(email) VALUES($1) RETURNING id")
                .bind(format!("{}@fixture.invalid", Uuid::new_v4().simple())).fetch_one(&f.owner).await.unwrap();
            let account: Uuid = sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES($1) RETURNING id")
                .bind(other).fetch_one(&f.owner).await.unwrap();
            sqlx::query_scalar("INSERT INTO graphs(account_id,name,kind,owner_user_id) VALUES($1,'Foreign ocean','member',$2) RETURNING id")
                .bind(account).bind(other).fetch_one(&f.owner).await.unwrap()
        };
        sqlx::query("INSERT INTO library_listing_changes(graph_id,source_item_id) SELECT $1,gen_random_uuid() FROM generate_series(1,5)")
            .bind(foreign).execute(&f.owner).await.unwrap();
        sqlx::query("INSERT INTO library_evidence_changes(graph_id,node_id) SELECT $1,gen_random_uuid() FROM generate_series(1,5)")
            .bind(foreign).execute(&f.owner).await.unwrap();
        let mut both = scope(f.graph, json!({}));
        both.graph_ids.push(foreign);
        let set = f.change_set(&both, Budgets::default()).await;
        assert_eq!((set.distinct_items, set.nodes.len()), (0, 0), "the foreign graph's changes are invisible to the reader");
        let mut owner = f.owner.acquire().await.unwrap();
        let mut tx = owner.begin_with("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY").await.unwrap();
        let unpoliced = changes::evaluate(&mut tx, &both, Budgets::default()).await.unwrap();
        tx.rollback().await.unwrap();
        assert_eq!((unpoliced.distinct_items, unpoliced.nodes.len()), (5, 5), "the owner bypasses RLS and sees them");
        drop(owner);
        f.close().await;
    }
}

/// Relevance soundness (design §5.5 item 6 shapes), against two independent
/// oracles in the reader's own snapshot: an item's OLD image is in scope iff
/// Live listed it when it was last published (its head is that answer), and
/// its NEW image is in scope iff Live lists it now. Shapes: a writer that
/// changes only out-of-scope items, moves into and out of scope (origin,
/// collection and access), a never-published capture, and outcomes added
/// exactly at the page boundary (ties on captured_at and occurred_at).
#[tokio::test]
async fn relevance_classifier_is_sound_for_out_of_scope_writers_moves_and_boundary_ties() {
    for tenant in [true, false] {
        let f = Fixture::new(tenant).await;
        let node = f.node("Relevance").await;
        // name -> (service, content kind) at T0.
        let shapes = [
            ("leaves_slack", "slack", "message"),
            ("joins_slack", "gmail", "email"),
            ("gmail_only", "gmail", "email"),
            ("denied", "slack", "message"),
            ("becomes_meeting", "slack", "message"),
            ("meeting", "zoom", "meeting"),
            ("tie", "slack", "message"),
            ("boundary", "slack", "message"),
        ];
        let mut ids = BTreeMap::new();
        for (name, service, kind) in shapes {
            ids.insert(name, f.capture_on(&f.owner, service, kind, 3, &[node]).await);
        }
        // Ties: two published items share one captured_at, and their atoms one
        // occurred_at, so they meet at the page boundary of a page of one.
        sqlx::query("UPDATE source_versions SET captured_at='2026-10-01 12:00:00+00',occurred_at='2026-10-01 11:00:00+00' WHERE source_item_id=ANY($1)")
            .bind(vec![ids["tie"], ids["boundary"]]).execute(&f.owner).await.unwrap();
        f.drain().await;
        let scopes = [
            ("all", json!({})),
            ("slack", json!({"service_id":"slack"})),
            ("gmail", json!({"service_id":"gmail"})),
            ("meetings", json!({"collection":"meetings"})),
            ("slack_messages", json!({"service_id":"slack","content_kind":"message"})),
        ];
        let mut before = BTreeMap::new();
        for (name, filter) in &scopes {
            before.insert(*name, f.live_items(&scope(f.graph, filter.clone())).await);
        }
        assert!(before["slack"].contains(&ids["leaves_slack"]) && !before["slack"].contains(&ids["joins_slack"]));
        assert!(before["meetings"].contains(&ids["meeting"]) && !before["meetings"].contains(&ids["becomes_meeting"]));

        // The out-of-scope-only writer: new atoms on the gmail item alone.
        let gmail_version: Uuid = sqlx::query_scalar("SELECT current_version_id FROM source_items WHERE source_item_id=$1").bind(ids["gmail_only"]).fetch_one(&f.owner).await.unwrap();
        sqlx::query("INSERT INTO semantic_atoms(graph_id,source_version_id,ordinal,kind,statement,extraction_version,model,entities) SELECT $1,$2,100+o,'fact','Gmail only','source-atoms-v1','fixture','[]' FROM generate_series(0,4) o")
            .bind(f.graph).bind(gmail_version).execute(&f.owner).await.unwrap();
        for (name, filter) in [("slack", json!({"service_id":"slack"})), ("meetings", json!({"collection":"meetings"}))] {
            let set = f.change_set(&scope(f.graph, filter), Budgets::default()).await;
            assert_eq!(set.fallback, None);
            assert_eq!(set.distinct_items, 1);
            assert_eq!(set.relevant_items().count(), 0, "a writer touching only out-of-scope items leaves {name} with nothing to overlay");
            assert_eq!(set.irrelevant_items().map(|i| i.source_item_id).collect::<Vec<_>>(), vec![ids["gmail_only"]]);
        }
        assert_eq!(f.change_set(&scope(f.graph, json!({"service_id":"gmail"})), Budgets::default()).await.relevant_items().count(), 1);

        // Moves, an access flip, a fresh capture, and a boundary tie.
        let origin = |service: &str, kind: &str| json!({"service_id":service,"content_kind":kind,"provider_namespace":"changes:busy","connection":{"kind":"oauth_grant","id":"fixture"},"container":{"kind":"channel","id":"busy","provider_namespace":"changes:busy","label":"Busy"}});
        for (name, service, kind) in [("leaves_slack", "gmail", "email"), ("joins_slack", "slack", "message"), ("becomes_meeting", "slack", "meeting")] {
            sqlx::query("UPDATE source_items SET origin=$2 WHERE source_item_id=$1").bind(ids[name]).bind(origin(service, kind)).execute(&f.owner).await.unwrap();
        }
        sqlx::query("UPDATE source_items SET access_state='restricted' WHERE source_item_id=$1").bind(ids["denied"]).execute(&f.owner).await.unwrap();
        let fresh = f.capture_on(&f.owner, "slack", "message", 2, &[node]).await;
        let tie_version: Uuid = sqlx::query_scalar("SELECT current_version_id FROM source_items WHERE source_item_id=$1").bind(ids["tie"]).fetch_one(&f.owner).await.unwrap();
        sqlx::query("INSERT INTO semantic_atoms(graph_id,source_version_id,ordinal,kind,statement,extraction_version,model,entities) SELECT $1,$2,200+o,'fact','At the boundary','source-atoms-v1','fixture','[]' FROM generate_series(0,2) o")
            .bind(f.graph).bind(tie_version).execute(&f.owner).await.unwrap();

        let dirty: BTreeSet<Uuid> = sqlx::query_scalar("SELECT DISTINCT source_item_id FROM library_listing_changes WHERE graph_id=$1 AND source_item_id IS NOT NULL")
            .bind(f.graph).fetch_all(&f.owner).await.unwrap().into_iter().collect();
        assert!(dirty.contains(&fresh) && dirty.contains(&ids["tie"]) && dirty.contains(&ids["denied"]));
        for (name, filter) in &scopes {
            let s = scope(f.graph, filter.clone());
            let after = f.live_items(&s).await;
            let set = f.change_set(&s, Budgets::default()).await;
            assert_eq!(set.fallback, None, "{name}");
            let classified: BTreeSet<Uuid> = set.items.iter().map(|i| i.source_item_id).collect();
            assert_eq!(classified, dirty, "{name}: every distinct dirty item is classified exactly once");
            assert_eq!(set.items.len(), dirty.len());
            for item in &set.items {
                assert_eq!(item.old_in_scope, before[name].contains(&item.source_item_id), "{name}: old image of {}", item.source_item_id);
                assert_eq!(item.new_in_scope, after.contains(&item.source_item_id), "{name}: new image of {}", item.source_item_id);
                if !item.relevant() {
                    assert!(!before[name].contains(&item.source_item_id) && !after.contains(&item.source_item_id), "{name}: an irrelevant item is in neither answer");
                }
            }
        }
        let slack = f.change_set(&scope(f.graph, json!({"service_id":"slack"})), Budgets::default()).await;
        let by = |id: Uuid| slack.items.iter().find(|i| i.source_item_id == id).copied().unwrap();
        let images = |id: Uuid| (by(id).old_in_scope, by(id).new_in_scope);
        assert_eq!(images(ids["leaves_slack"]), (true, false), "old in / new out");
        assert_eq!(images(ids["joins_slack"]), (false, true), "old out / new in");
        assert_eq!(images(ids["denied"]), (true, false), "an access flip leaves the scope");
        assert_eq!(images(fresh), (false, true), "never published");
        assert_eq!(images(ids["tie"]), (true, true), "a boundary tie stays relevant");
        assert!(!by(ids["gmail_only"]).relevant());
        let meetings = f.change_set(&scope(f.graph, json!({"collection":"meetings"})), Budgets::default()).await;
        let joined = meetings.items.iter().find(|i| i.source_item_id == ids["becomes_meeting"]).unwrap();
        assert_eq!((joined.old_in_scope, joined.new_in_scope), (false, true), "a collection move is relevant to the collection");

        // The boundary itself, through the unchanged reader: pages of one over
        // the tied items stay equal to Live, cursor by cursor, while dirty.
        let s = scope(f.graph, json!({"service_id":"slack"}));
        let mut after: [Option<String>; 3] = [None, None, None];
        let mut pages = 0;
        loop {
            let mut tx = f.reader_tx().await;
            let pair = library_read::compare_read_model_tx(&mut tx, &s, "", 1, std::array::from_fn(|i| after[i].as_deref())).await.unwrap();
            tx.rollback().await.unwrap();
            assert_eq!(pair.live, pair.candidate, "page of one at the tie");
            let next: Vec<Option<String>> = ["sources", "outcomes", "nodes"].iter().map(|g| pair.live[*g]["next_cursor"].as_str().map(str::to_owned)).collect();
            pages += 1;
            if next.iter().all(Option::is_none) || pages >= 60 {
                break;
            }
            for (slot, next) in after.iter_mut().zip(next) {
                if next.is_some() {
                    *slot = next;
                }
            }
        }
        assert!(pages > 3, "the walk crossed the tied boundary");
        // Draining clears the change set; the reader serves the model.
        f.drain().await;
        let set = f.change_set(&s, Budgets::default()).await;
        assert_eq!((set.fallback, set.distinct_items, set.nodes.len()), (None, 0, 0));
        f.assert_exact().await;
        f.close().await;
    }
}

// ---------------------------------------------------------------------------
// The reader wired to the change set (design §3.3 B-ii steps 5-8, §3.5).
// ---------------------------------------------------------------------------

impl Fixture {
    /// The certified class's reader on the NOBYPASSRLS app-role login, as
    /// `read_paths` reads it: the graph's account, no user, the system actor
    /// on the tenant plane. Every input and both logs stay RLS policed.
    async fn oracle_tx(&self) -> Transaction<'static, Postgres> {
        let mut tx = self.app.begin_with("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY").await.unwrap();
        sqlx::query("SELECT set_config('app.account_id',$1,true),set_config('app.user_id','',true),set_config('app.tenant_actor',$2,true)")
            .bind(self.account.to_string()).bind(if self.tenant { "system" } else { "" }).execute(&mut *tx).await.unwrap();
        library_read::configure_read_model_proof_tx(&mut tx).await.unwrap();
        tx
    }

    /// One page through the Live == Model oracle, as the member reader.
    async fn compare(&self, scope: &LibraryScope, query: &str, limit: i64, after: &[Option<String>; 3]) -> library_read::ModelComparison {
        let mut tx = self.oracle_tx().await;
        let pair = library_read::compare_read_model_tx(&mut tx, scope, query, limit, std::array::from_fn(|i| after[i].as_deref())).await.unwrap();
        tx.rollback().await.unwrap();
        pair
    }

    /// Up to `pages` cursor pages of one shape: every page is served by the
    /// Model, carries no stale marker, and is byte-equal to Live.
    async fn walk_exact(&self, scope: &LibraryScope, query: &str, limit: i64, pages: usize) {
        let mut after: [Option<String>; 3] = [None, None, None];
        for page in 0..pages {
            let pair = self.compare(scope, query, limit, &after).await;
            let shape = format!("{scope:?} query={query:?} limit={limit} page={page}");
            assert_eq!((pair.path.as_str(), pair.fallback_reason.as_deref()), ("model", None), "{shape}");
            assert!(pair.freshness.is_none(), "{shape}: an exact overlay is never marked stale");
            assert_eq!(pair.live, pair.candidate, "{shape}");
            let next: Vec<Option<String>> = ["sources", "outcomes", "nodes"].iter().map(|g| pair.live[*g]["next_cursor"].as_str().map(str::to_owned)).collect();
            if next.iter().all(Option::is_none) {
                return;
            }
            for (slot, next) in after.iter_mut().zip(next) {
                if next.is_some() {
                    *slot = next;
                }
            }
        }
    }

    /// The dirty admission's verdict and overlaid keys for one scope.
    async fn overlay(&self, scope: &LibraryScope) -> Value {
        let mut tx = self.oracle_tx().await;
        let keys = library_read::overlay_keys_for_test(&mut tx, scope).await.unwrap();
        tx.rollback().await.unwrap();
        keys
    }

    /// EXPLAIN ANALYZE of the dirty statement for an unfiltered, empty query
    /// (no origin filter, no collection), first page.
    async fn explain_dirty(&self, scope: &LibraryScope, limit: i64) -> Value {
        let mut tx = self.oracle_tx().await;
        sqlx::query("SET LOCAL plan_cache_mode=force_custom_plan").execute(&mut *tx).await.unwrap();
        let sql = format!("EXPLAIN(ANALYZE,FORMAT JSON) {}", library_read::read_model::definition_dirty_sql(scope, ""));
        let none_at = None::<chrono::DateTime<chrono::Utc>>;
        let plan: Value = sqlx::query_scalar(&sql).bind(&scope.graph_ids).bind(json!({})).bind(None::<Uuid>).bind(None::<Uuid>).bind(None::<Uuid>)
            .bind(None::<String>).bind(None::<String>).bind(none_at).bind(None::<Uuid>).bind(none_at).bind(None::<Uuid>)
            .bind(None::<i64>).bind(None::<i64>).bind(None::<Uuid>).bind(limit + 1).bind(none_at)
            .fetch_one(&mut *tx).await.unwrap();
        tx.rollback().await.unwrap();
        plan
    }
}

fn overlaid(keys: &Value, field: &str) -> BTreeSet<Uuid> {
    keys[field].as_array().unwrap().iter().map(|k| serde_json::from_value(k[1].clone()).unwrap()).collect()
}

fn cte_rows(plan: &Value, name: &str) -> i64 {
    fn find<'a>(v: &'a Value, name: &str) -> Option<&'a Value> {
        if v["Subplan Name"].as_str() == Some(name) {
            return Some(v);
        }
        v["Plans"].as_array().and_then(|p| p.iter().find_map(|c| find(c, name)))
    }
    find(&plan[0]["Plan"], name).unwrap_or_else(|| panic!("{name} in the plan"))["Actual Rows"].as_i64().unwrap()
}

/// Busy churn through the wired reader (design §5.5 items 6-7). A backlog
/// the raw-row admission refused (more than 128 log rows, more than 64
/// items) is pending: 70 out-of-scope captures, re-extractions above and
/// below the page threshold, a move out of scope, an access flip and node
/// renames. Every shape (five filters, both outcome modes, empty and text
/// queries, pages of one and four, walked by cursor) stays on the Model and
/// byte-equal to Live on both planes, under the NOBYPASSRLS member reader.
/// The exported overlay sets prove the relevance is not vacuous: a filtered
/// read overlays no out-of-scope capture and no node reached only by them,
/// and the ranked page materializes fresh rows only for items that can reach
/// it (step 6).
#[tokio::test]
async fn busy_churn_is_admitted_on_distinct_keys_and_every_shape_stays_exact() {
    for tenant in [true, false] {
        let f = Fixture::new(tenant).await;
        let mut nodes = Vec::new();
        for i in 0..6 {
            nodes.push(f.node(&format!("Busy entity {i}")).await);
        }
        // Reached only by gmail items: dirty under the churn, irrelevant to
        // slack and meeting reads.
        let mail_node = f.node("Mail only").await;
        let (mut slack, mut gmail, mut meetings) = (Vec::new(), Vec::new(), Vec::new());
        for i in 0..6 {
            slack.push(f.capture_on(&f.owner, "slack", "message", 3, &[nodes[i % 3], nodes[3]]).await);
            gmail.push(f.capture_on(&f.owner, "gmail", "email", 2, &[mail_node]).await);
            meetings.push(f.capture_on(&f.owner, "zoom", "meeting", 2, &[nodes[4 + i % 2]]).await);
        }
        // Old, distinct-in-pairs capture times (two items tie on each), a
        // third of them with no occurred_at: the three sort keys meet at the
        // page boundaries.
        sqlx::query("UPDATE source_versions v SET captured_at='2026-09-01'::timestamptz+make_interval(hours=>(o.n/2)::int),occurred_at=CASE WHEN o.n%3=0 THEN NULL ELSE '2026-08-01'::timestamptz+make_interval(hours=>(o.n/2)::int) END FROM (SELECT source_item_id,row_number() OVER(ORDER BY source_item_id) n FROM source_items WHERE graph_id=$1) o WHERE v.graph_id=$1 AND v.source_item_id=o.source_item_id")
            .bind(f.graph).execute(&f.owner).await.unwrap();
        f.drain().await;

        // The churn. Nothing below is drained until the reads are done.
        let mut fresh = Vec::new();
        for _ in 0..70 {
            fresh.push(f.capture_on(&f.owner, "gmail", "email", 2, &[mail_node]).await);
        }
        let extract = |item: Uuid, ordinal: i32, node: Uuid| {
            let pool = f.owner.clone();
            let graph = f.graph;
            async move {
                let atoms: Vec<Uuid> = sqlx::query_scalar("INSERT INTO semantic_atoms(graph_id,source_version_id,ordinal,kind,statement,extraction_version,model,entities) SELECT s.graph_id,s.current_version_id,$2+o,'fact','Busy re-extracted','source-atoms-v2','fixture','[]' FROM source_items s CROSS JOIN generate_series(0,2) o WHERE s.source_item_id=$1 RETURNING semantic_atom_id")
                    .bind(item).bind(ordinal).fetch_all(&pool).await.unwrap();
                sqlx::query("INSERT INTO node_semantic_atoms(graph_id,node_id,semantic_atom_id) SELECT $1,$2,a FROM unnest($3::uuid[]) a")
                    .bind(graph).bind(node).bind(&atoms).execute(&pool).await.unwrap();
            }
        };
        // Re-extractions of old items (below a full page's threshold) and of
        // one moved to the top by a fresh occurred_at.
        extract(slack[0], 100, nodes[0]).await;
        extract(meetings[1], 100, nodes[5]).await;
        sqlx::query("UPDATE source_versions SET occurred_at=now() WHERE source_item_id=$1").bind(slack[5]).execute(&f.owner).await.unwrap();
        extract(slack[5], 100, nodes[1]).await;
        let origin = json!({"service_id":"gmail","content_kind":"email","provider_namespace":"changes:busy","connection":{"kind":"oauth_grant","id":"fixture"},"container":{"kind":"channel","id":"busy","provider_namespace":"changes:busy","label":"Busy"}});
        sqlx::query("UPDATE source_items SET origin=$2 WHERE source_item_id=$1").bind(slack[2]).bind(origin).execute(&f.owner).await.unwrap();
        sqlx::query("UPDATE source_items SET access_state='restricted' WHERE source_item_id=$1").bind(gmail[0]).execute(&f.owner).await.unwrap();
        sqlx::query("UPDATE nodes SET title='Busy entity renamed' WHERE node_id=$1").bind(nodes[4]).execute(&f.owner).await.unwrap();
        sqlx::query("UPDATE nodes SET title='Mail renamed' WHERE node_id=$1").bind(mail_node).execute(&f.owner).await.unwrap();
        let (node_rows, listing_rows, items): (i64, i64, i64) = sqlx::query_as("SELECT (SELECT count(*) FROM library_evidence_changes WHERE graph_id=$1),(SELECT count(*) FROM library_listing_changes WHERE graph_id=$1),(SELECT count(DISTINCT source_item_id) FROM library_listing_changes WHERE graph_id=$1)")
            .bind(f.graph).fetch_one(&f.owner).await.unwrap();
        assert!(node_rows + listing_rows > 128 && items > 64, "the raw-row admission refused this backlog: {node_rows}+{listing_rows} rows, {items} items");

        // Relevance is not vacuous: what each read overlays.
        let slack_scope = scope(f.graph, json!({"service_id":"slack"}));
        let keys = f.overlay(&slack_scope).await;
        assert_eq!((keys["reason"].clone(), keys["hub"].clone()), (Value::Null, json!(false)), "{keys}");
        let items = overlaid(&keys, "items");
        assert!(fresh.iter().all(|i| !items.contains(i)), "no out-of-scope capture is overlaid on the slack read");
        assert!(items.contains(&slack[0]) && items.contains(&slack[2]) && items.contains(&slack[5]), "re-extracted, moved-out and re-ranked slack items are");
        assert!(!items.contains(&gmail[0]) && !items.contains(&meetings[1]));
        let nodes_overlaid = overlaid(&keys, "nodes");
        assert!(!nodes_overlaid.contains(&mail_node), "a node reached only by out-of-scope items is not overlaid on a filtered read");
        assert!(nodes_overlaid.contains(&nodes[0]) && nodes_overlaid.contains(&nodes[1]));
        assert!(keys["change_nodes"].as_i64().unwrap() > nodes_overlaid.len() as i64);
        let meeting_keys = f.overlay(&scope(f.graph, json!({"collection":"meetings"}))).await;
        assert!(overlaid(&meeting_keys, "nodes").contains(&nodes[4]) && !overlaid(&meeting_keys, "nodes").contains(&mail_node));
        let all = scope(f.graph, json!({}));
        let all_keys = f.overlay(&all).await;
        let all_nodes = overlaid(&all_keys, "nodes");
        assert!(all_nodes.contains(&mail_node), "an unfiltered read overlays every dirty node");
        assert_eq!(all_nodes.len() as i64, all_keys["change_nodes"].as_i64().unwrap());
        assert!(fresh.iter().all(|i| overlaid(&all_keys, "items").contains(i)), "every capture is relevant to the whole ocean");

        // Step 6: the ranked whole-ocean page of one materializes fresh rows
        // only for the items that can reach it, not for all 70+ fresh items.
        let plan = f.explain_dirty(&all, 1).await;
        let (fresh_items, row_items) = (cte_rows(&plan, "CTE model_listing_fresh"), cte_rows(&plan, "CTE model_listing_row_items"));
        assert!(fresh_items > 70 && row_items >= 1 && row_items < 10, "fresh headers {fresh_items}, items with fresh rows {row_items}");

        for filter in [json!({}), json!({"service_id":"slack"}), json!({"service_id":"gmail"}), json!({"collection":"meetings"}), json!({"service_id":"slack","content_kind":"message"})] {
            for mode in ["all_retained", "current"] {
                let mut s = scope(f.graph, filter.clone());
                s.outcome_versions = Some(serde_json::from_value(json!(mode)).unwrap());
                for query in ["", "Busy"] {
                    for limit in [1, 4] {
                        f.walk_exact(&s, query, limit, 4).await;
                    }
                }
            }
        }
        f.drain().await;
        f.assert_exact().await;
        f.close().await;
    }
}

/// The hub (design §3.5, founder decision 2026-10-28; §5.5 item 8). One
/// change whose node fan-out is over the node budget serves the stored nodes
/// for a bounded window: the response is marked `updating` for `nodes`,
/// sources, outcomes, totals and facets stay byte-equal to Live, and the
/// window is measured from the oldest pending node-grain change. Past it the
/// read is exact Live (`dirty_hub`); a removal during a hub is Live at once;
/// drained, the Model is exact and unmarked.
#[tokio::test]
async fn a_hub_serves_stale_nodes_marked_updating_with_exact_listing_then_live_past_the_window() {
    use library_read::read_model::{CHANGE_NODE_LIMIT, HUB_STALE_SECS};
    for tenant in [true, false] {
        let f = Fixture::new(tenant).await;
        let width = CHANGE_NODE_LIMIT + 40;
        let hub_nodes: Vec<Uuid> = sqlx::query_scalar("INSERT INTO nodes(graph_id,type,title,body) SELECT $1,'entity','Hub '||n::text,'{}'::jsonb FROM generate_series(1,$2) n RETURNING node_id")
            .bind(f.graph).bind(width as i32).fetch_all(&f.owner).await.unwrap();
        // One item with one outcome on each hub node, and two ordinary items.
        let hub = f.capture(width as i32, &hub_nodes).await;
        let other = f.capture(3, &hub_nodes[..2]).await;
        f.capture_on(&f.owner, "gmail", "email", 2, &hub_nodes[2..3]).await;
        f.drain().await;
        // A relabel of the hub item: one item key, whose stored item grain
        // reaches every hub node.
        sqlx::query("UPDATE source_items SET origin=jsonb_set(origin,'{container,label}','\"Relabelled\"') WHERE source_item_id=$1").bind(hub).execute(&f.owner).await.unwrap();
        let all = scope(f.graph, json!({}));
        let keys = f.overlay(&all).await;
        assert_eq!((keys["reason"].clone(), keys["hub"].clone()), (Value::Null, json!(true)), "{keys}");
        assert!(overlaid(&keys, "items").contains(&hub), "the hub is one exact dirty item");
        assert!(overlaid(&keys, "nodes").is_empty(), "no node is recomputed under a hub");
        let stale_since: chrono::DateTime<chrono::Utc> = sqlx::query_scalar("SELECT min(created_at) FROM library_evidence_changes WHERE graph_id=$1")
            .bind(f.graph).fetch_one(&f.owner).await.unwrap();
        let none: [Option<String>; 3] = [None, None, None];
        for (filter, query) in [(json!({}), ""), (json!({}), "Busy"), (json!({"service_id":"slack"}), ""), (json!({"service_id":"gmail"}), ""), (json!({"collection":"meetings"}), "")] {
            let s = scope(f.graph, filter.clone());
            let pair = f.compare(&s, query, 5, &none).await;
            assert_eq!((pair.path.as_str(), pair.fallback_reason.as_deref()), ("model", None), "{filter} {query:?}");
            let freshness = pair.freshness.clone().expect("a stale serve carries its marker");
            assert_eq!((freshness["state"].clone(), freshness["components"].clone()), (json!("updating"), json!(["nodes"])));
            let since = chrono::DateTime::parse_from_rfc3339(freshness["stale_since"].as_str().unwrap()).unwrap();
            assert_eq!(since, stale_since, "the window starts at the oldest pending node-grain change");
            assert_eq!(library_read::exact_components(&pair.live), library_read::exact_components(&pair.candidate), "{filter} {query:?}: everything but the nodes stays exact under a hub");
            assert!(pair.candidate.get("freshness").is_none(), "the marker is never part of the compared value");
        }
        // Past the window: exact Live.
        sqlx::query("UPDATE library_evidence_changes SET created_at=created_at-make_interval(secs=>$2) WHERE graph_id=$1")
            .bind(f.graph).bind((HUB_STALE_SECS + 1) as f64).execute(&f.owner).await.unwrap();
        let pair = f.compare(&all, "", 5, &none).await;
        assert_eq!((pair.path.as_str(), pair.fallback_reason.as_deref()), ("live", Some("dirty_hub")));
        assert_eq!(pair.live, pair.candidate);
        assert!(pair.freshness.is_none());
        // Back inside the window, a removal (an item no longer permitted)
        // never rides a stale window: exact Live at once.
        sqlx::query("UPDATE library_evidence_changes SET created_at=statement_timestamp() WHERE graph_id=$1").bind(f.graph).execute(&f.owner).await.unwrap();
        assert!(f.compare(&all, "", 5, &none).await.freshness.is_some());
        sqlx::query("UPDATE source_items SET access_state='restricted' WHERE source_item_id=$1").bind(other).execute(&f.owner).await.unwrap();
        let pair = f.compare(&all, "", 5, &none).await;
        assert_eq!((pair.path.as_str(), pair.fallback_reason.as_deref()), ("live", Some("dirty_hub")));
        assert_eq!(pair.live, pair.candidate);
        // Drained: the Model is exact again, with no marker.
        f.drain().await;
        f.walk_exact(&all, "", 5, 3).await;
        f.assert_exact().await;
        f.close().await;
    }
}

/// A big node (founder decision 2026-10-28): one dirty node with more
/// physical links than the node raw-edge budget gets the hub's window. Its
/// stored row is served stale and marked, everything else stays byte-equal
/// to Live; past the window the read is exact Live (`raw_edge_overflow`),
/// and a removal during the window is Live at once.
#[tokio::test]
async fn a_big_node_gets_the_hub_window_and_falls_back_past_it() {
    use library_read::read_model::{HUB_STALE_SECS, RAW_EDGE_LIMIT};
    for tenant in [true, false] {
        let f = Fixture::new(tenant).await;
        let big = f.node("Big entity").await;
        let small = f.node("Small entity").await;
        f.capture((RAW_EDGE_LIMIT + 20) as i32, &[big]).await;
        let other = f.capture(3, &[big, small]).await;
        f.drain().await;
        sqlx::query("UPDATE nodes SET title='Big entity renamed' WHERE node_id=$1").bind(big).execute(&f.owner).await.unwrap();
        let all = scope(f.graph, json!({}));
        let keys = f.overlay(&all).await;
        assert_eq!((keys["reason"].clone(), keys["hub"].clone()), (Value::Null, json!(true)), "{keys}");
        assert!(overlaid(&keys, "nodes").is_empty(), "the big node is not recomputed");
        let none: [Option<String>; 3] = [None, None, None];
        for (filter, query) in [(json!({}), ""), (json!({}), "Big"), (json!({"service_id":"slack"}), "")] {
            let pair = f.compare(&scope(f.graph, filter.clone()), query, 5, &none).await;
            assert_eq!((pair.path.as_str(), pair.fallback_reason.as_deref()), ("model", None), "{filter} {query:?}");
            let freshness = pair.freshness.clone().expect("a stale serve carries its marker");
            assert_eq!((freshness["state"].clone(), freshness["components"].clone()), (json!("updating"), json!(["nodes"])));
            assert_eq!(library_read::exact_components(&pair.live), library_read::exact_components(&pair.candidate), "{filter} {query:?}");
        }
        sqlx::query("UPDATE library_evidence_changes SET created_at=created_at-make_interval(secs=>$2) WHERE graph_id=$1")
            .bind(f.graph).bind((HUB_STALE_SECS + 1) as f64).execute(&f.owner).await.unwrap();
        let pair = f.compare(&all, "", 5, &none).await;
        assert_eq!((pair.path.as_str(), pair.fallback_reason.as_deref()), ("live", Some("raw_edge_overflow")));
        assert_eq!(pair.live, pair.candidate);
        sqlx::query("UPDATE library_evidence_changes SET created_at=statement_timestamp() WHERE graph_id=$1").bind(f.graph).execute(&f.owner).await.unwrap();
        assert!(f.compare(&all, "", 5, &none).await.freshness.is_some());
        sqlx::query("UPDATE source_items SET access_state='restricted' WHERE source_item_id=$1").bind(other).execute(&f.owner).await.unwrap();
        let pair = f.compare(&all, "", 5, &none).await;
        assert_eq!((pair.path.as_str(), pair.fallback_reason.as_deref()), ("live", Some("raw_edge_overflow")), "a removal is never served stale");
        assert_eq!(pair.live, pair.candidate);
        f.drain().await;
        f.walk_exact(&all, "", 5, 3).await;
        f.assert_exact().await;
        f.close().await;
    }
}
