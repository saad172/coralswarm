//! Statement co-occurrence edges (`co_occurs_with`, `coralswarm_backend::co_occurrence`)
//! on real local Postgres: the org tenant plane as `coralswarm_tenant_app`
//! (system scope) and as the table owner (production's `neondb_owner`
//! shape), and the consumer plane as `coralswarm_app`.
//!
//! `RELINFER_TEST_DATABASE_URL` names any database on a local server; every
//! test creates and drops its own databases. With `REQUIRE_PG_SUITE` set a
//! missing URL fails instead of skipping.
mod support;

use std::collections::BTreeSet;
use std::str::FromStr;

use coralswarm_backend::{
    co_occurrence::{self, Refresh, Write},
    jobs::{queue, JobKind},
    relationships::EdgeStatus,
    store::{PostgresStore, Store},
    tenancy::{
        scope::{begin_account_scoped, begin_tenant_system_scoped_without_account},
        TenantId,
    },
    workers::inference_closure,
};
use serde_json::{json, Value};
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    PgPool, Postgres, Transaction,
};
use uuid::Uuid;

fn base() -> Option<PgConnectOptions> {
    let Ok(url) = std::env::var("RELINFER_TEST_DATABASE_URL") else {
        assert!(std::env::var_os("REQUIRE_PG_SUITE").is_none(), "RELINFER_TEST_DATABASE_URL required");
        eprintln!("skipping co-occurrence test: set RELINFER_TEST_DATABASE_URL");
        return None;
    };
    let opts = PgConnectOptions::from_str(&url).unwrap();
    assert!(matches!(opts.get_host(), "127.0.0.1" | "localhost" | "::1"), "test database must be local");
    Some(opts)
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Role {
    App,
    Owner,
}

struct Plane {
    org: bool,
    owner: PgPool,
    _db: coralswarm_backend::test_db::TestDatabase,
    app: PgPool,
    account: Uuid,
    graph: Uuid,
}

impl Plane {
    async fn tenant(opts: &PgConnectOptions) -> Self {
        let admin = PgPoolOptions::new().max_connections(1).connect_with(opts.clone()).await.unwrap();
        let name = coralswarm_backend::test_db::TestDatabase::new(opts, "managed_atoms");
        sqlx::query(&format!("CREATE DATABASE {name}")).execute(&admin).await.unwrap();
        admin.close().await;
        let owner = PgPoolOptions::new().max_connections(4).connect_with(opts.clone().database(&name)).await.unwrap();
        sqlx::migrate!("./migrations_tenant").run(&owner).await.unwrap();
        support::tenant_role::set_app_role_password_with_retry(&owner).await;
        let app = PgPoolOptions::new()
            .max_connections(4)
            .connect_with(
                opts.clone()
                    .database(&name)
                    .username("coralswarm_tenant_app")
                    .password(support::tenant_role::APP_ROLE_TEST_PASSWORD),
            )
            .await
            .unwrap();
        let account = Uuid::new_v4();
        let graph = Self::tenant_graph(&owner, account).await;
        Self { org: true, owner, _db: name, app, account, graph }
    }

    async fn tenant_graph(owner: &PgPool, account: Uuid) -> Uuid {
        sqlx::query_scalar("INSERT INTO graphs(account_id,name,kind,owner_user_id) VALUES($1,'Co-occurrence','member',$2) RETURNING id")
            .bind(account)
            .bind(Uuid::new_v4())
            .fetch_one(owner)
            .await
            .unwrap()
    }

    async fn consumer(opts: &PgConnectOptions) -> Self {
        let (name, owner) = support::managed_extraction::isolated_database(opts).await;
        support::app_role::set_app_role_password_with_retry(&owner).await;
        let app = PgPoolOptions::new()
            .max_connections(4)
            .connect_with(
                owner
                    .connect_options()
                    .as_ref()
                    .clone()
                    .username("coralswarm_app")
                    .password(support::app_role::APP_ROLE_TEST_PASSWORD),
            )
            .await
            .unwrap();
        let account = Self::consumer_account(&owner).await;
        let graph: Uuid = sqlx::query_scalar("INSERT INTO graphs(account_id,name) VALUES($1,'Co-occurrence') RETURNING id")
            .bind(account)
            .fetch_one(&owner)
            .await
            .unwrap();
        Self { org: false, owner, _db: name, app, account, graph }
    }

    async fn consumer_account(owner: &PgPool) -> Uuid {
        let suffix = Uuid::new_v4();
        let user: Uuid = sqlx::query_scalar("INSERT INTO users(email,clerk_user_id) VALUES($1,$2) RETURNING id")
            .bind(format!("co-occurrence-{suffix}@test.local"))
            .bind(format!("co-occurrence-{suffix}"))
            .fetch_one(owner)
            .await
            .unwrap();
        let account: Uuid = sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES($1) RETURNING id")
            .bind(user)
            .fetch_one(owner)
            .await
            .unwrap();
        sqlx::query("INSERT INTO account_members(account_id,user_id,role) VALUES($1,$2,'owner')")
            .bind(account)
            .bind(user)
            .execute(owner)
            .await
            .unwrap();
        account
    }

    async fn done(self) {
        self.app.close().await;
        support::managed_extraction::drop_database(&self.owner).await;
    }

    async fn begin(&self, role: Role) -> Transaction<'static, Postgres> {
        match (role, self.org) {
            (Role::Owner, _) => self.owner.begin().await.unwrap(),
            (Role::App, true) => begin_tenant_system_scoped_without_account(&self.app).await.unwrap(),
            (Role::App, false) => begin_account_scoped(&self.app, self.account).await.unwrap(),
        }
    }

    async fn refresh(&self, graph: Uuid, role: Role) -> Refresh {
        let mut tx = self.begin(role).await;
        let out = co_occurrence::refresh_tx(&mut tx, graph, Write::Apply).await.unwrap();
        tx.commit().await.unwrap();
        out
    }

    async fn node(&self, graph: Uuid, title: &str) -> Uuid {
        sqlx::query_scalar("INSERT INTO nodes(graph_id,type,title,body) VALUES($1,'entity',$2,'{}') RETURNING node_id")
            .bind(graph)
            .bind(title)
            .fetch_one(&self.owner)
            .await
            .unwrap()
    }

    /// One source item and version holding one v2 statement linked to `links`.
    async fn statement(&self, graph: Uuid, access: &str, text: &str, links: &[Uuid]) -> Uuid {
        let o = &self.owner;
        let id = Uuid::new_v4().to_string();
        let origin = json!({"service_id":"linear","transport":"connector","content_kind":"issue"});
        let item: Uuid = sqlx::query_scalar(
            "INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id,origin,access_state) \
             VALUES($1,'linear_issue','composio:linear',$2,$3,$4) RETURNING source_item_id",
        )
        .bind(graph)
        .bind(&id)
        .bind(&origin)
        .bind(access)
        .fetch_one(o)
        .await
        .unwrap();
        let version: Uuid = sqlx::query_scalar(
            "INSERT INTO source_versions(graph_id,source_item_id,source_id,source_type,provider_namespace,raw_inline,\
                extraction_version,extraction_status,origin,captured_at,occurred_at) \
             VALUES($1,$2,$3,'linear_issue','composio:linear',$4,'source-atoms-v2','done',$5,now(),now()) RETURNING version_id",
        )
        .bind(graph)
        .bind(item)
        .bind(&id)
        .bind(text)
        .bind(&origin)
        .fetch_one(o)
        .await
        .unwrap();
        sqlx::query("UPDATE source_items SET current_version_id=$2 WHERE source_item_id=$1")
            .bind(item)
            .bind(version)
            .execute(o)
            .await
            .unwrap();
        let atom: Uuid = sqlx::query_scalar(
            "INSERT INTO semantic_atoms(graph_id,source_version_id,ordinal,kind,statement,extraction_version,model) \
             VALUES($1,$2,0,'fact',$3,'source-atoms-v2','fake-model') RETURNING semantic_atom_id",
        )
        .bind(graph)
        .bind(version)
        .bind(text)
        .fetch_one(o)
        .await
        .unwrap();
        for node in links {
            sqlx::query("INSERT INTO node_semantic_atoms(graph_id,node_id,semantic_atom_id) VALUES($1,$2,$3)")
                .bind(graph)
                .bind(node)
                .bind(atom)
                .execute(o)
                .await
                .unwrap();
        }
        atom
    }

    async fn pairs(&self, graph: Uuid) -> BTreeSet<(Uuid, Uuid)> {
        let rows: Vec<(Uuid, Uuid)> = sqlx::query_as(
            "SELECT src_node_id,dst_node_id FROM relationship_edge WHERE graph_id=$1 AND predicate='co_occurs_with' AND status='confirmed'",
        )
        .bind(graph)
        .fetch_all(&self.owner)
        .await
        .unwrap();
        rows.into_iter().collect()
    }

    async fn ops(&self) -> i64 {
        sqlx::query_scalar("SELECT count(*) FROM op_log WHERE entity='relationship_edge' AND graph_id=$1")
            .bind(self.graph)
            .fetch_one(&self.owner)
            .await
            .unwrap()
    }
}

fn pair(a: Uuid, b: Uuid) -> (Uuid, Uuid) {
    (a.min(b), a.max(b))
}

/// Two reefs named in one permitted statement get one labelled, provenanced
/// edge; the self reef, restricted sources and single-reef statements get
/// none; a re-run writes nothing; removed evidence removes the edge; a
/// rejected pair stays rejected; and fact-presenting readers never see them.
async fn links_with_provenance_and_converges(p: &Plane, role: Role) {
    let g = p.graph;
    let (a, b, c, d, me) = (
        p.node(g, "Apollo").await,
        p.node(g, "Bea Park").await,
        p.node(g, "Cy Lin").await,
        p.node(g, "Dee Moss").await,
        p.node(g, "Me").await,
    );
    sqlx::query("UPDATE graphs SET self_node_id=$2 WHERE id=$1").bind(g).bind(me).execute(&p.owner).await.unwrap();
    let ab = p.statement(g, "permitted", "Bea Park launched Apollo with me.", &[a, b, me]).await;
    let bc = p.statement(g, "permitted", "Bea Park handed Apollo docs to Cy Lin.", &[b, c]).await;
    p.statement(g, "restricted", "Cy Lin and Dee Moss met.", &[c, d]).await;
    p.statement(g, "permitted", "Dee Moss wrote alone.", &[d]).await;

    let first = p.refresh(g, role).await;
    assert_eq!((first.desired, first.inserted, first.changed()), (2, 2, 2), "{first:?}");
    assert_eq!(p.pairs(g).await, BTreeSet::from([pair(a, b), pair(b, c)]));
    let (origin, status, confidence, provenance): (String, String, f32, Value) = sqlx::query_as(
        "SELECT origin,status,confidence,provenance FROM relationship_edge WHERE graph_id=$1 AND predicate='co_occurs_with' AND src_node_id=$2 AND dst_node_id=$3",
    )
    .bind(g)
    .bind(a.min(b))
    .bind(a.max(b))
    .fetch_one(&p.owner)
    .await
    .unwrap();
    assert_eq!((origin.as_str(), status.as_str()), ("inferred", "confirmed"));
    assert_eq!(confidence, co_occurrence::CONFIDENCE);
    assert_eq!(provenance["derivation"], "co_occurrence");
    assert_eq!(provenance["co_occurrence"]["rule"], co_occurrence::RULE);
    assert_eq!(provenance["semantic_atom_ids"], json!([ab.to_string()]));
    assert_eq!(p.ops().await, 2, "each write appends its op-log entry");

    let again = p.refresh(g, role).await;
    assert_eq!((again.desired, again.unchanged, again.changed()), (2, 2, 0), "{again:?}");
    assert_eq!(p.ops().await, 2, "an unchanged re-run writes nothing");

    // Readers that present edges as facts never see a co-occurrence.
    let store = PostgresStore::new(p.owner.clone());
    let mut tx = p.begin(role).await;
    let around_b = store.list_relationship_edges_for_nodes_scoped(&mut tx, g, &[b]).await.unwrap();
    let confirmed = store.list_relationship_edges_by_status_scoped(&mut tx, g, EdgeStatus::Confirmed).await.unwrap();
    let impact = store.node_structural_impact_scoped(&mut tx, g, &[b]).await.unwrap();
    tx.commit().await.unwrap();
    assert!(around_b.is_empty() && confirmed.is_empty(), "{around_b:?} {confirmed:?}");
    assert_eq!(impact[0].degree, 0);

    // Evidence removed: the edge goes, with a delete op.
    sqlx::query("DELETE FROM node_semantic_atoms WHERE semantic_atom_id=$1 AND node_id=$2").bind(bc).bind(c).execute(&p.owner).await.unwrap();
    let removed = p.refresh(g, role).await;
    assert_eq!((removed.deleted, removed.unchanged), (1, 1), "{removed:?}");
    assert_eq!(p.pairs(g).await, BTreeSet::from([pair(a, b)]));
    assert_eq!(p.ops().await, 3);

    // A person's rejection stands and is never re-created.
    sqlx::query("UPDATE relationship_edge SET status='rejected' WHERE graph_id=$1 AND predicate='co_occurs_with'").bind(g).execute(&p.owner).await.unwrap();
    let kept = p.refresh(g, role).await;
    assert_eq!((kept.rejected_kept, kept.changed()), (1, 0), "{kept:?}");
    let rejected: i64 = sqlx::query_scalar("SELECT count(*) FROM relationship_edge WHERE graph_id=$1 AND predicate='co_occurs_with' AND status='rejected'")
        .bind(g)
        .fetch_one(&p.owner)
        .await
        .unwrap();
    assert_eq!(rejected, 1);
}

/// A reef named alongside many others keeps at most `PER_REEF` edges, its
/// strongest partners first; a statement naming too many reefs is ignored.
async fn hubs_are_capped(p: &Plane, role: Role) {
    let g = p.graph;
    let hub = p.node(g, "CoralSwarm").await;
    let mut partners = Vec::new();
    for i in 0..8 {
        let n = p.node(g, &format!("Partner {i}")).await;
        p.statement(g, "permitted", &format!("CoralSwarm and Partner {i}."), &[hub, n]).await;
        partners.push(n);
    }
    // Partner 7 is named with the hub in a second document: strongest.
    p.statement(g, "permitted", "CoralSwarm and Partner 7 again.", &[hub, partners[7]]).await;
    let mut crowd = Vec::new();
    for i in 0..(co_occurrence::MAX_REEFS_PER_STATEMENT + 1) {
        crowd.push(p.node(g, &format!("Crowd {i}")).await);
    }
    p.statement(g, "permitted", "A list of everyone.", &crowd).await;

    p.refresh(g, role).await;
    let pairs = p.pairs(g).await;
    let hub_partners: BTreeSet<Uuid> = pairs
        .iter()
        .filter_map(|&(s, d)| if s == hub { Some(d) } else if d == hub { Some(s) } else { None })
        .collect();
    assert_eq!(hub_partners.len() as i64, co_occurrence::PER_REEF);
    let mut others: Vec<Uuid> = partners[..7].to_vec();
    others.sort();
    let mut expected: BTreeSet<Uuid> = others.into_iter().take(co_occurrence::PER_REEF as usize - 1).collect();
    expected.insert(partners[7]);
    assert_eq!(hub_partners, expected);
    assert!(pairs.iter().all(|(s, d)| !crowd.contains(s) && !crowd.contains(d)), "an oversized statement links nothing");
}

/// No edge joins reefs of two oceans, and a refresh under another account's
/// scope sees and writes nothing.
async fn stays_inside_one_ocean(p: &Plane) {
    let g = p.graph;
    let (a, b) = (p.node(g, "Apollo").await, p.node(g, "Bea Park").await);
    let other = if p.org {
        Plane::tenant_graph(&p.owner, Uuid::new_v4()).await
    } else {
        let account = Plane::consumer_account(&p.owner).await;
        sqlx::query_scalar("INSERT INTO graphs(account_id,name) VALUES($1,'Other') RETURNING id")
            .bind(account)
            .fetch_one(&p.owner)
            .await
            .unwrap()
    };
    let (x, y) = (p.node(other, "Apollo").await, p.node(other, "Bea Park").await);
    p.statement(g, "permitted", "Bea Park launched Apollo.", &[a, b]).await;
    p.statement(other, "permitted", "Bea Park launched Apollo.", &[x, y]).await;

    p.refresh(g, Role::App).await;
    assert_eq!(p.pairs(g).await, BTreeSet::from([pair(a, b)]));
    assert!(p.pairs(other).await.is_empty(), "refreshing one ocean writes nothing in another");
    if !p.org {
        let foreign = p.refresh(other, Role::App).await;
        assert_eq!(foreign, Refresh::default(), "another account's ocean is invisible to this scope");
        assert!(p.pairs(other).await.is_empty());
    }
    p.refresh(other, Role::Owner).await;
    assert_eq!(p.pairs(other).await, BTreeSet::from([pair(x, y)]));
    let crossing: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM relationship_edge e JOIN nodes s ON s.node_id=e.src_node_id JOIN nodes d ON d.node_id=e.dst_node_id \
          WHERE s.graph_id<>e.graph_id OR d.graph_id<>e.graph_id",
    )
    .fetch_one(&p.owner)
    .await
    .unwrap();
    assert_eq!(crossing, 0);
}

/// The `InferRelationships` job (enqueued after binding) refreshes the
/// graph's co-occurrence edges under the job's scope.
async fn the_closure_job_refreshes(p: &Plane) {
    let g = p.graph;
    let (a, b) = (p.node(g, "Apollo").await, p.node(g, "Bea Park").await);
    p.statement(g, "permitted", "Bea Park launched Apollo.", &[a, b]).await;
    let job = queue::enqueue(&p.app, &JobKind::InferRelationships { graph_id: g }, Some(p.account), None, 0)
        .await
        .unwrap()
        .unwrap();
    let mut jobs = queue::dequeue(&p.app, &["InferRelationships"], 1).await.unwrap();
    assert_eq!(jobs.len(), 1);
    let tenant = if p.org { TenantId::Org("org_co_occurrence".into()) } else { TenantId::Consumer };
    let store = PostgresStore::new(p.app.clone());
    inference_closure::process_job(&tenant, &p.app, &store, jobs.remove(0)).await;
    let (status, error): (String, Option<String>) = sqlx::query_as("SELECT status::text,error_message FROM jobs WHERE id=$1")
        .bind(job)
        .fetch_one(&p.owner)
        .await
        .unwrap();
    assert_eq!(status, "done", "{error:?}");
    assert_eq!(p.pairs(g).await, BTreeSet::from([pair(a, b)]));
}

/// The operator backfill defaults to a dry run that writes nothing, then
/// applies, then converges.
async fn backfill_dry_run_then_apply(p: &Plane) {
    let g = p.graph;
    let (a, b) = (p.node(g, "Apollo").await, p.node(g, "Bea Park").await);
    p.statement(g, "permitted", "Bea Park launched Apollo.", &[a, b]).await;
    let dry = co_occurrence::backfill(&p.owner, None, Write::DryRun).await.unwrap();
    assert_eq!((dry.graphs, dry.total.inserted, dry.failed.len()), (1, 1, 0));
    assert!(p.pairs(g).await.is_empty(), "a dry run writes nothing");
    let applied = co_occurrence::backfill(&p.owner, Some(g), Write::Apply).await.unwrap();
    assert_eq!(applied.total.inserted, 1);
    assert_eq!(p.pairs(g).await, BTreeSet::from([pair(a, b)]));
    let again = co_occurrence::backfill(&p.owner, None, Write::Apply).await.unwrap();
    assert_eq!((again.total.changed(), again.total.unchanged), (0, 1));
}

macro_rules! planes {
    ($($name:ident: $plane:ident => $run:ident($($arg:expr),*);)*) => {$(
        #[tokio::test]
        async fn $name() {
            let Some(opts) = base() else { return };
            let p = Plane::$plane(&opts).await;
            $run(&p $(, $arg)*).await;
            p.done().await;
        }
    )*};
}

planes! {
    tenant_links_with_provenance_and_converges_as_app_role: tenant => links_with_provenance_and_converges(Role::App);
    tenant_links_with_provenance_and_converges_as_table_owner: tenant => links_with_provenance_and_converges(Role::Owner);
    consumer_links_with_provenance_and_converges_as_app_role: consumer => links_with_provenance_and_converges(Role::App);
    tenant_hubs_are_capped: tenant => hubs_are_capped(Role::App);
    consumer_hubs_are_capped: consumer => hubs_are_capped(Role::App);
    tenant_stays_inside_one_ocean: tenant => stays_inside_one_ocean();
    consumer_stays_inside_one_ocean: consumer => stays_inside_one_ocean();
    tenant_closure_job_refreshes_co_occurrence: tenant => the_closure_job_refreshes();
    consumer_closure_job_refreshes_co_occurrence: consumer => the_closure_job_refreshes();
    tenant_backfill_dry_run_then_apply: tenant => backfill_dry_run_then_apply();
    consumer_backfill_dry_run_then_apply: consumer => backfill_dry_run_then_apply();
}
