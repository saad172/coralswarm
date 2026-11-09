//! Append-only Library invalidation and RLS contracts on both real planes.
mod support;
use coralswarm_backend::{store::PostgresStore, tenancy::{extractor::Tenant, runtime::{TenancyDecision,TenantContext}}, workers::library_evidence, UserContext};
use serde_json::Value;
use sqlx::{postgres::{PgConnectOptions,PgPoolOptions}, Connection,Executor,PgConnection,PgPool};
use std::{str::FromStr,sync::Arc,time::Duration};
use support::rls_probe::RlsProbe;
use uuid::Uuid;

enum Cleanup {
    Consumer(RlsProbe),
    Tenant(PgConnection, coralswarm_backend::test_db::TestDatabase),
}
struct Fixture {
    owner: PgPool,
    app: PgPool,
    tenant: Tenant,
    user: UserContext,
    graph: Uuid,
    foreign_graph: Uuid,
    cleanup: Cleanup,
}
impl Fixture {
    async fn new(tenant_plane: bool, suffix: &str) -> Self {
        let base = std::env::var("ORGT_TEST_BASE_URL")
            .expect("ORGT_TEST_BASE_URL required for actual Library proof");
        let opts = PgConnectOptions::from_str(&base).unwrap();
        assert!(matches!(opts.get_host(), "localhost" | "127.0.0.1" | "::1"));
        if !tenant_plane {
            let f = RlsProbe::setup(&format!("library_consumer_{suffix}"))
                .await
                .unwrap();
            for account in [&f.a, &f.b] {
                sqlx::query("INSERT INTO account_members(account_id,user_id,role) VALUES($1,$2,'owner') ON CONFLICT DO NOTHING")
                    .bind(account.account_id).bind(account.user_id).execute(&f.owner).await.unwrap();
            }
            // Capture is switch-gated (0201/0380); these proofs run as an
            // On plane, as the operator command flips it.
            library_evidence::flip_listing_switch(&f.owner, true).await.unwrap();
            return Self {
                owner: f.owner.clone(),
                app: f.app_pool.clone(),
                tenant: Tenant {
                    store: Some(Arc::new(PostgresStore::new(f.app_pool.clone()))),
                    pool: Some(f.app_pool.clone()),
                    decision: None,
                    class: coralswarm_backend::route_class::RouteClass::Other,
                },
                user: UserContext {
                    user_id: f.a.user_id,
                    account_id: f.a.account_id,
                    role: "owner".into(),
                    device_id: "library-proof".into(),
                },
                graph: f.a.graph_id,
                foreign_graph: f.b.graph_id,
                cleanup: Cleanup::Consumer(f),
            };
        }
        let name = coralswarm_backend::test_db::TestDatabase::adopt(&opts, format!("library_tenant_{suffix}"));
        let mut admin = PgConnection::connect_with(&opts).await.unwrap();
        admin
            .execute(format!("DROP DATABASE IF EXISTS {name} WITH (FORCE)").as_str())
            .await
            .unwrap();
        admin
            .execute(format!("CREATE DATABASE {name}").as_str())
            .await
            .unwrap();
        let owner = PgPoolOptions::new()
            .max_connections(4)
            .connect_with(opts.clone().database(&name))
            .await
            .unwrap();
        sqlx::migrate!("./migrations_tenant")
            .run(&owner)
            .await
            .unwrap();
        library_evidence::flip_listing_switch(&owner, true).await.unwrap();
        // This constant is a disposable local fixture login, never a real secret.
        sqlx::query("ALTER ROLE coralswarm_tenant_app PASSWORD 'library-local-fixture'")
            .execute(&owner)
            .await
            .unwrap();
        let app = PgPoolOptions::new()
            .max_connections(4)
            .connect_with(
                opts.database(&name)
                    .username("coralswarm_tenant_app")
                    .password("library-local-fixture"),
            )
            .await
            .unwrap();
        let actor = Uuid::new_v4();
        let other = Uuid::new_v4();
        let account = Uuid::new_v4();
        let graph = Uuid::new_v4();
        let foreign_graph = Uuid::new_v4();
        // Blob locators require a valid canonical org segment (`org_…`).
        let fixture_org = if suffix == "capture_ingest" || suffix == "blob_fence" {
            "org_library_proof"
        } else {
            "org-library-proof"
        };
        sqlx::query("INSERT INTO tenant_meta(id,org_id) VALUES(true,$1)")
            .bind(fixture_org)
            .execute(&owner)
            .await
            .unwrap();
        for (g, u) in [(graph, actor), (foreign_graph, other)] {
            sqlx::query("INSERT INTO graphs(id,account_id,name,kind,owner_user_id) VALUES($1,$2,'Library fixture','member',$3)").bind(g).bind(account).bind(u).execute(&owner).await.unwrap();
        }
        let tenant = Tenant {
            store: Some(Arc::new(PostgresStore::new(app.clone()))),
            pool: Some(app.clone()),
            decision: Some(TenancyDecision::Org(TenantContext {
                org_id: fixture_org.into(),
                account_id: account,
                user_id: actor,
                pool: app.clone(),
                page_pool: None,
            })),
            class: coralswarm_backend::route_class::RouteClass::Other,
        };
        Self {
            owner,
            app,
            tenant,
            user: UserContext {
                user_id: actor,
                account_id: account,
                role: "owner".into(),
                device_id: "library-proof".into(),
            },
            graph,
            foreign_graph,
            cleanup: Cleanup::Tenant(admin, name),
        }
    }
    async fn close(self) {
        self.app.close().await;
        self.owner.close().await;
        match self.cleanup {
            Cleanup::Consumer(f) => f.teardown().await,
            Cleanup::Tenant(mut admin, name) => {
                admin
                    .execute(format!("DROP DATABASE {name} WITH (FORCE)").as_str())
                    .await
                    .unwrap();
            }
        }
    }
}
async fn clear(f: &Fixture) {
    sqlx::query("DELETE FROM library_evidence_changes").execute(&f.owner).await.unwrap();
}
async fn nodes(f: &Fixture, key: &str, id: Uuid) -> Vec<Uuid> {
    sqlx::query_scalar(&format!("SELECT DISTINCT r.node_id FROM library_evidence_changes c CROSS JOIN LATERAL library_evidence_change_nodes(c.graph_id,c.id,NULL,4096) r WHERE c.graph_id=$1 AND c.{key}=$2 ORDER BY r.node_id"))
        .bind(f.graph).bind(id).fetch_all(&f.owner).await.unwrap()
}
async fn cached(f: &Fixture, n: Uuid, item: Uuid) {
    sqlx::query("INSERT INTO library_node_evidence(graph_id,node_id,source_count,unknown_independence_record_count,atom_count_retained,atom_count_served,unmapped_legacy_count,admitted_unfiltered,has_source_link,title_visible,project_mention,work_eligible,stats_admitted,computed_version) VALUES($1,$2,0,0,0,0,0,false,false,false,false,false,false,1)")
        .bind(f.graph).bind(n).execute(&f.owner).await.unwrap();
    sqlx::query("INSERT INTO library_node_item_evidence(graph_id,node_id,source_item_id,semantic_link,legacy_link,legacy_independent_version,legacy_synthetic,atom_count_retained,atom_count_served,project_mention) VALUES($1,$2,$3,true,false,false,false,0,0,false)")
        .bind(f.graph).bind(n).bind(item).execute(&f.owner).await.unwrap();
}
async fn evidence_fixture(f: &Fixture) -> (Uuid,Uuid,Uuid,Uuid,Uuid,Uuid) {
    let (n,m,a,i,v,s)=(Uuid::new_v4(),Uuid::new_v4(),Uuid::new_v4(),Uuid::new_v4(),Uuid::new_v4(),Uuid::new_v4());
    f.owner.execute(format!(r#"
INSERT INTO nodes(node_id,graph_id,type,title,body) VALUES('{n}','{g}','entity','Correction label','{{}}'),('{m}','{g}','entity','Semantic node','{{}}');
INSERT INTO atoms(atom_id,graph_id,source_type,source_id,content_blob_key,content_inline,timestamp) VALUES('{a}','{g}','note','evidence-source','fixture-key','support without label',now());
INSERT INTO node_atoms(node_id,atom_id) VALUES('{n}','{a}');
INSERT INTO source_items(source_item_id,graph_id,source_type,provider_namespace,source_id,origin,access_state) VALUES('{i}','{g}','note','fixture','evidence-source','{{"transport":"native","service_id":"fixture","content_kind":"note"}}','restricted');
INSERT INTO source_versions(version_id,source_item_id,graph_id,source_id,source_type,provider_namespace,raw_inline,origin) VALUES('{v}','{i}','{g}','evidence-source','note','fixture','support','{{"transport":"native","service_id":"fixture","content_kind":"note"}}');
UPDATE source_items SET current_version_id='{v}' WHERE source_item_id='{i}';
INSERT INTO source_version_chunks(graph_id,source_version_id,atom_id) VALUES('{g}','{v}','{a}');
INSERT INTO semantic_atoms(semantic_atom_id,graph_id,source_version_id,ordinal,kind,statement,extraction_version,model) VALUES('{s}','{g}','{v}',0,'fact','support','source-atoms-v1','fixture');
INSERT INTO node_semantic_atoms(node_id,semantic_atom_id,graph_id) VALUES('{m}','{s}','{g}');
"#,g=f.graph).as_str()).await.unwrap();
    (n,m,a,i,v,s)
}
#[tokio::test]
async fn library_evidence_changes_both_planes() {
    if std::env::var("ORGT_TEST_BASE_URL").is_err() { eprintln!("skip: ORGT_TEST_BASE_URL required"); return; }
    for tenant in [false,true] {
        let f=Fixture::new(tenant,"codex_rm2_changes").await;
        let (n,m,a,i,v,s)=evidence_fixture(&f).await;
        // Nine families capture all three events at statement level, except
        // atoms and nodes: UPDATE is row level on evidence inputs only, and
        // INSERT is not captured (a new atom or node is evidence only once a
        // link to it logs the node).
        let triggers:i64=sqlx::query_scalar("SELECT count(*) FROM pg_trigger WHERE tgname LIKE 'library_evidence_%' AND NOT tgisinternal AND (tgtype & 1)=0")
            .fetch_one(&f.owner).await.unwrap();
        assert_eq!(triggers,23);
        let row_level:Vec<String>=sqlx::query_scalar("SELECT tgname::text FROM pg_trigger WHERE tgname LIKE 'library_evidence_%' AND NOT tgisinternal AND (tgtype & 1)=1 ORDER BY 1")
            .fetch_all(&f.owner).await.unwrap();
        assert_eq!(row_level,["library_evidence_atoms_update","library_evidence_nodes_update"]);
        // Simulate a completed denied-item backfill: no permitted cached links,
        // no remaining log. An access grant alone must expose BOTH live grains.
        clear(&f).await;
        sqlx::query("UPDATE source_items SET access_state='permitted' WHERE source_item_id=$1").bind(i).execute(&f.owner).await.unwrap();
        let mut expected=vec![n,m];expected.sort();
        assert_eq!(nodes(&f,"atom_id",a).await,vec![n]);
        assert_eq!(nodes(&f,"semantic_atom_id",s).await,vec![m]);
        // No cache rows existed; the grant's object keys cover both grains.
        cached(&f,n,i).await;cached(&f,m,i).await;
        // Metadata / representation and source identity affect the definition.
        clear(&f).await;
        sqlx::query("UPDATE source_versions SET representation='provider_summary' WHERE version_id=$1").bind(v).execute(&f.owner).await.unwrap();
        assert_eq!(nodes(&f,"source_item_id",i).await,expected);
        clear(&f).await;
        sqlx::query("UPDATE source_items SET source_id='different-identity' WHERE source_item_id=$1").bind(i).execute(&f.owner).await.unwrap();
        assert_eq!(nodes(&f,"source_item_id",i).await,expected);
        // Individual atom/outcome mutation and removed mapping preserve keys.
        clear(&f).await;
        sqlx::query("UPDATE atoms SET participants=ARRAY['Changed'] WHERE atom_id=$1").bind(a).execute(&f.owner).await.unwrap();
        assert_eq!(nodes(&f,"atom_id",a).await,vec![n]);
        clear(&f).await;
        sqlx::query("UPDATE semantic_atoms SET statement='changed' WHERE semantic_atom_id=$1").bind(s).execute(&f.owner).await.unwrap();
        assert_eq!(nodes(&f,"semantic_atom_id",s).await,vec![m]);
        clear(&f).await;
        sqlx::query("DELETE FROM source_version_chunks WHERE source_version_id=$1").bind(v).execute(&f.owner).await.unwrap();
        assert_eq!(nodes(&f,"atom_id",a).await,vec![n]);
        assert_eq!(nodes(&f,"source_item_id",i).await,expected);
        // Mapping UPDATE must capture old and new atom keys, not only INSERT.
        let b:Uuid=sqlx::query_scalar("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,timestamp) VALUES($1,'note','second','fixture-key','body',now()) RETURNING atom_id").bind(f.graph).fetch_one(&f.owner).await.unwrap();
        sqlx::query("INSERT INTO source_version_chunks(graph_id,source_version_id,atom_id) VALUES($1,$2,$3)").bind(f.graph).bind(v).bind(a).execute(&f.owner).await.unwrap();
        clear(&f).await;
        sqlx::query("UPDATE source_version_chunks SET atom_id=$1 WHERE source_version_id=$2").bind(b).bind(v).execute(&f.owner).await.unwrap();
        let keys:Vec<Uuid>=sqlx::query_scalar("SELECT DISTINCT atom_id FROM library_evidence_changes WHERE atom_id IS NOT NULL ORDER BY atom_id").fetch_all(&f.owner).await.unwrap();
        let mut atoms=vec![a,b];atoms.sort();assert_eq!(keys,atoms);
        // Removed semantic and legacy links record the old node directly.
        clear(&f).await;
        sqlx::query("DELETE FROM node_semantic_atoms WHERE semantic_atom_id=$1").bind(s).execute(&f.owner).await.unwrap();
        assert_eq!(nodes(&f,"node_id",m).await,vec![m]);
        clear(&f).await;
        sqlx::query("DELETE FROM node_atoms WHERE atom_id=$1").bind(a).execute(&f.owner).await.unwrap();
        assert_eq!(nodes(&f,"node_id",n).await,vec![n]);
        // Link reparenting and node updates retain both before/after keys.
        sqlx::query("INSERT INTO node_semantic_atoms(node_id,semantic_atom_id,graph_id) VALUES($1,$2,$3)").bind(m).bind(s).bind(f.graph).execute(&f.owner).await.unwrap();clear(&f).await;
        sqlx::query("UPDATE node_semantic_atoms SET node_id=$1 WHERE semantic_atom_id=$2").bind(n).bind(s).execute(&f.owner).await.unwrap();
        assert_eq!(nodes(&f,"node_id",n).await,vec![n]);assert_eq!(nodes(&f,"node_id",m).await,vec![m]);clear(&f).await;
        // Of body, the definition reads only the synthetic_hub flag.
        sqlx::query("UPDATE nodes SET body='{\"changed\":true}' WHERE node_id=$1").bind(n).execute(&f.owner).await.unwrap();assert_eq!(nodes(&f,"node_id",n).await,Vec::<Uuid>::new());
        sqlx::query("UPDATE nodes SET body='{\"synthetic_hub\":\"true\"}' WHERE node_id=$1").bind(n).execute(&f.owner).await.unwrap();assert_eq!(nodes(&f,"node_id",n).await,vec![n]);
        clear(&f).await;
        sqlx::query("DELETE FROM semantic_atoms WHERE semantic_atom_id=$1").bind(s).execute(&f.owner).await.unwrap();
        assert_eq!(nodes(&f,"node_id",n).await,vec![n]);assert_eq!(nodes(&f,"source_item_id",i).await,expected);
        // Deletion-safe stored item grain still resolves after source cascades.
        sqlx::query("DELETE FROM library_node_item_evidence WHERE node_id=$1").bind(m).execute(&f.owner).await.unwrap();
        clear(&f).await;
        sqlx::query("DELETE FROM source_items WHERE source_item_id=$1").bind(i).execute(&f.owner).await.unwrap();
        assert_eq!(nodes(&f,"source_item_id",i).await,vec![n]);
        // Unbound identity denial/restoration handles numeric chunk suffixes,
        // including the source namespace boundary and DELETE's old identity.
        // Source binding is deliberately sticky; use a genuinely unbound
        // atom, rather than trying to undo the earlier mapped atom's marker.
        let a:Uuid=sqlx::query_scalar("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,timestamp) VALUES($1,'note','unbound:chunk:12','fixture','support without label',now()) RETURNING atom_id").bind(f.graph).fetch_one(&f.owner).await.unwrap();
        sqlx::query("INSERT INTO node_atoms(node_id,atom_id) VALUES($1,$2)").bind(n).bind(a).execute(&f.owner).await.unwrap();
        let item=Uuid::new_v4();clear(&f).await;
        sqlx::query("INSERT INTO source_items(source_item_id,graph_id,source_type,provider_namespace,source_id,origin,access_state) VALUES($1,$2,'note','fixture','unbound','{}','restricted')").bind(item).bind(f.graph).execute(&f.owner).await.unwrap();
        assert_eq!(nodes(&f,"atom_id",a).await,vec![n]);
        let renamed:Uuid=sqlx::query_scalar("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,timestamp) VALUES($1,'note','renamed:chunk:9','fixture','body',now()) RETURNING atom_id").bind(f.graph).fetch_one(&f.owner).await.unwrap();
        sqlx::query("INSERT INTO node_atoms(node_id,atom_id) VALUES($1,$2)").bind(m).bind(renamed).execute(&f.owner).await.unwrap();clear(&f).await;
        sqlx::query("UPDATE source_items SET source_id='renamed' WHERE source_item_id=$1").bind(item).execute(&f.owner).await.unwrap();
        assert_eq!(nodes(&f,"atom_id",a).await,vec![n]);assert_eq!(nodes(&f,"atom_id",renamed).await,vec![m]);clear(&f).await;
        sqlx::query("UPDATE source_items SET source_id='unbound' WHERE source_item_id=$1").bind(item).execute(&f.owner).await.unwrap();
        assert_eq!(nodes(&f,"atom_id",a).await,vec![n]);assert_eq!(nodes(&f,"atom_id",renamed).await,vec![m]);clear(&f).await;
        sqlx::query("UPDATE source_items SET access_state='permitted' WHERE source_item_id=$1").bind(item).execute(&f.owner).await.unwrap();
        assert_eq!(nodes(&f,"atom_id",a).await,vec![n]);
        sqlx::query("UPDATE source_items SET access_state='restricted' WHERE source_item_id=$1").bind(item).execute(&f.owner).await.unwrap();clear(&f).await;
        sqlx::query("DELETE FROM source_items WHERE source_item_id=$1").bind(item).execute(&f.owner).await.unwrap();
        assert_eq!(nodes(&f,"atom_id",a).await,vec![n]);
        // Revocation is deliberately sticky. Correction support is a fresh
        // permitted legacy atom, with no content/entity/participant label.
        let a:Uuid=sqlx::query_scalar("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,timestamp) VALUES($1,'note','correction-support','fixture','support without label',now()) RETURNING atom_id").bind(f.graph).fetch_one(&f.owner).await.unwrap();
        sqlx::query("INSERT INTO node_atoms(node_id,atom_id) VALUES($1,$2)").bind(n).bind(a).execute(&f.owner).await.unwrap();
        let before:Option<String>=sqlx::query_scalar("SELECT library_node_title($1,$2)").bind(f.graph).bind(n).fetch_one(&f.owner).await.unwrap();assert!(before.is_none());
        let mut tx=f.owner.begin().await.unwrap();
        sqlx::query("SELECT set_config('app.account_id',$1,true),set_config('app.user_id',$2,true)").bind(f.user.account_id.to_string()).bind(f.user.user_id.to_string()).execute(&mut *tx).await.unwrap();
        clear(&f).await;
        let correction:Uuid=sqlx::query_scalar("INSERT INTO reef_identity_corrections(graph_id,node_id,label,recorded_by,support_atom_id,support_kind,content_revision,input_sha256) SELECT $1,$2,'Correction label',$3,atom_id,'legacy',content_revision,reef_identity_atom_fingerprint(a) FROM atoms a WHERE atom_id=$4 RETURNING correction_id")
            .bind(f.graph).bind(n).bind(f.user.user_id).bind(a).fetch_one(&mut *tx).await.unwrap();tx.commit().await.unwrap();
        assert_eq!(nodes(&f,"node_id",n).await,vec![n]);
        let after:Option<String>=sqlx::query_scalar("SELECT library_node_title($1,$2)").bind(f.graph).bind(n).fetch_one(&f.owner).await.unwrap();assert_eq!(after.as_deref(),Some("Correction label"));
        let mut actual_reader=f.tenant.begin_member_scoped(f.user.account_id).await.unwrap();
        let reader_title:Option<String>=sqlx::query_scalar("SELECT library_node_title($1,$2)").bind(f.graph).bind(n).fetch_one(&mut *actual_reader).await.unwrap();
        assert_eq!(reader_title.is_some(),tenant,"actual consumer path has no app.user_id; tenant path has actor");
        let actual_context:Value=sqlx::query_scalar("SELECT library_evidence_context($1)").bind(f.graph).fetch_one(&mut *actual_reader).await.unwrap();
        sqlx::query("SELECT set_config('app.user_id',$1,true)").bind(f.user.user_id.to_string()).execute(&mut *actual_reader).await.unwrap();
        let acting_context:Value=sqlx::query_scalar("SELECT library_evidence_context($1)").bind(f.graph).fetch_one(&mut *actual_reader).await.unwrap();
        if !tenant { assert_ne!(actual_context,acting_context,"effective correction RLS changes the stamp"); }
        actual_reader.rollback().await.unwrap();
        clear(&f).await;
        sqlx::query("UPDATE reef_identity_corrections SET state='superseded' WHERE correction_id=$1").bind(correction).execute(&f.owner).await.unwrap();assert_eq!(nodes(&f,"node_id",n).await,vec![n]);
        clear(&f).await;
        sqlx::query("DELETE FROM reef_identity_corrections WHERE correction_id=$1").bind(correction).execute(&f.owner).await.unwrap();assert_eq!(nodes(&f,"node_id",n).await,vec![n]);
        // Two writers append the same hub key without waiting for each other.
        // Semantic links avoid the pre-existing node_atoms coral-count row
        // update; this proves the new capture path adds no shared-row lock.
        clear(&f).await;
        let c:Uuid=sqlx::query_scalar("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,timestamp) VALUES($1,'note','third','fixture-key','body',now()) RETURNING atom_id").bind(f.graph).fetch_one(&f.owner).await.unwrap();
        let item2:Uuid=sqlx::query_scalar("INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id,origin) VALUES($1,'note','fixture','concurrent','{}') RETURNING source_item_id").bind(f.graph).fetch_one(&f.owner).await.unwrap();
        let version2:Uuid=sqlx::query_scalar("INSERT INTO source_versions(source_item_id,graph_id,source_id,source_type,provider_namespace,raw_inline,origin) VALUES($1,$2,'concurrent','note','fixture','body','{}') RETURNING version_id").bind(item2).bind(f.graph).fetch_one(&f.owner).await.unwrap();
        let outcomes:Vec<Uuid>=sqlx::query_scalar("INSERT INTO semantic_atoms(graph_id,source_version_id,ordinal,kind,statement,extraction_version,model) SELECT $1,$2,s,'fact','support','source-atoms-v1','fixture' FROM generate_series(0,1)s RETURNING semantic_atom_id").bind(f.graph).bind(version2).fetch_all(&f.owner).await.unwrap();
        clear(&f).await;
        let mut first=f.owner.begin().await.unwrap();
        sqlx::query("INSERT INTO node_semantic_atoms(node_id,semantic_atom_id,graph_id) VALUES($1,$2,$3)").bind(n).bind(outcomes[0]).bind(f.graph).execute(&mut *first).await.unwrap();
        let mut second=f.owner.begin().await.unwrap();sqlx::query("SELECT set_config('lock_timeout','300ms',true)").execute(&mut *second).await.unwrap();
        tokio::time::timeout(Duration::from_secs(2),async {
            sqlx::query("INSERT INTO node_semantic_atoms(node_id,semantic_atom_id,graph_id) VALUES($1,$2,$3)").bind(n).bind(outcomes[1]).bind(f.graph).execute(&mut *second).await.unwrap();second.commit().await.unwrap();
        }).await.expect("hub writer must commit while first writer is still open");
        first.commit().await.unwrap();
        let count:i64=sqlx::query_scalar("SELECT count(*) FROM library_evidence_changes WHERE node_id=$1").bind(n).fetch_one(&f.owner).await.unwrap();assert_eq!(count,2);
        sqlx::query("INSERT INTO node_atoms(node_id,atom_id) VALUES($1,$2)").bind(n).bind(c).execute(&f.owner).await.unwrap();
        // A resumable log cursor never narrows the overlay's original key.
        for value in 1..=3u128 {
            let node=Uuid::from_u128(value);
            sqlx::query("INSERT INTO nodes(node_id,graph_id,type,title,body) VALUES($1,$2,'entity',$3,'{}')").bind(node).bind(f.graph).bind(format!("cursor-{value}")).execute(&f.owner).await.unwrap();
            sqlx::query("INSERT INTO node_atoms(node_id,atom_id) VALUES($1,$2)").bind(node).bind(c).execute(&f.owner).await.unwrap();
        }
        let log:i64=sqlx::query_scalar("INSERT INTO library_evidence_changes(graph_id,atom_id) VALUES($1,$2) RETURNING id").bind(f.graph).bind(c).fetch_one(&f.owner).await.unwrap();
        let page:Vec<Uuid>=sqlx::query_scalar("SELECT node_id FROM library_evidence_change_nodes($1,$2,NULL,2)").bind(f.graph).bind(log).fetch_all(&f.owner).await.unwrap();assert_eq!(page,vec![Uuid::from_u128(1),Uuid::from_u128(2)]);
        sqlx::query("UPDATE library_evidence_changes SET resolved_after_node_id=$1 WHERE id=$2").bind(page[1]).bind(log).execute(&f.owner).await.unwrap();
        let tail:Vec<Uuid>=sqlx::query_scalar("SELECT node_id FROM library_evidence_change_nodes($1,$2,$3,2)").bind(f.graph).bind(log).bind(page[1]).fetch_all(&f.owner).await.unwrap();assert_eq!(tail,vec![Uuid::from_u128(3),n]);
        let overlay:Vec<Uuid>=sqlx::query_scalar("SELECT node_id FROM library_evidence_change_nodes($1,$2,NULL,5)").bind(f.graph).bind(log).fetch_all(&f.owner).await.unwrap();assert_eq!(overlay.len(),4);
        // State cannot become ready without an explicit computation context.
        assert!(sqlx::query("INSERT INTO library_evidence_state(graph_id,ready_version,backfilled_at,computed_account_id) VALUES($1,1,now(),$2)").bind(f.graph).bind(f.user.account_id).execute(&f.owner).await.is_err());
        sqlx::query("INSERT INTO library_evidence_state(graph_id) VALUES($1)").bind(f.graph).execute(&f.owner).await.unwrap();
        let windowed=if tenant {
            let graph=Uuid::new_v4();let team=Uuid::new_v4();
            sqlx::query("INSERT INTO graphs(id,account_id,name,kind) VALUES($1,$2,'windowed','team')").bind(graph).bind(f.user.account_id).execute(&f.owner).await.unwrap();
            sqlx::query("INSERT INTO teams(id,account_id,graph_id,name,created_by) VALUES($1,$2,$3,'windowed',$4)").bind(team).bind(f.user.account_id).bind(graph).bind(f.user.user_id).execute(&f.owner).await.unwrap();
            sqlx::query("INSERT INTO team_members(team_id,user_id,role) VALUES($1,$2,'member')").bind(team).bind(f.user.user_id).execute(&f.owner).await.unwrap();
            sqlx::query("INSERT INTO team_seat_grants(account_id,team_id,user_id,source,history_from) VALUES($1,$2,$3,'fixture','2026-01-01')").bind(f.user.account_id).bind(team).bind(f.user.user_id).execute(&f.owner).await.unwrap();Some(graph)
        }else{None};
        let mut app=f.tenant.begin_member_scoped(f.user.account_id).await.unwrap();
        if let Some(graph)=windowed {
            let visible:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM graphs WHERE id=$1)").bind(graph).fetch_one(&mut *app).await.unwrap();assert!(visible);
            let shareable:bool=sqlx::query_scalar("SELECT library_evidence_shareable($1)").bind(graph).fetch_one(&mut *app).await.unwrap();assert!(!shareable,"visible graph with history floor must use live evidence");
        }
        let role:String=sqlx::query_scalar("SELECT current_user").fetch_one(&mut *app).await.unwrap();assert!(role=="coralswarm_app"||role=="coralswarm_tenant_app");
        let context:Value=sqlx::query_scalar("SELECT library_evidence_context($1)").bind(f.graph).fetch_one(&mut *app).await.unwrap();assert_eq!(context["role"],role);
        let shared:bool=sqlx::query_scalar("SELECT library_evidence_shareable($1)").bind(f.graph).fetch_one(&mut *app).await.unwrap();assert!(shared);
        let foreign:bool=sqlx::query_scalar("SELECT library_evidence_shareable($1)").bind(f.foreign_graph).fetch_one(&mut *app).await.unwrap();assert!(!foreign);
        for table in ["library_node_evidence","library_node_item_evidence","library_evidence_state","library_evidence_changes"] {
            let own:i64=sqlx::query_scalar(&format!("SELECT count(*) FROM {table} WHERE graph_id=$1")).bind(f.graph).fetch_one(&mut *app).await.unwrap();assert!(own>0,"non-vacuous own rows {table}");
            let foreign:i64=sqlx::query_scalar(&format!("SELECT count(*) FROM {table} WHERE graph_id=$1")).bind(f.foreign_graph).fetch_one(&mut *app).await.unwrap();assert_eq!(foreign,0);
        }
        app.rollback().await.unwrap();
        let mut other=f.app.begin().await.unwrap();
        sqlx::query("SELECT set_config('app.account_id',$1,true),set_config('app.user_id',$2,true)").bind(Uuid::new_v4().to_string()).bind(Uuid::new_v4().to_string()).execute(&mut *other).await.unwrap();
        for table in ["library_node_evidence","library_node_item_evidence","library_evidence_state","library_evidence_changes"] {
            let visible:i64=sqlx::query_scalar(&format!("SELECT count(*) FROM {table} WHERE graph_id=$1")).bind(f.graph).fetch_one(&mut *other).await.unwrap();assert_eq!(visible,0);
            let updated=sqlx::query(&format!("UPDATE {table} SET graph_id=graph_id WHERE graph_id=$1")).bind(f.graph).execute(&mut *other).await.unwrap();assert_eq!(updated.rows_affected(),0);
            let deleted=sqlx::query(&format!("DELETE FROM {table} WHERE graph_id=$1")).bind(f.graph).execute(&mut *other).await.unwrap();assert_eq!(deleted.rows_affected(),0);
            let payload:Value=sqlx::query_scalar(&format!("SELECT to_jsonb(t) FROM {table} t WHERE graph_id=$1 LIMIT 1")).bind(f.graph).fetch_one(&f.owner).await.unwrap();
            sqlx::query("SAVEPOINT denied_insert").execute(&mut *other).await.unwrap();
            let error=sqlx::query(&format!("INSERT INTO {table} OVERRIDING SYSTEM VALUE SELECT (jsonb_populate_record(NULL::{table},$1::jsonb)).*")).bind(payload).execute(&mut *other).await.unwrap_err();
            assert_eq!(error.as_database_error().and_then(|e|e.code()).as_deref(),Some("42501"),"WITH CHECK denies {table}");
            sqlx::query("ROLLBACK TO SAVEPOINT denied_insert").execute(&mut *other).await.unwrap();
        }
        other.rollback().await.unwrap();
        clear(&f).await;
        sqlx::query("DELETE FROM atoms WHERE atom_id=$1").bind(renamed).execute(&f.owner).await.unwrap();
        assert_eq!(nodes(&f,"node_id",m).await,vec![m]);
        clear(&f).await;
        sqlx::query("DELETE FROM nodes WHERE node_id=$1").bind(m).execute(&f.owner).await.unwrap();
        assert_eq!(nodes(&f,"node_id",m).await,vec![m]);
        let stale:i64=sqlx::query_scalar("SELECT count(*) FROM library_node_evidence WHERE node_id=$1").bind(m).fetch_one(&f.owner).await.unwrap();assert_eq!(stale,0,"node deletion synchronously removes stored row");
        // A mismatching role/effective correction-policy context never aliases.
        let owner:Value=sqlx::query_scalar("SELECT library_evidence_context($1)").bind(f.graph).fetch_one(&f.owner).await.unwrap();assert_ne!(owner,context);
        f.close().await;
    }
}

/// Every (table, column) the stored evidence definition reads, as PostgreSQL
/// records it: column-level `pg_depend` rows of a view over the node-keyed
/// definition, followed through each SQL function it calls (re-created as a
/// temporary `BEGIN ATOMIC` clone so its body's column references are
/// recorded too) and each view it reads. Nothing here consults the capture
/// triggers. Whole-table rows (`refobjsubid = 0`) name every relation a query
/// mentions, not a whole-row read, so they are skipped: a row passed whole to
/// a function (`reef_identity_atom_fingerprint(a)`) is attributed through that
/// function's field selections instead.
async fn evidence_inputs(conn: &mut PgConnection) -> std::collections::BTreeSet<(String, String)> {
    use coralswarm_backend::library_read::evidence::node_evidence_ctes;
    let ctes = node_evidence_ctes("SELECT n.graph_id,n.node_id FROM nodes n");
    conn.execute(format!("CREATE TEMP VIEW evidence_inputs_nodes AS {ctes} SELECT * FROM node_evidence").as_str()).await.unwrap();
    conn.execute(format!("CREATE TEMP VIEW evidence_inputs_items AS {ctes} SELECT * FROM node_item_evidence").as_str()).await.unwrap();
    // (classid, objid) of objects whose dependencies are still to be read.
    let mut frontier: Vec<(String, u32)> = sqlx::query_as(
        "SELECT 'pg_rewrite',r.oid::int8 FROM pg_rewrite r WHERE r.ev_class IN ('evidence_inputs_nodes'::regclass,'evidence_inputs_items'::regclass)")
        .fetch_all(&mut *conn).await.unwrap().into_iter().map(|(c, o): (String, i64)| (c, o as u32)).collect();
    let mut seen = std::collections::BTreeSet::new();
    let mut inputs = std::collections::BTreeSet::new();
    let mut clones = 0;
    while let Some((class, oid)) = frontier.pop() {
        if !seen.insert((class.clone(), oid)) { continue; }
        let deps: Vec<(String, i64, i32, Option<String>, Option<String>, Option<String>, Option<String>)> = sqlx::query_as(
            "SELECT d.refclassid::regclass::text,d.refobjid::int8,d.refobjsubid,c.relkind::text,n.nspname::text,
              CASE WHEN d.refclassid='pg_class'::regclass THEN c.relname::text WHEN d.refclassid='pg_proc'::regclass THEN p.proname::text END,
              CASE WHEN d.refclassid='pg_class'::regclass AND d.refobjsubid>0 THEN a.attname::text END
             FROM pg_depend d
             LEFT JOIN pg_class c ON d.refclassid='pg_class'::regclass AND c.oid=d.refobjid
             LEFT JOIN pg_proc p ON d.refclassid='pg_proc'::regclass AND p.oid=d.refobjid
             LEFT JOIN pg_namespace n ON n.oid=COALESCE(c.relnamespace,p.pronamespace)
             LEFT JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum=d.refobjsubid
             WHERE d.classid=$1::regclass AND d.objid=$2::int8::oid")
            .bind(&class).bind(oid as i64).fetch_all(&mut *conn).await.unwrap();
        for (refclass, refoid, subid, relkind, nsp, name, column) in deps {
            let (Some(nsp), Some(name)) = (nsp, name) else { continue };
            if nsp == "pg_catalog" || nsp == "information_schema" { continue; }
            match (refclass.as_str(), relkind.as_deref()) {
                ("pg_class", Some("r")) if subid > 0 => { inputs.insert((name, column.unwrap())); }
                ("pg_class", Some("v")) => {
                    let rules: Vec<i64> = sqlx::query_scalar("SELECT oid::int8 FROM pg_rewrite WHERE ev_class=$1::int8::oid").bind(refoid).fetch_all(&mut *conn).await.unwrap();
                    frontier.extend(rules.into_iter().map(|r| ("pg_rewrite".to_string(), r as u32)));
                }
                ("pg_proc", _) => {
                    let (lang, atomic, args, result, body): (String, bool, String, String, String) = sqlx::query_as(
                        "SELECT l.lanname::text,p.prosqlbody IS NOT NULL,pg_get_function_arguments(p.oid),pg_get_function_result(p.oid),p.prosrc FROM pg_proc p JOIN pg_language l ON l.oid=p.prolang WHERE p.oid=$1::int8::oid")
                        .bind(refoid).fetch_one(&mut *conn).await.unwrap();
                    if nsp != "public" { continue; } // extension functions (pgvector, pg_trgm)
                    // The reader's team-seat history floors (#1525, tenant 0204 /
                    // control 0388). Every context that computes or admits stored
                    // evidence has none: the worker runs in system scope, and the
                    // accelerated reader refuses a floored reader
                    // (`permission_window`). So `history_floor_admits(..)` is
                    // constant true there, and the floor is not an evidence input.
                    if name == "reader_history_floors" { continue; }
                    assert_eq!(lang, "sql", "evidence reads {name} ({lang}): its inputs cannot be derived; extend evidence_inputs");
                    if atomic { frontier.push(("pg_proc".into(), refoid as u32)); continue; }
                    clones += 1;
                    let clone = format!("evidence_input_fn_{clones}");
                    let body = body.trim().trim_end_matches(';');
                    conn.execute(format!("CREATE FUNCTION pg_temp.{clone}({args}) RETURNS {result} LANGUAGE sql STABLE BEGIN ATOMIC {body}; END").as_str()).await
                        .unwrap_or_else(|e| panic!("clone {name}: {e}"));
                    let cloned: i64 = sqlx::query_scalar(&format!("SELECT 'pg_temp.{clone}'::regproc::oid::int8")).fetch_one(&mut *conn).await.unwrap();
                    frontier.push(("pg_proc".into(), cloned as u32));
                }
                _ => {}
            }
        }
    }
    inputs
}


/// The pristine rows one coverage mutation runs against: node `n` has a legacy
/// link to chunk atom `a` of item `i` (version `v`) and an active identity
/// correction; node `m` has a semantic link to outcome `s` of `v` and a legacy
/// link to the unbound atom `a2`; node `lone` has nothing. The `*2`
/// rows are valid alternative targets for key columns.
struct Inputs { owner: Uuid, lone: Uuid, n: Uuid, m: Uuid, a: Uuid, a2: Uuid, i: Uuid, i2: Uuid, v: Uuid, v2: Uuid, s: Uuid, s2: Uuid, c: Uuid }

async fn inputs_fixture(f: &Fixture) -> Inputs {
    let x = Inputs { owner: f.user.user_id, lone: Uuid::new_v4(), n: Uuid::new_v4(), m: Uuid::new_v4(), a: Uuid::new_v4(), a2: Uuid::new_v4(), i: Uuid::new_v4(), i2: Uuid::new_v4(),
        v: Uuid::new_v4(), v2: Uuid::new_v4(), s: Uuid::new_v4(), s2: Uuid::new_v4(), c: Uuid::new_v4() };
    let origin = r#"{"transport":"native","service_id":"fixture","content_kind":"note"}"#;
    f.owner.execute(format!(r#"
INSERT INTO nodes(node_id,graph_id,type,title,body) VALUES('{n}','{g}','entity','Coverage label','{{}}'),('{m}','{g}','entity','Coverage outcome','{{}}'),('{lone}','{g}','entity','Coverage lone','{{}}');
INSERT INTO atoms(atom_id,graph_id,source_type,source_id,content_blob_key,content_inline,timestamp,participants,header_len,ingested_by,title,promoted_from_revision)
 VALUES('{a}','{g}','note','coverage-source:chunk:0','coverage-key','support without label',now(),ARRAY['Someone'],1,'{actor}','Atom title',1),
       ('{a2}','{g}','note','coverage-other','coverage-key-2','other body',now(),ARRAY['Other'],1,'{actor}','Other title',1);
INSERT INTO node_atoms(node_id,atom_id) VALUES('{n}','{a}'),('{m}','{a2}');
INSERT INTO source_items(source_item_id,graph_id,source_type,provider_namespace,source_id,origin,access_state) VALUES
 ('{i}','{g}','note','fixture','coverage-source','{origin}','permitted'),('{i2}','{g}','note','fixture','coverage-other','{origin}','permitted');
INSERT INTO source_versions(version_id,source_item_id,graph_id,source_id,source_type,provider_namespace,raw_inline,origin,source_metadata) VALUES
 ('{v}','{i}','{g}','coverage-source','note','fixture','support','{origin}','{{"independence":"independent"}}'),
 ('{v2}','{i}','{g}','coverage-source','note','fixture','support v2','{origin}','{{}}');
UPDATE source_items SET current_version_id='{v}' WHERE source_item_id='{i}';
INSERT INTO source_version_chunks(graph_id,source_version_id,atom_id) VALUES('{g}','{v}','{a}');
INSERT INTO semantic_atoms(semantic_atom_id,graph_id,source_version_id,ordinal,kind,statement,extraction_version,model,entities,confidence,valid_from)
 VALUES('{s}','{g}','{v}',0,'fact','support','source-atoms-v1','fixture','[{{"mention":"Coverage outcome","type":"project"}}]',0.5,now()),
       ('{s2}','{g}','{v}',1,'fact','second','source-atoms-v1','fixture','[]',0.5,now());
INSERT INTO node_semantic_atoms(node_id,semantic_atom_id,graph_id) VALUES('{m}','{s}','{g}');
"#, n=x.n, m=x.m, a=x.a, a2=x.a2, i=x.i, i2=x.i2, v=x.v, v2=x.v2, s=x.s, s2=x.s2, lone=x.lone, g=f.graph, actor=f.user.user_id).as_str()).await.unwrap();
    // An active correction on n, recorded by the graph's owner (0157 checks
    // the actor, the account and the exact current support).
    let mut tx = f.owner.begin().await.unwrap();
    sqlx::query("SELECT set_config('app.account_id',$1,true),set_config('app.user_id',$2,true)").bind(f.user.account_id.to_string()).bind(f.user.user_id.to_string()).execute(&mut *tx).await.unwrap();
    sqlx::query("UPDATE nodes SET title='Coverage label' WHERE node_id=$1").bind(x.n).execute(&mut *tx).await.unwrap();
    let a_legacy: Uuid = sqlx::query_scalar("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,timestamp) VALUES($1,'note','coverage-correction','fixture','support without label',now()) RETURNING atom_id")
        .bind(f.graph).fetch_one(&mut *tx).await.unwrap();
    sqlx::query("INSERT INTO node_atoms(node_id,atom_id) VALUES($1,$2)").bind(x.n).bind(a_legacy).execute(&mut *tx).await.unwrap();
    sqlx::query("INSERT INTO reef_identity_corrections(correction_id,graph_id,node_id,label,recorded_by,support_atom_id,support_kind,content_revision,input_sha256) SELECT $1,$2,$3,'Coverage label',$4,atom_id,'legacy',content_revision,reef_identity_atom_fingerprint(a) FROM atoms a WHERE atom_id=$5")
        .bind(x.c).bind(f.graph).bind(x.n).bind(f.user.user_id).bind(a_legacy).execute(&mut *tx).await.unwrap();
    tx.commit().await.unwrap();
    // The stored item grain the item keys resolve through.
    for node in [x.n, x.m] { cached(f, node, x.i).await; }
    clear(f).await;
    x
}

/// The row a mutation of `table.column` targets, and the nodes whose evidence
/// it feeds.
fn coverage_row(x: &Inputs, table: &str, column: &str) -> (String, Vec<Uuid>) {
    match table {
        // Chunk mapping made `a` source-bound, which is sticky: bind `a2`.
        "atoms" if column == "source_evidence_bound" => (format!("atom_id='{}'", x.a2), vec![x.m]),
        "atoms" => (format!("atom_id='{}'", x.a), vec![x.n]),
        // A node row's own columns: a node no link or correction pins, so its
        // graph and id can move too.
        "nodes" => (format!("node_id='{}'", x.lone), vec![x.lone]),
        "node_atoms" => (format!("node_id='{}' AND atom_id='{}'", x.n, x.a), vec![x.n]),
        "node_semantic_atoms" => (format!("node_id='{}' AND semantic_atom_id='{}'", x.m, x.s), vec![x.m]),
        "semantic_atoms" => (format!("semantic_atom_id='{}'", x.s), vec![x.m]),
        "source_items" => (format!("source_item_id='{}'", x.i), vec![x.n, x.m]),
        "source_versions" => (format!("version_id='{}'", x.v), vec![x.n, x.m]),
        "source_version_chunks" => (format!("source_version_id='{}' AND atom_id='{}'", x.v, x.a), vec![x.n]),
        "reef_identity_corrections" => (format!("correction_id='{}'", x.c), vec![x.n]),
        other => panic!("the evidence definition reads {other}: add its coverage row"),
    }
}

/// A SET clause that changes `column`: a valid alternative for keys and
/// constrained values, otherwise a type-generic different value.
fn coverage_set(x: &Inputs, foreign_graph: Uuid, table: &str, column: &str, ty: &str) -> String {
    let fixed = match (table, column) {
        (_, "graph_id") => Some(format!("'{foreign_graph}'")),
        ("node_atoms", "node_id") | ("node_semantic_atoms", "node_id") => Some(format!("'{}'", if table == "node_atoms" { x.m } else { x.n })),
        ("node_atoms", "atom_id") | ("source_version_chunks", "atom_id") | ("atoms", "promoted_from_atom_id") => Some(format!("'{}'", x.a2)),
        ("node_semantic_atoms", "semantic_atom_id") => Some(format!("'{}'", x.s2)),
        ("source_items", "current_version_id") | ("semantic_atoms", "source_version_id") | ("source_version_chunks", "source_version_id") => Some(format!("'{}'", x.v2)),
        ("source_versions", "source_item_id") => Some(format!("'{}'", x.i2)),
        ("source_items", "access_state") => Some("'restricted'".into()),
        ("atoms", "ingested_by") => Some("NULL".into()),
        ("nodes", "owner_id") => Some(format!("'{}'", x.owner)),
        ("nodes", "promoted_from_node_id") => Some(format!("'{}'", x.m)),
        ("nodes", "promoted_from_graph_id") => Some(format!("'{foreign_graph}'")),
        ("source_versions", "extraction_status") => Some("CASE WHEN extraction_status='done' THEN 'pending' ELSE 'done' END".into()),
        ("source_versions", "representation") => Some("CASE WHEN representation='provider_summary' THEN 'original_text' ELSE 'provider_summary' END".into()),
        ("semantic_atoms", "ordinal") => Some("ordinal+10".into()),
        ("semantic_atoms", "stated_changes") => Some("'[{\"mutated\":true}]'::jsonb".into()),
        ("atoms", "chunk_sha256") => Some("repeat('a',64)".into()),
        ("nodes", "sync_mode") => Some("CASE WHEN sync_mode='live' THEN 'static' ELSE 'live' END".into()),
        ("source_versions", "extraction_version") | ("semantic_atoms", "extraction_version") => Some("'source-atoms-v2'".into()),
        ("semantic_atoms", "kind") => Some("CASE WHEN kind='decision' THEN 'fact' ELSE 'decision' END".into()),
        ("semantic_atoms", "confidence") => Some("0.25".into()),
        ("semantic_atoms", "entities") => Some("'[{\"mention\":\"Other\",\"type\":\"project\"}]'::jsonb".into()),
        ("nodes", "body") => Some("body||'{\"synthetic_hub\":\"true\"}'::jsonb".into()),
        ("nodes", "type") => Some("CASE WHEN type='thread' THEN 'entity' ELSE 'thread' END".into()),
        ("nodes", "entity_type") => Some("CASE WHEN entity_type='project' THEN 'person' ELSE 'project' END".into()),
        ("reef_identity_corrections", "state") => Some("'superseded'".into()),
        ("reef_identity_corrections", "label") => Some("NULL,recorded_by=NULL,state='invalidated'".into()),
        ("reef_identity_corrections", "support_kind") => Some("'bound'".into()),
        _ => None,
    };
    let value = fixed.unwrap_or_else(|| match ty {
        "uuid" => "gen_random_uuid()".into(),
        "text" => format!("COALESCE({column}||'-mutated','mutated')"),
        "text[]" => format!("COALESCE({column},'{{}}')||'mutated'::text"),
        "boolean" => format!("NOT COALESCE({column},false)"),
        "integer" | "bigint" | "smallint" => format!("COALESCE({column},0)+1"),
        "real" | "double precision" | "numeric" => format!("COALESCE({column},0)+1"),
        "timestamp with time zone" => format!("COALESCE({column},now())+interval '1 hour'"),
        "jsonb" => format!("COALESCE({column},'{{}}')||'{{\"mutated\":true}}'::jsonb"),
        t if t.starts_with("vector") => "array_fill(0.25::real,ARRAY[384])::vector".into(),
        "tsvector" => "to_tsvector('simple','mutated')".into(),
        "xid8" => "pg_current_xact_id()".into(),
        other => panic!("{table}.{column}: no generic mutation for {other}"),
    });
    format!("{column}={value}")
}

/// Writes the schema refuses outright, so no capture is needed for them. The
/// coverage pass asserts each one still fails: a write that becomes possible
/// must be covered instead.
const REFUSED: &[(&str, &str, &str)] = &[
    ("atoms", "atom_id", "node_atoms/source_version_chunks reference it without ON UPDATE"),
    ("atoms", "graph_id", "source_version_chunks references (atom_id,graph_id)"),
    ("node_semantic_atoms", "graph_id", "references (node_id,graph_id) and (semantic_atom_id,graph_id)"),
    ("semantic_atoms", "graph_id", "node_semantic_atoms references (semantic_atom_id,graph_id)"),
    ("semantic_atoms", "semantic_atom_id", "node_semantic_atoms references it without ON UPDATE"),
    ("source_items", "graph_id", "source_versions references it"),
    ("source_items", "source_item_id", "source_versions references it"),
    ("source_versions", "graph_id", "source_version_chunks/semantic_atoms reference (version_id,graph_id)"),
    ("source_versions", "source_item_id", "source_version_observations references (version_id,source_item_id)"),
    ("source_versions", "version_id", "source_version_chunks/semantic_atoms reference it"),
    ("source_version_chunks", "graph_id", "references atoms (atom_id,graph_id)"),
    ("reef_identity_corrections", "content_revision", "0157 corrections are immutable events"),
    ("reef_identity_corrections", "graph_id", "0157 corrections are immutable events"),
    ("reef_identity_corrections", "input_sha256", "0157 corrections are immutable events"),
    ("reef_identity_corrections", "node_id", "0157 corrections are immutable events"),
    ("reef_identity_corrections", "source_item_id", "0157 corrections are immutable events"),
    ("reef_identity_corrections", "source_version_id", "0157 corrections are immutable events"),
    ("reef_identity_corrections", "support_atom_id", "0157 corrections are immutable events"),
    ("reef_identity_corrections", "support_kind", "0157 corrections are immutable events"),
];

/// Runs one UPDATE per (table, column) on the pristine fixture, each in its own
/// rolled-back transaction, and returns every miss: an input write that did
/// not resolve to its node, a non-input atoms/nodes write that logged
/// anything, or a refusal that no longer holds. `drop_trigger` drops one
/// capture trigger inside each transaction (the failing control).
async fn coverage_misses(f: &Fixture, x: &Inputs, inputs: &std::collections::BTreeSet<(String, String)>, drop_trigger: Option<&str>) -> Vec<String> {
    let mut misses = vec![];
    let mut columns: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT c.relname::text,a.attname::text,format_type(a.atttypid,a.atttypmod) FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid
         WHERE c.relnamespace='public'::regnamespace AND c.relname=ANY($1) AND a.attnum>0 AND NOT a.attisdropped AND a.attgenerated='' ORDER BY 1,2")
        .bind(inputs.iter().map(|(t, _)| t.clone()).collect::<std::collections::BTreeSet<_>>().into_iter().collect::<Vec<_>>())
        .fetch_all(&f.owner).await.unwrap();
    columns.retain(|(t, c, _)| inputs.contains(&(t.clone(), c.clone())) || t == "atoms" || t == "nodes");
    for (table, column, ty) in columns {
        let input = inputs.contains(&(table.clone(), column.clone()));
        let refused = REFUSED.iter().any(|(t, c, _)| *t == table && *c == column);
        let (row, expected) = coverage_row(x, &table, &column);
        let set = coverage_set(x, f.foreign_graph, &table, &column, &ty);
        let mut tx = f.owner.begin().await.unwrap();
        if let Some(trigger) = drop_trigger { tx.execute(trigger).await.unwrap(); }
        tx.execute("DELETE FROM library_evidence_changes").await.unwrap();
        let before: Option<Option<Value>> = sqlx::query_scalar(&format!("SELECT to_jsonb(t)->'{column}' FROM {table} t WHERE {row}")).fetch_optional(&mut *tx).await.unwrap();
        let before = before.unwrap_or_else(|| panic!("{table}: fixture row missing"));
        tx.execute("SAVEPOINT mutation").await.unwrap();
        let result = tx.execute(format!("UPDATE {table} SET {set} WHERE {row}").as_str()).await;
        match (result, refused) {
            (Err(e), true) => {
                // Only the schema's own refusals count: a foreign key, or 0157's
                // immutable correction event. Anything else is a broken mutation.
                let code = e.as_database_error().and_then(|d| d.code()).map(|c| c.to_string());
                assert!(code.as_deref() == Some("23503") || e.to_string().contains("immutable"), "{table}.{column}: refused for another reason: {e}");
                tx.rollback().await.unwrap();
                continue;
            }
            (Err(e), false) => panic!("{table}.{column}: mutation `{set}` failed: {e}"),
            (Ok(_), true) => { misses.push(format!("{table}.{column}: declared refused but the write succeeded")); tx.rollback().await.unwrap(); continue; }
            (Ok(done), false) => assert_eq!(done.rows_affected(), 1, "{table}.{column}: mutation hit no row"),
        }
        // Key moves leave the row under its new key; read it back by either.
        let after: Vec<Option<Value>> = sqlx::query_scalar(&format!("SELECT to_jsonb(t)->'{column}' FROM {table} t WHERE {}", if column.ends_with("_id") { "true".to_string() } else { row.clone() }))
            .fetch_all(&mut *tx).await.unwrap();
        // An input write must really change the input; a non-input write is a
        // probe that nothing logs, changed or not (a BEFORE trigger pins some).
        assert!(!input || after.iter().any(|v| *v != before), "{table}.{column}: `{set}` did not change the column");
        let logged: i64 = sqlx::query_scalar("SELECT count(*) FROM library_evidence_changes").fetch_one(&mut *tx).await.unwrap();
        let resolved: Vec<Uuid> = sqlx::query_scalar("SELECT DISTINCT r.node_id FROM library_evidence_changes c CROSS JOIN LATERAL library_evidence_change_nodes(c.graph_id,c.id,NULL,4096) r")
            .fetch_all(&mut *tx).await.unwrap();
        tx.rollback().await.unwrap();
        if input {
            for node in expected { if !resolved.contains(&node) { misses.push(format!("{table}.{column}: write did not enqueue node {node} ({logged} keys)")); } }
        } else if logged != 0 {
            misses.push(format!("{table}.{column}: not an evidence input, but its write logged {logged} keys"));
        }
    }
    misses
}

/// Every evidence input's write enqueues its node(s); nothing else on atoms or
/// nodes does. The input set is derived from the definition, not from the
/// triggers. Dropping one capture trigger makes the same pass fail.
#[tokio::test]
async fn library_evidence_capture_covers_every_input() {
    if std::env::var("ORGT_TEST_BASE_URL").is_err() { eprintln!("skip: ORGT_TEST_BASE_URL required"); return; }
    for tenant in [false, true] {
        let f = Fixture::new(tenant, "capture_inputs").await;
        let x = inputs_fixture(&f).await;
        let mut conn = f.owner.acquire().await.unwrap();
        let mut tx = conn.begin().await.unwrap();
        sqlx::query("SET LOCAL search_path=public,pg_temp").execute(&mut *tx).await.unwrap();
        let inputs = evidence_inputs(&mut tx).await;
        tx.rollback().await.unwrap();
        drop(conn);
        // Non-vacuous: the definition reads every family the capture serves.
        for table in ["atoms", "nodes", "node_atoms", "node_semantic_atoms", "semantic_atoms", "source_items", "source_versions", "source_version_chunks", "reef_identity_corrections"] {
            assert!(inputs.iter().any(|(t, _)| t == table), "derived inputs lost {table}: {inputs:?}");
        }
        assert!(!inputs.contains(&("atoms".into(), "embedding".into())) && !inputs.contains(&("nodes".into(), "coral_count".into())));
        let misses = coverage_misses(&f, &x, &inputs, None).await;
        assert!(misses.is_empty(), "plane tenant={tenant}: {misses:#?}");
        eprintln!("plane tenant={tenant}: {} derived inputs over {} tables, all enqueue their nodes", inputs.len(),
            inputs.iter().map(|(t, _)| t).collect::<std::collections::BTreeSet<_>>().len());
        // The embed workers' writes, by name: nothing to log.
        clear(&f).await;
        sqlx::query("UPDATE atoms SET embedding=array_fill(0.5::real,ARRAY[384])::vector,embedding_next=array_fill(0.5::real,ARRAY[384])::vector,embed_model_next='fixture',embed_attempts=embed_attempts+1 WHERE graph_id=$1")
            .bind(f.graph).execute(&f.owner).await.unwrap();
        let logged: i64 = sqlx::query_scalar("SELECT count(*) FROM library_evidence_changes").fetch_one(&f.owner).await.unwrap();
        assert_eq!(logged, 0, "embedding-only UPDATE logged keys");
        // Atom and node INSERTs log nothing; the link that makes them evidence
        // logs its node once (node_atoms' count triggers UPDATE no input).
        let atom: Uuid = sqlx::query_scalar("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,timestamp) VALUES($1,'note','coverage-new','fixture','body',now()) RETURNING atom_id")
            .bind(f.graph).fetch_one(&f.owner).await.unwrap();
        let node = Uuid::new_v4();
        sqlx::query("INSERT INTO nodes(node_id,graph_id,type,title,body) VALUES($1,$2,'entity','Coverage new','{}')").bind(node).bind(f.graph).execute(&f.owner).await.unwrap();
        let logged: i64 = sqlx::query_scalar("SELECT count(*) FROM library_evidence_changes").fetch_one(&f.owner).await.unwrap();
        assert_eq!(logged, 0, "atom/node INSERT logged keys");
        // Its evidence before any link is the constant row no reader admits,
        // so the dropped INSERT capture cannot leave a stale stored row.
        let row: (i64, i64, i64, i64, i64, bool, bool, bool, bool, bool, bool) = sqlx::query_as(&format!(
            "SELECT source_count,unknown_independence_record_count,atom_count_retained,atom_count_served,unmapped_legacy_count,admitted_unfiltered,has_source_link,title_visible,project_mention,work_eligible,stats_admitted FROM ({}) e",
            coralswarm_backend::library_read::evidence::NODE_EVIDENCE_SQL.as_str()))
            .bind(f.graph).bind(vec![node]).fetch_one(&f.owner).await.unwrap();
        assert_eq!(row, (0, 0, 0, 0, 0, false, false, false, false, false, false), "a link-less node's evidence is not the constant row");
        sqlx::query("INSERT INTO node_atoms(node_id,atom_id) VALUES($1,$2)").bind(node).bind(atom).execute(&f.owner).await.unwrap();
        let keys: Vec<Option<Uuid>> = sqlx::query_scalar("SELECT node_id FROM library_evidence_changes").fetch_all(&f.owner).await.unwrap();
        assert_eq!(keys, vec![Some(node)], "a new link logs exactly its node");
        // The queue CHECK (exactly one key) cannot fail a writer: every capture
        // INSERT names graph_id and one key column, and every key it copies is
        // NOT NULL in its source table.
        let bodies: Vec<(String, String)> = sqlx::query_as("SELECT DISTINCT p.proname::text,p.prosrc FROM pg_trigger t JOIN pg_proc p ON p.oid=t.tgfoid WHERE t.tgname LIKE 'library_evidence_%' AND NOT t.tgisinternal")
            .fetch_all(&f.owner).await.unwrap();
        assert_eq!(bodies.len(), 3, "capture functions: {:?}", bodies.iter().map(|b| &b.0).collect::<Vec<_>>());
        let mut inserts = 0;
        for (name, body) in &bodies {
            for part in body.split("INSERT INTO library_evidence_changes(").skip(1) {
                inserts += 1;
                let cols: Vec<&str> = part[..part.find(')').unwrap()].split(',').map(str::trim).collect();
                assert!(cols.len() == 2 && cols[0] == "graph_id" && ["node_id", "source_item_id", "atom_id", "semantic_atom_id"].contains(&cols[1]), "{name}: capture INSERT names {cols:?}");
            }
        }
        assert!(inserts >= 15, "only {inserts} capture INSERTs parsed");
        let nullable: Vec<String> = sqlx::query_scalar("SELECT c.relname||'.'||a.attname FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid WHERE c.relnamespace='public'::regnamespace AND NOT a.attnotnull AND (c.relname,a.attname) IN (('node_atoms','node_id'),('nodes','node_id'),('node_semantic_atoms','node_id'),('reef_identity_corrections','node_id'),('atoms','atom_id'),('source_version_chunks','atom_id'),('semantic_atoms','semantic_atom_id'),('source_versions','source_item_id'),('source_items','source_item_id'))")
            .fetch_all(&f.owner).await.unwrap();
        assert!(nullable.is_empty(), "a captured key column is nullable: {nullable:?}");
        // Failing control: without the atoms UPDATE capture the same pass
        // reports every atoms input.
        let control = coverage_misses(&f, &x, &inputs, Some("DROP TRIGGER library_evidence_atoms_update ON atoms")).await;
        let atom_inputs = inputs.iter().filter(|(t, c)| t == "atoms" && !REFUSED.iter().any(|(rt, rc, _)| *rt == "atoms" && rc == c)).count();
        assert_eq!(control.iter().filter(|m| m.starts_with("atoms.")).count(), atom_inputs, "control: {control:#?}");
        assert_eq!(control.len(), atom_inputs, "control misses only atoms: {control:#?}");
        f.close().await;
    }
}

/// Capture honours the listing switch (0201/0380) on both planes: Off logs
/// nothing for any family or event, so an Off plane's log cannot grow; On
/// logs again from the next write.
#[tokio::test]
async fn an_off_switch_logs_no_evidence_change_on_either_plane() {
    // No skip: Fixture::new fails loudly without ORGT_TEST_BASE_URL.
    for tenant in [false, true] {
        let f = Fixture::new(tenant, "capture_switch").await;
        let (n, m, a, i, v, s) = evidence_fixture(&f).await;
        async fn logged(f: &Fixture) -> i64 {
            sqlx::query_scalar("SELECT count(*) FROM library_evidence_changes WHERE graph_id=$1").bind(f.graph).fetch_one(&f.owner).await.unwrap()
        }
        // A correction's legacy support: an unmapped atom linked to the node.
        let support: Uuid = sqlx::query_scalar("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,timestamp) VALUES($1,'note','correction-support','fixture-key','body',now()) RETURNING atom_id").bind(f.graph).fetch_one(&f.owner).await.unwrap();
        sqlx::query("INSERT INTO node_atoms(node_id,atom_id) VALUES($1,$2)").bind(n).bind(support).execute(&f.owner).await.unwrap();
        assert!(logged(&f).await > 0, "the On fixture captures its own setup");
        assert_eq!(library_evidence::flip_listing_switch(&f.owner, false).await.unwrap(), Some(false));
        clear(&f).await;
        // Every capture family, including the row-level UPDATE triggers, the
        // grant and identity arms of source_items, and the DELETE cascades.
        sqlx::query("UPDATE nodes SET title='Renamed while off' WHERE node_id=$1").bind(n).execute(&f.owner).await.unwrap();
        sqlx::query("UPDATE atoms SET participants=ARRAY['Changed while off'] WHERE atom_id=$1").bind(a).execute(&f.owner).await.unwrap();
        let b: Uuid = sqlx::query_scalar("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,timestamp) VALUES($1,'note','off-second','fixture-key','body',now()) RETURNING atom_id").bind(f.graph).fetch_one(&f.owner).await.unwrap();
        sqlx::query("INSERT INTO node_atoms(node_id,atom_id) VALUES($1,$2)").bind(m).bind(b).execute(&f.owner).await.unwrap();
        sqlx::query("UPDATE source_items SET access_state='permitted' WHERE source_item_id=$1").bind(i).execute(&f.owner).await.unwrap();
        // reef_identity_corrections: INSERT, UPDATE and DELETE, under the
        // writer's own account/user settings as an actual correction is made.
        let mut tx = f.owner.begin().await.unwrap();
        sqlx::query("SELECT set_config('app.account_id',$1,true),set_config('app.user_id',$2,true)").bind(f.user.account_id.to_string()).bind(f.user.user_id.to_string()).execute(&mut *tx).await.unwrap();
        let correction: Uuid = sqlx::query_scalar("INSERT INTO reef_identity_corrections(graph_id,node_id,label,recorded_by,support_atom_id,support_kind,content_revision,input_sha256) SELECT $1,n.node_id,n.title,$3,a.atom_id,'legacy',a.content_revision,reef_identity_atom_fingerprint(a) FROM atoms a JOIN nodes n ON n.node_id=$2 WHERE a.atom_id=$4 RETURNING correction_id")
            .bind(f.graph).bind(n).bind(f.user.user_id).bind(support).fetch_one(&mut *tx).await.unwrap();
        tx.commit().await.unwrap();
        sqlx::query("UPDATE reef_identity_corrections SET state='superseded' WHERE correction_id=$1").bind(correction).execute(&f.owner).await.unwrap();
        sqlx::query("DELETE FROM reef_identity_corrections WHERE correction_id=$1").bind(correction).execute(&f.owner).await.unwrap();
        sqlx::query("UPDATE source_items SET source_id='off-identity' WHERE source_item_id=$1").bind(i).execute(&f.owner).await.unwrap();
        sqlx::query("UPDATE source_versions SET representation='provider_summary' WHERE version_id=$1").bind(v).execute(&f.owner).await.unwrap();
        sqlx::query("UPDATE semantic_atoms SET statement='changed while off' WHERE semantic_atom_id=$1").bind(s).execute(&f.owner).await.unwrap();
        sqlx::query("DELETE FROM node_semantic_atoms WHERE semantic_atom_id=$1").bind(s).execute(&f.owner).await.unwrap();
        sqlx::query("DELETE FROM source_version_chunks WHERE source_version_id=$1").bind(v).execute(&f.owner).await.unwrap();
        sqlx::query("DELETE FROM node_atoms WHERE atom_id=$1").bind(a).execute(&f.owner).await.unwrap();
        sqlx::query("DELETE FROM source_items WHERE source_item_id=$1").bind(i).execute(&f.owner).await.unwrap();
        sqlx::query("DELETE FROM atoms WHERE atom_id=$1").bind(b).execute(&f.owner).await.unwrap();
        sqlx::query("DELETE FROM nodes WHERE node_id=$1").bind(m).execute(&f.owner).await.unwrap();
        assert_eq!(logged(&f).await, 0, "plane tenant={tenant}: Off logged a change");
        assert_eq!(library_evidence::flip_listing_switch(&f.owner, true).await.unwrap(), Some(true));
        sqlx::query("UPDATE nodes SET title='Renamed while on' WHERE node_id=$1").bind(n).execute(&f.owner).await.unwrap();
        assert_eq!(nodes(&f, "node_id", n).await, vec![n], "On logs from the next write");
        // Failing control for the corrections arm: the same write On logs.
        clear(&f).await;
        let mut tx = f.owner.begin().await.unwrap();
        sqlx::query("SELECT set_config('app.account_id',$1,true),set_config('app.user_id',$2,true)").bind(f.user.account_id.to_string()).bind(f.user.user_id.to_string()).execute(&mut *tx).await.unwrap();
        sqlx::query("INSERT INTO reef_identity_corrections(graph_id,node_id,label,recorded_by,support_atom_id,support_kind,content_revision,input_sha256) SELECT $1,n.node_id,n.title,$3,a.atom_id,'legacy',a.content_revision,reef_identity_atom_fingerprint(a) FROM atoms a JOIN nodes n ON n.node_id=$2 WHERE a.atom_id=$4")
            .bind(f.graph).bind(n).bind(f.user.user_id).bind(support).execute(&mut *tx).await.unwrap();
        tx.commit().await.unwrap();
        assert_eq!(nodes(&f, "node_id", n).await, vec![n], "On logs a correction");
        f.close().await;
    }
}
