//! Real-engine, real app-role SQL proof. No model/provider calls or role-password changes.
use coralswarm_backend::{library_read::{self,LibraryScope,evidence::READ_MODEL_VERSION},store::PostgresStore,tenancy::{extractor::Tenant,runtime::{TenancyDecision,TenantContext}},workers::library_evidence::{self,Limits}};
use serde_json::{json,Value};
use sqlx::{Connection,Executor,PgConnection,PgPool,postgres::{PgConnectOptions,PgPoolOptions}};
use std::{str::FromStr,sync::Arc};
use uuid::Uuid;

/// The sealed SQL hash must cover actual query selectors, including the
/// empty-only rank optimization and nonempty one/two-character scan template.
#[test]
fn sealed_definition_covers_empty_short_and_indexed_actual_statement_shapes() {
 use library_read::read_model::{DEFINITION_QUERY_VARIANTS,definition_sql};
 assert_eq!(DEFINITION_QUERY_VARIANTS,[("empty",""),("short","a"),("indexed","indexed")]);
 for mode in ["all_retained","current"] {for filtered in [false,true] {
  let mut shape=json!({"graph_ids":[Uuid::nil()],"outcome_versions":mode});
  if filtered {shape["collection"]=json!("meetings");}
  let scope:LibraryScope=serde_json::from_value(shape).unwrap();
  let empty=definition_sql(&scope,"");let short=definition_sql(&scope,"a");let indexed=definition_sql(&scope,"indexed");
  for query in ["a","ab","é","éé"] {assert_eq!(definition_sql(&scope,query),short,"mode={mode} filtered={filtered} query={query}");}
  for query in ["abc","indexed","ééé"] {assert_eq!(definition_sql(&scope,query),indexed,"mode={mode} filtered={filtered} query={query}");}
  assert_ne!(short,indexed);
  // Empty defers all semantic payload until the page; short must read
  // admitted statement/passage text, even when source filters are present.
  assert_ne!(empty,short);
  // Narrow requests use the same scoped text template even for long text;
  // admission selects the independent Live reader using the original binds.
  for kind in 0..3 {let mut narrow=scope.clone();match kind {0=>narrow.source_item_id=Some(Uuid::nil()),1=>narrow.source_version_id=Some(Uuid::nil()),_=>narrow.node_id=Some(Uuid::nil())};
   // An exact version intentionally reads retained captured membership even
   // when the request's outcome_versions is current; this is the existing
   // canonical retained-short template, already present in the sealed set.
   let mut reference=scope.clone();if kind==1 {reference.outcome_versions=Some(library_read::OutcomeVersions::AllRetained);}
   assert_eq!(definition_sql(&narrow,"indexed"),definition_sql(&reference,"a"));
  }
 }}
}

#[test]
fn sealed_program_covers_clean_dirty_live_and_contract_for_all_twelve_shapes() {
 use library_read::read_model::{definition_program,definition_sql,program_contract,DEFINITION_QUERY_VARIANTS};
 let mut hash=sha2::Sha256::new();use sha2::Digest;
 for mode in ["all_retained","current"] {for filtered in [false,true] {for (_,query) in DEFINITION_QUERY_VARIANTS {
  let mut shape=json!({"graph_ids":[Uuid::nil()],"outcome_versions":mode});if filtered{shape["collection"]=json!("meetings");}
  let scope:LibraryScope=serde_json::from_value(shape).unwrap();let program=definition_program(&scope,query);
  assert_eq!(program["clean_model_attempt_sql"],definition_sql(&scope,query));
  assert_eq!(program["dirty_model_attempt_sql"],library_read::read_model::definition_dirty_sql(&scope,query));assert_eq!(program["contract"],program_contract());
  let attempt=program["clean_model_attempt_sql"].as_str().unwrap();let dirty=program["dirty_model_attempt_sql"].as_str().unwrap();let live=program["conditional_live_sql"].as_str().unwrap();
  assert!(attempt.contains("ELSE NULL::jsonb END"));
  assert!(attempt.contains("model_clean_pending"));assert!(!attempt.contains("library_evidence_change_nodes"));
  assert!(!attempt.contains("node_evidence AS"));assert!(dirty.contains("library_evidence_change_nodes"));
  assert!(dirty.contains("model_dirty_items"));assert!(dirty.contains("dirty_item_input_overflow"));assert!(!live.contains("model_admission AS"));assert!(live.contains("scope_sources AS"));
  assert_eq!(program["contract"]["bind_types"].as_array().unwrap().len(),16);
  for bytes in [serde_json::to_string(&program["contract"]).unwrap(),attempt.into(),dirty.into(),live.into()] {hash.update(bytes.as_bytes());hash.update([0]);}
 }}}
 assert_eq!(format!("{:x}",hash.finalize()),library_read::read_model::definition_sha256());
}

/// The program contract seals actual clean and dirty bytes separately. Dirty
/// state cannot be advertised as a successful public Model response by clean SQL.
#[test]
fn definition_three_clean_has_no_raw_reconstruction_and_all_sixteen_bind_types() {
 for mode in ["all_retained","current"] {for filtered in [false,true] {for (_,query) in library_read::read_model::DEFINITION_QUERY_VARIANTS {
  let mut shape=json!({"graph_ids":[Uuid::nil()],"outcome_versions":mode});if filtered {shape["collection"]=json!("meetings");}
  let scope:LibraryScope=serde_json::from_value(shape).unwrap();
  let p=library_read::read_model::definition_program(&scope,query);
  let clean=p["clean_model_attempt_sql"].as_str().unwrap();let dirty=p["dirty_model_attempt_sql"].as_str().unwrap();
  assert_ne!(clean,dirty);assert_eq!(p["contract"]["version"],4);assert_eq!(p["contract"]["listing_definition"],3);
  assert!(clean.contains("library_listing_state"));assert!(clean.contains("library_evidence_state"));
  assert!(clean.contains("model_clean_pending"));assert!(clean.contains("sources_complete IS DISTINCT FROM true"));
  for raw in ["library_evidence_change_nodes(","library_evidence_raw_edges_safe(","listing_keys AS MATERIALIZED", "node_evidence AS"] {assert!(!clean.contains(raw));}
  assert!(dirty.contains("listing_keys AS MATERIALIZED"));assert!(dirty.contains("dirty_item_unresolved"));
  for bind in 1..=16 {assert!(clean.contains(&format!("${bind}")),"clean omitted bind{bind}");assert!(dirty.contains(&format!("${bind}")),"dirty omitted bind{bind}");}
 }}}
}

struct Fixture {admin:PgConnection,name:coralswarm_backend::test_db::TestDatabase,owner:PgPool,app:PgPool,tenant_plane:bool,graph:Uuid,foreign:Uuid,account:Uuid,user:Uuid}
impl Fixture {
    async fn new(tenant_plane: bool) -> Self {
        let base = std::env::var("ORGT_TEST_BASE_URL")
            .expect("ORGT_TEST_BASE_URL required for actual Library read-model proof");
        let opts = PgConnectOptions::from_str(&base).unwrap();
        assert!(matches!(opts.get_host(), "localhost" | "127.0.0.1" | "::1"));
        let mut admin = PgConnection::connect_with(&opts).await.unwrap();
        let name = coralswarm_backend::test_db::migrated_database(
            &opts,
            if tenant_plane {
                coralswarm_backend::test_db::Plane::Tenant
            } else {
                coralswarm_backend::test_db::Plane::Control
            },
            "library_rm4",
        )
        .await;
        let owner = PgPoolOptions::new()
            .max_connections(4)
            .connect_with(opts.clone().database(&name))
            .await
            .unwrap();
        sqlx::query("SELECT pg_advisory_lock($1)")
            .bind(0x0083_5C09_i64)
            .execute(&mut admin)
            .await
            .unwrap();
        // The migration ships the switch On; stamp its barrier, as the worker's first tick does.
        assert!(library_evidence::stamp_listing_barrier(&owner)
            .await
            .unwrap());
        sqlx::query("SELECT pg_advisory_unlock($1)")
            .bind(0x0083_5C09_i64)
            .execute(&mut admin)
            .await
            .unwrap();
        let role = if tenant_plane {
            "coralswarm_tenant_app"
        } else {
            "coralswarm_app"
        };
        // SET ROLE reaches the actual production role/policies without ALTERing a
        // cluster-global login/password used by another suite. No BYPASSRLS role.
        let app = PgPoolOptions::new()
            .max_connections(4)
            .after_connect(move |conn, _| {
                Box::pin(async move {
                    conn.execute(if role == "coralswarm_app" {
                        "SET ROLE coralswarm_app"
                    } else {
                        "SET ROLE coralswarm_tenant_app"
                    })
                    .await?;
                    Ok(())
                })
            })
            .connect_with(opts.database(&name))
            .await
            .unwrap();
        let user = Uuid::new_v4();
        let account = Uuid::new_v4();
        let graph = Uuid::new_v4();
        let foreign = Uuid::new_v4();
        if tenant_plane {
            sqlx::query("INSERT INTO tenant_meta(id,org_id) VALUES(true,'org_library_rm4')")
                .execute(&owner)
                .await
                .unwrap();
        } else {
            sqlx::query("INSERT INTO users(id,email) VALUES($1,$2)")
                .bind(user)
                .bind(format!("{user}@rm4.invalid"))
                .execute(&owner)
                .await
                .unwrap();
            sqlx::query("INSERT INTO accounts(id,owner_user_id) VALUES($1,$2)")
                .bind(account)
                .bind(user)
                .execute(&owner)
                .await
                .unwrap();
            sqlx::query("INSERT INTO account_members(account_id,user_id,role,seat_tier_id) VALUES($1,$2,'owner',(SELECT CASE WHEN a.is_org_account THEN (SELECT id FROM seat_tiers WHERE code='standard' AND is_active) END FROM accounts a WHERE a.id=$1))").bind(account).bind(user).execute(&owner).await.unwrap();
        }
        sqlx::query("INSERT INTO graphs(id,account_id,name,kind,owner_user_id) VALUES($1,$3,'Fixture','member',$4),($2,$3,'Foreign','member',$5)").bind(graph).bind(foreign).bind(account).bind(user).bind(Uuid::new_v4()).execute(&owner).await.unwrap();
        Self {
            admin,
            name,
            owner,
            app,
            tenant_plane,
            graph,
            foreign,
            account,
            user,
        }
    }
    fn tenant(&self, app: bool) -> Tenant {
        let pool = if app {
            self.app.clone()
        } else {
            self.owner.clone()
        };
        Tenant {
            pool: Some(pool.clone()),
            store: Some(Arc::new(PostgresStore::new(pool.clone()))),
            decision: self.tenant_plane.then(|| {
                TenancyDecision::Org(TenantContext {
                    org_id: "org_library_rm4".into(),
                    account_id: self.account,
                    user_id: self.user,
                    pool,
                    page_pool: None,
                })
            }),
            class: coralswarm_backend::route_class::RouteClass::Other,
        }
    }
    /// The same pool/role choice as [`Self::tenant`], read as another org user.
    fn tenant_for(&self, app: bool, user: Uuid) -> Tenant {
        let pool = if app {
            self.app.clone()
        } else {
            self.owner.clone()
        };
        Tenant {
            pool: Some(pool.clone()),
            store: Some(Arc::new(PostgresStore::new(pool.clone()))),
            decision: self.tenant_plane.then(|| {
                TenancyDecision::Org(TenantContext {
                    org_id: "org_library_rm4".into(),
                    account_id: self.account,
                    user_id: user,
                    pool,
                    page_pool: None,
                })
            }),
            class: coralswarm_backend::route_class::RouteClass::Other,
        }
    }
    /// Three cursor pages of one shape through the Live == Model oracle; returns
    /// the first page's path, fallback reason and Live value.
    async fn pair_as(
        &self,
        tenant: &Tenant,
        extra: Value,
        query: &str,
    ) -> (String, Option<String>, Value) {
        let mut shape = json!({"graph_ids":[self.graph]});
        shape
            .as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        let mut scope: LibraryScope = serde_json::from_value(shape).unwrap();
        scope.normalize().unwrap();
        let mut tx = tenant
            .begin_member_repeatable_read(self.account)
            .await
            .unwrap();
        library_read::configure_read_model_proof_tx(&mut tx)
            .await
            .unwrap();
        let mut previous: Option<Value> = None;
        let mut first = None;
        for _ in 0..3 {
            let after = std::array::from_fn(|i| {
                previous
                    .as_ref()
                    .and_then(|v| v[["sources", "outcomes", "nodes"][i]]["next_cursor"].as_str())
            });
            let pair = library_read::compare_read_model_tx(&mut tx, &scope, query, 2, after)
                .await
                .unwrap();
            assert_eq!(
                pair.live, pair.candidate,
                "plane={} extra={extra} query={query} path={} reason={:?}",
                self.tenant_plane, pair.path, pair.fallback_reason
            );
            if first.is_none() {
                first = Some((
                    pair.path.clone(),
                    pair.fallback_reason.clone(),
                    pair.live.clone(),
                ));
            } else {
                assert_eq!(
                    first.as_ref().map(|f| (&f.0, &f.1)),
                    Some((&pair.path, &pair.fallback_reason)),
                    "one admission per snapshot"
                );
            }
            previous = Some(pair.live);
        }
        tx.rollback().await.unwrap();
        first.unwrap()
    }
    /// Every shape the matrix proves: both outcome modes, unfiltered and
    /// filtered, empty and text queries. Asserts one path/reason for all.
    async fn shapes_as(&self, tenant: &Tenant, path: &str, reason: Option<&str>) -> Value {
        let mut whole = Value::Null;
        for mode in ["all_retained", "current"] {
            for extra in [
                json!({}),
                json!({"service_id":"slack"}),
                json!({"collection":"meetings"}),
            ] {
                for query in ["", "Atlas"] {
                    let mut extra = extra.clone();
                    extra["outcome_versions"] = json!(mode);
                    let (actual, why, value) = self.pair_as(tenant, extra.clone(), query).await;
                    assert_eq!(
                        (actual.as_str(), why.as_deref()),
                        (path, reason),
                        "plane={} extra={extra} query={query}",
                        self.tenant_plane
                    );
                    if mode == "all_retained"
                        && query.is_empty()
                        && extra.get("service_id").is_none()
                        && extra.get("collection").is_none()
                    {
                        whole = value;
                    }
                }
            }
        }
        whole
    }
    async fn refresh(&self, app: bool) {
        self.refresh_graph(app, self.graph).await;
    }
    async fn refresh_graph(&self, app: bool, graph: Uuid) {
        let pool = if app { &self.app } else { &self.owner };
        // Listing generations deliberately expose bounded metadata/row phases.
        // This fixture-only finite convergence bound is not a worker/query deadline;
        // zero-row phase transitions alone are not evidence of stalled maintenance.
        for _ in 0..5000 {
            let mut tx = pool.begin().await.unwrap();
            sqlx::query("SELECT set_config('app.account_id',$1,true),set_config('app.tenant_actor',$2,true)").bind(self.account.to_string()).bind(if self.tenant_plane{"system"}else{""}).execute(&mut *tx).await.unwrap();
            library_evidence::refresh_graph_tx(
                &mut tx,
                graph,
                Limits {
                    nodes: 128,
                    changes: 128,
                },
            )
            .await
            .unwrap();
            let ready:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM library_evidence_state WHERE graph_id=$1 AND ready_version=$2) AND EXISTS(SELECT 1 FROM library_listing_state WHERE graph_id=$1 AND ready_version=3 AND sources_complete AND nodes_complete AND aggregates_complete AND sources_old_complete) AND NOT EXISTS(SELECT 1 FROM library_evidence_changes WHERE graph_id=$1) AND NOT EXISTS(SELECT 1 FROM library_listing_changes WHERE graph_id=$1)").bind(graph).bind(READ_MODEL_VERSION).fetch_one(&mut *tx).await.unwrap();
            tx.commit().await.unwrap();
            if ready {
                return;
            }
        }
        panic!("fixture refresh did not become ready");
    }
    async fn node(&self, g: Uuid, title: &str) -> Uuid {
        sqlx::query_scalar("INSERT INTO nodes(graph_id,type,title,body) VALUES($1,'entity',$2,'{}') RETURNING node_id").bind(g).bind(title).fetch_one(&self.owner).await.unwrap()
    }
    async fn source(&self, node: Uuid, id: &str, access: &str, kind: &str) -> (Uuid, Uuid, Uuid) {
        self.source_in_graph(self.graph, node, id, access, kind)
            .await
    }
    async fn source_in_graph(
        &self,
        graph: Uuid,
        node: Uuid,
        id: &str,
        access: &str,
        kind: &str,
    ) -> (Uuid, Uuid, Uuid) {
        let origin = json!({"service_id":"slack","content_kind":kind,"provider_namespace":id,"connection":{"kind":"oauth_grant","id":"fixture"},"container":{"kind":"channel","id":"C1","provider_namespace":id}});
        let item:Uuid=sqlx::query_scalar("INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id,origin,access_state) VALUES($1,'slack',$2,$2,$3,$4) RETURNING source_item_id").bind(graph).bind(id).bind(&origin).bind(access).fetch_one(&self.owner).await.unwrap();
        let version:Uuid=sqlx::query_scalar("INSERT INTO source_versions(graph_id,source_item_id,source_id,source_type,provider_namespace,origin,raw_inline,source_metadata,captured_at,extraction_status) VALUES($1,$2,$3,'slack',$3,$4,'Atlas deploy evidence.',$5,'2026-02-01','done') RETURNING version_id").bind(graph).bind(item).bind(id).bind(origin).bind(json!({"title":"Atlas deploy capture"})).fetch_one(&self.owner).await.unwrap();
        sqlx::query("UPDATE source_items SET current_version_id=$2 WHERE source_item_id=$1")
            .bind(item)
            .bind(version)
            .execute(&self.owner)
            .await
            .unwrap();
        let atom:Uuid=sqlx::query_scalar("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,timestamp) VALUES($1,'slack',$2,'fixture','Atlas deploy evidence.',now()) RETURNING atom_id").bind(graph).bind(format!("{id}:chunk:1")).fetch_one(&self.owner).await.unwrap();
        sqlx::query("INSERT INTO source_version_chunks(graph_id,source_version_id,atom_id) VALUES($1,$2,$3)").bind(graph).bind(version).bind(atom).execute(&self.owner).await.unwrap();
        sqlx::query("INSERT INTO node_atoms(node_id,atom_id) VALUES($1,$2)")
            .bind(node)
            .bind(atom)
            .execute(&self.owner)
            .await
            .unwrap();
        let outcome:Uuid=sqlx::query_scalar("INSERT INTO semantic_atoms(graph_id,source_version_id,ordinal,kind,statement,extraction_version,model,entities) VALUES($1,$2,0,'fact','Atlas deploy evidence.','source-atoms-v1','fixture','[{\"mention\":\"Atlas\",\"type\":\"project\"}]') RETURNING semantic_atom_id").bind(graph).bind(version).fetch_one(&self.owner).await.unwrap();
        sqlx::query(
            "INSERT INTO node_semantic_atoms(graph_id,node_id,semantic_atom_id) VALUES($1,$2,$3)",
        )
        .bind(graph)
        .bind(node)
        .bind(outcome)
        .execute(&self.owner)
        .await
        .unwrap();
        (item, version, atom)
    }
    async fn pair(&self, app: bool, extra: Value, query: &str, expected_path: &str) -> Value {
        let mut shape = json!({"graph_ids":[self.graph]});
        shape
            .as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        let mut scope: LibraryScope = serde_json::from_value(shape).unwrap();
        scope.normalize().unwrap();
        let t = self.tenant(app);
        let mut tx = t.begin_member_repeatable_read(self.account).await.unwrap();
        library_read::configure_read_model_proof_tx(&mut tx)
            .await
            .unwrap();
        let mut previous: Option<Value> = None;
        for _ in 0..3 {
            let after = std::array::from_fn(|i| {
                previous
                    .as_ref()
                    .and_then(|v| v[["sources", "outcomes", "nodes"][i]]["next_cursor"].as_str())
            });
            let pair = library_read::compare_read_model_tx(&mut tx, &scope, query, 2, after)
                .await
                .unwrap();
            assert_eq!(
                pair.path, expected_path,
                "reason {:?}",
                pair.fallback_reason
            );
            assert_eq!(
                pair.live, pair.candidate,
                "plane={} app={app} extra={extra} query={query}",
                self.tenant_plane
            );
            previous = Some(pair.live);
        }
        tx.rollback().await.unwrap();
        previous.unwrap()
    }
    async fn close(mut self) {
        self.app.close().await;
        self.owner.close().await;
        self.admin
            .execute(format!("DROP DATABASE {} WITH(FORCE)", self.name).as_str())
            .await
            .unwrap();
    }
}


#[tokio::test]
async fn candidate_program_errors_do_not_fall_through_and_server_stages_share_one_wall() {
 // All sixteen existing bind types are parsed even for controlled responses.
 let bindings="WITH args AS MATERIALIZED (SELECT $1::uuid[],$2::jsonb,$3::uuid,$4::uuid,$5::uuid,$6::text,$7::text,$8::timestamptz,$9::uuid,$10::timestamptz,$11::uuid,$12::bigint,$13::bigint,$14::uuid,$15::bigint,$16::timestamptz)";
 let groups=json!({"sources":{"items":[],"total":0},"outcomes":{"items":[],"total":0},"nodes":{"items":[],"total":0}});
 for plane in [false,true] {for app in [false,true] {
  let f=Fixture::new(plane).await;let tenant=f.tenant(app);let scope:LibraryScope=serde_json::from_value(json!({"graph_ids":[f.graph]})).unwrap();
  let needs=json!({"path":"live","value":null,"fallback_reason":"not_ready"});
  let live=format!("{bindings} SELECT '{groups}'::jsonb FROM pg_sleep(0.06)");
  let model=format!("{bindings} SELECT '{needs}'::jsonb FROM pg_sleep(0.06)");
  let mut tx=library_read::begin_model_search_tx_for_test(&tenant,f.account).await.unwrap();
  let metadata="SELECT jsonb_build_object('backend_pid',pg_backend_pid(),'role',current_user,'session_role',session_user,'account',current_setting('app.account_id'),'user',current_setting('app.user_id',true),'isolation',current_setting('transaction_isolation'),'read_only',current_setting('transaction_read_only'),'statement_timeout',current_setting('statement_timeout'),'plan_cache_mode',current_setting('plan_cache_mode'),'jit',current_setting('jit'),'work_mem',current_setting('work_mem'),'parallel',current_setting('max_parallel_workers_per_gather'),'row_security',current_setting('row_security'))";
  let mut before:Value=sqlx::query_scalar(metadata).fetch_one(&mut *tx).await.unwrap();
  let started=std::time::Instant::now();let error=library_read::candidate_program_sql_for_test(&mut tx,&scope,std::time::Duration::from_millis(100),model,live.clone()).await.err().expect("two60ms stages cannot share100ms");
  assert!(error.is_timeout());assert!(started.elapsed()<std::time::Duration::from_millis(500));
  // A dropped timed-out fetch may leave its server57014 for rollback to read.
  // This is NOT an ACK: production proof handling settles it Interrupted.
  let cleanup=match library_read::rollback_read_model_tx(tx).await {
   Ok(())=>"acknowledged",
   Err(library_read::CandidateProgramError::Sql(ref e)) if e.as_database_error().and_then(|e|e.code()).as_deref()==Some("57014")=>"unacknowledged_pending57014",
   Err(error)=>panic!("unexpected cleanup failure: {error}"),
  };
  let mut fresh=library_read::begin_model_search_tx_for_test(&tenant,f.account).await.unwrap();
  let mut after:Value=sqlx::query_scalar(metadata).fetch_one(&mut *fresh).await.unwrap();
  println!("timeout_cleanup={cleanup} plane={plane} app={app} before={before} fresh={after}");
  before.as_object_mut().unwrap().remove("backend_pid");after.as_object_mut().unwrap().remove("backend_pid");assert_eq!(before,after);
  assert_eq!(after["isolation"],"repeatable read");assert_eq!(after["read_only"],"on");assert_eq!(after["statement_timeout"],"12s");assert_eq!(after["account"],f.account.to_string());
  let healthy:i32=sqlx::query_scalar("SELECT 1").fetch_one(&mut *fresh).await.unwrap();assert_eq!(healthy,1);library_read::rollback_read_model_tx(fresh).await.unwrap();
  for malformed in ["jsonb_build_object('path','live','value',null,'fallback_reason','unknown')","jsonb_build_object('path','model','value',null,'fallback_reason',null)","to_jsonb(1/0)"] {
   let mut tx=library_read::begin_model_search_tx_for_test(&tenant,f.account).await.unwrap();let model=format!("{bindings} SELECT {malformed}");
   // Live would abort the transaction if incorrectly dispatched.
   let error=library_read::candidate_program_sql_for_test(&mut tx,&scope,std::time::Duration::from_secs(1),model,format!("{bindings} SELECT to_jsonb(1::integer/0)")).await.err().unwrap();
   if malformed=="to_jsonb(1/0)" {assert!(matches!(error,library_read::CandidateProgramError::Sql(ref e) if e.as_database_error().and_then(|e|e.code()).as_deref()==Some("22012")));}
   else {assert!(matches!(error,library_read::CandidateProgramError::InvalidAttempt));let current:String=sqlx::query_scalar("SHOW transaction_isolation").fetch_one(&mut *tx).await.unwrap();assert_eq!(current,"repeatable read");}
   library_read::rollback_read_model_tx(tx).await.unwrap();
  }
  f.close().await;
 }}
}

/// Stage1 records not_ready, then a real writer commits before unchanged Live.
/// The served opener must pin one RO/RR snapshot, so neither permission loss,
/// later cache publication nor changed source content can change stage2's view.
#[tokio::test]
async fn model_program_fallback_uses_actual_served_rr_snapshot_across_permission_and_drain() {
 for plane in [false,true] {for app in [false,true] {
  let f=Fixture::new(plane).await;let node=f.node(f.graph,"Atlas snapshot").await;
  let (item,version,_)=f.source(node,"snapshot-source","permitted","message").await;
  let tenant=f.tenant(app);let scope:LibraryScope=serde_json::from_value(json!({"graph_ids":[f.graph]})).unwrap();
  let mut tx=library_read::begin_model_search_tx_for_test(&tenant,f.account).await.unwrap();
  let before:(String,String,String,String,String)=sqlx::query_as("SELECT current_setting('transaction_isolation'),current_setting('transaction_read_only'),current_user,current_setting('app.account_id'),current_setting('statement_timeout')").fetch_one(&mut *tx).await.unwrap();
  assert_eq!((&before.0,&before.1),(&"repeatable read".to_owned(),&"on".to_owned()));assert_eq!(before.4,"12s");
  let pair=library_read::candidate_program_interleaved_for_test(&mut tx,&scope,"",||async{
   sqlx::query("UPDATE source_items SET access_state='restricted' WHERE source_item_id=$1").bind(item).execute(&f.owner).await.unwrap();
   sqlx::query("UPDATE source_versions SET source_metadata=jsonb_build_object('title','Changed after snapshot') WHERE version_id=$1").bind(version).execute(&f.owner).await.unwrap();
   // Publishes a real cache and drains its actual mutations after Stage1.
   f.refresh(app).await;
  }).await.unwrap();
  assert_eq!(pair["path"],"live");assert_eq!(pair["fallback_reason"],"not_ready");
  assert_eq!(pair["value"]["sources"]["total"],1);assert_eq!(pair["value"]["outcomes"]["total"],1);
  assert_eq!(pair["value"]["sources"]["items"][0]["title"],"Atlas deploy capture");
  let after:(String,String,String,String,String)=sqlx::query_as("SELECT current_setting('transaction_isolation'),current_setting('transaction_read_only'),current_user,current_setting('app.account_id'),current_setting('statement_timeout')").fetch_one(&mut *tx).await.unwrap();assert_eq!(before,after);
  tx.rollback().await.unwrap();
  // A NEW request sees the committed permission loss; old evidence wasn't
  // obtained by bypassing RLS or reading an earlier comparator oracle.
  let mut fresh=library_read::begin_model_search_tx_for_test(&tenant,f.account).await.unwrap();
  let observed=library_read::candidate_program_interleaved_for_test(&mut fresh,&scope,"",||async{panic!("new ready cache must stay Model")}).await.unwrap();
  assert_eq!(observed["path"],"model");assert_eq!(observed["value"]["sources"]["total"],0);assert_eq!(observed["value"]["outcomes"]["total"],0);fresh.rollback().await.unwrap();
  f.close().await;
 }}
}

#[tokio::test]
async fn selector_membership_reuse_keeps_unresolved_current_versions_and_empty_source_scopes_exact() {
 for plane in [false,true] {for app in [false,true] {
  let f=Fixture::new(plane).await;let node=f.node(f.graph,"Atlas unresolved").await;f.refresh(app).await;
  async fn check(f:&Fixture,app:bool,mode:&str,extra:Value,sources:i64,outcomes:i64) {
   let mut shape=json!({"graph_ids":[f.graph],"outcome_versions":mode});shape.as_object_mut().unwrap().extend(extra.as_object().unwrap().clone());
   let scope:LibraryScope=serde_json::from_value(shape).unwrap();let tenant=f.tenant(app);let mut tx=tenant.begin_member_repeatable_read(f.account).await.unwrap();library_read::configure_read_model_proof_tx(&mut tx).await.unwrap();
   let pair=library_read::compare_read_model_tx(&mut tx,&scope,"",40,[None;3]).await.unwrap();assert_eq!(pair.path,"model","reason={:?}",pair.fallback_reason);assert_eq!(pair.live,pair.candidate);
   assert_eq!(pair.candidate["sources"]["total"],json!(sources));assert_eq!(pair.candidate["outcomes"]["total"],json!(outcomes));
   if outcomes>0 {assert!(pair.candidate["outcomes"]["items"].as_array().unwrap().iter().all(|r|r["source_version_is_current"]==json!(false)));}
   tx.rollback().await.unwrap();
  }
  // No admitted sources: the independent key-producer demand gate is false.
  for mode in ["all_retained","current"]{check(&f,app,mode,json!({}),0,0).await;}
  let (item,_,_)=f.source(node,"unresolved-current","permitted","message").await;
  // A real nullable captured pointer, with an older retained linked fact. Do
  // not fabricate an impossible FK mismatch to represent an unresolved row.
  sqlx::query("UPDATE source_items SET current_version_id=NULL WHERE source_item_id=$1").bind(item).execute(&f.owner).await.unwrap();f.refresh(app).await;
  check(&f,app,"current",json!({}),1,0).await;check(&f,app,"all_retained",json!({}),1,1).await;
  for mode in ["all_retained","current"]{check(&f,app,mode,json!({"collection":"meetings"}),0,0).await;}
  f.close().await;
 }}
}

fn selector_plan_walk<'a>(node:&'a Value,out:&mut Vec<&'a Value>) {
 out.push(node);if let Some(children)=node["Plans"].as_array(){for child in children{selector_plan_walk(child,out)}}
}
fn assert_listing_clean_physical_plan(plan:&Value,page_limit:u64) {
 let mut nodes=Vec::new();selector_plan_walk(&plan[0]["Plan"],&mut nodes);
 let active=|n:&&Value|n["Actual Loops"].as_f64().unwrap_or(0.0)>0.0;
 let candidate:Vec<_>=nodes.iter().copied().filter(|n|n["Subplan Name"]=="CTE model_candidate_result").filter(active).collect();assert_eq!(candidate.len(),1);
 let mut body=Vec::new();selector_plan_walk(candidate[0],&mut body);
 assert!(!body.iter().any(|n|n["Subplan Name"]=="CTE scope_graph_outcome_keys"||n["Subplan Name"]=="CTE scope_served_versions"),"clean reader must not rebuild the global raw selector");
 let rank:Vec<_>=body.iter().copied().filter(|n|n["Relation Name"]=="library_listing_outcomes").filter(active).collect();assert!(!rank.is_empty(),"active maintained outcome rank input");
 let raw:Vec<_>=body.iter().copied().filter(|n|n["Relation Name"]=="semantic_atoms").filter(active).collect();assert!(!raw.is_empty(),"page payload is still authoritative");
 for leaf in raw {assert!(leaf["Actual Rows"].as_f64().unwrap()*leaf["Actual Loops"].as_f64().unwrap()<=page_limit as f64,"semantic payload lookup must follow the bounded page, not scan the full graph");}
}

#[tokio::test]
async fn definition_three_served_memberships_are_maintained_and_clean_payload_is_page_bounded_under_app_rls() {
 for plane in [false,true] {
  let f=Fixture::new(plane).await;f.node(f.graph,"Selector fixture anchor").await;
  let mut retained=Vec::new();let mut current=Vec::new();
  for index in 0..16 {
   let item=Uuid::new_v4();let old=Uuid::new_v4();payload_source(&f,item,old,&format!("selector-{index}"),&format!("Capture {index}"),"meeting","2026-02-01T00:00:00Z",None).await;
   sqlx::query("UPDATE source_versions SET content_sha256='selector-same-content' WHERE version_id=$1").bind(old).execute(&f.owner).await.unwrap();
   let mut old_ids=Vec::new();for ordinal in 0..2 {let id=Uuid::new_v4();payload_outcome(&f,id,old,ordinal,"Selector old unlinked fact",&[]).await;old_ids.push(id);retained.push(id);}
   let next:Uuid=sqlx::query_scalar("INSERT INTO source_versions(graph_id,source_item_id,source_id,source_type,provider_namespace,origin,raw_inline,source_metadata,captured_at,content_sha256,extraction_status) SELECT graph_id,source_item_id,source_id,source_type,provider_namespace,origin,raw_inline,source_metadata,'2026-03-01',content_sha256,$2 FROM source_versions WHERE version_id=$1 RETURNING version_id").bind(old).bind(if index%4<2{"pending"}else{"done"}).fetch_one(&f.owner).await.unwrap();
   sqlx::query("UPDATE source_items SET current_version_id=$2 WHERE source_item_id=$1").bind(item).bind(next).execute(&f.owner).await.unwrap();
   if index%4==0||index%4==2 {let id=Uuid::new_v4();payload_outcome(&f,id,next,0,"Selector current unlinked fact",&[]).await;retained.push(id);if index%4==2{current.push(id);}}
   if index%4!=2{current.extend(old_ids);}
  }
  assert_eq!(retained.len(),40);assert_eq!(current.len(),28);
  for app in [false,true] {f.refresh(app).await;for mode in ["all_retained","current"] {
   let scope:LibraryScope=serde_json::from_value(json!({"graph_ids":[f.graph],"outcome_versions":mode})).unwrap();let tenant=f.tenant(app);let mut tx=tenant.begin_member_repeatable_read(f.account).await.unwrap();library_read::configure_read_model_proof_tx(&mut tx).await.unwrap();
   // Logical NULL/composite/duplicate counterexamples use VALUES rather than
   // disabling real schema constraints or manufacturing corrupt fixture rows.
   let truth:bool=sqlx::query_scalar("WITH keys(g,v) AS(VALUES($1::uuid,$2::uuid),($2,$1),(NULL::uuid,$1),($1,$2)), probes(g,v) AS(VALUES($1::uuid,$1::uuid),($1,$2),(NULL::uuid,$1),($1,NULL::uuid)) SELECT bool_and(old_positive=new_positive AND (NOT old_positive)=(NOT new_positive)) FROM (SELECT EXISTS(SELECT 1 FROM keys k WHERE k.g=p.g AND k.v=p.v) old_positive,COALESCE((p.g,p.v) IN(SELECT k.g,k.v FROM keys k),false) new_positive FROM probes p) x").bind(f.graph).bind(f.foreign).fetch_one(&mut *tx).await.unwrap();assert!(truth);
   let pair=library_read::compare_read_model_tx(&mut tx,&scope,"",40,[None;3]).await.unwrap();assert_eq!(pair.path,"model","reason={:?}",pair.fallback_reason);assert_eq!(pair.live,pair.candidate);
   assert_eq!(pair.candidate["sources"]["total"],json!(16));let mut actual:Vec<Uuid>=pair.candidate["outcomes"]["items"].as_array().unwrap().iter().map(|r|serde_json::from_value(r["semantic_atom_id"].clone()).unwrap()).collect();actual.sort();let mut expected=if mode=="current"{current.clone()}else{retained.clone()};expected.sort();assert_eq!(actual,expected);assert_eq!(pair.candidate["outcomes"]["total"],json!(expected.len()));
   if app {
    let metadata:Value=sqlx::query_scalar("SELECT jsonb_build_object('role',current_user,'semantic_rls_active',row_security_active('semantic_atoms'),'version',version(),'work_mem',current_setting('work_mem'),'plan_cache_mode',current_setting('plan_cache_mode'))").fetch_one(&mut *tx).await.unwrap();assert_eq!(metadata["role"],json!(if plane{"coralswarm_tenant_app"}else{"coralswarm_app"}));assert_eq!(metadata["semantic_rls_active"],json!(true));
    let sql=format!("EXPLAIN(ANALYZE,VERBOSE,BUFFERS,FORMAT JSON) {}",library_read::read_model_sql_for_test(&scope));
    let plan:Value=sqlx::query_scalar(&sql).bind(&scope.graph_ids).bind(json!({})).bind(None::<Uuid>).bind(None::<Uuid>).bind(None::<Uuid>).bind(None::<String>).bind(None::<String>).bind(None::<chrono::DateTime<chrono::Utc>>).bind(None::<Uuid>).bind(None::<chrono::DateTime<chrono::Utc>>).bind(None::<Uuid>).bind(None::<i64>).bind(None::<i64>).bind(None::<Uuid>).bind(3_i64).bind(None::<chrono::DateTime<chrono::Utc>>).fetch_one(&mut *tx).await.unwrap();
    // Capture before asserting so an unfavorable normal-plan choice survives.
    if let Ok(dir)=std::env::var("LIBRARY_SELECTOR_LOCAL_PLAN_DIR") {use std::io::Write;let path=std::path::Path::new(&dir).join(format!("selector-{}-{mode}.json",if plane{"tenant"}else{"consumer"}));let mut file=std::fs::OpenOptions::new().write(true).create_new(true).open(path).unwrap();file.write_all(serde_json::to_vec_pretty(&json!({"source_sha":env!("CORALSWARM_GIT_SHA_FULL"),"metadata":metadata,"known_graph_keys":40,"plan":plan})).unwrap().as_slice()).unwrap();}
    assert_listing_clean_physical_plan(&plan,3);
   }
   tx.rollback().await.unwrap();
  }}
  f.close().await;
 }
}

#[tokio::test]
async fn deferred_outcome_payload_preserves_source_wide_counts_unlinked_facts_and_version_metadata() {
 async fn check(f:&Fixture,app:bool,mode:&str,filter:Value,query:&str,sources:&[(Uuid,i64)],outcomes:&[Uuid])->Value {
  let mut shape=json!({"graph_ids":[f.graph],"outcome_versions":mode});shape.as_object_mut().unwrap().extend(filter.as_object().unwrap().clone());
  let scope:LibraryScope=serde_json::from_value(shape).unwrap();let tenant=f.tenant(app);let mut tx=tenant.begin_member_repeatable_read(f.account).await.unwrap();library_read::configure_read_model_proof_tx(&mut tx).await.unwrap();
  let pair=library_read::compare_read_model_tx(&mut tx,&scope,query,40,[None;3]).await.unwrap();assert_eq!(pair.path,"model","plane={} app={app} mode={mode} query={query} reason={:?}",f.tenant_plane,pair.fallback_reason);assert_eq!(pair.live,pair.candidate);
  let value=pair.candidate;let mut actual:Vec<(Uuid,i64)>=value["sources"]["items"].as_array().unwrap().iter().map(|s|(serde_json::from_value(s["source_item_id"].clone()).unwrap(),s["semantic_atom_count"].as_i64().unwrap())).collect();actual.sort();let mut expected=sources.to_vec();expected.sort();assert_eq!(actual,expected);assert_eq!(value["sources"]["total"],json!(sources.len()));
  let mut actual:Vec<Uuid>=value["outcomes"]["items"].as_array().unwrap().iter().map(|a|serde_json::from_value(a["semantic_atom_id"].clone()).unwrap()).collect();actual.sort();let mut expected=outcomes.to_vec();expected.sort();assert_eq!(actual,expected);assert_eq!(value["outcomes"]["total"],json!(outcomes.len()));
  if !query.is_empty(){assert_eq!(value["nodes"]["total"],json!(0));}
  tx.rollback().await.unwrap();value
 }
 for plane in [false,true] {for app in [false,true] {
  let f=Fixture::new(plane).await;let x=f.node(f.graph,"Entity X").await;let y=f.node(f.graph,"Entity Y").await;let z=f.node(f.graph,"Entity Z").await;
  let a=Uuid::new_v4();let b=Uuid::new_v4();let e=Uuid::new_v4();let old=Uuid::new_v4();let bv=Uuid::new_v4();let ev=Uuid::new_v4();
  payload_source(&f,a,old,"identityonly","old capture","meeting","2026-02-01T00:00:00Z",None).await;payload_source(&f,b,bv,"source-b","other capture","work_session","2026-02-02T00:00:00Z",None).await;payload_source(&f,e,ev,"source-e","zeroheading","meeting","2026-02-03T00:00:00Z",None).await;
  let ids:[Uuid;5]=std::array::from_fn(|_|Uuid::new_v4());
  for (index,(version,statement,links)) in [(old,"statementneedle ø",vec![x,y]),(old,"plain second fact",vec![x]),(old,"orphanneedle 漢字語",vec![]),(bv,"other first fact",vec![y]),(bv,"other second fact",vec![z])].into_iter().enumerate(){payload_outcome(&f,ids[index],version,index as i32,statement,&links).await;}
  sqlx::query("INSERT INTO semantic_atom_passages(graph_id,semantic_atom_id,source_version_id,start_char,end_char,quote) VALUES($1,$2,$3,0,16,'passageneedle éé')").bind(f.graph).bind(ids[1]).bind(old).execute(&f.owner).await.unwrap();
  sqlx::query("UPDATE source_versions SET content_sha256='same-content',origin=jsonb_set(origin,'{content_kind}',to_jsonb('work_session'::text)),source_metadata=$2 WHERE version_id=$1").bind(old).bind(json!({"title":"old capture","independence":"derived_copy"})).execute(&f.owner).await.unwrap();
  let current:Uuid=sqlx::query_scalar("INSERT INTO source_versions(graph_id,source_item_id,source_id,source_type,provider_namespace,origin,raw_inline,source_metadata,captured_at,content_sha256,extraction_status) SELECT graph_id,source_item_id,source_id,source_type,provider_namespace,jsonb_set(origin,'{content_kind}',to_jsonb('meeting'::text)),raw_inline,$2,'2026-03-01','same-content','pending' FROM source_versions WHERE version_id=$1 RETURNING version_id").bind(old).bind(json!({"title":"headingneedle"})).fetch_one(&f.owner).await.unwrap();
  sqlx::query("UPDATE source_items SET current_version_id=$2 WHERE source_item_id=$1").bind(a).bind(current).execute(&f.owner).await.unwrap();f.refresh(app).await;
  for mode in ["all_retained","current"] {for filter in [json!({}),json!({"service_id":"slack"})] {for (query,expected_sources,expected_outcomes) in [("",vec![(a,3),(b,2),(e,0)],ids.to_vec()),("ø",vec![(a,3)],vec![ids[0]]),("statementneedle",vec![(a,3)],vec![ids[0]])] {
   let value=check(&f,app,mode,filter.clone(),query,&expected_sources,&expected_outcomes).await;
   if query.is_empty(){let source=value["sources"]["items"].as_array().unwrap().iter().find(|s|s["source_item_id"]==json!(a)).unwrap();assert_eq!(source["selected_version_id"],json!(current));assert_eq!(source["title"],json!("headingneedle"));for row in value["outcomes"]["items"].as_array().unwrap().iter().filter(|r|r["source_item_id"]==json!(a)){assert_eq!(row["source_version_id"],json!(old));assert_eq!(row["origin"]["content_kind"],json!("work_session"));assert_eq!(row["source_independence"],json!("unknown"));assert_eq!(row["source_version_is_current"],json!(true));assert!(row["captured_at"].as_str().unwrap().starts_with("2026-02-01"));}}
  }}}
  for (query,expected_sources,expected_outcomes) in [("passageneedle",vec![(a,3)],vec![ids[1]]),("éé",vec![(a,3)],vec![ids[1]]),("漢字語",vec![(a,3)],vec![ids[2]]),("orphanneedle",vec![(a,3)],vec![ids[2]]),("headingneedle",vec![(a,3)],ids[..3].to_vec()),("identityonly",vec![(a,3)],vec![]),("zeroheading",vec![(e,0)],vec![]),("no-match-sentinel",vec![],vec![])] {check(&f,app,"all_retained",json!({}),query,&expected_sources,&expected_outcomes).await;}
  for mode in ["all_retained","current"]{check(&f,app,mode,json!({"collection":"meetings"}),"",&[(a,3),(e,0)],&ids[..3]).await;}
  let orphan=check(&f,app,"all_retained",json!({}),"orphanneedle",&[(a,3)],&[ids[2]]).await;assert_eq!(orphan["outcomes"]["items"][0]["nodes"],json!([]));assert_eq!(orphan["outcomes"]["items"][0]["nodes_truncated"],json!(false));
  sqlx::query("DELETE FROM node_semantic_atoms WHERE graph_id=$1 AND semantic_atom_id=ANY($2)").bind(f.graph).bind(&ids[..2]).execute(&f.owner).await.unwrap();
  for drained in [false,true]{if drained{f.refresh(app).await;}let value=check(&f,app,"current",json!({}),"",&[(a,3),(b,2),(e,0)],&ids).await;let mut actual:Vec<Uuid>=value["nodes"]["items"].as_array().unwrap().iter().map(|n|serde_json::from_value(n["node_id"].clone()).unwrap()).collect();actual.sort();let mut expected=vec![y,z];expected.sort();assert_eq!(actual,expected);}
  sqlx::query("UPDATE semantic_atoms SET statement='updatedorphanneedle' WHERE semantic_atom_id=$1").bind(ids[2]).execute(&f.owner).await.unwrap();
  for drained in [false,true]{if drained{f.refresh(app).await;}let value=check(&f,app,"current",json!({}),"updatedorphanneedle",&[(a,3)],&[ids[2]]).await;assert_eq!(value["outcomes"]["items"][0]["statement"],json!("updatedorphanneedle"));}
  sqlx::query("UPDATE source_versions SET content_sha256='different-content',raw_inline='changed capture' WHERE version_id=$1").bind(current).execute(&f.owner).await.unwrap();
  check(&f,app,"current",json!({}),"",&[(a,0),(b,2),(e,0)],&ids[3..]).await;check(&f,app,"all_retained",json!({}),"",&[(a,3),(b,2),(e,0)],&ids).await;
  let added=Uuid::new_v4();payload_outcome(&f,added,current,0,"new current fact",&[]).await;sqlx::query("UPDATE source_versions SET extraction_status='done' WHERE version_id=$1").bind(current).execute(&f.owner).await.unwrap();
  for drained in [false,true]{if drained{f.refresh(app).await;}let value=check(&f,app,"current",json!({}),"",&[(a,1),(b,2),(e,0)],&[added,ids[3],ids[4]]).await;let row=value["outcomes"]["items"].as_array().unwrap().iter().find(|r|r["semantic_atom_id"]==json!(added)).unwrap();assert_eq!(row["source_version_id"],json!(current));assert_eq!(row["source_independence"],json!("captured_source_item"));let mut all=ids.to_vec();all.push(added);let retained=check(&f,app,"all_retained",json!({}),"",&[(a,4),(b,2),(e,0)],&all).await;assert!(retained["outcomes"]["items"].as_array().unwrap().iter().filter(|r|r["source_item_id"]==json!(a)&&r["semantic_atom_id"]!=json!(added)).all(|r|r["source_version_is_current"]==json!(false)));}
  f.close().await;
 }}
}

async fn payload_source(f:&Fixture,item:Uuid,version:Uuid,id:&str,title:&str,kind:&str,captured:&str,occurred:Option<&str>) {
 let origin=json!({"service_id":"slack","content_kind":kind,"provider_namespace":id,"connection":{"kind":"oauth_grant","id":"fixture"},"container":{"kind":"channel","id":"C1","provider_namespace":id}});
 sqlx::query("INSERT INTO source_items(source_item_id,graph_id,source_type,provider_namespace,source_id,origin,access_state) VALUES($1,$2,'slack',$3,$3,$4,'permitted')").bind(item).bind(f.graph).bind(id).bind(&origin).execute(&f.owner).await.unwrap();
 sqlx::query("INSERT INTO source_versions(version_id,graph_id,source_item_id,source_id,source_type,provider_namespace,origin,raw_inline,source_metadata,captured_at,occurred_at,extraction_status) VALUES($1,$2,$3,$4,'slack',$4,$5,'fixture captured text',$6,$7,$8,'done')").bind(version).bind(f.graph).bind(item).bind(id).bind(origin).bind(json!({"title":title})).bind(chrono::DateTime::parse_from_rfc3339(captured).unwrap().with_timezone(&chrono::Utc)).bind(occurred.map(|s|chrono::DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&chrono::Utc))).execute(&f.owner).await.unwrap();
 sqlx::query("UPDATE source_items SET current_version_id=$2 WHERE source_item_id=$1").bind(item).bind(version).execute(&f.owner).await.unwrap();
}
async fn payload_outcome(f:&Fixture,id:Uuid,version:Uuid,ordinal:i32,statement:&str,links:&[Uuid]) {
 let mut entities=Vec::new();for node in links {let title:String=sqlx::query_scalar("SELECT title FROM nodes WHERE graph_id=$1 AND node_id=$2").bind(f.graph).bind(node).fetch_one(&f.owner).await.unwrap();entities.push(json!({"mention":title,"type":"person"}));}
 sqlx::query("INSERT INTO semantic_atoms(semantic_atom_id,graph_id,source_version_id,ordinal,kind,statement,extraction_version,model,entities) VALUES($1,$2,$3,$4,'fact',$5,'source-atoms-v1','fixture',$6)").bind(id).bind(f.graph).bind(version).bind(ordinal).bind(statement).bind(json!(entities)).execute(&f.owner).await.unwrap();
 sqlx::query("INSERT INTO node_semantic_atoms(graph_id,node_id,semantic_atom_id) SELECT $1,n,$3 FROM unnest($2::uuid[]) n").bind(f.graph).bind(links).bind(id).execute(&f.owner).await.unwrap();
}

#[tokio::test]
async fn deferred_page_payload_preserves_independent_cursor_orders_and_terminal_pages() {
 for plane in [false,true] {for app in [false,true] {
  let f=Fixture::new(plane).await;let sources:Vec<Uuid>=(1..=5).map(|n|Uuid::from_u128(1000+n)).collect();let versions:Vec<Uuid>=(1..=5).map(|n|Uuid::from_u128(2000+n)).collect();let outcomes:Vec<Uuid>=(1..=6).map(|n|Uuid::from_u128(3000+n)).collect();let nodes:Vec<Uuid>=(1..=5).map(|n|Uuid::from_u128(4000+n)).collect();
  for index in 0..5 {let captured=["2026-03-03T00:00:00Z","2026-03-02T00:00:00Z","2026-03-04T00:00:00Z","2026-03-05T00:00:00Z","2026-03-05T00:00:00Z"][index];let occurred=[Some("2026-04-03T00:00:00Z"),Some("2026-04-03T00:00:00Z"),Some("2026-04-02T00:00:00Z"),None,None][index];payload_source(&f,sources[index],versions[index],&format!("cursor-{index}"),&format!("Capture {index}"),"message",captured,occurred).await;sqlx::query("INSERT INTO nodes(node_id,graph_id,type,title,body) VALUES($1,$2,'entity',$3,'{}')").bind(nodes[index]).bind(f.graph).bind(format!("Cursor entity {index}")).execute(&f.owner).await.unwrap();}
  for index in 0..6 {let version=versions[[0,0,1,2,3,4][index]];let links=if index<5{vec![nodes[index]]}else{vec![]};payload_outcome(&f,outcomes[index],version,index as i32,&format!("Cursor fact {index}"),&links).await;}
  f.refresh(app).await;
  for mode in ["all_retained","current"] {
   let scope:LibraryScope=serde_json::from_value(json!({"graph_ids":[f.graph],"outcome_versions":mode})).unwrap();let tenant=f.tenant(app);let mut tx=tenant.begin_member_repeatable_read(f.account).await.unwrap();library_read::configure_read_model_proof_tx(&mut tx).await.unwrap();
   let mut after:[Option<String>;3]=[None,None,None];let mut done=[false;3];let mut seen:[Vec<Uuid>;3]=std::array::from_fn(|_|Vec::new());let mut lengths:[Vec<usize>;3]=std::array::from_fn(|_|Vec::new());
   for _ in 0..4 {
    let pair=library_read::compare_read_model_tx(&mut tx,&scope,"",2,std::array::from_fn(|i|after[i].as_deref())).await.unwrap();assert_eq!(pair.path,"model","reason {:?}",pair.fallback_reason);assert_eq!(pair.live,pair.candidate);
    for (index,(group,id_field,total)) in [("sources","source_item_id",5),("outcomes","semantic_atom_id",6),("nodes","node_id",5)].into_iter().enumerate(){assert_eq!(pair.candidate[group]["total"],json!(total));if done[index]{continue}let rows=pair.candidate[group]["items"].as_array().unwrap();lengths[index].push(rows.len());for row in rows{let id:Uuid=serde_json::from_value(row[id_field].clone()).unwrap();assert!(!seen[index].contains(&id));seen[index].push(id);
     if group=="sources"{assert_eq!(row["semantic_atom_count"],json!(if id==sources[0]{2}else{1}));}
     if group=="outcomes"{let i=outcomes.iter().position(|x|*x==id).unwrap();assert_eq!(row["statement"],json!(format!("Cursor fact {i}")));assert_eq!(row["source_version_id"],json!(versions[[0,0,1,2,3,4][i]]));if i==5{assert_eq!(row["nodes"],json!([]));}}
    }let next=pair.candidate[group]["next_cursor"].as_str().map(str::to_owned);if let Some(next)=next{assert_eq!(rows.len(),2);assert_ne!(after[index].as_deref(),Some(next.as_str()));after[index]=Some(next);}else{done[index]=true;}}
    if done.iter().all(|x|*x){break}
   }
   assert!(done.iter().all(|x|*x));assert_eq!(seen[0],vec![sources[4],sources[3],sources[2],sources[0],sources[1]]);assert_eq!(seen[1],vec![outcomes[1],outcomes[0],outcomes[2],outcomes[3],outcomes[5],outcomes[4]]);assert_eq!(seen[2],nodes.iter().rev().copied().collect::<Vec<_>>());assert_eq!(lengths,[vec![2,2,1],vec![2,2,2],vec![2,2,1]]);
   // Explicit local diagnostic only, after semantic checks. Preserve the
   // actual normal-plan choice and heap work, including an unfavorable plan.
   if plane&&app&&mode=="current" {if let Ok(path)=std::env::var("LIBRARY_PAYLOAD_LOCAL_PLAN") {
    let metadata:Value=sqlx::query_scalar("SELECT jsonb_build_object('role',current_user,'semantic_rls_active',row_security_active('semantic_atoms'),'version',version(),'work_mem',current_setting('work_mem'),'plan_cache_mode',current_setting('plan_cache_mode'))").fetch_one(&mut *tx).await.unwrap();
    let sql=format!("EXPLAIN(ANALYZE,BUFFERS,FORMAT JSON) {}",library_read::read_model_sql_for_test(&scope));
    let plan:Value=sqlx::query_scalar(&sql).bind(&scope.graph_ids).bind(json!({})).bind(None::<Uuid>).bind(None::<Uuid>).bind(None::<Uuid>).bind(None::<String>).bind(None::<String>).bind(None::<chrono::DateTime<chrono::Utc>>).bind(None::<Uuid>).bind(None::<chrono::DateTime<chrono::Utc>>).bind(None::<Uuid>).bind(None::<i64>).bind(None::<i64>).bind(None::<Uuid>).bind(3_i64).bind(None::<chrono::DateTime<chrono::Utc>>).fetch_one(&mut *tx).await.unwrap();
    use std::io::Write;let mut output=std::fs::OpenOptions::new().write(true).create_new(true).open(path).unwrap();output.write_all(serde_json::to_vec_pretty(&json!({"source_sha":env!("CORALSWARM_GIT_SHA_FULL"),"metadata":metadata,"known_outcome_rows":6,"requested_page_plus_one":3,"plan":plan})).unwrap().as_slice()).unwrap();
   }}
   tx.rollback().await.unwrap();
  }
  f.close().await;
 }}
}

#[tokio::test]
async fn clean_rank_prefix_preserves_multigraph_dirty_replacements_and_strict_cursor_pages() {
 async fn pages(f:&Fixture,app:bool,graphs:&[Uuid],expected:usize) {
  for mode in ["all_retained","current"] {
   // Do not normalize: repeated graph IDs must retain the canonical ANY
   // behavior even for direct callers of the statement/proof primitive.
   let scope:LibraryScope=serde_json::from_value(json!({"graph_ids":graphs,"outcome_versions":mode})).unwrap();
   let tenant=f.tenant(app);let mut tx=tenant.begin_member_repeatable_read(f.account).await.unwrap();library_read::configure_read_model_proof_tx(&mut tx).await.unwrap();
   let mut after:Option<String>=None;let mut seen=std::collections::HashSet::new();let mut finished=false;
   for _ in 0..16 {
    let pair=library_read::compare_read_model_tx(&mut tx,&scope,"",2,[None,None,after.as_deref()]).await.unwrap();
    assert_eq!(pair.path,"model","plane={} app={app} mode={mode} reason={:?}",f.tenant_plane,pair.fallback_reason);assert_eq!(pair.live,pair.candidate);
    let rows=pair.live["nodes"]["items"].as_array().unwrap();for row in rows {let id:Uuid=serde_json::from_value(row["node_id"].clone()).unwrap();assert!(seen.insert(id),"strict cursor repeated node");}
    let next=pair.live["nodes"]["next_cursor"].as_str().map(str::to_owned);
    if next.is_none(){finished=true;break}assert_eq!(rows.len(),2);assert_ne!(next,after);after=next;
   }
   assert!(finished,"rank pagination did not exhaust");assert_eq!(seen.len(),expected,"plane={} app={app} mode={mode}",f.tenant_plane);tx.rollback().await.unwrap();
  }
 }
 for plane in [false,true] {for app in [false,true] {
  let f=Fixture::new(plane).await;
  sqlx::query("UPDATE graphs SET owner_user_id=$2 WHERE id=$1").bind(f.foreign).bind(f.user).execute(&f.owner).await.unwrap();
  let empty:Uuid=sqlx::query_scalar("INSERT INTO graphs(account_id,name,kind,owner_user_id) VALUES($1,'Empty rank','member',$2) RETURNING id").bind(f.account).bind(f.user).fetch_one(&f.owner).await.unwrap();
  let mut groups=Vec::new();let mut sources=Vec::new();
  for (gindex,graph) in [f.graph,f.foreign].into_iter().enumerate() {
   let mut nodes=Vec::new();let mut items=Vec::new();
   for index in 0..6 {
    let title=if index==0{"Atlas".into()}else{format!("Rank {index}")};let node=f.node(graph,&title).await;nodes.push(node);let mut node_items=Vec::new();
    for source in 0..[2,2,1,1,0,0][index] {let facts=f.source_in_graph(graph,node,&format!("rank-{gindex}-{index}-{source}"),"permitted","meeting").await;node_items.push(facts);}
    items.push(node_items);
   }
   // An admitted zero-source legacy node belongs on node pages, even though
   // work_page's source-count/work predicates exclude it. The last node is
   // genuinely unsupported, and must remain absent until a dirty grant.
   let atom:Uuid=sqlx::query_scalar("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,timestamp) VALUES($1,'note','zero-rank','fixture','Rank 4 retained note.',now()) RETURNING atom_id").bind(graph).fetch_one(&f.owner).await.unwrap();sqlx::query("INSERT INTO node_atoms(node_id,atom_id) VALUES($1,$2)").bind(nodes[4]).bind(atom).execute(&f.owner).await.unwrap();
   // Retained/current secondary rank differs while source_count ties. Old
   // linked outcomes stay retained after a new linked current capture.
   let (item,old,_)=items[0][0];
   for ordinal in [1,2] {let outcome:Uuid=sqlx::query_scalar("INSERT INTO semantic_atoms(graph_id,source_version_id,ordinal,kind,statement,extraction_version,model) VALUES($1,$2,$3,'fact','Extra retained rank support.','source-atoms-v1','fixture') RETURNING semantic_atom_id").bind(graph).bind(old).bind(ordinal).fetch_one(&f.owner).await.unwrap();sqlx::query("INSERT INTO node_semantic_atoms(graph_id,node_id,semantic_atom_id) VALUES($1,$2,$3)").bind(graph).bind(nodes[0]).bind(outcome).execute(&f.owner).await.unwrap();}
   let current:Uuid=sqlx::query_scalar("INSERT INTO source_versions(graph_id,source_item_id,source_id,source_type,provider_namespace,origin,raw_inline,source_metadata,captured_at,extraction_status) SELECT graph_id,source_item_id,source_id,source_type,provider_namespace,origin,'New rank capture',source_metadata,'2026-03-01','done' FROM source_versions WHERE version_id=$1 RETURNING version_id").bind(old).fetch_one(&f.owner).await.unwrap();
   let outcome:Uuid=sqlx::query_scalar("INSERT INTO semantic_atoms(graph_id,source_version_id,ordinal,kind,statement,extraction_version,model) VALUES($1,$2,0,'fact','New current rank support.','source-atoms-v1','fixture') RETURNING semantic_atom_id").bind(graph).bind(current).fetch_one(&f.owner).await.unwrap();sqlx::query("INSERT INTO node_semantic_atoms(graph_id,node_id,semantic_atom_id) VALUES($1,$2,$3)").bind(graph).bind(nodes[0]).bind(outcome).execute(&f.owner).await.unwrap();sqlx::query("UPDATE source_items SET current_version_id=$2 WHERE source_item_id=$1").bind(item).bind(current).execute(&f.owner).await.unwrap();
   groups.push(nodes);sources.push(items);
  }
  for graph in [f.graph,f.foreign,empty] {f.refresh_graph(app,graph).await;}
  let rank:(i64,i64)=sqlx::query_as("SELECT atom_count_retained,atom_count_served FROM library_node_evidence WHERE graph_id=$1 AND node_id=$2").bind(f.graph).bind(groups[0][0]).fetch_one(&f.owner).await.unwrap();assert!(rank.0>rank.1,"fixture must exercise mode-specific rank");
  let graphs=[f.graph,f.foreign,f.graph,empty];pages(&f,app,&graphs,10).await;pages(&f,app,&[empty],0).await;
  // Existing clean top rows lose score/admission, a previously unsupported
  // node becomes eligible, and a low row gains a new top score. These dirty
  // replacements must be merged BEFORE the global page and strict cursor.
  sqlx::query("UPDATE source_items SET access_state='restricted' WHERE source_item_id=$1").bind(sources[0][0][0].0).execute(&f.owner).await.unwrap();
  for item in &sources[0][1] {sqlx::query("UPDATE source_items SET access_state='restricted' WHERE source_item_id=$1").bind(item.0).execute(&f.owner).await.unwrap();}
  f.source_in_graph(f.foreign,groups[1][5],"rank-newly-eligible","permitted","meeting").await;
  for index in 0..2 {f.source_in_graph(f.foreign,groups[1][3],&format!("rank-promoted-{index}"),"permitted","meeting").await;}
  pages(&f,app,&graphs,10).await;
  for graph in [f.graph,f.foreign] {f.refresh_graph(app,graph).await;}
  pages(&f,app,&graphs,10).await;
  // Physical deletion uses the existing historical-key admission fallback
  // until drain. The ready cache's FK deletion cannot leak a stale rank row.
  sqlx::query("DELETE FROM nodes WHERE node_id=$1").bind(groups[0][2]).execute(&f.owner).await.unwrap();
  let scope:LibraryScope=serde_json::from_value(json!({"graph_ids":graphs})).unwrap();let tenant=f.tenant(app);let mut tx=tenant.begin_member_repeatable_read(f.account).await.unwrap();library_read::configure_read_model_proof_tx(&mut tx).await.unwrap();let pair=library_read::compare_read_model_tx(&mut tx,&scope,"",2,[None;3]).await.unwrap();assert_eq!(pair.live,pair.candidate);tx.rollback().await.unwrap();
  f.refresh_graph(app,f.graph).await;pages(&f,app,&graphs,9).await;f.close().await;
 }}
}

#[tokio::test]
async fn work_rank_prefix_preserves_six_global_keys_across_clean_dirty_and_node_cursors() {
 // Expectations are fixture scores, never a query against the production cache.
 // Each graph has eight eligible tied rows, so local six and global six differ.
 type Score=(Uuid,i64,i64,i64,bool);
 async fn source_with_title(f:&Fixture,graph:Uuid,node:Uuid,id:&str)->(Uuid,Uuid,Uuid) {
  let title:String=sqlx::query_scalar("SELECT title FROM nodes WHERE graph_id=$1 AND node_id=$2").bind(graph).bind(node).fetch_one(&f.owner).await.unwrap();
  let facts=f.source_in_graph(graph,node,id,"permitted","meeting").await;let text=format!("{title} is tracked work.");
  // Real permitted witnesses support the full title in both canonical title
  // paths. Change existing facts, so independent semantic scores stay exact.
  sqlx::query("UPDATE source_versions SET raw_inline=$2 WHERE version_id=$1").bind(facts.1).bind(&text).execute(&f.owner).await.unwrap();
  sqlx::query("UPDATE atoms SET content_inline=$2 WHERE atom_id=$1").bind(facts.2).bind(&text).execute(&f.owner).await.unwrap();
  sqlx::query("UPDATE semantic_atoms SET statement=$2,entities=$3 WHERE source_version_id=$1").bind(facts.1).bind(text).bind(json!([{"mention":title,"type":"project"}])).execute(&f.owner).await.unwrap();facts
 }
 async fn check(f:&Fixture,app:bool,graphs:&[Uuid],scores:&[Score],phase:&str) {
  for mode in ["all_retained","current"] {
   let mut ranked:Vec<_>=scores.iter().filter(|s|s.4&&s.1>0).collect();
   ranked.sort_by_key(|s|std::cmp::Reverse((s.1,if mode=="current"{s.3}else{s.2},s.0)));
   let expected:Vec<_>=ranked.into_iter().take(6).map(|s|s.0).collect();assert_eq!(expected.len(),6);
   for (query,collection) in [("",None),("W",None),("Work",None),("",Some("meetings"))] {
    let mut shape=json!({"graph_ids":graphs,"outcome_versions":mode});if let Some(c)=collection{shape["collection"]=json!(c);}
    let scope:LibraryScope=serde_json::from_value(shape).unwrap();let tenant=f.tenant(app);let mut tx=tenant.begin_member_repeatable_read(f.account).await.unwrap();library_read::configure_read_model_proof_tx(&mut tx).await.unwrap();
    let mut after=None::<String>;let mut saw_private_title=false;
    for page in 0..3 {
     let pair=library_read::compare_read_model_tx(&mut tx,&scope,query,2,[None,None,after.as_deref()]).await.unwrap();assert_eq!(pair.path,"model","phase={phase} plane={} app={app} mode={mode} q={query} reason={:?}",f.tenant_plane,pair.fallback_reason);assert_eq!(pair.live,pair.candidate);
     let work=pair.candidate["prominent_work"].as_array().unwrap();let actual:Vec<Uuid>=work.iter().map(|n|serde_json::from_value(n["node_id"].clone()).unwrap()).collect();assert_eq!(actual,expected,"phase={phase} mode={mode} page={page}");
     // The semantic oracle covers all counts/facets; independently check the
     // selected work scores and the UUID tie-break at the sixth/seventh edge.
     for row in work {let id:Uuid=serde_json::from_value(row["node_id"].clone()).unwrap();let s=scores.iter().find(|s|s.0==id).unwrap();assert_eq!(row["contribution"]["source_support_count"],json!(s.1));assert_eq!(row["wider_context"]["semantic_atom_count"],json!(s.2));}
     if phase!="clean"&&query.is_empty(){for row in pair.candidate["nodes"]["items"].as_array().unwrap(){if row["node_id"]==json!(scores[14].0){assert_eq!(row["title"],Value::Null,"unsupported full title must stay private");saw_private_title=true;}}}
     if phase=="clean"&&app&&page==0&&query.is_empty()&&collection.is_none(){if let Ok(dir)=std::env::var("LIBRARY_SELECTOR_LOCAL_PLAN_DIR") {
      let metadata:Value=sqlx::query_scalar("SELECT jsonb_build_object('role',current_user,'semantic_rls_active',row_security_active('semantic_atoms'),'version',version(),'work_mem',current_setting('work_mem'),'plan_cache_mode',current_setting('plan_cache_mode'))").fetch_one(&mut *tx).await.unwrap();assert_eq!(metadata["semantic_rls_active"],json!(true));
      let sql=format!("EXPLAIN(ANALYZE,VERBOSE,BUFFERS,FORMAT JSON) {}",library_read::read_model_sql_for_test(&scope));
      let plan:Value=sqlx::query_scalar(&sql).bind(&scope.graph_ids).bind(json!({})).bind(None::<Uuid>).bind(None::<Uuid>).bind(None::<Uuid>).bind(None::<String>).bind(None::<String>).bind(None::<chrono::DateTime<chrono::Utc>>).bind(None::<Uuid>).bind(None::<chrono::DateTime<chrono::Utc>>).bind(None::<Uuid>).bind(None::<i64>).bind(None::<i64>).bind(None::<Uuid>).bind(3_i64).bind(None::<chrono::DateTime<chrono::Utc>>).fetch_one(&mut *tx).await.unwrap();
      use std::io::Write;let path=std::path::Path::new(&dir).join(format!("work-{}-{mode}.json",if f.tenant_plane{"tenant"}else{"consumer"}));let mut file=std::fs::OpenOptions::new().write(true).create_new(true).open(path).unwrap();file.write_all(serde_json::to_vec_pretty(&json!({"source_sha":env!("CORALSWARM_GIT_SHA_FULL"),"metadata":metadata,"eligible_clean_per_graph":8,"expected_work_ids":expected,"plan":plan})).unwrap().as_slice()).unwrap();
     }}
     let next=pair.candidate["nodes"]["next_cursor"].as_str().map(str::to_owned);if next.is_none(){break}assert_ne!(next,after);after=next;
    }
    if phase!="clean"&&query.is_empty(){assert!(saw_private_title,"cursor pages must actually exercise the private title");}tx.rollback().await.unwrap();
   }
  }
 }
 for plane in [false,true] {for app in [false,true] {
  let f=Fixture::new(plane).await;sqlx::query("UPDATE graphs SET owner_user_id=$2 WHERE id=$1").bind(f.foreign).bind(f.user).execute(&f.owner).await.unwrap();
  let empty:Uuid=sqlx::query_scalar("INSERT INTO graphs(account_id,name,kind,owner_user_id) VALUES($1,'Empty work rank','member',$2) RETURNING id").bind(f.account).bind(f.user).fetch_one(&f.owner).await.unwrap();
  let mut scores=Vec::<Score>::new();let mut facts=Vec::new();
  for (gindex,graph) in [f.graph,f.foreign].into_iter().enumerate(){for index in 0..8 {
   let node=Uuid::from_u128(10_000+gindex as u128*10+index as u128);sqlx::query("INSERT INTO nodes(node_id,graph_id,type,title,body) VALUES($1,$2,'entity',$3,'{}')").bind(node).bind(graph).bind(format!("Work {gindex} {index}")).execute(&f.owner).await.unwrap();
   let fact=source_with_title(&f,graph,node,&format!("work-{gindex}-{index}")).await;facts.push(fact);scores.push((node,1,1,1,true));
  }}
  let (item,old,_)=facts[0];sqlx::query("UPDATE source_versions SET content_sha256='old-work-content' WHERE version_id=$1").bind(old).execute(&f.owner).await.unwrap();
  for ordinal in [1,2]{let atom:Uuid=sqlx::query_scalar("INSERT INTO semantic_atoms(graph_id,source_version_id,ordinal,kind,statement,extraction_version,model) VALUES($1,$2,$3,'fact','Older work support.','source-atoms-v1','fixture') RETURNING semantic_atom_id").bind(f.graph).bind(old).bind(ordinal).fetch_one(&f.owner).await.unwrap();sqlx::query("INSERT INTO node_semantic_atoms(graph_id,node_id,semantic_atom_id) VALUES($1,$2,$3)").bind(f.graph).bind(scores[0].0).bind(atom).execute(&f.owner).await.unwrap();}
  let current:Uuid=sqlx::query_scalar("INSERT INTO source_versions(graph_id,source_item_id,source_id,source_type,provider_namespace,origin,raw_inline,source_metadata,captured_at,content_sha256,extraction_status) SELECT graph_id,source_item_id,source_id,source_type,provider_namespace,origin,'New work capture',source_metadata,'2026-03-01','new-work-content','done' FROM source_versions WHERE version_id=$1 RETURNING version_id").bind(old).fetch_one(&f.owner).await.unwrap();
  payload_outcome(&f,Uuid::new_v4(),current,0,"Work 0 0 has new support.",&[scores[0].0]).await;sqlx::query("UPDATE semantic_atoms SET entities='[{\"mention\":\"Work 0 0\",\"type\":\"project\"}]' WHERE source_version_id=$1").bind(current).execute(&f.owner).await.unwrap();sqlx::query("UPDATE source_items SET current_version_id=$2 WHERE source_item_id=$1").bind(item).bind(current).execute(&f.owner).await.unwrap();scores[0].2=4;
  let new_node=Uuid::from_u128(10_100);sqlx::query("INSERT INTO nodes(node_id,graph_id,type,title,body) VALUES($1,$2,'entity','Work newly eligible','{}')").bind(new_node).bind(f.graph).execute(&f.owner).await.unwrap();scores.push((new_node,0,0,0,false));
  for graph in [f.graph,f.foreign,empty]{f.refresh_graph(app,graph).await;}
  let graphs=[f.graph,f.foreign,f.graph,empty];check(&f,app,&graphs,&scores,"clean").await;
  // Remove clean winners before local K, change exact eligibility, promote a
  // lower row and introduce a formerly unsupported node via dirty replacement.
  sqlx::query("UPDATE source_items SET access_state='restricted' WHERE source_item_id=$1").bind(facts[15].0).execute(&f.owner).await.unwrap();scores[15].4=false;
  // Stored titles are NOT NULL. A real unsupported rename makes the public
  // title NULL through the canonical evidence rule, without invalid raw data.
  sqlx::query("UPDATE nodes SET title='Hidden work title' WHERE node_id=$1").bind(scores[14].0).execute(&f.owner).await.unwrap();scores[14].4=false;
  sqlx::query("UPDATE nodes SET owner_id=$2 WHERE node_id=$1").bind(scores[13].0).bind(f.user).execute(&f.owner).await.unwrap();scores[13].4=false;
  sqlx::query("UPDATE nodes SET body='{\"synthetic_hub\":true}' WHERE node_id=$1").bind(scores[12].0).execute(&f.owner).await.unwrap();scores[12].4=false;
  sqlx::query("UPDATE nodes SET type='entity',entity_type='project',entity_type_sticky=true WHERE node_id=$1").bind(scores[11].0).execute(&f.owner).await.unwrap();
  source_with_title(&f,f.graph,scores[1].0,"work-promoted").await;scores[1].1=2;scores[1].2=2;scores[1].3=2;
  source_with_title(&f,f.graph,new_node,"work-newly-eligible").await;scores[16]=(new_node,1,1,1,true);
  check(&f,app,&graphs,&scores,"dirty").await;for graph in [f.graph,f.foreign]{f.refresh_graph(app,graph).await;}check(&f,app,&graphs,&scores,"drained").await;
  // A true deletion retains the existing allowed predrain fallback. After
  // drain Model must again produce the exact independently ranked work list.
  sqlx::query("DELETE FROM nodes WHERE node_id=$1").bind(scores[11].0).execute(&f.owner).await.unwrap();scores[11].4=false;
  let tenant=f.tenant(app);let scope:LibraryScope=serde_json::from_value(json!({"graph_ids":graphs})).unwrap();let mut tx=tenant.begin_member_repeatable_read(f.account).await.unwrap();library_read::configure_read_model_proof_tx(&mut tx).await.unwrap();let pair=library_read::compare_read_model_tx(&mut tx,&scope,"",2,[None;3]).await.unwrap();assert_eq!(pair.live,pair.candidate);tx.rollback().await.unwrap();f.refresh_graph(app,f.foreign).await;check(&f,app,&graphs,&scores,"deleted").await;
  let scope:LibraryScope=serde_json::from_value(json!({"graph_ids":[empty,empty]})).unwrap();let mut tx=tenant.begin_member_repeatable_read(f.account).await.unwrap();library_read::configure_read_model_proof_tx(&mut tx).await.unwrap();let pair=library_read::compare_read_model_tx(&mut tx,&scope,"",20,[None;3]).await.unwrap();assert_eq!(pair.path,"model");assert_eq!(pair.live,pair.candidate);assert_eq!(pair.candidate["prominent_work"],json!([]));tx.rollback().await.unwrap();f.close().await;
 }}
}

#[tokio::test]
async fn candidate_current_metadata_keeps_job_windows_and_explicit_historical_live_distinct() {
 for plane in [false,true] {for app in [false,true] {
  let f=Fixture::new(plane).await;let node=f.node(f.graph,"Atlas").await;let(item,old,_)=f.source(node,"metadata-alias","permitted","meeting").await;
  sqlx::query("UPDATE source_versions SET content_sha256='same-metadata-content' WHERE version_id=$1").bind(old).execute(&f.owner).await.unwrap();
  let current:Uuid=sqlx::query_scalar("INSERT INTO source_versions(graph_id,source_item_id,source_id,source_type,provider_namespace,origin,raw_inline,source_metadata,captured_at,content_sha256,extraction_status,next_start_char) SELECT graph_id,source_item_id,source_id,source_type,provider_namespace,origin,raw_inline,'{\"title\":\"Current metadata heading\"}','2026-03-01',content_sha256,'pending',10 FROM source_versions WHERE version_id=$1 RETURNING version_id").bind(old).fetch_one(&f.owner).await.unwrap();sqlx::query("UPDATE source_items SET current_version_id=$2 WHERE source_item_id=$1").bind(item).bind(current).execute(&f.owner).await.unwrap();
  let low_job=Uuid::from_u128(20_001);let high_job=Uuid::from_u128(20_002);
  for (raw,error,job,expected,reason) in [("not_started",None,None,"not_started",None),("pending",None,None,"pending",None),("running",None,None,"running",None),("failed",None,None,"failed",None),("blocked",Some("execution_budget_exhausted"),None,"blocked",Some("execution_budget_exhausted")),("blocked",Some("untrusted provider detail"),None,"blocked",None),("failed",None,Some("pending"),"pending",None),("failed",None,Some("running"),"running",None),("failed",None,Some("dead"),"failed",None),("done",None,Some("dead"),"done",None)] {
   sqlx::query("DELETE FROM jobs WHERE payload->>'source_version_id'=$1").bind(current.to_string()).execute(&f.owner).await.unwrap();sqlx::query("UPDATE source_versions SET extraction_status=$2,extraction_error=$3 WHERE version_id=$1").bind(current).bind(raw).bind(error).execute(&f.owner).await.unwrap();
   if let Some(job)=job {
    // Same created_at: highest UUID wins. An even newer different start_char
    // must never override the exact current-window job, even when dead.
    for (id,status,start,created) in [(low_job,"pending",10,"2026-04-01T00:00:00Z"),(high_job,job,10,"2026-04-01T00:00:00Z"),(Uuid::from_u128(20_003),"dead",11,"2026-04-02T00:00:00Z")] {sqlx::query("INSERT INTO jobs(id,account_id,kind,payload,status,created_at) VALUES($1,$2,'ExtractSourceAtoms',$3,$4::job_status,$5)").bind(id).bind(f.account).bind(json!({"graph_id":f.graph,"source_version_id":current,"start_char":start})).bind(status).bind(created.parse::<chrono::DateTime<chrono::Utc>>().unwrap()).execute(&f.owner).await.unwrap();}
   }
   f.refresh(app).await;
   for mode in ["all_retained","current"] {
    let scope:LibraryScope=serde_json::from_value(json!({"graph_ids":[f.graph],"outcome_versions":mode})).unwrap();let tenant=f.tenant(app);let mut tx=tenant.begin_member_repeatable_read(f.account).await.unwrap();library_read::configure_read_model_proof_tx(&mut tx).await.unwrap();
    let pair=library_read::compare_read_model_tx(&mut tx,&scope,"",20,[None;3]).await.unwrap();assert_eq!(pair.path,"model","raw={raw} job={job:?} reason={:?}",pair.fallback_reason);assert_eq!(pair.live,pair.candidate);let row=&pair.candidate["sources"]["items"][0];assert_eq!(row["source_item_id"],json!(item));assert_eq!(row["selected_version_id"],json!(current));assert_eq!(row["current_version_id"],json!(current));assert_eq!(row["selected_extraction_status"],json!(expected));assert_eq!(row["current_extraction_status"],json!(expected));assert_eq!(row["current_extraction_reason"],json!(reason));assert!(row["current_captured_at"].as_str().unwrap().starts_with("2026-03-01"));assert_eq!(row["semantic_atom_count"],json!(1));tx.rollback().await.unwrap();
   }
   // Selected historical payload and current jobs metadata intentionally
   // differ. NeedsLive must use canonical SQL, not the specialized alias.
   let scope:LibraryScope=serde_json::from_value(json!({"graph_ids":[f.graph],"source_version_id":old,"outcome_versions":"current"})).unwrap();let tenant=f.tenant(app);let mut tx=tenant.begin_member_repeatable_read(f.account).await.unwrap();library_read::configure_read_model_proof_tx(&mut tx).await.unwrap();let pair=library_read::compare_read_model_tx(&mut tx,&scope,"",20,[None;3]).await.unwrap();assert_eq!(pair.path,"live");assert_eq!(pair.fallback_reason.as_deref(),Some("narrow_scope"));assert_eq!(pair.live,pair.candidate);let row=&pair.candidate["sources"]["items"][0];assert_eq!(row["selected_version_id"],json!(old));assert_eq!(row["current_version_id"],json!(current));assert_eq!(row["selected_extraction_status"],json!("done"));assert_eq!(row["current_extraction_status"],json!(expected));assert_eq!(row["selected_extraction_reason"],Value::Null);assert_eq!(row["current_extraction_reason"],json!(reason));assert!(row["captured_at"].as_str().unwrap().starts_with("2026-02-01"));assert!(row["current_captured_at"].as_str().unwrap().starts_with("2026-03-01"));tx.rollback().await.unwrap();
  }
  // A nullable current pointer keeps the original LEFT JOIN NULL extension.
  sqlx::query("UPDATE source_items SET current_version_id=NULL WHERE source_item_id=$1").bind(item).execute(&f.owner).await.unwrap();f.refresh(app).await;let tenant=f.tenant(app);let scope:LibraryScope=serde_json::from_value(json!({"graph_ids":[f.graph]})).unwrap();let mut tx=tenant.begin_member_repeatable_read(f.account).await.unwrap();library_read::configure_read_model_proof_tx(&mut tx).await.unwrap();let pair=library_read::compare_read_model_tx(&mut tx,&scope,"",20,[None;3]).await.unwrap();assert_eq!(pair.path,"model");assert_eq!(pair.live,pair.candidate);let row=&pair.candidate["sources"]["items"][0];for key in ["selected_version_id","selected_extraction_status","current_extraction_status","current_extraction_reason","current_captured_at"]{assert_eq!(row[key],Value::Null);}assert_eq!(row["semantic_atom_count"],json!(1));tx.rollback().await.unwrap();f.close().await;
 }}
}

#[tokio::test]
async fn stored_and_dirty_paths_equal_live_in_both_modes_roles_and_planes() {
 for tenant in [false,true] {
  let f=Fixture::new(tenant).await;let atlas=f.node(f.graph,"Atlas").await;let hidden=f.node(f.graph,"Hidden").await;
  let (item,version,atom)=f.source(atlas,"one","permitted","meeting").await;
  let unsupported=f.node(f.graph,"Unsupported visible node").await;
  sqlx::query("INSERT INTO node_semantic_atoms(graph_id,node_id,semantic_atom_id) SELECT $1,$2,semantic_atom_id FROM semantic_atoms WHERE graph_id=$1 AND source_version_id=$3").bind(f.graph).bind(unsupported).bind(version).execute(&f.owner).await.unwrap();
  f.source(atlas,"two","permitted","work_session").await;
  f.source(atlas,"three","permitted","agent_session").await;
  f.source(atlas,"four","permitted","message").await;
  f.source(atlas,"five","permitted","message").await;
  let _empty=f.node(f.graph,"Empty").await;
  for index in 0..5 {
   let title=format!("Unmapped {index}");let node=f.node(f.graph,&title).await;
   let atom:Uuid=sqlx::query_scalar("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,timestamp) VALUES($1,'note',$2,'fixture',$3,now()) RETURNING atom_id").bind(f.graph).bind(format!("unmapped-{index}")).bind(format!("{title} is retained evidence.")).fetch_one(&f.owner).await.unwrap();
   sqlx::query("INSERT INTO node_atoms(node_id,atom_id) VALUES($1,$2)").bind(node).bind(atom).execute(&f.owner).await.unwrap();
  }
  let long_title=format!("Longprefix {}","L".repeat(300));let long_node=f.node(f.graph,&long_title).await;
  let long_atom:Uuid=sqlx::query_scalar("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,timestamp) VALUES($1,'note','long-title','fixture',$2,now()) RETURNING atom_id").bind(f.graph).bind(&long_title).fetch_one(&f.owner).await.unwrap();
  sqlx::query("INSERT INTO node_atoms(node_id,atom_id) VALUES($1,$2)").bind(long_node).bind(long_atom).execute(&f.owner).await.unwrap();
  let (denied,_,_)=f.source(hidden,"denied","restricted","message").await;
  for (title,source_type,source_id,retracted,derived) in [("Positional denied","slack","denied:chunk:7",false,false),("Retracted","note","retracted",true,false),("Derived","note","derived",false,true)] {
   let node=f.node(f.graph,title).await;
   let atom:Uuid=sqlx::query_scalar("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,evidence_retracted,derived_copy_bound,timestamp) VALUES($1,$2,$3,'fixture',$4,$5,$6,now()) RETURNING atom_id").bind(f.graph).bind(source_type).bind(source_id).bind(title).bind(retracted).bind(derived).fetch_one(&f.owner).await.unwrap();
   sqlx::query("INSERT INTO node_atoms(node_id,atom_id) VALUES($1,$2)").bind(node).bind(atom).execute(&f.owner).await.unwrap();
  }
  for app in [false,true] {
   f.refresh(app).await;
   f.pair(app,json!({}),"Longprefix","model").await;
   for mode in ["all_retained","current"] {
    for extra in [json!({}),json!({"service_id":"slack"}),json!({"service_id":"slack","connection":{"kind":"oauth_grant","id":"fixture"}}),json!({"service_id":"slack","container":{"kind":"channel","id":"C1","provider_namespace":"one"}}),json!({"content_kind":"meeting"}),json!({"collection":"meetings"}),json!({"collection":"work_sessions"}),json!({"collection":"agent_sessions"})] {
     let mut extra=extra;extra["outcome_versions"]=json!(mode);
     for q in ["","Atlas","deploy","no-match"] {f.pair(app,extra.clone(),q,"model").await;}
    }
   }
   // Exact narrowing remains the independent live reader.
   for extra in [json!({"source_item_id":item}),json!({"source_version_id":version}),json!({"source_item_id":item,"source_version_id":version}),json!({"node_id":atlas})] {f.pair(app,extra,"Atlas","live").await;}
   // Dirty data is observed BEFORE drain and remains identical after drain.
   sqlx::query("UPDATE nodes SET title='Renamed' WHERE node_id=$1").bind(atlas).execute(&f.owner).await.unwrap();
   f.pair(app,json!({}),"Renamed","model").await;
   sqlx::query("UPDATE nodes SET title='Atlas' WHERE node_id=$1").bind(atlas).execute(&f.owner).await.unwrap();
   f.pair(app,json!({}),"Atlas","model").await;
   // Granting a previously uncached source must create dirty object keys.
   sqlx::query("UPDATE source_items SET access_state='permitted' WHERE source_item_id=$1").bind(denied).execute(&f.owner).await.unwrap();
   f.pair(app,json!({}),"","model").await;
   sqlx::query("UPDATE atoms SET content_inline='Atlas changed.' WHERE atom_id=$1").bind(atom).execute(&f.owner).await.unwrap();
   f.pair(app,json!({"collection":"meetings"}),"Atlas","model").await;
   f.refresh(app).await;f.pair(app,json!({}),"Atlas","model").await;
  }
  f.close().await;
 }
}

#[tokio::test]
async fn batched_page_context_preserves_historical_versions_and_filtered_legacy_union() {
 for plane in [false,true] {
  let f=Fixture::new(plane).await;let atlas=f.node(f.graph,"Atlas").await;let other=f.node(f.graph,"Other").await;
  let (item,old,_)=f.source(atlas,"historical","permitted","meeting").await;
  let (_,second,_)=f.source(other,"outside-filter","permitted","work_session").await;
  // One outcome supports two page nodes. The wider context must keep this
  // unfiltered historical support even when the meetings filter excludes it.
  sqlx::query("INSERT INTO node_semantic_atoms(graph_id,node_id,semantic_atom_id) SELECT graph_id,$2,semantic_atom_id FROM semantic_atoms WHERE source_version_id=$1").bind(second).bind(atlas).execute(&f.owner).await.unwrap();
  sqlx::query("UPDATE source_versions SET origin=origin||'{\"transport\":\"synthetic\"}'::jsonb WHERE version_id=$1").bind(old).execute(&f.owner).await.unwrap();
  // The linked old version is independent/synthetic; the newer unlinked
  // current version is derived and nonsynthetic. Reading current item or all
  // item versions would corrupt the old version's context flags/capture time.
  let current:Uuid=sqlx::query_scalar("INSERT INTO source_versions(graph_id,source_item_id,source_id,source_type,provider_namespace,origin,raw_inline,source_metadata,captured_at,extraction_status) SELECT graph_id,source_item_id,source_id,source_type,provider_namespace,origin-'transport','Unlinked recapture','{\"independence\":\"derived_copy\"}'::jsonb,'2026-03-01','done' FROM source_versions WHERE version_id=$1 RETURNING version_id").bind(old).fetch_one(&f.owner).await.unwrap();
  sqlx::query("UPDATE source_items SET current_version_id=$2 WHERE source_item_id=$1").bind(item).bind(current).execute(&f.owner).await.unwrap();
  for app in [false,true] {
   f.refresh(app).await;
   for mode in ["all_retained","current"] {for collection in [None,Some("meetings"),Some("work_sessions")] {
    let mut extra=json!({"outcome_versions":mode});if let Some(value)=collection {extra["collection"]=json!(value);}
    f.pair(app,extra,"","model").await;
   }}
  }
  f.close().await;
 }
}

#[tokio::test]
async fn version_two_page_context_retains_unmapped_and_legacy_only_zero_semantic_rows() {
 for plane in [false,true] {for app in [false,true] {
  let f=Fixture::new(plane).await;let unmapped=f.node(f.graph,"Unmapped").await;let legacy=f.node(f.graph,"Legacy").await;
  let atom:Uuid=sqlx::query_scalar("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,timestamp) VALUES($1,'note','unmapped-zero','fixture','Unmapped retained evidence.',now()) RETURNING atom_id").bind(f.graph).fetch_one(&f.owner).await.unwrap();sqlx::query("INSERT INTO node_atoms(node_id,atom_id) VALUES($1,$2)").bind(unmapped).bind(atom).execute(&f.owner).await.unwrap();
  let (_,version,legacy_atom)=f.source(legacy,"legacy-zero","permitted","meeting").await;
  sqlx::query("DELETE FROM node_semantic_atoms WHERE node_id=$1").bind(legacy).execute(&f.owner).await.unwrap();sqlx::query("UPDATE atoms SET content_inline='Legacy retained evidence.' WHERE atom_id=$1").bind(legacy_atom).execute(&f.owner).await.unwrap();sqlx::query("UPDATE source_versions SET source_metadata=source_metadata||'{\"synthetic_demo\":true}'::jsonb WHERE version_id=$1").bind(version).execute(&f.owner).await.unwrap();
  f.refresh(app).await;
  let item_count:i64=sqlx::query_scalar("SELECT count(*) FROM library_node_item_evidence WHERE graph_id=$1 AND node_id=$2").bind(f.graph).bind(unmapped).fetch_one(&f.owner).await.unwrap();assert_eq!(item_count,0);
  for mode in ["all_retained","current"] {
   let value=f.pair(app,json!({"outcome_versions":mode}),"","model").await;
   for (id,support,synthetic) in [(unmapped,0,false),(legacy,1,true)] {
    let row=value["nodes"]["items"].as_array().unwrap().iter().find(|n|n["node_id"]==json!(id)).unwrap();assert_eq!(row["wider_context"]["semantic_atom_count"],0);assert_eq!(row["wider_context"]["source_support_count"],support);assert_eq!(row["synthetic_demo"],synthetic);assert!(row["latest_captured_at"].is_null());
   }
  }
  f.close().await;
 }}
}

#[tokio::test]
async fn version_one_default_facts_stay_live_until_version_two_backfill_resumes_completely() {
 for plane in [false,true] {
  let f=Fixture::new(plane).await;
  for index in 0..3 {let node=f.node(f.graph,&format!("Atlas {index}")).await;f.source(node,&format!("upgrade-{index}"),"permitted","meeting").await;}
  f.refresh(false).await;
  // This is the exact default-filled legacy-row state after the additive
  // migration: existing v1 link/count facts with no trusted v2 metadata.
  sqlx::query("UPDATE library_node_item_evidence SET semantic_independent_version=false,semantic_synthetic=false,semantic_latest_captured_at=NULL,entity_type_has_match=false,entity_type_value=NULL,entity_type_captured_at=NULL,entity_type_semantic_atom_id=NULL,entity_type_ambiguous=false WHERE graph_id=$1").bind(f.graph).execute(&f.owner).await.unwrap();
  sqlx::query("UPDATE library_node_evidence SET computed_version=1 WHERE graph_id=$1").bind(f.graph).execute(&f.owner).await.unwrap();
  let last:Uuid=sqlx::query_scalar("SELECT node_id FROM nodes WHERE graph_id=$1 ORDER BY node_id DESC LIMIT 1").bind(f.graph).fetch_one(&f.owner).await.unwrap();
  sqlx::query("UPDATE library_evidence_state SET ready_version=1,backfill_version=1,backfill_after_node_id=$2 WHERE graph_id=$1").bind(f.graph).bind(last).execute(&f.owner).await.unwrap();f.pair(false,json!({}),"","live").await;
  let mut tx=f.owner.begin().await.unwrap();sqlx::query("SELECT set_config('app.account_id',$1,true),set_config('app.tenant_actor',$2,true)").bind(f.account.to_string()).bind(if plane{"system"}else{""}).execute(&mut *tx).await.unwrap();library_evidence::refresh_graph_tx(&mut tx,f.graph,Limits{nodes:1,changes:1}).await.unwrap();
  let state:(Option<i32>,Option<i32>,Option<Uuid>)=sqlx::query_as("SELECT ready_version,backfill_version,backfill_after_node_id FROM library_evidence_state WHERE graph_id=$1").bind(f.graph).fetch_one(&mut *tx).await.unwrap();assert_eq!(state.0,None);assert_eq!(state.1,Some(READ_MODEL_VERSION));assert!(state.2.is_some()&&state.2!=Some(last));tx.commit().await.unwrap();f.pair(false,json!({}),"","live").await;
  // A separate transaction resumes the persistent v2 cursor and fills every
  // item before readiness, never exposing the partially default-filled set.
  f.refresh(false).await;f.pair(false,json!({}),"","model").await;
  let facts:(i64,i64)=sqlx::query_as("SELECT (SELECT count(*) FROM library_node_item_evidence WHERE graph_id=$1 AND semantic_latest_captured_at IS NOT NULL AND semantic_independent_version),(SELECT count(*) FROM library_node_evidence WHERE graph_id=$1 AND computed_version=$2)").bind(f.graph).bind(READ_MODEL_VERSION).fetch_one(&f.owner).await.unwrap();assert_eq!(facts,(3,3));f.close().await;
 }
}

#[tokio::test]
async fn version_two_entity_winner_preserves_null_types_and_falls_back_only_for_selected_ambiguity() {
 async fn shape(f:&Fixture,app:bool,collection:Option<&str>,path:&str)->Value {
  let mut value=json!({"graph_ids":[f.graph]});if let Some(c)=collection{value["collection"]=json!(c);}
  let scope:LibraryScope=serde_json::from_value(value).unwrap();let tenant=f.tenant(app);let mut tx=tenant.begin_member_repeatable_read(f.account).await.unwrap();library_read::configure_read_model_proof_tx(&mut tx).await.unwrap();
  let pair=library_read::compare_read_model_tx(&mut tx,&scope,"",40,[None;3]).await.unwrap();assert_eq!(pair.path,if path=="model"{"model"}else{"live"},"collection={collection:?} reason={:?}",pair.fallback_reason);
  if path=="model" {assert_eq!(pair.live,pair.candidate);}else{assert_eq!(pair.fallback_reason.as_deref(),Some(path));}
  tx.rollback().await.unwrap();pair.candidate
 }
 for plane in [false,true] {for app in [false,true] {
  let f=Fixture::new(plane).await;let node=f.node(f.graph,"Atlas").await;let (_,old,_)=f.source(node,"old-type","permitted","meeting").await;let (_,new,_)=f.source(node,"new-type","permitted","work_session").await;
  sqlx::query("UPDATE source_versions SET captured_at='2026-03-01' WHERE version_id=$1").bind(new).execute(&f.owner).await.unwrap();
  sqlx::query("UPDATE semantic_atoms SET entities='[{\"mention\":\"Atlas\",\"type\":\"person\"},{\"mention\":\"Atlas\",\"type\":null}]' WHERE source_version_id=$1").bind(old).execute(&f.owner).await.unwrap();f.refresh(app).await;
  // An older ambiguous item is harmless until the collection selects it.
  shape(&f,app,None,"model").await;shape(&f,app,Some("meetings"),"entity_type_ambiguous").await;shape(&f,app,Some("work_sessions"),"model").await;
  for (entities,expected) in [(json!([{"mention":"Atlas","type":"project"},{"mention":"Atlas","type":"project"}]),Some("project")),(json!([{"mention":"Atlas","type":null},{"mention":"Atlas","type":null}]),None)] {
   sqlx::query("UPDATE semantic_atoms SET entities=$2 WHERE source_version_id=$1").bind(new).bind(entities).execute(&f.owner).await.unwrap();
   for drained in [false,true] {if drained {f.refresh(app).await;}let value=shape(&f,app,None,"model").await;let row=value["nodes"]["items"].as_array().unwrap().iter().find(|n|n["node_id"]==json!(node)).unwrap();assert_eq!(row["entity_type"],json!(expected));}
  }
  sqlx::query("UPDATE semantic_atoms SET entities='[{\"mention\":\"Atlas\",\"type\":null},{\"mention\":\"Atlas\",\"type\":\"project\"}]' WHERE source_version_id=$1").bind(new).execute(&f.owner).await.unwrap();
  shape(&f,app,None,"entity_type_ambiguous").await;f.refresh(app).await;shape(&f,app,None,"entity_type_ambiguous").await;
  // Sticky type is authoritative and never needs an ambiguous derived tuple.
  sqlx::query("UPDATE nodes SET entity_type_sticky=true,entity_type='person' WHERE node_id=$1").bind(node).execute(&f.owner).await.unwrap();shape(&f,app,None,"model").await;f.refresh(app).await;shape(&f,app,None,"model").await;
  sqlx::query("UPDATE nodes SET entity_type_sticky=false WHERE node_id=$1").bind(node).execute(&f.owner).await.unwrap();sqlx::query("DELETE FROM semantic_atoms WHERE source_version_id=$1").bind(new).execute(&f.owner).await.unwrap();sqlx::query("UPDATE semantic_atoms SET entities='[{\"mention\":\"Atlas\",\"type\":\"person\"}]' WHERE source_version_id=$1").bind(old).execute(&f.owner).await.unwrap();shape(&f,app,None,"raw_edge_overflow").await;f.refresh(app).await;shape(&f,app,None,"model").await;
  // Tuple coherence and semantic-only facts are guarded in the stored grain.
  for sql in ["UPDATE library_node_item_evidence SET entity_type_has_match=false WHERE entity_type_has_match", "UPDATE library_node_item_evidence SET entity_type_ambiguous=true,entity_type_value='person' WHERE entity_type_has_match", "UPDATE library_node_item_evidence SET semantic_link=false WHERE semantic_link"] {
   let error=sqlx::query(sql).execute(&f.owner).await.unwrap_err();assert_eq!(error.as_database_error().unwrap().code().as_deref(),Some("23514"));
  }
  f.close().await;
 }}
}

#[tokio::test]
async fn outcome_page_node_prefix_is_exact_at_twenty_and_preserves_null_titles() {
 for plane in [false,true] {
  let f=Fixture::new(plane).await;let atlas=f.node(f.graph,"Atlas").await;let (item,version,_)=f.source(atlas,"page-prefix","permitted","message").await;
  let outcome:Uuid=sqlx::query_scalar("SELECT semantic_atom_id FROM semantic_atoms WHERE graph_id=$1 AND source_version_id=$2").bind(f.graph).bind(version).fetch_one(&f.owner).await.unwrap();
  let mut keys=Vec::new();for index in 0..22 {keys.push(f.node(f.graph,&format!("Unsupported prefix {index}")).await);}
  let foreign=f.node(f.foreign,"Foreign prefix").await;
  let error=sqlx::query("INSERT INTO node_semantic_atoms(graph_id,node_id,semantic_atom_id) VALUES($1,$2,$3)").bind(f.graph).bind(foreign).bind(outcome).execute(&f.owner).await.unwrap_err();assert_eq!(error.as_database_error().unwrap().code().as_deref(),Some("23503"));
  for app in [false,true] {for count in [0_usize,20,21,22] {
   sqlx::query("DELETE FROM node_semantic_atoms WHERE graph_id=$1 AND semantic_atom_id=$2").bind(f.graph).bind(outcome).execute(&f.owner).await.unwrap();
   sqlx::query("INSERT INTO node_semantic_atoms(graph_id,node_id,semantic_atom_id) SELECT $1,k,$3 FROM unnest($2::uuid[]) k").bind(f.graph).bind(&keys[..count]).bind(outcome).execute(&f.owner).await.unwrap();
   if count>0 {let error=sqlx::query("INSERT INTO node_semantic_atoms(graph_id,node_id,semantic_atom_id) VALUES($1,$2,$3)").bind(f.graph).bind(keys[0]).bind(outcome).execute(&f.owner).await.unwrap_err();assert_eq!(error.as_database_error().unwrap().code().as_deref(),Some("23505"));}
   f.refresh(app).await;
   for mode in ["all_retained","current"] {for filtered in [false,true] {
    let mut scope:LibraryScope=serde_json::from_value(json!({"graph_ids":[f.graph],"outcome_versions":mode})).unwrap();if filtered{scope.service_id=Some(coralswarm_backend::source_origin::ServiceId::new("slack").unwrap());}
    let tenant=f.tenant(app);let mut tx=tenant.begin_member_repeatable_read(f.account).await.unwrap();library_read::configure_read_model_proof_tx(&mut tx).await.unwrap();
    let pair=library_read::compare_read_model_tx(&mut tx,&scope,"Atlas",40,[None;3]).await.unwrap();assert_eq!(pair.path,"model");assert_eq!(pair.live,pair.candidate,"prefix plane={plane} app={app} count={count} mode={mode} filtered={filtered}");
    let page=&pair.candidate["outcomes"]["items"][0];assert_eq!(page["nodes"].as_array().unwrap().len(),count.min(20));assert_eq!(page["nodes_truncated"],json!(count>20));
    assert!(page["nodes"].as_array().unwrap().iter().all(|n|n["title"].is_null()));tx.rollback().await.unwrap();
   }}
  }
   sqlx::query("UPDATE source_items SET access_state='restricted' WHERE source_item_id=$1").bind(item).execute(&f.owner).await.unwrap();f.refresh(app).await;f.pair(app,json!({}),"Atlas","model").await;
   sqlx::query("UPDATE source_items SET access_state='permitted' WHERE source_item_id=$1").bind(item).execute(&f.owner).await.unwrap();
  }
  f.close().await;
 }
}

#[tokio::test]
async fn outcome_prefix_reuse_preserves_shared_nodes_and_dirty_links_and_titles() {
 async fn verify(f:&Fixture,app:bool,outcomes:[Uuid;2],links:[&[Uuid];2],visible_title:Option<Uuid>) {
  for mode in ["all_retained","current"] {for filtered in [false,true] {for query in ["","a","Atlas"] {
   let mut scope:LibraryScope=serde_json::from_value(json!({"graph_ids":[f.graph],"outcome_versions":mode})).unwrap();
   if filtered {scope.service_id=Some(coralswarm_backend::source_origin::ServiceId::new("slack").unwrap());}
   let tenant=f.tenant(app);let mut tx=tenant.begin_member_repeatable_read(f.account).await.unwrap();
   library_read::configure_read_model_proof_tx(&mut tx).await.unwrap();
   let pair=library_read::compare_read_model_tx(&mut tx,&scope,query,40,[None;3]).await.unwrap();
   assert_eq!(pair.path,"model","plane={} app={app} mode={mode} filtered={filtered} query={query} reason={:?}",f.tenant_plane,pair.fallback_reason);
   assert_eq!(pair.live,pair.candidate,"plane={} app={app} mode={mode} filtered={filtered} query={query}",f.tenant_plane);
   let items=pair.candidate["outcomes"]["items"].as_array().unwrap();assert_eq!(items.len(),2);
   for (outcome,expected) in outcomes.into_iter().zip(links) {
    let row=items.iter().find(|row|row["semantic_atom_id"]==json!(outcome)).unwrap();
    let nodes=row["nodes"].as_array().unwrap();
    let ids:Vec<Uuid>=nodes.iter().map(|n|serde_json::from_value(n["node_id"].clone()).unwrap()).collect();
    assert_eq!(ids,expected.iter().take(20).copied().collect::<Vec<_>>());
    assert_eq!(row["nodes_truncated"],json!(expected.len()>20));
    for node in nodes {let id:Uuid=serde_json::from_value(node["node_id"].clone()).unwrap();
     assert_eq!(node["title"],if Some(id)==visible_title {json!("Atlas")}else{Value::Null});
    }
   }
   tx.rollback().await.unwrap();
  }}}
 }
 for plane in [false,true] {for app in [false,true] {
  let f=Fixture::new(plane).await;
  let mut outcomes=Vec::new();
  for (index,title) in ["Prefix anchor A","Prefix anchor B"].into_iter().enumerate() {
   let anchor=f.node(f.graph,title).await;
   let (_,version,_)=f.source(anchor,&format!("shared-prefix-{index}"),"permitted","message").await;
   let outcome:Uuid=sqlx::query_scalar("SELECT semantic_atom_id FROM semantic_atoms WHERE graph_id=$1 AND source_version_id=$2").bind(f.graph).bind(version).fetch_one(&f.owner).await.unwrap();outcomes.push(outcome);
  }
  let outcomes:[Uuid;2]=outcomes.try_into().unwrap();
  let mut keys=Vec::new();for index in 0..23 {keys.push(f.node(f.graph,&format!("Unsupported shared prefix {index}")).await);}keys.sort();
  sqlx::query("DELETE FROM node_semantic_atoms WHERE graph_id=$1 AND semantic_atom_id=ANY($2)").bind(f.graph).bind(outcomes.as_slice()).execute(&f.owner).await.unwrap();
  for (outcome,links) in [(outcomes[0],&keys[..22]),(outcomes[1],&keys[..3])] {
   sqlx::query("INSERT INTO node_semantic_atoms(graph_id,node_id,semantic_atom_id) SELECT $1,k,$3 FROM unnest($2::uuid[]) k").bind(f.graph).bind(links).bind(outcome).execute(&f.owner).await.unwrap();
  }
  f.refresh(app).await;verify(&f,app,outcomes,[&keys[..22],&keys[..3]],None).await;
  // The second outcome shares nodes with the first, but has its own prefix.
  // Dirty replacement must preserve both multiplicities and title selection.
  sqlx::query("DELETE FROM node_semantic_atoms WHERE graph_id=$1 AND semantic_atom_id=$2 AND node_id=$3").bind(f.graph).bind(outcomes[0]).bind(keys[0]).execute(&f.owner).await.unwrap();
  sqlx::query("INSERT INTO node_semantic_atoms(graph_id,node_id,semantic_atom_id) VALUES($1,$2,$3)").bind(f.graph).bind(keys[22]).bind(outcomes[1]).execute(&f.owner).await.unwrap();
  sqlx::query("UPDATE nodes SET title='Atlas' WHERE graph_id=$1 AND node_id=$2").bind(f.graph).bind(keys[1]).execute(&f.owner).await.unwrap();
  let second=[keys[0],keys[1],keys[2],keys[22]];
  verify(&f,app,outcomes,[&keys[1..22],&second],Some(keys[1])).await;
  f.refresh(app).await;verify(&f,app,outcomes,[&keys[1..22],&second],Some(keys[1])).await;
  f.close().await;
 }}
}

#[tokio::test]
async fn permission_context_and_history_floor_fall_back_without_cross_role_reuse() {
 for tenant in [false,true] {
  let f=Fixture::new(tenant).await;let n=f.node(f.graph,"Atlas").await;f.source(n,"floor","permitted","message").await;
  let old=f.node(f.graph,"Oldfloor").await;
  let old_atom:Uuid=sqlx::query_scalar("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,timestamp) VALUES($1,'note','oldfloor','fixture','Oldfloor was present.','2025-01-01') RETURNING atom_id").bind(f.graph).fetch_one(&f.owner).await.unwrap();
  sqlx::query("INSERT INTO node_atoms(node_id,atom_id) VALUES($1,$2)").bind(old).bind(old_atom).execute(&f.owner).await.unwrap();
  // Tenant: the owner-computed cache serves the app-role reader, whose class
  // (account, no floor, corrections visible, reviewed contract) is equal.
  // Control: that reader has no app.user_id, so correction RLS hides every
  // correction from it and its class differs from the owner worker's.
  f.refresh(false).await;
  let (path,reason,_)=f.pair_as(&f.tenant(true),json!({}),"Atlas").await;
  if tenant {assert_eq!((path.as_str(),reason),("model",None));}else{assert_eq!((path.as_str(),reason.as_deref()),("live",Some("context_mismatch")));}
  f.refresh(true).await;let no_floor=f.pair(true,json!({}),"","model").await;
  assert_eq!(no_floor["nodes"]["total"],json!(2));
  if tenant {
   sqlx::query("UPDATE graphs SET kind='team',owner_user_id=NULL WHERE id=$1").bind(f.graph).execute(&f.owner).await.unwrap();
   let team:Uuid=sqlx::query_scalar("INSERT INTO teams(account_id,graph_id,name,created_by) VALUES($1,$2,'Window',$3) RETURNING id").bind(f.account).bind(f.graph).bind(f.user).fetch_one(&f.owner).await.unwrap();
   sqlx::query("INSERT INTO team_members(team_id,user_id,role) VALUES($1,$2,'member')").bind(team).bind(f.user).execute(&f.owner).await.unwrap();
   sqlx::query("INSERT INTO team_seat_grants(account_id,team_id,user_id,source,history_from) VALUES($1,$2,$3,'fixture','2026-01-01')").bind(f.account).bind(team).bind(f.user).execute(&f.owner).await.unwrap();
   let with_floor=f.pair(true,json!({}),"","live").await;
   assert!(with_floor["nodes"]["total"].as_i64().unwrap()<no_floor["nodes"]["total"].as_i64().unwrap(),"positive floor must hide older evidence");
   for app in [false,true] {
    let (path,reason,_)=f.pair_as(&f.tenant(app),json!({}),"").await;
    assert_eq!((path.as_str(),reason.as_deref()),("live",Some("permission_window")),"a floored reader stays on Live (app={app})");
   }
   let mut tx=f.tenant(true).begin_member_repeatable_read(f.account).await.unwrap();
   let old_visible:i64=sqlx::query_scalar("SELECT count(*) FROM atoms WHERE atom_id=$1").bind(old_atom).fetch_one(&mut *tx).await.unwrap();assert_eq!(old_visible,0);
   let raw_guard:bool=sqlx::query_scalar("SELECT library_evidence_raw_edges_safe(ARRAY[$1]::uuid[],ARRAY[]::bigint[],ARRAY[$2]::uuid[])").bind(f.graph).bind(old).fetch_one(&mut *tx).await.unwrap();assert!(!raw_guard,"definercostguard mustrejectthecaller's nonnullhistoryfloor");tx.rollback().await.unwrap();
  }
  f.close().await;
 }
}

/// Builds the content the access-class proofs read: two graph nodes with
/// semantic and legacy support, an unmapped legacy node, and a denied source.
async fn class_fixture(f:&Fixture)->Uuid {
 let atlas=f.node(f.graph,"Atlas").await;f.source(atlas,"class-one","permitted","meeting").await;f.source(atlas,"class-two","permitted","message").await;
 let unmapped=f.node(f.graph,"Atlas unmapped").await;
 let atom:Uuid=sqlx::query_scalar("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,timestamp) VALUES($1,'note','class-unmapped','fixture','Atlas unmapped evidence.',now()) RETURNING atom_id").bind(f.graph).fetch_one(&f.owner).await.unwrap();
 sqlx::query("INSERT INTO node_atoms(node_id,atom_id) VALUES($1,$2)").bind(unmapped).bind(atom).execute(&f.owner).await.unwrap();
 let hidden=f.node(f.graph,"Hidden").await;f.source(hidden,"class-denied","restricted","message").await;
 atlas
}
/// A graph kind change is a permission event: `library_graph_access_purge_signal`
/// sets every permitted source to 'unknown'. Re-grant them (the restricted one
/// stays restricted) so the proofs below read a non-empty graph.
async fn set_kind(f:&Fixture,kind:&str) {
 sqlx::query("UPDATE graphs SET kind=$2,owner_user_id=NULL WHERE id=$1").bind(f.graph).bind(kind).execute(&f.owner).await.unwrap();
 sqlx::query("UPDATE source_items SET access_state='permitted' WHERE graph_id=$1 AND access_state='unknown'").bind(f.graph).execute(&f.owner).await.unwrap();
}
async fn make_team(f:&Fixture,member:Uuid)->Uuid {
 set_kind(f,"team").await;
 let team:Uuid=sqlx::query_scalar("INSERT INTO teams(account_id,graph_id,name,created_by) VALUES($1,$2,'Class team',$3) RETURNING id").bind(f.account).bind(f.graph).bind(f.user).fetch_one(&f.owner).await.unwrap();
 sqlx::query("INSERT INTO team_members(team_id,user_id,role) VALUES($1,$2,'member')").bind(team).bind(member).execute(&f.owner).await.unwrap();
 team
}

/// #1524 blocker 1, proof 1: worker {owner, app} x reader {owner, app} x
/// {member-kind owner, org member, team member with no grant, team member
/// with a NULL grant}: every non-floored reader of the graph is one access
/// class and reads the cache, Live == Model, whatever role computed it. A
/// floored member, a departed member and a non-member stay on Live.
#[tokio::test]
async fn access_class_matrix_admits_every_non_floored_member_across_roles() {
 for worker in [false,true] {
  let f=Fixture::new(true).await;class_fixture(&f).await;f.refresh(worker).await;
  let member=Uuid::new_v4();
  let stored:(Value,Value)=sqlx::query_as("SELECT e.computed_context,l.computed_context FROM library_evidence_state e JOIN library_listing_state l USING(graph_id) WHERE graph_id=$1").bind(f.graph).fetch_one(&f.owner).await.unwrap();
  assert_eq!(stored.0,stored.1);assert_eq!(stored.0["schema_version"],json!(2));assert_eq!(stored.0["policy_digest"],json!(coralswarm_backend::read_access::TENANT_CONTRACT_DIGEST));
  assert!(stored.0.get("role").is_none(),"the class carries no role");
  let mut totals=Vec::new();
  // Member-kind graph, read by its owner.
  for reader in [false,true] {totals.push(f.shapes_as(&f.tenant(reader),"model",None).await["nodes"]["total"].clone());}
  // Org graph, read by another org member.
  set_kind(&f,"org").await;f.refresh(worker).await;
  for reader in [false,true] {totals.push(f.shapes_as(&f.tenant_for(reader,member),"model",None).await["nodes"]["total"].clone());}
  // Team graph: a member with no grant (a legacy seat, full history).
  let team=make_team(&f,member).await;f.refresh(worker).await;
  for reader in [false,true] {totals.push(f.shapes_as(&f.tenant_for(reader,member),"model",None).await["nodes"]["total"].clone());}
  // A NULL grant is full history too, and absorbs any floored grant.
  sqlx::query("INSERT INTO team_seat_grants(account_id,team_id,user_id,source,history_from) VALUES($1,$2,$3,'fixture',NULL),($1,$2,$3,'fixture-floor','2026-01-01')").bind(f.account).bind(team).bind(member).execute(&f.owner).await.unwrap();
  for reader in [false,true] {totals.push(f.shapes_as(&f.tenant_for(reader,member),"model",None).await["nodes"]["total"].clone());}
  // Within each scenario both reader roles get one answer, never an empty
  // one. Across scenarios the kind change is itself a permission event (the
  // purge signal re-derives legacy evidence), so totals may differ there.
  for scenario in totals.chunks(2) {assert!(scenario[0]==scenario[1]&&scenario[0].as_i64().unwrap()>=1,"one class, one answer: {totals:?}");}
  assert!(totals[0].as_i64().unwrap()>=2,"{totals:?}");
  // Floored member: Live, which applies the floor (#1541).
  sqlx::query("DELETE FROM team_seat_grants WHERE team_id=$1 AND history_from IS NULL").bind(team).execute(&f.owner).await.unwrap();
  for reader in [false,true] {f.shapes_as(&f.tenant_for(reader,member),"live",Some("permission_window")).await;}
  // A user outside the team: the graph is not theirs to read at all.
  for reader in [false,true] {f.shapes_as(&f.tenant_for(reader,Uuid::new_v4()),"live",Some("permission_window")).await;}
  f.close().await;
 }
 // Control: graph visibility is the account, but correction visibility
 // follows app.user_id, which the consumer scope never sets. Same-role pairs
 // share a class; cross-role pairs do not, and fall back rather than serve
 // facts computed with corrections the reader cannot see.
 for worker in [false,true] {
  let f=Fixture::new(false).await;class_fixture(&f).await;f.refresh(worker).await;
  for reader in [false,true] {
   if reader==worker {f.shapes_as(&f.tenant(reader),"model",None).await;}
   else {f.shapes_as(&f.tenant(reader),"live",Some("context_mismatch")).await;}
  }
  f.close().await;
 }
}

/// Proof 2: legacy links from a graph node to atoms of a graph the reader
/// cannot see (node_atoms has no graph FK). The owner worker sees the foreign
/// atom; an app-role reader cannot. Model == Live for both readers proves the
/// cached definition is graph-qualified: the foreign row contributes nothing,
/// not even to a count.
#[tokio::test]
async fn foreign_graph_poison_links_never_reach_a_members_cached_facts() {
 let f=Fixture::new(true).await;let atlas=class_fixture(&f).await;
 let foreign_node=f.node(f.foreign,"Atlas foreign").await;
 let (_,_,foreign_atom)=f.source_in_graph(f.foreign,foreign_node,"poison","permitted","meeting").await;
 let poisoned=f.node(f.graph,"Atlas poisoned only").await;
 for node in [atlas,poisoned] {sqlx::query("INSERT INTO node_atoms(node_id,atom_id) VALUES($1,$2)").bind(node).bind(foreign_atom).execute(&f.owner).await.unwrap();}
 f.refresh(false).await;
 // The reader really cannot see the foreign graph's rows.
 let mut tx=f.tenant(true).begin_member_repeatable_read(f.account).await.unwrap();
 let visible:i64=sqlx::query_scalar("SELECT count(*) FROM atoms WHERE atom_id=$1").bind(foreign_atom).fetch_one(&mut *tx).await.unwrap();assert_eq!(visible,0);tx.rollback().await.unwrap();
 let owner=f.shapes_as(&f.tenant(false),"model",None).await;
 let member=f.shapes_as(&f.tenant(true),"model",None).await;
 assert_eq!(owner,member,"one class, one answer, poison or not");
 // Dirty: a fresh poison link observed before the drain, and after it.
 let extra=f.node(f.graph,"Atlas poisoned late").await;
 sqlx::query("INSERT INTO node_atoms(node_id,atom_id) VALUES($1,$2)").bind(extra).bind(foreign_atom).execute(&f.owner).await.unwrap();
 sqlx::query("UPDATE atoms SET content_inline='Atlas poison changed.' WHERE atom_id=$1").bind(foreign_atom).execute(&f.owner).await.unwrap();
 for drained in [false,true] {
  if drained {f.refresh(false).await;}
  for reader in [false,true] {
   let (path,reason,_)=f.pair_as(&f.tenant(reader),json!({}),"Atlas").await;
   assert_eq!((path.as_str(),reason),("model",None),"drained={drained} reader_app={reader}");
  }
 }
 f.close().await;
}

/// Proof 3: revocation is decided in the read's own snapshot. A read opened
/// before the member departs keeps one consistent view (Model, equal to its
/// own Live); the next read is refused, with no cache invalidation at all.
#[tokio::test]
async fn member_revocation_takes_effect_in_the_next_snapshot_with_no_invalidation() {
 for (departure,label) in [("UPDATE team_members SET departing_at=now() WHERE team_id=$1","departing"),("DELETE FROM team_members WHERE team_id=$1","deleted")] {
  let f=Fixture::new(true).await;class_fixture(&f).await;let member=Uuid::new_v4();let team=make_team(&f,member).await;f.refresh(false).await;
  let before:Value=sqlx::query_scalar("SELECT computed_context FROM library_evidence_state WHERE graph_id=$1").bind(f.graph).fetch_one(&f.owner).await.unwrap();
  let mut scope:LibraryScope=serde_json::from_value(json!({"graph_ids":[f.graph]})).unwrap();scope.normalize().unwrap();
  for reader in [false,true] {
   let tenant=f.tenant_for(reader,member);
   let mut tx=tenant.begin_member_repeatable_read(f.account).await.unwrap();
   library_read::configure_read_model_proof_tx(&mut tx).await.unwrap();
   let open=library_read::compare_read_model_tx(&mut tx,&scope,"",2,[None;3]).await.unwrap();
   assert_eq!((open.path.as_str(),open.fallback_reason.as_deref()),("model",None),"{label}");assert_eq!(open.live,open.candidate);
   sqlx::query(departure).bind(team).execute(&f.owner).await.unwrap();
   // The same snapshot still sees the membership: same path, same answer.
   let pinned=library_read::compare_read_model_tx(&mut tx,&scope,"",2,[None;3]).await.unwrap();
   assert_eq!((pinned.path.as_str(),pinned.fallback_reason.as_deref()),("model",None),"{label}");assert_eq!(pinned.live,pinned.candidate);assert_eq!(pinned.live,open.live);
   assert!(pinned.live["nodes"]["total"].as_i64().unwrap()>0);
   tx.rollback().await.unwrap();
   // A new snapshot: refused, under either role.
   f.shapes_as(&tenant,"live",Some("permission_window")).await;
   // Restore the seat for the other reader role.
   sqlx::query("DELETE FROM team_members WHERE team_id=$1").bind(team).execute(&f.owner).await.unwrap();
   sqlx::query("INSERT INTO team_members(team_id,user_id,role) VALUES($1,$2,'member')").bind(team).bind(member).execute(&f.owner).await.unwrap();
  }
  let after:Value=sqlx::query_scalar("SELECT computed_context FROM library_evidence_state WHERE graph_id=$1").bind(f.graph).fetch_one(&f.owner).await.unwrap();
  assert_eq!(before,after,"revocation needs no cache write");
  f.close().await;
 }
}

/// Proof 4: a change to the RLS contract the equivalence was reviewed against
/// sends every reader to Live as `access_contract_changed`, and keeps doing so
/// after the worker re-certifies under the changed contract, until the
/// contract is the reviewed one again and the cache is re-stamped under it.
#[tokio::test]
async fn access_contract_tamper_falls_back_until_the_reviewed_contract_returns() {
 for plane in [true,false] {
  let f=Fixture::new(plane).await;class_fixture(&f).await;f.refresh(false).await;
  let reviewed=if plane {coralswarm_backend::read_access::TENANT_CONTRACT_DIGEST}else{coralswarm_backend::read_access::CONTROL_CONTRACT_DIGEST};
  let digest:String=sqlx::query_scalar("SELECT library_access_contract_digest()").fetch_one(&f.owner).await.unwrap();assert_eq!(digest,reviewed,"a migrated fixture carries the reviewed contract");
  let app_digest:String=sqlx::query_scalar("SELECT library_access_contract_digest()").fetch_one(&f.app).await.unwrap();assert_eq!(app_digest,reviewed,"the app role may compute it too");
  f.shapes_as(&f.tenant(false),"model",None).await;
  let mut tampers=vec![("CREATE POLICY library_tamper ON semantic_atoms AS PERMISSIVE FOR SELECT USING (true)","DROP POLICY library_tamper ON semantic_atoms")];
  if plane {
   let body:String=sqlx::query_scalar("SELECT pg_get_functiondef('tenant_rls_visible_graph_ids()'::regprocedure)").fetch_one(&f.owner).await.unwrap();
   let changed=body.replacen("SELECT g.id","SELECT /* reviewed? */ g.id",1);assert_ne!(body,changed);
   tampers.push((Box::leak(changed.into_boxed_str()),Box::leak(body.into_boxed_str())));
  }
  for (tamper,restore) in tampers {
   sqlx::query(tamper).execute(&f.owner).await.unwrap();
   let changed:String=sqlx::query_scalar("SELECT library_access_contract_digest()").fetch_one(&f.owner).await.unwrap();assert_ne!(changed,reviewed);
   for reader in [false,true] {f.shapes_as(&f.tenant(reader),"live",Some("access_contract_changed")).await;}
   // Re-certified under the changed contract: still not the reviewed one.
   f.refresh(false).await;
   let stored:Value=sqlx::query_scalar("SELECT computed_context FROM library_evidence_state WHERE graph_id=$1").bind(f.graph).fetch_one(&f.owner).await.unwrap();assert_eq!(stored["policy_digest"],json!(changed));
   for reader in [false,true] {f.shapes_as(&f.tenant(reader),"live",Some("access_contract_changed")).await;}
   // Reviewed contract back, cache still stamped with the changed one.
   sqlx::query(restore).execute(&f.owner).await.unwrap();
   let back:String=sqlx::query_scalar("SELECT library_access_contract_digest()").fetch_one(&f.owner).await.unwrap();assert_eq!(back,reviewed);
   f.shapes_as(&f.tenant(false),"live",Some("access_contract_changed")).await;
   f.refresh(false).await;f.shapes_as(&f.tenant(false),"model",None).await;
  }
  f.close().await;
 }
}

/// The one-shot upgrade: facts certified under 0185's schema-1 context are
/// re-stamped to the class without a rebuild when the old context already
/// described a full view (same account, corrections visible). Otherwise the
/// graph rebuilds.
#[tokio::test]
async fn schema_one_context_is_restamped_without_rebuild_only_when_it_was_a_full_view() {
 for plane in [true,false] {for full in [true,false] {
  let f=Fixture::new(plane).await;class_fixture(&f).await;f.refresh(false).await;
  // The state a pre-0206 worker left behind: 0185's fingerprint on both grains.
  let mut tx=f.owner.begin().await.unwrap();
  sqlx::query("SELECT set_config('app.account_id',$1,true)").bind(f.account.to_string()).execute(&mut *tx).await.unwrap();
  let legacy:Value=sqlx::query_scalar("SELECT library_evidence_context($1)").bind(f.graph).fetch_one(&mut *tx).await.unwrap();tx.commit().await.unwrap();
  assert_eq!(legacy["schema_version"],json!(1));assert_eq!(legacy["correction_visible"],json!(true));
  let mut legacy=legacy;if !full {legacy["correction_visible"]=json!(false);}
  sqlx::query("UPDATE library_evidence_state SET computed_context=$2 WHERE graph_id=$1").bind(f.graph).bind(&legacy).execute(&f.owner).await.unwrap();
  sqlx::query("UPDATE library_listing_state SET computed_context=$2 WHERE graph_id=$1").bind(f.graph).bind(&legacy).execute(&f.owner).await.unwrap();
  f.shapes_as(&f.tenant(false),"live",Some("context_mismatch")).await;
  let backfilled:Option<chrono::DateTime<chrono::Utc>>=sqlx::query_scalar("SELECT backfilled_at FROM library_evidence_state WHERE graph_id=$1").bind(f.graph).fetch_one(&f.owner).await.unwrap();
  let mut tx=f.owner.begin().await.unwrap();
  sqlx::query("SELECT set_config('app.account_id',$1,true),set_config('app.tenant_actor',$2,true)").bind(f.account.to_string()).bind(if plane{"system"}else{""}).execute(&mut *tx).await.unwrap();
  library_evidence::refresh_graph_tx(&mut tx,f.graph,Limits{nodes:1,changes:1}).await.unwrap();tx.commit().await.unwrap();
  let state:(Option<i32>,Option<chrono::DateTime<chrono::Utc>>,Value,Value)=sqlx::query_as("SELECT e.ready_version,e.backfilled_at,COALESCE(e.computed_context,'null'),l.computed_context FROM library_evidence_state e JOIN library_listing_state l USING(graph_id) WHERE graph_id=$1").bind(f.graph).fetch_one(&f.owner).await.unwrap();
  if full {
   assert_eq!(state.0,Some(READ_MODEL_VERSION),"no rebuild");assert_eq!(state.1,backfilled,"no rebuild");
   assert_eq!(state.2["schema_version"],json!(2));assert_eq!(state.2,state.3);
   f.shapes_as(&f.tenant(false),"model",None).await;
  } else {
   assert_eq!(state.0,None,"a partial view rebuilds");
   f.refresh(false).await;f.shapes_as(&f.tenant(false),"model",None).await;
  }
  f.close().await;
 }}
}

#[tokio::test]
async fn raw_cross_graph_atom_fanout_falls_back_before_valid_node_resolution() {
 let f=Fixture::new(true).await;let n=f.node(f.graph,"Atlas").await;let (_,_,atom)=f.source(n,"hub","permitted","message").await;
 f.refresh(false).await;
 // node_atoms has no graph FK. Most raw inverse edges intentionally point
 // to another graph; counting only valid resolved nodes would hide the cost.
 // The change set's object-key guard counts them before any key resolves.
 let raw=library_read::read_model::CHANGE_OBJECT_EDGE_LIMIT+20;
 sqlx::query("WITH made AS (INSERT INTO nodes(graph_id,type,title,body) SELECT $1,'entity','Foreign '||n::text,'{}'::jsonb FROM generate_series(1,$3) n RETURNING node_id) INSERT INTO node_atoms(node_id,atom_id) SELECT node_id,$2 FROM made")
  .bind(f.foreign).bind(atom).bind(raw as i32).execute(&f.owner).await.unwrap();
 sqlx::query("UPDATE atoms SET content_inline='Atlas hub.' WHERE atom_id=$1").bind(atom).execute(&f.owner).await.unwrap();
 f.pair(false,json!({}),"Atlas","live").await;
 let mut scope:LibraryScope=serde_json::from_value(json!({"graph_ids":[f.graph]})).unwrap();scope.normalize().unwrap();
 let t=f.tenant(false);let mut tx=t.begin_member_repeatable_read(f.account).await.unwrap();
 library_read::configure_read_model_proof_tx(&mut tx).await.unwrap();
 let pair=library_read::compare_read_model_tx(&mut tx,&scope,"Atlas",2,[None;3]).await.unwrap();
 assert_eq!(pair.path,"live","raw fanout reason={:?}",pair.fallback_reason);
 assert_eq!(pair.fallback_reason.as_deref(),Some("raw_edge_overflow"));assert_eq!(pair.live,pair.candidate);
 sqlx::query("SET LOCAL plan_cache_mode=force_custom_plan").execute(&mut *tx).await.unwrap();
 // Definition three keeps physical raw-cost admission in the dirty stage.
 let sql=format!("EXPLAIN(ANALYZE,BUFFERS,FORMAT JSON) {}",library_read::read_model::definition_dirty_sql(&scope,"Atlas"));
 let plan:Value=sqlx::query_scalar(&sql).bind(&scope.graph_ids).bind(json!({})).bind(None::<Uuid>).bind(None::<Uuid>).bind(None::<Uuid>).bind(None::<String>).bind(Some("%Atlas%"))
  .bind(None::<chrono::DateTime<chrono::Utc>>).bind(None::<Uuid>).bind(None::<chrono::DateTime<chrono::Utc>>).bind(None::<Uuid>).bind(None::<i64>).bind(None::<i64>).bind(None::<Uuid>).bind(3_i64).bind(None::<chrono::DateTime<chrono::Utc>>)
  .fetch_one(&mut *tx).await.unwrap();
 fn find<'a>(v:&'a Value,name:&str)->Option<&'a Value> {
  if v["Subplan Name"].as_str()==Some(name){return Some(v)}
  v["Plans"].as_array().and_then(|p|p.iter().find_map(|c|find(c,name)))
 }
 let root=&plan[0]["Plan"];
 let probe=find(root,"CTE change_object_guard").expect("raw cost probe present");
 assert_eq!(probe["Actual Rows"],json!(1),"one boolean guard result; helpercapsrawphysicalworkbeforeRLS");
 let dirty=find(root,"CTE change_nodes").expect("resolved dirty set present");
 assert_eq!(dirty["Actual Rows"],json!(0),"unsafe raw fanout must not resolve even though only one node is valid");
 tx.rollback().await.unwrap();f.close().await;
}

#[tokio::test]
async fn dependency_mutations_are_exact_before_and_after_each_drain() {
 for plane in [false,true] {for app in [false,true] {
  let f=Fixture::new(plane).await;let node=f.node(f.graph,"Atlas").await;
  let (item,version,atom)=f.source(node,"matrix","permitted","meeting").await;
  let outcome:Uuid=sqlx::query_scalar("SELECT semantic_atom_id FROM semantic_atoms WHERE source_version_id=$1").bind(version).fetch_one(&f.owner).await.unwrap();
  let legacy=f.node(f.graph,"Corrected reef").await;
  let unbound:Uuid=sqlx::query_scalar("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,timestamp) VALUES($1,'note','legacy','fixture','Unrelated identity text.','2025-01-01') RETURNING atom_id").bind(f.graph).fetch_one(&f.owner).await.unwrap();
  sqlx::query("INSERT INTO node_atoms(node_id,atom_id) VALUES($1,$2)").bind(legacy).bind(unbound).execute(&f.owner).await.unwrap();
  let mut tx=f.owner.begin().await.unwrap();
  sqlx::query("SELECT set_config('app.account_id',$1,true),set_config('app.user_id',$2,true),set_config('app.tenant_actor','user',true)").bind(f.account.to_string()).bind(f.user.to_string()).execute(&mut *tx).await.unwrap();
  sqlx::query("INSERT INTO reef_identity_corrections(graph_id,node_id,label,recorded_by,support_atom_id,support_kind,content_revision,input_sha256) SELECT graph_id,$2,'Corrected reef',$3,atom_id,'legacy',content_revision,reef_identity_atom_fingerprint(a) FROM atoms a WHERE atom_id=$1").bind(unbound).bind(legacy).bind(f.user).execute(&mut *tx).await.unwrap();
  tx.commit().await.unwrap();
  f.refresh(app).await;f.pair(app,json!({}),"Corrected","model").await;
  let mutations=vec![
   ("node body/work flag",format!("UPDATE nodes SET entity_type='project',entity_type_sticky=true,body='{{\"synthetic_hub\":false}}' WHERE node_id='{node}'")),
   ("node title",format!("UPDATE nodes SET title='Changed Atlas' WHERE node_id='{node}'")),
   ("node participant title support",format!("UPDATE atoms SET participants=ARRAY['Changed Atlas'] WHERE atom_id='{atom}'")),
   ("atom content",format!("UPDATE atoms SET content_inline='Changed Atlas deploy evidence.' WHERE atom_id='{atom}'")),
   ("retract",format!("UPDATE atoms SET evidence_retracted=true WHERE atom_id='{atom}'")),
   ("unretract",format!("UPDATE atoms SET evidence_retracted=false WHERE atom_id='{atom}'")),
   ("semantic entities",format!("UPDATE semantic_atoms SET entities='[{{\"mention\":\"Changed Atlas\",\"type\":\"project\"}}]' WHERE semantic_atom_id='{outcome}'")),
   ("semantic statement",format!("UPDATE semantic_atoms SET statement='Changed Atlas release.' WHERE semantic_atom_id='{outcome}'")),
   ("version capture time",format!("UPDATE source_versions SET captured_at='2026-02-02' WHERE version_id='{version}'")),
   ("version independence",format!("UPDATE source_versions SET source_metadata=source_metadata||'{{\"independence\":\"derived_copy\"}}' WHERE version_id='{version}'")),
   ("legacy baseline",format!("UPDATE source_versions SET is_legacy_baseline=true WHERE version_id='{version}'")),
   ("synthetic metadata",format!("UPDATE source_versions SET source_metadata=source_metadata||'{{\"synthetic_demo\":true}}' WHERE version_id='{version}'")),
   ("metadata-only pending recapture",format!("WITH created AS(INSERT INTO source_versions(graph_id,source_item_id,source_id,source_type,provider_namespace,origin,raw_inline,content_sha256,source_metadata,captured_at,extraction_status) SELECT graph_id,source_item_id,source_id,source_type,provider_namespace,origin,raw_inline,content_sha256,source_metadata,'2026-03-01','pending' FROM source_versions WHERE version_id='{version}' RETURNING source_item_id,version_id) UPDATE source_items s SET current_version_id=c.version_id FROM created c WHERE s.source_item_id=c.source_item_id")),
   ("changed-content pending recapture",format!("WITH created AS(INSERT INTO source_versions(graph_id,source_item_id,source_id,source_type,provider_namespace,origin,raw_inline,content_sha256,source_metadata,captured_at,extraction_status) SELECT graph_id,source_item_id,source_id,source_type,provider_namespace,origin,'Different changed content.',repeat('a',64),source_metadata,'2026-04-01','pending' FROM source_versions WHERE version_id='{version}' RETURNING source_item_id,version_id) UPDATE source_items s SET current_version_id=c.version_id FROM created c WHERE s.source_item_id=c.source_item_id")),
   ("source origin",format!("UPDATE source_items SET origin=origin||'{{\"content_kind\":\"work_session\"}}' WHERE source_item_id='{item}'")),
   ("source permission loss",format!("UPDATE source_items SET access_state='restricted' WHERE source_item_id='{item}'")),
   ("source permission resurrection",format!("UPDATE source_items SET access_state='permitted' WHERE source_item_id='{item}'")),
   ("chunk removal",format!("DELETE FROM source_version_chunks WHERE source_version_id='{version}' AND atom_id='{atom}'")),
   ("chunk recreation",format!("INSERT INTO source_version_chunks(graph_id,source_version_id,atom_id) VALUES('{}','{version}','{atom}')",f.graph)),
   ("semantic unlink",format!("DELETE FROM node_semantic_atoms WHERE node_id='{node}' AND semantic_atom_id='{outcome}'")),
   ("semantic relink",format!("INSERT INTO node_semantic_atoms(graph_id,node_id,semantic_atom_id) VALUES('{}','{node}','{outcome}')",f.graph)),
   ("legacy unlink",format!("DELETE FROM node_atoms WHERE node_id='{node}' AND atom_id='{atom}'")),
   ("legacy relink",format!("INSERT INTO node_atoms(node_id,atom_id) VALUES('{node}','{atom}')")),
   ("correction retirement",format!("UPDATE reef_identity_corrections SET state='superseded' WHERE node_id='{legacy}'")),
   ("unmapped derived exclusion",format!("UPDATE atoms SET derived_copy_bound=true WHERE atom_id='{unbound}'")),
   ("outcome deletion",format!("DELETE FROM semantic_atoms WHERE semantic_atom_id='{outcome}'")),
   ("source deletion cascade",format!("DELETE FROM source_items WHERE source_item_id='{item}'")),
   ("node deletion cascade",format!("DELETE FROM nodes WHERE node_id='{node}'")),
  ];
  for (label,sql) in mutations {
   sqlx::query(&sql).execute(&f.owner).await.unwrap_or_else(|e|panic!("{label}: {e}"));
   for drained in [false,true] {
    if drained {f.refresh(app).await;}
    for mode in ["all_retained","current"] {
     for extra in [json!({"outcome_versions":mode}),json!({"outcome_versions":mode,"collection":"meetings"}),json!({"outcome_versions":mode,"service_id":"slack"})] {
      for query in ["","Atlas","Corrected"] {f.pair(app,extra.clone(),query,if label=="outcome deletion"&&!drained{"live"}else{"model"}).await;}
     }
    }
   }
   eprintln!("mutation proof plane={plane} app={app} case={label}");
  }
  f.close().await;
 }}
}

#[tokio::test]
async fn paired_readers_hold_one_snapshot_across_concurrent_permission_loss() {
 for plane in [false,true] {
  let f=Fixture::new(plane).await;let node=f.node(f.graph,"Atlas").await;let (item,_,_)=f.source(node,"snapshot","permitted","message").await;
  f.refresh(true).await;
  let mut scope:LibraryScope=serde_json::from_value(json!({"graph_ids":[f.graph]})).unwrap();scope.normalize().unwrap();
  let tenant=f.tenant(true);let mut tx=tenant.begin_member_repeatable_read(f.account).await.unwrap();
  // The scoped settings SELECT after BEGIN establishes the repeatable snapshot.
  library_read::configure_read_model_proof_tx(&mut tx).await.unwrap();
  sqlx::query("UPDATE source_items SET access_state='restricted' WHERE source_item_id=$1").bind(item).execute(&f.owner).await.unwrap();
  let before=library_read::compare_read_model_tx(&mut tx,&scope,"Atlas",2,[None;3]).await.unwrap();
  assert_eq!(before.path,"model");assert_eq!(before.live,before.candidate);assert!(before.live["nodes"]["total"].as_i64().unwrap()>0);
  tx.rollback().await.unwrap();
  let after=f.pair(true,json!({}),"Atlas","model").await;
  assert_eq!(after["nodes"]["total"],json!(0));
  f.close().await;
 }
}

/// Admission counts DISTINCT keys (#1524 blocker 2): a backlog the raw-row
/// budget refused (change_overflow at 129 log rows, dirty_node_overflow at
/// 65 nodes) is an exact Model overlay now, and the physical input budget
/// still sends an expensive node to exact Live.
#[tokio::test]
async fn readiness_and_distinct_key_budgets_choose_exact_paths() {
 use library_read::read_model::{CHANGE_NODE_LIMIT,RAW_EDGE_LIMIT};
 let f=Fixture::new(true).await;let node=f.node(f.graph,"Atlas").await;let (_,_,atom)=f.source(node,"budgets","permitted","message").await;
 let mut scope:LibraryScope=serde_json::from_value(json!({"graph_ids":[f.graph]})).unwrap();scope.normalize().unwrap();
 async fn reason(f:&Fixture,scope:&LibraryScope,expected:Option<&str>){
  let mut tx=f.tenant(false).begin_member_repeatable_read(f.account).await.unwrap();
  library_read::configure_read_model_proof_tx(&mut tx).await.unwrap();
  let pair=library_read::compare_read_model_tx(&mut tx,scope,"Atlas",2,[None;3]).await.unwrap();
  assert_eq!(pair.path,if expected.is_some(){"live"}else{"model"},"budget subcase {expected:?}");assert_eq!(pair.fallback_reason.as_deref(),expected,"budget subcase {expected:?}");
  assert_eq!(pair.live,pair.candidate);assert!(pair.freshness.is_none());tx.rollback().await.unwrap();
 }
 reason(&f,&scope,Some("not_ready")).await;f.refresh(false).await;
 sqlx::query("UPDATE library_evidence_state SET ready_version=1 WHERE graph_id=$1").bind(f.graph).execute(&f.owner).await.unwrap();
 reason(&f,&scope,Some("not_ready")).await;
 sqlx::query("UPDATE library_evidence_state SET ready_version=$2 WHERE graph_id=$1").bind(f.graph).bind(READ_MODEL_VERSION).execute(&f.owner).await.unwrap();
 // 140 nodes, each logged three times: 420 log rows (the raw-row budget was
 // 128) but 140 distinct dirty nodes, within the node budget.
 sqlx::query("INSERT INTO nodes(graph_id,type,title,body) SELECT $1,'entity','Budget '||n::text,'{}'::jsonb FROM generate_series(1,140) n").bind(f.graph).execute(&f.owner).await.unwrap();
 for round in 0..3 {
  let updated=sqlx::query("UPDATE nodes SET title=title||$2 WHERE graph_id=$1 AND title LIKE 'Budget %'").bind(f.graph).bind(format!(" r{round}")).execute(&f.owner).await.unwrap().rows_affected();
  assert_eq!(updated,140);
 }
 let (rows,distinct):(i64,i64)=sqlx::query_as("SELECT count(*),count(DISTINCT node_id) FROM library_evidence_changes WHERE graph_id=$1 AND node_id IS NOT NULL").bind(f.graph).fetch_one(&f.owner).await.unwrap();
 assert_eq!((rows,distinct),(420,140),"three rows per distinct node");
 assert!((distinct as usize)<=CHANGE_NODE_LIMIT);
 reason(&f,&scope,None).await;f.refresh(false).await;
 // One legitimate atom fans out to 66 valid nodes (over the old 64-node
 // budget): one object key, resolved once, overlaid exactly.
 let linked=sqlx::query("INSERT INTO node_atoms(node_id,atom_id) SELECT node_id,$2 FROM nodes WHERE graph_id=$1 AND title LIKE 'Budget %' ORDER BY node_id LIMIT 65").bind(f.graph).bind(atom).execute(&f.owner).await.unwrap().rows_affected();
 assert_eq!(linked,65,"fanout seed must create 65 new links");
 f.refresh(false).await;
 let updated:Vec<Uuid>=sqlx::query_scalar("UPDATE atoms SET participants=ARRAY['Atlas'] WHERE atom_id=$1 AND participants IS DISTINCT FROM ARRAY['Atlas'] RETURNING atom_id").bind(atom).fetch_all(&f.owner).await.unwrap();
 assert_eq!(updated,vec![atom],"fanout seed must actually update its atom");
 let keys:(i64,i64)=sqlx::query_as("SELECT count(*),count(*) FILTER(WHERE atom_id=$2 AND node_id IS NULL AND source_item_id IS NULL AND semantic_atom_id IS NULL) FROM library_evidence_changes WHERE graph_id=$1").bind(f.graph).bind(atom).fetch_one(&f.owner).await.unwrap();
 assert_eq!(keys,(1,1),"fanout probe must contain only its one actual atom key");
 reason(&f,&scope,None).await;f.refresh(false).await;
 // A single node with more raw inputs than the node raw-edge budget is not
 // recomputed: like a hub, its node rows are served stale for a bounded
 // window (founder decision 2026-10-28), then exact Live. This also covers
 // an unbound legacy hub's input cost.
 let extra=RAW_EDGE_LIMIT as i32+20;
 let linked=sqlx::query("WITH made AS(INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,timestamp) SELECT $1,'note','input-'||n::text,'fixture','Atlas input',now() FROM generate_series(1,$3) n RETURNING atom_id) INSERT INTO node_atoms(node_id,atom_id) SELECT $2,atom_id FROM made").bind(f.graph).bind(node).bind(extra).execute(&f.owner).await.unwrap().rows_affected();
 assert_eq!(linked,extra as u64);
 f.refresh(false).await;
 let updated:Vec<Uuid>=sqlx::query_scalar("UPDATE nodes SET title='Atlas hub' WHERE node_id=$1 AND title IS DISTINCT FROM 'Atlas hub' RETURNING node_id").bind(node).fetch_all(&f.owner).await.unwrap();
 assert_eq!(updated,vec![node],"raw-input probe must actually change the captured node title");
 let keys:(i64,i64)=sqlx::query_as("SELECT count(*),count(*) FILTER(WHERE node_id=$2 AND atom_id IS NULL AND source_item_id IS NULL AND semantic_atom_id IS NULL) FROM library_evidence_changes WHERE graph_id=$1").bind(f.graph).bind(node).fetch_one(&f.owner).await.unwrap();
 assert_eq!(keys,(1,1),"raw-input probe must contain only its one actual node key");
 let mut tx=f.tenant(false).begin_member_repeatable_read(f.account).await.unwrap();
 library_read::configure_read_model_proof_tx(&mut tx).await.unwrap();
 let pair=library_read::compare_read_model_tx(&mut tx,&scope,"Atlas",2,[None;3]).await.unwrap();tx.rollback().await.unwrap();
 assert_eq!((pair.path.as_str(),pair.fallback_reason.as_deref()),("model",None),"a big node is served stale, not Live");
 let freshness=pair.freshness.clone().expect("the stale serve is marked");
 assert_eq!((freshness["state"].clone(),freshness["components"].clone()),(json!("updating"),json!(["nodes"])));
 assert_eq!(library_read::exact_components(&pair.live),library_read::exact_components(&pair.candidate),"everything but the nodes stays exact");
 // Past the window: exact Live, with the big node's own reason.
 sqlx::query("UPDATE library_evidence_changes SET created_at=created_at-make_interval(secs=>$2) WHERE graph_id=$1").bind(f.graph).bind((library_read::read_model::HUB_STALE_SECS+1) as f64).execute(&f.owner).await.unwrap();
 reason(&f,&scope,Some("raw_edge_overflow")).await;
 f.close().await;
}

#[tokio::test]
async fn multigraph_overlay_keeps_corrupt_legacy_links_inside_exact_pairs() {
 for plane in [false,true] {
  let f=Fixture::new(plane).await;sqlx::query("UPDATE graphs SET owner_user_id=$2 WHERE id=$1").bind(f.foreign).bind(f.user).execute(&f.owner).await.unwrap();
  let atlas=f.node(f.graph,"Atlas").await;let (_,_,atom)=f.source(atlas,"multigraph","permitted","message").await;
  let beta=f.node(f.foreign,"Beta").await;
  let beta_atom:Uuid=sqlx::query_scalar("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,timestamp) VALUES($1,'note','beta','fixture','Beta evidence.',now()) RETURNING atom_id").bind(f.foreign).fetch_one(&f.owner).await.unwrap();
  sqlx::query("INSERT INTO node_atoms(node_id,atom_id) VALUES($1,$2),($1,$3)").bind(beta).bind(beta_atom).bind(atom).execute(&f.owner).await.unwrap();
  for app in [false,true] {
   // Readiness includes both evidence and listing generations and queues.
   for graph in [f.graph,f.foreign] {f.refresh_graph(app,graph).await;}
   // Only graphA's atom changes; the malformed edge must neither create an
   // out-of-pair item grain nor erase graphB's legitimate unmapped evidence.
   sqlx::query("UPDATE atoms SET participants=ARRAY['Atlas'] WHERE atom_id=$1").bind(atom).execute(&f.owner).await.unwrap();
   for mode in ["all_retained","current"] {for query in ["","Atlas","Beta"] {for filtered in [false,true] {
    let mut value=json!({"graph_ids":[f.graph,f.foreign],"outcome_versions":mode});if filtered{value["service_id"]=json!("slack");}
    let mut scope:LibraryScope=serde_json::from_value(value).unwrap();scope.normalize().unwrap();
    let mut tx=f.tenant(app).begin_member_repeatable_read(f.account).await.unwrap();library_read::configure_read_model_proof_tx(&mut tx).await.unwrap();let pair=library_read::compare_read_model_tx(&mut tx,&scope,query,40,[None;3]).await.unwrap();
    eprintln!("multi plane={plane} app={app} mode={mode} filtered={filtered} query={query} path={} reason={:?}",pair.path,pair.fallback_reason);
    assert_eq!(pair.path,"model","reason={:?}",pair.fallback_reason);assert_eq!(pair.fallback_reason,None);
    assert_eq!(pair.live,pair.candidate,"multi plane={plane} app={app} mode={mode} filtered={filtered} query={query}");tx.rollback().await.unwrap();
    scope.node_id=Some(beta);let mut tx=f.tenant(app).begin_member_repeatable_read(f.account).await.unwrap();library_read::configure_read_model_proof_tx(&mut tx).await.unwrap();
    let narrow=library_read::compare_read_model_tx(&mut tx,&scope,query,40,[None;3]).await.unwrap();assert_eq!(narrow.path,"live");assert_eq!(narrow.live,narrow.candidate);tx.rollback().await.unwrap();
   }}}
  }
  f.close().await;
 }
}

#[tokio::test]
async fn hidden_cross_graph_raw_edges_are_bounded_under_actual_app_rls() {
 let f=Fixture::new(true).await;let n=f.node(f.graph,"Atlas").await;let (_,_,atom)=f.source(n,"hidden-hub","permitted","message").await;
 sqlx::query("WITH made AS(INSERT INTO nodes(graph_id,type,title,body) SELECT $1,'entity','Hidden raw '||n::text,'{}'::jsonb FROM generate_series(1,20001) n RETURNING node_id) INSERT INTO node_atoms(node_id,atom_id) SELECT node_id,$2 FROM made").bind(f.foreign).bind(atom).execute(&f.owner).await.unwrap();
 f.refresh(true).await;
 sqlx::query("UPDATE atoms SET content_inline='Atlas modified.' WHERE atom_id=$1").bind(atom).execute(&f.owner).await.unwrap();
 let mut scope:LibraryScope=serde_json::from_value(json!({"graph_ids":[f.graph]})).unwrap();scope.normalize().unwrap();let mut tx=f.tenant(true).begin_member_repeatable_read(f.account).await.unwrap();
 library_read::configure_read_model_proof_tx(&mut tx).await.unwrap();
 let sql=format!("EXPLAIN(ANALYZE,BUFFERS,FORMAT JSON) {}",library_read::read_model_sql_for_test(&scope));
 let plan:Value=sqlx::query_scalar(&sql).bind(&scope.graph_ids).bind(json!({})).bind(None::<Uuid>).bind(None::<Uuid>).bind(None::<Uuid>).bind(None::<String>).bind(Some("%Atlas%"))
 .bind(None::<chrono::DateTime<chrono::Utc>>).bind(None::<Uuid>).bind(None::<chrono::DateTime<chrono::Utc>>).bind(None::<Uuid>).bind(None::<i64>).bind(None::<i64>).bind(None::<Uuid>).bind(3_i64).bind(None::<chrono::DateTime<chrono::Utc>>).fetch_one(&mut *tx).await.unwrap();
 std::fs::write("/private/tmp/rm4-app-hidden-raw-fanout-plan.json",serde_json::to_vec_pretty(&plan).unwrap()).unwrap();
 let pair=library_read::compare_read_model_tx(&mut tx,&scope,"Atlas",2,[None;3]).await.unwrap();
 eprintln!("hidden raw cost probe path={} reason={:?} time_ms={}",pair.path,pair.fallback_reason,plan[0]["Execution Time"]);
 assert_eq!(pair.live,pair.candidate);assert_eq!(pair.path,"live","actualapp RLS must not hide20001physicaledges fromcostadmission");tx.rollback().await.unwrap();f.close().await;
}

#[tokio::test]
async fn raw_cost_helper_is_boolean_bounded_and_keeps_caller_graph_admission() {
 for plane in [false,true] {
  let f=Fixture::new(plane).await;let node=f.node(f.graph,"Atlas").await;let (_,_,atom)=f.source(node,"guard-admission","permitted","message").await;f.refresh(true).await;
  sqlx::query("UPDATE atoms SET content_inline='Atlas guard proof.' WHERE atom_id=$1").bind(atom).execute(&f.owner).await.unwrap();
  let change:i64=sqlx::query_scalar("SELECT id FROM library_evidence_changes WHERE graph_id=$1 AND atom_id=$2 ORDER BY id DESC LIMIT 1").bind(f.graph).bind(atom).fetch_one(&f.owner).await.unwrap();
  let mut tx=f.tenant(true).begin_member_repeatable_read(f.account).await.unwrap();
  async fn safe(tx:&mut sqlx::Transaction<'_,sqlx::Postgres>,graphs:Vec<Uuid>,changes:Vec<i64>,nodes:Vec<Uuid>)->bool {
   sqlx::query_scalar("SELECT library_evidence_raw_edges_safe($1,$2,$3)").bind(graphs).bind(changes).bind(nodes).fetch_one(&mut **tx).await.unwrap()
  }
  sqlx::query("SELECT set_config('enable_nestloop','off',true),set_config('enable_mergejoin','off',true),set_config('enable_seqscan','on',true),set_config('enable_bitmapscan','on',true),set_config('enable_indexscan','off',true),set_config('enable_indexonlyscan','off',true),set_config('jit','on',true)").execute(&mut *tx).await.unwrap();
  let flags="SELECT ARRAY[current_setting('enable_nestloop'),current_setting('enable_mergejoin'),current_setting('enable_seqscan'),current_setting('enable_bitmapscan'),current_setting('enable_indexscan'),current_setting('enable_indexonlyscan'),current_setting('jit')]";
  let before:Vec<String>=sqlx::query_scalar(flags).fetch_one(&mut *tx).await.unwrap();
  assert!(safe(&mut tx,vec![f.graph],vec![change],vec![]).await);
  let after:Vec<String>=sqlx::query_scalar(flags).fetch_one(&mut *tx).await.unwrap();assert_eq!(before,after,"function-local rawguard settings mustrestoreallcallerplannerflags");
  let role:String=sqlx::query_scalar("SELECT current_user::text").fetch_one(&mut *tx).await.unwrap();assert_eq!(role,if plane{"coralswarm_tenant_app"}else{"coralswarm_app"});
  let rls:String=sqlx::query_scalar("SELECT current_setting('row_security')").fetch_one(&mut *tx).await.unwrap();assert_eq!(rls,"on","definercostprobe cannotleaveRLSdisabled initscaller");
  assert!(!safe(&mut tx,vec![f.graph;33],vec![],vec![]).await);
  assert!(!safe(&mut tx,vec![f.graph],vec![change;129],vec![]).await);
  assert!(!safe(&mut tx,vec![f.graph],vec![],vec![node;65]).await);
  assert!(!safe(&mut tx,vec![f.graph],vec![change],vec![node]).await);
  let null_input:bool=sqlx::query_scalar("SELECT library_evidence_raw_edges_safe(ARRAY[$1,NULL]::uuid[],ARRAY[]::bigint[],ARRAY[]::uuid[])").bind(f.graph).fetch_one(&mut *tx).await.unwrap();assert!(!null_input);
  if plane {
   assert!(!safe(&mut tx,vec![f.foreign],vec![],vec![]).await,"sameaccount privateforeigngraph must stayhidden underdefiner");
   sqlx::query("SELECT set_config('app.user_id','',true)").execute(&mut *tx).await.unwrap();assert!(!safe(&mut tx,vec![f.graph],vec![change],vec![]).await);
   sqlx::query("SELECT set_config('app.user_id',$1,true)").bind(Uuid::new_v4().to_string()).execute(&mut *tx).await.unwrap();assert!(!safe(&mut tx,vec![f.graph],vec![change],vec![]).await);
   sqlx::query("SELECT set_config('app.user_id',$1,true)").bind(f.user.to_string()).execute(&mut *tx).await.unwrap();
  }
  sqlx::query("SELECT set_config('app.account_id',$1,true)").bind(Uuid::new_v4().to_string()).execute(&mut *tx).await.unwrap();assert!(!safe(&mut tx,vec![f.graph],vec![change],vec![]).await);tx.rollback().await.unwrap();
  let public_execute:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_proc p CROSS JOIN LATERAL aclexplode(p.proacl) a WHERE p.oid='public.library_evidence_raw_edges_safe(uuid[],bigint[],uuid[])'::regprocedure AND a.grantee=0 AND a.privilege_type='EXECUTE')").fetch_one(&f.owner).await.unwrap();assert!(!public_execute);
  f.close().await;
 }
}

#[tokio::test]
async fn moved_atom_old_graph_key_falls_back_before_hidden_inverse_fanout() {
 // Exercise the served settings and retain a controlled shorter ceiling.
 // Each case builds a fresh fixture so its OLD-graph change is still fresh.
 for shorter_ceiling in [true,false] {
 let f=Fixture::new(true).await;let node=f.node(f.graph,"Atlas").await;
 let atom:Uuid=sqlx::query_scalar("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,timestamp) VALUES($1,'note','move-guard','fixture','Atlas evidence.',now()) RETURNING atom_id").bind(f.graph).fetch_one(&f.owner).await.unwrap();
 sqlx::query("INSERT INTO node_atoms(node_id,atom_id) VALUES($1,$2)").bind(node).bind(atom).execute(&f.owner).await.unwrap();
 sqlx::query("WITH made AS(INSERT INTO nodes(graph_id,type,title,body) SELECT $1,'entity','Moved hidden '||n::text,'{}'::jsonb FROM generate_series(1,20001) n RETURNING node_id) INSERT INTO node_atoms(node_id,atom_id) SELECT node_id,$2 FROM made").bind(f.foreign).bind(atom).execute(&f.owner).await.unwrap();
 f.refresh(true).await;
 sqlx::query("UPDATE atoms SET graph_id=$2 WHERE atom_id=$1").bind(atom).bind(f.foreign).execute(&f.owner).await.unwrap();
 let mut scope:LibraryScope=serde_json::from_value(json!({"graph_ids":[f.graph]})).unwrap();scope.normalize().unwrap();let mut tx=f.tenant(true).begin_member_repeatable_read(f.account).await.unwrap();
 library_read::configure_read_model_proof_tx(&mut tx).await.unwrap();
 if shorter_ceiling {sqlx::query("SELECT set_config('statement_timeout','10s',true)").execute(&mut *tx).await.unwrap();}
 let ceiling:String=sqlx::query_scalar("SELECT current_setting('statement_timeout')").fetch_one(&mut *tx).await.unwrap();
 assert_eq!(ceiling,if shorter_ceiling{"10s"}else{library_read::SEARCH_STATEMENT_TIMEOUT});
 let started=std::time::Instant::now();let result=library_read::compare_read_model_tx(&mut tx,&scope,"Atlas",2,[None;3]).await;
 match &result {Ok(pair)=>println!("moved-atom served ceiling={ceiling} elapsed_ms={:.3} path={} reason={:?}",started.elapsed().as_secs_f64()*1000.0,pair.path,pair.fallback_reason),Err(error)=>println!("moved-atom served ceiling={ceiling} elapsed_ms={:.3} error={error:?}",started.elapsed().as_secs_f64()*1000.0)}
 let pair=result.unwrap();assert_eq!(pair.path,"live");assert_eq!(pair.fallback_reason.as_deref(),Some("raw_edge_overflow"));assert_eq!(pair.live,pair.candidate);assert_eq!(pair.live["nodes"]["total"],json!(0));
 let guard:bool=sqlx::query_scalar("SELECT library_evidence_raw_edges_safe(ARRAY[$1]::uuid[],ARRAY(SELECT id FROM library_evidence_changes WHERE graph_id=$1),ARRAY[]::uuid[])").bind(f.graph).fetch_one(&mut *tx).await.unwrap();assert!(!guard,"OLDgraphkey cannot become silentlysafe when currentatom moved");
 tx.rollback().await.unwrap();
 // Outcome graph movement cannot retain old graph links: its version/graph
 // and node-semantic graph FKs reject that counterexample atomically.
 let (_,version,_)=f.source(node,"outcome-move-fk","permitted","message").await;
 let error=sqlx::query("UPDATE semantic_atoms SET graph_id=$2 WHERE source_version_id=$1").bind(version).bind(f.foreign).execute(&f.owner).await.unwrap_err();
 assert_eq!(error.as_database_error().and_then(|e|e.code()).as_deref(),Some("23503"));
 f.close().await;
 }
}

#[tokio::test]
async fn comparison_refuses_a_transaction_without_the_served_settings() {
 let f=Fixture::new(true).await;let node=f.node(f.graph,"Atlas").await;f.source(node,"settings-guard","permitted","message").await;f.refresh(true).await;
 let mut scope:LibraryScope=serde_json::from_value(json!({"graph_ids":[f.graph]})).unwrap();scope.normalize().unwrap();
 let mut tx=f.tenant(true).begin_member_repeatable_read(f.account).await.unwrap();
 let jit:String=sqlx::query_scalar("SELECT current_setting('jit')").fetch_one(&mut *tx).await.unwrap();assert_eq!(jit,"on","this engine must default to jit on for the guard to mean anything");
 let error=match library_read::compare_read_model_tx(&mut tx,&scope,"Atlas",2,[None;3]).await {Err(e)=>e,Ok(_)=>panic!("comparison ran without the served settings")};
 assert!(matches!(&error,library_read::ModelComparisonError::Context(e) if e.to_string().contains("served search settings")),"{error:?}");
 tx.rollback().await.unwrap();
 let mut tx=f.tenant(true).begin_member_repeatable_read(f.account).await.unwrap();library_read::configure_read_model_proof_tx(&mut tx).await.unwrap();
 let pair=library_read::compare_read_model_tx(&mut tx,&scope,"Atlas",2,[None;3]).await.unwrap();assert_eq!(pair.path,"model");assert_eq!(pair.live,pair.candidate);tx.rollback().await.unwrap();
 f.close().await;
}

/// Both change channels, counted as distinct keys, and the worker heartbeat
/// that replaced the 60 s change age (#1524 blocker 2). Log rows are valid
/// same-item obligations: duplicates never raise a budget. Age alone is not
/// a fallback any more; a change no refresh has followed past the heartbeat
/// window is.
#[tokio::test]
async fn definition_three_counts_distinct_keys_on_both_channels_and_refuses_a_stalled_worker() {
 use library_read::read_model::{CHANGE_ITEM_LIMIT,WORKER_HEARTBEAT_SECS};
 let stalled=WORKER_HEARTBEAT_SECS+5;
 for plane in [false,true] {for app in [false,true] {
  let f=Fixture::new(plane).await;let node=f.node(f.graph,"Atlas").await;
  let (item,_,_)=f.source(node,"combined-log-boundary","permitted","meeting").await;f.refresh(app).await;
  let scope:LibraryScope=serde_json::from_value(json!({"graph_ids":[f.graph]})).unwrap();
  // (node rows, listing rows of the one item, extra distinct items, change
  // age seconds, last refresh age seconds, expected reason)
  for (nodes,items,extra,age,refreshed,expected) in [
   (0_i64,129_i64,0_i64,0_i64,0_i64,None),(65,64,0,0,0,None),(64,64,0,0,0,None),
   // A minute-old change with a live worker: the old change_stale case.
   (0,1,0,61,0,None),
   // Older than the heartbeat window but refreshed since it was logged:
   // the worker is draining, the overlay is exact.
   (0,1,0,stalled,0,None),
   // No refresh committed since the change was logged, past the window.
   (0,1,0,stalled,stalled+10,Some("change_stale")),
   // One more distinct item than the item budget, however few rows.
   (0,1,CHANGE_ITEM_LIMIT as i64,0,0,Some("dirty_item_overflow")),
  ] {
   let mut write=f.owner.begin().await.unwrap();
   sqlx::query("DELETE FROM library_evidence_changes WHERE graph_id=$1").bind(f.graph).execute(&mut *write).await.unwrap();
   sqlx::query("DELETE FROM library_listing_changes WHERE graph_id=$1").bind(f.graph).execute(&mut *write).await.unwrap();
   sqlx::query("INSERT INTO library_evidence_changes(graph_id,source_item_id,created_at) SELECT $1,$2,statement_timestamp()-make_interval(secs=>$4) FROM generate_series(1,$3)").bind(f.graph).bind(item).bind(nodes).bind(age as f64).execute(&mut *write).await.unwrap();
   sqlx::query("INSERT INTO library_listing_changes(graph_id,source_item_id,created_at) SELECT $1,$2,statement_timestamp()-make_interval(secs=>$4) FROM generate_series(1,$3)").bind(f.graph).bind(item).bind(items).bind(age as f64).execute(&mut *write).await.unwrap();
   sqlx::query("INSERT INTO library_listing_changes(graph_id,source_item_id) SELECT $1,gen_random_uuid() FROM generate_series(1,$2)").bind(f.graph).bind(extra).execute(&mut *write).await.unwrap();
   sqlx::query("UPDATE library_evidence_state SET last_refreshed_at=statement_timestamp()-make_interval(secs=>$2) WHERE graph_id=$1").bind(f.graph).bind(refreshed as f64).execute(&mut *write).await.unwrap();
   write.commit().await.unwrap();
   let tenant=f.tenant(app);let mut tx=tenant.begin_member_repeatable_read(f.account).await.unwrap();library_read::configure_read_model_proof_tx(&mut tx).await.unwrap();
   let pair=library_read::compare_read_model_tx(&mut tx,&scope,"",40,[None;3]).await.unwrap();
   assert_eq!(pair.fallback_reason.as_deref(),expected,"node={nodes} listing={items} extra={extra} age={age} refreshed={refreshed}");
   assert_eq!(pair.path,if expected.is_some(){"live"}else{"model"});assert_eq!(pair.live,pair.candidate);
   library_read::rollback_read_model_tx(tx).await.unwrap();
  }
  f.close().await;
 }}
}

#[tokio::test]
async fn definition_three_dirty_container_labels_restore_next_maximum_multiplicity_and_null() {
 for plane in [false,true] {for app in [false,true] {
  let f=Fixture::new(plane).await;let mut items=Vec::new();
  for (index,label) in [Some("Alpha"),Some("Zulu"),None,Some("Alpha")].into_iter().enumerate() {
   let origin=json!({"service_id":"slack","content_kind":"meeting","provider_namespace":"listing-label",
    "connection":{"kind":"oauth_grant","id":"fixture"},"container":{"kind":"channel","id":"shared","label":label}});
   let item:Uuid=sqlx::query_scalar("INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id,origin,access_state) VALUES($1,'slack','listing-label',$2,$3,'permitted') RETURNING source_item_id")
    .bind(f.graph).bind(format!("listing-label-{index}")).bind(origin).fetch_one(&f.owner).await.unwrap();items.push(item);
  }
  f.refresh(app).await;
  let initial=f.pair(app,json!({}),"","model").await;
  assert_eq!(initial["facets"]["containers"][0]["label"],json!("Zulu"));assert_eq!(initial["facets"]["containers"][0]["source_count"],json!(4));
  for (item,label,count) in [(items[1],Some("Alpha"),3),(items[0],Some("Alpha"),2),(items[3],None,1)] {
   sqlx::query("DELETE FROM source_items WHERE graph_id=$1 AND source_item_id=$2").bind(f.graph).bind(item).execute(&f.owner).await.unwrap();
   for drained in [false,true] {
    if drained {f.refresh(app).await;}
    let value=f.pair(app,json!({}),"","model").await;
    assert_eq!(value["sources"]["total"],json!(count));
    assert_eq!(value["facets"]["containers"].as_array().unwrap().len(),1);
    assert_eq!(value["facets"]["containers"][0]["source_count"],json!(count));
    assert_eq!(value["facets"]["containers"][0]["label"],json!(label));
   }
  }
  f.close().await;
 }}
}

/// The outcome-text match drops other graphs inside its trigram index, so
/// their matching rows never reach the heap recheck (#1524 blocker 5). Each
/// such row cost one random heap page on a cold org tenant.
#[tokio::test]
async fn outcome_text_match_drops_other_graphs_inside_the_index() {
 for plane in [false,true] {
  let f=Fixture::new(plane).await;
  let node=f.node(f.graph,"Atlas").await;let other=f.node(f.foreign,"Atlas").await;
  for index in 0..4 {f.source_in_graph(f.graph,node,&format!("text-{index}"),"permitted","meeting").await;}
  for index in 0..12 {f.source_in_graph(f.foreign,other,&format!("foreign-text-{index}"),"permitted","meeting").await;}
  // Enough unrelated text that the planner reads both legs from their trigram index.
  sqlx::query("INSERT INTO semantic_atoms(graph_id,source_version_id,ordinal,kind,statement,extraction_version,model) SELECT v.graph_id,v.version_id,i,'fact','Unrelated note '||i,'source-atoms-v1','fixture' FROM (SELECT DISTINCT ON (graph_id) graph_id,version_id FROM source_versions ORDER BY graph_id,version_id) v CROSS JOIN generate_series(1,20000) i").execute(&f.owner).await.unwrap();
  sqlx::query("INSERT INTO semantic_atom_passages(semantic_atom_id,source_version_id,graph_id,start_char,end_char,quote) SELECT semantic_atom_id,source_version_id,graph_id,0,6,CASE WHEN statement LIKE '%deploy%' THEN 'deploy' ELSE 'unrelated' END FROM semantic_atoms").execute(&f.owner).await.unwrap();
  // Whole-table statistics, frozen, so the plan does not vary with the sample.
  sqlx::raw_sql("ALTER TABLE semantic_atoms SET (autovacuum_enabled=false); ALTER TABLE semantic_atom_passages SET (autovacuum_enabled=false); SET default_statistics_target=10000; ANALYZE semantic_atoms,semantic_atom_passages; RESET default_statistics_target").execute(&f.owner).await.unwrap();
  let scope:LibraryScope=serde_json::from_value(json!({"graph_ids":[f.graph],"outcome_versions":"all_retained"})).unwrap();
  let tenant=f.tenant(false);let mut tx=tenant.begin_member_repeatable_read(f.account).await.unwrap();library_read::configure_read_model_proof_tx(&mut tx).await.unwrap();
  // Fixture tables fit in a few pages, where a sequential scan wins; the proof is what the index admits.
  sqlx::query("SET LOCAL enable_seqscan=off").execute(&mut *tx).await.unwrap();
  // Live and Model render this match from one CTE; Live runs it without a refreshed listing.
  let program=library_read::read_model::definition_program(&scope,"deploy");
  let sql=format!("EXPLAIN(ANALYZE,FORMAT JSON) {}",program["conditional_live_sql"].as_str().unwrap());
  assert!(program["clean_model_attempt_sql"].as_str().unwrap().contains("text_matched_outcomes AS MATERIALIZED"));
  let plan:Value=sqlx::query_scalar(&sql).bind(&scope.graph_ids).bind(json!({})).bind(None::<Uuid>).bind(None::<Uuid>).bind(None::<Uuid>).bind(None::<String>).bind("%deploy%").bind(None::<chrono::DateTime<chrono::Utc>>).bind(None::<Uuid>).bind(None::<chrono::DateTime<chrono::Utc>>).bind(None::<Uuid>).bind(None::<i64>).bind(None::<i64>).bind(None::<Uuid>).bind(21_i64).bind(None::<chrono::DateTime<chrono::Utc>>).fetch_one(&mut *tx).await.unwrap();
  tx.rollback().await.unwrap();
  let mut nodes=Vec::new();selector_plan_walk(&plan[0]["Plan"],&mut nodes);
  let text:Vec<_>=nodes.iter().copied().filter(|n|n["Node Type"]=="Bitmap Heap Scan"&&matches!(n["Relation Name"].as_str(),Some("semantic_atoms"|"semantic_atom_passages"))&&n["Recheck Cond"].as_str().is_some_and(|c|c.contains("~~*"))&&n["Actual Loops"].as_f64().unwrap_or(0.0)>0.0).collect();
  assert_eq!(text.len(),2,"both outcome-text legs read through their trigram index: {plan}");
  for scan in text {
   assert_eq!(scan["Actual Rows"].as_f64(),Some(4.0),"{scan}");
   assert_eq!(scan["Rows Removed by Filter"].as_f64().unwrap_or(0.0),0.0,"another graph's text match reached the heap: {scan}");
   let index=&scan["Plans"][0];assert!(index["Index Cond"].as_str().unwrap().contains("graph_id"),"{index}");
   assert_eq!(index["Actual Rows"].as_f64(),Some(4.0),"{index}");
  }
  f.close().await;
 }
}

const BENCH_WORDS:&str="ARRAY['the','team','reviewed','Library','reader','rollout','meeting','notes','customer','asked','about','pricing','schedule','incident','postmortem','follow','up','owner','agreed','plan','release','branch','merge','review','budget','hiring','design','sync','backlog','sprint','metrics','dashboard','employ','explore','ploy','loyal','epic','depot','plot','alloy']";

/// Seeds a multi-graph org-tenant shape for the cold text-query benchmark
/// (#1524 blocker 5) and writes the clean Model statement as a psql PREPARE.
/// Run by hand with ORGT_TEST_BASE_URL and LIBRARY_TEXT_BENCH_DIR, then time
/// it with scripts/benchmark_library_text_cold.sh.
#[tokio::test]
#[ignore]
async fn seed_cold_text_bench() {
 let base=std::env::var("ORGT_TEST_BASE_URL").expect("ORGT_TEST_BASE_URL");
 let out=std::path::PathBuf::from(std::env::var("LIBRARY_TEXT_BENCH_DIR").expect("LIBRARY_TEXT_BENCH_DIR"));
 let items:i64=std::env::var("LIBRARY_TEXT_BENCH_ITEMS").ok().and_then(|v|v.parse().ok()).unwrap_or(3100);
 let others:usize=std::env::var("LIBRARY_TEXT_BENCH_OTHER_GRAPHS").ok().and_then(|v|v.parse().ok()).unwrap_or(7);
 let opts=PgConnectOptions::from_str(&base).unwrap();
 let mut admin=PgConnection::connect_with(&opts).await.unwrap();
 admin.execute("DROP DATABASE IF EXISTS library_text_bench").await.unwrap();
 admin.execute("CREATE DATABASE library_text_bench").await.unwrap();
 let owner=PgPoolOptions::new().max_connections(2).connect_with(opts.clone().database("library_text_bench")).await.unwrap();
 sqlx::migrate!("./migrations_tenant").run(&owner).await.unwrap();
 assert!(library_evidence::stamp_listing_barrier(&owner).await.unwrap());
 sqlx::query("INSERT INTO tenant_meta(id,org_id) VALUES(true,'org_library_text_bench')").execute(&owner).await.unwrap();
 let account=Uuid::new_v4();
 let graphs:Vec<Uuid>=(0..=others).map(|_|Uuid::new_v4()).collect();
 let target=graphs[0];
 let seed=[
  "INSERT INTO graphs(id,account_id,name,kind,owner_user_id) SELECT u.g,$2,'Bench '||u.o,'member',gen_random_uuid() FROM unnest($1::uuid[]) WITH ORDINALITY u(g,o)".to_owned(),
  // Graphs ingest concurrently in a real org tenant, so their rows share heap
  // pages. Text choice hashes ids: a random() there reuses the sort key. The
  // +0*column term makes each word list a per-row subquery, not one InitPlan.
  "INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id,origin,access_state) SELECT u.g,'slack','bench','bench-'||u.o||'-'||i,jsonb_build_object('service_id','slack','content_kind','message','provider_namespace','bench','connection',jsonb_build_object('kind','oauth_grant','id','bench'),'container',jsonb_build_object('kind','channel','id','C'||(i%40),'provider_namespace','bench')),'permitted' FROM generate_series(1,$3) i CROSS JOIN unnest($1::uuid[]) WITH ORDINALITY u(g,o) ORDER BY i,u.o".to_owned(),
  "INSERT INTO source_versions(graph_id,source_item_id,source_id,source_type,provider_namespace,origin,raw_inline,source_metadata,captured_at,extraction_status) SELECT graph_id,source_item_id,source_id,source_type,provider_namespace,origin,'Bench capture body.',jsonb_build_object('title','Capture '||source_id),'2026-01-01'::timestamptz+random()*interval '200 days','done' FROM source_items ORDER BY random()".to_owned(),
  "UPDATE source_items i SET current_version_id=v.version_id FROM source_versions v WHERE v.source_item_id=i.source_item_id".to_owned(),
  format!("INSERT INTO semantic_atoms(graph_id,source_version_id,ordinal,kind,statement,extraction_version,model,entities) SELECT v.graph_id,v.version_id,o,'fact',CASE WHEN abs(hashtext(v.version_id::text||':'||o))%1000<55 THEN 'We will deploy ' ELSE '' END||(SELECT string_agg(({BENCH_WORDS})[1+floor(random()*40)::int],' ') FROM generate_series(1,18+0*o)),'source-atoms-v2','bench','[]' FROM source_versions v CROSS JOIN generate_series(0,8) o ORDER BY random()"),
  format!("INSERT INTO semantic_atom_passages(semantic_atom_id,source_version_id,graph_id,start_char,end_char,quote) SELECT a.semantic_atom_id,a.source_version_id,a.graph_id,0,120,CASE WHEN abs(hashtext(a.semantic_atom_id::text))%1000<50 THEN 'the deploy ' ELSE '' END||(SELECT string_agg(({BENCH_WORDS})[1+floor(random()*40)::int],' ') FROM generate_series(1,20+0*a.ordinal)) FROM semantic_atoms a ORDER BY random()"),
  "INSERT INTO nodes(graph_id,type,title,body) SELECT $4,'entity','Bench node '||i||CASE WHEN i%800=0 THEN ' deploy' ELSE '' END,'{}' FROM generate_series(1,$3*3) i".to_owned(),
  "INSERT INTO node_semantic_atoms(graph_id,node_id,semantic_atom_id) SELECT DISTINCT a.graph_id,n.node_id,a.semantic_atom_id FROM semantic_atoms a CROSS JOIN generate_series(0,1) k JOIN LATERAL (SELECT node_id FROM nodes WHERE graph_id=a.graph_id OFFSET floor(random()*($3*3-1)+0*k)::int LIMIT 1) n ON true WHERE a.graph_id=$4".to_owned(),
 ];
 for sql in &seed {sqlx::query(sql).bind(&graphs).bind(account).bind(items).bind(target).execute(&owner).await.unwrap();}
 for _ in 0..20000 {
  let mut tx=owner.begin().await.unwrap();
  sqlx::query("SELECT set_config('app.account_id',$1,true),set_config('app.tenant_actor','system',true)").bind(account.to_string()).execute(&mut *tx).await.unwrap();
  library_evidence::refresh_graph_tx(&mut tx,target,Limits{nodes:128,changes:128}).await.unwrap();
  let ready:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM library_listing_state WHERE graph_id=$1 AND ready_version=3 AND sources_complete AND nodes_complete AND aggregates_complete AND sources_old_complete) AND NOT EXISTS(SELECT 1 FROM library_evidence_changes WHERE graph_id=$1) AND NOT EXISTS(SELECT 1 FROM library_listing_changes WHERE graph_id=$1)").bind(target).fetch_one(&mut *tx).await.unwrap();
  tx.commit().await.unwrap();
  if ready {break}
 }
 owner.execute("VACUUM ANALYZE").await.unwrap();
 let scope:LibraryScope=serde_json::from_value(serde_json::json!({"graph_ids":[target],"outcome_versions":"all_retained"})).unwrap();
 let types="uuid[],jsonb,uuid,uuid,uuid,text,text,timestamptz,uuid,timestamptz,uuid,bigint,bigint,uuid,bigint,timestamptz";
 let program=library_read::read_model::definition_program(&scope,"deploy");
 std::fs::write(out.join("prepare.sql"),format!("PREPARE m({types}) AS {};\n",program["clean_model_attempt_sql"].as_str().unwrap())).unwrap();
 std::fs::write(out.join("execute.sql"),format!("EXECUTE m('{{{target}}}','{{}}',NULL,NULL,NULL,NULL,'%deploy%',NULL,NULL,NULL,NULL,NULL,NULL,NULL,21,NULL);\n")).unwrap();
 std::fs::write(out.join("graph.txt"),target.to_string()).unwrap();
 let counts:serde_json::Value=sqlx::query_scalar("SELECT jsonb_build_object('atoms',(SELECT count(*) FROM semantic_atoms),'target_atoms',(SELECT count(*) FROM semantic_atoms WHERE graph_id=$1),'passages',(SELECT count(*) FROM semantic_atom_passages),'items',(SELECT count(*) FROM source_items WHERE graph_id=$1),'nodes',(SELECT count(*) FROM nodes WHERE graph_id=$1),'node_evidence',(SELECT count(*) FROM library_node_evidence WHERE graph_id=$1),'listing_outcomes',(SELECT count(*) FROM library_listing_outcomes WHERE graph_id=$1),'deploy_atoms',(SELECT count(*) FROM semantic_atoms WHERE statement ILIKE '%deploy%'),'target_deploy_atoms',(SELECT count(*) FROM semantic_atoms WHERE graph_id=$1 AND statement ILIKE '%deploy%'))").bind(target).fetch_one(&owner).await.unwrap();
 std::fs::write(out.join("counts.json"),counts.to_string()).unwrap();
 println!("{counts}");
}
