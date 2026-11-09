//! Real disposable PostgreSQL lifecycle, identity and access regression proof.
//! Prerequisites are asserted; these scenarios never silently skip.
mod support;
use axum::extract::{Extension,Path,State};
use axum::http::StatusCode;
use coralswarm_backend::{selected_publication,services::connector_audit::Actor,services::deletion::delete_source_item_in_graph,store::{PostgresStore,Store,ReefRefCtx,ReefResolution,CLOUD_DEVICE_ID},tenancy::extractor::Tenant,tenancy::scope::begin_account_scoped,test_app_state,UserContext};
use serde_json::json;
use sqlx::PgPool;
use std::sync::Arc;
use support::rls_probe::RlsProbe;
use uuid::Uuid;

async fn probe(name:&str)->RlsProbe {
 assert!(std::env::var("ORGT_TEST_BASE_URL").is_ok(),"ORGT_TEST_BASE_URL required for lifecycle proof");
 RlsProbe::setup(name).await.expect("real disposable PostgreSQL required")
}
async fn edge(pool:&PgPool,g:Uuid,a:Uuid,b:Uuid,p:&str,provenance:serde_json::Value)->Uuid {
 sqlx::query_scalar("INSERT INTO relationship_edge(graph_id,src_node_id,dst_node_id,predicate,status,provenance) VALUES($1,$2,$3,$4,'confirmed',$5) RETURNING edge_id")
 .bind(g).bind(a).bind(b).bind(p).bind(provenance).fetch_one(pool).await.unwrap()
}
async fn status(pool:&PgPool,id:Uuid)->String {sqlx::query_scalar("SELECT status FROM relationship_edge WHERE edge_id=$1").bind(id).fetch_one(pool).await.unwrap()}
async fn node(pool:&PgPool,g:Uuid,title:&str)->Uuid {sqlx::query_scalar("INSERT INTO nodes(graph_id,type,title) VALUES($1,'entity',$2) RETURNING node_id").bind(g).bind(title).fetch_one(pool).await.unwrap()}

#[tokio::test]
async fn rejected_and_deleted_parents_retract_transitive_inference_but_keep_independent_review() {
 let f=probe("cm_lifecycle_dependency").await;
 let g=f.a.graph_id;let a=f.a.node_id;let b=node(&f.owner,g,"B").await;let c=node(&f.owner,g,"C").await;
 let parent=edge(&f.owner,g,a,b,"supports",json!({"atom_ids":[f.a.atom_id]})).await;
 let dependent=edge(&f.owner,g,b,c,"dependent",json!({"rule":"composition","source_edge_ids":[parent],"reviewed_by":f.a.user_id})).await;
 let transitive=edge(&f.owner,g,a,c,"transitive",json!({"source_edge_ids":[dependent]})).await;
 let independent=edge(&f.owner,g,c,a,"independent",json!({"source_edge_ids":[parent],"atom_ids":[f.a.atom_id],"reviewed_by":f.a.user_id})).await;
 let mut tx=begin_account_scoped(&f.app_pool,f.a.account_id).await.unwrap();
 sqlx::query("UPDATE relationship_edge SET status='rejected' WHERE edge_id=$1").bind(parent).execute(&mut *tx).await.unwrap();tx.commit().await.unwrap();
 assert_eq!(status(&f.owner,dependent).await,"rejected");assert_eq!(status(&f.owner,transitive).await,"rejected");assert_eq!(status(&f.owner,independent).await,"confirmed");
 let p:serde_json::Value=sqlx::query_scalar("SELECT provenance FROM relationship_edge WHERE edge_id=$1").bind(independent).fetch_one(&f.owner).await.unwrap();
 assert_eq!(p["reviewed_by"],json!(f.a.user_id));assert!(p.get("source_edge_ids").is_none());
 // Deleting the final independent support invalidates even a human confirmed
 // claim, retaining its review attribution separately from current validity.
 let mut tx=begin_account_scoped(&f.app_pool,f.a.account_id).await.unwrap();
 sqlx::query("DELETE FROM atoms WHERE atom_id=$1").bind(f.a.atom_id).execute(&mut *tx).await.unwrap();tx.commit().await.unwrap();
 assert_eq!(status(&f.owner,independent).await,"rejected");
 let p:serde_json::Value=sqlx::query_scalar("SELECT provenance FROM relationship_edge WHERE edge_id=$1").bind(independent).fetch_one(&f.owner).await.unwrap();
 assert_eq!(p["reviewed_by"],json!(f.a.user_id));assert_eq!(p["human_review_status"],"confirmed");
 // A later explicit human rejection clears machine invalidation bookkeeping
 // and must remain durable when extraction finds a fresh readable support.
 let store=PostgresStore::new(f.app_pool.clone());
 let mut tx=begin_account_scoped(&f.app_pool,f.a.account_id).await.unwrap();
 store.set_relationship_edge_status_scoped(&mut tx,independent,coralswarm_backend::relationships::EdgeStatus::Rejected,Some(f.a.user_id),"review-fixture").await.unwrap();
 let fresh:Uuid=sqlx::query_scalar("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,timestamp) VALUES($1,'note','fresh-support','fixture',now()) RETURNING atom_id").bind(g).fetch_one(&mut *tx).await.unwrap();
 let reasserted=store.upsert_relationship_edge_scoped(&mut tx,g,c,"independent",a,coralswarm_backend::relationships::EdgeOrigin::Stated,coralswarm_backend::relationships::EdgeStatus::Confirmed,1.0,json!({"atom_ids":[fresh]})).await.unwrap();
 assert_eq!(reasserted.status,coralswarm_backend::relationships::EdgeStatus::Rejected);
 assert!(reasserted.provenance.get("lifecycle").is_none());assert_eq!(reasserted.provenance["reviewed_by"],json!(f.a.user_id));
 tx.commit().await.unwrap();
 assert_eq!(status(&f.owner,parent).await,"rejected");
 let foreign:i64=sqlx::query_scalar("SELECT count(*) FROM atoms WHERE atom_id=$1").bind(f.b.atom_id).fetch_one(&f.owner).await.unwrap();assert_eq!(foreign,1);
 f.teardown().await;
}

#[tokio::test]
async fn permission_loss_keeps_independent_derivation_and_rejects_forged_cross_graph_support() {
 let f=probe("cm_lifecycle_access_derivation").await;let g=f.a.graph_id;
 let item:Uuid=sqlx::query_scalar("INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id) VALUES($1,'jira','site:restricted','one') RETURNING source_item_id").bind(g).fetch_one(&f.owner).await.unwrap();
 let v:Uuid=sqlx::query_scalar("INSERT INTO source_versions(graph_id,source_item_id,source_id,source_type,provider_namespace,raw_inline) VALUES($1,$2,'one','jira','site:restricted','private fixture') RETURNING version_id").bind(g).bind(item).fetch_one(&f.owner).await.unwrap();
 sqlx::query("INSERT INTO source_version_chunks(graph_id,source_version_id,atom_id) VALUES($1,$2,$3)").bind(g).bind(v).bind(f.a.atom_id).execute(&f.owner).await.unwrap();
 let b=node(&f.owner,g,"Independent B").await;let c=node(&f.owner,g,"Independent C").await;
 // A separate readable atom supports the derivation; it is not mapped to the
 // source whose permission is being removed.
 let supporting:Uuid=sqlx::query_scalar("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,timestamp) VALUES($1,'note','permitted-support','fixture',now()) RETURNING atom_id").bind(g).fetch_one(&f.owner).await.unwrap();
 let parent=edge(&f.owner,g,f.a.node_id,b,"independent-parent",json!({"atom_ids":[supporting]})).await;
 let mixed=edge(&f.owner,g,b,c,"mixed-support",json!({"atom_ids":[f.a.atom_id],"source_edge_ids":[parent],"reviewed_by":f.a.user_id})).await;
 let foreign_node=node(&f.owner,f.b.graph_id,"Foreign B").await;
 let foreign_parent=edge(&f.owner,f.b.graph_id,f.b.node_id,foreign_node,"foreign-parent",json!({"atom_ids":[f.b.atom_id]})).await;
 let forged=edge(&f.owner,g,c,b,"forged-support",json!({"atom_ids":[f.a.atom_id],"source_edge_ids":[foreign_parent]})).await;
 let mut tx=begin_account_scoped(&f.app_pool,f.a.account_id).await.unwrap();
 coralswarm_backend::source_evidence::set_namespace_access_tx(&mut tx,g,"site:restricted","restricted").await.unwrap();tx.commit().await.unwrap();
 assert_eq!(status(&f.owner,mixed).await,"confirmed","permission loss cannot erase independent valid support");
 assert_eq!(status(&f.owner,forged).await,"rejected","another graph's parent is never support");
 assert_eq!(status(&f.owner,foreign_parent).await,"confirmed","permission observations stay in their graph");
 let p:serde_json::Value=sqlx::query_scalar("SELECT provenance FROM relationship_edge WHERE edge_id=$1").bind(mixed).fetch_one(&f.owner).await.unwrap();
 assert_eq!(p["reviewed_by"],json!(f.a.user_id));
 f.teardown().await;
}

#[tokio::test]
async fn source_delete_redacts_surviving_node_sync_history_and_keeps_independent_identity() {
 let f=probe("cm_lifecycle_sync_redaction").await;let g=f.a.graph_id;let n=f.a.node_id;
 sqlx::query("INSERT INTO node_atoms(node_id,atom_id) VALUES($1,$2)").bind(n).bind(f.a.atom_id).execute(&f.owner).await.unwrap();
 let legacy=node(&f.owner,g,"Legacy-only survivor").await;
 sqlx::query("INSERT INTO node_atoms(node_id,atom_id) VALUES($1,$2)").bind(legacy).bind(f.a.atom_id).execute(&f.owner).await.unwrap();
 let item:Uuid=sqlx::query_scalar("INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id) VALUES($1,'note','site:fixture','delete-me') RETURNING source_item_id").bind(g).fetch_one(&f.owner).await.unwrap();
 let v:Uuid=sqlx::query_scalar("INSERT INTO source_versions(graph_id,source_item_id,source_id,source_type,provider_namespace,raw_inline) VALUES($1,$2,'delete-me','note','site:fixture','deleted alias fixture') RETURNING version_id").bind(g).bind(item).fetch_one(&f.owner).await.unwrap();
 let a:Uuid=sqlx::query_scalar("INSERT INTO semantic_atoms(graph_id,source_version_id,ordinal,kind,statement,extraction_version,model) VALUES($1,$2,0,'fact','Alias observed','source-atoms-v1','mock-fixture') RETURNING semantic_atom_id").bind(g).bind(v).fetch_one(&f.owner).await.unwrap();
 let deleted_chunk=Uuid::new_v4();let key=coralswarm_core::r2::blob_key_for(None,g,deleted_chunk).unwrap();
 sqlx::query("INSERT INTO atoms(atom_id,graph_id,source_type,source_id,content_blob_key,content_inline,timestamp) VALUES($1,$2,'note','delete-me:chunk:0',$3,'deleted-private-alias',now())").bind(deleted_chunk).bind(g).bind(key).execute(&f.owner).await.unwrap();
 sqlx::query("INSERT INTO node_atoms(node_id,atom_id) VALUES($1,$2)").bind(legacy).bind(deleted_chunk).execute(&f.owner).await.unwrap();
 sqlx::query("INSERT INTO source_version_chunks(graph_id,source_version_id,atom_id) VALUES($1,$2,$3)").bind(g).bind(v).bind(deleted_chunk).execute(&f.owner).await.unwrap();
 sqlx::query("INSERT INTO node_semantic_atoms(graph_id,node_id,semantic_atom_id) VALUES($1,$2,$3)").bind(g).bind(n).bind(a).execute(&f.owner).await.unwrap();
 sqlx::query("UPDATE nodes SET body=jsonb_build_object('aliases',jsonb_build_array('deleted-private-alias'),'independent_fact','retained') WHERE node_id=$1").bind(n).execute(&f.owner).await.unwrap();
 sqlx::query("UPDATE nodes SET body=jsonb_build_object('aliases',jsonb_build_array('deleted-private-alias'),'independent_fact','retained legacy') WHERE node_id=$1").bind(legacy).execute(&f.owner).await.unwrap();
 sqlx::query("INSERT INTO op_log(op_id,entity,entity_id,op_type,payload,version,updated_at,device_id,graph_id) VALUES(gen_random_uuid(),'node',$1,'upsert',$2,1,now(),'fixture',$3)").bind(n.to_string()).bind(json!({"body":{"aliases":["deleted-private-alias"]}})).bind(g).execute(&f.owner).await.unwrap();
 sqlx::query("INSERT INTO op_log(op_id,entity,entity_id,op_type,payload,version,updated_at,device_id,graph_id) VALUES(gen_random_uuid(),'node',$1,'upsert',$2,1,now(),'fixture',$3)").bind(legacy.to_string()).bind(json!({"body":{"aliases":["deleted-private-alias"]}})).bind(g).execute(&f.owner).await.unwrap();
 let removed=coralswarm_backend::services::deletion::delete_source_item_in_graph(&f.app_pool,None,"review-fixture",g,item,coralswarm_backend::services::connector_audit::Actor::System,Some(&f.owner)).await.unwrap();
 let retained_source_rows:(i64,i64,i64,i64)=sqlx::query_as("SELECT (SELECT count(*) FROM source_items WHERE source_item_id=$1),(SELECT count(*) FROM source_versions WHERE version_id=$2),(SELECT count(*) FROM source_version_chunks WHERE source_version_id=$2),(SELECT count(*) FROM semantic_atoms WHERE semantic_atom_id=$3)").bind(item).bind(v).bind(a).fetch_one(&f.owner).await.unwrap();
 assert_eq!(retained_source_rows,(0,0,0,0),"deletion receipts/outbox events are not proof that source rows actually disappeared");
 assert!(removed.deleted_reef_ids.is_empty(),"legacy independent evidence preserves the node");
 let body:serde_json::Value=sqlx::query_scalar("SELECT body FROM nodes WHERE graph_id=$1 AND node_id=$2").bind(g).bind(n).fetch_one(&f.owner).await.unwrap();
 assert!(body.get("aliases").is_none());assert_eq!(body["independent_fact"],"retained");
 let snapshots:Vec<serde_json::Value>=sqlx::query_scalar("SELECT payload FROM op_log WHERE graph_id=$1 AND entity='node' AND entity_id=$2").bind(g).bind(n.to_string()).fetch_all(&f.owner).await.unwrap();
 assert_eq!(snapshots.len(),1,"only sanitized current identity remains syncable");
 assert!(!snapshots[0].to_string().contains("deleted-private-alias"));
 assert!(snapshots[0].to_string().contains("retained"));
 let legacy_body:serde_json::Value=sqlx::query_scalar("SELECT body FROM nodes WHERE node_id=$1").bind(legacy).fetch_one(&f.owner).await.unwrap();
 assert!(legacy_body.get("aliases").is_none());assert_eq!(legacy_body["independent_fact"],"retained legacy");
 let old_content:i64=sqlx::query_scalar("SELECT count(*) FROM op_log WHERE graph_id=$1 AND payload::text LIKE '%deleted-private-alias%'").bind(g).fetch_one(&f.owner).await.unwrap();assert_eq!(old_content,0,"neither semantic nor legacy sync history retains deleted aliases");
 f.teardown().await;
}

struct RepeatedMentionModel;
#[async_trait::async_trait]
impl coralswarm_backend::llm::LlmProvider for RepeatedMentionModel {
 async fn complete(&self,_req:coralswarm_backend::llm::CompletionRequest)->Result<coralswarm_backend::llm::CompletionResponse,coralswarm_backend::llm::LlmError>{
  Ok(coralswarm_backend::llm::CompletionResponse{content:json!({"atoms":[
   {"kind":"fact","text":"Alex starts today.","evidence_quote":"Alex starts today.","entities":[{"mention":"Alex","type":"person"}]},
   {"kind":"action","text":"Alex will review.","evidence_quote":"Alex will review.","entities":[{"mention":"Alex","type":"person"}]}
  ]}).to_string(),input_tokens:10,output_tokens:10,model:"mock-context".into(),cache_creation_input_tokens:0,cache_read_input_tokens:0,stop_reason:Some("tool_use".into()),cost_usd:None})
 }
 async fn stream_complete(&self,_req:coralswarm_backend::llm::CompletionRequest)->Result<std::pin::Pin<Box<dyn futures::Stream<Item=Result<coralswarm_backend::llm::StreamEvent,coralswarm_backend::llm::LlmError>>+Send>>,coralswarm_backend::llm::LlmError>{panic!("source extraction is nonstreaming")}
}

#[tokio::test]
async fn semantic_worker_shares_context_within_version_and_keeps_unrelated_versions_unresolved() {
 use coralswarm_backend::{source_evidence::{capture_source_version_tx,CaptureSource},jobs::queue::JobRow,tenancy::TenantId,workers::source_atoms};
 let f=probe("cm_lifecycle_worker_context").await;let g=f.a.graph_id;
 // Paid semantic processing requires the compatible lifecycle contract;
 // expand-only capture still retains raw evidence but does not model-enrich.
 let mut connection=f.owner.acquire().await.unwrap();
 sqlx::query("SET app.identity_serving_contract='composite-v1'").execute(&mut *connection).await.unwrap();
 sqlx::raw_sql(include_str!("../schema/identity-contract.sql")).execute(&mut *connection).await.unwrap();drop(connection);
 sqlx::query("INSERT INTO account_members(account_id,user_id,role) VALUES($1,$2,'owner') ON CONFLICT DO NOTHING").bind(f.a.account_id).bind(f.a.user_id).execute(&f.owner).await.unwrap();
 // The gated mock still requires finite synthetic funding before dispatch.
 let mut financial=begin_account_scoped(&f.owner,f.a.account_id).await.unwrap();
 coralswarm_backend::billing::renewal::lock_period_rotation(&mut financial,f.a.account_id).await.unwrap();
 sqlx::query("INSERT INTO credit_balances(account_id,balance) VALUES($1,100)")
  .bind(f.a.account_id).execute(&mut *financial).await.unwrap();
 financial.commit().await.unwrap();
 for source_id in ["managed-first","managed-second"] {
  let mut tx=begin_account_scoped(&f.app_pool,f.a.account_id).await.unwrap();
  let metadata=json!({});let saved=capture_source_version_tx(&mut tx,None,g,f.a.account_id,&CaptureSource{source_type:"note",provider_namespace:"site:context",source_id,normalized_text:"Alex starts today. Alex will review.",representation:"original_text",metadata:&metadata,occurred_at:None,participants:&[],ingested_by:Some(f.a.user_id),scope:"private",duration_ms:None,segments:None},None,&[42;32]).await.unwrap();tx.commit().await.unwrap();
  let state=std::sync::Arc::new(coralswarm_backend::test_app_state_owned_with_db_and_store(f.app_pool.clone(),None));
  let tenant=coralswarm_backend::tenancy::extractor::Tenant::legacy(&state);
  let user=coralswarm_backend::UserContext{user_id:f.a.user_id,account_id:f.a.account_id,role:"owner".into(),device_id:"managed-context-fixture".into()};
  let (code,axum::Json(run))=coralswarm_backend::corpus_enrichment::create_run(axum::extract::State(state.clone()),tenant.clone(),axum::Extension(user.clone()),axum::Json(serde_json::from_value(json!({"graph_id":g,"execute":true,"budget":{"items":2,"characters":50000,"model_calls":2}})).unwrap())).await;
  assert_eq!(code,axum::http::StatusCode::CREATED,"{run}");let run_id:Uuid=run["run_id"].as_str().unwrap().parse().unwrap();
  let (code,axum::Json(body))=coralswarm_backend::corpus_enrichment::advance_run(axum::extract::State(state),tenant,axum::Extension(user),axum::extract::Path(run_id),axum::Json(serde_json::from_value(json!({"graph_id":g,"page_size":100})).unwrap())).await;assert_eq!(code,axum::http::StatusCode::OK,"{body}");
  let row:(Uuid,String,serde_json::Value)=sqlx::query_as("SELECT id,kind,payload FROM jobs WHERE payload->>'source_version_id'=$1").bind(saved.version_id.to_string()).fetch_one(&f.owner).await.unwrap();
  source_atoms::process_job(&TenantId::Consumer,&f.app_pool,&f.owner,&PostgresStore::new(f.app_pool.clone()),&coralswarm_backend::llm::testing::GatedScript(&RepeatedMentionModel),None,JobRow{id:row.0,kind:row.1,payload:row.2,attempts:0,account_id:Some(f.a.account_id)}).await;
  let linked:(i64,i64)=sqlx::query_as("SELECT count(*),count(DISTINCT l.node_id) FROM node_semantic_atoms l JOIN semantic_atoms a ON a.semantic_atom_id=l.semantic_atom_id AND a.graph_id=l.graph_id WHERE a.source_version_id=$1").bind(saved.version_id).fetch_one(&f.owner).await.unwrap();
  assert_eq!(linked,(2,1),"two explicit mentions in one retained version must share their evidence context");
 }
 let identities:i64=sqlx::query_scalar("SELECT count(*) FROM nodes WHERE graph_id=$1 AND title='Alex'").bind(g).fetch_one(&f.owner).await.unwrap();
 // The cohort run is v2 now (v1 is retired). v1 never bound across versions
 // by name; v2's binding ladder binds an exact title to the reef that
 // already exists (`v2_exact_title_binds_existing_reef`), so the second
 // version's Alex joins the first.
 assert_eq!(identities,1,"v2 binds an exact title to the existing reef");
 f.teardown().await;
}

#[tokio::test]
async fn contract_rollout_preserves_old_writes_then_same_name_contexts_stay_separate() {
 let f=probe("cm_lifecycle_identity").await;let store=PostgresStore::new(f.owner.clone());let g=f.a.graph_id;
 // Tenant 0203 / control 0386 (#1516) end the expand phase on every plane:
 // the title-only unique index is gone, so the old binaries' title-only
 // conflict target no longer exists and the composite one is the arbiter.
 let legacy:bool=sqlx::query_scalar("SELECT to_regclass('nodes_graph_type_lower_title_unique') IS NOT NULL").fetch_one(&f.owner).await.unwrap();
 assert!(!legacy,"the converge migration dropped the title-only unique index");
 assert!(sqlx::query("INSERT INTO nodes(graph_id,type,title) VALUES($1,'entity','Legacy') ON CONFLICT(graph_id,type,lower(title)) DO UPDATE SET updated_at=now()")
 .bind(g).execute(&f.owner).await.is_err(),"no title-only conflict target remains");
 let ca=ReefRefCtx{atom_id:Some(Uuid::new_v4()),entity_type:Some("person".into()),..Default::default()};
 let cb=ReefRefCtx{atom_id:Some(Uuid::new_v4()),entity_type:Some("person".into()),..Default::default()};
 let first=store.mint_reef(g,"entity","Alex Lee",&ca).await.unwrap();
 assert!(!matches!(store.resolve_reef_ref(g,"entity","Alex Lee",&cb).await.unwrap(),ReefResolution::Resolved{node_id,..} if node_id==first),"a person name from other evidence is not that person");
 let second=store.mint_reef(g,"entity","Alex Lee",&cb).await.unwrap();assert_ne!(first,second);
 let titles:Vec<String>=sqlx::query_scalar("SELECT title FROM nodes WHERE node_id=ANY($1)").bind(vec![first,second]).fetch_all(&f.owner).await.unwrap();assert_eq!(titles,vec!["Alex Lee","Alex Lee"]);
 assert!(matches!(store.resolve_reef_ref(g,"entity","Alex Lee",&ReefRefCtx::default()).await.unwrap(),ReefResolution::Unknown),"no arbitrary title winner");
 sqlx::query("INSERT INTO reef_identifiers(graph_id,node_id,kind,value_norm,source) VALUES($1,$2,'email','alex.one@example.test','test')").bind(g).bind(first).execute(&f.owner).await.unwrap();
 assert!(matches!(store.resolve_reef_ref(g,"entity","alex.one@example.test",&cb).await.unwrap(),ReefResolution::Resolved{node_id,..} if node_id==first));
 let mut conn=f.owner.acquire().await.unwrap();let classes=coralswarm_backend::store::identity_class::classes_for(&mut conn,g,&[first,second]).await.unwrap();assert_eq!(classes.len(),2);drop(conn);
 f.teardown().await;
}

#[tokio::test]
async fn source_delete_has_noncontent_durable_tombstone_and_no_summary_window_exemption() {
 let f=probe("cm_lifecycle_tombstone").await;let g=f.a.graph_id;
 let item:Uuid=sqlx::query_scalar("INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id) VALUES($1,'linear','site:test','issue-sensitive-key') RETURNING source_item_id")
 .bind(g).fetch_one(&f.owner).await.unwrap();
 sqlx::query("INSERT INTO journal_rollups(account_id,user_id,graph_id,hour_start,period_start,summary,input_fingerprint,model) VALUES($1,$2,$3,now()-interval '90 days',now()-interval '90 days','sensitive derived text','test','mock')")
 .bind(f.a.account_id).bind(f.a.user_id).bind(g).execute(&f.owner).await.unwrap();
 let mut tx=begin_account_scoped(&f.app_pool,f.a.account_id).await.unwrap();sqlx::query("DELETE FROM source_items WHERE source_item_id=$1").bind(item).execute(&mut *tx).await.unwrap();tx.commit().await.unwrap();
 let remaining_source:i64=sqlx::query_scalar("SELECT count(*) FROM source_items WHERE source_item_id=$1").bind(item).fetch_one(&f.owner).await.unwrap();assert_eq!(remaining_source,0,"minimal tombstone is separate from actual deletion");
 let tombstone:serde_json::Value=sqlx::query_scalar("SELECT to_jsonb(t) FROM source_retraction_outbox t WHERE source_item_id=$1").bind(item).fetch_one(&f.owner).await.unwrap();
 assert!(!tombstone.to_string().contains("sensitive"));assert_eq!(tombstone["source_item_id"],json!(item));
 let remaining:i64=sqlx::query_scalar("SELECT count(*) FROM journal_rollups WHERE graph_id=$1").bind(g).fetch_one(&f.owner).await.unwrap();assert_eq!(remaining,0);
 let mut other=begin_account_scoped(&f.app_pool,f.b.account_id).await.unwrap();let hidden:i64=sqlx::query_scalar("SELECT count(*) FROM source_retraction_outbox WHERE source_item_id=$1").bind(item).fetch_one(&mut *other).await.unwrap();assert_eq!(hidden,0);other.rollback().await.unwrap();
 f.teardown().await;
}

#[tokio::test]
async fn permission_denial_retains_content_and_restores_only_exact_refetched_item() {
 let f=probe("cm_lifecycle_access").await;let g=f.a.graph_id;
 let mut ids=vec![];
 for native in ["one","two"] {let id:Uuid=sqlx::query_scalar("INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id) VALUES($1,'jira','site:test',$2) RETURNING source_item_id").bind(g).bind(native).fetch_one(&f.owner).await.unwrap();ids.push(id);}
 let mut tx=begin_account_scoped(&f.app_pool,f.a.account_id).await.unwrap();
 assert!(coralswarm_backend::source_evidence::set_namespace_access_tx(&mut tx,g,"site:test","permitted").await.is_err(),"reconnect never restores a whole namespace");
 assert_eq!(coralswarm_backend::source_evidence::set_namespace_access_tx(&mut tx,g,"site:test","restricted").await.unwrap(),2);
 assert_eq!(coralswarm_backend::source_evidence::set_source_identity_access_tx(&mut tx,g,"jira","site:test","one","permitted").await.unwrap(),1);tx.commit().await.unwrap();
 let states:Vec<String>=sqlx::query_scalar("SELECT access_state FROM source_items WHERE source_item_id=ANY($1) ORDER BY source_id").bind(ids).fetch_all(&f.owner).await.unwrap();assert_eq!(states,vec!["permitted","restricted"]);
 let mut other=begin_account_scoped(&f.app_pool,f.b.account_id).await.unwrap();assert_eq!(coralswarm_backend::source_evidence::set_namespace_access_tx(&mut other,g,"site:test","unknown").await.unwrap(),0);other.rollback().await.unwrap();
 f.teardown().await;
}

#[tokio::test]
async fn trusted_refetch_compares_restricted_history_without_restoring_namespace_access() {
 use coralswarm_backend::source_evidence::{capture_source_version_tx,load_version_scoped,set_namespace_access_tx,set_source_identity_access_tx,CaptureSource};
 let f=probe("cm_lifecycle_refetch").await;let g=f.a.graph_id;
 let metadata=json!({});let mut initial=None;
 for (state,text) in [("permitted","retained"),("restricted","retained"),("unknown","changed")] {
  let mut tx=begin_account_scoped(&f.app_pool,f.a.account_id).await.unwrap();
  if state!="permitted" {set_namespace_access_tx(&mut tx,g,"site:refetch",state).await.unwrap();}
  let captured=capture_source_version_tx(&mut tx,None,g,f.a.account_id,&CaptureSource{source_type:"note",provider_namespace:"site:refetch",source_id:"one",normalized_text:text,representation:"original_text",metadata:&metadata,occurred_at:None,participants:&[],ingested_by:Some(f.a.user_id),scope:"private",duration_ms:None,segments:None},None,&[42;32]).await.unwrap();
  if state=="permitted" {initial=Some(captured.version_id);} else {
   assert!(load_version_scoped(&mut tx,g,captured.version_id).await.unwrap().is_none(),"trusted writes never restore public evidence access");
   assert_eq!(captured.changed,state=="unknown");
   if state=="restricted" {assert_eq!(Some(captured.version_id),initial);}
  }
  tx.commit().await.unwrap();
 }
 sqlx::query("INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id,access_state) VALUES($1,'note','site:refetch','two','unknown')").bind(g).execute(&f.owner).await.unwrap();
 let mut tx=begin_account_scoped(&f.app_pool,f.a.account_id).await.unwrap();
 assert_eq!(set_source_identity_access_tx(&mut tx,g,"note","site:refetch","one","permitted").await.unwrap(),1);
 let version:Uuid=sqlx::query_scalar("SELECT current_version_id FROM source_items WHERE graph_id=$1 AND source_id='one'").bind(g).fetch_one(&mut *tx).await.unwrap();
 assert!(load_version_scoped(&mut tx,g,version).await.unwrap().is_some());tx.commit().await.unwrap();
 let other:String=sqlx::query_scalar("SELECT access_state FROM source_items WHERE graph_id=$1 AND source_id='two'").bind(g).fetch_one(&f.owner).await.unwrap();assert_eq!(other,"unknown");
 f.teardown().await;
}

#[tokio::test]
async fn correction_undo_restores_homonym_identity_scope_but_documents_identifier_union_limit() {
 let f=probe("cm_lifecycle_undo").await;let g=f.a.graph_id;let store=PostgresStore::new(f.owner.clone());
 sqlx::query("DROP INDEX IF EXISTS nodes_graph_type_lower_title_unique").execute(&f.owner).await.unwrap();
 let a=store.mint_reef(g,"entity","Alex",&ReefRefCtx{atom_id:Some(Uuid::new_v4()),..Default::default()}).await.unwrap();
 let b=store.mint_reef(g,"entity","Alex",&ReefRefCtx{atom_id:Some(Uuid::new_v4()),..Default::default()}).await.unwrap();
 let original:Uuid=sqlx::query_scalar("SELECT identity_scope FROM nodes WHERE node_id=$1").bind(b).fetch_one(&f.owner).await.unwrap();
 store.record_reef_identifier(g,b,"email","duplicate@example.test","test").await.unwrap();
 store.merge_nodes(a,b).await.unwrap();
 let merge:Uuid=sqlx::query_scalar("SELECT merge_id FROM node_merges WHERE keep_id=$1 AND dup_id=$2 AND undone_at IS NULL").bind(a).bind(b).fetch_one(&f.owner).await.unwrap();
 store.unmerge_nodes(merge).await.unwrap();
 let restored:Uuid=sqlx::query_scalar("SELECT identity_scope FROM nodes WHERE node_id=$1").bind(b).fetch_one(&f.owner).await.unwrap();assert_eq!(restored,original);
 assert!(matches!(store.resolve_reef_ref(g,"entity","duplicate@example.test",&ReefRefCtx::default()).await.unwrap(),ReefResolution::Resolved{node_id,..} if node_id==a),"undo currently retains identifier union on survivor; this is an explicit correction limit");
 f.teardown().await;
}

// Shape of a fresh capture held at the managed-admission fence: one source
// item, one blocked version, one legacy atom linked by a chunk, one queued job.
#[tokio::test]
async fn deleting_a_blocked_capture_removes_its_atom_and_kills_its_queued_job() {
 let f=probe("cm_lifecycle_delete_blocked_capture").await;let g=f.a.graph_id;
 let ns=format!("declared:user:{}",f.a.user_id);
 let item:Uuid=sqlx::query_scalar("INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id) VALUES($1,'note',$2,'p12-canary') RETURNING source_item_id").bind(g).bind(&ns).fetch_one(&f.owner).await.unwrap();
 let atom:Uuid=sqlx::query_scalar("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,timestamp) VALUES($1,'note','p12-canary','fixture',now()) RETURNING atom_id").bind(g).fetch_one(&f.owner).await.unwrap();
 let version:Uuid=sqlx::query_scalar("INSERT INTO source_versions(graph_id,source_item_id,source_id,source_type,provider_namespace,raw_inline,extraction_status,extraction_error) VALUES($1,$2,'p12-canary','note',$3,'P12 canary','blocked','managed_admission_required') RETURNING version_id").bind(g).bind(item).bind(&ns).fetch_one(&f.owner).await.unwrap();
 sqlx::query("INSERT INTO source_version_chunks(graph_id,source_version_id,atom_id) VALUES($1,$2,$3)").bind(g).bind(version).bind(atom).execute(&f.owner).await.unwrap();
 let job:Uuid=sqlx::query_scalar("INSERT INTO jobs(kind,payload,account_id) VALUES('ExtractSourceAtoms',jsonb_build_object('graph_id',$1::uuid,'source_version_id',$2::uuid,'start_char',0),$3) RETURNING id").bind(g).bind(version).bind(f.a.account_id).fetch_one(&f.owner).await.unwrap();
 let outcome=delete_source_item_in_graph(&f.app_pool,None,CLOUD_DEVICE_ID,g,item,Actor::User(f.a.user_id),None).await.expect("a captured source item can be deleted");
 assert_eq!(outcome.atoms_deleted,vec![(g,atom)]);
 let left:(i64,i64)=sqlx::query_as("SELECT (SELECT count(*) FROM source_items WHERE source_item_id=$1),(SELECT count(*) FROM atoms WHERE atom_id=$2)").bind(item).bind(atom).fetch_one(&f.owner).await.unwrap();
 assert_eq!(left,(0,0),"source item and atom are gone");
 let job_status:String=sqlx::query_scalar("SELECT status::text FROM jobs WHERE id=$1").bind(job).fetch_one(&f.owner).await.unwrap();
 assert_eq!(job_status,"dead","queued extraction of deleted evidence never runs");
 f.teardown().await;
}

#[tokio::test]
async fn retracting_a_selected_evidence_publication_removes_the_published_copy() {
 let f=probe("cm_lifecycle_retract_publication").await;let g=f.a.graph_id;let acc=f.a.account_id;let user=f.a.user_id;
 sqlx::query("INSERT INTO account_members(account_id,user_id,role) VALUES($1,$2,'owner') ON CONFLICT DO NOTHING").bind(acc).bind(user).execute(&f.owner).await.unwrap();
 let destination:Uuid=sqlx::query_scalar("INSERT INTO graphs(account_id,name,kind) VALUES($1,'destination','org') RETURNING id").bind(acc).fetch_one(&f.owner).await.unwrap();
 let item:Uuid=sqlx::query_scalar("INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id) VALUES($1,'note','declared:user','selected') RETURNING source_item_id").bind(g).fetch_one(&f.owner).await.unwrap();
 let version:Uuid=sqlx::query_scalar("INSERT INTO source_versions(graph_id,source_item_id,source_id,source_type,provider_namespace,raw_inline) VALUES($1,$2,'selected','note','declared:user','private body') RETURNING version_id").bind(g).bind(item).fetch_one(&f.owner).await.unwrap();
 let publication=Uuid::new_v4();
 let copy_item:Uuid=sqlx::query_scalar("INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id) VALUES($1,'note','publication','selected') RETURNING source_item_id").bind(destination).fetch_one(&f.owner).await.unwrap();
 let copy_version:Uuid=sqlx::query_scalar("INSERT INTO source_versions(graph_id,source_item_id,source_id,source_type,provider_namespace,raw_inline,content_sha256,extraction_version,source_metadata) VALUES($1,$2,'selected','note','publication','selected passage','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','selected-passages-v1',jsonb_build_object('publication_receipt_id',$3::text)) RETURNING version_id").bind(destination).bind(copy_item).bind(publication).fetch_one(&f.owner).await.unwrap();
 let copy_atom:Uuid=sqlx::query_scalar("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,timestamp) VALUES($1,'note','selected','fixture',now()) RETURNING atom_id").bind(destination).fetch_one(&f.owner).await.unwrap();
 sqlx::query("INSERT INTO source_version_chunks(graph_id,source_version_id,atom_id) VALUES($1,$2,$3)").bind(destination).bind(copy_version).bind(copy_atom).execute(&f.owner).await.unwrap();
 sqlx::query("INSERT INTO selected_evidence_publications(publication_id,account_id,owner_user_id,request_id,source_graph_id,source_item_id,source_version_id,selection,passage_ids,target_graph_id,target_source_item_id,target_source_version_id,selected_sha256) VALUES($1,$2,$3,$4,$5,$6,$7,'[]'::jsonb,ARRAY[$11]::uuid[],$8,$9,$10,'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')")
  .bind(publication).bind(acc).bind(user).bind(Uuid::new_v4()).bind(g).bind(item).bind(version).bind(destination).bind(copy_item).bind(copy_version).bind(Uuid::new_v4()).execute(&f.owner).await.unwrap();
 let store:Arc<dyn Store>=Arc::new(PostgresStore::new(f.app_pool.clone()));
 let tenant=Tenant {
     store: Some(store),
     pool: Some(f.app_pool.clone()),
     decision: None,
     class: coralswarm_backend::route_class::RouteClass::Other,
 };
 let ctx=UserContext{user_id:user,account_id:acc,role:"owner".into(),device_id:"test".into()};
 let response=selected_publication::retract(State(test_app_state()),tenant,Extension(ctx),Path(publication)).await;
 assert_eq!(response.status(),StatusCode::OK);
 let copies:i64=sqlx::query_scalar("SELECT count(*) FROM source_items WHERE source_item_id=$1").bind(copy_item).fetch_one(&f.owner).await.unwrap();
 assert_eq!(copies,0,"the published copy is gone");
 let (status,retracted):(String,bool)=sqlx::query_as("SELECT p.status,a.evidence_retracted FROM selected_evidence_publications p,atoms a WHERE p.publication_id=$1 AND a.atom_id=$2").bind(publication).bind(copy_atom).fetch_one(&f.owner).await.unwrap();
 assert_eq!((status.as_str(),retracted),("retracted",true),"consent is retracted and the copy's atom reads as retracted evidence");
 f.teardown().await;
}
