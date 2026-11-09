//! Shared integration fixtures for the current P8 contract, never serving code.
//! Synthetic retained inputs are real encrypted local-R2 objects. Consent and
//! destination admission rows are written only by the actual publication API.
use axum::{
    extract::{Query, State},
    http::StatusCode,
    Extension,
};
use coralswarm_backend::{
    library_read::LibraryJson,
    selected_publication,
    source_evidence::{self, EvidenceQuery},
    store::PostgresStore,
    tenancy::extractor::Tenant,
    workers::{promote, tenant_harness::WorkerTenant},
    UserContext,
};
use coralswarm_core::r2::{blob_key_for, R2Client};
use serde_json::{json, Value};
use sqlx::{postgres::PgPoolOptions, PgPool};
use std::sync::Arc;
use uuid::Uuid;

#[path = "app_role.rs"]
mod app_role;
#[path = "financial_fixture.rs"]
mod financial_fixture;

async fn account_effects(pool: &PgPool, account: Uuid) -> Value {
    let mut state = serde_json::Map::new();
    for table in [
        "credit_balances",
        "credit_ledger",
        "managed_budget_periods",
        "managed_attempt_receipts",
        "managed_credit_reservations",
        "managed_embedding_inputs",
        "managed_enrichment_inputs",
        "llm_usage",
        "llm_usage_pending",
    ] {
        let rows: Value = sqlx::query_scalar(&format!(
            "SELECT COALESCE(jsonb_agg(to_jsonb(r) ORDER BY to_jsonb(r)::text),'[]'::jsonb) FROM {table} r WHERE account_id=$1"
        )).bind(account).fetch_one(pool).await.unwrap();
        state.insert(table.into(), rows);
    }
    Value::Object(state)
}

async fn legacy_state(pool: &PgPool, account: Uuid, graphs: &[Uuid], job: Uuid) -> Value {
    let mut state = account_effects(pool, account).await;
    for table in [
        "atoms",
        "nodes",
        "semantic_atoms",
        "relationship_edge",
        "source_items",
        "source_versions",
        "op_log",
    ] {
        let rows: Value = sqlx::query_scalar(&format!(
            "SELECT COALESCE(jsonb_agg(to_jsonb(r) ORDER BY to_jsonb(r)::text),'[]'::jsonb) FROM {table} r WHERE graph_id=ANY($1)"
        )).bind(graphs).fetch_one(pool).await.unwrap();
        state[table] = rows;
    }
    state["links"] = sqlx::query_scalar::<_, Value>(
        "SELECT COALESCE(jsonb_agg(to_jsonb(l) ORDER BY to_jsonb(l)::text),'[]'::jsonb) FROM node_atoms l JOIN nodes n ON n.node_id=l.node_id WHERE n.graph_id=ANY($1)"
    ).bind(graphs).fetch_one(pool).await.unwrap();
    state["other_jobs"] = sqlx::query_scalar::<_, Value>(
        "SELECT COALESCE(jsonb_agg(to_jsonb(j) ORDER BY id),'[]'::jsonb) FROM jobs j WHERE account_id=$1 AND id<>$2"
    ).bind(account).bind(job).fetch_one(pool).await.unwrap();
    state
}

/// Real worker refusal before any supported-publication or maintenance writes.
/// No private-I/O counter is claimed: the first production branch precedes R2.
pub async fn assert_legacy_refusal(
    pool: &PgPool,
    worker: &WorkerTenant,
    r2: &R2Client,
    account: Uuid,
    graphs: &[Uuid],
    job: Uuid,
) {
    let before = legacy_state(pool, account, graphs, job).await;
    let identity: (String, Value, Option<Uuid>) =
        sqlx::query_as("SELECT kind,payload,account_id FROM jobs WHERE id=$1")
            .bind(job)
            .fetch_one(pool)
            .await
            .unwrap();
    assert_eq!(identity.0, "PromoteNode");
    assert_eq!(identity.2, Some(account));
    assert_eq!(
        promote::tick(worker, Some(r2)).await.unwrap().processed,
        1,
        "the first tick dequeues one job, even though its consent is refused"
    );
    let row: (String, Option<String>, Option<String>, i32, bool, Option<String>, Option<String>) = sqlx::query_as(
        "SELECT status::text,error_message,failure_class,attempts,finished_at IS NOT NULL,outcome,idempotency_key FROM jobs WHERE id=$1"
    ).bind(job).fetch_one(pool).await.unwrap();
    assert_eq!(
        row,
        (
            "dead".into(),
            Some("selected evidence consent required".into()),
            Some("structural".into()),
            1,
            true,
            None,
            None
        )
    );
    let retained: (String, Value, Option<Uuid>) =
        sqlx::query_as("SELECT kind,payload,account_id FROM jobs WHERE id=$1")
            .bind(job)
            .fetch_one(pool)
            .await
            .unwrap();
    assert_eq!(
        retained, identity,
        "refusal must preserve job identity, not fabricate completion"
    );
    assert_eq!(legacy_state(pool, account, graphs, job).await, before,
        "the refused legacy job must not copy private inputs, create children or change named state");
    assert_eq!(promote::tick(worker, Some(r2)).await.unwrap().processed, 0);
    assert_eq!(legacy_state(pool, account, graphs, job).await, before);
}

/// Account deletion is guarded even for the fixture owner; enter the real
/// account/period transaction rather than swallowing a cleanup refusal.
pub async fn cleanup_account(pool: &PgPool, account: Uuid) {
    financial_fixture::execute(pool, account,
        sqlx::query("DELETE FROM accounts WHERE id=$1").bind(account),
    ).await.unwrap();
}

fn actor(user: Uuid, account: Uuid, role: &str) -> UserContext {
    UserContext {
        user_id: user,
        account_id: account,
        role: role.into(),
        device_id: "synthetic-publication-fixture".into(),
    }
}
fn tenant(pool: &PgPool) -> Tenant {
    Tenant {
        store: Some(Arc::new(PostgresStore::new(pool.clone()))),
        pool: Some(pool.clone()),
        decision: None,
        class: coralswarm_backend::route_class::RouteClass::Other,
    }
}
async fn response_json(r: axum::response::Response) -> (StatusCode, Value) {
    let status = r.status();
    let bytes = axum::body::to_bytes(r.into_body(), 1024 * 1024)
        .await
        .unwrap();
    (status, serde_json::from_slice(&bytes).unwrap())
}
async fn evidence(
    state: Arc<coralswarm_backend::AppState>,
    pool: &PgPool,
    user: UserContext,
    graph: Uuid,
    item: Uuid,
    version: Uuid,
) -> (StatusCode, Value) {
    let (status, body) = source_evidence::get_evidence(
        State(state),
        tenant(pool),
        Extension(user),
        Query(EvidenceQuery {
            graph_id: graph,
            source_item_id: item,
            version_id: Some(version),
            before: None,
            atoms_after: None,
            atom_id: None,
        }),
    )
    .await;
    (status, body.0)
}

/// Exercise the actual publication and reader handlers under a genuine ordinary
/// login. Owner setup inserts only synthetic immutable source evidence, never
/// a consent receipt, destination admission or pretend provider outcome.
pub async fn selected_encrypted_roundtrip(
    owner: &PgPool,
    r2: Arc<R2Client>,
    dek: &[u8; 32],
    account: Uuid,
    publisher: Uuid,
    count: usize,
    case_name: &str,
) {
    assert!(count == 1 || count == 3);
    sqlx::query("INSERT INTO account_members(account_id,user_id,role) VALUES($1,$2,'owner') ON CONFLICT DO NOTHING")
        .bind(account).bind(publisher).execute(owner).await.unwrap();
    let reader: Uuid = sqlx::query_scalar("INSERT INTO users(email) VALUES($1) RETURNING id")
        .bind(format!("{}@selected-reader.test", Uuid::new_v4()))
        .fetch_one(owner)
        .await
        .unwrap();
    sqlx::query("INSERT INTO account_members(account_id,user_id,role) VALUES($1,$2,'member')")
        .bind(account)
        .bind(reader)
        .execute(owner)
        .await
        .unwrap();
    let foreign: Uuid = sqlx::query_scalar("INSERT INTO users(email) VALUES($1) RETURNING id")
        .bind(format!("{}@selected-foreign.test", Uuid::new_v4()))
        .fetch_one(owner)
        .await
        .unwrap();
    let foreign_account: Uuid =
        sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES($1) RETURNING id")
            .bind(foreign)
            .fetch_one(owner)
            .await
            .unwrap();
    sqlx::query("INSERT INTO account_members(account_id,user_id,role) VALUES($1,$2,'owner') ON CONFLICT DO NOTHING")
        .bind(foreign_account).bind(foreign).execute(owner).await.unwrap();
    eprintln!("SELECTED_PUBLICATION_FIXTURE account={account} publisher={publisher} reader={reader} foreign={foreign} foreign_account={foreign_account}");
    let source:Uuid=sqlx::query_scalar("INSERT INTO graphs(account_id,name,kind,owner_user_id) VALUES($1,'Private selected MinIO originals','member',$2) RETURNING id")
        .bind(account).bind(publisher).fetch_one(owner).await.unwrap();
    let target:Uuid=sqlx::query_scalar("INSERT INTO graphs(account_id,name,kind) VALUES($1,'Explicit selected destination','org') RETURNING id")
        .bind(account).fetch_one(owner).await.unwrap();
    app_role::set_app_role_password_with_retry(owner).await;
    let app = PgPoolOptions::new()
        .max_connections(4)
        .connect_with(
            owner
                .connect_options()
                .as_ref()
                .clone()
                .username("coralswarm_app")
                .password(app_role::APP_ROLE_TEST_PASSWORD),
        )
        .await
        .unwrap();
    let privileges:(String,bool,bool,i64,i64)=sqlx::query_as("SELECT current_user::text,rolsuper,rolbypassrls,(SELECT count(*) FROM pg_tables WHERE schemaname='public' AND tableowner=current_user),(SELECT count(*) FROM pg_auth_members WHERE member=(SELECT oid FROM pg_roles WHERE rolname=current_user)) FROM pg_roles WHERE rolname=current_user")
        .fetch_one(&app).await.unwrap();
    assert_eq!(privileges, ("coralswarm_app".into(), false, false, 0, 0));
    let mut configured = coralswarm_backend::test_app_state_owned_with_db(app.clone(), None);
    configured.r2 = Some(r2.clone());
    let state = Arc::new(configured);
    let origin = json!({"service_id":"fixture","transport":"synthetic","content_kind":"note","capture_surface":"synthetic_demo"});
    let mut inputs = Vec::new();
    for ordinal in 0..count {
        let private_prefix = format!("PRIVATE_PREFIX_{}_{}", ordinal, Uuid::new_v4());
        let private_suffix = format!("PRIVATE_SUFFIX_{}_{}", ordinal, Uuid::new_v4());
        let private_title = format!("PRIVATE_TITLE_{}", Uuid::new_v4());
        let selected = format!("Explicitly selected coral {ordinal} 🙂 {}", Uuid::new_v4());
        let text = format!("{private_prefix}\n{selected}\n{private_suffix}");
        let item:Uuid=sqlx::query_scalar("INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id,origin) VALUES($1,'note','selected-minio:synthetic',$2,$3) RETURNING source_item_id")
            .bind(source).bind(format!("retained-{ordinal}-{}",Uuid::new_v4())).bind(&origin).fetch_one(owner).await.unwrap();
        let version = Uuid::new_v4();
        let key = r2
            .put_atom_blob(None, source, version, text.as_bytes(), dek)
            .await
            .unwrap();
        assert_eq!(key, blob_key_for(None, source, version).unwrap());
        assert_eq!(r2.get_atom_blob(&key, dek).await.unwrap(), text.as_bytes());
        let hash: String = sqlx::query_scalar("SELECT encode(sha256(convert_to($1,'UTF8')),'hex')")
            .bind(&text)
            .fetch_one(owner)
            .await
            .unwrap();
        sqlx::query("INSERT INTO source_versions(version_id,graph_id,source_item_id,source_id,source_type,provider_namespace,raw_blob_key,raw_inline,content_sha256,representation,source_metadata,origin,ingested_by,scope,extraction_version) SELECT $1,$2,$3,s.source_id,'note','selected-minio:synthetic',$4,NULL,$5,'original_text',$6,$7,$8,'private','source-atoms-v1' FROM source_items s WHERE s.graph_id=$2 AND s.source_item_id=$3")
            .bind(version).bind(source).bind(item).bind(&key).bind(&hash)
            .bind(json!({"title":private_title,"private_prefix":private_prefix,"private_suffix":private_suffix}))
            .bind(&origin).bind(publisher).execute(owner).await.unwrap();
        sqlx::query(
            "UPDATE source_items SET current_version_id=$2 WHERE graph_id=$3 AND source_item_id=$1",
        )
        .bind(item)
        .bind(version)
        .bind(source)
        .execute(owner)
        .await
        .unwrap();
        let semantic:Uuid=sqlx::query_scalar("INSERT INTO semantic_atoms(graph_id,source_version_id,ordinal,kind,statement,extraction_version,model) VALUES($1,$2,0,'observation',$3,'source-atoms-v1','synthetic-fixture') RETURNING semantic_atom_id")
            .bind(source).bind(version).bind(&selected).fetch_one(owner).await.unwrap();
        let start = private_prefix.chars().count() as i32 + 1;
        let end = start + selected.chars().count() as i32;
        let passage:Uuid=sqlx::query_scalar("INSERT INTO semantic_atom_passages(graph_id,source_version_id,semantic_atom_id,start_char,end_char,quote) VALUES($1,$2,$3,$4,$5,$6) RETURNING passage_id")
            .bind(source).bind(version).bind(semantic).bind(start).bind(end).bind(&selected).fetch_one(owner).await.unwrap();
        let proof:(bool,Option<String>,String,String,String)=sqlx::query_as("SELECT s.current_version_id=v.version_id,v.raw_inline,v.raw_blob_key,v.content_sha256,p.quote FROM source_items s JOIN source_versions v ON v.graph_id=s.graph_id AND v.source_item_id=s.source_item_id JOIN semantic_atom_passages p ON p.graph_id=v.graph_id AND p.source_version_id=v.version_id WHERE s.graph_id=$1 AND s.source_item_id=$2 AND v.version_id=$3 AND p.passage_id=$4")
            .bind(source).bind(item).bind(version).bind(passage).fetch_one(owner).await.unwrap();
        assert_eq!(
            proof,
            (true, None, key.clone(), hash, selected.clone()),
            "no inline shortcut or fabricated current-version/passage premise"
        );
        inputs.push((
            item,
            version,
            passage,
            start,
            end,
            text,
            selected,
            private_prefix,
            private_suffix,
            private_title,
            key,
        ));
    }
    let finance = account_effects(owner, account).await;
    let jobs:Value=sqlx::query_scalar("SELECT COALESCE(jsonb_agg(to_jsonb(j) ORDER BY id),'[]'::jsonb) FROM jobs j WHERE account_id=$1")
        .bind(account).fetch_one(owner).await.unwrap();
    let mut encrypted_marker = None;
    for (item, version, passage, start, end, text, selected, prefix, suffix, title, key) in &inputs
    {
        let consent = json!({"request_id":Uuid::new_v4(),"source_graph_id":source,"source_item_id":item,"source_version_id":version,"target_graph_id":target,"passages":[{"passage_id":passage,"start_char":start,"end_char":end}]});
        let source_before: Value = sqlx::query_scalar(
            "SELECT to_jsonb(v) FROM source_versions v WHERE graph_id=$1 AND version_id=$2",
        )
        .bind(source)
        .bind(version)
        .fetch_one(owner)
        .await
        .unwrap();
        assert!(source_before["raw_inline"].is_null());
        let selected_hash: String =
            sqlx::query_scalar("SELECT encode(sha256(convert_to($1,'UTF8')),'hex')")
                .bind(selected)
                .fetch_one(owner)
                .await
                .unwrap();
        let publish = |q: Value| {
            selected_publication::publish(
                State(state.clone()),
                tenant(&app),
                Extension(actor(publisher, account, "owner")),
                LibraryJson(serde_json::from_value(q).unwrap()),
            )
        };
        let (status, receipt) = response_json(publish(consent.clone()).await).await;
        assert_eq!(status, StatusCode::CREATED, "{receipt}");
        assert_eq!(
            receipt["processing_status"],
            "deterministic_selected_passages"
        );
        assert_eq!(
            receipt["managed_consumers_status"],
            "pending_explicit_budget"
        );
        let dest_item: Uuid = serde_json::from_value(receipt["source_item_id"].clone()).unwrap();
        let dest_version: Uuid =
            serde_json::from_value(receipt["source_version_id"].clone()).unwrap();
        let publication: Uuid = serde_json::from_value(receipt["publication_id"].clone()).unwrap();
        let published:(String,Option<String>,Value,Value,String,String,String)=sqlx::query_as("SELECT v.raw_inline,v.raw_blob_key,v.source_metadata,v.origin,a.statement,a.model,p.quote FROM source_versions v JOIN semantic_atoms a ON a.graph_id=v.graph_id AND a.source_version_id=v.version_id JOIN semantic_atom_passages p ON p.graph_id=a.graph_id AND p.source_version_id=a.source_version_id AND p.semantic_atom_id=a.semantic_atom_id WHERE v.graph_id=$1 AND v.version_id=$2")
            .bind(target).bind(dest_version).fetch_one(owner).await.unwrap();
        assert_eq!(published.0, *selected);
        assert_eq!(published.1, None);
        assert_eq!(published.4, *selected);
        assert_eq!(published.5, "deterministic-selection");
        assert_eq!(published.6, *selected);
        assert_eq!(published.2["independence"], "derived_copy");
        assert_eq!(published.2["review_status"], "user_selected");
        assert_eq!(published.3["transport"], "synthetic");
        let bound:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM selected_evidence_publications p JOIN corpus_capture_admissions a ON a.graph_id=p.target_graph_id AND a.source_item_id=p.target_source_item_id AND a.source_version_id=p.target_source_version_id JOIN source_versions d ON d.graph_id=p.target_graph_id AND d.source_item_id=p.target_source_item_id AND d.version_id=p.target_source_version_id WHERE p.publication_id=$1 AND p.account_id=$2 AND p.owner_user_id=$3 AND p.source_graph_id=$4 AND p.source_item_id=$5 AND p.source_version_id=$6 AND p.target_graph_id=$7 AND p.target_source_item_id=$8 AND p.target_source_version_id=$9 AND p.request_id=$10 AND p.selection=$11 AND p.passage_ids=$12 AND p.selected_sha256=$13 AND d.content_sha256=$13 AND d.raw_inline=$14 AND p.status='active' AND a.state='processed' AND a.reason='deterministic_selected_passages_no_model' AND a.run_id IS NULL)")
            .bind(publication).bind(account).bind(publisher).bind(source).bind(item).bind(version).bind(target).bind(dest_item).bind(dest_version)
            .bind(serde_json::from_value::<Uuid>(consent["request_id"].clone()).unwrap())
            .bind(&consent["passages"]).bind(vec![*passage]).bind(&selected_hash).bind(selected)
            .fetch_one(owner).await.unwrap();
        assert!(
            bound,
            "only the actual API may write exact consent/admission authority"
        );
        let read = evidence(
            state.clone(),
            &app,
            actor(reader, account, "member"),
            target,
            dest_item,
            dest_version,
        )
        .await;
        assert_eq!(read.0, StatusCode::OK, "{}", read.1);
        assert_eq!(read.1["selected_version"]["raw_text"], *selected);
        assert_eq!(read.1["selected_version"]["text_status"], "available");
        for emitted in [&receipt, &read.1, &published.2, &published.3] {
            let all = emitted.to_string();
            for secret in [prefix, suffix, title, key] {
                assert!(
                    !all.contains(secret.as_str()),
                    "unselected private input leaked: {all}"
                );
            }
            for hidden in [
                source.to_string(),
                item.to_string(),
                version.to_string(),
                passage.to_string(),
            ] {
                assert!(
                    !all.contains(hidden.as_str()),
                    "private origin identifiers leaked: {all}"
                );
            }
        }
        for (who, acct, g, i, v) in [
            (reader, account, source, *item, *version),
            (foreign, foreign_account, target, dest_item, dest_version),
        ] {
            let denied = evidence(
                state.clone(),
                &app,
                actor(who, acct, if who == reader { "member" } else { "owner" }),
                g,
                i,
                v,
            )
            .await;
            assert_eq!(
                denied,
                (
                    StatusCode::NOT_FOUND,
                    json!({"error":"source not found or access denied"})
                ),
                "read denial must be generic"
            );
        }
        assert_eq!(
            response_json(publish(consent).await).await,
            (StatusCode::OK, receipt)
        );
        assert_eq!(
            r2.get_atom_blob(key, dek).await.unwrap(),
            text.as_bytes(),
            "publication does not alter the encrypted original"
        );
        let source_after: Value = sqlx::query_scalar(
            "SELECT to_jsonb(v) FROM source_versions v WHERE graph_id=$1 AND version_id=$2",
        )
        .bind(source)
        .bind(version)
        .fetch_one(owner)
        .await
        .unwrap();
        assert_eq!(
            source_after, source_before,
            "selection must not rewrite its immutable original"
        );
        encrypted_marker.get_or_insert_with(|| json!({
            "case":case_name,"account_id":account,"source_version_id":version,
            "raw_sha256":source_before["content_sha256"],"selected_sha256":selected_hash,
            "originals_verified":count,"verified":true,
        }));
    }
    let count_now: i64 =
        sqlx::query_scalar("SELECT count(*) FROM source_versions WHERE graph_id=$1")
            .bind(target)
            .fetch_one(owner)
            .await
            .unwrap();
    assert_eq!(
        count_now, count as i64,
        "one exact destination per original despite identical-consent retries"
    );
    let legacy_copies: i64 = sqlx::query_scalar("SELECT count(*) FROM atoms WHERE graph_id=$1")
        .bind(target)
        .fetch_one(owner)
        .await
        .unwrap();
    assert_eq!(
        legacy_copies, 0,
        "selected publication never silently copies whole legacy corals"
    );
    assert_eq!(account_effects(owner, account).await, finance);
    let after_jobs:Value=sqlx::query_scalar("SELECT COALESCE(jsonb_agg(to_jsonb(j) ORDER BY id),'[]'::jsonb) FROM jobs j WHERE account_id=$1")
        .bind(account).fetch_one(owner).await.unwrap();
    assert_eq!(
        after_jobs, jobs,
        "deterministic publication must not dispatch generic/model work"
    );
    // Only after all real encrypted-byte, consent, reader and no-dispatch checks.
    // Synthetic IDs/hashes only; never keys, credentials or plaintext.
    eprintln!("CORALSWARM_R2_ENCRYPTED_FIXTURE_V1 {}", encrypted_marker.unwrap());
    app.close().await;
    cleanup_account(owner, foreign_account).await;
    sqlx::query("DELETE FROM users WHERE id=ANY($1)")
        .bind(vec![reader, foreign])
        .execute(owner)
        .await
        .unwrap();
}
