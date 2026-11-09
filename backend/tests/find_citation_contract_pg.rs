//! Citation contract diagnostic: exact-source Unicode spans survive retrieval
//! and saved-page publication; direct SQL corruption is not a valid quote edit.
#[path = "support/page_model_fixture.rs"]
mod fixture;
use coralswarm_backend::{
    library_read::{self, LibraryScope},
    read_models::{
        self as rm,
        chats_detail::{ConversationDetail, DetailParams},
        ReadModel,
    },
    store::PostgresStore,
    tenancy::{
        extractor::Tenant,
        runtime::{TenancyDecision, TenantContext},
        scope::begin_tenant_scoped,
    },
    workers::read_model_refresh,
    UserContext,
};
use fixture::Fixture;
use serde_json::{json, Value};
use std::sync::Arc;
use uuid::Uuid;
async fn candidates(f: &Fixture, scope: &LibraryScope) -> Value {
    let tenant = Tenant {
        store: Some(Arc::new(PostgresStore::new(f.app.clone()))),
        pool: Some(f.app.clone()),
        decision: if f.org {
            Some(TenancyDecision::Org(TenantContext {
                org_id: "org_read_models".into(),
                account_id: f.account,
                user_id: f.user,
                pool: f.app.clone(),
                page_pool: None,
            }))
        } else {
            None
        },
        class: coralswarm_backend::route_class::RouteClass::Other,
    };
    let user = UserContext {
        user_id: f.user,
        account_id: f.account,
        role: "member".into(),
        device_id: "citation-contract".into(),
    };
    library_read::retrieve_ask_candidates(
        &tenant,
        &user,
        scope.clone(),
        &["launch".into()],
        None,
        None,
        10,
    )
    .await
    .unwrap_or_else(|r| panic!("candidate read failed {}", r.status()))
}
async fn saved(f: &Fixture, scope: &LibraryScope, value: &Value, stage: &str) -> Value {
    let conv:Uuid=sqlx::query_scalar("INSERT INTO conversations(account_id,graph_id,user_id,library_scope) VALUES($1,$2,$3,$4) RETURNING id").bind(f.account).bind(f.graph).bind(f.user).bind(json!(scope)).fetch_one(&f.owner).await.unwrap();
    sqlx::query("INSERT INTO conversation_messages(conversation_id,seq,role,text) VALUES($1,0,'user','Find launch')").bind(conv).execute(&f.owner).await.unwrap();
    let cites: Vec<Value> = value["outcomes"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|o| o["citations"].as_array().unwrap().clone())
        .collect();
    let timing = json!({"library_scope":scope,"outcomes":value["outcomes"],"retained_citations":cites,"legacy_evidence":[]});
    let assistant:Uuid=sqlx::query_scalar("INSERT INTO conversation_messages(conversation_id,seq,role,text,mode,timing,scope_graph_ids) VALUES($1,1,'assistant','', 'retrieval_only',$2,$3) RETURNING id").bind(conv).bind(&timing).bind(json!([f.graph])).fetch_one(&f.owner).await.unwrap();
    let support:Vec<Value>=cites.iter().map(|c| json!({"kind":"source_passage","graph_id":c["graph_id"],"source_item_id":c["source_item_id"],"source_version_id":c["source_version_id"],"semantic_atom_id":c["semantic_atom_id"],"passage_id":c["passage_id"]})).collect();
    sqlx::query("INSERT INTO answer_support_receipts(receipt_id,graph_id,owner_user_id,support,evidence_complete) VALUES($1,$2,$3,$4,true)").bind(assistant).bind(f.graph).bind(f.user).bind(json!(support)).execute(&f.owner).await.unwrap();
    let mut tx = begin_tenant_scoped(&f.app, f.account, f.user)
        .await
        .unwrap();
    let s = rm::Viewer::load(&mut tx, f.account, f.user)
        .await
        .unwrap()
        .scope::<ConversationDetail>(DetailParams { id: conv })
        .unwrap();
    tx.rollback().await.unwrap();
    rm::store::request_refresh::<ConversationDetail>(&f.app, &s)
        .await
        .unwrap();
    let claims = rm::store::claim(&f.app).await.unwrap();
    let claim = claims
        .iter()
        .find(|c| c.model == ConversationDetail::NAME && c.scope_key == s.key())
        .unwrap();
    let (payload, snapshot) = read_model_refresh::compute(&f.worker(), claim, &f.control)
        .await
        .unwrap();
    assert!(
        rm::store::publish(&f.app, claim, payload.clone(), &snapshot)
            .await
            .unwrap()
    );
    let mut tx = begin_tenant_scoped(&f.app, f.account, f.user)
        .await
        .unwrap();
    let stored = rm::store::read::<ConversationDetail>(&mut tx, &s)
        .await
        .unwrap();
    assert!(stored.computed_at.is_some());
    let body = serde_json::to_value(stored).unwrap();
    if let Ok(path) = std::env::var("CS_CITATION_PROOF_DIR") {
        std::fs::write(
            format!(
                "{path}/{}-{stage}.json",
                if f.org { "tenant" } else { "personal" }
            ),
            serde_json::to_vec_pretty(&body).unwrap(),
        )
        .unwrap();
    }
    body
}
fn cite(value: &Value) -> &Value {
    &value["outcomes"][0]["citations"][0]
}
fn exact(c: &Value, raw: &str) {
    let start = c["start_char"].as_u64().unwrap() as usize;
    let end = c["end_char"].as_u64().unwrap() as usize;
    let quote = c["quote"].as_str().unwrap();
    assert_eq!(end - start, quote.chars().count());
    assert_eq!(
        raw.chars()
            .skip(start)
            .take(end - start)
            .collect::<String>(),
        quote
    );
}
async fn contract(org: bool) {
    let f = Fixture::new(org).await;
    if let Ok(path) = std::env::var("CS_CITATION_PROOF_DIR") {
        use std::io::Write;
        writeln!(std::fs::OpenOptions::new().create(true).append(true).open(format!("{path}/owned-resources.jsonl")).unwrap(),"{}",json!({"database":f.owner.connect_options().get_database(),"control_database":f.control_owner.connect_options().get_database(),"app_role":f.app.connect_options().get_username(),"control_role":f.control.connect_options().get_username()})).unwrap();
    }
    let prefix = "Préface 🚀\n";
    let quote = "Café e\u{301} launch chose tiers 🐠.";
    let raw = format!("{prefix}{quote}\nTom owns launch.");
    let start = prefix.chars().count() as i32;
    let end = start + quote.chars().count() as i32;
    let item:Uuid=sqlx::query_scalar("INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id) VALUES($1,'note','fixture:citation-contract','citation-contract') RETURNING source_item_id").bind(f.graph).fetch_one(&f.owner).await.unwrap();
    let version:Uuid=sqlx::query_scalar("INSERT INTO source_versions(graph_id,source_item_id,source_type,provider_namespace,source_id,raw_inline,extraction_version,extraction_status) VALUES($1,$2,'note','fixture:citation-contract','citation-contract',$3,'v1','done') RETURNING version_id").bind(f.graph).bind(item).bind(&raw).fetch_one(&f.owner).await.unwrap();
    sqlx::query("UPDATE source_items SET current_version_id=$2 WHERE source_item_id=$1")
        .bind(item)
        .bind(version)
        .execute(&f.owner)
        .await
        .unwrap();
    let atom:Uuid=sqlx::query_scalar("INSERT INTO semantic_atoms(graph_id,source_version_id,ordinal,kind,statement,extraction_version,model) VALUES($1,$2,0,'fact',$3,'v1','fixture') RETURNING semantic_atom_id").bind(f.graph).bind(version).bind(quote).fetch_one(&f.owner).await.unwrap();
    let passage:Uuid=sqlx::query_scalar("INSERT INTO semantic_atom_passages(graph_id,source_version_id,semantic_atom_id,start_char,end_char,quote) VALUES($1,$2,$3,$4,$5,$6) RETURNING passage_id").bind(f.graph).bind(version).bind(atom).bind(start).bind(end).bind(quote).fetch_one(&f.owner).await.unwrap();
    let mut scope = LibraryScope::exact_node(f.graph, Uuid::nil());
    scope.node_id = None;
    scope.source_item_id = Some(item);
    scope.normalize().unwrap();
    let initial = candidates(&f, &scope).await;
    exact(cite(&initial), &raw);
    saved(&f, &scope, &initial, "initial").await;
    // The browser fixture's exact unsafe operation is a negative control. It
    // passes permissive SQL CHECKs but violates the internal source-span contract.
    sqlx::query(
        "UPDATE semantic_atom_passages SET quote='Current evidence changed' WHERE passage_id=$1",
    )
    .bind(passage)
    .execute(&f.owner)
    .await
    .unwrap();
    let admissible:bool=sqlx::query_scalar("SELECT end_char-start_char=char_length(p.quote) AND substring(v.raw_inline FROM p.start_char+1 FOR p.end_char-p.start_char)=p.quote FROM semantic_atom_passages p JOIN source_versions v ON v.graph_id=p.graph_id AND v.version_id=p.source_version_id WHERE p.passage_id=$1").bind(passage).fetch_one(&f.owner).await.unwrap();
    assert!(!admissible);
    let corrupted = candidates(&f, &scope).await;
    saved(&f, &scope, &corrupted, "corrupt-fixture").await;
    // Restore the exact original retained-source slice, then narrow that slice
    // with raw proof. Never invent a quote or adjust offsets to fit invented text.
    sqlx::query("UPDATE semantic_atom_passages p SET quote=substring(v.raw_inline FROM p.start_char+1 FOR p.end_char-p.start_char) FROM source_versions v WHERE v.graph_id=p.graph_id AND v.version_id=p.source_version_id AND p.passage_id=$1").bind(passage).execute(&f.owner).await.unwrap();
    let changed=sqlx::query("WITH k AS (SELECT p.passage_id,p.start_char+1 s,p.end_char e,substring(v.raw_inline FROM p.start_char+2 FOR p.end_char-p.start_char-1) q FROM semantic_atom_passages p JOIN source_versions v ON v.graph_id=p.graph_id AND v.version_id=p.source_version_id WHERE p.passage_id=$1 AND v.raw_inline IS NOT NULL AND p.end_char-p.start_char>1 AND substring(v.raw_inline FROM p.start_char+1 FOR p.end_char-p.start_char)=p.quote) UPDATE semantic_atom_passages p SET start_char=k.s,end_char=k.e,quote=k.q FROM k WHERE p.passage_id=k.passage_id").bind(passage).execute(&f.owner).await.unwrap().rows_affected();
    assert_eq!(changed, 1);
    let valid = candidates(&f, &scope).await;
    exact(cite(&valid), &raw);
    assert_eq!(cite(&valid)["start_char"], start + 1);
    assert_eq!(cite(&valid)["end_char"], end);
    assert_eq!(cite(&valid)["source_version_id"], json!(version));
    saved(&f, &scope, &valid, "valid-source-backed-edit").await;
    let unchanged: String =
        sqlx::query_scalar("SELECT raw_inline FROM source_versions WHERE version_id=$1")
            .bind(version)
            .fetch_one(&f.owner)
            .await
            .unwrap();
    assert_eq!(unchanged, raw);
    f.close().await;
}
#[tokio::test]
async fn personal_source_backed_citation_span_contract() {
    contract(false).await;
}
#[tokio::test]
async fn tenant_source_backed_citation_span_contract() {
    contract(true).await;
}
