//! Saved Find cards: real registered worker, non-bypass logins, both planes.
#[path = "support/page_model_api.rs"]
mod api;
#[path = "support/page_model_fixture.rs"]
mod fixture;
use coralswarm_backend::{
    library_read::LibraryScope,
    read_models::{
        self as rm,
        chats_detail::{ConversationDetail, DetailParams},
        ReadModel,
    },
    tenancy::scope::begin_tenant_scoped,
    workers::read_model_refresh,
};
use fixture::Fixture;
use serde_json::{json, Value};
use uuid::Uuid;

static ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
struct Flag(Option<std::ffi::OsString>);
impl Flag {
    fn on() -> Self {
        let old = std::env::var_os("CHAT_FIND_RESULTS_ENABLED");
        std::env::set_var("CHAT_FIND_RESULTS_ENABLED", "true");
        Self(old)
    }
}
impl Drop for Flag {
    fn drop(&mut self) {
        match &self.0 {
            Some(v) => std::env::set_var("CHAT_FIND_RESULTS_ENABLED", v),
            None => std::env::remove_var("CHAT_FIND_RESULTS_ENABLED"),
        }
    }
}
async fn scope(f: &Fixture, id: Uuid) -> rm::Scope<DetailParams> {
    let mut tx = begin_tenant_scoped(&f.app, f.account, f.user)
        .await
        .unwrap();
    rm::Viewer::load(&mut tx, f.account, f.user)
        .await
        .unwrap()
        .scope::<ConversationDetail>(DetailParams { id })
        .unwrap()
}
async fn fill(f: &Fixture, id: Uuid) -> Value {
    let s = scope(f, id).await;
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
    assert_eq!(
        serde_json::to_value(stored.payload).unwrap(),
        payload,
        "published indexed readback"
    );
    payload
}
async fn served_stale(f: &Fixture, id: Uuid) {
    let s = scope(f, id).await;
    served_stale_scope(f, &s).await;
}
async fn served_stale_scope(f: &Fixture, s: &rm::Scope<DetailParams>) {
    let mut tx = begin_tenant_scoped(&f.app, f.account, f.user)
        .await
        .unwrap();
    let stored = rm::store::read::<ConversationDetail>(&mut tx, s)
        .await
        .unwrap();
    assert!(
        stored.computed_at.is_some() && stored.stale,
        "reduction immediately marks the warm payload stale, never current"
    );
}
async fn answer(
    f: &Fixture,
    conv: Uuid,
    seq: i32,
    question: &str,
    mode: &str,
    payload: &Value,
    support: Option<&Value>,
    library: &LibraryScope,
) -> Uuid {
    sqlx::query("INSERT INTO conversation_messages(conversation_id,seq,role,text,library_scope,scope_graph_ids) VALUES($1,$2,'user',$3,$4,$5)")
        .bind(conv).bind(seq).bind(question).bind(json!(library)).bind(json!([f.graph])).execute(&f.owner).await.unwrap();
    let id:Uuid = sqlx::query_scalar("INSERT INTO conversation_messages(conversation_id,seq,role,text,mode,timing,scope_graph_ids) VALUES($1,$2,'assistant','Certified answer',$3,$4,$5) RETURNING id")
        .bind(conv).bind(seq+1).bind(mode).bind(payload).bind(json!([f.graph])).fetch_one(&f.owner).await.unwrap();
    if let Some(support) = support {
        sqlx::query("INSERT INTO answer_support_receipts(receipt_id,graph_id,owner_user_id,support,evidence_complete) VALUES($1,$2,$3,$4,true)")
            .bind(id).bind(f.graph).bind(f.user).bind(support).execute(&f.owner).await.unwrap();
    }
    id
}
#[track_caller]
fn msg(v: &Value, id: Uuid) -> &Value {
    v["messages"]
        .as_array()
        .unwrap()
        .iter()
        .find(|m| m["id"] == json!(id))
        .unwrap_or_else(|| panic!("message {id} absent from {v}"))
}
async fn contract(org: bool) {
    let _lock = ENV_LOCK.lock().await;
    let _flag = Flag::on();
    let f = Fixture::new(org).await;
    let api = api::Api::new(&f).await;
    let team = if org {
        sqlx::query("UPDATE graphs SET kind='team',owner_user_id=NULL WHERE id=$1")
            .bind(f.graph)
            .execute(&f.owner)
            .await
            .unwrap();
        let team:Uuid=sqlx::query_scalar("INSERT INTO teams(account_id,graph_id,name,created_by) VALUES($1,$2,'Find floor',$3) RETURNING id").bind(f.account).bind(f.graph).bind(f.user).fetch_one(&f.owner).await.unwrap();
        sqlx::query("INSERT INTO team_members(team_id,user_id,role) VALUES($1,$2,'member')")
            .bind(team)
            .bind(f.user)
            .execute(&f.owner)
            .await
            .unwrap();
        sqlx::query("INSERT INTO team_seat_grants(account_id,team_id,user_id,source,history_from) VALUES($1,$2,$3,'fixture',NULL)").bind(f.account).bind(team).bind(f.user).execute(&f.owner).await.unwrap();
        Some(team)
    } else {
        None
    };
    let mut library = LibraryScope::exact_node(f.graph, Uuid::nil());
    library.node_id = None;
    library.normalize().unwrap();
    let conv:Uuid=sqlx::query_scalar("INSERT INTO conversations(account_id,graph_id,user_id,library_scope) VALUES($1,$2,$3,$4) RETURNING id")
        .bind(f.account).bind(f.graph).bind(f.user).bind(json!(library)).fetch_one(&f.owner).await.unwrap();
    let item:Uuid=sqlx::query_scalar("INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id) VALUES($1,'note','fixture:find-page','find-page') RETURNING source_item_id").bind(f.graph).fetch_one(&f.owner).await.unwrap();
    let version:Uuid=sqlx::query_scalar("INSERT INTO source_versions(graph_id,source_item_id,source_type,provider_namespace,source_id,raw_inline,source_metadata,occurred_at,extraction_version,extraction_status,participants) VALUES($1,$2,'note','fixture:find-page','find-page','Exact quoted evidence','{\"title\":\"Current source title\"}',now()-interval '1 day','v1','done',ARRAY['Ada']) RETURNING version_id")
        .bind(f.graph).bind(item).fetch_one(&f.owner).await.unwrap();
    sqlx::query("UPDATE source_items SET current_version_id=$2 WHERE source_item_id=$1")
        .bind(item)
        .bind(version)
        .execute(&f.owner)
        .await
        .unwrap();
    let semantic:Uuid=sqlx::query_scalar("INSERT INTO semantic_atoms(graph_id,source_version_id,ordinal,kind,statement,entities,extraction_version,model) VALUES($1,$2,0,'decision','Ship the launch','[{\"mention\":\"Launch\"}]','v1','fixture') RETURNING semantic_atom_id").bind(f.graph).bind(version).fetch_one(&f.owner).await.unwrap();
    let passage:Uuid=sqlx::query_scalar("INSERT INTO semantic_atom_passages(graph_id,source_version_id,semantic_atom_id,start_char,end_char,quote) VALUES($1,$2,$3,0,21,'Exact quoted evidence') RETURNING passage_id").bind(f.graph).bind(version).bind(semantic).fetch_one(&f.owner).await.unwrap();
    let node:Uuid=sqlx::query_scalar("INSERT INTO nodes(graph_id,type,title,body) VALUES($1,'entity','Launch','{}') RETURNING node_id").bind(f.graph).fetch_one(&f.owner).await.unwrap();
    sqlx::query(
        "INSERT INTO node_semantic_atoms(graph_id,node_id,semantic_atom_id) VALUES($1,$2,$3)",
    )
    .bind(f.graph)
    .bind(node)
    .bind(semantic)
    .execute(&f.owner)
    .await
    .unwrap();
    let support = json!([{"kind":"source_passage","graph_id":f.graph,"source_item_id":item,"source_version_id":version,"semantic_atom_id":semantic,"passage_id":passage}]);
    let evidence = json!({"retained_citations":[{"kind":"source_passage","graph_id":f.graph,"source_item_id":item,"source_version_id":version,"semantic_atom_id":semantic,"passage_id":passage,"quote":"Exact quoted evidence"}]});
    let find = answer(
        &f,
        conv,
        0,
        "Find the launch note",
        "synthesized",
        &evidence,
        Some(&support),
        &library,
    )
    .await;
    let other = answer(
        &f,
        conv,
        2,
        "Explain the launch",
        "synthesized",
        &evidence,
        Some(&support),
        &library,
    )
    .await;
    let failed = answer(
        &f,
        conv,
        4,
        "Find the launch note",
        "failed",
        &evidence,
        Some(&support),
        &library,
    )
    .await;
    let unsupported = answer(
        &f,
        conv,
        6,
        "Find the launch note",
        "legacy",
        &evidence,
        Some(&support),
        &library,
    )
    .await;
    let missing = answer(
        &f,
        conv,
        8,
        "Find the launch note",
        "synthesized",
        &evidence,
        None,
        &library,
    )
    .await;
    let empty = answer(
        &f,
        conv,
        10,
        "Find a missing source",
        "retrieval_only",
        &json!({}),
        Some(&json!([])),
        &library,
    )
    .await;
    let legacy_atom = f.atom("find-page:chunk:0").await;
    let fingerprint:String=sqlx::query_scalar("SELECT encode(sha256(convert_to(jsonb_build_array(content_inline,header_len,source_type,source_id,timestamp,created_at)::text,'UTF8')),'hex') FROM atoms WHERE atom_id=$1").bind(legacy_atom).fetch_one(&f.owner).await.unwrap();
    let legacy=answer(&f,conv,12,"Find the old launch note","retrieval_only",
        &json!({"legacy_evidence":[{"graph_id":f.graph,"atom_id":legacy_atom}]}),
        Some(&json!([{"kind":"legacy_chunk","graph_id":f.graph,"atom_id":legacy_atom,"input_sha256":fingerprint}])),&library).await;
    // Establish the real request's member-ocean lifecycle before warming its
    // scope key (a team-only fixture initially has no personal member ocean).
    let (status, cold) = api
        .call("GET", &format!("/v1/load/conversations/{conv}"), &api.admin)
        .await;
    assert_eq!(status, axum::http::StatusCode::OK, "{cold}");
    assert!(cold["computed_at"].is_null());
    let warm_scope = scope(&f, conv).await;
    let v = fill(&f, conv).await;
    let cards = &msg(&v, find)["source_results"];
    assert_eq!(cards.as_array().unwrap().len(), 1);
    assert_eq!(cards[0]["title"], "Current source title");
    assert_eq!(cards[0]["quotes"][0]["quote"], "Exact quoted evidence");
    assert_eq!(cards[0]["outcome"], "Ship the launch");
    assert_eq!(cards[0]["people"], json!(["Ada"]));
    assert_eq!(cards[0]["reefs"][0]["title"], "Launch");
    assert_eq!(cards[0]["best"], true);
    assert_eq!(
        msg(&v, legacy)["source_results"][0]["source_item_id"],
        json!(item)
    );
    assert_eq!(
        msg(&v, legacy)["source_results"][0]["quotes"],
        json!([]),
        "batch hydration never shares one answer's quotes with another"
    );
    for id in [other, failed, unsupported, missing] {
        assert!(msg(&v, id).get("source_results").is_none());
    }
    assert_eq!(msg(&v, empty)["source_results"], json!([]));
    // Flag off removes a previously published field on the real GET too.
    std::env::set_var("CHAT_FIND_RESULTS_ENABLED", "false");
    let (status, warm) = api
        .call("GET", &format!("/v1/load/conversations/{conv}"), &api.admin)
        .await;
    assert_eq!(status, axum::http::StatusCode::OK, "{warm}");
    assert!(msg(&warm["payload"], find).get("source_results").is_none());
    let off = fill(&f, conv).await;
    assert!(msg(&off, find).get("source_results").is_none());
    std::env::set_var("CHAT_FIND_RESULTS_ENABLED", "true");
    fill(&f, conv).await;
    // Current metadata, not an earlier live receipt, is certified on recompute.
    sqlx::query("UPDATE source_versions SET source_metadata='{\"title\":\"Updated title\"}' WHERE version_id=$1").bind(version).execute(&f.owner).await.unwrap();
    served_stale(&f, conv).await;
    let v = fill(&f, conv).await;
    assert_eq!(msg(&v, find)["source_results"][0]["title"], "Updated title");
    // Identity-only receipts cannot certify an obsolete stored quote after a
    // passage edit. Cards fail closed without changing saved-answer semantics.
    sqlx::query(
        "UPDATE semantic_atom_passages SET quote='Changed passage text' WHERE passage_id=$1",
    )
    .bind(passage)
    .execute(&f.owner)
    .await
    .unwrap();
    served_stale(&f, conv).await;
    let v = fill(&f, conv).await;
    assert!(msg(&v, find)["source_results"]
        .as_array()
        .is_none_or(|v| v.is_empty()));
    sqlx::query(
        "UPDATE semantic_atom_passages SET quote='Exact quoted evidence' WHERE passage_id=$1",
    )
    .bind(passage)
    .execute(&f.owner)
    .await
    .unwrap();
    let v = fill(&f, conv).await;
    assert_eq!(msg(&v, find)["source_results"].as_array().unwrap().len(), 1);
    // Linked reef reduction immediately withdraws the entire warm envelope.
    sqlx::query("DELETE FROM node_semantic_atoms WHERE node_id=$1")
        .bind(node)
        .execute(&f.owner)
        .await
        .unwrap();
    served_stale(&f, conv).await;
    let v = fill(&f, conv).await;
    assert_eq!(msg(&v, find)["source_results"][0]["reefs"], json!([]));
    // Losing a source grant cannot retain title, quotes or cached cards.
    sqlx::query("UPDATE source_items SET access_state='restricted' WHERE source_item_id=$1")
        .bind(item)
        .execute(&f.owner)
        .await
        .unwrap();
    served_stale(&f, conv).await;
    let v = fill(&f, conv).await;
    assert!(msg(&v, find).get("source_results").is_none());
    assert_eq!(msg(&v, find)["support_status"], "unavailable");
    assert!(!v.to_string().contains("Exact quoted evidence"));
    sqlx::query("UPDATE source_items SET access_state='permitted' WHERE source_item_id=$1")
        .bind(item)
        .execute(&f.owner)
        .await
        .unwrap();
    fill(&f, conv).await;
    // Passage/support loss must also invalidate a published answer and its cards.
    sqlx::query("DELETE FROM semantic_atom_passages WHERE passage_id=$1")
        .bind(passage)
        .execute(&f.owner)
        .await
        .unwrap();
    served_stale(&f, conv).await;
    let v = fill(&f, conv).await;
    assert!(msg(&v, find).get("source_results").is_none());
    assert_eq!(msg(&v, find)["support_status"], "unavailable");
    // Restore a valid support passage to prove the floor/revoke paths are nonvacuous.
    sqlx::query("INSERT INTO semantic_atom_passages(passage_id,graph_id,source_version_id,semantic_atom_id,start_char,end_char,quote) VALUES($1,$2,$3,$4,0,21,'Exact quoted evidence')").bind(passage).bind(f.graph).bind(version).bind(semantic).execute(&f.owner).await.unwrap();
    let v = fill(&f, conv).await;
    assert!(
        msg(&v, find).get("source_results").is_none(),
        "revoked receipt never resurrects when its passage reappears"
    );
    // A new successful Ask would mint new receipts. Use new message identity,
    // rather than re-certifying the irreversibly incomplete original receipt.
    let find = answer(
        &f,
        conv,
        14,
        "Find the launch note",
        "synthesized",
        &evidence,
        Some(&support),
        &library,
    )
    .await;
    let legacy_atom = f.atom("find-page:chunk:1").await;
    let fingerprint:String=sqlx::query_scalar("SELECT encode(sha256(convert_to(jsonb_build_array(content_inline,header_len,source_type,source_id,timestamp,created_at)::text,'UTF8')),'hex') FROM atoms WHERE atom_id=$1").bind(legacy_atom).fetch_one(&f.owner).await.unwrap();
    let legacy=answer(&f,conv,16,"Find the old launch note","retrieval_only",
        &json!({"legacy_evidence":[{"graph_id":f.graph,"atom_id":legacy_atom}]}),
        Some(&json!([{"kind":"legacy_chunk","graph_id":f.graph,"atom_id":legacy_atom,"input_sha256":fingerprint}])),&library).await;
    let v = fill(&f, conv).await;
    assert_eq!(msg(&v, find)["source_results"].as_array().unwrap().len(), 1);
    assert_eq!(
        msg(&v, legacy)["source_results"].as_array().unwrap().len(),
        1
    );
    if let Some(team) = team {
        sqlx::query(
            "UPDATE team_seat_grants SET history_from=now() WHERE team_id=$1 AND user_id=$2",
        )
        .bind(team)
        .bind(f.user)
        .execute(&f.owner)
        .await
        .unwrap();
        // The floor moved. Where floors enter the scope key the current scope
        // is a never-computed new key; the warm key is only ever served stale.
        let current = scope(&f, conv).await;
        if current.key() == warm_scope.key() {
            served_stale_scope(&f, &current).await;
        } else {
            let mut tx = begin_tenant_scoped(&f.app, f.account, f.user)
                .await
                .unwrap();
            assert!(rm::store::read::<ConversationDetail>(&mut tx, &current)
                .await
                .unwrap()
                .computed_at
                .is_none());
        }
        served_stale_scope(&f, &warm_scope).await;
        let v = fill(&f, conv).await;
        // Card safety permits either omitted cards on an unavailable answer or
        // an authoritative empty list. Core message/prose admission is asserted
        // separately by the saved-answer history-floor regression.
        let tiles = msg(&v, find).get("source_results");
        assert!(tiles.is_none() || tiles == Some(&json!([])));
        sqlx::query(
            "UPDATE team_seat_grants SET history_from=NULL WHERE team_id=$1 AND user_id=$2",
        )
        .bind(team)
        .bind(f.user)
        .execute(&f.owner)
        .await
        .unwrap();
        let v = fill(&f, conv).await;
        assert_eq!(msg(&v, find)["source_results"].as_array().unwrap().len(), 1);
    }
    sqlx::query("UPDATE atoms SET evidence_retracted=true WHERE atom_id=$1")
        .bind(legacy_atom)
        .execute(&f.owner)
        .await
        .unwrap();
    served_stale(&f, conv).await;
    let v = fill(&f, conv).await;
    assert!(msg(&v, legacy).get("source_results").is_none());
    assert_eq!(msg(&v, find)["source_results"].as_array().unwrap().len(), 1);
    // Already selected page only: its first assistant's question is just outside
    // the 200-message window. A much older turn and another conversation stay out.
    let page = coralswarm_backend::store::CONVERSATION_MESSAGE_PAGE as i32;
    sqlx::query("INSERT INTO conversation_messages(conversation_id,seq,role,text) SELECT $1,n,'user','Explain ordinary history' FROM generate_series(18,$2) n").bind(conv).bind(page+1).execute(&f.owner).await.unwrap();
    let boundary = answer(
        &f,
        conv,
        page + 2,
        "Find the launch note",
        "retrieval_only",
        &evidence,
        Some(&support),
        &library,
    )
    .await;
    sqlx::query("INSERT INTO conversation_messages(conversation_id,seq,role,text) SELECT $1,n,'user','Explain ordinary history' FROM generate_series($2,$3) n").bind(conv).bind(page+4).bind(page*2+2).execute(&f.owner).await.unwrap();
    let v = fill(&f, conv).await;
    assert_eq!(v["messages"].as_array().unwrap().len(), page as usize);
    assert_eq!(v["messages"][0]["id"], json!(boundary));
    assert_eq!(
        v["messages"][0]["source_results"].as_array().unwrap().len(),
        1
    );
    assert_eq!(v["messages_truncated"], true);
    assert_eq!(v["older_before_seq"], page + 3);
    assert!(!v["messages"]
        .as_array()
        .unwrap()
        .iter()
        .any(|m| m["id"] == json!(find)));
    // A full selected page of 100 Find replies shares source hydration. This
    // exercises the registered compute deadline without claiming a benchmark.
    let start = page * 2 + 3;
    sqlx::query("INSERT INTO conversation_messages(conversation_id,seq,role,text,library_scope,scope_graph_ids) SELECT $1,$2+n*2,'user','Find the launch note',$3,$4 FROM generate_series(0,99) n")
        .bind(conv).bind(start).bind(json!(library)).bind(json!([f.graph])).execute(&f.owner).await.unwrap();
    sqlx::query("WITH added AS (INSERT INTO conversation_messages(conversation_id,seq,role,text,mode,timing,scope_graph_ids) SELECT $1,$2+n*2+1,'assistant','Certified page answer','synthesized',$3,$4 FROM generate_series(0,99) n RETURNING id) INSERT INTO answer_support_receipts(receipt_id,graph_id,owner_user_id,support,evidence_complete) SELECT id,$5,$6,$7,true FROM added")
        .bind(conv).bind(start).bind(&evidence).bind(json!([f.graph])).bind(f.graph).bind(f.user).bind(&support).execute(&f.owner).await.unwrap();
    let v = fill(&f, conv).await;
    assert_eq!(v["messages"].as_array().unwrap().len(), page as usize);
    let answers: Vec<_> = v["messages"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|m| m["role"] == "assistant")
        .collect();
    assert_eq!(answers.len(), 100);
    for m in answers {
        assert_eq!(m["source_results"].as_array().unwrap().len(), 1);
        assert_eq!(
            m["source_results"][0]["quotes"][0]["quote"],
            "Exact quoted evidence"
        );
    }
    // Unavailable selected conversation returns the model's safe empty payload.
    sqlx::query("UPDATE conversations SET user_id=$2,shared=false WHERE id=$1")
        .bind(conv)
        .bind(f.other)
        .execute(&f.owner)
        .await
        .unwrap();
    served_stale(&f, conv).await;
    let v = fill(&f, conv).await;
    assert!(v["conversation"].is_null());
    assert_eq!(v["messages"], json!([]));
    drop(api);
    f.close().await;
}
#[tokio::test]
async fn personal_saved_find_results() {
    contract(false).await;
}
#[tokio::test]
async fn tenant_saved_find_results() {
    contract(true).await;
}
