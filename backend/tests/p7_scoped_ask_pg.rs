//! Deterministic cloud Ask on actual consumer/tenant app roles. No network model.
mod support;

use coralswarm_backend::{
    library_read::{self, LibraryScope},
    llm::{testing::GatedScript, *},
    scoped_ask::{self, AskRequest},
    store::PostgresStore,
    tenancy::{
        extractor::Tenant,
        runtime::{TenancyDecision, TenantContext},
    },
    UserContext,
};
use coralswarm_core::embed::{EmbedBatchFuture, EmbedProvider};
use futures::StreamExt;
use serde_json::{json, Value};
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    Executor, PgPool,
};
use std::{
    pin::Pin,
    str::FromStr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
};
use uuid::Uuid;

async fn financial_fixture(pool:&PgPool,account:Uuid) -> sqlx::Transaction<'static,sqlx::Postgres> {
    let mut tx=coralswarm_backend::tenancy::scope::begin_account_scoped(pool,account).await.unwrap();
    coralswarm_backend::billing::renewal::lock_period_rotation(&mut tx,account).await.unwrap();
    tx
}

use support::scoped_ask_fixture::*;

async fn run(org: bool) {
    // The MCP and stream calls below are boxed: this test's own future is
    // larger than the 2 MiB test-thread stack otherwise.
    let Some(f) = Fixture::new(org).await else {
        return;
    };
    let state = f.state(None);
    coralswarm_backend::test_modern_cached_task_admission(&state, &f.tenant, &f.user).await;
    // Full eligible ranking recovers decade-old evidence beyond the latest 50.
    let answer = scoped_ask::execute(&state, &f.tenant, &f.user, f.request(None))
        .await
        .unwrap();
    assert_eq!(answer.payload["outcomes"].as_array().unwrap().len(), 2);
    assert!(answer.payload["outcomes"]
        .to_string()
        .contains("green release"));
    assert_eq!(
        answer.payload["legacy_evidence"][0]["atom_id"],
        json!(f.legacy)
    );
    assert_eq!(f.calls.load(Ordering::SeqCst), 0);
    assert_eq!(f.embeddings.load(Ordering::SeqCst), 0);
    assert_eq!(answer.payload["contains_synthetic_demo_evidence"], true);
    let citation = &answer.payload["retained_citations"][0];
    assert_eq!(
        citation["end_char"].as_i64().unwrap(),
        citation["quote"].as_str().unwrap().chars().count() as i64
    );
    // Exact item/version mismatches and private graph IDs share denial semantics.
    let mut bad = f.request(Some(f.old.0));
    bad.library_scope.as_mut().unwrap().source_version_id = Some(f.extra.1);
    assert_eq!(
        scoped_ask::execute(&f.state(None), &f.tenant, &f.user, bad)
            .await
            .err()
            .unwrap()
            .status(),
        axum::http::StatusCode::NOT_FOUND
    );
    let mut bad = f.request(None);
    bad.library_scope.as_mut().unwrap().graph_ids = vec![f.denied_graph];
    assert_eq!(
        scoped_ask::execute(&f.state(None), &f.tenant, &f.user, bad)
            .await
            .err()
            .unwrap()
            .status(),
        axum::http::StatusCode::NOT_FOUND
    );
    let private_item = source(
        &f.owner,
        f.denied_graph,
        "private-source",
        "PRIVATE other member source",
        false,
    )
    .await
    .0;
    let private_scope: LibraryScope =
        serde_json::from_value(json!({"graph_ids":[f.denied_graph]})).unwrap();
    assert_eq!(
        library_read::retrieve_ask_candidates(
            &f.tenant,
            &f.user,
            private_scope,
            &["private".into()],
            None,
            None,
            10
        )
        .await
        .err()
        .unwrap()
        .status(),
        axum::http::StatusCode::NOT_FOUND
    );
    let denied = coralswarm_backend::test_retained_source_read(
        f.state(None),
        f.tenant.clone(),
        f.user.clone(),
        f.denied_graph,
        private_item,
    )
    .await;
    assert_eq!(denied.status(), axum::http::StatusCode::NOT_FOUND);
    let allowed = coralswarm_backend::test_retained_source_read(
        f.state(None),
        f.tenant.clone(),
        f.user.clone(),
        f.graph,
        f.old.0,
    )
    .await;
    assert_eq!(allowed.status(), axum::http::StatusCode::OK);
    let commons:Uuid=sqlx::query_scalar("INSERT INTO graphs(account_id,name,kind) VALUES($1,'Synthetic shared commons','personal') RETURNING id").bind(f.user.account_id).fetch_one(&f.owner).await.unwrap();
    let commons_item = source(
        &f.owner,
        commons,
        "shared-source",
        "Acme common shared context",
        false,
    )
    .await
    .0;
    let allowed = coralswarm_backend::test_retained_source_read(
        f.state(None),
        f.tenant.clone(),
        f.user.clone(),
        commons,
        commons_item,
    )
    .await;
    assert_eq!(allowed.status(), axum::http::StatusCode::OK);
    assert_eq!(f.calls.load(Ordering::SeqCst), 0);
    assert_eq!(f.embeddings.load(Ordering::SeqCst), 0);
    // Scoped follow-up inherits every selector; changing it requires explicit replace.
    let narrow = scoped_ask::execute(&f.state(None), &f.tenant, &f.user, f.request(Some(f.old.0)))
        .await
        .unwrap();
    let conv: Uuid = serde_json::from_value(narrow.payload["conversation_id"].clone()).unwrap();
    let mut follow = f.request(None);
    follow.library_scope = None;
    follow.conversation_id = Some(conv);
    follow.text = "And what happened next?".into();
    let inherited = scoped_ask::execute(&f.state(None), &f.tenant, &f.user, follow)
        .await
        .unwrap();
    assert_eq!(inherited.scope.source_item_id, Some(f.old.0));
    assert_eq!(inherited.payload["outcomes"].as_array().unwrap().len(), 1);
    let mut broaden = f.request(None);
    broaden.conversation_id = Some(conv);
    assert_eq!(
        scoped_ask::execute(&f.state(None), &f.tenant, &f.user, broaden.clone())
            .await
            .err()
            .unwrap()
            .status(),
        axum::http::StatusCode::CONFLICT
    );
    broaden.scope_action = Some("replace".into());
    let broad = scoped_ask::execute(&f.state(None), &f.tenant, &f.user, broaden)
        .await
        .unwrap();
    assert!(broad.payload["scope_changed"].as_bool().unwrap());
    assert_eq!(
        broad.payload["previous_library_scope"]["source_item_id"],
        json!(f.old.0)
    );
    // Complete input receipts cover both sources even if the fake model cites neither.
    sqlx::query("UPDATE source_versions SET source_metadata=COALESCE(source_metadata,'{}'::jsonb)||'{\"title\":\"Acme green release notice\"}'::jsonb WHERE version_id=$1")
        .bind(f.old.1)
        .execute(&f.owner)
        .await
        .unwrap();
    let mut generated = f.request(None);
    generated.style = Some("detailed".into());
    // The answer model is told today's date in the ASKER's zone: 03:30Z Friday
    // is still Thursday evening in Chicago, and the week anchors on Monday.
    use coralswarm_backend::store::Store as _;
    PostgresStore::new(f.control_owner.clone()).set_user_timezone(f.user.user_id, "America/Chicago").await.unwrap();
    let generated = coralswarm_backend::scoped_ask::test_request_recap_clock(generated, "2026-10-23T03:30:00Z".parse().unwrap());
    let model_answer = scoped_ask::execute(&f.state(None), &f.tenant, &f.user, generated)
        .await
        .unwrap();
    PostgresStore::new(f.control_owner.clone()).set_user_timezone(f.user.user_id, "UTC").await.unwrap();
    assert!(model_answer.generated);
    assert_eq!(f.calls.load(Ordering::SeqCst), 1);
    let prompt = f.prompts.lock().unwrap()[0].clone();
    assert!(prompt.contains("green release") && prompt.contains("red release"));
    assert!(!prompt.contains("wider_context"));
    // The model cites [n]; n indexes retained_citations the way the web resolves it.
    let text = model_answer.payload["text"].as_str().unwrap();
    let marker: Option<usize> = text
        .split('[')
        .nth(1)
        .and_then(|rest| rest.split(']').next())
        .and_then(|digits| digits.parse().ok());
    let k = marker.unwrap_or_else(|| panic!("answer must carry a [n] marker, got: {text}"));
    assert!(k >= 1, "markers are 1-based, got [{k}] in: {text}");
    let cited = &model_answer.payload["retained_citations"][k - 1];
    assert!(
        cited["quote"].as_str().unwrap().contains("green release"),
        "[{k}] must resolve to the green-release passage, got: {cited}"
    );
    let system = f.systems.lock().unwrap().last().cloned().unwrap();
    assert!(system.contains("[n]"), "system prompt must ask for [n] markers: {system}");
    assert!(!system.contains("exact IDs"), "system prompt must not ask for IDs: {system}");
    assert!(system.contains("Today is Thursday 2026-10-22 in America/Chicago."), "system prompt must carry the asker-local date: {system}");
    assert!(system.contains("\"this week\" is Monday 2026-10-19 through today"), "system prompt must anchor the week on Monday: {system}");
    let retained = model_answer.payload["retained_citations"].as_array().unwrap();
    let by_quote = |needle: &str| {
        retained
            .iter()
            .find(|c| c["quote"].as_str().unwrap().contains(needle))
            .unwrap_or_else(|| panic!("no retained citation quoting {needle:?}"))
    };
    let green = by_quote("green release");
    assert_eq!(green["source_type"], json!("linear_issue"), "green: {green}");
    assert_eq!(green["title"], json!("Acme green release notice"), "green: {green}");
    let red = by_quote("red release");
    assert_eq!(red["source_type"], json!("linear_issue"), "red: {red}");
    assert_eq!(red["title"], Value::Null, "red: {red}");
    let mut tx = coralswarm_backend::tenancy::scope::begin_tenant_scoped(
        &f.app,
        f.user.account_id,
        f.user.user_id,
    )
    .await
    .unwrap();
    let support: Value =
        sqlx::query_scalar("SELECT support FROM answer_support_receipts WHERE receipt_id=$1")
            .bind(model_answer.receipt_id)
            .fetch_one(&mut *tx)
            .await
            .unwrap();
    assert_eq!(support.as_array().unwrap().len(), 4);
    tx.commit().await.unwrap();
    let read = coralswarm_backend::test_conversation_read(
        f.state(None),
        f.tenant.clone(),
        f.user.clone(),
        serde_json::from_value(model_answer.payload["conversation_id"].clone()).unwrap(),
    )
    .await;
    assert_eq!(read.status(), axum::http::StatusCode::OK);
    let body = axum::body::to_bytes(read.into_body(), 1_000_000)
        .await
        .unwrap();
    let body: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["conversation"]["scope_status"], "resolved");
    assert!(body["messages"]
        .as_array()
        .unwrap()
        .iter()
        .any(|m| m["role"] == "assistant"
            && m["text"]
                .as_str()
                .unwrap()
                .contains("Synthetic model answer")));
    f.access("restricted").await;
    assert!(!scoped_ask::emission_allowed(&f.state(None), &f.tenant, &f.user, &model_answer).await);
    let denied = scoped_ask::execute(&f.state(None), &f.tenant, &f.user, f.request(None))
        .await
        .unwrap();
    assert!(!denied.payload.to_string().contains("green release"));
    assert!(denied.payload.to_string().contains("red release"));
    let read = coralswarm_backend::test_conversation_read(
        f.state(None),
        f.tenant.clone(),
        f.user.clone(),
        serde_json::from_value(model_answer.payload["conversation_id"].clone()).unwrap(),
    )
    .await;
    let body = axum::body::to_bytes(read.into_body(), 1_000_000)
        .await
        .unwrap();
    assert!(!String::from_utf8_lossy(&body).contains("Synthetic model answer"));
    f.access("permitted").await;
    // A legacy correction rewrites content at the same UUID. Neither a live
    // answer nor saved replay may certify bytes selected before the rewrite.
    let start = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::new(tokio::sync::Notify::new());
    let delayed = f.state(Some((start.clone(), resume.clone())));
    let tenant = f.tenant.clone();
    let user = f.user.clone();
    let mut req = f.request(None);
    req.style = Some("concise".into());
    let task =
        tokio::spawn(async move { scoped_ask::execute(&delayed, &tenant, &user, req).await });
    start.notified().await;
    sqlx::query("UPDATE atoms SET content_inline='Acme corrected legacy chunk' WHERE atom_id=$1")
        .bind(f.legacy)
        .execute(&f.owner)
        .await
        .unwrap();
    resume.notify_one();
    // The first synthesis is withheld at the write (its bytes were rewritten), and the ask
    // retries on fresh evidence: a second model call, over the corrected chunk only.
    start.notified().await;
    resume.notify_one();
    let retried = task.await.unwrap().ok().expect("the retry answers from the corrected chunk");
    assert_eq!(retried.payload["support_status"], "complete");
    {
        let prompts = f.prompts.lock().unwrap();
        let last = prompts.last().unwrap();
        assert!(last.contains("Acme corrected legacy chunk") && !last.contains("Acme legacy chunk"), "the retry must not reuse the stale bytes");
    }
    assert!(!scoped_ask::emission_allowed(&f.state(None), &f.tenant, &f.user, &model_answer).await);
    let read = coralswarm_backend::test_conversation_read(
        f.state(None),
        f.tenant.clone(),
        f.user.clone(),
        serde_json::from_value(model_answer.payload["conversation_id"].clone()).unwrap(),
    )
    .await;
    let body = axum::body::to_bytes(read.into_body(), 1_000_000)
        .await
        .unwrap();
    assert!(!String::from_utf8_lossy(&body).contains("Synthetic model answer"));
    // A benign embedding update preserves exact content identity and remains readable.
    sqlx::query("UPDATE atoms SET content_inline='Acme legacy chunk',embed_model='benign-refresh' WHERE atom_id=$1").bind(f.legacy).execute(&f.owner).await.unwrap();
    // P8: the earlier restriction purged this answer's receipt and cached
    // prose. Re-permitting the source never resurrects a purged answer.
    assert!(!scoped_ask::emission_allowed(&f.state(None), &f.tenant, &f.user, &model_answer).await);
    let mut tx = coralswarm_backend::tenancy::scope::begin_tenant_scoped(
        &f.app,
        f.user.account_id,
        f.user.user_id,
    )
    .await
    .unwrap();
    let old_receipt = Uuid::new_v4();
    sqlx::query("INSERT INTO answer_support_receipts(receipt_id,graph_id,owner_user_id,support,evidence_complete) VALUES($1,$2,$3,$4,true)").bind(old_receipt).bind(f.graph).bind(f.user.user_id).bind(json!([{"kind":"legacy_chunk","graph_id":f.graph,"atom_id":f.legacy}])).execute(&mut *tx).await.unwrap();
    assert!(
        !sqlx::query_scalar::<_, bool>("SELECT library_answer_permitted($1)")
            .bind(old_receipt)
            .fetch_one(&mut *tx)
            .await
            .unwrap(),
        "old UUID-only receipts cannot attest to unchanged mutable content"
    );
    tx.commit().await.unwrap();
    // Provider I/O is deferred locally; revocation must withhold output and persistence.
    let start = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::new(tokio::sync::Notify::new());
    let delayed = f.state(Some((start.clone(), resume.clone())));
    let tenant = f.tenant.clone();
    let user = f.user.clone();
    let mut req = f.request(Some(f.old.0));
    req.style = Some("concise".into());
    let task =
        tokio::spawn(async move { scoped_ask::execute(&delayed, &tenant, &user, req).await });
    start.notified().await;
    f.access("unknown").await;
    resume.notify_one();
    let error = task.await.unwrap().err().unwrap();
    // The write withholds the answer, the ask retries on fresh evidence, and the retry
    // finds the source no longer readable: nothing is sent to the model a second time.
    assert_eq!(error.status(), axum::http::StatusCode::NOT_FOUND);
    f.access("permitted").await;
    // Fresh requester membership during model I/O.
    {
        let start = Arc::new(tokio::sync::Notify::new());
        let resume = Arc::new(tokio::sync::Notify::new());
        let state = f.state(Some((start.clone(), resume.clone())));
        let tenant = f.tenant.clone();
        let user = f.user.clone();
        let mut req = f.request(Some(f.old.0));
        req.style = Some("concise".into());
        let task =
            tokio::spawn(async move { scoped_ask::execute(&state, &tenant, &user, req).await });
        start.notified().await;
        sqlx::query("DELETE FROM account_members WHERE account_id=$1 AND user_id=$2")
            .bind(f.user.account_id)
            .bind(f.user.user_id)
            .execute(&f.control_owner)
            .await
            .unwrap();
        resume.notify_one();
        // Withheld either way: the write fences it (409), or the ask's retry is refused
        // admission by the requester's own membership (404). Never a second model call.
        let status = task.await.unwrap().err().unwrap().status();
        assert!(
            matches!(status, axum::http::StatusCode::CONFLICT | axum::http::StatusCode::NOT_FOUND),
            "unexpected status {status}"
        );
        sqlx::query(
            "INSERT INTO account_members(account_id,user_id,role,seat_tier_id) VALUES($1,$2,'owner',(SELECT CASE WHEN a.is_org_account THEN (SELECT id FROM seat_tiers WHERE code='standard' AND is_active) END FROM accounts a WHERE a.id=$1))",
        )
        .bind(f.user.account_id)
        .bind(f.user.user_id)
        .execute(&f.control_owner)
        .await
        .unwrap();
    }
    let mut current = f.request(Some(f.old.0));
    current.library_scope.as_mut().unwrap().outcome_versions =
        Some(library_read::OutcomeVersions::Current);
    let current = scoped_ask::execute(&f.state(None), &f.tenant, &f.user, current)
        .await
        .unwrap();
    sqlx::query("UPDATE source_items SET current_version_id=NULL WHERE source_item_id=$1")
        .bind(f.old.0)
        .execute(&f.owner)
        .await
        .unwrap();
    assert!(
        !scoped_ask::emission_allowed(&f.state(None), &f.tenant, &f.user, &current).await,
        "current-mode evidence must remain selected at emission"
    );
    sqlx::query("UPDATE source_items SET current_version_id=$2 WHERE source_item_id=$1")
        .bind(f.old.0)
        .bind(f.old.1)
        .execute(&f.owner)
        .await
        .unwrap();
    // The one SSE private frame is fenced again after meta and before publication.
    let response = Box::pin(coralswarm_backend::test_scoped_query_stream(
        f.state(None),
        f.tenant.clone(),
        f.user.clone(),
        json!({"text":"Acme","library_scope":f.scope(Some(f.old.0)),"style":"sources-only"}),
    ))
    .await;
    assert_eq!(response.status(), axum::http::StatusCode::OK);
    let mut stream = response.into_body().into_data_stream();
    skip_to_meta(&mut stream).await;
    f.access("restricted").await;
    let withheld = stream.next().await.unwrap().unwrap();
    assert!(String::from_utf8_lossy(&withheld).contains("answer withheld"));
    assert!(!String::from_utf8_lossy(&withheld).contains("green release"));
    f.access("permitted").await;
    let response = Box::pin(coralswarm_backend::test_scoped_query_stream(
        f.state(None),
        f.tenant.clone(),
        f.user.clone(),
        json!({"text":"Acme","library_scope":f.scope(None),"style":"sources-only"}),
    ))
    .await;
    let mut stream = response.into_body().into_data_stream();
    skip_to_meta(&mut stream).await;
    sqlx::query("UPDATE atoms SET content_inline='Acme corrected after meta' WHERE atom_id=$1")
        .bind(f.legacy)
        .execute(&f.owner)
        .await
        .unwrap();
    let withheld = stream.next().await.unwrap().unwrap();
    assert!(String::from_utf8_lossy(&withheld).contains("answer withheld"));
    assert!(!String::from_utf8_lossy(&withheld).contains("Acme legacy chunk"));
    sqlx::query("UPDATE atoms SET content_inline='Acme legacy chunk' WHERE atom_id=$1")
        .bind(f.legacy)
        .execute(&f.owner)
        .await
        .unwrap();
    // Statement vectors bridge only the in-scope, permitted statement.
    let mut vector = vec![0.0f32; 384];
    vector[0] = 1.0;
    statement_vector(&f.owner, f.graph, f.old.2, &vector).await;
    let mapped = library_read::retrieve_ask_candidates(
        &f.tenant,
        &f.user,
        f.scope(Some(f.old.0)),
        &[],
        Some(&vector),
        None,
        10,
    )
    .await
    .unwrap();
    assert_eq!(mapped["outcomes"].as_array().unwrap().len(), 1);
    assert_eq!(mapped["outcomes"][0]["source_version_id"], json!(f.old.1));
    let mismatch = library_read::retrieve_ask_candidates(
        &f.tenant,
        &f.user,
        f.scope(Some(f.extra.0)),
        &[],
        Some(&vector),
        None,
        10,
    )
    .await
    .unwrap();
    assert!(mismatch["outcomes"].as_array().unwrap().is_empty());
    f.access("restricted").await;
    let hidden = library_read::retrieve_ask_candidates(
        &f.tenant,
        &f.user,
        f.scope(None),
        &[],
        Some(&vector),
        None,
        10,
    )
    .await
    .unwrap();
    assert!(hidden["outcomes"].as_array().unwrap().is_empty());
    f.access("permitted").await;
    // Async reserve must finish before the admission future and provider dispatch.
    for revoke in ["source", "member", "legacy"] {
        let state = f.state(None);
        let provider = state.llm.as_ref().unwrap();
        let calls = f.calls.load(Ordering::SeqCst);
        let query = Uuid::new_v4();
        let probe = scoped_ask::Answer {
            payload: Value::Null,
            scope: answer.scope.clone(),
            account: answer.account,
            primary_graph: answer.primary_graph,
            receipt_id: answer.receipt_id,
            generated: true,
        };
        let usage = usage::UsageContext::new(f.app.clone(), Some(f.user.account_id), Some(f.graph))
            .with_user(f.user.user_id)
            .with_attributed_user(f.user.user_id)
            .with_source_query(query);
        let result=usage::metered_complete_admitted(provider.as_ref(),CompletionRequest{messages:vec![Message{role:Role::User,content:"synthetic guard fixture".into()}],model:"fake-model".into(),max_tokens:10,..Default::default()},Some(&usage),"p7_guard",async {
            let mut tx=coralswarm_backend::tenancy::scope::begin_tenant_scoped(&f.app,f.user.account_id,f.user.user_id).await.unwrap();
            let pending:i64=sqlx::query_scalar("SELECT count(*) FROM llm_usage_pending WHERE task='p7_guard'").fetch_one(&mut *tx).await.unwrap();assert_eq!(pending,1);tx.commit().await.unwrap();
            match revoke {
                "source"=>f.access("unknown").await,
                "legacy"=>{sqlx::query("UPDATE atoms SET content_inline='Acme rewritten after reserve' WHERE atom_id=$1").bind(f.legacy).execute(&f.owner).await.unwrap();},
                _=>{sqlx::query("DELETE FROM account_members WHERE account_id=$1 AND user_id=$2").bind(f.user.account_id).bind(f.user.user_id).execute(&f.control_owner).await.unwrap();},
            }
            scoped_ask::emission_allowed(&state,&f.tenant,&f.user,&probe).await
        }).await;
        assert!(
            matches!(result,Err(LlmError::Auth(ref reason)) if reason==usage::ADMISSION_WITHHELD)
        );
        assert_eq!(f.calls.load(Ordering::SeqCst), calls);
        let mut tx = coralswarm_backend::tenancy::scope::begin_tenant_scoped(
            &f.app,
            f.user.account_id,
            f.user.user_id,
        )
        .await
        .unwrap();
        let billed:(i32,i32,f64,String)=sqlx::query_as("SELECT input_tokens,output_tokens,cost_usd::float8,task FROM llm_usage WHERE source_query_id=$1").bind(query).fetch_one(&mut *tx).await.unwrap();
        assert_eq!(billed, (0, 0, 0.0, "p7_guard_withheld".into()));
        let pending: i64 =
            sqlx::query_scalar("SELECT count(*) FROM llm_usage_pending WHERE task='p7_guard'")
                .fetch_one(&mut *tx)
                .await
                .unwrap();
        assert_eq!(pending, 0);
        tx.commit().await.unwrap();
        match revoke {
            "source" => f.access("permitted").await,
            "legacy" => {
                sqlx::query("UPDATE atoms SET content_inline='Acme legacy chunk' WHERE atom_id=$1")
                    .bind(f.legacy)
                    .execute(&f.owner)
                    .await
                    .unwrap();
            }
            _ => {
                sqlx::query("INSERT INTO account_members(account_id,user_id,role,seat_tier_id) VALUES($1,$2,'owner',(SELECT CASE WHEN a.is_org_account THEN (SELECT id FROM seat_tiers WHERE code='standard' AND is_active) END FROM accounts a WHERE a.id=$1))").bind(f.user.account_id).bind(f.user.user_id).execute(&f.control_owner).await.unwrap();
            }
        }
    }
    // Same-graph broader questions do not enter a replaced narrower selector.
    let mut broad = f.request(None);
    broad.text = "Acme BroadQuestionMarker".into();
    let broad = scoped_ask::execute(&f.state(None), &f.tenant, &f.user, broad)
        .await
        .unwrap();
    let conv: Uuid = serde_json::from_value(broad.payload["conversation_id"].clone()).unwrap();
    let mut narrow = f.request(Some(f.old.0));
    narrow.text = "Acme NarrowQuestionMarker".into();
    narrow.style = Some("concise".into());
    narrow.scope_action = Some("replace".into());
    narrow.conversation_id = Some(conv);
    scoped_ask::execute(&f.state(None), &f.tenant, &f.user, narrow)
        .await
        .unwrap();
    assert!(!f
        .prompts
        .lock()
        .unwrap()
        .last()
        .unwrap()
        .contains("BroadQuestionMarker"));
    let mut follow = f.request(Some(f.old.0));
    follow.library_scope = None;
    follow.conversation_id = Some(conv);
    follow.style = Some("concise".into());
    follow.text = "Acme FollowQuestionMarker".into();
    scoped_ask::execute(&f.state(None), &f.tenant, &f.user, follow)
        .await
        .unwrap();
    let prompts = f.prompts.lock().unwrap();
    assert!(prompts.last().unwrap().contains("NarrowQuestionMarker"));
    assert!(!prompts.last().unwrap().contains("BroadQuestionMarker"));
    drop(prompts);
    // Question deletion invalidates complete response lineage.
    let question: Uuid = serde_json::from_value(
        support
            .as_array()
            .unwrap()
            .iter()
            .find(|s| s["kind"] == "user_question")
            .unwrap()["message_id"]
            .clone(),
    )
    .unwrap();
    let mut scoped = coralswarm_backend::tenancy::scope::begin_tenant_scoped(
        &f.app,
        f.user.account_id,
        f.user.user_id,
    )
    .await
    .unwrap();
    let mutation = sqlx::query("UPDATE conversation_messages SET library_scope='{}' WHERE id=$1")
        .bind(question)
        .execute(&mut *scoped)
        .await
        .err()
        .unwrap();
    assert!(
        mutation
            .as_database_error()
            .is_some_and(|e| e.code().as_deref() == Some("23514")),
        "server question selector snapshot is immutable under the app role"
    );
    scoped.rollback().await.unwrap();
    sqlx::query("DELETE FROM conversation_messages WHERE id=$1")
        .bind(question)
        .execute(&f.owner)
        .await
        .unwrap();
    assert!(!scoped_ask::emission_allowed(&f.state(None), &f.tenant, &f.user, &model_answer).await);
    // Real dispatcher/tool schemas and both era branches, with synthetic
    // verified identity and actual app-role data (no network auth/model).
    let state=f.state(None);
    let calls=f.calls.load(Ordering::SeqCst);let embeddings=f.embeddings.load(Ordering::SeqCst);
    let search=Box::pin(coralswarm_backend::test_retained_mcp_dispatch(state.clone(),&f.tenant,&f.user,json!({"method":"tools/call","params":{"name":"search_atoms","arguments":{"query":"Acme","library_scope":f.scope(Some(f.old.0))}}}))).await.unwrap();
    assert_eq!(search["structuredContent"]["mode"],"retrieval_only");
    assert_eq!(search["structuredContent"]["results"],json!([]));
    let sync=Box::pin(coralswarm_backend::test_retained_mcp_dispatch(state.clone(),&f.tenant,&f.user,json!({"method":"tools/call","params":{"name":"ask_ocean","arguments":{"question":"Acme","library_scope":f.scope(Some(f.old.0)),"style":"sources-only"}}}))).await.unwrap();
    assert_eq!(sync["structuredContent"]["support_status"],"complete");
    let meta=json!({"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientCapabilities":{"extensions":{"io.modelcontextprotocol/tasks":{}}}});
    let modern=Box::pin(coralswarm_backend::test_retained_mcp_dispatch(state.clone(),&f.tenant,&f.user,json!({"method":"tools/call","params":{"name":"ask_ocean","arguments":{"question":"Acme","library_scope":f.scope(Some(f.old.0)),"style":"sources-only"},"_meta":meta}}))).await.unwrap();
    let protocol_id=modern["taskId"].as_str().expect("modern Ask must return real protocol task handle").to_owned();
    let mut finished=false;
    for _ in 0..100 {
        let poll=Box::pin(coralswarm_backend::test_retained_mcp_dispatch(state.clone(),&f.tenant,&f.user,json!({"method":"tasks/get","params":{"taskId":protocol_id,"_meta":meta}}))).await.unwrap();
        if poll["status"]=="completed" {assert_eq!(poll["result"]["structuredContent"]["support_status"],"complete");finished=true;break;}
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(finished);
    assert_eq!(f.calls.load(Ordering::SeqCst),calls);assert_eq!(f.embeddings.load(Ordering::SeqCst),embeddings);
    let started=Arc::new(tokio::sync::Notify::new());let resume=Arc::new(tokio::sync::Notify::new());
    let deferred=f.state(Some((started.clone(),resume.clone())));
    let old_budget=std::env::var("MCP_ASK_SYNC_WAIT_MS").ok();std::env::set_var("MCP_ASK_SYNC_WAIT_MS","0");
    let async_result=Box::pin(coralswarm_backend::test_retained_mcp_dispatch(deferred.clone(),&f.tenant,&f.user,json!({"method":"tools/call","params":{"name":"ask_ocean","arguments":{"question":"Acme","library_scope":f.scope(Some(f.old.0)),"style":"concise"}}}))).await.unwrap();
    if let Some(value)=old_budget {std::env::set_var("MCP_ASK_SYNC_WAIT_MS",value);}else{std::env::remove_var("MCP_ASK_SYNC_WAIT_MS");}
    let async_id=async_result["structuredContent"]["task_id"].as_str().expect("legacy bounded wait must return durable UUID").to_owned();
    started.notified().await;resume.notify_one();
    let mut legacy_finished=false;
    for _ in 0..100 {
        let poll=Box::pin(coralswarm_backend::test_retained_mcp_dispatch(deferred.clone(),&f.tenant,&f.user,json!({"method":"tools/call","params":{"name":"get_task_result","arguments":{"task_id":async_id}}}))).await.unwrap();
        if poll["structuredContent"]["status"]=="completed" {assert_eq!(poll["structuredContent"]["result"]["support_status"],"complete");legacy_finished=true;break;}
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(legacy_finished);
    let unsupported=Box::pin(coralswarm_backend::test_retained_mcp_dispatch(state.clone(),&f.tenant,&f.user,json!({"method":"tools/call","params":{"name":"ask_ocean","arguments":{"question":"Acme","library_scope":f.scope(None),"project":"old unsupported"}}}))).await.unwrap();
    assert_eq!(unsupported["isError"],true);
    f.access("restricted").await;
    assert!(Box::pin(coralswarm_backend::test_retained_mcp_dispatch(state.clone(),&f.tenant,&f.user,json!({"method":"tasks/get","params":{"taskId":protocol_id,"_meta":meta}}))).await.is_err());
    f.access("permitted").await;
    // Both MCP eras use the same durable answer and complete receipt. The
    // protocol registry caches a reference only; polling always reads live.
    let state = f.state(None);
    let before_calls = f.calls.load(Ordering::SeqCst);
    let before_embeds = f.embeddings.load(Ordering::SeqCst);
    let key = Some("mcp-sources-only".to_owned());
    let (id, inserted, normalized) = coralswarm_backend::test_retained_mcp_claim(
        &state,
        &f.tenant,
        &f.user,
        f.request(Some(f.old.0)),
        key.clone(),
    )
    .await
    .unwrap();
    assert!(inserted);
    assert!(normalized
        .library_scope
        .as_ref()
        .unwrap()
        .outcome_versions
        .is_some());
    let handle = coralswarm_backend::test_retained_mcp_modern_reference(&state, &f.user, id);
    let pending =
        coralswarm_backend::test_retained_mcp_modern_get(&state, &f.tenant, &f.user, &handle)
            .await
            .unwrap();
    assert_eq!(pending["status"], "working");
    assert!(
        coralswarm_backend::test_retained_mcp_run(
            state.clone(),
            f.tenant.clone(),
            f.user.clone(),
            id,
            normalized
        )
        .await
    );
    let complete = coralswarm_backend::test_retained_mcp_poll(&state, &f.tenant, &f.user, id)
        .await
        .unwrap();
    assert_eq!(complete["result"]["mode"], "retrieval_only");
    assert_eq!(complete["result"]["text"], "");
    assert_eq!(complete["result"]["support_status"], "complete");
    assert_eq!(complete["result"]["contains_synthetic_demo_evidence"], true);
    assert!(!complete["result"]["retained_citations"]
        .as_array()
        .unwrap()
        .is_empty());
    assert_eq!(f.calls.load(Ordering::SeqCst), before_calls);
    assert_eq!(f.embeddings.load(Ordering::SeqCst), before_embeds);
    // A bounded tenant pool is sufficient for completion: the final live
    // admission fence must not acquire a second connection while holding one.
    let one = PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(std::time::Duration::from_secs(2))
        .connect_with((*f.app.connect_options()).clone())
        .await
        .unwrap();
    let mut one_tenant = f.tenant.clone();
    one_tenant.pool = Some(one.clone());
    one_tenant.store = Some(Arc::new(PostgresStore::new(one.clone())));
    if let Some(TenancyDecision::Org(ctx)) = one_tenant.decision.as_mut() {
        ctx.pool = one.clone();
    }
    let (one_id, _, one_request) = coralswarm_backend::test_retained_mcp_claim(
        &state, &one_tenant, &f.user, f.request(Some(f.old.0)), None,
    ).await.unwrap();
    assert!(tokio::time::timeout(std::time::Duration::from_secs(2),
        coralswarm_backend::test_retained_mcp_run(
            state.clone(), one_tenant.clone(), f.user.clone(), one_id, one_request,
        )).await.expect("completion must not wait on its own tenant connection"));
    let one_delivered = coralswarm_backend::test_retained_mcp_poll(
        &state, &one_tenant, &f.user, one_id,
    ).await.unwrap();
    assert_eq!(one_delivered["status"], "completed");
    assert_eq!(one_delivered["result"]["support_status"], "complete");
    // Revoke after the pre-acquisition fence, while the one completion
    // connection is held elsewhere. The on-executor check must suppress the
    // write once acquisition resumes, including CONTROL member changes.
    for changed in ["source", "membership"] {
        let mut request = f.request(Some(f.old.0));
        let (wait_id, _, request) = coralswarm_backend::test_retained_mcp_claim(
            &state, &one_tenant, &f.user, request, None,
        ).await.unwrap();
        let ready = Arc::new(tokio::sync::Notify::new());
        let resume = Arc::new(tokio::sync::Notify::new());
        let worker = tokio::spawn({
            let state = state.clone(); let tenant = one_tenant.clone(); let user = f.user.clone();
            let ready = ready.clone(); let resume = resume.clone();
            async move { coralswarm_backend::test_retained_mcp_run_before_completion(
                state, tenant, user, wait_id, request, ready, resume,
            ).await }
        });
        ready.notified().await;
        let held = one.acquire().await.unwrap();
        resume.notify_one();
        match changed {
            "source" => f.access("unknown").await,
            "membership" => { sqlx::query("DELETE FROM account_members WHERE account_id=$1 AND user_id=$2")
                .bind(f.user.account_id).bind(f.user.user_id).execute(&f.control_owner).await.unwrap(); },
            _ => unreachable!(),
        }
        drop(held);
        assert!(!tokio::time::timeout(std::time::Duration::from_secs(2), worker)
            .await.expect("revoked completion must not acquire a second connection").unwrap());
        let saved: Option<Value> = sqlx::query_scalar("SELECT result FROM mcp_tasks WHERE id=$1")
            .bind(wait_id).fetch_one(&f.owner).await.unwrap();
        assert!(saved.is_none(), "{changed} revocation during acquisition must prevent completion storage");
        match changed {
            "source" => f.access("permitted").await,
            "membership" => { sqlx::query("INSERT INTO account_members(account_id,user_id,role,seat_tier_id) VALUES($1,$2,'owner',(SELECT CASE WHEN a.is_org_account THEN (SELECT id FROM seat_tiers WHERE code='standard' AND is_active) END FROM accounts a WHERE a.id=$1))")
                .bind(f.user.account_id).bind(f.user.user_id).execute(&f.control_owner).await.unwrap(); },
            _ => unreachable!(),
        }
    }
    one.close().await;
    let modern =
        coralswarm_backend::test_retained_mcp_modern_get(&state, &f.tenant, &f.user, &handle)
            .await
            .unwrap();
    // P8: the loop's source and membership revocations purged this owner's
    // cached task output. Restoring access never resurrects a purged result.
    assert_eq!(modern["status"], "failed", "{modern}");
    assert!(modern["result"]["structuredContent"]["query_id"].is_null());
    let _ = &complete;
    // A purged task is terminal: its idempotency key cannot replay the
    // retracted result, nor silently start a fresh answer under the same key.
    let retry = coralswarm_backend::test_retained_mcp_claim(
        &state,
        &f.tenant,
        &f.user,
        f.request(Some(f.old.0)),
        key.clone(),
    )
    .await;
    assert!(retry.as_ref().is_err_and(|e| e.contains("expired or failed idempotency key")), "{retry:?}");
    let mut other_request = f.request(Some(f.old.0));
    other_request
        .library_scope
        .as_mut()
        .unwrap()
        .source_version_id = Some(f.old.1);
    let (exact_id, _, exact_request) =
        coralswarm_backend::test_retained_mcp_claim(&state, &f.tenant, &f.user, other_request, key)
            .await
            .unwrap();
    assert_ne!(exact_id, id, "exact-version key cannot widen cached scope");
    assert!(
        coralswarm_backend::test_retained_mcp_run(
            state.clone(),
            f.tenant.clone(),
            f.user.clone(),
            exact_id,
            exact_request
        )
        .await
    );
    let mut wrong_version = f.request(Some(f.old.0));
    wrong_version
        .library_scope
        .as_mut()
        .unwrap()
        .source_version_id = Some(Uuid::new_v4());
    assert!(coralswarm_backend::test_retained_mcp_claim(
        &state,
        &f.tenant,
        &f.user,
        wrong_version,
        None
    )
    .await
    .is_err());
    let mut intruder = f.user.clone();
    intruder.user_id = Uuid::new_v4();
    assert!(
        coralswarm_backend::test_retained_mcp_poll(&state, &f.tenant, &intruder, id)
            .await
            .is_err()
    );
    assert!(coralswarm_backend::test_retained_mcp_modern_get(
        &state, &f.tenant, &intruder, &handle
    )
    .await
    .is_err());
    let mut switched = f.user.clone();
    switched.account_id = Uuid::new_v4();
    assert!(
        coralswarm_backend::test_retained_mcp_poll(&state, &f.tenant, &switched, id)
            .await
            .is_err()
    );
    assert!(coralswarm_backend::test_retained_mcp_modern_get(
        &state, &f.tenant, &switched, &handle
    )
    .await
    .is_err());
    let mut private = f.request(None);
    private.library_scope.as_mut().unwrap().graph_ids = vec![f.denied_graph];
    assert!(
        coralswarm_backend::test_retained_mcp_claim(&state, &f.tenant, &f.user, private, None)
            .await
            .is_err()
    );
    let mut scoped=coralswarm_backend::tenancy::scope::begin_tenant_scoped(&f.app,f.user.account_id,f.user.user_id).await.unwrap();
    let mutation=sqlx::query("UPDATE mcp_tasks SET library_scope='{}' WHERE id=$1").bind(id).execute(&mut *scoped).await.err().unwrap();
    assert!(mutation.as_database_error().is_some_and(|e|e.code().as_deref()==Some("23514")),"app role cannot rewrite bound selector identity");scoped.rollback().await.unwrap();
    // P8: `id` was purged by the revocation loop above, so tamper and
    // restriction checks use the still-completed exact-version task.
    let original: Value = sqlx::query_scalar("SELECT result FROM mcp_tasks WHERE id=$1")
        .bind(exact_id)
        .fetch_one(&f.owner)
        .await
        .unwrap();
    sqlx::query("UPDATE mcp_tasks SET result=jsonb_set(result,'{text}','\"forged result secret\"') WHERE id=$1").bind(exact_id).execute(&f.owner).await.unwrap();
    assert!(
        coralswarm_backend::test_retained_mcp_poll(&state, &f.tenant, &f.user, exact_id)
            .await
            .is_err()
    );
    sqlx::query("UPDATE mcp_tasks SET result=$2 WHERE id=$1")
        .bind(exact_id)
        .bind(original)
        .execute(&f.owner)
        .await
        .unwrap();
    assert!(
        coralswarm_backend::test_retained_mcp_poll(&state, &f.tenant, &f.user, exact_id)
            .await
            .is_ok()
    );
    f.access("restricted").await;
    assert!(
        coralswarm_backend::test_retained_mcp_poll(&state, &f.tenant, &f.user, exact_id)
            .await
            .is_err()
    );
    f.access("unknown").await;
    assert!(
        coralswarm_backend::test_retained_mcp_poll(&state, &f.tenant, &f.user, exact_id)
            .await
            .is_err()
    );
    f.access("permitted").await;
    // Re-permitting never resurrects a purged completion.
    let purged: (String, Option<Value>) =
        sqlx::query_as("SELECT status,result FROM mcp_tasks WHERE id=$1")
            .bind(exact_id)
            .fetch_one(&f.owner)
            .await
            .unwrap();
    assert_eq!(purged.0, "failed");
    assert!(purged.1.is_none());
    // No unscoped/old cached completion may become an authoritative task.
    let old = Uuid::new_v4();
    sqlx::query("INSERT INTO mcp_tasks(id,account_id,graph_id,owner_user_id,tool_name,status,result) VALUES($1,$2,$3,$4,'ask_ocean','completed','{\"answer\":\"old cached secret\"}')")
        .bind(old).bind(f.user.account_id).bind(f.graph).bind(f.user.user_id).execute(&f.owner).await.unwrap();
    assert!(
        coralswarm_backend::test_retained_mcp_poll(&state, &f.tenant, &f.user, old)
            .await
            .is_err()
    );
    // Full mutable legacy input hash is mandatory on broad task receipts too.
    let (broad_id, _, broad_request) = coralswarm_backend::test_retained_mcp_claim(
        &state,
        &f.tenant,
        &f.user,
        f.request(None),
        None,
    )
    .await
    .unwrap();
    assert!(
        coralswarm_backend::test_retained_mcp_run(
            state.clone(),
            f.tenant.clone(),
            f.user.clone(),
            broad_id,
            broad_request
        )
        .await
    );
    let broad = coralswarm_backend::test_retained_mcp_poll(&state, &f.tenant, &f.user, broad_id)
        .await
        .unwrap();
    let legacy: Uuid =
        serde_json::from_value(broad["result"]["legacy_evidence"][0]["atom_id"].clone()).unwrap();
    assert!(!broad.to_string().contains("input_sha256"));
    sqlx::query("UPDATE atoms SET content_inline=content_inline||' rewritten' WHERE atom_id=$1")
        .bind(legacy)
        .execute(&f.owner)
        .await
        .unwrap();
    assert!(
        coralswarm_backend::test_retained_mcp_poll(&state, &f.tenant, &f.user, broad_id)
            .await
            .is_err()
    );
    // Model completes after source revocation: no completion/result copy is saved.
    let started = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::new(tokio::sync::Notify::new());
    let deferred = f.state(Some((started.clone(), resume.clone())));
    let mut request = f.request(Some(f.old.0));
    request.style = Some("concise".into());
    let (deferred_id, _, request) =
        coralswarm_backend::test_retained_mcp_claim(&deferred, &f.tenant, &f.user, request, None)
            .await
            .unwrap();
    let worker = tokio::spawn({
        let state = deferred.clone();
        let tenant = f.tenant.clone();
        let user = f.user.clone();
        async move {
            coralswarm_backend::test_retained_mcp_run(state, tenant, user, deferred_id, request)
                .await
        }
    });
    started.notified().await;
    f.access("restricted").await;
    resume.notify_one();
    assert!(!worker.await.unwrap());
    let result: Option<Value> = sqlx::query_scalar("SELECT result FROM mcp_tasks WHERE id=$1")
        .bind(deferred_id)
        .fetch_one(&f.owner)
        .await
        .unwrap();
    assert!(result.is_none());
    f.access("permitted").await;
    let failure =
        coralswarm_backend::test_retained_mcp_poll(&deferred, &f.tenant, &f.user, deferred_id)
            .await
            .unwrap();
    assert_eq!(failure["status"], "failed");
    assert!(failure["result"].is_null());
    sqlx::query("DELETE FROM source_items WHERE source_item_id=$1").bind(f.old.0).execute(&f.owner).await.unwrap();
    assert!(coralswarm_backend::test_retained_mcp_poll(&state,&f.tenant,&f.user,id).await.is_err());
    assert!(coralswarm_backend::test_retained_mcp_modern_get(&state,&f.tenant,&f.user,&handle).await.is_err());
    f.done().await;
}
#[tokio::test]
async fn consumer_scoped_ask_is_complete_and_rechecks_live_inputs() {
    run(false).await;
}
#[tokio::test]
async fn tenant_scoped_ask_is_complete_and_rechecks_live_inputs() {
    run(true).await;
}

async fn conversation_page(f: &Fixture, conv: Uuid, before_seq: Option<i32>) -> (axum::http::StatusCode, Value) {
    let read = coralswarm_backend::test_conversation_page(f.state(None), f.tenant.clone(), f.user.clone(), conv, before_seq).await;
    let status = read.status();
    let body = axum::body::to_bytes(read.into_body(), 10_000_000).await.unwrap();
    (status, serde_json::from_slice(&body).unwrap_or(Value::Null))
}
fn page_seqs(page: &Value) -> Vec<i64> {
    page["messages"].as_array().unwrap().iter().map(|m| m["seq"].as_i64().unwrap()).collect()
}
fn message_at(page: &Value, seq: i64) -> Value {
    page["messages"].as_array().unwrap().iter().find(|m| m["seq"] == json!(seq)).cloned().unwrap_or_else(|| panic!("no message at seq {seq}"))
}

async fn chats_history_pages(org: bool) {
    let Some(f) = Fixture::new(org).await else {
        return;
    };
    let state = f.state(None);
    coralswarm_backend::test_modern_cached_task_admission(&state, &f.tenant, &f.user).await;
    let answer = scoped_ask::execute(&state, &f.tenant, &f.user, f.request(None)).await.unwrap();
    let conv: Uuid = serde_json::from_value(answer.payload["conversation_id"].clone()).unwrap();
    let supported: Uuid = serde_json::from_value(answer.payload["message_id"].clone()).unwrap();
    let supported_seq: i32 = sqlx::query_scalar("SELECT seq FROM conversation_messages WHERE id=$1").bind(supported).fetch_one(&f.owner).await.unwrap();
    let (lo, first_fill): (i32, i32) = sqlx::query_as("SELECT min(seq),max(seq)+1 FROM conversation_messages WHERE conversation_id=$1").bind(conv).fetch_one(&f.owner).await.unwrap();
    let (hi, cursor) = (lo + 249, lo + 50);
    let masked_seq = first_fill + 3;
    sqlx::query("INSERT INTO conversation_messages(conversation_id,seq,role,text) SELECT $1,s,CASE WHEN s=$3 THEN 'assistant' ELSE 'user' END,CASE WHEN s=$3 THEN 'UNSUPPORTED PAGE TWO ANSWER' ELSE 'question '||s END FROM generate_series($2,$4) s")
        .bind(conv).bind(first_fill).bind(masked_seq).bind(hi).execute(&f.owner).await.unwrap();
    assert!(supported_seq < cursor && masked_seq < cursor, "both answers must land on the older page");

    let (status, newest) = conversation_page(&f, conv, None).await;
    assert_eq!(status, axum::http::StatusCode::OK);
    assert_eq!(page_seqs(&newest), (cursor as i64..=hi as i64).collect::<Vec<_>>());
    assert_eq!(newest["messages_truncated"], json!(true));
    assert_eq!(newest["older_before_seq"], json!(cursor));

    let (status, older) = conversation_page(&f, conv, Some(cursor)).await;
    assert_eq!(status, axum::http::StatusCode::OK);
    assert_eq!(page_seqs(&older), (lo as i64..cursor as i64).collect::<Vec<_>>());
    assert_eq!(older["messages_truncated"], json!(false));
    assert_eq!(older["older_before_seq"], Value::Null);
    assert_eq!(older["conversation"]["scope_status"], "resolved");
    let mut ids: Vec<Value> = [&older, &newest].iter().flat_map(|p| p["messages"].as_array().unwrap().iter().map(|m| m["id"].clone())).collect();
    ids.sort_by_key(|v| v.to_string());
    ids.dedup();
    assert_eq!(ids.len(), 250, "pages must neither repeat nor skip a message");
    let total: i64 = sqlx::query_scalar("SELECT count(*) FROM conversation_messages WHERE conversation_id=$1").bind(conv).fetch_one(&f.owner).await.unwrap();
    assert_eq!(total, 250);

    let kept = message_at(&older, supported_seq.into());
    assert_eq!(kept["id"], json!(supported));
    assert_eq!(kept["support_status"], "complete");
    // This answer has no support receipt at all; it must still read as masked.
    let masked = message_at(&older, masked_seq.into());
    assert_eq!(masked["support_status"], "unknown");
    assert_eq!(masked["text"], "[Evidence unavailable: retained support cannot be verified]");
    assert!(!older.to_string().contains("UNSUPPORTED PAGE TWO ANSWER"));

    let (status, _) = conversation_page(&f, conv, Some(0)).await;
    assert_eq!(status, axum::http::StatusCode::BAD_REQUEST);

    // An older page reruns the live receipt check rather than trusting the first read.
    f.access("restricted").await;
    let (status, rechecked) = conversation_page(&f, conv, Some(cursor)).await;
    assert_eq!(status, axum::http::StatusCode::OK);
    assert_eq!(message_at(&rechecked, supported_seq.into())["support_status"], "unavailable");
    f.done().await;
}
#[tokio::test]
async fn consumer_chats_history_pages_by_keyset() {
    chats_history_pages(false).await;
}
#[tokio::test]
async fn tenant_chats_history_pages_by_keyset() {
    chats_history_pages(true).await;
}

async fn titled_source(f: &Fixture, id: &str, text: &str, source_type: &str, metadata: Value, context: Option<Value>) {
    let item:Uuid=sqlx::query_scalar("INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id) VALUES($1,$2,'fixture:titles',$3) RETURNING source_item_id")
        .bind(f.graph).bind(source_type).bind(id).fetch_one(&f.owner).await.unwrap();
    let version:Uuid=sqlx::query_scalar("INSERT INTO source_versions(graph_id,source_item_id,source_id,source_type,provider_namespace,raw_inline,extraction_version,source_metadata,capture_context) VALUES($1,$2,$3,$4,'fixture:titles',$5,'source-atoms-v2',$6,$7) RETURNING version_id")
        .bind(f.graph).bind(item).bind(id).bind(source_type).bind(text).bind(metadata).bind(context).fetch_one(&f.owner).await.unwrap();
    sqlx::query("UPDATE source_items SET current_version_id=$2 WHERE source_item_id=$1").bind(item).bind(version).execute(&f.owner).await.unwrap();
    let atom:Uuid=sqlx::query_scalar("INSERT INTO semantic_atoms(graph_id,source_version_id,ordinal,kind,statement,extraction_version,model) VALUES($1,$2,0,'decision',$3,'source-atoms-v2','fake-model') RETURNING semantic_atom_id")
        .bind(f.graph).bind(version).bind(text).fetch_one(&f.owner).await.unwrap();
    sqlx::query("INSERT INTO semantic_atom_passages(graph_id,source_version_id,semantic_atom_id,start_char,end_char,quote) VALUES($1,$2,$3,0,$4,$5)")
        .bind(f.graph).bind(version).bind(atom).bind(text.chars().count() as i32).bind(text).execute(&f.owner).await.unwrap();
}

fn title_of(citations: &Value, needle: &str) -> Value {
    citations.as_array().unwrap().iter()
        .find(|c| c["quote"].as_str().unwrap().contains(needle))
        .unwrap_or_else(|| panic!("no citation quoting {needle:?} in {citations}"))["title"].clone()
}

async fn citation_titles(org: bool) {
    let Some(f) = Fixture::new(org).await else { return };
    let state = f.state(None);
    coralswarm_backend::test_modern_cached_task_admission(&state, &f.tenant, &f.user).await;
    let context = json!({"revision":1,"session_id":"cs-titles","session_title":"Fix flaky deploy"});
    titled_source(&f, "titles-coding", "Acme coding session decision", "coding_session",
        json!({"session_id":"cs-titles","session_title":"Stale label","capture_context":context}), Some(context.clone())).await;
    titled_source(&f, "titles-thread", "Acme slack thread decision", "slack",
        json!({"channel_name":"eng-release","thread_ts":"1785124832.747249"}), None).await;
    titled_source(&f, "titles-channel", "Acme slack channel decision", "slack",
        json!({"channel_name":"eng-release","thread_ts":null}), None).await;
    titled_source(&f, "titles-bare", "Acme slack bare decision", "slack", json!({"channel_name":""}), None).await;
    let answer = scoped_ask::execute(&state, &f.tenant, &f.user, f.request(None)).await.unwrap();
    let expected = [
        ("coding session", json!("Fix flaky deploy")),
        ("slack thread", json!("Thread in #eng-release")),
        ("slack channel", json!("#eng-release")),
        ("slack bare", Value::Null),
        ("red release", Value::Null),
    ];
    let retained = &answer.payload["retained_citations"];
    for (needle, title) in &expected {
        assert_eq!(&title_of(retained, needle), title, "live {needle}");
    }
    assert_eq!(f.calls.load(Ordering::SeqCst), 0, "titles need no model call");
    // Answers stored before titling carry null titles; reading the chat fills them.
    let conversation: Uuid = serde_json::from_value(answer.payload["conversation_id"].clone()).unwrap();
    let blanked = sqlx::query("UPDATE conversation_messages SET timing=jsonb_set(timing,'{retained_citations}',(SELECT jsonb_agg(c||'{\"title\":null}') FROM jsonb_array_elements(timing->'retained_citations') c)) WHERE conversation_id=$1 AND role='assistant'")
        .bind(conversation).execute(&f.owner).await.unwrap();
    assert_eq!(blanked.rows_affected(), 1);
    let read = coralswarm_backend::test_conversation_read(f.state(None), f.tenant.clone(), f.user.clone(), conversation).await;
    assert_eq!(read.status(), axum::http::StatusCode::OK);
    let body: Value = serde_json::from_slice(&axum::body::to_bytes(read.into_body(), 1_000_000).await.unwrap()).unwrap();
    let message = body["messages"].as_array().unwrap().iter().find(|m| m["role"] == "assistant").unwrap();
    for (needle, title) in &expected {
        assert_eq!(&title_of(&message["retained_citations"], needle), title, "read {needle}");
    }
    f.done().await;
}
#[tokio::test]
async fn consumer_citation_titles_come_from_retained_version_metadata() {
    citation_titles(false).await;
}
#[tokio::test]
async fn tenant_citation_titles_come_from_retained_version_metadata() {
    citation_titles(true).await;
}

// P10 uses this already-built two-plane harness. All payloads are synthetic;
// model failure/revocation is deterministic and never touches a provider.
struct HistoryModel {
    calls: AtomicUsize,
    fail: bool,
    revoke: Option<(PgPool, Uuid)>,
}
#[async_trait::async_trait]
impl LlmProvider for HistoryModel {
    async fn complete(&self, req: CompletionRequest) -> Result<CompletionResponse, LlmError> {
        assert!(!req.model.is_empty(), "managed run pins a model");
        self.calls.fetch_add(1, Ordering::SeqCst);
        if let Some((pool, item)) = &self.revoke {
            sqlx::query(
                "UPDATE source_items SET access_state='restricted' WHERE source_item_id=$1",
            )
            .bind(item)
            .execute(pool)
            .await
            .unwrap();
        }
        if self.fail {
            return Err(LlmError::Upstream {
                status: 503,
                body: "deterministic managed failure".into(),
            });
        }
        Ok(CompletionResponse {
            content:json!({"atoms":[{"kind":"fact","text":"Retained evidence.","evidence_quote":"Retained evidence.","entities":[{"mention":"Retained evidence","type":"topic"}]}]}).to_string(),
            input_tokens:10, output_tokens:10, model:req.model,
            cache_creation_input_tokens:0,cache_read_input_tokens:0,stop_reason:Some("tool_use".into()),cost_usd:None,
        })
    }
    async fn stream_complete(
        &self,
        _: CompletionRequest,
    ) -> Result<Pin<Box<dyn futures::Stream<Item = Result<StreamEvent, LlmError>> + Send>>, LlmError>
    {
        panic!("managed extraction is nonstreaming")
    }
}
impl Fixture {
    async fn financial(&self) -> sqlx::Transaction<'static,sqlx::Postgres> {
        financial_fixture(&self.control_owner,self.user.account_id).await
    }
    async fn finite_pool(&self, credits: i64) {
        { let mut seed_tx=self.financial().await; let value=sqlx::query("INSERT INTO credit_balances(account_id,balance) VALUES($1,$2::bigint) ON CONFLICT(account_id) DO UPDATE SET balance=excluded.balance")
            .bind(self.user.account_id).bind(credits).execute(&mut *seed_tx).await.unwrap(); seed_tx.commit().await.unwrap(); value };
    }
    async fn history_contract(&self) {
        let mut c = self.owner.acquire().await.unwrap();
        sqlx::query("SET app.identity_serving_contract='composite-v1'")
            .execute(&mut *c)
            .await
            .unwrap();
        sqlx::raw_sql(include_str!(
            "../schema/identity-contract.sql"
        ))
        .execute(&mut *c)
        .await
        .unwrap();
    }
    async fn retained(&self, id: &str) -> (Uuid, Uuid) {
        let mut tx = self
            .tenant
            .begin_member_scoped(self.user.account_id)
            .await
            .unwrap();
        let meta = json!({});
        let capture = coralswarm_backend::source_evidence::CaptureSource {
            source_type: "note",
            provider_namespace: "fixture:p10:retained",
            source_id: id,
            normalized_text: "Retained evidence.",
            representation: "normalized_text",
            metadata: &meta,
            occurred_at: None,
            participants: &[],
            ingested_by: Some(self.user.user_id),
            scope: "private",
            duration_ms: None,
            segments: None,
        };
        let v = coralswarm_backend::source_evidence::capture_source_version_tx(
            &mut tx,
            None,
            self.graph,
            self.user.account_id,
            &capture,
            None,
            &[42; 32],
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
        (v.source_item_id, v.version_id)
    }
    async fn managed_run(&self, items: i64, calls: i64) -> Uuid {
        use axum::{extract::State, Extension, Json};
        let (code,Json(body))=coralswarm_backend::corpus_enrichment::create_run(State(self.state(None)),self.tenant.clone(),Extension(self.user.clone()),Json(serde_json::from_value(json!({"graph_id":self.graph,"execute":true,"local_holdings_unavailable":true,"budget":{"items":items,"characters":50000,"model_calls":calls}})).unwrap())).await;
        assert_eq!(code, axum::http::StatusCode::CREATED, "{body}");
        body["run_id"].as_str().unwrap().parse().unwrap()
    }
    async fn history_advance(&self, run: Uuid) -> Value {
        use axum::{
            extract::{Path, State},
            Extension, Json,
        };
        let (code, Json(body)) = coralswarm_backend::corpus_enrichment::advance_run(
            State(self.state(None)),
            self.tenant.clone(),
            Extension(self.user.clone()),
            Path(run),
            Json(serde_json::from_value(json!({"graph_id":self.graph,"page_size":100})).unwrap()),
        )
        .await;
        assert_eq!(code, axum::http::StatusCode::OK, "{body}");
        body
    }
    async fn history_job(&self, version: Uuid) -> coralswarm_backend::jobs::queue::JobRow {
        let row:(Uuid,String,Value)=sqlx::query_as("UPDATE jobs SET status='running' WHERE id=(SELECT id FROM jobs WHERE payload->>'source_version_id'=$1 AND status='pending' ORDER BY created_at DESC LIMIT 1) RETURNING id,kind,payload")
            .bind(version.to_string()).fetch_one(&self.owner).await.unwrap();
        coralswarm_backend::jobs::queue::JobRow {
            id: row.0,
            kind: row.1,
            payload: row.2,
            attempts: 0,
            account_id: Some(self.user.account_id),
        }
    }
    async fn extract_history(&self, version: Uuid, model: &HistoryModel) {
        let tid = coralswarm_backend::tenancy::TenantId::from_decision(
            self.tenant.decision.as_ref().unwrap(),
        );
        coralswarm_backend::test_p10_source_worker(
            &tid,
            &self.app,
            &self.control_app,
            &GatedScript(model),
            self.history_job(version).await,
        )
        .await;
    }
}
async fn managed_history_plane(org: bool) {
    let Some(f) = Fixture::new(org).await else {
        return;
    };
    f.history_contract().await;
    f.finite_pool(1_000_000).await;
    // Central scalar and set selectors share the same monotonic root admission.
    let guarded:Uuid=sqlx::query_scalar("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,timestamp) VALUES($1,'note','multiple-roots','fixture-only','Fixture roots.',now()) RETURNING atom_id").bind(f.graph).fetch_one(&f.owner).await.unwrap();
    sqlx::query("INSERT INTO source_version_chunks(graph_id,source_version_id,atom_id) SELECT $1,unnest($2::uuid[]),$3").bind(f.graph).bind(vec![f.old.1,f.extra.1]).bind(guarded).execute(&f.owner).await.unwrap();
    for state in ["permitted", "unknown", "restricted"] {
        sqlx::query("UPDATE source_items SET access_state=$2 WHERE source_item_id=$1")
            .bind(f.extra.0)
            .bind(state)
            .execute(&f.owner)
            .await
            .unwrap();
        let mut tx = f
            .tenant
            .begin_member_scoped(f.user.account_id)
            .await
            .unwrap();
        let pair:(bool,bool)=sqlx::query_as("SELECT library_atom_permitted($1,$2),EXISTS(SELECT 1 FROM library_permitted_atoms($1) WHERE atom_id=$2)").bind(f.graph).bind(guarded).fetch_one(&mut *tx).await.unwrap();
        assert_eq!(pair, (state == "permitted", state == "permitted"));
        tx.rollback().await.unwrap();
    }
    sqlx::query("UPDATE source_items SET access_state='permitted' WHERE source_item_id=$1")
        .bind(f.extra.0)
        .execute(&f.owner)
        .await
        .unwrap();
    sqlx::query("DELETE FROM source_version_chunks WHERE atom_id=$1")
        .bind(guarded)
        .execute(&f.owner)
        .await
        .unwrap();
    let mut tx = f
        .tenant
        .begin_member_scoped(f.user.account_id)
        .await
        .unwrap();
    let orphan:(bool,bool)=sqlx::query_as("SELECT library_atom_permitted($1,$2),EXISTS(SELECT 1 FROM library_permitted_atoms($1) WHERE atom_id=$2)").bind(f.graph).bind(guarded).fetch_one(&mut *tx).await.unwrap();
    assert_eq!(orphan, (false, false));
    let genuine:(bool,bool)=sqlx::query_as("SELECT library_atom_permitted($1,$2),EXISTS(SELECT 1 FROM library_permitted_atoms($1) WHERE atom_id=$2)").bind(f.graph).bind(f.legacy).fetch_one(&mut *tx).await.unwrap();
    assert_eq!(genuine, (true, true));
    tx.rollback().await.unwrap();
    assert!(library_read::retrieve_scoped(
        &f.tenant,
        &f.user,
        serde_json::from_value(json!({"graph_ids":[f.denied_graph]})).unwrap(),
        "",
        10
    )
    .await
    .is_err());
    let (item, v) = f.retained("ordinary").await;
    let receipt:(String,Option<Uuid>,String)=sqlx::query_as("SELECT a.state,a.run_id,v.extraction_status FROM corpus_capture_admissions a JOIN source_versions v ON v.version_id=a.source_version_id WHERE a.source_version_id=$1").bind(v).fetch_one(&f.owner).await.unwrap();
    assert_eq!(receipt, ("blocked".into(), None, "blocked".into()));
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM jobs WHERE payload->>'source_version_id'=$1"
        )
        .bind(v.to_string())
        .fetch_one(&f.owner)
        .await
        .unwrap(),
        0
    );
    let read = library_read::retrieve_scoped(&f.tenant, &f.user, f.scope(Some(item)), "", 10)
        .await
        .unwrap();
    assert_eq!(
        read["sources"]["items"][0]["selected_extraction_status"],
        "blocked"
    );
    assert_eq!(
        read["sources"]["items"][0]["selected_extraction_reason"],
        "managed_admission_required"
    );
    // Old directly inserted version job cannot regain the unmanaged dispatch path.
    let id:Uuid=sqlx::query_scalar("INSERT INTO jobs(kind,payload,account_id,status) VALUES('ExtractSourceAtoms',$1,$2,'running') RETURNING id").bind(json!({"graph_id":f.graph,"source_version_id":v,"start_char":0})).bind(f.user.account_id).fetch_one(&f.owner).await.unwrap();
    let model = HistoryModel {
        calls: AtomicUsize::new(0),
        fail: false,
        revoke: None,
    };
    coralswarm_backend::test_p10_source_worker(
        &coralswarm_backend::tenancy::TenantId::from_decision(f.tenant.decision.as_ref().unwrap()),
        &f.app,
        &f.control_app,
        &GatedScript(&model),
        coralswarm_backend::jobs::queue::JobRow {
            id,
            kind: "ExtractSourceAtoms".into(),
            payload: json!({"graph_id":f.graph,"source_version_id":v,"start_char":0}),
            attempts: 0,
            account_id: Some(f.user.account_id),
        },
    )
    .await;
    assert_eq!(model.calls.load(Ordering::SeqCst), 0);
    let transcript:Uuid=sqlx::query_scalar("INSERT INTO meeting_transcripts(graph_id,source_id,raw_inline,summary_status) VALUES($1,'retained-transcript','Retained evidence.','failed') RETURNING id").bind(f.graph).fetch_one(&f.owner).await.unwrap();
    let foreign_key = coralswarm_core::r2::blob_key_for(f.tenant.org_context().map(|c| c.org_id.as_str()),f.denied_graph,Uuid::new_v4()).unwrap();
    let foreign_transcript: Uuid = sqlx::query_scalar("INSERT INTO meeting_transcripts(graph_id,source_id,raw_blob_key,summary_status) VALUES($1,'foreign-locator',$2,'failed') RETURNING id")
        .bind(f.graph).bind(&foreign_key).fetch_one(&f.owner).await.unwrap();
    let copied:Uuid=sqlx::query_scalar("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,promoted_from_atom_id,timestamp) VALUES($1,'note','derived-copy','fixture-only','Retained evidence.',$2,now()) RETURNING atom_id").bind(f.graph).bind(f.legacy).fetch_one(&f.owner).await.unwrap();
    let sibling:Uuid=sqlx::query_scalar("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,timestamp) VALUES($1,'slack','genuine-legacy:chunk:1','fixture-only','Retained evidence.',now()) RETURNING atom_id").bind(f.graph).fetch_one(&f.owner).await.unwrap();
    let run = f.managed_run(5, 4).await;
    // A cohort run defaults to the active source profile; v1 is refused.
    let profile: String = sqlx::query_scalar("SELECT extraction_version FROM corpus_enrichment_runs WHERE run_id=$1").bind(run).fetch_one(&f.owner).await.unwrap();
    assert_eq!(profile, "source-atoms-v2");
    {
        use axum::{extract::State, Extension, Json};
        let (code, Json(body)) = coralswarm_backend::corpus_enrichment::create_run(State(f.state(None)), f.tenant.clone(), Extension(f.user.clone()),
            Json(serde_json::from_value(json!({"graph_id":f.graph,"execute":true,"local_holdings_unavailable":true,"extraction_version":"source-atoms-v1","budget":{"items":1,"characters":100,"model_calls":1}})).unwrap())).await;
        assert_eq!(code, axum::http::StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    }
    let (_, late) = f.retained("late-cohort").await;
    for _ in 0..5 {
        f.history_advance(run).await;
    }
    assert!(!sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(SELECT 1 FROM corpus_enrichment_units WHERE run_id=$1 AND holding_id=$2)"
    )
    .bind(run)
    .bind(late)
    .fetch_one(&f.owner)
    .await
    .unwrap());
    let copied_unit:Uuid=sqlx::query_scalar("SELECT unit_id FROM corpus_enrichment_units WHERE run_id=$1 AND holding_id=$2 AND state='unavailable' AND reason='source_removed_or_access_lost'").bind(run).bind(copied).fetch_one(&f.owner).await.unwrap();
    let (copy_status, _) = coralswarm_backend::test_p10_promote(
        f.state(None),
        f.tenant.clone(),
        f.user.clone(),
        run,
        json!({"graph_id":f.graph,"unit_id":copied_unit}),
    )
    .await;
    // P8: a legacy whole-coral copy is never permitted evidence, so it is
    // unavailable (not merely excluded) and still cannot be promoted.
    assert!(copy_status.is_client_error(), "{copy_status}");
    f.extract_history(v, &model).await;
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);
    let result:(String,i64,i64)=sqlx::query_as("SELECT extraction_status,(SELECT count(*) FROM semantic_atoms WHERE source_version_id=v.version_id),(SELECT count(*) FROM semantic_atom_passages WHERE source_version_id=v.version_id) FROM source_versions v WHERE version_id=$1").bind(v).fetch_one(&f.owner).await.unwrap();
    assert_eq!(result.0, "done");
    assert_eq!(result.1, 1);
    assert_eq!(result.2, 1);
    let prior:(Uuid,i64)=sqlx::query_as("SELECT a.run_id,r.admitted_items FROM corpus_capture_admissions a JOIN corpus_enrichment_runs r ON r.run_id=a.run_id WHERE a.source_version_id=$1").bind(v).fetch_one(&f.owner).await.unwrap();
    assert_eq!(f.retained("ordinary").await.1, v);
    let after:(Uuid,String,i64)=sqlx::query_as("SELECT a.run_id,a.state,r.admitted_items FROM corpus_capture_admissions a JOIN corpus_enrichment_runs r ON r.run_id=a.run_id WHERE a.source_version_id=$1").bind(v).fetch_one(&f.owner).await.unwrap();
    assert_eq!(after, (prior.0, "processed".into(), prior.1));
    let ask = serde_json::from_value(
        json!({"text":"Retained","library_scope":f.scope(Some(item)),"style":"sources-only"}),
    )
    .unwrap();
    let answer = scoped_ask::execute(&f.state(None), &f.tenant, &f.user, ask)
        .await
        .unwrap()
        .payload;
    assert_eq!(answer["mode"], "retrieval_only");
    assert_eq!(f.calls.load(Ordering::SeqCst), 0);
    assert_eq!(f.embeddings.load(Ordering::SeqCst), 0);
    assert_eq!(
        answer["outcomes"][0]["citations"][0]["source_version_id"],
        json!(v)
    );
    // Exact inventoried genuine legacy representation; no fabricated source clock/context.
    sqlx::query("UPDATE atoms SET content_inline='Retained evidence.' WHERE atom_id=$1")
        .bind(f.legacy)
        .execute(&f.owner)
        .await
        .unwrap();
    let unit: Uuid = sqlx::query_scalar(
        "SELECT unit_id FROM corpus_enrichment_units WHERE run_id=$1 AND holding_id=$2",
    )
    .bind(run)
    .bind(f.legacy)
    .fetch_one(&f.owner)
    .await
    .unwrap();
    let (code, axum::Json(baseline)) = coralswarm_backend::test_p10_promote(
        f.state(None),
        f.tenant.clone(),
        f.user.clone(),
        run,
        json!({"graph_id":f.graph,"unit_id":unit}),
    )
    .await;
    assert_eq!(code, axum::http::StatusCode::OK, "{baseline}");
    let bv: Uuid = baseline["source_version_id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    let identity:(String,Option<chrono::DateTime<chrono::Utc>>,Option<Value>,Option<Value>,bool)=sqlx::query_as("SELECT representation,occurred_at,origin,capture_context,is_legacy_baseline FROM source_versions WHERE version_id=$1").bind(bv).fetch_one(&f.owner).await.unwrap();
    assert_eq!(identity, ("normalized_text".into(), None, None, None, true));
    // The baseline and its unit are pinned to the run's profile, never v1.
    let pins: (String, String, String) = sqlx::query_as("SELECT v.extraction_version,u.extraction_version,r.extraction_version FROM source_versions v JOIN corpus_enrichment_units u ON u.promoted_version_id=v.version_id JOIN corpus_enrichment_runs r ON r.run_id=u.run_id WHERE v.version_id=$1")
        .bind(bv).fetch_one(&f.owner).await.unwrap();
    assert_eq!(pins, ("source-atoms-v2".into(), "source-atoms-v2".into(), "source-atoms-v2".into()), "promotion pins its run's profile");
    let (code, axum::Json(retry)) = coralswarm_backend::test_p10_promote(
        f.state(None),
        f.tenant.clone(),
        f.user.clone(),
        run,
        json!({"graph_id":f.graph,"unit_id":unit}),
    )
    .await;
    assert_eq!(code, axum::http::StatusCode::OK, "{retry}");
    assert_eq!(retry["idempotent"], true);
    f.extract_history(bv, &model).await;
    assert_eq!(model.calls.load(Ordering::SeqCst), 2);
    let sibling_unit: Uuid = sqlx::query_scalar(
        "SELECT unit_id FROM corpus_enrichment_units WHERE run_id=$1 AND holding_id=$2",
    )
    .bind(run)
    .bind(sibling)
    .fetch_one(&f.owner)
    .await
    .unwrap();
    let (code, axum::Json(sibling_baseline)) = coralswarm_backend::test_p10_promote(
        f.state(None),
        f.tenant.clone(),
        f.user.clone(),
        run,
        json!({"graph_id":f.graph,"unit_id":sibling_unit}),
    )
    .await;
    assert_eq!(code, axum::http::StatusCode::OK, "{sibling_baseline}");
    let sibling_v: Uuid = sibling_baseline["source_version_id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    f.extract_history(sibling_v, &model).await;
    // Explicit fixture UUID support links exercise breadth aggregation;
    // production never merges these nodes from matching titles alone.
    let shared_node:Uuid=sqlx::query_scalar("SELECT l.node_id FROM node_semantic_atoms l JOIN semantic_atoms a ON a.semantic_atom_id=l.semantic_atom_id WHERE a.source_version_id=$1 ORDER BY l.node_id LIMIT 1").bind(v).fetch_one(&f.owner).await.unwrap();
    sqlx::query("INSERT INTO node_semantic_atoms(node_id,semantic_atom_id,graph_id) SELECT $1,semantic_atom_id,$2 FROM semantic_atoms WHERE source_version_id=ANY($3::uuid[]) ON CONFLICT DO NOTHING").bind(shared_node).bind(f.graph).bind(vec![bv,sibling_v]).execute(&f.owner).await.unwrap();
    let baseline_read =
        library_read::retrieve_scoped(&f.tenant, &f.user, f.scope(None), "Retained", 20)
            .await
            .unwrap();
    let node = baseline_read["nodes"]["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|n| n["node_id"] == json!(shared_node))
        .expect("actual semantic node");
    assert_eq!(
        node["contribution"]["source_support_count"], 1,
        "two unknown-original chunks cannot become three independent source votes"
    );
    assert_eq!(node["contribution"]["unknown_independence_record_count"], 2);
    assert_eq!(node["wider_context"]["source_support_count"], 1, "broad context must not turn normalized baseline records into independent support");
    // A published/derived copy of the genuine source adds no corroboration in
    // either the scoped or the wider projection.
    sqlx::query("UPDATE source_versions SET source_metadata=COALESCE(source_metadata,'{}'::jsonb)||'{\"independence\":\"derived_copy\"}' WHERE version_id=$1")
        .bind(v).execute(&f.owner).await.unwrap();
    let copy_read = library_read::retrieve_scoped(&f.tenant, &f.user, f.scope(None), "Retained", 20).await.unwrap();
    let copy_node = copy_read["nodes"]["items"].as_array().unwrap().iter()
        .find(|n| n["node_id"] == json!(shared_node)).expect("actual semantic node");
    assert_eq!(copy_node["wider_context"]["source_support_count"], 0, "derived copies never corroborate wider context");
    assert_eq!(copy_node["contribution"]["source_support_count"], 0);
    sqlx::query("UPDATE source_versions SET source_metadata=source_metadata-'independence' WHERE version_id=$1")
        .bind(v).execute(&f.owner).await.unwrap();
    assert!(baseline_read["outcomes"]["items"]
        .as_array()
        .unwrap()
        .iter()
        .filter(
            |a| a["source_version_id"] == json!(bv) || a["source_version_id"] == json!(sibling_v)
        )
        .all(|a| a["source_independence"] == "unknown"));

    // Same-org/account keys are not grants to a private foreign graph.
    let foreign_unit: Uuid = sqlx::query_scalar("SELECT unit_id FROM corpus_enrichment_units WHERE run_id=$1 AND holding_id=$2")
        .bind(run).bind(foreign_transcript).fetch_one(&f.owner).await.unwrap();
    let reads = Arc::new(AtomicUsize::new(0));
    let observed = reads.clone();
    let server_app = axum::Router::new().fallback(move || { let count = observed.clone(); async move {
        count.fetch_add(1,Ordering::SeqCst); axum::http::StatusCode::INTERNAL_SERVER_ERROR
    }});
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}",listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener,server_app).await.unwrap() });
    let mut artifact_state = coralswarm_backend::test_app_state_owned_with_db_and_store(f.control_app.clone(),None);
    artifact_state.dek_cache.insert(&coralswarm_backend::tenancy::TenantId::from_decision(f.tenant.decision.as_ref().unwrap()),f.user.account_id,[42;32]);
    artifact_state.r2 = Some(Arc::new(coralswarm_core::r2::R2Client::new_with_endpoint_override("fixture","fixture-access","fixture-secret","fixture-bucket",&endpoint).await));
    let (foreign_status, _) = coralswarm_backend::test_p10_promote(Arc::new(artifact_state),f.tenant.clone(),f.user.clone(),run,json!({"graph_id":f.graph,"unit_id":foreign_unit})).await;
    server.abort();
    assert_eq!(foreign_status,axum::http::StatusCode::CONFLICT);
    assert_eq!(reads.load(Ordering::SeqCst),0,"foreign graph locator rejected before crypto/R2");
    assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM corpus_enrichment_units WHERE run_id=$1 AND unit_id=$2 AND promoted_version_id IS NOT NULL").bind(run).bind(foreign_unit).fetch_one(&f.owner).await.unwrap(),0);

    let transcript_unit: Uuid = sqlx::query_scalar(
        "SELECT unit_id FROM corpus_enrichment_units WHERE run_id=$1 AND holding_id=$2",
    )
    .bind(run)
    .bind(transcript)
    .fetch_one(&f.owner)
    .await
    .unwrap();
    let (code, axum::Json(transcript_baseline)) = coralswarm_backend::test_p10_promote(
        f.state(None),
        f.tenant.clone(),
        f.user.clone(),
        run,
        json!({"graph_id":f.graph,"unit_id":transcript_unit}),
    )
    .await;
    assert_eq!(code, axum::http::StatusCode::OK, "{transcript_baseline}");
    let tv: Uuid = transcript_baseline["source_version_id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT raw_inline FROM source_versions WHERE version_id=$1"
        )
        .bind(tv)
        .fetch_one(&f.owner)
        .await
        .unwrap(),
        "Retained evidence."
    );
    f.extract_history(tv, &model).await;
    assert_eq!(model.calls.load(Ordering::SeqCst), 4);
    // A second immutable run adopts the late cohort. Failure reserves logical
    // cost; a retry cannot be free and the exhausted run pauses before dispatch.
    let next = f.managed_run(2, 1).await;
    for _ in 0..5 {
        f.history_advance(next).await;
    }
    let failure = HistoryModel {
        calls: AtomicUsize::new(0),
        fail: true,
        revoke: None,
    };
    f.extract_history(late, &failure).await;
    assert_eq!(failure.calls.load(Ordering::SeqCst), 1);
    sqlx::query("UPDATE jobs SET status='pending' WHERE payload->>'source_version_id'=$1 AND status='failed'").bind(late.to_string()).execute(&f.owner).await.unwrap();
    f.extract_history(late, &failure).await;
    assert_eq!(failure.calls.load(Ordering::SeqCst), 1);
    let spent:(String,i64,Option<String>)=sqlx::query_as("SELECT status,attempted_model_calls,pause_reason FROM corpus_enrichment_runs WHERE run_id=$1").bind(next).fetch_one(&f.owner).await.unwrap();
    assert_eq!(
        spent,
        (
            "paused".into(),
            1,
            Some("execution_budget_exhausted".into())
        )
    );
    // Every known automatic typed paid purpose is terminal at real queue
    // admission, and a predeployment RUNNING row cannot enter private/model I/O.
    let automatic = HistoryModel {
        calls: AtomicUsize::new(0),
        fail: false,
        revoke: None,
    };
    let cloud_embed_calls_before = f.embeddings.load(Ordering::SeqCst);
    // Re-embedding is now a managed embedding purpose (0324/0156), so queue
    // admission remains pending; unsupported automatic purposes still die below.
    let embedding_id = coralswarm_backend::jobs::queue::enqueue(
        &f.app,
        &coralswarm_backend::jobs::JobKind::ReembedOutdatedChunks { after_atom_id: None },
        Some(f.user.account_id), None, 0,
    ).await.unwrap().unwrap();
    let embedding_status: (String, Option<String>) = sqlx::query_as(
        "SELECT status::text,error_message FROM jobs WHERE id=$1",
    ).bind(embedding_id).fetch_one(&f.owner).await.unwrap();
    assert_eq!(embedding_status, ("pending".into(), None));
    assert_eq!(automatic.calls.load(Ordering::SeqCst), 0, "queue admission never invokes the model");
    assert_eq!(f.embeddings.load(Ordering::SeqCst), cloud_embed_calls_before, "queue admission never embeds");
    let payload = json!({"atom_ids":[f.legacy],"atom_id":f.legacy,"node_id":f.old.2,"graph_id":f.graph,"source_id":"retained-transcript","after_atom_id":null,"allow_cloud_override":false});
    // Retired kinds (2026-10-18) have no JobKind variant, so no Rust producer
    // can enqueue one; a hand-inserted row is still terminal at admission.
    for kind in ["IngestDoor","ConflictDetection","InferExpertise","ReevaluateKind","ExtractThreads","ExtractDecisions"] {
        assert!(serde_json::from_value::<coralswarm_backend::jobs::JobKind>(json!({"kind":kind,"payload":payload})).is_err(), "{kind} is retired");
        let row:(String,Option<String>)=sqlx::query_as("INSERT INTO jobs(kind,payload,account_id) VALUES($1,$2,$3) RETURNING status::text,error_message")
            .bind(kind).bind(&payload).bind(f.user.account_id).fetch_one(&f.owner).await.unwrap();
        assert_eq!(row, ("dead".into(), Some("managed_purpose_unsupported".into())), "{kind}");
    }
    // InferRelationships makes no model call: it is admitted, not dead-lettered.
    let closure = coralswarm_backend::jobs::queue::enqueue(
        &f.app,
        &coralswarm_backend::jobs::JobKind::InferRelationships { graph_id: f.graph },
        Some(f.user.account_id), None, 0,
    ).await.unwrap().unwrap();
    assert_eq!(
        sqlx::query_as::<_, (String, Option<String>)>("SELECT status::text,error_message FROM jobs WHERE id=$1").bind(closure).fetch_one(&f.owner).await.unwrap(),
        ("pending".into(), None)
    );
    sqlx::query("DELETE FROM jobs WHERE id=$1").bind(closure).execute(&f.owner).await.unwrap();
    // Managed enrichment kinds (source-agnostic PRs 6-8, 10): admitted pending;
    // with the shared switch off a direct worker invocation spends nothing
    // and hands the work back (never dead-lettered).
    std::env::remove_var("ENRICHMENT_PROCESSING_ENABLED");
    for kind in ["RegenerateDigest", "BackfillAtomTitles", "GenerateMeetingSummary"] {
        let typed: coralswarm_backend::jobs::JobKind =
            serde_json::from_value(json!({"kind":kind,"payload":payload.clone()})).unwrap();
        let id = coralswarm_backend::jobs::queue::enqueue(&f.app, &typed, Some(f.user.account_id), None, 0)
            .await.unwrap().unwrap();
        assert_eq!(
            sqlx::query_as::<_, (String, Option<String>)>("SELECT status::text,error_message FROM jobs WHERE id=$1").bind(id).fetch_one(&f.owner).await.unwrap(),
            ("pending".into(), None),
            "{kind} is managed enrichment work, admitted"
        );
        sqlx::query("UPDATE jobs SET status='running',attempts=1 WHERE id=$1").bind(id).execute(&f.owner).await.unwrap();
        coralswarm_backend::test_p10_automatic_worker(
            &coralswarm_backend::tenancy::TenantId::from_decision(f.tenant.decision.as_ref().unwrap()),
            &f.app,
            &f.control_app,
            &CountEmbed(f.embeddings.clone()),
            &automatic,
            coralswarm_backend::jobs::queue::JobRow { id, kind: kind.into(), payload: payload.clone(), attempts: 1, account_id: Some(f.user.account_id) },
        )
        .await;
        // Held in place under the shared retry rule (`jobs::queue::hold`).
        assert_eq!(
            sqlx::query_as::<_, (String, Option<String>)>("SELECT status::text,error_message FROM jobs WHERE id=$1").bind(id).fetch_one(&f.owner).await.unwrap(),
            ("pending".into(), Some("held: switched_off".into())),
            "{kind}: switched off, handed back"
        );
        sqlx::query("DELETE FROM jobs WHERE kind=$1").bind(kind).execute(&f.owner).await.unwrap();
    }
    // Retired per-atom extraction (source-agnostic PR 11): no variant, so no
    // Rust path can enqueue it, and a hand-inserted row is dead on arrival.
    for kind in ["ExtractEntities", "ExtractRelationships"] {
        assert!(
            serde_json::from_value::<coralswarm_backend::jobs::JobKind>(json!({"kind":kind,"payload":payload.clone()})).is_err(),
            "{kind} has no JobKind variant"
        );
        let status: (String, String) = sqlx::query_as(
            "INSERT INTO jobs(kind,payload,account_id) VALUES($1,$2,$3) RETURNING status::text,error_message",
        )
        .bind(kind).bind(&payload).bind(f.user.account_id).fetch_one(&f.app).await.unwrap();
        assert_eq!(status, ("dead".into(), "managed_purpose_unsupported".into()), "{kind}");
    }
    assert_eq!(f.embeddings.load(Ordering::SeqCst), cloud_embed_calls_before, "unsupported cloud reembed worker never dispatches a provider");
    // Embed jobs are admitted: their cloud vectors go through the managed
    // embedding path inside the worker (managed::gate::pg_tests::
    // embedding_workers proves it; workers_ingest_rls_pg / worker_dek_egress_pg
    // prove an ungated provider is never called on either plane).
    for kind in ["EmbedAtoms", "EmbedAtomsRetry"] {
        let typed: coralswarm_backend::jobs::JobKind =
            serde_json::from_value(json!({"kind":kind,"payload":{"atom_ids":[f.legacy]}})).unwrap();
        let id = coralswarm_backend::jobs::queue::enqueue(&f.app, &typed, Some(f.user.account_id), None, 0)
            .await.unwrap().unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT status::text FROM jobs WHERE id=$1").bind(id).fetch_one(&f.owner).await.unwrap(),
            "pending",
            "{kind} is managed embedding work, not a terminal paid purpose"
        );
        sqlx::query("DELETE FROM jobs WHERE id=$1").bind(id).execute(&f.owner).await.unwrap();
    }
    coralswarm_backend::test_p10_automatic_worker(
        &coralswarm_backend::tenancy::TenantId::from_decision(f.tenant.decision.as_ref().unwrap()),
        &f.app,
        &f.control_app,
        &CountEmbed(f.embeddings.clone()),
        &automatic,
        coralswarm_backend::jobs::queue::JobRow {
            id: Uuid::new_v4(),
            kind: "JournalSweep".into(),
            payload: json!({}),
            attempts: 0,
            account_id: None,
        },
    )
    .await;
    assert_eq!(automatic.calls.load(Ordering::SeqCst), 0);
    assert_eq!(f.embeddings.load(Ordering::SeqCst), 0);
    // Post-provider permission loss discards all output; reservation persists.
    let (revoked_item, revoked) = f.retained("revoke-model").await;
    let revoke_run = f.managed_run(2, 3).await;
    for _ in 0..5 {
        f.history_advance(revoke_run).await;
    }
    let revoked_model = HistoryModel {
        calls: AtomicUsize::new(0),
        fail: false,
        revoke: Some((f.owner.clone(), revoked_item)),
    };
    f.extract_history(revoked, &revoked_model).await;
    assert_eq!(revoked_model.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM semantic_atoms WHERE source_version_id=$1"
        )
        .bind(revoked)
        .fetch_one(&f.owner)
        .await
        .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT attempted_model_calls FROM corpus_enrichment_runs WHERE run_id=$1"
        )
        .bind(revoke_run)
        .fetch_one(&f.owner)
        .await
        .unwrap(),
        1
    );
    // Managed refusal is atomic: no baseline or job is retained as a downgraded success.
    let refused:Uuid=sqlx::query_scalar("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,timestamp) VALUES($1,'note','refused-baseline','fixture-only','Retained evidence.',now()) RETURNING atom_id").bind(f.graph).fetch_one(&f.owner).await.unwrap();
    let refusal_run = f.managed_run(1, 3).await;
    for _ in 0..5 {
        f.history_advance(refusal_run).await;
    }
    let refusal_unit: Uuid = sqlx::query_scalar(
        "SELECT unit_id FROM corpus_enrichment_units WHERE run_id=$1 AND holding_id=$2",
    )
    .bind(refusal_run)
    .bind(refused)
    .fetch_one(&f.owner)
    .await
    .unwrap();
    // The single slot is occupied by an existing failed version, or reserve it
    // here as fixture setup; the real promotion performs the atomic refusal.
    sqlx::query("UPDATE corpus_enrichment_runs SET admitted_items=max_items,status='running',pause_reason=NULL WHERE run_id=$1").bind(refusal_run).execute(&f.owner).await.unwrap();
    let (code, axum::Json(rejected)) = coralswarm_backend::test_p10_promote(
        f.state(None),
        f.tenant.clone(),
        f.user.clone(),
        refusal_run,
        json!({"graph_id":f.graph,"unit_id":refusal_unit}),
    )
    .await;
    assert_eq!(code, axum::http::StatusCode::CONFLICT, "{rejected}");
    assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM source_items WHERE graph_id=$1 AND provider_namespace LIKE 'legacy:baseline:%' AND source_id=$2").bind(f.graph).bind(refused.to_string()).fetch_one(&f.owner).await.unwrap(),0);
    assert!(sqlx::query_scalar::<_, Option<Uuid>>(
        "SELECT promoted_version_id FROM corpus_enrichment_units WHERE unit_id=$1"
    )
    .bind(refusal_unit)
    .fetch_one(&f.owner)
    .await
    .unwrap()
    .is_none());
    // Fresh requester policy before dispatch: no source/body/provider fallback.
    let (_, actor_version) = f.retained("revoke-before-dispatch").await;
    let actor_run = f.managed_run(3, 3).await;
    for _ in 0..5 {
        f.history_advance(actor_run).await;
    }
    sqlx::query("DELETE FROM account_members WHERE account_id=$1 AND user_id=$2")
        .bind(f.user.account_id)
        .bind(f.user.user_id)
        .execute(&f.control_owner)
        .await
        .unwrap();
    let denied_model = HistoryModel {
        calls: AtomicUsize::new(0),
        fail: false,
        revoke: None,
    };
    f.extract_history(actor_version, &denied_model).await;
    assert_eq!(denied_model.calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT attempted_model_calls FROM corpus_enrichment_runs WHERE run_id=$1"
        )
        .bind(actor_run)
        .fetch_one(&f.owner)
        .await
        .unwrap(),
        0
    );
    sqlx::query("INSERT INTO account_members(account_id,user_id,role,seat_tier_id) VALUES($1,$2,'owner',(SELECT CASE WHEN is_org_account THEN (SELECT id FROM seat_tiers WHERE code='standard' AND is_active) END FROM accounts WHERE id=$1))").bind(f.user.account_id).bind(f.user.user_id).execute(&f.control_owner).await.unwrap();
    // Synthetic service canary uses truthful origin/mode and the same managed
    // versions -> exact semantic passages/nodes -> Library/Ask path, no provider.
    let mut tx = f
        .tenant
        .begin_member_scoped(f.user.account_id)
        .await
        .unwrap();
    let origin:coralswarm_backend::source_origin::SourceOrigin=serde_json::from_value(json!({"service_id":"linear","transport":"synthetic","content_kind":"issue","capture_surface":"synthetic_demo"})).unwrap();
    let capture = coralswarm_backend::source_evidence::CaptureSource {
        source_type: "linear_issue",
        provider_namespace: "demo:linear:workspace:p10",
        source_id: "demo-immutable-id",
        normalized_text: "Retained evidence.",
        representation: "normalized_text",
        metadata: &json!({"synthetic_demo":true}),
        occurred_at: None,
        participants: &[],
        ingested_by: Some(f.user.user_id),
        scope: "private",
        duration_ms: None,
        segments: None,
    };
    let demo = coralswarm_backend::source_evidence::capture_source_version_with_origin_tx(
        &mut tx,
        None,
        f.graph,
        f.user.account_id,
        &capture,
        None,
        &[42; 32],
        Some(&origin),
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    let (code,axum::Json(demo_body))=coralswarm_backend::corpus_enrichment::create_run(axum::extract::State(f.state(None)),f.tenant.clone(),axum::Extension(f.user.clone()),axum::Json(serde_json::from_value(json!({"graph_id":f.graph,"execute":true,"synthetic_demo":true,"budget":{"items":100,"characters":50000,"model_calls":1}})).unwrap())).await;
    assert_eq!(code, axum::http::StatusCode::CREATED, "{demo_body}");
    let demo_run: Uuid = demo_body["run_id"].as_str().unwrap().parse().unwrap();
    for _ in 0..5 {
        f.history_advance(demo_run).await;
    }
    f.extract_history(demo.version_id, &model).await;
    let demo_answer=scoped_ask::execute(&f.state(None),&f.tenant,&f.user,serde_json::from_value(json!({"text":"Retained","style":"sources-only","library_scope":f.scope(Some(demo.source_item_id))})).unwrap()).await.unwrap().payload;
    assert_eq!(demo_answer["contains_synthetic_demo_evidence"], true);
    assert_eq!(
        demo_answer["retained_citations"][0]["source_version_id"],
        json!(demo.version_id)
    );
    assert!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM node_semantic_atoms l JOIN semantic_atoms a ON a.semantic_atom_id=l.semantic_atom_id WHERE a.source_version_id=$1").bind(demo.version_id).fetch_one(&f.owner).await.unwrap()>0);
    // Bound markers survive lost mappings; there is no orphan legacy escape.
    sqlx::query("DELETE FROM source_version_chunks WHERE atom_id=$1")
        .bind(f.legacy)
        .execute(&f.owner)
        .await
        .unwrap();
    let orphan_run = f.managed_run(3, 3).await;
    for _ in 0..5 {
        f.history_advance(orphan_run).await;
    }
    let orphan_unit: Uuid = sqlx::query_scalar(
        "SELECT unit_id FROM corpus_enrichment_units WHERE run_id=$1 AND holding_id=$2",
    )
    .bind(orphan_run)
    .bind(f.legacy)
    .fetch_one(&f.owner)
    .await
    .unwrap();
    let (code, _) = coralswarm_backend::test_p10_promote(
        f.state(None),
        f.tenant.clone(),
        f.user.clone(),
        orphan_run,
        json!({"graph_id":f.graph,"unit_id":orphan_unit}),
    )
    .await;
    assert_eq!(code, axum::http::StatusCode::CONFLICT);
    let coverage = f.history_advance(orphan_run).await;
    assert!(coverage["coverage"]
        .as_array()
        .unwrap()
        .iter()
        .any(|c| c["kind"] == "local_holdings"
            && c["reason"] == "device_key_read_timeout_15s_no_retry_or_upload"));
    f.done().await;
}
#[tokio::test]
async fn p10_managed_history_consumer() {
    managed_history_plane(false).await;
}
#[tokio::test]
async fn p10_managed_history_tenant() {
    managed_history_plane(true).await;
}

#[tokio::test]
async fn p10_managed_consumer_one_connection_preserves_fresh_admission() {
    let Some(mut f) = Fixture::new(false).await else { return; };
    f.history_contract().await;
    f.finite_pool(1_000_000).await;
    let (_, version) = f.retained("one-connection-allowed").await;
    let (_, revoked_version) = f.retained("one-connection-revoked").await;
    let run = f.managed_run(3, 3).await;
    for _ in 0..5 { f.history_advance(run).await; }
    let (_, unmanaged) = f.retained("unmanaged-private-inline").await;
    let one = PgPoolOptions::new().max_connections(1)
        .acquire_timeout(std::time::Duration::from_millis(300))
        .after_connect(|conn, _| Box::pin(async move {
            sqlx::query("SET search_path=fixture_original_observer,public").execute(conn).await?;
            Ok(())
        }))
        .connect_with((*f.app.connect_options()).clone()).await.unwrap();
    let prior_app = f.app.clone();
    let prior_control = f.control_app.clone();
    f.app = one.clone();
    f.control_app = one.clone();
    f.tenant.pool = Some(one.clone());
    f.tenant.store = Some(Arc::new(PostgresStore::new(one.clone())));
    // A role-invoking test view observes private text projections. Readiness
    // queries prune these observed columns; SELECT * evaluates them. No new
    // production helper or copied worker can make this assertion pass.
    sqlx::raw_sql("CREATE SCHEMA fixture_original_observer; CREATE SEQUENCE public.fixture_original_reads; CREATE FUNCTION public.fixture_observe_original(t TEXT) RETURNS TEXT LANGUAGE plpgsql STABLE AS $$ BEGIN IF t IS NOT NULL THEN PERFORM nextval('public.fixture_original_reads'); END IF; RETURN t; END $$;")
        .execute(&f.owner).await.unwrap();
    let columns: String = sqlx::query_scalar("SELECT string_agg(CASE WHEN attname IN ('raw_inline','raw_blob_key') THEN format('public.fixture_observe_original(%I) AS %I',attname,attname) ELSE format('%I',attname) END,',' ORDER BY attnum) FROM pg_attribute WHERE attrelid='source_versions'::regclass AND attnum>0 AND NOT attisdropped")
        .fetch_one(&f.owner).await.unwrap();
    // Keep the production physical relation and exact trigger binding intact.
    // Only this singleton pool resolves the test-owned invoker view; fixed-path
    // permission/cleanup functions continue to resolve public.source_versions.
    sqlx::raw_sql(&format!("CREATE VIEW fixture_original_observer.source_versions WITH(security_invoker=true) AS SELECT {columns} FROM public.source_versions; GRANT USAGE ON SCHEMA fixture_original_observer TO coralswarm_app; GRANT SELECT,UPDATE ON fixture_original_observer.source_versions TO coralswarm_app; GRANT USAGE,SELECT ON SEQUENCE public.fixture_original_reads TO coralswarm_app;"))
        .execute(&f.owner).await.unwrap();
    let physical_binding: (String, String, bool) = sqlx::query_as(
        "SELECT (SELECT relkind::text FROM pg_class WHERE oid='public.source_versions'::regclass),(SELECT relkind::text FROM pg_class WHERE oid='fixture_original_observer.source_versions'::regclass),EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid='public.source_versions'::regclass AND tgname='aa_reef_identity_version_loss' AND tgenabled<>'D')",
    ).fetch_one(&f.owner).await.unwrap();
    assert_eq!(physical_binding, ("r".into(), "v".into(), true));
    let old: Uuid = sqlx::query_scalar("INSERT INTO jobs(kind,payload,account_id,status) VALUES('ExtractSourceAtoms',$1,$2,'running') RETURNING id")
        .bind(json!({"graph_id":f.graph,"source_version_id":unmanaged,"start_char":0})).bind(f.user.account_id).fetch_one(&f.owner).await.unwrap();
    // This late source is outside the pinned cohort and has no managed owner.
    let model = HistoryModel { calls: AtomicUsize::new(0), fail: false, revoke: None };
    coralswarm_backend::test_p10_source_worker(&coralswarm_backend::tenancy::TenantId::Consumer,&one,&one,&GatedScript(&model),
        coralswarm_backend::jobs::queue::JobRow {id:old,kind:"ExtractSourceAtoms".into(),payload:json!({"graph_id":f.graph,"source_version_id":unmanaged,"start_char":0}),attempts:0,account_id:Some(f.user.account_id)}).await;
    let before_original: bool = sqlx::query_scalar("SELECT is_called FROM fixture_original_reads").fetch_one(&f.owner).await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), f.extract_history(version, &model))
        .await.expect("managed worker must not wait on its held CONTROL lease");
    let outcome: (String, String, i64) = sqlx::query_as(
        "SELECT v.extraction_status,a.state,r.attempted_model_calls FROM source_versions v JOIN corpus_capture_admissions a ON a.source_version_id=v.version_id JOIN corpus_enrichment_runs r ON r.run_id=a.run_id WHERE v.version_id=$1",
    ).bind(version).fetch_one(&f.owner).await.unwrap();
    // Denials must still be fresh on that one held executor, not cached grants.
    sqlx::query("DELETE FROM account_members WHERE account_id=$1 AND user_id=$2")
        .bind(f.user.account_id).bind(f.user.user_id).execute(&f.control_owner).await.unwrap();
    f.extract_history(revoked_version, &model).await;
    let denied: i64 = sqlx::query_scalar("SELECT count(*) FROM semantic_atoms WHERE source_version_id=ANY($1::uuid[])")
        .bind(vec![revoked_version]).fetch_one(&f.owner).await.unwrap();
    let original_reads: i64 = sqlx::query_scalar("SELECT last_value FROM fixture_original_reads").fetch_one(&f.owner).await.unwrap();
    let calls = model.calls.load(Ordering::SeqCst);
    assert!(!before_original, "unmanaged job must never project private inline text or locators");
    assert_eq!(original_reads, 1, "only admitted managed original is read; the member denial reads none");
    assert_eq!(calls, 1, "only the admitted retained version reaches the provider");
    assert_eq!(outcome, ("done".into(), "processed".into(), 1));
    assert_eq!(denied, 0, "member denial cannot persist semantic evidence");
    f.done().await;
    prior_app.close().await;
    prior_control.close().await;
}

async fn managed_inventory_shape(org: bool) {
    use axum::{
        extract::{Path, Query, State},
        Extension, Json,
    };
    let Some(f) = Fixture::new(org).await else {
        return;
    };
    f.history_contract().await;
    let (item, version) = f.retained("many-chunks-one-item").await;
    let ids:Vec<Uuid>=sqlx::query_scalar("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,timestamp) SELECT $1,'note','shape-'||n,'fixture-only','Retained evidence.',now() FROM generate_series(1,68000) n RETURNING atom_id").bind(f.graph).fetch_all(&f.owner).await.unwrap();
    sqlx::query("INSERT INTO source_version_chunks(graph_id,source_version_id,atom_id) SELECT $1,$2,unnest($3::uuid[])").bind(f.graph).bind(version).bind(&ids).execute(&f.owner).await.unwrap();
    let (code, Json(body)) = coralswarm_backend::corpus_enrichment::create_run(
        State(f.state(None)),
        f.tenant.clone(),
        Extension(f.user.clone()),
        Json(
            serde_json::from_value(json!({"graph_id":f.graph,"local_holdings_unavailable":true}))
                .unwrap(),
        ),
    )
    .await;
    assert_eq!(code, axum::http::StatusCode::CREATED, "{body}");
    let run: Uuid = body["run_id"].as_str().unwrap().parse().unwrap();
    // Fixture models completed bounded page discovery. The real advance route
    // below still exercises the 68k-chunk inventory selector/pinned cohort.
    sqlx::query("INSERT INTO corpus_enrichment_units(run_id,graph_id,kind,holding_id,source_item_id,extraction_version,state,original_availability,processing_path) VALUES($1,$2,'retained_version',$3,$4,'source-atoms-v2','pending','available','retained_version')").bind(run).bind(f.graph).bind(version).bind(item).execute(&f.owner).await.unwrap();
    sqlx::query("INSERT INTO corpus_enrichment_units(run_id,graph_id,kind,holding_id,state,reason,original_availability,processing_path) SELECT $1,$2,'legacy_chunk',unnest($3::uuid[]),'excluded','covered_by_retained_version','available','covered_by_retained_version'").bind(run).bind(f.graph).bind(&ids).execute(&f.owner).await.unwrap();
    sqlx::query("UPDATE corpus_enrichment_runs SET stage=1 WHERE run_id=$1")
        .bind(run)
        .execute(&f.owner)
        .await
        .unwrap();
    for table in [
        "atoms",
        "source_version_chunks",
        "corpus_enrichment_units",
        "source_items",
        "source_versions",
    ] {
        f.owner
            .execute(format!("ANALYZE {table}").as_str())
            .await
            .unwrap();
    }
    let start = std::time::Instant::now();
    let (code, Json(coverage)) = coralswarm_backend::corpus_enrichment::get_run(
        f.tenant.clone(),
        Extension(f.user.clone()),
        Path(run),
        Query(serde_json::from_value(json!({"graph_id":f.graph})).unwrap()),
    )
    .await;
    assert_eq!(code, axum::http::StatusCode::OK, "{coverage}");
    let elapsed = start.elapsed();
    let counts = coverage["coverage"].as_array().unwrap();
    assert!(counts.iter().any(|r| r["kind"] == "retained_version"
        && r["source_item_count"] == 1
        && r["holding_count"] == 1));
    assert!(counts.iter().any(|r| r["kind"] == "legacy_chunk"
        && r["holding_count"] == 68000
        && r["source_item_count"].is_null()));
    let start = std::time::Instant::now();
    let (code, Json(page)) = coralswarm_backend::corpus_enrichment::get_units(
        f.tenant.clone(),
        Extension(f.user.clone()),
        Path(run),
        Query(serde_json::from_value(json!({"graph_id":f.graph,"limit":100})).unwrap()),
    )
    .await;
    assert_eq!(code, axum::http::StatusCode::OK, "{page}");
    let paged = start.elapsed();
    let start = std::time::Instant::now();
    f.history_advance(run).await;
    let advanced = start.elapsed();
    eprintln!("P10 {org:?} 68000 chunks / 1 item: coverage={elapsed:?}, units100={paged:?}, inventory100={advanced:?}");
    assert!(
        elapsed.as_secs_f64() < 5.0 && paged.as_secs_f64() < 5.0 && advanced.as_secs_f64() < 5.0,
        "inventory-shaped query budget exceeded"
    );
    assert_eq!(f.calls.load(Ordering::SeqCst), 0);
    f.done().await;
}
#[tokio::test]
async fn p10_inventory_shape_consumer() {
    managed_inventory_shape(false).await;
}
#[tokio::test]
async fn p10_inventory_shape_tenant() {
    managed_inventory_shape(true).await;
}

// P8 extends this exact-role fixture instead of compiling a second large binary.
async fn p8_json(r: axum::response::Response) -> (axum::http::StatusCode, Value) {
    let status = r.status();
    assert_eq!(r.headers()["cache-control"], "no-store");
    let bytes = axum::body::to_bytes(r.into_body(), 1024 * 1024)
        .await
        .unwrap();
    (status, serde_json::from_slice(&bytes).unwrap())
}
async fn p8_run(org: bool) {
    use axum::{
        extract::{Path, State},
        http::StatusCode,
        Extension,
    };
    use coralswarm_backend::{
        library_read::LibraryJson,
        selected_publication::{self, PublishRequest},
    };
    let Some(f) = Fixture::new(org).await else {
        return;
    };
    let state = f.state(None);
    if let Some(ctx) = f.tenant.org_context() {
        sqlx::query("INSERT INTO tenant_meta(id,org_id) VALUES(true,$1) ON CONFLICT(id) DO UPDATE SET org_id=EXCLUDED.org_id").bind(&ctx.org_id).execute(&f.owner).await.unwrap();
    }
    let ready: bool =
        sqlx::query_scalar("SELECT to_regclass('public.corpus_capture_admissions') IS NOT NULL")
            .fetch_one(&f.owner)
            .await
            .unwrap();
    assert!(
        ready,
        "P8 requires the actual reviewed P10 capture-admission migration307/142"
    );
    let target:Uuid=sqlx::query_scalar("INSERT INTO graphs(account_id,name,kind) VALUES($1,'P8 synthetic shared selected evidence','org') RETURNING id").bind(f.user.account_id).fetch_one(&f.owner).await.unwrap();
    let text = "PRIVATE PREFIX🙂 APPROVED É secret suffix";
    let original = source(&f.owner, f.graph, "p8-unicode", text, false).await;
    sqlx::query("UPDATE source_versions SET source_metadata=$2 WHERE version_id=$1")
        .bind(original.1)
        .bind(json!({"title":"PRIVATE ANCESTOR LABEL","parent_session_id":"PRIVATE ANCESTOR ID"}))
        .execute(&f.owner)
        .await
        .unwrap();
    sqlx::query("UPDATE semantic_atoms SET statement='UNSELECTED PRIVATE MODEL INFERENCE' WHERE semantic_atom_id=$1").bind(original.2).execute(&f.owner).await.unwrap();
    let passage: Uuid = sqlx::query_scalar(
        "SELECT passage_id FROM semantic_atom_passages WHERE semantic_atom_id=$1",
    )
    .bind(original.2)
    .fetch_one(&f.owner)
    .await
    .unwrap();
    let start = "PRIVATE PREFIX🙂 ".chars().count() as i32;
    let end = start + "APPROVED É".chars().count() as i32;
    let request = Uuid::new_v4();
    let consent = json!({"request_id":request,"source_graph_id":f.graph,"source_item_id":original.0,"source_version_id":original.1,"target_graph_id":target,"passages":[{"passage_id":passage,"start_char":start,"end_char":end}]});
    let publish = |q: Value| {
        let q: PublishRequest = serde_json::from_value(q).unwrap();
        selected_publication::publish(
            State(state.clone()),
            f.tenant.clone(),
            Extension(f.user.clone()),
            LibraryJson(q),
        )
    };
    let (status, receipt) = p8_json(publish(consent.clone()).await).await;
    assert_eq!(status, StatusCode::CREATED, "{receipt}");
    let item: Uuid = serde_json::from_value(receipt["source_item_id"].clone()).unwrap();
    let version: Uuid = serde_json::from_value(receipt["source_version_id"].clone()).unwrap();
    let publication: Uuid = serde_json::from_value(receipt["publication_id"].clone()).unwrap();
    let (raw,metadata,origin,model,statement):(String,Value,Value,String,String)=sqlx::query_as("SELECT v.raw_inline,v.source_metadata,v.origin,a.model,a.statement FROM source_versions v JOIN semantic_atoms a ON a.graph_id=v.graph_id AND a.source_version_id=v.version_id WHERE v.version_id=$1").bind(version).fetch_one(&f.owner).await.unwrap();
    assert_eq!(raw, "APPROVED É");
    assert_eq!(statement, raw);
    assert_eq!(model, "deterministic-selection");
    assert_eq!(metadata["independence"], "derived_copy");
    assert_eq!(origin["transport"], "synthetic");
    assert_eq!(origin["service_id"], "coralswarm");
    for value in [&metadata, &receipt, &origin] {
        assert!(!value.to_string().contains("PRIVATE"));
        assert!(!value.to_string().contains(&original.0.to_string()));
        assert!(!value.to_string().contains(&f.graph.to_string()));
    }
    let admission:(String,String,Option<Uuid>)=sqlx::query_as("SELECT state,extraction_version,run_id FROM corpus_capture_admissions WHERE source_version_id=$1").bind(version).fetch_one(&f.owner).await.unwrap();
    assert_eq!(
        admission,
        ("processed".into(), "selected-passages-v1".into(), None)
    );
    assert_eq!(
        p8_json(publish(consent.clone()).await).await.0,
        StatusCode::OK
    );
    let mut mismatch = consent.clone();
    mismatch["source_item_id"] = json!(f.extra.0);
    assert_eq!(
        p8_json(publish(mismatch).await).await.0,
        StatusCode::NOT_FOUND
    );
    let mut changed = consent.clone();
    changed["passages"][0]["start_char"] = json!(start + 1);
    assert_eq!(
        p8_json(publish(changed).await).await.0,
        StatusCode::CONFLICT
    );
    // Same ID and same locator cannot certify changed bytes or forged consent.
    assert!(sqlx::query(
        "UPDATE source_versions SET raw_inline='REBIND SECRET' WHERE version_id=$1"
    )
    .bind(original.1)
    .execute(&f.owner)
    .await
    .is_err());
    assert!(sqlx::query(
        "UPDATE selected_evidence_publications SET target_graph_id=$2 WHERE publication_id=$1"
    )
    .bind(publication)
    .bind(f.denied_graph)
    .execute(&f.owner)
    .await
    .is_err());
    // Scoped shared readers get only the selected destination, without needing
    // authority to the original member-private graph.
    let other = UserContext {
        user_id: Uuid::new_v4(),
        ..f.user.clone()
    };
    sqlx::query("INSERT INTO users(id,email) VALUES($1,$2)")
        .bind(other.user_id)
        .bind(format!("p8-other-{}@fixture.test", other.user_id))
        .execute(&f.control_owner)
        .await
        .unwrap();
    sqlx::query("INSERT INTO account_members(account_id,user_id,role,seat_tier_id) VALUES($1,$2,'member',(SELECT CASE WHEN a.is_org_account THEN (SELECT id FROM seat_tiers WHERE code='standard' AND is_active) END FROM accounts a WHERE a.id=$1))").bind(other.account_id).bind(other.user_id).execute(&f.control_owner).await.unwrap();
    let mut shared = f.tenant.clone();
    if let Some(TenancyDecision::Org(ctx)) = shared.decision.as_mut() {
        ctx.user_id = other.user_id;
    }
    let mut scope: LibraryScope =
        serde_json::from_value(json!({"graph_ids":[target],"source_item_id":item})).unwrap();
    assert!(library_read::authorize_scope(&shared, &other, &mut scope)
        .await
        .is_ok());
    assert!(
        coralswarm_backend::source_evidence::still_admitted(
            &state,
            &shared,
            &other,
            target,
            other.account_id
        )
        .await
    );
    let mut private_scope: LibraryScope =
        serde_json::from_value(json!({"graph_ids":[f.graph]})).unwrap();
    assert!(
        library_read::authorize_scope(&shared, &other, &mut private_scope)
            .await
            .is_err()
    );
    // Permission loss removes the destination bytes, not the recoverable
    // canonical original. Independently allowed sources remain intact.
    sqlx::query("UPDATE source_items SET access_state='restricted' WHERE source_item_id=$1")
        .bind(original.0)
        .execute(&f.owner)
        .await
        .unwrap();
    let gone: i64 = sqlx::query_scalar("SELECT count(*) FROM source_versions WHERE version_id=$1")
        .bind(version)
        .fetch_one(&f.owner)
        .await
        .unwrap();
    assert_eq!(gone, 0);
    let retained: i64 =
        sqlx::query_scalar("SELECT count(*) FROM source_versions WHERE version_id=ANY($1)")
            .bind(vec![original.1, f.extra.1])
            .fetch_one(&f.owner)
            .await
            .unwrap();
    assert_eq!(retained, 2);
    let status: String = sqlx::query_scalar(
        "SELECT status FROM selected_evidence_publications WHERE publication_id=$1",
    )
    .bind(publication)
    .fetch_one(&f.owner)
    .await
    .unwrap();
    assert_eq!(status, "retracted");
    sqlx::query("UPDATE source_items SET access_state='permitted' WHERE source_item_id=$1")
        .bind(original.0)
        .execute(&f.owner)
        .await
        .unwrap();
    assert_eq!(
        p8_json(publish(consent.clone()).await).await.0,
        StatusCode::CONFLICT
    );
    let mut next = consent.clone();
    next["request_id"] = json!(Uuid::new_v4());
    let (_, next_receipt) = p8_json(publish(next).await).await;
    let next_id: Uuid = serde_json::from_value(next_receipt["publication_id"].clone()).unwrap();
    // An authorized retraction succeeds.
    assert_eq!(
        p8_json(
            selected_publication::retract(
                State(state.clone()),
                f.tenant.clone(),
                Extension(f.user.clone()),
                Path(next_id)
            )
            .await
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(f.calls.load(Ordering::SeqCst), 0);
    assert_eq!(f.embeddings.load(Ordering::SeqCst), 0);
    f.done().await;
}
#[tokio::test]
async fn consumer_p8_selected_evidence_is_exact_and_retracts() {
    p8_run(false).await;
}
#[tokio::test]
async fn tenant_p8_selected_evidence_is_exact_and_retracts() {
    p8_run(true).await;
}

struct P8Blobs(Mutex<Vec<String>>);
#[async_trait::async_trait]
impl coralswarm_backend::services::blob_store::PromotionBlobStore for P8Blobs {
    async fn get_blob(
        &self,
        _: &str,
        _: &[u8; 32],
    ) -> Result<Vec<u8>, coralswarm_core::r2::R2Error> {
        panic!("purge must not download plaintext")
    }
    async fn put_blob(
        &self,
        _: &str,
        _: &[u8],
        _: &[u8; 32],
    ) -> Result<(), coralswarm_core::r2::R2Error> {
        panic!("purge must not create copies")
    }
    async fn delete_blob(&self, key: &str) -> Result<(), coralswarm_core::r2::R2Error> {
        self.0.lock().unwrap().push(key.into());
        Ok(())
    }
}
async fn p8_physical(org: bool) {
    use coralswarm_backend::{
        tenancy::TenantId,
        workers::{source_blob_cleanup, tenant_harness::WorkerTenant},
    };
    let Some(f) = Fixture::new(org).await else {
        return;
    };
    if let Some(ctx) = f.tenant.org_context() {
        sqlx::query("INSERT INTO tenant_meta(id,org_id) VALUES(true,$1) ON CONFLICT(id) DO UPDATE SET org_id=EXCLUDED.org_id").bind(&ctx.org_id).execute(&f.owner).await.unwrap();
    }
    let state = f.state(None);
    let prefix = f
        .tenant
        .org_context()
        .map(|c| format!("{}/", c.org_id))
        .unwrap_or_default();
    let key = format!("{prefix}{}/{}", f.graph, Uuid::new_v4());
    let a = source(
        &f.owner,
        f.graph,
        "p8-physical-a",
        "Acme PRIVATE source material",
        false,
    )
    .await;
    let b = source(
        &f.owner,
        f.graph,
        "p8-physical-b",
        "Acme PRIVATE source material",
        false,
    )
    .await;
    sqlx::query("UPDATE source_versions SET raw_blob_key=$2 WHERE version_id=ANY($1)")
        .bind(vec![a.1, b.1])
        .bind(&key)
        .execute(&f.owner)
        .await
        .unwrap();
    let atom:Uuid=sqlx::query_scalar("INSERT INTO atoms(graph_id,source_type,source_id,content_inline,title,content_blob_key,timestamp) VALUES($1,'fixture','p8-root','PRIVATE HEADER','PRIVATE TITLE',$2,now()) RETURNING atom_id").bind(f.graph).bind(&key).fetch_one(&f.owner).await.unwrap();
    sqlx::query("INSERT INTO source_version_chunks VALUES($1,$2,$3)")
        .bind(f.graph)
        .bind(a.1)
        .bind(atom)
        .execute(&f.owner)
        .await
        .unwrap();
    let target:Uuid=sqlx::query_scalar("INSERT INTO graphs(account_id,name,kind) VALUES($1,'P8 physical shared','org') RETURNING id").bind(f.user.account_id).fetch_one(&f.owner).await.unwrap();
    let copied:Uuid=sqlx::query_scalar("INSERT INTO atoms(graph_id,source_type,source_id,content_inline,title,content_blob_key,timestamp,promoted_from_atom_id) VALUES($1,'fixture','p8-copy','PRIVATE COPIED BODY','PRIVATE COPIED TITLE',$2,now(),$3) RETURNING atom_id").bind(target).bind(&key).bind(atom).fetch_one(&f.owner).await.unwrap();
    let node:Uuid=sqlx::query_scalar("INSERT INTO nodes(graph_id,type,title,body) VALUES($1,'entity','Acme','{\"aliases\":[\"PRIVATE ALIAS\"],\"digest\":\"PRIVATE GENERATED PROSE\",\"human_review_status\":\"confirmed\"}') RETURNING node_id").bind(f.graph).fetch_one(&f.owner).await.unwrap();
    let decision:Uuid=sqlx::query_scalar("INSERT INTO nodes(graph_id,type,title,body) VALUES($1,'decision','Acme','{\"decided_at\":\"PRIVATE UNSUPPORTED DATE\",\"human_review_status\":\"confirmed\"}') RETURNING node_id").bind(f.graph).fetch_one(&f.owner).await.unwrap();
    for n in [node, decision] {
        for semantic in [a.2, b.2] {
            sqlx::query("INSERT INTO node_semantic_atoms VALUES($1,$2,$3)")
                .bind(n)
                .bind(semantic)
                .bind(f.graph)
                .execute(&f.owner)
                .await
                .unwrap();
        }
    }
    let edge:Uuid=sqlx::query_scalar("INSERT INTO relationship_edge(graph_id,src_node_id,dst_node_id,predicate,origin,status,confidence,provenance) VALUES($1,$2,$3,'related_to','stated','confirmed',1,$4) RETURNING edge_id").bind(f.graph).bind(node).bind(decision).bind(json!({"semantic_atom_ids":[a.2],"rationale":"PRIVATE EDGE PROSE","human_review_status":"confirmed"})).fetch_one(&f.owner).await.unwrap();
    let mut request = f.request(None);
    request.style = Some("detailed".into());
    let answer = scoped_ask::execute(&state, &f.tenant, &f.user, request)
        .await
        .unwrap();
    assert!(answer.generated);
    let support: Value =
        sqlx::query_scalar("SELECT support FROM answer_support_receipts WHERE receipt_id=$1")
            .bind(answer.receipt_id)
            .fetch_one(&f.owner)
            .await
            .unwrap();
    assert!(
        support
            .as_array()
            .unwrap()
            .iter()
            .any(|s| s["source_item_id"] == json!(a.0)),
        "uncited private input must be recorded"
    );
    let (task_id, _, request) = coralswarm_backend::test_retained_mcp_claim(
        &state,
        &f.tenant,
        &f.user,
        f.request(None),
        None,
    )
    .await
    .unwrap();
    assert!(
        coralswarm_backend::test_retained_mcp_run(
            state.clone(),
            f.tenant.clone(),
            f.user.clone(),
            task_id,
            request
        )
        .await
    );
    // Source-only task retains evidence text without a model; physical purge
    // removes its entire JSON, including sources the model never cited.
    let task_result: Option<Value> = sqlx::query_scalar("SELECT result FROM mcp_tasks WHERE id=$1")
        .bind(task_id)
        .fetch_one(&f.owner)
        .await
        .unwrap();
    assert!(task_result.is_some());
    let mut tx = f
        .tenant
        .begin_member_scoped(f.user.account_id)
        .await
        .unwrap();
    sqlx::query("DELETE FROM source_items WHERE source_item_id=$1")
        .bind(a.0)
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let message: (String, Value) =
        sqlx::query_as("SELECT text,timing FROM conversation_messages WHERE id=$1")
            .bind(Uuid::parse_str(answer.payload["message_id"].as_str().unwrap()).unwrap())
            .fetch_one(&f.owner)
            .await
            .unwrap();
    assert_eq!(message.0, "[Evidence no longer available]");
    assert_eq!(message.1, json!({}));
    let task_result: Option<Value> = sqlx::query_scalar("SELECT result FROM mcp_tasks WHERE id=$1")
        .bind(task_id)
        .fetch_one(&f.owner)
        .await
        .unwrap();
    assert!(task_result.is_none());
    let worker = WorkerTenant {
        tenant: if org {
            TenantId::Org(f.tenant.org_context().unwrap().org_id.clone())
        } else {
            TenantId::Consumer
        },
        pool: f.app.clone(),
        store: f.tenant.store.clone().unwrap(),
    };
    let blobs = P8Blobs(Mutex::new(vec![]));
    for _ in 0..4 {
        source_blob_cleanup::tick(&worker, Some(&blobs))
            .await
            .unwrap();
    }
    assert!(
        blobs.0.lock().unwrap().is_empty(),
        "independent canonical reference must retain object"
    );
    let atoms: i64 = sqlx::query_scalar("SELECT count(*) FROM atoms WHERE atom_id=ANY($1)")
        .bind(vec![atom, copied])
        .fetch_one(&f.owner)
        .await
        .unwrap();
    assert_eq!(
        atoms, 0,
        "physical headers/body/title must disappear, not just mask"
    );
    for n in [node, decision] {
        let body: Value = sqlx::query_scalar("SELECT body FROM nodes WHERE node_id=$1")
            .bind(n)
            .fetch_one(&f.owner)
            .await
            .unwrap();
        assert_eq!(
            body,
            json!({"human_review_status":"confirmed","lifecycle":"evidence_retracted"})
        );
    }
    let provenance: Value =
        sqlx::query_scalar("SELECT provenance FROM relationship_edge WHERE edge_id=$1")
            .bind(edge)
            .fetch_one(&f.owner)
            .await
            .unwrap();
    assert!(!provenance.to_string().contains("PRIVATE"));
    let surviving: i64 =
        sqlx::query_scalar("SELECT count(*) FROM semantic_atoms WHERE semantic_atom_id=$1")
            .bind(b.2)
            .fetch_one(&f.owner)
            .await
            .unwrap();
    assert_eq!(surviving, 1, "allowed independent fact remains");
    // Delete the graph through the actual application login. The durable queue
    // must still identify/decrypt nothing and delete ciphertext after no source,
    // graph or version row remains for a bootstrap lookup.
    let mut tx = f
        .tenant
        .begin_member_scoped(f.user.account_id)
        .await
        .unwrap();
    sqlx::query("DELETE FROM graphs WHERE id=$1")
        .bind(f.graph)
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    for _ in 0..4 {
        source_blob_cleanup::tick(&worker, Some(&blobs))
            .await
            .unwrap();
    }
    assert_eq!(
        blobs
            .0
            .lock()
            .unwrap()
            .iter()
            .filter(|k| *k == &key)
            .count(),
        1,
        "fake object deleted after parent graph disappearance"
    );
    let event: i64 =
        sqlx::query_scalar("SELECT count(*) FROM source_purge_events WHERE graph_id=$1")
            .bind(f.graph)
            .fetch_one(&f.owner)
            .await
            .unwrap();
    assert!(event > 0);
    assert_eq!(
        f.calls.load(Ordering::SeqCst),
        1,
        "only explicit manual mock Ask may call the model"
    );
    let before = blobs.0.lock().unwrap().len();
    source_blob_cleanup::tick(&worker, Some(&blobs))
        .await
        .unwrap();
    assert_eq!(
        blobs.0.lock().unwrap().len(),
        before,
        "idempotent convergence"
    );
    f.done().await;
}
#[tokio::test]
async fn consumer_p8_purge_removes_physical_copies_after_parent_disappears() {
    p8_physical(false).await;
}
#[tokio::test]
async fn tenant_p8_purge_removes_physical_copies_after_parent_disappears() {
    p8_physical(true).await;
}

// P12 known-new PUT cleanup uses the real native funnel and encrypted localhost
// S3 transport. SQL login roles remain NOBYPASS, with a one-connection content
// pool: cleanup must acquire its service executor only AFTER capture rollback.
async fn p8_known_upload_cleanup(org:bool) {
    use axum::{http::{Method,StatusCode},extract::OriginalUri,Router};
    use coralswarm_backend::{capture_upload_cleanup,tenancy::{scope,TenantId},workers::{source_blob_cleanup,tenant_harness::WorkerTenant}};
    use coralswarm_core::r2::R2Client;
    use std::{collections::HashSet,sync::atomic::AtomicBool};
    let Some(mut f)=Fixture::new(org).await else{return};
    if org {sqlx::query("INSERT INTO tenant_meta(id,org_id) VALUES(true,$1) ON CONFLICT(id) DO UPDATE SET org_id=EXCLUDED.org_id").bind(&f.tenant.org_context().unwrap().org_id).execute(&f.owner).await.unwrap();}
    let small=PgPoolOptions::new().max_connections(1).acquire_timeout(std::time::Duration::from_secs(2)).connect_with((*f.app.connect_options()).clone()).await.unwrap();
    f.app=small.clone();f.tenant.pool=Some(small.clone());f.tenant.store=Some(Arc::new(PostgresStore::new(small.clone())));
    if let Some(TenancyDecision::Org(ctx))=&mut f.tenant.decision {ctx.pool=small.clone();}else {f.control_app=small.clone();}
    let fail_delete=Arc::new(AtomicBool::new(true));let puts=Arc::new(AtomicUsize::new(0));let objects=Arc::new(Mutex::new(HashSet::<String>::new()));
    let fd=fail_delete.clone();let pc=puts.clone();let ob=objects.clone();let db=f.control_owner.clone();let account=f.user.account_id;let user=f.user.user_id;
    let fake=Router::new().fallback(move |method:Method,OriginalUri(uri):OriginalUri| {let fd=fd.clone();let pc=pc.clone();let ob=ob.clone();let db=db.clone();async move {
        let key=uri.path().strip_prefix("/fixture-bucket/").unwrap().to_string();
        match method {
            Method::PUT=>{ob.lock().unwrap().insert(key);if pc.fetch_add(1,Ordering::SeqCst)==0 {sqlx::query("DELETE FROM account_members WHERE account_id=$1 AND user_id=$2").bind(account).bind(user).execute(&db).await.unwrap();}StatusCode::OK},
            Method::DELETE if fd.load(Ordering::SeqCst)=>StatusCode::FORBIDDEN,
            Method::DELETE=>{ob.lock().unwrap().remove(&key);StatusCode::NO_CONTENT},
            _=>StatusCode::NOT_FOUND,
        }
    }});
    let listener=tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();let endpoint=format!("http://{}",listener.local_addr().unwrap());let server=tokio::spawn(async move {axum::serve(listener,fake).await.unwrap()});
    let plane=if org {TenantId::Org(f.tenant.org_context().unwrap().org_id.clone())}else {TenantId::Consumer};
    let mut state=coralswarm_backend::test_app_state_owned_with_db_and_store(f.control_app.clone(),None);
    state.dek_cache.insert(&plane,f.user.account_id,[7;32]);
    state.r2=Some(Arc::new(R2Client::new_with_endpoint_override("fixture","fixture-access","fixture-secret","fixture-bucket",&endpoint).await));
    let state=Arc::new(state);
    let response=tokio::time::timeout(std::time::Duration::from_secs(20),coralswarm_backend::test_ingest_with_source_context(&state,&f.tenant,&f.user,serde_json::from_value(json!({"graph_id":f.graph,"source_type":"note","source_id":"p8-known-rollback","content":"Synthetic capture must disappear after revoked PUT.","timestamp":"2009-01-01T00:00:00Z","participants":[]})).unwrap(),"fake:p8-known-upload")).await.expect("one-connection capture must release TX before retry persistence");
    let status=response.status();let body:Value=serde_json::from_slice(&axum::body::to_bytes(response.into_body(),1024*1024).await.unwrap()).unwrap();assert_eq!(status,StatusCode::FORBIDDEN,"{body}");assert_eq!(body["cleanup_status"],"queued");
    assert!(puts.load(Ordering::SeqCst)>=2);let uploaded=objects.lock().unwrap().len();assert!(uploaded>=2);
    let receipts:Vec<(Uuid,bool,String)>=sqlx::query_as("SELECT cleanup_id,abort_verified,state FROM unattached_capture_uploads WHERE graph_id=$1").bind(f.graph).fetch_all(&f.owner).await.unwrap();assert_eq!(receipts.len(),uploaded);assert!(receipts.iter().all(|r|r.1 && r.2=="pending"));
    if org {
        // Tenant plane uses the established system-actor policy, never the
        // consumer account GUC that every org member shares.
        let qual:String=sqlx::query_scalar("SELECT qual FROM pg_policies WHERE tablename='unattached_capture_uploads'").fetch_one(&f.owner).await.unwrap();
        assert!(qual.contains("tenant_rls_is_system") && !qual.contains("app.account_id"),"{qual}");
        let mut member=scope::begin_tenant_scoped(&f.app,f.user.account_id,f.user.user_id).await.unwrap();
        assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM unattached_capture_uploads").fetch_one(&mut *member).await.unwrap(),0);
        assert!(!sqlx::query_scalar::<_,bool>("SELECT library_stage_unattached_upload($1)").bind(receipts[0].0).fetch_one(&mut *member).await.unwrap());
        member.rollback().await.unwrap();
        let mut member=scope::begin_tenant_scoped(&f.app,f.user.account_id,f.user.user_id).await.unwrap();
        let denied=sqlx::query_scalar::<_,Uuid>("SELECT library_record_unattached_upload($1,$2,$3,$4,$5,current_database(),'1')").bind(f.user.account_id).bind(f.graph).bind(Uuid::nil()).bind(f.tenant.org_context().map(|c|c.org_id.as_str())).bind(format!("{}/{}/{}",f.tenant.org_context().unwrap().org_id,f.graph,Uuid::nil())).fetch_one(&mut *member).await;
        assert!(denied.unwrap_err().to_string().contains("tenant service scope"));
        member.rollback().await.unwrap();
    }
    assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM source_items WHERE graph_id=$1 AND source_id='p8-known-rollback'").bind(f.graph).fetch_one(&f.owner).await.unwrap(),0);
    assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM atoms WHERE graph_id=$1 AND source_id LIKE 'p8-known-rollback:%'").bind(f.graph).fetch_one(&f.owner).await.unwrap(),0);
    // No PUBLIC execute privilege for the private outcome/reference authority.
    let public_execute:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_proc p,LATERAL aclexplode(COALESCE(p.proacl,acldefault('f',p.proowner))) a WHERE p.proname IN('library_record_unattached_upload','library_stage_unattached_upload','library_finish_unattached_upload') AND a.grantee=0 AND a.privilege_type='EXECUTE')").fetch_one(&f.app).await.unwrap();assert!(!public_execute);
    // Parent/source can disappear and ordinary actor admission remain revoked.
    sqlx::query("DELETE FROM graphs WHERE id=$1").bind(f.graph).execute(&f.owner).await.unwrap();
    fail_delete.store(false,Ordering::SeqCst);
    let ids:Vec<_>=receipts.iter().map(|r|r.0).collect();
    let transport=state.r2.as_deref().map(|r|r as &dyn coralswarm_backend::services::blob_store::PromotionBlobStore);
    assert_eq!(capture_upload_cleanup::drain_account(&f.app,&plane,f.user.account_id,transport,Some(&ids)).await.unwrap(),uploaded);
    assert!(objects.lock().unwrap().is_empty());
    assert_eq!(capture_upload_cleanup::drain_account(&f.app,&plane,f.user.account_id,transport,Some(&ids)).await.unwrap(),0);
    // A surviving canonical object in another private graph is still retained,
    // even though the ordinary reader cannot see it. Aborted alone is insufficient.
    let key=coralswarm_core::r2::blob_key_for(f.tenant.org_context().map(|c|c.org_id.as_str()),f.denied_graph,Uuid::new_v4()).unwrap();
    let mut aborted=f.owner.begin().await.unwrap();let xid:String=sqlx::query_scalar("SELECT pg_current_xact_id()::text").fetch_one(&mut *aborted).await.unwrap();aborted.rollback().await.unwrap();
    sqlx::query("UPDATE source_versions SET raw_blob_key=$2 WHERE graph_id=$1 AND version_id=$3").bind(f.denied_graph).bind(&key).bind(source(&f.owner,f.denied_graph,"cleanup-independent-ref","Retained private original",false).await.1).execute(&f.owner).await.unwrap();
    async fn record(f:&Fixture,org:bool,graph:Uuid,key:&str,xid:&str)->Result<Uuid,sqlx::Error> {
        let mut tx=if org {scope::begin_tenant_system_scoped(&f.app,f.user.account_id).await.unwrap()}else {scope::begin_account_scoped(&f.app,f.user.account_id).await.unwrap()};
        let db:String=sqlx::query_scalar("SELECT current_database()::text").fetch_one(&mut *tx).await.unwrap();
        let result=sqlx::query_scalar("SELECT library_record_unattached_upload($1,$2,$3,$4,$5,$6,$7)").bind(f.user.account_id).bind(graph).bind(Uuid::parse_str(key.rsplit('/').next().unwrap()).unwrap()).bind(f.tenant.org_context().map(|c|c.org_id.as_str())).bind(key).bind(db).bind(xid).fetch_one(&mut *tx).await;
        if result.is_ok(){tx.commit().await.unwrap();}else{tx.rollback().await.unwrap();}result
    }
    let retained=record(&f,org,f.denied_graph,&key,&xid).await.unwrap();
    let untouched=P8Blobs(Mutex::new(vec![]));
    assert_eq!(capture_upload_cleanup::drain_account(&f.app,&plane,f.user.account_id,Some(&untouched),Some(&[retained])).await.unwrap(),0);
    assert!(untouched.0.lock().unwrap().is_empty());
    // In-progress/committed transactions are not cleanup authorization even if
    // no attachment exists. Outcome comes from this exact plane, never caller JSON.
    let mut uncertain=f.owner.begin().await.unwrap();let unknown_xid:String=sqlx::query_scalar("SELECT pg_current_xact_id()::text").fetch_one(&mut *uncertain).await.unwrap();
    let unknown_key=coralswarm_core::r2::blob_key_for(f.tenant.org_context().map(|c|c.org_id.as_str()),f.denied_graph,Uuid::new_v4()).unwrap();
    let unknown=record(&f,org,f.denied_graph,&unknown_key,&unknown_xid).await.unwrap();
    assert_eq!(capture_upload_cleanup::drain_account(&f.app,&plane,f.user.account_id,Some(&untouched),Some(&[unknown])).await.unwrap(),0);
    uncertain.commit().await.unwrap();
    assert_eq!(capture_upload_cleanup::drain_account(&f.app,&plane,f.user.account_id,Some(&untouched),Some(&[unknown])).await.unwrap(),0);
    assert_eq!(sqlx::query_scalar::<_,String>("SELECT state FROM unattached_capture_uploads WHERE cleanup_id=$1").bind(unknown).fetch_one(&f.owner).await.unwrap(),"committed");
    let foreign=coralswarm_core::r2::blob_key_for(f.tenant.org_context().map(|c|c.org_id.as_str()),Uuid::new_v4(),Uuid::new_v4()).unwrap();assert!(record(&f,org,f.denied_graph,&foreign,&xid).await.is_err());
    let worker=WorkerTenant {tenant:plane,pool:f.app.clone(),store:f.tenant.store.clone().unwrap()};
    source_blob_cleanup::tick(&worker,Some(&untouched)).await.unwrap();
    server.abort();f.done().await;
}
#[tokio::test]
async fn consumer_p8_known_rollback_upload_cleanup_survives_graph_loss(){p8_known_upload_cleanup(false).await;}
#[tokio::test]
async fn tenant_p8_known_rollback_upload_cleanup_survives_graph_loss(){p8_known_upload_cleanup(true).await;}

// Review fix: a per-source purge must not wipe unrelated graph state (queued
// work for other sources, other meetings' summaries), and a team removal is a
// specific user's permission loss on BOTH planes — the tenant plane has no
// account_members, so team_members is the trigger that reaches it.
async fn p8_scoped_side_effects(org: bool) {
    let Some(f) = Fixture::new(org).await else { return };
    let acc = f.user.account_id;
    let removed = f.user.user_id;
    let keeper: Uuid = if org {
        Uuid::new_v4()
    } else {
        sqlx::query_scalar("INSERT INTO users(email) VALUES($1) RETURNING id").bind(format!("p8-keeper-{}@test.local", Uuid::new_v4())).fetch_one(&f.owner).await.unwrap()
    };
    let team_graph: Uuid = sqlx::query_scalar("INSERT INTO graphs(account_id,name,kind) VALUES($1,'P8 synthetic team','team') RETURNING id").bind(acc).fetch_one(&f.owner).await.unwrap();
    let team: Uuid = sqlx::query_scalar("INSERT INTO teams(account_id,graph_id,name,created_by) VALUES($1,$2,'P8 team',$3) RETURNING id").bind(acc).bind(team_graph).bind(removed).fetch_one(&f.owner).await.unwrap();
    for u in [removed, keeper] {
        sqlx::query("INSERT INTO team_members(team_id,user_id,role) VALUES($1,$2,'member')").bind(team).bind(u).execute(&f.owner).await.unwrap();
    }
    let mut convs = vec![];
    for u in [removed, keeper] {
        let c: Uuid = sqlx::query_scalar("INSERT INTO conversations(account_id,graph_id,user_id,title) VALUES($1,$2,$3,'synthetic') RETURNING id").bind(acc).bind(team_graph).bind(u).fetch_one(&f.owner).await.unwrap();
        sqlx::query("INSERT INTO conversation_messages(conversation_id,seq,role,text) VALUES($1,1,'assistant','SYNTHETIC TEAM ANSWER')").bind(c).execute(&f.owner).await.unwrap();
        convs.push(c);
    }
    sqlx::query("DELETE FROM team_members WHERE team_id=$1 AND user_id=$2").bind(team).bind(removed).execute(&f.owner).await.unwrap();
    let text = |c: Uuid| { let o = f.owner.clone(); async move { sqlx::query_scalar::<_, String>("SELECT text FROM conversation_messages WHERE conversation_id=$1").bind(c).fetch_one(&o).await.unwrap() } };
    assert_eq!(text(convs[0]).await, "[Access no longer available]");
    assert_eq!(text(convs[1]).await, "SYNTHETIC TEAM ANSWER");

    let mut items = vec![];
    for sid in ["p8-scope-a", "p8-scope-b"] {
        let item: Uuid = sqlx::query_scalar("INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id) VALUES($1,'note','synthetic',$2) RETURNING source_item_id").bind(team_graph).bind(sid).fetch_one(&f.owner).await.unwrap();
        sqlx::query("INSERT INTO jobs(kind,payload,account_id) VALUES('ExtractSourceAtoms',jsonb_build_object('graph_id',$1::uuid,'source_item_id',$2::uuid,'content','SYNTHETIC '||$3),$4)").bind(team_graph).bind(item).bind(sid).bind(acc).execute(&f.owner).await.unwrap();
        items.push(item);
    }
    sqlx::query("DELETE FROM source_items WHERE graph_id=$1 AND source_item_id=$2").bind(team_graph).bind(items[0]).execute(&f.owner).await.unwrap();
    let job = |item: Uuid| { let o = f.owner.clone(); async move { sqlx::query_as::<_, (String, Value)>("SELECT status::text,payload FROM jobs WHERE kind='ExtractSourceAtoms' AND payload->>'graph_id'=$1 AND payload->>'source_item_id'=$2").bind(team_graph.to_string()).bind(item.to_string()).fetch_one(&o).await.unwrap() } };
    let (purged_status, purged_payload) = sqlx::query_as::<_, (String, Value)>("SELECT status::text,payload FROM jobs WHERE kind='ExtractSourceAtoms' AND payload->>'graph_id'=$1 AND payload ? 'evidence_retracted'").bind(team_graph.to_string()).fetch_one(&f.owner).await.unwrap();
    assert_eq!(purged_status, "dead");
    assert!(!purged_payload.to_string().contains("SYNTHETIC"), "{purged_payload}");
    let (kept_status, kept_payload) = job(items[1]).await;
    assert_eq!(kept_status, "pending", "unrelated queued work must survive a per-source purge");
    assert_eq!(kept_payload["content"], "SYNTHETIC p8-scope-b");
    f.done().await;
}
#[tokio::test]
async fn consumer_p8_purge_side_effects_are_source_scoped_and_team_loss_clears_caches() { p8_scoped_side_effects(false).await; }
#[tokio::test]
async fn tenant_p8_purge_side_effects_are_source_scoped_and_team_loss_clears_caches() { p8_scoped_side_effects(true).await; }


static LEDGER_ENV: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

struct LedgerEnv(Vec<&'static str>);
impl LedgerEnv {
    fn set(vars: &[(&'static str, &str)]) -> Self {
        let mut keys = vec!["LLM_RETRY_BASE_MS", "LLM_RETRY_MAX_DELAY_MS"];
        std::env::set_var("LLM_RETRY_BASE_MS", "1");
        std::env::set_var("LLM_RETRY_MAX_DELAY_MS", "5");
        for (key, value) in vars {
            std::env::set_var(key, value);
            keys.push(key);
        }
        Self(keys)
    }
}
impl Drop for LedgerEnv {
    fn drop(&mut self) {
        for key in &self.0 {
            std::env::remove_var(key);
        }
    }
}

struct LlmMock {
    url: String,
    hits: Arc<AtomicUsize>,
    bodies: Arc<Mutex<Vec<Value>>>,
}

async fn llm_mock<F, Fut>(reply: F) -> LlmMock
where
    F: Fn(usize) -> Fut + Clone + Send + Sync + 'static,
    Fut: std::future::Future<Output = (u16, Value)> + Send + 'static,
{
    use axum::response::IntoResponse;
    let hits = Arc::new(AtomicUsize::new(0));
    let bodies = Arc::new(Mutex::new(Vec::new()));
    let (counter, seen) = (hits.clone(), bodies.clone());
    let app = axum::Router::new().fallback(move |axum::Json(body): axum::Json<Value>| {
        let (counter, seen, reply) = (counter.clone(), seen.clone(), reply.clone());
        async move {
            seen.lock().unwrap().push(body);
            let n = counter.fetch_add(1, Ordering::SeqCst);
            let (status, body) = reply(n).await;
            (axum::http::StatusCode::from_u16(status).unwrap(), axum::Json(body)).into_response()
        }
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await.ok() });
    LlmMock { url, hits, bodies }
}

fn mock_llm(url: &str, timeout_ms: Option<u64>) -> coralswarm_backend::llm::openai::OpenAiProvider {
    let mut client = reqwest::Client::builder();
    if let Some(ms) = timeout_ms {
        client = client.timeout(std::time::Duration::from_millis(ms));
    }
    coralswarm_backend::llm::openai::OpenAiProvider::openrouter(client.build().unwrap(), "test-key")
        .with_base_url(url.to_string())
}

fn served(model: &str, cost: f64) -> Value {
    let atoms = json!({"atoms":[{"kind":"fact","text":"Retained evidence.","evidence_quote":"Retained evidence.","entities":[{"mention":"Retained evidence","type":"topic"}]}]});
    json!({
        "id": "gen-ledger",
        "model": model,
        "provider": "MockHost",
        "choices": [{"message": {"content": atoms.to_string()}, "finish_reason": "stop"}],
        "usage": {"prompt_tokens": 900, "completion_tokens": 60, "cost": cost}
    })
}

const PRIMARY: &str = "deepseek/deepseek-v4-flash-0731";

struct Ledger {
    f: Fixture,
    versions: Vec<Uuid>,
    run: Uuid,
}

async fn ledger(org: bool, sources: usize) -> Option<Ledger> {
    let f = Fixture::new(org).await?;
    f.history_contract().await;
    f.finite_pool(1_000_000).await;
    let mut versions = Vec::new();
    for n in 0..sources {
        versions.push(f.retained(&format!("ledger-{n}")).await.1);
    }
    let run = f.managed_run(50, 100).await;
    for _ in 0..5 {
        f.history_advance(run).await;
    }
    Some(Ledger { f, versions, run })
}

impl Ledger {
    async fn window(&self, i: usize, llm: &dyn LlmProvider) {
        let tid = coralswarm_backend::tenancy::TenantId::from_decision(self.f.tenant.decision.as_ref().unwrap());
        let job = self.f.history_job(self.versions[i]).await;
        coralswarm_backend::test_p10_source_worker(&tid, &self.f.app, &self.f.control_app, llm, job).await;
    }
    async fn receipts(&self) -> Vec<(String, i64, Option<i64>)> {
        sqlx::query_as("SELECT state,reserved_micro_usd,settled_micro_usd FROM managed_attempt_receipts ORDER BY created_at,attempt_no")
            .fetch_all(&self.f.owner).await.unwrap()
    }
    async fn today(&self) -> Option<(i64, i64, i64, i64)> {
        sqlx::query_as("SELECT limit_micro_usd,reserved_micro_usd,charged_micro_usd,attempts FROM managed_budget_periods WHERE account_id=$1 AND purpose='source_extraction' AND period_start=(now() AT TIME ZONE 'utc')::date")
            .bind(self.f.user.account_id).fetch_optional(&self.f.owner).await.unwrap()
    }
    async fn run_state(&self) -> (String, Option<String>) {
        sqlx::query_as("SELECT status,pause_reason FROM corpus_enrichment_runs WHERE run_id=$1")
            .bind(self.run).fetch_one(&self.f.owner).await.unwrap()
    }
    async fn unit_charged(&self, i: usize) -> i64 {
        sqlx::query_scalar("SELECT charged_micro_usd FROM corpus_enrichment_units WHERE run_id=$1 AND COALESCE(promoted_version_id,holding_id)=$2 AND execution_owner")
            .bind(self.run).bind(self.versions[i]).fetch_one(&self.f.owner).await.unwrap()
    }
    async fn version_state(&self, i: usize) -> (String, Option<String>) {
        sqlx::query_as("SELECT extraction_status,extraction_error FROM source_versions WHERE version_id=$1")
            .bind(self.versions[i]).fetch_one(&self.f.owner).await.unwrap()
    }
    async fn job_status(&self, i: usize) -> String {
        sqlx::query_scalar("SELECT status::text FROM jobs WHERE payload->>'source_version_id'=$1 ORDER BY created_at DESC LIMIT 1")
            .bind(self.versions[i].to_string()).fetch_one(&self.f.owner).await.unwrap()
    }
    async fn atoms(&self, i: usize) -> i64 {
        sqlx::query_scalar("SELECT count(*) FROM semantic_atoms WHERE source_version_id=$1")
            .bind(self.versions[i]).fetch_one(&self.f.owner).await.unwrap()
    }
}

async fn ledger_reserves_before_send(org: bool) {
    let _lock = LEDGER_ENV.lock().await;
    let _env = LedgerEnv::set(&[("LLM_MAX_RETRIES", "4")]);
    let Some(l) = ledger(org, 1).await else { return };
    let owner = l.f.owner.clone();
    let reserved_at_send = Arc::new(Mutex::new(Vec::new()));
    let observed = reserved_at_send.clone();
    let mock = llm_mock(move |_| {
        let (owner, observed) = (owner.clone(), observed.clone());
        async move {
            let open: i64 = sqlx::query_scalar("SELECT count(*) FROM managed_attempt_receipts WHERE state='reserved'")
                .fetch_one(&owner).await.unwrap();
            observed.lock().unwrap().push(open);
            (200, served(PRIMARY, 0.0004))
        }
    })
    .await;
    l.window(0, &mock_llm(&mock.url, None)).await;
    assert_eq!(mock.hits.load(Ordering::SeqCst), 1);
    assert_eq!(*reserved_at_send.lock().unwrap(), vec![1], "the receipt committed before the send");
    let receipts = l.receipts().await;
    assert_eq!(receipts.len(), 1);
    assert_eq!((receipts[0].0.as_str(), receipts[0].2), ("billed", Some(400)));
    let (_, reserved, charged, attempts) = l.today().await.unwrap();
    assert_eq!((reserved, charged, attempts), (0, 400, 1));
    assert_eq!(l.unit_charged(0).await, 400);
    assert_eq!(l.version_state(0).await.0, "done");
    assert_eq!(l.atoms(0).await, 1);
    l.f.done().await;
}
#[tokio::test]
async fn p10_ledger_reserves_before_send_consumer() {
    ledger_reserves_before_send(false).await;
}
#[tokio::test]
async fn p10_ledger_reserves_before_send_tenant() {
    ledger_reserves_before_send(true).await;
}

#[tokio::test]
async fn p10_ledger_timeout_forever_bounded_by_limit() {
    let _lock = LEDGER_ENV.lock().await;
    let _env = LedgerEnv::set(&[("LLM_MAX_RETRIES", "1")]);
    let Some(l) = ledger(false, 1).await else { return };
    let probe = llm_mock(|_| async { (429, json!({"error": {"message": "busy", "code": 429}})) }).await;
    l.window(0, &mock_llm(&probe.url, None)).await;
    let w = l.receipts().await[0].1;
    assert_eq!(l.today().await.unwrap().0, 10_000_000, "the day opened at the default cap");
    let limit = w + w / 2;
    sqlx::query("UPDATE managed_budget_periods SET limit_micro_usd=$2 WHERE account_id=$1 AND purpose='source_extraction' AND period_start=(now() AT TIME ZONE 'utc')::date")
        .bind(l.f.user.account_id).bind(limit).execute(&l.f.owner).await.unwrap();
    let cap = format!("{:.6}", limit as f64 / 1_000_000.0);
    let _cap = LedgerEnv::set(&[("MANAGED_DAILY_CAP_USD_EXTRACTION", cap.as_str())]);
    std::env::set_var("LLM_MAX_RETRIES", "4");
    let silent = llm_mock(|_| async {
        tokio::time::sleep(std::time::Duration::from_secs(30)).await;
        (200, json!({}))
    })
    .await;
    // v2 (the default profile) fails a timed-out window to the queue's
    // bounded retry rather than retrying it in place, so each retry is its
    // own window call. The day still closes after floor(limit / w) charges:
    // one here, and the retry is held before the model.
    for _ in 0..5 {
        if l.job_status(0).await != "pending" { break; }
        l.window(0, &mock_llm(&silent.url, Some(50))).await;
    }
    assert_eq!(silent.hits.load(Ordering::SeqCst), 1, "floor(limit / w) attempts, then the day is closed");
    let ambiguous: Vec<_> = l.receipts().await.into_iter().filter(|r| r.0 == "ambiguous").collect();
    assert_eq!(ambiguous.len(), 1);
    assert!(ambiguous.iter().all(|r| r.2 == Some(w)), "a timeout is charged in full");
    let (limit, reserved, charged, _) = l.today().await.unwrap();
    assert_eq!((reserved, charged), (0, w));
    assert!(charged <= limit);
    assert_eq!(l.run_state().await, ("paused".into(), Some("daily_budget_exhausted".into())));
    assert_eq!(l.job_status(0).await, "done", "a held window completes NothingToDo");
    l.f.done().await;
}

#[tokio::test]
async fn p10_ledger_429_forever_charges_zero() {
    let _lock = LEDGER_ENV.lock().await;
    let _env = LedgerEnv::set(&[("LLM_MAX_RETRIES", "4")]);
    let Some(l) = ledger(false, 1).await else { return };
    let mock = llm_mock(|_| async { (429, json!({"error": {"message": "busy", "code": 429}})) }).await;
    l.window(0, &mock_llm(&mock.url, None)).await;
    assert_eq!(mock.hits.load(Ordering::SeqCst), 4);
    let receipts = l.receipts().await;
    assert_eq!(receipts.len(), 4);
    assert!(receipts.iter().all(|r| r.0 == "not_generated" && r.2 == Some(0)));
    let (_, reserved, charged, attempts) = l.today().await.unwrap();
    assert_eq!((reserved, charged, attempts), (0, 0, 4));
    assert_eq!(l.unit_charged(0).await, 0);
    l.f.done().await;
}

#[tokio::test]
async fn p10_ledger_billed_fallback_settles_at_reported_cost() {
    let _lock = LEDGER_ENV.lock().await;
    let _env = LedgerEnv::set(&[("LLM_MAX_RETRIES", "4")]);
    let Some(l) = ledger(false, 1).await else { return };
    // Source extraction (v2, the default profile) runs its own fallback
    // with reasoning off; the global GLM fallback was v1's.
    let fallback = coralswarm_backend::llm::model_config::source_extraction_fallback_model();
    let fb = fallback.clone();
    let mock = llm_mock(move |_| { let fb = fb.clone(); async move { (200, served(&fb, 0.0007)) } }).await;
    l.window(0, &mock_llm(&mock.url, None)).await;
    let receipts = l.receipts().await;
    assert_eq!(receipts.len(), 1);
    assert_eq!((receipts[0].0.as_str(), receipts[0].2), ("billed", Some(700)));
    let served_model: String = sqlx::query_scalar("SELECT served_model FROM managed_attempt_receipts")
        .fetch_one(&l.f.owner).await.unwrap();
    assert_eq!(served_model, fallback);
    assert_eq!(l.unit_charged(0).await, 700, "the unit moves from w to the reported cost");
    assert_eq!(l.today().await.unwrap().2, 700);
    let body = mock.bodies.lock().unwrap()[0].clone();
    assert_eq!(body["model"], PRIMARY);
    assert_eq!(body["models"], json!([fallback]));
    assert_eq!(body["provider"]["max_price"], json!({"prompt": 0.22, "completion": 0.68, "request": 0}), "priced for the dearer of primary and fallback");
    assert_eq!(body["reasoning"], json!({"enabled": false}), "source extraction runs with reasoning off");
    l.f.done().await;
}

#[tokio::test]
async fn p10_ledger_overrun_charged_and_reserving_continues() {
    let _lock = LEDGER_ENV.lock().await;
    let _env = LedgerEnv::set(&[("LLM_MAX_RETRIES", "4")]);
    let Some(l) = ledger(false, 2).await else { return };
    let mock = llm_mock(|n| async move { (200, served(PRIMARY, if n == 0 { 1.0 } else { 0.0004 })) }).await;
    l.window(0, &mock_llm(&mock.url, None)).await;
    l.window(1, &mock_llm(&mock.url, None)).await;
    assert_eq!(mock.hits.load(Ordering::SeqCst), 2, "an overrun does not close the day");
    let receipts = l.receipts().await;
    assert_eq!(
        receipts.iter().map(|r| (r.0.as_str(), r.2)).collect::<Vec<_>>(),
        vec![("overrun", Some(1_000_000)), ("billed", Some(400))]
    );
    let (_, reserved, charged, _) = l.today().await.unwrap();
    assert_eq!((reserved, charged), (0, 1_000_400));
    l.f.done().await;
}

#[tokio::test]
async fn p10_ledger_daily_cap_rolls_over_utc() {
    let _lock = LEDGER_ENV.lock().await;
    let _env = LedgerEnv::set(&[("LLM_MAX_RETRIES", "4")]);
    let Some(l) = ledger(false, 2).await else { return };
    sqlx::query("INSERT INTO managed_budget_periods(account_id,purpose,period_start,limit_micro_usd,charged_micro_usd) VALUES($1,'source_extraction',(now() AT TIME ZONE 'utc')::date-1,1000,1000)")
        .bind(l.f.user.account_id).execute(&l.f.owner).await.unwrap();
    let mock = llm_mock(|_| async { (200, served(PRIMARY, 0.0004)) }).await;
    l.window(0, &mock_llm(&mock.url, None)).await;
    assert_eq!(mock.hits.load(Ordering::SeqCst), 1, "yesterday's exhausted day does not hold today");
    sqlx::query("UPDATE managed_budget_periods SET charged_micro_usd=limit_micro_usd WHERE account_id=$1 AND period_start=(now() AT TIME ZONE 'utc')::date")
        .bind(l.f.user.account_id).execute(&l.f.owner).await.unwrap();
    l.window(1, &mock_llm(&mock.url, None)).await;
    assert_eq!(mock.hits.load(Ordering::SeqCst), 1, "today's exhausted day holds");
    assert_eq!(l.run_state().await, ("paused".into(), Some("daily_budget_exhausted".into())));
    l.f.done().await;
}

#[tokio::test]
async fn p10_ledger_version_cap_refuses() {
    let _lock = LEDGER_ENV.lock().await;
    let _env = LedgerEnv::set(&[("LLM_MAX_RETRIES", "4")]);
    let Some(l) = ledger(false, 1).await else { return };
    sqlx::query("UPDATE corpus_enrichment_units SET charged_micro_usd=249999 WHERE run_id=$1 AND holding_id=$2")
        .bind(l.run).bind(l.versions[0]).execute(&l.f.owner).await.unwrap();
    let mock = llm_mock(|_| async { (200, served(PRIMARY, 0.0004)) }).await;
    l.window(0, &mock_llm(&mock.url, None)).await;
    assert_eq!(mock.hits.load(Ordering::SeqCst), 0);
    assert!(l.receipts().await.is_empty());
    assert_eq!(l.unit_charged(0).await, 249_999);
    assert_eq!(l.version_state(0).await, ("failed".into(), Some("version_cost_cap".into())));
    assert_eq!(l.today().await.map(|p| p.1), None, "the refused reservation rolled back");
    l.f.done().await;
}

#[tokio::test]
async fn p10_ledger_fence_runs_before_every_attempt() {
    let _lock = LEDGER_ENV.lock().await;
    let _env = LedgerEnv::set(&[("LLM_MAX_RETRIES", "4")]);
    for reason in ["requester_access_lost", "source_removed_or_access_lost", "operator_pause"] {
        let Some(l) = ledger(false, 1).await else { return };
        let (control, owner, account, user, version, run) = (
            l.f.control_owner.clone(), l.f.owner.clone(), l.f.user.account_id,
            l.f.user.user_id, l.versions[0], l.run,
        );
        let mock = llm_mock(move |_| {
            let (control, owner) = (control.clone(), owner.clone());
            async move {
                match reason {
                    "requester_access_lost" => {
                        sqlx::query("DELETE FROM account_members WHERE account_id=$1 AND user_id=$2")
                            .bind(account).bind(user).execute(&control).await.unwrap();
                    }
                    "source_removed_or_access_lost" => {
                        sqlx::query("UPDATE source_items SET access_state='restricted' WHERE source_item_id=(SELECT source_item_id FROM source_versions WHERE version_id=$1)")
                            .bind(version).execute(&owner).await.unwrap();
                    }
                    "operator_pause" => {
                        sqlx::query("UPDATE corpus_enrichment_runs SET status='paused',pause_reason='operator_pause' WHERE run_id=$1")
                            .bind(run).execute(&owner).await.unwrap();
                    }
                    _ => unreachable!(),
                }
                (500, json!({"error": {"message": "down", "code": 500}}))
            }
        }).await;
        l.window(0, &mock_llm(&mock.url, None)).await;
        assert_eq!(mock.hits.load(Ordering::SeqCst), 1, "{reason}: the fresh denial stops the retry");
        let receipts = l.receipts().await;
        assert_eq!(receipts.len(), 1, "{reason}: denied retry creates no additional receipt");
        assert_eq!((receipts[0].0.as_str(), receipts[0].2), ("not_generated", Some(0)), "{reason}: an observed HTTP refusal retains its exact zero-charge settlement");
        assert_eq!(l.run_state().await, ("paused".into(), Some(reason.into())));
        assert_eq!(l.version_state(0).await, ("blocked".into(), Some(reason.into())));
        let (_, reserved, charged, attempts) = l.today().await.unwrap();
        assert_eq!((reserved, charged, attempts), (0, 0, 1));
        let debits: i64 = sqlx::query_scalar("SELECT count(*) FROM credit_ledger WHERE account_id=$1 AND source_receipt_id IS NOT NULL")
            .bind(l.f.user.account_id).fetch_one(&l.f.control_owner).await.unwrap();
        assert_eq!(debits, 0);
        l.f.done().await;
    }
}

#[tokio::test]
async fn p10_ledger_pause_after_response_keeps_charge() {
    let _lock = LEDGER_ENV.lock().await;
    let _env = LedgerEnv::set(&[("LLM_MAX_RETRIES", "4")]);
    let Some(l) = ledger(false, 1).await else { return };
    let (owner, run) = (l.f.owner.clone(), l.run);
    let mock = llm_mock(move |_| {
        let owner = owner.clone();
        async move {
            sqlx::query("UPDATE corpus_enrichment_runs SET status='paused',pause_reason='operator_pause' WHERE run_id=$1")
                .bind(run).execute(&owner).await.unwrap();
            (200, served(PRIMARY, 0.0004))
        }
    })
    .await;
    l.window(0, &mock_llm(&mock.url, None)).await;
    assert_eq!(l.atoms(0).await, 0, "the pause discards the response");
    let receipts = l.receipts().await;
    assert_eq!((receipts[0].0.as_str(), receipts[0].2), ("billed", Some(400)), "the charge stays");
    assert_eq!(l.today().await.unwrap().2, 400);
    l.f.done().await;
}

#[tokio::test]
async fn p10_ledger_kill_switch_denies_without_send() {
    let _lock = LEDGER_ENV.lock().await;
    let _env = LedgerEnv::set(&[("LLM_MAX_RETRIES", "4"), ("MANAGED_PROCESSING_DISABLED", "1")]);
    let Some(l) = ledger(false, 1).await else { return };
    let mock = llm_mock(|_| async { (200, served(PRIMARY, 0.0004)) }).await;
    l.window(0, &mock_llm(&mock.url, None)).await;
    assert_eq!(mock.hits.load(Ordering::SeqCst), 0);
    assert!(l.receipts().await.is_empty());
    assert_eq!(l.run_state().await, ("paused".into(), Some("managed_processing_disabled".into())));
    assert_eq!(l.job_status(0).await, "done");
    l.f.done().await;
}

async fn ledger_reaper_then_late_settle(org: bool) {
    let Some(f) = Fixture::at_boundary(org,false).await else { return };
    sqlx::query("INSERT INTO managed_budget_periods(account_id,purpose,period_start,limit_micro_usd,reserved_micro_usd) VALUES($1,'source_extraction',(now() AT TIME ZONE 'utc')::date,10000,1000)")
        .bind(f.user.account_id).execute(&f.owner).await.unwrap();
    let receipt: Uuid = sqlx::query_scalar("INSERT INTO managed_attempt_receipts(account_id,purpose,period_start,graph_id,logical_call,attempt_no,candidate_models,input_bound_tokens,max_tokens,reserved_micro_usd,created_at) VALUES($1,'source_extraction',(now() AT TIME ZONE 'utc')::date,$2,gen_random_uuid(),1,ARRAY['m'],1,1,1000,now()-interval '1 hour') RETURNING receipt_id")
        .bind(f.user.account_id).bind(f.graph).fetch_one(&f.owner).await.unwrap();
    coralswarm_backend::CONTROL_MIGRATOR.run(&f.control_owner).await.unwrap();
    if org { sqlx::migrate!("./migrations_tenant").run(&f.owner).await.unwrap(); }
    let reap = "SELECT managed_reap_stale(interval '10 minutes')";
    assert_eq!(sqlx::query_scalar::<_, i32>(reap).fetch_one(&f.app).await.unwrap(), 1);
    assert_eq!(sqlx::query_scalar::<_, i32>(reap).fetch_one(&f.app).await.unwrap(), 0);
    let late: bool = sqlx::query_scalar("SELECT managed_settle($1,'billed',500,'m',NULL,NULL)")
        .bind(receipt).fetch_one(&f.owner).await.unwrap();
    assert!(!late, "a late live settle after the reap is a no-op");
    let period: (i64, i64) = sqlx::query_as("SELECT reserved_micro_usd,charged_micro_usd FROM managed_budget_periods WHERE account_id=$1")
        .bind(f.user.account_id).fetch_one(&f.owner).await.unwrap();
    assert_eq!(period, (0, 1000));
    let settled: (String, i64) = sqlx::query_as("SELECT state,settled_micro_usd FROM managed_attempt_receipts WHERE receipt_id=$1")
        .bind(receipt).fetch_one(&f.owner).await.unwrap();
    assert_eq!(settled, ("ambiguous".into(), 1000));
    f.done().await;
}
#[tokio::test]
async fn p10_ledger_reaper_then_late_settle_settles_once_consumer() {
    ledger_reaper_then_late_settle(false).await;
}
#[tokio::test]
async fn p10_ledger_reaper_then_late_settle_settles_once_tenant() {
    ledger_reaper_then_late_settle(true).await;
}

impl Fixture {
    fn worker(&self) -> coralswarm_backend::workers::tenant_harness::WorkerTenant {
        coralswarm_backend::workers::tenant_harness::WorkerTenant {
            tenant: coralswarm_backend::tenancy::TenantId::from_decision(self.tenant.decision.as_ref().unwrap()),
            pool: self.app.clone(),
            store: Arc::new(PostgresStore::new(self.app.clone())),
        }
    }
    async fn derive(&self) {
        coralswarm_backend::workers::billing_derivation::tick(&self.worker(), &self.control_app).await.unwrap();
    }
}

async fn credits_receipt_axis_debits_once(org: bool) {
    let _lock = LEDGER_ENV.lock().await;
    let _env = LedgerEnv::set(&[("LLM_MAX_RETRIES", "4")]);
    let Some(l) = ledger(org, 1).await else { return };
    { let mut seed_tx=l.f.financial().await; let value=sqlx::query("INSERT INTO member_allowance_periods(account_id,user_id,period_start,period_end,granted_credits) VALUES($1,$2,now()-interval '1 day',now()+interval '30 days',100000)")
        .bind(l.f.user.account_id).bind(l.f.user.user_id).execute(&mut *seed_tx).await.unwrap(); seed_tx.commit().await.unwrap(); value };
    let mock = llm_mock(|_| async { (200, served(PRIMARY, 0.0004)) }).await;
    l.window(0, &mock_llm(&mock.url, None)).await;
    let job: Uuid = sqlx::query_scalar("SELECT id FROM jobs WHERE payload->>'source_version_id'=$1")
        .bind(l.versions[0].to_string()).fetch_one(&l.f.owner).await.unwrap();
    l.f.derive().await;
    l.f.derive().await;
    let debits: Vec<(String, Option<Uuid>, String, Option<Uuid>)> = sqlx::query_as("SELECT entry_kind,source_receipt_id,cost_usd::text,attributed_user_id FROM credit_ledger WHERE account_id=$1 AND credits<0")
        .bind(l.f.user.account_id).fetch_all(&l.f.control_owner).await.unwrap();
    assert_eq!(debits.len(), 1, "{debits:?}");
    assert_eq!(debits[0].0, "usage_managed");
    assert!(debits[0].1.is_some());
    assert_eq!(debits[0].2.parse::<f64>().unwrap(), 0.0004);
    assert_eq!(debits[0].3, Some(l.f.user.user_id));
    let job_axis: i64 = sqlx::query_scalar("SELECT count(*) FROM credit_ledger WHERE source_job_id=$1")
        .bind(job).fetch_one(&l.f.control_owner).await.unwrap();
    assert_eq!(job_axis, 0, "the managed job is not billed again from llm_usage");
    let consumed: String = sqlx::query_scalar("SELECT consumed_credits::text FROM member_allowance_periods WHERE account_id=$1")
        .bind(l.f.user.account_id).fetch_one(&l.f.control_owner).await.unwrap();
    assert!(consumed.parse::<f64>().unwrap() > 0.0);
    l.f.done().await;
}
#[tokio::test]
async fn p10_credits_receipt_axis_debits_once_consumer() {
    credits_receipt_axis_debits_once(false).await;
}
#[tokio::test]
async fn p10_credits_receipt_axis_debits_once_tenant() {
    credits_receipt_axis_debits_once(true).await;
}

async fn out_of_credits(l: &Ledger) {
    sqlx::query("UPDATE accounts SET billing_enforcement_enabled=true WHERE id=$1")
        .bind(l.f.user.account_id).execute(&l.f.control_owner).await.unwrap();
    l.f.finite_pool(0).await;
}

#[tokio::test]
async fn p10_credits_exhausted_denies_at_reserve() {
    let _lock = LEDGER_ENV.lock().await;
    let _env = LedgerEnv::set(&[("LLM_MAX_RETRIES", "4")]);
    let Some(l) = ledger(false, 1).await else { return };
    out_of_credits(&l).await;
    let mock = llm_mock(|_| async { (200, served(PRIMARY, 0.0004)) }).await;
    l.window(0, &mock_llm(&mock.url, None)).await;
    assert_eq!(mock.hits.load(Ordering::SeqCst), 0);
    assert!(l.receipts().await.is_empty());
    assert_eq!(l.run_state().await, ("paused".into(), Some("credits_exhausted".into())));
    l.f.done().await;
}

async fn cor103_background_credits_allow_then_hold(org: bool) {
    let _lock = LEDGER_ENV.lock().await;
    let _env = LedgerEnv::set(&[("LLM_MAX_RETRIES", "1")]);
    let Some(l) = ledger(org, 2).await else { return };
    let (account, user) = (l.f.user.account_id, l.f.user.user_id);
    l.f.finite_pool(100).await;
    let mock = llm_mock(|_| async { (200, served(PRIMARY, 0.0004)) }).await;

    l.window(0, &mock_llm(&mock.url, None)).await;
    assert_eq!(mock.hits.load(Ordering::SeqCst), 1);
    let receipt: (Uuid, Option<Uuid>, String) = sqlx::query_as(
        "SELECT account_id,attributed_user_id,state FROM managed_attempt_receipts",
    )
    .fetch_one(&l.f.owner).await.unwrap();
    assert_eq!(receipt, (account, Some(user), "billed".into()));
    assert_eq!(l.version_state(0).await.0, "done");

    { let mut seed_tx=l.f.financial().await; let value=sqlx::query("UPDATE credit_balances SET balance=0 WHERE account_id=$1")
        .bind(account).execute(&mut *seed_tx).await.unwrap(); seed_tx.commit().await.unwrap(); value };
    l.window(1, &mock_llm(&mock.url, None)).await;
    assert_eq!(mock.hits.load(Ordering::SeqCst), 1, "exhaustion prevents the second provider send");
    let denied: Vec<(String, Option<String>, Option<i64>)> = sqlx::query_as(
        "SELECT state,dispatch_phase,settled_micro_usd FROM managed_attempt_receipts WHERE source_version_id=$1",
    ).bind(l.versions[1]).fetch_all(&l.f.owner).await.unwrap();
    if org {
        assert_eq!(denied, vec![("not_generated".into(), Some("cancelled_unsent".into()), Some(0))]);
        let released: (String, i64) = sqlx::query_as("SELECT state,settled_micro_usd FROM managed_credit_reservations WHERE account_id=$1 AND state='released'")
            .bind(account).fetch_one(&l.f.control_owner).await.unwrap();
        assert_eq!(released, ("released".into(), 0));
    } else {
        assert!(denied.is_empty(), "consumer funding and preparation roll back together");
    }
    assert_eq!(l.today().await.unwrap().1, 0);
    assert_eq!(l.unit_charged(1).await, 0);
    let charges: (i64, String) = sqlx::query_as("SELECT count(*),COALESCE(sum(cost_usd),0)::text FROM credit_ledger WHERE account_id=$1 AND entry_kind='usage_managed'")
        .bind(account).fetch_one(&l.f.control_owner).await.unwrap();
    assert_eq!(charges.0, 1);
    assert_eq!(charges.1.parse::<f64>().unwrap(), 0.0004);
    let balance: String = sqlx::query_scalar("SELECT balance::text FROM credit_balances WHERE account_id=$1")
        .bind(account).fetch_one(&l.f.control_owner).await.unwrap();
    assert_eq!(balance.parse::<f64>().unwrap(), 0.0);
    assert_eq!(l.run_state().await, ("paused".into(), Some("credits_exhausted".into())));
    assert_eq!(l.version_state(1).await, ("blocked".into(), Some("credits_exhausted".into())));
    l.f.done().await;
}

#[tokio::test]
async fn cor103_background_credits_allow_then_hold_consumer() {
    cor103_background_credits_allow_then_hold(false).await;
}

#[tokio::test]
async fn cor103_background_credits_allow_then_hold_tenant() {
    cor103_background_credits_allow_then_hold(true).await;
}

#[tokio::test]
async fn cor103_strict_credit_check_preserves_member_and_missing_state_policy() {
    use coralswarm_backend::billing::enforcement::{check_blocked, Spend};

    let Some(f) = Fixture::new(false).await else { return };
    let (account, user) = (f.user.account_id, f.user.user_id);
    assert!(!check_blocked(&f.control_app, account, user, Spend::Background).await.unwrap());
    { let mut seed_tx=f.financial().await; let value=sqlx::query("INSERT INTO credit_balances(account_id,balance) VALUES($1,0)")
        .bind(account).execute(&mut *seed_tx).await.unwrap(); seed_tx.commit().await.unwrap(); value };
    assert!(check_blocked(&f.control_app, account, user, Spend::Background).await.unwrap());
    { let mut seed_tx=f.financial().await; let value=sqlx::query("INSERT INTO member_allowance_periods(account_id,user_id,period_start,period_end,granted_credits,consumed_credits,cap_credits) VALUES($1,$2,now()-interval '1 day',now()+interval '30 days',1,0,1)")
        .bind(account).bind(user).execute(&mut *seed_tx).await.unwrap(); seed_tx.commit().await.unwrap(); value };
    assert!(!check_blocked(&f.control_app, account, user, Spend::Background).await.unwrap(), "the current member allowance remains available");
    assert!(check_blocked(&f.control_app, account, Uuid::new_v4(), Spend::Background).await.unwrap(), "another user cannot draw this member's allowance");
    { let mut seed_tx=f.financial().await; let value=sqlx::query("UPDATE member_allowance_periods SET consumed_credits=2 WHERE account_id=$1 AND user_id=$2")
        .bind(account).bind(user).execute(&mut *seed_tx).await.unwrap(); seed_tx.commit().await.unwrap(); value };
    { let mut seed_tx=f.financial().await; let value=sqlx::query("UPDATE credit_balances SET balance=10 WHERE account_id=$1")
        .bind(account).execute(&mut *seed_tx).await.unwrap(); seed_tx.commit().await.unwrap(); value };
    assert!(check_blocked(&f.control_app, account, user, Spend::Background).await.unwrap(), "an exhausted explicit member cap still blocks a positive pool");
    f.done().await;
}

#[tokio::test]
async fn p10_credits_enforcement_off_switch() {
    let _lock = LEDGER_ENV.lock().await;
    let _env = LedgerEnv::set(&[("LLM_MAX_RETRIES", "4"), ("MANAGED_CREDIT_ENFORCEMENT_DISABLED", "1")]);
    let Some(l) = ledger(false, 1).await else { return };
    out_of_credits(&l).await;
    let mock = llm_mock(|_| async { (200, served(PRIMARY, 0.0004)) }).await;
    l.window(0, &mock_llm(&mock.url, None)).await;
    assert_eq!(mock.hits.load(Ordering::SeqCst), 0, "the legacy enforcement switch cannot authorize an unfunded send");
    assert!(l.receipts().await.is_empty());
    assert_eq!(l.run_state().await, ("paused".into(), Some("credits_exhausted".into())));
    let debits: i64 = sqlx::query_scalar("SELECT count(*) FROM credit_ledger WHERE account_id=$1 AND credits<0")
        .bind(l.f.user.account_id).fetch_one(&l.f.control_owner).await.unwrap();
    assert_eq!(debits, 0);
    let balance: String = sqlx::query_scalar("SELECT balance::text FROM credit_balances WHERE account_id=$1")
        .bind(l.f.user.account_id).fetch_one(&l.f.control_owner).await.unwrap();
    assert_eq!(balance.parse::<f64>().unwrap(), 0.0);
    l.f.done().await;
}

fn meter_request() -> CompletionRequest {
    CompletionRequest {
        messages: vec![Message { role: Role::User, content: "synthetic".into() }],
        model: PRIMARY.into(),
        max_tokens: 64,
        system: None,
        tool_schema: None,
        effort: None,
        reasoning: ReasoningSetting::Off,
    }
}

async fn query_rows(f: &Fixture, query: Uuid) -> Vec<(String, Option<f64>)> {
    sqlx::query_as("SELECT task,cost_usd::float8 FROM llm_usage WHERE source_query_id=$1 ORDER BY created_at")
        .bind(query).fetch_all(&f.control_owner).await.unwrap()
}

#[tokio::test]
async fn p10_meter_retry_rows_for_unmanaged_calls() {
    let _lock = LEDGER_ENV.lock().await;
    let _env = LedgerEnv::set(&[("LLM_MAX_RETRIES", "4")]);
    let Some(f) = Fixture::new(false).await else { return };
    let ctx = |q| coralswarm_backend::llm::usage::UsageContext::new(f.control_app.clone(), Some(f.user.account_id), None).with_source_query(q);
    let errored = json!({"choices":[{"message":{"content":""},"finish_reason":"error"}],"usage":{"prompt_tokens":5,"completion_tokens":0,"cost":0.0001},"model":PRIMARY});
    let ok = json!({"choices":[{"message":{"content":"ok"},"finish_reason":"stop"}],"usage":{"prompt_tokens":5,"completion_tokens":2,"cost":0.0002},"model":PRIMARY});

    let (e, o) = (errored.clone(), ok.clone());
    let mock = llm_mock(move |n| {
        let (e, o) = (e.clone(), o.clone());
        async move {
            match n {
                0 => (200, e),
                1 => {
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    (200, o)
                }
                _ => (200, o),
            }
        }
    })
    .await;
    let q = Uuid::new_v4();
    coralswarm_backend::llm::usage::metered_complete(&mock_llm(&mock.url, Some(300)), meter_request(), Some(&ctx(q)), "meter_probe").await.unwrap();
    assert_eq!(mock.hits.load(Ordering::SeqCst), 3);
    let rows = query_rows(&f, q).await;
    assert_eq!(rows.len(), 1, "one row per logical call");
    assert_eq!(rows[0].0, "meter_probe");
    assert!((rows[0].1.unwrap() - 0.0003).abs() < 1e-12, "the billed failed attempt folds in; the timeout is not priced: {rows:?}");

    let o = ok.clone();
    let mock = llm_mock(move |n| {
        let o = o.clone();
        async move { if n == 0 { (429, json!({"error":{"message":"busy","code":429}})) } else { (200, o) } }
    })
    .await;
    let q = Uuid::new_v4();
    coralswarm_backend::llm::usage::metered_complete(&mock_llm(&mock.url, None), meter_request(), Some(&ctx(q)), "meter_probe").await.unwrap();
    let rows = query_rows(&f, q).await;
    assert_eq!(rows.len(), 1);
    assert!((rows[0].1.unwrap() - 0.0002).abs() < 1e-12, "a 429 cost nothing: {rows:?}");

    std::env::set_var("LLM_MAX_RETRIES", "3");
    let e = errored.clone();
    let mock = llm_mock(move |_| {
        let e = e.clone();
        async move { (200, e) }
    })
    .await;
    let q = Uuid::new_v4();
    coralswarm_backend::llm::usage::metered_complete(&mock_llm(&mock.url, None), meter_request(), Some(&ctx(q)), "meter_probe").await.unwrap_err();
    let rows = query_rows(&f, q).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, "meter_probe_errored");
    assert!((rows[0].1.unwrap() - 0.0003).abs() < 1e-12, "an exhausted call keeps what the provider billed: {rows:?}");
    f.done().await;
}

#[tokio::test]
async fn p10_meter_query_embedding_writes_usage_row() {
    use coralswarm_core::embed_openrouter::{OpenRouterEmbedder, CLOUD_EMBED_MODEL};
    let Some(f) = Fixture::new(false).await else { return };
    let mut vector = vec![json!(0.0); 384];
    vector[0] = json!(1.0);
    let body = json!({"id":"gen-emb","model":CLOUD_EMBED_MODEL,"data":[{"index":0,"embedding":vector}],"usage":{"prompt_tokens":12,"total_tokens":12,"cost":0.00000024}});
    let mock = llm_mock(move |_| {
        let body = body.clone();
        async move { (200, body) }
    })
    .await;
    let embedder = OpenRouterEmbedder::new(reqwest::Client::new(), "k").with_base_url(mock.url.clone());
    let q = Uuid::new_v4();
    let ctx = coralswarm_backend::llm::usage::UsageContext::new(f.control_app.clone(), Some(f.user.account_id), None)
        .with_user(f.user.user_id)
        .with_attributed_user(f.user.user_id)
        .with_source_query(q);
    let purpose = coralswarm_backend::embedding::Purpose::Metered { usage: Some(&ctx), task: "query_embed" };
    let v = coralswarm_backend::embedding::submit_one(&embedder, purpose, "what did Acme decide").await.unwrap();
    assert_eq!(v.len(), 384);
    let row: (String, String, String, Option<f64>, i32) = sqlx::query_as("SELECT task,provider,model,cost_usd::float8,input_tokens FROM llm_usage WHERE source_query_id=$1")
        .bind(q).fetch_one(&f.control_owner).await.unwrap();
    assert_eq!(row, ("query_embed".into(), "openrouter".into(), CLOUD_EMBED_MODEL.into(), Some(0.00000024), 12));
    let priced: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM model_rates WHERE provider='openrouter' AND strpos($1,model_pattern)>0 AND NOT is_blend_fallback)")
        .bind(CLOUD_EMBED_MODEL).fetch_one(&f.control_owner).await.unwrap();
    assert!(priced, "the embedding model has its own rate, not the blend");
    sqlx::query("UPDATE llm_usage SET created_at=now()-interval '10 minutes' WHERE source_query_id=$1")
        .bind(q).execute(&f.control_owner).await.unwrap();
    f.derive().await;
    let debit: String = sqlx::query_scalar("SELECT cost_usd::text FROM credit_ledger WHERE source_query_id=$1")
        .bind(q).fetch_one(&f.control_owner).await.unwrap();
    assert_eq!(debit.parse::<f64>().unwrap(), 0.00000024);
    f.done().await;
}

#[tokio::test]
async fn p10_query_axis_debits_a_query_once_all_rows_are_in() {
    let Some(f) = Fixture::new(false).await else { return };
    let q = Uuid::new_v4();
    let insert = "INSERT INTO llm_usage(account_id,task,model,provider,input_tokens,output_tokens,cost_usd,source_query_id,created_at) VALUES($1,$2,$3,'openrouter',10,$4,$5,$6,now()-$7::interval)";
    for (task, model, output, cost, age) in [
        ("query_embed", "openai/text-embedding-3-small", 0, 0.00001_f64, "5 minutes"),
        ("synthesis_prose", PRIMARY, 50, 0.001_f64, "1 minute"),
    ] {
        sqlx::query(insert).bind(f.user.account_id).bind(task).bind(model).bind(output).bind(cost).bind(q).bind(age)
            .execute(&f.control_owner).await.unwrap();
    }
    f.derive().await;
    sqlx::query("UPDATE llm_usage SET created_at=now()-interval '5 minutes' WHERE source_query_id=$1")
        .bind(q).execute(&f.control_owner).await.unwrap();
    f.derive().await;
    let debits: Vec<String> = sqlx::query_scalar("SELECT cost_usd::text FROM credit_ledger WHERE source_query_id=$1")
        .bind(q).fetch_all(&f.control_owner).await.unwrap();
    assert_eq!(debits.len(), 1);
    assert!((debits[0].parse::<f64>().unwrap() - 0.00101).abs() < 1e-12, "one debit carries every row of the query: {debits:?}");
    f.done().await;
}

async fn departed_capture_actor_charges_the_requester(org: bool) {
    let _lock = LEDGER_ENV.lock().await;
    let _env = LedgerEnv::set(&[("LLM_MAX_RETRIES", "4")]);
    let Some(l) = ledger(org, 1).await else { return };
    let departed: Uuid = sqlx::query_scalar("INSERT INTO users(email) VALUES($1) RETURNING id")
        .bind(format!("p10-departed-{}@test.local", Uuid::new_v4()))
        .fetch_one(&l.f.control_owner).await.unwrap();
    sqlx::query("UPDATE source_versions SET ingested_by=$2 WHERE version_id=$1")
        .bind(l.versions[0]).bind(departed).execute(&l.f.owner).await.unwrap();
    out_of_credits(&l).await;
    { let mut seed_tx=l.f.financial().await; let value=sqlx::query("INSERT INTO member_allowance_periods(account_id,user_id,period_start,period_end,granted_credits) VALUES($1,$2,now()-interval '1 day',now()+interval '30 days',100000)")
        .bind(l.f.user.account_id).bind(l.f.user.user_id).execute(&mut *seed_tx).await.unwrap(); seed_tx.commit().await.unwrap(); value };
    let mock = llm_mock(|_| async { (200, served(PRIMARY, 0.0004)) }).await;
    l.window(0, &mock_llm(&mock.url, None)).await;
    assert_eq!(mock.hits.load(Ordering::SeqCst), 1, "the cohort run is not held for a non-member's credits");
    let charged: Vec<(String, Option<Uuid>)> = sqlx::query_as("SELECT state,attributed_user_id FROM managed_attempt_receipts")
        .fetch_all(&l.f.owner).await.unwrap();
    assert_eq!(charged, vec![("billed".to_string(), Some(l.f.user.user_id))], "the receipt names the requester");
    assert_eq!(l.run_state().await.1, None);
    assert_eq!(l.version_state(0).await.0, "done");
    l.f.done().await;
}
#[tokio::test]
async fn p10_departed_capture_actor_charges_the_requester_consumer() {
    departed_capture_actor_charges_the_requester(false).await;
}
#[tokio::test]
async fn p10_departed_capture_actor_charges_the_requester_tenant() {
    departed_capture_actor_charges_the_requester(true).await;
}

async fn ask_status(f: &Fixture) -> axum::http::StatusCode {
    coralswarm_backend::test_scoped_query_stream(
        f.state(None),
        f.tenant.clone(),
        f.user.clone(),
        json!({"text":"Acme","library_scope":f.scope(None),"style":"sources-only"}),
    )
    .await
    .status()
}

#[tokio::test]
async fn p10_background_exhaustion_pauses_background_but_never_ask() {
    let _lock = LEDGER_ENV.lock().await;
    let _env = LedgerEnv::set(&[("LLM_MAX_RETRIES", "4")]);
    let Some(l) = ledger(false, 2).await else { return };
    let (control, account, user) = (l.f.control_owner.clone(), l.f.user.account_id, l.f.user.user_id);
    sqlx::query("UPDATE accounts SET billing_enforcement_enabled=true WHERE id=$1")
        .bind(account).execute(&control).await.unwrap();
    l.f.finite_pool(1).await;
    sqlx::query("INSERT INTO credit_conversion(effective_from,reference_cost_per_credit,note) VALUES(clock_timestamp(),1,'retained finite-funding overrun fixture')")
        .execute(&control).await.unwrap();
    { let mut seed_tx=financial_fixture(&control,account).await; let value=sqlx::query("INSERT INTO member_allowance_periods(account_id,user_id,period_start,period_end,granted_credits) VALUES($1,$2,now()-interval '1 day',now()+interval '30 days',1)")
        .bind(account).bind(user).execute(&mut *seed_tx).await.unwrap(); seed_tx.commit().await.unwrap(); value };
    let mock = llm_mock(|_| async { (200, served(PRIMARY, 2.0)) }).await;
    l.window(0, &mock_llm(&mock.url, None)).await;
    assert_eq!(mock.hits.load(Ordering::SeqCst), 1);
    l.f.derive().await;
    let (consumed, managed_consumed): (f64, f64) = sqlx::query_as("SELECT consumed_credits::float8,managed_consumed_credits::float8 FROM member_allowance_periods WHERE account_id=$1 AND user_id=$2")
        .bind(account).bind(user).fetch_one(&control).await.unwrap();
    let (balance, managed_debited): (f64, f64) = sqlx::query_as("SELECT balance::float8,managed_debited_credits::float8 FROM credit_balances WHERE account_id=$1")
        .bind(account).fetch_one(&control).await.unwrap();
    assert_eq!((consumed, balance), (2.0, -1.0), "the finite bound was funded, then the actual overrun was charged in full");
    assert_eq!((managed_consumed, managed_debited), (consumed, consumed), "all of it was background");

    assert_eq!(ask_status(&l.f).await, axum::http::StatusCode::OK, "Ask is not blocked by background spend");
    l.window(1, &mock_llm(&mock.url, None)).await;
    assert_eq!(mock.hits.load(Ordering::SeqCst), 1, "background work pauses before it sends");
    assert_eq!(l.run_state().await, ("paused".into(), Some("credits_exhausted".into())));
    assert_eq!(l.version_state(1).await, ("blocked".into(), Some("credits_exhausted".into())));

    { let mut seed_tx=financial_fixture(&control,account).await; let value=sqlx::query("UPDATE member_allowance_periods SET period_end=clock_timestamp() WHERE account_id=$1")
        .bind(account).execute(&mut *seed_tx).await.unwrap(); seed_tx.commit().await.unwrap(); value };
    let pinned_history: i64 = sqlx::query_scalar("SELECT count(*) FROM managed_credit_reservations r JOIN member_allowance_periods p ON p.id=r.member_period_id WHERE r.account_id=$1 AND r.state='settled' AND p.period_end<=clock_timestamp()")
        .bind(account).fetch_one(&control).await.unwrap();
    assert_eq!(pinned_history, 1, "pool-only Ask keeps the settled attempt's immutable allowance history");
    assert_eq!(ask_status(&l.f).await, axum::http::StatusCode::OK, "the pool check leaves background debits out too");
    { let mut seed_tx=financial_fixture(&control,account).await; let value=sqlx::query("UPDATE credit_balances SET balance=balance-1 WHERE account_id=$1")
        .bind(account).execute(&mut *seed_tx).await.unwrap(); seed_tx.commit().await.unwrap(); value };
    assert_eq!(ask_status(&l.f).await, axum::http::StatusCode::PAYMENT_REQUIRED);
    l.f.done().await;
}

#[tokio::test]
async fn p10_ledger_concurrent_reservations_share_one_budget_row() {
    let _lock = LEDGER_ENV.lock().await;
    let _env = LedgerEnv::set(&[("LLM_MAX_RETRIES", "1")]);
    let Some(l) = ledger(false, 3).await else { return };
    let probe = llm_mock(|_| async { (429, json!({"error": {"message": "busy", "code": 429}})) }).await;
    l.window(2, &mock_llm(&probe.url, None)).await;
    let w = l.receipts().await[0].1;
    let limit = w + w / 2;
    sqlx::query("UPDATE managed_budget_periods SET limit_micro_usd=$2 WHERE account_id=$1 AND purpose='source_extraction' AND period_start=(now() AT TIME ZONE 'utc')::date")
        .bind(l.f.user.account_id).bind(limit).execute(&l.f.owner).await.unwrap();
    let cap = format!("{:.6}", limit as f64 / 1_000_000.0);
    let _cap = LedgerEnv::set(&[("MANAGED_DAILY_CAP_USD_EXTRACTION", cap.as_str())]);
    let slow = llm_mock(|_| async {
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        (200, served(PRIMARY, 0.0004))
    })
    .await;
    let llm = mock_llm(&slow.url, None);
    tokio::join!(l.window(0, &llm), l.window(1, &llm));
    assert_eq!(slow.hits.load(Ordering::SeqCst), 1, "only one of two racing reservations fits");
    let (limit, reserved, charged, attempts) = l.today().await.unwrap();
    assert_eq!(reserved, 0);
    assert!(charged <= limit, "{charged} > {limit}");
    assert_eq!(attempts, 2, "the probe and the one admitted attempt");
    assert_eq!(l.run_state().await, ("paused".into(), Some("daily_budget_exhausted".into())));
    l.f.done().await;
}

#[tokio::test]
async fn p10_ledger_reaper_leaves_a_live_call_alone() {
    let _lock = LEDGER_ENV.lock().await;
    let _env = LedgerEnv::set(&[("LLM_MAX_RETRIES", "4")]);
    let Some(l) = ledger(false, 1).await else { return };
    let app = l.f.app.clone();
    let reaped = Arc::new(Mutex::new(Vec::new()));
    let seen = reaped.clone();
    let mock = llm_mock(move |_| {
        let (app, seen) = (app.clone(), seen.clone());
        async move {
            let n: i32 = sqlx::query_scalar("SELECT managed_reap_stale_v1(interval '0')")
                .fetch_one(&app).await.unwrap();
            seen.lock().unwrap().push(n);
            (200, served(PRIMARY, 0.0004))
        }
    })
    .await;
    l.window(0, &mock_llm(&mock.url, None)).await;
    assert_eq!(*reaped.lock().unwrap(), vec![0], "the reaper floor protects the live attempt");
    let receipts = l.receipts().await;
    assert_eq!((receipts[0].0.as_str(), receipts[0].2), ("billed", Some(400)), "the live settle wins");
    l.f.done().await;
}

#[tokio::test]
async fn p10_ledger_settle_retries_a_failed_write() {
    let _lock = LEDGER_ENV.lock().await;
    let _env = LedgerEnv::set(&[("LLM_MAX_RETRIES", "4")]);
    let Some(l) = ledger(false, 1).await else { return };
    // Fails the first settle write only; a sequence survives the rollback.
    sqlx::raw_sql(
        "CREATE SEQUENCE p10_settle_fault; \
         CREATE FUNCTION p10_settle_fault() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER AS $$ \
         BEGIN IF NEW.state <> 'reserved' AND nextval('p10_settle_fault') = 1 THEN \
           RAISE EXCEPTION 'injected settle fault'; END IF; RETURN NEW; END $$; \
         CREATE TRIGGER p10_settle_fault BEFORE UPDATE ON managed_attempt_receipts \
           FOR EACH ROW EXECUTE FUNCTION p10_settle_fault();",
    )
    .execute(&l.f.owner).await.unwrap();
    let mock = llm_mock(|_| async { (200, served(PRIMARY, 0.0004)) }).await;
    l.window(0, &mock_llm(&mock.url, None)).await;
    let faults: i64 = sqlx::query_scalar("SELECT last_value FROM p10_settle_fault").fetch_one(&l.f.owner).await.unwrap();
    assert_eq!(faults, 2, "the first settle failed and a second ran");
    let receipts = l.receipts().await;
    assert_eq!((receipts[0].0.as_str(), receipts[0].2), ("billed", Some(400)), "settled at its cost, not left to the reaper");
    assert_eq!(l.today().await.unwrap().1, 0, "the reservation was released");
    l.f.done().await;
}

#[tokio::test]
async fn p10_regression_default_background_credit_policy() {
    let _lock = LEDGER_ENV.lock().await;
    let _env = LedgerEnv::set(&[("LLM_MAX_RETRIES", "1")]);
    let l = ledger(false, 1).await.unwrap();
    let account = l.f.user.account_id;
    l.f.finite_pool(0).await;
    let enabled: bool = sqlx::query_scalar("SELECT billing_enforcement_enabled FROM accounts WHERE id=$1")
        .bind(account).fetch_one(&l.f.control_owner).await.unwrap();
    assert!(!enabled);
    let ask = ask_status(&l.f).await;
    let mock = llm_mock(|_| async { (200, served(PRIMARY, 0.0004)) }).await;
    l.window(0, &mock_llm(&mock.url, None)).await;
    let hits = mock.hits.load(Ordering::SeqCst);
    let receipts = l.receipts().await;
    println!("default flag={enabled}, Ask={ask}, zero-credit background sends={hits}, receipts={receipts:?}");
    l.f.done().await;
    assert_eq!(ask, axum::http::StatusCode::OK);
    assert_eq!(hits, 0, "background processing must enforce credits even while interactive billing rollout is off");
}

#[tokio::test]
async fn p10_regression_query_rows_arrive_across_derivation_ticks() {
    let f = Fixture::new(false).await.unwrap();
    let q = Uuid::new_v4();
    let insert = "INSERT INTO llm_usage(account_id,task,model,provider,input_tokens,output_tokens,cost_usd,source_query_id,created_at) VALUES($1,$2,$3,'openrouter',10,0,$4,$5,now()-interval '4 minutes')";
    sqlx::query(insert).bind(f.user.account_id).bind("query_embed").bind("openai/text-embedding-3-small").bind(0.00001_f64).bind(q).execute(&f.control_owner).await.unwrap();
    f.derive().await;
    let first: String = sqlx::query_scalar("SELECT cost_usd::text FROM credit_ledger WHERE source_query_id=$1").bind(q).fetch_one(&f.control_owner).await.unwrap();
    sqlx::query(insert).bind(f.user.account_id).bind("synthesis_prose").bind(PRIMARY).bind(0.001_f64).bind(q).execute(&f.control_owner).await.unwrap();
    f.derive().await;
    let final_cost: String = sqlx::query_scalar("SELECT sum(cost_usd)::text FROM credit_ledger WHERE source_query_id=$1").bind(q).fetch_one(&f.control_owner).await.unwrap();
    println!("first debit={first}, final debit={final_cost}, required=0.00101");
    f.done().await;
    assert!((final_cost.parse::<f64>().unwrap()-0.00101).abs()<1e-12, "late synthesis must join the query debit");
}

async fn query_usage(f: &Fixture, query: Uuid, cost: Option<sqlx::types::Decimal>, member: Option<Uuid>) {
    sqlx::query("INSERT INTO llm_usage(account_id,task,model,provider,input_tokens,output_tokens,cost_usd,source_query_id,attributed_user_id,graph_id,created_at) VALUES($1,'synthesis_prose',$2,'openrouter',10,0,$3,$4,$5,$6,now()-interval '4 minutes')")
        .bind(f.user.account_id).bind(PRIMARY).bind(cost).bind(query).bind(member).bind(f.graph)
        .execute(&f.owner).await.unwrap();
}

async fn incremental_query_accounting(org: bool) {
    use sqlx::types::Decimal;
    let f = Fixture::new(org).await.unwrap();
    let q = Uuid::new_v4();
    sqlx::query("UPDATE credit_conversion SET reference_cost_per_credit=3")
        .execute(&f.control_owner).await.unwrap();
    query_usage(&f, q, Some(Decimal::new(1, 8)), Some(f.user.user_id)).await;
    f.derive().await;
    let first: Decimal = sqlx::query_scalar("SELECT sum(cost_usd) FROM credit_ledger WHERE source_query_id=$1")
        .bind(q).fetch_one(&f.control_owner).await.unwrap();
    assert_eq!(first, Decimal::new(1, 8), "embedding-only failed queries still cost money");
    sqlx::query("UPDATE credit_conversion SET reference_cost_per_credit=9")
        .execute(&f.control_owner).await.unwrap();
    query_usage(&f, q, Some(Decimal::new(2, 8)), Some(f.user.user_id)).await;
    tokio::join!(f.derive(), f.derive());
    query_usage(&f, q, Some(Decimal::ZERO), Some(f.user.user_id)).await;
    f.derive().await;
    f.derive().await;
    let (cost, credits, count): (Decimal, Decimal, i64) = sqlx::query_as("SELECT sum(cost_usd),sum(credits),count(*) FROM credit_ledger WHERE source_query_id=$1")
        .bind(q).fetch_one(&f.control_owner).await.unwrap();
    let (balance, managed): (Decimal, Decimal) = sqlx::query_as("SELECT balance,managed_debited_credits FROM credit_balances WHERE account_id=$1")
        .bind(f.user.account_id).fetch_one(&f.control_owner).await.unwrap();
    assert_eq!(cost, Decimal::new(3, 8));
    assert_eq!(credits, -(Decimal::new(3, 8) / Decimal::from(3)), "cumulative rounding and conversion are pinned");
    assert_eq!(count, 2, "duplicate ticks and zero-cost errors create no extra debit");
    assert_eq!(balance, credits);
    assert_eq!(managed, Decimal::ZERO, "query revisions remain interactive spend");
    let mut other_scope = coralswarm_backend::tenancy::scope::begin_account_scoped(&f.control_app, Uuid::new_v4()).await.unwrap();
    let visible: i64 = sqlx::query_scalar("SELECT count(*) FROM query_billing_state WHERE query_id=$1")
        .bind(q).fetch_one(&mut *other_scope).await.unwrap();
    assert_eq!(visible, 0, "another account cannot read query billing state");
    let changed = sqlx::query("UPDATE query_billing_state SET next_revision=999 WHERE query_id=$1")
        .bind(q).execute(&mut *other_scope).await.unwrap().rows_affected();
    assert_eq!(changed, 0, "another account cannot alter query billing state");
    other_scope.rollback().await.unwrap();
    f.done().await;
}

#[tokio::test]
async fn p10_query_revisions_are_exact_and_idempotent_consumer() {
    incremental_query_accounting(false).await;
}

#[tokio::test]
async fn p10_query_revisions_are_exact_and_idempotent_tenant() {
    incremental_query_accounting(true).await;
}

#[tokio::test]
async fn p10_query_legacy_baseline_and_period_attribution_stay_immutable() {
    use sqlx::types::Decimal;
    let f = Fixture::new(false).await.unwrap();
    let account = f.user.account_id;
    let member = f.user.user_id;
    let q = Uuid::new_v4();
    sqlx::query("UPDATE credit_conversion SET reference_cost_per_credit=1")
        .execute(&f.control_owner).await.unwrap();
    { let mut seed_tx=f.financial().await; let value=sqlx::query("INSERT INTO member_allowance_periods(account_id,user_id,period_start,period_end,granted_credits,consumed_credits) VALUES($1,$2,now()-interval '2 days',now()-interval '1 day',100,7),( $1,$2,now()-interval '1 hour',now()+interval '1 day',100,0)")
        .bind(account).bind(member).execute(&mut *seed_tx).await.unwrap(); seed_tx.commit().await.unwrap(); value };
    { let mut seed_tx=f.financial().await; let value=sqlx::query("INSERT INTO credit_ledger(account_id,entry_kind,credits,source_query_id,cost_usd,attributed_user_id) VALUES($1,'usage_query',-7,$2,0.00001,$3)")
        .bind(account).bind(q).bind(member).execute(&mut *seed_tx).await.unwrap(); seed_tx.commit().await.unwrap(); value };
    { let mut seed_tx=f.financial().await; let value=sqlx::query("INSERT INTO credit_balances(account_id,balance) VALUES($1,-7)")
        .bind(account).execute(&mut *seed_tx).await.unwrap(); seed_tx.commit().await.unwrap(); value };
    query_usage(&f, q, Some(Decimal::new(1, 5)), Some(member)).await;
    f.derive().await;
    query_usage(&f, q, Some(Decimal::new(1, 3)), Some(member)).await;
    f.derive().await;
    let other = Uuid::new_v4();
    query_usage(&f, q, Some(Decimal::new(2, 3)), Some(other)).await;
    f.derive().await;
    f.derive().await;
    let rows: Vec<(i64, Decimal, Decimal, Option<Uuid>)> = sqlx::query_as("SELECT query_revision,cost_usd,credits,attributed_user_id FROM credit_ledger WHERE source_query_id=$1 ORDER BY query_revision")
        .bind(q).fetch_all(&f.control_owner).await.unwrap();
    assert_eq!(rows, vec![
        (0, Decimal::new(1, 5), Decimal::from(-7), Some(member)),
        (1, Decimal::new(1, 3), Decimal::new(-1, 3), Some(member)),
        (2, Decimal::new(2, 3), Decimal::new(-2, 3), None),
    ]);
    let periods: Vec<Decimal> = sqlx::query_scalar("SELECT consumed_credits FROM member_allowance_periods WHERE account_id=$1 ORDER BY period_start")
        .bind(account).fetch_all(&f.control_owner).await.unwrap();
    assert_eq!(periods, vec![Decimal::from(7), Decimal::new(1, 3)], "old charges stay in the old period, ambiguous new spend stays unattributed");
    let balance: Decimal = sqlx::query_scalar("SELECT balance FROM credit_balances WHERE account_id=$1")
        .bind(account).fetch_one(&f.control_owner).await.unwrap();
    assert_eq!(balance, Decimal::new(-7003, 3));
    f.done().await;
}

#[tokio::test]
async fn p10_query_fallback_prices_usage_at_its_effective_time() {
    use sqlx::types::Decimal;
    let f = Fixture::new(false).await.unwrap();
    let q = Uuid::new_v4();
    sqlx::query("INSERT INTO model_rates(provider,model_pattern,priority,input_per_million,output_per_million,cache_write_multiplier,cache_read_multiplier,effective_from,effective_to,is_blend_fallback) VALUES('openrouter',$1,-100,1,0,1,1,now()-interval '1 day',now()-interval '1 minute',false),('openrouter',$1,-100,20,0,1,1,now()-interval '1 minute',NULL,false)")
        .bind(PRIMARY).execute(&f.control_owner).await.unwrap();
    query_usage(&f, q, None, Some(f.user.user_id)).await;
    f.derive().await;
    let cost: Decimal = sqlx::query_scalar("SELECT sum(cost_usd) FROM credit_ledger WHERE source_query_id=$1")
        .bind(q).fetch_one(&f.control_owner).await.unwrap();
    assert_eq!(cost, Decimal::new(1, 5), "10 tokens at the usage-time rate of $1/M");
    sqlx::query("UPDATE model_rates SET input_per_million=90 WHERE model_pattern=$1 AND priority=-100 AND effective_to IS NULL")
        .bind(PRIMARY).execute(&f.control_owner).await.unwrap();
    f.derive().await;
    let total: Decimal = sqlx::query_scalar("SELECT sum(cost_usd) FROM credit_ledger WHERE source_query_id=$1")
        .bind(q).fetch_one(&f.control_owner).await.unwrap();
    assert_eq!(total, cost);
    f.done().await;
}

#[tokio::test]
async fn p10_old_worker_query_conflict_rolls_back_then_new_worker_recovers() {
    use sqlx::types::Decimal;
    for org in [false, true] {
        let f = Fixture::new(org).await.unwrap();
        let q = Uuid::new_v4();
        query_usage(&f, q, Some(Decimal::new(1, 5)), Some(f.user.user_id)).await;
        let mut old_tx = coralswarm_backend::tenancy::scope::begin_account_scoped(&f.control_app, f.user.account_id).await.unwrap();
        let error = sqlx::query("INSERT INTO credit_ledger(account_id,entry_kind,credits,source_query_id,cost_usd,attributed_user_id) VALUES($1,'usage_query',-1,$2,0.00001,$3) ON CONFLICT(source_query_id) WHERE source_query_id IS NOT NULL DO NOTHING RETURNING credits")
            .bind(f.user.account_id).bind(q).bind(f.user.user_id)
            .fetch_optional(&mut *old_tx).await.unwrap_err();
        assert_eq!(error.as_database_error().unwrap().code().as_deref(), Some("42P10"));
        old_tx.rollback().await.unwrap();
        let untouched: bool = sqlx::query_scalar("SELECT NOT EXISTS(SELECT 1 FROM credit_ledger WHERE account_id=$1) AND NOT EXISTS(SELECT 1 FROM credit_balances WHERE account_id=$1) AND NOT EXISTS(SELECT 1 FROM query_billing_state WHERE account_id=$1)")
            .bind(f.user.account_id).fetch_one(&f.control_owner).await.unwrap();
        assert!(untouched, "failed old-worker transaction cannot charge or advance discovery");
        assert_eq!(ask_status(&f).await, axum::http::StatusCode::OK);
        f.derive().await;
        query_usage(&f, q, Some(Decimal::new(1, 3)), Some(f.user.user_id)).await;
        f.derive().await;
        f.derive().await;
        let (cost, count): (Decimal, i64) = sqlx::query_as("SELECT sum(cost_usd),count(*) FROM credit_ledger WHERE source_query_id=$1")
            .bind(q).fetch_one(&f.control_owner).await.unwrap();
        assert_eq!(cost, Decimal::new(101, 5));
        assert_eq!(count, 2);
        f.done().await;
    }
}

#[derive(Clone, Copy, PartialEq)]
enum UnavailablePrivatePrimary {
    NonPrimary,
    Archived,
    OtherMember,
}

async fn public_embed_route_metering(org: bool, unavailable_primary: Option<UnavailablePrivatePrimary>) {
    use axum::{body::{Body, to_bytes}, http::Request, Router};
    use coralswarm_backend::{clerk::test_support::{mint, test_jwks, ISSUER}, tenancy::{registry::TenantRegistry, runtime::TenantRuntime, secrets::StaticMapResolver}};
    use tower::ServiceExt;
    let _lock = LEDGER_ENV.lock().await;
    let f = Fixture::new(org).await.unwrap();
    if org {
        sqlx::query("UPDATE graphs SET is_primary=true WHERE id=$1")
            .bind(f.graph).execute(&f.owner).await.unwrap();
        match unavailable_primary {
            Some(UnavailablePrivatePrimary::Archived) => {
                sqlx::query("UPDATE graphs SET archived_at=now() WHERE id=$1")
                    .bind(f.graph).execute(&f.owner).await.unwrap();
            }
            Some(UnavailablePrivatePrimary::NonPrimary) | Some(UnavailablePrivatePrimary::OtherMember) => {
                sqlx::query("UPDATE graphs SET is_primary=false WHERE id=$1")
                    .bind(f.graph).execute(&f.owner).await.unwrap();
                if unavailable_primary == Some(UnavailablePrivatePrimary::OtherMember) {
                    sqlx::query("UPDATE graphs SET is_primary=true WHERE id=$1")
                        .bind(f.denied_graph).execute(&f.owner).await.unwrap();
                }
            }
            None => {},
        }
    }
    let sub = format!("review-embed-{}", Uuid::new_v4());
    sqlx::query("UPDATE users SET clerk_user_id=$2 WHERE id=$1").bind(f.user.user_id).bind(&sub).execute(&f.control_owner).await.unwrap();
    let jwks = test_jwks();
    let jwks_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let jwks_addr = jwks_listener.local_addr().unwrap();
    let jwks_server = tokio::spawn(async move { axum::serve(jwks_listener, Router::new().route("/jwks", axum::routing::get(move || { let jwks = jwks.clone(); async move { ([("content-type", "application/json")], jwks) } }))).await.unwrap(); });
    let _env = LedgerEnv::set(&[("CLERK_ISSUER", ISSUER), ("CLERK_JWKS_URL", &format!("http://{jwks_addr}/jwks")), ("MASTER_KEK_HEX", &"ab".repeat(32))]);
    let hits = Arc::new(AtomicUsize::new(0));
    let counted = hits.clone();
    let embedding_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let embedding_addr = embedding_listener.local_addr().unwrap();
    let embedding_server = tokio::spawn(async move { axum::serve(embedding_listener, Router::new().route("/v1/embeddings", axum::routing::post(move || { let hits = counted.clone(); async move {
        hits.fetch_add(1,Ordering::SeqCst);
        let mut vector = vec![0.0;384]; vector[0]=1.0;
        axum::Json(json!({"id":"independent-embed", "model":"openai/text-embedding-3-small", "data":[{"index":0,"embedding":vector}], "usage":{"prompt_tokens":12,"total_tokens":12,"cost":0.00000024}}))
    } }))).await.unwrap(); });
    let mut state = (*f.state(None)).clone();
    state.embedder = Arc::new(coralswarm_core::embed_openrouter::OpenRouterEmbedder::new(reqwest::Client::new(), "fixture").with_base_url(format!("http://{embedding_addr}")));
    let mut claims = json!({"iss":ISSUER,"sub":sub,"exp":(chrono::Utc::now().timestamp()+3600) as usize,"sid":"review-embed-session","name":"Review Embed"});
    if let Some(ctx) = f.tenant.org_context() {
        sqlx::query("INSERT INTO tenant_meta(id,org_id) VALUES(true,$1) ON CONFLICT(id) DO UPDATE SET org_id=EXCLUDED.org_id").bind(&ctx.org_id).execute(&f.owner).await.unwrap();
        let mut routing = f.financial().await;
        sqlx::query("UPDATE tenant_databases SET slug='reviewembed', db_secret_ref='REVIEW_EMBED_DB' WHERE clerk_org_id=$1 AND account_id=$2").bind(&ctx.org_id).bind(f.user.account_id).execute(&mut *routing).await.unwrap();
        routing.commit().await.unwrap();
        let options = PgConnectOptions::from_str(&std::env::var("P7_TEST_BASE_URL").unwrap()).unwrap();
        let dsn = format!("postgres://coralswarm_tenant_app:{}@127.0.0.1:{}/{}?sslmode=disable", support::tenant_role::APP_ROLE_TEST_PASSWORD, options.get_port(), f.names[1]);
        let resolver = StaticMapResolver::from_pairs([("REVIEW_EMBED_DB", dsn)]);
        state.tenant_runtime = Some(TenantRuntime::new(true, "coralswarm.example", Arc::new(TenantRegistry::new(Arc::new(resolver)))));
        claims["org_id"] = json!(ctx.org_id); claims["org_role"]=json!("org:owner");
    }
    let router = coralswarm_backend::test_support_app(Arc::new(state));
    let response = router.oneshot(Request::builder().method("POST").uri("/v1/embed").header("host","coralswarm.example").header("authorization",format!("Bearer {}",mint(claims))).header("content-type","application/json").body(Body::from(json!({"texts":["synthetic review text"]}).to_string())).unwrap()).await.unwrap();
    let status=response.status();
    let bytes = to_bytes(response.into_body(),1024*1024).await.unwrap();
    let payload: Value = serde_json::from_slice(&bytes).unwrap();
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM llm_usage WHERE account_id=$1 AND task='desktop_embed'").bind(f.user.account_id).fetch_one(&f.owner).await.unwrap();
    let context: Option<(Option<Uuid>, Option<Uuid>, Option<Uuid>)> = sqlx::query_as("SELECT graph_id,attributed_user_id,source_query_id FROM llm_usage WHERE account_id=$1 AND task='desktop_embed'")
        .bind(f.user.account_id).fetch_optional(&f.owner).await.unwrap();
    if rows > 0 {
        let (graph, member, query) = context.unwrap();
        assert_eq!(graph, org.then_some(f.graph));
        assert_eq!(member, Some(f.user.user_id));
        assert!(query.is_some());
    }
    if org {
        let control_rows: i64 = sqlx::query_scalar("SELECT count(*) FROM llm_usage WHERE account_id=$1")
            .bind(f.user.account_id).fetch_one(&f.control_owner).await.unwrap();
        assert_eq!(control_rows, 0, "org usage stays on its content plane");
    }
    sqlx::query("UPDATE llm_usage SET created_at=now()-interval '5 minutes' WHERE account_id=$1 AND task='desktop_embed'").bind(f.user.account_id).execute(&f.owner).await.unwrap();
    f.derive().await;
    let derived_cost: String = sqlx::query_scalar("SELECT COALESCE(sum(cost_usd),0)::text FROM credit_ledger WHERE account_id=$1 AND entry_kind='usage_query'").bind(f.user.account_id).fetch_one(&f.control_owner).await.unwrap();
    println!("derived embedding cost={derived_cost}");
    println!("org={org}, /v1/embed HTTP={status}, provider_calls={}, usage_rows={rows}, returned_vectors={}",hits.load(Ordering::SeqCst),payload["embeddings"].as_array().map_or(0,Vec::len));
    if status != axum::http::StatusCode::OK { println!("route error={payload}"); }
    jwks_server.abort(); embedding_server.abort();
    f.done().await;
    if unavailable_primary.is_some() {
        assert_eq!(status, axum::http::StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(hits.load(Ordering::SeqCst), 0);
        assert_eq!(rows, 0);
        assert_eq!(derived_cost.parse::<f64>().unwrap(), 0.0);
        return;
    }
    assert_eq!(status,axum::http::StatusCode::OK);
    assert_eq!(hits.load(Ordering::SeqCst),1);
    assert_eq!(rows,1,"every successful physical embedding call must record its paid usage");
    assert!((derived_cost.parse::<f64>().unwrap()-0.00000024).abs()<1e-14,"paid embedding usage must reach the ledger");
}
#[tokio::test]
async fn p10_public_embed_consumer_metering() {
    public_embed_route_metering(false, None).await;
}

#[tokio::test]
async fn p10_public_embed_tenant_metering() {
    public_embed_route_metering(true, None).await;
}

#[tokio::test]
async fn p10_public_embed_refuses_unavailable_private_primary_before_send() {
    for unavailable in [UnavailablePrivatePrimary::NonPrimary, UnavailablePrivatePrimary::Archived, UnavailablePrivatePrimary::OtherMember] {
        public_embed_route_metering(true, Some(unavailable)).await;
    }
}

async fn selected_node_planning_policy(f: &Fixture, selected: &LibraryScope, support: &Value) {
    let mut missing = selected.clone();
    missing.node_id = Some(Uuid::new_v4());
    let denied = library_read::retrieve_ask_candidates(&f.tenant, &f.user, missing, &["Acme".into()], None, None, 40)
        .await.err().unwrap();
    assert_eq!(denied.status(), axum::http::StatusCode::NOT_FOUND);
    let mut connection = f.app.acquire().await.unwrap();
    let original: String = sqlx::query_scalar("SHOW jit").fetch_one(&mut *connection).await.unwrap();
    for initial in ["on", "off"] {
        sqlx::query("SELECT set_config('jit',$1,false)").bind(initial).execute(&mut *connection).await.unwrap();
        for node in [None, selected.node_id] {
            for commit in [false, true] {
                let mut scope = selected.clone();
                scope.node_id = node;
                let mut tx = sqlx::Connection::begin(&mut *connection).await.unwrap();
                sqlx::query("SELECT set_config('search_path',$1,true),set_config('app.account_id',$2,true)")
                    .bind("\"$user\", public").bind(f.user.account_id.to_string()).execute(&mut *tx).await.unwrap();
                sqlx::query("SELECT set_config('app.user_id',$1,true)").bind(f.user.user_id.to_string()).execute(&mut *tx).await.unwrap();
                assert!(library_read::ask_inputs_in_scope_tx(&mut tx, &scope, support).await.unwrap());
                let actual: String = sqlx::query_scalar("SHOW jit").fetch_one(&mut *tx).await.unwrap();
                assert_eq!(actual, "off", "the receipt fence plans without JIT");
                if commit { tx.commit().await.unwrap(); } else { tx.rollback().await.unwrap(); }
                let restored: String = sqlx::query_scalar("SHOW jit").fetch_one(&mut *connection).await.unwrap();
                assert_eq!(restored, initial, "planning policy must not leak beyond its transaction");
            }
        }
    }
    sqlx::query("SELECT set_config('jit',$1,false)").bind(original).execute(&mut *connection).await.unwrap();
}

async fn current_node_json_query_and_saved_readback(org: bool) {
    let Some(f) = Fixture::new(org).await else {
        return;
    };
    let state = f.state(None);
    sqlx::query("UPDATE accounts SET billing_enforcement_enabled=true WHERE id=$1")
        .bind(f.user.account_id)
        .execute(&f.control_owner)
        .await
        .unwrap();
    { let mut seed_tx=f.financial().await; let value=sqlx::query("INSERT INTO credit_balances(account_id,balance) VALUES($1,0) ON CONFLICT(account_id) DO UPDATE SET balance=0")
        .bind(f.user.account_id)
        .execute(&mut *seed_tx)
        .await
        .unwrap(); seed_tx.commit().await.unwrap(); value };
    let node: Uuid = sqlx::query_scalar("INSERT INTO nodes(graph_id,type,title,body) VALUES($1,'entity','Acme','{}') RETURNING node_id")
        .bind(f.graph)
        .fetch_one(&f.owner)
        .await
        .unwrap();
    sqlx::query("INSERT INTO node_semantic_atoms(node_id,semantic_atom_id,graph_id) VALUES($1,$2,$3)")
        .bind(node)
        .bind(f.old.2)
        .bind(f.graph)
        .execute(&f.owner)
        .await
        .unwrap();
    let scope = json!({"graph_ids":[f.graph],"node_id":node,"outcome_versions":"current"});
    let expected_scope = serde_json::to_value(serde_json::from_value::<LibraryScope>(scope.clone()).unwrap()).unwrap();
    let request = json!({"text":"What did Acme decide about the green release?","library_scope":scope,"style":"sources-only"});
    let blocked = coralswarm_backend::test_scoped_query_json(state.clone(), f.tenant.clone(), f.user.clone(), request.clone()).await;
    assert_eq!(blocked.status(), axum::http::StatusCode::PAYMENT_REQUIRED);
    let conversations: i64 = sqlx::query_scalar("SELECT count(*) FROM conversations WHERE account_id=$1")
        .bind(f.user.account_id)
        .fetch_one(&f.owner)
        .await
        .unwrap();
    assert_eq!(conversations, 0, "billing rejection must precede conversation persistence");
    assert_eq!(f.calls.load(Ordering::SeqCst), 0);
    assert_eq!(f.embeddings.load(Ordering::SeqCst), 0);
    { let mut seed_tx=f.financial().await; let value=sqlx::query("UPDATE credit_balances SET balance=100 WHERE account_id=$1")
        .bind(f.user.account_id)
        .execute(&mut *seed_tx)
        .await
        .unwrap(); seed_tx.commit().await.unwrap(); value };
    let response = coralswarm_backend::test_scoped_query_json(state.clone(), f.tenant.clone(), f.user.clone(), request).await;
    assert_eq!(response.status(), axum::http::StatusCode::OK);
    let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    let answer: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(answer["mode"], "retrieval_only");
    assert_eq!(answer["text"], "");
    assert_eq!(answer["answer"], Value::Null);
    assert_eq!(answer["library_scope"], expected_scope);
    assert_eq!(answer["support_status"], "complete");
    assert_eq!(answer["outcomes"].as_array().unwrap().len(), 1);
    assert_eq!(answer["outcomes"][0]["semantic_atom_id"], json!(f.old.2));
    assert_eq!(answer["outcomes"][0]["source_version_is_current"], true);
    assert_eq!(answer["retained_citations"].as_array().unwrap().len(), 1);
    let citation = &answer["retained_citations"][0];
    assert_eq!(citation["source_item_id"], json!(f.old.0));
    assert_eq!(citation["source_version_id"], json!(f.old.1));
    assert_eq!(citation["semantic_atom_id"], json!(f.old.2));
    assert_eq!(citation["quote"], "Acme approved green release");
    assert_eq!(citation["start_char"], 0);
    assert_eq!(citation["end_char"], 27);
    let conversation = Uuid::parse_str(answer["conversation_id"].as_str().unwrap()).unwrap();
    let read = coralswarm_backend::test_conversation_read(state, f.tenant.clone(), f.user.clone(), conversation).await;
    assert_eq!(read.status(), axum::http::StatusCode::OK);
    let bytes = axum::body::to_bytes(read.into_body(), 1024 * 1024).await.unwrap();
    let saved: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(saved["conversation"]["id"], json!(conversation));
    assert_eq!(saved["conversation"]["scope_status"], "resolved");
    assert_eq!(saved["conversation"]["library_scope"], expected_scope);
    let messages = saved["messages"].as_array().unwrap();
    assert_eq!(messages.len(), 2);
    assert_eq!(messages[0]["role"], "user");
    assert_eq!(messages[1]["id"], answer["message_id"]);
    assert_eq!(messages[1]["role"], "assistant");
    assert_eq!(messages[1]["mode"], "retrieval_only");
    assert_eq!(messages[1]["support_status"], "complete");
    assert_eq!(messages[1]["library_scope"], expected_scope);
    assert_eq!(messages[1]["retained_citations"], answer["retained_citations"]);
    let receipt: Value = sqlx::query_scalar("SELECT support FROM answer_support_receipts WHERE receipt_id=$1")
        .bind(Uuid::parse_str(answer["message_id"].as_str().unwrap()).unwrap()).fetch_one(&f.owner).await.unwrap();
    selected_node_planning_policy(&f, &serde_json::from_value(expected_scope).unwrap(), &receipt).await;
    assert_eq!(f.calls.load(Ordering::SeqCst), 0);
    assert_eq!(f.embeddings.load(Ordering::SeqCst), 0);
    f.done().await;
}

#[tokio::test]
async fn personal_current_node_json_query_and_readback_without_providers() {
    current_node_json_query_and_saved_readback(false).await;
}

#[tokio::test]
async fn org_current_node_json_query_and_readback_without_providers() {
    current_node_json_query_and_saved_readback(true).await;
}

async fn headline_counts(f:&Fixture)->(i64,i64,i64) {
    sqlx::query_as("SELECT (SELECT count(*) FROM conversations),(SELECT count(*) FROM conversation_messages),(SELECT count(*) FROM answer_support_receipts)").fetch_one(&f.owner).await.unwrap()
}

async fn headline_actor_plane_counts(f:&Fixture)->[(i64,i64,i64);2] {
    let control=sqlx::query_as("SELECT (SELECT count(*) FROM conversations),(SELECT count(*) FROM conversation_messages),(SELECT count(*) FROM answer_support_receipts)").fetch_one(&f.control_owner).await.unwrap();
    [headline_counts(f).await,control]
}

async fn headline_clock_contract(f:&Fixture) {
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD,Engine};
    use coralswarm_backend::library_read::{EventTimeWindow,test_recap_cursor,test_validate_recap_cursor};
    use coralswarm_backend::scoped_ask::test_request_recap_clock;
    use coralswarm_backend::store::Store;
    let t0:chrono::DateTime<chrono::Utc>="2026-10-16T02:00:00Z".parse().unwrap();
    let t1=t0+chrono::Duration::hours(1);
    let start=t0.date_naive().and_hms_opt(0,0,0).unwrap().and_utc();
    let window=EventTimeWindow {since:start,until:t0};
    let fresh=EventTimeWindow {since:start,until:t1};
    let query="recap what happened today";
    let mut scope:LibraryScope=serde_json::from_value(json!({"graph_ids":[f.graph]})).unwrap();scope.normalize().unwrap();
    let request=|text:&str|serde_json::from_value::<AskRequest>(json!({"text":text,"library_scope":scope,"style":"sources-only","candidate_limit":1})).unwrap();
    let original:chrono::DateTime<chrono::Utc>=sqlx::query_scalar("SELECT timestamp FROM atoms WHERE atom_id=$1").bind(f.legacy).fetch_one(&f.owner).await.unwrap();
    sqlx::query("UPDATE atoms SET timestamp=$2 WHERE atom_id=$1").bind(f.legacy).bind(t0-chrono::Duration::minutes(1)).execute(&f.owner).await.unwrap();
    let future:Uuid=sqlx::query_scalar("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,title,timestamp) VALUES($1,'note','clock-future','fixture-only',NULL,'After admitted cutoff',$2) RETURNING atom_id").bind(f.graph).bind(t0+chrono::Duration::minutes(30)).fetch_one(&f.owner).await.unwrap();
    PostgresStore::new(f.control_owner.clone()).set_user_timezone(f.user.user_id,"UTC").await.unwrap();
    let state=f.state(None);
    let (task,_,claimed)=coralswarm_backend::test_retained_mcp_claim(&state,&f.tenant,&f.user,test_request_recap_clock(request(query),t0),None).await.unwrap();
    assert!(coralswarm_backend::test_retained_mcp_run(state.clone(),f.tenant.clone(),f.user.clone(),task,test_request_recap_clock(claimed,t1)).await);
    let delivered=coralswarm_backend::test_retained_mcp_poll(&state,&f.tenant,&f.user,task).await.unwrap();
    assert_eq!(delivered["status"],"completed");
    assert_eq!(delivered["result"]["event_time_window"],json!(window));
    let headers=delivered["result"]["legacy_headlines"].as_array().unwrap();
    assert_eq!(headers.len(),1);assert_eq!(headers[0]["atom_id"],json!(f.legacy));
    assert!(!delivered.to_string().contains("After admitted cutoff"));
    let id=delivered["result"]["message_id"].as_str().unwrap().parse::<Uuid>().unwrap();
    let support:Value=sqlx::query_scalar("SELECT support FROM answer_support_receipts WHERE receipt_id=$1").bind(id).fetch_one(&f.owner).await.unwrap();
    assert!(support.as_array().unwrap().iter().any(|r|r["atom_id"]==json!(f.legacy)));
    assert!(!support.as_array().unwrap().iter().any(|r|r["atom_id"]==json!(future)));
    for (zone,run_at) in [
        ("Pacific/Auckland",t1),
        ("UTC",start+chrono::Duration::days(1)+chrono::Duration::minutes(1)),
        ("UTC",t0-chrono::Duration::seconds(1)),
    ] {
        PostgresStore::new(f.control_owner.clone()).set_user_timezone(f.user.user_id,"UTC").await.unwrap();
        let state=f.state(None);
        let (task,_,claimed)=coralswarm_backend::test_retained_mcp_claim(&state,&f.tenant,&f.user,test_request_recap_clock(request(query),t0),None).await.unwrap();
        let before=headline_counts(f).await;
        PostgresStore::new(f.control_owner.clone()).set_user_timezone(f.user.user_id,zone).await.unwrap();
        assert!(!coralswarm_backend::test_retained_mcp_run(state,f.tenant.clone(),f.user.clone(),task,test_request_recap_clock(claimed,run_at)).await,"{zone} {run_at}");
        assert_eq!(headline_counts(f).await,before,"admission rejection must precede conversation/message/receipt writes");
    }
    PostgresStore::new(f.control_owner.clone()).set_user_timezone(f.user.user_id,"UTC").await.unwrap();
    let old=start-chrono::Duration::hours(12);
    sqlx::query("UPDATE atoms SET timestamp=$2 WHERE atom_id=$1").bind(f.legacy).bind(old).execute(&f.owner).await.unwrap();
    let state=f.state(None);
    let (task,_,claimed)=coralswarm_backend::test_retained_mcp_claim(&state,&f.tenant,&f.user,test_request_recap_clock(request("recap yesterday"),t0),None).await.unwrap();
    assert!(coralswarm_backend::test_retained_mcp_run(state.clone(),f.tenant.clone(),f.user.clone(),task,test_request_recap_clock(claimed,t1)).await);
    let delivered=coralswarm_backend::test_retained_mcp_poll(&state,&f.tenant,&f.user,task).await.unwrap();
    assert_eq!(delivered["status"],"completed");
    assert_eq!(delivered["result"]["event_time_window"],json!({"since":start-chrono::Duration::days(1),"until":start}));
    assert_eq!(delivered["result"]["legacy_headlines"][0]["atom_id"],json!(f.legacy));
    sqlx::query("UPDATE atoms SET timestamp=$2 WHERE atom_id=$1").bind(f.legacy).bind(t0-chrono::Duration::minutes(1)).execute(&f.owner).await.unwrap();
    let cursor=test_recap_cursor(&scope,&f.user,f.user.account_id,query,&window,1,start,f.graph,f.legacy);
    assert_eq!(test_validate_recap_cursor(&scope,&f.user,f.user.account_id,query,Some(&fresh),&cursor,1).unwrap(),Some(window.clone()));
    let mut other_user=f.user.clone();other_user.user_id=Uuid::new_v4();other_user.role="member".into();
    sqlx::query("INSERT INTO users(id,email) VALUES($1,$2)").bind(other_user.user_id).bind(format!("headline-other-{}@fixture.test",other_user.user_id)).execute(&f.control_owner).await.unwrap();
    sqlx::query("INSERT INTO account_members(account_id,user_id,role,seat_tier_id) VALUES($1,$2,'member',(SELECT CASE WHEN a.is_org_account THEN (SELECT id FROM seat_tiers WHERE code='standard' AND is_active) END FROM accounts a WHERE a.id=$1))").bind(other_user.account_id).bind(other_user.user_id).execute(&f.control_owner).await.unwrap();
    let mut other_user_tenant=f.tenant.clone();
    if let Some(TenancyDecision::Org(ctx))=other_user_tenant.decision.as_mut() {ctx.user_id=other_user.user_id;}
    let other_fixture=Fixture::new(f.tenant.org_context().is_some()).await.unwrap();
    let other_account=UserContext {user_id:f.user.user_id,role:"member".into(),..other_fixture.user.clone()};
    sqlx::query("INSERT INTO users(id,email) VALUES($1,$2)").bind(other_account.user_id).bind(format!("headline-shared-{}@fixture.test",other_account.user_id)).execute(&other_fixture.control_owner).await.unwrap();
    sqlx::query("INSERT INTO account_members(account_id,user_id,role,seat_tier_id) VALUES($1,$2,'member',(SELECT CASE WHEN a.is_org_account THEN (SELECT id FROM seat_tiers WHERE code='standard' AND is_active) END FROM accounts a WHERE a.id=$1))").bind(other_account.account_id).bind(other_account.user_id).execute(&other_fixture.control_owner).await.unwrap();
    let mut other_account_tenant=other_fixture.tenant.clone();
    if let Some(TenancyDecision::Org(ctx))=other_account_tenant.decision.as_mut() {ctx.user_id=other_account.user_id;}
    assert_ne!(other_account.account_id,f.user.account_id);assert_eq!(other_account.user_id,f.user.user_id);
    if let (Some(original),Some(alternate))=(f.tenant.org_context(),other_account_tenant.org_context()) {
        let original_db:String=sqlx::query_scalar("SELECT current_database()").fetch_one(&original.pool).await.unwrap();
        let alternate_db:String=sqlx::query_scalar("SELECT current_database()").fetch_one(&alternate.pool).await.unwrap();assert_ne!(original_db,alternate_db);
        let bound_org:String=sqlx::query_scalar("SELECT org_id FROM tenant_meta WHERE id=true").fetch_one(&alternate.pool).await.unwrap();assert_eq!(bound_org,alternate.org_id);
        let registered:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM tenant_databases WHERE clerk_org_id=$1 AND account_id=$2 AND status='active')").bind(&alternate.org_id).bind(alternate.account_id).fetch_one(&other_fixture.control_owner).await.unwrap();assert!(registered);
    }
    let original_state=f.state(None);let other_state=other_fixture.state(None);
    for (tenant,actor,state,control) in [(&other_user_tenant,&other_user,&original_state,&f.control_owner),(&other_account_tenant,&other_account,&other_state,&other_fixture.control_owner)] {
        if let Some(ctx)=tenant.org_context() {assert_eq!(ctx.user_id,actor.user_id);assert_eq!(ctx.account_id,actor.account_id);}
        let member:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM account_members WHERE account_id=$1 AND user_id=$2)").bind(actor.account_id).bind(actor.user_id).fetch_one(control).await.unwrap();assert!(member);
        let before=(headline_actor_plane_counts(f).await,headline_actor_plane_counts(&other_fixture).await);
        let mut denied=request(query);denied.recap_after=Some(cursor.clone());
        let denied=scoped_ask::execute(state,tenant,actor,test_request_recap_clock(denied,t1)).await.err().expect("cursor cannot authorize another actor/account");
        assert_eq!(denied.status(),axum::http::StatusCode::NOT_FOUND);
        assert_eq!((headline_actor_plane_counts(f).await,headline_actor_plane_counts(&other_fixture).await),before,"actor denial must precede writes on both real account planes");
    }
    other_fixture.app.close().await;other_fixture.control_app.close().await;other_fixture.owner.close().await;other_fixture.control_owner.close().await;other_fixture.admin.close().await;
    let mut other_scope=scope.clone();other_scope.source_item_id=Some(f.old.0);
    let mut rejected=vec![
        "malformed".into(),"a".repeat(4097),
        test_recap_cursor(&scope,&other_user,f.user.account_id,query,&window,1,start,f.graph,f.legacy),
        test_recap_cursor(&scope,&f.user,Uuid::new_v4(),query,&window,1,start,f.graph,f.legacy),
        test_recap_cursor(&other_scope,&f.user,f.user.account_id,query,&window,1,start,f.graph,f.legacy),
        test_recap_cursor(&scope,&f.user,f.user.account_id,"recap today differently",&window,1,start,f.graph,f.legacy),
        test_recap_cursor(&scope,&f.user,f.user.account_id,query,&window,2,start,f.graph,f.legacy),
        test_recap_cursor(&scope,&f.user,f.user.account_id,query,&EventTimeWindow {since:start,until:t1+chrono::Duration::hours(1)},1,start,f.graph,f.legacy),
        test_recap_cursor(&scope,&f.user,f.user.account_id,query,&EventTimeWindow {since:start,until:start},1,start,f.graph,f.legacy),
        test_recap_cursor(&scope,&f.user,f.user.account_id,query,&EventTimeWindow {since:start,until:start-chrono::Duration::seconds(1)},1,start,f.graph,f.legacy),
        test_recap_cursor(&scope,&f.user,f.user.account_id,query,&EventTimeWindow {since:start-chrono::Duration::hours(1),until:t0},1,start,f.graph,f.legacy),
        test_recap_cursor(&scope,&f.user,f.user.account_id,query,&window,1,start-chrono::Duration::nanoseconds(1),f.graph,f.legacy),
        test_recap_cursor(&scope,&f.user,f.user.account_id,query,&window,1,t0,f.graph,f.legacy),
    ];
    let raw:Value=serde_json::from_slice(&URL_SAFE_NO_PAD.decode(&cursor).unwrap()).unwrap();
    for (key,value) in [("revision",json!(1)),("record_kind",json!("invented")),("unknown",json!(true))] {
        let mut invalid=raw.clone();invalid[key]=value;rejected.push(URL_SAFE_NO_PAD.encode(serde_json::to_vec(&invalid).unwrap()));
    }
    let mut invalid=raw.clone();invalid.as_object_mut().unwrap().remove("window");rejected.push(URL_SAFE_NO_PAD.encode(serde_json::to_vec(&invalid).unwrap()));
    let mut invalid=raw.clone();invalid["window"].as_object_mut().unwrap().remove("until");rejected.push(URL_SAFE_NO_PAD.encode(serde_json::to_vec(&invalid).unwrap()));
    let mut invalid=raw.clone();invalid["window"]["unknown"]=json!(true);rejected.push(URL_SAFE_NO_PAD.encode(serde_json::to_vec(&invalid).unwrap()));
    let mut invalid=raw.clone();invalid["window"]["until"]=json!("invalid timestamp");rejected.push(URL_SAFE_NO_PAD.encode(serde_json::to_vec(&invalid).unwrap()));
    for after in rejected {
        assert_eq!(test_validate_recap_cursor(&scope,&f.user,f.user.account_id,query,Some(&fresh),&after,1).unwrap_err(),400);
        let before=headline_counts(f).await;
        let mut invalid=request(query);invalid.recap_after=Some(after);
        let response=scoped_ask::execute(&f.state(None),&f.tenant,&f.user,test_request_recap_clock(invalid,t1)).await.err().expect("invalid cursor must reject");
        assert_eq!(response.status(),axum::http::StatusCode::BAD_REQUEST);
        assert_eq!(headline_counts(f).await,before);
    }
    let before=headline_counts(f).await;
    let response=scoped_ask::execute(&f.state(None),&f.tenant,&f.user,test_request_recap_clock(request(query),start)).await.err().expect("empty exact-midnight interval must reject");
    assert_eq!(response.status(),axum::http::StatusCode::BAD_REQUEST);assert_eq!(headline_counts(f).await,before);
    let serialized=serde_json::to_value(test_request_recap_clock(request(query),t0)).unwrap();
    assert!(serialized.get("recap_clock").is_none());assert!(serialized.get("admitted_recap_window").is_none());
    let injected=serde_json::from_value::<AskRequest>(json!({"text":query,"library_scope":scope,"style":"sources-only","recap_clock":start})).unwrap();
    let ordinary=scoped_ask::execute(&f.state(None),&f.tenant,&f.user,injected).await.unwrap();
    assert_ne!(ordinary.payload["event_time_window"]["until"],json!(start),"ordinary JSON must not set the private test clock");
    sqlx::query("UPDATE atoms SET timestamp=$2 WHERE atom_id=$1").bind(future).bind(t0-chrono::Duration::minutes(2)).execute(&f.owner).await.unwrap();
    let node:Uuid=sqlx::query_scalar("INSERT INTO nodes(graph_id,type,title,body) VALUES($1,'entity','Clock scope','{}') RETURNING node_id").bind(f.graph).fetch_one(&f.owner).await.unwrap();
    sqlx::query("INSERT INTO node_atoms(node_id,atom_id) VALUES($1,$2),($1,$3)").bind(node).bind(f.legacy).bind(future).execute(&f.owner).await.unwrap();
    sqlx::query("INSERT INTO node_semantic_atoms(node_id,semantic_atom_id,graph_id) VALUES($1,$2,$3)").bind(node).bind(f.old.2).bind(f.graph).execute(&f.owner).await.unwrap();
    let mut first=request(query);first.library_scope.as_mut().unwrap().node_id=Some(node);
    let first=scoped_ask::execute(&f.state(None),&f.tenant,&f.user,test_request_recap_clock(first,t0)).await.unwrap();
    assert_eq!(first.payload["legacy_headlines"][0]["atom_id"],json!(f.legacy));
    let after=first.payload["next_recap_cursor"].as_str().expect("actual first-page cursor").to_owned();
    let before=headline_counts(f).await;
    let mut historical=request(query);historical.library_scope.as_mut().unwrap().node_id=Some(node);historical.recap_after=Some(after.clone());
    historical.temporal=Some(coralswarm_backend::temporal::TemporalSelection::Historical {revision:1,state_key:"status".into(),effective_at:t0,as_known_at:t0});
    let historical=scoped_ask::execute(&f.state(None),&f.tenant,&f.user,test_request_recap_clock(historical,t1)).await.err().expect("ordinary recap cursor must not enter P11 reconstruction");
    assert_eq!(historical.status(),axum::http::StatusCode::BAD_REQUEST);assert_eq!(headline_counts(f).await,before);
    sqlx::query("DELETE FROM node_atoms WHERE node_id=$1 AND atom_id=$2").bind(node).bind(future).execute(&f.owner).await.unwrap();
    let mut next=request(query);next.library_scope.as_mut().unwrap().node_id=Some(node);next.recap_after=Some(after);
    let next=scoped_ask::execute(&f.state(None),&f.tenant,&f.user,test_request_recap_clock(next,t1)).await.unwrap();
    assert!(next.payload["legacy_headlines"].as_array().unwrap().is_empty(),"fresh selected-node membership must exclude the unlinked next-page header");
    assert_eq!(next.payload["event_time_window"],json!(window));
    sqlx::query("UPDATE atoms SET timestamp=$2 WHERE atom_id=$1").bind(f.legacy).bind(original).execute(&f.owner).await.unwrap();
    {
        let mut financial=f.financial().await;
        let deleted=if f.tenant.org_context().is_some() {
            let mut content=coralswarm_backend::tenancy::scope::begin_account_scoped(&f.owner,f.user.account_id).await.unwrap();
            let deleted=sqlx::query("DELETE FROM atoms WHERE atom_id=$1").bind(future).execute(&mut *content).await.unwrap().rows_affected();
            content.commit().await.unwrap();deleted
        } else {
            sqlx::query("DELETE FROM atoms WHERE atom_id=$1").bind(future).execute(&mut *financial).await.unwrap().rows_affected()
        };
        assert_eq!(deleted,1);financial.commit().await.unwrap();
    }
    assert_eq!(f.calls.load(Ordering::SeqCst),0);assert_eq!(f.embeddings.load(Ordering::SeqCst),0);
}

async fn headline_receipt_lifecycle(org: bool) {
    use coralswarm_backend::store::Store;
    let Some(f) = Fixture::new(org).await else {
        return;
    };
    use std::collections::BTreeSet;
    let state = f.state(None);
    let request = |graph| {
        serde_json::from_value::<AskRequest>(json!({"text":"recap what happened today","library_scope":{"graph_ids":[graph]},"style":"concise"})).unwrap()
    };
    sqlx::query("UPDATE atoms SET content_inline='PRIVATE BODY NOT A HEADLINE',title='Original café 🪸 headline',title_kind='generated' WHERE atom_id=$1").bind(f.legacy).execute(&f.owner).await.unwrap();
    headline_clock_contract(&f).await;
    PostgresStore::new(f.control_owner.clone()).set_user_timezone(f.user.user_id,"UTC").await.unwrap();
    let (window_task,_,window_request)=coralswarm_backend::test_retained_mcp_claim(&state,&f.tenant,&f.user,request(f.graph),None).await.unwrap();
    let before_window:(i64,i64,i64)=sqlx::query_as("SELECT (SELECT count(*) FROM conversations),(SELECT count(*) FROM conversation_messages),(SELECT count(*) FROM answer_support_receipts)").fetch_one(&f.owner).await.unwrap();
    PostgresStore::new(f.control_owner.clone()).set_user_timezone(f.user.user_id,"Pacific/Auckland").await.unwrap();
    assert!(!coralswarm_backend::test_retained_mcp_run(state.clone(),f.tenant.clone(),f.user.clone(),window_task,window_request).await);
    let after_window:(i64,i64,i64)=sqlx::query_as("SELECT (SELECT count(*) FROM conversations),(SELECT count(*) FROM conversation_messages),(SELECT count(*) FROM answer_support_receipts)").fetch_one(&f.owner).await.unwrap();
    assert_eq!(before_window,after_window,"changed admitted window must precede conversation writes");
    PostgresStore::new(f.control_owner.clone()).set_user_timezone(f.user.user_id,"UTC").await.unwrap();
    let answer = scoped_ask::execute(&f.state(None), &f.tenant, &f.user, request(f.graph))
        .await
        .unwrap();
    assert_eq!(answer.payload["answer_status"], "metadata_only");
    assert_eq!(
        answer.payload["legacy_headlines"][0]["title"],
        "Original café 🪸 headline"
    );
    assert!(answer.payload["legacy_evidence"]
        .as_array()
        .unwrap()
        .is_empty());
    assert!(answer.payload["retained_citations"]
        .as_array()
        .unwrap()
        .is_empty());
    assert!(!answer.payload.to_string().contains("PRIVATE BODY"));
    assert_eq!(f.calls.load(Ordering::SeqCst), 0);
    assert_eq!(f.embeddings.load(Ordering::SeqCst), 0);
    sqlx::query("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,title,timestamp) VALUES($1,'note','denied-header','fixture-only','PRIVATE DENIED BODY','PRIVATE DENIED TITLE',now())").bind(f.denied_graph).execute(&f.owner).await.unwrap();
    let denied = scoped_ask::execute(&f.state(None), &f.tenant, &f.user, request(f.denied_graph))
        .await
        .err()
        .unwrap();
    assert_eq!(denied.status(), axum::http::StatusCode::NOT_FOUND);
    let mut exact = request(f.graph);
    exact.library_scope.as_mut().unwrap().source_item_id = Some(f.old.0);
    let exact = scoped_ask::execute(&f.state(None), &f.tenant, &f.user, exact)
        .await
        .unwrap();
    assert!(exact.payload["legacy_headlines"]
        .as_array()
        .unwrap()
        .is_empty());
    let mut tx = coralswarm_backend::tenancy::scope::begin_tenant_scoped(
        &f.app,
        f.user.account_id,
        f.user.user_id,
    )
    .await
    .unwrap();
    let support: Value =
        sqlx::query_scalar("SELECT support FROM answer_support_receipts WHERE receipt_id=$1")
            .bind(answer.receipt_id)
            .fetch_one(&mut *tx)
            .await
            .unwrap();
    let receipt = support
        .as_array()
        .unwrap()
        .iter()
        .find(|e| e["kind"] == "legacy_chunk")
        .unwrap()
        .clone();
    assert_eq!(receipt["projection"], "headline_v1");
    assert_eq!(
        receipt
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect::<BTreeSet<_>>(),
        BTreeSet::from(["kind", "graph_id", "atom_id", "projection", "input_sha256"])
    );
    let mut hashes = Vec::new();
    for zone in ["UTC", "America/Chicago", "Pacific/Auckland"] {
        sqlx::query("SELECT set_config('TimeZone',$1,true)")
            .bind(zone)
            .execute(&mut *tx)
            .await
            .unwrap();
        hashes.push(
            sqlx::query_scalar::<_, Option<String>>("SELECT library_legacy_headline_sha256($1,$2)")
                .bind(f.graph)
                .bind(f.legacy)
                .fetch_one(&mut *tx)
                .await
                .unwrap()
                .unwrap(),
        );
    }
    assert!(hashes.iter().all(|h| *h == hashes[0]));
    assert_eq!(receipt["input_sha256"], hashes[0]);
    for bad in [
        {
            let mut v = receipt.clone();
            v["projection"] = json!("unknown");
            v
        },
        {
            let mut v = receipt.clone();
            v["projection"] = Value::Null;
            v
        },
        {
            let mut v = receipt.clone();
            v.as_object_mut().unwrap().remove("input_sha256");
            v
        },
        {
            let mut v = receipt.clone();
            v["input_sha256"] = json!("bad");
            v
        },
        {
            let mut v = receipt.clone();
            v["title"] = json!("supplied title");
            v
        },
    ] {
        assert!(
            !sqlx::query_scalar::<_, bool>("SELECT library_support_shape($1)")
                .bind(json!([bad]))
                .fetch_one(&mut *tx)
                .await
                .unwrap()
        );
    }
    let forged_id = Uuid::new_v4();
    let mut forged = receipt.clone();
    forged["input_sha256"] = json!("0".repeat(64));
    sqlx::query("INSERT INTO answer_support_receipts(receipt_id,graph_id,owner_user_id,support,evidence_complete) VALUES($1,$2,$3,$4,true)").bind(forged_id).bind(f.graph).bind(f.user.user_id).bind(json!([forged])).execute(&mut *tx).await.unwrap();
    assert!(
        !sqlx::query_scalar::<_, bool>("SELECT library_answer_permitted($1)")
            .bind(forged_id)
            .fetch_one(&mut *tx)
            .await
            .unwrap()
    );
    let wrapper:String=sqlx::query_scalar("SELECT prosrc FROM pg_proc WHERE oid='public.library_answer_permitted(uuid)'::regprocedure").fetch_one(&mut *tx).await.unwrap();
    let temporal_sql = include_str!("../migrations_control/0309_recorded_temporal_state.sql");
    let expected_wrapper=temporal_sql.split("CREATE FUNCTION library_answer_permitted(r_id UUID) RETURNS BOOLEAN LANGUAGE SQL STABLE AS $$").nth(1).unwrap().split("$$;").next().unwrap();
    assert_eq!(
        wrapper.trim(),
        expected_wrapper.trim(),
        "public temporal wrapper body must remain exact"
    );
    let temporal_probe = Uuid::new_v4();
    sqlx::query("INSERT INTO answer_support_receipts(receipt_id,graph_id,owner_user_id,support,evidence_complete) VALUES($1,$2,$3,$4,true)").bind(temporal_probe).bind(f.graph).bind(f.user.user_id).bind(&support).execute(&mut *tx).await.unwrap();
    sqlx::query("INSERT INTO answer_temporal_receipts(receipt_id,graph_id,owner_user_id,temporal_selection,event_ids) VALUES($1,$2,$3,$4,$5)").bind(temporal_probe).bind(f.graph).bind(f.user.user_id).bind(json!({"mode":"historical","revision":1})).bind(vec![Uuid::new_v4()]).execute(&mut *tx).await.unwrap();
    let temporal_gate: (bool, bool) = sqlx::query_as(
        "SELECT library_answer_permitted_before_temporal($1),library_answer_permitted($1)",
    )
    .bind(temporal_probe)
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    assert_eq!(
        temporal_gate,
        (true, false),
        "missing temporal event must deny despite valid headline support"
    );
    let history = f
        .tenant
        .store
        .as_ref()
        .unwrap()
        .list_query_history_scoped(
            &mut tx,
            f.user.account_id,
            f.user.user_id,
            Some(f.graph),
            50,
            0,
        )
        .await
        .unwrap();
    assert!(history.iter().any(|h| h.id == answer.receipt_id));
    assert!(history
        .iter()
        .all(|h| !h.answer_summary.contains("Original café")));
    tx.commit().await.unwrap();
    let node:Uuid=sqlx::query_scalar("INSERT INTO nodes(graph_id,type,title,body) VALUES($1,'entity','Headline scope fixture','{}') RETURNING node_id").bind(f.graph).fetch_one(&f.owner).await.unwrap();
    sqlx::query("INSERT INTO node_semantic_atoms(node_id,semantic_atom_id,graph_id) VALUES($1,$2,$3)").bind(node).bind(f.old.2).bind(f.graph).execute(&f.owner).await.unwrap();
    let yesterday=chrono::Utc::now().date_naive().pred_opt().unwrap().and_hms_opt(12,0,0).unwrap().and_utc();
    let other_header:Uuid=sqlx::query_scalar("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,title,timestamp) VALUES($1,'note','node-yesterday','fixture-only',NULL,'Independent yesterday headline',$2) RETURNING atom_id").bind(f.graph).bind(yesterday).fetch_one(&f.owner).await.unwrap();
    for atom in [f.legacy,other_header] {sqlx::query("INSERT INTO node_atoms(node_id,atom_id) VALUES($1,$2)").bind(node).bind(atom).execute(&f.owner).await.unwrap();}
    let scoped_request=|text:&str,conversation:Option<Uuid>|serde_json::from_value::<AskRequest>(json!({"text":text,"library_scope":{"graph_ids":[f.graph],"node_id":node},"conversation_id":conversation,"style":"sources-only"})).unwrap();
    let node_first=scoped_ask::execute(&f.state(None),&f.tenant,&f.user,scoped_request("recap what happened today",None)).await.unwrap();
    assert_eq!(node_first.payload["legacy_headlines"][0]["atom_id"],json!(f.legacy));
    let node_conversation:Uuid=serde_json::from_value(node_first.payload["conversation_id"].clone()).unwrap();
    let node_second=scoped_ask::execute(&f.state(None),&f.tenant,&f.user,scoped_request("recap yesterday",Some(node_conversation))).await.unwrap();
    assert_eq!(node_second.payload["legacy_headlines"][0]["atom_id"],json!(other_header));
    let (node_task,_,node_task_request)=coralswarm_backend::test_retained_mcp_claim(&state,&f.tenant,&f.user,scoped_request("recap what happened today",None),None).await.unwrap();
    assert!(coralswarm_backend::test_retained_mcp_run(state.clone(),f.tenant.clone(),f.user.clone(),node_task,node_task_request).await);
    sqlx::query("DELETE FROM node_atoms WHERE node_id=$1 AND atom_id=$2").bind(node).bind(f.legacy).execute(&f.owner).await.unwrap();
    let mut node_read=coralswarm_backend::tenancy::scope::begin_tenant_scoped(&f.app,f.user.account_id,f.user.user_id).await.unwrap();
    assert!(sqlx::query_scalar::<_,bool>("SELECT library_node_permitted($1,$2)").bind(f.graph).bind(node).fetch_one(&mut *node_read).await.unwrap(),"another permitted backing source keeps the node accessible");
    node_read.commit().await.unwrap();
    assert!(!scoped_ask::emission_allowed(&state,&f.tenant,&f.user,&node_first).await);
    assert!(scoped_ask::emission_allowed(&state,&f.tenant,&f.user,&node_second).await);
    assert!(coralswarm_backend::test_retained_mcp_poll(&state,&f.tenant,&f.user,node_task).await.is_err());
    let history=coralswarm_backend::test_conversation_read(state.clone(),f.tenant.clone(),f.user.clone(),node_conversation).await;
    assert_eq!(history.status(),axum::http::StatusCode::OK);
    let history:Value=serde_json::from_slice(&axum::body::to_bytes(history.into_body(),1_000_000).await.unwrap()).unwrap();
    let lost=history["messages"].as_array().unwrap().iter().find(|m|m["id"]==node_first.payload["message_id"]).unwrap();
    assert_eq!(lost["support_status"],"unavailable");assert_eq!(lost["legacy_headlines"],json!([]));
    let surviving=history["messages"].as_array().unwrap().iter().find(|m|m["id"]==node_second.payload["message_id"]).unwrap();
    assert_eq!(surviving["support_status"],"complete");assert_eq!(surviving["legacy_headlines"][0]["atom_id"],json!(other_header));
    let sibling_graph:Uuid=sqlx::query_scalar("INSERT INTO graphs(account_id,name,kind,owner_user_id) VALUES($1,'Headline sibling','member',$2) RETURNING id").bind(f.user.account_id).bind(f.user.user_id).fetch_one(&f.owner).await.unwrap();
    let sibling:Uuid=sqlx::query_scalar("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,title,timestamp) VALUES($1,'note','headline-sibling','fixture-only',NULL,'Unaffected sibling headline',now()) RETURNING atom_id").bind(sibling_graph).fetch_one(&f.owner).await.unwrap();
    let sibling_answer = scoped_ask::execute(&f.state(None), &f.tenant, &f.user, request(sibling_graph))
        .await
        .unwrap();
    let (sibling_task, _, sibling_request) = coralswarm_backend::test_retained_mcp_claim(
        &state,
        &f.tenant,
        &f.user,
        request(sibling_graph),
        None,
    )
    .await
    .unwrap();
    assert!(coralswarm_backend::test_retained_mcp_run(
        state.clone(),
        f.tenant.clone(),
        f.user.clone(),
        sibling_task,
        sibling_request,
    )
    .await);
    let (task, _, task_request) = coralswarm_backend::test_retained_mcp_claim(
        &state,
        &f.tenant,
        &f.user,
        request(f.graph),
        None,
    )
    .await
    .unwrap();
    assert!(coralswarm_backend::test_retained_mcp_run(
        state.clone(),
        f.tenant.clone(),
        f.user.clone(),
        task,
        task_request,
    )
    .await);
    assert_eq!(
        coralswarm_backend::test_retained_mcp_poll(&state, &f.tenant, &f.user, task)
            .await
            .unwrap()["status"],
        "completed"
    );
    sqlx::query("UPDATE atoms SET content_inline=NULL WHERE atom_id=$1").bind(f.legacy).execute(&f.owner).await.unwrap();
    assert!(scoped_ask::emission_allowed(&state,&f.tenant,&f.user,&answer).await,"clearing an unused body must preserve headline receipt delivery");
    let body_cleared=coralswarm_backend::test_retained_mcp_poll(&state,&f.tenant,&f.user,task).await.unwrap();
    assert_eq!(body_cleared["status"],"completed");
    assert_eq!(body_cleared["result"]["legacy_headlines"][0]["title"],"Original café 🪸 headline");
    let bodyless_read=coralswarm_backend::test_conversation_read(state.clone(),f.tenant.clone(),f.user.clone(),serde_json::from_value(answer.payload["conversation_id"].clone()).unwrap()).await;
    assert_eq!(bodyless_read.status(),axum::http::StatusCode::OK);
    let bodyless_read:Value=serde_json::from_slice(&axum::body::to_bytes(bodyless_read.into_body(),1_000_000).await.unwrap()).unwrap();
    assert!(bodyless_read["messages"].as_array().unwrap().iter().any(|m|m["legacy_headlines"][0]["title"]=="Original café 🪸 headline"));
    sqlx::query("UPDATE atoms SET title='Retitled current headline' WHERE atom_id=$1")
        .bind(f.legacy)
        .execute(&f.owner)
        .await
        .unwrap();
    assert!(!scoped_ask::emission_allowed(&state, &f.tenant, &f.user, &answer).await);
    assert!(
        coralswarm_backend::test_retained_mcp_poll(&state, &f.tenant, &f.user, task)
            .await
            .is_err()
    );
    let read = coralswarm_backend::test_conversation_read(
        state.clone(),
        f.tenant.clone(),
        f.user.clone(),
        serde_json::from_value(answer.payload["conversation_id"].clone()).unwrap(),
    )
    .await;
    assert_eq!(read.status(), axum::http::StatusCode::OK);
    let read: Value = serde_json::from_slice(
        &axum::body::to_bytes(read.into_body(), 1_000_000)
            .await
            .unwrap(),
    )
    .unwrap();
    assert!(!read.to_string().contains("Original café"));
    assert!(read["messages"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|m| m["role"] == "assistant")
        .all(|m| m["legacy_headlines"] == json!([])));
    let fresh = scoped_ask::execute(&f.state(None), &f.tenant, &f.user, request(f.graph))
        .await
        .unwrap();
    assert_eq!(
        fresh.payload["legacy_headlines"][0]["title"],
        "Retitled current headline"
    );
    for mutation in ["unset", "title_kind", "type", "time", "mapped"] {
        let before = scoped_ask::execute(&f.state(None), &f.tenant, &f.user, request(f.graph))
            .await
            .unwrap();
        match mutation {
            "unset" => {
                sqlx::query("UPDATE atoms SET title=NULL WHERE atom_id=$1")
                    .bind(f.legacy)
                    .execute(&f.owner)
                    .await
                    .unwrap();
            }
            "title_kind" => {
                sqlx::query("UPDATE atoms SET title_kind='fallback' WHERE atom_id=$1")
                    .bind(f.legacy)
                    .execute(&f.owner)
                    .await
                    .unwrap();
            }
            "type" => {
                sqlx::query("UPDATE atoms SET source_type='document' WHERE atom_id=$1")
                    .bind(f.legacy)
                    .execute(&f.owner)
                    .await
                    .unwrap();
            }
            "time" => {
                sqlx::query(
                    "UPDATE atoms SET timestamp=CASE WHEN timestamp=date_trunc('day',timestamp) THEN timestamp+interval '1 microsecond' ELSE date_trunc('day',timestamp) END WHERE atom_id=$1",
                )
                .bind(f.legacy)
                .execute(&f.owner)
                .await
                .unwrap();
            }
            "mapped" => {
                sqlx::query("INSERT INTO source_version_chunks(graph_id,source_version_id,atom_id) VALUES($1,$2,$3)").bind(f.graph).bind(f.old.1).bind(f.legacy).execute(&f.owner).await.unwrap();
            }
            _ => unreachable!(),
        }
        assert!(
            !scoped_ask::emission_allowed(&state, &f.tenant, &f.user, &before).await,
            "{mutation}"
        );
        if mutation == "unset" {
            sqlx::query("UPDATE atoms SET title='Restored distinct headline' WHERE atom_id=$1")
                .bind(f.legacy)
                .execute(&f.owner)
                .await
                .unwrap();
        }
    }
    sqlx::query("DELETE FROM atoms WHERE atom_id=$1")
        .bind(f.legacy)
        .execute(&f.owner)
        .await
        .unwrap();
    let mut tx = coralswarm_backend::tenancy::scope::begin_tenant_scoped(
        &f.app,
        f.user.account_id,
        f.user.user_id,
    )
    .await
    .unwrap();
    for _ in 0..3 {
        sqlx::query("SELECT library_drain_purge(100)")
            .execute(&mut *tx)
            .await
            .unwrap();
    }
    tx.commit().await.unwrap();
    let deleted: (String, Option<Value>) =
        sqlx::query_as("SELECT status,result FROM mcp_tasks WHERE id=$1")
            .bind(task)
            .fetch_one(&f.owner)
            .await
            .unwrap();
    assert_eq!(deleted, ("failed".into(), None));
    let timing: Value = sqlx::query_scalar("SELECT timing FROM conversation_messages WHERE id=$1")
        .bind(serde_json::from_value::<Uuid>(answer.payload["message_id"].clone()).unwrap())
        .fetch_one(&f.owner)
        .await
        .unwrap();
    assert_eq!(timing, json!({}));
    assert!(scoped_ask::emission_allowed(&state, &f.tenant, &f.user, &sibling_answer).await);
    let delivered =
        coralswarm_backend::test_retained_mcp_poll(&state, &f.tenant, &f.user, sibling_task)
            .await
            .unwrap();
    assert_eq!(
        delivered["result"]["legacy_headlines"][0]["atom_id"],
        json!(sibling)
    );
    let mut membership = coralswarm_backend::tenancy::scope::begin_account_scoped(
        &f.control_owner,
        f.user.account_id,
    )
    .await
    .unwrap();
    coralswarm_backend::billing::renewal::lock_period_rotation(&mut membership, f.user.account_id)
        .await
        .unwrap();
    sqlx::query("DELETE FROM account_members WHERE account_id=$1 AND user_id=$2")
        .bind(f.user.account_id)
        .bind(f.user.user_id)
        .execute(&mut *membership)
        .await
        .unwrap();
    membership.commit().await.unwrap();
    assert!(!scoped_ask::emission_allowed(&state, &f.tenant, &f.user, &sibling_answer).await);
    assert!(
        coralswarm_backend::test_retained_mcp_poll(&state, &f.tenant, &f.user, sibling_task)
            .await
            .is_err()
    );
    assert_eq!(f.calls.load(Ordering::SeqCst), 0);
    assert_eq!(f.embeddings.load(Ordering::SeqCst), 0);
    f.done().await;
}

#[tokio::test]
async fn consumer_headline_receipts_recheck_metadata_and_purge_copies() {
    headline_receipt_lifecycle(false).await;
}
#[tokio::test]
async fn tenant_headline_receipts_recheck_metadata_and_purge_copies() {
    headline_receipt_lifecycle(true).await;
}

/// A local OpenRouter stand-in for Jev. Pack Scores drop every card whose
/// digest contains `drop`; each citation check answers `supports`. It keeps
/// every request body so a test can read exactly what left the server.
struct JevStandIn {
    hits: Arc<AtomicUsize>,
    bodies: Arc<Mutex<Vec<String>>>,
}

async fn jev_stand_in(drop: &'static str, delay: std::time::Duration) -> JevStandIn {
    let hits = Arc::new(AtomicUsize::new(0));
    let bodies = Arc::new(Mutex::new(Vec::new()));
    let (counter, seen) = (hits.clone(), bodies.clone());
    let app = axum::Router::new().fallback(move |body: String| {
        let (counter, seen) = (counter.clone(), seen.clone());
        async move {
            counter.fetch_add(1, Ordering::SeqCst);
            seen.lock().unwrap().push(body.clone());
            tokio::time::sleep(delay).await;
            let request: Value = serde_json::from_str(&body).unwrap();
            let user = request["messages"].as_array().unwrap().iter().rev().find(|m| m["role"] == "user").unwrap();
            let user: Value = serde_json::from_str(user["content"].as_str().unwrap()).unwrap();
            let digests: std::collections::HashMap<String, String> = user["state"]["candidates"].as_array().into_iter().flatten()
                .map(|c| (c["atom_id"].as_str().unwrap().to_owned(), c["digest"].as_str().unwrap().to_owned()))
                .collect();
            let answers: serde_json::Map<String, Value> = user["questions"].as_object().unwrap().iter().map(|(key, q)| {
                let answer = match q["type"].as_str() {
                    Some("score") => {
                        let digest = &digests[q["instructions"]["atom_id"].as_str().unwrap()];
                        json!(if digest.contains(drop) { 0 } else { 3 })
                    }
                    _ => json!({"choice":"supports","confidence":0.9}),
                };
                (key.clone(), answer)
            }).collect();
            axum::Json(json!({
                "id":"gen-scoped-jev","model":"deepseek/deepseek-v4-flash-0731","provider":"MockHost",
                "choices":[{"message":{"content":null,"tool_calls":[{"function":{"arguments":Value::Object(answers).to_string()}}]},"finish_reason":"tool_calls"}],
                "usage":{"prompt_tokens":900,"completion_tokens":40,"cost":0.00021}
            }))
        }
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await.ok() });
    let http = reqwest::Client::builder().timeout(std::time::Duration::from_secs(10)).build().unwrap();
    let llm = coralswarm_backend::llm::openai::OpenAiProvider::openrouter(http, "test-key").with_base_url(url);
    coralswarm_backend::decision::test_override_decision_provider(Some(Arc::new(
        coralswarm_backend::decision::OpenRouterDecisionClient::new(
            Arc::new(llm),
            coralswarm_backend::decision::openrouter::JevTimeouts::uniform(std::time::Duration::from_secs(8)),
        ),
    )));
    JevStandIn { hits, bodies }
}

impl JevStandIn {
    fn sent(&self) -> String {
        self.bodies.lock().unwrap().join("\n")
    }
}

impl Drop for JevStandIn {
    fn drop(&mut self) {
        coralswarm_backend::decision::test_override_decision_provider(None);
    }
}

/// Process env for one test, restored on drop (`--test-threads=1`).
struct JevEnv(Vec<(&'static str, Option<std::ffi::OsString>)>);

impl JevEnv {
    fn set(pairs: &[(&'static str, Option<&str>)]) -> Self {
        let defaults = [
            ("JEV_ASK_PATH_MODE", Some("live")),
            ("JEV_ASK_PACK", Some("on")),
            ("JEV_JUDGE_MODE", Some("live")),
            ("JEV_CATALOG_SCAN_MODE", Some("live")),
            ("JEV_TIMEOUT_SECS", None),
            ("MANAGED_PROCESSING_DISABLED", None),
            ("MANAGED_DAILY_CAP_USD_INTERACTIVE_ASK", None),
            ("MANAGED_PER_ATTEMPT_CAP_USD", None),
            ("JEV_MODEL", None),
            ("JEV_MODEL_ASK", None),
        ];
        let all: Vec<(&'static str, Option<&str>)> = defaults
            .into_iter()
            .filter(|(key, _)| !pairs.iter().any(|(k, _)| k == key))
            .chain(pairs.iter().copied())
            .collect();
        let saved = all.iter().map(|(key, _)| (*key, std::env::var_os(key))).collect();
        for (key, value) in &all {
            match value {
                Some(value) => std::env::set_var(key, value),
                None => std::env::remove_var(key),
            }
        }
        Self(saved)
    }
}

impl Drop for JevEnv {
    fn drop(&mut self) {
        for (key, value) in self.0.drain(..) {
            match value {
                Some(value) => std::env::set_var(key, value),
                None => std::env::remove_var(key),
            }
        }
    }
}

async fn skip_to_meta(stream: &mut axum::body::BodyDataStream) {
    loop {
        let frame = stream.next().await.expect("meta frame").unwrap();
        let frame = String::from_utf8_lossy(&frame);
        if frame.contains("event: meta") {
            return;
        }
        assert!(frame.contains("event: progress"), "only progress precedes meta: {frame}");
    }
}

fn sse_final(body: &[u8]) -> Value {
    let body = String::from_utf8_lossy(body);
    let frame = body.split("\n\n").find(|f| f.starts_with("event: final")).expect("final frame");
    let data = frame.lines().find_map(|l| l.strip_prefix("data: ")).expect("final data");
    serde_json::from_str(data).unwrap()
}

async fn interactive_receipts(f: &Fixture) -> Vec<(String, Uuid, Option<Uuid>)> {
    sqlx::query_as("SELECT state,attributed_user_id,job_id FROM managed_attempt_receipts WHERE purpose='interactive_ask' ORDER BY created_at")
        .fetch_all(&f.owner)
        .await
        .unwrap()
}

/// A Neon-shaped tenant runtime: a non-superuser BYPASSRLS table owner that
/// inherits `coralswarm_tenant_app` (prod's `neondb_owner` since 0159).
async fn owner_runtime(f: &Fixture) -> (PgPool, String) {
    // Owns the tenant tables, so it is dropped after that database.
    let role = f.names.last().expect("tenant database").role("p7_jev_owner");
    let password = Uuid::new_v4().simple().to_string();
    f.admin.execute(format!("CREATE ROLE {role} LOGIN NOSUPERUSER NOCREATEROLE BYPASSRLS NOREPLICATION PASSWORD '{password}'").as_str()).await.unwrap();
    f.admin.execute(format!("GRANT coralswarm_tenant_app TO {role} WITH INHERIT TRUE, SET FALSE").as_str()).await.unwrap();
    let su: String = sqlx::query_scalar("SELECT current_user::text").fetch_one(&f.owner).await.unwrap();
    sqlx::raw_sql(&format!(r#"DO $$ DECLARE r record; BEGIN
      FOR r IN SELECT c.oid::regclass AS rel, c.relkind FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname='public' AND c.relkind IN ('r','p','v','m') AND c.relowner='{su}'::regrole
          AND NOT EXISTS(SELECT 1 FROM pg_depend d WHERE d.objid=c.oid AND d.deptype='e') LOOP
        EXECUTE format('ALTER %s %s OWNER TO {role}', CASE r.relkind WHEN 'v' THEN 'VIEW' WHEN 'm' THEN 'MATERIALIZED VIEW' ELSE 'TABLE' END, r.rel);
      END LOOP;
      FOR r IN SELECT c.oid::regclass AS rel FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname='public' AND c.relkind='S' AND c.relowner='{su}'::regrole
          AND NOT EXISTS(SELECT 1 FROM pg_depend d WHERE d.objid=c.oid AND d.deptype IN ('a','i','e')) LOOP
        EXECUTE format('ALTER SEQUENCE %s OWNER TO {role}', r.rel);
      END LOOP;
      FOR r IN SELECT p.oid::regprocedure AS fn FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
        WHERE n.nspname='public' AND p.proowner='{su}'::regrole AND p.prokind IN ('f','p')
          AND NOT EXISTS(SELECT 1 FROM pg_depend d WHERE d.objid=p.oid AND d.deptype='e') LOOP
        EXECUTE format('ALTER ROUTINE %s OWNER TO {role}', r.fn);
      END LOOP;
    END $$;"#)).execute(&f.owner).await.unwrap();
    let opts = PgConnectOptions::from_str(&std::env::var("P7_TEST_BASE_URL").unwrap()).unwrap();
    let pool = PgPoolOptions::new().max_connections(4)
        .connect_with(opts.database(&f.names[1]).username(&role).password(&password)).await.unwrap();
    let shape: (bool, bool, bool, bool) = sqlx::query_as("SELECT r.rolsuper,r.rolbypassrls,pg_get_userbyid(c.relowner)=current_user,pg_has_role(current_user,'coralswarm_tenant_app','USAGE') FROM pg_roles r,pg_class c WHERE r.rolname=current_user AND c.oid='public.atoms'::regclass")
        .fetch_one(&pool).await.unwrap();
    assert_eq!(shape, (false, true, true, true), "owner runtime is a non-superuser BYPASSRLS table owner");
    (pool, role)
}

/// Jev live inside scoped Ask (founder decision 2026-10-19). Jev packs and
/// checks only the evidence the scoped read admitted: a dropped card leaves
/// the answer, a seeded out-of-scope atom is never sent or returned, the
/// cited passage is verified, and every Jev call is one billed
/// `interactive_ask` receipt charged to the asker and billed once.
async fn jev_packs_and_cites_only_admitted_evidence(org: bool, table_owner: bool) {
    let Some(mut f) = Fixture::new(org).await else {
        return;
    };
    let _env = JevEnv::set(&[]);
    f.finite_pool(100).await;
    f.history_contract().await;
    let runtime = if table_owner {
        let (pool, role) = owner_runtime(&f).await;
        f.tenant.pool = Some(pool.clone());
        f.tenant.store = Some(Arc::new(PostgresStore::new(pool.clone())));
        if let Some(TenancyDecision::Org(ctx)) = f.tenant.decision.as_mut() {
            ctx.pool = pool.clone();
        }
        Some((pool, role))
    } else {
        None
    };
    let foreign = source(&f.owner, f.denied_graph, "foreign", "Acme approved the purple foreign plan", false).await;
    let jev = jev_stand_in("red release", std::time::Duration::ZERO).await;
    let state = f.state(None);

    let ask = json!({"text":"What did Acme decide?","library_scope":f.scope(None),"style":"concise"});
    let response = coralswarm_backend::test_scoped_query_json(state.clone(), f.tenant.clone(), f.user.clone(), ask).await;
    assert_eq!(response.status(), axum::http::StatusCode::OK);
    let answer: Value = serde_json::from_slice(&axum::body::to_bytes(response.into_body(), 4 * 1024 * 1024).await.unwrap()).unwrap();
    assert_eq!((answer["jev"]["pack"].as_str(), answer["jev"]["cite"].as_str()), (Some("applied"), Some("applied")), "{}", answer["jev"]);
    assert_eq!(jev.hits.load(Ordering::SeqCst), 2, "one pack call and one cite call");
    let sent = jev.sent();
    assert!(sent.contains("green release") && sent.contains("red release") && sent.contains("Acme legacy chunk"), "Jev judged the admitted set");
    assert!(!sent.contains("purple") && !sent.contains(&foreign.2.to_string()), "an out-of-scope atom never reaches Jev");
    let outcomes: Vec<Value> = answer["outcomes"].as_array().unwrap().clone();
    assert_eq!(outcomes.iter().map(|o| o["semantic_atom_id"].clone()).collect::<Vec<_>>(), vec![json!(f.old.2)], "Jev dropped the red-release card");
    assert_eq!(answer["legacy_evidence"][0]["atom_id"], json!(f.legacy));
    assert!(!answer.to_string().contains("purple"), "the out-of-scope atom is never returned");
    assert_eq!(answer["retained_citations"][0]["jev_verdict"], "verified", "the cited passage was checked");
    assert!(answer["legacy_evidence"][0].get("jev_verdict").is_none(), "an uncited chunk gets no verdict");
    let prompt = f.prompts.lock().unwrap().last().unwrap().clone();
    assert!(!prompt.contains("red release") && prompt.contains("\"jev_role\":\"evidence\""), "synthesis saw the packed set");

    // The receipt binds every input that left the server, including the card
    // Jev dropped, and every emitted outcome is inside it.
    let query: Uuid = answer["query_id"].as_str().unwrap().parse().unwrap();
    let support: Value = sqlx::query_scalar("SELECT support FROM answer_support_receipts WHERE receipt_id=$1").bind(query).fetch_one(&f.owner).await.unwrap();
    let receipted: Vec<&str> = support.as_array().unwrap().iter().filter_map(|e| e["semantic_atom_id"].as_str()).collect();
    assert!(receipted.contains(&f.extra.2.to_string().as_str()) && receipted.contains(&f.old.2.to_string().as_str()), "{support}");
    assert!(!support.to_string().contains(&foreign.2.to_string()));
    let message: Uuid = answer["message_id"].as_str().unwrap().parse().unwrap();
    let replay = coralswarm_backend::test_conversation_read(state.clone(), f.tenant.clone(), f.user.clone(), answer["conversation_id"].as_str().unwrap().parse().unwrap()).await;
    assert_eq!(replay.status(), axum::http::StatusCode::OK, "the packed answer replays with its receipt");
    let replay: Value = serde_json::from_slice(&axum::body::to_bytes(replay.into_body(), 4 * 1024 * 1024).await.unwrap()).unwrap();
    assert!(replay.to_string().contains(&message.to_string()) && replay.to_string().contains("\"jev_verdict\":\"verified\""), "{replay}");

    // Item scope: the sibling item and the legacy chunk are outside it.
    let before = jev.bodies.lock().unwrap().len();
    let item = scoped_ask::execute(&state, &f.tenant, &f.user,
        serde_json::from_value(json!({"text":"What did Acme decide?","library_scope":f.scope(Some(f.old.0)),"style":"concise"})).unwrap()).await.unwrap();
    let item_sent = jev.bodies.lock().unwrap()[before..].join("\n");
    assert!(item_sent.contains("green release"));
    assert!(!item_sent.contains("red release") && !item_sent.contains("Acme legacy chunk") && !item_sent.contains("purple"), "Jev sees only the item's evidence");
    assert_eq!(item.payload["outcomes"].as_array().unwrap().len(), 1);

    // Four calls, four billed receipts charged to the asker, no job.
    let receipts = interactive_receipts(&f).await;
    assert_eq!(receipts, vec![("billed".to_string(), f.user.user_id, None); 4]);
    let usage: Vec<(String, Option<Uuid>)> = sqlx::query_as("SELECT task,source_query_id FROM llm_usage WHERE task LIKE 'jev\\_%' ESCAPE '\\'")
        .fetch_all(&f.owner).await.unwrap();
    assert_eq!(usage, vec![("jev_ask".to_string(), None); 4], "no query key, so the query axis cannot bill Jev");
    let worker = coralswarm_backend::workers::tenant_harness::WorkerTenant {
        tenant: if org { coralswarm_backend::tenancy::TenantId::Org(f.tenant.org_context().unwrap().org_id.clone()) } else { coralswarm_backend::tenancy::TenantId::Consumer },
        pool: f.app.clone(),
        store: Arc::new(PostgresStore::new(f.app.clone())),
    };
    for _ in 0..2 {
        coralswarm_backend::workers::billing_derivation::tick(&worker, &f.control_app).await.unwrap();
    }
    let managed: Vec<(Option<Uuid>, Option<Uuid>)> = sqlx::query_as("SELECT source_receipt_id,source_query_id FROM credit_ledger WHERE account_id=$1 AND entry_kind='usage_managed'")
        .bind(f.user.account_id).fetch_all(&f.control_owner).await.unwrap();
    assert_eq!(managed.len(), 4, "one managed debit per receipt across settle and two derivation passes: {managed:?}");
    assert!(managed.iter().all(|(receipt, query)| receipt.is_some() && query.is_none()));

    drop(jev);
    let role = runtime.map(|(pool, role)| { drop(pool); role });
    f.done().await;
    if let Some(role) = role {
        let admin = PgPool::connect(&std::env::var("P7_TEST_BASE_URL").unwrap()).await.unwrap();
        admin.execute(format!("DROP ROLE IF EXISTS {role}").as_str()).await.unwrap();
    }
}

#[tokio::test]
async fn consumer_jev_packs_and_cites_only_admitted_evidence() {
    jev_packs_and_cites_only_admitted_evidence(false, false).await;
}
#[tokio::test]
async fn tenant_jev_packs_and_cites_only_admitted_evidence() {
    jev_packs_and_cites_only_admitted_evidence(true, false).await;
}
#[tokio::test]
async fn tenant_table_owner_jev_packs_and_cites_only_admitted_evidence() {
    jev_packs_and_cites_only_admitted_evidence(true, true).await;
}

/// Pack is opt-in (`JEV_ASK_PACK`): by default an Ask sends one Jev call, the
/// citation check, and synthesis reads every admitted outcome. The prompt
/// copy carries no ids or offsets; the response and receipt keep them.
async fn jev_pack_is_off_by_default(org: bool) {
    let Some(f) = Fixture::new(org).await else {
        return;
    };
    let _env = JevEnv::set(&[("JEV_ASK_PACK", None)]);
    f.finite_pool(100).await;
    f.history_contract().await;
    let jev = jev_stand_in("red release", std::time::Duration::ZERO).await;
    let state = f.state(None);
    let ask = serde_json::from_value::<AskRequest>(json!({"text":"What did Acme decide?","library_scope":f.scope(None),"style":"concise"})).unwrap();
    let answer = scoped_ask::execute(&state, &f.tenant, &f.user, ask).await.unwrap().payload;
    assert_eq!((answer["jev"]["pack"].as_str(), answer["jev"]["cite"].as_str()), (Some("off"), Some("applied")), "{}", answer["jev"]);
    assert_eq!(answer["jev"]["pack_ms"], 0);
    let timing = &answer["timing"];
    let ms = |k: &str| timing[k].as_u64().unwrap_or_else(|| panic!("timing.{k} missing: {timing}"));
    assert!(ms("before_write_ms") >= ms("prepare_ms") + ms("retrieve_ms") + ms("answer_ms"), "{timing}");
    assert!(ms("synthesize_ms") >= ms("answer_ms"), "{timing}");
    // The response reports every live fence and the write; neither is stored.
    let _ = (ms("fence_ms"), ms("write_ms"));
    let stored: Value = sqlx::query_scalar("SELECT timing FROM conversation_messages WHERE role='assistant' ORDER BY seq DESC LIMIT 1")
        .fetch_one(&f.owner).await.unwrap();
    assert!(stored.get("fence_ms").is_none() && stored.get("write_ms").is_none(), "{stored}");
    assert_eq!(stored["before_write_ms"], timing["before_write_ms"]);
    assert_eq!(jev.hits.load(Ordering::SeqCst), 1, "only the citation check is sent");
    let sent: Value = serde_json::from_str(&jev.bodies.lock().unwrap()[0]).unwrap();
    assert_eq!(sent["reasoning"], json!({"enabled": false}), "no reasoning floor from a GLM fallback");
    assert_eq!(sent["models"], json!(["deepseek/deepseek-v4.1-flash"]));
    assert!(sent["provider"].get("sort").is_none(), "Jev's calls keep OpenRouter's default routing");
    assert_eq!((sent["provider"]["zdr"].clone(), sent["provider"]["data_collection"].clone()), (json!(true), json!("deny")));
    assert_eq!(answer["outcomes"].as_array().unwrap().len(), 2, "nothing is packed away");
    let verdicts: Vec<&Value> = answer["retained_citations"].as_array().unwrap().iter().filter_map(|c| c.get("jev_verdict")).collect();
    assert_eq!(verdicts, vec![&json!("verified")], "the cited passage was checked: {}", answer["retained_citations"]);
    let prompt = f.prompts.lock().unwrap().last().unwrap().clone();
    assert!(prompt.contains("red release") && prompt.contains("green release"), "synthesis saw the admitted set");
    assert!(!prompt.contains("jev_role"), "no pack, no roles");
    let ids: Vec<String> = answer["retained_citations"].as_array().unwrap().iter()
        .flat_map(|c| ["graph_id", "source_item_id", "source_version_id", "semantic_atom_id", "passage_id"].map(|k| c[k].as_str().unwrap().to_owned()))
        .collect();
    assert!(!ids.is_empty());
    for id in &ids {
        assert!(!prompt.contains(id.as_str()), "{id} reached the answer prompt");
    }
    assert!(!prompt.contains("start_char") && !prompt.contains("end_char"));
    let receipts = interactive_receipts(&f).await;
    assert_eq!(receipts, vec![("billed".to_string(), f.user.user_id, None)], "one billed interactive_ask receipt");
    drop(jev);
    f.done().await;
}

#[tokio::test]
async fn consumer_jev_pack_is_off_by_default() {
    jev_pack_is_off_by_default(false).await;
}
#[tokio::test]
async fn tenant_jev_pack_is_off_by_default() {
    jev_pack_is_off_by_default(true).await;
}

/// A budget hold and a Jev timeout each fail open: the Ask still answers
/// from the unpacked admitted set, cites are marked unverified, and a hold
/// sends and bills nothing.
async fn jev_fails_open_and_still_answers(org: bool) {
    let Some(f) = Fixture::new(org).await else {
        return;
    };
    f.finite_pool(100).await;
    f.history_contract().await;
    let state = f.state(None);
    let ask = || serde_json::from_value::<AskRequest>(json!({"text":"What did Acme decide?","library_scope":f.scope(None),"style":"concise"})).unwrap();

    {
        let _env = JevEnv::set(&[("MANAGED_DAILY_CAP_USD_INTERACTIVE_ASK", Some("0"))]);
        let jev = jev_stand_in("red release", std::time::Duration::ZERO).await;
        let answer = scoped_ask::execute(&state, &f.tenant, &f.user, ask()).await.unwrap().payload;
        assert_eq!((answer["mode"].as_str(), answer["jev"]["pack"].as_str(), answer["jev"]["cite"].as_str()),
            (Some("synthesized"), Some("fail_open"), Some("fail_open")), "{}", answer["jev"]);
        assert_eq!(answer["outcomes"].as_array().unwrap().len(), 2, "the unpacked admitted set answers");
        let green = answer["retained_citations"].as_array().unwrap().iter().find(|c| c["quote"].as_str().unwrap().contains("green release")).unwrap();
        assert_eq!(green["jev_verdict"], "unverified", "the cited passage is marked unchecked, not passed");
        assert_eq!(jev.hits.load(Ordering::SeqCst), 0, "a held call is never sent");
        assert!(interactive_receipts(&f).await.iter().all(|(state, _, _)| state != "billed"), "nothing billed");
    }
    {
        let _env = JevEnv::set(&[("JEV_TIMEOUT_SECS", Some("1"))]);
        let jev = jev_stand_in("red release", std::time::Duration::from_secs(4)).await;
        let started = std::time::Instant::now();
        let answer = scoped_ask::execute(&state, &f.tenant, &f.user, ask()).await.unwrap().payload;
        let elapsed = started.elapsed();
        assert_eq!((answer["mode"].as_str(), answer["jev"]["pack"].as_str(), answer["jev"]["cite"].as_str()),
            (Some("synthesized"), Some("fail_open"), Some("fail_open")), "{}", answer["jev"]);
        assert_eq!(answer["outcomes"].as_array().unwrap().len(), 2);
        assert!(jev.hits.load(Ordering::SeqCst) >= 1, "the pack call was sent before the deadline");
        assert!(elapsed < std::time::Duration::from_secs(6), "each phase stops at JEV_TIMEOUT_SECS plus grace: {elapsed:?}");
    }
    f.done().await;
}

#[tokio::test]
async fn consumer_jev_fails_open_and_still_answers() {
    jev_fails_open_and_still_answers(false).await;
}
#[tokio::test]
async fn tenant_jev_fails_open_and_still_answers() {
    jev_fails_open_and_still_answers(true).await;
}

/// HTTP JSON, HTTP SSE and MCP `ask_ocean` run the same Jev pass: two billed
/// `interactive_ask` receipts each, the same packed outcomes and the same
/// verdicts. `search_atoms` is a raw sources-only search and sends nothing.
async fn cloud_ask_entry_points_run_jev_identically(org: bool) {
    let Some(f) = Fixture::new(org).await else {
        return;
    };
    let _env = JevEnv::set(&[]);
    f.finite_pool(100).await;
    f.history_contract().await;
    let jev = jev_stand_in("red release", std::time::Duration::ZERO).await;
    let state = f.state(None);
    let ask = json!({"text":"What did Acme decide?","library_scope":f.scope(None),"style":"concise"});
    let shape = |answer: &Value| {
        (
            answer["jev"]["pack"].clone(),
            answer["jev"]["cite"].clone(),
            answer["outcomes"].as_array().unwrap().iter().map(|o| o["semantic_atom_id"].clone()).collect::<Vec<_>>(),
            answer["retained_citations"].as_array().unwrap().iter().map(|c| c["jev_verdict"].clone()).collect::<Vec<_>>(),
        )
    };
    let mut seen = Vec::new();
    let mut delta = || {
        let now = jev.hits.load(Ordering::SeqCst);
        let previous = seen.last().copied().unwrap_or(0);
        seen.push(now);
        now - previous
    };

    let response = coralswarm_backend::test_scoped_query_json(state.clone(), f.tenant.clone(), f.user.clone(), ask.clone()).await;
    assert_eq!(response.status(), axum::http::StatusCode::OK);
    let json_answer: Value = serde_json::from_slice(&axum::body::to_bytes(response.into_body(), 4 * 1024 * 1024).await.unwrap()).unwrap();
    assert_eq!(delta(), 2, "HTTP JSON");
    let response = coralswarm_backend::test_scoped_query_stream(state.clone(), f.tenant.clone(), f.user.clone(), ask.clone()).await;
    assert_eq!(response.status(), axum::http::StatusCode::OK);
    let stream_answer = sse_final(&axum::body::to_bytes(response.into_body(), 4 * 1024 * 1024).await.unwrap());
    assert_eq!(delta(), 2, "HTTP SSE");
    let mcp = coralswarm_backend::test_retained_mcp_dispatch(state.clone(), &f.tenant, &f.user,
        json!({"method":"tools/call","params":{"name":"ask_ocean","arguments":{"question":"What did Acme decide?","library_scope":f.scope(None),"style":"concise"}}}))
        .await
        .unwrap();
    assert_eq!(mcp["structuredContent"]["support_status"], "complete", "MCP ask_ocean answers: {mcp}");
    assert_eq!(delta(), 2, "MCP ask_ocean");
    let search = coralswarm_backend::test_retained_mcp_dispatch(state.clone(), &f.tenant, &f.user,
        json!({"method":"tools/call","params":{"name":"search_atoms","arguments":{"query":"Acme","library_scope":f.scope(None)}}}))
        .await
        .unwrap();
    assert_eq!(search["structuredContent"]["mode"], "retrieval_only");
    assert_eq!(search["structuredContent"]["jev"]["pack"], "off");
    assert_eq!(delta(), 0, "search_atoms sends nothing to Jev");

    let expected = (json!("applied"), json!("applied"), vec![json!(f.old.2)], vec![json!("verified")]);
    assert_eq!(shape(&json_answer), expected);
    assert_eq!(shape(&stream_answer), expected, "SSE matches JSON");
    assert_eq!(shape(&mcp["structuredContent"]), expected, "MCP matches JSON");
    assert_eq!(interactive_receipts(&f).await, vec![("billed".to_string(), f.user.user_id, None); 6]);
    drop(jev);
    f.done().await;
}

#[tokio::test]
async fn consumer_http_and_mcp_ask_run_jev_identically() {
    cloud_ask_entry_points_run_jev_identically(false).await;
}
#[tokio::test]
async fn tenant_http_and_mcp_ask_run_jev_identically() {
    cloud_ask_entry_points_run_jev_identically(true).await;
}

// ── Hybrid (keyword + vector) scoped Ask ────────────────────────────────────
// Stub vectors only: axis i is a unit vector; `near(i)` is cos 0.995 to it.

/// Answers every query with one fixed vector and counts the calls.
struct QueryEmbed(Vec<f32>, Arc<AtomicUsize>);
impl EmbedProvider for QueryEmbed {
    fn embed_batch<'a>(
        &'a self,
        texts: &'a [coralswarm_core::embed::EmbedInput],
        _: coralswarm_core::embed::ProviderPermit,
    ) -> EmbedBatchFuture<'a> {
        self.1.fetch_add(1, Ordering::SeqCst);
        let v = vec![self.0.clone(); texts.len()];
        Box::pin(async move { Ok(v) })
    }
}
fn axis(i: usize) -> Vec<f32> {
    let mut v = vec![0.0f32; 384];
    v[i] = 1.0;
    v
}
fn near(i: usize) -> Vec<f32> {
    let mut v = vec![0.0f32; 384];
    v[i] = 0.995;
    v[383] = (1.0f32 - 0.995 * 0.995).sqrt();
    v
}
fn ask(f: &Fixture, text: &str, style: &str) -> AskRequest {
    serde_json::from_value(json!({"text":text,"library_scope":f.scope(None),"style":style})).unwrap()
}
fn outcome_ids(v: &Value) -> Vec<Value> {
    v["outcomes"].as_array().unwrap().iter().map(|o| o["semantic_atom_id"].clone()).collect()
}
/// Shares no query term with any fixture statement: only the statement
/// leg can find "Acme approved green release".
const PARAPHRASE: &str = "Which colour launch got sign-off?";

async fn hybrid_determinism(org: bool) {
    let Some(f) = Fixture::new(org).await else { return };
    // Six sources tie on both legs; only their event time differs. Event
    // times run against insertion order so neither heap nor UUID order wins.
    let mut tied = Vec::new();
    for n in 0..6i64 {
        let (_, version, atom) = source(&f.owner, f.graph, &format!("tie-{n}"), "Acme shipped the blue widget", false).await;
        sqlx::query("UPDATE source_versions SET occurred_at=now()-make_interval(days=>$2::int) WHERE version_id=$1")
            .bind(version).bind(n as i32).execute(&f.owner).await.unwrap();
        statement_vector(&f.owner, f.graph, atom, &axis(2)).await;
        tied.push(atom);
    }
    // One more with the same words but a closer statement outranks every tie.
    let (_, closer_version, closer) = source(&f.owner, f.graph, "tie-closer", "Acme shipped the blue widget", false).await;
    sqlx::query("UPDATE source_versions SET occurred_at=now()-interval '30 days' WHERE version_id=$1")
        .bind(closer_version).execute(&f.owner).await.unwrap();
    statement_vector(&f.owner, f.graph, closer, &near(2)).await;
    // `near(2)` as a statement is the query itself below.
    let terms = scoped_ask::query_terms("blue widget");
    // The tied statements sit at cos 0.995 to the query; the closer one at 1.0.
    let query = near(2);
    let first = library_read::retrieve_ask_candidates(&f.tenant, &f.user, f.scope(None), &terms, Some(&query), None, 40).await.unwrap();
    for _ in 0..3 {
        let again = library_read::retrieve_ask_candidates(&f.tenant, &f.user, f.scope(None), &terms, Some(&query), None, 40).await.unwrap();
        assert_eq!(outcome_ids(&again), outcome_ids(&first), "same inputs, same order");
    }
    let mut expected = vec![json!(closer)];
    expected.extend(tied.iter().map(|a| json!(a)));
    assert_eq!(&outcome_ids(&first)[..7], &expected[..], "statement score first, then newest event first");
    // Lexical-only ties also break by recency, not by UUID.
    let lexical = library_read::retrieve_ask_candidates(&f.tenant, &f.user, f.scope(None), &terms, None, None, 40).await.unwrap();
    let lexical_ids = outcome_ids(&lexical);
    let tie_order: Vec<Value> = lexical_ids.iter().filter(|id| tied.iter().any(|a| json!(a) == **id)).cloned().collect();
    assert_eq!(tie_order, tied.iter().map(|a| json!(a)).collect::<Vec<_>>(), "newest first among lexical ties");
    f.done().await;
}
#[tokio::test]
async fn consumer_fused_ranking_is_deterministic() {
    hybrid_determinism(false).await;
}
#[tokio::test]
async fn tenant_fused_ranking_is_deterministic() {
    hybrid_determinism(true).await;
}

async fn hybrid_query_embedding_is_metered(org: bool) {
    use coralswarm_core::embed_openrouter::{OpenRouterEmbedder, CLOUD_EMBED_MODEL};
    let Some(f) = Fixture::new(org).await else { return };
    statement_vector(&f.owner, f.graph, f.old.2, &axis(0)).await;
    let vector: Vec<Value> = near(0).into_iter().map(|x| json!(x)).collect();
    let body = json!({"id":"gen-ask-emb","model":"text-embedding-3-small","data":[{"index":0,"embedding":vector}],"usage":{"prompt_tokens":9,"total_tokens":9,"cost":0.00000018}});
    let mock = llm_mock(move |_| { let body = body.clone(); async move { (200, body) } }).await;
    let state = f.state_with_embedder(Arc::new(OpenRouterEmbedder::new(reqwest::Client::new(), "k").with_base_url(mock.url.clone())));
    coralswarm_backend::test_modern_cached_task_admission(&state, &f.tenant, &f.user).await;
    let answer = scoped_ask::execute(&state, &f.tenant, &f.user, ask(&f, PARAPHRASE, "concise")).await.unwrap();
    assert_eq!(mock.hits.load(Ordering::SeqCst), 1);
    let sent = mock.bodies.lock().unwrap()[0].clone();
    assert_eq!(sent["model"], CLOUD_EMBED_MODEL);
    assert_eq!(sent["dimensions"], 384);
    assert_eq!(sent["input"], json!([PARAPHRASE]), "only the question is sent, never evidence");
    assert_eq!(outcome_ids(&answer.payload).first(), Some(&json!(f.old.2)));
    let query: Uuid = serde_json::from_value(answer.payload["query_id"].clone()).unwrap();
    let row: (String, Option<f64>, i32) = sqlx::query_as("SELECT task,cost_usd::float8,input_tokens FROM llm_usage WHERE source_query_id=$1 AND task='query_embed'")
        .bind(query).fetch_one(&f.owner).await.unwrap();
    assert_eq!(row, ("query_embed".into(), Some(0.00000018), 9), "metered on this Ask's query axis");
    f.done().await;
}
#[tokio::test]
async fn consumer_ask_query_embedding_is_metered() {
    hybrid_query_embedding_is_metered(false).await;
}
#[tokio::test]
async fn tenant_ask_query_embedding_is_metered() {
    hybrid_query_embedding_is_metered(true).await;
}

// ── Statement-vector leg ────────────────────────────────────────────────────

/// One statement vector for `atom`, as the managed statement pipeline writes
/// it: the cloud model tag and the sha256 of the stored statement.
async fn statement_vector(pool: &PgPool, graph: Uuid, atom: Uuid, v: &[f32]) {
    sqlx::query("INSERT INTO semantic_atom_embeddings(graph_id,semantic_atom_id,embed_model,statement_sha256,embedding) \
         SELECT graph_id,semantic_atom_id,$3,sha256(convert_to(statement,'UTF8')),$4 FROM semantic_atoms WHERE graph_id=$1 AND semantic_atom_id=$2 \
         ON CONFLICT (graph_id,semantic_atom_id) DO UPDATE SET embedding=EXCLUDED.embedding,embed_model=EXCLUDED.embed_model,statement_sha256=EXCLUDED.statement_sha256")
        .bind(graph).bind(atom).bind(coralswarm_core::embed::EMBED_MODEL_CLOUD).bind(pgvector::Vector::from(v.to_vec()))
        .execute(pool).await.unwrap();
}
/// The same tenant read through the table owner (prod's tenant role).
fn owner_tenant(f: &Fixture) -> Tenant {
    let mut t = Tenant {
        store: Some(Arc::new(PostgresStore::new(f.owner.clone()))),
        pool: Some(f.owner.clone()),
        decision: None,
        class: coralswarm_backend::route_class::RouteClass::Other,
    };
    t.decision = match &f.tenant.decision {
        Some(TenancyDecision::Org(c)) => Some(TenancyDecision::Org(TenantContext {
            org_id: c.org_id.clone(),
            account_id: c.account_id,
            user_id: c.user_id,
            pool: f.owner.clone(),
            page_pool: None,
        })),
        other => other.clone(),
    };
    t
}

async fn statement_paraphrase(org: bool) {
    let Some(f) = Fixture::new(org).await else { return };
    // Only a statement vector can find the paraphrase.
    statement_vector(&f.owner, f.graph, f.old.2, &axis(0)).await;
    statement_vector(&f.owner, f.graph, f.extra.2, &axis(1)).await;
    let terms = scoped_ask::query_terms(PARAPHRASE);
    for tenant in [f.tenant.clone(), owner_tenant(&f)] {
        let hybrid = library_read::retrieve_ask_candidates(&tenant, &f.user, f.scope(None), &terms, Some(&near(0)), None, 40).await.unwrap();
        assert_eq!(outcome_ids(&hybrid).first(), Some(&json!(f.old.2)), "{hybrid}");
        assert_eq!(hybrid["retrieval"]["mode"], "hybrid");
        assert!(hybrid["retrieval"]["top_statement_score"].as_f64().unwrap() > 0.99);
        assert_eq!(hybrid["retrieval"]["statement_vectors"], 2);
    }
    // A stale vector (statement changed since, or another model) never scores.
    sqlx::query("UPDATE semantic_atom_embeddings SET statement_sha256=sha256('an older statement'::bytea) WHERE semantic_atom_id=$1")
        .bind(f.old.2).execute(&f.owner).await.unwrap();
    let stale = library_read::retrieve_ask_candidates(&f.tenant, &f.user, f.scope(None), &terms, Some(&near(0)), None, 40).await.unwrap();
    assert_ne!(outcome_ids(&stale).first(), Some(&json!(f.old.2)), "{stale}");
    sqlx::query("UPDATE semantic_atom_embeddings SET statement_sha256=sha256(convert_to((SELECT statement FROM semantic_atoms WHERE semantic_atom_id=$1),'UTF8')),embed_model='bge-small-en-v1.5' WHERE semantic_atom_id=$1")
        .bind(f.old.2).execute(&f.owner).await.unwrap();
    let other_model = library_read::retrieve_ask_candidates(&f.tenant, &f.user, f.scope(None), &terms, Some(&near(0)), None, 40).await.unwrap();
    assert_ne!(outcome_ids(&other_model).first(), Some(&json!(f.old.2)));
    statement_vector(&f.owner, f.graph, f.old.2, &axis(0)).await;
    // End to end: the Ask answers from the statement match; strength comes
    // from the statement score.
    let calls = Arc::new(AtomicUsize::new(0));
    let state = f.state_with_embedder(Arc::new(QueryEmbed(near(0), calls.clone())));
    coralswarm_backend::test_modern_cached_task_admission(&state, &f.tenant, &f.user).await;
    let answer = scoped_ask::execute(&state, &f.tenant, &f.user, ask(&f, PARAPHRASE, "concise")).await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1, "only the question is embedded at Ask time");
    assert_eq!(outcome_ids(&answer.payload).first(), Some(&json!(f.old.2)));
    assert_eq!(answer.payload["retrieval"]["strength"], "ok");
    assert!(f.prompts.lock().unwrap().last().unwrap().contains("green release"));
    // Nothing near the question by statement: weak.
    let far = f.state_with_embedder(Arc::new(QueryEmbed(axis(7), calls.clone())));
    let weak = scoped_ask::execute(&far, &f.tenant, &f.user, ask(&f, PARAPHRASE, "concise")).await.unwrap();
    assert_eq!(weak.payload["retrieval"]["strength"], "weak", "{}", weak.payload["retrieval"]);
    assert!(f.prompts.lock().unwrap().last().unwrap().contains(r#""evidence_strength":"weak""#));
    assert!(!outcome_ids(&weak.payload).is_empty(), "weak never drops evidence");
    f.done().await;
}
#[tokio::test]
async fn consumer_paraphrase_found_only_through_statement_leg() {
    statement_paraphrase(false).await;
}
#[tokio::test]
async fn tenant_paraphrase_found_only_through_statement_leg() {
    statement_paraphrase(true).await;
}

async fn statement_permission(org: bool) {
    let Some(f) = Fixture::new(org).await else { return };
    statement_vector(&f.owner, f.graph, f.old.2, &axis(0)).await;
    statement_vector(&f.owner, f.graph, f.extra.2, &axis(1)).await;
    // Another member's private graph holds an exact statement match.
    let private = source(&f.owner, f.denied_graph, "private-statement", "PRIVATE sign-off statement", false).await;
    statement_vector(&f.owner, f.denied_graph, private.2, &axis(0)).await;
    let terms = scoped_ask::query_terms(PARAPHRASE);
    let readable = library_read::retrieve_ask_candidates(&f.tenant, &f.user, f.scope(None), &terms, Some(&near(0)), None, 40).await.unwrap();
    assert_eq!(outcome_ids(&readable).first(), Some(&json!(f.old.2)));
    assert!(!readable.to_string().contains("PRIVATE"));
    // Restrict the matching source: its statement leaves, and so does its score.
    f.access("restricted").await;
    for tenant in [f.tenant.clone(), owner_tenant(&f)] {
        let restricted = library_read::retrieve_ask_candidates(&tenant, &f.user, f.scope(None), &terms, Some(&near(0)), None, 40).await.unwrap();
        assert!(!outcome_ids(&restricted).contains(&json!(f.old.2)), "{restricted}");
        assert!(!restricted.to_string().contains("PRIVATE"));
        let top = restricted["retrieval"]["top_statement_score"].as_f64().unwrap();
        assert!(top < 0.2, "only the readable axis-1 statement may score: {top}");
        assert_eq!(restricted["retrieval"]["statement_vectors"], 1);
    }
    let denied: LibraryScope = serde_json::from_value(json!({"graph_ids":[f.denied_graph]})).unwrap();
    assert_eq!(library_read::retrieve_ask_candidates(&f.tenant, &f.user, denied, &terms, Some(&near(0)), None, 40)
        .await.err().unwrap().status(), axum::http::StatusCode::NOT_FOUND);
    let calls = Arc::new(AtomicUsize::new(0));
    let state = f.state_with_embedder(Arc::new(QueryEmbed(near(0), calls.clone())));
    coralswarm_backend::test_modern_cached_task_admission(&state, &f.tenant, &f.user).await;
    let answer = scoped_ask::execute(&state, &f.tenant, &f.user, ask(&f, PARAPHRASE, "concise")).await.unwrap();
    assert!(!outcome_ids(&answer.payload).contains(&json!(f.old.2)));
    assert!(f.prompts.lock().unwrap().iter().all(|p| !p.contains("green release") && !p.contains("PRIVATE")));
    f.access("permitted").await;
    f.done().await;
}
#[tokio::test]
async fn consumer_statement_leg_never_returns_unreadable_atoms() {
    statement_permission(false).await;
}
#[tokio::test]
async fn tenant_statement_leg_never_returns_unreadable_atoms() {
    statement_permission(true).await;
}

const LIVE_FACT: &str = "CoralSwarm backend 0.9.4 is live in production";

/// Captures `text` for item `id` through the real capture path on the app role.
async fn capture(f: &Fixture, id: &str, text: &str, metadata: Value) -> coralswarm_backend::source_evidence::CaptureOutcome {
    use coralswarm_backend::tenancy::scope;
    let mut tx = if f.tenant.decision.as_ref().is_some_and(|d| matches!(d, TenancyDecision::Org(_))) {
        scope::begin_tenant_system_scoped(&f.app, f.user.account_id).await.unwrap()
    } else {
        scope::begin_account_scoped(&f.app, f.user.account_id).await.unwrap()
    };
    let source = coralswarm_backend::source_evidence::CaptureSource {
        source_type: "slack", provider_namespace: "fixture:carry", source_id: id,
        normalized_text: text, representation: "original_text", metadata: &metadata, occurred_at: None,
        participants: &[], ingested_by: Some(f.user.user_id), scope: "private",
        duration_ms: None, segments: None,
    };
    let saved = coralswarm_backend::source_evidence::capture_source_version_tx(
        &mut tx, None, f.graph, f.user.account_id, &source, None, &[42; 32],
    ).await.unwrap();
    tx.commit().await.unwrap();
    saved
}

/// Records `statement` as the extracted fact of `version`, as extraction does.
async fn extracted(f: &Fixture, version: Uuid, statement: &str) -> Uuid {
    let atom: Uuid = sqlx::query_scalar("INSERT INTO semantic_atoms(graph_id,source_version_id,ordinal,kind,statement,extraction_version,model) VALUES($1,$2,0,'fact',$3,'source-atoms-v2','fake-model') RETURNING semantic_atom_id")
        .bind(f.graph).bind(version).bind(statement).fetch_one(&f.owner).await.unwrap();
    sqlx::query("INSERT INTO semantic_atom_passages(graph_id,source_version_id,semantic_atom_id,start_char,end_char,quote) VALUES($1,$2,$3,0,$4,$5)")
        .bind(f.graph).bind(version).bind(atom).bind(statement.chars().count() as i32).bind(statement).execute(&f.owner).await.unwrap();
    sqlx::query("UPDATE source_versions SET extraction_status='done',extraction_version='source-atoms-v2',next_start_char=$3 WHERE graph_id=$1 AND version_id=$2")
        .bind(f.graph).bind(version).bind(statement.chars().count() as i64).execute(&f.owner).await.unwrap();
    atom
}

/// A pre-fix re-capture of `version`: same bytes, pending under v3, no facts,
/// made current. Returns the new version id.
async fn pending_identical_recapture(f: &Fixture, version: Uuid, metadata: Value) -> Uuid {
    let pending: Uuid = sqlx::query_scalar("INSERT INTO source_versions(graph_id,source_item_id,source_id,source_type,provider_namespace,raw_inline,content_sha256,representation,source_metadata,extraction_version,extraction_status,captured_at) SELECT graph_id,source_item_id,source_id,source_type,provider_namespace,raw_inline,content_sha256,representation,$2,'source-atoms-v3','pending',now()+interval '1 second' FROM source_versions WHERE version_id=$1 RETURNING version_id")
        .bind(version).bind(&metadata).fetch_one(&f.owner).await.unwrap();
    sqlx::query("UPDATE source_items SET current_version_id=(SELECT version_id FROM source_versions WHERE version_id=$2) WHERE source_item_id=(SELECT source_item_id FROM source_versions WHERE version_id=$1)")
        .bind(version).bind(pending).execute(&f.owner).await.unwrap();
    pending
}

/// The fact's outcome under a current-only scoped Ask read of `item`, on the
/// app role and on the owner role prod connects as.
async fn current_outcomes(f: &Fixture, item: Uuid) -> [Vec<Value>; 2] {
    let scope: LibraryScope = serde_json::from_value(json!({"graph_ids":[f.graph],"source_item_id":item,"outcome_versions":"current"})).unwrap();
    let owner_pool = f.owner.clone();
    let owner = Tenant {
        store: Some(Arc::new(PostgresStore::new(owner_pool.clone()))),
        pool: Some(owner_pool.clone()),
        decision: f.tenant.decision.clone().map(|d| match d {
            TenancyDecision::Org(ctx) => TenancyDecision::Org(TenantContext { pool: owner_pool.clone(), ..ctx }),
            other => other,
        }),
        class: f.tenant.class,
    };
    let mut out = [vec![], vec![]];
    for (i, tenant) in [&f.tenant, &owner].into_iter().enumerate() {
        let page = library_read::retrieve_ask_candidates(tenant, &f.user, scope.clone(), &["backend".into()], None, None, 10)
            .await.unwrap_or_else(|e| panic!("scoped read failed: {}", e.status()));
        out[i] = page["outcomes"].as_array().unwrap().iter()
            .filter(|o| o["source_version_is_current"] == json!(true)).cloned().collect();
    }
    out
}

/// Extraction work a version put on the paid path: queued jobs, and a
/// standing admission row the sweep can still adopt. A `processed` receipt
/// is terminal (a carried capture's clock, tenant 0193 / control 0362) and
/// is not work.
async fn extraction_work(f: &Fixture, version: Uuid) -> (i64, i64) {
    sqlx::query_as("SELECT (SELECT count(*) FROM jobs WHERE kind LIKE 'Extract%' AND payload->>'source_version_id'=$1::text),(SELECT count(*) FROM corpus_capture_admissions WHERE source_version_id=$1 AND state<>'processed')")
        .bind(version).fetch_one(&f.owner).await.unwrap()
}

async fn carry_forward(org: bool) {
    let Some(f) = Fixture::new(org).await else { return; };
    let first = capture(&f, "C1", LIVE_FACT, json!({"title":"deploys"})).await;
    let atom = extracted(&f, first.version_id, LIVE_FACT).await;
    for side in current_outcomes(&f, first.source_item_id).await {
        assert_eq!(side.len(), 1, "the extracted fact is current before the re-capture");
    }

    // #1382: identical bytes, new anchor keys in metadata.
    let anchored = json!({"title":"deploys","channel_name":"eng","url":"https://example.invalid/C1"});
    let second = capture(&f, "C1", LIVE_FACT, anchored).await;
    assert!(second.changed && second.version_id != first.version_id, "metadata change still mints a version");
    let (status, profile): (String, String) = sqlx::query_as("SELECT extraction_status,extraction_version FROM source_versions WHERE version_id=$1")
        .bind(second.version_id).fetch_one(&f.owner).await.unwrap();
    assert_eq!((status.as_str(), profile.as_str()), ("done", "source-atoms-v2"), "carried under the extracted profile");
    assert_eq!(extraction_work(&f, second.version_id).await, (0, 0), "a metadata-only re-capture puts nothing on the paid path");
    let receipt: (String, String, bool) = sqlx::query_as("SELECT state,hold_basis,ever_held FROM corpus_capture_admissions WHERE source_version_id=$1")
        .bind(second.version_id).fetch_one(&f.owner).await.unwrap();
    assert_eq!(receipt, ("processed".into(), "carried_forward".into(), false), "the carry has a clock, not an unknown");
    for side in current_outcomes(&f, first.source_item_id).await {
        assert_eq!(side.len(), 1, "metadata-only re-capture keeps the fact current");
        assert_eq!(side[0]["semantic_atom_id"], json!(atom));
        assert_eq!(side[0]["source_version_id"], json!(first.version_id));
    }

    // Changed content waits for its own extraction; the old fact is history.
    let edited = capture(&f, "C1", "CoralSwarm backend 0.9.5 is live in production", json!({"title":"deploys"})).await;
    assert!(edited.changed);
    assert_eq!(extraction_work(&f, edited.version_id).await.1, 1, "changed content goes to extraction");
    let status: String = sqlx::query_scalar("SELECT extraction_status FROM source_versions WHERE version_id=$1").bind(edited.version_id).fetch_one(&f.owner).await.unwrap();
    assert_ne!(status, "done", "changed content waits for its own extraction");
    for side in current_outcomes(&f, first.source_item_id).await {
        assert!(side.is_empty(), "stale fact from different bytes is not served as current: {side:?}");
    }

    // A pre-fix re-capture: same bytes, pending under v3 with a queued job and
    // no facts. The read fallback serves the extracted facts meanwhile.
    let legacy = capture(&f, "C2", LIVE_FACT, json!({"title":"deploys"})).await;
    let legacy_atom = extracted(&f, legacy.version_id, LIVE_FACT).await;
    let pending = pending_identical_recapture(&f, legacy.version_id, json!({"title":"deploys","url":"https://example.invalid/C2"})).await;
    for side in current_outcomes(&f, legacy.source_item_id).await {
        assert_eq!(side.len(), 1, "read fallback serves the identical extracted version");
        assert_eq!(side[0]["semantic_atom_id"], json!(legacy_atom));
    }

    // Repair: dry run reports, execute carries, a second run finds nothing.
    use coralswarm_backend::source_carry_forward::{carry_forward_uncarried, preview_uncarried};
    let preview = preview_uncarried(&f.owner, Some(f.graph), 100).await.unwrap();
    assert_eq!((preview.versions, preview.prior_atoms), (1, 1));
    let status: String = sqlx::query_scalar("SELECT extraction_status FROM source_versions WHERE version_id=$1").bind(pending).fetch_one(&f.owner).await.unwrap();
    assert_eq!(status, "pending", "the preview writes nothing");
    assert_eq!(carry_forward_uncarried(&f.owner, Some(f.graph), 100).await.unwrap(), 1);
    assert_eq!(carry_forward_uncarried(&f.owner, Some(f.graph), 100).await.unwrap(), 0, "idempotent");
    let (status, profile): (String, String) = sqlx::query_as("SELECT extraction_status,extraction_version FROM source_versions WHERE version_id=$1")
        .bind(pending).fetch_one(&f.owner).await.unwrap();
    assert_eq!((status.as_str(), profile.as_str()), ("done", "source-atoms-v2"));
    assert_eq!(current_outcomes(&f, legacy.source_item_id).await[0].len(), 1);

    // Deleting the source retracts every version's facts, carried or not.
    for id in ["C1", "C2"] {
        let mut tx = f.owner.begin().await.unwrap();
        let changed = coralswarm_backend::source_evidence::set_source_identity_access_tx(&mut tx, f.graph, "slack", "fixture:carry", id, "restricted").await.unwrap();
        assert_eq!(changed, 1);
        tx.commit().await.unwrap();
    }
    let all: LibraryScope = serde_json::from_value(json!({"graph_ids":[f.graph]})).unwrap();
    let page = library_read::retrieve_ask_candidates(&f.tenant, &f.user, all, &["backend".into()], None, None, 50).await.unwrap();
    assert!(!page.to_string().contains("0.9.4"), "a deleted source serves no fact from any version");
    f.done().await;
}
#[tokio::test]
async fn consumer_metadata_only_recapture_carries_extracted_facts() {
    carry_forward(false).await;
}
#[tokio::test]
async fn tenant_metadata_only_recapture_carries_extracted_facts() {
    carry_forward(true).await;
}

async fn served_facts_only(org: bool) {
    let Some(f) = Fixture::new(org).await else { return; };
    let first = capture(&f, "C1", LIVE_FACT, json!({"title":"deploys"})).await;
    let stale = extracted(&f, first.version_id, LIVE_FACT).await;
    let all: LibraryScope = serde_json::from_value(json!({"graph_ids":[f.graph]})).unwrap();
    let edited = capture(&f, "C1", "CoralSwarm backend 0.9.5 is live in production", json!({"title":"deploys"})).await;
    assert!(edited.changed && edited.version_id != first.version_id);
    let page = library_read::retrieve_ask_candidates(&f.tenant, &f.user, all.clone(), &["backend".into()], None, None, 50).await.unwrap();
    assert!(!outcome_ids(&page).contains(&json!(stale)), "a changed re-capture waits for its own extraction; the earlier fact is history: {page}");
    let current = extracted(&f, edited.version_id, "CoralSwarm backend 0.9.5 is live in production").await;
    let page = library_read::retrieve_ask_candidates(&f.tenant, &f.user, all.clone(), &["backend".into()], None, None, 50).await.unwrap();
    let ids = outcome_ids(&page);
    assert!(ids.contains(&json!(current)), "the current version's fact is ranked: {page}");
    assert!(!ids.contains(&json!(stale)), "the earlier version's repeated fact is not ranked: {page}");
    assert!(page["outcomes"].as_array().unwrap().iter().all(|o| o["source_version_is_current"] == json!(true)));
    assert_eq!(page["scope"]["outcome_versions"], "current", "the returned scope says what was read");

    pending_identical_recapture(&f, edited.version_id, json!({"title":"deploys"})).await;
    let page = library_read::retrieve_ask_candidates(&f.tenant, &f.user, all, &["backend".into()], None, None, 50).await.unwrap();
    let ids = outcome_ids(&page);
    assert!(ids.contains(&json!(current)), "the identical extracted version serves while the re-capture is pending: {page}");
    assert!(!ids.contains(&json!(stale)), "{page}");
    f.done().await;
}
#[tokio::test]
async fn consumer_ask_ranks_served_facts_only() {
    served_facts_only(false).await;
}
#[tokio::test]
async fn tenant_ask_ranks_served_facts_only() {
    served_facts_only(true).await;
}

/// The receipt fence under `scope`, on the app role as the Ask path runs it.
async fn fence(f: &Fixture, scope: &LibraryScope, support: &Value) -> bool {
    let mut tx = f.app.begin().await.unwrap();
    sqlx::query("SELECT set_config('search_path',$1,true),set_config('app.account_id',$2,true),set_config('app.user_id',$3,true)")
        .bind("\"$user\", public").bind(f.user.account_id.to_string()).bind(f.user.user_id.to_string()).execute(&mut *tx).await.unwrap();
    let allowed = library_read::ask_inputs_in_scope_tx(&mut tx, scope, support).await.unwrap();
    tx.rollback().await.unwrap();
    allowed
}

async fn receipt_fence_is_per_citation(org: bool) {
    let Some(f) = Fixture::new(org).await else { return; };
    let cited = capture(&f, "C1", LIVE_FACT, json!({"title":"deploys"})).await;
    let cited_atom = extracted(&f, cited.version_id, LIVE_FACT).await;
    let other = capture(&f, "C2", "CoralSwarm web 2.1 shipped", json!({"title":"web"})).await;
    let other_atom = extracted(&f, other.version_id, "CoralSwarm web 2.1 shipped").await;
    let support = json!([{"kind":"source_passage","graph_id":f.graph,"source_item_id":cited.source_item_id,
        "source_version_id":cited.version_id,"semantic_atom_id":cited_atom}]);
    let mut all: LibraryScope = serde_json::from_value(json!({"graph_ids":[f.graph]})).unwrap();
    all.normalize().unwrap();
    let mut served = all.clone();
    served.serve_current();
    assert!(fence(&f, &all, &support).await);
    assert!(fence(&f, &served, &support).await);

    // Another item in scope is re-extracted: its rows are replaced, the receipt stands.
    sqlx::query("DELETE FROM semantic_atom_passages WHERE semantic_atom_id=$1").bind(other_atom).execute(&f.owner).await.unwrap();
    sqlx::query("DELETE FROM semantic_atoms WHERE semantic_atom_id=$1").bind(other_atom).execute(&f.owner).await.unwrap();
    extracted(&f, other.version_id, "CoralSwarm web 2.2 shipped").await;
    assert!(fence(&f, &all, &support).await, "a re-extraction elsewhere in scope leaves the receipt valid");
    assert!(fence(&f, &served, &support).await);

    // The cited item is re-captured with new content: the citation is history.
    let edited = capture(&f, "C1", "CoralSwarm backend 0.9.5 is live in production", json!({"title":"deploys"})).await;
    extracted(&f, edited.version_id, "CoralSwarm backend 0.9.5 is live in production").await;
    assert!(fence(&f, &all, &support).await, "all_retained keeps an earlier version's citation");
    assert!(!fence(&f, &served, &support).await, "served-only reads do not");

    // Access to the cited item is lost, then the cited row itself is gone.
    sqlx::query("UPDATE source_items SET access_state='restricted' WHERE source_item_id=$1").bind(cited.source_item_id).execute(&f.owner).await.unwrap();
    assert!(!fence(&f, &all, &support).await, "a restricted item fails the fence");
    sqlx::query("UPDATE source_items SET access_state='permitted' WHERE source_item_id=$1").bind(cited.source_item_id).execute(&f.owner).await.unwrap();
    assert!(fence(&f, &all, &support).await);
    sqlx::query("DELETE FROM semantic_atom_passages WHERE semantic_atom_id=$1").bind(cited_atom).execute(&f.owner).await.unwrap();
    sqlx::query("DELETE FROM semantic_atoms WHERE semantic_atom_id=$1").bind(cited_atom).execute(&f.owner).await.unwrap();
    assert!(!fence(&f, &all, &support).await, "a deleted cited row fails the fence");
    f.done().await;
}
#[tokio::test]
async fn consumer_receipt_fence_is_per_citation() {
    receipt_fence_is_per_citation(false).await;
}
#[tokio::test]
async fn tenant_receipt_fence_is_per_citation() {
    receipt_fence_is_per_citation(true).await;
}

// ── COR-108: one interactive credit gate for HTTP and MCP Ask/search ──────────

async fn mcp_tool(f: &Fixture, state: &Arc<coralswarm_backend::AppState>, name: &str, arguments: Value) -> Value {
    coralswarm_backend::test_retained_mcp_dispatch(state.clone(), &f.tenant, &f.user, json!({"method":"tools/call","params":{"name":name,"arguments":arguments}}))
        .await
        .unwrap()
}
fn mcp_error_code(result: &Value) -> String {
    assert_eq!(result["isError"], true, "expected a tool error: {result}");
    let text = result["content"][0]["text"].as_str().expect("tool error text");
    serde_json::from_str::<Value>(text).unwrap()["error"]["code"].as_str().unwrap_or_default().to_owned()
}
async fn http_reason(response: axum::response::Response) -> (axum::http::StatusCode, Value) {
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    (status, serde_json::from_slice(&bytes).unwrap_or(Value::Null))
}
/// Rows any admitted read would leave behind on the content plane.
async fn read_footprint(f: &Fixture) -> Value {
    sqlx::query_scalar("SELECT jsonb_build_array((SELECT count(*) FROM mcp_tasks),(SELECT count(*) FROM conversations),(SELECT count(*) FROM query_history),(SELECT count(*) FROM llm_usage))")
        .fetch_one(&f.owner)
        .await
        .unwrap()
}

/// Blocked: MCP `search_atoms` and `ask_ocean` refuse with `payment_required`
/// before scope, tasks, embedding or model calls, the same as HTTP JSON and
/// SSE. Unreadable eligibility refuses as `service_unavailable` / 503 on all
/// four. Funded: the same canonical `LibraryScope` reads work, with scope
/// denial and receipts unchanged.
async fn interactive_credit_gate(org: bool) {
    let Some(f) = Fixture::new(org).await else { return };
    sqlx::query("UPDATE accounts SET billing_enforcement_enabled=true WHERE id=$1")
        .bind(f.user.account_id).execute(&f.control_owner).await.unwrap();
    f.finite_pool(0).await;
    let state = f.state(None);
    let footprint = read_footprint(&f).await;
    let scope = f.scope(Some(f.old.0));
    let denied: LibraryScope = serde_json::from_value(json!({"graph_ids":[f.denied_graph]})).unwrap();

    // ── Out of credits ────────────────────────────────────────────────────────
    let search = mcp_tool(&f, &state, "search_atoms", json!({"query":"Acme","library_scope":scope})).await;
    assert_eq!(mcp_error_code(&search), "payment_required", "{search}");
    let denied_search = mcp_tool(&f, &state, "search_atoms", json!({"query":"Acme","library_scope":denied})).await;
    assert_eq!(denied_search, search, "a credit refusal must not reveal which scopes exist");
    for style in ["sources-only", "concise"] {
        let ask = mcp_tool(&f, &state, "ask_ocean", json!({"question":"What did Acme decide?","library_scope":scope,"style":style})).await;
        assert_eq!(mcp_error_code(&ask), "payment_required", "{style}: {ask}");
    }
    let body = json!({"text":"What did Acme decide?","library_scope":scope,"style":"concise"});
    let (status, json_body) = http_reason(coralswarm_backend::test_scoped_query_json(state.clone(), f.tenant.clone(), f.user.clone(), body.clone()).await).await;
    assert_eq!((status, json_body["reason"].clone()), (axum::http::StatusCode::PAYMENT_REQUIRED, json!("insufficient_credits")));
    let (status, _) = http_reason(coralswarm_backend::test_scoped_query_stream(state.clone(), f.tenant.clone(), f.user.clone(), body.clone()).await).await;
    assert_eq!(status, axum::http::StatusCode::PAYMENT_REQUIRED);

    // ── Eligibility unreadable: fail closed, and say so ───────────────────────
    sqlx::query("REVOKE SELECT ON credit_balances FROM coralswarm_app").execute(&f.control_owner).await.unwrap();
    let search = mcp_tool(&f, &state, "search_atoms", json!({"query":"Acme","library_scope":scope})).await;
    assert_eq!(mcp_error_code(&search), "service_unavailable", "{search}");
    let ask = mcp_tool(&f, &state, "ask_ocean", json!({"question":"What did Acme decide?","library_scope":scope,"style":"concise"})).await;
    assert_eq!(mcp_error_code(&ask), "service_unavailable", "{ask}");
    let (status, json_body) = http_reason(coralswarm_backend::test_scoped_query_json(state.clone(), f.tenant.clone(), f.user.clone(), body.clone()).await).await;
    assert_eq!((status, json_body["reason"].clone()), (axum::http::StatusCode::SERVICE_UNAVAILABLE, json!("credit_eligibility_unavailable")));
    let (status, _) = http_reason(coralswarm_backend::test_scoped_query_stream(state.clone(), f.tenant.clone(), f.user.clone(), body.clone()).await).await;
    assert_eq!(status, axum::http::StatusCode::SERVICE_UNAVAILABLE);
    sqlx::query("GRANT SELECT ON credit_balances TO coralswarm_app").execute(&f.control_owner).await.unwrap();

    assert_eq!(read_footprint(&f).await, footprint, "refused reads leave no task, conversation, history or usage row");
    assert_eq!(f.calls.load(Ordering::SeqCst), 0, "a refused read calls no model");
    assert_eq!(f.embeddings.load(Ordering::SeqCst), 0, "a refused read embeds nothing");

    // ── Funded: the same reads work, with scope and receipts intact ───────────
    f.finite_pool(500).await;
    let search = mcp_tool(&f, &state, "search_atoms", json!({"query":"Acme","library_scope":scope})).await;
    assert_ne!(search["isError"], true, "{search}");
    assert_eq!(search["structuredContent"]["mode"], "retrieval_only");
    let denied_search = mcp_tool(&f, &state, "search_atoms", json!({"query":"Acme","library_scope":denied})).await;
    assert_eq!(mcp_error_code(&denied_search), "not_found", "a funded caller still cannot read a private graph");
    let sources = mcp_tool(&f, &state, "ask_ocean", json!({"question":"What did Acme decide?","library_scope":scope,"style":"sources-only"})).await;
    assert_ne!(sources["isError"], true, "{sources}");
    assert_eq!(sources["structuredContent"]["support_status"], "complete");
    assert_eq!((f.calls.load(Ordering::SeqCst), f.embeddings.load(Ordering::SeqCst)), (0, 0), "sources-only stays free");
    let answer = mcp_tool(&f, &state, "ask_ocean", json!({"question":"What did Acme decide?","library_scope":scope,"style":"concise"})).await;
    assert_ne!(answer["isError"], true, "{answer}");
    assert_eq!(answer["structuredContent"]["support_status"], "complete", "{answer}");
    assert!(f.calls.load(Ordering::SeqCst) > 0, "a funded concise Ask reaches the answer model");
    assert!(f.embeddings.load(Ordering::SeqCst) > 0, "a funded concise Ask embeds its query, so the refused zero above is meaningful");
    let (status, _) = http_reason(coralswarm_backend::test_scoped_query_json(state.clone(), f.tenant.clone(), f.user.clone(), body.clone()).await).await;
    assert_eq!(status, axum::http::StatusCode::OK);
    f.done().await;
}
#[tokio::test]
async fn consumer_interactive_credit_gate_is_shared_by_http_and_mcp() {
    interactive_credit_gate(false).await;
}
#[tokio::test]
async fn tenant_interactive_credit_gate_is_shared_by_http_and_mcp() {
    interactive_credit_gate(true).await;
}

// ---------------------------------------------------------------------------
// A saved question is never left without a reply, and transient failures are
// retried without asking the model (or the user) twice.
// ---------------------------------------------------------------------------
async fn failure_body(response: axum::response::Response) -> (axum::http::StatusCode, Value) {
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), 1 << 20).await.unwrap();
    (status, serde_json::from_slice(&bytes).unwrap_or(Value::Null))
}
fn synthesizing(f: &Fixture) -> AskRequest {
    let mut request = f.request(None);
    request.style = Some("concise".into());
    request
}
async fn thread_rows(f: &Fixture, conv: Uuid) -> Vec<(i32, String, Option<String>)> {
    sqlx::query_as("SELECT seq,role,mode FROM conversation_messages WHERE conversation_id=$1 ORDER BY seq")
        .bind(conv).fetch_all(&f.owner).await.unwrap()
}
fn with_fault(request: AskRequest, phase: &'static str, times: u32, reason: &'static str, transient: bool) -> AskRequest {
    scoped_ask::test_request_fault(request, scoped_ask::FaultPlan::new(phase, times, reason, transient))
}

async fn ask_never_orphans(org: bool) {
    let Some(f) = Fixture::new(org).await else { return };
    let state = f.state(None);
    coralswarm_backend::test_modern_cached_task_admission(&state, &f.tenant, &f.user).await;

    // 1. A busy database during retrieval is retried; one question, one answer.
    let answer = scoped_ask::execute(&state, &f.tenant, &f.user, with_fault(synthesizing(&f), "retrieve", 2, "db_busy", true)).await.ok().expect("retried to success");
    let conv: Uuid = serde_json::from_value(answer.payload["conversation_id"].clone()).unwrap();
    assert_eq!(thread_rows(&f, conv).await, vec![(0, "user".into(), None), (1, "assistant".into(), Some("synthesized".into()))],
        "retries must not insert the question again");

    // 2. Only the write is busy: the answer is kept, so the model is asked once.
    let calls_before = f.calls.load(Ordering::SeqCst);
    let mut next = with_fault(synthesizing(&f), "write", 2, "db_busy", true);
    next.conversation_id = Some(conv);
    let answer = scoped_ask::execute(&state, &f.tenant, &f.user, next).await.ok().expect("write retried");
    assert_eq!(f.calls.load(Ordering::SeqCst) - calls_before, 1, "a retried write must not re-bill the model");
    assert_eq!(answer.payload["new_conversation"], json!(false));
    assert_eq!(thread_rows(&f, conv).await.len(), 4);

    // 3. Evidence changed at the write: retrieval and synthesis run again.
    let calls_before = f.calls.load(Ordering::SeqCst);
    let mut next = with_fault(synthesizing(&f), "write", 1, "evidence_changed", true);
    next.conversation_id = Some(conv);
    scoped_ask::execute(&state, &f.tenant, &f.user, next).await.ok().expect("re-synthesized");
    assert_eq!(f.calls.load(Ordering::SeqCst) - calls_before, 2, "changed evidence is re-synthesized");
    assert_eq!(thread_rows(&f, conv).await.len(), 6);

    // 4. A failure that outlasts the retries is recorded as the question's reply.
    let mut doomed = with_fault(synthesizing(&f), "retrieve", 99, "db_busy", true);
    doomed.conversation_id = Some(conv);
    let queries_before: i64 = sqlx::query_scalar("SELECT count(*) FROM query_history").fetch_one(&f.owner).await.unwrap();
    let Err(response) = scoped_ask::execute(&state, &f.tenant, &f.user, doomed).await else { panic!("must fail") };
    let (status, body) = failure_body(response).await;
    assert_eq!(status, axum::http::StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["reason"], "db_busy");
    assert_eq!(body["attempts"], json!(3));
    assert_eq!(body["conversation_id"], json!(conv));
    let question: Uuid = serde_json::from_value(body["question_message_id"].clone()).unwrap();
    let failed: Uuid = serde_json::from_value(body["failed_message_id"].clone()).expect("the failure was recorded");
    let rows = thread_rows(&f, conv).await;
    assert_eq!(&rows[6..], &[(6, "user".to_string(), None), (7, "assistant".to_string(), Some("failed".to_string()))]);
    let (text, timing): (String, Value) = sqlx::query_as("SELECT text,timing FROM conversation_messages WHERE id=$1").bind(failed).fetch_one(&f.owner).await.unwrap();
    assert_eq!(text, "");
    assert_eq!(timing["support_status"], "failed");
    assert_eq!(timing["failure_reason"], "db_busy");
    assert_eq!(timing["failure_phase"], "retrieve");
    assert!(timing.get("outcomes").is_none() && timing.get("retained_citations").is_none(), "no evidence on a failed reply");
    assert_eq!(sqlx::query_scalar::<_, i64>("SELECT count(*) FROM query_history").fetch_one(&f.owner).await.unwrap(), queries_before,
        "a failed ask is not a recorded answer");

    // 5. Readers: the failed reply is a failure, never an answer with evidence.
    let (status, page) = conversation_page(&f, conv, None).await;
    assert_eq!(status, axum::http::StatusCode::OK);
    let reply = message_at(&page, 7);
    assert_eq!(reply["support_status"], "failed");
    assert_eq!(reply["failure_reason"], "db_busy");
    assert_eq!(reply["question_message_id"], json!(question));
    assert_eq!(reply["text"], "");
    assert_eq!(reply["retained_citations"], json!([]));
    assert_eq!(reply["outcomes"], json!([]));
    assert_eq!(reply["legacy_evidence"], json!([]));
    assert_eq!(message_at(&page, 5)["support_status"], "complete", "real answers are unaffected");

    // 6. Retry answers the same saved question: no second copy, failed reply replaced.
    let mut retry = synthesizing(&f);
    retry.conversation_id = Some(conv);
    retry.retry_message_id = Some(question);
    retry.text = "ignored: the saved text is the authority".into();
    let retried = scoped_ask::execute(&state, &f.tenant, &f.user, retry).await.ok().expect("retry answers");
    assert_eq!(retried.payload["new_conversation"], json!(false));
    let rows = thread_rows(&f, conv).await;
    assert_eq!(rows.len(), 8);
    assert_eq!(rows[7], (7, "assistant".to_string(), Some("synthesized".to_string())));
    assert_eq!(sqlx::query_scalar::<_, i64>("SELECT count(*) FROM conversation_messages WHERE id=$1 AND text='What did Acme decide?'").bind(question).fetch_one(&f.owner).await.unwrap(), 1);
    assert_eq!(sqlx::query_scalar::<_, i64>("SELECT count(*) FROM answer_support_receipts WHERE receipt_id=$1").bind(failed).fetch_one(&f.owner).await.unwrap(), 0,
        "the replaced failure record leaves no receipt behind");
    let (_, page) = conversation_page(&f, conv, None).await;
    assert_eq!(message_at(&page, 7)["support_status"], "complete");

    // 7. A retry is only for the latest unanswered question, in this chat, under its scope.
    let mut again = synthesizing(&f);
    again.conversation_id = Some(conv);
    again.retry_message_id = Some(question);
    let Err(response) = scoped_ask::execute(&state, &f.tenant, &f.user, again).await else { panic!("answered question") };
    assert_eq!(failure_body(response).await.0, axum::http::StatusCode::CONFLICT);
    let mut stranger = synthesizing(&f);
    stranger.conversation_id = Some(conv);
    stranger.retry_message_id = Some(Uuid::new_v4());
    let Err(response) = scoped_ask::execute(&state, &f.tenant, &f.user, stranger).await else { panic!("unknown question") };
    assert_eq!(failure_body(response).await.0, axum::http::StatusCode::NOT_FOUND);
    let mut loose = synthesizing(&f);
    loose.retry_message_id = Some(question);
    let Err(response) = scoped_ask::execute(&state, &f.tenant, &f.user, loose).await else { panic!("no conversation") };
    assert_eq!(failure_body(response).await.0, axum::http::StatusCode::BAD_REQUEST);

    // 8. A genuine conflict is not retried, and still leaves a reply.
    let mut conflict = with_fault(synthesizing(&f), "write", 99, "conversation_changed", false);
    conflict.conversation_id = Some(conv);
    let Err(response) = scoped_ask::execute(&state, &f.tenant, &f.user, conflict).await else { panic!("conflict") };
    let (status, body) = failure_body(response).await;
    assert_eq!(status, axum::http::StatusCode::CONFLICT);
    assert_eq!((body["reason"].clone(), body["attempts"].clone()), (json!("conversation_changed"), json!(1)));
    assert!(body["failed_message_id"].is_string());

    // 9. A legacy orphan (a question saved with no reply and no record) is retried in place.
    let orphan: Uuid = serde_json::from_value(body["question_message_id"].clone()).unwrap();
    sqlx::query("WITH d AS (DELETE FROM conversation_messages WHERE id=$1 RETURNING id) DELETE FROM answer_support_receipts WHERE receipt_id IN (SELECT id FROM d)")
        .bind(Uuid::parse_str(body["failed_message_id"].as_str().unwrap()).unwrap()).execute(&f.owner).await.unwrap();
    let last = thread_rows(&f, conv).await.pop().unwrap();
    assert_eq!((last.1.as_str(), last.2), ("user", None), "the question is orphaned");
    let mut fill = synthesizing(&f);
    fill.conversation_id = Some(conv);
    fill.retry_message_id = Some(orphan);
    scoped_ask::execute(&state, &f.tenant, &f.user, fill).await.ok().expect("orphan answered");
    let rows = thread_rows(&f, conv).await;
    assert_eq!(rows.iter().filter(|r| r.1 == "user").count(), rows.iter().filter(|r| r.1 == "assistant").count(), "every question has a reply");
    assert!(rows.iter().all(|r| r.2.as_deref() != Some("failed")));
    f.done().await;
}
#[tokio::test]
async fn consumer_ask_never_leaves_a_question_unanswered() {
    ask_never_orphans(false).await;
}
#[tokio::test]
async fn tenant_ask_never_leaves_a_question_unanswered() {
    ask_never_orphans(true).await;
}
