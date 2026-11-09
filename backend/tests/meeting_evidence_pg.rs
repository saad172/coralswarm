//! Immutable source capture, scoped evidence and pagination against local PG:
//! `RELINFER_TEST_DATABASE_URL=postgres://...@127.0.0.1:.../scratch cargo test
//! -p coralswarm-backend --test meeting_list_pg -- --nocapture`.

mod support;

use std::{str::FromStr, sync::Arc};

use axum::{body::Body, http::Request, http::StatusCode, Router};
use chrono::{DateTime, Duration, Utc};
use coralswarm_backend::clerk::test_support::{mint, test_jwks, ISSUER};
use serde_json::{json, Value};
use sqlx::{postgres::PgConnectOptions, postgres::PgPoolOptions, PgPool};
use tower::ServiceExt;
use uuid::Uuid;

async fn seed_account(pool: &PgPool) -> (Uuid, Uuid, Uuid, String) {
    let sub = format!("meeting-list-{}", Uuid::new_v4());
    let user_id: Uuid =
        sqlx::query_scalar("INSERT INTO users(email, clerk_user_id) VALUES ($1, $2) RETURNING id")
            .bind(format!("{sub}@test.local"))
            .bind(&sub)
            .fetch_one(pool)
            .await
            .unwrap();
    let account_id: Uuid =
        sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES ($1) RETURNING id")
            .bind(user_id)
            .fetch_one(pool)
            .await
            .unwrap();
    sqlx::query("INSERT INTO account_members(account_id, user_id, role) VALUES ($1, $2, 'owner')")
        .bind(account_id)
        .bind(user_id)
        .execute(pool)
        .await
        .unwrap();
    let graph_id: Uuid = sqlx::query_scalar(
        "INSERT INTO graphs(account_id, name) VALUES ($1, 'meeting-list-test') RETURNING id",
    )
    .bind(account_id)
    .fetch_one(pool)
    .await
    .unwrap();
    (user_id, account_id, graph_id, sub)
}

async fn attach_fixture_source(pool: &PgPool, graph: Uuid, source: &str) -> Uuid {
    let item:Uuid=sqlx::query_scalar("INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id) VALUES($1,'transcript','legacy:meeting',$2) ON CONFLICT(graph_id,source_type,provider_namespace,source_id) DO UPDATE SET source_id=EXCLUDED.source_id RETURNING source_item_id")
        .bind(graph).bind(source).fetch_one(pool).await.unwrap();
    sqlx::query(
        "UPDATE meeting_transcripts SET source_item_id=$3 WHERE graph_id=$1 AND source_id=$2",
    )
    .bind(graph)
    .bind(source)
    .bind(item)
    .execute(pool)
    .await
    .unwrap();
    item
}

async fn get(app: &Router, token: Option<&str>, uri: &str) -> (StatusCode, Value) {
    let mut request = Request::builder().uri(uri);
    if let Some(token) = token {
        request = request.header("authorization", format!("Bearer {token}"));
    }
    let response = app
        .clone()
        .oneshot(request.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), 1_000_000)
        .await
        .unwrap();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

#[derive(Default)]
struct RecordingBlobs(std::sync::Mutex<Vec<String>>);
#[async_trait::async_trait]
impl coralswarm_backend::services::blob_store::PromotionBlobStore for RecordingBlobs {
    async fn get_blob(
        &self,
        _key: &str,
        _dek: &[u8; 32],
    ) -> Result<Vec<u8>, coralswarm_core::r2::R2Error> {
        Ok(vec![])
    }
    async fn put_blob(
        &self,
        _key: &str,
        _content: &[u8],
        _dek: &[u8; 32],
    ) -> Result<(), coralswarm_core::r2::R2Error> {
        Ok(())
    }
    async fn delete_blob(&self, key: &str) -> Result<(), coralswarm_core::r2::R2Error> {
        self.0.lock().unwrap().push(key.to_string());
        Ok(())
    }
}

async fn post(app: &Router, token: &str, uri: &str, body: Value) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), 1_000_000)
        .await
        .unwrap();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}
#[tokio::test]
async fn meeting_evidence_capture_history_authorization_and_retry_are_durable() {
    let Ok(url) = std::env::var("RELINFER_TEST_DATABASE_URL") else {
        assert!(
            std::env::var_os("REQUIRE_PG_SUITE").is_none(),
            "RELINFER_TEST_DATABASE_URL required"
        );
        eprintln!("skipping meeting evidence test: set RELINFER_TEST_DATABASE_URL");
        return;
    };
    let opts = PgConnectOptions::from_str(&url).unwrap();
    assert!(matches!(opts.get_host(), "127.0.0.1" | "localhost" | "::1"));
    let owner = PgPoolOptions::new()
        .max_connections(4)
        .connect_with(opts.clone())
        .await
        .unwrap();
    support::app_role::set_app_role_password_with_retry(&owner).await;
    let app_pool = PgPoolOptions::new()
        .max_connections(6)
        .connect_with(
            opts.username("coralswarm_app")
                .password(support::app_role::APP_ROLE_TEST_PASSWORD),
        )
        .await
        .unwrap();
    let a = seed_account(&owner).await;
    let b = seed_account(&owner).await;
    let jwks = test_jwks();
    let jwks_app = Router::new().route(
        "/.well-known/jwks.json",
        axum::routing::get(move || {
            let body = jwks.clone();
            async move { ([("content-type", "application/json")], body) }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let jwks_url = format!(
        "http://{}/.well-known/jwks.json",
        listener.local_addr().unwrap()
    );
    let jwks_task = tokio::spawn(async move { axum::serve(listener, jwks_app).await.unwrap() });
    unsafe {
        std::env::set_var("CLERK_ISSUER", ISSUER);
        std::env::set_var("CLERK_JWKS_URL", jwks_url);
    }
    let state = Arc::new(coralswarm_backend::test_app_state_owned_with_db_and_store(
        app_pool.clone(),
        None,
    ));
    state.dek_cache.insert(
        &coralswarm_backend::tenancy::TenantId::Consumer,
        a.1,
        [42; 32],
    );
    let app = coralswarm_backend::test_support_app(state.clone());
    let token_a =
        mint(json!({"iss":ISSUER,"sub":a.3,"exp":(Utc::now()+Duration::hours(1)).timestamp()}));
    let token_b =
        mint(json!({"iss":ISSUER,"sub":b.3,"exp":(Utc::now()+Duration::hours(1)).timestamp()}));
    let source_id = "meeting-evidence";
    let text_a = "Ada: Café 🌊 will ship Friday.\nBob: I will prepare the launch.";
    let text_b = "Ada: Café 🌊 will ship Monday.\nBob: I will prepare the launch.";
    let ingest = |text: &str| json!({"graph_id":a.2,"source_type":"teams_transcript","source_id":source_id,"content":text,"timestamp":"2026-10-01T12:00:00Z","participants":["Ada","Bob"]});
    for text in [text_a, text_a, text_b, text_a] {
        let (status, body) = post(&app, &token_a, "/v1/ingest/atom", ingest(text)).await;
        assert!(status.is_success(), "ingest {status}: {body}");
    }
    let versions:Vec<(Uuid,String)>=sqlx::query_as("SELECT version_id,raw_inline FROM source_versions WHERE graph_id=$1 AND source_id=$2 ORDER BY captured_at,version_id").bind(a.2).bind(source_id).fetch_all(&owner).await.unwrap();
    assert_eq!(
        versions.len(),
        3,
        "A→A→B→A preserves three snapshots even when retrieval chunks deduplicate"
    );
    assert_eq!(
        versions.iter().map(|v| v.1.as_str()).collect::<Vec<_>>(),
        [text_a, text_b, text_a]
    );
    let queued: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM jobs WHERE kind='ExtractSourceAtoms' AND account_id=$1",
    )
    .bind(a.1)
    .fetch_one(&owner)
    .await
    .unwrap();
    assert_eq!(
        queued, 0,
        "ordinary capture retains evidence without minting an unmanaged extraction job"
    );
    let admissions:Vec<(String,Option<String>,String)>=sqlx::query_as("SELECT a.state,a.reason,v.extraction_status FROM corpus_capture_admissions a JOIN source_versions v ON v.version_id=a.source_version_id WHERE a.graph_id=$1").bind(a.2).fetch_all(&owner).await.unwrap();
    assert_eq!(admissions.len(), 3, "every retained snapshot records its admission");
    assert!(
        admissions.iter().all(|r| r
            == &(
                "blocked".to_string(),
                Some("managed_admission_required".to_string()),
                "blocked".to_string()
            )),
        "{admissions:?}"
    );
    // A legacy current transcript is preserved at observation time, without
    // inventing an original capture time from its long-lived parent row.
    let legacy_original = Utc::now() - Duration::days(30);
    sqlx::query("INSERT INTO meeting_transcripts(graph_id,source_id,raw_inline,created_at,segments) VALUES($1,'legacy-source','Ada: Legacy text.',$2,$3)").bind(a.2).bind(legacy_original).bind(json!([{"speaker":"Ada","start_ms":0,"end_ms":100,"text":"Legacy text."}])).execute(&owner).await.unwrap();
    let mut legacy_ingest = ingest("Ada: Legacy text.");
    legacy_ingest["source_id"] = json!("legacy-source");
    assert!(
        post(&app, &token_a, "/v1/ingest/atom", legacy_ingest.clone())
            .await
            .0
            .is_success()
    );
    let preserved:(Uuid,bool,DateTime<Utc>,Value)=sqlx::query_as("SELECT version_id,is_legacy_baseline,captured_at,segments FROM source_versions WHERE graph_id=$1 AND source_id='legacy-source' AND is_legacy_baseline").bind(a.2).fetch_one(&owner).await.unwrap();
    assert!(preserved.1);
    assert!(preserved.2 > legacy_original + Duration::days(29));
    legacy_ingest["content"] = json!("Ada: Changed legacy text.");
    assert!(post(&app, &token_a, "/v1/ingest/atom", legacy_ingest)
        .await
        .0
        .is_success());
    let latest_segments: Option<Value> = sqlx::query_scalar(
        "SELECT v.segments FROM source_versions v JOIN source_items s ON s.current_version_id=v.version_id WHERE s.graph_id=$1 AND s.source_type='teams_transcript' AND s.source_id='legacy-source'",
    ).bind(a.2).fetch_one(&owner).await.unwrap();
    assert!(
        latest_segments.is_none(),
        "a separately identified provider source must not borrow legacy timing segments"
    );
    let projection_segments: Value = sqlx::query_scalar(
        "SELECT segments FROM meeting_transcripts WHERE graph_id=$1 AND source_id='legacy-source'",
    )
    .bind(a.2)
    .fetch_one(&owner)
    .await
    .unwrap();
    assert_eq!(
        projection_segments, preserved.3,
        "unknown legacy projection must remain attached to its own evidence"
    );
    let old_segments: Value =
        sqlx::query_scalar("SELECT segments FROM source_versions WHERE version_id=$1")
            .bind(preserved.0)
            .fetch_one(&owner)
            .await
            .unwrap();
    assert_eq!(old_segments, preserved.3);
    let uri = format!(
        "/v1/meetings/evidence?graph_id={}&source_id={source_id}",
        a.2
    );
    assert_eq!(get(&app, None, &uri).await.0, StatusCode::UNAUTHORIZED);
    assert_eq!(
        get(&app, Some(&token_b), &uri).await.0,
        StatusCode::NOT_FOUND
    );
    let (status, body) = get(&app, Some(&token_a), &uri).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["selected_version"]["raw_text"], text_a);
    assert_eq!(body["versions"].as_array().unwrap().len(), 3);
    assert!(!body.to_string().contains("raw_blob_key"));
    let item = body["source_item_id"].as_str().unwrap();
    let generic_uri = format!(
        "/v1/sources/evidence?graph_id={}&source_item_id={item}",
        a.2
    );
    let (generic_status, generic) = get(&app, Some(&token_a), &generic_uri).await;
    assert_eq!(generic_status, StatusCode::OK);
    assert_eq!(generic["source"]["source_type"], "teams_transcript");
    assert_eq!(
        generic["selected_version"]["representation"],
        "normalized_text"
    );
    assert_eq!(
        get(&app, Some(&token_b), &generic_uri).await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        get(
            &app,
            Some(&token_a),
            &format!(
                "/v1/sources/evidence?graph_id={}&source_item_id={}&version_id={}",
                a.2,
                Uuid::new_v4(),
                versions[0].0
            )
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    let mapped_chunk: Uuid = sqlx::query_scalar(
        "SELECT atom_id FROM source_version_chunks WHERE source_version_id=$1 LIMIT 1",
    )
    .bind(versions[0].0)
    .fetch_one(&owner)
    .await
    .unwrap();
    let (inventory_status, inventory) = get(
        &app,
        Some(&token_a),
        &format!("/v1/sources/items?graph_id={}&atom_id={mapped_chunk}", a.2),
    )
    .await;
    assert_eq!(inventory_status, StatusCode::OK);
    assert_eq!(inventory["items"].as_array().unwrap().len(), 1);
    assert_eq!(inventory["items"][0]["source_item_id"], item);
    // Content-verified positional ingest: the B capture replaced the chunk
    // in place and the final A capture replaced it back, each linking it as
    // verified evidence of its own snapshot. The latest mapping is therefore
    // the final A snapshot, whose text the chunk holds now. (Before, the
    // chunk silently kept A's text through the B capture and only the first
    // snapshot was mapped.)
    assert_eq!(
        inventory["items"][0]["mapped_version_id"],
        versions[2].0.to_string(),
        "the chunk maps to the latest snapshot whose text it verifiably holds"
    );
    let mapped_to: Vec<Uuid> = sqlx::query_scalar(
        "SELECT source_version_id FROM source_version_chunks WHERE atom_id=$1",
    )
    .bind(mapped_chunk)
    .fetch_all(&owner)
    .await
    .unwrap();
    assert_eq!(mapped_to.len(), 3, "one verified link per snapshot");
    let (_, missing_mapping) = get(
        &app,
        Some(&token_a),
        &format!(
            "/v1/sources/items?graph_id={}&atom_id={}",
            a.2,
            Uuid::new_v4()
        ),
    )
    .await;
    assert!(missing_mapping["items"].as_array().unwrap().is_empty());
    let historic = format!("{uri}&version_id={}", versions[1].0);
    let (_, body) = get(&app, Some(&token_a), &historic).await;
    assert_eq!(body["selected_version"]["raw_text"], text_b);
    assert_eq!(
        get(
            &app,
            Some(&token_a),
            &format!(
                "/v1/meetings/evidence?graph_id={}&source_id=wrong&version_id={}",
                a.2, versions[0].0
            )
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        get(
            &app,
            Some(&token_a),
            &format!("{uri}&version_id={}", Uuid::new_v4())
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    // Version history is bounded and cursor-bound to its exact source.
    sqlx::query("INSERT INTO meeting_transcripts(graph_id,source_id) VALUES($1,'page-source')")
        .bind(a.2)
        .execute(&owner)
        .await
        .unwrap();
    attach_fixture_source(&owner, a.2, "page-source").await;
    sqlx::query("INSERT INTO source_versions(graph_id,source_id,source_item_id,raw_inline,captured_at) SELECT $1,'page-source',(SELECT source_item_id FROM meeting_transcripts WHERE graph_id=$1 AND source_id='page-source'),'page text',now()+(n*interval '1 second') FROM generate_series(1,23) n").bind(a.2).execute(&owner).await.unwrap();
    let pages_uri = format!(
        "/v1/meetings/evidence?graph_id={}&source_id=page-source",
        a.2
    );
    let (_, first) = get(&app, Some(&token_a), &pages_uri).await;
    assert_eq!(first["versions"].as_array().unwrap().len(), 20);
    let cursor = first["next_cursor"].as_str().unwrap();
    let (_, last) = get(
        &app,
        Some(&token_a),
        &format!("{pages_uri}&before={cursor}"),
    )
    .await;
    assert_eq!(last["versions"].as_array().unwrap().len(), 3);
    assert!(last["next_cursor"].is_null());
    assert_eq!(
        get(&app, Some(&token_a), &format!("{uri}&before={cursor}"))
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    // Failure to record the capture's managed admission rolls back the ENTIRE
    // ingest; preserving a source is never reported as success without the
    // durable admission record that gates its extraction job.
    let trigger = format!("evidence_admission_fail_{}", Uuid::new_v4().simple());
    sqlx::raw_sql(&format!("CREATE FUNCTION {trigger}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.graph_id='{}' THEN RAISE EXCEPTION 'test admission failure'; END IF; RETURN NEW; END $$; CREATE TRIGGER {trigger} BEFORE INSERT ON corpus_capture_admissions FOR EACH ROW EXECUTE FUNCTION {trigger}();",a.2)).execute(&owner).await.unwrap();
    let mut failed_ingest = ingest("Ada: This capture must roll back.");
    failed_ingest["source_id"] = json!("queue-failure-source");
    let (status, _) = post(&app, &token_a, "/v1/ingest/atom", failed_ingest).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    sqlx::raw_sql(&format!(
        "DROP TRIGGER {trigger} ON corpus_capture_admissions; DROP FUNCTION {trigger}();"
    ))
    .execute(&owner)
    .await
    .unwrap();
    let failed_rows:i64=sqlx::query_scalar("SELECT count(*) FROM meeting_transcripts WHERE graph_id=$1 AND source_id='queue-failure-source'").bind(a.2).fetch_one(&owner).await.unwrap();
    assert_eq!(failed_rows, 0);
    let failed_versions:i64=sqlx::query_scalar("SELECT count(*) FROM source_versions WHERE graph_id=$1 AND source_id='queue-failure-source'").bind(a.2).fetch_one(&owner).await.unwrap();
    assert_eq!(failed_versions, 0);
    let failed_chunks: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM atoms WHERE graph_id=$1 AND source_id LIKE 'queue-failure-source:%'",
    )
    .bind(a.2)
    .fetch_one(&owner)
    .await
    .unwrap();
    assert_eq!(failed_chunks, 0);
    // Real non-BYPASSRLS shared member access scopes to the graph owner account.
    sqlx::query("INSERT INTO account_members(account_id,user_id,role) VALUES($1,$2,'member')")
        .bind(a.1)
        .bind(b.0)
        .execute(&owner)
        .await
        .unwrap();
    assert_eq!(get(&app, Some(&token_b), &historic).await.0, StatusCode::OK);
    // A retry stays held until a running managed run owns the version.
    let retry = json!({"graph_id":a.2,"source_id":source_id,"version_id":versions[1].0});
    let (_, body) = get(&app, Some(&token_a), &historic).await;
    assert_eq!(body["selected_version"]["extraction_status"], "blocked");
    assert_eq!(body["selected_version"]["can_retry"], true);
    let (_, held) = post(&app, &token_a, "/v1/meetings/evidence/retry", retry.clone()).await;
    assert_eq!(held["accepted"], false, "{held}");
    assert_eq!(
        held["blocked_reason"],
        "managed_admission_required_or_run_paused"
    );
    // Managed execution also needs the identity contract phase (the legacy
    // title index dropped); the operator script is the real transition.
    let mut contract = owner.acquire().await.unwrap();
    sqlx::query("SET app.identity_serving_contract='composite-v1'")
        .execute(&mut *contract)
        .await
        .unwrap();
    sqlx::raw_sql(include_str!(
        "../schema/identity-contract.sql"
    ))
    .execute(&mut *contract)
    .await
    .unwrap();
    drop(contract);
    let run:Uuid=sqlx::query_scalar("INSERT INTO corpus_enrichment_runs(graph_id,account_id,requested_by,mode,status,extraction_version,model,max_items,max_characters,max_model_calls) VALUES($1,$2,$3,'execute','running','source-atoms-v2','pinned-test-model',10,50000,10) RETURNING run_id").bind(a.2).bind(a.1).bind(a.0).fetch_one(&owner).await.unwrap();
    sqlx::query("INSERT INTO corpus_enrichment_units(run_id,graph_id,kind,holding_id,source_item_id,extraction_version,state,original_availability,execution_owner) VALUES($1,$2,'retained_version',$3,$4,'source-atoms-v2','pending','available',true)").bind(run).bind(a.2).bind(versions[1].0).bind(Uuid::parse_str(item).unwrap()).execute(&owner).await.unwrap();
    assert_eq!(
        post(&app, &token_a, "/v1/meetings/evidence/retry", retry.clone())
            .await
            .1["accepted"],
        true
    );
    let admitted:(String,Option<Uuid>)=sqlx::query_as("SELECT state,run_id FROM corpus_capture_admissions WHERE source_version_id=$1").bind(versions[1].0).fetch_one(&owner).await.unwrap();
    assert_eq!(admitted, ("queued".to_string(), Some(run)));
    // Once admitted, a dead worker cannot leave the UI permanently running,
    // and retries debounce.
    sqlx::query("UPDATE source_versions SET extraction_status='running' WHERE version_id=$1")
        .bind(versions[1].0)
        .execute(&owner)
        .await
        .unwrap();
    sqlx::query("UPDATE jobs SET status='dead',idempotency_key=NULL WHERE kind='ExtractSourceAtoms' AND payload->>'source_version_id'=$1").bind(versions[1].0.to_string()).execute(&owner).await.unwrap();
    let (_, body) = get(&app, Some(&token_a), &historic).await;
    assert_eq!(body["selected_version"]["extraction_status"], "failed");
    assert_eq!(body["selected_version"]["can_retry"], true);
    assert_eq!(
        post(&app, &token_a, "/v1/meetings/evidence/retry", retry.clone())
            .await
            .1["accepted"],
        true
    );
    assert_eq!(
        post(&app, &token_a, "/v1/meetings/evidence/retry", retry)
            .await
            .1["accepted"],
        false
    );
    // Semantic evidence points to exact scalar ranges in its immutable version.
    let node:Uuid=sqlx::query_scalar("INSERT INTO nodes(graph_id,type,title) VALUES($1,'entity','Café project') RETURNING node_id").bind(a.2).fetch_one(&owner).await.unwrap();
    for ordinal in 0..105i32 {
        let atom = Uuid::new_v4();
        let quote = "Café 🌊";
        let start = text_b.chars().position(|c| c == 'C').unwrap() as i32;
        sqlx::query("INSERT INTO semantic_atoms(semantic_atom_id,graph_id,source_version_id,ordinal,kind,statement,extraction_version,model) VALUES($1,$2,$3,$4,'decision','Ship Monday','test','mock')").bind(atom).bind(a.2).bind(versions[1].0).bind(ordinal).execute(&owner).await.unwrap();
        sqlx::query("INSERT INTO semantic_atom_passages(semantic_atom_id,source_version_id,graph_id,start_char,end_char,quote) VALUES($1,$2,$3,$4,$5,$6)").bind(atom).bind(versions[1].0).bind(a.2).bind(start).bind(start+quote.chars().count() as i32).bind(quote).execute(&owner).await.unwrap();
        sqlx::query(
            "INSERT INTO node_semantic_atoms(node_id,semantic_atom_id,graph_id) VALUES($1,$2,$3)",
        )
        .bind(node)
        .bind(atom)
        .bind(a.2)
        .execute(&owner)
        .await
        .unwrap();
    }
    let (_, body) = get(&app, Some(&token_a), &historic).await;
    assert_eq!(
        body["selected_version"]["atoms"].as_array().unwrap().len(),
        100
    );
    let cursor = body["selected_version"]["atoms_next_cursor"]
        .as_str()
        .unwrap();
    let (_, page) = get(
        &app,
        Some(&token_a),
        &format!("{historic}&atoms_after={cursor}"),
    )
    .await;
    assert_eq!(
        page["selected_version"]["atoms"].as_array().unwrap().len(),
        5
    );
    let target = page["selected_version"]["atoms"][4]["atom_id"]
        .as_str()
        .unwrap();
    let (_, anchored) = get(
        &app,
        Some(&token_a),
        &format!("{historic}&atom_id={target}"),
    )
    .await;
    assert_eq!(anchored["selected_version"]["atoms"][0]["atom_id"], target);
    let reverse = format!("/v1/nodes/{node}/semantic-atoms?graph_id={}", a.2);
    let (status, reverse_body) = get(&app, Some(&token_a), &reverse).await;
    assert_eq!(status, StatusCode::OK, "{reverse_body}");
    assert_eq!(reverse_body["items"].as_array().unwrap().len(), 20);
    assert!(reverse_body["next_cursor"].is_string());
    assert_eq!(
        reverse_body["items"][0]["source_version_id"],
        versions[1].0.to_string()
    );
    // Same-graph FKs prevent semantic evidence from attaching to foreign nodes.
    let alien:Uuid=sqlx::query_scalar("INSERT INTO nodes(graph_id,type,title) VALUES($1,'entity','Other graph') RETURNING node_id").bind(b.2).fetch_one(&owner).await.unwrap();
    assert!(sqlx::query(
        "INSERT INTO node_semantic_atoms(node_id,semantic_atom_id,graph_id) VALUES($1,$2,$3)"
    )
    .bind(alien)
    .bind(Uuid::parse_str(target).unwrap())
    .bind(a.2)
    .execute(&owner)
    .await
    .is_err());
    // Node merging preserves semantic associations and deduplicates shared links.
    use coralswarm_backend::store::Store;
    let keep:Uuid=sqlx::query_scalar("INSERT INTO nodes(graph_id,type,title) VALUES($1,'entity','Canonical project') RETURNING node_id").bind(a.2).fetch_one(&owner).await.unwrap();
    sqlx::query(
        "INSERT INTO node_semantic_atoms(node_id,semantic_atom_id,graph_id) VALUES($1,$2,$3)",
    )
    .bind(keep)
    .bind(Uuid::parse_str(target).unwrap())
    .bind(a.2)
    .execute(&owner)
    .await
    .unwrap();
    let store = coralswarm_backend::store::PostgresStore::new(owner.clone());
    store.merge_nodes(keep, node).await.unwrap();
    let links: i64 =
        sqlx::query_scalar("SELECT count(*) FROM node_semantic_atoms WHERE node_id=$1")
            .bind(keep)
            .fetch_one(&owner)
            .await
            .unwrap();
    assert_eq!(links, 105);
    let merge_id: Uuid = sqlx::query_scalar(
        "SELECT merge_id FROM node_merges WHERE keep_id=$1 AND dup_id=$2 AND undone_at IS NULL",
    )
    .bind(keep)
    .bind(node)
    .fetch_one(&owner)
    .await
    .unwrap();
    store.unmerge_nodes(merge_id).await.unwrap();
    let restored: i64 =
        sqlx::query_scalar("SELECT count(*) FROM node_semantic_atoms WHERE node_id=$1")
            .bind(node)
            .fetch_one(&owner)
            .await
            .unwrap();
    assert_eq!(restored, 105);
    let retained: i64 =
        sqlx::query_scalar("SELECT count(*) FROM node_semantic_atoms WHERE node_id=$1")
            .bind(keep)
            .fetch_one(&owner)
            .await
            .unwrap();
    assert_eq!(
        retained, 1,
        "pre-existing shared support survives undo; moved support returns to original node"
    );

    // Correction cleanup must retain a node backed by immutable passages.
    let legacy_atom:Uuid=sqlx::query_scalar("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,timestamp) VALUES($1,'transcript','correction-evidence:chunk:0',$2,'Ada: correction fixture.',now()) RETURNING atom_id").bind(a.2).bind(format!("{}/{}",a.2,Uuid::new_v4())).fetch_one(&owner).await.unwrap();
    sqlx::query("INSERT INTO node_atoms(node_id,atom_id) VALUES($1,$2)")
        .bind(node)
        .bind(legacy_atom)
        .execute(&owner)
        .await
        .unwrap();
    sqlx::query("INSERT INTO transcript_corrections(graph_id,atom_id,source_id,surface_form,canonical,status,kind,origin) VALUES($1,$2,'correction-evidence','Ada','Adele','applied','speaker','user')").bind(a.2).bind(Uuid::nil()).execute(&owner).await.unwrap();
    let corrected = coralswarm_backend::services::corrections::propagate(
        &app_pool,
        None,
        None,
        "evidence-test",
        a.2,
        "correction-evidence",
    )
    .await
    .unwrap();
    assert!(!corrected.rewritten_atoms.is_empty());

    let retained_node: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM nodes WHERE node_id=$1)")
            .bind(node)
            .fetch_one(&owner)
            .await
            .unwrap();
    assert!(retained_node);
    // Both ordinary deletion and publication retraction protect semantic support
    // when removing an unrelated legacy atom that was the only chunk on a node.
    for retract in [false, true] {
        let note:Uuid=sqlx::query_scalar("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,timestamp) VALUES($1,'note',$2,$3,'temporary retrieval note',now()) RETURNING atom_id").bind(a.2).bind(format!("note-{retract}")).bind(format!("{}/{}",a.2,Uuid::new_v4())).fetch_one(&owner).await.unwrap();
        sqlx::query("INSERT INTO node_atoms(node_id,atom_id) VALUES($1,$2)")
            .bind(keep)
            .bind(note)
            .execute(&owner)
            .await
            .unwrap();
        if retract {
            sqlx::query("UPDATE nodes SET owner_id=$2,promoted_from_node_id=$3 WHERE node_id=$1")
                .bind(keep)
                .bind(a.0)
                .bind(Uuid::new_v4())
                .execute(&owner)
                .await
                .unwrap();
            coralswarm_backend::services::retraction::unpublish_atom(
                &app_pool,
                None,
                "evidence-test",
                a.2,
                note,
                a.0,
            )
            .await
            .unwrap();
        } else {
            coralswarm_backend::services::deletion::delete_atom_cascade(
                &app_pool,
                a.1,
                None,
                "evidence-test",
                a.2,
                note,
                Some(true),
            )
            .await
            .unwrap();
        }
        let remains: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM nodes WHERE node_id=$1)")
                .bind(keep)
                .fetch_one(&owner)
                .await
                .unwrap();
        assert!(remains);
    }
    // Shared semantic evidence from another source keeps its canonical node.
    let other_version:Uuid=sqlx::query_scalar("SELECT version_id FROM source_versions WHERE graph_id=$1 AND source_id='page-source' LIMIT 1").bind(a.2).fetch_one(&owner).await.unwrap();
    let other_atom:Uuid=sqlx::query_scalar("INSERT INTO semantic_atoms(graph_id,source_version_id,ordinal,kind,statement,extraction_version,model) VALUES($1,$2,0,'fact','Other source support','test','mock') RETURNING semantic_atom_id").bind(a.2).bind(other_version).fetch_one(&owner).await.unwrap();
    sqlx::query(
        "INSERT INTO node_semantic_atoms(graph_id,node_id,semantic_atom_id) VALUES($1,$2,$3)",
    )
    .bind(a.2)
    .bind(keep)
    .bind(other_atom)
    .execute(&owner)
    .await
    .unwrap();
    // Deleting the last retrieval chunk queues each historical blob exactly
    // once, then cascades semantic atoms/passages/node associations.
    let blobs = RecordingBlobs::default();
    let historical_keys: Vec<String> = versions
        .iter()
        .map(|_| format!("{}/{}", a.2, Uuid::new_v4()))
        .collect();
    for (v, key) in versions.iter().zip(&historical_keys) {
        sqlx::query("UPDATE source_versions SET raw_blob_key=$2 WHERE version_id=$1")
            .bind(v.0)
            .bind(key)
            .execute(&owner)
            .await
            .unwrap();
    }
    sqlx::query("UPDATE meeting_transcripts SET raw_blob_key=$2 WHERE graph_id=$1 AND source_id='meeting-evidence'").bind(a.2).bind(&historical_keys[2]).execute(&owner).await.unwrap();
    let retrieval_ids: Vec<Uuid> =
        sqlx::query_scalar("SELECT atom_id FROM atoms WHERE graph_id=$1")
            .bind(a.2)
            .fetch_all(&owner)
            .await
            .unwrap();
    assert!(!retrieval_ids.is_empty());
    for id in retrieval_ids {
        coralswarm_backend::services::deletion::delete_atom_cascade(
            &app_pool,
            a.1,
            Some(&blobs),
            "evidence-test",
            a.2,
            id,
            Some(true),
        )
        .await
        .unwrap();
    }
    assert!(
        blobs.0.lock().unwrap().is_empty(),
        "blob cleanup must follow the committed durable job"
    );
    let queued_keys: Vec<String> = sqlx::query_scalar(
        "SELECT payload->>'blob_key' FROM jobs WHERE account_id=$1 AND kind='DeleteSourceBlob' AND status='pending'",
    ).bind(a.1).fetch_all(&owner).await.unwrap();
    for key in historical_keys {
        assert_eq!(queued_keys.iter().filter(|k| *k == &key).count(), 1);
    }
    let remaining: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM semantic_atoms WHERE graph_id=$1 AND source_version_id=ANY($2)",
    )
    .bind(a.2)
    .bind(versions.iter().map(|v| v.0).collect::<Vec<_>>())
    .fetch_one(&owner)
    .await
    .unwrap();
    assert_eq!(remaining, 0);
    let removed_private_node: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM nodes WHERE node_id=$1)")
            .bind(node)
            .fetch_one(&owner)
            .await
            .unwrap();
    assert!(
        !removed_private_node,
        "semantic-only node title must leave with its last deleted source"
    );
    let shared_node_survives: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM nodes WHERE node_id=$1)")
            .bind(keep)
            .fetch_one(&owner)
            .await
            .unwrap();
    assert!(
        shared_node_survives,
        "other-source semantic support must keep the canonical node"
    );

    assert_eq!(
        get(&app, Some(&token_a), &historic).await.0,
        StatusCode::NOT_FOUND
    );
    jwks_task.abort();
    drop(app);
    drop(state);
    app_pool.close().await;
    sqlx::query("DELETE FROM jobs WHERE account_id=ANY($1)")
        .bind([a.1, b.1].as_slice())
        .execute(&owner)
        .await
        .unwrap();
    sqlx::query("DELETE FROM meeting_transcripts WHERE graph_id=ANY($1)")
        .bind([a.2, b.2].as_slice())
        .execute(&owner)
        .await
        .unwrap();
    sqlx::query("DELETE FROM graphs WHERE id=ANY($1)")
        .bind([a.2, b.2].as_slice())
        .execute(&owner)
        .await
        .unwrap();
    for account_id in [a.1, b.1] {
        let mut financial = coralswarm_backend::tenancy::scope::begin_account_scoped(&owner, account_id).await.unwrap();
        coralswarm_backend::billing::renewal::lock_period_rotation(&mut financial, account_id).await.unwrap();
        sqlx::query("DELETE FROM accounts WHERE id=ANY($1)")
            .bind([account_id].as_slice())
            .execute(&mut *financial)
            .await
            .unwrap();
        financial.commit().await.unwrap();
    }
    sqlx::query("DELETE FROM users WHERE id=ANY($1)")
        .bind([a.0, b.0].as_slice())
        .execute(&owner)
        .await
        .unwrap();
    owner.close().await;
}

#[tokio::test]
async fn meeting_evidence_tenant_migration_enforces_member_scope() {
    use coralswarm_backend::tenancy::scope::begin_tenant_scoped;
    use sqlx::{Connection, Executor, PgConnection};
    let Ok(url) = std::env::var("RELINFER_TEST_DATABASE_URL") else {
        assert!(
            std::env::var_os("REQUIRE_PG_SUITE").is_none(),
            "RELINFER_TEST_DATABASE_URL required"
        );
        eprintln!("skipping tenant evidence test: set RELINFER_TEST_DATABASE_URL");
        return;
    };
    let opts = PgConnectOptions::from_str(&url).unwrap();
    assert!(
        matches!(opts.get_host(), "127.0.0.1" | "localhost" | "::1"),
        "only disposable local PostgreSQL is permitted"
    );
    let db = coralswarm_backend::test_db::TestDatabase::new(&opts, "meeting_evidence_tenant");
    let mut admin = PgConnection::connect_with(&opts).await.unwrap();
    admin
        .execute(format!("CREATE DATABASE {db}").as_str())
        .await
        .unwrap();
    let tenant_opts = opts.clone().database(&db);
    let owner = PgPoolOptions::new()
        .max_connections(4)
        .connect_with(tenant_opts.clone())
        .await
        .unwrap();
    sqlx::migrate!("./migrations_tenant")
        .run(&owner)
        .await
        .unwrap();
    support::tenant_role::set_app_role_password_with_retry(&owner).await;
    let app = PgPoolOptions::new()
        .max_connections(3)
        .connect_with(
            tenant_opts
                .username("coralswarm_tenant_app")
                .password(support::tenant_role::APP_ROLE_TEST_PASSWORD),
        )
        .await
        .unwrap();
    let bypass: (bool, bool) =
        sqlx::query_as("SELECT rolbypassrls,rolsuper FROM pg_roles WHERE rolname=current_user")
            .fetch_one(&app)
            .await
            .unwrap();
    assert_eq!(bypass, (false, false));
    let account = Uuid::new_v4();
    let member_a = Uuid::new_v4();
    let member_b = Uuid::new_v4();
    let mut graph_a = Uuid::nil();
    let mut version_a = Uuid::nil();
    for user in [member_a, member_b] {
        let graph:Uuid=sqlx::query_scalar("INSERT INTO graphs(account_id,name,kind,owner_user_id) VALUES($1,'member memory','member',$2) RETURNING id").bind(account).bind(user).fetch_one(&owner).await.unwrap();
        sqlx::query("INSERT INTO meeting_transcripts(graph_id,source_id,raw_inline) VALUES($1,'source','private raw')").bind(graph).execute(&owner).await.unwrap();
        attach_fixture_source(&owner, graph, "source").await;
        let version:Uuid=sqlx::query_scalar("INSERT INTO source_versions(graph_id,source_id,source_item_id,raw_inline) VALUES($1,'source',(SELECT source_item_id FROM meeting_transcripts WHERE graph_id=$1 AND source_id='source'),'private raw') RETURNING version_id").bind(graph).fetch_one(&owner).await.unwrap();
        let atom:Uuid=sqlx::query_scalar("INSERT INTO semantic_atoms(graph_id,source_version_id,ordinal,kind,statement,extraction_version,model) VALUES($1,$2,0,'fact','Private fact','test','mock') RETURNING semantic_atom_id").bind(graph).bind(version).fetch_one(&owner).await.unwrap();
        sqlx::query("INSERT INTO semantic_atom_passages(graph_id,source_version_id,semantic_atom_id,start_char,end_char,quote) VALUES($1,$2,$3,0,7,'private')").bind(graph).bind(version).bind(atom).execute(&owner).await.unwrap();
        let node:Uuid=sqlx::query_scalar("INSERT INTO nodes(graph_id,type,title) VALUES($1,'entity','Private topic') RETURNING node_id").bind(graph).fetch_one(&owner).await.unwrap();
        sqlx::query(
            "INSERT INTO node_semantic_atoms(graph_id,semantic_atom_id,node_id) VALUES($1,$2,$3)",
        )
        .bind(graph)
        .bind(atom)
        .bind(node)
        .execute(&owner)
        .await
        .unwrap();
        let chunk:Uuid=sqlx::query_scalar("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,timestamp) VALUES($1,'transcript','source:chunk:0',$2,'private raw',now()) RETURNING atom_id").bind(graph).bind(format!("{}/{}",graph,Uuid::new_v4())).fetch_one(&owner).await.unwrap();
        sqlx::query("INSERT INTO source_version_chunks(graph_id,source_version_id,atom_id) VALUES($1,$2,$3)").bind(graph).bind(version).bind(chunk).execute(&owner).await.unwrap();
        sqlx::query("INSERT INTO semantic_atom_windows(graph_id,source_version_id,extraction_version,start_char,end_char) VALUES($1,$2,'test',0,11)").bind(graph).bind(version).execute(&owner).await.unwrap();
        if user == member_a {
            graph_a = graph;
            version_a = version;
        }
    }
    for table in [
        "source_items",
        "source_version_chunks",
        "source_versions",
        "semantic_atoms",
        "semantic_atom_passages",
        "node_semantic_atoms",
        "semantic_atom_windows",
    ] {
        let query = format!("SELECT count(*) FROM {table} WHERE graph_id=$1");
        let unscoped: i64 = sqlx::query_scalar(&query)
            .bind(graph_a)
            .fetch_one(&app)
            .await
            .unwrap();
        assert_eq!(unscoped, 0, "{table}: no actor must hide member memory");
        let mut own = begin_tenant_scoped(&app, account, member_a).await.unwrap();
        let visible: i64 = sqlx::query_scalar(&query)
            .bind(graph_a)
            .fetch_one(&mut *own)
            .await
            .unwrap();
        assert_eq!(visible, 1, "{table}: owner reads own evidence");
        own.rollback().await.unwrap();
        let mut other = begin_tenant_scoped(&app, account, member_b).await.unwrap();
        let hidden: i64 = sqlx::query_scalar(&query)
            .bind(graph_a)
            .fetch_one(&mut *other)
            .await
            .unwrap();
        assert_eq!(
            hidden, 0,
            "{table}: same account does not bypass member privacy"
        );
        other.rollback().await.unwrap();
    }
    let mut other = begin_tenant_scoped(&app, account, member_b).await.unwrap();
    assert!(coralswarm_backend::meeting_evidence::load_version_scoped(
        &mut other, graph_a, version_a
    )
    .await
    .unwrap()
    .is_none());
    assert!(sqlx::query("INSERT INTO semantic_atom_windows(graph_id,source_version_id,extraction_version,start_char,end_char) VALUES($1,$2,'attacker',0,11)").bind(graph_a).bind(version_a).execute(&mut *other).await.is_err(),"cross-member writes must be denied");
    other.rollback().await.unwrap();
    app.close().await;
    owner.close().await;
    admin
        .execute(format!("DROP DATABASE {db} WITH (FORCE)").as_str())
        .await
        .unwrap();
    admin.close().await.unwrap();
}

#[tokio::test]
async fn final_source_delete_locks_version_before_nodes_during_first_extraction() {
    let Ok(url) = std::env::var("RELINFER_TEST_DATABASE_URL") else {
        assert!(
            std::env::var_os("REQUIRE_PG_SUITE").is_none(),
            "RELINFER_TEST_DATABASE_URL required"
        );
        eprintln!("skipping evidence lock test: set RELINFER_TEST_DATABASE_URL");
        return;
    };
    let opts = PgConnectOptions::from_str(&url).unwrap();
    assert!(matches!(opts.get_host(), "127.0.0.1" | "localhost" | "::1"));
    let owner = PgPoolOptions::new()
        .max_connections(4)
        .connect_with(opts.clone())
        .await
        .unwrap();
    let delete_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(opts.application_name("meeting-evidence-delete-lock-proof"))
        .await
        .unwrap();
    let a = seed_account(&owner).await;
    sqlx::query("INSERT INTO meeting_transcripts(graph_id,source_id,raw_inline) VALUES($1,'lock-source','Ada: private fact')")
        .bind(a.2).execute(&owner).await.unwrap();
    attach_fixture_source(&owner, a.2, "lock-source").await;
    let version: Uuid = sqlx::query_scalar("INSERT INTO source_versions(graph_id,source_id,source_item_id,raw_inline) VALUES($1,'lock-source',(SELECT source_item_id FROM meeting_transcripts WHERE graph_id=$1 AND source_id='lock-source'),'Ada: private fact') RETURNING version_id")
        .bind(a.2).fetch_one(&owner).await.unwrap();
    let legacy: Uuid = sqlx::query_scalar("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,timestamp) VALUES($1,'teams_transcript','lock-source:chunk:0',$2,'Ada: private fact',now()) RETURNING atom_id")
        .bind(a.2).bind(format!("{}/{}",a.2,Uuid::new_v4())).fetch_one(&owner).await.unwrap();
    let node: Uuid = sqlx::query_scalar("INSERT INTO nodes(graph_id,type,title) VALUES($1,'entity','Private first extraction topic') RETURNING node_id")
        .bind(a.2).fetch_one(&owner).await.unwrap();
    sqlx::query("INSERT INTO node_atoms(node_id,atom_id) VALUES($1,$2)")
        .bind(node)
        .bind(legacy)
        .execute(&owner)
        .await
        .unwrap();

    // This is persist_window's lock order: own the version, then bind nodes.
    let mut extraction = owner.begin().await.unwrap();
    sqlx::query("SELECT version_id FROM source_versions WHERE version_id=$1 FOR UPDATE")
        .bind(version)
        .fetch_one(&mut *extraction)
        .await
        .unwrap();
    let delete_task = tokio::spawn({
        let pool = delete_pool.clone();
        async move {
            coralswarm_backend::services::deletion::delete_atom_cascade(
                &pool,
                a.1,
                None,
                "lock-proof",
                a.2,
                legacy,
                Some(true),
            )
            .await
        }
    });
    // Wait for the real deletion transaction to reach the held version lock.
    // This avoids hoping that a fixed sleep happens to reproduce the race.
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let waiting: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name='meeting-evidence-delete-lock-proof' AND wait_event_type='Lock' AND query LIKE '%source_versions%')")
                .fetch_one(&owner).await.unwrap();
            if waiting { break; }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }).await.expect("deletion must reach the version lock");
    // The old node-first deletion order fails NOWAIT here and deadlocks a
    // normal FK association insert. The corrected order leaves the node free.
    sqlx::query("SELECT node_id FROM nodes WHERE node_id=$1 FOR KEY SHARE NOWAIT")
        .bind(node)
        .fetch_one(&mut *extraction)
        .await
        .expect("deletion must not lock nodes before versions");
    let semantic: Uuid = sqlx::query_scalar("INSERT INTO semantic_atoms(graph_id,source_version_id,ordinal,kind,statement,extraction_version,model) VALUES($1,$2,0,'fact','private fact','test','mock') RETURNING semantic_atom_id")
        .bind(a.2).bind(version).fetch_one(&mut *extraction).await.unwrap();
    sqlx::query(
        "INSERT INTO node_semantic_atoms(graph_id,node_id,semantic_atom_id) VALUES($1,$2,$3)",
    )
    .bind(a.2)
    .bind(node)
    .bind(semantic)
    .execute(&mut *extraction)
    .await
    .unwrap();
    extraction.commit().await.unwrap();
    let deleted = tokio::time::timeout(std::time::Duration::from_secs(5), delete_task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(deleted.deleted_reef_ids.contains(&node));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM source_versions WHERE graph_id=$1")
            .bind(a.2)
            .fetch_one(&owner)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM semantic_atoms WHERE graph_id=$1")
            .bind(a.2)
            .fetch_one(&owner)
            .await
            .unwrap(),
        0
    );
    delete_pool.close().await;
    sqlx::query("DELETE FROM graphs WHERE id=$1")
        .bind(a.2)
        .execute(&owner)
        .await
        .unwrap();
    let mut financial = coralswarm_backend::tenancy::scope::begin_account_scoped(&owner, a.1).await.unwrap();
    coralswarm_backend::billing::renewal::lock_period_rotation(&mut financial, a.1).await.unwrap();
    sqlx::query("DELETE FROM accounts WHERE id=$1")
        .bind(a.1)
        .execute(&mut *financial)
        .await
        .unwrap();
    financial.commit().await.unwrap();
    sqlx::query("DELETE FROM users WHERE id=$1")
        .bind(a.0)
        .execute(&owner)
        .await
        .unwrap();
    owner.close().await;
}

#[tokio::test]
async fn generic_source_identity_metadata_and_preserved_support_are_stable() {
    use coralswarm_backend::{
        source_evidence::{capture_source_version_tx, node_evidence_counts_tx, CaptureSource},
        tenancy::scope::begin_account_scoped,
    };
    let Ok(url) = std::env::var("RELINFER_TEST_DATABASE_URL") else {
        assert!(
            std::env::var_os("REQUIRE_PG_SUITE").is_none(),
            "RELINFER_TEST_DATABASE_URL required"
        );
        return;
    };
    let opts = PgConnectOptions::from_str(&url).unwrap();
    assert!(matches!(opts.get_host(), "127.0.0.1" | "localhost" | "::1"));
    let owner = PgPoolOptions::new()
        .max_connections(3)
        .connect_with(opts.clone())
        .await
        .unwrap();
    support::app_role::set_app_role_password_with_retry(&owner).await;
    let app = PgPoolOptions::new()
        .max_connections(3)
        .connect_with(
            opts.username("coralswarm_app")
                .password(support::app_role::APP_ROLE_TEST_PASSWORD),
        )
        .await
        .unwrap();
    let a = seed_account(&owner).await;
    let text = "Ada: Café 🌊 launches Friday.";
    let first_meta = json!({"title":"Launch","attendees":["Ada","Bob"],"event_start":"2026-10-01T12:00:00Z","imported_at":"first"});
    let reordered = json!({"imported_at":"second","event_start":"2026-10-01T12:00:00Z","attendees":["Bob","Ada"],"title":"Launch"});
    let changed =
        json!({"title":"Launch","attendees":["Ada","Bob"],"event_start":"2026-10-02T12:00:00Z"});
    let mut ids = vec![];
    for (kind, namespace, metadata, people) in [
        (
            "calendar_event",
            "google:calendar:one",
            &first_meta,
            vec!["Ada".into(), "Bob".into()],
        ),
        (
            "calendar_event",
            "google:calendar:one",
            &reordered,
            vec!["Bob".into(), "Ada".into()],
        ),
        (
            "calendar_event",
            "google:calendar:one",
            &changed,
            vec!["Ada".into(), "Bob".into()],
        ),
        (
            "calendar_event",
            "google:calendar:one",
            &first_meta,
            vec!["Ada".into(), "Bob".into()],
        ),
        (
            "email",
            "google:mailbox:one",
            &first_meta,
            vec!["Ada".into()],
        ),
        (
            "email",
            "google:mailbox:two",
            &first_meta,
            vec!["Ada".into()],
        ),
    ] {
        let mut tx = begin_account_scoped(&app, a.1).await.unwrap();
        let result = capture_source_version_tx(
            &mut tx,
            None,
            a.2,
            a.1,
            &CaptureSource {
                source_type: kind,
                provider_namespace: namespace,
                source_id: "same-native-id",
                normalized_text: text,
                representation: "normalized_text",
                metadata,
                occurred_at: None,
                participants: &people,
                ingested_by: Some(a.0),
                scope: "private",
                duration_ms: None,
                segments: None,
            },
            None,
            &[42; 32],
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
        ids.push(result);
    }
    assert_eq!(
        ids[0].version_id, ids[1].version_id,
        "reordered sets/runtime metadata do not invent a version"
    );
    assert_ne!(
        ids[1].version_id, ids[2].version_id,
        "calendar metadata-only change is versioned"
    );
    assert_ne!(
        ids[0].version_id, ids[3].version_id,
        "A→B→A retains three observations"
    );
    assert_ne!(
        ids[0].source_item_id, ids[4].source_item_id,
        "source kind belongs to identity"
    );
    assert_ne!(
        ids[4].source_item_id, ids[5].source_item_id,
        "mailboxes keep native ID collisions separate"
    );
    let counts:(i64,i64)=sqlx::query_as("SELECT (SELECT count(*) FROM source_versions WHERE graph_id=$1),(SELECT count(*) FROM source_version_chunks WHERE graph_id=$1)").bind(a.2).fetch_one(&owner).await.unwrap();
    assert_eq!(
        counts,
        (5, 0),
        "zero retrieval chunks still retain five source versions"
    );
    let queued: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM jobs WHERE account_id=$1 AND kind='ExtractSourceAtoms'",
    )
    .bind(a.1)
    .fetch_one(&owner)
    .await
    .unwrap();
    assert_eq!(
        queued, 0,
        "retention cannot mint unmanaged extraction jobs"
    );
    let admissions:Vec<(String,Option<String>,String)>=sqlx::query_as("SELECT a.state,a.reason,v.extraction_status FROM corpus_capture_admissions a JOIN source_versions v ON v.version_id=a.source_version_id WHERE a.graph_id=$1").bind(a.2).fetch_all(&owner).await.unwrap();
    assert_eq!(admissions.len(), 5, "every retained version records its admission");
    assert!(
        admissions.iter().all(|r| r
            == &(
                "blocked".to_string(),
                Some("managed_admission_required".to_string()),
                "blocked".to_string()
            )),
        "{admissions:?}"
    );
    let node: Uuid = sqlx::query_scalar(
        "INSERT INTO nodes(graph_id,type,title) VALUES($1,'entity','Launch') RETURNING node_id",
    )
    .bind(a.2)
    .fetch_one(&owner)
    .await
    .unwrap();
    for (ordinal, version) in [ids[0].version_id, ids[2].version_id, ids[3].version_id]
        .into_iter()
        .enumerate()
    {
        // A library title must be grounded in an entity mention of its evidence.
        let atom:Uuid=sqlx::query_scalar("INSERT INTO semantic_atoms(graph_id,source_version_id,ordinal,kind,statement,extraction_version,model,entities) VALUES($1,$2,$3,'fact','Launch','source-atoms-v1','fixture','[{\"mention\":\"Launch\",\"type\":\"project\"}]') RETURNING semantic_atom_id").bind(a.2).bind(version).bind(ordinal as i32).fetch_one(&owner).await.unwrap();
        sqlx::query(
            "INSERT INTO node_semantic_atoms(graph_id,node_id,semantic_atom_id) VALUES($1,$2,$3)",
        )
        .bind(a.2)
        .bind(node)
        .bind(atom)
        .execute(&owner)
        .await
        .unwrap();
    }
    // Semantic-only typed nodes must be visible before a retrieval chunk exists.
    sqlx::query("UPDATE nodes SET entity_type='project' WHERE node_id=$1")
        .bind(node)
        .execute(&owner)
        .await
        .unwrap();
    let empty_node: Uuid = sqlx::query_scalar("INSERT INTO nodes(graph_id,type,title,entity_type) VALUES($1,'entity','No evidence','topic') RETURNING node_id")
        .bind(a.2).fetch_one(&owner).await.unwrap();
    let other_graph: Uuid = sqlx::query_scalar(
        "INSERT INTO graphs(account_id,name) VALUES($1,'other scoped graph') RETURNING id",
    )
    .bind(a.1)
    .fetch_one(&owner)
    .await
    .unwrap();
    let mut library_tx = begin_account_scoped(&app, a.1).await.unwrap();
    let (topics, total) = coralswarm_backend::store::library::list_library_topics(
        &mut library_tx,
        &[a.2],
        Some("project"),
        None,
        0,
        20,
    )
    .await
    .unwrap();
    assert_eq!(total, 1);
    assert_eq!(topics.len(), 1);
    assert_eq!(topics[0].node_id, node);
    assert_eq!(
        (
            topics[0].atom_count,
            topics[0].semantic_atom_count,
            topics[0].source_support_count,
            topics[0].unmapped_legacy_chunk_count
        ),
        (0, 3, 1, 0)
    );
    let (all_topics, total) = coralswarm_backend::store::library::list_library_topics(
        &mut library_tx,
        &[a.2, other_graph],
        None,
        None,
        0,
        20,
    )
    .await
    .unwrap();
    assert_eq!(total, 1, "typed empty nodes are still excluded");
    assert!(all_topics.iter().all(|topic| topic.node_id != empty_node));
    let wrong_graph = coralswarm_backend::source_evidence::node_evidence_counts_for_graphs_tx(
        &mut library_tx,
        &[other_graph],
        &[node],
    )
    .await
    .unwrap();
    assert!(
        wrong_graph.is_empty(),
        "requested IDs cannot escape the selected graph set"
    );
    let (_, title_total) = coralswarm_backend::store::library::list_library_topics(
        &mut library_tx,
        &[a.2],
        None,
        Some("No evidence"),
        0,
        20,
    )
    .await
    .unwrap();
    assert_eq!(title_total, 0);
    library_tx.commit().await.unwrap();
    let chunk:Uuid=sqlx::query_scalar("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,timestamp) VALUES($1,'calendar_event','same-native-id:chunk:0',$2,'Launch',now()) RETURNING atom_id").bind(a.2).bind(format!("{}/{}",a.2,Uuid::new_v4())).fetch_one(&owner).await.unwrap();
    sqlx::query("INSERT INTO node_atoms(node_id,atom_id) VALUES($1,$2)")
        .bind(node)
        .bind(chunk)
        .execute(&owner)
        .await
        .unwrap();
    let mut tx = begin_account_scoped(&app, a.1).await.unwrap();
    let before = node_evidence_counts_tx(&mut tx, a.2, &[node])
        .await
        .unwrap();
    assert_eq!(
        (
            before[0].semantic_atom_count,
            before[0].source_support_count,
            before[0].unmapped_legacy_chunk_count
        ),
        (3, 1, 1)
    );
    coralswarm_backend::source_evidence::link_verified_chunks_tx(
        &mut tx,
        a.2,
        ids[3].version_id,
        &[chunk],
    )
    .await
    .unwrap();
    let after = node_evidence_counts_tx(&mut tx, a.2, &[node])
        .await
        .unwrap();
    assert_eq!(
        (
            after[0].semantic_atom_count,
            after[0].source_support_count,
            after[0].unmapped_legacy_chunk_count
        ),
        (3, 1, 0),
        "chunks and versions of one source count once"
    );
    tx.commit().await.unwrap();
    sqlx::query("DELETE FROM jobs WHERE account_id=$1")
        .bind(a.1)
        .execute(&owner)
        .await
        .unwrap();
    let mut financial = coralswarm_backend::tenancy::scope::begin_account_scoped(&owner, a.1).await.unwrap();
    coralswarm_backend::billing::renewal::lock_period_rotation(&mut financial, a.1).await.unwrap();
    sqlx::query("DELETE FROM accounts WHERE id=$1")
        .bind(a.1)
        .execute(&mut *financial)
        .await
        .unwrap();
    financial.commit().await.unwrap();
    sqlx::query("DELETE FROM users WHERE id=$1")
        .bind(a.0)
        .execute(&owner)
        .await
        .unwrap();
    app.close().await;
    owner.close().await;
}

#[tokio::test]
async fn populated_meeting_migration_preserves_exact_evidence_identifiers() {
    let Ok(url) = std::env::var("RELINFER_TEST_DATABASE_URL") else {
        assert!(
            std::env::var_os("REQUIRE_PG_SUITE").is_none(),
            "RELINFER_TEST_DATABASE_URL required"
        );
        return;
    };
    let opts = PgConnectOptions::from_str(&url).unwrap();
    assert!(matches!(opts.get_host(), "127.0.0.1" | "localhost" | "::1"));
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(opts)
        .await
        .unwrap();
    let mut tx = pool.begin().await.unwrap();
    let schema = format!("source_upgrade_{}", Uuid::new_v4().simple());
    sqlx::raw_sql(&format!("CREATE SCHEMA {schema}; SET LOCAL search_path TO {schema},public;
        CREATE TABLE graphs(id UUID PRIMARY KEY,account_id UUID);
        CREATE TABLE meeting_transcripts(graph_id UUID,source_id TEXT,raw_blob_key TEXT,raw_inline TEXT,duration_ms BIGINT,segments JSONB,created_at TIMESTAMPTZ DEFAULT now(),PRIMARY KEY(graph_id,source_id));
        CREATE TABLE nodes(node_id UUID PRIMARY KEY,graph_id UUID);
        CREATE TABLE atoms(atom_id UUID PRIMARY KEY,graph_id UUID);
        CREATE TABLE jobs(payload JSONB,created_at TIMESTAMPTZ,kind TEXT);")).execute(&mut *tx).await.unwrap();
    let graph = Uuid::new_v4();
    let node = Uuid::new_v4();
    sqlx::query("INSERT INTO graphs(id,account_id) VALUES($1,$2)")
        .bind(graph)
        .bind(Uuid::new_v4())
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("INSERT INTO meeting_transcripts(graph_id,source_id,raw_inline) VALUES($1,'legacy','🌊 go')").bind(graph).execute(&mut *tx).await.unwrap();
    sqlx::query("INSERT INTO nodes(node_id,graph_id) VALUES($1,$2)")
        .bind(node)
        .bind(graph)
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::raw_sql(include_str!(
        "../migrations_control/0291_meeting_semantic_evidence.sql"
    ))
    .execute(&mut *tx)
    .await
    .unwrap();
    let version: Uuid =
        sqlx::query_scalar("SELECT version_id FROM meeting_source_versions WHERE graph_id=$1")
            .bind(graph)
            .fetch_one(&mut *tx)
            .await
            .unwrap();
    let atom:Uuid=sqlx::query_scalar("INSERT INTO semantic_atoms(graph_id,source_version_id,ordinal,kind,statement,extraction_version,model) VALUES($1,$2,0,'action','Go','meeting-atoms-v1','fixture') RETURNING semantic_atom_id").bind(graph).bind(version).fetch_one(&mut *tx).await.unwrap();
    let passage:Uuid=sqlx::query_scalar("INSERT INTO semantic_atom_passages(graph_id,source_version_id,semantic_atom_id,start_char,end_char,quote) VALUES($1,$2,$3,0,4,'🌊 go') RETURNING passage_id").bind(graph).bind(version).bind(atom).fetch_one(&mut *tx).await.unwrap();
    sqlx::query(
        "INSERT INTO node_semantic_atoms(graph_id,node_id,semantic_atom_id) VALUES($1,$2,$3)",
    )
    .bind(graph)
    .bind(node)
    .bind(atom)
    .execute(&mut *tx)
    .await
    .unwrap();
    // Production commits migration 291 before applying 292. Flush its
    // deferred FK events here too, while retaining the fixture's final rollback.
    sqlx::query("SET CONSTRAINTS ALL IMMEDIATE")
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::raw_sql(include_str!(
        "../migrations_control/0292_shared_source_evidence.sql"
    ))
    .execute(&mut *tx)
    .await
    .unwrap();
    let row:(Uuid,Uuid,Uuid,String,String,i32,i32,String)=sqlx::query_as("SELECT v.version_id,a.semantic_atom_id,p.passage_id,v.raw_inline,v.extraction_version,p.start_char,p.end_char,v.provider_namespace FROM source_versions v JOIN semantic_atoms a ON a.source_version_id=v.version_id JOIN semantic_atom_passages p ON p.semantic_atom_id=a.semantic_atom_id JOIN node_semantic_atoms n ON n.semantic_atom_id=a.semantic_atom_id WHERE n.node_id=$1").bind(node).fetch_one(&mut *tx).await.unwrap();
    assert_eq!(
        row,
        (
            version,
            atom,
            passage,
            "🌊 go".into(),
            "meeting-atoms-v1".into(),
            0,
            4,
            "legacy:meeting".into()
        )
    );
    let pointer:Uuid=sqlx::query_scalar("SELECT s.current_version_id FROM source_items s JOIN meeting_transcripts m ON m.source_item_id=s.source_item_id WHERE m.graph_id=$1").bind(graph).fetch_one(&mut *tx).await.unwrap();
    assert_eq!(pointer, version);
    let mappings: i64 = sqlx::query_scalar("SELECT count(*) FROM source_version_chunks")
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    assert_eq!(mappings, 0, "migration must not guess old chunk ownership");
    tx.rollback().await.unwrap();
    pool.close().await;
}

#[tokio::test]
async fn unknown_legacy_meeting_identity_is_not_adopted_and_deletes_independently() {
    use coralswarm_backend::{
        services::{connector_audit::Actor, deletion::delete_source_item_in_graph},
        source_evidence::{capture_source_version_tx, CaptureSource},
        tenancy::scope::begin_account_scoped,
    };
    let Ok(url) = std::env::var("RELINFER_TEST_DATABASE_URL") else {
        assert!(
            std::env::var_os("REQUIRE_PG_SUITE").is_none(),
            "RELINFER_TEST_DATABASE_URL required"
        );
        return;
    };
    let opts = PgConnectOptions::from_str(&url).unwrap();
    assert!(matches!(opts.get_host(), "127.0.0.1" | "localhost" | "::1"));
    let owner = PgPoolOptions::new()
        .max_connections(2)
        .connect_with(opts.clone())
        .await
        .unwrap();
    support::app_role::set_app_role_password_with_retry(&owner).await;
    // A one-connection application pool also detects a deletion implementation
    // that holds a lock connection while trying to open another transaction.
    let app = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(
            opts.username("coralswarm_app")
                .password(support::app_role::APP_ROLE_TEST_PASSWORD),
        )
        .await
        .unwrap();
    for (bound_legacy, source_type, namespace, delete_new_first) in [
        (true, "zoom", "zoom:account-one", true),
        (false, "transcript", "legacy:declared", false),
    ] {
        let a = seed_account(&owner).await;
        let source_id = "colliding-native-id";
        let legacy_text = "Ada: preserved meeting whose provider is unknown.";
        sqlx::query("INSERT INTO meeting_transcripts(graph_id,source_id,raw_inline,segments) VALUES($1,$2,$3,$4)")
            .bind(a.2).bind(source_id).bind(legacy_text).bind(json!([{"speaker":"Ada","start_ms":0,"end_ms":100,"text":"preserved"}])).execute(&owner).await.unwrap();
        if bound_legacy {
            let item = attach_fixture_source(&owner, a.2, source_id).await;
            let version:Uuid=sqlx::query_scalar("INSERT INTO source_versions(graph_id,source_item_id,source_id,source_type,provider_namespace,raw_inline,is_legacy_baseline,extraction_version) VALUES($1,$2,$3,'transcript','legacy:meeting',$4,true,'meeting-atoms-v1') RETURNING version_id")
                .bind(a.2).bind(item).bind(source_id).bind(legacy_text).fetch_one(&owner).await.unwrap();
            sqlx::query("UPDATE source_items SET current_version_id=$2 WHERE source_item_id=$1")
                .bind(item)
                .bind(version)
                .execute(&owner)
                .await
                .unwrap();
            sqlx::query("UPDATE meeting_transcripts SET current_version_id=$3 WHERE graph_id=$1 AND source_id=$2").bind(a.2).bind(source_id).bind(version).execute(&owner).await.unwrap();
        }
        let mut captured = vec![];
        for text in [
            "Bob: first provider observation.",
            "Bob: corrected provider observation.",
            "Bob: first provider observation.",
        ] {
            let mut tx = begin_account_scoped(&app, a.1).await.unwrap();
            let result = capture_source_version_tx(
                &mut tx,
                None,
                a.2,
                a.1,
                &CaptureSource {
                    source_type,
                    provider_namespace: namespace,
                    source_id,
                    normalized_text: text,
                    representation: "normalized_text",
                    metadata: &json!({}),
                    occurred_at: None,
                    participants: &[],
                    ingested_by: Some(a.0),
                    scope: "private",
                    duration_ms: None,
                    segments: None,
                },
                None,
                &[42; 32],
            )
            .await
            .unwrap();
            assert!(
                !result.meeting_projection,
                "native ID equality cannot reassign an existing meeting projection"
            );
            tx.commit().await.unwrap();
            captured.push(result);
        }
        assert!(captured
            .iter()
            .all(|v| v.source_item_id == captured[0].source_item_id));
        assert_ne!(
            captured[0].version_id, captured[2].version_id,
            "provider A→B→A remains history within its own item"
        );
        let legacy:(Uuid,Uuid,String,String)=sqlx::query_as("SELECT s.source_item_id,s.current_version_id,s.source_type,v.raw_inline FROM source_items s JOIN source_versions v ON v.version_id=s.current_version_id WHERE s.graph_id=$1 AND s.provider_namespace='legacy:meeting' AND s.source_id=$2")
            .bind(a.2).bind(source_id).fetch_one(&owner).await.unwrap();
        assert_ne!(legacy.0, captured[0].source_item_id);
        assert_eq!(legacy.2, "transcript");
        assert_eq!(legacy.3, legacy_text);
        let projection:(Uuid,Uuid,String)=sqlx::query_as("SELECT source_item_id,current_version_id,raw_inline FROM meeting_transcripts WHERE graph_id=$1 AND source_id=$2")
            .bind(a.2).bind(source_id).fetch_one(&owner).await.unwrap();
        assert_eq!(projection, (legacy.0, legacy.1, legacy_text.into()));
        let semantic:Uuid=sqlx::query_scalar("INSERT INTO semantic_atoms(graph_id,source_version_id,ordinal,kind,statement,extraction_version,model) VALUES($1,$2,0,'observation','Preserved meeting','meeting-atoms-v1','fixture') RETURNING semantic_atom_id")
            .bind(a.2).bind(legacy.1).fetch_one(&owner).await.unwrap();
        let passage:Uuid=sqlx::query_scalar("INSERT INTO semantic_atom_passages(graph_id,source_version_id,semantic_atom_id,start_char,end_char,quote) VALUES($1,$2,$3,0,3,'Ada') RETURNING passage_id")
            .bind(a.2).bind(legacy.1).bind(semantic).fetch_one(&owner).await.unwrap();
        let (deleted, surviving) = if delete_new_first {
            (captured[0].source_item_id, legacy.0)
        } else {
            (legacy.0, captured[0].source_item_id)
        };
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            delete_source_item_in_graph(
                &app,
                None,
                "identity-regression",
                a.2,
                deleted,
                Actor::User(a.0),
                None,
            ),
        )
        .await
        .expect("source deletion must work with one connection")
        .unwrap();
        let remaining: Vec<Uuid> =
            sqlx::query_scalar("SELECT source_item_id FROM source_items WHERE graph_id=$1")
                .bind(a.2)
                .fetch_all(&owner)
                .await
                .unwrap();
        assert_eq!(
            remaining,
            vec![surviving],
            "deleting one item cannot erase same-ID evidence from another namespace"
        );
        let surviving_versions:Vec<Uuid>=sqlx::query_scalar("SELECT version_id FROM source_versions WHERE graph_id=$1 AND source_item_id=$2 ORDER BY captured_at,version_id").bind(a.2).bind(surviving).fetch_all(&owner).await.unwrap();
        if delete_new_first {
            assert_eq!(surviving_versions, vec![legacy.1]);
            let remaining_passage: Uuid = sqlx::query_scalar(
                "SELECT passage_id FROM semantic_atom_passages WHERE passage_id=$1",
            )
            .bind(passage)
            .fetch_one(&owner)
            .await
            .unwrap();
            assert_eq!(remaining_passage, passage);
            let pointer: Uuid = sqlx::query_scalar(
                "SELECT source_item_id FROM meeting_transcripts WHERE graph_id=$1 AND source_id=$2",
            )
            .bind(a.2)
            .bind(source_id)
            .fetch_one(&owner)
            .await
            .unwrap();
            assert_eq!(pointer, legacy.0);
        } else {
            assert_eq!(
                surviving_versions,
                captured.iter().map(|v| v.version_id).collect::<Vec<_>>()
            );
            let projection_exists:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM meeting_transcripts WHERE graph_id=$1 AND source_id=$2)").bind(a.2).bind(source_id).fetch_one(&owner).await.unwrap();
            assert!(
                !projection_exists,
                "legacy link is not silently retargeted when its evidence is deleted"
            );
        }
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            delete_source_item_in_graph(
                &app,
                None,
                "identity-regression",
                a.2,
                surviving,
                Actor::User(a.0),
                None,
            ),
        )
        .await
        .expect("second source deletion must work with one connection")
        .unwrap();
        sqlx::query("DELETE FROM jobs WHERE account_id=$1")
            .bind(a.1)
            .execute(&owner)
            .await
            .unwrap();
        let mut financial = coralswarm_backend::tenancy::scope::begin_account_scoped(&owner, a.1).await.unwrap();
        coralswarm_backend::billing::renewal::lock_period_rotation(&mut financial, a.1).await.unwrap();
        sqlx::query("DELETE FROM accounts WHERE id=$1")
            .bind(a.1)
            .execute(&mut *financial)
            .await
            .unwrap();
        financial.commit().await.unwrap();
        sqlx::query("DELETE FROM users WHERE id=$1")
            .bind(a.0)
            .execute(&owner)
            .await
            .unwrap();
    }
    app.close().await;
    owner.close().await;
}
