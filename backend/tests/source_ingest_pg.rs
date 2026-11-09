//! Actual HTTP ingest plus the trusted connector capture entrypoint on isolated PG.
mod support;
use axum::{
    body::Body,
    http::{Request, StatusCode},
    Router,
};
use chrono::{Duration, Utc};
use coralswarm_backend::clerk::test_support::{mint, test_jwks, ISSUER};
use serde_json::{json, Value};
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    PgPool,
};
use std::{str::FromStr, sync::Arc};
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
async fn admitted_sources_use_versions_and_uuid_deletion_preserves_other_mailboxes() {
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

    // Ordinary capture retains permission-admitted evidence blocked until an
    // explicit managed run adopts it; coding checkpoints remain separate.
    for (kind, id, text, session) in [
        ("gmail", "mail-1", "Ada approved the launch.", None),
        ("notion", "doc-1", "The deployment window is Friday.", None),
        (
            "note",
            "note-1",
            "Check the backup before deployment.",
            None,
        ),
        (
            "coding_session",
            "cc:session-1:checkpoint-1",
            "The retry bug is fixed.",
            Some("session-1"),
        ),
    ] {
        let request = json!({"graph_id":a.2,"source_type":kind,"source_id":id,"content":text,
            "timestamp":"2026-10-01T12:00:00Z","participants":["Ada"],"session_id":session,
            "session_kind":if session.is_some(){Some("coding")}else{None}});
        for _ in 0..2 {
            let (status, body) = post(&app, &token_a, "/v1/ingest/atom", request.clone()).await;
            assert_eq!(status, StatusCode::OK, "{kind}: {body}");
        }
        let rows:Vec<(String,String,Value,Option<Uuid>)>=sqlx::query_as(
            "SELECT v.raw_inline,s.provider_namespace,v.source_metadata,v.ingested_by FROM source_versions v \
             JOIN source_items s ON s.source_item_id=v.source_item_id AND s.graph_id=v.graph_id WHERE s.graph_id=$1 AND s.source_type=$2 AND s.source_id=$3"
        ).bind(a.2).bind(kind).bind(id).fetch_all(&owner).await.unwrap();
        assert_eq!(
            rows.len(),
            1,
            "identical retry creates no extra source version"
        );
        assert_eq!(rows[0].0, text);
        assert_eq!(rows[0].1, format!("declared:user:{}", a.0));
        assert_eq!(rows[0].3, Some(a.0));
        if session.is_some() {
            assert_eq!(rows[0].2["capture_extent"], "checkpoint");
            assert_eq!(rows[0].2["session_id"], "session-1");
        }
    }
    let queued: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM jobs WHERE account_id=$1 AND kind='ExtractSourceAtoms'",
    )
    .bind(a.1)
    .fetch_one(&owner)
    .await
    .unwrap();
    assert_eq!(queued, 0, "retention cannot mint unmanaged model jobs");
    let meetings: i64 =
        sqlx::query_scalar("SELECT count(*) FROM meeting_transcripts WHERE graph_id=$1")
            .bind(a.2)
            .fetch_one(&owner)
            .await
            .unwrap();
    assert_eq!(
        meetings, 0,
        "email/doc/checkpoint capture must not create fictional meetings"
    );

    // Same provider-native ID in two real connector namespaces is two sources,
    // even though legacy positional retrieval chunk uniqueness still collides.
    let user = coralswarm_backend::UserContext {
        user_id: a.0,
        account_id: a.1,
        role: "owner".into(),
        device_id: String::new(),
    };
    let tenant = coralswarm_backend::tenancy::extractor::Tenant::legacy(&state);
    let connector_request = |text: &str| {
        serde_json::from_value(json!({
            "graph_id":a.2,"source_type":"gmail","source_id":"same-native-id","content":text,
            "timestamp":"2026-10-01T12:00:00Z","participants":["Ada"]
        }))
        .unwrap()
    };
    for (namespace, text) in [
        ("google_gmail:user:mailbox-a", "Mailbox A approved Friday."),
        ("google_gmail:user:mailbox-b", "Mailbox B approved Monday."),
    ] {
        let response = coralswarm_backend::test_ingest_with_source_context(
            &state,
            &tenant,
            &user,
            connector_request(text),
            namespace,
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
    }
    let items:Vec<(Uuid,String,i64)>=sqlx::query_as(
        "SELECT s.source_item_id,s.provider_namespace,count(c.atom_id) FROM source_items s \
         JOIN source_versions v ON v.source_item_id=s.source_item_id AND v.graph_id=s.graph_id \
         LEFT JOIN source_version_chunks c ON c.source_version_id=v.version_id AND c.graph_id=v.graph_id \
         WHERE s.graph_id=$1 AND s.source_id='same-native-id' GROUP BY s.source_item_id,s.provider_namespace ORDER BY s.provider_namespace"
    ).bind(a.2).fetch_all(&owner).await.unwrap();
    assert_eq!(items.len(), 2);
    assert_eq!(items[0].2, 1);
    assert_eq!(
        items[1].2, 0,
        "old positional chunk must not be falsely mapped to mailbox B"
    );
    let delete = |id: Uuid, token: &str| {
        Request::builder()
            .method("DELETE")
            .uri(format!("/v1/sources/items/{id}?graph_id={}", a.2))
            .header("authorization", format!("Bearer {token}"))
            .body(Body::empty())
            .unwrap()
    };
    assert_eq!(
        app.clone()
            .oneshot(delete(items[1].0, &token_b))
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );
    let response = app
        .clone()
        .oneshot(delete(items[1].0, &token_a))
        .await
        .unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), 1_000_000)
        .await
        .unwrap();
    assert_eq!(
        status,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&bytes)
    );
    let remaining:Vec<String>=sqlx::query_scalar("SELECT provider_namespace FROM source_items WHERE graph_id=$1 AND source_id='same-native-id'")
        .bind(a.2).fetch_all(&owner).await.unwrap();
    assert_eq!(remaining, ["google_gmail:user:mailbox-a"]);
    let chunks: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM atoms WHERE graph_id=$1 AND source_id='same-native-id:chunk:0'",
    )
    .bind(a.2)
    .fetch_one(&owner)
    .await
    .unwrap();
    assert_eq!(
        chunks, 1,
        "deleting B's source-only snapshot must leave A's chunk"
    );
    let response = coralswarm_backend::test_ingest_with_source_context(
        &state,
        &tenant,
        &user,
        connector_request("Mailbox B approved Monday."),
        "google_gmail:user:mailbox-b",
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let response = app
        .clone()
        .oneshot(delete(items[0].0, &token_a))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let chunks: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM atoms WHERE graph_id=$1 AND source_id='same-native-id:chunk:0'",
    )
    .bind(a.2)
    .fetch_one(&owner)
    .await
    .unwrap();
    assert_eq!(chunks, 0);
    let remaining:Vec<String>=sqlx::query_scalar("SELECT provider_namespace FROM source_items WHERE graph_id=$1 AND source_id='same-native-id'")
        .bind(a.2).fetch_all(&owner).await.unwrap();
    assert_eq!(
        remaining,
        ["google_gmail:user:mailbox-b"],
        "deleting A must preserve B even though B owns no legacy chunk"
    );

    jwks_task.abort();
    app_pool.close().await;
    owner.close().await;
}

fn isolated_options() -> Option<PgConnectOptions> {
    let Ok(url) = std::env::var("RELINFER_TEST_DATABASE_URL") else {
        assert!(
            std::env::var_os("REQUIRE_PG_SUITE").is_none(),
            "RELINFER_TEST_DATABASE_URL required"
        );
        eprintln!("skipping source lifecycle test: set RELINFER_TEST_DATABASE_URL");
        return None;
    };
    let options = PgConnectOptions::from_str(&url).unwrap();
    assert!(matches!(
        options.get_host(),
        "127.0.0.1" | "localhost" | "::1"
    ));
    assert_eq!(options.get_database(), Some("memory_provenance_test"));
    Some(options)
}

async fn source_test_app_pool(
    options: &PgConnectOptions,
    name: &str,
    max_connections: u32,
) -> PgPool {
    let pool = PgPoolOptions::new()
        .max_connections(max_connections)
        .connect_with(
            options
                .clone()
                .username("coralswarm_app")
                .password(support::app_role::APP_ROLE_TEST_PASSWORD)
                .application_name(name),
        )
        .await
        .unwrap();
    let permissions: (bool, bool) =
        sqlx::query_as("SELECT rolbypassrls,rolsuper FROM pg_roles WHERE rolname=current_user")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(permissions, (false, false));
    pool
}

fn source_test_state(pool: &PgPool, account: Uuid) -> Arc<coralswarm_backend::AppState> {
    let state = Arc::new(coralswarm_backend::test_app_state_owned_with_db_and_store(
        pool.clone(),
        None,
    ));
    state.dek_cache.insert(
        &coralswarm_backend::tenancy::TenantId::Consumer,
        account,
        [42; 32],
    );
    state
}

async fn ingest_source_observation(
    state: &Arc<coralswarm_backend::AppState>,
    user: &coralswarm_backend::UserContext,
    graph: Uuid,
    native: &str,
    namespace: &str,
    text: &str,
) {
    let tenant = coralswarm_backend::tenancy::extractor::Tenant::legacy(state);
    let request = serde_json::from_value(json!({
        "graph_id":graph,"source_type":"gmail","source_id":native,"content":text,
        "timestamp":"2026-10-01T12:00:00Z","participants":["Ada"],
    }))
    .unwrap();
    let response = coralswarm_backend::test_ingest_with_source_context(
        state, &tenant, user, request, namespace,
    )
    .await;
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), 1_000_000)
        .await
        .unwrap();
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
}

async fn cleanup_source_account(owner: &PgPool, account: Uuid, user: Uuid) {
    sqlx::query("DELETE FROM jobs WHERE account_id=$1")
        .bind(account)
        .execute(owner)
        .await
        .unwrap();
    let mut financial = coralswarm_backend::tenancy::scope::begin_account_scoped(owner, account).await.unwrap();
    coralswarm_backend::billing::renewal::lock_period_rotation(&mut financial, account).await.unwrap();
    sqlx::query("DELETE FROM accounts WHERE id=$1")
        .bind(account)
        .execute(&mut *financial)
        .await
        .unwrap();
    financial.commit().await.unwrap();
    sqlx::query("DELETE FROM users WHERE id=$1")
        .bind(user)
        .execute(owner)
        .await
        .unwrap();
}

#[tokio::test]
async fn source_deletion_serializes_recapture_before_any_new_chunk_is_written() {
    use coralswarm_backend::services::{
        connector_audit::Actor, deletion::delete_source_item_in_graph,
    };
    let Some(options) = isolated_options() else {
        return;
    };
    let owner = PgPoolOptions::new()
        .max_connections(3)
        .connect_with(options.clone())
        .await
        .unwrap();
    support::app_role::set_app_role_password_with_retry(&owner).await;
    let suffix = Uuid::new_v4().simple().to_string();
    let deletion_name = format!("source-delete-{suffix}");
    let capture_name = format!("source-capture-{suffix}");
    let deletion_pool = source_test_app_pool(&options, &deletion_name, 1).await;
    let capture_pool = source_test_app_pool(&options, &capture_name, 4).await;
    let a = seed_account(&owner).await;
    let user = coralswarm_backend::UserContext {
        user_id: a.0,
        account_id: a.1,
        role: "owner".into(),
        device_id: "source-race".into(),
    };
    let state = source_test_state(&capture_pool, a.1);
    let native = "serialized-mail";
    let namespace = "google_gmail:user:serialized-mailbox";
    let old_text = "ObsoleteXylophoneMarker approved the old launch.";
    let new_text = "CurrentMarigoldMarker approved the new launch.";
    ingest_source_observation(&state, &user, a.2, native, namespace, old_text).await;
    let (old_item,old_version):(Uuid,Uuid)=sqlx::query_as("SELECT source_item_id,current_version_id FROM source_items WHERE graph_id=$1 AND source_id=$2")
        .bind(a.2).bind(native).fetch_one(&owner).await.unwrap();
    let old_chunks: Vec<Uuid> = sqlx::query_scalar("SELECT atom_id FROM atoms WHERE graph_id=$1")
        .bind(a.2)
        .fetch_all(&owner)
        .await
        .unwrap();
    assert_eq!(old_chunks.len(), 1);

    // Pause the real deletion after it obtains the identity advisory lock but
    // before it can lock the source row, enumerate chunks or change evidence.
    let mut barrier = owner.begin().await.unwrap();
    sqlx::query("SELECT source_item_id FROM source_items WHERE source_item_id=$1 FOR UPDATE")
        .bind(old_item)
        .fetch_one(&mut *barrier)
        .await
        .unwrap();
    let deletion = tokio::spawn({
        let pool = deletion_pool.clone();
        let graph = a.2;
        let actor = a.0;
        async move {
            delete_source_item_in_graph(
                &pool,
                None,
                "source-race",
                graph,
                old_item,
                Actor::User(actor),
                None,
            )
            .await
        }
    });
    tokio::time::timeout(std::time::Duration::from_secs(10),async {
        loop {
            let paused:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity activity WHERE activity.application_name=$1 AND activity.wait_event_type='Lock' AND activity.query LIKE '%source_items%' AND EXISTS(SELECT 1 FROM pg_locks held WHERE held.pid=activity.pid AND held.locktype='advisory' AND held.granted))")
                .bind(&deletion_name).fetch_one(&owner).await.unwrap();
            if paused {break;}
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }).await.expect("deletion must hold identity serialization before the source-row barrier");
    let capture = tokio::spawn({
        let state = state.clone();
        let graph = a.2;
        async move {
            ingest_source_observation(&state, &user, graph, native, namespace, new_text).await;
        }
    });
    // Check the actual blocking relationship instead of relying on scheduling
    // or a fixed sleep: the capture must wait on this deletion's advisory lock.
    tokio::time::timeout(std::time::Duration::from_secs(10),async {
        loop {
            let blocked:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity capture JOIN pg_stat_activity deletion ON deletion.application_name=$2 WHERE capture.application_name=$1 AND deletion.pid=ANY(pg_blocking_pids(capture.pid)) AND EXISTS(SELECT 1 FROM pg_locks waiting WHERE waiting.pid=capture.pid AND waiting.locktype='advisory' AND NOT waiting.granted))")
                .bind(&capture_name).bind(&deletion_name).fetch_one(&owner).await.unwrap();
            if blocked {break;}
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }).await.expect("capture must wait for deletion before writing source content or retrieval chunks");
    let during:(Uuid,i64)=sqlx::query_as("SELECT current_version_id,(SELECT count(*) FROM atoms WHERE graph_id=$1) FROM source_items WHERE source_item_id=$2")
        .bind(a.2).bind(old_item).fetch_one(&owner).await.unwrap();
    assert_eq!(during, (old_version, 1));
    barrier.commit().await.unwrap();
    let removed = tokio::time::timeout(std::time::Duration::from_secs(10), deletion)
        .await
        .expect("atomic deletion completes")
        .unwrap()
        .unwrap();
    assert_eq!(removed.atoms_deleted.len(), 1);
    tokio::time::timeout(std::time::Duration::from_secs(10), capture)
        .await
        .expect("recapture completes after deletion")
        .unwrap();

    // Serialized outcome: the old item/version/chunk are gone; recapture has
    // a new item, current snapshot and exactly matching searchable chunk.
    let current:(Uuid,Uuid,String)=sqlx::query_as("SELECT s.source_item_id,v.version_id,v.raw_inline FROM source_items s JOIN source_versions v ON v.version_id=s.current_version_id AND v.graph_id=s.graph_id WHERE s.graph_id=$1 AND s.source_id=$2")
        .bind(a.2).bind(native).fetch_one(&owner).await.unwrap();
    assert_ne!(current.0, old_item);
    assert_ne!(current.1, old_version);
    assert_eq!(current.2, new_text);
    let live_chunks:Vec<(Uuid,String,Uuid)>=sqlx::query_as("SELECT a.atom_id,a.content_inline,c.source_version_id FROM atoms a JOIN source_version_chunks c ON c.atom_id=a.atom_id AND c.graph_id=a.graph_id WHERE a.graph_id=$1")
        .bind(a.2).fetch_all(&owner).await.unwrap();
    assert_eq!(live_chunks.len(), 1);
    assert!(!old_chunks.contains(&live_chunks[0].0));
    assert!(live_chunks[0].1.contains("CurrentMarigoldMarker"));
    assert!(!live_chunks[0].1.contains("ObsoleteXylophoneMarker"));
    assert_eq!(live_chunks[0].2, current.1);
    let leftovers:(i64,i64,i64)=sqlx::query_as("SELECT (SELECT count(*) FROM source_versions WHERE version_id=$1),(SELECT count(*) FROM atoms WHERE atom_id=ANY($2)),(SELECT count(*) FROM atoms a WHERE graph_id=$3 AND NOT EXISTS(SELECT 1 FROM source_version_chunks c WHERE c.atom_id=a.atom_id AND c.graph_id=a.graph_id))")
        .bind(old_version).bind(&old_chunks).bind(a.2).fetch_one(&owner).await.unwrap();
    assert_eq!(
        leftovers,
        (0, 0, 0),
        "no stale or unowned searchable chunk may survive"
    );
    let jobs:i64=sqlx::query_scalar("SELECT count(*) FROM jobs WHERE kind='ExtractSourceAtoms' AND account_id=$1 AND payload->>'source_version_id'=$2")
        .bind(a.1).bind(current.1.to_string()).fetch_one(&owner).await.unwrap();
    assert_eq!(jobs, 0, "reingest retains evidence without implicit managed admission");
    cleanup_source_account(&owner, a.1, a.0).await;
    drop(state);
    capture_pool.close().await;
    deletion_pool.close().await;
    owner.close().await;
}

#[tokio::test]
async fn source_uuid_deletion_completes_with_one_connection_and_stale_uuid_is_inert() {
    use coralswarm_backend::services::{
        connector_audit::Actor, deletion::delete_source_item_in_graph,
    };
    let Some(options) = isolated_options() else {
        return;
    };
    let owner = PgPoolOptions::new()
        .max_connections(2)
        .connect_with(options.clone())
        .await
        .unwrap();
    support::app_role::set_app_role_password_with_retry(&owner).await;
    let capture_pool = source_test_app_pool(&options, "source-one-connection-capture", 4).await;
    let deletion_pool = source_test_app_pool(&options, "source-one-connection-delete", 1).await;
    let a = seed_account(&owner).await;
    let state = source_test_state(&capture_pool, a.1);
    let user = coralswarm_backend::UserContext {
        user_id: a.0,
        account_id: a.1,
        role: "owner".into(),
        device_id: "source-one-connection".into(),
    };
    let native = "single-connection-mail";
    let namespace = "google_gmail:user:one-connection";
    ingest_source_observation(
        &state,
        &user,
        a.2,
        native,
        namespace,
        "Ada: delete this source.",
    )
    .await;
    let old_item: Uuid = sqlx::query_scalar(
        "SELECT source_item_id FROM source_items WHERE graph_id=$1 AND source_id=$2",
    )
    .bind(a.2)
    .bind(native)
    .fetch_one(&owner)
    .await
    .unwrap();
    let removed = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        delete_source_item_in_graph(
            &deletion_pool,
            None,
            "source-one-connection",
            a.2,
            old_item,
            Actor::User(a.0),
            None,
        ),
    )
    .await
    .expect("source delete must commit and audit with max_connections=1")
    .unwrap();
    assert_eq!(removed.atoms_deleted.len(), 1);
    let counts:(i64,i64,i64)=sqlx::query_as("SELECT (SELECT count(*) FROM source_items WHERE graph_id=$1),(SELECT count(*) FROM source_versions WHERE graph_id=$1),(SELECT count(*) FROM atoms WHERE graph_id=$1)")
        .bind(a.2).fetch_one(&owner).await.unwrap();
    assert_eq!(counts, (0, 0, 0));
    ingest_source_observation(
        &state,
        &user,
        a.2,
        native,
        namespace,
        "Ada: this later observation is independent.",
    )
    .await;
    let new_item: Uuid = sqlx::query_scalar(
        "SELECT source_item_id FROM source_items WHERE graph_id=$1 AND source_id=$2",
    )
    .bind(a.2)
    .bind(native)
    .fetch_one(&owner)
    .await
    .unwrap();
    assert_ne!(new_item, old_item);
    let stale = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        delete_source_item_in_graph(
            &deletion_pool,
            None,
            "source-one-connection",
            a.2,
            old_item,
            Actor::User(a.0),
            None,
        ),
    )
    .await
    .expect("stale UUID deletion must release its one connection")
    .unwrap();
    assert!(stale.atoms_deleted.is_empty());
    let preserved: Uuid =
        sqlx::query_scalar("SELECT source_item_id FROM source_items WHERE graph_id=$1")
            .bind(a.2)
            .fetch_one(&owner)
            .await
            .unwrap();
    assert_eq!(
        preserved, new_item,
        "an old UUID never means a newly recreated identity"
    );
    cleanup_source_account(&owner, a.1, a.0).await;
    drop(state);
    capture_pool.close().await;
    deletion_pool.close().await;
    owner.close().await;
}

#[tokio::test]
async fn semantic_only_subject_deletion_removes_its_source_and_preserves_unrelated_evidence() {
    use coralswarm_backend::{
        source_evidence::{capture_source_version_tx, CaptureSource},
        tenancy::scope::begin_account_scoped,
    };
    let Some(options) = isolated_options() else {
        return;
    };
    let owner = PgPoolOptions::new()
        .max_connections(2)
        .connect_with(options.clone())
        .await
        .unwrap();
    support::app_role::set_app_role_password_with_retry(&owner).await;
    let app = source_test_app_pool(&options, "semantic-only-subject", 1).await;
    let a = seed_account(&owner).await;
    let mut fixtures = vec![];
    for (source, title, text) in [
        ("subject-source", "Ada", "Ada approved the launch."),
        ("unrelated-source", "Weather", "Rain is expected tomorrow."),
    ] {
        let mut tx = begin_account_scoped(&app, a.1).await.unwrap();
        let captured = capture_source_version_tx(
            &mut tx,
            None,
            a.2,
            a.1,
            &CaptureSource {
                source_type: "note",
                provider_namespace: "declared:subject-fixture",
                source_id: source,
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
        let semantic:Uuid=sqlx::query_scalar("INSERT INTO semantic_atoms(graph_id,source_version_id,ordinal,kind,statement,extraction_version,model) VALUES($1,$2,0,'fact',$3,'source-atoms-v1','fixture') RETURNING semantic_atom_id")
            .bind(a.2).bind(captured.version_id).bind(text).fetch_one(&mut *tx).await.unwrap();
        let passage:Uuid=sqlx::query_scalar("INSERT INTO semantic_atom_passages(graph_id,source_version_id,semantic_atom_id,start_char,end_char,quote) VALUES($1,$2,$3,0,$4,$5) RETURNING passage_id")
            .bind(a.2).bind(captured.version_id).bind(semantic).bind(text.chars().count() as i32).bind(text).fetch_one(&mut *tx).await.unwrap();
        let node: Uuid = sqlx::query_scalar(
            "INSERT INTO nodes(graph_id,type,title) VALUES($1,'entity',$2) RETURNING node_id",
        )
        .bind(a.2)
        .bind(title)
        .fetch_one(&mut *tx)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO node_semantic_atoms(graph_id,node_id,semantic_atom_id) VALUES($1,$2,$3)",
        )
        .bind(a.2)
        .bind(node)
        .bind(semantic)
        .execute(&mut *tx)
        .await
        .unwrap();
        tx.commit().await.unwrap();
        fixtures.push((
            captured.source_item_id,
            captured.version_id,
            semantic,
            passage,
            node,
        ));
    }
    let chunk_count: i64 = sqlx::query_scalar("SELECT count(*) FROM atoms WHERE graph_id=$1")
        .bind(a.2)
        .fetch_one(&owner)
        .await
        .unwrap();
    assert_eq!(
        chunk_count, 0,
        "the subject is known only through immutable semantic evidence"
    );
    let deleted = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        coralswarm_backend::services::deletion::delete_subject(
            &app,
            None,
            "semantic-subject-regression",
            a.2,
            fixtures[0].4,
            "admin",
            a.0,
            None,
        ),
    )
    .await
    .expect("semantic-only subject retraction completes with one connection")
    .unwrap();
    assert!(
        !deleted.surface_outcomes.is_empty(),
        "subject deletion must report the removed source evidence"
    );
    assert!(deleted
        .deleted_reef_ids
        .iter()
        .any(|(_, id)| *id == fixtures[0].4));
    let removed:(bool,bool,bool,bool,bool)=sqlx::query_as("SELECT EXISTS(SELECT 1 FROM source_items WHERE source_item_id=$1),EXISTS(SELECT 1 FROM source_versions WHERE version_id=$2),EXISTS(SELECT 1 FROM semantic_atoms WHERE semantic_atom_id=$3),EXISTS(SELECT 1 FROM semantic_atom_passages WHERE passage_id=$4),EXISTS(SELECT 1 FROM nodes WHERE node_id=$5)")
        .bind(fixtures[0].0).bind(fixtures[0].1).bind(fixtures[0].2).bind(fixtures[0].3).bind(fixtures[0].4).fetch_one(&owner).await.unwrap();
    assert_eq!(removed, (false, false, false, false, false));
    let preserved:(Uuid,Uuid,Uuid,Uuid,Uuid)=sqlx::query_as("SELECT s.source_item_id,v.version_id,a.semantic_atom_id,p.passage_id,n.node_id FROM source_items s JOIN source_versions v ON v.version_id=s.current_version_id AND v.graph_id=s.graph_id JOIN semantic_atoms a ON a.source_version_id=v.version_id AND a.graph_id=v.graph_id JOIN semantic_atom_passages p ON p.semantic_atom_id=a.semantic_atom_id AND p.graph_id=a.graph_id JOIN node_semantic_atoms n ON n.semantic_atom_id=a.semantic_atom_id AND n.graph_id=a.graph_id WHERE s.graph_id=$1")
        .bind(a.2).fetch_one(&owner).await.unwrap();
    assert_eq!(
        preserved, fixtures[1],
        "unrelated source, passage and subject must survive"
    );
    cleanup_source_account(&owner, a.1, a.0).await;
    app.close().await;
    owner.close().await;
}

#[derive(Default)]
struct SourceDeletionBlobProbe(std::sync::Mutex<Vec<String>>);
#[async_trait::async_trait]
impl coralswarm_backend::services::blob_store::PromotionBlobStore for SourceDeletionBlobProbe {
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
        self.0.lock().unwrap().push(key.to_owned());
        Ok(())
    }
}

#[tokio::test]
async fn source_delete_commit_failure_preserves_evidence_and_never_deletes_blobs() {
    use coralswarm_backend::services::{
        connector_audit::Actor, deletion::delete_source_item_in_graph,
    };
    let Some(options) = isolated_options() else {
        return;
    };
    let owner = PgPoolOptions::new()
        .max_connections(2)
        .connect_with(options.clone())
        .await
        .unwrap();
    support::app_role::set_app_role_password_with_retry(&owner).await;
    let capture_pool = source_test_app_pool(&options, "source-rollback-capture", 4).await;
    let deletion_pool = source_test_app_pool(&options, "source-rollback-delete", 1).await;
    let a = seed_account(&owner).await;
    let state = source_test_state(&capture_pool, a.1);
    let user = coralswarm_backend::UserContext {
        user_id: a.0,
        account_id: a.1,
        role: "owner".into(),
        device_id: "source-rollback".into(),
    };
    ingest_source_observation(
        &state,
        &user,
        a.2,
        "rollback-mail",
        "google_gmail:user:rollback",
        "Ada: retain this private source after rollback.",
    )
    .await;
    let (item, version): (Uuid, Uuid) = sqlx::query_as(
        "SELECT source_item_id,current_version_id FROM source_items WHERE graph_id=$1",
    )
    .bind(a.2)
    .fetch_one(&owner)
    .await
    .unwrap();
    let source_key = format!("{}/{}", a.2, Uuid::new_v4());
    sqlx::query("UPDATE source_versions SET raw_blob_key=$2 WHERE version_id=$1")
        .bind(version)
        .bind(&source_key)
        .execute(&owner)
        .await
        .unwrap();
    let chunk_key: String =
        sqlx::query_scalar("SELECT content_blob_key FROM atoms WHERE graph_id=$1")
            .bind(a.2)
            .fetch_one(&owner)
            .await
            .unwrap();
    let fault = format!("source_delete_fault_{}", Uuid::new_v4().simple());
    // Fail at COMMIT, after the actual production path has removed its rows
    // and queued both cleanup keys. The database must roll back the whole unit.
    sqlx::raw_sql(&format!("CREATE FUNCTION {fault}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF OLD.graph_id='{}'::uuid THEN RAISE EXCEPTION 'forced source deletion commit failure'; END IF; RETURN NULL; END $$; CREATE CONSTRAINT TRIGGER {fault} AFTER DELETE ON source_items DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION {fault}();",a.2))
        .execute(&owner).await.unwrap();
    let blobs = SourceDeletionBlobProbe::default();
    let failed = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        delete_source_item_in_graph(
            &deletion_pool,
            Some(&blobs),
            "source-rollback",
            a.2,
            item,
            Actor::User(a.0),
            None,
        ),
    )
    .await
    .expect("failing source delete must release its connection");
    assert!(
        failed.is_err(),
        "a rejected transaction must never report deletion success"
    );
    assert!(
        blobs.0.lock().unwrap().is_empty(),
        "rollback may not leave a referenced blob deleted"
    );
    let retained:(i64,i64,i64,i64,i64)=sqlx::query_as("SELECT (SELECT count(*) FROM source_items WHERE source_item_id=$1),(SELECT count(*) FROM source_versions WHERE version_id=$2),(SELECT count(*) FROM atoms WHERE graph_id=$3),(SELECT count(*) FROM source_version_chunks WHERE graph_id=$3),(SELECT count(*) FROM jobs WHERE account_id=$4 AND kind='DeleteSourceBlob')")
        .bind(item).bind(version).bind(a.2).bind(a.1).fetch_one(&owner).await.unwrap();
    assert_eq!(
        retained,
        (1, 1, 1, 1, 0),
        "content, mapping and cleanup intent must roll back together"
    );
    sqlx::raw_sql(&format!(
        "DROP TRIGGER {fault} ON source_items; DROP FUNCTION {fault}();"
    ))
    .execute(&owner)
    .await
    .unwrap();
    let removed = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        delete_source_item_in_graph(
            &deletion_pool,
            Some(&blobs),
            "source-rollback",
            a.2,
            item,
            Actor::User(a.0),
            None,
        ),
    )
    .await
    .expect("retry must complete with one connection")
    .unwrap();
    assert_eq!(removed.atoms_deleted.len(), 1);
    assert!(
        blobs.0.lock().unwrap().is_empty(),
        "committed deletion delegates blob cleanup to durable jobs"
    );
    let mut keys:Vec<String>=sqlx::query_scalar("SELECT payload->>'blob_key' FROM jobs WHERE account_id=$1 AND kind='DeleteSourceBlob' AND status='pending'")
        .bind(a.1).fetch_all(&owner).await.unwrap();
    keys.sort();
    let mut expected = vec![source_key, chunk_key];
    expected.sort();
    assert_eq!(
        keys, expected,
        "both owned keys become recoverable cleanup intent only on commit"
    );
    let surviving: i64 = sqlx::query_scalar("SELECT count(*) FROM source_items WHERE graph_id=$1")
        .bind(a.2)
        .fetch_one(&owner)
        .await
        .unwrap();
    assert_eq!(surviving, 0);
    cleanup_source_account(&owner, a.1, a.0).await;
    drop(state);
    capture_pool.close().await;
    deletion_pool.close().await;
    owner.close().await;
}

#[tokio::test]
async fn source_uuid_deletion_preserves_literal_chunk_suffix_native_id() {
    use coralswarm_backend::{
        services::{connector_audit::Actor, deletion::delete_source_item_in_graph},
        source_evidence::lock_identity_tx,
    };
    let Some(options) = isolated_options() else {
        return;
    };
    let owner = PgPoolOptions::new()
        .max_connections(3)
        .connect_with(options.clone())
        .await
        .unwrap();
    support::app_role::set_app_role_password_with_retry(&owner).await;
    let deletion_name = format!("literal-delete-{}", Uuid::new_v4().simple());
    let deletion_pool = source_test_app_pool(&options, &deletion_name, 1).await;
    let capture_pool = source_test_app_pool(&options, "literal-source-capture", 4).await;
    for mapped_parent in [true, false] {
        let a = seed_account(&owner).await;
        let state = source_test_state(&capture_pool, a.1);
        let user = coralswarm_backend::UserContext {
            user_id: a.0,
            account_id: a.1,
            role: "owner".into(),
            device_id: "literal-source".into(),
        };
        let namespace = "google_gmail:user:literal-mailbox";
        if mapped_parent {
            ingest_source_observation(
                &state,
                &user,
                a.2,
                "memo",
                namespace,
                "ParentOnlyMarker remains available.",
            )
            .await;
        } else {
            // This pre-generic parent has no preserved source identity or mapping.
            // Its retrieval ID happens to equal the new source's full native ID.
            sqlx::query("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,timestamp) VALUES($1,'gmail','memo:chunk:0',$2,'LegacyParentOnlyMarker remains available.',now())")
                .bind(a.2).bind(format!("{}/{}",a.2,Uuid::new_v4())).execute(&owner).await.unwrap();
        }
        let parent_chunk: Uuid = sqlx::query_scalar(
            "SELECT atom_id FROM atoms WHERE graph_id=$1 AND source_id='memo:chunk:0'",
        )
        .bind(a.2)
        .fetch_one(&owner)
        .await
        .unwrap();
        let parent_source: Option<(Uuid, Uuid)> = sqlx::query_as("SELECT source_item_id,current_version_id FROM source_items WHERE graph_id=$1 AND source_id='memo'")
            .bind(a.2).fetch_optional(&owner).await.unwrap();
        assert_eq!(parent_source.is_some(), mapped_parent);
        ingest_source_observation(
            &state,
            &user,
            a.2,
            "memo:chunk:0",
            namespace,
            "LiteralNativeOnlyMarker is a different source.",
        )
        .await;
        let (literal_item,literal_version,literal_chunk):(Uuid,Uuid,Uuid)=sqlx::query_as("SELECT s.source_item_id,s.current_version_id,c.atom_id FROM source_items s JOIN source_version_chunks c ON c.source_version_id=s.current_version_id AND c.graph_id=s.graph_id JOIN atoms a ON a.atom_id=c.atom_id AND a.graph_id=c.graph_id WHERE s.graph_id=$1 AND s.source_id='memo:chunk:0' AND a.source_id='memo:chunk:0:chunk:0'")
            .bind(a.2).fetch_one(&owner).await.unwrap();
        assert_ne!(literal_chunk, parent_chunk);
        let parent_mappings: i64 =
            sqlx::query_scalar("SELECT count(*) FROM source_version_chunks WHERE atom_id=$1")
                .bind(parent_chunk)
                .fetch_one(&owner)
                .await
                .unwrap();
        assert_eq!(parent_mappings, if mapped_parent { 1 } else { 0 });

        // Hold the precise literal identity lock. Observe the real delete
        // waiting on this backend, proving it does not strip the native suffix.
        let mut barrier = owner.begin().await.unwrap();
        lock_identity_tx(&mut barrier, a.2, "memo:chunk:0")
            .await
            .unwrap();
        let barrier_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut *barrier)
            .await
            .unwrap();
        let deletion = tokio::spawn({
            let pool = deletion_pool.clone();
            let graph = a.2;
            let actor = a.0;
            async move {
                delete_source_item_in_graph(
                    &pool,
                    None,
                    "literal-source",
                    graph,
                    literal_item,
                    Actor::User(actor),
                    None,
                )
                .await
            }
        });
        tokio::time::timeout(std::time::Duration::from_secs(10),async {
            loop {
                let waiting:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity activity WHERE application_name=$1 AND $2=ANY(pg_blocking_pids(activity.pid)) AND EXISTS(SELECT 1 FROM pg_locks l WHERE l.pid=activity.pid AND l.locktype='advisory' AND NOT l.granted))")
                    .bind(&deletion_name).bind(barrier_pid).fetch_one(&owner).await.unwrap();
                if waiting {break;}
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        }).await.expect("UUID deletion must acquire the verbatim native identity lock");
        barrier.commit().await.unwrap();
        let removed = tokio::time::timeout(std::time::Duration::from_secs(10), deletion)
            .await
            .expect("literal native deletion completes")
            .unwrap()
            .unwrap();
        assert_eq!(removed.atoms_deleted.len(), 1);
        let removed_counts:(i64,i64,i64)=sqlx::query_as("SELECT (SELECT count(*) FROM source_items WHERE source_item_id=$1),(SELECT count(*) FROM source_versions WHERE version_id=$2),(SELECT count(*) FROM atoms WHERE atom_id=$3)")
            .bind(literal_item).bind(literal_version).bind(literal_chunk).fetch_one(&owner).await.unwrap();
        assert_eq!(removed_counts, (0, 0, 0));
        let survivors: Vec<Uuid> =
            sqlx::query_scalar("SELECT atom_id FROM atoms WHERE graph_id=$1")
                .bind(a.2)
                .fetch_all(&owner)
                .await
                .unwrap();
        assert_eq!(
            survivors,
            vec![parent_chunk],
            "the parent chunk survives whether mapped or truly legacy-only"
        );
        let preserved_parent:Option<(Uuid,Uuid)>=sqlx::query_as("SELECT source_item_id,current_version_id FROM source_items WHERE graph_id=$1 AND source_id='memo'")
            .bind(a.2).fetch_optional(&owner).await.unwrap();
        assert_eq!(preserved_parent, parent_source);
        let mappings: i64 =
            sqlx::query_scalar("SELECT count(*) FROM source_version_chunks WHERE atom_id=$1")
                .bind(parent_chunk)
                .fetch_one(&owner)
                .await
                .unwrap();
        assert_eq!(mappings, parent_mappings);
        cleanup_source_account(&owner, a.1, a.0).await;
        drop(state);
    }
    capture_pool.close().await;
    deletion_pool.close().await;
    owner.close().await;
}

#[tokio::test]
async fn source_uuid_deletion_preserves_a_verified_chunk_shared_by_another_namespace() {
    use coralswarm_backend::{
        services::{connector_audit::Actor, deletion::delete_source_item_in_graph},
        source_evidence::{capture_source_version_tx, link_verified_chunks_tx, CaptureSource},
        tenancy::scope::begin_account_scoped,
    };
    let Some(options) = isolated_options() else {
        return;
    };
    let owner = PgPoolOptions::new()
        .max_connections(2)
        .connect_with(options.clone())
        .await
        .unwrap();
    support::app_role::set_app_role_password_with_retry(&owner).await;
    let app = source_test_app_pool(&options, "shared-verified-source", 1).await;
    let a = seed_account(&owner).await;
    let text = "Ada approved the shared launch.";
    let mut fixtures = vec![];
    let mut shared_chunk = None;
    let chunk_key = format!("{}/{}", a.2, Uuid::new_v4());
    for namespace in ["slack:workspace:one", "slack:workspace:two"] {
        let mut tx = begin_account_scoped(&app, a.1).await.unwrap();
        let captured = capture_source_version_tx(
            &mut tx,
            None,
            a.2,
            a.1,
            &CaptureSource {
                source_type: "slack",
                provider_namespace: namespace,
                source_id: "shared-native",
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
        let chunk = match shared_chunk {
            Some(id) => id,
            None => {
                let id:Uuid=sqlx::query_scalar("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,timestamp) VALUES($1,'slack','shared-native:chunk:0',$2,$3,now()) RETURNING atom_id")
                    .bind(a.2).bind(&chunk_key).bind(text).fetch_one(&mut *tx).await.unwrap();
                shared_chunk = Some(id);
                id
            }
        };
        // A verified identical body can legitimately back multiple namespaces.
        // Never infer this mapping from the positional retrieval ID alone.
        let bodies:(String,String)=sqlx::query_as("SELECT v.raw_inline,a.content_inline FROM source_versions v JOIN atoms a ON a.graph_id=v.graph_id WHERE v.version_id=$1 AND a.atom_id=$2")
            .bind(captured.version_id).bind(chunk).fetch_one(&mut *tx).await.unwrap();
        assert_eq!(bodies, (text.to_owned(), text.to_owned()));
        link_verified_chunks_tx(&mut tx, a.2, captured.version_id, &[chunk])
            .await
            .unwrap();
        let semantic:Uuid=sqlx::query_scalar("INSERT INTO semantic_atoms(graph_id,source_version_id,ordinal,kind,statement,extraction_version,model) VALUES($1,$2,0,'decision',$3,'source-atoms-v1','fixture') RETURNING semantic_atom_id")
            .bind(a.2).bind(captured.version_id).bind(text).fetch_one(&mut *tx).await.unwrap();
        let passage:Uuid=sqlx::query_scalar("INSERT INTO semantic_atom_passages(graph_id,source_version_id,semantic_atom_id,start_char,end_char,quote) VALUES($1,$2,$3,0,$4,$5) RETURNING passage_id")
            .bind(a.2).bind(captured.version_id).bind(semantic).bind(text.chars().count() as i32).bind(text).fetch_one(&mut *tx).await.unwrap();
        let node: Uuid = sqlx::query_scalar(
            "INSERT INTO nodes(graph_id,type,title) VALUES($1,'entity',$2) RETURNING node_id",
        )
        .bind(a.2)
        .bind(namespace)
        .fetch_one(&mut *tx)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO node_semantic_atoms(graph_id,node_id,semantic_atom_id) VALUES($1,$2,$3)",
        )
        .bind(a.2)
        .bind(node)
        .bind(semantic)
        .execute(&mut *tx)
        .await
        .unwrap();
        tx.commit().await.unwrap();
        fixtures.push((
            captured.source_item_id,
            captured.version_id,
            semantic,
            passage,
            node,
        ));
    }
    let chunk = shared_chunk.unwrap();
    assert_ne!(fixtures[0].0, fixtures[1].0);
    for (index, fixture) in fixtures.iter().enumerate() {
        let removed = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            delete_source_item_in_graph(
                &app,
                None,
                "verified-shared-source",
                a.2,
                fixture.0,
                Actor::User(a.0),
                None,
            ),
        )
        .await
        .expect("one-connection source deletion completes")
        .unwrap();
        assert_eq!(
            removed.atoms_deleted.len(),
            index,
            "only the final owner removes the shared chunk"
        );
        let absent:(bool,bool,bool,bool,bool)=sqlx::query_as("SELECT EXISTS(SELECT 1 FROM source_items WHERE source_item_id=$1),EXISTS(SELECT 1 FROM source_versions WHERE version_id=$2),EXISTS(SELECT 1 FROM semantic_atoms WHERE semantic_atom_id=$3),EXISTS(SELECT 1 FROM semantic_atom_passages WHERE passage_id=$4),EXISTS(SELECT 1 FROM nodes WHERE node_id=$5)")
            .bind(fixture.0).bind(fixture.1).bind(fixture.2).bind(fixture.3).bind(fixture.4).fetch_one(&owner).await.unwrap();
        assert_eq!(absent, (false, false, false, false, false));
        let remaining:(i64,i64,i64)=sqlx::query_as("SELECT (SELECT count(*) FROM atoms WHERE atom_id=$1),(SELECT count(*) FROM source_version_chunks WHERE atom_id=$1),(SELECT count(*) FROM jobs WHERE account_id=$2 AND kind='DeleteSourceBlob' AND payload->>'blob_key'=$3)")
            .bind(chunk).bind(a.1).bind(&chunk_key).fetch_one(&owner).await.unwrap();
        if index == 0 {
            assert_eq!(
                remaining,
                (1, 1, 0),
                "a live owner's retrieval chunk must not be queued for deletion"
            );
            let preserved:(Uuid,Uuid,Uuid,Uuid,Uuid)=sqlx::query_as("SELECT s.source_item_id,v.version_id,a.semantic_atom_id,p.passage_id,n.node_id FROM source_items s JOIN source_versions v ON v.version_id=s.current_version_id AND v.graph_id=s.graph_id JOIN semantic_atoms a ON a.source_version_id=v.version_id AND a.graph_id=v.graph_id JOIN semantic_atom_passages p ON p.semantic_atom_id=a.semantic_atom_id AND p.graph_id=a.graph_id JOIN node_semantic_atoms n ON n.semantic_atom_id=a.semantic_atom_id AND n.graph_id=a.graph_id JOIN nodes topic ON topic.node_id=n.node_id AND topic.graph_id=n.graph_id WHERE s.graph_id=$1")
                .bind(a.2).fetch_one(&owner).await.unwrap();
            assert_eq!(
                preserved, fixtures[1],
                "the other namespace keeps its complete evidence and node association"
            );
            let mapped_version: Uuid = sqlx::query_scalar(
                "SELECT source_version_id FROM source_version_chunks WHERE atom_id=$1",
            )
            .bind(chunk)
            .fetch_one(&owner)
            .await
            .unwrap();
            assert_eq!(mapped_version, fixtures[1].1);
        } else {
            assert_eq!(
                remaining,
                (0, 0, 1),
                "the last owner removes the chunk and queues its blob exactly once"
            );
        }
    }
    cleanup_source_account(&owner, a.1, a.0).await;
    app.close().await;
    owner.close().await;
}

/// Attribution is independently observed: it must not fork content snapshots,
/// rewrite retained evidence, merge provider IDs, or escape graph authorization.
#[tokio::test]
async fn source_origin_preserves_identity_snapshots_and_authorized_projections() {
    use coralswarm_backend::{
        source_evidence::{
            capture_source_version_tx, capture_source_version_with_origin_tx, CaptureSource,
        },
        source_origin::SourceOrigin,
        tenancy::scope::begin_account_scoped,
    };
    let options =
        isolated_options().expect("origin integration requires disposable local PostgreSQL");
    let owner = PgPoolOptions::new()
        .max_connections(2)
        .connect_with(options.clone())
        .await
        .unwrap();
    coralswarm_backend::CONTROL_MIGRATOR
        .run(&owner)
        .await
        .expect("apply real control migrations to disposable test database");
    support::app_role::set_app_role_password_with_retry(&owner).await;
    let pool = source_test_app_pool(&options, "source-origin-contract", 3).await;
    let a = seed_account(&owner).await;
    let b = seed_account(&owner).await;
    let origin_a: SourceOrigin = serde_json::from_value(json!({
        "service_id":"slack","transport":"native","content_kind":"message",
        "capture_surface":"slack_message_events",
        "connection":{"kind":"oauth_grant","id":"fixture-grant-a"},
        "container":{"kind":"channel","id":"C1","provider_namespace":"slack:workspace:T1","label":"Engineering"}
    })).unwrap();
    let mut origin_b = origin_a.clone();
    origin_b.connection.as_mut().unwrap().id =
        coralswarm_backend::source_origin::CaptureConnectionId::new("fixture-grant-b").unwrap();
    origin_b.container.as_mut().unwrap().label = Some("Renamed channel".into());
    let metadata = json!({"title":"Retained message"});
    let mut snapshots = vec![];
    for (text, origin) in [
        ("first content", Some(&origin_a)),
        ("first content", Some(&origin_a)),
        ("first content", Some(&origin_b)),
        ("first content", None),
        ("changed content", Some(&origin_b)),
    ] {
        let mut tx = begin_account_scoped(&pool, a.1).await.unwrap();
        let capture = CaptureSource {
            source_type: "slack",
            provider_namespace: "slack:workspace:T1",
            source_id: "C1:123",
            normalized_text: text,
            representation: "normalized_text",
            metadata: &metadata,
            occurred_at: None,
            participants: &[],
            ingested_by: Some(a.0),
            scope: "private",
            duration_ms: None,
            segments: None,
        };
        let captured = if origin.is_some() {
            capture_source_version_with_origin_tx(
                &mut tx, None, a.2, a.1, &capture, None, &[42; 32], origin,
            )
            .await
            .unwrap()
        } else {
            capture_source_version_tx(&mut tx, None, a.2, a.1, &capture, None, &[42; 32])
                .await
                .unwrap()
        };
        tx.commit().await.unwrap();
        if snapshots.len() == 2 || snapshots.len() == 3 {
            let retained: Value =
                sqlx::query_scalar("SELECT origin FROM source_items WHERE source_item_id=$1")
                    .bind(captured.source_item_id)
                    .fetch_one(&owner)
                    .await
                    .unwrap();
            assert_eq!(
                retained,
                json!(origin_b),
                "origin-only recapture updates item; None retains it"
            );
            let retained_snapshot: Value =
                sqlx::query_scalar("SELECT origin FROM source_versions WHERE version_id=$1")
                    .bind(captured.version_id)
                    .fetch_one(&owner)
                    .await
                    .unwrap();
            assert_eq!(
                retained_snapshot,
                json!(origin_a),
                "recapture cannot rewrite snapshot attribution"
            );
        }
        snapshots.push((
            captured.source_item_id,
            captured.version_id,
            captured.changed,
        ));
    }
    for retry in &snapshots[1..4] {
        assert_eq!(*retry, (snapshots[0].0, snapshots[0].1, false));
    }
    assert_eq!(snapshots[4].0, snapshots[0].0);
    assert_ne!(snapshots[4].1, snapshots[0].1);
    let versions:Vec<(Uuid,Value)>=sqlx::query_as("SELECT version_id,origin FROM source_versions WHERE graph_id=$1 ORDER BY captured_at,version_id")
        .bind(a.2).fetch_all(&owner).await.unwrap();
    assert_eq!(
        versions,
        vec![
            (snapshots[0].1, json!(origin_a)),
            (snapshots[4].1, json!(origin_b))
        ]
    );
    let jobs: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM jobs WHERE account_id=$1 AND kind='ExtractSourceAtoms'",
    )
    .bind(a.1)
    .fetch_one(&owner)
    .await
    .unwrap();
    assert_eq!(jobs, 0, "origin changes cannot authorize unmanaged extraction");
    let item_origin: Value =
        sqlx::query_scalar("SELECT origin FROM source_items WHERE source_item_id=$1")
            .bind(snapshots[0].0)
            .fetch_one(&owner)
            .await
            .unwrap();
    assert_eq!(item_origin, json!(origin_b));

    // Same provider-local ID under another namespace stays a distinct item;
    // the legacy wrapper supplies no inferred attribution.
    let mut tx = begin_account_scoped(&pool, a.1).await.unwrap();
    let capture = CaptureSource {
        source_type: "slack",
        provider_namespace: "slack:workspace:T2",
        source_id: "C1:123",
        normalized_text: "first content",
        representation: "normalized_text",
        metadata: &metadata,
        occurred_at: None,
        participants: &[],
        ingested_by: Some(a.0),
        scope: "private",
        duration_ms: None,
        segments: None,
    };
    let other = capture_source_version_tx(&mut tx, None, a.2, a.1, &capture, None, &[42; 32])
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert_ne!(other.source_item_id, snapshots[0].0);
    let absent:(Option<Value>,Option<Value>)=sqlx::query_as("SELECT s.origin,v.origin FROM source_items s JOIN source_versions v ON v.version_id=s.current_version_id WHERE s.source_item_id=$1")
        .bind(other.source_item_id).fetch_one(&owner).await.unwrap();
    assert_eq!(absent, (None, None));

    // Invalid optional labels fail before persisting a source or queuing work.
    let mut invalid = origin_b.clone();
    invalid.container.as_mut().unwrap().label = Some("x".repeat(257));
    let mut tx = begin_account_scoped(&pool, a.1).await.unwrap();
    assert!(capture_source_version_with_origin_tx(
        &mut tx,
        None,
        a.2,
        a.1,
        &capture,
        None,
        &[42; 32],
        Some(&invalid)
    )
    .await
    .is_err());
    tx.rollback().await.unwrap();

    let state = source_test_state(&pool, a.1);
    let user = coralswarm_backend::UserContext {
        user_id: a.0,
        account_id: a.1,
        role: "owner".into(),
        device_id: "origin-fixture".into(),
    };
    let app = Router::new()
        .route(
            "/items",
            axum::routing::get(coralswarm_backend::source_evidence::list_items),
        )
        .route(
            "/evidence",
            axum::routing::get(coralswarm_backend::source_evidence::get_evidence),
        )
        .layer(axum::Extension(user))
        .with_state(state.clone());
    let get = |uri: String| Request::builder().uri(uri).body(Body::empty()).unwrap();
    let response = app
        .clone()
        .oneshot(get(format!("/items?graph_id={}", a.2)))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let inventory: Value = serde_json::from_slice(
        &axum::body::to_bytes(response.into_body(), 1_000_000)
            .await
            .unwrap(),
    )
    .unwrap();
    let item = inventory["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|i| i["source_item_id"] == json!(snapshots[0].0))
        .unwrap();
    assert_eq!(item["origin"], json!(origin_b));
    assert_eq!(
        item["capture_contract"],
        json!({"history":"unknown","updates":"unsupported","deletes":"unsupported","reconnect":"unknown"})
    );
    assert!(item.get("raw_text").is_none() && item.get("raw_blob_key").is_none());
    let response = app
        .clone()
        .oneshot(get(format!(
            "/evidence?graph_id={}&source_item_id={}&version_id={}",
            a.2, snapshots[0].0, snapshots[0].1
        )))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let evidence: Value = serde_json::from_slice(
        &axum::body::to_bytes(response.into_body(), 1_000_000)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(evidence["source"]["origin"], json!(origin_b));
    assert_eq!(evidence["selected_version"]["origin"], json!(origin_a));
    assert_eq!(evidence["selected_version"]["raw_text"], "first content");
    let response = app
        .clone()
        .oneshot(get(format!(
            "/evidence?graph_id={}&source_item_id={}",
            b.2, snapshots[0].0
        )))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let response = app
        .oneshot(get(format!("/items?graph_id={}", b.2)))
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        StatusCode::NOT_FOUND,
        "attribution never weakens graph authorization"
    );
    cleanup_source_account(&owner, a.1, a.0).await;
    cleanup_source_account(&owner, b.1, b.0).await;
    drop(state);
    pool.close().await;
    owner.close().await;
}

#[tokio::test]
async fn trusted_native_origin_preserves_declared_namespace_and_untrusted_http_origin_is_ignored() {
    use coralswarm_backend::source_origin::SourceOrigin;
    let options =
        isolated_options().expect("origin compatibility requires disposable local PostgreSQL");
    let owner = PgPoolOptions::new()
        .max_connections(2)
        .connect_with(options.clone())
        .await
        .unwrap();
    coralswarm_backend::CONTROL_MIGRATOR
        .run(&owner)
        .await
        .unwrap();
    support::app_role::set_app_role_password_with_retry(&owner).await;
    let pool = source_test_app_pool(&options, "trusted-origin-compatibility", 3).await;
    let a = seed_account(&owner).await;
    let state = source_test_state(&pool, a.1);
    let tenant = coralswarm_backend::tenancy::extractor::Tenant::legacy(&state);
    let user = coralswarm_backend::UserContext {
        user_id: a.0,
        account_id: a.1,
        role: "owner".into(),
        device_id: "origin-compatibility".into(),
    };
    let request = || {
        serde_json::from_value(json!({"graph_id":a.2,"source_type":"slack","source_id":"C1/123","content":"Verified native message",
        "timestamp":"2026-10-01T12:00:00Z","channel_id":"C1","participants":[],
        "origin":{"service_id":"forged","transport":"native","content_kind":"message"}})).unwrap()
    };
    let namespace = format!("declared:user:{}", a.0);
    let response = coralswarm_backend::test_ingest_with_source_context(
        &state,
        &tenant,
        &user,
        request(),
        &namespace,
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let initial: (Uuid, Uuid, Option<Value>) = sqlx::query_as(
        "SELECT source_item_id,current_version_id,origin FROM source_items WHERE graph_id=$1",
    )
    .bind(a.2)
    .fetch_one(&owner)
    .await
    .unwrap();
    assert!(
        initial.2.is_none(),
        "HTTP request fields cannot attest native origins"
    );
    let origin = SourceOrigin::native("slack", "message", "slack_member_capture")
        .unwrap()
        .with_container("channel", "C1", "slack:workspace:T1", None)
        .unwrap()
        .with_oauth_grant(a.1, "slack_user", "T1", Some(a.0))
        .unwrap();
    for _ in 0..2 {
        let response = coralswarm_backend::test_ingest_with_origin_context(
            &state,
            &tenant,
            &user,
            request(),
            None,
            &origin,
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
    }
    let current:(Uuid,Uuid,String,Value)=sqlx::query_as("SELECT source_item_id,current_version_id,provider_namespace,origin FROM source_items WHERE graph_id=$1")
        .bind(a.2).fetch_one(&owner).await.unwrap();
    assert_eq!((current.0, current.1), (initial.0, initial.1));
    assert_eq!(current.2, namespace);
    assert_eq!(current.3, json!(origin));
    let snapshot_origin: Option<Value> =
        sqlx::query_scalar("SELECT origin FROM source_versions WHERE version_id=$1")
            .bind(initial.1)
            .fetch_one(&owner)
            .await
            .unwrap();
    assert!(
        snapshot_origin.is_none(),
        "trusted enrichment never rewrites historical attribution"
    );
    let demo_origin:SourceOrigin=serde_json::from_value(json!({"service_id":"linear","transport":"synthetic","content_kind":"issue","capture_surface":"synthetic_demo"})).unwrap();
    let mut demo = request();
    demo.source_type = "note".into();
    demo.source_id = "demo:issue:1".into();
    let response = coralswarm_backend::test_ingest_with_origin_context(
        &state,
        &tenant,
        &user,
        demo,
        Some("demo:linear:workspace:fixture"),
        &demo_origin,
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let classified:(Value,Value)=sqlx::query_as("SELECT v.source_metadata,v.origin FROM source_versions v JOIN source_items s ON s.source_item_id=v.source_item_id WHERE s.graph_id=$1 AND s.source_id='demo:issue:1'")
        .bind(a.2).fetch_one(&owner).await.unwrap();
    assert_eq!(classified.0["synthetic_demo"], true);
    assert_eq!(classified.1["transport"], "synthetic");
    cleanup_source_account(&owner, a.1, a.0).await;
    drop(state);
    pool.close().await;
    owner.close().await;
}

/// Time-only re-captures: a GitHub capture whose snapshot differs from the
/// current version ONLY in `occurred_at` is that version (GitHub derives the
/// time from whichever head/`updated_at` a capture read; 2026-10-18 sweeps
/// minted a version per pass for byte-identical files). Every other source
/// keeps the time in its snapshot: a rescheduled event with the same body is
/// a real new version. A real content change is a new version for both.
#[tokio::test]
async fn time_only_recapture_is_one_version_for_github_and_a_new_version_otherwise() {
    use chrono::Timelike;
    use coralswarm_backend::{
        source_evidence::{capture_source_version_tx, CaptureSource},
        tenancy::scope::begin_account_scoped,
    };
    let Some(options) = isolated_options() else {
        return;
    };
    let owner = PgPoolOptions::new()
        .max_connections(2)
        .connect_with(options.clone())
        .await
        .unwrap();
    support::app_role::set_app_role_password_with_retry(&owner).await;
    let app = source_test_app_pool(&options, "time-only-recapture", 1).await;
    let a = seed_account(&owner).await;
    let first = Utc::now() - Duration::hours(2);
    let later = first + Duration::minutes(20);
    let edited = first + Duration::minutes(45);

    for (source_type, namespace, time_only_is_new) in [
        ("github_code", "github:repository:77", false),
        ("github_pr", "github:repository:77", false),
        ("google_calendar", "google:calendar:primary", true),
    ] {
        let native = format!("{source_type}:fixture");
        let mut versions = Vec::new();
        for (text, at) in [("Body one.", first), ("Body one.", later), ("Body two.", edited)] {
            let mut tx = begin_account_scoped(&app, a.1).await.unwrap();
            let captured = capture_source_version_tx(
                &mut tx,
                None,
                a.2,
                a.1,
                &CaptureSource {
                    source_type,
                    provider_namespace: namespace,
                    source_id: &native,
                    normalized_text: text,
                    representation: "normalized_text",
                    metadata: &json!({"title": "fixture"}),
                    occurred_at: Some(at),
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
            tx.commit().await.unwrap();
            versions.push((captured.version_id, captured.changed));
        }
        assert!(versions[0].1, "{source_type}: first capture is a version");
        assert_eq!(
            versions[1].1, time_only_is_new,
            "{source_type}: a time-only re-capture changed={time_only_is_new}"
        );
        assert_eq!(versions[1].0 != versions[0].0, time_only_is_new, "{source_type}: version identity");
        assert!(versions[2].1, "{source_type}: a content change is a new version");
        let times: Vec<Option<chrono::DateTime<Utc>>> = sqlx::query_scalar(
            "SELECT occurred_at FROM source_versions WHERE graph_id=$1 AND source_id=$2 ORDER BY captured_at,version_id",
        )
        .bind(a.2)
        .bind(&native)
        .fetch_all(&owner)
        .await
        .unwrap();
        let round = |t: chrono::DateTime<Utc>| Some(t.with_nanosecond((t.nanosecond() / 1000) * 1000).unwrap());
        let expected = if time_only_is_new {
            vec![round(first), round(later), round(edited)]
        } else {
            vec![round(first), round(edited)]
        };
        assert_eq!(times, expected, "{source_type}: retained versions and their times");
    }
    cleanup_source_account(&owner, a.1, a.0).await;
    app.close().await;
    owner.close().await;
}

/// A paragraph of `words` distinct words; each paragraph is near the generic
/// chunker's token target, so positions map to paragraphs.
fn paragraph(tag: &str, words: usize) -> String {
    (0..words).map(|i| format!("{tag}w{i}")).collect::<Vec<_>>().join(" ") + "."
}

async fn ingest_note(
    state: &Arc<coralswarm_backend::AppState>,
    user: &coralswarm_backend::UserContext,
    graph: Uuid,
    native: &str,
    text: &str,
) -> Value {
    let tenant = coralswarm_backend::tenancy::extractor::Tenant::legacy(state);
    let request = serde_json::from_value(json!({
        "graph_id":graph,"source_type":"note","source_id":native,"content":text,
        "timestamp":"2026-10-01T12:00:00Z","participants":["Ada"],
    }))
    .unwrap();
    let response = coralswarm_backend::test_ingest_with_source_context(
        state, &tenant, user, request, "declared:test:notes",
    )
    .await;
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), 1_000_000).await.unwrap();
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    serde_json::from_slice(&body).unwrap()
}

/// `(position, atom_id, content_revision, text after header_len)` of every
/// positional chunk of `native`, by position.
async fn positional_atoms(pool: &PgPool, graph: Uuid, native: &str) -> Vec<(i64, Uuid, i64, Option<String>)> {
    sqlx::query_as(
        "SELECT substring(source_id FROM char_length($2)+8)::bigint AS pos,atom_id,content_revision,\
                substring(content_inline FROM COALESCE(header_len,0)+1) \
           FROM atoms WHERE graph_id=$1 AND source_type='note' \
            AND left(source_id,char_length($2)+7)=$2||':chunk:' ORDER BY pos",
    )
    .bind(graph)
    .bind(native)
    .fetch_all(pool)
    .await
    .unwrap()
}

/// Chunk atoms linked to the item's CURRENT version.
async fn current_chunk_ids(pool: &PgPool, graph: Uuid, native: &str) -> (Uuid, Vec<Uuid>) {
    let version: Uuid = sqlx::query_scalar(
        "SELECT current_version_id FROM source_items WHERE graph_id=$1 AND source_id=$2",
    )
    .bind(graph)
    .bind(native)
    .fetch_one(pool)
    .await
    .unwrap();
    let ids = sqlx::query_scalar(
        "SELECT atom_id FROM source_version_chunks WHERE graph_id=$1 AND source_version_id=$2 ORDER BY atom_id",
    )
    .bind(graph)
    .bind(version)
    .fetch_all(pool)
    .await
    .unwrap();
    (version, ids)
}

/// Content-verified positional ingest (adopt or replace): an edited
/// re-capture replaces stale positions in place, adopts unchanged ones by
/// hash with no new atom row, drops positions past the new end, and links
/// EVERY chunk to the new version. Before the fix the edit kept the old text
/// at every existing position and the new version had zero chunk rows.
/// Run under the non-bypass app role and the table owner (prod's tenant
/// runtime connects as the owner).
#[tokio::test]
async fn edited_reingest_adopts_unchanged_replaces_edited_and_drops_surplus() {
    use coralswarm_core::chunker::{chunk, ChunkStrategy};
    let Some(options) = isolated_options() else {
        return;
    };
    let owner = PgPoolOptions::new()
        .max_connections(4)
        .connect_with(options.clone())
        .await
        .unwrap();
    support::app_role::set_app_role_password_with_retry(&owner).await;
    let app = source_test_app_pool(&options, "adopt-or-replace", 4).await;
    let (a, b, c, d) = (paragraph("alpha", 280), paragraph("bravo", 280), paragraph("charlie", 280), paragraph("delta", 280));
    let v1 = [a.as_str(), b.as_str(), c.as_str(), d.as_str()].join("\n\n");
    let c_edited = paragraph("CHARLIE", 280);
    let v2 = [a.as_str(), b.as_str(), c_edited.as_str()].join("\n\n");
    let chunks1: Vec<String> = chunk(&v1, ChunkStrategy::for_source_type("note")).iter().map(|c| c.text_with_speaker()).collect();
    let chunks2: Vec<String> = chunk(&v2, ChunkStrategy::for_source_type("note")).iter().map(|c| c.text_with_speaker()).collect();
    assert!(chunks1.len() > chunks2.len() && chunks2.len() >= 3, "fixture: {} -> {} chunks", chunks1.len(), chunks2.len());
    assert_eq!(chunks1[0], chunks2[0], "fixture: position 0 is unchanged");
    assert_ne!(chunks1[chunks2.len() - 1], chunks2[chunks2.len() - 1], "fixture: the last kept position is edited");

    for (label, pool) in [("app_role", &app), ("owner", &owner)] {
        let acct = seed_account(&owner).await;
        let state = source_test_state(pool, acct.1);
        let user = coralswarm_backend::UserContext {
            user_id: acct.0,
            account_id: acct.1,
            role: "owner".into(),
            device_id: "adopt-or-replace".into(),
        };
        let native = "doc-edit";

        let first = ingest_note(&state, &user, acct.2, native, &v1).await;
        let before = positional_atoms(&owner, acct.2, native).await;
        assert_eq!(before.len(), chunks1.len(), "{label}: {first}");
        let (v1_id, v1_chunks) = current_chunk_ids(&owner, acct.2, native).await;
        assert_eq!(v1_chunks.len(), chunks1.len(), "{label}: first capture links every chunk");

        let second = ingest_note(&state, &user, acct.2, native, &v2).await;
        let after = positional_atoms(&owner, acct.2, native).await;
        assert_eq!(after.len(), chunks2.len(), "{label}: surplus positions are dropped: {second}");
        for (pos, (old, new)) in before.iter().zip(after.iter()).enumerate() {
            assert_eq!(old.1, new.1, "{label}: position {pos} keeps its atom identity (no new atom row)");
            assert_eq!(new.3.as_deref(), Some(chunks2[pos].as_str()), "{label}: position {pos} holds the NEW text");
            if chunks1[pos] == chunks2[pos] {
                assert_eq!(old.2, new.2, "{label}: unchanged position {pos} is adopted without a content write");
            } else {
                assert!(new.2 > old.2, "{label}: edited position {pos} bumps its content revision");
            }
        }
        let (v2_id, mut v2_chunks) = current_chunk_ids(&owner, acct.2, native).await;
        assert_ne!(v1_id, v2_id, "{label}: the edit is a new version");
        let mut expected: Vec<Uuid> = after.iter().map(|r| r.1).collect();
        expected.sort();
        v2_chunks.sort();
        assert_eq!(v2_chunks, expected, "{label}: every chunk of the edit is linked to the new version");
        let replaced = chunks1.iter().zip(chunks2.iter()).filter(|(x, y)| x != y).count();
        assert_eq!(second["replaced_chunks"], replaced, "{label}: {second}");
        assert_eq!(second["retired_chunks"], chunks1.len() - chunks2.len(), "{label}: {second}");
        assert_eq!(second["skipped_duplicate_chunks"], chunks2.len() - replaced, "{label}: {second}");
        let embedded: Vec<Value> = sqlx::query_scalar(
            "SELECT payload->'atom_ids' FROM jobs WHERE account_id=$1 AND kind='EmbedAtoms' ORDER BY created_at",
        )
        .bind(acct.1)
        .fetch_all(&owner)
        .await
        .unwrap();
        assert_eq!(embedded.len(), 2, "{label}: one embed job per capture that wrote content");
        assert_eq!(embedded[1].as_array().unwrap().len(), replaced, "{label}: only replaced chunks re-embed");
        let stale_links: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM source_version_chunks c LEFT JOIN atoms a ON a.atom_id=c.atom_id \
              WHERE c.graph_id=$1 AND c.source_version_id=$2 AND a.atom_id IS NULL",
        )
        .bind(acct.2)
        .bind(v1_id)
        .fetch_one(&owner)
        .await
        .unwrap();
        assert_eq!(stale_links, 0);
        let hashed: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM atoms WHERE graph_id=$1 AND source_type='note' AND chunk_sha256 IS NOT NULL",
        )
        .bind(acct.2)
        .fetch_one(&owner)
        .await
        .unwrap();
        assert_eq!(hashed as usize, chunks2.len(), "{label}: every live chunk carries its hash");

        // An identical re-capture is a no-op: same version, no write.
        let third = ingest_note(&state, &user, acct.2, native, &v2).await;
        assert_eq!(third["atom_ids"].as_array().unwrap().len(), 0, "{label}: {third}");
        assert_eq!(third["skipped_duplicate_chunks"], chunks2.len());
        assert_eq!(positional_atoms(&owner, acct.2, native).await, after, "{label}: nothing rewritten");
        assert_eq!(current_chunk_ids(&owner, acct.2, native).await.0, v2_id);

        cleanup_source_account(&owner, acct.1, acct.0).await;
        drop(state);
    }
    app.close().await;
    owner.close().await;
}

/// A legacy positional atom (written before versions existed, never linked)
/// whose text equals the re-captured chunk is ADOPTED by hash and linked to
/// the new version; one whose text differs is replaced. Neither gets a new
/// atom row. A same-native-id atom owned by ANOTHER namespace is never
/// rewritten (covered by `admitted_sources_use_versions_...`).
#[tokio::test]
async fn legacy_positional_atoms_are_adopted_by_hash_or_replaced() {
    let Some(options) = isolated_options() else {
        return;
    };
    let owner = PgPoolOptions::new()
        .max_connections(4)
        .connect_with(options.clone())
        .await
        .unwrap();
    support::app_role::set_app_role_password_with_retry(&owner).await;
    let app = source_test_app_pool(&options, "legacy-adopt", 4).await;
    let acct = seed_account(&owner).await;
    let state = source_test_state(&app, acct.1);
    let user = coralswarm_backend::UserContext {
        user_id: acct.0,
        account_id: acct.1,
        role: "owner".into(),
        device_id: "legacy-adopt".into(),
    };
    let mut legacy = Vec::new();
    for (native, stored) in [("same-text", "Ship the release on Friday."), ("old-text", "Ship the release on Monday.")] {
        let id: Uuid = sqlx::query_scalar(
            "INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,timestamp) \
             VALUES($1,'note',$2,$3,$4,now()) RETURNING atom_id",
        )
        .bind(acct.2)
        .bind(format!("{native}:chunk:0"))
        .bind(format!("{}/{}", acct.2, Uuid::new_v4()))
        .bind(stored)
        .fetch_one(&owner)
        .await
        .unwrap();
        legacy.push((native, id));
    }
    for (native, id) in &legacy {
        let body = ingest_note(&state, &user, acct.2, native, "Ship the release on Friday.").await;
        let rows = positional_atoms(&owner, acct.2, native).await;
        assert_eq!(rows.len(), 1, "{native}: {body}");
        assert_eq!(rows[0].1, *id, "{native}: the legacy atom keeps its identity");
        assert_eq!(rows[0].3.as_deref(), Some("Ship the release on Friday."));
        let (_, linked) = current_chunk_ids(&owner, acct.2, native).await;
        assert_eq!(linked, vec![*id], "{native}: the legacy atom is linked to the new version");
        // First-time binding flips `source_evidence_bound`, which the reef
        // identity fingerprint covers, so the revision moves either way;
        // the content locator only moves on a replacement.
        let (bound, superseded): (bool, Vec<String>) =
            sqlx::query_as("SELECT source_evidence_bound,superseded_blob_keys FROM atoms WHERE atom_id=$1")
                .bind(id)
                .fetch_one(&owner)
                .await
                .unwrap();
        assert!(bound, "{native}: linking marks the atom source-bound");
        if *native == "same-text" {
            assert!(superseded.is_empty(), "matching legacy text is adopted without a content write");
            assert_eq!(body["replaced_chunks"], 0);
            assert_eq!(body["skipped_duplicate_chunks"], 1);
        } else {
            assert_eq!(superseded.len(), 1, "differing legacy text is replaced in place");
            assert_eq!(body["replaced_chunks"], 1);
        }
    }
    cleanup_source_account(&owner, acct.1, acct.0).await;
    drop(state);
    app.close().await;
    owner.close().await;
}
