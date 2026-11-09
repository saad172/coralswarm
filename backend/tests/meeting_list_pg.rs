//! Real HTTP, authorization, RLS and keyset-pagination coverage for meeting
//! source inventory. Uses a migrated, disposable LOCAL Postgres only:
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

async fn seed_meeting(pool: &PgPool, graph_id: Uuid, source_id: &str, created_at: DateTime<Utc>) {
    sqlx::query(
        "INSERT INTO meeting_transcripts \
         (graph_id, source_id, created_at, duration_ms, summary_status, raw_inline, raw_blob_key) \
         VALUES ($1, $2, $3, 12345, 'done', 'Private transcript text', 'private/blob/key')",
    )
    .bind(graph_id)
    .bind(source_id)
    .bind(created_at)
    .execute(pool)
    .await
    .unwrap();
}

async fn seed_atom(
    pool: &PgPool,
    graph_id: Uuid,
    source_id: &str,
    occurred_at: DateTime<Utc>,
    participants: &[&str],
) {
    sqlx::query(
        "INSERT INTO atoms(graph_id, source_type, source_id, content_blob_key, timestamp, participants) \
         VALUES ($1, 'teams_transcript', $2, 'private/atom/key', $3, $4)",
    )
    .bind(graph_id)
    .bind(source_id)
    .bind(occurred_at)
    .bind(participants)
    .execute(pool)
    .await
    .unwrap();
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

fn source_ids(body: &Value) -> Vec<&str> {
    body["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|item| item["source_id"].as_str().unwrap())
        .collect()
}

#[tokio::test]
async fn meeting_list_is_scoped_and_pages_exact_sources_without_leaking_content() {
    let Ok(url) = std::env::var("RELINFER_TEST_DATABASE_URL") else {
        eprintln!("skipping meeting list HTTP test: set RELINFER_TEST_DATABASE_URL");
        return;
    };
    let opts = PgConnectOptions::from_str(&url).unwrap();
    assert!(
        matches!(opts.get_host(), "127.0.0.1" | "localhost" | "::1"),
        "test database must be local"
    );
    let owner = PgPoolOptions::new()
        .max_connections(4)
        .connect_with(opts.clone())
        .await
        .unwrap();
    support::app_role::set_app_role_password_with_retry(&owner).await;
    let app_pool = PgPoolOptions::new()
        .max_connections(4)
        .connect_with(
            opts.username("coralswarm_app")
                .password(support::app_role::APP_ROLE_TEST_PASSWORD),
        )
        .await
        .unwrap();
    let a = seed_account(&owner).await;
    let b = seed_account(&owner).await;
    let base: DateTime<Utc> = "2026-10-01T12:00:00.123456Z".parse().unwrap();
    let occurred_at = base - Duration::days(2);
    const SPECIAL: &str = "meeting:x/%_\\?#💬";
    for (source_id, captured_at) in [
        ("meeting:newer", base + Duration::seconds(1)),
        ("meeting:z", base),
        ("meeting:y", base),
        (SPECIAL, base),
        ("meeting:older", base - Duration::seconds(1)),
    ] {
        seed_meeting(&owner, a.2, source_id, captured_at).await;
    }
    seed_meeting(&owner, b.2, "private:other-account", base).await;

    // Exact source IDs, numeric chunk boundaries and literal wildcard characters
    // must all survive enrichment. Sibling-prefix, nonnumeric and other-graph
    // rows contain deliberately earlier dates and different people.
    seed_atom(
        &owner,
        a.2,
        SPECIAL,
        occurred_at + Duration::hours(1),
        &["Bob"],
    )
    .await;
    seed_atom(
        &owner,
        a.2,
        &format!("{SPECIAL}:chunk:0"),
        occurred_at,
        &["Ada", "Bob"],
    )
    .await;
    seed_atom(
        &owner,
        a.2,
        &format!("{SPECIAL}:chunk:10"),
        occurred_at + Duration::hours(2),
        &["Chloé"],
    )
    .await;
    for sibling in [
        format!("{SPECIAL}-other:chunk:0"),
        format!("{SPECIAL}:chunk:0-extra"),
        format!("{SPECIAL}:chunk:nope"),
    ] {
        seed_atom(
            &owner,
            a.2,
            &sibling,
            base - Duration::days(20),
            &["Wrong sibling"],
        )
        .await;
    }
    seed_atom(
        &owner,
        a.2,
        "meeting:x/anythingZ\\?#💬:chunk:0",
        base - Duration::days(20),
        &["Wildcard mismatch"],
    )
    .await;
    seed_atom(
        &owner,
        b.2,
        SPECIAL,
        base - Duration::days(30),
        &["Other account"],
    )
    .await;

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
    // This is the only test in this binary and the only env mutation.
    unsafe {
        std::env::set_var("CLERK_ISSUER", ISSUER);
        std::env::set_var("CLERK_JWKS_URL", jwks_url);
    }
    let state = coralswarm_backend::test_app_state_owned_with_db_and_store(app_pool.clone(), None);
    let app = coralswarm_backend::test_support_app(Arc::new(state));
    let token_a = mint(
        json!({"iss": ISSUER, "sub": a.3, "exp": (Utc::now() + Duration::hours(1)).timestamp()}),
    );
    let token_b = mint(
        json!({"iss": ISSUER, "sub": b.3, "exp": (Utc::now() + Duration::hours(1)).timestamp()}),
    );
    let list_uri = format!("/v1/meetings?graph_id={}&limit=2", a.2);
    assert_eq!(get(&app, None, &list_uri).await.0, StatusCode::UNAUTHORIZED);
    assert_eq!(
        get(&app, Some(&token_b), &list_uri).await.0,
        StatusCode::NOT_FOUND
    );
    let (status, first) = get(&app, Some(&token_a), &list_uri).await;
    assert_eq!(status, StatusCode::OK, "{first}");
    assert_eq!(source_ids(&first), ["meeting:newer", "meeting:z"]);
    assert_eq!(first["has_more"], true);
    assert_eq!(first["items"][0]["summary_status"], "done");
    assert_eq!(first["items"][0]["duration_ms"], 12345);
    for field in ["title", "occurred_at", "participants"] {
        assert!(first["items"][0][field].is_null());
    }
    let first_cursor = first["next_cursor"].as_str().unwrap();
    seed_meeting(
        &owner,
        a.2,
        "meeting:inserted-after-first-page",
        base + Duration::hours(1),
    )
    .await;
    let (status, second) = get(
        &app,
        Some(&token_a),
        &format!("{list_uri}&cursor={first_cursor}"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{second}");
    assert_eq!(source_ids(&second), ["meeting:y", SPECIAL]);
    let special = &second["items"][1];
    assert_eq!(special["occurred_at"], json!(occurred_at));
    assert_eq!(special["created_at"], json!(base));
    assert_eq!(special["participants"], json!(["Ada", "Bob", "Chloé"]));
    let second_cursor = second["next_cursor"].as_str().unwrap();
    let (status, last) = get(
        &app,
        Some(&token_a),
        &format!("{list_uri}&cursor={second_cursor}"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{last}");
    assert_eq!(source_ids(&last), ["meeting:older"]);
    assert_eq!(last["has_more"], false);
    assert!(last["next_cursor"].is_null());
    for page in [&first, &second, &last] {
        let rendered = page.to_string();
        assert!(!rendered.contains("Private transcript"));
        assert!(!rendered.contains("private/blob/key"));
        assert!(!rendered.contains("private/atom/key"));
    }
    let (status, _) = get(
        &app,
        Some(&token_a),
        &format!("{list_uri}&cursor=not-a-cursor"),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    // Authorization precedes cursor decoding, so malformed input cannot probe
    // whether a graph in another account exists.
    assert_eq!(
        get(&app, Some(&token_b), &format!("{list_uri}&cursor=invalid"))
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    let (status, missing) = get(
        &app,
        Some(&token_a),
        &format!("/v1/meetings?graph_id={}", Uuid::new_v4()),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, denied) = get(
        &app,
        Some(&token_a),
        &format!("/v1/meetings?graph_id={}", b.2),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(missing, denied);
    let (status, _) = get(
        &app,
        Some(&token_b),
        &format!("/v1/meetings?graph_id={}&cursor={first_cursor}", b.2),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "cursor is bound to its original graph"
    );

    // A legitimately shared account must scope content to the ocean's owning
    // account, even when the caller's active account belongs elsewhere.
    sqlx::query("INSERT INTO account_members(account_id, user_id, role) VALUES ($1, $2, 'member')")
        .bind(a.1)
        .bind(b.0)
        .execute(&owner)
        .await
        .unwrap();
    let (status, shared) = get(&app, Some(&token_b), &list_uri).await;
    assert_eq!(status, StatusCode::OK, "{shared}");
    assert_eq!(source_ids(&shared)[0], "meeting:inserted-after-first-page");

    // No session scope leaks back into the next pooled request.
    let (status, own_b) = get(
        &app,
        Some(&token_b),
        &format!("/v1/meetings?graph_id={}", b.2),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{own_b}");
    assert_eq!(source_ids(&own_b), ["private:other-account"]);
    jwks_task.abort();
    drop(app);
    app_pool.close().await;
    sqlx::query("DELETE FROM meeting_transcripts WHERE graph_id = ANY($1)")
        .bind([a.2, b.2].as_slice())
        .execute(&owner)
        .await
        .unwrap();
    for account_id in [a.1, b.1] {
        let mut financial = coralswarm_backend::tenancy::scope::begin_account_scoped(&owner, account_id).await.unwrap();
        coralswarm_backend::billing::renewal::lock_period_rotation(&mut financial, account_id).await.unwrap();
        sqlx::query("DELETE FROM accounts WHERE id = ANY($1)")
            .bind([account_id].as_slice())
            .execute(&mut *financial)
            .await
            .unwrap();
        financial.commit().await.unwrap();
    }
    sqlx::query("DELETE FROM users WHERE id = ANY($1)")
        .bind([a.0, b.0].as_slice())
        .execute(&owner)
        .await
        .unwrap();
    owner.close().await;
}
