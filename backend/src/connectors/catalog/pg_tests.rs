//! Postgres tests for the catalog's reads, driven through the real
//! `GET /v1/connectors[/{provider}]` handlers as `coralswarm_app` (the
//! production role, subject to RLS).
//!
//! Gated on `RELINFER_TEST_DATABASE_URL`, a migrated CONTROL database, like every
//! billing pg suite; without it each test prints a skip line and returns. A skip
//! is NOT a pass.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use serde_json::{json, Value};
use sqlx::PgPool;
use tower::ServiceExt;
use uuid::Uuid;

use super::{connect, pause, registry, AdapterCx, Facts};
use crate::billing::test_support::{body_json, pg_test_pool, seed_personal_account, user_context};
use crate::middleware::auth::UserContext;

async fn app_pool(owner: &PgPool) -> PgPool {
    use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
    use std::str::FromStr;
    // Roles are cluster-wide and the tests run in parallel: set the password once,
    // or concurrent `ALTER ROLE`s fail with "tuple concurrently updated".
    static ROLE: tokio::sync::OnceCell<()> = tokio::sync::OnceCell::const_new();
    ROLE.get_or_init(|| async {
        sqlx::query("ALTER ROLE coralswarm_app WITH LOGIN PASSWORD 'app_test_pw'")
            .execute(owner)
            .await
            .unwrap();
    })
    .await;
    let url = std::env::var("RELINFER_TEST_DATABASE_URL").unwrap();
    let opts = PgConnectOptions::from_str(&url)
        .unwrap()
        .username("coralswarm_app")
        .password("app_test_pw");
    PgPoolOptions::new()
        .max_connections(4)
        .connect_with(opts)
        .await
        .unwrap()
}

fn router(app: PgPool, user: UserContext) -> Router {
    let mut state = crate::test_app_state_owned_with_db(app.clone(), None);
    state.store = Some(std::sync::Arc::new(crate::store::PostgresStore::new(app)));
    let state = std::sync::Arc::new(state);
    Router::new()
        .nest("/v1", crate::v1_routes().layer(axum::Extension(user)))
        .with_state(state)
}

async fn get(router: &Router, uri: &str) -> (StatusCode, Value) {
    let req = Request::builder().uri(uri).body(Body::empty()).unwrap();
    let resp = router.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    (status, body_json(resp).await)
}

async fn token(
    owner: &PgPool,
    account: Uuid,
    owner_user: Option<Uuid>,
    provider: &str,
    workspace: &str,
) {
    sqlx::query(
        "INSERT INTO oauth_tokens (account_id, owner_user_id, provider, scope, access_token, provider_workspace_id) \
         VALUES ($1, $2, $3, 'scope', '\\x00', $4)",
    )
    .bind(account)
    .bind(owner_user)
    .bind(provider)
    .bind(workspace)
    .execute(owner)
    .await
    .unwrap();
}

fn connector<'a>(list: &'a Value, key: &str) -> &'a Value {
    list["connectors"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["key"] == key)
        .unwrap_or_else(|| panic!("no {key} in {list}"))
}

#[tokio::test]
async fn a_fresh_account_lists_ten_not_connected_connectors_pg() {
    let Some(owner) = pg_test_pool("catalog::fresh").await else {
        return;
    };
    let (account, user) = seed_personal_account(&owner).await;
    let app = app_pool(&owner).await;
    let r = router(app, user_context(account, user));

    let (status, list) = get(&r, "/v1/connectors").await;
    assert_eq!(status, StatusCode::OK, "{list}");
    let all = list["connectors"].as_array().unwrap();
    assert_eq!(all.len(), 10);
    assert!(all.iter().all(|c| c["status"] == "not_connected"), "{list}");
    assert_eq!(all.iter().filter(|c| c["beta"] == true).count(), 6);
    assert!(!list.to_string().to_lowercase().contains("composio"));

    let (status, one) = get(&r, "/v1/connectors/jira").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(one["status"], "not_connected");
    assert!(one["connected_at"].is_null() && one["account_label"].is_null());
    assert_eq!(one["topics"], serde_json::json!([]));
    assert_eq!(
        get(&r, "/v1/connectors/nope").await.0,
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn a_member_sees_their_own_connections_and_never_a_colleagues_pg() {
    let Some(owner) = pg_test_pool("catalog::isolation").await else {
        return;
    };
    let (account, alice) = seed_personal_account(&owner).await;
    // A second member of the SAME account: the only thing separating them is the
    // `owner_user_id` predicate, so a leak would show here.
    let bob: Uuid = sqlx::query_scalar("INSERT INTO users (email) VALUES ($1) RETURNING id")
        .bind(format!("{}@catalog.test", Uuid::new_v4()))
        .fetch_one(&owner)
        .await
        .unwrap();
    token(&owner, account, Some(alice), "microsoft", "tenant-a").await;
    token(
        &owner,
        account,
        Some(alice),
        "google_gmail",
        "alice@example.com",
    )
    .await;
    sqlx::query(
        "INSERT INTO composio_connections (account_id, owner_user_id, service, connected_account_id, auth_config_id, status, provider_namespace) \
         VALUES ($1, $2, 'jira', 'ca_alice', 'ac_1', 'active', 'jira:site:acme')",
    )
    .bind(account)
    .bind(alice)
    .execute(&owner)
    .await
    .unwrap();

    let app = app_pool(&owner).await;

    // Positive control: Alice sees all three.
    let (_, a) = get(
        &router(app.clone(), user_context(account, alice)),
        "/v1/connectors",
    )
    .await;
    assert_eq!(connector(&a, "microsoft")["status"], "active", "{a}");
    assert_eq!(connector(&a, "google")["status"], "active", "{a}");
    assert_eq!(connector(&a, "google")["sources"][0]["key"], "gmail");
    assert_eq!(connector(&a, "google")["sources"][0]["status"], "active");
    assert_eq!(connector(&a, "google")["sources"][1]["status"], "off");
    assert_eq!(connector(&a, "jira")["status"], "active", "{a}");
    assert_eq!(connector(&a, "slack")["status"], "not_connected");

    // Bob, same account, sees none of them.
    let rb = router(app.clone(), user_context(account, bob));
    let (_, b) = get(&rb, "/v1/connectors").await;
    for c in b["connectors"].as_array().unwrap() {
        assert_eq!(c["status"], "not_connected", "{b}");
    }
    let (_, detail) = get(&rb, "/v1/connectors/microsoft").await;
    assert!(detail["connected_at"].is_null(), "{detail}");

    // Alice's detail carries the label and connect time.
    let (_, ad) = get(
        &router(app, user_context(account, alice)),
        "/v1/connectors/google",
    )
    .await;
    assert_eq!(ad["account_label"], "alice@example.com");
    assert!(ad["connected_at"].is_string());
}

#[tokio::test]
async fn a_dead_credential_reads_reconnect_needed_and_a_source_row_drives_setup_pg() {
    let Some(owner) = pg_test_pool("catalog::health").await else {
        return;
    };
    let (account, user) = seed_personal_account(&owner).await;
    token(&owner, account, Some(user), "microsoft", "tenant").await;
    sqlx::query(
        "UPDATE oauth_tokens SET credential_invalid_at = now(), credential_invalid_reason = 'ms_grant_expired' \
         WHERE account_id = $1 AND provider = 'microsoft'",
    )
    .bind(account)
    .execute(&owner)
    .await
    .unwrap();
    token(&owner, account, Some(user), "slack_user", "T1").await;
    token(&owner, account, None, "slack", "T1").await;
    // A bot grant with every scope this build asks for, so nothing is stale.
    sqlx::query("UPDATE oauth_tokens SET scope = $2 WHERE account_id = $1 AND provider = 'slack'")
        .bind(account)
        .bind(crate::connectors::slack::slack_scopes())
        .execute(&owner)
        .await
        .unwrap();
    sqlx::query("INSERT INTO connector_sources (account_id, owner_user_id, provider, source, enabled) VALUES ($1, $2, 'slack', 'channels', true)")
        .bind(account)
        .bind(user)
        .execute(&owner)
        .await
        .unwrap();

    let r = router(app_pool(&owner).await, user_context(account, user));
    let (_, list) = get(&r, "/v1/connectors").await;

    let ms = connector(&list, "microsoft");
    assert_eq!(ms["status"], "reconnect_needed", "{ms}");
    assert_eq!(ms["problem"]["code"], "credential_invalid");
    assert_eq!(ms["problem"]["params"]["reason"], "ms_grant_expired");
    assert!(ms["sources"]
        .as_array()
        .unwrap()
        .iter()
        .all(|s| s["status"] == "stopped"));

    // A `connector_sources` row that has not finished setup keeps Slack in setup,
    // resuming at the scope step.
    let slack = connector(&list, "slack");
    assert_eq!(slack["status"], "setting_up", "{slack}");
    assert_eq!(slack["resume_step"], "scope");
    assert_eq!(slack["sources_on"], 1);
}

// ---------------------------------------------------------------------------
// Connect, complete and disconnect (B2)
// ---------------------------------------------------------------------------

/// Any method; an empty body (204) reads as `Value::Null`.
async fn send(
    router: &Router,
    method: &str,
    uri: &str,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let mut req = Request::builder().method(method).uri(uri);
    let body = match body {
        Some(b) => {
            req = req.header("content-type", "application/json");
            Body::from(b.to_string())
        }
        None => Body::empty(),
    };
    let resp = router
        .clone()
        .oneshot(req.body(body).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

async fn source_rows(
    owner: &PgPool,
    account: Uuid,
    user: Uuid,
    provider: &str,
) -> Vec<(String, bool, bool)> {
    sqlx::query_as(
        "SELECT source, enabled, setup_completed_at IS NOT NULL FROM connector_sources \
         WHERE account_id = $1 AND owner_user_id = $2 AND provider = $3 ORDER BY source",
    )
    .bind(account)
    .bind(user)
    .bind(provider)
    .fetch_all(owner)
    .await
    .unwrap()
}

fn state_on(pool: &PgPool) -> std::sync::Arc<crate::AppState> {
    std::sync::Arc::new(crate::test_app_state_owned_with_db(pool.clone(), None))
}

fn catalog_cx_parts(
    app: PgPool,
) -> (
    std::sync::Arc<crate::AppState>,
    crate::tenancy::extractor::Tenant,
) {
    let mut state = crate::test_app_state_owned_with_db(app.clone(), None);
    state.store = Some(std::sync::Arc::new(crate::store::PostgresStore::new(app)));
    let state = std::sync::Arc::new(state);
    let tenant = crate::tenancy::extractor::Tenant {
        store: None,
        pool: None,
        decision: None,
        class: crate::route_class::RouteClass::Other,
    };
    (state, tenant)
}

#[tokio::test]
async fn a_new_connection_reads_setting_up_until_setup_finishes_pg() {
    let Some(owner) = pg_test_pool("catalog::record_connect").await else {
        return;
    };
    let (account, user) = seed_personal_account(&owner).await;
    // What the Slack callback leaves behind: the member's token and the bot's.
    token(&owner, account, Some(user), "slack_user", "T1").await;
    token(&owner, account, None, "slack", "T1").await;
    sqlx::query("UPDATE oauth_tokens SET scope = $2 WHERE account_id = $1 AND provider = 'slack'")
        .bind(account)
        .bind(crate::connectors::slack::slack_scopes())
        .execute(&owner)
        .await
        .unwrap();
    let app = app_pool(&owner).await;
    let r = router(app.clone(), user_context(account, user));

    // Before any rows: a connection from before the catalog reads as set up.
    let (_, list) = get(&r, "/v1/connectors").await;
    assert_eq!(connector(&list, "slack")["status"], "active", "{list}");

    let slack = registry::lookup("slack").unwrap();
    connect::clear_sources(&state_on(&app), account, user, "slack")
        .await
        .unwrap();
    connect::record_connect(&state_on(&app), account, user, slack)
        .await
        .unwrap();
    assert_eq!(
        source_rows(&owner, account, user, "slack").await,
        vec![("channels".to_string(), false, false)]
    );
    let (_, list) = get(&r, "/v1/connectors").await;
    let s = connector(&list, "slack");
    assert_eq!(s["status"], "setting_up", "{s}");
    assert_eq!(s["resume_step"], "sources");
    assert_eq!(s["sources_on"], 0);

    // Recording again, as a reconnect does, never resets the member's choices.
    sqlx::query("UPDATE connector_sources SET enabled = true, scope_mode = 'everything', setup_completed_at = now() WHERE account_id = $1")
        .bind(account)
        .execute(&owner)
        .await
        .unwrap();
    connect::record_connect(&state_on(&app), account, user, slack)
        .await
        .unwrap();
    assert_eq!(
        source_rows(&owner, account, user, "slack").await,
        vec![("channels".to_string(), true, true)]
    );
    let (_, list) = get(&r, "/v1/connectors").await;
    assert_eq!(connector(&list, "slack")["status"], "active");
}

#[tokio::test]
async fn reconnecting_a_connection_from_before_the_catalog_keeps_it_set_up_pg() {
    let Some(owner) = pg_test_pool("catalog::legacy_reconnect").await else {
        return;
    };
    let (account, user) = seed_personal_account(&owner).await;
    token(&owner, account, None, "microsoft", "tenant").await;
    let app = app_pool(&owner).await;
    let r = router(app.clone(), user_context(account, user));
    let ms = registry::lookup("microsoft").unwrap();

    let (state, tenant) = catalog_cx_parts(app.clone());
    let uc = user_context(account, user);
    let facts = Facts::load(&state, &tenant, account, user).await.unwrap();
    let cx = AdapterCx::new(&facts, &state, &tenant, &uc);

    // `connect` runs this first, then the callback records the new rows.
    connect::keep_legacy_connection_set_up(&cx, ms)
        .await
        .unwrap();
    connect::record_connect(&state_on(&app), account, user, ms)
        .await
        .unwrap();

    let rows = source_rows(&owner, account, user, "microsoft").await;
    assert_eq!(rows.len(), 5, "{rows:?}");
    assert!(
        rows.iter().all(|(_, enabled, done)| *enabled && *done),
        "{rows:?}"
    );
    let (_, list) = get(&r, "/v1/connectors").await;
    assert_eq!(
        connector(&list, "microsoft")["status"],
        "active",
        "fixing a dead sign-in must not send the member back through setup"
    );

    // Doing it again changes nothing.
    let facts = Facts::load(&state, &tenant, account, user).await.unwrap();
    let cx = AdapterCx::new(&facts, &state, &tenant, &uc);
    connect::keep_legacy_connection_set_up(&cx, ms)
        .await
        .unwrap();
    assert_eq!(source_rows(&owner, account, user, "microsoft").await, rows);
}

#[tokio::test]
async fn disconnect_removes_the_sign_in_and_this_members_setup_and_nobody_elses_pg() {
    let Some(owner) = pg_test_pool("catalog::disconnect").await else {
        return;
    };
    let (account, alice) = seed_personal_account(&owner).await;
    let (other_account, bob) = seed_personal_account(&owner).await;
    for (a, u) in [(account, alice), (other_account, bob)] {
        // A personal account stores Microsoft account-wide.
        token(&owner, a, None, "microsoft", "tenant").await;
        connect::record_connect(
            &state_on(&owner),
            a,
            u,
            registry::lookup("microsoft").unwrap(),
        )
        .await
        .unwrap();
    }
    let app = app_pool(&owner).await;
    let ra = router(app.clone(), user_context(account, alice));
    let rb = router(app, user_context(other_account, bob));
    assert_eq!(
        connector(&get(&ra, "/v1/connectors").await.1, "microsoft")["status"],
        "setting_up"
    );

    let (status, body) = send(&ra, "DELETE", "/v1/connectors/microsoft", None).await;
    assert_eq!(status, StatusCode::NO_CONTENT, "{body}");
    let (_, list) = get(&ra, "/v1/connectors").await;
    assert_eq!(
        connector(&list, "microsoft")["status"],
        "not_connected",
        "{list}"
    );
    assert!(source_rows(&owner, account, alice, "microsoft")
        .await
        .is_empty());
    let left: i64 = sqlx::query_scalar("SELECT count(*) FROM oauth_tokens WHERE account_id = $1")
        .bind(account)
        .fetch_one(&owner)
        .await
        .unwrap();
    assert_eq!(left, 0);

    // Nothing left to disconnect.
    let (status, body) = send(&ra, "DELETE", "/v1/connectors/microsoft", None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["code"], "not_connected");

    // The other member's connection and setup are untouched.
    assert_eq!(
        connector(&get(&rb, "/v1/connectors").await.1, "microsoft")["status"],
        "setting_up"
    );
    assert_eq!(
        source_rows(&owner, other_account, bob, "microsoft")
            .await
            .len(),
        5
    );

    // An unknown connector is a 404 before anything else.
    assert_eq!(
        send(&ra, "DELETE", "/v1/connectors/nope", None).await.0,
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn a_personal_slack_disconnect_removes_the_member_and_the_workspace_app_pg() {
    let Some(owner) = pg_test_pool("catalog::slack_disconnect").await else {
        return;
    };
    let (account, user) = seed_personal_account(&owner).await;
    token(&owner, account, Some(user), "slack_user", "T1").await;
    token(&owner, account, None, "slack", "T1").await;
    connect::record_connect(
        &state_on(&owner),
        account,
        user,
        registry::lookup("slack").unwrap(),
    )
    .await
    .unwrap();
    let r = router(app_pool(&owner).await, user_context(account, user));

    let (status, body) = send(&r, "DELETE", "/v1/connectors/slack", None).await;
    assert_eq!(status, StatusCode::NO_CONTENT, "{body}");
    let left: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM oauth_tokens WHERE account_id = $1 AND provider IN ('slack', 'slack_user')",
    )
    .bind(account)
    .fetch_one(&owner)
    .await
    .unwrap();
    assert_eq!(left, 0);
    assert!(source_rows(&owner, account, user, "slack").await.is_empty());
    let (_, list) = get(&r, "/v1/connectors/slack").await;
    assert_eq!(list["status"], "not_connected", "{list}");
    assert_eq!(
        send(&r, "DELETE", "/v1/connectors/slack", None).await.0,
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn connect_refuses_a_non_connector_and_a_bad_body_before_touching_anything_pg() {
    let Some(owner) = pg_test_pool("catalog::connect_refusals").await else {
        return;
    };
    let (account, user) = seed_personal_account(&owner).await;
    let r = router(app_pool(&owner).await, user_context(account, user));

    let ok = serde_json::json!({"return_to": "wizard"});
    assert_eq!(
        send(
            &r,
            "POST",
            "/v1/connectors/bitbucket/connect",
            Some(ok.clone())
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    let (status, _) = send(
        &r,
        "POST",
        "/v1/connectors/slack/connect",
        Some(serde_json::json!({"return_to": "https://evil.example"})),
    )
    .await;
    assert!(status.is_client_error(), "{status}");
    // Slack and Microsoft are redirect sign-ins: nothing to complete.
    let (status, body) = send(
        &r,
        "POST",
        "/v1/connectors/slack/connect/complete",
        Some(serde_json::json!({"pending_id": Uuid::new_v4()})),
    )
    .await;
    assert_eq!(
        (status, body["code"].as_str()),
        (StatusCode::BAD_REQUEST, Some("nothing_to_complete"))
    );
    // A hosted toolkit refuses a malformed pending id. (One that is not the caller's
    // is a 404; the e2e isolation scenario covers it against the Composio fake.)
    let (status, _) = send(
        &r,
        "POST",
        "/v1/connectors/jira/connect/complete",
        Some(serde_json::json!({"pending_id": "not-a-uuid"})),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

// ---------------------------------------------------------------------------
// Scope and pause through the API (B3)
// ---------------------------------------------------------------------------

/// A member who connected Slack through the new flow: both tokens, the app's
/// scopes, and the catalog's rows (one per source, off, setup unfinished).
async fn new_slack_connection(
    owner: &PgPool,
    app: &PgPool,
    account: Uuid,
    user: Uuid,
    workspace: &str,
) {
    token(owner, account, Some(user), "slack_user", workspace).await;
    token(owner, account, None, "slack", workspace).await;
    sqlx::query("UPDATE oauth_tokens SET scope = $2 WHERE account_id = $1 AND provider = 'slack'")
        .bind(account)
        .bind(crate::connectors::slack::slack_scopes())
        .execute(owner)
        .await
        .unwrap();
    connect::record_connect(
        &state_on(app),
        account,
        user,
        registry::lookup("slack").unwrap(),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn pick_pause_and_resume_through_the_api_pg() {
    let Some(owner) = pg_test_pool("catalog::scope_pause").await else {
        return;
    };
    let (account, user) = seed_personal_account(&owner).await;
    let app = app_pool(&owner).await;
    new_slack_connection(&owner, &app, account, user, "T-pick").await;
    let r = router(app.clone(), user_context(account, user));
    let src = "/v1/connectors/slack/sources/channels";

    // A new connection: the source is off and nothing is chosen.
    let (_, slack) = get(&r, "/v1/connectors/slack").await;
    let ch = &slack["sources"][0];
    assert_eq!(
        (
            ch["status"].as_str(),
            ch["enabled"].as_bool(),
            ch["paused"].as_bool()
        ),
        (Some("off"), Some(false), Some(false)),
        "{ch}"
    );
    assert!(ch["scope"].is_null(), "{ch}");

    // On, then "Only what I pick" with two channels: the count is the picks.
    let (st, one) = send(&r, "PATCH", src, Some(json!({"enabled": true}))).await;
    assert_eq!(st, StatusCode::OK, "{one}");
    assert_eq!(
        (one["enabled"].as_bool(), one["status"].as_str()),
        (Some(true), Some("active")),
        "{one}"
    );
    let (st, one) = send(
        &r,
        "PUT",
        &format!("{src}/scope"),
        Some(json!({"mode": "pick", "item_ids": ["C1", "C2"]})),
    )
    .await;
    assert_eq!(st, StatusCode::OK, "{one}");
    assert_eq!(one["scope"]["mode"], "pick", "{one}");
    assert_eq!(one["scope"]["count"], 2, "{one}");
    assert_eq!(one["scope"]["noun"], "channels", "{one}");
    // Picking replaces the picks, and the capture filter reads the same rows.
    let (st, one) = send(
        &r,
        "PUT",
        &format!("{src}/scope"),
        Some(json!({"mode": "pick", "item_ids": ["C2", "C3", "C4"]})),
    )
    .await;
    assert_eq!(
        (st, one["scope"]["count"].as_u64()),
        (StatusCode::OK, Some(3)),
        "{one}"
    );
    let picked: Vec<String> = sqlx::query_scalar(
        "SELECT channel_id FROM slack_member_channels WHERE account_id = $1 AND user_id = $2 AND selected ORDER BY 1",
    )
    .bind(account)
    .bind(user)
    .fetch_all(&owner)
    .await
    .unwrap();
    assert_eq!(picked, ["C2", "C3", "C4"]);

    // "Everything" has no count; the picks are kept for the way back.
    let (_, one) = send(
        &r,
        "PUT",
        &format!("{src}/scope"),
        Some(json!({"mode": "everything"})),
    )
    .await;
    assert_eq!(one["scope"]["mode"], "everything", "{one}");
    assert!(one["scope"]["count"].is_null(), "{one}");

    // Pause: the status says so; resume: active again. The scope is untouched.
    let (st, one) = send(&r, "PATCH", src, Some(json!({"paused": true}))).await;
    assert_eq!(
        (st, one["status"].as_str(), one["paused"].as_bool()),
        (StatusCode::OK, Some("paused"), Some(true)),
        "{one}"
    );
    assert_eq!(one["scope"]["mode"], "everything");
    assert!(pause::is_paused(&owner, account, user, pause::SLACK_CHANNELS).await);
    let (_, list) = get(&r, "/v1/connectors").await;
    assert_eq!(connector(&list, "slack")["sources"][0]["status"], "paused");
    let (_, one) = send(&r, "PATCH", src, Some(json!({"paused": false}))).await;
    assert_eq!(
        (one["status"].as_str(), one["paused"].as_bool()),
        (Some("active"), Some(false)),
        "{one}"
    );
    assert!(!pause::is_paused(&owner, account, user, pause::SLACK_CHANNELS).await);

    // Off beats paused in the status; turning it on again restores it.
    send(
        &r,
        "PATCH",
        src,
        Some(json!({"paused": true, "enabled": false})),
    )
    .await;
    let (_, slack) = get(&r, "/v1/connectors/slack").await;
    assert_eq!(slack["sources"][0]["status"], "off");

    // Bad requests.
    let (st, body) = send(&r, "PATCH", src, Some(json!({}))).await;
    assert_eq!(
        (st, body["code"].as_str()),
        (StatusCode::BAD_REQUEST, Some("nothing_to_change")),
        "{body}"
    );
    assert_eq!(
        send(
            &r,
            "PATCH",
            "/v1/connectors/slack/sources/nope",
            Some(json!({"paused": true}))
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        send(
            &r,
            "PATCH",
            "/v1/connectors/bitbucket/sources/x",
            Some(json!({"paused": true}))
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        send(
            &r,
            "PUT",
            &format!("{src}/scope"),
            Some(json!({"mode": "some"}))
        )
        .await
        .0,
        StatusCode::UNPROCESSABLE_ENTITY
    );
    // Not connected: nothing to pause or pick.
    assert_eq!(
        send(
            &r,
            "PATCH",
            "/v1/connectors/jira/sources/workspace",
            Some(json!({"paused": true}))
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        get(&r, "/v1/connectors/jira/sources/workspace/scope-items")
            .await
            .0,
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn a_scope_mode_the_source_does_not_allow_is_a_422_pg() {
    let Some(owner) = pg_test_pool("catalog::scope_shapes").await else {
        return;
    };
    let (account, user) = seed_personal_account(&owner).await;
    token(&owner, account, Some(user), "microsoft", "tenant-shapes").await;
    token(
        &owner,
        account,
        Some(user),
        "google_meet",
        "meet@example.com",
    )
    .await;
    let app = app_pool(&owner).await;
    let r = router(app, user_context(account, user));
    let scope = |p: &str, s: &str| format!("/v1/connectors/{p}/sources/{s}/scope");

    for (p, s, mode, code) in [
        (
            "microsoft",
            "teams_meetings",
            "everything",
            "everything_not_allowed",
        ),
        ("google", "meet", "everything", "everything_not_allowed"),
        ("microsoft", "calendar", "pick", "pick_not_allowed"),
        ("google", "calendar", "pick", "pick_not_allowed"),
    ] {
        let (st, body) = send(&r, "PUT", &scope(p, s), Some(json!({"mode": mode}))).await;
        assert_eq!(
            st,
            StatusCode::UNPROCESSABLE_ENTITY,
            "{p}/{s} {mode}: {body}"
        );
        assert_eq!(body["code"], code, "{p}/{s}");
    }
    // A workspace-wide source has nothing to list.
    let (st, body) = get(&r, "/v1/connectors/microsoft/sources/calendar/scope-items").await;
    assert_eq!(
        (st, body["code"].as_str()),
        (StatusCode::UNPROCESSABLE_ENTITY, Some("no_scope_items")),
        "{body}"
    );
    // Too many picks.
    let ids: Vec<String> = (0..1001).map(|i| i.to_string()).collect();
    let (st, body) = send(
        &r,
        "PUT",
        &scope("microsoft", "teams_chats"),
        Some(json!({"mode": "pick", "item_ids": ids})),
    )
    .await;
    assert_eq!(
        (st, body["code"].as_str()),
        (StatusCode::UNPROCESSABLE_ENTITY, Some("too_many_items")),
        "{body}"
    );
    // The allowed combinations store.
    let (st, body) = send(
        &r,
        "PUT",
        &scope("microsoft", "calendar"),
        Some(json!({"mode": "everything"})),
    )
    .await;
    assert_eq!(st, StatusCode::OK, "{body}");
    assert_eq!(body["scope"]["mode"], "everything");
    assert!(body["scope"]["count"].is_null());
}

#[tokio::test]
async fn one_member_cannot_change_a_colleagues_scope_or_pause_pg() {
    let Some(owner) = pg_test_pool("catalog::scope_isolation").await else {
        return;
    };
    let (account, alice) = seed_personal_account(&owner).await;
    // Another account altogether, with no Slack sign-in.
    let (other_account, bob) = seed_personal_account(&owner).await;
    // A second member of ALICE's account, who shares only the workspace app.
    let carol: Uuid = sqlx::query_scalar("INSERT INTO users (email) VALUES ($1) RETURNING id")
        .bind(format!("{}@scope.test", Uuid::new_v4()))
        .fetch_one(&owner)
        .await
        .unwrap();
    let app = app_pool(&owner).await;
    new_slack_connection(&owner, &app, account, alice, "T-iso").await;
    let src = "/v1/connectors/slack/sources/channels";
    let ra = router(app.clone(), user_context(account, alice));
    let rb = router(app.clone(), user_context(other_account, bob));
    let rc = router(app.clone(), user_context(account, carol));
    send(&ra, "PATCH", src, Some(json!({"enabled": true}))).await;
    send(
        &ra,
        "PUT",
        &format!("{src}/scope"),
        Some(json!({"mode": "pick", "item_ids": ["C1"]})),
    )
    .await;

    // Bob has no Slack sign-in: every call answers what an unconnected connector
    // answers, and none of them touches Alice.
    for (m, u, b) in [
        ("GET", format!("{src}/scope-items"), json!(null)),
        ("PATCH", src.to_string(), json!({"paused": true})),
        ("PATCH", src.to_string(), json!({"enabled": false})),
        ("PUT", format!("{src}/scope"), json!({"mode": "everything"})),
        (
            "PUT",
            format!("{src}/scope"),
            json!({"mode": "pick", "item_ids": ["C9"]}),
        ),
    ] {
        let body = if b.is_null() { None } else { Some(b) };
        let (st, body) = send(&rb, m, &u, body).await;
        assert_eq!(
            (st, body["code"].as_str()),
            (StatusCode::NOT_FOUND, Some("not_connected")),
            "{m} {u}: {body}"
        );
    }
    let rows: i64 =
        sqlx::query_scalar("SELECT count(*) FROM connector_sources WHERE owner_user_id = $1")
            .bind(bob)
            .fetch_one(&owner)
            .await
            .unwrap();
    assert_eq!(rows, 0, "Bob's refused writes left no row behind");

    // Carol shares Alice's account and the workspace app (a connection from before
    // per-member sign-ins counts for every member). What she changes is her own
    // row: Alice's source, scope and picks are as Alice left them.
    let (st, mine) = send(&rc, "PATCH", src, Some(json!({"paused": true}))).await;
    assert_eq!(st, StatusCode::OK, "{mine}");
    assert!(pause::is_paused(&owner, account, carol, pause::SLACK_CHANNELS).await);
    assert!(
        !pause::is_paused(&owner, account, alice, pause::SLACK_CHANNELS).await,
        "Alice is not paused by Carol"
    );

    let (_, slack) = get(&ra, "/v1/connectors/slack").await;
    let ch = &slack["sources"][0];
    assert_eq!(
        (
            ch["enabled"].as_bool(),
            ch["paused"].as_bool(),
            ch["scope"]["mode"].as_str(),
            ch["scope"]["count"].as_u64()
        ),
        (Some(true), Some(false), Some("pick"), Some(1)),
        "{ch}"
    );
    let picked: Vec<(Uuid, String)> = sqlx::query_as(
        "SELECT user_id, channel_id FROM slack_member_channels WHERE account_id = $1 AND selected",
    )
    .bind(account)
    .fetch_all(&owner)
    .await
    .unwrap();
    assert_eq!(picked, vec![(alice, "C1".to_string())]);
    let (_, theirs) = get(&rb, "/v1/connectors/slack").await;
    assert_eq!(theirs["status"], "not_connected", "{theirs}");
}

#[tokio::test]
async fn activity_counts_filter_pagination_retractions_and_isolation_pg() {
    let Some(owner) = pg_test_pool("catalog::activity").await else {
        return;
    };
    let (account, alice) = seed_personal_account(&owner).await;
    let (other, bob) = seed_personal_account(&owner).await;
    let graph = Uuid::new_v4();
    sqlx::query("INSERT INTO graphs(id,account_id,name) VALUES($1,$2,'activity')")
        .bind(graph)
        .bind(account)
        .execute(&owner)
        .await
        .unwrap();
    for (kind, who, days, retracted) in [
        ("slack", alice, 0, false),
        ("slack", alice, 1, false),
        ("slack", alice, 8, false),
        ("gmail", alice, 0, false),
        ("outlook", alice, 0, false),
        ("microsoft_calendar", alice, 0, false),
        ("jira_future_kind", alice, 0, false),
        ("slack", bob, 0, false),
        ("slack", alice, 0, true),
    ] {
        sqlx::query("INSERT INTO atoms(atom_id,graph_id,source_type,source_id,content_blob_key,timestamp,ingested_by,created_at,evidence_retracted,title) VALUES($1,$2,$3,$4,'test',now(),$5,now()-make_interval(days=>$6),$7,'Capture')")
            .bind(Uuid::new_v4()).bind(graph).bind(kind).bind(Uuid::new_v4().to_string()).bind(who).bind(days).bind(retracted).execute(&owner).await.unwrap();
    }
    let node = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO nodes(node_id,graph_id,type,title) VALUES($1,$2,'entity','Activity topic')",
    )
    .bind(node)
    .bind(graph)
    .execute(&owner)
    .await
    .unwrap();
    sqlx::query("INSERT INTO node_atoms(node_id,atom_id) SELECT $1,atom_id FROM atoms WHERE graph_id=$2 AND source_type='slack'").bind(node).bind(graph).execute(&owner).await.unwrap();
    // Equal timestamps exercise the UUID cursor tie breaker, including the final page.
    sqlx::query("UPDATE atoms SET created_at=date_trunc('day',now()) WHERE graph_id=$1 AND created_at>=now()-interval '7 days'")
        .bind(graph).execute(&owner).await.unwrap();
    let app = app_pool(&owner).await;
    let r = router(app.clone(), user_context(account, alice));
    let (status, first) = get(&r, "/v1/connectors/slack/captures?source=channels&limit=1").await;
    assert_eq!(status, StatusCode::OK, "{first}");
    assert_eq!(first["items"].as_array().unwrap().len(), 1);
    let cursor = first["next_cursor"].as_str().unwrap();
    let (_, second) = get(
        &r,
        &format!("/v1/connectors/slack/captures?source=channels&limit=1&cursor={cursor}"),
    )
    .await;
    assert_eq!(
        first["items"][0]["captured_at"],
        second["items"][0]["captured_at"]
    );
    assert!(
        first["items"][0]["atom_id"].as_str().unwrap()
            > second["items"][0]["atom_id"].as_str().unwrap()
    );
    let second_cursor = second["next_cursor"].as_str().unwrap();
    let (status, third) = get(
        &r,
        &format!("/v1/connectors/slack/captures?source=channels&limit=1&cursor={second_cursor}"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(third["next_cursor"].is_null());
    let pages = [&first, &second, &third].map(|p| p["items"][0]["atom_id"].clone());
    assert_eq!(
        pages.iter().collect::<std::collections::HashSet<_>>().len(),
        3
    );
    let (_, all) = get(&r, "/v1/connectors/slack/captures").await;
    assert_eq!(all["items"].as_array().unwrap().len(), 3, "{all}");
    assert!(all["next_cursor"].is_null());
    let (_, summary) = get(&r, "/v1/connectors/slack").await;
    assert_eq!(summary["topics"][0]["total"], 3);
    assert_eq!(summary["topics"][0]["last_7_days"], 2);
    assert_eq!(first["items"][0]["topics"][0]["id"], node.to_string());
    assert_eq!(
        summary["sources"][0]["week"],
        serde_json::json!([0, 0, 0, 0, 0, 0, 2])
    );
    assert_eq!(
        summary["last_captured_at"],
        first["items"][0]["captured_at"]
    );
    assert_eq!(
        get(&r, "/v1/connectors/slack/captures?source=gmail")
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        get(&r, "/v1/connectors/slack/captures?cursor=bad").await.0,
        StatusCode::BAD_REQUEST
    );
    let (_, mail) = get(&r, "/v1/connectors/microsoft/captures?source=mail").await;
    assert_eq!(mail["items"].as_array().unwrap().len(), 1);
    assert_eq!(mail["items"][0]["source"], "mail");
    let (_, microsoft) = get(&r, "/v1/connectors/microsoft/captures").await;
    assert_eq!(microsoft["items"].as_array().unwrap().len(), 2);
    let (_, jira) = get(&r, "/v1/connectors/jira/captures").await;
    assert_eq!(
        jira["items"].as_array().unwrap().len(),
        1,
        "toolkit prefixes include new capture kinds"
    );
    let (_, list) = get(&r, "/v1/connectors").await;
    assert_eq!(
        connector(&list, "slack")["last_captured_at"],
        summary["last_captured_at"]
    );
    assert_eq!(
        connector(&list, "slack")["sources"][0]["week"],
        summary["sources"][0]["week"]
    );
    // Same account B is an authorized positive control for B's own capture.
    sqlx::query("INSERT INTO account_members(account_id,user_id,role) VALUES($1,$2,'member')")
        .bind(account).bind(bob).execute(&owner).await.unwrap();
    let b_router = router(app.clone(), user_context(account, bob));
    let (status, b_feed) = get(&b_router, "/v1/connectors/slack/captures").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(b_feed["items"].as_array().unwrap().len(), 1);
    assert!(!pages.contains(&b_feed["items"][0]["atom_id"]));
    let (_, b_detail) = get(&b_router, "/v1/connectors/slack").await;
    assert_eq!(b_detail["topics"][0]["total"], 1);
    assert_eq!(b_detail["topics"][0]["last_7_days"], 1);
    assert_eq!(
        b_detail["sources"][0]["week"],
        serde_json::json!([0, 0, 0, 0, 0, 0, 1])
    );
    assert_eq!(
        b_detail["last_captured_at"],
        b_feed["items"][0]["captured_at"]
    );
    let (_, foreign) = get(
        &router(app, user_context(other, bob)),
        "/v1/connectors/slack/captures",
    )
    .await;
    assert_eq!(foreign["items"], serde_json::json!([]));
}

#[tokio::test]
async fn activity_history_floor_on_owner_tenant_connection_pg() {
    use sqlx::{Connection, Executor};
    use std::str::FromStr;
    let Ok(base) = std::env::var("ORGT_TEST_BASE_URL") else {
        eprintln!("skipping activity tenant floor: ORGT_TEST_BASE_URL missing");
        return;
    };
    let opts = sqlx::postgres::PgConnectOptions::from_str(&base).unwrap();
    assert!(matches!(opts.get_host(), "localhost" | "127.0.0.1" | "::1"));
    let mut admin = sqlx::PgConnection::connect_with(&opts).await.unwrap();
    let name = crate::test_db::TestDatabase::new(&opts, "connector_activity_floor");
    admin
        .execute(format!("CREATE DATABASE {name}").as_str())
        .await
        .unwrap();
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect_with(opts.database(&name))
        .await
        .unwrap();
    sqlx::migrate!("./migrations_tenant")
        .run(&pool)
        .await
        .unwrap();
    let account = Uuid::new_v4();
    let user = Uuid::new_v4();
    sqlx::query("INSERT INTO tenant_meta(id,org_id) VALUES(true,'org_activity')")
        .execute(&pool)
        .await
        .unwrap();
    let graph: Uuid = sqlx::query_scalar(
        "INSERT INTO graphs(account_id,name,kind) VALUES($1,'Activity','team') RETURNING id",
    )
    .bind(account)
    .fetch_one(&pool)
    .await
    .unwrap();
    let team: Uuid=sqlx::query_scalar("INSERT INTO teams(account_id,graph_id,name,created_by) VALUES($1,$2,'Activity',$3) RETURNING id").bind(account).bind(graph).bind(user).fetch_one(&pool).await.unwrap();
    sqlx::query("INSERT INTO team_members(team_id,user_id,role) VALUES($1,$2,'member')")
        .bind(team)
        .bind(user)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO team_seat_grants(account_id,team_id,user_id,source,history_from) VALUES($1,$2,$3,'test',now()-interval '1 day')").bind(account).bind(team).bind(user).execute(&pool).await.unwrap();
    for days in [0, 2] {
        sqlx::query("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,timestamp,ingested_by) VALUES($1,'slack',$2,'test',now()-make_interval(days=>$3),$4)").bind(graph).bind(days.to_string()).bind(days).bind(user).execute(&pool).await.unwrap();
    }
    let hidden: Uuid = sqlx::query_scalar(
        "INSERT INTO graphs(account_id,name,kind) VALUES($1,'Hidden','team') RETURNING id",
    )
    .bind(account)
    .fetch_one(&pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,timestamp,ingested_by,created_at,evidence_retracted) VALUES($1,'slack','hidden','test',now(),$3,now()+interval '1 hour',false),($2,'slack','retracted','test',now(),$3,now()+interval '2 hours',true)")
        .bind(hidden).bind(graph).bind(user).execute(&pool).await.unwrap();
    let node = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO nodes(node_id,graph_id,type,title) VALUES($1,$2,'entity','Floor topic')",
    )
    .bind(node)
    .bind(graph)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO node_atoms(node_id,atom_id) SELECT $1,atom_id FROM atoms WHERE graph_id IN ($2,$3)")
        .bind(node).bind(graph).bind(hidden).execute(&pool).await.unwrap();
    let control = pg_test_pool("catalog::activity_floor_control").await.expect("control test DB required");
    let state = std::sync::Arc::new(crate::test_app_state_owned_with_db(control, None));
    let tenant = crate::tenancy::extractor::Tenant {
        pool: Some(pool.clone()),
        store: Some(std::sync::Arc::new(crate::store::PostgresStore::new(
            pool.clone(),
        ))),
        decision: Some(crate::tenancy::runtime::TenancyDecision::Org(
            crate::tenancy::runtime::TenantContext {
                org_id: "org_activity".into(),
                account_id: account,
                user_id: user,
                pool: pool.clone(),
                page_pool: None,
            },
        )),
        class: crate::route_class::RouteClass::Other,
    };
    // Exercise the actual HTTP handlers with the org tenancy decision attached.
    let decision = tenant.decision.clone().unwrap();
    let user = user_context(account, user);
    let r = Router::new()
        .nest(
            "/v1",
            crate::v1_routes()
                .layer(axum::Extension(user.clone()))
                .layer(axum::Extension(decision)),
        )
        .with_state(state.clone());
    let (status, page) = get(&r, "/v1/connectors/slack/captures").await;
    assert_eq!(status, StatusCode::OK, "{page}");
    assert_eq!(
        page["items"].as_array().unwrap().len(),
        1,
        "pre-floor capture excluded: {page}"
    );
    let (status, detail) = get(&r, "/v1/connectors/slack").await;
    assert_eq!(status, StatusCode::OK, "{detail}");
    assert_eq!(
        detail["sources"][0]["week"],
        serde_json::json!([0, 0, 0, 0, 0, 0, 1])
    );
    assert_eq!(detail["last_captured_at"], page["items"][0]["captured_at"]);
    assert_eq!(detail["topics"][0]["total"], 1);
    assert_eq!(detail["topics"][0]["last_7_days"], 1);
    assert_eq!(page["items"][0]["topics"][0]["total"], 1);
    let (_, list) = get(&r, "/v1/connectors").await;
    assert_eq!(
        connector(&list, "slack")["sources"][0]["week"],
        detail["sources"][0]["week"]
    );
    assert_eq!(
        connector(&list, "slack")["last_captured_at"],
        detail["last_captured_at"]
    );

    sqlx::query("DELETE FROM team_members WHERE team_id=$1 AND user_id=$2")
        .bind(team)
        .bind(user.user_id)
        .execute(&pool)
        .await
        .unwrap();
    let (_, denied) = get(&r, "/v1/connectors/slack/captures").await;
    assert_eq!(denied["items"], serde_json::json!([]));
    let (_, denied_detail) = get(&r, "/v1/connectors/slack").await;
    assert!(denied_detail["last_captured_at"].is_null());
    assert_eq!(denied_detail["topics"], serde_json::json!([]));
    assert_eq!(
        denied_detail["sources"][0]["week"],
        serde_json::json!([0, 0, 0, 0, 0, 0, 0])
    );
    drop(r);
    drop(tenant);
    drop(state);
    pool.close().await;
    admin
        .execute(format!("DROP DATABASE {name} WITH (FORCE)").as_str())
        .await
        .unwrap();
}

#[tokio::test]
async fn unsupported_setup_completion_is_durable_and_member_owned_pg() {
    let Some(owner) = pg_test_pool("catalog::unsupported_setup").await else { return; };
    let (account, user) = seed_personal_account(&owner).await;
    sqlx::query("INSERT INTO composio_connections (account_id,owner_user_id,service,connected_account_id,auth_config_id,status,provider_namespace) VALUES ($1,$2,'jira',$3,'ac_test','active','jira:site:setup')")
        .bind(account).bind(user).bind(format!("ca_{user}")).execute(&owner).await.unwrap();
    let app = app_pool(&owner).await;
    connect::record_connect(&state_on(&app), account, user, registry::lookup("jira").unwrap()).await.unwrap();
    let r = router(app.clone(), user_context(account,user));
    let root = "/v1/connectors/jira/sources/workspace";
    assert_eq!(send(&r,"PATCH",root,Some(json!({"enabled":true}))).await.0, StatusCode::OK);
    let (_,before)=get(&r,"/v1/connectors/jira").await;
    assert_eq!(before["resume_step"],"scope","{before}");
    assert!(before["sources"][0]["setup_completed_at"].is_null());
    assert_eq!(send(&r,"PUT",&format!("{root}/scope"),Some(json!({"mode":"pick","item_ids":[]}))).await.0,StatusCode::UNPROCESSABLE_ENTITY);
    assert!(!source_rows(&owner,account,user,"jira").await[0].2);
    let (status,saved)=send(&r,"PUT",&format!("{root}/scope"),Some(json!({"mode":"everything"}))).await;
    assert_eq!(status,StatusCode::OK,"{saved}");
    assert!(saved["setup_completed_at"].is_string(),"{saved}");
    assert!(saved["history_requested_at"].is_null());
    let (_,fresh)=get(&router(app.clone(),user_context(account,user)),"/v1/connectors/jira").await;
    assert_eq!(fresh["status"],"active","{fresh}");
    assert!(fresh["resume_step"].is_null());
    let (_,again)=send(&r,"PUT",&format!("{root}/scope"),Some(json!({"mode":"everything"}))).await;
    assert_eq!(again["setup_completed_at"],saved["setup_completed_at"]);
    // Repair an older client's saved scope without a completion timestamp.
    sqlx::query("UPDATE connector_sources SET setup_completed_at=NULL WHERE account_id=$1 AND owner_user_id=$2").bind(account).bind(user).execute(&owner).await.unwrap();
    let (_,legacy)=get(&r,"/v1/connectors/jira").await;
    assert_eq!(legacy["resume_step"],"scope","{legacy}");
    let (_,other)=seed_personal_account(&owner).await;
    let (status,_)=send(&router(app,user_context(account,other)),"PUT",&format!("{root}/scope"),Some(json!({"mode":"everything"}))).await;
    assert_eq!(status,StatusCode::NOT_FOUND);
    assert!(!source_rows(&owner,account,user,"jira").await[0].2);
}

#[tokio::test]
async fn mixed_setup_completes_only_after_scope_and_history_choices_pg() {
    let Some(owner) = pg_test_pool("catalog::mixed_setup").await else { return; };
    let (account,user)=seed_personal_account(&owner).await;
    token(&owner,account,Some(user),"microsoft","setup-tenant").await;
    let app=app_pool(&owner).await;
    connect::record_connect(&state_on(&app),account,user,registry::lookup("microsoft").unwrap()).await.unwrap();
    // Seed the provider-selected chat scope; transport discovery is separately tested.
    sqlx::query("UPDATE connector_sources SET enabled=true,scope_mode='pick' WHERE account_id=$1 AND owner_user_id=$2 AND source='teams_chats'").bind(account).bind(user).execute(&owner).await.unwrap();
    let r=router(app.clone(),user_context(account,user));
    let root="/v1/connectors/microsoft/sources/calendar";
    assert_eq!(send(&r,"PATCH",root,Some(json!({"enabled":true}))).await.0,StatusCode::OK);
    let (_,before)=get(&r,"/v1/connectors/microsoft").await;
    assert_eq!(before["resume_step"],"scope","{before}");
    assert_eq!(send(&r,"PUT",&format!("{root}/scope"),Some(json!({"mode":"everything"}))).await.0,StatusCode::OK);
    let (_,scoped)=get(&r,"/v1/connectors/microsoft").await;
    assert_eq!(scoped["resume_step"],"history","{scoped}");
    let (status,body)=send(&r,"POST","/v1/connectors/microsoft/sources/teams_chats/backfill",Some(json!({"days":0}))).await;
    assert_eq!(status,StatusCode::ACCEPTED,"{body}");
    let (_,fresh)=get(&router(app,user_context(account,user)),"/v1/connectors/microsoft").await;
    assert!(fresh["resume_step"].is_null(),"{fresh}");
    assert_ne!(fresh["status"],"setting_up","{fresh}");
    assert_eq!(source_rows(&owner,account,user,"microsoft").await.iter().filter(|(_,on,done)| *on && *done).count(),2);
}
