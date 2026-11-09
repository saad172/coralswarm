//! Connect-and-done Composio capture through the real scoped funnel against a
//! local mock broker: no backfill at connect, watermark polling, overlap
//! dedupe, self-check verdicts and beta, revocation, reconnect, disconnect, and
//! the migration of PR #1239 pilot scopes. The database must be task-owned.
mod support;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{delete, post},
    Extension, Json,
};
use chrono::{DateTime, Duration, Utc};
use coralswarm_backend::{
    tenancy::extractor::Tenant, test_composio as composio, test_composio_recipe as recipe,
    TestComposioClient as Client, UserContext,
};
use serde_json::{json, Value};
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    PgPool,
};
use std::{
    collections::BTreeMap,
    str::FromStr,
    sync::{Arc, Mutex},
};
use uuid::Uuid;

const ORG: &str = "0f0f0f0f-0000-4000-8000-00000000c0de";

async fn result(response: Response) -> (StatusCode, Value) {
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), 1_000_000)
        .await
        .unwrap();
    (status, serde_json::from_slice(&body).unwrap())
}
async fn seed(pool: &PgPool) -> UserContext {
    let user: Uuid = sqlx::query_scalar("INSERT INTO users(email) VALUES($1) RETURNING id")
        .bind(format!("composio-{}@test.local", Uuid::new_v4()))
        .fetch_one(pool)
        .await
        .unwrap();
    let account: Uuid =
        sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES($1) RETURNING id")
            .bind(user)
            .fetch_one(pool)
            .await
            .unwrap();
    sqlx::query("INSERT INTO account_members(account_id,user_id,role) VALUES($1,$2,'owner')")
        .bind(account)
        .bind(user)
        .execute(pool)
        .await
        .unwrap();
    UserContext {
        account_id: account,
        user_id: user,
        role: "owner".into(),
        device_id: "composio-fixture".into(),
    }
}

/// What the mock broker serves and what it saw.
#[derive(Default)]
struct Broker {
    issues: Vec<Value>,
    /// Serve every issue whatever `since` says (a provider ignoring the filter).
    ignore_since: bool,
    /// Answer tool executions as Composio does for a revoked provider grant.
    revoked: bool,
    /// Page issues forever through two cursors (c1, c2, c1, ...).
    cycle: bool,
    sinces: Vec<String>,
    served: BTreeMap<String, usize>,
    /// Every connected-account delete Composio received, failed ones too.
    deleted: Vec<String>,
    /// Answer deletes with this status instead of success.
    delete_status: Option<StatusCode>,
    /// Connected accounts by id, as `GET /connected_accounts/{id}` serves them;
    /// an absent id answers 404 (deleted at Composio).
    accounts: BTreeMap<String, Value>,
    account_reads: BTreeMap<String, usize>,
    /// Every tool execution, as its GraphQL operation (Linear) or tool slug.
    calls: Vec<String>,
    /// Change streams answer with a shape the recipe cannot parse.
    contract: bool,
    /// Linear organization id, when it is not `ORG`.
    org: Option<String>,
    /// The Jira site Composio routes tool calls to, when it is not `one`.
    jira_site: Option<&'static str>,
    /// Issues the Jira JQL search returns, whatever the window.
    jira_issues: Vec<Value>,
    /// Zendesk, Dart, Confluence and HubSpot answer as another workspace.
    moved: bool,
    /// Items the first change stream of those four toolkits returns, by toolkit.
    items: BTreeMap<&'static str, Vec<Value>>,
    /// Linear comments the comment change stream returns.
    comments: Vec<Value>,
    /// Comments every Jira issue's comment list returns.
    jira_comments: Vec<Value>,
    /// Events every Zendesk ticket's conversation log returns.
    zendesk_events: Vec<Value>,
}
type Shared = Arc<Mutex<Broker>>;
fn operation(args: &Value) -> String {
    let doc = args["query_or_mutation"].as_str().unwrap_or("").trim_start();
    let doc = doc.strip_prefix("query").unwrap_or("").trim_start();
    doc.chars().take_while(|c| c.is_ascii_alphanumeric() || *c == '_').collect()
}
fn empty(field: &str) -> Value {
    json!({"data":{field:{"nodes":[],"pageInfo":{"hasNextPage":false}}},"errors":null})
}
const NAMESPACE_CALLS: [&str; 7] = [
    "CoralSwarmWorkspace",
    "JIRA_GET_SERVER_INFO",
    "JIRA_WHO_AM_I",
    "DART_GET_CONFIG",
    "CONFLUENCE_SPACE_ANCHOR",
    "CONFLUENCE_WHO_AM_I",
    "HUBSPOT_GET_ACCOUNT_INFO",
];
/// The toolkit, and for shared tools the stream, a recipe call addresses.
fn call_name(tool: &str, args: &Value) -> String {
    match tool {
        "LINEAR_RUN_QUERY_OR_MUTATION" => operation(args),
        "CONFLUENCE_CQL_SEARCH" => match args["cql"].as_str().unwrap_or("") {
            "type = space" => "CONFLUENCE_SPACE_ANCHOR".into(),
            cql if cql.starts_with("type = comment") => "CONFLUENCE_COMMENTS".into(),
            _ => "CONFLUENCE_CONTENT".into(),
        },
        "HUBSPOT_SEARCH_CRM_OBJECTS_BY_CRITERIA" => format!("HUBSPOT_SEARCH_{}", args["objectType"].as_str().unwrap_or("").to_ascii_uppercase()),
        "JIRA_GET_SERVER_INFO" | "JIRA_WHO_AM_I" | "JIRA_SEARCH_FOR_ISSUES_USING_JQL_GET" | "JIRA_LIST_ISSUE_COMMENTS" | "ZENDESK_GET_SEARCH_EXPORT"
        | "ZENDESK_GET_TICKETS_CONVERSATION_LOG" | "DART_GET_CONFIG" | "DART_LIST_TASKS" | "CONFLUENCE_WHO_AM_I" | "HUBSPOT_GET_ACCOUNT_INFO" => tool.to_owned(),
        other => panic!("only the recipes' read tools are called: {other}"),
    }
}
async fn execute(State(broker): State<Shared>, Path(tool): Path<String>, Json(body): Json<Value>) -> Response {
    let args = &body["arguments"];
    let op = call_name(&tool, args);
    assert!(!op.is_empty() && (op.starts_with("CoralSwarm") || !tool.starts_with("LINEAR_")), "fixed recipe operations only: {op}");
    let mut b = broker.lock().unwrap();
    b.calls.push(op.clone());
    if b.revoked {
        return Json(json!({"successful":false,"data":{},"error":{"status_code":401}})).into_response();
    }
    if b.contract && !NAMESPACE_CALLS.contains(&op.as_str()) {
        return Json(json!({"successful":true,"data":{"renamed":[]}})).into_response();
    }
    let data = match op.as_str() {
        "CoralSwarmWorkspace" => json!({"data":{"organization":{"id":b.org.as_deref().unwrap_or(ORG)}},"errors":null}),
        "JIRA_GET_SERVER_INFO" => json!({"baseUrl":format!("https://{}.atlassian.net", b.jira_site.unwrap_or("one"))}),
        "JIRA_WHO_AM_I" => json!({"data":{"sites":[
            {"id":"cloud-one","url":"https://one.atlassian.net"},
            {"id":"cloud-two","url":"https://two.atlassian.net"}
        ]}}),
        "JIRA_SEARCH_FOR_ISSUES_USING_JQL_GET" => json!({"issues":b.jira_issues,"isLast":true}),
        "JIRA_LIST_ISSUE_COMMENTS" => json!({"comments":b.jira_comments,"total":b.jira_comments.len(),"startAt":0,"maxResults":100}),
        "ZENDESK_GET_SEARCH_EXPORT" => json!({"results":b.items.get("zendesk").cloned().unwrap_or_default(),"meta":{"has_more":false,"after_cursor":null}}),
        "ZENDESK_GET_TICKETS_CONVERSATION_LOG" => json!({"events":b.zendesk_events,"meta":{"has_more":false,"after_cursor":null}}),
        "DART_GET_CONFIG" => json!({"user":{"email":if b.moved { "other@example.com" } else { "morgan@example.com" }}}),
        "DART_LIST_TASKS" => {
            let results = b.items.get("dart").cloned().unwrap_or_default();
            json!({"count":results.len(),"results":results})
        }
        "CONFLUENCE_SPACE_ANCHOR" => json!({"results":[],"_links":{"base":format!("https://{}.atlassian.net/wiki", if b.moved { "two" } else { "one" })}}),
        "CONFLUENCE_WHO_AM_I" => json!({"data":{"sites":[
            {"id":"cloud-one","url":"https://one.atlassian.net"},
            {"id":"cloud-two","url":"https://two.atlassian.net"}
        ]}}),
        "CONFLUENCE_CONTENT" => {
            let results = b.items.get("confluence").cloned().unwrap_or_default();
            json!({"totalSize":results.len(),"results":results})
        }
        "CONFLUENCE_COMMENTS" => json!({"totalSize":0,"results":[]}),
        "HUBSPOT_GET_ACCOUNT_INFO" => json!({"portalId":if b.moved { 9002 } else { 9001 }}),
        "HUBSPOT_SEARCH_COMPANIES" => json!({"results":b.items.get("hubspot").cloned().unwrap_or_default()}),
        hubspot if hubspot.starts_with("HUBSPOT_SEARCH_") => json!({"results":[]}),
        "CoralSwarmIssuesChanged" => {
            let since = args["variables"]["since"].as_str().unwrap().to_owned();
            let floor = DateTime::parse_from_rfc3339(&since).unwrap();
            let nodes: Vec<Value> = b
                .issues
                .iter()
                .filter(|i| b.ignore_since || DateTime::parse_from_rfc3339(i["updatedAt"].as_str().unwrap()).unwrap() >= floor)
                .cloned()
                .collect();
            for n in &nodes {
                *b.served.entry(n["id"].as_str().unwrap().to_owned()).or_default() += 1;
            }
            b.sinces.push(since);
            if b.cycle {
                let next = if args["variables"]["after"] == "c1" { "c2" } else { "c1" };
                return Json(json!({"successful":true,"data":{"data":{"issues":{"nodes":nodes,"pageInfo":{"hasNextPage":true,"endCursor":next}}},"errors":null}})).into_response();
            }
            json!({"data":{"issues":{"nodes":nodes,"pageInfo":{"hasNextPage":false,"endCursor":null}}},"errors":null})
        }
        "CoralSwarmCommentsChanged" => json!({"data":{"comments":{"nodes":b.comments,"pageInfo":{"hasNextPage":false,"endCursor":null}}},"errors":null}),
        "CoralSwarmProjectsChanged" => empty("projects"),
        other => panic!("unexpected operation {other}"),
    };
    Json(json!({"successful":true,"data":data})).into_response()
}
async fn delete_account(
    State(broker): State<Shared>,
    Path(id): Path<String>,
    Query(q): Query<BTreeMap<String, String>>,
) -> Response {
    assert_eq!(q.get("revoke_on_delete").map(String::as_str), Some("true"));
    let mut b = broker.lock().unwrap();
    b.deleted.push(id);
    match b.delete_status {
        Some(status) => status.into_response(),
        None => Json(json!({"success":true})).into_response(),
    }
}
async fn get_account(State(broker): State<Shared>, Path(id): Path<String>) -> Response {
    let mut b = broker.lock().unwrap();
    *b.account_reads.entry(id.clone()).or_default() += 1;
    match b.accounts.get(&id) {
        Some(account) if b.moved && account["toolkit"]["slug"] == "zendesk" => {
            let mut account = account.clone();
            account["params"]["base_url"] = json!("https://other.zendesk.com");
            Json(account).into_response()
        }
        Some(account) => Json(account.clone()).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}
async fn mock_broker(broker: Shared) -> Client {
    let app = axum::Router::new()
        .route("/tools/execute/{tool}", post(execute))
        .route("/connected_accounts/{id}", delete(delete_account).get(get_account))
        .with_state(broker);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    Client::for_mock_broker(&base).unwrap()
}
fn issue(id: &str, title: &str, updated: DateTime<Utc>) -> Value {
    json!({"id":id,"identifier":format!("COR-{id}"),"title":title,"description":"Decision: ship it.","createdAt":updated.to_rfc3339(),"updatedAt":updated.to_rfc3339(),"state":{"name":"Todo"},"creator":{"id":"u1","name":"Morgan"},"project":{"id":"6f1d1c2a-3b4c-4d5e-8f60-718293a4b5c6","name":"Search"}})
}
fn task_owned(dsn: &str, database: &str) -> PgConnectOptions {
    let opts = PgConnectOptions::from_str(dsn).unwrap();
    assert!(matches!(opts.get_host(), "127.0.0.1" | "localhost" | "::1"));
    opts.database(database)
}
async fn connection(owner: &PgPool, pool: &PgPool, user: &UserContext, account: &str) -> Uuid {
    let id: Uuid = sqlx::query_scalar("INSERT INTO composio_connections(account_id,owner_user_id,service,connected_account_id,auth_config_id) VALUES($1,$2,'linear',$3,'fixture-read-only') RETURNING id")
        .bind(user.account_id).bind(user.user_id).bind(account).fetch_one(owner).await.unwrap();
    let mut tx = coralswarm_backend::tenancy::scope::begin_account_scoped(pool, user.account_id)
        .await
        .unwrap();
    assert!(composio::bind_connection_namespace_tx(&mut tx, user, id, &format!("linear:workspace:{ORG}"))
        .await
        .unwrap());
    tx.commit().await.unwrap();
    id
}
async fn count(owner: &PgPool, sql: &str, id: Uuid) -> i64 {
    sqlx::query_scalar(sql).bind(id).fetch_one(owner).await.unwrap()
}

#[tokio::test]
async fn connect_and_done_capture_polls_changes_only_and_keeps_atoms() {
    let Ok(dsn) = std::env::var("COMPOSIO_TEST_DATABASE_URL") else {
        assert!(
            std::env::var_os("REQUIRE_PG_SUITE").is_none(),
            "COMPOSIO_TEST_DATABASE_URL required"
        );
        eprintln!("composio PG fixture skipped: missing task-owned database");
        return;
    };
    let opts = task_owned(&dsn, "composio_capture_test");
    let owner = PgPoolOptions::new().max_connections(4).connect_with(opts.clone()).await.unwrap();
    coralswarm_backend::CONTROL_MIGRATOR.run(&owner).await.unwrap();
    support::app_role::set_app_role_password_with_retry(&owner).await;
    let pool = PgPoolOptions::new()
        .max_connections(6)
        .connect_with(opts.username("coralswarm_app").password(support::app_role::APP_ROLE_TEST_PASSWORD))
        .await
        .unwrap();
    let role: (bool, bool) =
        sqlx::query_as("SELECT rolsuper,rolbypassrls FROM pg_roles WHERE rolname=current_user")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(role, (false, false));
    let alice = seed(&owner).await;
    let bob = seed(&owner).await;
    let member_id: Uuid = sqlx::query_scalar("INSERT INTO users(email) VALUES($1) RETURNING id")
        .bind(format!("composio-member-{}@test.local", Uuid::new_v4()))
        .fetch_one(&owner)
        .await
        .unwrap();
    sqlx::query("INSERT INTO account_members(account_id,user_id,role) VALUES($1,$2,'member')")
        .bind(alice.account_id)
        .bind(member_id)
        .execute(&owner)
        .await
        .unwrap();
    let member = UserContext { user_id: member_id, role: "member".into(), ..alice.clone() };

    // Two verifications of one unbound connection: the second cannot replace
    // the first workspace, and another member cannot bind it at all.
    let raced: Uuid = sqlx::query_scalar("INSERT INTO composio_connections(account_id,owner_user_id,service,connected_account_id,auth_config_id) VALUES($1,$2,'jira',$3,'fixture-read-only') RETURNING id")
        .bind(alice.account_id).bind(alice.user_id).bind(format!("ca_{}", Uuid::new_v4().simple())).fetch_one(&owner).await.unwrap();
    let mut first = coralswarm_backend::tenancy::scope::begin_account_scoped(&pool, alice.account_id).await.unwrap();
    assert!(composio::bind_connection_namespace_tx(&mut first, &alice, raced, "jira:site:first").await.unwrap());
    let (second_pool, second_user) = (pool.clone(), alice.clone());
    let concurrent = tokio::spawn(async move {
        let mut second = coralswarm_backend::tenancy::scope::begin_account_scoped(&second_pool, second_user.account_id).await.unwrap();
        let bound = composio::bind_connection_namespace_tx(&mut second, &second_user, raced, "jira:site:other").await.unwrap();
        second.commit().await.unwrap();
        bound
    });
    first.commit().await.unwrap();
    assert!(!tokio::time::timeout(std::time::Duration::from_secs(5), concurrent).await.unwrap().unwrap());
    let mut private = coralswarm_backend::tenancy::scope::begin_account_scoped(&pool, alice.account_id).await.unwrap();
    assert!(!composio::bind_connection_namespace_tx(&mut private, &member, raced, "jira:site:first").await.unwrap());
    private.commit().await.unwrap();

    let state = Arc::new(coralswarm_backend::test_app_state_owned_with_db_and_store(pool.clone(), None));
    for user in [&alice, &bob] {
        state.dek_cache.insert(&coralswarm_backend::tenancy::TenantId::Consumer, user.account_id, [42; 32]);
    }
    let tenant = Tenant::legacy(&state);
    let linear = recipe::get("linear").unwrap();
    let broker: Shared = Arc::default();
    let client = mock_broker(broker.clone()).await;
    let connected_at = Utc::now();
    // Existing history: the self-check reads it, capture must never ingest it.
    broker.lock().unwrap().issues = vec![issue("old-1", "Old decision", connected_at - Duration::days(10))];

    // Connect: self-check passes on real-shaped data, capture starts at now.
    let conn = connection(&owner, &pool, &alice, "ca_fixture1").await;
    let (status, body) = result(composio::activate(&state, &tenant, &alice, linear, conn, &client).await).await;
    assert_eq!(status, StatusCode::OK, "activate {body}");
    assert_eq!(body["self_check"], "passed");
    assert_eq!(body["status"], "active");
    let capture = Uuid::parse_str(body["capture_id"].as_str().unwrap()).unwrap();
    let graph = Uuid::parse_str(body["graph_id"].as_str().unwrap()).unwrap();
    let graph_meta: (bool, Option<Uuid>) = sqlx::query_as("SELECT is_primary,owner_user_id FROM graphs WHERE id=$1")
        .bind(graph).fetch_one(&owner).await.unwrap();
    assert!(graph_meta.0, "captures land in the member's primary ocean");
    let items_sql = "SELECT count(*) FROM source_items WHERE graph_id=$1";
    let versions_sql = "SELECT count(*) FROM source_versions WHERE graph_id=$1";
    assert_eq!(count(&owner, items_sql, graph).await, 0, "connect never backfills history");
    let watermark: DateTime<Utc> = sqlx::query_scalar("SELECT watermark FROM composio_capture_scopes WHERE id=$1").bind(capture).fetch_one(&owner).await.unwrap();
    assert!((watermark - connected_at).abs() < Duration::seconds(30));
    let verified: Vec<String> = sqlx::query_scalar("SELECT * FROM composio_verified_toolkits()").fetch_all(&pool).await.unwrap();
    assert_eq!(verified, ["linear"], "the first passing real connection takes Linear out of beta");
    let (status, body) = result(composio::activate(&state, &tenant, &alice, linear, conn, &client).await).await;
    assert_eq!((status, body["capture_id"].as_str()), (StatusCode::OK, Some(capture.to_string().as_str())), "re-verify is idempotent");

    // A change after connect is captured on the next poll.
    let changed_at = Utc::now();
    broker.lock().unwrap().issues.push(issue("new-1", "Ship search", changed_at));
    let (status, body) = result(composio::poll(&state, &tenant, &alice, linear, capture, &client).await).await;
    assert_eq!(status, StatusCode::OK, "poll {body}");
    assert_eq!(body["pass_complete"], true);
    let ids: Vec<String> = sqlx::query_scalar("SELECT source_id FROM source_items WHERE graph_id=$1").bind(graph).fetch_all(&owner).await.unwrap();
    assert_eq!(ids, [format!("linear:workspace:{ORG}/issue/new-1")]);
    assert_eq!(count(&owner, versions_sql, graph).await, 1);
    let (next, last_change, moved): (DateTime<Utc>, Option<DateTime<Utc>>, DateTime<Utc>) =
        sqlx::query_as("SELECT next_poll_at,last_change_at,watermark FROM composio_capture_scopes WHERE id=$1").bind(capture).fetch_one(&owner).await.unwrap();
    assert!(last_change.is_some() && moved > watermark, "a finished pass advances the watermark");
    let wait = next - Utc::now();
    assert!(wait > Duration::minutes(4) && wait <= Duration::minutes(5), "recent change polls every 5 minutes: {wait}");

    // The next poll re-reads the overlap; content-hash dedupe adds nothing.
    let (status, body) = result(composio::poll(&state, &tenant, &alice, linear, capture, &client).await).await;
    assert_eq!(status, StatusCode::OK, "overlap poll {body}");
    assert_eq!(broker.lock().unwrap().served.get("new-1"), Some(&2), "the overlap really re-read the item");
    assert_eq!(count(&owner, versions_sql, graph).await, 1, "overlap never duplicates");
    // An edit versions the same identity.
    broker.lock().unwrap().issues[1] = issue("new-1", "Ship search behind a flag", Utc::now());
    let (status, _) = result(composio::poll(&state, &tenant, &alice, linear, capture, &client).await).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!((count(&owner, items_sql, graph).await, count(&owner, versions_sql, graph).await), (1, 2));
    assert_eq!(broker.lock().unwrap().served.get("old-1"), Some(&1), "pre-connect history only ever reached the probe");

    // Due discovery honours next_poll_at; other accounts and members cannot poll it.
    let due_sql = "SELECT count(*) FROM resolve_composio_capture_due() WHERE id=$1";
    assert_eq!(count(&pool, due_sql, capture).await, 0);
    sqlx::query("UPDATE composio_capture_scopes SET next_poll_at=now()-interval '1 second' WHERE id=$1").bind(capture).execute(&owner).await.unwrap();
    assert_eq!(count(&pool, due_sql, capture).await, 1);
    for other in [&bob, &member] {
        let (status, _) = result(composio::poll(&state, &tenant, other, linear, capture, &client).await).await;
        assert_eq!(status, StatusCode::CONFLICT, "another account or member never polls this capture");
    }

    // A provider cycling through continuation tokens fails the poll as a
    // contract breach instead of paging to the per-poll cap every poll.
    let issue_pages = {
        let mut b = broker.lock().unwrap();
        b.cycle = true;
        b.sinces.len()
    };
    let (status, body) = result(composio::poll(&state, &tenant, &alice, linear, capture, &client).await).await;
    assert_eq!((status, body["error_code"].as_str()), (StatusCode::SERVICE_UNAVAILABLE, Some("contract")), "{body}");
    let mut b = broker.lock().unwrap();
    assert_eq!(b.sinces.len() - issue_pages, 3, "stops at the first revisited cursor");
    b.cycle = false;
    drop(b);
    assert_eq!(count(&owner, versions_sql, graph).await, 2, "re-read pages add nothing");

    // Provider-side revocation: reconnect needed, atoms untouched and visible.
    broker.lock().unwrap().revoked = true;
    let (status, body) = result(composio::poll(&state, &tenant, &alice, linear, capture, &client).await).await;
    assert_eq!((status, body["state"].as_str()), (StatusCode::UNPROCESSABLE_ENTITY, Some("reconnect_needed")), "{body}");
    let conn_status: String = sqlx::query_scalar("SELECT status FROM composio_connections WHERE id=$1").bind(conn).fetch_one(&owner).await.unwrap();
    assert_eq!(conn_status, "reconnect_needed");
    assert_eq!(count(&owner, "SELECT count(*) FROM source_items WHERE graph_id=$1 AND access_state='permitted'", graph).await, 1);
    assert_eq!(count(&pool, due_sql, capture).await, 0, "a revoked capture is not polled");
    broker.lock().unwrap().revoked = false;

    // Reconnect: the new connection's capture inherits the old watermark.
    let conn2 = connection(&owner, &pool, &alice, "ca_fixture2").await;
    let (status, body) = result(composio::activate(&state, &tenant, &alice, linear, conn2, &client).await).await;
    assert_eq!(status, StatusCode::OK, "reconnect {body}");
    let capture2 = Uuid::parse_str(body["capture_id"].as_str().unwrap()).unwrap();
    let (inherited, old_state): (DateTime<Utc>, String) = sqlx::query_as("SELECT n.watermark,o.state FROM composio_capture_scopes n, composio_capture_scopes o WHERE n.id=$1 AND o.id=$2")
        .bind(capture2).bind(capture).fetch_one(&owner).await.unwrap();
    let old_watermark: DateTime<Utc> = sqlx::query_scalar("SELECT watermark FROM composio_capture_scopes WHERE id=$1").bind(capture).fetch_one(&owner).await.unwrap();
    assert_eq!((inherited, old_state.as_str()), (old_watermark, "disconnected"));

    // Disconnect: revoked at Composio, polling stops, atoms stay.
    let (status, body) = result(composio::disconnect_with(&state, &tenant, &alice, linear, conn2, &client).await).await;
    assert_eq!((status, body["revoked"].as_bool()), (StatusCode::OK, Some(true)), "{body}");
    assert_eq!(broker.lock().unwrap().deleted, ["ca_fixture2"]);
    let (conn_status, revoked): (String, Option<DateTime<Utc>>) = sqlx::query_as("SELECT status,revoked_at FROM composio_connections WHERE id=$1").bind(conn2).fetch_one(&owner).await.unwrap();
    assert_eq!(conn_status, "disconnected");
    assert!(revoked.is_some());
    sqlx::query("UPDATE composio_capture_scopes SET next_poll_at=now()-interval '1 second' WHERE id=$1").bind(capture2).execute(&owner).await.unwrap();
    assert_eq!(count(&pool, due_sql, capture2).await, 0);
    let (status, _) = result(composio::poll(&state, &tenant, &alice, linear, capture2, &client).await).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!((count(&owner, items_sql, graph).await, count(&owner, versions_sql, graph).await), (1, 2), "disconnect keeps every atom");
    let (status, body) = result(composio::disconnect_with(&state, &tenant, &alice, linear, conn2, &client).await).await;
    assert_eq!((status, body["revoked"].as_bool()), (StatusCode::OK, Some(true)), "disconnect is idempotent");
    assert_eq!(broker.lock().unwrap().deleted.len(), 1, "an already revoked account is not deleted twice");
    let (status, _) = result(composio::activate(&state, &tenant, &alice, linear, conn2, &client).await).await;
    assert_eq!(status, StatusCode::CONFLICT, "a disconnected connection does not restart");

    // A provider that ignores the change filter fails the self-check: the
    // connection pauses, nothing is ingested, and the toolkit's beta state
    // comes only from passing connections.
    {
        let mut b = broker.lock().unwrap();
        b.ignore_since = true;
        b.issues = vec![issue("ancient", "Ancient", Utc::now() - Duration::days(400))];
    }
    let bob_conn = connection(&owner, &pool, &bob, "ca_fixture3").await;
    let (status, body) = result(composio::activate(&state, &tenant, &bob, linear, bob_conn, &client).await).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!((body["status"].as_str(), body["self_check"].as_str()), (Some("needs_attention"), Some("failed")));
    assert!(body["message"].as_str().unwrap().contains("isn't working with CoralSwarm yet"));
    let bob_graph = Uuid::parse_str(body["graph_id"].as_str().unwrap()).unwrap();
    let bob_capture = Uuid::parse_str(body["capture_id"].as_str().unwrap()).unwrap();
    let reason: Option<String> = sqlx::query_scalar("SELECT self_check_reason FROM composio_connections WHERE id=$1").bind(bob_conn).fetch_one(&owner).await.unwrap();
    assert_eq!(reason.as_deref(), Some("change filter not honored"));
    assert_eq!(count(&owner, items_sql, bob_graph).await, 0);
    sqlx::query("UPDATE composio_capture_scopes SET next_poll_at=now()-interval '1 second' WHERE id=$1").bind(bob_capture).execute(&owner).await.unwrap();
    assert_eq!(count(&pool, due_sql, bob_capture).await, 0, "a failed self-check pauses polling");

    // Status: each member sees only their own connections; Linear is out of
    // beta and Jira (never passed on a real connection) is not.
    let (status, body) = result(composio::status(State(state.clone()), tenant.clone(), Extension(alice.clone()), Path("linear".into())).await).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["beta"], false);
    let conns = body["connections"].as_array().unwrap();
    assert_eq!(conns.len(), 2);
    assert_eq!(conns[0]["status"], "reconnect_needed");
    assert_eq!(conns[1]["status"], "disconnected");
    assert_eq!(conns[1]["capture"]["state"], "disconnected");
    let (_, body) = result(composio::status(State(state.clone()), tenant.clone(), Extension(member.clone()), Path("linear".into())).await).await;
    assert_eq!(body["connections"], json!([]), "same-account connection metadata remains private");
    let (_, body) = result(composio::status(State(state.clone()), tenant.clone(), Extension(alice.clone()), Path("jira".into())).await).await;
    assert_eq!(body["beta"], true);
    eprintln!(
        "composio PG: alice items={} versions={} captures={}",
        count(&owner, items_sql, graph).await,
        count(&owner, versions_sql, graph).await,
        count(&owner, "SELECT count(*) FROM composio_capture_scopes WHERE account_id=$1", alice.account_id).await
    );
    for account_id in [alice.account_id, bob.account_id] {
        let mut financial = coralswarm_backend::tenancy::scope::begin_account_scoped(&owner, account_id).await.unwrap();
        coralswarm_backend::billing::renewal::lock_period_rotation(&mut financial, account_id).await.unwrap();
        sqlx::query("DELETE FROM accounts WHERE id=ANY($1)")
            .bind([account_id].as_slice())
            .execute(&mut *financial)
            .await
            .unwrap();
        financial.commit().await.unwrap();
    }
}

/// Applies a tree up to (excluding) `before`, then the rest.
fn up_to(m: &sqlx::migrate::Migrator, before: i64) -> sqlx::migrate::Migrator {
    sqlx::migrate::Migrator {
        migrations: std::borrow::Cow::Owned(m.migrations.iter().filter(|x| x.version < before).cloned().collect()),
        ignore_missing: m.ignore_missing,
        locking: m.locking,
        no_tx: m.no_tx,
    }
}
async fn fresh_database(dsn: &str, name: &str) -> (coralswarm_backend::test_db::TestDatabase, PgPool) {
    let database = coralswarm_backend::test_db::TestDatabase::adopt(&task_owned(dsn, "composio_capture_test"), name);
    let admin = PgPoolOptions::new().max_connections(1).connect_with(task_owned(dsn, "composio_capture_test")).await.unwrap();
    sqlx::query(&format!("DROP DATABASE IF EXISTS {name}")).execute(&admin).await.unwrap();
    sqlx::query(&format!("CREATE DATABASE {name}")).execute(&admin).await.unwrap();
    let pool = PgPoolOptions::new().max_connections(2).connect_with(task_owned(dsn, name)).await.unwrap();
    (database, pool)
}
async fn drop_database(dsn: &str, name: &str, pool: PgPool) {
    pool.close().await;
    let admin = PgPoolOptions::new().max_connections(1).connect_with(task_owned(dsn, "composio_capture_test")).await.unwrap();
    sqlx::query(&format!("DROP DATABASE IF EXISTS {name} WITH (FORCE)")).execute(&admin).await.unwrap();
}

/// PR #1239 pilot scopes keep their atoms and become one workspace-wide
/// capture per connection, watermarked at migration time. Both trees.
#[tokio::test]
async fn pilot_project_scopes_migrate_to_workspace_captures() {
    let Ok(dsn) = std::env::var("COMPOSIO_TEST_DATABASE_URL") else {
        assert!(std::env::var_os("REQUIRE_PG_SUITE").is_none(), "COMPOSIO_TEST_DATABASE_URL required");
        return;
    };
    for (tree, migrator, head) in [
        ("control", &coralswarm_backend::CONTROL_MIGRATOR, 311),
        ("tenant", &coralswarm_backend::tenancy::migrate::TENANT_MIGRATOR, 146),
    ] {
        let name = format!("composio_migration_{tree}");
        let (_database, db) = fresh_database(&dsn, &name).await;
        up_to(migrator, head).run(&db).await.unwrap();
        let user: Uuid = Uuid::new_v4();
        let account: Uuid = Uuid::new_v4();
        let graph = Uuid::new_v4();
        let connection = Uuid::new_v4();
        let other_connection = Uuid::new_v4();
        if tree == "control" {
            sqlx::query("INSERT INTO users(id,email) VALUES($1,$2)").bind(user).bind(format!("{user}@test.local")).execute(&db).await.unwrap();
            sqlx::query("INSERT INTO accounts(id,owner_user_id) VALUES($1,$2)").bind(account).bind(user).execute(&db).await.unwrap();
            for (id, status) in [(connection, "active"), (other_connection, "reconnect")] {
                sqlx::query("INSERT INTO composio_connections(id,account_id,owner_user_id,service,connected_account_id,auth_config_id,status,provider_namespace) VALUES($1,$2,$3,'linear',$4,'cfg',$5,'linear:workspace:w')")
                    .bind(id).bind(account).bind(user).bind(format!("ca_{}", id.simple())).bind(status).execute(&db).await.unwrap();
            }
        }
        sqlx::query("INSERT INTO graphs(id,account_id,name,kind,owner_user_id,is_primary,created_at) VALUES($1,$2,'primary','member',$3,true,now())")
            .bind(graph).bind(account).bind(user).execute(&db).await.unwrap();
        let demo_graph = Uuid::new_v4();
        sqlx::query("INSERT INTO graphs(id,account_id,name,kind,owner_user_id,is_primary,created_at) VALUES($1,$2,'demo','member',$3,false,now())")
            .bind(demo_graph).bind(account).bind(user).execute(&db).await.unwrap();
        let pilot = "INSERT INTO composio_capture_scopes(graph_id,account_id,owner_user_id,connection_id,service,mode,provider_namespace,project_id,project_name,cursor,state,captured_items,created_at) VALUES($1,$2,$3,$4,'linear',$5,$6,$7,'p',$8,$9,$10,$11)";
        for (g, conn, mode, ns, project, cursor, st, n, created) in [
            (graph, Some(connection), "composio", "linear:workspace:w", "proj-a", Some("c1"), "caught_up", 7i64, "2026-10-12T00:00:00Z"),
            (graph, Some(connection), "composio", "linear:workspace:w", "proj-b", None, "running", 5, "2026-10-12T01:00:00Z"),
            (graph, Some(other_connection), "composio", "linear:workspace:w", "proj-c", None, "reconnect", 2, "2026-10-12T02:00:00Z"),
            (demo_graph, None, "synthetic", "demo:linear:workspace:x", "demo-project", None, "caught_up", 3, "2026-10-12T03:00:00Z"),
        ] {
            sqlx::query(pilot).bind(g).bind(account).bind(user).bind(conn).bind(mode).bind(ns).bind(project).bind(cursor).bind(st).bind(n)
                .bind(DateTime::parse_from_rfc3339(created).unwrap().with_timezone(&Utc)).execute(&db).await.unwrap();
        }
        let before = Utc::now();
        migrator.run(&db).await.unwrap();
        let rows: Vec<(Uuid, String, i64, Option<String>, DateTime<Utc>, DateTime<Utc>)> = sqlx::query_as(
            "SELECT connection_id,state,captured_items,cursor,watermark,created_at FROM composio_capture_scopes ORDER BY created_at",
        )
        .fetch_all(&db)
        .await
        .unwrap();
        assert_eq!(rows.len(), 2, "{tree}: one capture per connection; synthetic demo scope retired");
        assert_eq!((rows[0].0, rows[0].1.as_str(), rows[0].2, rows[0].3.as_deref()), (connection, "active", 12, None), "{tree}");
        assert_eq!(rows[0].5, DateTime::parse_from_rfc3339("2026-10-12T00:00:00Z").unwrap().with_timezone(&Utc), "{tree}: oldest scope kept");
        assert_eq!((rows[1].0, rows[1].1.as_str()), (other_connection, "reconnect_needed"), "{tree}");
        assert!(rows.iter().all(|r| r.4 >= before - Duration::seconds(5)), "{tree}: watermark = migration time, no backfill");
        let due: i64 = sqlx::query_scalar("SELECT count(*) FROM resolve_composio_capture_due()").fetch_one(&db).await.unwrap();
        assert_eq!(due, 1, "{tree}: the active pilot capture is due immediately");
        let fresh: String = sqlx::query_scalar("INSERT INTO composio_capture_scopes(graph_id,account_id,owner_user_id,connection_id,service,provider_namespace) VALUES($1,$2,$3,gen_random_uuid(),'jira','jira:site:s') RETURNING state")
            .bind(graph).bind(account).bind(user).fetch_one(&db).await.unwrap();
        assert_eq!(fresh, "active", "{tree}: column defaults satisfy the new state check");
        if tree == "control" {
            let statuses: Vec<String> = sqlx::query_scalar("SELECT status FROM composio_connections ORDER BY status").fetch_all(&db).await.unwrap();
            assert_eq!(statuses, ["active", "reconnect_needed"]);
            let verified: i64 = sqlx::query_scalar("SELECT count(*) FROM composio_verified_toolkits()").fetch_one(&db).await.unwrap();
            assert_eq!(verified, 0, "pilot connections never ran the self-check; Linear stays beta");
        }
        eprintln!("composio PG migration {tree}: {} captures after migrating 4 pilot scopes", rows.len());
        drop_database(&dsn, &name, db).await;
    }
}

/// A member who finishes Composio sign-in after the web page stopped watching
/// still gets capture: the worker sweep runs the verify logic for pending
/// connections. Expired, failed or deleted sign-ins, and any still unfinished
/// after an hour, stop waiting as `reconnect_needed`.
#[tokio::test]
async fn worker_sweep_finishes_pending_connections() {
    let Ok(dsn) = std::env::var("COMPOSIO_TEST_DATABASE_URL") else {
        assert!(std::env::var_os("REQUIRE_PG_SUITE").is_none(), "COMPOSIO_TEST_DATABASE_URL required");
        return;
    };
    let name = "composio_pending_sweep";
    let (_database, owner) = fresh_database(&dsn, name).await;
    coralswarm_backend::CONTROL_MIGRATOR.run(&owner).await.unwrap();
    support::app_role::set_app_role_password_with_retry(&owner).await;
    let pool = PgPoolOptions::new()
        .max_connections(6)
        .connect_with(task_owned(&dsn, name).username("coralswarm_app").password(support::app_role::APP_ROLE_TEST_PASSWORD))
        .await
        .unwrap();
    let alice = seed(&owner).await;
    let state = Arc::new(coralswarm_backend::test_app_state_owned_with_db_and_store(pool.clone(), None));
    state.dek_cache.insert(&coralswarm_backend::tenancy::TenantId::Consumer, alice.account_id, [42; 32]);
    let worker = coralswarm_backend::workers::tenant_harness::WorkerTenant {
        tenant: coralswarm_backend::tenancy::TenantId::Consumer,
        store: Arc::new(coralswarm_backend::store::PostgresStore::new(pool.clone())),
        pool: pool.clone(),
    };
    let broker: Shared = Arc::default();
    let client = mock_broker(broker.clone()).await;
    let owner_key = format!("coralswarm:{}:{}", alice.account_id, alice.user_id);
    let account = |status: &str| json!({"status":status,"user_id":owner_key,"toolkit":{"slug":"linear"},"auth_config":{"id":"fixture-read-only"},"requested_scopes":["read"]});
    {
        let mut b = broker.lock().unwrap();
        b.issues = vec![issue("old-1", "Old decision", Utc::now() - Duration::days(10))];
        for (id, status) in [("ca_active", "ACTIVE"), ("ca_expired", "EXPIRED"), ("ca_failed", "FAILED"), ("ca_initiated", "INITIATED"), ("ca_stale", "INITIATED")] {
            b.accounts.insert(id.into(), account(status));
        }
    }
    // Connect created each row; the page that would have verified them is gone.
    let mut ids = BTreeMap::new();
    for (remote, age) in [("ca_active", "2 minutes"), ("ca_expired", "2 minutes"), ("ca_failed", "2 minutes"), ("ca_deleted", "2 minutes"), ("ca_initiated", "2 minutes"), ("ca_stale", "2 hours")] {
        let id: Uuid = sqlx::query_scalar(&format!("INSERT INTO composio_connections(account_id,owner_user_id,service,connected_account_id,auth_config_id,created_at,updated_at) VALUES($1,$2,'linear',$3,'fixture-read-only',now()-interval '{age}',now()-interval '{age}') RETURNING id"))
            .bind(alice.account_id).bind(alice.user_id).bind(remote).fetch_one(&owner).await.unwrap();
        ids.insert(remote, id);
    }
    let pending_sql = "SELECT count(*) FROM resolve_composio_pending_connections($1,10)";
    let consumer: i64 = sqlx::query_scalar(pending_sql).bind(None::<String>).fetch_one(&pool).await.unwrap();
    assert_eq!(consumer, 6, "the app role discovers every consumer-plane pending connection");
    let other_org: i64 = sqlx::query_scalar(pending_sql).bind(Some("org_someone_else")).fetch_one(&pool).await.unwrap();
    assert_eq!(other_org, 0, "an org tenant never sees consumer-plane connections");

    let connected_at = Utc::now();
    let mut activated = 0;
    // PENDING_PER_TICK bounds each pass; two passes cover all six rows.
    for _ in 0..2 {
        activated += coralswarm_backend::workers::composio_capture::sweep_pending(&worker, &pool, &state, &client).await.unwrap();
    }
    assert_eq!(activated, 1);
    let row = |remote: &'static str| {
        let (owner, id) = (owner.clone(), ids[remote]);
        async move {
            sqlx::query_as::<_, (String, Option<String>, String, DateTime<Utc>)>("SELECT status,provider_namespace,self_check,updated_at FROM composio_connections WHERE id=$1")
                .bind(id).fetch_one(&owner).await.unwrap()
        }
    };

    // ACTIVE at Composio: bound, self-checked, capturing from now.
    let (status, namespace, self_check, _) = row("ca_active").await;
    let expected_namespace = format!("linear:workspace:{ORG}");
    assert_eq!((status.as_str(), namespace.as_deref(), self_check.as_str()), ("active", Some(expected_namespace.as_str()), "passed"));
    let (state_name, watermark, graph): (String, DateTime<Utc>, Uuid) = sqlx::query_as("SELECT state,watermark,graph_id FROM composio_capture_scopes WHERE connection_id=$1")
        .bind(ids["ca_active"]).fetch_one(&owner).await.unwrap();
    assert_eq!(state_name, "active");
    assert!((watermark - connected_at).abs() < Duration::seconds(30), "capture starts at the moment it completed");
    assert_eq!(count(&owner, "SELECT count(*) FROM source_items WHERE graph_id=$1", graph).await, 0, "no backfill");

    // Expired, failed and deleted sign-ins stop waiting.
    for remote in ["ca_expired", "ca_failed", "ca_deleted"] {
        let (status, namespace, ..) = row(remote).await;
        assert_eq!((status.as_str(), namespace), ("reconnect_needed", None), "{remote}");
        assert_eq!(count(&owner, "SELECT count(*) FROM composio_capture_scopes WHERE connection_id=$1", ids[remote]).await, 0, "{remote}");
    }
    // Still signing in: stays pending, checked again later.
    let (status, _, _, checked_at) = row("ca_initiated").await;
    assert_eq!(status, "pending");
    assert!(Utc::now() - checked_at < Duration::seconds(30), "a check moves it to the back of the queue");
    // Unfinished after an hour: abandoned, never checked again.
    assert_eq!(row("ca_stale").await.0, "reconnect_needed");

    // Rate bound: a just-checked connection is not checked again this minute.
    let reads = broker.lock().unwrap().account_reads.clone();
    assert_eq!(reads.get("ca_initiated"), Some(&1));
    assert_eq!(coralswarm_backend::workers::composio_capture::sweep_pending(&worker, &pool, &state, &client).await.unwrap(), 0);
    assert_eq!(broker.lock().unwrap().account_reads, reads, "nothing re-read inside the 30s window");
    sqlx::query("UPDATE composio_connections SET updated_at=now()-interval '1 minute' WHERE id=$1").bind(ids["ca_initiated"]).execute(&owner).await.unwrap();
    broker.lock().unwrap().accounts.insert("ca_initiated".into(), account("ACTIVE"));
    assert_eq!(coralswarm_backend::workers::composio_capture::sweep_pending(&worker, &pool, &state, &client).await.unwrap(), 1, "finishing later still completes");
    assert_eq!(row("ca_initiated").await.0, "active");
    let remaining: i64 = sqlx::query_scalar(pending_sql).bind(None::<String>).fetch_one(&pool).await.unwrap();
    assert_eq!(remaining, 0);

    // The page shows the abandoned sign-in as such.
    let tenant = Tenant::legacy(&state);
    let (_, body) = result(composio::status(State(state.clone()), tenant, Extension(alice.clone()), Path("linear".into())).await).await;
    let expired = body["connections"].as_array().unwrap().iter().find(|c| c["id"] == json!(ids["ca_expired"])).unwrap().clone();
    assert_eq!(expired["message"], composio::SIGN_IN_EXPIRED);
    eprintln!(
        "composio PG sweep: {} connections, {} active, {} reconnect_needed, {} captures",
        count(&owner, "SELECT count(*) FROM composio_connections WHERE account_id=$1", alice.account_id).await,
        count(&owner, "SELECT count(*) FROM composio_connections WHERE account_id=$1 AND status='active'", alice.account_id).await,
        count(&owner, "SELECT count(*) FROM composio_connections WHERE account_id=$1 AND status='reconnect_needed'", alice.account_id).await,
        count(&owner, "SELECT count(*) FROM composio_capture_scopes WHERE account_id=$1", alice.account_id).await,
    );
    pool.close().await;
    drop_database(&dsn, name, owner).await;
}

/// A migrated control database, its app-role pool and a mock broker for the
/// namespace re-check tests. `None` when no task-owned database is configured.
struct NamespaceFixture {
    dsn: String,
    name: &'static str,
    owner: PgPool,
    state: Arc<coralswarm_backend::AppState>,
    tenant: Tenant,
    broker: Shared,
    client: Client,
}
async fn namespace_fixture(name: &'static str) -> Option<NamespaceFixture> {
    let Ok(dsn) = std::env::var("COMPOSIO_TEST_DATABASE_URL") else {
        assert!(std::env::var_os("REQUIRE_PG_SUITE").is_none(), "COMPOSIO_TEST_DATABASE_URL required");
        return None;
    };
    let (_database, owner) = fresh_database(&dsn, name).await;
    coralswarm_backend::CONTROL_MIGRATOR.run(&owner).await.unwrap();
    support::app_role::set_app_role_password_with_retry(&owner).await;
    let pool = PgPoolOptions::new()
        .max_connections(6)
        .connect_with(task_owned(&dsn, name).username("coralswarm_app").password(support::app_role::APP_ROLE_TEST_PASSWORD))
        .await
        .unwrap();
    let state = Arc::new(coralswarm_backend::test_app_state_owned_with_db_and_store(pool, None));
    let tenant = Tenant::legacy(&state);
    let broker: Shared = Arc::default();
    let client = mock_broker(broker.clone()).await;
    Some(NamespaceFixture { dsn, name, owner, state, tenant, broker, client })
}
impl NamespaceFixture {
    /// Broker traffic since the last drain: (namespace tool calls, all tool
    /// calls, connected-account reads).
    fn drain(&self) -> (usize, usize, usize) {
        let mut b = self.broker.lock().unwrap();
        let calls = std::mem::take(&mut b.calls);
        let reads = std::mem::take(&mut b.account_reads).into_values().sum();
        (calls.iter().filter(|c| NAMESPACE_CALLS.contains(&c.as_str())).count(), calls.len(), reads)
    }
    /// Every shipped toolkit answers as another workspace (or back again).
    fn moved(&self, moved: bool) {
        let mut b = self.broker.lock().unwrap();
        b.org = moved.then(|| "1e1e1e1e-0000-4000-8000-0000000000ff".into());
        b.jira_site = moved.then_some("two");
        b.moved = moved;
    }
    /// A verified, self-checked connection of `toolkit` and its capture, with
    /// the Composio account the verify route reads.
    async fn capture(&self, toolkit: &str) -> (UserContext, Uuid, Uuid) {
        let user = seed(&self.owner).await;
        self.state.dek_cache.insert(&coralswarm_backend::tenancy::TenantId::Consumer, user.account_id, [42; 32]);
        let remote = format!("ca_{toolkit}_{}", Uuid::new_v4().simple());
        let scopes = match toolkit {
            "jira" => json!(["read:jira-work"]),
            "confluence" => json!(["read:content-details:confluence"]),
            "hubspot" => json!(["oauth", "crm.objects.contacts.read", "crm.objects.companies.read", "crm.objects.deals.read"]),
            _ => json!(["read"]),
        };
        self.broker.lock().unwrap().accounts.insert(
            remote.clone(),
            json!({"status":"ACTIVE","user_id":format!("coralswarm:{}:{}", user.account_id, user.user_id),"toolkit":{"slug":toolkit},"auth_config":{"id":"fixture-read-only"},"requested_scopes":scopes,"params":{"base_url":"https://acme.zendesk.com"}}),
        );
        let namespace = self.client.namespace(recipe::get(toolkit).unwrap(), &remote, "fixture").await.unwrap();
        let conn: Uuid = sqlx::query_scalar("INSERT INTO composio_connections(account_id,owner_user_id,service,connected_account_id,auth_config_id,self_check) VALUES($1,$2,$3,$4,'fixture-read-only','passed') RETURNING id")
            .bind(user.account_id).bind(user.user_id).bind(toolkit).bind(&remote).fetch_one(&self.owner).await.unwrap();
        let mut tx = coralswarm_backend::tenancy::scope::begin_account_scoped(self.state.control_db().unwrap(), user.account_id).await.unwrap();
        assert!(composio::bind_connection_namespace_tx(&mut tx, &user, conn, &namespace).await.unwrap());
        tx.commit().await.unwrap();
        let (status, body) = result(composio::activate(&self.state, &self.tenant, &user, recipe::get(toolkit).unwrap(), conn, &self.client).await).await;
        assert_eq!(status, StatusCode::OK, "{toolkit} activate {body}");
        self.drain();
        (user, conn, Uuid::parse_str(body["capture_id"].as_str().unwrap()).unwrap())
    }
    async fn poll(&self, user: &UserContext, toolkit: &str, capture: Uuid) -> (StatusCode, Value) {
        result(composio::poll(&self.state, &self.tenant, user, recipe::get(toolkit).unwrap(), capture, &self.client).await).await
    }
    async fn items(&self, capture: Uuid) -> i64 {
        sqlx::query_scalar("SELECT count(*) FROM source_items WHERE graph_id=(SELECT graph_id FROM composio_capture_scopes WHERE id=$1)").bind(capture).fetch_one(&self.owner).await.unwrap()
    }
    /// Serves one item updated now on `toolkit`'s first change stream.
    fn serve_issue(&self, toolkit: &'static str, id: &str) {
        let now = Utc::now();
        let stamp = now.to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
        let mut b = self.broker.lock().unwrap();
        let item = match toolkit {
            "zendesk" => json!({"id":id,"subject":"Fresh decision","description":"Decision: ship it.","status":"open","created_at":stamp,"updated_at":stamp}),
            "dart" => json!({"id":id,"title":"Fresh decision","description":"Decision: ship it.","status":"To-do","createdBy":"Morgan","createdAt":stamp,"updatedAt":stamp}),
            "confluence" => json!({"url":format!("/spaces/ENG/pages/{id}"),"lastModified":stamp,"content":{"id":id,"title":"Fresh decision","status":"current","body":{"storage":{"value":"<p>Decision: ship it.</p>"}},"version":{"when":stamp,"by":{"accountId":"acc-1","displayName":"Morgan"}},"history":{"createdBy":{"accountId":"acc-1","displayName":"Morgan"}},"space":{"key":"ENG","name":"Engineering"}}}),
            "hubspot" => json!({"id":id,"updatedAt":stamp,"properties":{"name":"Fresh decision","description":"Decision: ship it.","lifecyclestage":"lead","domain":"acme.com","hubspot_owner_id":"7","hs_lastmodifieddate":stamp}}),
            _ => Value::Null,
        };
        if !item.is_null() {
            b.items.entry(toolkit).or_default().push(item);
            return;
        }
        match toolkit {
            "linear" => b.issues.push(issue(id, "Fresh decision", now)),
            _ => b.jira_issues.push(json!({"id":id,"key":format!("SC-{id}"),"fields":{"summary":"Fresh decision","description":{"type":"doc","version":1,"content":[{"type":"paragraph","content":[{"type":"text","text":"Decision: ship it."}]}]},"status":{"name":"To Do"},"project":{"id":"10001","name":"Search"},"creator":{"accountId":"acc-1","displayName":"Morgan"},"created":now.format("%Y-%m-%dT%H:%M:%S%.3f%z").to_string(),"updated":now.format("%Y-%m-%dT%H:%M:%S%.3f%z").to_string()}})),
        }
    }
    async fn checked_at(&self, conn: Uuid) -> Option<DateTime<Utc>> {
        sqlx::query_scalar("SELECT namespace_checked_at FROM composio_connections WHERE id=$1").bind(conn).fetch_one(&self.owner).await.unwrap()
    }
    async fn checked_ago(&self, conn: Uuid, hours: i64) {
        sqlx::query("UPDATE composio_connections SET namespace_checked_at=now()-make_interval(hours=>$2::int) WHERE id=$1")
            .bind(conn).bind(hours as i32).execute(&self.owner).await.unwrap();
    }
    async fn done(self) {
        drop_database(&self.dsn, self.name, self.owner).await;
    }
}
const TOOLKITS: [&str; 6] = ["jira", "linear", "zendesk", "dart", "confluence", "hubspot"];
/// What one live namespace check costs, and how many change streams a recipe
/// pages: (namespace tool calls, connected-account reads, streams).
fn shape(toolkit: &str) -> (usize, usize, usize) {
    match toolkit {
        "linear" => (1, 0, 3),
        "jira" => (2, 0, 1),
        // Zendesk's namespace is the connected account's host: a read, never a tool call.
        "zendesk" => (0, 1, 1),
        "dart" => (1, 0, 1),
        "confluence" => (2, 0, 2),
        "hubspot" => (1, 0, 6),
        other => panic!("{other}"),
    }
}
/// The broker traffic of a poll that makes `checks` live namespace checks and
/// `streams` change-stream calls: what `drain` returns.
fn traffic(toolkit: &str, checks: usize, streams: usize) -> (usize, usize, usize) {
    let (calls, reads, _) = shape(toolkit);
    (checks * calls, checks * calls + streams, checks * reads)
}

#[tokio::test]
async fn a_normal_poll_trusts_the_bound_namespace() {
    let Some(fx) = namespace_fixture("composio_ns_normal").await else { return };
    let mut measured = vec![];
    for toolkit in TOOLKITS {
        let (user, conn, capture) = fx.capture(toolkit).await;
        let (status, body) = fx.poll(&user, toolkit, capture).await;
        assert_eq!(status, StatusCode::OK, "{toolkit} {body}");
        let first = fx.drain();
        let (status, body) = fx.poll(&user, toolkit, capture).await;
        assert_eq!(status, StatusCode::OK, "{toolkit} {body}");
        let idle = fx.drain();
        eprintln!(
            "composio idle poll {toolkit}: first poll {} tool calls ({} namespace, {} account reads), normal poll {} tool calls ({} namespace, {} account reads)",
            first.1, first.0, first.2, idle.1, idle.0, idle.2
        );
        measured.push((toolkit, user, conn, capture, first, idle));
    }
    for (toolkit, user, conn, capture, first, idle) in measured {
        let streams = shape(toolkit).2;
        assert_eq!(first, traffic(toolkit, 1, streams), "{toolkit}: a never-checked connection re-resolves once");
        assert!(fx.checked_at(conn).await.is_some_and(|t| Utc::now() - t < Duration::minutes(1)), "{toolkit}: the check is stored");
        assert_eq!(idle, traffic(toolkit, 0, streams), "{toolkit}: a normal poll neither calls a tool nor reads the account for the namespace");
        // A pending self-check treats a refused call as a recipe failure only
        // after a live namespace call, so it re-resolves on every poll.
        sqlx::query("UPDATE composio_connections SET self_check='pending' WHERE id=$1").bind(conn).execute(&fx.owner).await.unwrap();
        let (status, body) = fx.poll(&user, toolkit, capture).await;
        assert_eq!(status, StatusCode::OK, "{toolkit} {body}");
        assert_eq!(fx.drain(), traffic(toolkit, 1, 2 * streams), "{toolkit}: namespace, probe and change streams");
    }
    fx.done().await;
}

#[tokio::test]
async fn a_poll_after_24_hours_rechecks_the_namespace() {
    let Some(fx) = namespace_fixture("composio_ns_daily").await else { return };
    for toolkit in TOOLKITS {
        let streams = shape(toolkit).2;
        let (user, conn, capture) = fx.capture(toolkit).await;
        fx.poll(&user, toolkit, capture).await;
        fx.drain();
        fx.checked_ago(conn, 23).await;
        assert_eq!(fx.poll(&user, toolkit, capture).await.0, StatusCode::OK);
        assert_eq!(fx.drain(), traffic(toolkit, 0, streams), "{toolkit}: checked 23 hours ago");
        fx.checked_ago(conn, 25).await;
        assert_eq!(fx.poll(&user, toolkit, capture).await.0, StatusCode::OK);
        assert_eq!(fx.drain(), traffic(toolkit, 1, streams), "{toolkit}: checked 25 hours ago");
        assert!(fx.checked_at(conn).await.is_some_and(|t| Utc::now() - t < Duration::minutes(1)), "{toolkit}: the new check is stored");
        assert_eq!(fx.poll(&user, toolkit, capture).await.0, StatusCode::OK);
        assert_eq!(fx.drain(), traffic(toolkit, 0, streams), "{toolkit}: the daily check resets the clock");
    }
    fx.done().await;
}

#[tokio::test]
async fn a_poll_after_an_auth_or_contract_failure_rechecks_the_namespace() {
    let Some(fx) = namespace_fixture("composio_ns_failure").await else { return };
    for toolkit in TOOLKITS {
        let streams = shape(toolkit).2;
        let (user, conn, capture) = fx.capture(toolkit).await;
        fx.poll(&user, toolkit, capture).await;
        fx.drain();
        assert!(fx.checked_at(conn).await.is_some());

        // Contract failure on a change stream: retry, and re-resolve next time.
        fx.broker.lock().unwrap().contract = true;
        let (status, body) = fx.poll(&user, toolkit, capture).await;
        assert_eq!((status, body["state"].as_str(), body["error_code"].as_str()), (StatusCode::SERVICE_UNAVAILABLE, Some("retry"), Some("contract")), "{toolkit}");
        let failing = fx.drain();
        assert_eq!((failing.0, failing.2), (0, 0), "{toolkit}: the failing poll trusted the stored check");
        assert_eq!(fx.checked_at(conn).await, None, "{toolkit}: a contract failure makes the check due");
        fx.broker.lock().unwrap().contract = false;
        assert_eq!(fx.poll(&user, toolkit, capture).await.0, StatusCode::OK);
        assert_eq!(fx.drain(), traffic(toolkit, 1, streams), "{toolkit}: poll after a contract failure");

        // Auth failure on a change stream: reconnect needed until verified again.
        fx.broker.lock().unwrap().revoked = true;
        let (status, body) = fx.poll(&user, toolkit, capture).await;
        assert_eq!((status, body["state"].as_str()), (StatusCode::UNPROCESSABLE_ENTITY, Some("reconnect_needed")), "{toolkit} {body}");
        let failing = fx.drain();
        assert_eq!((failing.0, failing.2), (0, 0), "{toolkit}: the failing poll trusted the stored check");
        assert_eq!(fx.checked_at(conn).await, None, "{toolkit}: an auth failure makes the check due");
        fx.broker.lock().unwrap().revoked = false;
        let (status, body) = result(composio::verify_with(&fx.state, &fx.tenant, &user, recipe::get(toolkit).unwrap(), conn, &fx.client).await).await;
        assert_eq!(status, StatusCode::OK, "{toolkit} re-verify {body}");
        fx.drain();
        assert_eq!(fx.poll(&user, toolkit, capture).await.0, StatusCode::OK);
        assert_eq!(fx.drain(), traffic(toolkit, 1, streams), "{toolkit}: poll after an auth failure");
        assert_eq!(fx.poll(&user, toolkit, capture).await.0, StatusCode::OK);
        assert_eq!(fx.drain(), traffic(toolkit, 0, streams), "{toolkit}: then back to normal");
    }
    fx.done().await;
}

#[tokio::test]
async fn a_changed_namespace_keeps_the_mismatch_handling() {
    let Some(fx) = namespace_fixture("composio_ns_changed").await else { return };
    for toolkit in TOOLKITS {
        let (user, conn, capture) = fx.capture(toolkit).await;
        fx.poll(&user, toolkit, capture).await;
        fx.moved(true);
        fx.checked_ago(conn, 25).await;
        let (status, body) = fx.poll(&user, toolkit, capture).await;
        assert_eq!(
            (status, body["state"].as_str(), body["error_code"].as_str()),
            (StatusCode::UNPROCESSABLE_ENTITY, Some("reconnect_needed"), Some("reconnect")),
            "{toolkit} {body}"
        );
        let (conn_status, namespace): (String, String) = sqlx::query_as("SELECT status,provider_namespace FROM composio_connections WHERE id=$1").bind(conn).fetch_one(&fx.owner).await.unwrap();
        assert_eq!(conn_status, "reconnect_needed", "{toolkit}");
        let kept: String = sqlx::query_scalar("SELECT provider_namespace FROM composio_capture_scopes WHERE id=$1").bind(capture).fetch_one(&fx.owner).await.unwrap();
        assert_eq!(kept, namespace, "{toolkit}: the bound namespace is never rewritten");
        assert_eq!(fx.checked_at(conn).await, None, "{toolkit}");
        let (status, _) = result(composio::verify_with(&fx.state, &fx.tenant, &user, recipe::get(toolkit).unwrap(), conn, &fx.client).await).await;
        assert_eq!(status, StatusCode::CONFLICT, "{toolkit}: the grant now points elsewhere; connect it separately");
        fx.moved(false);
        fx.drain();
    }
    fx.done().await;
}

#[tokio::test]
async fn a_pass_with_items_rechecks_the_namespace_before_ingesting() {
    let Some(fx) = namespace_fixture("composio_ns_items").await else { return };
    for toolkit in TOOLKITS {
        let streams = shape(toolkit).2;
        // One changed item fans out to one child list where the recipe has one.
        let children = usize::from(matches!(toolkit, "jira" | "zendesk"));
        let (user, conn, capture) = fx.capture(toolkit).await;
        fx.poll(&user, toolkit, capture).await;
        fx.drain();

        // A fresh stored check still re-resolves once a pass has something to ingest.
        fx.serve_issue(toolkit, &format!("{toolkit}-1"));
        let (status, body) = fx.poll(&user, toolkit, capture).await;
        assert_eq!(status, StatusCode::OK, "{toolkit} {body}");
        assert_eq!(fx.drain(), traffic(toolkit, 1, streams + children), "{toolkit}: an active pass checks once");
        assert_eq!(fx.items(capture).await, 1, "{toolkit}");

        // The grant now reaches another site or workspace: nothing from it lands
        // under the bound namespace, even with a fresh stored check.
        fx.moved(true);
        fx.serve_issue(toolkit, &format!("{toolkit}-2"));
        assert!(fx.checked_at(conn).await.is_some_and(|t| Utc::now() - t < Duration::minutes(1)));
        let (status, body) = fx.poll(&user, toolkit, capture).await;
        assert_eq!((status, body["state"].as_str()), (StatusCode::UNPROCESSABLE_ENTITY, Some("reconnect_needed")), "{toolkit} {body}");
        assert_eq!(fx.items(capture).await, 1, "{toolkit}: the other workspace's item is never ingested");
        fx.moved(false);
        {
            let mut b = fx.broker.lock().unwrap();
            b.issues.clear();
            b.jira_issues.clear();
            b.items.clear();
        }
        fx.drain();
    }
    fx.done().await;
}

/// Recipe capture hints title atoms from the provider's own fields: the issue
/// key and title, the parent a comment belongs to, the ticket subject. Before
/// the hints, every Composio atom was titled from the rendered content, so a
/// Library list read `comment Service: linear Project: <uuid> ...`. Every
/// atom also reaches the funnel's stages (version linked, embed queued).
#[tokio::test]
async fn recipe_hints_title_every_toolkits_atoms_from_provider_fields() {
    let Some(fx) = namespace_fixture("composio_hint_titles").await else { return };
    let now = Utc::now();
    let stamp = now.to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    let jira_time = now.format("%Y-%m-%dT%H:%M:%S%.3f%z").to_string();
    {
        let mut b = fx.broker.lock().unwrap();
        b.comments = vec![json!({"id":"c-1","body":"Agreed, ship it on Friday.","createdAt":stamp,"updatedAt":stamp,
            "user":{"id":"u2","name":"Casey"},
            "issue":{"id":"linear-h","title":"Fresh decision","identifier":"COR-linear-h","project":{"id":"6f1d1c2a-3b4c-4d5e-8f60-718293a4b5c6","name":"Search"}}})];
        b.jira_comments = vec![json!({"id":"20001","created":jira_time,"updated":jira_time,
            "body":{"type":"doc","version":1,"content":[{"type":"paragraph","content":[{"type":"text","text":"Agreed, ship it on Friday."}]}]},
            "author":{"accountId":"acc-2","displayName":"Casey"}})];
        b.zendesk_events = vec![json!({"id":9001,"created_at":stamp,"content":{"body":"<p>Thanks, we shipped it.</p>"},"author":{"display_name":"Casey"}})];
    }
    let expected: [(&str, &[(&str, &str)]); 6] = [
        ("linear", &[("linear_comment", "Comment on COR-linear-h: Fresh decision"), ("linear_issue", "COR-linear-h: Fresh decision")]),
        ("jira", &[("jira_comment", "Comment on SC-jira-h: Fresh decision"), ("jira_issue", "SC-jira-h: Fresh decision")]),
        ("zendesk", &[("zendesk_comment", "Comment on Fresh decision"), ("zendesk_ticket", "Fresh decision")]),
        ("dart", &[("dart_task", "Fresh decision")]),
        ("confluence", &[("confluence_content", "Fresh decision")]),
        ("hubspot", &[("hubspot_company", "Fresh decision")]),
    ];
    for (toolkit, want) in expected {
        let (user, _conn, capture) = fx.capture(toolkit).await;
        fx.serve_issue(toolkit, &format!("{toolkit}-h"));
        let (status, body) = fx.poll(&user, toolkit, capture).await;
        assert_eq!(status, StatusCode::OK, "{toolkit} {body}");
        let graph: Uuid = sqlx::query_scalar("SELECT graph_id FROM composio_capture_scopes WHERE id=$1").bind(capture).fetch_one(&fx.owner).await.unwrap();
        let rows: Vec<(String, Option<String>, Option<String>)> = sqlx::query_as(
            "SELECT source_type,title,title_kind FROM atoms WHERE graph_id=$1 ORDER BY source_type,source_id",
        )
        .bind(graph)
        .fetch_all(&fx.owner)
        .await
        .unwrap();
        let got: Vec<(&str, &str)> = rows.iter().map(|(t, title, _)| (t.as_str(), title.as_deref().unwrap_or("<none>"))).collect();
        assert_eq!(got, want.to_vec(), "{toolkit}: atom titles come from the recipe's title hints");
        assert!(rows.iter().all(|r| r.2.as_deref() == Some("source")),
            "{toolkit}: a recipe title is the provider's own (title_kind='source'), never overwritten by the title sweep: {rows:?}");
        // Every atom reached the funnel: linked to its version, embed queued.
        let unlinked: i64 = sqlx::query_scalar("SELECT count(*) FROM atoms a WHERE a.graph_id=$1 AND NOT EXISTS(SELECT 1 FROM source_version_chunks c WHERE c.graph_id=a.graph_id AND c.atom_id=a.atom_id)")
            .bind(graph).fetch_one(&fx.owner).await.unwrap();
        assert_eq!(unlinked, 0, "{toolkit}: every chunk is linked to its source version");
        let unembedded: i64 = sqlx::query_scalar("SELECT count(*) FROM atoms a WHERE a.graph_id=$1 AND NOT EXISTS(SELECT 1 FROM jobs j WHERE j.kind='EmbedAtoms' AND j.payload->'atom_ids' ? a.atom_id::text)")
            .bind(graph).fetch_one(&fx.owner).await.unwrap();
        assert_eq!(unembedded, 0, "{toolkit}: every chunk has an embed job");
        let mut b = fx.broker.lock().unwrap();
        b.issues.clear();
        b.jira_issues.clear();
        b.items.clear();
    }
    fx.done().await;
}

fn capture_worker() -> Arc<coralswarm_backend::workers::composio_capture::Worker> {
    coralswarm_backend::workers::composio_capture::Worker::new(
        Arc::new(coralswarm_backend::test_composio_budget::CallBudget::per_minute(1_000)),
        2,
        std::time::Duration::from_secs(5),
        coralswarm_backend::shutdown::Shutdown::new(),
    )
}
impl NamespaceFixture {
    /// One worker tick on the consumer plane, and the round it starts.
    async fn round(&self, worker: &Arc<coralswarm_backend::workers::composio_capture::Worker>) {
        let pool = self.state.control_db().unwrap().clone();
        let plane = coralswarm_backend::workers::tenant_harness::WorkerTenant {
            tenant: coralswarm_backend::tenancy::TenantId::Consumer,
            store: Arc::new(coralswarm_backend::store::PostgresStore::new(pool.clone())),
            pool: pool.clone(),
        };
        let client = self.client.clone();
        worker.tick_with(&plane, &pool, &self.state, || Ok(client)).await.unwrap();
        worker.settle().await;
    }
    async fn due_now(&self, capture: Uuid) {
        sqlx::query("UPDATE composio_capture_scopes SET next_poll_at=now()-interval '1 second' WHERE id=$1").bind(capture).execute(&self.owner).await.unwrap();
    }
    async fn leave(&self, user: &UserContext) {
        sqlx::query("DELETE FROM account_members WHERE account_id=$1 AND user_id=$2").bind(user.account_id).bind(user.user_id).execute(&self.owner).await.unwrap();
    }
    async fn remote(&self, conn: Uuid) -> String {
        sqlx::query_scalar("SELECT connected_account_id FROM composio_connections WHERE id=$1").bind(conn).fetch_one(&self.owner).await.unwrap()
    }
    async fn revocation(&self, conn: Uuid) -> (String, Option<DateTime<Utc>>) {
        sqlx::query_as("SELECT status,revoked_at FROM composio_connections WHERE id=$1").bind(conn).fetch_one(&self.owner).await.unwrap()
    }
    async fn capture_state(&self, capture: Uuid) -> String {
        sqlx::query_scalar("SELECT state FROM composio_capture_scopes WHERE id=$1").bind(capture).fetch_one(&self.owner).await.unwrap()
    }
    /// Moves every connection's last attempt and disconnect back, as if the
    /// retry backoff had passed.
    async fn age_connections(&self, minutes: i32) {
        sqlx::query("UPDATE composio_connections SET updated_at=updated_at-make_interval(mins=>$1),disconnected_at=disconnected_at-make_interval(mins=>$1)")
            .bind(minutes).execute(&self.owner).await.unwrap();
    }
    fn deletes(&self) -> Vec<String> {
        self.broker.lock().unwrap().deleted.clone()
    }
    /// A capture whose member left after it captured one item, due now.
    async fn departed(&self) -> (Uuid, Uuid, String) {
        let (user, conn, capture) = self.capture("linear").await;
        self.serve_issue("linear", &format!("left-{}", Uuid::new_v4().simple()));
        assert_eq!(self.poll(&user, "linear", capture).await.0, StatusCode::OK);
        assert_eq!(self.items(capture).await, 1);
        self.broker.lock().unwrap().issues.clear();
        self.drain();
        self.leave(&user).await;
        self.due_now(capture).await;
        (conn, capture, self.remote(conn).await)
    }
}

/// A member who left loses provider access as Disconnect would take it: the
/// worker parks their capture and revokes the connected account once. Atoms stay.
#[tokio::test]
async fn a_departed_members_park_revokes_the_connected_account() {
    let Some(fx) = namespace_fixture("composio_revoke_left").await else { return };
    let (conn, capture, remote) = fx.departed().await;
    fx.round(&capture_worker()).await;
    assert_eq!(fx.deletes(), vec![remote], "exactly one revoke, with revoke_on_delete=true");
    let (status, revoked) = fx.revocation(conn).await;
    assert_eq!(status, "disconnected");
    assert!(revoked.is_some(), "the revocation is recorded");
    assert_eq!(fx.capture_state(capture).await, "disconnected");
    assert_eq!(fx.items(capture).await, 1, "no atom is deleted");
    fx.done().await;
}

/// Composio's 404 means the connected account is already gone: revoked.
#[tokio::test]
async fn a_departed_members_revoke_counts_404_as_revoked() {
    let Some(fx) = namespace_fixture("composio_revoke_404").await else { return };
    let (conn, _, remote) = fx.departed().await;
    fx.broker.lock().unwrap().delete_status = Some(StatusCode::NOT_FOUND);
    fx.round(&capture_worker()).await;
    assert_eq!(fx.deletes(), vec![remote]);
    assert!(fx.revocation(conn).await.1.is_some(), "404 is recorded as revoked");
    fx.done().await;
}

/// A failed revoke leaves the connection disconnected and unrevoked; a later
/// round retries it once the backoff has passed, and not before.
#[tokio::test]
async fn a_failed_revoke_retries_in_a_later_round() {
    let Some(fx) = namespace_fixture("composio_revoke_retry").await else { return };
    let (conn, capture, remote) = fx.departed().await;
    let worker = capture_worker();
    fx.broker.lock().unwrap().delete_status = Some(StatusCode::INTERNAL_SERVER_ERROR);
    fx.round(&worker).await;
    assert_eq!(fx.deletes(), vec![remote.clone()]);
    assert_eq!(fx.revocation(conn).await, ("disconnected".to_string(), None), "a 5xx is not revoked");
    assert_eq!(fx.capture_state(capture).await, "disconnected", "the capture is parked regardless");
    fx.broker.lock().unwrap().delete_status = None;
    fx.round(&worker).await;
    assert_eq!(fx.deletes().len(), 1, "no retry inside the backoff");
    fx.age_connections(2).await;
    fx.round(&worker).await;
    assert_eq!(fx.deletes(), vec![remote.clone(), remote], "retried once the backoff passed");
    assert!(fx.revocation(conn).await.1.is_some());
    assert_eq!(fx.items(capture).await, 1, "no atom is deleted");
    fx.done().await;
}

/// Only a member who left is revoked: an account moved off this plane keeps
/// its connection, and a member's own Disconnect whose revoke failed is theirs
/// to retry.
#[tokio::test]
async fn an_off_plane_park_sends_no_revoke() {
    let Some(fx) = namespace_fixture("composio_revoke_off_plane").await else { return };
    let (moved_user, moved_conn, moved) = fx.capture("linear").await;
    let mut tx = coralswarm_backend::tenancy::scope::begin_account_scoped(&fx.owner, moved_user.account_id).await.unwrap();
    coralswarm_backend::billing::renewal::lock_period_rotation(&mut tx, moved_user.account_id).await.unwrap();
    sqlx::query("SET CONSTRAINTS account_members_account_org_fk DEFERRED").execute(&mut *tx).await.unwrap();
    sqlx::query("UPDATE accounts SET clerk_org_id='org_moved',is_org_account=true WHERE id=$1")
        .bind(moved_user.account_id).execute(&mut *tx).await.unwrap();
    sqlx::query("UPDATE account_members SET account_is_org_account=true,seat_tier_id=(SELECT id FROM seat_tiers WHERE code='standard' AND is_active) WHERE account_id=$1")
        .bind(moved_user.account_id).execute(&mut *tx).await.unwrap();
    sqlx::query("INSERT INTO tenant_databases(clerk_org_id,account_id,slug,db_secret_ref,status) VALUES('org_moved',$1,'moved','MOVED','active')")
        .bind(moved_user.account_id).execute(&mut *tx).await.unwrap();
    tx.commit().await.unwrap();
    fx.due_now(moved).await;
    let (_, stayed_conn, _) = fx.capture("linear").await;
    sqlx::query("UPDATE composio_connections SET status='disconnected',disconnected_at=now()-interval '1 day',updated_at=now()-interval '1 day' WHERE id=$1")
        .bind(stayed_conn).execute(&fx.owner).await.unwrap();
    let (left_conn, _, left_remote) = fx.departed().await;
    fx.round(&capture_worker()).await;
    assert_eq!(fx.capture_state(moved).await, "disconnected", "the off-plane capture is parked");
    assert_eq!(fx.revocation(moved_conn).await, ("active".to_string(), None), "its connection is left alone");
    assert_eq!(fx.revocation(stayed_conn).await, ("disconnected".to_string(), None), "a present member's Disconnect is not retried");
    assert_eq!(fx.deletes(), vec![left_remote], "only the departed member's account is revoked");
    assert!(fx.revocation(left_conn).await.1.is_some());
    fx.done().await;
}

/// Revoking is idempotent: later rounds send nothing more, even once any
/// backoff has passed and the capture comes due again.
#[tokio::test]
async fn rerunning_a_round_sends_no_more_revokes() {
    let Some(fx) = namespace_fixture("composio_revoke_rerun").await else { return };
    let (conn, capture, remote) = fx.departed().await;
    let worker = capture_worker();
    fx.round(&worker).await;
    assert_eq!(fx.deletes(), vec![remote.clone()]);
    let revoked = fx.revocation(conn).await.1;
    assert!(revoked.is_some());
    for _ in 0..2 {
        fx.age_connections(600).await;
        fx.due_now(capture).await;
        fx.round(&worker).await;
    }
    assert_eq!(fx.deletes(), vec![remote], "nothing more is sent");
    assert_eq!(fx.capture_state(capture).await, "disconnected");
    assert_eq!(fx.revocation(conn).await.1, revoked, "revoked_at is not rewritten");
    fx.done().await;
}
