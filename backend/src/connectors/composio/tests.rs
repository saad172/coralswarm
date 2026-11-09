use super::*;
use std::collections::BTreeMap;

fn linear() -> &'static Recipe {
    recipe::get("linear").unwrap()
}
fn jira() -> &'static Recipe {
    recipe::get("jira").unwrap()
}
fn at(s: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
}
fn issue(r: &Recipe, ns: &str, v: &Value) -> Result<Item, Error> {
    normalize(r, ns, stream_shape(&r.streams[0]), v, None, None, None)
}
#[test]
fn null_or_empty_graphql_errors_are_success() {
    assert!(!has_provider_errors(&json!({"data":{"organization":{"id":"x"}},"errors":null})));
    assert!(!has_provider_errors(&json!({"errors":[]})));
    assert!(!has_provider_errors(&json!({"data":{}})));
    assert!(has_provider_errors(&json!({"errors":[{"message":"denied"}]})));
    assert!(has_provider_errors(&json!({"errors":"denied"})));
}
#[test]
fn mutable_reference_does_not_change_source_identity() {
    let mut v = json!({"id":"10001","key":"OLD-1","fields":{"summary":"Issue","updated":"2026-10-10T14:00:00Z","project":{"id":"10000"}}});
    let a = issue(jira(), "jira:site:cloud-id", &v).unwrap();
    v["key"] = json!("NEW-8");
    let b = issue(jira(), "jira:site:cloud-id", &v).unwrap();
    assert_eq!(a.source_id, b.source_id);
    assert_ne!(a.content, b.content);
}
#[test]
fn moving_an_issue_between_projects_keeps_its_identity() {
    let mut v = json!({"id":"issue-1","identifier":"ENG-1","title":"t","updatedAt":"2026-10-10T14:00:00Z","project":{"id":"p1"}});
    let a = issue(linear(), "linear:workspace:w", &v).unwrap();
    v["project"] = Value::Null;
    let b = issue(linear(), "linear:workspace:w", &v).unwrap();
    assert_eq!(a.source_id, "linear:workspace:w/issue/issue-1");
    assert_eq!(a.source_id, b.source_id);
    assert_eq!(a.container.unwrap().id, "p1");
    assert!(b.container.is_none());
    assert!(!b.content.contains("Project:"), "no project line for an issue outside any project");
}
#[test]
fn no_id_or_timestamp_never_becomes_an_item() {
    assert!(issue(linear(), "n", &json!({"identifier":"CS-1","updatedAt":"2026-10-10T14:00:00Z"})).is_err());
    assert!(issue(jira(), "n", &json!({"id":"1","fields":{}})).is_err());
    let comments = &linear().streams[1];
    assert!(
        normalize(linear(), "n", stream_shape(comments), &json!({"id":"c","updatedAt":"2026-10-10T14:00:00Z","issue":null}), None, None, None).is_err(),
        "a comment without its immutable parent issue has no identity"
    );
}
#[test]
fn item_limit_is_enforced_before_retaining_extra_children() {
    let item = issue(linear(), "linear:workspace:a", &json!({"id":"i","updatedAt":"2026-10-10T14:00:00Z"})).unwrap();
    let mut items = vec![item.clone(); MAX_CAPTURE_ITEMS - 1];
    assert!(push_item(&mut items, item.clone()).is_ok());
    assert!(push_item(&mut items, item).is_err());
    assert_eq!(items.len(), MAX_CAPTURE_ITEMS);
}
#[test]
fn aggregate_content_and_native_path_components_are_bounded() {
    let mut item = issue(linear(), "linear:workspace:a", &json!({"id":"i","updatedAt":"2026-10-10T14:00:00Z"})).unwrap();
    item.content = "x".repeat(MAX_CAPTURE_BYTES);
    let mut items = Vec::new();
    assert!(push_item(&mut items, item.clone()).is_ok());
    assert!(push_item(&mut items, item).is_err());
    assert!(native_id(Some(&json!("parent/child"))).is_err());
    assert!(native_id(Some(&json!(" native-id "))).is_err());
    assert_eq!(native_id(Some(&json!(7))).unwrap(), "7");
    assert_eq!(native_id(Some(&json!(-7))).unwrap(), "-7");
    assert_eq!(native_id(Some(&json!(u64::MAX))).unwrap(), u64::MAX.to_string());
    assert_eq!(native_id(Some(&json!("0042"))).unwrap(), "0042", "strings are kept byte for byte");
    assert!(native_id(Some(&json!(7.0))).is_err());
    assert!(native_id(Some(&json!(false))).is_err());
}
fn cursor_paging(stream: &Stream) -> &CursorPaging {
    match &stream.paging {
        StreamPaging::Cursor(c) => c,
        StreamPaging::Numbered { .. } => panic!("cursor stream"),
    }
}
#[test]
fn cursors_fail_closed() {
    let issues = cursor_paging(&linear().streams[0]);
    assert_eq!(
        cursor_next(issues, &json!({"data":{"issues":{"pageInfo":{"hasNextPage":true,"endCursor":"opaque"}}}})).unwrap(),
        Some("opaque".into())
    );
    assert!(cursor_next(issues, &json!({"data":{"issues":{"pageInfo":{"hasNextPage":true,"endCursor":""}}}})).is_err());
    assert!(cursor_next(cursor_paging(&jira().streams[0]), &json!({"isLast":false})).is_err());
    assert!(cursor_next(issues, &json!({})).is_err());
}
#[test]
fn child_pages_require_progress() {
    let comments = jira().streams[0].children.as_ref().unwrap();
    assert!(child_page(comments, &json!({"comments":[],"total":2}), 0).is_err());
    assert!(child_page(comments, &json!({"comments":[],"total":0}), 0).unwrap().1.is_none());
    assert!(child_page(comments, &json!({"comments":[{}]}), 0).is_err());
}
#[test]
fn documented_offset_timestamp_is_supported() {
    let item = issue(
        jira(),
        "jira:site:cloud-id",
        &json!({"id":"10001","fields":{"summary":"Issue","updated":"2021-01-18T23:45:00.000+0000"}}),
    )
    .unwrap();
    assert_eq!(item.timestamp.to_rfc3339(), "2021-01-18T23:45:00+00:00");
}
#[test]
fn jira_flat_search_shape_parses_through_fallback_paths() {
    let item = issue(
        jira(),
        "jira:site:cloud-id",
        &json!({"id":"10009","key":"ACME-9","summary":"Flat","description":"Plain text","status":{"name":"Done"},
                "project":{"id":"10000","name":"Acme"},"reporter":{"account_id":"acct-9","display_name":"Riley"},
                "created":"2026-10-01T09:00:00.000+0000","updated":"2026-10-02T09:00:00.000+0000"}),
    )
    .unwrap();
    assert_eq!(item.source_id, "jira:site:cloud-id/issue/10009");
    assert_eq!(item.author_name.as_deref(), Some("Riley"));
    assert_eq!(item.content, "Flat\nService: jira\nProject: 10000\nReference: ACME-9\nState: Done\nPlain text");
    assert_eq!(item.timestamp, at("2026-10-02T09:00:00Z"));
}
#[test]
fn adf_preserves_nested_text() {
    assert_eq!(
        text(&json!({"type":"doc","content":[{"type":"paragraph","content":[{"type":"text","text":"Decision"}]}]})),
        "Decision\n"
    );
}
#[test]
fn window_overlaps_the_watermark_and_rounds_minutes_up() {
    let w = Window::from_watermark(at("2026-10-13T12:00:00Z"), at("2026-10-13T12:15:30Z"));
    assert_eq!(w.since, at("2026-10-13T11:55:00Z"));
    assert_eq!(w.minutes(), 21, "20.5 minutes rounds up so the relative window covers since");
    assert!(w.filter_floor() <= w.since - Duration::seconds(FILTER_SLACK_SECONDS));
    let w = Window { since: at("2026-10-13T12:00:00Z"), now: at("2026-10-13T12:00:00Z") };
    assert_eq!(w.minutes(), 1);
}
#[test]
fn poll_interval_follows_recent_activity() {
    let connected = at("2026-10-01T00:00:00Z");
    let now = at("2026-10-05T00:00:00Z");
    assert_eq!(next_poll_in(now, None, connected), Duration::minutes(15));
    assert_eq!(next_poll_in(now, Some(now - Duration::minutes(59)), connected), Duration::minutes(5));
    assert_eq!(next_poll_in(now, Some(now - Duration::hours(2)), connected), Duration::minutes(15));
    let later = at("2026-10-09T00:00:00Z");
    assert_eq!(next_poll_in(later, None, connected), Duration::minutes(60), "a week without any change");
    assert_eq!(next_poll_in(later, Some(later - Duration::days(6)), connected), Duration::minutes(15));
    assert_eq!(next_poll_in(later, Some(later - Duration::days(7)), connected), Duration::minutes(60));
}
async fn server(handler: axum::Router) -> (Client, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        axum::serve(listener, handler).await.unwrap();
    });
    (Client::for_mock_broker(&base).unwrap(), task)
}
#[tokio::test]
async fn mock_transport_pins_version_owner_and_connection_and_never_dispatches_unlisted_tools() {
    use axum::{extract::Path, routing::post, Json};
    let app = axum::Router::new().route(
        "/tools/execute/{tool}",
        post(|Path(tool): Path<String>, Json(body): Json<Value>| async move {
            assert_eq!(tool, "LINEAR_RUN_QUERY_OR_MUTATION");
            assert_eq!(body["version"], "20260924_00");
            assert_eq!(body["user_id"], "coralswarm:account:owner");
            assert_eq!(body["connected_account_id"], "ca_selected");
            let args = &body["arguments"];
            assert!(args["variables"].get("after").is_none() || args["variables"]["after"].is_null());
            assert_eq!(args["variables"]["first"], PAGE_SIZE);
            assert_eq!(args["variables"]["since"], "2026-10-13T11:55:00.000Z");
            assert!(args["query_or_mutation"].as_str().unwrap().starts_with("query CoralSwarmIssuesChanged"));
            Json(json!({"successful":true,"data":json!({"data":{"issues":{"nodes":[],"pageInfo":{"hasNextPage":false}}},"errors":null}).to_string()}))
        }),
    );
    let (client, task) = server(app).await;
    let window = Window::from_watermark(at("2026-10-13T12:00:00Z"), at("2026-10-13T12:10:00Z"));
    let page = client
        .page(linear(), "ca_selected", "coralswarm:account:owner", "linear:workspace:w", &window, &Resume::default(), false)
        .await
        .unwrap();
    assert!(page.items.is_empty());
    assert_eq!(page.next, Some(Resume { stream: 1, cursor: None }), "an exhausted stream hands over to the next");
    let write = Call {
        tool: "LINEAR_UPDATE_ISSUE".into(),
        args: serde_json::from_value(json!({})).unwrap(),
    };
    assert!(matches!(
        client.execute(linear(), "ca_selected", "owner", &write, &Bindings::default()).await,
        Err(Error::Contract(_))
    ));
    // A tool allowed for another toolkit is still outside this recipe.
    let foreign = &jira().streams[0].call;
    assert!(matches!(
        client.execute(linear(), "ca_selected", "owner", foreign, &Bindings::default()).await,
        Err(Error::Contract(_))
    ));
    task.abort();
}
#[tokio::test]
async fn mock_transport_never_follows_broker_redirects() {
    use axum::{http::StatusCode, routing::get};
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    let calls = Arc::new(AtomicUsize::new(0));
    let app = axum::Router::new()
        .route(
            "/redirect",
            get(|| async { (StatusCode::TEMPORARY_REDIRECT, [("location", "/credential-target")]) }),
        )
        .route(
            "/credential-target",
            get({
                let calls = calls.clone();
                move || {
                    let calls = calls.clone();
                    async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        "{}"
                    }
                }
            }),
        );
    let (client, task) = server(app).await;
    assert!(matches!(
        client.request(reqwest::Method::GET, "/redirect", None).await,
        Err(Error::Upstream)
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    task.abort();
}
#[tokio::test]
async fn mock_transport_preserves_retry_after_and_permission_classification() {
    use axum::{http::StatusCode, routing::get};
    let app = axum::Router::new()
        .route(
            "/rate",
            get(|| async { (StatusCode::TOO_MANY_REQUESTS, [("retry-after", "17")], "private provider text") }),
        )
        .route("/denied", get(|| async { (StatusCode::FORBIDDEN, "private provider text") }))
        .route("/expired", get(|| async { (StatusCode::UNAUTHORIZED, "private provider text") }))
        .route("/gone", get(|| async { (StatusCode::NOT_FOUND, "private provider text") }));
    let (client, task) = server(app).await;
    assert!(matches!(client.request(reqwest::Method::GET, "/rate", None).await, Err(Error::RateLimited(17))));
    assert!(matches!(client.request(reqwest::Method::GET, "/denied", None).await, Err(Error::PermissionDenied)));
    assert!(matches!(client.request(reqwest::Method::GET, "/expired", None).await, Err(Error::Reconnect)));
    assert!(matches!(client.request(reqwest::Method::GET, "/gone", None).await, Err(Error::Gone)));
    task.abort();
}
#[tokio::test]
async fn revoke_deletes_with_provider_revocation_and_treats_missing_as_done() {
    use axum::{
        extract::{Path, Query},
        http::StatusCode,
        routing::delete,
        Json,
    };
    let app = axum::Router::new().route(
        "/connected_accounts/{id}",
        delete(|Path(id): Path<String>, Query(q): Query<BTreeMap<String, String>>| async move {
            assert_eq!(q.get("revoke_on_delete").map(String::as_str), Some("true"));
            if id == "ca_gone" {
                return (StatusCode::NOT_FOUND, Json(json!({"error":{"status":404}})));
            }
            assert_eq!(id, "ca_live");
            (StatusCode::OK, Json(json!({"success":true,"revoke_job_id":"job"})))
        }),
    );
    let (client, task) = server(app).await;
    client.revoke("ca_live").await.unwrap();
    client.revoke("ca_gone").await.unwrap();
    assert!(matches!(client.revoke("../auth_configs/x").await, Err(Error::Contract(_))));
    assert!(matches!(client.revoke("ca_x?revoke_on_delete=false").await, Err(Error::Contract(_))));
    task.abort();
}
/// GraphQL operation name of a recipe document (`query Name(...)`).
fn operation(args: &Value) -> Option<&str> {
    let doc = args["query_or_mutation"].as_str()?.trim_start().strip_prefix("query")?.trim_start();
    let end = doc.find(|c: char| !(c.is_ascii_alphanumeric() || c == '_')).unwrap_or(doc.len());
    Some(&doc[..end])
}
/// Answers each tool from compat/inputs.json-shaped routes: the first response
/// whose `op` and `when` pointers equal the request arguments (null = absent).
fn fixture_app(routes: Value) -> axum::Router {
    use axum::{extract::Path, routing::post, Json};
    axum::Router::new().route(
        "/tools/execute/{tool}",
        post(move |Path(tool): Path<String>, Json(body): Json<Value>| {
            let routes = routes.clone();
            async move {
                let args = &body["arguments"];
                let data = routes[&tool]
                    .as_array()
                    .unwrap_or_else(|| panic!("unexpected tool {tool}"))
                    .iter()
                    .find(|r| {
                        r.get("op").and_then(Value::as_str).is_none_or(|op| operation(args) == Some(op))
                            && r["when"]
                                .as_object()
                                .unwrap()
                                .iter()
                                .all(|(p, want)| args.pointer(p).unwrap_or(&Value::Null) == want)
                    })
                    .unwrap_or_else(|| panic!("no fixture for {tool} {args}"))["data"]
                    .clone();
                Json(json!({"successful":true,"data":data}))
            }
        }),
    )
}
async fn full_pass(r: &Recipe, client: &Client, namespace: &str, window: &Window) -> Vec<Item> {
    let mut items = vec![];
    let mut resume = Some(Resume::default());
    let mut pages = 0;
    while let Some(at) = resume {
        let page = client.page(r, "ca_fixture", "owner", namespace, window, &at, false).await.unwrap();
        items.extend(page.items);
        resume = page.next;
        pages += 1;
        assert!(pages < 20, "pass must terminate");
    }
    items
}
/// PR #1239 identities must survive the switch to workspace-wide change
/// queries: every item the new streams produce must equal the item the
/// project-scoped capture retained for the same record (golden.json, recorded
/// from the hand-built code before recipes and never regenerated). The only
/// content difference allowed is Linear's transition snapshot, which the
/// GraphQL change query does not return; that changes a version, never an
/// identity.
#[tokio::test]
async fn workspace_change_queries_reproduce_pr1239_identities_and_content() {
    let inputs: Value = serde_json::from_str(include_str!("compat/inputs.json")).unwrap();
    let golden: Value = serde_json::from_str(include_str!("compat/golden.json")).unwrap();
    let mut checked = 0;
    for (name, scenario) in inputs["scenarios"].as_object().unwrap() {
        let expected = &golden[scenario["golden"].as_str().unwrap()];
        let r = recipe::get(scenario["toolkit"].as_str().unwrap()).unwrap();
        let (client, task) = server(fixture_app(scenario["routes"].clone())).await;
        let namespace = client.namespace(r, "ca_fixture", "owner").await.unwrap();
        assert_eq!(namespace, expected["namespace"], "{name} namespace");
        let window = Window::from_watermark(
            at(scenario["watermark"].as_str().unwrap()),
            at(scenario["now"].as_str().unwrap()),
        );
        let items = full_pass(r, &client, &namespace, &window).await;
        task.abort();
        let mut golden_by_id: BTreeMap<&str, &Value> = BTreeMap::new();
        for g in expected["items"].as_array().unwrap() {
            golden_by_id.insert(g["source_id"].as_str().unwrap(), g);
        }
        let produced_kinds: Vec<&str> = r.kinds();
        let mut seen = BTreeMap::new();
        for item in &items {
            let g = golden_by_id
                .get(item.source_id.as_str())
                .unwrap_or_else(|| panic!("{name}: {} has no PR #1239 counterpart", item.source_id));
            assert_eq!(item.source_type, g["source_type"], "{name} {}", item.source_id);
            assert_eq!(item.kind, g["kind"], "{name} {}", item.source_id);
            assert_eq!(item.author_id.as_deref(), g["author_id"].as_str(), "{name} {}", item.source_id);
            assert_eq!(item.author_name.as_deref(), g["author_name"].as_str(), "{name} {}", item.source_id);
            assert_eq!(item.container.as_ref().map(|c| c.id.as_str()), g["project"].as_str(), "{name} {}", item.source_id);
            // Observed snapshots carried capture time; the change query has the real update time.
            if !g["timestamp"].is_null() {
                assert_eq!(json!(item.timestamp.to_rfc3339()), g["timestamp"], "{name} {}", item.source_id);
            }
            let old = g["content"].as_str().unwrap();
            let old = old.find("\nProvider transition snapshot: ").map_or(old, |at| &old[..at]);
            assert_eq!(item.content, old, "{name} {} content", item.source_id);
            *seen.entry(item.source_id.clone()).or_insert(0) += 1;
        }
        assert!(seen.values().all(|n| *n == 1), "{name}: one item per identity per pass");
        for (id, g) in &golden_by_id {
            if produced_kinds.contains(&g["kind"].as_str().unwrap()) {
                assert!(seen.contains_key(*id), "{name}: {id} is no longer captured");
            }
        }
        checked += 1;
    }
    assert_eq!(checked, 2);
}
fn sort_stop_recipe() -> Recipe {
    let mut v: Value = serde_json::from_str(include_str!("recipes/linear.json")).unwrap();
    v["streams"] = json!([v["streams"][0].clone()]);
    let stream = &mut v["streams"][0];
    stream["change"] = json!("sort_stop");
    stream["call"]["args"]["query_or_mutation"] = json!("query CoralSwarmIssuesRecent($first: Int!, $after: String) { issues(orderBy: updatedAt, first: $first, after: $after) { nodes { id updatedAt } pageInfo { hasNextPage endCursor } } }");
    stream["call"]["args"]["variables"].as_object_mut().unwrap().remove("since");
    recipe::load_for_test("linear", &v).unwrap()
}
fn issues_route(nodes: Value, more: bool) -> Value {
    json!({"LINEAR_RUN_QUERY_OR_MUTATION":[{"when":{},"data":{"data":{"issues":{"nodes":nodes,"pageInfo":{"hasNextPage":more,"endCursor":"next"}}},"errors":null}}]})
}
#[tokio::test]
async fn sort_stop_stops_at_the_watermark_and_requires_newest_first() {
    let r = sort_stop_recipe();
    let window = Window::from_watermark(at("2026-10-10T00:05:00Z"), at("2026-10-13T00:00:00Z"));
    let (client, task) = server(fixture_app(issues_route(
        json!([{"id":"n","updatedAt":"2026-10-12T00:00:00Z"},{"id":"m","updatedAt":"2026-10-11T00:00:00Z"},{"id":"old","updatedAt":"2026-10-09T00:00:00Z"},{"id":"older","updatedAt":"2026-10-08T00:00:00Z"}]),
        true,
    )))
    .await;
    let page = client.page(&r, "ca", "owner", "linear:workspace:w", &window, &Resume::default(), false).await.unwrap();
    assert_eq!(page.items.iter().map(|i| i.source_id.as_str()).collect::<Vec<_>>(), ["linear:workspace:w/issue/n", "linear:workspace:w/issue/m"]);
    assert_eq!(page.next, None, "paging stops at the first item older than the watermark even when the provider has more");
    task.abort();
    let (client, task) = server(fixture_app(issues_route(
        json!([{"id":"m","updatedAt":"2026-10-11T00:00:00Z"},{"id":"n","updatedAt":"2026-10-12T00:00:00Z"}]),
        false,
    )))
    .await;
    assert!(matches!(
        client.page(&r, "ca", "owner", "linear:workspace:w", &window, &Resume::default(), false).await,
        Err(Error::Contract("change sort order not honored"))
    ));
    task.abort();
}
#[tokio::test]
async fn filter_streams_fail_closed_when_the_provider_ignores_since() {
    let window = Window::from_watermark(at("2026-10-10T00:05:00Z"), at("2026-10-13T00:00:00Z"));
    let (client, task) = server(fixture_app(issues_route(
        json!([{"id":"new","updatedAt":"2026-10-12T00:00:00Z"},{"id":"ancient","updatedAt":"2025-01-01T00:00:00Z"}]),
        false,
    )))
    .await;
    assert!(matches!(
        client.page(linear(), "ca", "owner", "linear:workspace:w", &window, &Resume::default(), false).await,
        Err(Error::Contract("change filter not honored"))
    ));
    task.abort();
}
fn linear_routes(issues: Value, comments: Value) -> Value {
    json!({"LINEAR_RUN_QUERY_OR_MUTATION":[
        {"op":"CoralSwarmIssuesChanged","when":{},"data":{"data":{"issues":{"nodes":issues,"pageInfo":{"hasNextPage":false}}},"errors":null}},
        {"op":"CoralSwarmCommentsChanged","when":{},"data":{"data":{"comments":{"nodes":comments,"pageInfo":{"hasNextPage":false}}},"errors":null}},
        {"op":"CoralSwarmProjectsChanged","when":{},"data":{"data":{"projects":{"nodes":[],"pageInfo":{"hasNextPage":false}}},"errors":null}}
    ]})
}
#[tokio::test]
async fn self_check_passes_waits_or_fails_on_recipe_contract() {
    let now = at("2026-10-13T00:00:00Z");
    let check = |routes: Value| async move {
        let (client, task) = server(fixture_app(routes)).await;
        let verdict = client.self_check(linear(), "ca", "owner", "linear:workspace:w", now).await;
        task.abort();
        verdict
    };
    let fresh = json!([{"id":"i","title":"t","updatedAt":"2026-10-08T21:31:10.496Z","state":{"name":"Todo"}}]);
    assert_eq!(check(linear_routes(fresh.clone(), json!([]))).await.unwrap(), SelfCheck::Passed);
    assert_eq!(check(linear_routes(json!([]), json!([]))).await.unwrap(), SelfCheck::Inconclusive);
    let ignored_filter = json!([{"id":"i","updatedAt":"2026-01-01T00:00:00Z"}]);
    assert_eq!(
        check(linear_routes(ignored_filter, json!([]))).await.unwrap(),
        SelfCheck::Failed("change filter not honored")
    );
    let renamed_path = json!({"LINEAR_RUN_QUERY_OR_MUTATION":[{"when":{},"data":{"data":{"issueList":[]},"errors":null}}]});
    assert_eq!(check(renamed_path).await.unwrap(), SelfCheck::Failed("items"));
    let orphan_comment = json!([{"id":"c","updatedAt":"2026-10-08T00:00:00Z","issue":null}]);
    assert_eq!(
        check(linear_routes(fresh, orphan_comment)).await.unwrap(),
        SelfCheck::Failed("immutable native id")
    );
}
/// Read-only dry run of the checked-in Linear recipe against a real connected
/// account. Needs `COMPOSIO_API_KEY`, `COMPOSIO_DRY_RUN_ACCOUNT` and
/// `COMPOSIO_DRY_RUN_USER`; prints identities and paths, never content.
#[tokio::test]
#[ignore = "calls the live Composio API"]
async fn live_linear_dry_run() {
    let account = std::env::var("COMPOSIO_DRY_RUN_ACCOUNT").expect("COMPOSIO_DRY_RUN_ACCOUNT");
    let user = std::env::var("COMPOSIO_DRY_RUN_USER").expect("COMPOSIO_DRY_RUN_USER");
    let since = std::env::var("COMPOSIO_DRY_RUN_SINCE").unwrap_or_else(|_| "2026-09-01T00:00:00Z".into());
    let client = Client::configured().unwrap();
    let r = linear();
    let namespace = client.namespace(r, &account, &user).await.unwrap();
    println!("namespace {namespace}");
    let now = Utc::now();
    println!("self_check {:?}", client.self_check(r, &account, &user, &namespace, now).await.unwrap());
    for (label, window) in [
        ("since", Window { since: at(&since), now }),
        ("watermark=now", Window::from_watermark(now, now)),
    ] {
        let mut resume = Some(Resume::default());
        let mut total = 0;
        while let Some(at) = resume {
            let page = client.page(r, &account, &user, &namespace, &window, &at, false).await.unwrap();
            for item in &page.items {
                println!(
                    "{label} stream={} {} {} updated={} container={:?} content_bytes={}",
                    at.stream,
                    item.source_type,
                    item.source_id,
                    item.timestamp.to_rfc3339(),
                    item.container.as_ref().map(|c| &c.id),
                    item.content.len()
                );
            }
            total += page.items.len();
            resume = page.next;
        }
        println!("{label} window since={} items={total}", window.since.to_rfc3339());
    }
}
#[tokio::test]
async fn jira_namespace_reads_sites_nested_under_who_am_i_data() {
    use axum::{extract::Path, routing::post, Json};
    let app = axum::Router::new().route(
        "/tools/execute/{tool}",
        post(|Path(tool): Path<String>| async move {
            let data = match tool.as_str() {
                "JIRA_GET_SERVER_INFO" => json!({"baseUrl":"https://example-site.atlassian.net","deploymentType":"Cloud"}),
                "JIRA_WHO_AM_I" => json!({"data":{"account_id":null,"sites":[
                    {"id":"other-cloud-id","name":"other","url":"https://other.atlassian.net"},
                    {"id":"cloud-id-1","name":"example-site","url":"https://example-site.atlassian.net"}
                ]},"display_name":"example-site"}),
                other => panic!("unexpected tool {other}"),
            };
            Json(json!({"successful":true,"data":data}))
        }),
    );
    let (client, task) = server(app).await;
    let namespace = client.namespace(jira(), "ca_selected", "owner").await.unwrap();
    assert_eq!(namespace, "jira:site:cloud-id-1");
    task.abort();
}
/// Every `tool` named anywhere in a recipe fragment.
fn tools_in(v: &Value, out: &mut std::collections::BTreeSet<String>) {
    match v {
        Value::Object(o) => {
            for (k, x) in o {
                match (k.as_str(), x) {
                    ("tool", Value::String(t)) => {
                        out.insert(t.clone());
                    }
                    _ => tools_in(x, out),
                }
            }
        }
        Value::Array(a) => a.iter().for_each(|x| tools_in(x, out)),
        _ => {}
    }
}
/// A synthetic `acme` recipe around a namespace source and streams, so engine
/// shapes are tested without a real toolkit's recipe.
fn acme(namespace: Value, streams: Value) -> Result<Recipe, String> {
    let mut tools = std::collections::BTreeSet::new();
    tools_in(&namespace, &mut tools);
    tools_in(&streams, &mut tools);
    recipe::load_for_test(
        "acme",
        &json!({"toolkit":"acme","label":"Acme","version":"20261014_00","scopes":{"required":["read"]},
                "read_tools":tools,"namespace":{"kind":"workspace","source":namespace},"streams":streams}),
    )
}
fn acme_lookup() -> Value {
    json!({"lookup":{"call":{"tool":"ACME_GET_ACCOUNT","args":{}},"id":"/account_id"}})
}
fn source_ids(items: &[Item]) -> Vec<&str> {
    items.iter().map(|i| i.source_id.as_str()).collect()
}
#[tokio::test]
async fn integer_ids_become_their_decimal_strings() {
    let r = acme(
        acme_lookup(),
        json!([{"kind":"ticket","change":"filter",
            "call":{"tool":"ACME_SEARCH","args":{"since":"{{since}}","after":"{{cursor}}"}},
            "items":"/results","paging":{"has_more":"/more","next":"/next"},
            "fields":{"id":"/id","updated_at":"/updated_at","container":{"kind":"group","id":"/group_id"},
                      "title":"/subject","author":{"object":"/requester","id":"/id","name":"/name"}},
            "children":{"kind":"comment","call":{"tool":"ACME_LIST_COMMENTS","args":{"ticket":"{{parent}}","after":"{{cursor}}"}},
                "page":"","items":"/comments","paging":{"style":"cursor","has_more":"/more","next":"/next"},
                "fields":{"id":"/id","updated_at":"/created_at","parent":"/ticket_id","body":"/body"}}}]),
    )
    .unwrap();
    let ticket = |id: Value| json!({"id":id,"updated_at":"2026-10-12T00:00:00Z","group_id":7,"subject":"Printer","requester":{"id":991,"name":"Riley"}});
    let routes = |id: Value| {
        json!({
            "ACME_GET_ACCOUNT":[{"when":{},"data":{"account_id":4242}}],
            "ACME_SEARCH":[{"when":{},"data":{"results":[ticket(id)],"more":false}}],
            "ACME_LIST_COMMENTS":[{"when":{"/ticket":"35436"},"data":{"comments":[{"id":123456789012u64,"ticket_id":35436,"created_at":"2026-10-12T00:00:00Z","body":"hi"}],"more":false}}]
        })
    };
    let window = Window::from_watermark(at("2026-10-10T00:00:00Z"), at("2026-10-13T00:00:00Z"));
    let (client, task) = server(fixture_app(routes(json!(35436)))).await;
    let namespace = client.namespace(&r, "ca", "owner").await.unwrap();
    assert_eq!(namespace, "acme:workspace:4242");
    let items = full_pass(&r, &client, &namespace, &window).await;
    task.abort();
    assert_eq!(source_ids(&items), ["acme:workspace:4242/ticket/35436", "acme:workspace:4242/comment/35436/123456789012"]);
    assert_eq!(items[0].container.as_ref().unwrap().id, "7");
    assert_eq!(items[0].author_id.as_deref(), Some("991"));
    assert!(items[0].content.contains("Group: 7\n"));
    // Floats and booleans are not ids.
    for bad in [json!(35436.0), json!(35436.5), json!(true)] {
        let (client, task) = server(fixture_app(routes(bad.clone()))).await;
        let page = client.page(&r, "ca", "owner", &namespace, &window, &Resume::default(), false).await;
        task.abort();
        assert!(matches!(page, Err(Error::Contract("immutable native id"))), "{bad}");
    }
}
#[tokio::test]
async fn a_next_token_alone_says_there_is_another_page() {
    let r = acme(
        acme_lookup(),
        json!([{"kind":"deal","change":"filter",
            "call":{"tool":"ACME_LIST_DEALS","args":{"updated_since":"{{since}}","limit":"{{page_size}}","cursor":"{{cursor}}"}},
            "items":"/data","paging":{"next":"/additional_data/next_cursor"},
            "fields":{"id":"/id","updated_at":"/update_time"},
            "children":{"kind":"note","call":{"tool":"ACME_LIST_NOTES","args":{"deal":"{{parent}}","after":"{{cursor}}"}},
                "page":"","items":"/notes","paging":{"style":"cursor","next":"/paging/next/after"},
                "fields":{"id":"/id","updated_at":"/updated_at"}}}]),
    )
    .unwrap();
    let deal = |id: &str| json!({"id":id,"update_time":"2026-10-12T00:00:00Z"});
    let notes = |next: Value| json!({"notes":[],"paging":{"next":{"after":next}}});
    let routes = |last: Value| {
        json!({"ACME_LIST_DEALS":[
            {"when":{"/cursor":null},"data":{"data":[deal("a")],"additional_data":{"next_cursor":"c2"}}},
            {"when":{"/cursor":"c2"},"data":{"data":[deal("b")],"additional_data":{"next_cursor":3}}},
            {"when":{"/cursor":"3"},"data":{"data":[deal("c")],"additional_data":last}}
        ],"ACME_LIST_NOTES":[{"when":{},"data":notes(Value::Null)}]})
    };
    let window = Window::from_watermark(at("2026-10-10T00:00:00Z"), at("2026-10-13T00:00:00Z"));
    for last in [json!({"next_cursor":null}), json!({"next_cursor":""}), json!({})] {
        let (client, task) = server(fixture_app(routes(last.clone()))).await;
        let items = full_pass(&r, &client, "acme:workspace:w", &window).await;
        task.abort();
        assert_eq!(source_ids(&items), ["acme:workspace:w/deal/a", "acme:workspace:w/deal/b", "acme:workspace:w/deal/c"], "{last}");
    }
    // A provider handing back the token it was given would page forever.
    let (client, task) = server(fixture_app(routes(json!({"next_cursor":"3"})))).await;
    let at3 = Resume { stream: 0, cursor: Some("3".into()) };
    let repeat = client.page(&r, "ca", "owner", "acme:workspace:w", &window, &at3, false).await;
    task.abort();
    assert!(matches!(repeat, Err(Error::Contract("non-advancing item cursor"))));
    let (client, task) = server(fixture_app(routes(json!({"next_cursor":{"opaque":true}})))).await;
    let wrong_type = client.page(&r, "ca", "owner", "acme:workspace:w", &window, &at3, false).await;
    task.abort();
    assert!(matches!(wrong_type, Err(Error::Contract("continuation token"))));
    // Child tokens may not come back around either, even after another token.
    let cycle = json!({"ACME_LIST_DEALS":[{"when":{},"data":{"data":[deal("a")],"additional_data":{}}}],
        "ACME_LIST_NOTES":[
            {"when":{"/after":null},"data":notes(json!("n1"))},
            {"when":{"/after":"n1"},"data":notes(json!("n2"))},
            {"when":{"/after":"n2"},"data":notes(json!("n1"))}
        ]});
    let (client, task) = server(fixture_app(cycle)).await;
    let looped = client.page(&r, "ca", "owner", "acme:workspace:w", &window, &Resume::default(), false).await;
    task.abort();
    assert!(matches!(looped, Err(Error::Contract("non-advancing child cursor"))));
}
fn numbered(change: &str, paging: Value, args: Value) -> Recipe {
    acme(
        acme_lookup(),
        json!([{"kind":"page","change":change,"call":{"tool":"ACME_SEARCH","args":args},
            "items":"/results","paging":paging,"fields":{"id":"/id","updated_at":"/updated"}}]),
    )
    .unwrap()
}
fn rows(prefix: &str, n: usize, updated: &str) -> Value {
    Value::Array((0..n).map(|i| json!({"id":format!("{prefix}{i}"),"updated":updated})).collect())
}
async fn pass_pages(r: &Recipe, routes: Value, window: &Window) -> (Vec<Item>, Vec<Option<String>>) {
    let (client, task) = server(fixture_app(routes)).await;
    let (mut items, mut cursors) = (vec![], vec![]);
    let mut resume = Some(Resume::default());
    while let Some(at) = resume {
        cursors.push(at.cursor.clone());
        let page = client.page(r, "ca", "owner", "acme:workspace:w", window, &at, false).await.unwrap();
        items.extend(page.items);
        resume = page.next;
        assert!(cursors.len() < 20, "pass must terminate");
    }
    task.abort();
    (items, cursors)
}
#[tokio::test]
async fn offset_and_page_streams_page_by_count_and_stop() {
    let window = Window::from_watermark(at("2026-10-10T00:00:00Z"), at("2026-10-13T00:00:00Z"));
    let fresh = "2026-10-12T00:00:00Z";
    // A total: short provider pages continue until the total is read.
    let r = numbered("filter", json!({"style":"offset","total":"/totalSize"}),
        json!({"cql":"lastmodified >= now(\"-{{since_minutes}}m\")","limit":"{{page_size}}","start":"{{offset}}"}));
    let page = |ids: Value| json!({"results":ids,"totalSize":5});
    let routes = json!({"ACME_SEARCH":[
        {"when":{"/start":0,"/limit":PAGE_SIZE},"data":page(rows("a", 2, fresh))},
        {"when":{"/start":2},"data":page(rows("b", 2, fresh))},
        {"when":{"/start":4},"data":page(rows("c", 1, fresh))}
    ]});
    let (items, cursors) = pass_pages(&r, routes, &window).await;
    assert_eq!(items.len(), 5);
    assert_eq!(cursors, [None, Some("2".into()), Some("4".into())]);
    // No total: a full page means more, a short page ends the stream.
    let r = numbered("filter", json!({"style":"offset"}),
        json!({"updated_since":"{{since}}","limit":"{{page_size}}","start":"{{offset}}"}));
    let routes = json!({"ACME_SEARCH":[
        {"when":{"/start":0},"data":{"results":rows("a", PAGE_SIZE, fresh)}},
        {"when":{"/start":PAGE_SIZE},"data":{"results":rows("b", 3, fresh)}}
    ]});
    let (items, cursors) = pass_pages(&r, routes, &window).await;
    assert_eq!((items.len(), cursors.len()), (PAGE_SIZE + 3, 2));
    // Page numbers start at 1; an empty page ends the stream even with a total.
    let r = numbered("filter", json!({"style":"page","total":"/total"}),
        json!({"updated_since":"{{since}}","per_page":"{{page_size}}","page":"{{page}}"}));
    let routes = json!({"ACME_SEARCH":[
        {"when":{"/page":1},"data":{"results":rows("a", PAGE_SIZE, fresh),"total":90}},
        {"when":{"/page":2},"data":{"results":[],"total":90}}
    ]});
    let (items, cursors) = pass_pages(&r, routes, &window).await;
    assert_eq!(items.len(), PAGE_SIZE);
    assert_eq!(cursors, [None, Some("2".into())]);
    // Sort-and-stop still stops at the watermark on a numbered stream.
    let r = numbered("sort_stop", json!({"style":"page"}), json!({"sort":"-updated_at","per_page":"{{page_size}}","page":"{{page}}"}));
    let mut first = rows("n", PAGE_SIZE - 1, fresh);
    first.as_array_mut().unwrap().push(json!({"id":"old","updated":"2026-10-01T00:00:00Z"}));
    let (items, cursors) = pass_pages(&r, json!({"ACME_SEARCH":[{"when":{"/page":1},"data":{"results":first}}]}), &window).await;
    assert_eq!((items.len(), cursors.len()), (PAGE_SIZE - 1, 1));
    // A named total that is missing is a contract failure, not the end.
    let r = numbered("filter", json!({"style":"offset","total":"/totalSize"}),
        json!({"updated_since":"{{since}}","limit":"{{page_size}}","start":"{{offset}}"}));
    let (client, task) = server(fixture_app(json!({"ACME_SEARCH":[{"when":{},"data":{"results":rows("a", 1, fresh)}}]}))).await;
    let missing = client.page(&r, "ca", "owner", "acme:workspace:w", &window, &Resume::default(), false).await;
    task.abort();
    assert!(matches!(missing, Err(Error::Contract("item total"))));
}
#[tokio::test]
async fn self_check_and_paging_judge_epoch_update_times() {
    let now = at("2026-10-13T00:00:00Z");
    // The probe window starts 30 days back: 2026-09-13T00:00:00Z.
    let probe_ms = "1789257600000";
    let filter = acme(
        acme_lookup(),
        json!([{"kind":"deal","change":"filter",
            "call":{"tool":"ACME_SEARCH","args":{"filterGroups":[{"filters":[{"propertyName":"modified","operator":"GTE","value":"{{since_epoch_ms_text}}"}]}],"after":"{{cursor}}"}},
            "items":"/results","paging":{"next":"/paging/next/after"},"fields":{"id":"/id","updated_at":"/modified_ms"}}]),
    )
    .unwrap();
    let check = |r: Recipe, routes: Value| async move {
        let (client, task) = server(fixture_app(routes)).await;
        let verdict = client.self_check(&r, "ca", "owner", "acme:workspace:w", now).await.unwrap();
        task.abort();
        verdict
    };
    let served = |rows: Value| json!({"ACME_SEARCH":[{"when":{"/filterGroups/0/filters/0/value":probe_ms},"data":{"results":rows}}]});
    assert_eq!(check(filter.clone(), served(json!([{"id":"d","modified_ms":"1791763200000"}]))).await, SelfCheck::Passed);
    assert_eq!(
        check(filter, served(json!([{"id":"d","modified_ms":1700000000000u64}]))).await,
        SelfCheck::Failed("change filter not honored"),
        "an epoch-ms item from 2023 means the provider ignored the filter"
    );
    let sorted = acme(
        acme_lookup(),
        json!([{"kind":"conversation","change":"sort_stop",
            "call":{"tool":"ACME_SEARCH","args":{"sort_order":"descending","starting_after":"{{cursor}}"}},
            "items":"/conversations","paging":{"next":"/pages/next/starting_after"},"fields":{"id":"/id","updated_at":"/updated_at"}}]),
    )
    .unwrap();
    let conversations = |a: u64, b: u64| json!({"ACME_SEARCH":[{"when":{},"data":{"conversations":[{"id":"a","updated_at":a},{"id":"b","updated_at":b}]}}]});
    assert_eq!(check(sorted.clone(), conversations(1791763200, 1791676800)).await, SelfCheck::Passed);
    assert_eq!(
        check(sorted.clone(), conversations(1791676800, 1791763200)).await,
        SelfCheck::Failed("change sort order not honored")
    );
    // Sort-and-stop stops at the first epoch time older than the watermark.
    let window = Window::from_watermark(at("2026-10-11T00:05:00Z"), now);
    let (client, task) = server(fixture_app(conversations(1791763200, 1791590400))).await;
    let page = client.page(&sorted, "ca", "owner", "acme:workspace:w", &window, &Resume::default(), false).await.unwrap();
    task.abort();
    assert_eq!(source_ids(&page.items), ["acme:workspace:w/conversation/a"]);
    assert_eq!(page.next, None);
}
#[test]
fn epoch_timestamps_pick_the_unit_by_magnitude() {
    let ts = |v: Value| parse_timestamp(&v);
    let day = at("2025-10-13T00:00:00Z");
    for v in [json!(1760313600), json!("1760313600"), json!(1760313600000u64), json!("1760313600000")] {
        assert_eq!(ts(v.clone()).unwrap(), day, "{v}");
    }
    assert_eq!(ts(json!(1760313600123u64)).unwrap(), at("2025-10-13T00:00:00.123Z"));
    assert_eq!(ts(json!(99_999_999_999u64)).unwrap(), at("5138-11-16T09:46:39Z"), "largest seconds value");
    assert_eq!(ts(json!(100_000_000_000u64)).unwrap(), at("1973-03-03T09:46:40Z"), "smallest milliseconds value");
    for bad in [json!(1760313600.5), json!(-1), json!("+1760313600"), json!("1760313600.5"), json!(""), json!("99999999999999999999999"), json!(u64::MAX)] {
        assert!(matches!(ts(bad.clone()), Err(Error::Contract("provider timestamp"))), "{bad}");
    }
    for absent in [Value::Null, json!(true), json!({"ms":1})] {
        assert!(matches!(ts(absent.clone()), Err(Error::Contract("timestamp"))), "{absent}");
    }
    assert_eq!(ts(json!("2021-01-18T23:45:00.000+0000")).unwrap(), at("2021-01-18T23:45:00Z"));
}
#[tokio::test]
async fn namespace_can_come_from_the_connected_account() {
    use axum::{extract::Path, routing::get, Json};
    let account = |toolkit: &str, params: Value| {
        // The shape of a real v3.1 connected account (outputs/composio-live-capture-evidence),
        // tokens replaced; only the allowlisted field may be read.
        json!({"id":"ca_z","toolkit":{"slug":toolkit},"status":"ACTIVE","auth_config":{"id":"ac_x"},
               "data":{"access_token":"not-a-namespace","scope":"read"},
               "params":params,"state":{"val":{"subdomain":"not-this"}}})
    };
    let resolve = |r: Recipe, served: Value, id: &'static str| async move {
        let app = axum::Router::new().route(
            "/connected_accounts/{id}",
            get(move |Path(got): Path<String>| {
                let served = served.clone();
                async move {
                    assert_eq!(got, "ca_z");
                    Json(served)
                }
            }),
        );
        let (client, task) = server(app).await;
        let ns = client.namespace(&r, id, "owner").await;
        task.abort();
        ns
    };
    let streams = json!([{"kind":"ticket","change":"filter","call":{"tool":"ACME_SEARCH","args":{"since":"{{since}}","after":"{{cursor}}"}},
        "items":"/results","paging":{"next":"/next"},"fields":{"id":"/id","updated_at":"/updated_at"}}]);
    let by_host = acme(json!({"connection":{"id":["/params/base_url","/data/base_url"],"host":true}}), streams.clone()).unwrap();
    let by_subdomain = acme(json!({"connection":{"id":"/params/subdomain"}}), streams).unwrap();
    let host = |url: &str| account("acme", json!({"base_url":url,"access_token":"not-a-namespace"}));
    assert_eq!(resolve(by_host.clone(), host("https://Acme.ZenDesk.com/api/v2"), "ca_z").await.unwrap(), "acme:workspace:acme.zendesk.com");
    assert_eq!(
        resolve(by_subdomain.clone(), account("acme", json!({"subdomain":"Acme"})), "ca_z").await.unwrap(),
        "acme:workspace:acme"
    );
    for (r, served) in [
        (by_host.clone(), host("http://acme.zendesk.com")),
        (by_host.clone(), host("https://10.0.0.1/api")),
        (by_host.clone(), host("not a url")),
        (by_host.clone(), account("acme", json!({}))),
        (by_subdomain.clone(), account("acme", json!({"subdomain":"acme/../other"}))),
        (by_subdomain.clone(), account("acme", json!({"subdomain":""}))),
        (by_subdomain.clone(), account("acme", json!({"subdomain":7}))),
    ] {
        let got = resolve(r, served.clone(), "ca_z").await;
        assert!(matches!(got, Err(Error::Contract("connection namespace"))), "{served}: {got:?}");
    }
    assert!(matches!(
        resolve(by_host.clone(), account("zendesk", json!({"base_url":"https://acme.zendesk.com"})), "ca_z").await,
        Err(Error::Contract("connected account toolkit"))
    ));
    assert!(matches!(
        resolve(by_host, host("https://acme.zendesk.com"), "../ca_z").await,
        Err(Error::Contract("connected account id"))
    ));
}
#[tokio::test]
async fn single_page_children_are_read_in_one_call() {
    use axum::{extract::Path, routing::post, Json};
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    let r = acme(
        acme_lookup(),
        json!([{"kind":"conversation","change":"filter",
            "call":{"tool":"ACME_SEARCH","args":{"since":"{{since}}","after":"{{cursor}}"}},
            "items":"/conversations","paging":{"has_more":"/more","next":"/next"},
            "fields":{"id":"/id","updated_at":"/updated_at","title":"/title"},
            "children":{"kind":"part","call":{"tool":"ACME_GET_CONVERSATION","args":{"conversation_id":"{{parent}}","display_as":"plaintext"}},
                "page":"/conversation_parts","items":"/conversation_parts","paging":{"style":"single"},
                "fields":{"id":"/id","updated_at":"/updated_at","body":"/body","author":{"object":"/author","id":"/id","name":"/name"}}}}]),
    )
    .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let app = axum::Router::new().route(
        "/tools/execute/{tool}",
        post({
            let calls = calls.clone();
            move |Path(tool): Path<String>, Json(body): Json<Value>| {
                let calls = calls.clone();
                async move {
                    let args = &body["arguments"];
                    let data = match tool.as_str() {
                        "ACME_SEARCH" => json!({"conversations":[
                            {"id":"c1","updated_at":"2026-10-12T00:00:00Z","title":"Refund"},
                            {"id":"c2","updated_at":"2026-10-12T01:00:00Z","title":"Login"}],"more":false}),
                        "ACME_GET_CONVERSATION" => {
                            calls.fetch_add(1, Ordering::SeqCst);
                            assert!(args.get("starting_after").is_none() && args.get("page").is_none());
                            let c = args["conversation_id"].as_str().unwrap();
                            json!({"id":c,"conversation_parts":{"total_count":2,"conversation_parts":[
                                {"id":format!("{c}-p1"),"updated_at":"2026-10-12T02:00:00Z","body":"First","author":{"id":"a1","name":"Ana"}},
                                {"id":format!("{c}-p2"),"updated_at":"2026-10-12T03:00:00Z","body":"Second","author":{"id":"a2","name":"Bo"}}]}})
                        }
                        other => panic!("unexpected tool {other}"),
                    };
                    Json(json!({"successful":true,"data":data}))
                }
            }
        }),
    );
    let (client, task) = server(app).await;
    let window = Window::from_watermark(at("2026-10-10T00:00:00Z"), at("2026-10-13T00:00:00Z"));
    let items = full_pass(&r, &client, "acme:workspace:w", &window).await;
    task.abort();
    assert_eq!(calls.load(Ordering::SeqCst), 2, "one call per changed conversation");
    assert_eq!(
        source_ids(&items),
        [
            "acme:workspace:w/conversation/c1",
            "acme:workspace:w/part/c1/c1-p1",
            "acme:workspace:w/part/c1/c1-p2",
            "acme:workspace:w/conversation/c2",
            "acme:workspace:w/part/c2/c2-p1",
            "acme:workspace:w/part/c2/c2-p2"
        ]
    );
    assert_eq!(items[1].author_name.as_deref(), Some("Ana"));
}
/// The reviewed toolkits, registered (`recipes/`) or still drafts
/// (`docs/connectors/composio-toolkit-review/`), load through the validator and
/// run a mock-broker pass and self-check against responses shaped by each
/// tool's Composio output schema (`fixtures/review_drafts.json`).
#[tokio::test]
async fn review_drafts_load_and_poll_against_schema_shaped_fixtures() {
    use axum::{extract::Path, routing::get, Json};
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../docs/connectors/composio-toolkit-review");
    let fixtures: Value = serde_json::from_str(include_str!("fixtures/review_drafts.json")).unwrap();
    let mut drafts = std::collections::BTreeSet::new();
    for (name, f) in fixtures["scenarios"].as_object().unwrap() {
        let draft = f["draft"].as_str().unwrap();
        drafts.insert(draft);
        let draft_file = dir.join(format!("{draft}.json"));
        let r = match recipe::get(draft) {
            Some(registered) => {
                assert!(registered.toolkit == draft && !draft_file.exists(), "{draft}: a registered recipe has no second draft copy");
                registered.clone()
            }
            None => {
                let text = std::fs::read_to_string(&draft_file).unwrap();
                recipe::load_for_test(draft, &serde_json::from_str(&text).unwrap())
                    .unwrap_or_else(|e| panic!("{name}: draft does not load: {e}"))
            }
        };
        if let Some(check) = f.get("rendered") {
            let args = r.streams[check["stream"].as_u64().unwrap() as usize]
                .call
                .args
                .render(&Bindings { since: Some(at(check["since"].as_str().unwrap())), page_size: Some(PAGE_SIZE), ..Default::default() })
                .unwrap();
            assert_eq!(args.pointer(check["pointer"].as_str().unwrap()), Some(&check["value"]), "{name} rendered slot");
        }
        let account = f.get("connected_account").cloned().unwrap_or(Value::Null);
        let app = fixture_app(f["routes"].clone()).route(
            "/connected_accounts/{id}",
            get(move |Path(_): Path<String>| {
                let account = account.clone();
                async move { Json(account) }
            }),
        );
        let (client, task) = server(app).await;
        let namespace = match client.namespace(&r, "ca_fixture", "owner").await {
            Ok(ns) => {
                assert_eq!(ns, f["namespace"], "{name} namespace");
                ns
            }
            Err(Error::Contract(why)) if f["namespace_error"] == why => f["poll_namespace"].as_str().unwrap().to_owned(),
            Err(e) => panic!("{name}: namespace {e:?}"),
        };
        let window = Window::from_watermark(at(f["watermark"].as_str().unwrap()), at(f["now"].as_str().unwrap()));
        let mut items = vec![];
        let mut resume = Some(Resume::default());
        let mut failed = None;
        while let Some(at) = resume.take() {
            match client.page(&r, "ca_fixture", "owner", &namespace, &window, &at, false).await {
                Ok(page) => {
                    items.extend(page.items);
                    resume = page.next;
                }
                Err(Error::Contract(why)) => failed = Some(why),
                Err(e) => panic!("{name}: {e:?}"),
            }
            assert!(items.len() < 100, "{name}: pass must terminate");
        }
        match f.get("error") {
            Some(want) => assert_eq!(failed.map(Value::from).as_ref(), Some(want), "{name} contract failure"),
            None => {
                assert_eq!(failed, None, "{name}");
                assert_eq!(json!(source_ids(&items)), f["items"], "{name} items");
                assert!(items.iter().all(|i| !i.content.is_empty() && i.source_type.starts_with(draft)));
                if f["plain_text"] == true {
                    assert!(items.iter().all(|i| !i.content.contains('<')), "{name}: HTML bodies render as text");
                }
            }
        }
        let verdict = client.self_check(&r, "ca_fixture", "owner", &namespace, at(f["now"].as_str().unwrap())).await.unwrap();
        let verdict = match verdict {
            SelfCheck::Passed => "passed".to_owned(),
            SelfCheck::Inconclusive => "inconclusive".to_owned(),
            SelfCheck::Failed(why) => format!("failed:{why}"),
        };
        assert_eq!(json!(verdict), f["self_check"], "{name} self-check");
        task.abort();
    }
    assert_eq!(drafts.into_iter().collect::<Vec<_>>(), ["confluence", "dart", "hubspot", "intercom", "pipedrive", "zendesk"]);
    let registered: Vec<&str> = recipe::all().map(|r| r.toolkit.as_str()).collect();
    assert_eq!(registered, ["confluence", "dart", "hubspot", "jira", "linear", "zendesk"]);
}
#[tokio::test]
async fn html_bodies_render_as_plain_text() {
    let with_body = |body: Value| {
        acme(
            acme_lookup(),
            json!([{"kind":"page","change":"filter",
                "call":{"tool":"ACME_SEARCH","args":{"since":"{{since}}","after":"{{cursor}}"}},
                "items":"/results","paging":{"next":"/next"},
                "fields":{"id":"/id","updated_at":"/updated_at","title":"/title","body":body}}]),
        )
    };
    let storage = "<h1>Plan</h1><p>Ship <strong>first</strong> &amp; measure&nbsp;&mdash; then</p><ac:structured-macro ac:name=\"info\"><ac:rich-text-body><p>Pilot</p></ac:rich-text-body></ac:structured-macro><ul><li>one</li><li>two</li></ul>";
    let routes = json!({"ACME_SEARCH":[{"when":{},"data":{"results":[
        {"id":"p1","updated_at":"2026-10-12T00:00:00Z","title":"Plan","html":storage},
        {"id":"p2","updated_at":"2026-10-12T00:00:00Z","title":"Doc","html":{"type":"doc","content":[{"type":"paragraph","content":[{"type":"text","text":"adf"}]}]}},
        {"id":"p3","updated_at":"2026-10-12T00:00:00Z","title":"Empty","html":null}
    ]}}]});
    let window = Window::from_watermark(at("2026-10-10T00:00:00Z"), at("2026-10-13T00:00:00Z"));
    let pass = |r: Recipe| {
        let routes = routes.clone();
        async move {
            let (client, task) = server(fixture_app(routes)).await;
            let items = full_pass(&r, &client, "acme:workspace:w", &window).await;
            task.abort();
            items.into_iter().map(|i| i.content).collect::<Vec<_>>()
        }
    };
    let html = pass(with_body(json!({"path":"/html","format":"html"})).unwrap()).await;
    let head = "Service: acme\nReference: \nState: \n";
    assert_eq!(html[0], format!("Plan\n{head}Plan\nShip first & measure \u{2014} then\nPilot\none\ntwo"));
    assert_eq!(html[1], format!("Doc\n{head}adf\n"), "a non-string body is read as before");
    assert_eq!(html[2], format!("Empty\n{head}"));
    // A bare path is still text: markup is kept byte for byte.
    let raw = pass(with_body(json!("/html")).unwrap()).await;
    assert_eq!(raw[0], format!("Plan\n{head}{storage}"));
    assert_eq!(pass(with_body(json!({"path":"/html","format":"text"})).unwrap()).await, raw);
    for bad in [json!({"path":"/html","format":"markdown"}), json!({"path":"/html"}), json!({"path":"/html","format":"html","strip":true}), json!({"path":"html","format":"html"})] {
        assert!(with_body(bad.clone()).is_err(), "{bad}");
    }
}
#[tokio::test]
async fn match_namespace_can_compare_url_origins() {
    let recipe = |compare: Option<&str>| {
        let mut source = json!({"match":{
            "anchor":{"call":{"tool":"ACME_SEARCH_SPACES","args":{"limit":1}},"value":"/_links/base"},
            "candidates":{"call":{"tool":"ACME_WHO_AM_I","args":{}},"list":"/sites"},
            "key":"/url","id":"/id"}});
        if let Some(c) = compare {
            source["match"]["compare"] = json!(c);
        }
        acme(
            source,
            json!([{"kind":"page","change":"filter","call":{"tool":"ACME_SEARCH","args":{"since":"{{since}}","after":"{{cursor}}"}},
                "items":"/results","paging":{"next":"/next"},"fields":{"id":"/id","updated_at":"/updated_at"}}]),
        )
    };
    let resolve = |r: Recipe, base: &str, sites: Value| {
        let routes = json!({"ACME_SEARCH_SPACES":[{"when":{},"data":{"_links":{"base":base}}}],
                            "ACME_WHO_AM_I":[{"when":{},"data":{"sites":sites}}]});
        async move {
            let (client, task) = server(fixture_app(routes)).await;
            let ns = client.namespace(&r, "ca", "owner").await;
            task.abort();
            ns
        }
    };
    let sites = json!([{"id":"other-cloud","url":"https://other.atlassian.net"},{"id":"acme-cloud","url":"https://acme.atlassian.net"}]);
    let origin = recipe(Some("origin")).unwrap();
    for base in ["https://acme.atlassian.net/wiki", "https://ACME.atlassian.net/wiki/", "https://acme.atlassian.net:443/wiki", "https://acme.atlassian.net"] {
        assert_eq!(resolve(origin.clone(), base, sites.clone()).await.unwrap(), "acme:workspace:acme-cloud", "{base}");
    }
    for base in ["http://acme.atlassian.net/wiki", "https://acme.atlassian.net:8443/wiki", "https://acme.atlassian.net.evil.example/wiki", "acme.atlassian.net/wiki", "https://10.0.0.1/wiki"] {
        assert!(matches!(resolve(origin.clone(), base, sites.clone()).await, Err(Error::Contract("immutable workspace id"))), "{base}");
    }
    let http_site = json!([{"id":"acme-cloud","url":"http://acme.atlassian.net"}]);
    assert!(matches!(resolve(origin, "https://acme.atlassian.net/wiki", http_site).await, Err(Error::Contract("immutable workspace id"))));
    // The default stays an exact comparison, as Jira's recipe relies on.
    for exact in [recipe(None).unwrap(), recipe(Some("exact")).unwrap()] {
        assert!(matches!(resolve(exact.clone(), "https://acme.atlassian.net/wiki", sites.clone()).await, Err(Error::Contract("immutable workspace id"))));
        assert_eq!(resolve(exact, "https://acme.atlassian.net", sites.clone()).await.unwrap(), "acme:workspace:acme-cloud");
    }
    assert!(recipe(Some("prefix")).is_err());
}
/// The recipes' capture hints reach the item: a title from the provider's own
/// fields (a comment names the issue or ticket it belongs to) and the
/// chunking the recipe declares, while the retained content is unchanged.
#[tokio::test]
async fn capture_hints_title_and_chunk_linear_and_zendesk_items() {
    let linear_routes = json!({"LINEAR_RUN_QUERY_OR_MUTATION":[
        {"op":"CoralSwarmIssuesChanged","when":{},"data":{"data":{"issues":{"nodes":[
            {"id":"i-1","identifier":"COR-7","title":"Ship search","description":"Decision: ship it.","createdAt":"2026-10-12T00:00:00Z","updatedAt":"2026-10-12T00:00:00Z","state":{"name":"Todo"},"creator":{"id":"u1","name":"Morgan"},"project":{"id":"p-1","name":"Search"}},
            {"id":"i-2","identifier":"COR-8","title":"   ","description":"Untitled body.","createdAt":"2026-10-12T00:00:00Z","updatedAt":"2026-10-12T00:00:00Z","state":{"name":"Todo"},"creator":{"id":"u1","name":"Morgan"},"project":null}
        ],"pageInfo":{"hasNextPage":false,"endCursor":null}}},"errors":null}},
        {"op":"CoralSwarmCommentsChanged","when":{},"data":{"data":{"comments":{"nodes":[
            {"id":"c-1","body":"Agreed, ship it on Friday.","createdAt":"2026-10-12T00:00:00Z","updatedAt":"2026-10-12T00:00:00Z","user":{"id":"u2","name":"Casey"},"issue":{"id":"i-1","title":"Ship search","identifier":"COR-7","project":{"id":"p-1","name":"Search"}}},
            {"id":"c-2","body":"Old shape without the issue title.","createdAt":"2026-10-12T00:00:00Z","updatedAt":"2026-10-12T00:00:00Z","user":{"id":"u2","name":"Casey"},"issue":{"id":"i-1","project":null}}
        ],"pageInfo":{"hasNextPage":false,"endCursor":null}}},"errors":null}}
    ]});
    let r = linear();
    let (client, task) = server(fixture_app(linear_routes)).await;
    let window = Window::from_watermark(at("2026-10-10T00:05:00Z"), at("2026-10-13T00:00:00Z"));
    let issues = client.page(r, "ca", "owner", "linear:workspace:w", &window, &Resume::default(), false).await.unwrap();
    let comments = client
        .page(r, "ca", "owner", "linear:workspace:w", &window, &Resume { stream: 1, cursor: None }, false)
        .await
        .unwrap();
    task.abort();
    let titled: Vec<(&str, Option<&str>)> = issues.items.iter().chain(&comments.items)
        .map(|i| (i.source_type.as_str(), i.title.as_deref()))
        .collect();
    assert_eq!(titled, [
        ("linear_issue", Some("COR-7: Ship search")),
        // A blank title with a reference: neither template resolves, so the
        // funnel derives the title from the content.
        ("linear_issue", None),
        ("linear_comment", Some("Comment on COR-7: Ship search")),
        ("linear_comment", Some("Comment: Old shape without the issue title")),
    ]);
    assert!(issues.items[0].content.starts_with("Ship search\nService: linear\n"), "content keeps the retained render");
    assert!(comments.items[0].content.starts_with("comment\nService: linear\n"));
    for item in issues.items.iter().chain(&comments.items) {
        assert_eq!(item.chunk, ChunkHint::Document);
        let hints = item.capture_hints();
        assert!(matches!(hints.chunk, Some(coralswarm_core::chunker::ChunkStrategy::Generic)));
        assert_eq!(hints.title, item.title);
    }

    let zendesk = recipe::get("zendesk").unwrap();
    let zendesk_routes = json!({
        "ZENDESK_GET_SEARCH_EXPORT":[{"when":{},"data":{"results":[
            {"id":42,"subject":"Login fails on SSO","description":"Customer cannot log in.","status":"open","created_at":"2026-10-12T00:00:00Z","updated_at":"2026-10-12T00:00:00Z"}
        ],"meta":{"has_more":false,"after_cursor":null}}}],
        "ZENDESK_GET_TICKETS_CONVERSATION_LOG":[{"when":{},"data":{"events":[
            {"id":9001,"created_at":"2026-10-12T00:00:00Z","content":{"body":"<p>We rolled back the change.</p>"},"author":{"display_name":"Casey"}}
        ],"meta":{"has_more":false,"after_cursor":null}}}]
    });
    let (client, task) = server(fixture_app(zendesk_routes)).await;
    let page = client.page(zendesk, "ca", "owner", "zendesk:host:acme.zendesk.com", &window, &Resume::default(), false).await.unwrap();
    task.abort();
    let titled: Vec<(&str, Option<&str>)> = page.items.iter().map(|i| (i.source_type.as_str(), i.title.as_deref())).collect();
    assert_eq!(titled, [("zendesk_ticket", Some("Login fails on SSO")), ("zendesk_comment", Some("Comment on Login fails on SSO"))]);
    assert!(page.items.iter().all(|i| i.chunk == ChunkHint::Document));
}
/// A Linear comment's anchor is its issue: the recorded change-query payload
/// carries `issue { identifier title }`, and the item's capture hints name
/// both (title and issue key), so the source version's metadata can. The
/// content keeps the retained render (PR #1239 golden), and an issue names
/// its own key.
#[tokio::test]
async fn linear_comment_anchor_names_its_issue_key_and_title() {
    let routes: Value = serde_json::from_str(include_str!("fixtures/linear_comment_anchor.json")).unwrap();
    let r = linear();
    let (client, task) = server(fixture_app(routes)).await;
    let window = Window::from_watermark(at("2026-10-18T00:00:00Z"), at("2026-10-20T00:00:00Z"));
    let items = full_pass(r, &client, "linear:workspace:w", &window).await;
    task.abort();
    let anchors: Vec<(&str, Option<String>, Option<String>)> = items
        .iter()
        .map(|i| {
            let h = i.capture_hints();
            (i.source_type.as_str(), h.identifier, h.title)
        })
        .collect();
    assert_eq!(anchors, [
        ("linear_issue", Some("COR-65".into()), Some("COR-65: Knowledge pipeline quality eval".into())),
        ("linear_comment", Some("COR-65".into()), Some("Comment on COR-65: Knowledge pipeline quality eval".into())),
        // An orphaned-shape comment (no issue key in the payload) names nothing.
        ("linear_comment", None, Some("Comment: Second pass looks better".into())),
    ]);
    let comment = &items[1];
    assert!(comment.content.starts_with("comment\nService: linear\n"), "content keeps the retained render");
    assert!(comment.content.contains("\nReference: \n"), "the anchor travels as metadata, not content");
    let mut metadata = json!({"author_name": "Casey", "title": null});
    comment.capture_hints().record_anchor(&mut metadata);
    assert_eq!(metadata["identifier"], "COR-65");
    assert_eq!(metadata["title"], "Comment on COR-65: Knowledge pipeline quality eval");
    assert!(metadata.get("url").is_none());
}

/// End to end on real Postgres: a recorded Linear comment captured through
/// the Composio route records its issue key and title in
/// `source_versions.source_metadata`, and the v3 extraction input names
/// them in its anchor (`identifier`, `title`).
#[tokio::test]
async fn linear_comment_capture_records_its_issue_anchor_pg() {
    use sqlx::{Connection, Executor};
    let Some(base) = std::env::var("ORGT_TEST_BASE_URL").ok() else {
        assert!(std::env::var("REQUIRE_PG_SUITE").is_err(), "REQUIRE_PG_SUITE set without ORGT_TEST_BASE_URL");
        eprintln!("skipping linear_comment_capture_records_its_issue_anchor_pg: set ORGT_TEST_BASE_URL");
        return;
    };
    // SAFETY: every pg test in this crate sets the same constant KEK.
    unsafe { std::env::set_var("MASTER_KEK_HEX", "cc".repeat(32)) };
    let db = "orgt_linear_comment_anchor";
    let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
    let (prefix, rest) = base.split_at(base.rfind('/').unwrap());
    let url = format!("{prefix}/{db}{}", rest.find('?').map(|i| &rest[i..]).unwrap_or(""));
    let mut admin = sqlx::PgConnection::connect(&base).await.unwrap();
    admin.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.unwrap();
    admin.execute(format!("CREATE DATABASE {db}").as_str()).await.unwrap();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();
    sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();
    let user: uuid::Uuid = sqlx::query_scalar("INSERT INTO users(id, email) VALUES (gen_random_uuid(), $1) RETURNING id")
        .bind(format!("linear-{}@test.local", uuid::Uuid::new_v4())).fetch_one(&pool).await.unwrap();
    let kek = coralswarm_core::kek::load_kek().unwrap();
    let dek = coralswarm_core::kek::wrap_dek(&kek, &coralswarm_core::kek::generate_dek()).unwrap();
    let account: uuid::Uuid = sqlx::query_scalar("INSERT INTO accounts(owner_user_id, dek_wrapped) VALUES ($1, $2) RETURNING id")
        .bind(user).bind(&dek).fetch_one(&pool).await.unwrap();
    sqlx::query("INSERT INTO account_members(account_id, user_id, role) VALUES ($1, $2, 'owner')")
        .bind(account).bind(user).execute(&pool).await.unwrap();
    let graph: uuid::Uuid = sqlx::query_scalar("INSERT INTO graphs (account_id, name, kind, is_primary) VALUES ($1, 'Primary', 'personal', true) RETURNING id")
        .bind(account).fetch_one(&pool).await.unwrap();

    let routes: Value = serde_json::from_str(include_str!("fixtures/linear_comment_anchor.json")).unwrap();
    let (client, task) = server(fixture_app(routes)).await;
    let window = Window::from_watermark(at("2026-10-18T00:00:00Z"), at("2026-10-20T00:00:00Z"));
    let items = full_pass(linear(), &client, "linear:workspace:w", &window).await;
    task.abort();
    let comment = items.into_iter().find(|i| i.source_type == "linear_comment").unwrap();

    let state = std::sync::Arc::new(crate::test_app_state_owned_with_db_and_store(pool.clone(), None));
    let store: std::sync::Arc<dyn crate::store::Store> = std::sync::Arc::new(crate::store::PostgresStore::new(pool.clone()));
    let tenant = crate::tenancy::extractor::Tenant {
        store: Some(store),
        pool: Some(pool.clone()),
        decision: None,
        class: crate::route_class::RouteClass::Other,
    };
    let user_ctx = crate::middleware::auth::UserContext { user_id: user, account_id: account, role: "owner".into(), device_id: String::new() };
    let now = Utc::now();
    let capture = crate::routes::composio::Capture {
        id: uuid::Uuid::new_v4(), graph_id: graph, connection_id: uuid::Uuid::new_v4(), service: "linear".into(),
        provider_namespace: "linear:workspace:w".into(), cursor: None, state: "active".into(), captured_items: 0,
        watermark: now, pass_started_at: None, next_poll_at: now, last_change_at: None, last_capture_at: None,
        retry_at: None, error_code: None, created_at: now,
    };
    assert!(crate::routes::composio::ingest_item(&state, &tenant, &user_ctx, &capture, comment).await, "the comment is admitted");

    let v: crate::source_evidence::SourceVersion = sqlx::query_as("SELECT * FROM source_versions WHERE graph_id = $1 AND source_type = 'linear_comment'")
        .bind(graph).fetch_one(&pool).await.unwrap();
    let m = &v.source_metadata;
    assert_eq!(m["identifier"], "COR-65");
    assert_eq!(m["title"], "Comment on COR-65: Knowledge pipeline quality eval");
    assert_eq!(m["author_name"], "Casey");
    assert!(v.source_id.ends_with("/comment/9f0c4c1e-6c1f-4d43-9d7a-1b2f6f1c0a65/3b7e2a90-51d4-4c1b-8f0e-2d9a6c4e7f11"), "identity unchanged: {}", v.source_id);
    let text = v.raw_inline.as_deref().unwrap();
    let v3 = crate::workers::source_atoms::input_for_version("source-atoms-v3", text, &v).unwrap();
    for needle in [r#""identifier":"COR-65""#, r#""title":"Comment on COR-65: Knowledge pipeline quality eval""#, r#""author":"Casey""#] {
        assert!(v3.contains(needle), "v3 input lacks {needle}: {v3}");
    }
    pool.close().await;
    admin.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
}

#[test]
fn broker_base_defaults_to_composio_and_honors_an_override() {
    assert_eq!(super::resolve_base(None), super::BASE);
    assert_eq!(super::resolve_base(Some("")), super::BASE);
    assert_eq!(super::resolve_base(Some("  \n")), super::BASE);
    assert_eq!(super::resolve_base(Some(" http://127.0.0.1:18090/composio/api/v3.1/ ")), "http://127.0.0.1:18090/composio/api/v3.1");
}

// Without the `e2e` feature the env var is never read: the broker key cannot be
// redirected in a real build.
#[cfg(not(feature = "e2e"))]
#[test]
fn configured_base_ignores_the_env_outside_e2e_builds() {
    assert_eq!(super::configured_base(), super::BASE);
}
