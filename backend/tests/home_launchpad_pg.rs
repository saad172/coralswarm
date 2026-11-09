//! Route-level tests for the Home launchpad reads (`GET /v1/home/continue`,
//! `/v1/home/whats-new`, `/v1/home/attention`) against the REAL production
//! router (`test_support_app`) served off a `coralswarm_app` login pool, the
//! harness `home_metrics_pg.rs` uses: Clerk-shaped JWTs against a local JWKS
//! listener, `begin_member_scoped` as in production. Plain page reads, so no
//! read-model worker is involved: every call answers from the database.
//!
//! Covers the rules of `docs/implementation/home-launchpad-api.md`: the flag
//! and parameter gates; Continue's 3-per-kind cap, 6-row limit and
//! repo+branch session grouping, viewer-only items, session ownership by the
//! longest real session id and by the first coral in time, the viewer's own
//! sessions and meetings behind a busy shared ocean, meeting sessions listed
//! as agent rows, workspace rows dated by readable content or membership;
//! What's new's 7-day window, kind order (decisions and threads before
//! entities), hidden-title and invisible-ocean exclusion, and no starvation
//! behind hidden reefs; `has_content` false then true (also past unreadable
//! newest corals) and its per-ocean scan bound and total order on tied
//! `created_at`, `has_source` / `has_mcp` / `has_asked`, and the first broken
//! connector only, by name; the viewer's own corals in an ocean they cannot
//! see never using up Continue's bound; the owner walk passing older corals of
//! sibling sessions; and every read on a one-connection pool (one pooled
//! connection held at a time).
//!
//!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test home_launchpad_pg

mod support;

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request as HttpRequest, StatusCode};
use axum::Router;
use chrono::{DateTime, Duration, Utc};
use serde_json::{json, Value};
use sqlx::PgPool;
use tokio::sync::Mutex;
use tower::ServiceExt;
use uuid::Uuid;

use coralswarm_backend::clerk::test_support::{mint, test_jwks, ISSUER};
use coralswarm_backend::store::PostgresStore;
use coralswarm_backend::AppState;
use support::rls_probe::RlsProbe;

static ENV_LOCK: Mutex<()> = Mutex::const_new(());

async fn spawn_local_jwks() -> String {
    let jwks_body = test_jwks();
    let jwks_app = axum::Router::new().route(
        "/.well-known/jwks.json",
        axum::routing::get(move || {
            let body = jwks_body.clone();
            async move { ([("content-type", "application/json")], body) }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, jwks_app).await.ok();
    });
    format!("http://{addr}/.well-known/jwks.json")
}

struct EnvGuard;
impl EnvGuard {
    fn set(jwks_url: &str, flag: Option<&str>) -> Self {
        std::env::set_var("CLERK_ISSUER", ISSUER);
        std::env::set_var("CLERK_JWKS_URL", jwks_url);
        match flag {
            Some(v) => std::env::set_var("HOME_DASHBOARD_ENABLED", v),
            None => std::env::remove_var("HOME_DASHBOARD_ENABLED"),
        }
        EnvGuard
    }
}
impl Drop for EnvGuard {
    fn drop(&mut self) {
        std::env::remove_var("CLERK_ISSUER");
        std::env::remove_var("CLERK_JWKS_URL");
        std::env::remove_var("HOME_DASHBOARD_ENABLED");
    }
}

fn app_over(probe: &RlsProbe) -> Router {
    let mut state: AppState = (*coralswarm_backend::test_app_state()).clone();
    state.db = Some(probe.app_pool.clone());
    state.store = Some(Arc::new(PostgresStore::new(probe.app_pool.clone())));
    coralswarm_backend::test_support_app(Arc::new(state))
}

async fn call(app: &Router, uri: &str, token: Option<&str>) -> (StatusCode, Option<String>, Value) {
    let mut req = HttpRequest::builder().uri(uri);
    if let Some(t) = token {
        req = req.header("Authorization", format!("Bearer {t}"));
    }
    let resp = app
        .clone()
        .oneshot(req.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    let cache = resp
        .headers()
        .get("cache-control")
        .map(|v| v.to_str().unwrap().to_string());
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, cache, serde_json::from_slice(&bytes).unwrap_or(Value::Null))
}

struct Caller {
    user_id: Uuid,
    account_id: Uuid,
    graph_id: Uuid,
    token: String,
}

async fn seed_caller(owner: &PgPool, tag: &str) -> Caller {
    let sub = format!("home_l_{tag}_{}", Uuid::new_v4().simple());
    let user_id: Uuid =
        sqlx::query_scalar("INSERT INTO users (email, clerk_user_id) VALUES ($1, $2) RETURNING id")
            .bind(format!("{sub}@home-launchpad.example"))
            .bind(&sub)
            .fetch_one(owner)
            .await
            .unwrap();
    let account_id: Uuid =
        sqlx::query_scalar("INSERT INTO accounts (owner_user_id) VALUES ($1) RETURNING id")
            .bind(user_id)
            .fetch_one(owner)
            .await
            .unwrap();
    sqlx::query("INSERT INTO account_members (account_id, user_id, role) VALUES ($1, $2, 'owner')")
        .bind(account_id)
        .bind(user_id)
        .execute(owner)
        .await
        .unwrap();
    let graph_id: Uuid =
        sqlx::query_scalar("INSERT INTO graphs (account_id, name) VALUES ($1, $2) RETURNING id")
            .bind(account_id)
            .bind(format!("{tag} ocean"))
            .fetch_one(owner)
            .await
            .unwrap();
    let token = mint(json!({
        "iss": ISSUER,
        "sub": sub,
        "exp": (Utc::now().timestamp() + 3600) as usize,
    }));
    Caller {
        user_id,
        account_id,
        graph_id,
        token,
    }
}

/// A second member of `account` with their own private (member) ocean, which
/// the caller can never see.
async fn seed_member(owner: &PgPool, account: Uuid, tag: &str) -> (Uuid, Uuid) {
    let user: Uuid = sqlx::query_scalar("INSERT INTO users (email) VALUES ($1) RETURNING id")
        .bind(format!("{tag}-{}@home-launchpad.example", Uuid::new_v4().simple()))
        .fetch_one(owner)
        .await
        .unwrap();
    sqlx::query("INSERT INTO account_members (account_id, user_id, role) VALUES ($1, $2, 'member')")
        .bind(account)
        .bind(user)
        .execute(owner)
        .await
        .unwrap();
    let private: Uuid = sqlx::query_scalar(
        "INSERT INTO graphs (account_id, name, kind, owner_user_id) VALUES ($1, $2, 'member', $3) RETURNING id",
    )
    .bind(account)
    .bind(format!("{tag} private"))
    .bind(user)
    .fetch_one(owner)
    .await
    .unwrap();
    (user, private)
}

async fn atom(
    owner: &PgPool,
    graph: Uuid,
    by: Uuid,
    source_type: &str,
    source_id: &str,
    content: &str,
    at: DateTime<Utc>,
) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO atoms(graph_id, source_type, source_id, content_blob_key, content_inline, ingested_by, \"timestamp\", created_at) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $7) RETURNING atom_id",
    )
    .bind(graph)
    .bind(source_type)
    .bind(source_id)
    .bind(format!("blob-{source_id}"))
    .bind(content)
    .bind(by)
    .bind(at)
    .fetch_one(owner)
    .await
    .expect("insert atom")
}

async fn chat(owner: &PgPool, account: Uuid, graph: Uuid, user: Uuid, title: &str, at: DateTime<Utc>) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO conversations(account_id, graph_id, user_id, title, created_at, updated_at) \
         VALUES ($1, $2, $3, $4, $5, $5) RETURNING id",
    )
    .bind(account)
    .bind(graph)
    .bind(user)
    .bind(title)
    .bind(at)
    .fetch_one(owner)
    .await
    .expect("insert conversation")
}

async fn message(owner: &PgPool, conversation: Uuid, seq: i32, role: &str) {
    sqlx::query("INSERT INTO conversation_messages(conversation_id, seq, role, text) VALUES ($1, $2, $3, 'words')")
        .bind(conversation)
        .bind(seq)
        .bind(role)
        .execute(owner)
        .await
        .expect("insert message");
}

#[allow(clippy::too_many_arguments)]
async fn session(
    owner: &PgPool,
    graph: Uuid,
    by: Uuid,
    id: &str,
    kind: &str,
    branch: &str,
    parent: Option<&str>,
    at: DateTime<Utc>,
) {
    sqlx::query(
        "INSERT INTO coding_sessions(graph_id, session_id, session_kind, project_key, branch, title, \
         started_at, last_activity_at, parent_session_id, platform) \
         VALUES ($1, $2, $3, 'github.com/acme/coralswarm', $4, $5, $6, $6, $7, 'claude-code')",
    )
    .bind(graph)
    .bind(id)
    .bind(kind)
    .bind(branch)
    .bind(format!("{id} session"))
    .bind(at)
    .bind(parent)
    .execute(owner)
    .await
    .expect("insert session");
    let source_type = match kind {
        "agent" => "agent_session",
        "meeting" => "meeting_session",
        _ => "coding_session",
    };
    atom(owner, graph, by, source_type, &format!("cc:{id}:ck1"), "checkpoint", at).await;
}

async fn reef(owner: &PgPool, graph: Uuid, kind: &str, title: &str, by: Uuid, at: DateTime<Utc>, named: bool) -> Uuid {
    let body = if kind == "decision" {
        json!({"decided_at": at.to_rfc3339()})
    } else {
        json!({})
    };
    let node: Uuid = sqlx::query_scalar(
        "INSERT INTO nodes(graph_id, type, title, body, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $5) RETURNING node_id",
    )
    .bind(graph)
    .bind(kind)
    .bind(title)
    .bind(body)
    .bind(at)
    .fetch_one(owner)
    .await
    .expect("insert node");
    // A named reef's coral mentions its title (the Library's title witness);
    // an unnamed one links only to a coral that never names it.
    let content = if named {
        format!("Notes: {title} was discussed.")
    } else {
        "Notes about something else entirely.".to_string()
    };
    let a = atom(owner, graph, by, "slack", &format!("C1:{}", Uuid::new_v4()), &content, at).await;
    sqlx::query("INSERT INTO node_atoms(node_id, atom_id) VALUES ($1, $2)")
        .bind(node)
        .bind(a)
        .execute(owner)
        .await
        .expect("link");
    node
}

#[tokio::test]
async fn flag_parameters_and_auth_gate_every_read() {
    let _env = ENV_LOCK.lock().await;
    let Some(probe) = RlsProbe::setup("orgt_home_launchpad_gates").await else {
        eprintln!("skipping flag_parameters_and_auth_gate_every_read: set ORGT_TEST_BASE_URL");
        return;
    };
    let jwks = spawn_local_jwks().await;
    let a = seed_caller(&probe.owner, "gates").await;
    let app = app_over(&probe);
    let paths = ["/v1/home/continue", "/v1/home/whats-new", "/v1/home/attention"];
    {
        let _g = EnvGuard::set(&jwks, None);
        for path in paths {
            let (status, _, _) = call(&app, path, Some(&a.token)).await;
            assert_eq!(status, StatusCode::NOT_FOUND, "{path}: flag unset must hide the route");
        }
    }
    {
        let _g = EnvGuard::set(&jwks, Some("1"));
        for path in paths {
            let (status, cache, body) = call(&app, path, Some(&a.token)).await;
            assert_eq!(status, StatusCode::OK, "{path}: {body}");
            assert_eq!(cache.as_deref(), Some("no-store"), "{path}: private reads stay live");
            assert!(body["generated_at"].is_string(), "{path}: {body}");
            let (status, _, _) = call(&app, &format!("{path}?since=2026-01-01"), Some(&a.token)).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{path} takes no parameters");
            let (status, _, _) = call(&app, path, None).await;
            assert_eq!(status, StatusCode::UNAUTHORIZED, "{path} needs a token");
        }
        // A brand-new viewer: nothing to continue, nothing new, the setup state.
        let (_, _, body) = call(&app, "/v1/home/continue", Some(&a.token)).await;
        assert_eq!(body["items"], json!([]));
        let (_, _, body) = call(&app, "/v1/home/whats-new", Some(&a.token)).await;
        assert_eq!(body["items"], json!([]));
        assert_eq!(body["window_days"], 7);
        let (_, _, body) = call(&app, "/v1/home/attention", Some(&a.token)).await;
        assert_eq!(body["has_content"], false, "{body}");
        assert_eq!(body["has_source"], false, "{body}");
        assert_eq!(body["has_mcp"], false, "{body}");
        assert_eq!(body["has_asked"], false, "{body}");
        assert!(body["attention"].is_null(), "{body}");
    }
    probe.teardown().await;
}

#[tokio::test]
async fn continue_lists_own_items_newest_first_with_caps_and_session_groups() {
    let _env = ENV_LOCK.lock().await;
    let Some(probe) = RlsProbe::setup("orgt_home_launchpad_continue").await else {
        eprintln!("skipping continue_lists_own_items...: set ORGT_TEST_BASE_URL");
        return;
    };
    let jwks = spawn_local_jwks().await;
    let _g = EnvGuard::set(&jwks, Some("true"));
    let owner = &probe.owner;
    let a = seed_caller(owner, "cont").await;
    let (b, b_private) = seed_member(owner, a.account_id, "cont-b").await;
    let org: Uuid = sqlx::query_scalar(
        "INSERT INTO graphs (account_id, name, kind) VALUES ($1, 'Company', 'org') RETURNING id",
    )
    .bind(a.account_id)
    .fetch_one(owner)
    .await
    .unwrap();
    let now = Utc::now();
    let h = |hours: f64| now - Duration::minutes((hours * 60.0) as i64);
    let g = a.graph_id;

    // Chats: four of the viewer's (the fourth is newer than the grouped
    // session row but cut by the per-kind cap) and a newer one of another
    // member's.
    let c1 = chat(owner, a.account_id, g, a.user_id, "Why is the read model stale?", h(1.0)).await;
    let c2 = chat(owner, a.account_id, org, a.user_id, "Seat caps", h(1.2)).await;
    let c3 = chat(owner, a.account_id, g, a.user_id, "Pricing", h(1.4)).await;
    chat(owner, a.account_id, g, a.user_id, "Fourth chat", h(1.6)).await;
    chat(owner, a.account_id, org, b, "Someone else's chat", h(0.2)).await;

    // Sessions: two of one repo+branch in one ocean collapse into one row; the
    // same repo+branch in another ocean is its own row, and so is another
    // branch (both cut here by the 6-row limit); an agent session is its own
    // kind (cut too); another member's session and the viewer's own sub-agent
    // session are never rows.
    session(owner, g, a.user_id, "s-home-1", "coding", "cs-home", None, h(1.7)).await;
    session(owner, g, a.user_id, "s-home-2", "coding", "cs-home", None, h(5.0)).await;
    session(owner, org, a.user_id, "s-home-3", "coding", "cs-home", None, h(6.0)).await;
    session(owner, g, a.user_id, "s-main", "coding", "main", None, h(7.0)).await;
    session(owner, g, a.user_id, "s-agent", "agent", "cs-home", None, h(2.5)).await;
    session(owner, g, a.user_id, "s-child", "coding", "cs-home", Some("s-home-1"), h(0.3)).await;
    session(owner, org, b, "s-theirs", "coding", "cs-home", None, h(0.5)).await;
    session(owner, b_private, b, "s-private", "coding", "secret", None, h(0.4)).await;

    // Meetings: the viewer's own, and another member's newer one.
    for (source, by, at) in [("mtg-mine", a.user_id, h(1.8)), ("mtg-theirs", b, h(0.6))] {
        sqlx::query(
            "INSERT INTO meeting_transcripts(graph_id, source_id, raw_inline, duration_ms, created_at) \
             VALUES ($1, $2, $3, 2040000, $4)",
        )
        .bind(org)
        .bind(source)
        .bind(format!("Design review {source}\nSpeaker: words"))
        .bind(at)
        .execute(owner)
        .await
        .unwrap();
        atom(owner, org, by, "meeting_transcript", &format!("{source}:chunk:0"), "words", at).await;
    }

    let (status, _, body) = call(&app_over(&probe), "/v1/home/continue", Some(&a.token)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let items = body["items"].as_array().unwrap();
    let got: Vec<(String, String)> = items
        .iter()
        .map(|i| (i["kind"].as_str().unwrap().to_string(), i["id"].as_str().unwrap().to_string()))
        .collect();
    let expect = |k: &str, id: &str| (k.to_string(), id.to_string());
    // The org ocean's newest coral (another member's session, 0.5 h ago) makes
    // the workspace row the newest; its author's items are still not listed.
    assert_eq!(
        got,
        vec![
            expect("workspace", &org.to_string()),
            expect("chat", &c1.to_string()),
            expect("chat", &c2.to_string()),
            expect("chat", &c3.to_string()),
            expect("coding", "s-home-1"),
            expect("meeting", "mtg-mine"),
        ],
        "newest first, 3 chats at most, 6 rows at most: {body}"
    );
    assert_eq!(items[0]["title"], "Company");
    assert_eq!(items[0]["context"], "organization ocean");
    assert!(items[0]["graph_id"].is_null(), "a workspace's id is its ocean");
    assert_eq!(items[1]["graph_id"], g.to_string(), "a chat carries its ocean");
    assert!(items[4]["graph_id"].is_null(), "a session row carries no ocean");
    let grouped = &items[4];
    assert_eq!(grouped["title"], "coralswarm \u{b7} cs-home");
    assert_eq!(
        grouped["count"], 2,
        "the same repo and branch in another ocean, a sub-agent and another member's sessions are not counted: {grouped}"
    );
    assert_eq!(
        grouped["context"], "claude code \u{b7} last: s-home-1 session",
        "the newest session's harness and title, never the count"
    );
    assert!(items[1]["count"].is_null());
    assert_eq!(items[1]["context"], "cont ocean", "a chat's context is its ocean, never message text");
    assert_eq!(items[2]["context"], "Company");
    assert_eq!(items[5]["title"], "Design review mtg-mine");
    assert_eq!(items[5]["context"], "34 min \u{b7} Company");
    assert_eq!(items[5]["graph_id"], org.to_string());
    let text = body.to_string();
    for foreign in ["Someone else's chat", "Fourth chat", "s-theirs", "s-private", "mtg-theirs", "s-child", "secret", "s-agent"] {
        assert!(!text.contains(foreign), "{foreign} leaked into Continue: {body}");
    }
    assert!(items.windows(2).all(|w| w[0]["at"].as_str() >= w[1]["at"].as_str()));
    probe.teardown().await;
}

#[tokio::test]
async fn continue_lists_workspaces_the_viewer_belongs_to() {
    let _env = ENV_LOCK.lock().await;
    let Some(probe) = RlsProbe::setup("orgt_home_launchpad_ws").await else {
        eprintln!("skipping continue_lists_workspaces...: set ORGT_TEST_BASE_URL");
        return;
    };
    let jwks = spawn_local_jwks().await;
    let _g = EnvGuard::set(&jwks, Some("true"));
    let owner = &probe.owner;
    let a = seed_caller(owner, "ws").await;
    let (b, b_private) = seed_member(owner, a.account_id, "ws-b").await;
    let org: Uuid = sqlx::query_scalar(
        "INSERT INTO graphs (account_id, name, kind) VALUES ($1, 'Company', 'org') RETURNING id",
    )
    .bind(a.account_id)
    .fetch_one(owner)
    .await
    .unwrap();
    let now = Utc::now();
    atom(owner, org, b, "slack", "C1:ws", "hello", now - Duration::hours(2)).await;
    // A newer coral the viewer may not read never dates the row.
    let gone = atom(owner, org, b, "slack", "C1:ws-gone", "gone", now - Duration::minutes(5)).await;
    sqlx::query("UPDATE atoms SET evidence_retracted = true WHERE atom_id = $1")
        .bind(gone)
        .execute(owner)
        .await
        .unwrap();
    atom(owner, b_private, b, "slack", "C1:priv", "hello", now).await;
    atom(owner, a.graph_id, a.user_id, "slack", "C1:mine", "hello", now).await;
    // A team ocean the viewer joined ten minutes ago, with no coral yet, and
    // one they are not on.
    let mut teams = Vec::new();
    for (name, member, joined) in [("Launch team", a.user_id, 10), ("Not mine", b, 1)] {
        let graph: Uuid = sqlx::query_scalar(
            "INSERT INTO graphs (account_id, name, kind, created_at) VALUES ($1, $2, 'team', now() - interval '30 days') RETURNING id",
        )
        .bind(a.account_id)
        .bind(name)
        .fetch_one(owner)
        .await
        .unwrap();
        let team: Uuid = sqlx::query_scalar(
            "INSERT INTO teams (account_id, graph_id, name, created_by) VALUES ($1, $2, $3, $4) RETURNING id",
        )
        .bind(a.account_id)
        .bind(graph)
        .bind(name)
        .bind(member)
        .fetch_one(owner)
        .await
        .unwrap();
        sqlx::query("INSERT INTO team_members (team_id, user_id, role, joined_at) VALUES ($1, $2, 'member', now() - make_interval(mins => $3))")
            .bind(team)
            .bind(member)
            .bind(joined)
            .execute(owner)
            .await
            .unwrap();
        teams.push(graph);
    }

    let (status, _, body) = call(&app_over(&probe), "/v1/home/continue", Some(&a.token)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let items = body["items"].as_array().unwrap();
    let got: Vec<(&str, &str, &str)> = items
        .iter()
        .map(|i| (i["kind"].as_str().unwrap(), i["id"].as_str().unwrap(), i["context"].as_str().unwrap()))
        .collect();
    let (team, org_id) = (teams[0].to_string(), org.to_string());
    assert_eq!(
        got,
        [
            ("workspace", team.as_str(), "shared workspace"),
            ("workspace", org_id.as_str(), "organization ocean"),
        ],
        "the shared oceans the viewer belongs to, a just-joined empty one first: {body}"
    );
    assert_eq!(items[0]["title"], "Launch team");
    assert_eq!(items[1]["title"], "Company");
    let at = |i: usize| {
        DateTime::parse_from_rfc3339(items[i]["at"].as_str().unwrap())
            .unwrap()
            .with_timezone(&Utc)
    };
    assert!(
        (now - Duration::minutes(11)..now - Duration::minutes(9)).contains(&at(0)),
        "an empty team ocean is dated by the viewer's membership: {body}"
    );
    assert!(
        (now - Duration::minutes(121)..now - Duration::minutes(119)).contains(&at(1)),
        "dated by the newest coral the viewer can read, not the retracted one: {body}"
    );
    probe.teardown().await;
}

#[tokio::test]
async fn continue_attributes_sessions_by_the_longest_real_id_and_lists_meeting_sessions_as_agent_rows() {
    let _env = ENV_LOCK.lock().await;
    let Some(probe) = RlsProbe::setup("orgt_home_launchpad_sess").await else {
        eprintln!("skipping continue_attributes_sessions...: set ORGT_TEST_BASE_URL");
        return;
    };
    let jwks = spawn_local_jwks().await;
    let _g = EnvGuard::set(&jwks, Some("true"));
    let owner = &probe.owner;
    let a = seed_caller(owner, "sess").await;
    let (b, _) = seed_member(owner, a.account_id, "sess-b").await;
    let org: Uuid = sqlx::query_scalar(
        "INSERT INTO graphs (account_id, name, kind) VALUES ($1, 'Company', 'org') RETURNING id",
    )
    .bind(a.account_id)
    .fetch_one(owner)
    .await
    .unwrap();
    let now = Utc::now();
    let ago = |minutes: i64| now - Duration::minutes(minutes);
    // Session ids may contain colons. "team" is another member's and
    // "team:alpha" the viewer's: the viewer's coral `cc:team:alpha:ck1` sorts
    // first in the `cc:team:` range and must not make "team" the viewer's.
    session(owner, org, b, "team", "coding", "b-team", None, ago(10)).await;
    session(owner, org, a.user_id, "team:alpha", "coding", "a-alpha", None, ago(20)).await;
    // The reverse: "crew" is the viewer's, "crew:beta" the other member's,
    // whose coral sorts first in the `cc:crew:` range.
    session(owner, org, a.user_id, "crew", "coding", "a-crew", None, ago(30)).await;
    session(owner, org, b, "crew:beta", "coding", "b-beta", None, ago(5)).await;
    // An agent-captured meeting: no transcript for /meeting to open, so it is
    // an agent row, never a meeting row.
    session(owner, a.graph_id, a.user_id, "mtg-sess", "meeting", "notes", None, ago(40)).await;

    let (status, _, body) = call(&app_over(&probe), "/v1/home/continue", Some(&a.token)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let rows: Vec<&Value> = body["items"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|i| i["kind"] != "workspace")
        .collect();
    let got: Vec<(&str, &str)> = rows
        .iter()
        .map(|i| (i["kind"].as_str().unwrap(), i["id"].as_str().unwrap()))
        .collect();
    assert_eq!(
        got,
        [("coding", "team:alpha"), ("coding", "crew"), ("agent", "mtg-sess")],
        "each session is its owner's by its longest real id: {body}"
    );
    assert_eq!(rows[0]["title"], "coralswarm \u{b7} a-alpha");
    let meeting = rows[2];
    assert_eq!(meeting["title"], "mtg-sess session");
    assert_eq!(meeting["context"], "claude code \u{b7} sess ocean");
    assert!(meeting["graph_id"].is_null() && meeting["count"].is_null(), "{meeting}");
    let text = body.to_string();
    for foreign in ["b-team", "b-beta", "\"team\"", "crew:beta"] {
        assert!(!text.contains(foreign), "{foreign} leaked into Continue: {body}");
    }
    probe.teardown().await;
}

#[tokio::test]
async fn whats_new_orders_by_kind_within_seven_days_and_hides_what_the_viewer_cannot_see() {
    let _env = ENV_LOCK.lock().await;
    let Some(probe) = RlsProbe::setup("orgt_home_launchpad_new").await else {
        eprintln!("skipping whats_new_orders...: set ORGT_TEST_BASE_URL");
        return;
    };
    let jwks = spawn_local_jwks().await;
    let _g = EnvGuard::set(&jwks, Some("true"));
    let owner = &probe.owner;
    let a = seed_caller(owner, "new").await;
    let (b, b_private) = seed_member(owner, a.account_id, "new-b").await;
    let other = seed_caller(owner, "new-other").await;
    let now = Utc::now();
    let ago = |hours: i64| now - Duration::hours(hours);
    let g = a.graph_id;
    let u = a.user_id;

    let d1 = reef(owner, g, "decision", "Usage billing starts at renewal", u, ago(48), true).await;
    let t1 = reef(owner, g, "thread", "Connector retry policy", u, ago(120), true).await;
    let e1 = reef(owner, g, "entity", "Kim Ng", u, ago(1), true).await;
    let e2 = reef(owner, g, "entity", "EU invoicing", u, ago(72), true).await;
    let r1 = reef(owner, g, "reference", "Runbook link", u, ago(24), true).await;
    // Outside the window, hidden title, another member's private ocean, another account.
    reef(owner, g, "decision", "Old decision", u, ago(24 * 8), true).await;
    reef(owner, g, "decision", "Hidden decision", u, ago(1), false).await;
    reef(owner, b_private, "decision", "Their private decision", b, ago(1), true).await;
    reef(owner, other.graph_id, "decision", "Other account decision", other.user_id, ago(1), true).await;
    let extra = reef(owner, g, "entity", "Sixth reef", u, ago(100), true).await;

    let (status, _, body) = call(&app_over(&probe), "/v1/home/whats-new", Some(&a.token)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["window_days"], 7);
    let items = body["items"].as_array().unwrap();
    let ids: Vec<String> = items
        .iter()
        .map(|i| i["reef_id"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(
        ids,
        [d1, t1, e1, e2, extra].map(|u| u.to_string()),
        "decisions and threads first, then entities, newest first within each, 5 at most: {body}"
    );
    assert!(!ids.contains(&r1.to_string()), "a reference ranks after entities");
    assert_eq!(items[0]["kind"], "decision");
    assert_eq!(items[0]["title"], "Usage billing starts at renewal");
    assert_eq!(items[0]["ocean"], json!({"graph_id": g, "name": "new ocean", "kind": "personal"}));
    assert!(items[0]["formed_at"].is_string());
    let text = body.to_string();
    for hidden in ["Old decision", "Hidden decision", "Their private decision", "Other account decision"] {
        assert!(!text.contains(hidden), "{hidden} must not be listed: {body}");
    }

    // With fewer than five decisions/threads/entities in the window, a
    // reference fills in.
    sqlx::query("UPDATE nodes SET created_at = now() - interval '10 days' WHERE node_id = ANY($1)")
        .bind(vec![e2, extra])
        .execute(owner)
        .await
        .unwrap();
    let (_, _, body) = call(&app_over(&probe), "/v1/home/whats-new", Some(&a.token)).await;
    let ids: Vec<String> = body["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["reef_id"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(ids, [d1, t1, e1, r1].map(|u| u.to_string()), "{body}");
    probe.teardown().await;
}

#[tokio::test]
async fn whats_new_reaches_visible_reefs_behind_many_hidden_ones() {
    let _env = ENV_LOCK.lock().await;
    let Some(probe) = RlsProbe::setup("orgt_home_launchpad_starve").await else {
        eprintln!("skipping whats_new_reaches_visible...: set ORGT_TEST_BASE_URL");
        return;
    };
    let jwks = spawn_local_jwks().await;
    let _g = EnvGuard::set(&jwks, Some("true"));
    let owner = &probe.owner;
    let a = seed_caller(owner, "starve").await;
    let now = Utc::now();
    let ago = |minutes: i64| now - Duration::minutes(minutes);
    let (g, u) = (a.graph_id, a.user_id);
    // Twenty hidden decisions, newer than every visible reef: more than any
    // per-kind candidate cap would examine.
    for i in 0..20 {
        reef(owner, g, "decision", &format!("Hidden decision {i}"), u, ago(i + 1), false).await;
    }
    let d1 = reef(owner, g, "decision", "Visible decision one", u, ago(30 * 60), true).await;
    let d2 = reef(owner, g, "thread", "Visible thread two", u, ago(31 * 60), true).await;
    let e1 = reef(owner, g, "entity", "Entity one", u, ago(120), true).await;
    let e2 = reef(owner, g, "entity", "Entity two", u, ago(180), true).await;
    let e3 = reef(owner, g, "entity", "Entity three", u, ago(240), true).await;
    reef(owner, g, "entity", "Entity four", u, ago(300), true).await;

    let (status, _, body) = call(&app_over(&probe), "/v1/home/whats-new", Some(&a.token)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let ids: Vec<String> = body["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["reef_id"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(
        ids,
        [d1, d2, e1, e2, e3].map(|u| u.to_string()),
        "five visible reefs exist in the window, so five are listed, in rank then recency order: {body}"
    );
    assert!(!body.to_string().contains("Hidden decision"), "{body}");
    probe.teardown().await;
}

#[tokio::test]
async fn attention_flags_content_and_only_the_first_broken_connector() {
    let _env = ENV_LOCK.lock().await;
    let Some(probe) = RlsProbe::setup("orgt_home_launchpad_attention").await else {
        eprintln!("skipping attention_flags...: set ORGT_TEST_BASE_URL");
        return;
    };
    let jwks = spawn_local_jwks().await;
    let _g = EnvGuard::set(&jwks, Some("true"));
    let owner = &probe.owner;
    let a = seed_caller(owner, "att").await;
    let (b, b_private) = seed_member(owner, a.account_id, "att-b").await;
    let app = app_over(&probe);

    // Content only in another member's private ocean is not the viewer's.
    atom(owner, b_private, b, "slack", "C1:p", "private", Utc::now()).await;
    let (_, _, body) = call(&app, "/v1/home/attention", Some(&a.token)).await;
    assert_eq!(
        body,
        json!({"has_content": false, "has_source": false, "has_mcp": false, "has_asked": false, "attention": null, "generated_at": body["generated_at"]})
    );

    // A retracted coral is not content the viewer can see.
    let retracted = atom(owner, a.graph_id, a.user_id, "slack", "C1:r", "gone", Utc::now()).await;
    sqlx::query("UPDATE atoms SET evidence_retracted = true WHERE atom_id = $1")
        .bind(retracted)
        .execute(owner)
        .await
        .unwrap();
    let (_, _, body) = call(&app, "/v1/home/attention", Some(&a.token)).await;
    assert_eq!(body["has_content"], false, "{body}");

    // One readable coral behind more unreadable newer ones (source-bound with
    // no permitted source) than the first pass probes is still content.
    atom(owner, a.graph_id, a.user_id, "slack", "C1:1", "hello", Utc::now() - Duration::hours(2)).await;
    for i in 0..12 {
        let bound = atom(owner, a.graph_id, a.user_id, "slack", &format!("C1:bound-{i}"), "restricted", Utc::now() - Duration::minutes(i)).await;
        sqlx::query("UPDATE atoms SET source_evidence_bound = true WHERE atom_id = $1")
            .bind(bound)
            .execute(owner)
            .await
            .unwrap();
    }
    let (_, _, body) = call(&app, "/v1/home/attention", Some(&a.token)).await;
    assert_eq!(body["has_content"], true, "{body}");
    assert!(body["attention"].is_null(), "no connectors, nothing broken: {body}");
    assert_eq!((body["has_source"].as_bool(), body["has_mcp"].as_bool()), (Some(false), Some(false)));

    // Two dead credentials yield ONE row: the first failing row by name.
    // Attention never reads the 30-day counts (a whole-ocean read), so Slack's
    // readable coral does not put it ahead of Microsoft, as the status page's
    // volume tiebreak would.
    for provider in ["slack", "microsoft"] {
        sqlx::query(
            "INSERT INTO oauth_tokens(account_id, owner_user_id, provider, scope, access_token, credential_invalid_at, credential_invalid_reason) \
             VALUES ($1, $2, $3, '', 'unused-ciphertext', now(), 'slack_token_revoked')",
        )
        .bind(a.account_id)
        .bind(a.user_id)
        .bind(provider)
        .execute(owner)
        .await
        .unwrap();
    }
    let (status, _, body) = call(&app, "/v1/home/attention", Some(&a.token)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let att = &body["attention"];
    assert!(att.is_object(), "{body}");
    assert_eq!(att["reason"], "reauth");
    assert_eq!(att["provider"], "microsoft", "first failing row by name: {body}");
    assert_eq!(att["connector_id"], "microsoft:-:member");
    assert_eq!(att["label"], "Microsoft 365 needs you to sign in again", "{body}");
    assert_eq!(att.as_object().unwrap().len(), 4, "exactly the contract fields: {att}");
    assert_eq!(body["has_source"], true, "a broken source is still connected: {body}");
    assert_eq!(body["has_mcp"], false, "{body}");

    // An MCP client that has called a tool is the checklist's second step.
    sqlx::query("INSERT INTO mcp_tool_calls (account_id, user_id, tool, outcome) VALUES ($1, $2, 'ask', 'ok')")
        .bind(a.account_id)
        .bind(a.user_id)
        .execute(owner)
        .await
        .unwrap();
    let (_, _, body) = call(&app, "/v1/home/attention", Some(&a.token)).await;
    assert_eq!(body["has_mcp"], true, "{body}");
    assert_eq!(body["attention"]["provider"], "microsoft", "the MCP row is never broken: {body}");

    // Checklist step 3: a conversation alone (only an assistant message, or
    // none) is not a question; one user message, from any surface, is. Another
    // member's question is never the viewer's.
    let empty = chat(owner, a.account_id, a.graph_id, a.user_id, "New chat", Utc::now()).await;
    message(owner, empty, 1, "assistant").await;
    let theirs = chat(owner, a.account_id, a.graph_id, b, "Their question", Utc::now()).await;
    message(owner, theirs, 1, "user").await;
    let (_, _, body) = call(&app, "/v1/home/attention", Some(&a.token)).await;
    assert_eq!(body["has_asked"], false, "{body}");
    let asked = chat(owner, a.account_id, a.graph_id, a.user_id, "Asked", Utc::now() - Duration::days(3)).await;
    message(owner, asked, 1, "user").await;
    let (_, _, body) = call(&app, "/v1/home/attention", Some(&a.token)).await;
    assert_eq!(body["has_asked"], true, "{body}");

    // Another account's connectors are never this viewer's.
    let other = seed_caller(owner, "att-other").await;
    let (_, _, body) = call(&app, "/v1/home/attention", Some(&other.token)).await;
    assert!(body["attention"].is_null(), "{body}");
    assert_eq!((body["has_source"].as_bool(), body["has_mcp"].as_bool()), (Some(false), Some(false)), "{body}");
    assert_eq!(body["has_asked"], false, "{body}");
    probe.teardown().await;
}

/// Attention reads the control plane and the content plane. On the personal
/// plane both are the same pool, so a request that held its content
/// connection while it waited for a control one would deadlock a
/// one-connection pool (and starve a small one under concurrent load). Each
/// request must hold one connection at a time: with ONE connection, every
/// read answers, one after another and concurrently.
#[tokio::test]
async fn reads_hold_one_pooled_connection_at_a_time() {
    let _env = ENV_LOCK.lock().await;
    let Some(probe) = RlsProbe::setup("orgt_home_launchpad_pool").await else {
        eprintln!("skipping reads_hold_one_pooled_connection_at_a_time: set ORGT_TEST_BASE_URL");
        return;
    };
    let jwks = spawn_local_jwks().await;
    let _g = EnvGuard::set(&jwks, Some("1"));
    let a = seed_caller(&probe.owner, "pool").await;
    atom(&probe.owner, a.graph_id, a.user_id, "slack", "C1:pool", "hello", Utc::now()).await;
    let one = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(std::time::Duration::from_secs(3))
        .connect_with((*probe.app_pool.connect_options()).clone())
        .await
        .expect("one-connection pool");
    let mut state: AppState = (*coralswarm_backend::test_app_state()).clone();
    state.db = Some(one.clone());
    state.store = Some(Arc::new(PostgresStore::new(one)));
    let app = coralswarm_backend::test_support_app(Arc::new(state));
    for path in ["/v1/home/attention", "/v1/home/continue", "/v1/home/whats-new"] {
        let (status, _, body) = call(&app, path, Some(&a.token)).await;
        assert_eq!(status, StatusCode::OK, "{path} on a one-connection pool: {body}");
    }
    let (_, _, body) = call(&app, "/v1/home/attention", Some(&a.token)).await;
    assert_eq!(body["has_content"], true, "{body}");
    let calls = (0..4).map(|_| call(&app, "/v1/home/attention", Some(&a.token)));
    for (status, _, body) in futures::future::join_all(calls).await {
        assert_eq!(status, StatusCode::OK, "concurrent attention on one connection: {body}");
    }
    probe.teardown().await;
}

/// `has_content` reads at most `CONTENT_SCAN_PER_OCEAN` (300) corals per
/// ocean, newest first, whatever their state: retracted and restricted corals
/// use up the bound like any other. A readable coral 300th is found; 301st is
/// past the bound (the documented false negative).
#[tokio::test]
async fn has_content_reads_at_most_the_scan_bound_per_ocean() {
    let _env = ENV_LOCK.lock().await;
    let Some(probe) = RlsProbe::setup("orgt_home_launchpad_bound").await else {
        eprintln!("skipping has_content_reads_at_most_the_scan_bound_per_ocean: set ORGT_TEST_BASE_URL");
        return;
    };
    let jwks = spawn_local_jwks().await;
    let _g = EnvGuard::set(&jwks, Some("1"));
    let owner = &probe.owner;
    let a = seed_caller(owner, "bound").await;
    let app = app_over(&probe);
    let now = Utc::now();
    atom(owner, a.graph_id, a.user_id, "slack", "C1:readable", "hello", now - Duration::hours(1)).await;
    // `n` unreadable corals newer than the readable one, alternately
    // retracted and source-bound with no permitted source.
    let unreadable = |from: i32, to: i32| {
        let (graph, user) = (a.graph_id, a.user_id);
        async move {
            sqlx::query(
                "INSERT INTO atoms(graph_id, source_type, source_id, content_blob_key, content_inline, ingested_by, \"timestamp\", created_at) \
                 SELECT $1, 'slack', 'C1:hidden-' || i, 'blob-hidden-' || i, 'hidden', $2, now() - make_interval(secs => i), now() - make_interval(secs => i) \
                 FROM generate_series($3, $4) i",
            )
            .bind(graph)
            .bind(user)
            .bind(from)
            .bind(to)
            .execute(owner)
            .await
            .unwrap();
            sqlx::query("UPDATE atoms SET evidence_retracted = true WHERE graph_id = $1 AND source_id LIKE 'C1:hidden-%' AND substr(source_id, 11)::int % 2 = 0 AND NOT evidence_retracted")
                .bind(graph)
                .execute(owner)
                .await
                .unwrap();
            sqlx::query("UPDATE atoms SET source_evidence_bound = true WHERE graph_id = $1 AND source_id LIKE 'C1:hidden-%' AND substr(source_id, 11)::int % 2 = 1 AND NOT source_evidence_bound")
                .bind(graph)
                .execute(owner)
                .await
                .unwrap();
        }
    };
    unreadable(1, 299).await;
    let (status, _, body) = call(&app, "/v1/home/attention", Some(&a.token)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["has_content"], true, "the readable coral is the 300th newest: {body}");
    unreadable(300, 300).await;
    let (_, _, body) = call(&app, "/v1/home/attention", Some(&a.token)).await;
    assert_eq!(body["has_content"], false, "the 301st newest is past the bound: {body}");
    probe.teardown().await;
}

/// Sessions and meetings are found from the viewer's own corals, so another
/// member's 150 newer sessions and transcripts in the same shared ocean never
/// push the viewer's own out (the per-ocean newest-100 cap before the owner
/// filter did).
#[tokio::test]
async fn continue_finds_own_sessions_and_meetings_behind_a_busy_shared_ocean() {
    let _env = ENV_LOCK.lock().await;
    let Some(probe) = RlsProbe::setup("orgt_home_launchpad_busy").await else {
        eprintln!("skipping continue_finds_own_sessions_and_meetings...: set ORGT_TEST_BASE_URL");
        return;
    };
    let jwks = spawn_local_jwks().await;
    let _g = EnvGuard::set(&jwks, Some("true"));
    let owner = &probe.owner;
    let a = seed_caller(owner, "busy").await;
    let (b, _) = seed_member(owner, a.account_id, "busy-b").await;
    let org: Uuid = sqlx::query_scalar(
        "INSERT INTO graphs (account_id, name, kind) VALUES ($1, 'Company', 'org') RETURNING id",
    )
    .bind(a.account_id)
    .fetch_one(owner)
    .await
    .unwrap();
    for sql in [
        "INSERT INTO coding_sessions(graph_id, session_id, session_kind, project_key, branch, title, started_at, last_activity_at, platform) \
         SELECT $1, 'busy-' || i, 'coding', 'github.com/acme/other', 'b-' || i, 'Busy ' || i, now() - make_interval(mins => i), now() - make_interval(mins => i), 'claude-code' \
         FROM generate_series(1, 150) i",
        "INSERT INTO atoms(graph_id, source_type, source_id, content_blob_key, content_inline, ingested_by, \"timestamp\", created_at) \
         SELECT $1, 'coding_session', 'cc:busy-' || i || ':ck1', 'blob-busy-' || i, 'checkpoint', $2, now() - make_interval(mins => i), now() - make_interval(mins => i) \
         FROM generate_series(1, 150) i",
        "INSERT INTO meeting_transcripts(graph_id, source_id, raw_inline, duration_ms, created_at) \
         SELECT $1, 'busy-mtg-' || i, 'Their sync ' || i, 60000, now() - make_interval(mins => i) FROM generate_series(1, 150) i",
        "INSERT INTO atoms(graph_id, source_type, source_id, content_blob_key, content_inline, ingested_by, \"timestamp\", created_at) \
         SELECT $1, 'meeting_transcript', 'busy-mtg-' || i || ':chunk:0', 'blob-busy-mtg-' || i, 'words', $2, now() - make_interval(mins => i), now() - make_interval(mins => i) \
         FROM generate_series(1, 150) i",
    ] {
        let q = sqlx::query(sql).bind(org);
        let q = if sql.contains("$2") { q.bind(b) } else { q };
        q.execute(owner).await.unwrap();
    }
    let ago = Utc::now() - Duration::hours(5);
    session(owner, org, a.user_id, "mine-old", "coding", "cs-mine", None, ago).await;
    sqlx::query(
        "INSERT INTO meeting_transcripts(graph_id, source_id, raw_inline, duration_ms, created_at) VALUES ($1, 'mtg-mine-old', 'My planning', 600000, $2)",
    )
    .bind(org)
    .bind(ago)
    .execute(owner)
    .await
    .unwrap();
    atom(owner, org, a.user_id, "meeting_transcript", "mtg-mine-old:chunk:0", "words", ago).await;

    let (status, _, body) = call(&app_over(&probe), "/v1/home/continue", Some(&a.token)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let got: Vec<(&str, &str)> = body["items"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|i| i["kind"] != "workspace")
        .map(|i| (i["kind"].as_str().unwrap(), i["id"].as_str().unwrap()))
        .collect();
    assert_eq!(
        got,
        [("coding", "mine-old"), ("meeting", "mtg-mine-old")],
        "the viewer's own items behind 150 newer ones of another member: {body}"
    );
    assert!(!body.to_string().contains("busy-"), "{body}");
    probe.teardown().await;
}

/// A session's owner is whoever ingested its FIRST coral by creation time,
/// not the coral first in byte order. Both members touched both sessions.
#[tokio::test]
async fn continue_owner_is_the_first_coral_by_creation_time() {
    let _env = ENV_LOCK.lock().await;
    let Some(probe) = RlsProbe::setup("orgt_home_launchpad_first").await else {
        eprintln!("skipping continue_owner_is_the_first_coral_by_creation_time: set ORGT_TEST_BASE_URL");
        return;
    };
    let jwks = spawn_local_jwks().await;
    let _g = EnvGuard::set(&jwks, Some("true"));
    let owner = &probe.owner;
    let a = seed_caller(owner, "first").await;
    let (b, _) = seed_member(owner, a.account_id, "first-b").await;
    let org: Uuid = sqlx::query_scalar(
        "INSERT INTO graphs (account_id, name, kind) VALUES ($1, 'Company', 'org') RETURNING id",
    )
    .bind(a.account_id)
    .fetch_one(owner)
    .await
    .unwrap();
    let now = Utc::now();
    let ago = |minutes: i64| now - Duration::minutes(minutes);
    for (id, branch) in [("theirs", "b-first"), ("mine", "a-first")] {
        sqlx::query(
            "INSERT INTO coding_sessions(graph_id, session_id, session_kind, project_key, branch, title, started_at, last_activity_at, platform) \
             VALUES ($1, $2, 'coding', 'github.com/acme/coralswarm', $3, $2, $4, $5, 'claude-code')",
        )
        .bind(org)
        .bind(id)
        .bind(branch)
        .bind(ago(60))
        .bind(ago(10))
        .execute(owner)
        .await
        .unwrap();
    }
    // "theirs": the viewer's coral sorts first by bytes (`:a`) but the other
    // member's (`:z`) was created first. "mine": the reverse.
    atom(owner, org, a.user_id, "coding_session", "cc:theirs:a", "late", ago(10)).await;
    atom(owner, org, b, "coding_session", "cc:theirs:z", "first", ago(60)).await;
    atom(owner, org, b, "coding_session", "cc:mine:a", "late", ago(10)).await;
    atom(owner, org, a.user_id, "coding_session", "cc:mine:z", "first", ago(60)).await;

    let (status, _, body) = call(&app_over(&probe), "/v1/home/continue", Some(&a.token)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let got: Vec<&str> = body["items"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|i| i["kind"] == "coding")
        .map(|i| i["id"].as_str().unwrap())
        .collect();
    assert_eq!(got, ["mine"], "owned by the first coral in time: {body}");
    assert!(!body.to_string().contains("b-first"), "{body}");
    probe.teardown().await;
}

/// The viewer's own corals in an ocean they cannot see (here an archived one)
/// are skipped before Continue's per-type bound, never counted against it: 200
/// newer session checkpoints and 200 newer meeting chunks there used to take
/// the whole bound, and the viewer's visible session and meeting vanished.
#[tokio::test]
async fn continue_skips_own_corals_in_an_ocean_the_viewer_cannot_see() {
    let _env = ENV_LOCK.lock().await;
    let Some(probe) = RlsProbe::setup("orgt_home_launchpad_invisible").await else {
        eprintln!("skipping continue_skips_own_corals_in_an_ocean_the_viewer_cannot_see: set ORGT_TEST_BASE_URL");
        return;
    };
    let jwks = spawn_local_jwks().await;
    let _g = EnvGuard::set(&jwks, Some("true"));
    let owner = &probe.owner;
    let a = seed_caller(owner, "invisible").await;
    let archived: Uuid = sqlx::query_scalar(
        "INSERT INTO graphs (account_id, name, kind, owner_user_id, archived_at) VALUES ($1, 'Archived', 'member', $2, now()) RETURNING id",
    )
    .bind(a.account_id)
    .bind(a.user_id)
    .fetch_one(owner)
    .await
    .unwrap();
    for (source_type, source_id) in [("coding_session", "'cc:gone-' || i || ':ck1'"), ("meeting_transcript", "'gone-mtg-' || i || ':chunk:0'")] {
        sqlx::query(&format!(
            "INSERT INTO atoms(graph_id, source_type, source_id, content_blob_key, content_inline, ingested_by, \"timestamp\", created_at) \
             SELECT $1, $2, {source_id}, 'blob-' || $2 || i, 'hidden', $3, now() - make_interval(mins => i), now() - make_interval(mins => i) \
             FROM generate_series(1, 200) i"
        ))
        .bind(archived)
        .bind(source_type)
        .bind(a.user_id)
        .execute(owner)
        .await
        .unwrap();
    }
    let ago = Utc::now() - Duration::hours(5);
    session(owner, a.graph_id, a.user_id, "mine-visible", "coding", "cs-visible", None, ago).await;
    sqlx::query(
        "INSERT INTO meeting_transcripts(graph_id, source_id, raw_inline, duration_ms, created_at) VALUES ($1, 'mtg-visible', 'My planning', 600000, $2)",
    )
    .bind(a.graph_id)
    .bind(ago)
    .execute(owner)
    .await
    .unwrap();
    atom(owner, a.graph_id, a.user_id, "meeting_transcript", "mtg-visible:chunk:0", "words", ago).await;

    let (status, _, body) = call(&app_over(&probe), "/v1/home/continue", Some(&a.token)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let got: Vec<(&str, &str)> = body["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| (i["kind"].as_str().unwrap(), i["id"].as_str().unwrap()))
        .collect();
    assert_eq!(
        got,
        [("coding", "mine-visible"), ("meeting", "mtg-visible")],
        "behind 200 newer own corals of each type in an archived ocean: {body}"
    );
    probe.teardown().await;
}

/// The owner walk visits a session's key group (the first segment of its id)
/// in time order. An OLDER coral of a sibling session in that group, owned by
/// another member, is passed over, whichever of the two ids is the longer:
/// `pod` (theirs) before `pod:x` (the viewer's), `den:y` (theirs) before `den`
/// (the viewer's).
#[tokio::test]
async fn continue_owner_walk_passes_older_corals_of_sibling_sessions() {
    let _env = ENV_LOCK.lock().await;
    let Some(probe) = RlsProbe::setup("orgt_home_launchpad_sibling").await else {
        eprintln!("skipping continue_owner_walk_passes_older_corals_of_sibling_sessions: set ORGT_TEST_BASE_URL");
        return;
    };
    let jwks = spawn_local_jwks().await;
    let _g = EnvGuard::set(&jwks, Some("true"));
    let owner = &probe.owner;
    let a = seed_caller(owner, "sibling").await;
    let (b, _) = seed_member(owner, a.account_id, "sibling-b").await;
    let org: Uuid = sqlx::query_scalar(
        "INSERT INTO graphs (account_id, name, kind) VALUES ($1, 'Company', 'org') RETURNING id",
    )
    .bind(a.account_id)
    .fetch_one(owner)
    .await
    .unwrap();
    let now = Utc::now();
    let ago = |minutes: i64| now - Duration::minutes(minutes);
    session(owner, org, b, "pod", "coding", "b-pod", None, ago(50)).await;
    session(owner, org, a.user_id, "pod:x", "coding", "a-pod-x", None, ago(20)).await;
    session(owner, org, b, "den:y", "coding", "b-den-y", None, ago(60)).await;
    session(owner, org, a.user_id, "den", "coding", "a-den", None, ago(25)).await;

    let (status, _, body) = call(&app_over(&probe), "/v1/home/continue", Some(&a.token)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let got: Vec<&str> = body["items"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|i| i["kind"] == "coding")
        .map(|i| i["id"].as_str().unwrap())
        .collect();
    assert_eq!(got, ["pod:x", "den"], "each the viewer's by its own first coral: {body}");
    let text = body.to_string();
    for foreign in ["b-pod", "b-den-y"] {
        assert!(!text.contains(foreign), "{foreign} leaked into Continue: {body}");
    }
    probe.teardown().await;
}

/// `has_content`'s windows split a run of corals with one `created_at` (one
/// capture writes all its chunks with one) by `atom_id`, newest first, so the
/// bound and the window boundary fall on the same corals every time. 300
/// unreadable corals and a readable one all share a `created_at`: the readable
/// one with the smallest `atom_id` is the 301st, past the bound (inserted last,
/// so the heap order alone would put it first); a readable coral 9th in that
/// order, the first of the second window, is found.
#[tokio::test]
async fn has_content_orders_a_created_at_tie_by_atom_id() {
    let _env = ENV_LOCK.lock().await;
    let Some(probe) = RlsProbe::setup("orgt_home_launchpad_tie").await else {
        eprintln!("skipping has_content_orders_a_created_at_tie_by_atom_id: set ORGT_TEST_BASE_URL");
        return;
    };
    let jwks = spawn_local_jwks().await;
    let _g = EnvGuard::set(&jwks, Some("1"));
    let owner = &probe.owner;
    let a = seed_caller(owner, "tie").await;
    let app = app_over(&probe);
    let at = Utc::now() - Duration::hours(1);
    // 300 unreadable corals at one instant, `atom_id` ascending with `i` (odd
    // ids are left free), alternately source-bound with no permitted source
    // and retracted.
    sqlx::query(
        "INSERT INTO atoms(atom_id, graph_id, source_type, source_id, content_blob_key, content_inline, ingested_by, \"timestamp\", created_at, evidence_retracted, source_evidence_bound) \
         SELECT ('00000000-0000-4000-8000-' || lpad(to_hex(4096 + 2 * i), 12, '0'))::uuid, $1, 'slack', 'C1:tie-' || i, 'blob-tie-' || i, 'tied', $2, $3, $3, i % 2 = 1, i % 2 = 0 \
         FROM generate_series(1, 300) i",
    )
    .bind(a.graph_id)
    .bind(a.user_id)
    .bind(at)
    .execute(owner)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO atoms(atom_id, graph_id, source_type, source_id, content_blob_key, content_inline, ingested_by, \"timestamp\", created_at) \
         VALUES ('00000000-0000-4000-8000-000000000001', $1, 'slack', 'C1:tie-last', 'blob-tie-last', 'readable', $2, $3, $3)",
    )
    .bind(a.graph_id)
    .bind(a.user_id)
    .bind(at)
    .execute(owner)
    .await
    .unwrap();
    let (status, _, body) = call(&app, "/v1/home/attention", Some(&a.token)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["has_content"], false, "the smallest atom_id of the tie is the 301st newest: {body}");

    sqlx::query("UPDATE atoms SET evidence_retracted = true WHERE source_id = 'C1:tie-last'")
        .execute(owner)
        .await
        .unwrap();
    // Between i = 293 (8th newest) and i = 292: the 9th newest.
    sqlx::query(
        "INSERT INTO atoms(atom_id, graph_id, source_type, source_id, content_blob_key, content_inline, ingested_by, \"timestamp\", created_at) \
         VALUES ($1::uuid, $2, 'slack', 'C1:tie-ninth', 'blob-tie-ninth', 'readable', $3, $4, $4)",
    )
    .bind(format!("00000000-0000-4000-8000-{:012x}", 4096 + 2 * 293 - 1))
    .bind(a.graph_id)
    .bind(a.user_id)
    .bind(at)
    .execute(owner)
    .await
    .unwrap();
    let (_, _, body) = call(&app, "/v1/home/attention", Some(&a.token)).await;
    assert_eq!(body["has_content"], true, "the first coral of the second window, inside a tie: {body}");
    probe.teardown().await;
}
