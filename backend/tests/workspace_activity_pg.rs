//! Route-level tests for `GET /v1/home/workspace-activity` against the REAL
//! production router (`test_support_app`) served off a `coralswarm_app` login
//! pool, the same harness as `home_metrics_pg.rs`: Clerk-shaped JWTs against a
//! local JWKS listener, `begin_member_scoped` as in production.
//!
//! Covers: the flag (404 off), parameter validation, headline selection over
//! seeded rows (busiest visible reef, `reef_added` vs `reef_updated`, a hidden
//! title never named, the `corals_added` fallback), retracted corals and
//! out-of-window corals not counting, ordering and `limit`, and isolation: the
//! caller's own private ocean, another member's private ocean, a team the
//! caller is not on and another account's ocean never appear.
//!
//! Env-gated on `ORGT_TEST_BASE_URL` like every other `*_pg.rs` test:
//!
//!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test workspace_activity_pg

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

async fn body_json(resp: axum::response::Response) -> Value {
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice(&bytes).unwrap_or(Value::Null)
}

fn get(uri: &str, token: Option<&str>) -> HttpRequest<Body> {
    let mut b = HttpRequest::builder().uri(uri);
    if let Some(t) = token {
        b = b.header("Authorization", format!("Bearer {t}"));
    }
    b.body(Body::empty()).unwrap()
}

fn app_over(probe: &RlsProbe) -> Router {
    let mut state: AppState = (*coralswarm_backend::test_app_state()).clone();
    state.db = Some(probe.app_pool.clone());
    state.store = Some(Arc::new(PostgresStore::new(probe.app_pool.clone())));
    coralswarm_backend::test_support_app(Arc::new(state))
}

async fn call(app: &Router, pool:&PgPool, uri: &str, token: &str) -> (StatusCode, Value) {
    support::read_models::stored_call(app,pool,uri,token).await
}

struct Caller {
    user_id: Uuid,
    account_id: Uuid,
    token: String,
}

async fn seed_caller(owner: &PgPool, tag: &str) -> Caller {
    let sub = format!("wsa_{tag}_{}", Uuid::new_v4().simple());
    let user_id: Uuid =
        sqlx::query_scalar("INSERT INTO users (email, clerk_user_id) VALUES ($1, $2) RETURNING id")
            .bind(format!("{sub}@workspace-activity.example"))
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
    let token = mint(json!({
        "iss": ISSUER,
        "sub": sub,
        "exp": (Utc::now().timestamp() + 3600) as usize,
    }));
    Caller {
        user_id,
        account_id,
        token,
    }
}

async fn graph(
    owner: &PgPool,
    account: Uuid,
    name: &str,
    kind: &str,
    owner_user: Option<Uuid>,
) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO graphs (account_id, name, kind, owner_user_id) VALUES ($1, $2, $3, $4) RETURNING id",
    )
    .bind(account)
    .bind(name)
    .bind(kind)
    .bind(owner_user)
    .fetch_one(owner)
    .await
    .expect("insert graph")
}

/// A team ocean plus its `teams` row; `member` is added when given.
async fn team(
    owner: &PgPool,
    account: Uuid,
    name: &str,
    creator: Uuid,
    member: Option<Uuid>,
) -> Uuid {
    let g = graph(owner, account, name, "team", None).await;
    let team_id: Uuid = sqlx::query_scalar(
        "INSERT INTO teams (account_id, graph_id, name, created_by) VALUES ($1, $2, $3, $4) RETURNING id",
    )
    .bind(account)
    .bind(g)
    .bind(name)
    .bind(creator)
    .fetch_one(owner)
    .await
    .unwrap();
    if let Some(u) = member {
        sqlx::query("INSERT INTO team_members (team_id, user_id, role) VALUES ($1, $2, 'member')")
            .bind(team_id)
            .bind(u)
            .execute(owner)
            .await
            .unwrap();
    }
    g
}

#[allow(clippy::too_many_arguments)]
async fn atom(
    owner: &PgPool,
    graph_id: Uuid,
    user: Uuid,
    source_id: &str,
    content: &str,
    created_at: DateTime<Utc>,
    retracted: bool,
) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO atoms(graph_id, source_type, source_id, content_blob_key, content_inline, ingested_by, \"timestamp\", created_at, evidence_retracted) \
         VALUES ($1, 'slack', $2, $3, $4, $5, $6, $6, $7) RETURNING atom_id",
    )
    .bind(graph_id)
    .bind(source_id)
    .bind(format!("blob-{source_id}"))
    .bind(content)
    .bind(user)
    .bind(created_at)
    .bind(retracted)
    .fetch_one(owner)
    .await
    .expect("insert atom")
}

async fn reef(
    owner: &PgPool,
    graph_id: Uuid,
    title: &str,
    created: DateTime<Utc>,
    updated: DateTime<Utc>,
    atoms: &[Uuid],
) -> Uuid {
    let node: Uuid = sqlx::query_scalar(
        "INSERT INTO nodes(graph_id, type, title, created_at, updated_at) VALUES ($1,'entity',$2,$3,$4) RETURNING node_id",
    )
    .bind(graph_id)
    .bind(title)
    .bind(created)
    .bind(updated)
    .fetch_one(owner)
    .await
    .unwrap();
    for a in atoms {
        sqlx::query("INSERT INTO node_atoms(node_id, atom_id) VALUES ($1,$2)")
            .bind(node)
            .bind(a)
            .execute(owner)
            .await
            .unwrap();
    }
    node
}

#[tokio::test]
async fn flag_off_is_404_params_are_400_and_auth_is_required() {
    let _env = ENV_LOCK.lock().await;
    let Some(probe) = RlsProbe::setup("orgt_wsa_flag").await else {
        eprintln!(
            "skipping flag_off_is_404_params_are_400_and_auth_is_required: set ORGT_TEST_BASE_URL"
        );
        return;
    };
    let jwks = spawn_local_jwks().await;
    let a = seed_caller(&probe.owner, "flag").await;
    let app = app_over(&probe);

    {
        let _g = EnvGuard::set(&jwks, None);
        let (status, _) = call(&app, &probe.app_pool, "/v1/home/workspace-activity", &a.token).await;
        assert_eq!(
            status,
            StatusCode::NOT_FOUND,
            "flag unset must hide the route"
        );
    }
    {
        let _g = EnvGuard::set(&jwks, Some("1"));
        for uri in [
            "/v1/home/workspace-activity?limit=0",
            "/v1/home/workspace-activity?limit=21",
            "/v1/home/workspace-activity?limit=-3",
            "/v1/home/workspace-activity?limit=abc",
            "/v1/home/workspace-activity?since_days=0",
            "/v1/home/workspace-activity?since_days=31",
        ] {
            let (status, body) = call(&app, &probe.app_pool, uri, &a.token).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{uri}: {body}");
        }
        let (status, body) = call(
            &app,
            &probe.app_pool,
            "/v1/home/workspace-activity?limit=6&since_days=7",
            &a.token,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["workspaces"], json!([]));
        let resp = app
            .clone()
            .oneshot(get("/v1/home/workspace-activity", None))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }
    probe.teardown().await;
}

#[tokio::test]
async fn headlines_ordering_limit_and_isolation() {
    let _env = ENV_LOCK.lock().await;
    let Some(probe) = RlsProbe::setup("orgt_wsa_happy").await else {
        eprintln!("skipping headlines_ordering_limit_and_isolation: set ORGT_TEST_BASE_URL");
        return;
    };
    let jwks = spawn_local_jwks().await;
    let _g = EnvGuard::set(&jwks, Some("true"));
    let owner = &probe.owner;

    let a = seed_caller(owner, "alpha").await;
    // Another member of the SAME account, with their own private ocean.
    let b = seed_caller(owner, "bravo").await;
    sqlx::query(
        "INSERT INTO account_members (account_id, user_id, role) VALUES ($1, $2, 'member')",
    )
    .bind(a.account_id)
    .bind(b.user_id)
    .execute(owner)
    .await
    .unwrap();
    // A stranger's account with its own shared ocean.
    let c = seed_caller(owner, "charlie").await;

    let now = Utc::now();
    let ago = |h: i64| now - Duration::hours(h);
    let days = |d: i64| now - Duration::days(d);

    // Shared oceans A can read.
    let org = graph(owner, a.account_id, "Engineering", "org", None).await;
    let eu = team(owner, a.account_id, "EU launch", a.user_id, Some(a.user_id)).await;
    let quiet = team(
        owner,
        a.account_id,
        "Quiet team",
        a.user_id,
        Some(a.user_id),
    )
    .await;
    let bare = team(owner, a.account_id, "Bare team", a.user_id, Some(a.user_id)).await;
    // Not readable by A: a team A is not on, A's and B's private oceans, and a stranger's ocean.
    let other_team = team(owner, a.account_id, "Other team", b.user_id, None).await;
    let mine = graph(owner, a.account_id, "My Ocean", "member", Some(a.user_id)).await;
    let theirs = graph(owner, a.account_id, "B's Ocean", "member", Some(b.user_id)).await;
    let stranger = graph(owner, c.account_id, "Stranger org", "org", None).await;

    // Engineering: busiest VISIBLE reef is old ("Auth rewrite", 2 corals); a hidden
    // reef (title appears in no coral) is busier and must never be named.
    let e1 = atom(
        owner,
        org,
        a.user_id,
        "eng-1",
        "Auth rewrite: token refresh moved",
        ago(30),
        false,
    )
    .await;
    let e2 = atom(
        owner,
        org,
        a.user_id,
        "eng-2",
        "Auth rewrite: session store",
        ago(26),
        false,
    )
    .await;
    let e3 = atom(
        owner,
        org,
        a.user_id,
        "eng-3",
        "unrelated chatter",
        ago(28),
        false,
    )
    .await;
    let e4 = atom(
        owner,
        org,
        a.user_id,
        "eng-4",
        "more chatter",
        ago(27),
        false,
    )
    .await;
    atom(
        owner,
        org,
        a.user_id,
        "eng-retracted",
        "Auth rewrite: retracted",
        ago(1),
        true,
    )
    .await;
    atom(
        owner,
        org,
        a.user_id,
        "eng-old",
        "Auth rewrite: ancient",
        days(30),
        false,
    )
    .await;
    reef(owner, org, "Auth rewrite", days(60), ago(26), &[e1, e2]).await;
    reef(owner, org, "Secret plan", days(60), ago(26), &[e1, e3, e4]).await;

    // EU launch: a reef created inside the window with 3 corals; newest activity overall.
    let u1 = atom(
        owner,
        eu,
        a.user_id,
        "eu-1",
        "EU launch plan: scope",
        ago(5),
        false,
    )
    .await;
    let u2 = atom(
        owner,
        eu,
        a.user_id,
        "eu-2",
        "EU launch plan: dates",
        ago(4),
        false,
    )
    .await;
    let u3 = atom(
        owner,
        eu,
        a.user_id,
        "eu-3",
        "EU launch plan: owners",
        ago(3),
        false,
    )
    .await;
    reef(owner, eu, "EU launch plan", days(1), ago(3), &[u1, u2, u3]).await;

    // Quiet team: activity only outside the 7-day window.
    atom(owner, quiet, a.user_id, "q-1", "old news", days(20), false).await;

    // Bare team: corals but no reef at all.
    atom(
        owner,
        bare,
        a.user_id,
        "b-1",
        "loose note one",
        ago(60),
        false,
    )
    .await;
    atom(
        owner,
        bare,
        a.user_id,
        "b-2",
        "loose note two",
        ago(59),
        false,
    )
    .await;

    // Everything below must never show.
    for (g, who, tag) in [
        (other_team, b.user_id, "ot"),
        (mine, a.user_id, "mine"),
        (theirs, b.user_id, "theirs"),
        (stranger, c.user_id, "st"),
    ] {
        let x = atom(
            owner,
            g,
            who,
            &format!("{tag}-1"),
            "Auth rewrite: private",
            ago(1),
            false,
        )
        .await;
        reef(owner, g, "Auth rewrite", days(1), ago(1), &[x]).await;
    }

    let app = app_over(&probe);
    let (status, body) = call(&app, &probe.app_pool, "/v1/home/workspace-activity", &a.token).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let rows = body["workspaces"].as_array().unwrap();
    let names: Vec<&str> = rows.iter().map(|r| r["name"].as_str().unwrap()).collect();
    assert_eq!(
        names,
        ["EU launch", "Engineering", "Bare team"],
        "newest first; quiet and private excluded: {body}"
    );

    let eu_row = &rows[0];
    assert_eq!(eu_row["graph_id"], json!(eu));
    assert_eq!(eu_row["kind"], "team");
    assert_eq!(eu_row["total_corals_window"], 3);
    assert_eq!(eu_row["headline"]["kind"], "reef_added");
    assert_eq!(eu_row["headline"]["reef"]["title"], "EU launch plan");
    assert_eq!(eu_row["headline"]["coral_count"], 3);

    let eng = &rows[1];
    assert_eq!(eng["kind"], "org");
    assert_eq!(
        eng["total_corals_window"], 4,
        "retracted and out-of-window corals do not count: {body}"
    );
    assert_eq!(eng["headline"]["kind"], "reef_updated");
    assert_eq!(eng["headline"]["reef"]["title"], "Auth rewrite");
    assert_eq!(eng["headline"]["coral_count"], 2);
    assert!(
        !body.to_string().contains("Secret plan"),
        "a hidden title must never be named: {body}"
    );

    let bare_row = &rows[2];
    assert_eq!(bare_row["headline"]["kind"], "corals_added");
    assert!(bare_row["headline"]["reef"].is_null());
    assert_eq!(bare_row["headline"]["coral_count"], 2);

    // Isolation: none of the unreadable or private oceans leaks, by id or by content.
    let text = body.to_string();
    for (g, label) in [
        (other_team, "other team"),
        (mine, "own private"),
        (theirs, "member private"),
        (stranger, "stranger"),
        (quiet, "quiet"),
    ] {
        assert!(
            !text.contains(&g.to_string()),
            "{label} ocean leaked: {body}"
        );
    }
    assert!(!text.contains("private"));

    // since_days widens the window to include the quiet team; limit truncates after ordering.
    let (_, wide) = call(&app, &probe.app_pool, "/v1/home/workspace-activity?since_days=30", &a.token).await;
    assert!(wide["workspaces"]
        .as_array()
        .unwrap()
        .iter()
        .any(|r| r["graph_id"] == json!(quiet)));
    let (_, one) = call(&app, &probe.app_pool, "/v1/home/workspace-activity?limit=1", &a.token).await;
    assert_eq!(one["workspaces"].as_array().unwrap().len(), 1);
    assert_eq!(one["workspaces"][0]["graph_id"], json!(eu));

    // The other member of the same account does not see A's shared oceans through
    // their own (personal-plane) session, and never A's private ocean.
    let (_, theirs_body) = call(&app, &probe.app_pool, "/v1/home/workspace-activity", &b.token).await;
    let t = theirs_body.to_string();
    assert!(!t.contains(&mine.to_string()), "{theirs_body}");
    assert!(
        !t.contains(&theirs.to_string()),
        "B's own private ocean is never a workspace row: {theirs_body}"
    );

    // The stranger sees none of A's oceans.
    let (_, stranger_body) = call(&app, &probe.app_pool, "/v1/home/workspace-activity", &c.token).await;
    let s = stranger_body.to_string();
    for g in [org, eu, bare, mine, theirs] {
        assert!(!s.contains(&g.to_string()), "{stranger_body}");
    }
    probe.teardown().await;
}

/// `ACTIVITY_SQL` as written in the service (the raw string literal).
fn activity_sql() -> &'static str {
    let src = include_str!("../src/services/workspace_activity.rs");
    let start = src.find("ACTIVITY_SQL: &str = r#\"").expect("sql const")
        + "ACTIVITY_SQL: &str = r#\"".len();
    let end = start + src[start..].find("\"#;").unwrap();
    &src[start..end]
}

/// Volume check, not part of the normal run: seeds a busy org ocean (96k atoms,
/// 24k of them in the last 30 days) and a busy team ocean, then prints the
/// plan and the route latency. The numbers are in
/// docs/implementation/workspace-activity-validation.md.
///
///   ORGT_TEST_BASE_URL=... cargo test -p coralswarm-backend \
///     --test workspace_activity_pg explain_at_volume -- --ignored --nocapture
#[tokio::test]
#[ignore]
async fn explain_at_volume() {
    let _env = ENV_LOCK.lock().await;
    let Some(probe) = RlsProbe::setup("orgt_wsa_volume").await else {
        eprintln!("skipping explain_at_volume: set ORGT_TEST_BASE_URL");
        return;
    };
    let jwks = spawn_local_jwks().await;
    let _g = EnvGuard::set(&jwks, Some("1"));
    let owner = &probe.owner;
    let a = seed_caller(owner, "vol").await;
    let org = graph(owner, a.account_id, "Busy org", "org", None).await;
    let tm = team(owner, a.account_id, "Busy team", a.user_id, Some(a.user_id)).await;

    // 96k org atoms spread over 120 days (24k inside 30 days), 32k team atoms over
    // 120 days. Inserted in small statements: the capture-cleanup trigger takes an
    // advisory lock per row, which a single huge transaction would exhaust.
    for (g, n) in [(org, 96_000i64), (tm, 32_000i64)] {
        let mut lo = 1i64;
        while lo <= n {
            let hi = (lo + 199).min(n);
            sqlx::query(
                "INSERT INTO atoms(graph_id, source_type, source_id, content_blob_key, content_inline, ingested_by, \"timestamp\", created_at) \
                 SELECT $1, 'slack', 'v-' || i, 'blob-' || i, 'Reef ' || (i % 400) || ' note ' || i, $2, \
                        now() - (i::float8 / $3) * interval '120 days', now() - (i::float8 / $3) * interval '120 days' \
                 FROM generate_series($4::bigint, $5::bigint) i",
            )
            .bind(g)
            .bind(a.user_id)
            .bind(n as f64)
            .bind(lo)
            .bind(hi)
            .execute(owner)
            .await
            .unwrap();
            lo = hi + 1;
        }
    }
    // 400 reefs per ocean; each links the atoms with i % 400 = its index.
    for g in [org, tm] {
        sqlx::query(
            "INSERT INTO nodes(graph_id, type, title, created_at, updated_at) \
             SELECT $1, 'entity', 'Reef ' || k, now() - interval '200 days', now() - (k || ' hours')::interval \
             FROM generate_series(0, 399) k",
        )
        .bind(g)
        .execute(owner)
        .await
        .unwrap();
        let ids: Vec<Uuid> = sqlx::query_scalar("SELECT atom_id FROM atoms WHERE graph_id = $1")
            .bind(g)
            .fetch_all(owner)
            .await
            .unwrap();
        for chunk in ids.chunks(2000) {
            sqlx::query(
                "INSERT INTO node_atoms(node_id, atom_id) \
                 SELECT n.node_id, a.atom_id FROM atoms a \
                 JOIN nodes n ON n.graph_id = a.graph_id \
                  AND n.title = 'Reef ' || (substring(a.source_id from 3)::bigint % 400) \
                 WHERE a.atom_id = ANY($1)",
            )
            .bind(chunk)
            .execute(owner)
            .await
            .unwrap();
        }
    }
    sqlx::query("ANALYZE atoms").execute(owner).await.unwrap();
    sqlx::query("ANALYZE node_atoms")
        .execute(owner)
        .await
        .unwrap();
    sqlx::query("ANALYZE nodes").execute(owner).await.unwrap();

    let now = Utc::now();
    for days in [7i64, 30] {
        let plan: Vec<(String,)> =
            sqlx::query_as(&format!("EXPLAIN (ANALYZE, BUFFERS) {}", activity_sql()))
                .bind(vec![org, tm])
                .bind(now - Duration::days(days))
                .bind(now)
                .bind(5000i64)
                .bind(8i64)
                .fetch_all(owner)
                .await
                .unwrap();
        eprintln!("=== EXPLAIN (ANALYZE, BUFFERS), since_days={days} ===");
        for (l,) in plan {
            eprintln!("{l}");
        }
    }

    let app = app_over(&probe);
    for days in [7, 30] {
        let t = std::time::Instant::now();
        let (status, body) = call(
            &app,
            &probe.app_pool,
            &format!("/v1/home/workspace-activity?since_days={days}"),
            &a.token,
        )
        .await;
        eprintln!("route since_days={days}: {status} in {:?}", t.elapsed());
        assert_eq!(status, StatusCode::OK, "{body}");
        eprintln!("{body}");
    }
    probe.teardown().await;
}
