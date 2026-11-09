//! Route-level tests for `GET /v1/home/metrics` against the REAL production
//! router (`test_support_app`) served off a `coralswarm_app` login pool, the
//! same harness as `journal_endpoint_pg.rs`: Clerk-shaped JWTs against a
//! local JWKS listener, `begin_member_scoped` as in production.
//!
//! Covers: the flag (404 off), parameter validation, the KPI/bucket/group
//! arithmetic over seeded rows (including the prior window, chunk collapse
//! for meetings, retracted corals and sub-agent sessions being excluded), the
//! per-caller `questions_asked`, and cross-account isolation.
//!
//! Env-gated on `ORGT_TEST_BASE_URL` like every other `*_pg.rs` test:
//!
//!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test home_metrics_pg

mod support;

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request as HttpRequest, StatusCode};
use axum::Router;
use chrono::{Duration, Utc};
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

fn get(uri: &str, token: &str) -> HttpRequest<Body> {
    HttpRequest::builder()
        .uri(uri)
        .header("Authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap()
}

fn app_over(probe: &RlsProbe) -> Router {
    let mut state: AppState = (*coralswarm_backend::test_app_state()).clone();
    state.db = Some(probe.app_pool.clone());
    state.store = Some(Arc::new(PostgresStore::new(probe.app_pool.clone())));
    coralswarm_backend::test_support_app(Arc::new(state))
}

struct Caller {
    user_id: Uuid,
    account_id: Uuid,
    graph_id: Uuid,
    token: String,
}

async fn seed_caller(owner: &PgPool, tag: &str) -> Caller {
    let sub = format!("home_m_{tag}_{}", Uuid::new_v4().simple());
    let user_id: Uuid =
        sqlx::query_scalar("INSERT INTO users (email, clerk_user_id) VALUES ($1, $2) RETURNING id")
            .bind(format!("{sub}@home-metrics.example"))
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
            .bind(format!("{tag}-ocean"))
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

async fn seed_atom(
    owner: &PgPool,
    graph_id: Uuid,
    user: Uuid,
    source_type: &str,
    source_id: &str,
    created_at: chrono::DateTime<Utc>,
    retracted: bool,
) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO atoms(graph_id, source_type, source_id, content_blob_key, ingested_by, \"timestamp\", created_at, evidence_retracted) \
         VALUES ($1, $2, $3, $4, $5, $6, $6, $7) RETURNING atom_id",
    )
    .bind(graph_id)
    .bind(source_type)
    .bind(source_id)
    .bind(format!("blob-{source_id}"))
    .bind(user)
    .bind(created_at)
    .bind(retracted)
    .fetch_one(owner)
    .await
    .expect("insert atom")
}

async fn call(app: &Router, pool:&PgPool, uri: &str, token: &str) -> (StatusCode, Value) {
    support::read_models::stored_call(app,pool,uri,token).await
}

#[tokio::test]
async fn flag_off_is_404_and_bad_params_are_400() {
    let _env = ENV_LOCK.lock().await;
    let Some(probe) = RlsProbe::setup("orgt_home_metrics_flag").await else {
        eprintln!("skipping flag_off_is_404_and_bad_params_are_400: set ORGT_TEST_BASE_URL");
        return;
    };
    let jwks = spawn_local_jwks().await;
    let a = seed_caller(&probe.owner, "flag").await;
    let app = app_over(&probe);

    {
        let _g = EnvGuard::set(&jwks, None);
        let (status, _) = call(&app, &probe.app_pool, "/v1/home/metrics", &a.token).await;
        assert_eq!(
            status,
            StatusCode::NOT_FOUND,
            "flag unset must hide the route"
        );
    }
    {
        let _g = EnvGuard::set(&jwks, Some("1"));
        for uri in [
            "/v1/home/metrics?days=6",
            "/v1/home/metrics?days=61",
            "/v1/home/metrics?days=-3",
            "/v1/home/metrics?days=abc",
            "/v1/home/metrics?tz=Mars/Base",
        ] {
            let (status, body) = call(&app, &probe.app_pool, uri, &a.token).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{uri}: {body}");
        }
        let (status, body) =
            call(&app, &probe.app_pool, "/v1/home/metrics?days=7&tz=America/Chicago", &a.token).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["window"]["days"], 7);
        assert_eq!(body["window"]["tz"], "America/Chicago");
        assert_eq!(
            body["kpis"]["corals_ingested"]["daily"]
                .as_array()
                .unwrap()
                .len(),
            7
        );
        assert_eq!(body["ingest_by_day"].as_array().unwrap().len(), 7);
        assert_eq!(body["kpis"]["corals_ingested"]["total"], 0);
    }
    probe.teardown().await;
}

#[tokio::test]
async fn counts_buckets_groups_and_isolates() {
    let _env = ENV_LOCK.lock().await;
    let Some(probe) = RlsProbe::setup("orgt_home_metrics_happy").await else {
        eprintln!("skipping counts_buckets_groups_and_isolates: set ORGT_TEST_BASE_URL");
        return;
    };
    let jwks = spawn_local_jwks().await;
    let _g = EnvGuard::set(&jwks, Some("true"));
    let owner = &probe.owner;

    let a = seed_caller(owner, "alpha").await;
    let b = seed_caller(owner, "bravo").await;

    // Mid-day offsets keep each row away from a UTC day edge.
    let now = Utc::now();
    let midday = |days_ago: i64| {
        (now - Duration::days(days_ago))
            .date_naive()
            .and_hms_opt(12, 0, 0)
            .unwrap()
            .and_utc()
    };

    // Current window: 2 slack + 1 github_pr today-ish, 1 retracted slack (must not count).
    let s1 = seed_atom(
        owner,
        a.graph_id,
        a.user_id,
        "slack",
        "C1:1",
        midday(1),
        false,
    )
    .await;
    seed_atom(
        owner,
        a.graph_id,
        a.user_id,
        "slack",
        "C1:2",
        midday(1),
        false,
    )
    .await;
    seed_atom(
        owner,
        a.graph_id,
        a.user_id,
        "github_pr",
        "pr-9",
        midday(3),
        false,
    )
    .await;
    seed_atom(
        owner,
        a.graph_id,
        a.user_id,
        "slack",
        "C1:retracted",
        midday(1),
        true,
    )
    .await;
    // One meeting = two chunks of one transcript; its screen channel is not counted.
    seed_atom(
        owner,
        a.graph_id,
        a.user_id,
        "transcript",
        "mtg-1:chunk:0",
        midday(2),
        false,
    )
    .await;
    seed_atom(
        owner,
        a.graph_id,
        a.user_id,
        "transcript",
        "mtg-1:chunk:1",
        midday(2),
        false,
    )
    .await;
    // A meeting first captured long before the window, with one more chunk inside it:
    // a coral, but not a newly captured meeting.
    seed_atom(
        owner,
        a.graph_id,
        a.user_id,
        "transcript",
        "mtg-old:chunk:0",
        midday(100),
        false,
    )
    .await;
    seed_atom(
        owner,
        a.graph_id,
        a.user_id,
        "transcript",
        "mtg-old:chunk:1",
        midday(2),
        false,
    )
    .await;
    seed_atom(
        owner,
        a.graph_id,
        a.user_id,
        "meeting_session",
        "cc:mtg-1:ckpt",
        midday(2),
        false,
    )
    .await;
    // Prior window (days 31..59 ago for the default 30).
    seed_atom(
        owner,
        a.graph_id,
        a.user_id,
        "slack",
        "C1:old",
        midday(40),
        false,
    )
    .await;
    // Outside both windows.
    seed_atom(
        owner,
        a.graph_id,
        a.user_id,
        "slack",
        "C1:ancient",
        midday(200),
        false,
    )
    .await;
    // Another account's coral must never show up.
    seed_atom(
        owner,
        b.graph_id,
        b.user_id,
        "slack",
        "B:1",
        midday(1),
        false,
    )
    .await;

    // One reef linked to a permitted coral; one linked only to the retracted one.
    let node: Uuid = sqlx::query_scalar(
        "INSERT INTO nodes(graph_id, type, title, created_at) VALUES ($1,'entity','Reef A',$2) RETURNING node_id",
    )
    .bind(a.graph_id)
    .bind(midday(1))
    .fetch_one(owner)
    .await
    .unwrap();
    sqlx::query("INSERT INTO node_atoms(node_id, atom_id) VALUES ($1,$2)")
        .bind(node)
        .bind(s1)
        .execute(owner)
        .await
        .unwrap();

    // Questions: two by the caller, one by someone else in the same ocean.
    for (user, ago) in [(a.user_id, 1), (a.user_id, 2), (b.user_id, 1)] {
        sqlx::query("INSERT INTO query_history(account_id, graph_id, user_id, query_text, created_at) VALUES ($1,$2,$3,'q',$4)")
            .bind(a.account_id)
            .bind(a.graph_id)
            .bind(user)
            .bind(midday(ago))
            .execute(owner)
            .await
            .unwrap();
    }

    sqlx::query("INSERT INTO query_history(account_id, graph_id, user_id, query_text, created_at) VALUES ($1,$2,$3,'q',$4)")
        .bind(b.account_id)
        .bind(b.graph_id)
        .bind(b.user_id)
        .bind(midday(1))
        .execute(owner)
        .await
        .unwrap();

    // Sessions: one root coding session, one sub-agent (excluded), one meeting kind (excluded).
    for (sid, kind, parent) in [
        ("s-root", "coding", None),
        ("s-child", "agent", Some("s-root")),
        ("s-mtg", "meeting", None),
    ] {
        sqlx::query(
            "INSERT INTO coding_sessions(graph_id, session_id, session_kind, parent_session_id, started_at) VALUES ($1,$2,$3,$4,$5)",
        )
        .bind(a.graph_id)
        .bind(sid)
        .bind(kind)
        .bind(parent)
        .bind(midday(1))
        .execute(owner)
        .await
        .unwrap();
    }

    let app = app_over(&probe);
    let (status, body) = call(&app, &probe.app_pool, "/v1/home/metrics", &a.token).await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let k = &body["kpis"];
    // slack x2 + github_pr + transcript x3 + meeting_session = 7; retracted excluded.
    assert_eq!(k["corals_ingested"]["total"], 7, "{body}");
    assert_eq!(k["corals_ingested"]["prior_total"], 1, "{body}");
    let daily: Vec<i64> = k["corals_ingested"]["daily"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_i64().unwrap())
        .collect();
    assert_eq!(daily.len(), 30);
    assert_eq!(daily.iter().sum::<i64>(), 7);
    assert_eq!(
        k["meetings_captured"]["total"], 1,
        "two chunks of one transcript = one meeting; one first seen before the window is not new"
    );
    assert_eq!(k["reefs_formed"]["total"], 1);
    assert_eq!(
        k["questions_asked"]["total"], 2,
        "only the caller's own questions"
    );
    assert_eq!(
        k["agent_sessions"]["total"], 1,
        "root coding/agent sessions only"
    );

    // Grouping: chat 2, code_tickets 1, meetings 4.
    let totals = &body["source_type_totals"];
    assert_eq!(
        (
            totals["chat"].as_i64(),
            totals["code_tickets"].as_i64(),
            totals["meetings"].as_i64()
        ),
        (Some(2), Some(1), Some(4))
    );
    assert_eq!(totals["agent_sessions"], 0);

    // One ocean, the caller's; the other account's ocean is invisible.
    let oceans = body["oceans"].as_array().unwrap();
    assert_eq!(oceans.len(), 1, "{body}");
    assert_eq!(oceans[0]["graph_id"], json!(a.graph_id));
    assert_eq!(oceans[0]["total"], 7);
    assert!(!body.to_string().contains(&b.graph_id.to_string()));

    // The matrix agrees with the totals.
    let matrix_total: i64 = body["ingest_by_day"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|d| d["by_graph"].as_array().unwrap().iter())
        .flat_map(|g| g["by_source_type"].as_object().unwrap().values())
        .map(|v| v.as_i64().unwrap())
        .sum();
    assert_eq!(matrix_total, 7);

    // The other caller sees only their own coral.
    let (_, other) = call(&app, &probe.app_pool, "/v1/home/metrics", &b.token).await;
    assert_eq!(other["kpis"]["corals_ingested"]["total"], 1, "{other}");
    assert_eq!(other["kpis"]["questions_asked"]["total"], 1);

    probe.teardown().await;
}

/// The `reefs_formed` admission (`reefs_sql`, rewritten for the 2026-11-02
/// Home incident as one `LIMIT 1` probe per reef) must admit exactly the
/// reefs `library_node_permitted` admits: a semantic link to a served source,
/// or a legacy link to a permitted atom of the reef's OWN ocean. Compared one
/// reef at a time, under the serving role's account scope, over a matrix
/// that exercises every branch, including a link to another ocean's atom.
///
/// The matrix includes reefs whose only link is a retracted or derived-copy
/// coral (refused: "retracted, derived-copy and denied-source atoms are
/// out"), so the comparison also proves the two rules agree on those.
#[tokio::test]
async fn reef_admission_matches_library_node_permitted_reef_by_reef() {
    let _env = ENV_LOCK.lock().await;
    let Some(probe) = RlsProbe::setup("orgt_home_metrics_reef_admission").await else {
        eprintln!("skipping reef_admission_matches_library_node_permitted_reef_by_reef: set ORGT_TEST_BASE_URL");
        return;
    };
    let owner = &probe.owner;
    let a = seed_caller(owner, "admit").await;
    let second: Uuid =
        sqlx::query_scalar("INSERT INTO graphs (account_id, name) VALUES ($1, 'admit-second') RETURNING id")
            .bind(a.account_id)
            .fetch_one(owner)
            .await
            .unwrap();
    let outsider = seed_caller(owner, "outsider").await;
    let now = Utc::now();
    let at = now - Duration::days(1);

    let atom = |graph: Uuid, id: &'static str, retracted: bool| seed_atom(owner, graph, a.user_id, "slack", id, at, retracted);
    let permitted = atom(a.graph_id, "A:permitted", false).await;
    let retracted = atom(a.graph_id, "A:retracted", true).await;
    let derived = atom(a.graph_id, "A:derived", false).await;
    sqlx::query("UPDATE atoms SET derived_copy_bound = true WHERE atom_id = $1")
        .bind(derived)
        .execute(owner)
        .await
        .unwrap();
    let bound_ok = atom(a.graph_id, "A:bound-ok", false).await;
    let bound_denied = atom(a.graph_id, "A:bound-denied", false).await;
    let foreign = atom(second, "S:permitted", false).await;
    let outsiders = atom(outsider.graph_id, "O:permitted", false).await;

    // Source items and versions: one permitted, one restricted, in ocean A.
    async fn source(owner: &PgPool, graph: Uuid, id: &str, state: &str) -> Uuid {
        let item = Uuid::new_v4();
        let version = Uuid::new_v4();
        sqlx::query("INSERT INTO source_items(source_item_id, graph_id, source_type, provider_namespace, source_id, access_state) VALUES ($1,$2,'slack','test',$3,$4)")
            .bind(item).bind(graph).bind(id).bind(state).execute(owner).await.unwrap();
        sqlx::query("INSERT INTO source_versions(version_id, graph_id, source_id, source_item_id, source_type, provider_namespace, captured_at) VALUES ($1,$2,$3,$4,'slack','test',now())")
            .bind(version).bind(graph).bind(id).bind(item).execute(owner).await.unwrap();
        sqlx::query("UPDATE source_items SET current_version_id = $1 WHERE source_item_id = $2")
            .bind(version).bind(item).execute(owner).await.unwrap();
        version
    }
    let v_ok = source(owner, a.graph_id, "src-ok", "permitted").await;
    let v_denied = source(owner, a.graph_id, "src-denied", "restricted").await;
    for (version, atom) in [(v_ok, bound_ok), (v_denied, bound_denied)] {
        sqlx::query("INSERT INTO source_version_chunks(graph_id, source_version_id, atom_id) VALUES ($1,$2,$3)")
            .bind(a.graph_id).bind(version).bind(atom).execute(owner).await.unwrap();
    }
    async fn semantic(owner: &PgPool, graph: Uuid, version: Uuid) -> Uuid {
        let id = Uuid::new_v4();
        sqlx::query("INSERT INTO semantic_atoms(semantic_atom_id, graph_id, source_version_id, ordinal, kind, statement, extraction_version, model, entities) VALUES ($1,$2,$3,1,'decision','we decided','source-atoms-v1','test','[]')")
            .bind(id).bind(graph).bind(version).execute(owner).await.unwrap();
        id
    }
    let sem_ok = semantic(owner, a.graph_id, v_ok).await;
    let sem_denied = semantic(owner, a.graph_id, v_denied).await;

    async fn reef(owner: &PgPool, graph: Uuid, title: &str, at: chrono::DateTime<Utc>) -> Uuid {
        sqlx::query_scalar("INSERT INTO nodes(graph_id, type, title, created_at) VALUES ($1,'entity',$2,$3) RETURNING node_id")
            .bind(graph).bind(title).bind(at).fetch_one(owner).await.unwrap()
    }
    async fn link(owner: &PgPool, node: Uuid, atom: Uuid) {
        sqlx::query("INSERT INTO node_atoms(node_id, atom_id) VALUES ($1,$2)")
            .bind(node).bind(atom).execute(owner).await.unwrap();
    }
    async fn link_semantic(owner: &PgPool, graph: Uuid, node: Uuid, semantic: Uuid) {
        sqlx::query("INSERT INTO node_semantic_atoms(node_id, semantic_atom_id, graph_id) VALUES ($1,$2,$3)")
            .bind(node).bind(semantic).bind(graph).execute(owner).await.unwrap();
    }

    // (name, reef, admitted by the Library rule)
    let mut matrix: Vec<(&str, Uuid, bool)> = Vec::new();
    let n = reef(owner, a.graph_id, "legacy permitted", at).await;
    link(owner, n, permitted).await;
    matrix.push(("legacy link to a permitted coral", n, true));
    let n = reef(owner, a.graph_id, "legacy retracted only", at).await;
    link(owner, n, retracted).await;
    matrix.push(("only link is a retracted coral", n, false));
    let n = reef(owner, a.graph_id, "legacy derived only", at).await;
    link(owner, n, derived).await;
    matrix.push(("only link is a derived copy", n, false));
    let n = reef(owner, a.graph_id, "no links", at).await;
    matrix.push(("no links at all", n, false));
    let n = reef(owner, a.graph_id, "semantic served", at).await;
    link_semantic(owner, a.graph_id, n, sem_ok).await;
    matrix.push(("semantic link to a served source", n, true));
    let n = reef(owner, a.graph_id, "semantic denied", at).await;
    link_semantic(owner, a.graph_id, n, sem_denied).await;
    matrix.push(("semantic link to a restricted source", n, false));
    let n = reef(owner, a.graph_id, "bound permitted", at).await;
    link(owner, n, bound_ok).await;
    matrix.push(("legacy link to a bound coral of a permitted source", n, true));
    let n = reef(owner, a.graph_id, "bound denied", at).await;
    link(owner, n, bound_denied).await;
    matrix.push(("legacy link to a bound coral of a restricted source", n, false));
    let n = reef(owner, a.graph_id, "cross ocean", at).await;
    link(owner, n, foreign).await;
    matrix.push(("only link is to another ocean's permitted coral", n, false));
    let n = reef(owner, a.graph_id, "retracted plus served semantic", at).await;
    link(owner, n, retracted).await;
    link_semantic(owner, a.graph_id, n, sem_ok).await;
    matrix.push(("retracted legacy link but a served semantic link", n, true));
    let n = reef(owner, second, "second ocean own coral", at).await;
    link(owner, n, foreign).await;
    matrix.push(("second ocean: legacy link to its own permitted coral", n, true));
    let n = reef(owner, second, "second ocean foreign coral", at).await;
    link(owner, n, permitted).await;
    matrix.push(("second ocean: only link is ocean A's coral", n, false));
    let n = reef(owner, a.graph_id, "outside the window", now - Duration::days(200)).await;
    link(owner, n, permitted).await;
    let outside_window = n;
    let n = reef(owner, outsider.graph_id, "outsider", at).await;
    link(owner, n, outsiders).await;
    let outsider_reef = n;

    let graphs = vec![a.graph_id, second];
    let from = now - Duration::days(60);
    let to = now + Duration::days(1);
    let bounds = vec![from, to];
    let prior_cut = now - Duration::days(30);
    let start = now - Duration::days(29);
    let mut tx = probe.scoped_tx(a.account_id).await;
    let admitted: Vec<Option<Uuid>> =
        sqlx::query_scalar(&coralswarm_backend::services::reefs_admitted_sql())
            .bind(&graphs)
            .bind(from)
            .bind(to)
            .bind(&bounds)
            .bind(prior_cut)
            .bind(start)
            .fetch_all(&mut *tx)
            .await
            .expect("reefs admission under the serving role");
    let admitted: std::collections::BTreeSet<Uuid> = admitted.into_iter().flatten().collect();
    let reference: std::collections::BTreeSet<Uuid> = sqlx::query_scalar(
        "SELECT n.node_id FROM nodes n WHERE n.graph_id = ANY($1) AND n.created_at >= $2 AND n.created_at < $3 AND library_node_permitted(n.graph_id, n.node_id)",
    )
    .bind(&graphs)
    .bind(from)
    .bind(to)
    .fetch_all(&mut *tx)
    .await
    .expect("library_node_permitted under the serving role")
    .into_iter()
    .collect();
    tx.rollback().await.ok();

    for (name, node, expected) in &matrix {
        assert_eq!(admitted.contains(node), *expected, "reefs_sql: {name}");
        assert_eq!(reference.contains(node), *expected, "library_node_permitted: {name}");
    }
    assert!(!admitted.contains(&outside_window) && !reference.contains(&outside_window));
    assert!(!admitted.contains(&outsider_reef) && !reference.contains(&outsider_reef), "another account's reef");
    assert_eq!(admitted, reference, "reef-by-reef admission over every windowed reef in scope");
    assert_eq!(admitted.len(), matrix.iter().filter(|m| m.2).count());

    probe.teardown().await;
}
