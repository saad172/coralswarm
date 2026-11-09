//! Composio capture at scale against a local mock broker that serves Linear and
//! Jira recipes with realistic pages and injected per-call latency. The pinned
//! test asserts the broker calls a poll costs (the cost model's inputs) and the
//! per-tick poll cap; the ignored bench sweeps N captures and reports tick wall
//! time, broker calls, DB statements and DB time. See
//! `docs/connectors/composio-scale-and-cost.md` for the method and results.
//!
//! Bench knobs (all optional): `COMPOSIO_SCALE_N` (`10,100,1000,10000`),
//! `COMPOSIO_SCALE_LATENCY_MS` (`300-800`), `COMPOSIO_SCALE_ACTIVE` (`0.2`),
//! `COMPOSIO_SCALE_TICKS` (`8`), `COMPOSIO_SCALE_REPEATS` (`5`). DB statement
//! counts need the server started with `shared_preload_libraries=pg_stat_statements`;
//! without it they print as n/a.
mod support;
use axum::{
    extract::{Path, Query, State},
    routing::{delete, post},
    Json,
};
use chrono::Utc;
use coralswarm_backend::{
    tenancy::extractor::Tenant, test_composio as composio, test_composio_recipe as recipe,
    workers::composio_capture, TestComposioClient as Client, UserContext,
};
use futures::StreamExt;
use serde_json::{json, Value};
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    PgPool,
};
use std::{
    collections::HashMap,
    str::FromStr,
    sync::{
        atomic::{AtomicI64, AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};
use uuid::Uuid;

const GROUPS: usize = 4;
/// Items on a full change page (the engine's `PAGE_SIZE`) and on the last one.
const FULL_PAGE: usize = 25;
const LAST_PAGE: usize = 5;
const COMMENTS_PER_ISSUE: usize = 2;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Call {
    Namespace,
    Stream,
    Child,
}

#[derive(Default)]
struct Counts {
    calls: HashMap<(&'static str, Call), u64>,
    polls_started: u64,
    handler: Duration,
    injected: Duration,
    /// When each call arrived, for calls per minute at peak.
    stamps: Vec<Instant>,
    max_in_flight: i64,
    /// Connected accounts deleted with provider revocation.
    revoked: Vec<String>,
}
impl Counts {
    fn sum(&self, call: Call) -> u64 {
        self.calls.iter().filter(|((_, c), _)| *c == call).map(|(_, n)| n).sum()
    }
    fn total(&self) -> u64 {
        self.calls.values().sum()
    }
}

struct Mock {
    latency_ms: Mutex<(u64, u64)>,
    active_fraction: Mutex<f64>,
    /// Pages every poll serves on its first stream, overriding the draw.
    force_pages: Mutex<Option<usize>>,
    plans: Mutex<HashMap<String, usize>>,
    counts: Mutex<Counts>,
    rng: Mutex<u64>,
    next_item: AtomicU64,
    in_flight: AtomicI64,
}
impl Mock {
    fn new() -> Self {
        Self {
            latency_ms: Mutex::new((0, 0)),
            active_fraction: Mutex::new(0.0),
            force_pages: Mutex::new(None),
            plans: Mutex::default(),
            counts: Mutex::default(),
            rng: Mutex::new(0x9E37_79B9_7F4A_7C15),
            next_item: AtomicU64::new(1),
            in_flight: AtomicI64::new(0),
        }
    }
    fn uniform(&self) -> f64 {
        let mut s = self.rng.lock().unwrap();
        *s ^= *s << 13;
        *s ^= *s >> 7;
        *s ^= *s << 17;
        (*s >> 11) as f64 / (1u64 << 53) as f64
    }
    fn delay(&self) -> Duration {
        let (lo, hi) = *self.latency_ms.lock().unwrap();
        Duration::from_micros(((lo as f64 + (hi - lo) as f64 * self.uniform()) * 1000.0) as u64)
    }
    /// Decided once per poll, at the first page of its first stream (polls
    /// re-resolve the namespace only when due or when a pass has items).
    fn plan(&self, account: &str) -> usize {
        let pages = match *self.force_pages.lock().unwrap() {
            Some(p) => p,
            None if self.uniform() < *self.active_fraction.lock().unwrap() => 1 + (self.uniform() * 3.0) as usize % 3,
            None => 0,
        };
        self.plans.lock().unwrap().insert(account.to_owned(), pages);
        pages
    }
    fn pages_for(&self, account: &str) -> usize {
        self.plans.lock().unwrap().get(account).copied().unwrap_or(0)
    }
    fn take(&self) -> Counts {
        std::mem::take(&mut *self.counts.lock().unwrap())
    }
    fn item(&self) -> u64 {
        self.next_item.fetch_add(1, Ordering::Relaxed)
    }
}

fn group(account: &str) -> usize {
    account.split('_').nth(2).and_then(|g| g.parse().ok()).unwrap_or(0)
}
fn linear_org(g: usize) -> String {
    format!("0f0f0f0f-0000-4000-8000-{g:012}")
}
fn jira_url(g: usize) -> String {
    format!("https://scale{g}.atlassian.net")
}
fn adf(text: &str) -> Value {
    json!({"type":"doc","version":1,"content":[{"type":"paragraph","content":[{"type":"text","text":text}]}]})
}
fn page_bounds(index: usize, pages: usize) -> (usize, bool) {
    if index >= pages {
        (0, false)
    } else if index + 1 < pages {
        (FULL_PAGE, true)
    } else {
        (LAST_PAGE, false)
    }
}
fn page_index(cursor: &Value) -> usize {
    cursor.as_str().and_then(|c| c.strip_prefix('p')).and_then(|n| n.parse().ok()).unwrap_or(0)
}
fn linear_op(args: &Value) -> String {
    let doc = args["query_or_mutation"].as_str().unwrap_or("").trim_start();
    let doc = doc.strip_prefix("query").unwrap_or("").trim_start();
    doc.chars().take_while(|c| c.is_ascii_alphanumeric() || *c == '_').collect()
}

impl Mock {
    fn respond(&self, tool: &str, account: &str, args: &Value) -> (&'static str, Call, Value) {
        let g = group(account);
        let now = Utc::now();
        match tool {
            "LINEAR_RUN_QUERY_OR_MUTATION" => {
                let op = linear_op(args);
                if op == "CoralSwarmWorkspace" {
                    return ("linear", Call::Namespace, json!({"data":{"organization":{"id":linear_org(g)}},"errors":null}));
                }
                let field = match op.as_str() {
                    "CoralSwarmIssuesChanged" => "issues",
                    "CoralSwarmCommentsChanged" => "comments",
                    "CoralSwarmProjectsChanged" => "projects",
                    other => panic!("unexpected Linear operation {other}"),
                };
                let index = page_index(&args["variables"]["after"]);
                if field == "issues" && index == 0 {
                    self.plan(account);
                    self.counts.lock().unwrap().polls_started += 1;
                }
                let (n, more) = if field == "issues" { page_bounds(index, self.pages_for(account)) } else { (0, false) };
                let stamp = now.to_rfc3339();
                let nodes: Vec<Value> = (0..n)
                    .map(|_| {
                        let id = self.item();
                        json!({"id":format!("lin-{id}"),"identifier":format!("SC-{id}"),"title":format!("Scale issue {id}"),"description":"Decision: keep the poll cheap.","createdAt":stamp,"updatedAt":stamp,"state":{"name":"In Progress"},"creator":{"id":"u1","name":"Morgan"},"project":{"id":format!("6f1d1c2a-3b4c-4d5e-8f60-{g:012}"),"name":"Scale"}})
                    })
                    .collect();
                let cursor = more.then(|| format!("p{}", index + 1));
                ("linear", Call::Stream, json!({"data":{field:{"nodes":nodes,"pageInfo":{"hasNextPage":more,"endCursor":cursor}}},"errors":null}))
            }
            "JIRA_GET_SERVER_INFO" => ("jira", Call::Namespace, json!({"baseUrl":jira_url(g),"version":"1001.0.0"})),
            "JIRA_WHO_AM_I" => ("jira", Call::Namespace, json!({"data":{"sites":[{"url":jira_url(g),"id":format!("s{g}"),"name":format!("scale{g}")}]}})),
            "JIRA_SEARCH_FOR_ISSUES_USING_JQL_GET" => {
                assert!(args["jql"].as_str().unwrap().starts_with("updated >= -"), "watermark JQL only");
                let index = page_index(&args["next_page_token"]);
                if index == 0 {
                    self.plan(account);
                    self.counts.lock().unwrap().polls_started += 1;
                }
                let (n, more) = page_bounds(index, self.pages_for(account));
                let stamp = now.format("%Y-%m-%dT%H:%M:%S%.3f%z").to_string();
                let issues: Vec<Value> = (0..n)
                    .map(|_| {
                        let id = self.item();
                        json!({"id":format!("{id}"),"key":format!("SC-{id}"),"fields":{"summary":format!("Scale issue {id}"),"description":adf("Decision: keep the poll cheap."),"status":{"name":"In Progress"},"project":{"id":format!("100{g}"),"name":"Scale"},"creator":{"accountId":"acc-1","displayName":"Morgan"},"created":stamp,"updated":stamp}})
                    })
                    .collect();
                let mut data = json!({"issues":issues,"isLast":!more});
                if more {
                    data["nextPageToken"] = json!(format!("p{}", index + 1));
                }
                ("jira", Call::Stream, data)
            }
            "JIRA_LIST_ISSUE_COMMENTS" => {
                let stamp = now.format("%Y-%m-%dT%H:%M:%S%.3f%z").to_string();
                let comments: Vec<Value> = (0..COMMENTS_PER_ISSUE)
                    .map(|_| json!({"id":format!("c{}", self.item()),"body":adf("Agreed."),"author":{"accountId":"acc-2","displayName":"Riley"},"created":stamp,"updated":stamp}))
                    .collect();
                ("jira", Call::Child, json!({"comments":comments,"total":COMMENTS_PER_ISSUE,"startAt":0,"maxResults":100}))
            }
            other => panic!("not a recipe read tool: {other}"),
        }
    }
}

async fn execute(State(mock): State<Arc<Mock>>, Path(tool): Path<String>, Json(body): Json<Value>) -> Json<Value> {
    let started = Instant::now();
    let in_flight = mock.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
    let account = body["connected_account_id"].as_str().unwrap_or("").to_owned();
    let (toolkit, call, data) = mock.respond(&tool, &account, &body["arguments"]);
    let delay = mock.delay();
    {
        let mut c = mock.counts.lock().unwrap();
        *c.calls.entry((toolkit, call)).or_default() += 1;
        c.handler += started.elapsed();
        c.injected += delay;
        c.stamps.push(started);
        c.max_in_flight = c.max_in_flight.max(in_flight);
    }
    tokio::time::sleep(delay).await;
    mock.in_flight.fetch_sub(1, Ordering::SeqCst);
    Json(json!({"successful":true,"data":data}))
}

async fn revoke(State(mock): State<Arc<Mock>>, Path(id): Path<String>, Query(q): Query<HashMap<String, String>>) -> Json<Value> {
    assert_eq!(q.get("revoke_on_delete").map(String::as_str), Some("true"));
    mock.counts.lock().unwrap().revoked.push(id);
    Json(json!({"success":true}))
}

async fn mock_broker(mock: Arc<Mock>) -> Client {
    let app = axum::Router::new()
        .route("/tools/execute/{tool}", post(execute))
        .route("/connected_accounts/{id}", delete(revoke))
        .with_state(mock);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    Client::for_mock_broker(&base).unwrap()
}

fn task_owned(dsn: &str, database: &str) -> PgConnectOptions {
    let opts = PgConnectOptions::from_str(dsn).unwrap();
    assert!(matches!(opts.get_host(), "127.0.0.1" | "localhost" | "::1"));
    opts.database(database)
}

/// A fresh, migrated, task-owned database plus the RLS-bound app pool.
struct Db {
    _database: coralswarm_backend::test_db::TestDatabase,
    dsn: String,
    name: &'static str,
    owner: PgPool,
    app: PgPool,
    pgss: bool,
}
impl Db {
    async fn fresh(dsn: &str, name: &'static str, app_connections: u32) -> Self {
        let database = coralswarm_backend::test_db::TestDatabase::adopt(&task_owned(dsn, "composio_capture_test"), name);
        let admin = PgPoolOptions::new().max_connections(1).connect_with(task_owned(dsn, "composio_capture_test")).await.unwrap();
        sqlx::query(&format!("DROP DATABASE IF EXISTS {name} WITH (FORCE)")).execute(&admin).await.unwrap();
        sqlx::query(&format!("CREATE DATABASE {name}")).execute(&admin).await.unwrap();
        admin.close().await;
        let owner = PgPoolOptions::new().max_connections(4).connect_with(task_owned(dsn, name)).await.unwrap();
        coralswarm_backend::CONTROL_MIGRATOR.run(&owner).await.unwrap();
        support::app_role::set_app_role_password_with_retry(&owner).await;
        let app = PgPoolOptions::new()
            .max_connections(app_connections)
            .connect_with(task_owned(dsn, name).username("coralswarm_app").password(support::app_role::APP_ROLE_TEST_PASSWORD))
            .await
            .unwrap();
        let pgss = sqlx::query("CREATE EXTENSION IF NOT EXISTS pg_stat_statements").execute(&owner).await.is_ok()
            && sqlx::query("SELECT pg_stat_statements_reset()").execute(&owner).await.is_ok();
        Self { _database: database, dsn: dsn.into(), name, owner, app, pgss }
    }
    async fn reset_stats(&self) {
        if self.pgss {
            sqlx::query("SELECT pg_stat_statements_reset()").execute(&self.owner).await.unwrap();
        }
    }
    /// Top-level statements the app role ran since the last reset, and their
    /// server execution time. Nested statements inside SQL functions are
    /// excluded so the count tracks client round trips.
    async fn stats(&self) -> Option<(i64, f64)> {
        if !self.pgss {
            return None;
        }
        sqlx::query_as("SELECT coalesce(sum(s.calls),0)::int8, coalesce(sum(s.total_exec_time),0)::float8 FROM pg_stat_statements s JOIN pg_database d ON d.oid=s.dbid JOIN pg_roles r ON r.oid=s.userid WHERE d.datname=current_database() AND r.rolname='coralswarm_app' AND s.toplevel")
            .fetch_one(&self.owner)
            .await
            .ok()
    }
    /// Calls and server time of the due query alone since the last reset.
    async fn due_stats(&self) -> Option<(i64, f64)> {
        if !self.pgss {
            return None;
        }
        sqlx::query_as("SELECT coalesce(sum(s.calls),0)::int8, coalesce(sum(s.total_exec_time),0)::float8 FROM pg_stat_statements s JOIN pg_database d ON d.oid=s.dbid JOIN pg_roles r ON r.oid=s.userid WHERE d.datname=current_database() AND r.rolname='coralswarm_app' AND s.toplevel AND s.query LIKE '%resolve_composio_capture_due%'")
            .fetch_one(&self.owner)
            .await
            .ok()
    }
    async fn drop(self) {
        self.app.close().await;
        self.owner.close().await;
        let admin = PgPoolOptions::new().max_connections(1).connect_with(task_owned(&self.dsn, "composio_capture_test")).await.unwrap();
        sqlx::query(&format!("DROP DATABASE IF EXISTS {} WITH (FORCE)", self.name)).execute(&admin).await.unwrap();
    }
}

struct Fleet {
    state: Arc<coralswarm_backend::AppState>,
    tenant: Tenant,
    client: Client,
    mock: Arc<Mock>,
    captures: Vec<(Uuid, &'static str, UserContext)>,
    /// Seeds org accounts for this Clerk org instead of personal ones.
    org: Option<String>,
}
impl Fleet {
    async fn new(db: &Db) -> Self {
        let state = Arc::new(coralswarm_backend::test_app_state_owned_with_db_and_store(db.app.clone(), None));
        let tenant = Tenant::legacy(&state);
        let mock = Arc::new(Mock::new());
        let client = mock_broker(mock.clone()).await;
        Self { state, tenant, client, mock, captures: Vec::new(), org: None }
    }
    /// Grows to `n` captures, alternating Linear and Jira, each owned by its own
    /// member account and spread over `GROUPS` provider workspaces. Seeding goes
    /// through `activate`; the self-check is pre-passed so no broker call runs.
    async fn grow(&mut self, db: &Db, n: usize) {
        let start = self.captures.len();
        let seeded: Vec<(Uuid, &'static str, UserContext)> = futures::stream::iter(start..n)
            .map(|i| {
                let (owner, state, tenant, client, org) = (db.owner.clone(), self.state.clone(), self.tenant.clone(), self.client.clone(), self.org.clone());
                async move {
                    let service: &'static str = if i % 2 == 0 { "linear" } else { "jira" };
                    let g = i % GROUPS;
                    let user: Uuid = sqlx::query_scalar("INSERT INTO users(email) VALUES($1) RETURNING id").bind(format!("scale-{i}-{}@test.local", Uuid::new_v4().simple())).fetch_one(&owner).await.unwrap();
                    // An org account must exist before its member row, which copies whether it is one.
                    let account: Uuid = sqlx::query_scalar("INSERT INTO accounts(owner_user_id,clerk_org_id) VALUES($1,$2) RETURNING id").bind(user).bind(&org).fetch_one(&owner).await.unwrap();
                    sqlx::query("INSERT INTO account_members(account_id,user_id,role,seat_tier_id) VALUES($1,$2,'owner',CASE WHEN $3 THEN (SELECT id FROM seat_tiers WHERE code='standard' AND is_active) END)").bind(account).bind(user).bind(org.is_some()).execute(&owner).await.unwrap();
                    seat_key(&state, &org, account);
                    let namespace = if service == "linear" { format!("linear:workspace:{}", linear_org(g)) } else { format!("jira:site:s{g}") };
                    let conn: Uuid = sqlx::query_scalar("INSERT INTO composio_connections(account_id,owner_user_id,service,connected_account_id,auth_config_id,status,provider_namespace,self_check,self_checked_at,namespace_checked_at) VALUES($1,$2,$3,$4,'fixture-read-only','active',$5,'passed',now(),now()) RETURNING id")
                        .bind(account).bind(user).bind(service).bind(format!("ca_{service}_{g}_{i}")).bind(namespace).fetch_one(&owner).await.unwrap();
                    let user_ctx = UserContext { account_id: account, user_id: user, role: "owner".into(), device_id: "composio-scale".into() };
                    let response = composio::activate(&state, &tenant, &user_ctx, recipe::get(service).unwrap(), conn, &client).await;
                    assert!(response.status().is_success(), "seed activate {}", response.status());
                    let body = axum::body::to_bytes(response.into_body(), 1_000_000).await.unwrap();
                    let body: Value = serde_json::from_slice(&body).unwrap();
                    (Uuid::parse_str(body["capture_id"].as_str().unwrap()).unwrap(), service, user_ctx)
                }
            })
            .buffer_unordered(16)
            .collect()
            .await;
        self.captures.extend(seeded);
        let mut by_id: Vec<_> = std::mem::take(&mut self.captures);
        by_id.sort_by_key(|c| c.0);
        self.captures = by_id;
    }
    /// The test DEK cache expires entries after 5 minutes and these accounts
    /// have no wrapped key to fall back to, so re-seat them before each phase.
    fn refresh_keys(&self) {
        for (_, _, user) in &self.captures {
            seat_key(&self.state, &self.org, user.account_id);
        }
    }
    async fn poll(&self, capture: &(Uuid, &'static str, UserContext)) -> u16 {
        composio::poll(&self.state, &self.tenant, &capture.2, recipe::get(capture.1).unwrap(), capture.0, &self.client).await.status().as_u16()
    }
    fn latency(&self, lo: u64, hi: u64) {
        *self.mock.latency_ms.lock().unwrap() = (lo, hi);
    }
}

fn seat_key(state: &coralswarm_backend::AppState, org: &Option<String>, account: Uuid) {
    let tenant = match org {
        Some(org) => coralswarm_backend::tenancy::TenantId::Org(org.clone()),
        None => coralswarm_backend::tenancy::TenantId::Consumer,
    };
    state.dek_cache.insert(&tenant, account, [42; 32]);
}

/// Every capture idle, unleased, connected 10 minutes ago and due now, oldest
/// first in a random order so ticks interleave toolkits as production does.
async fn make_all_due(db: &Db) {
    sqlx::query("UPDATE composio_capture_scopes SET state='active',cursor=NULL,lease_id=NULL,lease_until=NULL,pass_started_at=NULL,error_code=NULL,retry_at=NULL,last_change_at=NULL,watermark=now()-interval '10 minutes',created_at=now()-interval '10 minutes',next_poll_at=now()-interval '1 second'-random()*interval '10 minutes'")
        .execute(&db.owner)
        .await
        .unwrap();
}

fn summary(xs: &[f64]) -> (f64, f64, f64, f64) {
    let n = xs.len().max(1) as f64;
    let mean = xs.iter().sum::<f64>() / n;
    let sd = (xs.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / (n - 1.0).max(1.0)).sqrt();
    let min = xs.iter().copied().fold(f64::INFINITY, f64::min);
    let max = xs.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    (mean, sd, min, max)
}
fn fmt(xs: &[f64]) -> String {
    let (mean, sd, min, max) = summary(xs);
    format!("{mean:.1} ± {sd:.1} [{min:.1}..{max:.1}]")
}

struct PollSample {
    namespace: u64,
    stream: u64,
    child: u64,
    statements: Option<i64>,
    db_ms: Option<f64>,
    wall_ms: f64,
}
/// One poll of `capture` serving `pages` change pages, measured at the mock and
/// in `pg_stat_statements`.
async fn sample_poll(db: &Db, fleet: &Fleet, capture: &(Uuid, &'static str, UserContext), pages: usize) -> PollSample {
    *fleet.mock.force_pages.lock().unwrap() = Some(pages);
    fleet.mock.take();
    db.reset_stats().await;
    let started = Instant::now();
    let status = fleet.poll(capture).await;
    let wall_ms = started.elapsed().as_secs_f64() * 1000.0;
    let stats = db.stats().await;
    let counts = fleet.mock.take();
    *fleet.mock.force_pages.lock().unwrap() = None;
    assert_eq!(status, 200, "poll {} pages={pages}", capture.1);
    PollSample {
        namespace: counts.sum(Call::Namespace),
        stream: counts.sum(Call::Stream),
        child: counts.sum(Call::Child),
        statements: stats.map(|s| s.0),
        db_ms: stats.map(|s| s.1),
        wall_ms,
    }
}


fn percentile(xs: &[f64], p: f64) -> f64 {
    let mut v = xs.to_vec();
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    if v.is_empty() {
        return 0.0;
    }
    v[((v.len() - 1) as f64 * p).round() as usize]
}
/// Most calls that arrived inside any 60-second window.
fn peak_per_minute(stamps: &[Instant]) -> usize {
    let mut v = stamps.to_vec();
    v.sort();
    let (mut lo, mut best) = (0, 0);
    for hi in 0..v.len() {
        while v[hi].duration_since(v[lo]) >= Duration::from_secs(60) {
            lo += 1;
        }
        best = best.max(hi - lo + 1);
    }
    best
}

/// One plane under the real harness loop for `secs`, with the production
/// cadence. Returns per-tick wall times (how long the harness was blocked).
async fn run_harness(db: &Db, fleet: &Fleet, secs: u64, rearm: bool) -> (Vec<f64>, Counts, Option<(i64, f64, i64, f64)>) {
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let rearmer = rearm.then(|| {
        let (owner, stop) = (db.owner.clone(), stop.clone());
        tokio::spawn(async move {
            // Every capture stays due, so the plane always has more demand than capacity.
            while !stop.load(Ordering::SeqCst) {
                sqlx::query("UPDATE composio_capture_scopes SET next_poll_at=now()-interval '1 second' WHERE state IN ('active','retry') AND lease_id IS NULL AND next_poll_at>now()")
                    .execute(&owner)
                    .await
                    .unwrap();
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        })
    });
    let ticks = Arc::new(Mutex::new(Vec::new()));
    let shutdown = coralswarm_backend::shutdown::Shutdown::new();
    let harness = {
        let (control, state, client, ticks) = (db.app.clone(), fleet.state.clone(), fleet.client.clone(), ticks.clone());
        let worker = harness_worker(&shutdown);
        tokio::spawn(coralswarm_backend::workers::tenant_harness::run_multi_tenant(
            db.app.clone(),
            Arc::new(coralswarm_backend::store::PostgresStore::new(db.app.clone())),
            None,
            coralswarm_backend::workers::tenant_harness::TenantActivity::new(),
            coralswarm_backend::workers::tenant_harness::HarnessConfig::sweep_worker("composio_capture", composio_capture::POLL_INTERVAL),
            shutdown.clone(),
            move |t| {
                let (control, state, client, ticks, worker) = (control.clone(), state.clone(), client.clone(), ticks.clone(), worker.clone());
                async move {
                    let started = Instant::now();
                    let r = harness_tick(&worker, &t, &control, &state, client).await;
                    ticks.lock().unwrap().push(started.elapsed().as_secs_f64() * 1000.0);
                    r
                }
            },
        ))
    };
    tokio::time::sleep(Duration::from_secs(secs)).await;
    let counts = fleet.mock.take();
    let stats = db.stats().await.zip(db.due_stats().await).map(|(a, d)| (a.0, a.1, d.0, d.1));
    shutdown.trigger();
    stop.store(true, Ordering::SeqCst);
    harness.await.unwrap();
    if let Some(r) = rearmer {
        r.await.unwrap();
    }
    settle(&shutdown).await;
    let ticks = ticks.lock().unwrap().clone();
    (ticks, counts, stats)
}

// The worker as the harness runs it. Kept apart so the same bench drives both
// the old and new worker shapes.
type HarnessWorker = composio_capture::Worker;
fn harness_worker(shutdown: &coralswarm_backend::shutdown::Shutdown) -> Arc<HarnessWorker> {
    let concurrency = env_num("COMPOSIO_SCALE_CONCURRENCY", composio_capture::DEFAULT_CONCURRENCY);
    let budget = Arc::new(coralswarm_backend::test_composio_budget::CallBudget::per_minute(env_num(
        "COMPOSIO_SCALE_CALLS_PER_MINUTE",
        coralswarm_backend::test_composio_budget::DEFAULT_CALLS_PER_MINUTE,
    )));
    let worker = composio_capture::Worker::new(budget, concurrency, composio_capture::ROUND_BUDGET, shutdown.clone());
    LAST_WORKER.lock().unwrap().replace(worker.clone());
    worker
}
static LAST_WORKER: Mutex<Option<Arc<HarnessWorker>>> = Mutex::new(None);
async fn harness_tick(
    worker: &Arc<HarnessWorker>,
    t: &coralswarm_backend::workers::tenant_harness::WorkerTenant,
    control: &PgPool,
    state: &Arc<coralswarm_backend::AppState>,
    client: Client,
) -> Result<coralswarm_backend::workers::tenant_harness::Tick, String> {
    worker.tick_with(t, control, state, || Ok(client)).await
}
/// Polls a round started keep running after the harness stops; wait them out.
async fn settle(_: &coralswarm_backend::shutdown::Shutdown) {
    let worker = LAST_WORKER.lock().unwrap().take();
    if let Some(worker) = worker {
        worker.settle().await;
    }
}

fn dsn_or_skip() -> Option<String> {
    match std::env::var("COMPOSIO_TEST_DATABASE_URL") {
        Ok(dsn) => Some(dsn),
        Err(_) => {
            assert!(std::env::var_os("REQUIRE_PG_SUITE").is_none(), "COMPOSIO_TEST_DATABASE_URL required");
            eprintln!("composio scale skipped: missing task-owned database");
            None
        }
    }
}

/// Broker calls per poll, the cost model's inputs, with the namespace checked
/// in the last 24 hours: an idle Linear poll is one page of each of its three
/// streams and an idle Jira poll is one search page. A pass with items first
/// re-resolves the namespace (Linear 1 call, Jira 2: server info and accessible
/// sites). Active pages add one stream call each, and each changed Jira issue
/// adds one comments call. A pending self-check re-probes every stream.
/// One tick starts a round that polls every due capture, a bounded number at a
/// time.
#[tokio::test]
async fn composio_poll_call_counts_match_cost_model() {
    let Some(dsn) = dsn_or_skip() else { return };
    let db = Db::fresh(&dsn, "composio_scale_counts", 6).await;
    let mut fleet = Fleet::new(&db).await;
    fleet.grow(&db, 8).await;
    let linear = fleet.captures.iter().find(|c| c.1 == "linear").unwrap().clone();
    let jira = fleet.captures.iter().find(|c| c.1 == "jira").unwrap().clone();
    for pages in 0..=3 {
        let l = sample_poll(&db, &fleet, &linear, pages).await;
        assert_eq!((l.namespace, l.stream, l.child), ((pages > 0) as u64, 3 + pages.saturating_sub(1) as u64, 0), "linear pages={pages}");
        let j = sample_poll(&db, &fleet, &jira, pages).await;
        let issues = if pages == 0 { 0 } else { (pages - 1) * FULL_PAGE + LAST_PAGE } as u64;
        assert_eq!((j.namespace, j.stream, j.child), (2 * (pages > 0) as u64, pages.max(1) as u64, issues), "jira pages={pages}");
        eprintln!("composio scale counts pages={pages}: linear ns={} stream={} child={} | jira ns={} stream={} child={}", l.namespace, l.stream, l.child, j.namespace, j.stream, j.child);
    }
    sqlx::query("UPDATE composio_connections SET self_check='pending' WHERE id IN (SELECT connection_id FROM composio_capture_scopes WHERE id IN ($1,$2))")
        .bind(linear.0)
        .bind(jira.0)
        .execute(&db.owner)
        .await
        .unwrap();
    let l = sample_poll(&db, &fleet, &linear, 0).await;
    let j = sample_poll(&db, &fleet, &jira, 0).await;
    eprintln!("composio scale counts self-check pending idle: linear {} calls, jira {} calls", l.namespace + l.stream, j.namespace + j.stream);
    assert_eq!(l.namespace + l.stream, 7, "a pending check re-probes all three Linear streams every poll");
    assert_eq!(j.namespace + j.stream, 4);
    sqlx::query("UPDATE composio_connections SET self_check='passed'").execute(&db.owner).await.unwrap();

    make_all_due(&db).await;
    let worker = coralswarm_backend::workers::tenant_harness::WorkerTenant {
        tenant: coralswarm_backend::tenancy::TenantId::Consumer,
        store: Arc::new(coralswarm_backend::store::PostgresStore::new(db.app.clone())),
        pool: db.app.clone(),
    };
    fleet.mock.take();
    fleet.latency(20, 40);
    let shutdown = coralswarm_backend::shutdown::Shutdown::new();
    let capture_worker = composio_capture::Worker::new(Arc::new(coralswarm_backend::test_composio_budget::CallBudget::per_minute(1_000)), 3, composio_capture::ROUND_BUDGET, shutdown);
    let client = fleet.client.clone();
    let tick = capture_worker.tick_with(&worker, &db.app, &fleet.state, || Ok(client)).await.unwrap();
    capture_worker.settle().await;
    let counts = fleet.mock.take();
    assert_eq!((tick.processed, counts.polls_started), (8, 8), "8 due, one tick's round polls all of them");
    assert!(counts.max_in_flight <= 3, "at most 3 polls in flight on a plane, saw {} calls at once", counts.max_in_flight);
    let due: i64 = sqlx::query_scalar("SELECT count(*) FROM resolve_composio_capture_due()").fetch_one(&db.app).await.unwrap();
    assert_eq!(due, 0, "every capture moved to its next interval");
    eprintln!("composio scale counts tick: 8 due, {} polled, {} calls in flight at most", counts.polls_started, counts.max_in_flight);
    db.drop().await;
}


/// A second database standing in for an org plane: one capture owned by a
/// member of an account bound to `org`.
async fn org_plane(dsn: &str, name: &'static str, org: &str) -> (Db, Fleet) {
    let db = Db::fresh(dsn, name, 6).await;
    let mut fleet = Fleet::new(&db).await;
    fleet.org = Some(org.into());
    fleet.grow(&db, 1).await;
    make_all_due(&db).await;
    (db, fleet)
}

/// The harness ticks every plane from one serial loop. A plane whose polls are
/// slow must not hold that loop: the next plane's due capture is polled while
/// the slow plane's polls are still waiting on the broker.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slow_plane_does_not_delay_another_planes_due_captures() {
    let Some(dsn) = dsn_or_skip() else { return };
    let slow = Db::fresh(&dsn, "composio_plane_slow", 6).await;
    let mut slow_fleet = Fleet::new(&slow).await;
    slow_fleet.grow(&slow, 6).await;
    slow_fleet.latency(1_500, 1_500);
    make_all_due(&slow).await;
    let (fast, fast_fleet) = org_plane(&dsn, "composio_plane_fast", "org_composio_fast_plane").await;
    let slow_plane = coralswarm_backend::workers::tenant_harness::WorkerTenant {
        tenant: coralswarm_backend::tenancy::TenantId::Consumer,
        store: Arc::new(coralswarm_backend::store::PostgresStore::new(slow.app.clone())),
        pool: slow.app.clone(),
    };
    let fast_plane = coralswarm_backend::workers::tenant_harness::WorkerTenant {
        tenant: coralswarm_backend::tenancy::TenantId::Org("org_composio_fast_plane".into()),
        store: Arc::new(coralswarm_backend::store::PostgresStore::new(fast.app.clone())),
        pool: fast.app.clone(),
    };
    let shutdown = coralswarm_backend::shutdown::Shutdown::new();
    let worker = composio_capture::Worker::new(Arc::new(coralswarm_backend::test_composio_budget::CallBudget::per_minute(1_000)), 2, composio_capture::ROUND_BUDGET, shutdown.clone());

    // One scheduler pass, in the harness's order: the slow plane first.
    let started = Instant::now();
    let client = slow_fleet.client.clone();
    worker.tick_with(&slow_plane, &slow.app, &slow_fleet.state, || Ok(client)).await.unwrap();
    let client = fast_fleet.client.clone();
    let fast_tick = worker.tick_with(&fast_plane, &fast.app, &fast_fleet.state, || Ok(client)).await.unwrap();
    assert_eq!(fast_tick.processed, 1);
    let fast_capture = fast_fleet.captures[0].0;
    let polled_after = loop {
        let (next, error): (chrono::DateTime<Utc>, Option<String>) = sqlx::query_as("SELECT next_poll_at,error_code FROM composio_capture_scopes WHERE id=$1").bind(fast_capture).fetch_one(&fast.owner).await.unwrap();
        if next > Utc::now() {
            assert_eq!(error, None, "the fast plane's poll succeeded");
            break started.elapsed();
        }
        assert!(started.elapsed() < Duration::from_secs(60), "the fast plane's capture was never polled");
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    let slow_counts_so_far = slow_fleet.mock.counts.lock().unwrap().polls_started;
    let slow_in_flight = slow_fleet.mock.in_flight.load(Ordering::SeqCst);
    eprintln!("composio planes: fast plane polled after {polled_after:?}; slow plane {slow_counts_so_far} polls started, {slow_in_flight} calls in flight");
    // One slow idle poll is at least 3 calls of 1.5 s. Serially, the fast
    // plane would wait for two of them.
    assert!(polled_after < Duration::from_millis(4_500), "fast plane waited {polled_after:?} behind the slow plane");
    assert!(slow_in_flight > 0, "the slow plane's polls were still running");

    worker.settle().await;
    let slow_due: i64 = sqlx::query_scalar("SELECT count(*) FROM composio_capture_scopes WHERE next_poll_at<=now()").fetch_one(&slow.owner).await.unwrap();
    assert_eq!(slow_due, 0, "the slow plane still drained its backlog");
    shutdown.trigger();
    slow.drop().await;
    fast.drop().await;
}

/// The broker call budget holds however many polls are in flight: no window
/// of its length ever carries more calls than the limit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn call_budget_caps_broker_calls_per_window() {
    let Some(dsn) = dsn_or_skip() else { return };
    let db = Db::fresh(&dsn, "composio_call_budget", 10).await;
    let mut fleet = Fleet::new(&db).await;
    fleet.grow(&db, 12).await;
    fleet.latency(5, 10);
    make_all_due(&db).await;
    let plane = coralswarm_backend::workers::tenant_harness::WorkerTenant {
        tenant: coralswarm_backend::tenancy::TenantId::Consumer,
        store: Arc::new(coralswarm_backend::store::PostgresStore::new(db.app.clone())),
        pool: db.app.clone(),
    };
    let (limit, window) = (10usize, Duration::from_secs(2));
    let shutdown = coralswarm_backend::shutdown::Shutdown::new();
    let worker = composio_capture::Worker::new(Arc::new(coralswarm_backend::test_composio_budget::CallBudget::new(limit, window)), 8, composio_capture::ROUND_BUDGET, shutdown);
    fleet.mock.take();
    let started = Instant::now();
    let client = fleet.client.clone();
    worker.tick_with(&plane, &db.app, &fleet.state, || Ok(client)).await.unwrap();
    worker.settle().await;
    let elapsed = started.elapsed();
    let counts = fleet.mock.take();
    let total = counts.total() as usize;
    // With the namespace trusted, Linear idle is 3 calls and Jira idle 1: 6 of each.
    assert_eq!((counts.polls_started, total), (12, 24));
    let mut stamps = counts.stamps.clone();
    stamps.sort();
    // Calls are counted when sent and stamped when they arrive; a little slack
    // keeps loopback jitter from straddling a window edge.
    let measured = window - Duration::from_millis(100);
    let peak = (0..stamps.len()).map(|i| stamps[i..].iter().take_while(|t| t.duration_since(stamps[i]) < measured).count()).max().unwrap();
    eprintln!("composio budget: {total} calls in {elapsed:?}, at most {peak} in any {measured:?} (limit {limit} per {window:?})");
    assert!(peak <= limit, "{peak} calls inside one window, limit {limit}");
    assert!(elapsed >= window * 2, "24 calls at 10 per 2s need at least two refills, took {elapsed:?}");
    db.drop().await;
}

/// Due captures that can never take a lease used to stay due and fill the due
/// query's `LIMIT 10`, so a plane polled nothing else (#1258). One round now
/// parks or defers every one of them and still polls the live captures behind.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn captures_that_cannot_lease_do_not_starve_the_plane() {
    let Some(dsn) = dsn_or_skip() else { return };
    let db = Db::fresh(&dsn, "composio_orphan_captures", 10).await;
    let mut fleet = Fleet::new(&db).await;
    fleet.grow(&db, 20).await;
    let ids = |r: std::ops::Range<usize>| fleet.captures[r].iter().map(|c| c.0).collect::<Vec<Uuid>>();
    let (left, off_plane, retired, revoked, live) = (ids(0..12), ids(12..14), ids(14..16), ids(16..18), ids(18..20));
    // The member who leaves first captured something; it must survive.
    sample_poll(&db, &fleet, &fleet.captures[0], 1).await;
    let atoms: i64 = sqlx::query_scalar("SELECT count(*) FROM atoms").fetch_one(&db.owner).await.unwrap();
    assert!(atoms > 0);
    sqlx::query("DELETE FROM account_members m USING composio_capture_scopes c WHERE c.id=ANY($1) AND m.account_id=c.account_id AND m.user_id=c.owner_user_id")
        .bind(&left).execute(&db.owner).await.unwrap();
    // The account moved to an org plane; this consumer-plane copy can never be polled here.
    let moved_accounts: Vec<(Uuid, Uuid)> = sqlx::query_as("SELECT id,account_id FROM composio_capture_scopes WHERE id=ANY($1) ORDER BY account_id")
        .bind(&off_plane).fetch_all(&db.owner).await.unwrap();
    for (capture, account) in moved_accounts {
        let mut tx = coralswarm_backend::tenancy::scope::begin_account_scoped(&db.owner, account).await.unwrap();
        coralswarm_backend::billing::renewal::lock_period_rotation(&mut tx, account).await.unwrap();
        sqlx::query("SET CONSTRAINTS account_members_account_org_fk DEFERRED").execute(&mut *tx).await.unwrap();
        sqlx::query("UPDATE accounts SET clerk_org_id='org_moved_'||left($1::uuid::text,8),is_org_account=true WHERE id=$2")
            .bind(capture).bind(account).execute(&mut *tx).await.unwrap();
        sqlx::query("UPDATE account_members SET account_is_org_account=true,seat_tier_id=(SELECT id FROM seat_tiers WHERE code='standard' AND is_active) WHERE account_id=$1")
            .bind(account).execute(&mut *tx).await.unwrap();
        sqlx::query("INSERT INTO tenant_databases(clerk_org_id,account_id,slug,db_secret_ref,status) SELECT 'org_moved_'||left(c.id::text,8),c.account_id,'moved-'||left(c.id::text,8),'MOVED_'||left(c.id::text,8),'active' FROM composio_capture_scopes c WHERE c.id=ANY($1)")
            .bind(vec![capture]).execute(&mut *tx).await.unwrap();
        tx.commit().await.unwrap();
    }
    sqlx::query("UPDATE composio_capture_scopes SET service='retired_toolkit' WHERE id=ANY($1)").bind(&retired).execute(&db.owner).await.unwrap();
    sqlx::query("UPDATE composio_connections SET status='disconnected' WHERE id IN (SELECT connection_id FROM composio_capture_scopes WHERE id=ANY($1))")
        .bind(&revoked).execute(&db.owner).await.unwrap();
    make_all_due(&db).await;
    // Every capture that cannot lease sorts ahead of the live ones.
    sqlx::query("UPDATE composio_capture_scopes SET next_poll_at=now()-interval '1 day'+random()*interval '1 hour' WHERE NOT id=ANY($1)")
        .bind(&live).execute(&db.owner).await.unwrap();
    fleet.refresh_keys();

    let plane = coralswarm_backend::workers::tenant_harness::WorkerTenant {
        tenant: coralswarm_backend::tenancy::TenantId::Consumer,
        store: Arc::new(coralswarm_backend::store::PostgresStore::new(db.app.clone())),
        pool: db.app.clone(),
    };
    let shutdown = coralswarm_backend::shutdown::Shutdown::new();
    let worker = composio_capture::Worker::new(Arc::new(coralswarm_backend::test_composio_budget::CallBudget::per_minute(1_000)), 3, Duration::from_secs(10), shutdown.clone());
    fleet.mock.take();
    let client = fleet.client.clone();
    worker.tick_with(&plane, &db.app, &fleet.state, || Ok(client)).await.unwrap();
    worker.settle().await;
    let counts = fleet.mock.take();
    let state_of = |ids: Vec<Uuid>| {
        let owner = db.owner.clone();
        async move {
            sqlx::query_as::<_, (String, bool)>("SELECT state,next_poll_at>now() FROM composio_capture_scopes WHERE id=ANY($1) ORDER BY id")
                .bind(ids).fetch_all(&owner).await.unwrap()
        }
    };
    let live_after = state_of(live.clone()).await;
    eprintln!("composio orphans: {} polls started; live captures after one round {live_after:?}", counts.polls_started);
    assert_eq!(live_after, vec![("active".to_string(), true); 2], "both live captures were polled behind 16 that cannot lease");
    assert_eq!(counts.polls_started, 2, "only the live captures reached the broker");
    let remotes = |ids: Vec<Uuid>| {
        let owner = db.owner.clone();
        async move {
            sqlx::query_scalar::<_, String>("SELECT n.connected_account_id FROM composio_connections n JOIN composio_capture_scopes c ON c.connection_id=n.id WHERE c.id=ANY($1) ORDER BY 1")
                .bind(ids).fetch_all(&owner).await.unwrap()
        }
    };
    let mut revokes = counts.revoked.clone();
    revokes.sort();
    assert_eq!(revokes, remotes(left.clone()).await, "each departed member's connected account is revoked once, and nothing else");

    let connection_status = |ids: Vec<Uuid>| {
        let owner = db.owner.clone();
        async move {
            sqlx::query_scalar::<_, String>("SELECT n.status FROM composio_connections n JOIN composio_capture_scopes c ON c.connection_id=n.id WHERE c.id=ANY($1)")
                .bind(ids).fetch_all(&owner).await.unwrap()
        }
    };
    assert!(state_of(left.clone()).await.iter().all(|s| s.0 == "disconnected"), "a capture whose member left is parked");
    assert!(connection_status(left.clone()).await.iter().all(|s| s == "disconnected"), "and its connection reads as disconnected");
    assert!(state_of(off_plane.clone()).await.iter().all(|s| s.0 == "disconnected"), "a capture off its account's plane is parked");
    assert!(connection_status(off_plane.clone()).await.iter().all(|s| s == "active"), "the connection may still serve the account's new plane");
    assert_eq!(state_of(retired.clone()).await, vec![("active".to_string(), true); 2], "a removed recipe defers, so a restored recipe resumes");
    assert!(state_of(revoked.clone()).await.iter().all(|s| s.0 == "disconnected"), "a disconnected connection already parked its capture");
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM composio_capture_scopes").fetch_one(&db.owner).await.unwrap();
    assert_eq!(rows, 20, "no capture row is deleted");
    let atoms_after: i64 = sqlx::query_scalar("SELECT count(*) FROM atoms").fetch_one(&db.owner).await.unwrap();
    assert_eq!(atoms_after, atoms, "no atom is deleted");
    let due: i64 = sqlx::query_scalar("SELECT count(*) FROM resolve_composio_capture_due()").fetch_one(&db.app).await.unwrap();
    assert_eq!(due, 0);

    // Idempotent: another round over the same rows changes nothing, and a
    // member who comes back does not silently resume a parked capture.
    sqlx::query("INSERT INTO account_members(account_id,user_id,role) SELECT account_id,owner_user_id,'owner' FROM composio_capture_scopes WHERE id=$1")
        .bind(left[0]).execute(&db.owner).await.unwrap();
    let before: Vec<(Uuid, String, chrono::DateTime<Utc>)> = sqlx::query_as("SELECT id,state,updated_at FROM composio_capture_scopes WHERE NOT id=ANY($1) ORDER BY id")
        .bind(&retired).fetch_all(&db.owner).await.unwrap();
    sqlx::query("UPDATE composio_capture_scopes SET next_poll_at=now()-interval '1 day' WHERE id=ANY($1)").bind(&retired).execute(&db.owner).await.unwrap();
    let client = fleet.client.clone();
    worker.tick_with(&plane, &db.app, &fleet.state, || Ok(client)).await.unwrap();
    worker.settle().await;
    let counts = fleet.mock.take();
    assert_eq!(counts.polls_started, 0);
    assert!(counts.revoked.is_empty(), "a revoked connection is not revoked again");
    let after: Vec<(Uuid, String, chrono::DateTime<Utc>)> = sqlx::query_as("SELECT id,state,updated_at FROM composio_capture_scopes WHERE NOT id=ANY($1) ORDER BY id")
        .bind(&retired).fetch_all(&db.owner).await.unwrap();
    assert_eq!(after, before);
    assert_eq!(state_of(retired).await, vec![("active".to_string(), true); 2]);
    shutdown.trigger();
    db.drop().await;
}

fn env_list(key: &str, default: &str) -> Vec<usize> {
    std::env::var(key).unwrap_or_else(|_| default.into()).split(',').map(|n| n.trim().parse().unwrap()).collect()
}
fn env_num<T: FromStr>(key: &str, default: T) -> T {
    std::env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "bench: run with --ignored --nocapture against a task-owned database"]
async fn composio_scale_bench() {
    let Some(dsn) = dsn_or_skip() else { return };
    let ns = env_list("COMPOSIO_SCALE_N", "10,100,1000,10000");
    let (lo, hi) = {
        let spec = std::env::var("COMPOSIO_SCALE_LATENCY_MS").unwrap_or_else(|_| "300-800".into());
        let (a, b) = spec.split_once('-').unwrap();
        (a.parse::<u64>().unwrap(), b.parse::<u64>().unwrap())
    };
    let active: f64 = env_num("COMPOSIO_SCALE_ACTIVE", 0.2);
    let ticks: usize = env_num("COMPOSIO_SCALE_TICKS", 8);
    let repeats: usize = env_num("COMPOSIO_SCALE_REPEATS", 5);
    let sections = std::env::var("COMPOSIO_SCALE_SECTIONS").unwrap_or_else(|_| "poll,tick,sustain,concurrent".into());
    let on = |name: &str| sections.split(',').any(|s| s.trim() == name);
    let sustain_secs: u64 = env_num("COMPOSIO_SCALE_SUSTAIN_SECS", 180);
    let sustain_repeats: usize = env_num("COMPOSIO_SCALE_SUSTAIN_REPEATS", 1);
    let keepup_secs: u64 = env_num("COMPOSIO_SCALE_KEEPUP_SECS", 1200);
    let app_connections = env_num("COMPOSIO_SCALE_APP_POOL", 16u32);
    let db = Db::fresh(&dsn, "composio_scale_bench", app_connections).await;
    let mut fleet = Fleet::new(&db).await;
    eprintln!("SCALE config N={ns:?} latency={lo}-{hi}ms active={active} ticks={ticks} repeats={repeats} app_pool={app_connections} sections={sections} sustain={sustain_secs}s x{sustain_repeats} pg_stat_statements={}", db.pgss);

    // Per poll, at zero injected latency: broker calls, DB work, worker floor.
    fleet.grow(&db, 8).await;
    let linear = fleet.captures.iter().find(|c| c.1 == "linear").unwrap().clone();
    let jira = fleet.captures.iter().find(|c| c.1 == "jira").unwrap().clone();
    let poll_cases: &[(&(Uuid, &'static str, UserContext), usize)] = if on("poll") { &[(&linear, 0), (&linear, 1), (&linear, 3), (&jira, 0), (&jira, 1), (&jira, 3)] } else { &[] };
    eprintln!("SCALE poll | recipe | pages | ns calls | stream calls | child calls | DB statements | DB exec ms | wall ms (0 latency)");
    for &(capture, pages) in poll_cases {
        sample_poll(&db, &fleet, capture, pages).await;
        let samples: Vec<PollSample> = {
            let mut v = Vec::new();
            for _ in 0..repeats {
                v.push(sample_poll(&db, &fleet, capture, pages).await);
            }
            v
        };
        let st: Vec<f64> = samples.iter().filter_map(|s| s.statements.map(|x| x as f64)).collect();
        let dbt: Vec<f64> = samples.iter().filter_map(|s| s.db_ms).collect();
        let wall: Vec<f64> = samples.iter().map(|s| s.wall_ms).collect();
        let s0 = &samples[0];
        eprintln!(
            "SCALE poll | {} | {pages} | {} | {} | {} | {} | {} | {}",
            capture.1,
            s0.namespace,
            s0.stream,
            s0.child,
            if st.is_empty() { "n/a".into() } else { fmt(&st) },
            if dbt.is_empty() { "n/a".into() } else { fmt(&dbt) },
            fmt(&wall)
        );
    }

    let worker = coralswarm_backend::workers::tenant_harness::WorkerTenant {
        tenant: coralswarm_backend::tenancy::TenantId::Consumer,
        store: Arc::new(coralswarm_backend::store::PostgresStore::new(db.app.clone())),
        pool: db.app.clone(),
    };
    eprintln!("SCALE sustain | N | mix | polls/min | broker calls/min | peak calls in any 60s | max calls in flight | tick ms p50 | tick ms p95 | tick ms max | ticks | DB statements/min | DB exec ms/min | due queries/min | due query ms/min | failed polls");
    eprintln!("SCALE tick | N | latency | due query ms | tick wall ms | polls/tick | broker calls/tick | injected sleep ms/tick | mock handler ms/tick | DB statements/tick | DB exec ms/tick");
    for &n in &ns {
        let seed = Instant::now();
        fleet.grow(&db, n).await;
        let seeded_s = seed.elapsed().as_secs_f64();
        make_all_due(&db).await;
        for table in ["composio_capture_scopes", "graphs"] {
            sqlx::query(&format!("ANALYZE {table}")).execute(&db.owner).await.unwrap();
        }
        let mut due_ms = Vec::new();
        for _ in 0..20 {
            let t = Instant::now();
            let rows: Vec<(Uuid,)> = sqlx::query_as("SELECT id FROM resolve_composio_capture_due()").fetch_all(&db.app).await.unwrap();
            due_ms.push(t.elapsed().as_secs_f64() * 1000.0);
            assert_eq!(rows.len(), n.min(10));
        }
        let tick_rows: &[(&str, u64, u64, f64)] = if on("tick") { &[("idle 0ms", 0, 0, 0.0), ("idle injected", lo, hi, 0.0), ("mixed injected", lo, hi, active)] } else { &[] };
        for &(label, l, h, fraction) in tick_rows {
            fleet.latency(l, h);
            *fleet.mock.active_fraction.lock().unwrap() = fraction;
            make_all_due(&db).await;
            fleet.refresh_keys();
            let shutdown = coralswarm_backend::shutdown::Shutdown::new();
            let harness = harness_worker(&shutdown);
            harness_tick(&harness, &worker, &db.app, &fleet.state, fleet.client.clone()).await.unwrap();
            settle(&shutdown).await;
            let (mut wall, mut polls, mut calls, mut injected, mut handler, mut stmts, mut dbms) = (vec![], vec![], vec![], vec![], vec![], vec![], vec![]);
            for _ in 0..ticks {
                // Keep all N due so every tick sees a full backlog.
                make_all_due(&db).await;
                fleet.mock.take();
                db.reset_stats().await;
                let harness = harness_worker(&shutdown);
                let t = Instant::now();
                let tick = harness_tick(&harness, &worker, &db.app, &fleet.state, fleet.client.clone()).await.unwrap();
                settle(&shutdown).await;
                wall.push(t.elapsed().as_secs_f64() * 1000.0);
                let stats = db.stats().await;
                let c = fleet.mock.take();
                assert!(c.polls_started >= tick.processed as u64, "a round polls at least what its tick found");
                polls.push(c.polls_started as f64);
                calls.push(c.total() as f64);
                injected.push(c.injected.as_secs_f64() * 1000.0);
                handler.push(c.handler.as_secs_f64() * 1000.0);
                if let Some((s, ms)) = stats {
                    stmts.push(s as f64);
                    dbms.push(ms);
                }
            }
            eprintln!(
                "SCALE tick | {n} | {label} | {} | {} | {} | {} | {} | {} | {} | {}",
                fmt(&due_ms),
                fmt(&wall),
                fmt(&polls),
                fmt(&calls),
                fmt(&injected),
                fmt(&handler),
                if stmts.is_empty() { "n/a".into() } else { fmt(&stmts) },
                if dbms.is_empty() { "n/a".into() } else { fmt(&dbms) }
            );
        }
        eprintln!("SCALE seeded N={n} in {seeded_s:.1}s");
        if on("sustain") {
            for (label, fraction) in [("idle", 0.0), ("mixed", active)] {
                for _ in 0..sustain_repeats {
                    fleet.latency(lo, hi);
                    *fleet.mock.active_fraction.lock().unwrap() = fraction;
                    make_all_due(&db).await;
                    fleet.refresh_keys();
                    fleet.mock.take();
                    db.reset_stats().await;
                    let (ticks, c, stats) = run_harness(&db, &fleet, sustain_secs, true).await;
                    if std::env::var("COMPOSIO_SCALE_TOP_STATEMENTS").is_ok() && db.pgss {
                        let top: Vec<(String, i64, f64)> = sqlx::query_as("SELECT left(regexp_replace(s.query,'\\s+',' ','g'),140), s.calls, s.total_exec_time FROM pg_stat_statements s JOIN pg_database d ON d.oid=s.dbid JOIN pg_roles r ON r.oid=s.userid WHERE d.datname=current_database() AND r.rolname='coralswarm_app' ORDER BY s.total_exec_time DESC LIMIT 12")
                            .fetch_all(&db.owner)
                            .await
                            .unwrap();
                        for (query, calls, ms) in top {
                            eprintln!("SCALE top | {n} | {label} | {ms:.0} ms | {calls} calls | {:.3} ms/call | {query}", ms / calls.max(1) as f64);
                        }
                    }
                    let failed: i64 = sqlx::query_scalar("SELECT count(*) FROM composio_capture_scopes WHERE error_code IS NOT NULL").fetch_one(&db.owner).await.unwrap();
                    let minutes = sustain_secs as f64 / 60.0;
                    eprintln!(
                        "SCALE sustain | {n} | {label} | {:.1} | {:.0} | {} | {} | {:.1} | {:.1} | {:.1} | {:.1} | {} | {} | {} | {} | {failed}",
                        c.polls_started as f64 / minutes,
                        c.total() as f64 / minutes,
                        peak_per_minute(&c.stamps),
                        c.max_in_flight,
                        percentile(&ticks, 0.5),
                        percentile(&ticks, 0.95),
                        ticks.iter().copied().fold(0.0, f64::max),
                        ticks.len() as f64,
                        stats.map_or("n/a".into(), |s| format!("{:.0}", s.0 as f64 / minutes)),
                        stats.map_or("n/a".into(), |s| format!("{:.0}", s.1 / minutes)),
                        stats.map_or("n/a".into(), |s| format!("{:.1}", s.2 as f64 / minutes)),
                        stats.map_or("n/a".into(), |s| format!("{:.1}", s.3 / minutes)),
                    );
                }
            }
        }
    }
    if on("keepup") {
        // Real intervals, no re-arming: every capture idle for a day (15-minute
        // interval), next polls spread evenly over the next 15 minutes. A plane
        // that keeps up holds the oldest overdue capture near one tick.
        fleet.latency(lo, hi);
        *fleet.mock.active_fraction.lock().unwrap() = env_num("COMPOSIO_SCALE_KEEPUP_ACTIVE", 0.0);
        sqlx::query("UPDATE composio_capture_scopes SET state='active',cursor=NULL,lease_id=NULL,lease_until=NULL,pass_started_at=NULL,error_code=NULL,retry_at=NULL,last_change_at=NULL,watermark=now()-interval '10 minutes',created_at=now()-interval '1 day',next_poll_at=now()+random()*interval '15 minutes'")
            .execute(&db.owner)
            .await
            .unwrap();
        fleet.refresh_keys();
        fleet.mock.take();
        db.reset_stats().await;
        let sampler = {
            let owner = db.owner.clone();
            tokio::spawn(async move {
                let mut samples = Vec::new();
                let started = Instant::now();
                while started.elapsed() < Duration::from_secs(keepup_secs) {
                    let overdue: f64 = sqlx::query_scalar("SELECT coalesce(extract(epoch FROM now()-min(next_poll_at)),0)::float8 FROM composio_capture_scopes WHERE state IN ('active','retry') AND lease_id IS NULL AND next_poll_at<=now()")
                        .fetch_one(&owner)
                        .await
                        .unwrap();
                    samples.push((started.elapsed().as_secs_f64(), overdue));
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
                samples
            })
        };
        let (ticks, c, stats) = run_harness(&db, &fleet, keepup_secs, false).await;
        let samples = sampler.await.unwrap();
        let steady: Vec<f64> = samples.iter().filter(|(t, _)| *t >= 60.0).map(|(_, o)| *o).collect();
        let minutes = keepup_secs as f64 / 60.0;
        for minute in (0..samples.len()).step_by(12) {
            eprintln!("SCALE keepup trace | t={:.0}s | oldest overdue {:.1}s", samples[minute].0, samples[minute].1);
        }
        eprintln!(
            "SCALE keepup | {} | {:.1} polls/min | overdue s p50 {:.1} p95 {:.1} max {:.1} | tick ms p50 {:.1} p95 {:.1} | calls/min peak {} | DB statements/min {}",
            fleet.captures.len(),
            c.polls_started as f64 / minutes,
            percentile(&steady, 0.5),
            percentile(&steady, 0.95),
            steady.iter().copied().fold(0.0, f64::max),
            percentile(&ticks, 0.5),
            percentile(&ticks, 0.95),
            peak_per_minute(&c.stamps),
            stats.map_or("n/a".into(), |s| format!("{:.0}", s.0 as f64 / minutes)),
        );
    }
    if !on("concurrent") {
        db.drop().await;
        return;
    }

    // Counterfactual only: the worker polls one capture at a time. These runs
    // call `poll` directly, C at once, to show where concurrency would move the
    // ceiling (latency-bound vs DB-bound). Idle and active mixed as above.
    fleet.latency(lo, hi);
    eprintln!("SCALE concurrent | mix | C | wall ms | polls/s | broker calls | DB statements | DB exec ms");
    for (mix, fraction, c) in [0.0, active].into_iter().flat_map(|f| [1usize, 8, 32, 64].map(move |c| (if f == 0.0 { "idle" } else { "mixed" }, f, c))) {
        *fleet.mock.active_fraction.lock().unwrap() = fraction;
        let mut rates = Vec::new();
        let mut walls = Vec::new();
        let (mut calls, mut stmts, mut dbms) = (0u64, None, None);
        for r in 0..3 {
            make_all_due(&db).await;
            fleet.refresh_keys();
            let picked: Vec<_> = fleet.captures.iter().cycle().skip(r * 97).take(c.min(fleet.captures.len())).cloned().collect();
            fleet.mock.take();
            db.reset_stats().await;
            let t = Instant::now();
            let statuses: Vec<u16> = futures::future::join_all(picked.iter().map(|p| fleet.poll(p))).await;
            let secs = t.elapsed().as_secs_f64();
            for (p, status) in picked.iter().zip(&statuses).filter(|(_, s)| **s != 200) {
                let code: Option<String> = sqlx::query_scalar("SELECT error_code FROM composio_capture_scopes WHERE id=$1").bind(p.0).fetch_one(&db.owner).await.unwrap();
                eprintln!("SCALE concurrent failure | {mix} | C={c} | {} | status {status} | error_code {code:?}", p.1);
            }
            walls.push(secs * 1000.0);
            rates.push(picked.len() as f64 / secs);
            calls = fleet.mock.take().total();
            if let Some((s, ms)) = db.stats().await {
                stmts = Some(s);
                dbms = Some(ms);
            }
        }
        eprintln!("SCALE concurrent | {mix} | {c} | {} | {} | {calls} | {} | {}", fmt(&walls), fmt(&rates), stmts.map_or("n/a".into(), |s| s.to_string()), dbms.map_or("n/a".into(), |m| format!("{m:.1}")));
    }
    db.drop().await;
}

/// Exercise the real poller's namespace-check hook. Routine control timestamps
/// must use the durable freshness notification, not a reducing account event.
#[tokio::test]
async fn namespace_recheck_preserves_usable_stored_answer() {
    use coralswarm_backend::{
        read_models as rm,
        tenancy::scope::begin_tenant_scoped,
        workers::{read_model_refresh, tenant_harness::WorkerTenant},
    };
    let dsn = std::env::var("READ_MODEL_TEST_BASE_URL")
        .expect("owned PG required; no skipped namespace regression");
    let opts = PgConnectOptions::from_str(&dsn).unwrap();
    assert!(matches!(opts.get_host(), "localhost" | "127.0.0.1"));
    let database = coralswarm_backend::test_db::migrated_database(
        &opts,
        coralswarm_backend::test_db::Plane::Control,
        "l3title_namespace",
    )
    .await;
    let owner = PgPoolOptions::new()
        .max_connections(4)
        .connect_with(opts.clone().database(database.name()))
        .await
        .unwrap();
    let role = format!("l3title_namespace_{}", Uuid::new_v4().simple());
    let password = Uuid::new_v4().to_string();
    sqlx::query(&format!("CREATE ROLE {role} LOGIN INHERIT NOSUPERUSER NOBYPASSRLS PASSWORD '{password}' IN ROLE coralswarm_app")).execute(&owner).await.unwrap();
    let app = PgPoolOptions::new()
        .max_connections(4)
        .connect_with(
            opts.database(database.name())
                .username(&role)
                .password(&password),
        )
        .await
        .unwrap();
    let attrs:(bool,bool,i64)=sqlx::query_as("SELECT rolsuper,rolbypassrls,(SELECT count(*) FROM pg_class WHERE relnamespace='public'::regnamespace AND relowner=(SELECT oid FROM pg_roles WHERE rolname=current_user)) FROM pg_roles WHERE rolname=current_user").fetch_one(&app).await.unwrap();
    assert_eq!(attrs, (false, false, 0));
    // Reuse the broker/activation fixture without the legacy fixed-name DB
    // constructor or its global serving-role password mutation.
    let name = Box::leak(database.name().to_owned().into_boxed_str());
    let db = Db {
        _database: database,
        dsn,
        name,
        owner,
        app,
        pgss: false,
    };
    eprintln!(
        "NAMESPACE_RESOURCE database={} role={role}",
        db._database.name()
    );
    let mut fleet = Fleet::new(&db).await;
    fleet.grow(&db, 1).await;
    let capture = &fleet.captures[0];
    let user = &capture.2;
    sqlx::query("UPDATE composio_connections SET namespace_checked_at=now()-interval '2 days' WHERE account_id=$1").bind(user.account_id).execute(&db.owner).await.unwrap();
    type Model = rm::connectors::ConnectorsList;
    async fn scope(app: &PgPool, user: &UserContext) -> (rm::Scope<rm::connectors::NoParams>, i64) {
        let mut tx = begin_tenant_scoped(app, user.account_id, user.user_id)
            .await
            .unwrap();
        let mut viewer = rm::Viewer::load(&mut tx, user.account_id, user.user_id)
            .await
            .unwrap();
        viewer.certify_control(&mut tx).await.unwrap();
        let freshness: i64 = sqlx::query_scalar(
            "SELECT freshness FROM read_model_control_revisions WHERE account_id=$1",
        )
        .bind(user.account_id)
        .fetch_one(&mut *tx)
        .await
        .unwrap();
        (
            viewer.scope::<Model>(rm::connectors::NoParams {}).unwrap(),
            freshness,
        )
    }
    let (before, freshness_before) = scope(&db.app, user).await;
    rm::store::request_refresh::<Model>(&db.app, &before)
        .await
        .unwrap();
    let claim = rm::store::claim(&db.app).await.unwrap().remove(0);
    let worker = WorkerTenant {
        tenant: coralswarm_backend::tenancy::TenantId::Consumer,
        pool: db.app.clone(),
        store: Arc::new(coralswarm_backend::store::PostgresStore::new(
            db.app.clone(),
        )),
    };
    let (payload, snapshot) = read_model_refresh::compute(&worker, &claim, &db.app)
        .await
        .unwrap();
    assert!(
        rm::store::publish(&db.app, &claim, payload.clone(), &snapshot)
            .await
            .unwrap()
    );
    *fleet.mock.force_pages.lock().unwrap() = Some(0);
    fleet.mock.take();
    assert_eq!(fleet.poll(capture).await, 200);
    assert!(
        fleet.mock.take().sum(Call::Namespace) > 0,
        "must execute actual live namespace recheck"
    );
    let (after, freshness_after) = scope(&db.app, user).await;
    assert_eq!(before.key(), after.key());
    assert!(
        freshness_after > freshness_before,
        "namespace timestamps must commit a durable control notification"
    );
    let mut tx = begin_tenant_scoped(&db.app, user.account_id, user.user_id)
        .await
        .unwrap();
    let stored = rm::store::read::<Model>(&mut tx, &after).await.unwrap();
    assert!(
        stored.computed_at.is_some(),
        "ordinary namespace recheck must not withhold the warmed model"
    );
    assert!(stored.stale, "actual poll must durably notify freshness");
    assert_eq!(stored.payload, payload);
    tx.rollback().await.unwrap();
    db.app.close().await;
    sqlx::query(&format!("DROP ROLE {role}"))
        .execute(&db.owner)
        .await
        .unwrap();
    db.owner.close().await;
}
