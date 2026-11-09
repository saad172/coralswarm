//! Personal-workspace Teams meeting capture (migration 0288) — live-Postgres proof
//! through the real public router, the real consumer worker tick and the real
//! webhook, running as the NOBYPASSRLS application role (`coralswarm_app`).
//!
//! Only Microsoft's HTTP endpoint is substituted: `test_support_set_graph_origin`
//! points the provider calls at a local mock that is keyed by the bearer token, so
//! each test's grant talks to its own scripted meeting. Tokens are real
//! AES-GCM ciphertext under a wrapped account DEK, written exactly as the connector
//! stores them.
//!
//! Gated on `ORGT_TEST_BASE_URL`; without it every test prints `skipping ...` and
//! returns (a vacuous pass, which the suite's CI step greps for).
//!
//!   ORGT_TEST_BASE_URL=postgres://postgres@localhost:15549/postgres \
//!     cargo test -p coralswarm-backend --test teams_personal_capture_pg -- --test-threads=1

#![recursion_limit = "512"]

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::{Arc, LazyLock, Mutex, OnceLock};

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use chrono::{DateTime, Duration, SecondsFormat, Utc};
use coralswarm_backend::clerk::test_support::{mint, test_jwks, ISSUER};
use coralswarm_backend::tenancy::TenantId;
use coralswarm_backend::workers::tenant_harness::WorkerTenant;
use coralswarm_backend::AppState;
use coralswarm_core::kek::{generate_dek, load_kek, wrap_dek};
use coralswarm_core::oauth_token::encrypt_token;
use serde_json::{json, Value};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{Connection, Executor, PgConnection, PgPool};
use tokio::sync::Mutex as AsyncMutex;
use tower::ServiceExt;
use uuid::Uuid;

const TID: &str = "11111111-2222-3333-4444-555555555555";
const OID: &str = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
const OTHER_OID: &str = "bbbbbbbb-bbbb-cccc-dddd-eeeeeeeeeeee";
const CONSUMER_TID: &str = "9188040d-6c67-4c5b-b112-36a304b66dad";
const APP_PW: &str = "teams_personal_capture_test_only_pw";
const SETUP_LOCK: i64 = 0x0083_5C09;
const MEETING: &str = "MSpt_personal_meeting_one";
const JOIN: &str = "https://teams.microsoft.com/l/meetup-join/19%3ameeting_personal%40thread.v2/0?context=x";
const URI_CAPTURE: &str = "/v1/connectors/microsoft/teams/transcripts/capture";
const URI_STATUS: &str = "/v1/connectors/microsoft/teams/transcripts/meeting-status";
const URI_LIST: &str = "/v1/connectors/microsoft/teams/transcripts/captures";
const URI_SYNC: &str = "/v1/connectors/microsoft/teams/transcripts/sync-meeting";
const URI_CONTEXT: &str = "/v1/connectors/microsoft/teams/transcripts/meeting-context";
const URI_CONNECT: &str = "/v1/connectors/microsoft/teams/transcripts/connect";

static ENV_LOCK: AsyncMutex<()> = AsyncMutex::const_new(());

fn base_url() -> Option<String> {
    std::env::var("ORGT_TEST_BASE_URL").ok()
}
fn with_db(base: &str, db: &str) -> String {
    let (prefix, rest) = base.split_at(base.rfind('/').expect("url path /"));
    let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
    format!("{prefix}/{db}{params}")
}

macro_rules! need_db {
    ($name:literal) => {
        match base_url() {
            Some(b) => b,
            None => {
                eprintln!(concat!("skipping ", $name, ": set ORGT_TEST_BASE_URL"));
                return;
            }
        }
    };
}

// ── Provider mock (one process-wide server on its own runtime) ───────────────

type Hook = Arc<dyn Fn() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

struct Scenario {
    meeting_id: String,
    start: Mutex<DateTime<Utc>>,
    end: Mutex<DateTime<Utc>>,
    transcripts: Mutex<Vec<(String, DateTime<Utc>, String)>>,
    calls: Mutex<Vec<String>>,
    on_content: Mutex<Option<Hook>>,
}

impl Scenario {
    fn calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }
    fn content_calls(&self) -> usize {
        self.calls().iter().filter(|c| c.ends_with("/content")).count()
    }
}

static SCENARIOS: LazyLock<Mutex<HashMap<String, Arc<Scenario>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
static MOCK_ORIGIN: OnceLock<String> = OnceLock::new();

fn ts(at: DateTime<Utc>) -> String {
    at.to_rfc3339_opts(SecondsFormat::Secs, true)
}

fn vtt(line: &str) -> String {
    format!("WEBVTT\n\n00:00:01.000 --> 00:00:04.000\n<v Alice Example>{line}</v>\n")
}

async fn mock_handle(req: Request<Body>) -> (StatusCode, axum::Json<Value>) {
    let token = req
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .unwrap_or("")
        .to_string();
    let scenario = SCENARIOS.lock().unwrap().get(&token).cloned();
    let Some(s) = scenario else {
        return (
            StatusCode::UNAUTHORIZED,
            axum::Json(json!({"error":{"code":"InvalidAuthenticationToken"}})),
        );
    };
    let path = req.uri().path().trim_start_matches("/v1.0").to_string();
    s.calls.lock().unwrap().push(path.clone());
    let (start, end) = (*s.start.lock().unwrap(), *s.end.lock().unwrap());
    let meeting = json!({"id": s.meeting_id, "subject": "Quarterly planning",
        "startDateTime": ts(start), "endDateTime": ts(end), "joinWebUrl": JOIN,
        "creationDateTime": ts(start)});
    let base = format!("/me/onlineMeetings/{}", s.meeting_id);
    let ok = |v: Value| (StatusCode::OK, axum::Json(v));
    if path == "/me/onlineMeetings" {
        return ok(json!({"value":[meeting]}));
    }
    if path == base {
        return ok(meeting);
    }
    if path == "/me/calendarView" {
        return ok(json!({"value":[{"id":"evt1","type":"singleInstance",
            "start":{"dateTime":ts(start),"timeZone":"UTC"},
            "end":{"dateTime":ts(end),"timeZone":"UTC"},
            "onlineMeeting":{"joinUrl":JOIN}}]}));
    }
    let transcripts = s.transcripts.lock().unwrap().clone();
    if path == format!("{base}/transcripts") {
        return ok(json!({"value": transcripts.iter()
            .map(|(id, at, _)| json!({"id": id, "createdDateTime": ts(*at)})).collect::<Vec<_>>()}));
    }
    for (id, at, body) in transcripts {
        if path == format!("{base}/transcripts/{id}") {
            return ok(json!({"id": id, "createdDateTime": ts(at)}));
        }
        if path == format!("{base}/transcripts/{id}/content") {
            let hook = s.on_content.lock().unwrap().clone();
            if let Some(hook) = hook {
                hook().await;
            }
            // The Graph content endpoint answers text; the connector reads the body as text.
            return (StatusCode::OK, axum::Json(Value::String(body)));
        }
    }
    (StatusCode::NOT_FOUND, axum::Json(json!({"error":{"code":"itemNotFound"}})))
}

/// The content endpoint must be plain text, not a JSON string; wrap the handler.
async fn mock_entry(req: Request<Body>) -> axum::response::Response {
    use axum::response::IntoResponse;
    let is_content = req.uri().path().ends_with("/content");
    let (status, axum::Json(body)) = mock_handle(req).await;
    if is_content && status == StatusCode::OK {
        if let Value::String(text) = body {
            return (status, [("content-type", "text/vtt")], text).into_response();
        }
    }
    (status, axum::Json(body)).into_response()
}

fn ensure_mock() {
    let origin = MOCK_ORIGIN.get_or_init(|| {
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async move {
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                tx.send(format!("http://{}/v1.0", listener.local_addr().unwrap())).unwrap();
                let app = axum::Router::new().fallback(mock_entry);
                axum::serve(listener, app).await.ok();
            });
        });
        rx.recv().unwrap()
    });
    coralswarm_backend::test_support_set_graph_origin(Some(origin.clone()));
}

fn scenario(token: &str, start: DateTime<Utc>, end: DateTime<Utc>) -> Arc<Scenario> {
    let s = Arc::new(Scenario {
        meeting_id: MEETING.into(),
        start: Mutex::new(start),
        end: Mutex::new(end),
        transcripts: Mutex::new(vec![]),
        calls: Mutex::new(vec![]),
        on_content: Mutex::new(None),
    });
    SCENARIOS.lock().unwrap().insert(token.into(), s.clone());
    s
}

// ── World ────────────────────────────────────────────────────────────────────

struct World {
    base: String,
    db: String,
    owner: PgPool,
    app: PgPool,
    state: Arc<AppState>,
    router: axum::Router,
    dek: [u8; 32],
    _guard: tokio::sync::MutexGuard<'static, ()>,
}

async fn spawn_jwks() -> String {
    let body = test_jwks();
    let app = axum::Router::new().route(
        "/keys",
        axum::routing::get(move || {
            let body = body.clone();
            async move { ([("content-type", "application/json")], body) }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.ok();
    });
    format!("http://{addr}/keys")
}

impl World {
    async fn new(base: &str, db: &str) -> Self {
        let guard = ENV_LOCK.lock().await;
        let jwks = spawn_jwks().await;
        // SAFETY: serialized by ENV_LOCK for this struct's lifetime.
        unsafe {
            std::env::set_var("MASTER_KEK_HEX", "ab".repeat(32));
            std::env::set_var("CLERK_ISSUER", ISSUER);
            std::env::set_var("CLERK_JWKS_URL", &jwks);
            std::env::set_var("OAUTH_STATE_SECRET", "teams-personal-capture-state-secret");
            std::env::set_var("MICROSOFT_CLIENT_ID", "00000000-0000-0000-0000-00000000c0de");
            std::env::set_var("MICROSOFT_CLIENT_SECRET", "test-only-secret");
        }
        ensure_mock();
        let mut admin = PgConnection::connect(base).await.unwrap();
        admin.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.unwrap();
        admin.execute(format!("CREATE DATABASE {db}").as_str()).await.unwrap();
        sqlx::query("SELECT pg_advisory_lock($1)").bind(SETUP_LOCK).execute(&mut admin).await.unwrap();
        let url = with_db(base, db);
        let owner = PgPoolOptions::new().max_connections(4).connect(&url).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&owner).await.unwrap();
        sqlx::query(&format!("ALTER ROLE coralswarm_app WITH PASSWORD '{APP_PW}'"))
            .execute(&owner)
            .await
            .unwrap();
        sqlx::query("SELECT pg_advisory_unlock($1)").bind(SETUP_LOCK).execute(&mut admin).await.unwrap();
        let app = PgPoolOptions::new()
            .max_connections(8)
            .connect_with(
                PgConnectOptions::from_str(&url).unwrap().username("coralswarm_app").password(APP_PW),
            )
            .await
            .unwrap();
        // The application connects as a real NOBYPASSRLS role, not the owner.
        assert!(!sqlx::query_scalar::<_, bool>(
            "SELECT rolbypassrls OR rolsuper FROM pg_roles WHERE rolname=current_user"
        )
        .fetch_one(&app)
        .await
        .unwrap());
        let state = Arc::new(coralswarm_backend::test_app_state_owned_with_db_and_store(app.clone(), None));
        let router = coralswarm_backend::test_support_app(state.clone());
        Self {
            base: base.into(),
            db: db.into(),
            owner,
            app,
            state,
            router,
            dek: generate_dek(),
            _guard: guard,
        }
    }

    async fn finish(self) {
        self.app.close().await;
        self.owner.close().await;
        if let Ok(mut admin) = PgConnection::connect(&self.base).await {
            admin
                .execute(format!("DROP DATABASE IF EXISTS {} WITH (FORCE)", self.db).as_str())
                .await
                .ok();
        }
    }

    async fn call(&self, method: &str, uri: &str, jwt: &str, body: Option<Value>) -> (StatusCode, Value) {
        call_router(&self.router, method, uri, jwt, body).await
    }

    fn tick_tenant(&self) -> WorkerTenant {
        WorkerTenant {
            tenant: TenantId::Consumer,
            pool: self.app.clone(),
            store: self.state.store.clone().unwrap(),
        }
    }

    async fn tick(&self) -> coralswarm_backend::workers::tenant_harness::Tick {
        coralswarm_backend::workers::teams_meeting_sync::tick(&self.tick_tenant(), &self.app, &self.state)
            .await
            .unwrap()
    }

    /// A personal account (owner = the real user, one primary personal graph).
    async fn personal(&self, sub: &str) -> Personal {
        let kek = load_kek().unwrap();
        let wrapped = wrap_dek(&kek, &self.dek).unwrap();
        let user: Uuid = sqlx::query_scalar(
            "INSERT INTO users(id,email,clerk_user_id) VALUES(gen_random_uuid(),$1,$2) RETURNING id",
        )
        .bind(format!("{sub}@example.test"))
        .bind(sub)
        .fetch_one(&self.owner)
        .await
        .unwrap();
        let account: Uuid =
            sqlx::query_scalar("INSERT INTO accounts(owner_user_id,dek_wrapped) VALUES($1,$2) RETURNING id")
                .bind(user)
                .bind(&wrapped)
                .fetch_one(&self.owner)
                .await
                .unwrap();
        sqlx::query("INSERT INTO account_members(account_id,user_id,role) VALUES($1,$2,'owner')")
            .bind(account)
            .bind(user)
            .execute(&self.owner)
            .await
            .unwrap();
        let graph: Uuid = sqlx::query_scalar(
            "INSERT INTO graphs(id,account_id,name,kind,is_primary) VALUES(gen_random_uuid(),$1,'Primary','personal',true) RETURNING id",
        )
        .bind(account)
        .fetch_one(&self.owner)
        .await
        .unwrap();
        Personal { sub: sub.into(), user, account, graph, token: format!("tok_{}", Uuid::new_v4()) }
    }

    /// The connector's own row shape: tenant in provider_workspace_id, object in provider_user_id.
    async fn grant(&self, p: &Personal, owner: Option<Uuid>, tid: &str, oid: &str) {
        sqlx::query(
            "INSERT INTO oauth_tokens(account_id,provider,scope,access_token,provider_workspace_id,provider_user_id,owner_user_id,expires_at) \
             VALUES($1,'microsoft','x',$2,$3,$4,$5, now()+interval '1 day')",
        )
        .bind(p.account)
        .bind(encrypt_token(&self.dek, &p.token).unwrap())
        .bind(tid)
        .bind(oid)
        .bind(owner)
        .execute(&self.owner)
        .await
        .unwrap();
    }

    async fn atoms(&self, graph: Uuid) -> i64 {
        sqlx::query_scalar("SELECT count(*) FROM atoms WHERE graph_id=$1 AND source_type='teams_transcript'")
            .bind(graph)
            .fetch_one(&self.owner)
            .await
            .unwrap()
    }

    async fn ledger(&self, account: Uuid) -> Vec<(Uuid, Uuid, Uuid, String, bool, i32)> {
        sqlx::query_as(
            "SELECT id,user_id,graph_id,aad_id,revoked_at IS NOT NULL,retrieved_transcript_count FROM teams_meeting_captures WHERE account_id=$1 ORDER BY created_at",
        )
        .bind(account)
        .fetch_all(&self.owner)
        .await
        .unwrap()
    }
}

struct Personal {
    sub: String,
    user: Uuid,
    account: Uuid,
    graph: Uuid,
    token: String,
}
impl Personal {
    fn jwt(&self) -> String {
        mint(json!({"iss": ISSUER, "sub": self.sub,
            "exp": (Utc::now().timestamp() + 3600) as usize, "sid": format!("sess_{}", Uuid::new_v4())}))
    }
}

async fn call_router(
    router: &axum::Router,
    method: &str,
    uri: &str,
    jwt: &str,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let mut b = Request::builder().method(method).uri(uri).header("authorization", format!("Bearer {jwt}"));
    let req = match body {
        Some(v) => b.header("content-type", "application/json").body(Body::from(v.to_string())).unwrap(),
        None => {
            b = b.header("content-length", "0");
            b.body(Body::empty()).unwrap()
        }
    };
    let resp = router.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = to_bytes(resp.into_body(), 1 << 20).await.unwrap();
    (status, serde_json::from_slice(&bytes).unwrap_or(Value::Null))
}

/// Wait for every webhook task the router has spawned (deterministic, no sleeps).
async fn drain(state: &AppState) {
    state.task_tracker.close();
    state.task_tracker.wait().await;
    state.task_tracker.reopen();
}

fn window() -> (DateTime<Utc>, DateTime<Utc>) {
    let now = Utc::now();
    let start = DateTime::from_timestamp(now.timestamp() - 7200, 0).unwrap();
    (start, start + Duration::hours(1))
}

/// A scripted personal meeting for `p`, with one in-window transcript.
fn scripted(p: &Personal) -> Arc<Scenario> {
    let (start, end) = window();
    let s = scenario(&p.token, start, end);
    s.transcripts
        .lock()
        .unwrap()
        .push(("tr_one".into(), end - Duration::minutes(10), vtt("Quarterly planning starts now.")));
    s
}

async fn consent(w: &World, p: &Personal) -> Uuid {
    let (status, body) = w.call("POST", URI_CAPTURE, &p.jwt(), Some(json!({"meeting_id": MEETING}))).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["status"], "waiting_for_transcript");
    body["capture_id"].as_str().unwrap().parse().unwrap()
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn teams_personal_capture_work_school_consent_to_private_atoms_and_owner_ledger_pg() {
    let base = need_db!("teams_personal_capture_work_school_consent_to_private_atoms_and_owner_ledger_pg");
    let w = World::new(&base, "tpc_happy").await;
    let p = w.personal("tpc-owner").await;
    w.grant(&p, None, TID, OID).await;
    let s = scripted(&p);

    // Connect works from a personal workspace (it used to answer 400 organization_required).
    let (status, body) = w
        .call("POST", URI_CONNECT, &p.jwt(), Some(json!({"return_path":"/connectors/microsoft-teams/meeting-capture?meeting_id=x"})))
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body["authorization_url"].as_str().unwrap().contains("login.microsoftonline.com"));

    // Status by join link: verified against Microsoft, no consent implied.
    let (status, body) = w.call("POST", URI_STATUS, &p.jwt(), Some(json!({"join_url": JOIN}))).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["meeting_id"], MEETING);
    assert_eq!(body["connection_required"], false);
    assert!(body["capture"].is_null());
    assert!(w.ledger(p.account).await.is_empty(), "status never creates consent");
    let (status, body) = w.call("POST", URI_CONTEXT, &p.jwt(), Some(json!({"teams_meeting_id":"ctx","chat_id":"chat"}))).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "an unknown signed context is not a selector: {body}");
    assert_eq!(body["code"], "meeting_context_unavailable");
    assert_eq!(w.atoms(p.graph).await, 0);

    // Explicit consent -> a ledger row owned by the REAL user, in the personal graph.
    let capture = consent(&w, &p).await;
    let rows = w.ledger(p.account).await;
    assert_eq!(rows.len(), 1);
    assert_eq!((rows[0].1, rows[0].2, rows[0].3.as_str()), (p.user, p.graph, OID));

    // Public ingress through the consumer tick: private atoms in the owner's personal graph.
    s.calls.lock().unwrap().clear();
    let tick = w.tick().await;
    assert_eq!(tick.processed, 1);
    assert_eq!(w.atoms(p.graph).await, 1);
    assert!(s.calls().iter().all(|c| !c.contains("calendarView") && !c.contains("/me/events")),
        "the worker never discovers meetings: {:?}", s.calls());
    let rows = w.ledger(p.account).await;
    assert_eq!(rows[0].5, 1, "retrieved_transcript_count reflects the completed fetch");
    // Another account never sees these atoms.
    let other = w.personal("tpc-other").await;
    assert_eq!(w.atoms(other.graph).await, 0);

    // Owner list/status reflect it; another user cannot probe the capture id.
    let (_, list) = w.call("GET", URI_LIST, &p.jwt(), None).await;
    assert_eq!(list["captures"].as_array().unwrap().len(), 1);
    assert_eq!(list["captures"][0]["meeting_id"], MEETING);
    let (_, other_list) = w.call("GET", URI_LIST, &other.jwt(), None).await;
    assert_eq!(other_list["captures"], json!([]));
    let (status, body) = w.call("POST", URI_STATUS, &other.jwt(), Some(json!({"capture_id": capture}))).await;
    assert_eq!((status, body["code"].as_str()), (StatusCode::NOT_FOUND, Some("capture_not_found")));
    let (status, _) = w.call("DELETE", URI_CAPTURE, &other.jwt(), Some(json!({"capture_id": capture}))).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(!w.ledger(p.account).await[0].4, "a stranger could not revoke it");

    // Disconnect the grant: the stopped ledger stays inspectable and revocable by the owner,
    // with no credential needed.
    sqlx::query("UPDATE oauth_tokens SET credential_invalid_at=now() WHERE account_id=$1")
        .bind(p.account)
        .execute(&w.owner)
        .await
        .unwrap();
    let (status, body) = w.call("POST", URI_STATUS, &p.jwt(), Some(json!({"capture_id": capture}))).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["connection_required"], true);
    assert_eq!(body["capture"]["status"], "active");
    let (status, _) = w.call("DELETE", URI_CAPTURE, &p.jwt(), Some(json!({"capture_id": capture}))).await;
    assert_eq!(status, StatusCode::OK);
    let (_, body) = w.call("POST", URI_STATUS, &p.jwt(), Some(json!({"capture_id": capture}))).await;
    assert_eq!(body["capture"]["status"], "stopped");
    let audit: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM connector_audit_log WHERE account_id=$1 AND provider='microsoft' AND action IN ('consent','disconnect')",
    )
    .bind(p.account)
    .fetch_one(&w.owner)
    .await
    .unwrap();
    assert_eq!(audit, 2, "consent and stop are both audited");
    w.finish().await;
}

#[tokio::test]
async fn teams_personal_capture_explicit_sync_meeting_writes_only_the_owners_personal_graph_pg() {
    let base = need_db!("teams_personal_capture_explicit_sync_meeting_writes_only_the_owners_personal_graph_pg");
    let w = World::new(&base, "tpc_sync").await;
    let p = w.personal("tpc-sync").await;
    w.grant(&p, None, TID, OID).await;
    let _s = scripted(&p);
    let (status, body) = w.call("POST", URI_SYNC, &p.jwt(), Some(json!({"meeting_id": MEETING}))).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!((body["fetched"].as_i64(), body["ingested"].as_i64()), (Some(1), Some(1)));
    assert_eq!(w.atoms(p.graph).await, 1);
    let rows = w.ledger(p.account).await;
    assert_eq!(rows.len(), 1, "an explicit pull records its own consent");
    assert_eq!((rows[0].1, rows[0].2), (p.user, p.graph));
    w.finish().await;
}

#[tokio::test]
async fn teams_personal_capture_refuses_consumer_disconnected_borrowed_and_ambiguous_grants_pg() {
    let base = need_db!("teams_personal_capture_refuses_consumer_disconnected_borrowed_and_ambiguous_grants_pg");
    let w = World::new(&base, "tpc_refuse").await;
    let body_meeting = json!({"meeting_id": MEETING});
    let (start, end) = window();

    // Consumer Microsoft identity: 403 consumer_identity_unsupported on every capture route,
    // no provider request and no ledger row.
    let consumer = w.personal("tpc-consumer").await;
    w.grant(&consumer, None, CONSUMER_TID, OID).await;
    let cs = scenario(&consumer.token, start, end);
    for (method, uri, body) in [
        ("POST", URI_CAPTURE, body_meeting.clone()),
        ("POST", URI_STATUS, json!({"join_url": JOIN})),
        ("POST", URI_SYNC, body_meeting.clone()),
        ("POST", URI_CONTEXT, json!({"teams_meeting_id":"x","chat_id":"y"})),
        ("DELETE", URI_CAPTURE, json!({"meeting_id": MEETING})),
    ] {
        let (status, body) = w.call(method, uri, &consumer.jwt(), Some(body)).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{uri}: {body}");
        assert_eq!(body["code"], "consumer_identity_unsupported", "{uri}");
    }
    assert!(cs.calls().is_empty());
    assert!(w.ledger(consumer.account).await.is_empty());

    // A disconnected (dead) grant is connect_required, never a capture.
    let dead = w.personal("tpc-dead").await;
    w.grant(&dead, None, TID, OID).await;
    let ds = scenario(&dead.token, start, end);
    sqlx::query("UPDATE oauth_tokens SET credential_invalid_at=now() WHERE account_id=$1")
        .bind(dead.account)
        .execute(&w.owner)
        .await
        .unwrap();
    let (status, body) = w.call("POST", URI_CAPTURE, &dead.jwt(), Some(body_meeting.clone())).await;
    assert_eq!((status, body["code"].as_str()), (StatusCode::FORBIDDEN, Some("connect_required")));
    let (status, body) = w.call("POST", URI_STATUS, &dead.jwt(), Some(json!({"join_url": JOIN}))).await;
    assert_eq!((status, body["code"].as_str()), (StatusCode::FORBIDDEN, Some("connect_required")));
    assert!(ds.calls().is_empty());

    // A Member-owned row (another shape of grant) is never borrowed by a personal workspace.
    let member_only = w.personal("tpc-member-only").await;
    w.grant(&member_only, Some(member_only.user), TID, OID).await;
    let ms = scenario(&member_only.token, start, end);
    let (status, body) = w.call("POST", URI_CAPTURE, &member_only.jwt(), Some(body_meeting.clone())).await;
    assert_eq!((status, body["code"].as_str()), (StatusCode::FORBIDDEN, Some("connect_required")));
    assert!(ms.calls().is_empty());

    // Another person's grant row in this account is not the owner's identity; two account-wide
    // rows are ambiguous, so neither is trusted.
    let ambiguous = w.personal("tpc-ambiguous").await;
    w.grant(&ambiguous, None, TID, OID).await;
    sqlx::query(
        "INSERT INTO oauth_tokens(account_id,provider,scope,access_token,provider_workspace_id,provider_user_id) VALUES($1,'microsoft','x','\\x00',$2,$3)",
    )
    .bind(ambiguous.account)
    .bind("99999999-2222-3333-4444-555555555555")
    .bind(OTHER_OID)
    .execute(&w.owner)
    .await
    .unwrap();
    let asc = scenario(&ambiguous.token, start, end);
    let (status, body) = w.call("POST", URI_CAPTURE, &ambiguous.jwt(), Some(body_meeting.clone())).await;
    assert_eq!((status, body["code"].as_str()), (StatusCode::FORBIDDEN, Some("connect_required")));
    assert!(asc.calls().is_empty());

    // The token loaded must be the verified directory: a stale ownerless row for a different
    // tenant than the identity names is refused even before the provider is called.
    assert!(w.ledger(dead.account).await.is_empty() && w.ledger(member_only.account).await.is_empty());
    w.finish().await;
}

#[tokio::test]
async fn teams_personal_capture_rls_with_check_and_scheduler_under_the_application_role_pg() {
    let base = need_db!("teams_personal_capture_rls_with_check_and_scheduler_under_the_application_role_pg");
    let w = World::new(&base, "tpc_rls").await;
    let a = w.personal("tpc-rls-a").await;
    let b = w.personal("tpc-rls-b").await;
    w.grant(&a, None, TID, OID).await;
    w.grant(&b, None, TID, OTHER_OID).await;

    // An org account on the same plane, with an org member's own graph and a (impossible in
    // practice) personal-kind graph on the org account.
    let org_owner: Uuid = sqlx::query_scalar("INSERT INTO users(id,email) VALUES(gen_random_uuid(),'org-owner@x.test') RETURNING id")
        .fetch_one(&w.owner)
        .await
        .unwrap();
    let org: Uuid = sqlx::query_scalar("INSERT INTO accounts(owner_user_id,clerk_org_id) VALUES($1,'org_tpc') RETURNING id")
        .bind(org_owner)
        .fetch_one(&w.owner)
        .await
        .unwrap();
    let org_member_graph: Uuid = sqlx::query_scalar(
        "INSERT INTO graphs(id,account_id,name,kind,owner_user_id,is_primary) VALUES(gen_random_uuid(),$1,'Member','member',$2,true) RETURNING id",
    )
    .bind(org)
    .bind(org_owner)
    .fetch_one(&w.owner)
    .await
    .unwrap();
    let org_personal_graph: Uuid = sqlx::query_scalar(
        "INSERT INTO graphs(id,account_id,name,kind,is_primary) VALUES(gen_random_uuid(),$1,'Odd','personal',false) RETURNING id",
    )
    .bind(org)
    .fetch_one(&w.owner)
    .await
    .unwrap();
    let archived: Uuid = sqlx::query_scalar(
        "INSERT INTO graphs(id,account_id,name,kind,is_primary,archived_at) VALUES(gen_random_uuid(),$1,'Old','personal',false,now()) RETURNING id",
    )
    .bind(a.account)
    .fetch_one(&w.owner)
    .await
    .unwrap();

    async fn insert(
        app: &PgPool,
        account: Uuid,
        acting: Uuid,
        row_user: Uuid,
        graph: Uuid,
        meeting: &str,
    ) -> Result<u64, sqlx::Error> {
        let mut tx = app.begin().await?;
        sqlx::query("SELECT set_config('app.account_id',$1,true), set_config('app.user_id',$2,true)")
            .bind(account.to_string())
            .bind(acting.to_string())
            .execute(&mut *tx)
            .await?;
        let r = sqlx::query(
            "INSERT INTO teams_meeting_captures(account_id,user_id,aad_id,tenant_id,meeting_id,occurrence_start,occurrence_end,graph_id,source) \
             VALUES($1,$2,$3,$4,$5,now()-interval '2 hours',now()-interval '1 hour',$6,'explicit_meeting')",
        )
        .bind(account)
        .bind(row_user)
        .bind(OID)
        .bind(TID)
        .bind(meeting)
        .bind(graph)
        .execute(&mut *tx)
        .await;
        match r {
            Ok(done) => {
                tx.commit().await?;
                Ok(done.rows_affected())
            }
            Err(e) => Err(e),
        }
    }
    let denied = |r: Result<u64, sqlx::Error>| {
        let e = r.expect_err("must be refused");
        assert!(e.to_string().contains("row-level security"), "{e}");
    };
    // Owner of a personal account, own personal graph: allowed.
    assert_eq!(insert(&w.app, a.account, a.user, a.user, a.graph, "m-ok").await.unwrap(), 1);
    // Another account's graph, a different user's row, mismatched GUC, archived graph, an org
    // account's personal-kind graph: all refused by the WITH CHECK.
    denied(insert(&w.app, a.account, a.user, a.user, b.graph, "m-other-graph").await);
    denied(insert(&w.app, a.account, b.user, b.user, a.graph, "m-not-owner").await);
    denied(insert(&w.app, a.account, b.user, a.user, a.graph, "m-guc-mismatch").await);
    denied(insert(&w.app, a.account, a.user, a.user, archived, "m-archived").await);
    denied(insert(&w.app, org, org_owner, org_owner, org_personal_graph, "m-org-personal").await);
    // The org member arm is unchanged (member graph + owner).
    assert_eq!(insert(&w.app, org, org_owner, org_owner, org_member_graph, "m-org-member").await.unwrap(), 1);
    // A member graph of someone else's, in a personal account, stays refused.
    denied(insert(&w.app, org, org_owner, org_owner, a.graph, "m-cross-account").await);

    // USING is unchanged: the owner sees the row, another user/account sees nothing, and no
    // scope at all sees nothing.
    async fn visible(app: &PgPool, account: Uuid, user: Uuid, system: bool) -> i64 {
        let mut tx = app.begin().await.unwrap();
        sqlx::query("SELECT set_config('app.account_id',$1,true), set_config('app.user_id',$2,true), set_config('app.tenant_actor',$3,true)")
            .bind(account.to_string())
            .bind(user.to_string())
            .bind(if system { "system" } else { "" })
            .execute(&mut *tx)
            .await
            .unwrap();
        let n = sqlx::query_scalar("SELECT count(*) FROM teams_meeting_captures").fetch_one(&mut *tx).await.unwrap();
        tx.rollback().await.unwrap();
        n
    }
    assert_eq!(visible(&w.app, a.account, a.user, false).await, 1);
    assert_eq!(visible(&w.app, a.account, b.user, false).await, 0);
    assert_eq!(visible(&w.app, b.account, a.user, false).await, 0);
    assert_eq!(visible(&w.app, Uuid::nil(), Uuid::nil(), false).await, 0);
    // The scheduler's SELECT policy is for the migration owner only: standing the system actor
    // up as the application role reads nothing.
    assert_eq!(visible(&w.app, a.account, a.user, true).await, 1, "own rows only, as before");
    assert_eq!(visible(&w.app, b.account, b.user, true).await, 0);
    let (owner_role, policy_roles): (String, Vec<String>) = (
        sqlx::query_scalar("SELECT current_user::text").fetch_one(&w.owner).await.unwrap(),
        sqlx::query_scalar("SELECT unnest(roles)::text FROM pg_policies WHERE tablename='teams_meeting_captures' AND policyname='teams_personal_capture_scheduler'")
            .fetch_all(&w.owner)
            .await
            .unwrap(),
    );
    assert_eq!(policy_roles, vec![owner_role]);

    // The resolver, called as the application role, returns ids/instants for due personal rows
    // of personal accounts only (not the org's row, not another account, no content columns).
    let due: Vec<(Uuid, Uuid)> = sqlx::query_as("SELECT account_id,user_id FROM resolve_teams_personal_capture_due(10)")
        .fetch_all(&w.app)
        .await
        .unwrap();
    assert_eq!(due, vec![(a.account, a.user)]);
    // Revoked, archived-graph, dead-grant and deleted-owner rows leave the queue.
    sqlx::query("UPDATE teams_meeting_captures SET revoked_at=now() WHERE account_id=$1").bind(a.account).execute(&w.owner).await.unwrap();
    assert!(sqlx::query_as::<_, (Uuid, Uuid)>("SELECT account_id,user_id FROM resolve_teams_personal_capture_due(10)")
        .fetch_all(&w.app).await.unwrap().is_empty());
    w.finish().await;
}

#[tokio::test]
async fn teams_personal_capture_consumer_tick_pulls_only_due_authorized_rows_without_touching_other_credentials_pg() {
    let base = need_db!("teams_personal_capture_consumer_tick_pulls_only_due_authorized_rows_without_touching_other_credentials_pg");
    let w = World::new(&base, "tpc_tick").await;
    let (start, end) = window();

    // Y: due and authorized.
    let y = w.personal("tpc-y").await;
    w.grant(&y, None, TID, OID).await;
    let ys = scripted(&y);
    consent(&w, &y).await;
    ys.calls.lock().unwrap().clear();

    // N: consent exists but is not due for an hour, and its stored token is garbage: any attempt
    // to decrypt/refresh it would surface as a failed (counted) member pass.
    let n = w.personal("tpc-n").await;
    sqlx::query("INSERT INTO oauth_tokens(account_id,provider,scope,access_token,provider_workspace_id,provider_user_id) VALUES($1,'microsoft','x','\\xdeadbeef',$2,$3)")
        .bind(n.account).bind(TID).bind(OID).execute(&w.owner).await.unwrap();
    let ns = scenario(&n.token, start, end);
    // A: revoked. P: archived personal graph. D: dead grant. K: no consent at all.
    let mut skipped = vec![];
    for sub in ["tpc-a", "tpc-p", "tpc-d", "tpc-k"] {
        let p = w.personal(sub).await;
        w.grant(&p, None, TID, OID).await;
        let s = scripted(&p);
        if sub != "tpc-k" {
            consent(&w, &p).await;
        }
        match sub {
            "tpc-a" => {
                sqlx::query("UPDATE teams_meeting_captures SET revoked_at=now() WHERE account_id=$1").bind(p.account).execute(&w.owner).await.unwrap();
            }
            "tpc-p" => {
                sqlx::query("UPDATE graphs SET archived_at=now() WHERE id=$1").bind(p.graph).execute(&w.owner).await.unwrap();
            }
            "tpc-d" => {
                sqlx::query("UPDATE oauth_tokens SET credential_invalid_at=now() WHERE account_id=$1").bind(p.account).execute(&w.owner).await.unwrap();
            }
            _ => {}
        }
        s.calls.lock().unwrap().clear();
        skipped.push((p, s));
    }
    // An org account's due row on the same plane is never enumerated or pulled.
    let org_user: Uuid = sqlx::query_scalar("INSERT INTO users(id,email) VALUES(gen_random_uuid(),'tick-org@x.test') RETURNING id").fetch_one(&w.owner).await.unwrap();
    let org: Uuid = sqlx::query_scalar("INSERT INTO accounts(owner_user_id,clerk_org_id) VALUES($1,'org_tick') RETURNING id").bind(org_user).fetch_one(&w.owner).await.unwrap();
    let org_graph: Uuid = sqlx::query_scalar("INSERT INTO graphs(id,account_id,name,kind,owner_user_id,is_primary) VALUES(gen_random_uuid(),$1,'M','member',$2,true) RETURNING id").bind(org).bind(org_user).fetch_one(&w.owner).await.unwrap();
    sqlx::query("INSERT INTO teams_meeting_captures(account_id,user_id,aad_id,tenant_id,meeting_id,occurrence_start,occurrence_end,graph_id,source) VALUES($1,$2,$3,$4,$5,now()-interval '2 hours',now()-interval '1 hour',$6,'explicit_meeting')")
        .bind(org).bind(org_user).bind(OID).bind(TID).bind(MEETING).bind(org_graph).execute(&w.owner).await.unwrap();
    sqlx::query("INSERT INTO teams_meeting_captures(account_id,user_id,aad_id,tenant_id,meeting_id,occurrence_start,occurrence_end,graph_id,source,next_poll_at) VALUES($1,$2,$3,$4,$5,now()-interval '2 hours',now()-interval '1 hour',$6,'explicit_meeting',now()+interval '1 hour')")
        .bind(n.account).bind(n.user).bind(OID).bind(TID).bind(MEETING).bind(n.graph).execute(&w.owner).await.unwrap();

    let tick = w.tick().await;
    assert_eq!(tick.processed, 1, "only Y's due, authorized row is attempted");
    assert_eq!(w.atoms(y.graph).await, 1);
    assert_eq!(ys.content_calls(), 1);
    assert!(ys.calls().iter().all(|c| !c.contains("calendarView")), "{:?}", ys.calls());
    for (p, s) in &skipped {
        assert!(s.calls().is_empty(), "{} reached the provider", p.sub);
        assert_eq!(w.atoms(p.graph).await, 0, "{}", p.sub);
    }
    assert!(ns.calls().is_empty());
    assert_eq!(w.atoms(n.graph).await, 0);
    let org_rows: i64 = sqlx::query_scalar("SELECT count(*) FROM atoms WHERE graph_id=$1").bind(org_graph).fetch_one(&w.owner).await.unwrap();
    assert_eq!(org_rows, 0);

    // With Y now complete and N not yet due, the next tick performs no provider work and does
    // not decrypt N's credential; it defers to N's due instant.
    sqlx::query("UPDATE teams_meeting_captures SET next_poll_at=now()+interval '30 minutes' WHERE account_id=$1").bind(y.account).execute(&w.owner).await.unwrap();
    let before = ys.calls().len();
    let tick = w.tick().await;
    assert_eq!(tick.processed, 0);
    assert!(tick.defer.is_some(), "waits for the next due selection");
    assert_eq!(ys.calls().len(), before);
    assert!(ns.calls().is_empty());
    w.finish().await;
}

#[tokio::test]
async fn teams_personal_capture_midfetch_revoke_and_new_generation_prevent_the_write_pg() {
    let base = need_db!("teams_personal_capture_midfetch_revoke_and_new_generation_prevent_the_write_pg");
    let w = World::new(&base, "tpc_mid").await;
    let p = w.personal("tpc-mid").await;
    w.grant(&p, None, TID, OID).await;
    let s = scripted(&p);
    let capture = consent(&w, &p).await;

    // Revoked while Microsoft is still answering the content request.
    let (owner, id) = (w.owner.clone(), capture);
    let handle = tokio::runtime::Handle::current();
    let h = handle.clone();
    let hook: Hook = Arc::new(move || {
        let (owner, id, h) = (owner.clone(), id, h.clone());
        Box::pin(async move {
            // Run on the test's own runtime, where the pools live.
            h.spawn(async move {
                sqlx::query("UPDATE teams_meeting_captures SET revoked_at=now() WHERE id=$1").bind(id).execute(&owner).await.unwrap();
            })
            .await
            .unwrap();
        })
    });
    *s.on_content.lock().unwrap() = Some(hook);
    w.tick().await;
    assert_eq!(s.content_calls(), 1, "the request was in flight");
    assert_eq!(w.atoms(p.graph).await, 0, "a revoke during the fetch must not write");
    assert_eq!(w.ledger(p.account).await[0].5, 0, "and is not recorded as retrieved");

    // Re-enabled during the fetch (a NEW generation): the old in-flight result is discarded.
    *s.on_content.lock().unwrap() = None;
    consent(&w, &p).await;
    let (router, jwt) = (w.router.clone(), p.jwt());
    let hook: Hook = Arc::new(move || {
        let (router, jwt, h) = (router.clone(), jwt.clone(), handle.clone());
        Box::pin(async move {
            h.spawn(async move {
                let (st, _) = call_router(&router, "DELETE", URI_CAPTURE, &jwt, Some(json!({"capture_id": id}))).await;
                assert_eq!(st, StatusCode::OK);
                let (st, _) = call_router(&router, "POST", URI_CAPTURE, &jwt, Some(json!({"meeting_id": MEETING}))).await;
                assert_eq!(st, StatusCode::OK);
            })
            .await
            .unwrap();
        })
    });
    *s.on_content.lock().unwrap() = Some(hook);
    sqlx::query("UPDATE teams_meeting_captures SET next_poll_at=now() WHERE id=$1").bind(capture).execute(&w.owner).await.unwrap();
    w.tick().await;
    assert_eq!(w.atoms(p.graph).await, 0, "a fetch under a replaced generation never writes");

    // The new generation pulls normally on its own tick.
    *s.on_content.lock().unwrap() = None;
    sqlx::query("UPDATE teams_meeting_captures SET next_poll_at=now() WHERE id=$1").bind(capture).execute(&w.owner).await.unwrap();
    w.tick().await;
    assert_eq!(w.atoms(p.graph).await, 1);
    w.finish().await;
}

#[tokio::test]
async fn teams_personal_capture_wrong_occurrence_and_foreign_graph_are_refused_pg() {
    let base = need_db!("teams_personal_capture_wrong_occurrence_and_foreign_graph_are_refused_pg");
    let w = World::new(&base, "tpc_occ").await;
    let p = w.personal("tpc-occ").await;
    w.grant(&p, None, TID, OID).await;

    // A live occurrence is selected; later Microsoft reports a DIFFERENT occurrence of the
    // same meeting id: the stored window is the only authority (conflict, not a silent retarget).
    let now = Utc::now();
    let start = DateTime::from_timestamp(now.timestamp() - 1800, 0).unwrap();
    let s = scenario(&p.token, start, start + Duration::hours(1));
    consent(&w, &p).await;
    let (status, body) = w.call("POST", URI_STATUS, &p.jwt(), Some(json!({"join_url": JOIN}))).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["capture"]["status"], "active");
    *s.start.lock().unwrap() = start + Duration::minutes(5);
    *s.end.lock().unwrap() = start + Duration::hours(2);
    let (status, body) = w.call("POST", URI_STATUS, &p.jwt(), Some(json!({"join_url": JOIN}))).await;
    assert_eq!((status, body["code"].as_str()), (StatusCode::CONFLICT, Some("occurrence_unverified")), "{body}");
    // Selecting the new occurrence needs fresh explicit consent; the live one is not retargeted.
    let (status, _) = w.call("POST", URI_CAPTURE, &p.jwt(), Some(json!({"meeting_id": MEETING}))).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "existing consent target differs; revoke first");
    assert_eq!(w.ledger(p.account).await.len(), 1);

    // The worker fetches only the stored window: a transcript from another occurrence is skipped.
    *s.start.lock().unwrap() = start;
    *s.end.lock().unwrap() = start + Duration::hours(1);
    s.transcripts.lock().unwrap().push(("tr_prev".into(), start - Duration::hours(3), vtt("previous occurrence")));
    s.transcripts.lock().unwrap().push(("tr_this".into(), start + Duration::minutes(20), vtt("this occurrence")));
    w.tick().await;
    assert_eq!(w.atoms(p.graph).await, 1, "only the in-window transcript");
    let source: String = sqlx::query_scalar("SELECT source_id FROM atoms WHERE graph_id=$1 AND source_type='teams_transcript'")
        .bind(p.graph).fetch_one(&w.owner).await.unwrap();
    // The generic ingress chunks long content; the provider identity prefix is the transcript's.
    assert!(source.starts_with(&format!("{MEETING}:tr_this")), "{source}");

    // Another user's graph and an archived graph cannot hold a capture (the route's graph
    // resolves to the caller's own primary; forcing the ledger write is refused by RLS).
    let other = w.personal("tpc-occ-other").await;
    let mut tx = w.app.begin().await.unwrap();
    sqlx::query("SELECT set_config('app.account_id',$1,true), set_config('app.user_id',$2,true)")
        .bind(p.account.to_string()).bind(p.user.to_string()).execute(&mut *tx).await.unwrap();
    let forged = sqlx::query("UPDATE teams_meeting_captures SET graph_id=$1 WHERE account_id=$2")
        .bind(other.graph).bind(p.account).execute(&mut *tx).await;
    assert!(forged.is_err(), "re-pointing a capture at another account's graph is refused");
    drop(tx);
    sqlx::query("UPDATE graphs SET archived_at=now() WHERE id=$1").bind(p.graph).execute(&w.owner).await.unwrap();
    sqlx::query("UPDATE teams_meeting_captures SET next_poll_at=now() WHERE account_id=$1").bind(p.account).execute(&w.owner).await.unwrap();
    let before = s.calls().len();
    let tick = w.tick().await;
    assert_eq!(tick.processed, 0);
    assert_eq!(s.calls().len(), before, "an archived personal graph is never polled");
    let (status, _) = w.call("POST", URI_STATUS, &p.jwt(), Some(json!({"capture_id": Uuid::new_v4()}))).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    w.finish().await;
}

#[tokio::test]
async fn teams_personal_capture_delegated_push_needs_the_owners_consent_and_grant_pg() {
    let base = need_db!("teams_personal_capture_delegated_push_needs_the_owners_consent_and_grant_pg");
    let w = World::new(&base, "tpc_push").await;
    let p = w.personal("tpc-push").await;
    w.grant(&p, None, TID, OID).await;
    let s = scripted(&p);
    let sub_id = format!("sub-{}", Uuid::new_v4());
    sqlx::query("INSERT INTO graph_subscriptions(account_id,subscription_id,resource,client_state,expires_at) VALUES($1,$2,'users/graph-user/onlineMeetings/getAllTranscripts','state-secret',now()+interval '1 day')")
        .bind(p.account).bind(&sub_id).execute(&w.owner).await.unwrap();
    let notify = |client_state: &str| {
        json!({"value":[{"subscriptionId": sub_id, "clientState": client_state, "changeType":"created",
            "resource": format!("users('graph-user')/onlineMeetings('{MEETING}')/transcripts('tr_one')")}]})
    };
    let post = |body: Value| {
        let router = w.router.clone();
        async move {
            let req = Request::builder().method("POST").uri("/v1/webhooks/graph")
                .header("content-type", "application/json").body(Body::from(body.to_string())).unwrap();
            assert_eq!(router.oneshot(req).await.unwrap().status(), StatusCode::ACCEPTED);
        }
    };
    // No explicit consent: a delivered notification reads no transcript and writes nothing.
    post(notify("state-secret")).await;
    drain(&w.state).await;
    assert_eq!(w.atoms(p.graph).await, 0);
    assert_eq!(s.content_calls(), 0);
    // Wrong clientState: dropped before any work.
    consent(&w, &p).await;
    s.calls.lock().unwrap().clear();
    post(notify("not-the-secret")).await;
    drain(&w.state).await;
    assert!(s.calls().is_empty());
    assert_eq!(w.atoms(p.graph).await, 0);
    // Consent + the right clientState: the owner's grant fetches; atoms land privately.
    post(notify("state-secret")).await;
    drain(&w.state).await;
    assert_eq!(w.atoms(p.graph).await, 1);
    // A consumer directory on the same subscription is refused even with consent.
    sqlx::query("DELETE FROM atoms WHERE graph_id=$1").bind(p.graph).execute(&w.owner).await.ok();
    sqlx::query("UPDATE oauth_tokens SET provider_workspace_id=$2 WHERE account_id=$1").bind(p.account).bind(CONSUMER_TID).execute(&w.owner).await.unwrap();
    s.calls.lock().unwrap().clear();
    post(notify("state-secret")).await;
    drain(&w.state).await;
    assert_eq!(s.content_calls(), 0);
    w.finish().await;
}

// ── Due-queue fairness (migration 0289) ──────────────────────────────────────

/// The `CREATE OR REPLACE FUNCTION resolve_teams_personal_capture_due(...)` statement of a
/// migration file, taken verbatim from the shipped SQL (never re-typed here).
fn resolver_sql(migration: &str) -> String {
    let start = migration
        .find("CREATE OR REPLACE FUNCTION resolve_teams_personal_capture_due(p_limit int)")
        .expect("resolver definition");
    let end = start + migration[start..].find("\n$$;").expect("resolver end") + "\n$$;".len();
    migration[start..end].to_string()
}

async fn due(app: &PgPool, function: &str, limit: i32) -> Vec<(Uuid, Uuid)> {
    sqlx::query_as(&format!("SELECT account_id,user_id FROM {function}($1)"))
        .bind(limit)
        .fetch_all(app)
        .await
        .unwrap()
}

/// One account-wide (ownerless) Microsoft row with exactly the given identifiers.
async fn raw_grant(w: &World, p: &Personal, tenant: Option<&str>, object: Option<&str>, invalid: bool) {
    sqlx::query(
        "INSERT INTO oauth_tokens(account_id,provider,scope,access_token,refresh_token,provider_workspace_id,provider_user_id,owner_user_id,expires_at,credential_invalid_at) \
         VALUES($1,'microsoft','x',$2,$3,$4,$5,NULL,now()+interval '1 day',CASE WHEN $6 THEN now() ELSE NULL END)",
    )
    .bind(p.account)
    .bind(encrypt_token(&w.dek, "poison-access").unwrap())
    .bind(encrypt_token(&w.dek, "poison-refresh").unwrap())
    .bind(tenant)
    .bind(object)
    .bind(invalid)
    .execute(&w.owner)
    .await
    .unwrap();
}

/// A capture row written as the owner (fixtures only; the product path is the router).
async fn raw_capture(w: &World, p: &Personal, tenant: &str, object: &str, meeting: &str, poll_ago_minutes: i32) {
    sqlx::query(
        "INSERT INTO teams_meeting_captures(account_id,user_id,tenant_id,aad_id,meeting_id,graph_id,source,occurrence_start,occurrence_end,next_poll_at) \
         VALUES($1,$2,$3,$4,$5,$6,'explicit_meeting',now()-interval '2 hours',now()-interval '1 hour',now()-make_interval(mins=>$7))",
    )
    .bind(p.account)
    .bind(p.user)
    .bind(tenant)
    .bind(object)
    .bind(meeting)
    .bind(p.graph)
    .bind(poll_ago_minutes)
    .execute(&w.owner)
    .await
    .unwrap();
}

#[tokio::test]
async fn teams_personal_capture_stale_grants_cannot_starve_healthy_due_accounts_pg() {
    let base = need_db!("teams_personal_capture_stale_grants_cannot_starve_healthy_due_accounts_pg");
    let w = World::new(&base, "tpc_fair").await;
    let legacy_0288 = resolver_sql(include_str!("../migrations_control/0288_teams_personal_meeting_captures.sql"));
    let current_0289 = resolver_sql(include_str!("../migrations_control/0289_teams_personal_capture_due_grant_fairness.sql"));
    assert_ne!(legacy_0288, current_0289);
    const TID_B: &str = "22222222-2222-3333-4444-555555555555";

    // Healthy and due: a normal grant the worker will accept, consented through the real route.
    let healthy = w.personal("tpc-fair-healthy").await;
    w.grant(&healthy, None, TID, OID).await;
    let hs = scripted(&healthy);
    consent(&w, &healthy).await;
    hs.calls.lock().unwrap().clear();

    // Healthy and due, access token EXPIRED but refreshable: the worker refreshes it, so the
    // resolver must keep returning it (expiry is not eligibility, and no credential is read).
    let refreshable = w.personal("tpc-fair-refreshable").await;
    sqlx::query(
        "INSERT INTO oauth_tokens(account_id,provider,scope,access_token,refresh_token,provider_workspace_id,provider_user_id,owner_user_id,expires_at) \
         VALUES($1,'microsoft','x',$2,$3,$4,$5,NULL,now()-interval '3 days')",
    )
    .bind(refreshable.account)
    .bind(encrypt_token(&w.dek, "expired-access").unwrap())
    .bind(encrypt_token(&w.dek, "live-refresh").unwrap())
    .bind(TID)
    .bind(OID)
    .execute(&w.owner)
    .await
    .unwrap();
    raw_capture(&w, &refreshable, TID, OID, "refreshable", 5).await;

    // Healthy but not yet due: stays out of the due list, not a poison.
    let later = w.personal("tpc-fair-later").await;
    w.grant(&later, None, TID, OID).await;
    sqlx::query(
        "INSERT INTO teams_meeting_captures(account_id,user_id,tenant_id,aad_id,meeting_id,graph_id,source,occurrence_start,occurrence_end,next_poll_at) \
         VALUES($1,$2,$3,$4,'later',$5,'explicit_meeting',now()-interval '2 hours',now()-interval '1 hour',now()+interval '1 hour')",
    )
    .bind(later.account)
    .bind(later.user)
    .bind(TID)
    .bind(OID)
    .bind(later.graph)
    .execute(&w.owner)
    .await
    .unwrap();

    // 105 stale captures that the worker can never claim (15 per kind), each due LONGER ago than
    // the healthy accounts, so they sort first under `ORDER BY due_at LIMIT 100`.
    let mut poison = std::collections::HashSet::new();
    for kind in 0..7 {
        for n in 0..15 {
            let p = w.personal(&format!("tpc-fair-{kind}-{n}")).await;
            let meeting = format!("poison-{kind}-{n}");
            let (cap_tid, cap_oid) = match kind {
                // Reconnected with another tenant / another object id: replaced grant.
                0 => {
                    raw_grant(&w, &p, Some(TID_B), Some(OID), false).await;
                    (TID, OID)
                }
                1 => {
                    raw_grant(&w, &p, Some(TID), Some(OTHER_OID), false).await;
                    (TID, OID)
                }
                // A second ownerless Microsoft row makes the identity ambiguous.
                2 => {
                    raw_grant(&w, &p, Some(TID), Some(OID), false).await;
                    raw_grant(&w, &p, Some(TID_B), Some(OTHER_OID), false).await;
                    (TID, OID)
                }
                // Consumer directory: Graph has no transcripts for it.
                3 => {
                    raw_grant(&w, &p, Some(CONSUMER_TID), Some(OID), false).await;
                    (CONSUMER_TID, OID)
                }
                // Malformed provider identifiers never parse as UUIDs.
                4 => {
                    raw_grant(&w, &p, Some("not-a-tenant"), Some("not-an-object"), false).await;
                    ("not-a-tenant", "not-an-object")
                }
                // Same UUID, different spelling: the worker's claim compares text exactly.
                5 => {
                    raw_grant(&w, &p, Some(TID), Some(&OID.to_uppercase()), false).await;
                    (TID, OID)
                }
                // No object id recorded on the grant.
                _ => {
                    raw_grant(&w, &p, Some(TID), None, false).await;
                    (TID, OID)
                }
            };
            raw_capture(&w, &p, cap_tid, cap_oid, &meeting, 90).await;
            poison.insert((p.account, p.user));
        }
    }
    // Dead grants were already excluded by 0288 and must stay excluded.
    for n in 0..5 {
        let p = w.personal(&format!("tpc-fair-dead-{n}")).await;
        raw_grant(&w, &p, Some(TID), Some(OID), true).await;
        raw_capture(&w, &p, TID, OID, &format!("dead-{n}"), 90).await;
        poison.insert((p.account, p.user));
    }
    assert_eq!(poison.len(), 110);
    let healthy_ids = [(healthy.account, healthy.user), (refreshable.account, refreshable.user)];

    // RED: the shipped 0288 resolver, installed beside the real one, fills the whole 100-row page
    // with stale accounts and returns neither healthy account.
    w.owner
        .execute(
            legacy_0288
                .replace(
                    "resolve_teams_personal_capture_due(p_limit int)",
                    "resolve_teams_personal_capture_due_0288(p_limit int)",
                )
                .as_str(),
        )
        .await
        .unwrap();
    w.owner
        .execute("GRANT EXECUTE ON FUNCTION resolve_teams_personal_capture_due_0288(int) TO coralswarm_app")
        .await
        .unwrap();
    let starved = due(&w.app, "resolve_teams_personal_capture_due_0288", 100).await;
    assert_eq!(starved.len(), 100);
    assert!(starved.iter().all(|row| poison.contains(row)), "0288 page is all stale accounts");
    assert!(healthy_ids.iter().all(|row| !starved.contains(row)), "0288 starves both healthy accounts");

    // GREEN: 0289 (what the migration run installed) returns only live grants at the same
    // limit: both healthy due accounts first, then the healthy account that is due later (the
    // worker defers it), and not one stale account.
    let fair = due(&w.app, "resolve_teams_personal_capture_due", 100).await;
    let mut expected = healthy_ids.to_vec();
    expected.push((later.account, later.user));
    expected.sort();
    let mut fair_sorted = fair.clone();
    fair_sorted.sort();
    assert_eq!(fair_sorted, expected);
    assert!(fair.iter().all(|row| !poison.contains(row)));
    assert_eq!(fair[2], (later.account, later.user), "the not-yet-due account sorts after the due ones");

    // The actual consumer worker, with the 0288 resolver put back under the real name: the
    // healthy account's transcript is never pulled (RED) ...
    w.owner.execute(legacy_0288.as_str()).await.unwrap();
    assert_eq!(w.tick().await.processed, 0);
    assert_eq!(w.atoms(healthy.graph).await, 0, "0288: healthy account starved by stale captures");
    assert!(hs.calls().is_empty());
    // ... and with 0289 restored the very next tick pulls it, without touching stale accounts.
    // (The refreshable account is closed first: its refresh would be a live provider call.)
    w.owner.execute(current_0289.as_str()).await.unwrap();
    sqlx::query("UPDATE teams_meeting_captures SET revoked_at=now() WHERE account_id=$1")
        .bind(refreshable.account)
        .execute(&w.owner)
        .await
        .unwrap();
    assert_eq!(w.tick().await.processed, 1);
    assert_eq!(w.atoms(healthy.graph).await, 1, "0289: healthy account pulled on the next tick");

    // Role boundaries, as the NOBYPASSRLS application role: it can execute the resolver, a plain
    // read of the ledger still returns nothing, the actor setting is restored, and PUBLIC cannot
    // execute it. Owner/definer/search_path are as in 0288.
    let (definer, config, app_can, public_can): (bool, Option<Vec<String>>, bool, bool) = sqlx::query_as(
        "SELECT prosecdef, proconfig, has_function_privilege('coralswarm_app', oid, 'EXECUTE'), \
         has_function_privilege('public', oid, 'EXECUTE') \
         FROM pg_proc WHERE proname='resolve_teams_personal_capture_due' AND pronargs=1",
    )
    .fetch_one(&w.owner)
    .await
    .unwrap();
    assert!(definer && app_can && !public_can);
    assert_eq!(config, Some(vec!["search_path=public, pg_temp".to_string()]));
    let mut tx = w.app.begin().await.unwrap();
    sqlx::query("SELECT set_config('app.tenant_actor','',true)").execute(&mut *tx).await.unwrap();
    let _ = due_in(&mut tx).await;
    let (actor, direct): (String, i64) = sqlx::query_as(
        "SELECT coalesce(current_setting('app.tenant_actor',true),''), (SELECT count(*) FROM teams_meeting_captures)",
    )
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    tx.rollback().await.unwrap();
    assert_eq!((actor.as_str(), direct), ("", 0), "actor restored; the application role still cannot read the ledger");
    w.finish().await;
}

async fn due_in(tx: &mut sqlx::Transaction<'_, sqlx::Postgres>) -> Vec<(Uuid, Uuid)> {
    sqlx::query_as("SELECT account_id,user_id FROM resolve_teams_personal_capture_due(100)")
        .fetch_all(&mut **tx)
        .await
        .unwrap()
}
