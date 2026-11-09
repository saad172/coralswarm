//! #1495 `BACKEND_ROLE`: boot the real `coralswarm-backend` binary in each
//! role against a migrated scratch control database and check what it starts.
//!
//! * `http`: full API on the public port, and NO job workers. Proven two
//!   ways: no `... spawned` worker log line, and a pending
//!   `ContentTsvRegconfigRepair` seed job is never claimed.
//! * `workers`: the public port answers only `/healthz` and `/readyz` (an API
//!   route is 404, not 401), and the same job is claimed.
//! * unset (`all`): API and workers, as before #1495.
//! * an unknown value refuses to start.
//!
//! The binary runs as a subprocess (not in-process `run()`), because `run()`
//! installs a process-global tracing subscriber and its log lines are what
//! the "no workers" assertion reads.
//!
//! Gated on `ORGT_TEST_BASE_URL` (CREATE DATABASE rights); with
//! `REQUIRE_PG_SUITE` set a missing URL fails instead of skipping.
//!
//!   ORGT_TEST_BASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test backend_role_boot_pg -- --test-threads=1

#[cfg(not(feature = "binaries"))]
compile_error!("This suite runs utilities. Enable defaults or --features binaries.");

use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use coralswarm_backend::jobs::JobKind;
use sqlx::postgres::PgPoolOptions;
use sqlx::{Connection, Executor, PgConnection, PgPool};
use uuid::Uuid;

fn base_url() -> Option<String> {
    match std::env::var("ORGT_TEST_BASE_URL") {
        Ok(url) => Some(url),
        Err(_) if std::env::var_os("REQUIRE_PG_SUITE").is_some() => {
            panic!("ORGT_TEST_BASE_URL required")
        }
        Err(_) => None,
    }
}

fn with_db(base: &str, db: &str) -> String {
    let (prefix, rest) = base.split_at(base.rfind('/').expect("url has a path /"));
    let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
    format!("{prefix}/{db}{params}")
}

/// A migrated scratch database, dropped on `cleanup`.
struct ScratchDb {
    _database: coralswarm_backend::test_db::TestDatabase,
    base: String,
    name: String,
    url: String,
}

impl ScratchDb {
    async fn create(base: &str, name: &str) -> Self {
        let database = coralswarm_backend::test_db::TestDatabase::adopt_from_url(base, name);
        let mut admin = PgConnection::connect(base).await.expect("connect admin");
        admin.execute(format!("DROP DATABASE IF EXISTS {name} WITH (FORCE)").as_str()).await.unwrap();
        admin.execute(format!("CREATE DATABASE {name}").as_str()).await.unwrap();
        let url = with_db(base, name);
        let out = Command::new(env!("CARGO_BIN_EXE_migrate_control"))
            .env_remove("DATABASE_URL")
            .env("MIGRATOR_DATABASE_URL", &url)
            .output()
            .expect("spawn migrate_control");
        assert!(
            out.status.success(),
            "migrate_control must succeed\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        ScratchDb { _database: database, base: base.to_string(), name: name.to_string(), url }
    }

    async fn pool(&self) -> PgPool {
        PgPoolOptions::new().max_connections(2).connect(&self.url).await.expect("scratch pool")
    }

    async fn cleanup(self) {
        if let Ok(mut admin) = PgConnection::connect(&self.base).await {
            let _ = admin
                .execute(format!("DROP DATABASE IF EXISTS {} WITH (FORCE)", self.name).as_str())
                .await;
        }
    }
}

/// The backend subprocess and the file its stdout/stderr go to.
struct Backend {
    child: Child,
    log: PathBuf,
    public: String,
    internal: String,
}

impl Backend {
    fn spawn(db_url: &str, role: Option<&str>, port: u16, internal_port: u16) -> Self {
        let log = std::env::temp_dir().join(format!("backend_role_boot_{}.log", Uuid::new_v4().simple()));
        let out = std::fs::File::create(&log).expect("log file");
        let err = out.try_clone().expect("log file clone");
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_coralswarm-backend"));
        cmd.env("DATABASE_URL", db_url)
            .env_remove("MIGRATOR_DATABASE_URL")
            .env_remove("WORKER_WAKE_DATABASE_URL")
            .env("PORT", port.to_string())
            .env("INTERNAL_PORT", internal_port.to_string())
            .env("INTERNAL_BIND_ADDR", "127.0.0.1")
            .env("RUST_LOG", "info")
            .stdout(Stdio::from(out))
            .stderr(Stdio::from(err));
        match role {
            Some(r) => cmd.env("BACKEND_ROLE", r),
            None => cmd.env_remove("BACKEND_ROLE"),
        };
        let child = cmd.spawn().expect("spawn coralswarm-backend");
        Backend {
            child,
            log,
            public: format!("http://127.0.0.1:{port}"),
            internal: format!("http://127.0.0.1:{internal_port}"),
        }
    }

    fn log_text(&self) -> String {
        std::fs::read_to_string(&self.log).unwrap_or_default()
    }

    async fn wait_ready(&mut self) {
        let http = reqwest::Client::new();
        let deadline = Instant::now() + Duration::from_secs(60);
        while Instant::now() < deadline {
            if let Ok(Some(status)) = self.child.try_wait() {
                panic!("backend exited early ({status}):\n{}", self.log_text());
            }
            if let Ok(resp) = http.get(format!("{}/readyz", self.public)).send().await {
                if resp.status().is_success() {
                    return;
                }
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        panic!("backend never became ready:\n{}", self.log_text());
    }

    async fn status(&self, url: String) -> u16 {
        reqwest::Client::new().get(url).send().await.expect("request").status().as_u16()
    }

    /// SIGTERM, then wait (bounded) for a clean exit; returns the log.
    fn stop(mut self) -> String {
        if let Ok(None) = self.child.try_wait() {
            let _ = Command::new("kill").args(["-TERM", &self.child.id().to_string()]).status();
        }
        let deadline = Instant::now() + Duration::from_secs(40);
        while Instant::now() < deadline {
            if let Ok(Some(_)) = self.child.try_wait() {
                break;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
        let text = self.log_text();
        let _ = std::fs::remove_file(&self.log);
        text
    }
}

/// Log lines announcing a spawned background worker. `startup.rs` logs
/// "<name> worker spawned" (and a few "... sweep spawned") for every worker;
/// "NOT spawned" warnings, the wake bridge's own line, the tenant-pool
/// eviction sweep (both roles) and the HTTP drain summary are excluded.
fn worker_spawn_lines(log: &str) -> Vec<&str> {
    log.lines()
        .filter(|l| l.contains("spawned") && !l.contains("NOT spawned") && !l.contains("worker wake"))
        .filter(|l| !l.contains("tenant pool idle-eviction sweep spawned"))
        // The HTTP drain's own summary of `task_tracker` tasks, not a worker.
        .filter(|l| !l.contains("handler-spawned background tasks"))
        .collect()
}

/// How long the canary job gets. The `workers` and `all` tests must see it
/// claimed inside this window, so the `http` test waiting the same window
/// and seeing it still pending is a real negative, not a slow poll.
const CLAIM_WINDOW: Duration = Duration::from_secs(8);

async fn enqueue_canary(pool: &PgPool) -> Uuid {
    coralswarm_backend::jobs::queue::enqueue(
        pool,
        &JobKind::ContentTsvRegconfigRepair { after_atom_id: None },
        None,
        Some(&format!("backend_role_canary_{}", Uuid::new_v4().simple())),
        0,
    )
    .await
    .expect("enqueue canary")
    .expect("canary inserted")
}

async fn job_status(pool: &PgPool, id: Uuid) -> String {
    sqlx::query_scalar("SELECT status::text FROM jobs WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("job status")
}

async fn wait_claimed(pool: &PgPool, id: Uuid, within: Duration) -> Option<String> {
    let deadline = Instant::now() + within;
    while Instant::now() < deadline {
        let s = job_status(pool, id).await;
        if s != "pending" {
            return Some(s);
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    None
}

#[tokio::test]
async fn http_role_serves_the_api_and_spawns_no_workers() {
    let Some(base) = base_url() else { return eprintln!("skipping: set ORGT_TEST_BASE_URL") };
    let db = ScratchDb::create(&base, "orgt_backend_role_http").await;
    let pool = db.pool().await;
    let canary = enqueue_canary(&pool).await;

    let mut backend = Backend::spawn(&db.url, Some("http"), 18290, 18291);
    backend.wait_ready().await;
    assert_eq!(backend.status(format!("{}/healthz", backend.public)).await, 200);
    // An authenticated API route exists (401 without a token), not 404.
    assert_eq!(backend.status(format!("{}/v1/me", backend.public)).await, 401);
    // The window the other roles claim the job in; nothing may claim it here.
    tokio::time::sleep(CLAIM_WINDOW).await;
    assert_eq!(job_status(&pool, canary).await, "pending", "http role must not run workers");
    let log = backend.stop();

    assert!(log.contains("backend role resolved") && log.contains("\"backend_role\":\"http\""), "{log}");
    assert!(log.contains("spawns NO job workers"), "the http role announces it runs no workers");
    assert!(log.contains("worker wake publisher spawned"), "{log}");
    let spawned = worker_spawn_lines(&log);
    assert!(spawned.is_empty(), "http role spawned workers: {spawned:#?}");
    pool.close().await;
    db.cleanup().await;
}

#[tokio::test]
async fn workers_role_serves_only_health_on_the_public_port_and_runs_workers() {
    let Some(base) = base_url() else { return eprintln!("skipping: set ORGT_TEST_BASE_URL") };
    let db = ScratchDb::create(&base, "orgt_backend_role_workers").await;
    let pool = db.pool().await;
    let canary = enqueue_canary(&pool).await;

    let mut backend = Backend::spawn(&db.url, Some("workers"), 18292, 18293);
    backend.wait_ready().await;
    assert_eq!(backend.status(format!("{}/healthz", backend.public)).await, 200);
    assert_eq!(backend.status(format!("{}/readyz", backend.public)).await, 200);
    for path in ["/v1/me", "/mcp", "/openapi.json", "/"] {
        assert_eq!(
            backend.status(format!("{}{path}", backend.public)).await,
            404,
            "workers role must not serve {path} on the public port"
        );
    }
    // The internal router stays bound (flyctl proxy may land on this machine):
    // a token-gated route answers (401/403), it is not connection-refused.
    let internal = backend.status(format!("{}/internal/admin/fleet", backend.internal)).await;
    assert!(matches!(internal, 401 | 403 | 503), "internal router answered {internal}");

    let claimed = wait_claimed(&pool, canary, CLAIM_WINDOW).await;
    let log = backend.stop();
    assert!(claimed.is_some(), "workers role must claim the canary job:\n{log}");
    assert!(log.contains("\"backend_role\":\"workers\""), "{log}");
    assert!(log.contains("worker wake listener spawned"), "{log}");
    assert!(!worker_spawn_lines(&log).is_empty(), "workers role logged no worker spawns");
    pool.close().await;
    db.cleanup().await;
}

#[tokio::test]
async fn unset_role_is_all_and_runs_both_halves() {
    let Some(base) = base_url() else { return eprintln!("skipping: set ORGT_TEST_BASE_URL") };
    let db = ScratchDb::create(&base, "orgt_backend_role_all").await;
    let pool = db.pool().await;
    let canary = enqueue_canary(&pool).await;

    let mut backend = Backend::spawn(&db.url, None, 18294, 18295);
    backend.wait_ready().await;
    assert_eq!(backend.status(format!("{}/v1/me", backend.public)).await, 401);
    let claimed = wait_claimed(&pool, canary, CLAIM_WINDOW).await;
    let log = backend.stop();
    assert!(claimed.is_some(), "the default role must still run workers:\n{log}");
    assert!(log.contains("\"backend_role\":\"all\""), "{log}");
    assert!(!log.contains("worker wake"), "`all` needs no wake bridge");
    pool.close().await;
    db.cleanup().await;
}

#[tokio::test]
async fn unknown_role_refuses_to_start() {
    let Some(base) = base_url() else { return eprintln!("skipping: set ORGT_TEST_BASE_URL") };
    let db = ScratchDb::create(&base, "orgt_backend_role_bad").await;
    let mut backend = Backend::spawn(&db.url, Some("worker"), 18296, 18297);
    let deadline = Instant::now() + Duration::from_secs(30);
    let status = loop {
        if let Some(s) = backend.child.try_wait().expect("try_wait") {
            break s;
        }
        assert!(Instant::now() < deadline, "backend with BACKEND_ROLE=worker kept running");
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    let log = backend.stop();
    assert!(!status.success(), "must exit non-zero");
    assert!(log.contains("BACKEND_ROLE"), "error names the variable:\n{log}");
    db.cleanup().await;
}
