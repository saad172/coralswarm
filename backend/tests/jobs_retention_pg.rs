use std::collections::{BTreeMap, BTreeSet};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use coralswarm_backend::jobs::JobKind;
use coralswarm_backend::store::{PostgresStore, Store};
use coralswarm_backend::tenancy::{read_only_poison, TenantId};
use coralswarm_backend::test_db::{migrated_database, Plane, TestDatabase};
use coralswarm_backend::workers::billing_derivation;
use coralswarm_backend::workers::jobs_retention::{self, Limits, Policy, Status, SweepReport, Target, HANG_GRACE, SWEPT_KINDS};
use coralswarm_backend::workers::tenant_harness::{Tick, WorkerTenant};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{Connection, PgConnection, PgPool};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use uuid::Uuid;

const PLANES: [Plane; 2] = [Plane::Tenant, Plane::Control];
const ROOMY: Limits = Limits { batch_size: 7, budget: Duration::from_secs(120) };

fn base_options() -> Option<PgConnectOptions> {
    let Ok(url) = std::env::var("ORGT_TEST_BASE_URL") else {
        assert!(std::env::var_os("REQUIRE_PG_SUITE").is_none(), "ORGT_TEST_BASE_URL required");
        eprintln!("skipping jobs retention test: set ORGT_TEST_BASE_URL");
        return None;
    };
    Some(PgConnectOptions::from_str(&url).unwrap())
}

async fn fresh_database(base: &PgConnectOptions, plane: Plane) -> (PgPool, TestDatabase) {
    let name = migrated_database(base, plane, "jobs_retention").await;
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect_with(base.clone().database(&name))
        .await
        .unwrap();
    (pool, name)
}

fn database_url(name: &str) -> String {
    let base_url = std::env::var("ORGT_TEST_BASE_URL").unwrap();
    format!("{}/{name}", base_url.rsplit_once('/').unwrap().0)
}

async fn connect(base: &PgConnectOptions, name: &str) -> PgConnection {
    PgConnection::connect_with(&base.clone().database(name)).await.unwrap()
}

async fn seed_newest_first(pool: &PgPool, kind: &str, status: &str, days: i32, outcome: Option<&str>, n: i32) -> Vec<Uuid> {
    sqlx::query_scalar(
        "INSERT INTO jobs(kind, payload, status, finished_at, outcome) \
         SELECT $1, '{}'::jsonb, $2::job_status, now() - make_interval(days => $3) - make_interval(mins => g), $4 \
         FROM generate_series(1, $5) g ORDER BY g RETURNING id",
    )
    .bind(kind)
    .bind(status)
    .bind(days)
    .bind(outcome)
    .bind(n)
    .fetch_all(pool)
    .await
    .unwrap()
}

async fn one_job(pool: &PgPool, kind: &str, status: &str, days: i32, outcome: Option<&str>) -> Uuid {
    seed_newest_first(pool, kind, status, days, outcome, 1).await[0]
}

async fn point_connection_at(pool: &PgPool, job: Uuid) {
    let account = Uuid::new_v4();
    let owner = Uuid::new_v4();
    let control: bool = sqlx::query_scalar("SELECT to_regclass('public.users') IS NOT NULL")
        .fetch_one(pool)
        .await
        .unwrap();
    if control {
        sqlx::query("INSERT INTO users(id, email) VALUES($1, $2)")
            .bind(owner)
            .bind(format!("{owner}@jobs-retention.test"))
            .execute(pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO accounts(id, owner_user_id) VALUES($1, $2)")
            .bind(account)
            .bind(owner)
            .execute(pool)
            .await
            .unwrap();
    }
    let mut graphs = Vec::new();
    for (name, kind) in [("retention source", "team"), ("retention target", "org")] {
        let id: Uuid = sqlx::query_scalar("INSERT INTO graphs(account_id, name, kind) VALUES($1, $2, $3) RETURNING id")
            .bind(account)
            .bind(name)
            .bind(kind)
            .fetch_one(pool)
            .await
            .unwrap();
        graphs.push(id);
    }
    sqlx::query(
        "INSERT INTO source_connections(account_id, owner_id, source_graph_id, target_graph_id, source_type, \
         source_id, label, mode, include_history, current_job_id) \
         VALUES($1, $2, $3, $4, 'note', 'retention', 'retention', 'live', false, $5)",
    )
    .bind(account)
    .bind(owner)
    .bind(graphs[0])
    .bind(graphs[1])
    .bind(job)
    .execute(pool)
    .await
    .unwrap();
}

fn worker_tenant(plane: Plane, pool: PgPool) -> WorkerTenant {
    WorkerTenant {
        tenant: match plane {
            Plane::Tenant => TenantId::Org("org_jobs_retention".to_string()),
            Plane::Control => TenantId::Consumer,
        },
        store: Arc::new(PostgresStore::new(pool.clone())) as Arc<dyn Store>,
        pool,
    }
}

async fn link(pool: &PgPool, parent: Uuid, child: Uuid) {
    sqlx::query("INSERT INTO job_dependencies(parent_id, child_id) VALUES($1, $2)")
        .bind(parent)
        .bind(child)
        .execute(pool)
        .await
        .unwrap();
}

struct Fixture {
    kept_embed_atoms: BTreeSet<Uuid>,
    tied_at_the_100th: Vec<Uuid>,
    referenced: Vec<Uuid>,
    newest_old_did_work: Uuid,
    older_did_work: Uuid,
}

async fn seed(pool: &PgPool) -> Fixture {
    let embed_atoms = seed_newest_first(pool, "EmbedAtoms", "done", 40, Some("did_work"), 130).await;
    let unfinished: Uuid =
        sqlx::query_scalar("INSERT INTO jobs(kind, payload, status, created_at) VALUES('EmbedAtoms', '{}', 'done', now() - interval '400 days') RETURNING id")
            .fetch_one(pool)
            .await
            .unwrap();

    seed_newest_first(pool, "RegenerateDigest", "done", 35, Some("nothing_to_do"), 99).await;
    let tied_at_the_100th: Vec<Uuid> = sqlx::query_scalar(
        "INSERT INTO jobs(kind, payload, status, finished_at, outcome) \
         SELECT 'RegenerateDigest', '{}', 'done', now() - interval '40 days', 'nothing_to_do' FROM generate_series(1, 2) RETURNING id",
    )
    .fetch_all(pool)
    .await
    .unwrap();
    seed_newest_first(pool, "RegenerateDigest", "done", 41, Some("nothing_to_do"), 1).await;

    seed_newest_first(pool, "OceanHealth", "done", 40, Some("did_work"), 10).await;
    for status in ["pending", "running", "failed"] {
        seed_newest_first(pool, "GraphMessageIngest", status, 40, None, 1).await;
    }

    seed_newest_first(pool, "SlackMessageIngest", "done", 1, Some("did_work"), 100).await;
    seed_newest_first(pool, "SlackMessageIngest", "done", 40, Some("did_work"), 50).await;
    seed_newest_first(pool, "SlackMessageIngest", "done", 20, Some("did_work"), 5).await;

    seed_newest_first(pool, "GraphMessageIngest", "dead", 20, None, 10).await;
    seed_newest_first(pool, "GraphMessageIngest", "dead", 10, None, 10).await;
    seed_newest_first(pool, "IngestDoor", "dead", 20, None, 3).await;

    seed_newest_first(pool, "GithubEventIngest", "done", 40, Some("nothing_to_do"), 120).await;
    let newest_old_did_work = one_job(pool, "GithubEventIngest", "done", 60, Some("did_work")).await;
    let older_did_work = one_job(pool, "GithubEventIngest", "done", 61, Some("did_work")).await;

    seed_newest_first(pool, "ExtractEntities", "done", 400, Some("did_work"), 150).await;
    seed_newest_first(pool, "DeleteSourceBlob", "dead", 400, None, 5).await;
    seed_newest_first(pool, "HealthViolation", "done", 400, Some("did_work"), 5).await;

    let child = one_job(pool, "RegenerateDigest", "dead", 30, None).await;
    let publish = one_job(pool, "PublishSource", "done", 1, Some("did_work")).await;
    link(pool, publish, child).await;
    let parent = one_job(pool, "OceanHealth", "dead", 30, None).await;
    let pending: Uuid = sqlx::query_scalar("INSERT INTO jobs(kind, payload) VALUES('PublishSource', '{}') RETURNING id")
        .fetch_one(pool)
        .await
        .unwrap();
    link(pool, parent, pending).await;
    point_connection_at(pool, publish).await;

    Fixture {
        kept_embed_atoms: embed_atoms[..100].iter().copied().chain([unfinished]).collect(),
        tied_at_the_100th,
        referenced: vec![child, parent, publish],
        newest_old_did_work,
        older_did_work,
    }
}

fn expected_eligible() -> BTreeMap<Target, i64> {
    let mut counts: BTreeMap<Target, i64> = jobs_retention::targets().map(|t| (t, 0)).collect();
    counts.insert(("EmbedAtoms", Status::Done), 30);
    counts.insert(("SlackMessageIngest", Status::Done), 50);
    counts.insert(("GraphMessageIngest", Status::Dead), 10);
    counts.insert(("IngestDoor", Status::Dead), 3);
    counts.insert(("GithubEventIngest", Status::Done), 21);
    counts.insert(("RegenerateDigest", Status::Done), 1);
    counts
}

fn eligible_total() -> i64 {
    expected_eligible().values().sum()
}

fn deleted(report: &SweepReport) -> BTreeMap<Target, i64> {
    let mut counts: BTreeMap<Target, i64> = jobs_retention::targets().map(|t| (t, 0)).collect();
    for (target, n) in &report.deleted {
        counts.insert(*target, *n as i64);
    }
    counts
}

async fn count(pool: &PgPool, sql: &str) -> i64 {
    sqlx::query_scalar(sql).fetch_one(pool).await.unwrap()
}

async fn total_jobs(pool: &PgPool) -> i64 {
    count(pool, "SELECT count(*) FROM jobs").await
}

async fn ids(pool: &PgPool, sql: &str) -> BTreeSet<Uuid> {
    sqlx::query_scalar(sql).fetch_all(pool).await.unwrap().into_iter().collect()
}

async fn exists(pool: &PgPool, id: Uuid) -> bool {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM jobs WHERE id = $1)")
        .bind(id)
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn assert_only_eligible_rows_went(pool: &PgPool, plane: Plane, fx: &Fixture) {
    assert_eq!(
        ids(pool, "SELECT id FROM jobs WHERE kind = 'EmbedAtoms' AND status = 'done'").await,
        fx.kept_embed_atoms,
        "{plane:?}: the newest 100 done rows of a kind stay, however old, and a row with no finished_at stays"
    );
    assert_eq!(count(pool, "SELECT count(*) FROM jobs WHERE kind = 'RegenerateDigest' AND status = 'done'").await, 101, "{plane:?}: a tie at the 100th newest keeps both rows");
    for id in &fx.tied_at_the_100th {
        assert!(exists(pool, *id).await, "{plane:?}: tied row {id} stays");
    }
    assert_eq!(count(pool, "SELECT count(*) FROM jobs WHERE kind = 'OceanHealth' AND status = 'done'").await, 10, "{plane:?}: a kind with fewer than 100 done rows keeps them all");
    assert_eq!(
        count(pool, "SELECT count(*) FROM jobs WHERE kind = 'GraphMessageIngest' AND status IN ('pending', 'running', 'failed')").await,
        3,
        "{plane:?}: only done and dead rows go"
    );
    assert_eq!(count(pool, "SELECT count(*) FROM jobs WHERE kind = 'SlackMessageIngest'").await, 105, "{plane:?}: done rows inside the window stay");
    assert_eq!(
        count(pool, "SELECT count(*) FROM jobs WHERE kind = 'GraphMessageIngest' AND finished_at > now() - interval '14 days'").await,
        10,
        "{plane:?}: dead rows inside the window stay"
    );
    assert_eq!(count(pool, "SELECT count(*) FROM jobs WHERE kind = 'GraphMessageIngest' AND status = 'dead'").await, 10, "{plane:?}: old dead rows go");
    assert_eq!(count(pool, "SELECT count(*) FROM jobs WHERE kind = 'IngestDoor'").await, 0, "{plane:?}: retired-kind dead rows go");
    assert!(exists(pool, fx.newest_old_did_work).await, "{plane:?}: the newest did_work row of a kind stays");
    assert!(!exists(pool, fx.older_did_work).await, "{plane:?}: an older did_work row goes");
    assert_eq!(
        count(pool, "SELECT count(*) FROM jobs WHERE kind = 'GithubEventIngest' AND status = 'done'").await,
        101,
        "{plane:?}: the newest 100 plus the newest did_work row stay"
    );
    for (kind, n) in [("ExtractEntities", 150), ("DeleteSourceBlob", 5), ("HealthViolation", 5)] {
        assert_eq!(
            count(pool, &format!("SELECT count(*) FROM jobs WHERE kind = '{kind}'")).await,
            n,
            "{plane:?}: {kind} is outside the allowlist and stays"
        );
    }
    for id in &fx.referenced {
        assert!(exists(pool, *id).await, "{plane:?}: referenced row {id} stays");
    }
    assert_eq!(count(pool, "SELECT count(*) FROM job_dependencies").await, 2, "{plane:?}: dependency links stay");
    assert_eq!(
        count(pool, "SELECT count(*) FROM source_connections s JOIN jobs j ON j.id = s.current_job_id WHERE j.kind = 'PublishSource'").await,
        1,
        "{plane:?}: the connection keeps its job"
    );
}

#[tokio::test]
async fn sweep_deletes_exactly_what_the_dry_run_counts_on_both_planes() {
    let Some(base) = base_options() else { return };
    for plane in PLANES {
        let (pool, name) = fresh_database(&base, plane).await;
        let fx = seed(&pool).await;
        let before = total_jobs(&pool).await;
        let mut conn = connect(&base, &name).await;

        let counts = jobs_retention::count_eligible(&mut conn, Policy::DEFAULT).await.unwrap();
        assert_eq!(counts, expected_eligible(), "{plane:?}: dry-run counts");
        assert_eq!(total_jobs(&pool).await, before, "{plane:?}: a dry run deletes nothing");

        let report = jobs_retention::sweep_once(&mut conn, Policy::DEFAULT, ROOMY).await;
        assert!(report.failed.is_empty(), "{plane:?}: {:?}", report.failed);
        assert!(!report.budget_exhausted, "{plane:?}");
        assert_eq!(deleted(&report), expected_eligible(), "{plane:?}: deleted per kind and status");
        assert!(report.batches >= 5 + 8 + 2 + 1 + 4 + 1, "{plane:?}: batches of 7 drain each target: {}", report.batches);
        assert_eq!(total_jobs(&pool).await, before - eligible_total(), "{plane:?}");
        assert_only_eligible_rows_went(&pool, plane, &fx).await;

        let counts = jobs_retention::count_eligible(&mut conn, Policy::DEFAULT).await.unwrap();
        assert!(counts.values().all(|n| *n == 0), "{plane:?}: nothing left: {counts:?}");
        let again = jobs_retention::sweep_once(&mut conn, Policy::DEFAULT, ROOMY).await;
        assert_eq!(again.total_deleted(), 0, "{plane:?}: a second sweep deletes nothing");
        assert!(again.failed.is_empty(), "{plane:?}: {:?}", again.failed);
    }
}

#[tokio::test]
async fn longer_windows_keep_more() {
    let Some(base) = base_options() else { return };
    let (pool, name) = fresh_database(&base, Plane::Tenant).await;
    seed(&pool).await;
    let mut conn = connect(&base, &name).await;
    let policy = Policy { done_days: 45, dead_days: 25 };
    let counts = jobs_retention::count_eligible(&mut conn, policy).await.unwrap();
    let mut expected: BTreeMap<Target, i64> = jobs_retention::targets().map(|t| (t, 0)).collect();
    expected.insert(("GithubEventIngest", Status::Done), 1);
    assert_eq!(counts, expected, "only the 61-day did_work row is past 45 days; no dead row is past 25");
}

#[tokio::test]
async fn an_exhausted_budget_deletes_nothing_more() {
    let Some(base) = base_options() else { return };
    for plane in PLANES {
        let (pool, name) = fresh_database(&base, plane).await;
        seed(&pool).await;
        let before = total_jobs(&pool).await;
        let mut conn = connect(&base, &name).await;
        let report = jobs_retention::sweep_once(&mut conn, Policy::DEFAULT, Limits { batch_size: 7, budget: Duration::ZERO }).await;
        assert!(report.budget_exhausted, "{plane:?}");
        assert_eq!((report.batches, report.total_deleted()), (0, 0), "{plane:?}: no batch starts past the budget");
        assert_eq!(total_jobs(&pool).await, before, "{plane:?}");
    }
}

#[tokio::test]
async fn a_blocked_batch_is_cancelled_at_the_budget() {
    let Some(base) = base_options() else { return };
    for plane in PLANES {
        let (pool, name) = fresh_database(&base, plane).await;
        seed(&pool).await;
        let before = total_jobs(&pool).await;
        let mut locker = connect(&base, &name).await;
        sqlx::query("BEGIN").execute(&mut locker).await.unwrap();
        sqlx::query("LOCK TABLE job_dependencies IN ACCESS EXCLUSIVE MODE").execute(&mut locker).await.unwrap();

        let mut conn = connect(&base, &name).await;
        let started = Instant::now();
        let report = jobs_retention::sweep_once(&mut conn, Policy::DEFAULT, Limits { batch_size: 7, budget: Duration::from_secs(2) }).await;
        let elapsed = started.elapsed();
        sqlx::query("ROLLBACK").execute(&mut locker).await.unwrap();

        assert!(report.budget_exhausted, "{plane:?}: {report:?}");
        assert!(report.failed.is_empty(), "{plane:?}: a cancelled batch is the budget, not a failure: {:?}", report.failed);
        assert_eq!(report.total_deleted(), 0, "{plane:?}");
        assert!(elapsed < Duration::from_secs(3), "{plane:?}: stopped after {elapsed:?}");
        assert_eq!(total_jobs(&pool).await, before, "{plane:?}");
    }
}

#[tokio::test]
async fn tick_deletes_and_still_reports_idle() {
    let Some(base) = base_options() else { return };
    for plane in PLANES {
        let (pool, _name) = fresh_database(&base, plane).await;
        let old = seed_newest_first(&pool, "GraphMessageIngest", "dead", 20, None, 5).await;
        let tick = jobs_retention::tick(&worker_tenant(plane, pool.clone()), Policy::DEFAULT).await.unwrap();
        assert_eq!(tick, Tick::idle(), "{plane:?}: a tick that deleted still reports idle so it wakes no peer");
        for id in old {
            assert!(!exists(&pool, id).await, "{plane:?}");
        }
    }
}

#[tokio::test]
async fn a_connection_naming_a_swept_row_stalls_only_that_target() {
    let Some(base) = base_options() else { return };
    for plane in PLANES {
        let (pool, name) = fresh_database(&base, plane).await;
        let fx = seed(&pool).await;
        let stuck = seed_newest_first(&pool, "GithubEventIngest", "dead", 30, None, 3).await;
        point_connection_at(&pool, stuck[1]).await;
        let before = total_jobs(&pool).await;
        let mut conn = connect(&base, &name).await;
        if plane == Plane::Control {
            sqlx::query("SET ROLE coralswarm_app").execute(&mut conn).await.unwrap();
            let hidden: bool = sqlx::query_scalar("SELECT row_security_active('source_connections')").fetch_one(&mut conn).await.unwrap();
            assert!(hidden, "precondition: RLS applies to the app role");
        }

        let counts = jobs_retention::count_eligible(&mut conn, Policy::DEFAULT).await.unwrap();
        assert_eq!(counts[&("GithubEventIngest", Status::Dead)], 3, "{plane:?}: the count and the delete share one predicate");
        for pass in 0..2 {
            let report = jobs_retention::sweep_once(&mut conn, Policy::DEFAULT, ROOMY).await;
            assert_eq!(report.failed.len(), 1, "{plane:?} pass {pass}: {:?}", report.failed);
            let ((kind, status), error) = &report.failed[0];
            assert_eq!((*kind, *status), ("GithubEventIngest", Status::Dead));
            assert!(error.contains("source_connections"), "{error}");
        }
        for id in &stuck {
            assert!(exists(&pool, *id).await, "{plane:?}: every row of the stalled target stays");
        }
        assert_eq!(total_jobs(&pool).await, before - eligible_total(), "{plane:?}: every other target drains");
        assert_only_eligible_rows_went(&pool, plane, &fx).await;

        let tenant = worker_tenant(plane, pool.clone());
        let error = jobs_retention::tick(&tenant, Policy::DEFAULT).await.unwrap_err();
        assert!(error.contains("GithubEventIngest dead") && error.contains("source_connections"), "{plane:?}: {error}");
    }
}

#[tokio::test]
async fn a_read_only_backend_fails_the_tick_for_the_harness_to_heal() {
    let Some(base) = base_options() else { return };
    let (pool, name) = fresh_database(&base, Plane::Tenant).await;
    seed_newest_first(&pool, "GraphMessageIngest", "dead", 20, None, 5).await;
    let before = total_jobs(&pool).await;
    let read_only = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(base.clone().database(&name).options([("default_transaction_read_only", "on")]))
        .await
        .unwrap();
    let error = jobs_retention::tick(&worker_tenant(Plane::Tenant, read_only), Policy::DEFAULT).await.unwrap_err();
    assert!(read_only_poison::is_read_only_rejection(&error), "{error}");
    assert_eq!(total_jobs(&pool).await, before);
}

#[tokio::test]
async fn counting_leaves_the_session_writable() {
    let Some(base) = base_options() else { return };
    let (_pool, name) = fresh_database(&base, Plane::Tenant).await;
    let mut conn = connect(&base, &name).await;
    jobs_retention::count_eligible(&mut conn, Policy::DEFAULT).await.unwrap();
    let read_only: String = sqlx::query_scalar("SHOW transaction_read_only").fetch_one(&mut conn).await.unwrap();
    assert_eq!(read_only, "off", "a pooled server connection must not keep a dry run's read-only setting");
    sqlx::query("INSERT INTO jobs(kind, payload) VALUES('OceanHealth', '{}')").execute(&mut conn).await.unwrap();
}

#[test]
fn the_shortest_window_covers_the_weekly_health_read() {
    assert!(i64::from(jobs_retention::MIN_WINDOW_DAYS) >= coralswarm_backend::ocean_health::WINDOW.num_days());
}

#[test]
fn a_failed_tick_waits_the_full_interval() {
    let cfg = jobs_retention::harness_config();
    assert_eq!((cfg.base_poll, cfg.error_sleep), (jobs_retention::SWEEP_INTERVAL, jobs_retention::SWEEP_INTERVAL));
    assert!(!cfg.wake_on_enqueue && !cfg.hot_loop_on_work);
}

#[test]
fn swept_kinds_exclude_every_kind_whose_old_rows_are_read() {
    let publish = serde_json::to_value(JobKind::PublishSource { connection_id: Uuid::nil(), atom_ids: vec![] }).unwrap();
    let publish = publish["kind"].as_str().unwrap();
    let read = billing_derivation::JOB_AXIS_KINDS
        .iter()
        .copied()
        .chain([publish, "PromoteNode", "ExtractMeetingAtoms", "ExtractSourceAtoms", "DeleteSourceBlob", "HealthViolation"]);
    for kind in read {
        assert!(!SWEPT_KINDS.contains(&kind), "{kind} has readers of its old rows");
    }
}

#[test]
fn swept_kinds_name_job_kinds() {
    for kind in SWEPT_KINDS {
        let parsed = serde_json::from_value::<JobKind>(serde_json::json!({"kind": kind, "payload": {}}));
        let unknown = parsed.as_ref().is_err_and(|e| e.to_string().contains("unknown variant"));
        assert_eq!(unknown, kind == "IngestDoor", "{kind} (IngestDoor is retired): {parsed:?}");
    }
}

fn run_binary(args: &[&str], env: &[(&str, &str)]) -> (i32, String) {
    let mut cmd = std::process::Command::new(env!("CARGO_BIN_EXE_jobs_retention"));
    cmd.args(args)
        .env_remove("DATABASE_URL")
        .env_remove(jobs_retention::DONE_DAYS_ENV)
        .env_remove(jobs_retention::DEAD_DAYS_ENV);
    for (k, v) in env {
        cmd.env(k, v);
    }
    let out = cmd.output().unwrap();
    (out.status.code().unwrap(), String::from_utf8_lossy(&out.stdout).into_owned() + &String::from_utf8_lossy(&out.stderr))
}

#[tokio::test]
async fn binary_dry_run_counts_and_execute_drains() {
    let Some(base) = base_options() else { return };
    for plane in PLANES {
        let (pool, name) = fresh_database(&base, plane).await;
        let fx = seed(&pool).await;
        let before = total_jobs(&pool).await;
        let url = database_url(&name);

        let (code, out) = run_binary(&["--dry-run", "--database-url", &url], &[]);
        assert_eq!(code, 0, "{out}");
        assert!(!out.contains(&url) && !out.contains("postgres:postgres"), "{plane:?}: the URL is never printed: {out}");
        for line in ["EmbedAtoms\tdone\t30", "SlackMessageIngest\tdone\t50", "GraphMessageIngest\tdead\t10", "IngestDoor\tdead\t3", "GithubEventIngest\tdone\t21", "RegenerateDigest\tdone\t1", "OceanHealth\tdead\t0", &format!("total eligible {}", eligible_total())] {
            assert!(out.lines().any(|l| l == line), "{plane:?}: missing {line:?} in {out}");
        }
        assert!(out.contains(match plane {
            Plane::Tenant => "plane=tenant",
            Plane::Control => "plane=control",
        }), "{out}");
        assert_eq!(total_jobs(&pool).await, before, "{plane:?}: --dry-run deletes nothing");

        let (code, out) = run_binary(&["--execute", "--database-url", &url], &[]);
        assert_eq!(code, 0, "{out}");
        assert!(out.contains(&format!("total deleted {} in ", eligible_total())), "{out}");
        assert!(out.lines().any(|l| l == "total left 0"), "{out}");
        assert_eq!(total_jobs(&pool).await, before - eligible_total(), "{plane:?}");
        assert_only_eligible_rows_went(&pool, plane, &fx).await;
    }
}

#[test]
fn binary_refuses_an_ambiguous_mode_or_a_short_window() {
    let url = "postgres://nobody@127.0.0.1:1/none";
    for args in [vec!["--database-url", url], vec!["--dry-run", "--execute", "--database-url", url], vec!["--dry-run"]] {
        let (code, out) = run_binary(&args, &[]);
        assert_eq!(code, 2, "{args:?}: {out}");
    }
    let (code, out) = run_binary(&["--dry-run", "--database-url", url], &[(jobs_retention::DONE_DAYS_ENV, "3")]);
    assert_eq!(code, 2, "{out}");
    assert!(out.contains("below the 7-day minimum"), "{out}");
    let (code, out) = run_binary(&["--dry-run", "--database-url", url], &[(jobs_retention::DEAD_DAYS_ENV, "two weeks")]);
    assert_eq!(code, 2, "{out}");
}

#[tokio::test]
async fn an_edge_committed_during_a_sweep_keeps_its_parent() {
    let Some(base) = base_options() else { return };
    let (pool, name) = fresh_database(&base, Plane::Tenant).await;
    let old = Arc::new(seed_newest_first(&pool, "GraphMessageIngest", "dead", 20, None, 20_000).await);
    let stop = Arc::new(AtomicBool::new(false));
    let inserters: Vec<_> = (0..4)
        .map(|w| {
            let (pool, old, stop) = (pool.clone(), old.clone(), stop.clone());
            tokio::spawn(async move {
                let mut linked = Vec::new();
                let mut i = w * 4_999;
                while !stop.load(Ordering::Relaxed) {
                    i = (i + 7_919) % old.len();
                    let parent = old[i];
                    let edge = async {
                        let mut tx = pool.begin().await?;
                        let child: Uuid = sqlx::query_scalar("INSERT INTO jobs(kind, payload) VALUES('PublishSource', '{}') RETURNING id")
                            .fetch_one(&mut *tx)
                            .await?;
                        sqlx::query("INSERT INTO job_dependencies(parent_id, child_id) VALUES($1, $2)")
                            .bind(parent)
                            .bind(child)
                            .execute(&mut *tx)
                            .await?;
                        tx.commit().await?;
                        Ok::<_, sqlx::Error>(child)
                    };
                    if let Ok(child) = edge.await {
                        linked.push((parent, child));
                    }
                }
                linked
            })
        })
        .collect();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut conn = connect(&base, &name).await;
    sqlx::query("SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL REPEATABLE READ").execute(&mut conn).await.unwrap();
    let report = jobs_retention::sweep_once(&mut conn, Policy::DEFAULT, Limits { batch_size: 20_000, budget: Duration::from_secs(120) }).await;
    stop.store(true, Ordering::Relaxed);
    let mut linked = Vec::new();
    for inserter in inserters {
        linked.extend(inserter.await.unwrap());
    }

    assert!(report.total_deleted() > 0, "{report:?}");
    assert!(!linked.is_empty());
    let mut lost = 0;
    for (parent, child) in &linked {
        let edge: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM job_dependencies WHERE parent_id = $1 AND child_id = $2)")
            .bind(parent)
            .bind(child)
            .fetch_one(&pool)
            .await
            .unwrap();
        if !edge || !exists(&pool, *parent).await {
            lost += 1;
        }
    }
    assert_eq!(lost, 0, "{lost} of {} committed edges lost their parent", linked.len());
}

async fn stalling_proxy(base: &PgConnectOptions) -> (u16, Arc<AtomicBool>) {
    let upstream = format!("{}:{}", base.get_host(), base.get_port());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let stall = Arc::new(AtomicBool::new(false));
    let flag = stall.clone();
    tokio::spawn(async move {
        loop {
            let (client, _) = listener.accept().await.unwrap();
            let server = TcpStream::connect(&upstream).await.unwrap();
            let (client_read, client_write) = client.into_split();
            let (server_read, server_write) = server.into_split();
            tokio::spawn(relay(client_read, server_write, flag.clone()));
            tokio::spawn(relay(server_read, client_write, flag.clone()));
        }
    });
    (port, stall)
}

async fn relay(mut from: OwnedReadHalf, mut to: OwnedWriteHalf, stall: Arc<AtomicBool>) {
    let mut buf = [0u8; 8192];
    loop {
        let n = match from.read(&mut buf).await {
            Ok(0) | Err(_) => return,
            Ok(n) => n,
        };
        if stall.load(Ordering::Relaxed) {
            std::future::pending::<()>().await;
        }
        if to.write_all(&buf[..n]).await.is_err() {
            return;
        }
    }
}

fn through(base: &PgConnectOptions, port: u16, name: &str) -> PgConnectOptions {
    base.clone().host("127.0.0.1").port(port).database(name)
}

#[tokio::test]
async fn a_hung_batch_returns_at_the_budget_plus_grace() {
    let Some(base) = base_options() else { return };
    let (pool, name) = fresh_database(&base, Plane::Tenant).await;
    seed_newest_first(&pool, "GraphMessageIngest", "dead", 20, None, 5).await;
    let before = total_jobs(&pool).await;
    let (port, stall) = stalling_proxy(&base).await;
    let mut conn = PgConnection::connect_with(&through(&base, port, &name)).await.unwrap();
    stall.store(true, Ordering::Relaxed);

    let limits = Limits { batch_size: 7, budget: Duration::from_secs(1) };
    let started = Instant::now();
    let report = tokio::time::timeout(
        limits.budget + HANG_GRACE + Duration::from_secs(5),
        jobs_retention::sweep_once(&mut conn, Policy::DEFAULT, limits),
    )
    .await
    .expect("a connection that stops answering must not hang the sweep");
    let elapsed = started.elapsed();

    assert!(report.hung && report.budget_exhausted, "{report:?}");
    assert!(report.failed.is_empty(), "{:?}", report.failed);
    assert_eq!((report.batches, report.total_deleted()), (1, 0));
    assert!(elapsed >= limits.budget + HANG_GRACE, "gave up early after {elapsed:?}");
    assert!(elapsed < limits.budget + HANG_GRACE + Duration::from_secs(1), "gave up late after {elapsed:?}");
    assert_eq!(total_jobs(&pool).await, before);
}

#[tokio::test]
async fn a_hung_tick_drops_its_connection_so_the_pool_replaces_it() {
    let Some(base) = base_options() else { return };
    let (_pool, name) = fresh_database(&base, Plane::Tenant).await;
    let (port, stall) = stalling_proxy(&base).await;
    let proxied = PgPoolOptions::new()
        .max_connections(1)
        .test_before_acquire(false)
        .acquire_timeout(Duration::from_secs(5))
        .connect_with(through(&base, port, &name))
        .await
        .unwrap();
    stall.store(true, Ordering::Relaxed);

    let limits = Limits { batch_size: 7, budget: Duration::from_secs(1) };
    let tenant = worker_tenant(Plane::Tenant, proxied.clone());
    let tick = tokio::time::timeout(
        limits.budget + HANG_GRACE + Duration::from_secs(5),
        jobs_retention::tick_with(&tenant, Policy::DEFAULT, limits),
    )
    .await
    .expect("a connection that stops answering must not hang the tick")
    .unwrap();
    assert_eq!(tick, Tick::idle(), "a hung batch is the budget, not a failure");

    stall.store(false, Ordering::Relaxed);
    let mut fresh = proxied.acquire().await.expect("the pool opens a replacement for the dropped connection");
    let one: i32 = tokio::time::timeout(Duration::from_secs(5), sqlx::query_scalar("SELECT 1").fetch_one(&mut *fresh))
        .await
        .expect("the dead connection must not come back out of the pool")
        .unwrap();
    assert_eq!(one, 1);
}
