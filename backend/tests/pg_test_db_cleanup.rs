//! Subprocess tests exercise Drop through a panic and a real process kill.
use coralswarm_backend::test_db::{sweep, TestDatabase};
use sqlx::{Connection, Executor, PgConnection, postgres::PgConnectOptions};
use std::{process::{Command, Stdio}, time::Duration};

#[tokio::test]
#[ignore = "subprocess fixture, invoked only by lifecycle test"]
async fn cleanup_worker() {
    let url = std::env::var("RELINFER_TEST_DATABASE_URL").unwrap();
    let opts: PgConnectOptions = url.parse().unwrap();
    let db = TestDatabase::new(&opts, "cleanup_probe");
    let mut conn = PgConnection::connect_with(&opts).await.unwrap();
    conn.execute(format!("CREATE DATABASE {db}").as_str()).await.unwrap();
    let role = db.role("cleanup_role");
    conn.execute(format!("CREATE ROLE {role}").as_str()).await.unwrap();
    conn.close().await.unwrap();
    std::fs::write(std::env::var("CSTEST_PROBE_FILE").unwrap(), format!("{db}\n{role}")).unwrap();
    match std::env::var("CSTEST_PROBE_MODE").unwrap().as_str() {
        "panic" => panic!("intentional cleanup probe"),
        "kill" => loop { tokio::time::sleep(Duration::from_secs(1)).await; },
        _ => {}
    }
}

#[tokio::test]
async fn cleanup_normal_panic_and_killed_process() {
    let Ok(url) = std::env::var("RELINFER_TEST_DATABASE_URL") else {
        assert!(std::env::var_os("REQUIRE_PG_SUITE").is_none(), "test server URL required");
        return;
    };
    assert_ne!(std::env::var("ORGT_TEST_RETAIN_DATABASES").as_deref(), Ok("1"));
    let opts: PgConnectOptions = url.parse().unwrap();
    let mut conn = PgConnection::connect_with(&opts).await.unwrap();
    let baseline: i64 = sqlx::query_scalar("SELECT count(*) FROM pg_database").fetch_one(&mut conn).await.unwrap();
    for mode in ["normal", "panic", "kill"] {
        let file = std::env::temp_dir().join(format!("cstest_probe_{}", uuid::Uuid::new_v4()));
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["--ignored", "--exact", "cleanup_worker", "--nocapture"])
            .env("CSTEST_PROBE_MODE", mode).env("CSTEST_PROBE_FILE", &file)
            .stdout(Stdio::null()).stderr(Stdio::null()).spawn().unwrap();
        tokio::time::timeout(Duration::from_secs(30), async {
            while !file.exists() { tokio::time::sleep(Duration::from_millis(20)).await; }
        }).await.expect("worker created its fixture");
        let fixture = std::fs::read_to_string(&file).unwrap();
        let mut names = fixture.lines();
        let db = names.next().unwrap();
        let role = names.next().unwrap();
        if mode == "kill" {
            child.kill().unwrap();
            child.wait().unwrap();
            let exists: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname=$1)")
                .bind(db).fetch_one(&mut conn).await.unwrap();
            assert!(exists, "kill must actually leave a database to sweep");
            let report = sweep(&opts, Duration::ZERO).await.unwrap();
            assert!(report.dropped_databases.iter().any(|x| x == db));
            assert!(report.dropped_roles.iter().any(|x| x == role));
        } else {
            let status = child.wait().unwrap();
            assert_eq!(status.success(), mode == "normal");
        }
        let exists: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname=$1) OR EXISTS(SELECT 1 FROM pg_roles WHERE rolname=$2)")
            .bind(db).bind(role).fetch_one(&mut conn).await.unwrap();
        assert!(!exists, "{mode} leaked its database or role");
        std::fs::remove_file(file).unwrap();
    }
    let after: i64 = sqlx::query_scalar("SELECT count(*) FROM pg_database").fetch_one(&mut conn).await.unwrap();
    assert_eq!(after, baseline, "lifecycle probes must return to baseline");
}
