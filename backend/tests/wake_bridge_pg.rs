//! #1495 wake bridge: a `note_enqueue` in a `BACKEND_ROLE=http` process must
//! reach the `TenantActivity` of a `BACKEND_ROLE=workers` process through
//! Postgres `NOTIFY`/`LISTEN`, and keep doing so after the listener's
//! connection is killed.
//!
//! Two `TenantActivity` instances stand in for the two processes; they share
//! nothing but the database. Gated on `ORGT_TEST_BASE_URL`; with
//! `REQUIRE_PG_SUITE` set a missing URL fails instead of skipping.
//!
//!   ORGT_TEST_BASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test wake_bridge_pg

use std::time::{Duration, Instant};

use coralswarm_backend::shutdown::Shutdown;
use coralswarm_backend::workers::tenant_harness::TenantActivity;
use coralswarm_backend::workers::wake_bridge;
use sqlx::postgres::PgPoolOptions;

fn base_url() -> Option<String> {
    match std::env::var("ORGT_TEST_BASE_URL") {
        Ok(url) => Some(url),
        Err(_) if std::env::var_os("REQUIRE_PG_SUITE").is_some() => {
            panic!("ORGT_TEST_BASE_URL required")
        }
        Err(_) => None,
    }
}

fn with_application_name(url: &str, name: &str) -> String {
    let sep = if url.contains('?') { '&' } else { '?' };
    format!("{url}{sep}application_name={name}")
}

/// Wait until `activity`'s generation for `org` exceeds `above`.
async fn wait_generation(activity: &TenantActivity, org: &str, above: u64, within: Duration) -> bool {
    let deadline = Instant::now() + within;
    while Instant::now() < deadline {
        if activity.enqueue_generation(org) > above {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

#[tokio::test]
async fn notify_from_http_role_wakes_workers_role_and_survives_a_dropped_listener() {
    let Some(base) = base_url() else {
        eprintln!("skipping wake_bridge_pg: set ORGT_TEST_BASE_URL");
        return;
    };
    let app_name = format!("wake_listener_{}", uuid::Uuid::new_v4().simple());
    let org = format!("org-{}", uuid::Uuid::new_v4().simple());
    let shutdown = Shutdown::new();

    // "workers" process: its own activity map and a dedicated LISTEN connection.
    let workers_activity = TenantActivity::new();
    let listener = wake_bridge::spawn_listener(
        with_application_name(&base, &app_name),
        workers_activity.clone(),
        shutdown.clone(),
    );

    // "http" process: a separate pool and activity map with the publisher installed.
    let http_pool = PgPoolOptions::new().max_connections(2).connect(&base).await.expect("http pool");
    let http_activity = TenantActivity::new();
    let publisher = wake_bridge::spawn_publisher(http_pool.clone(), &http_activity, shutdown.clone())
        .expect("first publisher installs");
    assert!(
        wake_bridge::spawn_publisher(http_pool.clone(), &http_activity, shutdown.clone()).is_none(),
        "a second publisher on the same activity is refused"
    );

    let admin = PgPoolOptions::new().max_connections(1).connect(&base).await.expect("admin pool");
    // Wait for the LISTEN connection to exist before sending, or the first
    // NOTIFY can race the subscription.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let n: i64 = sqlx::query_scalar("SELECT count(*) FROM pg_stat_activity WHERE application_name = $1")
            .bind(&app_name)
            .fetch_one(&admin)
            .await
            .unwrap();
        if n > 0 {
            break;
        }
        assert!(Instant::now() < deadline, "listener never connected");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    tokio::time::sleep(Duration::from_millis(200)).await;

    // 1. A plain cross-process wake.
    assert_eq!(workers_activity.enqueue_generation(&org), 0);
    http_activity.note_enqueue(&org);
    assert_eq!(http_activity.enqueue_generation(&org), 1, "local bump still happens");
    assert!(
        wait_generation(&workers_activity, &org, 0, Duration::from_secs(5)).await,
        "NOTIFY from the http pool must bump the workers process's generation"
    );

    // 2. Kill the listener's backend; the bridge must reconnect and re-LISTEN.
    let before = workers_activity.enqueue_generation(&org);
    let killed: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM (SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
         WHERE application_name = $1) t",
    )
    .bind(&app_name)
    .fetch_one(&admin)
    .await
    .unwrap();
    assert!(killed >= 1, "terminated the listener connection");

    // Wakes sent during the gap may be lost (documented); keep sending until
    // one arrives, bounded by the reconnect backoff plus slack.
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut woke = false;
    while Instant::now() < deadline {
        http_activity.note_enqueue(&org);
        if wait_generation(&workers_activity, &org, before, Duration::from_millis(500)).await {
            woke = true;
            break;
        }
    }
    assert!(woke, "the listener must reconnect after its connection is terminated");

    // 3. Shutdown stops both tasks promptly.
    shutdown.trigger();
    tokio::time::timeout(Duration::from_secs(10), listener).await.expect("listener stops").unwrap();
    tokio::time::timeout(Duration::from_secs(10), publisher).await.expect("publisher stops").unwrap();
}
