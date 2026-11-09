//! Opt-in cleanup for marked leftovers of killed test processes.
use coralswarm_backend::test_db::sweep;
use std::time::Duration;

#[tokio::test]
#[ignore = "explicitly deletes old marked test databases; set RELINFER_TEST_DATABASE_URL"]
async fn sweep_marked_test_databases() {
    let url = std::env::var("RELINFER_TEST_DATABASE_URL").expect("explicit test server URL required");
    let minutes: u64 = std::env::var("CSTEST_SWEEP_MIN_AGE_MINUTES")
        .unwrap_or_else(|_| "60".into()).parse().expect("minimum age in minutes");
    let report = sweep(&url.parse().expect("Postgres URL"), Duration::from_secs(minutes * 60))
        .await.expect("sweep test leftovers");
    eprintln!("{report:#?}");
}
