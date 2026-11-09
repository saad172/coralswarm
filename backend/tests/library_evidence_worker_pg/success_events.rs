//! Actual tick boundary proofs; JSON is captured from the real tracing formatter.
use super::*;
use coralswarm_backend::{
    store::PostgresStore, tenancy::TenantId, workers::tenant_harness::WorkerTenant,
};
use std::{
    io::Write,
    sync::{Arc, Mutex},
};
use tracing::instrument::WithSubscriber;
use tracing::Instrument;
use tracing_subscriber::prelude::*;

#[derive(Clone, Default)]
struct Logs(Arc<Mutex<Vec<u8>>>);
impl Write for Logs {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Logs {
    type Writer = Self;
    fn make_writer(&'a self) -> Self {
        self.clone()
    }
}
impl Logs {
    fn subscriber(&self) -> impl tracing::Subscriber + Send + Sync + 'static {
        tracing_subscriber::fmt()
            .json()
            .with_max_level(tracing::Level::INFO)
            .with_writer(self.clone())
            .finish()
    }
    fn subscriber_without_root(&self) -> impl tracing::Subscriber + Send + Sync + 'static {
        tracing_subscriber::registry().with(
            tracing_subscriber::fmt::layer()
                .json()
                .with_writer(self.clone())
                .with_filter(tracing_subscriber::filter::filter_fn(|metadata| {
                    !(metadata.is_span() && metadata.name() == "library_evidence_commit")
                })),
        )
    }
    fn events(&self) -> Vec<Value> {
        String::from_utf8(self.0.lock().unwrap().clone())
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .filter(|event| event["fields"]["event"] == "library_evidence_batch_committed")
            .collect()
    }
}

fn tenant(f: &Fixture) -> WorkerTenant {
    WorkerTenant {
        tenant: if f.tenant {
            TenantId::Org(format!("org_success_{}", f.db))
        } else {
            TenantId::Consumer
        },
        pool: f.app.clone(),
        store: Arc::new(PostgresStore::new(f.app.clone())),
    }
}

/// The committed units one tick reports. `processed` is their plain sum.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
struct Units {
    nodes: u64,
    changes: u64,
    purged: u64,
    backfill: u64,
    listing_outcomes: u64,
    listing_changes: u64,
    listing_published: u64,
    listing_backfill: u64,
}
impl Units {
    fn processed(self) -> u64 {
        self.nodes
            + self.changes
            + self.purged
            + self.backfill
            + self.listing_outcomes
            + self.listing_changes
            + self.listing_published
            + self.listing_backfill
    }
    fn from_event(fields: &Value) -> Self {
        let n = |key: &str| fields[key].as_u64().unwrap();
        Self {
            nodes: n("nodes"),
            changes: n("changes_consumed"),
            purged: n("changes_purged"),
            backfill: n("backfill_completed"),
            listing_outcomes: n("listing_outcomes"),
            listing_changes: n("listing_changes_consumed"),
            listing_published: n("listing_items_published"),
            listing_backfill: n("listing_backfill_completed"),
        }
    }
}

fn positive(logs: &Logs, tenant: bool, units: Units) {
    let events = logs.events();
    assert_eq!(events.len(), 1);
    let fields = events[0]["fields"].as_object().unwrap();
    assert_eq!(
        fields.keys().map(String::as_str).collect::<Vec<_>>(),
        vec![
            "backfill_completed",
            "changes_consumed",
            "changes_purged",
            "elapsed_ms",
            "event",
            "listing_backfill_completed",
            "listing_changes_consumed",
            "listing_items_published",
            "listing_outcomes",
            "message",
            "nodes",
            "plane",
            "processed",
            "tenant",
            "worker",
        ],
        "explicit fields must not acquire account/graph/node IDs, payloads or errors"
    );
    assert_eq!(fields["worker"], "library_evidence");
    assert_eq!(fields["plane"], if tenant { "tenant" } else { "consumer" });
    assert_eq!(Units::from_event(&events[0]["fields"]), units);
    assert_eq!(fields["processed"].as_u64(), Some(units.processed()));
    assert!(fields["processed"].as_u64().unwrap() > 0);
    assert!(fields["elapsed_ms"].as_u64().is_some());
    assert_eq!(events[0]["level"], "INFO");
    let root = json!({"name":"library_evidence_commit"});
    assert_eq!(
        events[0]["span"], root,
        "only the field-free root may be serialized"
    );
    assert_eq!(
        events[0]["spans"],
        json!([root]),
        "ambient private spans must be excluded"
    );
    assert!(!events[0].to_string().contains("must-not-appear"));
    eprintln!("LIBRARY_WORKER_COMMIT_EVENT {}", events[0]);
}

async fn wait_for_gate(f: &Fixture, gate: i32) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let waiting: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND objid=$1::bigint::oid AND NOT granted AND database=(SELECT oid FROM pg_database WHERE datname=current_database()))")
                .bind(i64::from(gate)).fetch_one(&f.owner).await.unwrap();
            if waiting { break; }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.expect("actual worker reached the fixture lock");
}

async fn snapshot(f: &Fixture) -> Value {
    sqlx::query_scalar("SELECT jsonb_build_object('state',(SELECT jsonb_agg(to_jsonb(s) ORDER BY graph_id) FROM library_evidence_state s),'evidence',(SELECT jsonb_agg(to_jsonb(n) ORDER BY node_id) FROM library_node_evidence n),'changes',(SELECT jsonb_agg(to_jsonb(c) ORDER BY id) FROM library_evidence_changes c))")
        .fetch_one(&f.owner).await.unwrap()
}

#[tokio::test]
async fn positive_event_requires_commit_and_failed_cancelled_idle_contended_ticks_are_silent() {
    for is_tenant in [false, true] {
        let f = Fixture::new(is_tenant).await;
        let node = f.node("Successful commit witness").await;
        let atom = f.atom(node, Some("Successful commit witness"), &[]).await;
        let t = tenant(&f);
        let gate = 0x4353_4f50_i32;
        f.owner.execute(format!("CREATE FUNCTION fixture_commit_gate() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN PERFORM pg_advisory_xact_lock({gate}::bigint); RETURN NEW; END$$; CREATE CONSTRAINT TRIGGER fixture_commit_gate AFTER INSERT OR UPDATE ON library_evidence_state DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION fixture_commit_gate()").as_str()).await.unwrap();
        let mut blocker = f.owner.begin().await.unwrap();
        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(i64::from(gate))
            .execute(&mut *blocker)
            .await
            .unwrap();
        let logs = Logs::default();
        let owned = t.clone();
        let task = tokio::spawn(
            async move {
                let span = tracing::info_span!(
                    "private_canary",
                    account_id = "must-not-appear",
                    payload = "must-not-appear"
                );
                worker::tick(&owned).instrument(span).await
            }
            .with_subscriber(logs.subscriber()),
        );
        wait_for_gate(&f, gate).await;
        assert!(
            logs.events().is_empty(),
            "positive work before COMMIT is not success"
        );
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM library_node_evidence")
            .fetch_one(&f.owner)
            .await
            .unwrap();
        assert_eq!(count, 0, "deferred commit has not published either grain");
        blocker.rollback().await.unwrap();
        let tick = task.await.unwrap().unwrap();
        // One node backfilled and certified, the link's change row (logged
        // before the backfill's start snapshot, so covered by it) purged,
        // and the listing refresh then loops its steps inside the same commit
        // while budgets remain: the node count from the certified grain (one
        // statement, no unit of its own) and the certification once the
        // inventory walks are exhausted (listing_backfill 1).
        let gate_units = Units {
            nodes: 1,
            backfill: 1,
            purged: 1,
            listing_backfill: 1,
            ..Units::default()
        };
        positive(&logs, is_tenant, gate_units);
        assert_eq!(tick.processed, 4);
        let certified: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM library_listing_state WHERE graph_id=$1 AND ready_version=3)",
        )
        .bind(f.graph)
        .fetch_one(&f.owner)
        .await
        .unwrap();
        assert!(certified, "listing_backfill_completed reports the listing certification");
        f.owner.execute("DROP TRIGGER fixture_commit_gate ON library_evidence_state; DROP FUNCTION fixture_commit_gate()").await.unwrap();

        // A write after the backfill logs an atom key the purge did not cover;
        // consume it. The event's mixed total is the actual scheduler total,
        // never claimed to be node throughput. The certified listing grain
        // has no change to consume, so this tick is node work alone.
        sqlx::query("UPDATE atoms SET participants=ARRAY['Late witness'] WHERE atom_id=$1")
            .bind(atom)
            .execute(&f.owner)
            .await
            .unwrap();
        let logs = Logs::default();
        let tick = worker::tick(&t)
            .with_subscriber(logs.subscriber())
            .await
            .unwrap();
        positive(
            &logs,
            is_tenant,
            Units {
                nodes: 1,
                changes: 1,
                ..Units::default()
            },
        );
        assert_eq!(tick.processed, 2);
        let logs = Logs::default();
        assert_eq!(
            worker::tick(&t)
                .with_subscriber(logs.subscriber())
                .await
                .unwrap()
                .processed,
            0
        );
        assert!(logs.events().is_empty());

        // A subscriber can enable events while disabling the privacy root.
        // Dropping only the event must preserve committed work and Tick output.
        sqlx::query("UPDATE nodes SET title='Filtered span work' WHERE node_id=$1")
            .bind(node)
            .execute(&f.owner)
            .await
            .unwrap();
        let logs = Logs::default();
        let tick = async {
            let span = tracing::info_span!(
                "private_canary",
                account_id = "must-not-appear",
                payload = "must-not-appear"
            );
            worker::tick(&t).instrument(span).await
        }
        .with_subscriber(logs.subscriber_without_root())
        .await
        .unwrap();
        assert!(
            tick.processed > 0,
            "disabled log root cannot suppress committed work"
        );
        assert!(
            logs.events().is_empty(),
            "disabled root cannot fall back to private ambient spans"
        );
        let remaining: i64 = sqlx::query_scalar("SELECT count(*) FROM library_evidence_changes")
            .fetch_one(&f.owner)
            .await
            .unwrap();
        assert_eq!(remaining, 0, "tick still commits its acknowledgments");

        let mut lock = f.tx_on(&f.app).await;
        assert!(worker::lock_graph(&mut lock, f.graph).await.unwrap());
        let logs = Logs::default();
        assert_eq!(
            worker::tick(&t)
                .with_subscriber(logs.subscriber())
                .await
                .unwrap()
                .processed,
            0
        );
        assert!(logs.events().is_empty());
        lock.rollback().await.unwrap();

        sqlx::query("UPDATE nodes SET title='Changed after readiness' WHERE node_id=$1")
            .bind(node)
            .execute(&f.owner)
            .await
            .unwrap();
        let before = snapshot(&f).await;
        f.owner.execute("CREATE FUNCTION fixture_commit_fail() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RAISE EXCEPTION 'fixture commit failed'; END$$; CREATE CONSTRAINT TRIGGER fixture_commit_fail AFTER UPDATE ON library_evidence_state DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION fixture_commit_fail()").await.unwrap();
        let logs = Logs::default();
        assert!(worker::tick(&t)
            .with_subscriber(logs.subscriber())
            .await
            .is_err());
        assert!(
            logs.events().is_empty(),
            "COMMIT failure cannot emit success"
        );
        assert_eq!(
            snapshot(&f).await,
            before,
            "failed commit rolls back work and cursor acknowledgments"
        );
        f.owner.execute("DROP TRIGGER fixture_commit_fail ON library_evidence_state; DROP FUNCTION fixture_commit_fail()").await.unwrap();

        f.owner.execute(format!("CREATE FUNCTION fixture_cancel_gate() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN PERFORM pg_advisory_xact_lock({gate}::bigint); RETURN NEW; END$$; CREATE TRIGGER fixture_cancel_gate BEFORE INSERT ON library_node_evidence FOR EACH ROW EXECUTE FUNCTION fixture_cancel_gate()").as_str()).await.unwrap();
        let mut blocker = f.owner.begin().await.unwrap();
        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(i64::from(gate))
            .execute(&mut *blocker)
            .await
            .unwrap();
        let logs = Logs::default();
        let owned = t.clone();
        let task = tokio::spawn(
            async move { worker::tick(&owned).await }.with_subscriber(logs.subscriber()),
        );
        wait_for_gate(&f, gate).await;
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        blocker.rollback().await.unwrap();
        // Wait for sqlx's dropped transaction rollback rather than assuming
        // dropping the Rust future instantly removes a server-side query.
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let active: i64 = sqlx::query_scalar("SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND objid=$1::bigint::oid AND database=(SELECT oid FROM pg_database WHERE datname=current_database())")
                    .bind(i64::from(gate)).fetch_one(&f.owner).await.unwrap();
                if active == 0 { break; }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }).await.expect("cancelled worker transaction retired");
        assert!(logs.events().is_empty());
        assert_eq!(
            snapshot(&f).await,
            before,
            "cancellation must not commit or acknowledge work"
        );
        f.owner.execute("DROP TRIGGER fixture_cancel_gate ON library_node_evidence; DROP FUNCTION fixture_cancel_gate()").await.unwrap();
        f.close().await;
    }
}

#[tokio::test]
async fn disabled_tick_is_silent_without_opening_a_database_connection() {
    const CHILD: &str = "CS_LIBRARY_DISABLED_EVENT_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "success_events::disabled_tick_is_silent_without_opening_a_database_connection",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .env("LIBRARY_EVIDENCE_REFRESH_DISABLED", "1")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "disabled child failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed; 0 failed;"));
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("LIBRARY_DISABLED_EVENT_CHILD_PROVEN")
        );
        return;
    }
    // Child-only environment avoids changing a flag under parallel test ticks.
    assert_eq!(
        std::env::var("LIBRARY_EVIDENCE_REFRESH_DISABLED").unwrap(),
        "1"
    );
    let pool = PgPoolOptions::new()
        .acquire_timeout(Duration::from_millis(100))
        .connect_lazy("postgres://unused@127.0.0.1:1/unused")
        .unwrap();
    for tenant in [
        TenantId::Consumer,
        TenantId::Org("org_disabled_fixture".into()),
    ] {
        let t = WorkerTenant {
            tenant,
            pool: pool.clone(),
            store: Arc::new(PostgresStore::new(pool.clone())),
        };
        let logs = Logs::default();
        assert_eq!(
            worker::tick(&t)
                .with_subscriber(logs.subscriber())
                .await
                .unwrap()
                .processed,
            0
        );
        assert!(logs.events().is_empty());
    }
    pool.close().await;
    eprintln!("LIBRARY_DISABLED_EVENT_CHILD_PROVEN");
}
