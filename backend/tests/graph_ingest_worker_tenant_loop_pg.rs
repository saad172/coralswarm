//! m365 Train 2 PR 2.3: proves the durable org hand-off's CONSUMER side —
//! `workers::graph_ingest::tick` dequeuing a `JobKind::GraphMessageIngest`
//! job from an org tenant's OWN `jobs` table, against a REAL second Postgres
//! database, and that the job is invisible to a tick scoped to a DIFFERENT
//! tenant. Mirrors `graph_subscriptions_worker_tenant_loop_pg.rs`'s
//! `ThreeDbFixture` shape (this suite's closest precedent — same PR family,
//! same "control + tenant_a + tenant_b" fixture) and `slack_worker_tenant_
//! loop_pg.rs`'s "job on tenant A is invisible to tenant B worker" pattern.
//!
//! **Scope, matching `graph_subscriptions_worker_tenant_loop_pg.rs`'s own
//! precedent:** every scenario here seeds NO `graphs`/DEK/`account_keys` rows
//! for the job's `graph_id`, so `ingest_atom`'s own `graph_account_id` lookup
//! always resolves to `None` and the ingest 403s — a normal job FAILURE, not
//! a panic or a stuck job. That is a deliberate, documented limitation of
//! this PR (see `workers::graph_ingest`'s own module doc: `graph_id`/
//! `user_id` are captured at enqueue time from tables that are still
//! control-plane-only regardless of plane, so an org-tenant job fails closed
//! until PR 2.4 lands) — this suite proves the MECHANISM (durable hand-off,
//! tenant-scoped dequeue, terminal job state, tenant isolation), not a full
//! successful ingest, exactly the same "processing itself fails closed, but
//! dequeue/discovery visibility is what this test proves" posture
//! `graph_subscriptions_worker_tenant_loop_pg.rs` accepts for its own
//! renewal tick.
//!
//! Gated on `ORGT_TEST_BASE_URL`, same scratch-DB convention as every other
//! `*_pg.rs` suite in this tree:
//!
//!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test graph_ingest_worker_tenant_loop_pg

use std::sync::Arc;

use coralswarm_backend::jobs::{self, queue};
use coralswarm_backend::store::{PostgresStore, Store};
use coralswarm_backend::tenancy::TenantId;
use coralswarm_backend::workers::graph_ingest;
use coralswarm_backend::workers::tenant_harness::{Tick, WorkerTenant};
use coralswarm_backend::AppState;
use sqlx::postgres::PgPoolOptions;
use sqlx::{Connection, Executor, PgConnection, PgPool};
use uuid::Uuid;

fn base_url() -> Option<String> {
    std::env::var("ORGT_TEST_BASE_URL").ok()
}

fn with_db(base: &str, db: &str) -> String {
    let (prefix, rest) = base.split_at(base.rfind('/').expect("url has a path /"));
    let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
    format!("{prefix}/{db}{params}")
}

async fn admin(base: &str) -> PgConnection {
    PgConnection::connect(base).await.expect("connect admin")
}

async fn recreate(conn: &mut PgConnection, db: &str) {
    conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
        .await
        .unwrap();
    conn.execute(format!("CREATE DATABASE {db}").as_str())
        .await
        .unwrap();
}

async fn drop_db(conn: &mut PgConnection, db: &str) {
    conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
        .await
        .ok();
}

fn worker_tenant(tenant: TenantId, pool: &PgPool) -> WorkerTenant {
    WorkerTenant {
        tenant,
        pool: pool.clone(),
        store: Arc::new(PostgresStore::new(pool.clone())) as Arc<dyn Store>,
    }
}

/// Control + two independent org tenant databases — same shape as
/// `graph_subscriptions_worker_tenant_loop_pg.rs`'s own fixture.
struct ThreeDbFixture {
    _databases: Vec<coralswarm_backend::test_db::TestDatabase>,
    admin_conn: PgConnection,
    control: PgPool,
    tenant_a: PgPool,
    tenant_b: PgPool,
}

async fn setup_three_db(prefix: &str) -> ThreeDbFixture {
    let base = base_url().expect("caller already checked base_url().is_some()");
    let (ctl, ten_a, ten_b) = (
        format!("{prefix}_ctl"),
        format!("{prefix}_ten_a"),
        format!("{prefix}_ten_b"),
    );
    let databases = [&ctl, &ten_a, &ten_b].iter().map(|name| coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, name)).collect();
    let mut admin_conn = admin(&base).await;
    for db in [&ctl, &ten_a, &ten_b] {
        recreate(&mut admin_conn, db).await;
    }

    let control: PgPool = PgPoolOptions::new().connect(&with_db(&base, &ctl)).await.unwrap();
    sqlx::migrate!("./migrations_control").run(&control).await.unwrap();

    let tenant_a: PgPool = PgPoolOptions::new().connect(&with_db(&base, &ten_a)).await.unwrap();
    sqlx::migrate!("./migrations_tenant").run(&tenant_a).await.unwrap();
    sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, 'org_a')")
        .execute(&tenant_a)
        .await
        .unwrap();

    let tenant_b: PgPool = PgPoolOptions::new().connect(&with_db(&base, &ten_b)).await.unwrap();
    sqlx::migrate!("./migrations_tenant").run(&tenant_b).await.unwrap();
    sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, 'org_b')")
        .execute(&tenant_b)
        .await
        .unwrap();

    ThreeDbFixture { _databases: databases, admin_conn, control, tenant_a, tenant_b }
}

impl ThreeDbFixture {
    async fn teardown(mut self, prefix: &str) {
        self.control.close().await;
        self.tenant_a.close().await;
        self.tenant_b.close().await;
        for suffix in ["_ctl", "_ten_a", "_ten_b"] {
            drop_db(&mut self.admin_conn, &format!("{prefix}{suffix}")).await;
        }
    }
}

fn state_with_control(control: PgPool) -> Arc<AppState> {
    let mut state = coralswarm_backend::test_app_state_owned(None);
    state.db = Some(control.clone());
    state.store = Some(Arc::new(PostgresStore::new(control)));
    Arc::new(state)
}

async fn job_status(pool: &PgPool, job_id: Uuid) -> String {
    sqlx::query_scalar("SELECT status::text FROM jobs WHERE id = $1")
        .bind(job_id)
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn job_attempts(pool: &PgPool, job_id: Uuid) -> i32 {
    sqlx::query_scalar("SELECT attempts FROM jobs WHERE id = $1")
        .bind(job_id)
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn job_payload_content(pool: &PgPool, job_id: Uuid) -> String {
    let payload: serde_json::Value = sqlx::query_scalar("SELECT payload FROM jobs WHERE id = $1")
        .bind(job_id)
        .fetch_one(pool)
        .await
        .unwrap();
    payload["content"].as_str().unwrap_or_default().to_string()
}

/// A `GraphMessageIngest` job durably enqueued on tenant A's OWN `jobs`
/// table (the exact hand-off `routes::graph_webhook::resolve_graph_ingest_
/// target` performs for an org account whose routing status resolves to
/// tenant A's pool) must be discovered and processed by a `graph_ingest::
/// tick` scoped to tenant A — proving the tenant-loop dequeue works against
/// a REAL second database — and must be completely invisible to a tick
/// scoped to tenant B (a different org's tenant plane).
#[tokio::test]
async fn graph_message_ingest_job_on_tenant_a_is_invisible_to_tenant_b_worker_pg() {
    let Some(_) = base_url() else {
        eprintln!(
            "skipping graph_message_ingest_job_on_tenant_a_is_invisible_to_tenant_b_worker_pg: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };
    let fx = setup_three_db("orgt_t23_ingest_isolation").await;

    let account_id = Uuid::new_v4();
    let user_id = Uuid::new_v4();
    let graph_id = Uuid::new_v4(); // deliberately no matching `graphs` row — see module doc.
    let kind = jobs::JobKind::GraphMessageIngest {
        account_id,
        user_id,
        graph_id,
        channel_name: Some("general".to_string()),
        source_type: "teams".to_string(),
        source_id: format!("msg-{}", Uuid::new_v4()),
        content: "the secret teams message body".to_string(),
        participants: vec![],
        timestamp: chrono::Utc::now(),
        scope: None,
        channel_id: Some("chan-1".to_string()),
        author_id: Some("aad-user-1".to_string()),
        author_name: Some("Alice".to_string()),
    };
    let job_id = queue::enqueue(&fx.tenant_a, &kind, Some(account_id), None, 0)
        .await
        .unwrap()
        .expect("first enqueue must not be deduped");

    let state = state_with_control(fx.control.clone());

    // ── Tenant B's tick must see nothing — org A's job is invisible cross-tenant.
    let wt_b = worker_tenant(TenantId::Org("org_b".into()), &fx.tenant_b);
    let tick_b = graph_ingest::tick(&wt_b, &state).await.expect("tick against tenant B must not error");
    assert_eq!(
        tick_b,
        Tick::idle(),
        "org A's GraphMessageIngest job must be invisible to a tick scoped to org B's own \
         (empty) tenant database"
    );
    assert_eq!(
        job_status(&fx.tenant_a, job_id).await,
        "pending",
        "tenant B's tick must not have touched tenant A's job"
    );

    // ── Tenant A's tick must dequeue and process it (the graph doesn't
    // exist, so ingest 403s and `queue::fail` runs the ordinary retry ladder
    // — `attempts` (0) + 1 < the row's `max_attempts` (3, the column
    // default), so this first failure lands back on 'pending' with a bumped
    // `attempts`/backed-off `run_after`, not a terminal state yet; three such
    // failures would dead-letter it. Either way: a normal recorded failure,
    // never a panic and never an immediately-re-dequeueable stuck row.
    let wt_a = worker_tenant(TenantId::Org("org_a".into()), &fx.tenant_a);
    let tick_a = graph_ingest::tick(&wt_a, &state).await.expect("tick against tenant A must not error");
    assert_eq!(
        tick_a,
        Tick::worked(1),
        "the job must be discovered and processed against org A's OWN tenant database — this \
         dequeue path never ran against a genuine second database before this PR"
    );
    let status = job_status(&fx.tenant_a, job_id).await;
    assert_eq!(
        status, "pending",
        "with no matching `graphs` row, ingest_atom must 403 and the worker must record that via \
         `queue::fail` — the first of up to `max_attempts` retries, so the row goes back to \
         'pending' (backed off), not 'dead'/'failed' yet"
    );
    let attempts = job_attempts(&fx.tenant_a, job_id).await;
    assert_eq!(attempts, 1, "one failed attempt must be recorded");
    // Content-at-rest: only a `done` job is scrubbed (see `workers::
    // graph_ingest`'s own module doc) — a 'pending' (retrying) job must keep
    // its original payload, available for the next attempt and for an
    // operator to diagnose in the meantime.
    assert_eq!(
        job_payload_content(&fx.tenant_a, job_id).await,
        "the secret teams message body",
        "a retrying (not done) job must keep its original payload, unscrubbed"
    );

    fx.teardown("orgt_t23_ingest_isolation").await;
}

/// A `GraphMessageIngest` job idempotency-keyed the same way `routes::
/// graph_webhook`'s enqueue site keys it (`graph_ingest:{account}:{graph}:
/// {source_type}:{source_id}`) must dedupe on a second enqueue attempt,
/// exactly like `SlackMessageIngest`'s own idempotency contract — proving
/// Graph's at-least-once redelivery can't pile up duplicate pending rows for
/// the same notification.
#[tokio::test]
async fn graph_message_ingest_job_dedupes_on_the_same_idempotency_key_pg() {
    let Some(_) = base_url() else {
        eprintln!(
            "skipping graph_message_ingest_job_dedupes_on_the_same_idempotency_key_pg: set \
             ORGT_TEST_BASE_URL"
        );
        return;
    };
    let fx = setup_three_db("orgt_t23_ingest_dedup").await;

    let account_id = Uuid::new_v4();
    let graph_id = Uuid::new_v4();
    let kind = jobs::JobKind::GraphMessageIngest {
        account_id,
        user_id: Uuid::new_v4(),
        graph_id,
        channel_name: None,
        source_type: "outlook".to_string(),
        source_id: "msg-dedup-1".to_string(),
        content: "reply body".to_string(),
        participants: vec![],
        timestamp: chrono::Utc::now(),
        scope: None,
        channel_id: None,
        author_id: None,
        author_name: None,
    };
    let idempotency = format!("graph_ingest:{account_id}:{graph_id}:outlook:msg-dedup-1");

    let first = queue::enqueue(&fx.tenant_a, &kind, Some(account_id), Some(&idempotency), 0)
        .await
        .unwrap();
    assert!(first.is_some(), "first enqueue must land a pending row");

    let second = queue::enqueue(&fx.tenant_a, &kind, Some(account_id), Some(&idempotency), 0)
        .await
        .unwrap();
    assert!(
        second.is_none(),
        "a redelivered notification with the SAME idempotency key must dedupe, not pile up a \
         second pending row"
    );

    let pending_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM jobs WHERE kind = 'GraphMessageIngest'")
            .fetch_one(&fx.tenant_a)
            .await
            .unwrap();
    assert_eq!(pending_count, 1, "exactly one row, not two");

    fx.teardown("orgt_t23_ingest_dedup").await;
}

/// In-app org deletion (control 0387): capture continues during the grace
/// period so an owner's undo loses nothing, and stops at revocation exactly
/// as before. A Teams transcript job for an org with a live deletion is
/// ingested INTACT (not replaced by the consent-redaction marker); once the
/// org is revoked (what the deletion's `revoke_access` step does) the same
/// kind of job is still refused and redacted.
#[tokio::test]
async fn a_teams_transcript_is_ingested_during_org_deletion_grace_and_refused_after_revocation_pg() {
    let Some(_) = base_url() else {
        eprintln!("skipping a_teams_transcript_is_ingested_during_org_deletion_grace_and_refused_after_revocation_pg: set ORGT_TEST_BASE_URL");
        return;
    };
    use coralswarm_core::kek::{generate_dek, load_kek, wrap_dek};
    // SAFETY: this binary's other tests never read MASTER_KEK_HEX.
    unsafe { std::env::set_var("MASTER_KEK_HEX", "ab".repeat(32)) };
    let prefix = "orgt_orgdel_capture";
    let fx = setup_three_db(prefix).await;
    const TID: &str = "11111111-2222-3333-4444-555555555555";
    const AAD: &str = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
    const MEETING: &str = "MSpt_orgdel_meeting";

    let wrapped = wrap_dek(&load_kek().unwrap(), &generate_dek()).unwrap();
    let user: Uuid = sqlx::query_scalar("INSERT INTO users(id,email) VALUES(gen_random_uuid(),'m@orgdel.test') RETURNING id")
        .fetch_one(&fx.control).await.unwrap();
    let account: Uuid = sqlx::query_scalar("INSERT INTO accounts(owner_user_id, clerk_org_id, dek_wrapped) VALUES($1,'org_a',$2) RETURNING id")
        .bind(user).bind(&wrapped).fetch_one(&fx.control).await.unwrap();
    sqlx::query("INSERT INTO account_members(account_id,user_id,role,seat_tier_id) VALUES($1,$2,'owner',(SELECT id FROM seat_tiers WHERE code='standard' AND is_active=true))")
        .bind(account).bind(user).execute(&fx.control).await.unwrap();
    sqlx::query("INSERT INTO account_keys(account_id,dek_wrapped) VALUES($1,$2)")
        .bind(account).bind(&wrapped).execute(&fx.tenant_a).await.unwrap();
    sqlx::query("INSERT INTO oauth_tokens(account_id,provider,scope,access_token,provider_workspace_id,provider_user_id,owner_user_id) VALUES($1,'microsoft','x','\\x00',$2,$3,$4)")
        .bind(account).bind(TID).bind(AAD).bind(user).execute(&fx.control).await.unwrap();
    sqlx::query("INSERT INTO org_connector_flags(clerk_org_id,provider,enabled) VALUES('org_a','microsoft',true)")
        .execute(&fx.control).await.unwrap();
    let graph: Uuid = sqlx::query_scalar("INSERT INTO graphs(id,account_id,name,kind,owner_user_id) VALUES(gen_random_uuid(),$1,'Member','member',$2) RETURNING id")
        .bind(account).bind(user).fetch_one(&fx.tenant_a).await.unwrap();
    sqlx::query("INSERT INTO teams_meeting_captures(account_id,user_id,aad_id,tenant_id,meeting_id,occurrence_start,occurrence_end,graph_id,source) VALUES($1,$2,$3,$4,$5,now()-interval '2 hours',now()-interval '1 hour',$6,'explicit_meeting')")
        .bind(account).bind(user).bind(AAD).bind(TID).bind(MEETING).bind(graph)
        .execute(&fx.tenant_a).await.unwrap();

    let state = state_with_control(fx.control.clone());
    let wt = worker_tenant(TenantId::Org("org_a".into()), &fx.tenant_a);
    let enqueue = |text: &str| {
        let kind = jobs::JobKind::GraphMessageIngest {
            account_id: account,
            user_id: user,
            graph_id: graph,
            channel_name: None,
            source_type: "teams_transcript".to_string(),
            source_id: format!("{MEETING}:{text}"),
            content: text.to_string(),
            participants: vec![],
            timestamp: chrono::Utc::now() - chrono::Duration::minutes(90),
            scope: None,
            channel_id: None,
            author_id: None,
            author_name: None,
        };
        let pool = fx.tenant_a.clone();
        async move { queue::enqueue(&pool, &kind, Some(account), None, 0).await.unwrap().unwrap() }
    };
    let atoms = |needle: &'static str| {
        let pool = fx.tenant_a.clone();
        async move {
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM atoms WHERE source_type='teams_transcript' AND source_id LIKE 'MSpt_orgdel_meeting:' || $1 || '%'")
                .bind(needle).fetch_one(&pool).await.unwrap_or(-1)
        }
    };

    // A job the consent gate lets through is ingested (the payload is scrubbed
    // to "[redacted on completion]" only after success); a job it refuses is
    // completed `done` with the consent-redaction marker instead.
    const MARKER: &str = "[redacted: meeting consent no longer active]";

    // Baseline: no deletion.
    let j0 = enqueue("BASELINE-TRANSCRIPT-LINE").await;
    assert_eq!(graph_ingest::tick(&wt, &state).await.unwrap(), Tick::worked(1));
    let baseline_status = job_status(&fx.tenant_a, j0).await;
    assert_ne!(job_payload_content(&fx.tenant_a, j0).await, MARKER, "baseline: the gate admits the member");

    // The owner requests deletion: members are locked out, capture is NOT.
    sqlx::query("INSERT INTO org_deletions (account_id, clerk_org_id, initiated_by, state) VALUES ($1,'org_a','owner','grace')")
        .bind(account).execute(&fx.control).await.unwrap();
    let st: Option<String> = sqlx::query_scalar("SELECT org_deletion_state_for_org('org_a')").fetch_one(&fx.control).await.unwrap();
    assert_eq!(st.as_deref(), Some("grace"), "the strict member predicate now refuses this org");
    let j1 = enqueue("GRACE-TRANSCRIPT-LINE").await;
    assert_eq!(graph_ingest::tick(&wt, &state).await.unwrap(), Tick::worked(1));
    assert_ne!(
        job_payload_content(&fx.tenant_a, j1).await,
        MARKER,
        "capture during grace must not be dropped as 'consent no longer active'"
    );
    assert_eq!(
        job_status(&fx.tenant_a, j1).await,
        baseline_status,
        "a transcript during grace follows exactly the path it follows with no deletion"
    );
    assert_eq!(baseline_status, "done");
    assert_eq!(atoms("BASELINE-TRANSCRIPT-LINE").await, 1);
    assert_eq!(atoms("GRACE-TRANSCRIPT-LINE").await, 1, "the transcript is stored intact during grace");

    // Revocation (the destroy step's first act): refused and redacted as before.
    sqlx::query("INSERT INTO org_revocations(clerk_org_id) VALUES('org_a')").execute(&fx.control).await.unwrap();
    let j2 = enqueue("AFTER-REVOKE-TRANSCRIPT-LINE").await;
    assert_eq!(graph_ingest::tick(&wt, &state).await.unwrap(), Tick::worked(1));
    assert_eq!(job_payload_content(&fx.tenant_a, j2).await, "[redacted: meeting consent no longer active]");
    assert_eq!(atoms("AFTER-REVOKE-TRANSCRIPT-LINE").await, 0);

    fx.teardown(prefix).await;
}
