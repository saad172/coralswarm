use coralswarm_backend::{
    jobs::{queue, JobKind, STANDING_EXTRACTION_KINDS as STANDING_JOB_KINDS},
    source_evidence::{self, CaptureSource},
    tenancy::scope::{begin_account_scoped, begin_tenant_system_scoped_without_account},
};
use serde_json::{json, Value};
use sqlx::{postgres::{PgConnectOptions, PgPoolOptions}, PgPool, Postgres, Row, Transaction};
use std::{str::FromStr, time::Duration};
use uuid::Uuid;

const OLD_KINDS: &[&str] = &["ExtractMeetingAtoms", "ExtractSourceAtoms"];
const OLD_CLAIM_SQL: &str = "
        WITH claimed AS (
            SELECT id
            FROM   jobs
            WHERE  status    = 'pending'
              AND  run_after <= now()
              AND  kind       = ANY($1)
            ORDER BY run_after, id
            LIMIT  $2
            FOR UPDATE SKIP LOCKED
        )
        UPDATE jobs j
        SET    status     = 'running',
               started_at = now()
        FROM   claimed
        WHERE  j.id = claimed.id
        RETURNING j.id, j.kind, j.payload, j.attempts, j.account_id
        ";

struct Fixture {
    admin: PgPool,
    owner: PgPool,
    app: PgPool,
    database: coralswarm_backend::test_db::TestDatabase,
    role: String,
    tenant: bool,
    account: Uuid,
    user: Uuid,
    graph: Uuid,
}

impl Fixture {
    async fn new(tenant: bool) -> Self {
        let url = std::env::var("ORGT_TEST_BASE_URL").expect("local ORGT_TEST_BASE_URL required");
        let options = PgConnectOptions::from_str(&url).unwrap();
        assert!(matches!(options.get_host(), "127.0.0.1" | "localhost" | "::1"));
        let admin = PgPoolOptions::new().max_connections(2).connect_with(options.clone()).await.unwrap();
        let suffix = Uuid::new_v4().simple().to_string();
        let database = coralswarm_backend::test_db::TestDatabase::new(&options, "cor22_controller_queue");
        let role = database.role("cor22_controller_role");
        sqlx::query(&format!("CREATE DATABASE {database}")).execute(&admin).await.unwrap();
        let owner = PgPoolOptions::new().max_connections(10)
            .connect_with(options.clone().database(&database)).await.unwrap();
        let mut base_migrator = if tenant {
            sqlx::migrate!("./migrations_tenant")
        } else {
            sqlx::migrate!("./migrations_control")
        };
        base_migrator.migrations = std::borrow::Cow::Owned(base_migrator.iter()
            .filter(|migration| migration.version <= if tenant { 150 } else { 318 })
            .cloned().collect());
        base_migrator.run(&owner).await.unwrap();
        let mut connection = owner.acquire().await.unwrap();
        sqlx::query("SELECT set_config('app.identity_serving_contract','composite-v1',false)")
            .execute(&mut *connection).await.unwrap();
        sqlx::raw_sql(include_str!("../schema/identity-contract.sql"))
            .execute(&mut *connection).await.unwrap();
        drop(connection);
        let user = Uuid::new_v4();
        let account = Uuid::new_v4();
        if !tenant {
            sqlx::query("INSERT INTO users(id,email,clerk_user_id) VALUES($1,$2,$2)")
                .bind(user).bind(format!("cor22-{suffix}@test.local")).execute(&owner).await.unwrap();
            sqlx::query("INSERT INTO accounts(id,owner_user_id) VALUES($1,$2)")
                .bind(account).bind(user).execute(&owner).await.unwrap();
            sqlx::query("INSERT INTO account_members(account_id,user_id,role) VALUES($1,$2,'owner')")
                .bind(account).bind(user).execute(&owner).await.unwrap();
        }
        let graph = sqlx::query_scalar("INSERT INTO graphs(account_id,name) VALUES($1,'COR22 queue proof') RETURNING id")
            .bind(account).fetch_one(&owner).await.unwrap();
        let password = Uuid::new_v4().simple().to_string();
        sqlx::query(&format!("CREATE ROLE {role} LOGIN INHERIT NOSUPERUSER NOBYPASSRLS PASSWORD '{password}'"))
            .execute(&admin).await.unwrap();
        let base_role = if tenant { "coralswarm_tenant_app" } else { "coralswarm_app" };
        sqlx::query(&format!("GRANT {base_role} TO {role}")).execute(&admin).await.unwrap();
        let app = PgPoolOptions::new().max_connections(10)
            .connect_with(options.database(&database).username(&role).password(&password))
            .await.unwrap();
        let restricted: bool = sqlx::query_scalar("SELECT NOT rolsuper AND NOT rolbypassrls FROM pg_roles WHERE rolname=current_user")
            .fetch_one(&app).await.unwrap();
        assert!(restricted);
        let fixture = Self { admin, owner, app, database, role, tenant, account, user, graph };
        let historical = fixture.capture("historical-before-standing-migration").await;
        sqlx::query("UPDATE source_versions SET extraction_status='done' WHERE version_id=$1")
            .bind(historical).execute(&fixture.owner).await.unwrap();
        sqlx::query("UPDATE corpus_capture_admissions SET state='processed',reason='pre_controller_completed_evidence' WHERE source_version_id=$1")
            .bind(historical).execute(&fixture.owner).await.unwrap();
        if tenant {
            sqlx::migrate!("./migrations_tenant").run(&fixture.owner).await.unwrap();
        } else {
            sqlx::migrate!("./migrations_control").run(&fixture.owner).await.unwrap();
        }
        let history: (bool, bool) = sqlx::query_as("SELECT ever_held,processed_at IS NULL FROM corpus_capture_admissions WHERE source_version_id=$1")
            .bind(historical).fetch_one(&fixture.owner).await.unwrap();
        assert_eq!(history, (true, true));
        fixture
    }

    async fn begin(&self) -> Transaction<'static, Postgres> {
        if self.tenant {
            begin_tenant_system_scoped_without_account(&self.app).await.unwrap()
        } else {
            begin_account_scoped(&self.app, self.account).await.unwrap()
        }
    }

    async fn capture(&self, name: &str) -> Uuid {
        let mut tx = self.begin().await;
        let source = CaptureSource {
            source_type: "note", provider_namespace: "fixture:cor22", source_id: name,
            normalized_text: "The queue compatibility proof retains this exact source.",
            representation: "original_text", metadata: &json!({}), occurred_at: None,
            participants: &[], ingested_by: Some(self.user), scope: "private",
            duration_ms: None, segments: None,
        };
        let saved = source_evidence::capture_source_version_tx(
            &mut tx, None, self.graph, self.account, &source, None, &[42; 32],
        ).await.unwrap();
        tx.commit().await.unwrap();
        saved.version_id
    }

    async fn run(&self, profile: &str) -> Uuid {
        let mut tx = self.begin().await;
        let id = sqlx::query_scalar(
            "INSERT INTO corpus_enrichment_runs(graph_id,account_id,requested_by,mode,corpus_kind,status,
             extraction_version,model,max_items,max_characters,max_model_calls,run_kind,execution_principal,standing_pause_state)
             VALUES($1,$2,$3,'execute','real','running',$4,'fixture/pinned',1000,8000000,10000,'standing',$3,'none')
             RETURNING run_id",
        ).bind(self.graph).bind(self.account).bind(self.user).bind(profile)
            .fetch_one(&mut *tx).await.unwrap();
        tx.commit().await.unwrap();
        id
    }

    async fn lock_version(&self, tx: &mut Transaction<'_, Postgres>, version: Uuid) {
        sqlx::query("SELECT pg_advisory_xact_lock(hashtext('corpus-version:' || $1::text))")
            .bind(version.to_string()).execute(&mut **tx).await.unwrap();
        sqlx::query("SELECT source_item_id FROM source_items WHERE graph_id=$1 AND source_item_id=(SELECT source_item_id FROM source_versions WHERE version_id=$2) FOR UPDATE")
            .bind(self.graph).bind(version).fetch_one(&mut **tx).await.unwrap();
        sqlx::query("SELECT version_id FROM source_versions WHERE graph_id=$1 AND version_id=$2 FOR UPDATE")
            .bind(self.graph).bind(version).fetch_one(&mut **tx).await.unwrap();
    }

    async fn adopt(&self, tx: &mut Transaction<'_, Postgres>, run: Uuid, version: Uuid) -> bool {
        sqlx::query("SELECT run_id FROM corpus_enrichment_runs WHERE run_id=$1 FOR SHARE")
            .bind(run).fetch_one(&mut **tx).await.unwrap();
        self.lock_version(tx, version).await;
        let jobs: Vec<(String, Option<Uuid>, Option<String>)> = sqlx::query_as(
            "SELECT status::text,account_id,payload->>'graph_id' FROM jobs WHERE payload->>'source_version_id'=$1
             AND status::text IN ('pending','running') FOR UPDATE",
        ).bind(version.to_string()).fetch_all(&mut **tx).await.unwrap();
        if jobs.iter().any(|(status, account, graph)| status == "running"
            || account.is_some_and(|account| account!=self.account)
            || graph.as_deref()!=Some(self.graph.to_string().as_str())) { return false; }
        let owned: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM corpus_enrichment_units WHERE graph_id=$1 AND COALESCE(promoted_version_id,holding_id)=$2 AND execution_owner)")
            .bind(self.graph).bind(version).fetch_one(&mut **tx).await.unwrap();
        if owned { return false; }
        sqlx::query(
            "INSERT INTO corpus_enrichment_units(run_id,graph_id,kind,holding_id,source_item_id,
             extraction_version,state,execution_owner)
             SELECT $1,graph_id,'retained_version',version_id,source_item_id,extraction_version,'queued',true
             FROM source_versions WHERE version_id=$2",
        ).bind(run).bind(version).execute(&mut **tx).await.unwrap();
        sqlx::query(
            "UPDATE jobs SET kind=CASE kind WHEN 'ExtractSourceAtoms' THEN 'ExtractStandingSourceAtoms'
             WHEN 'ExtractMeetingAtoms' THEN 'ExtractStandingMeetingAtoms' ELSE kind END
             WHERE payload->>'source_version_id'=$1 AND status::text='pending'",
        ).bind(version.to_string()).execute(&mut **tx).await.unwrap();
        let start: i64 = sqlx::query_scalar("SELECT next_start_char FROM source_versions WHERE version_id=$1")
            .bind(version).fetch_one(&mut **tx).await.unwrap();
        source_evidence::queue_extraction_tx(tx, self.graph, version, self.account, start).await.unwrap();
        true
    }

    async fn enqueue_old(&self, tx: &mut Transaction<'_, Postgres>, version: Uuid, start: i64) -> Uuid {
        let id = queue::enqueue_tx(tx, &JobKind::ExtractSourceAtoms {
            graph_id: self.graph, source_version_id: version, start_char: start,
        }, Some(self.account), Some(&format!("source-atoms:{version}:{start}")), 0).await.unwrap();
        if let Some(id) = id { return id; }
        sqlx::query_scalar("SELECT id FROM jobs WHERE idempotency_key=$1 AND status='pending'")
            .bind(format!("source-atoms:{version}:{start}")).fetch_one(&mut **tx).await.unwrap()
    }

    async fn assert_owned(&self, version: Uuid, expected_jobs: i64) {
        let owners: i64 = sqlx::query_scalar("SELECT count(*) FROM corpus_enrichment_units WHERE holding_id=$1 AND execution_owner")
            .bind(version).fetch_one(&self.owner).await.unwrap();
        assert_eq!(owners, 1);
        let jobs: i64 = sqlx::query_scalar("SELECT count(*) FROM jobs WHERE payload->>'source_version_id'=$1 AND status::text='pending' AND kind='ExtractStandingSourceAtoms'")
            .bind(version.to_string()).fetch_one(&self.owner).await.unwrap();
        assert_eq!(jobs, expected_jobs);
        let linked: bool = sqlx::query_scalar("SELECT run_id IS NOT NULL AND unit_id IS NOT NULL FROM corpus_capture_admissions WHERE source_version_id=$1")
            .bind(version).fetch_one(&self.owner).await.unwrap();
        assert!(linked);
    }

    async fn finish(self) {
        self.app.close().await;
        self.owner.close().await;
        sqlx::query(&format!("DROP DATABASE {} WITH (FORCE)", self.database)).execute(&self.admin).await.unwrap();
        sqlx::query(&format!("DROP ROLE {}", self.role)).execute(&self.admin).await.unwrap();
        self.admin.close().await;
    }
}

async fn wait_for_lock(owner: &PgPool, pid: i32) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let waiting: bool = sqlx::query_scalar("SELECT COALESCE((SELECT wait_event_type='Lock' FROM pg_stat_activity WHERE pid=$1),false)")
                .bind(pid).fetch_one(owner).await.unwrap();
            if waiting { break; }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.expect("the competing SQL must actually wait on its version lock");
}

#[tokio::test]
async fn standing_schema_preserves_identity_retirement_and_observation_on_both_planes() {
    for tenant in [false, true] {
        let f = Fixture::new(tenant).await;
        let version = f.capture("shape").await;
        let fresh: (bool, bool) = sqlx::query_as("SELECT ever_held,processed_at IS NULL FROM corpus_capture_admissions WHERE source_version_id=$1")
            .bind(version).fetch_one(&f.owner).await.unwrap();
        assert_eq!(fresh, (false, true));
        let run = f.run("source-atoms-v2").await;
        for statement in [
            "UPDATE corpus_enrichment_runs SET run_kind='cohort' WHERE run_id=$1",
            "UPDATE corpus_enrichment_runs SET status='complete' WHERE run_id=$1",
            "UPDATE corpus_enrichment_runs SET execution_principal=NULL WHERE run_id=$1",
            "UPDATE corpus_enrichment_runs SET mode='dry_run' WHERE run_id=$1",
            "UPDATE corpus_enrichment_runs SET requested_by=gen_random_uuid() WHERE run_id=$1",
        ] {
            assert!(sqlx::query(statement).bind(run).execute(&f.owner).await.is_err());
        }
        let duplicate = sqlx::query("INSERT INTO corpus_enrichment_runs(graph_id,account_id,requested_by,mode,corpus_kind,status,extraction_version,model,max_items,max_characters,max_model_calls,run_kind,execution_principal,standing_pause_state) SELECT graph_id,account_id,requested_by,mode,corpus_kind,status,extraction_version,model,max_items,max_characters,max_model_calls,run_kind,execution_principal,standing_pause_state FROM corpus_enrichment_runs WHERE run_id=$1")
            .bind(run).execute(&f.owner).await.unwrap_err();
        assert_eq!(duplicate.as_database_error().unwrap().constraint(), Some("corpus_standing_run_live"));
        let mut tx = f.begin().await;
        assert!(f.adopt(&mut tx, run, version).await);
        tx.commit().await.unwrap();
        sqlx::query("UPDATE corpus_enrichment_runs SET retired_at=now() WHERE run_id=$1")
            .bind(run).execute(&f.owner).await.unwrap();
        assert!(sqlx::query("UPDATE corpus_enrichment_runs SET retired_at=NULL WHERE run_id=$1")
            .bind(run).execute(&f.owner).await.is_err());
        let replacement = f.run("source-atoms-v2").await;
        assert_ne!(replacement, run);
        for statement in [
            "UPDATE corpus_enrichment_units SET execution_owner=false WHERE holding_id=$1",
            "UPDATE corpus_enrichment_units SET extraction_version='meeting-atoms-v1' WHERE holding_id=$1",
        ] { assert!(sqlx::query(statement).bind(version).execute(&f.owner).await.is_err()); }
        sqlx::query("UPDATE corpus_capture_admissions SET ever_held=true,processed_at=now() WHERE source_version_id=$1")
            .bind(version).execute(&f.owner).await.unwrap();
        let before: Value = sqlx::query_scalar("SELECT to_jsonb(a) FROM corpus_capture_admissions a WHERE source_version_id=$1")
            .bind(version).fetch_one(&f.owner).await.unwrap();
        sqlx::query("UPDATE corpus_capture_admissions SET ever_held=false,processed_at=NULL WHERE source_version_id=$1")
            .bind(version).execute(&f.owner).await.unwrap();
        let after: Value = sqlx::query_scalar("SELECT to_jsonb(a) FROM corpus_capture_admissions a WHERE source_version_id=$1")
            .bind(version).fetch_one(&f.owner).await.unwrap();
        assert_eq!(before, after);
        f.finish().await;
    }
}

#[tokio::test]
async fn receipt_worker_completion_uses_database_clock_and_keeps_first_stamp_on_both_planes() {
    for tenant in [false, true] {
        let f = Fixture::new(tenant).await;
        let version = f.capture("receipt-worker-clock").await;
        let run = f.run("source-atoms-v2").await;
        let mut tx = f.begin().await;
        assert!(f.adopt(&mut tx, run, version).await);
        tx.commit().await.unwrap();
        let mut tx = f.begin().await;
        let started: chrono::DateTime<chrono::Utc> = sqlx::query_scalar("SELECT transaction_timestamp()")
            .fetch_one(&mut *tx).await.unwrap();
        sqlx::query("SELECT pg_sleep(0.12)").execute(&mut *tx).await.unwrap();
        let lower: chrono::DateTime<chrono::Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *tx).await.unwrap();
        sqlx::query("UPDATE source_versions SET extraction_status='done' WHERE version_id=$1")
            .bind(version).execute(&mut *tx).await.unwrap();
        coralswarm_backend::corpus_enrichment::window_outcome_tx(
            &mut tx, f.graph, version, "source-atoms-v2", "processed", None,
        ).await.unwrap();
        let upper: chrono::DateTime<chrono::Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *tx).await.unwrap();
        let stamp: Option<chrono::DateTime<chrono::Utc>> = sqlx::query_scalar(
            "SELECT processed_at FROM corpus_capture_admissions WHERE source_version_id=$1",
        ).bind(version).fetch_one(&mut *tx).await.unwrap();
        let stamp = stamp.expect("genuine worker completion records database processed_at");
        assert!(started < lower && lower <= stamp && stamp <= upper);
        tx.commit().await.unwrap();
        for pause in ["operator", "both", "processing", "none"] {
            set_pause(&f, run, pause).await;
            let mut tx = f.begin().await;
            coralswarm_backend::corpus_enrichment::window_outcome_tx(
                &mut tx, f.graph, version, "source-atoms-v2", "queued", None,
            ).await.unwrap();
            coralswarm_backend::corpus_enrichment::window_outcome_tx(
                &mut tx, f.graph, version, "source-atoms-v2", "processed", None,
            ).await.unwrap();
            tx.commit().await.unwrap();
            let row: (chrono::DateTime<chrono::Utc>, String, bool) = sqlx::query_as(
                "SELECT processed_at,hold_basis,ever_held FROM corpus_capture_admissions WHERE source_version_id=$1",
            ).bind(version).fetch_one(&f.owner).await.unwrap();
            assert_eq!(row, (stamp, "unverified_transition".into(), false));
        }
        f.finish().await;
    }
}

#[tokio::test]
async fn receipt_history_and_effective_app_grants_prevent_clock_and_hold_rewrites() {
    for tenant in [false, true] {
        let f = Fixture::new(tenant).await;
        let historical: Uuid = sqlx::query_scalar(
            "SELECT source_version_id FROM corpus_capture_admissions WHERE reason='pre_controller_completed_evidence'",
        ).fetch_one(&f.owner).await.unwrap();
        let before: Value = sqlx::query_scalar("SELECT to_jsonb(a) FROM corpus_capture_admissions a WHERE source_version_id=$1")
            .bind(historical).fetch_one(&f.owner).await.unwrap();
        assert_eq!(before["hold_basis"], "legacy_unknown");
        assert_eq!(before["ever_held"], true);
        assert!(before["processed_at"].is_null());
        sqlx::query("UPDATE corpus_capture_admissions SET state=state,processed_at=now(),ever_held=false,hold_basis='never_held',observed_at='1900-01-01' WHERE source_version_id=$1")
            .bind(historical).execute(&f.owner).await.unwrap();
        let after: Value = sqlx::query_scalar("SELECT to_jsonb(a) FROM corpus_capture_admissions a WHERE source_version_id=$1")
            .bind(historical).fetch_one(&f.owner).await.unwrap();
        assert_eq!(before, after);
        for statement in [
            "UPDATE corpus_capture_admissions SET processed_at=now() WHERE source_version_id=$1",
            "UPDATE corpus_capture_admissions SET ever_held=false WHERE source_version_id=$1",
            "UPDATE corpus_capture_admissions SET hold_basis='never_held' WHERE source_version_id=$1",
            "UPDATE corpus_capture_admissions SET captured_by=gen_random_uuid() WHERE source_version_id=$1",
            "DELETE FROM corpus_capture_admissions WHERE source_version_id=$1",
        ] {
            let mut tx = f.begin().await;
            let error = sqlx::query(statement).bind(historical).execute(&mut *tx).await.unwrap_err();
            assert_eq!(error.as_database_error().unwrap().code().as_deref(), Some("42501"));
            tx.rollback().await.unwrap();
        }
        for column in ["source_version_id", "graph_id", "account_id", "source_item_id", "captured_by", "extraction_version"] {
            let value = if column == "extraction_version" { "'forged-profile'" } else { "gen_random_uuid()" };
            let error = sqlx::query(&format!("UPDATE corpus_capture_admissions SET {column}={value} WHERE source_version_id=$1"))
                .bind(historical).execute(&f.owner).await.unwrap_err();
            assert_eq!(error.as_database_error().unwrap().code().as_deref(), Some("23514"));
        }
        assert_eq!(f.capture("historical-before-standing-migration").await, historical);
        let mut tx = f.begin().await;
        for state in ["queued", "processed"] {
            coralswarm_backend::corpus_enrichment::window_outcome_tx(
                &mut tx, f.graph, historical, "source-atoms-v2", state, None,
            ).await.unwrap();
        }
        let row: (String, bool, bool) = sqlx::query_as("SELECT hold_basis,ever_held,processed_at IS NOT NULL FROM corpus_capture_admissions WHERE source_version_id=$1")
            .bind(historical).fetch_one(&mut *tx).await.unwrap();
        assert_eq!(row, ("legacy_unknown".into(), true, true));
        tx.commit().await.unwrap();
        let mut tx = f.begin().await;
        sqlx::query("DELETE FROM graphs WHERE id=$1").bind(f.graph).execute(&mut *tx).await.unwrap();
        tx.commit().await.unwrap();
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM corpus_capture_admissions WHERE graph_id=$1")
            .bind(f.graph).fetch_one(&f.owner).await.unwrap();
        assert_eq!(count, 0);
        f.finish().await;
    }
}

#[tokio::test]
async fn receipt_unknown_survives_old_upsert_rebind_and_invalid_completion() {
    for tenant in [false, true] {
        let f = Fixture::new(tenant).await;
        let version = f.capture("receipt-old-writer").await;
        sqlx::query("DELETE FROM corpus_capture_admissions WHERE source_version_id=$1")
            .bind(version).execute(&f.owner).await.unwrap();
        let mut tx = f.begin().await;
        let lower: chrono::DateTime<chrono::Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *tx).await.unwrap();
        sqlx::query("INSERT INTO corpus_capture_admissions(source_version_id,graph_id,account_id,source_item_id,captured_by,extraction_version,state,reason,observed_at) SELECT version_id,graph_id,$2,source_item_id,ingested_by,extraction_version,'blocked','managed_admission_required','1900-01-01' FROM source_versions WHERE version_id=$1")
            .bind(version).bind(f.account).execute(&mut *tx).await.unwrap();
        let upper: chrono::DateTime<chrono::Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *tx).await.unwrap();
        tx.commit().await.unwrap();
        let original: chrono::DateTime<chrono::Utc> = sqlx::query_scalar("SELECT observed_at FROM corpus_capture_admissions WHERE source_version_id=$1")
            .bind(version).fetch_one(&f.owner).await.unwrap();
        assert!(lower <= original && original <= upper);
        for state in ["queued", "failed", "unavailable"] {
            let mut tx = f.begin().await;
            coralswarm_backend::corpus_enrichment::window_outcome_tx(
                &mut tx, f.graph, version, "source-atoms-v2", state, Some("operator_paused"),
            ).await.unwrap();
            tx.commit().await.unwrap();
            let row: (bool, String, bool) = sqlx::query_as("SELECT processed_at IS NULL,hold_basis,ever_held FROM corpus_capture_admissions WHERE source_version_id=$1")
                .bind(version).fetch_one(&f.owner).await.unwrap();
            assert_eq!(row, (true, "unverified_transition".into(), false));
        }
        let before = standing_snapshot(&f).await;
        let mut tx = f.begin().await;
        let error = coralswarm_backend::corpus_enrichment::window_outcome_tx(
            &mut tx, f.graph, version, "source-atoms-v2", "processed", None,
        ).await.unwrap_err();
        assert_eq!(error.as_database_error().unwrap().code().as_deref(), Some("23514"));
        tx.rollback().await.unwrap();
        assert_eq!(before, standing_snapshot(&f).await);
        let run = f.run("source-atoms-v2").await;
        let mut tx = f.begin().await;
        assert!(f.adopt(&mut tx, run, version).await);
        sqlx::query("UPDATE corpus_capture_admissions SET run_id=NULL,unit_id=NULL,state='blocked',reason=NULL WHERE source_version_id=$1")
            .bind(version).execute(&mut *tx).await.unwrap();
        sqlx::query("INSERT INTO corpus_capture_admissions(source_version_id,graph_id,account_id,source_item_id,captured_by,extraction_version,run_id,unit_id,state,reason,observed_at) SELECT version_id,graph_id,$2,source_item_id,ingested_by,extraction_version,NULL,NULL,'blocked','managed_admission_required','1900-01-01' FROM source_versions WHERE version_id=$1 ON CONFLICT(source_version_id) DO UPDATE SET run_id=COALESCE(corpus_capture_admissions.run_id,EXCLUDED.run_id),unit_id=COALESCE(corpus_capture_admissions.unit_id,EXCLUDED.unit_id),state=CASE WHEN corpus_capture_admissions.state='processed' THEN corpus_capture_admissions.state ELSE EXCLUDED.state END,reason=CASE WHEN corpus_capture_admissions.state='processed' THEN corpus_capture_admissions.reason ELSE EXCLUDED.reason END,updated_at=now()")
            .bind(version).bind(f.account).execute(&mut *tx).await.unwrap();
        tx.commit().await.unwrap();
        let row: (chrono::DateTime<chrono::Utc>, bool, String, bool) = sqlx::query_as("SELECT observed_at,processed_at IS NULL,hold_basis,ever_held FROM corpus_capture_admissions WHERE source_version_id=$1")
            .bind(version).fetch_one(&f.owner).await.unwrap();
        assert_eq!(row, (original, true, "unverified_transition".into(), false));
        f.finish().await;
    }
}

#[tokio::test]
async fn receipt_selected_publication_insert_stamps_only_active_matching_proof() {
    for tenant in [false, true] {
        let f = Fixture::new(tenant).await;
        let source = f.capture("receipt-selected-original").await;
        let target: Uuid = sqlx::query_scalar("INSERT INTO graphs(account_id,name,kind) VALUES($1,'selected receipt target','org') RETURNING id")
            .bind(f.account).fetch_one(&f.owner).await.unwrap();
        let item = Uuid::new_v4();
        let version = Uuid::new_v4();
        let publication = Uuid::new_v4();
        let mut tx = f.begin().await;
        sqlx::query("SELECT set_config('app.user_id',$1,true)").bind(f.user.to_string()).execute(&mut *tx).await.unwrap();
        sqlx::query("INSERT INTO source_items(source_item_id,graph_id,source_type,provider_namespace,source_id,origin) VALUES($1,$2,'selected_evidence',$3,$4,$5)")
            .bind(item).bind(target).bind(format!("selected:{target}")).bind(publication.to_string()).bind(Option::<Value>::None).execute(&mut *tx).await.unwrap();
        sqlx::query("INSERT INTO source_versions(version_id,graph_id,source_item_id,source_id,source_type,provider_namespace,raw_inline,content_sha256,fingerprint_sha256,representation,source_metadata,ingested_by,scope,extraction_version,extraction_status,next_start_char,origin) VALUES($1,$2,$3,$4,'selected_evidence',$5,$6,$7,$7,$12,$8,$9,'shared','selected-passages-v1','done',$10,$11)")
            .bind(version).bind(target).bind(item).bind(publication.to_string()).bind(format!("selected:{target}")).bind("retained selection").bind("b".repeat(64)).bind(json!({"publication_receipt_id":publication})).bind(f.user).bind(18_i64).bind(Option::<Value>::None).bind("original_text").execute(&mut *tx).await.unwrap();
        sqlx::query("UPDATE source_items SET current_version_id=$2 WHERE source_item_id=$1")
            .bind(item).bind(version).execute(&mut *tx).await.unwrap();
        tx.commit().await.unwrap();
        let insert = "INSERT INTO corpus_capture_admissions(graph_id,account_id,source_item_id,source_version_id,captured_by,extraction_version,state,reason) VALUES($1,$2,$3,$4,$5,'selected-passages-v1','processed','deterministic_selected_passages_no_model')";
        let mut tx = f.begin().await;
        sqlx::query("SELECT set_config('app.user_id',$1,true)").bind(f.user.to_string()).execute(&mut *tx).await.unwrap();
        let error = sqlx::query(insert).bind(target).bind(f.account).bind(item).bind(version).bind(f.user).execute(&mut *tx).await.unwrap_err();
        assert_eq!(error.as_database_error().unwrap().code().as_deref(), Some("23514"));
        tx.rollback().await.unwrap();
        let mut tx = f.begin().await;
        sqlx::query("SELECT set_config('app.user_id',$1,true)").bind(f.user.to_string()).execute(&mut *tx).await.unwrap();
        let original_item: Uuid = sqlx::query_scalar("SELECT source_item_id FROM source_versions WHERE version_id=$1")
            .bind(source).fetch_one(&mut *tx).await.unwrap();
        sqlx::query("INSERT INTO selected_evidence_publications(publication_id,account_id,owner_user_id,request_id,source_graph_id,source_item_id,source_version_id,selection,passage_ids,target_graph_id,target_source_item_id,target_source_version_id,selected_sha256) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)")
            .bind(publication).bind(f.account).bind(f.user).bind(Uuid::new_v4()).bind(f.graph).bind(original_item).bind(source).bind(json!([])).bind(vec![Uuid::new_v4()]).bind(target).bind(item).bind(version).bind("b".repeat(64)).execute(&mut *tx).await.unwrap();
        let lower: chrono::DateTime<chrono::Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *tx).await.unwrap();
        sqlx::query(insert).bind(target).bind(f.account).bind(item).bind(version).bind(f.user).execute(&mut *tx).await.unwrap();
        let upper: chrono::DateTime<chrono::Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *tx).await.unwrap();
        let stamp: Option<chrono::DateTime<chrono::Utc>> = sqlx::query_scalar("SELECT processed_at FROM corpus_capture_admissions WHERE source_version_id=$1")
            .bind(version).fetch_one(&mut *tx).await.unwrap();
        let stamp = stamp.expect("selected-publication writer records database processed_at");
        assert!(lower <= stamp && stamp <= upper);
        let row: (String, bool, bool, bool) = sqlx::query_as("SELECT hold_basis,ever_held,run_id IS NULL,unit_id IS NULL FROM corpus_capture_admissions WHERE source_version_id=$1")
            .bind(version).fetch_one(&mut *tx).await.unwrap();
        assert_eq!(row, ("unverified_transition".into(), false, true, true));
        tx.commit().await.unwrap();
        f.finish().await;
    }
}

#[tokio::test]
async fn standing_adoption_wins_old_claim_and_preserves_pending_key_on_both_planes() {
    for tenant in [false, true] {
        let f = Fixture::new(tenant).await;
        let version = f.capture("adoption-first").await;
        let run = f.run("source-atoms-v2").await;
        let mut old = f.begin().await;
        let job = f.enqueue_old(&mut old, version, 0).await;
        old.commit().await.unwrap();
        let mut adoption = f.begin().await;
        assert!(f.adopt(&mut adoption, run, version).await);
        assert!(queue::dequeue(&f.app, OLD_KINDS, 50).await.unwrap().is_empty());
        adoption.commit().await.unwrap();
        assert!(queue::dequeue(&f.app, OLD_KINDS, 50).await.unwrap().is_empty());
        f.assert_owned(version, 1).await;
        let mut continuation = f.begin().await;
        f.enqueue_old(&mut continuation, version, 0).await;
        continuation.commit().await.unwrap();
        f.assert_owned(version, 1).await;
        let claimed = queue::dequeue(&f.app, STANDING_JOB_KINDS, 2).await.unwrap();
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].id, job);
        f.finish().await;
    }
}

#[tokio::test]
async fn standing_old_claim_at_any_cursor_excludes_adoption_on_both_planes() {
    assert!(include_str!("../src/jobs/queue.rs").contains(OLD_CLAIM_SQL));
    for tenant in [false, true] {
        let f = Fixture::new(tenant).await;
        let version = f.capture("claim-first").await;
        let run = f.run("source-atoms-v2").await;
        let mut tx = f.begin().await;
        let job = f.enqueue_old(&mut tx, version, 8000).await;
        tx.commit().await.unwrap();
        let mut old_claim = f.app.begin().await.unwrap();
        let claimed = sqlx::query(OLD_CLAIM_SQL).bind(OLD_KINDS).bind(2_i64)
            .fetch_all(&mut *old_claim).await.unwrap();
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].get::<Uuid,_>("id"), job);
        let mut adoption = f.begin().await;
        let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()").fetch_one(&mut *adoption).await.unwrap();
        {
            let attempt = f.adopt(&mut adoption, run, version);
            tokio::pin!(attempt);
            tokio::select! {
                result = &mut attempt => panic!("adoption did not wait for old claim, result={result}"),
                _ = wait_for_lock(&f.owner, pid) => old_claim.commit().await.unwrap(),
            }
            assert!(!attempt.await);
        }
        adoption.commit().await.unwrap();
        let owners: i64 = sqlx::query_scalar("SELECT count(*) FROM corpus_enrichment_units WHERE holding_id=$1 AND execution_owner")
            .bind(version).fetch_one(&f.owner).await.unwrap();
        assert_eq!(owners, 0);
        sqlx::query("UPDATE jobs SET status='done' WHERE id=$1").bind(job).execute(&f.owner).await.unwrap();
        let mut adoption = f.begin().await;
        assert!(f.adopt(&mut adoption, run, version).await);
        adoption.commit().await.unwrap();
        f.assert_owned(version, 1).await;
        f.finish().await;
    }
}

#[tokio::test]
async fn standing_old_insert_waits_for_adoption_then_reads_committed_owner_on_both_planes() {
    for tenant in [false, true] {
        let f = Fixture::new(tenant).await;
        let version = f.capture("insert-waits").await;
        let run = f.run("source-atoms-v2").await;
        let mut adoption = f.begin().await;
        assert!(f.adopt(&mut adoption, run, version).await);
        let mut insert = f.begin().await;
        let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()").fetch_one(&mut *insert).await.unwrap();
        let graph = f.graph;
        let account = f.account;
        let pending = tokio::spawn(async move {
            let kind: String = sqlx::query_scalar("INSERT INTO jobs(kind,payload,account_id) VALUES('ExtractSourceAtoms',$1,$2) RETURNING kind")
                .bind(json!({"graph_id":graph,"source_version_id":version,"start_char":8000})).bind(account)
                .fetch_one(&mut *insert).await.unwrap();
            insert.commit().await.unwrap();
            kind
        });
        wait_for_lock(&f.owner, pid).await;
        adoption.commit().await.unwrap();
        assert_eq!(pending.await.unwrap(), "ExtractStandingSourceAtoms");
        assert!(queue::dequeue(&f.app, OLD_KINDS, 50).await.unwrap().is_empty());
        f.assert_owned(version, 2).await;
        f.finish().await;
    }
}

#[tokio::test]
async fn standing_old_version_first_retry_finishes_before_adoption_without_deadlock() {
    for tenant in [false, true] {
        let f = Fixture::new(tenant).await;
        let version = f.capture("retry-first").await;
        let run = f.run("source-atoms-v2").await;
        sqlx::query("INSERT INTO corpus_enrichment_units(run_id,graph_id,kind,holding_id,source_item_id,extraction_version,state,execution_owner) SELECT $1,graph_id,'retained_version',version_id,source_item_id,extraction_version,'queued',true FROM source_versions WHERE version_id=$2")
            .bind(run).bind(version).execute(&f.owner).await.unwrap();
        let mut retry = f.begin().await;
        sqlx::query("SELECT version_id FROM source_versions WHERE version_id=$1 FOR UPDATE")
            .bind(version).fetch_one(&mut *retry).await.unwrap();
        let mut adoption = f.begin().await;
        sqlx::query("SELECT source_item_id FROM source_items WHERE source_item_id=(SELECT source_item_id FROM source_versions WHERE version_id=$1) FOR UPDATE")
            .bind(version).fetch_one(&mut *adoption).await.unwrap();
        let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()").fetch_one(&mut *adoption).await.unwrap();
        let waiting = tokio::spawn(async move {
            sqlx::query("SELECT version_id FROM source_versions WHERE version_id=$1 FOR UPDATE")
                .bind(version).fetch_one(&mut *adoption).await.unwrap();
            adoption
        });
        wait_for_lock(&f.owner, pid).await;
        tokio::time::timeout(Duration::from_secs(2), source_evidence::queue_extraction_tx(
            &mut retry, f.graph, version, f.account, 0,
        )).await.expect("old retry must not acquire the parent lock").unwrap();
        retry.commit().await.unwrap();
        let mut adoption = waiting.await.unwrap();
        assert!(!f.adopt(&mut adoption, run, version).await);
        adoption.commit().await.unwrap();
        f.assert_owned(version, 1).await;
        f.finish().await;
    }
}

#[tokio::test]
async fn standing_capture_continuation_reaper_and_dead_history_keep_queue_classification() {
    for tenant in [false, true] {
        let f = Fixture::new(tenant).await;
        let version = f.capture("continuation").await;
        let run = f.run("source-atoms-v2").await;
        let mut tx = f.begin().await;
        let dead = f.enqueue_old(&mut tx, version, 16000).await;
        tx.commit().await.unwrap();
        sqlx::query("UPDATE jobs SET status='dead',finished_at=now(),error_message='historical' WHERE id=$1")
            .bind(dead).execute(&f.owner).await.unwrap();
        let before: Value = sqlx::query_scalar("SELECT to_jsonb(j) FROM jobs j WHERE id=$1")
            .bind(dead).fetch_one(&f.owner).await.unwrap();
        let mut adoption = f.begin().await;
        assert!(f.adopt(&mut adoption, run, version).await);
        adoption.commit().await.unwrap();
        let claimed = queue::dequeue(&f.app, STANDING_JOB_KINDS, 1).await.unwrap();
        assert_eq!(claimed.len(), 1);
        sqlx::query("UPDATE jobs SET started_at=now()-interval '2 hours' WHERE id=$1")
            .bind(claimed[0].id).execute(&f.owner).await.unwrap();
        let reaped = queue::reap(&f.app, 60, 50, None).await.unwrap();
        assert_eq!(reaped.len(), 1);
        assert!(queue::dequeue(&f.app, OLD_KINDS, 50).await.unwrap().is_empty());
        let mut continuation = f.begin().await;
        source_evidence::queue_extraction_tx(&mut continuation, f.graph, version, f.account, 8000).await.unwrap();
        continuation.commit().await.unwrap();
        let next: String = sqlx::query_scalar("SELECT kind FROM jobs WHERE payload->>'source_version_id'=$1 AND payload->>'start_char'='8000'")
            .bind(version.to_string()).fetch_one(&f.owner).await.unwrap();
        assert_eq!(next, "ExtractStandingSourceAtoms");
        let after: Value = sqlx::query_scalar("SELECT to_jsonb(j) FROM jobs j WHERE id=$1")
            .bind(dead).fetch_one(&f.owner).await.unwrap();
        assert_eq!(before, after);
        f.finish().await;
    }
}

#[tokio::test]
async fn standing_legacy_claim_backstop_refuses_without_taking_source_locks() {
    for tenant in [false, true] {
        let f = Fixture::new(tenant).await;
        let version = f.capture("claim-backstop").await;
        let run = f.run("source-atoms-v2").await;
        let mut tx = f.begin().await;
        let job = f.enqueue_old(&mut tx, version, 0).await;
        tx.commit().await.unwrap();
        sqlx::query("INSERT INTO corpus_enrichment_units(run_id,graph_id,kind,holding_id,source_item_id,extraction_version,state,execution_owner) SELECT $1,graph_id,'retained_version',version_id,source_item_id,extraction_version,'queued',true FROM source_versions WHERE version_id=$2")
            .bind(run).bind(version).execute(&f.owner).await.unwrap();
        let mut locked = f.begin().await;
        f.lock_version(&mut locked, version).await;
        let denied = tokio::time::timeout(Duration::from_secs(2), queue::dequeue(&f.app, OLD_KINDS, 50))
            .await.expect("claim guard must not wait for source/version locks").unwrap_err();
        assert_eq!(denied.as_database_error().unwrap().code().as_deref(), Some("23514"));
        locked.rollback().await.unwrap();
        let status: String = sqlx::query_scalar("SELECT status::text FROM jobs WHERE id=$1")
            .bind(job).fetch_one(&f.owner).await.unwrap();
        assert_eq!(status, "pending");
        sqlx::query("UPDATE jobs SET kind='ExtractStandingSourceAtoms' WHERE id=$1")
            .bind(job).execute(&f.owner).await.unwrap();
        assert_eq!(queue::dequeue(&f.app, STANDING_JOB_KINDS, 50).await.unwrap().len(), 1);
        f.finish().await;
    }
}

#[tokio::test]
async fn standing_both_profiles_have_matching_wire_billing_and_database_routes() {
    for tenant in [false, true] {
        let f = Fixture::new(tenant).await;
        let version = f.capture("meeting-route").await;
        sqlx::query("UPDATE source_versions SET extraction_version='meeting-atoms-v1' WHERE version_id=$1")
            .bind(version).execute(&f.owner).await.unwrap();
        let run = f.run("meeting-atoms-v1").await;
        let mut adoption = f.begin().await;
        assert!(f.adopt(&mut adoption, run, version).await);
        adoption.commit().await.unwrap();
        let claimed = queue::dequeue(&f.app, STANDING_JOB_KINDS, 50).await.unwrap();
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].kind, "ExtractStandingMeetingAtoms");
        let parsed: JobKind = serde_json::from_value(json!({"kind":claimed[0].kind,"payload":claimed[0].payload})).unwrap();
        assert!(matches!(parsed, JobKind::ExtractStandingMeetingAtoms { .. }));
        for kind in STANDING_JOB_KINDS {
            assert!(!coralswarm_backend::workers::billing_derivation::JOB_AXIS_KINDS.contains(kind));
        }
        let function: String = sqlx::query_scalar("SELECT pg_get_functiondef('corpus_standing_queue_route()'::regprocedure)")
            .fetch_one(&f.owner).await.unwrap();
        for kind in STANDING_JOB_KINDS { assert!(function.contains(kind)); }
        let secure: bool = sqlx::query_scalar("SELECT prosecdef AND proconfig=ARRAY['search_path=pg_catalog, pg_temp']::text[] FROM pg_proc WHERE oid='corpus_standing_queue_route()'::regprocedure")
            .fetch_one(&f.owner).await.unwrap();
        assert!(secure);
        f.finish().await;
    }
}

#[tokio::test]
async fn standing_queue_guard_binds_account_profile_and_exact_source_under_restricted_logins() {
    for tenant in [false, true] {
        let f = Fixture::new(tenant).await;
        let version = f.capture("identity-guard").await;
        let run = f.run("source-atoms-v2").await;
        let mut adoption = f.begin().await;
        assert!(f.adopt(&mut adoption, run, version).await);
        adoption.commit().await.unwrap();
        for (kind, account) in [
            ("ExtractSourceAtoms", Some(Uuid::new_v4())),
            ("ExtractSourceAtoms", None),
            ("ExtractMeetingAtoms", Some(f.account)),
        ] {
            let mut tx = f.begin().await;
            let denied = sqlx::query("INSERT INTO jobs(kind,payload,account_id) VALUES($1,$2,$3)")
                .bind(kind).bind(json!({"graph_id":f.graph,"source_version_id":version,"start_char":8000}))
                .bind(account).execute(&mut *tx).await.unwrap_err();
            assert_eq!(denied.as_database_error().unwrap().code().as_deref(), Some("23514"));
            assert_eq!(denied.as_database_error().unwrap().message(), "standing extraction identity is incompatible");
            tx.rollback().await.unwrap();
        }
        for payload in [
            json!({"graph_id":Uuid::new_v4(),"source_version_id":version,"start_char":8000}),
            json!({"graph_id":f.graph,"source_version_id":Uuid::new_v4(),"start_char":8000}),
            json!({"graph_id":f.graph,"source_version_id":"malformed","start_char":8000}),
        ] {
            let mut tx = f.begin().await;
            let kind: String = sqlx::query_scalar("INSERT INTO jobs(kind,payload,account_id) VALUES('ExtractSourceAtoms',$1,$2) RETURNING kind")
                .bind(payload).bind(f.account).fetch_one(&mut *tx).await.unwrap();
            assert_eq!(kind, "ExtractSourceAtoms");
            tx.rollback().await.unwrap();
        }
        let denied = sqlx::query("SELECT corpus_standing_queue_route()")
            .execute(&f.app).await.unwrap_err();
        assert_eq!(denied.as_database_error().unwrap().code().as_deref(), Some("42501"));
        let public_execute: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_proc p CROSS JOIN LATERAL aclexplode(p.proacl) a WHERE p.oid='corpus_standing_queue_route()'::regprocedure AND a.grantee=0 AND a.privilege_type='EXECUTE')")
            .fetch_one(&f.owner).await.unwrap();
        assert!(!public_execute);
        let mut shadow = f.begin().await;
        sqlx::query("CREATE TEMP TABLE source_versions(graph_id uuid,version_id uuid,extraction_version text)")
            .execute(&mut *shadow).await.unwrap();
        sqlx::query("CREATE TEMP TABLE corpus_enrichment_units(run_id uuid,graph_id uuid,holding_id uuid,promoted_version_id uuid,extraction_version text,execution_owner bool)")
            .execute(&mut *shadow).await.unwrap();
        sqlx::query("SELECT set_config('search_path','pg_temp,public',true)")
            .execute(&mut *shadow).await.unwrap();
        let routed: String = sqlx::query_scalar("INSERT INTO public.jobs(kind,payload,account_id) VALUES('ExtractSourceAtoms',$1,$2) RETURNING kind")
            .bind(json!({"graph_id":f.graph,"source_version_id":version,"start_char":8000})).bind(f.account)
            .fetch_one(&mut *shadow).await.unwrap();
        assert_eq!(routed, "ExtractStandingSourceAtoms");
        shadow.rollback().await.unwrap();
        f.assert_owned(version, 1).await;
        f.finish().await;
    }
}

#[tokio::test]
async fn standing_stale_old_admission_repair_keeps_owner_cursor_and_one_pending_job() {
    for tenant in [false, true] {
        let f = Fixture::new(tenant).await;
        let version = f.capture("stale-admission").await;
        let run = f.run("source-atoms-v2").await;
        sqlx::query("UPDATE source_versions SET next_start_char=777 WHERE version_id=$1")
            .bind(version).execute(&f.owner).await.unwrap();
        let old_owner: Option<Uuid> = sqlx::query_scalar("SELECT unit_id FROM corpus_enrichment_units WHERE graph_id=$1 AND holding_id=$2 AND execution_owner")
            .bind(f.graph).bind(version).fetch_optional(&f.owner).await.unwrap();
        assert_eq!(old_owner, None);
        let mut adoption = f.begin().await;
        assert!(f.adopt(&mut adoption, run, version).await);
        adoption.commit().await.unwrap();
        let mut stale = f.begin().await;
        sqlx::query("INSERT INTO corpus_capture_admissions(source_version_id,graph_id,account_id,source_item_id,captured_by,extraction_version,run_id,unit_id,state,reason)
            SELECT version_id,graph_id,$2,source_item_id,ingested_by,extraction_version,NULL,NULL,'blocked','managed_admission_required' FROM source_versions WHERE version_id=$1
            ON CONFLICT(source_version_id) DO UPDATE SET run_id=COALESCE(corpus_capture_admissions.run_id,EXCLUDED.run_id),unit_id=COALESCE(corpus_capture_admissions.unit_id,EXCLUDED.unit_id),state=CASE WHEN corpus_capture_admissions.state='processed' THEN corpus_capture_admissions.state ELSE EXCLUDED.state END,reason=CASE WHEN corpus_capture_admissions.state='processed' THEN corpus_capture_admissions.reason ELSE EXCLUDED.reason END,updated_at=now()")
            .bind(version).bind(f.account).execute(&mut *stale).await.unwrap();
        sqlx::query("UPDATE source_versions SET extraction_status='blocked',extraction_error='managed_admission_required' WHERE version_id=$1 AND extraction_status<>'done'")
            .bind(version).execute(&mut *stale).await.unwrap();
        stale.commit().await.unwrap();
        let state: String = sqlx::query_scalar("SELECT state FROM corpus_capture_admissions WHERE source_version_id=$1")
            .bind(version).fetch_one(&f.owner).await.unwrap();
        assert_eq!(state, "blocked");
        for _ in 0..2 {
            let mut repair = f.begin().await;
            f.lock_version(&mut repair, version).await;
            source_evidence::queue_extraction_tx(&mut repair, f.graph, version, f.account, 777).await.unwrap();
            repair.commit().await.unwrap();
        }
        f.assert_owned(version, 1).await;
        let repaired: (String, String, i64, Uuid) = sqlx::query_as("SELECT a.state,v.extraction_status,v.next_start_char,a.run_id FROM corpus_capture_admissions a JOIN source_versions v ON v.version_id=a.source_version_id WHERE v.version_id=$1")
            .bind(version).fetch_one(&f.owner).await.unwrap();
        assert_eq!(repaired, ("queued".into(), "pending".into(), 777, run));
        f.finish().await;
    }
}

#[tokio::test]
async fn standing_queue_guard_leaves_cohort_and_unrelated_job_updates_unchanged() {
    for tenant in [false, true] {
        let f = Fixture::new(tenant).await;
        let version = f.capture("cohort").await;
        let run: Uuid = sqlx::query_scalar("INSERT INTO corpus_enrichment_runs(graph_id,account_id,requested_by,mode,status,extraction_version,model,max_items,max_characters,max_model_calls) VALUES($1,$2,$3,'execute','running','source-atoms-v2','fixture/cohort',1,10,1) RETURNING run_id")
            .bind(f.graph).bind(f.account).bind(f.user).fetch_one(&f.owner).await.unwrap();
        sqlx::query("INSERT INTO corpus_enrichment_units(run_id,graph_id,kind,holding_id,source_item_id,extraction_version,state,execution_owner) SELECT $1,graph_id,'retained_version',version_id,source_item_id,extraction_version,'queued',true FROM source_versions WHERE version_id=$2")
            .bind(run).bind(version).execute(&f.owner).await.unwrap();
        let mut enqueue = f.begin().await;
        let job = f.enqueue_old(&mut enqueue, version, 0).await;
        enqueue.commit().await.unwrap();
        let claimed = queue::dequeue(&f.app, OLD_KINDS, 50).await.unwrap();
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].id, job);
        assert_eq!(claimed[0].kind, "ExtractSourceAtoms");
        let mut reserve = f.begin().await;
        assert!(matches!(coralswarm_backend::corpus_enrichment::reserve_window_tx(&mut reserve, f.graph, version, "source-atoms-v2", 10, true).await.unwrap(), coralswarm_backend::corpus_enrichment::WindowGate::Permit { .. }));
        assert_eq!(coralswarm_backend::corpus_enrichment::reserve_window_tx(&mut reserve, f.graph, version, "source-atoms-v2", 1, true).await.unwrap(), coralswarm_backend::corpus_enrichment::WindowGate::Hold);
        reserve.commit().await.unwrap();
        let held: (String, String) = sqlx::query_as("SELECT status,pause_reason FROM corpus_enrichment_runs WHERE run_id=$1")
            .bind(run).fetch_one(&f.owner).await.unwrap();
        assert_eq!(held, ("paused".into(), "execution_budget_exhausted".into()));
        let unrelated: Uuid = sqlx::query_scalar("INSERT INTO jobs(kind,payload,account_id) VALUES('DeleteSourceBlob','{}',$1) RETURNING id")
            .bind(f.account).fetch_one(&f.owner).await.unwrap();
        sqlx::query("UPDATE jobs SET payload='{\"changed\":true}',status='running' WHERE id=$1")
            .bind(unrelated).execute(&f.app).await.unwrap();
        let mut forbidden = f.begin().await;
        let denied = sqlx::query("UPDATE jobs SET payload=jsonb_set(payload,'{source_version_id}',to_jsonb($2::text)),status='pending' WHERE id=$1")
            .bind(job).bind(Uuid::new_v4().to_string()).execute(&mut *forbidden).await.unwrap_err();
        assert_eq!(denied.as_database_error().unwrap().message(), "active extraction job identity is immutable");
        forbidden.rollback().await.unwrap();
        f.finish().await;
    }
}

fn uuid_spellings(id: Uuid) -> Vec<String> {
    let canonical = id.to_string();
    let upper = canonical.to_ascii_uppercase();
    vec![canonical.clone(), upper.clone(), id.simple().to_string(), id.simple().to_string().to_ascii_uppercase(),
        format!("{{{canonical}}}"), format!("{{{upper}}}"), format!("urn:uuid:{canonical}"), format!("urn:uuid:{upper}")]
}

async fn standing_snapshot(f: &Fixture) -> Value {
    sqlx::query_scalar("SELECT jsonb_build_object('runs',(SELECT jsonb_agg(to_jsonb(r) ORDER BY run_id) FROM corpus_enrichment_runs r),'units',(SELECT jsonb_agg(to_jsonb(u) ORDER BY unit_id) FROM corpus_enrichment_units u),'versions',(SELECT jsonb_agg(to_jsonb(v) ORDER BY version_id) FROM source_versions v),'receipts',(SELECT jsonb_agg(to_jsonb(a) ORDER BY source_version_id) FROM corpus_capture_admissions a),'jobs',(SELECT jsonb_agg(to_jsonb(j) ORDER BY id) FROM jobs j))")
        .fetch_one(&f.owner).await.unwrap()
}

fn assert_identity_refusal(error: sqlx::Error) {
    let error = error.as_database_error().unwrap();
    assert_eq!(error.code().as_deref(), Some("23514"));
    assert_eq!(error.message(), "active extraction job identity is immutable");
}

#[tokio::test]
async fn standing_uuid_parser_matches_actual_jobkind_on_both_planes() {
    let id = Uuid::parse_str("aBcDef01-2345-6789-aBcD-eF0123456789").unwrap();
    let mut cases: Vec<(Option<Value>, Option<Uuid>)> = [id, Uuid::nil()].into_iter()
        .flat_map(|id| uuid_spellings(id).into_iter().map(move |text| (Some(json!(text)), Some(id))))
        .collect();
    for value in [
        json!(&id.simple().to_string()[..31]), json!("abcdef01-23456789-abcd-ef0123456789"),
        json!("abcdef012345-6789-abcd-ef0123456789"), json!("abcdef01-2345-6789-abcd-ef01234567890"),
        json!("abcdef01-2345-6789-abcd-ef012345678g"), json!(format!("URN:UUID:{id}")),
        json!(format!("urn:UUID:{id}")), json!(format!("{{{}}}", id.simple())),
        json!(format!(" {id}")), json!(format!("{id} ")), json!(format!("{id}\n")),
        json!(format!("{id}\t")), json!(format!("{{{id}}}\n")), json!(format!("urn:uuid:{id}\n")),
        json!(format!("{{{id}")), json!(format!("{id}}}")), json!(format!("urn:uuid:{}", id.simple())),
        json!("0000-0000-0000-0000-0000-0000-0000-0000"), json!("ａｂｃｄｅｆ0123456789abcdef0123456789"),
        json!(""), Value::Null, json!(12), json!(true), json!([]), json!(vec![0; 16]), json!({"id":id}),
    ] { cases.push((Some(value), None)); }
    cases.push((Some(json!("aBcDef01-2345-6789-aBcD-eF0123456789")), Some(id)));
    cases.push((None, None));
    for tenant in [false, true] {
        let f = Fixture::new(tenant).await;
        let properties: bool = sqlx::query_scalar("SELECT NOT prosecdef AND proisstrict AND provolatile='i' AND proconfig=ARRAY['search_path=pg_catalog']::text[] AND NOT EXISTS(SELECT 1 FROM aclexplode(proacl) a WHERE a.grantee=0 AND a.privilege_type='EXECUTE') FROM pg_proc WHERE oid='public.corpus_queue_uuid(jsonb)'::regprocedure")
            .fetch_one(&f.owner).await.unwrap();
        assert!(properties);
        let denied = sqlx::query("SELECT public.corpus_queue_uuid($1)").bind(json!(id))
            .execute(&f.app).await.unwrap_err();
        assert_eq!(denied.as_database_error().unwrap().code().as_deref(), Some("42501"));
        for (value, expected) in &cases {
            let parsed: Option<Uuid> = sqlx::query_scalar("SELECT public.corpus_queue_uuid($1)")
                .bind(value).fetch_one(&f.owner).await.unwrap();
            assert_eq!(parsed, *expected, "SQL tenant={tenant} value={value:?}");
            for kind in OLD_KINDS.iter().chain(STANDING_JOB_KINDS) {
                for field in ["graph_id", "source_version_id"] {
                    let mut payload = json!({"graph_id":id,"source_version_id":id,"start_char":777});
                    match value {
                        Some(value) => { payload[field] = value.clone(); }
                        None => { payload.as_object_mut().unwrap().remove(field); }
                    }
                    let parsed = serde_json::from_value::<JobKind>(json!({"kind":kind,"payload":payload}));
                    assert_eq!(parsed.is_ok(), expected.is_some(), "JobKind tenant={tenant} kind={kind} field={field} value={value:?}");
                    if let Ok(parsed) = parsed {
                        let canonical = serde_json::to_value(parsed).unwrap();
                        assert_eq!(canonical["payload"][field], json!(expected.unwrap()));
                        assert_eq!(canonical["payload"]["start_char"], json!(777));
                    } else {
                        let mut tx = f.begin().await;
                        let row: (String, Value) = sqlx::query_as("INSERT INTO jobs(kind,payload,account_id) VALUES($1,$2,$3) RETURNING kind,payload")
                            .bind(kind).bind(&payload).bind(f.account).fetch_one(&mut *tx).await.unwrap();
                        assert_eq!(row, (kind.to_string(), payload));
                        tx.rollback().await.unwrap();
                    }
                }
            }
        }
        println!("UUID_GRAMMAR tenant={tenant} cases={} kinds=4 fields=2", cases.len());
        f.finish().await;
    }
}

#[tokio::test]
async fn standing_uuid_spellings_route_and_block_preexisting_old_claims_on_both_planes() {
    const CONTENT_SNAPSHOT_SQL: &str = "SELECT jsonb_build_object('runs',(SELECT jsonb_agg(to_jsonb(r) ORDER BY run_id) FROM corpus_enrichment_runs r),'units',(SELECT jsonb_agg(to_jsonb(u) ORDER BY unit_id) FROM corpus_enrichment_units u),'versions',(SELECT jsonb_agg(to_jsonb(v) ORDER BY version_id) FROM source_versions v),'receipts',(SELECT jsonb_agg(to_jsonb(a) ORDER BY source_version_id) FROM corpus_capture_admissions a))";
    for tenant in [false, true] {
        let f = Fixture::new(tenant).await;
        for (profile, legacy, standing) in [
            ("source-atoms-v2", "ExtractSourceAtoms", "ExtractStandingSourceAtoms"),
            ("meeting-atoms-v1", "ExtractMeetingAtoms", "ExtractStandingMeetingAtoms"),
        ] {
            let version = f.capture(profile).await;
            sqlx::query("UPDATE source_versions SET extraction_version=$2 WHERE version_id=$1")
                .bind(version).bind(profile).execute(&f.owner).await.unwrap();
            let run = f.run(profile).await;
            let mut old_jobs = Vec::new();
            for (graph, version_text) in uuid_spellings(f.graph).into_iter().zip(uuid_spellings(version)) {
                let payload = json!({"graph_id":graph,"source_version_id":version_text,"start_char":777});
                let id: Uuid = sqlx::query_scalar("INSERT INTO jobs(kind,payload,account_id) VALUES($1,$2,$3) RETURNING id")
                    .bind(legacy).bind(&payload).bind(f.account).fetch_one(&f.app).await.unwrap();
                old_jobs.push((id, payload));
            }
            sqlx::query("INSERT INTO corpus_enrichment_units(run_id,graph_id,kind,holding_id,source_item_id,extraction_version,state,execution_owner) SELECT $1,graph_id,'retained_version',version_id,source_item_id,extraction_version,'queued',true FROM source_versions WHERE version_id=$2")
                .bind(run).bind(version).execute(&f.owner).await.unwrap();
            let mut locked = f.begin().await;
            f.lock_version(&mut locked, version).await;
            for _ in &old_jobs {
                let before = standing_snapshot(&f).await;
                let denied = tokio::time::timeout(Duration::from_secs(2),
                    sqlx::query(OLD_CLAIM_SQL).bind(OLD_KINDS).bind(1_i64).fetch_all(&f.app))
                    .await.expect("legacy claim must not take a source lock").unwrap_err();
                assert_eq!(denied.as_database_error().unwrap().code().as_deref(), Some("23514"));
                assert_eq!(denied.as_database_error().unwrap().message(), "standing extraction requires a compatible worker");
                assert_eq!(before, standing_snapshot(&f).await);
                let row: (Uuid, Value) = tokio::time::timeout(Duration::from_secs(2),
                    sqlx::query_as("UPDATE jobs SET kind=$1 WHERE id=(SELECT id FROM jobs WHERE kind=$2 AND status='pending' ORDER BY run_after,id LIMIT 1) RETURNING id,payload")
                        .bind(standing).bind(legacy).fetch_one(&f.app))
                    .await.expect("pending alias must not take a source lock").unwrap();
                assert!(old_jobs.contains(&row));
            }
            locked.rollback().await.unwrap();
            assert!(sqlx::query(OLD_CLAIM_SQL).bind(OLD_KINDS).bind(100_i64).fetch_all(&f.app).await.unwrap().is_empty());
            let before = standing_snapshot(&f).await;
            for graph in uuid_spellings(f.graph) {
                for version_text in uuid_spellings(version) {
                    let payload = json!({"graph_id":graph,"source_version_id":version_text,"start_char":8000});
                    for input_kind in [legacy, standing] {
                        let parsed: JobKind = serde_json::from_value(json!({"kind":input_kind,"payload":payload})).unwrap();
                        let canonical = serde_json::to_value(parsed).unwrap();
                        assert_eq!(canonical["payload"]["graph_id"], json!(f.graph));
                        assert_eq!(canonical["payload"]["source_version_id"], json!(version));
                        let mut tx = f.begin().await;
                        let content_before: Value = sqlx::query_scalar(CONTENT_SNAPSHOT_SQL).fetch_one(&mut *tx).await.unwrap();
                        let row: (String, Value) = sqlx::query_as("INSERT INTO jobs(kind,payload,account_id) VALUES($1,$2,$3) RETURNING kind,payload")
                            .bind(input_kind).bind(&payload).bind(f.account).fetch_one(&mut *tx).await.unwrap();
                        assert_eq!(row, (standing.to_string(), payload.clone()));
                        assert!(sqlx::query(OLD_CLAIM_SQL).bind(OLD_KINDS).bind(100_i64).fetch_all(&mut *tx).await.unwrap().is_empty());
                        let content_after: Value = sqlx::query_scalar(CONTENT_SNAPSHOT_SQL).fetch_one(&mut *tx).await.unwrap();
                        assert_eq!(content_before, content_after);
                        tx.rollback().await.unwrap();
                    }
                }
            }
            for (graph, version_text) in uuid_spellings(f.graph).into_iter().zip(uuid_spellings(version)) {
                let payload = json!({"graph_id":graph,"source_version_id":version_text,"start_char":8000});
                for input_kind in OLD_KINDS.iter().chain(STANDING_JOB_KINDS) {
                    for account in [None, Some(Uuid::new_v4()), Some(f.account)] {
                        if (*input_kind == legacy || *input_kind == standing) && account == Some(f.account) { continue; }
                        let mut tx = f.begin().await;
                        let denied = sqlx::query("INSERT INTO jobs(kind,payload,account_id) VALUES($1,$2,$3)")
                            .bind(input_kind).bind(&payload).bind(account).execute(&mut *tx).await.unwrap_err();
                        assert_eq!(denied.as_database_error().unwrap().code().as_deref(), Some("23514"));
                        assert_eq!(denied.as_database_error().unwrap().message(), "standing extraction identity is incompatible");
                        tx.rollback().await.unwrap();
                    }
                }
            }
            assert_eq!(before, standing_snapshot(&f).await);
            println!("UUID_ROUTES tenant={tenant} profile={profile} inserts=128 incompatible=80 legacy_backstops=8 snapshot_unchanged=true");
        }
        f.finish().await;
    }
}

#[tokio::test]
async fn standing_active_raw_identity_is_immutable_before_new_fallback_on_both_planes() {
    for tenant in [false, true] {
        let f = Fixture::new(tenant).await;
        let version = f.capture("raw-identity").await;
        let payload = json!({"graph_id":f.graph,"source_version_id":version,"start_char":777});
        let mut replacements = vec![Value::Null, json!({}), json!([]), json!("replacement")];
        for field in ["graph_id", "source_version_id"] {
            for value in [json!(Uuid::new_v4()), json!("malformed"), Value::Null, json!(123), json!(false), json!([]), json!({}), json!(if field == "graph_id" { f.graph.simple().to_string() } else { version.simple().to_string() })] {
                let mut replacement = payload.clone();
                replacement[field] = value;
                replacements.push(replacement);
            }
            let mut replacement = payload.clone();
            replacement.as_object_mut().unwrap().remove(field);
            replacements.push(replacement);
        }
        let kinds: Vec<&str> = OLD_KINDS.iter().chain(STANDING_JOB_KINDS).copied().collect();
        let mut refused = 0;
        for kind in &kinds {
            for status in ["pending", "running"] {
                let job: Uuid = sqlx::query_scalar("INSERT INTO jobs(kind,status,payload,account_id) VALUES($1,$2::job_status,$3,$4) RETURNING id")
                    .bind(kind).bind(status).bind(&payload).bind(f.account).fetch_one(&f.app).await.unwrap();
                let before = standing_snapshot(&f).await;
                for replacement in &replacements {
                    for new_status in [status, "done", "dead"] {
                        let mut tx = f.begin().await;
                        assert_identity_refusal(sqlx::query("UPDATE jobs SET payload=$2,status=$3::job_status WHERE id=$1")
                            .bind(job).bind(replacement).bind(new_status).execute(&mut *tx).await.unwrap_err());
                        tx.rollback().await.unwrap();
                        refused += 1;
                    }
                }
                for new_kind in kinds.iter().copied().chain(["DeleteSourceBlob"]) {
                    for new_status in [status, "done"] {
                        let alias = status == "pending" && new_status == "pending" &&
                            ((*kind == "ExtractSourceAtoms" && new_kind == "ExtractStandingSourceAtoms") ||
                             (*kind == "ExtractMeetingAtoms" && new_kind == "ExtractStandingMeetingAtoms"));
                        if new_kind == *kind || alias { continue; }
                        let mut tx = f.begin().await;
                        assert_identity_refusal(sqlx::query("UPDATE jobs SET kind=$2,status=$3::job_status WHERE id=$1")
                            .bind(job).bind(new_kind).bind(new_status).execute(&mut *tx).await.unwrap_err());
                        tx.rollback().await.unwrap();
                        refused += 1;
                    }
                }
                let alias = if *kind == "ExtractSourceAtoms" { "ExtractStandingSourceAtoms" } else { "ExtractStandingMeetingAtoms" };
                for (new_kind, new_status) in [(None, Some(status)), (Some(alias), None)] {
                    if new_kind == Some(*kind) { continue; }
                    let mut tx = f.begin().await;
                    assert_identity_refusal(sqlx::query("UPDATE jobs SET kind=$2,status=$3::job_status WHERE id=$1")
                        .bind(job).bind(new_kind).bind(new_status).execute(&mut *tx).await.unwrap_err());
                    tx.rollback().await.unwrap();
                    refused += 1;
                }
                for account in [None, Some(Uuid::new_v4())] {
                    for new_status in [status, "done"] {
                        let mut tx = f.begin().await;
                        assert_identity_refusal(sqlx::query("UPDATE jobs SET account_id=$2,status=$3::job_status WHERE id=$1")
                            .bind(job).bind(account).bind(new_status).execute(&mut *tx).await.unwrap_err());
                        tx.rollback().await.unwrap();
                        refused += 1;
                    }
                }
                assert_eq!(before, standing_snapshot(&f).await);
                for field in ["graph_id", "source_version_id"] {
                    for (old_value, new_value) in [
                        (Some(json!("12")), Some(json!(12))),
                        (Some(json!(12)), Some(json!("12"))),
                        (Some(Value::Null), None), (None, Some(Value::Null)),
                        (Some(json!("malformed")), Some(json!(version))),
                    ] {
                        let mut old_payload = payload.clone();
                        let mut new_payload = payload.clone();
                        for (target, value) in [(&mut old_payload, old_value), (&mut new_payload, new_value)] {
                            match value {
                                Some(value) => { target[field] = value; }
                                None => { target.as_object_mut().unwrap().remove(field); }
                            }
                        }
                        let malformed: Uuid = sqlx::query_scalar("INSERT INTO jobs(kind,status,payload,account_id) VALUES($1,$2::job_status,$3,$4) RETURNING id")
                            .bind(kind).bind(status).bind(&old_payload).bind(f.account).fetch_one(&f.app).await.unwrap();
                        let before = standing_snapshot(&f).await;
                        let mut tx = f.begin().await;
                        assert_identity_refusal(sqlx::query("UPDATE jobs SET payload=$2,status='done' WHERE id=$1")
                            .bind(malformed).bind(&new_payload).execute(&mut *tx).await.unwrap_err());
                        tx.rollback().await.unwrap();
                        assert_eq!(before, standing_snapshot(&f).await);
                        refused += 1;
                        sqlx::query("UPDATE jobs SET status='dead' WHERE id=$1").bind(malformed).execute(&f.app).await.unwrap();
                    }
                }
            }
        }
        println!("RAW_IDENTITY tenant={tenant} refused={refused} snapshot_unchanged=true");
        f.finish().await;
    }
}

#[tokio::test]
async fn standing_identity_allows_backfill_cursor_and_status_lifecycle_on_both_planes() {
    for tenant in [false, true] {
        let f = Fixture::new(tenant).await;
        let version = f.capture("identity-lifecycle").await;
        for legacy in OLD_KINDS {
            let payload = json!({"graph_id":f.graph,"source_version_id":version,"start_char":777});
            let job: Uuid = sqlx::query_scalar("INSERT INTO jobs(kind,payload) VALUES($1,$2) RETURNING id")
                .bind(legacy).bind(&payload).fetch_one(&f.app).await.unwrap();
            let claimed = queue::dequeue(&f.app, &[*legacy], 50).await.unwrap();
            assert_eq!(claimed.len(), 1);
            assert_eq!((claimed[0].id, claimed[0].account_id), (job, Some(f.account)));
            let persisted: Option<Uuid> = sqlx::query_scalar("SELECT account_id FROM jobs WHERE id=$1")
                .bind(job).fetch_one(&f.owner).await.unwrap();
            assert_eq!(persisted, Some(f.account));
            queue::fail(&f.app, job, "fixture retry").await.unwrap();
            sqlx::query("UPDATE jobs SET status='pending',payload=jsonb_set(payload,'{start_char}','8000') WHERE id=$1")
                .bind(job).execute(&f.app).await.unwrap();
            let standing = if *legacy == "ExtractSourceAtoms" { "ExtractStandingSourceAtoms" } else { "ExtractStandingMeetingAtoms" };
            sqlx::query("UPDATE jobs SET kind=$2 WHERE id=$1").bind(job).bind(standing).execute(&f.app).await.unwrap();
            sqlx::query("UPDATE jobs SET status='running' WHERE id=$1").bind(job).execute(&f.app).await.unwrap();
            sqlx::query("UPDATE jobs SET status='done' WHERE id=$1").bind(job).execute(&f.app).await.unwrap();
            let row: (String, Value, Uuid) = sqlx::query_as("SELECT kind,payload,account_id FROM jobs WHERE id=$1")
                .bind(job).fetch_one(&f.owner).await.unwrap();
            assert_eq!(row, (standing.into(), json!({"graph_id":f.graph,"source_version_id":version,"start_char":8000}), f.account));
            let pending: Uuid = sqlx::query_scalar("INSERT INTO jobs(kind,payload) VALUES($1,$2) RETURNING id")
                .bind(legacy).bind(&payload).fetch_one(&f.app).await.unwrap();
            let updated = sqlx::query("UPDATE jobs SET account_id=$1 WHERE id=$2 AND account_id IS NULL")
                .bind(f.account).bind(pending).execute(&f.app).await.unwrap();
            assert_eq!(updated.rows_affected(), 1);
            sqlx::query("UPDATE jobs SET status='dead' WHERE id=$1").bind(pending).execute(&f.app).await.unwrap();
        }
        f.finish().await;
    }
}


async fn set_pause(f: &Fixture, run: Uuid, state: &str) {
    let (status, reason) = match state {
        "none" => ("running", None),
        "operator" | "both" => ("paused", Some("operator_paused")),
        "processing" => ("paused", Some("managed_processing_disabled")),
        _ => panic!("unknown fixture pause state"),
    };
    let mut tx = f.begin().await;
    sqlx::query("UPDATE corpus_enrichment_runs SET standing_pause_state=$2,status=$3,pause_reason=$4 WHERE run_id=$1")
        .bind(run).bind(state).bind(status).bind(reason).execute(&mut *tx).await.unwrap();
    tx.commit().await.unwrap();
}

async fn pause_tuple(f: &Fixture, run: Uuid) -> (String, String, Option<String>) {
    sqlx::query_as("SELECT standing_pause_state,status,pause_reason FROM corpus_enrichment_runs WHERE run_id=$1")
        .bind(run).fetch_one(&f.owner).await.unwrap()
}

#[tokio::test]
async fn standing_pause_actions_preserve_both_axes_and_retirement_on_both_planes() {
    use coralswarm_backend::corpus_enrichment::test_standing_pause_tx;
    for tenant in [false, true] {
        let f = Fixture::new(tenant).await;
        let run = f.run("source-atoms-v2").await;
        sqlx::query("UPDATE corpus_enrichment_runs SET retired_at=now() WHERE run_id=$1").bind(run).execute(&f.owner).await.unwrap();
        let retired: Value = sqlx::query_scalar("SELECT to_jsonb(retired_at) FROM corpus_enrichment_runs WHERE run_id=$1").bind(run).fetch_one(&f.owner).await.unwrap();
        let actions = ["operator_pause", "operator_resume", "processing_off", "processing_on"];
        for (state, expected) in [
            ("none", ["operator", "none", "processing", "none"]),
            ("operator", ["operator", "none", "both", "operator"]),
            ("processing", ["both", "processing", "processing", "none"]),
            ("both", ["both", "processing", "both", "operator"]),
        ] {
            for (action, expected) in actions.into_iter().zip(expected) {
                set_pause(&f, run, state).await;
                for _ in 0..2 {
                    let mut tx = f.begin().await;
                    test_standing_pause_tx(&mut tx, f.graph, run, action).await.unwrap();
                    tx.commit().await.unwrap();
                    let row = pause_tuple(&f, run).await;
                    assert_eq!(row.0, expected);
                    assert_eq!(row.1, if expected=="none" {"running"} else {"paused"});
                    assert_eq!(row.2.as_deref(), match expected {"none"=>None,"processing"=>Some("managed_processing_disabled"),_=>Some("operator_paused")});
                    let current: Value = sqlx::query_scalar("SELECT to_jsonb(retired_at) FROM corpus_enrichment_runs WHERE run_id=$1").bind(run).fetch_one(&f.owner).await.unwrap();
                    assert_eq!(current, retired);
                }
            }
        }
        f.finish().await;
    }
}

#[tokio::test]
async fn standing_shape_and_budget_guards_reject_old_writes_under_both_app_roles() {
    for tenant in [false, true] {
        let f = Fixture::new(tenant).await;
        let run = f.run("source-atoms-v2").await;
        for assignment in [
            "standing_pause_state=NULL", "standing_pause_state='other'", "pause_reason='operator_paused'",
            "standing_pause_state='operator',status='paused',pause_reason=NULL",
            "standing_pause_state='processing',status='paused',pause_reason='operator_paused'",
            "admitted_items=1", "attempted_characters=1", "attempted_model_calls=1", "stage=1",
            "cursor_time=now()", "cursor_id=gen_random_uuid()", "execution_principal=NULL",
            "max_items=max_items", "max_characters=max_characters", "max_model_calls=max_model_calls",
            "max_items=1", "max_characters=1", "max_model_calls=1",
        ] {
            let before = standing_snapshot(&f).await;
            let mut tx = f.begin().await;
            let error = sqlx::query(&format!("UPDATE corpus_enrichment_runs SET {assignment} WHERE run_id=$1"))
                .bind(run).execute(&mut *tx).await.unwrap_err();
            assert_eq!(error.as_database_error().unwrap().code().as_deref(), Some("23514"), "{assignment}");
            tx.rollback().await.unwrap();
            assert_eq!(before, standing_snapshot(&f).await, "{assignment}");
        }
        for state in ["none", "operator", "processing", "both"] {
            set_pause(&f,run,state).await;
            for (items,characters,calls) in [(1000_i64,8_000_000_i64,10_000_i64),(1,10,1)] {
                let before=standing_snapshot(&f).await;
                let mut tx=f.begin().await;
                let error=sqlx::query("UPDATE corpus_enrichment_runs SET max_items=$2,max_characters=$3,max_model_calls=$4 WHERE run_id=$1")
                    .bind(run).bind(items).bind(characters).bind(calls).execute(&mut *tx).await.unwrap_err();
                assert_eq!(error.as_database_error().unwrap().message(),"standing runs do not accept logical budgets");
                tx.rollback().await.unwrap();
                assert_eq!(before,standing_snapshot(&f).await);
            }
            for (resume, allowed) in [(false, matches!(state,"operator"|"both")), (true,state=="none")] {
                set_pause(&f, run, state).await;
                let before = pause_tuple(&f,run).await;
                let mut tx=f.begin().await;
                let result=sqlx::query("UPDATE corpus_enrichment_runs SET status=$2,pause_reason=$3,updated_at=now() WHERE run_id=$1 AND status<>'complete'")
                    .bind(run).bind(if resume {"running"}else{"paused"}).bind(if resume {None}else{Some("operator_paused")})
                    .execute(&mut *tx).await;
                assert_eq!(result.is_ok(),allowed,"{state} resume={resume}");
                if allowed { tx.commit().await.unwrap(); }else{ tx.rollback().await.unwrap(); }
                assert_eq!(pause_tuple(&f,run).await,before);
            }
        }
        let cohort: Uuid=sqlx::query_scalar("INSERT INTO corpus_enrichment_runs(graph_id,account_id,requested_by,mode,status,extraction_version,model,max_items,max_characters,max_model_calls) VALUES($1,$2,$3,'execute','running','source-atoms-v2','fixture/cohort',1,10,1) RETURNING run_id")
            .bind(f.graph).bind(f.account).bind(f.user).fetch_one(&f.owner).await.unwrap();
        let mut tx=f.begin().await;
        sqlx::query("UPDATE corpus_enrichment_runs SET max_items=2,max_characters=20,max_model_calls=2,status='paused',pause_reason='operator_paused' WHERE run_id=$1").bind(cohort).execute(&mut *tx).await.unwrap();
        tx.commit().await.unwrap();
        let mut tx=f.begin().await;
        let error=sqlx::query("UPDATE corpus_enrichment_runs SET run_kind='standing',execution_principal=requested_by,standing_pause_state='operator' WHERE run_id=$1").bind(cohort).execute(&mut *tx).await.unwrap_err();
        assert_eq!(error.as_database_error().unwrap().message(),"managed run identity/model/cohort/retirement is immutable");
        tx.rollback().await.unwrap();
        f.finish().await;
    }
}

#[tokio::test]
async fn standing_unit_shape_is_null_safe_and_parent_lookup_fails_closed() {
    for tenant in [false,true] {
        let f=Fixture::new(tenant).await;
        let run=f.run("source-atoms-v2").await;
        let version=f.capture("unit-shape").await;
        for (parent, graph, kind, profile) in [
            (run,f.graph,"legacy_chunk",Some("source-atoms-v2")),
            (run,f.graph,"retained_transcript",Some("source-atoms-v2")),
            (run,f.graph,"local_holdings",Some("source-atoms-v2")),
            (run,f.graph,"retained_version",None),
            (run,f.graph,"retained_version",Some("meeting-atoms-v1")),
            (Uuid::new_v4(),f.graph,"retained_version",Some("source-atoms-v2")),
        ] {
            let mut tx=f.begin().await;
            let error=sqlx::query("INSERT INTO corpus_enrichment_units(run_id,graph_id,kind,holding_id,extraction_version,state) VALUES($1,$2,$3,$4,$5,'pending')")
                .bind(parent).bind(graph).bind(kind).bind(version).bind(profile).execute(&mut *tx).await.unwrap_err();
            assert_eq!(error.as_database_error().unwrap().code().as_deref(),Some("23514"));
            tx.rollback().await.unwrap();
        }
        let mut tx=f.begin().await;
        assert!(f.adopt(&mut tx,run,version).await);
        tx.commit().await.unwrap();
        for assignment in ["extraction_version=NULL","kind='legacy_chunk'","promoted_version_id=holding_id,promotion_fingerprint=repeat('a',64)"] {
            let mut tx=f.begin().await;
            assert!(sqlx::query(&format!("UPDATE corpus_enrichment_units SET {assignment} WHERE run_id=$1")).bind(run).execute(&mut *tx).await.is_err());
            tx.rollback().await.unwrap();
        }
        let protected: bool=sqlx::query_scalar("SELECT prosecdef AND proconfig=ARRAY['search_path=pg_catalog, pg_temp']::text[] AND NOT EXISTS(SELECT 1 FROM aclexplode(proacl) WHERE grantee=0 AND privilege_type='EXECUTE') FROM pg_proc WHERE oid='corpus_standing_unit_shape()'::regprocedure")
            .fetch_one(&f.owner).await.unwrap();
        assert!(protected);
        let hidden_user=Uuid::new_v4();
        sqlx::query("UPDATE graphs SET kind='member',owner_user_id=$2 WHERE id=$1").bind(f.graph).bind(f.user).execute(&f.owner).await.unwrap();
        let visible_account=if tenant {f.account}else{
            sqlx::query("INSERT INTO users(id,email,clerk_user_id) VALUES($1,$2,$2)").bind(hidden_user).bind(format!("hidden-{hidden_user}@test.local")).execute(&f.owner).await.unwrap();
            sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES($1) RETURNING id").bind(hidden_user).fetch_one(&f.owner).await.unwrap()
        };
        let visible: Uuid=sqlx::query_scalar("INSERT INTO graphs(account_id,name,kind) VALUES($1,'visible target','org') RETURNING id")
            .bind(visible_account).fetch_one(&f.owner).await.unwrap();
        let mut tx=if tenant {
            coralswarm_backend::tenancy::scope::begin_tenant_scoped(&f.app,f.account,hidden_user).await.unwrap()
        }else{
            let mut tx=begin_account_scoped(&f.app,visible_account).await.unwrap();
            coralswarm_backend::tenancy::scope::set_tenant_user(&mut tx,hidden_user).await.unwrap();
            tx
        };
        let hidden: bool=sqlx::query_scalar("SELECT NOT EXISTS(SELECT 1 FROM corpus_enrichment_runs WHERE run_id=$1)").bind(run).fetch_one(&mut *tx).await.unwrap();
        assert!(hidden,"the parent must really be hidden by RLS");
        let error=sqlx::query("INSERT INTO corpus_enrichment_units(run_id,graph_id,kind,holding_id,extraction_version,state) VALUES($1,$2,'legacy_chunk',$3,'source-atoms-v2','pending')")
            .bind(run).bind(visible).bind(Uuid::new_v4()).execute(&mut *tx).await.unwrap_err();
        assert_eq!(error.as_database_error().unwrap().message(),"standing runs require retained version holdings");
        tx.rollback().await.unwrap();
        f.finish().await;
    }
}

#[tokio::test]
async fn standing_old_advance_rolls_back_inventory_owner_receipt_and_jobs_on_both_planes() {
    for tenant in [false,true] {
        for scenario in ["empty","ordinary","pre_floor_failed","pending_item_limit"] {
            let f=Fixture::new(tenant).await;
            if scenario=="empty" {
                sqlx::query("DELETE FROM source_items WHERE graph_id=$1").bind(f.graph).execute(&f.owner).await.unwrap();
                let versions:i64=sqlx::query_scalar("SELECT count(*) FROM source_versions WHERE graph_id=$1").bind(f.graph).fetch_one(&f.owner).await.unwrap();
                assert_eq!(versions,0,"the empty page must contain no historical fixture seed");
            }
            let version=if scenario=="empty" {None}else{Some(f.capture(scenario).await)};
            if scenario=="pre_floor_failed" {
                sqlx::query("UPDATE source_versions SET captured_at='2026-10-01T00:00:00Z',extraction_status='failed' WHERE version_id=$1").bind(version.unwrap()).execute(&f.owner).await.unwrap();
            }
            let run=f.run("source-atoms-v2").await;
            if scenario=="pending_item_limit" {
                sqlx::query("INSERT INTO corpus_enrichment_units(run_id,graph_id,kind,holding_id,extraction_version,state,reason) VALUES($1,$2,'retained_version',$3,'source-atoms-v2','pending','item_admission_limit')")
                    .bind(run).bind(f.graph).bind(version.unwrap()).execute(&f.owner).await.unwrap();
            }
            let before=standing_snapshot(&f).await;
            let mut tx=f.begin().await;
            let error=coralswarm_backend::corpus_enrichment::test_old_cohort_advance_tx(&mut tx,f.graph,run,100).await.unwrap_err();
            assert_eq!(error.as_database_error().unwrap().code().as_deref(),Some("23514"),"{scenario}");
            tx.rollback().await.unwrap();
            assert_eq!(before,standing_snapshot(&f).await,"{scenario}");
            for assignment in ["stage=4","status='complete'","attempted_characters=attempted_characters+1,attempted_model_calls=attempted_model_calls+1","status='paused',pause_reason='execution_budget_exhausted'","status='paused',pause_reason='cloud_processing_disabled'","status='paused',pause_reason='lifecycle_contract_not_ready'"] {
                let mut tx=f.begin().await;
                assert!(sqlx::query(&format!("UPDATE corpus_enrichment_runs SET {assignment} WHERE run_id=$1")).bind(run).execute(&mut *tx).await.is_err());
                tx.rollback().await.unwrap();
                assert_eq!(before,standing_snapshot(&f).await);
            }
            f.finish().await;
        }
    }
}

#[tokio::test]
async fn standing_pause_waits_for_share_lock_and_reloads_current_state_on_both_planes() {
    for tenant in [false,true] {
        let f=Fixture::new(tenant).await;
        let run=f.run("source-atoms-v2").await;
        let mut persister=f.begin().await;
        sqlx::query("SELECT run_id FROM corpus_enrichment_runs WHERE run_id=$1 FOR SHARE").bind(run).fetch_one(&mut *persister).await.unwrap();
        let mut pause=f.begin().await;
        let pid:i32=sqlx::query_scalar("SELECT pg_backend_pid()").fetch_one(&mut *pause).await.unwrap();
        let graph=f.graph;
        let waiter=tokio::spawn(async move {
            coralswarm_backend::corpus_enrichment::test_standing_pause_tx(&mut pause,graph,run,"operator_pause").await.unwrap();
            pause.commit().await.unwrap();
        });
        wait_for_lock(&f.owner,pid).await;
        assert_eq!(pause_tuple(&f,run).await.0,"none");
        persister.commit().await.unwrap();
        tokio::time::timeout(Duration::from_secs(5),waiter).await.unwrap().unwrap();
        assert_eq!(pause_tuple(&f,run).await.0,"operator");
        let mut policy=f.begin().await;
        coralswarm_backend::corpus_enrichment::test_standing_pause_tx(&mut policy,graph,run,"processing_off").await.unwrap();
        let mut resume=f.begin().await;
        let pid:i32=sqlx::query_scalar("SELECT pg_backend_pid()").fetch_one(&mut *resume).await.unwrap();
        let waiter=tokio::spawn(async move {
            coralswarm_backend::corpus_enrichment::test_standing_pause_tx(&mut resume,graph,run,"operator_resume").await.unwrap();
            resume.commit().await.unwrap();
        });
        wait_for_lock(&f.owner,pid).await;
        policy.commit().await.unwrap();
        tokio::time::timeout(Duration::from_secs(5),waiter).await.unwrap().unwrap();
        assert_eq!(pause_tuple(&f,run).await.0,"processing");
        let mut persister=f.begin().await;
        sqlx::query("SELECT run_id FROM corpus_enrichment_runs WHERE run_id=$1 FOR SHARE").bind(run).fetch_one(&mut *persister).await.unwrap();
        let mut old=f.begin().await;
        let pid:i32=sqlx::query_scalar("SELECT pg_backend_pid()").fetch_one(&mut *old).await.unwrap();
        let waiter=tokio::spawn(async move {
            let error=sqlx::query("UPDATE corpus_enrichment_runs SET status='running',pause_reason=NULL,updated_at=now() WHERE run_id=$1 AND status<>'complete'").bind(run).execute(&mut *old).await.unwrap_err();
            assert_eq!(error.as_database_error().unwrap().constraint(),Some("corpus_standing_pause_shape"));
            old.rollback().await.unwrap();
        });
        wait_for_lock(&f.owner,pid).await;
        persister.commit().await.unwrap();
        tokio::time::timeout(Duration::from_secs(5),waiter).await.unwrap().unwrap();
        assert_eq!(pause_tuple(&f,run).await.0,"processing");
        let mut pause=f.begin().await;
        coralswarm_backend::corpus_enrichment::test_standing_pause_tx(&mut pause,graph,run,"operator_pause").await.unwrap();
        let mut persister=f.begin().await;
        let pid:i32=sqlx::query_scalar("SELECT pg_backend_pid()").fetch_one(&mut *persister).await.unwrap();
        let waiter=tokio::spawn(async move {
            let status:String=sqlx::query_scalar("SELECT status FROM corpus_enrichment_runs WHERE run_id=$1 FOR SHARE")
                .bind(run).fetch_one(&mut *persister).await.unwrap();
            assert_eq!(status,"paused");
            persister.commit().await.unwrap();
        });
        wait_for_lock(&f.owner,pid).await;
        pause.commit().await.unwrap();
        tokio::time::timeout(Duration::from_secs(5),waiter).await.unwrap().unwrap();
        assert_eq!(pause_tuple(&f,run).await.0,"both");
        f.finish().await;
    }
}


struct NeverCalledModel(std::sync::atomic::AtomicUsize);
#[async_trait::async_trait]
impl coralswarm_backend::llm::LlmProvider for NeverCalledModel {
    async fn complete(&self, _: coralswarm_backend::llm::CompletionRequest) -> Result<coralswarm_backend::llm::CompletionResponse,coralswarm_backend::llm::LlmError> {
        self.0.fetch_add(1,std::sync::atomic::Ordering::SeqCst);
        panic!("controller must not dispatch a model");
    }
    async fn stream_complete(&self, _: coralswarm_backend::llm::CompletionRequest) -> Result<std::pin::Pin<Box<dyn futures::Stream<Item=Result<coralswarm_backend::llm::StreamEvent,coralswarm_backend::llm::LlmError>>+Send>>,coralswarm_backend::llm::LlmError> {
        self.0.fetch_add(1,std::sync::atomic::Ordering::SeqCst);
        panic!("controller must not dispatch a model stream");
    }
}

async fn controller_request(app: &axum::Router, method: &str, path: &str, body: Value) -> (axum::http::StatusCode,Value) {
    use tower::ServiceExt;
    let req=axum::http::Request::builder().method(method).uri(path).header("content-type","application/json")
        .body(axum::body::Body::from(body.to_string())).unwrap();
    let response=tokio::time::timeout(Duration::from_secs(5),app.clone().oneshot(req)).await.unwrap().unwrap();
    let code=response.status();
    let bytes=axum::body::to_bytes(response.into_body(),1024*1024).await.unwrap();
    (code,serde_json::from_slice(&bytes).unwrap())
}

#[tokio::test]
async fn standing_http_commands_refuse_before_cohort_fences_and_get_is_read_only() {
    use std::sync::{Arc,atomic::{AtomicUsize,Ordering}};
    use axum::http::StatusCode;
    use coralswarm_backend::{tenancy::runtime::{TenantContext,TenancyDecision},UserContext};
    for tenant in [false,true] {
        let f=Fixture::new(tenant).await;
        let separate_control=if tenant {Some(Fixture::new(false).await)}else{None};
        let control=separate_control.as_ref().unwrap_or(&f);
        let org=format!("org_cor22_{}",Uuid::new_v4().simple());
        if tenant {
            sqlx::query("INSERT INTO users(id,email,clerk_user_id) VALUES($1,$2,$2)").bind(f.user).bind(format!("http-{}@test.local",f.user)).execute(&control.owner).await.unwrap();
            sqlx::query("INSERT INTO accounts(id,owner_user_id,clerk_org_id) VALUES($1,$2,$3)").bind(f.account).bind(f.user).bind(&org).execute(&control.owner).await.unwrap();
            sqlx::query("INSERT INTO account_members(account_id,user_id,role,seat_tier_id) VALUES($1,$2,'owner',(SELECT id FROM seat_tiers WHERE code='standard' AND is_active))").bind(f.account).bind(f.user).execute(&control.owner).await.unwrap();
        }
        sqlx::query("UPDATE graphs SET kind='org' WHERE id=$1")
            .bind(f.graph)
            .execute(&f.owner)
            .await
            .unwrap();
        let actor = UserContext {
            account_id: f.account,
            user_id: f.user,
            role: "owner".into(),
            device_id: "cor22-controller".into(),
        };
        let request_control = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(control.app.connect_options().as_ref().clone())
            .await
            .unwrap();
        let mut state = coralswarm_backend::test_app_state_owned_with_db_and_store(
            request_control.clone(),
            None,
        );
        let model = Arc::new(NeverCalledModel(AtomicUsize::new(0)));
        state.llm = Some(model.clone());
        let decision = if tenant {
            TenancyDecision::Org(TenantContext {
                org_id: org.clone(),
                account_id: f.account,
                user_id: f.user,
                pool: f.app.clone(),
                page_pool: None,
            })
        } else {
            TenancyDecision::Personal
        };
        let app = coralswarm_backend::test_support_v1_app(Arc::new(state), actor)
            .layer(axum::Extension(decision));
        let version = f.capture("http-retained").await;
        let run = f.run("source-atoms-v2").await;
        let mut tx = f.begin().await;
        assert!(f.adopt(&mut tx, run, version).await);
        tx.commit().await.unwrap();
        let unit:Uuid=sqlx::query_scalar("SELECT unit_id FROM corpus_enrichment_units WHERE run_id=$1").bind(run).fetch_one(&f.owner).await.unwrap();
        let base=format!("/v1/sources/enrichment/runs/{run}");
        for lifecycle in [true,false] {
            if !lifecycle {
                sqlx::query("ALTER INDEX nodes_graph_type_lower_title_identity_unique RENAME TO cor22_controller_hidden_identity").execute(&f.owner).await.unwrap();
            }
            for state in ["none","operator","processing","both"] {
                set_pause(&f,run,state).await;
                let before=standing_snapshot(&f).await;
                let (code,body)=controller_request(&app,"GET",&format!("{base}?graph_id={}",f.graph),json!({})).await;
                assert_eq!(code,StatusCode::OK,"GET {state} lifecycle={lifecycle}: {body}");
                assert_eq!(body["standing_pause_state"],state);
                assert_eq!(before,standing_snapshot(&f).await);
                for (suffix,body) in [
                    ("advance",json!({"graph_id":f.graph})),
                    ("resume",json!({"graph_id":f.graph,"budget":{"items":1000,"characters":8000000,"model_calls":10000}})),
                    ("promote",json!({"graph_id":f.graph,"unit_id":unit})),
                    ("promote",json!({"graph_id":f.graph,"unit_id":Uuid::new_v4()})),
                ] {
                    let (code,body)=controller_request(&app,"POST",&format!("{base}/{suffix}"),body).await;
                    assert_eq!(code,StatusCode::UNPROCESSABLE_ENTITY,"{suffix} {state} lifecycle={lifecycle}: {body}");
                    assert_eq!(before,standing_snapshot(&f).await);
                }
                let (code,body)=controller_request(&app,"POST",&format!("{base}/pause"),json!({"graph_id":f.graph})).await;
                assert_eq!(code,StatusCode::OK,"pause {body}");
                assert_eq!(pause_tuple(&f,run).await.0,if matches!(state,"processing"|"both") {"both"}else{"operator"});
                let (code,body)=controller_request(&app,"POST",&format!("{base}/resume"),json!({"graph_id":f.graph})).await;
                assert_eq!(code,StatusCode::OK,"resume {body}");
                assert_eq!(pause_tuple(&f,run).await.0,if matches!(state,"processing"|"both") {"processing"}else{"none"});
            }
            if !lifecycle {
                sqlx::query("ALTER INDEX cor22_controller_hidden_identity RENAME TO nodes_graph_type_lower_title_identity_unique").execute(&f.owner).await.unwrap();
            }
        }
        sqlx::query("INSERT INTO account_prefs(account_id,prefs) VALUES($1,'{}') ON CONFLICT(account_id) DO UPDATE SET prefs='{}'").bind(f.account).execute(&control.owner).await.unwrap();
        let (code,body)=controller_request(&app,"POST","/v1/sources/enrichment/runs",json!({"graph_id":f.graph,"execute":true,"budget":{"items":2,"characters":1000,"model_calls":2}})).await;
        assert_eq!(code,StatusCode::CREATED,"cohort creation {body}");
        let cohort=body["run_id"].as_str().unwrap();
        let cohort_path=format!("/v1/sources/enrichment/runs/{cohort}");
        for suffix in ["pause","resume"] {
            let body=if suffix=="resume" {json!({"graph_id":f.graph,"budget":{"items":3,"characters":2000,"model_calls":3}})}else{json!({"graph_id":f.graph})};
            let (code,body)=controller_request(&app,"POST",&format!("{cohort_path}/{suffix}"),body).await;
            assert_eq!(code,StatusCode::OK,"cohort {suffix}: {body}");
        }
        let (code,body)=controller_request(&app,"POST",&format!("{cohort_path}/advance"),json!({"graph_id":f.graph})).await;
        assert_eq!(code,StatusCode::OK,"cohort advance {body}");
        let (code,_)=controller_request(&app,"POST",&format!("{cohort_path}/promote"),json!({"graph_id":f.graph,"unit_id":Uuid::new_v4()})).await;
        assert_eq!(code,StatusCode::CONFLICT);
        let peer=Uuid::new_v4();
        sqlx::query("INSERT INTO users(id,email,clerk_user_id) VALUES($1,$2,$2)").bind(peer).bind(format!("peer-{peer}@test.local")).execute(&control.owner).await.unwrap();
        sqlx::query("INSERT INTO account_members(account_id,user_id,role,seat_tier_id) VALUES($1,$2,'member',(SELECT CASE WHEN a.is_org_account THEN (SELECT id FROM seat_tiers WHERE code='standard' AND is_active) END FROM accounts a WHERE a.id=$1))").bind(f.account).bind(peer).execute(&control.owner).await.unwrap();
        let peer_actor = UserContext {
            account_id: f.account,
            user_id: peer,
            role: "member".into(),
            device_id: "cor22-peer".into(),
        };
        let peer_state = Arc::new(coralswarm_backend::test_app_state_owned_with_db_and_store(
            request_control.clone(),
            None,
        ));
        let peer_decision = if tenant {
            TenancyDecision::Org(TenantContext {
                org_id: org.clone(),
                account_id: f.account,
                user_id: peer,
                pool: f.app.clone(),
                page_pool: None,
            })
        } else {
            TenancyDecision::Personal
        };
        let peer_app = coralswarm_backend::test_support_v1_app(peer_state, peer_actor)
            .layer(axum::Extension(peer_decision));
        let before = standing_snapshot(&f).await;
        let (code, _) = controller_request(
            &peer_app,
            "GET",
            &format!("{base}?graph_id={}", f.graph),
            json!({}),
        )
        .await;
        assert_eq!(
            code,
            StatusCode::CONFLICT,
            "shared graph membership must not reveal private run history"
        );
        for (suffix, body) in [
            ("advance", json!({"graph_id":f.graph})),
            ("pause", json!({"graph_id":f.graph})),
            ("resume", json!({"graph_id":f.graph})),
            ("promote", json!({"graph_id":f.graph,"unit_id":unit})),
        ] {
            let (code, _) =
                controller_request(&peer_app, "POST", &format!("{base}/{suffix}"), body).await;
            assert_eq!(
                code,
                StatusCode::CONFLICT,
                "private run actor must be authorized before standing dispatch"
            );
            assert_eq!(before, standing_snapshot(&f).await);
        }
        drop(peer_app);
        sqlx::query("DELETE FROM account_members WHERE account_id=$1 AND user_id=$2").bind(f.account).bind(f.user).execute(&control.owner).await.unwrap();
        let before=standing_snapshot(&f).await;
        for (suffix,body) in [("advance",json!({"graph_id":f.graph})),("pause",json!({"graph_id":f.graph})),("resume",json!({"graph_id":f.graph})),("promote",json!({"graph_id":f.graph,"unit_id":unit}))] {
            let (code,_)=controller_request(&app,"POST",&format!("{base}/{suffix}"),body).await;
            assert_ne!(code,StatusCode::UNPROCESSABLE_ENTITY,"revoked actor must fail authorization before standing dispatch");
            assert!(!code.is_success());
            assert_eq!(before,standing_snapshot(&f).await);
        }
        assert_eq!(model.0.load(Ordering::SeqCst),0);
        drop(app);
        request_control.close().await;
        f.finish().await;
        if let Some(control)=separate_control {control.finish().await;}
    }
}

const PRIVACY_RETRY_SQL: &str = r#"WITH RECURSIVE work(id) AS (
              SELECT id FROM jobs WHERE id=$1 AND account_id=$2 UNION
              SELECT d.child_id FROM job_dependencies d JOIN work w ON d.parent_id=w.id)
              UPDATE jobs SET status='pending', attempts=0,error_message=NULL,finished_at=NULL,
                 run_after=now(),idempotency_key=NULL WHERE id IN(SELECT id FROM work)
                 AND account_id=$2 AND (status IN('failed','dead')
                     OR (status='pending' AND error_message IS NOT NULL)
                     OR (status='done' AND error_message IS NOT NULL))"#;

async fn privacy_job(f: &Fixture, kind: &str, status: &str, payload: &Value) -> Uuid {
    let mut tx=f.begin().await;
    let job=sqlx::query_scalar("INSERT INTO jobs(kind,payload,status,account_id,error_message,idempotency_key) VALUES($1,$2,$3::job_status,$4,'old error',gen_random_uuid()::text) RETURNING id")
        .bind(kind).bind(payload).bind(status).bind(f.account).fetch_one(&mut *tx).await.unwrap();
    tx.commit().await.unwrap(); job
}

async fn privacy_row(f: &Fixture, job: Uuid) -> Value {
    sqlx::query_scalar("SELECT to_jsonb(j) FROM jobs j WHERE id=$1").bind(job).fetch_one(&f.owner).await.unwrap()
}

async fn privacy_ledgers(f: &Fixture) -> Value {
    sqlx::query_scalar("SELECT jsonb_build_object('runs',(SELECT jsonb_agg(to_jsonb(r) ORDER BY run_id) FROM corpus_enrichment_runs r),'units',(SELECT jsonb_agg(to_jsonb(u) ORDER BY unit_id) FROM corpus_enrichment_units u),'receipts',(SELECT jsonb_agg(to_jsonb(a) ORDER BY source_version_id) FROM corpus_capture_admissions a))")
        .fetch_one(&f.owner).await.unwrap()
}

fn privacy_refusal(error: sqlx::Error) {
    let e=error.as_database_error().unwrap();
    assert_eq!(e.code().as_deref(),Some("23514"));
}

async fn privacy_assert_scrub(f: &Fixture, job: Uuid, kind: &str) {
    let row=privacy_row(f,job).await;
    assert_eq!(row["status"],json!("dead"));
    assert_eq!(row["payload"],json!({"graph_id":f.graph,"evidence_retracted":true}));
    assert_eq!(row["kind"],json!(kind));
    assert_eq!(row["account_id"],json!(f.account));
    assert_eq!(row["error_message"],Value::Null);
    assert_eq!(row["idempotency_key"],Value::Null);
    assert!(row["finished_at"].is_string());
}

#[tokio::test]
async fn privacy_actual_source_retraction_scrubs_all_extraction_routes_and_preserves_bystanders() {
    for tenant in [false,true] {
        let f=Fixture::new(tenant).await;
        let other_version=f.capture("privacy-other-source").await;
        let same_account_graph:Uuid=sqlx::query_scalar("INSERT INTO graphs(account_id,name) VALUES($1,'same account bystander') RETURNING id").bind(f.account).fetch_one(&f.owner).await.unwrap();
        let other_account=Uuid::new_v4();
        if !tenant {
            let other_user=Uuid::new_v4();
            sqlx::query("INSERT INTO users(id,email,clerk_user_id) VALUES($1,$2,$2)").bind(other_user).bind(format!("privacy-{other_user}@test.local")).execute(&f.owner).await.unwrap();
            sqlx::query("INSERT INTO accounts(id,owner_user_id) VALUES($1,$2)").bind(other_account).bind(other_user).execute(&f.owner).await.unwrap();
        }
        let other_graph:Uuid=sqlx::query_scalar("INSERT INTO graphs(account_id,name) VALUES($1,'other account bystander') RETURNING id").bind(other_account).fetch_one(&f.owner).await.unwrap();
        let mut cases=0;
        for kind in OLD_KINDS.iter().chain(STANDING_JOB_KINDS) {
            for (i,(gs,vs)) in [(0,0),(2,0),(4,0),(6,0),(0,2),(0,4),(0,6),(2,2),(4,4),(6,6)].into_iter().enumerate() {
                for status in ["pending","running","done","failed","dead"] {
                    let version=f.capture(&format!("privacy-{kind}-{i}-{status}")).await;
                    sqlx::query("DELETE FROM jobs WHERE payload->>'source_version_id'=$1").bind(version.to_string()).execute(&f.owner).await.unwrap();
                    let payload=json!({"graph_id":uuid_spellings(f.graph)[gs],"source_version_id":uuid_spellings(version)[vs],"start_char":777,"raw_blob_key":"private locator"});
                    assert!(serde_json::from_value::<JobKind>(json!({"kind":kind,"payload":payload})).is_ok());
                    let target=privacy_job(&f,kind,status,&payload).await;
                    let source:Uuid=sqlx::query_scalar("SELECT source_item_id FROM source_versions WHERE version_id=$1").bind(version).fetch_one(&f.owner).await.unwrap();
                    let mut bystanders=Vec::new();
                    for (g,v,k) in [(f.graph,other_version,*kind),(same_account_graph,version,*kind),(other_graph,version,*kind),(f.graph,version,"OceanHealth")] {
                        let id=privacy_job(&f,k,"pending",&json!({"graph_id":g,"source_version_id":v,"start_char":0})).await;
                        bystanders.push((id,privacy_row(&f,id).await));
                    }
                    let ledgers=privacy_ledgers(&f).await;
                    let mut tx=f.begin().await;
                    match i % 4 {
                        0 => {sqlx::query("UPDATE source_items SET access_state='restricted' WHERE source_item_id=(SELECT source_item_id FROM source_versions WHERE version_id=$1)").bind(version).execute(&mut *tx).await.unwrap();}
                        1 => {sqlx::query("UPDATE source_items SET access_state='unknown' WHERE source_item_id=(SELECT source_item_id FROM source_versions WHERE version_id=$1)").bind(version).execute(&mut *tx).await.unwrap();}
                        2 => {sqlx::query("DELETE FROM source_items WHERE source_item_id=(SELECT source_item_id FROM source_versions WHERE version_id=$1)").bind(version).execute(&mut *tx).await.unwrap();}
                        _ => {sqlx::query("UPDATE source_items SET access_state='restricted' WHERE source_item_id=$1").bind(source).execute(&mut *tx).await.unwrap();}
                    }
                    tx.commit().await.unwrap();
                    privacy_assert_scrub(&f,target,kind).await;
                    let event:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM source_purge_events WHERE graph_id=$1 AND source_item_id=$2 AND account_id=$3 AND event_kind=$4)").bind(f.graph).bind(source).bind(f.account).bind(match i % 4 {1=>"unknown",2=>"deleted",_=>"restricted"}).fetch_one(&f.owner).await.unwrap();assert!(event);
                    assert_eq!(privacy_ledgers(&f).await,ledgers);
                    for (id,before) in &bystanders { assert_eq!(&privacy_row(&f,*id).await,before); }
                    sqlx::query("DELETE FROM jobs WHERE id=ANY($1)").bind(bystanders.iter().map(|(id,_)|*id).collect::<Vec<_>>()).execute(&f.owner).await.unwrap();
                    cases+=1;
                }
            }
        }
        println!("PRIVACY_SOURCE tenant={tenant} cases={cases} four_nonempty_bystanders_per_case=true durable_ledgers_unchanged=true");
        f.finish().await;
    }
}

#[tokio::test]
async fn privacy_exact_terminal_transition_is_irreversible_and_source_lock_free() {
    for tenant in [false,true] {
        let f=Fixture::new(tenant).await;
        for kind in OLD_KINDS.iter().chain(STANDING_JOB_KINDS) {
            let version=f.capture(&format!("privacy-immutable-{kind}")).await;
            let payload=json!({"graph_id":uuid_spellings(f.graph)[6],"source_version_id":version,"start_char":7});
            let job=privacy_job(&f,kind,"running",&payload).await;
            let scrub=json!({"graph_id":f.graph,"evidence_retracted":true});
            let before=privacy_row(&f,job).await;
            let mut invalid=vec![json!({}),json!({"graph_id":Uuid::new_v4(),"evidence_retracted":true}),json!({"graph_id":f.graph,"evidence_retracted":"true"}),json!({"graph_id":f.graph,"evidence_retracted":false}),json!({"graph_id":f.graph,"evidence_retracted":null}),json!({"graph_id":7,"evidence_retracted":true}),json!({"graph_id":"bad","evidence_retracted":true}),json!({"evidence_retracted":true})];
            let mut extra=scrub.clone();extra["extra"]=json!(1);invalid.push(extra);
            let mut retained=scrub.clone();retained["source_version_id"]=json!(version);invalid.push(retained);
            for bad in invalid {
                privacy_refusal(sqlx::query("UPDATE jobs SET status='dead',payload=$2 WHERE id=$1").bind(job).bind(bad).execute(&f.app).await.unwrap_err());
                assert_eq!(privacy_row(&f,job).await,before);
            }
            for sql in ["UPDATE jobs SET status='pending',payload=$2 WHERE id=$1","UPDATE jobs SET status='dead',payload=$2,account_id=gen_random_uuid() WHERE id=$1","UPDATE jobs SET status='dead',payload=$2,kind='OceanHealth' WHERE id=$1"] {
                privacy_refusal(sqlx::query(sql).bind(job).bind(&scrub).execute(&f.app).await.unwrap_err());
                assert_eq!(privacy_row(&f,job).await,before);
            }
            let mut lock=f.owner.begin().await.unwrap();
            sqlx::query("SELECT version_id FROM source_versions WHERE version_id=$1 FOR UPDATE").bind(version).execute(&mut *lock).await.unwrap();
            sqlx::query("SELECT source_item_id FROM source_items WHERE source_item_id=(SELECT source_item_id FROM source_versions WHERE version_id=$1) FOR UPDATE").bind(version).execute(&mut *lock).await.unwrap();
            tokio::time::timeout(Duration::from_secs(2),sqlx::query("UPDATE jobs SET status='dead',payload=$2 WHERE id=$1").bind(job).bind(&scrub).execute(&f.app)).await.expect("scrub must not lock source").unwrap();
            lock.rollback().await.unwrap();
            let frozen=privacy_row(&f,job).await;
            for sql in ["UPDATE jobs SET status='pending' WHERE id=$1","UPDATE jobs SET status='running' WHERE id=$1","UPDATE jobs SET payload=payload-'evidence_retracted' WHERE id=$1","UPDATE jobs SET payload=jsonb_set(payload,'{source_version_id}',to_jsonb(gen_random_uuid())) WHERE id=$1","UPDATE jobs SET account_id=gen_random_uuid() WHERE id=$1","UPDATE jobs SET kind='OceanHealth' WHERE id=$1","UPDATE jobs SET status='pending',attempts=0,error_message=NULL WHERE id=$1"] {
                privacy_refusal(sqlx::query(sql).bind(job).execute(&f.app).await.unwrap_err());
                assert_eq!(privacy_row(&f,job).await,frozen);
            }
            privacy_refusal(sqlx::query(PRIVACY_RETRY_SQL).bind(job).bind(f.account).execute(&f.app).await.unwrap_err());
            assert_eq!(privacy_row(&f,job).await,frozen);
            privacy_refusal(queue::complete(&f.app,job,queue::JobOutcome::NothingToDo).await.unwrap_err());
            assert_eq!(privacy_row(&f,job).await,frozen);
            privacy_refusal(queue::fail(&f.app,job,"late failure").await.unwrap_err());
            assert_eq!(privacy_row(&f,job).await,frozen);
            sqlx::query("UPDATE jobs SET status='dead',payload=$2,attempts=attempts+1 WHERE id=$1").bind(job).bind(&scrub).execute(&f.app).await.unwrap();
            assert_eq!(privacy_row(&f,job).await["payload"],scrub);
            let missing=privacy_job(&f,kind,"pending",&json!({"graph_id":f.graph,"source_version_id":Uuid::new_v4(),"start_char":0})).await;
            sqlx::query("UPDATE jobs SET status='dead',payload=$2 WHERE id=$1").bind(missing).bind(&scrub).execute(&f.app).await.unwrap();
            let historical=privacy_job(&f,kind,"dead",&json!({"evidence_retracted":true,"old_extra":"retained"})).await;
            let historical_before=privacy_row(&f,historical).await;
            privacy_refusal(sqlx::query("UPDATE jobs SET payload='{}',status='pending' WHERE id=$1").bind(historical).execute(&f.app).await.unwrap_err());
            assert_eq!(privacy_row(&f,historical).await,historical_before);
            let malformed=privacy_job(&f,kind,"running",&json!({"graph_id":"malformed","source_version_id":version,"start_char":0})).await;
            let malformed_before=privacy_row(&f,malformed).await;
            privacy_refusal(sqlx::query("UPDATE jobs SET status='dead',payload=$2 WHERE id=$1").bind(malformed).bind(&scrub).execute(&f.app).await.unwrap_err());
            assert_eq!(privacy_row(&f,malformed).await,malformed_before);
            let null_account:Uuid=sqlx::query_scalar("INSERT INTO jobs(kind,payload,status) VALUES($1,$2,'pending') RETURNING id").bind(kind).bind(json!({"graph_id":f.graph,"source_version_id":Uuid::new_v4(),"start_char":0})).fetch_one(&f.app).await.unwrap();
            sqlx::query("UPDATE jobs SET status='dead',payload=$2 WHERE id=$1").bind(null_account).bind(&scrub).execute(&f.app).await.unwrap();
            let null_frozen=privacy_row(&f,null_account).await;
            assert_eq!(null_frozen["account_id"],Value::Null);
            privacy_refusal(sqlx::query("UPDATE jobs SET account_id=$2 WHERE id=$1").bind(null_account).bind(f.account).execute(&f.app).await.unwrap_err());
            assert_eq!(privacy_row(&f,null_account).await,null_frozen);
            let ordinary=privacy_job(&f,kind,"dead",&json!({"graph_id":f.graph,"evidence_retracted":"true"})).await;
            sqlx::query("UPDATE jobs SET status='pending' WHERE id=$1").bind(ordinary).execute(&f.app).await.unwrap();
        }
        println!("PRIVACY_TERMINAL tenant={tenant} kinds=4 exact_scrub=true late_complete_refused=true late_fail_refused=true source_locks_not_taken=true");
        f.finish().await;
    }
}

#[tokio::test]
async fn privacy_per_atom_mixed_support_and_running_controls_remain_unchanged() {
    for tenant in [false,true] {
        let f=Fixture::new(tenant).await;
        let mut atoms=Vec::new();
        for name in ["retiring","surviving"] {
            let atom:Uuid=sqlx::query_scalar("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,timestamp,ingested_by) VALUES($1,'note',$2,$1::text||'/'||gen_random_uuid()::text,now(),$3) RETURNING atom_id").bind(f.graph).bind(name).bind(f.user).fetch_one(&f.owner).await.unwrap();atoms.push(atom);
        }
        let mut expected=Vec::new();
        for kind in OLD_KINDS.iter().chain(STANDING_JOB_KINDS) {
            for (spelling,status) in [(0,"pending"),(2,"pending"),(0,"running")] {
                let payload=json!({"graph_id":uuid_spellings(f.graph)[spelling],"source_version_id":Uuid::new_v4(),"start_char":0,"atom_ids":atoms});
                let id=privacy_job(&f,kind,status,&payload).await;
                let mut after=privacy_row(&f,id).await;
                if spelling==0 && status=="pending" {after["payload"]["atom_ids"]=json!([atoms[1]]);}
                expected.push((id,after));
            }
        }
        let mut tx=f.begin().await;
        sqlx::query("DELETE FROM atoms WHERE atom_id=$1").bind(atoms[0]).execute(&mut *tx).await.unwrap();
        tx.commit().await.unwrap();
        for (id,after) in expected {assert_eq!(privacy_row(&f,id).await,after);}
        let mut graph_jobs=Vec::new();
        for kind in OLD_KINDS.iter().chain(STANDING_JOB_KINDS) {
            let id=privacy_job(&f,kind,"running",&json!({"graph_id":uuid_spellings(f.graph)[6],"source_version_id":Uuid::new_v4(),"start_char":9})).await;
            graph_jobs.push((id,*kind));
        }
        sqlx::query("DELETE FROM graphs WHERE id=$1").bind(f.graph).execute(&f.owner).await.unwrap();
        for (id,kind) in graph_jobs {privacy_assert_scrub(&f,id,kind).await;}
        println!("PRIVACY_PER_ATOM tenant={tenant} canonical_mixed_trim=4 alternate_mixed_unchanged=4 running_unchanged=4 actual_graph_delete_scrubs_four_urn_kinds=true");
        f.finish().await;
    }
}

struct PrivacyNoProvider(std::sync::atomic::AtomicUsize);
#[async_trait::async_trait]
impl coralswarm_backend::llm::LlmProvider for PrivacyNoProvider {
    async fn complete(&self,_:coralswarm_backend::llm::CompletionRequest)->Result<coralswarm_backend::llm::CompletionResponse,coralswarm_backend::llm::LlmError> {
        self.0.fetch_add(1,std::sync::atomic::Ordering::SeqCst); panic!("privacy retraction must not dispatch a provider")
    }
    async fn stream_complete(&self,_:coralswarm_backend::llm::CompletionRequest)->Result<std::pin::Pin<Box<dyn futures::Stream<Item=Result<coralswarm_backend::llm::StreamEvent,coralswarm_backend::llm::LlmError>>+Send>>,coralswarm_backend::llm::LlmError> {
        self.0.fetch_add(1,std::sync::atomic::Ordering::SeqCst); panic!("privacy retraction must not stream a provider")
    }
}

#[tokio::test]
async fn privacy_real_missing_source_worker_preserves_history_and_cannot_complete_tombstone() {
    use coralswarm_backend::{store::PostgresStore,tenancy::TenantId,workers::source_atoms};
    for tenant in [false,true] {
        let f=Fixture::new(tenant).await;
        let version=f.capture("privacy-worker-delete").await;
        let mut tx=f.begin().await;
        let run:Uuid=sqlx::query_scalar("INSERT INTO corpus_enrichment_runs(graph_id,account_id,requested_by,mode,corpus_kind,status,extraction_version,model,max_items,max_characters,max_model_calls) VALUES($1,$2,$3,'execute','real','running','source-atoms-v2','fixture/pinned',1000,8000000,10000) RETURNING run_id").bind(f.graph).bind(f.account).bind(f.user).fetch_one(&mut *tx).await.unwrap();
        let unit:Uuid=sqlx::query_scalar("INSERT INTO corpus_enrichment_units(run_id,graph_id,kind,holding_id,source_item_id,extraction_version,state,execution_owner) SELECT $1,graph_id,'retained_version',version_id,source_item_id,extraction_version,'queued',true FROM source_versions WHERE version_id=$2 RETURNING unit_id").bind(run).bind(version).fetch_one(&mut *tx).await.unwrap();
        sqlx::query("UPDATE corpus_capture_admissions SET run_id=$2,unit_id=$3 WHERE source_version_id=$1").bind(version).bind(run).bind(unit).execute(&mut *tx).await.unwrap();
        f.enqueue_old(&mut tx,version,0).await;
        let (id,kind,payload):(Uuid,String,Value)=sqlx::query_as("UPDATE jobs SET status='running' WHERE payload->>'source_version_id'=$1 RETURNING id,kind,payload").bind(version.to_string()).fetch_one(&mut *tx).await.unwrap();
        tx.commit().await.unwrap();
        let before=privacy_ledgers(&f).await;
        let source:Uuid=sqlx::query_scalar("SELECT source_item_id FROM source_versions WHERE version_id=$1").bind(version).fetch_one(&f.owner).await.unwrap();
        let mut tx=f.begin().await;
        sqlx::query("DELETE FROM source_items WHERE source_item_id=$1").bind(source).execute(&mut *tx).await.unwrap();
        tx.commit().await.unwrap();
        assert_eq!(privacy_ledgers(&f).await,before);
        let frozen=privacy_row(&f,id).await;
        let spy=PrivacyNoProvider(std::sync::atomic::AtomicUsize::new(0));
        let tenant_id=if tenant {TenantId::Org("privacy-local".into())} else {TenantId::Consumer};
        source_atoms::process_job(&tenant_id,&f.app,&f.owner,&PostgresStore::new(f.app.clone()),&spy,None,queue::JobRow{id,kind,payload,attempts:0,account_id:Some(f.account)}).await;
        assert_eq!(spy.0.load(std::sync::atomic::Ordering::SeqCst),0);
        assert_eq!(privacy_row(&f,id).await,frozen);
        let unit_state:(String,String)=sqlx::query_as("SELECT state,reason FROM corpus_enrichment_units WHERE unit_id=$1").bind(unit).fetch_one(&f.owner).await.unwrap();
        assert_eq!(unit_state,("unavailable".into(),"source_removed_or_access_lost".into()));
        let receipt:(String,String,Uuid,Uuid)=sqlx::query_as("SELECT state,reason,run_id,unit_id FROM corpus_capture_admissions WHERE source_version_id=$1").bind(version).fetch_one(&f.owner).await.unwrap();
        assert_eq!(receipt,("unavailable".into(),"source_removed_or_access_lost".into(),run,unit));
        let mut after=privacy_ledgers(&f).await;
        let mut expected=before;
        for value in [&mut expected,&mut after] {
            for table in ["units","receipts"] {
                for row in value[table].as_array_mut().unwrap() {
                    for field in ["updated_at","state","reason"] {row.as_object_mut().unwrap().remove(field);}
                }
            }
        }
        assert_eq!(after,expected);
        let mut tx=f.owner.begin().await.unwrap();
        sqlx::query("SELECT set_config('app.account_id',$1,true)").bind(f.account.to_string()).execute(&mut *tx).await.unwrap();
        sqlx::query("SELECT library_capture_purge($1,$2,'deleted')").bind(f.graph).bind(source).execute(&mut *tx).await.unwrap();
        sqlx::query("SELECT library_capture_purge($1,$2,'graph_deleted')").bind(f.graph).bind(Uuid::nil()).execute(&mut *tx).await.unwrap();
        tx.commit().await.unwrap();
        let repeated=privacy_row(&f,id).await;
        for field in ["status","payload","kind","account_id"] {assert_eq!(repeated[field],frozen[field]);}
        sqlx::query("DELETE FROM graphs WHERE id=$1").bind(f.graph).execute(&f.owner).await.unwrap();
        let remaining:i64=sqlx::query_scalar("SELECT (SELECT count(*) FROM corpus_enrichment_runs WHERE graph_id=$1)+(SELECT count(*) FROM corpus_enrichment_units WHERE graph_id=$1)+(SELECT count(*) FROM corpus_capture_admissions WHERE graph_id=$1)").bind(f.graph).fetch_one(&f.owner).await.unwrap();
        assert_eq!(remaining,0);
        println!("PRIVACY_WORKER tenant={tenant} actual_source_delete=true provider_calls=0 ledger_commit=true completion_refused_tombstone_unchanged=true repeated_source_and_graph_purge=true graph_cascades=true");
        f.finish().await;
    }
}

#[tokio::test]
async fn privacy_standing_ownership_and_accounting_survive_real_source_delete() {
    for tenant in [false,true] {
        let f=Fixture::new(tenant).await;
        for profile in ["source-atoms-v2","meeting-atoms-v1"] {
            let version=f.capture(&format!("privacy-standing-{profile}")).await;
            sqlx::query("DELETE FROM jobs WHERE payload->>'source_version_id'=$1").bind(version.to_string()).execute(&f.owner).await.unwrap();
            sqlx::query("UPDATE source_versions SET extraction_version=$2 WHERE version_id=$1").bind(version).bind(profile).execute(&f.owner).await.unwrap();
            let run=f.run(profile).await;
            let mut tx=f.begin().await;
            assert!(f.adopt(&mut tx,run,version).await);
            tx.commit().await.unwrap();
            let job:Uuid=sqlx::query_scalar("SELECT id FROM jobs WHERE payload->>'source_version_id'=$1 AND status='pending'").bind(version.to_string()).fetch_one(&f.owner).await.unwrap();
            let kind:String=sqlx::query_scalar("SELECT kind FROM jobs WHERE id=$1").bind(job).fetch_one(&f.owner).await.unwrap();
            let before=privacy_ledgers(&f).await;
            let mut tx=f.begin().await;
            sqlx::query("DELETE FROM source_items WHERE source_item_id=(SELECT source_item_id FROM source_versions WHERE version_id=$1)").bind(version).execute(&mut *tx).await.unwrap();
            tx.commit().await.unwrap();
            privacy_assert_scrub(&f,job,&kind).await;
            assert_eq!(privacy_ledgers(&f).await,before);
            let owned:bool=sqlx::query_scalar("SELECT execution_owner FROM corpus_enrichment_units WHERE run_id=$1 AND holding_id=$2").bind(run).bind(version).fetch_one(&f.owner).await.unwrap();assert!(owned);
        }
        println!("PRIVACY_STANDING_HISTORY tenant={tenant} profiles=2 full_runs_units_receipts_retained=true");
        f.finish().await;
    }
}

#[tokio::test]
async fn integrated_source_delete_preserves_standing_history_in_every_pause_state() {
    use coralswarm_backend::corpus_enrichment::test_standing_pause_tx;
    for tenant in [false, true] {
        for profile in ["source-atoms-v2", "meeting-atoms-v1"] {
            for state in ["none", "operator", "processing", "both"] {
                let f = Fixture::new(tenant).await;
                let version = f.capture(&format!("integrated-delete-{profile}-{state}")).await;
                sqlx::query("DELETE FROM jobs WHERE payload->>'source_version_id'=$1")
                    .bind(version.to_string()).execute(&f.owner).await.unwrap();
                sqlx::query("UPDATE source_versions SET extraction_version=$2 WHERE version_id=$1")
                    .bind(version).bind(profile).execute(&f.owner).await.unwrap();
                let run = f.run(profile).await;
                let mut tx = f.begin().await;
                assert!(f.adopt(&mut tx, run, version).await);
                tx.commit().await.unwrap();
                let actions: &[&str] = match state {
                    "none" => &[],
                    "operator" => &["operator_pause"],
                    "processing" => &["processing_off"],
                    "both" => &["operator_pause", "processing_off"],
                    _ => unreachable!(),
                };
                for action in actions {
                    let mut tx = f.begin().await;
                    test_standing_pause_tx(&mut tx, f.graph, run, action).await.unwrap();
                    tx.commit().await.unwrap();
                }
                let tuple = pause_tuple(&f, run).await;
                assert_eq!(tuple.0, state);
                let (job, kind): (Uuid, String) = sqlx::query_as("SELECT id,kind FROM jobs WHERE payload->>'source_version_id'=$1 AND status='pending'")
                    .bind(version.to_string()).fetch_one(&f.owner).await.unwrap();
                let owned: bool = sqlx::query_scalar("SELECT execution_owner FROM corpus_enrichment_units WHERE run_id=$1 AND holding_id=$2")
                    .bind(run).bind(version).fetch_one(&f.owner).await.unwrap();
                assert!(owned);
                let before = privacy_ledgers(&f).await;
                let mut tx = f.begin().await;
                let deleted = sqlx::query("DELETE FROM source_items WHERE source_item_id=(SELECT source_item_id FROM source_versions WHERE version_id=$1)")
                    .bind(version).execute(&mut *tx).await.unwrap();
                assert_eq!(deleted.rows_affected(), 1);
                tx.commit().await.unwrap();
                privacy_assert_scrub(&f, job, &kind).await;
                assert_eq!(privacy_ledgers(&f).await, before);
                assert_eq!(pause_tuple(&f, run).await, tuple);
                let owned: bool = sqlx::query_scalar("SELECT execution_owner FROM corpus_enrichment_units WHERE run_id=$1 AND holding_id=$2")
                    .bind(run).bind(version).fetch_one(&f.owner).await.unwrap();
                assert!(owned);
                println!("INTEGRATED_PRIVACY tenant={tenant} profile={profile} pause={state} source_rows_deleted=1 exact_scrub=true durable_ledgers_unchanged=true execution_owner=true worker_invoked=false");
                f.finish().await;
            }
        }
    }
}
