//! Current Org atom-title contracts under a real non-exempt tenant app login.
//!
//! BackfillAtomTitles runs on `managed_enrichment` (source-agnostic plan PR 7):
//! typed queue admission keeps the job pending, and while the shared switch
//! (`ENRICHMENT_PROCESSING_ENABLED`) is off the worker claims nothing, and a
//! direct invocation hands the same cursor back before any private candidate
//! read, provider work or title write. The managed path itself is proven in
//! `managed::gate::pg_tests::enrichment_consumers`.
//!
//! Independently preserve the useful historical maintenance proof: the exact real
//! candidate query denies an ambient member-private ocean, admits it under the
//! transaction-local system actor, and the existing model-free batch primitive
//! derives the fallback title or terminally clears an invalid Org blob key. Only
//! the narrow test-support wrapper reaches that dormant primitive, with LLM/R2
//! hardcoded None; production process_job/tick/admission remain guarded.
//!
//! No TenantRegistry bypass claim: the test directly authenticates the actual
//! coralswarm_tenant_app role, never an owner/BYPASSRLS role. Provider spies are
//! inert and must stay unused; tenant budget/attempt/usage snapshots stay exact.
//! CONTROL credits are not seeded, so this is not a funded-dispatch proof.
//! Gated on ORGT_TEST_BASE_URL; actual compiled whole-target execution required.

mod support;

#[path = "support/retained_database.rs"]
mod retained_database;

use std::str::FromStr;

use coralswarm_backend::jobs::{queue::{self, JobRow}, JobKind};
use coralswarm_backend::llm::{CompletionRequest, CompletionResponse, LlmError, LlmProvider, StreamEvent};
use futures::Stream;
use serde_json::{json, Value};
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use coralswarm_backend::tenancy::scope::begin_tenant_system_scoped_without_account;
use coralswarm_backend::tenancy::TenantId;
use coralswarm_backend::workers::atom_title_backfill::{
    process_job, test_support_candidate_atom_ids, test_support_model_free_org_batch,
};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{Connection, PgConnection, PgPool, Row};
use support::tenant_role::{set_app_role_password_with_retry, APP_ROLE_TEST_PASSWORD};
use uuid::Uuid;

const SKIP_REASON: &str =
    "skipping: set ORGT_TEST_BASE_URL (a base Postgres with CREATE DATABASE + CREATE/ALTER ROLE \
     rights) to run the atom-title-backfill tenant-RLS negative control";

fn base_url() -> Option<String> {
    std::env::var("ORGT_TEST_BASE_URL").ok()
}

fn with_db(base: &str, db: &str) -> String {
    let (prefix, rest) = base.split_at(base.rfind('/').expect("url has a path /"));
    let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
    format!("{prefix}/{db}{params}")
}

/// Same base URL, authenticated as `coralswarm_tenant_app` — the literal DSN
/// shape `TenantRegistry` would resolve in production. Mirrors
/// `tenant_rls_phase2_pg.rs::as_app_role_dsn` exactly.
fn as_app_role_dsn(db_url: &str) -> String {
    let opts = PgConnectOptions::from_str(db_url)
        .expect("parse scratch db url")
        .username("coralswarm_tenant_app")
        .password(APP_ROLE_TEST_PASSWORD);
    let host = opts.get_host();
    let port = opts.get_port();
    let db = db_url.rsplit('/').next().unwrap_or("postgres");
    format!("postgres://coralswarm_tenant_app:{APP_ROLE_TEST_PASSWORD}@{host}:{port}/{db}")
}

#[derive(Default)]
struct RefusalSpy(AtomicUsize);
#[async_trait::async_trait]
impl LlmProvider for RefusalSpy {
    async fn complete(&self, _: CompletionRequest) -> Result<CompletionResponse, LlmError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        panic!("unsupported Org title job must not call a provider");
    }
    async fn stream_complete(
        &self,
        _: CompletionRequest,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<StreamEvent, LlmError>> + Send>>, LlmError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        panic!("unsupported Org title job must not stream");
    }
}

async fn assert_app_role(pool: &PgPool) {
    let role: (String, bool, bool, i64, i64) = sqlx::query_as(
        "SELECT current_user::text,rolsuper,rolbypassrls, \
         (SELECT count(*) FROM pg_tables WHERE schemaname='public' AND tableowner=current_user), \
         (SELECT count(*) FROM pg_auth_members WHERE member=(SELECT oid FROM pg_roles WHERE rolname=current_user)) \
         FROM pg_roles WHERE rolname=current_user",
    ).fetch_one(pool).await.expect("inspect actual app login");
    assert_eq!(role, ("coralswarm_tenant_app".into(), false, false, 0, 0));
}

async fn atoms_snapshot(owner: &PgPool) -> Value {
    sqlx::query_scalar("SELECT COALESCE(jsonb_agg(to_jsonb(a) ORDER BY atom_id),'[]'::jsonb) FROM atoms a")
        .fetch_one(owner).await.expect("snapshot all tenant atoms")
}
async fn accounting_snapshot(owner: &PgPool) -> Value {
    sqlx::query_scalar(
        "SELECT jsonb_build_object( \
         'budgets',(SELECT COALESCE(jsonb_agg(to_jsonb(b) ORDER BY account_id,purpose,period_start),'[]'::jsonb) FROM managed_budget_periods b), \
         'attempts',(SELECT COALESCE(jsonb_agg(to_jsonb(a) ORDER BY receipt_id),'[]'::jsonb) FROM managed_attempt_receipts a), \
         'usage',(SELECT count(*) FROM llm_usage), \
         'pending_usage',(SELECT count(*) FROM llm_usage_pending), \
         'embedding_inputs',(SELECT count(*) FROM managed_embedding_inputs))",
    ).fetch_one(owner).await.expect("snapshot actual tenant accounting tables")
}
async fn jobs_snapshot(owner: &PgPool) -> Value {
    sqlx::query_scalar("SELECT COALESCE(jsonb_agg(to_jsonb(j) ORDER BY id),'[]'::jsonb) FROM jobs j")
        .fetch_one(owner).await.expect("snapshot durable jobs")
}
async fn enqueue_refused_job(tenant: &TenantId, app: &PgPool, owner: &PgPool, account: Uuid) -> Uuid {
    std::env::remove_var("ENRICHMENT_PROCESSING_ENABLED");
    let key = format!("org-title-refusal:{}", Uuid::new_v4());
    let id = queue::enqueue(app, &JobKind::BackfillAtomTitles { after_atom_id: None }, Some(account), Some(&key), 0)
        .await.expect("typed real queue insertion").expect("new receipt");
    let row: (String, Option<String>, Option<String>, i32, bool, Option<String>, Option<Uuid>, Value, String, Option<String>) =
        sqlx::query_as("SELECT status::text,error_message,failure_class,attempts,finished_at IS NOT NULL,outcome::text,account_id,payload,kind,idempotency_key FROM jobs WHERE id=$1")
            .bind(id).fetch_one(owner).await.expect("read genuine admission receipt");
    assert_eq!(row, ("pending".into(), None, None, 0, false, None, Some(account), json!({"after_atom_id":null}), "BackfillAtomTitles".into(), Some(key)),
        "managed enrichment: admitted pending, not dead-lettered");
    let worker = coralswarm_backend::workers::tenant_harness::WorkerTenant {
        tenant: tenant.clone(), pool: app.clone(), store: std::sync::Arc::new(coralswarm_backend::store::PostgresStore::new(app.clone())),
    };
    let tick = coralswarm_backend::workers::atom_title_backfill::tick(&worker, app, None, None, &coralswarm_backend::workers::atom_title_backfill::SeedClock::new())
        .await.expect("switched-off tick");
    assert_eq!(tick.processed, 0, "switched off: nothing claimed");
    let still: String = sqlx::query_scalar("SELECT status::text FROM jobs WHERE id=$1").bind(id).fetch_one(owner).await.unwrap();
    assert_eq!(still, "pending");
    id
}
async fn assert_worker_refuses(
    tenant: &TenantId, app: &PgPool, owner: &PgPool, account: Uuid,
    id: Uuid, spy: &RefusalSpy,
) {
    let before_atoms = atoms_snapshot(owner).await;
    let before_money = accounting_snapshot(owner).await;
    let before_jobs = jobs_snapshot(owner).await;
    assert_eq!(before_jobs.as_array().expect("job array").len(), 1);
    assert_app_role(app).await;
    let can_read: bool = sqlx::query_scalar("SELECT has_table_privilege(current_user,'atoms','SELECT')")
        .fetch_one(app).await.expect("initial read privilege");
    assert!(can_read);
    // A decreasing privilege probe, not an RLS bypass. Restore before the
    // separate maintenance proof. Refusal must not require private read authority.
    sqlx::query("REVOKE SELECT ON atoms FROM coralswarm_tenant_app").execute(owner).await.expect("deny content reads during refusal");
    let can_read: bool = sqlx::query_scalar("SELECT has_table_privilege(current_user,'atoms','SELECT')")
        .fetch_one(app).await.expect("read privilege after revoke");
    assert!(!can_read);
    let actual: (String, Value, Option<Uuid>, i32) = sqlx::query_as("SELECT kind,payload,account_id,attempts FROM jobs WHERE id=$1")
        .bind(id).fetch_one(owner).await.expect("read real job arguments");
    assert_eq!(actual.2, Some(account));
    // Claimed, as `queue::dequeue` leaves it: a hold acts on a running row.
    sqlx::query("UPDATE jobs SET status='running',started_at=now() WHERE id=$1")
        .bind(id).execute(owner).await.expect("claim the job");
    process_job(tenant, app, app, Some(spy), None, JobRow { id, kind: actual.0, payload: actual.1, account_id: actual.2, attempts: actual.3 }).await;
    let handed_back: (String, Option<String>) =
        sqlx::query_as("SELECT status::text,error_message FROM jobs WHERE id=$1")
            .bind(id).fetch_one(owner).await.expect("read the direct worker hand-back");
    assert_eq!(handed_back, ("pending".into(), Some("held: switched_off".into())),
        "switched off: held in place under the shared retry rule, never dead-lettered");
    assert_eq!(atoms_snapshot(owner).await, before_atoms, "refused work must change no title/body/key/provenance");
    assert_eq!(accounting_snapshot(owner).await, before_money, "no attempt/budget/usage/embedding effects");
    assert_eq!(spy.0.load(Ordering::SeqCst), 0);
    let resumed: Vec<(Option<Uuid>, Value)> = sqlx::query_as(
        "SELECT account_id,payload FROM jobs WHERE kind='BackfillAtomTitles' AND status='pending'",
    ).fetch_all(owner).await.expect("read continuation");
    assert_eq!(resumed, vec![(Some(account), json!({"after_atom_id":null}))], "only the same cursor is handed back; no fan-out");
    sqlx::query("DELETE FROM jobs WHERE kind='BackfillAtomTitles'").execute(owner).await.expect("clear the hand-back");
    sqlx::query("GRANT SELECT ON atoms TO coralswarm_tenant_app").execute(owner).await.expect("restore original read grant");
    let restored: bool = sqlx::query_scalar("SELECT has_table_privilege(current_user,'atoms','SELECT')")
        .fetch_one(app).await.expect("restored read privilege");
    assert!(restored);
}

#[tokio::test]
async fn org_candidates_are_scoped_switched_off_jobs_hand_back_and_model_free_maintenance_titles_inline_atom() {
    let Some(base) = base_url() else {
        eprintln!("{SKIP_REASON}");
        return;
    };

    let db = "orgt_atom_title_tenant_rls";
    let mut admin_conn = PgConnection::connect(&base).await.expect("connect admin");
    let _database_cleanup_0 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    retained_database::recreate(&mut admin_conn, db).await.expect("create owned scratch db");
    let db_url = with_db(&base, db);

    // `owner`: the table-owner/superuser connection — applies migrations,
    // seeds fixtures, and reads back final state for assertions (same "seed
    // and verify as the trusted admin, exercise as the low-privilege role"
    // split `rls_probe.rs`/`tenant_rls_phase2_pg.rs` both use).
    let owner: PgPool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&db_url)
        .await
        .expect("connect owner");
    sqlx::migrate!("./migrations_tenant")
        .run(&owner)
        .await
        .expect("apply tenant tree incl. 0009_tenant_rls");
    set_app_role_password_with_retry(&owner).await;

    let account_id = Uuid::new_v4();
    let user_id = Uuid::new_v4();
    // `kind = 'member'`, owned by a SPECIFIC user with no `app.user_id` ever
    // set in this test: `tenant_rls_visible_graph_ids()` only admits a
    // member-kind graph via `tenant_rls_is_system()` OR an owner match. A
    // non-member-kind graph would be visible regardless of scope (the
    // policy's `g.kind <> 'member'` escape hatch), which would make the
    // negative control below pass for the wrong reason — this graph shape is
    // what makes it a genuine proof.
    let graph_id: Uuid = sqlx::query_scalar(
        "INSERT INTO graphs (id, account_id, name, kind, owner_user_id) \
         VALUES (gen_random_uuid(), $1, 'member ocean', 'member', $2) RETURNING id",
    )
    .bind(account_id)
    .bind(user_id)
    .fetch_one(&owner)
    .await
    .expect("seed member-kind graph");

    let atom_id: Uuid = sqlx::query_scalar(
        "INSERT INTO atoms (graph_id, source_type, source_id, content_blob_key, content_inline, \"timestamp\") \
         VALUES ($1, 'note', 'org-rls-1', 'blob', $2, now()) RETURNING atom_id",
    )
    .bind(graph_id)
    .bind("Shipped the tenant-RLS fix for the atom-title sweep's Org arm.")
    .fetch_one(&owner)
    .await
    .expect("seed untitled atom");

    // The exact DSN production would resolve for this org, authenticated as
    // the intended non-exempt role — NOT built through `TenantRegistry`
    // (this is not the TenantRegistry pool-exemption test).
    let app_pool: PgPool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&as_app_role_dsn(&db_url))
        .await
        .expect("connect as coralswarm_tenant_app");

    // ── Precondition: the probe role genuinely lacks bypass ───────────────
    // Without this the whole test could pass vacuously against an exempt
    // role (e.g. a misconfigured test cluster granting BYPASSRLS by
    // default) and prove nothing.
    let bypass: bool = sqlx::query("SELECT rolbypassrls FROM pg_roles WHERE rolname = current_user")
        .fetch_one(&app_pool)
        .await
        .expect("read rolbypassrls")
        .get(0);
    assert!(
        !bypass,
        "coralswarm_tenant_app must NOT have BYPASSRLS — this test proves nothing under a role \
         RLS does not apply to"
    );

    // ── Negative control: no `app.tenant_actor` set at all ────────────────
    // A bare, unscoped call on the SAME pool/role — the exact ambient shape
    // the Org arm used to run before this fix. Must be a SUCCESSFUL empty
    // result (RLS filtering the row), never an error — those are different
    // failure modes and this assertion is deliberately written to tell them
    // apart (`Ok(rows) if rows.is_empty()`, never `is_err()` or
    // `unwrap_or_default()`).
    match test_support_candidate_atom_ids(&app_pool).await {
        Ok(rows) => assert!(
            rows.is_empty(),
            "unscoped, the non-exempt role must see ZERO candidate atoms — got {rows:?}. If this \
             is non-empty, either RLS is not actually applying (owner/BYPASSRLS leaked into the \
             role) or the seeded graph is visible for a reason unrelated to app.tenant_actor."
        ),
        Err(e) => panic!(
            "the unscoped candidate query must SUCCEED with zero rows (RLS row-filtering), not \
             error: {e}"
        ),
    }

    // ── Sanity: the row IS visible once the transaction-local system actor
    //    is set — proves the fixture and the policy, isolated from run_batch
    //    itself, before trusting the end-to-end assertion below. ───────────
    let mut sys_tx = begin_tenant_system_scoped_without_account(&app_pool)
        .await
        .expect("begin_tenant_system_scoped_without_account");
    let visible = test_support_candidate_atom_ids(&mut *sys_tx)
        .await
        .expect("scoped candidate query");
    assert!(
        visible.contains(&atom_id),
        "system-scoped, the same role must see the seeded atom — got {visible:?}"
    );
    sys_tx.rollback().await.expect("close candidate scope");

    // The real automatic entry stays switched off even with real private candidates.
    let tenant = TenantId::Org("org_atom_title_tenant_rls".into());
    let spy = RefusalSpy::default();
    let receipt = enqueue_refused_job(&tenant, &app_pool, &owner, account_id).await;
    assert_worker_refuses(&tenant, &app_pool, &owner, account_id, receipt, &spy).await;
    let receipt = enqueue_refused_job(&tenant, &app_pool, &owner, account_id).await;
    assert_worker_refuses(&tenant, &app_pool, &owner, account_id, receipt, &spy).await;

    // Separate, explicitly model-free maintenance seam: actual existing run_batch,
    // no worker/queue activation. Preserve its useful scoped local-value proof.
    let jobs_before_maintenance = jobs_snapshot(&owner).await;
    let money_before_maintenance = accounting_snapshot(&owner).await;
    let outcome = test_support_model_free_org_batch(&app_pool, "org_atom_title_tenant_rls", account_id)
        .await.expect("real model-free Org batch");
    assert_eq!(outcome, (1, 1, 0));
    assert_eq!(jobs_snapshot(&owner).await, jobs_before_maintenance);
    assert_eq!(accounting_snapshot(&owner).await, money_before_maintenance);

    let (title, title_kind): (Option<String>, Option<String>) =
        sqlx::query_as("SELECT title, title_kind FROM atoms WHERE atom_id = $1")
            .bind(atom_id)
            .fetch_one(&owner)
            .await
            .expect("read back title/title_kind as owner");
    assert!(
        title.as_deref().is_some_and(|t| t.starts_with("Shipped the tenant-RLS fix")),
        "the real model-free maintenance primitive must title through its system-scoped Org arm; got {title:?}"
    );
    assert_eq!(
        title_kind.as_deref(),
        Some("fallback"),
        "no LLM configured in this test ⇒ the deterministic fallback path, stamped accordingly"
    );

    app_pool.close().await;
    owner.close().await;
    retained_database::finish(&mut admin_conn, db, true).await.expect("success-only scratch cleanup");
}

// ── Independent review finding M4: a 2-segment blob_key terminates, it
// does not retry forever ────────────────────────────────────────────────────

/// The exact literal `atom_title_backfill::UNFETCHABLE_TERMINAL_TITLE`
/// stamps — pinned here (not exported) so this test proves the OBSERVABLE
/// title text, the same way a caller reading the row back would.
const EXPECTED_TERMINAL_TITLE: &str = "Archived coral (content unavailable)";

#[tokio::test]
async fn switched_off_job_preserves_two_segment_key_and_model_free_maintenance_is_terminal_and_idempotent() {
    let Some(base) = base_url() else {
        eprintln!("{SKIP_REASON}");
        return;
    };

    let db = "orgt_atom_title_terminal";
    let mut admin_conn = PgConnection::connect(&base).await.expect("connect admin");
    let _database_cleanup_1 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    retained_database::recreate(&mut admin_conn, db).await.expect("create owned scratch db");
    let db_url = with_db(&base, db);

    let owner: PgPool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&db_url)
        .await
        .expect("connect owner");
    sqlx::migrate!("./migrations_tenant")
        .run(&owner)
        .await
        .expect("apply tenant tree incl. 0009_tenant_rls");
    set_app_role_password_with_retry(&owner).await;

    let account_id = Uuid::new_v4();
    let user_id = Uuid::new_v4();
    let graph_id: Uuid = sqlx::query_scalar(
        "INSERT INTO graphs (id, account_id, name, kind, owner_user_id) \
         VALUES (gen_random_uuid(), $1, 'member ocean', 'member', $2) RETURNING id",
    )
    .bind(account_id)
    .bind(user_id)
    .fetch_one(&owner)
    .await
    .expect("seed member-kind graph");

    // The concrete production case (PR body / independent review finding
    // M4): `copy_atoms_to_graph`'s pre-org-scope 2-segment blob key
    // ("{graph}/{atom}") — no org segment, so `assert_blob_key_org_scope`
    // rejects it for THIS Org tenant on every pass, forever, unless
    // something removes the row from the candidate set. No
    // `content_inline` either (an R2-only atom, the shape that actually
    // reaches `fetch_r2_body` at all).
    let two_segment_key = format!("{graph_id}/{}", Uuid::new_v4());
    let atom_id: Uuid = sqlx::query_scalar(
        "INSERT INTO atoms (graph_id, source_type, source_id, content_blob_key, \"timestamp\") \
         VALUES ($1, 'note', 'org-terminal-1', $2, now()) RETURNING atom_id",
    )
    .bind(graph_id)
    .bind(&two_segment_key)
    .fetch_one(&owner)
    .await
    .expect("seed R2-only atom with a 2-segment blob key");

    let app_pool: PgPool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&as_app_role_dsn(&db_url))
        .await
        .expect("connect as coralswarm_tenant_app");

    let tenant = TenantId::Org("org_atom_title_terminal".to_string());

    // ── Precondition: the row IS a candidate before pass one ──────────────
    let mut sys_tx = begin_tenant_system_scoped_without_account(&app_pool)
        .await
        .expect("begin_tenant_system_scoped_without_account");
    let before = test_support_candidate_atom_ids(&mut *sys_tx).await.expect("candidate query");
    sys_tx.rollback().await.expect("close candidate scope");
    assert!(
        before.contains(&atom_id),
        "the seeded 2-segment-key atom must be a candidate before any pass runs — got {before:?}"
    );

    let spy = RefusalSpy::default();
    let receipt = enqueue_refused_job(&tenant, &app_pool, &owner, account_id).await;
    assert_worker_refuses(&tenant, &app_pool, &owner, account_id, receipt, &spy).await;
    let jobs_before_maintenance = jobs_snapshot(&owner).await;
    let money_before_maintenance = accounting_snapshot(&owner).await;
    // Actual first model-free maintenance batch, not an admitted automatic job.
    let first = test_support_model_free_org_batch(&app_pool, "org_atom_title_terminal", account_id)
        .await.expect("real first terminal batch");
    assert_eq!(first, (1, 1, 1));
    assert_eq!(jobs_snapshot(&owner).await, jobs_before_maintenance);
    assert_eq!(accounting_snapshot(&owner).await, money_before_maintenance);

    let (title_after_1, kind_after_1, blob_key_after_1): (Option<String>, Option<String>, String) =
        sqlx::query_as("SELECT title, title_kind, content_blob_key FROM atoms WHERE atom_id = $1")
            .bind(atom_id)
            .fetch_one(&owner)
            .await
            .expect("read back after pass one");
    assert_eq!(
        title_after_1.as_deref(),
        Some(EXPECTED_TERMINAL_TITLE),
        "pass one must stamp the deterministic terminal title, not leave it null forever"
    );
    assert_eq!(kind_after_1.as_deref(), Some("fallback"));
    assert_eq!(
        blob_key_after_1, "",
        "pass one must blank the unusable blob_key — this, not the title/kind change alone, is \
         what removes the row from CANDIDATE_SQL's selection"
    );

    // ── The row must have LEFT the candidate set ───────────────────────────
    let mut sys_tx2 = begin_tenant_system_scoped_without_account(&app_pool)
        .await
        .expect("begin_tenant_system_scoped_without_account (2)");
    let after_pass_1 =
        test_support_candidate_atom_ids(&mut *sys_tx2).await.expect("candidate query after pass 1");
    sys_tx2.rollback().await.expect("close candidate scope after maintenance");
    assert!(
        !after_pass_1.contains(&atom_id),
        "a terminally-stamped atom must no longer be a CANDIDATE_SQL candidate — got {after_pass_1:?}"
    );

    // A fresh job stays switched off even after separate maintenance succeeded.
    let receipt = enqueue_refused_job(&tenant, &app_pool, &owner, account_id).await;
    assert_worker_refuses(&tenant, &app_pool, &owner, account_id, receipt, &spy).await;
    let jobs_before_second = jobs_snapshot(&owner).await;
    let second = test_support_model_free_org_batch(&app_pool, "org_atom_title_terminal", account_id)
        .await.expect("second real maintenance batch");
    assert_eq!(second, (0, 0, 0), "terminal maintenance must not re-examine the row");
    assert_eq!(jobs_snapshot(&owner).await, jobs_before_second);
    assert_eq!(accounting_snapshot(&owner).await, money_before_maintenance);

    let (title_after_2, kind_after_2, blob_key_after_2): (Option<String>, Option<String>, String) =
        sqlx::query_as("SELECT title, title_kind, content_blob_key FROM atoms WHERE atom_id = $1")
            .bind(atom_id)
            .fetch_one(&owner)
            .await
            .expect("read back after pass two");
    assert_eq!(
        (title_after_2, kind_after_2, blob_key_after_2),
        (Some(EXPECTED_TERMINAL_TITLE.to_string()), Some("fallback".to_string()), String::new()),
        "pass two must be a complete no-op on this atom — it was never re-examined, let alone \
         re-fetched, because CANDIDATE_SQL no longer selects it"
    );

    app_pool.close().await;
    owner.close().await;
    retained_database::finish(&mut admin_conn, db, true).await.expect("success-only scratch cleanup");
}
