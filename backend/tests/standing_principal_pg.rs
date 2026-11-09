use coralswarm_backend::{tenancy::TenantId, test_standing_principal};
use serde_json::Value;
use sqlx::{postgres::{PgConnectOptions, PgPoolOptions}, PgPool};
use std::{str::FromStr, time::Duration, sync::{Arc, atomic::{AtomicUsize, Ordering}}};
use tracing::{instrument::WithSubscriber, field::{Field, Visit}};
use tracing_subscriber::{layer::{Context, SubscriberExt}, Layer};
use uuid::Uuid;

/// The standing profile every fixture run reconciles under.
const PROFILE: &str = "source-atoms-v2";

struct Alerts(Arc<AtomicUsize>);
struct AlertFlag(bool);
impl Visit for AlertFlag {
    fn record_bool(&mut self, field: &Field, value: bool) { if field.name()=="alert" { self.0=value; } }
    fn record_debug(&mut self, _: &Field, _: &dyn std::fmt::Debug) {}
}
impl<S: tracing::Subscriber> Layer<S> for Alerts {
    fn on_event(&self, event: &tracing::Event<'_>, _: Context<'_, S>) {
        let mut flag=AlertFlag(false);
        event.record(&mut flag);
        if flag.0 { self.0.fetch_add(1,Ordering::SeqCst); }
    }
}

struct Database { owner: PgPool, app: PgPool, name: coralswarm_backend::test_db::TestDatabase, role: String }
struct Fixture {
    admin: PgPool,
    control: Database,
    content: Option<Database>,
    tenant: TenantId,
    account: Uuid,
    requester: Uuid,
    graph: Uuid,
    run: Uuid,
}

impl Database {
    async fn new(admin: &PgPool, options: &PgConnectOptions, tenant: bool, migrations: &sqlx::migrate::Migrator) -> Self {
        let name = coralswarm_backend::test_db::TestDatabase::new(options, "cor22_principal");
        let role = name.role("cor22_principal_role");
        sqlx::query(&format!("CREATE DATABASE {name}")).execute(admin).await.unwrap();
        let owner = PgPoolOptions::new().max_connections(6).connect_with(options.clone().database(&name)).await.unwrap();
        migrations.run(&owner).await.unwrap();
        let mut connection = owner.acquire().await.unwrap();
        sqlx::query("SELECT set_config('app.identity_serving_contract','composite-v1',false)")
            .execute(&mut *connection).await.unwrap();
        sqlx::raw_sql(include_str!("../schema/identity-contract.sql"))
            .execute(&mut *connection).await.unwrap();
        drop(connection);
        let password = Uuid::new_v4().simple().to_string();
        sqlx::query(&format!("CREATE ROLE {role} LOGIN INHERIT NOSUPERUSER NOBYPASSRLS PASSWORD '{password}'")).execute(admin).await.unwrap();
        let base = if tenant { "coralswarm_tenant_app" } else { "coralswarm_app" };
        sqlx::query(&format!("GRANT {base} TO {role}")).execute(admin).await.unwrap();
        let app = PgPoolOptions::new().max_connections(1).acquire_timeout(Duration::from_secs(2))
            .connect_with(options.clone().database(&name).username(&role).password(&password)).await.unwrap();
        let restricted: bool = sqlx::query_scalar("SELECT NOT rolsuper AND NOT rolbypassrls FROM pg_roles WHERE rolname=current_user")
            .fetch_one(&app).await.unwrap();
        assert!(restricted);
        Self { owner, app, name, role }
    }

    async fn close(self, admin: &PgPool) {
        self.app.close().await;
        self.owner.close().await;
        sqlx::query(&format!("DROP DATABASE {} WITH (FORCE)", self.name)).execute(admin).await.unwrap();
        sqlx::query(&format!("DROP ROLE {}", self.role)).execute(admin).await.unwrap();
    }
}

impl Fixture {
    async fn new(org: bool) -> Self {
        Self::migrated(org, &sqlx::migrate!("./migrations_control"), &sqlx::migrate!("./migrations_tenant"), PROFILE).await
    }

    async fn with_legacy_unit(org: bool) -> Self {
        let control = sqlx::migrate!("./migrations_control");
        let tenant = sqlx::migrate!("./migrations_tenant");
        let before_fence = |full: &sqlx::migrate::Migrator, fence| sqlx::migrate::Migrator {
            migrations: std::borrow::Cow::Owned(full.iter().filter(|m| m.version < fence).cloned().collect()),
            ignore_missing: false, locking: true, no_tx: false,
        };
        // This schema predates v2: the run, unit and version start on v1 and
        // move to v2 once the full schema is in place.
        let fixture = Self::migrated(org, &before_fence(&control, 321), &before_fence(&tenant, 153), "source-atoms-v1").await;
        use coralswarm_backend::{source_evidence::{self, CaptureSource}, tenancy::scope};
        let mut tx = if matches!(fixture.tenant, TenantId::Org(_)) {
            scope::begin_tenant_system_scoped(fixture.app(), fixture.account).await.unwrap()
        } else { scope::begin_account_scoped(fixture.app(), fixture.account).await.unwrap() };
        let source = CaptureSource {
            source_type: "note", provider_namespace: "fixture:principal", source_id: "retained-unit",
            normalized_text: "This source remains unchanged during principal handoff.",
            representation: "original_text", metadata: &serde_json::json!({}), occurred_at: None,
            participants: &[], ingested_by: Some(fixture.requester), scope: "private", duration_ms: None, segments: None,
        };
        // A capture from before v2 existed (this schema predates it): pinned v1.
        let saved = source_evidence::capture_historical_v1_tx(&mut tx, fixture.graph, fixture.account, &source, &[42;32]).await.unwrap();
        sqlx::query("INSERT INTO corpus_enrichment_units(run_id,graph_id,kind,holding_id,source_item_id,extraction_version,state,execution_owner) SELECT $1,graph_id,'retained_version',version_id,source_item_id,extraction_version,'pending',true FROM source_versions WHERE version_id=$2")
            .bind(fixture.run).bind(saved.version_id).execute(&mut *tx).await.unwrap();
        sqlx::query("INSERT INTO managed_budget_periods(account_id,purpose,period_start,limit_micro_usd,reserved_micro_usd,attempts) VALUES($1,'source_extraction',current_date,1000000,17,1)")
            .bind(fixture.account).execute(&mut *tx).await.unwrap();
        sqlx::query("INSERT INTO managed_attempt_receipts(account_id,purpose,period_start,graph_id,source_version_id,attributed_user_id,logical_call,attempt_no,candidate_models,input_bound_tokens,max_tokens,reserved_micro_usd) VALUES($1,'source_extraction',current_date,$2,$3,$4,$5,1,ARRAY['fixture/pinned'],10,10,17)")
            .bind(fixture.account).bind(fixture.graph).bind(saved.version_id).bind(fixture.requester).bind(Uuid::new_v4()).execute(&mut *tx).await.unwrap();
        tx.commit().await.unwrap();
        sqlx::migrate!("./migrations_control").run(&fixture.control.owner).await.unwrap();
        if let Some(content) = &fixture.content {
            sqlx::migrate!("./migrations_tenant").run(&content.owner).await.unwrap();
        }
        // A v1 unit's pin is immutable once written; the fixture owner rewrites
        // the history as if it had been adopted under v2.
        sqlx::raw_sql(&format!("BEGIN; ALTER TABLE source_versions DISABLE TRIGGER USER; ALTER TABLE corpus_enrichment_units DISABLE TRIGGER USER; ALTER TABLE corpus_enrichment_runs DISABLE TRIGGER USER; \
            UPDATE source_versions SET extraction_version='{PROFILE}'; UPDATE corpus_enrichment_units SET extraction_version='{PROFILE}'; UPDATE corpus_enrichment_runs SET extraction_version='{PROFILE}'; \
            ALTER TABLE source_versions ENABLE TRIGGER USER; ALTER TABLE corpus_enrichment_units ENABLE TRIGGER USER; ALTER TABLE corpus_enrichment_runs ENABLE TRIGGER USER; COMMIT;"))
            .execute(fixture.owner()).await.unwrap();
        fixture
    }

    async fn migrated(org: bool, control_migrations: &sqlx::migrate::Migrator, tenant_migrations: &sqlx::migrate::Migrator, profile: &str) -> Self {
        let url = std::env::var("ORGT_TEST_BASE_URL").expect("isolated local ORGT_TEST_BASE_URL required");
        let options = PgConnectOptions::from_str(&url).unwrap();
        assert!(matches!(options.get_host(), "localhost" | "127.0.0.1" | "::1"));
        let admin = PgPoolOptions::new().max_connections(2).connect_with(options.clone()).await.unwrap();
        sqlx::raw_sql("DO $$ BEGIN IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='coralswarm_tenant_app') THEN CREATE ROLE coralswarm_tenant_app NOLOGIN NOSUPERUSER NOBYPASSRLS; END IF; END $$;")
            .execute(&admin).await.unwrap();
        let control = Database::new(&admin, &options, false, control_migrations).await;
        let content = if org { Some(Database::new(&admin, &options, true, tenant_migrations).await) } else { None };
        let account = Uuid::new_v4();
        let requester = Uuid::new_v4();
        let org_id = format!("org_principal_{}", account.simple());
        sqlx::query("INSERT INTO users(id,email,clerk_user_id) VALUES($1,$2,$2)")
            .bind(requester).bind(format!("{requester}@test.local")).execute(&control.owner).await.unwrap();
        sqlx::query("INSERT INTO accounts(id,owner_user_id,clerk_org_id) VALUES($1,$2,$3)")
            .bind(account).bind(requester).bind(if org { Some(org_id.clone()) } else { None }).execute(&control.owner).await.unwrap();
        if org {
            // An org's standing work runs on its own tenant database, so the
            // org resolves to that plane (`managed::plane`, COR-114) only
            // through an active routing row, as every org has in production.
            let mut financial = coralswarm_backend::tenancy::scope::begin_account_scoped(&control.owner, account).await.unwrap();
            // The financial entry exists only from the funded fence (0321) on;
            // the pre-fence fixtures migrate below it.
            let fenced: bool = sqlx::query_scalar("SELECT to_regprocedure('public.billing_enter_account(uuid)') IS NOT NULL")
                .fetch_one(&mut *financial).await.unwrap();
            if fenced {
                coralswarm_backend::billing::renewal::lock_period_rotation(&mut financial, account).await.unwrap();
            }
            sqlx::query("INSERT INTO tenant_databases(clerk_org_id,account_id,slug,db_secret_ref,status) VALUES($1,$2,$3,$4,'active')")
                .bind(&org_id).bind(account).bind(format!("sp-{}", &account.simple().to_string()[..20]))
                .bind(format!("TENANT_SP_{}", account.simple())).execute(&mut *financial).await.unwrap();
            financial.commit().await.unwrap();
        }
        sqlx::query("INSERT INTO account_members(account_id,user_id,role,joined_at,seat_tier_id) VALUES($1,$2,'member','2026-01-01',(SELECT CASE WHEN clerk_org_id IS NOT NULL THEN (SELECT id FROM seat_tiers WHERE code='standard' AND is_active) END FROM accounts WHERE id=$1))")
            .bind(account).bind(requester).execute(&control.owner).await.unwrap();
        let pool = content.as_ref().map(|d| &d.owner).unwrap_or(&control.owner);
        let graph = sqlx::query_scalar("INSERT INTO graphs(account_id,name) VALUES($1,'Principal reconciliation') RETURNING id")
            .bind(account).fetch_one(pool).await.unwrap();
        let run = sqlx::query_scalar("INSERT INTO corpus_enrichment_runs(graph_id,account_id,requested_by,mode,corpus_kind,status,extraction_version,model,max_items,max_characters,max_model_calls,run_kind,execution_principal,standing_pause_state) VALUES($1,$2,$3,'execute','real','running',$4,'fixture/pinned',1000,8000000,10000,'standing',$3,'none') RETURNING run_id")
            .bind(graph).bind(account).bind(requester).bind(profile).fetch_one(pool).await.unwrap();
        Self { admin, control, content, tenant: if org { TenantId::Org(org_id) } else { TenantId::Consumer }, account, requester, graph, run }
    }

    fn owner(&self) -> &PgPool { self.content.as_ref().map(|d| &d.owner).unwrap_or(&self.control.owner) }
    fn app(&self) -> &PgPool { self.content.as_ref().map(|d| &d.app).unwrap_or(&self.control.app) }

    async fn member(&self, id: Uuid, role: &str, joined: &str) {
        sqlx::query("INSERT INTO users(id,email,clerk_user_id) VALUES($1,$2,$2)")
            .bind(id).bind(format!("{id}@test.local")).execute(&self.control.owner).await.unwrap();
        sqlx::query("INSERT INTO account_members(account_id,user_id,role,joined_at,seat_tier_id) VALUES($1,$2,$3,$4::text::timestamptz,(SELECT CASE WHEN clerk_org_id IS NOT NULL THEN (SELECT id FROM seat_tiers WHERE code='standard' AND is_active) END FROM accounts WHERE id=$1))")
            .bind(self.account).bind(id).bind(role).bind(joined).execute(&self.control.owner).await.unwrap();
    }

    async fn reconcile(&self, expected: Uuid) -> Result<String, String> {
        let count=Arc::new(AtomicUsize::new(0));
        let subscriber=tracing_subscriber::registry().with(Alerts(count.clone()));
        let result=test_standing_principal(self.tenant.clone(), self.app(), &self.control.app, self.account, self.graph, self.run, PROFILE, expected).with_subscriber(subscriber).await;
        assert_eq!(count.load(Ordering::SeqCst), usize::from(result.as_ref().is_ok_and(|s| s.starts_with("Changed"))), "only acknowledged changes emit one alert");
        result
    }

    async fn run_snapshot(&self) -> Value {
        sqlx::query_scalar("SELECT to_jsonb(r)-'execution_principal'-'updated_at' FROM corpus_enrichment_runs r WHERE run_id=$1")
            .bind(self.run).fetch_one(self.owner()).await.unwrap()
    }

    async fn excluded_snapshot(&self) -> Value {
        sqlx::query_scalar("SELECT jsonb_build_object('units',(SELECT COALESCE(jsonb_agg(to_jsonb(t) ORDER BY unit_id),'[]') FROM corpus_enrichment_units t),'sources',(SELECT COALESCE(jsonb_agg(to_jsonb(t) ORDER BY version_id),'[]') FROM source_versions t),'receipts',(SELECT COALESCE(jsonb_agg(to_jsonb(t) ORDER BY source_version_id),'[]') FROM corpus_capture_admissions t),'jobs',(SELECT count(*) FROM jobs),'atoms',(SELECT count(*) FROM atoms),'attempts',(SELECT COALESCE(jsonb_agg(to_jsonb(t) ORDER BY receipt_id),'[]') FROM managed_attempt_receipts t),'budget',(SELECT COALESCE(jsonb_agg(to_jsonb(t) ORDER BY account_id,purpose,period_start),'[]') FROM managed_budget_periods t),'usage',(SELECT count(*) FROM llm_usage))")
            .fetch_one(self.owner()).await.unwrap()
    }

    async fn seed_unit(&self) -> Uuid {
        sqlx::query_scalar("SELECT holding_id FROM corpus_enrichment_units WHERE run_id=$1 AND graph_id=$2 AND kind='retained_version' AND execution_owner")
            .bind(self.run).bind(self.graph).fetch_one(self.owner()).await.unwrap()
    }

    async fn requester_fence(&self, version: Uuid) -> bool {
        use coralswarm_backend::{corpus_enrichment, tenancy::scope};
        let mut tx = if matches!(self.tenant, TenantId::Org(_)) {
            scope::begin_tenant_system_scoped(self.app(), self.account).await.unwrap()
        } else { scope::begin_account_scoped(self.app(), self.account).await.unwrap() };
        let allowed = corpus_enrichment::requester_allowed_tx(&mut tx, &self.control.app, &self.tenant, self.graph, version, PROFILE).await.unwrap();
        tx.rollback().await.unwrap();
        allowed
    }

    async fn principal(&self) -> Uuid {
        sqlx::query_scalar("SELECT execution_principal FROM corpus_enrichment_runs WHERE run_id=$1")
            .bind(self.run).fetch_one(self.owner()).await.unwrap()
    }

    async fn close(self) {
        if let Some(content) = self.content { content.close(&self.admin).await; }
        self.control.close(&self.admin).await;
        self.admin.close().await;
    }
}

async fn shared_handoff(org: bool) {
    let f = Fixture::with_legacy_unit(org).await;
    let later = Uuid::from_u128(0x200);
    let earlier = Uuid::from_u128(0x100);
    f.member(later, "admin", "2026-02-01").await;
    f.member(earlier, "owner", "2026-02-01").await;
    let returning = Uuid::from_u128(0x50);
    f.member(returning, "admin", "2025-01-01").await;
    sqlx::query("DELETE FROM account_members WHERE account_id=$1 AND user_id=$2").bind(f.account).bind(returning).execute(&f.control.owner).await.unwrap();
    let version = f.seed_unit().await;
    let before = f.run_snapshot().await;
    let excluded = f.excluded_snapshot().await;
    assert_eq!(f.reconcile(f.requester).await.unwrap(), format!("Changed {{ from: {}, to: {earlier} }}", f.requester));
    assert_eq!(f.principal().await, earlier);
    assert_eq!(f.run_snapshot().await, before);
    assert_eq!(f.excluded_snapshot().await, excluded);
    assert_eq!(f.reconcile(f.requester).await.unwrap(), format!("Superseded {{ current: {earlier} }}"));
    sqlx::query("INSERT INTO account_members(account_id,user_id,role,joined_at,seat_tier_id) VALUES($1,$2,'admin','2025-01-01',(SELECT CASE WHEN clerk_org_id IS NOT NULL THEN (SELECT id FROM seat_tiers WHERE code='standard' AND is_active) END FROM accounts WHERE id=$1))")
        .bind(f.account).bind(returning).execute(&f.control.owner).await.unwrap();
    assert_eq!(f.reconcile(earlier).await.unwrap(), format!("Unchanged {{ principal: {earlier} }}"));
    for principal in [later,earlier] {
        sqlx::query("UPDATE corpus_enrichment_runs SET execution_principal=$2 WHERE run_id=$1").bind(f.run).bind(principal).execute(f.owner()).await.unwrap();
    }
    assert_eq!(f.reconcile(earlier).await.unwrap(), format!("Unchanged {{ principal: {earlier} }}"));
    sqlx::query("UPDATE account_members SET role='member' WHERE account_id=$1 AND user_id=$2")
        .bind(f.account).bind(earlier).execute(&f.control.owner).await.unwrap();
    assert_eq!(f.reconcile(earlier).await.unwrap(), format!("Changed {{ from: {earlier}, to: {returning} }}"));
    assert_eq!(f.run_snapshot().await, before);
    assert_eq!(f.excluded_snapshot().await, excluded);
    sqlx::query("DELETE FROM account_members WHERE account_id=$1 AND user_id=$2").bind(f.account).bind(f.requester).execute(&f.control.owner).await.unwrap();
    assert!(!f.requester_fence(version).await);
    sqlx::query("INSERT INTO account_members(account_id,user_id,role,seat_tier_id) VALUES($1,$2,'member',(SELECT CASE WHEN clerk_org_id IS NOT NULL THEN (SELECT id FROM seat_tiers WHERE code='standard' AND is_active) END FROM accounts WHERE id=$1))").bind(f.account).bind(f.requester).execute(&f.control.owner).await.unwrap();
    sqlx::query("DELETE FROM account_members WHERE account_id=$1 AND user_id=$2").bind(f.account).bind(returning).execute(&f.control.owner).await.unwrap();
    assert!(f.requester_fence(version).await);
    eprintln!("requester={} principal={} fence_without_requester=false fence_without_principal=true", f.requester, returning);
    f.close().await;
}

#[tokio::test]
async fn consumer_single_connection_handoff_ties_and_incumbent_retention() { shared_handoff(false).await; }
#[tokio::test]
async fn org_handoff_ties_and_incumbent_retention() { shared_handoff(true).await; }

async fn member_owner(org: bool) {
    let f = Fixture::new(org).await;
    let owner = Uuid::new_v4();
    let admin = Uuid::new_v4();
    f.member(owner, "member", "2026-02-01").await;
    f.member(admin, "admin", "2025-01-01").await;
    sqlx::query("UPDATE graphs SET kind='member',owner_user_id=$2 WHERE id=$1")
        .bind(f.graph).bind(owner).execute(f.owner()).await.unwrap();
    assert_eq!(f.reconcile(f.requester).await.unwrap(), format!("Changed {{ from: {}, to: {owner} }}", f.requester));
    sqlx::query("DELETE FROM account_members WHERE account_id=$1 AND user_id=$2")
        .bind(f.account).bind(owner).execute(&f.control.owner).await.unwrap();
    assert_eq!(f.reconcile(owner).await.unwrap(), "NoEligible(OwnerNotAccountMember)");
    assert_eq!(f.principal().await, owner);
    sqlx::query("UPDATE users SET deleted_at=now() WHERE id=$1").bind(owner).execute(&f.control.owner).await.unwrap();
    assert_eq!(f.reconcile(owner).await.unwrap(), "NoEligible(OwnerUserDeleted)");
    sqlx::query("UPDATE graphs SET owner_user_id=$2 WHERE id=$1").bind(f.graph).bind(Uuid::new_v4()).execute(f.owner()).await.unwrap();
    assert_eq!(f.reconcile(owner).await.unwrap(), "NoEligible(OwnerUserMissing)");
    sqlx::query("UPDATE graphs SET archived_at=now() WHERE id=$1").bind(f.graph).execute(f.owner()).await.unwrap();
    assert_eq!(f.reconcile(owner).await.unwrap(), "Denied(GraphArchived)");
    assert_eq!(f.principal().await, owner);
    f.close().await;
}
#[tokio::test]
async fn consumer_member_owner_never_falls_back_to_admin() { member_owner(false).await; }
#[tokio::test]
async fn org_member_owner_never_falls_back_to_admin() { member_owner(true).await; }

async fn paused_and_retired(org: bool) {
    let f = Fixture::new(org).await;
    let mut incumbent = f.requester;
    for (pause, status, reason) in [("none","running",None),("operator","paused",Some("operator_paused")),("processing","paused",Some("managed_processing_disabled")),("both","paused",Some("operator_paused"))] {
        let next = Uuid::new_v4();
        f.member(next, "admin", "2026-01-01").await;
        sqlx::query("UPDATE account_members SET role='member' WHERE account_id=$1 AND user_id=$2").bind(f.account).bind(incumbent).execute(&f.control.owner).await.unwrap();
        sqlx::query("UPDATE corpus_enrichment_runs SET standing_pause_state=$2,status=$3,pause_reason=$4 WHERE run_id=$1")
            .bind(f.run).bind(pause).bind(status).bind(reason).execute(f.owner()).await.unwrap();
        let before = f.run_snapshot().await;
        assert_eq!(f.reconcile(incumbent).await.unwrap(), format!("Changed {{ from: {incumbent}, to: {next} }}"));
        assert_eq!(f.run_snapshot().await, before);
        incumbent = next;
    }
    sqlx::query("UPDATE corpus_enrichment_runs SET retired_at=now() WHERE run_id=$1").bind(f.run).execute(f.owner()).await.unwrap();
    let before = f.run_snapshot().await;
    assert_eq!(f.reconcile(incumbent).await.unwrap(), "Skipped(Retired)");
    assert_eq!(f.run_snapshot().await, before);
    assert_eq!(f.principal().await, incumbent);
    f.close().await;
}
#[tokio::test]
async fn consumer_preserves_every_pause_axis_and_skips_retired() { paused_and_retired(false).await; }
#[tokio::test]
async fn org_preserves_every_pause_axis_and_skips_retired() { paused_and_retired(true).await; }

async fn known_failure(org: bool) {
    let f = Fixture::new(org).await;
    assert_eq!(f.reconcile(f.requester).await.unwrap(), "NoEligible(NoCurrentPrivilegedMember)");
    f.member(Uuid::new_v4(), "admin", "2026-01-01").await;
    sqlx::raw_sql("CREATE FUNCTION principal_reject_update() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'principal witness rollback'; END $$; CREATE TRIGGER principal_reject_update BEFORE UPDATE ON corpus_enrichment_runs FOR EACH ROW EXECUTE FUNCTION principal_reject_update();")
        .execute(f.owner()).await.unwrap();
    let error = f.reconcile(f.requester).await.unwrap_err();
    assert!(error.contains("Unavailable { stage: Update"), "{error}");
    assert_eq!(f.principal().await, f.requester);
    sqlx::raw_sql("DROP TRIGGER principal_reject_update ON corpus_enrichment_runs; DROP FUNCTION principal_reject_update();").execute(f.owner()).await.unwrap();
    let held = f.app().acquire().await.unwrap();
    assert_eq!(f.reconcile(f.requester).await.unwrap_err(), "Deadline { stage: AcquireContent }");
    drop(held);
    if org {
        let held = f.control.app.acquire().await.unwrap();
        assert_eq!(f.reconcile(f.requester).await.unwrap_err(), "Deadline { stage: AcquireControl }");
        drop(held);
    }
    let mut locked=f.owner().begin().await.unwrap();
    sqlx::query("SELECT run_id FROM corpus_enrichment_runs WHERE run_id=$1 FOR UPDATE").bind(f.run).execute(&mut *locked).await.unwrap();
    let error=f.reconcile(f.requester).await.unwrap_err();
    assert!(error.starts_with("Unavailable { stage: RunLock"),"{error}");
    locked.rollback().await.unwrap();
    sqlx::query("REVOKE SELECT ON users FROM coralswarm_app").execute(&f.control.owner).await.unwrap();
    let error=f.reconcile(f.requester).await.unwrap_err();
    assert!(error.starts_with(if org { "Unavailable { stage: ControlRead" } else { "Unavailable { stage: Reachability" }),"{error}");
    assert_eq!(f.principal().await,f.requester);
    sqlx::query("GRANT SELECT ON users TO coralswarm_app").execute(&f.control.owner).await.unwrap();
    assert!(f.reconcile(f.requester).await.unwrap().starts_with("Changed"));
    f.close().await;
}
#[tokio::test]
async fn consumer_failure_is_not_absence_and_pool_is_released() { known_failure(false).await; }
#[tokio::test]
async fn org_failure_is_not_absence_and_pool_is_released() { known_failure(true).await; }

async fn team_prefix(org: bool) {
    let f = Fixture::new(org).await;
    sqlx::query("UPDATE graphs SET kind='team' WHERE id=$1").bind(f.graph).execute(f.owner()).await.unwrap();
    let team: Uuid = sqlx::query_scalar("INSERT INTO teams(account_id,graph_id,name,created_by) VALUES($1,$2,'Principal team',$3) RETURNING id")
        .bind(f.account).bind(f.graph).bind(f.requester).fetch_one(f.owner()).await.unwrap();
    for i in 1..=130 {
        f.member(Uuid::from_u128(i), "admin", "2026-01-01").await;
    }
    let beyond = Uuid::from_u128(130);
    sqlx::query("INSERT INTO team_members(team_id,user_id,role) VALUES($1,$2,'member')").bind(team).bind(beyond).execute(f.owner()).await.unwrap();
    if org {
        assert_eq!(f.reconcile(f.requester).await.unwrap_err(), "LookupIncomplete");
        sqlx::query("UPDATE corpus_enrichment_runs SET execution_principal=$2 WHERE run_id=$1").bind(f.run).bind(beyond).execute(f.owner()).await.unwrap();
    } else {
        assert_eq!(f.reconcile(f.requester).await.unwrap(), format!("Changed {{ from: {}, to: {beyond} }}", f.requester));
    }
    assert_eq!(f.reconcile(beyond).await.unwrap(), format!("Unchanged {{ principal: {beyond} }}"));
    let first_reachable = Uuid::from_u128(2);
    sqlx::query("INSERT INTO team_members(team_id,user_id,role) VALUES($1,$2,'member')").bind(team).bind(first_reachable).execute(f.owner()).await.unwrap();
    assert_eq!(f.reconcile(beyond).await.unwrap(), format!("Unchanged {{ principal: {beyond} }}"));
    sqlx::query("UPDATE team_members SET departing_at=now() WHERE team_id=$1 AND user_id=$2").bind(team).bind(beyond).execute(f.owner()).await.unwrap();
    assert_eq!(f.reconcile(beyond).await.unwrap(), format!("Changed {{ from: {beyond}, to: {first_reachable} }}"));
    sqlx::query("UPDATE team_members SET departing_at=now() WHERE team_id=$1").bind(team).execute(f.owner()).await.unwrap();
    if org { assert_eq!(f.reconcile(first_reachable).await.unwrap_err(), "LookupIncomplete"); }
    sqlx::query("DELETE FROM account_members WHERE account_id=$1 AND user_id=ANY($2::uuid[])")
        .bind(f.account).bind(vec![Uuid::from_u128(129),beyond]).execute(&f.control.owner).await.unwrap();
    assert_eq!(f.reconcile(first_reachable).await.unwrap(), "NoEligible(NoReachablePrivilegedMember)");
    assert_eq!(f.principal().await, first_reachable);
    f.close().await;
}
#[tokio::test]
async fn consumer_complete_set_selects_after_128_unreachable_admins() { team_prefix(false).await; }
#[tokio::test]
async fn org_prefix_sentinel_and_incumbent_outside_prefix() { team_prefix(true).await; }

async fn binding_errors(org: bool) {
    let f = Fixture::new(org).await;
    let invoke = |account, graph, run, profile: &'static str| test_standing_principal(f.tenant.clone(), f.app(), &f.control.app, account, graph, run, profile, f.requester);
    assert_eq!(invoke(f.account,f.graph,f.run,"unknown-profile").await.unwrap_err(), "InvalidBinding(Profile)");
    assert_eq!(invoke(f.account,Uuid::new_v4(),f.run,PROFILE).await.unwrap_err(), "InvalidBinding(GraphRun)");
    assert_eq!(invoke(f.account,f.graph,Uuid::new_v4(),PROFILE).await.unwrap_err(), "MissingData(Run)");
    assert_eq!(invoke(f.account,f.graph,f.run,"meeting-atoms-v1").await.unwrap_err(), "InvalidBinding(Profile)");
    if let TenantId::Org(org_id) = &f.tenant {
        sqlx::query("INSERT INTO org_revocations(clerk_org_id) VALUES($1)").bind(org_id).execute(&f.control.owner).await.unwrap();
        assert_eq!(f.reconcile(f.requester).await.unwrap(), "Denied(OrgRevoked)");
        sqlx::query("DELETE FROM org_revocations WHERE clerk_org_id=$1").bind(org_id).execute(&f.control.owner).await.unwrap();
        assert_eq!(test_standing_principal(TenantId::Org("wrong-org".into()),f.app(),&f.control.app,f.account,f.graph,f.run,PROFILE,f.requester).await.unwrap(), "Denied(AccountPlaneMismatch)");
    } else {
        let mut financial = coralswarm_backend::tenancy::scope::begin_account_scoped(&f.control.owner, f.account).await.unwrap();
        coralswarm_backend::billing::renewal::lock_period_rotation(&mut financial, f.account).await.unwrap();
        sqlx::query("DELETE FROM account_members WHERE account_id=$1").bind(f.account).execute(&mut *financial).await.unwrap();
        sqlx::query("UPDATE accounts SET clerk_org_id='new-org-binding' WHERE id=$1").bind(f.account).execute(&mut *financial).await.unwrap();
        financial.commit().await.unwrap();
        assert_eq!(f.reconcile(f.requester).await.unwrap(), "Denied(AccountPlaneMismatch)");
    }
    assert_eq!(f.principal().await, f.requester);
    f.close().await;
}
#[tokio::test]
async fn consumer_binding_errors_are_not_absence() { binding_errors(false).await; }
#[tokio::test]
async fn org_binding_errors_and_revocation_are_not_absence() { binding_errors(true).await; }

async fn wait_for_lock(pool: &PgPool, query: &str) {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let blocked: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE $1)")
                .bind(query).fetch_one(pool).await.unwrap();
            if blocked { break; }
            tokio::task::yield_now().await;
        }
    }).await.expect("operation reached deterministic lock barrier");
}

async fn competing_reconciliation(org: bool) {
    let f = Fixture::new(org).await;
    let successor = Uuid::new_v4();
    f.member(successor,"admin","2026-01-01").await;
    let concurrent = PgPoolOptions::new().max_connections(2).connect_with(f.app().connect_options().as_ref().clone()).await.unwrap();
    let concurrent_control = PgPoolOptions::new().max_connections(2).connect_with(f.control.app.connect_options().as_ref().clone()).await.unwrap();
    let reconcile = || test_standing_principal(f.tenant.clone(), &concurrent, &concurrent_control, f.account, f.graph, f.run, PROFILE, f.requester);
    let alerts=Arc::new(AtomicUsize::new(0));
    let subscriber=tracing::Dispatch::new(tracing_subscriber::registry().with(Alerts(alerts.clone())));
    let (first,second) = tokio::join!(reconcile().with_subscriber(subscriber.clone()),reconcile().with_subscriber(subscriber));
    assert_eq!(alerts.load(Ordering::SeqCst),1);
    concurrent.close().await;
    concurrent_control.close().await;
    let mut results = vec![first.unwrap(),second.unwrap()];
    results.sort();
    assert_eq!(results,vec![format!("Changed {{ from: {}, to: {successor} }}",f.requester),format!("Superseded {{ current: {successor} }}")]);
    for retired in [false,true] {
        let mut blocker = f.owner().begin().await.unwrap();
        sqlx::query("SELECT run_id FROM corpus_enrichment_runs WHERE run_id=$1 FOR UPDATE").bind(f.run).execute(&mut *blocker).await.unwrap();
        let result = async {
            wait_for_lock(f.owner(),"SELECT account_id,graph_id,extraction_version%").await;
            if retired {
                sqlx::query("UPDATE corpus_enrichment_runs SET retired_at=now() WHERE run_id=$1").bind(f.run).execute(&mut *blocker).await.unwrap();
            } else {
                sqlx::query("UPDATE corpus_enrichment_runs SET standing_pause_state='both',status='paused',pause_reason='operator_paused' WHERE run_id=$1").bind(f.run).execute(&mut *blocker).await.unwrap();
            }
            blocker.commit().await.unwrap();
        };
        let (observed,()) = tokio::join!(f.reconcile(successor),result);
        assert_eq!(observed.unwrap(),if retired { "Skipped(Retired)".to_string() } else { format!("Unchanged {{ principal: {successor} }}") });
    }
    let pause: (String,String,String) = sqlx::query_as("SELECT standing_pause_state,status,pause_reason FROM corpus_enrichment_runs WHERE run_id=$1").bind(f.run).fetch_one(f.owner()).await.unwrap();
    assert_eq!(pause,("both".into(),"paused".into(),"operator_paused".into()));
    f.close().await;
}
#[tokio::test]
async fn consumer_concurrent_reconciliation_pause_and_retirement() { competing_reconciliation(false).await; }
#[tokio::test]
async fn org_concurrent_reconciliation_pause_and_retirement() { competing_reconciliation(true).await; }

async fn commit_ack_loss(org: bool, hold_ack: bool) {
    use sqlx::postgres::PgSslMode;
    use tokio::{io::{AsyncReadExt,AsyncWriteExt}, net::{TcpListener,TcpStream}};
    use std::sync::atomic::AtomicBool;
    let f = Fixture::new(org).await;
    let successor = Uuid::new_v4();
    f.member(successor,"admin","2026-01-01").await;
    let options = f.app().connect_options().as_ref().clone();
    let remote = (options.get_host().to_string(),options.get_port());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let commit_seen=Arc::new(AtomicBool::new(false));
    let observed=commit_seen.clone();
    let proxy=tokio::spawn(async move {
        let (client,_) = listener.accept().await.unwrap();
        let server=TcpStream::connect(remote).await.unwrap();
        let (mut client_read,mut client_write)=client.into_split();
        let (mut server_read,mut server_write)=server.into_split();
        let outgoing=async {
            let mut buffer=[0u8;16384];
            let mut tail=Vec::new();
            loop {
                let size=client_read.read(&mut buffer).await.unwrap();
                if size==0 { return; }
                tail.extend_from_slice(&buffer[..size]);
                if tail.windows(6).any(|w| w==b"COMMIT") { observed.store(true,Ordering::SeqCst); }
                tail=tail[tail.len().saturating_sub(5)..].to_vec();
                if server_write.write_all(&buffer[..size]).await.is_err() { return; }
            }
        };
        let incoming=async {
            let mut buffer=[0u8;16384];
            loop {
                let size=server_read.read(&mut buffer).await.unwrap();
                if size==0 { return; }
                if observed.load(Ordering::SeqCst) {
                    if hold_ack { tokio::time::sleep(Duration::from_secs(3)).await; }
                    return;
                }
                if client_write.write_all(&buffer[..size]).await.is_err() { return; }
            }
        };
        tokio::select! { _=outgoing=>{}, _=incoming=>{} }
    });
    let proxy_pool=PgPoolOptions::new().max_connections(1).connect_with(options.host("127.0.0.1").port(port).ssl_mode(PgSslMode::Disable)).await.unwrap();
    let count=Arc::new(AtomicUsize::new(0));
    let result=test_standing_principal(f.tenant.clone(),&proxy_pool,&f.control.app,f.account,f.graph,f.run,PROFILE,f.requester)
        .with_subscriber(tracing_subscriber::registry().with(Alerts(count.clone()))).await;
    assert_eq!(result.unwrap_err(),"CommitOutcomeUnknown");
    assert!(commit_seen.load(Ordering::SeqCst));
    assert_eq!(count.load(Ordering::SeqCst),0);
    assert_eq!(f.principal().await,successor);
    assert_eq!(f.reconcile(f.requester).await.unwrap(),format!("Superseded {{ current: {successor} }}"));
    proxy_pool.close().await;
    proxy.await.unwrap();
    f.close().await;
}
#[tokio::test]
async fn consumer_lost_commit_ack_is_unknown_without_alert() { commit_ack_loss(false,false).await; }
#[tokio::test]
async fn org_lost_commit_ack_is_unknown_without_alert() { commit_ack_loss(true,false).await; }

async fn graph_observation_race(org: bool) {
    let f=Fixture::new(org).await;
    let first=Uuid::from_u128(10);
    let owner=Uuid::from_u128(20);
    f.member(first,"admin","2026-01-01").await;
    f.member(owner,"member","2026-01-01").await;
    let mut blocker=f.control.owner.begin().await.unwrap();
    sqlx::query("LOCK TABLE users IN ACCESS EXCLUSIVE MODE").execute(&mut *blocker).await.unwrap();
    let mutation=async {
        wait_for_lock(&f.control.owner,if org { "WITH members%" } else { "WITH bound%" }).await;
        if org {
            sqlx::query("UPDATE graphs SET kind='member',owner_user_id=$2 WHERE id=$1").bind(f.graph).bind(owner).execute(f.owner()).await.unwrap();
        } else {
            sqlx::query("UPDATE graphs SET archived_at=now() WHERE id=$1").bind(f.graph).execute(f.owner()).await.unwrap();
        }
        blocker.commit().await.unwrap();
    };
    let (result,())=tokio::join!(f.reconcile(f.requester),mutation);
    if org {
        assert_eq!(result.unwrap(),format!("Changed {{ from: {}, to: {owner} }}",f.requester));
        assert_eq!(f.principal().await,owner);
    } else {
        assert_eq!(result.unwrap_err(),"ObservationChanged");
        assert_eq!(f.principal().await,f.requester);
    }
    f.close().await;
}
#[tokio::test]
async fn consumer_graph_archive_race_never_combines_shapes() { graph_observation_race(false).await; }
#[tokio::test]
async fn org_graph_kind_and_owner_race_reobserves_control() { graph_observation_race(true).await; }

#[tokio::test]
async fn org_demotion_during_local_lookup_is_seen_by_second_control_observation() {
    let f=Fixture::new(true).await;
    let first=Uuid::from_u128(10);
    let second=Uuid::from_u128(20);
    f.member(first,"admin","2026-01-01").await;
    f.member(second,"admin","2026-01-01").await;
    sqlx::query("UPDATE graphs SET kind='team' WHERE id=$1").bind(f.graph).execute(f.owner()).await.unwrap();
    let team: Uuid=sqlx::query_scalar("INSERT INTO teams(account_id,graph_id,name,created_by) VALUES($1,$2,'Principal race',$3) RETURNING id")
        .bind(f.account).bind(f.graph).bind(f.requester).fetch_one(f.owner()).await.unwrap();
    for user in [first,second] {
        sqlx::query("INSERT INTO team_members(team_id,user_id,role) VALUES($1,$2,'member')").bind(team).bind(user).execute(f.owner()).await.unwrap();
    }
    sqlx::raw_sql("ALTER FUNCTION resolve_tenant_graph_reachable_by_user(uuid,uuid,uuid) RENAME TO principal_reachable_base; CREATE FUNCTION resolve_tenant_graph_reachable_by_user(g uuid,a uuid,u uuid) RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=public,pg_temp AS $$ BEGIN PERFORM pg_advisory_xact_lock(771123); RETURN principal_reachable_base(g,a,u); END $$; REVOKE ALL ON FUNCTION resolve_tenant_graph_reachable_by_user(uuid,uuid,uuid) FROM PUBLIC; GRANT EXECUTE ON FUNCTION resolve_tenant_graph_reachable_by_user(uuid,uuid,uuid) TO coralswarm_tenant_app;")
        .execute(f.owner()).await.unwrap();
    let mut blocker=f.owner().acquire().await.unwrap();
    sqlx::query("SELECT pg_advisory_lock(771123)").execute(&mut *blocker).await.unwrap();
    let mutation=async {
        wait_for_lock(f.owner(),"SELECT g.account_id,g.kind,g.owner_user_id%").await;
        sqlx::query("UPDATE account_members SET role='member' WHERE account_id=$1 AND user_id=$2").bind(f.account).bind(first).execute(&f.control.owner).await.unwrap();
        sqlx::query("SELECT pg_advisory_unlock(771123)").execute(&mut *blocker).await.unwrap();
    };
    let (result,())=tokio::join!(f.reconcile(f.requester),mutation);
    assert_eq!(result.unwrap(),format!("Changed {{ from: {}, to: {second} }}",f.requester));
    assert_eq!(f.principal().await,second);
    f.close().await;
}

async fn handoff_before_lifecycle_and_late_revocation(org: bool) {
    let f=Fixture::new(org).await;
    let excluded=f.excluded_snapshot().await;
    sqlx::raw_sql("CREATE FUNCTION principal_update_barrier() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock(771122); RETURN NEW; END $$; CREATE TRIGGER principal_update_barrier BEFORE UPDATE ON corpus_enrichment_runs FOR EACH ROW WHEN (OLD.execution_principal IS DISTINCT FROM NEW.execution_principal) EXECUTE FUNCTION principal_update_barrier();")
        .execute(f.owner()).await.unwrap();
    let mut incumbent=f.requester;
    for retired in [false,true] {
        let successor=Uuid::new_v4();
        f.member(successor,"admin","2026-01-01").await;
        let mut blocker=f.owner().acquire().await.unwrap();
        sqlx::query("SELECT pg_advisory_lock(771122)").execute(&mut *blocker).await.unwrap();
        let mutation=async {
            wait_for_lock(f.owner(),"UPDATE corpus_enrichment_runs SET execution_principal%").await;
            let pool=f.owner().clone();
            let run=f.run;
            let lifecycle=tokio::spawn(async move {
                sqlx::query(if retired { "UPDATE corpus_enrichment_runs SET retired_at=now() WHERE run_id=$1" }
                    else { "UPDATE corpus_enrichment_runs SET standing_pause_state='both',status='paused',pause_reason='operator_paused' WHERE run_id=$1" })
                    .bind(run).execute(&pool).await.unwrap();
            });
            wait_for_lock(f.owner(),if retired { "UPDATE corpus_enrichment_runs SET retired_at%" } else { "UPDATE corpus_enrichment_runs SET standing_pause_state%" }).await;
            sqlx::query("UPDATE account_members SET role='member' WHERE account_id=$1 AND user_id=$2").bind(f.account).bind(successor).execute(&f.control.owner).await.unwrap();
            sqlx::query("SELECT pg_advisory_unlock(771122)").execute(&mut *blocker).await.unwrap();
            lifecycle.await.unwrap();
        };
        let (result,())=tokio::join!(f.reconcile(incumbent),mutation);
        assert_eq!(result.unwrap(),format!("Changed {{ from: {incumbent}, to: {successor} }}"));
        assert_eq!(f.principal().await,successor);
        assert_eq!(f.excluded_snapshot().await,excluded);
        if !retired { assert_eq!(f.reconcile(successor).await.unwrap(),"NoEligible(NoCurrentPrivilegedMember)"); }
        incumbent=successor;
    }
    let state:(String,String,bool)=sqlx::query_as("SELECT standing_pause_state,status,retired_at IS NOT NULL FROM corpus_enrichment_runs WHERE run_id=$1").bind(f.run).fetch_one(f.owner()).await.unwrap();
    assert_eq!(state,("both".into(),"paused".into(),true));
    assert_eq!(f.reconcile(incumbent).await.unwrap(),"Skipped(Retired)");
    f.close().await;
}
#[tokio::test]
async fn consumer_handoff_before_pause_retire_and_post_observation_revocation() { handoff_before_lifecycle_and_late_revocation(false).await; }
#[tokio::test]
async fn org_handoff_before_pause_retire_and_post_observation_revocation() { handoff_before_lifecycle_and_late_revocation(true).await; }

async fn known_commit_rollback(org: bool) {
    let f=Fixture::new(org).await;
    f.member(Uuid::new_v4(),"admin","2026-01-01").await;
    for code in ["23514","40003"] {
        sqlx::raw_sql(&format!("CREATE OR REPLACE FUNCTION principal_commit_reject() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION USING ERRCODE='{code}',MESSAGE='principal commit rejected'; END $$;"))
            .execute(f.owner()).await.unwrap();
        if code=="23514" {
            sqlx::raw_sql("CREATE CONSTRAINT TRIGGER principal_commit_reject AFTER UPDATE ON corpus_enrichment_runs DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION principal_commit_reject();")
                .execute(f.owner()).await.unwrap();
        }
        let error=f.reconcile(f.requester).await.unwrap_err();
        if code=="23514" { assert!(error.starts_with("Unavailable { stage: Commit"),"{error}"); }
        else { assert_eq!(error,"CommitOutcomeUnknown"); }
        assert_eq!(f.principal().await,f.requester);
    }
    f.close().await;
}
#[tokio::test]
async fn consumer_commit_rejection_is_known_rollback_without_alert() { known_commit_rollback(false).await; }
#[tokio::test]
async fn org_commit_rejection_is_known_rollback_without_alert() { known_commit_rollback(true).await; }

#[tokio::test]
async fn consumer_commit_ack_deadline_is_unknown_without_alert() { commit_ack_loss(false,true).await; }
#[tokio::test]
async fn org_commit_ack_deadline_is_unknown_without_alert() { commit_ack_loss(true,true).await; }

async fn no_content_or_money_authority(org: bool) {
    let f=Fixture::with_legacy_unit(org).await;
    f.seed_unit().await;
    let successor=Uuid::new_v4();
    f.member(successor,"admin","2026-01-01").await;
    let before=f.excluded_snapshot().await;
    let base=if org { "coralswarm_tenant_app" } else { "coralswarm_app" };
    sqlx::query(&format!("REVOKE ALL ON source_items,source_versions,atoms,jobs,managed_attempt_receipts,managed_budget_periods,llm_usage FROM {base}"))
        .execute(f.owner()).await.unwrap();
    let denied: bool=sqlx::query_scalar("SELECT NOT has_table_privilege(current_user,'source_versions','SELECT') AND NOT has_table_privilege(current_user,'managed_attempt_receipts','INSERT') AND NOT has_table_privilege(current_user,'jobs','INSERT')")
        .fetch_one(f.app()).await.unwrap();
    assert!(denied);
    assert_eq!(f.reconcile(f.requester).await.unwrap(),format!("Changed {{ from: {}, to: {successor} }}",f.requester));
    assert_eq!(f.excluded_snapshot().await,before);
    f.close().await;
}
#[tokio::test]
async fn consumer_reconciliation_needs_no_original_queue_or_money_access() { no_content_or_money_authority(false).await; }
#[tokio::test]
async fn org_reconciliation_needs_no_original_queue_or_money_access() { no_content_or_money_authority(true).await; }

#[tokio::test]
async fn org_existing_requester_and_reconcile_pool_pressure_is_bounded() {
    use coralswarm_backend::{corpus_enrichment,tenancy::scope};
    let f=Fixture::with_legacy_unit(true).await;
    let version=f.seed_unit().await;
    let successor=Uuid::new_v4();
    f.member(successor,"admin","2026-01-01").await;
    let legacy_pool=PgPoolOptions::new().max_connections(1).connect_with(f.app().connect_options().as_ref().clone()).await.unwrap();
    let mut legacy=scope::begin_tenant_system_scoped(&legacy_pool,f.account).await.unwrap();
    sqlx::query("SELECT run_id FROM corpus_enrichment_runs WHERE run_id=$1 FOR SHARE").bind(f.run).execute(&mut *legacy).await.unwrap();
    let existing_caller=async {
        wait_for_lock(f.owner(),"SELECT account_id,graph_id,extraction_version%").await;
        let allowed=corpus_enrichment::requester_allowed_tx(&mut legacy,&f.control.app,&f.tenant,f.graph,version,PROFILE).await.unwrap();
        assert!(allowed);
        legacy.rollback().await.unwrap();
    };
    let (result,())=tokio::time::timeout(Duration::from_secs(3),async {
        tokio::join!(f.reconcile(f.requester),existing_caller)
    }).await.expect("bounded contention releases CONTROL for the existing requester caller");
    assert!(result.unwrap_err().starts_with("Unavailable { stage: RunLock"));
    assert_eq!(f.principal().await,f.requester);
    assert_eq!(f.reconcile(f.requester).await.unwrap(),format!("Changed {{ from: {}, to: {successor} }}",f.requester));
    legacy_pool.close().await;
    f.close().await;
}
