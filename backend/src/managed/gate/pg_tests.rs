use super::*;
use crate::tenancy::TenantId;
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    PgConnection, PgPool,
};
use std::{str::FromStr, time::Duration};

struct Database {
    owner: PgPool,
    app: PgPool,
    /// Shared: a test may clone a fixture's handles into a second view.
    db: std::sync::Arc<crate::test_db::TestDatabase>,
    role: String,
}
struct Fixture {
    admin: PgPool,
    control: Database,
    content: Option<Database>,
    tenant: TenantId,
    account: Uuid,
    payer: Uuid,
    graph: Uuid,
    version: Uuid,
    unit: Uuid,
}

impl Database {
    async fn new(admin: &PgPool, options: &PgConnectOptions, tenant: bool) -> Self {
        let db = std::sync::Arc::new(crate::test_db::TestDatabase::new(options, "cor22_finance"));
        let role = db.role("cor22_finance_role");
        sqlx::query(&format!("CREATE DATABASE {db}"))
            .execute(admin)
            .await
            .unwrap();
        let owner = PgPoolOptions::new()
            .max_connections(5)
            .connect_with(options.clone().database(&db))
            .await
            .unwrap();
        let full = if tenant { sqlx::migrate!("./migrations_tenant") } else { sqlx::migrate!("./migrations_control") };
        let before_fence=sqlx::migrate::Migrator {
            migrations:std::borrow::Cow::Owned(full.iter().filter(|m|m.version < if tenant {153} else {321}).cloned().collect()),
            ignore_missing:false,locking:true,no_tx:false,
        };
        before_fence.run(&owner).await.unwrap();
        let mut connection = owner.acquire().await.unwrap();
        sqlx::query("SELECT set_config('app.identity_serving_contract','composite-v1',false)")
            .execute(&mut *connection)
            .await
            .unwrap();
        sqlx::raw_sql(include_str!(
            "../../../schema/identity-contract.sql"
        ))
        .execute(&mut *connection)
        .await
        .unwrap();
        drop(connection);
        let password = Uuid::new_v4().simple().to_string();
        sqlx::query(&format!("CREATE ROLE {role} LOGIN INHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '{password}'"))
            .execute(admin).await.unwrap();
        let base = if tenant {
            "coralswarm_tenant_app"
        } else {
            "coralswarm_app"
        };
        sqlx::query(&format!("GRANT {base} TO {role}"))
            .execute(admin)
            .await
            .unwrap();
        let app = PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(Duration::from_secs(3))
            .connect_with(
                options
                    .clone()
                    .database(&db)
                    .username(&role)
                    .password(&password),
            )
            .await
            .unwrap();
        let restricted: bool = sqlx::query_scalar(
            "SELECT NOT rolsuper AND NOT rolbypassrls AND NOT rolcreatedb AND NOT rolcreaterole \
                AND NOT rolreplication AND NOT EXISTS(SELECT 1 FROM pg_class WHERE relowner=pg_roles.oid) \
               FROM pg_roles WHERE rolname=current_user",
        ).fetch_one(&app).await.unwrap();
        assert!(
            restricted,
            "normal LOGIN must own no tables and carry no unsafe attributes"
        );
        Self {
            owner,
            app,
            db,
            role,
        }
    }
    async fn finish(self) {
        self.app.close().await;
        self.owner.close().await;
    }
}

impl Fixture {
    async fn new(org: bool) -> Self { Self::at_boundary(org,true).await }
    /// COR-114's shape (prod account 78d0d2ba): a personal org. It lives on
    /// the consumer plane, has a `clerk_org_id` (so `is_org_account`) and a
    /// `personal_orgs` row, and no tenant database.
    async fn personal_org() -> Self { Self::shaped(false,true,true).await }
    async fn at_boundary(org: bool, install_fence: bool) -> Self { Self::shaped(org,false,install_fence).await }
    async fn shaped(org: bool, personal: bool, install_fence: bool) -> Self {
        let url = std::env::var("ORGT_TEST_BASE_URL")
            .expect("isolated localhost ORGT_TEST_BASE_URL required");
        let options = PgConnectOptions::from_str(&url).unwrap();
        assert!(matches!(
            options.get_host(),
            "localhost" | "127.0.0.1" | "::1"
        ));
        let admin = PgPoolOptions::new()
            .max_connections(3)
            .connect_with(options.clone())
            .await
            .unwrap();
        sqlx::raw_sql("DO $$ BEGIN IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='coralswarm_app') THEN CREATE ROLE coralswarm_app NOLOGIN NOSUPERUSER NOBYPASSRLS; END IF; IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='coralswarm_tenant_app') THEN CREATE ROLE coralswarm_tenant_app NOLOGIN NOSUPERUSER NOBYPASSRLS; END IF; END $$;")
            .execute(&admin).await.unwrap();
        let control = Database::new(&admin, &options, false).await;
        let content = if org {
            Some(Database::new(&admin, &options, true).await)
        } else {
            None
        };
        let account = Uuid::new_v4();
        let payer = Uuid::new_v4();
        let org_id = format!("org_finance_{}", account.simple());
        sqlx::query("INSERT INTO users(id,email,clerk_user_id) VALUES($1,$2,$2)")
            .bind(payer)
            .bind(format!("{payer}@finance.test"))
            .execute(&control.owner)
            .await
            .unwrap();
        sqlx::query("INSERT INTO accounts(id,owner_user_id,clerk_org_id) VALUES($1,$2,$3)")
            .bind(account)
            .bind(payer)
            .bind(if org || personal { Some(&org_id) } else { None })
            .execute(&control.owner)
            .await
            .unwrap();
        if personal {
            sqlx::query("INSERT INTO personal_orgs(clerk_org_id,clerk_user_id) VALUES($1,$2)")
                .bind(&org_id)
                .bind(format!("{payer}@finance.test"))
                .execute(&control.owner)
                .await
                .unwrap();
        }
        if let Some(content)=&content {
            sqlx::query("INSERT INTO tenant_meta(org_id) VALUES($1)").bind(&org_id)
                .execute(&content.owner).await.unwrap();
            let mut financial=begin_account_scoped(&control.owner,account).await.unwrap();
            sqlx::query("INSERT INTO tenant_databases(clerk_org_id,account_id,slug,db_secret_ref,status,schema_version) VALUES($1,$2,$3,$4,'active',153)")
                .bind(&org_id).bind(account).bind(format!("fin-{}",&account.simple().to_string()[..20]))
                .bind(format!("TENANT_FIN_{}",account.simple())).execute(&mut *financial).await.unwrap();
            financial.commit().await.unwrap();
        }
        sqlx::query("INSERT INTO account_members(account_id,user_id,role,seat_tier_id) VALUES($1,$2,'owner', \
            (SELECT CASE WHEN clerk_org_id IS NOT NULL THEN (SELECT id FROM seat_tiers WHERE code='standard' AND is_active) END FROM accounts WHERE id=$1))")
            .bind(account).bind(payer).execute(&control.owner).await.unwrap();
        let pool = content.as_ref().map(|d| &d.owner).unwrap_or(&control.owner);
        let graph = sqlx::query_scalar(
            "INSERT INTO graphs(account_id,name) VALUES($1,'funded dispatch') RETURNING id",
        )
        .bind(account)
        .fetch_one(pool)
        .await
        .unwrap();
        let run: Uuid = sqlx::query_scalar("INSERT INTO corpus_enrichment_runs(graph_id,account_id,requested_by,mode,corpus_kind,status,extraction_version,model,max_items,max_characters,max_model_calls) VALUES($1,$2,$3,'execute','real','running','source-atoms-v1','deepseek/deepseek-v4-flash-0731',1000,8000000,10000) RETURNING run_id")
            .bind(graph).bind(account).bind(payer).fetch_one(pool).await.unwrap();
        let app = content.as_ref().map(|d| &d.app).unwrap_or(&control.app);
        let mut tx = if org {
            crate::tenancy::scope::begin_tenant_system_scoped(app, account)
                .await
                .unwrap()
        } else {
            begin_account_scoped(app, account).await.unwrap()
        };
        let source = crate::source_evidence::CaptureSource {
            source_type: "note",
            provider_namespace: "fixture:finance",
            source_id: "retained-finance",
            normalized_text: "A real retained source backs this managed attempt.",
            representation: "original_text",
            metadata: &serde_json::json!({}),
            occurred_at: None,
            participants: &[],
            ingested_by: Some(payer),
            scope: "private",
            duration_ms: None,
            segments: None,
        };
        // The fixture's schema and run are pinned v1 (from before v2): the
        // retained version is the v1 capture of that time.
        let saved = crate::source_evidence::capture_historical_v1_tx(
            &mut tx, graph, account, &source, &[43; 32],
        )
        .await
        .unwrap();
        let version = saved.version_id;
        let unit = sqlx::query_scalar("INSERT INTO corpus_enrichment_units(run_id,graph_id,kind,holding_id,source_item_id,extraction_version,state,execution_owner) SELECT $1,graph_id,'retained_version',version_id,source_item_id,extraction_version,'pending',true FROM source_versions WHERE version_id=$2 RETURNING unit_id")
            .bind(run).bind(version).fetch_one(&mut *tx).await.unwrap();
        tx.commit().await.unwrap();
        let fixture=Self {
            admin, control, content,
            tenant: if org { TenantId::Org(org_id) } else { TenantId::Consumer },
            account, payer, graph, version, unit,
        };
        if install_fence { fixture.install_fence().await; }
        fixture
    }
    fn app(&self) -> &PgPool {
        self.content
            .as_ref()
            .map(|d| &d.app)
            .unwrap_or(&self.control.app)
    }
    fn owner(&self) -> &PgPool {
        self.content
            .as_ref()
            .map(|d| &d.owner)
            .unwrap_or(&self.control.owner)
    }
    fn plane(&self) -> JobScope<'_> {
        match self.tenant {
            TenantId::Consumer => JobScope::Consumer {
                pool: self.app(),
                account_id: self.account,
            },
            TenantId::Org(_) => JobScope::Org(self.app()),
        }
    }
    fn call<'a>(&'a self, plane: &'a JobScope<'a>) -> ManagedCall<'a> {
        ManagedCall {
            plane,
            control: &self.control.app,
            tenant: &self.tenant,
            account_id: self.account,
            principal: self.payer,
            fallback_model: None,
            subject: Subject::Window {
                graph: self.graph,
                version: self.version,
                profile: "source-atoms-v1",
                job: Uuid::new_v4(),
            },
        }
    }
    async fn finish(self) {
        if let Some(content) = self.content {
            content.finish().await;
        }
        self.control.finish().await;
        self.admin.close().await;
    }
}

fn completion_request() -> CompletionRequest {
    CompletionRequest {
        messages: vec![crate::llm::Message {
            role: crate::llm::Role::User,
            content: "retained evidence".into(),
        }],
        model: "deepseek/deepseek-v4-flash-0731".into(),
        max_tokens: 64,
        ..Default::default()
    }
}

async fn no_funding(org: bool) {
    let f = Fixture::new(org).await;
    let plane = f.plane();
    let gate = ManagedGate::open(f.call(&plane), &completion_request())
        .await
        .unwrap();
    let result = coralswarm_core::send_gate::acquire(&gate, 1).await;
    assert!(matches!(result,Err(Deny::Hold(HoldCode::CreditsExhausted))),
        "missing all finite allowance and pool funding must never mint a send permit; actual={result:?}");
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM managed_attempt_receipts WHERE dispatch_phase='armed'"
        )
        .fetch_one(f.owner())
        .await
        .unwrap(),
        0
    );
    drop(gate);
    drop(plane);
    f.finish().await;
}

#[tokio::test]
async fn consumer_no_funding_mints_no_permit() {
    no_funding(false).await;
}
#[tokio::test]
async fn tenant_no_funding_mints_no_permit() {
    no_funding(true).await;
}

/// Extraction v2's Jev seat is an aside call: a second logical call on a
/// STANDING window. The gate refuses it for any other subject before any
/// write, because only a standing window's holds are not written by the gate
/// (a held aside call must never pause the cohort window it rides on).
async fn aside_requires_a_standing_window(org: bool) {
    let f = Fixture::new(org).await;
    let plane = f.plane();
    let aside = ManagedGate::open_aside(f.call(&plane), &completion_request()).await;
    assert!(matches!(aside, Err(Deny::Unavailable)), "aside on a cohort window must be refused");
    let receipts: i64 = sqlx::query_scalar("SELECT count(*) FROM managed_attempt_receipts")
        .fetch_one(f.owner()).await.unwrap();
    assert_eq!(receipts, 0, "a refused aside call writes nothing");
    let unit_charge: i64 = sqlx::query_scalar("SELECT charged_micro_usd FROM corpus_enrichment_units WHERE unit_id=$1")
        .bind(f.unit).fetch_one(f.owner()).await.unwrap();
    assert_eq!(unit_charge, 0);
    // The window's own call still opens normally.
    assert!(ManagedGate::open(f.call(&plane), &completion_request()).await.is_ok());
    drop(plane);
    f.finish().await;
}

#[tokio::test]
async fn consumer_aside_call_requires_a_standing_window() {
    aside_requires_a_standing_window(false).await;
}
#[tokio::test]
async fn tenant_aside_call_requires_a_standing_window() {
    aside_requires_a_standing_window(true).await;
}

/// Source extraction's reasoning-off fallback is priced by the migrated
/// control `model_rates` sheet, read by the owner (prod) and the app role
/// (CI), on both planes. Without its row the gate refuses `UnpricedModel`.
async fn extraction_fallback_is_priced(org: bool) {
    let f = Fixture::new(org).await;
    let v41 = "deepseek/deepseek-v4.1-flash";
    let now = chrono::Utc::now();
    for control in [&f.control.owner, &f.control.app] {
        let sheet = PriceSheet::load(control).await.unwrap();
        let rate = sheet.rate("openrouter", v41, now);
        assert!(rate.matched, "v4.1-flash has its own model_rates row");
        assert!((rate.input_per_million - 0.11).abs() < 1e-12 && (rate.output_per_million - 0.34).abs() < 1e-12);
        let old = sheet.rate("openrouter", "deepseek/deepseek-v4-flash-0731", now);
        assert!((old.input_per_million - 0.08).abs() < 1e-12, "the v4.1 pattern must not capture 0731");
    }
    let owner_plane = match f.tenant {
        TenantId::Consumer => JobScope::Consumer { pool: f.owner(), account_id: f.account },
        TenantId::Org(_) => JobScope::Org(f.owner()),
    };
    let app_plane = f.plane();
    for (plane, control) in [(&app_plane, &f.control.app), (&owner_plane, &f.control.owner)] {
        let mut call = f.call(plane);
        call.control = control;
        call.fallback_model = Some(v41.to_string());
        let gate = ManagedGate::open(call, &completion_request()).await.unwrap();
        assert_eq!(gate.worst.terms.models, vec!["deepseek/deepseek-v4-flash-0731".to_string(), v41.to_string()]);
        let ceiling = gate.worst.ceiling();
        assert!((ceiling.prompt - 0.22).abs() < 1e-12 && (ceiling.completion - 0.68).abs() < 1e-12,
            "v4.1's card times the default 2.0 headroom keeps its fp8 ZDR hosts eligible: {ceiling:?}");
        let mut other = f.call(plane);
        other.control = control;
        let gate = ManagedGate::open(other, &completion_request()).await.unwrap();
        assert_eq!(gate.worst.terms.models, vec!["deepseek/deepseek-v4-flash-0731".to_string(), "z-ai/glm-5.3-flash".to_string()],
            "a call that names no fallback keeps the global map");
    }
    drop(app_plane);
    drop(owner_plane);
    f.finish().await;
}

#[tokio::test]
async fn consumer_extraction_fallback_is_priced() {
    extraction_fallback_is_priced(false).await;
}
#[tokio::test]
async fn tenant_extraction_fallback_is_priced() {
    extraction_fallback_is_priced(true).await;
}

use crate::billing::managed_reservation::{self as credit, FundingRequest, ReserveOutcome};
use sqlx::types::Decimal;

impl Fixture {
    async fn balance(&self, credits: Decimal) {
        { let mut seed_tx=self.financial().await; let value=sqlx::query("INSERT INTO credit_balances(account_id,balance) VALUES($1,$2) ON CONFLICT(account_id) DO UPDATE SET balance=EXCLUDED.balance")
            .bind(self.account).bind(credits).execute(&mut *seed_tx).await.unwrap(); seed_tx.commit().await.unwrap(); value };
    }
    async fn allowance(&self, credits: Decimal) -> Uuid {
        { let mut seed_tx=self.financial().await; let value=sqlx::query_scalar("INSERT INTO member_allowance_periods(account_id,user_id,period_start,period_end,granted_credits) VALUES($1,$2,now()-interval '2 days',now()+interval '1 day',$3) RETURNING id")
            .bind(self.account).bind(self.payer).bind(credits).fetch_one(&mut *seed_tx).await.unwrap(); seed_tx.commit().await.unwrap(); value }
    }
    fn funding(&self, bound_micro_usd: i64) -> FundingRequest {
        FundingRequest {
            plane: match &self.tenant {
                TenantId::Consumer => "consumer".into(),
                TenantId::Org(org) => format!("org:{org}"),
            },
            receipt: Uuid::new_v4(),
            account: self.account,
            payer: self.payer,
            graph: self.graph,
            logical_call: Uuid::new_v4(),
            request_hash: "a".repeat(64),
            bound_micro_usd,
        }
    }
    async fn install_fence(&self) {
        sqlx::migrate!("./migrations_control").run(&self.control.owner).await.unwrap();
        if let Some(content)=&self.content {
            sqlx::migrate!("./migrations_tenant").run(&content.owner).await.unwrap();
        }
    }
    async fn financial(&self) -> Transaction<'static,Postgres> {
        let mut tx=begin_account_scoped(&self.control.owner,self.account).await.unwrap();
        crate::billing::renewal::lock_period_rotation(&mut tx,self.account).await.unwrap();
        tx
    }
    fn legacy_context(&self) -> Option<(PgPool,String)> {
        match (&self.content,&self.tenant) {
            (Some(content),TenantId::Org(org)) => Some((content.app.clone(),org.clone())),
            _ => None,
        }
    }
    async fn reserve_credit(&self, request: &FundingRequest) -> ReserveOutcome {
        let mut tx = begin_account_scoped(&self.control.app, self.account)
            .await
            .unwrap();
        let outcome = reserve_with_content(&mut tx, request, self.legacy_context()).await.unwrap();
        tx.commit().await.unwrap();
        outcome
    }
}

async fn reserve_with_content(
    financial: &mut PgConnection, request: &FundingRequest, context: Option<(PgPool,String)>,
) -> Result<ReserveOutcome,sqlx::Error> {
    crate::billing::renewal::lock_period_rotation(financial,request.account).await?;
    match context {
        Some((pool,org)) => {
            let mut content=crate::tenancy::scope::begin_tenant_system_scoped(&pool,request.account)
                .await.map_err(|e|sqlx::Error::Protocol(e.to_string()))?;
            let outcome=credit::reserve_tx(financial,request,crate::workers::billing_derivation::LegacyContent::Org { content:&mut content,org:&org }).await;
            content.commit().await?;
            outcome
        },
        None => credit::reserve_tx(financial,request,crate::workers::billing_derivation::LegacyContent::Control).await,
    }
}

async fn funding_projection(org: bool) {
    let f = Fixture::new(org).await;
    sqlx::query("INSERT INTO credit_conversion(effective_from,reference_cost_per_credit,note) VALUES(now(),1,'isolated financial fixture')")
        .execute(&f.control.owner).await.unwrap();
    let first = f.funding(4_000_000);
    assert!(matches!(
        f.reserve_credit(&first).await,
        ReserveOutcome::Unfunded
    ));
    let period = f.allowance(Decimal::from(10)).await;
    let ReserveOutcome::Held(binding) = f.reserve_credit(&first).await else {
        panic!("finite allowance must fund without a balance row")
    };
    assert_eq!(binding.member_period_id, Some(period));
    assert_eq!(binding.bound_credits, Decimal::from(4));
    assert_eq!(binding.funding_basis, "allowance");
    let ReserveOutcome::Held(duplicate) = f.reserve_credit(&first).await else {
        panic!("same binding must converge")
    };
    assert_eq!(
        duplicate.conversion_effective_from,
        binding.conversion_effective_from
    );
    let mut wrong = first.clone();
    wrong.payer = Uuid::new_v4();
    let mut tx = begin_account_scoped(&f.control.app, f.account)
        .await
        .unwrap();
    assert!(reserve_with_content(&mut tx, &wrong, f.legacy_context()).await.is_err());
    tx.rollback().await.unwrap();
    let mut rebound = first.clone();
    rebound.receipt = Uuid::new_v4();
    rebound.payer = Uuid::new_v4();
    let mut tx = begin_account_scoped(&f.control.app, f.account)
        .await
        .unwrap();
    assert!(
        reserve_with_content(&mut tx, &rebound, f.legacy_context()).await.is_err(),
        "a different receipt cannot rebind the same logical-call payer"
    );
    tx.rollback().await.unwrap();
    let mut tx = begin_account_scoped(&f.control.app, f.account)
        .await
        .unwrap();
    crate::billing::renewal::lock_period_rotation(&mut tx,f.account).await.unwrap();
    let mutation =
        sqlx::query("UPDATE managed_credit_reservations SET payer_id=$2 WHERE receipt_id=$1")
            .bind(first.receipt)
            .bind(Uuid::new_v4())
            .execute(&mut *tx)
            .await
            .unwrap_err();
    assert_eq!(
        mutation
            .as_database_error()
            .and_then(|e| e.code())
            .as_deref(),
        Some("P0001"),
        "normal app writes cannot rebind a financial receipt"
    );
    tx.rollback().await.unwrap();
    f.balance(Decimal::from(5)).await;
    let mut second = f.funding(2_000_000);
    second.payer = Uuid::new_v4();
    assert!(
        matches!(f.reserve_credit(&second).await, ReserveOutcome::Unfunded),
        "ALL outstanding bounds, including allowance-backed bounds, reduce the account projection"
    );
    let mut tx = begin_account_scoped(&f.control.app, f.account)
        .await
        .unwrap();
    assert!(credit::release_unsent_tx(&mut tx, &first).await.unwrap());
    tx.commit().await.unwrap();
    assert!(
        matches!(f.reserve_credit(&first).await, ReserveOutcome::Exhausted),
        "a cancelled receipt never gains another hold"
    );
    assert!(matches!(
        f.reserve_credit(&second).await,
        ReserveOutcome::Held(_)
    ));
    let cancelled = f.funding(1_000_000);
    let mut tx = begin_account_scoped(&f.control.app, f.account)
        .await
        .unwrap();
    assert!(credit::release_unsent_tx(&mut tx, &cancelled)
        .await
        .unwrap());
    tx.commit().await.unwrap();
    assert!(
        matches!(
            f.reserve_credit(&cancelled).await,
            ReserveOutcome::Exhausted
        ),
        "cancel-before-delayed-reserve tombstone wins"
    );
    let row: (Option<Decimal>,String)=sqlx::query_as("SELECT reference_cost_per_credit,state FROM managed_credit_reservations WHERE receipt_id=$1")
        .bind(cancelled.receipt).fetch_one(&f.control.owner).await.unwrap();
    assert_eq!(row, (None, "released".into()));
    { let mut seed_tx=f.financial().await; let value=sqlx::query(
        "UPDATE member_allowance_periods SET consumed_credits=10,cap_credits=0 WHERE id=$1",
    )
    .bind(period)
    .execute(&mut *seed_tx)
    .await
    .unwrap(); seed_tx.commit().await.unwrap(); value };
    assert!(
        matches!(
            f.reserve_credit(&f.funding(1_000_000)).await,
            ReserveOutcome::Unfunded
        ),
        "explicit member overage cap blocks even with a funded pool"
    );
    { let mut seed_tx=f.financial().await; let value=sqlx::query("UPDATE member_allowance_periods SET cap_credits=2 WHERE id=$1")
        .bind(period)
        .execute(&mut *seed_tx)
        .await
        .unwrap(); seed_tx.commit().await.unwrap(); value };
    let ReserveOutcome::Held(pool) = f.reserve_credit(&f.funding(1_000_000)).await else {
        panic!("available pool and cap must fund the whole bound")
    };
    assert_eq!(pool.funding_basis, "pool");
    { let mut seed_tx=f.financial().await; let value=sqlx::query("INSERT INTO stripe_subscriptions(account_id,stripe_customer_id,status) VALUES($1,$2,'canceled')")
        .bind(f.account).bind(format!("cus_projection_{}",f.account.simple())).execute(&mut *seed_tx).await.unwrap(); seed_tx.commit().await.unwrap(); value };
    assert!(
        matches!(
            f.reserve_credit(&f.funding(1_000_000)).await,
            ReserveOutcome::Unfunded
        ),
        "frozen pool cannot fund a bound even when its projected balance and member cap fit"
    );
    if org {
        { let mut seed_tx=f.financial().await; let value=sqlx::query("INSERT INTO grants(account_id,plan_id,seats,plan_is_seat_priced) SELECT $1,id,1,true FROM plans WHERE code='team_standard'")
            .bind(f.account).execute(&mut *seed_tx).await.unwrap(); seed_tx.commit().await.unwrap(); value };
        { let mut seed_tx=f.financial().await; let value=sqlx::query("UPDATE stripe_subscriptions SET status='active' WHERE account_id=$1")
            .bind(f.account)
            .execute(&mut *seed_tx)
            .await
            .unwrap(); seed_tx.commit().await.unwrap(); value };
        { let mut seed_tx=f.financial().await; let value=sqlx::query(
            "UPDATE member_allowance_periods SET consumed_credits=20,cap_credits=NULL WHERE id=$1",
        )
        .bind(period)
        .execute(&mut *seed_tx)
        .await
        .unwrap(); seed_tx.commit().await.unwrap(); value };
        f.balance(Decimal::from(100)).await;
        assert!(
            matches!(
                f.reserve_credit(&f.funding(1_000_000)).await,
                ReserveOutcome::Unfunded
            ),
            "default seat overage cap includes held bounds and cannot eat a funded account pool"
        );
    }
    f.finish().await;
}

#[tokio::test]
async fn consumer_finite_funding_and_all_bound_projection() {
    funding_projection(false).await;
}
#[tokio::test]
async fn tenant_finite_funding_and_all_bound_projection() {
    funding_projection(true).await;
}

async fn loopback_and_pinned_settlement(org: bool) {
    use crate::llm::{openai::OpenAiProvider, GatedLlm};
    use axum::{routing::post, Json, Router};
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    let f = Fixture::new(org).await;
    let period = f.allowance(Decimal::from(10)).await;
    let count = Arc::new(AtomicUsize::new(0));
    let observed = count.clone();
    let owner = f.control.owner.clone();
    let account = f.account;
    let payer = f.payer;
    let router=Router::new().route("/v1/chat/completions",post(move |Json(body):Json<serde_json::Value>|{
        let observed=observed.clone();let owner=owner.clone();async move {
            observed.fetch_add(1,Ordering::SeqCst);
            assert_eq!(body["model"],"deepseek/deepseek-v4-flash-0731");
            assert_eq!(body["models"],serde_json::json!(["z-ai/glm-5.3-flash"]));
            assert_eq!(body["provider"]["max_price"]["request"],0);
            assert!(body["provider"]["max_price"]["prompt"].as_f64().unwrap()>0.0);
            let mut tx=begin_account_scoped(&owner,account).await.unwrap();
            crate::billing::renewal::lock_period_rotation(&mut tx,account).await.unwrap();
            sqlx::query("UPDATE member_allowance_periods SET period_end=clock_timestamp() WHERE id=$1")
                .bind(period).execute(&mut *tx).await.unwrap();
            sqlx::query("INSERT INTO member_allowance_periods(account_id,user_id,period_start,period_end,granted_credits) VALUES($1,$2,clock_timestamp(),clock_timestamp()+interval '1 day',10)")
                .bind(account).bind(payer).execute(&mut *tx).await.unwrap();
            sqlx::query("INSERT INTO credit_conversion(effective_from,reference_cost_per_credit,note) VALUES(clock_timestamp(),1,'after-arm rate')")
                .execute(&mut *tx).await.unwrap();
            tx.commit().await.unwrap();
            Json(serde_json::json!({"id":"loopback-fixture","model":"deepseek/deepseek-v4-flash-0731",
                "choices":[{"message":{"content":"fixture answer"},"finish_reason":"stop"}],
                "usage":{"prompt_tokens":2,"completion_tokens":2,"cost":0.000044}}))
        }
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let provider = OpenAiProvider::openrouter(reqwest::Client::new(), "local-fixture")
        .with_base_url(format!("http://{addr}"));
    let plane = f.plane();
    let req = completion_request();
    let gate = ManagedGate::open(f.call(&plane), &req).await.unwrap();
    provider.complete_gated(req, &gate).await.unwrap();
    assert_eq!(count.load(Ordering::SeqCst), 1);
    let (payer_seen, credits): (Uuid, Decimal) = sqlx::query_as(
        "SELECT attributed_user_id,credits FROM credit_ledger WHERE entry_kind='usage_managed'",
    )
    .fetch_one(&f.control.owner)
    .await
    .unwrap();
    assert_eq!(payer_seen, f.payer);
    assert_eq!(
        credits,
        Decimal::new(-1, 2),
        "original positive conversion remains pinned after the rate changes"
    );
    let old: Decimal =
        sqlx::query_scalar("SELECT consumed_credits FROM member_allowance_periods WHERE id=$1")
            .bind(period)
            .fetch_one(&f.control.owner)
            .await
            .unwrap();
    assert_eq!(old, Decimal::new(1, 2));
    let new: Decimal = sqlx::query_scalar(
        "SELECT consumed_credits FROM member_allowance_periods WHERE account_id=$1 AND id<>$2",
    )
    .bind(f.account)
    .bind(period)
    .fetch_one(&f.control.owner)
    .await
    .unwrap();
    assert_eq!(new, Decimal::ZERO);
    let receipt: Uuid = sqlx::query_scalar(
        "SELECT receipt_id FROM managed_attempt_receipts WHERE protocol_version=1",
    )
    .fetch_one(f.owner())
    .await
    .unwrap();
    funded::reconcile(&gate, receipt).await.unwrap();
    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM credit_ledger WHERE source_receipt_id=$1")
            .bind(receipt)
            .fetch_one(&f.control.owner)
            .await
            .unwrap();
    assert_eq!(count, 1);
    assert_eq!(
        crate::workers::billing_derivation::native_receipt_derivation(
            &f.control.app,
            f.app(),
            &f.tenant,
            f.account,
        )
        .await
        .unwrap(),
        0,
        "billing receipt-axis replay uses the existing pinned debit, never current conversion"
    );
    let repeated = coralswarm_core::send_gate::acquire(&gate, 2).await.unwrap();
    let repeated_id = repeated.into_sent();
    let frozen: (Uuid,Decimal) = sqlx::query_as("SELECT member_period_id,reference_cost_per_credit FROM managed_credit_reservations WHERE receipt_id=$1")
        .bind(repeated_id.receipt_id()).fetch_one(&f.control.owner).await.unwrap();
    assert_eq!(
        frozen,
        (period, Decimal::new(44, 4)),
        "every retry of the same logical call keeps the first member allocation and conversion"
    );
    gate.settle(repeated_id, &SendOutcome::NotGenerated).await;
    let first_limit: i64 = sqlx::query_scalar(
        "SELECT limit_micro_usd FROM managed_budget_periods WHERE account_id=$1",
    )
    .bind(f.account)
    .fetch_one(f.owner())
    .await
    .unwrap();
    let mut changed = ManagedGate::open(f.call(&plane), &completion_request())
        .await
        .unwrap();
    changed.caps.daily_extraction = MicroUsd::from_usd_ceil(1000.0);
    let permit = coralswarm_core::send_gate::acquire(&changed, 1)
        .await
        .unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT limit_micro_usd FROM managed_budget_periods WHERE account_id=$1"
        )
        .bind(f.account)
        .fetch_one(f.owner())
        .await
        .unwrap(),
        first_limit,
        "first-use period limit cannot be rewritten by another Caps snapshot"
    );
    let sent = permit.into_sent();
    changed.settle(sent, &SendOutcome::NotGenerated).await;
    let operator_limit = first_limit + 123;
    sqlx::query("UPDATE managed_budget_periods SET limit_micro_usd=$2 WHERE account_id=$1")
        .bind(f.account)
        .bind(operator_limit)
        .execute(f.owner())
        .await
        .unwrap();
    for cap in [0.0, 100.0] {
        let mut snapshot = ManagedGate::open(f.call(&plane), &completion_request())
            .await
            .unwrap();
        snapshot.caps.daily_extraction = MicroUsd::from_usd_ceil(cap);
        let permit = coralswarm_core::send_gate::acquire(&snapshot, 1)
            .await
            .unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT limit_micro_usd FROM managed_budget_periods WHERE account_id=$1"
            )
            .bind(f.account)
            .fetch_one(f.owner())
            .await
            .unwrap(),
            operator_limit,
            "explicit operator limit survives later Caps0 and Caps100 snapshots"
        );
        snapshot
            .settle(permit.into_sent(), &SendOutcome::NotGenerated)
            .await;
    }

    server.abort();
    drop(changed);
    drop(gate);
    drop(plane);
    f.finish().await;
}
#[tokio::test]
async fn consumer_loopback_pins_payer_conversion_period_and_limit() {
    loopback_and_pinned_settlement(false).await;
}
#[tokio::test]
async fn tenant_loopback_pins_payer_conversion_period_and_limit() {
    loopback_and_pinned_settlement(true).await;
}

async fn concurrent_last_credit_and_rollback(org: bool) {
    let f = Fixture::new(org).await;
    sqlx::query("INSERT INTO credit_conversion(effective_from,reference_cost_per_credit,note) VALUES(now(),1,'isolated concurrency rate')")
        .execute(&f.control.owner).await.unwrap();
    f.balance(Decimal::from(5)).await;
    let first = f.funding(5_000_000);
    let second = f.funding(5_000_000);
    let mut winner = begin_account_scoped(&f.control.app, f.account)
        .await
        .unwrap();
    assert!(matches!(
        reserve_with_content(&mut winner, &first, f.legacy_context()).await.unwrap(),
        ReserveOutcome::Held(_)
    ));
    let second_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(
            f.control
                .app
                .connect_options()
                .as_ref()
                .clone()
                .application_name("cor22-last-credit-loser"),
        )
        .await
        .unwrap();
    let account = f.account;
    let actor = second_pool.clone();
    let legacy_content=f.legacy_context();
    let loser = tokio::spawn(async move {
        let mut tx = begin_account_scoped(&actor, account).await.unwrap();
        let outcome = reserve_with_content(&mut tx, &second, legacy_content).await.unwrap();
        tx.commit().await.unwrap();
        outcome
    });
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    loop {
        let blocked:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name='cor22-last-credit-loser' AND wait_event='advisory')")
            .fetch_one(&f.control.owner).await.unwrap();
        if blocked {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "losing normal-role reserve must wait on the common account lock"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    winner.commit().await.unwrap();
    assert!(
        matches!(loser.await.unwrap(), ReserveOutcome::Unfunded),
        "two managed callers cannot both hold the final pool credits"
    );
    let mut tx = begin_account_scoped(&f.control.app, f.account)
        .await
        .unwrap();
    credit::release_unsent_tx(&mut tx, &first).await.unwrap();
    tx.commit().await.unwrap();
    let rolled = f.funding(5_000_000);
    let mut tx = begin_account_scoped(&f.control.app, f.account)
        .await
        .unwrap();
    assert!(matches!(
        reserve_with_content(&mut tx, &rolled, f.legacy_context()).await.unwrap(),
        ReserveOutcome::Held(_)
    ));
    tx.rollback().await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM managed_credit_reservations WHERE receipt_id=$1"
        )
        .bind(rolled.receipt)
        .fetch_one(&f.control.owner)
        .await
        .unwrap(),
        0,
        "known rollback leaves no financial hold"
    );
    let overrun = f.funding(5_000_000);
    assert!(matches!(
        f.reserve_credit(&overrun).await,
        ReserveOutcome::Held(_)
    ));
    let mut tx = begin_account_scoped(&f.control.app, f.account)
        .await
        .unwrap();
    assert!(credit::settle_tx(&mut tx, &overrun, 6_000_000)
        .await
        .unwrap());
    tx.commit().await.unwrap();
    let mut tx = begin_account_scoped(&f.control.app, f.account)
        .await
        .unwrap();
    assert!(!credit::settle_tx(&mut tx, &overrun, 6_000_000)
        .await
        .unwrap());
    tx.commit().await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, Decimal>("SELECT balance FROM credit_balances WHERE account_id=$1")
            .bind(f.account)
            .fetch_one(&f.control.owner)
            .await
            .unwrap(),
        Decimal::from(-1),
        "overrun charges the full cost once"
    );
    let invalid = sqlx::query("INSERT INTO credit_conversion(effective_from,reference_cost_per_credit,note) VALUES(clock_timestamp(),0,'invalid conversion fixture')")
        .execute(&f.control.owner).await.unwrap_err();
    assert_eq!(
        invalid
            .as_database_error()
            .and_then(|error| error.code())
            .as_deref(),
        Some("23514"),
        "the database rejects nonpositive conversion configuration"
    );
    sqlx::query("DELETE FROM credit_conversion")
        .execute(&f.control.owner)
        .await
        .unwrap();
    assert!(
        matches!(
            f.reserve_credit(&f.funding(1)).await,
            ReserveOutcome::Unavailable
        ),
        "a missing conversion never creates zero-credit funding"
    );
    second_pool.close().await;
    f.finish().await;
}
#[tokio::test]
async fn consumer_last_credit_race_known_rollback_and_overrun() {
    concurrent_last_credit_and_rollback(false).await;
}
#[tokio::test]
async fn tenant_last_credit_race_known_rollback_and_overrun() {
    concurrent_last_credit_and_rollback(true).await;
}

#[derive(Clone, Copy)]
enum CommitAckLoss {
    Nth(usize),
    Arm,
}

async fn lose_commit_ack(
    options: PgConnectOptions,
    nth: usize,
) -> (
    PgPool,
    tokio::task::JoinHandle<()>,
    std::sync::Arc<std::sync::atomic::AtomicUsize>,
) {
    lose_commit_ack_at(options, CommitAckLoss::Nth(nth)).await
}

async fn lose_commit_ack_at(
    options: PgConnectOptions,
    target: CommitAckLoss,
) -> (
    PgPool,
    tokio::task::JoinHandle<()>,
    std::sync::Arc<std::sync::atomic::AtomicUsize>,
) {
    use sqlx::postgres::PgSslMode;
    use std::sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    };
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
    };
    let remote = (options.get_host().to_owned(), options.get_port());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let seen = Arc::new(AtomicUsize::new(0));
    let observed = seen.clone();
    let drop_ack = Arc::new(AtomicBool::new(false));
    let drop_outgoing = drop_ack.clone();
    let proxy = tokio::spawn(async move {
        let (client, _) = listener.accept().await.unwrap();
        let server = TcpStream::connect(remote).await.unwrap();
        let (mut client_read, mut client_write) = client.into_split();
        let (mut server_read, mut server_write) = server.into_split();
        let outgoing = async {
            let mut buffer = [0u8; 16384];
            let mut tail = Vec::new();
            let mut arm_seen = false;
            loop {
                let size = client_read.read(&mut buffer).await.unwrap();
                if size == 0 {
                    return;
                }
                for byte in &buffer[..size] {
                    tail.push(*byte);
                    if tail.ends_with(b"managed_arm_v1") {
                        arm_seen = true;
                    }
                    if tail.ends_with(b"COMMIT") {
                        let commit = observed.fetch_add(1, Ordering::SeqCst) + 1;
                        let selected = match target {
                            CommitAckLoss::Nth(nth) => commit >= nth,
                            CommitAckLoss::Arm => arm_seen,
                        };
                        if selected {
                            drop_outgoing.store(true, Ordering::SeqCst);
                        }
                    }
                    if tail.len() > b"managed_arm_v1".len() {
                        tail.remove(0);
                    }
                }
                if server_write.write_all(&buffer[..size]).await.is_err() {
                    return;
                }
            }
        };
        let incoming = async {
            let mut buffer = [0u8; 16384];
            loop {
                let size = server_read.read(&mut buffer).await.unwrap();
                if size == 0 {
                    return;
                }
                if drop_ack.load(Ordering::SeqCst) {
                    return;
                }
                if client_write.write_all(&buffer[..size]).await.is_err() {
                    return;
                }
            }
        };
        tokio::select! {_=outgoing=>{},_=incoming=>{}}
    });
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_secs(2))
        .connect_with(
            options
                .host("127.0.0.1")
                .port(port)
                .ssl_mode(PgSslMode::Disable),
        )
        .await
        .unwrap();
    (pool, proxy, seen)
}

async fn acknowledgement_loss(org: bool, control_loss: bool, target: CommitAckLoss, armed: bool) {
    use crate::llm::{openai::OpenAiProvider, GatedLlm};
    use axum::{routing::post, Json, Router};
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    let f = Fixture::new(org).await;
    f.balance(Decimal::from(10)).await;
    let options = if control_loss {
        f.control.app.connect_options()
    } else {
        f.app().connect_options()
    };
    let (proxy, relay, seen) = lose_commit_ack_at(options.as_ref().clone(), target).await;
    let count = Arc::new(AtomicUsize::new(0));
    let observed = count.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let router = Router::new().route(
        "/v1/chat/completions",
        post(move || {
            let observed = observed.clone();
            async move {
                observed.fetch_add(1, Ordering::SeqCst);
                Json(serde_json::json!({}))
            }
        }),
    );
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let provider = OpenAiProvider::openrouter(reqwest::Client::new(), "local-ack-fixture")
        .with_base_url(format!("http://{addr}"));
    let plane = if control_loss {
        f.plane()
    } else if org {
        JobScope::Org(&proxy)
    } else {
        JobScope::Consumer {
            pool: &proxy,
            account_id: f.account,
        }
    };
    let mut call = f.call(&plane);
    if control_loss {
        call.control = &proxy;
    }
    let req = completion_request();
    let gate = ManagedGate::open(call, &req).await.unwrap();
    let logical = gate.logical_call;
    let result = provider.complete_gated(req, &gate).await;
    assert!(
        matches!(
            result,
            Err(crate::llm::LlmError::Managed(Deny::Unavailable))
        ),
        "lost acknowledgement must not emit a permit, actual={result:?}"
    );
    assert_eq!(count.load(Ordering::SeqCst), 0);
    let commits = seen.load(Ordering::SeqCst);
    match target {
        CommitAckLoss::Nth(nth) => assert!(commits >= nth),
        CommitAckLoss::Arm => assert!(commits > 0),
    }
    eprintln!("lost acknowledgement after {commits} actual commits; armed target={armed}");
    let (receipt,phase,state,bound):(Uuid,String,String,i64)=sqlx::query_as("SELECT receipt_id,dispatch_phase,state,reserved_micro_usd FROM managed_attempt_receipts WHERE logical_call=$1")
        .bind(logical).fetch_one(f.owner()).await.unwrap();
    assert_eq!(
        phase,
        if armed {
            "armed"
        } else if control_loss {
            "cancelled_unsent"
        } else {
            "prepared"
        }
    );
    assert_eq!(
        state,
        if control_loss {
            "not_generated"
        } else {
            "reserved"
        }
    );
    let healthy = f.plane();
    funded::recover_logical_call(&healthy, &f.control.app, &f.tenant, f.account, logical)
        .await
        .unwrap();
    funded::recover_logical_call(&healthy, &f.control.app, &f.tenant, f.account, logical)
        .await
        .unwrap();
    let (phase,state,cost):(String,String,i64)=sqlx::query_as("SELECT dispatch_phase,state,settled_micro_usd FROM managed_attempt_receipts WHERE receipt_id=$1")
        .bind(receipt).fetch_one(f.owner()).await.unwrap();
    assert_eq!(phase, if armed { "armed" } else { "cancelled_unsent" });
    assert_eq!(state, if armed { "ambiguous" } else { "not_generated" });
    assert_eq!(cost, if armed { bound } else { 0 });
    let financial: String =
        sqlx::query_scalar("SELECT state FROM managed_credit_reservations WHERE receipt_id=$1")
            .bind(receipt)
            .fetch_one(&f.control.owner)
            .await
            .unwrap();
    assert_eq!(financial, if armed { "settled" } else { "released" });
    let ledger: i64 =
        sqlx::query_scalar("SELECT count(*) FROM credit_ledger WHERE source_receipt_id=$1")
            .bind(receipt)
            .fetch_one(&f.control.owner)
            .await
            .unwrap();
    assert_eq!(ledger, i64::from(armed));
    let mut tx = begin_plane(&healthy).await.unwrap();
    assert!(
        !sqlx::query_scalar::<_, bool>("SELECT managed_arm_v1($1)")
            .bind(receipt)
            .fetch_one(&mut *tx)
            .await
            .unwrap(),
        "an old receipt cannot arm again"
    );
    assert!(
        !sqlx::query_scalar::<_, bool>("SELECT managed_settle_v1($1,'billed',1,NULL,NULL,NULL)")
            .bind(receipt)
            .fetch_one(&mut *tx)
            .await
            .unwrap(),
        "late settle cannot debit twice"
    );
    tx.commit().await.unwrap();
    server.abort();
    drop(gate);
    drop(plane);
    proxy.close().await;
    relay.await.unwrap();
    drop(healthy);
    f.finish().await;
}
#[tokio::test]
async fn consumer_lost_arm_ack_keeps_funds_and_charges_once() {
    acknowledgement_loss(false, false, CommitAckLoss::Arm, true).await;
}
#[tokio::test]
async fn tenant_lost_prepare_ack_cancels_without_funding() {
    acknowledgement_loss(true, false, CommitAckLoss::Nth(1), false).await;
}
#[tokio::test]
async fn tenant_lost_control_ack_never_sends_then_releases_unsent() {
    acknowledgement_loss(true, true, CommitAckLoss::Nth(2), false).await;
}
#[tokio::test]
async fn tenant_lost_arm_ack_keeps_funds_and_charges_once() {
    acknowledgement_loss(true, false, CommitAckLoss::Arm, true).await;
}

async fn committed_interactive_and_managed_limit(org: bool) {
    let f = Fixture::new(org).await;
    f.balance(Decimal::from(1)).await;
    assert!(!crate::billing::enforcement::check_blocked(
        &f.control.app,
        f.account,
        f.payer,
        crate::billing::enforcement::Spend::Background
    )
    .await
    .unwrap());
    crate::workers::billing_derivation::native_interactive_debit(
        &f.control.app,
        f.account,
        f.payer,
        Decimal::ONE,
    )
    .await
    .unwrap();
    let plane = f.plane();
    let gate = ManagedGate::open(f.call(&plane), &completion_request())
        .await
        .unwrap();
    assert!(matches!(coralswarm_core::send_gate::acquire(&gate,1).await,Err(Deny::Hold(HoldCode::CreditsExhausted))),"committed interactive debit between eligibility and reservation is seen under the account lock");
    f.balance(Decimal::ONE).await;
    sqlx::query(
        "UPDATE corpus_enrichment_runs SET status='running',pause_reason=NULL WHERE graph_id=$1",
    )
    .bind(f.graph)
    .execute(f.owner())
    .await
    .unwrap();
    let gate = ManagedGate::open(f.call(&plane), &completion_request())
        .await
        .unwrap();
    let permit = coralswarm_core::send_gate::acquire(&gate, 1).await.unwrap();
    crate::workers::billing_derivation::native_interactive_debit(
        &f.control.app,
        f.account,
        f.payer,
        Decimal::ONE,
    )
    .await
    .unwrap();
    gate.settle(
        permit.into_sent(),
        &SendOutcome::Ambiguous {
            generation_id: None,
        },
    )
    .await;
    let balance: Decimal =
        sqlx::query_scalar("SELECT balance FROM credit_balances WHERE account_id=$1")
            .bind(f.account)
            .fetch_one(&f.control.owner)
            .await
            .unwrap();
    assert!(balance<Decimal::ZERO,"an already-armed managed call and later interactive commit may both charge; this is the documented limit, not a global credit guarantee");
    let fresh = ManagedGate::open(f.call(&plane), &completion_request())
        .await
        .unwrap();
    assert!(
        matches!(
            coralswarm_core::send_gate::acquire(&fresh, 1).await,
            Err(Deny::Hold(HoldCode::CreditsExhausted))
        ),
        "future managed attempts deny after the committed interactive debit"
    );
    let mut blocker = begin_account_scoped(&f.control.app, f.account)
        .await
        .unwrap();
    crate::billing::renewal::lock_period_rotation(&mut blocker, f.account)
        .await
        .unwrap();
    let actor = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(
            f.control
                .app
                .connect_options()
                .as_ref()
                .clone()
                .application_name("cor22-interactive-lock"),
        )
        .await
        .unwrap();
    let account = f.account;
    let payer = f.payer;
    let task_pool = actor.clone();
    let writer = tokio::spawn(async move {
        crate::workers::billing_derivation::native_interactive_debit(
            &task_pool,
            account,
            payer,
            Decimal::ONE,
        )
        .await
    });
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    loop {
        let blocked:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name='cor22-interactive-lock' AND wait_event='advisory')")
            .fetch_one(&f.control.owner).await.unwrap();
        if blocked {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the actual committed billing writer participates in the same first financial lock"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    blocker.commit().await.unwrap();
    assert_eq!(writer.await.unwrap().unwrap(), 1);
    actor.close().await;
    drop(fresh);
    drop(gate);
    drop(plane);
    f.finish().await;
}
#[tokio::test]
async fn consumer_committed_interactive_serialization_and_explicit_limit() {
    committed_interactive_and_managed_limit(false).await;
}
#[tokio::test]
async fn tenant_committed_interactive_serialization_and_explicit_limit() {
    committed_interactive_and_managed_limit(true).await;
}

async fn stripe_financial_writers(org: bool) {
    let f = Fixture::new(org).await;
    let customer = format!("cus_{}", f.account.simple());
    let subscription = format!("sub_{}", f.account.simple());
    let price = format!("price_{}", f.account.simple());
    { let mut seed_tx=f.financial().await; let value=sqlx::query("INSERT INTO stripe_subscriptions(account_id,stripe_customer_id,stripe_subscription_id,status) VALUES($1,$2,$3,'active')")
        .bind(f.account).bind(customer).bind(&subscription).execute(&mut *seed_tx).await.unwrap(); seed_tx.commit().await.unwrap(); value };
    sqlx::query("UPDATE credit_packs SET stripe_price_id=$1 WHERE code='pack_500'")
        .bind(&price)
        .execute(&f.control.owner)
        .await
        .unwrap();
    f.balance(Decimal::ONE).await;
    let actor = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(
            f.control
                .app
                .connect_options()
                .as_ref()
                .clone()
                .application_name("cor22-stripe-financial-writer"),
        )
        .await
        .unwrap();
    for cancel in [false, true] {
        let mut blocker = begin_account_scoped(&f.control.app, f.account)
            .await
            .unwrap();
        crate::billing::renewal::lock_period_rotation(&mut blocker, f.account)
            .await
            .unwrap();
        let event = format!("evt_finance_{}", Uuid::new_v4().simple());
        let (pool, task_price, task_subscription, event_id, account) = (
            actor.clone(),
            price.clone(),
            subscription.clone(),
            event.clone(),
            f.account,
        );
        let task = tokio::spawn(async move {
            if cancel {
                assert!(crate::routes::stripe_webhook::native_cancel_writer(
                    &pool,
                    account,
                    &event_id,
                    &task_subscription
                )
                .await
                .unwrap());
            } else {
                assert_eq!(
                    crate::routes::stripe_webhook::native_topup_writer(
                        &pool,
                        account,
                        &event_id,
                        &task_price
                    )
                    .await,
                    axum::http::StatusCode::OK
                );
            }
        });
        let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        loop {
            let blocked:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name='cor22-stripe-financial-writer' AND wait_event='advisory')")
                .fetch_one(&f.control.owner).await.unwrap();
            if blocked {
                break;
            }
            assert!(tokio::time::Instant::now()<deadline,"actual Stripe writer must acquire the common account lock before financial mutation");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            !sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(SELECT 1 FROM stripe_webhook_events WHERE event_id=$1)"
            )
            .bind(&event)
            .fetch_one(&f.control.owner)
            .await
            .unwrap(),
            "the account lock precedes even the event claim"
        );
        blocker.commit().await.unwrap();
        task.await.unwrap();
        if cancel {
            assert!(!crate::routes::stripe_webhook::native_cancel_writer(
                &actor,
                f.account,
                &event,
                &subscription
            )
            .await
            .unwrap());
            assert_eq!(
                sqlx::query_scalar::<_, String>(
                    "SELECT status FROM stripe_subscriptions WHERE account_id=$1"
                )
                .bind(f.account)
                .fetch_one(&f.control.owner)
                .await
                .unwrap(),
                "canceled"
            );
        } else {
            assert_eq!(
                crate::routes::stripe_webhook::native_topup_writer(
                    &actor, f.account, &event, &price
                )
                .await,
                axum::http::StatusCode::OK
            );
            // Since settings launch PR 3.4 a pack feeds the extra-usage
            // balance ($11 for pack_500), not the plan pool, which keeps the
            // 1 credit the fixture seeded.
            assert_eq!(
                sqlx::query_scalar::<_, i64>(
                    "SELECT balance_micro_usd FROM extra_usage_balances WHERE account_id=$1"
                )
                .bind(f.account)
                .fetch_one(&f.control.owner)
                .await
                .unwrap(),
                11_000_000,
                "actual topup commits once despite event replay"
            );
            assert_eq!(
                sqlx::query_scalar::<_, Decimal>(
                    "SELECT balance FROM credit_balances WHERE account_id=$1"
                )
                .bind(f.account)
                .fetch_one(&f.control.owner)
                .await
                .unwrap(),
                Decimal::ONE,
                "a pack no longer touches the plan pool"
            );
        }
    }
    let plane = f.plane();
    let gate = ManagedGate::open(f.call(&plane), &completion_request())
        .await
        .unwrap();
    assert!(
        matches!(
            coralswarm_core::send_gate::acquire(&gate, 1).await,
            Err(Deny::Hold(HoldCode::CreditsExhausted))
        ),
        "cancelled subscription freezes a positive pool before new managed sends"
    );
    actor.close().await;
    drop(gate);
    drop(plane);
    f.finish().await;
}
#[tokio::test]
async fn consumer_actual_topup_and_cancel_take_first_financial_lock() {
    stripe_financial_writers(false).await;
}
#[tokio::test]
async fn tenant_actual_topup_and_cancel_take_first_financial_lock() {
    stripe_financial_writers(true).await;
}

#[tokio::test]
async fn tenant_arm_and_cancel_serialize_and_recover_both_schedules() {
    for arm_first in [true, false] {
        let f = Fixture::new(true).await;
        f.balance(Decimal::from(10)).await;
        let (proxy, relay, seen) =
            lose_commit_ack(f.app().connect_options().as_ref().clone(), 1).await;
        let fault_plane = JobScope::Org(&proxy);
        let gate = ManagedGate::open(f.call(&fault_plane), &completion_request())
            .await
            .unwrap();
        let logical = gate.logical_call;
        assert!(matches!(
            coralswarm_core::send_gate::acquire(&gate, 1).await,
            Err(Deny::Unavailable)
        ));
        assert_eq!(seen.load(std::sync::atomic::Ordering::SeqCst), 1);
        drop(gate);
        drop(fault_plane);
        proxy.close().await;
        relay.await.unwrap();
        let (receipt,payer,hash,bound):(Uuid,Uuid,String,i64)=sqlx::query_as("SELECT receipt_id,attributed_user_id,request_hash,reserved_micro_usd FROM managed_attempt_receipts WHERE logical_call=$1 AND dispatch_phase='prepared'")
            .bind(logical).fetch_one(f.owner()).await.unwrap();
        let request = FundingRequest {
            plane: match &f.tenant {
                TenantId::Org(org) => format!("org:{org}"),
                _ => unreachable!(),
            },
            receipt,
            account: f.account,
            payer,
            graph: f.graph,
            logical_call: logical,
            request_hash: hash,
            bound_micro_usd: bound,
        };
        assert!(matches!(
            f.reserve_credit(&request).await,
            ReserveOutcome::Held(_)
        ));
        let healthy = f.plane();
        let mut first = begin_plane(&healthy).await.unwrap();
        let first_sql = if arm_first {
            "SELECT managed_arm_v1($1)"
        } else {
            "SELECT managed_cancel_unsent_v1($1)"
        };
        assert!(sqlx::query_scalar::<_, bool>(first_sql)
            .bind(receipt)
            .fetch_one(&mut *first)
            .await
            .unwrap());
        let actor = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(
                f.app()
                    .connect_options()
                    .as_ref()
                    .clone()
                    .application_name("cor22-arm-cancel-loser"),
            )
            .await
            .unwrap();
        let task_pool = actor.clone();
        let loser = tokio::spawn(async move {
            let scope = JobScope::Org(&task_pool);
            let mut tx = begin_plane(&scope).await.unwrap();
            let sql = if arm_first {
                "SELECT managed_cancel_unsent_v1($1)"
            } else {
                "SELECT managed_arm_v1($1)"
            };
            let won = sqlx::query_scalar::<_, bool>(sql)
                .bind(receipt)
                .fetch_one(&mut *tx)
                .await
                .unwrap();
            tx.commit().await.unwrap();
            won
        });
        let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        loop {
            let blocked:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name='cor22-arm-cancel-loser' AND wait_event_type='Lock')")
                .fetch_one(f.owner()).await.unwrap();
            if blocked {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "arm and cancellation must wait on the same content locks"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT state FROM managed_credit_reservations WHERE receipt_id=$1"
            )
            .bind(receipt)
            .fetch_one(&f.control.owner)
            .await
            .unwrap(),
            "held",
            "unacknowledged cancellation never frees funds"
        );
        first.commit().await.unwrap();
        assert!(
            !loser.await.unwrap(),
            "one phase transition wins, never both"
        );
        funded::recover_logical_call(&healthy, &f.control.app, &f.tenant, f.account, logical)
            .await
            .unwrap();
        funded::recover_logical_call(&healthy, &f.control.app, &f.tenant, f.account, logical)
            .await
            .unwrap();
        let (phase,state,cost):(String,String,i64)=sqlx::query_as("SELECT dispatch_phase,state,settled_micro_usd FROM managed_attempt_receipts WHERE receipt_id=$1")
            .bind(receipt).fetch_one(f.owner()).await.unwrap();
        assert_eq!(
            phase,
            if arm_first {
                "armed"
            } else {
                "cancelled_unsent"
            }
        );
        assert_eq!(
            state,
            if arm_first {
                "ambiguous"
            } else {
                "not_generated"
            }
        );
        assert_eq!(cost, if arm_first { bound } else { 0 });
        let (credit_state,ledger):(String,i64)=sqlx::query_as("SELECT state,(SELECT count(*) FROM credit_ledger WHERE source_receipt_id=$1) FROM managed_credit_reservations WHERE receipt_id=$1")
            .bind(receipt).fetch_one(&f.control.owner).await.unwrap();
        assert_eq!(credit_state, if arm_first { "settled" } else { "released" });
        assert_eq!(ledger, i64::from(arm_first));
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT charged_micro_usd FROM corpus_enrichment_units WHERE unit_id=$1"
            )
            .bind(f.unit)
            .fetch_one(f.owner())
            .await
            .unwrap(),
            if arm_first { bound } else { 0 }
        );
        let mut tx = begin_plane(&healthy).await.unwrap();
        assert!(!sqlx::query_scalar::<_, bool>("SELECT managed_arm_v1($1)")
            .bind(receipt)
            .fetch_one(&mut *tx)
            .await
            .unwrap());
        assert!(!sqlx::query_scalar::<_, bool>(
            "SELECT managed_settle_v1($1,'billed',1,NULL,NULL,NULL)"
        )
        .bind(receipt)
        .fetch_one(&mut *tx)
        .await
        .unwrap());
        tx.commit().await.unwrap();
        actor.close().await;
        drop(healthy);
        f.finish().await;
    }
}


fn completion_rate_history() -> (PriceSheet, chrono::DateTime<Utc>) {
    use crate::billing::rates::RateRow;
    let boundary = chrono::DateTime::<Utc>::from_timestamp(94_608_000, 0).unwrap();
    let mut rows = Vec::new();
    for (model, before_input, before_output, after_input, after_output) in [
        ("deepseek-v4-flash", 0.08, 0.18, 0.8, 1.8),
        ("glm-5.3-flash", 0.075, 0.25, 0.0075, 0.025),
    ] {
        let historical = RateRow {
            provider: "openrouter".into(),
            model_pattern: model.into(),
            priority: 0,
            input_per_million: before_input,
            output_per_million: before_output,
            cache_write_multiplier: 1.0,
            cache_read_multiplier: 1.0,
            effective_from: chrono::DateTime::<Utc>::from_timestamp(0, 0).unwrap(),
            effective_to: Some(boundary),
            is_blend_fallback: false,
        };
        rows.push(historical.clone());
        rows.push(RateRow {
            input_per_million: after_input,
            output_per_million: after_output,
            effective_from: boundary,
            effective_to: None,
            ..historical
        });
    }
    (PriceSheet::from_rows(rows), boundary - chrono::Duration::seconds(1))
}

#[tokio::test]
async fn token_only_primary_and_fallback_keep_admitted_rates_in_both_planes() {
    use crate::llm::{openai::OpenAiProvider, GatedLlm};
    use axum::{routing::post, Json, Router};
    use std::sync::{atomic::{AtomicUsize, Ordering}, Arc};
    for org in [false, true] {
        let f = Fixture::new(org).await;
        f.balance(Decimal::from(10)).await;
        sqlx::query("INSERT INTO credit_conversion(effective_from,reference_cost_per_credit,note) VALUES(clock_timestamp(),1,'token-only boundary fixture')")
            .execute(&f.control.owner).await.unwrap();
        let hits = Arc::new(AtomicUsize::new(0));
        let observed = hits.clone();
        let router = Router::new().route("/v1/chat/completions", post(move |Json(body): Json<serde_json::Value>| {
            let observed = observed.clone();
            async move {
                let attempt = observed.fetch_add(1, Ordering::SeqCst);
                assert_eq!(body["model"], "deepseek/deepseek-v4-flash-0731");
                assert_eq!(body["models"], serde_json::json!(["z-ai/glm-5.3-flash"]));
                assert_eq!(body["provider"]["max_price"], serde_json::json!({"prompt":0.16,"completion":0.5,"request":0}));
                let model = if attempt == 1 { "z-ai/glm-5.3-flash" } else { "deepseek/deepseek-v4-flash-0731" };
                let mut usage = serde_json::json!({"prompt_tokens":1000,"completion_tokens":10});
                if attempt == 2 { usage["cost"] = serde_json::json!(0.0007); }
                Json(serde_json::json!({"id":format!("rate-boundary-{attempt}"),"model":model,
                    "choices":[{"message":{"content":"synthetic boundary answer"},"finish_reason":"stop"}],
                    "usage":usage}))
            }
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let provider = OpenAiProvider::openrouter(reqwest::Client::new(), "local-fixture")
            .with_base_url(format!("http://{addr}"));
        let plane = f.plane();
        let (sheet, as_of) = completion_rate_history();
        assert_eq!(sheet.rate("openrouter", "deepseek/deepseek-v4-flash-0731", Utc::now()).input_per_million, 0.8);
        assert_eq!(sheet.rate("openrouter", "z-ai/glm-5.3-flash", Utc::now()).input_per_million, 0.0075);
        for (expected, served) in [(82, "deepseek/deepseek-v4-flash-0731"), (78, "z-ai/glm-5.3-flash"), (700, "deepseek/deepseek-v4-flash-0731")] {
            let req = completion_request();
            let mut gate = ManagedGate::open(f.call(&plane), &req).await.unwrap();
            let historical_worst = worst_case(&req, None, &sheet, gate.caps.headroom, as_of).unwrap();
            assert_eq!(historical_worst, gate.worst, "the seeded historical cards have exactly the admitted request terms and hash");
            gate.sheet = sheet.clone();
            gate.pricing_as_of = as_of;
            gate.worst = historical_worst;
            provider.complete_gated(req, &gate).await.unwrap();
            let (receipt, state, cost, model): (Uuid, String, i64, String) = sqlx::query_as(
                "SELECT receipt_id,state,settled_micro_usd,served_model FROM managed_attempt_receipts WHERE logical_call=$1",
            ).bind(gate.logical_call).fetch_one(f.owner()).await.unwrap();
            assert_eq!(cost, expected, "token-only primary/fallback settlement keeps admission pricing; reported cost still wins");
            assert_eq!(model, served);
            assert_eq!(state, if expected > gate.worst.per_attempt.get() { "overrun" } else { "billed" });
            let debit: (Decimal, Decimal, Uuid) = sqlx::query_as(
                "SELECT credits,cost_usd,attributed_user_id FROM credit_ledger WHERE source_receipt_id=$1",
            ).bind(receipt).fetch_one(&f.control.owner).await.unwrap();
            assert_eq!(debit, (Decimal::new(-expected, 6), Decimal::new(expected, 6), f.payer));
            let financial: (String, i64) = sqlx::query_as(
                "SELECT state,settled_micro_usd FROM managed_credit_reservations WHERE receipt_id=$1",
            ).bind(receipt).fetch_one(&f.control.owner).await.unwrap();
            assert_eq!(financial, ("settled".into(), expected));
            funded::reconcile(&gate, receipt).await.unwrap();
            let count: i64 = sqlx::query_scalar("SELECT count(*) FROM credit_ledger WHERE source_receipt_id=$1")
                .bind(receipt).fetch_one(&f.control.owner).await.unwrap();
            assert_eq!(count, 1);
        }
        assert_eq!(hits.load(Ordering::SeqCst), 3);
        server.abort();
        drop(plane);
        f.finish().await;
    }
}

#[path = "pg_tests/release_c.rs"]
mod release_c;

#[path = "pg_tests/embedding_workers.rs"]
mod embedding_workers;

#[path = "pg_tests/enrichment.rs"]
mod enrichment;

#[path = "pg_tests/entity_resolution.rs"]
mod entity_resolution;

#[path = "pg_tests/interactive_ask.rs"]
mod interactive_ask;

#[path = "pg_tests/enrichment_consumers.rs"]
mod enrichment_consumers;

#[path = "pg_tests/meeting_summary.rs"]
mod meeting_summary;

#[path = "pg_tests/admission_latency.rs"]
mod admission_latency;

#[path = "pg_tests/settlement_recovery.rs"]
mod settlement_recovery;
#[path = "pg_tests/call_timeouts.rs"]
mod call_timeouts;
#[path = "pg_tests/statement_embedding.rs"]
mod statement_embedding;
#[path = "pg_tests/personal_org.rs"]
mod personal_org;
