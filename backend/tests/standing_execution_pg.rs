use coralswarm_backend::{
    library_read::{self, LibraryScope},
    source_evidence::{self, CaptureSource},
    store::PostgresStore,
    tenancy::{
        extractor::Tenant,
        runtime::{TenancyDecision, TenantContext},
        scope, TenantId,
    },
    workers::{source_atoms, tenant_harness::WorkerTenant},
    UserContext,
};
use serde_json::{json, Value};
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    PgPool,
};
use std::{
    str::FromStr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use coralswarm_backend::test_db::TestDatabase;
use uuid::Uuid;

const PRIMARY: &str = "deepseek/deepseek-v4-flash-0731";

#[derive(Clone,Copy)]
struct HistoricalSeed { user: Uuid, account: Uuid, graph: Uuid, org: bool }

async fn restore_synthetic_history_before_standing_migration(owner: &PgPool, org_plane: bool, seed: HistoricalSeed) {
    if !org_plane {
        sqlx::query("INSERT INTO users(id,email,clerk_user_id) VALUES($1,$2,$2)").bind(seed.user).bind(format!("{}@fixture.local",seed.user)).execute(owner).await.unwrap();
        sqlx::query("INSERT INTO accounts(id,owner_user_id,clerk_org_id) VALUES($1,$2,$3)").bind(seed.account).bind(seed.user).bind(seed.org.then(||format!("org_execution_{}",seed.account.simple()))).execute(owner).await.unwrap();
    }
    sqlx::query("INSERT INTO graphs(id,account_id,name,kind) VALUES($1,$2,'Synthetic restored clock history','org')")
        .bind(seed.graph).bind(seed.account).execute(owner).await.unwrap();
    // Offline synthetic archive restore before the new standing migration, never an application clock writer.
    sqlx::query("ALTER TABLE corpus_capture_admissions DISABLE TRIGGER corpus_admission_observation_immutable").execute(owner).await.unwrap();
    for (basis,held,processed,age,lag,receipt_processed) in [
        ("never_held",false,true,600,300,true),
        ("legacy_unknown",false,true,3600,3300,true),
        ("operator_paused",true,true,7200,6900,true),
        ("backlog_policy_excluded",true,true,10800,10500,true),
        ("never_held",false,false,4200,0,false),
        ("never_held",false,true,900,0,false),
    ] {
        let (item,version)=(Uuid::new_v4(),Uuid::new_v4());
        sqlx::query("INSERT INTO source_items(source_item_id,graph_id,source_type,provider_namespace,source_id) VALUES($1,$2,'note','fixture:historical-clock',$3)")
            .bind(item).bind(seed.graph).bind(version.to_string()).execute(owner).await.unwrap();
        sqlx::query("INSERT INTO source_versions(version_id,graph_id,source_item_id,source_id,source_type,provider_namespace,representation,raw_inline,captured_at,extraction_status) VALUES($1,$2,$3,$4,'note','fixture:historical-clock','original_text','Historical retained fixture.',now()-$5::double precision*interval '1 second',$6)")
            .bind(version).bind(seed.graph).bind(item).bind(version.to_string()).bind(age as f64).bind(if processed {"done"} else {"pending"}).execute(owner).await.unwrap();
        sqlx::query("INSERT INTO corpus_capture_admissions(source_version_id,graph_id,account_id,source_item_id,captured_by,extraction_version,state,observed_at,processed_at,ever_held,hold_basis) SELECT version_id,graph_id,$2,source_item_id,$3,extraction_version,$4,captured_at,CASE WHEN $5 THEN captured_at+$6::double precision*interval '1 second' END,$7,$8 FROM source_versions WHERE version_id=$1")
            .bind(version).bind(seed.account).bind(seed.user).bind(if receipt_processed {"processed"} else {"queued"}).bind(receipt_processed).bind(lag as f64).bind(held).bind(basis).execute(owner).await.unwrap();
    }
    sqlx::query("ALTER TABLE corpus_capture_admissions ENABLE TRIGGER corpus_admission_observation_immutable").execute(owner).await.unwrap();
}

struct Database {
    owner: PgPool,
    app: PgPool,
    name: TestDatabase,
    role: String,
}
struct Fixture {
    admin: PgPool,
    control: Database,
    content: Option<Database>,
    worker: WorkerTenant,
    tenant: Tenant,
    user: UserContext,
    graph: Uuid,
}
impl Database {
    async fn new(admin: &PgPool, options: &PgConnectOptions, org: bool, history: Option<HistoricalSeed>) -> Self {
        Self::create(admin, options, org, history, false).await
    }
    /// `owner_runtime` reproduces the production tenant configuration that
    /// `tenancy::registry` documents: a Neon-shaped owner (NOSUPERUSER,
    /// CREATEDB, CREATEROLE, BYPASSRLS) migrates the tree, owns every table,
    /// and the runtime pool connects as that SAME owner rather than as
    /// `coralswarm_tenant_app`. A superuser fixture cannot reproduce a missing
    /// grant, because a superuser skips every privilege check.
    async fn create(admin: &PgPool, options: &PgConnectOptions, org: bool, history: Option<HistoricalSeed>, owner_runtime: bool) -> Self {
        let name = TestDatabase::new(options, "cor22_execution");
        let owner_login = if owner_runtime {
            let role = name.role("cor22_neon_owner");
            let password = Uuid::new_v4().simple().to_string();
            sqlx::query(&format!("CREATE ROLE {role} LOGIN NOSUPERUSER CREATEDB CREATEROLE BYPASSRLS NOREPLICATION PASSWORD '{password}'"))
                .execute(admin).await.unwrap();
            sqlx::query(&format!("CREATE DATABASE {name} OWNER {role}"))
                .execute(admin).await.unwrap();
            // A Neon project ships pgvector enabled; its owner cannot install an untrusted extension.
            let superuser_in_db = PgPoolOptions::new().max_connections(1)
                .connect_with(options.clone().database(&name)).await.unwrap();
            sqlx::raw_sql("CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public; CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public;")
                .execute(&superuser_in_db).await.unwrap();
            superuser_in_db.close().await;
            // In production the owner CREATEd coralswarm_tenant_app (0009) and
            // coralswarm_managed_clock (0155), so PG16 gave it the creator's
            // ADMIN-only edge on each. On a shared test cluster another fixture
            // created them first; reproduce exactly that edge, nothing more.
            sqlx::raw_sql(&format!("DO $$ BEGIN GRANT coralswarm_tenant_app TO {role} WITH ADMIN TRUE, INHERIT FALSE, SET FALSE; IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname='coralswarm_managed_clock') THEN GRANT coralswarm_managed_clock TO {role} WITH ADMIN TRUE, INHERIT FALSE, SET FALSE; END IF; END $$;"))
                .execute(admin).await.unwrap();
            Some((role, password))
        } else {
            sqlx::query(&format!("CREATE DATABASE {name}"))
                .execute(admin)
                .await
                .unwrap();
            None
        };
        let owner_options = match &owner_login {
            Some((role, password)) => options.clone().database(&name).username(role).password(password),
            None => options.clone().database(&name),
        };
        let owner = PgPoolOptions::new()
            .max_connections(4)
            .connect_with(owner_options)
            .await
            .unwrap();
        let tenant_migrator=sqlx::migrate!("./migrations_tenant");
        let migrator=if org { &tenant_migrator } else { &coralswarm_backend::CONTROL_MIGRATOR };
        if let Some(seed)=history {
            let standing=migrator.iter().find(|migration|migration.sql.contains("corpus_standing_claim_shape")).map(|migration|migration.version);
            let prefix=sqlx::migrate::Migrator {
                migrations:std::borrow::Cow::Owned(migrator.iter().filter(|migration|standing.is_none_or(|version|migration.version<version)).cloned().collect()),
                ..sqlx::migrate::Migrator::DEFAULT
            };
            prefix.run(&owner).await.unwrap();
            let claim_exists:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='corpus_enrichment_units' AND column_name='standing_generation')").fetch_one(&owner).await.unwrap();
            assert!(!claim_exists,"synthetic historical archive must be restored before the actual standing migration");
            restore_synthetic_history_before_standing_migration(&owner,org,seed).await;
        }
        migrator.run(&owner).await.unwrap();
        if let Ok(path) = std::env::var("COR22_STANDING_DDL_FILE") {
            sqlx::raw_sql(&std::fs::read_to_string(path).unwrap())
                .execute(&owner)
                .await
                .unwrap();
            let role = if org {
                "coralswarm_tenant_app"
            } else {
                "coralswarm_app"
            };
            sqlx::query(&format!("GRANT EXECUTE ON FUNCTION corpus_standing_receipt_event(uuid,uuid,text,text) TO {role}"))
                .execute(&owner).await.unwrap();
        }
        let mut conn = owner.acquire().await.unwrap();
        sqlx::query("SET app.identity_serving_contract='composite-v1'")
            .execute(&mut *conn)
            .await
            .unwrap();
        sqlx::raw_sql(include_str!(
            "../schema/identity-contract.sql"
        ))
        .execute(&mut *conn)
        .await
        .unwrap();
        drop(conn);
        if let Some((role, _)) = owner_login {
            // After tenant 0159 the owner inherits (never SETs) the app role, and
            // still bypasses RLS as itself: current_user and row security are unchanged.
            let shape: (bool, bool, bool, bool, bool, bool) = sqlx::query_as("SELECT r.rolsuper,r.rolbypassrls,pg_get_userbyid(c.relowner)=current_user,pg_has_role(current_user,'coralswarm_tenant_app','SET'),row_security_active('public.corpus_capture_admissions'),pg_has_role('coralswarm_tenant_app',current_user,'MEMBER') FROM pg_roles r,pg_class c WHERE r.rolname=current_user AND c.oid='public.corpus_capture_admissions'::regclass")
                .fetch_one(&owner).await.unwrap();
            assert_eq!(shape, (false, true, true, false, false, false),
                "owner runtime must be the non-superuser BYPASSRLS table owner, with no SET path to tenant_app and no RLS");
            println!("fixture={name} owner_runtime={role} shape={shape:?}");
            return Self { app: owner.clone(), owner, name, role };
        }
        let base_role = if org {
            "coralswarm_tenant_app"
        } else {
            "coralswarm_app"
        };
        let role = name.role("cor22_execution_role");
        let password = Uuid::new_v4().simple().to_string();
        sqlx::query(&format!("CREATE ROLE {role} LOGIN INHERIT NOSUPERUSER NOBYPASSRLS NOCREATEDB NOCREATEROLE NOREPLICATION PASSWORD '{password}'"))
            .execute(admin).await.unwrap();
        sqlx::query(&format!("GRANT {base_role} TO {role}"))
            .execute(admin)
            .await
            .unwrap();
        let app = PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(Duration::from_secs(2))
            .connect_with(
                options
                    .clone()
                    .database(&name)
                    .username(&role)
                    .password(&password),
            )
            .await
            .unwrap();
        let attrs: (bool,bool,bool,bool,bool,bool,i64) = sqlx::query_as("SELECT rolsuper,rolbypassrls,rolcreatedb,rolcreaterole,rolreplication,pg_has_role(current_user,'coralswarm_managed_clock','MEMBER'),(SELECT count(*) FROM pg_tables WHERE schemaname='public' AND tableowner=current_user) FROM pg_roles WHERE rolname=current_user")
            .fetch_one(&app).await.unwrap();
        assert_eq!(attrs, (false, false, false, false, false, false, 0));
        println!("fixture={name} role={role} attrs={attrs:?}");
        Self {
            owner,
            app,
            name,
            role,
        }
    }
}
impl Fixture {
    async fn new(org: bool) -> Self { Self::with_history(org,false).await }
    async fn with_history(org: bool, history: bool) -> Self { Self::create(org,history,false).await }
    async fn create(org: bool, history: bool, owner_runtime: bool) -> Self {
        std::env::set_var("STANDING_PROCESSING_ENABLED", "true");
        // Captures pin the active profile, v2 by default; tests opt in to v3
        // with `Fixture::use_profile`. A version pinned before v2 existed is
        // `Fixture::capture_v1`.
        std::env::set_var("SOURCE_EXTRACTION_PROFILE", "source-atoms-v2");
        std::env::remove_var("JEV_EXTRACTION_BINDING_MODE");
        // The worker's 500 ms transaction-open bound is wall clock. On a
        // loaded machine these one-connection fixture pools can exceed it, so
        // a sequential sweep fails with `PoolTimedOut`. No test here asserts
        // that bound. 5 s still fails a genuine stall fast.
        std::env::set_var("STANDING_BEGIN_BUDGET_MS", "5000");
        let user_id=Uuid::new_v4();
        let account=Uuid::new_v4();
        let historical=history.then(||HistoricalSeed {user:user_id,account,graph:Uuid::new_v4(),org});
        let options = PgConnectOptions::from_str(
            &std::env::var("STANDING_TEST_BASE_URL")
                .expect("isolated STANDING_TEST_BASE_URL required"),
        )
        .unwrap();
        assert!(matches!(
            options.get_host(),
            "127.0.0.1" | "localhost" | "::1"
        ));
        let admin = PgPoolOptions::new()
            .max_connections(2)
            .connect_with(options.clone())
            .await
            .unwrap();
        sqlx::raw_sql("DO $$ BEGIN IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='coralswarm_app') THEN CREATE ROLE coralswarm_app LOGIN NOSUPERUSER NOBYPASSRLS NOCREATEDB NOCREATEROLE NOREPLICATION PASSWORD 'local-standing-fixture'; END IF; IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='coralswarm_tenant_app') THEN CREATE ROLE coralswarm_tenant_app LOGIN NOSUPERUSER NOBYPASSRLS NOCREATEDB NOCREATEROLE NOREPLICATION PASSWORD 'local-standing-fixture'; END IF; END $$;")
            .execute(&admin).await.unwrap();
        let control = Database::new(&admin, &options, false,historical).await;
        let content = if org {
            Some(Database::create(&admin, &options, true,historical,owner_runtime).await)
        } else {
            None
        };
        let org_id = format!("org_execution_{}", account.simple());
        sqlx::query("INSERT INTO users(id,email,clerk_user_id) VALUES($1,$2,$2) ON CONFLICT(id) DO NOTHING")
            .bind(user_id)
            .bind(format!("{user_id}@fixture.local"))
            .execute(&control.owner)
            .await
            .unwrap();
        sqlx::query("INSERT INTO accounts(id,owner_user_id,clerk_org_id) VALUES($1,$2,$3) ON CONFLICT(id) DO NOTHING")
            .bind(account)
            .bind(user_id)
            .bind(org.then_some(&org_id))
            .execute(&control.owner)
            .await
            .unwrap();
        sqlx::query("INSERT INTO account_members(account_id,user_id,role,seat_tier_id) VALUES($1,$2,'owner',(SELECT CASE WHEN is_org_account THEN (SELECT id FROM seat_tiers WHERE code='standard' AND is_active) END FROM accounts WHERE id=$1))")
            .bind(account).bind(user_id).execute(&control.owner).await.unwrap();
        let mut financial = scope::begin_account_scoped(&control.owner, account).await.unwrap();
        coralswarm_backend::billing::renewal::lock_period_rotation(&mut financial, account).await.unwrap();
        sqlx::query("INSERT INTO member_allowance_periods(account_id,user_id,period_start,period_end,granted_credits) VALUES($1,$2,now()-interval '1 day',now()+interval '30 days',100000)")
            .bind(account).bind(user_id).execute(&mut *financial).await.unwrap();
        financial.commit().await.unwrap();
        if let Some(content) = &content {
            sqlx::query("INSERT INTO tenant_meta(org_id) VALUES($1)")
                .bind(&org_id).execute(&content.owner).await.unwrap();
            let mut financial = scope::begin_account_scoped(&control.owner, account).await.unwrap();
            coralswarm_backend::billing::renewal::lock_period_rotation(&mut financial, account).await.unwrap();
            sqlx::query("INSERT INTO tenant_databases(clerk_org_id,account_id,slug,db_secret_ref,status) VALUES($1,$2,$3,'fixture','active')")
                .bind(&org_id).bind(account).bind(format!("standing-{}",&Uuid::new_v4().simple().to_string()[..20]))
                .execute(&mut *financial).await.unwrap();
            financial.commit().await.unwrap();
            let mappings: Vec<(String, Uuid, String, Option<String>, bool, Uuid, Uuid)> = sqlx::query_as(
                "SELECT t.clerk_org_id,t.account_id,t.status,a.clerk_org_id,a.is_org_account,a.owner_user_id,m.user_id FROM tenant_databases t JOIN accounts a ON a.id=t.account_id JOIN account_members m ON m.account_id=a.id",
            ).fetch_all(&control.owner).await.unwrap();
            assert_eq!(mappings, vec![(org_id.clone(), account, "active".into(), Some(org_id.clone()), true, user_id, user_id)]);
            let fingerprints: Vec<String> = sqlx::query_scalar("SELECT org_id FROM tenant_meta WHERE id")
                .fetch_all(&content.owner).await.unwrap();
            assert_eq!(fingerprints, vec![org_id.clone()]);
        }
        let db = content.as_ref().unwrap_or(&control);
        let graph = sqlx::query_scalar("INSERT INTO graphs(account_id,name,kind,owner_user_id) VALUES($1,'Standing native acceptance','member',$2) RETURNING id")
            .bind(account).bind(user_id).fetch_one(&db.owner).await.unwrap();
        let worker = WorkerTenant {
            tenant: if org {
                TenantId::Org(org_id.clone())
            } else {
                TenantId::Consumer
            },
            pool: db.app.clone(),
            store: Arc::new(PostgresStore::new(db.app.clone())),
        };
        let tenant = Tenant {
            store: Some(worker.store.clone()),
            pool: Some(db.app.clone()),
            decision: Some(if org {
                TenancyDecision::Org(TenantContext {
                    org_id,
                    account_id: account,
                    user_id,
                    pool: db.app.clone(),
                    page_pool: None,
                })
            } else {
                TenancyDecision::Personal
            }),
            class: coralswarm_backend::route_class::RouteClass::Other,
        };
        Self {
            admin,
            control,
            content,
            worker,
            tenant,
            user: UserContext {
                user_id,
                account_id: account,
                role: "owner".into(),
                device_id: "fixture".into(),
            },
            graph,
        }
    }
    fn owner(&self) -> &PgPool {
        &self.content.as_ref().unwrap_or(&self.control).owner
    }
    async fn capture(&self, kind: &str, id: &str, text: &str) -> Uuid {
        self.capture_with(kind, "fixture:standing", id, text, json!({}), Some(self.user.user_id)).await
    }
    async fn capture_with(&self, kind: &str, namespace: &str, id: &str, text: &str, metadata: Value, actor: Option<Uuid>) -> Uuid {
        self.capture_into(self.graph, kind, namespace, id, text, metadata, actor).await
    }
    #[allow(clippy::too_many_arguments)]
    async fn capture_into(&self, graph: Uuid, kind: &str, namespace: &str, id: &str, text: &str, metadata: Value, actor: Option<Uuid>) -> Uuid {
        let mut tx = match self.worker.tenant {
            TenantId::Consumer => {
                scope::begin_account_scoped(&self.worker.pool, self.user.account_id)
                    .await
                    .unwrap()
            }
            TenantId::Org(_) => {
                scope::begin_tenant_system_scoped(&self.worker.pool, self.user.account_id)
                    .await
                    .unwrap()
            }
        };
        let capture = CaptureSource {
            source_type: kind,
            provider_namespace: namespace,
            source_id: id,
            normalized_text: text,
            representation: "original_text",
            metadata: &metadata,
            occurred_at: None,
            participants: &[],
            ingested_by: actor,
            scope: "private",
            duration_ms: None,
            segments: None,
        };
        let saved = source_evidence::capture_source_version_tx(
            &mut tx,
            None,
            graph,
            self.user.account_id,
            &capture,
            None,
            &[42; 32],
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
        saved.version_id
    }
    /// A version captured while `source-atoms-v1` was the profile, extracted
    /// under it: done, one v1 window, and one v1 atom quoting the whole text.
    /// No worker extracts v1 anymore, so the state is written directly.
    /// Returns the version, the atom and its passage.
    async fn capture_v1(&self, id: &str, text: &str, statement: &str) -> (Uuid, Uuid, Uuid) {
        let mut tx = match self.worker.tenant {
            TenantId::Consumer => scope::begin_account_scoped(&self.worker.pool, self.user.account_id).await.unwrap(),
            TenantId::Org(_) => scope::begin_tenant_system_scoped(&self.worker.pool, self.user.account_id).await.unwrap(),
        };
        let metadata = json!({});
        let capture = CaptureSource {
            source_type: "note", provider_namespace: "fixture:standing", source_id: id, normalized_text: text,
            representation: "original_text", metadata: &metadata, occurred_at: None, participants: &[],
            ingested_by: Some(self.user.user_id), scope: "private", duration_ms: None, segments: None,
        };
        let version = source_evidence::capture_historical_v1_tx(&mut tx, self.graph, self.user.account_id, &capture, &[42; 32])
            .await.unwrap().version_id;
        tx.commit().await.unwrap();
        let end = text.chars().count() as i64;
        sqlx::query("UPDATE source_versions SET extraction_status='done',next_start_char=$2 WHERE version_id=$1")
            .bind(version).bind(end).execute(self.owner()).await.unwrap();
        sqlx::query("INSERT INTO semantic_atom_windows(graph_id,source_version_id,extraction_version,start_char,end_char) VALUES($1,$2,'source-atoms-v1',0,$3)")
            .bind(self.graph).bind(version).bind(end as i32).execute(self.owner()).await.unwrap();
        let atom: Uuid = sqlx::query_scalar("INSERT INTO semantic_atoms(graph_id,source_version_id,ordinal,kind,statement,extraction_version,model) VALUES($1,$2,0,'fact',$3,'source-atoms-v1','fixture') RETURNING semantic_atom_id")
            .bind(self.graph).bind(version).bind(statement).fetch_one(self.owner()).await.unwrap();
        let passage: Uuid = sqlx::query_scalar("INSERT INTO semantic_atom_passages(semantic_atom_id,source_version_id,graph_id,start_char,end_char,quote) VALUES($1,$2,$3,0,$4,$5) RETURNING passage_id")
            .bind(atom).bind(version).bind(self.graph).bind(end as i32).bind(text).fetch_one(self.owner()).await.unwrap();
        (version, atom, passage)
    }
    async fn served(&self, version: Uuid) -> Vec<(String, String)> {
        sqlx::query_as("SELECT extraction_version,statement FROM served_semantic_atoms WHERE source_version_id=$1 ORDER BY extraction_version,ordinal")
            .bind(version).fetch_all(self.owner()).await.unwrap()
    }
    async fn sweep(&self) {
        coralswarm_backend::test_standing_sweep(&self.worker, &self.control.app)
            .await
            .unwrap();
    }
    async fn done(self) {
        self.worker.pool.close().await;
        self.control.app.close().await;
        if let Some(content) = &self.content {
            content.owner.close().await;
        }
        self.control.owner.close().await;
        self.admin.close().await;
    }
}

async fn provider() -> (
    coralswarm_backend::llm::openai::OpenAiProvider,
    Arc<AtomicUsize>,
    tokio::task::JoinHandle<()>,
) {
    let hits = Arc::new(AtomicUsize::new(0));
    let counter = hits.clone();
    let app = axum::Router::new().fallback(move |axum::Json(body): axum::Json<Value>| {
        let counter = counter.clone(); async move {
            counter.fetch_add(1,Ordering::SeqCst);
            assert_eq!(body["model"],PRIMARY); assert_eq!(body["provider"]["require_parameters"],true);
            let atoms = json!({"atoms":[{"kind":"fact","text":"Retained evidence.","evidence_quote":"Retained evidence.","entities":[{"mention":"Retained evidence","type":"topic"}]}]});
            axum::Json(json!({"id":"gen-standing","model":PRIMARY,"provider":"Loopback","choices":[{"message":{"content":atoms.to_string()},"finish_reason":"stop"}],"usage":{"prompt_tokens":20,"completion_tokens":10,"cost":0.0004}}))
        }
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (
        coralswarm_backend::llm::openai::OpenAiProvider::openrouter(
            reqwest::Client::new(),
            "fixture-only",
        )
        .with_base_url(base),
        hits,
        handle,
    )
}

async fn sweep_to_library(org: bool) {
    // One job per tick. A tenant-plane job needs a connection from BOTH
    // one-connection fixture pools (control `app`, then the content `app`); at
    // the default SOURCE_ATOMS_CONCURRENCY of 8, sibling jobs each take one
    // pool and wait on the other until the 2 s `acquire_timeout` fires, and
    // the version is held as `infrastructure_unavailable` (extraction_status
    // `blocked`). Production pools are not one connection wide; serialising
    // keeps this test about what it asserts rather than fixture pool width.
    let _serial = EnvGuard::set(source_atoms::CONCURRENCY_ENV, "1");
    let f = Fixture::new(org).await;
    let mut versions = Vec::new();
    for kind in [
        "slack",
        "coding_session",
        "outlook",
        "teams_channel",
        "note",
    ] {
        versions.push(f.capture(kind, kind, "Retained evidence.").await);
    }
    versions.push(
        f.capture(
            "coding_session",
            "multi-window",
            &format!(
                "Retained evidence.\n{}Retained evidence.",
                "padding\n".repeat(1400)
            ),
        )
        .await,
    );
    assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM jobs WHERE kind IN ('ExtractStandingSourceAtoms','ExtractStandingMeetingAtoms')").fetch_one(f.owner()).await.unwrap(),0);
    f.sweep().await;
    f.sweep().await;
    let owners: i64 =
        sqlx::query_scalar("SELECT count(*) FROM corpus_enrichment_units WHERE execution_owner")
            .fetch_one(f.owner())
            .await
            .unwrap();
    assert_eq!(owners, 6);
    let counters: (i64,i64,i64)=sqlx::query_as("SELECT sum(admitted_items)::bigint,sum(attempted_characters)::bigint,sum(attempted_model_calls)::bigint FROM corpus_enrichment_runs WHERE run_kind='standing'").fetch_one(f.owner()).await.unwrap();
    assert_eq!(counters, (0, 0, 0));
    let pending: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM jobs WHERE kind='ExtractStandingSourceAtoms' AND status='pending'",
    )
    .fetch_one(f.owner())
    .await
    .unwrap();
    assert_eq!(pending, 6);
    let (llm, hits, server) = provider().await;
    for _ in 0..8 {
        source_atoms::tick(&f.worker, &f.control.app, &llm, None)
            .await
            .unwrap();
    }
    let states: Vec<(String, i64)> = sqlx::query_as(
        "SELECT extraction_status,next_start_char FROM source_versions ORDER BY version_id",
    )
    .fetch_all(f.owner())
    .await
    .unwrap();
    assert_eq!(states.len(), 6);
    assert!(
        states.iter().all(|s| s.0 == "done" && s.1 > 0),
        "{states:?}"
    );
    assert!(
        hits.load(Ordering::SeqCst) > 6,
        "multiwindow must make another actual HTTP attempt"
    );
    let receipt_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM managed_attempt_receipts WHERE state='billed'")
            .fetch_one(f.owner())
            .await
            .unwrap();
    assert_eq!(receipt_count, hits.load(Ordering::SeqCst) as i64);
    let persisted: (i64,i64,i64,i64)=sqlx::query_as("SELECT (SELECT count(*) FROM semantic_atoms),(SELECT count(*) FROM semantic_atom_passages),(SELECT count(*) FROM node_semantic_atoms),(SELECT count(*) FROM semantic_atom_windows)").fetch_one(f.owner()).await.unwrap();
    assert_eq!(
        persisted,
        (receipt_count, receipt_count, receipt_count, receipt_count)
    );
    let clocks: (i64,i64)=sqlx::query_as("SELECT count(*) FILTER(WHERE state='processed' AND processed_at IS NOT NULL),count(*) FILTER(WHERE NOT ever_held AND hold_basis='standing_admitted') FROM corpus_capture_admissions").fetch_one(f.owner()).await.unwrap();
    assert_eq!(clocks, (6, 6));
    // Terminal receipt history is immutable. Let the actual clocks cross the
    // billing worker's 180-second CURSOR_LAG, with one second of boundary margin.
    // This bounds fixture eligibility waiting; it does not measure a capture SLO.
    tokio::time::timeout(Duration::from_secs(185), async {
        let (eligible_at, database_now): (chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>) =
            sqlx::query_as("SELECT max(settled_at)+interval '181 seconds',clock_timestamp() FROM managed_attempt_receipts WHERE state='billed'")
                .fetch_one(f.owner()).await.unwrap();
        // Derivation uses Utc::now(), while receipts use the database clock.
        let wait = (eligible_at - database_now.min(chrono::Utc::now()))
            .to_std().unwrap_or(Duration::ZERO);
        assert!(wait <= Duration::from_secs(182), "billing eligibility wait exceeds fixture bound: {wait:?}");
        let started = std::time::Instant::now();
        tokio::time::sleep(wait).await;
        let (database_now, host_now) = loop {
            let database_now: chrono::DateTime<chrono::Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
                .fetch_one(f.owner()).await.unwrap();
            let host_now = chrono::Utc::now();
            if database_now >= eligible_at && host_now >= eligible_at {
                break (database_now, host_now);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        assert!(database_now >= eligible_at && host_now >= eligible_at,
            "real clocks have not crossed billing eligibility: database={database_now} host={host_now} eligible={eligible_at}");
        println!("actual_billing_eligibility org={org} waited={:?} eligible_at={eligible_at} database_now={database_now}", started.elapsed());
    }).await.expect("real billing eligibility must complete within 185 seconds");
    coralswarm_backend::workers::billing_derivation::tick(&f.worker, &f.control.app)
        .await
        .unwrap();
    coralswarm_backend::workers::billing_derivation::tick(&f.worker, &f.control.app)
        .await
        .unwrap();
    let charged: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM credit_ledger WHERE source_receipt_id IS NOT NULL AND credits<0",
    )
    .fetch_one(&f.control.owner)
    .await
    .unwrap();
    assert_eq!(charged, receipt_count);
    let scope: LibraryScope = serde_json::from_value(json!({"graph_ids":[f.graph]})).unwrap();
    let read = library_read::retrieve_scoped(&f.tenant, &f.user, scope, "Retained", 20)
        .await
        .unwrap();
    assert!(
        !read["outcomes"]["items"].as_array().unwrap().is_empty(),
        "{read}"
    );
    println!("actual_sweep_twice_to_library org={org} captures={} http={receipt_count} persisted={persisted:?} charged={charged}",versions.len());
    server.abort();
    f.done().await;
}
#[tokio::test]
async fn consumer_standing_actual_sweep_queue_http_billing_atoms_library() {
    sweep_to_library(false).await;
}
#[tokio::test]
async fn tenant_standing_actual_sweep_queue_http_billing_atoms_library() {
    sweep_to_library(true).await;
}

/// Production tenant DSNs still connect as the tables' OWNER (see
/// `tenancy::registry`, until OPS-4-T), not as `coralswarm_tenant_app`. Tenant
/// 0155 granted the receipt clock only to the app role, so on 2026-10-18 every
/// standing adoption failed in production with `permission denied for function
/// corpus_standing_receipt_event` while every other test here, all of which
/// run the worker as an app-role member, stayed green. Tenant 0159 bridges this
/// until OPS-4-T by making the owner an INHERIT member of the app role.
#[tokio::test]
async fn tenant_standing_owner_runtime_adopts_and_clocks_receipts() {
    let f = Fixture::create(true, false, true).await;
    let runtime: (bool, bool, bool) = sqlx::query_as("SELECT r.rolsuper,r.rolbypassrls,pg_get_userbyid(c.relowner)=current_user FROM pg_roles r,pg_class c WHERE r.rolname=current_user AND c.oid='public.corpus_capture_admissions'::regclass")
        .fetch_one(&f.worker.pool).await.unwrap();
    assert_eq!(runtime, (false, true, true), "worker must run as the non-superuser table owner, as production does");
    let mut versions = Vec::new();
    for id in ["owner-runtime-a", "owner-runtime-b"] {
        versions.push(f.capture("note", id, "Retained evidence.").await);
    }
    f.sweep().await;
    f.sweep().await;
    let adopted: (i64, i64, i64) = sqlx::query_as("SELECT (SELECT count(*) FROM corpus_enrichment_units WHERE execution_owner),(SELECT count(*) FROM jobs WHERE kind='ExtractStandingSourceAtoms' AND status='pending'),(SELECT count(*) FROM corpus_capture_admissions WHERE NOT ever_held AND hold_basis='standing_admitted')")
        .fetch_one(f.owner()).await.unwrap();
    assert_eq!(adopted, (2, 2, 2), "owner runtime must adopt, queue and clock every standing receipt");
    let inherited: (bool, bool, bool) = sqlx::query_as("SELECT pg_has_role(current_user,'coralswarm_tenant_app','USAGE'),pg_has_role(current_user,'coralswarm_tenant_app','SET'),row_security_active('public.graphs')")
        .fetch_one(&f.worker.pool).await.unwrap();
    assert_eq!(inherited, (true, false, false), "0159: inherit app-role grants without SET ROLE or row security");
    let mut tx = scope::begin_tenant_system_scoped(&f.worker.pool, f.user.account_id).await.unwrap();
    let requeued: bool = sqlx::query_scalar("SELECT corpus_standing_receipt_event($1,$2,'requeue',NULL)")
        .bind(f.graph).bind(versions[0]).fetch_one(&mut *tx).await.unwrap();
    assert!(requeued, "owner runtime executes the clock directly");
    tx.rollback().await.unwrap();
    let public_execute: bool = sqlx::query_scalar("SELECT has_function_privilege('public','public.corpus_standing_receipt_event(uuid,uuid,text,text)','EXECUTE')")
        .fetch_one(f.owner()).await.unwrap();
    assert!(!public_execute, "the clock stays closed to PUBLIC");
    println!("owner_runtime_adoption adopted={adopted:?}");
    f.done().await;
}

impl Fixture {
    async fn capture_as(&self, id: &str, actor: Uuid) -> Uuid {
        let mut tx = match self.worker.tenant {
            TenantId::Consumer => {
                scope::begin_account_scoped(&self.worker.pool, self.user.account_id)
                    .await
                    .unwrap()
            }
            TenantId::Org(_) => {
                scope::begin_tenant_system_scoped(&self.worker.pool, self.user.account_id)
                    .await
                    .unwrap()
            }
        };
        let capture = CaptureSource {
            source_type: "slack",
            provider_namespace: "fixture:standing",
            source_id: id,
            normalized_text: "Retained evidence.",
            representation: "original_text",
            metadata: &json!({}),
            occurred_at: None,
            participants: &[],
            ingested_by: Some(actor),
            scope: "private",
            duration_ms: None,
            segments: None,
        };
        let saved = source_evidence::capture_source_version_tx(
            &mut tx,
            None,
            self.graph,
            self.user.account_id,
            &capture,
            None,
            &[42; 32],
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
        saved.version_id
    }
    async fn member(&self, role: &str) -> Uuid {
        let user = Uuid::new_v4();
        sqlx::query("INSERT INTO users(id,email,clerk_user_id) VALUES($1,$2,$2)")
            .bind(user)
            .bind(format!("{user}@fixture.local"))
            .execute(&self.control.owner)
            .await
            .unwrap();
        sqlx::query("INSERT INTO account_members(account_id,user_id,role,seat_tier_id) VALUES($1,$2,$3,(SELECT CASE WHEN is_org_account THEN (SELECT id FROM seat_tiers WHERE code='standard' AND is_active) END FROM accounts WHERE id=$1))")
            .bind(self.user.account_id).bind(user).bind(role).execute(&self.control.owner).await.unwrap();
        let mut financial = scope::begin_account_scoped(&self.control.owner, self.user.account_id).await.unwrap();
        coralswarm_backend::billing::renewal::lock_period_rotation(&mut financial, self.user.account_id).await.unwrap();
        sqlx::query("INSERT INTO member_allowance_periods(account_id,user_id,period_start,period_end,granted_credits) VALUES($1,$2,now()-interval '1 day',now()+interval '30 days',100000)")
            .bind(self.user.account_id).bind(user).execute(&mut *financial).await.unwrap();
        financial.commit().await.unwrap();
        user
    }
    async fn run(&self) -> Uuid {
        sqlx::query_scalar("SELECT run_id FROM corpus_enrichment_runs WHERE graph_id=$1 AND extraction_version='source-atoms-v2' AND retired_at IS NULL AND run_kind='standing'")
            .bind(self.graph).fetch_one(self.owner()).await.unwrap()
    }
}

async fn fresh_handoff_retired_owned_payer(org: bool) {
    let f = Fixture::new(org).await;
    sqlx::query("UPDATE graphs SET kind='org',owner_user_id=NULL WHERE id=$1")
        .bind(f.graph)
        .execute(f.owner())
        .await
        .unwrap();
    let replacement = f.member("admin").await;
    let actor = f.member("member").await;
    let version = f.capture_as("capture-actor-payer", actor).await;
    f.sweep().await;
    let run = f.run().await;
    sqlx::query("UPDATE corpus_enrichment_runs SET retired_at=now() WHERE run_id=$1")
        .bind(run)
        .execute(f.owner())
        .await
        .unwrap();
    sqlx::query("DELETE FROM account_members WHERE account_id=$1 AND user_id=$2")
        .bind(f.user.account_id)
        .bind(f.user.user_id)
        .execute(&f.control.owner)
        .await
        .unwrap();
    let metadata = coralswarm_backend::test_standing_principal(
        f.worker.tenant.clone(),
        &f.worker.pool,
        &f.control.app,
        f.user.account_id,
        f.graph,
        run,
        "source-atoms-v2",
        f.user.user_id,
    )
    .await
    .unwrap();
    assert_eq!(metadata, "Skipped(Retired)");
    let (llm, hits, server) = provider().await;
    source_atoms::tick(&f.worker, &f.control.app, &llm, None)
        .await
        .unwrap();
    assert_eq!(
        hits.load(Ordering::SeqCst),
        1,
        "retired owner must continue its existing unit with fresh replacement authority"
    );
    let binding: (Uuid, Uuid) = sqlx::query_as(
        "SELECT requested_by,execution_principal FROM corpus_enrichment_runs WHERE run_id=$1",
    )
    .bind(run)
    .fetch_one(f.owner())
    .await
    .unwrap();
    assert_eq!(binding, (f.user.user_id, replacement));
    let payer: Uuid = sqlx::query_scalar(
        "SELECT attributed_user_id FROM managed_attempt_receipts WHERE source_version_id=$1",
    )
    .bind(version)
    .fetch_one(f.owner())
    .await
    .unwrap();
    assert_eq!(
        payer, actor,
        "replacement admin is authority only; current capture member remains frozen payer"
    );
    let status: String =
        sqlx::query_scalar("SELECT extraction_status FROM source_versions WHERE version_id=$1")
            .bind(version)
            .fetch_one(f.owner())
            .await
            .unwrap();
    assert_eq!(status, "done");
    let fresh = f.capture_as("no-retired-adoption", actor).await;
    f.sweep().await;
    let old_adopted: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM corpus_enrichment_units WHERE run_id=$1 AND holding_id=$2)",
    )
    .bind(run)
    .bind(fresh)
    .fetch_one(f.owner())
    .await
    .unwrap();
    assert!(!old_adopted);
    server.abort();
    f.done().await;
}
#[tokio::test]
async fn consumer_standing_retired_owned_handoff_preserves_capture_payer() {
    fresh_handoff_retired_owned_payer(false).await;
}
#[tokio::test]
async fn tenant_standing_retired_owned_handoff_preserves_capture_payer() {
    fresh_handoff_retired_owned_payer(true).await;
}

async fn standing_holds_exclusions_and_clocks(org: bool) {
    let f = Fixture::new(org).await;
    let good = f.capture("note", "good", "Retained evidence.").await;
    let denied = f
        .capture("slack", "source-loss", "Retained evidence.")
        .await;
    let failed = f.capture("outlook", "failed", "Retained evidence.").await;
    sqlx::query("UPDATE source_versions SET extraction_status='failed',extraction_error='invalid schema' WHERE version_id=$1").bind(failed).execute(f.owner()).await.unwrap();
    let old = f
        .capture("teams_channel", "pre-floor", "Retained evidence.")
        .await;
    sqlx::query(
        "UPDATE source_versions SET captured_at='2026-10-12T00:00:00Z' WHERE version_id=$1",
    )
    .bind(old)
    .execute(f.owner())
    .await
    .unwrap();
    f.sweep().await;
    f.sweep().await;
    let excluded: i64 =
        sqlx::query_scalar("SELECT count(*) FROM corpus_enrichment_units WHERE holding_id=ANY($1)")
            .bind(vec![failed, old])
            .fetch_one(f.owner())
            .await
            .unwrap();
    assert_eq!(excluded, 0);
    sqlx::query("UPDATE source_items SET access_state='restricted' WHERE source_item_id=(SELECT source_item_id FROM source_versions WHERE version_id=$1)").bind(denied).execute(f.owner()).await.unwrap();
    let (llm, hits, server) = provider().await;
    for _ in 0..3 {
        source_atoms::tick(&f.worker, &f.control.app, &llm, None)
            .await
            .unwrap();
    }
    let good_status: String =
        sqlx::query_scalar("SELECT extraction_status FROM source_versions WHERE version_id=$1")
            .bind(good)
            .fetch_one(f.owner())
            .await
            .unwrap();
    assert_eq!(good_status, "done");
    let run = f.run().await;
    let status: String =
        sqlx::query_scalar("SELECT status FROM corpus_enrichment_runs WHERE run_id=$1")
            .bind(run)
            .fetch_one(f.owner())
            .await
            .unwrap();
    assert_eq!(
        status, "running",
        "one inaccessible unit must not pause other work"
    );
    assert_eq!(hits.load(Ordering::SeqCst), 1);
    let before:Value=sqlx::query_scalar("SELECT jsonb_build_object('observed',observed_at,'processed',processed_at,'held',ever_held,'basis',hold_basis) FROM corpus_capture_admissions WHERE source_version_id=$1").bind(good).fetch_one(f.owner()).await.unwrap();
    let mut tx = match f.worker.tenant {
        TenantId::Consumer => scope::begin_account_scoped(&f.worker.pool, f.user.account_id)
            .await
            .unwrap(),
        TenantId::Org(_) => scope::begin_tenant_system_scoped(&f.worker.pool, f.user.account_id)
            .await
            .unwrap(),
    };
    let error=sqlx::query("UPDATE corpus_capture_admissions SET ever_held=false,hold_basis='never_held',processed_at=now()-interval '1 year' WHERE source_version_id=$1").bind(good).execute(&mut *tx).await.unwrap_err();
    assert_eq!(
        error.as_database_error().unwrap().code().as_deref(),
        Some("42501")
    );
    tx.rollback().await.unwrap();
    f.sweep().await;
    let after:Value=sqlx::query_scalar("SELECT jsonb_build_object('observed',observed_at,'processed',processed_at,'held',ever_held,'basis',hold_basis) FROM corpus_capture_admissions WHERE source_version_id=$1").bind(good).fetch_one(f.owner()).await.unwrap();
    assert_eq!(before, after);
    let attrs:(bool,bool,bool,bool,bool,bool,bool,bool)=sqlx::query_as("SELECT r.rolcanlogin,r.rolsuper,r.rolbypassrls,r.rolcreatedb,r.rolcreaterole,r.rolreplication,pg_has_role(current_user,r.oid,'MEMBER'),has_function_privilege(current_user,p.oid,'EXECUTE') FROM pg_proc p JOIN pg_roles r ON r.oid=p.proowner WHERE p.oid='corpus_standing_receipt_event(uuid,uuid,text,text)'::regprocedure")
        .fetch_one(&f.worker.pool).await.unwrap();
    assert_eq!(attrs,(false,false,false,false,false,false,false,true));
    for (event,reason) in [("unrecognized",None),("hold",Some("unrecognized"))] {
        let mut tx=match f.worker.tenant {
            TenantId::Consumer=>scope::begin_account_scoped(&f.worker.pool,f.user.account_id).await.unwrap(),
            TenantId::Org(_)=>scope::begin_tenant_system_scoped(&f.worker.pool,f.user.account_id).await.unwrap(),
        };
        let error=sqlx::query_scalar::<_,bool>("SELECT corpus_standing_receipt_event($1,$2,$3,$4)")
            .bind(f.graph).bind(good).bind(event).bind(reason).fetch_one(&mut *tx).await.unwrap_err();
        assert_eq!(error.as_database_error().and_then(|error|error.code()).as_deref(),Some("P0001"));
        tx.rollback().await.unwrap();
    }
    let pending=f.capture("note","clock-queue-prerequisite","Retained evidence.").await;
    f.sweep().await;
    let job=pending_job(&f,pending).await;
    for (version,expected) in [(pending,true),(Uuid::new_v4(),false),(good,false)] {
        let mut tx=match f.worker.tenant {
            TenantId::Consumer=>scope::begin_account_scoped(&f.worker.pool,f.user.account_id).await.unwrap(),
            TenantId::Org(_)=>scope::begin_tenant_system_scoped(&f.worker.pool,f.user.account_id).await.unwrap(),
        };
        let event:bool=sqlx::query_scalar("SELECT corpus_standing_receipt_event($1,$2,'requeue',NULL)")
            .bind(f.graph).bind(version).fetch_one(&mut *tx).await.unwrap();
        assert_eq!(event,expected);
        tx.rollback().await.unwrap();
    }
    coralswarm_backend::jobs::queue::complete(&f.worker.pool,job,coralswarm_backend::jobs::queue::JobOutcome::NothingToDo).await.unwrap();
    let mut tx=match f.worker.tenant {
        TenantId::Consumer=>scope::begin_account_scoped(&f.worker.pool,f.user.account_id).await.unwrap(),
        TenantId::Org(_)=>scope::begin_tenant_system_scoped(&f.worker.pool,f.user.account_id).await.unwrap(),
    };
    let no_queue:bool=sqlx::query_scalar("SELECT corpus_standing_receipt_event($1,$2,'requeue',NULL)")
        .bind(f.graph).bind(pending).fetch_one(&mut *tx).await.unwrap();
    assert!(!no_queue,"clock cannot certify queued history without an actual active compatible queue row");
    tx.rollback().await.unwrap();
    let final_history:Value=sqlx::query_scalar("SELECT jsonb_build_object('observed',observed_at,'processed',processed_at,'held',ever_held,'basis',hold_basis) FROM corpus_capture_admissions WHERE source_version_id=$1")
        .bind(good).fetch_one(f.owner()).await.unwrap();
    assert_eq!(final_history,before);
    server.abort();
    f.done().await;
}
#[tokio::test]
async fn consumer_standing_unit_hold_failed_floor_and_protected_clocks() {
    standing_holds_exclusions_and_clocks(false).await;
}
#[tokio::test]
async fn tenant_standing_unit_hold_failed_floor_and_protected_clocks() {
    standing_holds_exclusions_and_clocks(true).await;
}

#[tokio::test]
async fn standing_registered_worker_completes_job_omitted_by_old_kind_filter() {
    let f = Fixture::new(false).await;
    f.capture("note", "old-kind-red", "Retained evidence.")
        .await;
    f.sweep().await;
    let queued: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM jobs WHERE kind='ExtractStandingSourceAtoms' AND status='pending'",
    )
    .fetch_one(f.owner())
    .await
    .unwrap();
    assert_eq!(queued, 1);
    let old_jobs = coralswarm_backend::jobs::queue::dequeue(
        &f.worker.pool,
        &["ExtractMeetingAtoms", "ExtractSourceAtoms"],
        2,
    )
    .await
    .unwrap();
    assert!(
        old_jobs.is_empty(),
        "native old kind filter omits the actual standing row"
    );
    let (llm, hits, server) = provider().await;
    let tick = source_atoms::tick(&f.worker, &f.control.app, &llm, None)
        .await
        .unwrap();
    assert_eq!(tick.processed, 1);
    assert_eq!(hits.load(Ordering::SeqCst), 1);
    let done: i64 =
        sqlx::query_scalar("SELECT count(*) FROM source_versions WHERE extraction_status='done'")
            .fetch_one(f.owner())
            .await
            .unwrap();
    assert_eq!(done, 1);
    server.abort();
    f.done().await;
}

async fn standing_pause_axes_and_daily_resume(org: bool) {
    let f = Fixture::new(org).await;
    let version = f
        .capture("note", "resume-source", "Retained evidence.")
        .await;
    f.sweep().await;
    let run = f.run().await;
    let mut tx = match f.worker.tenant {
        TenantId::Consumer => scope::begin_account_scoped(&f.worker.pool, f.user.account_id)
            .await
            .unwrap(),
        TenantId::Org(_) => scope::begin_tenant_system_scoped(&f.worker.pool, f.user.account_id)
            .await
            .unwrap(),
    };
    coralswarm_backend::corpus_enrichment::test_standing_pause_tx(
        &mut tx,
        f.graph,
        run,
        "operator_pause",
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    sqlx::query("UPDATE corpus_enrichment_runs SET retired_at=now() WHERE run_id=$1")
        .bind(run)
        .execute(f.owner())
        .await
        .unwrap();
    sqlx::query("INSERT INTO account_prefs(account_id,prefs) VALUES($1,'{\"managed_processing\":\"off\"}') ON CONFLICT(account_id) DO UPDATE SET prefs=EXCLUDED.prefs").bind(f.user.account_id).execute(&f.control.owner).await.unwrap();
    f.sweep().await;
    let pause: String = sqlx::query_scalar(
        "SELECT standing_pause_state FROM corpus_enrichment_runs WHERE run_id=$1",
    )
    .bind(run)
    .fetch_one(f.owner())
    .await
    .unwrap();
    assert_eq!(pause, "both");
    sqlx::query("UPDATE account_prefs SET prefs='{}' WHERE account_id=$1")
        .bind(f.user.account_id)
        .execute(&f.control.owner)
        .await
        .unwrap();
    f.sweep().await;
    let pause: String = sqlx::query_scalar(
        "SELECT standing_pause_state FROM corpus_enrichment_runs WHERE run_id=$1",
    )
    .bind(run)
    .fetch_one(f.owner())
    .await
    .unwrap();
    assert_eq!(pause, "operator");
    let (llm, hits, server) = provider().await;
    source_atoms::tick(&f.worker, &f.control.app, &llm, None)
        .await
        .unwrap();
    assert_eq!(hits.load(Ordering::SeqCst), 0);
    let mut tx = match f.worker.tenant {
        TenantId::Consumer => scope::begin_account_scoped(&f.worker.pool, f.user.account_id)
            .await
            .unwrap(),
        TenantId::Org(_) => scope::begin_tenant_system_scoped(&f.worker.pool, f.user.account_id)
            .await
            .unwrap(),
    };
    coralswarm_backend::corpus_enrichment::test_standing_pause_tx(
        &mut tx,
        f.graph,
        run,
        "operator_resume",
    )
    .await
    .unwrap();
    coralswarm_backend::corpus_enrichment::hold_window_tx(
        &mut tx,
        f.graph,
        version,
        "source-atoms-v2",
        "daily_budget_exhausted",
    )
    .await
    .unwrap();
    let next_utc:bool=sqlx::query_scalar("SELECT retry_after=(date_trunc('day',now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC')+interval '1 day' FROM corpus_enrichment_units WHERE holding_id=$1")
        .bind(version).fetch_one(&mut *tx).await.unwrap();
    assert!(next_utc);
    tx.commit().await.unwrap();
    sqlx::query("INSERT INTO managed_budget_periods(account_id,purpose,period_start,limit_micro_usd,charged_micro_usd) VALUES($1,'source_extraction',(now() AT TIME ZONE 'UTC')::date-1,10000000,10000000)")
        .bind(f.user.account_id).execute(f.owner()).await.unwrap();
    // Move only fixture retry readiness to represent arrival of the next UTC day; financial history remains immutable.
    sqlx::query("UPDATE corpus_enrichment_units SET retry_after=now()-interval '1 second' WHERE holding_id=$1").bind(version).execute(f.owner()).await.unwrap();
    f.sweep().await;
    source_atoms::tick(&f.worker, &f.control.app, &llm, None)
        .await
        .unwrap();
    assert_eq!(hits.load(Ordering::SeqCst), 1);
    let status: String =
        sqlx::query_scalar("SELECT extraction_status FROM source_versions WHERE version_id=$1")
            .bind(version)
            .fetch_one(f.owner())
            .await
            .unwrap();
    assert_eq!(status, "done");
    let old:i64=sqlx::query_scalar("SELECT charged_micro_usd FROM managed_budget_periods WHERE account_id=$1 AND purpose='source_extraction' AND period_start=(now() AT TIME ZONE 'UTC')::date-1").bind(f.user.account_id).fetch_one(f.owner()).await.unwrap();
    assert_eq!(old, 10000000);
    let current:i64=sqlx::query_scalar("SELECT charged_micro_usd FROM managed_budget_periods WHERE account_id=$1 AND purpose='source_extraction' AND period_start=(now() AT TIME ZONE 'UTC')::date").bind(f.user.account_id).fetch_one(f.owner()).await.unwrap();
    assert_eq!(current, 400);
    server.abort();
    f.done().await;
}
#[tokio::test]
async fn consumer_standing_retired_pause_axes_and_next_utc_state_resumes() {
    standing_pause_axes_and_daily_resume(false).await;
}
#[tokio::test]
async fn tenant_standing_retired_pause_axes_and_next_utc_state_resumes() {
    standing_pause_axes_and_daily_resume(true).await;
}

async fn late_old_response_cannot_hold_new_generation(org: bool) {
    let f = Fixture::new(org).await;
    let version = f
        .capture("note", "generation-race", "Retained evidence.")
        .await;
    f.sweep().await;
    let first_started = Arc::new(tokio::sync::Notify::new());
    let second_started = Arc::new(tokio::sync::Notify::new());
    let release_first = Arc::new(tokio::sync::Notify::new());
    let release_second = Arc::new(tokio::sync::Notify::new());
    let hits = Arc::new(AtomicUsize::new(0));
    let (counter, one, two, release_one, release_two) = (
        hits.clone(),
        first_started.clone(),
        second_started.clone(),
        release_first.clone(),
        release_second.clone(),
    );
    let app=axum::Router::new().fallback(move |_body: axum::Json<Value>| {
        let (counter,one,two,release_one,release_two)=(counter.clone(),one.clone(),two.clone(),release_one.clone(),release_two.clone());
        async move {
            let n=counter.fetch_add(1,Ordering::SeqCst);
            if n==0 { one.notify_one(); release_one.notified().await; }
            else if n==1 { two.notify_one(); release_two.notified().await; }
            else { panic!("unexpected extra generation HTTP"); }
            let atoms=json!({"atoms":[{"kind":"fact","text":"Retained evidence.","evidence_quote":"Retained evidence.","entities":[]}]});
            axum::Json(json!({"id":format!("generation-{n}"),"model":PRIMARY,"choices":[{"message":{"content":atoms.to_string()},"finish_reason":"stop"}],"usage":{"prompt_tokens":20,"completion_tokens":10,"cost":0.0004}}))
        }
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let llm = Arc::new(
        coralswarm_backend::llm::openai::OpenAiProvider::openrouter(
            reqwest::Client::new(),
            "fixture-only",
        )
        .with_base_url(url),
    );
    let start = |worker: WorkerTenant,
                 control: PgPool,
                 llm: Arc<coralswarm_backend::llm::openai::OpenAiProvider>| {
        tokio::spawn(async move {
            source_atoms::tick(&worker, &control, llm.as_ref(), None)
                .await
                .unwrap()
        })
    };
    let old = start(f.worker.clone(), f.control.app.clone(), llm.clone());
    tokio::time::timeout(Duration::from_secs(10), first_started.notified())
        .await
        .unwrap();
    sqlx::query("UPDATE corpus_enrichment_units SET standing_claim_until=now()-interval '1 second' WHERE holding_id=$1")
        .bind(version).execute(f.owner()).await.unwrap();
    sqlx::query("INSERT INTO jobs(kind,payload,account_id,idempotency_key) SELECT kind,payload,account_id,idempotency_key FROM jobs WHERE payload->>'source_version_id'=$1 AND status='running'")
        .bind(version.to_string()).execute(f.owner()).await.unwrap();
    let newer = start(f.worker.clone(), f.control.app.clone(), llm.clone());
    tokio::time::timeout(Duration::from_secs(10), second_started.notified())
        .await
        .unwrap();
    release_first.notify_one();
    old.await.unwrap();
    let active:(i64,String,bool)=sqlx::query_as("SELECT standing_generation,state,COALESCE(standing_claim_until>now(),false) FROM corpus_enrichment_units WHERE holding_id=$1")
        .bind(version).fetch_one(f.owner()).await.unwrap();
    assert_eq!(
        active,
        (2, "queued".into(), true),
        "late old response must neither publish nor hold current generation"
    );
    release_second.notify_one();
    newer.await.unwrap();
    let result:(String,i64,i64)=sqlx::query_as("SELECT v.extraction_status,(SELECT count(*) FROM semantic_atoms WHERE source_version_id=v.version_id),(SELECT count(*) FROM semantic_atom_windows WHERE source_version_id=v.version_id) FROM source_versions v WHERE version_id=$1")
        .bind(version).fetch_one(f.owner()).await.unwrap();
    assert_eq!(result, ("done".into(), 1, 1));
    let receipts: Vec<(String, i64)> = sqlx::query_as(
        "SELECT state,settled_micro_usd FROM managed_attempt_receipts ORDER BY created_at",
    )
    .fetch_all(f.owner())
    .await
    .unwrap();
    assert_eq!(receipts.len(), 2);
    assert_eq!(receipts[0].0, "ambiguous");
    assert!(receipts[0].1 >= 400);
    assert_eq!(receipts[1], ("billed".into(), 400));
    assert_eq!(hits.load(Ordering::SeqCst), 2);
    server.abort();
    f.done().await;
}
#[tokio::test]
async fn consumer_standing_late_old_response_cannot_hold_new_generation() {
    late_old_response_cannot_hold_new_generation(false).await;
}
#[tokio::test]
async fn tenant_standing_late_old_response_cannot_hold_new_generation() {
    late_old_response_cannot_hold_new_generation(true).await;
}

/// Sets an env var for one test and restores the previous value on drop,
/// including on panic. The suite runs with `--test-threads=1`.
struct EnvGuard(&'static str, Option<String>);
impl EnvGuard {
    fn set(key: &'static str, value: &str) -> Self {
        let previous = std::env::var(key).ok();
        std::env::set_var(key, value);
        Self(key, previous)
    }
}
impl Drop for EnvGuard {
    fn drop(&mut self) {
        match &self.1 {
            Some(value) => std::env::set_var(self.0, value),
            None => std::env::remove_var(self.0),
        }
    }
}

/// Text of the one source whose model response is invalid. The mock routes on
/// it, not on call order: `invalid-output` and `unknown-actor` both reach the
/// model, and which one arrives first is a race.
const INVALID_OUTPUT_TEXT: &str = "Retained evidence the model mangles.";

async fn standing_blank_invalid_synthetic_and_unknown_actor(org: bool) {
    // One job per tick. The tick's jobs share the fixture's one-connection
    // pools (2 s `acquire_timeout`), and under load a job queued behind its
    // siblings timed out and was held as `infrastructure_unavailable`. That
    // outcome is not under test here. Each job still runs end to end against
    // the same pools, so a nested acquisition would still fail.
    let _serial = EnvGuard::set(source_atoms::CONCURRENCY_ENV, "1");
    let f = Fixture::new(org).await;
    let blank = f.capture("note", "blank", "").await;
    let bad = f
        .capture("note", "invalid-output", INVALID_OUTPUT_TEXT)
        .await;
    let mut tx = match f.worker.tenant {
        TenantId::Consumer => scope::begin_account_scoped(&f.worker.pool, f.user.account_id)
            .await
            .unwrap(),
        TenantId::Org(_) => scope::begin_tenant_system_scoped(&f.worker.pool, f.user.account_id)
            .await
            .unwrap(),
    };
    let unknown_capture = CaptureSource {
        source_type: "outlook",
        provider_namespace: "fixture:standing",
        source_id: "unknown-actor",
        normalized_text: "Retained evidence.",
        representation: "original_text",
        metadata: &json!({}),
        occurred_at: None,
        participants: &[],
        ingested_by: None,
        scope: "private",
        duration_ms: None,
        segments: None,
    };
    let unknown = source_evidence::capture_source_version_tx(
        &mut tx,
        None,
        f.graph,
        f.user.account_id,
        &unknown_capture,
        None,
        &[42; 32],
    )
    .await
    .unwrap()
    .version_id;
    let origin=serde_json::from_value(json!({"service_id":"linear","transport":"synthetic","content_kind":"issue","capture_surface":"synthetic_demo"})).unwrap();
    let synthetic_capture = CaptureSource {
        source_type: "linear_issue",
        provider_namespace: "demo:linear:standing",
        source_id: "synthetic-excluded",
        normalized_text: "Retained evidence.",
        representation: "normalized_text",
        metadata: &json!({"synthetic_demo":true}),
        occurred_at: None,
        participants: &[],
        ingested_by: Some(f.user.user_id),
        scope: "private",
        duration_ms: None,
        segments: None,
    };
    let synthetic = source_evidence::capture_source_version_with_origin_tx(
        &mut tx,
        None,
        f.graph,
        f.user.account_id,
        &synthetic_capture,
        None,
        &[42; 32],
        Some(&origin),
    )
    .await
    .unwrap()
    .version_id;
    tx.commit().await.unwrap();
    f.sweep().await;
    f.sweep().await;
    let excluded: i64 =
        sqlx::query_scalar("SELECT count(*) FROM corpus_enrichment_units WHERE holding_id=$1")
            .bind(synthetic)
            .fetch_one(f.owner())
            .await
            .unwrap();
    assert_eq!(excluded, 0);
    let hits = Arc::new(AtomicUsize::new(0));
    let counter = hits.clone();
    let app=axum::Router::new().fallback(move |body:axum::Json<Value>| {
        let counter=counter.clone(); async move {
            let n=counter.fetch_add(1,Ordering::SeqCst);
            let invalid=body.0.to_string().contains(INVALID_OUTPUT_TEXT);
            let text=if invalid { "invalid JSON schema".to_owned() } else { json!({"atoms":[{"kind":"fact","text":"Retained evidence.","evidence_quote":"Retained evidence.","entities":[]}]}).to_string() };
            axum::Json(json!({"id":format!("validation-{n}"),"model":PRIMARY,"choices":[{"message":{"content":text},"finish_reason":"stop"}],"usage":{"prompt_tokens":20,"completion_tokens":10,"cost":0.0004}}))
        }
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let llm = coralswarm_backend::llm::openai::OpenAiProvider::openrouter(
        reqwest::Client::new(),
        "fixture-only",
    )
    .with_base_url(url);
    for _ in 0..4 {
        source_atoms::tick(&f.worker, &f.control.app, &llm, None)
            .await
            .unwrap();
    }
    let status = |id: Uuid| {
        let owner = f.owner().clone();
        async move {
            sqlx::query_scalar::<_, String>(
                "SELECT extraction_status FROM source_versions WHERE version_id=$1",
            )
            .bind(id)
            .fetch_one(&owner)
            .await
            .unwrap()
        }
    };
    assert_eq!(status(blank).await, "done");
    assert_eq!(status(bad).await, "failed");
    assert_eq!(status(unknown).await, "done");
    // The invalid answer is retried once (a schema error gets the window
    // retry since v3.4.3), and each call is billed once.
    assert_eq!(hits.load(Ordering::SeqCst), 3);
    let bad_accounted:(i64,i64)=sqlx::query_as("SELECT (SELECT count(*) FROM managed_attempt_receipts WHERE source_version_id=$1 AND state='billed' AND settled_micro_usd=400),(SELECT count(*) FROM semantic_atoms WHERE source_version_id=$1)")
        .bind(bad).fetch_one(f.owner()).await.unwrap();
    assert_eq!(bad_accounted, (2, 0));
    let blank_accounted:(i64,i64)=sqlx::query_as("SELECT (SELECT count(*) FROM managed_attempt_receipts WHERE source_version_id=$1),(SELECT count(*) FROM semantic_atom_windows WHERE source_version_id=$1)")
        .bind(blank).fetch_one(f.owner()).await.unwrap();
    assert_eq!(blank_accounted, (0, 0));
    let payer: Uuid = sqlx::query_scalar(
        "SELECT attributed_user_id FROM managed_attempt_receipts WHERE source_version_id=$1",
    )
    .bind(unknown)
    .fetch_one(f.owner())
    .await
    .unwrap();
    assert_eq!(payer, f.user.user_id);
    f.sweep().await;
    source_atoms::tick(&f.worker, &f.control.app, &llm, None)
        .await
        .unwrap();
    assert_eq!(
        hits.load(Ordering::SeqCst),
        3,
        "failed output and synthetic source must never be auto-requeued"
    );
    assert_eq!(status(bad).await, "failed");
    server.abort();
    f.done().await;
}
#[tokio::test]
async fn consumer_standing_blank_invalid_synthetic_unknown_actor() {
    standing_blank_invalid_synthetic_and_unknown_actor(false).await;
}
#[tokio::test]
async fn tenant_standing_blank_invalid_synthetic_unknown_actor() {
    standing_blank_invalid_synthetic_and_unknown_actor(true).await;
}

async fn standing_live_requester_is_not_execution_authority(org: bool) {
    let f = Fixture::new(org).await;
    sqlx::query("UPDATE graphs SET kind='org',owner_user_id=NULL WHERE id=$1")
        .bind(f.graph)
        .execute(f.owner())
        .await
        .unwrap();
    let version = f
        .capture("slack", "live-ineligible-requester", "Retained evidence.")
        .await;
    f.sweep().await;
    let run = f.run().await;
    sqlx::query("UPDATE account_members SET role='member' WHERE account_id=$1 AND user_id=$2")
        .bind(f.user.account_id)
        .bind(f.user.user_id)
        .execute(&f.control.owner)
        .await
        .unwrap();
    let (llm, hits, server) = provider().await;
    source_atoms::tick(&f.worker, &f.control.app, &llm, None)
        .await
        .unwrap();
    assert_eq!(
        hits.load(Ordering::SeqCst),
        0,
        "a live requester cannot authorize a shared graph after losing admin eligibility"
    );
    let held: (String, String) =
        sqlx::query_as("SELECT state,reason FROM corpus_enrichment_units WHERE holding_id=$1")
            .bind(version)
            .fetch_one(f.owner())
            .await
            .unwrap();
    assert_eq!(held, ("pending".into(), "principal_unavailable".into()));
    let replacement = f.member("admin").await;
    sqlx::query("UPDATE corpus_enrichment_units SET retry_after=now()-interval '1 second' WHERE holding_id=$1").bind(version).execute(f.owner()).await.unwrap();
    f.sweep().await;
    source_atoms::tick(&f.worker, &f.control.app, &llm, None)
        .await
        .unwrap();
    assert_eq!(hits.load(Ordering::SeqCst), 1);
    let binding: (Uuid, Uuid) = sqlx::query_as(
        "SELECT requested_by,execution_principal FROM corpus_enrichment_runs WHERE run_id=$1",
    )
    .bind(run)
    .fetch_one(f.owner())
    .await
    .unwrap();
    assert_eq!(binding, (f.user.user_id, replacement));
    let payer: Uuid = sqlx::query_scalar(
        "SELECT attributed_user_id FROM managed_attempt_receipts WHERE source_version_id=$1",
    )
    .bind(version)
    .fetch_one(f.owner())
    .await
    .unwrap();
    assert_eq!(
        payer, f.user.user_id,
        "capture member payer remains distinct from fresh admin authority"
    );
    server.abort();
    f.done().await;
}
#[tokio::test]
async fn consumer_standing_live_requester_is_not_authority() {
    standing_live_requester_is_not_execution_authority(false).await;
}
#[tokio::test]
async fn tenant_standing_live_requester_is_not_authority() {
    standing_live_requester_is_not_execution_authority(true).await;
}

async fn standing_and_cohort_contest_one_owner(org: bool) {
    use axum::{extract::State, Extension, Json};
    let f = Fixture::new(org).await;
    let version = f
        .capture("note", "owner-contest", "Retained evidence.")
        .await;
    let state = Arc::new(coralswarm_backend::test_app_state_owned_with_db_and_store(
        f.control.app.clone(),
        None,
    ));
    let (code,Json(body))=coralswarm_backend::corpus_enrichment::create_run(State(state.clone()),f.tenant.clone(),Extension(f.user.clone()),Json(serde_json::from_value(json!({"graph_id":f.graph,"execute":true,"budget":{"items":100,"characters":50000,"model_calls":100}})).unwrap())).await;
    assert_eq!(code, axum::http::StatusCode::CREATED, "{body}");
    let cohort: Uuid = body["run_id"].as_str().unwrap().parse().unwrap();
    let second = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(f.worker.pool.connect_options().as_ref().clone())
        .await
        .unwrap();
    let competing = WorkerTenant {
        tenant: f.worker.tenant.clone(),
        pool: second.clone(),
        store: Arc::new(PostgresStore::new(second.clone())),
    };
    let (standing, (status, Json(reply))) = tokio::join!(
        coralswarm_backend::test_standing_sweep(&competing, &f.control.app),
        coralswarm_backend::corpus_enrichment::advance_run(
            State(state),
            f.tenant.clone(),
            Extension(f.user.clone()),
            axum::extract::Path(cohort),
            Json(serde_json::from_value(json!({"graph_id":f.graph,"page_size":100})).unwrap())
        )
    );
    standing.unwrap();
    assert_eq!(status, axum::http::StatusCode::OK, "{reply}");
    let owners: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM corpus_enrichment_units WHERE holding_id=$1 AND execution_owner",
    )
    .bind(version)
    .fetch_one(f.owner())
    .await
    .unwrap();
    assert_eq!(owners, 1);
    let (llm, hits, server) = provider().await;
    source_atoms::tick(&f.worker, &f.control.app, &llm, None)
        .await
        .unwrap();
    assert_eq!(hits.load(Ordering::SeqCst), 1);
    let done: i64 =
        sqlx::query_scalar("SELECT count(*) FROM semantic_atom_windows WHERE source_version_id=$1")
            .bind(version)
            .fetch_one(f.owner())
            .await
            .unwrap();
    assert_eq!(done, 1);
    second.close().await;
    server.abort();
    f.done().await;
}
#[tokio::test]
async fn consumer_standing_cohort_owner_contest() {
    standing_and_cohort_contest_one_owner(false).await;
}
#[tokio::test]
async fn tenant_standing_cohort_owner_contest() {
    standing_and_cohort_contest_one_owner(true).await;
}

async fn standing_source_revoked_at_backoff_and_publication(org: bool) {
    for at_publication in [false, true] {
        let f = Fixture::new(org).await;
        let version = f
            .capture(
                "teams_channel",
                "revoked-during-request",
                "Retained evidence.",
            )
            .await;
        f.sweep().await;
        let started = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let hits = Arc::new(AtomicUsize::new(0));
        let (seen, ready, go) = (hits.clone(), started.clone(), release.clone());
        let app=axum::Router::new().fallback(move |_body:axum::Json<Value>| {
            let (seen,ready,go)=(seen.clone(),ready.clone(),go.clone()); async move {
                use axum::response::IntoResponse;
                let n=seen.fetch_add(1,Ordering::SeqCst); assert_eq!(n,0,"fresh attempt fence must prevent a second HTTP");
                ready.notify_one(); go.notified().await;
                if !at_publication { return (axum::http::StatusCode::TOO_MANY_REQUESTS,axum::Json(json!({"error":{"message":"fixture rate limit"}}))).into_response(); }
                let atoms=json!({"atoms":[{"kind":"fact","text":"Retained evidence.","evidence_quote":"Retained evidence.","entities":[]}]});
                axum::Json(json!({"id":"revoked-response","model":PRIMARY,"choices":[{"message":{"content":atoms.to_string()},"finish_reason":"stop"}],"usage":{"prompt_tokens":20,"completion_tokens":10,"cost":0.0004}})).into_response()
            }
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let llm = Arc::new(
            coralswarm_backend::llm::openai::OpenAiProvider::openrouter(
                reqwest::Client::new(),
                "fixture-only",
            )
            .with_base_url(url),
        );
        let (worker, control) = (f.worker.clone(), f.control.app.clone());
        let task = tokio::spawn(async move {
            source_atoms::tick(&worker, &control, llm.as_ref(), None)
                .await
                .unwrap();
        });
        tokio::time::timeout(Duration::from_secs(10), started.notified())
            .await
            .unwrap();
        sqlx::query("UPDATE source_items SET access_state='restricted' WHERE source_item_id=(SELECT source_item_id FROM source_versions WHERE version_id=$1)").bind(version).execute(f.owner()).await.unwrap();
        release.notify_one();
        tokio::time::timeout(Duration::from_secs(15), task)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(hits.load(Ordering::SeqCst), 1);
        let atoms: i64 =
            sqlx::query_scalar("SELECT count(*) FROM semantic_atoms WHERE source_version_id=$1")
                .bind(version)
                .fetch_one(f.owner())
                .await
                .unwrap();
        assert_eq!(atoms, 0);
        let costs:Vec<(String,i64)>=sqlx::query_as("SELECT state,settled_micro_usd FROM managed_attempt_receipts WHERE source_version_id=$1").bind(version).fetch_all(f.owner()).await.unwrap();
        assert_eq!(
            costs,
            vec![(
                if at_publication {
                    "billed"
                } else {
                    "not_generated"
                }
                .into(),
                if at_publication { 400 } else { 0 }
            )]
        );
        server.abort();
        f.done().await;
    }
}
#[tokio::test]
async fn consumer_standing_source_revoked_on_backoff_and_publication() {
    standing_source_revoked_at_backoff_and_publication(false).await;
}
#[tokio::test]
async fn tenant_standing_source_revoked_on_backoff_and_publication() {
    standing_source_revoked_at_backoff_and_publication(true).await;
}

async fn standing_wrong_job_and_lost_unclaimed_delivery(org: bool) {
    let f = Fixture::new(org).await;
    let version = f
        .capture("note", "unclaimed-delivery", "Retained evidence.")
        .await;
    f.sweep().await;
    let payload: Value = sqlx::query_scalar(
        "SELECT payload FROM jobs WHERE payload->>'source_version_id'=$1 AND status='pending'",
    )
    .bind(version.to_string())
    .fetch_one(f.owner())
    .await
    .unwrap();
    let (llm, hits, server) = provider().await;
    source_atoms::process_job(
        &f.worker.tenant,
        &f.worker.pool,
        &f.control.app,
        f.worker.store.as_ref(),
        &llm,
        None,
        coralswarm_backend::jobs::queue::JobRow {
            id: Uuid::new_v4(),
            kind: "ExtractStandingSourceAtoms".into(),
            payload,
            attempts: 1,
            account_id: Some(f.user.account_id),
        },
    )
    .await;
    let untouched: (i64, String) = sqlx::query_as(
        "SELECT standing_generation,state FROM corpus_enrichment_units WHERE holding_id=$1",
    )
    .bind(version)
    .fetch_one(f.owner())
    .await
    .unwrap();
    assert_eq!(untouched, (0, "queued".into()));
    assert_eq!(
        hits.load(Ordering::SeqCst),
        0,
        "descriptive work IDs without the actual running job must not claim or send"
    );
    let id: Uuid = sqlx::query_scalar(
        "SELECT id FROM jobs WHERE payload->>'source_version_id'=$1 AND status='pending'",
    )
    .bind(version.to_string())
    .fetch_one(f.owner())
    .await
    .unwrap();
    coralswarm_backend::jobs::queue::could_not_do(
        &f.worker.pool,
        id,
        coralswarm_backend::jobs::queue::FailureClass::Structural,
        "fixture lost before source claim",
    )
    .await
    .unwrap();
    f.sweep().await;
    source_atoms::tick(&f.worker, &f.control.app, &llm, None)
        .await
        .unwrap();
    assert_eq!(hits.load(Ordering::SeqCst), 1);
    let state: String =
        sqlx::query_scalar("SELECT extraction_status FROM source_versions WHERE version_id=$1")
            .bind(version)
            .fetch_one(f.owner())
            .await
            .unwrap();
    assert_eq!(
        state, "done",
        "a lost queue delivery before first claim must recover without a human"
    );
    f.sweep().await;
    source_atoms::tick(&f.worker, &f.control.app, &llm, None)
        .await
        .unwrap();
    assert_eq!(hits.load(Ordering::SeqCst), 1);
    server.abort();
    f.done().await;
}
#[tokio::test]
async fn consumer_standing_actual_job_binding_and_unclaimed_delivery_recovery() {
    standing_wrong_job_and_lost_unclaimed_delivery(false).await;
}
#[tokio::test]
async fn tenant_standing_actual_job_binding_and_unclaimed_delivery_recovery() {
    standing_wrong_job_and_lost_unclaimed_delivery(true).await;
}

async fn standing_control_fences_after_network(org: bool) {
    let mut cases = vec!["member", "processing", "lifecycle"];
    if org {
        cases.push("org_revoked");
    }
    for fence in cases {
        let f = Fixture::new(org).await;
        let version = f.capture("slack", fence, "Retained evidence.").await;
        f.sweep().await;
        let at_publication = matches!(fence, "org_revoked");
        let started = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let hits = Arc::new(AtomicUsize::new(0));
        let (seen, ready, go) = (hits.clone(), started.clone(), release.clone());
        let app=axum::Router::new().fallback(move |_body:axum::Json<Value>| {
            let (seen,ready,go)=(seen.clone(),ready.clone(),go.clone()); async move {
                use axum::response::IntoResponse;
                assert_eq!(seen.fetch_add(1,Ordering::SeqCst),0,"fresh CONTROL fence must prevent a second HTTP");
                ready.notify_one(); go.notified().await;
                if !at_publication { return (axum::http::StatusCode::TOO_MANY_REQUESTS,axum::Json(json!({"error":{"message":"fixture rate limit"}}))).into_response(); }
                let atoms=json!({"atoms":[{"kind":"fact","text":"Retained evidence.","evidence_quote":"Retained evidence.","entities":[]}]});
                axum::Json(json!({"id":"control-fenced-response","model":PRIMARY,"choices":[{"message":{"content":atoms.to_string()},"finish_reason":"stop"}],"usage":{"prompt_tokens":20,"completion_tokens":10,"cost":0.0004}})).into_response()
            }
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let llm = Arc::new(
            coralswarm_backend::llm::openai::OpenAiProvider::openrouter(
                reqwest::Client::new(),
                "fixture-only",
            )
            .with_base_url(url),
        );
        let (worker, control) = (f.worker.clone(), f.control.app.clone());
        let task = tokio::spawn(async move {
            source_atoms::tick(&worker, &control, llm.as_ref(), None)
                .await
                .unwrap();
        });
        tokio::time::timeout(Duration::from_secs(10), started.notified())
            .await
            .unwrap();
        match fence {
            "member" => {
                sqlx::query("DELETE FROM account_members WHERE account_id=$1 AND user_id=$2")
                    .bind(f.user.account_id)
                    .bind(f.user.user_id)
                    .execute(&f.control.owner)
                    .await
                    .unwrap();
            }
            "processing" => {
                let prefs = json!({"managed_processing":"off"});
                sqlx::query("INSERT INTO account_prefs(account_id,prefs) VALUES($1,$2) ON CONFLICT(account_id) DO UPDATE SET prefs=EXCLUDED.prefs").bind(f.user.account_id).bind(prefs).execute(&f.control.owner).await.unwrap();
            }
            "lifecycle" => {
                sqlx::query("CREATE INDEX nodes_graph_type_lower_title_unique ON nodes(node_id)")
                    .execute(f.owner())
                    .await
                    .unwrap();
            }
            "org_revoked" => {
                let TenantId::Org(org_id) = &f.worker.tenant else {
                    unreachable!()
                };
                sqlx::query("INSERT INTO org_revocations(clerk_org_id) VALUES($1)")
                    .bind(org_id)
                    .execute(&f.control.owner)
                    .await
                    .unwrap();
            }
            _ => unreachable!(),
        }
        release.notify_one();
        tokio::time::timeout(Duration::from_secs(15), task)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(hits.load(Ordering::SeqCst), 1, "{fence}");
        let atoms: i64 =
            sqlx::query_scalar("SELECT count(*) FROM semantic_atoms WHERE source_version_id=$1")
                .bind(version)
                .fetch_one(f.owner())
                .await
                .unwrap();
        assert_eq!(atoms, 0, "{fence}");
        let costs:Vec<(String,i64)>=sqlx::query_as("SELECT state,settled_micro_usd FROM managed_attempt_receipts WHERE source_version_id=$1").bind(version).fetch_all(f.owner()).await.unwrap();
        assert_eq!(
            costs,
            vec![(
                if at_publication {
                    "billed"
                } else {
                    "not_generated"
                }
                .into(),
                if at_publication { 400 } else { 0 }
            )],
            "{fence}"
        );
        let expected = match fence {
            "member" | "org_revoked" => "principal_unavailable",
            "processing" => "managed_processing_disabled",
            "lifecycle" => "lifecycle_contract_not_ready",
            _ => unreachable!(),
        };
        let reason: String =
            sqlx::query_scalar("SELECT reason FROM corpus_enrichment_units WHERE holding_id=$1")
                .bind(version)
                .fetch_one(f.owner())
                .await
                .unwrap();
        assert_eq!(reason, expected);
        server.abort();
        f.done().await;
    }
}
#[tokio::test]
async fn consumer_standing_control_member_preference_lifecycle_fences() {
    standing_control_fences_after_network(false).await;
}
#[tokio::test]
async fn tenant_standing_control_member_preference_lifecycle_fences() {
    standing_control_fences_after_network(true).await;
}

async fn standing_legacy_meeting_profile_continuation(org: bool) {
    let f = Fixture::new(org).await;
    let text = format!(
        "Retained evidence.\n{}Retained evidence.",
        "padding\n".repeat(1400)
    );
    let item:Uuid=sqlx::query_scalar("INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id) VALUES($1,'transcript','legacy:meeting','retained-legacy-meeting') RETURNING source_item_id").bind(f.graph).fetch_one(f.owner()).await.unwrap();
    let version:Uuid=sqlx::query_scalar("INSERT INTO source_versions(graph_id,source_item_id,source_id,source_type,provider_namespace,raw_inline,is_legacy_baseline,extraction_version,extraction_status,extraction_error) VALUES($1,$2,'retained-legacy-meeting','transcript','legacy:meeting',$3,true,'meeting-atoms-v1','blocked','managed_admission_required') RETURNING version_id")
        .bind(f.graph).bind(item).bind(text).fetch_one(f.owner()).await.unwrap();
    sqlx::query("UPDATE source_items SET current_version_id=$2 WHERE source_item_id=$1")
        .bind(item)
        .bind(version)
        .execute(f.owner())
        .await
        .unwrap();
    f.sweep().await;
    f.sweep().await;
    let kind: String = sqlx::query_scalar(
        "SELECT kind FROM jobs WHERE payload->>'source_version_id'=$1 AND status='pending'",
    )
    .bind(version.to_string())
    .fetch_one(f.owner())
    .await
    .unwrap();
    assert_eq!(kind, "ExtractStandingMeetingAtoms");
    let (llm, hits, server) = provider().await;
    for _ in 0..3 {
        source_atoms::tick(&f.worker, &f.control.app, &llm, None)
            .await
            .unwrap();
    }
    assert_eq!(hits.load(Ordering::SeqCst), 2);
    let outcome:(String,i64,i64)=sqlx::query_as("SELECT extraction_status,(SELECT count(*) FROM semantic_atom_windows WHERE source_version_id=v.version_id AND extraction_version='meeting-atoms-v1'),(SELECT count(*) FROM semantic_atoms WHERE source_version_id=v.version_id AND extraction_version='meeting-atoms-v1') FROM source_versions v WHERE version_id=$1")
        .bind(version).fetch_one(f.owner()).await.unwrap();
    assert_eq!(outcome, ("done".into(), 2, 2));
    let incompatible:i64=sqlx::query_scalar("SELECT count(*) FROM jobs WHERE payload->>'source_version_id'=$1 AND kind<>'ExtractStandingMeetingAtoms'").bind(version.to_string()).fetch_one(f.owner()).await.unwrap();
    assert_eq!(incompatible, 0);
    server.abort();
    f.done().await;
}
#[tokio::test]
async fn consumer_standing_meeting_profile_uses_compatible_continuation() {
    standing_legacy_meeting_profile_continuation(false).await;
}
#[tokio::test]
async fn tenant_standing_meeting_profile_uses_compatible_continuation() {
    standing_legacy_meeting_profile_continuation(true).await;
}

fn spawn_tick(f: &Fixture, llm: Arc<coralswarm_backend::llm::openai::OpenAiProvider>) -> tokio::task::JoinHandle<()> {
    let (worker,control)=(f.worker.clone(),f.control.app.clone());
    tokio::spawn(async move { source_atoms::tick(&worker,&control,llm.as_ref(),None).await.unwrap(); })
}
async fn pending_job(f: &Fixture, version: Uuid) -> Uuid {
    sqlx::query_scalar("SELECT id FROM jobs WHERE payload->>'source_version_id'=$1 AND status='pending' ORDER BY created_at LIMIT 1")
        .bind(version.to_string()).fetch_one(f.owner()).await.unwrap()
}
/// Liveness bound for a spawned tick to reach a probe or finish. It catches a
/// hang, not a slow machine: each tick is a sequence of fixture-pool
/// transactions and one local HTTP call, and under load that has taken more
/// than the old 10 s / 15 s. Nothing asserts how fast it runs.
const SPAWNED_TICK_LIVENESS: Duration = Duration::from_secs(90);
async fn wait_probe(probe: &coralswarm_backend::test_standing_probe::Barrier) {
    tokio::time::timeout(SPAWNED_TICK_LIVENESS,probe.reached()).await.expect("spawned tick never reached the probe");
}
async fn finish_tick(task: tokio::task::JoinHandle<()>) {
    tokio::time::timeout(SPAWNED_TICK_LIVENESS,task).await.expect("spawned tick never finished").unwrap();
}
async fn completed_snapshot(f: &Fixture, version: Uuid) -> Value {
    sqlx::query_scalar("SELECT jsonb_build_object('source',to_jsonb(v),'unit',(SELECT to_jsonb(u) FROM corpus_enrichment_units u WHERE u.holding_id=v.version_id AND u.execution_owner),'admission',(SELECT to_jsonb(a) FROM corpus_capture_admissions a WHERE a.source_version_id=v.version_id),'atoms',(SELECT count(*) FROM semantic_atoms WHERE source_version_id=v.version_id),'passages',(SELECT count(*) FROM semantic_atom_passages WHERE source_version_id=v.version_id),'windows',(SELECT count(*) FROM semantic_atom_windows WHERE source_version_id=v.version_id)) FROM source_versions v WHERE version_id=$1")
        .bind(version).fetch_one(f.owner()).await.unwrap()
}
async fn prefs(f: &Fixture, value: Value) {
    sqlx::query("INSERT INTO account_prefs(account_id,prefs) VALUES($1,$2) ON CONFLICT(account_id) DO UPDATE SET prefs=EXCLUDED.prefs")
        .bind(f.user.account_id).bind(value).execute(&f.control.owner).await.unwrap();
}

async fn standing_stale_before_read_and_status(org: bool) {
    use coralswarm_backend::test_standing_probe::{pause_at,Stage};
    for stage in [Stage::MetadataMissing,Stage::BeforeRunningStatus,Stage::FundedFenceDenied] {
        let f=Fixture::new(org).await;
        let version=f.capture("note","stale-pre-read","Retained evidence.").await;
        if stage==Stage::FundedFenceDenied {
            sqlx::query("UPDATE source_versions SET ingested_by=NULL WHERE version_id=$1").bind(version).execute(f.owner()).await.unwrap();
        }
        f.sweep().await;
        let job=pending_job(&f,version).await;
        let claimed=pause_at(job,Stage::Claimed);
        let stale=pause_at(job,stage);
        let payer_gate=(stage==Stage::FundedFenceDenied).then(|| pause_at(job,Stage::BeforeChargePrincipal));
        let mut payer_membership: Option<Value>=None;
        let (llm,http,server)=provider().await;
        let llm=Arc::new(llm);
        let old=spawn_tick(&f,llm.clone());
        wait_probe(&claimed).await;
        match stage {
            Stage::MetadataMissing => {
                sqlx::query("UPDATE source_items SET access_state='restricted' WHERE source_item_id=(SELECT source_item_id FROM source_versions WHERE version_id=$1)")
                    .bind(version).execute(f.owner()).await.unwrap();
            }
            Stage::BeforeRunningStatus | Stage::FundedFenceDenied => {},
            _ => unreachable!(),
        }
        claimed.release();
        if let Some(payer_gate)=payer_gate {
            wait_probe(&payer_gate).await;
            payer_membership=Some(sqlx::query_scalar("SELECT to_jsonb(m) FROM account_members m WHERE account_id=$1 AND user_id=$2")
                .bind(f.user.account_id).bind(f.user.user_id).fetch_one(&f.control.owner).await.unwrap());
            assert_eq!(sqlx::query("DELETE FROM account_members WHERE account_id=$1 AND user_id=$2")
                .bind(f.user.account_id).bind(f.user.user_id).execute(&f.control.owner).await.unwrap().rows_affected(),1);
            payer_gate.release();
        }
        wait_probe(&stale).await;
        if let Some(membership)=payer_membership {
            // The real funded gate denied the missing principal before permission is restored.
            assert_eq!(http.load(Ordering::SeqCst),0);
            let receipts:i64=sqlx::query_scalar("SELECT count(*) FROM managed_attempt_receipts WHERE source_version_id=$1")
                .bind(version).fetch_one(f.owner()).await.unwrap();
            assert_eq!(receipts,0);
            let owner:(bool,i64)=sqlx::query_as("SELECT execution_owner,standing_generation FROM corpus_enrichment_units WHERE holding_id=$1")
                .bind(version).fetch_one(f.owner()).await.unwrap();
            assert_eq!(owner,(true,1));
            let absent:bool=sqlx::query_scalar("SELECT NOT EXISTS(SELECT 1 FROM account_members WHERE account_id=$1 AND user_id=$2)")
                .bind(f.user.account_id).bind(f.user.user_id).fetch_one(&f.control.owner).await.unwrap();
            assert!(absent);
            sqlx::query("INSERT INTO account_members SELECT * FROM jsonb_populate_record(NULL::account_members,$1)")
                .bind(&membership).execute(&f.control.owner).await.unwrap();
            let restored:Value=sqlx::query_scalar("SELECT to_jsonb(m) FROM account_members m WHERE account_id=$1 AND user_id=$2")
                .bind(f.user.account_id).bind(f.user.user_id).fetch_one(&f.control.owner).await.unwrap();
            assert_eq!(restored,membership);
        }
        sqlx::query("UPDATE source_items SET access_state='permitted' WHERE source_item_id=(SELECT source_item_id FROM source_versions WHERE version_id=$1)")
            .bind(version).execute(f.owner()).await.unwrap();
        prefs(&f,json!({})).await;
        sqlx::query("UPDATE corpus_enrichment_units SET standing_claim_until=now()-interval '1 second' WHERE holding_id=$1")
            .bind(version).execute(f.owner()).await.unwrap();
        if stage==Stage::MetadataMissing {
            let retracted:(String,Value,Option<String>)=sqlx::query_as("SELECT status::text,payload,idempotency_key FROM jobs WHERE id=$1")
                .bind(job).fetch_one(f.owner()).await.unwrap();
            assert_eq!(retracted,("dead".into(),json!({"graph_id":f.graph,"evidence_retracted":true}),None));
            f.sweep().await;
            assert_ne!(pending_job(&f,version).await,job);
        } else {
            sqlx::query("INSERT INTO jobs(kind,payload,account_id,idempotency_key) SELECT kind,payload,account_id,idempotency_key FROM jobs WHERE id=$1")
                .bind(job).execute(f.owner()).await.unwrap();
        }
        finish_tick(spawn_tick(&f,llm.clone())).await;
        let before=completed_snapshot(&f,version).await;
        assert_eq!(before["source"]["extraction_status"],json!("done"));
        assert_eq!(before["unit"]["standing_generation"],json!(2));
        assert_eq!(before["atoms"],json!(1));
        stale.release();
        finish_tick(old).await;
        assert_eq!(completed_snapshot(&f,version).await,before,"old {stage:?} decision changed a newer completed owner");
        assert_eq!(http.load(Ordering::SeqCst),1);
        server.abort(); f.done().await;
    }
}
#[tokio::test]
async fn consumer_standing_stale_metadata_running_payer_cannot_mutate_newer_work() { standing_stale_before_read_and_status(false).await; }
#[tokio::test]
async fn tenant_standing_stale_metadata_running_payer_cannot_mutate_newer_work() { standing_stale_before_read_and_status(true).await; }

async fn standing_unknown_reason_is_actionable(org: bool) {
    let f=Fixture::new(org).await;
    let version=f.capture("note","unknown-hold","Retained evidence.").await;
    f.sweep().await;
    let unit:Uuid=sqlx::query_scalar("UPDATE corpus_enrichment_units SET state='pending',reason='unknown_fixture_reason',retry_after=now() WHERE holding_id=$1 RETURNING unit_id")
        .bind(version).fetch_one(f.owner()).await.unwrap();
    let before=completed_snapshot(&f,version).await;
    let error=coralswarm_backend::test_standing_sweep(&f.worker,&f.control.app).await.unwrap_err();
    assert!(error.contains("UnknownHoldReason") && error.contains(&unit.to_string()) && error.contains("unknown_fixture_reason"),"{error}");
    assert_eq!(completed_snapshot(&f,version).await,before);
    let (llm,http,server)=provider().await;
    source_atoms::tick(&f.worker,&f.control.app,&llm,None).await.unwrap();
    // A pending unit is not a paid queued claim until recovery accepts its stored reason.
    assert_eq!(http.load(Ordering::SeqCst),0);
    server.abort();f.done().await;
}
#[tokio::test]
async fn consumer_standing_unknown_hold_reason_is_actionable() { standing_unknown_reason_is_actionable(false).await; }
#[tokio::test]
async fn tenant_standing_unknown_hold_reason_is_actionable() { standing_unknown_reason_is_actionable(true).await; }

async fn standing_rollout_defaults_off(org: bool) {
    let f=Fixture::new(org).await;
    let version=f.capture("note","rollout-default-off","Retained evidence.").await;
    std::env::remove_var("STANDING_PROCESSING_ENABLED");
    f.sweep().await;
    let count:i64=sqlx::query_scalar("SELECT count(*) FROM corpus_enrichment_units WHERE holding_id=$1").bind(version).fetch_one(f.owner()).await.unwrap();
    assert_eq!(count,0);
    std::env::set_var("STANDING_PROCESSING_ENABLED","true");
    f.sweep().await;
    let job=pending_job(&f,version).await;
    std::env::remove_var("STANDING_PROCESSING_ENABLED");
    let (llm,http,server)=provider().await;
    source_atoms::tick(&f.worker,&f.control.app,&llm,None).await.unwrap();
    assert_eq!(http.load(Ordering::SeqCst),0);
    let generation:i64=sqlx::query_scalar("SELECT standing_generation FROM corpus_enrichment_units WHERE holding_id=$1").bind(version).fetch_one(f.owner()).await.unwrap();
    assert_eq!(generation,0);
    std::env::set_var("STANDING_PROCESSING_ENABLED","true");
    f.sweep().await;
    let new_job=pending_job(&f,version).await;
    assert_eq!(job,new_job,"the paused tick left the job queued; resuming needs no new job");
    source_atoms::tick(&f.worker,&f.control.app,&llm,None).await.unwrap();
    assert_eq!(http.load(Ordering::SeqCst),1);
    assert_eq!(completed_snapshot(&f,version).await["source"]["extraction_status"],json!("done"));
    server.abort();f.done().await;
}
#[tokio::test]
async fn consumer_standing_rollout_off_then_explicit_enable_uses_real_worker() { standing_rollout_defaults_off(false).await; }
#[tokio::test]
async fn tenant_standing_rollout_off_then_explicit_enable_uses_real_worker() { standing_rollout_defaults_off(true).await; }

async fn standing_local_commit_acknowledgement_loss(org: bool) {
    use coralswarm_backend::test_standing_probe::{Ack,drop_ack_once,ack_was_dropped};
    for ack in [Ack::Publication,Ack::QueueCompletion] {
        let f=Fixture::new(org).await;
        let version=f.capture("note","local-ack-loss","Retained evidence.").await;
        f.sweep().await;
        let job=pending_job(&f,version).await;
        drop_ack_once(job,ack);
        let (llm,http,server)=provider().await;
        source_atoms::tick(&f.worker,&f.control.app,&llm,None).await.unwrap();
        assert!(ack_was_dropped(job,ack));
        let committed=completed_snapshot(&f,version).await;
        assert_eq!(committed["source"]["extraction_status"],json!("done"));
        assert_eq!(committed["atoms"],json!(1));
        assert_eq!(committed["passages"],json!(1));
        assert_eq!(committed["windows"],json!(1));
        // Ordinary local redelivery after the committed response is lost, with no finance privilege fault.
        sqlx::query("INSERT INTO jobs(kind,payload,account_id,idempotency_key) SELECT kind,payload,account_id,idempotency_key FROM jobs WHERE id=$1")
            .bind(job).execute(f.owner()).await.unwrap();
        f.sweep().await;
        source_atoms::tick(&f.worker,&f.control.app,&llm,None).await.unwrap();
        assert_eq!(completed_snapshot(&f,version).await,committed);
        assert_eq!(http.load(Ordering::SeqCst),1);
        let receipts:(i64,i64)=sqlx::query_as("SELECT count(*),sum(settled_micro_usd)::bigint FROM managed_attempt_receipts").fetch_one(f.owner()).await.unwrap();
        assert_eq!(receipts,(1,400));
        server.abort();f.done().await;
    }
}
#[tokio::test]
async fn consumer_standing_publication_and_queue_commit_ack_loss_is_idempotent() { standing_local_commit_acknowledgement_loss(false).await; }
#[tokio::test]
async fn tenant_standing_publication_and_queue_commit_ack_loss_is_idempotent() { standing_local_commit_acknowledgement_loss(true).await; }

async fn standing_denied_original_projection_is_not_read(org: bool) {
    use coralswarm_backend::test_standing_probe::{pause_at,Stage};
    let f=Fixture::new(org).await;
    let version=f.capture("note","private-no-read","Retained evidence.").await;
    f.sweep().await;
    let job=pending_job(&f,version).await;
    let before_private=pause_at(job,Stage::BeforePrivateFence);
    let (llm,http,server)=provider().await;
    let llm=Arc::new(llm);
    let task=spawn_tick(&f,llm.clone());
    wait_probe(&before_private).await;
    let base_role=if org {"coralswarm_tenant_app"} else {"coralswarm_app"};
    let columns:Vec<String>=sqlx::query_scalar("SELECT quote_ident(column_name) FROM information_schema.columns WHERE table_schema='public' AND table_name='source_versions' AND column_name NOT IN ('raw_inline','raw_blob_key') ORDER BY ordinal_position")
        .fetch_all(f.owner()).await.unwrap();
    sqlx::query(&format!("REVOKE SELECT ON source_versions FROM {base_role}"))
        .execute(f.owner()).await.unwrap();
    sqlx::query(&format!("GRANT SELECT({}) ON source_versions TO {base_role}",columns.join(",")))
        .execute(f.owner()).await.unwrap();
    let mut read_scope=match f.worker.tenant {
        TenantId::Consumer=>scope::begin_account_scoped(&f.worker.pool,f.user.account_id).await.unwrap(),
        TenantId::Org(_)=>scope::begin_tenant_system_scoped(&f.worker.pool,f.user.account_id).await.unwrap(),
    };
    let denied=sqlx::query("SELECT raw_inline,raw_blob_key FROM source_versions WHERE version_id=$1")
        .bind(version).fetch_optional(&mut *read_scope).await.unwrap_err();
    assert_eq!(denied.as_database_error().and_then(|error|error.code()).as_deref(),Some("42501"));
    read_scope.rollback().await.unwrap();
    prefs(&f,json!({"managed_processing":"off"})).await;
    before_private.release();
    finish_tick(task).await;
    let queue:(String,Option<String>)=sqlx::query_as("SELECT status::text,error_message FROM jobs WHERE id=$1").bind(job).fetch_one(f.owner()).await.unwrap();
    assert_eq!(queue,("done".into(),None),"denied path must not attempt a forbidden private projection");
    assert_eq!(http.load(Ordering::SeqCst),0);
    assert_eq!(completed_snapshot(&f,version).await["unit"]["reason"],json!("managed_processing_disabled"));
    sqlx::query(&format!("GRANT SELECT ON source_versions TO {base_role}")).execute(f.owner()).await.unwrap();
    prefs(&f,json!({})).await;
    sqlx::query("UPDATE corpus_enrichment_units SET retry_after=now() WHERE holding_id=$1").bind(version).execute(f.owner()).await.unwrap();
    f.sweep().await;
    finish_tick(spawn_tick(&f,llm)).await;
    assert_eq!(completed_snapshot(&f,version).await["source"]["extraction_status"],json!("done"));
    assert_eq!(http.load(Ordering::SeqCst),1);
    server.abort();f.done().await;
}
#[tokio::test]
async fn consumer_standing_denied_private_original_and_locator_are_never_read() { standing_denied_original_projection_is_not_read(false).await; }
#[tokio::test]
async fn tenant_standing_denied_private_original_and_locator_are_never_read() { standing_denied_original_projection_is_not_read(true).await; }

async fn standing_graph_mutation_before_publication(org: bool) {
    use coralswarm_backend::test_standing_probe::{pause_at,Stage};
    for mutation in ["account","owner","archive","shape","input"] {
        let f=Fixture::new(org).await;
        let version=f.capture("note",mutation,"Retained evidence.").await;
        f.sweep().await;
        let job=pending_job(&f,version).await;
        let publication=pause_at(job,Stage::BeforePublication);
        let (llm,http,server)=provider().await;
        let task=spawn_tick(&f,Arc::new(llm));
        wait_probe(&publication).await;
        match mutation {
            "account" => {
                let other=Uuid::new_v4();
                let other_owner=Uuid::new_v4();
                sqlx::query("INSERT INTO users(id,email,clerk_user_id) VALUES($1,$2,$2)")
                    .bind(other_owner).bind(format!("{other_owner}@fixture.local")).execute(&f.control.owner).await.unwrap();
                sqlx::query("INSERT INTO accounts(id,owner_user_id) VALUES($1,$2)").bind(other).bind(other_owner).execute(&f.control.owner).await.unwrap();
                let shape:Value=sqlx::query_scalar("SELECT jsonb_build_array(kind,owner_user_id,archived_at) FROM graphs WHERE id=$1")
                    .bind(f.graph).fetch_one(f.owner()).await.unwrap();
                sqlx::query("UPDATE graphs SET account_id=$2 WHERE id=$1").bind(f.graph).bind(other).execute(f.owner()).await.unwrap();
                let changed:(Uuid,Value)=sqlx::query_as("SELECT account_id,jsonb_build_array(kind,owner_user_id,archived_at) FROM graphs WHERE id=$1")
                    .bind(f.graph).fetch_one(f.owner()).await.unwrap();
                assert_eq!(changed,(other,shape),"account mutation must preserve graph kind, owner and archive state");
            },
            "owner" => {
                let stranger=Uuid::new_v4();
                sqlx::query("INSERT INTO users(id,email,clerk_user_id) VALUES($1,$2,$2)").bind(stranger).bind(format!("{stranger}@fixture.local")).execute(&f.control.owner).await.unwrap();
                sqlx::query("UPDATE graphs SET owner_user_id=$2 WHERE id=$1").bind(f.graph).bind(stranger).execute(f.owner()).await.unwrap();
            },
            "archive" => { sqlx::query("UPDATE graphs SET archived_at=now() WHERE id=$1").bind(f.graph).execute(f.owner()).await.unwrap(); },
            "shape" => {
                sqlx::query("UPDATE graphs SET kind='org',owner_user_id=NULL WHERE id=$1").bind(f.graph).execute(f.owner()).await.unwrap();
                sqlx::query("UPDATE account_members SET role='member' WHERE account_id=$1").bind(f.user.account_id).execute(&f.control.owner).await.unwrap();
            },
            "input" => { sqlx::query("UPDATE source_versions SET raw_inline='Replaced retained text.',content_sha256=$2 WHERE version_id=$1").bind(version).bind("d43acef5cdf6a370cf608b02ee2b21922af533f4112034715ac9e0fd10d00445").execute(f.owner()).await.unwrap(); },
            _=>unreachable!(),
        }
        publication.release(); finish_tick(task).await;
        let semantic:(i64,i64,i64)=sqlx::query_as("SELECT (SELECT count(*) FROM semantic_atoms WHERE source_version_id=$1),(SELECT count(*) FROM semantic_atom_passages WHERE source_version_id=$1),(SELECT count(*) FROM semantic_atom_windows WHERE source_version_id=$1)")
            .bind(version).fetch_one(f.owner()).await.unwrap();
        assert_eq!(semantic,(0,0,0),"stale {mutation} accepted semantic publication");
        assert_eq!(http.load(Ordering::SeqCst),1);
        let charged:i64=sqlx::query_scalar("SELECT sum(settled_micro_usd)::bigint FROM managed_attempt_receipts").fetch_one(f.owner()).await.unwrap();
        assert_eq!(charged,400);
        server.abort();f.done().await;
    }
}
#[tokio::test]
async fn consumer_standing_concurrent_graph_account_owner_archive_shape_and_input_fences() { standing_graph_mutation_before_publication(false).await; }
#[tokio::test]
async fn tenant_standing_concurrent_graph_account_owner_archive_shape_and_input_fences() { standing_graph_mutation_before_publication(true).await; }

async fn standing_source_lock_cycle(org: bool) {
    use coralswarm_backend::test_standing_probe::{pause_at,Stage};
    for delete in [false,true] {
        let f=Fixture::new(org).await;
        let version=f.capture("note","parent-lock-cycle","Retained evidence.").await;
        f.sweep().await;
        let job=pending_job(&f,version).await;
        let publication=pause_at(job,Stage::BeforePublication);
        let (llm,http,server)=provider().await;
        let task=spawn_tick(&f,Arc::new(llm));
        wait_probe(&publication).await;
        let writer=PgPoolOptions::new().max_connections(1)
            .connect_with(f.worker.pool.connect_options().as_ref().clone()).await.unwrap();
        let mut tx=match f.worker.tenant {
            TenantId::Consumer=>scope::begin_account_scoped(&writer,f.user.account_id).await.unwrap(),
            TenantId::Org(_)=>scope::begin_tenant_system_scoped(&writer,f.user.account_id).await.unwrap(),
        };
        let parent:Uuid=sqlx::query_scalar("SELECT source_item_id FROM source_items WHERE graph_id=$1 AND source_item_id=(SELECT source_item_id FROM source_versions WHERE version_id=$2) FOR UPDATE")
            .bind(f.graph).bind(version).fetch_one(&mut *tx).await.unwrap();
        publication.release();
        tokio::time::timeout(Duration::from_millis(300),async {
            loop {
                let waiting:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE '%SELECT source_item_id FROM source_items%')")
                    .fetch_one(f.owner()).await.unwrap();
                if waiting { break; }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }).await.expect("actual publication must wait on the parent's normal-role row lock");
        if delete {
            sqlx::query("DELETE FROM source_items WHERE graph_id=$1 AND source_item_id=$2")
                .bind(f.graph).bind(parent).execute(&mut *tx).await.unwrap();
        } else {
            let saved=source_evidence::capture_source_version_tx(&mut tx,None,f.graph,f.user.account_id,&CaptureSource {
                source_type:"note", provider_namespace:"fixture:standing", source_id:"parent-lock-cycle",
                normalized_text:"Replacement retained evidence.", representation:"original_text",metadata:&json!({}),
                occurred_at:None,participants:&[],ingested_by:Some(f.user.user_id),scope:"private",duration_ms:None,segments:None,
            },None,&[42;32]).await.unwrap();
            assert_ne!(saved.version_id,version);
        }
        tx.commit().await.unwrap();
        finish_tick(task).await;
        let counts:(i64,i64,i64)=sqlx::query_as("SELECT (SELECT count(*) FROM semantic_atoms WHERE source_version_id=$1),(SELECT count(*) FROM semantic_atom_passages WHERE source_version_id=$1),(SELECT count(*) FROM semantic_atom_windows WHERE source_version_id=$1)")
            .bind(version).fetch_one(f.owner()).await.unwrap();
        assert_eq!(counts,if delete {(0,0,0)} else {(1,1,1)},"replacement preserves immutable old input; deletion forbids orphan publication");
        let versions:i64=sqlx::query_scalar("SELECT count(*) FROM source_versions WHERE graph_id=$1")
            .bind(f.graph).fetch_one(f.owner()).await.unwrap();
        assert_eq!(versions,if delete {0} else {2});
        assert_eq!(http.load(Ordering::SeqCst),1);
        let billed:i64=sqlx::query_scalar("SELECT sum(settled_micro_usd)::bigint FROM managed_attempt_receipts").fetch_one(f.owner()).await.unwrap();
        assert_eq!(billed,400);
        writer.close().await;server.abort();f.done().await;
    }
}
#[tokio::test]
async fn consumer_standing_parent_lock_deletion_and_replacement_cycle() { standing_source_lock_cycle(false).await; }
#[tokio::test]
async fn tenant_standing_parent_lock_deletion_and_replacement_cycle() { standing_source_lock_cycle(true).await; }

async fn standing_later_scope_survives_busy_first(org: bool, same_account: bool) {
    let f=Fixture::new(org).await;
    let first=if same_account { f.user.account_id } else { Uuid::from_u128(1) };
    let graph=Uuid::from_u128(1);
    let first_principal=if same_account { f.user.user_id } else { Uuid::new_v4() };
    if !same_account {
    sqlx::query("INSERT INTO users(id,email,clerk_user_id) VALUES($1,$2,$2)")
        .bind(first_principal).bind(format!("{first_principal}@fixture.local")).execute(&f.control.owner).await.unwrap();
    sqlx::query("INSERT INTO accounts(id,owner_user_id) VALUES($1,$2)")
        .bind(first).bind(first_principal).execute(&f.control.owner).await.unwrap();
    sqlx::query("INSERT INTO account_members(account_id,user_id,role) VALUES($1,$2,'owner')")
        .bind(first).bind(first_principal).execute(&f.control.owner).await.unwrap();
    }
    sqlx::query("INSERT INTO graphs(id,account_id,name,kind,owner_user_id) VALUES($1,$2,'Busy first account','org',NULL)")
        .bind(graph).bind(first).execute(f.owner()).await.unwrap();
    let run:Uuid=sqlx::query_scalar("INSERT INTO corpus_enrichment_runs(graph_id,account_id,requested_by,execution_principal,mode,run_kind,corpus_kind,status,standing_pause_state,extraction_version,model,max_items,max_characters,max_model_calls) VALUES($1,$2,$3,$3,'execute','standing','real','running','none','source-atoms-v2',$4,1,1,1) RETURNING run_id")
        .bind(graph).bind(first).bind(first_principal).bind(PRIMARY).fetch_one(f.owner()).await.unwrap();
    let writer=PgPoolOptions::new().max_connections(1)
        .connect_with(f.worker.pool.connect_options().as_ref().clone()).await.unwrap();
    let mut blocker=match f.worker.tenant {
        TenantId::Consumer=>scope::begin_account_scoped(&writer,first).await.unwrap(),
        TenantId::Org(_)=>scope::begin_tenant_system_scoped(&writer,first).await.unwrap(),
    };
    let locked:Uuid=sqlx::query_scalar("SELECT run_id FROM corpus_enrichment_runs WHERE run_id=$1 FOR UPDATE")
        .bind(run).fetch_one(&mut *blocker).await.unwrap();
    assert_eq!(locked,run,"the earlier run must be genuinely locked");
    let (llm,http,server)=provider().await;
    for id in ["later-account-first","later-account-again"] {
        let version=f.capture("note",id,"Retained evidence.").await;
        let error=tokio::time::timeout(Duration::from_secs(20),coralswarm_backend::test_standing_sweep(&f.worker,&f.control.app))
            .await.expect("a busy earlier account must not consume the whole sweep").unwrap_err();
        assert!(error.contains(&first.to_string()),"{error}");
        let queued:i64=sqlx::query_scalar("SELECT count(*) FROM corpus_enrichment_units WHERE holding_id=$1 AND state='queued' AND execution_owner")
            .bind(version).fetch_one(f.owner()).await.unwrap();
        assert_eq!(queued,1,"later account must be serviced despite earlier account failure");
        source_atoms::tick(&f.worker,&f.control.app,&llm,None).await.unwrap();
        assert_eq!(completed_snapshot(&f,version).await["source"]["extraction_status"],json!("done"));
    }
    assert_eq!(http.load(Ordering::SeqCst),2);
    blocker.rollback().await.unwrap();writer.close().await;server.abort();f.done().await;
}
#[tokio::test]
async fn consumer_standing_busy_first_account_does_not_starve_later_account() { for same_account in [false,true] { standing_later_scope_survives_busy_first(false,same_account).await; } }
#[tokio::test]
async fn tenant_standing_busy_first_account_does_not_starve_later_account() { for same_account in [false,true] { standing_later_scope_survives_busy_first(true,same_account).await; } }

async fn standing_reporter_uses_durable_eligible_history(org: bool) {
    use axum::{extract::{State,Path},Extension,Json};
    use coralswarm_backend::{capture_admission_health::{self as health,ClockClass},workers::capture_admission_health as reporter};
    let f=Fixture::with_history(org,true).await;
    let historical:Uuid=sqlx::query_scalar("SELECT id FROM graphs WHERE name='Synthetic restored clock history'")
        .fetch_one(f.owner()).await.unwrap();
    let version=f.capture("note","ordinary-fresh-unknown-clock","Retained evidence.").await;
    let before=reporter::measure(&f.worker,&f.control.app,chrono::Utc::now()).await.unwrap();
    let coverage=before.clock_coverage.as_ref().unwrap();
    assert!(coverage.iter().any(|row|row.graph_id==f.graph && row.class==ClockClass::Unknown && row.captures==1 && row.processed==0));
    for (class,count) in [(ClockClass::NeverHeld,2),(ClockClass::Held,1),(ClockClass::Backlog,1),(ClockClass::Legacy,1),(ClockClass::Unknown,1)] {
        assert_eq!(coverage.iter().filter(|row|row.graph_id==historical && row.class==class).map(|row|row.captures).sum::<i64>(),count);
    }
    assert!(coverage.iter().any(|row|row.graph_id==historical && row.class==ClockClass::Unknown && row.hold_basis.as_deref()==Some("never_held") && row.captures==1 && row.processed==0 && row.waiting==0),"source DONE with nonprocessed trusted receipt must be unknown/red, not an empty eligible sample");
    let lag=before.lag.iter().find(|row|row.graph_id==historical).unwrap();
    assert_eq!((lag.processed,lag.p95_secs,lag.p99_secs,lag.max_secs,lag.slo_met),(1,300.0,300.0,300.0,true));
    assert!(before.waiting.iter().any(|row|row.graph_id==historical && row.state=="queued" && row.never_held && row.over_alert_age==1 && row.over_lag_age==1));
    let verdict=health::verdict(&[before.clone()],&[before.tenant.clone()],before.measured_at);
    assert!(verdict.violated() && !verdict.unknown_clock.is_empty() && verdict.queued_lag_violations.len()==1);
    let state=Arc::new(coralswarm_backend::test_app_state_owned_with_db_and_store(f.control.app.clone(),None));
    let (status,Json(body))=coralswarm_backend::corpus_enrichment::create_run(State(state.clone()),f.tenant.clone(),Extension(f.user.clone()),Json(serde_json::from_value(json!({"graph_id":f.graph,"execute":true,"budget":{"items":100,"characters":50000,"model_calls":100}})).unwrap())).await;
    assert_eq!(status,axum::http::StatusCode::CREATED,"{body}");
    let run:Uuid=body["run_id"].as_str().unwrap().parse().unwrap();
    let (status,Json(body))=coralswarm_backend::corpus_enrichment::advance_run(State(state),f.tenant.clone(),Extension(f.user.clone()),Path(run),Json(serde_json::from_value(json!({"graph_id":f.graph,"page_size":100})).unwrap())).await;
    assert_eq!(status,axum::http::StatusCode::OK,"{body}");
    let (llm,http,server)=provider().await;
    source_atoms::tick(&f.worker,&f.control.app,&llm,None).await.unwrap();
    assert_eq!(http.load(Ordering::SeqCst),1);
    assert_eq!(completed_snapshot(&f,version).await["source"]["extraction_status"],json!("done"));
    reporter::tick(&f.worker,&f.control.app).await.unwrap();
    let report:Value=sqlx::query_scalar("SELECT report FROM capture_admission_health WHERE tenant=$1").bind(&before.tenant).fetch_one(&f.control.app).await.unwrap();
    let after:health::PlaneReport=serde_json::from_value(report).unwrap();
    assert!(after.clock_coverage.as_ref().unwrap().iter().any(|row|row.graph_id==f.graph && row.class==ClockClass::Unknown && row.processed==1 && row.captures==1));
    assert!(after.lag.iter().all(|row|row.graph_id!=f.graph));
    let unknown=health::verdict(&[after.clone()],&[after.tenant.clone()],after.measured_at);
    assert!(unknown.violated() && unknown.unknown_clock.iter().any(|(_,row)|row.graph_id==f.graph && row.processed==1));
    prefs(&f,json!({"managed_processing":"off"})).await;
    let off=reporter::measure(&f.worker,&f.control.app,chrono::Utc::now()).await.unwrap();
    assert!(off.lag.is_empty());
    assert!(off.clock_coverage.as_ref().unwrap().iter().all(|row|row.processing_off));
    assert_eq!(off.stalled().count(),0,"current processing-off is a deliberate hold, independently of unknown history");
    let off_verdict=health::verdict(&[off.clone()],&[off.tenant.clone()],off.measured_at);
    assert!(off_verdict.violated() && !off_verdict.unknown_clock.is_empty() && off_verdict.queued_lag_violations.is_empty());
    server.abort();f.done().await;
}
#[tokio::test]
async fn consumer_standing_reporter_excludes_held_preserves_unknown_and_consumes_trusted_history() { standing_reporter_uses_durable_eligible_history(false).await; }
#[tokio::test]
async fn tenant_standing_reporter_excludes_held_preserves_unknown_and_consumes_trusted_history() { standing_reporter_uses_durable_eligible_history(true).await; }

// Exercise the real scoped sweep, then its actual harness success scheduler.
// A capture arrives just behind the advanced cursor among >4 account batches.
async fn standing_partial_scan_continues_without_minute_sleeps(org: bool) {
    use coralswarm_backend::test_standing_probe::{configure_sweep_clock, clear_sweep_clock, sweep_accounts};
    use coralswarm_backend::workers::tenant_harness::test_standing_scheduled_delay;
    let f=Fixture::new(org).await;
    let database=f.worker.pool.connect_options().get_database().unwrap().to_owned();
    for n in 0..106 {
        let account=Uuid::from_u128(u128::MAX-n);
        let user=Uuid::new_v4();
        sqlx::query("INSERT INTO users(id,email,clerk_user_id) VALUES($1,$2,$2)")
            .bind(user).bind(format!("{user}@fixture.local")).execute(&f.control.owner).await.unwrap();
        sqlx::query("INSERT INTO accounts(id,owner_user_id) VALUES($1,$2)")
            .bind(account).bind(user).execute(&f.control.owner).await.unwrap();
        // Org discovery enumerates content accounts; archived graphs need no
        // maintenance, but must still cost an account visit on either plane.
        sqlx::query("INSERT INTO graphs(account_id,name,kind,archived_at) VALUES($1,'Idle scan fixture','org',now())")
            .bind(account).execute(f.owner()).await.unwrap();
    }
    configure_sweep_clock(&database,Duration::ZERO);
    let mut visited=std::collections::HashSet::new();
    let first=coralswarm_backend::test_standing_sweep(&f.worker,&f.control.app).await.unwrap();
    assert_eq!(sweep_accounts(&database).first(),Some(&f.user.account_id));
    assert_eq!(sweep_accounts(&database).len(),24);
    visited.extend(sweep_accounts(&database));
    assert_eq!(test_standing_scheduled_delay(first,&f.worker.tenant),Duration::from_secs(1),"partial idle scan must continue through the actual sweep scheduler");
    let version=f.capture("note","fresh-behind-cursor","Retained evidence.").await;
    let mut scheduled=Duration::from_secs(1);
    for pass in 1..5 {
        let tick=coralswarm_backend::test_standing_sweep(&f.worker,&f.control.app).await.unwrap();
        let accounts=sweep_accounts(&database);
        assert_eq!(accounts.len(),if pass==4 {11} else {24});
        for account in accounts { assert!(visited.insert(account),"no repeated account in one finite round"); }
        let delay=test_standing_scheduled_delay(tick,&f.worker.tenant);
        assert_eq!(delay,Duration::from_secs(if pass==4 {60} else {1}));
        scheduled+=delay;
    }
    assert_eq!(visited.len(),107,"idle inventory must make fair bounded progress");
    assert_eq!(scheduled,Duration::from_secs(64));
    let before:i64=sqlx::query_scalar("SELECT count(*) FROM corpus_enrichment_units WHERE holding_id=$1").bind(version).fetch_one(f.owner()).await.unwrap();
    assert_eq!(before,0,"fresh capture really is behind the cursor");
    let tick=coralswarm_backend::test_standing_sweep(&f.worker,&f.control.app).await.unwrap();
    assert_eq!(test_standing_scheduled_delay(tick,&f.worker.tenant),Duration::from_secs(1));
    let job=pending_job(&f,version).await;
    // Another finite round must not multiply admissions/jobs or spin on idle
    // inventory; provider gates are still exercised by the real worker.
    for pass in 1..5 {
        let tick=coralswarm_backend::test_standing_sweep(&f.worker,&f.control.app).await.unwrap();
        assert_eq!(test_standing_scheduled_delay(tick,&f.worker.tenant),Duration::from_secs(if pass==4 {60} else {1}));
    }
    assert_eq!(pending_job(&f,version).await,job);
    let counts:(i64,i64)=sqlx::query_as("SELECT (SELECT count(*) FROM corpus_enrichment_units WHERE holding_id=$1 AND execution_owner),(SELECT count(*) FROM jobs WHERE kind='ExtractStandingSourceAtoms' AND status='pending')")
        .bind(version).fetch_one(f.owner()).await.unwrap();
    assert_eq!(counts,(1,1));
    clear_sweep_clock(&database);
    let (llm,http,server)=provider().await;
    source_atoms::tick(&f.worker,&f.control.app,&llm,None).await.unwrap();
    assert_eq!(completed_snapshot(&f,version).await["source"]["extraction_status"],json!("done"));
    assert_eq!(http.load(Ordering::SeqCst),1);
    server.abort();f.done().await;
}
#[tokio::test]
async fn consumer_standing_partial_scan_continues_without_minute_sleeps() { standing_partial_scan_continues_without_minute_sleeps(false).await; }
#[tokio::test]
async fn tenant_standing_partial_scan_continues_without_minute_sleeps() { standing_partial_scan_continues_without_minute_sleeps(true).await; }

async fn standing_deadline_resumes_inner_profile_and_retired_slot(org: bool) {
    use coralswarm_backend::test_standing_probe::{configure_sweep_clock,clear_sweep_clock,sweep_slots};
    let mut f=Fixture::new(org).await;
    sqlx::query("UPDATE graphs SET archived_at=now() WHERE id=$1").bind(f.graph).execute(f.owner()).await.unwrap();
    let graphs:Vec<Uuid>=(100..120).map(Uuid::from_u128).collect();
    for graph in &graphs {
        sqlx::query("INSERT INTO graphs(id,account_id,name,kind,owner_user_id) VALUES($1,$2,'Controlled deadline traversal','member',$3)")
            .bind(graph).bind(f.user.account_id).bind(f.user.user_id).execute(f.owner()).await.unwrap();
    }
    f.graph=graphs[9];
    let retired_version=f.capture("note","deadline-retired-owner","Retained evidence.").await;
    f.sweep().await;
    let retired_run:Uuid=sqlx::query_scalar("UPDATE corpus_enrichment_runs SET retired_at=now() WHERE run_id=(SELECT run_id FROM corpus_enrichment_units WHERE holding_id=$1 AND execution_owner) RETURNING run_id")
        .bind(retired_version).fetch_one(f.owner()).await.unwrap();
    let old_job=pending_job(&f,retired_version).await;
    coralswarm_backend::jobs::queue::complete(&f.worker.pool,old_job,coralswarm_backend::jobs::queue::JobOutcome::NothingToDo).await.unwrap();
    let mut tx=match f.worker.tenant {
        TenantId::Consumer=>scope::begin_account_scoped(&f.worker.pool,f.user.account_id).await.unwrap(),
        TenantId::Org(_)=>scope::begin_tenant_system_scoped(&f.worker.pool,f.user.account_id).await.unwrap(),
    };
    coralswarm_backend::corpus_enrichment::hold_window_tx(&mut tx,f.graph,retired_version,"source-atoms-v2","infrastructure_unavailable").await.unwrap();
    sqlx::query("UPDATE corpus_enrichment_units SET retry_after=now() WHERE holding_id=$1")
        .bind(retired_version).execute(&mut *tx).await.unwrap();
    tx.commit().await.unwrap();
    let item:Uuid=sqlx::query_scalar("INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id) VALUES($1,'transcript','legacy:meeting','deadline-later-profile') RETURNING source_item_id")
        .bind(f.graph).fetch_one(f.owner()).await.unwrap();
    let meeting:Uuid=sqlx::query_scalar("INSERT INTO source_versions(graph_id,source_item_id,source_id,source_type,provider_namespace,raw_inline,is_legacy_baseline,extraction_version,extraction_status,extraction_error) VALUES($1,$2,'deadline-later-profile','transcript','legacy:meeting','Retained evidence.',true,'meeting-atoms-v1','blocked','managed_admission_required') RETURNING version_id")
        .bind(f.graph).bind(item).fetch_one(f.owner()).await.unwrap();
    sqlx::query("UPDATE source_items SET current_version_id=$2 WHERE source_item_id=$1").bind(item).bind(meeting).execute(f.owner()).await.unwrap();
    let database=f.worker.pool.connect_options().get_database().unwrap().to_owned();
    configure_sweep_clock(&database,Duration::from_secs(3));
    f.sweep().await;
    let first=sweep_slots(&database);
    assert_eq!(first.len(),37);
    assert_eq!(first.last(),Some(&(f.graph,"source-atoms-v2".to_owned(),false)));
    let state:String=sqlx::query_scalar("SELECT state FROM corpus_enrichment_units WHERE holding_id=$1 AND run_id=$2")
        .bind(retired_version).bind(retired_run).fetch_one(f.owner()).await.unwrap();
    assert_eq!(state,"pending");
    let meeting_owner:i64=sqlx::query_scalar("SELECT count(*) FROM corpus_enrichment_units WHERE holding_id=$1 AND execution_owner")
        .bind(meeting).fetch_one(f.owner()).await.unwrap();
    assert_eq!(meeting_owner,0,"later profile must genuinely wait past the first deadline");
    f.sweep().await;
    let second=sweep_slots(&database);
    assert_eq!(second.first(),Some(&(f.graph,"source-atoms-v2".to_owned(),true)),"resume the exact interrupted inner slot rather than restarting a slow graph");
    assert!(second.contains(&(f.graph,"meeting-atoms-v1".to_owned(),false)));
    assert!(second.contains(&(f.graph,"meeting-atoms-v1".to_owned(),true)));
    assert_eq!(second.len(),37,"all slots stay identically slow on every sweep");
    f.sweep().await;
    let third=sweep_slots(&database);
    for profile in ["source-atoms-v2","meeting-atoms-v1"] {
        for retired in [false,true] {
            assert!(third.contains(&(graphs[19],profile.to_owned(),retired)),"the second cutoff graph must also eventually receive every inner slot");
        }
    }
    clear_sweep_clock(&database);
    let (llm,http,server)=provider().await;
    for _ in 0..2 { source_atoms::tick(&f.worker,&f.control.app,&llm,None).await.unwrap(); }
    for version in [retired_version,meeting] {
        let done:(String,i64,i64)=sqlx::query_as("SELECT extraction_status,(SELECT count(*) FROM semantic_atoms WHERE source_version_id=$1),(SELECT count(*) FROM semantic_atom_windows WHERE source_version_id=$1) FROM source_versions WHERE version_id=$1")
            .bind(version).fetch_one(f.owner()).await.unwrap();
        assert_eq!(done,("done".into(),1,1),"both the retired-owned source and later meeting profile must complete through the actual registered worker");
    }
    let owner:Uuid=sqlx::query_scalar("SELECT run_id FROM corpus_enrichment_units WHERE holding_id=$1 AND execution_owner")
        .bind(retired_version).fetch_one(f.owner()).await.unwrap();
    assert_eq!(owner,retired_run);
    assert_eq!(http.load(Ordering::SeqCst),2);
    server.abort();f.done().await;
}
#[tokio::test]
async fn consumer_standing_interrupted_deadline_resumes_later_profile_and_retired_work() { standing_deadline_resumes_inner_profile_and_retired_slot(false).await; }
#[tokio::test]
async fn tenant_standing_interrupted_deadline_resumes_later_profile_and_retired_work() { standing_deadline_resumes_inner_profile_and_retired_slot(true).await; }

// ── node_atoms bridge (source-agnostic pipeline plan PR 1) ──────────────────
//
// Managed extraction links reefs through `node_semantic_atoms`; the bridge
// mirrors each link into `node_atoms` for the cited version's chunk atoms so
// the `node_atoms` readers (MCP `get_reef`, reef corals, atlas, digests via
// `coral_count`) see it. These drive the actual standing sweep and worker.

/// Retrieval chunk atoms for a captured version, adopted the way ingest does
/// (`link_verified_chunks_tx`). `reuse` are chunks of an earlier version kept
/// unchanged by a re-capture.
async fn attach_chunks(f: &Fixture, version: Uuid, source_id: &str, fresh: usize, reuse: &[Uuid]) -> Vec<Uuid> {
    let mut chunks = reuse.to_vec();
    for i in 0..fresh {
        let id: Uuid = sqlx::query_scalar("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,timestamp,ingested_by) VALUES($1,'note',$2,'',$3,now(),$4) RETURNING atom_id")
            .bind(f.graph).bind(format!("{source_id}:{}:chunk:{i}", Uuid::new_v4().simple())).bind("Retained evidence.").bind(f.user.user_id)
            .fetch_one(f.owner()).await.unwrap();
        chunks.push(id);
    }
    let mut tx = f.owner().begin().await.unwrap();
    source_evidence::link_verified_chunks_tx(&mut tx, f.graph, version, &chunks).await.unwrap();
    tx.commit().await.unwrap();
    chunks.sort();
    chunks
}

/// Two standing sweeps, then the actual worker until every version is done.
async fn extract_all(f: &Fixture) {
    f.sweep().await;
    f.sweep().await;
    let (llm, _hits, server) = provider().await;
    for _ in 0..8 {
        source_atoms::tick(&f.worker, &f.control.app, &llm, None).await.unwrap();
    }
    server.abort();
    let pending: Vec<(Uuid, String)> = sqlx::query_as("SELECT version_id,extraction_status FROM source_versions WHERE extraction_status<>'done'")
        .fetch_all(f.owner()).await.unwrap();
    assert!(pending.is_empty(), "every version must finish extraction: {pending:?}");
}

/// MCP `get_reef`'s coral reader, through the same member-scoped transaction
/// the tool opens.
async fn reef_corals(f: &Fixture, node: Uuid) -> Vec<Uuid> {
    let mut tx = f.tenant.begin_member_scoped(f.user.account_id).await.unwrap();
    let atoms = f.worker.store.reef_atoms_scoped(&mut tx, node, f.user.account_id, 50).await.unwrap()
        .expect("reef visible to its owner");
    tx.rollback().await.unwrap();
    let mut ids: Vec<Uuid> = atoms.into_iter().map(|a| a.atom_id).collect();
    ids.sort();
    ids
}

/// Every reef the semantic layer links, with the union of chunk atoms of the
/// versions its semantic atoms cite (the bridge's contract).
async fn expected_bridge(f: &Fixture) -> Vec<(Uuid, Vec<Uuid>)> {
    sqlx::query_as("SELECT l.node_id, COALESCE(array_agg(DISTINCT c.atom_id ORDER BY c.atom_id) FILTER (WHERE c.atom_id IS NOT NULL),'{}') FROM node_semantic_atoms l JOIN semantic_atoms a USING(semantic_atom_id) LEFT JOIN source_version_chunks c ON c.source_version_id=a.source_version_id AND c.graph_id=a.graph_id GROUP BY l.node_id ORDER BY l.node_id")
        .fetch_all(f.owner()).await.unwrap()
}

async fn assert_bridged(f: &Fixture, label: &str) -> usize {
    let expected = expected_bridge(f).await;
    assert!(!expected.is_empty(), "{label}: extraction must link reefs");
    let mut total = 0;
    for (node, chunks) in &expected {
        let mut linked: Vec<Uuid> = sqlx::query_scalar("SELECT atom_id FROM node_atoms WHERE node_id=$1")
            .bind(node).fetch_all(f.owner()).await.unwrap();
        linked.sort();
        assert_eq!(&linked, chunks, "{label}: node_atoms for reef {node}");
        let count: i32 = sqlx::query_scalar("SELECT coral_count FROM nodes WHERE node_id=$1")
            .bind(node).fetch_one(f.owner()).await.unwrap();
        assert_eq!(count as usize, chunks.len(), "{label}: coral_count for reef {node}");
        assert_eq!(&reef_corals(f, *node).await, chunks, "{label}: get_reef corals for reef {node}");
        total += chunks.len();
    }
    let ops: i64 = sqlx::query_scalar("SELECT count(*) FROM op_log WHERE entity='node_atom'")
        .fetch_one(f.owner()).await.unwrap();
    assert_eq!(ops as usize, total, "{label}: one sync op per inserted link, none for repeats");
    let cross: i64 = sqlx::query_scalar("SELECT count(*) FROM node_atoms na JOIN nodes n USING(node_id) JOIN atoms a USING(atom_id) WHERE a.graph_id<>n.graph_id")
        .fetch_one(f.owner()).await.unwrap();
    assert_eq!(cross, 0, "{label}: a reef only gains corals from its own ocean");
    total
}

/// RLS on the new rows: the owning member sees them, another principal on the
/// same plane does not. Needs an app-role pool; the owner-runtime fixture's
/// pools bypass RLS by design, so its scoping is proven by the app-role
/// tenant test instead.
async fn assert_scoped(f: &Fixture, org: bool, total: usize) {
    let app = &f.content.as_ref().unwrap_or(&f.control).app;
    if owner_runtime(f).await {
        return;
    }
    let visible = |account: Uuid, user: Uuid| async move {
        let mut tx = if org {
            scope::begin_tenant_scoped(app, account, user).await.unwrap()
        } else {
            scope::begin_account_scoped(app, account).await.unwrap()
        };
        let n: i64 = sqlx::query_scalar("SELECT count(*) FROM node_atoms").fetch_one(&mut *tx).await.unwrap();
        tx.rollback().await.unwrap();
        n as usize
    };
    assert_eq!(visible(f.user.account_id, f.user.user_id).await, total);
    let (stranger_account, stranger_user) = if org { (f.user.account_id, Uuid::new_v4()) } else { (Uuid::new_v4(), Uuid::new_v4()) };
    assert_eq!(visible(stranger_account, stranger_user).await, 0, "another principal must not see the bridged corals");
}

enum BridgeShape { Consumer, Tenant, TenantOwner }

async fn bridge_fixture(shape: &BridgeShape) -> (Fixture, bool) {
    match shape {
        BridgeShape::Consumer => (Fixture::new(false).await, false),
        BridgeShape::Tenant => (Fixture::new(true).await, true),
        BridgeShape::TenantOwner => (Fixture::create(true, false, true).await, true),
    }
}

/// Whether the content pools connect as the table owner (production's org
/// shape; `Database::create(.., owner_runtime: true)`).
async fn owner_runtime(f: &Fixture) -> bool {
    sqlx::query_scalar("SELECT pg_get_userbyid(relowner)=current_user FROM pg_class WHERE oid='public.node_atoms'::regclass")
        .fetch_one(&f.worker.pool).await.unwrap()
}

async fn standing_extraction_links_node_atoms(shape: BridgeShape) {
    // One job per tick. A tenant-plane job needs a connection from BOTH
    // one-connection fixture pools (control `app`, then the content `app`); at
    // the default SOURCE_ATOMS_CONCURRENCY of 8, sibling jobs each take one
    // pool and wait on the other until the 2 s `acquire_timeout` fires, and
    // the version is held as `infrastructure_unavailable` (extraction_status
    // `blocked`). Production pools are not one connection wide; serialising
    // keeps this test about what it asserts rather than fixture pool width.
    let _serial = EnvGuard::set(source_atoms::CONCURRENCY_ENV, "1");
    let (f, org) = bridge_fixture(&shape).await;
    let a = f.capture("note", "bridge-a", "Retained evidence.").await;
    let a_chunks = attach_chunks(&f, a, "bridge-a", 3, &[]).await;
    let b = f.capture("slack", "bridge-b", "Retained evidence.").await;
    attach_chunks(&f, b, "bridge-b", 2, &[]).await;
    // Two windows of one version resolve to the same reef, so the second
    // window re-links chunks the first already linked (primary-key no-op).
    let multi = f.capture("coding_session", "bridge-multi-window",
        &format!("Retained evidence.\n{}Retained evidence.", "padding\n".repeat(1400))).await;
    attach_chunks(&f, multi, "bridge-multi-window", 2, &[]).await;
    // A version whose chunks ingest did not adopt: nothing to bridge.
    f.capture("note", "bridge-unmapped", "Retained evidence.").await;
    extract_all(&f).await;
    let windows: i64 = sqlx::query_scalar("SELECT count(*) FROM semantic_atom_windows WHERE source_version_id=$1")
        .bind(multi).fetch_one(f.owner()).await.unwrap();
    let multi_reefs: i64 = sqlx::query_scalar("SELECT count(DISTINCT l.node_id) FROM node_semantic_atoms l JOIN semantic_atoms a USING(semantic_atom_id) WHERE a.source_version_id=$1")
        .bind(multi).fetch_one(f.owner()).await.unwrap();
    assert_eq!((windows, multi_reefs), (2, 1), "both windows link one reef");
    let total = assert_bridged(&f, "first extraction").await;
    assert_eq!(total, 7, "3 + 2 + 2 chunks bridged once each");
    assert_scoped(&f, org, total).await;

    // Re-capture keeps one unchanged chunk and adds one; extracting the new
    // version re-links the kept chunk, which must not duplicate or re-count.
    let a2 = f.capture("note", "bridge-a", "Retained evidence. Revised.").await;
    attach_chunks(&f, a2, "bridge-a", 1, &a_chunks[..1]).await;
    extract_all(&f).await;
    let after = assert_bridged(&f, "re-extraction").await;
    // Under v2 the new version's mention binds the existing reef by exact
    // title, which already holds the kept chunk: only the new chunk is new.
    assert_eq!(after, total + 1, "the reef gets the new chunk; the kept one is not re-counted");

    // The backfill finds nothing left to do on bridged output.
    let preview = coralswarm_backend::node_atoms_bridge::preview(f.owner(), None).await.unwrap();
    assert_eq!(preview.missing_links, 0, "{preview:?}");
    let report = coralswarm_backend::node_atoms_bridge::execute(f.owner(), None, 1, None, |_| {}).await.unwrap();
    assert_eq!((report.batches, report.inserted), (0, 0));
    assert_eq!(assert_bridged(&f, "after backfill").await, after);
    println!("node_atoms_bridge_live org={org} links={after}");
    f.done().await;
}

#[tokio::test]
async fn consumer_standing_extraction_links_node_atoms() { standing_extraction_links_node_atoms(BridgeShape::Consumer).await; }
#[tokio::test]
async fn tenant_standing_extraction_links_node_atoms() { standing_extraction_links_node_atoms(BridgeShape::Tenant).await; }
#[tokio::test]
async fn tenant_owner_standing_extraction_links_node_atoms() { standing_extraction_links_node_atoms(BridgeShape::TenantOwner).await; }

async fn node_atoms_backfill_bridges_existing_output(shape: BridgeShape) {
    use coralswarm_backend::node_atoms_bridge;
    let (f, org) = bridge_fixture(&shape).await;
    let a = f.capture("note", "backfill-a", "Retained evidence.").await;
    attach_chunks(&f, a, "backfill-a", 3, &[]).await;
    let b = f.capture("slack", "backfill-b", "Retained evidence.").await;
    attach_chunks(&f, b, "backfill-b", 2, &[]).await;
    // Pre-bridge production state: semantic links, no node_atoms. The guard
    // clears the kill switch even if extraction panics, so later tests in this
    // process see the default.
    struct KillSwitch;
    impl Drop for KillSwitch {
        fn drop(&mut self) { std::env::remove_var(coralswarm_backend::node_atoms_bridge::ENV_FLAG); }
    }
    {
        let _off = KillSwitch;
        std::env::set_var(node_atoms_bridge::ENV_FLAG, "false");
        extract_all(&f).await;
    }
    let expected = expected_bridge(&f).await;
    let want: usize = expected.iter().map(|(_, c)| c.len()).sum();
    assert!(want >= 5);
    for (node, _) in &expected {
        assert!(reef_corals(&f, *node).await.is_empty(), "kill switch leaves get_reef empty");
    }

    assert!(node_atoms_bridge::assert_policy_bypass(f.owner()).await.is_ok());
    if !owner_runtime(&f).await {
        let app = &f.content.as_ref().unwrap_or(&f.control).app;
        assert!(node_atoms_bridge::assert_policy_bypass(app).await.is_err(), "an RLS-filtered role is refused");
    }
    let preview = node_atoms_bridge::preview(f.owner(), None).await.unwrap();
    assert_eq!(preview.missing_links as usize, want, "{preview:?}");
    assert_eq!(preview.reefs_currently_empty as usize, expected.len(), "{preview:?}");
    let other_graph = node_atoms_bridge::preview(f.owner(), Some(Uuid::new_v4())).await.unwrap();
    assert_eq!(other_graph.missing_links, 0, "--graph-id narrows to one ocean");

    // Bounded rehearsal, then the rest; one pair per transaction.
    let first = node_atoms_bridge::execute(f.owner(), None, 1, Some(1), |_| {}).await.unwrap();
    assert_eq!(first.batches, 1);
    let mut seen = Vec::new();
    let rest = node_atoms_bridge::execute(f.owner(), None, 1, None, |b| seen.push(*b)).await.unwrap();
    assert_eq!((first.inserted + rest.inserted) as usize, want);
    assert_eq!(seen.len() as u64, rest.batches);
    let total = assert_bridged(&f, "backfill").await;
    assert_eq!(total, want);
    assert_scoped(&f, org, total).await;

    let again = node_atoms_bridge::execute(f.owner(), None, 50, None, |_| {}).await.unwrap();
    assert_eq!((again.batches, again.inserted), (0, 0), "re-run is a no-op");
    assert_eq!(node_atoms_bridge::preview(f.owner(), None).await.unwrap().missing_links, 0);
    assert_eq!(assert_bridged(&f, "backfill re-run").await, want);
    println!("node_atoms_bridge_backfill org={org} inserted={want} batches={}", first.batches + rest.batches);
    f.done().await;
}

#[tokio::test]
async fn consumer_node_atoms_backfill_bridges_existing_output() { node_atoms_backfill_bridges_existing_output(BridgeShape::Consumer).await; }
#[tokio::test]
async fn tenant_owner_node_atoms_backfill_bridges_existing_output() { node_atoms_backfill_bridges_existing_output(BridgeShape::TenantOwner).await; }

// ── Extraction v2 (source-agnostic pipeline plan PR 2) ──────────────────────
//
// v2 is a new profile on the same standing path: adoption repins blocked v1
// captures to `source-atoms-v2`, one funded call returns atoms + relations +
// temporal validity, and Jev adjudicates mentions the ladder cannot bind as a
// second managed logical call on the same window and unit.

impl Fixture {
    fn use_profile(&self, profile: &str) {
        std::env::set_var("SOURCE_EXTRACTION_PROFILE", profile);
    }
    async fn seed_reef(&self, title: &str, entity_type: &str) -> Uuid {
        sqlx::query_scalar("INSERT INTO nodes(graph_id,type,title,body) VALUES($1,'entity',$2,jsonb_build_object('entity_type',$3::text)) RETURNING node_id")
            .bind(self.graph).bind(title).bind(entity_type).fetch_one(self.owner()).await.unwrap()
    }
    /// A reef with no recorded entity type: never bound by exact title alone.
    async fn seed_untyped_reef(&self, title: &str) -> Uuid {
        sqlx::query_scalar("INSERT INTO nodes(graph_id,type,title,body) VALUES($1,'entity',$2,'{}'::jsonb) RETURNING node_id")
            .bind(self.graph).bind(title).fetch_one(self.owner()).await.unwrap()
    }
    async fn tick_until_idle(&self, llm: &coralswarm_backend::llm::openai::OpenAiProvider) {
        for _ in 0..6 {
            source_atoms::tick(&self.worker, &self.control.app, llm, None).await.unwrap();
        }
    }
}

const V2_TEXT: &str = "Dana Kim: I will own the Linear migration until 2026-12-31.\nAna Ruiz manages Bo Chen at Initech.";

fn v2_rel(s: &str, p: &str, o: &str, until: Option<&str>, quote: Option<&str>) -> Value {
    json!({"subject":s,"predicate":p,"object":o,"valid_from":null,"valid_until":until,"validity_quote":quote})
}

/// The model's v2 answer for `V2_TEXT`: two valid relations per atom plus
/// three the validator must drop (non-extractable `expert_in`, retired
/// `related_to`, and an endpoint that is not a mention of the window).
fn v2_atoms() -> Value {
    json!({"atoms":[
        {"kind":"decision","text":"Dana Kim will own the Linear migration until 2026-12-31.",
         "evidence_quote":"I will own the Linear migration until 2026-12-31.",
         "entities":[{"mention":"Linear","type":"org"}],
         "relations":[v2_rel("Dana Kim","responsible_for","Linear",Some("2026-12-31"),Some("until 2026-12-31")),
                      v2_rel("Dana Kim","expert_in","Linear",None,None),
                      v2_rel("Dana Kim","works_on","Ghost",None,None)],
         "valid_from":null,"valid_until":"2026-12-31","validity_quote":"until 2026-12-31"},
        {"kind":"fact","text":"Ana Ruiz manages Bo Chen at Initech.",
         "evidence_quote":"Ana Ruiz manages Bo Chen at Initech.",
         "entities":[{"mention":"Ana Ruiz","type":"person"},{"mention":"Bo Chen","type":"person"},{"mention":"Initech","type":"org"}],
         "relations":[v2_rel("Ana Ruiz","manages","Bo Chen",None,None),
                      v2_rel("Bo Chen","works_at","Initech",None,None),
                      v2_rel("Ana Ruiz","related_to","Initech",None,None)],
         "valid_from":null,"valid_until":null,"validity_quote":null}
    ]})
}

#[derive(Clone)]
enum JevReply {
    /// Bind these mentions to their first candidate; answer `new` otherwise.
    Bind(Vec<&'static str>),
    /// HTTP 500 on every Jev request.
    Unavailable,
    /// A reply that fails Jev's strict validation.
    Garbage,
}

/// Loopback OpenRouter that serves the extraction tool and Jev's tool.
async fn v2_provider(atoms: Value, jev: JevReply) -> (
    coralswarm_backend::llm::openai::OpenAiProvider,
    Arc<AtomicUsize>,
    Arc<AtomicUsize>,
    tokio::task::JoinHandle<()>,
) {
    structured_provider("emit_source_atoms_v2", atoms, jev).await
}

/// [`v2_provider`] for a profile's extraction tool (`emit_source_atoms_v2`
/// or `emit_source_atoms_v3`).
async fn structured_provider(expected_tool: &'static str, atoms: Value, jev: JevReply) -> (
    coralswarm_backend::llm::openai::OpenAiProvider,
    Arc<AtomicUsize>,
    Arc<AtomicUsize>,
    tokio::task::JoinHandle<()>,
) {
    use axum::response::IntoResponse;
    let (extractions, jevs) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
    let (ex, jv) = (extractions.clone(), jevs.clone());
    let app = axum::Router::new().fallback(move |axum::Json(body): axum::Json<Value>| {
        let (ex, jv, atoms, jev) = (ex.clone(), jv.clone(), atoms.clone(), jev.clone());
        async move {
            assert_eq!(body["model"], PRIMARY, "neither call changes the model");
            let tool = body["tools"][0]["function"]["name"].as_str().unwrap_or_default().to_owned();
            let content = if tool == "jev_answers" {
                jv.fetch_add(1, Ordering::SeqCst);
                match jev {
                    JevReply::Unavailable => {
                        return (axum::http::StatusCode::INTERNAL_SERVER_ERROR, axum::Json(json!({"error":{"message":"loopback down"}}))).into_response();
                    }
                    JevReply::Garbage => json!({"q0":{"choice":"c9","confidence":2}}).to_string(),
                    JevReply::Bind(names) => {
                        let user = body["messages"].as_array().unwrap().iter().find(|m| m["role"] == "user").unwrap();
                        let state: Value = serde_json::from_str(user["content"].as_str().unwrap()).unwrap();
                        let mut answers = serde_json::Map::new();
                        for (i, (_, mention)) in state["state"]["mentions"].as_object().unwrap().iter().enumerate() {
                            let bind = names.contains(&mention["mention"].as_str().unwrap());
                            answers.insert(format!("q{i}"), json!({"choice": if bind {"c0"} else {"new"}, "confidence": 0.9}));
                        }
                        Value::Object(answers).to_string()
                    }
                }
            } else {
                assert_eq!(tool, expected_tool, "a pinned window uses its profile's tool");
                ex.fetch_add(1, Ordering::SeqCst);
                atoms.to_string()
            };
            axum::Json(json!({"id":"gen-v2","model":PRIMARY,"provider":"Loopback","choices":[{"message":{"content":content},"finish_reason":"stop"}],"usage":{"prompt_tokens":20,"completion_tokens":10,"cost":0.0004}})).into_response()
        }
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let handle = tokio::spawn(async move { axum::serve(listener, app).await.unwrap(); });
    (coralswarm_backend::llm::openai::OpenAiProvider::openrouter(reqwest::Client::new(), "fixture-only").with_base_url(base), extractions, jevs, handle)
}

async fn mention_hows(f: &Fixture, version: Uuid) -> std::collections::BTreeMap<String, (String, Option<Uuid>)> {
    let rows: Vec<(Value,)> = sqlx::query_as("SELECT entities FROM semantic_atoms WHERE source_version_id=$1 AND extraction_version='source-atoms-v2' ORDER BY ordinal")
        .bind(version).fetch_all(f.owner()).await.unwrap();
    let mut out = std::collections::BTreeMap::new();
    for (entities,) in rows {
        for e in entities.as_array().unwrap() {
            let mention = e["mention"].as_str().unwrap().to_owned();
            let node: Option<Uuid> = sqlx::query_scalar("SELECT l.node_id FROM node_semantic_atoms l JOIN semantic_atoms a USING(semantic_atom_id) JOIN nodes n ON n.node_id=l.node_id WHERE a.source_version_id=$1 AND a.extraction_version='source-atoms-v2' AND lower(n.title)=lower($2) LIMIT 1")
                .bind(version).bind(&mention).fetch_optional(f.owner()).await.unwrap();
            out.insert(mention, (e["how"].as_str().unwrap_or_default().to_owned(), node));
        }
    }
    out
}

enum V2Shape { Consumer, Tenant, TenantOwner }

async fn v2_fixture(shape: &V2Shape) -> Fixture {
    let f = match shape {
        V2Shape::Consumer => Fixture::new(false).await,
        V2Shape::Tenant => Fixture::new(true).await,
        V2Shape::TenantOwner => Fixture::create(true, false, true).await,
    };
    f.use_profile("source-atoms-v2");
    f
}

async fn v2_writes_relations_temporal_and_jev_bindings(shape: V2Shape) {
    let f = v2_fixture(&shape).await;
    let linear = f.seed_untyped_reef("Linear").await;
    let seeded_ana = f.seed_untyped_reef("Ana Ruiz").await;
    let version = f.capture_with("slack", "fixture:standing", "v2-e2e", V2_TEXT, json!({"author_name":"Dana Kim"}), Some(f.user.user_id)).await;
    let chunks = attach_chunks(&f, version, "v2-e2e", 2, &[]).await;
    let pinned: (String, String) = sqlx::query_as("SELECT extraction_version,extraction_status FROM source_versions WHERE version_id=$1")
        .bind(version).fetch_one(f.owner()).await.unwrap();
    assert_eq!(pinned, ("source-atoms-v2".into(), "blocked".into()), "capture pins the active profile, never v1");
    f.sweep().await;
    let adopted: (String, String, i64) = sqlx::query_as("SELECT v.extraction_version,(SELECT a.extraction_version FROM corpus_capture_admissions a WHERE a.source_version_id=v.version_id),(SELECT count(*) FROM corpus_enrichment_units u WHERE u.holding_id=v.version_id AND u.execution_owner AND u.extraction_version='source-atoms-v2') FROM source_versions v WHERE v.version_id=$1")
        .bind(version).fetch_one(f.owner()).await.unwrap();
    assert_eq!(adopted, ("source-atoms-v2".into(), "source-atoms-v2".into(), 1), "the version, its receipt and its owner unit move to v2 together");

    let (llm, extractions, jevs, server) = v2_provider(v2_atoms(), JevReply::Bind(vec!["Linear"])).await;
    f.tick_until_idle(&llm).await;
    let status: (String, String) = sqlx::query_as("SELECT v.extraction_status,(SELECT state FROM corpus_capture_admissions WHERE source_version_id=v.version_id) FROM source_versions v WHERE v.version_id=$1")
        .bind(version).fetch_one(f.owner()).await.unwrap();
    assert_eq!(status, ("done".into(), "processed".into()));
    assert_eq!((extractions.load(Ordering::SeqCst), jevs.load(Ordering::SeqCst)), (1, 1), "one funded extraction call and one Jev request for the window");

    // Entities and Jev's identity decisions.
    let hows = mention_hows(&f, version).await;
    assert_eq!(hows["Linear"], ("jev".into(), Some(linear)), "Jev bound the cross-context exact match the ladder refuses");
    assert_eq!(hows["Ana Ruiz"].0, "jev_new", "Jev said `new`, so a distinct reef is minted");
    assert_ne!(hows["Ana Ruiz"].1, Some(seeded_ana));
    assert_eq!(hows["Dana Kim"].0, "minted", "the author becomes a person mention for first-person edges");
    let linear_reefs: i64 = sqlx::query_scalar("SELECT count(*) FROM nodes WHERE graph_id=$1 AND lower(title)='linear'")
        .bind(f.graph).fetch_one(f.owner()).await.unwrap();
    assert_eq!(linear_reefs, 1, "no duplicate reef for the adjudicated mention");

    // Temporal validity on the atom.
    let atom_validity: (Option<chrono::DateTime<chrono::Utc>>, Option<chrono::DateTime<chrono::Utc>>, Option<String>) = sqlx::query_as("SELECT valid_from,valid_until,validity_quote FROM semantic_atoms WHERE source_version_id=$1 AND kind='decision'")
        .bind(version).fetch_one(f.owner()).await.unwrap();
    assert_eq!((atom_validity.0, atom_validity.1.map(|d| d.to_rfc3339()), atom_validity.2.as_deref()),
        (None, Some("2026-12-31T00:00:00+00:00".into()), Some("until 2026-12-31")));

    // Edges: closed vocabulary only, bound endpoints only, validity on the edge.
    let edges: Vec<(String, String, String, Option<chrono::DateTime<chrono::Utc>>, Value, String, String)> = sqlx::query_as(
        "SELECT e.predicate,s.title,d.title,e.valid_until,e.provenance,e.origin,e.status FROM relationship_edge e JOIN nodes s ON s.node_id=e.src_node_id JOIN nodes d ON d.node_id=e.dst_node_id WHERE e.graph_id=$1 ORDER BY 1,2")
        .bind(f.graph).fetch_all(f.owner()).await.unwrap();
    let shape: Vec<(&str, &str, &str)> = edges.iter().map(|e| (e.0.as_str(), e.1.as_str(), e.2.as_str())).collect();
    assert_eq!(shape, vec![
        ("manages", "Ana Ruiz", "Bo Chen"),
        ("reports_to", "Bo Chen", "Ana Ruiz"),
        ("responsible_for", "Dana Kim", "Linear"),
        ("works_at", "Bo Chen", "Initech"),
    ], "expert_in/related_to/unbound endpoints dropped; inverses follow the legacy rule");
    let chunk_ids: Vec<String> = chunks.iter().map(Uuid::to_string).collect();
    for (predicate, _, _, valid_until, provenance, origin, status) in &edges {
        assert_eq!((origin.as_str(), status.as_str()), ("stated", "confirmed"));
        assert_eq!(provenance["extraction_version"], "source-atoms-v2");
        assert_eq!(provenance["source_version_id"], version.to_string());
        assert_eq!(provenance["semantic_atom_ids"].as_array().unwrap().len(), 1, "{predicate}: semantic provenance");
        assert_eq!(provenance["atom_ids"], json!(chunk_ids), "{predicate}: chunk supports for the legacy edge readers");
        assert_eq!(valid_until.map(|d| d.to_rfc3339()), (predicate == "responsible_for").then(|| "2026-12-31T00:00:00+00:00".to_string()), "{predicate}");
    }

    // Metering: both calls reserved and settled on the window's unit under
    // source_extraction, as two logical calls.
    let receipts: (i64, i64, i64, bool, i64) = sqlx::query_as("SELECT count(*),count(DISTINCT logical_call),count(DISTINCT unit_id),bool_and(purpose='source_extraction' AND state='billed'),count(*) FILTER (WHERE state='reserved') FROM managed_attempt_receipts WHERE source_version_id=$1")
        .bind(version).fetch_one(f.owner()).await.unwrap();
    assert_eq!(receipts, (2, 2, 1, true, 0));
    let jev_usage: i64 = sqlx::query_scalar("SELECT count(*) FROM llm_usage WHERE task='jev_extraction_binding'")
        .fetch_one(f.owner()).await.unwrap();
    assert_eq!(jev_usage, 1, "Jev's call is visible in llm_usage under its own task");
    let unit_charge: i64 = sqlx::query_scalar("SELECT charged_micro_usd FROM corpus_enrichment_units WHERE holding_id=$1 AND execution_owner")
        .bind(version).fetch_one(f.owner()).await.unwrap();
    let settled: i64 = sqlx::query_scalar("SELECT sum(settled_micro_usd)::bigint FROM managed_attempt_receipts WHERE source_version_id=$1")
        .bind(version).fetch_one(f.owner()).await.unwrap();
    assert_eq!((unit_charge, settled), (800, 800), "Jev's settled cost counts against the version's cap with the extraction's");

    // The node_atoms bridge covers every v2-linked reef.
    let bridged = assert_bridged(&f, "v2").await;
    assert_eq!(bridged, 5 * 2, "five reefs (Linear, Dana Kim, Ana Ruiz, Bo Chen, Initech) x two chunks");
    server.abort();
    f.done().await;
}
#[tokio::test]
async fn consumer_v2_writes_relations_temporal_and_jev_bindings() { v2_writes_relations_temporal_and_jev_bindings(V2Shape::Consumer).await; }
#[tokio::test]
async fn tenant_v2_writes_relations_temporal_and_jev_bindings() { v2_writes_relations_temporal_and_jev_bindings(V2Shape::Tenant).await; }
#[tokio::test]
async fn tenant_owner_v2_writes_relations_temporal_and_jev_bindings() { v2_writes_relations_temporal_and_jev_bindings(V2Shape::TenantOwner).await; }

async fn v2_jev_unavailable_or_refused_falls_back_to_ladder(shape: V2Shape) {
    for mode in ["unavailable", "garbage", "off"] {
        let f = v2_fixture(&shape).await;
        f.seed_untyped_reef("Linear").await;
        let seeded_ana = f.seed_reef("Ana Ruiz", "person").await;
        if mode == "off" {
            std::env::set_var("JEV_EXTRACTION_BINDING_MODE", "off");
        }
        let version = f.capture_with("slack", "fixture:standing", "v2-fallback", V2_TEXT, json!({"author_name":"Dana Kim"}), Some(f.user.user_id)).await;
        f.sweep().await;
        let reply = if mode == "unavailable" { JevReply::Unavailable } else { JevReply::Garbage };
        let (llm, extractions, jevs, server) = v2_provider(v2_atoms(), reply).await;
        f.tick_until_idle(&llm).await;
        let status: String = sqlx::query_scalar("SELECT extraction_status FROM source_versions WHERE version_id=$1")
            .bind(version).fetch_one(f.owner()).await.unwrap();
        assert_eq!(status, "done", "{mode}: Jev failing never fails the window");
        assert_eq!(extractions.load(Ordering::SeqCst), 1, "{mode}");
        assert_eq!(jevs.load(Ordering::SeqCst) > 0, mode != "off", "{mode}: the seat is called unless switched off");
        let hows = mention_hows(&f, version).await;
        assert_eq!(hows["Linear"].0, "minted", "{mode}: an untyped title match without Jev mints");
        assert_eq!(hows["Ana Ruiz"], ("exact_title".into(), Some(seeded_ana)), "{mode}: an exact title of the same type binds without Jev");
        let receipts: (i64, i64) = sqlx::query_as("SELECT count(*) FILTER (WHERE state='billed'),count(*) FILTER (WHERE state='reserved') FROM managed_attempt_receipts WHERE source_version_id=$1")
            .bind(version).fetch_one(f.owner()).await.unwrap();
        assert_eq!(receipts.1, 0, "{mode}: every Jev attempt is settled, none left reserved");
        assert!(receipts.0 >= 1, "{mode}: the extraction is billed");
        let edges: i64 = sqlx::query_scalar("SELECT count(*) FROM relationship_edge e JOIN semantic_atoms a ON a.semantic_atom_id::text=e.provenance->'semantic_atom_ids'->>0 WHERE a.source_version_id=$1")
            .bind(version).fetch_one(f.owner()).await.unwrap();
        assert!(edges >= 1, "{mode}: relations still bind and write through the fallback");
        server.abort();
        std::env::remove_var("JEV_EXTRACTION_BINDING_MODE");
        f.done().await;
    }
}
#[tokio::test]
async fn consumer_v2_jev_unavailable_or_refused_falls_back_to_ladder() { v2_jev_unavailable_or_refused_falls_back_to_ladder(V2Shape::Consumer).await; }
#[tokio::test]
async fn tenant_v2_jev_unavailable_or_refused_falls_back_to_ladder() { v2_jev_unavailable_or_refused_falls_back_to_ladder(V2Shape::Tenant).await; }

/// An exact title of a known, equal type binds a visible existing reef,
/// single-token names included, and picks the oldest copy, even when ER
/// was unsure the copies are one. A type conflict, another member's private
/// reef, copies judged different, or copies with conflicting emails never
/// bind deterministically, and Jev is never offered the private reef. A
/// later mention never retypes the bound reef.
async fn v2_exact_title_binds_existing_reef(shape: V2Shape) {
    let f = v2_fixture(&shape).await;
    let reef = |title: &'static str, entity_type: &'static str, owner: Option<Uuid>, age_days: i32| {
        let f = &f;
        async move {
            sqlx::query_scalar::<_, Uuid>("INSERT INTO nodes(graph_id,type,title,body,identity_scope,owner_id,created_at) VALUES($1,'entity',$2,jsonb_build_object('entity_type',$3::text,'identity_status','unresolved'),gen_random_uuid(),$4,now()-make_interval(days=>$5)) RETURNING node_id")
                .bind(f.graph).bind(title).bind(entity_type).bind(owner).bind(age_days).fetch_one(f.owner()).await.unwrap()
        }
    };
    let keeper = reef("Linear", "org", None, 9).await;
    let copy = reef("linear", "org", None, 1).await;
    let (lo, hi) = if keeper < copy { (keeper, copy) } else { (copy, keeper) };
    sqlx::query("INSERT INTO reef_judgement(graph_id,node_a_id,node_b_id,verdict,source,evidence) \
                 VALUES($1,$2,$3,'negative','llm','{\"llm_verdict\":\"same\",\"recorded_as\":\"unsure\"}')")
        .bind(f.graph).bind(lo).bind(hi).execute(f.owner()).await.unwrap();
    let wrong_type = reef("Initech", "person", None, 5).await;
    let mut initech = Vec::new();
    for (age, email) in [(6, "ops@initech.test"), (3, "ops@initech-labs.test")] {
        let node = reef("Initech", "org", None, age).await;
        sqlx::query("INSERT INTO reef_identifiers(graph_id,node_id,kind,value_norm,source) VALUES($1,$2,'email',$3,'test')")
            .bind(f.graph).bind(node).bind(email).execute(f.owner()).await.unwrap();
        initech.push(node);
    }
    let other_member = f.member("member").await;
    let private = reef("Bo Chen", "person", Some(other_member), 5).await;
    let ana_a = reef("Ana Ruiz", "person", None, 5).await;
    let ana_b = reef("Ana Ruiz", "person", None, 4).await;
    let (lo, hi) = if ana_a < ana_b { (ana_a, ana_b) } else { (ana_b, ana_a) };
    sqlx::query("INSERT INTO reef_judgement(graph_id,node_a_id,node_b_id,verdict,source) VALUES($1,$2,$3,'negative','human')")
        .bind(f.graph).bind(lo).bind(hi).execute(f.owner()).await.unwrap();

    let version = f.capture_with("slack", "fixture:standing", "v2-exact", V2_TEXT, json!({"author_name":"Dana Kim"}), Some(f.user.user_id)).await;
    f.sweep().await;
    let (llm, _, _, server) = v2_provider(v2_atoms(), JevReply::Bind(vec!["Bo Chen"])).await;
    f.tick_until_idle(&llm).await;
    let hows = mention_hows(&f, version).await;
    println!("v2_exact_title hows={hows:?}");
    // #1516: a non-person exact title binds in the door's ladder (step 3a),
    // before the binder's own exact-title rule is reached.
    assert_eq!(hows["Linear"], ("ladder".into(), Some(keeper)), "a single-token exact org title binds the oldest same-kind copy, past an unsure verdict");
    assert_ne!(hows["Initech"].0, "exact_title", "copies with conflicting emails are two orgs: no deterministic pick");
    assert!(![Some(wrong_type), Some(initech[0]), Some(initech[1])].contains(&hows["Initech"].1), "{:?}", hows["Initech"]);
    assert_eq!(hows["Bo Chen"].0, "minted", "another member's private reef is neither bound nor offered to Jev");
    assert_ne!(hows["Bo Chen"].1, Some(private));
    assert_eq!(hows["Ana Ruiz"].0, "jev_new", "copies judged different are Jev's call, not a deterministic pick");
    let linear_reefs: i64 = sqlx::query_scalar("SELECT count(*) FROM nodes WHERE graph_id=$1 AND lower(title)='linear'")
        .bind(f.graph).fetch_one(f.owner()).await.unwrap();
    assert_eq!(linear_reefs, 2, "no new Linear reef is minted");
    let store = PostgresStore::new(f.owner().clone());
    coralswarm_backend::store::Store::record_entity_type_hint(&store, keeper, "topic").await.unwrap();
    let kind: Option<String> = sqlx::query_scalar("SELECT entity_type FROM nodes WHERE node_id=$1").bind(keeper).fetch_one(f.owner()).await.unwrap();
    assert_eq!(kind.as_deref(), Some("org"), "a later topic mention does not retype the org reef");
    server.abort();
    f.done().await;
}
#[tokio::test]
async fn consumer_v2_exact_title_binds_existing_reef() { v2_exact_title_binds_existing_reef(V2Shape::Consumer).await; }
#[tokio::test]
async fn tenant_v2_exact_title_binds_existing_reef() { v2_exact_title_binds_existing_reef(V2Shape::Tenant).await; }
#[tokio::test]
async fn tenant_owner_v2_exact_title_binds_existing_reef() { v2_exact_title_binds_existing_reef(V2Shape::TenantOwner).await; }

// ── Extraction v3: stated changes, re-extraction of v2 versions ─────────────

const V3_TEXT: &str = "Atlas moved from Bo Chen to Ana Ruiz.\nCy Lo works at Initech.";

fn v3_rel(s: &str, p: &str, o: &str) -> Value {
    json!({"subject":s,"predicate":p,"object":o,"valid_from":null,"valid_until":null,"validity_quote":null})
}

/// What v2 made of `V3_TEXT`: the new owner as a plain relation, no change.
fn v2_answer_for_v3_text() -> Value {
    json!({"atoms":[
        {"kind":"fact","text":"Atlas moved from Bo Chen to Ana Ruiz.","evidence_quote":"Atlas moved from Bo Chen to Ana Ruiz.",
         "entities":[{"mention":"Atlas","type":"project"},{"mention":"Bo Chen","type":"person"},{"mention":"Ana Ruiz","type":"person"}],
         "relations":[v3_rel("Ana Ruiz","responsible_for","Atlas")],"valid_from":null,"valid_until":null,"validity_quote":null}
    ]})
}

/// v3's answer: the same sentence as one change, and the employer relation.
fn v3_answer() -> Value {
    json!({"atoms":[
        {"kind":"fact","text":"Atlas moved from Bo Chen to Ana Ruiz.","evidence_quote":"Atlas moved from Bo Chen to Ana Ruiz.",
         "entities":[{"mention":"Atlas","type":"project"},{"mention":"Bo Chen","type":"person"},{"mention":"Ana Ruiz","type":"person"}],
         "relations":[],"changes":[{"subject":"Atlas","attribute":"owner","old_value":"Bo Chen","new_value":"Ana Ruiz",
            "effective_from":null,"effective_quote":null,"quote":"Atlas moved from Bo Chen to Ana Ruiz."}],
         "valid_from":null,"valid_until":null,"validity_quote":null},
        {"kind":"fact","text":"Cy Lo works at Initech.","evidence_quote":"Cy Lo works at Initech.",
         "entities":[{"mention":"Cy Lo","type":"person"},{"mention":"Initech","type":"org"}],
         "relations":[v3_rel("Cy Lo","works_at","Initech")],"changes":[],
         "valid_from":null,"valid_until":null,"validity_quote":null}
    ]})
}

/// A v2-extracted version is requested under v3 (bounded, idempotent, no
/// model call), adopted only once v3 is the active profile, extracted with
/// one funded call that is billed once, and its v2 atoms retire on
/// completion. The one-sentence change becomes a supersession proposal and
/// the employer relation is keyed. Nothing runs or bills twice.
async fn v3_reextraction_of_v2_versions_is_idempotent_and_billed_once(shape: V2Shape) {
    let f = v2_fixture(&shape).await;
    let version = f.capture_with("slack", "fixture:standing", "v3-reextract", V3_TEXT, json!({"author_name":"Dana Kim"}), Some(f.user.user_id)).await;
    f.sweep().await;
    let (v2_llm, v2_calls, _, v2_server) = v2_provider(v2_answer_for_v3_text(), JevReply::Bind(vec![])).await;
    f.tick_until_idle(&v2_llm).await;
    v2_server.abort();
    assert_eq!(v2_calls.load(Ordering::SeqCst), 1);
    let pinned: (String, String) = sqlx::query_as("SELECT extraction_version,extraction_status FROM source_versions WHERE version_id=$1")
        .bind(version).fetch_one(f.owner()).await.unwrap();
    assert_eq!(pinned, ("source-atoms-v2".into(), "done".into()));
    let reefs_before: i64 = sqlx::query_scalar("SELECT count(*) FROM nodes WHERE graph_id=$1").bind(f.graph).fetch_one(f.owner()).await.unwrap();
    // The capture predates the standing floor (the existing v2 corpus): only
    // the re-extraction request makes it eligible again.
    sqlx::query("UPDATE source_versions SET captured_at='2026-10-12T12:00:00Z' WHERE version_id=$1").bind(version).execute(f.owner()).await.unwrap();

    // The request: a dry preview, one repin, and a second request that finds nothing.
    use coralswarm_backend::source_reextract as reextract;
    let preview = reextract::preview(f.owner(), Some(f.graph), false, 500).await.unwrap();
    assert_eq!((preview.versions, preview.windows, preview.prior_atoms), (1, 1, 1));
    assert_eq!(reextract::execute(f.owner(), Some(f.graph), false, 500).await.unwrap(), (1, 0));
    assert_eq!(reextract::execute(f.owner(), Some(f.graph), false, 500).await.unwrap(), (0, 0), "a second request finds nothing");
    let requested: (String, String, Option<String>, i64, String, Option<Uuid>) = sqlx::query_as("SELECT v.extraction_version,v.extraction_status,v.extraction_error,v.next_start_char,a.extraction_version,a.run_id FROM source_versions v JOIN corpus_capture_admissions a ON a.source_version_id=v.version_id WHERE v.version_id=$1")
        .bind(version).fetch_one(f.owner()).await.unwrap();
    assert_eq!(requested, ("source-atoms-v3".into(), "blocked".into(), Some("reextraction_requested".into()), 0, "source-atoms-v3".into(), None),
        "the version and its receipt move to v3 together");
    // v2 is still the active profile: the request waits.
    f.sweep().await;
    let v3_owners: i64 = sqlx::query_scalar("SELECT count(*) FROM corpus_enrichment_units WHERE extraction_version='source-atoms-v3'").fetch_one(f.owner()).await.unwrap();
    assert_eq!(v3_owners, 0);

    f.use_profile("source-atoms-v3");
    f.sweep().await;
    let (llm, extractions, jevs, server) = structured_provider("emit_source_atoms_v3", v3_answer(), JevReply::Bind(vec![])).await;
    f.tick_until_idle(&llm).await;
    let after: (String, String, i64, i64) = sqlx::query_as("SELECT v.extraction_version,v.extraction_status,\
        (SELECT count(*) FROM semantic_atoms a WHERE a.source_version_id=v.version_id AND a.extraction_version='source-atoms-v2'),\
        (SELECT count(*) FROM semantic_atoms a WHERE a.source_version_id=v.version_id AND a.extraction_version='source-atoms-v3') \
        FROM source_versions v WHERE v.version_id=$1")
        .bind(version).fetch_one(f.owner()).await.unwrap();
    assert_eq!(after, ("source-atoms-v3".into(), "done".into(), 0, 2), "v2 atoms retire when the version completes under v3");
    assert_eq!(extractions.load(Ordering::SeqCst), 1);
    let reefs_after: i64 = sqlx::query_scalar("SELECT count(*) FROM nodes WHERE graph_id=$1").bind(f.graph).fetch_one(f.owner()).await.unwrap();
    assert_eq!(reefs_after, reefs_before + 2, "re-extraction reuses the version's reefs; only Cy Lo and Initech are new");

    // The one sentence: recorded, and proposed to supersession (dry run).
    let stated: i64 = sqlx::query_scalar("SELECT count(*) FROM semantic_atoms WHERE source_version_id=$1 AND jsonb_array_length(stated_changes)=1")
        .bind(version).fetch_one(f.owner()).await.unwrap();
    assert_eq!(stated, 1);
    let decided: Vec<(String, String, String, bool)> = sqlx::query_as(
        "SELECT state_key,rule,status,superseder_atom_id=superseded_atom_id FROM fact_supersession_edges WHERE graph_id=$1")
        .bind(f.graph).fetch_all(f.owner()).await.unwrap();
    assert_eq!(decided, vec![("rel.owner".into(), "stated_change".into(), "proposed".into(), true)]);
    // Single-valued relations, keyed for supersession.
    let keyed: Vec<(String, String)> = sqlx::query_as(
        "SELECT r.state_key,r.value FROM temporal_evidence_records r WHERE r.graph_id=$1 AND r.operation='assert' AND r.state_key IN ('rel.employer','rel.owner') ORDER BY 1,2")
        .bind(f.graph).fetch_all(f.owner()).await.unwrap();
    assert_eq!(keyed, vec![("rel.employer".into(), "Initech".into()), ("rel.owner".into(), "Ana Ruiz".into())]);
    let owner_edge: Value = sqlx::query_scalar("SELECT provenance FROM relationship_edge e JOIN nodes s ON s.node_id=e.src_node_id WHERE e.graph_id=$1 AND e.predicate='responsible_for' AND s.title='Ana Ruiz'")
        .bind(f.graph).fetch_one(f.owner()).await.unwrap();
    assert_eq!(owner_edge["extraction_version"], "source-atoms-v3", "the edge v2 stated is restated by v3");

    // Billed once: every receipt on the v3 unit is billed, one per logical call.
    let receipts: (i64, i64, i64) = sqlx::query_as("SELECT count(*),count(*) FILTER (WHERE r.state='billed'),count(DISTINCT r.logical_call) FROM managed_attempt_receipts r JOIN corpus_enrichment_units u ON u.unit_id=r.unit_id WHERE r.source_version_id=$1 AND u.extraction_version='source-atoms-v3'")
        .bind(version).fetch_one(f.owner()).await.unwrap();
    let calls = (extractions.load(Ordering::SeqCst) + jevs.load(Ordering::SeqCst)) as i64;
    assert_eq!(receipts, (calls, calls, calls), "one billed receipt per call");
    let usage: i64 = sqlx::query_scalar("SELECT count(*) FROM llm_usage WHERE task='source_atoms_v3'").fetch_one(f.owner()).await.unwrap();
    assert_eq!(usage, 1);

    // Idempotent: nothing is requested, adopted, extracted or billed again.
    f.sweep().await;
    f.tick_until_idle(&llm).await;
    assert_eq!(extractions.load(Ordering::SeqCst), 1);
    assert_eq!(reextract::preview(f.owner(), Some(f.graph), true, 500).await.unwrap().versions, 0);
    assert_eq!(reextract::execute(f.owner(), Some(f.graph), true, 500).await.unwrap(), (0, 0));
    let again: i64 = sqlx::query_scalar("SELECT count(*) FROM managed_attempt_receipts r JOIN corpus_enrichment_units u ON u.unit_id=r.unit_id WHERE r.source_version_id=$1 AND u.extraction_version='source-atoms-v3'")
        .bind(version).fetch_one(f.owner()).await.unwrap();
    assert_eq!(again, calls);
    server.abort();
    f.done().await;
}
/// Switching the active profile to v3 takes only what standing would take
/// anyway: new captures, and versions still blocked under an earlier pin
/// (repinned forward). A version already DONE under v2 is never touched: its
/// pin, status, atoms and receipts stay as they were, and no v3 owner or
/// model call is made for it. Re-extracting a done version is only ever the
/// explicit `reextract_source_atoms_v2 --to v3` request.
async fn v3_active_profile_never_takes_a_done_v2_version(shape: V2Shape) {
    let f = v2_fixture(&shape).await;
    let done = f.capture_with("slack", "fixture:standing", "v3-guard-done", V3_TEXT, json!({"author_name":"Dana Kim"}), Some(f.user.user_id)).await;
    f.sweep().await;
    let (v2_llm, v2_calls, _, v2_server) = v2_provider(v2_answer_for_v3_text(), JevReply::Bind(vec![])).await;
    f.tick_until_idle(&v2_llm).await;
    v2_server.abort();
    assert_eq!(v2_calls.load(Ordering::SeqCst), 1);
    let snapshot = || async {
        sqlx::query_as::<_, (String, String, Vec<Uuid>, i64)>(
            "SELECT v.extraction_version,v.extraction_status, \
                    ARRAY(SELECT a.semantic_atom_id FROM semantic_atoms a WHERE a.source_version_id=v.version_id ORDER BY 1), \
                    (SELECT count(*) FROM managed_attempt_receipts r WHERE r.source_version_id=v.version_id) \
               FROM source_versions v WHERE v.version_id=$1")
            .bind(done).fetch_one(f.owner()).await.unwrap()
    };
    let before = snapshot().await;
    assert_eq!((before.0.as_str(), before.1.as_str(), before.2.len()), ("source-atoms-v2", "done", 1));

    // A second capture pinned to v2 but never extracted (held for admission).
    let held = f.capture_with("slack", "fixture:standing", "v3-guard-held", "Cy Lo works at Initech.", json!({"author_name":"Dana Kim"}), Some(f.user.user_id)).await;
    let mut tx = f.owner().begin().await.unwrap();
    sqlx::query("UPDATE source_versions SET extraction_version='source-atoms-v2' WHERE version_id=$1").bind(held).execute(&mut *tx).await.unwrap();
    sqlx::query("SELECT corpus_capture_admission_follow_pin($1,$2)").bind(f.graph).bind(held).execute(&mut *tx).await.unwrap();
    tx.commit().await.unwrap();
    let pinned: (String, String) = sqlx::query_as("SELECT extraction_version,extraction_status FROM source_versions WHERE version_id=$1")
        .bind(held).fetch_one(f.owner()).await.unwrap();
    assert_eq!(pinned, ("source-atoms-v2".into(), "blocked".into()));

    f.use_profile("source-atoms-v3");
    let answer = json!({"atoms":[{"kind":"fact","text":"Cy Lo works at Initech.","evidence_quote":"Cy Lo works at Initech.",
        "entities":[{"mention":"Cy Lo","type":"person"},{"mention":"Initech","type":"org"}],
        "relations":[v3_rel("Cy Lo","works_at","Initech")],"changes":[],"valid_from":null,"valid_until":null,"validity_quote":null}]});
    let (llm, extractions, _, server) = structured_provider("emit_source_atoms_v3", answer, JevReply::Bind(vec![])).await;
    for _ in 0..2 {
        f.sweep().await;
        f.tick_until_idle(&llm).await;
    }
    assert_eq!(snapshot().await, before, "the done v2 version: same pin, status, atoms and receipts");
    let v3_owners: i64 = sqlx::query_scalar("SELECT count(*) FROM corpus_enrichment_units WHERE holding_id=$1 AND extraction_version='source-atoms-v3'")
        .bind(done).fetch_one(f.owner()).await.unwrap();
    assert_eq!(v3_owners, 0, "no v3 owner for a done version");
    let taken: (String, String) = sqlx::query_as("SELECT extraction_version,extraction_status FROM source_versions WHERE version_id=$1")
        .bind(held).fetch_one(f.owner()).await.unwrap();
    assert_eq!(taken, ("source-atoms-v3".into(), "done".into()), "a blocked, never-extracted v2 version moves forward");
    assert_eq!(extractions.load(Ordering::SeqCst), 1, "one v3 call, for the held version only");
    server.abort();
    f.done().await;
}
/// The race behind the guard: a v2 version is still blocked when the v3
/// sweep reads its candidates, and is finished (done under v2) before
/// adoption takes its locks. Adoption must not repin it: a done version is
/// never re-extracted by a profile switch.
async fn v3_adoption_never_repins_a_version_done_since_the_candidate_read(shape: V2Shape) {
    let f = v2_fixture(&shape).await;
    let racer = f.capture_with("slack", "fixture:standing", "v3-guard-race", "Cy Lo works at Initech.", json!({"author_name":"Dana Kim"}), Some(f.user.user_id)).await;
    let mut tx = f.owner().begin().await.unwrap();
    sqlx::query("UPDATE source_versions SET extraction_version='source-atoms-v2' WHERE version_id=$1").bind(racer).execute(&mut *tx).await.unwrap();
    sqlx::query("SELECT corpus_capture_admission_follow_pin($1,$2)").bind(f.graph).bind(racer).execute(&mut *tx).await.unwrap();
    tx.commit().await.unwrap();
    f.use_profile("source-atoms-v3");
    let barrier = coralswarm_backend::test_standing_probe::pause_at(racer, coralswarm_backend::test_standing_probe::Stage::BeforeAdoptionRepin);
    let owner = f.owner().clone();
    let finish = async {
        barrier.reached().await;
        // What a v2 owner finishing the version leaves behind.
        sqlx::query("UPDATE source_versions SET extraction_status='done',extraction_error=NULL WHERE version_id=$1")
            .bind(racer).execute(&owner).await.unwrap();
        barrier.release();
    };
    tokio::join!(f.sweep(), finish);
    let after: (String, String) = sqlx::query_as("SELECT extraction_version,extraction_status FROM source_versions WHERE version_id=$1")
        .bind(racer).fetch_one(f.owner()).await.unwrap();
    assert_eq!(after, ("source-atoms-v2".into(), "done".into()), "a version done since the candidate read is never repinned");
    let v3_owners: i64 = sqlx::query_scalar("SELECT count(*) FROM corpus_enrichment_units WHERE holding_id=$1 AND extraction_version='source-atoms-v3'")
        .bind(racer).fetch_one(f.owner()).await.unwrap();
    assert_eq!(v3_owners, 0);
    f.done().await;
}
#[tokio::test]
async fn consumer_v3_adoption_never_repins_a_version_done_since_the_candidate_read() { v3_adoption_never_repins_a_version_done_since_the_candidate_read(V2Shape::Consumer).await; }
#[tokio::test]
async fn tenant_v3_adoption_never_repins_a_version_done_since_the_candidate_read() { v3_adoption_never_repins_a_version_done_since_the_candidate_read(V2Shape::Tenant).await; }

#[tokio::test]
async fn consumer_v3_active_profile_never_takes_a_done_v2_version() { v3_active_profile_never_takes_a_done_v2_version(V2Shape::Consumer).await; }
#[tokio::test]
async fn tenant_v3_active_profile_never_takes_a_done_v2_version() { v3_active_profile_never_takes_a_done_v2_version(V2Shape::Tenant).await; }
#[tokio::test]
async fn tenant_owner_v3_active_profile_never_takes_a_done_v2_version() { v3_active_profile_never_takes_a_done_v2_version(V2Shape::TenantOwner).await; }

#[tokio::test]
async fn consumer_v3_reextraction_of_v2_versions_is_idempotent_and_billed_once() { v3_reextraction_of_v2_versions_is_idempotent_and_billed_once(V2Shape::Consumer).await; }
#[tokio::test]
async fn tenant_v3_reextraction_of_v2_versions_is_idempotent_and_billed_once() { v3_reextraction_of_v2_versions_is_idempotent_and_billed_once(V2Shape::Tenant).await; }
#[tokio::test]
async fn tenant_owner_v3_reextraction_of_v2_versions_is_idempotent_and_billed_once() { v3_reextraction_of_v2_versions_is_idempotent_and_billed_once(V2Shape::TenantOwner).await; }

/// No worker extracts `source-atoms-v1` anymore. A v1-extracted version is
/// served from its v1 atoms until a request moves it to v3; then readers
/// serve only its v3 atoms, even though a cited v1 atom is kept.
async fn v1_versions_serve_v1_atoms_until_v3_replaces_them(shape: V2Shape) {
    let f = v2_fixture(&shape).await;
    let (version, v1_atom, passage) = f.capture_v1("v1-history", V3_TEXT, "Atlas belongs to Ana Ruiz.").await;
    let v1_served = vec![("source-atoms-v1".to_string(), "Atlas belongs to Ana Ruiz.".to_string())];
    assert_eq!(f.served(version).await, v1_served, "nothing replaced the v1 atoms: they are served");
    // A past answer cites the v1 atom, so re-extraction keeps it.
    let item: Uuid = sqlx::query_scalar("SELECT source_item_id FROM source_versions WHERE version_id=$1").bind(version).fetch_one(f.owner()).await.unwrap();
    sqlx::query("INSERT INTO answer_support_receipts(receipt_id,graph_id,owner_user_id,support) VALUES($1,$2,$3,$4)")
        .bind(Uuid::new_v4()).bind(f.graph).bind(f.user.user_id)
        .bind(json!([{"kind":"source_passage","graph_id":f.graph,"source_item_id":item,"source_version_id":version,"semantic_atom_id":v1_atom,"passage_id":passage}]))
        .execute(f.owner()).await.unwrap();

    use coralswarm_backend::source_reextract as reextract;
    assert_eq!(reextract::preview(f.owner(), Some(f.graph), false, 500).await.unwrap(), reextract::Preview { versions: 1, windows: 1, prior_atoms: 1 });
    assert_eq!(reextract::execute(f.owner(), Some(f.graph), false, 500).await.unwrap(), (1, 0));
    assert_eq!(reextract::execute(f.owner(), Some(f.graph), false, 500).await.unwrap(), (0, 0), "a second request finds nothing");
    let requested: (String, String) = sqlx::query_as("SELECT extraction_version,extraction_status FROM source_versions WHERE version_id=$1")
        .bind(version).fetch_one(f.owner()).await.unwrap();
    assert_eq!(requested, ("source-atoms-v3".into(), "blocked".into()), "v1 moves straight to v3");
    assert_eq!(f.served(version).await, v1_served, "until v3 finishes, the v1 atoms are all the version has");

    f.use_profile("source-atoms-v3");
    f.sweep().await;
    let (llm, extractions, _, server) = structured_provider("emit_source_atoms_v3", v3_answer(), JevReply::Bind(vec![])).await;
    f.tick_until_idle(&llm).await;
    server.abort();
    assert_eq!(extractions.load(Ordering::SeqCst), 1);
    let kept: i64 = sqlx::query_scalar("SELECT count(*) FROM semantic_atoms WHERE source_version_id=$1 AND extraction_version='source-atoms-v1'")
        .bind(version).fetch_one(f.owner()).await.unwrap();
    assert_eq!(kept, 1, "the cited v1 atom stays for its receipt");
    assert_eq!(f.served(version).await, vec![
        ("source-atoms-v3".to_string(), "Atlas moved from Bo Chen to Ana Ruiz.".to_string()),
        ("source-atoms-v3".to_string(), "Cy Lo works at Initech.".to_string()),
    ], "only the v3 atoms are served once v3 is done");
    f.done().await;
}
#[tokio::test]
async fn consumer_v1_versions_serve_v1_atoms_until_v3_replaces_them() { v1_versions_serve_v1_atoms_until_v3_replaces_them(V2Shape::Consumer).await; }
#[tokio::test]
async fn tenant_v1_versions_serve_v1_atoms_until_v3_replaces_them() { v1_versions_serve_v1_atoms_until_v3_replaces_them(V2Shape::Tenant).await; }
#[tokio::test]
async fn tenant_owner_v1_versions_serve_v1_atoms_until_v3_replaces_them() { v1_versions_serve_v1_atoms_until_v3_replaces_them(V2Shape::TenantOwner).await; }

/// The 264 `capture_actor_unknown` Slack workspace captures: adopted, and
/// billed to the org account through the run's CURRENT execution principal,
/// not the principal the run was created with.
#[tokio::test]
async fn tenant_v2_actor_unknown_workspace_capture_bills_the_current_org_principal() {
    let mut f = v2_fixture(&V2Shape::Tenant).await;
    f.graph = sqlx::query_scalar("INSERT INTO graphs(account_id,name,kind) VALUES($1,'Workspace channel','org') RETURNING id")
        .bind(f.user.account_id).fetch_one(f.owner()).await.unwrap();
    let text = "Ana Ruiz manages Bo Chen at Initech.";
    let version = f.capture_with("slack", "slack:workspace", "T1:C1:1700000000.0001", text, json!({}), None).await;
    let blocked: Option<String> = sqlx::query_scalar("SELECT extraction_error FROM source_versions WHERE version_id=$1")
        .bind(version).fetch_one(f.owner()).await.unwrap();
    assert_eq!(blocked.as_deref(), Some("capture_actor_unknown_managed_admission_required"));
    f.sweep().await;
    // A second org admin becomes the run's execution principal.
    let admin = Uuid::new_v4();
    sqlx::query("INSERT INTO users(id,email,clerk_user_id) VALUES($1,$2,$2)").bind(admin).bind(format!("{admin}@fixture.local")).execute(&f.control.owner).await.unwrap();
    sqlx::query("INSERT INTO account_members(account_id,user_id,role,seat_tier_id) VALUES($1,$2,'admin',(SELECT id FROM seat_tiers WHERE code='standard' AND is_active))")
        .bind(f.user.account_id).bind(admin).execute(&f.control.owner).await.unwrap();
    let mut financial = scope::begin_account_scoped(&f.control.owner, f.user.account_id).await.unwrap();
    coralswarm_backend::billing::renewal::lock_period_rotation(&mut financial, f.user.account_id).await.unwrap();
    sqlx::query("INSERT INTO member_allowance_periods(account_id,user_id,period_start,period_end,granted_credits) VALUES($1,$2,now()-interval '1 day',now()+interval '30 days',100000)")
        .bind(f.user.account_id).bind(admin).execute(&mut *financial).await.unwrap();
    financial.commit().await.unwrap();
    let run: Uuid = sqlx::query_scalar("UPDATE corpus_enrichment_runs SET execution_principal=$2 WHERE graph_id=$1 AND run_kind='standing' AND extraction_version='source-atoms-v2' RETURNING run_id")
        .bind(f.graph).bind(admin).fetch_one(f.owner()).await.unwrap();
    let atoms = json!({"atoms":[{"kind":"fact","text":text,"evidence_quote":text,
        "entities":[{"mention":"Ana Ruiz","type":"person"},{"mention":"Bo Chen","type":"person"}],
        "relations":[v2_rel("Ana Ruiz","manages","Bo Chen",None,None)],"valid_from":null,"valid_until":null,"validity_quote":null}]});
    let (llm, extractions, _jevs, server) = v2_provider(atoms, JevReply::Bind(vec![])).await;
    f.tick_until_idle(&llm).await;
    let done: (String, Uuid, Uuid) = sqlx::query_as("SELECT v.extraction_status,(SELECT execution_principal FROM corpus_enrichment_runs WHERE run_id=$2),(SELECT attributed_user_id FROM managed_attempt_receipts WHERE source_version_id=$1 AND state='billed' LIMIT 1) FROM source_versions v WHERE v.version_id=$1")
        .bind(version).bind(run).fetch_one(f.owner()).await.unwrap();
    assert_eq!(done, ("done".into(), admin, admin), "unknown actor: the current principal pays, not the run's original requester");
    assert_eq!(extractions.load(Ordering::SeqCst), 1);
    let edges: i64 = sqlx::query_scalar("SELECT count(*) FROM relationship_edge WHERE graph_id=$1").bind(f.graph).fetch_one(f.owner()).await.unwrap();
    assert_eq!(edges, 2);
    server.abort();
    f.done().await;
}

/// Standing adoption under a backlog whose oldest versions keep failing: the
/// failures cost their own lock timeouts, never the later versions' turn.
async fn standing_backlog_adoption_does_not_starve_behind_failing_versions(org: bool) {
    let f = Fixture::new(org).await;
    let mut versions = Vec::new();
    for i in 0..30 {
        versions.push(f.capture("note", &format!("backlog-{i:02}"), "Retained evidence.").await);
    }
    let items: Vec<Uuid> = sqlx::query_scalar("SELECT source_item_id FROM source_versions WHERE version_id=ANY($1) ORDER BY captured_at,version_id LIMIT 8")
        .bind(&versions).fetch_all(f.owner()).await.unwrap();
    let writer = PgPoolOptions::new().max_connections(1).connect_with(f.worker.pool.connect_options().as_ref().clone()).await.unwrap();
    let mut blocker = match f.worker.tenant {
        TenantId::Consumer => scope::begin_account_scoped(&writer, f.user.account_id).await.unwrap(),
        TenantId::Org(_) => scope::begin_tenant_system_scoped(&writer, f.user.account_id).await.unwrap(),
    };
    let locked: Vec<Uuid> = sqlx::query_scalar("SELECT source_item_id FROM source_items WHERE source_item_id=ANY($1) FOR UPDATE")
        .bind(&items).fetch_all(&mut *blocker).await.unwrap();
    assert_eq!(locked.len(), 8, "the eight oldest captures are genuinely locked");
    let owners = || async {
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM corpus_enrichment_units WHERE execution_owner").fetch_one(f.owner()).await.unwrap()
    };
    let mut adopted = Vec::new();
    for _ in 0..2 {
        let started = std::time::Instant::now();
        let _ = coralswarm_backend::test_standing_sweep(&f.worker, &f.control.app).await;
        adopted.push((owners().await, started.elapsed()));
    }
    println!("backlog_adoption org={org} sweeps={adopted:?}");
    assert_eq!(adopted[0].0, 22, "every unlocked version is adopted on the first sweep");
    assert_eq!(adopted[1].0, 22, "a still-locked version is retried, never adopted twice");
    blocker.rollback().await.unwrap();
    writer.close().await;
    f.sweep().await;
    assert_eq!(owners().await, 30, "the cursor wraps back to the released versions");
    f.done().await;
}
#[tokio::test]
async fn consumer_standing_backlog_adoption_does_not_starve_behind_failing_versions() { standing_backlog_adoption_does_not_starve_behind_failing_versions(false).await; }
#[tokio::test]
async fn tenant_standing_backlog_adoption_does_not_starve_behind_failing_versions() { standing_backlog_adoption_does_not_starve_behind_failing_versions(true).await; }

fn control_plane(f: &Fixture) -> String {
    match &f.worker.tenant {
        TenantId::Consumer => "consumer".into(),
        TenantId::Org(org) => format!("org:{org}"),
    }
}

// ── retry_failed_extractions_v2 ─────────────────────────────────────────────
//
// Failed v2/v3 versions (windows with no valid atom) are handed back to
// standing under their own profile. A failed v1 version is not: no worker
// extracts v1. The tool never calls a model; standing extracts through the
// gate.

/// Every atom of this answer quotes text the window does not contain.
fn unanchored_atoms(v2: bool) -> Value {
    let mut atom = json!({"kind":"fact","text":"Not in the source.","evidence_quote":"absent from the window","entities":[]});
    if v2 {
        atom["relations"] = json!([]);
    }
    json!({"atoms":[atom]})
}

/// Loopback OpenRouter answering both tools with `atoms` (v1 tool: `v1`).
async fn answering(v1: Value, v2: Value) -> (coralswarm_backend::llm::openai::OpenAiProvider, Arc<AtomicUsize>, tokio::task::JoinHandle<()>) {
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = calls.clone();
    let app = axum::Router::new().fallback(move |axum::Json(body): axum::Json<Value>| {
        let (counter, v1, v2) = (counter.clone(), v1.clone(), v2.clone());
        async move {
            let tool = body["tools"][0]["function"]["name"].as_str().unwrap_or_default().to_owned();
            let content = match tool.as_str() {
                "jev_answers" => json!({}).to_string(),
                "emit_source_atoms_v2" => { counter.fetch_add(1, Ordering::SeqCst); v2.to_string() }
                _ => { counter.fetch_add(1, Ordering::SeqCst); v1.to_string() }
            };
            axum::Json(json!({"id":"gen-retry","model":PRIMARY,"provider":"Loopback","choices":[{"message":{"content":content},"finish_reason":"stop"}],"usage":{"prompt_tokens":20,"completion_tokens":10,"cost":0.0004}}))
        }
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let handle = tokio::spawn(async move { axum::serve(listener, app).await.unwrap(); });
    (coralswarm_backend::llm::openai::OpenAiProvider::openrouter(reqwest::Client::new(), "fixture-only").with_base_url(base), calls, handle)
}

async fn status_of(f: &Fixture, version: Uuid) -> (String, String, Option<String>) {
    sqlx::query_as("SELECT extraction_version,extraction_status,extraction_error FROM source_versions WHERE version_id=$1")
        .bind(version).fetch_one(f.owner()).await.unwrap()
}

async fn billed(f: &Fixture, version: Uuid) -> (i64, i64, i64) {
    sqlx::query_as("SELECT count(*) FILTER (WHERE state='billed'),count(*) FILTER (WHERE state='reserved'),COALESCE(sum(settled_micro_usd),0)::bigint FROM managed_attempt_receipts WHERE source_version_id=$1")
        .bind(version).fetch_one(f.owner()).await.unwrap()
}

async fn retry_moves_failed_versions_to_v2_and_they_extract(shape: V2Shape) {
    use coralswarm_backend::failed_extraction_retry::{self as retry, ControlCheck};
    let f = v2_fixture(&shape).await;
    // Receipt counts below are extraction calls only.
    std::env::set_var("JEV_EXTRACTION_BINDING_MODE", "off");
    let author = json!({"author_name":"Dana Kim"});
    // A v2 window whose every atom is unanchored: retried once, then failed.
    let failed_v2 = f.capture_with("slack", "fixture:standing", "retry-v2", V2_TEXT, author.clone(), Some(f.user.user_id)).await;
    f.sweep().await;
    let (llm, bad_calls, server) = answering(unanchored_atoms(false), unanchored_atoms(true)).await;
    f.tick_until_idle(&llm).await;
    assert_eq!(status_of(&f, failed_v2).await, ("source-atoms-v2".into(), "failed".into(), Some("no valid atoms".into())));
    assert_eq!(bad_calls.load(Ordering::SeqCst), 2, "an all-bad window is retried once, then fails");
    let (billed_v2, open_v2, _) = billed(&f, failed_v2).await;
    assert_eq!((billed_v2, open_v2), (2, 0), "two calls, each billed once, none left open");

    let legacy = f.capture_with("slack", "fixture:standing", "retry-legacy", V2_TEXT, author.clone(), Some(f.user.user_id)).await;
    let held = f.capture("note", "retry-held", "Retained evidence.").await;
    let pre_floor = f.capture("note", "retry-pre-floor", "Retained evidence.").await;
    let removed = f.capture("note", "retry-removed", "Retained evidence.").await;
    f.sweep().await;
    f.tick_until_idle(&llm).await;
    server.abort();
    for version in [legacy, held, pre_floor, removed] {
        assert_eq!(status_of(&f, version).await.1, "failed");
    }
    sqlx::query("UPDATE source_versions SET extraction_error='evidence quote not found' WHERE version_id=$1").bind(legacy).execute(f.owner()).await.unwrap();
    sqlx::query("UPDATE source_versions SET extraction_error='model request failed' WHERE version_id=$1").bind(held).execute(f.owner()).await.unwrap();
    sqlx::query("UPDATE source_versions SET captured_at='2026-10-12T12:00:00Z' WHERE version_id=$1").bind(pre_floor).execute(f.owner()).await.unwrap();
    sqlx::query("UPDATE source_items SET access_state='restricted' WHERE source_item_id=(SELECT source_item_id FROM source_versions WHERE version_id=$1)").bind(removed).execute(f.owner()).await.unwrap();
    // `held`: its last logical call still holds control-plane credit.
    let held_call: Uuid = sqlx::query_scalar("SELECT DISTINCT logical_call FROM managed_attempt_receipts WHERE source_version_id=$1 LIMIT 1")
        .bind(held).fetch_one(f.owner()).await.unwrap();
    let mut financial = scope::begin_account_scoped(&f.control.owner, f.user.account_id).await.unwrap();
    coralswarm_backend::billing::renewal::lock_period_rotation(&mut financial, f.user.account_id).await.unwrap();
    sqlx::query("INSERT INTO managed_credit_reservations(plane,receipt_id,account_id,payer_id,graph_id,logical_call,request_hash,bound_micro_usd,bound_credits,conversion_effective_from,reference_cost_per_credit,funding_basis) VALUES($1,gen_random_uuid(),$2,$3,$4,$5,repeat('a',64),400,1,now(),1,'pool')")
        .bind(control_plane(&f)).bind(f.user.account_id).bind(f.user.user_id).bind(f.graph).bind(held_call)
        .execute(&mut *financial).await.unwrap();
    financial.commit().await.unwrap();
    // A v1 version that failed before v1 was retired.
    let (failed_v1, ..) = f.capture_v1("retry-v1", "Retained evidence.", "Retained evidence.").await;
    sqlx::query("UPDATE source_versions SET extraction_status='failed',extraction_error='evidence quote not found',captured_at=now() WHERE version_id=$1")
        .bind(failed_v1).execute(f.owner()).await.unwrap();

    // The tool refuses an RLS-filtered role (it would see part of the data).
    if matches!(shape, V2Shape::Consumer) {
        assert!(coralswarm_backend::node_atoms_bridge::assert_policy_bypass_on(&f.worker.pool, &["source_versions", "corpus_enrichment_units"]).await.is_err(),
            "the app role is RLS-filtered and must be refused");
    }
    // Without the control plane, a version that ever called is not touched.
    let blind = retry::summarize(&retry::classify(f.owner(), Some(f.graph), None).await.unwrap(), &retry::CostBasis::default());
    assert_eq!(blind.total.eligible, 0);
    assert_eq!(blind.total.skipped["unverified_control_settlement"], 4);
    assert_eq!(blind.total.skipped["unsupported_profile"], 1, "no worker extracts v1: a failed v1 version is never retried");
    let control = ControlCheck { pool: &f.control.owner, plane: control_plane(&f) };
    let basis = retry::cost_basis(f.owner()).await.unwrap();
    let p = retry::summarize(&retry::classify(f.owner(), Some(f.graph), Some(&control)).await.unwrap(), &basis);
    assert_eq!((p.total.failed, p.total.eligible), (6, 2));
    assert_eq!(p.by_error["evidence quote not found"].eligible, 1);
    assert_eq!(p.by_error["no valid atoms"].eligible, 1);
    assert_eq!(p.by_error["model request failed"].skipped["in_flight"], 1, "held credit is never double-charged");
    assert_eq!(p.total.skipped["outside_standing_floor"], 1);
    assert_eq!(p.total.skipped["source_unavailable"], 1);
    assert_eq!(p.eligible_by_action["v2_requeue"], 2);
    assert_eq!(p.total.skipped["unsupported_profile"], 1);
    assert!(p.measured_cost && p.estimated_usd() > 0.0, "the estimate uses this database's billed receipts");

    let done = retry::execute(f.owner(), Some(f.graph), Some(&control), 500).await.unwrap();
    assert_eq!((done.requeued_v2, done.readopted_v2, done.skipped), (2, 0, 0));
    let again = retry::execute(f.owner(), Some(f.graph), Some(&control), 500).await.unwrap();
    assert_eq!(again, retry::Executed::default(), "idempotent: a second run finds nothing");
    for version in [legacy, failed_v2] {
        assert_eq!(status_of(&f, version).await, ("source-atoms-v2".into(), "blocked".into(), Some(retry::REQUEUE_REASON.into())));
    }
    assert_eq!(status_of(&f, failed_v1).await.0, "source-atoms-v1");
    for version in [held, pre_floor, removed] {
        assert_eq!(status_of(&f, version).await.1, "failed", "{version}: skipped versions stay failed");
    }

    // Standing requeues both failed v2 units, and both extract with the
    // item-level validator.
    f.sweep().await;
    let (llm, good_calls, server) = answering(json!({"atoms":[]}), v2_atoms()).await;
    f.tick_until_idle(&llm).await;
    f.sweep().await;
    f.tick_until_idle(&llm).await;
    for version in [legacy, failed_v2] {
        assert_eq!(status_of(&f, version).await.1, "done", "{version}");
        let atoms: i64 = sqlx::query_scalar("SELECT count(*) FROM semantic_atoms WHERE source_version_id=$1 AND extraction_version='source-atoms-v2'")
            .bind(version).fetch_one(f.owner()).await.unwrap();
        assert_eq!(atoms, 2, "{version}: both atoms kept; their bad relations dropped one by one");
        assert_eq!(billed(&f, version).await.1, 0, "{version}: nothing left reserved");
    }
    assert_eq!(good_calls.load(Ordering::SeqCst), 2, "one v2 extraction per retried version");
    let (billed_after, _, _) = billed(&f, failed_v2).await;
    assert_eq!(billed_after, billed_v2 + 1, "the requeued window adds one billed call");
    assert_eq!(status_of(&f, failed_v1).await, ("source-atoms-v1".into(), "failed".into(), Some("evidence quote not found".into())));
    assert_eq!(status_of(&f, held).await.1, "failed");
    println!("retry_failed_v2 shape_owner={} bad_calls={} good_calls={}", owner_runtime(&f).await, bad_calls.load(Ordering::SeqCst), good_calls.load(Ordering::SeqCst));
    server.abort();
    f.done().await;
}
#[tokio::test]
async fn consumer_retry_moves_failed_versions_to_v2_and_they_extract() { retry_moves_failed_versions_to_v2_and_they_extract(V2Shape::Consumer).await; }
#[tokio::test]
async fn tenant_owner_retry_moves_failed_versions_to_v2_and_they_extract() { retry_moves_failed_versions_to_v2_and_they_extract(V2Shape::TenantOwner).await; }

/// A failed `source-atoms-v3` version (every atom unanchored, the window
/// retried once, then failed) used to be `unsupported_profile` to the retry
/// tool, and standing never re-adopts `failed`: it stayed failed for good,
/// hiding its item's earlier versions from Ask. It is now requeued on v3
/// (cursor kept, never repinned to v2) and extracts under v3, billed once.
async fn retry_requeues_failed_v3_versions_on_v3(shape: V2Shape) {
    use coralswarm_backend::failed_extraction_retry::{self as retry, ControlCheck};
    let f = v2_fixture(&shape).await;
    std::env::set_var("JEV_EXTRACTION_BINDING_MODE", "off");
    f.use_profile("source-atoms-v3");
    let text = "Cy Lo works at Initech.";
    let failed = f.capture_with("slack", "fixture:standing", "retry-v3", text, json!({"author_name":"Dana Kim"}), Some(f.user.user_id)).await;
    f.sweep().await;
    // Every atom invalid by schema: the window fails after its retry. (An
    // answer whose only fault is unanchorable quotes finishes empty since
    // v3.4.3, so it would not fail the version.)
    let invalid = json!({"atoms":[{"kind":"fact"}]});
    let (llm, bad_calls, _, server) = structured_provider("emit_source_atoms_v3", invalid, JevReply::Bind(vec![])).await;
    f.tick_until_idle(&llm).await;
    server.abort();
    assert_eq!(status_of(&f, failed).await, ("source-atoms-v3".into(), "failed".into(), Some("no valid atoms".into())));
    assert_eq!(bad_calls.load(Ordering::SeqCst), 2, "an all-bad window is retried once, then fails");
    let (billed_before, open_before, _) = billed(&f, failed).await;
    assert_eq!((billed_before, open_before), (2, 0));

    let control = ControlCheck { pool: &f.control.owner, plane: control_plane(&f) };
    let basis = retry::cost_basis(f.owner()).await.unwrap();
    let p = retry::summarize(&retry::classify(f.owner(), Some(f.graph), Some(&control)).await.unwrap(), &basis);
    assert_eq!((p.total.failed, p.total.eligible), (1, 1), "a failed v3 version is retryable: {p:?}");
    assert!(!p.total.skipped.contains_key("unsupported_profile"), "{p:?}");
    assert_eq!(p.eligible_by_action.get("v3_requeue"), Some(&1), "{p:?}");

    let done = retry::execute(f.owner(), Some(f.graph), Some(&control), 500).await.unwrap();
    assert_eq!((done.requeued_v3, done.readopted_v3, done.requeued_v2, done.skipped), (1, 0, 0, 0));
    let again = retry::execute(f.owner(), Some(f.graph), Some(&control), 500).await.unwrap();
    assert_eq!(again, retry::Executed::default(), "idempotent: a second run finds nothing");
    assert_eq!(status_of(&f, failed).await, ("source-atoms-v3".into(), "blocked".into(), Some(retry::REQUEUE_REASON.into())),
        "stays on v3: never repinned backwards");

    let answer = json!({"atoms":[{"kind":"fact","text":"Cy Lo works at Initech.","evidence_quote":"Cy Lo works at Initech.",
        "entities":[{"mention":"Cy Lo","type":"person"},{"mention":"Initech","type":"org"}],
        "relations":[v3_rel("Cy Lo","works_at","Initech")],"changes":[],"valid_from":null,"valid_until":null,"validity_quote":null}]});
    let (llm, good_calls, _, server) = structured_provider("emit_source_atoms_v3", answer, JevReply::Bind(vec![])).await;
    for _ in 0..2 {
        f.sweep().await;
        f.tick_until_idle(&llm).await;
    }
    server.abort();
    assert_eq!(status_of(&f, failed).await.1, "done");
    let atoms: i64 = sqlx::query_scalar("SELECT count(*) FROM semantic_atoms WHERE source_version_id=$1 AND extraction_version='source-atoms-v3'")
        .bind(failed).fetch_one(f.owner()).await.unwrap();
    assert_eq!(atoms, 1);
    assert_eq!(good_calls.load(Ordering::SeqCst), 1, "one v3 extraction for the retried version");
    let (billed_after, open_after, _) = billed(&f, failed).await;
    assert_eq!((billed_after, open_after), (billed_before + 1, 0), "the requeued window adds one billed call, nothing left reserved");
    println!("retry_failed_v3 shape_owner={} bad_calls={} good_calls={}", owner_runtime(&f).await, bad_calls.load(Ordering::SeqCst), good_calls.load(Ordering::SeqCst));
    f.done().await;
}
#[tokio::test]
async fn consumer_retry_requeues_failed_v3_versions_on_v3() { retry_requeues_failed_v3_versions_on_v3(V2Shape::Consumer).await; }
#[tokio::test]
async fn tenant_owner_retry_requeues_failed_v3_versions_on_v3() { retry_requeues_failed_v3_versions_on_v3(V2Shape::TenantOwner).await; }

/// The before-floor one-off (`--before-floor`): failed captures from before
/// standing's 2026-10-13 floor, with no committed window, are never processed
/// by standing. Only the listed ones get a recorded `standing_floor_overrides`
/// row; standing then extracts exactly those under v2 once it is running,
/// billed once each. Everything not selected is untouched.
async fn before_floor_override_extracts_only_listed_versions(shape: V2Shape) {
    // One job per tick. A tenant-plane job needs a connection from BOTH
    // one-connection fixture pools (control `app`, then the content `app`); at
    // the default SOURCE_ATOMS_CONCURRENCY of 8, sibling jobs each take one
    // pool and wait on the other until the 2 s `acquire_timeout` fires, and
    // the version is held as `infrastructure_unavailable` (extraction_status
    // `blocked`). Production pools are not one connection wide; serialising
    // keeps this test about what it asserts rather than fixture pool width.
    let _serial = EnvGuard::set(source_atoms::CONCURRENCY_ENV, "1");
    use coralswarm_backend::failed_extraction_retry::{self as retry, ControlCheck, FloorOverride, Selection};
    let f = v2_fixture(&shape).await;
    std::env::set_var("JEV_EXTRACTION_BINDING_MODE", "off");
    let author = json!({"author_name":"Dana Kim"});
    // Captures whose whole window fails: no committed window.
    let selected = f.capture_with("slack", "fixture:standing", "floor-selected", V2_TEXT, author.clone(), Some(f.user.user_id)).await;
    let unselected = f.capture_with("slack", "fixture:standing", "floor-unselected", V2_TEXT, author.clone(), Some(f.user.user_id)).await;
    let bare = f.capture_with("slack", "fixture:standing", "floor-bare", V2_TEXT, author.clone(), Some(f.user.user_id)).await;
    let removed = f.capture("note", "floor-removed", "Retained evidence.").await;
    let recent = f.capture_with("slack", "fixture:standing", "floor-recent", V2_TEXT, author.clone(), Some(f.user.user_id)).await;
    let (llm, _, server) = answering(unanchored_atoms(false), unanchored_atoms(true)).await;
    // Two passes: a sweep adopts a bounded number of captures per pass.
    for _ in 0..2 {
        f.sweep().await;
        f.tick_until_idle(&llm).await;
    }
    server.abort();
    for version in [selected, unselected, bare, removed, recent] {
        assert_eq!(status_of(&f, version).await.1, "failed", "{version}");
    }
    let pre_floor = vec![selected, unselected, bare, removed];
    sqlx::query("UPDATE source_versions SET captured_at='2026-10-12T12:00:00Z' WHERE version_id=ANY($1)").bind(&pre_floor).execute(f.owner()).await.unwrap();
    sqlx::query("UPDATE source_items SET access_state='restricted' WHERE source_item_id=(SELECT source_item_id FROM source_versions WHERE version_id=$1)").bind(removed).execute(f.owner()).await.unwrap();
    let windows: i64 = sqlx::query_scalar("SELECT count(*) FROM semantic_atom_windows WHERE source_version_id=ANY($1)").bind(&pre_floor).fetch_one(f.owner()).await.unwrap();
    assert_eq!(windows, 0, "the class standing never picks up: pre-floor, no window");
    let receipts_before: Vec<(i64, i64, i64)> = {
        let mut out = Vec::new();
        for version in [selected, unselected, bare] { out.push(billed(&f, version).await); }
        out
    };

    let control = ControlCheck { pool: &f.control.owner, plane: control_plane(&f) };
    let basis = retry::cost_basis(f.owner()).await.unwrap();
    // The standing pass still skips them: the global floor is unchanged.
    let standing = retry::summarize(&retry::classify(f.owner(), Some(f.graph), Some(&control)).await.unwrap(), &basis);
    assert_eq!(standing.total.skipped["outside_standing_floor"], 3);
    assert_eq!(standing.total.eligible, 1, "only the post-floor capture");

    let grant = FloorOverride::new("COR-99 pre-floor one-off (fixture)", "fixture-operator").unwrap();
    let one_off = Selection { versions: Some(vec![selected, bare, removed, recent]), floor_override: Some(grant.clone()) };
    let p = retry::summarize(&retry::classify_selected(f.owner(), Some(f.graph), Some(&control), &one_off).await.unwrap(), &basis);
    assert_eq!((p.total.failed, p.total.eligible), (3, 2), "listed and pre-floor only: `recent` is post-floor, `unselected` is not listed");
    assert_eq!(p.total.skipped["source_unavailable"], 1, "access state is respected");
    assert_eq!(p.eligible_by_action["v2_requeue"], 2);

    // Retried while standing is paused (the prod state): nothing is adopted.
    std::env::set_var("STANDING_PROCESSING_ENABLED", "false");
    let done = retry::execute_selected(f.owner(), Some(f.graph), Some(&control), 500, &one_off).await.unwrap();
    assert_eq!((done.requeued_v2, done.skipped, done.overrides_recorded), (2, 0, 2));
    let again = retry::execute_selected(f.owner(), Some(f.graph), Some(&control), 500, &one_off).await.unwrap();
    assert_eq!(again, retry::Executed::default(), "idempotent: a second run finds nothing");
    let rows: Vec<(Uuid, String, String, String, String)> = sqlx::query_as(
        "SELECT source_version_id,extraction_version,reason,granted_by,granted_via FROM standing_floor_overrides WHERE graph_id=$1 ORDER BY source_version_id")
        .bind(f.graph).fetch_all(f.owner()).await.unwrap();
    let mut expected = vec![selected, bare];
    expected.sort();
    assert_eq!(rows.iter().map(|r| r.0).collect::<Vec<_>>(), expected);
    for row in &rows {
        assert_eq!((row.1.as_str(), row.2.as_str(), row.3.as_str(), row.4.as_str()),
            ("source-atoms-v2", grant.reason.as_str(), grant.granted_by.as_str(), "retry_failed_extractions_v2"));
    }
    assert!(sqlx::query("UPDATE standing_floor_overrides SET reason='rewritten after the fact' WHERE source_version_id=$1")
        .bind(selected).execute(f.owner()).await.is_err(), "the audit row is append-only, even for the owner");
    if !matches!(shape, V2Shape::TenantOwner) {
        let mut tx = match shape {
            V2Shape::Consumer => scope::begin_account_scoped(&f.worker.pool, f.user.account_id).await.unwrap(),
            _ => scope::begin_tenant_system_scoped(&f.worker.pool, f.user.account_id).await.unwrap(),
        };
        assert!(sqlx::query("INSERT INTO standing_floor_overrides(source_version_id,graph_id,extraction_version,reason,granted_by) VALUES($1,$2,'source-atoms-v2','runtime self-grant','app')")
            .bind(unselected).bind(f.graph).execute(&mut *tx).await.is_err(), "the runtime role cannot grant itself an override");
        tx.rollback().await.unwrap();
    }
    // The control: same retry, override removed. Standing must not take it.
    sqlx::query("DELETE FROM standing_floor_overrides WHERE source_version_id=$1").bind(bare).execute(f.owner()).await.unwrap();
    for version in [selected, bare] {
        assert_eq!(status_of(&f, version).await, ("source-atoms-v2".into(), "blocked".into(), Some(retry::REQUEUE_REASON.into())));
    }
    f.sweep().await;
    let queued: i64 = sqlx::query_scalar("SELECT count(*) FROM jobs WHERE payload->>'source_version_id'=ANY($1) AND status::text IN ('pending','running')")
        .bind(vec![selected.to_string(), bare.to_string()]).fetch_one(f.owner()).await.unwrap();
    assert_eq!(queued, 0, "paused standing requeues nothing");

    // Standing resumes: it adopts and extracts the overridden version only.
    std::env::set_var("STANDING_PROCESSING_ENABLED", "true");
    let (llm, good_calls, server) = answering(json!({"atoms":[]}), v2_atoms()).await;
    for _ in 0..2 {
        f.sweep().await;
        f.tick_until_idle(&llm).await;
    }
    server.abort();
    assert_eq!(status_of(&f, selected).await.1, "done");
    let atoms: i64 = sqlx::query_scalar("SELECT count(*) FROM semantic_atoms WHERE source_version_id=$1 AND extraction_version='source-atoms-v2'")
        .bind(selected).fetch_one(f.owner()).await.unwrap();
    assert_eq!(atoms, 2, "extracted under v2 with the item-level validator");
    assert_eq!(good_calls.load(Ordering::SeqCst), 1, "one v2 extraction call, for the overridden version only");
    let (billed_after, open_after, _) = billed(&f, selected).await;
    assert_eq!((billed_after, open_after), (receipts_before[0].0 + 1, 0), "billed once; nothing left reserved");
    let bare_status = status_of(&f, bare).await;
    assert_eq!((bare_status.0.as_str(), bare_status.1.as_str()), ("source-atoms-v2", "blocked"),
        "without its override row a pre-floor version stays outside standing");
    assert_eq!(billed(&f, bare).await, receipts_before[2], "the control is never charged");
    assert_eq!(status_of(&f, unselected).await, ("source-atoms-v2".into(), "failed".into(), Some("no valid atoms".into())), "not selected: untouched");
    assert_eq!(billed(&f, unselected).await, receipts_before[1]);
    assert_eq!(status_of(&f, removed).await.1, "failed");
    assert_eq!(status_of(&f, recent).await.1, "failed", "post-floor captures are not part of the one-off");
    println!("before_floor_override shape_owner={} good_calls={}", owner_runtime(&f).await, good_calls.load(Ordering::SeqCst));
    f.done().await;
}
#[tokio::test]
async fn consumer_before_floor_override_extracts_only_listed_versions() { before_floor_override_extracts_only_listed_versions(V2Shape::Consumer).await; }
/// Org tenant on the RLS-subject app role: the tenant read policy
/// (`tenant_rls_is_system()`) is what lets the standing sweep see the row.
#[tokio::test]
async fn tenant_before_floor_override_extracts_only_listed_versions() { before_floor_override_extracts_only_listed_versions(V2Shape::Tenant).await; }
#[tokio::test]
async fn tenant_owner_before_floor_override_extracts_only_listed_versions() { before_floor_override_extracts_only_listed_versions(V2Shape::TenantOwner).await; }

async fn reefs_titled(f: &Fixture, graph: Uuid, title: &str) -> Vec<Uuid> {
    sqlx::query_scalar("SELECT node_id FROM nodes WHERE graph_id=$1 AND lower(title)=lower($2) ORDER BY created_at,node_id")
        .bind(graph).bind(title).fetch_all(f.owner()).await.unwrap()
}

/// Same title and type is not enough when decisive identifiers disagree:
/// the copy with another email stays a separate reef, the one sharing the
/// keeper's email (or carrying none) folds in.
async fn dedupe_never_merges_conflicting_identities(shape: V2Shape) {
    use coralswarm_backend::reef_dedupe::{self, Exclusion, Policy};
    let f = v2_fixture(&shape).await;
    let reef = |age_days: i32| {
        let f = &f;
        async move {
            sqlx::query_scalar::<_, Uuid>("INSERT INTO nodes(graph_id,type,title,entity_type,body,identity_scope,created_at) VALUES($1,'entity','Alex Kim','person','{}',gen_random_uuid(),now()-make_interval(days=>$2)) RETURNING node_id")
                .bind(f.graph).bind(age_days).fetch_one(f.owner()).await.unwrap()
        }
    };
    let (keep, same, bare, other) = (reef(9).await, reef(5).await, reef(4).await, reef(3).await);
    for (node, email) in [(keep, "alex@one.test"), (other, "alex@two.test")] {
        sqlx::query("INSERT INTO reef_identifiers(graph_id,node_id,kind,value_norm,source) VALUES($1,$2,'email',$3,'test')")
            .bind(f.graph).bind(node).bind(email).execute(f.owner()).await.unwrap();
    }
    let _ = same;
    let policy = Policy { cutoff: "2000-01-01T00:00:00Z".parse().unwrap(), include_preexisting: true, ..Policy::default() };
    let groups = reef_dedupe::plan(f.owner(), &policy).await.unwrap();
    let r = reef_dedupe::report(&groups, &policy);
    assert_eq!((r.groups_to_merge, r.reefs_to_merge), (1, 2));
    assert_eq!(r.excluded.get(Exclusion::IdentifierConflict.as_str()), Some(&(1, 1)));
    let done = reef_dedupe::execute(f.owner(), &policy, f.user.user_id, None).await.unwrap();
    assert_eq!((done.groups_merged, done.reefs_merged, done.groups_failed), (1, 2, 0), "{:?}", done.failures);
    let left = reefs_titled(&f, f.graph, "Alex Kim").await;
    assert_eq!(left, vec![keep, other], "the other email is another person");
    let again = reef_dedupe::execute(f.owner(), &policy, f.user.user_id, None).await.unwrap();
    assert_eq!(again.reefs_merged, 0);
    assert!(!left.contains(&bare));
    f.done().await;
}
#[tokio::test]
async fn consumer_dedupe_never_merges_conflicting_identities() { dedupe_never_merges_conflicting_identities(V2Shape::Consumer).await; }
#[tokio::test]
async fn tenant_owner_dedupe_never_merges_conflicting_identities() { dedupe_never_merges_conflicting_identities(V2Shape::TenantOwner).await; }

/// Production on 2026-10-20: about 2,350 standing windows were queued
/// oldest-first and the worker claimed about 95 an hour, so every capture of
/// the last 24 hours waited behind roughly a day of backlog. The fresh lane
/// claims half of each tick from captures inside `FRESH_CAPTURE_WINDOW`,
/// interleaved across (graph, source type), and the rest oldest-first.
async fn fresh_captures_extract_ahead_of_backlog(org: bool, owner_runtime: bool) {
    let f = Fixture::create(org, false, owner_runtime).await;
    let _concurrency = EnvGuard::set(source_atoms::CONCURRENCY_ENV, "2");
    let second: Uuid = sqlx::query_scalar("INSERT INTO graphs(account_id,name,kind,owner_user_id) VALUES($1,'Second standing ocean','member',$2) RETURNING id")
        .bind(f.user.account_id).bind(f.user.user_id).fetch_one(f.owner()).await.unwrap();
    let mut backlog = Vec::new();
    for i in 0..4 {
        backlog.push(f.capture("slack", &format!("backlog-{i}"), "Retained evidence.").await);
    }
    f.sweep().await;
    sqlx::query("UPDATE source_versions SET captured_at=now()-interval '3 days' WHERE version_id=ANY($1)")
        .bind(&backlog).execute(f.owner()).await.unwrap();
    let mut code = Vec::new();
    for i in 0..3 {
        code.push(f.capture("github_code", &format!("code-{i}"), "Retained evidence.").await);
    }
    f.sweep().await;
    let slack = f.capture("slack", "fresh-slack", "Retained evidence.").await;
    let other = f.capture_into(second, "slack", "fixture:standing", "fresh-other-ocean", "Retained evidence.", json!({}), Some(f.user.user_id)).await;
    f.sweep().await;
    let queued: i64 = sqlx::query_scalar("SELECT count(*) FROM jobs WHERE kind='ExtractStandingSourceAtoms' AND status='pending'")
        .fetch_one(f.owner()).await.unwrap();
    assert_eq!(queued, 9, "every capture is adopted and queued, backlog first, then code, then Slack");
    let done = |versions: Vec<Uuid>| {
        let pool = f.owner().clone();
        async move {
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM source_versions WHERE version_id=ANY($1) AND extraction_status='done'")
                .bind(versions).fetch_one(&pool).await.unwrap()
        }
    };
    let (llm, _hits, server) = provider().await;
    let fresh: Vec<Uuid> = code.iter().copied().chain([slack, other]).collect();
    source_atoms::tick(&f.worker, &f.control.app, &llm, None).await.unwrap();
    assert_eq!(done(fresh.clone()).await, 1, "a fresh capture is extracted on the first tick while the backlog waits");
    assert_eq!(done(backlog.clone()).await, 1, "the backlog still drains, one window per tick");
    source_atoms::tick(&f.worker, &f.control.app, &llm, None).await.unwrap();
    assert_eq!(done(vec![slack, other]).await, 2, "a single new capture in each ocean goes before an earlier burst of code files");
    assert_eq!(done(code.clone()).await, 0);
    source_atoms::tick(&f.worker, &f.control.app, &llm, None).await.unwrap();
    assert_eq!(done(code.clone()).await, 1, "the burst is served once the other pairs are empty");
    assert_eq!(done(backlog.clone()).await, 3);
    println!("fresh_lane org={org} owner_runtime={owner_runtime}");
    server.abort();
    f.done().await;
}
#[tokio::test]
async fn consumer_fresh_captures_extract_ahead_of_backlog() { fresh_captures_extract_ahead_of_backlog(false, false).await; }
#[tokio::test]
async fn tenant_fresh_captures_extract_ahead_of_backlog() { fresh_captures_extract_ahead_of_backlog(true, false).await; }
#[tokio::test]
async fn tenant_owner_fresh_captures_extract_ahead_of_backlog() { fresh_captures_extract_ahead_of_backlog(true, true).await; }

/// Production at 05:12Z on 2026-10-20: 174 of the fresh lane's 328 windows
/// had been captured more than 6 h earlier and re-queued by recovery. The lane
/// served each pair oldest-first, so a Slack message captured two minutes
/// earlier sat 49th of 49 in its pair. Newest-first by `captured_at` serves the
/// new capture, then its next window, ahead of the recovered windows. While new
/// captures keep arriving, the recovered windows still drain through the
/// oldest-first half of each tick.
async fn fresh_lane_serves_newest_capture_first(org: bool, owner_runtime: bool) {
    let f = Fixture::create(org, false, owner_runtime).await;
    let _concurrency = EnvGuard::set(source_atoms::CONCURRENCY_ENV, "2");
    let mut backlog = Vec::new();
    for i in 0..2 {
        backlog.push(f.capture("slack", &format!("backlog-{i}"), "Retained evidence.").await);
    }
    f.sweep().await;
    sqlx::query("UPDATE source_versions SET captured_at=now()-interval '3 days' WHERE version_id=ANY($1)")
        .bind(&backlog).execute(f.owner()).await.unwrap();
    // Recovery re-queues captures hours after they arrived: still inside the
    // fresh window, and queued ahead of anything captured since.
    let mut recovered = Vec::new();
    for i in 0..3 {
        recovered.push(f.capture("slack", &format!("recovered-{i}"), "Retained evidence.").await);
    }
    f.sweep().await;
    sqlx::query("UPDATE source_versions SET captured_at=now()-interval '8 hours' WHERE version_id=ANY($1)")
        .bind(&recovered).execute(f.owner()).await.unwrap();
    let new = f.capture("slack", "new-two-windows",
        &format!("Retained evidence.\n{}Retained evidence.", "padding\n".repeat(1400))).await;
    f.sweep().await;
    let queued: i64 = sqlx::query_scalar("SELECT count(*) FROM jobs WHERE kind='ExtractStandingSourceAtoms' AND status='pending'")
        .fetch_one(f.owner()).await.unwrap();
    assert_eq!(queued, 6, "backlog, recovered and the new capture's first window are queued in that order");
    let done = |versions: Vec<Uuid>| {
        let pool = f.owner().clone();
        async move {
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM source_versions WHERE version_id=ANY($1) AND extraction_status='done'")
                .bind(versions).fetch_one(&pool).await.unwrap()
        }
    };
    let (llm, _hits, server) = provider().await;
    source_atoms::tick(&f.worker, &f.control.app, &llm, None).await.unwrap();
    let next: i64 = sqlx::query_scalar("SELECT next_start_char FROM source_versions WHERE version_id=$1")
        .bind(new).fetch_one(f.owner()).await.unwrap();
    assert!(next > 0, "the new capture's first window runs on the first tick, ahead of recovered windows (next_start_char={next})");
    assert_eq!(done(recovered.clone()).await, 0, "a recovered window does not take the fresh slot from a new capture");
    assert_eq!(done(backlog.clone()).await, 1, "the backlog still drains, one window per tick");
    source_atoms::tick(&f.worker, &f.control.app, &llm, None).await.unwrap();
    assert_eq!(done(vec![new]).await, 1, "the new capture's next window keeps its place and the document finishes");
    assert_eq!(done(recovered.clone()).await, 0);
    assert_eq!(done(backlog.clone()).await, 2);
    // A new capture arrives before every tick, so the fresh slot never reaches
    // the recovered windows. They are not starved: they keep their queue
    // position and drain through the oldest-first half.
    for i in 0..3 {
        let arrival = f.capture("slack", &format!("arrival-{i}"), "Retained evidence.").await;
        f.sweep().await;
        source_atoms::tick(&f.worker, &f.control.app, &llm, None).await.unwrap();
        assert_eq!(done(vec![arrival]).await, 1, "arrival {i} is extracted on the tick after it is adopted");
        assert_eq!(done(recovered.clone()).await, i + 1, "the oldest-first half serves one recovered window per tick");
    }
    println!("fresh_lane_newest org={org} owner_runtime={owner_runtime}");
    server.abort();
    f.done().await;
}
#[tokio::test]
async fn consumer_fresh_lane_serves_newest_capture_first() { fresh_lane_serves_newest_capture_first(false, false).await; }
#[tokio::test]
async fn tenant_fresh_lane_serves_newest_capture_first() { fresh_lane_serves_newest_capture_first(true, false).await; }
#[tokio::test]
async fn tenant_owner_fresh_lane_serves_newest_capture_first() { fresh_lane_serves_newest_capture_first(true, true).await; }

/// Production on 2026-10-20: 1,198 GitHub units sat `queued` with no job for
/// eleven hours, and their graph's adoption slot failed `Elapsed` on every
/// sweep. Requeue ran up to 200 units in one transaction under the 8 s slot
/// ceiling; at Neon round-trip cost that never committed, so the same units
/// rolled back every minute. The trigger adds 45 ms to each requeue, the
/// order of one unit's round trips against Neon.
async fn undelivered_units_requeue_in_committed_batches(org: bool) {
    let f = Fixture::new(org).await;
    let mut versions = Vec::new();
    for i in 0..300 {
        versions.push(f.capture("github_code", &format!("lost-{i}"), "Retained evidence.").await);
    }
    f.sweep().await;
    f.sweep().await;
    let owned: i64 = sqlx::query_scalar("SELECT count(*) FROM corpus_enrichment_units WHERE execution_owner AND state='queued'")
        .fetch_one(f.owner()).await.unwrap();
    assert_eq!(owned, 300);
    sqlx::query("UPDATE jobs SET status='done',finished_at=now() WHERE kind='ExtractStandingSourceAtoms' AND status='pending'")
        .execute(f.owner()).await.unwrap();
    sqlx::raw_sql("CREATE FUNCTION fixture_requeue_round_trips() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_sleep(0.045); RETURN NEW; END $$; \
        CREATE TRIGGER fixture_requeue_round_trips BEFORE UPDATE OF state ON corpus_enrichment_units FOR EACH ROW WHEN (OLD.state='pending' AND NEW.state='queued') EXECUTE FUNCTION fixture_requeue_round_trips();")
        .execute(f.owner()).await.unwrap();
    let delivered = || async {
        sqlx::query_scalar::<_, i64>("SELECT count(DISTINCT payload->>'source_version_id') FROM jobs WHERE kind='ExtractStandingSourceAtoms' AND status='pending'")
            .fetch_one(f.owner()).await.unwrap()
    };
    let swept = coralswarm_backend::test_standing_sweep(&f.worker, &f.control.app).await;
    let first = delivered().await;
    assert!(swept.is_ok(), "a large recovery set must not fail its slot: {swept:?}");
    assert!(first >= 50, "the first sweep commits whole batches: {first} of 300 redelivered");
    for _ in 0..6 {
        if delivered().await == 300 { break; }
        f.sweep().await;
    }
    assert_eq!(delivered().await, 300, "every lost delivery is requeued across sweeps");
    println!("requeue_batches org={org} first_sweep={first}");
    f.done().await;
}
#[tokio::test]
async fn consumer_undelivered_units_requeue_in_committed_batches() { undelivered_units_requeue_in_committed_batches(false).await; }
#[tokio::test]
async fn tenant_undelivered_units_requeue_in_committed_batches() { undelivered_units_requeue_in_committed_batches(true).await; }

/// Adoption reads 200 candidates per graph per sweep, oldest first. With a
/// larger blocked backlog (a repin, a failed-version retry, floor overrides),
/// a new capture waited until the backlog cursor wrapped. Fresh candidates
/// now have their own lane, adopted before the backlog page.
async fn fresh_capture_adopted_ahead_of_adoption_backlog(org: bool) {
    let f = Fixture::new(org).await;
    let mut backlog = Vec::new();
    for i in 0..210 {
        backlog.push(f.capture("github_code", &format!("blocked-{i:03}"), "Retained evidence.").await);
    }
    sqlx::query("UPDATE source_versions SET captured_at=now()-interval '3 days' WHERE version_id=ANY($1)")
        .bind(&backlog).execute(f.owner()).await.unwrap();
    let fresh = f.capture("slack", "fresh-behind-backlog", "Retained evidence.").await;
    // On a loaded machine the 200-version backlog page can outlast the slot.
    // The fresh lane commits before it, so the sweep's result is not the claim.
    let _ = coralswarm_backend::test_standing_sweep(&f.worker, &f.control.app).await;
    let owned: (bool, i64) = sqlx::query_as("SELECT EXISTS(SELECT 1 FROM corpus_enrichment_units WHERE holding_id=$1 AND execution_owner),(SELECT count(*) FROM corpus_enrichment_units WHERE execution_owner AND holding_id=ANY($2))")
        .bind(fresh).bind(&backlog).fetch_one(f.owner()).await.unwrap();
    assert!(owned.0, "the fresh capture is adopted on the first sweep");
    assert!(owned.1 > 0, "the backlog is still adopted in the same sweep");
    println!("fresh_adoption org={org} backlog_adopted={}", owned.1);
    f.done().await;
}
#[tokio::test]
async fn consumer_fresh_capture_adopted_ahead_of_adoption_backlog() { fresh_capture_adopted_ahead_of_adoption_backlog(false).await; }
#[tokio::test]
async fn tenant_fresh_capture_adopted_ahead_of_adoption_backlog() { fresh_capture_adopted_ahead_of_adoption_backlog(true).await; }

/// Standing owner units left `queued` with no job to deliver them (#1360).
async fn stranded(f: &Fixture) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM corpus_enrichment_units u WHERE u.execution_owner AND u.state='queued' AND (u.standing_claim_until IS NULL OR u.standing_claim_until<=now()) AND NOT EXISTS(SELECT 1 FROM jobs j WHERE j.payload->>'source_version_id'=u.holding_id::text AND j.status::text IN ('pending','running'))")
        .fetch_one(f.owner()).await.unwrap()
}
async fn standing_jobs(f: &Fixture) -> (i64, i64) {
    sqlx::query_as("SELECT count(*) FILTER (WHERE status::text IN ('pending','running')),count(*) FILTER (WHERE status::text NOT IN ('pending','running')) FROM jobs WHERE kind='ExtractStandingSourceAtoms'")
        .fetch_one(f.owner()).await.unwrap()
}

/// Production on 2026-10-19: `STANDING_PROCESSING_ENABLED=false` (Fly v638,
/// 14:48:43Z) restarted the workers, and within 19 s they closed all 2,670
/// pending standing jobs `done`/`nothing_to_do` while every unit stayed
/// `queued`. Pausing must leave the backlog queued, and resuming must finish
/// it without any recovery sweep.
async fn pause_mid_backlog_strands_nothing(org: bool, owner_runtime: bool) {
    let f = Fixture::create(org, false, owner_runtime).await;
    let mut versions = Vec::new();
    for id in ["paused-a", "paused-b", "paused-c"] {
        versions.push(f.capture("note", id, "Retained evidence.").await);
    }
    f.sweep().await;
    assert_eq!(standing_jobs(&f).await, (3, 0));
    let (llm, http, server) = provider().await;
    std::env::set_var("STANDING_PROCESSING_ENABLED", "false");
    for _ in 0..3 {
        source_atoms::tick(&f.worker, &f.control.app, &llm, None).await.unwrap();
    }
    std::env::set_var("STANDING_PROCESSING_ENABLED", "true");
    assert_eq!((standing_jobs(&f).await, stranded(&f).await, http.load(Ordering::SeqCst)), ((3, 0), 0, 0),
        "a paused worker leaves every standing job queued and calls no model");
    for _ in 0..3 {
        source_atoms::tick(&f.worker, &f.control.app, &llm, None).await.unwrap();
    }
    for version in &versions {
        assert_eq!(completed_snapshot(&f, *version).await["source"]["extraction_status"], json!("done"), "resumed without a sweep");
    }
    assert_eq!((http.load(Ordering::SeqCst), stranded(&f).await), (3, 0));
    server.abort();
    f.done().await;
}
#[tokio::test]
async fn consumer_pause_mid_backlog_strands_nothing() { pause_mid_backlog_strands_nothing(false, false).await; }
#[tokio::test]
async fn tenant_pause_mid_backlog_strands_nothing() { pause_mid_backlog_strands_nothing(true, false).await; }
#[tokio::test]
async fn tenant_owner_pause_mid_backlog_strands_nothing() { pause_mid_backlog_strands_nothing(true, true).await; }


/// A job the claim refuses (here its window cursor moved on) is closed in the
/// same transaction that hands its unit back to requeue, and the receipt keeps
/// the hold that actually happened.
async fn unclaimable_job_releases_its_unit(org: bool) {
    let f = Fixture::new(org).await;
    let version = f.capture("note", "unclaimable", "Retained evidence.").await;
    f.sweep().await;
    sqlx::query("UPDATE source_versions SET next_start_char=1 WHERE version_id=$1").bind(version).execute(f.owner()).await.unwrap();
    let (llm, http, server) = provider().await;
    source_atoms::tick(&f.worker, &f.control.app, &llm, None).await.unwrap();
    let unit: (String, Option<String>) = sqlx::query_as("SELECT state,reason FROM corpus_enrichment_units WHERE holding_id=$1 AND execution_owner")
        .bind(version).fetch_one(f.owner()).await.unwrap();
    assert_eq!((standing_jobs(&f).await, stranded(&f).await, http.load(Ordering::SeqCst)), ((0, 1), 0, 0));
    assert_eq!(unit, ("pending".into(), Some("infrastructure_unavailable".into())));
    f.sweep().await;
    assert_eq!((standing_jobs(&f).await.0, clock_basis(&f, version).await), (1, ("infrastructure_unavailable".into(), true)), "requeued, labelled with the hold it had");
    server.abort();
    f.done().await;
}
#[tokio::test]
async fn consumer_unclaimable_job_releases_its_unit() { unclaimable_job_releases_its_unit(false).await; }
#[tokio::test]
async fn tenant_unclaimable_job_releases_its_unit() { unclaimable_job_releases_its_unit(true).await; }

/// Production on 2026-10-20 01:45Z: twelve claims failed on storage three
/// times, the jobs went `dead`, and their units stayed `queued`.
async fn dead_lettered_claim_releases_its_unit(org: bool) {
    let f = Fixture::new(org).await;
    let version = f.capture("note", "dead-claim", "Retained evidence.").await;
    f.sweep().await;
    sqlx::query("UPDATE jobs SET attempts=max_attempts-1 WHERE kind='ExtractStandingSourceAtoms'").execute(f.owner()).await.unwrap();
    sqlx::raw_sql("CREATE FUNCTION fixture_claim_unavailable() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture: claim storage unavailable'; END $$; \
        CREATE TRIGGER fixture_claim_unavailable BEFORE UPDATE OF standing_generation ON corpus_enrichment_units FOR EACH ROW EXECUTE FUNCTION fixture_claim_unavailable();")
        .execute(f.owner()).await.unwrap();
    let (llm, http, server) = provider().await;
    source_atoms::tick(&f.worker, &f.control.app, &llm, None).await.unwrap();
    sqlx::raw_sql("DROP TRIGGER fixture_claim_unavailable ON corpus_enrichment_units").execute(f.owner()).await.unwrap();
    let job: String = sqlx::query_scalar("SELECT status::text FROM jobs WHERE payload->>'source_version_id'=$1").bind(version.to_string()).fetch_one(f.owner()).await.unwrap();
    assert_eq!((job.as_str(), stranded(&f).await, http.load(Ordering::SeqCst)), ("dead", 0, 0));
    f.sweep().await;
    source_atoms::tick(&f.worker, &f.control.app, &llm, None).await.unwrap();
    assert_eq!(completed_snapshot(&f, version).await["source"]["extraction_status"], json!("done"));
    server.abort();
    f.done().await;
}
#[tokio::test]
async fn consumer_dead_lettered_claim_releases_its_unit() { dead_lettered_claim_releases_its_unit(false).await; }
#[tokio::test]
async fn tenant_dead_lettered_claim_releases_its_unit() { dead_lettered_claim_releases_its_unit(true).await; }

async fn clock_basis(f: &Fixture, version: Uuid) -> (String, bool) {
    sqlx::query_as("SELECT hold_basis,ever_held FROM corpus_capture_admissions WHERE source_version_id=$1")
        .bind(version).fetch_one(f.owner()).await.unwrap()
}
/// #1359: every capture now reaches standing through adoption. A capture
/// adopted within a day of its receipt is `standing_admitted` and counts in
/// the freshness SLO; an older one is backlog. The health worker reports both.
async fn adopted_capture_clock_labels(org: bool, owner_runtime: bool) {
    use coralswarm_backend::{capture_admission_health::ClockClass, workers::capture_admission_health as reporter};
    let f = Fixture::create(org, false, owner_runtime).await;
    let fresh = f.capture("note", "label-fresh", "Retained evidence.").await;
    let old = f.capture("note", "label-old", "Retained evidence.").await;
    sqlx::query("ALTER TABLE corpus_capture_admissions DISABLE TRIGGER corpus_admission_observation_immutable").execute(f.owner()).await.unwrap();
    sqlx::query("UPDATE corpus_capture_admissions SET observed_at=now()-interval '2 days' WHERE source_version_id=$1").bind(old).execute(f.owner()).await.unwrap();
    sqlx::query("ALTER TABLE corpus_capture_admissions ENABLE TRIGGER corpus_admission_observation_immutable").execute(f.owner()).await.unwrap();
    f.sweep().await;
    assert_eq!(clock_basis(&f, fresh).await, ("standing_admitted".into(), false));
    assert_eq!(clock_basis(&f, old).await, ("backlog_policy_excluded".into(), true));
    let (llm, _http, server) = provider().await;
    for _ in 0..3 {
        source_atoms::tick(&f.worker, &f.control.app, &llm, None).await.unwrap();
    }
    assert_eq!(completed_snapshot(&f, fresh).await["source"]["extraction_status"], json!("done"));
    let report = reporter::measure(&f.worker, &f.control.app, chrono::Utc::now()).await.unwrap();
    let lag = report.lag.iter().find(|row| row.graph_id == f.graph).expect("the adopted fresh capture is an SLO sample");
    assert_eq!((lag.processed, lag.slo_met), (1, true));
    let classes: Vec<(ClockClass, Option<String>, i64)> = report.clock_coverage.unwrap().into_iter()
        .filter(|row| row.graph_id == f.graph).map(|row| (row.class, row.hold_basis, row.captures)).collect();
    assert!(classes.contains(&(ClockClass::NeverHeld, Some("standing_admitted".into()), 1)), "{classes:?}");
    assert!(classes.contains(&(ClockClass::Backlog, Some("backlog_policy_excluded".into()), 1)), "{classes:?}");
    server.abort();
    f.done().await;
}
/// 2026-10-25: the capture SLO stayed breached for a week on "unknown clock
/// groups" with every lag sample inside the SLO. Two sources, both fixed at
/// the receipt rather than by exempting a group:
/// * a carried re-capture (`carries_extraction`, #1397) is `done` at capture
///   and was never receipted, so it read `unknown` forever (423 on the org
///   tenant). Capture now receipts it and the trigger, from the source facts
///   alone, records `carried_forward`;
/// * a receipt from before the clock columns (`legacy_unknown`) can never get
///   a clock (5,138 org receipts). It is `legacy`, unless its state
///   contradicts its source, which stays `unknown`.
async fn carried_and_legacy_receipts_have_a_clock_class(org: bool) {
    use coralswarm_backend::{capture_admission_health::{self as health, ClockClass}, source_carry_forward, workers::capture_admission_health as reporter};
    let f = Fixture::new(org).await;
    let user = Some(f.user.user_id);
    let first = f.capture_with("note", "fixture:standing", "carry-me", "Retained evidence.", json!({"title":"before"}), user).await;
    f.sweep().await;
    let (llm, http, server) = provider().await;
    for _ in 0..3 {
        source_atoms::tick(&f.worker, &f.control.app, &llm, None).await.unwrap();
    }
    assert_eq!(completed_snapshot(&f, first).await["source"]["extraction_status"], json!("done"));
    let calls = http.load(Ordering::SeqCst);
    let carried = f.capture_with("note", "fixture:standing", "carry-me", "Retained evidence.", json!({"title":"after"}), user).await;
    assert_ne!(carried, first, "a metadata-only change mints a new version");
    let status: String = sqlx::query_scalar("SELECT extraction_status FROM source_versions WHERE version_id=$1").bind(carried).fetch_one(f.owner()).await.unwrap();
    assert_eq!(status, "done", "byte-identical content is carried, not queued");
    assert_eq!(clock_basis(&f, carried).await, ("carried_forward".into(), false), "capture must receipt a carry");
    let coverage = |report: &health::PlaneReport| -> Vec<(ClockClass, Option<String>, i64)> {
        report.clock_coverage.clone().unwrap().into_iter().filter(|row| row.graph_id == f.graph).map(|row| (row.class, row.hold_basis, row.captures)).collect()
    };
    let report = reporter::measure(&f.worker, &f.control.app, chrono::Utc::now()).await.unwrap();
    let classes = coverage(&report);
    assert!(classes.contains(&(ClockClass::Carried, Some("carried_forward".into()), 1)), "{classes:?}");
    assert!(classes.iter().all(|(class, _, _)| *class != ClockClass::Unknown), "{classes:?}");
    let lag = report.lag.iter().find(|row| row.graph_id == f.graph).unwrap();
    assert_eq!(lag.processed, 1, "a carry waited for nothing and is not a lag sample");
    let verdict = health::verdict(&[report.clone()], &[report.tenant.clone()], report.measured_at);
    assert!(verdict.unknown_clock.is_empty() && !verdict.slo_breached(), "{verdict:?}");

    // A carry captured before capture receipted carries: no receipt, unknown.
    // The one-shot repair receipts it, from the same facts, once.
    sqlx::query("DELETE FROM corpus_capture_admissions WHERE source_version_id=$1").bind(carried).execute(f.owner()).await.unwrap();
    let report = reporter::measure(&f.worker, &f.control.app, chrono::Utc::now()).await.unwrap();
    assert!(coverage(&report).contains(&(ClockClass::Unknown, None, 1)), "an unreceipted carry is red");
    let preview = source_carry_forward::preview_unreceipted_carries(f.owner(), Some(f.graph), 100).await.unwrap();
    assert_eq!((preview.versions, preview.other_unreceipted), (1, 0));
    assert_eq!(source_carry_forward::receipt_carried(f.owner(), Some(f.graph), 100).await.unwrap(), (1, 1));
    assert_eq!(source_carry_forward::receipt_carried(f.owner(), Some(f.graph), 100).await.unwrap(), (0, 0), "idempotent");
    assert_eq!(clock_basis(&f, carried).await, ("carried_forward".into(), false));
    let report = reporter::measure(&f.worker, &f.control.app, chrono::Utc::now()).await.unwrap();
    assert!(coverage(&report).contains(&(ClockClass::Carried, Some("carried_forward".into()), 1)));

    // The label comes from the source facts, not the caller: a done version
    // that is not a carry gets an unverified (red) clock from the same path.
    let other = f.capture("note", "not-a-carry", "Different evidence.").await;
    sqlx::query("DELETE FROM corpus_capture_admissions WHERE source_version_id=$1").bind(other).execute(f.owner()).await.unwrap();
    sqlx::query("UPDATE source_versions SET extraction_status='done' WHERE version_id=$1").bind(other).execute(f.owner()).await.unwrap();
    let mut tx = match f.worker.tenant {
        TenantId::Consumer => scope::begin_account_scoped(&f.worker.pool, f.user.account_id).await.unwrap(),
        TenantId::Org(_) => scope::begin_tenant_system_scoped(&f.worker.pool, f.user.account_id).await.unwrap(),
    };
    coralswarm_backend::corpus_enrichment::record_carried_receipt_tx(&mut tx, f.graph, other, f.user.account_id).await.unwrap();
    tx.commit().await.unwrap();
    assert_eq!(clock_basis(&f, other).await, ("unverified_transition".into(), false));
    let preview = source_carry_forward::preview_unreceipted_carries(f.owner(), Some(f.graph), 100).await.unwrap();
    assert_eq!((preview.versions, preview.other_unreceipted), (0, 0));

    // A pre-clock receipt (migration 0142's backfill shape: processed with no
    // processed_at) is legacy, not unknown; a state contradiction still is.
    sqlx::query("ALTER TABLE corpus_capture_admissions DISABLE TRIGGER corpus_admission_observation_immutable").execute(f.owner()).await.unwrap();
    sqlx::query("UPDATE corpus_capture_admissions SET hold_basis='legacy_unknown',processed_at=NULL WHERE source_version_id=$1").bind(first).execute(f.owner()).await.unwrap();
    let report = reporter::measure(&f.worker, &f.control.app, chrono::Utc::now()).await.unwrap();
    assert!(coverage(&report).contains(&(ClockClass::Legacy, Some("legacy_unknown".into()), 1)), "{:?}", coverage(&report));
    let verdict = health::verdict(&[report.clone()], &[report.tenant.clone()], report.measured_at);
    assert!(verdict.unknown_clock.iter().all(|(_, row)| row.hold_basis.as_deref() != Some("legacy_unknown")), "{verdict:?}");
    sqlx::query("UPDATE corpus_capture_admissions SET state='blocked' WHERE source_version_id=$1").bind(first).execute(f.owner()).await.unwrap();
    sqlx::query("ALTER TABLE corpus_capture_admissions ENABLE TRIGGER corpus_admission_observation_immutable").execute(f.owner()).await.unwrap();
    let report = reporter::measure(&f.worker, &f.control.app, chrono::Utc::now()).await.unwrap();
    assert!(coverage(&report).contains(&(ClockClass::Unknown, Some("legacy_unknown".into()), 1)), "a done source with a blocked legacy receipt is red");
    assert_eq!(http.load(Ordering::SeqCst), calls, "the carry cost no model call");
    server.abort();
    f.done().await;
}
#[tokio::test]
async fn consumer_carried_and_legacy_receipts_have_a_clock_class() { carried_and_legacy_receipts_have_a_clock_class(false).await; }
#[tokio::test]
async fn tenant_carried_and_legacy_receipts_have_a_clock_class() { carried_and_legacy_receipts_have_a_clock_class(true).await; }

#[tokio::test]
async fn consumer_adopted_capture_clock_labels() { adopted_capture_clock_labels(false, false).await; }
#[tokio::test]
async fn tenant_adopted_capture_clock_labels() { adopted_capture_clock_labels(true, false).await; }
#[tokio::test]
async fn tenant_owner_adopted_capture_clock_labels() { adopted_capture_clock_labels(true, true).await; }

/// Production on 2026-10-21: every standing claim, model-call admission and
/// publication of a graph took its run row `FOR UPDATE` with a 500 ms lock
/// timeout, so the eight windows of a tick queued on one row behind whichever
/// was publishing. In 6 hours on the org tenant 611 of 2,233 standing windows
/// called the model, timed out at publication and were thrown away, and
/// 6.6 % of claims failed outright. Fences now share the row: another
/// window's claim and model call go ahead while one window publishes, and
/// publications of one graph still take turns.
async fn standing_fences_of_one_graph_do_not_exclude_each_other(org: bool, owner_runtime: bool) {
    use coralswarm_backend::test_standing_probe::{pause_at, Stage};
    let f = Fixture::create(org, false, owner_runtime).await;
    let _concurrency = EnvGuard::set(source_atoms::CONCURRENCY_ENV, "2");
    let first = f.capture("note", "fence-first", "Retained evidence.").await;
    let second = f.capture("note", "fence-second", "Retained evidence.").await;
    f.sweep().await;
    let (a, b) = (pending_job(&f, first).await, pending_job(&f, second).await);
    // Production pools hold several connections; the fixture's app pools hold one.
    let widen = |pool: &PgPool| {
        let options = pool.connect_options().as_ref().clone();
        async move { PgPoolOptions::new().max_connections(6).acquire_timeout(Duration::from_secs(5)).connect_with(options).await.unwrap() }
    };
    let worker = WorkerTenant { tenant: f.worker.tenant.clone(), pool: widen(&f.worker.pool).await, store: f.worker.store.clone() };
    let control = widen(&f.control.app).await;
    let a_fenced = pause_at(a, Stage::PublicationFenced);
    let b_claimed = pause_at(b, Stage::Claimed);
    let b_publication = pause_at(b, Stage::BeforePublication);
    let (llm, http, server) = provider().await;
    let task = {
        let (worker, control, llm) = (worker.clone(), control.clone(), Arc::new(llm));
        tokio::spawn(async move { source_atoms::tick(&worker, &control, llm.as_ref(), None).await.unwrap(); })
    };
    wait_probe(&a_fenced).await;
    tokio::time::timeout(Duration::from_secs(20), b_claimed.reached()).await
        .expect("a claim must not wait for another window's publication fence");
    b_claimed.release();
    tokio::time::timeout(Duration::from_secs(20), b_publication.reached()).await
        .expect("a window's fences and model call must not wait for another window's publication");
    b_publication.release();
    tokio::time::sleep(Duration::from_millis(800)).await;
    let windows = |version: Uuid| {
        let pool = f.owner().clone();
        async move {
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM semantic_atom_windows WHERE source_version_id=$1")
                .bind(version).fetch_one(&pool).await.unwrap()
        }
    };
    assert_eq!(windows(second).await, 0, "publications of one graph take turns");
    a_fenced.release();
    finish_tick(task).await;
    assert_eq!((windows(first).await, windows(second).await), (1, 1), "both paid windows are published");
    assert_eq!(http.load(Ordering::SeqCst), 2, "no window called the model twice");
    let failed: Vec<(String, Option<String>)> = sqlx::query_as("SELECT status::text,error_message FROM jobs WHERE id=ANY($1) AND (status::text<>'done' OR error_message IS NOT NULL)")
        .bind(vec![a, b]).fetch_all(f.owner()).await.unwrap();
    assert!(failed.is_empty(), "{failed:?}");
    println!("standing_fences org={org} owner_runtime={owner_runtime}");
    worker.pool.close().await;
    control.close().await;
    server.abort();
    f.done().await;
}
#[tokio::test]
async fn consumer_standing_fences_of_one_graph_do_not_exclude_each_other() { standing_fences_of_one_graph_do_not_exclude_each_other(false, false).await; }
#[tokio::test]
async fn tenant_standing_fences_of_one_graph_do_not_exclude_each_other() { standing_fences_of_one_graph_do_not_exclude_each_other(true, false).await; }
#[tokio::test]
async fn tenant_owner_standing_fences_of_one_graph_do_not_exclude_each_other() { standing_fences_of_one_graph_do_not_exclude_each_other(true, true).await; }

/// Production on 2026-10-20: about 1,300 Slack messages more than a week old
/// were re-captured in one afternoon. Each got a new `captured_at`, so the
/// fresh lane served the burst newest-first, ahead of live messages in the
/// same pair, and the freshness SLO counted every one as a capture someone
/// was waiting on (p95 24,736 s for that graph). History drains with the
/// backlog and is not an SLO sample.
async fn recaptured_history_is_backlog_not_a_fresh_capture(org: bool, owner_runtime: bool) {
    use coralswarm_backend::workers::capture_admission_health as reporter;
    let f = Fixture::create(org, false, owner_runtime).await;
    let _concurrency = EnvGuard::set(source_atoms::CONCURRENCY_ENV, "2");
    let backlog = f.capture("slack", "recapture-backlog", "Retained evidence.").await;
    f.sweep().await;
    sqlx::query("UPDATE source_versions SET captured_at=now()-interval '3 days' WHERE version_id=$1")
        .bind(backlog).execute(f.owner()).await.unwrap();
    let live = f.capture("slack", "recapture-live", "Retained evidence.").await;
    let history = f.capture("slack", "recapture-history", "Retained evidence.").await;
    sqlx::query("UPDATE source_versions SET captured_at=now()-interval '1 minute' WHERE version_id=$1")
        .bind(live).execute(f.owner()).await.unwrap();
    sqlx::query("UPDATE source_versions SET occurred_at=captured_at-interval '9 days' WHERE version_id=$1")
        .bind(history).execute(f.owner()).await.unwrap();
    f.sweep().await;
    let done = |versions: Vec<Uuid>| {
        let pool = f.owner().clone();
        async move {
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM source_versions WHERE version_id=ANY($1) AND extraction_status='done'")
                .bind(versions).fetch_one(&pool).await.unwrap()
        }
    };
    let (llm, _hits, server) = provider().await;
    source_atoms::tick(&f.worker, &f.control.app, &llm, None).await.unwrap();
    assert_eq!(done(vec![live]).await, 1, "the live capture takes the fresh slot, though the re-captured history is newer");
    assert_eq!(done(vec![history]).await, 0);
    assert_eq!(done(vec![backlog]).await, 1, "the oldest-first slot still serves the backlog");
    source_atoms::tick(&f.worker, &f.control.app, &llm, None).await.unwrap();
    assert_eq!(done(vec![history]).await, 1, "history drains with the backlog");
    let report = reporter::measure(&f.worker, &f.control.app, chrono::Utc::now()).await.unwrap();
    let samples: Vec<i64> = report.lag.iter().filter(|row| row.graph_id == f.graph).map(|row| row.processed).collect();
    // The backlog capture's receipt was observed when the fixture captured it,
    // so it is a sample too; the re-captured history is not.
    assert_eq!(samples, vec![2], "re-captured history is not a freshness sample");
    println!("recaptured_history org={org} owner_runtime={owner_runtime}");
    server.abort();
    f.done().await;
}
#[tokio::test]
async fn consumer_recaptured_history_is_backlog_not_a_fresh_capture() { recaptured_history_is_backlog_not_a_fresh_capture(false, false).await; }
#[tokio::test]
async fn tenant_recaptured_history_is_backlog_not_a_fresh_capture() { recaptured_history_is_backlog_not_a_fresh_capture(true, false).await; }
#[tokio::test]
async fn tenant_owner_recaptured_history_is_backlog_not_a_fresh_capture() { recaptured_history_is_backlog_not_a_fresh_capture(true, true).await; }

/// The pause contract under shared fences (#1411). A fence holds the run row
/// `FOR SHARE` until it commits, so an operator pause issued while a window is
/// inside its publication fence waits for that window: the window that was
/// already admitted publishes, and the pause commits after it. A fence opened
/// after the pause sees it: a window whose model call was admitted before the
/// pause is held at publication, not published, and no new window calls the
/// model. Fences no longer exclude each other, so this pins what the
/// `FOR UPDATE` fence used to guarantee by construction.
async fn operator_pause_during_open_fences(org: bool, owner_runtime: bool) {
    use coralswarm_backend::test_standing_probe::{pause_at, Stage};
    let f = Fixture::create(org, false, owner_runtime).await;
    let _concurrency = EnvGuard::set(source_atoms::CONCURRENCY_ENV, "2");
    let first = f.capture("note", "pause-fence-first", "Retained evidence.").await;
    let second = f.capture("note", "pause-fence-second", "Retained evidence.").await;
    f.sweep().await;
    let run = f.run().await;
    let (a, b) = (pending_job(&f, first).await, pending_job(&f, second).await);
    let widen = |pool: &PgPool| {
        let options = pool.connect_options().as_ref().clone();
        async move { PgPoolOptions::new().max_connections(6).acquire_timeout(Duration::from_secs(5)).connect_with(options).await.unwrap() }
    };
    let worker = WorkerTenant { tenant: f.worker.tenant.clone(), pool: widen(&f.worker.pool).await, store: f.worker.store.clone() };
    let control = widen(&f.control.app).await;
    let a_fenced = pause_at(a, Stage::PublicationFenced);
    let b_publication = pause_at(b, Stage::BeforePublication);
    let (llm, http, server) = provider().await;
    let llm = Arc::new(llm);
    let task = {
        let (worker, control, llm) = (worker.clone(), control.clone(), llm.clone());
        tokio::spawn(async move { source_atoms::tick(&worker, &control, llm.as_ref(), None).await.unwrap(); })
    };
    wait_probe(&a_fenced).await;
    wait_probe(&b_publication).await;
    assert_eq!(http.load(Ordering::SeqCst), 2, "both windows were admitted and called the model before the pause");
    let pause = {
        let (pool, tenant, account, graph) = (worker.pool.clone(), worker.tenant.clone(), f.user.account_id, f.graph);
        tokio::spawn(async move {
            let mut tx = match tenant {
                TenantId::Consumer => scope::begin_account_scoped(&pool, account).await.unwrap(),
                TenantId::Org(_) => scope::begin_tenant_system_scoped(&pool, account).await.unwrap(),
            };
            coralswarm_backend::corpus_enrichment::test_standing_pause_tx(&mut tx, graph, run, "operator_pause").await.unwrap();
            tx.commit().await.unwrap();
        })
    };
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert!(!pause.is_finished(), "a pause must wait for the window inside its publication fence");
    let state = |pool: PgPool| async move {
        sqlx::query_scalar::<_, String>("SELECT standing_pause_state FROM corpus_enrichment_runs WHERE run_id=$1")
            .bind(run).fetch_one(&pool).await.unwrap()
    };
    assert_eq!(state(f.owner().clone()).await, "none");
    a_fenced.release();
    tokio::time::timeout(SPAWNED_TICK_LIVENESS, pause).await.expect("the pause commits once the open fence does").unwrap();
    assert_eq!(state(f.owner().clone()).await, "operator");
    b_publication.release();
    finish_tick(task).await;
    let windows = |version: Uuid| {
        let pool = f.owner().clone();
        async move {
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM semantic_atom_windows WHERE source_version_id=$1")
                .bind(version).fetch_one(&pool).await.unwrap()
        }
    };
    assert_eq!(windows(first).await, 1, "the window fenced before the pause publishes");
    assert_eq!(windows(second).await, 0, "a publication fence opened after the pause does not publish");
    let held: (String, Option<String>) = sqlx::query_as("SELECT state,reason FROM corpus_enrichment_units WHERE holding_id=$1 AND execution_owner")
        .bind(second).fetch_one(f.owner()).await.unwrap();
    assert_eq!(held, ("pending".into(), Some("operator_paused".into())), "the paused window is held, not stranded");
    for _ in 0..2 {
        source_atoms::tick(&worker, &control, llm.as_ref(), None).await.unwrap();
    }
    assert_eq!(http.load(Ordering::SeqCst), 2, "nothing calls the model after the pause");
    assert_eq!(windows(second).await, 0);
    println!("operator_pause_during_open_fences org={org} owner_runtime={owner_runtime}");
    worker.pool.close().await;
    control.close().await;
    server.abort();
    f.done().await;
}
#[tokio::test]
async fn consumer_operator_pause_during_open_fences() { operator_pause_during_open_fences(false, false).await; }
#[tokio::test]
async fn tenant_operator_pause_during_open_fences() { operator_pause_during_open_fences(true, false).await; }
#[tokio::test]
async fn tenant_owner_operator_pause_during_open_fences() { operator_pause_during_open_fences(true, true).await; }

/// #1411 follow-up: with fences sharing the run row, a pause `UPDATE` waits
/// for every share holder while new `FOR SHARE` requests are granted beside
/// them, so fences that keep overlapping held an operator pause off for as
/// long as they kept coming (7.57 s measured on PG17). Fences take the
/// standing gate shared and writers take it exclusively; heavyweight locks
/// queue, so the pause lands once the fences already open commit.
async fn operator_pause_lands_while_fences_keep_overlapping(org: bool, owner_runtime: bool) {
    let f = Fixture::create(org, false, owner_runtime).await;
    f.capture("note", "gate-fair", "Retained evidence.").await;
    f.sweep().await;
    let run = f.run().await;
    let options = f.worker.pool.connect_options().as_ref().clone();
    let pool = PgPoolOptions::new().max_connections(8).acquire_timeout(Duration::from_secs(5)).connect_with(options).await.unwrap();
    let (tenant, account, graph) = (f.worker.tenant.clone(), f.user.account_id, f.graph);
    let begin = move |pool: PgPool, tenant: TenantId| async move {
        match tenant {
            TenantId::Consumer => scope::begin_account_scoped(&pool, account).await.unwrap(),
            TenantId::Org(_) => scope::begin_tenant_system_scoped(&pool, account).await.unwrap(),
        }
    };
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut fences = Vec::new();
    for i in 0..3u64 {
        let (pool, tenant, stop) = (pool.clone(), tenant.clone(), stop.clone());
        fences.push(tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100 * i)).await;
            let mut opened = 0;
            while !stop.load(Ordering::SeqCst) {
                let mut tx = begin(pool.clone(), tenant.clone()).await;
                coralswarm_backend::corpus_enrichment::test_standing_fence_tx(&mut tx, graph, run).await.unwrap();
                tokio::time::sleep(Duration::from_millis(300)).await;
                tx.commit().await.unwrap();
                opened += 1;
            }
            opened
        }));
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    let started = std::time::Instant::now();
    let pause = {
        let (pool, tenant) = (pool.clone(), tenant.clone());
        tokio::spawn(async move {
            let mut tx = begin(pool, tenant).await;
            coralswarm_backend::corpus_enrichment::test_standing_pause_tx(&mut tx, graph, run, "operator_pause").await.unwrap();
            tx.commit().await.unwrap();
        })
    };
    let landed = tokio::time::timeout(Duration::from_secs(8), pause).await;
    let waited = started.elapsed();
    stop.store(true, Ordering::SeqCst);
    let mut opened = 0;
    for fence in fences {
        opened += fence.await.unwrap();
    }
    landed.expect("the pause never landed while fences kept overlapping").unwrap();
    assert!(waited < Duration::from_secs(2), "an operator pause waits only for the fences already open, not for every later one: waited {waited:?}");
    assert!(opened >= 6, "the fences kept overlapping throughout ({opened} opened)");
    let state: String = sqlx::query_scalar("SELECT standing_pause_state FROM corpus_enrichment_runs WHERE run_id=$1")
        .bind(run).fetch_one(f.owner()).await.unwrap();
    assert_eq!(state, "operator");
    println!("operator_pause_overlap org={org} owner_runtime={owner_runtime} waited_ms={}", waited.as_millis());
    pool.close().await;
    f.done().await;
}
#[tokio::test]
async fn consumer_operator_pause_lands_while_fences_keep_overlapping() { operator_pause_lands_while_fences_keep_overlapping(false, false).await; }
#[tokio::test]
async fn tenant_operator_pause_lands_while_fences_keep_overlapping() { operator_pause_lands_while_fences_keep_overlapping(true, false).await; }
#[tokio::test]
async fn tenant_owner_operator_pause_lands_while_fences_keep_overlapping() { operator_pause_lands_while_fences_keep_overlapping(true, true).await; }

/// #1411 follow-up: an edit is a live capture even when the source keeps the
/// original time. A Slack message edited days after it was posted keeps its
/// `ts`, so `occurred_at` alone classed the edit as history and it waited
/// with the backlog. A new version whose content differs from the item's
/// previous version is live; a re-capture with the same content is not.
async fn an_edit_of_old_content_is_a_live_capture(org: bool, owner_runtime: bool) {
    let f = Fixture::create(org, false, owner_runtime).await;
    let _concurrency = EnvGuard::set(source_atoms::CONCURRENCY_ENV, "2");
    let mut backlog = Vec::new();
    for i in 0..2 {
        backlog.push(f.capture("slack", &format!("edit-backlog-{i}"), "Retained evidence.").await);
    }
    f.sweep().await;
    sqlx::query("UPDATE source_versions SET captured_at=now()-interval '3 days' WHERE version_id=ANY($1)")
        .bind(&backlog).execute(f.owner()).await.unwrap();
    // The first versions of both items are old and finished.
    let posted = f.capture("slack", "edited-message", "Retained evidence.").await;
    let recaptured = f.capture_with("slack", "fixture:standing", "recaptured-message", "Retained evidence.", json!({"anchor": "none"}), Some(f.user.user_id)).await;
    sqlx::query("UPDATE source_versions SET captured_at=now()-interval '6 days',occurred_at=now()-interval '6 days',extraction_status='failed' WHERE version_id=ANY($1)")
        .bind(vec![posted, recaptured]).execute(f.owner()).await.unwrap();
    let edit = f.capture("slack", "edited-message", "Retained evidence. Edited later.").await;
    let history = f.capture_with("slack", "fixture:standing", "recaptured-message", "Retained evidence.", json!({"anchor": "added"}), Some(f.user.user_id)).await;
    assert!(edit != posted && history != recaptured, "both are new versions");
    sqlx::query("UPDATE source_versions SET occurred_at=now()-interval '6 days',captured_at=now()-interval '2 minutes' WHERE version_id=$1")
        .bind(edit).execute(f.owner()).await.unwrap();
    sqlx::query("UPDATE source_versions SET occurred_at=now()-interval '6 days' WHERE version_id=$1")
        .bind(history).execute(f.owner()).await.unwrap();
    f.sweep().await;
    let done = |versions: Vec<Uuid>| {
        let pool = f.owner().clone();
        async move {
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM source_versions WHERE version_id=ANY($1) AND extraction_status='done'")
                .bind(versions).fetch_one(&pool).await.unwrap()
        }
    };
    let (llm, _hits, server) = provider().await;
    source_atoms::tick(&f.worker, &f.control.app, &llm, None).await.unwrap();
    assert_eq!(done(vec![edit]).await, 1, "the edit takes the fresh slot");
    assert_eq!(done(vec![history]).await, 0, "a same-content re-capture of old content is backlog");
    assert_eq!(done(backlog.clone()).await, 1, "the oldest-first slot still serves the backlog");
    println!("edit_is_live org={org} owner_runtime={owner_runtime}");
    server.abort();
    f.done().await;
}
#[tokio::test]
async fn consumer_an_edit_of_old_content_is_a_live_capture() { an_edit_of_old_content_is_a_live_capture(false, false).await; }
#[tokio::test]
async fn tenant_an_edit_of_old_content_is_a_live_capture() { an_edit_of_old_content_is_a_live_capture(true, false).await; }
#[tokio::test]
async fn tenant_owner_an_edit_of_old_content_is_a_live_capture() { an_edit_of_old_content_is_a_live_capture(true, true).await; }

// ── #1516: same title and kind is one reef, except for people ─────────────

impl Fixture {
    /// The worker's own transaction shape for this plane: the app role on
    /// consumer and tenant shapes, the owner on the tenant-owner shape (the
    /// prod tenant runtime).
    async fn worker_tx(&self) -> sqlx::Transaction<'static, sqlx::Postgres> {
        match self.worker.tenant {
            TenantId::Consumer => scope::begin_account_scoped(&self.worker.pool, self.user.account_id).await.unwrap(),
            TenantId::Org(_) => scope::begin_tenant_system_scoped(&self.worker.pool, self.user.account_id).await.unwrap(),
        }
    }
}

fn door_ctx(entity_type: &str, version: Uuid) -> coralswarm_backend::store::ReefRefCtx {
    coralswarm_backend::store::ReefRefCtx {
        atom_id: Some(version),
        entity_type: Some(entity_type.into()),
        worker: "source_atoms_v3",
        ..Default::default()
    }
}

/// One mention through the door, the way the v2/v3 binder sees it: the
/// ladder's answer, then the mint (which a v3 `Unknown`, Jev `new` or the
/// sweep reaches), committed.
async fn door_mention(f: &Fixture, title: &str, entity_type: &str) -> (coralswarm_backend::store::ReefResolution, Uuid) {
    use coralswarm_backend::store::Store;
    let ctx = door_ctx(entity_type, Uuid::new_v4());
    let mut tx = f.worker_tx().await;
    let resolution = f.worker.store.resolve_reef_ref_scoped(&mut tx, f.graph, "entity", title, &ctx).await.unwrap();
    let minted = f.worker.store.mint_reef_scoped(&mut tx, f.graph, "entity", title, &ctx).await.unwrap();
    // The binder records the mention's type on the reef it linked
    // (`record_entity_type_hint_scoped`); a bound reef must keep its kind.
    f.worker.store.record_entity_type_hint_scoped(&mut tx, minted, entity_type).await.unwrap();
    tx.commit().await.unwrap();
    (resolution, minted)
}

/// A reef's Library kind (`nodes.entity_type`) and its `body.entity_type`.
async fn reef_kind(f: &Fixture, node: Uuid) -> (Option<String>, Option<String>) {
    sqlx::query_as("SELECT entity_type, body->>'entity_type' FROM nodes WHERE node_id=$1").bind(node).fetch_one(f.owner()).await.unwrap()
}

fn kind(k: &str) -> (Option<String>, Option<String>) {
    (Some(k.into()), Some(k.into()))
}

fn resolved_to(r: &coralswarm_backend::store::ReefResolution) -> Option<Uuid> {
    match r {
        coralswarm_backend::store::ReefResolution::Resolved { node_id, .. } => Some(*node_id),
        _ => None,
    }
}

/// The founder's rules (2026-10-27): a second mint of a non-person title in
/// its identity class reuses the reef, wherever its first evidence came
/// from. `topic` and `project` are one class (decision 5): a project mention
/// binds a same-title topic reef and the reverse, and the reef keeps its own
/// (older) kind. `org` is its own class. A person mint with the same name
/// does not reuse, and keeps its per-evidence scope. The prod cause is
/// covered too: same-title topic copies a model judged "different" still
/// bind the oldest, and no third copy is minted.
async fn non_person_same_title_reuses_and_person_does_not(shape: V2Shape) {
    let f = v2_fixture(&shape).await;
    let (first, topic) = door_mention(&f, "Atlas", "topic").await;
    assert_eq!(resolved_to(&first), None, "a new title resolves to nothing");
    let (second, topic_again) = door_mention(&f, "atlas", "topic").await;
    assert_eq!(resolved_to(&second), Some(topic), "the ladder binds the same title and kind from other evidence");
    assert_eq!(topic_again, topic, "the mint backstop reuses it too");
    // Decision 5: a project mention of a topic's title is that topic.
    let (project_res, project) = door_mention(&f, "Atlas", "project").await;
    assert_eq!((resolved_to(&project_res), project), (Some(topic), topic), "a project mention reuses the same-title topic reef");
    assert_eq!(reef_kind(&f, topic).await, kind("topic"), "the reused reef keeps its older kind");
    // And the reverse: a topic mention of a project's title.
    let (_, orion) = door_mention(&f, "Orion", "project").await;
    let (orion_res, orion_again) = door_mention(&f, "orion", "topic").await;
    assert_eq!((resolved_to(&orion_res), orion_again), (Some(orion), orion), "a topic mention reuses the same-title project reef");
    assert_eq!(reef_kind(&f, orion).await, kind("project"), "the project keeps kind project");
    // `org` is its own name space: neither way across topic/project.
    let (atlas_org_res, atlas_org) = door_mention(&f, "Atlas", "org").await;
    assert_eq!(resolved_to(&atlas_org_res), None, "an org mention does not resolve to a same-title topic");
    assert_ne!(atlas_org, topic, "an org mention does not reuse a same-title topic reef");
    assert_eq!(reef_kind(&f, atlas_org).await, kind("org"));
    let (_, orion_org) = door_mention(&f, "Orion", "org").await;
    assert_ne!(orion_org, orion, "an org mention does not reuse a same-title project reef");
    let (_, org) = door_mention(&f, "Initrode", "org").await;
    let (org_res, org_again) = door_mention(&f, "Initrode", "org").await;
    assert_eq!((resolved_to(&org_res), org_again), (Some(org), org));
    let (initrode_project_res, initrode_project) = door_mention(&f, "Initrode", "project").await;
    assert_ne!((resolved_to(&initrode_project_res), initrode_project), (Some(org), org), "a project mention does not reuse a same-title org");
    let (_, initrode_topic) = door_mention(&f, "initrode", "topic").await;
    assert_eq!(initrode_topic, initrode_project, "the topic mention joins the project, not the org");
    assert_eq!(reef_kind(&f, org).await, kind("org"));

    let (_, alex) = door_mention(&f, "Alex Kim", "person").await;
    let (person_res, alex_again) = door_mention(&f, "Alex Kim", "person").await;
    assert_ne!(resolved_to(&person_res), Some(alex), "a person name from other evidence is not that person");
    assert_ne!(alex_again, alex, "a person mint keeps per-evidence identity");

    let scope_of = |node: Uuid| {
        let f = &f;
        async move {
            sqlx::query_as::<_, (Uuid, Option<String>)>("SELECT identity_scope,body->>'identity_status' FROM nodes WHERE node_id=$1")
                .bind(node).fetch_one(f.owner()).await.unwrap()
        }
    };
    let class_scope = |class: &str| {
        let f = &f;
        let class = class.to_string();
        async move {
            sqlx::query_scalar::<_, Uuid>("SELECT md5($1 || $2)::uuid")
                .bind(coralswarm_backend::store::DOOR_TITLE_CLASS_SCOPE_PREFIX).bind(class).fetch_one(f.owner()).await.unwrap()
        }
    };
    assert_eq!(scope_of(topic).await, (class_scope("topic_project").await, None), "a first non-person mint takes the class's scope, no ER flag");
    assert_eq!(scope_of(orion).await.0, class_scope("topic_project").await, "a project mint takes the same topic/project scope");
    assert_eq!(scope_of(atlas_org).await.0, class_scope("org").await, "an org takes its own class scope");
    let (alex_scope, alex_status) = scope_of(alex).await;
    assert!(alex_scope != Uuid::nil() && alex_scope != class_scope("person").await, "person scope is the evidence id");
    assert_eq!(alex_status.as_deref(), Some("unresolved"));

    // The prod shape: per-evidence topic copies a model judged different.
    let copy = |age_days: i32| {
        let f = &f;
        async move {
            sqlx::query_scalar::<_, Uuid>("INSERT INTO nodes(graph_id,type,title,entity_type,body,identity_scope,created_at) VALUES($1,'entity','Zod','topic',jsonb_build_object('entity_type','topic','identity_status','unresolved'),gen_random_uuid(),now()-make_interval(days=>$2)) RETURNING node_id")
                .bind(f.graph).bind(age_days).fetch_one(f.owner()).await.unwrap()
        }
    };
    let (zod_old, zod_new) = (copy(9).await, copy(1).await);
    let (lo, hi) = if zod_old < zod_new { (zod_old, zod_new) } else { (zod_new, zod_old) };
    sqlx::query("INSERT INTO reef_judgement(graph_id,node_a_id,node_b_id,verdict,source,evidence) VALUES($1,$2,$3,'negative','llm','{\"llm_verdict\":\"different\"}')")
        .bind(f.graph).bind(lo).bind(hi).execute(f.owner()).await.unwrap();
    let (zod_res, zod) = door_mention(&f, "Zod", "topic").await;
    assert_eq!((resolved_to(&zod_res), zod), (Some(zod_old), zod_old), "a model verdict does not split a topic: the oldest copy binds");

    // A reef whose `nodes.entity_type` the hint has not set yet is still
    // found under its folded raw type: `Technology` and `tool` are both topic.
    let kafka: Uuid = sqlx::query_scalar("INSERT INTO nodes(graph_id,type,title,body,identity_scope) VALUES($1,'entity','Kafka','{\"entity_type\":\"Technology\"}',gen_random_uuid()) RETURNING node_id")
        .bind(f.graph).fetch_one(f.owner()).await.unwrap();
    let (kafka_res, kafka_again) = door_mention(&f, "kafka", "tool").await;
    assert_eq!((resolved_to(&kafka_res), kafka_again), (Some(kafka), kafka), "the raw body type folds like library_kind");
    // A project mention binding a raw-typed topic reef must not retype it:
    // the hint folds the reef's own raw type before it falls back to the
    // mention's.
    let redis: Uuid = sqlx::query_scalar("INSERT INTO nodes(graph_id,type,title,body,identity_scope) VALUES($1,'entity','Redis','{\"entity_type\":\"Technology\"}',gen_random_uuid()) RETURNING node_id")
        .bind(f.graph).fetch_one(f.owner()).await.unwrap();
    let (_, redis_again) = door_mention(&f, "Redis", "project").await;
    assert_eq!(redis_again, redis);
    assert_eq!(reef_kind(&f, redis).await, kind("topic"), "a raw `Technology` reef stays topic when a project mention binds it");

    let count = |title: &str| {
        let f = &f;
        let title = title.to_string();
        async move {
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM nodes WHERE graph_id=$1 AND type='entity' AND lower(title)=lower($2)")
                .bind(f.graph).bind(title).fetch_one(f.owner()).await.unwrap()
        }
    };
    assert_eq!(
        (count("Atlas").await, count("Orion").await, count("Initrode").await, count("Zod").await, count("Alex Kim").await, count("Kafka").await, count("Redis").await),
        (2, 2, 2, 2, 2, 1, 1),
        "Atlas topic+org, Orion project+org, Initrode org+project"
    );
    f.done().await;
}
#[tokio::test]
async fn consumer_non_person_same_title_reuses_and_person_does_not() { non_person_same_title_reuses_and_person_does_not(V2Shape::Consumer).await; }
#[tokio::test]
async fn tenant_non_person_same_title_reuses_and_person_does_not() { non_person_same_title_reuses_and_person_does_not(V2Shape::Tenant).await; }
#[tokio::test]
async fn tenant_owner_non_person_same_title_reuses_and_person_does_not() { non_person_same_title_reuses_and_person_does_not(V2Shape::TenantOwner).await; }

/// Two concurrent first mints of one non-person title fold into one reef:
/// the class scope makes the composite unique index the arbiter, and the
/// loser re-reads the winner instead of minting a copy. `kinds` are the two
/// mentions' types: a topic mint racing a project mint shares the
/// topic/project class scope, so it folds too, into the winner's kind. Runs
/// on the worker's own pool (the app role, except on the tenant-owner shape).
async fn concurrent_first_mints_fold(shape: V2Shape, kinds: (&'static str, &'static str)) {
    use coralswarm_backend::store::Store;
    let f = v2_fixture(&shape).await;
    let owner = f.owner().clone();
    let worker_pool = f.worker.pool.clone();
    let store = Arc::new(PostgresStore::new(worker_pool.clone()));
    let begin = |pool: PgPool, tenant: TenantId, account: Uuid| async move {
        match tenant {
            TenantId::Consumer => scope::begin_account_scoped(&pool, account).await.unwrap(),
            TenantId::Org(_) => scope::begin_tenant_system_scoped(&pool, account).await.unwrap(),
        }
    };
    let mut first = begin(worker_pool.clone(), f.worker.tenant.clone(), f.user.account_id).await;
    let winner = store.mint_reef_scoped(&mut first, f.graph, "entity", "Borealis", &door_ctx(kinds.0, Uuid::new_v4())).await.unwrap();
    let (s, pool, tenant, account, graph) = (store.clone(), worker_pool.clone(), f.worker.tenant.clone(), f.user.account_id, f.graph);
    let racer = tokio::spawn(async move {
        let mut tx = begin(pool, tenant, account).await;
        let node = s.mint_reef_scoped(&mut tx, graph, "entity", "borealis", &door_ctx(kinds.1, Uuid::new_v4())).await.unwrap();
        s.record_entity_type_hint_scoped(&mut tx, node, kinds.1).await.unwrap();
        tx.commit().await.unwrap();
        node
    });
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(!racer.is_finished(), "the second mint waits on the first one's key");
    first.commit().await.unwrap();
    assert_eq!(racer.await.unwrap(), winner);
    let n: i64 = sqlx::query_scalar("SELECT count(*) FROM nodes WHERE graph_id=$1 AND lower(title)='borealis'").bind(f.graph).fetch_one(&owner).await.unwrap();
    assert_eq!(n, 1);
    let winner_kind: Option<String> = sqlx::query_scalar("SELECT body->>'entity_type' FROM nodes WHERE node_id=$1").bind(winner).fetch_one(&owner).await.unwrap();
    assert_eq!(winner_kind.as_deref(), Some(kinds.0), "the reef keeps the winner's kind");
    f.done().await;
}
#[tokio::test]
async fn consumer_concurrent_first_mints_fold() { concurrent_first_mints_fold(V2Shape::Consumer, ("project", "project")).await; }
#[tokio::test]
async fn tenant_owner_concurrent_first_mints_fold() { concurrent_first_mints_fold(V2Shape::TenantOwner, ("project", "project")).await; }
#[tokio::test]
async fn consumer_concurrent_topic_and_project_first_mints_fold() { concurrent_first_mints_fold(V2Shape::Consumer, ("topic", "project")).await; }
#[tokio::test]
async fn tenant_concurrent_topic_and_project_first_mints_fold() { concurrent_first_mints_fold(V2Shape::Tenant, ("project", "topic")).await; }
#[tokio::test]
async fn tenant_owner_concurrent_topic_and_project_first_mints_fold() { concurrent_first_mints_fold(V2Shape::TenantOwner, ("topic", "project")).await; }

/// The one-off merge (`dedupe_reefs --rule same-title`): the dry run reads
/// the same plan twice and writes nothing; the real merge folds every
/// non-person same-title group (generic and short titles too, past model
/// verdicts) into the keeper the mint binds, re-points edges, skips people
/// and human "different" rulings, and a second run merges nothing.
async fn same_title_merge_dry_run_and_execute_are_idempotent(shape: V2Shape) {
    use coralswarm_backend::reef_dedupe::{self, Exclusion, Policy, Rule};
    let f = v2_fixture(&shape).await;
    let reef = |title: &'static str, kind: &'static str, age_days: i32| {
        let f = &f;
        async move {
            sqlx::query_scalar::<_, Uuid>("INSERT INTO nodes(graph_id,type,title,entity_type,body,identity_scope,created_at) VALUES($1,'entity',$2,$3,jsonb_build_object('entity_type',$3::text,'identity_status','unresolved'),gen_random_uuid(),now()-make_interval(days=>$4)) RETURNING node_id")
                .bind(f.graph).bind(title).bind(kind).bind(age_days).fetch_one(f.owner()).await.unwrap()
        }
    };
    let negative = |a: Uuid, b: Uuid, source: &'static str| {
        let f = &f;
        async move {
            let (lo, hi) = if a < b { (a, b) } else { (b, a) };
            sqlx::query("INSERT INTO reef_judgement(graph_id,node_a_id,node_b_id,verdict,source,evidence) VALUES($1,$2,$3,'negative',$4,'{\"llm_verdict\":\"different\"}')")
                .bind(f.graph).bind(lo).bind(hi).bind(source).execute(f.owner()).await.unwrap();
        }
    };
    let org_keep = reef("org", "topic", 9).await;
    let org_dups = [reef("Org", "topic", 5).await, reef("ORG", "topic", 1).await];
    // Mixed topic/project groups (decision 5): the oldest keeps its kind.
    let atlas_keep = reef("Atlas", "project", 8).await;
    let atlas_dup = reef("atlas", "project", 2).await;
    let atlas_topic = reef("Atlas", "topic", 3).await;
    let helix_keep = reef("Helix", "topic", 9).await;
    let helix_projects = [reef("Helix", "project", 4).await, reef("HELIX", "project", 1).await];
    let helix_org = reef("Helix", "org", 12).await;
    let alex = [reef("Alex Kim", "person", 7).await, reef("Alex Kim", "person", 2).await];
    let composio = [reef("Composio", "org", 6).await, reef("Composio", "org", 2).await];
    negative(composio[0], composio[1], "llm").await;
    let initech = [reef("Initech", "org", 6).await, reef("Initech", "org", 2).await];
    negative(initech[0], initech[1], "human").await;
    // A copy the kind hint has not reached yet: column NULL, raw `Company`.
    let globex_keep = reef("Globex", "org", 6).await;
    let globex_raw: Uuid = sqlx::query_scalar("INSERT INTO nodes(graph_id,type,title,body,identity_scope,created_at) VALUES($1,'entity','Globex','{\"entity_type\":\"Company\"}',gen_random_uuid(),now()-interval '1 day') RETURNING node_id")
        .bind(f.graph).fetch_one(f.owner()).await.unwrap();
    let edge: Uuid = sqlx::query_scalar("INSERT INTO relationship_edge(graph_id,src_node_id,predicate,dst_node_id,status) VALUES($1,$2,'works_on',$3,'confirmed') RETURNING edge_id")
        .bind(f.graph).bind(alex[0]).bind(atlas_dup).fetch_one(f.owner()).await.unwrap();

    let policy = Policy { graph: Some(f.graph), ..Policy::same_title() };
    assert_eq!(policy.rule, Rule::SameTitle);
    let state = || {
        let f = &f;
        async move {
            sqlx::query_as::<_, (i64, i64, i64)>("SELECT (SELECT count(*) FROM nodes WHERE graph_id=$1),(SELECT count(*) FROM node_merges),(SELECT count(*) FROM reef_judgement WHERE graph_id=$1)")
                .bind(f.graph).fetch_one(f.owner()).await.unwrap()
        }
    };
    let before = state().await;
    let summary = |r: &reef_dedupe::Report| (r.groups_to_merge, r.reefs_to_merge, r.by_entity_type.clone(), r.excluded.clone(), r.by_kinds.clone(), r.by_keeper_kind.clone());
    let dry1 = summary(&reef_dedupe::report(&reef_dedupe::plan(f.owner(), &policy).await.unwrap(), &policy));
    let dry2 = summary(&reef_dedupe::report(&reef_dedupe::plan(f.owner(), &policy).await.unwrap(), &policy));
    println!("same_title dry run={dry1:?}");
    assert_eq!(dry1, dry2, "the dry run is repeatable");
    assert_eq!(state().await, before, "the dry run writes nothing");
    assert_eq!((dry1.0, dry1.1), (5, 8), "org x2, Atlas project+topic x2, Helix topic+project x2, Composio x1, Globex x1");
    assert_eq!(dry1.2.get("topic_project"), Some(&(3, 6)), "topic and project group as one identity class");
    assert_eq!(dry1.2.get("org"), Some(&(2, 2)), "a raw `Company` copy folds into its org group; the Helix org stays alone");
    assert_eq!(dry1.4.get("project+topic"), Some(&(2, 4)), "two mixed groups");
    assert_eq!(dry1.4.get("topic"), Some(&(1, 2)));
    assert_eq!(dry1.5.get("project"), Some(&(1, 2)), "Atlas keeps project");
    assert_eq!(dry1.5.get("topic"), Some(&(2, 4)), "org (topic) and Helix keep topic");
    assert_eq!(dry1.3.get(Exclusion::PersonTitle.as_str()), Some(&(1, 1)), "people are never merged by title");
    assert_eq!(dry1.3.get(Exclusion::NegativeJudgement.as_str()), Some(&(1, 1)), "a human ruling keeps its pair apart");
    let named = Policy { graph: Some(f.graph), cutoff: "2000-01-01T00:00:00Z".parse().unwrap(), include_preexisting: true, entity_types: vec!["topic".into()], ..Policy::default() };
    let named_report = reef_dedupe::report(&reef_dedupe::plan(f.owner(), &named).await.unwrap(), &named);
    assert!(named_report.excluded.contains_key(Exclusion::Stoplist.as_str()), "the old rule still stoplists `org`: {:?}", named_report.excluded);

    let done = reef_dedupe::execute(f.owner(), &policy, f.user.user_id, None).await.unwrap();
    assert_eq!((done.groups_merged, done.reefs_merged, done.groups_failed), (5, 8, 0), "{:?}", done.failures);
    let again = reef_dedupe::execute(f.owner(), &policy, f.user.user_id, None).await.unwrap();
    assert_eq!((again.groups_merged, again.reefs_merged, again.groups_failed), (0, 0, 0), "a second run finds nothing");
    let after = reef_dedupe::report(&reef_dedupe::plan(f.owner(), &policy).await.unwrap(), &policy);
    assert_eq!((after.groups_to_merge, after.reefs_to_merge), (0, 0));

    assert_eq!(reefs_titled(&f, f.graph, "org").await, vec![org_keep]);
    for d in org_dups {
        assert!(!reefs_titled(&f, f.graph, "org").await.contains(&d));
    }
    assert_eq!(reefs_titled(&f, f.graph, "Atlas").await, vec![atlas_keep], "the topic Atlas folds into the older project");
    assert!(!reefs_titled(&f, f.graph, "Atlas").await.contains(&atlas_topic));
    assert_eq!(reef_kind(&f, atlas_keep).await, kind("project"), "the survivor keeps the oldest kind");
    let mut helix_left = reefs_titled(&f, f.graph, "Helix").await;
    helix_left.sort();
    let mut helix_want = vec![helix_keep, helix_org];
    helix_want.sort();
    assert_eq!(helix_left, helix_want, "the projects fold into the older topic; the org is its own name space");
    assert!(helix_projects.iter().all(|p| !helix_left.contains(p)));
    assert_eq!(reef_kind(&f, helix_keep).await, kind("topic"), "the survivor keeps the oldest kind");
    assert_eq!(reef_kind(&f, helix_org).await, kind("org"));
    let merged_kinds: Vec<(String, String)> = sqlx::query_as("SELECT j.evidence->>'keep_kind', j.evidence->>'dup_kind' FROM node_merges m JOIN reef_judgement j USING(judgement_id) WHERE m.keep_id=$1 ORDER BY 2")
        .bind(helix_keep).fetch_all(f.owner()).await.unwrap();
    assert_eq!(merged_kinds, vec![("topic".to_string(), "project".to_string()); 2], "the judgement records both kinds");
    let snapshot_kind: Vec<Option<String>> = sqlx::query_scalar("SELECT dup_snapshot->'body'->>'entity_type' FROM node_merges WHERE keep_id=$1")
        .bind(helix_keep).fetch_all(f.owner()).await.unwrap();
    assert_eq!(snapshot_kind, vec![Some("project".to_string()); 2], "undo keeps each copy's own kind");
    assert_eq!(reefs_titled(&f, f.graph, "Alex Kim").await, alex.to_vec());
    assert_eq!(reefs_titled(&f, f.graph, "Composio").await, vec![composio[0]]);
    assert_eq!(reefs_titled(&f, f.graph, "Initech").await, initech.to_vec());
    assert_eq!(reefs_titled(&f, f.graph, "Globex").await, vec![globex_keep]);
    assert!(!reefs_titled(&f, f.graph, "Globex").await.contains(&globex_raw));
    let dst: Uuid = sqlx::query_scalar("SELECT dst_node_id FROM relationship_edge WHERE edge_id=$1").bind(edge).fetch_one(f.owner()).await.unwrap();
    assert_eq!(dst, atlas_keep, "the merge primitive re-points edges onto the keeper");
    let undo: Vec<String> = sqlx::query_scalar("SELECT j.evidence->>'rule' FROM node_merges m JOIN reef_judgement j USING(judgement_id) WHERE j.evidence->>'route'='ops_dedupe_reefs_cli'")
        .fetch_all(f.owner()).await.unwrap();
    assert_eq!(undo, vec!["same-title".to_string(); 8], "each merge has an undo row attributed to the rule");

    // The mint now binds the keeper.
    let (r, node) = door_mention(&f, "ORG", "topic").await;
    assert_eq!((resolved_to(&r), node), (Some(org_keep), org_keep));
    let (r, node) = door_mention(&f, "helix", "project").await;
    assert_eq!((resolved_to(&r), node), (Some(helix_keep), helix_keep), "a project mention binds the surviving topic");
    assert_eq!(reef_kind(&f, helix_keep).await, kind("topic"));
    f.done().await;
}
#[tokio::test]
async fn consumer_same_title_merge_dry_run_and_execute_are_idempotent() { same_title_merge_dry_run_and_execute_are_idempotent(V2Shape::Consumer).await; }
#[tokio::test]
async fn tenant_owner_same_title_merge_dry_run_and_execute_are_idempotent() { same_title_merge_dry_run_and_execute_are_idempotent(V2Shape::TenantOwner).await; }

/// One transaction per duplicate (2026-10-28): a many-copy group merges
/// across several transactions (one `node_merges` row and judgement each); a
/// copy that gains a human "different" ruling after planning is skipped while
/// the others fold in; a copy whose row lock is held elsewhere times out,
/// is retried, recorded as failed, and does not stop the run or its group's
/// other copies; the next run folds it in.
async fn dedupe_merges_one_duplicate_per_transaction(shape: V2Shape) {
    use coralswarm_backend::reef_dedupe::{self, ExecOptions, Policy};
    let f = v2_fixture(&shape).await;
    let reef = |title: &'static str, age_days: i32| {
        let f = &f;
        async move {
            sqlx::query_scalar::<_, Uuid>("INSERT INTO nodes(graph_id,type,title,entity_type,body,identity_scope,created_at) VALUES($1,'entity',$2,'org','{}',gen_random_uuid(),now()-make_interval(days=>$3)) RETURNING node_id")
                .bind(f.graph).bind(title).bind(age_days).fetch_one(f.owner()).await.unwrap()
        }
    };
    let policy = Policy { graph: Some(f.graph), cutoff: "2000-01-01T00:00:00Z".parse().unwrap(), include_preexisting: true, ..Policy::default() };
    let fast = ExecOptions { lock_timeout_ms: 150, max_attempts: 2, retry_backoff_ms: 10, ..ExecOptions::default() };

    // 1. Several copies, several transactions.
    let keep = reef("Contoso", 30).await;
    let copies = [reef("Contoso", 20).await, reef("CONTOSO", 19).await, reef("contoso", 18).await, reef("Contoso", 17).await];
    let done = reef_dedupe::execute_with(f.owner(), &policy, f.user.user_id, None, &fast).await.unwrap();
    assert_eq!((done.groups_merged, done.groups_partial, done.reefs_merged, done.groups_failed, done.reefs_changed), (1, 0, 4, 0, 0), "{done:?}");
    assert_eq!(reefs_titled(&f, f.graph, "Contoso").await, vec![keep]);
    let txids: Vec<i64> = sqlx::query_scalar("SELECT DISTINCT xmin::text::bigint FROM node_merges WHERE keep_id=$1").bind(keep).fetch_all(f.owner()).await.unwrap();
    assert_eq!(txids.len(), 4, "each duplicate committed in its own transaction");

    // 2. A ruling that lands after planning skips only that copy.
    let keep = reef("Fabrikam", 30).await;
    let (a, b, c) = (reef("Fabrikam", 20).await, reef("Fabrikam", 19).await, reef("Fabrikam", 18).await);
    let planned = reef_dedupe::plan(f.owner(), &policy).await.unwrap();
    let (lo, hi) = if keep < b { (keep, b) } else { (b, keep) };
    sqlx::query("INSERT INTO reef_judgement(graph_id,node_a_id,node_b_id,verdict,source,evidence) VALUES($1,$2,$3,'negative','human','{}')")
        .bind(f.graph).bind(lo).bind(hi).execute(f.owner()).await.unwrap();
    let done = reef_dedupe::execute_groups(f.owner(), &planned, &policy, f.user.user_id, None, &fast).await;
    assert_eq!((done.groups_merged, done.groups_partial, done.reefs_merged, done.reefs_changed, done.groups_failed), (0, 1, 2, 1, 0), "{done:?}");
    let mut left = reefs_titled(&f, f.graph, "Fabrikam").await;
    left.sort();
    let mut want = vec![keep, b];
    want.sort();
    assert_eq!(left, want, "the ruled-different copy stays, the others fold in");
    assert!(!left.contains(&a) && !left.contains(&c));

    // 3. A lock held on one copy times out that copy only.
    let keep = reef("Globex Corp", 30).await;
    let held = reef("Globex Corp", 20).await;
    let others = [reef("Globex Corp", 19).await, reef("Globex Corp", 18).await];
    let mut blocker = f.owner().begin().await.unwrap();
    sqlx::query("SELECT node_id FROM nodes WHERE node_id=$1 FOR UPDATE").bind(held).fetch_all(&mut *blocker).await.unwrap();
    let started = std::time::Instant::now();
    let done = reef_dedupe::execute_with(f.owner(), &policy, f.user.user_id, None, &fast).await.unwrap();
    assert!(started.elapsed() < std::time::Duration::from_secs(20), "a held lock must time out, not wait");
    assert_eq!((done.groups_merged, done.groups_partial, done.reefs_merged, done.reefs_failed, done.groups_failed), (0, 1, 2, 1, 1), "{done:?}");
    assert!(done.aborted.is_none() && done.failures.len() == 1 && done.failures[0].contains(&held.to_string()), "{done:?}");
    let mut left = reefs_titled(&f, f.graph, "Globex Corp").await;
    left.sort();
    let mut want = vec![keep, held];
    want.sort();
    assert_eq!(left, want);
    assert!(others.iter().all(|o| !left.contains(o)));
    blocker.rollback().await.unwrap();
    let done = reef_dedupe::execute_with(f.owner(), &policy, f.user.user_id, None, &fast).await.unwrap();
    assert_eq!((done.reefs_merged, done.reefs_failed), (1, 0), "{done:?}");
    assert_eq!(reefs_titled(&f, f.graph, "Globex Corp").await, vec![keep]);
    f.done().await;
}
#[tokio::test]
async fn consumer_dedupe_merges_one_duplicate_per_transaction() { dedupe_merges_one_duplicate_per_transaction(V2Shape::Consumer).await; }
#[tokio::test]
async fn tenant_owner_dedupe_merges_one_duplicate_per_transaction() { dedupe_merges_one_duplicate_per_transaction(V2Shape::TenantOwner).await; }

/// Tenant 0203 / control 0386 bring a plane to the same nodes identity
/// indexes from either state prod and the trees were in: the tree's own
/// title-only unique index present (every migration-built database), or
/// that index already dropped by hand with same-title copies present (the
/// prod planes after the identity contract). Re-running the SQL is a no-op.
#[tokio::test]
async fn reef_identity_index_migration_converges_from_both_states() {
    let options = PgConnectOptions::from_str(&std::env::var("STANDING_TEST_BASE_URL").expect("isolated STANDING_TEST_BASE_URL required")).unwrap();
    assert!(matches!(options.get_host(), "127.0.0.1" | "localhost" | "::1"));
    let admin = PgPoolOptions::new().max_connections(2).connect_with(options.clone()).await.unwrap();
    sqlx::raw_sql("DO $$ BEGIN IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='coralswarm_app') THEN CREATE ROLE coralswarm_app LOGIN NOSUPERUSER NOBYPASSRLS NOCREATEDB NOCREATEROLE NOREPLICATION PASSWORD 'local-standing-fixture'; END IF; IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='coralswarm_tenant_app') THEN CREATE ROLE coralswarm_tenant_app LOGIN NOSUPERUSER NOBYPASSRLS NOCREATEDB NOCREATEROLE NOREPLICATION PASSWORD 'local-standing-fixture'; END IF; END $$;")
        .execute(&admin).await.unwrap();
    const TENANT: &str = include_str!("../migrations_tenant/0203_reef_identity_index_converge.sql");
    const CONTROL: &str = include_str!("../migrations_control/0386_reef_identity_index_converge.sql");
    for (tree, version, sql) in [("tenant", 203_i64, TENANT), ("control", 386_i64, CONTROL)] {
        for hand_dropped in [false, true] {
            let name = TestDatabase::new(&options, &format!("reef_index_{tree}"));
            sqlx::query(&format!("CREATE DATABASE {name}")).execute(&admin).await.unwrap();
            let owner = PgPoolOptions::new().max_connections(2).connect_with(options.clone().database(&name)).await.unwrap();
            let full = if tree == "tenant" { sqlx::migrate!("./migrations_tenant") } else { sqlx::migrate!("./migrations_control") };
            let found = full.iter().find(|m| m.version == version).expect("migration in tree");
            assert_eq!(found.sql.as_ref(), sql, "{tree}: the tree applies this file");
            let mut before = if tree == "tenant" { sqlx::migrate!("./migrations_tenant") } else { sqlx::migrate!("./migrations_control") };
            before.migrations = std::borrow::Cow::Owned(before.iter().filter(|m| m.version < version).cloned().collect());
            before.run(&owner).await.unwrap();
            let index = |name: &'static str| {
                let owner = owner.clone();
                async move { sqlx::query_scalar::<_, bool>("SELECT to_regclass($1) IS NOT NULL").bind(name).fetch_one(&owner).await.unwrap() }
            };
            assert!(index("nodes_graph_type_lower_title_unique").await, "{tree}: the tree before {version} still builds the title-only unique index");
            let mut copies = 0_i64;
            if hand_dropped {
                sqlx::query("DROP INDEX nodes_graph_type_lower_title_unique").execute(&owner).await.unwrap();
                let account = Uuid::new_v4();
                if tree == "control" {
                    let user = Uuid::new_v4();
                    sqlx::query("INSERT INTO users(id,email,clerk_user_id) VALUES($1,$2,$2)").bind(user).bind(format!("{user}@fixture.local")).execute(&owner).await.unwrap();
                    sqlx::query("INSERT INTO accounts(id,owner_user_id) VALUES($1,$2)").bind(account).bind(user).execute(&owner).await.unwrap();
                }
                let graph: Uuid = sqlx::query_scalar("INSERT INTO graphs(account_id,name,kind) VALUES($1,'Converge','org') RETURNING id").bind(account).fetch_one(&owner).await.unwrap();
                for _ in 0..3 {
                    sqlx::query("INSERT INTO nodes(graph_id,type,title,entity_type,body,identity_scope) VALUES($1,'entity','Atlas','topic','{\"entity_type\":\"topic\"}',gen_random_uuid())")
                        .bind(graph).execute(&owner).await.unwrap();
                }
                copies = 3;
            }
            full.run(&owner).await.unwrap();
            assert!(!index("nodes_graph_type_lower_title_unique").await, "{tree} hand_dropped={hand_dropped}: title-only unique index gone");
            assert!(index("nodes_graph_type_lower_title_identity_unique").await, "{tree}: composite identity index present");
            assert!(index("idx_nodes_graph_type_lower_title").await, "{tree}: title lookup index present");
            let n: i64 = sqlx::query_scalar("SELECT count(*) FROM nodes WHERE lower(title)='atlas'").fetch_one(&owner).await.unwrap();
            assert_eq!(n, copies, "{tree}: existing same-title copies are untouched (merged only by the reviewed tool)");
            let defs: Vec<String> = sqlx::query_scalar("SELECT indexdef FROM pg_indexes WHERE tablename='nodes' AND indexname IN ('nodes_graph_type_lower_title_identity_unique','idx_nodes_graph_type_lower_title') ORDER BY indexname").fetch_all(&owner).await.unwrap();
            sqlx::raw_sql(sql).execute(&owner).await.unwrap();
            let again: Vec<String> = sqlx::query_scalar("SELECT indexdef FROM pg_indexes WHERE tablename='nodes' AND indexname IN ('nodes_graph_type_lower_title_identity_unique','idx_nodes_graph_type_lower_title') ORDER BY indexname").fetch_all(&owner).await.unwrap();
            assert_eq!(defs, again, "{tree}: re-running the migration is a no-op");
            println!("converged tree={tree} hand_dropped={hand_dropped} copies={copies} indexes={defs:?}");
            owner.close().await;
            sqlx::query(&format!("DROP DATABASE {name} WITH (FORCE)")).execute(&admin).await.unwrap();
        }
    }
    admin.close().await;
}
