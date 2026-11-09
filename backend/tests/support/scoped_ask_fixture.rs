use coralswarm_backend::{
    library_read::LibraryScope,
    llm::*,
    scoped_ask::AskRequest,
    store::PostgresStore,
    tenancy::{
        extractor::Tenant,
        runtime::{TenancyDecision, TenantContext},
    },
    UserContext,
};
use coralswarm_core::embed::{EmbedBatchFuture, EmbedProvider};
use serde_json::{json, Value};
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    Executor, PgPool,
};
use std::{
    pin::Pin,
    str::FromStr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
};
use uuid::Uuid;

use super::{app_role, tenant_role};

pub struct CountEmbed(pub Arc<AtomicUsize>);
impl EmbedProvider for CountEmbed {
    fn embed_batch<'a>(
        &'a self,
        inputs: &'a [coralswarm_core::embed::EmbedInput],
        _: coralswarm_core::embed::ProviderPermit,
    ) -> EmbedBatchFuture<'a> {
        self.0.fetch_add(1, Ordering::SeqCst);
        let n = inputs.len();
        Box::pin(async move { Ok(vec![vec![1.0; 384]; n]) })
    }
}
pub struct FakeModel {
    pub calls: Arc<AtomicUsize>,
    pub prompts: Arc<Mutex<Vec<String>>>,
    pub systems: Arc<Mutex<Vec<String>>>,
    pub started: Option<Arc<tokio::sync::Notify>>,
    pub resume: Option<Arc<tokio::sync::Notify>>,
}
/// The ordinal the prompt hands the model for the green-release passage, if any.
pub fn green_release_ordinal(prompt: &str) -> Option<u64> {
    let prompt: Value = serde_json::from_str(prompt).ok()?;
    let retained = prompt["outcomes"]
        .as_array()
        .into_iter()
        .flatten()
        .flat_map(|o| o["citations"].as_array().into_iter().flatten());
    let legacy = prompt["legacy_evidence"].as_array().into_iter().flatten();
    retained
        .chain(legacy)
        .find(|c| c["quote"].as_str().is_some_and(|q| q.contains("green release")))
        .and_then(|c| c["n"].as_u64())
}
#[async_trait::async_trait]
impl LlmProvider for FakeModel {
    async fn complete(&self, req: CompletionRequest) -> Result<CompletionResponse, LlmError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.prompts
            .lock()
            .unwrap()
            .push(req.messages[0].content.clone());
        self.systems
            .lock()
            .unwrap()
            .push(req.system.clone().unwrap_or_default());
        if let Some(started) = &self.started {
            started.notify_one();
        }
        if let Some(resume) = &self.resume {
            resume.notified().await;
        }
        let content = match green_release_ordinal(&req.messages[0].content) {
            Some(n) => format!("Synthetic model answer: Acme approved the green release [{n}]. Disagreement remains unresolved."),
            None => "Synthetic model answer about Acme. Disagreement remains unresolved.".into(),
        };
        Ok(CompletionResponse {
            content,
            input_tokens: 10,
            output_tokens: 10,
            model: req.model,
            cache_creation_input_tokens: 0,
            cache_read_input_tokens: 0,
            stop_reason: None,
            cost_usd: None,
        })
    }
    async fn stream_complete(
        &self,
        _: CompletionRequest,
    ) -> Result<Pin<Box<dyn futures::Stream<Item = Result<StreamEvent, LlmError>> + Send>>, LlmError>
    {
        panic!("scoped answers must buffer completion")
    }
}
pub struct Fixture {
    pub admin: PgPool,
    pub control_owner: PgPool,
    pub owner: PgPool,
    pub app: PgPool,
    pub control_app: PgPool,
    pub names: Vec<coralswarm_backend::test_db::TestDatabase>,
    pub tenant: Tenant,
    pub user: UserContext,
    pub graph: Uuid,
    pub denied_graph: Uuid,
    pub old: (Uuid, Uuid, Uuid),
    pub extra: (Uuid, Uuid, Uuid),
    pub legacy: Uuid,
    pub embeddings: Arc<AtomicUsize>,
    pub calls: Arc<AtomicUsize>,
    pub prompts: Arc<Mutex<Vec<String>>>,
    pub systems: Arc<Mutex<Vec<String>>>,
}
impl Fixture {
    pub async fn new(org: bool) -> Option<Self> { Self::at_boundary(org,true).await }
    pub async fn at_boundary(org: bool, funded: bool) -> Option<Self> {
        let Ok(dsn) = std::env::var("P7_TEST_BASE_URL") else {
            assert!(
                std::env::var_os("REQUIRE_PG_SUITE").is_none(),
                "local PG required"
            );
            return None;
        };
        let opts = PgConnectOptions::from_str(&dsn).unwrap();
        assert!(matches!(opts.get_host(), "127.0.0.1" | "localhost" | "::1"));
        assert_eq!(opts.get_database(), Some("postgres"));
        let admin = PgPoolOptions::new()
            .max_connections(2)
            .connect_with(opts.clone())
            .await
            .unwrap();
        let control_name = coralswarm_backend::test_db::TestDatabase::new(&opts, "p7_ask_control");
        admin
            .execute(format!("CREATE DATABASE {control_name}").as_str())
            .await
            .unwrap();
        let control_owner = PgPoolOptions::new()
            .max_connections(3)
            .connect_with(opts.clone().database(&control_name))
            .await
            .unwrap();
        let full=&coralswarm_backend::CONTROL_MIGRATOR;
        sqlx::migrate::Migrator {
            migrations:std::borrow::Cow::Owned(full.iter().filter(|m|funded || m.version<321).cloned().collect()),
            ignore_missing:false,locking:true,no_tx:false,
        }.run(&control_owner).await.unwrap();
        app_role::set_app_role_password_with_retry(&control_owner).await;
        let control_app = PgPoolOptions::new()
            .max_connections(4)
            .connect_with(
                opts.clone()
                    .database(&control_name)
                    .username("coralswarm_app")
                    .password(app_role::APP_ROLE_TEST_PASSWORD),
            )
            .await
            .unwrap();
        let user_id: Uuid = sqlx::query_scalar("INSERT INTO users(email) VALUES($1) RETURNING id")
            .bind(format!("p7-ask-{}@test.local", Uuid::new_v4()))
            .fetch_one(&control_owner)
            .await
            .unwrap();
        let org_id = format!("org_fake_{}", Uuid::new_v4().simple());
        let account: Uuid = sqlx::query_scalar(
            "INSERT INTO accounts(owner_user_id,clerk_org_id) VALUES($1,$2) RETURNING id",
        )
        .bind(user_id)
        .bind(org.then_some(&org_id))
        .fetch_one(&control_owner)
        .await
        .unwrap();
        sqlx::query("INSERT INTO account_members(account_id,user_id,role,seat_tier_id) VALUES($1,$2,'owner',(SELECT CASE WHEN a.is_org_account THEN (SELECT id FROM seat_tiers WHERE code='standard' AND is_active) END FROM accounts a WHERE a.id=$1))")
            .bind(account)
            .bind(user_id)
            .execute(&control_owner)
            .await
            .unwrap();
        let mut names = vec![control_name];
        let (owner, app) = if org {
            let name = coralswarm_backend::test_db::TestDatabase::new(&opts, "p7_ask_tenant");
            admin
                .execute(format!("CREATE DATABASE {name}").as_str())
                .await
                .unwrap();
            let owner = PgPoolOptions::new()
                .max_connections(3)
                .connect_with(opts.clone().database(&name))
                .await
                .unwrap();
            let full=sqlx::migrate!("./migrations_tenant");
            sqlx::migrate::Migrator {
                migrations:std::borrow::Cow::Owned(full.iter().filter(|m|funded || m.version<153).cloned().collect()),
                ignore_missing:false,locking:true,no_tx:false,
            }.run(&owner).await.unwrap();
            sqlx::query("INSERT INTO tenant_meta(org_id) VALUES($1)").bind(&org_id).execute(&owner).await.unwrap();
            let mut routing=coralswarm_backend::tenancy::scope::begin_account_scoped(&control_owner,account).await.unwrap();
            if funded { coralswarm_backend::billing::renewal::lock_period_rotation(&mut routing,account).await.unwrap(); }
            sqlx::query("INSERT INTO tenant_databases(clerk_org_id,account_id,slug,db_secret_ref,status,schema_version) VALUES($1,$2,$3,$3,'active',153)")
                .bind(&org_id).bind(account).bind(format!("slug-{}",&account.simple().to_string()[..20]))
                .execute(&mut *routing).await.unwrap();
            routing.commit().await.unwrap();
            tenant_role::set_app_role_password_with_retry(&owner).await;
            let app = PgPoolOptions::new()
                .max_connections(4)
                .connect_with(
                    opts.database(&name)
                        .username("coralswarm_tenant_app")
                        .password(tenant_role::APP_ROLE_TEST_PASSWORD),
                )
                .await
                .unwrap();
            names.push(name);
            (owner, app)
        } else {
            (control_owner.clone(), control_app.clone())
        };
        let privs:(bool,bool,i64)=sqlx::query_as("SELECT rolsuper,rolbypassrls,(SELECT count(*) FROM pg_tables WHERE schemaname='public' AND tableowner=current_user) FROM pg_roles WHERE rolname=current_user").fetch_one(&app).await.unwrap();
        assert_eq!(privs, (false, false, 0));
        let graph:Uuid=sqlx::query_scalar("INSERT INTO graphs(account_id,name,kind,owner_user_id) VALUES($1,'P7 synthetic Ask','member',$2) RETURNING id").bind(account).bind(user_id).fetch_one(&owner).await.unwrap();
        let denied_graph:Uuid=sqlx::query_scalar("INSERT INTO graphs(account_id,name,kind,owner_user_id) VALUES($1,'Other private graph','member',$2) RETURNING id").bind(account).bind(Uuid::new_v4()).fetch_one(&owner).await.unwrap();
        let old = source(&owner, graph, "old", "Acme approved green release", true).await;
        let extra = source(&owner, graph, "other", "Acme disputed red release", false).await;
        for n in 0..65 {
            source(
                &owner,
                graph,
                &format!("new-{n}"),
                &format!("Unrelated newest capture {n}"),
                false,
            )
            .await;
        }
        let legacy:Uuid=sqlx::query_scalar("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,title,timestamp) VALUES($1,'slack','genuine-legacy','fixture-only','Acme legacy chunk','Acme legacy chunk',now()) RETURNING atom_id").bind(graph).fetch_one(&owner).await.unwrap();
        let store = Arc::new(PostgresStore::new(app.clone()));
        let tenant = Tenant {
            store: Some(store),
            pool: Some(app.clone()),
            decision: Some(if org {
                TenancyDecision::Org(TenantContext {
                    org_id,
                    account_id: account,
                    user_id,
                    pool: app.clone(),
                    page_pool: None,
                })
            } else {
                TenancyDecision::Personal
            }),
            class: coralswarm_backend::route_class::RouteClass::Other,
        };
        Some(Self {
            admin,
            control_owner,
            owner,
            app,
            control_app,
            names,
            tenant,
            user: UserContext {
                user_id,
                account_id: account,
                role: "owner".into(),
                device_id: "fake-fixture".into(),
            },
            graph,
            denied_graph,
            old,
            extra,
            legacy,
            embeddings: Arc::new(AtomicUsize::new(0)),
            calls: Arc::new(AtomicUsize::new(0)),
            prompts: Arc::new(Mutex::new(vec![])),
            systems: Arc::new(Mutex::new(vec![])),
        })
    }
    pub fn scope(&self, item: Option<Uuid>) -> LibraryScope {
        serde_json::from_value(json!({"graph_ids":[self.graph],"source_item_id":item})).unwrap()
    }
    pub fn request(&self, item: Option<Uuid>) -> AskRequest {
        serde_json::from_value(json!({"text":"What did Acme decide?","library_scope":self.scope(item),"style":"sources-only"})).unwrap()
    }
    /// The plain state with `embedder` answering the Ask's query embedding.
    pub fn state_with_embedder(
        &self,
        embedder: Arc<dyn EmbedProvider + Send + Sync>,
    ) -> Arc<coralswarm_backend::AppState> {
        let mut s = coralswarm_backend::test_app_state_owned_with_db_and_store(
            self.control_app.clone(),
            None,
        );
        s.embedder = embedder;
        s.llm = Some(Arc::new(FakeModel {
            calls: self.calls.clone(),
            prompts: self.prompts.clone(),
            systems: self.systems.clone(),
            started: None,
            resume: None,
        }));
        Arc::new(s)
    }
    pub fn state(
        &self,
        deferred: Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>,
    ) -> Arc<coralswarm_backend::AppState> {
        let mut s = coralswarm_backend::test_app_state_owned_with_db_and_store(
            self.control_app.clone(),
            None,
        );
        s.embedder = Arc::new(CountEmbed(self.embeddings.clone()));
        s.llm = Some(Arc::new(FakeModel {
            calls: self.calls.clone(),
            prompts: self.prompts.clone(),
            systems: self.systems.clone(),
            started: deferred.as_ref().map(|d| d.0.clone()),
            resume: deferred.map(|d| d.1),
        }));
        Arc::new(s)
    }
    pub async fn access(&self, state: &str) {
        sqlx::query("UPDATE source_items SET access_state=$2 WHERE source_item_id=$1")
            .bind(self.old.0)
            .bind(state)
            .execute(&self.owner)
            .await
            .unwrap();
    }
    pub async fn done(self) {
        self.app.close().await;
        self.control_app.close().await;
        self.owner.close().await;
        self.control_owner.close().await;
        for name in self.names {
            if std::env::var("ORGT_TEST_RETAIN_DATABASES").as_deref() == Ok("1") {
                eprintln!("retained_fixture={name}");
            } else {
                self.admin
                    .execute(format!("DROP DATABASE {name} WITH (FORCE)").as_str())
                    .await
                    .unwrap();
            }
        }
        self.admin.close().await;
    }
}
pub async fn source(pool: &PgPool, graph: Uuid, id: &str, text: &str, old: bool) -> (Uuid, Uuid, Uuid) {
    let origin = json!({"service_id":"linear","transport":"synthetic","content_kind":"issue","capture_surface":"synthetic_demo"});
    let item:Uuid=sqlx::query_scalar("INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id,origin) VALUES($1,'linear_issue','demo:linear:workspace:fixture',$2,$3) RETURNING source_item_id").bind(graph).bind(id).bind(&origin).fetch_one(pool).await.unwrap();
    let version:Uuid=sqlx::query_scalar("INSERT INTO source_versions(graph_id,source_item_id,source_id,source_type,provider_namespace,raw_inline,extraction_version,origin,captured_at) VALUES($1,$2,$3,'linear_issue','demo:linear:workspace:fixture',$4,'source-atoms-v2',$5,CASE WHEN $6 THEN now()-interval '10 years' ELSE now() END) RETURNING version_id").bind(graph).bind(item).bind(id).bind(text).bind(&origin).bind(old).fetch_one(pool).await.unwrap();
    sqlx::query("UPDATE source_items SET current_version_id=$2 WHERE source_item_id=$1")
        .bind(item)
        .bind(version)
        .execute(pool)
        .await
        .unwrap();
    let atom:Uuid=sqlx::query_scalar("INSERT INTO semantic_atoms(graph_id,source_version_id,ordinal,kind,statement,extraction_version,model) VALUES($1,$2,0,'decision',$3,'source-atoms-v2','fake-model') RETURNING semantic_atom_id").bind(graph).bind(version).bind(text).fetch_one(pool).await.unwrap();
    sqlx::query("INSERT INTO semantic_atom_passages(graph_id,source_version_id,semantic_atom_id,start_char,end_char,quote) VALUES($1,$2,$3,0,$4,$5)").bind(graph).bind(version).bind(atom).bind(text.chars().count() as i32).bind(text).execute(pool).await.unwrap();
    (item, version, atom)
}
