//! Actual migrations, non-bypass logins, real compute and handler lifecycle.
//! Required local URL, never a green skipped suite.
use coralswarm_backend::{
    read_models::{self as rm, Params, ReadModel},
    store::PostgresStore,
    tenancy::{scope::begin_tenant_scoped, TenantId},
    test_db::{migrated_database, Plane, TestDatabase},
    workers::{read_model_refresh, tenant_harness::WorkerTenant},
};
use serde_json::{json, Value};
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    ConnectOptions, Executor, PgPool,
};
use std::{
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};
use uuid::Uuid;
pub struct Fixture {
    _db: TestDatabase,
    pub owner: PgPool,
    pub app: PgPool,
    pub control: PgPool,
    pub control_owner: PgPool,
    _control_db: Option<TestDatabase>,
    pub account: Uuid,
    pub user: Uuid,
    pub other: Uuid,
    pub graph: Uuid,
    pub foreign: Uuid,
    pub org: bool,
}
impl Fixture {
    pub async fn new(org: bool) -> Self {
        let url = std::env::var("PAGE_MODEL_TEST_BASE_URL")
            .expect("PAGE_MODEL_TEST_BASE_URL required; no skipped read-model proof");
        let opts = PgConnectOptions::from_str(&url).unwrap();
        assert!(matches!(opts.get_host(), "localhost" | "127.0.0.1"));
        let setup = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(opts.clone())
            .await
            .unwrap();
        setup.execute("DO $$ BEGIN IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='coralswarm_app') THEN CREATE ROLE coralswarm_app NOLOGIN NOBYPASSRLS; END IF; IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='coralswarm_tenant_app') THEN CREATE ROLE coralswarm_tenant_app NOLOGIN NOBYPASSRLS; END IF; END $$;").await.unwrap();
        setup.close().await;
        let db = migrated_database(
            &opts,
            if org { Plane::Tenant } else { Plane::Control },
            "l4_models",
        )
        .await;
        let opts = opts.database(db.name());
        let control_opts = opts.clone();
        let owner = PgPoolOptions::new()
            .max_connections(4)
            .connect_with(opts.clone())
            .await
            .unwrap();
        let role = format!("read_model_{}", Uuid::new_v4().simple());
        let password = Uuid::new_v4().to_string();
        let base = if org {
            "coralswarm_tenant_app"
        } else {
            "coralswarm_app"
        };
        owner.execute(format!("CREATE ROLE {role} LOGIN INHERIT NOBYPASSRLS NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD '{password}' IN ROLE {base}").as_str()).await.unwrap();
        let app = PgPoolOptions::new()
            .max_connections(4)
            .connect_with(opts.username(&role).password(&password))
            .await
            .unwrap();
        let attrs:(bool,bool,i64)=sqlx::query_as("SELECT rolsuper,rolbypassrls,(SELECT count(*) FROM pg_class WHERE relnamespace='public'::regnamespace AND relowner=(SELECT oid FROM pg_roles WHERE rolname=current_user)) FROM pg_roles WHERE rolname=current_user").fetch_one(&app).await.unwrap();
        assert_eq!(attrs, (false, false, 0));
        let (account, user, other) = if org {
            sqlx::query("INSERT INTO tenant_meta(id,org_id) VALUES(true,'org_read_models')")
                .execute(&owner)
                .await
                .unwrap();
            (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4())
        } else {
            let mut ids = vec![];
            for _ in 0..2 {
                let user: Uuid =
                    sqlx::query_scalar("INSERT INTO users(email) VALUES($1) RETURNING id")
                        .bind(format!("read-model-{}@test.local", Uuid::new_v4()))
                        .fetch_one(&owner)
                        .await
                        .unwrap();
                let acc: Uuid = sqlx::query_scalar(
                    "INSERT INTO accounts(owner_user_id) VALUES($1) RETURNING id",
                )
                .bind(user)
                .fetch_one(&owner)
                .await
                .unwrap();
                sqlx::query(
                    "INSERT INTO account_members(account_id,user_id,role) VALUES($1,$2,'owner')",
                )
                .bind(acc)
                .bind(user)
                .execute(&owner)
                .await
                .unwrap();
                ids.push((acc, user));
            }
            (ids[0].0, ids[0].1, ids[1].1)
        };
        let graph:Uuid=sqlx::query_scalar("INSERT INTO graphs(account_id,name,kind,owner_user_id) VALUES($1,'Read-model fixture','member',$2) RETURNING id").bind(account).bind(user).fetch_one(&owner).await.unwrap();
        let foreign:Uuid=sqlx::query_scalar("INSERT INTO graphs(account_id,name,kind,owner_user_id) VALUES($1,'Foreign fixture','member',$2) RETURNING id").bind(account).bind(other).fetch_one(&owner).await.unwrap();
        let (control, control_db, control_owner) = if org {
            let db = migrated_database(
                &control_opts.clone().database("postgres"),
                Plane::Control,
                "l4_identity",
            )
            .await;
            let owner = PgPoolOptions::new()
                .max_connections(2)
                .connect_with(control_opts.clone().database(db.name()))
                .await
                .unwrap();
            for actor in [user, other] {
                sqlx::query("INSERT INTO users(id,email) VALUES($1,$2)")
                    .bind(actor)
                    .bind(format!("rm-identity-{actor}@test.local"))
                    .execute(&owner)
                    .await
                    .unwrap();
            }
            sqlx::query("INSERT INTO accounts(id,owner_user_id,clerk_org_id) VALUES($1,$2,'org_read_models')").bind(account).bind(user).execute(&owner).await.unwrap();
            for actor in [user, other] {
                sqlx::query(
                    "INSERT INTO account_members(account_id,user_id,role,seat_tier_id) VALUES($1,$2,'member',(SELECT id FROM seat_tiers WHERE code='standard' AND is_active=true))",
                )
                .bind(account)
                .bind(actor)
                .execute(&owner)
                .await
                .unwrap();
            }
            let role = format!("read_model_control_{}", Uuid::new_v4().simple());
            owner.execute(format!("CREATE ROLE {role} LOGIN INHERIT NOBYPASSRLS NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD '{password}' IN ROLE coralswarm_app").as_str()).await.unwrap();
            let control = PgPoolOptions::new()
                .max_connections(2)
                .connect_with(
                    control_opts
                        .database(db.name())
                        .username(&role)
                        .password(&password),
                )
                .await
                .unwrap();
            (control, Some(db), owner)
        } else {
            (app.clone(), None, owner.clone())
        };
        Self {
            _db: db,
            owner,
            app,
            control,
            control_owner,
            _control_db: control_db,
            account,
            user,
            other,
            graph,
            foreign,
            org,
        }
    }
    /// Private local manifest for profile comparisons on one immutable fixture.
    /// Never print/commit it: connection URLs may contain generated test passwords.
    pub fn retain_benchmark(&self, path: &std::path::Path) {
        use std::os::unix::fs::OpenOptionsExt;
        assert_eq!(std::env::var("ORGT_TEST_RETAIN_DATABASES").as_deref(), Ok("1"));
        let value = json!({"owner": self.owner.connect_options().to_url_lossy().to_string(),
            "app": self.app.connect_options().to_url_lossy().to_string(),
            "control": self.control.connect_options().to_url_lossy().to_string(),
            "control_owner": self.control_owner.connect_options().to_url_lossy().to_string(),
            "account":self.account,"user":self.user,"other":self.other,"graph":self.graph,"foreign":self.foreign,
            "tenant_schema":coralswarm_backend::test_db::template_name(Plane::Tenant),
            "control_schema":coralswarm_backend::test_db::template_name(Plane::Control)});
        let file=std::fs::OpenOptions::new().write(true).create_new(true).mode(0o600).open(path).unwrap();
        serde_json::to_writer_pretty(file,&value).unwrap();
    }
    pub async fn reopen_benchmark(path: &std::path::Path) -> Self {
        assert_eq!(std::env::var("ORGT_TEST_RETAIN_DATABASES").as_deref(), Ok("1"));
        let v:Value=serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        assert_eq!(v["tenant_schema"],coralswarm_backend::test_db::template_name(Plane::Tenant));
        assert_eq!(v["control_schema"],coralswarm_backend::test_db::template_name(Plane::Control));
        let options=|key:&str| {
            let o=PgConnectOptions::from_str(v[key].as_str().unwrap()).unwrap();
            assert_eq!(o.get_host(),"127.0.0.1"); assert_eq!(o.get_port(),15761);
            assert!(o.get_database().unwrap().starts_with("cstest_l4_")); o
        };
        let owner_opts=options("owner"); let control_opts=options("control_owner");
        let owner=PgPoolOptions::new().max_connections(4).connect_with(owner_opts.clone()).await.unwrap();
        let app=PgPoolOptions::new().max_connections(4).connect_with(options("app")).await.unwrap();
        let control=PgPoolOptions::new().max_connections(2).connect_with(options("control")).await.unwrap();
        let control_owner=PgPoolOptions::new().max_connections(2).connect_with(control_opts.clone()).await.unwrap();
        Self {_db:TestDatabase::adopt(&owner_opts,owner_opts.get_database().unwrap()),
            _control_db:Some(TestDatabase::adopt(&control_opts,control_opts.get_database().unwrap())),
            owner,app,control,control_owner,account:serde_json::from_value(v["account"].clone()).unwrap(),
            user:serde_json::from_value(v["user"].clone()).unwrap(),other:serde_json::from_value(v["other"].clone()).unwrap(),
            graph:serde_json::from_value(v["graph"].clone()).unwrap(),foreign:serde_json::from_value(v["foreign"].clone()).unwrap(),org:true}
    }
    pub fn worker(&self) -> WorkerTenant {
        WorkerTenant {
            tenant: if self.org {
                TenantId::Org("org_read_models".into())
            } else {
                TenantId::Consumer
            },
            pool: self.app.clone(),
            store: Arc::new(PostgresStore::new(self.app.clone())),
        }
    }
    pub async fn atom(&self, name: &str) -> Uuid {
        sqlx::query_scalar("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,timestamp) VALUES($1,'note',$2,'fixture','Synthetic evidence',now()) RETURNING atom_id").bind(self.graph).bind(name).fetch_one(&self.owner).await.unwrap()
    }
    pub async fn close(self) {
        self.control.close().await;
        self.control_owner.close().await;
        self.app.close().await;
        self.owner.close().await;
    }
}
