//! In-app org deletion (control 0387), the DESTROY step end to end against a
//! real Postgres: a tenant really provisioned by the A1 job (LOCAL
//! provisioner, `CREATE DATABASE`), then deleted by `services::org_deletion`
//! running as production runs it: every control read and write on a real
//! `coralswarm_app` login, the routing row and credential deletes on a real
//! login granted `coralswarm_provisioner`.
//!
//! Proves: the routing row, the encrypted DSN and the tenant database are
//! gone and the org is a `done` tombstone; a second run converges; a crash
//! after ANY destroying step's side effect resumes to the same end; the
//! database step refuses a database not NAMED for the org, and one not
//! FINGERPRINTED for it, and deletes nothing; the serving role cannot call
//! the routing delete, and the provisioner only for a destroying deletion;
//! the Clerk delete treats 404 as done; the export written before the
//! database went opens with the recorded key and carries the tenant's rows.
//!
//!   REQUIRE_PG_SUITE=1 ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test org_destroy_pg -- --test-threads=2

mod support;

#[path = "support/retained_database.rs"]
mod retained_database;

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use coralswarm_backend::services::org_deletion::{
    self, Ctx, Destroyer, ExportScope, ExportStore, Initiator, Mail, Mailer, OrgDataPlane, OrgDeletion, OrgDirectory,
    RunEnd,
};
use coralswarm_backend::services::org_deletion_live::{build_export, build_export_with_cap, manifest_counts};
use coralswarm_backend::tenancy::credentials;
use coralswarm_backend::tenancy::provisioner::{LocalPgProvisioner, TenantDbProvisioner};
use coralswarm_backend::tenancy::provisioning::{
    self, Deps, EnqueueRequest, RetryPolicy, RunOutcome, StaticOrgDirectory, SELFSERVE_SIGNUP_FLAG,
};
use coralswarm_backend::tenancy::registry::TenantRegistry;
use coralswarm_backend::tenancy::routing;
use coralswarm_backend::tenancy::secrets::{
    ChainSecretResolver, ControlTableSecretResolver, SecretResolver, StaticMapResolver,
};
use serde_json::Value;
use sqlx::postgres::PgPoolOptions;
use sqlx::{Connection, PgConnection, PgPool};
use support::app_role::{set_app_role_password_with_retry, APP_ROLE_TEST_PASSWORD};
use support::provisioner_role::{prepare_provisioner_login, PROVISIONER_TEST_LOGIN, PROVISIONER_TEST_PASSWORD};
use tokio::sync::Mutex as AsyncMutex;
use uuid::Uuid;

const KEK: [u8; 32] = [0x61u8; 32];
static FLAG_LOCK: AsyncMutex<()> = AsyncMutex::const_new(());

fn base_url() -> Option<String> {
    let url = std::env::var("ORGT_TEST_BASE_URL").ok();
    assert!(
        url.is_some() || std::env::var_os("REQUIRE_PG_SUITE").is_none(),
        "REQUIRE_PG_SUITE=1 but ORGT_TEST_BASE_URL is unset"
    );
    url
}
fn with_db(base: &str, db: &str) -> String {
    let (prefix, rest) = base.split_at(base.rfind('/').expect("url path /"));
    let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
    format!("{prefix}/{db}{params}")
}
fn as_role(url: &str, user: &str, pass: &str) -> String {
    let scheme_end = url.find("://").expect("url has a scheme") + 3;
    let (scheme, rest) = url.split_at(scheme_end);
    let host_and_rest = rest.split_once('@').map(|(_, h)| h).unwrap_or(rest);
    format!("{scheme}{user}:{pass}@{host_and_rest}")
}

// ── fakes for what is not under test here ─────────────────────────────────

/// The real tenant database, read through the resolver chain (as the live
/// data plane does through the registry), with the live export builder.
struct TenantData {
    app: PgPool,
    resolver: Arc<dyn SecretResolver>,
}

impl TenantData {
    async fn tenant(&self, org: &str) -> Result<PgPool, String> {
        let row = routing::lookup_by_org(&self.app, org).await.map_err(|e| e.to_string())?.ok_or("no routing row")?;
        let dsn = self.resolver.resolve_async(&row.db_secret_ref).await.map_err(|e| e.to_string())?;
        PgPoolOptions::new().max_connections(2).connect(&dsn).await.map_err(|e| e.to_string())
    }
}

#[async_trait]
impl OrgDataPlane for TenantData {
    async fn source_manifest(&self, d: &OrgDeletion) -> Result<Value, String> {
        let t = self.tenant(&d.clerk_org_id).await?;
        let v = manifest_counts(&t).await.map_err(|e| e.to_string());
        t.close().await;
        v
    }
    async fn export_bundle(&self, d: &OrgDeletion, scope: &ExportScope) -> Result<Vec<u8>, String> {
        let t = self.tenant(&d.clerk_org_id).await?;
        type F = fn(String) -> std::future::Ready<Result<Vec<u8>, String>>;
        let v = build_export::<F, _>(&t, d, scope, None).await;
        t.close().await;
        v
    }
    async fn forget_tenant(&self, _org: &str, _account: Uuid) {}
}

#[derive(Default)]
struct MemExports(Mutex<HashMap<String, ([u8; 32], Vec<u8>)>>);

#[async_trait]
impl ExportStore for MemExports {
    async fn put(&self, key: &str, plaintext: &[u8], k: &[u8; 32]) -> Result<(), String> {
        self.0.lock().unwrap().insert(key.into(), (*k, plaintext.to_vec()));
        Ok(())
    }
    async fn get(&self, key: &str, k: &[u8; 32]) -> Result<Vec<u8>, String> {
        match self.0.lock().unwrap().get(key) {
            Some((kk, v)) if kk == k => Ok(v.clone()),
            _ => Err("missing or wrong key".into()),
        }
    }
    async fn delete(&self, key: &str) -> Result<(), String> {
        self.0.lock().unwrap().remove(key);
        Ok(())
    }
}

struct NoMail;
#[async_trait]
impl Mailer for NoMail {
    async fn send(&self, _mail: Mail) -> Result<(), String> {
        Ok(())
    }
}

/// A Clerk Backend API stand-in that answers every DELETE with 404 (the
/// org is already gone): the real `ClerkAdminClient` must treat that as done.
async fn clerk_404() -> (coralswarm_backend::clerk_admin::ClerkAdminClient, Arc<Mutex<Vec<String>>>) {
    let seen = Arc::new(Mutex::new(Vec::<String>::new()));
    let s = seen.clone();
    let app = axum::Router::new().fallback(axum::routing::any(move |uri: axum::http::Uri| {
        let s = s.clone();
        async move {
            s.lock().unwrap().push(uri.path().to_string());
            (
                axum::http::StatusCode::NOT_FOUND,
                [("content-type", "application/json")],
                r#"{"errors":[{"code":"resource_not_found","message":"not found"}]}"#,
            )
        }
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let client = coralswarm_backend::clerk_admin::ClerkAdminClient::new(reqwest::Client::new(), "sk_test_unused")
        .with_base_url(format!("http://{addr}"));
    (client, seen)
}

// ── fixture ───────────────────────────────────────────────────────────────

struct Fx {
    base: String,
    admin: PgConnection,
    owner: PgPool,
    app: PgPool,
    prov: PgPool,
    resolver: Arc<dyn SecretResolver>,
    registry: Arc<TenantRegistry>,
    data: TenantData,
    exports: MemExports,
    clerk: coralswarm_backend::clerk_admin::ClerkAdminClient,
    clerk_seen: Arc<Mutex<Vec<String>>>,
    destroyer: Destroyer,
}

struct Org {
    org: String,
    account: Uuid,
    user: Uuid,
    slug: String,
}

impl Fx {
    async fn new(db: &str) -> Option<Self> {
        let base = base_url()?;
        let mut admin = PgConnection::connect(&base).await.expect("connect admin");
        retained_database::recreate(&mut admin, db).await.unwrap();
        let url = with_db(&base, db);
        let owner = PgPoolOptions::new().max_connections(4).connect(&url).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&owner).await.expect("control tree");
        set_app_role_password_with_retry(&owner).await;
        // The database's own approval floor (owner-writable only): open it for
        // this suite, which drives the destroy path with `approval_first_n: 0`.
        sqlx::query("UPDATE org_deletion_policy SET approval_first_n = 0").execute(&owner).await.unwrap();
        let app = PgPoolOptions::new()
            .max_connections(6)
            .connect(&as_role(&url, "coralswarm_app", APP_ROLE_TEST_PASSWORD))
            .await
            .expect("connect as coralswarm_app");
        prepare_provisioner_login(&owner).await;
        let prov = PgPoolOptions::new()
            .max_connections(2)
            .connect(&as_role(&url, PROVISIONER_TEST_LOGIN, PROVISIONER_TEST_PASSWORD))
            .await
            .expect("connect as the provisioner login");
        let chain: Arc<dyn SecretResolver> = Arc::new(ChainSecretResolver::new(
            Arc::new(StaticMapResolver::from_pairs::<[(&str, &str); 0], _, _>([])),
            ControlTableSecretResolver::new(app.clone(), KEK, None),
        ));
        let registry = Arc::new(TenantRegistry::new(chain.clone()));
        let (clerk, clerk_seen) = clerk_404().await;
        let destroyer = Destroyer {
            provisioner: Arc::new(LocalPgProvisioner::new(base.clone())),
            provisioner_control: prov.clone(),
        };
        Some(Fx {
            data: TenantData { app: app.clone(), resolver: chain.clone() },
            base,
            admin,
            owner,
            app,
            prov,
            resolver: chain,
            registry,
            exports: MemExports::default(),
            clerk,
            clerk_seen,
            destroyer,
        })
    }

    fn ctx(&self, crash_after: Option<org_deletion::CrashHook>) -> Ctx<'_> {
        Ctx {
            control: &self.app,
            stripe: None,
            directory: Some(&self.clerk as &dyn OrgDirectory),
            data: &self.data,
            exports: Some(&self.exports),
            destroyer: Some(&self.destroyer),
            mailer: &NoMail,
            kek: Some(KEK),
            web_base: "https://app.example.test",
            approval_first_n: 0,
            crash_after,
        }
    }

    fn deps(&self, org: &str, slug: &str) -> Deps {
        Deps {
            control: self.app.clone(),
            provisioner_control: self.prov.clone(),
            provisioner: Arc::new(LocalPgProvisioner::new(self.base.clone())),
            directory: Arc::new(StaticOrgDirectory::selfserve([(org.to_string(), slug.to_string())])),
            registry: self.registry.clone(),
            resolver: self.resolver.clone(),
            kek: KEK,
            next_kek: None,
            policy: RetryPolicy {
                max_attempts: 3,
                base_backoff: Duration::from_millis(1),
                max_backoff: Duration::from_millis(1),
                lease_ttl: Duration::from_secs(60),
                subscription_poll: Duration::from_millis(1),
                version_poll: Duration::from_millis(1),
                ..RetryPolicy::default()
            },
            owner: "org-deletion-test".into(),
            fault: None,
        }
    }

    async fn set_subscription(&self, account: Uuid, status: &str, sub: &str) {
        let mut tx = coralswarm_backend::tenancy::scope::begin_account_scoped(&self.owner, account).await.unwrap();
        coralswarm_backend::billing::renewal::lock_period_rotation(&mut tx, account).await.unwrap();
        sqlx::query(
            "INSERT INTO stripe_subscriptions (account_id, stripe_customer_id, stripe_subscription_id, status) \
             VALUES ($1, 'cus_' || replace($1::text, '-', ''), $2, $3) \
             ON CONFLICT (account_id) DO UPDATE SET stripe_subscription_id = EXCLUDED.stripe_subscription_id, status = EXCLUDED.status",
        )
        .bind(account)
        .bind(sub)
        .bind(status)
        .execute(&mut *tx)
        .await
        .unwrap();
        tx.commit().await.unwrap();
    }

    /// A self-serve org provisioned to `ready` by the real A1 job.
    async fn provisioned(&mut self, org: &str) -> Org {
        let (account, user): (Uuid, Uuid) = sqlx::query_as(
            "WITH u AS (INSERT INTO users(email) VALUES($1) RETURNING id) \
             INSERT INTO accounts(owner_user_id, clerk_org_id) SELECT id, $2 FROM u RETURNING id, owner_user_id",
        )
        .bind(format!("{org}@org-deletion.test"))
        .bind(org)
        .fetch_one(&self.owner)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO account_members (account_id, user_id, role, seat_tier_id) \
             SELECT $1, $2, 'admin', id FROM seat_tiers WHERE code = 'standard' AND is_active",
        )
        .bind(account)
        .bind(user)
        .execute(&self.owner)
        .await
        .unwrap();
        self.set_subscription(account, "active", &format!("sub_{org}")).await;
        let slug = provisioning::opaque_slug_for_account(account);
        self.drop_db(&slug).await;
        {
            let _g = FLAG_LOCK.lock().await;
            std::env::set_var(SELFSERVE_SIGNUP_FLAG, "1");
            provisioning::enqueue(
                &self.app,
                EnqueueRequest { clerk_org_id: org.into(), account_id: account, slug: slug.clone(), region: None },
            )
            .await
            .unwrap();
        }
        let out = provisioning::run_job(&self.deps(org, &slug), org).await.unwrap().unwrap();
        assert_eq!(out, RunOutcome::Ready);
        // Billing is covered by the in-crate suite with a recording Stripe
        // mock; this suite has no Stripe client, so the org has no Stripe
        // customer (the steps that talk to Stripe finish without one).
        let mut tx = coralswarm_backend::tenancy::scope::begin_account_scoped(&self.owner, account).await.unwrap();
        coralswarm_backend::billing::renewal::lock_period_rotation(&mut tx, account).await.unwrap();
        sqlx::query("DELETE FROM stripe_subscriptions WHERE account_id = $1").bind(account).execute(&mut *tx).await.unwrap();
        tx.commit().await.unwrap();
        Org { org: org.into(), account, user, slug }
    }

    fn db_name(&self, slug: &str) -> String {
        LocalPgProvisioner::new(self.base.clone()).database_name(slug)
    }

    async fn db_exists(&mut self, slug: &str) -> bool {
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)")
            .bind(self.db_name(slug))
            .fetch_one(&mut self.admin)
            .await
            .unwrap()
    }

    async fn drop_db(&mut self, slug: &str) {
        let name = self.db_name(slug);
        retained_database::finish(&mut self.admin, &name, true).await.unwrap();
    }

    async fn row(&self, id: Uuid) -> OrgDeletion {
        sqlx::query_as("SELECT * FROM org_deletions WHERE id = $1").bind(id).fetch_one(&self.owner).await.unwrap()
    }

    /// The row as the worker holds it right after `org_deletion_claim`: a
    /// fresh fencing token and a live lease.
    async fn leased(&self, id: Uuid) -> OrgDeletion {
        sqlx::query_as(
            "UPDATE org_deletions SET lease_id = gen_random_uuid(), lease_until = now() + interval '120 seconds' \
              WHERE id = $1 RETURNING *",
        )
        .bind(id)
        .fetch_one(&self.owner)
        .await
        .unwrap()
    }

    /// Request, run the request phase, end grace: the deletion is ready to
    /// destroy (approval relaxed: `approval_first_n = 0`).
    async fn to_destroying(&self, o: &Org) -> Uuid {
        let d = org_deletion::insert(&self.app, o.account, &o.org, Initiator::Owner, Some(o.user)).await.unwrap().deletion;
        let end = org_deletion::run(&self.ctx(None), self.leased(d.id).await).await;
        assert_eq!(end, RunEnd::Waiting, "parks in grace");
        sqlx::query("UPDATE org_deletions SET grace_ends_at = now() - interval '1 second', next_attempt_at = now() WHERE id = $1")
            .bind(d.id)
            .execute(&self.owner)
            .await
            .unwrap();
        d.id
    }

    async fn drive(&self, id: Uuid, crash_after: Option<org_deletion::CrashHook>) -> RunEnd {
        org_deletion::run(&self.ctx(crash_after), self.leased(id).await).await
    }

    async fn routing_rows(&self, org: &str) -> i64 {
        sqlx::query_scalar("SELECT count(*) FROM tenant_databases WHERE clerk_org_id = $1")
            .bind(org)
            .fetch_one(&self.owner)
            .await
            .unwrap()
    }
}

// ── tests ─────────────────────────────────────────────────────────────────

#[tokio::test]
async fn destroy_removes_routing_credential_and_database_and_converges_on_rerun() {
    let Some(mut fx) = Fx::new("orgt_orgdel_destroy").await else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };
    let o = fx.provisioned("org_del_destroy").await;
    let secret_ref = provisioning::secret_ref_for_slug(&o.slug);
    assert!(fx.db_exists(&o.slug).await);
    let id = fx.to_destroying(&o).await;

    let end = fx.drive(id, None).await;
    let done = fx.row(id).await;
    let db_gone = !fx.db_exists(&o.slug).await;
    let cred = credentials::fetch_ciphertext(&fx.app, &secret_ref).await.unwrap();
    let job = provisioning::get(&fx.app, &o.org).await.unwrap().unwrap();
    let accounts: i64 = sqlx::query_scalar("SELECT count(*) FROM accounts WHERE id = $1")
        .bind(o.account)
        .fetch_one(&fx.owner)
        .await
        .unwrap();
    let revoked = routing::org_is_revoked(&fx.app, &o.org).await.unwrap();
    let export = org_deletion::read_export(&fx.exports, &KEK, None, &done).await.unwrap();
    let export = String::from_utf8(export).unwrap();

    assert_eq!(end, RunEnd::Done, "last_error: {:?}", done.last_error);
    assert_eq!(done.state, "done");
    assert_eq!(fx.routing_rows(&o.org).await, 0, "routing row deleted");
    assert!(cred.is_none(), "encrypted DSN deleted");
    assert!(db_gone, "tenant database deleted");
    assert_eq!(done.database_name.as_deref(), Some(fx.db_name(&o.slug).as_str()));
    assert_eq!((job.status.as_str(), accounts), ("ready", 1), "the job row and the accounts row are kept");
    assert!(revoked);
    assert_eq!(fx.clerk_seen.lock().unwrap().as_slice(), &[format!("/v1/organizations/{}", o.org)], "404 counted as done");
    assert!(export.lines().next().unwrap().contains("\"kind\":\"manifest\""));
    assert!(export.contains("\"table\":\"graphs\""), "the tenant's rows are in the export");
    assert!(done.source_manifest.is_some());

    // Rerun every destroying step against what is left: all converge.
    sqlx::query(
        "UPDATE org_deletions SET state = 'destroying', done_at = NULL, \
                completed_steps = ARRAY['trial_end','stripe_cancel','notify_requested','notify_members','revoke_access','manifest','export']::text[] \
          WHERE id = $1",
    )
    .bind(id)
    .execute(&fx.owner)
    .await
    .unwrap();
    let again = fx.drive(id, None).await;
    let again_row = fx.row(id).await;
    assert_eq!(again, RunEnd::Done, "rerun converges: {:?}", again_row.last_error);
    assert_eq!(again_row.state, "done");
    assert_eq!(fx.routing_rows(&o.org).await, 0);
}

#[tokio::test]
async fn a_crash_after_any_destroying_step_resumes_to_the_same_end() {
    let Some(mut fx) = Fx::new("orgt_orgdel_crash").await else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };
    let o = fx.provisioned("org_del_crash").await;
    let id = fx.to_destroying(&o).await;
    for step in ["stripe_sweep", "revoke_access", "manifest", "export", "routing", "credential", "database", "clerk", "notify_done"] {
        let hook: org_deletion::CrashHook = Arc::new(move |s: &str| s == step);
        let end = fx.drive(id, Some(hook)).await;
        let row = fx.row(id).await;
        assert_eq!(end, RunEnd::Crashed, "crash after {step}: {:?}", row.last_error);
        assert!(!row.has(step), "{step}'s checkpoint was not written");
        // The lease of the "dead" process expires.
        sqlx::query("UPDATE org_deletions SET lease_until = NULL WHERE id = $1").bind(id).execute(&fx.owner).await.unwrap();
    }
    let end = fx.drive(id, None).await;
    let row = fx.row(id).await;
    assert_eq!(end, RunEnd::Done, "{:?}", row.last_error);
    assert_eq!(row.state, "done");
    assert!(!fx.db_exists(&o.slug).await);
    assert_eq!(fx.routing_rows(&o.org).await, 0);
}

/// The Neon name check (and the fingerprint check behind it): a recorded id
/// that points at ANOTHER org's database is refused, and so is this org's
/// database once its fingerprint names someone else. Nothing is deleted.
#[tokio::test]
async fn the_database_step_refuses_a_database_not_named_or_fingerprinted_for_the_org() {
    let Some(mut fx) = Fx::new("orgt_orgdel_namecheck").await else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };
    let a = fx.provisioned("org_del_name_a").await;
    let b = fx.provisioned("org_del_name_b").await;
    let a_db = fx.db_name(&a.slug);
    let b_db = fx.db_name(&b.slug);
    // A's job now names B's database.
    sqlx::query("UPDATE tenant_provisioning_jobs SET provider_database_id = $2 WHERE clerk_org_id = $1")
        .bind(&a.org)
        .bind(&b_db)
        .execute(&fx.owner)
        .await
        .unwrap();
    let id = fx.to_destroying(&a).await;
    let end = fx.drive(id, None).await;
    let row = fx.row(id).await;
    assert_eq!(end, RunEnd::Waiting);
    assert_eq!(row.next_step(), Some("database"));
    assert!(row.last_error.as_deref().unwrap_or("").contains("is named"), "{:?}", row.last_error);
    assert!(fx.db_exists(&b.slug).await, "B's database is untouched");
    assert!(fx.db_exists(&a.slug).await, "and A's too");

    // Point it back, but make A's database claim another org.
    sqlx::query("UPDATE tenant_provisioning_jobs SET provider_database_id = $2 WHERE clerk_org_id = $1")
        .bind(&a.org)
        .bind(&a_db)
        .execute(&fx.owner)
        .await
        .unwrap();
    let a_url = with_db(&fx.base, &a_db);
    let tenant = PgPoolOptions::new().max_connections(1).connect(&a_url).await.unwrap();
    sqlx::query("UPDATE tenant_meta SET org_id = 'org_someone_else'").execute(&tenant).await.unwrap();
    tenant.close().await;
    sqlx::query("UPDATE org_deletions SET next_attempt_at = now(), lease_until = NULL WHERE id = $1")
        .bind(id)
        .execute(&fx.owner)
        .await
        .unwrap();
    let end = fx.drive(id, None).await;
    let row = fx.row(id).await;
    assert_eq!(end, RunEnd::Waiting);
    assert!(row.last_error.as_deref().unwrap_or("").contains("fingerprinted"), "{:?}", row.last_error);
    assert!(fx.db_exists(&a.slug).await, "a database fingerprinted for another org is never deleted");

    // Restore the fingerprint: now it goes.
    let tenant = PgPoolOptions::new().max_connections(1).connect(&a_url).await.unwrap();
    sqlx::query("UPDATE tenant_meta SET org_id = $1").bind(&a.org).execute(&tenant).await.unwrap();
    tenant.close().await;
    let end = fx.drive(id, None).await;
    assert_eq!(end, RunEnd::Done, "{:?}", fx.row(id).await.last_error);
    assert!(!fx.db_exists(&a.slug).await);
    assert!(fx.db_exists(&b.slug).await);
    fx.drop_db(&b.slug).await;
}

/// The routing delete is the provisioner's alone, and only for a deletion
/// that is destroying with access already revoked.
#[tokio::test]
async fn only_the_provisioner_deletes_routing_and_only_while_destroying() {
    let Some(mut fx) = Fx::new("orgt_orgdel_privileges").await else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };
    let o = fx.provisioned("org_del_priv").await;
    let d = org_deletion::insert(&fx.app, o.account, &o.org, Initiator::Owner, Some(o.user)).await.unwrap().deletion;

    let as_app = sqlx::query_scalar::<_, i32>("SELECT org_deletion_delete_routing($1)").bind(d.id).fetch_one(&fx.app).await;
    let in_requested = sqlx::query_scalar::<_, i32>("SELECT org_deletion_delete_routing($1)").bind(d.id).fetch_one(&fx.prov).await;
    let unknown = sqlx::query_scalar::<_, i32>("SELECT org_deletion_delete_routing($1)").bind(Uuid::new_v4()).fetch_one(&fx.prov).await;
    // The serving role cannot write the routing table directly either.
    let direct = sqlx::query("DELETE FROM tenant_databases WHERE clerk_org_id = $1").bind(&o.org).execute(&fx.app).await;

    let as_app = as_app.unwrap_err().to_string();
    assert!(as_app.contains("permission denied"), "{as_app}");
    assert!(in_requested.unwrap_err().to_string().contains("not destroying"));
    assert!(unknown.is_err());
    assert!(direct.is_err());
    assert_eq!(fx.routing_rows(&o.org).await, 1, "untouched");
    fx.drop_db(&o.slug).await;
}

/// BLOCKER 1: the owner's grace-period export must carry only what the owner
/// may see (the shared org ocean, the owner's own member ocean, the owner's
/// own conversations), filtered EXPLICITLY in SQL. This runs on the real
/// tenant database through a connection that bypasses row-level security (the
/// tenant owner, as prod tenant pools do), where every row is visible to a
/// plain `SELECT`: so a leak can only be stopped by the export's own filter.
/// The operator's whole-tenant copy still carries everything.
#[tokio::test]
async fn the_owner_export_excludes_other_members_private_oceans_and_conversations() {
    let Some(mut fx) = Fx::new("orgt_orgdel_owner_export").await else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };
    let o = fx.provisioned("org_del_owner_export").await;
    let tenant = fx.data.tenant(&o.org).await.unwrap();
    let owner_user = o.user;
    let other_user = Uuid::new_v4();
    let acct = o.account;

    async fn graph(t: &PgPool, acct: Uuid, name: &str, kind: &str, owner: Option<Uuid>) -> Uuid {
        sqlx::query_scalar("INSERT INTO graphs (account_id, name, kind, owner_user_id) VALUES ($1, $2, $3, $4) RETURNING id")
            .bind(acct)
            .bind(name)
            .bind(kind)
            .bind(owner)
            .fetch_one(t)
            .await
            .unwrap()
    }
    async fn atom(t: &PgPool, g: Uuid, text: &str) -> Uuid {
        sqlx::query_scalar(
            "INSERT INTO atoms (graph_id, source_type, source_id, content_blob_key, content_inline, \"timestamp\") \
             VALUES ($1, 'note', $2, '', $3, now()) RETURNING atom_id",
        )
        .bind(g)
        .bind(format!("src-{text}"))
        .bind(text)
        .fetch_one(t)
        .await
        .unwrap()
    }
    async fn node(t: &PgPool, g: Uuid, title: &str) -> Uuid {
        sqlx::query_scalar("INSERT INTO nodes (graph_id, type, title) VALUES ($1, 'entity', $2) RETURNING node_id")
            .bind(g)
            .bind(title)
            .fetch_one(t)
            .await
            .unwrap()
    }
    async fn convo(t: &PgPool, acct: Uuid, g: Uuid, user: Uuid, title: &str, text: &str) {
        let id: Uuid = sqlx::query_scalar("INSERT INTO conversations (account_id, graph_id, user_id, title) VALUES ($1, $2, $3, $4) RETURNING id")
            .bind(acct)
            .bind(g)
            .bind(user)
            .bind(title)
            .fetch_one(t)
            .await
            .unwrap();
        sqlx::query("INSERT INTO conversation_messages (conversation_id, seq, role, text) VALUES ($1, 1, 'user', $2)")
            .bind(id)
            .bind(text)
            .execute(t)
            .await
            .unwrap();
    }

    // The org's shared ocean was created when the tenant was provisioned.
    let (shared, shared_name): (Uuid, String) = sqlx::query_as("SELECT id, name FROM graphs WHERE kind = 'org'")
        .fetch_one(&tenant)
        .await
        .unwrap();
    let mine = graph(&tenant, acct, "Owner member ocean", "member", Some(owner_user)).await;
    let theirs = graph(&tenant, acct, "Other member ocean", "member", Some(other_user)).await;
    let team = graph(&tenant, acct, "A team ocean", "team", None).await;
    let a_shared = atom(&tenant, shared, "SHARED-ATOM-TEXT").await;
    let a_mine = atom(&tenant, mine, "OWNER-PRIVATE-ATOM-TEXT").await;
    let a_theirs = atom(&tenant, theirs, "SECRET-OTHER-MEMBER-ATOM").await;
    let _a_team = atom(&tenant, team, "TEAM-ATOM-TEXT").await;
    let n_shared = node(&tenant, shared, "SHARED-NODE").await;
    let n_theirs = node(&tenant, theirs, "SECRET-OTHER-MEMBER-NODE").await;
    let n_theirs2 = node(&tenant, theirs, "SECRET-OTHER-MEMBER-NODE-2").await;
    sqlx::query("INSERT INTO node_atoms (node_id, atom_id) VALUES ($1, $2), ($3, $4)")
        .bind(n_shared)
        .bind(a_shared)
        .bind(n_theirs)
        .bind(a_theirs)
        .execute(&tenant)
        .await
        .unwrap();
    sqlx::query("INSERT INTO relationship_edge (graph_id, src_node_id, predicate, dst_node_id) VALUES ($1, $2, 'SECRET-OTHER-MEMBER-EDGE', $3)")
        .bind(theirs)
        .bind(n_theirs)
        .bind(n_theirs2)
        .execute(&tenant)
        .await
        .unwrap();
    convo(&tenant, acct, shared, owner_user, "OWNER-CONVO", "OWNER-CONVO-MESSAGE").await;
    convo(&tenant, acct, shared, other_user, "SECRET-OTHER-USER-CONVO", "SECRET-OTHER-USER-MESSAGE").await;
    convo(&tenant, acct, theirs, other_user, "SECRET-OTHER-MEMBER-OCEAN-CONVO", "SECRET-OTHER-OCEAN-MESSAGE").await;

    // Proof the connection sees everything (so only the export's filter can hide it).
    let atoms_visible: i64 = sqlx::query_scalar("SELECT count(*) FROM atoms").fetch_one(&tenant).await.unwrap();
    let convos_visible: i64 = sqlx::query_scalar("SELECT count(*) FROM conversations").fetch_one(&tenant).await.unwrap();
    let who: String = sqlx::query_scalar("SELECT current_user").fetch_one(&tenant).await.unwrap();
    assert_eq!((atoms_visible, convos_visible), (4, 3), "this pool bypasses RLS ({who}): every row is visible to it");
    assert_ne!(who, "coralswarm_tenant_app");

    let d = org_deletion::insert(&fx.app, o.account, &o.org, Initiator::Owner, Some(owner_user)).await.unwrap().deletion;
    type F = fn(String) -> std::future::Ready<Result<Vec<u8>, String>>;
    let owner_export = build_export::<F, _>(&tenant, &d, &ExportScope::Owner { user_id: owner_user }, None).await.unwrap();
    let owner_export = String::from_utf8(owner_export).unwrap();
    let operator_export = build_export::<F, _>(&tenant, &d, &ExportScope::Operator, None).await.unwrap();
    let operator_export = String::from_utf8(operator_export).unwrap();

    // The size cap is enforced as rows stream, for both scopes: a cap just
    // under the real document fails with the too-large marker, one at the
    // document's size still passes.
    for (scope, full) in [(ExportScope::Owner { user_id: owner_user }, owner_export.len()), (ExportScope::Operator, operator_export.len())] {
        let over = build_export_with_cap::<F, _>(&tenant, &d, &scope, None, full - 1).await.unwrap_err();
        assert!(over.starts_with(org_deletion::EXPORT_TOO_LARGE), "{over}");
        let tiny = build_export_with_cap::<F, _>(&tenant, &d, &scope, None, 64).await.unwrap_err();
        assert!(tiny.starts_with(org_deletion::EXPORT_TOO_LARGE), "{tiny}");
        let exact = build_export_with_cap::<F, _>(&tenant, &d, &scope, None, full + 1).await.unwrap();
        assert_eq!(exact.len(), full);
    }

    for present in ["SHARED-ATOM-TEXT", "OWNER-PRIVATE-ATOM-TEXT", "SHARED-NODE", "OWNER-CONVO", "OWNER-CONVO-MESSAGE", "Owner member ocean", shared_name.as_str()] {
        assert!(owner_export.contains(present), "the owner's export must contain {present}");
    }
    for absent in [
        "SECRET-OTHER-MEMBER-ATOM",
        "SECRET-OTHER-MEMBER-NODE",
        "SECRET-OTHER-MEMBER-EDGE",
        "SECRET-OTHER-USER-CONVO",
        "SECRET-OTHER-USER-MESSAGE",
        "SECRET-OTHER-MEMBER-OCEAN-CONVO",
        "SECRET-OTHER-OCEAN-MESSAGE",
        "Other member ocean",
        "TEAM-ATOM-TEXT",
        &theirs.to_string(),
        &other_user.to_string(),
    ] {
        assert!(!owner_export.contains(absent), "the owner's export must NOT contain {absent}");
    }
    assert!(owner_export.lines().next().unwrap().contains("\"scope\":\"owner\""));
    // The worker's operator copy is the whole tenant.
    for present in ["SECRET-OTHER-MEMBER-ATOM", "SECRET-OTHER-USER-MESSAGE", "SECRET-OTHER-MEMBER-EDGE", "OWNER-CONVO-MESSAGE", "TEAM-ATOM-TEXT"] {
        assert!(operator_export.contains(present), "the operator copy must contain {present}");
    }
    assert!(operator_export.lines().next().unwrap().contains("\"scope\":\"operator\""));
    let _ = (a_mine, mine);
    tenant.close().await;
}

/// A provisioner that finds the project and its name but has no connection
/// string for it (Neon answering 404 on the connection-URI endpoint).
struct NoConnection(LocalPgProvisioner);

#[async_trait]
impl TenantDbProvisioner for NoConnection {
    fn provider_name(&self) -> &'static str {
        self.0.provider_name()
    }
    fn database_name(&self, slug: &str) -> String {
        self.0.database_name(slug)
    }
    async fn create(
        &self,
        name: &str,
        region: Option<&str>,
    ) -> Result<coralswarm_backend::tenancy::provisioner::ProvisionedDatabase, coralswarm_backend::tenancy::provisioner::ProvisionerError> {
        self.0.create(name, region).await
    }
    async fn find_by_name(
        &self,
        name: &str,
    ) -> Result<Option<coralswarm_backend::tenancy::provisioner::ProvisionedDatabase>, coralswarm_backend::tenancy::provisioner::ProvisionerError> {
        self.0.find_by_name(name).await
    }
    async fn connection_dsn(&self, _id: &str) -> Result<Option<String>, coralswarm_backend::tenancy::provisioner::ProvisionerError> {
        Ok(None)
    }
    async fn name_of(&self, id: &str) -> Result<Option<String>, coralswarm_backend::tenancy::provisioner::ProvisionerError> {
        self.0.name_of(id).await
    }
    fn is_selfserve_name(&self, name: &str) -> bool {
        self.0.is_selfserve_name(name)
    }
    async fn delete(
        &self,
        id: &str,
    ) -> Result<coralswarm_backend::tenancy::provisioner::DeleteOutcome, coralswarm_backend::tenancy::provisioner::ProvisionerError> {
        self.0.delete(id).await
    }
}

/// MEDIUM 8: only an ABSENT project (`name_of == None`) means "gone". A
/// project that exists but returns no connection string is not: the step
/// waits with an alert and deletes nothing.
#[tokio::test]
async fn a_missing_connection_string_is_not_proof_the_database_is_gone() {
    let Some(mut fx) = Fx::new("orgt_orgdel_noconn").await else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };
    let o = fx.provisioned("org_del_noconn").await;
    let id = fx.to_destroying(&o).await;
    let strange = Destroyer {
        provisioner: Arc::new(NoConnection(LocalPgProvisioner::new(fx.base.clone()))),
        provisioner_control: fx.prov.clone(),
    };
    let ctx = Ctx { destroyer: Some(&strange), ..fx.ctx(None) };
    let end = org_deletion::run(&ctx, fx.leased(id).await).await;
    let row = fx.row(id).await;

    assert_eq!(end, RunEnd::Waiting, "{:?}", row.last_error);
    assert_eq!(row.next_step(), Some("database"), "routing and credential went; the database step parked");
    assert!(row.last_error.as_deref().unwrap_or("").contains("no connection string"), "{:?}", row.last_error);
    assert!(fx.db_exists(&o.slug).await, "nothing was deleted");
    // The same step with a provider that really has no such project is done.
    let id_gone = {
        sqlx::query("UPDATE tenant_provisioning_jobs SET provider_database_id = 'coralswarm_tenant_does_not_exist' WHERE clerk_org_id = $1")
            .bind(&o.org)
            .execute(&fx.owner)
            .await
            .unwrap();
        sqlx::query("UPDATE org_deletions SET next_attempt_at = now(), lease_until = NULL WHERE id = $1").bind(id).execute(&fx.owner).await.unwrap();
        id
    };
    let end = fx.drive(id_gone, None).await;
    assert_eq!(end, RunEnd::Done, "an absent project is gone: {:?}", fx.row(id).await.last_error);
    fx.drop_db(&o.slug).await;
}

/// MEDIUM 5 and 9: the credential and database steps act only on a job bound
/// to THIS deletion's account, and the routing delete refuses a routing row
/// whose secret ref is not the job's.
#[tokio::test]
async fn the_destroy_steps_require_the_job_and_routing_row_to_belong_to_this_org() {
    let Some(mut fx) = Fx::new("orgt_orgdel_binding").await else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };
    let o = fx.provisioned("org_del_binding").await;
    let secret_ref = provisioning::secret_ref_for_slug(&o.slug);
    let id = fx.to_destroying(&o).await;
    // Run everything up to (and including) the export.
    let hook: org_deletion::CrashHook = Arc::new(|s: &str| s == "export");
    let _ = fx.drive(id, Some(hook)).await;
    sqlx::query("UPDATE org_deletions SET completed_steps = completed_steps || ARRAY['export']::text[], lease_until = NULL WHERE id = $1")
        .bind(id)
        .execute(&fx.owner)
        .await
        .unwrap();

    // (9) A routing row bound to some other secret ref is not deleted.
    let mut tx = coralswarm_backend::tenancy::scope::begin_account_scoped(&fx.owner, o.account).await.unwrap();
    coralswarm_backend::billing::renewal::lock_period_rotation(&mut tx, o.account).await.unwrap();
    sqlx::query("UPDATE tenant_databases SET db_secret_ref = 'TENANT_DB_SOMEONE_ELSE' WHERE clerk_org_id = $1")
        .bind(&o.org)
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let refused = sqlx::query_scalar::<_, i32>("SELECT org_deletion_delete_routing($1)").bind(id).fetch_one(&fx.prov).await;
    assert!(refused.unwrap_err().to_string().contains("not bound to its job"));
    assert_eq!(fx.routing_rows(&o.org).await, 1);
    let mut tx = coralswarm_backend::tenancy::scope::begin_account_scoped(&fx.owner, o.account).await.unwrap();
    coralswarm_backend::billing::renewal::lock_period_rotation(&mut tx, o.account).await.unwrap();
    sqlx::query("UPDATE tenant_databases SET db_secret_ref = $2 WHERE clerk_org_id = $1")
        .bind(&o.org)
        .bind(&secret_ref)
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();

    // (5) The job row names a different account: every provisioner step parks.
    let stranger: Uuid = sqlx::query_scalar("INSERT INTO accounts (owner_user_id) VALUES ($1) RETURNING id")
        .bind(o.user)
        .fetch_one(&fx.owner)
        .await
        .unwrap();
    // The schema derives the slug from the account, so it normally cannot
    // disagree; drop that CHECK in this scratch database to prove the code's
    // own check does not depend on it.
    sqlx::query("ALTER TABLE tenant_provisioning_jobs DROP CONSTRAINT tenant_provisioning_jobs_slug_is_derived")
        .execute(&fx.owner)
        .await
        .unwrap();
    sqlx::query("UPDATE tenant_provisioning_jobs SET account_id = $2 WHERE clerk_org_id = $1")
        .bind(&o.org)
        .bind(stranger)
        .execute(&fx.owner)
        .await
        .unwrap();
    for done in [&["routing"][..], &["routing", "credential"][..]] {
        sqlx::query("UPDATE org_deletions SET completed_steps = completed_steps || $2::text[], next_attempt_at = now(), lease_until = NULL WHERE id = $1 AND NOT (completed_steps @> $2::text[])")
            .bind(id)
            .bind(done.iter().map(|s| s.to_string()).collect::<Vec<_>>())
            .execute(&fx.owner)
            .await
            .unwrap();
        let end = fx.drive(id, None).await;
        let row = fx.row(id).await;
        assert_eq!(end, RunEnd::Waiting, "{:?}", row.last_error);
        assert!(row.last_error.as_deref().unwrap_or("").contains("different account"), "{:?}", row.last_error);
        assert!(fx.db_exists(&o.slug).await, "the database is never deleted on a job bound elsewhere");
        let cred = credentials::fetch_ciphertext(&fx.app, &secret_ref).await.unwrap();
        assert!(cred.is_some(), "the credential is kept");
    }
    // Bound again: it goes.
    sqlx::query("UPDATE tenant_provisioning_jobs SET account_id = $2 WHERE clerk_org_id = $1")
        .bind(&o.org)
        .bind(o.account)
        .execute(&fx.owner)
        .await
        .unwrap();
    sqlx::query("UPDATE org_deletions SET next_attempt_at = now(), lease_until = NULL WHERE id = $1").bind(id).execute(&fx.owner).await.unwrap();
    // `routing` was stamped, not run; the definer-run routing delete still has a row to remove.
    let end = fx.drive(id, None).await;
    assert_eq!(end, RunEnd::Done, "{:?}", fx.row(id).await.last_error);
    assert!(!fx.db_exists(&o.slug).await);
}
