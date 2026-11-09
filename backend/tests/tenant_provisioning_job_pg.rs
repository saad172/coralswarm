//! Self-serve org signup, PR A1b: the tenant provisioning job end to end
//! against a real Postgres, with the LOCAL provisioner (`CREATE DATABASE`)
//! standing in for Neon and the job running as production runs it: reads on
//! a real `coralswarm_app` login (the serving role) and every job, routing
//! and credential write on a real login granted `coralswarm_provisioner`
//! (the worker's own pool, `PROVISIONER_DATABASE_URL`).
//!
//! Proves: the happy path lands every artefact and the binding gate stays
//! closed until `ready`; a crash after ANY step's side effect converges on
//! retry with no duplicate database/credential/routing row; a terminal
//! failure (permanent error, exhausted retries, cancel) deletes exactly what
//! the job created and leaves a `failed` row with a reason and the A2
//! Clerk-org hook set; the lease keeps two runners apart; a personal org is
//! refused at enqueue AND at execution; the provisioned tenant works both as
//! the table owner and as `coralswarm_tenant_app`.
//!
//!   REQUIRE_PG_SUITE=1 ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test tenant_provisioning_job_pg

mod support;

#[path = "support/retained_database.rs"]
mod retained_database;

#[path = "support/financial_fixture.rs"]
mod financial_fixture;

use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use coralswarm_backend::tenancy::credentials;
use coralswarm_backend::tenancy::gate::TokenKind;
use coralswarm_backend::tenancy::migrate;
use coralswarm_backend::tenancy::provisioner::{
    LocalPgProvisioner, NeonProvisioner, ProvisionerError, TenantDbProvisioner,
};
use coralswarm_backend::tenancy::provisioning::{
    self, Deps, EnqueueError, EnqueueOutcome, EnqueueRequest, Fault, ProvisionerSelfCheckError, RetryPolicy,
    RunOutcome, StaticOrgDirectory, Step, SELFSERVE_SIGNUP_FLAG,
};
use coralswarm_backend::tenancy::registry::TenantRegistry;
use coralswarm_backend::tenancy::routing;
use coralswarm_backend::tenancy::runtime::{GateReject, TenancyDecision, TenantRuntime};
use coralswarm_backend::tenancy::secrets::{
    ChainSecretResolver, ControlTableSecretResolver, SecretResolver, StaticMapResolver,
};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{Connection, PgConnection, PgPool};
use support::app_role::{set_app_role_password_with_retry, APP_ROLE_TEST_PASSWORD};
use support::provisioner_role::{prepare_provisioner_login, PROVISIONER_TEST_LOGIN, PROVISIONER_TEST_PASSWORD};
use tokio::sync::Mutex;
use uuid::Uuid;

const APEX: &str = "coralswarm.example";
const KEK: [u8; 32] = [0x51u8; 32];

/// `ORG_SELFSERVE_SIGNUP_ENABLED` is process-global; every enqueue in this
/// binary sets it under this lock, and the one test that needs it UNSET
/// holds the lock for its whole assertion.
static FLAG_LOCK: Mutex<()> = Mutex::const_new(());

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
/// The routing key a self-serve job derives for this account (control 0381
/// pins it): the only slug a job can have.
fn tenant_slug(account_id: Uuid) -> String {
    provisioning::opaque_slug_for_account(account_id)
}
fn as_role(url: &str, user: &str, pass: &str) -> String {
    let scheme_end = url.find("://").expect("url has a scheme") + 3;
    let (scheme, rest) = url.split_at(scheme_end);
    let host_and_rest = rest.split_once('@').map(|(_, h)| h).unwrap_or(rest);
    format!("{scheme}{user}:{pass}@{host_and_rest}")
}

struct Fx {
    base: String,
    admin: PgConnection,
    owner: PgPool,
    app: PgPool,
    /// A real login granted `coralswarm_provisioner`: the worker's writes.
    prov: PgPool,
    resolver: Arc<dyn SecretResolver>,
    registry: Arc<TenantRegistry>,
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
        let app = PgPoolOptions::new()
            .max_connections(6)
            .connect(&as_role(&url, "coralswarm_app", APP_ROLE_TEST_PASSWORD))
            .await
            .expect("connect as coralswarm_app");
        prepare_provisioner_login(&owner).await;
        let prov = PgPoolOptions::new()
            // Two: the worker issues one statement at a time, and the suite
            // runs every test concurrently against one local server.
            .max_connections(2)
            .connect(&as_role(&url, PROVISIONER_TEST_LOGIN, PROVISIONER_TEST_PASSWORD))
            .await
            .expect("connect as the provisioner login");
        let chain: Arc<dyn SecretResolver> = Arc::new(ChainSecretResolver::new(
            Arc::new(StaticMapResolver::from_pairs::<[(&str, &str); 0], _, _>([])),
            ControlTableSecretResolver::new(app.clone(), KEK, None),
        ));
        let registry = Arc::new(TenantRegistry::new(chain.clone()));
        Some(Fx {
            base,
            admin,
            owner,
            app,
            prov,
            resolver: chain,
            registry,
        })
    }

    fn deps(&self, owner: &str, fault: Option<Fault>, on: Option<Step>) -> Deps {
        let fault_hook: Option<provisioning::FaultHook> = match (fault, on) {
            (Some(f), Some(step)) => Some(Arc::new(move |s: Step| if s == step { Some(f) } else { None })),
            _ => None,
        };
        Deps {
            control: self.app.clone(),
            provisioner_control: self.prov.clone(),
            provisioner: Arc::new(LocalPgProvisioner::new(self.base.clone())),
            directory: Arc::new(StaticOrgDirectory::selfserve(
                ORGS.iter().map(|(o, s)| (o.to_string(), s.to_string())),
            )),
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
            owner: owner.to_string(),
            fault: fault_hook,
        }
    }

    /// An org account with a LIVE subscription (Checkout completed), the
    /// state the Stripe webhook enqueues from.
    async fn org_account(&self, org: &str) -> (Uuid, Uuid) {
        let ids = self.org_account_unpaid(org).await;
        self.set_subscription(ids.0, "active", Some(&format!("sub_{org}"))).await;
        ids
    }

    /// Upsert the account's `stripe_subscriptions` mirror (owner pool: test
    /// setup, never the code under test).
    async fn set_subscription(&self, account_id: Uuid, status: &str, sub_id: Option<&str>) {
        // A financial row: 0321's guard needs the account entered, which
        // `lock_period_rotation` does inside an account-scoped transaction.
        let mut tx = coralswarm_backend::tenancy::scope::begin_account_scoped(&self.owner, account_id)
            .await
            .unwrap();
        coralswarm_backend::billing::renewal::lock_period_rotation(&mut tx, account_id)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO stripe_subscriptions (account_id, stripe_customer_id, stripe_subscription_id, status) \
             VALUES ($1, 'cus_' || replace($1::text, '-', ''), $2, $3) \
             ON CONFLICT (account_id) DO UPDATE \
                SET stripe_subscription_id = EXCLUDED.stripe_subscription_id, status = EXCLUDED.status",
        )
        .bind(account_id)
        .bind(sub_id)
        .bind(status)
        .execute(&mut *tx)
        .await
        .unwrap();
        tx.commit().await.unwrap();
    }

    async fn org_account_unpaid(&self, org: &str) -> (Uuid, Uuid) {
        sqlx::query_as(
            "WITH u AS (INSERT INTO users(email) VALUES($1) RETURNING id) \
             INSERT INTO accounts(owner_user_id, clerk_org_id) SELECT id, $2 FROM u \
             RETURNING id, owner_user_id",
        )
        .bind(format!("{org}@provisioning-job.test"))
        .bind(org)
        .fetch_one(&self.owner)
        .await
        .unwrap()
    }

    async fn enqueue(&self, org: &str, account_id: Uuid, slug: &str) -> Result<EnqueueOutcome, EnqueueError> {
        let _g = FLAG_LOCK.lock().await;
        std::env::set_var(SELFSERVE_SIGNUP_FLAG, "1");
        provisioning::enqueue(
            &self.app,
            EnqueueRequest {
                clerk_org_id: org.into(),
                account_id,
                slug: slug.into(),
                region: None,
            },
        )
        .await
    }

    async fn db_count(&mut self, slug: &str) -> i64 {
        sqlx::query_scalar("SELECT count(*) FROM pg_database WHERE datname = $1")
            .bind(LocalPgProvisioner::new(self.base.clone()).database_name(slug))
            .fetch_one(&mut self.admin)
            .await
            .unwrap()
    }

    async fn drop_tenant_db(&mut self, slug: &str) {
        let name = LocalPgProvisioner::new(self.base.clone()).database_name(slug);
        retained_database::finish(&mut self.admin, &name, true).await.unwrap();
    }

    /// Everything a ready tenant must have, counted. Asserts exactly one of each.
    async fn assert_ready_artefacts(&mut self, org: &str, slug: &str, account_id: Uuid) {
        assert_eq!(self.db_count(slug).await, 1, "exactly one database for {slug}");
        let secret_ref = provisioning::secret_ref_for_slug(slug);
        assert!(credentials::fetch_ciphertext(&self.app, &secret_ref).await.unwrap().is_some());
        let row = routing::lookup_by_org(&self.app, org).await.unwrap().expect("routing row");
        assert_eq!(row.status, "active");
        assert_eq!(row.schema_version, Some(migrate::head_version()));
        assert_eq!(row.db_secret_ref, secret_ref);
        let job = provisioning::get(&self.app, org).await.unwrap().unwrap();
        assert_eq!((job.status.as_str(), job.step.as_str()), ("ready", "done"));
        assert!(job.ready_at.is_some() && job.lease_owner.is_none());
        assert_eq!(job.provider.as_deref(), Some("local"));
        assert_eq!(job.provider_database_id.as_deref(), Some(LocalPgProvisioner::new(self.base.clone()).database_name(slug).as_str()));

        let dsn = self.resolver.resolve_async(&secret_ref).await.expect("resolves through the table");
        let tenant = PgPoolOptions::new().max_connections(2).connect(&dsn).await.unwrap();
        let fp: String = sqlx::query_scalar("SELECT org_id FROM tenant_meta WHERE id = true")
            .fetch_one(&tenant)
            .await
            .unwrap();
        assert_eq!(fp, org);
        let keys: i64 = sqlx::query_scalar("SELECT count(*) FROM account_keys WHERE account_id = $1")
            .bind(account_id)
            .fetch_one(&tenant)
            .await
            .unwrap();
        assert_eq!(keys, 1, "exactly one DEK");
        let oceans: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM graphs WHERE account_id = $1 AND kind = 'org'",
        )
        .bind(account_id)
        .fetch_one(&tenant)
        .await
        .unwrap();
        assert_eq!(oceans, 1, "exactly one org ocean");
        let applied: i64 = sqlx::query_scalar("SELECT max(version) FROM _sqlx_migrations")
            .fetch_one(&tenant)
            .await
            .unwrap();
        assert_eq!(applied, migrate::head_version());
        assert_lifecycle_contract_applied(&tenant).await;
        tenant.close().await;
    }
}

/// The P2 identity contract has been applied: the composite identity index
/// is the only one, and the readiness predicate every managed admission
/// path checks (`corpus_enrichment::lifecycle_ready_tx`) is true.
async fn assert_lifecycle_contract_applied(tenant: &PgPool) {
    let (composite, legacy): (bool, bool) = sqlx::query_as(
        "SELECT to_regclass('nodes_graph_type_lower_title_identity_unique') IS NOT NULL, \
                to_regclass('nodes_graph_type_lower_title_unique') IS NOT NULL",
    )
    .fetch_one(tenant)
    .await
    .unwrap();
    assert!(composite, "composite identity index present");
    assert!(!legacy, "legacy nodes_graph_type_lower_title_unique index dropped");
    let mut tx = tenant.begin().await.unwrap();
    assert!(
        coralswarm_backend::corpus_enrichment::lifecycle_ready_tx(&mut tx).await.unwrap(),
        "a freshly provisioned tenant is lifecycle-ready"
    );
    tx.rollback().await.unwrap();
}

/// The orgs the static Clerk directory knows (org id, Clerk slug). The Clerk
/// slug is informational only: every job's routing slug is
/// [`tenant_slug`], derived from its account.
const ORGS: &[(&str, &str)] = &[
    ("org_a1_version_park", "a1-version-park"),
    ("org_a1_ahead", "a1-ahead"),
    ("org_a1_panic", "a1-panic"),
    ("org_a1_retry_definer", "a1-retry-definer"),
    ("org_a1_retry_clerk_gone", "a1-retry-clerk-gone"),
    ("org_a1_retry_routed", "a1-retry-routed"),
    ("org_a1_happy", "a1-happy"),
    ("org_a1_crash_verify_org", "a1-crash-verify-org"),
    ("org_a1_crash_create_database", "a1-crash-create-database"),
    ("org_a1_crash_insert_routing", "a1-crash-insert-routing"),
    ("org_a1_crash_migrate", "a1-crash-migrate"),
    ("org_a1_crash_contract", "a1-crash-contract"),
    ("org_a1_crash_fingerprint", "a1-crash-fingerprint"),
    ("org_a1_crash_provision_dek", "a1-crash-provision-dek"),
    ("org_a1_crash_activate", "a1-crash-activate"),
    ("org_a1_perm", "a1-perm"),
    ("org_a1_exhaust", "a1-exhaust"),
    ("org_a1_cancel", "a1-cancel"),
    ("org_a1_lease", "a1-lease"),
    ("org_a1_personal_late", "a1-personal-late"),
    ("org_a1_personal_early", "a1-personal-early"),
    ("org_a1_unmarked", "a1-unmarked"),
    ("org_a1_unpaid", "a1-unpaid"),
    ("org_a1_canceled", "a1-canceled"),
    ("org_a1_canceled_before_create", "a1-canceled-before-create"),
    ("org_a1_webhook", "a1-webhook"),
    ("org_a1_misnamed", "a1-misnamed"),
    ("org_a1_orphan", "a1-orphan"),
    ("org_a1_cred_unrecorded", "a1-cred-unrecorded"),
    ("org_a1_ach", "a1-ach"),
];

#[tokio::test]
async fn happy_path_lands_every_artefact_and_the_gate_opens_only_on_ready() {
    let Some(mut fx) = Fx::new("orgt_prov_job_happy").await else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };
    let (org, _) = ORGS[0];
    let (account_id, user_id) = fx.org_account(org).await;
    let slug = tenant_slug(account_id);
    let slug = slug.as_str();
    fx.drop_tenant_db(slug).await;

    assert_eq!(fx.enqueue(org, account_id, slug).await.unwrap(), EnqueueOutcome::Enqueued);
    assert!(matches!(
        fx.enqueue(org, account_id, slug).await.unwrap(),
        EnqueueOutcome::AlreadyExists { .. }
    ));

    // Before the job runs: no routing row at all, so the gate 404s / the
    // org is not provisioned. Mid-job it is `provisioning` → 503. Either
    // way: blocked. Drive the job to just after the routing insert to see
    // the 503 specifically.
    let deps = fx.deps("runner-a", Some(Fault::CrashAfterEffect), Some(Step::InsertRouting));
    let out = provisioning::run_job(&deps, org).await.unwrap().unwrap();
    assert!(matches!(out, RunOutcome::Deferred { step: Step::InsertRouting, .. }), "{out:?}");
    let rt = TenantRuntime::new(true, APEX, fx.registry.clone());
    assert!(matches!(
        rt.resolve_binding(&fx.app, APEX, TokenKind::Org(org.into()), account_id, user_id, None).await,
        Err(GateReject::Provisioning)
    ), "a provisioning org must be blocked with 503 (readable as provisioning, signup PR A2)");

    let deps = fx.deps("runner-a", None, None);
    let out = provisioning::run_job(&deps, org).await.unwrap().unwrap();
    assert_eq!(out, RunOutcome::Ready);
    fx.assert_ready_artefacts(org, slug, account_id).await;

    // A fresh runtime (no 30s routing cache) now binds the org to ITS tenant.
    let rt = TenantRuntime::new(true, APEX, fx.registry.clone());
    match rt.resolve_binding(&fx.app, APEX, TokenKind::Org(org.into()), account_id, user_id, None).await {
        Ok(TenancyDecision::Org(ctx)) => {
            let fp: String = sqlx::query_scalar("SELECT org_id FROM tenant_meta")
                .fetch_one(&ctx.pool)
                .await
                .unwrap();
            assert_eq!(fp, org);
        }
        other => panic!("expected an org binding after ready, got {other:?}"),
    }

    // Idempotent: a second run of a ready job is a no-op (no live job).
    assert!(provisioning::run_job(&deps, org).await.unwrap().is_none());
    // The release orchestrator now selects it like any other active tenant.
    assert!(routing::list_migratable(&fx.app).await.unwrap().iter().any(|t| t.clerk_org_id == org));
    fx.drop_tenant_db(slug).await;
}

#[tokio::test]
async fn provisioned_tenant_works_as_owner_and_as_coralswarm_tenant_app() {
    let Some(mut fx) = Fx::new("orgt_prov_job_roles").await else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };
    let org = "org_a1_roles";
    let (account_id, user_id) = fx.org_account(org).await;
    let slug = tenant_slug(account_id);
    let slug = slug.as_str();
    fx.drop_tenant_db(slug).await;
    fx.enqueue(org, account_id, slug).await.unwrap();
    let mut deps = fx.deps("runner-r", None, None);
    deps.directory = Arc::new(StaticOrgDirectory::selfserve([(org.to_string(), slug.to_string())]));
    assert_eq!(provisioning::run_job(&deps, org).await.unwrap().unwrap(), RunOutcome::Ready);

    let secret_ref = provisioning::secret_ref_for_slug(slug);
    let owner_dsn = fx.resolver.resolve_async(&secret_ref).await.unwrap();
    let owner = PgPoolOptions::new().max_connections(2).connect(&owner_dsn).await.unwrap();
    // Owner (production's `neondb_owner` shape): everything readable.
    let n: i64 = sqlx::query_scalar("SELECT count(*) FROM graphs").fetch_one(&owner).await.unwrap();
    assert_eq!(n, 1);
    assert_lifecycle_contract_applied(&owner).await;
    support::tenant_role::set_app_role_password_with_retry(&owner).await;

    // The least-privilege tenant role, a REAL login: the tables the request
    // path reads must be reachable, and the org ocean visible to the system
    // actor under 0009's policies.
    let opts = PgConnectOptions::from_str(&owner_dsn).unwrap();
    let app_dsn = format!(
        "postgres://coralswarm_tenant_app:{}@{}:{}/{}",
        support::tenant_role::APP_ROLE_TEST_PASSWORD,
        opts.get_host(),
        opts.get_port(),
        opts.get_database().unwrap()
    );
    let app = PgPoolOptions::new().max_connections(2).connect(&app_dsn).await.expect("connect as coralswarm_tenant_app");
    let (role, bypass): (String, bool) =
        sqlx::query_as("SELECT current_user::text, rolbypassrls FROM pg_roles WHERE rolname = current_user")
            .fetch_one(&app)
            .await
            .unwrap();
    assert_eq!(role, "coralswarm_tenant_app");
    assert!(!bypass);
    let fp: String = sqlx::query_scalar("SELECT org_id FROM tenant_meta WHERE id = true").fetch_one(&app).await.unwrap();
    assert_eq!(fp, org);
    let keys: i64 = sqlx::query_scalar("SELECT count(*) FROM account_keys").fetch_one(&app).await.unwrap();
    assert_eq!(keys, 1);
    let mut tx = app.begin().await.unwrap();
    sqlx::query("SELECT set_config('app.tenant_actor', 'system', true)").execute(&mut *tx).await.unwrap();
    let oceans: i64 = sqlx::query_scalar("SELECT count(*) FROM graphs WHERE kind = 'org'")
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    assert_eq!(oceans, 1, "the system actor sees the org ocean under tenant RLS");
    // The managed admission paths evaluate readiness inside the tenant
    // content transaction, as the tenant app role: system-scoped (workers)
    // and member-scoped (the request path).
    let mut tx = coralswarm_backend::tenancy::scope::begin_tenant_system_scoped(&app, account_id).await.unwrap();
    assert!(
        coralswarm_backend::corpus_enrichment::lifecycle_ready_tx(&mut tx).await.unwrap(),
        "lifecycle-ready as coralswarm_tenant_app (system scope)"
    );
    tx.rollback().await.unwrap();
    let mut tx = coralswarm_backend::tenancy::scope::begin_tenant_scoped(&app, account_id, user_id).await.unwrap();
    assert!(
        coralswarm_backend::corpus_enrichment::lifecycle_ready_tx(&mut tx).await.unwrap(),
        "lifecycle-ready as coralswarm_tenant_app (member scope)"
    );
    tx.rollback().await.unwrap();
    app.close().await;
    owner.close().await;
    fx.drop_tenant_db(slug).await;
}

#[tokio::test]
async fn a_crash_after_any_step_converges_on_retry_with_no_duplicates() {
    let Some(mut fx) = Fx::new("orgt_prov_job_crash").await else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };
    for step in Step::FORWARD {
        let (org, _) = ORGS
            .iter()
            .find(|(o, _)| *o == format!("org_a1_crash_{}", step.as_str()))
            .copied()
            .unwrap();
        let (account_id, _user) = fx.org_account(org).await;
        let slug = tenant_slug(account_id);
        let slug = slug.as_str();
        fx.drop_tenant_db(slug).await;
        fx.enqueue(org, account_id, slug).await.unwrap();

        // Attempt 1: the step's side effects happen, then the process "dies"
        // before recording progress.
        let deps = fx.deps("runner-c", Some(Fault::CrashAfterEffect), Some(step));
        let out = provisioning::run_job(&deps, org).await.unwrap().unwrap();
        let job = provisioning::get(&fx.app, org).await.unwrap().unwrap();
        if step == Step::Activate {
            // Activation is one atomic definer call that also flips the job
            // to ready; a crash after it has nothing left to redo.
            assert_eq!(job.status, "ready", "{out:?}");
        } else {
            assert!(
                matches!(&out, RunOutcome::Deferred { step: s, attempts: 1, .. } if *s == step),
                "step {step:?}: {out:?}"
            );
            assert_eq!(job.step, step.as_str(), "the job is still on the crashed step");
            assert!(job.lease_owner.is_none(), "the lease is released on defer");
            // Attempt 2: converge.
            let deps = fx.deps("runner-c", None, None);
            assert_eq!(provisioning::run_job(&deps, org).await.unwrap().unwrap(), RunOutcome::Ready, "step {step:?}");
        }
        fx.assert_ready_artefacts(org, slug, account_id).await;
        fx.drop_tenant_db(slug).await;
    }
}

#[tokio::test]
async fn terminal_failure_cleans_up_exactly_what_was_created_and_records_why() {
    let Some(mut fx) = Fx::new("orgt_prov_job_fail").await else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };

    // ── A permanent error after the routing row exists ───────────────────
    let org = "org_a1_perm";
    let (account_id, _user) = fx.org_account(org).await;
    let slug = tenant_slug(account_id);
    let slug = slug.as_str();
    fx.drop_tenant_db(slug).await;
    fx.enqueue(org, account_id, slug).await.unwrap();
    let deps = fx.deps("runner-f", Some(Fault::Permanent), Some(Step::Fingerprint));
    let out = provisioning::run_job(&deps, org).await.unwrap().unwrap();
    assert!(matches!(&out, RunOutcome::Failed { reason } if reason.contains("fingerprint")), "{out:?}");
    let job = provisioning::get(&fx.app, org).await.unwrap().unwrap();
    assert_eq!(job.status, "failed");
    assert!(job.failed_at.is_some());
    assert_eq!(job.clerk_org_cleanup, "pending", "A2's hook is set");
    assert!(job.last_error.as_deref().unwrap().contains("injected permanent failure"));
    assert!(routing::lookup_by_org(&fx.app, org).await.unwrap().is_none(), "routing row deleted");
    assert!(credentials::fetch_ciphertext(&fx.app, &provisioning::secret_ref_for_slug(slug)).await.unwrap().is_none(), "credential deleted");
    assert_eq!(fx.db_count(slug).await, 0, "database deleted");
    // Listed for A2, and settled once A2 acts.
    let pending = provisioning::list_pending_clerk_org_cleanup(&fx.app).await.unwrap();
    assert!(pending.iter().any(|j| j.clerk_org_id == org));
    // Since control 0384 "done" is only reachable from the sweep's own
    // CURRENT claim (`pending → deleting`, its token), never straight from
    // `pending`.
    assert!(!provisioning::mark_clerk_org_cleanup_done(&fx.app, org, chrono::Utc::now()).await.unwrap());
    let stale = std::time::Duration::from_secs(600);
    let claim = provisioning::claim_clerk_org_cleanup(&fx.app, org, stale).await.unwrap().expect("claimed");
    assert!(!provisioning::mark_clerk_org_cleanup_done(&fx.app, org, claim + chrono::Duration::microseconds(1)).await.unwrap(),
        "a different token is not the claim");
    assert!(provisioning::mark_clerk_org_cleanup_done(&fx.app, org, claim).await.unwrap());
    assert!(!provisioning::mark_clerk_org_cleanup_done(&fx.app, org, claim).await.unwrap());
    // No live job remains to run.
    assert!(provisioning::run_job(&fx.deps("runner-f", None, None), org).await.unwrap().is_none());

    // ── Exhausted retries (bounded: max_attempts = 3) ─────────────────────
    let org = "org_a1_exhaust";
    let (account_id, _user) = fx.org_account(org).await;
    let slug = tenant_slug(account_id);
    let slug = slug.as_str();
    fx.drop_tenant_db(slug).await;
    fx.enqueue(org, account_id, slug).await.unwrap();
    let deps = fx.deps("runner-f", Some(Fault::CrashAfterEffect), Some(Step::Migrate));
    let out1 = provisioning::run_job(&deps, org).await.unwrap().unwrap();
    assert!(matches!(out1, RunOutcome::Deferred { attempts: 1, .. }), "{out1:?}");
    let out2 = provisioning::run_job(&deps, org).await.unwrap().unwrap();
    assert!(matches!(out2, RunOutcome::Deferred { attempts: 2, .. }), "{out2:?}");
    let job = provisioning::get(&fx.app, org).await.unwrap().unwrap();
    assert!(job.next_attempt_at >= job.updated_at - chrono::Duration::seconds(1), "backoff scheduled");
    let out3 = provisioning::run_job(&deps, org).await.unwrap().unwrap();
    assert!(matches!(&out3, RunOutcome::Failed { reason } if reason.contains("failed 3 times")), "{out3:?}");
    assert!(routing::lookup_by_org(&fx.app, org).await.unwrap().is_none());
    assert_eq!(fx.db_count(slug).await, 0);
    let job = provisioning::get(&fx.app, org).await.unwrap().unwrap();
    assert_eq!((job.status.as_str(), job.clerk_org_cleanup.as_str()), ("failed", "pending"));

    // ── Explicit cancel mid-flight ────────────────────────────────────────
    let org = "org_a1_cancel";
    let (account_id, _user) = fx.org_account(org).await;
    let slug = tenant_slug(account_id);
    let slug = slug.as_str();
    fx.drop_tenant_db(slug).await;
    fx.enqueue(org, account_id, slug).await.unwrap();
    let deps = fx.deps("runner-f", Some(Fault::CrashAfterEffect), Some(Step::Migrate));
    provisioning::run_job(&deps, org).await.unwrap().unwrap();
    assert_eq!(fx.db_count(slug).await, 1);
    assert!(provisioning::cancel(&fx.app, org).await.unwrap());
    let out = provisioning::run_job(&fx.deps("runner-f", None, None), org).await.unwrap().unwrap();
    assert_eq!(out, RunOutcome::Failed { reason: "cancelled".into() });
    assert_eq!(fx.db_count(slug).await, 0);
    assert!(routing::lookup_by_org(&fx.app, org).await.unwrap().is_none());
    assert!(!provisioning::cancel(&fx.app, org).await.unwrap(), "nothing live to cancel");

    // A tenant that is NOT this job's is never touched: the happy org from
    // another database name still exists only if we created it (we did not).
    assert_eq!(fx.db_count("a1-happy").await, 0);
}

#[tokio::test]
async fn contract_refuses_a_database_fingerprinted_for_another_org() {
    let Some(mut fx) = Fx::new("orgt_prov_job_contract_foreign").await else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };
    let org = "org_a1_contract_foreign";
    let (account_id, _user) = fx.org_account(org).await;
    let slug = tenant_slug(account_id);
    let slug = slug.as_str();
    fx.drop_tenant_db(slug).await;
    fx.enqueue(org, account_id, slug).await.unwrap();
    let mut deps = fx.deps("runner-x", Some(Fault::CrashAfterEffect), Some(Step::Migrate));
    deps.directory = Arc::new(StaticOrgDirectory::selfserve([(org.to_string(), slug.to_string())]));
    let out = provisioning::run_job(&deps, org).await.unwrap().unwrap();
    assert!(matches!(out, RunOutcome::Deferred { step: Step::Migrate, .. }), "{out:?}");

    // Migrate's own post-apply check also refuses a foreign fingerprint, so
    // record migrate as done (the state after a crash between its record and
    // the next step) and plant the fingerprint after it: the contract must
    // refuse the database before changing it.
    sqlx::query("UPDATE tenant_provisioning_jobs SET step = 'contract' WHERE clerk_org_id = $1")
        .bind(org)
        .execute(&fx.owner)
        .await
        .unwrap();
    let dsn = fx.resolver.resolve_async(&provisioning::secret_ref_for_slug(slug)).await.unwrap();
    let tenant = PgPoolOptions::new().max_connections(1).connect(&dsn).await.unwrap();
    sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, 'org_someone_else')")
        .execute(&tenant)
        .await
        .unwrap();
    tenant.close().await;

    deps.fault = None;
    let out = provisioning::run_job(&deps, org).await.unwrap().unwrap();
    assert!(
        matches!(&out, RunOutcome::Failed { reason } if reason.starts_with("step contract:") && reason.contains("different org")),
        "{out:?}"
    );
    // Cleanup refuses to delete a database another org's fingerprint now
    // claims, even though this job created it; what the job made is removed.
    assert!(
        matches!(&out, RunOutcome::Failed { reason } if reason.contains("cleanup refused") && reason.contains("org_someone_else")),
        "{out:?}"
    );
    assert_eq!(fx.db_count(slug).await, 1, "the database is left in place");
    assert!(routing::lookup_by_org(&fx.app, org).await.unwrap().is_none());
    fx.drop_tenant_db(slug).await;
}

/// Captures `tracing` output for the current-thread test runtime, so a test
/// can assert the alertable `error!` (`connector_health = true`) was emitted.
#[derive(Clone, Default)]
struct LogBuf(Arc<std::sync::Mutex<Vec<u8>>>);
impl std::io::Write for LogBuf {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(b);
        Ok(b.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for LogBuf {
    type Writer = LogBuf;
    fn make_writer(&'a self) -> LogBuf {
        self.clone()
    }
}
impl LogBuf {
    fn install(&self) -> tracing::subscriber::DefaultGuard {
        tracing::subscriber::set_default(
            tracing_subscriber::fmt().with_writer(self.clone()).with_ansi(false).finish(),
        )
    }
    fn text(&self) -> String {
        String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
    }
}

/// Drive a job until it stops deferring (bounded).
async fn run_to_terminal(deps: &Deps, org: &str) -> RunOutcome {
    for _ in 0..8 {
        match provisioning::run_job(deps, org).await.unwrap().expect("a live job") {
            RunOutcome::Deferred { .. } => continue,
            other => return other,
        }
    }
    panic!("job {org} never reached a terminal outcome");
}

/// Someone else's database, by the name this job would give its own: a fully
/// migrated tenant tree fingerprinted for `owner_org`, plus a canary row.
async fn plant_foreign_database(fx: &mut Fx, slug: &str, owner_org: Option<&str>) -> String {
    let name = LocalPgProvisioner::new(fx.base.clone()).database_name(slug);
    fx.drop_tenant_db(slug).await;
    sqlx::Executor::execute(&mut fx.admin, format!("CREATE DATABASE \"{name}\"").as_str())
        .await
        .unwrap();
    let foreign = PgPoolOptions::new().max_connections(1).connect(&with_db(&fx.base, &name)).await.unwrap();
    if let Some(owner_org) = owner_org {
        sqlx::migrate!("./migrations_tenant").run(&foreign).await.expect("tenant tree on the foreign db");
        sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, $1)")
            .bind(owner_org)
            .execute(&foreign)
            .await
            .unwrap();
    }
    sqlx::Executor::execute(&foreign, "CREATE TABLE canary (v text); INSERT INTO canary VALUES ('theirs')")
        .await
        .unwrap();
    foreign.close().await;
    name
}

/// The foreign database is exactly as it was planted.
async fn assert_foreign_database_untouched(fx: &mut Fx, slug: &str, owner_org: Option<&str>, migrations: Option<i64>) {
    assert_eq!(fx.db_count(slug).await, 1, "the other org's database still exists");
    let name = LocalPgProvisioner::new(fx.base.clone()).database_name(slug);
    let foreign = PgPoolOptions::new().max_connections(1).connect(&with_db(&fx.base, &name)).await.unwrap();
    let canary: String = sqlx::query_scalar("SELECT v FROM canary").fetch_one(&foreign).await.unwrap();
    assert_eq!(canary, "theirs");
    if let Some(owner_org) = owner_org {
        let fp: String = sqlx::query_scalar("SELECT org_id FROM tenant_meta WHERE id = true")
            .fetch_one(&foreign)
            .await
            .unwrap();
        assert_eq!(fp, owner_org, "fingerprint unchanged");
        let applied: i64 = sqlx::query_scalar("SELECT count(*) FROM _sqlx_migrations").fetch_one(&foreign).await.unwrap();
        assert_eq!(Some(applied), migrations, "no migration applied to it");
    }
    foreign.close().await;
}

#[tokio::test]
async fn a_preexisting_database_fingerprinted_for_another_org_is_refused_and_never_deleted() {
    let Some(mut fx) = Fx::new("orgt_prov_job_name_collision").await else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };
    let org = "org_a1_name_collision";
    let (account_id, _user) = fx.org_account(org).await;
    let slug = tenant_slug(account_id);
    let slug = slug.as_str();
    let name = plant_foreign_database(&mut fx, slug, Some("org_someone_else")).await;
    let migrations = {
        let foreign = PgPoolOptions::new().max_connections(1).connect(&with_db(&fx.base, &name)).await.unwrap();
        let n: i64 = sqlx::query_scalar("SELECT count(*) FROM _sqlx_migrations").fetch_one(&foreign).await.unwrap();
        foreign.close().await;
        n
    };
    fx.enqueue(org, account_id, slug).await.unwrap();
    let mut deps = fx.deps("runner-n", None, None);
    deps.directory = Arc::new(StaticOrgDirectory::selfserve([(org.to_string(), slug.to_string())]));

    let logs = LogBuf::default();
    let out = {
        let _g = logs.install();
        run_to_terminal(&deps, org).await
    };
    assert!(
        matches!(&out, RunOutcome::Failed { reason }
            if reason.starts_with("step create_database:") && reason.contains(&name) && reason.contains("org_someone_else")),
        "{out:?}"
    );
    assert_foreign_database_untouched(&mut fx, slug, Some("org_someone_else"), Some(migrations)).await;
    let job = provisioning::get(&fx.app, org).await.unwrap().unwrap();
    assert_eq!(job.status, "failed");
    assert_eq!(job.provider_database_id, None, "never recorded as this job's");
    assert_eq!(job.database_create_requested_at, None);
    assert!(credentials::fetch_ciphertext(&fx.app, &provisioning::secret_ref_for_slug(slug)).await.unwrap().is_none());
    assert!(routing::lookup_by_org(&fx.app, org).await.unwrap().is_none());
    let text = logs.text();
    assert!(
        text.lines().any(|l| l.contains("ERROR") && l.contains("connector_health=true") && l.contains(&name) && l.contains("org_someone_else")),
        "an alertable error names the collision:\n{text}"
    );
    fx.drop_tenant_db(slug).await;
}

/// A database by this job's name that the job never asked to create is
/// someone else's even without a fingerprint: `scripts/onboard_tenant.sh`
/// creates a concierge tenant's database before its routing row, so a
/// half-onboarded tenant looks exactly like this. Refused, never touched.
#[tokio::test]
async fn a_preexisting_unfingerprinted_database_is_refused_and_left_untouched() {
    let Some(mut fx) = Fx::new("orgt_prov_job_unclaimed").await else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };
    let org = "org_a1_unclaimed";
    let (account_id, _user) = fx.org_account(org).await;
    let slug = tenant_slug(account_id);
    let slug = slug.as_str();
    let name = plant_foreign_database(&mut fx, slug, None).await;
    fx.enqueue(org, account_id, slug).await.unwrap();
    let mut deps = fx.deps("runner-a", None, None);
    deps.directory = Arc::new(StaticOrgDirectory::selfserve([(org.to_string(), slug.to_string())]));

    let logs = LogBuf::default();
    let out = {
        let _g = logs.install();
        run_to_terminal(&deps, org).await
    };
    assert!(
        matches!(&out, RunOutcome::Failed { reason }
            if reason.starts_with("step create_database:") && reason.contains(&name) && reason.contains("did not create")),
        "{out:?}"
    );
    assert_foreign_database_untouched(&mut fx, slug, None, None).await;
    let foreign = PgPoolOptions::new().max_connections(1).connect(&with_db(&fx.base, &name)).await.unwrap();
    let (migrations, meta): (bool, bool) =
        sqlx::query_as("SELECT to_regclass('_sqlx_migrations') IS NOT NULL, to_regclass('tenant_meta') IS NOT NULL")
            .fetch_one(&foreign)
            .await
            .unwrap();
    foreign.close().await;
    assert_eq!((migrations, meta), (false, false), "no migration applied, no fingerprint written");
    let job = provisioning::get(&fx.app, org).await.unwrap().unwrap();
    assert_eq!(job.status, "failed");
    assert_eq!(job.provider_database_id, None, "never recorded as this job's");
    assert_eq!(job.routing_inserted_at, None);
    assert!(credentials::fetch_ciphertext(&fx.app, &provisioning::secret_ref_for_slug(slug)).await.unwrap().is_none());
    assert!(routing::lookup_by_org(&fx.app, org).await.unwrap().is_none());
    let text = logs.text();
    assert!(
        text.lines().any(|l| l.contains("ERROR") && l.contains("connector_health=true") && l.contains(&name) && l.contains("did not create")),
        "an alertable error names the database:\n{text}"
    );
    fx.drop_tenant_db(slug).await;
}

#[tokio::test]
async fn the_jobs_own_database_found_by_name_after_a_crash_is_created_and_cleanup_deletes_it() {
    let Some(mut fx) = Fx::new("orgt_prov_job_own_crash").await else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };
    for (org, then_fail) in [("org_a1_own_crash_ok", false), ("org_a1_own_crash_fail", true)] {
        let (account_id, _user) = fx.org_account(org).await;
        let slug = tenant_slug(account_id);
        let slug = slug.as_str();
        fx.drop_tenant_db(slug).await;
        fx.enqueue(org, account_id, slug).await.unwrap();
        let mut deps = fx.deps("runner-o", Some(Fault::CrashAfterEffect), Some(Step::CreateDatabase));
        deps.directory = Arc::new(StaticOrgDirectory::selfserve([(org.to_string(), slug.to_string())]));
        let out = provisioning::run_job(&deps, org).await.unwrap().unwrap();
        assert!(matches!(out, RunOutcome::Deferred { step: Step::CreateDatabase, .. }), "{out:?}");
        // The process died after CREATE DATABASE but before the id landed:
        // only the intent to create is on the row.
        sqlx::query(
            "UPDATE tenant_provisioning_jobs SET provider = NULL, provider_database_id = NULL, \
                    credential_stored_at = NULL WHERE clerk_org_id = $1",
        )
        .bind(org)
        .execute(&fx.owner)
        .await
        .unwrap();
        assert_eq!(fx.db_count(slug).await, 1);

        deps.fault = if then_fail {
            Some(Arc::new(|s: Step| (s == Step::Migrate).then_some(Fault::Permanent)))
        } else {
            None
        };
        let out = run_to_terminal(&deps, org).await;
        let job = provisioning::get(&fx.app, org).await.unwrap().unwrap();
        if then_fail {
            assert!(matches!(&out, RunOutcome::Failed { .. }), "{out:?}");
            assert_eq!(fx.db_count(slug).await, 0, "cleanup deletes the database this job created");
        } else {
            assert_eq!(out, RunOutcome::Ready);
            assert!(job.provider_database_id.is_some() && job.database_create_requested_at.is_some());
            fx.assert_ready_artefacts(org, slug, account_id).await;
        }
        fx.drop_tenant_db(slug).await;
    }
}

/// Cleanup that deleted the job's own database clears the record of it, so a
/// retried job (A2's `POST …/provisioning/retry` resets the row to
/// `verify_org`) cannot treat a same-name database someone else created in
/// the meantime as the one it once asked for.
#[tokio::test]
async fn a_retry_after_cleanup_does_not_inherit_the_deleted_databases_create_intent() {
    let Some(mut fx) = Fx::new("orgt_prov_job_retry_intent").await else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };
    let org = "org_a1_retry_intent";
    let (account_id, _user) = fx.org_account(org).await;
    let slug = tenant_slug(account_id);
    let slug = slug.as_str();
    fx.drop_tenant_db(slug).await;
    fx.enqueue(org, account_id, slug).await.unwrap();
    let mut deps = fx.deps("runner-r", Some(Fault::Permanent), Some(Step::Fingerprint));
    deps.directory = Arc::new(StaticOrgDirectory::selfserve([(org.to_string(), slug.to_string())]));
    let out = run_to_terminal(&deps, org).await;
    assert!(matches!(&out, RunOutcome::Failed { .. }), "{out:?}");
    assert_eq!(fx.db_count(slug).await, 0, "its own database was deleted");
    let job = provisioning::get(&fx.app, org).await.unwrap().unwrap();
    assert_eq!(
        (job.provider_database_id.as_deref(), job.database_create_requested_at),
        (None, None),
        "the deleted database is no longer on the row"
    );

    // Someone else creates a database by that name; then the org retries.
    plant_foreign_database(&mut fx, slug, None).await;
    // A2's retry, on the app role, through its one narrow definer.
    assert!(provisioning::retry(&fx.app, org).await.unwrap());
    deps.fault = None;
    let out = run_to_terminal(&deps, org).await;
    assert!(
        matches!(&out, RunOutcome::Failed { reason } if reason.starts_with("step create_database:") && reason.contains("did not create")),
        "{out:?}"
    );
    let job = provisioning::get(&fx.app, org).await.unwrap().unwrap();
    assert_eq!(job.provider_database_id, None, "the other database is never recorded");
    assert_foreign_database_untouched(&mut fx, slug, None, None).await;
    fx.drop_tenant_db(slug).await;
}

#[tokio::test]
async fn local_create_never_claims_a_database_that_already_exists() {
    let Some(mut fx) = Fx::new("orgt_prov_job_local_exists").await else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };
    let slug = "a1-local-exists";
    plant_foreign_database(&mut fx, slug, None).await;
    let p = LocalPgProvisioner::new(fx.base.clone());
    let r = p.create(&p.database_name(slug), None).await;
    assert!(matches!(r, Err(ProvisionerError::AlreadyExists(_))), "{r:?}");
    assert_foreign_database_untouched(&mut fx, slug, None, None).await;
    fx.drop_tenant_db(slug).await;
}

#[tokio::test]
async fn lease_keeps_two_runners_apart() {
    let Some(mut fx) = Fx::new("orgt_prov_job_lease").await else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };
    let org = "org_a1_lease";
    let (account_id, _user) = fx.org_account(org).await;
    let slug = tenant_slug(account_id);
    let slug = slug.as_str();
    fx.drop_tenant_db(slug).await;
    fx.enqueue(org, account_id, slug).await.unwrap();

    // Another machine holds the lease: this one gets nothing, by either path.
    sqlx::query("UPDATE tenant_provisioning_jobs SET lease_owner = 'other-machine', lease_expires_at = now() + interval '10 minutes' WHERE clerk_org_id = $1")
        .bind(org)
        .execute(&fx.owner)
        .await
        .unwrap();
    let deps = fx.deps("runner-l", None, None);
    assert!(provisioning::run_job(&deps, org).await.unwrap().is_none());
    assert!(provisioning::run_next_due(&deps).await.unwrap().is_none());
    assert_eq!(fx.db_count(slug).await, 0, "nothing ran");

    // The lease expired (that machine died): the job is claimable again.
    sqlx::query("UPDATE tenant_provisioning_jobs SET lease_expires_at = now() - interval '1 second' WHERE clerk_org_id = $1")
        .bind(org)
        .execute(&fx.owner)
        .await
        .unwrap();
    // …but not before it is due.
    sqlx::query("UPDATE tenant_provisioning_jobs SET next_attempt_at = now() + interval '1 hour' WHERE clerk_org_id = $1")
        .bind(org)
        .execute(&fx.owner)
        .await
        .unwrap();
    assert!(provisioning::run_next_due(&deps).await.unwrap().is_none());
    sqlx::query("UPDATE tenant_provisioning_jobs SET next_attempt_at = now() WHERE clerk_org_id = $1")
        .bind(org)
        .execute(&fx.owner)
        .await
        .unwrap();
    let (claimed_org, out) = provisioning::run_next_due(&deps).await.unwrap().unwrap();
    assert_eq!(claimed_org, org);
    assert_eq!(out, RunOutcome::Ready);
    fx.assert_ready_artefacts(org, slug, account_id).await;
    fx.drop_tenant_db(slug).await;
}

#[tokio::test]
async fn personal_orgs_and_unmarked_orgs_are_refused_and_the_flag_gates_enqueue() {
    let Some(mut fx) = Fx::new("orgt_prov_job_personal").await else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };
    // Flag off: nothing can be queued.
    {
        let _g = FLAG_LOCK.lock().await;
        std::env::remove_var(SELFSERVE_SIGNUP_FLAG);
        let (account_id, _u) = fx.org_account("org_a1_flag_off").await;
        let r = provisioning::enqueue(
            &fx.app,
            EnqueueRequest::selfserve("org_a1_flag_off", account_id),
        )
        .await;
        assert!(matches!(r, Err(EnqueueError::Disabled)), "{r:?}");
        std::env::set_var(SELFSERVE_SIGNUP_FLAG, "1");
    }

    // Personal org, known at enqueue time: refused before anything exists.
    let org = "org_a1_personal_early";
    let (account_id, _u) = fx.org_account(org).await;
    let slug = tenant_slug(account_id);
    let slug = slug.as_str();
    sqlx::query("INSERT INTO personal_orgs (clerk_org_id, clerk_user_id) VALUES ($1, 'user_early')")
        .bind(org)
        .execute(&fx.owner)
        .await
        .unwrap();
    let r = fx.enqueue(org, account_id, slug).await;
    assert!(matches!(r, Err(EnqueueError::PersonalOrg(_))), "{r:?}");
    assert!(provisioning::get(&fx.app, org).await.unwrap().is_none());

    // Personal org recorded AFTER enqueue (the webhook race, audit 4i): the
    // job re-checks at execution and creates nothing.
    let org = "org_a1_personal_late";
    let (account_id, _u) = fx.org_account(org).await;
    let slug = tenant_slug(account_id);
    let slug = slug.as_str();
    fx.drop_tenant_db(slug).await;
    fx.enqueue(org, account_id, slug).await.unwrap();
    sqlx::query("INSERT INTO personal_orgs (clerk_org_id, clerk_user_id) VALUES ($1, 'user_late')")
        .bind(org)
        .execute(&fx.owner)
        .await
        .unwrap();
    let out = provisioning::run_job(&fx.deps("runner-p", None, None), org).await.unwrap().unwrap();
    assert!(matches!(&out, RunOutcome::Failed { reason } if reason.contains("personal")), "{out:?}");
    assert_eq!(fx.db_count(slug).await, 0);
    assert!(credentials::fetch_ciphertext(&fx.app, &provisioning::secret_ref_for_slug(slug)).await.unwrap().is_none());
    assert!(routing::lookup_by_org(&fx.app, org).await.unwrap().is_none());

    // An org Clerk knows but that lacks the self-serve marker: refused.
    let org = "org_a1_unmarked";
    let (account_id, _u) = fx.org_account(org).await;
    let slug = tenant_slug(account_id);
    let slug = slug.as_str();
    fx.enqueue(org, account_id, slug).await.unwrap();
    let mut deps = fx.deps("runner-p", None, None);
    deps.directory = Arc::new(StaticOrgDirectory::new(
        [(org.to_string(), provisioning::OrgRecord { slug: Some(slug.into()), private_metadata: serde_json::json!({}) })]
            .into_iter()
            .collect(),
    ));
    let out = provisioning::run_job(&deps, org).await.unwrap().unwrap();
    assert!(matches!(&out, RunOutcome::Failed { reason } if reason.contains("marker")), "{out:?}");
    assert_eq!(fx.db_count(slug).await, 0);

    // Wrong account binding: refused at enqueue (the 0321 precondition).
    let nobody = Uuid::new_v4();
    let r = fx.enqueue("org_a1_nobody", nobody, &tenant_slug(nobody)).await;
    assert!(matches!(r, Err(EnqueueError::NoOrgAccount(_))), "{r:?}");
    let r = fx.enqueue("org_a1_badslug", account_id, "Bad_Slug").await;
    assert!(matches!(r, Err(EnqueueError::InvalidSlug(_))), "{r:?}");
    // A well-formed slug that is not THIS account's routing key: refused,
    // so no caller can choose (or collide with) another tenant's key.
    let (other_account, _u) = fx.org_account("org_a1_chosen_slug").await;
    for chosen in ["acme-corp", tenant_slug(account_id).as_str()] {
        let r = fx.enqueue("org_a1_chosen_slug", other_account, chosen).await;
        assert!(matches!(r, Err(EnqueueError::InvalidSlug(_))), "{chosen}: {r:?}");
    }
    // The app role cannot write a job row itself (S1): only the definers can.
    let e = sqlx::query(
        "INSERT INTO tenant_provisioning_jobs (clerk_org_id, account_id, slug, db_secret_ref) VALUES ($1, $2, $3, $4)",
    )
    .bind("org_a1_chosen_slug")
    .bind(other_account)
    .bind(tenant_slug(other_account))
    .bind(provisioning::secret_ref_for_slug(&tenant_slug(other_account)))
    .execute(&fx.app)
    .await
    .expect_err("app role has no INSERT on the job table");
    assert_eq!(e.as_database_error().and_then(|d| d.code()).as_deref(), Some("42501"));
    let e = sqlx::query("UPDATE tenant_provisioning_jobs SET step = 'activate' WHERE clerk_org_id = $1")
        .bind("org_a1_unmarked")
        .execute(&fx.app)
        .await
        .expect_err("app role has no UPDATE on the job table");
    assert_eq!(e.as_database_error().and_then(|d| d.code()).as_deref(), Some("42501"));
}


/// One live run against Neon. Opt-in (`NEON_LIVE_CHECK=1` + `NEON_API_KEY`),
/// never in CI: creates a project named `selfserve-test-<slug>`, runs the
/// full job against it through the real REST provisioner, proves the tenant
/// is `ready` and reachable, then deletes the project and proves it is gone.
/// Prints the project id (never the DSN or key).
#[tokio::test]
async fn neon_live_check_creates_provisions_and_deletes_a_test_project() {
    if std::env::var("NEON_LIVE_CHECK").ok().as_deref() != Some("1") {
        eprintln!("skipping neon live check: set NEON_LIVE_CHECK=1 and NEON_API_KEY");
        return;
    }
    let api_key = std::env::var("NEON_API_KEY").expect("NEON_API_KEY");
    let Some(fx) = Fx::new("orgt_prov_job_neon_live").await else {
        panic!("the live check needs ORGT_TEST_BASE_URL for its control database");
    };
    let org = format!(
        "org_neon_live_{}",
        std::env::var("NEON_LIVE_SLUG").unwrap_or_else(|_| "a1_live".into()).replace('-', "_")
    );
    let neon: Arc<dyn TenantDbProvisioner> =
        Arc::new(NeonProvisioner::new(api_key).with_name_prefix("selfserve-test-"));
    let (account_id, _user) = fx.org_account(&org).await;
    let slug = tenant_slug(account_id);
    let project_name = neon.database_name(&slug);
    assert!(project_name.starts_with("selfserve-test-o-"), "{project_name}");

    fx.enqueue(&org, account_id, &slug).await.unwrap();
    let mut deps = fx.deps("runner-neon", None, None);
    deps.provisioner = neon.clone();
    deps.directory = Arc::new(StaticOrgDirectory::selfserve([(org.clone(), slug.clone())]));
    deps.policy = RetryPolicy {
        max_attempts: 12,
        base_backoff: Duration::from_secs(1),
        max_backoff: Duration::from_secs(5),
        lease_ttl: Duration::from_secs(300),
        ..RetryPolicy::default()
    };

    // A fresh Neon compute can refuse connections for a few seconds after
    // creation; the job's own retries cover that. Drive it to a terminal
    // outcome either way, then ALWAYS try to delete the project.
    let mut outcome = None;
    for _ in 0..40 {
        match provisioning::run_job(&deps, &org).await.unwrap() {
            Some(RunOutcome::Deferred { .. }) => tokio::time::sleep(Duration::from_secs(5)).await,
            Some(o) => {
                outcome = Some(o);
                break;
            }
            None => break,
        }
    }
    let job = provisioning::get(&fx.app, &org).await.unwrap().unwrap();
    let project_id = job.provider_database_id.clone();
    eprintln!("neon live check: project {:?} ({project_name}) job status={} step={} last_error={:?}",
        project_id, job.status, job.step, job.last_error);

    let verify: Result<(), String> = async {
        if outcome != Some(RunOutcome::Ready) {
            return Err(format!("job did not reach ready: {outcome:?}"));
        }
        let row = routing::lookup_by_org(&fx.app, &org).await.unwrap().ok_or("no routing row")?;
        if row.status != "active" || row.schema_version != Some(migrate::head_version()) {
            return Err(format!("routing row not active at head: {row:?}"));
        }
        // Reachable through the same registry the request path uses.
        let pool = fx.registry.get_or_build(&org, account_id, &row.db_secret_ref).await.map_err(|e| e.to_string())?;
        let fp: String = sqlx::query_scalar("SELECT org_id FROM tenant_meta").fetch_one(&pool).await.map_err(|e| e.to_string())?;
        if fp != org {
            return Err(format!("fingerprint {fp} != {org}"));
        }
        let keys: i64 = sqlx::query_scalar("SELECT count(*) FROM account_keys").fetch_one(&pool).await.map_err(|e| e.to_string())?;
        if keys != 1 {
            return Err(format!("{keys} DEK rows"));
        }
        // find-by-name finds the very project the job created.
        let found = neon.find_by_name(&project_name).await.map_err(|e| e.to_string())?;
        if found.as_ref().map(|f| f.provider_id.as_str()) != project_id.as_deref() {
            return Err(format!("find_by_name returned {:?}", found.map(|f| f.provider_id)));
        }
        Ok(())
    }
    .await;

    // Teardown: cancel is only meaningful for a live job; a ready job is
    // torn down directly — this is a test project, never a real tenant.
    fx.registry.evict_org(&org).await;
    let deleted = match project_id.as_deref() {
        Some(id) => neon.delete(id).await,
        None => Ok(coralswarm_backend::tenancy::provisioner::DeleteOutcome::AlreadyGone),
    };
    let gone = neon.find_by_name(&project_name).await;
    eprintln!("neon live check: delete {:?}; find_by_name after delete -> {:?}", deleted, gone.as_ref().map(|g| g.as_ref().map(|p| p.provider_id.clone())));
    verify.expect("neon live check");
    assert!(matches!(deleted, Ok(coralswarm_backend::tenancy::provisioner::DeleteOutcome::Deleted)), "{deleted:?}");
    assert!(matches!(gone, Ok(None)), "project must be gone after delete");
}


/// Card-up-front: a job enqueued for an org whose subscription is not live
/// (no completed Checkout, or `pending`) creates nothing and retries; once
/// the subscription is live the same job converges to ready.
#[tokio::test]
async fn job_waits_for_a_live_subscription_before_creating_anything() {
    let Some(mut fx) = Fx::new("orgt_prov_job_unpaid").await else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };
    let org = "org_a1_unpaid";
    let (account_id, _u) = fx.org_account_unpaid(org).await;
    let slug = tenant_slug(account_id);
    let slug = slug.as_str();
    fx.drop_tenant_db(slug).await;
    assert_eq!(fx.enqueue(org, account_id, slug).await.unwrap(), EnqueueOutcome::Enqueued);
    let deps = fx.deps("runner-a", None, None);

    // No stripe_subscriptions row at all.
    let out = provisioning::run_job(&deps, org).await.unwrap().unwrap();
    assert!(matches!(out, RunOutcome::Deferred { step: Step::VerifyOrg, .. }), "{out:?}");
    assert_eq!(fx.db_count(slug).await, 0, "no database before payment");

    // A bare customer mapping (`pending`, no subscription id) is not live.
    fx.set_subscription(account_id, "pending", None).await;
    let out = provisioning::run_job(&deps, org).await.unwrap().unwrap();
    assert!(matches!(out, RunOutcome::Deferred { step: Step::VerifyOrg, .. }), "{out:?}");
    assert_eq!(fx.db_count(slug).await, 0);

    // Checkout completed (Stripe `trialing` is stored as `active`).
    fx.set_subscription(account_id, "active", Some("sub_a1_unpaid")).await;
    let out = provisioning::run_job(&deps, org).await.unwrap().unwrap();
    assert_eq!(out, RunOutcome::Ready);
    fx.assert_ready_artefacts(org, slug, account_id).await;
    fx.drop_tenant_db(slug).await;
}

/// A canceled subscription is terminal at `verify_org`: no database.
#[tokio::test]
async fn canceled_subscription_fails_the_job_before_any_database() {
    let Some(mut fx) = Fx::new("orgt_prov_job_canceled").await else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };
    let org = "org_a1_canceled";
    let (account_id, _u) = fx.org_account(org).await;
    let slug = tenant_slug(account_id);
    let slug = slug.as_str();
    fx.drop_tenant_db(slug).await;
    assert_eq!(fx.enqueue(org, account_id, slug).await.unwrap(), EnqueueOutcome::Enqueued);
    fx.set_subscription(account_id, "canceled", Some("sub_org_a1_canceled")).await;
    let out = provisioning::run_job(&fx.deps("runner-a", None, None), org).await.unwrap().unwrap();
    assert!(matches!(&out, RunOutcome::Failed { reason } if reason.contains("canceled")), "{out:?}");
    assert_eq!(fx.db_count(slug).await, 0);
    let job = provisioning::get(&fx.app, org).await.unwrap().unwrap();
    assert_eq!(job.status, "failed");
    assert!(job.provider_database_id.is_none() && job.database_create_requested_at.is_none());
}

/// The re-check runs again immediately before the provider create: a job
/// already past `verify_org` whose subscription is canceled meanwhile never
/// creates a database.
#[tokio::test]
async fn create_database_rechecks_the_subscription_before_creating() {
    let Some(mut fx) = Fx::new("orgt_prov_job_canceled_late").await else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };
    let org = "org_a1_canceled_before_create";
    let (account_id, _u) = fx.org_account(org).await;
    let slug = tenant_slug(account_id);
    let slug = slug.as_str();
    fx.drop_tenant_db(slug).await;
    assert_eq!(fx.enqueue(org, account_id, slug).await.unwrap(), EnqueueOutcome::Enqueued);
    // As if `verify_org` passed on an earlier run.
    sqlx::query("UPDATE tenant_provisioning_jobs SET step = 'create_database' WHERE clerk_org_id = $1")
        .bind(org)
        .execute(&fx.owner)
        .await
        .unwrap();
    fx.set_subscription(account_id, "canceled", Some("sub_org_a1_canceled_before_create")).await;
    let out = provisioning::run_job(&fx.deps("runner-a", None, None), org).await.unwrap().unwrap();
    assert!(matches!(&out, RunOutcome::Failed { reason } if reason.contains("canceled")), "{out:?}");
    assert_eq!(fx.db_count(slug).await, 0, "no database after the subscription was canceled");
    let job = provisioning::get(&fx.app, org).await.unwrap().unwrap();
    assert!(job.database_create_requested_at.is_none(), "no create intent recorded");
}

/// The webhook entry point: `EnqueueRequest::selfserve` derives an opaque,
/// deterministic slug, and `enqueue_in` runs inside an account-scoped
/// transaction as `coralswarm_app` (the shape the Stripe handler has), rolls
/// back with it, and is a no-op on redelivery.
#[tokio::test]
async fn webhook_enqueue_in_scoped_tx_is_opaque_transactional_and_idempotent() {
    let Some(fx) = Fx::new("orgt_prov_job_webhook").await else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };
    let org = "org_a1_webhook";
    let (account_id, _u) = fx.org_account(org).await;
    let req = EnqueueRequest::selfserve(org, account_id);
    assert_eq!(req.slug, provisioning::opaque_slug_for_account(account_id));
    assert!(req.slug.starts_with("o-") && req.slug.len() == 18, "{}", req.slug);
    assert!(coralswarm_backend::tenancy::gate::is_valid_slug(&req.slug));
    assert_eq!(EnqueueRequest::selfserve(org, account_id).slug, req.slug, "deterministic");

    let _g = FLAG_LOCK.lock().await;
    std::env::set_var(SELFSERVE_SIGNUP_FLAG, "1");

    // Rolled back with the webhook's transaction: no job.
    let mut tx = coralswarm_backend::tenancy::scope::begin_account_scoped(&fx.app, account_id).await.unwrap();
    let out = provisioning::enqueue_in(&mut tx, req.clone()).await.unwrap();
    assert_eq!(out, EnqueueOutcome::Enqueued);
    tx.rollback().await.unwrap();
    assert!(provisioning::get(&fx.app, org).await.unwrap().is_none(), "rolled back with the event");

    // Committed: one job; a redelivery is a no-op.
    for expect_new in [true, false] {
        let mut tx = coralswarm_backend::tenancy::scope::begin_account_scoped(&fx.app, account_id).await.unwrap();
        let out = provisioning::enqueue_in(&mut tx, req.clone()).await.unwrap();
        tx.commit().await.unwrap();
        if expect_new {
            assert_eq!(out, EnqueueOutcome::Enqueued);
        } else {
            assert!(matches!(out, EnqueueOutcome::AlreadyExists { .. }), "{out:?}");
        }
    }
    let job = provisioning::get(&fx.app, org).await.unwrap().unwrap();
    assert_eq!(job.slug, req.slug);
    assert_eq!(job.db_secret_ref, provisioning::secret_ref_for_slug(&req.slug));
    assert_eq!(
        provisioning::subscription_state(&fx.app, account_id).await.unwrap(),
        provisioning::SubscriptionState::Live,
        "the RLS'd subscription read works under coralswarm_app"
    );
}


// ── Security review fixes (S4-S9, N4) ────────────────────────────────────────

/// S4: cleanup deletes a recorded database only when the provider names it
/// as THIS job's (`database_name(slug)`, a self-serve name). A row whose
/// `provider_database_id` points at somebody else's database is refused, and
/// that database is never connected to for deletion, let alone dropped.
#[tokio::test]
async fn cleanup_refuses_a_recorded_database_that_is_not_named_for_the_job() {
    let Some(mut fx) = Fx::new("orgt_prov_job_misnamed").await else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };
    let org = "org_a1_misnamed";
    let (account_id, _u) = fx.org_account(org).await;
    let slug = tenant_slug(account_id);
    fx.drop_tenant_db(&slug).await;
    let victim = plant_foreign_database(&mut fx, "victim-misnamed", None).await;
    fx.enqueue(org, account_id, &slug).await.unwrap();
    let deps = fx.deps("runner-m", Some(Fault::CrashAfterEffect), Some(Step::Migrate));
    let out = provisioning::run_job(&deps, org).await.unwrap().unwrap();
    assert!(matches!(out, RunOutcome::Deferred { step: Step::Migrate, .. }), "{out:?}");

    // A tampered row: the recorded id now names the victim.
    sqlx::query("UPDATE tenant_provisioning_jobs SET provider_database_id = $2 WHERE clerk_org_id = $1")
        .bind(org)
        .bind(&victim)
        .execute(&fx.owner)
        .await
        .unwrap();
    assert!(provisioning::cancel(&fx.app, org).await.unwrap());
    let out = provisioning::run_job(&fx.deps("runner-m", None, None), org).await.unwrap().unwrap();
    assert!(
        matches!(&out, RunOutcome::Failed { reason } if reason.contains("cleanup refused") && reason.contains("not this job's")),
        "{out:?}"
    );
    assert_foreign_database_untouched(&mut fx, "victim-misnamed", None, None).await;
    fx.drop_tenant_db("victim-misnamed").await;
    fx.drop_tenant_db(&slug).await;
}

/// S5: the create response was lost (or the process died before recording
/// the id): only the create intent is on the row. Cleanup finds the job's
/// database by its name and deletes it rather than leaking a billable project.
#[tokio::test]
async fn cleanup_finds_an_unrecorded_database_by_name_and_deletes_it() {
    let Some(mut fx) = Fx::new("orgt_prov_job_orphan").await else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };
    let org = "org_a1_orphan";
    let (account_id, _u) = fx.org_account(org).await;
    let slug = tenant_slug(account_id);
    fx.drop_tenant_db(&slug).await;
    fx.enqueue(org, account_id, &slug).await.unwrap();
    let deps = fx.deps("runner-o", Some(Fault::CrashAfterEffect), Some(Step::CreateDatabase));
    let out = provisioning::run_job(&deps, org).await.unwrap().unwrap();
    assert!(matches!(out, RunOutcome::Deferred { step: Step::CreateDatabase, .. }), "{out:?}");
    sqlx::query("UPDATE tenant_provisioning_jobs SET provider = NULL, provider_database_id = NULL WHERE clerk_org_id = $1")
        .bind(org)
        .execute(&fx.owner)
        .await
        .unwrap();
    assert_eq!(fx.db_count(&slug).await, 1);
    let job = provisioning::get(&fx.app, org).await.unwrap().unwrap();
    assert!(job.database_create_requested_at.is_some() && job.provider_database_id.is_none());

    assert!(provisioning::cancel(&fx.app, org).await.unwrap());
    let out = provisioning::run_job(&fx.deps("runner-o", None, None), org).await.unwrap().unwrap();
    assert_eq!(out, RunOutcome::Failed { reason: "cancelled".into() });
    assert_eq!(fx.db_count(&slug).await, 0, "the orphan was found by name and deleted");
    let job = provisioning::get(&fx.app, org).await.unwrap().unwrap();
    assert_eq!((job.provider_database_id, job.database_create_requested_at), (None, None));
}

/// S6: a crash between storing the credential and recording
/// `credential_stored_at` leaves a credential the row does not mention.
/// Cleanup deletes the job's credential regardless.
#[tokio::test]
async fn cleanup_deletes_the_credential_even_when_it_was_never_recorded() {
    let Some(mut fx) = Fx::new("orgt_prov_job_cred_unrecorded").await else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };
    let org = "org_a1_cred_unrecorded";
    let (account_id, _u) = fx.org_account(org).await;
    let slug = tenant_slug(account_id);
    let secret_ref = provisioning::secret_ref_for_slug(&slug);
    fx.drop_tenant_db(&slug).await;
    fx.enqueue(org, account_id, &slug).await.unwrap();
    let deps = fx.deps("runner-c", Some(Fault::CrashAfterEffect), Some(Step::CreateDatabase));
    provisioning::run_job(&deps, org).await.unwrap().unwrap();
    sqlx::query("UPDATE tenant_provisioning_jobs SET credential_stored_at = NULL WHERE clerk_org_id = $1")
        .bind(org)
        .execute(&fx.owner)
        .await
        .unwrap();
    assert!(credentials::fetch_ciphertext(&fx.app, &secret_ref).await.unwrap().is_some());

    assert!(provisioning::cancel(&fx.app, org).await.unwrap());
    let out = provisioning::run_job(&fx.deps("runner-c", None, None), org).await.unwrap().unwrap();
    assert_eq!(out, RunOutcome::Failed { reason: "cancelled".into() });
    assert!(
        credentials::fetch_ciphertext(&fx.app, &secret_ref).await.unwrap().is_none(),
        "the unrecorded credential is deleted, so the account is not poisoned for a retry"
    );
    assert_eq!(fx.db_count(&slug).await, 0);
}

/// S8: every job write that keeps the lease renews it, so a job that runs
/// longer than one TTL overall is not re-claimable mid-run. And only the
/// holder can write.
#[tokio::test]
async fn every_held_write_renews_the_lease() {
    let Some(fx) = Fx::new("orgt_prov_job_lease_renew").await else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };
    let org = "org_a1_lease_renew";
    let (account_id, _u) = fx.org_account(org).await;
    fx.enqueue(org, account_id, &tenant_slug(account_id)).await.unwrap();
    let claimed: Option<String> =
        sqlx::query_scalar("SELECT clerk_org_id FROM tenant_provisioning_claim('runner-r', interval '2 seconds', $1)")
            .bind(org)
            .fetch_optional(&fx.prov)
            .await
            .unwrap();
    assert_eq!(claimed.as_deref(), Some(org));

    let nearly_expired = || async {
        sqlx::query("UPDATE tenant_provisioning_jobs SET lease_expires_at = now() + interval '1 second' WHERE clerk_org_id = $1")
            .bind(org)
            .execute(&fx.owner)
            .await
            .unwrap();
    };
    let renewed = || async {
        sqlx::query_scalar::<_, bool>(
            "SELECT lease_expires_at > now() + interval '9 minutes' FROM tenant_provisioning_jobs WHERE clerk_org_id = $1",
        )
        .bind(org)
        .fetch_one(&fx.app)
        .await
        .unwrap()
    };

    nearly_expired().await;
    let moved: bool = sqlx::query_scalar(
        "SELECT tenant_provisioning_set_step($1, 'runner-r', interval '10 minutes', 'create_database', NULL)",
    )
    .bind(org)
    .fetch_one(&fx.prov)
    .await
    .unwrap();
    assert!(moved);
    assert!(renewed().await, "a step change renews the lease");

    nearly_expired().await;
    let held: bool = sqlx::query_scalar("SELECT tenant_provisioning_record_create_intent($1, 'runner-r', interval '10 minutes')")
        .bind(org)
        .fetch_one(&fx.prov)
        .await
        .unwrap();
    assert!(held);
    assert!(renewed().await, "a progress write inside a step renews the lease");

    // Not the holder: no write, and no transition off the forward path.
    let intruder: bool = sqlx::query_scalar(
        "SELECT tenant_provisioning_set_step($1, 'intruder', interval '10 minutes', 'insert_routing', NULL)",
    )
    .bind(org)
    .fetch_one(&fx.prov)
    .await
    .unwrap();
    assert!(!intruder);
    let e = sqlx::query_scalar::<_, bool>(
        "SELECT tenant_provisioning_set_step($1, 'runner-r', interval '10 minutes', 'activate', NULL)",
    )
    .bind(org)
    .fetch_one(&fx.prov)
    .await
    .expect_err("create_database -> activate skips the forward path");
    assert_eq!(e.as_database_error().and_then(|d| d.code()).as_deref(), Some("23514"));
}

/// S9: an org whose subscription is not live yet (an ACH / SEPA payment
/// still settling) is waited for on its own clock, well past the
/// transient-error attempts budget; only the subscription deadline ends it.
#[tokio::test]
async fn a_subscription_that_is_not_live_waits_past_the_attempts_budget() {
    let Some(mut fx) = Fx::new("orgt_prov_job_ach").await else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };
    let org = "org_a1_ach";
    let (account_id, _u) = fx.org_account_unpaid(org).await;
    let slug = tenant_slug(account_id);
    fx.enqueue(org, account_id, &slug).await.unwrap();
    fx.set_subscription(account_id, "pending", None).await;
    let deps = fx.deps("runner-ach", None, None);
    assert_eq!(deps.policy.max_attempts, 3);

    for _ in 0..(deps.policy.max_attempts * 2) {
        let out = provisioning::run_job(&deps, org).await.unwrap().unwrap();
        assert!(matches!(out, RunOutcome::Deferred { step: Step::VerifyOrg, attempts: 0, .. }), "{out:?}");
    }
    let job = provisioning::get(&fx.app, org).await.unwrap().unwrap();
    assert_eq!((job.status.as_str(), job.attempts), ("provisioning", 0));
    assert!(job.subscription_wait_started_at.is_some());
    assert_eq!(fx.db_count(&slug).await, 0);

    // Six days into an incomplete ACH debit: still waiting.
    sqlx::query("UPDATE tenant_provisioning_jobs SET subscription_wait_started_at = now() - interval '6 days' WHERE clerk_org_id = $1")
        .bind(org)
        .execute(&fx.owner)
        .await
        .unwrap();
    let out = provisioning::run_job(&deps, org).await.unwrap().unwrap();
    assert!(matches!(out, RunOutcome::Deferred { .. }), "{out:?}");

    // Past the deadline: abandoned, with a reason that says why.
    sqlx::query("UPDATE tenant_provisioning_jobs SET subscription_wait_started_at = now() - interval '9 days' WHERE clerk_org_id = $1")
        .bind(org)
        .execute(&fx.owner)
        .await
        .unwrap();
    let out = provisioning::run_job(&deps, org).await.unwrap().unwrap();
    assert!(matches!(&out, RunOutcome::Failed { reason } if reason.contains("did not become live")), "{out:?}");
    assert_eq!(fx.db_count(&slug).await, 0);
}

/// N4: a uniqueness conflict inside `enqueue_in` is a typed error, and the
/// caller's (the webhook's) transaction is still usable and committable.
#[tokio::test]
async fn enqueue_in_conflict_is_typed_and_leaves_the_callers_transaction_usable() {
    let Some(fx) = Fx::new("orgt_prov_job_enqueue_conflict").await else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };
    let org = "org_a1_enqueue_conflict";
    let (account_id, _u) = fx.org_account(org).await;
    // Another org already routed under this account's derived routing key:
    // the Rust pre-checks (by org id) cannot see it; the definer can.
    let (other_account, _u) = fx.org_account("org_a1_key_holder").await;
    financial_fixture::execute(
        &fx.owner,
        other_account,
        sqlx::query(
            "INSERT INTO tenant_databases (clerk_org_id, account_id, slug, db_secret_ref, status) \
             VALUES ('org_a1_key_holder', $1, $2, $3, 'active')",
        )
        .bind(other_account)
        .bind(tenant_slug(account_id))
        .bind(provisioning::secret_ref_for_slug(&tenant_slug(account_id))),
    )
    .await
    .unwrap();

    let _g = FLAG_LOCK.lock().await;
    std::env::set_var(SELFSERVE_SIGNUP_FLAG, "1");
    let mut tx = coralswarm_backend::tenancy::scope::begin_account_scoped(&fx.app, account_id).await.unwrap();
    let r = provisioning::enqueue_in(&mut tx, EnqueueRequest::selfserve(org, account_id)).await;
    assert!(matches!(&r, Err(EnqueueError::Conflict { org: o, .. }) if o == org), "{r:?}");
    let still_usable: i32 = sqlx::query_scalar("SELECT 1").fetch_one(&mut *tx).await.expect("transaction not aborted");
    assert_eq!(still_usable, 1);
    tx.commit().await.expect("the webhook's event still commits");
    assert!(provisioning::get(&fx.app, org).await.unwrap().is_none());
}

fn code(e: &sqlx::Error) -> Option<String> {
    e.as_database_error().and_then(|d| d.code()).map(|c| c.to_string())
}

/// S7-a: `activate` refusing because a newer tenant tree is already rolled
/// out (55000) is a park, not a transient error. It never spends the
/// attempts budget, so it is never abandoned, so cleanup never deletes the
/// customer's database; past its own deadline it alerts and stays parked.
/// Once the newer tree is no longer ahead of this build, the job activates.
#[tokio::test]
async fn a_version_refusal_parks_the_job_and_never_deletes_its_database() {
    let Some(mut fx) = Fx::new("orgt_prov_job_version_park").await else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };
    let org = "org_a1_version_park";
    let (account_id, user_id) = fx.org_account(org).await;
    let slug = tenant_slug(account_id);
    fx.drop_tenant_db(&slug).await;
    fx.enqueue(org, account_id, &slug).await.unwrap();

    // A newer release has already migrated another (concierge) tenant past
    // this build's head.
    let head = migrate::head_version();
    let newer = "org_a1_version_newer";
    let (newer_account, _u) = fx.org_account(newer).await;
    financial_fixture::execute(
        &fx.owner,
        newer_account,
        sqlx::query(
            "INSERT INTO tenant_databases(clerk_org_id,account_id,slug,db_secret_ref,status,schema_version) \
             VALUES($1,$2,'version-newer','TENANT_DB_VERSION_NEWER','active',$3)",
        )
        .bind(newer)
        .bind(newer_account)
        .bind(head + 1),
    )
    .await
    .unwrap();

    let deps = fx.deps("runner-v", None, None);
    for _ in 0..(deps.policy.max_attempts * 2) {
        let out = provisioning::run_job(&deps, org).await.unwrap().unwrap();
        assert!(
            matches!(&out, RunOutcome::Deferred { step: Step::Activate, attempts: 0, reason } if reason.contains("newer tenant tree")),
            "{out:?}"
        );
    }
    let job = provisioning::get(&fx.app, org).await.unwrap().unwrap();
    assert_eq!((job.status.as_str(), job.step.as_str(), job.attempts), ("provisioning", "activate", 0));
    assert!(job.version_wait_started_at.is_some(), "the park runs on its own clock");
    assert_eq!(fx.db_count(&slug).await, 1, "the database is kept");
    assert_eq!(routing::lookup_by_org(&fx.app, org).await.unwrap().unwrap().status, "provisioning");

    // A day later and still no newer build: it alerts, and stays parked.
    sqlx::query("UPDATE tenant_provisioning_jobs SET version_wait_started_at = now() - interval '25 hours' WHERE clerk_org_id = $1")
        .bind(org)
        .execute(&fx.owner)
        .await
        .unwrap();
    let out = provisioning::run_job(&deps, org).await.unwrap().unwrap();
    assert!(matches!(out, RunOutcome::Deferred { step: Step::Activate, attempts: 0, .. }), "{out:?}");
    let job = provisioning::get(&fx.app, org).await.unwrap().unwrap();
    assert_eq!((job.status.as_str(), job.attempts), ("provisioning", 0));
    assert!(job.failed_at.is_none() && job.clerk_org_cleanup == "none");
    assert_eq!(fx.db_count(&slug).await, 1, "still nothing deleted");

    // The other tenant is no longer ahead (the operator's catch-up, or the
    // newer build's worker re-migrating this one): the job activates.
    sqlx::query("UPDATE tenant_databases SET schema_version = $2 WHERE clerk_org_id = $1")
        .bind(newer)
        .bind(head)
        .execute(&fx.owner)
        .await
        .unwrap();
    let out = provisioning::run_job(&deps, org).await.unwrap().unwrap();
    assert_eq!(out, RunOutcome::Ready);
    let job = provisioning::get(&fx.app, org).await.unwrap().unwrap();
    assert!(job.version_wait_started_at.is_none());
    fx.assert_ready_artefacts(org, &slug, account_id).await;
    let _ = user_id;
    fx.drop_tenant_db(&slug).await;
}

/// Nit 3: a run that panics is caught by the worker AND charged to its job
/// as an attempt, so a job that panics on every run ends `failed` (cleaned
/// up, alerted once) instead of alerting forever. The panic text never
/// reaches the row.
#[tokio::test]
async fn a_job_that_panics_on_every_run_is_charged_and_eventually_fails() {
    let Some(mut fx) = Fx::new("orgt_prov_job_panic").await else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };
    let org = "org_a1_panic";
    let (account_id, _u) = fx.org_account(org).await;
    let slug = tenant_slug(account_id);
    fx.drop_tenant_db(&slug).await;
    fx.enqueue(org, account_id, &slug).await.unwrap();
    let deps = fx.deps("runner-panic", Some(Fault::Panic), Some(Step::VerifyOrg));

    for expected in 1..deps.policy.max_attempts {
        tokio::time::sleep(Duration::from_millis(20)).await;
        let ticked = coralswarm_backend::workers::tenant_provisioning::tick(&deps).await.unwrap();
        assert!(
            matches!(&ticked, Some((o, RunOutcome::Deferred { step: Step::VerifyOrg, attempts, .. })) if o == org && *attempts == expected),
            "attempt {expected}: {ticked:?}"
        );
        let job = provisioning::get(&fx.app, org).await.unwrap().unwrap();
        assert_eq!((job.attempts, job.lease_owner.as_deref()), (expected, None), "charged and released");
    }
    tokio::time::sleep(Duration::from_millis(20)).await;
    let ticked = coralswarm_backend::workers::tenant_provisioning::tick(&deps).await.unwrap();
    assert!(
        matches!(&ticked, Some((o, RunOutcome::Failed { reason })) if o == org && reason.contains("panicked")),
        "{ticked:?}"
    );
    let job = provisioning::get(&fx.app, org).await.unwrap().unwrap();
    assert_eq!((job.status.as_str(), job.clerk_org_cleanup.as_str()), ("failed", "pending"));
    let reason = job.last_error.unwrap_or_default();
    assert!(reason.contains("panicked") && !reason.contains("injected panic"), "{reason}");
    assert_eq!(fx.db_count(&slug).await, 0);
    // Nothing left to claim.
    assert!(coralswarm_backend::workers::tenant_provisioning::tick(&deps).await.unwrap().is_none());
}

/// A2 compatibility: the app role's one way to restart a failed job.
/// `failed → verify_org` with a clean slate, and nothing else.
#[tokio::test]
async fn retry_moves_only_a_failed_job_back_to_verify_org() {
    let Some(mut fx) = Fx::new("orgt_prov_job_retry_definer").await else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };
    let e = provisioning::retry(&fx.app, "org_a1_retry_nobody").await.expect_err("no job");
    assert_eq!(code(&e).as_deref(), Some("P0002"));

    let org = "org_a1_retry_definer";
    let (account_id, _u) = fx.org_account(org).await;
    let slug = tenant_slug(account_id);
    fx.drop_tenant_db(&slug).await;
    fx.enqueue(org, account_id, &slug).await.unwrap();
    let e = provisioning::retry(&fx.app, org).await.expect_err("live job");
    assert_eq!(code(&e).as_deref(), Some("23514"));

    let out = provisioning::run_job(&fx.deps("runner-rt", Some(Fault::Permanent), Some(Step::VerifyOrg)), org)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(out, RunOutcome::Failed { .. }), "{out:?}");
    // Every field the retry must clear, dirtied.
    sqlx::query(
        "UPDATE tenant_provisioning_jobs SET attempts = 5, cancel_requested_at = now(), \
                subscription_wait_started_at = now(), version_wait_started_at = now(), \
                lease_owner = 'stale', lease_expires_at = now() + interval '1 hour', \
                next_attempt_at = now() + interval '1 day' \
          WHERE clerk_org_id = $1",
    )
    .bind(org)
    .execute(&fx.owner)
    .await
    .unwrap();

    assert!(provisioning::retry(&fx.app, org).await.unwrap());
    let job = provisioning::get(&fx.app, org).await.unwrap().unwrap();
    assert_eq!((job.status.as_str(), job.step.as_str(), job.attempts), ("provisioning", "verify_org", 0));
    assert_eq!(
        (
            job.cancel_requested_at,
            job.subscription_wait_started_at,
            job.version_wait_started_at,
            job.lease_owner.as_deref(),
            job.lease_expires_at,
            job.failed_at,
            job.last_error.as_deref(),
            job.clerk_org_cleanup.as_str(),
        ),
        (None, None, None, None, None, None, None, "none")
    );
    assert!(job.next_attempt_at <= chrono::Utc::now());
    let e = provisioning::retry(&fx.app, org).await.expect_err("already live again");
    assert_eq!(code(&e).as_deref(), Some("23514"));

    // It resumes for real.
    let out = provisioning::run_job(&fx.deps("runner-rt", None, None), org).await.unwrap().unwrap();
    assert_eq!(out, RunOutcome::Ready);
    fx.assert_ready_artefacts(org, &slug, account_id).await;
    let e = provisioning::retry(&fx.app, org).await.expect_err("ready job");
    assert_eq!(code(&e).as_deref(), Some("23514"));
    fx.drop_tenant_db(&slug).await;

    // A2 already dealt with the orphaned Clerk org: not retryable.
    let gone = "org_a1_retry_clerk_gone";
    let (gone_account, _u) = fx.org_account(gone).await;
    fx.enqueue(gone, gone_account, &tenant_slug(gone_account)).await.unwrap();
    let out = provisioning::run_job(&fx.deps("runner-rt", Some(Fault::Permanent), Some(Step::VerifyOrg)), gone)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(out, RunOutcome::Failed { .. }), "{out:?}");
    let claim = provisioning::claim_clerk_org_cleanup(&fx.app, gone, std::time::Duration::from_secs(600))
        .await
        .unwrap()
        .expect("claimed");
    let e = provisioning::retry(&fx.app, gone).await.expect_err("Clerk org being cleaned up");
    assert_eq!(code(&e).as_deref(), Some("23514"), "control 0384: no retry while the sweep holds the claim");
    assert!(provisioning::mark_clerk_org_cleanup_done(&fx.app, gone, claim).await.unwrap());
    let e = provisioning::retry(&fx.app, gone).await.expect_err("Clerk org cleaned up");
    assert_eq!(code(&e).as_deref(), Some("23514"));

    // An org routed in another status meanwhile: not retryable.
    let routed = "org_a1_retry_routed";
    let (routed_account, _u) = fx.org_account(routed).await;
    fx.enqueue(routed, routed_account, &tenant_slug(routed_account)).await.unwrap();
    let out = provisioning::run_job(&fx.deps("runner-rt", Some(Fault::Permanent), Some(Step::VerifyOrg)), routed)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(out, RunOutcome::Failed { .. }), "{out:?}");
    financial_fixture::execute(
        &fx.owner,
        routed_account,
        sqlx::query(
            "INSERT INTO tenant_databases(clerk_org_id,account_id,slug,db_secret_ref,status) \
             VALUES($1,$2,'retry-routed','TENANT_DB_RETRY_ROUTED','active')",
        )
        .bind(routed)
        .bind(routed_account),
    )
    .await
    .unwrap();
    let e = provisioning::retry(&fx.app, routed).await.expect_err("routed elsewhere");
    assert_eq!(code(&e).as_deref(), Some("23505"));
    // Control 0384 (A2 security review N7, N8, S6): the sweep's requeue of a
    // routed org lands in `held` instead of failing and leaving the claim in
    // `deleting`; the requeue cap is the definer's; only the claim's current
    // holder can release.
    let stale = std::time::Duration::from_secs(600);
    let claim = provisioning::claim_clerk_org_cleanup(&fx.app, routed, stale).await.unwrap().expect("claimed");
    let wrong = claim + chrono::Duration::microseconds(1);
    assert_eq!(
        provisioning::release_clerk_org_cleanup(&fx.app, routed, provisioning::CleanupRelease::Requeue, wrong, 5).await.unwrap(),
        provisioning::ReleaseOutcome::Lost,
        "not the holder: nothing written"
    );
    assert_eq!(
        provisioning::release_clerk_org_cleanup(&fx.app, routed, provisioning::CleanupRelease::Requeue, claim, 5).await.unwrap(),
        provisioning::ReleaseOutcome::HeldRouted
    );
    let row: (String, String) = sqlx::query_as("SELECT status, clerk_org_cleanup FROM tenant_provisioning_jobs WHERE clerk_org_id = $1")
        .bind(routed)
        .fetch_one(&fx.owner)
        .await
        .unwrap();
    assert_eq!(row, ("failed".to_string(), "held".to_string()), "held, not stuck in deleting");
    // The cap: a requeue at the cap is held instead.
    let capped = "org_a1_requeue_capped";
    let (capped_account, _u) = fx.org_account(capped).await;
    fx.enqueue(capped, capped_account, &tenant_slug(capped_account)).await.unwrap();
    let out = provisioning::run_job(&fx.deps("runner-rt", Some(Fault::Permanent), Some(Step::VerifyOrg)), capped)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(out, RunOutcome::Failed { .. }), "{out:?}");
    let claim = provisioning::claim_clerk_org_cleanup(&fx.app, capped, stale).await.unwrap().expect("claimed");
    assert_eq!(
        provisioning::release_clerk_org_cleanup(&fx.app, capped, provisioning::CleanupRelease::Requeue, claim, 0).await.unwrap(),
        provisioning::ReleaseOutcome::HeldCap,
        "past the cap: held"
    );
    let claim = {
        sqlx::query("UPDATE tenant_provisioning_jobs SET clerk_org_cleanup = 'pending' WHERE clerk_org_id = $1")
            .bind(capped)
            .execute(&fx.owner)
            .await
            .unwrap();
        provisioning::claim_clerk_org_cleanup(&fx.app, capped, stale).await.unwrap().expect("claimed")
    };
    assert_eq!(
        provisioning::release_clerk_org_cleanup(&fx.app, capped, provisioning::CleanupRelease::Requeue, claim, 1).await.unwrap(),
        provisioning::ReleaseOutcome::Requeued,
        "under the cap: requeued"
    );
    // The provisioner role is not the web tier: it cannot retry.
    let e = provisioning::retry(&fx.prov, routed).await.expect_err("provisioner role");
    assert_eq!(code(&e).as_deref(), Some("42501"));
}

/// SF-1: a rollback. A NEWER build's worker migrated this job's database
/// past this build's head (its ledger and its routing row both say so), then
/// this older build's worker picks the job up. Whether the job is on
/// `activate` (the row is ahead) or `migrate` (the row is ahead, or only the
/// database's own ledger is: sqlx's `VersionMissing`), it is the version
/// park: no attempt is spent, so it is never abandoned, so cleanup never
/// deletes the customer's database. Once the database is no longer ahead
/// (the newer build is back), the job activates.
#[tokio::test]
async fn a_tenant_ahead_of_this_build_parks_and_is_never_deleted() {
    let Some(mut fx) = Fx::new("orgt_prov_job_ahead").await else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };
    let org = "org_a1_ahead";
    let (account_id, _user_id) = fx.org_account(org).await;
    let slug = tenant_slug(account_id);
    fx.drop_tenant_db(&slug).await;
    fx.enqueue(org, account_id, &slug).await.unwrap();

    // Run up to just past `migrate` (a crash after `fingerprint`'s effect).
    let deps = fx.deps("runner-ahead", Some(Fault::CrashAfterEffect), Some(Step::Fingerprint));
    let out = provisioning::run_job(&deps, org).await.unwrap().unwrap();
    assert!(matches!(out, RunOutcome::Deferred { step: Step::Fingerprint, .. }), "{out:?}");
    let head = migrate::head_version();
    assert_eq!(routing::lookup_by_org(&fx.app, org).await.unwrap().unwrap().schema_version, Some(head));

    // The newer build: one more tenant migration in the database's ledger,
    // and the routing row records it.
    let secret_ref = provisioning::secret_ref_for_slug(&slug);
    let tenant_dsn = fx.resolver.resolve_async(&secret_ref).await.unwrap();
    let tenant = PgPoolOptions::new().max_connections(1).connect(&tenant_dsn).await.unwrap();
    sqlx::query(
        "INSERT INTO _sqlx_migrations (version, description, success, checksum, execution_time) \
         VALUES ($1, 'from a newer build', true, '\\x00'::bytea, 1)",
    )
    .bind(head + 1)
    .execute(&tenant)
    .await
    .unwrap();
    let owner = fx.owner.clone();
    let set_version = |v: i64| {
        let owner = owner.clone();
        async move {
            sqlx::query("UPDATE tenant_databases SET schema_version = $2 WHERE clerk_org_id = $1")
                .bind(org)
                .bind(v)
                .execute(&owner)
                .await
                .unwrap();
        }
    };
    let set_step = |step: &'static str| {
        let owner = owner.clone();
        async move {
            sqlx::query("UPDATE tenant_provisioning_jobs SET step = $2, next_attempt_at = now() WHERE clerk_org_id = $1")
                .bind(org)
                .bind(step)
                .execute(&owner)
                .await
                .unwrap();
        }
    };
    set_version(head + 1).await;
    // The injected crash spent one attempt; the park must spend none.
    let spent = provisioning::get(&fx.app, org).await.unwrap().unwrap().attempts;

    let deps = fx.deps("runner-ahead", None, None);
    let assert_parks = |want: Step| {
        let deps = &deps;
        async move {
            for _ in 0..(deps.policy.max_attempts * 2) {
                let out = provisioning::run_job(deps, org).await.unwrap().unwrap();
                assert!(
                    matches!(&out, RunOutcome::Deferred { step, attempts, reason }
                        if *step == want && *attempts == spent && reason.contains("ahead of this build")),
                    "{want:?}: {out:?}"
                );
            }
        }
    };

    // On `activate`, the row ahead: park (it used to go back to `migrate`,
    // fail there on every attempt, and end in cleanup).
    set_step("activate").await;
    assert_parks(Step::Activate).await;
    // On `migrate`, the row ahead: park before touching the database.
    set_step("migrate").await;
    assert_parks(Step::Migrate).await;
    // On `migrate`, only the database's ledger ahead: sqlx's VersionMissing.
    set_version(head).await;
    assert_parks(Step::Migrate).await;

    let job = provisioning::get(&fx.app, org).await.unwrap().unwrap();
    assert_eq!((job.status.as_str(), job.attempts), ("provisioning", spent));
    assert!(job.version_wait_started_at.is_some(), "the version park's clock");
    assert!(job.failed_at.is_none() && job.clerk_org_cleanup == "none");
    assert_eq!(fx.db_count(&slug).await, 1, "the customer's database is never deleted");
    assert_eq!(routing::lookup_by_org(&fx.app, org).await.unwrap().unwrap().status, "provisioning");
    assert!(credentials::fetch_ciphertext(&fx.app, &secret_ref).await.unwrap().is_some());

    // The database is no longer ahead (the newer build is back, or the
    // operator reconciled): the job finishes.
    sqlx::query("DELETE FROM _sqlx_migrations WHERE version = $1")
        .bind(head + 1)
        .execute(&tenant)
        .await
        .unwrap();
    tenant.close().await;
    let out = provisioning::run_job(&deps, org).await.unwrap().unwrap();
    assert_eq!(out, RunOutcome::Ready);
    fx.assert_ready_artefacts(org, &slug, account_id).await;
    fx.drop_tenant_db(&slug).await;
}

/// SF-3 / N5: the worker's boot self-check of its provisioner pool. The
/// real provisioner login passes; the serving role, a login that is ALSO a
/// member of the serving role, a superuser (a member of every role) and a
/// login without EXECUTE on the claim definer are all refused.
#[tokio::test]
async fn the_provisioner_self_check_refuses_the_serving_role_and_a_login_that_cannot_claim() {
    let db = "orgt_prov_job_selfcheck";
    let Some(fx) = Fx::new(db).await else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };
    assert_eq!(provisioning::provisioner_self_check(&fx.prov).await.unwrap(), PROVISIONER_TEST_LOGIN);
    assert!(matches!(
        provisioning::provisioner_self_check(&fx.app).await,
        Err(ProvisionerSelfCheckError::ServingRole { current_user }) if current_user == "coralswarm_app"
    ));
    let superuser: bool = sqlx::query_scalar("SELECT rolsuper FROM pg_roles WHERE rolname = current_user")
        .fetch_one(&fx.owner)
        .await
        .unwrap();
    if superuser {
        assert!(matches!(
            provisioning::provisioner_self_check(&fx.owner).await,
            Err(ProvisionerSelfCheckError::ServingRole { .. })
        ));
    }

    // Two throwaway logins (cluster-wide, so uniquely named and dropped).
    let url = with_db(&fx.base, db);
    let pw = "orgt_selfcheck_test_only_pw";
    let tag = std::process::id();
    let bare = format!("orgt_selfcheck_bare_{tag}");
    let both = format!("orgt_selfcheck_both_{tag}");
    let owner_level = format!("orgt_selfcheck_upd_{tag}");
    for (role, grants) in [
        (&bare, ""),
        (&both, "coralswarm_provisioner, coralswarm_app"),
        (&owner_level, "coralswarm_provisioner"),
    ] {
        sqlx::query(&format!("DROP ROLE IF EXISTS {role}")).execute(&fx.owner).await.unwrap();
        sqlx::query(&format!(
            "CREATE ROLE {role} LOGIN PASSWORD '{pw}' NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS"
        ))
        .execute(&fx.owner)
        .await
        .unwrap();
        if !grants.is_empty() {
            sqlx::query(&format!("GRANT {grants} TO {role}")).execute(&fx.owner).await.unwrap();
        }
    }
    // N4: an owner-level DSN can UPDATE routing directly.
    sqlx::query(&format!("GRANT UPDATE ON public.tenant_databases TO {owner_level}"))
        .execute(&fx.owner)
        .await
        .unwrap();
    let pool_as = |role: &str| {
        PgPoolOptions::new()
            .max_connections(1)
            .connect_lazy(&as_role(&url, role, pw))
            .unwrap()
    };
    let bare_pool = pool_as(&bare);
    let both_pool = pool_as(&both);
    let bare_result = provisioning::provisioner_self_check(&bare_pool).await;
    let both_result = provisioning::provisioner_self_check(&both_pool).await;
    let owner_level_pool = pool_as(&owner_level);
    let owner_level_result = provisioning::provisioner_self_check(&owner_level_pool).await;
    bare_pool.close().await;
    both_pool.close().await;
    owner_level_pool.close().await;
    sqlx::query(&format!("REVOKE ALL ON public.tenant_databases FROM {owner_level}"))
        .execute(&fx.owner)
        .await
        .unwrap();
    for role in [&bare, &both, &owner_level] {
        sqlx::query(&format!("DROP ROLE IF EXISTS {role}")).execute(&fx.owner).await.unwrap();
    }
    assert_eq!(bare_result, Err(ProvisionerSelfCheckError::MissingClaimExecute { current_user: bare.clone() }));
    assert_eq!(both_result, Err(ProvisionerSelfCheckError::ServingRole { current_user: both.clone() }));
    assert_eq!(
        owner_level_result,
        Err(ProvisionerSelfCheckError::OwnerLevelLogin { current_user: owner_level.clone() })
    );
}

/// SF (pre-merge): the boot self-check sees the LIVE group role's
/// attributes, which 0381 asserts only once and the deploy guard never sees
/// (it checks a fresh CI database). A group role that can log in (or holds
/// CREATEDB, ...) is refused even though the login itself is fine. Planted on
/// a throwaway group role: the real `coralswarm_provisioner` is cluster-wide
/// and shared with every concurrent scratch database.
#[tokio::test]
async fn the_provisioner_self_check_refuses_a_group_role_with_login_or_other_attributes() {
    let db = "orgt_prov_job_selfcheck_attrs";
    let Some(fx) = Fx::new(db).await else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };
    // The real group role, as 0381 created it, passes.
    assert_eq!(
        provisioning::provisioner_self_check_with_group(&fx.prov, provisioning::PROVISIONER_GROUP_ROLE)
            .await
            .unwrap(),
        PROVISIONER_TEST_LOGIN
    );
    let tag = std::process::id();
    let clean = format!("orgt_selfcheck_group_ok_{tag}");
    let bad = format!("orgt_selfcheck_group_bad_{tag}");
    for (role, attrs) in [
        (&clean, "NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS"),
        (&bad, "LOGIN CREATEDB NOSUPERUSER NOCREATEROLE NOREPLICATION NOBYPASSRLS"),
    ] {
        sqlx::query(&format!("DROP ROLE IF EXISTS {role}")).execute(&fx.owner).await.unwrap();
        sqlx::query(&format!("CREATE ROLE {role} {attrs}")).execute(&fx.owner).await.unwrap();
    }
    let clean_result = provisioning::provisioner_self_check_with_group(&fx.prov, &clean).await;
    let bad_result = provisioning::provisioner_self_check_with_group(&fx.prov, &bad).await;
    for role in [&clean, &bad] {
        sqlx::query(&format!("DROP ROLE IF EXISTS {role}")).execute(&fx.owner).await.unwrap();
    }
    assert_eq!(clean_result.as_deref(), Ok(PROVISIONER_TEST_LOGIN));
    assert_eq!(
        bad_result,
        Err(ProvisionerSelfCheckError::GroupRoleAttributes {
            role: bad.clone(),
            attributes: vec!["createdb".to_string(), "login".to_string()],
        })
    );
}
