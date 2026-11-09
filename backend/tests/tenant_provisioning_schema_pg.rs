//! Self-serve org signup, PR A1a: the control-plane schema for automated
//! tenant provisioning (migration 0381) and the resolver chain.
//!
//! What this suite proves, each from a REAL login: the least-privilege
//! `coralswarm_app` role (the serving role in production since OPS-4) and a
//! login granted `coralswarm_provisioner` (the provisioning worker's role):
//!
//! * neither role has table write on `tenant_databases` (0083's hardening
//!   is preserved); the SECURITY DEFINER functions are the only way a
//!   routing row advances, each refusing out-of-order or mismatched calls
//!   rather than silently no-op'ing, and only the provisioner may call the
//!   ones that write routing or credentials or advance a job (the app role
//!   gets 42501);
//! * every definer write satisfies migration 0321's
//!   `billing_guard_tenant_binding` trigger (INSERT, DELETE, and the
//!   `accounts`-row precondition) — the audit's row 4c understated this;
//! * `migrate_all` run in-process by the provisioner, under the job's lease,
//!   records `schema_version` (audit 4d: previously a `warn!` and a NULL that
//!   the worker harness reads as "skip forever");
//! * the resolver chain is environment-first (an existing concierge tenant
//!   resolves byte-identically), then the KEK-encrypted table, and fails
//!   closed on a wrong KEK;
//! * the operator bins' shared `OperatorDsnResolver` (issue #1514) refuses
//!   any non-`TENANT_DB_` ref, lets the environment win, falls back to the
//!   encrypted row as either SELECT-holding role, and never writes a DSN to
//!   a log or an error;
//! * `provision_org_dek_if_absent` mints once and never rewrites;
//! * (security review) the app role cannot write the job table, a forged job
//!   row cannot get a routing row, `schema_version` is never rewritten on a
//!   live tenant through the definer, the ref CHECKs pin `TENANT_DB_` names,
//!   activation refuses behind a newer rolled-out tree, and a ciphertext
//!   moved to another ref does not decrypt.
//!
//! Gated on `ORGT_TEST_BASE_URL` (CREATE DATABASE + ALTER ROLE rights);
//! `REQUIRE_PG_SUITE=1` turns a missing URL into a failure:
//!
//!   REQUIRE_PG_SUITE=1 ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test tenant_provisioning_schema_pg

mod support;

#[path = "support/retained_database.rs"]
mod retained_database;

#[path = "support/financial_fixture.rs"]
mod financial_fixture;

use std::sync::Arc;

use coralswarm_backend::tenancy::credentials::{self, StoreOutcome};
use coralswarm_backend::tenancy::dek::{provision_org_dek_if_absent, DekProvision};
use coralswarm_backend::tenancy::migrate::{self, MigrateOpts, TenantResult};
use coralswarm_backend::tenancy::provisioning::{opaque_slug_for_account, secret_ref_for_slug};
use coralswarm_backend::tenancy::routing;
use coralswarm_backend::tenancy::secrets::{
    ChainSecretResolver, ControlTableSecretResolver, OperatorDsnError, OperatorDsnResolver,
    SecretError, SecretResolver, StaticMapResolver,
};
use sqlx::postgres::PgPoolOptions;
use sqlx::{Connection, PgConnection, PgPool};
use std::time::Duration;

use support::app_role::{set_app_role_password_with_retry, APP_ROLE_TEST_PASSWORD};
use support::provisioner_role::{prepare_provisioner_login, PROVISIONER_TEST_LOGIN, PROVISIONER_TEST_PASSWORD};
use uuid::Uuid;

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
async fn admin(base: &str) -> PgConnection {
    PgConnection::connect(base).await.expect("connect admin")
}

struct Control {
    owner: PgPool,
    app: PgPool,
    /// A real login granted `coralswarm_provisioner` (the worker's role).
    prov: PgPool,
}

/// The lease every fixture job is held under (see [`job_at_step`]).
const LEASE: &str = "fixture-owner";
const TTL: Duration = Duration::from_secs(600);

/// A fresh control database at the control tree's head, plus an owner pool
/// and a REAL `coralswarm_app` login against it.
async fn control(base: &str, db: &str) -> Control {
    let mut conn = admin(base).await;
    retained_database::recreate(&mut conn, db).await.unwrap();
    let url = with_db(base, db);
    let owner = PgPoolOptions::new()
        .max_connections(4)
        .connect(&url)
        .await
        .expect("connect owner");
    sqlx::migrate!("./migrations_control")
        .run(&owner)
        .await
        .expect("control tree");
    set_app_role_password_with_retry(&owner).await;
    let app = PgPoolOptions::new()
        .max_connections(4)
        .connect(&as_role(&url, "coralswarm_app", APP_ROLE_TEST_PASSWORD))
        .await
        .expect("connect as coralswarm_app");
    let (user, bypass): (String, bool) =
        sqlx::query_as("SELECT current_user::text, rolbypassrls FROM pg_roles WHERE rolname = current_user")
            .fetch_one(&app)
            .await
            .unwrap();
    assert_eq!(user, "coralswarm_app");
    assert!(!bypass, "the app pool must not bypass RLS or this suite proves nothing");
    prepare_provisioner_login(&owner).await;
    let prov = PgPoolOptions::new()
        // Two: the worker issues one statement at a time, and the suites
        // run every test concurrently against one local server.
        .max_connections(2)
        .connect(&as_role(&url, PROVISIONER_TEST_LOGIN, PROVISIONER_TEST_PASSWORD))
        .await
        .expect("connect as the provisioner login");
    let (user, member, sup): (String, bool, bool) = sqlx::query_as(
        "SELECT current_user::text, pg_has_role(current_user, 'coralswarm_provisioner', 'USAGE'), \
                (SELECT rolsuper OR rolbypassrls FROM pg_roles WHERE rolname = current_user)",
    )
    .fetch_one(&prov)
    .await
    .unwrap();
    assert_eq!(user, PROVISIONER_TEST_LOGIN);
    assert!(member && !sup, "the provisioner pool must be an unprivileged coralswarm_provisioner member");
    Control { owner, app, prov }
}

async fn tenant_db(base: &str, db: &str) -> (String, PgPool) {
    let mut conn = admin(base).await;
    retained_database::recreate(&mut conn, db).await.unwrap();
    let url = with_db(base, db);
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await
        .expect("connect tenant");
    (url, pool)
}

async fn org_account(owner: &PgPool, org: &str) -> Uuid {
    sqlx::query_scalar(
        "WITH u AS (INSERT INTO users(email) VALUES($1) RETURNING id) \
         INSERT INTO accounts(owner_user_id, clerk_org_id) SELECT id, $2 FROM u RETURNING id",
    )
    .bind(format!("{org}@provisioning-schema.test"))
    .bind(org)
    .fetch_one(owner)
    .await
    .unwrap()
}

/// A live self-serve job for `org`, enqueued the way `provisioning::enqueue`
/// does it: through the `tenant_provisioning_enqueue` definer as the app role
/// (it holds no INSERT on the job table). Returns `(slug, secret_ref)`, both
/// derived from the account.
async fn enqueue_job(app: &PgPool, org: &str, account_id: Uuid) -> (String, String) {
    let inserted: bool = sqlx::query_scalar("SELECT tenant_provisioning_enqueue($1, $2, NULL)")
        .bind(org)
        .bind(account_id)
        .fetch_one(app)
        .await
        .expect("app role enqueues through the definer");
    assert!(inserted);
    let slug = opaque_slug_for_account(account_id);
    let secret_ref = secret_ref_for_slug(&slug);
    (slug, secret_ref)
}

/// Test setup only (the owner, never the code under test): put a job on
/// `step` with its database recorded and held under [`LEASE`], as the worker
/// would have it by then.
async fn job_at_step(owner: &PgPool, org: &str, step: &str) {
    sqlx::query(
        "UPDATE tenant_provisioning_jobs \
            SET step = $2, database_create_requested_at = COALESCE(database_create_requested_at, now()), \
                provider = 'local', provider_database_id = COALESCE(provider_database_id, 'coralswarm_tenant_fixture'), \
                lease_owner = 'fixture-owner', lease_expires_at = now() + interval '1 hour' \
          WHERE clerk_org_id = $1",
    )
    .bind(org)
    .bind(step)
    .execute(owner)
    .await
    .unwrap();
}

/// Upsert the account's `stripe_subscriptions` mirror (owner, test setup).
async fn set_subscription(owner: &PgPool, account_id: Uuid, status: &str, sub_id: Option<&str>) {
    financial_fixture::execute(
        owner,
        account_id,
        sqlx::query(
            "INSERT INTO stripe_subscriptions (account_id, stripe_customer_id, stripe_subscription_id, status) \
             VALUES ($1, 'cus_' || replace($1::text, '-', ''), $2, $3) \
             ON CONFLICT (account_id) DO UPDATE \
                SET stripe_subscription_id = EXCLUDED.stripe_subscription_id, status = EXCLUDED.status",
        )
        .bind(account_id)
        .bind(sub_id)
        .bind(status),
    )
    .await
    .unwrap();
}

/// Store a credential row directly as the owner (test setup for resolver
/// tests that have no job), encrypted exactly as `credentials::store` does.
async fn put_credential(owner: &PgPool, secret_ref: &str, ciphertext: &[u8]) {
    sqlx::query("INSERT INTO tenant_db_credentials (secret_ref, dsn_encrypted) VALUES ($1, $2)")
        .bind(secret_ref)
        .bind(ciphertext)
        .execute(owner)
        .await
        .unwrap();
}

/// A routing row for a concierge tenant, the way `onboard_tenant.sh` writes
/// it (owner, no job).
async fn concierge_row(owner: &PgPool, org: &str, account_id: Uuid, slug: &str, schema_version: Option<i64>) {
    routing_row(owner, org, account_id, slug, "active", schema_version).await;
}

async fn routing_row(owner: &PgPool, org: &str, account_id: Uuid, slug: &str, status: &str, schema_version: Option<i64>) {
    financial_fixture::execute(
        owner,
        account_id,
        sqlx::query(
            "INSERT INTO tenant_databases(clerk_org_id,account_id,slug,db_secret_ref,status,schema_version) \
             VALUES($1,$2,$3,$4,$5,$6)",
        )
        .bind(org)
        .bind(account_id)
        .bind(slug)
        .bind(secret_ref_for_slug(slug))
        .bind(status)
        .bind(schema_version),
    )
    .await
    .unwrap();
}

/// The version write the provisioning job makes, under [`LEASE`].
async fn record_version(prov: &PgPool, org: &str, v: i64) -> Result<(), sqlx::Error> {
    routing::record_schema_version_via_definer(prov, org, LEASE, TTL, v).await
}

fn sqlstate(e: &sqlx::Error) -> String {
    e.as_database_error()
        .and_then(|d| d.code().map(|c| c.to_string()))
        .unwrap_or_else(|| format!("<not a database error: {e}>"))
}

const KEK: [u8; 32] = [0x42u8; 32];

#[tokio::test]
async fn provisioner_advances_a_routing_row_only_through_the_definer_functions() {
    let Some(base) = base_url() else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };
    let c = control(&base, "orgt_prov_schema_definer").await;
    let org = "org_prov_definer";
    let account_id = org_account(&c.owner, org).await;
    set_subscription(&c.owner, account_id, "active", Some("sub_prov_definer")).await;
    let slug = opaque_slug_for_account(account_id);
    let secret_ref = secret_ref_for_slug(&slug);
    let (slug, secret_ref) = (slug.as_str(), secret_ref.as_str());

    // 0083 preserved: no table write for either role.
    for (pool, who) in [(&c.app, "app"), (&c.prov, "provisioner")] {
        let direct = sqlx::query(
            "INSERT INTO tenant_databases (clerk_org_id, account_id, slug, db_secret_ref) \
             VALUES ($1, $2, $3, $4)",
        )
        .bind(org)
        .bind(account_id)
        .bind(slug)
        .bind(secret_ref)
        .execute(pool)
        .await
        .expect_err("no direct INSERT on tenant_databases");
        assert_eq!(sqlstate(&direct), "42501", "{who}");
    }

    // No job → the insert function refuses (nothing to advance).
    let e = routing::provisioning_insert_routing(&c.prov, org, account_id, slug, secret_ref, None)
        .await
        .expect_err("insert without a job row");
    assert_eq!(sqlstate(&e), "P0002");

    enqueue_job(&c.app, org, account_id).await;
    // Not on its step yet → refused (the job's own step order is enforced).
    let e = routing::provisioning_insert_routing(&c.prov, org, account_id, slug, secret_ref, None)
        .await
        .expect_err("insert while the job is on verify_org");
    assert_eq!(sqlstate(&e), "23514");
    job_at_step(&c.owner, org, "insert_routing").await;

    // Job but no credential → refused: a routing row must never exist that a
    // release's migrate_tenants cannot resolve.
    let e = routing::provisioning_insert_routing(&c.prov, org, account_id, slug, secret_ref, None)
        .await
        .expect_err("insert before the credential is stored");
    assert_eq!(sqlstate(&e), "23514");

    // Credential, through the provisioner role.
    let dsn = "postgres://fixture:synthetic@localhost:5432/orgt_prov_definer_tenant";
    assert_eq!(
        credentials::store(&c.prov, &KEK, None, secret_ref, dsn).await.unwrap(),
        StoreOutcome::Inserted
    );
    assert_eq!(
        credentials::store(&c.prov, &KEK, None, secret_ref, dsn).await.unwrap(),
        StoreOutcome::AlreadyPresent,
        "a retried store converges"
    );
    assert!(
        matches!(
            credentials::store(&c.prov, &KEK, None, secret_ref, "postgres://other@localhost/x").await,
            Err(credentials::CredentialError::Conflict(_))
        ),
        "a different DSN under the same ref is never silently replaced"
    );
    // Neither role can bypass the function.
    for (pool, who) in [(&c.app, "app"), (&c.prov, "provisioner")] {
        let e = sqlx::query("UPDATE tenant_db_credentials SET dsn_encrypted = '\\x00' WHERE secret_ref = $1")
            .bind(secret_ref)
            .execute(pool)
            .await
            .expect_err("no direct UPDATE on credentials");
        assert_eq!(sqlstate(&e), "42501", "{who}");
    }

    // A mismatched binding is refused even with a job and a credential.
    let e = routing::provisioning_insert_routing(&c.prov, org, account_id, "other-slug", secret_ref, None)
        .await
        .expect_err("slug must match the job");
    assert_eq!(sqlstate(&e), "23514");

    // The real insert — this is the path that must satisfy 0321's trigger.
    assert!(routing::provisioning_insert_routing(&c.prov, org, account_id, slug, secret_ref, Some("aws-us-east-2"))
        .await
        .unwrap());
    assert!(
        !routing::provisioning_insert_routing(&c.prov, org, account_id, slug, secret_ref, Some("aws-us-east-2"))
            .await
            .unwrap(),
        "a retry converges on the existing provisioning row"
    );
    let row = routing::lookup_by_org(&c.app, org).await.unwrap().unwrap();
    assert_eq!(row.status, "provisioning");
    assert_eq!(row.schema_version, None);
    let inserted_at: Option<chrono::DateTime<chrono::Utc>> = sqlx::query_scalar(
        "SELECT routing_inserted_at FROM tenant_provisioning_jobs WHERE clerk_org_id = $1",
    )
    .bind(org)
    .fetch_one(&c.app)
    .await
    .unwrap();
    assert!(inserted_at.is_some(), "the job records what it created");

    // Activation refuses until schema_version is at the expected head.
    job_at_step(&c.owner, org, "activate").await;
    let e = routing::provisioning_activate(&c.prov, org, 7).await.expect_err("NULL schema_version");
    assert_eq!(sqlstate(&e), "23514");

    // schema_version: the direct UPDATE is denied (and no longer falls back),
    // so the job records it through the definer under its lease, on
    // `migrate`, and reads back what landed.
    job_at_step(&c.owner, org, "migrate").await;
    let e = routing::set_schema_version(&c.prov, org, 7).await.expect_err("direct UPDATE");
    assert_eq!(sqlstate(&e), "42501");
    record_version(&c.prov, org, 7).await.expect("the lease holder records the version");
    let row = routing::lookup_by_org(&c.app, org).await.unwrap().unwrap();
    assert_eq!(row.schema_version, Some(7));
    // Against a missing row it errors instead of reporting success.
    assert!(record_version(&c.prov, "org_does_not_exist", 7).await.is_err());
    // S2: never downward, even on a row still being provisioned.
    let e = record_version(&c.prov, org, 6).await.expect_err("lower version");
    assert_eq!(sqlstate(&e), "23514");

    job_at_step(&c.owner, org, "activate").await;
    let e = routing::provisioning_activate(&c.prov, org, 8).await.expect_err("version behind this build");
    assert_eq!(sqlstate(&e), "23514");
    assert!(routing::provisioning_activate(&c.prov, org, 7).await.unwrap());
    assert!(!routing::provisioning_activate(&c.prov, org, 7).await.unwrap(), "already active converges");
    let row = routing::lookup_by_org(&c.app, org).await.unwrap().unwrap();
    assert_eq!(row.status, "active");
    let (status, step): (String, String) =
        sqlx::query_as("SELECT status, step FROM tenant_provisioning_jobs WHERE clerk_org_id = $1")
            .bind(org)
            .fetch_one(&c.app)
            .await
            .unwrap();
    assert_eq!((status.as_str(), step.as_str()), ("ready", "done"));

    // S2: once active, nobody but the release rewrites the tenant's version,
    // up or down; it stays what the job recorded.
    for v in [9_i64, 3] {
        let e = record_version(&c.prov, org, v).await.expect_err("active tenant");
        assert_eq!(sqlstate(&e), "23514", "v{v}");
        let e = routing::set_schema_version(&c.prov, org, v).await.expect_err("direct UPDATE");
        assert_eq!(sqlstate(&e), "42501", "v{v}");
    }
    assert_eq!(routing::lookup_by_org(&c.app, org).await.unwrap().unwrap().schema_version, Some(7));

    // A live tenant's routing row is never deletable through the cleanup path.
    let e = routing::provisioning_delete_routing(&c.prov, org).await.expect_err("active row");
    assert_eq!(sqlstate(&e), "23514");
    let e = credentials::delete(&c.prov, secret_ref).await.expect_err("referenced credential");
    assert!(matches!(e, credentials::CredentialError::Db(ref d) if sqlstate(d) == "23503"));
}

/// S1-a: the app role can satisfy every precondition `insert_routing`
/// checks for itself (it writes `stripe_subscriptions` and enters accounts),
/// so it must not be able to call it, or any other definer that writes
/// routing or credentials or advances a job. Target: an org account with no
/// routing row (a pre-split shared-DB org, a Clerk-only org, a concierge
/// org mid-onboarding). What the app role keeps still works.
#[tokio::test]
async fn app_role_cannot_call_the_provisioner_definers() {
    let Some(base) = base_url() else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };
    let c = control(&base, "orgt_prov_schema_app_denied").await;
    let org = "org_prov_app_denied";
    let account_id = org_account(&c.owner, org).await;
    let slug = opaque_slug_for_account(account_id);
    let secret_ref = secret_ref_for_slug(&slug);

    // What the app role can do on its own: a live subscription for the
    // victim org, and a job for it through enqueue.
    set_subscription(&c.owner, account_id, "active", Some("sub_app_denied")).await;
    enqueue_job(&c.app, org, account_id).await;
    job_at_step(&c.owner, org, "insert_routing").await;

    let e = routing::provisioning_insert_routing(&c.app, org, account_id, &slug, &secret_ref, None)
        .await
        .expect_err("app role calling insert_routing");
    assert_eq!(sqlstate(&e), "42501");
    let e = routing::provisioning_activate(&c.app, org, migrate::head_version())
        .await
        .expect_err("app role calling activate");
    assert_eq!(sqlstate(&e), "42501");

    // Every other worker definer, by signature.
    for sql in [
        "SELECT tenant_provisioning_store_credential($1, '\\x00000000000000000000000000000000000000000000000000000000000000'::bytea)",
        "SELECT tenant_provisioning_delete_credential($1)",
        "SELECT tenant_provisioning_record_schema_version($1, 'x', interval '1 minute', 1)",
        "SELECT tenant_provisioning_delete_routing($1)",
        "SELECT clerk_org_id FROM tenant_provisioning_claim('x', interval '1 minute', $1)",
        "SELECT tenant_provisioning_set_step($1, 'x', interval '1 minute', 'migrate', NULL)",
        "SELECT tenant_provisioning_defer($1, 'x', 0, NULL, interval '1 minute', NULL)",
        "SELECT tenant_provisioning_record_create_intent($1, 'x', interval '1 minute')",
        "SELECT tenant_provisioning_clear_create_intent($1, 'x', interval '1 minute')",
        "SELECT tenant_provisioning_record_database($1, 'x', interval '1 minute', 'local', 'db')",
        "SELECT tenant_provisioning_record_credential_stored($1, 'x', interval '1 minute')",
        "SELECT tenant_provisioning_fail($1, 'x', 'r', false)",
    ] {
        let e = sqlx::query(sql).bind(org).execute(&c.app).await.expect_err(sql);
        assert_eq!(sqlstate(&e), "42501", "{sql}");
    }
    assert!(routing::lookup_by_org(&c.app, org).await.unwrap().is_none(), "no routing row appeared");

    // The provisioner, conversely, cannot enqueue or retry: it is not the
    // web tier.
    for sql in [
        "SELECT tenant_provisioning_enqueue($1, gen_random_uuid(), NULL)",
        "SELECT tenant_provisioning_retry($1)",
        "SELECT tenant_provisioning_cancel($1)",
    ] {
        let e = sqlx::query(sql).bind(org).execute(&c.prov).await.expect_err(sql);
        assert_eq!(sqlstate(&e), "42501", "{sql}");
    }

    // What the app role keeps.
    assert!(coralswarm_backend::tenancy::provisioning::cancel(&c.app, org).await.unwrap());
    assert!(!coralswarm_backend::tenancy::provisioning::mark_clerk_org_cleanup_done(&c.app, org, chrono::Utc::now()).await.unwrap());
}

/// S2-a: `record_schema_version` is the lease holder's, on `migrate`, and
/// only for a version some build could have.
#[tokio::test]
async fn record_schema_version_needs_the_lease_the_step_and_a_real_version() {
    let Some(base) = base_url() else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };
    let c = control(&base, "orgt_prov_schema_version_lease").await;
    let org = "org_prov_version_lease";
    let account_id = org_account(&c.owner, org).await;
    set_subscription(&c.owner, account_id, "active", Some("sub_version_lease")).await;
    let (slug, secret_ref) = enqueue_job(&c.app, org, account_id).await;
    job_at_step(&c.owner, org, "insert_routing").await;
    credentials::store(&c.prov, &KEK, None, &secret_ref, "postgres://f:s@localhost/lease").await.unwrap();
    routing::provisioning_insert_routing(&c.prov, org, account_id, &slug, &secret_ref, None).await.unwrap();
    job_at_step(&c.owner, org, "migrate").await;

    // Not the lease holder.
    for owner in ["intruder", ""] {
        let e = routing::record_schema_version_via_definer(&c.prov, org, owner, TTL, 5)
            .await
            .expect_err("not the lease holder");
        assert_eq!(sqlstate(&e), "23514", "{owner:?}");
    }
    // Beyond any four-digit tenant migration.
    for v in [10_000_i64, i64::MAX, -1] {
        let e = record_version(&c.prov, org, v).await.expect_err("out of range");
        assert_eq!(sqlstate(&e), "22023", "v{v}");
    }
    assert!(migrate::head_version() <= routing::MAX_TENANT_SCHEMA_VERSION, "the cap admits this build's head");
    // Not on `migrate`.
    job_at_step(&c.owner, org, "contract").await;
    let e = record_version(&c.prov, org, 5).await.expect_err("wrong step");
    assert_eq!(sqlstate(&e), "23514");
    assert_eq!(routing::lookup_by_org(&c.app, org).await.unwrap().unwrap().schema_version, None);

    // The holder, on `migrate`: recorded, up to the cap, and the lease renewed.
    job_at_step(&c.owner, org, "migrate").await;
    sqlx::query("UPDATE tenant_provisioning_jobs SET lease_expires_at = now() + interval '1 second' WHERE clerk_org_id = $1")
        .bind(org)
        .execute(&c.owner)
        .await
        .unwrap();
    record_version(&c.prov, org, routing::MAX_TENANT_SCHEMA_VERSION).await.unwrap();
    let renewed: bool = sqlx::query_scalar(
        "SELECT lease_expires_at > now() + interval '9 minutes' FROM tenant_provisioning_jobs WHERE clerk_org_id = $1",
    )
    .bind(org)
    .fetch_one(&c.app)
    .await
    .unwrap();
    assert!(renewed, "a version write renews the lease");
}

#[tokio::test]
async fn cleanup_deletes_in_reverse_order_and_converges() {
    let Some(base) = base_url() else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };
    let c = control(&base, "orgt_prov_schema_cleanup").await;
    let org = "org_prov_cleanup";
    let account_id = org_account(&c.owner, org).await;
    set_subscription(&c.owner, account_id, "active", Some("sub_prov_cleanup")).await;
    let (slug, secret_ref) = enqueue_job(&c.app, org, account_id).await;
    let (slug, secret_ref) = (slug.as_str(), secret_ref.as_str());
    job_at_step(&c.owner, org, "insert_routing").await;
    credentials::store(&c.prov, &KEK, None, secret_ref, "postgres://f:s@localhost/cleanup")
        .await
        .unwrap();
    assert!(routing::provisioning_insert_routing(&c.prov, org, account_id, slug, secret_ref, None)
        .await
        .unwrap());

    // The credential is refused while the routing row references it …
    assert!(credentials::delete(&c.prov, secret_ref).await.is_err());
    // … so the routing row goes first (0321's DELETE check is satisfied
    // inside the function), then the credential, each converging on 0.
    assert_eq!(routing::provisioning_delete_routing(&c.prov, org).await.unwrap(), 1);
    assert_eq!(routing::provisioning_delete_routing(&c.prov, org).await.unwrap(), 0);
    assert!(routing::lookup_by_org(&c.app, org).await.unwrap().is_none());
    assert_eq!(credentials::delete(&c.prov, secret_ref).await.unwrap(), 1);
    assert_eq!(credentials::delete(&c.prov, secret_ref).await.unwrap(), 0);
    assert!(credentials::fetch_ciphertext(&c.app, secret_ref).await.unwrap().is_none());

    // A ready job's row can never be deleted through this path.
    sqlx::query("UPDATE tenant_provisioning_jobs SET status = 'ready', step = 'done' WHERE clerk_org_id = $1")
        .bind(org)
        .execute(&c.owner)
        .await
        .unwrap();
    let e = routing::provisioning_delete_routing(&c.prov, org).await.expect_err("ready job");
    assert_eq!(sqlstate(&e), "23514");
}

#[tokio::test]
async fn resolver_chain_is_environment_first_then_encrypted_table() {
    let Some(base) = base_url() else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };
    let c = control(&base, "orgt_prov_schema_resolver").await;
    let table_only = "TENANT_DB_PROV_TABLE_ONLY";
    let shadowed = "TENANT_DB_PROV_SHADOWED";
    put_credential(&c.owner, table_only, &credentials::encrypt_dsn(&KEK, table_only, "postgres://t:s@localhost/from_table").unwrap()).await;
    put_credential(&c.owner, shadowed, &credentials::encrypt_dsn(&KEK, shadowed, "postgres://t:s@localhost/from_table_losing").unwrap()).await;
    // The app role cannot store a credential at all; the provisioner cannot
    // plant one under a ref no live job owns.
    let e = credentials::store(&c.app, &KEK, None, "TENANT_DB_PLANTED", "postgres://evil@localhost/x")
        .await
        .expect_err("app role");
    assert!(matches!(e, credentials::CredentialError::Db(ref d) if sqlstate(d) == "42501"), "{e:?}");
    let e = credentials::store(&c.prov, &KEK, None, "TENANT_DB_PLANTED", "postgres://evil@localhost/x")
        .await
        .expect_err("no job owns this ref");
    assert!(matches!(e, credentials::CredentialError::Db(ref d) if sqlstate(d) == "P0002"), "{e:?}");

    // A static "environment" stands in for EnvSecretResolver so this test
    // never mutates process env: an existing concierge tenant's ref.
    let env = Arc::new(StaticMapResolver::from_pairs([
        ("TENANT_DB_CONCIERGE", "postgres://c:s@localhost/from_env"),
        (shadowed, "postgres://c:s@localhost/from_env_winning"),
    ]));
    let chain = ChainSecretResolver::new(
        env,
        ControlTableSecretResolver::new(c.app.clone(), KEK, None),
    );

    // Existing tenant: byte-identical, and the sync path still answers.
    assert_eq!(chain.resolve_async("TENANT_DB_CONCIERGE").await.unwrap(), "postgres://c:s@localhost/from_env");
    assert_eq!(chain.resolve("TENANT_DB_CONCIERGE").unwrap(), "postgres://c:s@localhost/from_env");
    // Environment wins over a table row with the same ref.
    assert_eq!(chain.resolve_async(shadowed).await.unwrap(), "postgres://c:s@localhost/from_env_winning");
    // Table-only ref: resolved async, NotFound sync (never a half answer).
    assert_eq!(chain.resolve_async(table_only).await.unwrap(), "postgres://t:s@localhost/from_table");
    assert!(matches!(chain.resolve(table_only), Err(SecretError::NotFound(_))));
    // Unknown everywhere.
    assert!(matches!(chain.resolve_async("TENANT_DB_NOWHERE").await, Err(SecretError::NotFound(_))));

    // Wrong KEK: fail closed, and NOT as NotFound.
    let wrong = ChainSecretResolver::new(
        Arc::new(StaticMapResolver::from_pairs::<[(&str, &str); 0], _, _>([])),
        ControlTableSecretResolver::new(c.app.clone(), [0x99u8; 32], None),
    );
    assert!(matches!(wrong.resolve_async(table_only).await, Err(SecretError::Undecryptable(_))));
    // Rotation: the row was encrypted under KEK, which is now NEXT.
    let rotating = ChainSecretResolver::new(
        Arc::new(StaticMapResolver::from_pairs::<[(&str, &str); 0], _, _>([])),
        ControlTableSecretResolver::new(c.app.clone(), [0x99u8; 32], Some(KEK)),
    );
    assert_eq!(rotating.resolve_async(table_only).await.unwrap(), "postgres://t:s@localhost/from_table");

    // N1: the ciphertext is bound to its ref. Copied verbatim onto another
    // ref's row it fails closed there, under the right KEK.
    let swapped = "TENANT_DB_PROV_SWAPPED";
    let bytes = credentials::fetch_ciphertext(&c.app, table_only).await.unwrap().unwrap();
    put_credential(&c.owner, swapped, &bytes).await;
    assert!(matches!(chain.resolve_async(swapped).await, Err(SecretError::Undecryptable(_))));
    assert_eq!(chain.resolve_async(table_only).await.unwrap(), "postgres://t:s@localhost/from_table");
}

/// A `tracing` writer that keeps everything, so a test can prove a DSN was
/// never logged at any level (sqlx's own query logging included).
#[derive(Clone, Default)]
struct CapturedLog(Arc<std::sync::Mutex<Vec<u8>>>);
impl std::io::Write for CapturedLog {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CapturedLog {
    type Writer = CapturedLog;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

/// Issue #1514: every per-tenant operator bin resolves through
/// `OperatorDsnResolver`. Same chain as `migrate_tenants --resolve-dsn`
/// (environment first, then the row under AAD `tenant-dsn:v1:<ref>`), behind
/// the `^TENANT_DB_` gate, from either role that holds SELECT on
/// `tenant_db_credentials`, and with no DSN in any log line or error.
#[tokio::test]
async fn operator_dsn_resolver_gates_refs_prefers_env_falls_back_to_table_and_never_logs() {
    let Some(base) = base_url() else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };
    let c = control(&base, "orgt_prov_schema_operator_resolver").await;
    // Every DSN carries a unique password marker, so "never logged" is a
    // substring check that cannot pass by accident.
    let table_only = "TENANT_DB_OPR_TABLE_ONLY";
    let shadowed = "TENANT_DB_OPR_SHADOWED";
    let undecryptable = "TENANT_DB_OPR_WRONG_KEK";
    let table_dsn = "postgres://t:opr_pw_table@localhost/from_table";
    let losing_dsn = "postgres://t:opr_pw_losing@localhost/from_table_losing";
    let env_dsn = "postgres://c:opr_pw_env@localhost/from_env_winning";
    let wrong_kek_dsn = "postgres://t:opr_pw_wrongkek@localhost/x";
    let markers = ["opr_pw_table", "opr_pw_losing", "opr_pw_env", "opr_pw_wrongkek", "opr_pw_gate"];
    put_credential(&c.owner, table_only, &credentials::encrypt_dsn(&KEK, table_only, table_dsn).unwrap()).await;
    put_credential(&c.owner, shadowed, &credentials::encrypt_dsn(&KEK, shadowed, losing_dsn).unwrap()).await;
    put_credential(&c.owner, undecryptable, &credentials::encrypt_dsn(&[0x77u8; 32], undecryptable, wrong_kek_dsn).unwrap()).await;

    let log = CapturedLog::default();
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .with_ansi(false)
        .with_writer(log.clone())
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);

    let mut errors = Vec::new();
    // Both roles that may run the resolver: the owner/migrator (the bins'
    // `MIGRATOR_DATABASE_URL`) and the app role (`--resolve-dsn`'s
    // `DATABASE_URL`). Neither needs, or is given, any write.
    for (role, pool) in [("owner", &c.owner), ("coralswarm_app", &c.app)] {
        // A static map stands in for the process environment (no env
        // mutation). It also holds a non-tenant name and a DSN-shaped key,
        // so only the gate can be what refuses them.
        let env = Arc::new(StaticMapResolver::from_pairs([
            (shadowed, env_dsn),
            ("CLERK_SECRET_KEY", "sk_live_opr_pw_gate"),
            ("postgres://u:opr_pw_gate@h/db", "postgres://u:opr_pw_gate@h/db"),
        ]));
        let r = OperatorDsnResolver::new(Arc::new(ChainSecretResolver::new(
            env,
            ControlTableSecretResolver::new(pool.clone(), KEK, None),
        )));

        // The gate: refused before either source is consulted, and the
        // rejected value is not echoed.
        for bad in ["CLERK_SECRET_KEY", "postgres://u:opr_pw_gate@h/db", "tenant_db_opr_table_only", "TENANT_DB_"] {
            let e = r.resolve(bad).await.expect_err(bad);
            assert!(matches!(e, OperatorDsnError::NotATenantRef), "{role} {bad}: {e:?}");
            errors.push(format!("{e} {e:?}"));
        }
        // The environment wins over a table row under the same ref.
        assert_eq!(r.resolve(shadowed).await.unwrap(), env_dsn, "{role}");
        // Not in the environment: the encrypted row, decrypted under the KEK.
        assert_eq!(r.resolve(table_only).await.unwrap(), table_dsn, "{role}");
        // Nowhere: NotFound, never a silent empty string.
        let e = r.resolve("TENANT_DB_OPR_NOWHERE").await.expect_err("nowhere");
        assert!(matches!(e, OperatorDsnError::Secret(SecretError::NotFound(_))), "{role}: {e:?}");
        errors.push(format!("{e} {e:?}"));
        // Wrong KEK: fails closed, naming the ref and not the DSN.
        let e = r.resolve(undecryptable).await.expect_err("wrong KEK");
        assert!(matches!(e, OperatorDsnError::Secret(SecretError::Undecryptable(_))), "{role}: {e:?}");
        errors.push(format!("{e} {e:?}"));
    }
    // A control plane the resolver cannot read: a Lookup error, not NotFound,
    // and still no DSN.
    let closed = c.app.clone();
    closed.close().await;
    let r = OperatorDsnResolver::new(Arc::new(ChainSecretResolver::new(
        Arc::new(StaticMapResolver::from_pairs::<[(&str, &str); 0], _, _>([])),
        ControlTableSecretResolver::new(closed, KEK, None),
    )));
    let e = r.resolve(table_only).await.expect_err("closed pool");
    assert!(matches!(e, OperatorDsnError::Secret(SecretError::Lookup { .. })), "{e:?}");
    errors.push(format!("{e} {e:?}"));

    drop(_guard);
    let logged = String::from_utf8(log.0.lock().unwrap().clone()).unwrap();
    for m in markers {
        assert!(!logged.contains(m), "a DSN ({m}) reached the log");
        for e in &errors {
            assert!(!e.contains(m), "a DSN ({m}) reached an error: {e}");
        }
    }
    // The capture is live (sqlx logged the credential SELECT), so an empty
    // log cannot make the assertion above vacuous.
    assert!(logged.contains("tenant_db_credentials"), "the log capture recorded nothing");
}

#[tokio::test]
async fn migrate_all_records_schema_version_only_as_owner_or_lease_holder_and_skips_live_jobs() {
    let Some(base) = base_url() else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };
    let c = control(&base, "orgt_prov_schema_migrate").await;
    let (tenant_url, _tenant_pool) = tenant_db(&base, "orgt_prov_schema_migrate_t").await;
    let (live_url, _live_pool) = tenant_db(&base, "orgt_prov_schema_migrate_live_t").await;

    // An existing concierge tenant: active row inserted as the owner, exactly
    // as onboard_tenant.sh does, with NO job row.
    let org = "org_prov_migrate";
    let account_id = org_account(&c.owner, org).await;
    concierge_row(&c.owner, org, account_id, "prov-migrate", None).await;

    // A self-serve tenant mid-job: provisioning row + live job. The release
    // orchestrator must leave it alone.
    let org_live = "org_prov_migrate_live";
    let account_live = org_account(&c.owner, org_live).await;
    set_subscription(&c.owner, account_live, "active", Some("sub_prov_migrate_live")).await;
    let (live_slug, live_ref) = enqueue_job(&c.app, org_live, account_live).await;
    job_at_step(&c.owner, org_live, "insert_routing").await;
    credentials::store(&c.prov, &KEK, None, &live_ref, &live_url).await.unwrap();
    routing::provisioning_insert_routing(&c.prov, org_live, account_live, &live_slug, &live_ref, None)
        .await
        .unwrap();
    job_at_step(&c.owner, org_live, "migrate").await;

    let selected = routing::list_migratable(&c.app).await.unwrap();
    assert_eq!(
        selected.iter().map(|t| t.clerk_org_id.as_str()).collect::<Vec<_>>(),
        vec![org],
        "a provisioning row owned by a live job is not the release's to migrate"
    );

    let resolver: Arc<dyn SecretResolver> = Arc::new(ChainSecretResolver::new(
        Arc::new(StaticMapResolver::from_pairs([("TENANT_DB_PROV_MIGRATE", tenant_url.as_str())])),
        ControlTableSecretResolver::new(c.app.clone(), KEK, None),
    ));

    // S2: under the APP role, a live (concierge) tenant's version cannot be
    // recorded through the definer fallback: that is the release's
    // owner-role write. The run reports it FAILED, never a silent success.
    let report = migrate::migrate_all(&c.app, resolver.clone(), &MigrateOpts::default())
        .await
        .unwrap();
    assert_eq!(report.outcomes.len(), 1);
    assert!(
        matches!(&report.outcomes[0].result, TenantResult::Failed { error } if error.contains("schema_version was not recorded")),
        "got {:?}",
        report.outcomes[0].result
    );
    assert_eq!(routing::lookup_by_org(&c.app, org).await.unwrap().unwrap().schema_version, None);

    // The release's owner role records it.
    let report = migrate::migrate_all(&c.owner, resolver.clone(), &MigrateOpts::default())
        .await
        .unwrap();
    assert!(matches!(report.outcomes[0].result, TenantResult::Migrated { .. }), "{:?}", report.outcomes[0].result);
    let row = routing::lookup_by_org(&c.app, org).await.unwrap().unwrap();
    assert_eq!(row.schema_version, Some(migrate::head_version()), "the version LANDED, not warned");

    // The provisioning job's own in-process migrate: the provisioner pool,
    // under the job's lease. Without the lease (or as the app role) the
    // version is NOT recorded: the direct UPDATE is denied and there is no
    // fallback, so the run reports it failed.
    let only_live = MigrateOpts { only: Some(org_live.to_string()), jobs: 1, ..Default::default() };
    for (pool, who) in [(&c.prov, "provisioner without a lease"), (&c.app, "app")] {
        let report = migrate::migrate_all(pool, resolver.clone(), &only_live).await.unwrap();
        assert!(
            matches!(&report.outcomes[0].result, TenantResult::Failed { error } if error.contains("schema_version was not recorded")),
            "{who}: {:?}",
            report.outcomes[0].result
        );
    }
    assert_eq!(routing::lookup_by_org(&c.app, org_live).await.unwrap().unwrap().schema_version, None);
    let leased = MigrateOpts {
        provisioning_lease: Some(migrate::ProvisioningLease { owner: LEASE.into(), ttl: TTL }),
        ..only_live.clone()
    };
    let report = migrate::migrate_all(&c.prov, resolver.clone(), &leased).await.unwrap();
    assert!(matches!(report.outcomes[0].result, TenantResult::Migrated { .. }), "{:?}", report.outcomes[0].result);
    let row = routing::lookup_by_org(&c.app, org_live).await.unwrap().unwrap();
    assert_eq!(row.schema_version, Some(migrate::head_version()));
    // Convergent: a second run re-records the same value.
    let report = migrate::migrate_all(&c.prov, resolver, &leased).await.unwrap();
    assert!(matches!(report.outcomes[0].result, TenantResult::Migrated { .. }));
}

/// S1: a forged job row gets no routing row. The app role cannot write one
/// at all; a row written behind its back (here: by the owner) must still be
/// the self-serve shape, for an org that is not already routed, with a live
/// subscription.
#[tokio::test]
async fn a_forged_job_is_rejected_by_insert_routing() {
    let Some(base) = base_url() else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };
    let c = control(&base, "orgt_prov_schema_forged").await;

    // The app role holds no write on the job table.
    let victim = "org_prov_forged_victim";
    let victim_account = org_account(&c.owner, victim).await;
    let victim_slug = opaque_slug_for_account(victim_account);
    for (sql, what) in [
        ("INSERT INTO tenant_provisioning_jobs (clerk_org_id, account_id, slug, db_secret_ref) VALUES ($1, $2, $3, $4)", "INSERT"),
        ("UPDATE tenant_provisioning_jobs SET slug = $3, db_secret_ref = $4 WHERE clerk_org_id = $1 AND account_id = $2", "UPDATE"),
    ] {
        let e = sqlx::query(sql)
            .bind(victim)
            .bind(victim_account)
            .bind(&victim_slug)
            .bind(secret_ref_for_slug(&victim_slug))
            .execute(&c.app)
            .await
            .expect_err(what);
        assert_eq!(sqlstate(&e), "42501", "{what}");
    }

    // An existing (concierge) org: active routing row, no job. A forged job
    // for it must never repoint it.
    concierge_row(&c.owner, victim, victim_account, "acme-corp", Some(1)).await;
    set_subscription(&c.owner, victim_account, "active", Some("sub_forged_victim")).await;
    // Enqueue refuses it outright.
    let e = sqlx::query_scalar::<_, bool>("SELECT tenant_provisioning_enqueue($1, $2, NULL)")
        .bind(victim)
        .bind(victim_account)
        .fetch_one(&c.app)
        .await
        .expect_err("routed org");
    assert_eq!(sqlstate(&e), "23505");
    let forge = |org: &'static str, account: Uuid| {
        let owner = c.owner.clone();
        async move {
            let slug = opaque_slug_for_account(account);
            sqlx::query(
                "INSERT INTO tenant_provisioning_jobs \
                   (clerk_org_id, account_id, slug, db_secret_ref, step, database_create_requested_at, provider, provider_database_id) \
                 VALUES ($1, $2, $3, $4, 'insert_routing', now(), 'local', 'coralswarm_tenant_attacker')",
            )
            .bind(org)
            .bind(account)
            .bind(&slug)
            .bind(secret_ref_for_slug(&slug))
            .execute(&owner)
            .await
            .unwrap();
            put_credential(&owner, &secret_ref_for_slug(&slug), &credentials::encrypt_dsn(&KEK, &secret_ref_for_slug(&slug), "postgres://attacker@evil/x").unwrap()).await;
            slug
        }
    };
    let slug = forge(victim, victim_account).await;
    let e = routing::provisioning_insert_routing(&c.prov, victim, victim_account, &slug, &secret_ref_for_slug(&slug), None)
        .await
        .expect_err("already routed");
    assert_eq!(sqlstate(&e), "23505");
    let row = routing::lookup_by_org(&c.app, victim).await.unwrap().unwrap();
    assert_eq!((row.slug.as_str(), row.status.as_str()), ("acme-corp", "active"), "the live tenant is untouched");

    // A new org with no live subscription: refused, however well-formed.
    let unpaid = "org_prov_forged_unpaid";
    let unpaid_account = org_account(&c.owner, unpaid).await;
    let slug = forge(unpaid, unpaid_account).await;
    for (status, sub) in [(None, None), (Some("pending"), None), (Some("past_due"), Some("sub_x")), (Some("canceled"), Some("sub_x"))] {
        if let Some(status) = status {
            set_subscription(&c.owner, unpaid_account, status, sub).await;
        }
        let e = routing::provisioning_insert_routing(&c.prov, unpaid, unpaid_account, &slug, &secret_ref_for_slug(&slug), None)
            .await
            .expect_err("no live subscription");
        assert_eq!(sqlstate(&e), "23514", "{status:?}");
    }
    assert!(routing::lookup_by_org(&c.app, unpaid).await.unwrap().is_none());

    // S3: the table itself refuses a job whose slug/ref is not derived from
    // its account, so the routing definer never sees a chosen key.
    let other = "org_prov_forged_chosen";
    let other_account = org_account(&c.owner, other).await;
    for (slug, secret_ref) in [
        ("acme-corp".to_string(), "TENANT_DB_ACME_CORP".to_string()),
        (opaque_slug_for_account(other_account), "TENANT_DB_ACME_CORP".to_string()),
        (victim_slug.clone(), secret_ref_for_slug(&victim_slug)),
    ] {
        let e = sqlx::query("INSERT INTO tenant_provisioning_jobs (clerk_org_id, account_id, slug, db_secret_ref) VALUES ($1, $2, $3, $4)")
            .bind(other)
            .bind(other_account)
            .bind(&slug)
            .bind(&secret_ref)
            .execute(&c.owner)
            .await
            .expect_err("non-derived key");
        assert_eq!(sqlstate(&e), "23514", "{slug} / {secret_ref}");
    }
}

/// S3: both tables accept only `TENANT_DB_` refs.
#[tokio::test]
async fn bad_secret_refs_are_rejected_by_the_check_constraints() {
    let Some(base) = base_url() else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };
    let c = control(&base, "orgt_prov_schema_refcheck").await;
    for bad in ["CLERK_SECRET_KEY", "MASTER_KEK_HEX", "TENANT_DB_", "TENANT_DB_lower", "TENANT_DB_A-B", "XTENANT_DB_A"] {
        let e = sqlx::query("INSERT INTO tenant_db_credentials (secret_ref, dsn_encrypted) VALUES ($1, $2)")
            .bind(bad)
            .bind(vec![0u8; 40])
            .execute(&c.owner)
            .await
            .expect_err(bad);
        assert_eq!(sqlstate(&e), "23514", "{bad}");
    }
    put_credential(&c.owner, "TENANT_DB_ACME_CORP", &[0u8; 40]).await;
}

/// S7: an old build must not activate a tenant behind a tree that a newer
/// release has already rolled out to other tenants. Refused with a
/// retryable code (55000), not as a permanent failure.
#[tokio::test]
async fn activation_refuses_while_a_newer_tree_is_rolled_out() {
    let Some(base) = base_url() else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };
    let c = control(&base, "orgt_prov_schema_rolling").await;
    let org = "org_prov_rolling";
    let account_id = org_account(&c.owner, org).await;
    set_subscription(&c.owner, account_id, "active", Some("sub_prov_rolling")).await;
    let (slug, secret_ref) = enqueue_job(&c.app, org, account_id).await;
    job_at_step(&c.owner, org, "insert_routing").await;
    credentials::store(&c.prov, &KEK, None, &secret_ref, "postgres://f:s@localhost/rolling").await.unwrap();
    routing::provisioning_insert_routing(&c.prov, org, account_id, &slug, &secret_ref, None).await.unwrap();
    job_at_step(&c.owner, org, "migrate").await;
    record_version(&c.prov, org, 7).await.unwrap();
    job_at_step(&c.owner, org, "activate").await;

    // S7-a: only ACTIVE rows speak for the rolled-out tree. Another org's
    // job, still provisioning, whose row records a higher version (a newer
    // build's job, or a row nobody released) does not block activation.
    let pending = "org_prov_rolling_pending";
    let pending_account = org_account(&c.owner, pending).await;
    routing_row(&c.owner, pending, pending_account, "rolling-pending", "provisioning", Some(9)).await;
    let mut tx = c.owner.begin().await.unwrap();
    assert!(
        sqlx::query_scalar::<_, bool>("SELECT tenant_provisioning_activate($1, 7)")
            .bind(org)
            .fetch_one(&mut *tx)
            .await
            .unwrap(),
        "a provisioning row's version is not a release's"
    );
    tx.rollback().await.unwrap();

    // A concierge tenant the new release has already migrated to v8.
    let other = "org_prov_rolling_other";
    let other_account = org_account(&c.owner, other).await;
    concierge_row(&c.owner, other, other_account, "rolling-other", Some(8)).await;

    let e = routing::provisioning_activate(&c.prov, org, 7).await.expect_err("behind the rolled-out tree");
    assert_eq!(sqlstate(&e), "55000");
    assert_eq!(routing::lookup_by_org(&c.app, org).await.unwrap().unwrap().status, "provisioning");

    // The new build (expecting v8) migrates the row up and activates it.
    job_at_step(&c.owner, org, "migrate").await;
    record_version(&c.prov, org, 8).await.unwrap();
    job_at_step(&c.owner, org, "activate").await;
    assert!(routing::provisioning_activate(&c.prov, org, 8).await.unwrap());
}

#[tokio::test]
async fn provision_org_dek_if_absent_mints_once_and_never_rewrites() {
    let Some(base) = base_url() else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };
    let (_url, tenant) = tenant_db(&base, "orgt_prov_schema_dek").await;
    sqlx::migrate!("./migrations_tenant").run(&tenant).await.expect("tenant tree");
    let account_id = Uuid::new_v4();

    assert_eq!(
        provision_org_dek_if_absent(&tenant, account_id, &KEK).await.unwrap(),
        DekProvision::Provisioned
    );
    let first: Vec<u8> = sqlx::query_scalar("SELECT dek_wrapped FROM account_keys WHERE account_id = $1")
        .bind(account_id)
        .fetch_one(&tenant)
        .await
        .unwrap();
    let dek = coralswarm_core::kek::unwrap_dek(&KEK, &first).expect("wrapped under the KEK we passed");
    assert_eq!(dek.len(), 32);

    assert_eq!(
        provision_org_dek_if_absent(&tenant, account_id, &KEK).await.unwrap(),
        DekProvision::AlreadyPresent
    );
    let second: Vec<u8> = sqlx::query_scalar("SELECT dek_wrapped FROM account_keys WHERE account_id = $1")
        .bind(account_id)
        .fetch_one(&tenant)
        .await
        .unwrap();
    assert_eq!(first, second, "an existing DEK is left byte-identical");
}
