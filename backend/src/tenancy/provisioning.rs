//! The tenant provisioning job (self-serve org signup PR A1b; audit rows
//! 4a-4i). Automates `scripts/onboard_tenant.sh` as a durable, resumable
//! state machine persisted in `tenant_provisioning_jobs` (migration 0381).
//!
//! # Data shape
//!
//! One row per org. `step` is the step the job runs NEXT; `status` is the
//! job's lifecycle (`provisioning` → `ready` | `failed`). Every step is
//! idempotent: re-running a completed step converges on the same state
//! (finds the database it already created, sees the credential it already
//! stored, `ON CONFLICT DO NOTHING`s the fingerprint, …). A crash between a
//! step's side effect and the record of that step is therefore safe: the
//! next run redoes the step and finds the effect already in place.
//!
//! | step              | effect                                                                       | records                       |
//! |-------------------|------------------------------------------------------------------------------|-------------------------------|
//! | `verify_org`      | not a personal org; org `accounts` row exists; subscription live (`stripe_subscriptions`, re-checked before `create_database` creates); Clerk org exists and carries the self-serve marker; no live routing row | — |
//! | `create_database` | find-by-name or create the provider database (a found one is refused unless this job asked to create it); store the KEK-encrypted DSN | `database_create_requested_at` (before create), `provider`, `provider_database_id`, `credential_stored_at` |
//! | `insert_routing`  | routing row as `provisioning` (definer fn; credential must already exist)     | `routing_inserted_at`         |
//! | `migrate`         | apply the tenant tree in-process; `schema_version` written and read back      | —                             |
//! | `contract`        | apply the P2 identity contract (`docs/implementation/p2-identity-contract.sql`) on a direct owner session; `lifecycle_ready_tx` read back | — |
//! | `fingerprint`     | `tenant_meta.org_id` = this org, asserted                                     | —                             |
//! | `provision_dek`   | pool via the registry (fingerprint re-verified, org ocean ensured); DEK if absent | —                          |
//! | `activate`        | `provisioning → active` + job `ready`, atomically, iff `schema_version` is at head | `ready_at`               |
//! | `cleanup`         | delete routing row → credential (always: the delete is idempotent) → database (only one the provider names `database_name(slug)`, found by id or — after a lost create response — by name, and not claimed by another org's fingerprint), reverse creation order; job `failed` | `failed_at`, `clerk_org_cleanup = pending` |
//!
//! Order matters in two places. The credential is stored before the routing
//! row exists (so a release's `migrate_tenants` can always resolve any row
//! it sees), and the routing row is deleted before the credential (so an
//! orphan row never outlives the credential). The provider id is recorded
//! the moment it is known, before anything connects to the database.
//!
//! # Writes
//!
//! The serving role holds SELECT only on `tenant_provisioning_jobs`. Every
//! job write is one of migration 0381's narrow SECURITY DEFINER functions
//! (`tenant_provisioning_enqueue`, `_claim`, `_set_step`, `_defer`, …), which
//! derive the slug and ref themselves, only move a job along its forward
//! path, and record a provider database once. The routing definers trust
//! nothing on the row they cannot re-derive.
//!
//! Two roles. The serving role (`coralswarm_app`, [`Deps::control`]) may
//! only enqueue, cancel, [`retry`] and read. Everything that advances a job
//! or writes routing or credentials is granted to `coralswarm_provisioner`
//! only, which the worker reaches through its own pool
//! ([`Deps::provisioner_control`], [`PROVISIONER_DATABASE_URL`]). The app
//! role can satisfy every precondition `insert_routing` checks (it writes
//! `stripe_subscriptions`), so it must not be able to call it.
//!
//! # Retries, failure, alerting
//!
//! A transient step error (network, a database still starting) schedules a
//! retry with exponential backoff, bounded by [`RetryPolicy::max_attempts`].
//! A worker panic mid-run counts as one such attempt.
//! Waiting for the org's subscription to go live is NOT such an error: an
//! ACH / SEPA payment can take days, so that wait polls on its own interval
//! ([`RetryPolicy::subscription_poll`]) up to its own deadline
//! ([`RetryPolicy::subscription_wait`]) and never spends the attempts budget.
//! Nor is `activate` refusing because a newer tenant tree is already rolled
//! out (this build is behind head): the job parks until a newer build's
//! worker takes it over ([`RetryPolicy::version_poll`]), alerts once the
//! park outlasts [`RetryPolicy::version_wait`], and stays parked. A park
//! never leads to cleanup, so it never deletes the customer's database.
//! A permanent error (personal org, slug mismatch, fingerprint mismatch,
//! cancel) or an exhausted budget moves the job to `cleanup`, which deletes
//! exactly what this job recorded creating (a database only when no other
//! org's fingerprint claims it) and then marks the job `failed`
//! with a human-readable `last_error` and one `error!` carrying
//! `connector_health = true` — the repo's alertable shape. A failed job is
//! never silently absent: the row, its reason, and the alert all remain.
//! `clerk_org_cleanup = 'pending'` is the hook PR A2 (which creates the Clerk
//! org) uses to delete or mark the orphan: `tenancy::org_selfserve_cleanup`
//! claims it (`pending → deleting`, control 0384), re-checks the subscription
//! at Stripe, and then requeues, holds, or deletes ([`ClerkOrgCleanup`]).
//! This module never calls Clerk's write API.
//!
//! # Lease
//!
//! A job is claimed by writing `lease_owner`/`lease_expires_at` under
//! `FOR UPDATE SKIP LOCKED`; every subsequent write is conditioned on the
//! claimant still holding the lease, so two backend machines never advance
//! the same job, and a machine that dies mid-step loses the lease on expiry.
//! Every write that keeps the lease also renews it (`now() + lease_ttl`), so
//! a job that takes longer overall than one TTL is never re-claimed while it
//! is still making progress.
//!
//! # Flag
//!
//! `ORG_SELFSERVE_SIGNUP_ENABLED` (unset = off) gates [`enqueue`] and the
//! worker. With it unset nothing can be queued and the worker idles;
//! existing tenants are untouched either way (the job never writes a row it
//! did not create). This is a different question from the pre-existing
//! `ORG_SELFSERVE_DISABLED` — see [`selfserve_signup_enabled`].

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::postgres::PgPoolOptions;
use sqlx::{Connection, PgConnection, PgPool};
use uuid::Uuid;

use super::credentials::{self, CredentialError, StoreOutcome};
use super::dek::provision_org_dek_if_absent;
use super::migrate::{self, MigrateOpts, TenantResult};
use super::provisioner::{DeleteOutcome, ProvisionerError, TenantDbProvisioner};
use super::registry::TenantRegistry;
use super::routing;
use super::secrets::SecretResolver;
use crate::clerk_admin::{ClerkAdminClient, ClerkAdminError};

// ── Flag ──────────────────────────────────────────────────────────────────────

/// The prod gate the founder flips. Unset = off.
pub const SELFSERVE_SIGNUP_FLAG: &str = "ORG_SELFSERVE_SIGNUP_ENABLED";

/// The control-plane DSN the provisioning worker writes through: a login
/// that is a member of `coralswarm_provisioner` (control 0381). Only the
/// worker builds a pool from it. It is a Fly secret, and Fly secrets are
/// app-wide: the `http` machines (another process group of the same app)
/// carry it in their environment too, so the split protects against the
/// serving role being misused through SQL (injection, a confused deputy),
/// not against code execution on an `http` machine. The serving role cannot
/// call the definers that advance a job or write routing, so without this the
/// worker has no way to provision and refuses to start when the flag is on.
pub const PROVISIONER_DATABASE_URL: &str = "PROVISIONER_DATABASE_URL";

/// Why the worker's provisioner pool is unfit to run ([`provisioner_self_check`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProvisionerSelfCheckError {
    /// The login is `coralswarm_app`, or a member of it: the split between
    /// the serving role and the worker would be gone (the serving role could
    /// do what the worker does, and the worker everything the app can).
    ServingRole { current_user: String },
    /// The login cannot call `tenant_provisioning_claim`: it is not a
    /// `coralswarm_provisioner` member, so no job could ever be claimed.
    MissingClaimExecute { current_user: String },
    /// The login holds UPDATE on `tenant_databases` (directly, through a
    /// role or PUBLIC): it is an owner-level DSN, not the provisioner login.
    /// The worker must write routing only through the definers.
    OwnerLevelLogin { current_user: String },
    /// The `coralswarm_provisioner` group role itself carries an attribute it
    /// must not (LOGIN, SUPERUSER, CREATEDB, CREATEROLE, REPLICATION or
    /// BYPASSRLS), e.g. a later `ALTER ROLE coralswarm_provisioner LOGIN` in
    /// prod. Control 0381 asserts this only once, when it runs, and the
    /// deploy guard only sees a fresh CI database, so this boot check is the
    /// one that sees the live role. `attributes` is sorted.
    GroupRoleAttributes { role: String, attributes: Vec<String> },
    /// The check itself could not run (control plane unreachable). Not a
    /// verdict: try again.
    Unreachable(String),
}

impl std::fmt::Display for ProvisionerSelfCheckError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ServingRole { current_user } => write!(
                f,
                "{PROVISIONER_DATABASE_URL} logs in as {current_user}, which is coralswarm_app or a member of it; \
                 it must be a separate login granted coralswarm_provisioner only"
            ),
            Self::MissingClaimExecute { current_user } => write!(
                f,
                "{PROVISIONER_DATABASE_URL} logs in as {current_user}, which cannot EXECUTE \
                 tenant_provisioning_claim; grant it coralswarm_provisioner"
            ),
            Self::OwnerLevelLogin { current_user } => write!(
                f,
                "{PROVISIONER_DATABASE_URL} logs in as {current_user}, which can UPDATE tenant_databases \
                 directly (an owner-level DSN); it must be a separate login granted coralswarm_provisioner only"
            ),
            Self::GroupRoleAttributes { role, attributes } => write!(
                f,
                "the {role} group role has attributes it must not hold ({}); it must be NOLOGIN NOSUPERUSER \
                 NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS (control 0381)",
                attributes.join(", ")
            ),
            Self::Unreachable(e) => write!(f, "provisioner self-check could not run: {e}"),
        }
    }
}

/// Boot self-check of the worker's provisioner pool, run against the live
/// control plane every time the worker starts: its `current_user` must not
/// be `coralswarm_app` or a member of it (a superuser is a member of every
/// role, so it is refused too), must not hold UPDATE on `tenant_databases`
/// (an owner-level DSN), and must hold EXECUTE on
/// `tenant_provisioning_claim`; and the `coralswarm_provisioner` group role
/// itself must hold none of LOGIN, SUPERUSER, CREATEDB, CREATEROLE,
/// REPLICATION or BYPASSRLS (a missing group role has no attributes; the
/// claim check then fails). `Ok` carries the login name. A missing
/// `coralswarm_app` role (a dev cluster) cannot be a membership.
pub async fn provisioner_self_check(pool: &PgPool) -> Result<String, ProvisionerSelfCheckError> {
    provisioner_self_check_with_group(pool, PROVISIONER_GROUP_ROLE).await
}

/// The group role the provisioner login is granted (control 0381).
pub const PROVISIONER_GROUP_ROLE: &str = "coralswarm_provisioner";

/// [`provisioner_self_check`] with the group role whose attributes are
/// checked named explicitly. Only the integration suite passes anything but
/// [`PROVISIONER_GROUP_ROLE`]: the real role is cluster-wide and shared by
/// concurrent scratch databases, so a test plants a bad attribute on a
/// throwaway role instead.
#[doc(hidden)]
pub async fn provisioner_self_check_with_group(
    pool: &PgPool,
    group_role: &str,
) -> Result<String, ProvisionerSelfCheckError> {
    let (current_user, serving, owner_level, can_claim, bad_attributes): (String, bool, bool, bool, Vec<String>) =
        sqlx::query_as(
            "SELECT current_user::text, \
                current_user = 'coralswarm_app' \
                  OR CASE WHEN pg_catalog.to_regrole('coralswarm_app') IS NULL THEN false \
                          ELSE pg_catalog.pg_has_role(current_user, 'coralswarm_app', 'MEMBER') END, \
                CASE WHEN pg_catalog.to_regclass('public.tenant_databases') IS NULL THEN false \
                     ELSE pg_catalog.has_table_privilege(pg_catalog.to_regclass('public.tenant_databases'), 'UPDATE') END, \
                CASE WHEN pg_catalog.to_regprocedure('public.tenant_provisioning_claim(text,interval,text)') IS NULL THEN false \
                     ELSE pg_catalog.has_function_privilege( \
                            pg_catalog.to_regprocedure('public.tenant_provisioning_claim(text,interval,text)'), 'EXECUTE') END, \
                ARRAY(SELECT a.attr FROM pg_catalog.pg_roles r \
                        CROSS JOIN LATERAL (VALUES ('bypassrls', r.rolbypassrls), ('createdb', r.rolcreatedb), \
                                                   ('createrole', r.rolcreaterole), ('login', r.rolcanlogin), \
                                                   ('replication', r.rolreplication), ('superuser', r.rolsuper)) a(attr, held) \
                       WHERE r.rolname = $1 AND a.held ORDER BY a.attr)",
        )
        .bind(group_role)
        .fetch_one(pool)
        .await
        .map_err(|e| ProvisionerSelfCheckError::Unreachable(e.to_string()))?;
    if serving {
        return Err(ProvisionerSelfCheckError::ServingRole { current_user });
    }
    if !bad_attributes.is_empty() {
        return Err(ProvisionerSelfCheckError::GroupRoleAttributes {
            role: group_role.to_string(),
            attributes: bad_attributes,
        });
    }
    if owner_level {
        return Err(ProvisionerSelfCheckError::OwnerLevelLogin { current_user });
    }
    if !can_claim {
        return Err(ProvisionerSelfCheckError::MissingClaimExecute { current_user });
    }
    Ok(current_user)
}

/// `ORG_SELFSERVE_SIGNUP_ENABLED` is `1`/`true`.
///
/// Relationship to the older `ORG_SELFSERVE_DISABLED`
/// (`tenancy::runtime::TenantRuntime::org_selfserve_disabled`): that flag is
/// a *backstop against stray org claims* — it stops the Clerk webhook and a
/// first-sight JWT from auto-creating a control `accounts` row for an org
/// nobody is onboarding. It is not the inverse of this one and the two
/// compose: self-serve signup creates the `accounts` row itself, server-side,
/// before enqueueing (a precondition `verify_org` checks), so it works
/// identically whether `ORG_SELFSERVE_DISABLED` is set or not. Neither flag
/// is read by the other's code path; their names predate each other and are
/// kept rather than renamed in a deployed system.
pub fn selfserve_signup_enabled() -> bool {
    std::env::var(SELFSERVE_SIGNUP_FLAG)
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

/// The `private_metadata` key `POST /v1/orgs` (PR A2) stamps on an org it
/// creates. `verify_org` requires it to be `true`: an org that did not come
/// through the self-serve path (a concierge org, a personal org, a stray
/// Clerk-dashboard org) is never provisioned by this job.
pub const SELFSERVE_ORG_MARKER: &str = "coralswarm_selfserve";

// ── Step / status vocabulary ──────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Step {
    VerifyOrg,
    CreateDatabase,
    InsertRouting,
    Migrate,
    Contract,
    Fingerprint,
    ProvisionDek,
    Activate,
    Done,
    Cleanup,
}

impl Step {
    pub fn as_str(self) -> &'static str {
        match self {
            Step::VerifyOrg => "verify_org",
            Step::CreateDatabase => "create_database",
            Step::InsertRouting => "insert_routing",
            Step::Migrate => "migrate",
            Step::Contract => "contract",
            Step::Fingerprint => "fingerprint",
            Step::ProvisionDek => "provision_dek",
            Step::Activate => "activate",
            Step::Done => "done",
            Step::Cleanup => "cleanup",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "verify_org" => Step::VerifyOrg,
            "create_database" => Step::CreateDatabase,
            "insert_routing" => Step::InsertRouting,
            "migrate" => Step::Migrate,
            "contract" => Step::Contract,
            "fingerprint" => Step::Fingerprint,
            "provision_dek" => Step::ProvisionDek,
            "activate" => Step::Activate,
            "done" => Step::Done,
            "cleanup" => Step::Cleanup,
            _ => return None,
        })
    }

    /// The forward path, in order. `Done` and `Cleanup` are terminal/side.
    pub const FORWARD: [Step; 8] = [
        Step::VerifyOrg,
        Step::CreateDatabase,
        Step::InsertRouting,
        Step::Migrate,
        Step::Contract,
        Step::Fingerprint,
        Step::ProvisionDek,
        Step::Activate,
    ];

}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobStatus {
    Provisioning,
    Ready,
    Failed,
}

impl JobStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            JobStatus::Provisioning => "provisioning",
            JobStatus::Ready => "ready",
            JobStatus::Failed => "failed",
        }
    }
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "provisioning" => JobStatus::Provisioning,
            "ready" => JobStatus::Ready,
            "failed" => JobStatus::Failed,
            _ => return None,
        })
    }
}

/// One `tenant_provisioning_jobs` row.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct Job {
    pub clerk_org_id: String,
    pub account_id: Uuid,
    pub slug: String,
    pub db_secret_ref: String,
    pub region: Option<String>,
    pub step: String,
    pub status: String,
    pub attempts: i32,
    pub last_error: Option<String>,
    pub next_attempt_at: DateTime<Utc>,
    pub lease_owner: Option<String>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub provider: Option<String>,
    pub provider_database_id: Option<String>,
    pub database_create_requested_at: Option<DateTime<Utc>>,
    pub credential_stored_at: Option<DateTime<Utc>>,
    pub routing_inserted_at: Option<DateTime<Utc>>,
    pub cancel_requested_at: Option<DateTime<Utc>>,
    pub subscription_wait_started_at: Option<DateTime<Utc>>,
    pub version_wait_started_at: Option<DateTime<Utc>>,
    pub clerk_org_cleanup: String,
    pub failed_at: Option<DateTime<Utc>>,
    pub ready_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl Job {
    pub fn step(&self) -> Option<Step> {
        Step::parse(&self.step)
    }
    pub fn status(&self) -> Option<JobStatus> {
        JobStatus::parse(&self.status)
    }
}

const JOB_COLS: &str = "clerk_org_id, account_id, slug, db_secret_ref, region, step, status, attempts, \
     last_error, next_attempt_at, lease_owner, lease_expires_at, provider, provider_database_id, \
     database_create_requested_at, credential_stored_at, routing_inserted_at, cancel_requested_at, \
     subscription_wait_started_at, version_wait_started_at, clerk_org_cleanup, failed_at, \
     ready_at, created_at, updated_at";

/// Derive the secret ref from a slug exactly as `orgt_secret_ref` does
/// (`TENANT_DB_` + uppercase, `-` → `_`), so an operator-set Fly secret of
/// the same name is what the env-first resolver would find.
pub fn secret_ref_for_slug(slug: &str) -> String {
    let mut s = String::from("TENANT_DB_");
    for c in slug.chars() {
        s.push(if c == '-' { '_' } else { c.to_ascii_uppercase() });
    }
    s
}


// ── Enqueue / cancel / read ───────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct EnqueueRequest {
    pub clerk_org_id: String,
    pub account_id: Uuid,
    /// Internal routing key (`tenant_databases.slug`); never shown to a user
    /// and never a hostname (every org is served from app.coralswarm.example).
    /// Self-serve callers use [`EnqueueRequest::selfserve`], which derives it.
    pub slug: String,
    pub region: Option<String>,
}

impl EnqueueRequest {
    /// The request the Stripe `checkout.session.completed` handler builds for
    /// a self-serve org: the slug is [`opaque_slug_for_account`], so it is
    /// deterministic (a redelivered event asks for the same job) and never
    /// collides with a human-chosen concierge slug.
    pub fn selfserve(clerk_org_id: impl Into<String>, account_id: Uuid) -> Self {
        Self {
            clerk_org_id: clerk_org_id.into(),
            account_id,
            slug: opaque_slug_for_account(account_id),
            region: None,
        }
    }
}

/// `o-` + the first 16 hex digits of the org's control account id: an
/// opaque, server-generated routing key. Deterministic per account, so a
/// retried enqueue converges on the same job, and shaped to pass
/// [`super::gate::is_valid_slug`] and the job table's slug CHECK.
pub fn opaque_slug_for_account(account_id: Uuid) -> String {
    let hex = account_id.simple().to_string();
    format!("o-{}", &hex[..16])
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EnqueueOutcome {
    Enqueued,
    /// A job for this org already exists; nothing was written. A redelivered
    /// webhook lands here.
    AlreadyExists { status: String, step: String },
}

#[derive(Debug, thiserror::Error)]
pub enum EnqueueError {
    #[error("self-serve org signup is disabled ({SELFSERVE_SIGNUP_FLAG} unset)")]
    Disabled,
    #[error("org {0} is a personal organization (personal_orgs); it gets no tenant database")]
    PersonalOrg(String),
    #[error("org {0} has no control accounts row bound to the given account")]
    NoOrgAccount(String),
    #[error("org {org} already has a routing row in status {status}")]
    AlreadyRouted { org: String, status: String },
    /// The slug is not the self-serve routing key for this account
    /// ([`opaque_slug_for_account`]); [`EnqueueRequest::selfserve`] builds it.
    #[error("slug {0:?} is not this account's self-serve routing key")]
    InvalidSlug(String),
    /// A uniqueness conflict the pre-checks could not see (another org's job
    /// already holds this account, or the derived routing key is taken). The
    /// insert ran under a SAVEPOINT, so the caller's transaction is intact.
    #[error("org {org} conflicts with an existing tenant or job: {detail}")]
    Conflict { org: String, detail: String },
    #[error("database error: {0}")]
    Db(#[from] sqlx::Error),
}

/// Queue a provisioning job on its own transaction. See [`enqueue_in`].
pub async fn enqueue(control: &PgPool, req: EnqueueRequest) -> Result<EnqueueOutcome, EnqueueError> {
    if !selfserve_signup_enabled() {
        return Err(EnqueueError::Disabled);
    }
    let mut tx = control.begin().await?;
    let out = enqueue_in(&mut tx, req).await?;
    tx.commit().await?;
    Ok(out)
}

/// Queue a provisioning job on the caller's connection or transaction.
///
/// The entry point for the Stripe `checkout.session.completed` handler (the
/// trigger since the founder's card-up-front decision): call it INSIDE the
/// webhook's account-scoped transaction, after the event claim and the
/// subscription mirror write, with [`EnqueueRequest::selfserve`]. The job
/// row then commits or rolls back with the event, so "paid but never
/// enqueued" cannot happen, and a redelivered event is a no-op
/// ([`EnqueueOutcome::AlreadyExists`]; the job's primary key is the org id).
///
/// Never the Clerk `organization.created` webhook (audit 4i: it fires for
/// personal orgs too). Enqueueing alone creates nothing billable: the worker
/// re-checks, at run time and again immediately before asking the provider
/// for a database, that the account's subscription is live
/// ([`subscription_state`]).
///
/// Refuses: flag off; personal org; missing/mismatched org `accounts` row;
/// an existing routing row; a slug that is not [`opaque_slug_for_account`]
/// (only self-serve jobs exist). Idempotent on the org id. Only touches
/// tables without RLS (`personal_orgs`, `accounts`, `tenant_databases`,
/// `tenant_provisioning_jobs`), so it works on a plain or an account-scoped
/// connection alike. The row itself is written by the
/// `tenant_provisioning_enqueue` definer (the app role has no INSERT on the
/// job table), which derives the slug and ref again on its own.
///
/// # In the webhook's transaction
///
/// The definer call runs under a SAVEPOINT (sqlx nests `begin` on a
/// connection already in a transaction). A uniqueness violation the
/// pre-checks raced past comes back as [`EnqueueError::Conflict`] and the
/// caller's transaction is still usable; it is never left aborted.
///
/// For the Stripe handler, [`EnqueueError::Disabled`] (the flag is off) and
/// [`EnqueueError::AlreadyRouted`] (the org already has a tenant) are
/// NON-FATAL: the subscription event itself must still commit. Log them and
/// carry on. `PersonalOrg`, `NoOrgAccount`, `InvalidSlug` and `Conflict` are
/// bugs or data problems worth an alert, but are equally no reason to fail
/// the billing event; only [`EnqueueError::Db`] means the connection is in
/// trouble.
pub async fn enqueue_in(conn: &mut PgConnection, req: EnqueueRequest) -> Result<EnqueueOutcome, EnqueueError> {
    if !selfserve_signup_enabled() {
        return Err(EnqueueError::Disabled);
    }
    if !super::gate::is_valid_slug(&req.slug) || req.slug != opaque_slug_for_account(req.account_id) {
        return Err(EnqueueError::InvalidSlug(req.slug));
    }
    let personal: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM personal_orgs WHERE clerk_org_id = $1)")
        .bind(&req.clerk_org_id)
        .fetch_one(&mut *conn)
        .await?;
    if personal {
        return Err(EnqueueError::PersonalOrg(req.clerk_org_id));
    }
    let has_account: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM accounts WHERE id = $1 AND clerk_org_id = $2 AND is_org_account)",
    )
    .bind(req.account_id)
    .bind(&req.clerk_org_id)
    .fetch_one(&mut *conn)
    .await?;
    if !has_account {
        return Err(EnqueueError::NoOrgAccount(req.clerk_org_id));
    }
    let routed: Option<String> = sqlx::query_scalar("SELECT status FROM tenant_databases WHERE clerk_org_id = $1")
        .bind(&req.clerk_org_id)
        .fetch_optional(&mut *conn)
        .await?;
    if let Some(status) = routed {
        return Err(EnqueueError::AlreadyRouted { org: req.clerk_org_id, status });
    }
    let mut savepoint = Connection::begin(&mut *conn).await?;
    let inserted = sqlx::query_scalar::<_, bool>("SELECT tenant_provisioning_enqueue($1, $2, $3)")
        .bind(&req.clerk_org_id)
        .bind(req.account_id)
        .bind(&req.region)
        .fetch_one(&mut *savepoint)
        .await;
    let inserted = match inserted {
        Ok(inserted) => {
            savepoint.commit().await?;
            inserted
        }
        Err(e) => {
            savepoint.rollback().await?;
            let detail = e.as_database_error().map(|d| d.message().to_string()).unwrap_or_default();
            return Err(match e.as_database_error().and_then(|d| d.code()).as_deref() {
                Some("23505") => EnqueueError::Conflict { org: req.clerk_org_id, detail },
                Some("23503") => EnqueueError::NoOrgAccount(req.clerk_org_id),
                Some("23514") => EnqueueError::PersonalOrg(req.clerk_org_id),
                _ => EnqueueError::Db(e),
            });
        }
    };
    if inserted {
        tracing::info!(org = %req.clerk_org_id, slug = %req.slug, "tenant provisioning job enqueued");
        return Ok(EnqueueOutcome::Enqueued);
    }
    let (status, step): (String, String) =
        sqlx::query_as("SELECT status, step FROM tenant_provisioning_jobs WHERE clerk_org_id = $1")
            .bind(&req.clerk_org_id)
            .fetch_one(&mut *conn)
            .await?;
    Ok(EnqueueOutcome::AlreadyExists { status, step })
}

// ── Subscription re-check ─────────────────────────────────────────────────────

/// What `stripe_subscriptions` says about an account, as far as provisioning
/// cares. Stripe's `trialing` is stored as `active` (`map_subscription_status`),
/// so a card-up-front trial is [`SubscriptionState::Live`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubscriptionState {
    /// `active` with a Stripe subscription id.
    Live,
    /// No row, a bare customer mapping (`pending`, or no subscription id), or
    /// `past_due`: not payable right now, but it may become so.
    NotLive,
    /// `canceled`: the org will not be paid for; never create its database.
    Canceled,
}

/// Read the account's subscription state. `stripe_subscriptions` has RLS
/// (`account_isolation`, 0162), so the read runs in its own
/// `begin_account_scoped` transaction; under `coralswarm_app` an unscoped read
/// would see nothing and refuse every paid org.
pub async fn subscription_state(control: &PgPool, account_id: Uuid) -> Result<SubscriptionState, sqlx::Error> {
    let mut tx = super::scope::begin_account_scoped(control, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(format!("begin_account_scoped: {e}")))?;
    let row: Option<(Option<String>, String)> =
        sqlx::query_as("SELECT stripe_subscription_id, status FROM stripe_subscriptions WHERE account_id = $1")
            .bind(account_id)
            .fetch_optional(&mut *tx)
            .await?;
    tx.commit().await?;
    Ok(match row {
        Some((Some(_), status)) if status == "active" => SubscriptionState::Live,
        Some((_, status)) if status == "canceled" => SubscriptionState::Canceled,
        _ => SubscriptionState::NotLive,
    })
}

/// [`subscription_state`] as a step gate: live passes; not-yet-live waits
/// (its own poll and deadline, [`RetryPolicy::subscription_wait`], not the
/// attempts budget); canceled is terminal.
async fn require_live_subscription(deps: &Deps, job: &Job) -> Result<(), StepError> {
    match subscription_state(&deps.control, job.account_id)
        .await
        .map_err(|e| StepError::Transient(format!("stripe_subscriptions: {e}")))?
    {
        SubscriptionState::Live => Ok(()),
        SubscriptionState::NotLive => Err(StepError::AwaitingSubscription(
            "the org's subscription is not live yet (no completed Checkout, or past due); \
             no database is created until it is"
                .into(),
        )),
        SubscriptionState::Canceled => Err(StepError::Permanent(
            "the org's subscription is canceled; no database is created".into(),
        )),
    }
}

/// Ask the worker to abandon a live job: its next pass runs cleanup and
/// marks it `failed` with reason `cancelled`. `Ok(false)` when there is no
/// live job to cancel.
pub async fn cancel(control: &PgPool, clerk_org_id: &str) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar("SELECT tenant_provisioning_cancel($1)")
        .bind(clerk_org_id)
        .fetch_one(control)
        .await
}

pub async fn get(control: &PgPool, clerk_org_id: &str) -> Result<Option<Job>, sqlx::Error> {
    let q = format!("SELECT {JOB_COLS} FROM tenant_provisioning_jobs WHERE clerk_org_id = $1");
    sqlx::query_as::<_, Job>(&q)
        .bind(clerk_org_id)
        .fetch_optional(crate::tenancy::scope::request_read(control))
        .await
}

/// Jobs that failed terminally and whose Clerk org PR A2 still has to deal
/// with (`clerk_org_cleanup = 'pending'`).
pub async fn list_pending_clerk_org_cleanup(control: &PgPool) -> Result<Vec<Job>, sqlx::Error> {
    let q = format!(
        "SELECT {JOB_COLS} FROM tenant_provisioning_jobs \
         WHERE status = 'failed' AND clerk_org_cleanup = 'pending' ORDER BY failed_at"
    );
    sqlx::query_as::<_, Job>(&q).fetch_all(control).await
}

/// `tenant_provisioning_jobs.clerk_org_cleanup` (control 0381; `deleting` and
/// `held` added by 0384). The sweep in `tenancy::org_selfserve_cleanup` owns
/// what happens out of `pending`; every edge below is one 0384 definer.
///
/// ```text
/// none ──(job fails)──► pending ──claim──► deleting ──done──► done
///                          ▲                  │
///                          └──── release ─────┤
///                                             ├──► held   (paid; auto-requeues spent)
///                                             └──► none   (requeue: paid after all)
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ClerkOrgCleanup {
    None,
    Pending,
    Deleting,
    Held,
    Done,
}

impl ClerkOrgCleanup {
    pub const ALL: [ClerkOrgCleanup; 5] = [
        ClerkOrgCleanup::None,
        ClerkOrgCleanup::Pending,
        ClerkOrgCleanup::Deleting,
        ClerkOrgCleanup::Held,
        ClerkOrgCleanup::Done,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            ClerkOrgCleanup::None => "none",
            ClerkOrgCleanup::Pending => "pending",
            ClerkOrgCleanup::Deleting => "deleting",
            ClerkOrgCleanup::Held => "held",
            ClerkOrgCleanup::Done => "done",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|c| c.as_str() == s)
    }

    /// The transition table the 0381/0384 definers implement. `retry` (the
    /// owner's) is `pending | held → none`; it is refused from `deleting`
    /// and `done`.
    pub const fn can_become(self, to: ClerkOrgCleanup) -> bool {
        use ClerkOrgCleanup::*;
        matches!(
            (self, to),
            (None, Pending)
                | (Pending | Held, None)
                | (Pending, Deleting)
                // A stale claim taken over by another pass.
                | (Deleting, Deleting)
                | (Deleting, Pending | Held | None | Done)
        )
    }

    /// Whether the owner's retry is allowed from here.
    pub const fn owner_may_retry(self) -> bool {
        matches!(self, ClerkOrgCleanup::Pending | ClerkOrgCleanup::Held | ClerkOrgCleanup::None)
    }
}

/// How the sweep leaves `deleting` without deleting the Clerk org.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CleanupRelease {
    /// Give the claim back; the next pass decides again.
    Pending,
    /// Paid, but the automatic requeue budget is spent: an operator (or the
    /// owner's retry) decides.
    Held,
    /// Paid after all: run the job again (counted).
    Requeue,
}

impl CleanupRelease {
    pub const fn as_str(self) -> &'static str {
        match self {
            CleanupRelease::Pending => "pending",
            CleanupRelease::Held => "held",
            CleanupRelease::Requeue => "requeue",
        }
    }

    pub const fn target(self) -> ClerkOrgCleanup {
        match self {
            CleanupRelease::Pending => ClerkOrgCleanup::Pending,
            CleanupRelease::Held => ClerkOrgCleanup::Held,
            CleanupRelease::Requeue => ClerkOrgCleanup::None,
        }
    }
}

/// The sweep's atomic claim on a failed job's orphaned Clerk org: `pending →
/// deleting` in one conditional write (`tenant_provisioning_claim_clerk_org_cleanup`,
/// control 0384), or a takeover of a `deleting` claim older than `stale`.
/// Returns the claim TOKEN (`clerk_org_cleanup_claimed_at`): every later
/// write of the pass ([`release_clerk_org_cleanup`],
/// [`mark_clerk_org_cleanup_done`]) must present it, so a pass whose claim
/// was taken over can no longer write. `Ok(None)`: someone else holds it, or
/// the job is no longer `failed` (an owner retry won).
pub async fn claim_clerk_org_cleanup(
    control: &PgPool,
    clerk_org_id: &str,
    stale: Duration,
) -> Result<Option<DateTime<Utc>>, sqlx::Error> {
    sqlx::query_scalar("SELECT tenant_provisioning_claim_clerk_org_cleanup($1, $2)")
        .bind(clerk_org_id)
        .bind(stale)
        .fetch_one(control)
        .await
}

/// What [`release_clerk_org_cleanup`] did (the definer's answer).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReleaseOutcome {
    /// Back to `pending`.
    Pending,
    /// To `held`, as asked.
    Held,
    /// A requeue past the cap: `held` instead (N8: the cap is the definer's).
    HeldCap,
    /// A requeue of an org already routed in another status: `held` instead
    /// of leaving the claim stuck (N7).
    HeldRouted,
    /// The job was reset to run again (counted).
    Requeued,
    /// Not the claim's holder (taken over, or moved by someone else):
    /// nothing was written.
    Lost,
}

impl ReleaseOutcome {
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "pending" => ReleaseOutcome::Pending,
            "held" => ReleaseOutcome::Held,
            "held_cap" => ReleaseOutcome::HeldCap,
            "held_routed" => ReleaseOutcome::HeldRouted,
            "requeued" => ReleaseOutcome::Requeued,
            "lost" => ReleaseOutcome::Lost,
            _ => return None,
        })
    }

    pub const fn is_held(self) -> bool {
        matches!(self, ReleaseOutcome::Held | ReleaseOutcome::HeldCap | ReleaseOutcome::HeldRouted)
    }
}

/// Leave `deleting` without deleting anything
/// (`tenant_provisioning_release_clerk_org_cleanup`), only as the holder of
/// `claim`. A `Requeue` past `max_requeues`, or of an org already routed in
/// another status, lands in `held` (the definer decides, under its row lock).
pub async fn release_clerk_org_cleanup(
    control: &PgPool,
    clerk_org_id: &str,
    how: CleanupRelease,
    claim: DateTime<Utc>,
    max_requeues: i32,
) -> Result<ReleaseOutcome, sqlx::Error> {
    let s: String = sqlx::query_scalar("SELECT tenant_provisioning_release_clerk_org_cleanup($1, $2, $3, $4)")
        .bind(clerk_org_id)
        .bind(how.as_str())
        .bind(claim)
        .bind(max_requeues)
        .fetch_one(control)
        .await?;
    ReleaseOutcome::parse(&s).ok_or_else(|| sqlx::Error::Protocol(format!("unknown release outcome {s}")))
}

/// Whether `claim` is still the CURRENT claim on this org's cleanup (the
/// sweep re-checks this immediately before each irreversible step).
pub async fn holds_clerk_org_cleanup(control: &PgPool, clerk_org_id: &str, claim: DateTime<Utc>) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM tenant_provisioning_jobs WHERE clerk_org_id = $1 AND status = 'failed' \
                          AND clerk_org_cleanup = 'deleting' AND clerk_org_cleanup_claimed_at = $2)",
    )
    .bind(clerk_org_id)
    .bind(claim)
    .fetch_one(control)
    .await
}

/// How many times the sweep has requeued this org's job (control 0384).
pub async fn clerk_org_cleanup_requeues(control: &PgPool, clerk_org_id: &str) -> Result<i32, sqlx::Error> {
    sqlx::query_scalar("SELECT clerk_org_cleanup_requeues FROM tenant_provisioning_jobs WHERE clerk_org_id = $1")
        .bind(clerk_org_id)
        .fetch_optional(control)
        .await
        .map(|n| n.unwrap_or(0))
}

/// PR A2 records that it has acted on the orphaned Clerk org. Since control
/// 0384 only from the sweep's own CURRENT claim (`deleting`, held by
/// `claim`); see [`claim_clerk_org_cleanup`].
pub async fn mark_clerk_org_cleanup_done(
    control: &PgPool,
    clerk_org_id: &str,
    claim: DateTime<Utc>,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar("SELECT tenant_provisioning_mark_clerk_org_cleanup_done($1, $2)")
        .bind(clerk_org_id)
        .bind(claim)
        .fetch_one(control)
        .await
}

/// PR A2's `POST …/provisioning/retry`, on the serving role: put a `failed`
/// job back on `verify_org` with a fresh attempts budget, no waits, no
/// cancel and no lease, due now (`tenant_provisioning_retry`, control 0381).
/// Refused by the definer for anything else (a job that is not failed, one
/// whose Clerk org was already cleaned up, an org routed in another status);
/// those come back as database errors (`23514`, `23505`, `P0002` for no job).
pub async fn retry(control: &PgPool, clerk_org_id: &str) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar("SELECT tenant_provisioning_retry($1)")
        .bind(clerk_org_id)
        .fetch_one(control)
        .await
}

async fn is_personal_org(control: &PgPool, clerk_org_id: &str) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM personal_orgs WHERE clerk_org_id = $1)")
        .bind(clerk_org_id)
        .fetch_one(control)
        .await
}

async fn org_account_exists(control: &PgPool, clerk_org_id: &str, account_id: Uuid) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM accounts WHERE id = $1 AND clerk_org_id = $2 AND is_org_account)",
    )
    .bind(account_id)
    .bind(clerk_org_id)
    .fetch_one(control)
    .await
}

// ── Org directory (Clerk read) ────────────────────────────────────────────────

/// What `verify_org` needs from Clerk: the org's slug and private metadata.
#[derive(Debug, Clone, Default)]
pub struct OrgRecord {
    pub slug: Option<String>,
    pub private_metadata: Value,
}

/// Read-only org lookup. The Clerk Backend API in production; a static map in
/// tests. `Err` is transient (transport); `Ok(None)` is "no such org".
#[async_trait]
pub trait OrgDirectory: Send + Sync {
    async fn lookup(&self, clerk_org_id: &str) -> Result<Option<OrgRecord>, String>;
}

#[async_trait]
impl OrgDirectory for ClerkAdminClient {
    async fn lookup(&self, clerk_org_id: &str) -> Result<Option<OrgRecord>, String> {
        match self.get_organization(clerk_org_id).await {
            Ok(org) => Ok(Some(OrgRecord {
                slug: org.slug,
                private_metadata: org.private_metadata,
            })),
            Err(e) if e.status() == Some(404) => Ok(None),
            Err(ClerkAdminError::Transport(m)) => Err(format!("clerk transport: {m}")),
            Err(e) => Err(format!("clerk: {e}")),
        }
    }
}

pub struct StaticOrgDirectory {
    orgs: HashMap<String, OrgRecord>,
}

impl StaticOrgDirectory {
    pub fn new(orgs: HashMap<String, OrgRecord>) -> Self {
        StaticOrgDirectory { orgs }
    }
    /// An org that carries the self-serve marker and the given slug.
    pub fn selfserve(pairs: impl IntoIterator<Item = (String, String)>) -> Self {
        let orgs = pairs
            .into_iter()
            .map(|(org, slug)| {
                (
                    org,
                    OrgRecord {
                        slug: Some(slug),
                        private_metadata: serde_json::json!({ SELFSERVE_ORG_MARKER: true }),
                    },
                )
            })
            .collect();
        StaticOrgDirectory { orgs }
    }
}

#[async_trait]
impl OrgDirectory for StaticOrgDirectory {
    async fn lookup(&self, clerk_org_id: &str) -> Result<Option<OrgRecord>, String> {
        Ok(self.orgs.get(clerk_org_id).cloned())
    }
}

// ── Runner ────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct RetryPolicy {
    /// Transient failures tolerated before the job is abandoned (cleanup).
    pub max_attempts: i32,
    pub base_backoff: Duration,
    pub max_backoff: Duration,
    /// How long one claim is honoured before another machine may take over.
    /// Renewed by every write that keeps the lease.
    pub lease_ttl: Duration,
    /// How long a job waits for the org's subscription to become live before
    /// it is abandoned. Separate from `max_attempts`: an ACH / SEPA payment
    /// can sit `incomplete` for days and is not an error.
    pub subscription_wait: Duration,
    /// Re-check interval while waiting for the subscription.
    pub subscription_poll: Duration,
    /// How long a job parked behind a newer rolled-out tenant tree (its
    /// build is behind head) waits before the park ESCALATES (its alert says
    /// it is past its deadline). It is never abandoned for this: it stays
    /// parked, and nothing is deleted.
    pub version_wait: Duration,
    /// Re-check interval while parked behind a newer tree, before the first
    /// alert.
    pub version_poll: Duration,
    /// How long the version park stays quiet before its first alertable
    /// `error!`. About an hour: a rolling deploy finishes well inside it, so
    /// an alert means no newer build has taken the job over.
    pub version_alert_after: Duration,
    /// Once alerting, the park re-checks (and alerts) this often.
    pub version_alert_every: Duration,
}

/// The subscription wait: at least 7 days, the settlement window for async
/// payment methods (ACH debits, SEPA Direct Debit), plus a day of slack.
pub const SUBSCRIPTION_WAIT: Duration = Duration::from_secs(8 * 24 * 60 * 60);

impl Default for RetryPolicy {
    fn default() -> Self {
        RetryPolicy {
            max_attempts: 8,
            base_backoff: Duration::from_secs(30),
            max_backoff: Duration::from_secs(30 * 60),
            lease_ttl: Duration::from_secs(10 * 60),
            subscription_wait: SUBSCRIPTION_WAIT,
            subscription_poll: Duration::from_secs(15 * 60),
            version_wait: Duration::from_secs(24 * 60 * 60),
            version_poll: Duration::from_secs(15 * 60),
            version_alert_after: Duration::from_secs(60 * 60),
            version_alert_every: Duration::from_secs(60 * 60),
        }
    }
}

impl RetryPolicy {
    fn backoff(&self, attempts: i32) -> Duration {
        let pow = attempts.clamp(0, 20) as u32;
        let secs = self.base_backoff.as_secs().saturating_mul(1u64 << pow.min(16));
        Duration::from_secs(secs).min(self.max_backoff)
    }

    /// The version park after `waited`: when to check next, and how loudly
    /// to say so. Quiet (a `warn!`, every [`Self::version_poll`]) until
    /// [`Self::version_alert_after`]; then an alertable `error!` on every
    /// check, hourly ([`Self::version_alert_every`]); past
    /// [`Self::version_wait`] the same, marked escalated. Never "give up".
    fn version_park(&self, waited: Duration) -> (Duration, ParkAlert) {
        if waited < self.version_alert_after {
            (self.version_poll, ParkAlert::Quiet)
        } else if waited < self.version_wait {
            (self.version_alert_every, ParkAlert::Alert)
        } else {
            (self.version_alert_every, ParkAlert::Escalated)
        }
    }
}

/// How loudly one version-park check reports ([`RetryPolicy::version_park`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ParkAlert {
    Quiet,
    Alert,
    Escalated,
}

/// Test-only fault injection: what to do AFTER a step's side effects, before
/// its progress is recorded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fault {
    /// Simulate a crash between effect and record (a transient error).
    CrashAfterEffect,
    /// Simulate an unrecoverable condition.
    Permanent,
    /// Panic (a bug): the worker's supervision must catch it and charge the
    /// job an attempt.
    Panic,
}

pub type FaultHook = Arc<dyn Fn(Step) -> Option<Fault> + Send + Sync>;

/// Everything a run needs. Shared by the worker and by tests.
pub struct Deps {
    /// The serving role's control pool (`coralswarm_app`): every read the
    /// job makes (the job row, routing lookups, the RLS'd subscription).
    pub control: PgPool,
    /// The worker's own control pool, a `coralswarm_provisioner` login
    /// ([`PROVISIONER_DATABASE_URL`]): every job, routing and credential
    /// write, and the in-process `migrate_all` (whose version record goes
    /// through a provisioner-only definer).
    pub provisioner_control: PgPool,
    pub provisioner: Arc<dyn TenantDbProvisioner>,
    pub directory: Arc<dyn OrgDirectory>,
    /// Builds the tenant pool for `provision_dek` — the same
    /// fingerprint-verifying registry the request path uses. Its resolver
    /// must be the chain (env, then `tenant_db_credentials`).
    pub registry: Arc<TenantRegistry>,
    /// The same resolver, for `migrate`/`fingerprint`.
    pub resolver: Arc<dyn SecretResolver>,
    pub kek: [u8; 32],
    pub next_kek: Option<[u8; 32]>,
    pub policy: RetryPolicy,
    /// This machine's lease identity.
    pub owner: String,
    pub fault: Option<FaultHook>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunOutcome {
    Ready,
    /// A transient error; the job will be retried at `next_attempt_at`.
    Deferred { step: Step, attempts: i32, reason: String },
    /// Terminal: cleaned up (or cleanup itself gave up) and marked failed.
    Failed { reason: String },
    /// Another machine holds (or took) the lease.
    LeaseLost,
}

#[derive(Debug)]
enum StepError {
    Transient(String),
    Permanent(String),
    /// The org's subscription is not live yet. Waited out on its own clock.
    AwaitingSubscription(String),
    /// `activate` refused (55000): a newer tenant tree is already rolled
    /// out and this build is behind it. Parked on its own clock; never
    /// abandoned, so it never deletes anything.
    AwaitingNewerBuild(String),
}

/// Which park a deferral is (control 0381 `tenant_provisioning_defer`'s
/// `p_wait`): each runs on its own clock, not the attempts budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Wait {
    Subscription,
    Version,
}

impl Wait {
    fn as_str(self) -> &'static str {
        match self {
            Wait::Subscription => "subscription",
            Wait::Version => "version",
        }
    }
}

impl From<ProvisionerError> for StepError {
    fn from(e: ProvisionerError) -> Self {
        match e {
            ProvisionerError::Transient(m) => StepError::Transient(m),
            ProvisionerError::Permanent(m) => StepError::Permanent(m),
            ProvisionerError::AlreadyExists(m) => StepError::Transient(m),
        }
    }
}

/// Definer-function refusals are invariant violations (permanent); anything
/// else from the database is transient.
fn db_step_error(context: &str, e: sqlx::Error) -> StepError {
    match e.as_database_error().and_then(|d| d.code()).as_deref() {
        Some("23514") | Some("23503") | Some("23505") | Some("P0002") | Some("22023") => {
            StepError::Permanent(format!("{context}: {}", e.as_database_error().map(|d| d.message().to_string()).unwrap_or_default()))
        }
        _ => StepError::Transient(format!("{context}: {e}")),
    }
}

/// Claim one due job and run it as far as it will go. `Ok(None)` when nothing
/// is due.
pub async fn run_next_due(deps: &Deps) -> Result<Option<(String, RunOutcome)>, sqlx::Error> {
    let Some(job) = claim_next_due(deps).await? else {
        return Ok(None);
    };
    let org = job.clerk_org_id.clone();
    let outcome = run_claimed(deps, job).await?;
    Ok(Some((org, outcome)))
}

/// Claim the earliest due job under this machine's lease. The worker claims
/// and runs separately ([`run_claimed_job`]) so a panicking run can still be
/// charged to the job it was running ([`record_panic`]).
pub async fn claim_next_due(deps: &Deps) -> Result<Option<Job>, sqlx::Error> {
    claim_due(&deps.provisioner_control, &deps.owner, deps.policy.lease_ttl).await
}

/// Run a job [`claim_next_due`] returned, as far as it will go.
pub async fn run_claimed_job(deps: &Deps, job: Job) -> Result<RunOutcome, sqlx::Error> {
    run_claimed(deps, job).await
}

/// Claim and run a specific org's job regardless of `next_attempt_at` (an
/// operator path and tests). `Ok(None)` when no live job.
pub async fn run_job(deps: &Deps, clerk_org_id: &str) -> Result<Option<RunOutcome>, sqlx::Error> {
    let Some(job) = claim(&deps.provisioner_control, clerk_org_id, &deps.owner, deps.policy.lease_ttl).await? else {
        return Ok(None);
    };
    Ok(Some(run_claimed(deps, job).await?))
}

/// A run of `clerk_org_id`'s job panicked (the worker caught it). Charge it
/// as one transient attempt, so a job that panics on every run reaches
/// [`RetryPolicy::max_attempts`] and is abandoned (or, on `cleanup`, marked
/// failed) instead of alerting forever. The row's `last_error` says only
/// that a run panicked: the panic text goes to the log, never to the row,
/// since nobody vetted it for secrets. `LeaseLost` when this machine no
/// longer holds the job.
pub async fn record_panic(deps: &Deps, clerk_org_id: &str) -> Result<RunOutcome, sqlx::Error> {
    let Some(job) = get(&deps.control, clerk_org_id).await? else {
        return Ok(RunOutcome::LeaseLost);
    };
    if job.status != JobStatus::Provisioning.as_str() || job.lease_owner.as_deref() != Some(deps.owner.as_str()) {
        return Ok(RunOutcome::LeaseLost);
    }
    let reason = "a run of this step panicked (see the worker's error log)".to_string();
    let attempts = job.attempts + 1;
    if job.step() == Some(Step::Cleanup) {
        if attempts >= deps.policy.max_attempts {
            let why = format!(
                "{} (cleanup incomplete after {attempts} attempts: {reason})",
                job.last_error.clone().unwrap_or_else(|| "unknown".into())
            );
            if !mark_failed(deps, clerk_org_id, &why, false).await? {
                return Ok(RunOutcome::LeaseLost);
            }
            alert_terminal_failure(&job, &why, false);
            return Ok(RunOutcome::Failed { reason: why });
        }
        if !defer(deps, clerk_org_id, attempts, None, deps.policy.backoff(attempts), None).await? {
            return Ok(RunOutcome::LeaseLost);
        }
        return Ok(RunOutcome::Deferred { step: Step::Cleanup, attempts, reason });
    }
    let step = job.step().unwrap_or(Step::Cleanup);
    if attempts >= deps.policy.max_attempts {
        let why = format!("step {} failed {attempts} times; last error: {reason}", job.step);
        return Box::pin(abandon(deps, &job, why)).await;
    }
    if !defer(deps, clerk_org_id, attempts, Some(reason.clone()), deps.policy.backoff(attempts), None).await? {
        return Ok(RunOutcome::LeaseLost);
    }
    Ok(RunOutcome::Deferred { step, attempts, reason })
}

async fn claim_due(control: &PgPool, owner: &str, ttl: Duration) -> Result<Option<Job>, sqlx::Error> {
    let q = format!("SELECT {JOB_COLS} FROM tenant_provisioning_claim($1, $2, NULL)");
    sqlx::query_as::<_, Job>(&q)
        .bind(owner.to_string())
        .bind(ttl)
        .fetch_optional(control)
        .await
}

async fn claim(control: &PgPool, clerk_org_id: &str, owner: &str, ttl: Duration) -> Result<Option<Job>, sqlx::Error> {
    let q = format!("SELECT {JOB_COLS} FROM tenant_provisioning_claim($1, $2, $3)");
    sqlx::query_as::<_, Job>(&q)
        .bind(owner.to_string())
        .bind(ttl)
        .bind(clerk_org_id.to_string())
        .fetch_optional(control)
        .await
}

// Lease-conditioned job writes, each one 0381 definer. `Ok(false)` means the
// lease was lost. Those that keep the lease renew it to `now() + lease_ttl`.

/// Move the job to `step` (forward path, `activate → migrate`, or cleanup).
async fn set_step(deps: &Deps, org: &str, step: Step, last_error: Option<String>) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar("SELECT tenant_provisioning_set_step($1, $2, $3, $4, $5)")
        .bind(org.to_string())
        .bind(deps.owner.clone())
        .bind(deps.policy.lease_ttl)
        .bind(step.as_str().to_string())
        .bind(last_error)
        .fetch_one(&deps.provisioner_control)
        .await
}

/// Defer the job by `delay` and release the lease. `last_error: None` keeps
/// the recorded reason; `wait` starts that park's clock (None: a plain
/// transient retry).
async fn defer(
    deps: &Deps,
    org: &str,
    attempts: i32,
    last_error: Option<String>,
    delay: Duration,
    wait: Option<Wait>,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar("SELECT tenant_provisioning_defer($1, $2, $3, $4, $5, $6)")
        .bind(org.to_string())
        .bind(deps.owner.clone())
        .bind(attempts)
        .bind(last_error)
        .bind(delay)
        .bind(wait.map(Wait::as_str))
        .fetch_one(&deps.provisioner_control)
        .await
}

/// One of the `(org, owner, ttl)` definers: create intent, clear intent,
/// credential stored.
async fn held_write(deps: &Deps, function: &'static str, org: &str) -> Result<bool, sqlx::Error> {
    let q = format!("SELECT {function}($1, $2, $3)");
    sqlx::query_scalar(&q)
        .bind(org.to_string())
        .bind(deps.owner.clone())
        .bind(deps.policy.lease_ttl)
        .fetch_one(&deps.provisioner_control)
        .await
}

async fn record_database(deps: &Deps, org: &str, provider: &str, provider_id: &str) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar("SELECT tenant_provisioning_record_database($1, $2, $3, $4, $5)")
        .bind(org.to_string())
        .bind(deps.owner.clone())
        .bind(deps.policy.lease_ttl)
        .bind(provider.to_string())
        .bind(provider_id.to_string())
        .fetch_one(&deps.provisioner_control)
        .await
}

/// Mark the job (on `cleanup`) failed.
async fn mark_failed(deps: &Deps, org: &str, reason: &str, forget_database: bool) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar("SELECT tenant_provisioning_fail($1, $2, $3, $4)")
        .bind(org.to_string())
        .bind(deps.owner.clone())
        .bind(reason.to_string())
        .bind(forget_database)
        .fetch_one(&deps.provisioner_control)
        .await
}

/// Erase a future to `dyn Future + Send`. See [`run_claimed`] for why the
/// step loop needs this at each await.
fn boxed<'a, F>(f: F) -> std::pin::Pin<Box<dyn std::future::Future<Output = F::Output> + Send + 'a>>
where
    F: std::future::Future + Send + 'a,
{
    Box::pin(f)
}

type BoxedRun<'a> = std::pin::Pin<Box<dyn std::future::Future<Output = Result<RunOutcome, sqlx::Error>> + Send + 'a>>;

/// Boxed rather than `async fn`: the step loop recurses into cleanup and
/// rustc's auto-trait inference for the unboxed future trips on the
/// higher-ranked borrows (`Send` "not general enough" at the spawn site).
fn run_claimed(deps: &Deps, job: Job) -> BoxedRun<'_> {
    Box::pin(run_claimed_inner(deps, job))
}

async fn run_claimed_inner(deps: &Deps, mut job: Job) -> Result<RunOutcome, sqlx::Error> {
    let org = job.clerk_org_id.clone();
    loop {
        if job.cancel_requested_at.is_some() && job.step != Step::Cleanup.as_str() {
            tracing::warn!(org = %org, step = %job.step, "tenant provisioning cancelled; cleaning up");
            if !boxed(set_step(deps, &org, Step::Cleanup, Some("cancelled".into()))).await? {
                return Ok(RunOutcome::LeaseLost);
            }
            job.step = Step::Cleanup.as_str().to_string();
            job.last_error = Some("cancelled".into());
        }
        let Some(step) = job.step() else {
            // A step no code knows: treat as permanent and clean up.
            return Box::pin(abandon(deps, &job, format!("unknown step {:?}", job.step))).await;
        };

        if step == Step::Cleanup {
            return Box::pin(run_cleanup(deps, job)).await;
        }
        if step == Step::Done {
            return Ok(RunOutcome::Ready);
        }

        let result = boxed(run_step(deps, &job, step)).await;
        let result = match (&result, deps.fault.as_ref().and_then(|f| f(step))) {
            (Ok(_), Some(Fault::CrashAfterEffect)) => Err(StepError::Transient("injected crash after effect".into())),
            (Ok(_), Some(Fault::Permanent)) => Err(StepError::Permanent("injected permanent failure".into())),
            (Ok(_), Some(Fault::Panic)) => panic!("injected panic in step {}", step.as_str()),
            _ => result,
        };

        match result {
            Ok(next) => {
                if next == Step::Done {
                    // `activate` already flipped the job to ready inside the
                    // definer function (lease released there too).
                    return Ok(RunOutcome::Ready);
                }
                if !boxed(set_step(deps, &org, next, None)).await? {
                    return Ok(RunOutcome::LeaseLost);
                }
                tracing::info!(org = %org, from = step.as_str(), to = next.as_str(), "tenant provisioning step complete");
                job = match boxed(get(&deps.control, &org)).await? {
                    Some(j) => j,
                    None => return Ok(RunOutcome::LeaseLost),
                };
            }
            Err(StepError::Transient(reason)) => {
                let attempts = job.attempts + 1;
                if attempts >= deps.policy.max_attempts {
                    let why = format!(
                        "step {} failed {attempts} times; last error: {reason}",
                        step.as_str()
                    );
                    return Box::pin(abandon(deps, &job, why)).await;
                }
                let delay = deps.policy.backoff(attempts);
                tracing::warn!(org = %org, step = step.as_str(), attempts, delay_secs = delay.as_secs(), error = %reason,
                    "tenant provisioning step failed; will retry");
                if !boxed(defer(deps, &org, attempts, Some(reason.clone()), delay, None)).await? {
                    return Ok(RunOutcome::LeaseLost);
                }
                return Ok(RunOutcome::Deferred { step, attempts, reason });
            }
            Err(StepError::AwaitingSubscription(reason)) => {
                // Not an error and not the attempts budget: its own deadline.
                let waited = job
                    .subscription_wait_started_at
                    .map(|since| (Utc::now() - since).to_std().unwrap_or_default())
                    .unwrap_or_default();
                if waited >= deps.policy.subscription_wait {
                    let why = format!(
                        "step {}: the org's subscription did not become live within {} hours; last check: {reason}",
                        step.as_str(),
                        deps.policy.subscription_wait.as_secs() / 3600
                    );
                    return Box::pin(abandon(deps, &job, why)).await;
                }
                let delay = deps.policy.subscription_poll;
                tracing::info!(org = %org, step = step.as_str(), waited_secs = waited.as_secs(),
                    "tenant provisioning waiting for a live subscription");
                if !boxed(defer(deps, &org, job.attempts, Some(reason.clone()), delay, Some(Wait::Subscription))).await? {
                    return Ok(RunOutcome::LeaseLost);
                }
                return Ok(RunOutcome::Deferred { step, attempts: job.attempts, reason });
            }
            Err(StepError::AwaitingNewerBuild(reason)) => {
                // A park, not an error: no attempts, never cleanup. A newer
                // build's worker takes the job over (its `activate` sees the
                // row behind its head and re-migrates). Past the deadline
                // the park alerts on every (slower) check and stays parked:
                // the customer's database is never deleted for this.
                let waited = job
                    .version_wait_started_at
                    .map(|since| (Utc::now() - since).to_std().unwrap_or_default())
                    .unwrap_or_default();
                let (delay, alert) = deps.policy.version_park(waited);
                match alert {
                    ParkAlert::Quiet => tracing::warn!(org = %org, step = step.as_str(), waited_secs = waited.as_secs(), reason = %reason,
                        "tenant provisioning parked: a newer tenant tree is rolled out; waiting for a newer build"),
                    ParkAlert::Alert | ParkAlert::Escalated => tracing::error!(
                        connector_health = true,
                        org = %org,
                        slug = %job.slug,
                        step = step.as_str(),
                        waited_secs = waited.as_secs(),
                        escalated = alert == ParkAlert::Escalated,
                        head = migrate::head_version(),
                        reason = %reason,
                        "tenant provisioning parked behind a newer tenant tree and no newer build has \
                         taken it over (the job stays parked; nothing is deleted)"
                    ),
                }
                if !boxed(defer(deps, &org, job.attempts, Some(reason.clone()), delay, Some(Wait::Version))).await? {
                    return Ok(RunOutcome::LeaseLost);
                }
                return Ok(RunOutcome::Deferred { step, attempts: job.attempts, reason });
            }
            Err(StepError::Permanent(reason)) => {
                let why = format!("step {}: {reason}", step.as_str());
                return Box::pin(abandon(deps, &job, why)).await;
            }
        }
    }
}

/// Move a live job to `cleanup` with its terminal reason, then run it.
async fn abandon(deps: &Deps, job: &Job, reason: String) -> Result<RunOutcome, sqlx::Error> {
    let org = &job.clerk_org_id;
    tracing::warn!(org = %org, step = %job.step, reason = %reason, "tenant provisioning abandoned; cleaning up");
    if !set_step(deps, org, Step::Cleanup, Some(reason.clone())).await? {
        return Ok(RunOutcome::LeaseLost);
    }
    let job = match get(&deps.control, org).await? {
        Some(j) => j,
        None => return Ok(RunOutcome::LeaseLost),
    };
    Box::pin(run_cleanup(deps, job)).await
}

// ── Steps ─────────────────────────────────────────────────────────────────────

async fn run_step(deps: &Deps, job: &Job, step: Step) -> Result<Step, StepError> {
    match step {
        Step::VerifyOrg => boxed(step_verify_org(deps, job)).await,
        Step::CreateDatabase => boxed(step_create_database(deps, job)).await,
        Step::InsertRouting => boxed(step_insert_routing(deps, job)).await,
        Step::Migrate => boxed(step_migrate(deps, job)).await,
        Step::Contract => boxed(step_contract(deps, job)).await,
        Step::Fingerprint => boxed(step_fingerprint(deps, job)).await,
        Step::ProvisionDek => boxed(step_provision_dek(deps, job)).await,
        Step::Activate => boxed(step_activate(deps, job)).await,
        Step::Done | Step::Cleanup => Ok(step),
    }
}

async fn step_verify_org(deps: &Deps, job: &Job) -> Result<Step, StepError> {
    let org = &job.clerk_org_id;
    // Re-checked at execution time, not only at enqueue (audit 4i).
    if is_personal_org(&deps.control, org)
        .await
        .map_err(|e| StepError::Transient(format!("personal_orgs: {e}")))?
    {
        return Err(StepError::Permanent(
            "org is a personal organization (personal_orgs); it gets no tenant database".into(),
        ));
    }
    // 0321's trigger needs this row; the job checks it rather than assumes.
    if !org_account_exists(&deps.control, org, job.account_id)
        .await
        .map_err(|e| StepError::Transient(format!("accounts: {e}")))?
    {
        return Err(StepError::Permanent(format!(
            "no control accounts row for org bound to account {}",
            job.account_id
        )));
    }
    // Card-up-front: the job is enqueued by the Checkout-completed webhook,
    // but the subscription can be canceled or fail before the job runs.
    // Re-checked here and again right before the provider create.
    require_live_subscription(deps, job).await?;
    if let Some(row) = routing::lookup_by_org(&deps.control, org)
        .await
        .map_err(|e| StepError::Transient(format!("routing: {e}")))?
    {
        if row.status != "provisioning" {
            return Err(StepError::Permanent(format!(
                "org already has a routing row in status {}",
                row.status
            )));
        }
    }
    let record = deps
        .directory
        .lookup(org)
        .await
        .map_err(StepError::Transient)?
        .ok_or_else(|| StepError::Permanent("org not found in Clerk".into()))?;
    // The slug is the JOB's input (PR A2 generates and validates it
    // server-side and it becomes `tenant_databases.slug`); it is NOT read
    // from Clerk. The Clerk dev instance has organization slugs disabled
    // (`organization_slugs_disabled`), so `onboard_tenant.sh` step 0's
    // slug-parity check cannot run there, and since claim-based tenancy the
    // Clerk slug no longer routes anything. A Clerk slug that happens to
    // exist and differ is logged, never a failure.
    if let Some(clerk_slug) = record.slug.as_deref() {
        if clerk_slug != job.slug {
            tracing::warn!(org = %org, clerk_slug, job_slug = %job.slug,
                "Clerk org slug differs from the routing slug (informational; the routing slug is authoritative)");
        }
    }
    if record.private_metadata.get(SELFSERVE_ORG_MARKER).and_then(|v| v.as_bool()) != Some(true) {
        return Err(StepError::Permanent(format!(
            "Clerk org does not carry the {SELFSERVE_ORG_MARKER} marker; not a self-serve org"
        )));
    }
    Ok(Step::CreateDatabase)
}

/// Who a database's `tenant_meta` fingerprint says it belongs to.
#[derive(Debug, Clone, PartialEq, Eq)]
enum DbOwner {
    /// No `tenant_meta` table, or no row in it.
    Unclaimed,
    This,
    Other(String),
}

#[derive(Debug)]
enum ProbeError {
    /// The database would not take a connection.
    Connect(String),
    Read(String),
}

impl From<ProbeError> for StepError {
    fn from(e: ProbeError) -> Self {
        match e {
            ProbeError::Connect(m) | ProbeError::Read(m) => StepError::Transient(m),
        }
    }
}

/// Read a database's fingerprint over a short-lived owner connection. Only
/// reads: nothing is created or changed on a database that may not be ours.
async fn probe_owner(dsn: &str, org: &str) -> Result<DbOwner, ProbeError> {
    let dsn = dsn.to_string();
    let org = org.to_string();
    // Owned and boxed: see `step_contract`.
    let probe: std::pin::Pin<Box<dyn std::future::Future<Output = Result<DbOwner, ProbeError>> + Send>> =
        Box::pin(async move {
            let mut conn = tokio::time::timeout(Duration::from_secs(15), PgConnection::connect(&dsn))
                .await
                .map_err(|_| ProbeError::Connect("connect database (owner check): timed out".into()))?
                .map_err(|e| ProbeError::Connect(format!("connect database (owner check): {e}")))?;
            let read = async {
                let has_meta: bool = sqlx::query_scalar("SELECT to_regclass('tenant_meta') IS NOT NULL")
                    .fetch_one(&mut conn)
                    .await?;
                if !has_meta {
                    return Ok::<_, sqlx::Error>(None);
                }
                sqlx::query_scalar::<_, String>("SELECT org_id FROM tenant_meta LIMIT 1")
                    .fetch_optional(&mut conn)
                    .await
            }
            .await;
            let _ = conn.close().await;
            match read.map_err(|e| ProbeError::Read(format!("fingerprint read (owner check): {e}")))? {
                None => Ok(DbOwner::Unclaimed),
                Some(o) if o == org => Ok(DbOwner::This),
                Some(o) => Ok(DbOwner::Other(o)),
            }
        });
    probe.await
}

/// Find-or-create the provider database and record it.
///
/// Only a database this job created is ever recorded. One found by name is
/// this job's only if the job had already recorded its intent to create one
/// (`database_create_requested_at`, written before every `create` call): the
/// crash-between-create-and-record case. Found without that intent it
/// predates the job and is refused, fingerprinted or not:
/// `scripts/onboard_tenant.sh` creates a concierge tenant's database before
/// its routing row, so a half-onboarded tenant by the same name is exactly
/// that. A refused database is never recorded, migrated or fingerprinted;
/// the only connection to it is the read-only owner check.
async fn step_create_database(deps: &Deps, job: &Job) -> Result<Step, StepError> {
    let org = &job.clerk_org_id;
    let provider = deps.provisioner.provider_name();
    let name = deps.provisioner.database_name(&job.slug);
    let db = match deps.provisioner.find_by_name(&name).await? {
        Some(db) => {
            if let Some(recorded) = job.provider_database_id.as_deref() {
                if recorded != db.provider_id {
                    return Err(StepError::Permanent(format!(
                        "database {name} is now provider id {}, but this job recorded {recorded}",
                        db.provider_id
                    )));
                }
            }
            if let DbOwner::Other(other) = boxed(probe_owner(&db.dsn, org)).await? {
                tracing::error!(
                    connector_health = true,
                    org = %org,
                    slug = %job.slug,
                    provider,
                    database = %name,
                    provider_database_id = %db.provider_id,
                    fingerprinted_for = %other,
                    "tenant provisioning name collision: the database this org would get already belongs to another org"
                );
                return Err(StepError::Permanent(format!(
                    "a database named {name} already exists and is fingerprinted for a different org ({other}); \
                     it is not this job's and was left untouched"
                )));
            }
            if job.database_create_requested_at.is_none() {
                tracing::error!(
                    connector_health = true,
                    org = %org,
                    slug = %job.slug,
                    provider,
                    database = %name,
                    provider_database_id = %db.provider_id,
                    "tenant provisioning name collision: a database this job did not create already has this org's name"
                );
                return Err(StepError::Permanent(format!(
                    "a database named {name} already exists and this job did not create it \
                     (possibly a concierge tenant mid-onboarding); it was left untouched"
                )));
            }
            db
        }
        None => {
            // The last check before something billable exists: a retry of
            // this step can run long after `verify_org` passed.
            require_live_subscription(deps, job).await?;
            if !held_write(deps, "tenant_provisioning_record_create_intent", org)
                .await
                .map_err(|e| StepError::Transient(format!("record create intent: {e}")))?
            {
                return Err(StepError::Transient("lease lost while recording create intent".into()));
            }
            match deps.provisioner.create(&name, job.region.as_deref()).await {
                Ok(db) => db,
                Err(ProvisionerError::AlreadyExists(m)) => {
                    // Not ours. Drop the intent so the next attempt, which
                    // will find it, refuses it. A recorded id keeps its
                    // intent; the next attempt refuses on the id mismatch.
                    held_write(deps, "tenant_provisioning_clear_create_intent", org)
                        .await
                        .map_err(|e| StepError::Transient(format!("clear create intent: {e}")))?;
                    return Err(StepError::Transient(format!("{m}; re-checking whose it is")));
                }
                Err(e) => return Err(e.into()),
            }
        }
    };
    // Record the id BEFORE anything else connects to it. The definer
    // records it once (a different id over a recorded one is refused) and
    // the column CHECK pins its shape, since cleanup deletes by it.
    if !record_database(deps, org, provider, &db.provider_id)
        .await
        .map_err(|e| db_step_error("record provider id", e))?
    {
        return Err(StepError::Transient("lease lost while recording provider id".into()));
    }
    match credentials::store(&deps.provisioner_control, &deps.kek, deps.next_kek.as_ref(), &job.db_secret_ref, &db.dsn).await {
        // `AlreadyPresent` is this job's own earlier write, so cleanup may
        // delete it: the ref is unique to this job, this job is the only
        // writer of the table, and the stored DSN equals that of a database
        // this job created.
        Ok(StoreOutcome::Inserted) | Ok(StoreOutcome::AlreadyPresent) => {}
        Err(CredentialError::Conflict(r)) => {
            return Err(StepError::Permanent(format!(
                "credential {r} already names a different database"
            )))
        }
        Err(CredentialError::Db(e)) => return Err(db_step_error("store credential", e)),
        Err(e) => return Err(StepError::Permanent(format!("store credential: {e}"))),
    }
    // A crash right here leaves the credential stored but unrecorded. That
    // is safe: the retry finds it (`AlreadyPresent`), and cleanup deletes
    // the job's credential unconditionally.
    if !held_write(deps, "tenant_provisioning_record_credential_stored", org)
        .await
        .map_err(|e| StepError::Transient(format!("record credential: {e}")))?
    {
        return Err(StepError::Transient("lease lost while recording the credential".into()));
    }
    Ok(Step::InsertRouting)
}

async fn step_insert_routing(deps: &Deps, job: &Job) -> Result<Step, StepError> {
    routing::provisioning_insert_routing(
        &deps.provisioner_control,
        &job.clerk_org_id,
        job.account_id,
        &job.slug,
        &job.db_secret_ref,
        job.region.as_deref(),
    )
    .await
    .map_err(|e| db_step_error("insert routing row", e))?;
    Ok(Step::Migrate)
}

/// The routing row records a tenant tree NEWER than this build's head: a
/// newer build's worker already migrated this job's database, and this build
/// is older (a rollback, or an older machine mid-deploy). Nothing this build
/// does can fix that, and migrating would only fail (sqlx refuses a database
/// that records a migration it does not embed). So it is the version park:
/// no attempt spent, never abandoned, so cleanup never deletes the
/// customer's database. A build at least that new takes the job over.
fn ahead_of_build(step: &str, recorded: i64, head: i64) -> StepError {
    StepError::AwaitingNewerBuild(format!(
        "{step}: the tenant database is at schema_version {recorded}, ahead of this build's head {head} \
         (a newer tenant tree is rolled out; waiting for a build at least that new)"
    ))
}

async fn step_migrate(deps: &Deps, job: &Job) -> Result<Step, StepError> {
    let org = &job.clerk_org_id;
    let head = migrate::head_version();
    // Before touching the database: a row already ahead of this build parks.
    let before = routing::lookup_by_org(&deps.control, org)
        .await
        .map_err(|e| StepError::Transient(format!("routing: {e}")))?
        .ok_or_else(|| StepError::Permanent("routing row vanished before migrate".into()))?;
    if let Some(v) = before.schema_version.filter(|v| *v > head) {
        return Err(ahead_of_build("migrate", v, head));
    }
    // On the provisioner pool, under this job's lease: the version record
    // goes through `tenant_provisioning_record_schema_version`, which only
    // the lease holder of a job on `migrate` may call.
    let opts = MigrateOpts {
        only: Some(org.clone()),
        jobs: 1,
        provisioning_lease: Some(migrate::ProvisioningLease {
            owner: deps.owner.clone(),
            ttl: deps.policy.lease_ttl,
        }),
        ..Default::default()
    };
    let report = boxed(migrate::migrate_all(&deps.provisioner_control, deps.resolver.clone(), &opts))
        .await
    .map_err(|e| StepError::Transient(format!("migrate: {e}")))?;
    let outcome = report
        .outcomes
        .into_iter()
        .find(|o| &o.clerk_org_id == org)
        .ok_or_else(|| StepError::Permanent("migrate: the routing row was not selected".into()))?;
    match outcome.result {
        TenantResult::Migrated { .. } | TenantResult::UpToDate { .. } => {}
        TenantResult::Skipped { reason } => {
            return Err(StepError::Transient(format!("migrate skipped: {reason}")))
        }
        TenantResult::Failed { error } => return Err(StepError::Transient(format!("migrate: {error}"))),
        TenantResult::Drift { detail } => return Err(StepError::Permanent(format!("migrate drift: {detail}"))),
        // The database itself is ahead (its ledger records a migration this
        // build does not embed), whatever the routing row says.
        TenantResult::AheadOfBuild { version, head } => return Err(ahead_of_build("migrate", version, head)),
    }
    // Read back: the write must have landed (audit 4d).
    let row = routing::lookup_by_org(&deps.control, org)
        .await
        .map_err(|e| StepError::Transient(format!("routing: {e}")))?
        .ok_or_else(|| StepError::Permanent("routing row vanished during migrate".into()))?;
    if let Some(v) = row.schema_version.filter(|v| *v > head) {
        return Err(ahead_of_build("migrate", v, head));
    }
    if row.schema_version != Some(head) {
        return Err(StepError::Transient(format!(
            "schema_version after migrate is {:?}, expected {head}",
            row.schema_version
        )));
    }
    Ok(Step::Contract)
}

/// The operator-run P2 identity contract, included rather than copied so the
/// job and the runbook (`docs/implementation/connected-memory-p2-lifecycle.md`)
/// cannot drift. Until it runs, `corpus_enrichment::lifecycle_ready_tx` is
/// false and every managed admission path (embedding, the funded gate,
/// standing admission) holds the tenant's content.
const IDENTITY_CONTRACT_SQL: &str = include_str!("../../schema/identity-contract.sql");

/// Apply the identity contract to the database THIS job created.
///
/// The script's attestation (`app.identity_serving_contract='composite-v1'`)
/// asserts no serving binary still upserts `nodes` on the legacy
/// `(graph_id, type, lower(title))` target. Every binary that can run this
/// step was built with the composite target, and no serving image reaches
/// the tenant before `activate`, so for a brand-new database the attestation
/// is true by construction. Existing tenants are never touched here: the job
/// only ever connects through its own `db_secret_ref`.
///
/// One direct owner session (the migrate connect options, never the pooler),
/// the attestation `SET LOCAL` inside the transaction the script then commits
/// (its own leading `BEGIN` is a no-op warning there). Idempotent: every
/// statement in the script converges on a second run.
async fn step_contract(deps: &Deps, job: &Job) -> Result<Step, StepError> {
    let dsn = deps
        .resolver
        .resolve_async(&job.db_secret_ref)
        .await
        .map_err(|e| StepError::Transient(format!("resolve tenant DSN: {e}")))?;
    let options = migrate::migration_connect_options(&dsn)
        .map_err(|_| StepError::Permanent("invalid tenant connection options".into()))?;
    let org = job.clerk_org_id.clone();
    // Owned and boxed for the same reason as `migrate_all`'s per-tenant
    // futures: a `&mut PgConnection` executor borrowed across these awaits
    // is otherwise not provably `Send` from the spawned worker task.
    let apply: std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), StepError>> + Send>> =
        Box::pin(async move {
            let mut conn = tokio::time::timeout(Duration::from_secs(15), PgConnection::connect_with(&options))
                .await
                .map_err(|_| StepError::Transient("connect tenant (contract): timed out".into()))?
                .map_err(|e| StepError::Transient(format!("connect tenant (contract): {e}")))?;
            let applied = apply_contract(&mut conn, &org).await;
            let _ = conn.close().await;
            applied
        });
    apply.await?;

    let pool = tenant_conn(deps, job).await?;
    let check = pool.clone();
    let ready: std::pin::Pin<Box<dyn std::future::Future<Output = Result<bool, sqlx::Error>> + Send>> =
        Box::pin(async move {
            let mut tx = check.begin().await?;
            let ready = crate::corpus_enrichment::lifecycle_ready_tx(&mut tx).await?;
            tx.rollback().await?;
            Ok(ready)
        });
    let ready = ready.await;
    pool.close().await;
    match ready {
        Ok(true) => Ok(Step::Fingerprint),
        Ok(false) => Err(StepError::Transient(
            "identity contract applied but the tenant is not lifecycle-ready".into(),
        )),
        Err(e) => Err(StepError::Transient(format!("lifecycle readiness read: {e}"))),
    }
}

async fn apply_contract(conn: &mut PgConnection, org: &str) -> Result<(), StepError> {
    let fail = |what: &str, e: sqlx::Error| StepError::Transient(format!("identity contract {what}: {e}"));
    sqlx::Executor::execute(
        &mut *conn,
        "BEGIN; SET LOCAL app.identity_serving_contract = 'composite-v1'; SET LOCAL lock_timeout = '30s'",
    )
    .await
    .map_err(|e| fail("begin", e))?;
    // `fingerprint` runs after this step, so a database found by name that
    // already belongs to another org is refused here, before it is changed.
    let owner: Option<String> = sqlx::query_scalar("SELECT org_id FROM tenant_meta WHERE id = true")
        .fetch_optional(&mut *conn)
        .await
        .map_err(|e| fail("fingerprint read", e))?;
    if let Some(other) = owner.filter(|o| o != org) {
        return Err(StepError::Permanent(format!(
            "tenant_meta is fingerprinted for a different org ({other}); this database is not ours"
        )));
    }
    sqlx::Executor::execute(&mut *conn, IDENTITY_CONTRACT_SQL)
        .await
        .map_err(|e| fail("apply", e))?;
    // The script's COMMIT must have ended the transaction the attestation was
    // scoped to; a still-visible setting would mean it did not.
    let leaked: Option<String> =
        sqlx::query_scalar("SELECT NULLIF(current_setting('app.identity_serving_contract', true), '')")
            .fetch_one(&mut *conn)
            .await
            .map_err(|e| fail("commit check", e))?;
    if leaked.is_some() {
        let _ = sqlx::Executor::execute(&mut *conn, "ROLLBACK").await;
        return Err(StepError::Transient("identity contract did not commit".into()));
    }
    Ok(())
}

async fn tenant_conn(deps: &Deps, job: &Job) -> Result<PgPool, StepError> {
    let dsn = deps
        .resolver
        .resolve_async(&job.db_secret_ref)
        .await
        .map_err(|e| StepError::Transient(format!("resolve tenant DSN: {e}")))?;
    PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_secs(15))
        .connect(&dsn)
        .await
        .map_err(|e| StepError::Transient(format!("connect tenant: {e}")))
}

async fn step_fingerprint(deps: &Deps, job: &Job) -> Result<Step, StepError> {
    let pool = tenant_conn(deps, job).await?;
    let result = fingerprint_in(&pool, &job.clerk_org_id).await;
    pool.close().await;
    result?;
    Ok(Step::ProvisionDek)
}

async fn fingerprint_in(pool: &PgPool, org: &str) -> Result<(), StepError> {
    sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, $1) ON CONFLICT (id) DO NOTHING")
        .bind(org.to_string())
        .execute(pool)
        .await
        .map_err(|e| StepError::Transient(format!("fingerprint insert: {e}")))?;
    let found: Option<String> = sqlx::query_scalar("SELECT org_id FROM tenant_meta WHERE id = true")
        .fetch_optional(pool)
        .await
        .map_err(|e| StepError::Transient(format!("fingerprint read: {e}")))?;
    match found {
        Some(f) if f == org => Ok(()),
        Some(f) => Err(StepError::Permanent(format!(
            "tenant_meta is fingerprinted for a different org ({f}); this database is not ours"
        ))),
        None => Err(StepError::Transient("fingerprint did not land".into())),
    }
}

async fn step_provision_dek(deps: &Deps, job: &Job) -> Result<Step, StepError> {
    let pool = deps
        .registry
        .get_or_build(&job.clerk_org_id, job.account_id, &job.db_secret_ref)
        .await
        .map_err(|e| match e {
            super::registry::TenantPoolError::FingerprintMismatch { .. }
            | super::registry::TenantPoolError::MissingFingerprint(_) => {
                StepError::Permanent(format!("tenant pool: {e}"))
            }
            other => StepError::Transient(format!("tenant pool: {other}")),
        })?;
    provision_org_dek_if_absent(&pool, job.account_id, &deps.kek)
        .await
        .map_err(|e| StepError::Transient(format!("provision DEK: {e}")))?;
    Ok(Step::Activate)
}

async fn step_activate(deps: &Deps, job: &Job) -> Result<Step, StepError> {
    let org = &job.clerk_org_id;
    let head = migrate::head_version();
    let row = routing::lookup_by_org(&deps.control, org)
        .await
        .map_err(|e| StepError::Transient(format!("routing: {e}")))?
        .ok_or_else(|| StepError::Permanent("routing row vanished before activate".into()))?;
    if let Some(v) = row.schema_version.filter(|v| *v > head) {
        // A NEWER build migrated this tenant and this one is older: going
        // back to `migrate` would fail on every attempt and end in cleanup
        // (deleting the customer's database). Park instead.
        return Err(ahead_of_build("activate", v, head));
    }
    if row.schema_version != Some(head) {
        // Behind this build's head (an older build ran `migrate`, and this
        // newer one took the job over): go back and migrate under this
        // build's head rather than activating a behind-head tenant the
        // harness would skip.
        tracing::info!(org = %org, have = ?row.schema_version, head, "schema_version behind this build; re-running migrate");
        return Ok(Step::Migrate);
    }
    routing::provisioning_activate(&deps.provisioner_control, org, head)
        .await
        .map_err(|e| match e.as_database_error().and_then(|d| d.code()).as_deref() {
            // A newer tree is already rolled out: park, never abandon (an
            // abandoned job's cleanup deletes the customer's database).
            Some("55000") => StepError::AwaitingNewerBuild(format!(
                "activate: {}",
                e.as_database_error().map(|d| d.message().to_string()).unwrap_or_default()
            )),
            _ => db_step_error("activate", e),
        })?;
    tracing::info!(org = %org, slug = %job.slug, "tenant provisioning complete; routing row active");
    Ok(Step::Done)
}

// ── Cleanup ───────────────────────────────────────────────────────────────────

/// Delete, in reverse creation order, exactly what this job created; then
/// mark the job failed and alert. Each delete converges, so a cleanup that
/// dies halfway resumes cleanly.
async fn run_cleanup(deps: &Deps, job: Job) -> Result<RunOutcome, sqlx::Error> {
    let org = job.clerk_org_id.clone();
    let reason = job.last_error.clone().unwrap_or_else(|| "unknown".into());
    let result = cleanup_resources(deps, &job).await;

    // Evict any pool the registry built for this org during the attempt.
    deps.registry.evict_org(&org).await;

    match result {
        Ok(released) => {
            // A deleted database is forgotten, create intent included: a
            // retried job (A2 resets the row to `verify_org`) must not take a
            // same-name database someone else creates later for the one this
            // job once asked for. The alert below still names it.
            if !mark_failed(deps, &org, &reason, released).await? {
                return Ok(RunOutcome::LeaseLost);
            }
            alert_terminal_failure(&job, &reason, true);
            Ok(RunOutcome::Failed { reason })
        }
        Err(StepError::Transient(e))
        | Err(StepError::AwaitingSubscription(e))
        | Err(StepError::AwaitingNewerBuild(e)) => {
            let attempts = job.attempts + 1;
            if attempts >= deps.policy.max_attempts {
                // Cleanup itself is stuck. Still terminal, still loud: the
                // row says what may be left behind.
                let why = format!("{reason} (cleanup incomplete after {attempts} attempts: {e})");
                if !mark_failed(deps, &org, &why, false).await? {
                    return Ok(RunOutcome::LeaseLost);
                }
                alert_terminal_failure(&job, &why, false);
                return Ok(RunOutcome::Failed { reason: why });
            }
            let delay = deps.policy.backoff(attempts);
            tracing::warn!(org = %org, attempts, error = %e, "tenant provisioning cleanup failed; will retry");
            if !defer(deps, &org, attempts, None, delay, None).await? {
                return Ok(RunOutcome::LeaseLost);
            }
            Ok(RunOutcome::Deferred { step: Step::Cleanup, attempts, reason: e })
        }
        Err(StepError::Permanent(e)) => {
            let why = format!("{reason} (cleanup refused: {e})");
            if !mark_failed(deps, &org, &why, false).await? {
                return Ok(RunOutcome::LeaseLost);
            }
            alert_terminal_failure(&job, &why, false);
            Ok(RunOutcome::Failed { reason: why })
        }
    }
}

/// What cleanup may do with this job's provider database.
enum DatabaseDisposal {
    /// The job never asked for one.
    None,
    /// Asked for, and none exists (any more).
    Gone,
    Delete(String),
    /// Left in place; the reason becomes part of the terminal error.
    Keep(String),
}

/// Decide before deleting anything. Only a database this job asked to create
/// (`database_create_requested_at`), on this provider, whose provider-side
/// NAME is this job's self-serve name (`database_name(slug)`, which the
/// provider must recognise as a self-serve tenant name), and that no other
/// org's fingerprint claims, is deleted. The name check runs before anything
/// connects, so a row whose `provider_database_id` points at somebody else's
/// project is refused even if that project will not take a connection.
///
/// A job that asked to create a database but never recorded one (the create
/// response was lost, or the process died before recording it) looks the
/// database up by name and disposes of it under the same checks, so an
/// abandoned job never leaks a billable project.
async fn database_disposal(deps: &Deps, job: &Job) -> Result<DatabaseDisposal, StepError> {
    let recorded = job.provider_database_id.as_deref();
    if job.database_create_requested_at.is_none() {
        return Ok(match recorded {
            None => DatabaseDisposal::None,
            Some(id) => DatabaseDisposal::Keep(format!(
                "database {id} is recorded without this job's create intent; left in place"
            )),
        });
    }
    if let Some(p) = job.provider.as_deref() {
        if p != deps.provisioner.provider_name() {
            return Err(StepError::Permanent(format!(
                "cleanup: database {} was created by provider {p:?}, this worker is {:?}",
                recorded.unwrap_or("-"),
                deps.provisioner.provider_name()
            )));
        }
    }
    let expected = deps.provisioner.database_name(&job.slug);
    if !deps.provisioner.is_selfserve_name(&expected) {
        return Ok(DatabaseDisposal::Keep(format!(
            "this job's database name {expected:?} is not a self-serve tenant name; nothing deleted"
        )));
    }
    let id = match recorded {
        Some(id) => id.to_string(),
        None => match deps.provisioner.find_by_name(&expected).await? {
            None => return Ok(DatabaseDisposal::Gone),
            Some(db) => {
                tracing::warn!(org = %job.clerk_org_id, database = %expected, provider_database_id = %db.provider_id,
                    "tenant provisioning cleanup found the job's unrecorded database by name");
                db.provider_id
            }
        },
    };
    let Some(actual) = deps.provisioner.name_of(&id).await? else {
        return Ok(DatabaseDisposal::Gone);
    };
    if actual != expected {
        tracing::error!(
            connector_health = true,
            org = %job.clerk_org_id,
            provider_database_id = %id,
            provider_name = %actual,
            expected_name = %expected,
            "tenant provisioning cleanup refused to delete a database that is not named for this job"
        );
        return Ok(DatabaseDisposal::Keep(format!(
            "database {id} is named {actual:?}, not this job's {expected:?}; left in place"
        )));
    }
    let Some(dsn) = deps.provisioner.connection_dsn(&id).await? else {
        return Ok(DatabaseDisposal::Gone);
    };
    match boxed(probe_owner(&dsn, &job.clerk_org_id)).await {
        Ok(DbOwner::Other(other)) => {
            tracing::error!(
                connector_health = true,
                org = %job.clerk_org_id,
                provider_database_id = %id,
                fingerprinted_for = %other,
                "tenant provisioning cleanup refused to delete a database fingerprinted for another org"
            );
            Ok(DatabaseDisposal::Keep(format!(
                "database {id} is fingerprinted for a different org ({other}); left in place"
            )))
        }
        Ok(_) => Ok(DatabaseDisposal::Delete(id)),
        // It exists, carries this job's name (checked above), but will not
        // take a connection (a compute still coming up, say). Nothing could
        // have fingerprinted it through this job; leaving it would leak it.
        Err(ProbeError::Connect(e)) => {
            tracing::warn!(org = %job.clerk_org_id, provider_database_id = %id, error = %e,
                "tenant provisioning cleanup could not read the fingerprint of its own database; deleting it anyway");
            Ok(DatabaseDisposal::Delete(id))
        }
        Err(e) => Err(e.into()),
    }
}

/// The deletes themselves: routing row, then credential, then the provider
/// database — the reverse of the order they were created in. `Ok(true)` when
/// the job's own database is gone, so the row can forget it.
///
/// The credential delete is unconditional: `credential_stored_at` is
/// recorded AFTER the store, so a crash between the two leaves a stored
/// credential the row does not mention. The delete is idempotent, the ref is
/// unique to this job, and the definer refuses while any routing row still
/// references it.
async fn cleanup_resources(deps: &Deps, job: &Job) -> Result<bool, StepError> {
    let org = &job.clerk_org_id;
    let disposal = database_disposal(deps, job).await?;
    if job.routing_inserted_at.is_some() {
        routing::provisioning_delete_routing(&deps.provisioner_control, org)
            .await
            .map_err(|e| db_step_error("cleanup: delete routing row", e))?;
    }
    credentials::delete(&deps.provisioner_control, &job.db_secret_ref)
        .await
        .map_err(|e| match e {
            CredentialError::Db(e) => db_step_error("cleanup: delete credential", e),
            other => StepError::Permanent(format!("cleanup: delete credential: {other}")),
        })?;
    match disposal {
        DatabaseDisposal::None => Ok(false),
        DatabaseDisposal::Gone => Ok(true),
        DatabaseDisposal::Delete(id) => match deps.provisioner.delete(&id).await? {
            DeleteOutcome::Deleted | DeleteOutcome::AlreadyGone => Ok(true),
        },
        DatabaseDisposal::Keep(why) => Err(StepError::Permanent(why)),
    }
}

/// The alertable shape (`connector_health = true`, see
/// `scripts/check_silent_drop_is_alertable.sh`): one `error!` per terminal
/// failure, carrying the org, slug, the step it died on, and the reason.
/// Never a DSN or key — `reason` is built only from step messages that
/// exclude them.
pub fn alert_terminal_failure(job: &Job, reason: &str, cleaned_up: bool) {
    tracing::error!(
        connector_health = true,
        org = %job.clerk_org_id,
        slug = %job.slug,
        step = %job.step,
        attempts = job.attempts,
        cleaned_up,
        provider = job.provider.as_deref().unwrap_or("-"),
        provider_database_id = job.provider_database_id.as_deref().unwrap_or("-"),
        reason = %reason,
        "tenant provisioning failed terminally"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn steps_round_trip_through_their_stored_names() {
        for s in Step::FORWARD.iter().chain([Step::Done, Step::Cleanup].iter()) {
            assert_eq!(Step::parse(s.as_str()), Some(*s));
        }
        assert_eq!(Step::parse("bogus"), None);
        assert_eq!(Step::FORWARD[0], Step::VerifyOrg);
        assert_eq!(Step::FORWARD[4], Step::Contract);
        assert_eq!(Step::FORWARD[7], Step::Activate);
    }

    #[test]
    fn secret_ref_matches_the_shell_derivation() {
        assert_eq!(secret_ref_for_slug("acme-corp"), "TENANT_DB_ACME_CORP");
        assert_eq!(secret_ref_for_slug("a1"), "TENANT_DB_A1");
        use super::super::provisioner::{LocalPgProvisioner, NeonProvisioner, TenantDbProvisioner};
        assert_eq!(NeonProvisioner::new("k").database_name("acme-corp"), "coralswarm-tenant-acme-corp");
        assert_eq!(LocalPgProvisioner::new("postgres://x@localhost/postgres").database_name("acme-corp"), "coralswarm_tenant_acme_corp");
    }

    #[test]
    fn backoff_is_exponential_and_capped() {
        let p = RetryPolicy {
            max_attempts: 8,
            base_backoff: Duration::from_secs(30),
            max_backoff: Duration::from_secs(600),
            lease_ttl: Duration::from_secs(60),
            ..RetryPolicy::default()
        };
        assert_eq!(p.backoff(0), Duration::from_secs(30));
        assert_eq!(p.backoff(1), Duration::from_secs(60));
        assert_eq!(p.backoff(3), Duration::from_secs(240));
        assert_eq!(p.backoff(10), Duration::from_secs(600));
        assert_eq!(p.backoff(100), Duration::from_secs(600));
    }

    /// SF-2: the version park alerts at about an hour, then hourly; the 24 h
    /// wait only escalates it. It never stops checking.
    #[test]
    fn the_version_park_alerts_after_an_hour_then_hourly() {
        let p = RetryPolicy::default();
        let h = Duration::from_secs(3600);
        assert_eq!(p.version_park(Duration::ZERO), (p.version_poll, ParkAlert::Quiet));
        assert_eq!(p.version_park(h - Duration::from_secs(1)), (p.version_poll, ParkAlert::Quiet));
        assert!(p.version_alert_after <= h, "first alert by about an hour, not a day");
        assert_eq!(p.version_park(h), (h, ParkAlert::Alert));
        assert_eq!(p.version_park(h * 5), (h, ParkAlert::Alert));
        assert_eq!(p.version_park(h * 24), (h, ParkAlert::Escalated));
        assert_eq!(p.version_park(h * 24 * 30), (h, ParkAlert::Escalated), "still checking, hourly");
        assert_eq!(p.version_wait, h * 24);
    }

    #[test]
    fn the_subscription_wait_outlasts_async_payment_settlement() {
        let p = RetryPolicy::default();
        assert!(p.subscription_wait >= Duration::from_secs(7 * 24 * 3600));
        // And it is not the attempts budget: that runs out in hours.
        let budget: u64 = (1..p.max_attempts).map(|a| p.backoff(a).as_secs()).sum();
        assert!(Duration::from_secs(budget) < p.subscription_wait);
    }

    #[test]
    fn opaque_slug_and_ref_are_the_shapes_0381_derives() {
        let id = Uuid::parse_str("0123abcd-4567-89ef-0123-456789abcdef").unwrap();
        let slug = opaque_slug_for_account(id);
        assert_eq!(slug, "o-0123abcd456789ef");
        assert_eq!(secret_ref_for_slug(&slug), "TENANT_DB_O_0123ABCD456789EF");
    }

    #[test]
    fn flag_is_off_unless_set() {
        // Never mutate the real env here; the name is the contract.
        assert_eq!(SELFSERVE_SIGNUP_FLAG, "ORG_SELFSERVE_SIGNUP_ENABLED");
        if std::env::var_os(SELFSERVE_SIGNUP_FLAG).is_none() {
            assert!(!selfserve_signup_enabled());
        }
    }

    #[test]
    fn terminal_failure_alert_carries_the_alertable_field() {
        use std::sync::{Arc, Mutex};
        use tracing_subscriber::fmt::MakeWriter;

        #[derive(Clone, Default)]
        struct Buf(Arc<Mutex<Vec<u8>>>);
        impl std::io::Write for Buf {
            fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(b);
                Ok(b.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        impl<'a> MakeWriter<'a> for Buf {
            type Writer = Buf;
            fn make_writer(&'a self) -> Buf {
                self.clone()
            }
        }
        let buf = Buf::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(buf.clone())
            .with_ansi(false)
            .finish();
        let job = Job {
            clerk_org_id: "org_x".into(),
            account_id: Uuid::nil(),
            slug: "acme".into(),
            db_secret_ref: "TENANT_DB_ACME".into(),
            region: None,
            step: "cleanup".into(),
            status: "provisioning".into(),
            attempts: 3,
            last_error: None,
            next_attempt_at: Utc::now(),
            lease_owner: None,
            lease_expires_at: None,
            provider: Some("neon".into()),
            provider_database_id: Some("proj-1".into()),
            database_create_requested_at: Some(Utc::now()),
            credential_stored_at: None,
            routing_inserted_at: None,
            cancel_requested_at: None,
            subscription_wait_started_at: None,
            version_wait_started_at: None,
            clerk_org_cleanup: "none".into(),
            failed_at: None,
            ready_at: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        tracing::subscriber::with_default(subscriber, || {
            alert_terminal_failure(&job, "step migrate: boom", true);
        });
        let out = String::from_utf8(buf.0.lock().unwrap().clone()).unwrap();
        assert!(out.contains("ERROR"), "{out}");
        assert!(out.contains("connector_health=true"), "{out}");
        assert!(out.contains("org_x") && out.contains("proj-1") && out.contains("boom"), "{out}");
    }
}

