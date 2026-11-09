//! In-app ORG deletion (`docs/plans/org-offboarding-design.md`; founder
//! decisions 2026-10-27 items 4, 8, 11, 12, 13). The resumable job behind
//! `POST /v1/orgs/{org_id}/deletion`, the 30-days-unpaid sweep and the
//! Clerk-dashboard adoption sweep. Modelled on [`super::account_deletion`]:
//! one `org_deletions` row (control 0387) is the job, its checkpoint and,
//! once done, the tombstone.
//!
//! # State machine
//!
//! ```text
//!   owner POST (session, fresh re-verification, typed org name)
//!   | 30 days unpaid (system)  | org deleted in the Clerk dashboard (system)
//!   v
//! requested --trial_end, stripe_cancel, notify_requested, notify_members--> grace (7 days)
//!   members locked out NOW; the owner may export or undo during grace
//!                                                  |            |
//!                                     undo (owner) |            | grace over
//!                                                  v            v
//!                                              canceled   approval needed? --yes--> awaiting_approval
//!                                                               | no                   | operator approves
//!                                                               v                      v
//!                                                           destroying <---------------+
//!   stripe_sweep, revoke_access, manifest, export, routing, credential, database, clerk, notify_done
//!                                                               |
//!                                                               v
//!                                                             done (tombstone)
//! ```
//!
//! | phase | step | what |
//! |---|---|---|
//! | requested | `trial_end` | a live self-serve trial is ended: `trialing`/`converting` -> `canceled` (its unused credits are expired, i.e. forfeited) |
//! | requested | `stripe_cancel` | the subscription ends NOW: `prorate=false`, `invoice_now=false`, keyed `org-deleted-cancel:{sub}` (the same key #1538's `organization.deleted` handler uses), read-first so a replay never writes twice; extra usage and auto-reload off |
//! | requested | `notify_requested` | email the requester (best effort) |
//! | requested | `notify_members` | email every other member: the org is being deleted, access has ended, their private notes in it are deleted on the grace end date unless the owner undoes it. One email per member per deletion (`members_notified` ledger + Resend idempotency key) |
//! | destroying | `stripe_sweep` | every subscription the Stripe CUSTOMER has, whatever our mirror says, is ended now, BEFORE anything is destroyed (a Stripe outage parks the deletion while the org is still whole) (keyed, `prorate=false`): a Checkout that completed after the request cannot leave billing live |
//! | destroying | `revoke_access` | `clerk_provision::revoke_org_access`: `org_revocations`, membership tombstones, member-ocean archive intents, memberships dropped. Members' own captures in their PERSONAL accounts are untouched |
//! | destroying | `manifest` | per-source-type counts on the row; the full `(source_type, source_id)` list goes into the export |
//! | destroying | `export` | the whole tenant as NDJSON, encrypted under a fresh key, uploaded; the key wrapped under the KEK; kept 30 days |
//! | destroying | `routing` | the active routing row deleted (control 0387 definer, provisioner role only) |
//! | destroying | `credential` | the encrypted DSN deleted (0381 definer, provisioner role only) |
//! | destroying | `database` | the tenant database deleted through the provisioner, only after its provider-side NAME is this org's self-serve name and its fingerprint is this org |
//! | destroying | `clerk` | the Clerk org deleted (404 = done). Clerk then sends `organization.deleted`, which finds everything already done |
//! | destroying | `notify_done` | email the requester; the row becomes the `done` tombstone |
//!
//! Every step is idempotent and checkpointed in `completed_steps`; a crash
//! anywhere re-runs the step it was in and finds its effect already there.
//!
//! # What undo restores, and what it does not
//!
//! Undo (owner, during `grace`) restores ACCESS: the gate stops refusing and
//! nothing else changed. It does NOT restore the Stripe subscription: that
//! was ended for good at request time, so the org is unpaid and must
//! resubscribe from Billing. An undone UNPAID deletion gets
//! [`UNPAID_UNDO_WINDOW_DAYS`] to pay before the unpaid sweep re-enters it.
//! Undo is refused while still `requested` (the Stripe cancel has not
//! finished yet, so undoing then could leave the subscription live).
//!
//! # Interaction with `organization.deleted` (#1538)
//!
//! The `clerk` step deletes the Clerk org, so Clerk then delivers
//! `organization.deleted`. On main that runs `revoke_org_access` (already
//! done: its writes are upserts); with #1538 it also ends the trial (already
//! ended: a guarded state UPDATE finds nothing) and cancels the subscription
//! keyed `org-deleted-cancel:{sub}` (already canceled: no Stripe write). The
//! webhook can also arrive MID-FLOW (an operator deletes the org in the
//! Clerk dashboard during grace): access is revoked early, undo becomes
//! impossible (the owner is no longer a member), and the job carries on to
//! the same destroy step, where `revoke_access` converges and the Clerk
//! delete answers 404.
//!
//! # Prod runs as `coralswarm_app`
//!
//! Every write to `org_deletions` is account-scoped (RLS) or one of 0387's
//! definers. The two writes the serving role may never make, the routing row
//! and the credential, go through the worker's provisioner pool
//! (`PROVISIONER_DATABASE_URL`, a `coralswarm_provisioner` login), as in A1.
//!
//! # Flag
//!
//! `ORG_DELETION_ENABLED` (unset = off): the routes answer 404, the worker
//! idles, the gate check is a no-op (no row can exist).

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use uuid::Uuid;

use crate::billing::cancel::CancelOutcome;
use crate::billing::stripe_client::StripeClient;
use crate::tenancy::provisioner::{DeleteOutcome, TenantDbProvisioner};

// ── Flag and policy ──────────────────────────────────────────────────────

pub const FLAG: &str = "ORG_DELETION_ENABLED";

/// `ORG_DELETION_ENABLED` is `1`/`true`. Unset = off.
pub fn enabled() -> bool {
    std::env::var(FLAG)
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

/// Grace before anything is destroyed (decision 11, same as account deletion).
pub const GRACE_DAYS: i32 = 7;
/// How long the export (and its key) is kept after it is written (decision 13).
pub const EXPORT_RETENTION_DAYS: i64 = 30;
/// An org unpaid this long enters this flow (decision 4).
pub const UNPAID_DAYS: i32 = 30;
/// An undone unpaid deletion re-enters after this long if still unpaid.
pub const UNPAID_UNDO_WINDOW_DAYS: i64 = 7;
/// Clerk-dashboard deletes revoked longer ago than this are not adopted.
pub const ADOPT_CLERK_DELETES_DAYS: i32 = 30;
/// Operator approval is required for the first N completed prod deletions
/// (decision 13). `ORG_DELETION_APPROVAL_FIRST_N` relaxes it (0 = never).
pub const DEFAULT_APPROVAL_FIRST_N: i64 = 10;
pub const APPROVAL_FIRST_N_ENV: &str = "ORG_DELETION_APPROVAL_FIRST_N";
pub const LEASE_SECS: i32 = 120;
const RUN_BUDGET: Duration = Duration::from_secs(90);
/// Re-check interval while waiting on something outside the job.
const WAIT_SECS: u64 = 600;

pub fn approval_first_n_from_env() -> i64 {
    std::env::var(APPROVAL_FIRST_N_ENV)
        .ok()
        .and_then(|v| v.trim().parse::<i64>().ok())
        .filter(|n| *n >= 0)
        .unwrap_or(DEFAULT_APPROVAL_FIRST_N)
}

// ── Steps ────────────────────────────────────────────────────────────────

/// The ordered steps. Mirrors `org_deletions_steps_known` in 0387.
pub const STEPS: [&str; 13] = [
    "trial_end",
    "stripe_cancel",
    "notify_requested",
    "notify_members",
    "stripe_sweep",
    "revoke_access",
    "manifest",
    "export",
    "routing",
    "credential",
    "database",
    "clerk",
    "notify_done",
];

/// Steps before this index run in `requested`; the rest only in `destroying`.
pub const FIRST_DESTRUCTIVE_STEP: usize = 4;

pub fn is_destructive(step: &str) -> bool {
    STEPS.iter().position(|s| *s == step).is_some_and(|i| i >= FIRST_DESTRUCTIVE_STEP)
}

/// The Stripe `Idempotency-Key` for ending a deleted org's subscription.
/// Deliberately the SAME string #1538's `organization.deleted` handler uses
/// (`billing::cancel::deleted_org_cancel_key` there), so the two paths share
/// one replay slot at Stripe. Fold into that function once #1538 is on main.
pub fn deleted_org_cancel_key(subscription_id: &str) -> String {
    format!("org-deleted-cancel:{subscription_id}")
}

// ── Rows ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct OrgDeletion {
    pub id: Uuid,
    pub account_id: Uuid,
    pub clerk_org_id: String,
    pub initiated_by: String,
    pub requested_by_user_id: Option<Uuid>,
    pub state: String,
    pub completed_steps: Vec<String>,
    pub grace_ends_at: DateTime<Utc>,
    pub approval_required: Option<bool>,
    pub approved_at: Option<DateTime<Utc>>,
    pub approved_by: Option<String>,
    pub trial_outcome: Option<String>,
    pub stripe_outcome: Option<String>,
    pub source_manifest: Option<Value>,
    pub export_object_key: Option<String>,
    pub export_key_wrapped: Option<Vec<u8>>,
    pub export_sha256: Option<String>,
    pub export_bytes: Option<i64>,
    pub export_expires_at: Option<DateTime<Utc>>,
    pub export_destroyed_at: Option<DateTime<Utc>>,
    pub provider: Option<String>,
    pub provider_database_id: Option<String>,
    pub database_name: Option<String>,
    pub attempts: i32,
    pub last_error: Option<String>,
    pub next_attempt_at: DateTime<Utc>,
    pub lease_until: Option<DateTime<Utc>>,
    /// Fencing token minted by `org_deletion_claim`.
    pub lease_id: Option<Uuid>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub destroying_since: Option<DateTime<Utc>>,
    pub done_at: Option<DateTime<Utc>>,
    pub canceled_at: Option<DateTime<Utc>>,
    pub canceled_by: Option<String>,
    /// Members already emailed by `notify_members` (the idempotency ledger).
    pub members_notified: Vec<Uuid>,
    /// When the deletion entered `awaiting_approval`.
    pub awaiting_approval_since: Option<DateTime<Utc>>,
    /// When the operator was last alerted that approval is overdue.
    pub approval_alerted_at: Option<DateTime<Utc>>,
}

impl OrgDeletion {
    pub fn next_step(&self) -> Option<&'static str> {
        STEPS.iter().copied().find(|s| !self.completed_steps.iter().any(|d| d == s))
    }

    pub fn has(&self, step: &str) -> bool {
        self.completed_steps.iter().any(|d| d == step)
    }

    /// Undo is the owner's, during grace only.
    pub fn can_cancel(&self) -> bool {
        self.state == "grace" && self.initiated_by != "clerk_dashboard"
    }

    /// The status shape. Never the wrapped key or the object key. Who asked
    /// (`initiated_by`) and what happened to the subscription are the owner's
    /// to know, not a locked-out member's: they are present only when
    /// `is_owner`.
    pub fn status_json(&self, is_owner: bool) -> Value {
        let mut v = json!({
            "id": self.id,
            "state": self.state,
            "grace_ends_at": self.grace_ends_at,
            "can_cancel": is_owner && self.can_cancel(),
            "export_available": is_owner && matches!(self.state.as_str(), "requested" | "grace"),
            "approval_required": self.approval_required,
            "approved": self.approved_at.is_some(),
            "current_step": self.next_step(),
            "retrying": self.last_error.is_some()
                && matches!(self.state.as_str(), "requested" | "destroying"),
            "created_at": self.created_at,
            "done_at": self.done_at,
            "canceled_at": self.canceled_at,
        });
        if is_owner {
            v["initiated_by"] = json!(self.initiated_by);
            v["subscription"] = json!(self.stripe_outcome);
        }
        v
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Initiator {
    Owner,
    Unpaid,
    ClerkDashboard,
}

impl Initiator {
    pub fn as_str(self) -> &'static str {
        match self {
            Initiator::Owner => "owner",
            Initiator::Unpaid => "unpaid",
            Initiator::ClerkDashboard => "clerk_dashboard",
        }
    }
}

fn db_err(e: impl std::fmt::Display) -> String {
    format!("database error: {e}")
}

async fn scoped(control: &PgPool, account: Uuid) -> Result<sqlx::Transaction<'static, sqlx::Postgres>, sqlx::Error> {
    crate::tenancy::scope::begin_request_account_read(control, account)
        .await
        .map_err(|e| sqlx::Error::Protocol(e.to_string()))
}

// ── What the request path knows about an org ─────────────────────────────

/// The caller's relationship to an org, read from the control plane only
/// (never through the tenant gate).
#[derive(Debug, Clone)]
pub struct OrgFacts {
    pub account_id: Uuid,
    pub clerk_org_id: String,
    /// `account_members.role` of the caller; `None` when not a member.
    pub role: Option<String>,
    /// The in-app owner: role `owner`, or the admin who created the org
    /// (`accounts.owner_user_id`; a self-serve creator is `admin` in
    /// `account_members`). Never just any admin.
    pub is_owner: bool,
    pub personal: bool,
    pub revoked: bool,
    /// A `ready` self-serve provisioning job: the only kind of org whose
    /// database this flow may delete. Concierge orgs are refused.
    pub self_serve_ready: bool,
}

/// Drop an account's unpaid clock. The SQL function re-applies the table's RLS
/// (it only acts on the entered account), so the worker enters the account in
/// a transaction first.
pub(crate) async fn unpaid_forget(control: &PgPool, account: Uuid) -> Result<i32, sqlx::Error> {
    let mut tx = scoped(control, account).await?;
    let n: i32 = sqlx::query_scalar("SELECT org_deletion_unpaid_forget($1)").bind(account).fetch_one(&mut *tx).await?;
    tx.commit().await?;
    Ok(n)
}

pub async fn org_facts(control: &PgPool, clerk_org_id: &str, user_id: Uuid) -> Result<Option<OrgFacts>, sqlx::Error> {
    let row: Option<(Uuid, Uuid, Option<String>, bool, bool, bool)> = sqlx::query_as(
        "SELECT a.id, a.owner_user_id, am.role, \
                EXISTS (SELECT 1 FROM personal_orgs p WHERE p.clerk_org_id = a.clerk_org_id), \
                EXISTS (SELECT 1 FROM org_revocations r WHERE r.clerk_org_id = a.clerk_org_id), \
                EXISTS (SELECT 1 FROM tenant_provisioning_jobs j \
                         WHERE j.clerk_org_id = a.clerk_org_id AND j.account_id = a.id AND j.status = 'ready') \
           FROM accounts a \
           LEFT JOIN account_members am ON am.account_id = a.id AND am.user_id = $2 \
          WHERE a.clerk_org_id = $1",
    )
    .bind(clerk_org_id)
    .bind(user_id)
    .fetch_optional(crate::tenancy::scope::request_read(control))
    .await?;
    Ok(row.map(|(account_id, owner_user_id, role, personal, revoked, self_serve_ready)| {
        let is_owner = match role.as_deref() {
            Some("owner") => true,
            Some("admin") => owner_user_id == user_id,
            _ => false,
        };
        OrgFacts {
            account_id,
            clerk_org_id: clerk_org_id.to_string(),
            role,
            is_owner,
            personal,
            revoked,
            self_serve_ready,
        }
    }))
}

/// The live (not canceled) deletion of an org account, if any.
pub async fn live_for_account(control: &PgPool, account_id: Uuid) -> Result<Option<OrgDeletion>, sqlx::Error> {
    let mut tx = scoped(control, account_id).await?;
    let row = sqlx::query_as::<_, OrgDeletion>(
        "SELECT * FROM org_deletions WHERE account_id = $1 AND state <> 'canceled'",
    )
    .bind(account_id)
    .fetch_optional(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(row)
}

/// `org_deletion_state_for_org` (0387): the gate's read.
pub async fn state_for_org(control: &PgPool, clerk_org_id: &str) -> Result<Option<String>, sqlx::Error> {
    sqlx::query_scalar("SELECT org_deletion_state_for_org($1)")
        .bind(clerk_org_id)
        .fetch_one(control)
        .await
}

/// Whether a live deletion state locks members out of the org (the same four
/// states `live_org_membership` refuses on).
pub fn locks_members_out(state: &str) -> bool {
    matches!(state, "requested" | "grace" | "awaiting_approval" | "destroying")
}

/// Whether an org is being closed down: a live deletion that locks members
/// out, or already revoked. Side effects that reach people or grant access
/// on the org's behalf (seats, invite emails) stop while this is true.
/// `Err` is the caller's to treat as "closing" (fail closed).
pub async fn org_closing_down(control: &PgPool, clerk_org_id: &str) -> Result<bool, sqlx::Error> {
    if state_for_org(control, clerk_org_id).await?.is_some_and(|st| locks_members_out(&st)) {
        return Ok(true);
    }
    crate::tenancy::routing::org_is_revoked(control, clerk_org_id).await
}

/// [`org_closing_down`] for an account id. A personal account (no org) is
/// never closing. Fails closed on a read error.
pub async fn account_org_closing_down(control: &PgPool, account_id: Uuid) -> bool {
    let org: Result<Option<Option<String>>, _> = sqlx::query_scalar("SELECT clerk_org_id FROM accounts WHERE id = $1")
        .bind(account_id)
        .fetch_optional(control)
        .await;
    match org {
        Ok(Some(Some(org))) => org_closing_down(control, &org).await.unwrap_or(true),
        Ok(_) => false,
        Err(_) => true,
    }
}

/// The refusal every non-deletion request bound to a deleting org gets.
pub fn lockout_code(state: &str) -> Option<&'static str> {
    match state {
        "requested" | "grace" => Some("org_deletion_pending"),
        "awaiting_approval" | "destroying" => Some("org_deletion_in_progress"),
        // `done`: the org is revoked; the membership check refuses first.
        _ => None,
    }
}

/// Same predicate as `routes::account_deletion`'s "may type the email" rule,
/// for the org name: trimmed, case-insensitive, never empty.
pub fn name_matches(typed: &str, name: &str) -> bool {
    let n = name.trim();
    !n.is_empty() && typed.trim().eq_ignore_ascii_case(n)
}

#[derive(Debug, thiserror::Error)]
pub enum RequestError {
    /// Not a member (or no such org): indistinguishable on purpose.
    #[error("organization not found")]
    NotFound,
    #[error("only the organization's owner can delete it")]
    NotOwner,
    #[error("a personal workspace is deleted with the account, not here")]
    PersonalOrg,
    #[error("the organization is already deleted")]
    Revoked,
    #[error("this organization was set up by CoralSwarm; contact support to delete it")]
    NotSelfServe,
    #[error("the typed name does not match the organization")]
    NameMismatch,
    #[error("database error: {0}")]
    Db(#[from] sqlx::Error),
}

#[derive(Debug)]
pub struct Started {
    pub deletion: OrgDeletion,
    /// False when a live deletion already existed (idempotent re-request).
    pub created: bool,
}

/// The owner asks for the org to be deleted. Idempotent: a second call
/// returns the live deletion unchanged. The interactive-session and
/// re-verification checks are the route's; this re-checks everything that
/// depends on data, inside the inserting transaction.
pub async fn request_by_owner(
    control: &PgPool,
    user_id: Uuid,
    clerk_org_id: &str,
    typed_name: &str,
    org_name: &str,
) -> Result<Started, RequestError> {
    let facts = org_facts(control, clerk_org_id, user_id).await?.ok_or(RequestError::NotFound)?;
    if facts.role.is_none() {
        return Err(RequestError::NotFound);
    }
    if facts.personal {
        return Err(RequestError::PersonalOrg);
    }
    if facts.revoked {
        return Err(RequestError::Revoked);
    }
    if !facts.is_owner {
        return Err(RequestError::NotOwner);
    }
    if let Some(existing) = live_for_account(control, facts.account_id).await? {
        return Ok(Started { deletion: existing, created: false });
    }
    if !facts.self_serve_ready {
        return Err(RequestError::NotSelfServe);
    }
    if !name_matches(typed_name, org_name) {
        return Err(RequestError::NameMismatch);
    }
    insert(control, facts.account_id, clerk_org_id, Initiator::Owner, Some(user_id)).await.map_err(RequestError::Db)
}

/// Insert a `requested` row, or return the live one. Serialised per org.
pub async fn insert(
    control: &PgPool,
    account_id: Uuid,
    clerk_org_id: &str,
    initiator: Initiator,
    requested_by: Option<Uuid>,
) -> Result<Started, sqlx::Error> {
    let mut tx = scoped(control, account_id).await?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended('org_deletion:' || $1, 0))")
        .bind(clerk_org_id)
        .execute(&mut *tx)
        .await?;
    if let Some(existing) = sqlx::query_as::<_, OrgDeletion>(
        "SELECT * FROM org_deletions WHERE account_id = $1 AND state <> 'canceled'",
    )
    .bind(account_id)
    .fetch_optional(&mut *tx)
    .await?
    {
        tx.commit().await?;
        return Ok(Started { deletion: existing, created: false });
    }
    let deletion = sqlx::query_as::<_, OrgDeletion>(
        "INSERT INTO org_deletions (account_id, clerk_org_id, initiated_by, requested_by_user_id, grace_ends_at) \
         VALUES ($1, $2, $3, $4, now() + make_interval(days => $5)) RETURNING *",
    )
    .bind(account_id)
    .bind(clerk_org_id)
    .bind(initiator.as_str())
    .bind(requested_by)
    .bind(GRACE_DAYS)
    .fetch_one(&mut *tx)
    .await?;
    tx.commit().await?;
    tracing::warn!(
        deletion_id = %deletion.id, org = %clerk_org_id, %account_id, initiated_by = initiator.as_str(),
        "organization deletion requested; members are locked out"
    );
    Ok(Started { deletion, created: true })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelResult {
    Canceled,
    /// Still `requested`: the subscription cancel has not finished. Retry in
    /// a few seconds.
    NotYet,
    /// Past grace: no way back.
    TooLate,
    /// The org was deleted in Clerk (an `org_revocations` row): the deletion
    /// can never be undone, whatever its state. #1538 leaves billing to this
    /// flow in that case, so a cancel would strand a live subscription.
    RevokedInClerk,
    NotFound,
}

/// Undo by the owner. Guarded on `grace`, so a cancel racing the worker's
/// end-of-grace transition has exactly one winner. Restores access only;
/// the subscription stays ended (see the module doc).
pub async fn cancel_by_owner(control: &PgPool, user_id: Uuid, clerk_org_id: &str) -> Result<CancelResult, sqlx::Error> {
    let Some(facts) = org_facts(control, clerk_org_id, user_id).await? else {
        return Ok(CancelResult::NotFound);
    };
    if facts.role.is_none() {
        return Ok(CancelResult::NotFound);
    }
    if !facts.is_owner {
        return Ok(CancelResult::NotFound);
    }
    let Some(d) = live_for_account(control, facts.account_id).await? else {
        return Ok(CancelResult::NotFound);
    };
    if facts.revoked {
        return Ok(CancelResult::RevokedInClerk);
    }
    if d.initiated_by == "clerk_dashboard" {
        return Ok(CancelResult::TooLate);
    }
    match d.state.as_str() {
        "requested" => return Ok(CancelResult::NotYet),
        "grace" => {}
        _ => return Ok(CancelResult::TooLate),
    }
    cancel_row(control, &d, "owner", &["grace"]).await
}

/// Operator cancel (owner-level DSN): also allowed while awaiting approval.
pub async fn cancel_by_operator(control: &PgPool, deletion_id: Uuid, operator: &str) -> Result<CancelResult, sqlx::Error> {
    let d: Option<OrgDeletion> = sqlx::query_as("SELECT * FROM org_deletions WHERE id = $1")
        .bind(deletion_id)
        .fetch_optional(control)
        .await?;
    let Some(d) = d else { return Ok(CancelResult::NotFound) };
    cancel_row(control, &d, &format!("operator:{operator}"), &["grace", "awaiting_approval"]).await
}

/// What `org_deletion_admin cancel` prints after the result. For an
/// UNPAID-origin deletion that was just canceled it says the org goes back
/// into deletion if it is still unpaid when the undo window closes, and
/// when that is (the watch row's `not_before`, set by the cancel itself).
pub async fn operator_cancel_message(control: &PgPool, deletion_id: Uuid, result: CancelResult) -> Result<String, sqlx::Error> {
    let base = format!("{result:?}");
    if result != CancelResult::Canceled {
        return Ok(base);
    }
    let row: Option<(String, Option<DateTime<Utc>>)> = sqlx::query_as(
        "SELECT d.initiated_by, w.not_before FROM org_deletions d \
           LEFT JOIN org_unpaid_watch w ON w.account_id = d.account_id WHERE d.id = $1",
    )
    .bind(deletion_id)
    .fetch_optional(control)
    .await?;
    Ok(match row {
        Some((by, not_before)) if by == "unpaid" => {
            let when = not_before.unwrap_or_else(|| Utc::now() + chrono::Duration::days(UNPAID_UNDO_WINDOW_DAYS));
            unpaid_cancel_message(&base, when)
        }
        _ => base,
    })
}

/// The line itself, split out so a test can pin the words.
pub fn unpaid_cancel_message(base: &str, reenters_after: DateTime<Utc>) -> String {
    format!(
        "{base}\nNOTE: this deletion started because the org was unpaid. Unless it pays before then, the org re-enters deletion after {} ({UNPAID_UNDO_WINDOW_DAYS} days from the cancel) if it is still unpaid.",
        reenters_after.format("%Y-%m-%d %H:%M UTC"),
    )
}

async fn cancel_row(control: &PgPool, d: &OrgDeletion, by: &str, from: &[&str]) -> Result<CancelResult, sqlx::Error> {
    if crate::tenancy::routing::org_is_revoked(control, &d.clerk_org_id).await? {
        return Ok(CancelResult::RevokedInClerk);
    }
    let mut tx = scoped(control, d.account_id).await?;
    let n = sqlx::query(
        "UPDATE org_deletions SET state = 'canceled', canceled_at = now(), canceled_by = $2, \
                lease_until = NULL, updated_at = now() \
          WHERE id = $1 AND state = ANY($3)",
    )
    .bind(d.id)
    .bind(by)
    .bind(from)
    .execute(&mut *tx)
    .await?
    .rows_affected();
    if n == 1 && d.initiated_by == "unpaid" {
        sqlx::query(
            "UPDATE org_unpaid_watch SET not_before = now() + make_interval(days => $2), updated_at = now() \
              WHERE account_id = $1",
        )
        .bind(d.account_id)
        .bind(UNPAID_UNDO_WINDOW_DAYS as i32)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    if n == 1 {
        tracing::warn!(deletion_id = %d.id, org = %d.clerk_org_id, by, "organization deletion canceled; access restored, subscription stays ended");
        Ok(CancelResult::Canceled)
    } else {
        Ok(CancelResult::TooLate)
    }
}

/// Operator approval (`org_deletion_approve`, 0387). Only a role that owns
/// `org_deletions` gets past the approval trigger; the serving role is
/// refused with 42501.
pub async fn approve(owner_pool: &PgPool, deletion_id: Uuid, operator: &str) -> Result<String, sqlx::Error> {
    sqlx::query_scalar("SELECT org_deletion_approve($1, $2)")
        .bind(deletion_id)
        .bind(operator)
        .fetch_one(owner_pool)
        .await
}

// ── Dependencies the steps call out to ───────────────────────────────────

/// Reads of the org's tenant database while it still exists.
#[async_trait]
pub trait OrgDataPlane: Send + Sync {
    /// Per-source-type counts (`{"atoms": n, "by_source_type": {...}}`).
    async fn source_manifest(&self, d: &OrgDeletion) -> Result<Value, String>;
    /// An export as NDJSON bytes (plaintext; the caller encrypts). The
    /// [`ExportScope`] decides WHOSE data: the owner's own download carries
    /// only what that owner may see; the operator's 30-day copy, built by the
    /// worker, is the whole tenant.
    async fn export_bundle(&self, d: &OrgDeletion, scope: &ExportScope) -> Result<Vec<u8>, String>;
    /// Drop any cached pool / DEK for the org, before its database goes.
    async fn forget_tenant(&self, clerk_org_id: &str, account_id: Uuid);
}

/// Whose data an export carries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExportScope {
    /// The whole tenant. Only the worker's 30-day operator copy.
    Operator,
    /// What the org's owner may see: the shared org oceans, the owner's own
    /// member ocean, and the owner's own conversations. `user_id` is the
    /// control-plane user id the tenant rows are keyed on.
    Owner { user_id: Uuid },
}

/// Prefix of the error a data plane returns when an export exceeds its size
/// cap, so the worker can park (alert) instead of retrying forever.
pub const EXPORT_TOO_LARGE: &str = "export_too_large";

/// Where the encrypted export lives.
#[async_trait]
pub trait ExportStore: Send + Sync {
    async fn put(&self, key: &str, plaintext: &[u8], export_key: &[u8; 32]) -> Result<(), String>;
    async fn get(&self, key: &str, export_key: &[u8; 32]) -> Result<Vec<u8>, String>;
    async fn delete(&self, key: &str) -> Result<(), String>;
}

/// One outbound email (Resend in production).
#[derive(Debug, Clone)]
pub struct Mail {
    pub to: String,
    pub subject: String,
    pub text: String,
    /// Sent as the provider's `Idempotency-Key` so a retry after a crash
    /// between the send and its ledger write cannot email twice (within the
    /// provider's 24 hour window).
    pub idempotency_key: Option<String>,
}

/// Outbound email. Best effort: a failure is logged as an alert and never
/// blocks the deletion.
#[async_trait]
pub trait Mailer: Send + Sync {
    async fn send(&self, mail: Mail) -> Result<(), String>;
}

/// The Clerk side. `delete_org` treats "already gone" (404) as success.
#[async_trait]
pub trait OrgDirectory: Send + Sync {
    async fn delete_org(&self, clerk_org_id: &str) -> Result<(), String>;
    /// The org's display name, best effort (copy degrades without it).
    async fn org_name(&self, _clerk_org_id: &str) -> Option<String> {
        None
    }
}

#[async_trait]
impl OrgDirectory for crate::clerk_admin::ClerkAdminClient {
    async fn delete_org(&self, clerk_org_id: &str) -> Result<(), String> {
        match self.delete_organization(clerk_org_id).await {
            Ok(()) => Ok(()),
            Err(e) if e.status() == Some(404) => Ok(()),
            Err(e) => Err(format!("Clerk delete organization: {e}")),
        }
    }
    async fn org_name(&self, clerk_org_id: &str) -> Option<String> {
        let org = self.get_organization(clerk_org_id).await.ok()?;
        let name = org.name.trim();
        (!name.is_empty()).then(|| name.chars().take(80).collect())
    }
}

/// The destroy step's provisioner: the A1 provisioner and the worker's own
/// `coralswarm_provisioner` control pool.
#[derive(Clone)]
pub struct Destroyer {
    pub provisioner: Arc<dyn TenantDbProvisioner>,
    pub provisioner_control: PgPool,
}

/// Test hook: pretend the process died right after `step`'s side effect,
/// before its checkpoint was written.
pub type CrashHook = Arc<dyn Fn(&str) -> bool + Send + Sync>;

pub struct Ctx<'a> {
    pub control: &'a PgPool,
    pub stripe: Option<&'a StripeClient>,
    pub directory: Option<&'a dyn OrgDirectory>,
    pub data: &'a dyn OrgDataPlane,
    pub exports: Option<&'a dyn ExportStore>,
    pub destroyer: Option<&'a Destroyer>,
    pub mailer: &'a dyn Mailer,
    /// Wraps the per-export key. `None` parks the export step.
    pub kek: Option<[u8; 32]>,
    pub web_base: &'a str,
    pub approval_first_n: i64,
    pub crash_after: Option<CrashHook>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum StepResult {
    Done,
    /// Nothing to do until something outside the job changes.
    Wait(String),
}

type Step<T = StepResult> = Result<T, String>;

#[derive(Debug, PartialEq, Eq)]
pub enum RunEnd {
    Done,
    Yielded,
    Waiting,
    Canceled,
    Failed,
    /// Test only: the crash hook fired.
    Crashed,
    /// Another worker holds the lease now (ours expired and the row was
    /// re-claimed). Nothing more is written.
    LeaseLost,
}

/// The error every fenced write returns when the lease is no longer ours.
pub const LEASE_LOST: &str = "lease lost: another worker has claimed this deletion";

// ── Running ──────────────────────────────────────────────────────────────

/// Claim one due deletion and run it. `Ok(false)` when nothing was due.
pub async fn tick(ctx: &Ctx<'_>) -> Result<bool, sqlx::Error> {
    let claimed: Option<OrgDeletion> = sqlx::query_as("SELECT * FROM org_deletion_claim($1)")
        .bind(LEASE_SECS)
        .fetch_optional(ctx.control)
        .await?;
    let Some(d) = claimed else { return Ok(false) };
    let end = run(ctx, d).await;
    tracing::debug!(?end, "org deletion run ended");
    Ok(true)
}

pub async fn reload(control: &PgPool, d: &OrgDeletion) -> Result<Option<OrgDeletion>, sqlx::Error> {
    let mut tx = scoped(control, d.account_id).await?;
    let row = sqlx::query_as::<_, OrgDeletion>("SELECT * FROM org_deletions WHERE id = $1")
        .bind(d.id)
        .fetch_optional(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(row)
}

/// Run until done, waiting, failed, canceled or out of budget. Every
/// transition is persisted before the next step starts.
pub async fn run(ctx: &Ctx<'_>, claimed: OrgDeletion) -> RunEnd {
    let started = Instant::now();
    // The fencing token this run was handed by `org_deletion_claim`. Every
    // write below is `WHERE lease_id = token`.
    let Some(token) = claimed.lease_id else {
        tracing::error!(deletion_id = %claimed.id, "org deletion: run without a lease token");
        return RunEnd::LeaseLost;
    };
    let mut current = claimed;
    loop {
        let d = match reload(ctx.control, &current).await {
            Ok(Some(d)) => d,
            Ok(None) => return RunEnd::Failed,
            Err(e) => {
                tracing::error!(deletion_id = %current.id, error = %e, "org deletion: reload failed");
                return RunEnd::Failed;
            }
        };
        if d.lease_id != Some(token) {
            tracing::warn!(deletion_id = %d.id, "org deletion: lease lost; stopping");
            return RunEnd::LeaseLost;
        }
        current = d.clone();
        match d.state.as_str() {
            "canceled" => return RunEnd::Canceled,
            "done" => return RunEnd::Done,
            "grace" => match end_grace(ctx, &d).await {
                Ok(true) => continue,
                Ok(false) => {
                    let wait = (d.grace_ends_at - Utc::now()).to_std().unwrap_or(Duration::from_secs(1));
                    let _ = release(ctx.control, &d, wait.min(Duration::from_secs(3600)), None).await;
                    return RunEnd::Waiting;
                }
                Err(e) => return fail(ctx, &d, "end_grace", &e).await,
            },
            "awaiting_approval" => match begin_destroying_if_approved(ctx.control, &d).await {
                Ok(true) => continue,
                Ok(false) => {
                    tracing::warn!(deletion_id = %d.id, org = %d.clerk_org_id,
                        "org deletion waiting for operator approval (bin/org_deletion_admin approve)");
                    let _ = release(ctx.control, &d, Duration::from_secs(WAIT_SECS), None).await;
                    return RunEnd::Waiting;
                }
                Err(e) => return fail(ctx, &d, "approval", &e).await,
            },
            _ => {}
        }
        let Some(step) = d.next_step() else {
            return RunEnd::Done;
        };
        if d.state == "requested" && is_destructive(step) {
            // Request phase finished: enter grace.
            match enter_grace(ctx.control, &d).await {
                Ok(()) => continue,
                Err(e) => return fail(ctx, &d, "enter_grace", &e).await,
            }
        }
        if started.elapsed() > RUN_BUDGET {
            let _ = release(ctx.control, &d, Duration::ZERO, None).await;
            return RunEnd::Yielded;
        }
        let _ = renew_lease(ctx.control, &d).await;
        match run_step(ctx, &d, step).await {
            Ok(StepResult::Done) => {
                if ctx.crash_after.as_ref().is_some_and(|f| f(step)) {
                    return RunEnd::Crashed;
                }
                if let Err(e) = mark_step_done(ctx.control, &d, step).await {
                    return fail(ctx, &d, step, &e).await;
                }
                tracing::info!(deletion_id = %d.id, org = %d.clerk_org_id, step, "org deletion: step complete");
            }
            Ok(StepResult::Wait(why)) => {
                tracing::error!(deletion_id = %d.id, org = %d.clerk_org_id, step, alert = true, why,
                    "org deletion: waiting on something outside the job");
                let _ = release(ctx.control, &d, Duration::from_secs(WAIT_SECS), Some(&why)).await;
                return RunEnd::Waiting;
            }
            Err(e) => return fail(ctx, &d, step, &e).await,
        }
    }
}

async fn fail(ctx: &Ctx<'_>, d: &OrgDeletion, step: &str, e: &str) -> RunEnd {
    tracing::error!(
        deletion_id = %d.id, org = %d.clerk_org_id, step, attempts = d.attempts,
        alert = d.attempts >= 5, error = %e, "org deletion: step failed; will retry with backoff"
    );
    if e == LEASE_LOST {
        return RunEnd::LeaseLost;
    }
    let _ = record_failure(ctx.control, d, e).await;
    RunEnd::Failed
}

async fn run_step(ctx: &Ctx<'_>, d: &OrgDeletion, step: &str) -> Step {
    match step {
        "trial_end" => step_trial_end(ctx, d).await,
        "stripe_cancel" => step_stripe_cancel(ctx, d).await,
        "notify_requested" => step_notify_requested(ctx, d).await,
        "notify_members" => step_notify_members(ctx, d).await,
        "revoke_access" => step_revoke_access(ctx, d).await,
        "manifest" => step_manifest(ctx, d).await,
        "export" => step_export(ctx, d).await,
        "routing" => step_routing(ctx, d).await,
        "credential" => step_credential(ctx, d).await,
        "database" => step_database(ctx, d).await,
        "stripe_sweep" => step_stripe_sweep(ctx, d).await,
        "clerk" => step_clerk(ctx, d).await,
        "notify_done" => step_notify_done(ctx, d).await,
        other => Err(format!("unknown step {other}")),
    }
}

async fn renew_lease(control: &PgPool, d: &OrgDeletion) -> Result<(), String> {
    let mut tx = scoped(control, d.account_id).await.map_err(db_err)?;
    let n = sqlx::query(
        "UPDATE org_deletions SET lease_until = now() + make_interval(secs => $2), updated_at = now() \
          WHERE id = $1 AND lease_id = $3",
    )
    .bind(d.id)
    .bind(LEASE_SECS as f64)
    .bind(d.lease_id)
    .execute(&mut *tx)
    .await
    .map_err(db_err)?
    .rows_affected();
    tx.commit().await.map_err(db_err)?;
    if n == 0 { Err(LEASE_LOST.to_string()) } else { Ok(()) }
}

/// Run `fut` while keeping the lease alive (a long export or upload outlives
/// one lease). Resolves to the future's answer, or `LEASE_LOST` the moment a
/// renewal finds the lease is no longer ours.
async fn with_lease_renewal<T>(
    control: &PgPool,
    d: &OrgDeletion,
    fut: impl std::future::Future<Output = Result<T, String>>,
) -> Result<T, String> {
    tokio::pin!(fut);
    let mut tick = tokio::time::interval(Duration::from_secs((LEASE_SECS as u64 / 3).max(1)));
    tick.tick().await;
    loop {
        tokio::select! {
            r = &mut fut => return r,
            _ = tick.tick() => renew_lease(control, d).await?,
        }
    }
}

async fn mark_step_done(control: &PgPool, d: &OrgDeletion, step: &str) -> Result<(), String> {
    let last = step == STEPS[STEPS.len() - 1];
    let mut tx = scoped(control, d.account_id).await.map_err(db_err)?;
    let n = sqlx::query(
        "UPDATE org_deletions \
            SET completed_steps = completed_steps || ARRAY[$2]::text[], \
                attempts = 0, last_error = NULL, next_attempt_at = now(), updated_at = now(), \
                lease_until = CASE WHEN $3 THEN NULL ELSE lease_until END, \
                state = CASE WHEN $3 THEN 'done' ELSE state END, \
                done_at = CASE WHEN $3 THEN now() ELSE done_at END \
          WHERE id = $1 AND state = ANY($4) AND NOT ($2 = ANY (completed_steps)) AND lease_id = $5",
    )
    .bind(d.id)
    .bind(step)
    .bind(last)
    .bind(if is_destructive(step) { vec!["destroying"] } else { vec!["requested"] })
    .bind(d.lease_id)
    .execute(&mut *tx)
    .await
    .map_err(db_err)?
    .rows_affected();
    tx.commit().await.map_err(db_err)?;
    if n == 0 { Err(LEASE_LOST.to_string()) } else { Ok(()) }
}

/// Seconds to wait after the `attempts`-th consecutive failure: 30s doubling,
/// capped at an hour.
pub fn backoff_secs(attempts: i32) -> i64 {
    let exp = (attempts.max(1) - 1).min(7) as u32;
    (30i64 << exp).min(3600)
}

async fn record_failure(control: &PgPool, d: &OrgDeletion, error: &str) -> Result<(), String> {
    let msg: String = error.chars().take(500).collect();
    let mut tx = scoped(control, d.account_id).await.map_err(db_err)?;
    sqlx::query(
        "UPDATE org_deletions SET last_error = $2, next_attempt_at = now() + make_interval(secs => $3), \
                lease_until = NULL, updated_at = now() WHERE id = $1 AND lease_id = $4",
    )
    .bind(d.id)
    .bind(msg)
    .bind(backoff_secs(d.attempts) as f64)
    .bind(d.lease_id)
    .execute(&mut *tx)
    .await
    .map_err(db_err)?;
    tx.commit().await.map_err(db_err)
}

async fn release(control: &PgPool, d: &OrgDeletion, after: Duration, note: Option<&str>) -> Result<(), String> {
    let mut tx = scoped(control, d.account_id).await.map_err(db_err)?;
    sqlx::query(
        "UPDATE org_deletions SET lease_until = NULL, next_attempt_at = now() + make_interval(secs => $2), \
                last_error = COALESCE($3, last_error), attempts = 0, updated_at = now() \
          WHERE id = $1 AND lease_id = $4",
    )
    .bind(d.id)
    .bind(after.as_secs_f64())
    .bind(note.map(|n| n.chars().take(500).collect::<String>()))
    .bind(d.lease_id)
    .execute(&mut *tx)
    .await
    .map_err(db_err)?;
    tx.commit().await.map_err(db_err)
}

/// requested -> grace, once the request-phase steps are checkpointed.
async fn enter_grace(control: &PgPool, d: &OrgDeletion) -> Result<(), String> {
    let mut tx = scoped(control, d.account_id).await.map_err(db_err)?;
    sqlx::query(
        "UPDATE org_deletions SET state = 'grace', next_attempt_at = grace_ends_at, updated_at = now() \
          WHERE id = $1 AND state = 'requested' AND lease_id = $2 \
            AND completed_steps @> ARRAY['trial_end', 'stripe_cancel', 'notify_requested', 'notify_members']::text[]",
    )
    .bind(d.id)
    .bind(d.lease_id)
    .execute(&mut *tx)
    .await
    .map_err(db_err)?;
    tx.commit().await.map_err(db_err)
}

/// grace -> awaiting_approval | destroying, once grace is over. Decides
/// whether this deletion needs an operator's approval (the first N completed
/// deletions do). `Ok(false)` while grace still runs.
async fn end_grace(ctx: &Ctx<'_>, d: &OrgDeletion) -> Result<bool, String> {
    if Utc::now() < d.grace_ends_at {
        return Ok(false);
    }
    let completed: i64 = sqlx::query_scalar("SELECT org_deletion_completed_count()")
        .fetch_one(ctx.control)
        .await
        .map_err(db_err)?;
    // The database's own threshold is the floor: the environment can only
    // RAISE it (the trigger would refuse a lower decision anyway).
    let db_first_n: i32 = sqlx::query_scalar("SELECT org_deletion_approval_first_n()")
        .fetch_one(ctx.control)
        .await
        .map_err(db_err)?;
    let required = completed < ctx.approval_first_n.max(db_first_n as i64);
    let mut tx = scoped(ctx.control, d.account_id).await.map_err(db_err)?;
    let n = sqlx::query(
        "UPDATE org_deletions \
            SET approval_required = COALESCE(approval_required, $2), \
                state = CASE WHEN COALESCE(approval_required, $2) AND approved_at IS NULL \
                             THEN 'awaiting_approval' ELSE 'destroying' END, \
                destroying_since = CASE WHEN COALESCE(approval_required, $2) AND approved_at IS NULL \
                                        THEN destroying_since ELSE now() END, \
                awaiting_approval_since = CASE WHEN COALESCE(approval_required, $2) AND approved_at IS NULL \
                                               THEN now() ELSE awaiting_approval_since END, \
                next_attempt_at = now(), updated_at = now() \
          WHERE id = $1 AND state = 'grace' AND grace_ends_at <= now() AND lease_id = $3",
    )
    .bind(d.id)
    .bind(required)
    .bind(d.lease_id)
    .execute(&mut *tx)
    .await
    .map_err(db_err)?
    .rows_affected();
    tx.commit().await.map_err(db_err)?;
    if n == 1 {
        tracing::warn!(deletion_id = %d.id, org = %d.clerk_org_id, approval_required = required,
            "org deletion: grace over");
    }
    // Lost a race (an undo landed first): the reload decides.
    Ok(true)
}

async fn begin_destroying_if_approved(control: &PgPool, d: &OrgDeletion) -> Result<bool, String> {
    if d.approved_at.is_none() {
        return Ok(false);
    }
    let mut tx = scoped(control, d.account_id).await.map_err(db_err)?;
    sqlx::query(
        "UPDATE org_deletions SET state = 'destroying', destroying_since = now(), next_attempt_at = now(), updated_at = now() \
          WHERE id = $1 AND state = 'awaiting_approval' AND approved_at IS NOT NULL AND lease_id = $2",
    )
    .bind(d.id)
    .bind(d.lease_id)
    .execute(&mut *tx)
    .await
    .map_err(db_err)?;
    tx.commit().await.map_err(db_err)?;
    tracing::warn!(deletion_id = %d.id, org = %d.clerk_org_id, approved_by = d.approved_by.as_deref().unwrap_or("-"),
        "org deletion approved; destroying");
    Ok(true)
}

/// A fenced single-column write: `sql` is `UPDATE ... WHERE id = $1`, with
/// `$2` the value; the lease fence is appended here as `$3`.
async fn set_column(control: &PgPool, d: &OrgDeletion, sql: &str, bind: Option<String>) -> Result<(), String> {
    let mut tx = scoped(control, d.account_id).await.map_err(db_err)?;
    let n = sqlx::query(&format!("{sql} AND lease_id = $3"))
        .bind(d.id)
        .bind(bind)
        .bind(d.lease_id)
        .execute(&mut *tx)
        .await
        .map_err(db_err)?
        .rows_affected();
    tx.commit().await.map_err(db_err)?;
    if n == 0 { Err(LEASE_LOST.to_string()) } else { Ok(()) }
}

// ── Request-phase steps ──────────────────────────────────────────────────

/// End a live self-serve trial. `trialing`/`converting` -> `canceled`, which
/// also clears the 0-credit override and expires what is left of the trial
/// pool: unused trial credits are forfeited (decision 12). Anything else
/// (no trial, already ended) is left alone; the UPDATE is guarded by state.
async fn step_trial_end(ctx: &Ctx<'_>, d: &OrgDeletion) -> Step {
    use crate::billing::org_trial::{db, TrialState};
    let mut tx = scoped(ctx.control, d.account_id).await.map_err(db_err)?;
    crate::billing::renewal::lock_period_rotation(&mut tx, d.account_id).await.map_err(db_err)?;
    let row = db::load_for_update(&mut *tx, d.account_id).await.map_err(db_err)?;
    let outcome = match row.map(|r| r.state) {
        None => "no_trial".to_string(),
        Some(TrialState::Trialing | TrialState::Converting) => {
            let ended = db::end(&mut *tx, d.account_id, TrialState::Canceled, "organization deleted by its owner")
                .await
                .map_err(db_err)?;
            if ended { "canceled".into() } else { "unchanged".into() }
        }
        Some(TrialState::CheckoutOpen) => {
            let ended = db::end(&mut *tx, d.account_id, TrialState::Abandoned, "organization deleted by its owner")
                .await
                .map_err(db_err)?;
            if ended { "abandoned".into() } else { "unchanged".into() }
        }
        Some(other) => format!("already_{}", other.as_str()),
    };
    tx.commit().await.map_err(db_err)?;
    set_column(
        ctx.control,
        d,
        "UPDATE org_deletions SET trial_outcome = COALESCE(trial_outcome, $2), updated_at = now() WHERE id = $1",
        Some(outcome),
    )
    .await?;
    Ok(StepResult::Done)
}

fn outcome_label(o: &CancelOutcome) -> &'static str {
    match o {
        CancelOutcome::NoSubscription => "no_subscription",
        CancelOutcome::AlreadyEnded => "already_ended",
        CancelOutcome::AlreadyScheduled { .. } => "already_scheduled",
        CancelOutcome::Scheduled { .. } => "scheduled",
        CancelOutcome::EndedNow => "ended_now",
    }
}

/// End the org's subscription NOW (decision 12: access ends now, no refund):
/// `prorate=false`, `invoice_now=false`, one keyed `DELETE`. Same contract as
/// #1538's `cancel_deleted_org_subscription`, so whichever of the two runs
/// second finds the subscription ended and writes nothing:
///
/// * no subscription id: no call. A mirror that says `canceled` is NOT trusted: Stripe is asked;
/// * the live subscription is read first; `canceled`/`incomplete_expired`
///   is `AlreadyEnded` with no write;
/// * otherwise ONE `DELETE` keyed [`deleted_org_cancel_key`];
/// * a `DELETE` that errors is followed by a re-read, and "ended anyway"
///   counts as success.
pub async fn cancel_org_subscription_now(
    control: &PgPool,
    stripe: Option<&StripeClient>,
    account_id: Uuid,
) -> Result<CancelOutcome, String> {
    use crate::billing::seat_lifecycle::with_stripe_deadline;
    let mut tx = scoped(control, account_id).await.map_err(db_err)?;
    let row: Option<(Option<String>, String)> =
        sqlx::query_as("SELECT stripe_subscription_id, status FROM stripe_subscriptions WHERE account_id = $1")
            .bind(account_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(db_err)?;
    tx.commit().await.map_err(db_err)?;
    let Some((Some(subscription_id), status)) = row else {
        return Ok(CancelOutcome::NoSubscription);
    };
    // Our stored status is a mirror and may lag (a resubscribe, a missed
    // webhook): even `canceled` is confirmed with Stripe, never trusted.
    let _ = status;
    let ended = |s: &str| matches!(s, "canceled" | "incomplete_expired");
    let Some(stripe) = stripe else {
        return Err("stripe is not configured but the org has a subscription".into());
    };
    let live = with_stripe_deadline(stripe.retrieve_subscription(&subscription_id))
        .await
        .map_err(|e| format!("stripe read: {e}"))?;
    if ended(&live.status) {
        return Ok(CancelOutcome::AlreadyEnded);
    }
    end_subscription(stripe, &subscription_id).await?;
    crate::billing::cancel::mirror_cancel_state(control, account_id, &subscription_id, false, None)
        .await
        .map_err(db_err)?;
    tracing::info!(%account_id, subscription_id, "subscription ended immediately (organization deletion)");
    Ok(CancelOutcome::EndedNow)
}

/// The org's Stripe customer id (our mirror's link to it), if it has one.
async fn stripe_customer_id(control: &PgPool, account_id: Uuid) -> Result<Option<String>, String> {
    let mut tx = scoped(control, account_id).await.map_err(db_err)?;
    let id: Option<String> = sqlx::query_scalar("SELECT stripe_customer_id FROM stripe_subscriptions WHERE account_id = $1")
        .bind(account_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db_err)?;
    tx.commit().await.map_err(db_err)?;
    Ok(id)
}

/// Expire every OPEN Checkout Session of the customer, so a Checkout opened
/// before the request cannot start a subscription after it. An expire that
/// Stripe refuses because the session just completed or expired is fine (the
/// `stripe_sweep` step and the webhook refusal catch what it started).
/// Returns how many sessions were expired.
pub async fn expire_open_checkouts(stripe: &StripeClient, customer_id: &str) -> Result<usize, String> {
    use crate::billing::seat_lifecycle::with_stripe_deadline;
    let open = with_stripe_deadline(stripe.list_open_checkout_sessions(customer_id))
        .await
        .map_err(|e| format!("stripe list checkout sessions: {e}"))?;
    let mut n = 0;
    for id in open {
        match with_stripe_deadline(stripe.expire_checkout_session(&id)).await {
            Ok(()) => n += 1,
            Err(e) => tracing::warn!(session = %id, error = %e, "org deletion: could not expire checkout session (already done?)"),
        }
    }
    Ok(n)
}

/// End one subscription at Stripe now, keyed, no proration, no final invoice.
/// A failed `DELETE` followed by a re-read that says ended counts as success.
pub(crate) async fn end_subscription(stripe: &StripeClient, subscription_id: &str) -> Result<(), String> {
    use crate::billing::seat_lifecycle::with_stripe_deadline;
    let ended = |s: &str| matches!(s, "canceled" | "incomplete_expired");
    let key = deleted_org_cancel_key(subscription_id);
    if let Err(e) = with_stripe_deadline(stripe.cancel_subscription_now_keyed(subscription_id, &key)).await {
        let after = with_stripe_deadline(stripe.retrieve_subscription(subscription_id))
            .await
            .map_err(|e| format!("stripe re-read: {e}"))?;
        if !ended(&after.status) {
            return Err(format!("stripe cancel {subscription_id}: {e}"));
        }
    }
    Ok(())
}

/// Destroying-phase billing sweep: list the customer's subscriptions AT STRIPE
/// and end any that is not ended. Returns the ids ended.
pub async fn end_customer_subscriptions(stripe: &StripeClient, customer_id: &str) -> Result<Vec<String>, String> {
    use crate::billing::seat_lifecycle::with_stripe_deadline;
    let subs = with_stripe_deadline(stripe.list_customer_subscriptions(customer_id))
        .await
        .map_err(|e| format!("stripe list subscriptions: {e}"))?;
    let mut ended = Vec::new();
    for (id, status) in subs {
        if matches!(status.as_str(), "canceled" | "incomplete_expired") {
            continue;
        }
        end_subscription(stripe, &id).await?;
        ended.push(id);
    }
    Ok(ended)
}

async fn step_stripe_sweep(ctx: &Ctx<'_>, d: &OrgDeletion) -> Step {
    let Some(customer) = stripe_customer_id(ctx.control, d.account_id).await? else {
        return Ok(StepResult::Done);
    };
    let Some(stripe) = ctx.stripe else {
        return Ok(StepResult::Wait("stripe is not configured but the org has a Stripe customer".into()));
    };
    expire_open_checkouts(stripe, &customer).await?;
    let ended = end_customer_subscriptions(stripe, &customer).await?;
    if !ended.is_empty() {
        tracing::error!(alert = true, deletion_id = %d.id, org = %d.clerk_org_id, ?ended,
            "org deletion: found and ended subscriptions that were live after the request");
    }
    Ok(StepResult::Done)
}

async fn step_stripe_cancel(ctx: &Ctx<'_>, d: &OrgDeletion) -> Step {
    if let Some(customer) = stripe_customer_id(ctx.control, d.account_id).await? {
        let Some(stripe) = ctx.stripe else {
            return Err("stripe is not configured but the org has a Stripe customer".into());
        };
        expire_open_checkouts(stripe, &customer).await?;
    }
    let outcome = cancel_org_subscription_now(ctx.control, ctx.stripe, d.account_id).await?;
    // Extra usage and auto-reload off: an auto-reload charges the saved card
    // independently of the subscription.
    let mut tx = scoped(ctx.control, d.account_id).await.map_err(db_err)?;
    crate::billing::renewal::lock_period_rotation(&mut tx, d.account_id).await.map_err(db_err)?;
    sqlx::query(
        "UPDATE extra_usage_settings SET enabled = false, auto_reload_enabled = false, updated_at = now() \
          WHERE account_id = $1 AND (enabled OR auto_reload_enabled)",
    )
    .bind(d.account_id)
    .execute(&mut *tx)
    .await
    .map_err(db_err)?;
    tx.commit().await.map_err(db_err)?;
    set_column(
        ctx.control,
        d,
        "UPDATE org_deletions SET stripe_outcome = COALESCE(stripe_outcome, $2), updated_at = now() WHERE id = $1",
        Some(outcome_label(&outcome).to_string()),
    )
    .await?;
    Ok(StepResult::Done)
}

async fn requester_email(control: &PgPool, d: &OrgDeletion) -> Option<String> {
    let user = match d.requested_by_user_id {
        Some(u) => Some(u),
        None => sqlx::query_scalar("SELECT owner_user_id FROM accounts WHERE id = $1")
            .bind(d.account_id)
            .fetch_optional(control)
            .await
            .ok()
            .flatten(),
    }?;
    let email: Option<String> = sqlx::query_scalar("SELECT email FROM users WHERE id = $1")
        .bind(user)
        .fetch_optional(control)
        .await
        .ok()
        .flatten();
    email.filter(|e| e.contains('@') && !e.ends_with(".invalid"))
}

async fn send_best_effort(ctx: &Ctx<'_>, d: &OrgDeletion, subject: &str, text: String) {
    let Some(to) = requester_email(ctx.control, d).await else { return };
    if let Err(e) = ctx.mailer.send(Mail { to, subject: subject.into(), text, idempotency_key: None }).await {
        tracing::error!(deletion_id = %d.id, alert = true, error = %e, "org deletion: email could not be sent");
    }
}

async fn step_notify_requested(ctx: &Ctx<'_>, d: &OrgDeletion) -> Step {
    let why = match d.initiated_by.as_str() {
        "unpaid" => "because it has been unpaid for 30 days",
        "clerk_dashboard" => "because it was deleted from the identity dashboard",
        _ => "because you asked us to",
    };
    let text = format!(
        "Your CoralSwarm organization is scheduled for deletion {why}.\n\n\
         Access for everyone in the organization has ended, and the subscription was cancelled with no refund.\n\
         Until {}, you can download an export of the organization's data or undo the deletion at {}/org/deletion.\n\
         Undoing restores access but not the subscription: you will need to subscribe again from Billing.\n\
         After that date the organization and its data are permanently deleted.\n\nCoralSwarm",
        d.grace_ends_at.format("%B %-d, %Y"),
        ctx.web_base.trim_end_matches('/'),
    );
    send_best_effort(ctx, d, "Your CoralSwarm organization is scheduled for deletion", text).await;
    Ok(StepResult::Done)
}

/// The members other than the owner and the requester: who the org is
/// being deleted FOR, not BY. Read while access is still whole (this step
/// runs in `requested`, before `revoke_access`).
async fn members_to_notify(control: &PgPool, d: &OrgDeletion) -> Result<Vec<(Uuid, Option<String>)>, String> {
    sqlx::query_as(
        "SELECT u.id, u.email FROM account_members am JOIN users u ON u.id = am.user_id \
          WHERE am.account_id = $1 AND am.role <> 'owner' \
            AND u.id IS DISTINCT FROM (SELECT owner_user_id FROM accounts WHERE id = $1) \
            AND u.id IS DISTINCT FROM $2 \
          ORDER BY u.id",
    )
    .bind(d.account_id)
    .bind(d.requested_by_user_id)
    .fetch_all(control)
    .await
    .map_err(db_err)
}

/// The words a member reads. Plain: what happened, what they lost, what will
/// be deleted and when, and that the only way to keep it is the owner undoing
/// the deletion. No copy is made for them (founder decision).
pub fn member_notice_text(org_name: Option<&str>, grace_ends_at: DateTime<Utc>, owner_can_undo: bool) -> (String, String) {
    let org = match org_name.map(str::trim).filter(|n| !n.is_empty()) {
        Some(n) => format!("\"{n}\""),
        None => "a CoralSwarm organization you belong to".to_string(),
    };
    let date = grace_ends_at.format("%B %-d, %Y");
    let keep = if owner_can_undo { "unless the organization's owner undoes the deletion before then" } else { "" };
    let subject = "A CoralSwarm organization you belong to is being deleted".to_string();
    let text = format!(
        "{org} is being deleted, and you no longer have access to it.\n\n\
         Your private notes in this organization will be permanently deleted on {date}{}. \
         You will not get a copy of them.\n\n\
         Anything you captured in your own personal CoralSwarm account is not affected.\n\nCoralSwarm",
        if keep.is_empty() { String::new() } else { format!(", {keep}") },
    );
    (subject, text)
}

/// Email each member once. Each send is followed at once by its ledger write
/// (`members_notified`), so a retry skips who already got it, and the send
/// carries a per-(deletion, member) idempotency key for the crash window
/// between the two. A failed send is alerted; the step is retried (with the
/// usual backoff) for the first few attempts and then lets the deletion
/// proceed rather than hold an owner's request open on a mail outage.
async fn step_notify_members(ctx: &Ctx<'_>, d: &OrgDeletion) -> Step {
    let members = members_to_notify(ctx.control, d).await?;
    let pending: Vec<_> = members.into_iter().filter(|(id, _)| !d.members_notified.contains(id)).collect();
    if pending.is_empty() {
        return Ok(StepResult::Done);
    }
    let org_name = match ctx.directory {
        Some(dir) => dir.org_name(&d.clerk_org_id).await,
        None => None,
    };
    let (subject, text) = member_notice_text(org_name.as_deref(), d.grace_ends_at, d.initiated_by != "clerk_dashboard");
    let mut failed = 0usize;
    for (user_id, email) in pending {
        let usable = email.filter(|e| e.contains('@') && !e.ends_with(".invalid"));
        if let Some(to) = usable {
            let mail = Mail {
                to,
                subject: subject.clone(),
                text: text.clone(),
                idempotency_key: Some(format!("org-deletion-member:{}:{user_id}", d.id)),
            };
            if let Err(e) = ctx.mailer.send(mail).await {
                failed += 1;
                tracing::error!(deletion_id = %d.id, member = %user_id, alert = true, error = %e,
                    "org deletion: a member could not be told their private notes will be deleted");
                continue;
            }
        }
        // Sent, or no usable address (nothing to send; do not retry forever).
        let mut tx = scoped(ctx.control, d.account_id).await.map_err(db_err)?;
        let n = sqlx::query(
            "UPDATE org_deletions SET members_notified = array_append(members_notified, $2), updated_at = now() \
              WHERE id = $1 AND lease_id = $3 AND NOT ($2 = ANY (members_notified))",
        )
        .bind(d.id)
        .bind(user_id)
        .bind(d.lease_id)
        .execute(&mut *tx)
        .await
        .map_err(db_err)?
        .rows_affected();
        tx.commit().await.map_err(db_err)?;
        if n == 0 {
            // Either the ledger already had them (a racing run) or the lease
            // moved; the reload at the top of the next loop decides which.
            let now = reload(ctx.control, d).await.map_err(db_err)?;
            if now.as_ref().map(|x| x.lease_id) != Some(d.lease_id) {
                return Err(LEASE_LOST.to_string());
            }
        }
    }
    if failed > 0 && d.attempts < 3 {
        return Err(format!("{failed} member notice(s) could not be sent; retrying"));
    }
    Ok(StepResult::Done)
}

// ── Destroying steps ─────────────────────────────────────────────────────

async fn step_revoke_access(ctx: &Ctx<'_>, d: &OrgDeletion) -> Step {
    crate::clerk_provision::revoke_org_access(ctx.control, &d.clerk_org_id)
        .await
        .map_err(|e| format!("revoke_org_access: {e}"))?;
    Ok(StepResult::Done)
}

async fn step_manifest(ctx: &Ctx<'_>, d: &OrgDeletion) -> Step {
    if d.source_manifest.is_some() {
        return Ok(StepResult::Done);
    }
    let manifest = ctx.data.source_manifest(d).await?;
    let mut tx = scoped(ctx.control, d.account_id).await.map_err(db_err)?;
    let n = sqlx::query("UPDATE org_deletions SET source_manifest = $2, updated_at = now() WHERE id = $1 AND lease_id = $3")
        .bind(d.id)
        .bind(manifest)
        .bind(d.lease_id)
        .execute(&mut *tx)
        .await
        .map_err(db_err)?
        .rows_affected();
    tx.commit().await.map_err(db_err)?;
    if n == 0 {
        return Err(LEASE_LOST.to_string());
    }
    Ok(StepResult::Done)
}

/// The R2 key of one ATTEMPT at a deletion's export: `{org}/{deletion}/{attempt}`,
/// the org-prefixed three-segment shape every blob key must have. Each attempt
/// writes its own object, so a stale worker can never overwrite the object the
/// recorded key opens.
pub fn export_object_key(d: &OrgDeletion, attempt: Uuid) -> String {
    format!("{}/{}/{}", d.clerk_org_id, d.id, attempt)
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

/// Write the 30-day export: NDJSON of the whole tenant, encrypted under a
/// fresh key; the key wrapped under the KEK and recorded with the digest.
///
/// Fenced three ways: the lease is renewed while the bundle is built and
/// uploaded; the object key is per attempt; and the key is recorded only
/// `WHERE lease_id = <ours> AND export_key_wrapped IS NULL`, so a worker that
/// lost its lease cannot replace the key a newer attempt recorded (it deletes
/// its own orphan object instead).
async fn step_export(ctx: &Ctx<'_>, d: &OrgDeletion) -> Step {
    if d.export_key_wrapped.is_some() {
        return Ok(StepResult::Done);
    }
    let (Some(store), Some(kek)) = (ctx.exports, ctx.kek.as_ref()) else {
        return Ok(StepResult::Wait("export storage (R2) or MASTER_KEK_HEX is not configured".into()));
    };
    let bundle = match with_lease_renewal(ctx.control, d, ctx.data.export_bundle(d, &ExportScope::Operator)).await {
        Ok(b) => b,
        Err(e) if e.starts_with(EXPORT_TOO_LARGE) => return Ok(StepResult::Wait(e)),
        Err(e) => return Err(e),
    };
    let key = coralswarm_core::kek::generate_dek();
    let object = export_object_key(d, Uuid::new_v4());
    with_lease_renewal(ctx.control, d, store.put(&object, &bundle, &key)).await?;
    let wrapped = coralswarm_core::kek::wrap_dek(kek, &key).map_err(|e| format!("wrap export key: {e}"))?;
    let mut tx = scoped(ctx.control, d.account_id).await.map_err(db_err)?;
    let n = sqlx::query(
        "UPDATE org_deletions SET export_object_key = $2, export_key_wrapped = $3, export_sha256 = $4, \
                export_bytes = $5, export_expires_at = now() + make_interval(days => $6), updated_at = now() \
          WHERE id = $1 AND lease_id = $7 AND export_key_wrapped IS NULL",
    )
    .bind(d.id)
    .bind(&object)
    .bind(wrapped)
    .bind(sha256_hex(&bundle))
    .bind(bundle.len() as i64)
    .bind(EXPORT_RETENTION_DAYS as i32)
    .bind(d.lease_id)
    .execute(&mut *tx)
    .await
    .map_err(db_err)?
    .rows_affected();
    tx.commit().await.map_err(db_err)?;
    if n == 0 {
        // Someone else's export is the recorded one. Ours is an orphan whose
        // key was never stored: unreadable, and removed best effort.
        let _ = store.delete(&object).await;
        return Err(LEASE_LOST.to_string());
    }
    Ok(StepResult::Done)
}

/// The org's self-serve provisioning job, read-only to the serving role.
async fn provisioning_job(control: &PgPool, org: &str) -> Result<Option<crate::tenancy::provisioning::Job>, String> {
    sqlx::query_as::<_, crate::tenancy::provisioning::Job>("SELECT * FROM tenant_provisioning_jobs WHERE clerk_org_id = $1")
        .bind(org)
        .fetch_optional(control)
        .await
        .map_err(db_err)
}

/// The job for THIS deletion's org AND account. A job row for the same
/// `clerk_org_id` bound to a different account is not this org's; the step
/// parks instead of acting on it.
async fn bound_job(ctx: &Ctx<'_>, d: &OrgDeletion) -> Result<Result<crate::tenancy::provisioning::Job, StepResult>, String> {
    let Some(job) = provisioning_job(ctx.control, &d.clerk_org_id).await? else {
        return Ok(Err(StepResult::Wait("no self-serve provisioning job for this org".into())));
    };
    if job.account_id != d.account_id {
        return Ok(Err(StepResult::Wait(format!(
            "the provisioning job for {} is bound to a different account; nothing deleted",
            d.clerk_org_id
        ))));
    }
    Ok(Ok(job))
}

fn destroyer<'a>(ctx: &Ctx<'a>) -> Result<&'a Destroyer, StepResult> {
    ctx.destroyer.ok_or_else(|| {
        StepResult::Wait("the provisioner (TENANT_PROVISIONER + PROVISIONER_DATABASE_URL) is not configured".into())
    })
}

/// Delete the routing row through the 0387 definer (provisioner role only).
/// Records what the database step will delete first, from the job row.
async fn step_routing(ctx: &Ctx<'_>, d: &OrgDeletion) -> Step {
    let destroyer = match destroyer(ctx) {
        Ok(x) => x,
        Err(w) => return Ok(w),
    };
    let job = match bound_job(ctx, d).await? {
        Ok(j) => j,
        Err(w) => return Ok(w),
    };
    if d.provider_database_id.is_none() {
        let mut tx = scoped(ctx.control, d.account_id).await.map_err(db_err)?;
        sqlx::query(
            "UPDATE org_deletions SET provider = $2, provider_database_id = $3, database_name = $4, updated_at = now() \
              WHERE id = $1 AND lease_id = $5",
        )
        .bind(d.id)
        .bind(job.provider.as_deref())
        .bind(job.provider_database_id.as_deref())
        .bind(destroyer.provisioner.database_name(&job.slug))
        .bind(d.lease_id)
        .execute(&mut *tx)
        .await
        .map_err(db_err)?;
        tx.commit().await.map_err(db_err)?;
    }
    ctx.data.forget_tenant(&d.clerk_org_id, d.account_id).await;
    let n: i32 = sqlx::query_scalar("SELECT org_deletion_delete_routing($1)")
        .bind(d.id)
        .fetch_one(&destroyer.provisioner_control)
        .await
        .map_err(db_err)?;
    tracing::info!(deletion_id = %d.id, org = %d.clerk_org_id, deleted = n, "org deletion: routing row deleted");
    Ok(StepResult::Done)
}

async fn step_credential(ctx: &Ctx<'_>, d: &OrgDeletion) -> Step {
    let destroyer = match destroyer(ctx) {
        Ok(x) => x,
        Err(w) => return Ok(w),
    };
    let job = match bound_job(ctx, d).await? {
        Ok(j) => j,
        Err(w) => return Ok(w),
    };
    crate::tenancy::credentials::delete(&destroyer.provisioner_control, &job.db_secret_ref)
        .await
        .map_err(|e| format!("delete credential: {e}"))?;
    Ok(StepResult::Done)
}

/// Delete the tenant database, with A1 cleanup's name check and more:
///
/// * the job must be this worker's provider, and its name
///   (`database_name(slug)`) a SELF-SERVE tenant name;
/// * the provider's own name for the recorded id must equal it (a recorded
///   id pointing at someone else's project is refused, never deleted);
/// * the database's `tenant_meta` fingerprint must be THIS org (stricter
///   than cleanup, which also deletes an unclaimed database: a live tenant
///   always carries its fingerprint);
/// * an id the provider no longer knows is done.
///
/// A refusal parks the job with an alert; it never deletes.
async fn step_database(ctx: &Ctx<'_>, d: &OrgDeletion) -> Step {
    let destroyer = match destroyer(ctx) {
        Ok(x) => x,
        Err(w) => return Ok(w),
    };
    let p = &destroyer.provisioner;
    let job = match bound_job(ctx, d).await? {
        Ok(j) => j,
        Err(w) => return Ok(w),
    };
    if job.provider.as_deref() != Some(p.provider_name()) {
        return Ok(StepResult::Wait(format!(
            "the database was created by provider {:?}; this worker is {:?}",
            job.provider,
            p.provider_name()
        )));
    }
    let expected = p.database_name(&job.slug);
    if !p.is_selfserve_name(&expected) {
        return Ok(StepResult::Wait(format!("{expected:?} is not a self-serve tenant name; nothing deleted")));
    }
    let Some(id) = job.provider_database_id.clone() else {
        return Ok(StepResult::Wait("the provisioning job recorded no database id".into()));
    };
    let Some(actual) = p.name_of(&id).await.map_err(|e| format!("provider name lookup: {e}"))? else {
        return Ok(StepResult::Done);
    };
    if actual != expected {
        tracing::error!(connector_health = true, deletion_id = %d.id, org = %d.clerk_org_id,
            provider_database_id = %id, provider_name = %actual, expected_name = %expected,
            "org deletion refused to delete a database that is not named for this org");
        return Ok(StepResult::Wait(format!(
            "database {id} is named {actual:?}, not {expected:?}; left in place"
        )));
    }
    // The project exists (its name was just read). A missing connection
    // string for it is NOT "gone": only an absent project is. Wait and alert.
    let Some(dsn) = p.connection_dsn(&id).await.map_err(|e| format!("provider connection: {e}"))? else {
        tracing::error!(connector_health = true, deletion_id = %d.id, org = %d.clerk_org_id, provider_database_id = %id,
            "org deletion: the provider has the database but no connection string for it; not treating it as gone");
        return Ok(StepResult::Wait(format!(
            "database {id} exists but the provider returned no connection string for it; left in place"
        )));
    };
    match fingerprint(&dsn).await? {
        Some(org) if org == d.clerk_org_id => {}
        other => {
            tracing::error!(connector_health = true, deletion_id = %d.id, org = %d.clerk_org_id,
                provider_database_id = %id, fingerprinted_for = other.as_deref().unwrap_or("-"),
                "org deletion refused to delete a database not fingerprinted for this org");
            return Ok(StepResult::Wait(format!(
                "database {id} is fingerprinted for {:?}, not this org; left in place",
                other
            )));
        }
    }
    ctx.data.forget_tenant(&d.clerk_org_id, d.account_id).await;
    match p.delete(&id).await.map_err(|e| format!("provider delete: {e}"))? {
        DeleteOutcome::Deleted => tracing::warn!(deletion_id = %d.id, org = %d.clerk_org_id, provider_database_id = %id,
            "org deletion: tenant database deleted"),
        DeleteOutcome::AlreadyGone => {}
    }
    Ok(StepResult::Done)
}

/// `tenant_meta.org_id` of the database behind `dsn` (read-only).
async fn fingerprint(dsn: &str) -> Result<Option<String>, String> {
    use sqlx::Connection;
    let mut conn = tokio::time::timeout(Duration::from_secs(15), sqlx::PgConnection::connect(dsn))
        .await
        .map_err(|_| "connect tenant (fingerprint): timed out".to_string())?
        .map_err(|e| format!("connect tenant (fingerprint): {e}"))?;
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
    read.map_err(|e| format!("fingerprint read: {e}"))
}

async fn step_clerk(ctx: &Ctx<'_>, d: &OrgDeletion) -> Step {
    let Some(directory) = ctx.directory else {
        return Ok(StepResult::Wait("CLERK_SECRET_KEY is not configured".into()));
    };
    directory.delete_org(&d.clerk_org_id).await?;
    Ok(StepResult::Done)
}

async fn step_notify_done(ctx: &Ctx<'_>, d: &OrgDeletion) -> Step {
    let text = "Your CoralSwarm organization and its data have been permanently deleted.\n\n\
                Billing records are kept, as the law and our financial obligations require. \
                People's own personal workspaces were not affected.\n\nCoralSwarm"
        .to_string();
    send_best_effort(ctx, d, "Your CoralSwarm organization was deleted", text).await;
    Ok(StepResult::Done)
}

// ── Billing events for a deleting org ─────────────────────────────────────

/// Why a billing event for this account must not activate anything: the org
/// has a live-or-finished deletion, or has been revoked (deleted in Clerk).
/// `None` for an ordinary org and for personal accounts. A personal account can
/// carry an ADOPTED personal Clerk org id (`bin/adopt_personal_org_account`
/// relabels it), recorded in `personal_orgs`; that id being revoked or deleted
/// in Clerk must not cancel the person's own Pro Checkout, so those are skipped.
pub async fn billing_block_reason(control: &PgPool, account_id: Uuid) -> Result<Option<String>, sqlx::Error> {
    let row: Option<(Option<String>, bool)> = sqlx::query_as(
        "SELECT org_deletion_state_for_org(a.clerk_org_id), \
                EXISTS (SELECT 1 FROM org_revocations r WHERE r.clerk_org_id = a.clerk_org_id) \
           FROM accounts a WHERE a.id = $1 AND a.clerk_org_id IS NOT NULL \
            AND NOT EXISTS (SELECT 1 FROM personal_orgs po WHERE po.clerk_org_id = a.clerk_org_id)",
    )
    .bind(account_id)
    .fetch_optional(control)
    .await?;
    Ok(row.and_then(|(state, revoked)| match (state, revoked) {
        (Some(st), _) => Some(format!("org_deletion_{st}")),
        (None, true) => Some("org_revoked".to_string()),
        (None, false) => None,
    }))
}

// ── Sweeps ───────────────────────────────────────────────────────────────

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize)]
pub struct SweepReport {
    pub unpaid_entered: usize,
    pub clerk_adopted: usize,
    pub exports_destroyed: usize,
    pub stuck: usize,
    pub approval_overdue: usize,
}

/// One pass of the system-initiated entries and the retention sweep.
pub async fn sweep(ctx: &Ctx<'_>) -> SweepReport {
    let mut report = SweepReport::default();
    match unpaid_entry(ctx.control, ctx.stripe).await {
        Ok(n) => report.unpaid_entered = n,
        Err(e) => tracing::error!(alert = true, error = %e, "org deletion: unpaid sweep failed"),
    }
    match adopt_clerk_deletions(ctx.control).await {
        Ok(n) => report.clerk_adopted = n,
        Err(e) => tracing::error!(alert = true, error = %e, "org deletion: Clerk-dashboard adoption failed"),
    }
    match destroy_expired_exports(ctx.control, ctx.exports).await {
        Ok(n) => report.exports_destroyed = n,
        Err(e) => tracing::error!(alert = true, error = %e, "org deletion: export retention sweep failed"),
    }
    match alert_stuck(ctx.control, 6).await {
        Ok(n) => report.stuck = n,
        Err(e) => tracing::error!(error = %e, "org deletion: stuck sweep failed"),
    }
    match alert_approval_overdue(ctx.control, APPROVAL_OVERDUE_DAYS, APPROVAL_ALERT_REPEAT_HOURS).await {
        Ok(n) => report.approval_overdue = n,
        Err(e) => tracing::error!(error = %e, "org deletion: approval-overdue sweep failed"),
    }
    report
}

/// Decision 4: a self-serve org unpaid for [`UNPAID_DAYS`] enters this flow
/// with the system as requester and the same grace.
///
/// The SQL sweep works from our MIRROR of the subscription, which can lag, and
/// skips orgs with a live comped grant. Before anyone is locked out, each
/// candidate is CONFIRMED with Stripe: if the customer has an `active` or
/// `trialing` subscription there, the org is not unpaid, its watch clock is
/// dropped and nothing is inserted. Without a Stripe client nothing can be
/// confirmed, so nobody enters.
pub async fn unpaid_entry(control: &PgPool, stripe: Option<&StripeClient>) -> Result<usize, sqlx::Error> {
    let due: Vec<(Uuid, String, DateTime<Utc>)> = sqlx::query_as("SELECT * FROM org_deletion_unpaid_sweep($1, $2)")
        .bind(UNPAID_DAYS)
        .bind(50i32)
        .fetch_all(control)
        .await?;
    let mut n = 0;
    for (account, org, since) in due {
        let Some(stripe) = stripe else {
            tracing::error!(alert = true, org = %org, "org deletion: an org is due for the unpaid sweep but Stripe is not configured to confirm it; not entering");
            continue;
        };
        let customer = match stripe_customer_id(control, account).await {
            Ok(c) => c,
            Err(e) => {
                tracing::error!(org = %org, error = %e, "org deletion: unpaid sweep could not read the customer");
                continue;
            }
        };
        if let Some(customer) = customer {
            match crate::billing::seat_lifecycle::with_stripe_deadline(stripe.list_customer_subscriptions(&customer)).await {
                Ok(subs) if subs.iter().any(|(_, st)| matches!(st.as_str(), "active" | "trialing")) => {
                    tracing::warn!(org = %org, %account, "org deletion: Stripe says the subscription is live; dropping the unpaid clock");
                    let _ = unpaid_forget(control, account).await;
                    continue;
                }
                Ok(_) => {}
                Err(e) => {
                    tracing::error!(org = %org, error = %e, "org deletion: could not confirm the subscription with Stripe; not entering");
                    continue;
                }
            }
        }
        let started = insert(control, account, &org, Initiator::Unpaid, None).await?;
        if started.created {
            n += 1;
            tracing::warn!(org = %org, %account, unpaid_since = %since, deletion_id = %started.deletion.id,
                "org deletion: unpaid for 30 days; entering deletion with the system as requester");
        }
    }
    Ok(n)
}

/// Decision 13: an org deleted in the Clerk dashboard keeps its tenant data
/// through the same grace (its membership is already gone).
pub async fn adopt_clerk_deletions(control: &PgPool) -> Result<usize, sqlx::Error> {
    let rows: Vec<(Uuid, String)> = sqlx::query_as("SELECT * FROM org_deletion_adoptable_clerk_deletions($1, $2)")
        .bind(ADOPT_CLERK_DELETES_DAYS)
        .bind(50i32)
        .fetch_all(control)
        .await?;
    let mut n = 0;
    for (account, org) in rows {
        if insert(control, account, &org, Initiator::ClerkDashboard, None).await?.created {
            n += 1;
        }
    }
    Ok(n)
}

/// Day 30: delete the export object, then destroy its wrapped key. The
/// object goes first so a failed delete is retried on the next pass with the
/// key still recorded; once the key is gone, any copy that survived anywhere
/// is unreadable.
pub async fn destroy_expired_exports(control: &PgPool, store: Option<&dyn ExportStore>) -> Result<usize, sqlx::Error> {
    let rows: Vec<(Uuid, Uuid, Option<String>)> = sqlx::query_as("SELECT * FROM org_deletion_expired_exports($1)")
        .bind(50i32)
        .fetch_all(control)
        .await?;
    let mut n = 0;
    for (id, account, key) in rows {
        if let (Some(store), Some(key)) = (store, key.as_deref()) {
            if let Err(e) = store.delete(key).await {
                tracing::error!(deletion_id = %id, alert = true, error = %e, "org deletion: expired export not deleted; retrying next pass");
                continue;
            }
        }
        let mut tx = scoped(control, account).await?;
        sqlx::query(
            "UPDATE org_deletions SET export_key_wrapped = NULL, export_destroyed_at = now(), updated_at = now() \
              WHERE id = $1 AND export_key_wrapped IS NOT NULL",
        )
        .bind(id)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        n += 1;
        tracing::info!(deletion_id = %id, "org deletion: 30-day export and its key destroyed");
    }
    Ok(n)
}

/// A deletion waiting on an operator's approval for longer than this pages.
pub const APPROVAL_OVERDUE_DAYS: i32 = 7;
/// Once it has paged, it pages again this often until approved or canceled
/// (the database stamps `approval_alerted_at`, so two sweeps never both fire).
pub const APPROVAL_ALERT_REPEAT_HOURS: i32 = 24;

/// Alert (the same `alert = true` error line the parked steps use) for every
/// deletion that has sat in `awaiting_approval` for more than `days`, at most
/// once per `repeat_hours` each. Returns how many alerts fired.
pub async fn alert_approval_overdue(control: &PgPool, days: i32, repeat_hours: i32) -> Result<usize, sqlx::Error> {
    let rows: Vec<(Uuid, String, f64, bool)> = sqlx::query_as("SELECT * FROM org_deletion_approval_overdue($1, $2)")
        .bind(days)
        .bind(repeat_hours)
        .fetch_all(control)
        .await?;
    for (id, org, waited, first) in &rows {
        tracing::error!(deletion_id = %id, org, days_waiting = *waited, first_alert = *first, alert = true,
            "org deletion: awaiting operator approval for over {days} days (bin/org_deletion_admin approve, or cancel)");
    }
    Ok(rows.len())
}

pub async fn alert_stuck(control: &PgPool, hours: i32) -> Result<usize, sqlx::Error> {
    let rows: Vec<(Uuid, String, i32, f64, Option<String>)> = sqlx::query_as("SELECT * FROM org_deletion_stuck($1)")
        .bind(hours)
        .fetch_all(control)
        .await?;
    for (id, state, steps, age, last_error) in &rows {
        tracing::error!(deletion_id = %id, state, steps, age_hours = age, last_error = last_error.as_deref().unwrap_or("-"),
            alert = true, "org deletion: stuck");
    }
    Ok(rows.len())
}

/// A session-level Postgres advisory lock on a connection of its own, held for
/// the length of an owner export. The connection is detached from the pool and
/// closed on release (or on drop), so the lock can never outlive the export
/// on a pooled connection.
pub struct ExportGuard {
    conn: Option<sqlx::PgConnection>,
}

impl ExportGuard {
    pub async fn try_acquire(control: &PgPool, org: &str) -> Result<Option<Self>, sqlx::Error> {
        let mut conn = control.acquire().await?.detach();
        let got: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock(hashtextextended('org_deletion_export:' || $1, 0))")
            .bind(org)
            .fetch_one(&mut conn)
            .await?;
        if !got {
            use sqlx::Connection;
            let _ = conn.close().await;
            return Ok(None);
        }
        Ok(Some(Self { conn: Some(conn) }))
    }

    pub async fn release(&mut self) {
        if let Some(conn) = self.conn.take() {
            use sqlx::Connection;
            // Closing the session releases the lock.
            let _ = conn.close().await;
        }
    }
}

// ── Founder download after grace ─────────────────────────────────────────

/// Decrypt a stored export (operator tool). Fails once the key is destroyed.
pub async fn read_export(store: &dyn ExportStore, kek: &[u8; 32], next_kek: Option<&[u8; 32]>, d: &OrgDeletion) -> Result<Vec<u8>, String> {
    let (Some(object), Some(wrapped)) = (d.export_object_key.as_deref(), d.export_key_wrapped.as_deref()) else {
        return Err("this deletion has no export (not written yet, or its key was destroyed at day 30)".into());
    };
    let key = coralswarm_core::kek::unwrap_dek_with_rotation(kek, next_kek, wrapped).map_err(|e| format!("unwrap: {e}"))?;
    let bytes = store.get(object, &key).await?;
    if let Some(want) = d.export_sha256.as_deref() {
        if sha256_hex(&bytes) != want {
            return Err("export digest does not match the recorded sha256".into());
        }
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn steps_match_the_migration_and_split_at_the_point_of_no_return() {
        let sql = include_str!("../../migrations_control/0387_org_deletions.sql");
        for s in STEPS {
            assert!(sql.contains(&format!("'{s}'")), "{s} missing from 0387's step CHECK");
        }
        assert_eq!(STEPS[FIRST_DESTRUCTIVE_STEP], "stripe_sweep");
        assert_eq!(STEPS[FIRST_DESTRUCTIVE_STEP + 1], "revoke_access");
        assert!(!is_destructive("stripe_cancel"));
        assert!(is_destructive("database"));
        assert_eq!(STEPS[STEPS.len() - 1], "notify_done");
    }

    #[test]
    fn the_cancel_key_is_the_one_1538_uses() {
        assert_eq!(deleted_org_cancel_key("sub_123"), "org-deleted-cancel:sub_123");
    }

    #[test]
    fn name_confirmation_is_trimmed_case_insensitive_and_never_empty() {
        assert!(name_matches("  acme inc ", "Acme Inc"));
        assert!(!name_matches("acme", "Acme Inc"));
        assert!(!name_matches("", ""));
        assert!(!name_matches("   ", "  "));
    }

    #[test]
    fn lockout_codes_cover_every_live_state() {
        assert_eq!(lockout_code("requested"), Some("org_deletion_pending"));
        assert_eq!(lockout_code("grace"), Some("org_deletion_pending"));
        assert_eq!(lockout_code("awaiting_approval"), Some("org_deletion_in_progress"));
        assert_eq!(lockout_code("destroying"), Some("org_deletion_in_progress"));
        assert_eq!(lockout_code("done"), None);
        assert_eq!(lockout_code("canceled"), None);
    }

    #[test]
    fn backoff_doubles_and_caps() {
        assert_eq!(backoff_secs(1), 30);
        assert_eq!(backoff_secs(2), 60);
        assert_eq!(backoff_secs(50), 3600);
    }
}

#[path = "org_deletion_pg_tests.rs"]
#[cfg(test)]
mod pg_tests;
