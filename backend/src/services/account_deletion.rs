//! Account deletion (settings launch PR 2.4, decisions 14, 17, 19, 24): the
//! resumable job behind `POST /v1/me/delete-account` and the Clerk
//! `user.deleted` webhook.
//!
//! # State machine (migration 0370)
//!
//! ```text
//!                 POST (session, fresh re-verification, typed email)
//!                                  |
//!                                  v
//!   +-----------------------  grace  ------------------------+
//!   | lockout, Stripe ended, tokens revoked, email + cancel   |
//!   | link. Nothing destructive. Lasts until purge_after      |
//!   | (7 days). Status + cancel are the only routes reachable.|
//!   +---------+---------------------------+-------------------+
//!             | cancel (signed link, or   | purge_after passed, eligibility
//!             | signed in)                | re-checked, state flipped by a
//!             v                           | guarded UPDATE
//!        cancelled                        v
//!     (access restored;                running  --- every step ok --> completed
//!      Stripe stays ended)       (destructive steps; no way back)     (tombstone)
//! ```
//!
//! A Clerk-initiated delete (Clerk dashboard / API, observed through the
//! `user.deleted` webhook) skips grace: the Clerk user is already gone, so
//! there is nothing to cancel and nobody to sign in. It starts in `running`.
//!
//! # Steps
//!
//! Each step is checkpointed in `completed_steps` and safe to run twice.
//!
//! | phase | step | what |
//! |---|---|---|
//! | grace | `detach` | webhook path only: remove team-org memberships (seats, member workspaces). A user-initiated deletion that finds one cancels itself instead |
//! | grace | `stripe_cancel` | end the subscription now (no proration, no final invoice) |
//! | grace | `revoke_tokens` | desktop / add-in / session bearers, pairing codes |
//! | grace | `notify_started` | email with the cancel link (best effort, alerts on failure) |
//! | running | `forfeit` | extra-usage credits forfeited by one ledger adjustment (decision 19) |
//! | running | `connectors` | best-effort upstream revoke, then delete connector rows |
//! | running | `keys` | private key, escrow material, account DEK |
//! | running | `data` | atoms (existing cascade), reefs, graphs, every account- and user-keyed row |
//! | running | `tenant_planes` | member-private workspaces in org databases |
//! | running | `stripe_customer` | delete the Stripe customer (PII, saved cards) |
//! | running | `clerk` | delete the user's personal Clerk org, then the Clerk user |
//! | running | `blob_drain` | wait for the durable R2 cleanup queues |
//! | running | `notify_completed` | "your account was deleted" email |
//! | running | `tombstone` | final user-keyed purge, anonymize `users`, mark completed |
//!
//! # Prod runs as `coralswarm_app`
//!
//! Not the owner, subject to RLS and to 0083's deliberate revokes. Everything
//! the app role cannot do is a narrow SECURITY DEFINER function in 0370 that
//! only acts for one deletion row in the state that permits it; the pg suite
//! runs the job on an app-role pool so a missing privilege fails there.
//!
//! # What is deleted, and what is kept (and why)
//!
//! Deleted: every graph with its atoms, reefs, edges, sources, transcripts,
//! conversations and sync ops; blobs (the atom cascade feeds the durable
//! `DeleteSourceBlob` queue); connector grants and upstream tokens where a
//! revoke primitive exists; every device, add-in and session token; the
//! private key, escrow material and account DEK; the user's rows in every
//! account (including organizations already left); the Stripe customer; the
//! Clerk personal org and user.
//!
//! Kept, on purpose: the `accounts` row and an anonymized `users` row, the
//! Stripe subscription ids, and the financial audit trail ([`KEEP_FINANCIAL`]):
//! the 0321 entry fence and the immutability triggers refuse deleting it and no
//! fence is disabled. Also the append-only `retraction_audit_log` (ids and
//! source identifiers only, the actor is the anonymized user id).
//!
//! # Shared personal accounts
//!
//! A personal account other users are members of (`POST /v1/account/members`)
//! is never wiped by one member's deletion: the request is refused (409
//! `shared_personal_account`) and the SQL function behind every step
//! (`account_deletion_deletable_accounts`) excludes such accounts, so even a
//! Clerk-initiated delete only removes the departing user's own rows there.

use std::time::{Duration, Instant};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use serde::Serialize;
use serde_json::{json, Value};
use sha2::Sha256;
use sqlx::PgPool;
use uuid::Uuid;

use crate::billing::cancel::{cancel_account_subscription_now, CancelOutcome};
use crate::billing::stripe_client::{StripeClient, StripeError};
use crate::clerk_admin::{ClerkAdminClient, ClerkAdminError};
use crate::services::blob_store::PromotionBlobStore;

// ── Steps ────────────────────────────────────────────────────────────────

/// The ordered step names. Mirrors `account_deletions_steps_known` in 0370.
pub const STEPS: [&str; 14] = [
    "detach",
    "stripe_cancel",
    "revoke_tokens",
    "notify_started",
    "forfeit",
    "connectors",
    "keys",
    "data",
    "tenant_planes",
    "stripe_customer",
    "clerk",
    "blob_drain",
    "notify_completed",
    "tombstone",
];

/// Steps before this index run during grace; the rest are destructive.
pub const FIRST_DESTRUCTIVE_STEP: usize = 4;

pub fn is_destructive(step: &str) -> bool {
    STEPS.iter().position(|s| *s == step).is_some_and(|i| i >= FIRST_DESTRUCTIVE_STEP)
}

/// Grace period before the destructive steps (decision 24).
pub const GRACE_DAYS: i64 = 7;
/// Clerk's `strict` reverification preset is `{afterMinutes: 10, level:
/// "second_factor"}`; a session verified this recently (strictly under, as
/// Clerk compares `afterMinutes > age`) may delete an account (decision 24).
pub const REVERIFY_MAX_MINUTES: i64 = 10;

/// Port of Clerk's `checkReverificationAuthorization` for the `second_factor`
/// level, over the session token's `fva` claim `[first_minutes,
/// second_minutes]` (`-1` = factor not enrolled):
/// * a user with a second factor must have verified THAT factor recently (a
///   fresh first factor alone is not enough, and only the second factor's age
///   refreshes when they re-verify);
/// * a user with only a first factor needs that one fresh;
/// * a user with neither, or a v1 token with no `fva`, never passes.
pub fn reverification_ok(fva: Option<[i64; 2]>) -> bool {
    let Some([first, second]) = fva else { return false };
    let recent = |age: i64| age != -1 && age < REVERIFY_MAX_MINUTES;
    if second != -1 {
        recent(second)
    } else {
        recent(first)
    }
}

/// Rows removed per statement in the big deletes.
const DELETE_BATCH: i64 = 5000;
/// Atoms removed per `data` call (each goes through the audited cascade, one
/// transaction apiece).
const ATOM_BATCH: i64 = 100;
/// Reef rows removed per call before the graph itself is deleted.
const NODE_BATCH: i64 = 2000;
/// How long a worker holds a claimed row between lease renewals.
pub const LEASE_SECS: i32 = 120;
/// Longest one claim keeps working before it hands the row back.
const RUN_BUDGET: Duration = Duration::from_secs(90);
/// Re-check interval while waiting on something outside the job.
const WAIT_SECS: i64 = 300;

// ── Table classification ─────────────────────────────────────────────────
//
// `pg_tests::every_table_is_classified` reads the live catalog (as the app
// role) and fails if a table that carries `account_id` or `graph_id`, or has a
// foreign key to users/accounts/graphs, is in none of these lists.

/// Financial and audit tables. NEVER deleted or modified by the job (the 0321
/// fence and the immutability triggers refuse it, and they are the audit trail).
#[allow(dead_code)] // classification, read by the schema-completeness test
pub const KEEP_FINANCIAL: &[&str] = &[
    // The tombstone anchor itself: every financial row hangs off it.
    "accounts",
    "account_deletions",
    "credit_balances",
    "credit_ledger",
    "enforcement_events",
    "extra_usage_balances",
    "extra_usage_ledger",
    "extra_usage_periods",
    "extra_usage_reloads",
    "grant_seat_tiers",
    "grants",
    "llm_usage",
    "llm_usage_pending",
    "managed_attempt_receipts",
    "managed_budget_periods",
    "managed_credit_reservations",
    "managed_embedding_inputs",
    "managed_enrichment_inputs",
    "managed_receipt_recovery_queue",
    "member_allowance_periods",
    "query_billing_state",
    "seat_grant_sources",
    "stripe_subscriptions",
    "stripe_webhook_events",
    "tenant_databases",
    // Self-serve org trials (control 0382, #1521): one row per TEAM org
    // account (FK to `accounts`, which is kept as the tombstone anchor), with
    // the org's Stripe subscription id, the Checkout session and the card
    // fingerprint the trial was taken with. Billing state of the org, not of
    // the person: a personal account never has a row, and the creator
    // pointer (`creator_user_id`) is `ON DELETE SET NULL` to the anonymized
    // `users` row. Kept for the same reason as `stripe_subscriptions`.
    "org_trials",
    // One trial per user AND per card (0382, founder decision #9). Kept on
    // purpose, as anti-abuse records: deleting them would let anyone get a
    // second free trial by deleting their account and signing up again with
    // the same card. A row holds a claim kind, a value (the `users.id` uuid,
    // which after the tombstone points at an anonymized row, or the Stripe
    // card FINGERPRINT, a Stripe-issued hash that is not the card number and
    // cannot be turned back into one) and the ORG's account id (no FK: the
    // claim outlives the org). No email, no name, no content. Card claims
    // are deleted 24 months after they were made (control 0385, by the
    // self-serve cleanup sweep); user claims are kept. Deleting by the
    // user's `account_id` would match nothing anyway: `account_id` here is
    // the team org's, never the person's. The app role cannot delete from it
    // either (only the 0382 definers write it).
    "org_trial_claims",
    // Self-serve provisioning jobs (control 0381, A1): one row per team org,
    // `account_id` FK RESTRICT to the kept `accounts` row. Infrastructure
    // bookkeeping next to `tenant_databases` (slug, secret ref, step, status;
    // never a DSN or key), read-only to the app role. A personal account
    // never has one.
    "tenant_provisioning_jobs",
    // In-app org deletion's unpaid clock (control 0387): one row per TEAM
    // org account whose subscription is not live (FK to the kept `accounts`
    // row). Billing state of the org, never a person's: no user column.
    "org_unpaid_watch",
    // Work queue, not content: pending jobs for a deleted graph finish cleanly
    // ("a graph that resolves to no account is not an error"), and the
    // `DeleteSourceBlob` jobs the atom cascade enqueues live here.
    "jobs",
    "job_dependencies",
    // Managed-budget bookkeeping that the receipts above reference.
    "corpus_enrichment_units",
];

/// Durable cleanup queues owned by existing workers. Kept until they drain
/// (the `blob_drain` step waits on them); they hold locators, never content.
#[allow(dead_code)]
pub const DRAIN_QUEUES: &[&str] = &[
    "source_purge_dependencies",
    "source_purge_events",
    "source_purge_reviews",
    "unattached_capture_uploads",
];

/// Operational audit with ids only (no email, name or content), kept. The
/// retraction log is append-only by design (0196: the app role has no UPDATE
/// or DELETE) and holds uuids and source identifiers; its actor is the
/// now-anonymized user id.
///
/// The retired Library rollout proof ledger (0364: `library_rollout_runs`,
/// `_samples`, `_windows`; nothing writes it since the reader became the
/// default) is kept for a different reason: its `account_id` was the
/// OPERATOR's account, not the user's. A user appears only pseudonymously as
/// `tenant_key = 'consumer:<account uuid>'`, with no content. Deleting by the
/// user's `account_id` would remove nothing of theirs, and would wipe the
/// ledger if an operator deleted their own account.
#[allow(dead_code)]
pub const KEEP_AUDIT: &[&str] = &[
    "library_rollout_runs",
    "library_rollout_samples",
    "library_rollout_windows",
    "member_ocean_lifecycle_log",
    "membership_events",
    "retraction_audit_log",
    // In-app org deletion job + tombstone (control 0387). `account_id` is the
    // deleted TEAM org's (never a personal account), kept forever with the
    // `accounts` row as the record that the org was deleted;
    // `requested_by_user_id` is `ON DELETE SET NULL` to the anonymized
    // `users` row. Ids, states, counts and digests only: no email, no name,
    // no content (the source manifest holds per-source-type COUNTS).
    "org_deletions",
];

/// Tables whose only link to the user is a nullable pointer (`ON DELETE SET
/// NULL`/RESTRICT) to the anonymized `users` row; the pointed-to row holds no
/// PII after the tombstone, so the pointer is harmless.
#[allow(dead_code)]
///
/// `org_selfserve_creations` (control 0384, self-serve signup A2): both
/// `user_id` and `account_id` are `ON DELETE SET NULL`, and the row is the
/// cleanup sweep's bookkeeping for an org that may still exist in Clerk (its
/// creator deleting their account must not strand the org, A2 security
/// review S9). It holds the org name the creator typed and, for its first
/// 24 hours, a `cap_bucket`: for a company address the registrable email
/// DOMAIN (`acme.com`), and for a free-mail address an HMAC-SHA256 of the
/// creator's canonical mailbox under a server key derived from
/// `MASTER_KEK_HEX` (`*freemail:<hex>`; #1540). The address itself is never
/// stored, but that digest is pseudonymous data about the creator: the server
/// can recompute it for a known address. The cleanup sweep sets `cap_bucket`
/// to NULL once the row is older than 24 hours (the caps' longest window),
/// so after a day the row holds no email-derived value at all.
pub const USER_POINTER_ONLY: &[&str] = &["non_supersession_pair", "org_selfserve_creations"];

/// Tables with a `graph_id` column and NO foreign key to `graphs`, so deleting
/// the graph would not reach them.
pub const PURGE_BY_GRAPH_NO_FK: &[&str] = &[
    "fact_supersession_edges",
    "meeting_transcripts",
    "node_semantic_atoms",
    "op_log",
    "semantic_atom_embeddings",
    "semantic_atom_passages",
    "semantic_atom_windows",
    "semantic_atoms",
    "source_version_chunks",
    "source_version_observations",
    "temporal_evidence_records",
    "transcript_corrections",
];

/// Tables the app role cannot delete from (0083 revokes and append-only
/// grants); removed by `account_deletion_purge_restricted` (0370).
#[allow(dead_code)]
pub const PURGE_VIA_DEFINER: &[&str] = &[
    "escrow_shares",
    "node_case_fold_log",
    "reef_identity_corrections",
    "corpus_capture_admissions",
];

/// `DELETE FROM t WHERE account_id = $1`, in an order that removes children
/// first. Includes org-only tables: a personal account has no rows there, and
/// if one ever does, it is deleted rather than left behind.
pub const PURGE_BY_ACCOUNT: &[&str] = &[
    "activity_events",
    "addin_microsoft_sessions",
    "addin_microsoft_links",
    "addin_pairing_codes",
    "addin_tokens",
    "billing_notices",
    "calendar_prefs",
    "chat_cold_start_notices",
    "cold_start_notices",
    "composio_capture_scopes",
    "composio_connections",
    "connector_audit_log",
    "connector_health",
    "connector_installs",
    "connector_sources",
    "conversations",
    "corpus_enrichment_runs",
    "escrow_recovery_log",
    "escrow_recovery_pubkey",
    "escrow_wrapped_keys",
    "extra_usage_settings",
    "extraction_cache",
    "github_code_groups",
    "github_installations",
    "github_watched_repos",
    "gmail_everything_mode",
    "gmail_hidden_messages",
    "gmail_watched_threads",
    "google_calendar_sync_attempts",
    "google_chat_watched_spaces",
    "google_meet_captures",
    "google_meet_ingested_conferences",
    "google_meet_sync_state",
    "google_watch_channels",
    "graph_mail_routes",
    "graph_subscriptions",
    "journal_rollups",
    "mcp_tasks",
    "mcp_tool_calls",
    "microsoft_admin_consents",
    "microsoft_calendar_sync_attempts",
    "microsoft_calendar_sync_state",
    "oauth_tokens",
    "org_auto_add_settings",
    "org_auto_adds",
    "org_domains",
    "org_join_request_digests",
    "org_join_requests",
    "outlook_capture_receipts",
    "outlook_everything_mode",
    "outlook_schedule_intents",
    "outlook_watched_conversations",
    "outlook_workflows",
    "queries",
    "query_audit_log",
    "query_history",
    "selected_evidence_publications",
    "slack_huddle_captures",
    "slack_huddle_consents",
    "slack_integrations",
    "slack_member_channels",
    "slack_unmatched_email_notices",
    "slack_watched_channels",
    "slot_reservations",
    "source_connections",
    "team_seat_grants",
    "teams",
    "teams_container_watchers",
    "teams_meeting_captures",
    "teams_meeting_installations",
    "teams_watched_resources",
    "user_account_prefs",
    "user_active_graph",
    "account_prefs",
];

/// Same idea, keyed on a column other than `account_id`.
pub const PURGE_BY_OTHER_ACCOUNT_COLUMN: &[(&str, &str)] =
    &[("teams_container_subscriptions", "owner_account_id")];

/// Reached by cascading from `graphs` (graph FK `ON DELETE CASCADE`), or
/// deliberately handled later: `member_ocean_desired_state` is the
/// `tenant_planes` step's work list and is removed by `tombstone`.
#[allow(dead_code)]
pub const PURGE_VIA_GRAPH_OR_LATER: &[&str] = &[
    // Deleted explicitly by the `data` step (after its atoms and reefs).
    "graphs",
    "coding_session_shares",
    "meeting_shares",
    "member_ocean_desired_state",
    // Indirect graph cascades: state -> generations -> staged rows. These
    // have no direct graph FK.
    "library_listing_generations",
    "library_listing_staged_sources",
    "library_listing_staged_outcomes",
];

/// Stop-ingestion set run by the `connectors` step. Subset of
/// [`PURGE_BY_ACCOUNT`]; everything else is removed by `data`.
pub const CONNECTOR_TABLES: &[&str] = &[
    "oauth_tokens",
    "composio_connections",
    "composio_capture_scopes",
    "connector_installs",
    "connector_sources",
    "slack_integrations",
    "slack_huddle_consents",
    "slack_member_channels",
    "slack_watched_channels",
    "github_installations",
    "github_watched_repos",
    "github_code_groups",
    "gmail_everything_mode",
    "gmail_hidden_messages",
    "gmail_watched_threads",
    "google_chat_watched_spaces",
    "google_meet_ingested_conferences",
    "google_meet_sync_state",
    "google_watch_channels",
    "graph_mail_routes",
    "graph_subscriptions",
    "microsoft_calendar_sync_state",
    "outlook_everything_mode",
    "outlook_watched_conversations",
    "outlook_workflows",
    "teams_container_watchers",
    "teams_watched_resources",
    "source_connections",
    "calendar_prefs",
    "connector_health",
];

// ── Rows and status ──────────────────────────────────────────────────────

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct Deletion {
    pub id: Uuid,
    pub user_id: Uuid,
    pub account_id: Uuid,
    pub clerk_user_id: Option<String>,
    pub initiated_by: String,
    pub state: String,
    pub completed_steps: Vec<String>,
    pub clerk_user_deleted: bool,
    pub stripe_outcome: Option<String>,
    pub forfeited_extra_usage_micro_usd: i64,
    pub attempts: i32,
    pub last_error: Option<String>,
    pub next_attempt_at: DateTime<Utc>,
    pub lease_until: Option<DateTime<Utc>>,
    pub purge_after: DateTime<Utc>,
    pub running_since: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub cancelled_at: Option<DateTime<Utc>>,
}

impl Deletion {
    pub fn next_step(&self) -> Option<&'static str> {
        STEPS.iter().copied().find(|s| !self.completed_steps.iter().any(|d| d == s))
    }

    /// The public status shape (also embedded in the pre-start eligibility
    /// read, and returned to the user during grace).
    pub fn status_json(&self) -> Value {
        json!({
            "id": self.id,
            "state": self.state,
            "initiated_by": self.initiated_by,
            "purge_after": self.purge_after,
            "can_cancel": self.state == "grace" && self.initiated_by == "user",
            "current_step": self.next_step(),
            "steps": STEPS.iter().map(|s| json!({
                "name": s,
                "done": self.completed_steps.iter().any(|d| d == s),
            })).collect::<Vec<_>>(),
            "retrying": self.last_error.is_some() && matches!(self.state.as_str(), "grace" | "running"),
            "created_at": self.created_at,
            "completed_at": self.completed_at,
            "cancelled_at": self.cancelled_at,
        })
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct TeamOrg {
    pub account_id: Uuid,
    pub clerk_org_id: String,
    /// Subdomain slug when the org has a tenant database.
    pub slug: Option<String>,
    pub role: String,
    /// The caller is this org's in-app OWNER (role `owner`, or the admin who
    /// created it). An owner cannot simply leave: they delete the
    /// organization first (`/v1/orgs/{clerk_org_id}/deletion`, control 0387,
    /// when `ORG_DELETION_ENABLED`), after which the org is revoked and no
    /// longer blocks this deletion.
    pub owner: bool,
    /// Whether that in-app org deletion is available for this org right now
    /// (flag on, self-serve org, the caller is the owner).
    pub can_delete_org: bool,
}

/// Another live member of one of the caller's personal accounts.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct SharedMember {
    pub account_id: Uuid,
    pub user_id: Uuid,
    pub email: String,
    pub name: Option<String>,
    pub role: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Initiator {
    User,
    ClerkWebhook,
}

impl Initiator {
    fn as_str(self) -> &'static str {
        match self {
            Initiator::User => "user",
            Initiator::ClerkWebhook => "clerk_webhook",
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum StartError {
    #[error("the account belongs to {} team organization(s)", .0.len())]
    BlockedByOrgs(Vec<TeamOrg>),
    #[error("the personal account has {} other member(s)", .0.len())]
    SharedAccount(Vec<SharedMember>),
    #[error("database error: {0}")]
    Db(#[from] sqlx::Error),
}

pub struct Started {
    pub deletion: Deletion,
    /// False when a deletion already existed (idempotent re-request).
    pub created: bool,
}

fn db_err(e: impl std::fmt::Display) -> String {
    format!("database error: {e}")
}

async fn scoped(
    control: &PgPool,
    account_id: Uuid,
) -> Result<sqlx::Transaction<'static, sqlx::Postgres>, String> {
    crate::tenancy::scope::begin_account_scoped(control, account_id)
        .await
        .map_err(db_err)
}

// ── Queries shared with the routes ───────────────────────────────────────

const TEAM_ORGS_SQL: &str = "\
SELECT a.id, a.clerk_org_id, am.role, \
       (SELECT t.slug FROM tenant_databases t WHERE t.account_id = a.id LIMIT 1), \
       (am.role = 'owner' OR (am.role = 'admin' AND a.owner_user_id = am.user_id)), \
       EXISTS (SELECT 1 FROM tenant_provisioning_jobs j \
                WHERE j.clerk_org_id = a.clerk_org_id AND j.account_id = a.id AND j.status = 'ready') \
  FROM account_members am \
  JOIN accounts a ON a.id = am.account_id \
 WHERE am.user_id = $1 \
   AND a.clerk_org_id IS NOT NULL \
   AND NOT EXISTS (SELECT 1 FROM org_revocations r WHERE r.clerk_org_id = a.clerk_org_id) \
   AND NOT EXISTS ( \
       SELECT 1 FROM personal_orgs p \
         JOIN users u ON u.clerk_user_id = p.clerk_user_id \
        WHERE p.clerk_org_id = a.clerk_org_id AND u.id = am.user_id) \
 ORDER BY a.clerk_org_id";

type TeamOrgRow = (Uuid, String, String, Option<String>, bool, bool);

fn to_team_orgs(rows: Vec<TeamOrgRow>) -> Vec<TeamOrg> {
    let deletion_on = crate::services::org_deletion::enabled();
    rows.into_iter()
        .map(|(account_id, clerk_org_id, role, slug, owner, self_serve)| TeamOrg {
            account_id,
            clerk_org_id,
            slug,
            role,
            owner,
            can_delete_org: deletion_on && owner && self_serve,
        })
        .collect()
}

/// The team organizations `user_id` is a live member of. A personal org (the
/// caller's own, `personal_orgs`) is not a team org, and neither is a revoked
/// one. Same predicate as `mcp::tenant_selection::has_any_org_membership`,
/// but returning the rows.
pub async fn blocking_team_orgs(control: &PgPool, user_id: Uuid) -> Result<Vec<TeamOrg>, sqlx::Error> {
    Ok(to_team_orgs(sqlx::query_as(TEAM_ORGS_SQL).bind(user_id).fetch_all(crate::tenancy::scope::request_read(control)).await?))
}

/// Every account this user owns as their personal home: the plain personal
/// account and, once adopted, the account that stands for their personal org.
/// The first element is the primary (plain personal account when present).
pub async fn personal_account_ids(control: &PgPool, user_id: Uuid) -> Result<Vec<Uuid>, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT a.id FROM accounts a \
          WHERE a.owner_user_id = $1 \
            AND (a.clerk_org_id IS NULL OR EXISTS ( \
                  SELECT 1 FROM personal_orgs p JOIN users u ON u.clerk_user_id = p.clerk_user_id \
                   WHERE p.clerk_org_id = a.clerk_org_id AND u.id = $1)) \
          ORDER BY (a.clerk_org_id IS NOT NULL), a.created_at, a.id",
    )
    .bind(user_id)
    .fetch_all(crate::tenancy::scope::request_read(control))
    .await
}

/// The personal accounts a deletion may wipe: [`personal_account_ids`] minus
/// any account another user is a live member of.
pub async fn deletable_account_ids(control: &PgPool, user_id: Uuid) -> Result<Vec<Uuid>, sqlx::Error> {
    sqlx::query_scalar("SELECT account_deletion_deletable_accounts($1)")
        .bind(user_id)
        .fetch_all(control)
        .await
}

const SHARED_SQL: &str = "\
SELECT m.account_id, m.user_id, u.email, u.name, m.role \
  FROM account_members m JOIN users u ON u.id = m.user_id \
 WHERE m.user_id <> $1 \
   AND m.account_id IN ( \
       SELECT a.id FROM accounts a \
        WHERE a.owner_user_id = $1 \
          AND (a.clerk_org_id IS NULL OR EXISTS ( \
                SELECT 1 FROM personal_orgs p JOIN users pu ON pu.clerk_user_id = p.clerk_user_id \
                 WHERE p.clerk_org_id = a.clerk_org_id AND pu.id = $1))) \
 ORDER BY m.account_id, m.joined_at";

fn to_shared(rows: Vec<(Uuid, Uuid, String, Option<String>, String)>) -> Vec<SharedMember> {
    rows.into_iter()
        .map(|(account_id, user_id, email, name, role)| SharedMember { account_id, user_id, email, name, role })
        .collect()
}

/// Other users who are live members of one of this user's personal accounts
/// (`POST /v1/account/members` lets a personal account have co-owners). Such
/// an account holds their data too and is never wiped by this deletion.
pub async fn shared_personal_members(control: &PgPool, user_id: Uuid) -> Result<Vec<SharedMember>, sqlx::Error> {
    Ok(to_shared(sqlx::query_as(SHARED_SQL).bind(user_id).fetch_all(crate::tenancy::scope::request_read(control)).await?))
}

pub async fn get_by_user(control: &PgPool, account_id: Uuid, user_id: Uuid) -> Result<Option<Deletion>, sqlx::Error> {
    let mut tx = crate::tenancy::scope::begin_request_account_read(control, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
    let row = sqlx::query_as::<_, Deletion>(
        "SELECT * FROM account_deletions WHERE user_id = $1 AND state <> 'cancelled'",
    )
    .bind(user_id)
    .fetch_optional(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(row)
}

/// Start (or join) the deletion for `user_id`. Idempotent: a second call
/// returns the existing row with `created = false` and changes nothing.
///
/// For [`Initiator::User`] the team-org rule (decisions 14, 17) and the
/// shared-account rule are re-checked inside the transaction that inserts the
/// row, under a per-user advisory lock, and the row starts in `grace`. A
/// membership that appears after this commits is caught again when grace ends
/// (see [`begin_destructive`]). A webhook-initiated delete skips both rules
/// and starts in `running`: Clerk already deleted the user, so refusing would
/// only strand data; the `detach` step removes the memberships first.
pub async fn start(
    control: &PgPool,
    user_id: Uuid,
    account_id: Uuid,
    clerk_user_id: Option<&str>,
    initiator: Initiator,
) -> Result<Started, StartError> {
    let mut tx = crate::tenancy::scope::begin_account_scoped(control, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
    // Serialize concurrent starts for one user (route + webhook, double click).
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended('account_deletion_user:' || $1::text, 0))")
        .bind(user_id)
        .execute(&mut *tx)
        .await?;
    if let Some(existing) = sqlx::query_as::<_, Deletion>(
        "SELECT * FROM account_deletions WHERE user_id = $1 AND state <> 'cancelled'",
    )
    .bind(user_id)
    .fetch_optional(&mut *tx)
    .await?
    {
        tx.commit().await?;
        return Ok(Started { deletion: existing, created: false });
    }
    if initiator == Initiator::User {
        let orgs = to_team_orgs(sqlx::query_as(TEAM_ORGS_SQL).bind(user_id).fetch_all(&mut *tx).await?);
        if !orgs.is_empty() {
            tx.rollback().await?;
            return Err(StartError::BlockedByOrgs(orgs));
        }
        let shared = to_shared(sqlx::query_as(SHARED_SQL).bind(user_id).fetch_all(&mut *tx).await?);
        if !shared.is_empty() {
            tx.rollback().await?;
            return Err(StartError::SharedAccount(shared));
        }
    }
    let deletion = match initiator {
        Initiator::User => sqlx::query_as::<_, Deletion>(
            "INSERT INTO account_deletions (user_id, account_id, clerk_user_id, initiated_by, state, purge_after) \
             VALUES ($1, $2, $3, 'user', 'grace', now() + make_interval(days => $4)) RETURNING *",
        )
        .bind(user_id)
        .bind(account_id)
        .bind(clerk_user_id)
        .bind(GRACE_DAYS as i32)
        .fetch_one(&mut *tx)
        .await?,
        Initiator::ClerkWebhook => sqlx::query_as::<_, Deletion>(
            "INSERT INTO account_deletions \
                 (user_id, account_id, clerk_user_id, initiated_by, state, purge_after, running_since, clerk_user_deleted) \
             VALUES ($1, $2, $3, 'clerk_webhook', 'running', now(), now(), true) RETURNING *",
        )
        .bind(user_id)
        .bind(account_id)
        .bind(clerk_user_id)
        .fetch_one(&mut *tx)
        .await?,
    };
    lockout_tx(&mut tx, user_id).await?;
    tx.commit().await?;
    tracing::warn!(
        deletion_id = %deletion.id, %user_id, %account_id, initiated_by = initiator.as_str(),
        state = %deletion.state, "account deletion started"
    );
    Ok(Started { deletion, created: true })
}

/// Revoke every non-Clerk bearer credential in the same transaction that
/// creates the deletion, so none of them works after commit.
async fn lockout_tx(tx: &mut sqlx::Transaction<'static, sqlx::Postgres>, user_id: Uuid) -> Result<(), sqlx::Error> {
    for sql in [
        "UPDATE addin_tokens SET revoked_at = COALESCE(revoked_at, now()) WHERE owner_user_id = $1",
        "UPDATE addin_microsoft_sessions SET revoked_at = COALESCE(revoked_at, now()) WHERE user_id = $1",
        "DELETE FROM addin_pairing_codes WHERE owner_user_id = $1",
        "DELETE FROM sessions WHERE user_id = $1",
    ] {
        sqlx::query(sql).bind(user_id).execute(&mut **tx).await?;
    }
    Ok(())
}

/// Record that Clerk no longer has this user. Returns whether a (non
/// cancelled) deletion for that Clerk id exists.
pub async fn note_clerk_deleted(control: &PgPool, clerk_user_id: &str) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar("SELECT account_deletion_note_clerk_deleted($1)")
        .bind(clerk_user_id)
        .fetch_one(control)
        .await
}

/// State of a (non cancelled) deletion for an account, for the Stripe webhook.
pub async fn state_for_account(control: &PgPool, account_id: Uuid) -> Result<Option<String>, sqlx::Error> {
    sqlx::query_scalar("SELECT account_deletion_state_for_account($1)")
        .bind(account_id)
        .fetch_one(control)
        .await
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelResult {
    Cancelled,
    /// The deletion was already cancelled (a replayed cancel link). The user's
    /// intent is met, so the routes answer success, not an error.
    AlreadyCancelled,
    /// Already running or completed: the point of no return passed.
    TooLate,
    NotFound,
}

/// Cancel a deletion that is still in grace. One guarded UPDATE in the
/// database decides a race with the worker's grace -> running transition.
pub async fn cancel(control: &PgPool, deletion_id: Uuid) -> Result<CancelResult, sqlx::Error> {
    let r: String = sqlx::query_scalar("SELECT account_deletion_cancel($1)")
        .bind(deletion_id)
        .fetch_one(control)
        .await?;
    Ok(match r.as_str() {
        "cancelled" => CancelResult::Cancelled,
        "already_cancelled" => CancelResult::AlreadyCancelled,
        "not_found" => CancelResult::NotFound,
        _ => CancelResult::TooLate,
    })
}

// ── Cancel-link tokens ───────────────────────────────────────────────────

type HmacSha256 = Hmac<Sha256>;

/// Derive the cancel-link signing key from `ikm` (HKDF-SHA256 via
/// [`crate::crypto::kdf`], purpose `ACCOUNT_DELETION_CANCEL`), so the key is NOT
/// the secret it came from. That matters because `OAUTH_STATE_SECRET` is also
/// used, as a fallback, to key digests an admin can read
/// (`services::query_audit`): signing cancel links with the raw secret would make
/// that a signing oracle.
pub fn derive_cancel_key(ikm: &[u8]) -> [u8; 32] {
    crate::crypto::kdf::derive_key(ikm, crate::crypto::kdf::purpose::ACCOUNT_DELETION_CANCEL)
}

/// The signing key from the environment: `ACCOUNT_DELETION_LINK_SECRET` when
/// set (preferred; a dedicated secret), otherwise derived from
/// `OAUTH_STATE_SECRET`. `None` when neither is configured.
pub fn cancel_key_from_env() -> Option<[u8; 32]> {
    let ikm = ["ACCOUNT_DELETION_LINK_SECRET", "OAUTH_STATE_SECRET"]
        .iter()
        .find_map(|k| std::env::var(k).ok().filter(|v| !v.is_empty()))?;
    Some(derive_cancel_key(ikm.as_bytes()))
}

/// `{deletion_id}.{hex hmac}` under a key from [`derive_cancel_key`]. Signed,
/// not stored: the worker can mint the same link again if the email has to be
/// retried, and a token can only ever cancel its own deletion. "Single use"
/// comes from the state machine: the first click cancels, a replay answers
/// `AlreadyCancelled` (idempotent success) and a link past grace answers
/// `TooLate`.
pub fn mint_cancel_token(key: &[u8], deletion_id: Uuid) -> String {
    let mut mac = HmacSha256::new_from_slice(key).expect("hmac accepts any key length");
    mac.update(format!("account-deletion-cancel:v1:{deletion_id}").as_bytes());
    format!("{deletion_id}.{}", hex::encode(mac.finalize().into_bytes()))
}

pub fn verify_cancel_token(key: &[u8], token: &str) -> Option<Uuid> {
    use subtle::ConstantTimeEq;
    let (id, _) = token.split_once('.')?;
    let id = Uuid::parse_str(id).ok()?;
    let expected = mint_cancel_token(key, id);
    bool::from(expected.as_bytes().ct_eq(token.as_bytes())).then_some(id)
}

// ── Dependencies the steps call out to ───────────────────────────────────

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct UpstreamReport {
    pub attempted: usize,
    pub failed: usize,
    pub skipped: usize,
}

/// Best-effort upstream teardown of an account's external grants. Never fails
/// the job: whatever cannot be revoked upstream is deleted locally anyway and
/// lapses on the provider's side.
#[async_trait]
pub trait ConnectorTeardown: Send + Sync {
    async fn revoke_upstream(&self, control: &PgPool, account_id: Uuid) -> UpstreamReport;
}

/// The part of deletion that reaches org tenant planes.
#[async_trait]
pub trait TenantPlane: Send + Sync {
    /// Remove the user's remaining team-org memberships (seats released,
    /// member workspaces archived).
    async fn detach_org_memberships(&self, user_id: Uuid, clerk_user_id: Option<&str>) -> Result<(), String>;
    /// Make every member-private workspace of this user purge-eligible and
    /// return how many still exist on tenant planes.
    async fn member_workspaces_remaining(&self, user_id: Uuid) -> Result<usize, String>;
    /// `(MEMBER_OCEANS_ENABLED, MEMBER_OCEAN_PURGE_ENABLED)`: the reconciler
    /// only purges when both are on.
    fn reconciler_flags(&self) -> (bool, bool);
}

#[derive(Debug, Clone)]
pub struct Mail {
    pub to: String,
    pub subject: String,
    pub text: String,
    /// Provider `Idempotency-Key` (org deletion's per-member notice).
    pub idempotency_key: Option<String>,
}

/// Outbound email (Resend in production).
#[async_trait]
pub trait Mailer: Send + Sync {
    async fn send(&self, mail: Mail) -> Result<(), String>;
}

pub struct Ctx<'a> {
    pub control: &'a PgPool,
    pub stripe: Option<&'a StripeClient>,
    pub clerk: Option<&'a ClerkAdminClient>,
    pub blobs: Option<&'a dyn PromotionBlobStore>,
    pub connectors: &'a dyn ConnectorTeardown,
    pub tenant_plane: &'a dyn TenantPlane,
    pub dek_cache: Option<&'a crate::dek_cache::DekCache>,
    pub mailer: &'a dyn Mailer,
    /// Cancel-link signing key ([`cancel_key_from_env`]). `None` sends the
    /// start email without a link (the user can still cancel signed in) and
    /// alerts.
    pub cancel_key: Option<&'a [u8]>,
    /// Where the cancel page lives, e.g. `https://app.coralswarm.example`.
    pub web_base: &'a str,
}

#[derive(Debug, PartialEq, Eq)]
pub enum StepResult {
    /// The step is finished; record it.
    Done,
    /// Progress was made but more remains; call again.
    More,
    /// Nothing to do until something outside the job changes; retry later.
    Wait(String),
    /// The deletion must not proceed (the user became ineligible during
    /// grace): cancel it, restore access, alert.
    Abort(String),
}

type Step<T = StepResult> = Result<T, String>;

// ── Running ──────────────────────────────────────────────────────────────

/// How a [`run`] ended.
#[derive(Debug, PartialEq, Eq)]
pub enum RunEnd {
    Completed,
    /// Budget used up; the row was handed back for immediate re-claim.
    Yielded,
    /// Waiting on something outside the job (grace, a queue, a flag).
    Waiting,
    Cancelled,
    Failed,
}

/// Run `claimed` until it completes, waits, fails, is cancelled or the time
/// budget is up. Every transition is persisted before the next step starts.
pub async fn run(ctx: &Ctx<'_>, claimed: Deletion) -> RunEnd {
    let started = Instant::now();
    let id = claimed.id;
    let account = claimed.account_id;
    loop {
        let deletion = match reload(ctx.control, account, id).await {
            Ok(Some(d)) => d,
            Ok(None) => return RunEnd::Failed,
            Err(e) => {
                tracing::error!(deletion_id = %id, error = %e, "account deletion: reload failed");
                return RunEnd::Failed;
            }
        };
        if deletion.state == "cancelled" {
            return RunEnd::Cancelled;
        }
        let Some(step) = deletion.next_step() else {
            return RunEnd::Completed;
        };
        if started.elapsed() > RUN_BUDGET {
            let _ = release(ctx.control, &deletion, Duration::ZERO, None).await;
            return RunEnd::Yielded;
        }
        // The point of no return: destructive steps only from `running`.
        if is_destructive(step) && deletion.state == "grace" {
            match begin_destructive(ctx, &deletion).await {
                Ok(Transition::Began) => continue,
                Ok(Transition::NotYet) => {
                    let wait = (deletion.purge_after - Utc::now()).to_std().unwrap_or(Duration::from_secs(1));
                    let _ = release(ctx.control, &deletion, wait.min(Duration::from_secs(3600)), None).await;
                    return RunEnd::Waiting;
                }
                Ok(Transition::Cancelled) => return RunEnd::Cancelled,
                Err(e) => {
                    tracing::error!(deletion_id = %id, alert = true, error = %e, "account deletion: grace transition failed");
                    let _ = record_failure(ctx.control, &deletion, &e).await;
                    return RunEnd::Failed;
                }
            }
        }
        let _ = renew_lease(ctx.control, &deletion).await;
        let result = run_step(ctx, &deletion, step).await;
        match result {
            Ok(StepResult::Done) => {
                if let Err(e) = mark_step_done(ctx.control, &deletion, step).await {
                    tracing::error!(deletion_id = %id, step, alert = true, error = %e, "account deletion: could not record step");
                    let _ = record_failure(ctx.control, &deletion, &e).await;
                    return RunEnd::Failed;
                }
                tracing::info!(deletion_id = %id, step, "account deletion: step complete");
                // End of the grace-phase steps: sleep until purge_after.
                if step == STEPS[FIRST_DESTRUCTIVE_STEP - 1] && deletion.state == "grace" {
                    let wait = (deletion.purge_after - Utc::now()).to_std().unwrap_or(Duration::ZERO);
                    let _ = release(ctx.control, &deletion, wait.min(Duration::from_secs(3600)), None).await;
                    return RunEnd::Waiting;
                }
            }
            Ok(StepResult::More) => continue,
            Ok(StepResult::Wait(why)) => {
                tracing::error!(deletion_id = %id, step, alert = true, why, "account deletion: waiting on something outside the job");
                let _ = release(ctx.control, &deletion, Duration::from_secs(WAIT_SECS as u64), Some(&why)).await;
                return RunEnd::Waiting;
            }
            Ok(StepResult::Abort(why)) => {
                tracing::error!(deletion_id = %id, step, alert = true, why, "account deletion: cancelled, user is no longer eligible");
                let _ = cancel(ctx.control, id).await;
                notify_auto_cancelled(ctx, &deletion).await;
                return RunEnd::Cancelled;
            }
            Err(e) => {
                // Alertable: a stuck deletion must not be silent.
                tracing::error!(
                    deletion_id = %id, step, attempts = deletion.attempts, alert = deletion.attempts >= 5, error = %e,
                    "account deletion: step failed; will retry with backoff"
                );
                let _ = record_failure(ctx.control, &deletion, &e).await;
                return RunEnd::Failed;
            }
        }
    }
}

enum Transition {
    Began,
    NotYet,
    Cancelled,
}

/// grace -> running, once `purge_after` has passed. Re-validates what the
/// request checked days ago: a user who has since joined a team organization,
/// or whose personal account has since gained members, is cancelled instead of
/// deleted. The flip itself is a guarded UPDATE, so a cancel racing it has
/// exactly one winner.
async fn begin_destructive(ctx: &Ctx<'_>, d: &Deletion) -> Step<Transition> {
    if Utc::now() < d.purge_after {
        return Ok(Transition::NotYet);
    }
    if d.initiated_by == "user" {
        let orgs = blocking_team_orgs(ctx.control, d.user_id).await.map_err(db_err)?;
        let shared = shared_personal_members(ctx.control, d.user_id).await.map_err(db_err)?;
        if !orgs.is_empty() || !shared.is_empty() {
            tracing::error!(
                deletion_id = %d.id, alert = true, orgs = orgs.len(), shared_members = shared.len(),
                "account deletion: grace ended but the user is now in a team organization or shares a personal account; cancelling"
            );
            let _ = cancel(ctx.control, d.id).await.map_err(db_err)?;
            notify_auto_cancelled(ctx, d).await;
            return Ok(Transition::Cancelled);
        }
    }
    let mut tx = scoped(ctx.control, d.account_id).await?;
    let flipped = sqlx::query(
        "UPDATE account_deletions SET state = 'running', running_since = now(), next_attempt_at = now(), updated_at = now() \
          WHERE id = $1 AND state = 'grace' AND purge_after <= now()",
    )
    .bind(d.id)
    .execute(&mut *tx)
    .await
    .map_err(db_err)?
    .rows_affected();
    tx.commit().await.map_err(db_err)?;
    if flipped == 1 {
        tracing::warn!(deletion_id = %d.id, "account deletion: grace ended, destructive phase begins");
        return Ok(Transition::Began);
    }
    match reload(ctx.control, d.account_id, d.id).await? {
        Some(r) if r.state == "cancelled" => Ok(Transition::Cancelled),
        Some(r) if r.state == "running" => Ok(Transition::Began),
        _ => Ok(Transition::NotYet),
    }
}

/// Tell the user their deletion was cancelled because they are no longer
/// eligible (they joined a team organization, or someone joined their personal
/// account), so they are not surprised that the account is still there.
async fn notify_auto_cancelled(ctx: &Ctx<'_>, d: &Deletion) {
    let Ok(Some(to)) = user_email(ctx.control, d.user_id).await else { return };
    if !deliverable(&to) {
        return;
    }
    let mail = Mail {
        to,
        subject: "Your CoralSwarm account deletion was cancelled".into(),
        text: "Your scheduled account deletion was cancelled and your account was not deleted, because it can no longer be deleted as it is: \
               you have joined a team organization, or another person has been added to your personal account. \
               Leave the organization or remove the other member, then request deletion again if you still want it.\n\nCoralSwarm"
            .into(),
        idempotency_key: None,
    };
    if let Err(e) = ctx.mailer.send(mail).await {
        tracing::error!(deletion_id = %d.id, alert = true, error = %e, "account deletion: the cancellation email could not be sent");
    }
}

async fn run_step(ctx: &Ctx<'_>, d: &Deletion, step: &str) -> Step {
    match step {
        "detach" => step_detach(ctx, d).await,
        "stripe_cancel" => step_stripe_cancel(ctx, d).await,
        "revoke_tokens" => step_revoke_tokens(ctx, d).await,
        "notify_started" => step_notify_started(ctx, d).await,
        "forfeit" => step_forfeit(ctx, d).await,
        "connectors" => step_connectors(ctx, d).await,
        "keys" => step_keys(ctx, d).await,
        "data" => step_data(ctx, d).await,
        "tenant_planes" => step_tenant_planes(ctx, d).await,
        "stripe_customer" => step_stripe_customer(ctx, d).await,
        "clerk" => step_clerk(ctx, d).await,
        "blob_drain" => step_blob_drain(ctx, d).await,
        "notify_completed" => step_notify_completed(ctx, d).await,
        "tombstone" => step_tombstone(ctx, d).await,
        other => Err(format!("unknown step {other}")),
    }
}

async fn reload(control: &PgPool, account: Uuid, id: Uuid) -> Step<Option<Deletion>> {
    let mut tx = scoped(control, account).await?;
    let row = sqlx::query_as::<_, Deletion>("SELECT * FROM account_deletions WHERE id = $1")
        .bind(id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db_err)?;
    tx.commit().await.map_err(db_err)?;
    Ok(row)
}

async fn renew_lease(control: &PgPool, d: &Deletion) -> Step<()> {
    let mut tx = scoped(control, d.account_id).await?;
    sqlx::query(
        "UPDATE account_deletions SET lease_until = now() + make_interval(secs => $2), updated_at = now() WHERE id = $1",
    )
    .bind(d.id)
    .bind(LEASE_SECS as f64)
    .execute(&mut *tx)
    .await
    .map_err(db_err)?;
    tx.commit().await.map_err(db_err)
}

async fn mark_step_done(control: &PgPool, d: &Deletion, step: &str) -> Step<()> {
    let last = step == STEPS[STEPS.len() - 1];
    let mut tx = scoped(control, d.account_id).await?;
    sqlx::query(
        "UPDATE account_deletions \
            SET completed_steps = completed_steps || ARRAY[$2]::text[], \
                attempts = 0, last_error = NULL, next_attempt_at = now(), updated_at = now(), \
                lease_until = CASE WHEN $3 THEN NULL ELSE lease_until END, \
                state = CASE WHEN $3 THEN 'completed' ELSE state END, \
                completed_at = CASE WHEN $3 THEN now() ELSE completed_at END \
          WHERE id = $1 AND state IN ('grace', 'running') AND NOT ($2 = ANY (completed_steps))",
    )
    .bind(d.id)
    .bind(step)
    .bind(last)
    .execute(&mut *tx)
    .await
    .map_err(db_err)?;
    tx.commit().await.map_err(db_err)
}

/// Seconds to wait after the `attempts`-th consecutive failure: 30s doubling,
/// capped at an hour.
pub fn backoff_secs(attempts: i32) -> i64 {
    let exp = (attempts.max(1) - 1).min(7) as u32;
    (30i64 << exp).min(3600)
}

async fn record_failure(control: &PgPool, d: &Deletion, error: &str) -> Step<()> {
    let msg: String = error.chars().take(500).collect();
    let mut tx = scoped(control, d.account_id).await?;
    sqlx::query(
        "UPDATE account_deletions \
            SET last_error = $2, next_attempt_at = now() + make_interval(secs => $3), \
                lease_until = NULL, updated_at = now() \
          WHERE id = $1",
    )
    .bind(d.id)
    .bind(msg)
    .bind(backoff_secs(d.attempts) as f64)
    .execute(&mut *tx)
    .await
    .map_err(db_err)?;
    tx.commit().await.map_err(db_err)
}

async fn release(control: &PgPool, d: &Deletion, after: Duration, note: Option<&str>) -> Step<()> {
    let mut tx = scoped(control, d.account_id).await?;
    sqlx::query(
        "UPDATE account_deletions \
            SET lease_until = NULL, next_attempt_at = now() + make_interval(secs => $2), \
                last_error = COALESCE($3, last_error), attempts = 0, updated_at = now() \
          WHERE id = $1",
    )
    .bind(d.id)
    .bind(after.as_secs_f64())
    .bind(note.map(|n| n.chars().take(500).collect::<String>()))
    .execute(&mut *tx)
    .await
    .map_err(db_err)?;
    tx.commit().await.map_err(db_err)
}

// ── Grace-phase steps ────────────────────────────────────────────────────

/// Detach team-org memberships BEFORE anything else. A user-initiated
/// deletion that finds one (joined after the request) cancels itself; a
/// Clerk-initiated one removes them through the tenant plane (seats released,
/// member workspace archived) and checks they are gone.
async fn step_detach(ctx: &Ctx<'_>, d: &Deletion) -> Step {
    let orgs = blocking_team_orgs(ctx.control, d.user_id).await.map_err(db_err)?;
    if orgs.is_empty() {
        return Ok(StepResult::Done);
    }
    if d.initiated_by == "user" {
        return Ok(StepResult::Abort(format!("the user is now a member of {} team organization(s)", orgs.len())));
    }
    ctx.tenant_plane.detach_org_memberships(d.user_id, d.clerk_user_id.as_deref()).await?;
    let left = blocking_team_orgs(ctx.control, d.user_id).await.map_err(db_err)?;
    if !left.is_empty() {
        return Err(format!("{} team membership(s) remain after detaching", left.len()));
    }
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

/// End the subscription immediately and switch extra usage / auto-reload off.
///
/// Immediate rather than at period end (the choice `cancel_account_subscription`
/// makes for a live customer): this customer is being deleted, so nothing may
/// bill them again. No proration credit or final invoice is created; remaining
/// paid time is forfeited (refunds are manual at launch, decision 18). It runs
/// at the START of grace, so a Checkout paid mid-grace can never revive it, and
/// a cancelled deletion leaves the subscription ended (they can resubscribe).
async fn step_stripe_cancel(ctx: &Ctx<'_>, d: &Deletion) -> Step {
    let mut labels = Vec::new();
    for account in deletable_account_ids(ctx.control, d.user_id).await.map_err(db_err)? {
        let outcome = cancel_account_subscription_now(ctx.control, ctx.stripe, account)
            .await
            .map_err(|e| format!("stripe cancel failed: {e}"))?;
        labels.push(outcome_label(&outcome));
        disable_extra_usage(ctx.control, account).await?;
    }
    let mut tx = scoped(ctx.control, d.account_id).await?;
    sqlx::query("UPDATE account_deletions SET stripe_outcome = COALESCE(stripe_outcome, $2) WHERE id = $1")
        .bind(d.id)
        .bind(labels.join(","))
        .execute(&mut *tx)
        .await
        .map_err(db_err)?;
    tx.commit().await.map_err(db_err)?;
    Ok(StepResult::Done)
}

/// Turn extra usage and auto-reload OFF at the start of grace, inside the 0321
/// entry fence. Ending the subscription is not enough: an auto-reload worker
/// charges the saved card from `extra_usage_settings`, independent of the
/// subscription, so it would keep charging for the whole grace period. The row
/// and its limits are kept (a cancelled deletion leaves them as they were,
/// disabled); the `forfeit` step deletes it later. Idempotent.
async fn disable_extra_usage(control: &PgPool, account: Uuid) -> Step<()> {
    let mut tx = scoped(control, account).await?;
    crate::billing::renewal::lock_period_rotation(&mut tx, account).await.map_err(db_err)?;
    sqlx::query(
        "UPDATE extra_usage_settings SET enabled = false, auto_reload_enabled = false, updated_at = now() \
          WHERE account_id = $1 AND (enabled OR auto_reload_enabled)",
    )
    .bind(account)
    .execute(&mut *tx)
    .await
    .map_err(db_err)?;
    tx.commit().await.map_err(db_err)
}

async fn step_revoke_tokens(ctx: &Ctx<'_>, d: &Deletion) -> Step {
    let mut tx = scoped(ctx.control, d.account_id).await?;
    lockout_tx(&mut tx, d.user_id).await.map_err(db_err)?;
    tx.commit().await.map_err(db_err)?;
    Ok(StepResult::Done)
}

async fn user_email(control: &PgPool, user_id: Uuid) -> Step<Option<String>> {
    sqlx::query_scalar("SELECT email FROM users WHERE id = $1")
        .bind(user_id)
        .fetch_optional(control)
        .await
        .map_err(db_err)
}

fn deliverable(email: &str) -> bool {
    email.contains('@') && !email.ends_with("@deleted.invalid") && !email.ends_with("@users.clerk.local")
}

/// Email the cancel link. Best effort by design: a missing mail provider must
/// not stop the lockout or the Stripe cancel (the user can still cancel signed
/// in), but it is an alert, because the safety net of the grace period is the
/// user knowing about it.
async fn step_notify_started(ctx: &Ctx<'_>, d: &Deletion) -> Step {
    if d.initiated_by != "user" {
        return Ok(StepResult::Done);
    }
    let Some(to) = user_email(ctx.control, d.user_id).await? else {
        return Ok(StepResult::Done);
    };
    if !deliverable(&to) {
        tracing::error!(deletion_id = %d.id, alert = true, "account deletion: no deliverable email address; the user was not notified");
        return Ok(StepResult::Done);
    }
    // The token rides in the URL FRAGMENT, never the query string: fragments are
    // not sent to servers, proxies or `Referer`, and mail scanners that prefetch
    // links do not execute the page that reads it. The web page requires a
    // button click before it POSTs the token.
    let link = match ctx.cancel_key {
        Some(key) => Some(format!(
            "{}/account/delete/cancel#token={}",
            ctx.web_base.trim_end_matches('/'),
            urlencoding::encode(&mint_cancel_token(key, d.id))
        )),
        None => {
            tracing::error!(deletion_id = %d.id, alert = true, "account deletion: neither ACCOUNT_DELETION_LINK_SECRET nor OAUTH_STATE_SECRET is set; the cancel link was omitted");
            None
        }
    };
    let when = d.purge_after.format("%B %-d, %Y (UTC)");
    let how = match link {
        Some(l) => format!("To keep your account, cancel the deletion before then: {l}\n\nYou can also sign in and cancel from the account page."),
        None => "To keep your account, sign in before then and cancel the deletion from the account page.".to_string(),
    };
    let mail = Mail {
        to,
        subject: "Your CoralSwarm account is scheduled for deletion".into(),
        text: format!(
            "You asked to delete your CoralSwarm account.\n\nYour subscription has been ended and your sessions and devices signed out. \
             Your data will be permanently deleted on {when}.\n\n{how}\n\nIf you did not ask for this, cancel it now and change your password.\n\nCoralSwarm"
        ),
        idempotency_key: None,
    };
    if let Err(e) = ctx.mailer.send(mail).await {
        tracing::error!(deletion_id = %d.id, alert = true, error = %e, "account deletion: the start email could not be sent");
    }
    Ok(StepResult::Done)
}

// ── Destructive-phase steps ──────────────────────────────────────────────

/// Forfeit extra-usage credits (decision 19): one atomic ledger adjustment +
/// zero balance under the 0321 fence, then the settings row goes (a missing
/// row is "off", which also turns auto-reload off).
async fn step_forfeit(ctx: &Ctx<'_>, d: &Deletion) -> Step {
    for account in deletable_account_ids(ctx.control, d.user_id).await.map_err(db_err)? {
        forfeit_extra_usage(ctx.control, d, account).await?;
    }
    Ok(StepResult::Done)
}

async fn forfeit_extra_usage(control: &PgPool, d: &Deletion, account: Uuid) -> Step<()> {
    let mut tx = scoped(control, account).await?;
    // Enters the 0321 financial fence for this account, as every billing writer does.
    crate::billing::renewal::lock_period_rotation(&mut tx, account).await.map_err(db_err)?;
    let balance: Option<i64> =
        sqlx::query_scalar("SELECT balance_micro_usd FROM extra_usage_balances WHERE account_id = $1 FOR UPDATE")
            .bind(account)
            .fetch_optional(&mut *tx)
            .await
            .map_err(db_err)?;
    if let Some(balance) = balance.filter(|b| *b > 0) {
        let prior: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM extra_usage_ledger WHERE account_id = $1 AND idempotency_key LIKE $2",
        )
        .bind(account)
        .bind(format!("account_deletion:{}:forfeit:%", d.id))
        .fetch_one(&mut *tx)
        .await
        .map_err(db_err)?;
        sqlx::query(
            "INSERT INTO extra_usage_ledger (account_id, entry_kind, amount_micro_usd, idempotency_key, note) \
             VALUES ($1, 'adjustment', $2, $3, 'forfeited: account deleted')",
        )
        .bind(account)
        .bind(-balance)
        .bind(format!("account_deletion:{}:forfeit:{}", d.id, prior + 1))
        .execute(&mut *tx)
        .await
        .map_err(db_err)?;
        sqlx::query("UPDATE extra_usage_balances SET balance_micro_usd = 0, updated_at = now() WHERE account_id = $1")
            .bind(account)
            .execute(&mut *tx)
            .await
            .map_err(db_err)?;
        sqlx::query(
            "UPDATE account_deletions SET forfeited_extra_usage_micro_usd = forfeited_extra_usage_micro_usd + $2 \
              WHERE id = $1",
        )
        .bind(d.id)
        .bind(balance)
        .execute(&mut *tx)
        .await
        .map_err(db_err)?;
    }
    sqlx::query("DELETE FROM extra_usage_settings WHERE account_id = $1")
        .bind(account)
        .execute(&mut *tx)
        .await
        .map_err(db_err)?;
    tx.commit().await.map_err(db_err)
}

async fn step_connectors(ctx: &Ctx<'_>, d: &Deletion) -> Step {
    for account in deletable_account_ids(ctx.control, d.user_id).await.map_err(db_err)? {
        // Upstream first: it needs the stored tokens and the DEK.
        let report = ctx.connectors.revoke_upstream(ctx.control, account).await;
        tracing::info!(
            deletion_id = %d.id, attempted = report.attempted, failed = report.failed, skipped = report.skipped,
            "account deletion: upstream connector revoke"
        );
        for table in CONNECTOR_TABLES {
            delete_in_batches(ctx, d, account, table, "account_id", &[account]).await?;
        }
    }
    Ok(StepResult::Done)
}

/// `DELETE FROM table WHERE col = ANY(keys)` in bounded batches, each its own
/// transaction, renewing the lease between batches so a long delete never
/// outlives it.
async fn delete_in_batches(
    ctx: &Ctx<'_>,
    d: &Deletion,
    scope_account: Uuid,
    table: &str,
    column: &str,
    keys: &[Uuid],
) -> Step<u64> {
    let sql = format!(
        "DELETE FROM {table} WHERE ctid IN (SELECT ctid FROM {table} WHERE {column} = ANY($1) LIMIT {DELETE_BATCH})"
    );
    let mut total = 0u64;
    loop {
        let mut tx = scoped(ctx.control, scope_account).await?;
        let n = sqlx::query(&sql)
            .bind(keys)
            .execute(&mut *tx)
            .await
            .map_err(|e| format!("delete {table}: {e}"))?
            .rows_affected();
        tx.commit().await.map_err(db_err)?;
        total += n;
        if (n as i64) < DELETE_BATCH {
            return Ok(total);
        }
        let _ = renew_lease(ctx.control, d).await;
    }
}

async fn step_keys(ctx: &Ctx<'_>, d: &Deletion) -> Step {
    for account in deletable_account_ids(ctx.control, d.user_id).await.map_err(db_err)? {
        for table in ["escrow_wrapped_keys", "escrow_recovery_pubkey", "escrow_recovery_log"] {
            delete_in_batches(ctx, d, account, table, "account_id", &[account]).await?;
        }
        let mut tx = scoped(ctx.control, account).await?;
        // Crypto-shred: whatever ciphertext is still in R2 (cleanup is queued
        // and asynchronous) becomes unreadable.
        sqlx::query("UPDATE accounts SET dek_wrapped = NULL WHERE id = $1")
            .bind(account)
            .execute(&mut *tx)
            .await
            .map_err(db_err)?;
        tx.commit().await.map_err(db_err)?;
        if let Some(cache) = ctx.dek_cache {
            cache.invalidate(&crate::tenancy::TenantId::Consumer, account);
        }
    }
    // escrow_shares (0083 revoke) and friends: through the definer function.
    purge_restricted(ctx.control, d).await?;
    sqlx::query("DELETE FROM user_private_keys WHERE user_id = $1")
        .bind(d.user_id)
        .execute(ctx.control)
        .await
        .map_err(db_err)?;
    Ok(StepResult::Done)
}

async fn purge_restricted(control: &PgPool, d: &Deletion) -> Step<()> {
    sqlx::query("SELECT account_deletion_purge_restricted($1)")
        .bind(d.id)
        .execute(control)
        .await
        .map_err(|e| format!("restricted purge: {e}"))?;
    Ok(())
}

async fn graph_ids(control: &PgPool, account: Uuid) -> Step<Vec<Uuid>> {
    let mut tx = scoped(control, account).await?;
    let ids = sqlx::query_scalar("SELECT id FROM graphs WHERE account_id = $1 ORDER BY id")
        .bind(account)
        .fetch_all(&mut *tx)
        .await
        .map_err(db_err)?;
    tx.commit().await.map_err(db_err)?;
    Ok(ids)
}

async fn step_data(ctx: &Ctx<'_>, d: &Deletion) -> Step {
    let all = personal_account_ids(ctx.control, d.user_id).await.map_err(db_err)?;
    let accounts = deletable_account_ids(ctx.control, d.user_id).await.map_err(db_err)?;
    if accounts.len() != all.len() {
        tracing::error!(
            deletion_id = %d.id, alert = true,
            "account deletion: a personal account is shared with other members and was not wiped; only the departing user's own rows are removed from it"
        );
    }

    // A. Atoms, through the audited cascade (queues blob cleanup durably).
    for account in &accounts {
        for graph in graph_ids(ctx.control, *account).await? {
            let mut tx = scoped(ctx.control, *account).await?;
            let atoms: Vec<Uuid> =
                sqlx::query_scalar("SELECT atom_id FROM atoms WHERE graph_id = $1 ORDER BY atom_id LIMIT $2")
                    .bind(graph)
                    .bind(ATOM_BATCH)
                    .fetch_all(&mut *tx)
                    .await
                    .map_err(db_err)?;
            tx.commit().await.map_err(db_err)?;
            if !atoms.is_empty() {
                crate::services::deletion::delete_atoms_in_graph_only(
                    ctx.control,
                    ctx.blobs,
                    "account-deletion",
                    graph,
                    &atoms,
                    crate::services::connector_audit::Actor::System,
                    None,
                )
                .await
                .map_err(|e| format!("atom cascade failed: {e}"))?;
                return Ok(StepResult::More);
            }
        }
    }

    // B. Reefs in bounded batches, so the graph delete below stays small.
    for account in &accounts {
        for graph in graph_ids(ctx.control, *account).await? {
            let mut tx = scoped(ctx.control, *account).await?;
            let removed = sqlx::query(
                "DELETE FROM nodes WHERE node_id IN (SELECT node_id FROM nodes WHERE graph_id = $1 LIMIT $2)",
            )
            .bind(graph)
            .bind(NODE_BATCH)
            .execute(&mut *tx)
            .await
            .map_err(db_err)?
            .rows_affected();
            tx.commit().await.map_err(db_err)?;
            if removed > 0 {
                return Ok(StepResult::More);
            }
        }
    }

    // C. Everything else, batched, per account.
    purge_restricted(ctx.control, d).await?;
    for account in &accounts {
        let graphs = graph_ids(ctx.control, *account).await?;
        for table in PURGE_BY_GRAPH_NO_FK {
            delete_in_batches(ctx, d, *account, table, "graph_id", &graphs).await?;
        }
        let mut tx = scoped(ctx.control, *account).await?;
        sqlx::query("DELETE FROM graphs WHERE account_id = $1")
            .bind(account)
            .execute(&mut *tx)
            .await
            .map_err(|e| format!("delete graphs: {e}"))?;
        tx.commit().await.map_err(db_err)?;
        for table in PURGE_BY_ACCOUNT {
            delete_in_batches(ctx, d, *account, table, "account_id", &[*account]).await?;
        }
        for (table, column) in PURGE_BY_OTHER_ACCOUNT_COLUMN {
            delete_in_batches(ctx, d, *account, table, column, &[*account]).await?;
        }
    }
    // The user's own rows in EVERY account (organizations already left
    // included, which RLS hides from this role), through the definer function.
    purge_user_rows(ctx.control, d).await?;

    // D. Verify before declaring the step done.
    for account in &accounts {
        let left = graph_ids(ctx.control, *account).await?;
        if !left.is_empty() {
            return Err(format!("{} graph(s) remain after purge", left.len()));
        }
    }
    Ok(StepResult::Done)
}

async fn purge_user_rows(control: &PgPool, d: &Deletion) -> Step<i64> {
    sqlx::query_scalar("SELECT account_deletion_purge_user_rows($1)")
        .bind(d.id)
        .fetch_one(control)
        .await
        .map_err(|e| format!("user-keyed purge: {e}"))
}

async fn step_tenant_planes(ctx: &Ctx<'_>, d: &Deletion) -> Step {
    let remaining = ctx.tenant_plane.member_workspaces_remaining(d.user_id).await?;
    if remaining == 0 {
        return Ok(StepResult::Done);
    }
    let (oceans, purge) = ctx.tenant_plane.reconciler_flags();
    let flag = |on: bool| if on { "on" } else { "OFF" };
    Ok(StepResult::Wait(format!(
        "{remaining} member workspace(s) in org databases await the member-ocean reconciler purge, which needs both \
         MEMBER_OCEANS_ENABLED (currently {}) and MEMBER_OCEAN_PURGE_ENABLED (currently {})",
        flag(oceans),
        flag(purge)
    )))
}

/// Delete the Stripe customer (email, name, saved cards) once the
/// subscription is ended. Invoices and charges remain in Stripe for tax and
/// disputes; the local `stripe_subscriptions` row keeps the opaque ids.
async fn step_stripe_customer(ctx: &Ctx<'_>, d: &Deletion) -> Step {
    for account in deletable_account_ids(ctx.control, d.user_id).await.map_err(db_err)? {
        let mut tx = scoped(ctx.control, account).await?;
        let customer: Option<String> =
            sqlx::query_scalar("SELECT stripe_customer_id FROM stripe_subscriptions WHERE account_id = $1")
                .bind(account)
                .fetch_optional(&mut *tx)
                .await
                .map_err(db_err)?;
        tx.commit().await.map_err(db_err)?;
        let Some(customer) = customer.filter(|c| !c.is_empty()) else { continue };
        let Some(stripe) = ctx.stripe else {
            return Err("stripe is not configured but a customer exists".into());
        };
        match crate::billing::seat_lifecycle::with_stripe_deadline(stripe.delete_customer(&customer)).await {
            Ok(_) => {}
            Err(StripeError::Api { status: 404, .. }) => {}
            Err(e) => return Err(format!("stripe customer delete failed: {e}")),
        }
    }
    Ok(StepResult::Done)
}

async fn step_clerk(ctx: &Ctx<'_>, d: &Deletion) -> Step {
    if d.clerk_user_deleted {
        return Ok(StepResult::Done);
    }
    let Some(clerk_user_id) = d.clerk_user_id.as_deref() else {
        return Ok(StepResult::Done);
    };
    let Some(clerk) = ctx.clerk else {
        return Err("clerk is not configured (CLERK_SECRET_KEY)".to_string());
    };
    // The personal organization stands for the user's own workspace; it has
    // no meaning without them.
    let orgs: Vec<String> = sqlx::query_scalar("SELECT clerk_org_id FROM personal_orgs WHERE clerk_user_id = $1")
        .bind(clerk_user_id)
        .fetch_all(ctx.control)
        .await
        .map_err(db_err)?;
    for org in orgs {
        match clerk.delete_organization(&org).await {
            Ok(()) => {}
            Err(ClerkAdminError::Api { status: 404, .. }) => {}
            Err(e) => return Err(format!("clerk org delete failed: {e}")),
        }
    }
    match clerk.delete_user(clerk_user_id).await {
        Ok(()) => {}
        // Already gone: success.
        Err(ClerkAdminError::Api { status: 404, .. }) => {}
        Err(e) => return Err(format!("clerk delete failed: {e}")),
    }
    // Persist the fact immediately, separately from the step checkpoint, so a
    // crash right here does not make the next run call Clerk again.
    let mut tx = scoped(ctx.control, d.account_id).await?;
    sqlx::query("UPDATE account_deletions SET clerk_user_deleted = true, updated_at = now() WHERE id = $1")
        .bind(d.id)
        .execute(&mut *tx)
        .await
        .map_err(db_err)?;
    tx.commit().await.map_err(db_err)?;
    Ok(StepResult::Done)
}

async fn step_blob_drain(ctx: &Ctx<'_>, d: &Deletion) -> Step {
    let accounts = personal_account_ids(ctx.control, d.user_id).await.map_err(db_err)?;
    let mut pending = 0i64;
    let mut dead = 0i64;
    for account in &accounts {
        let mut tx = scoped(ctx.control, *account).await?;
        pending += sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM jobs WHERE account_id = $1 AND kind = 'DeleteSourceBlob' \
                AND status IN ('pending', 'running')",
        )
        .bind(account)
        .fetch_one(&mut *tx)
        .await
        .map_err(db_err)?;
        pending += sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM source_purge_dependencies d JOIN source_purge_events e ON e.event_id = d.event_id \
              WHERE e.account_id = $1 AND d.processed_at IS NULL AND d.error_category IS NULL",
        )
        .bind(account)
        .fetch_one(&mut *tx)
        .await
        .map_err(db_err)?;
        pending += sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM unattached_capture_uploads \
              WHERE account_id = $1 AND state IN ('pending', 'retained_reference')",
        )
        .bind(account)
        .fetch_one(&mut *tx)
        .await
        .map_err(db_err)?;
        dead += sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM jobs WHERE account_id = $1 AND kind = 'DeleteSourceBlob' AND status IN ('failed', 'dead')",
        )
        .bind(account)
        .fetch_one(&mut *tx)
        .await
        .map_err(db_err)?;
        tx.commit().await.map_err(db_err)?;
    }
    if dead > 0 {
        // Not a blocker (the DEK is already destroyed, so what is left in R2
        // is unreadable) but an operator should sweep it.
        tracing::error!(deletion_id = %d.id, dead, alert = true, "account deletion: blob cleanup jobs are dead; objects remain in R2 (encrypted under a destroyed DEK)");
    }
    if pending > 0 {
        return Ok(StepResult::Wait(format!("waiting for {pending} blob cleanup item(s) to drain")));
    }
    Ok(StepResult::Done)
}

/// Sent while the address still exists (before `tombstone` anonymizes it).
async fn step_notify_completed(ctx: &Ctx<'_>, d: &Deletion) -> Step {
    if d.initiated_by != "user" {
        return Ok(StepResult::Done);
    }
    let Some(to) = user_email(ctx.control, d.user_id).await? else {
        return Ok(StepResult::Done);
    };
    if !deliverable(&to) {
        return Ok(StepResult::Done);
    }
    let mail = Mail {
        to,
        subject: "Your CoralSwarm account has been deleted".into(),
        text: "Your CoralSwarm account and its data have been permanently deleted, and your subscription is ended. \
               Billing records we must keep for tax and legal reasons are retained without your name or email.\n\nCoralSwarm"
            .into(),
        idempotency_key: None,
    };
    if let Err(e) = ctx.mailer.send(mail).await {
        tracing::error!(deletion_id = %d.id, alert = true, error = %e, "account deletion: the completion email could not be sent");
    }
    Ok(StepResult::Done)
}

/// Anonymize `users` (shared with the webhook's no-personal-account path).
pub async fn anonymize_user(control: &PgPool, user_id: Uuid) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE users SET email = 'deleted-' || id::text || '@deleted.invalid', name = NULL, \
                clerk_user_id = NULL, timezone = NULL, \
                deleted_at = COALESCE(deleted_at, now()), updated_at = now() \
          WHERE id = $1",
    )
    .bind(user_id)
    .execute(control)
    .await?;
    Ok(())
}

async fn step_tombstone(ctx: &Ctx<'_>, d: &Deletion) -> Step {
    // Late rows written after `data` (an `org_membership_tombstones` row with
    // the user's email from a delayed Clerk event, say) are cleaned here.
    purge_user_rows(ctx.control, d).await?;
    let mut tx = scoped(ctx.control, d.account_id).await?;
    // What deleting the Clerk user would have cascaded away on the tenant side.
    sqlx::query("DELETE FROM member_ocean_desired_state WHERE user_id = $1")
        .bind(d.user_id)
        .execute(&mut *tx)
        .await
        .map_err(db_err)?;
    // The personal-org link names the Clerk user; the opaque id stays on the
    // deletion row only.
    sqlx::query(
        "DELETE FROM personal_orgs WHERE clerk_user_id = (SELECT clerk_user_id FROM users WHERE id = $1) \
            OR clerk_user_id = $2",
    )
    .bind(d.user_id)
    .bind(d.clerk_user_id.as_deref())
    .execute(&mut *tx)
    .await
    .map_err(db_err)?;
    tx.commit().await.map_err(db_err)?;
    anonymize_user(ctx.control, d.user_id).await.map_err(db_err)?;
    Ok(StepResult::Done)
}

// ── Worker entry ─────────────────────────────────────────────────────────

/// Claim and run at most one due deletion. Returns whether one was claimed.
pub async fn tick(ctx: &Ctx<'_>) -> Result<bool, sqlx::Error> {
    let claimed: Option<Deletion> = sqlx::query_as("SELECT * FROM account_deletion_claim($1)")
        .bind(LEASE_SECS)
        .fetch_optional(ctx.control)
        .await?;
    let Some(claimed) = claimed else {
        return Ok(false);
    };
    let end = run(ctx, claimed).await;
    tracing::info!(?end, "account deletion: run ended");
    Ok(true)
}

/// Log (ERROR, `alert = true`) every deletion that should have finished:
/// destructive phase running for more than `hours`, or grace whose first phase
/// never completed. Returns how many. The worker calls this periodically.
pub async fn alert_stuck(control: &PgPool, hours: i32) -> Result<usize, sqlx::Error> {
    let rows: Vec<(Uuid, String, i32, f64, Option<String>)> =
        sqlx::query_as("SELECT * FROM account_deletion_stuck($1)").bind(hours).fetch_all(control).await?;
    for (id, state, steps_done, age_hours, last_error) in &rows {
        tracing::error!(
            deletion_id = %id, state, steps_done, age_hours, last_error = last_error.as_deref().unwrap_or(""),
            alert = true, "account deletion is stuck"
        );
    }
    Ok(rows.len())
}

#[path = "account_deletion_pg_tests.rs"]
#[cfg(test)]
mod pg_tests;
