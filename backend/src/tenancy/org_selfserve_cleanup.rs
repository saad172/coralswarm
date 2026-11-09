//! The sweep that cleans up self-serve team orgs that were never paid for,
//! or whose provisioning failed while unpaid (self-serve signup PR A2).
//! Runs only in the workers process group (`workers::org_selfserve_cleanup`).
//!
//! Each kind of org has its own explicit state column and its own atomic
//! claim, so a sweep pass, a second machine's pass, a `POST /v1/orgs` replay
//! and an owner's retry can never act on the same org at once. Every claim
//! returns a TOKEN (its clock); every later write of the pass presents it,
//! and the pass re-checks it, and re-reads Stripe, immediately before each
//! irreversible step (the subscription cancel and the Clerk delete), so a
//! pass whose claim was taken over as stale stops instead of acting on a
//! decision another pass now owns (security review S6).
//!
//! # 0. Dead attempts (`org_selfserve_creations.status`, [`IntentStatus`])
//!
//! ```text
//!   creating ──► created   (Clerk answered; or the sweep adopted the org by its marker, N2)
//!       │
//!       ▼
//!   refused                (Clerk refused; or the sweep found no org for an attempt
//!       │                   left in `creating` for [`CleanupPolicy::dead_attempt_after`])
//!       ▼
//!   creating               (the creator tried the same key again, after the caps re-check, N3)
//! ```
//!
//! # 1. Abandoned sign-ups (`org_selfserve_creations.sweep_state`, control 0384)
//!
//! An org `POST /v1/orgs` created whose Checkout was never completed. #1521
//! marks its trial `abandoned` after 24 hours; an org whose Checkout was
//! never even issued (Stripe was down when it was created, and the creator
//! never retried) has no trial row and is treated the same once the window
//! is over.
//!
//! ```text
//!   none ──claim──► deleting ──► deleted   (Clerk org and accounts row gone)
//!     │                │
//!     ▼                ▼
//!    kept             none                 (released: a Checkout completed after all,
//!                                           a live subscription exists, or a step failed)
//! ```
//!
//! `kept` is terminal: the org's trial moved past checkout (or it has a
//! provisioning job), so it is a real customer and the sweep never looks at
//! it again. The claim is taken only while no `POST /v1/orgs` request holds
//! the intent's lease, and it clears that lease, so a replay and the sweep
//! never overlap (S5). Rows the sweep leaves alone (not due yet, released)
//! get a later `sweep_next_check_at`, and the scan is ordered by it, so they
//! cannot starve the rows that are due (N4).
//!
//! In `deleting` the pass, in order: expires the org's open Checkout session
//! (after which no completion can arrive for it); lists every subscription on
//! the org's Stripe customer; keeps the org if any is live (`active`/
//! `trialing`, or unpaid with a payment still `processing`, S3) and cancels
//! at Stripe, with no proration and no invoice, any that exists but is
//! unpaid; deletes the Clerk org; and, in one transaction under the account's
//! financial lock that re-checks the trial is still `abandoned` (or absent)
//! and that no job exists, deletes the `accounts` row (cascading the trial,
//! the customer mapping and the rest) and marks the intent `deleted`. The
//! intent outlives its creator (`user_id` is SET NULL, S9), so an org whose
//! creator deleted their account is still cleaned up.
//!
//! # 2. Failed provisioning (`tenant_provisioning_jobs.clerk_org_cleanup`, 0381/0384)
//!
//! A job A1 left `failed` with `clerk_org_cleanup = 'pending'`. The claim is
//! the 0384 definer (`pending → deleting`); see
//! [`crate::tenancy::provisioning::ClerkOrgCleanup`] for the whole table.
//! Before anything is deleted every subscription on the org's Stripe
//! customer is read AT STRIPE (the mirror can lag a late SEPA/ACH settlement,
//! #1517 S9, or miss a subscription altogether, S2):
//!
//! * live: the org is paid. Requeue the job (the same reset as the owner's
//!   retry) and alert. The definer caps the requeues
//!   ([`CleanupPolicy::max_auto_requeues`], N8) and sends an org it cannot
//!   requeue (cap spent, or already routed, N7) to `held` instead.
//! * exists but unpaid: cancel it at Stripe now (no charge), then delete the
//!   Clerk org.
//! * none or ended: delete the Clerk org.
//! * unreadable: give the claim back; the next pass decides again.
//!
//! The `accounts` row of a failed org is kept (it is the billing anchor, and
//! the job row references it).
//!
//! # 3. Held, paid orgs keep no bill running (S7, [`CollectionState`])
//!
//! ```text
//!   collecting ──(job held)──► paused ──(job ready)──► collecting
//! ```
//!
//! A `held` org is paid but has no workspace. The pass pauses collection on
//! its subscriptions at Stripe (`pause_collection[behavior]=void`, keyed per
//! subscription and failure), so neither a renewal nor Stripe's own 90-day
//! trial end can charge it, and alerts; the trial worker does not convert a
//! held org (`billing::org_trial::lifecycle`). Once an operator (or the
//! owner's retry) gets the job to `ready`, the pass resumes collection.

use std::time::Duration;

use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::PgPool;
use uuid::Uuid;

use crate::billing::org_trial::TrialState;
use crate::billing::stripe_client::{Idem, Params, StripeClient};
use crate::clerk_admin::ClerkAdminClient;
use crate::tenancy::provisioning::{self, CleanupRelease, ReleaseOutcome};

// ── Policy ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct CleanupPolicy {
    /// How long an `abandoned` trial is left alone before the sweep deletes
    /// its org (on top of #1521's own 24 h).
    pub abandoned_grace: chrono::Duration,
    /// An org with no trial row at all (its Checkout was never issued) is
    /// swept this long after it was created.
    pub no_checkout_after: chrono::Duration,
    /// How long a failed job's Clerk org is left alone (the owner may retry).
    pub failed_grace: chrono::Duration,
    /// A `deleting` claim older than this is taken over (its pass crashed).
    pub stale_claim: Duration,
    /// Automatic requeues of a paid org's failed job before it is `held`
    /// (enforced by the release definer, N8).
    pub max_auto_requeues: i32,
    /// When an abandoned-org row the pass left alone is looked at again.
    pub recheck_after: chrono::Duration,
    /// An intent still `creating` (and unleased) this long after it was made
    /// is a dead attempt (N2).
    pub dead_attempt_after: chrono::Duration,
    /// Orgs looked at per pass, per kind.
    pub batch: i64,
}

impl Default for CleanupPolicy {
    fn default() -> Self {
        CleanupPolicy {
            abandoned_grace: chrono::Duration::hours(1),
            no_checkout_after: chrono::Duration::hours(25),
            failed_grace: chrono::Duration::hours(1),
            stale_claim: Duration::from_secs(15 * 60),
            max_auto_requeues: 2,
            recheck_after: chrono::Duration::minutes(30),
            dead_attempt_after: chrono::Duration::hours(2),
            batch: 50,
        }
    }
}

// ── The intent's own status (`org_selfserve_creations.status`) ────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IntentStatus {
    Creating,
    Created,
    Refused,
}

impl IntentStatus {
    pub const ALL: [IntentStatus; 3] = [IntentStatus::Creating, IntentStatus::Created, IntentStatus::Refused];

    pub const fn as_str(self) -> &'static str {
        match self {
            IntentStatus::Creating => "creating",
            IntentStatus::Created => "created",
            IntentStatus::Refused => "refused",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|t| t.as_str() == s)
    }

    /// The transition table: `creating → created | refused` (Clerk's answer,
    /// or the dead-attempt sweep's), and `refused → creating` (the creator's
    /// retry of the same key, after the caps re-check). `created` is final.
    pub const fn can_become(self, to: IntentStatus) -> bool {
        use IntentStatus::*;
        matches!((self, to), (Creating, Created) | (Creating, Refused) | (Refused, Creating))
    }
}

// ── The abandoned-org state machine ───────────────────────────────────────

/// `org_selfserve_creations.sweep_state`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SweepState {
    None,
    Kept,
    Deleting,
    Deleted,
}

impl SweepState {
    pub const ALL: [SweepState; 4] = [SweepState::None, SweepState::Kept, SweepState::Deleting, SweepState::Deleted];

    pub const fn as_str(self) -> &'static str {
        match self {
            SweepState::None => "none",
            SweepState::Kept => "kept",
            SweepState::Deleting => "deleting",
            SweepState::Deleted => "deleted",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|t| t.as_str() == s)
    }

    /// The transition table. Every write below is a guarded `UPDATE ...
    /// WHERE sweep_state = <from>` for one of these edges.
    pub const fn can_become(self, to: SweepState) -> bool {
        use SweepState::*;
        matches!(
            (self, to),
            (None, Kept) | (None, Deleting) | (Deleting, Deleting) | (Deleting, None) | (Deleting, Deleted)
        )
    }

    pub const fn is_terminal(self) -> bool {
        matches!(self, SweepState::Kept | SweepState::Deleted)
    }
}

// ── Collection on a held org (S7) ─────────────────────────────────────────

/// Whether a held org's subscriptions are paused at Stripe
/// (`org_selfserve_creations.collection_paused_at` set or not).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CollectionState {
    Collecting,
    Paused,
}

impl CollectionState {
    /// `collecting → paused` while the job is `held`; `paused → collecting`
    /// once the job is `ready`. Anything else stays.
    pub fn next(self, job_held: bool, job_ready: bool) -> CollectionState {
        match self {
            CollectionState::Collecting if job_held => CollectionState::Paused,
            CollectionState::Paused if job_ready => CollectionState::Collecting,
            s => s,
        }
    }
}

/// What the intent row and its trial say, before any claim.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AbandonedPlan {
    /// Not abandoned (yet): leave it, look again later.
    NotYet,
    /// A real customer: mark `kept`, never look again.
    Keep,
    /// Claim it and run the deletion checks.
    Sweep,
}

/// The trial as the sweep sees it: state, when its Checkout was opened, and
/// when it ended (`abandoned`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TrialFacts {
    pub state: TrialState,
    pub opened_at: DateTime<Utc>,
    pub ended_at: Option<DateTime<Utc>>,
}

pub fn plan_abandoned(
    created_at: DateTime<Utc>,
    trial: Option<TrialFacts>,
    job_exists: bool,
    now: DateTime<Utc>,
    p: &CleanupPolicy,
) -> AbandonedPlan {
    if job_exists {
        return AbandonedPlan::Keep;
    }
    match trial {
        None if now >= created_at + p.no_checkout_after => AbandonedPlan::Sweep,
        None => AbandonedPlan::NotYet,
        Some(t) => match t.state {
            // #1521's worker abandons it at 24 h; until then it is the
            // creator's to resume.
            TrialState::CheckoutOpen => AbandonedPlan::NotYet,
            TrialState::Abandoned => {
                let since = t.ended_at.unwrap_or(t.opened_at);
                if now >= since + p.abandoned_grace {
                    AbandonedPlan::Sweep
                } else {
                    AbandonedPlan::NotYet
                }
            }
            // Past checkout: paid, trialing, converting, or ended through
            // the subscription (org deletion owns that, not this sweep).
            TrialState::Trialing | TrialState::Converting | TrialState::Converted | TrialState::Canceled => {
                AbandonedPlan::Keep
            }
        },
    }
}

/// The org's latest Checkout session, as Stripe reports it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionFact {
    /// No session recorded.
    None,
    Open,
    Complete,
    Expired,
    /// Stripe could not be read.
    Unknown,
}

impl SessionFact {
    pub fn from_status(status: Option<&str>) -> Self {
        match status {
            Some("open") => SessionFact::Open,
            Some("complete") => SessionFact::Complete,
            Some("expired") => SessionFact::Expired,
            _ => SessionFact::Unknown,
        }
    }
}

/// One Stripe subscription status, as far as cleanup cares.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubClass {
    /// `active` / `trialing`, or unpaid with a payment still `processing`:
    /// the org is paid (or will be), or holds a card on a trial.
    Live,
    /// Exists and could still charge, but has not been paid.
    Unpaid,
    /// `canceled` / `incomplete_expired`: nothing to do.
    Ended,
    /// A status this code does not know, or an unpaid one whose payment
    /// could not be read: never delete on it.
    Unknown,
}

/// Whether a status is unpaid-but-could-still-be-paying (the statuses whose
/// latest invoice's payment the sweep reads before deciding, S3).
pub fn needs_payment_check(status: &str) -> bool {
    matches!(status, "incomplete" | "past_due" | "unpaid")
}

/// `payment_processing`: `Some(true)` when the latest invoice has a
/// PaymentIntent in `processing` (a bank debit, SEPA or ACH, settling: the
/// money is on its way, S3), `Some(false)` when it was read and has none,
/// `None` when it could not be read (only consulted for the statuses
/// [`needs_payment_check`] names).
pub fn classify_subscription(status: &str, payment_processing: Option<bool>) -> SubClass {
    match status {
        "active" | "trialing" => SubClass::Live,
        s if needs_payment_check(s) => match payment_processing {
            Some(true) => SubClass::Live,
            Some(false) => SubClass::Unpaid,
            None => SubClass::Unknown,
        },
        "paused" => SubClass::Unpaid,
        "canceled" | "incomplete_expired" => SubClass::Ended,
        _ => SubClass::Unknown,
    }
}

/// Every subscription found for the org (the mirror's id and every one on
/// its Stripe customer), or `unreadable` when Stripe could not be read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscriptionFacts {
    pub subs: Vec<(String, SubClass)>,
    pub unreadable: bool,
}

impl SubscriptionFacts {
    pub fn none() -> Self {
        SubscriptionFacts { subs: Vec::new(), unreadable: false }
    }
    fn any(&self, c: SubClass) -> bool {
        self.subs.iter().any(|(_, k)| *k == c)
    }
    fn unpaid_ids(&self) -> Vec<String> {
        self.subs.iter().filter(|(_, k)| *k == SubClass::Unpaid).map(|(id, _)| id.clone()).collect()
    }
    fn not_ended_ids(&self) -> Vec<String> {
        self.subs.iter().filter(|(_, k)| *k != SubClass::Ended).map(|(id, _)| id.clone()).collect()
    }
}

/// What a claimed abandoned org gets next.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AbandonedAction {
    /// Give the claim back (`deleting → none`): a completion is landing, a
    /// live subscription exists, or Stripe could not be read.
    Release,
    /// Expire the open session, then decide again.
    ExpireSession,
    /// Cancel these unpaid subscriptions at Stripe, then delete.
    CancelThenDelete(Vec<String>),
    Delete,
}

pub fn decide_abandoned(session: SessionFact, subs: &SubscriptionFacts) -> AbandonedAction {
    match session {
        SessionFact::Complete | SessionFact::Unknown => return AbandonedAction::Release,
        SessionFact::Open => return AbandonedAction::ExpireSession,
        SessionFact::None | SessionFact::Expired => {}
    }
    decide_by_subscription(subs)
}

fn decide_by_subscription(subs: &SubscriptionFacts) -> AbandonedAction {
    if subs.unreadable || subs.any(SubClass::Live) || subs.any(SubClass::Unknown) {
        return AbandonedAction::Release;
    }
    let unpaid = subs.unpaid_ids();
    if unpaid.is_empty() {
        AbandonedAction::Delete
    } else {
        AbandonedAction::CancelThenDelete(unpaid)
    }
}

/// Whether a fresh read still allows the irreversible step a pass decided
/// on before it (S6). `planned` is what the pass is about to do; the re-read
/// must decide a deletion too, and must not name an unpaid subscription the
/// plan did not (a cancel list only ever shrinks: one canceled meanwhile, or
/// by this pass, reads `Ended`).
pub fn still_deletes(planned: &[String], fresh: &AbandonedAction) -> bool {
    match fresh {
        AbandonedAction::Delete => true,
        AbandonedAction::CancelThenDelete(ids) => ids.iter().all(|id| planned.contains(id)),
        AbandonedAction::Release | AbandonedAction::ExpireSession => false,
    }
}

/// What a claimed failed job's org gets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FailedJobAction {
    /// Paid after all: run the job again (the definer may hold it instead).
    Requeue,
    /// Paid, but the automatic requeues are spent: an operator decides.
    Hold,
    /// Stripe could not be read: give the claim back.
    Release,
    CancelThenDelete(Vec<String>),
    Delete,
}

pub fn decide_failed_job(subs: &SubscriptionFacts, requeues: i32, p: &CleanupPolicy) -> FailedJobAction {
    if subs.unreadable || subs.any(SubClass::Unknown) {
        return FailedJobAction::Release;
    }
    if subs.any(SubClass::Live) {
        return if requeues < p.max_auto_requeues { FailedJobAction::Requeue } else { FailedJobAction::Hold };
    }
    let unpaid = subs.unpaid_ids();
    if unpaid.is_empty() {
        FailedJobAction::Delete
    } else {
        FailedJobAction::CancelThenDelete(unpaid)
    }
}

fn failed_as_abandoned(a: &FailedJobAction) -> AbandonedAction {
    match a {
        FailedJobAction::Delete => AbandonedAction::Delete,
        FailedJobAction::CancelThenDelete(ids) => AbandonedAction::CancelThenDelete(ids.clone()),
        _ => AbandonedAction::Release,
    }
}

// ── The sweep ─────────────────────────────────────────────────────────────

pub struct Sweeper<'a> {
    pub control: &'a PgPool,
    pub clerk: &'a ClerkAdminClient,
    pub stripe: Option<&'a StripeClient>,
    pub policy: CleanupPolicy,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SweepReport {
    pub abandoned_deleted: Vec<String>,
    pub kept: Vec<String>,
    pub released: Vec<String>,
    pub failed_deleted: Vec<String>,
    pub requeued: Vec<String>,
    pub held: Vec<String>,
    pub canceled_subscriptions: Vec<String>,
    /// Intents left in `creating` whose org was found by its marker (N2).
    pub adopted: Vec<Uuid>,
    /// Intents left in `creating` with no org in Clerk: now `refused` (N2).
    pub dead_refused: Vec<Uuid>,
    /// Subscriptions whose collection was paused (S7).
    pub collection_paused: Vec<String>,
    /// Subscriptions whose collection was resumed (S7).
    pub collection_resumed: Vec<String>,
    /// Resumed subscriptions re-anchored because Stripe voided an invoice
    /// during the pause (#1540 SF1).
    pub collection_reanchored: Vec<String>,
    /// Failed jobs whose Clerk org does not carry the self-serve marker: left
    /// in Clerk, cleanup marked done (#1540 N7).
    pub unmarked_skipped: Vec<String>,
    /// Passes that stopped because their claim was taken over (S6).
    pub lost_claims: Vec<String>,
    /// Intent rows whose `cap_bucket` was cleared: older than the caps'
    /// longest window (#1540 review SF-d).
    pub cap_buckets_cleared: u64,
    /// Card-fingerprint trial claims deleted past [`CARD_CLAIM_RETENTION`].
    pub card_claims_expired: u64,
    pub errors: Vec<String>,
}

impl SweepReport {
    pub fn worked(&self) -> usize {
        self.abandoned_deleted.len()
            + self.kept.len()
            + self.failed_deleted.len()
            + self.requeued.len()
            + self.held.len()
            + self.adopted.len()
            + self.dead_refused.len()
            + self.collection_paused.len()
            + self.collection_resumed.len()
            + self.collection_reanchored.len()
            + self.unmarked_skipped.len()
    }
}

#[derive(Debug, sqlx::FromRow)]
struct IntentRow {
    id: Uuid,
    clerk_org_id: Option<String>,
    account_id: Option<Uuid>,
    created_at: DateTime<Utc>,
}

/// Intent rows cleared of `cap_bucket`, and card claims expired, per pass.
pub const RETENTION_BATCH: i64 = 1000;

/// How long a card-fingerprint trial claim (`org_trial_claims`, kind
/// `card`) is kept for fraud prevention: 24 months. Documentation only; the
/// interval itself is fixed inside the `org_trial_claims_expire_cards`
/// definer (control migration 0385), so the app role cannot delete a newer
/// claim.
pub const CARD_CLAIM_RETENTION: &str = "24 months";

pub fn cancel_key(account: Uuid, subscription_id: &str) -> String {
    format!("org-selfserve-cleanup-cancel:{account}:{subscription_id}")
}

/// Pausing collection on a held org's subscription (S7): one key per
/// subscription per failure, so a later hold after a resume is a new call.
pub fn pause_key(account: Uuid, subscription_id: &str, failed_at: DateTime<Utc>) -> String {
    format!("org-selfserve-held-pause:{account}:{subscription_id}:{}", failed_at.timestamp())
}

/// Re-anchoring a resumed subscription whose invoice was voided during the
/// pause (#1540 SF1): one key per subscription per pause.
pub fn reanchor_key(account: Uuid, subscription_id: &str, paused_at: DateTime<Utc>) -> String {
    format!("org-selfserve-held-reanchor:{account}:{subscription_id}:{}", paused_at.timestamp())
}

/// Whether a retrieved invoice was voided by the pause (#1540 SF1): `void`,
/// and VOIDED at or after `paused_at`: Stripe's
/// `status_transitions.voided_at` (unix seconds). A draft made before the
/// pause and voided during it counts (#1540 review SF-b); one voided before
/// the pause does not. Only when `voided_at` is missing is the invoice's
/// `created` used instead. An invoice with neither is not assumed to be one.
pub fn voided_during_pause(invoice: &Value, paused_at: DateTime<Utc>) -> bool {
    if invoice.get("status").and_then(Value::as_str) != Some("void") {
        return false;
    }
    let voided_at = invoice.get("status_transitions").and_then(|t| t.get("voided_at")).and_then(Value::as_i64);
    voided_at
        .or_else(|| invoice.get("created").and_then(Value::as_i64))
        .is_some_and(|at| at >= paused_at.timestamp())
}

/// Whether the org is served: its job is `ready`, or its routing row is
/// `active` (#1540 SF5).
pub(crate) async fn org_is_served(control: &PgPool, org: &str) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM tenant_provisioning_jobs WHERE clerk_org_id = $1 AND status = 'ready') \
             OR EXISTS (SELECT 1 FROM tenant_databases WHERE clerk_org_id = $1 AND status = 'active')",
    )
    .bind(org)
    .fetch_one(control)
    .await
}

/// Resuming it once the org is ready: one key per pause.
pub fn resume_key(account: Uuid, subscription_id: &str, paused_at: DateTime<Utc>) -> String {
    format!("org-selfserve-held-resume:{account}:{subscription_id}:{}", paused_at.timestamp())
}

/// The re-check a pass makes immediately before an irreversible step (S6).
enum Recheck {
    /// Still the claim's holder, and a fresh read still deletes.
    Proceed,
    /// Still the holder, but Stripe now says something else: give it back.
    Changed,
    /// The claim was taken over: stop, write nothing.
    Lost,
}

impl Sweeper<'_> {
    pub async fn sweep_once(&self, now: DateTime<Utc>) -> SweepReport {
        let mut report = SweepReport::default();
        if let Err(e) = self.sweep_dead_attempts(&mut report).await {
            report.errors.push(format!("dead-attempt scan: {e}"));
        }
        if let Err(e) = self.sweep_abandoned(now, &mut report).await {
            report.errors.push(format!("abandoned scan: {e}"));
        }
        if let Err(e) = self.sweep_failed(&mut report).await {
            report.errors.push(format!("failed-job scan: {e}"));
        }
        if let Err(e) = self.sweep_held_collection(&mut report).await {
            report.errors.push(format!("held-collection scan: {e}"));
        }
        if let Err(e) = self.sweep_retention(&mut report).await {
            report.errors.push(format!("retention: {e}"));
        }
        for e in &report.errors {
            tracing::error!(error = %e, alert = true, "org self-serve cleanup: a step failed; the next pass retries");
        }
        report
    }

    // ── retention (#1540 review) ──

    /// Clears `cap_bucket` on intent rows older than 24 h, the longest
    /// window any creation cap counts (SF-d: a free-mail bucket is a keyed
    /// digest of one person's mailbox, kept no longer than the cap needs
    /// it), and deletes card-fingerprint trial claims older than 24 months
    /// through the `org_trial_claims_expire_cards` definer (the app role has
    /// no privilege on `org_trial_claims`).
    async fn sweep_retention(&self, report: &mut SweepReport) -> Result<(), sqlx::Error> {
        let cleared = sqlx::query(
            "UPDATE org_selfserve_creations SET cap_bucket = NULL \
              WHERE id IN (SELECT id FROM org_selfserve_creations \
                            WHERE cap_bucket IS NOT NULL AND created_at < now() - interval '24 hours' \
                            ORDER BY created_at LIMIT $1)",
        )
        .bind(RETENTION_BATCH)
        .execute(self.control)
        .await?;
        report.cap_buckets_cleared = cleared.rows_affected();
        let expired: i32 = match sqlx::query_scalar("SELECT public.org_trial_claims_expire_cards($1)")
            .bind(RETENTION_BATCH as i32)
            .fetch_one(self.control)
            .await
        {
            Ok(n) => n,
            // SQLSTATE 42883 (undefined_function): the definer's migration
            // (0385) is not applied on this database yet. Not a failing step
            // to alert on every pass: warn once per process and skip.
            Err(sqlx::Error::Database(e)) if e.code().as_deref() == Some("42883") => {
                static WARNED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
                if !WARNED.swap(true, std::sync::atomic::Ordering::Relaxed) {
                    tracing::warn!(
                        "org self-serve cleanup: org_trial_claims_expire_cards() does not exist (migration 0385 not \
                         applied?); card-claim retention is skipped until it does"
                    );
                }
                0
            }
            Err(e) => return Err(e),
        };
        report.card_claims_expired = expired.max(0) as u64;
        Ok(())
    }

    // ── dead attempts (N2) ──

    async fn sweep_dead_attempts(&self, report: &mut SweepReport) -> Result<(), sqlx::Error> {
        let dead: Vec<(Uuid, Option<Uuid>)> = sqlx::query_as(
            "SELECT id, user_id FROM org_selfserve_creations \
              WHERE status = 'creating' AND created_at < now() - $1::interval \
                AND (lease_until IS NULL OR lease_until < now()) \
              ORDER BY created_at LIMIT $2",
        )
        .bind(chrono_to_std(self.policy.dead_attempt_after))
        .bind(self.policy.batch)
        .fetch_all(self.control)
        .await?;
        for (id, user) in dead {
            if let Err(e) = self.dead_attempt_one(id, user, report).await {
                report.errors.push(format!("intent {id}: {e}"));
            }
        }
        Ok(())
    }

    async fn dead_attempt_one(&self, id: Uuid, user: Option<Uuid>, report: &mut SweepReport) -> Result<(), String> {
        // The sweep takes the same lease a request takes, so a creator who
        // retries right now gets `409 create_in_progress` instead of racing.
        let token = Uuid::new_v4();
        let leased: Option<Uuid> = sqlx::query_scalar(
            "UPDATE org_selfserve_creations SET lease_token = $2, lease_until = now() + interval '2 minutes', updated_at = now() \
              WHERE id = $1 AND status = 'creating' AND (lease_until IS NULL OR lease_until < now()) RETURNING id",
        )
        .bind(id)
        .bind(token)
        .fetch_optional(self.control)
        .await
        .map_err(|e| e.to_string())?;
        if leased.is_none() {
            return Ok(());
        }
        let lease = crate::routes::orgs_create::Lease { intent: id, token };
        let result = self.dead_attempt_leased(lease, user, report).await;
        crate::routes::orgs_create::release_lease(self.control, lease).await;
        result
    }

    async fn dead_attempt_leased(
        &self,
        lease: crate::routes::orgs_create::Lease,
        user: Option<Uuid>,
        report: &mut SweepReport,
    ) -> Result<(), String> {
        let clerk_user: Option<String> = match user {
            Some(u) => sqlx::query_scalar("SELECT clerk_user_id FROM users WHERE id = $1")
                .bind(u)
                .fetch_optional(self.control)
                .await
                .map_err(|e| e.to_string())?
                .flatten(),
            None => None,
        };
        let found = match (&clerk_user, user) {
            (Some(clerk_user), Some(_)) => crate::routes::orgs_create::find_by_marker(self.clerk, clerk_user, lease.intent)
                .await
                .map_err(|_| "Clerk organization list unavailable".to_string())?,
            _ => None,
        };
        match (found, user) {
            (Some(org), Some(user)) => {
                match crate::routes::orgs_create::record_created(self.control, lease, &org, user).await {
                    Ok(Some(_)) => {
                        tracing::warn!(intent = %lease.intent, org = %org, alert = true,
                            "org self-serve cleanup: adopted the Clerk org of a dead create attempt; the abandoned-org \
                             sweep now owns it");
                        report.adopted.push(lease.intent);
                        Ok(())
                    }
                    Ok(None) => Ok(()),
                    Err(_) => Err("recording the adopted org failed".into()),
                }
            }
            _ => {
                debug_assert!(IntentStatus::Creating.can_become(IntentStatus::Refused));
                let r = sqlx::query(
                    "UPDATE org_selfserve_creations SET status = 'refused', updated_at = now() \
                      WHERE id = $1 AND status = 'creating' AND lease_token = $2",
                )
                .bind(lease.intent)
                .bind(lease.token)
                .execute(self.control)
                .await
                .map_err(|e| e.to_string())?;
                if r.rows_affected() == 1 {
                    if user.is_none() || clerk_user.is_none() {
                        tracing::warn!(intent = %lease.intent, alert = true,
                            "org self-serve cleanup: a dead create attempt's creator is gone; marked refused without \
                             asking Clerk (a Clerk org it may have made carries the intent marker)");
                    }
                    report.dead_refused.push(lease.intent);
                }
                Ok(())
            }
        }
    }

    // ── abandoned sign-ups ──

    async fn sweep_abandoned(&self, now: DateTime<Utc>, report: &mut SweepReport) -> Result<(), sqlx::Error> {
        let candidates: Vec<IntentRow> = sqlx::query_as(
            "SELECT id, clerk_org_id, account_id, created_at FROM org_selfserve_creations \
              WHERE status = 'created' \
                AND ((sweep_state = 'none' AND sweep_next_check_at <= now()) \
                     OR (sweep_state = 'deleting' AND sweep_claimed_at < now() - $1::interval)) \
              ORDER BY sweep_next_check_at, id LIMIT $2",
        )
        .bind(self.policy.stale_claim)
        .bind(self.policy.batch)
        .fetch_all(self.control)
        .await?;
        for intent in candidates {
            let org = intent.clerk_org_id.clone().unwrap_or_default();
            if let Err(e) = self.abandoned_one(&intent, now, report).await {
                report.errors.push(format!("{org}: {e}"));
            }
        }
        Ok(())
    }

    async fn abandoned_facts(&self, account: Option<Uuid>, session_id: Option<&str>) -> (SessionFact, SubscriptionFacts) {
        let session = match session_id {
            None => SessionFact::None,
            Some(id) => self.session_fact(id).await,
        };
        let subs = match account {
            Some(account) => self.subscription_facts(account).await,
            None => SubscriptionFacts::none(),
        };
        (session, subs)
    }

    async fn abandoned_one(&self, intent: &IntentRow, now: DateTime<Utc>, report: &mut SweepReport) -> Result<(), String> {
        let org = intent.clerk_org_id.clone().ok_or("created intent without an org id")?;
        let (trial, job_exists, session_id) = match intent.account_id {
            Some(account) => {
                let (t, s) = read_trial(self.control, account).await.map_err(|e| format!("org_trials: {e}"))?;
                (t, job_exists(self.control, &org).await.map_err(|e| e.to_string())?, s)
            }
            // The account is already gone (an earlier pass deleted it and
            // crashed before marking the intent): only Clerk may be left.
            None => (None, false, None),
        };
        match plan_abandoned(intent.created_at, trial, job_exists, now, &self.policy) {
            AbandonedPlan::NotYet => {
                defer_intent(self.control, intent.id, self.policy.recheck_after).await.map_err(|e| e.to_string())?;
                return Ok(());
            }
            AbandonedPlan::Keep => {
                set_sweep(self.control, intent.id, SweepState::None, SweepState::Kept, None)
                    .await
                    .map_err(|e| e.to_string())?;
                report.kept.push(org);
                return Ok(());
            }
            AbandonedPlan::Sweep => {}
        }
        let Some(claim) = claim_intent(self.control, intent.id, self.policy.stale_claim).await.map_err(|e| e.to_string())?
        else {
            return Ok(()); // another pass holds it, or a request holds the lease
        };
        let release = |why: &'static str| {
            let control = self.control;
            let id = intent.id;
            let after = self.policy.recheck_after;
            async move {
                tracing::warn!(intent = %id, why, "org self-serve cleanup: abandoned org kept for now");
                release_intent(control, id, claim, after).await
            }
        };

        let (mut session, subs) = self.abandoned_facts(intent.account_id, session_id.as_deref()).await;
        let mut action = decide_abandoned(session, &subs);
        if action == AbandonedAction::ExpireSession {
            let id = session_id.as_deref().unwrap_or_default();
            if let Some(stripe) = self.stripe {
                // A 400 means it completed or expired meanwhile: re-read.
                let _ = crate::billing::seat_lifecycle::with_stripe_deadline(stripe.expire_checkout_session(id)).await;
            }
            session = self.session_fact(id).await;
            action = match session {
                SessionFact::Open => AbandonedAction::Release,
                s => decide_abandoned(s, &subs),
            };
        }
        let cancel = match &action {
            AbandonedAction::Release | AbandonedAction::ExpireSession => {
                release("a completion may land, a subscription is live, or Stripe was unreadable")
                    .await
                    .map_err(|e| e.to_string())?;
                report.released.push(org);
                return Ok(());
            }
            AbandonedAction::CancelThenDelete(ids) => ids.clone(),
            AbandonedAction::Delete => Vec::new(),
        };

        // S6: immediately before the cancel, and again before the Clerk
        // delete, the claim must still be ours and a fresh Stripe read must
        // still decide a deletion.
        if let Some(account) = intent.account_id.filter(|_| !cancel.is_empty()) {
            match self.recheck_intent(intent, claim, session_id.as_deref(), &cancel).await? {
                Recheck::Proceed => {}
                Recheck::Changed => {
                    release("Stripe changed before the cancel").await.map_err(|e| e.to_string())?;
                    report.released.push(org);
                    return Ok(());
                }
                Recheck::Lost => {
                    report.lost_claims.push(org);
                    return Ok(());
                }
            }
            if let Err(e) = self.cancel_unpaid(account, &cancel, report).await {
                release("the cancel failed").await.map_err(|e| e.to_string())?;
                return Err(e);
            }
        }
        match self.recheck_intent(intent, claim, session_id.as_deref(), &cancel).await? {
            Recheck::Proceed => {}
            Recheck::Changed => {
                release("Stripe changed before the Clerk delete").await.map_err(|e| e.to_string())?;
                report.released.push(org);
                return Ok(());
            }
            Recheck::Lost => {
                report.lost_claims.push(org);
                return Ok(());
            }
        }
        if let Err(e) = self.delete_clerk_org(&org).await {
            release("the Clerk delete failed").await.map_err(|e| e.to_string())?;
            return Err(e);
        }
        match delete_abandoned_account(self.control, intent.id, intent.account_id, claim).await {
            Ok(true) => {
                tracing::info!(org = %org, "org self-serve cleanup: abandoned sign-up deleted (Clerk org and account)");
                report.abandoned_deleted.push(org);
                Ok(())
            }
            Ok(false) => {
                // The trial moved on between the checks and the delete. The
                // Clerk org is already gone, so this needs a person.
                tracing::error!(org = %org, alert = true,
                    "org self-serve cleanup: Clerk org deleted but the account changed underneath (trial no longer \
                     abandoned, or a job appeared); the account row was kept, operator action needed");
                let _ = release("the account changed after the Clerk delete").await;
                Err("account changed after its Clerk org was deleted".into())
            }
            Err(e) => {
                let _ = release("the account delete failed").await;
                Err(format!("account delete: {e}"))
            }
        }
    }

    async fn recheck_intent(
        &self,
        intent: &IntentRow,
        claim: DateTime<Utc>,
        session_id: Option<&str>,
        planned: &[String],
    ) -> Result<Recheck, String> {
        if !holds_intent_claim(self.control, intent.id, claim).await.map_err(|e| e.to_string())? {
            tracing::warn!(intent = %intent.id, "org self-serve cleanup: claim taken over; this pass stops");
            return Ok(Recheck::Lost);
        }
        let (session, subs) = self.abandoned_facts(intent.account_id, session_id).await;
        Ok(if still_deletes(planned, &decide_abandoned(session, &subs)) { Recheck::Proceed } else { Recheck::Changed })
    }

    // ── failed provisioning ──

    async fn sweep_failed(&self, report: &mut SweepReport) -> Result<(), sqlx::Error> {
        let grace_secs = self.policy.failed_grace.num_seconds().max(0) as u64;
        // N4: by `updated_at`, which every release bumps, so a released org
        // goes to the back instead of being picked first again.
        let candidates: Vec<(String, Uuid)> = sqlx::query_as(
            "SELECT clerk_org_id, account_id FROM tenant_provisioning_jobs \
              WHERE status = 'failed' \
                AND ((clerk_org_cleanup = 'pending' AND failed_at < now() - $1::interval) \
                     OR (clerk_org_cleanup = 'deleting' AND clerk_org_cleanup_claimed_at < now() - $2::interval)) \
              ORDER BY updated_at, clerk_org_id LIMIT $3",
        )
        .bind(Duration::from_secs(grace_secs))
        .bind(self.policy.stale_claim)
        .bind(self.policy.batch)
        .fetch_all(self.control)
        .await?;
        for (org, account) in candidates {
            if let Err(e) = self.failed_one(&org, account, report).await {
                report.errors.push(format!("{org}: {e}"));
            }
        }
        Ok(())
    }

    async fn failed_one(&self, org: &str, account: Uuid, report: &mut SweepReport) -> Result<(), String> {
        let Some(claim) = provisioning::claim_clerk_org_cleanup(self.control, org, self.policy.stale_claim)
            .await
            .map_err(|e| e.to_string())?
        else {
            return Ok(()); // another pass or an owner retry won
        };
        let max = self.policy.max_auto_requeues;
        let release = |how| provisioning::release_clerk_org_cleanup(self.control, org, how, claim, max);
        // S2: the customer's whole list, not only the mirror's subscription.
        let subs = self.subscription_facts(account).await;
        let requeues = provisioning::clerk_org_cleanup_requeues(self.control, org).await.map_err(|e| e.to_string())?;
        let decided = decide_failed_job(&subs, requeues, &self.policy);
        let cancel = match decided {
            FailedJobAction::Release => {
                if release(CleanupRelease::Pending).await.map_err(|e| e.to_string())? == ReleaseOutcome::Lost {
                    report.lost_claims.push(org.to_string());
                } else {
                    report.released.push(org.to_string());
                }
                return Ok(());
            }
            FailedJobAction::Requeue | FailedJobAction::Hold => {
                let how = if decided == FailedJobAction::Requeue { CleanupRelease::Requeue } else { CleanupRelease::Held };
                let outcome = release(how).await.map_err(|e| e.to_string())?;
                match outcome {
                    ReleaseOutcome::Requeued => {
                        tracing::error!(org = %org, %account, requeues = requeues + 1, alert = true,
                            "org self-serve cleanup: a failed org's subscription is live (a late payment?); its \
                             provisioning job was requeued instead of deleting the org");
                        report.requeued.push(org.to_string());
                    }
                    o if o.is_held() => {
                        tracing::error!(org = %org, %account, requeues, outcome = ?o, alert = true,
                            "org self-serve cleanup: a PAID org's provisioning cannot be requeued (requeues spent, or \
                             already routed); held for an operator, collection is paused at Stripe (the owner can \
                             still retry)");
                        report.held.push(org.to_string());
                        if let Err(e) = self.pause_held(org, account, report).await {
                            report.errors.push(format!("{org}: pause collection: {e}"));
                        }
                    }
                    ReleaseOutcome::Lost => report.lost_claims.push(org.to_string()),
                    ReleaseOutcome::Pending => report.released.push(org.to_string()),
                    _ => {}
                }
                return Ok(());
            }
            FailedJobAction::CancelThenDelete(ids) => ids,
            FailedJobAction::Delete => Vec::new(),
        };
        // #1540 N7: only an org `POST /v1/orgs` created is ever cancelled or
        // deleted by this sweep. Checked before ANY destructive step, on
        // both sides: our own record (an `org_selfserve_creations` row in
        // status `created` for this org, review follow-up: a Clerk 404 alone
        // must never let the sweep cancel a concierge org's subscriptions),
        // and Clerk's (`private_metadata` carries the marker). An org Clerk
        // no longer has (404) goes on only with our record: its delete is a
        // no-op, its unpaid subscriptions are ours to cancel.
        if !self.has_selfserve_intent(org).await.map_err(|e| e.to_string())? {
            tracing::error!(org = %org, %account, alert = true,
                "org self-serve cleanup: a failed provisioning job has no self-serve creation record; its Clerk org \
                 is NOT deleted and its subscriptions are NOT cancelled (cleanup marked done; an operator decides)");
            if !provisioning::mark_clerk_org_cleanup_done(self.control, org, claim).await.map_err(|e| e.to_string())? {
                report.lost_claims.push(org.to_string());
                return Ok(());
            }
            report.unmarked_skipped.push(org.to_string());
            return Ok(());
        }
        match self.clerk.get_organization(org).await {
            Ok(o) if o.private_metadata.get(provisioning::SELFSERVE_ORG_MARKER).and_then(Value::as_bool) == Some(true) => {}
            Ok(_) => {
                tracing::error!(org = %org, %account, alert = true,
                    "org self-serve cleanup: a failed provisioning job's Clerk org has no self-serve marker; it is NOT \
                     deleted and its subscriptions are NOT cancelled (cleanup marked done; an operator decides)");
                if !provisioning::mark_clerk_org_cleanup_done(self.control, org, claim).await.map_err(|e| e.to_string())? {
                    report.lost_claims.push(org.to_string());
                    return Ok(());
                }
                report.unmarked_skipped.push(org.to_string());
                return Ok(());
            }
            Err(crate::clerk_admin::ClerkAdminError::Api { status: 404, .. }) => {}
            Err(e) => {
                release(CleanupRelease::Pending).await.map_err(|e| e.to_string())?;
                return Err(format!("Clerk org read before delete: {e}"));
            }
        }
        if !cancel.is_empty() {
            match self.recheck_job(org, account, claim, &cancel, requeues).await? {
                Recheck::Proceed => {}
                Recheck::Changed => {
                    release(CleanupRelease::Pending).await.map_err(|e| e.to_string())?;
                    report.released.push(org.to_string());
                    return Ok(());
                }
                Recheck::Lost => {
                    report.lost_claims.push(org.to_string());
                    return Ok(());
                }
            }
            if let Err(e) = self.cancel_unpaid(account, &cancel, report).await {
                release(CleanupRelease::Pending).await.map_err(|e| e.to_string())?;
                return Err(e);
            }
        }
        match self.recheck_job(org, account, claim, &cancel, requeues).await? {
            Recheck::Proceed => {}
            Recheck::Changed => {
                release(CleanupRelease::Pending).await.map_err(|e| e.to_string())?;
                report.released.push(org.to_string());
                return Ok(());
            }
            Recheck::Lost => {
                report.lost_claims.push(org.to_string());
                return Ok(());
            }
        }
        if let Err(e) = self.delete_clerk_org(org).await {
            release(CleanupRelease::Pending).await.map_err(|e| e.to_string())?;
            return Err(e);
        }
        if !provisioning::mark_clerk_org_cleanup_done(self.control, org, claim).await.map_err(|e| e.to_string())? {
            return Err("lost the cleanup claim after deleting the Clerk org".into());
        }
        tracing::info!(org = %org, "org self-serve cleanup: failed, unpaid org's Clerk org deleted");
        report.failed_deleted.push(org.to_string());
        Ok(())
    }

    /// Whether `POST /v1/orgs` recorded creating this Clerk org (#1540 N7).
    async fn has_selfserve_intent(&self, org: &str) -> Result<bool, sqlx::Error> {
        sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM org_selfserve_creations WHERE clerk_org_id = $1 AND status = 'created')",
        )
        .bind(org)
        .fetch_one(self.control)
        .await
    }

    async fn recheck_job(
        &self,
        org: &str,
        account: Uuid,
        claim: DateTime<Utc>,
        planned: &[String],
        requeues: i32,
    ) -> Result<Recheck, String> {
        if !provisioning::holds_clerk_org_cleanup(self.control, org, claim).await.map_err(|e| e.to_string())? {
            tracing::warn!(org = %org, "org self-serve cleanup: failed-org claim taken over; this pass stops");
            return Ok(Recheck::Lost);
        }
        let fresh = decide_failed_job(&self.subscription_facts(account).await, requeues, &self.policy);
        Ok(if still_deletes(planned, &failed_as_abandoned(&fresh)) { Recheck::Proceed } else { Recheck::Changed })
    }

    // ── held, paid orgs (S7) ──

    async fn sweep_held_collection(&self, report: &mut SweepReport) -> Result<(), sqlx::Error> {
        // Held with collection still running (a pause that failed when the
        // org was held is retried here), and paused orgs that are served.
        // #1540 SF5: "served" is the job being `ready` OR the org's routing
        // row being `active`. A `held_routed` org (a requeue of an
        // already-routed org) has a failed job forever, but if its routing
        // row is active it has a workspace: it is never paused, and one
        // paused before is resumed, so it is not given unbilled service.
        let rows: Vec<(String, Uuid, bool, bool, Option<DateTime<Utc>>)> = sqlx::query_as(
            "SELECT j.clerk_org_id, j.account_id, \
                    (j.status = 'failed' AND j.clerk_org_cleanup = 'held') AS held, \
                    (j.status = 'ready' OR td.status IS NOT DISTINCT FROM 'active') AS served, \
                    c.collection_paused_at \
               FROM tenant_provisioning_jobs j JOIN org_selfserve_creations c ON c.clerk_org_id = j.clerk_org_id \
               LEFT JOIN tenant_databases td ON td.clerk_org_id = j.clerk_org_id \
              WHERE (j.status = 'failed' AND j.clerk_org_cleanup = 'held' AND c.collection_paused_at IS NULL \
                     AND c.status = 'created' AND td.status IS DISTINCT FROM 'active') \
                 OR (c.collection_paused_at IS NOT NULL AND (j.status = 'ready' OR td.status IS NOT DISTINCT FROM 'active')) \
              ORDER BY j.updated_at LIMIT $1",
        )
        .bind(self.policy.batch)
        .fetch_all(self.control)
        .await?;
        for (org, account, held, served, paused_at) in rows {
            let state = if paused_at.is_some() { CollectionState::Paused } else { CollectionState::Collecting };
            let result = match state.next(held && !served, served) {
                CollectionState::Paused if state == CollectionState::Collecting => self.pause_held(&org, account, report).await,
                CollectionState::Collecting if state == CollectionState::Paused => {
                    self.resume_ready(&org, account, paused_at.unwrap_or_else(Utc::now), report).await
                }
                _ => Ok(()),
            };
            if let Err(e) = result {
                report.errors.push(format!("{org}: collection: {e}"));
            }
        }
        Ok(())
    }

    /// Pause collection on every subscription of a held org (S7), keyed, and
    /// record it. Nothing to pause (no subscription found) is recorded too.
    async fn pause_held(&self, org: &str, account: Uuid, report: &mut SweepReport) -> Result<(), String> {
        let stripe = self.stripe.ok_or("Stripe is not configured; a held org's collection was not paused")?;
        // #1540 N7: only an org `POST /v1/orgs` created is ever touched at
        // Stripe by this sweep, the same record check as the cancel/delete
        // path. Covers both callers (the failed-job Hold arm and the
        // held-collection pass).
        if !self.has_selfserve_intent(org).await.map_err(|e| e.to_string())? {
            tracing::error!(org = %org, %account, alert = true,
                "org self-serve cleanup: a held org has no self-serve creation record; its collection is NOT paused \
                 (an operator decides)");
            return Ok(());
        }
        // #1540 SF5: an org whose routing row is active is served; it keeps
        // paying (a `held_routed` org).
        if org_is_served(self.control, org).await.map_err(|e| e.to_string())? {
            tracing::warn!(org = %org, %account, alert = true,
                "org self-serve cleanup: a held org's routing row is active (it is served); collection NOT paused");
            return Ok(());
        }
        let failed_at: Option<DateTime<Utc>> =
            sqlx::query_scalar("SELECT failed_at FROM tenant_provisioning_jobs WHERE clerk_org_id = $1")
                .bind(org)
                .fetch_optional(self.control)
                .await
                .map_err(|e| e.to_string())?
                .flatten();
        let subs = self.subscription_facts(account).await;
        if subs.unreadable {
            return Err("subscriptions unreadable".into());
        }
        let anchor = failed_at.unwrap_or_else(Utc::now);
        for id in subs.not_ended_ids() {
            let mut p = Params::new();
            p.push("pause_collection[behavior]", "void");
            let key = pause_key(account, &id, anchor);
            crate::billing::seat_lifecycle::with_stripe_deadline(stripe.post_form(
                &format!("/v1/subscriptions/{id}"),
                &p,
                Idem::Key(&key),
            ))
            .await
            .map_err(|e| format!("pause {id}: {e}"))?;
            tracing::error!(org = %org, %account, subscription = %id, alert = true,
                "org self-serve cleanup: collection paused (invoices voided) on a held org's subscription until its \
                 workspace is ready");
            report.collection_paused.push(id);
        }
        sqlx::query(
            "UPDATE org_selfserve_creations SET collection_paused_at = now(), updated_at = now() \
              WHERE clerk_org_id = $1 AND collection_paused_at IS NULL",
        )
        .bind(org)
        .execute(self.control)
        .await
        .map_err(|e| e.to_string())?;
        Ok(())
    }

    async fn resume_ready(&self, org: &str, account: Uuid, paused_at: DateTime<Utc>, report: &mut SweepReport) -> Result<(), String> {
        let stripe = self.stripe.ok_or("Stripe is not configured; a ready org's collection was not resumed")?;
        let subs = self.subscription_facts(account).await;
        if subs.unreadable {
            return Err("subscriptions unreadable".into());
        }
        for id in subs.not_ended_ids() {
            let mut p = Params::new();
            p.push("pause_collection", "");
            let key = resume_key(account, &id, paused_at);
            crate::billing::seat_lifecycle::with_stripe_deadline(stripe.post_form(
                &format!("/v1/subscriptions/{id}"),
                &p,
                Idem::Key(&key),
            ))
            .await
            .map_err(|e| format!("resume {id}: {e}"))?;
            tracing::warn!(org = %org, %account, subscription = %id, alert = true,
                "org self-serve cleanup: a held org is now ready; collection resumed on its subscription");
            if self.reanchor_if_voided_in_pause(stripe, account, &id, paused_at).await? {
                report.collection_reanchored.push(id.clone());
            }
            report.collection_resumed.push(id);
        }
        sqlx::query(
            "UPDATE org_selfserve_creations SET collection_paused_at = NULL, updated_at = now() \
              WHERE clerk_org_id = $1 AND collection_paused_at = $2",
        )
        .bind(org)
        .bind(paused_at)
        .execute(self.control)
        .await
        .map_err(|e| e.to_string())?;
        Ok(())
    }

    /// #1540 SF1. While a held org was paused (`pause_collection` with
    /// `behavior=void`), Stripe voided every invoice it made: the trial's
    /// conversion invoice when `trial_end` (the 90-day backstop) fell inside
    /// the pause, or a renewal. The subscription is then `active` with its
    /// latest invoice `void`, and the trial conversion would read that as
    /// "not paid" and cancel the subscription of an org that has only just
    /// got its workspace.
    ///
    /// So on resume, a subscription whose latest invoice is `void` and was
    /// created at or after the pause is re-anchored to now
    /// (`billing_cycle_anchor=now`, `proration_behavior=none`, keyed per
    /// pause): Stripe invoices a full period from today, charged like any
    /// other invoice, with no credit for the voided period (it was never
    /// paid) and no charge for the paused time (the org had no workspace).
    /// The conversion then reads that new invoice. `Ok(true)` when it
    /// re-anchored. A `trialing` subscription is left alone (its trial has
    /// not ended, nothing was voided), and so is one scheduled to end
    /// (`cancel_at_period_end`, or `cancel_at` set; #1540 review SF-c): the
    /// customer asked to stop, so a fresh full period must not be charged;
    /// the trial worker then cancels it as it would any unpaid conversion.
    async fn reanchor_if_voided_in_pause(
        &self,
        stripe: &StripeClient,
        account: Uuid,
        subscription_id: &str,
        paused_at: DateTime<Utc>,
    ) -> Result<bool, String> {
        use crate::billing::seat_lifecycle::with_stripe_deadline;
        let sub = with_stripe_deadline(stripe.retrieve_subscription(subscription_id))
            .await
            .map_err(|e| format!("re-read {subscription_id} after resume: {e}"))?;
        if matches!(sub.status.as_str(), "trialing" | "canceled" | "incomplete_expired") {
            return Ok(false);
        }
        if sub.cancel_at_period_end || sub.cancel_at.is_some() {
            tracing::warn!(%account, subscription = %subscription_id, alert = true,
                "org self-serve cleanup: a resumed subscription is scheduled to cancel; not re-anchored (no new \
                 period is charged)");
            return Ok(false);
        }
        let Some(invoice_id) = sub.latest_invoice.clone() else { return Ok(false) };
        let invoice = with_stripe_deadline(stripe.get(&format!("/v1/invoices/{invoice_id}"), &Params::new()))
            .await
            .map_err(|e| format!("read invoice {invoice_id}: {e}"))?;
        if !voided_during_pause(&invoice, paused_at) {
            return Ok(false);
        }
        let mut p = Params::new();
        p.push("billing_cycle_anchor", "now");
        p.push("proration_behavior", "none");
        let key = reanchor_key(account, subscription_id, paused_at);
        with_stripe_deadline(stripe.post_form(&format!("/v1/subscriptions/{subscription_id}"), &p, Idem::Key(&key)))
            .await
            .map_err(|e| format!("re-anchor {subscription_id}: {e}"))?;
        tracing::warn!(%account, subscription = %subscription_id, invoice = %invoice_id, alert = true,
            "org self-serve cleanup: an invoice was voided while collection was paused; the subscription was \
             re-anchored to now so the first period is charged afresh");
        Ok(true)
    }

    // ── shared steps ──

    async fn session_fact(&self, id: &str) -> SessionFact {
        let Some(stripe) = self.stripe else { return SessionFact::Unknown };
        match crate::billing::seat_lifecycle::with_stripe_deadline(stripe.retrieve_checkout_session(id)).await {
            Ok(v) => SessionFact::from_status(v.get("status").and_then(Value::as_str)),
            Err(e) => {
                tracing::warn!(error = %e, session = id, "org self-serve cleanup: Checkout session unreadable");
                SessionFact::Unknown
            }
        }
    }

    /// Whether the latest invoice of an unpaid subscription has a payment
    /// still `processing` (S3). Read through the invoice (its legacy
    /// `payment_intent`, or the `payments` list on API versions that dropped
    /// it) and then each PaymentIntent, so it works whatever API version the
    /// account is pinned to. `None` when anything could not be read.
    async fn payment_processing(&self, stripe: &StripeClient, latest_invoice: Option<&str>) -> Option<bool> {
        use crate::billing::seat_lifecycle::with_stripe_deadline;
        let Some(invoice) = latest_invoice else { return Some(false) };
        let mut q = Params::new();
        q.push("expand[0]", "payments");
        let inv = with_stripe_deadline(stripe.get(&format!("/v1/invoices/{invoice}"), &q)).await.ok()?;
        let mut intents: Vec<String> = Vec::new();
        if let Some(pi) = reference_id(inv.get("payment_intent")) {
            intents.push(pi);
        }
        for row in inv.get("payments").and_then(|p| p.get("data")).and_then(Value::as_array).into_iter().flatten() {
            if let Some(pi) = row.get("payment").and_then(|p| reference_id(p.get("payment_intent"))) {
                if !intents.contains(&pi) {
                    intents.push(pi);
                }
            }
        }
        for pi in intents {
            let v = with_stripe_deadline(stripe.get(&format!("/v1/payment_intents/{pi}"), &Params::new())).await.ok()?;
            if v.get("status").and_then(Value::as_str) == Some("processing") {
                return Some(true);
            }
        }
        Some(false)
    }

    async fn classify(&self, stripe: &StripeClient, status: &str, latest_invoice: Option<&str>) -> SubClass {
        let processing = if needs_payment_check(status) {
            self.payment_processing(stripe, latest_invoice).await
        } else {
            Some(false)
        };
        classify_subscription(status, processing)
    }

    /// The mirror's subscription, re-read at Stripe (its status there is the
    /// truth; the mirror can lag a late settlement), and every subscription
    /// on the org's Stripe customer, every page (S2, N9).
    async fn subscription_facts(&self, account: Uuid) -> SubscriptionFacts {
        let mirror = match read_mirror(self.control, account).await {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!(error = %e, %account, "org self-serve cleanup: subscription mirror unreadable");
                return SubscriptionFacts { subs: Vec::new(), unreadable: true };
            }
        };
        let Some((customer, sub_id)) = mirror else { return SubscriptionFacts::none() };
        let mut facts = SubscriptionFacts::none();
        let Some(stripe) = self.stripe else {
            facts.unreadable = sub_id.is_some() || customer.as_deref().is_some_and(|c| !c.is_empty());
            return facts;
        };
        if let Some(id) = &sub_id {
            match crate::billing::seat_lifecycle::with_stripe_deadline(stripe.retrieve_subscription(id)).await {
                Ok(s) => {
                    let class = self.classify(stripe, &s.status, s.latest_invoice.as_deref()).await;
                    facts.subs.push((s.id, class));
                }
                Err(e) => {
                    tracing::warn!(error = %e, %account, subscription = %id, "org self-serve cleanup: subscription unreadable");
                    facts.unreadable = true;
                }
            }
        }
        let Some(customer) = customer.filter(|c| !c.is_empty()) else { return facts };
        let mut starting_after: Option<String> = None;
        for _page in 0..20 {
            let mut q = Params::new();
            q.push("customer", customer.clone()).push("status", "all").push("limit", "100");
            if let Some(after) = &starting_after {
                q.push("starting_after", after.clone());
            }
            let v = match crate::billing::seat_lifecycle::with_stripe_deadline(stripe.get("/v1/subscriptions", &q)).await {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!(error = %e, %account, "org self-serve cleanup: customer subscriptions unreadable");
                    facts.unreadable = true;
                    return facts;
                }
            };
            let data: Vec<Value> = v.get("data").and_then(Value::as_array).cloned().unwrap_or_default();
            for s in &data {
                let (Some(id), Some(status)) = (s.get("id").and_then(Value::as_str), s.get("status").and_then(Value::as_str))
                else {
                    continue;
                };
                if !facts.subs.iter().any(|(seen, _)| seen == id) {
                    let latest = reference_id(s.get("latest_invoice"));
                    let class = self.classify(stripe, status, latest.as_deref()).await;
                    facts.subs.push((id.to_string(), class));
                }
            }
            let has_more = v.get("has_more").and_then(Value::as_bool).unwrap_or(false);
            match data.last().and_then(|s| s.get("id")).and_then(Value::as_str) {
                Some(last) if has_more => starting_after = Some(last.to_string()),
                _ => return facts,
            }
        }
        // More than 20 pages of subscriptions on one org's customer is not a
        // shape this sweep should decide on.
        facts.unreadable = true;
        facts
    }

    async fn cancel_unpaid(&self, account: Uuid, ids: &[String], report: &mut SweepReport) -> Result<(), String> {
        if ids.is_empty() {
            return Ok(());
        }
        let stripe = self.stripe.ok_or("Stripe is not configured but an unpaid subscription exists")?;
        for id in ids {
            let key = cancel_key(account, id);
            match crate::billing::seat_lifecycle::with_stripe_deadline(stripe.cancel_subscription_now_keyed(id, &key)).await
            {
                Ok(_) => {}
                Err(e) => {
                    // Already canceled is done; anything else is retried.
                    let ended = crate::billing::seat_lifecycle::with_stripe_deadline(stripe.retrieve_subscription(id))
                        .await
                        .map(|s| matches!(s.status.as_str(), "canceled" | "incomplete_expired"))
                        .unwrap_or(false);
                    if !ended {
                        return Err(format!("cancel subscription {id}: {e}"));
                    }
                }
            }
            tracing::warn!(%account, subscription = %id, alert = true,
                "org self-serve cleanup: canceled an unpaid subscription of an org being deleted (no charge)");
            report.canceled_subscriptions.push(id.clone());
        }
        Ok(())
    }

    async fn delete_clerk_org(&self, org: &str) -> Result<(), String> {
        match self.clerk.delete_organization(org).await {
            Ok(()) => Ok(()),
            // Already gone (an earlier pass, or deleted by hand): done.
            Err(e) if e.status() == Some(404) => Ok(()),
            Err(e) => Err(format!("Clerk delete organization: {e}")),
        }
    }
}

/// A Stripe reference: a bare id or an expanded object's `id`.
fn reference_id(v: Option<&Value>) -> Option<String> {
    match v? {
        Value::String(s) if !s.is_empty() => Some(s.clone()),
        Value::Object(o) => o.get("id").and_then(Value::as_str).map(str::to_string),
        _ => None,
    }
}

fn chrono_to_std(d: chrono::Duration) -> Duration {
    d.to_std().unwrap_or(Duration::ZERO)
}

// ── SQL ───────────────────────────────────────────────────────────────────

/// `(trial facts, latest Checkout session id)`. `org_trials` is RLS'd, so
/// the read runs inside `begin_account_scoped`.
async fn read_trial(control: &PgPool, account: Uuid) -> Result<(Option<TrialFacts>, Option<String>), sqlx::Error> {
    let mut tx = crate::tenancy::scope::begin_account_scoped(control, account)
        .await
        .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
    let row: Option<(String, DateTime<Utc>, Option<DateTime<Utc>>, Option<String>)> = sqlx::query_as(
        "SELECT state, checkout_opened_at, ended_at, checkout_session_id FROM org_trials WHERE account_id = $1",
    )
    .bind(account)
    .fetch_optional(&mut *tx)
    .await?;
    tx.commit().await?;
    let Some((state, opened_at, ended_at, session)) = row else { return Ok((None, None)) };
    let state = TrialState::parse(&state).ok_or_else(|| sqlx::Error::Protocol(format!("unknown trial state {state}")))?;
    Ok((Some(TrialFacts { state, opened_at, ended_at }), session))
}

async fn job_exists(control: &PgPool, org: &str) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM tenant_provisioning_jobs WHERE clerk_org_id = $1)")
        .bind(org)
        .fetch_one(control)
        .await
}

/// `Some((customer id, subscription id))` from the RLS'd mirror.
async fn read_mirror(control: &PgPool, account: Uuid) -> Result<Option<(Option<String>, Option<String>)>, sqlx::Error> {
    let mut tx = crate::tenancy::scope::begin_account_scoped(control, account)
        .await
        .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
    let row: Option<(Option<String>, Option<String>)> = sqlx::query_as(
        "SELECT stripe_customer_id, stripe_subscription_id FROM stripe_subscriptions WHERE account_id = $1",
    )
    .bind(account)
    .fetch_optional(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(row)
}

/// `none → kept` (the only non-claim edge written outside a claim).
async fn set_sweep(
    control: &PgPool,
    id: Uuid,
    from: SweepState,
    to: SweepState,
    claim: Option<DateTime<Utc>>,
) -> Result<bool, sqlx::Error> {
    debug_assert!(from.can_become(to));
    let r = sqlx::query(
        "UPDATE org_selfserve_creations SET sweep_state = $3, updated_at = now() \
          WHERE id = $1 AND sweep_state = $2 AND sweep_claimed_at IS NOT DISTINCT FROM $4",
    )
    .bind(id)
    .bind(from.as_str())
    .bind(to.as_str())
    .bind(claim)
    .execute(control)
    .await?;
    Ok(r.rows_affected() == 1)
}

/// A row the pass leaves alone (not due yet) is looked at again later, not
/// first in line next pass (N4).
async fn defer_intent(control: &PgPool, id: Uuid, after: chrono::Duration) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE org_selfserve_creations SET sweep_next_check_at = now() + $2::interval \
          WHERE id = $1 AND sweep_state = 'none'",
    )
    .bind(id)
    .bind(chrono_to_std(after))
    .execute(control)
    .await?;
    Ok(())
}

/// The atomic claim: `none → deleting`, or a takeover of a stale
/// `deleting`. Only while no `POST /v1/orgs` request holds the intent's
/// lease, and it clears that lease, so a request whose lease ran out can no
/// longer write (S5). Returns the claim's clock, which every later write of
/// this pass must still match (a takeover changes it, so a slow first pass
/// can no longer write).
pub(crate) async fn claim_intent(control: &PgPool, id: Uuid, stale: Duration) -> Result<Option<DateTime<Utc>>, sqlx::Error> {
    sqlx::query_scalar(
        "UPDATE org_selfserve_creations \
            SET sweep_state = 'deleting', sweep_claimed_at = clock_timestamp(), \
                lease_token = NULL, lease_until = NULL, updated_at = now() \
          WHERE id = $1 AND status = 'created' \
            AND (lease_until IS NULL OR lease_until < now()) \
            AND (sweep_state = 'none' \
                 OR (sweep_state = 'deleting' AND sweep_claimed_at < now() - $2::interval)) \
          RETURNING sweep_claimed_at",
    )
    .bind(id)
    .bind(stale)
    .fetch_optional(control)
    .await
}

/// Whether `claim` is still the current claim on the intent (S6).
pub(crate) async fn holds_intent_claim(control: &PgPool, id: Uuid, claim: DateTime<Utc>) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM org_selfserve_creations WHERE id = $1 AND sweep_state = 'deleting' AND sweep_claimed_at = $2)",
    )
    .bind(id)
    .bind(claim)
    .fetch_one(control)
    .await
}

/// `deleting → none`, only for the holder of `claim`; looked at again after
/// `after` (N4).
pub(crate) async fn release_intent(
    control: &PgPool,
    id: Uuid,
    claim: DateTime<Utc>,
    after: chrono::Duration,
) -> Result<bool, sqlx::Error> {
    let r = sqlx::query(
        "UPDATE org_selfserve_creations \
            SET sweep_state = 'none', sweep_claimed_at = NULL, sweep_next_check_at = now() + $3::interval, \
                updated_at = now() \
          WHERE id = $1 AND sweep_state = 'deleting' AND sweep_claimed_at = $2",
    )
    .bind(id)
    .bind(claim)
    .bind(chrono_to_std(after))
    .execute(control)
    .await?;
    Ok(r.rows_affected() == 1)
}

/// The last step for an abandoned org, in one transaction under the
/// account's financial lock: re-check that the trial is still `abandoned`
/// (or absent), that no provisioning job exists and that the mirror names
/// no live subscription; delete the account's memberships and the account
/// (cascading its trial, customer mapping and the rest); mark the intent
/// `deleted`. `Ok(false)` (nothing written) when a re-check fails, the claim
/// was taken over, or the account row was not the one deleted (N5).
pub(crate) async fn delete_abandoned_account(
    control: &PgPool,
    intent: Uuid,
    account: Option<Uuid>,
    claim: DateTime<Utc>,
) -> Result<bool, sqlx::Error> {
    let Some(account) = account else {
        let r = sqlx::query(
            "UPDATE org_selfserve_creations \
                SET sweep_state = 'deleted', sweep_claimed_at = NULL, deleted_at = now(), updated_at = now() \
              WHERE id = $1 AND sweep_state = 'deleting' AND sweep_claimed_at = $2",
        )
        .bind(intent)
        .bind(claim)
        .execute(control)
        .await?;
        return Ok(r.rows_affected() == 1);
    };
    let mut tx = crate::tenancy::scope::begin_account_scoped(control, account)
        .await
        .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
    crate::billing::renewal::lock_period_rotation(&mut tx, account).await?;
    let held: Option<Uuid> = sqlx::query_scalar(
        "SELECT id FROM org_selfserve_creations WHERE id = $1 AND sweep_state = 'deleting' AND sweep_claimed_at = $2 FOR UPDATE",
    )
    .bind(intent)
    .bind(claim)
    .fetch_optional(&mut *tx)
    .await?;
    if held.is_none() {
        tx.rollback().await?;
        return Ok(false);
    }
    let trial: Option<String> = sqlx::query_scalar("SELECT state FROM org_trials WHERE account_id = $1 FOR UPDATE")
        .bind(account)
        .fetch_optional(&mut *tx)
        .await?;
    let has_job: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM tenant_provisioning_jobs WHERE account_id = $1)")
        .bind(account)
        .fetch_one(&mut *tx)
        .await?;
    // #1540 N4: a deny-list, not an allow-list. The mirror folds Stripe's
    // `trialing` into `active` today (`stripe_webhook::map_subscription_status`),
    // but any subscription the mirror names that it does not say is
    // `canceled` (`trialing` if the mapping ever passes it through,
    // `pending`, anything new) keeps the org.
    let live_mirror: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM stripe_subscriptions WHERE account_id = $1 \
                          AND stripe_subscription_id IS NOT NULL AND status IS DISTINCT FROM 'canceled')",
    )
    .bind(account)
    .fetch_one(&mut *tx)
    .await?;
    let trial_ok = matches!(trial.as_deref(), None | Some("abandoned"));
    if !trial_ok || has_job || live_mirror {
        tx.rollback().await?;
        return Ok(false);
    }
    sqlx::query("DELETE FROM account_members WHERE account_id = $1")
        .bind(account)
        .execute(&mut *tx)
        .await?;
    let deleted = sqlx::query("DELETE FROM accounts WHERE id = $1 AND is_org_account")
        .bind(account)
        .execute(&mut *tx)
        .await?;
    if deleted.rows_affected() != 1 {
        // N5: RLS hid it, or it is not an org account. Nothing is marked.
        tx.rollback().await?;
        return Ok(false);
    }
    let marked = sqlx::query(
        "UPDATE org_selfserve_creations \
            SET sweep_state = 'deleted', sweep_claimed_at = NULL, deleted_at = now(), updated_at = now() \
          WHERE id = $1 AND sweep_state = 'deleting' AND sweep_claimed_at = $2",
    )
    .bind(intent)
    .bind(claim)
    .execute(&mut *tx)
    .await?;
    if marked.rows_affected() != 1 {
        tx.rollback().await?;
        return Ok(false);
    }
    tx.commit().await?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn t0() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 10, 27, 12, 0, 0).unwrap()
    }

    fn subs(list: &[(&str, SubClass)]) -> SubscriptionFacts {
        SubscriptionFacts { subs: list.iter().map(|(i, c)| (i.to_string(), *c)).collect(), unreadable: false }
    }

    #[test]
    fn the_sweep_state_table_is_exactly_the_documented_edges() {
        use SweepState::*;
        let allowed = [(None, Kept), (None, Deleting), (Deleting, Deleting), (Deleting, None), (Deleting, Deleted)];
        for from in SweepState::ALL {
            for to in SweepState::ALL {
                assert_eq!(from.can_become(to), allowed.contains(&(from, to)), "{from:?} -> {to:?}");
            }
            assert_eq!(SweepState::parse(from.as_str()), Some(from));
        }
        assert!(Kept.is_terminal() && Deleted.is_terminal());
        assert!(!None.is_terminal() && !Deleting.is_terminal());
        // Terminal states have no way out.
        for to in SweepState::ALL {
            assert!(!Kept.can_become(to) && !Deleted.can_become(to));
        }
    }

    #[test]
    fn the_clerk_org_cleanup_table_matches_control_0384() {
        use crate::tenancy::provisioning::ClerkOrgCleanup::{self, *};
        let allowed = [
            (None, Pending),
            (Pending, None),
            (Held, None),
            (Pending, Deleting),
            (Deleting, Deleting),
            (Deleting, Pending),
            (Deleting, Held),
            (Deleting, None),
            (Deleting, Done),
        ];
        for from in ClerkOrgCleanup::ALL {
            for to in ClerkOrgCleanup::ALL {
                assert_eq!(from.can_become(to), allowed.contains(&(from, to)), "{from:?} -> {to:?}");
            }
            assert_eq!(ClerkOrgCleanup::parse(from.as_str()), Some(from));
        }
        assert!(!Done.owner_may_retry() && !Deleting.owner_may_retry());
        assert!(Pending.owner_may_retry() && Held.owner_may_retry());
        for r in [CleanupRelease::Pending, CleanupRelease::Held, CleanupRelease::Requeue] {
            assert!(Deleting.can_become(r.target()), "{r:?}");
        }
    }

    #[test]
    fn abandoned_planning_waits_for_the_window_and_keeps_customers() {
        let p = CleanupPolicy::default();
        let now = t0();
        let trial = |state, ended: Option<i64>| {
            Some(TrialFacts {
                state,
                opened_at: now - chrono::Duration::hours(30),
                ended_at: ended.map(|h| now - chrono::Duration::hours(h)),
            })
        };
        let created = now - chrono::Duration::hours(30);
        assert_eq!(plan_abandoned(created, trial(TrialState::CheckoutOpen, None), false, now, &p), AbandonedPlan::NotYet);
        assert_eq!(plan_abandoned(created, trial(TrialState::Abandoned, Some(0)), false, now, &p), AbandonedPlan::NotYet,
            "inside the grace after #1521 abandoned it");
        assert_eq!(plan_abandoned(created, trial(TrialState::Abandoned, Some(2)), false, now, &p), AbandonedPlan::Sweep);
        for s in [TrialState::Trialing, TrialState::Converting, TrialState::Converted, TrialState::Canceled] {
            assert_eq!(plan_abandoned(created, trial(s, None), false, now, &p), AbandonedPlan::Keep, "{s:?}");
        }
        // A job means the Checkout completed: keep, whatever the trial says.
        assert_eq!(plan_abandoned(created, trial(TrialState::Abandoned, Some(5)), true, now, &p), AbandonedPlan::Keep);
        // No Checkout was ever issued.
        assert_eq!(plan_abandoned(now - chrono::Duration::hours(24), None, false, now, &p), AbandonedPlan::NotYet);
        assert_eq!(plan_abandoned(now - chrono::Duration::hours(26), None, false, now, &p), AbandonedPlan::Sweep);
    }

    #[test]
    fn an_abandoned_org_is_deleted_only_when_no_completion_can_land_and_nothing_is_live() {
        let none = SubscriptionFacts::none();
        assert_eq!(decide_abandoned(SessionFact::Complete, &none), AbandonedAction::Release);
        assert_eq!(decide_abandoned(SessionFact::Unknown, &none), AbandonedAction::Release);
        assert_eq!(decide_abandoned(SessionFact::Open, &none), AbandonedAction::ExpireSession);
        assert_eq!(decide_abandoned(SessionFact::Expired, &none), AbandonedAction::Delete);
        assert_eq!(decide_abandoned(SessionFact::None, &none), AbandonedAction::Delete);
        // A live subscription keeps the org; an unpaid one is canceled first.
        assert_eq!(decide_abandoned(SessionFact::Expired, &subs(&[("sub_a", SubClass::Live)])), AbandonedAction::Release);
        assert_eq!(
            decide_abandoned(SessionFact::Expired, &subs(&[("sub_a", SubClass::Unpaid), ("sub_b", SubClass::Ended)])),
            AbandonedAction::CancelThenDelete(vec!["sub_a".into()])
        );
        assert_eq!(decide_abandoned(SessionFact::Expired, &subs(&[("sub_a", SubClass::Unknown)])), AbandonedAction::Release);
        let unreadable = SubscriptionFacts { subs: vec![], unreadable: true };
        assert_eq!(decide_abandoned(SessionFact::Expired, &unreadable), AbandonedAction::Release);
    }

    #[test]
    fn a_failed_job_is_requeued_held_canceled_or_deleted_by_its_subscription() {
        let p = CleanupPolicy::default();
        let live = subs(&[("sub_a", SubClass::Live)]);
        assert_eq!(decide_failed_job(&live, 0, &p), FailedJobAction::Requeue);
        assert_eq!(decide_failed_job(&live, p.max_auto_requeues - 1, &p), FailedJobAction::Requeue);
        assert_eq!(decide_failed_job(&live, p.max_auto_requeues, &p), FailedJobAction::Hold);
        assert_eq!(
            decide_failed_job(&subs(&[("sub_a", SubClass::Unpaid)]), 0, &p),
            FailedJobAction::CancelThenDelete(vec!["sub_a".into()])
        );
        assert_eq!(decide_failed_job(&subs(&[("sub_a", SubClass::Ended)]), 0, &p), FailedJobAction::Delete);
        assert_eq!(decide_failed_job(&SubscriptionFacts::none(), 0, &p), FailedJobAction::Delete);
        assert_eq!(
            decide_failed_job(&SubscriptionFacts { subs: vec![], unreadable: true }, 0, &p),
            FailedJobAction::Release
        );
        // Live wins over an unpaid sibling: never cancel a paying org.
        assert_eq!(
            decide_failed_job(&subs(&[("sub_a", SubClass::Unpaid), ("sub_b", SubClass::Live)]), 0, &p),
            FailedJobAction::Requeue
        );
    }

    #[test]
    fn stripe_statuses_are_classified_fail_safe() {
        for s in ["active", "trialing"] {
            assert_eq!(classify_subscription(s, None), SubClass::Live);
        }
        // S3: an unpaid status is Unpaid only once its payment was read and
        // is not processing; a processing payment is Live; unread is Unknown.
        for s in ["incomplete", "past_due", "unpaid"] {
            assert_eq!(classify_subscription(s, Some(false)), SubClass::Unpaid);
            assert_eq!(classify_subscription(s, Some(true)), SubClass::Live, "{s} with a processing payment");
            assert_eq!(classify_subscription(s, None), SubClass::Unknown, "{s} with an unreadable payment");
        }
        assert_eq!(classify_subscription("paused", Some(false)), SubClass::Unpaid);
        for s in ["canceled", "incomplete_expired"] {
            assert_eq!(classify_subscription(s, None), SubClass::Ended);
        }
        assert_eq!(classify_subscription("something_new", Some(false)), SubClass::Unknown);
        assert_eq!(SessionFact::from_status(Some("open")), SessionFact::Open);
        assert_eq!(SessionFact::from_status(None), SessionFact::Unknown);
    }

    #[test]
    fn the_intent_status_and_collection_tables_are_exactly_the_documented_edges() {
        use IntentStatus::*;
        let allowed = [(Creating, Created), (Creating, Refused), (Refused, Creating)];
        for from in IntentStatus::ALL {
            for to in IntentStatus::ALL {
                assert_eq!(from.can_become(to), allowed.contains(&(from, to)), "{from:?} -> {to:?}");
            }
            assert_eq!(IntentStatus::parse(from.as_str()), Some(from));
        }
        use CollectionState::*;
        assert_eq!(Collecting.next(true, false), Paused);
        assert_eq!(Collecting.next(false, true), Collecting);
        assert_eq!(Paused.next(true, false), Paused);
        assert_eq!(Paused.next(false, true), Collecting);
        assert_eq!(Paused.next(false, false), Paused, "a held org being retried stays paused until ready");
    }

    #[test]
    fn a_fresh_read_must_still_delete_and_name_no_new_unpaid_subscription() {
        let planned = vec!["sub_a".to_string()];
        assert!(still_deletes(&planned, &AbandonedAction::Delete));
        assert!(still_deletes(&planned, &AbandonedAction::CancelThenDelete(vec!["sub_a".into()])));
        assert!(!still_deletes(&planned, &AbandonedAction::CancelThenDelete(vec!["sub_b".into()])));
        assert!(!still_deletes(&planned, &AbandonedAction::Release));
        assert!(!still_deletes(&[], &AbandonedAction::ExpireSession));
    }
}
