//! Billing notices: the emails that tell an admin, a member or a Pro owner
//! that extra usage has run into a limit (Settings launch plan PR 3.6,
//! decision 12), claimed once and delivered with retry.
//!
//! # The three stages
//!
//! 1. **Claim.** A notice is a `billing_notices` row, unique on
//!    `(account_id, notice_kind, dedupe_key)`. Whoever's
//!    `INSERT ... ON CONFLICT DO NOTHING` inserts the row is the only one who
//!    can ever send that notice; everyone else's insert is a no-op. The claim
//!    is a cheap insert and is the only thing the hot paths do.
//! 2. **Detect** (what claims, and from where):
//!    * the *settlement* side, by a worker scan ([`scan_account`]) of what the
//!      debit path already records: `extra_usage_periods.limit_reached_at`
//!      (the debit that reached the limit stamps it), a zero
//!      `extra_usage_balances` row after spend, and a clamped debit whose
//!      `extra_usage_ledger.note` is `member_cap_reached`. The debit
//!      transaction itself does nothing new;
//!    * the *refusal* side, by [`notify_refusal`], called from the pre-flight
//!      gates when they refuse with a cap/limit/balance reason. A debit that
//!      extra usage funded none of writes no ledger row, so the scan cannot
//!      see it; the refusal is the only witness. It runs on a spawned task
//!      with an in-process dedupe so a blocked member hammering the endpoint
//!      does not write a row per request.
//! 3. **Deliver.** A worker leases due notices ([`lease_due`]), resolves the
//!    recipients, renders and sends, and records the outcome. A send failure
//!    leaves the notice due again after a backoff, never lost.
//!
//! # Never twice
//!
//! * One claim per notice (stage 1).
//! * A lease moves `next_attempt_at` forward under `FOR UPDATE SKIP LOCKED`,
//!   so two workers cannot take the same notice. A worker that dies mid-send
//!   simply stops renewing nothing: the notice is due again when the lease
//!   expires.
//! * Per recipient, `delivered_to` is written after each successful send, so a
//!   retry after a partial failure skips the people already emailed.
//! * Every send carries Resend's `Idempotency-Key` (`notice id` + user), so
//!   the one window left (Resend accepted, the database write lost) does not
//!   produce a second email either.
//!
//! # Dedupe keys
//!
//! `dedupe_key` is the extra-usage period start (UTC, microsecond ISO form
//! from [`period_key_sql`]) for the limit and depleted notices;
//! `user:<user id>:<period start>` for a member cap; the reload id for reload
//! notices (3.5 claims those with [`claim`] and the reload id).
//!
//! # Recipients
//!
//! | kind | team organization | personal (Pro) |
//! |---|---|---|
//! | limit reached | every owner/admin | the owner |
//! | balance depleted | every owner/admin | the owner |
//! | member cap reached | that member only | the owner |
//! | reload succeeded / failed | every owner/admin | the owner |
//!
//! A member cap is a policy the admins already set for everyone; telling every
//! admin each time one member reaches it would be an email per member per
//! month. The member is the one who is blocked and needs to know, and their
//! email names the admins as the way to get more. Admins learn about the
//! account-wide events, which are the ones that block everybody.

pub mod email;
#[cfg(test)]
pub(crate) mod pg_tests;

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use sqlx::{PgConnection, PgPool};
use uuid::Uuid;

use self::email::{Audience, Mailer, NoticeFacts};
use super::funding_policy::db::extra_window;
use super::funding_policy::RefusalReason;

/// A notice kind. The strings are `billing_notices.notice_kind`'s CHECK list.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum NoticeKind {
    ExtraUsageLimitReached,
    MemberCapReached,
    BalanceDepleted,
    /// Claimed by the auto-reload worker (3.5), dedupe key = the reload id.
    AutoReloadSucceeded,
    /// Claimed by the auto-reload worker (3.5), dedupe key = the reload id.
    AutoReloadFailed,
    /// Self-serve PR C′: a team org's trial has used 80% of its credits.
    /// Claimed by `workers::org_trials`, dedupe key `trial`.
    TrialCredits80,
    /// The trial credits are used up and the subscription is starting.
    TrialCreditsUsed,
    /// The 90-day backstop converts the trial in 7 days or less.
    TrialEndingSoon,
}

impl NoticeKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            NoticeKind::ExtraUsageLimitReached => "extra_usage_limit_reached",
            NoticeKind::MemberCapReached => "member_cap_reached",
            NoticeKind::BalanceDepleted => "balance_depleted",
            NoticeKind::AutoReloadSucceeded => "auto_reload_succeeded",
            NoticeKind::AutoReloadFailed => "auto_reload_failed",
            NoticeKind::TrialCredits80 => "trial_credits_80",
            NoticeKind::TrialCreditsUsed => "trial_credits_used",
            NoticeKind::TrialEndingSoon => "trial_ending_soon",
        }
    }

    /// The trial emails go to the org's owner only (decision #1: "email the
    /// owner"), never to every admin.
    pub const fn is_trial(self) -> bool {
        matches!(self, NoticeKind::TrialCredits80 | NoticeKind::TrialCreditsUsed | NoticeKind::TrialEndingSoon)
    }

    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "extra_usage_limit_reached" => NoticeKind::ExtraUsageLimitReached,
            "member_cap_reached" => NoticeKind::MemberCapReached,
            "balance_depleted" => NoticeKind::BalanceDepleted,
            "auto_reload_succeeded" => NoticeKind::AutoReloadSucceeded,
            "auto_reload_failed" => NoticeKind::AutoReloadFailed,
            "trial_credits_80" => NoticeKind::TrialCredits80,
            "trial_credits_used" => NoticeKind::TrialCreditsUsed,
            "trial_ending_soon" => NoticeKind::TrialEndingSoon,
            _ => return None,
        })
    }

    /// The notice a pre-flight refusal reason calls for, if any. Free plan,
    /// frozen credits and "extra usage off" are not events an email helps
    /// with: nothing was reached.
    pub const fn for_refusal(reason: RefusalReason) -> Option<Self> {
        match reason {
            RefusalReason::MemberCapReached => Some(NoticeKind::MemberCapReached),
            RefusalReason::MonthlyLimitReached => Some(NoticeKind::ExtraUsageLimitReached),
            RefusalReason::ExtraBalanceExhausted => Some(NoticeKind::BalanceDepleted),
            RefusalReason::FreePlan | RefusalReason::CreditsFrozen | RefusalReason::ExtraUsageOff => None,
        }
    }
}

/// SQL for the canonical text form of a period start, used in every dedupe
/// key. Computed in SQL on both the scan side and the refusal side so the two
/// can never format the same instant differently and claim twice.
pub fn period_key_sql(col: &str) -> String {
    format!("to_char({col} AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"')")
}

/// How far back the scan looks for limit/depleted/cap events. Events older
/// than this have either been claimed already or are stale: nobody wants an
/// email about last week's limit.
const SCAN_LOOKBACK: &str = "3 days";

/// A leased notice is invisible to other workers for this long.
const LEASE: &str = "10 minutes";

/// Sends per notice before it is left as `failed` for an operator.
pub const MAX_ATTEMPTS: i32 = 8;

/// Most recipients one notice will email (an admin roster is single digits).
const MAX_RECIPIENTS: i64 = 50;

/// Backoff before attempt `n + 1`, after attempt `n` failed.
pub fn backoff_after(attempt: i32) -> Duration {
    const STEPS_SECS: [u64; 7] = [60, 300, 900, 3_600, 10_800, 21_600, 43_200];
    let i = (attempt.max(1) as usize - 1).min(STEPS_SECS.len() - 1);
    Duration::from_secs(STEPS_SECS[i])
}

// ── Claim ──────────────────────────────────────────────────────────────────

/// Claim a notice by an explicit dedupe key. `true` when this call is the
/// winner (the row did not exist). Used by 3.5 for reload notices
/// (`dedupe_key` = the reload id). The caller's connection must be inside an
/// account-scoped transaction.
pub async fn claim(
    conn: &mut PgConnection,
    account: Uuid,
    kind: NoticeKind,
    dedupe_key: &str,
    period_start: Option<DateTime<Utc>>,
) -> Result<bool, sqlx::Error> {
    let n = sqlx::query(
        "INSERT INTO billing_notices (account_id, notice_kind, dedupe_key, period_start) \
         VALUES ($1, $2, $3, $4) \
         ON CONFLICT (account_id, notice_kind, dedupe_key) DO NOTHING",
    )
    .bind(account)
    .bind(kind.as_str())
    .bind(dedupe_key)
    .bind(period_start)
    .execute(&mut *conn)
    .await?
    .rows_affected();
    Ok(n == 1)
}

/// Claim a period-keyed notice (`user` set for a member cap). The key is built
/// in SQL by [`period_key_sql`].
pub async fn claim_for_period(
    conn: &mut PgConnection,
    account: Uuid,
    kind: NoticeKind,
    user: Option<Uuid>,
    period_start: DateTime<Utc>,
) -> Result<bool, sqlx::Error> {
    let key = period_key_sql("$4::timestamptz");
    let sql = format!(
        "INSERT INTO billing_notices (account_id, notice_kind, dedupe_key, period_start) \
         VALUES ($1, $2, CASE WHEN $3::text IS NULL THEN {key} ELSE 'user:' || $3::text || ':' || {key} END, $4) \
         ON CONFLICT (account_id, notice_kind, dedupe_key) DO NOTHING"
    );
    let n = sqlx::query(&sql)
        .bind(account)
        .bind(kind.as_str())
        .bind(user.map(|u| u.to_string()))
        .bind(period_start)
        .execute(&mut *conn)
        .await?
        .rows_affected();
    Ok(n == 1)
}

// ── Detect: refusal side ───────────────────────────────────────────────────

/// Refusals already turned into a claim attempt recently, so a blocked member
/// retrying does not cost a database write per request. Losing this map (a
/// restart) only costs one extra no-op insert.
type RefusalKey = (Uuid, NoticeKind, Option<Uuid>);
static RECENT_REFUSALS: Mutex<Option<HashMap<RefusalKey, Instant>>> = Mutex::new(None);
const REFUSAL_MEMO: Duration = Duration::from_secs(600);
const REFUSAL_MEMO_CAP: usize = 10_000;

fn first_sighting(key: RefusalKey) -> bool {
    let mut guard = RECENT_REFUSALS.lock().unwrap_or_else(|p| p.into_inner());
    let map = guard.get_or_insert_with(HashMap::new);
    let now = Instant::now();
    if map.len() >= REFUSAL_MEMO_CAP {
        map.retain(|_, at| now.duration_since(*at) < REFUSAL_MEMO);
        if map.len() >= REFUSAL_MEMO_CAP {
            map.clear();
        }
    }
    match map.get(&key) {
        Some(at) if now.duration_since(*at) < REFUSAL_MEMO => false,
        _ => {
            map.insert(key, now);
            true
        }
    }
}

fn forget_sighting(key: &RefusalKey) {
    let mut guard = RECENT_REFUSALS.lock().unwrap_or_else(|p| p.into_inner());
    if let Some(map) = guard.as_mut() {
        map.remove(key);
    }
}

/// A pre-flight gate refused `user` on `account` for `reason`. If that calls
/// for a notice, claim it on a spawned task: the request that was refused
/// does not wait on, or fail because of, the claim.
pub fn notify_refusal(
    control: &PgPool,
    account: Uuid,
    user: Uuid,
    reason: RefusalReason,
) {
    let Some(kind) = NoticeKind::for_refusal(reason) else {
        return;
    };
    let key = (account, kind, (kind == NoticeKind::MemberCapReached).then_some(user));
    if !first_sighting(key) {
        return;
    }
    let control = control.clone();
    tokio::spawn(async move {
        if let Err(e) = claim_refusal(&control, account, user, kind).await {
            forget_sighting(&key);
            tracing::warn!(
                %account, notice_kind = kind.as_str(), error = %e,
                "billing notices: could not claim a refusal notice; the next refusal retries it"
            );
        }
    });
}

/// The claim [`notify_refusal`] spawns; public so tests can await it.
pub async fn claim_refusal(
    control: &PgPool,
    account: Uuid,
    user: Uuid,
    kind: NoticeKind,
) -> Result<bool, sqlx::Error> {
    let mut tx = crate::tenancy::scope::begin_account_scoped(control, account)
        .await
        .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
    if kind == NoticeKind::BalanceDepleted && auto_reload_on(&mut tx, account).await? {
        tx.commit().await?;
        return Ok(false);
    }
    // The same window a settlement debit counts this spend in: the UTC calendar month.
    let (start, _end) = extra_window(&mut tx).await?;
    let won = claim_for_period(
        &mut tx,
        account,
        kind,
        (kind == NoticeKind::MemberCapReached).then_some(user),
        start,
    )
    .await?;
    tx.commit().await?;
    Ok(won)
}

/// Is auto-reload switched on (and so extra usage too) for this account? A
/// depleted balance is then not news worth an email: the reload is coming.
pub async fn auto_reload_on(conn: &mut PgConnection, account: Uuid) -> Result<bool, sqlx::Error> {
    Ok(sqlx::query_scalar::<_, bool>(
        "SELECT enabled AND auto_reload_enabled FROM extra_usage_settings WHERE account_id = $1",
    )
    .bind(account)
    .fetch_optional(&mut *conn)
    .await?
    .unwrap_or(false))
}

// ── Detect: settlement side ────────────────────────────────────────────────

/// Claim the notices the debit path left evidence of, for one account. Reads
/// what settlement already wrote; the debit transaction does nothing for
/// this. Returns how many notices this call claimed.
pub async fn scan_account(conn: &mut PgConnection, account: Uuid) -> Result<u64, sqlx::Error> {
    let p_key = period_key_sql("p.period_start");
    let l_key = period_key_sql("l.period_start");
    let mut claimed = 0;

    // The debit that reached the limit stamped `limit_reached_at`. Only while
    // extra usage is still on: an admin who turned it off has seen the limit.
    claimed += sqlx::query(&format!(
        "INSERT INTO billing_notices (account_id, notice_kind, dedupe_key, period_start) \
         SELECT p.account_id, 'extra_usage_limit_reached', {p_key}, p.period_start \
           FROM extra_usage_periods p \
          WHERE p.account_id = $1 \
            AND p.limit_reached_at > now() - interval '{SCAN_LOOKBACK}' \
            AND EXISTS (SELECT 1 FROM extra_usage_settings s WHERE s.account_id = p.account_id AND s.enabled) \
         ON CONFLICT (account_id, notice_kind, dedupe_key) DO NOTHING"
    ))
    .bind(account)
    .execute(&mut *conn)
    .await?
    .rows_affected();

    // A balance that is zero after spending in the current window. Not while
    // auto-reload is on: the email would tell the admins to add funds while a
    // reload is already on its way (a failed reload has its own notice).
    claimed += sqlx::query(&format!(
        "INSERT INTO billing_notices (account_id, notice_kind, dedupe_key, period_start) \
         SELECT b.account_id, 'balance_depleted', {p_key}, p.period_start \
           FROM extra_usage_balances b \
           JOIN extra_usage_settings s ON s.account_id = b.account_id AND s.enabled \
           JOIN extra_usage_periods p ON p.account_id = b.account_id \
                AND p.period_start <= now() AND p.period_end > now() \
          WHERE b.account_id = $1 AND b.balance_micro_usd = 0 AND p.spent_micro_usd > 0 \
            AND b.updated_at > now() - interval '{SCAN_LOOKBACK}' \
            AND NOT s.auto_reload_enabled \
         ON CONFLICT (account_id, notice_kind, dedupe_key) DO NOTHING"
    ))
    .bind(account)
    .execute(&mut *conn)
    .await?
    .rows_affected();

    // A debit extra usage only partly funded because the member hit their cap
    // records the refusal code in `note` (3.2).
    claimed += sqlx::query(&format!(
        "INSERT INTO billing_notices (account_id, notice_kind, dedupe_key, period_start) \
         SELECT DISTINCT l.account_id, 'member_cap_reached', 'user:' || l.user_id::text || ':' || {l_key}, l.period_start \
           FROM extra_usage_ledger l \
          WHERE l.account_id = $1 AND l.entry_kind = 'debit' AND l.note = 'member_cap_reached' \
            AND l.user_id IS NOT NULL AND l.created_at > now() - interval '{SCAN_LOOKBACK}' \
         ON CONFLICT (account_id, notice_kind, dedupe_key) DO NOTHING"
    ))
    .bind(account)
    .execute(&mut *conn)
    .await?
    .rows_affected();

    Ok(claimed)
}

// ── Deliver ────────────────────────────────────────────────────────────────

/// A notice a worker has leased and now owns until the lease runs out.
#[derive(Clone, Debug)]
pub struct Leased {
    pub id: Uuid,
    pub account_id: Uuid,
    pub kind: NoticeKind,
    pub dedupe_key: String,
    pub period_start: Option<DateTime<Utc>>,
    pub attempt: i32,
    pub delivered_to: Vec<Uuid>,
}

/// Take up to `limit` due notices of `account`. Due means claimed or failed,
/// past `next_attempt_at`, with attempts left. Taking is the lease: the row's
/// `next_attempt_at` moves out so no other worker takes it, and `attempts`
/// counts this attempt (the first lease does not bump the claim's own 1).
pub async fn lease_due(conn: &mut PgConnection, account: Uuid, limit: i64) -> Result<Vec<Leased>, sqlx::Error> {
    #[allow(clippy::type_complexity)]
    let rows: Vec<(Uuid, Uuid, String, String, Option<DateTime<Utc>>, i32, Vec<Uuid>)> = sqlx::query_as(&format!(
        "WITH due AS ( \
             SELECT id FROM billing_notices \
              WHERE account_id = $1 AND state IN ('claimed', 'failed') AND next_attempt_at <= now() \
                AND attempts + CASE WHEN last_attempt_at IS NULL THEN 0 ELSE 1 END <= $3 \
              ORDER BY claimed_at LIMIT $2 \
              FOR UPDATE SKIP LOCKED) \
         UPDATE billing_notices n SET \
                attempts = n.attempts + CASE WHEN n.last_attempt_at IS NULL THEN 0 ELSE 1 END, \
                last_attempt_at = now(), next_attempt_at = now() + interval '{LEASE}', updated_at = now() \
           FROM due WHERE n.id = due.id \
         RETURNING n.id, n.account_id, n.notice_kind, n.dedupe_key, n.period_start, n.attempts, n.delivered_to"
    ))
    .bind(account)
    .bind(limit)
    .bind(MAX_ATTEMPTS)
    .fetch_all(&mut *conn)
    .await?;
    Ok(rows
        .into_iter()
        .filter_map(|(id, account_id, kind, dedupe_key, period_start, attempt, delivered_to)| {
            // The CHECK list and `NoticeKind::parse` are the same list.
            NoticeKind::parse(&kind).map(|kind| Leased {
                id,
                account_id,
                kind,
                dedupe_key,
                period_start,
                attempt,
                delivered_to,
            })
        })
        .collect())
}

/// One person a notice is addressed to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Recipient {
    pub user_id: Uuid,
    pub email: String,
}

/// What kind of account the notice is about.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AccountShape {
    pub is_team_org: bool,
}

pub async fn account_shape(conn: &mut PgConnection, account: Uuid) -> Result<Option<AccountShape>, sqlx::Error> {
    // Same definition as `funding_policy::db::load_extra_shared`: a team org
    // is an org account that is not someone's personal org.
    let row: Option<(bool,)> = sqlx::query_as(
        "SELECT COALESCE(a.is_org_account AND NOT EXISTS( \
                    SELECT 1 FROM personal_orgs po WHERE po.clerk_org_id = a.clerk_org_id), false) \
           FROM accounts a WHERE a.id = $1",
    )
    .bind(account)
    .fetch_optional(&mut *conn)
    .await?;
    Ok(row.map(|(is_team_org,)| AccountShape { is_team_org }))
}

/// The user a member-cap dedupe key (`user:<id>:<period>`) names.
pub fn member_of_key(dedupe_key: &str) -> Option<Uuid> {
    dedupe_key.strip_prefix("user:")?.split(':').next()?.parse().ok()
}

/// Who the notice goes to, and in what voice. See the module's table.
pub async fn recipients(
    conn: &mut PgConnection,
    account: Uuid,
    kind: NoticeKind,
    dedupe_key: &str,
) -> Result<(Audience, Vec<Recipient>), sqlx::Error> {
    let Some(shape) = account_shape(conn, account).await? else {
        return Ok((Audience::OrgAdmin, Vec::new()));
    };
    if kind.is_trial() {
        let rows: Vec<(Uuid, String)> = sqlx::query_as(
            "SELECT u.id, u.email FROM accounts a JOIN users u ON u.id = a.owner_user_id WHERE a.id = $1",
        )
        .bind(account)
        .fetch_all(&mut *conn)
        .await?;
        return Ok((Audience::OrgAdmin, non_blank(rows)));
    }
    if !shape.is_team_org {
        let rows: Vec<(Uuid, String)> = sqlx::query_as(
            "SELECT u.id, u.email FROM accounts a JOIN users u ON u.id = a.owner_user_id WHERE a.id = $1",
        )
        .bind(account)
        .fetch_all(&mut *conn)
        .await?;
        return Ok((Audience::PersonalOwner, non_blank(rows)));
    }
    if kind == NoticeKind::MemberCapReached {
        let Some(member) = member_of_key(dedupe_key) else {
            return Ok((Audience::OrgMember, Vec::new()));
        };
        // Still a member: someone removed since has no use for the email.
        let rows: Vec<(Uuid, String)> = sqlx::query_as(
            "SELECT u.id, u.email FROM account_members am JOIN users u ON u.id = am.user_id \
              WHERE am.account_id = $1 AND am.user_id = $2",
        )
        .bind(account)
        .bind(member)
        .fetch_all(&mut *conn)
        .await?;
        return Ok((Audience::OrgMember, non_blank(rows)));
    }
    let rows: Vec<(Uuid, String)> = sqlx::query_as(
        "SELECT u.id, u.email FROM account_members am JOIN users u ON u.id = am.user_id \
          WHERE am.account_id = $1 AND am.role IN ('owner', 'admin') \
          ORDER BY am.joined_at ASC, u.id ASC LIMIT $2",
    )
    .bind(account)
    .bind(MAX_RECIPIENTS)
    .fetch_all(&mut *conn)
    .await?;
    Ok((Audience::OrgAdmin, non_blank(rows)))
}

fn non_blank(rows: Vec<(Uuid, String)>) -> Vec<Recipient> {
    rows.into_iter()
        .filter(|(_, email)| !email.trim().is_empty())
        .map(|(user_id, email)| Recipient { user_id, email })
        .collect()
}

/// The facts a template needs, read at send time (so the email shows the
/// limit as it is now, not as it was when the notice was claimed).
pub async fn load_facts(
    conn: &mut PgConnection,
    notice: &Leased,
    audience: Audience,
    web_base: &str,
    org_name: Option<String>,
) -> Result<NoticeFacts, sqlx::Error> {
    let resets_at: Option<DateTime<Utc>> = match notice.period_start {
        Some(start) => {
            sqlx::query_scalar(
                "SELECT period_end FROM extra_usage_periods WHERE account_id = $1 AND period_start = $2",
            )
            .bind(notice.account_id)
            .bind(start)
            .fetch_optional(&mut *conn)
            .await?
        }
        None => None,
    };
    let limit_micro_usd: Option<i64> = sqlx::query_scalar(
        "SELECT monthly_limit_micro_usd FROM extra_usage_settings WHERE account_id = $1",
    )
    .bind(notice.account_id)
    .fetch_optional(&mut *conn)
    .await?
    .flatten();
    let reload: Option<(i64, bool)> = match (notice.kind, notice.dedupe_key.parse::<Uuid>()) {
        (NoticeKind::AutoReloadSucceeded | NoticeKind::AutoReloadFailed, Ok(reload)) => {
            sqlx::query_as(
                "SELECT amount_micro_usd, disabled_auto_reload FROM extra_usage_reloads \
                  WHERE id = $1 AND account_id = $2",
            )
            .bind(reload)
            .bind(notice.account_id)
            .fetch_optional(&mut *conn)
            .await?
        }
        _ => None,
    };
    let trial: Option<(i32, Option<DateTime<Utc>>)> = if notice.kind.is_trial() {
        sqlx::query_as("SELECT seats, trial_ends_at FROM org_trials WHERE account_id = $1")
            .bind(notice.account_id)
            .fetch_optional(&mut *conn)
            .await?
    } else {
        None
    };
    let reload_amount_micro_usd = reload.map(|r| r.0);
    let auto_reload_turned_off = reload.is_some_and(|r| r.1);
    // A member cannot open Billing (org pages are admin-only), so the member
    // email links to the page where their own usage is.
    let link = match audience {
        Audience::OrgAdmin => format!("{web_base}/settings/organization/billing"),
        Audience::PersonalOwner => format!("{web_base}/settings/billing"),
        Audience::OrgMember => format!("{web_base}/settings"),
    };
    Ok(NoticeFacts {
        org_name,
        resets_at,
        limit_micro_usd,
        reload_amount_micro_usd,
        auto_reload_turned_off,
        trial_seats: trial.map(|t| t.0),
        trial_ends_at: trial.and_then(|t| t.1),
        link,
    })
}

/// How one delivery pass for one notice ended.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Delivery {
    /// Every recipient has the email.
    Sent { recipients: usize },
    /// Nobody to tell (member left, no address). Closed, not retried.
    NoRecipients,
    /// At least one send failed. The notice is due again after the backoff.
    Retry { error: String },
}

async fn scoped(control: &PgPool, account: Uuid) -> Result<sqlx::Transaction<'static, sqlx::Postgres>, sqlx::Error> {
    crate::tenancy::scope::begin_account_scoped(control, account)
        .await
        .map_err(|e| sqlx::Error::Protocol(e.to_string()))
}

async fn mark_closed_without_recipients(control: &PgPool, n: &Leased) -> Result<(), sqlx::Error> {
    close_as(control, n, "no_recipients").await
}

/// Close a notice that will not be sent, recording why in `error`.
async fn close_as(control: &PgPool, n: &Leased, why: &str) -> Result<(), sqlx::Error> {
    let mut tx = scoped(control, n.account_id).await?;
    sqlx::query(
        "UPDATE billing_notices SET state = 'sent', sent_at = now(), error = $2, updated_at = now() \
          WHERE id = $1",
    )
    .bind(n.id)
    .bind(why)
    .execute(&mut *tx)
    .await?;
    tx.commit().await
}

async fn mark_delivered(control: &PgPool, n: &Leased, user: Uuid) -> Result<(), sqlx::Error> {
    let mut tx = scoped(control, n.account_id).await?;
    sqlx::query(
        "UPDATE billing_notices SET delivered_to = array_append(delivered_to, $2), updated_at = now() \
          WHERE id = $1 AND NOT ($2 = ANY(delivered_to))",
    )
    .bind(n.id)
    .bind(user)
    .execute(&mut *tx)
    .await?;
    tx.commit().await
}

async fn mark_sent(control: &PgPool, n: &Leased, message_id: Option<&str>) -> Result<(), sqlx::Error> {
    let mut tx = scoped(control, n.account_id).await?;
    sqlx::query(
        "UPDATE billing_notices SET state = 'sent', sent_at = now(), error = NULL, \
                provider_message_id = COALESCE($2, provider_message_id), updated_at = now() \
          WHERE id = $1",
    )
    .bind(n.id)
    .bind(message_id)
    .execute(&mut *tx)
    .await?;
    tx.commit().await
}

async fn mark_failed(control: &PgPool, n: &Leased, error: &str, wait: Duration) -> Result<(), sqlx::Error> {
    let mut tx = scoped(control, n.account_id).await?;
    sqlx::query(
        "UPDATE billing_notices SET state = 'failed', error = left($2, 500), \
                next_attempt_at = now() + make_interval(secs => $3), updated_at = now() \
          WHERE id = $1",
    )
    .bind(n.id)
    .bind(error)
    .bind(wait.as_secs_f64())
    .execute(&mut *tx)
    .await?;
    tx.commit().await
}

/// Deliver one leased notice. Safe to call again for the same notice: people
/// already in `delivered_to` are skipped, and each send is idempotent at the
/// provider.
pub async fn deliver(
    control: &PgPool,
    mailer: &Mailer,
    notice: &Leased,
    web_base: &str,
    org_name: Option<String>,
) -> Result<Delivery, sqlx::Error> {
    let (audience, people, facts) = {
        let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(control, notice.account_id)
            .await
            .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
        // Claimed before auto-reload was switched on, still unsent: drop it.
        if notice.kind == NoticeKind::BalanceDepleted && auto_reload_on(&mut tx, notice.account_id).await? {
            tx.rollback().await.ok();
            close_as(control, notice, "suppressed_auto_reload").await?;
            return Ok(Delivery::NoRecipients);
        }
        let (audience, people) = recipients(&mut tx, notice.account_id, notice.kind, &notice.dedupe_key).await?;
        let facts = load_facts(&mut tx, notice, audience, web_base, org_name).await?;
        tx.rollback().await.ok();
        (audience, people, facts)
    };
    if people.is_empty() {
        mark_closed_without_recipients(control, notice).await?;
        return Ok(Delivery::NoRecipients);
    }

    let rendered = email::render(notice.kind, audience, &facts);
    let mut first_error: Option<String> = None;
    let mut last_message_id: Option<String> = None;
    let done = &notice.delivered_to;
    for person in &people {
        if done.contains(&person.user_id) {
            continue;
        }
        let idempotency_key = format!("billing-notice/{}/{}", notice.id, person.user_id);
        match mailer.send(&person.email, &rendered, &idempotency_key).await {
            Ok(message_id) => {
                last_message_id = message_id.or(last_message_id);
                // Recorded now, not at the end: a crash between two sends
                // must not make the retry email the first person again.
                mark_delivered(control, notice, person.user_id).await?;
            }
            Err(e) => {
                first_error.get_or_insert_with(|| e.to_string());
            }
        }
    }

    match first_error {
        None => {
            mark_sent(control, notice, last_message_id.as_deref()).await?;
            Ok(Delivery::Sent { recipients: people.len() })
        }
        Some(error) => {
            mark_failed(control, notice, &error, backoff_after(notice.attempt)).await?;
            if notice.attempt >= MAX_ATTEMPTS {
                tracing::error!(
                    alert = true, notice_id = %notice.id, account_id = %notice.account_id,
                    notice_kind = notice.kind.as_str(), attempts = notice.attempt,
                    "billing notices: a notice failed its last attempt and will not be retried"
                );
            }
            Ok(Delivery::Retry { error })
        }
    }
}

/// A delivery died on a database error rather than a send failure. The lease
/// already counted this attempt (`lease_due` bumps `attempts` on every lease
/// after the first), so the MAX_ATTEMPTS ceiling is reached by these errors
/// too; what was missing was the alert, which lived only on the send-failure
/// path. Raise it here, and best-effort mark the row `failed` with the error so
/// an operator can see it (the database may be the thing that is down).
pub async fn record_delivery_error(control: &PgPool, notice: &Leased, error: &str) {
    if notice.attempt >= MAX_ATTEMPTS {
        tracing::error!(
            alert = true, notice_id = %notice.id, account_id = %notice.account_id,
            notice_kind = notice.kind.as_str(), attempts = notice.attempt, %error,
            "billing notices: a notice failed its last attempt on a database error and will not be retried"
        );
    }
    if let Err(e) = mark_failed(control, notice, &format!("database error: {error}"), backoff_after(notice.attempt)).await {
        tracing::warn!(notice_id = %notice.id, error = %e, "billing notices: could not record a delivery database error");
    }
}

/// What one worker pass over one account did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PassStats {
    pub claimed: u64,
    pub leased: usize,
    pub sent: usize,
    pub retrying: usize,
}

/// Notices delivered per account per pass.
const BATCH: i64 = 20;

/// One worker pass for one account: claim what the debit path left behind,
/// lease what is due, deliver it. `org_name`/`web_base` are resolved lazily by
/// the caller only when something is due.
pub async fn run_account<F, Fut>(
    control: &PgPool,
    mailer: Option<&Mailer>,
    account: Uuid,
    context: F,
) -> Result<PassStats, sqlx::Error>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = (String, Option<String>)>,
{
    let mut stats = PassStats::default();
    let mut tx = crate::tenancy::scope::begin_account_scoped(control, account)
        .await
        .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
    stats.claimed = scan_account(&mut tx, account).await?;
    // Without mail configured, leave everything claimed: nothing is lost, and
    // it goes out once a key is set.
    let due = if mailer.is_some() { lease_due(&mut tx, account, BATCH).await? } else { Vec::new() };
    tx.commit().await?;
    stats.leased = due.len();
    let (Some(mailer), false) = (mailer, due.is_empty()) else {
        return Ok(stats);
    };
    let (web_base, org_name) = context().await;
    for notice in &due {
        match deliver(control, mailer, notice, &web_base, org_name.clone()).await {
            Ok(Delivery::Sent { .. }) | Ok(Delivery::NoRecipients) => stats.sent += 1,
            Ok(Delivery::Retry { error }) => {
                stats.retrying += 1;
                tracing::warn!(
                    notice_id = %notice.id, %account, notice_kind = notice.kind.as_str(),
                    attempt = notice.attempt, %error,
                    "billing notices: send failed; it will be retried after a backoff"
                );
            }
            Err(e) => {
                // The lease keeps the notice from being retried until it
                // runs out, then it is due again.
                tracing::warn!(
                    notice_id = %notice.id, %account, error = %e,
                    "billing notices: delivery bookkeeping failed; the lease will expire and retry"
                );
                record_delivery_error(control, notice, &e.to_string()).await;
            }
        }
    }
    Ok(stats)
}
