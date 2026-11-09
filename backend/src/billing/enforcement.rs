//! Balance-enforcement flag (usage-metering PR10,
//! `.scratch/usage-metering/plan.md` §1 PR10 / §2 item 6), plus (real-pricing-
//! and-limits PR) the first REAL gate built on top of it.
//!
//! # What this is, precisely
//!
//! An `account_prefs` JSONB key that says "would this account's zero balance
//! actually block something, if blocking were implemented" — **control-only,
//! zero migrations** (`account_prefs` already exists, migration
//! `0029_account_prefs.sql`), layered under a process-wide env kill switch.
//!
//! **[`check_and_record`] never blocks anything — that has not changed.** It
//! is called from "the debit path" ([`crate::workers::billing_derivation`]'s
//! `write_account_ledger` call site) purely to OBSERVE and RECORD: it writes
//! an [`crate::workers::billing_derivation`]-adjacent `enforcement_events`
//! row when an account's balance is at or below zero, so an operator can see
//! "how many accounts would this affect" — but the write it debits/finalizes
//! always proceeds regardless of what this function returns. See this crate's
//! `admission::AdmissionConfig` for the exact same observe-then-decide
//! rollout shape (`rate_enforce`/`concurrency_enforce`, both `false` by
//! default, counted but never enforced until a human flips them after
//! reading a week of real data).
//!
//! **[`is_blocked`] DOES block — this is the real gate.** Request paths now
//! reach it through [`interactive_admission`], which refuses when eligibility
//! cannot be read (COR-108); the history below is otherwise unchanged. Added by the
//! real-pricing-and-limits PR once a real self-serve tier ladder (migration
//! `0155_billing_plans_pricing.sql`) made "an account can run out of
//! credits" a real, actionable event rather than a purely observational one.
//! Unlike `check_and_record`, which is wired into the debit path AFTER an
//! LLM call has already been made and billed, `is_blocked` is wired into the
//! REQUEST path — `routes::queries::create_query`/`create_query_stream` and
//! the MCP `ask_ocean`/`search_atoms` tools (via `interactive_credit_gate`)
//! — and actually refuses the request
//! when it returns `true`, BEFORE any embedding/retrieval/synthesis spend
//! happens. It reuses [`kill_switch_active`] and [`enforcement_enabled`]
//! rather than re-implementing that logic: the kill switch and the
//! per-account preference now govern something real, not just what
//! `enforcement_events` records. Writes/captures are deliberately NOT gated
//! by this function — see [`is_blocked`]'s own doc for why only the
//! expensive LLM read paths (ask, search) are in scope for this PR.
//!
//! # Default-OFF by construction
//!
//! A DB read failure reads as OFF (`false`) — never as some default value
//! this module writes down anywhere else. This flag fails *open* (treats a
//! DB error as "not enabled") because OFF is unambiguously the safe
//! direction here: a transient read fault is not meaningfully different
//! from a missing/false column for a flag that, even fully ON, gates a
//! request rejection rather than data exposure.
//!
//! # Not a user preference (enforcement-rollout PR)
//!
//! This flag used to live at `account_prefs.prefs->>'billing_enforcement_
//! enabled'` — a plain JSONB blob any authenticated account holder can
//! wholesale-overwrite via `PUT /v1/prefs` (that handler replaces the whole
//! object; it does not merge — see its own doc comment). A billing-security
//! switch has no business living somewhere the account it constrains can
//! read or write. Migration `0164_accounts_billing_enforcement_enabled.sql`
//! moved it to a real `accounts.billing_enforcement_enabled` column that
//! only server-side billing code (this module's own callers, plus
//! [`crate::clerk_provision`] at signup) ever writes — there is no HTTP
//! route that lets a client set it.
//!
//! # The kill switch
//!
//! [`kill_switch_active`] (env [`KILL_SWITCH_ENV`]) is checked FIRST and, if
//! set, forces [`enforcement_enabled`] to `false` for every account
//! regardless of what any individual `account_prefs` row says — "layer a
//! process-wide env kill switch over it" (plan §1 PR10), so enforcement
//! recording can be killed globally in one env var flip without touching
//! per-account state. Same truthy-parse discipline as `admission::env_flag`:
//! only `1`/`true` (trimmed) enable anything; every other value, including
//! unset, is the safe (OFF) reading.

use sqlx::types::Decimal;
use sqlx::PgPool;
use uuid::Uuid;

use super::funding_policy::{
    db::{evaluate, Mode, PeriodSel},
    FundingOutcome, Intent, RefusalReason,
};

/// Process-wide kill switch. When set to `1`/`true`, [`enforcement_enabled`]
/// returns `false` for EVERY account, overriding its own
/// `accounts.billing_enforcement_enabled` column. See the module doc's "The
/// kill switch" section.
pub const KILL_SWITCH_ENV: &str = "BILLING_ENFORCEMENT_KILL_SWITCH";

/// `1`/`true` (trimmed) enable; anything else — unset, empty, a typo — is
/// the safe (inactive) reading. Mirrors `admission::env_flag` exactly.
fn kill_switch_active() -> bool {
    matches!(
        std::env::var(KILL_SWITCH_ENV).ok().as_deref().map(str::trim),
        Some("1") | Some("true")
    )
}

/// Whether the per-account enforcement preference is ON for `account_id`,
/// after the kill switch has had first say.
///
/// **This being `true` does not block anything in this PR** — see the
/// module doc. It only changes what [`check_and_record`] writes into
/// `would_have_blocked`.
///
/// `pool` must always be the CONTROL/consumer pool — `accounts` has no row
/// on any tenant database. Returns `false` when there is no pool (in
/// practice every call site here already holds a control pool by
/// construction — see [`check_and_record`]).
pub async fn enforcement_enabled(pool: Option<&PgPool>, account_id: Uuid) -> bool {
    let Some(pool) = pool else {
        return false;
    };
    match enforcement_enabled_checked(pool, account_id).await {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(
                error = %e, account_id = %account_id,
                "billing enforcement: accounts.billing_enforcement_enabled read failed — reading \
                 as OFF (fail-open; see module doc)"
            );
            false
        }
    }
}

/// [`enforcement_enabled`] without its fail-open mapping: a failed read is an
/// error, not OFF. The kill switch still wins before any read. A missing
/// `accounts` row reads as OFF, as it always has.
async fn enforcement_enabled_checked(pool: &PgPool, account_id: Uuid) -> Result<bool, sqlx::Error> {
    if kill_switch_active() {
        return Ok(false);
    }
    // The column is NOT NULL, so a present row's value is the whole answer.
    Ok(sqlx::query_scalar::<_, bool>(
        "SELECT billing_enforcement_enabled FROM accounts WHERE id = $1",
    )
    .bind(account_id)
    .fetch_optional(pool)
    .await?
    .unwrap_or(false))
}

/// Result of one [`check_and_record`] call — `None` when there was nothing
/// to evaluate (no `credit_balances` row yet, or a positive balance).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EnforcementCheck {
    /// The account's materialized balance at observation time (`<= 0`,
    /// otherwise this struct would not have been constructed).
    pub balance: Decimal,
    /// Whether real enforcement — if it existed — would have blocked this
    /// account right now. Purely observational in this PR; see the module
    /// doc.
    pub would_have_blocked: bool,
}

/// The one entry point "the debit path" (`billing_derivation::tick`) calls
/// after writing a debit. Reads `credit_balances.balance` for `account_id`;
/// if it is `<= 0`, records one `enforcement_events` row (control-plane,
/// migration `0150`) carrying the CURRENT `would_have_blocked` reading —
/// written **regardless of whether enforcement is enabled**, so an operator
/// gets a true count of "accounts that would be affected right now" even
/// while every account's flag is off. A positive balance, or no
/// `credit_balances` row at all (an account that has never been debited or
/// granted — nothing to evaluate yet), writes nothing and returns `Ok(None)`.
///
/// **Never blocks.** There is no code path in this function, or in any of
/// its callers, that turns a `would_have_blocked: true` result into a
/// rejected operation. A caller that failed because of this function would
/// be a bug — see `workers::billing_derivation::pg_tests::
/// enforcement_flag_on_records_would_have_blocked_but_never_blocks_the_debit`
/// for the negative control this rests on.
///
/// Errors are returned (not swallowed) so the caller can decide how to log
/// them — `billing_derivation::tick` treats this as best-effort exactly like
/// its own unattributed-jobs count: a failure to write an OBSERVABILITY row
/// must never fail, retry, or roll back the debit tick that already
/// committed.
pub async fn check_and_record(
    control: &PgPool,
    account_id: Uuid,
    reason: &str,
) -> Result<Option<EnforcementCheck>, sqlx::Error> {
    // Scoped, for the same reason the INSERT below is: `credit_balances` is
    // RLS-covered, and an unscoped read raises `invalid input syntax for type
    // uuid: ""` rather than returning no rows — see [`read_balance_scoped`].
    let balance: Option<Decimal> = read_balance_scoped(control, account_id).await?;

    let Some(balance) = balance else {
        // No credit_balances row yet — this account has never been debited
        // or granted. Nothing to observe; not the same thing as "zero".
        return Ok(None);
    };
    if balance > Decimal::ZERO {
        return Ok(None);
    }

    let would_have_blocked = enforcement_enabled(Some(control), account_id).await;

    // account_isolation-scoped (0150) — `enforcement_events` carries the
    // same policy shape as `credit_ledger`/`credit_balances`/`grants`.
    let mut tx = crate::tenancy::scope::begin_account_scoped(control, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
    sqlx::query(
        "INSERT INTO enforcement_events (account_id, would_have_blocked, reason) VALUES ($1, $2, $3)",
    )
    .bind(account_id)
    .bind(would_have_blocked)
    .bind(reason)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;

    Ok(Some(EnforcementCheck { balance, would_have_blocked }))
}

/// Which spend a block decision is for. See [`is_blocked`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Spend {
    /// Ask, search and queries: managed (background) charges are left out.
    Interactive,
    /// Managed background work: every charge counts.
    Background,
}

/// Returns whether this member has exhausted their allowance and available pool.
/// Interactive spend honors the legacy account rollout and global billing switch,
/// and adds managed debits back before evaluating credits. Background spend is
/// enforced for every account; ManagedGate owns its operator override.
/// Missing balances and failed reads retain the existing fail-open policy.
/// Managed admission uses [`check_blocked`] to distinguish unavailable reads.
/// Interactive request paths use [`interactive_admission`], which fails closed.
pub async fn is_blocked(control: &PgPool, account_id: Uuid, user_id: Uuid, spend: Spend) -> bool {
    match check_blocked(control, account_id, user_id, spend).await {
        Ok(blocked) => blocked,
        Err(e) => {
            tracing::warn!(
                error = %e, account_id = %account_id, user_id = %user_id,
                "billing enforcement: eligibility unavailable; preserving interactive fail-open policy"
            );
            false
        }
    }
}

/// Why an interactive Ask or search was refused before it could spend.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InteractiveRefusal {
    /// The member's allowance and the account's pool are both used up, and
    /// extra usage cannot fund the spend. The reason names the gate that
    /// stopped it (`billing::funding_policy::RefusalReason`) so the client can
    /// show the right paywall.
    OutOfCredits(RefusalReason),
    /// Credit eligibility could not be read, so we cannot show the caller is
    /// funded. Retryable. This is not reported as "out of credits".
    EligibilityUnavailable,
}

/// The one credit gate for interactive reads: HTTP JSON and SSE Ask
/// (`POST /v1/queries`, `/v1/queries/stream`) and MCP `ask_ocean` and
/// `search_atoms`. Every surface calls this before it resolves scope, makes a
/// task or conversation, embeds the query or calls a model.
///
/// - No control pool: allowed, as there is no billing to read.
/// - The kill switch, enforcement OFF for the account, or a missing balance
///   row: allowed, as before.
/// - Eligibility read failed: refused (fail closed, COR-108). Ask spends model,
///   Jev and embedding money, so an unreadable balance does not count as a
///   funded one. The kill switch is checked before any read, so operators can
///   still open the gate during an incident.
pub async fn interactive_admission(
    control: Option<&PgPool>,
    account_id: Uuid,
    user_id: Uuid,
) -> Result<(), InteractiveRefusal> {
    let Some(control) = control else {
        return Ok(());
    };
    match check_funding_noticing(control, account_id, user_id, Spend::Interactive).await {
        Ok(None) => Ok(()),
        Ok(Some(reason)) => Err(InteractiveRefusal::OutOfCredits(reason)),
        Err(e) => {
            tracing::warn!(
                error = %e, account_id = %account_id, user_id = %user_id,
                "billing enforcement: interactive credit eligibility unavailable; refusing"
            );
            Err(InteractiveRefusal::EligibilityUnavailable)
        }
    }
}

/// Evaluates the funding policy without treating failed eligibility reads as
/// available credits. Background callers must propagate errors before
/// reserving or sending. Missing billing rows retain the existing allowed
/// policy.
///
/// `true` means the spend is refused; [`check_funding`] says why.
pub async fn check_blocked(
    control: &PgPool,
    account_id: Uuid,
    user_id: Uuid,
    spend: Spend,
) -> Result<bool, sqlx::Error> {
    Ok(check_funding_noticing(control, account_id, user_id, spend).await?.is_some())
}

/// The pre-flight form of [`super::funding_policy::FundingPolicy::decide`]:
/// `None` when the next unit of spend can be funded, else the reason it
/// cannot. This is the one pre-flight gate; [`check_blocked`] and
/// [`interactive_admission`] are views of it.
///
/// The order is the plan allowance first (the member's own seat allowance,
/// then the shared pool bounded by the member's pool cap, which is closed
/// while credits are frozen), then extra usage. See the funding policy module
/// for the extra-usage gates and `funding_policy::db` for the reads.
///
/// One account-scoped transaction covers every read.
pub async fn check_funding(
    control: &PgPool,
    account_id: Uuid,
    user_id: Uuid,
    spend: Spend,
) -> Result<Option<RefusalReason>, sqlx::Error> {
    Ok(funding_verdict(control, account_id, user_id, spend).await?)
}

/// [`check_funding`] for the gates that actually refuse work (interactive
/// admission, [`check_blocked`]): when the refusal is a cap, limit or balance
/// one, it also queues the matching billing notice (Settings launch plan 3.6)
/// on a spawned task. [`check_funding`] itself stays a pure read for status
/// views such as `GET /v1/billing/me`, which must never send mail by being
/// looked at.
async fn check_funding_noticing(
    control: &PgPool,
    account_id: Uuid,
    user_id: Uuid,
    spend: Spend,
) -> Result<Option<RefusalReason>, sqlx::Error> {
    let refusal = funding_verdict(control, account_id, user_id, spend).await?;
    if let Some(reason) = refusal {
        super::notices::notify_refusal(control, account_id, user_id, reason);
    }
    Ok(refusal)
}

async fn funding_verdict(
    control: &PgPool,
    account_id: Uuid,
    user_id: Uuid,
    spend: Spend,
) -> Result<Option<RefusalReason>, sqlx::Error> {
    if spend == Spend::Interactive && !enforcement_enabled_checked(control, account_id).await? {
        return Ok(None);
    }
    let mut tx = crate::tenancy::scope::begin_account_scoped(control, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
    let evaluation = evaluate(
        &mut tx,
        account_id,
        user_id,
        Mode::Preflight { interactive: spend == Spend::Interactive },
        PeriodSel::Current,
        Intent::Probe,
    )
    .await?;
    tx.commit().await?;
    Ok(evaluation.outcome.refusal())
}

/// Whether a `stripe_subscriptions.status` value reads as FROZEN — the one
/// definition [`subscription_frozen`] and `billing::renewal`'s mid-period
/// seed guard both defer to, so the two call sites cannot silently drift on
/// what "frozen" means (billing self-serve PR D review round, finding on
/// `enforcement.rs:450`: they were previously two independent copies of the
/// identical `matches!`).
///
/// **A two-value denylist over a CLOSED three-value domain — not a gap.**
/// `stripe_subscriptions.status` carries a DB-level CHECK (`0162`: `status
/// IN ('active', 'past_due', 'canceled')`), and `routes::stripe_webhook::
/// map_subscription_status` collapses every raw Stripe status this
/// deployment can ever observe — `"unpaid"`, `"incomplete"`,
/// `"incomplete_expired"`, `"paused"`, and any future value, via its own
/// catch-all arm — onto `"past_due"` BEFORE a write reaches this column (see
/// that function's own doc comment and its
/// `map_subscription_status_lands_unrecognized_statuses_on_past_due` test,
/// which already asserts `"unpaid"`/`"incomplete_expired"` land on
/// `"past_due"`). `"canceled"` is written only by `apply_subscription_
/// deleted`. So `Some("past_due") | Some("canceled")` is exhaustive over
/// every value this column can ever actually hold — a dunning-exhausted
/// `"unpaid"` subscription cannot reach here still spelled `"unpaid"`, it
/// arrives already remapped. Rewriting this as an allowlist (`None`,
/// `"active"`, `"trialing"` unfrozen; everything else frozen) would read as
/// more defensive but is not: `stripe_subscriptions.status` never stores
/// `"trialing"` either (`map_subscription_status` folds it into
/// `"active"`), so an allowlist naming it would be dead code testing a
/// string this column's own CHECK constraint forbids.
///
/// `None` (no `stripe_subscriptions` row at all) reads as NOT frozen —
/// mirrors this module's own "missing state is not an ON state" convention
/// ([`enforcement_enabled`]'s `Ok(None) => false`,
/// [`read_balance_scoped`]'s `None` balance): an account that has never gone
/// through Stripe checkout (a `free`-tier personal account, a hand-granted
/// `founder_comp`) has nothing to be frozen FROM, so this check must not
/// invent a blocked state for it. There is no third "resubscribe me" signal
/// to read separately: a resubscribe runs a fresh checkout, which flips this
/// same row back to `"active"` the same way `apply_invoice_paid` already
/// restores an involuntary `past_due` lapse.
pub(crate) fn status_is_frozen(status: Option<&str>) -> bool {
    matches!(status, Some("past_due") | Some("canceled"))
}

#[derive(Debug, sqlx::FromRow)]
pub(crate) struct ManagedFunding {
    pub member_period: Option<Uuid>,
    pub basis: String,
    /// Credits of the bound that extra usage funds (`basis == "extra"`).
    pub extra_credits: Option<Decimal>,
}

pub(crate) enum ManagedAllocation {
    Current,
    Pinned(Option<Uuid>),
}

/// Reservation-time funding: [`super::funding_policy::FundingPolicy::decide`]
/// with [`Intent::Admit`] over the bound. `None`: not fundable (the reason is
/// logged). The caller holds the account's financial lock.
///
/// Basis: `allowance` while the bound fits the member's remaining allowance,
/// `pool` when it spills into the shared pool, `extra` when extra usage must
/// fund part of it. An `extra` reservation holds only its extra share
/// (`extra_credits`) against extra usage; the rest is held on the allowance.
pub(crate) async fn managed_funding_tx(
    conn: &mut sqlx::PgConnection,
    account: Uuid,
    payer: Uuid,
    bound: Decimal,
    allocation: ManagedAllocation,
) -> Result<Option<ManagedFunding>, sqlx::Error> {
    let sel = match allocation {
        ManagedAllocation::Pinned(id) => PeriodSel::Pinned(id),
        ManagedAllocation::Current => PeriodSel::Current,
    };
    let evaluation = evaluate(conn, account, payer, Mode::Admission, sel, Intent::Admit(bound)).await?;
    if evaluation.pinned_period_missing {
        return Ok(None);
    }
    let period = evaluation.period.as_ref();
    match evaluation.outcome {
        FundingOutcome::Funded { extra, .. } if extra.micro_usd > 0 => Ok(Some(ManagedFunding {
            member_period: period.map(|p| p.id),
            basis: "extra".into(),
            extra_credits: Some(extra.credits),
        })),
        FundingOutcome::Funded { .. } => {
            let fits_allowance = period.is_some_and(|p| {
                p.member
                    .consumed
                    .checked_add(p.member.held)
                    .and_then(|v| v.checked_add(bound))
                    .is_some_and(|projected| projected <= p.member.granted)
            });
            Ok(Some(ManagedFunding {
                member_period: period.map(|p| p.id),
                basis: if fits_allowance { "allowance" } else { "pool" }.into(),
                extra_credits: None,
            }))
        }
        FundingOutcome::Refused(reason) => {
            tracing::info!(
                account_id = %account, payer_id = %payer, reason = reason.code(),
                "managed funding refused"
            );
            Ok(None)
        }
        FundingOutcome::Partial { .. } => Ok(None),
    }
}

/// Whether this account's Stripe-linked pool is currently FROZEN — the
/// `stripe_subscriptions.status` half of freeze-not-forfeit (plan §1.11).
/// See [`status_is_frozen`] for what "frozen" means and why that two-value
/// denylist is complete over this column's closed domain.
///
/// **Fails OPEN**, same direction and justification as every other read in
/// this module: a lookup failure must never take down asks/search.
///
/// `pub(crate)`, not private (billing self-serve PR D, finding 7, review
/// round): `routes::checkout::create_pack_checkout` reuses this EXACT
/// predicate to refuse selling a frozen account credits it cannot yet spend,
/// rather than re-typing the `stripe_subscriptions.status` read as a second
/// copy that could drift from the read-side gate it is supposed to mirror —
/// the same "one place this logic lives" discipline this whole module's
/// sibling functions already follow (see `org_pool_exhausted` (now `funding_policy::db`)'s own doc
/// comment on why it is extracted rather than duplicated for `is_blocked`'s
/// two call sites).
pub(crate) async fn subscription_frozen(control: &PgPool, account_id: Uuid) -> bool {
    let mut tx = match crate::tenancy::scope::begin_account_scoped(control, account_id).await {
        Ok(tx) => tx,
        Err(e) => {
            tracing::warn!(
                error = %e, account_id = %account_id,
                "billing enforcement: stripe_subscriptions scope failed — reading as NOT FROZEN \
                 (fail-open; a billing lookup failure must never take down asks/search)"
            );
            return false;
        }
    };
    let status: Option<String> =
        match sqlx::query_scalar("SELECT status FROM stripe_subscriptions WHERE account_id = $1")
            .bind(account_id)
            .fetch_optional(&mut *tx)
            .await
        {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(
                    error = %e, account_id = %account_id,
                    "billing enforcement: stripe_subscriptions read failed — reading as NOT FROZEN \
                     (fail-open; a billing lookup failure must never take down asks/search)"
                );
                return false;
            }
        };
    // Read-only — commit rather than let it implicitly roll back on drop,
    // matching this module's other scoped-read call sites.
    if let Err(e) = tx.commit().await {
        tracing::warn!(
            error = %e, account_id = %account_id,
            "billing enforcement: stripe_subscriptions read commit failed — reading as NOT FROZEN"
        );
        return false;
    }
    status_is_frozen(status.as_deref())
}

/// `SELECT balance FROM credit_balances` inside an account-scoped transaction.
///
/// # Why the scope is mandatory, not hygiene
///
/// `credit_balances` carries the `account_isolation` RLS policy
/// `USING (account_id = current_setting('app.account_id')::uuid)` (migration
/// `0150`). On a connection that has not been through
/// [`crate::tenancy::scope::begin_account_scoped`], `app.account_id` is the
/// empty string, and `''::uuid` raises `invalid input syntax for type uuid:
/// ""`. Postgres evaluates that cast at executor init to build the
/// primary-key index condition, so the error fires **even against an empty
/// table** — an unscoped read does not quietly return `None`, it errors.
///
/// That distinction is the whole point here. Every caller in this module fails
/// OPEN on `Err`, so an unscoped read would be caught, logged at `warn`, and
/// turned into "not blocked" — for every account, forever. The gate would be
/// switched on and enforce nothing, which is precisely the failure mode this
/// build has been warned about twice. Getting the scope right is what makes
/// the fail-open path mean "the database is sick" rather than "we asked wrong".
///
/// The same defect shipped in `workers::billing_derivation::load_cursor` and
/// kept `credit_ledger` empty in production for the worker's entire life; see
/// that function's doc for the measured impact.
async fn read_balance_scoped(
    control: &PgPool,
    account_id: Uuid,
) -> Result<Option<Decimal>, sqlx::Error> {
    let mut tx = crate::tenancy::scope::begin_account_scoped(control, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
    let balance: Option<Decimal> =
        sqlx::query_scalar("SELECT balance FROM credit_balances WHERE account_id = $1")
            .bind(account_id)
            .fetch_optional(&mut *tx)
            .await?;
    tx.commit().await?;
    Ok(balance)
}

#[cfg(test)]
/// Serialises every test that mutates [`KILL_SWITCH_ENV`].
///
/// The kill switch is read from a PROCESS-WIDE env var, and `cargo test`
/// runs tests in parallel THREADS of one process — so two tests setting
/// and removing the same variable race, and the loser reads a value the
/// other one just cleared. Observed failing 4 runs out of 5 locally
/// (`value "1": left: false, right: true`). It went unnoticed because the
/// CI step for this module passes `--test-threads=1`, which serialises
/// them by accident rather than by design; a plain `cargo test` does not.
///
/// `std::env::set_var` is process-global and cannot be scoped to a thread,
/// so a lock is the fix — not a differently-named variable per test.
static KILL_SWITCH_ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Take the env lock, tolerating a previous test's panic (a poisoned lock
/// still hands back the guard — the data is `()`, so there is nothing to
/// be corrupted).
#[cfg(test)]
fn lock_kill_switch_env() -> std::sync::MutexGuard<'static, ()> {
    KILL_SWITCH_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

// ── Pure unit tests (no Postgres) ────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kill_switch_only_explicit_truthy_values_activate() {
        let _env = lock_kill_switch_env();
        for (val, expected) in [
            ("1", true),
            ("true", true),
            (" true ", true),
            ("0", false),
            ("false", false),
            ("yes", false),
            ("TRUE", false),
            ("", false),
        ] {
            std::env::set_var(KILL_SWITCH_ENV, val);
            assert_eq!(kill_switch_active(), expected, "value {val:?}");
        }
        std::env::remove_var(KILL_SWITCH_ENV);
    }

    #[test]
    fn kill_switch_defaults_inactive_when_unset() {
        let _env = super::lock_kill_switch_env();
        std::env::remove_var(KILL_SWITCH_ENV);
        assert!(!kill_switch_active());
    }

    #[tokio::test]
    async fn enforcement_enabled_is_false_with_no_pool() {
        let _env = super::lock_kill_switch_env();
        std::env::remove_var(KILL_SWITCH_ENV);
        assert!(!enforcement_enabled(None, Uuid::new_v4()).await);
    }

    #[tokio::test]
    async fn kill_switch_forces_false_even_with_no_pool_consulted() {
        let _env = lock_kill_switch_env();
        // The kill switch must short-circuit BEFORE any DB read is even
        // attempted — proven here by never providing a pool at all and
        // still getting `false` (which `None` alone would also produce, but
        // the ordering matters: see the pg_tests below for a pool-present
        // proof that the switch wins over a `true` account_prefs row).
        std::env::set_var(KILL_SWITCH_ENV, "true");
        assert!(!enforcement_enabled(None, Uuid::new_v4()).await);
        std::env::remove_var(KILL_SWITCH_ENV);
    }

    /// A pool whose every acquire fails: nothing listens on port 1.
    fn unreadable_control_pool() -> PgPool {
        sqlx::postgres::PgPoolOptions::new()
            .acquire_timeout(std::time::Duration::from_secs(2))
            .connect_lazy("postgres://unused@127.0.0.1:1/unused")
            .unwrap()
    }

    /// COR-108: an interactive Ask/search whose credit eligibility cannot be
    /// read is refused as unavailable, not admitted as funded and not
    /// reported as out of credits. The legacy `is_blocked` keeps its
    /// fail-open answer for the callers that still use it.
    #[tokio::test]
    async fn interactive_admission_fails_closed_when_eligibility_is_unreadable() {
        let _env = lock_kill_switch_env();
        std::env::remove_var(KILL_SWITCH_ENV);
        let pool = unreadable_control_pool();
        let (account, user) = (Uuid::new_v4(), Uuid::new_v4());
        assert_eq!(
            interactive_admission(Some(&pool), account, user).await,
            Err(InteractiveRefusal::EligibilityUnavailable)
        );
        assert!(!is_blocked(&pool, account, user, Spend::Interactive).await);
    }

    /// The kill switch opens the interactive gate before any read, so it
    /// stays the operator override when eligibility reads are failing.
    #[tokio::test]
    async fn interactive_admission_kill_switch_opens_before_any_read() {
        let _env = lock_kill_switch_env();
        std::env::set_var(KILL_SWITCH_ENV, "true");
        let pool = unreadable_control_pool();
        let admitted = interactive_admission(Some(&pool), Uuid::new_v4(), Uuid::new_v4()).await;
        std::env::remove_var(KILL_SWITCH_ENV);
        assert_eq!(admitted, Ok(()));
    }

    /// No control pool, nothing to gate.
    #[tokio::test]
    async fn interactive_admission_allows_without_a_control_pool() {
        assert_eq!(interactive_admission(None, Uuid::new_v4(), Uuid::new_v4()).await, Ok(()));
    }
}

// ── Live-Postgres tests ──────────────────────────────────────────────────
//
// Gated on `RELINFER_TEST_DATABASE_URL` — same convention as
// `workers::billing_derivation::pg_tests` (see that module for the
// precedent). `cargo test --lib` with no env var set SKIPS these with an
// `eprintln!` marker rather than failing.
#[cfg(test)]
mod pg_tests {
    use super::*;

    async fn execute_financial_fixture<'q>(
        pool: &PgPool,
        account_id: Uuid,
        query: sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
    ) -> Result<sqlx::postgres::PgQueryResult, sqlx::Error> {
        let mut tx = crate::tenancy::scope::begin_account_scoped(pool, account_id)
            .await
            .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
        crate::billing::renewal::lock_period_rotation(&mut tx, account_id).await?;
        match query.execute(&mut *tx).await {
            Ok(result) => {
                tx.commit().await?;
                Ok(result)
            }
            Err(error) => {
                tx.rollback().await?;
                Err(error)
            }
        }
    }

    async fn pg_test_pool(test_name: &str) -> Option<PgPool> {
        let url = match std::env::var("RELINFER_TEST_DATABASE_URL") {
            Ok(u) => u,
            Err(_) => {
                eprintln!("skipping {test_name}: set RELINFER_TEST_DATABASE_URL to run it");
                return None;
            }
        };
        Some(
            sqlx::postgres::PgPoolOptions::new()
                .max_connections(4)
                .connect(&url)
                .await
                .expect("connect to test Postgres"),
        )
    }

    async fn seed_account(pool: &PgPool) -> Uuid {
        let user_id = Uuid::new_v4();
        let account_id = Uuid::new_v4();
        sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
            .bind(user_id)
            .bind(format!("{user_id}@enforcement-pr10.test"))
            .execute(pool)
            .await
            .expect("insert user");
        sqlx::query("INSERT INTO accounts (id, owner_user_id) VALUES ($1, $2)")
            .bind(account_id)
            .bind(user_id)
            .execute(pool)
            .await
            .expect("insert account");
        account_id
    }

    async fn set_balance(pool: &PgPool, account_id: Uuid, balance: Decimal) {
        execute_financial_fixture(
            pool,
            account_id,
        sqlx::query(
            "INSERT INTO credit_balances (account_id, balance) VALUES ($1, $2) \
             ON CONFLICT (account_id) DO UPDATE SET balance = EXCLUDED.balance",
        )
        .bind(account_id)
        .bind(balance),
        )
        .await
        .expect("seed balance");
    }

    /// An ORG account plus one member holding `tier_code` (`"standard"` or
    /// `"premium"`), behind a REAL seat-priced `grants` row (`team_standard`)
    /// — the seat-tier counterpart to [`seed_account`]'s personal-account
    /// shape.
    ///
    /// **Billing self-serve launch PR E; rewritten in review round 2
    /// (findings 2 & 6):** [`is_blocked`]'s cap-default resolution now reads
    /// the ACCOUNT's current grant's `plan_is_seat_priced` — NOT the
    /// member's own `account_members.seat_tier_id` — see
    /// `read_member_period_scoped`'s own doc comment for why the member-row
    /// signal was wrong in two directions (a removed member's row vanishing
    /// widened their cap; a flat-plan org member's trigger-assigned
    /// `seat_tier_id` narrowed theirs). This helper therefore seeds a real
    /// `grants` row too, not just `account_members` — a bare seat-tier
    /// member row is no longer enough to exercise the 1x default on its own.
    /// [`seed_account`] alone still is not enough either: it creates a
    /// PERSONAL account (`clerk_org_id IS NULL`) with no `grants` row and no
    /// `account_members` row for the caller's `user_id` at all, which
    /// resolves to the UNLIMITED default. Any test whose whole point is the
    /// 1x seat-priced default must go through this helper instead.
    async fn seed_org_member(pool: &PgPool, tier_code: &str) -> (Uuid, Uuid) {
        let user_id = Uuid::new_v4();
        let account_id = Uuid::new_v4();
        sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
            .bind(user_id)
            .bind(format!("{user_id}@enforcement-pr10.test"))
            .execute(pool)
            .await
            .expect("insert user");
        // `clerk_org_id` set (any unique value) is what flips
        // `accounts.is_org_account` true via `0169`'s sync trigger — the fact
        // `account_members_seat_tier_iff_org` keys off.
        sqlx::query("INSERT INTO accounts (id, owner_user_id, clerk_org_id) VALUES ($1, $2, $3)")
            .bind(account_id)
            .bind(user_id)
            .bind(format!("org_{}", account_id.simple()))
            .execute(pool)
            .await
            .expect("insert org account");
        let seat_tier_id: Uuid =
            sqlx::query_scalar("SELECT id FROM seat_tiers WHERE code = $1 AND is_active = true")
                .bind(tier_code)
                .fetch_one(pool)
                .await
                .unwrap_or_else(|e| panic!("seat_tiers {tier_code:?} must be seeded (0165): {e}"));
        sqlx::query(
            "INSERT INTO account_members (account_id, user_id, role, seat_tier_id) \
             VALUES ($1, $2, 'owner', $3)",
        )
        .bind(account_id)
        .bind(user_id)
        .bind(seat_tier_id)
        .execute(pool)
        .await
        .expect("insert account_members row");
        seed_seat_priced_grant(pool, account_id, "team_standard").await;
        (account_id, user_id)
    }

    /// Inserts a `grants` row for `account_id` against plan `plan_code`
    /// (e.g. `"team_standard"`, seat-priced; `"free"`, flat) — the ACCOUNT
    /// signal `read_member_period_scoped` (now `funding_policy::db::load_period`)'s `plan_is_seat_priced` lookup
    /// now reads (review round 2, findings 2 & 6), shared by every test
    /// fixture below that needs to control it independently of whatever the
    /// member's OWN `account_members.seat_tier_id` happens to be.
    async fn seed_seat_priced_grant(pool: &PgPool, account_id: Uuid, plan_code: &str) -> Uuid {
        let (plan_id, is_seat_priced): (Uuid, bool) =
            sqlx::query_as("SELECT id, is_seat_priced FROM plans WHERE code = $1")
                .bind(plan_code)
                .fetch_one(pool)
                .await
                .unwrap_or_else(|e| panic!("plan {plan_code:?} must be seeded: {e}"));
        let mut tx = crate::tenancy::scope::begin_account_scoped(pool, account_id)
            .await
            .expect("begin grant fixture");
        crate::billing::renewal::lock_period_rotation(&mut tx, account_id)
            .await
            .expect("enter grant fixture");
        let grant_id = sqlx::query_scalar(
            "INSERT INTO grants (account_id, plan_id, seats, plan_is_seat_priced) \
             VALUES ($1, $2, $3, $4) RETURNING id",
        )
        .bind(account_id)
        .bind(plan_id)
        .bind(is_seat_priced.then_some(1))
        .bind(is_seat_priced)
        .fetch_one(&mut *tx)
        .await
        .unwrap_or_else(|e| panic!("insert {plan_code:?} grant: {e}"));
        tx.commit().await.expect("commit grant fixture");
        grant_id
    }

    async fn set_enforcement_flag(pool: &PgPool, account_id: Uuid, value: &str) {
        execute_financial_fixture(
            pool,
            account_id,
        sqlx::query("UPDATE accounts SET billing_enforcement_enabled = $2 WHERE id = $1")
            .bind(account_id)
            .bind(value == "true"),
        )
            .await
            .expect("set accounts.billing_enforcement_enabled");
    }

    /// Seeds/updates the account's `stripe_subscriptions` row status — the
    /// freeze-not-forfeit gate ([`subscription_frozen`]) reads this column.
    /// `ON CONFLICT` so a test can flip status mid-scenario (e.g. active ->
    /// canceled, simulating a cancellation) without a second insert.
    async fn set_subscription_status(pool: &PgPool, account_id: Uuid, status: &str) {
        execute_financial_fixture(
            pool,
            account_id,
        sqlx::query(
            "INSERT INTO stripe_subscriptions (account_id, stripe_customer_id, status) \
             VALUES ($1, $2, $3) \
             ON CONFLICT (account_id) DO UPDATE SET status = EXCLUDED.status",
        )
        .bind(account_id)
        .bind(format!("cus_enforcement_{}", Uuid::new_v4().simple()))
        .bind(status),
        )
        .await
        .expect("seed stripe_subscriptions status");
    }

    /// Deletes the `accounts` row for `account_id` (cascades to
    /// `credit_balances` and `stripe_subscriptions`) and the owning `users`
    /// row, so a test doesn't leak fixture rows into the shared mcp-e2e
    /// database that other suites in this CI run also query — mirrors the
    /// inline cleanup at the end of
    /// `a_period_row_ends_the_fallthrough_to_the_org_pool`.
    async fn cleanup_account(pool: &PgPool, account_id: Uuid) -> Result<(), sqlx::Error> {
        let owner_user_id: Option<Uuid> =
            sqlx::query_scalar("SELECT owner_user_id FROM accounts WHERE id = $1")
                .bind(account_id)
                .fetch_optional(pool)
                .await?;
        execute_financial_fixture(
            pool,
            account_id,
            sqlx::query("DELETE FROM accounts WHERE id = $1")
            .bind(account_id),
        )
            .await?;
        if let Some(user_id) = owner_user_id {
            sqlx::query("DELETE FROM users WHERE id = $1")
                .bind(user_id)
                .execute(pool)
                .await?;
        }
        Ok(())
    }

    /// **Negative control 2** — a freshly-seeded account gets the column's
    /// `DEFAULT false` (migration `0164`), never an implicit ON. Then, in the
    /// SAME test, it is flipped on and re-read as ON, and finally flipped
    /// back off and re-read as OFF — the same "flip, re-read, confirm" shape
    /// the pre-`0164` `account_prefs`-based version of this test used, now
    /// against the real column rather than a JSONB row's presence/absence.
    #[tokio::test]
    async fn fresh_account_defaults_off_then_flips_on_then_back_off() {
        let _env = super::lock_kill_switch_env();
        let Some(pool) = pg_test_pool("fresh_account_defaults_off_then_flips_on_then_back_off").await
        else {
            return;
        };
        std::env::remove_var(KILL_SWITCH_ENV);
        let account_id = seed_account(&pool).await;

        assert!(
            !enforcement_enabled(Some(&pool), account_id).await,
            "a freshly-seeded account must default to OFF (accounts.billing_enforcement_enabled \
             DEFAULT false)"
        );

        set_enforcement_flag(&pool, account_id, "true").await;
        assert!(
            enforcement_enabled(Some(&pool), account_id).await,
            "an explicit true value must read as ON"
        );

        set_enforcement_flag(&pool, account_id, "false").await;
        assert!(
            !enforcement_enabled(Some(&pool), account_id).await,
            "flipping back to false must read back as OFF, not retain the prior ON reading"
        );
    }

    /// The kill switch overrides a `true` per-account row — proven with a
    /// REAL pool this time (the unit test above only proved it short-circuits
    /// with `pool: None`, which is a weaker claim).
    #[tokio::test]
    async fn kill_switch_overrides_an_explicit_true_account_pref() {
        let _env = super::lock_kill_switch_env();
        let Some(pool) = pg_test_pool("kill_switch_overrides_an_explicit_true_account_pref").await
        else {
            return;
        };
        let account_id = seed_account(&pool).await;
        set_enforcement_flag(&pool, account_id, "true").await;

        std::env::remove_var(KILL_SWITCH_ENV);
        assert!(enforcement_enabled(Some(&pool), account_id).await, "sanity: ON without the switch");

        std::env::set_var(KILL_SWITCH_ENV, "true");
        assert!(
            !enforcement_enabled(Some(&pool), account_id).await,
            "the kill switch must force OFF even though this account's own flag is true"
        );
        std::env::remove_var(KILL_SWITCH_ENV);
    }

    /// `check_and_record` with a positive balance: nothing is evaluated,
    /// nothing is written.
    #[tokio::test]
    async fn positive_balance_writes_nothing() {
        let Some(pool) = pg_test_pool("positive_balance_writes_nothing").await else {
            return;
        };
        let account_id = seed_account(&pool).await;
        set_balance(&pool, account_id, Decimal::from(500)).await;

        let result = check_and_record(&pool, account_id, "test")
            .await
            .expect("check_and_record must not error");
        assert!(result.is_none());

        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM enforcement_events WHERE account_id = $1")
            .bind(account_id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count, 0);
    }

    /// No `credit_balances` row at all (never debited, never granted):
    /// nothing to evaluate, nothing written — NOT treated as zero.
    #[tokio::test]
    async fn missing_balance_row_writes_nothing() {
        let Some(pool) = pg_test_pool("missing_balance_row_writes_nothing").await else {
            return;
        };
        let account_id = seed_account(&pool).await;

        let result = check_and_record(&pool, account_id, "test")
            .await
            .expect("check_and_record must not error");
        assert!(result.is_none());
    }

    /// **Negative control 1, this module's half.** Zero balance, flag OFF:
    /// a row IS written (observability, regardless of the flag), but
    /// `would_have_blocked` is `false`.
    #[tokio::test]
    async fn zero_balance_flag_off_records_would_have_blocked_false() {
        let _env = super::lock_kill_switch_env();
        let Some(pool) = pg_test_pool("zero_balance_flag_off_records_would_have_blocked_false").await
        else {
            return;
        };
        std::env::remove_var(KILL_SWITCH_ENV);
        let account_id = seed_account(&pool).await;
        set_balance(&pool, account_id, Decimal::ZERO).await;

        let result = check_and_record(&pool, account_id, "test_zero_flag_off")
            .await
            .expect("check_and_record must not error")
            .expect("a zero balance must be evaluated");
        assert!(!result.would_have_blocked);

        let row: (bool, String) = sqlx::query_as(
            "SELECT would_have_blocked, reason FROM enforcement_events \
             WHERE account_id = $1 ORDER BY created_at DESC LIMIT 1",
        )
        .bind(account_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(!row.0);
        assert_eq!(row.1, "test_zero_flag_off");
    }

    /// **Negative control 1, this module's other half.** Negative balance,
    /// flag ON: `would_have_blocked = true` is recorded, AND `check_and_record`
    /// still returns `Ok` — proving this function itself has no mechanism to
    /// reject/error out the caller just because enforcement would apply.
    #[tokio::test]
    async fn negative_balance_flag_on_records_would_have_blocked_true_and_still_returns_ok() {
        let _env = super::lock_kill_switch_env();
        let Some(pool) =
            pg_test_pool("negative_balance_flag_on_records_would_have_blocked_true_and_still_returns_ok")
                .await
        else {
            return;
        };
        std::env::remove_var(KILL_SWITCH_ENV);
        let account_id = seed_account(&pool).await;
        set_balance(&pool, account_id, Decimal::from(-5)).await;
        set_enforcement_flag(&pool, account_id, "true").await;

        let result = check_and_record(&pool, account_id, "test_negative_flag_on")
            .await
            .expect("check_and_record must return Ok even when it would_have_blocked");
        let check = result.expect("a negative balance must be evaluated");
        assert!(check.would_have_blocked);
        assert_eq!(check.balance, Decimal::from(-5));
    }

    // ── `is_blocked` — the real gate (real-pricing-and-limits PR) ──────────

    /// **The headline case.** Flag ON, balance exactly zero: `is_blocked`
    /// returns `true`. Written and watched FAIL first against a deliberately
    /// wrong stub (`is_blocked` returning `false` unconditionally) before the
    /// real balance check was written — see the PR body / session report for
    /// the exact failing-then-passing transcript.
    /// Seed one member allowance period covering "now".
    async fn set_allowance(
        pool: &PgPool,
        account_id: Uuid,
        user_id: Uuid,
        granted: i64,
        consumed: i64,
        cap: Option<i64>,
    ) {
        execute_financial_fixture(
            pool,
            account_id,
        sqlx::query(
            "INSERT INTO member_allowance_periods \
                 (account_id, user_id, period_start, period_end, granted_credits, \
                  consumed_credits, cap_credits) \
             VALUES ($1, $2, now() - interval '1 day', now() + interval '1 day', $3, $4, $5)",
        )
        .bind(account_id)
        .bind(user_id)
        .bind(Decimal::from(granted))
        .bind(Decimal::from(consumed))
        .bind(cap.map(Decimal::from)),
        )
        .await
        .expect("seed member_allowance_periods");
    }

    /// A member INSIDE their seat allowance is never blocked — not even when
    /// the org's purchased pool is empty. Allowance is drawn first; the pool
    /// only matters once the allowance is gone.
    #[tokio::test]
    async fn member_inside_allowance_is_not_blocked_even_with_an_empty_pool() {
        let _env = super::lock_kill_switch_env();
        let Some(pool) = pg_test_pool("member_inside_allowance_is_not_blocked_even_with_an_empty_pool").await else {
            return;
        };
        std::env::remove_var(KILL_SWITCH_ENV);
        let account_id = seed_account(&pool).await;
        let user_id = Uuid::new_v4();
        set_balance(&pool, account_id, Decimal::ZERO).await;
        set_enforcement_flag(&pool, account_id, "true").await;
        set_allowance(&pool, account_id, user_id, 1000, 400, None).await;

        assert!(
            !is_blocked(&pool, account_id, user_id, Spend::Interactive).await,
            "600 credits of allowance remain — an empty ORG pool must not block this member"
        );
    }

    /// **The founder's rule, and the reason the cap exists.** A member past
    /// their allowance AND at their cap is blocked even though the org pool
    /// still has plenty — nobody can eat the team's month.
    ///
    /// **Billing self-serve launch PR E:** this 1x default only applies to a
    /// SEAT-TIER member — see [`seed_org_member`]'s own doc comment for why
    /// this test now goes through it rather than [`seed_account`] (which
    /// would exercise the OTHER, unlimited default post-PR-E — see
    /// [`a_flat_plan_members_cap_default_is_unlimited_not_one_times`]).
    #[tokio::test]
    async fn member_at_their_cap_is_blocked_even_though_the_pool_has_credits() {
        let _env = super::lock_kill_switch_env();
        let Some(pool) = pg_test_pool("member_at_their_cap_is_blocked_even_though_the_pool_has_credits").await else {
            return;
        };
        std::env::remove_var(KILL_SWITCH_ENV);
        let (account_id, user_id) = seed_org_member(&pool, "standard").await;
        set_balance(&pool, account_id, Decimal::from(50_000)).await;
        set_enforcement_flag(&pool, account_id, "true").await;
        // granted 1000, consumed 2000 -> 1000 of overage used, cap defaults
        // to 1x granted = 1000 for a SEAT-TIER member, so the cap is exactly
        // reached.
        set_allowance(&pool, account_id, user_id, 1000, 2000, None).await;

        assert!(
            is_blocked(&pool, account_id, user_id, Spend::Interactive).await,
            "overage used (1000) has reached the default 1x cap — blocked despite a 50,000 pool"
        );
    }

    /// **PR E review round 2, finding 2 (MOST IMPORTANT).**
    /// `clerk_provision::remove_org_membership` DELETES the
    /// `account_members` row on revocation while `member_allowance_periods`
    /// rows survive by design — a removed member's cap must NOT silently
    /// widen from 1x to unlimited just because their membership row is gone.
    /// Proven here by literally deleting the `account_members` row (the
    /// production revocation path's exact effect on this table) after
    /// seeding the allowance, then asserting the member is STILL blocked at
    /// their 1x cap — which only holds if the cap default is resolved from
    /// the ACCOUNT's grant (untouched by the deletion), not the now-gone
    /// member row.
    #[tokio::test]
    async fn a_removed_team_members_cap_default_stays_one_times_not_unlimited() {
        let _env = super::lock_kill_switch_env();
        let Some(pool) =
            pg_test_pool("a_removed_team_members_cap_default_stays_one_times_not_unlimited").await
        else {
            return;
        };
        std::env::remove_var(KILL_SWITCH_ENV);
        let (account_id, user_id) = seed_org_member(&pool, "standard").await;
        set_balance(&pool, account_id, Decimal::from(50_000)).await;
        set_enforcement_flag(&pool, account_id, "true").await;
        set_allowance(&pool, account_id, user_id, 1000, 2000, None).await;

        sqlx::query("DELETE FROM account_members WHERE account_id = $1 AND user_id = $2")
            .bind(account_id)
            .bind(user_id)
            .execute(&pool)
            .await
            .expect("simulate clerk_provision::remove_org_membership's effect on account_members");

        assert!(
            is_blocked(&pool, account_id, user_id, Spend::Interactive).await,
            "a removed member's cap must stay at the 1x default — the account's grant is still \
             seat-priced, and the member row's absence must not widen it to unlimited"
        );
    }

    /// **PR E review round 2, finding 6.** `0169`'s trigger assigns EVERY org
    /// member a `seat_tier_id` (defaulting to `standard`) regardless of what
    /// plan the account actually holds — so an org account sitting on the
    /// flat `free` plan (the auto-granted default at creation, pre-#651)
    /// still has a member row with a non-NULL `seat_tier_id`. Plan §1.10's
    /// cap default is decided by the ACCOUNT's pricing shape, not that
    /// column, so this member must resolve UNLIMITED (falling through to the
    /// pool check) exactly like a personal flat-plan account would.
    #[tokio::test]
    async fn a_flat_plan_org_members_cap_default_is_also_unlimited_despite_their_seat_tier_id() {
        let _env = super::lock_kill_switch_env();
        let Some(pool) = pg_test_pool(
            "a_flat_plan_org_members_cap_default_is_also_unlimited_despite_their_seat_tier_id",
        )
        .await
        else {
            return;
        };
        std::env::remove_var(KILL_SWITCH_ENV);

        let user_id = Uuid::new_v4();
        let account_id = Uuid::new_v4();
        sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
            .bind(user_id)
            .bind(format!("{user_id}@enforcement-pr10.test"))
            .execute(&pool)
            .await
            .expect("insert user");
        sqlx::query("INSERT INTO accounts (id, owner_user_id, clerk_org_id) VALUES ($1, $2, $3)")
            .bind(account_id)
            .bind(user_id)
            .bind(format!("org_{}", account_id.simple()))
            .execute(&pool)
            .await
            .expect("insert org account");
        // 0169's trigger stamps a seat_tier_id on every org member regardless
        // of plan shape — explicit here rather than relied upon, so the test
        // is unambiguous about what it is proving.
        let standard_id: Uuid =
            sqlx::query_scalar("SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true")
                .fetch_one(&pool)
                .await
                .expect("standard seat tier must be seeded (0165)");
        sqlx::query(
            "INSERT INTO account_members (account_id, user_id, role, seat_tier_id) \
             VALUES ($1, $2, 'owner', $3)",
        )
        .bind(account_id)
        .bind(user_id)
        .bind(standard_id)
        .execute(&pool)
        .await
        .expect("insert account_members row with a seat_tier_id despite the flat plan below");
        // The account's actual grant is the FLAT `free` plan, not seat-priced.
        seed_seat_priced_grant(&pool, account_id, "free").await;

        set_balance(&pool, account_id, Decimal::from(50_000)).await;
        set_enforcement_flag(&pool, account_id, "true").await;
        set_allowance(&pool, account_id, user_id, 1000, 2000, None).await;

        assert!(
            !is_blocked(&pool, account_id, user_id, Spend::Interactive).await,
            "a flat-plan org account's member must resolve the UNLIMITED cap default despite \
             carrying a seat_tier_id on their own account_members row — falls through to the \
             (funded) pool check instead of being blocked at a 1x cap that only applies to \
             seat-priced accounts"
        );
    }

    /// **Billing self-serve launch PR E, plan §1.10: the OTHER cap default.**
    /// A flat-plan (`pro`) member — no `seat_tier_id` at all, the shape
    /// [`seed_account`] already produces — has an UNLIMITED cap by default:
    /// no matter how much overage they draw, step 3 (the cap check) never
    /// blocks them; only the org pool actually running out (step 4) can. A
    /// solo user drawing their own prepaid balance must never be stopped by
    /// an arbitrary 1x-of-allowance ceiling on THEIR OWN money.
    #[tokio::test]
    async fn a_flat_plan_members_cap_default_is_unlimited_not_one_times() {
        let _env = super::lock_kill_switch_env();
        let Some(pool) =
            pg_test_pool("a_flat_plan_members_cap_default_is_unlimited_not_one_times").await
        else {
            return;
        };
        std::env::remove_var(KILL_SWITCH_ENV);
        let account_id = seed_account(&pool).await;
        let user_id = Uuid::new_v4();
        // `account_members.user_id` carries a real FK to `users` (unlike
        // `member_allowance_periods.user_id`, which deliberately has none —
        // see `0161`'s header), so this member needs an actual `users` row
        // before it can be inserted.
        sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
            .bind(user_id)
            .bind(format!("{user_id}@enforcement-pr10.test"))
            .execute(&pool)
            .await
            .expect("insert test user");
        sqlx::query(
            "INSERT INTO account_members (account_id, user_id, role) VALUES ($1, $2, 'owner')",
        )
        .bind(account_id)
        .bind(user_id)
        .execute(&pool)
        .await
        .expect("insert account_members row (personal account -> seat_tier_id stays NULL)");
        // A real `pro` (flat) grant — without this, the query under test has
        // no non-revoked grant row at all, and resolves `plan_is_seat_priced`
        // via `COALESCE(cur_grant.plan_is_seat_priced, false)`'s no-grant
        // fallback rather than actually reading `plan_is_seat_priced = false`
        // off a grant. The no-grant path is already covered elsewhere; this
        // test exists to prove the flat-GRANT branch specifically.
        seed_seat_priced_grant(&pool, account_id, "pro").await;
        set_balance(&pool, account_id, Decimal::from(50_000)).await;
        set_enforcement_flag(&pool, account_id, "true").await;
        // The identical numbers `member_at_their_cap_is_blocked_even_though_the_pool_has_credits`
        // uses to prove a BLOCK for a seat-tier member — 1000 of overage
        // used, no explicit cap — prove the OPPOSITE here: a flat-plan
        // member with the SAME overage is NOT blocked, because their cap
        // default is unlimited rather than 1x.
        set_allowance(&pool, account_id, user_id, 1000, 2000, None).await;

        assert!(
            !is_blocked(&pool, account_id, user_id, Spend::Interactive).await,
            "a flat-plan member's cap default is UNLIMITED — 1000 of overage against a 50,000 \
             pool must not block, even though the identical numbers block a seat-tier member"
        );

        // And the pool actually running out is STILL the real ceiling —
        // unlimited cap does not mean unlimited spend.
        set_balance(&pool, account_id, Decimal::ZERO).await;
        assert!(
            is_blocked(&pool, account_id, user_id, Spend::Interactive).await,
            "unlimited CAP does not mean unlimited spend — an exhausted pool still blocks a \
             flat-plan member"
        );
    }

    /// Past the allowance but still UNDER the cap, with pool left: allowed.
    /// This is the "work continues instead of stopping" half of the model.
    #[tokio::test]
    async fn member_over_allowance_but_under_cap_draws_the_pool() {
        let _env = super::lock_kill_switch_env();
        let Some(pool) = pg_test_pool("member_over_allowance_but_under_cap_draws_the_pool").await else {
            return;
        };
        std::env::remove_var(KILL_SWITCH_ENV);
        let account_id = seed_account(&pool).await;
        let user_id = Uuid::new_v4();
        set_balance(&pool, account_id, Decimal::from(50_000)).await;
        set_enforcement_flag(&pool, account_id, "true").await;
        set_allowance(&pool, account_id, user_id, 1000, 1500, None).await;

        assert!(
            !is_blocked(&pool, account_id, user_id, Spend::Interactive).await,
            "500 of overage used against a 1000 cap, pool positive — must keep working"
        );
    }

    /// An explicit `cap_credits` overrides the 1x default — the documented
    /// escape hatch for "admin raises that member's cap".
    #[tokio::test]
    async fn an_explicit_cap_overrides_the_one_times_default() {
        let _env = super::lock_kill_switch_env();
        let Some(pool) = pg_test_pool("an_explicit_cap_overrides_the_one_times_default").await else {
            return;
        };
        std::env::remove_var(KILL_SWITCH_ENV);
        let account_id = seed_account(&pool).await;
        let user_id = Uuid::new_v4();
        set_balance(&pool, account_id, Decimal::from(50_000)).await;
        set_enforcement_flag(&pool, account_id, "true").await;
        // Same numbers as the cap test above, but the admin raised the cap to
        // 3000 — the member that would otherwise be blocked keeps working.
        set_allowance(&pool, account_id, user_id, 1000, 2000, Some(3000)).await;

        assert!(
            !is_blocked(&pool, account_id, user_id, Spend::Interactive).await,
            "a raised cap must unblock a member the 1x default would have stopped"
        );
    }

    /// Past the allowance, under the cap, but the org pool is drained.
    #[tokio::test]
    async fn member_under_cap_is_blocked_when_the_pool_is_empty() {
        let _env = super::lock_kill_switch_env();
        let Some(pool) = pg_test_pool("member_under_cap_is_blocked_when_the_pool_is_empty").await else {
            return;
        };
        std::env::remove_var(KILL_SWITCH_ENV);
        let account_id = seed_account(&pool).await;
        let user_id = Uuid::new_v4();
        set_balance(&pool, account_id, Decimal::ZERO).await;
        set_enforcement_flag(&pool, account_id, "true").await;
        set_allowance(&pool, account_id, user_id, 1000, 1500, None).await;

        assert!(
            is_blocked(&pool, account_id, user_id, Spend::Interactive).await,
            "under the cap but the shared pool is exhausted — nothing left to draw"
        );
    }

    /// **The regression test for freeze-not-forfeit (billing self-serve plan
    /// §1.11).** A POSITIVE pool balance — purchased credits that survived a
    /// cancellation, exactly the case the plan calls out by name — must
    /// still be BLOCKED while `stripe_subscriptions.status` is not
    /// `"active"`. Before `subscription_frozen` existed, this exact scenario
    /// (frozen status, real balance left) sailed straight through
    /// `org_pool_exhausted`'s bare `balance <= 0` check and stayed fully
    /// spendable — the money-shaped hole this test closes.
    #[tokio::test]
    async fn frozen_subscription_blocks_a_positive_pool_balance() {
        let _env = super::lock_kill_switch_env();
        let Some(pool) = pg_test_pool("frozen_subscription_blocks_a_positive_pool_balance").await else {
            return;
        };
        std::env::remove_var(KILL_SWITCH_ENV);
        let account_id = seed_account(&pool).await;
        set_balance(&pool, account_id, Decimal::from(5_000)).await;
        set_enforcement_flag(&pool, account_id, "true").await;
        set_subscription_status(&pool, account_id, "canceled").await;

        let blocked = is_blocked(&pool, account_id, Uuid::new_v4(), Spend::Interactive).await;
        cleanup_account(&pool, account_id)
            .await
            .expect("cleanup test fixture");
        assert!(
            blocked,
            "a positive balance must NOT be spendable while the subscription is frozen \
             (canceled) — purchased credits survive cancellation but stay unspendable until \
             resubscribe"
        );
    }

    /// Same scenario, `past_due` (an involuntary lapse, subscription still
    /// alive) rather than `canceled` (deleted) — both freeze identically.
    #[tokio::test]
    async fn past_due_subscription_also_blocks_a_positive_pool_balance() {
        let _env = super::lock_kill_switch_env();
        let Some(pool) =
            pg_test_pool("past_due_subscription_also_blocks_a_positive_pool_balance").await
        else {
            return;
        };
        std::env::remove_var(KILL_SWITCH_ENV);
        let account_id = seed_account(&pool).await;
        set_balance(&pool, account_id, Decimal::from(5_000)).await;
        set_enforcement_flag(&pool, account_id, "true").await;
        set_subscription_status(&pool, account_id, "past_due").await;

        let blocked = is_blocked(&pool, account_id, Uuid::new_v4(), Spend::Interactive).await;
        cleanup_account(&pool, account_id)
            .await
            .expect("cleanup test fixture");
        assert!(blocked, "past_due must freeze the pool exactly like canceled does");
    }

    /// **Positive control for the fix above** — an ACTIVE subscription with
    /// the identical positive balance must stay spendable. Without this test
    /// alongside the two above, a bug that froze EVERY account regardless of
    /// status would also pass.
    #[tokio::test]
    async fn active_subscription_leaves_a_positive_pool_balance_spendable() {
        let _env = super::lock_kill_switch_env();
        let Some(pool) =
            pg_test_pool("active_subscription_leaves_a_positive_pool_balance_spendable").await
        else {
            return;
        };
        std::env::remove_var(KILL_SWITCH_ENV);
        let account_id = seed_account(&pool).await;
        set_balance(&pool, account_id, Decimal::from(5_000)).await;
        set_enforcement_flag(&pool, account_id, "true").await;
        set_subscription_status(&pool, account_id, "active").await;

        let blocked = is_blocked(&pool, account_id, Uuid::new_v4(), Spend::Interactive).await;
        cleanup_account(&pool, account_id)
            .await
            .expect("cleanup test fixture");
        assert!(
            !blocked,
            "an active subscription must not be gated by this check at all"
        );
    }

    /// **No `stripe_subscriptions` row at all** (never went through Stripe
    /// checkout — a free-tier or hand-comp'd account) must NOT be treated as
    /// frozen — "missing state is not an ON state", this module's standing
    /// convention. Only the bare balance check applies.
    #[tokio::test]
    async fn no_stripe_subscriptions_row_is_not_treated_as_frozen() {
        let _env = super::lock_kill_switch_env();
        let Some(pool) =
            pg_test_pool("no_stripe_subscriptions_row_is_not_treated_as_frozen").await
        else {
            return;
        };
        std::env::remove_var(KILL_SWITCH_ENV);
        let account_id = seed_account(&pool).await;
        set_balance(&pool, account_id, Decimal::from(5_000)).await;
        set_enforcement_flag(&pool, account_id, "true").await;
        // Deliberately no `set_subscription_status` call.

        let blocked = is_blocked(&pool, account_id, Uuid::new_v4(), Spend::Interactive).await;
        cleanup_account(&pool, account_id)
            .await
            .expect("cleanup test fixture");
        assert!(
            !blocked,
            "an account with no Stripe linkage at all must not be gated by a freeze check that \
             has nothing to freeze"
        );
    }

    /// **The re-graining must never make the gate WEAKER.** A member with no
    /// allowance-period row at all still falls back to the account-level pool
    /// check that predates Phase 2b. Period rows do not exist for any account
    /// yet, so without this the whole gate would silently become a no-op the
    /// moment it shipped.
    #[tokio::test]
    async fn no_period_row_still_falls_back_to_the_account_level_check() {
        let _env = super::lock_kill_switch_env();
        let Some(pool) = pg_test_pool("no_period_row_still_falls_back_to_the_account_level_check").await else {
            return;
        };
        std::env::remove_var(KILL_SWITCH_ENV);
        let account_id = seed_account(&pool).await;
        set_balance(&pool, account_id, Decimal::ZERO).await;
        set_enforcement_flag(&pool, account_id, "true").await;

        assert!(
            is_blocked(&pool, account_id, Uuid::new_v4(), Spend::Interactive).await,
            "no member_allowance_periods row must NOT weaken the pre-existing account-level gate"
        );
    }

    #[tokio::test]
    async fn zero_balance_flag_on_is_blocked() {
        let _env = super::lock_kill_switch_env();
        let Some(pool) = pg_test_pool("zero_balance_flag_on_is_blocked").await else {
            return;
        };
        std::env::remove_var(KILL_SWITCH_ENV);
        let account_id = seed_account(&pool).await;
        set_balance(&pool, account_id, Decimal::ZERO).await;
        set_enforcement_flag(&pool, account_id, "true").await;

        assert!(is_blocked(&pool, account_id, Uuid::new_v4(), Spend::Interactive).await, "a zero balance with enforcement ON must block");
    }

    /// Same as above but a NEGATIVE balance (the shape `check_and_record`'s
    /// own negative-balance test already covers on the observe-only side) —
    /// `<= 0`, not `== 0`, must be the actual predicate.
    #[tokio::test]
    async fn negative_balance_flag_on_is_blocked() {
        let _env = super::lock_kill_switch_env();
        let Some(pool) = pg_test_pool("negative_balance_flag_on_is_blocked").await else {
            return;
        };
        std::env::remove_var(KILL_SWITCH_ENV);
        let account_id = seed_account(&pool).await;
        set_balance(&pool, account_id, Decimal::from(-5)).await;
        set_enforcement_flag(&pool, account_id, "true").await;

        assert!(is_blocked(&pool, account_id, Uuid::new_v4(), Spend::Interactive).await, "a negative balance with enforcement ON must block");
    }

    /// **Negative control.** A POSITIVE balance, flag ON: never blocked,
    /// regardless of enforcement being enabled.
    #[tokio::test]
    async fn positive_balance_is_not_blocked() {
        let _env = super::lock_kill_switch_env();
        let Some(pool) = pg_test_pool("positive_balance_is_not_blocked").await else {
            return;
        };
        std::env::remove_var(KILL_SWITCH_ENV);
        let account_id = seed_account(&pool).await;
        set_balance(&pool, account_id, Decimal::from(500)).await;
        set_enforcement_flag(&pool, account_id, "true").await;

        assert!(
            !is_blocked(&pool, account_id, Uuid::new_v4(), Spend::Interactive).await,
            "a positive balance must never block, even with enforcement ON"
        );
    }

    /// **Negative control.** No `credit_balances` row at all — an account
    /// that has never been granted or debited has nothing to evaluate. Same
    /// "missing row is not the same as zero" semantics `check_and_record`
    /// already documents.
    #[tokio::test]
    async fn missing_balance_row_is_not_blocked() {
        let _env = super::lock_kill_switch_env();
        let Some(pool) = pg_test_pool("missing_balance_row_is_not_blocked").await else {
            return;
        };
        std::env::remove_var(KILL_SWITCH_ENV);
        let account_id = seed_account(&pool).await;
        set_enforcement_flag(&pool, account_id, "true").await;

        assert!(
            !is_blocked(&pool, account_id, Uuid::new_v4(), Spend::Interactive).await,
            "an account with no credit_balances row must not be blocked"
        );
    }

    /// **Negative control — the kill switch overrides an enabled account.**
    /// Same account, same zero balance, same `true` per-account flag as
    /// `zero_balance_flag_on_is_blocked` above — the ONLY difference is the
    /// kill switch — proving the switch wins over a real would-block
    /// condition, not merely over `enforcement_enabled`'s own boolean in
    /// isolation (that narrower claim is already proven by the pure
    /// `kill_switch_overrides_an_explicit_true_account_pref` test above).
    #[tokio::test]
    async fn kill_switch_overrides_a_would_be_blocked_account() {
        let _env = super::lock_kill_switch_env();
        let Some(pool) = pg_test_pool("kill_switch_overrides_a_would_be_blocked_account").await else {
            return;
        };
        let account_id = seed_account(&pool).await;
        set_balance(&pool, account_id, Decimal::ZERO).await;
        set_enforcement_flag(&pool, account_id, "true").await;

        std::env::remove_var(KILL_SWITCH_ENV);
        assert!(is_blocked(&pool, account_id, Uuid::new_v4(), Spend::Interactive).await, "sanity: blocked without the switch");

        std::env::set_var(KILL_SWITCH_ENV, "true");
        assert!(
            !is_blocked(&pool, account_id, Uuid::new_v4(), Spend::Interactive).await,
            "the kill switch must force NOT BLOCKED even though this account would otherwise block"
        );
        std::env::remove_var(KILL_SWITCH_ENV);
    }

    /// **Negative control — `billing_enforcement_enabled = false` at zero
    /// balance.** The per-account flag off is enough on its own (no kill
    /// switch involved) to keep a zero-balance account unblocked.
    #[tokio::test]
    async fn enforcement_disabled_account_is_not_blocked_even_at_zero_balance() {
        let _env = super::lock_kill_switch_env();
        let Some(pool) =
            pg_test_pool("enforcement_disabled_account_is_not_blocked_even_at_zero_balance").await
        else {
            return;
        };
        std::env::remove_var(KILL_SWITCH_ENV);
        let account_id = seed_account(&pool).await;
        set_balance(&pool, account_id, Decimal::ZERO).await;
        // No account_prefs row at all -- the same "default-OFF" shape
        // enforcement_enabled's own tests already prove; set_enforcement_flag(..., "false")
        // would prove the identical thing for an explicit false, so this test
        // takes the "no row" branch to also exercise that default alongside
        // the zero-balance predicate, rather than duplicating a pure
        // enforcement_enabled test with a live balance grafted on.
        assert!(
            !is_blocked(&pool, account_id, Uuid::new_v4(), Spend::Interactive).await,
            "billing_enforcement_enabled unset (default OFF) must not block, even at balance <= 0"
        );

        // And the explicit-false case, for completeness against the same
        // zero balance.
        set_enforcement_flag(&pool, account_id, "false").await;
        assert!(
            !is_blocked(&pool, account_id, Uuid::new_v4(), Spend::Interactive).await,
            "an explicit billing_enforcement_enabled = false must not block, even at balance <= 0"
        );
    }

    /// **The proof that Phase 4a ends the inertness.**
    ///
    /// Phase 2b re-grained this gate to `(account, member)` but nothing wrote
    /// `member_allowance_periods` rows, so every call fell through to
    /// `org_pool_exhausted` — deliberately, because re-graining may only make
    /// the gate stricter. This test is the before/after: the SAME account, the
    /// SAME empty pool, blocked before a period row exists and NOT blocked
    /// after, purely because `billing::renewal::roll_period` ran.
    ///
    /// It is deliberately NOT a duplicate of the sibling tests above, which
    /// hand-INSERT a `member_allowance_periods` row to set up their state. This
    /// one drives the same state change through the PRODUCTION writer, so it is
    /// the only test here that would catch a `roll_period` that wrote rows the
    /// gate cannot actually match — a period the gate never finds, a
    /// `granted_credits` of zero, or a `cap_credits` the renewal should have
    /// left NULL. A hand-built fixture proves the gate reads correctly; only
    /// this proves the two halves fit together.
    ///
    /// It then walks the two ways a member becomes blocked again, which are
    /// deliberately different reasons:
    ///
    ///   * allowance spent, pool empty  -> blocked via the pool,
    ///   * allowance spent, pool FUNDED, own cap reached -> blocked anyway.
    ///
    /// The second is the founder's stated rule — "nobody can eat the team's
    /// month" — and it is the one that cannot be expressed at all without the
    /// per-member row, so it is the single most important assertion here.
    #[tokio::test]
    async fn a_period_row_ends_the_fallthrough_to_the_org_pool() {
        let _env = super::lock_kill_switch_env();
        let Some(pool) = pg_test_pool("a_period_row_ends_the_fallthrough_to_the_org_pool").await
        else {
            return;
        };
        std::env::remove_var(KILL_SWITCH_ENV);

        let account_id = seed_account(&pool).await;
        let member_id: Uuid = sqlx::query_scalar("SELECT owner_user_id FROM accounts WHERE id = $1")
            .bind(account_id)
            .fetch_one(&pool)
            .await
            .expect("owner");
        // Billing self-serve launch PR E: this test's final assertion relies
        // on the CAP actually blocking a member whose overage reaches the 1x
        // default despite a funded pool — that default now only applies to a
        // SEAT-TIER member (`read_member_period_scoped`'s doc comment), so
        // this account must be flagged as an org account (`clerk_org_id`,
        // which `0169`'s trigger turns into `accounts.is_org_account`) and
        // this member's row must carry a `seat_tier_id`, mirroring
        // [`seed_org_member`] inline rather than through that helper (this
        // test needs `account_id` fixed up front, before the helper's own
        // account-creation step would run).
        execute_financial_fixture(
            &pool,
            account_id,
        sqlx::query("UPDATE accounts SET clerk_org_id = $2 WHERE id = $1")
            .bind(account_id)
            .bind(format!("org_{}", account_id.simple())),
        )
            .await
            .expect("flip account to an org account");
        let seat_tier_id: Uuid =
            sqlx::query_scalar("SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true")
                .fetch_one(&pool)
                .await
                .expect("standard seat tier must be seeded (0165)");
        sqlx::query(
            "INSERT INTO account_members (account_id, user_id, role, seat_tier_id) \
             VALUES ($1, $2, 'owner', $3)",
        )
        .bind(account_id)
        .bind(member_id)
        .bind(seat_tier_id)
        .execute(&pool)
        .await
        .expect("seed membership");

        set_enforcement_flag(&pool, account_id, "true").await;
        // The org's purchased pool is empty, which is what the pre-Phase-4a
        // fallback keys on.
        set_balance(&pool, account_id, Decimal::ZERO).await;

        assert!(
            is_blocked(&pool, account_id, member_id, Spend::Interactive).await,
            "BEFORE: with no allowance-period row the gate must still fall through to the \
             account-level pool check — that fallback is what stopped Phase 2b from making \
             the gate weaker"
        );

        // Grant a seat-priced plan and roll a period that spans now(), which is
        // what `is_blocked`'s `period_start <= now() AND period_end > now()`
        // lookup requires.
        let plan_id: Uuid = sqlx::query_scalar("SELECT id FROM plans WHERE code = 'team_standard'")
            .fetch_one(&pool)
            .await
            .expect("team_standard must be seeded (0155)");
        execute_financial_fixture(
            &pool,
            account_id,
        sqlx::query(
            "INSERT INTO grants (account_id, plan_id, seats, plan_is_seat_priced) \
             VALUES ($1, $2, 1, true)",
        )
        .bind(account_id)
        .bind(plan_id),
        )
        .await
        .expect("insert grant");

        let start = chrono::Utc::now() - chrono::Duration::days(1);
        let end = chrono::Utc::now() + chrono::Duration::days(30);
        let mut tx = crate::tenancy::scope::begin_account_scoped(&pool, account_id).await.expect("begin period fixture");
        crate::billing::renewal::lock_period_rotation(&mut tx, account_id)
            .await
            .expect("enter period fixture");
        let outcome = crate::billing::renewal::roll_period(&mut tx, account_id, start, end)
            .await
            .expect("roll the period");
        tx.commit().await.expect("commit period fixture");
        assert_eq!(outcome.rows_granted, 1, "the member must have been granted");

        assert!(
            !is_blocked(&pool, account_id, member_id, Spend::Interactive).await,
            "AFTER: a member inside their seat allowance is NOT blocked, even though the org \
             pool is still empty — this is the whole point of the split buckets, and it is \
             the assertion that proves Phase 4a ended the fall-through"
        );

        // ── Blocked again, reason one: allowance spent AND the pool is empty.
        execute_financial_fixture(
            &pool,
            account_id,
        sqlx::query(
            "UPDATE member_allowance_periods SET consumed_credits = granted_credits \
              WHERE account_id = $1 AND user_id = $2",
        )
        .bind(account_id)
        .bind(member_id),
        )
        .await
        .expect("spend the allowance");
        assert!(
            is_blocked(&pool, account_id, member_id, Spend::Interactive).await,
            "allowance spent and no purchased pool to draw on"
        );

        // ── Blocked again, reason two: the pool is FUNDED, but the member has
        // reached their own cap. This is the founder's rule, and it is
        // inexpressible without the per-member row.
        set_balance(&pool, account_id, Decimal::from(50_000)).await;
        assert!(
            !is_blocked(&pool, account_id, member_id, Spend::Interactive).await,
            "just past their allowance and under their cap, the member draws the funded pool"
        );

        execute_financial_fixture(
            &pool,
            account_id,
        sqlx::query(
            "UPDATE member_allowance_periods \
                SET consumed_credits = granted_credits + COALESCE(cap_credits, granted_credits) \
              WHERE account_id = $1 AND user_id = $2",
        )
        .bind(account_id)
        .bind(member_id),
        )
        .await
        .expect("exhaust the cap");
        assert!(
            is_blocked(&pool, account_id, member_id, Spend::Interactive).await,
            "AT THE CAP the member is blocked even though the org pool holds 50,000 credits — \
             one heavy user cannot eat the team's month"
        );

        let _ = execute_financial_fixture(
            &pool,
            account_id,
            sqlx::query("DELETE FROM accounts WHERE id = $1")
            .bind(account_id),
        )
            .await;
        let _ = sqlx::query("DELETE FROM users WHERE id = $1")
            .bind(member_id)
            .execute(&pool)
            .await;
    }

}
