//! `backfill_billing_enforcement` — the backfill named in the registered plan
//! (`.scratch/billing-self-serve/plan.md` §1.9): "flip orgs at the same
//! grant-gated point in `ensure_org_account` ... plus a one-off backfill
//! script (grant-if-grantless, then flip) for pre-#651 accounts." **No longer
//! only a one-off**, as of the enforcement-completion PR's review round 2
//! (finding 6): this binary now ALSO doubles as a general-purpose, always-
//! safe-to-re-run REPAIR tool — see "Doubles as a repair tool" below.
//!
//! ## Why this exists
//!
//! Migration `0164_accounts_billing_enforcement_enabled.sql`'s own header
//! names this exact script and says nobody wrote it yet: "adding the column
//! must never itself turn on enforcement for any existing account — that is
//! a separate, deliberate step run only after every account holds a real
//! grant (see the enforcement-rollout PR's backfill script)". PR #651's
//! commit message records the same gap explicitly: "Not included: the
//! production backfill ... that's a deliberate follow-up." This is that
//! follow-up.
//!
//! Every account provisioned BEFORE the enforcement-completion PR shipped —
//! six, as of the plan's own count, all personal accounts predating any org
//! — was granted `free` but never flipped (`clerk_provision::
//! ensure_org_account`'s old comment; `ensure_personal_account`'s
//! `provision_billing` DID flip personal accounts from #651 onward, so this
//! backfill is a no-op for those, and reports them as `already-complete`).
//! Any account created going forward is granted-and-flipped at provisioning
//! time (`clerk_provision::provision_billing`) and a joining org member is
//! seeded at join time (`billing::renewal::seed_member_period`) — this
//! binary's PRIMARY job is catching up accounts that predate both of those
//! code paths.
//!
//! ## Doubles as a repair tool
//!
//! Every live join path now seeds its member's allowance on a best-effort
//! basis (`clerk_provision::ensure_org_membership`,
//! `clerk_provision::refresh_org_membership_from_claims`,
//! `routes::teams::invite_member`) — a failure there is logged
//! (`tracing::warn!`) and swallowed rather than failing the request, by
//! design (see each call site's own comment for why). This binary is the
//! OTHER half of that contract: an operator can re-run it — narrowed to one
//! account via `--account-id`, or unnarrowed over everything — at ANY time,
//! not just once before this PR shipped, to catch up whatever a best-effort
//! seed missed. Every one of its four steps is independently idempotent (see
//! "Idempotent" below) and it now refuses to flip an account whose seeding
//! did not fully complete (see finding 1 in the enforcement-completion PR's
//! review round 2, and [`Totals::incomplete`]), so re-running it is always
//! safe and a repeated pass over already-complete accounts costs nothing but
//! read queries.
//!
//! ## What one pass does, per account (personal AND org)
//!
//! 1. **Skip revoked orgs.** An account whose `clerk_org_id` has an
//!    `org_revocations` row (`organization.deleted`) is left alone
//!    entirely — no grant, no seed, no flip. Nobody can join a revoked org
//!    again (`clerk_provision::ensure_org_membership`'s own `NOT EXISTS
//!    org_revocations` guard), so granting or enforcing it would be pure
//!    dead weight, and flipping `billing_enforcement_enabled` on a
//!    tombstoned account is simply not this backfill's job.
//! 2. **Grant-if-grantless.** If the account holds NO active (non-revoked)
//!    grant for ANY plan, issue the `free` plan via
//!    [`coralswarm_backend::billing::issuance::issue_grant`] — the exact
//!    call [`coralswarm_backend::clerk_provision`]'s (private)
//!    `auto_grant_free_plan` makes. An account that already holds an active
//!    grant, of ANY plan (not just `free` — a hand-sold `founder_comp` or a
//!    real paid plan both count), is left alone: this is "grant-if-
//!    grantless", never "re-grant" or "upgrade".
//! 3. **Seed missing member period rows.** For every current
//!    `account_members` row, call
//!    [`coralswarm_backend::billing::renewal::seed_member_period`] — the
//!    SAME function `clerk_provision::ensure_org_membership` calls on every
//!    live join, reused here rather than re-implemented so a backfilled
//!    account's rows are byte-for-byte what a live join would have
//!    produced. Idempotent per member (its own natural-key `ON CONFLICT DO
//!    NOTHING`), so a member who already holds a current-period row is
//!    skipped with no write.
//! 4. **Flip the enforcement flag**, if not already on — but ONLY once step
//!    2/3 leave the account holding a real, active grant. An account whose
//!    `free`-plan lookup itself failed (see migration
//!    `0153_billing_plans_seed.sql`'s seed) is never flipped: mirrors
//!    `clerk_provision::provision_billing`'s own invariant that enforcement
//!    must never be enabled for an account with nothing to spend, exactly
//!    as strictly here as at live provisioning time.
//!
//! ## Idempotent
//!
//! Every one of the four steps above is independently a safe no-op on
//! re-run: `issue_grant` no-ops on an existing active grant,
//! `seed_member_period` no-ops on an existing current-period row per member,
//! and the flip is a plain `UPDATE ... WHERE NOT
//! billing_enforcement_enabled`. A second pass over the exact same accounts
//! reports every one of them `already-complete` and writes nothing.
//!
//! ## Dry-run by default
//!
//! Same inverted-default discipline as `grant_plan` and
//! `backfill_reef_judgement_from_history`: the safe outcome of a mistyped
//! invocation is a printed report, not a write. `--execute` is required to
//! actually grant/seed/flip anything.
//!
//! ## Usage
//!
//! ```text
//! backfill_billing_enforcement [--execute] [--account-id <uuid>]
//! ```
//!
//! `--account-id <uuid>` narrows the whole run to exactly one account —
//! a rehearsal/repair aid, never how the real backfill is expected to run
//! (the default, no-filter invocation is what actually clears the
//! production gap this binary exists to close).
//!
//! ## Env
//!
//!   MIGRATOR_DATABASE_URL / DATABASE_URL   control-plane DSN, resolved via
//!       [`coralswarm_backend::migrator_database_url`] (`MIGRATOR_DATABASE_URL`
//!       preferred, `DATABASE_URL` as fallback — same precedence
//!       `migrate_control`/`backfill_reef_judgement_from_history` use).
//!       Unlike that judgement backfill, an ELEVATED role is not actually
//!       REQUIRED here: every write goes through
//!       `tenancy::scope::begin_account_scoped` (via `issue_grant`/
//!       `seed_member_period`) or a plain `UPDATE accounts` (which carries
//!       no RLS policy at all — see migration `0164`'s header), exactly the
//!       same posture `grant_plan`'s own doc comment documents. The elevated
//!       DSN is still honored when set, since it is a strict superset of
//!       what this binary needs.
//!
//! ## Exit codes
//!
//!   0  every selected account was processed with zero per-account errors
//!      and zero accounts left `incomplete` (see below).
//!   1  a database/connection error occurred, at least one per-account error
//!      occurred, or at least one account holding a real grant was left
//!      un-flipped because seeding did not leave every current member
//!      covered ("incomplete" — finding 1, enforcement-completion PR review
//!      round 2; see [`Totals::incomplete`]). The run does not stop at the
//!      first failure in either case — see "one bad account must not strand
//!      the rest" below. Re-running is safe either way.
//!   2  usage/argument error.
//!
//! ## One bad account must not strand the rest
//!
//! Same posture as `backfill_reef_judgement_from_history::run_one`: a
//! per-account failure (an `issue_grant` error, a `seed_member_period`
//! error) is recorded and the loop CONTINUES to the next account, rather
//! than aborting the whole pass. The run exits non-zero so the failure is
//! not silently lost, but a single unlucky account never blocks every other
//! account's flip.
//!
//! An account is also never flipped on a pass where seeding left it
//! incomplete, even with zero hard errors — see `process_account`'s own
//! comment for the exact re-check. That account is reported distinctly
//! (`INCOMPLETE` on stderr, tallied in `Totals::incomplete`) and simply
//! tried again on the NEXT run, same as an errored one.

use std::process::ExitCode;

use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use uuid::Uuid;

use coralswarm_backend::billing::{issuance, renewal};
use coralswarm_backend::migrator_database_url;

const USAGE: &str = "usage: backfill_billing_enforcement [--execute] [--account-id <uuid>]";

#[derive(Debug)]
struct Args {
    execute: bool,
    account_id: Option<Uuid>,
}

/// Takes `impl Iterator<Item = String>` (rather than reading
/// `std::env::args()` itself) so the test below exercises this EXACT
/// function against a synthetic argv, instead of a hand-copied restatement
/// of the flag loop that could drift from the real one silently (finding 10,
/// review round 2 — `grant_plan`'s own parser tests already avoid this by
/// testing helper functions directly; this takes the equivalent path for a
/// single-function parser: make the function itself injectable).
fn parse_args(args: impl Iterator<Item = String>) -> Result<Args, String> {
    let mut execute = false;
    let mut account_id = None;
    let mut args = args;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--execute" => execute = true,
            "--account-id" => {
                let v = args.next().ok_or("--account-id requires a value")?;
                account_id = Some(
                    Uuid::parse_str(&v).map_err(|e| format!("--account-id: invalid UUID: {e}"))?,
                );
            }
            "-h" | "--help" => {
                println!("{USAGE}");
                std::process::exit(0);
            }
            other => return Err(format!("unknown argument: {other}")),
        }
    }
    Ok(Args {
        execute,
        account_id,
    })
}

/// One account's row, as much as this backfill needs to decide what to do
/// with it. `clerk_org_id` doubles as the personal/org discriminator for
/// reporting (`None` = personal).
struct AccountRow {
    id: Uuid,
    clerk_org_id: Option<String>,
    billing_enforcement_enabled: bool,
}

async fn list_accounts(pool: &PgPool, only: Option<Uuid>) -> Result<Vec<AccountRow>, sqlx::Error> {
    let rows: Vec<(Uuid, Option<String>, bool)> = if let Some(id) = only {
        sqlx::query_as(
            "SELECT id, clerk_org_id, billing_enforcement_enabled FROM accounts WHERE id = $1",
        )
        .bind(id)
        .fetch_all(pool)
        .await?
    } else {
        sqlx::query_as("SELECT id, clerk_org_id, billing_enforcement_enabled FROM accounts ORDER BY created_at")
            .fetch_all(pool)
            .await?
    };
    Ok(rows
        .into_iter()
        .map(
            |(id, clerk_org_id, billing_enforcement_enabled)| AccountRow {
                id,
                clerk_org_id,
                billing_enforcement_enabled,
            },
        )
        .collect())
}

/// Whether-not this org has been wholesale-revoked
/// (`organization.deleted`, migration `0082`). Always `false` for a personal
/// account (`clerk_org_id IS NULL`).
async fn is_revoked(pool: &PgPool, clerk_org_id: Option<&str>) -> Result<bool, sqlx::Error> {
    let Some(org) = clerk_org_id else {
        return Ok(false);
    };
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM org_revocations WHERE clerk_org_id = $1)")
        .bind(org)
        .fetch_one(pool)
        .await
}

/// `grants` is RLS-covered (`account_isolation`, migration `0150`) — same
/// requirement `billing::issuance`/`billing::renewal`/`billing::enforcement`
/// document on every one of their own reads: an unscoped query against it
/// hard-errors under the least-privilege `coralswarm_app` role rather than
/// silently returning no rows (see `enforcement::read_balance_scoped`'s doc
/// comment for exactly why the cast in the RLS predicate raises even against
/// an empty table). Scoped via `tenancy::scope::begin_account_scoped`, the
/// same primitive every other billing read/write in this tree uses.
async fn has_active_grant(pool: &PgPool, account_id: Uuid) -> Result<bool, sqlx::Error> {
    let mut tx = coralswarm_backend::tenancy::scope::begin_account_scoped(pool, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM grants WHERE account_id = $1 AND revoked_at IS NULL)",
    )
    .bind(account_id)
    .fetch_one(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(exists)
}

async fn member_ids(pool: &PgPool, account_id: Uuid) -> Result<Vec<Uuid>, sqlx::Error> {
    sqlx::query_scalar("SELECT user_id FROM account_members WHERE account_id = $1")
        .bind(account_id)
        .fetch_all(pool)
        .await
}

/// Members with no `member_allowance_periods` row covering "now" — the
/// query shared by [`members_missing_current_period`] (a standalone,
/// read-only count for the dry-run report and the pre-seed snapshot) and
/// [`recheck_coverage_and_maybe_flip`] (the post-seed, lock-protected
/// re-check that gates the enforcement flip). Deliberately independent of
/// whether the window has ever been established (it counts every member
/// with nothing current, whether zero rows exist for this account at all or
/// some already do), which is exactly what `seed_member_period`'s own
/// two-branch window resolution (an existing open period, or a freshly
/// computed one for a zero-row account) will close on `--execute`.
///
/// # `statement_timestamp()`, not `now()` (CodeRabbit re-review, finding 1's
/// audit, applied here per finding 3)
///
/// [`recheck_coverage_and_maybe_flip`] runs this AFTER taking
/// `renewal::lock_period_rotation` — inside a transaction, `now()` is frozen
/// at `BEGIN` and does not advance across the wait a contended lock forces,
/// so a stale `now()` here could under-count "missing" members relative to a
/// `seed_member_period` call that committed WHILE this call was blocked on
/// the lock, in either direction depending on exactly which now-covered
/// window it looks like it's missing. `statement_timestamp()` always
/// reflects this statement's own start time, i.e. genuinely after whatever
/// wait already happened, which is what the lock's serialization is FOR.
/// [`members_missing_current_period`]'s own (unlocked, informational-only)
/// calls get the identical, correct-either-way answer for free by sharing
/// this one query rather than keeping a second near-identical copy that
/// could drift — the same "one formula, one owner" discipline
/// `billing::renewal`'s module doc documents for
/// `PER_MEMBER_GRANT_SQL`/`entitlement::PLAN_ENTITLEMENT_SQL`.
const MEMBERS_MISSING_CURRENT_PERIOD_SQL: &str = "\
    SELECT COUNT(*) FROM account_members am \
     WHERE am.account_id = $1 \
       AND NOT EXISTS ( \
         SELECT 1 FROM member_allowance_periods p \
          WHERE p.account_id = am.account_id AND p.user_id = am.user_id \
            AND p.period_start <= statement_timestamp() AND p.period_end > statement_timestamp() \
       )";

/// Runs [`MEMBERS_MISSING_CURRENT_PERIOD_SQL`] against a connection the
/// caller already owns — a scoped, possibly lock-holding transaction for
/// [`recheck_coverage_and_maybe_flip`], or a fresh scoped one opened by
/// [`members_missing_current_period`] below.
async fn count_missing_current_period(
    conn: &mut sqlx::PgConnection,
    account_id: Uuid,
) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar(MEMBERS_MISSING_CURRENT_PERIOD_SQL)
        .bind(account_id)
        .fetch_one(conn)
        .await
}

/// Members with no `member_allowance_periods` row covering "now" — a
/// read-only count for the dry-run report and the pre-seed snapshot. See
/// [`MEMBERS_MISSING_CURRENT_PERIOD_SQL`]'s own doc comment for why this
/// shares its query with the lock-protected post-seed re-check rather than
/// re-typing it.
///
/// `member_allowance_periods` is RLS-covered too (migration `0161`, not on
/// `scripts/check_rls_coverage.sh`'s own 0083-derived roster — see
/// `billing/issuance.rs`'s manifest comment for that gate's documented blind
/// spot — but genuinely enforced at the database regardless of which static
/// checker tracks it). Scoped for the same reason [`has_active_grant`] is.
///
/// Never gates the flip by itself — it is informational only (the dry-run
/// report, and `missing_before`'s pre-seed snapshot in `process_account`).
/// [`recheck_coverage_and_maybe_flip`] is what the flip actually depends on.
async fn members_missing_current_period(
    pool: &PgPool,
    account_id: Uuid,
) -> Result<i64, sqlx::Error> {
    let mut tx = coralswarm_backend::tenancy::scope::begin_account_scoped(pool, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
    let missing = count_missing_current_period(&mut tx, account_id).await?;
    tx.commit().await?;
    Ok(missing)
}

/// Runs the post-seed coverage re-check AND the conditional enforcement flip
/// inside ONE account-scoped transaction holding `renewal::
/// lock_period_rotation` — CodeRabbit re-review, finding 3 (MAJOR).
///
/// # The race this closes
///
/// Before this fix, [`process_account`] read the post-seed coverage count
/// via a plain, unlocked [`members_missing_current_period`] call, and — in a
/// SEPARATE, later transaction — flipped `accounts.billing_enforcement_enabled`
/// with a bare `UPDATE ... WHERE id = $1`. Neither took the account's
/// period-rotation lock, and the two were not even the same transaction. A
/// membership row that had already committed (so this backfill's roster read
/// saw it) but whose own `seed_member_period` call was still in flight — or
/// had failed and would only succeed on retry — could land in the gap: the
/// coverage read observes it as covered a moment too early (or the flip
/// commits before a slow seed lands), and the account is flipped enforced
/// while that member genuinely has no current-period row. Holding
/// `lock_period_rotation` for the ENTIRE read-then-write here serializes it
/// against every `seed_member_period` call for this account, which takes the
/// identical lock as its own first statement:
///
///   * A seed still in-flight when this transaction starts blocks this
///     transaction until it commits — so by the time the coverage count
///     below runs, that seed's row is either fully committed (and counted)
///     or the seed genuinely failed (and the member is correctly still
///     "missing", so the flip below correctly does not proceed).
///   * A seed that starts AFTER this transaction takes the lock simply
///     blocks until this transaction commits or rolls back, then runs
///     normally against whatever `billing_enforcement_enabled` ended up as
///     — it does not need the flip to have already happened, so there is
///     nothing for it to race.
///
/// # `flip`
///
/// `false` when the caller already knows the flip must not be attempted this
/// pass regardless of what the coverage count turns out to be — an
/// already-`account_errored` account (a per-member `seed_member_period` call
/// failed earlier in this pass; flipping despite that would contradict the
/// "seeding must have actually LEFT every member covered" invariant even if
/// this particular re-check happens to read zero missing), or an account
/// that is already enforced (nothing to flip). The coverage count is still
/// computed and returned either way — [`process_account`] needs `missing_after`
/// for its own `INCOMPLETE` reporting regardless of whether a flip was ever
/// on the table this pass.
///
/// # Dry-run
///
/// Never called in dry-run mode — [`process_account`] returns before reaching
/// this function whenever `execute` is `false`, so dry-run's report continues
/// to come entirely from the unlocked, read-only
/// [`members_missing_current_period`]/[`has_active_grant`] snapshot, exactly
/// as before this fix. A dry run performs no writes and therefore has no flip
/// to serialize in the first place.
///
/// # Unbounded lock wait (CodeRabbit re-review, finding 4: MINOR)
///
/// `begin_account_scoped` sets no `lock_timeout` of its own (it only opens
/// the transaction and sets the RLS scoping GUC — see its own doc comment),
/// so without the `SET LOCAL lock_timeout` immediately below,
/// [`renewal::lock_period_rotation`] would wait on the advisory lock
/// FOREVER if some other actor (most plausibly a `seed_member_period` call
/// that is itself stuck — a hung connection, an operator's `psql` session
/// left inside a transaction holding it, or a genuine deadlock elsewhere)
/// never releases it. An unattended backfill run has no operator watching to
/// notice a hang and Ctrl-C it, so an unbounded wait here would silently
/// turn "one stuck account" into "the entire run never terminates". Ten
/// seconds is chosen as long enough that a merely slow, still-progressing
/// seed pass for a large org (this account's own contention, not a hang)
/// comfortably clears it, while short enough that a genuinely stuck holder
/// does not stall this backfill's overall wall-clock time noticeably against
/// the cost of just retrying the account on the next run.
///
/// On a lock-timeout specifically (Postgres `55P03 lock_not_available`, the
/// SQLSTATE `SET LOCAL lock_timeout` produces), this function does NOT
/// propagate a generic `Err` the way every other failure here does — it
/// rolls back and returns `Ok((None, false))` instead: fail-closed (no
/// flip), but reported through the SAME "coverage unknown" shape
/// [`process_account`] uses for a generic re-check failure too (the
/// `missing_after: Option<i64>` shape — finding 5), rather than being
/// counted as a distinct per-account processing error. A stuck lock is
/// contention, expected to
/// clear on a later run, not a bug in this backfill; folding it into the
/// same `errors` bucket the way an unexpected SQL failure is would make a
/// perfectly ordinary "try again later" outcome look like something broke.
/// Every OTHER `sqlx::Error` from this function (a genuinely failed query,
/// a connection drop, ...) still propagates via `?` exactly as before this
/// fix, and [`process_account`]'s existing `Err` arm still reports and
/// counts those as errors.
async fn recheck_coverage_and_maybe_flip(
    pool: &PgPool,
    account_id: Uuid,
    flip: bool,
) -> Result<(Option<i64>, bool), sqlx::Error> {
    let mut tx = coralswarm_backend::tenancy::scope::begin_account_scoped(pool, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;

    // Bound the wait below — see this function's own doc comment
    // ("Unbounded lock wait", finding 4) for why `begin_account_scoped`
    // alone does not already do this and why 10s was chosen. `SET LOCAL`
    // (not `SET`) so this never outlives the transaction/connection this
    // pool hands back.
    sqlx::query("SET LOCAL lock_timeout = '10s'")
        .execute(&mut *tx)
        .await?;

    // Step 1: the SAME per-account advisory lock every `seed_member_period`
    // call takes as ITS first statement — see that function's and
    // `lock_period_rotation`'s own doc comments for the general shape of
    // the race this closes. First statement here too, before any read of
    // `member_allowance_periods`, so the lock actually guards the coverage
    // decision rather than being taken after the race window already
    // closed on its own.
    if let Err(e) = renewal::lock_period_rotation(&mut tx, account_id).await {
        if is_lock_timeout(&e) {
            // Fail closed: unknown coverage, no flip — see this function's
            // own doc comment for why this is deliberately NOT propagated
            // as a generic `Err`. Best-effort rollback: the transaction has
            // written nothing yet (this is its first statement to actually
            // touch account-scoped state beyond the RLS GUC), and the
            // connection is about to be dropped/returned to the pool
            // either way, so a rollback failure here is not worth escalating
            // over.
            let _ = tx.rollback().await;
            return Ok((None, false));
        }
        return Err(e);
    }

    let missing_after = count_missing_current_period(&mut tx, account_id).await?;

    let mut flipped = false;
    if flip && missing_after == 0 {
        let res = sqlx::query(
            "UPDATE accounts SET billing_enforcement_enabled = true \
              WHERE id = $1 AND NOT billing_enforcement_enabled",
        )
        .bind(account_id)
        .execute(&mut *tx)
        .await?;
        flipped = res.rows_affected() > 0;
    }

    tx.commit().await?;
    Ok((Some(missing_after), flipped))
}

/// Whether `e` is Postgres's `55P03 lock_not_available` — the error a `SET
/// LOCAL lock_timeout` produces when a lock wait exceeds it, distinct from
/// `57014 query_canceled` (statement_timeout/user cancel) or any other
/// database error. [`recheck_coverage_and_maybe_flip`] uses this to route
/// specifically a timed-out lock wait onto its fail-closed "unknown
/// coverage" outcome rather than its generic error propagation — see that
/// function's own doc comment (finding 4).
fn is_lock_timeout(e: &sqlx::Error) -> bool {
    e.as_database_error()
        .is_some_and(|d| d.code().as_deref() == Some("55P03"))
}

/// Tallies across the whole run, printed as the final summary — same
/// "counts, not just a pass/fail" discipline as
/// `judgement_backfill::BackfillCounts`.
#[derive(Default)]
struct Totals {
    accounts_seen: u64,
    skipped_revoked: u64,
    granted: u64,
    seeded_rows: u64,
    flipped: u64,
    already_complete: u64,
    /// Accounts that hold a real grant but were deliberately NOT flipped this
    /// pass because seeding did not leave every current member with a
    /// current-period row (finding 1, review round 2) — either a per-member
    /// `seed_member_period` call errored, or one still has no current-period
    /// row even after a clean seed pass (e.g. a lapsed account with no open
    /// period to seed into — see `renewal::seed_member_period`'s "lapsed
    /// account" case). Distinct from `errors`: an account can land here with
    /// zero per-account errors, if seeding was clean but genuinely could not
    /// establish coverage for everyone. Forces a non-zero exit so the gap is
    /// never silently reported as done.
    incomplete: u64,
    errors: u64,
}

async fn free_plan(pool: &PgPool) -> Result<issuance::PlanRow, String> {
    issuance::resolve_active_plan(pool, "free")
        .await
        .map_err(|e| format!("free-plan lookup failed: {e}"))?
        .ok_or_else(|| {
            "no ACTIVE plan with code \"free\" (see migration 0153_billing_plans_seed.sql)"
                .to_string()
        })
}

async fn process_account(
    pool: &PgPool,
    account: &AccountRow,
    free: &issuance::PlanRow,
    execute: bool,
    totals: &mut Totals,
) {
    totals.accounts_seen += 1;
    let label = match &account.clerk_org_id {
        Some(org) => format!("org:{org}"),
        None => "personal".to_string(),
    };

    let revoked = match is_revoked(pool, account.clerk_org_id.as_deref()).await {
        Ok(r) => r,
        Err(e) => {
            eprintln!(
                "ERROR account={} ({label}): revocation check failed: {e}",
                account.id
            );
            totals.errors += 1;
            return;
        }
    };
    if revoked {
        println!("account={} ({label}): SKIP — org is revoked", account.id);
        totals.skipped_revoked += 1;
        return;
    }

    let mut has_grant = match has_active_grant(pool, account.id).await {
        Ok(g) => g,
        Err(e) => {
            eprintln!(
                "ERROR account={} ({label}): grant lookup failed: {e}",
                account.id
            );
            totals.errors += 1;
            return;
        }
    };
    let would_grant = !has_grant;

    let missing_before = match members_missing_current_period(pool, account.id).await {
        Ok(n) => n,
        Err(e) => {
            eprintln!(
                "ERROR account={} ({label}): period-coverage check failed: {e}",
                account.id
            );
            totals.errors += 1;
            return;
        }
    };
    let would_flip = !account.billing_enforcement_enabled;

    if !execute {
        if !would_grant && missing_before == 0 && !would_flip {
            println!("account={} ({label}): already-complete", account.id);
            totals.already_complete += 1;
        } else {
            println!(
                "account={} ({label}): would-grant={} would-seed={} would-flip={}",
                account.id, would_grant, missing_before, would_flip
            );
        }
        return;
    }

    let mut account_errored = false;
    // Rows the grant's OWN internal roll already seeded (finding 7, review
    // round 2) — `issue_grant` rolls `roll_period` for every CURRENT member
    // in the same transaction that writes the `grants` row, so a grantless
    // account's members are seeded THERE, not by the per-member loop below
    // (which then correctly no-ops on all of them via `seed_member_period`'s
    // own idempotent `ON CONFLICT`). Folding this count into
    // `seeded_this_account` is what makes the `--execute` tally match the
    // dry-run's `would-seed` prediction — both now count "how many rows this
    // run actually caused to exist" the same way, and `members_missing_current_period`'s
    // dry-run count is grant-status-agnostic (finding 7's "verify the
    // dry-run prediction logic counts the same way").
    let mut rolled_rows_from_grant = 0u64;

    if would_grant {
        match issuance::issue_grant(
            pool,
            account.id,
            free,
            None,
            None,
            Some("backfill: grant-if-grantless (enforcement-completion PR)"),
        )
        .await
        {
            Ok(issuance::GrantOutcome::Granted(outcome)) => {
                has_grant = true;
                totals.granted += 1;
                rolled_rows_from_grant = outcome.rows_granted;
            }
            Ok(issuance::GrantOutcome::AlreadyGranted) => {
                // Raced by a concurrent grant between the `has_active_grant`
                // check above and this call — nothing was rolled by THIS
                // call, so nothing to fold in here.
                has_grant = true;
                totals.granted += 1;
            }
            Err(e) => {
                eprintln!(
                    "ERROR account={} ({label}): issue_grant failed: {e}",
                    account.id
                );
                totals.errors += 1;
                account_errored = true;
            }
        }
    }

    let mut seeded_this_account = rolled_rows_from_grant;
    if has_grant {
        match member_ids(pool, account.id).await {
            Ok(members) => {
                for user_id in members {
                    match renewal::seed_member_period(pool, account.id, user_id).await {
                        Ok(true) => seeded_this_account += 1,
                        Ok(false) => {}
                        Err(e) => {
                            eprintln!(
                                "ERROR account={} ({label}) user={user_id}: seed_member_period failed: {e}",
                                account.id
                            );
                            totals.errors += 1;
                            account_errored = true;
                        }
                    }
                }
            }
            Err(e) => {
                eprintln!(
                    "ERROR account={} ({label}): member lookup failed: {e}",
                    account.id
                );
                totals.errors += 1;
                account_errored = true;
            }
        }
    }
    totals.seeded_rows += seeded_this_account;

    // Finding 1 (review round 2): the flip must be gated on seeding having
    // actually LEFT every current member covered, not merely on `has_grant`.
    // A grant existing is necessary but not sufficient — a per-member seed
    // can fail (`account_errored`), or (rarer, but real: `renewal::
    // seed_member_period`'s own "lapsed account" case) genuinely have
    // nothing to seed into for a member despite an active grant existing.
    // Re-checking coverage AFTER the seed pass, rather than trusting the
    // pre-seed `missing_before` count, is what catches both.
    //
    // Finding 3 (CodeRabbit re-review, MAJOR): this re-check and the flip it
    // gates must not be two separate, unlocked reads/writes — see
    // `recheck_coverage_and_maybe_flip`'s own doc comment for the in-flight-
    // seed race that leaves a member unseeded on an already-flipped account
    // otherwise. `flip_candidate` mirrors exactly the pair of conditions the
    // OLD, separate flip block below used to gate on
    // (`has_grant && !account.billing_enforcement_enabled && !account_errored`,
    // the last of which was previously folded into `seeding_complete`
    // instead) — computed here, BEFORE the locked re-check, purely to decide
    // whether that re-check should even attempt the write half of its job;
    // the coverage COUNT itself is always computed regardless, since
    // `missing_after` is needed for the `INCOMPLETE` report below whether or
    // not a flip was ever on the table this pass.
    let flip_candidate = has_grant && !account_errored && !account.billing_enforcement_enabled;
    let (missing_after, flipped_from_recheck): (Option<i64>, bool) = if has_grant {
        match recheck_coverage_and_maybe_flip(pool, account.id, flip_candidate).await {
            Ok((n, flipped)) => (n, flipped),
            Err(e) => {
                eprintln!(
                    "ERROR account={} ({label}): post-seed coverage/flip check failed: {e}",
                    account.id
                );
                totals.errors += 1;
                account_errored = true;
                // Fail closed: an unknown post-seed coverage state must
                // never let the flip proceed. `None`, not a fabricated
                // sentinel count (CodeRabbit re-review, finding 5: MINOR) —
                // this branch genuinely does not know how many members (if
                // any) still lack a current-period row; the OLD `(1, false)`
                // here read as a real, specific count of exactly one missing
                // member in every log line and exit-code accounting that
                // touched it, when the true number could be zero, one, or
                // the account's entire roster. `missing_after`'s type makes
                // "unknown" a distinct, unrepresentable-as-a-number state
                // rather than a number this call made up.
                (None, false)
            }
        }
    } else {
        (Some(missing_before), false)
    };
    // `missing_after == Some(0)`, not `== 0`: `None` (unknown coverage, from
    // either this function's own re-check failure above or
    // `recheck_coverage_and_maybe_flip`'s lock-timeout fail-closed path —
    // finding 4) must never compare equal to a real, confirmed zero. Both
    // are "not proven complete", but only `Some(0)` is proven complete, and
    // conflating the two would let an account whose coverage is genuinely
    // UNKNOWN report as `seeding_complete`.
    let seeding_complete = !account_errored && missing_after == Some(0);

    // Finding 2 (enforcement-completion PR review round 2): an account left
    // with `missing_after > 0` after the seed pass must be counted
    // `incomplete` and keep the run's exit code non-zero REGARDLESS of
    // whether `billing_enforcement_enabled` is already `true`. The old gate
    // below only ever set `incomplete_this_account` inside the
    // "currently-disabled" branch, so an ALREADY-enabled account that still
    // had `missing_after > 0` (a `seed_member_period` call that legitimately
    // resolved nothing to seed — the "lapsed account" case its own doc
    // comment documents — or any other partial-coverage state) fell through
    // to the ordinary per-account print below and this binary could exit `0`
    // while a real member sat unseeded. This classification is evaluated
    // FIRST, independently of the flip, so `totals.incomplete` and the
    // printed `INCOMPLETE` line reflect coverage state alone.
    let incomplete_this_account = has_grant && !seeding_complete;
    if incomplete_this_account {
        totals.incomplete += 1;
        // `missing_after` renders as the real count when it is known, or as
        // an explicit "unknown" when it is not (CodeRabbit re-review,
        // finding 5) — never a fabricated number standing in for a count
        // this pass never actually confirmed.
        let missing_after_display = match missing_after {
            Some(n) => n.to_string(),
            None if account_errored => "unknown (coverage re-check failed)".to_string(),
            None => {
                "unknown (coverage re-check skipped: period-rotation lock timed out)".to_string()
            }
        };
        eprintln!(
            "INCOMPLETE account={} ({label}): {missing_after_display} member(s) still lack a \
             current allowance period after seeding (account_errored={account_errored}, \
             billing_enforcement_enabled={}); re-run this backfill to retry",
            account.id, account.billing_enforcement_enabled
        );
    }

    // The flip itself already happened (or didn't) inside
    // `recheck_coverage_and_maybe_flip` above, atomically with the coverage
    // count it depends on — finding 3. `flipped_from_recheck` reports
    // exactly what that locked transaction did; there is no separate UPDATE
    // to perform here any more, only the same tallying/reporting this block
    // always did. Gated on the ORIGINAL pair of conditions — currently
    // disabled AND seeding complete — exactly as before:
    // `flip_candidate`/`recheck_coverage_and_maybe_flip`'s own `missing_after
    // == 0` check together reproduce `has_grant && !account_errored &&
    // !account.billing_enforcement_enabled && seeding_complete` bit for bit,
    // so `flipped_from_recheck` can only ever be `true` when that same
    // conjunction held. `incomplete_this_account` above is computed
    // independently rather than as a side effect of this branch.
    let flipped_this_account = flipped_from_recheck;
    if flipped_this_account {
        totals.flipped += 1;
    }
    // A `has_grant && !billing_enforcement_enabled` account that did NOT
    // flip this pass needs no extra message here: either
    // `incomplete_this_account` already reported it above, or this pass's
    // own coverage/flip re-check errored (already reported and counted
    // above too). Only the no-grant-at-all case gets its own message, since
    // `!has_grant` is the one shape `incomplete_this_account` never covers
    // (it is gated on `has_grant`, see its own comment above).
    if !has_grant && would_flip {
        // Mirrors `clerk_provision::provision_billing`'s own invariant: never
        // flip an account with nothing to spend. Reachable only when
        // `issue_grant` itself failed above (already reported as an error)
        // or the "free" plan resolved but the grant attempt was skipped for
        // some other reason — either way, loud, not silently skipped.
        eprintln!(
            "WARNING account={} ({label}): enforcement NOT flipped — no active grant could be \
             established",
            account.id
        );
    }

    if !account_errored && !incomplete_this_account {
        if !would_grant && seeded_this_account == 0 && !flipped_this_account && missing_before == 0
        {
            totals.already_complete += 1;
            println!("account={} ({label}): already-complete", account.id);
        } else {
            println!(
                "account={} ({label}): granted={} seeded={} flipped={}",
                account.id, would_grant, seeded_this_account, flipped_this_account
            );
        }
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let args = match parse_args(std::env::args().skip(1)) {
        Ok(a) => a,
        Err(e) => {
            eprintln!("{e}");
            eprintln!("{USAGE}");
            return ExitCode::from(2);
        }
    };

    let dsn = match migrator_database_url() {
        Some(u) => u,
        None => {
            eprintln!("MIGRATOR_DATABASE_URL (preferred) or DATABASE_URL must be set");
            return ExitCode::from(2);
        }
    };
    let pool = match PgPoolOptions::new().max_connections(4).connect(&dsn).await {
        Ok(p) => p,
        Err(e) => {
            eprintln!("failed to connect: {e}");
            return ExitCode::FAILURE;
        }
    };

    println!(
        "backfill_billing_enforcement — mode={}",
        if args.execute { "EXECUTE" } else { "DRY RUN" }
    );
    if let Some(id) = args.account_id {
        println!("  narrowed to account {id}");
    }

    let free = match free_plan(&pool).await {
        Ok(p) => p,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::FAILURE;
        }
    };

    let accounts = match list_accounts(&pool, args.account_id).await {
        Ok(a) => a,
        Err(e) => {
            eprintln!("failed to enumerate accounts: {e}");
            return ExitCode::FAILURE;
        }
    };
    if accounts.is_empty() {
        if let Some(id) = args.account_id {
            eprintln!("no account with id {id}");
            return ExitCode::from(2);
        }
        println!("no accounts found");
    }

    let mut totals = Totals::default();
    for account in &accounts {
        process_account(&pool, account, &free, args.execute, &mut totals).await;
    }

    println!(
        "summary: accounts={} skipped_revoked={} granted={} seeded_rows={} flipped={} \
         already_complete={} incomplete={} errors={}",
        totals.accounts_seen,
        totals.skipped_revoked,
        totals.granted,
        totals.seeded_rows,
        totals.flipped,
        totals.already_complete,
        totals.incomplete,
        totals.errors
    );
    if !args.execute {
        println!(
            "Nothing was written. Read the tallies above, then re-run with --execute to apply."
        );
    } else if totals.incomplete > 0 {
        println!(
            "{} account(s) were left un-flipped this pass (see INCOMPLETE lines above) — \
             re-run this backfill to retry them; it is idempotent and safe to re-run at any \
             time.",
            totals.incomplete
        );
    }

    // Finding 1 (review round 2): an incomplete account must keep the exit
    // code non-zero, same as a hard per-account error — either way the run
    // did not fully clear the gap this binary exists to close, and an
    // automated caller (or a human skimming just the exit code) must not
    // read a clean `0` here.
    if totals.errors > 0 || totals.incomplete > 0 {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `parse_args` needs no database — pure argument-shape tests, same
    /// split as `grant_plan`'s own `#[cfg(test)] mod tests`.
    ///
    /// Calls the REAL `parse_args` against a synthetic argv (finding 10,
    /// review round 2) — `parse_args` takes `impl Iterator<Item = String>`
    /// precisely so this test can do that instead of hand-copying the flag
    /// loop a second time, which proved nothing about the actual parser.
    #[test]
    fn execute_and_account_id_parse_together() {
        let argv = vec![
            "--execute".to_string(),
            "--account-id".to_string(),
            "00000000-0000-0000-0000-000000000001".to_string(),
        ];
        let args = parse_args(argv.into_iter()).expect("must parse");
        assert!(args.execute);
        assert_eq!(
            args.account_id,
            Some(Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap())
        );
    }

    /// An unknown flag must be rejected, not silently ignored — proven
    /// against the real parser now that it is directly callable.
    #[test]
    fn an_unknown_flag_is_rejected() {
        let argv = vec!["--bogus".to_string()];
        let err = parse_args(argv.into_iter()).expect_err("must reject an unknown flag");
        assert!(err.contains("--bogus"), "{err:?}");
    }

    /// `--account-id` with no following value must be a usage error, not a
    /// panic — the real parser's `ok_or` path, exercised directly.
    #[test]
    fn account_id_without_a_value_is_a_usage_error() {
        let argv = vec!["--account-id".to_string()];
        let err = parse_args(argv.into_iter()).expect_err("must reject a missing value");
        assert!(err.contains("--account-id"), "{err:?}");
    }
}
