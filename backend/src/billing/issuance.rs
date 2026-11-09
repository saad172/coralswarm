//! Shared grant-issuance path — the ONE place a `plans` grant is actually
//! written, so `bin/grant_plan` (operator hand-sales) and
//! [`crate::clerk_provision`] (auto-granted `free` at signup, enforcement-
//! rollout PR) issue grants through identical code rather than two
//! hand-typed copies of the same INSERT + period-seed sequence drifting
//! apart. Extracted from `bin/grant_plan.rs`, which owned this logic first —
//! see that binary's own module doc for the fuller "what one grant actually
//! writes" rationale this module inherits unchanged.

use sqlx::types::Decimal;
use sqlx::PgPool;
use uuid::Uuid;

use crate::tenancy::scope::begin_account_scoped;

/// A resolved, active `plans` row — just the columns a grant needs.
#[derive(Debug, Clone)]
pub struct PlanRow {
    pub id: Uuid,
    pub code: String,
    pub credits: Decimal,
    pub is_seat_priced: bool,
    pub min_seats: Option<i32>,
}

/// Resolve an ACTIVE plan by code. `None` when no active plan carries that
/// code — never an error, since "the plan code doesn't exist" is a normal,
/// checkable outcome for a caller to report.
pub async fn resolve_active_plan(pool: &PgPool, code: &str) -> Result<Option<PlanRow>, sqlx::Error> {
    let row: Option<(Uuid, String, Decimal, bool, Option<i32>)> = sqlx::query_as(
        "SELECT id, code, credits, is_seat_priced, min_seats \
         FROM plans WHERE code = $1 AND is_active = true",
    )
    .bind(code)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(id, code, credits, is_seat_priced, min_seats)| PlanRow {
        id,
        code,
        credits,
        is_seat_priced,
        min_seats,
    }))
}

/// An existing NON-REVOKED grant for this `(account_id, plan_id)` pair, if
/// any — what makes re-issuing the same plan to the same account a safe
/// no-op rather than a double-grant.
///
/// Generic over the executor (matching `billing::entitlement::current_plan`'s
/// own reasoning) so [`issue_grant`] can call this TWICE with the identical
/// query: once against `pool` as an unlocked fast-path check before opening a
/// transaction, and once against its own open, lock-holding transaction
/// (`&mut *tx`) to re-check under serialization — see [`issue_grant`]'s
/// in-transaction call site for why the second call exists (CodeRabbit
/// re-review, finding 1).
pub async fn existing_active_grant<'e, E>(
    executor: E,
    account_id: Uuid,
    plan_id: Uuid,
) -> Result<Option<Uuid>, sqlx::Error>
where
    E: sqlx::PgExecutor<'e>,
{
    sqlx::query_scalar(
        "SELECT id FROM grants WHERE account_id = $1 AND plan_id = $2 AND revoked_at IS NULL",
    )
    .bind(account_id)
    .bind(plan_id)
    .fetch_optional(executor)
    .await
}

/// The `[period_start, period_end)` window the FIRST allowance period covers,
/// derived from the plan's own `refresh_kind`/`period_length`. See
/// `bin/grant_plan.rs::resolve_initial_period`'s original doc comment for why
/// a lifetime plan gets a finite (century-long) window rather than
/// `'infinity'` — sqlx's chrono decoder has no representation for an infinite
/// timestamp and errors reading it back out.
///
/// `pub(crate)` (rather than private) so
/// [`crate::billing::renewal::seed_member_period`]'s first-joiner branch can
/// reuse this EXACT formula instead of hand-duplicating it in a second query
/// (finding 9, enforcement-completion PR review round 2) — the two sites
/// compute the identical window for the identical reason (a grant with
/// nobody yet on the roster to spend it), so the formula, including the
/// 100-year lifetime-plan fallback and its rationale, has exactly one owner.
///
/// # `statement_timestamp()`, not `now()` (CodeRabbit re-review, finding 1)
///
/// Both call sites — [`issue_grant`] below and
/// [`crate::billing::renewal::seed_member_period`]'s fresh-window branch —
/// invoke this only AFTER taking
/// [`crate::billing::renewal::lock_period_rotation`]. Inside a Postgres
/// transaction, `now()` is fixed at `BEGIN` and does not advance even while a
/// statement blocks waiting on that advisory lock, so a caller that
/// genuinely contended for the lock would resolve a period boundary from
/// BEFORE it ever got to look — stale by exactly the wait. A concurrent
/// holder's newer period, committed during that wait, would then satisfy
/// `period_start > (stale) boundary` and get destroyed by
/// `renewal::DELETE_UNSTARTED_OPEN_PERIOD_SQL` the moment `close_open_period`
/// ran with it (this was reachable from `issue_grant` specifically, before
/// this fix reordered it to lock first — see that function's own doc
/// comment). `statement_timestamp()` is this statement's own start time, so
/// it always reflects time AFTER whatever this call's caller already waited
/// through, never before. Both callers of this function need exactly that —
/// a boundary decided under the lock, not one decided before it was even
/// acquired — so `now()` would not have been correct here even before either
/// caller took the lock explicitly, since the lock's entire purpose is
/// letting this decision safely reflect "now" as of AFTER contention
/// resolves.
pub(crate) async fn resolve_initial_period(
    tx: &mut sqlx::Transaction<'static, sqlx::Postgres>,
    plan_id: Uuid,
) -> Result<(chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>), sqlx::Error> {
    sqlx::query_as(
        "SELECT statement_timestamp(), \
                statement_timestamp() + COALESCE(p.period_length, interval '100 years') \
           FROM plans p WHERE p.id = $1",
    )
    .bind(plan_id)
    .fetch_one(&mut **tx)
    .await
}

/// What [`issue_grant`] did.
#[derive(Debug)]
pub enum GrantOutcome {
    /// A fresh grant was written, and the outcome of seeding its first
    /// allowance period (per-member credits, member/seat counts) is
    /// reported verbatim rather than interpreted here — same discipline as
    /// [`crate::billing::renewal::RenewalOutcome`] itself.
    Granted(crate::billing::renewal::RenewalOutcome),
    /// This account already holds a non-revoked grant for this exact plan.
    /// Nothing was written — re-issuing the same plan is always a safe
    /// no-op, never a second grant.
    AlreadyGranted,
}

/// Issue `plan` to `account_id`: write the `grants` row, close any
/// still-open prior period, and seed the first `member_allowance_periods`
/// row for every current member — in ONE account-scoped transaction, or not
/// at all.
///
/// Idempotent: an account that already holds a non-revoked grant for this
/// plan gets [`GrantOutcome::AlreadyGranted`] and no write. This is what
/// lets [`crate::clerk_provision::ensure_personal_account`]/
/// [`crate::clerk_provision::ensure_org_account`] call this unconditionally
/// on every provisioning pass (both are themselves idempotent "on first
/// sight" functions) without double-granting a returning account.
///
/// `account_members` for `account_id` MUST already reflect the roster this
/// grant should cover — `roll_period` reads it inside the same transaction,
/// so a caller granting to a personal account must insert the owner's
/// `account_members` row first (as `clerk_provision::ensure_personal_account`
/// already does before this is ever called).
pub async fn issue_grant(
    pool: &PgPool,
    account_id: Uuid,
    plan: &PlanRow,
    seats: Option<i32>,
    credits_override: Option<Decimal>,
    note: Option<&str>,
) -> Result<GrantOutcome, sqlx::Error> {
    // Scoped, not a bare read against `pool` (finding 2, billing self-serve PR
    // D review round) — `grants` is RLS-covered (0150), so an unscoped SELECT
    // under the production `coralswarm_app` role does not return "no rows", it
    // HARD-ERRORS: `current_setting('app.account_id')` reads back as `''`
    // outside any `begin_account_scoped` transaction, and `''::uuid` fails the
    // cast (see `billing::enforcement::read_balance_scoped`'s doc comment for
    // the identical mechanism). Every existing caller of this function
    // (`clerk_provision::auto_grant_free_plan`, `bin/grant_plan`,
    // `bin/backfill_billing_enforcement`) passed this same bare `pool` here
    // too — this was a latent bug across the board, masked because every
    // pg-test suite in this tree runs against an owner/`BYPASSRLS` role. It
    // was only NEWLY REACHED (not newly introduced) by
    // `routes::stripe_webhook`'s `checkout.session.completed` handler, which
    // is what surfaced it. See [`scoped_existing_active_grant`].
    if scoped_existing_active_grant(pool, account_id, plan.id)
        .await?
        .is_some()
    {
        return Ok(GrantOutcome::AlreadyGranted);
    }

    let mut tx = begin_account_scoped(pool, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(format!("issue_grant scoped tx: {e}")))?;

    let outcome = issue_grant_in_tx(&mut tx, account_id, plan, seats, credits_override, note).await?;

    // `issue_grant_in_tx` never commits/rolls back its own transaction — see
    // its own doc comment for why (it is shared with a caller,
    // `routes::stripe_webhook::apply_checkout_completed_subscription`, whose
    // transaction holds an event CLAIM this function knows nothing about and
    // must never roll back on this function's behalf). This is the ONLY
    // caller for which a losing race means the transaction is worth
    // discarding outright — see `GrantOutcome::AlreadyGranted`'s arm below.
    match &outcome {
        GrantOutcome::AlreadyGranted => tx.rollback().await?,
        GrantOutcome::Granted(_) => tx.commit().await?,
    }
    Ok(outcome)
}

/// The core of [`issue_grant`] — lock, re-check, INSERT `grants`, seed the
/// first period — assuming the caller has ALREADY opened an account-scoped
/// transaction (`tenancy::scope::begin_account_scoped`) and owns its
/// commit/rollback.
///
/// **Never commits or rolls back `tx` itself.** [`issue_grant`] is the only
/// caller that owns an otherwise-empty transaction it can safely discard on
/// [`GrantOutcome::AlreadyGranted`]. The other caller,
/// `routes::stripe_webhook::apply_checkout_completed_subscription` (finding
/// 1 + finding 2, billing self-serve PR D review round), opens its OWN
/// account-scoped transaction, claims the `checkout.session.completed` event
/// as the FIRST statement in it, and only THEN calls this function — so that
/// transaction is never empty by the time this function runs, and rolling it
/// back here would silently undo a claim this function has no idea exists.
/// See that call site's own doc comment for the full "claim and work in one
/// transaction" restructuring this split makes possible.
///
/// `pub(crate)`, not private: `routes::stripe_webhook` is a sibling module,
/// not a submodule of `billing::issuance`.
pub(crate) async fn issue_grant_in_tx(
    tx: &mut sqlx::Transaction<'static, sqlx::Postgres>,
    account_id: Uuid,
    plan: &PlanRow,
    seats: Option<i32>,
    credits_override: Option<Decimal>,
    note: Option<&str>,
) -> Result<GrantOutcome, sqlx::Error> {
    // Take the SAME per-account advisory lock `seed_member_period` takes —
    // as the FIRST statement THIS function performs, before EVEN the
    // `grants` INSERT below, and specifically before `resolve_initial_period`
    // (CodeRabbit re-review, finding 1: CRITICAL). Note this is not
    // necessarily the first statement of the ENCLOSING transaction any more
    // — `apply_checkout_completed_subscription`'s claim INSERT runs before
    // this function is even called — but the ordering THIS lock exists to
    // guarantee (lock before any time-based decision or `grants`/
    // `member_allowance_periods` write) is unaffected: a claim INSERT into
    // `stripe_webhook_events` touches neither table.
    //
    // # Why this had to move ahead of `resolve_initial_period`
    //
    // The lock used to be taken AFTER `resolve_initial_period` resolved
    // `period_start`/`period_end` — safe for the double-grant race that
    // originally motivated it (finding 1, enforcement-completion PR review
    // round 2: a concurrent `seed_member_period` inserting into a window
    // this call is about to close), but not for a SECOND, sharper race the
    // re-review caught: `resolve_initial_period`'s boundary was resolved
    // from a Postgres `now()` that is frozen at this transaction's `BEGIN`.
    // If this call then had to WAIT on the lock — because another roller
    // already held it — and that other roller committed a genuinely NEWER
    // period while this transaction sat blocked, the boundary this
    // transaction eventually used to call `close_open_period` would still be
    // the STALE, pre-wait timestamp. `close_open_period`'s
    // `DELETE_UNSTARTED_OPEN_PERIOD_SQL` arm deletes any row whose
    // `period_start` is strictly after its boundary — so the newer,
    // already-committed period (and every consumption row against it) would
    // be destroyed by the very call meant to make room for THIS account's
    // period, not the other one.
    //
    // Taking the lock first closes that: `resolve_initial_period` (below)
    // now always runs AFTER any wait this call had to do, and its SQL uses
    // `statement_timestamp()` rather than `now()` for exactly this reason —
    // see that function's own doc comment. Moving the lock ahead of the
    // `grants` INSERT too (rather than only ahead of
    // `resolve_initial_period`) is not required by this specific race — the
    // INSERT writes no time-based decision this bug depends on — but doing
    // so anyway costs nothing (`pg_advisory_xact_lock` touches no table row)
    // and keeps this function's shape matching `seed_member_period`'s own
    // "lock first, read second" discipline exactly, rather than carving out
    // a special case for one INSERT.
    //
    // Without this reordering, a concurrent `seed_member_period` call for a
    // member of this account could ALSO still race the close/roll below to
    // completion and insert its own row into what is now a CLOSED window —
    // abutting rather than overlapping the fresh one this call opens, so
    // `0163`'s exclusion constraint never catches it — which is the
    // ORIGINAL double-grant this lock was added to close. See
    // `renewal::lock_period_rotation`'s own doc comment for that race in
    // full.
    crate::billing::renewal::lock_period_rotation(tx, account_id).await?;

    // Re-check for a non-revoked grant of this SAME plan, now INSIDE the
    // transaction and AFTER the lock — CodeRabbit re-review, finding 1:
    // MAJOR (TOCTOU on the duplicate-grant check).
    //
    // # Why the pre-transaction check in [`issue_grant`] is not enough by
    // # itself
    //
    // [`scoped_existing_active_grant`]/`existing_active_grant` called before
    // this function reads a moment-in-time snapshot — no lock, nothing
    // serializing it against a concurrent caller. Two concurrent issuance
    // attempts for the SAME `(account_id, plan_id)` — a provisioning retry
    // racing `bin/grant_plan`, or the backfill racing a webhook, both
    // entirely realistic given this function's callers — can both read "no
    // existing grant" before either has written anything, both pass that
    // check, and both then go on to take `lock_period_rotation` above and
    // serialize on IT instead. The first to acquire it proceeds exactly as
    // an uncontended call would: INSERT `grants`, close any open period,
    // roll a fresh one. The SECOND then wakes up holding the lock with a
    // transaction that has already decided (via the pre-transaction check,
    // now stale) that it should insert a grant — nothing in the schema
    // stops a second non-revoked `grants` row for the same
    // `(account_id, plan_id)` pair (unlike `0161`'s UNIQUE index on
    // `member_allowance_periods`, there is no equivalent constraint on
    // `grants` itself — see `0150`'s header, which deliberately allows
    // multiple simultaneous non-revoked grants for DIFFERENT plans on one
    // account, and a uniqueness constraint scoped to
    // `(account_id, plan_id, revoked_at IS NULL)` was never added because
    // this call path was assumed to be the only writer and assumed
    // race-free). So the second caller inserts a SECOND grant row, then
    // proceeds to `close_open_period`/`roll_period` — churning the period
    // the FIRST caller just committed, right after it was correctly rolled,
    // for no reason other than losing a race neither caller could observe.
    //
    // # Why re-checking here, under the lock, closes it
    //
    // `lock_period_rotation` above is the SAME per-account advisory lock
    // every writer of this account's `grants`/`member_allowance_periods`
    // state takes as its first transactional act (see that function's own
    // doc comment). By the time this SELECT runs, any concurrent issuance
    // attempt for this account has either already committed (so its
    // `grants` row, if any, is visible here) or is still blocked behind
    // this same lock (so it cannot race this transaction's decision at all).
    // A re-check here therefore sees the TRUE current state, not a stale
    // snapshot from before either caller decided anything — closing exactly
    // the window the pre-transaction check cannot close on its own.
    if let Some(existing_id) = existing_active_grant(&mut **tx, account_id, plan.id).await? {
        // Someone else won the race. Unlike `issue_grant`'s own (empty at
        // this point) transaction, THIS caller's transaction is not
        // necessarily empty — `apply_checkout_completed_subscription`'s
        // already holds a committed-worthy event claim — so this function
        // must NOT roll anything back; it just reports the loss and lets the
        // caller decide what to do with its own transaction.
        tracing::debug!(
            %account_id, plan_id = %plan.id, %existing_id,
            "issue_grant_in_tx: lost a concurrent issuance race — another caller already \
             granted this exact plan; no-op"
        );
        return Ok(GrantOutcome::AlreadyGranted);
    }

    sqlx::query(
        "INSERT INTO grants \
             (account_id, plan_id, credits_override, seats, plan_is_seat_priced, note) \
         VALUES ($1, $2, $3, $4, $5, $6)",
    )
    .bind(account_id)
    .bind(plan.id)
    .bind(credits_override)
    .bind(seats)
    .bind(plan.is_seat_priced)
    .bind(note)
    .execute(&mut **tx)
    .await?;

    let (period_start, period_end) = resolve_initial_period(tx, plan.id).await?;

    // Close any period this account is still holding open BEFORE seeding the
    // new one — 0163's exclusion constraint makes an overlapping seed a hard
    // error rather than a silent double-insert. See
    // `bin/grant_plan.rs::write_grant`'s identical comment for why this
    // matters even though the re-check above already refused a SECOND grant
    // of the SAME plan: a caller that grants a DIFFERENT plan onto an
    // account still holding an open period from a prior one reaches here.
    crate::billing::renewal::close_open_period(tx, account_id, period_start).await?;

    let outcome =
        crate::billing::renewal::roll_period(tx, account_id, period_start, period_end).await?;

    Ok(GrantOutcome::Granted(outcome))
}

/// [`existing_active_grant`], scoped — the fast-path check
/// [`issue_grant`] runs BEFORE opening its own transaction. See that
/// call site's own comment for why this must be scoped rather than a bare
/// read against `pool`.
async fn scoped_existing_active_grant(
    pool: &PgPool,
    account_id: Uuid,
    plan_id: Uuid,
) -> Result<Option<Uuid>, sqlx::Error> {
    let mut tx = begin_account_scoped(pool, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(format!("scoped_existing_active_grant tx: {e}")))?;
    let existing = existing_active_grant(&mut *tx, account_id, plan_id).await?;
    // Read-only — commit rather than let it implicitly roll back on drop,
    // matching `billing::enforcement::read_balance_scoped`'s identical shape.
    tx.commit().await?;
    Ok(existing)
}

// ── Live-Postgres tests ──────────────────────────────────────────────────────

/// `issue_grant`, proven against real Postgres — same
/// `RELINFER_TEST_DATABASE_URL`-gated discipline as
/// `billing::renewal::pg_tests` (see that module's doc comment for why every
/// billing suite in this tree is live-DB rather than mocked).
///
/// This module did not exist before CodeRabbit re-review finding 1: the
/// concurrency-mechanism regression test itself lives in
/// `billing::renewal::pg_tests` (it exercises `close_open_period` directly,
/// which already has a full fixture kit there) — see that test's own doc
/// comment for why a true two-connection blocking reproduction was set aside
/// in favor of the documented sequential fallback. What belongs HERE is a
/// plain end-to-end sanity check that `issue_grant` still does its ordinary
/// job correctly after finding 1's reordering (`lock_period_rotation` moved
/// ahead of both the `grants` INSERT and `resolve_initial_period`) — a purely
/// mechanical reorder of three statements inside one transaction is exactly
/// the kind of change that silently drops a statement or reverses two
/// data-dependent ones, and nothing above proved the reordered function still
/// grants.
///
/// Run with:
///   RELINFER_TEST_DATABASE_URL=postgres://localhost:5432/postgres \
///     cargo test -p coralswarm-backend --lib "billing::issuance::" -- --test-threads=1
#[cfg(test)]
mod pg_tests {

    async fn execute_financial_fixture<'q>(
        pool: &sqlx::PgPool,
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

    use super::*;
    use sqlx::PgPool;

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

    /// A one-member account with nothing granted yet — same shape
    /// `renewal::pg_tests::seed_account` builds, duplicated here rather than
    /// shared across modules because `renewal::pg_tests` is a private
    /// `#[cfg(test)]` module with no `pub` surface of its own (matching the
    /// established per-file pg-test-fixture pattern already used by
    /// `renewal.rs` and `routes/stripe_webhook.rs`, each of which keeps its
    /// own `seed_account`/`cleanup` rather than sharing one).
    async fn seed_account(pool: &PgPool) -> (Uuid, Uuid) {
        let owner = Uuid::new_v4();
        sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
            .bind(owner)
            .bind(format!("issuance-{}@test.example", owner.simple()))
            .execute(pool)
            .await
            .expect("create test user");
        let account_id = Uuid::new_v4();
        sqlx::query("INSERT INTO accounts (id, owner_user_id) VALUES ($1, $2)")
            .bind(account_id)
            .bind(owner)
            .execute(pool)
            .await
            .expect("create test account");
        sqlx::query(
            "INSERT INTO account_members (account_id, user_id, role) VALUES ($1, $2, 'owner')",
        )
        .bind(account_id)
        .bind(owner)
        .execute(pool)
        .await
        .expect("create account_members row");
        (account_id, owner)
    }

    async fn cleanup(pool: &PgPool, account_id: Uuid, owner: Uuid) {
        let _ = execute_financial_fixture(
            pool,
            account_id,
            sqlx::query("DELETE FROM accounts WHERE id = $1")
            .bind(account_id),
        )
            .await;
        let _ = sqlx::query("DELETE FROM users WHERE id = $1")
            .bind(owner)
            .execute(pool)
            .await;
    }

    /// The ordinary case, unchanged in outward behavior by finding 1's
    /// reorder: a grantless account granted `free` gets a `grants` row and a
    /// first `member_allowance_periods` row for its one current member, and
    /// the resolved window opens at (real, live) "now" — proving
    /// `resolve_initial_period`'s switch from `now()` to
    /// `statement_timestamp()` still produces a sane, current-time window
    /// for the ordinary uncontended case, not just the contended one the
    /// `renewal::pg_tests` mechanism test covers.
    #[tokio::test]
    async fn issue_grant_seeds_the_first_period_for_the_current_member() {
        let Some(pool) =
            pg_test_pool("issue_grant_seeds_the_first_period_for_the_current_member").await
        else {
            return;
        };
        let (account_id, owner) = seed_account(&pool).await;
        let plan = resolve_active_plan(&pool, "free")
            .await
            .expect("plan lookup")
            .expect("free plan must be seeded (migration 0153)");

        // Server-side bounds, not `chrono::Utc::now()` host-clock reads: the
        // assertion below compares against `period_start`, which
        // `resolve_initial_period` derives from Postgres's
        // `statement_timestamp()` (this module's own doc comment, above).
        // Bounding with the HOST clock instead would compare two different
        // clocks and could spuriously fail under real clock skew between the
        // test process and the database server; reading the bounds from the
        // server itself removes the skew entirely.
        let before: chrono::DateTime<chrono::Utc> =
            sqlx::query_scalar("SELECT statement_timestamp()")
                .fetch_one(&pool)
                .await
                .expect("read statement_timestamp() before the call");
        let outcome = issue_grant(&pool, account_id, &plan, None, None, Some("pg_tests"))
            .await
            .expect("issue_grant");
        let after: chrono::DateTime<chrono::Utc> =
            sqlx::query_scalar("SELECT statement_timestamp()")
                .fetch_one(&pool)
                .await
                .expect("read statement_timestamp() after the call");

        let renewal = match outcome {
            GrantOutcome::Granted(r) => r,
            GrantOutcome::AlreadyGranted => panic!("a grantless account must actually be granted"),
        };
        assert_eq!(
            renewal.rows_granted, 1,
            "one row for the one current member"
        );
        assert_eq!(renewal.members, 1);
        assert!(!renewal.no_active_plan());

        let (period_start, period_end, granted): (
            chrono::DateTime<chrono::Utc>,
            chrono::DateTime<chrono::Utc>,
            Decimal,
        ) = sqlx::query_as(
            "SELECT period_start, period_end, granted_credits FROM member_allowance_periods \
             WHERE account_id = $1 AND user_id = $2",
        )
        .bind(account_id)
        .bind(owner)
        .fetch_one(&pool)
        .await
        .expect("read the seeded row");

        assert!(
            period_start >= before && period_start <= after,
            "the window must open at LIVE current time (statement_timestamp() taken after the \
             lock, evaluated here in the uncontended case), not some other timestamp: {period_start}"
        );
        assert!(
            period_end > period_start,
            "period_end must be strictly after period_start"
        );
        assert_eq!(
            granted,
            Decimal::from(1000),
            "the free plan's flat allowance (migration 0153)"
        );

        cleanup(&pool, account_id, owner).await;
    }

    /// Re-issuing the SAME plan to an account that already holds a
    /// non-revoked grant for it must stay a safe no-op — proving the
    /// early-return `existing_active_grant` check (which runs BEFORE the
    /// reordered lock/insert/resolve sequence, and is therefore untouched by
    /// finding 1) still short-circuits correctly and never reaches the
    /// reordered code at all.
    #[tokio::test]
    async fn re_issuing_the_same_plan_is_a_no_op() {
        let Some(pool) = pg_test_pool("re_issuing_the_same_plan_is_a_no_op").await else {
            return;
        };
        let (account_id, owner) = seed_account(&pool).await;
        let plan = resolve_active_plan(&pool, "free")
            .await
            .expect("plan lookup")
            .expect("free plan must be seeded (migration 0153)");

        issue_grant(&pool, account_id, &plan, None, None, Some("pg_tests"))
            .await
            .expect("first issue_grant");
        let second = issue_grant(&pool, account_id, &plan, None, None, Some("pg_tests"))
            .await
            .expect("second issue_grant");

        assert!(
            matches!(second, GrantOutcome::AlreadyGranted),
            "a second issue_grant for the same plan must no-op, not double-grant"
        );

        let total: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM member_allowance_periods WHERE account_id = $1",
        )
        .bind(account_id)
        .fetch_one(&pool)
        .await
        .expect("count member_allowance_periods");
        assert_eq!(
            total, 1,
            "no duplicate allowance row from the no-op re-issue"
        );

        cleanup(&pool, account_id, owner).await;
    }

    /// **Regression test for finding 1 (MAJOR, CodeRabbit re-review round
    /// 3): TOCTOU on the duplicate-grant check.**
    ///
    /// [`re_issuing_the_same_plan_is_a_no_op`] above only proves the
    /// SEQUENTIAL case — the second `issue_grant` call's PRE-TRANSACTION
    /// `existing_active_grant` fast path already sees the first call's
    /// committed grant, so it never even reaches the lock, let alone the
    /// in-transaction re-check finding 1 added. That leaves the actual bug —
    /// two calls that both pass the unlocked pre-check BEFORE either has
    /// written anything — completely unexercised. This test closes that gap
    /// with a genuine concurrent race rather than another sequential call.
    ///
    /// # Why `tokio::try_join!` on two real `issue_grant` futures, not a
    /// # simulated/sequential fallback
    ///
    /// Unlike `renewal::pg_tests`'s frozen-`now()` regression test (see that
    /// test's own doc comment for why a true two-connection race was set
    /// aside there in favor of a deterministic simulation), this fix does
    /// not depend on timing a specific instant — it depends on Postgres's
    /// own `pg_advisory_xact_lock` serialization actually kicking in, which
    /// requires two REAL, concurrently-live connections/transactions, not a
    /// single connection replaying two states in sequence. `pg_test_pool`
    /// sets `max_connections(4)` (checked: comfortably above the two
    /// concurrent connections this needs), and `tokio::try_join!` polls both
    /// `issue_grant` futures from the same task — each performs several real
    /// `.await`ed round-trips to Postgres, so the executor genuinely
    /// interleaves them rather than running one to completion before
    /// starting the other. Whichever call's transaction reaches
    /// `lock_period_rotation` second then genuinely BLOCKS, at the database
    /// level, on the advisory lock the first is holding — a real
    /// reproduction of the race, not an approximation of one.
    ///
    /// # Why the assertions do not depend on WHICH call wins
    ///
    /// The race is nondeterministic — either call's unlocked pre-check can
    /// happen to run first, and either call's transaction can happen to
    /// acquire the advisory lock first. This test deliberately asserts only
    /// on the OUTCOME SHAPE (exactly one `Granted`, exactly one
    /// `AlreadyGranted`, exactly one `grants` row, exactly one
    /// `member_allowance_periods` row) rather than on which of the two
    /// `note`s (`"pg_tests race A"`/`"pg_tests race B"`) ended up attached to
    /// the surviving row, so it cannot flake based on scheduling.
    #[tokio::test]
    async fn concurrent_issue_grant_calls_for_the_same_plan_never_double_grant() {
        let Some(pool) =
            pg_test_pool("concurrent_issue_grant_calls_for_the_same_plan_never_double_grant").await
        else {
            return;
        };
        let (account_id, owner) = seed_account(&pool).await;
        let plan = resolve_active_plan(&pool, "free")
            .await
            .expect("plan lookup")
            .expect("free plan must be seeded (migration 0153)");

        let (first, second) = tokio::try_join!(
            issue_grant(
                &pool,
                account_id,
                &plan,
                None,
                None,
                Some("pg_tests race A")
            ),
            issue_grant(
                &pool,
                account_id,
                &plan,
                None,
                None,
                Some("pg_tests race B")
            ),
        )
        .expect("neither concurrent issue_grant call should itself error");

        let granted_count = [&first, &second]
            .into_iter()
            .filter(|o| matches!(o, GrantOutcome::Granted(_)))
            .count();
        let already_granted_count = [&first, &second]
            .into_iter()
            .filter(|o| matches!(o, GrantOutcome::AlreadyGranted))
            .count();
        assert_eq!(
            granted_count, 1,
            "exactly ONE of the two racing calls may actually write the grant — finding 1's \
             in-transaction re-check (taken under `lock_period_rotation`) must refuse the loser \
             rather than let both insert: first={first:?} second={second:?}"
        );
        assert_eq!(
            already_granted_count, 1,
            "the loser must observe AlreadyGranted (not error, and not a second Granted): \
             first={first:?} second={second:?}"
        );

        let grants_total: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM grants WHERE account_id = $1 AND revoked_at IS NULL",
        )
        .bind(account_id)
        .fetch_one(&pool)
        .await
        .expect("count non-revoked grants");
        assert_eq!(
            grants_total, 1,
            "exactly one non-revoked grants row must survive the race — a second, TOCTOU-raced \
             INSERT for the same (account_id, plan_id) is exactly what finding 1 closes, and \
             nothing in the `grants` schema itself would reject it"
        );

        let periods_total: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM member_allowance_periods WHERE account_id = $1",
        )
        .bind(account_id)
        .fetch_one(&pool)
        .await
        .expect("count member_allowance_periods");
        assert_eq!(
            periods_total, 1,
            "exactly one allowance period row for the account's one member — the loser's \
             transaction must roll back before ever reaching `close_open_period`/`roll_period`, \
             not seed a second, overlapping or abutting period for the same member"
        );

        cleanup(&pool, account_id, owner).await;
    }
}
