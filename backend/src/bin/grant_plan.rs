//! `grant_plan` — operator one-shot that issues a `plans` grant to a
//! specific account (usage-metering PR10,
//! `.scratch/usage-metering/plan.md` §1 PR10).
//!
//! ## Why this exists
//!
//! "Hand-sales get this script instead of Stripe" (plan §1 PR10 — there is
//! no Stripe integration in this tree, not even webhooks, per the plan's
//! own "Not in scope" list). It is also the ONLY way the Founder/Comp plan
//! (1,000,000 credits, founder-decided 2026-08-13 — see migration
//! `0153_billing_plans_seed.sql`) reaches its two intended recipients:
//! Alex's personal account and `testorg`, nobody else.
//!
//! **No account UUID for either recipient is hardcoded anywhere in this
//! train.** This binary takes a RESOLVABLE IDENTIFIER — an account UUID
//! directly, a personal account's owner email, or an org's
//! `tenant_databases.slug` — and resolves it against the live control-plane
//! DB at run time. See "Running this for Alex + testorg" below for the
//! actual invocations.
//!
//! ## Usage
//!
//! ```text
//! grant_plan --plan <code> (--account-id <uuid> | --email <email> | --org-slug <slug>)
//!     [--seats <n>] [--credits <override>] [--note <text>] [--execute]
//! ```
//!
//! ## `--seats` — required for, and only for, seat-priced plans
//!
//! Migration `0156_grants_seats.sql` gave `grants` a seat count, and the
//! allowance one grant issues is now:
//!
//! ```text
//! COALESCE(credits_override, plans.credits * COALESCE(seats, 1))
//! ```
//!
//! So `--seats` is **required** when the plan is seat-priced
//! (`plans.is_seat_priced` — `team_standard`, `team_premium`; migration
//! `0155_billing_plans_pricing.sql`) and **rejected** when it is not
//! (`free`, `pro`, `founder_comp`). That is not this binary being fussy: it
//! is the same invariant the schema itself enforces via
//! `grants_seats_iff_seat_priced`, checked here first so the operator gets
//! a sentence instead of a constraint violation.
//!
//! `plans.min_seats` (3 for `team_standard`) is enforced here too, and ONLY
//! here — see `0156`'s header for why a policy floor on what may be sold
//! deliberately lives in code rather than in the schema.
//!
//! `--credits` remains an ABSOLUTE total that wins over everything,
//! including the seat multiplication — it is not a per-seat override. A
//! `--seats 10 --credits 5000` invocation grants 5,000 credits, not 50,000.
//!
//! ## `--plan team` is REFUSED, deliberately (billing self-serve launch PR E)
//!
//! `team` (migration `0167`, flipped `is_active = true` by `0170`) is a
//! MIXED-tier umbrella plan: an account can hold some `standard` seats and
//! some `premium` seats simultaneously (plan §1.4), recorded per-tier in
//! `grant_seat_tiers`. This binary's whole interface is built around ONE
//! `--seats <n>` against ONE plan's ONE price point — it has no way to say
//! "3 standard, 2 premium" at all, and `resolve_seats`/`entitled_credits`
//! would happily compute `plan.credits * seats = 0 * n = 0` for it (`team`'s
//! own `plans.credits` is `0` by design — see `0167`'s header, "a real total
//! only exists via `grant_seat_tiers`, a join this binary does not perform").
//! Letting the operator run `--plan team --seats 5 --execute` today would
//! silently issue a ZERO-CREDIT grant with no `grant_seat_tiers` rows behind
//! it at all — the exact "active `team` row is a live footgun" scenario
//! `0167`'s header warned about, reached through a different door once
//! `is_active` flips. [`resolve_plan`] therefore rejects `team` by name,
//! before any seat/credit resolution runs, with a message pointing at PR F
//! (seat lifecycle — not landed as of this PR), which is expected to write
//! `grants` + `grant_seat_tiers` directly rather than route through this
//! binary's single-seats-column shape at all.
//!
//! `--execute` is required to actually write anything; the default is a DRY
//! RUN that resolves the account and the plan, reports what WOULD be
//! granted, and checks whether a grant already exists — writing nothing.
//! Same inverted-default discipline as
//! `backfill_reef_judgement_from_history` (a durable-write one-shot in this
//! same codebase): the safe outcome of a mistyped invocation is a report,
//! not a ledger entry.
//!
//! ## Running this for Alex + testorg (the two Founder/Comp recipients)
//!
//! ```text
//! # Alex's personal account — resolved by the email on his `users` row.
//! DATABASE_URL=... cargo run --bin grant_plan -- \
//!     --plan founder_comp --email <Alex's account email> --execute
//!
//! # testorg — resolved by its tenant_databases.slug.
//! DATABASE_URL=... cargo run --bin grant_plan -- \
//!     --plan founder_comp --org-slug testorg --execute
//! ```
//!
//! Run each once WITHOUT `--execute` first to confirm the resolved account
//! id and the plan's credit amount before committing. Neither invocation
//! above hardcodes an account id — the operator supplies an identifier this
//! binary resolves itself, per the task's explicit instruction not to guess
//! a UUID.
//!
//! ## Idempotency — re-running does NOT double-grant
//!
//! `grants` deliberately carries no DB-level uniqueness constraint on
//! `(account_id, plan_id)` — migration `0150_credit_ledger.sql`'s own
//! header: an account can legitimately hold more than one simultaneous
//! grant (e.g. Free plus a hand-sale top-up), so the schema cannot enforce
//! "at most one grant per plan" globally. This script therefore owns the
//! idempotency check itself: before writing, it looks for an existing
//! NON-REVOKED grant for the SAME `(account_id, plan_id)` and, if one
//! exists, no-ops — reporting "already granted" — rather than issuing a
//! second one. Re-running the exact same invocation is always safe.
//!
//! ## What one grant actually writes
//!
//! In ONE account-scoped transaction (all committed together or not at all):
//!
//!   1. `grants` — the durable "why" (`plan_id`, `granted_at`, `note`,
//!      `credits_override` if `--credits` was passed, `seats` if the plan is
//!      seat-priced, and `plan_is_seat_priced` — the denormalized mirror
//!      migration `0156` uses to keep "seat-priced grant with no seat
//!      count" structurally impossible; see that migration's header).
//!   2. `member_allowance_periods` — one row per CURRENT member at the
//!      per-member allowance, seeded via `billing::renewal::roll_period` so
//!      the formula is not re-typed here. This is what the grant actually
//!      buys the account under the split-bucket model. Immediately before
//!      that seed, `billing::renewal::close_open_period` truncates (or
//!      deletes, for a period that has not started yet) any period this
//!      account is still holding open as of the new one's start — migration
//!      `0163`'s exclusion constraint makes an overlapping seed a hard error
//!      rather than a silent double-insert, so this has to run first, in the
//!      same transaction.
//!
//! ## What it deliberately NO LONGER writes (Billing Phase 4c)
//!
//! Until Phase 4c this also wrote a positive `credit_ledger` row and upserted
//! `credit_balances` by `plans.credits * seats`. Both were the WRONG BUCKET.
//!
//! Migration `0161` re-defined `credit_balances` as the org's **purchased
//! overage pool** — the bucket that does NOT reset and is funded by buying
//! credits, not by holding a plan. A plan's allowance now lives in
//! `member_allowance_periods` and resets every period. Crediting the pool with
//! the plan allowance therefore issued the same entitlement TWICE: once as the
//! per-member allowance the gate reads, and again as "purchased" overage the
//! account never bought. A ten-seat `team_standard` grant put 30,000 credits of
//! phantom overage into a pool whose entire purpose is to answer "what did this
//! org pay extra for".
//!
//! The `credit_ledger` row went with it, and for the same reason rather than as
//! a side effect: `credit_ledger` is the pool's journal and `credit_balances` is
//! its materialized cache (`workers::billing_derivation::upsert_credit_balance`
//! maintains `balance = balance + delta` from exactly these rows). Writing a
//! positive entry there while leaving the balance alone would leave the journal
//! and its cache permanently disagreeing by the size of every grant ever made.
//! The grant's durable audit record is the `grants` row — the module doc above
//! already calls that "the durable why" — and what each member was actually
//! given is recorded, per member and per period, in
//! `member_allowance_periods.granted_credits`. Nothing is lost; it is written
//! where it is true.
//!
//! ## Env
//!
//!   DATABASE_URL   control-plane DSN. The ordinary `coralswarm_app` role is
//!                  sufficient — every write here goes through
//!                  `tenancy::scope::begin_account_scoped`, which is what
//!                  satisfies `grants` / `credit_ledger` / `credit_balances`'s
//!                  `account_isolation` RLS policies (migration `0150`). No
//!                  elevated/owner role is required, unlike
//!                  `backfill_reef_judgement_from_history`.
//!
//! ## Exit codes
//!
//!   0  resolved successfully, and either wrote the grant (`--execute`),
//!      reported a dry run, or reported an already-existing grant (no-op).
//!   1  a Postgres error occurred.
//!   2  usage/argument error, the plan code did not resolve to an active
//!      plan, or the identifier did not resolve to exactly one account.

use anyhow::Context;
use sqlx::postgres::PgPoolOptions;
use sqlx::types::Decimal;
use sqlx::PgPool;
use uuid::Uuid;

use coralswarm_backend::billing::issuance;

const USAGE: &str = "usage: grant_plan --plan <code> \
(--account-id <uuid> | --email <email> | --org-slug <slug>) \
[--seats <n>] [--credits <override>] [--note <text>] [--execute]";

enum AccountIdentifier {
    AccountId(Uuid),
    Email(String),
    OrgSlug(String),
}

struct Args {
    plan_code: String,
    identifier: AccountIdentifier,
    /// Purchased seat count. Required for a seat-priced plan and rejected
    /// for a flat one — validated in [`resolve_seats`] once the plan is
    /// known, since which case applies is a property of the plan, not of
    /// the argv.
    seats: Option<i32>,
    credits_override: Option<Decimal>,
    note: Option<String>,
    execute: bool,
}

fn parse_args() -> Result<Args, String> {
    let argv: Vec<String> = std::env::args().collect();
    let mut plan_code: Option<String> = None;
    let mut account_id: Option<Uuid> = None;
    let mut email: Option<String> = None;
    let mut org_slug: Option<String> = None;
    let mut seats: Option<i32> = None;
    let mut credits_override: Option<Decimal> = None;
    let mut note: Option<String> = None;
    let mut execute = false;

    let mut i = 1;
    while i < argv.len() {
        let need_value = |i: usize| -> Result<&String, String> {
            argv.get(i + 1).ok_or_else(|| format!("{} requires a value", argv[i]))
        };
        match argv[i].as_str() {
            "--plan" => {
                plan_code = Some(need_value(i)?.clone());
                i += 2;
            }
            "--account-id" => {
                let v = need_value(i)?;
                account_id =
                    Some(Uuid::parse_str(v).map_err(|e| format!("--account-id: invalid UUID: {e}"))?);
                i += 2;
            }
            "--email" => {
                email = Some(need_value(i)?.clone());
                i += 2;
            }
            "--org-slug" => {
                org_slug = Some(need_value(i)?.clone());
                i += 2;
            }
            "--seats" => {
                let v = need_value(i)?;
                let n = v.parse::<i32>().map_err(|e| format!("--seats: invalid integer: {e}"))?;
                // `grants.seats`' own CHECK is `seats IS NULL OR seats >= 1`;
                // rejecting 0 and negatives here means the operator reads a
                // sentence rather than a constraint violation.
                if n < 1 {
                    return Err(format!("--seats: must be at least 1, got {n}"));
                }
                seats = Some(n);
                i += 2;
            }
            "--credits" => {
                let v = need_value(i)?;
                credits_override = Some(
                    v.parse::<Decimal>()
                        .map_err(|e| format!("--credits: invalid decimal: {e}"))?,
                );
                i += 2;
            }
            "--note" => {
                note = Some(need_value(i)?.clone());
                i += 2;
            }
            "--execute" => {
                execute = true;
                i += 1;
            }
            other => return Err(format!("unknown argument: {other}\n{USAGE}")),
        }
    }

    let plan_code = plan_code.ok_or_else(|| format!("--plan is required\n{USAGE}"))?;

    let identifiers = [account_id.is_some(), email.is_some(), org_slug.is_some()];
    if identifiers.iter().filter(|x| **x).count() != 1 {
        return Err(format!(
            "exactly ONE of --account-id, --email, --org-slug is required\n{USAGE}"
        ));
    }
    let identifier = if let Some(id) = account_id {
        AccountIdentifier::AccountId(id)
    } else if let Some(e) = email {
        AccountIdentifier::Email(e)
    } else {
        AccountIdentifier::OrgSlug(org_slug.expect("checked above"))
    };

    Ok(Args { plan_code, identifier, seats, credits_override, note, execute })
}

struct Plan {
    id: Uuid,
    code: String,
    name: String,
    credits: Decimal,
    /// `plans.is_seat_priced` (migration `0155`) — whether `credits` above
    /// is a PER-SEAT allowance rather than a flat one.
    is_seat_priced: bool,
    /// `plans.min_seats` (migration `0155`) — the policy floor on a
    /// purchase of this plan, enforced in [`resolve_seats`] and nowhere
    /// else. See `0156`'s header for why it is deliberately not a
    /// constraint.
    min_seats: Option<i32>,
}

async fn resolve_plan(pool: &PgPool, code: &str) -> Result<Plan, String> {
    // Refused BEFORE the lookup even runs — see this module's own doc
    // comment ("`--plan team` is REFUSED, deliberately") for the zero-credit
    // footgun this closes now that `0170` has flipped `team.is_active =
    // true`. Named explicitly (not "any plan with grant_seat_tiers
    // involved", which this binary has no way to detect ahead of a grant it
    // hasn't issued yet) because `team` is the only plan this schema defines
    // that mixes tiers under one grant.
    if code == coralswarm_backend::billing::MIXED_TIER_PLAN_CODE {
        return Err(
            "plan \"team\" cannot be issued via grant_plan — it is a MIXED-tier plan \
             (grant_seat_tiers holds a per-tier breakdown this binary's single --seats <n> \
             cannot express), and plans.credits = 0 for \"team\" by design (see migration \
             0167's header), so issuing it here would silently grant ZERO credits with no \
             grant_seat_tiers rows behind them. Use the seat-lifecycle routes (PR F) once they \
             land instead."
                .to_string(),
        );
    }
    let row: Option<(Uuid, String, String, Decimal, bool, Option<i32>)> = sqlx::query_as(
        "SELECT id, code, name, credits, is_seat_priced, min_seats \
         FROM plans WHERE code = $1 AND is_active = true",
    )
    .bind(code)
    .fetch_optional(pool)
    .await
    .map_err(|e| format!("plan lookup failed: {e}"))?;
    row.map(|(id, code, name, credits, is_seat_priced, min_seats)| Plan {
        id,
        code,
        name,
        credits,
        is_seat_priced,
        min_seats,
    })
    .ok_or_else(|| {
        format!("no ACTIVE plan with code {code:?} (see migration 0153_billing_plans_seed.sql)")
    })
}

/// Reconciles `--seats` against the resolved plan's pricing shape.
///
/// Returns the seat count to STORE in `grants.seats`: `Some(n)` for a
/// seat-priced plan, `None` for a flat one. Both directions are errors
/// rather than silent coercions, because both directions are a wrong
/// number rather than a matter of taste:
///
///   * a seat-priced plan with no `--seats` would fall through
///     `COALESCE(seats, 1)` and issue the ONE-SEAT allowance to a customer
///     who bought ten — the exact bug migration `0156` exists to kill;
///   * a flat plan carrying `--seats` means the operator believes they are
///     buying something the price does not reflect, and silently dropping
///     the argument would confirm that belief.
///
/// The schema enforces the first pair via `grants_seats_iff_seat_priced`;
/// this function exists so the operator gets a sentence first.
fn resolve_seats(plan: &Plan, seats: Option<i32>) -> Result<Option<i32>, String> {
    match (plan.is_seat_priced, seats) {
        (true, None) => Err(format!(
            "plan {:?} is seat-priced ({} credits and its price are PER SEAT) — \
             --seats <n> is required",
            plan.code, plan.credits
        )),
        (false, Some(n)) => Err(format!(
            "plan {:?} is not seat-priced — --seats {n} is not meaningful for it; \
             omit --seats (use --credits to override the flat allowance)",
            plan.code
        )),
        (false, None) => Ok(None),
        (true, Some(n)) => {
            if let Some(min) = plan.min_seats {
                if n < min {
                    return Err(format!(
                        "plan {:?} has a {min}-seat minimum (plans.min_seats) — \
                         --seats {n} is below it",
                        plan.code
                    ));
                }
            }
            Ok(Some(n))
        }
    }
}

/// The allowance one grant issues:
/// `COALESCE(credits_override, plans.credits * COALESCE(seats, 1))`.
///
/// Mirrors, in Rust, the identical expression `routes::usage` computes in
/// SQL — see migration `0156`'s header, which states the formula once as
/// the shared reference for both. `credits_override` is an ABSOLUTE total
/// and therefore short-circuits the multiplication rather than feeding it.
fn entitled_credits(plan: &Plan, seats: Option<i32>, credits_override: Option<Decimal>) -> Decimal {
    if let Some(total) = credits_override {
        return total;
    }
    plan.credits * Decimal::from(seats.unwrap_or(1))
}

/// Resolves `identifier` to exactly one `accounts.id`, per the task's
/// explicit instruction: never guess a UUID, always resolve one from a
/// live, checkable identifier.
///
/// Binary-edge convention (see `provision_stripe.rs`,
/// `adopt_personal_org_account.rs`): returns `anyhow::Result` and threads
/// `sqlx::Error` through `anyhow::Context` rather than collapsing it to a
/// bare `String`, so a lookup failure keeps its underlying source instead
/// of only the hand-written wrapper message.
async fn resolve_account(pool: &PgPool, identifier: &AccountIdentifier) -> anyhow::Result<Uuid> {
    match identifier {
        AccountIdentifier::AccountId(id) => {
            let exists: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM accounts WHERE id = $1)")
                .bind(id)
                .fetch_one(pool)
                .await
                .context("account lookup failed")?;
            if !exists {
                anyhow::bail!("--account-id {id}: no such account");
            }
            Ok(*id)
        }
        AccountIdentifier::Email(email) => {
            // Resolve the user first, then hand off to
            // `personal_account::personal_account_id` — the same function
            // `clerk_provision::ensure_personal_account` calls for its own
            // find step. A plain `clerk_org_id IS NULL` filter here would
            // guess wrong for an ADOPTED user: `adopt_personal_org_account`
            // relabels that user's only account with a `clerk_org_id`, so
            // they have no `clerk_org_id IS NULL` row left at all. Sharing
            // the one function means this operator tool cannot drift from
            // the request path's answer about which row is "the" personal
            // account.
            let user_id: Option<Uuid> = sqlx::query_scalar("SELECT id FROM users WHERE email = $1")
                .bind(email)
                .fetch_optional(pool)
                .await
                .context("user lookup failed")?;
            let Some(user_id) = user_id else {
                anyhow::bail!("--email {email:?}: no such user");
            };
            coralswarm_backend::personal_account::personal_account_id(pool, user_id)
                .await
                .context("account lookup failed")?
                .ok_or_else(|| anyhow::anyhow!("--email {email:?}: no personal account found for this email"))
        }
        AccountIdentifier::OrgSlug(slug) => {
            let row: Option<Uuid> =
                sqlx::query_scalar("SELECT account_id FROM tenant_databases WHERE slug = $1")
                    .bind(slug)
                    .fetch_optional(pool)
                    .await
                    .context("org lookup failed")?;
            row.ok_or_else(|| anyhow::anyhow!("--org-slug {slug:?}: no tenant_databases row with this slug"))
        }
    }
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    let args = match parse_args() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("{e}");
            return std::process::ExitCode::from(2);
        }
    };

    let dsn = match std::env::var("DATABASE_URL") {
        Ok(v) if !v.trim().is_empty() => v,
        _ => {
            eprintln!("DATABASE_URL must be set");
            return std::process::ExitCode::from(2);
        }
    };
    let pool = match PgPoolOptions::new().max_connections(2).connect(&dsn).await {
        Ok(p) => p,
        Err(e) => {
            eprintln!("failed to connect: {e}");
            return std::process::ExitCode::from(1);
        }
    };

    let plan = match resolve_plan(&pool, &args.plan_code).await {
        Ok(p) => p,
        Err(e) => {
            eprintln!("{e}");
            return std::process::ExitCode::from(2);
        }
    };
    let account_id = match resolve_account(&pool, &args.identifier).await {
        Ok(id) => id,
        Err(e) => {
            eprintln!("{e:#}");
            return std::process::ExitCode::from(2);
        }
    };
    // Validated BEFORE the already-granted check and long before any write,
    // so a malformed invocation fails on its own terms rather than being
    // masked by an unrelated no-op.
    let seats = match resolve_seats(&plan, args.seats) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("{e}");
            return std::process::ExitCode::from(2);
        }
    };
    let credits = entitled_credits(&plan, seats, args.credits_override);

    // Show the multiplication rather than only its result: "3000 x 4 seats"
    // is checkable against the invoice at a glance, "12000" is not.
    let basis = match (seats, args.credits_override) {
        (_, Some(_)) => " (--credits override, absolute)".to_string(),
        (Some(n), None) => format!(" ({} per seat x {n} seats)", plan.credits),
        (None, None) => String::new(),
    };
    eprintln!(
        "resolved: account={account_id} plan={} ({}) credits={credits}{basis}",
        plan.code, plan.name
    );

    match issuance::existing_active_grant(&pool, account_id, plan.id).await {
        Ok(Some(grant_id)) => {
            eprintln!(
                "already granted: account {account_id} already holds a non-revoked grant \
                 {grant_id} for plan {:?} — no-op (this is what makes re-running safe)",
                plan.code
            );
            return std::process::ExitCode::SUCCESS;
        }
        Ok(None) => {}
        Err(e) => {
            eprintln!("{e}");
            return std::process::ExitCode::from(1);
        }
    }

    if !args.execute {
        eprintln!(
            "DRY RUN (pass --execute to write): would grant {credits} credits total to account \
             {account_id} under plan {:?}, as a grants row plus one member_allowance_periods row \
             per current member.",
            plan.code
        );
        return std::process::ExitCode::SUCCESS;
    }

    let plan_row = issuance::PlanRow {
        id: plan.id,
        code: plan.code.clone(),
        credits: plan.credits,
        is_seat_priced: plan.is_seat_priced,
        min_seats: plan.min_seats,
    };
    match issuance::issue_grant(
        &pool,
        account_id,
        &plan_row,
        seats,
        args.credits_override,
        args.note.as_deref(),
    )
    .await
    {
        Ok(issuance::GrantOutcome::AlreadyGranted) => {
            eprintln!(
                "already granted: account {account_id} already holds a non-revoked grant for \
                 plan {:?} — no-op (a concurrent grant landed between the check above and this \
                 write)",
                plan.code
            );
            std::process::ExitCode::SUCCESS
        }
        Ok(issuance::GrantOutcome::Granted(outcome)) => {
            eprintln!(
                "granted: account {account_id} under plan {:?} — {}",
                plan.code,
                outcome.summary()
            );
            // A grant that seeded NO allowance rows bought the account nothing
            // usable, and the two ways that happens are worth naming rather
            // than leaving the operator to infer from "rows=0".
            if outcome.members == 0 {
                eprintln!(
                    "WARNING: this account has no members, so no allowance period was seeded. \
                     The grant exists but nobody can spend it until a member is added and a \
                     period is rolled."
                );
            } else if outcome.no_active_plan() {
                eprintln!(
                    "WARNING: no active grant resolved when seeding the allowance — nothing was \
                     written to member_allowance_periods."
                );
            }
            if outcome.seat_mismatch() {
                eprintln!(
                    "WARNING: {} members were granted an allowance but only {} seats were \
                     purchased. Every member was granted in full; reconcile the seat count.",
                    outcome.members,
                    outcome.seats.unwrap_or(0)
                );
            }
            std::process::ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("{e}");
            std::process::ExitCode::from(1)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Needs no database, unlike `tests/billing_grant_seats_pg.rs` which
    /// proves the SQL half of the same formula. Both halves are asserted
    /// because they are two independent implementations of one rule, and a
    /// disagreement between them is a customer being issued the wrong
    /// number of credits.
    fn plan(code: &str, credits: i64, is_seat_priced: bool, min_seats: Option<i32>) -> Plan {
        Plan {
            id: Uuid::nil(),
            code: code.to_string(),
            name: code.to_string(),
            credits: Decimal::from(credits),
            is_seat_priced,
            min_seats,
        }
    }

    fn team_standard() -> Plan {
        plan("team_standard", 3000, true, Some(3))
    }

    fn pro() -> Plan {
        plan("pro", 1500, false, None)
    }

    #[test]
    fn seat_priced_plan_requires_seats() {
        let err = resolve_seats(&team_standard(), None).unwrap_err();
        assert!(
            err.contains("--seats <n> is required"),
            "a seat-priced plan with no --seats must be refused outright, never \
             silently defaulted to 1 seat: {err}"
        );
    }

    #[test]
    fn flat_plan_rejects_seats() {
        let err = resolve_seats(&pro(), Some(5)).unwrap_err();
        assert!(
            err.contains("not seat-priced"),
            "passing --seats to a flat plan must be an error, not a silently \
             dropped argument: {err}"
        );
    }

    #[test]
    fn min_seats_floor_is_enforced() {
        // team_standard carries a 3-seat minimum (migration 0155).
        let err = resolve_seats(&team_standard(), Some(2)).unwrap_err();
        assert!(err.contains("3-seat minimum"), "expected the min_seats floor to fire: {err}");

        assert_eq!(resolve_seats(&team_standard(), Some(3)).unwrap(), Some(3), "3 is at the floor");
        assert_eq!(resolve_seats(&team_standard(), Some(9)).unwrap(), Some(9), "9 is above it");
    }

    #[test]
    fn plan_with_no_minimum_accepts_a_single_seat() {
        // team_premium is seat-priced with min_seats NULL (migration 0155).
        let premium = plan("team_premium", 15000, true, None);
        assert_eq!(resolve_seats(&premium, Some(1)).unwrap(), Some(1));
    }

    #[test]
    fn flat_plan_entitlement_is_the_plans_credits() {
        assert_eq!(entitled_credits(&pro(), None, None), Decimal::from(1500));
    }

    #[test]
    fn seat_priced_entitlement_multiplies() {
        assert_eq!(
            entitled_credits(&team_standard(), Some(4), None),
            Decimal::from(12_000),
            "4 seats x 3,000/seat — returning 3,000 here is the pre-0156 bug"
        );
    }

    #[test]
    fn override_is_absolute_not_per_seat() {
        assert_eq!(
            entitled_credits(&team_standard(), Some(4), Some(Decimal::from(7000))),
            Decimal::from(7000),
            "credits_override is a TOTAL; multiplying it by seats would give 28,000 and \
             would retroactively inflate every grant written under 0155's \
             'set credits_override by hand' guidance"
        );
    }
}
