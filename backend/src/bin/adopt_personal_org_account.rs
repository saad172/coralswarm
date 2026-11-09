//! Give a personal organization's identity to the account that already holds
//! the person's data, and delete the empty one Clerk created.
//!
//! # Why this exists
//!
//! Creating a personal organization fires Clerk's `organization.created`
//! webhook, and `clerk_webhook::handle_org_created` writes a fresh `accounts`
//! row for it. So a user who has a personal org has TWO accounts: the new,
//! empty org account, and their original personal account holding everything.
//!
//! The founder's decision (2026-09-08) is that the organization's account is
//! the home and the duplicate must genuinely go — not be hidden.
//!
//! # Why this RELABELS instead of migrating rows
//!
//! The obvious reading of "move the data onto the org account" is a migration.
//! Measured in production first, it is **~213,000 rows across 30 tables** —
//! `account_id` appears on 54 tables, and the mass is not oceans. `graphs` is
//! EIGHT rows. The bulk is `llm_usage` (131k), `jobs` (49k),
//! `extraction_cache`, `activity_events`, and — the reason this route was
//! rejected — the **billing ledgers**: `credit_ledger`, `credit_balances`,
//! `enforcement_events`, `member_allowance_periods`, plus `oauth_tokens`.
//!
//! Re-pointing all of that risks a billing ledger to relocate eight rows.
//!
//! This tool reaches the identical end state — one account, identified by the
//! organization, holding everything — by moving the IDENTITY to the data
//! rather than the data to the identity. Per user it is one delete and two
//! updates. Nothing that holds content or money is touched.
//!
//! The account keeps its own UUID either way. The only difference is WHICH
//! uuid survives: this keeps the one 213,000 rows already point at.
//!
//! # What one pass does, per user
//!
//! 1. Read the `personal_orgs` row. No row -> `no-personal-org`, skipped.
//! 2. Find the org account (`accounts.clerk_org_id = <org>`) and the personal
//!    account (`owner_user_id = <user> AND clerk_org_id IS NULL`).
//!    Either missing -> reported and skipped, never guessed at.
//! 3. **Prove the org account is EMPTY** — see the guard below. Not empty ->
//!    refused, and the whole run exits non-zero.
//! 4. In ONE transaction, with `account_members_account_org_fk` deferred
//!    (it is `DEFERRABLE`, checked against the live schema):
//!      * delete the org account's `account_members` rows, then the account;
//!      * `UPDATE accounts SET clerk_org_id = <org>, is_org_account = true`
//!        on the PERSONAL account;
//!      * `UPDATE account_members SET account_is_org_account = true,
//!        seat_tier_id = <the tier the webhook assigned>` on its members.
//! 5. Re-read and verify the end state before reporting success.
//!
//! ## The emptiness guard is the whole safety story
//!
//! Deleting the org account is the irreversible step, so "it is empty" may not
//! be an assumption. The guard enumerates every table carrying an `account_id`
//! column **from `information_schema` at run time**, not from a list in this
//! file — a hard-coded list silently goes stale the moment a migration adds a
//! table, and this is exactly the check that must not be quietly incomplete.
//! `account_members` is excluded (step 4 deletes those deliberately); anything
//! else with a single row refuses the user.
//!
//! ## Why the seat tier is copied rather than chosen
//!
//! `account_members_seat_tier_iff_org` is
//! `account_is_org_account = (seat_tier_id IS NOT NULL)`, so the personal
//! membership MUST gain a tier as it becomes an org membership. The tier is
//! copied from the org account's own membership — the one Clerk's webhook
//! caused — so the end state is the seat the user already had, not a new
//! billing decision made by this tool.
//!
//! ## Idempotent
//!
//! A second run finds the personal account already carrying `clerk_org_id`
//! and no separate org account, and reports `already-adopted` without writing.
//!
//! # Usage
//!
//! ```text
//! adopt_personal_org_account (--user <clerk_user_id> | --all) [--execute]
//! ```
//!
//! Dry run is the default and writes nothing — no DELETE, no UPDATE. It
//! reports the exact branch an `--execute` run would take, per user.

use std::process::ExitCode;

use coralswarm_backend::migrator_database_url;
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};
use uuid::Uuid;

const USAGE: &str =
    "usage: adopt_personal_org_account (--user <clerk_user_id> | --all) [--execute] [--discard-bookkeeping]";

/// Tables whose rows for the empty org account may be discarded under
/// `--discard-bookkeeping`, and ONLY those.
///
/// Every entry is state the system provisions for itself — not anything a
/// person created. Confirmed against the real account before this list was
/// written: `credit_balances` held a balance of **0**, the `jobs` rows were
/// seven `BackfillAtomTitles` all `done`, and the rest are per-account
/// bookkeeping created automatically within two hours of the org existing.
///
/// **The staleness direction is deliberate and safe.** Unlike the emptiness
/// guard — which reads `information_schema` at run time because a stale list
/// there would let the tool MISS data — a stale list HERE only ever makes the
/// tool refuse MORE. A table added by a future migration is not on it, so an
/// account holding that table's rows is rejected until someone looks. Never
/// widen this list to make a run pass; look at what wrote the rows first.
const DISCARDABLE_BOOKKEEPING: &[&str] = &[
    "credit_balances",
    "grants",
    "jobs",
    "member_allowance_periods",
    "user_account_prefs",
];

#[derive(Debug, PartialEq, Eq)]
enum Target {
    OneUser(String),
    AllUsers,
}

#[derive(Debug)]
struct Args {
    execute: bool,
    target: Target,
    /// Allow deleting the org account even when it carries rows, PROVIDED
    /// every occupied table is in [`DISCARDABLE_BOOKKEEPING`]. Never implied
    /// by `--execute`: discarding rows is a separate decision from performing
    /// the relabel.
    discard_bookkeeping: bool,
}

/// Takes an iterator rather than reading `std::env::args()` itself, so the
/// tests below drive this exact function against a synthetic argv.
fn parse_args(args: impl Iterator<Item = String>) -> Result<Args, String> {
    let mut execute = false;
    let mut discard_bookkeeping = false;
    let mut user: Option<String> = None;
    let mut all = false;
    let mut args = args;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--execute" => execute = true,
            "--discard-bookkeeping" => discard_bookkeeping = true,
            "--all" => all = true,
            "--user" => {
                user = Some(args.next().ok_or_else(|| "--user needs a value".to_string())?);
            }
            "-h" | "--help" => {
                println!("{USAGE}");
                std::process::exit(0);
            }
            other => return Err(format!("unknown argument: {other}")),
        }
    }
    let target = match (user, all) {
        (Some(_), true) => return Err("--user and --all are mutually exclusive".into()),
        (Some(u), false) => Target::OneUser(u),
        (None, true) => Target::AllUsers,
        // No default on purpose: defaulting to --all would make a forgotten
        // flag rewrite every account in the instance.
        (None, false) => return Err("one of --user or --all is required".into()),
    };
    Ok(Args { execute, target, discard_bookkeeping })
}

#[derive(Default, Debug)]
struct Totals {
    adopted: usize,
    would_adopt: usize,
    already_adopted: usize,
    no_personal_org: usize,
    skipped: usize,
    refused_not_empty: usize,
    errors: usize,
}

/// Every table carrying an `account_id`, read from the live catalogue.
///
/// Deliberately NOT a constant in this file. A hard-coded list is correct on
/// the day it is written and silently wrong after the next migration — and the
/// cost of it being wrong here is deleting an account that turned out to hold
/// something.
async fn account_id_tables(pool: &PgPool) -> Result<Vec<String>, sqlx::Error> {
    let rows = sqlx::query(
        "SELECT c.table_name FROM information_schema.columns c \
         JOIN information_schema.tables t \
           ON t.table_schema = c.table_schema AND t.table_name = c.table_name \
         WHERE c.table_schema = 'public' AND c.column_name = 'account_id' \
           AND t.table_type = 'BASE TABLE' \
           AND c.table_name <> 'account_members' \
         ORDER BY c.table_name",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows.iter().map(|r| r.get::<String, _>("table_name")).collect())
}

/// Which tables hold rows for `account_id`, and how many. Empty vec = safe to
/// delete.
async fn non_empty_tables(
    pool: &PgPool,
    tables: &[String],
    account_id: Uuid,
) -> Result<Vec<(String, i64)>, sqlx::Error> {
    let mut found = Vec::new();
    for t in tables {
        // `t` comes from `information_schema`, never from user input, and is
        // quoted as an identifier regardless.
        let sql = format!("SELECT count(*) FROM \"{t}\" WHERE account_id = $1");
        let n: i64 = sqlx::query_scalar(&sql).bind(account_id).fetch_one(pool).await?;
        if n > 0 {
            found.push((t.clone(), n));
        }
    }
    Ok(found)
}

/// One user. Never panics; every failure is counted and reported.
async fn process_user(
    pool: &PgPool,
    tables: &[String],
    clerk_user_id: &str,
    execute: bool,
    discard_bookkeeping: bool,
    totals: &mut Totals,
) {
    // Step 1 — the mapping row.
    let org_id: Option<String> = match sqlx::query_scalar(
        "SELECT clerk_org_id FROM personal_orgs WHERE clerk_user_id = $1",
    )
    .bind(clerk_user_id)
    .fetch_optional(pool)
    .await
    {
        Ok(v) => v,
        Err(e) => {
            eprintln!("user={clerk_user_id}: personal_orgs lookup failed: {e}");
            totals.errors += 1;
            return;
        }
    };
    let Some(org_id) = org_id else {
        println!("user={clerk_user_id}: no-personal-org — nothing to adopt");
        totals.no_personal_org += 1;
        return;
    };

    let user_uuid: Option<Uuid> =
        match sqlx::query_scalar("SELECT id FROM users WHERE clerk_user_id = $1")
            .bind(clerk_user_id)
            .fetch_optional(pool)
            .await
        {
            Ok(v) => v,
            Err(e) => {
                eprintln!("user={clerk_user_id}: users lookup failed: {e}");
                totals.errors += 1;
                return;
            }
        };
    let Some(user_uuid) = user_uuid else {
        println!("user={clerk_user_id}: skipped — no local users row");
        totals.skipped += 1;
        return;
    };

    // Step 2 — the two accounts.
    let org_account: Option<Uuid> =
        match sqlx::query_scalar("SELECT id FROM accounts WHERE clerk_org_id = $1")
            .bind(&org_id)
            .fetch_optional(pool)
            .await
        {
            Ok(v) => v,
            Err(e) => {
                eprintln!("user={clerk_user_id}: org account lookup failed: {e}");
                totals.errors += 1;
                return;
            }
        };
    let personal_account: Option<Uuid> = match sqlx::query_scalar(
        "SELECT id FROM accounts WHERE owner_user_id = $1 AND clerk_org_id IS NULL",
    )
    .bind(user_uuid)
    .fetch_optional(pool)
    .await
    {
        Ok(v) => v,
        Err(e) => {
            eprintln!("user={clerk_user_id}: personal account lookup failed: {e}");
            totals.errors += 1;
            return;
        }
    };

    match (org_account, personal_account) {
        // Already done: the org id sits on an account, and no separate
        // personal account remains. Idempotent, no write.
        (Some(acct), None) => {
            println!(
                "user={clerk_user_id}: already-adopted account={acct} org={org_id} \
                 (no separate personal account remains)"
            );
            totals.already_adopted += 1;
        }
        (None, Some(personal)) => {
            println!(
                "user={clerk_user_id}: skipped — personal account {personal} exists but NO \
                 account carries org {org_id}. Refusing to guess; run create_personal_orgs \
                 or investigate."
            );
            totals.skipped += 1;
        }
        (None, None) => {
            println!("user={clerk_user_id}: skipped — no accounts found at all for org {org_id}");
            totals.skipped += 1;
        }
        (Some(org_acct), Some(personal)) => {
            // Step 3 — the guard. This is the step that makes the delete safe.
            let occupied = match non_empty_tables(pool, tables, org_acct).await {
                Ok(v) => v,
                Err(e) => {
                    eprintln!("user={clerk_user_id}: emptiness check failed: {e}");
                    totals.errors += 1;
                    return;
                }
            };
            let detail = occupied
                .iter()
                .map(|(t, n)| format!("{t}={n}"))
                .collect::<Vec<_>>()
                .join(" ");
            let unexpected: Vec<&(String, i64)> = occupied
                .iter()
                .filter(|(t, _)| !DISCARDABLE_BOOKKEEPING.contains(&t.as_str()))
                .collect();
            if !occupied.is_empty() {
                if !unexpected.is_empty() {
                    let names = unexpected
                        .iter()
                        .map(|(t, n)| format!("{t}={n}"))
                        .collect::<Vec<_>>()
                        .join(" ");
                    eprintln!(
                        "user={clerk_user_id}: REFUSED — org account {org_acct} holds rows in \
                         tables that are NOT known bookkeeping ({names}). Full contents: \
                         {detail}. --discard-bookkeeping does not cover these; look at what \
                         wrote them before widening DISCARDABLE_BOOKKEEPING."
                    );
                    totals.refused_not_empty += 1;
                    return;
                }
                if !discard_bookkeeping {
                    eprintln!(
                        "user={clerk_user_id}: REFUSED — org account {org_acct} is not empty \
                         ({detail}). Every occupied table is known bookkeeping, so \
                         --discard-bookkeeping would allow this; it is a separate, explicit \
                         decision and is not implied by --execute."
                    );
                    totals.refused_not_empty += 1;
                    return;
                }
                println!(
                    "user={clerk_user_id}: will DISCARD bookkeeping on account {org_acct}: {detail}"
                );
            }

            // DISTINCT, not `LIMIT 1`. A bare `LIMIT 1` with no `ORDER BY`
            // returns an arbitrary row, so two runs could pick different tiers
            // — and the tier is then written onto EVERY member of the personal
            // account. Choosing between tiers is a billing decision, which this
            // tool must not make: more than one distinct tier is a refusal.
            let tiers: Vec<Uuid> = match sqlx::query_scalar(
                "SELECT DISTINCT seat_tier_id FROM account_members \
                 WHERE account_id = $1 AND seat_tier_id IS NOT NULL",
            )
            .bind(org_acct)
            .fetch_all(pool)
            .await
            {
                Ok(v) => v,
                Err(e) => {
                    eprintln!("user={clerk_user_id}: seat-tier lookup failed: {e}");
                    totals.errors += 1;
                    return;
                }
            };
            if tiers.len() > 1 {
                eprintln!(
                    "user={clerk_user_id}: REFUSED — org account {org_acct} carries {} DISTINCT \
                     seat tiers. Picking one and writing it onto every member of the personal \
                     account is a billing decision this tool must not make.",
                    tiers.len()
                );
                totals.refused_not_empty += 1;
                return;
            }
            let Some(seat_tier) = tiers.into_iter().next() else {
                eprintln!(
                    "user={clerk_user_id}: skipped — org account {org_acct} has no seat tier to \
                     copy, and `account_members_seat_tier_iff_org` requires one. Not inventing a \
                     billing decision."
                );
                totals.skipped += 1;
                return;
            };

            if !execute {
                println!(
                    "user={clerk_user_id}: would-adopt — give org {org_id} to account {personal} \
                     (the one holding the data), delete account {org_acct}, seat_tier={seat_tier}"
                );
                totals.would_adopt += 1;
                return;
            }

            let discard: Vec<String> = occupied.iter().map(|(t, _)| t.clone()).collect();
            match adopt(pool, tables, org_acct, personal, &org_id, seat_tier, &discard).await {
                Ok(()) => {}
                Err(AdoptError::RolledBack(e)) => {
                    eprintln!(
                        "user={clerk_user_id}: adopt failed BEFORE commit — the transaction rolled \
                         back and nothing changed. Safe to re-run once the cause is fixed: {e:#}"
                    );
                    totals.errors += 1;
                    return;
                }
                Err(AdoptError::CommittedButUnverified(e)) => {
                    eprintln!(
                        "user={clerk_user_id}: *** COMMITTED, THEN VERIFICATION FAILED *** \
                         The delete and both updates ARE durable. DO NOT re-run blindly — \
                         inspect accounts {personal} and {org_acct} by hand first: {e:#}"
                    );
                    totals.errors += 1;
                    return;
                }
            }
            println!(
                "user={clerk_user_id}: OK account={personal} now carries org {org_id}; \
                 empty account {org_acct} deleted"
            );
            totals.adopted += 1;
        }
    }
}

/// Why a two-variant error rather than a bare `anyhow::Error`.
///
/// The caller must BRANCH on which class of failure this was, because the two
/// demand opposite actions from an operator: one is safe to re-run, the other
/// must not be re-run until a human has looked. Branching on a stringly-typed
/// error would be guesswork. Each variant still carries an `anyhow::Error`, so
/// the binary edge keeps `anyhow`'s context chain as the coding guidelines ask.
#[derive(Debug)]
enum AdoptError {
    /// Failed BEFORE commit. The transaction rolled back; nothing changed.
    RolledBack(anyhow::Error),
    /// The transaction COMMITTED and the post-commit verification then failed.
    /// The change is durable. Re-running is not safe.
    CommittedButUnverified(anyhow::Error),
}

/// The whole change, in ONE transaction.
///
/// `account_members_account_org_fk` is `(account_id, account_is_org_account)
/// -> accounts(id, is_org_account)` and is `DEFERRABLE` (verified against the
/// live schema). Between the `accounts` update and the `account_members`
/// update the pair disagrees, so the constraint is deferred to commit. The
/// CHECK `account_members_seat_tier_iff_org` is NOT deferrable, which is why
/// `account_is_org_account` and `seat_tier_id` are set in the SAME statement.
async fn adopt(
    pool: &PgPool,
    tables: &[String],
    org_acct: Uuid,
    personal: Uuid,
    org_id: &str,
    seat_tier: Uuid,
    discard: &[String],
) -> Result<(), AdoptError> {
    use anyhow::anyhow;
    let rolled = AdoptError::RolledBack;

    let mut tx = pool.begin().await.map_err(|e| rolled(anyhow!(e)))?;
    sqlx::query("SET CONSTRAINTS account_members_account_org_fk DEFERRED")
        .execute(&mut *tx)
        .await
        .map_err(|e| rolled(anyhow!(e)))?;

    let mut account_ids=[org_acct,personal];
    account_ids.sort_unstable();
    for account in account_ids {
        sqlx::query("SELECT set_config('app.account_id',$1,true)").bind(account.to_string())
            .execute(&mut *tx).await.map_err(|e|rolled(anyhow!(e)))?;
        coralswarm_backend::billing::renewal::lock_period_rotation(&mut tx,account)
            .await.map_err(|e|rolled(anyhow!(e)))?;
    }
    for account in account_ids {
        sqlx::query("SELECT id FROM accounts WHERE id=$1 FOR UPDATE").bind(account)
            .fetch_optional(&mut *tx).await.map_err(|e|rolled(anyhow!(e)))?
            .ok_or_else(||rolled(anyhow!("account {account} vanished before lock")))?;
    }
    sqlx::query("SELECT set_config('app.account_id',$1,true)").bind(org_acct.to_string())
        .execute(&mut *tx).await.map_err(|e|rolled(anyhow!(e)))?;

    // RE-RUN the emptiness guard INSIDE the transaction.
    //
    // The check in `process_user` ran on a pooled connection, before this
    // transaction existed. Anything that wrote to the account in between — the
    // Clerk webhook path, a job worker, a billing sweep — would be invisible to
    // it, and `DELETE FROM accounts` would then remove those rows silently via
    // `ON DELETE CASCADE` rather than failing. On a `--all` run that window is
    // a whole round trip per user. This is the check that actually protects the
    // delete; the earlier one only decides what to REPORT.
    for t in tables {
        let sql = format!("SELECT count(*) FROM \"{t}\" WHERE account_id = $1");
        let n: i64 = sqlx::query_scalar(&sql)
            .bind(org_acct)
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| rolled(anyhow!(e)))?;
        if n > 0 && !DISCARDABLE_BOOKKEEPING.contains(&t.as_str()) {
            return Err(rolled(anyhow!(
                "{t} gained {n} row(s) for account {org_acct} between the pre-check and this \
                 transaction, and it is not discardable bookkeeping — refusing to delete an \
                 account that something is still writing to"
            )));
        }
    }

    for t in discard {
        // A HARD check, not `debug_assert!`: that compiles out under
        // `--release`, which is a build an operator may well run, leaving
        // nothing between this loop and `DELETE FROM "<table>"` but the
        // caller's own filter.
        if !DISCARDABLE_BOOKKEEPING.contains(&t.as_str()) {
            return Err(rolled(anyhow!(
                "refusing to discard {t}: it is not in DISCARDABLE_BOOKKEEPING"
            )));
        }
        let sql = format!("DELETE FROM \"{t}\" WHERE account_id = $1");
        sqlx::query(&sql)
            .bind(org_acct)
            .execute(&mut *tx)
            .await
            .map_err(|e| rolled(anyhow!(e)))?;
    }

    sqlx::query("DELETE FROM account_members WHERE account_id = $1")
        .bind(org_acct)
        .execute(&mut *tx)
        .await
        .map_err(|e| rolled(anyhow!(e)))?;
    sqlx::query("DELETE FROM accounts WHERE id = $1")
        .bind(org_acct)
        .execute(&mut *tx)
        .await
        .map_err(|e| rolled(anyhow!(e)))?;
    sqlx::query("SELECT set_config('app.account_id',$1,true)").bind(personal.to_string())
        .execute(&mut *tx).await.map_err(|e|rolled(anyhow!(e)))?;
    sqlx::query("UPDATE accounts SET clerk_org_id = $1, is_org_account = true WHERE id = $2")
        .bind(org_id)
        .bind(personal)
        .execute(&mut *tx)
        .await
        .map_err(|e| rolled(anyhow!(e)))?;
    sqlx::query(
        "UPDATE account_members SET account_is_org_account = true, seat_tier_id = $1 \
         WHERE account_id = $2",
    )
    .bind(seat_tier)
    .bind(personal)
    .execute(&mut *tx)
    .await
    .map_err(|e| rolled(anyhow!(e)))?;

    tx.commit().await.map_err(|e| rolled(anyhow!(e)))?;

    // Everything from here is POST-COMMIT. A failure below does not undo
    // anything, so it must never be reported as a rollback.
    let post = AdoptError::CommittedButUnverified;
    let carries: Option<String> = sqlx::query_scalar("SELECT clerk_org_id FROM accounts WHERE id = $1")
        .bind(personal)
        .fetch_optional(pool)
        .await
        .map_err(|e| post(anyhow!(e)))?
        .flatten();
    if carries.as_deref() != Some(org_id) {
        return Err(post(anyhow!(
            "account {personal} carries {carries:?}, expected {org_id:?}"
        )));
    }
    let ghost: Option<Uuid> = sqlx::query_scalar("SELECT id FROM accounts WHERE id = $1")
        .bind(org_acct)
        .fetch_optional(pool)
        .await
        .map_err(|e| post(anyhow!(e)))?;
    if ghost.is_some() {
        return Err(post(anyhow!("deleted account {org_acct} still exists")));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn execute_defaults_to_false() {
        let a = parse_args(["--user", "user_a"].iter().map(|s| s.to_string())).unwrap();
        assert!(!a.execute, "dry run must be the default — a run without --execute may not write");
    }

    #[test]
    fn no_target_is_a_usage_error() {
        parse_args(["--execute"].iter().map(|s| s.to_string()))
            .expect_err("must require a target: defaulting to --all would rewrite every account");
    }

    #[test]
    fn user_and_all_together_are_rejected() {
        parse_args(["--user", "user_a", "--all"].iter().map(|s| s.to_string()))
            .expect_err("--user and --all are mutually exclusive");
    }

    #[test]
    fn user_without_a_value_is_a_usage_error() {
        parse_args(["--user"].iter().map(|s| s.to_string())).expect_err("--user needs a value");
    }

    #[test]
    fn all_and_execute_parse_together() {
        let a = parse_args(["--all", "--execute"].iter().map(|s| s.to_string())).unwrap();
        assert!(a.execute);
        assert_eq!(a.target, Target::AllUsers);
    }

    #[test]
    fn discard_bookkeeping_defaults_to_false() {
        let a = parse_args(["--user", "u", "--execute"].iter().map(|s| s.to_string())).unwrap();
        assert!(
            !a.discard_bookkeeping,
            "discarding rows must never be implied by --execute — it is a separate decision"
        );
    }

    #[test]
    fn discard_bookkeeping_parses_independently_of_execute() {
        let a = parse_args(["--all", "--discard-bookkeeping"].iter().map(|s| s.to_string())).unwrap();
        assert!(a.discard_bookkeeping);
        assert!(!a.execute, "--discard-bookkeeping alone must still be a DRY RUN");
    }

    /// The allowlist must stay a list of BOOKKEEPING tables. If a future edit
    /// adds something that holds a person's content, this is the test that
    /// should have stopped it — checked against an independent literal set,
    /// not against the constant itself.
    #[test]
    fn the_discard_allowlist_holds_no_content_tables() {
        const CONTENT_TABLES: &[&str] = &[
            "graphs", "atoms", "conversations", "oauth_tokens", "credit_ledger",
            "query_history", "teams", "account_members", "accounts",
        ];
        for t in DISCARDABLE_BOOKKEEPING {
            assert!(
                !CONTENT_TABLES.contains(t),
                "{t} holds content or money and must never be discardable"
            );
        }
        assert_eq!(
            DISCARDABLE_BOOKKEEPING.len(),
            5,
            "the allowlist changed size — every entry needs a written justification, so this \
             literal is here to force the conversation"
        );
    }

    #[test]
    fn an_unknown_flag_is_rejected_rather_than_ignored() {
        parse_args(["--all", "--force"].iter().map(|s| s.to_string()))
            .expect_err("an unknown flag must not be silently ignored on a destructive tool");
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let args = match parse_args(std::env::args().skip(1)) {
        Ok(a) => a,
        Err(e) => {
            eprintln!("{e}\n{USAGE}");
            return ExitCode::from(2);
        }
    };
    let Some(dsn) = migrator_database_url() else {
        eprintln!("MIGRATOR_DATABASE_URL (preferred) or DATABASE_URL must be set");
        return ExitCode::from(2);
    };
    let pool = match PgPoolOptions::new().max_connections(4).connect(&dsn).await {
        Ok(p) => p,
        Err(e) => {
            eprintln!("could not connect: {e}");
            return ExitCode::FAILURE;
        }
    };

    println!(
        "adopt_personal_org_account — mode={}",
        if args.execute { "EXECUTE" } else { "DRY RUN" }
    );
    println!(
        "  This moves the ORGANIZATION'S IDENTITY onto the account that already\n\
        \x20 holds the data, then DELETES the empty account Clerk's webhook made.\n\
        \x20 No content and no billing row is moved or rewritten."
    );

    let tables = match account_id_tables(&pool).await {
        Ok(t) => t,
        Err(e) => {
            eprintln!("could not enumerate account_id tables: {e}");
            return ExitCode::FAILURE;
        }
    };
    println!("  emptiness guard covers {} tables (read from the live catalogue)", tables.len());

    let users: Vec<String> = match &args.target {
        Target::OneUser(u) => vec![u.clone()],
        Target::AllUsers => {
            match sqlx::query_scalar::<_, String>("SELECT clerk_user_id FROM personal_orgs ORDER BY clerk_user_id")
                .fetch_all(&pool)
                .await
            {
                Ok(v) => v,
                Err(e) => {
                    eprintln!("could not list personal_orgs: {e}");
                    return ExitCode::FAILURE;
                }
            }
        }
    };

    let mut totals = Totals::default();
    for clerk_user_id in &users {
        process_user(&pool, &tables, clerk_user_id, args.execute, args.discard_bookkeeping, &mut totals)
            .await;
    }

    println!(
        "summary: users={} adopted={} would_adopt={} already_adopted={} no_personal_org={} skipped={} refused_not_empty={} errors={}",
        users.len(),
        totals.adopted,
        totals.would_adopt,
        totals.already_adopted,
        totals.no_personal_org,
        totals.skipped,
        totals.refused_not_empty,
        totals.errors
    );
    if !args.execute {
        println!("Nothing was written — no rows were deleted or updated. Re-run with --execute to apply.");
    }
    if totals.errors > 0 || totals.refused_not_empty > 0 {
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}
