//! `backfill_slack_oauth_tokens` — the one-time backfill named in Foundation
//! train PR 1.5's own plan entry: writes the missing `oauth_tokens` row for
//! every Slack workspace connected through the legacy
//! `routes::slack::oauth_callback` path, which (before this PR) never wrote
//! `oauth_tokens` at all. All the actual logic lives in the shared, `pub`
//! `coralswarm_backend::slack_oauth_backfill` module — this binary is a
//! thin CLI wrapper (argument parsing, dry-run reporting, exit codes), same
//! split as `bin/backfill_oauth_tokens_wrapped_dek.rs`.
//!
//! ## Operational runbook
//!
//! `MASTER_KEK_HEX` must be set for `--execute` (the same wrapped-DEK KEK
//! every other post-cutover `oauth_tokens` write already requires — see
//! `crypto.rs`'s own module doc).
//!
//!   1. **Deploy this PR's code** (the dual-write in
//!      `routes::slack::oauth_callback`, and every migrated read site). No
//!      migration accompanies this PR — no schema changed, only rows are
//!      added to the already-existing `oauth_tokens` table, so there is no
//!      separate "apply the migration first" step.
//!   2. **Run this binary with `--execute`** against the control-plane
//!      database. It writes one `oauth_tokens` row per legacy Slack
//!      workspace that does not already have one. Idempotent — safe to run
//!      more than once, and safe to re-run if a later pass finds stragglers
//!      (e.g. a workspace connected via the legacy path in the gap between
//!      step 1's deploy finishing on one instance and another).
//!   3. **Verify completion**: re-run with no `--execute` (a dry run) and
//!      confirm `workspaces_seen=0` — this binary's own summary line gives
//!      that count directly.
//!
//! ## Dry-run by default
//!
//! Same inverted-default discipline as `bin/backfill_oauth_tokens_wrapped_dek`/
//! `bin/backfill_billing_enforcement`: the safe outcome of a mistyped
//! invocation is a printed report, not a write. `--execute` is required to
//! actually write anything. A dry run needs no `MASTER_KEK_HEX` configured —
//! it only counts legacy workspaces lacking an `oauth_tokens` row.
//!
//! ## Usage
//!
//! ```text
//! backfill_slack_oauth_tokens [--execute] [--account-id <uuid>]
//! ```
//!
//! `--account-id <uuid>` narrows the whole run to exactly one account — a
//! rehearsal/repair aid, never how the real backfill is expected to run.
//!
//! ## Env
//!
//!   MIGRATOR_DATABASE_URL / DATABASE_URL   control-plane DSN, resolved via
//!       [`coralswarm_backend::migrator_database_url`] (same precedence every
//!       other backfill binary here uses). No elevated role is required:
//!       every RLS-covered `slack_integrations`/`oauth_tokens` read/write
//!       goes through `tenancy::scope::begin_account_scoped`, exactly like
//!       `backfill_oauth_tokens_wrapped_dek`'s own reads.
//!   MASTER_KEK_HEX   required for `--execute` (wraps/unwraps the account DEK).
//!
//! ## Exit codes
//!
//!   0  every selected account was processed with zero row-level errors.
//!   1  a database/connection error occurred, or at least one row-level
//!      error occurred. Re-running is safe either way — see the runbook
//!      above.
//!   2  usage/argument error.

use std::process::ExitCode;

use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

use coralswarm_backend::migrator_database_url;
use coralswarm_backend::slack_oauth_backfill::{backfill_slack_oauth_tokens, preview_slack_oauth_backfill};

const USAGE: &str = "usage: backfill_slack_oauth_tokens [--execute] [--account-id <uuid>]";

#[derive(Debug)]
struct Args {
    execute: bool,
    account_id: Option<Uuid>,
}

/// Takes `impl Iterator<Item = String>` so the test below exercises this
/// EXACT function against a synthetic argv rather than a hand-copied
/// restatement of the flag loop — same discipline as
/// `backfill_oauth_tokens_wrapped_dek::parse_args`.
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
    Ok(Args { execute, account_id })
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
        "backfill_slack_oauth_tokens — mode={}",
        if args.execute { "EXECUTE" } else { "DRY RUN" }
    );
    if let Some(id) = args.account_id {
        println!("  narrowed to account {id}");
    }

    let outcome = if args.execute {
        backfill_slack_oauth_tokens(&pool, args.account_id).await
    } else {
        preview_slack_oauth_backfill(&pool, args.account_id).await
    };

    for msg in &outcome.messages {
        eprintln!("ERROR {msg}");
    }
    println!(
        "summary: accounts={} org_skipped={} dek_provisioned={} workspaces_seen={} \
         rows_written={} errors={}",
        outcome.accounts_seen,
        // Org accounts are skipped by design — their DEK lives in their own
        // tenant database, out of this consumer-plane binary's reach (see
        // `slack_oauth_backfill::process_account`'s own comment).
        outcome.accounts_skipped_org,
        outcome.accounts_provisioned_dek,
        outcome.workspaces_seen,
        outcome.rows_written,
        outcome.errors
    );

    if !args.execute {
        if outcome.workspaces_seen == 0 {
            println!("Nothing left to backfill (0 legacy Slack workspaces without an oauth_tokens row).");
        } else {
            println!(
                "{} legacy workspace(s) still have no oauth_tokens row. Nothing was written — \
                 re-run with --execute to convert them (see this binary's own doc comment for the \
                 full runbook).",
                outcome.workspaces_seen
            );
        }
        return if outcome.errors > 0 { ExitCode::FAILURE } else { ExitCode::SUCCESS };
    }

    if outcome.errors == 0 {
        println!("done: every selected legacy workspace now has an oauth_tokens row.");
        ExitCode::SUCCESS
    } else {
        println!(
            "{} error(s) this pass — re-run this backfill to retry (idempotent, safe to re-run at \
             any time). See the runbook in this binary's own doc comment.",
            outcome.errors
        );
        ExitCode::FAILURE
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn defaults_are_dry_run_with_no_account_filter() {
        let args = parse_args(std::iter::empty()).expect("empty argv is valid");
        assert!(!args.execute);
        assert_eq!(args.account_id, None);
    }

    #[test]
    fn an_unknown_flag_is_rejected() {
        let argv = vec!["--bogus".to_string()];
        let err = parse_args(argv.into_iter()).expect_err("must reject an unknown flag");
        assert!(err.contains("--bogus"), "{err:?}");
    }

    #[test]
    fn account_id_without_a_value_is_a_usage_error() {
        let argv = vec!["--account-id".to_string()];
        let err = parse_args(argv.into_iter()).expect_err("must reject a missing value");
        assert!(err.contains("--account-id"), "{err:?}");
    }

    #[test]
    fn account_id_with_invalid_uuid_is_a_usage_error() {
        let argv = vec!["--account-id".to_string(), "not-a-uuid".to_string()];
        let err = parse_args(argv.into_iter()).expect_err("must reject a malformed UUID");
        assert!(err.contains("invalid UUID"), "{err:?}");
    }
}
