//! `backfill_oauth_tokens_wrapped_dek` — the cutover backfill named in
//! migration `0177_oauth_tokens_dek_migrated_at.sql`'s own header (Foundation
//! train PR 1.2, CS-031): re-encrypts every `oauth_tokens` row still on the
//! legacy `ACCOUNT_MASTER_KEK`-XOR DEK onto the SAME wrapped per-account DEK
//! content encryption already uses. All the actual logic lives in the shared,
//! `pub` `coralswarm_backend::oauth_dek_cutover` module — this binary is a
//! thin CLI wrapper (argument parsing, dry-run reporting, exit codes), same
//! split as `bin/backfill_billing_enforcement.rs`.
//!
//! ## Operational runbook
//!
//! `ACCOUNT_MASTER_KEK` (the OLD scheme) and `MASTER_KEK_HEX` (the wrapped-DEK
//! scheme, already required for content encryption) must BOTH be set in the
//! environment this binary runs in — it needs to decrypt under the old key
//! and encrypt under the new one in the same pass.
//!
//!   1. **Deploy this PR's migration** (`0177_oauth_tokens_dek_migrated_at`,
//!      applied by the normal control-plane migrator). Schema-only, no
//!      behavior change yet — every existing row is still legacy-encrypted
//!      and the OLD server code (pre-cutover) is still running and reading it
//!      exactly as before.
//!   2. **Run this binary with `--execute`** against the SAME database, before
//!      deploying this PR's code changes. It re-encrypts every existing row
//!      and sets `dek_migrated_at`. Idempotent — safe to run more than once,
//!      and safe to re-run if step 4 below finds stragglers.
//!   3. **Deploy this PR's code** (the cutover: every request/worker path
//!      resolves the account DEK via `crate::crypto::resolve_dek`/
//!      `resolve_dek_cached` only — no XOR derivation remains reachable from
//!      any request path). Any `oauth_tokens` row written or refreshed
//!      DURING the gap between step 2 finishing and this deploy completing
//!      was still written by the OLD (pre-deploy) code, i.e. still
//!      legacy-encrypted.
//!   4. **Re-run this binary with `--execute` once more**, immediately after
//!      step 3's deploy completes, to catch exactly that gap — any row
//!      created/refreshed by old code between steps 2 and 3. This is why the
//!      binary is built to be safely re-run rather than a single "run once"
//!      step: real Fly.io deploys are not perfectly atomic, so a strict
//!      "convert everything, then flip the code" ordering cannot fully
//!      eliminate this window on its own.
//!   5. **Verify completion**: `SELECT count(*) FROM oauth_tokens WHERE
//!      dek_migrated_at IS NULL` must be `0` (this binary's own dry-run report
//!      — see below — gives the same count without a manual query). Once
//!      confirmed across every deployment that shares this database, **delete
//!      `ACCOUNT_MASTER_KEK` from deployment secrets** — nothing reads it
//!      after this point (see `crypto::derive_legacy_oauth_account_dek`'s doc
//!      comment, the function's only remaining reader, which this binary is
//!      the only caller of).
//!
//! ## Dry-run by default
//!
//! Same inverted-default discipline as `bin/grant_plan`/
//! `bin/backfill_billing_enforcement`: the safe outcome of a mistyped
//! invocation is a printed report, not a write. `--execute` is required to
//! actually decrypt/re-encrypt/write anything. A dry run needs neither
//! `ACCOUNT_MASTER_KEK` nor `MASTER_KEK_HEX` configured — it only counts rows
//! with `dek_migrated_at IS NULL`.
//!
//! ## Usage
//!
//! ```text
//! backfill_oauth_tokens_wrapped_dek [--execute] [--account-id <uuid>]
//! ```
//!
//! `--account-id <uuid>` narrows the whole run to exactly one account — a
//! rehearsal/repair aid, never how the real backfill is expected to run.
//!
//! ## Env
//!
//!   MIGRATOR_DATABASE_URL / DATABASE_URL   control-plane DSN, resolved via
//!       [`coralswarm_backend::migrator_database_url`] (same precedence every
//!       other backfill binary here uses). No elevated role is required: every
//!       RLS-covered `oauth_tokens` read/write goes through
//!       `tenancy::scope::begin_account_scoped`, exactly like
//!       `backfill_billing_enforcement`'s own `grants` reads.
//!   ACCOUNT_MASTER_KEK   required for `--execute` (decrypts legacy rows).
//!   MASTER_KEK_HEX       required for `--execute` (wraps/unwraps the new DEK).
//!
//! ## Exit codes
//!
//!   0  every selected account was processed with zero row-level errors, and
//!      (on `--execute`) zero rows remain unmigrated.
//!   1  a database/connection error occurred, or at least one row-level error
//!      occurred, or (on `--execute`) at least one row was left unmigrated.
//!      Re-running is safe either way — see the runbook above.
//!   2  usage/argument error.

use std::process::ExitCode;

use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

use coralswarm_backend::migrator_database_url;
use coralswarm_backend::oauth_dek_cutover::{preview_oauth_tokens_cutover, reencrypt_oauth_tokens};

const USAGE: &str = "usage: backfill_oauth_tokens_wrapped_dek [--execute] [--account-id <uuid>]";

#[derive(Debug)]
struct Args {
    execute: bool,
    account_id: Option<Uuid>,
}

/// Takes `impl Iterator<Item = String>` so the test below exercises this
/// EXACT function against a synthetic argv rather than a hand-copied
/// restatement of the flag loop — same discipline as
/// `backfill_billing_enforcement::parse_args`.
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
        "backfill_oauth_tokens_wrapped_dek — mode={}",
        if args.execute { "EXECUTE" } else { "DRY RUN" }
    );
    if let Some(id) = args.account_id {
        println!("  narrowed to account {id}");
    }

    let outcome = if args.execute {
        reencrypt_oauth_tokens(&pool, args.account_id).await
    } else {
        preview_oauth_tokens_cutover(&pool, args.account_id).await
    };

    for msg in &outcome.messages {
        eprintln!("ERROR {msg}");
    }
    println!(
        "summary: accounts={} dek_provisioned={} rows_seen={} rows_migrated={} errors={}",
        outcome.accounts_seen,
        outcome.accounts_provisioned_dek,
        outcome.rows_seen,
        outcome.rows_migrated,
        outcome.errors
    );

    if !args.execute {
        if outcome.rows_seen == 0 {
            println!(
                "Nothing left to migrate (0 legacy-encrypted rows). Safe to remove \
                 ACCOUNT_MASTER_KEK once every deployment sharing this database confirms the same."
            );
        } else {
            println!(
                "{} row(s) still on the legacy DEK. Nothing was written — re-run with \
                 --execute to convert them (see this binary's own doc comment for the full \
                 runbook).",
                outcome.rows_seen
            );
        }
        return if outcome.errors > 0 { ExitCode::FAILURE } else { ExitCode::SUCCESS };
    }

    if outcome.errors == 0 && outcome.rows_migrated == outcome.rows_seen {
        println!("done: every selected row is now on the wrapped DEK.");
        ExitCode::SUCCESS
    } else {
        println!(
            "{} row(s) remain unmigrated this pass — re-run this backfill to retry (idempotent, \
             safe to re-run at any time). See the runbook in this binary's own doc comment.",
            outcome.rows_seen.saturating_sub(outcome.rows_migrated)
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
