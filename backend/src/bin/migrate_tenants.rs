//! `migrate_tenants` — applies the per-org TENANT migration tree across the
//! tenant databases named in the control-plane `tenant_databases` table.
//!
//! Run out-of-band, NEVER at web-server boot. Since OPS-5 this binary ships in
//! the runtime image and runs on every deploy: Fly's `release_command` is
//! `/app/release.sh`, which applies the CONTROL tree (`migrate_control`) and
//! then this, to completion on a throwaway machine before any traffic is
//! routed to the new image. Boot applies neither tree, it only preflights the
//! control schema read-only. `scripts/onboard_tenant.sh` still invokes this
//! directly with `--only` for a brand-new tenant, which by definition did not
//! exist when the running version was released.
//!
//! Usage:
//!   migrate_tenants [--check] [--only <slug|clerk_org_id>] [--jobs N]
//!   migrate_tenants --resolve-dsn <SECRET_REF>   (print one DSN; see `resolve_dsn_mode`)
//!
//! Env:
//!   MIGRATOR_DATABASE_URL  elevated (owner-role) control-plane DSN. Preferred.
//!   DATABASE_URL           fallback when MIGRATOR_DATABASE_URL is unset/blank
//!                          — the pre-OPS-4 config, where DATABASE_URL still
//!                          IS the owner role. At least one of the two must
//!                          be set. Same precedence as `migrate_control`
//!                          (`migrator_database_url`), and for the same
//!                          reason: on a successful apply this orchestrator
//!                          writes the applied version back onto the control
//!                          `tenant_databases` row
//!                          (`tenancy::routing::set_schema_version`), an
//!                          UPDATE that migration `0083_consumer_rls` (OPS-4)
//!                          REVOKEs from the least-privilege `coralswarm_app`
//!                          role — the role `DATABASE_URL` points at once the
//!                          serving app has cut over. Reads (tenant
//!                          enumeration) would still work under
//!                          `coralswarm_app` (SELECT on `tenant_databases`
//!                          stays granted), but this binary uses ONE pool for
//!                          both, so it needs the elevated DSN either way.
//!   Tenant DSNs are resolved by their `db_secret_ref` (e.g.
//!   TENANT_DB_ACME_CORP) from the environment FIRST (EnvSecretResolver —
//!   every concierge tenant, unchanged) and then from the KEK-encrypted
//!   `tenant_db_credentials` control table (self-serve tenants, migration
//!   0381) via `ChainSecretResolver`, which needs MASTER_KEK_HEX. Each tenant
//!   database has its own owner-role credential either way.
//!
//! Exit codes (a deploy gates on these — see `backend/release.sh`):
//!   0  every selected tenant is at the tree head (migrated or already
//!      up-to-date), or no tenant matched the selection at all
//!   1  at least one tenant FAILED (or, under `--check`, DRIFTED), or the
//!      control plane itself was unreachable
//!   2  usage error / no control-plane DSN in the environment
//!   3  no hard failure, but at least one tenant was SKIPPED (its advisory
//!      lock was held by another runner), so this run did NOT establish
//!      `schema_version >= head_version()` for it. Distinct from 1 because the
//!      remedy differs: 3 means "re-run once the other orchestrator finishes",
//!      not "something is broken". Non-zero on purpose — a silent skip is
//!      exactly the invisible gap that leaves an org 503ing.
//!
//! Every run prints a one-line `summary:` tally to stderr before exiting, so a
//! deploy log always says how many tenants were migrated vs skipped vs failed.

use std::process::ExitCode;
use std::time::Duration;

use coralswarm_backend::migrator_database_url;
use coralswarm_backend::tenancy::migrate::{self, MigrateOpts, Mode, TenantResult};
use coralswarm_backend::tenancy::secrets::OperatorDsnResolver;
use sqlx::postgres::PgPoolOptions;

#[tokio::main]
async fn main() -> ExitCode {
    // Handled before the tracing subscriber exists: `fmt()` writes to
    // stdout, and in this mode stdout must carry the DSN and nothing else.
    let raw: Vec<String> = std::env::args().skip(1).collect();
    if raw.first().map(String::as_str) == Some("--resolve-dsn") {
        return resolve_dsn_mode(&raw[1..]).await;
    }

    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let opts = match parse_args() {
        Ok(o) => o,
        Err(msg) => {
            eprintln!("{msg}");
            eprintln!("usage: migrate_tenants [--check] [--only <slug|clerk_org_id>] [--jobs N]");
            return ExitCode::from(2);
        }
    };

    // MIGRATOR_DATABASE_URL wins; DATABASE_URL is the pre-OPS-4 fallback —
    // same precedence/blank-handling as `migrate_control`'s `control_url`
    // resolution (see the module doc comment for why this binary needs the
    // elevated role too).
    let control_url = match migrator_database_url() {
        Some(u) => u,
        None => {
            eprintln!(
                "MIGRATOR_DATABASE_URL (preferred) or DATABASE_URL must be set to an elevated \
                 control-plane DSN — the role must own the `tenant_databases` row it records \
                 schema_version onto (post-OPS-4 the least-privilege `coralswarm_app` role \
                 deliberately cannot write it)"
            );
            return ExitCode::from(2);
        }
    };

    let control = match PgPoolOptions::new()
        .max_connections(4)
        .acquire_timeout(Duration::from_secs(15))
        .connect(&control_url)
        .await
    {
        Ok(p) => p,
        Err(e) => {
            eprintln!("failed to connect control-plane database: {e}");
            return ExitCode::FAILURE;
        }
    };

    // Environment first (every concierge tenant, byte-identical), then the
    // KEK-encrypted `tenant_db_credentials` table (self-serve tenants, PR
    // A1a). The chain needs MASTER_KEK_HEX, which release machines receive as
    // an app-wide Fly secret; without it (dev without a KEK) the table is
    // unreachable and this binary behaves exactly as before.
    // Shared with every per-tenant operator bin (issue #1514).
    let resolver = OperatorDsnResolver::from_env_or_env_only(control.clone(), "migrate_tenants");
    let report = match migrate::migrate_all(&control, resolver.secret_resolver(), &opts).await {
        Ok(r) => r,
        Err(e) => {
            eprintln!("failed to enumerate tenants: {e}");
            return ExitCode::FAILURE;
        }
    };

    let verb = match opts.mode {
        Mode::Migrate => "migrate",
        Mode::Check => "check",
    };
    if report.outcomes.is_empty() {
        println!("{verb}: no matching tenants");
    }
    for o in &report.outcomes {
        let line = match &o.result {
            TenantResult::Migrated { applied_to_version } => {
                format!("OK migrated → v{applied_to_version}")
            }
            TenantResult::UpToDate { version } => format!("OK up-to-date (v{version})"),
            TenantResult::Drift { detail } => format!("DRIFT {detail}"),
            TenantResult::Skipped { reason } => format!("SKIP {reason}"),
            TenantResult::Failed { error } => format!("FAIL {error}"),
            TenantResult::AheadOfBuild { version, head } => {
                format!("FAIL the database records tenant migration {version}, ahead of this build's head v{head} (a newer build migrated it)")
            }
        };
        println!("  [{}] {} — {}", o.slug, o.clerk_org_id, line);
    }

    // One machine-greppable tally, ALWAYS printed. This is what makes a deploy
    // that skipped a tenant visibly different from one that migrated them all:
    // the release log otherwise ends on an exit code nobody reads.
    let c = report.counts();
    eprintln!(
        "summary: {verb} {} tenant(s) at tenant-tree head v{} — {} migrated, {} up-to-date, \
         {} drifted, {} skipped, {} failed",
        c.total(),
        migrate::head_version(),
        c.migrated,
        c.up_to_date,
        c.drifted,
        c.skipped,
        c.failed
    );

    if report.any_failed() {
        eprintln!("{verb}: one or more tenants failed");
        ExitCode::FAILURE
    } else if report.any_skipped() {
        // Not an error, but not success either: the advisory lock was held, so
        // those tenants' schemas are unverified by THIS run. Fail non-zero
        // (exit 3) rather than let a deploy record a green release it did not
        // earn — re-running after the other orchestrator finishes clears it.
        eprintln!(
            "{verb}: {} tenant(s) skipped (advisory lock held by another runner) — their schema \
             version is unverified by this run; re-run once it finishes",
            c.skipped
        );
        ExitCode::from(3)
    } else {
        ExitCode::SUCCESS
    }
}

/// `migrate_tenants --resolve-dsn <SECRET_REF>`: print one tenant DSN on
/// stdout (no newline) through the same chain the server uses (environment
/// first, then the KEK-encrypted `tenant_db_credentials` row), and migrate
/// nothing.
///
/// This is the nightly tenant backup's resolver
/// (`.github/workflows/backup-tenants.yml`): `printenv` covers concierge
/// tenants, whose DSN is a Fly secret, and this covers self-serve tenants,
/// whose DSN exists only in the control table. Without it the backup would
/// skip every self-serve tenant.
///
/// Read-only: the control pool prefers `DATABASE_URL` (the least-privilege
/// app role, which holds SELECT on `tenant_db_credentials`) over
/// `MIGRATOR_DATABASE_URL`. Errors go to stderr and print nothing on
/// stdout; the DSN itself is never logged. Exit 0 = printed, 1 = not
/// resolvable, 2 = usage.
async fn resolve_dsn_mode(args: &[String]) -> ExitCode {
    let [secret_ref] = args else {
        eprintln!("usage: migrate_tenants --resolve-dsn <SECRET_REF>");
        return ExitCode::from(2);
    };
    // The ref gate and the environment lookup are the shared operator
    // helper's; a concierge ref answers without opening a control pool.
    match OperatorDsnResolver::resolve_from_env(secret_ref) {
        Ok(Some(dsn)) => {
            print!("{dsn}");
            return ExitCode::SUCCESS;
        }
        Ok(None) => {}
        Err(e) => {
            // `e` deliberately does not echo the rejected value.
            eprintln!("--resolve-dsn: {e} (e.g. TENANT_DB_ACME)");
            return ExitCode::from(2);
        }
    }
    let control_url = std::env::var("DATABASE_URL")
        .ok()
        .filter(|u| !u.trim().is_empty())
        .or_else(migrator_database_url);
    let Some(control_url) = control_url else {
        eprintln!("--resolve-dsn: {secret_ref} is not in the environment and no control-plane DSN is set");
        return ExitCode::FAILURE;
    };
    let control = match PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_secs(15))
        .connect(&control_url)
        .await
    {
        Ok(p) => p,
        Err(e) => {
            eprintln!("--resolve-dsn: failed to connect control-plane database: {e}");
            return ExitCode::FAILURE;
        }
    };
    let chain = match OperatorDsnResolver::chain_from_env(control.clone()) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("--resolve-dsn: {secret_ref} is not in the environment and MASTER_KEK_HEX is unavailable ({e})");
            return ExitCode::FAILURE;
        }
    };
    let out = chain.resolve(secret_ref).await;
    control.close().await;
    match out {
        Ok(dsn) => {
            print!("{dsn}");
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("--resolve-dsn: {secret_ref}: {e}");
            ExitCode::FAILURE
        }
    }
}

fn parse_args() -> Result<MigrateOpts, String> {
    let mut opts = MigrateOpts::default();
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--check" => opts.mode = Mode::Check,
            "--only" => {
                opts.only = Some(args.next().ok_or("--only requires a value")?);
            }
            "--jobs" => {
                let n = args.next().ok_or("--jobs requires a value")?;
                opts.jobs = n
                    .parse::<usize>()
                    .map_err(|_| format!("--jobs: not a number: {n}"))?
                    .max(1);
            }
            other => return Err(format!("unknown argument: {other}")),
        }
    }
    Ok(opts)
}
