//! `purge_desktop_data` — the one-shot that deletes the retired desktop
//! app's work-session data (ADR 0015) from the control plane and every
//! active tenant database.
//!
//! **This binary is a thin wrapper.** What is deleted, how, and why it is
//! safe to re-run is in [`coralswarm_backend::desktop_purge`]; this file
//! parses arguments, resolves DSNs, loops the selected databases and
//! reports. Runbook: `docs/runbooks/purge-desktop-data.md`.
//!
//! ## Usage
//!
//!   purge_desktop_data [--execute] [--dry-run]
//!       [--control-only | --only <slug|clerk_org_id>] [--only-graph <uuid>]
//!
//! `--dry-run` IS THE DEFAULT: it counts, per database, the work-session
//! source items, atoms, sessions (deleted and kept) and their op_log rows,
//! the reefs that would lose all evidence, the blob keys that would be
//! queued or skipped, and the derived read surfaces the deletion
//! invalidates (conversations, answers, query history, journal rollups,
//! MCP tasks, extraction cache) with the graphs and accounts they belong
//! to. It writes nothing. The founder approves those counts before
//! `--execute`. Passing both flags is a usage error. `--only <tenant>`
//! excludes the control plane and requires an active, unrevoked tenant;
//! `--only-graph` narrows to one ocean (rehearsal only).
//!
//! The source-blob-cleanup worker drains the purge dependencies and
//! `DeleteSourceBlob` jobs an execute run leaves queued; the run prints
//! how many are pending so the operator can watch them reach zero.
//!
//! ## Env
//!
//!   MIGRATOR_DATABASE_URL  elevated control-plane DSN (DATABASE_URL is the
//!                          fallback, as in `migrate_control`). The purge
//!                          reads and deletes across every account, so
//!                          `desktop_purge::assert_policy_bypass` refuses a
//!                          role under RLS.
//!   TENANT_DB_<SLUG>       tenant owner DSNs, via `OperatorDsnResolver`.
//!
//! ## Exit codes
//!
//!   0  every selected database was processed and, after `--execute`, holds
//!      no work-session data
//!   1  a database could not be processed (connection, DSN, preflight,
//!      fingerprint mismatch), a source failed to purge, or data remains.
//!      The run continues past each failure; fix and re-run (idempotent).
//!   2  usage error, no control-plane DSN, or `--only` named no tenant or
//!      a tenant that is not active / whose org is revoked
//!
//! Every run prints one `summary:` line per database to stderr, plus TOTAL.

use std::process::ExitCode;
use std::time::Duration;

use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use uuid::Uuid;

use coralswarm_backend::desktop_purge::{purge_database, report_block, summary_line, PurgeOpts, Report};
use coralswarm_backend::migrator_database_url;
use coralswarm_backend::tenancy::routing::{self, TenantDatabase};
use coralswarm_backend::tenancy::secrets::OperatorDsnResolver;

const USAGE: &str = "usage: purge_desktop_data [--execute] [--dry-run] \
[--control-only | --only <slug|clerk_org_id>] [--only-graph <uuid>]";

const BATCH: i64 = 200;

struct Args {
    opts: PurgeOpts,
    control_only: bool,
    only: Option<String>,
}

fn parse_args() -> Result<Args, String> {
    let mut execute = false;
    let mut explicit_dry_run = false;
    let mut control_only = false;
    let mut only: Option<String> = None;
    let mut only_graph: Option<Uuid> = None;

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--execute" => execute = true,
            "--dry-run" => explicit_dry_run = true,
            "--control-only" => control_only = true,
            "--only" => only = Some(args.next().ok_or("--only requires a value")?),
            "--only-graph" => {
                let v = args.next().ok_or("--only-graph requires a value")?;
                only_graph =
                    Some(Uuid::parse_str(&v).map_err(|_| format!("--only-graph: not a UUID: {v}"))?);
            }
            "-h" | "--help" => {
                println!("{USAGE}");
                std::process::exit(0);
            }
            other => return Err(format!("unknown argument: {other}")),
        }
    }
    if execute && explicit_dry_run {
        return Err("--execute and --dry-run are mutually exclusive".to_string());
    }
    if control_only && only.is_some() {
        return Err("--control-only and --only are mutually exclusive".to_string());
    }
    Ok(Args { opts: PurgeOpts { execute, only_graph, batch: BATCH }, control_only, only })
}

/// `max_connections(2)`: the cascade holds one connection for its
/// transaction while the audit write may take a second.
async fn connect(dsn: &str) -> Result<PgPool, String> {
    PgPoolOptions::new()
        .max_connections(2)
        .acquire_timeout(Duration::from_secs(15))
        .connect(dsn)
        .await
        .map_err(|e| format!("connect: {e}"))
}

/// Resolve a tenant's DSN and refuse unless `tenant_meta` names the org the
/// routing row names: a swapped `TENANT_DB_<SLUG>` must never purge the
/// wrong org's data.
async fn connect_tenant(resolver: &OperatorDsnResolver, t: &TenantDatabase) -> Result<PgPool, String> {
    let dsn = resolver
        .resolve(&t.db_secret_ref)
        .await
        .map_err(|e| format!("resolving tenant DSN for {}: {e}", t.slug))?;
    let pool = connect(&dsn).await?;
    let fingerprint: Option<String> =
        sqlx::query_scalar("SELECT org_id FROM tenant_meta WHERE id = true")
            .fetch_optional(&pool)
            .await
            .map_err(|e| format!("tenant_meta fingerprint read: {e}"))?;
    if fingerprint.as_deref() == Some(t.clerk_org_id.as_str()) {
        return Ok(pool);
    }
    pool.close().await;
    Err(format!(
        "tenant_meta fingerprint {fingerprint:?} does not match routing org {} — refusing (DSN-swap guard)",
        t.clerk_org_id
    ))
}

async fn run_one(label: &str, pool: &PgPool, control: &PgPool, opts: &PurgeOpts, total: &mut Report) -> bool {
    match purge_database(pool, Some(control), opts).await {
        Ok(report) => {
            print!("{}", report_block(label, opts, &report));
            eprintln!("{}", summary_line(label, opts, &report));
            total.add(&report);
            report.clean(opts)
        }
        Err(e) => {
            eprintln!("FAILED [{label}]: {e}");
            false
        }
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let args = match parse_args() {
        Ok(a) => a,
        Err(msg) => {
            eprintln!("{msg}");
            eprintln!("{USAGE}");
            return ExitCode::from(2);
        }
    };
    let Some(control_url) = migrator_database_url() else {
        eprintln!("MIGRATOR_DATABASE_URL (preferred) or DATABASE_URL must be set to an ELEVATED control-plane DSN");
        return ExitCode::from(2);
    };

    println!("purge_desktop_data — ONE-SHOT (mode={})", args.opts.mode());
    if let Some(g) = args.opts.only_graph {
        println!("  narrowed to ocean {g} — a rehearsal, NOT a complete run");
    }

    let control = match connect(&control_url).await {
        Ok(p) => p,
        Err(e) => {
            eprintln!("failed to connect control-plane database: {e}");
            return ExitCode::FAILURE;
        }
    };

    let mut total = Report::default();
    let mut all_clean = true;

    let tenants: Vec<TenantDatabase> = if let Some(name) = &args.only {
        let row = match routing::lookup_by_slug(&control, name).await {
            Ok(Some(r)) => Ok(Some(r)),
            Ok(None) => routing::lookup_by_org(&control, name).await,
            Err(e) => Err(e),
        };
        match row {
            // The same gate as the full run: `routing::list_active` (status
            // 'active' and no `org_revocations` row) must include this tenant.
            Ok(Some(r)) => match routing::list_active(&control).await {
                Ok(active) if active.iter().any(|t| t.clerk_org_id == r.clerk_org_id) => vec![r],
                Ok(_) => {
                    eprintln!(
                        "tenant '{name}' (org {}, status {}) is not active or its org is revoked; \
                         the purge runs only on tenants routing::list_active serves",
                        r.clerk_org_id, r.status
                    );
                    return ExitCode::from(2);
                }
                Err(e) => {
                    eprintln!("failed to enumerate active tenants: {e}");
                    return ExitCode::FAILURE;
                }
            },
            Ok(None) => {
                eprintln!("no tenant_databases routing row for '{name}'");
                return ExitCode::from(2);
            }
            Err(e) => {
                eprintln!("routing lookup failed: {e}");
                return ExitCode::FAILURE;
            }
        }
    } else {
        all_clean &= run_one("control", &control, &control, &args.opts, &mut total).await;
        if args.control_only {
            Vec::new()
        } else {
            match routing::list_active(&control).await {
                Ok(rows) => rows,
                Err(e) => {
                    eprintln!("failed to enumerate active tenants: {e}");
                    return ExitCode::FAILURE;
                }
            }
        }
    };

    let resolver = OperatorDsnResolver::from_env_or_env_only(control.clone(), "purge_desktop_data");
    for t in &tenants {
        let label = format!("tenant:{}", t.slug);
        match connect_tenant(&resolver, t).await {
            Ok(pool) => {
                all_clean &= run_one(&label, &pool, &control, &args.opts, &mut total).await;
                pool.close().await;
            }
            Err(e) => {
                eprintln!("FAILED [{label}]: {e}");
                all_clean = false;
            }
        }
    }
    control.close().await;

    print!("{}", report_block("TOTAL", &args.opts, &total));
    eprintln!("{}", summary_line("TOTAL", &args.opts, &total));
    if args.opts.execute {
        println!(
            "The source-blob-cleanup worker drains the pending purge dependencies and \
             DeleteSourceBlob jobs above. Re-run the dry run until every count is 0, and \
             watch `pending_purge_deps`/`pending_blob_jobs` fall (runbook: verify)."
        );
    } else {
        println!("Nothing was written. Get the counts above approved, then re-run with --execute.");
    }

    if all_clean {
        ExitCode::SUCCESS
    } else {
        eprintln!("one or more databases failed, reported errors, or still hold data — see above. Re-running is safe.");
        ExitCode::FAILURE
    }
}
