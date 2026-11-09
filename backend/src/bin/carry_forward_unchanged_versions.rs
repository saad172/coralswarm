//! `carry_forward_unchanged_versions`: repair current source versions that a
//! metadata-only re-capture minted before capture carried extraction forward
//! (#1382's anchor keys changed every Slack/Linear fingerprint). Each such
//! version has byte-identical content to an earlier extracted version of the
//! same item, but waits for extraction with no facts of its own.
//!
//! **Thin wrapper.** The work is `source_carry_forward::preview_uncarried` and
//! `source_carry_forward::carry_forward_uncarried`. A repaired version becomes
//! `done` under its earlier version's profile and cursor, exactly as
//! `capture_source_version_tracked_tx` now records such a capture. Nothing
//! is copied, deleted or queued, and no model is called. Extraction jobs
//! still queued for those versions find them `done` and complete without a
//! model call. Scoped Ask and the Library already serve the earlier facts
//! through the read fallback (`library_read::SERVED_VERSIONS_SQL`); this makes
//! the stored state agree and stops the paid re-extraction.
//!
//! ```text
//! carry_forward_unchanged_versions [--execute] [--receipts] [--graph-id <uuid>] [--limit <N>]
//!     (--tenant-dsn <dsn> | --only <slug|clerk_org_id> | --all-tenants | --control)
//! ```
//!
//! `--receipts` runs the other carry repair instead:
//! `source_carry_forward::receipt_carried` writes the missing capture
//! receipt for each version capture carried before carries were receipted
//! (tenant 0193 / control 0362). Without one, `capture_admission_health`
//! counts the version an `unknown` clock and the capture SLO stays breached.
//! The observation trigger records each new receipt `carried_forward`; the
//! run prints how many it did, and any done-but-unreceipted version that is
//! not a carry, which it leaves alone.
//!
//! Default is a dry run (READ ONLY). `--limit` (default 5000, max 20000)
//! bounds one run; repeat to continue. Idempotent.
//!
//! ## Env
//!
//!   MIGRATOR_DATABASE_URL  elevated control-plane DSN (falls back to
//!                          DATABASE_URL). Required by --control / --only /
//!                          --all-tenants.
//!   TENANT_DB_<SLUG>       each tenant's owner DSN, via OperatorDsnResolver.
//!
//! Every DSN must bypass RLS on the tables touched. The tool refuses otherwise.
//!
//! ## Exit codes
//!
//!   0  every selected database processed
//!   1  a database failed
//!   2  usage error, missing control DSN, or unknown --only tenant

use std::process::ExitCode;
use std::time::Duration;

use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use uuid::Uuid;

use coralswarm_backend::migrator_database_url;
use coralswarm_backend::{node_atoms_bridge, source_carry_forward};
use coralswarm_backend::tenancy::routing::{self, TenantDatabase};
use coralswarm_backend::tenancy::secrets::OperatorDsnResolver;

const USAGE: &str = "usage: carry_forward_unchanged_versions [--execute] [--receipts] [--graph-id <uuid>] \
[--limit <N>] (--tenant-dsn <dsn> | --only <slug|clerk_org_id> | --all-tenants | --control)";
const DEFAULT_LIMIT: i64 = 5000;

#[derive(Debug)]
enum Selector {
    TenantDsn(String),
    Only(String),
    AllTenants,
    Control,
}

#[derive(Debug)]
struct Args {
    execute: bool,
    receipts: bool,
    graph_id: Option<Uuid>,
    limit: i64,
    selector: Selector,
}

fn parse_args(mut args: impl Iterator<Item = String>) -> Result<Args, String> {
    let mut execute = false;
    let mut receipts = false;
    let mut graph_id = None;
    let mut limit = DEFAULT_LIMIT;
    let mut selector: Option<Selector> = None;
    let set = |s: Selector, selector: &mut Option<Selector>| -> Result<(), String> {
        if selector.is_some() {
            return Err("--tenant-dsn / --only / --all-tenants / --control are mutually exclusive".into());
        }
        *selector = Some(s);
        Ok(())
    };
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--execute" => execute = true,
            "--dry-run" => execute = false,
            "--receipts" => receipts = true,
            "--graph-id" => {
                let v = args.next().ok_or("--graph-id requires a value")?;
                graph_id = Some(Uuid::parse_str(&v).map_err(|e| format!("--graph-id: invalid UUID: {e}"))?);
            }
            "--limit" => {
                let v = args.next().ok_or("--limit requires a value")?;
                limit = v.parse::<i64>().ok().filter(|n| (1..=20_000).contains(n)).ok_or("--limit must be 1..=20000")?;
            }
            "--tenant-dsn" => {
                let v = args.next().ok_or("--tenant-dsn requires a value")?;
                set(Selector::TenantDsn(v), &mut selector)?;
            }
            "--only" => {
                let v = args.next().ok_or("--only requires a value")?;
                set(Selector::Only(v), &mut selector)?;
            }
            "--all-tenants" => set(Selector::AllTenants, &mut selector)?,
            "--control" => set(Selector::Control, &mut selector)?,
            "-h" | "--help" => {
                println!("{USAGE}");
                std::process::exit(0);
            }
            other => return Err(format!("unknown argument: {other}")),
        }
    }
    let selector = selector.ok_or("exactly one of --tenant-dsn / --only / --all-tenants / --control is required")?;
    Ok(Args { execute, receipts, graph_id, limit, selector })
}

/// Two connections: a one-shot must never eat a tenant's connection budget.
async fn connect(dsn: &str) -> Result<PgPool, String> {
    PgPoolOptions::new()
        .max_connections(2)
        .acquire_timeout(Duration::from_secs(15))
        .connect(dsn)
        .await
        .map_err(|e| format!("connect: {e}"))
}

/// DSN-swap guard: the tenant DB's
/// `tenant_meta` fingerprint must match the routing row before any write.
async fn connect_tenant(resolver: &OperatorDsnResolver, t: &TenantDatabase) -> Result<PgPool, String> {
    // Environment first, then the KEK-encrypted tenant_db_credentials row
    // (self-serve tenants have no Fly secret; issue #1514). Labelled by slug:
    // the error never carries a DSN.
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
        "tenant_meta fingerprint {fingerprint:?} does not match routing row {}; refusing \
         (DSN-swap guard)",
        t.clerk_org_id
    ))
}

async fn run_one(label: &str, pool: &PgPool, args: &Args) -> bool {
    if let Err(msg) = node_atoms_bridge::assert_policy_bypass(pool).await {
        eprintln!("FAILED [{label}]: {msg}");
        return false;
    }
    if args.receipts {
        return run_receipts(label, pool, args).await;
    }
    let preview = match source_carry_forward::preview_uncarried(pool, args.graph_id, args.limit).await {
        Ok(p) => p,
        Err(e) => {
            eprintln!("FAILED [{label}] preview: {e}");
            return false;
        }
    };
    println!(
        "[{label}] uncarried_versions={} prior_atoms={} queued_jobs={}",
        preview.versions, preview.prior_atoms, preview.queued_jobs
    );
    if !args.execute {
        println!("  dry run: nothing written. Pass --execute to carry them forward.");
        return true;
    }
    match source_carry_forward::carry_forward_uncarried(pool, args.graph_id, args.limit).await {
        Ok(carried) => {
            println!("  done: carried={carried}");
            true
        }
        Err(e) => {
            eprintln!("FAILED [{label}] execute: {e} (nothing committed; re-run continues)");
            false
        }
    }
}

async fn run_receipts(label: &str, pool: &PgPool, args: &Args) -> bool {
    let preview = match source_carry_forward::preview_unreceipted_carries(pool, args.graph_id, args.limit).await {
        Ok(p) => p,
        Err(e) => {
            eprintln!("FAILED [{label}] preview: {e}");
            return false;
        }
    };
    println!(
        "[{label}] unreceipted_carries={} other_unreceipted_done={}",
        preview.versions, preview.other_unreceipted
    );
    if !args.execute {
        println!("  dry run: nothing written. Pass --execute to receipt the carries.");
        return true;
    }
    match source_carry_forward::receipt_carried(pool, args.graph_id, args.limit).await {
        Ok((written, carried)) => {
            println!("  done: receipts={written} carried_forward={carried}");
            if carried != written as i64 {
                eprintln!("FAILED [{label}]: {} receipt(s) did not record carried_forward; they read unknown", written as i64 - carried);
                return false;
            }
            true
        }
        Err(e) => {
            eprintln!("FAILED [{label}] execute: {e} (nothing committed; re-run continues)");
            false
        }
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let args = match parse_args(std::env::args().skip(1)) {
        Ok(a) => a,
        Err(msg) => {
            eprintln!("{msg}");
            eprintln!("{USAGE}");
            return ExitCode::from(2);
        }
    };
    println!(
        "carry_forward_unchanged_versions (mode={}, repair={}, limit={})",
        if args.execute { "EXECUTE" } else { "DRY RUN" },
        if args.receipts { "receipts" } else { "carry" },
        args.limit
    );
    let mut clean = true;
    match &args.selector {
        Selector::TenantDsn(dsn) => match connect(dsn).await {
            Ok(pool) => {
                clean = run_one("tenant-dsn", &pool, &args).await;
                pool.close().await;
            }
            Err(e) => {
                eprintln!("failed to connect: {e}");
                return ExitCode::FAILURE;
            }
        },
        Selector::Control | Selector::Only(_) | Selector::AllTenants => {
            let Some(url) = migrator_database_url() else {
                eprintln!(
                    "MIGRATOR_DATABASE_URL (preferred) or DATABASE_URL must be set for \
                     --control/--only/--all-tenants"
                );
                return ExitCode::from(2);
            };
            let control = match connect(&url).await {
                Ok(p) => p,
                Err(e) => {
                    eprintln!("failed to connect control-plane database: {e}");
                    return ExitCode::FAILURE;
                }
            };
            let tenants: Vec<TenantDatabase> = match &args.selector {
                Selector::Control => {
                    clean = run_one("control", &control, &args).await;
                    Vec::new()
                }
                Selector::Only(name) => {
                    let row = match routing::lookup_by_slug(&control, name).await {
                        Ok(Some(r)) => Ok(Some(r)),
                        Ok(None) => routing::lookup_by_org(&control, name).await,
                        Err(e) => Err(e),
                    };
                    match row {
                        Ok(Some(r)) => vec![r],
                        Ok(None) => {
                            eprintln!("no tenant_databases routing row for '{name}'");
                            control.close().await;
                            return ExitCode::from(2);
                        }
                        Err(e) => {
                            eprintln!("routing lookup failed: {e}");
                            control.close().await;
                            return ExitCode::FAILURE;
                        }
                    }
                }
                Selector::AllTenants => match routing::list_active(&control).await {
                    Ok(rows) => {
                        if rows.is_empty() {
                            println!("--all-tenants: no active tenants in the registry");
                        }
                        rows
                    }
                    Err(e) => {
                        eprintln!("failed to enumerate active tenants: {e}");
                        control.close().await;
                        return ExitCode::FAILURE;
                    }
                },
                Selector::TenantDsn(_) => unreachable!("handled above"),
            };
            let resolver = OperatorDsnResolver::from_env_or_env_only(control.clone(), "carry_forward_unchanged_versions");
            for t in &tenants {
                let label = format!("tenant:{}", t.slug);
                match connect_tenant(&resolver, t).await {
                    Ok(pool) => {
                        clean &= run_one(&label, &pool, &args).await;
                        pool.close().await;
                    }
                    Err(e) => {
                        eprintln!("FAILED [{label}]: {e}");
                        clean = false;
                    }
                }
            }
            control.close().await;
        }
    }
    if clean {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(v: &[&str]) -> Result<Args, String> {
        parse_args(v.iter().map(|s| s.to_string()))
    }

    #[test]
    fn dry_run_is_the_default_and_selector_is_required() {
        assert!(parse(&[]).is_err());
        assert!(parse(&["--control", "--all-tenants"]).is_err());
        let a = parse(&["--only", "coralswarm"]).unwrap();
        assert!(!a.execute);
        assert_eq!(a.limit, DEFAULT_LIMIT);
        assert!(parse(&["--control", "--limit", "0"]).is_err());
        assert!(parse(&["--control", "--execute"]).unwrap().execute);
        assert!(!a.receipts);
        let r = parse(&["--only", "coralswarm", "--receipts"]).unwrap();
        assert!(r.receipts && !r.execute, "--receipts alone stays a dry run");
    }
}
