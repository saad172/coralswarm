//! `reextract_source_atoms_v2`: request re-extraction of done v1 and v2
//! source versions under source-atoms v3, per database. Bounded and
//! idempotent.
//!
//! **Thin wrapper.** The work lives in [`coralswarm_backend::source_reextract`];
//! read its module doc. This tool never calls a model: it repins versions to
//! `source-atoms-v3` and marks them `reextraction_requested`. The standing
//! sweep (with `SOURCE_EXTRACTION_PROFILE=source-atoms-v3`) then extracts
//! them through the managed gate, under `MANAGED_DAILY_CAP_USD_EXTRACTION`
//! and the per-attempt/per-version caps.
//!
//! ```text
//! reextract_source_atoms_v2 [--execute] [--graph-id <uuid>] [--limit <N>]
//!     [--include-failed] [--to v3]
//!     (--tenant-dsn <dsn> | --only <slug|clerk_org_id> | --all-tenants | --control)
//! ```
//!
//! Default is a dry run (READ ONLY) that prints versions, their windows and
//! atoms under the replaced profile, and an estimated v3 cost. `--limit`
//! (default 500, max 20000) bounds one run; repeat to continue.
//! `--include-failed` also requests failed versions. `--to v3` is the only
//! target and may be omitted.
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
use std::time::{Duration, Instant};

use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use uuid::Uuid;

use coralswarm_backend::migrator_database_url;
use coralswarm_backend::{node_atoms_bridge, source_reextract};
use coralswarm_backend::tenancy::routing::{self, TenantDatabase};
use coralswarm_backend::tenancy::secrets::OperatorDsnResolver;

const USAGE: &str = "usage: reextract_source_atoms_v2 [--execute] [--graph-id <uuid>] \
[--limit <N>] [--include-failed] [--to v3] \
(--tenant-dsn <dsn> | --only <slug|clerk_org_id> | --all-tenants | --control)";
const DEFAULT_LIMIT: i64 = 500;

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
    graph_id: Option<Uuid>,
    limit: i64,
    include_failed: bool,
    selector: Selector,
}

fn parse_args(args: impl Iterator<Item = String>) -> Result<Args, String> {
    let mut execute = false;
    let mut graph_id = None;
    let mut limit = DEFAULT_LIMIT;
    let mut include_failed = false;
    let mut selector: Option<Selector> = None;
    let set = |s: Selector, selector: &mut Option<Selector>| -> Result<(), String> {
        if selector.is_some() {
            return Err(
                "--tenant-dsn / --only / --all-tenants / --control are mutually exclusive".into(),
            );
        }
        *selector = Some(s);
        Ok(())
    };
    let mut args = args;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--execute" => execute = true,
            "--dry-run" => execute = false,
            "--graph-id" => {
                let v = args.next().ok_or("--graph-id requires a value")?;
                graph_id =
                    Some(Uuid::parse_str(&v).map_err(|e| format!("--graph-id: invalid UUID: {e}"))?);
            }
            "--limit" => {
                let v = args.next().ok_or("--limit requires a value")?;
                limit = v
                    .parse::<i64>()
                    .ok()
                    .filter(|n| (1..=20_000).contains(n))
                    .ok_or("--limit must be 1..=20000")?;
            }
            "--include-failed" => include_failed = true,
            "--to" => {
                match args.next().ok_or("--to requires a value")?.as_str() {
                    "v3" | "source-atoms-v3" => {}
                    other => return Err(format!("--to must be v3, not {other}")),
                }
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
    let selector = selector.ok_or(
        "exactly one of --tenant-dsn / --only / --all-tenants / --control is required",
    )?;
    Ok(Args {
        execute,
        graph_id,
        limit,
        include_failed,
        selector,
    })
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
    let db: String = match sqlx::query_scalar("SELECT current_database()")
        .fetch_one(pool)
        .await
    {
        Ok(name) => name,
        Err(e) => {
            eprintln!("FAILED [{label}]: {e}");
            return false;
        }
    };
    let preview = match source_reextract::preview(pool, args.graph_id, args.include_failed, args.limit).await {
        Ok(p) => p,
        Err(e) => {
            eprintln!("FAILED [{label}] preview: {e}");
            return false;
        }
    };
    println!("[{label}] database={db}");
    println!(
        "  v1/v2 -> v3: versions={} prior_windows={} prior_atoms={} estimated_cost_usd={:.2}",
        preview.versions,
        preview.windows,
        preview.prior_atoms,
        preview.windows as f64 * source_reextract::ESTIMATED_USD_PER_WINDOW_V3
    );
    if !args.execute {
        println!("  dry run: nothing written. Pass --execute to request re-extraction.");
        eprintln!("summary: [{label}] mode=dry-run versions={}", preview.versions);
        return true;
    }
    let started = Instant::now();
    match source_reextract::execute(pool, args.graph_id, args.include_failed, args.limit).await {
        Ok((repinned, skipped)) => {
            println!(
                "  done: repinned={repinned} skipped_in_flight={skipped} elapsed={:.1}s",
                started.elapsed().as_secs_f64()
            );
            eprintln!("summary: [{label}] mode=execute repinned={repinned} skipped={skipped}");
            true
        }
        Err(e) => {
            eprintln!("FAILED [{label}] execute: {e} (committed versions stay requested; re-run continues)");
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
        "reextract_source_atoms_v2 (mode={}, to=source-atoms-v3, limit={}, include_failed={})",
        if args.execute { "EXECUTE" } else { "DRY RUN" },
        args.limit,
        args.include_failed
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
            let resolver = OperatorDsnResolver::from_env_or_env_only(control.clone(), "reextract_source_atoms_v2");
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
    fn selector_is_required_and_exclusive() {
        assert!(parse(&[]).is_err());
        assert!(parse(&["--control", "--all-tenants"]).is_err());
        let a = parse(&["--only", "coralswarm"]).unwrap();
        assert!(!a.execute, "dry run is the default");
        assert_eq!((a.limit, a.include_failed), (DEFAULT_LIMIT, false));
        assert!(matches!(a.selector, Selector::Only(ref s) if s == "coralswarm"));
    }

    #[test]
    fn bounds_are_validated() {
        assert!(parse(&["--control", "--limit", "0"]).is_err());
        assert!(parse(&["--control", "--limit", "20001"]).is_err());
        let a = parse(&["--control", "--execute", "--limit", "50", "--include-failed"]).unwrap();
        assert_eq!((a.execute, a.limit, a.include_failed), (true, 50, true));
    }

    #[test]
    fn the_only_target_is_v3() {
        assert!(parse(&["--control", "--to", "v3"]).is_ok());
        assert!(parse(&["--control", "--to", "v2"]).is_err(), "nothing re-extracts under v2 on request");
        assert!(parse(&["--control", "--to", "v4"]).is_err());
        assert!(parse(&["--control", "--to"]).is_err());
    }
}
