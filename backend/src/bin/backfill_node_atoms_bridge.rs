//! `backfill_node_atoms_bridge`: one-shot, per database. Mirror existing
//! managed-extraction reef links (`node_semantic_atoms`) into `node_atoms`,
//! so reefs extracted before the live dual write shipped show their corals
//! on every `node_atoms` reader (reef corals, MCP `get_reef`, atlas,
//! digests).
//!
//! **Thin wrapper.** Counts and writes live in
//! [`coralswarm_backend::node_atoms_bridge`]; read its module doc for the
//! passage → chunk mapping. BRIDGE: delete with that module (pipeline plan
//! PR 11).
//!
//! ## Run once per database
//!
//! `node_atoms`/`semantic_atoms` live per database: the control plane
//! (consumer oceans) and every org tenant each hold their own. Exactly one
//! selector is required, so each invocation's scope is legible:
//!
//! ```text
//! backfill_node_atoms_bridge [--execute] [--graph-id <uuid>]
//!     [--batch-size <N>] [--max-batches <N>]
//!     (--tenant-dsn <dsn> | --only <slug|clerk_org_id> | --all-tenants | --control)
//! ```
//!
//! Default is a dry run in a READ ONLY transaction. `--execute` inserts,
//! `--batch-size` (reef, version) pairs per committed transaction (default
//! 200). `--max-batches` bounds a rehearsal. Re-running is safe: the
//! `(node_id, atom_id)` primary key makes every repeat a no-op, and a
//! finished run's second pass finds nothing pending.
//!
//! ## Env
//!
//!   MIGRATOR_DATABASE_URL  elevated control-plane DSN (falls back to
//!                          DATABASE_URL). Required by --control / --only /
//!                          --all-tenants.
//!   TENANT_DB_<SLUG>       each tenant's owner DSN, via OperatorDsnResolver
//!                          (the same convention as migrate_tenants/reextract).
//!
//! Every DSN must bypass RLS on the tables touched (owner role without FORCE,
//! or BYPASSRLS). The tool refuses otherwise.
//!
//! ## Exit codes
//!
//!   0  every selected database processed
//!   1  a database failed (connection, preflight, fingerprint, query)
//!   2  usage error, missing control DSN, or unknown --only tenant

use std::process::ExitCode;
use std::time::{Duration, Instant};

use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use uuid::Uuid;

use coralswarm_backend::migrator_database_url;
use coralswarm_backend::node_atoms_bridge::{self, DEFAULT_BATCH_SIZE};
use coralswarm_backend::tenancy::routing::{self, TenantDatabase};
use coralswarm_backend::tenancy::secrets::OperatorDsnResolver;

const USAGE: &str = "usage: backfill_node_atoms_bridge [--execute] [--graph-id <uuid>] \
[--batch-size <N>] [--max-batches <N>] \
(--tenant-dsn <dsn> | --only <slug|clerk_org_id> | --all-tenants | --control)";

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
    batch_size: i64,
    max_batches: Option<u64>,
    selector: Selector,
}

fn parse_args(args: impl Iterator<Item = String>) -> Result<Args, String> {
    let mut execute = false;
    let mut graph_id = None;
    let mut batch_size = DEFAULT_BATCH_SIZE;
    let mut max_batches = None;
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
            "--batch-size" => {
                let v = args.next().ok_or("--batch-size requires a value")?;
                batch_size = v
                    .parse::<i64>()
                    .ok()
                    .filter(|n| (1..=5000).contains(n))
                    .ok_or("--batch-size must be 1..=5000")?;
            }
            "--max-batches" => {
                let v = args.next().ok_or("--max-batches requires a value")?;
                max_batches = Some(
                    v.parse::<u64>()
                        .ok()
                        .filter(|n| *n > 0)
                        .ok_or("--max-batches must be a positive integer")?,
                );
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
        batch_size,
        max_batches,
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
    let preview = match node_atoms_bridge::preview(pool, args.graph_id).await {
        Ok(p) => p,
        Err(e) => {
            eprintln!("FAILED [{label}] preview: {e}");
            return false;
        }
    };
    println!("[{label}] database={db}");
    println!(
        "  semantic_atoms={} reef_version_pairs={} pairs_without_chunks={} \
         missing_links={} reefs_touched={} reefs_currently_empty={}",
        preview.semantic_atoms,
        preview.reef_version_pairs,
        preview.pairs_without_chunks,
        preview.missing_links,
        preview.reefs_touched,
        preview.reefs_currently_empty
    );
    if !args.execute {
        println!("  dry run: nothing written. Pass --execute to insert.");
        eprintln!(
            "summary: [{label}] mode=dry-run missing_links={}",
            preview.missing_links
        );
        return true;
    }
    let started = Instant::now();
    let result = node_atoms_bridge::execute(
        pool,
        args.graph_id,
        args.batch_size,
        args.max_batches,
        |b| println!("  batch pairs={} inserted={}", b.pairs, b.inserted),
    )
    .await;
    match result {
        Ok(r) => {
            println!(
                "  done: batches={} pairs={} inserted={} elapsed={:.1}s",
                r.batches,
                r.pairs,
                r.inserted,
                started.elapsed().as_secs_f64()
            );
            eprintln!(
                "summary: [{label}] mode=execute batches={} pairs={} inserted={}",
                r.batches, r.pairs, r.inserted
            );
            true
        }
        Err(e) => {
            eprintln!(
                "FAILED [{label}] execute: {e} (committed batches stay; re-run resumes)"
            );
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
        "backfill_node_atoms_bridge (mode={}, batch_size={}, max_batches={:?})",
        if args.execute { "EXECUTE" } else { "DRY RUN" },
        args.batch_size,
        args.max_batches
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
            let resolver = OperatorDsnResolver::from_env_or_env_only(control.clone(), "backfill_node_atoms_bridge");
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
        assert!(matches!(a.selector, Selector::Only(ref s) if s == "coralswarm"));
    }

    #[test]
    fn bounds_are_validated() {
        assert!(parse(&["--control", "--batch-size", "0"]).is_err());
        assert!(parse(&["--control", "--max-batches", "0"]).is_err());
        let a = parse(&["--control", "--execute", "--batch-size", "50", "--max-batches", "2"]).unwrap();
        assert_eq!((a.execute, a.batch_size, a.max_batches), (true, 50, Some(2)));
    }
}
