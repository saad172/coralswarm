//! `backfill_co_occurrence`: write statement co-occurrence edges
//! (`co_occurs_with`, see [`coralswarm_backend::co_occurrence`]) for every
//! graph in a database. No model call. Idempotent: a second run writes
//! nothing. The `InferRelationships` job keeps them current after this.
//!
//! ```text
//! backfill_co_occurrence [--execute] [--graph-id <uuid>]
//!     (--tenant-dsn <dsn> | --only <slug|clerk_org_id> | --all-tenants | --control)
//! ```
//!
//! Default is a dry run: each graph runs in a `READ ONLY` transaction that is
//! rolled back, and the counts say what `--execute` would write.
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
//!   0  every selected graph processed
//!   1  a database or a graph failed
//!   2  usage error, missing control DSN, or unknown --only tenant
//!   3  a DSN does not bypass RLS

use std::process::ExitCode;
use std::time::{Duration, Instant};

use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use uuid::Uuid;

use coralswarm_backend::co_occurrence::{self, Write};
use coralswarm_backend::migrator_database_url;
use coralswarm_backend::node_atoms_bridge;
use coralswarm_backend::tenancy::routing::{self, TenantDatabase};
use coralswarm_backend::tenancy::secrets::OperatorDsnResolver;

const USAGE: &str = "usage: backfill_co_occurrence [--execute] [--graph-id <uuid>] \
(--tenant-dsn <dsn> | --only <slug|clerk_org_id> | --all-tenants | --control)";

const TABLES: &[&str] = &[
    "graphs",
    "nodes",
    "node_semantic_atoms",
    "semantic_atoms",
    "source_versions",
    "source_items",
    "source_version_chunks",
    "relationship_edge",
    "op_log",
];

#[derive(Debug, PartialEq, Eq)]
enum Selector {
    TenantDsn(String),
    Only(String),
    AllTenants,
    Control,
}

#[derive(Debug)]
struct Args {
    write: Write,
    graph_id: Option<Uuid>,
    selector: Selector,
}

fn parse_args(mut args: impl Iterator<Item = String>) -> Result<Args, String> {
    let mut write = Write::DryRun;
    let mut graph_id = None;
    let mut selector: Option<Selector> = None;
    let mut set = |s: Selector| -> Result<(), String> {
        if selector.replace(s).is_some() {
            return Err("--tenant-dsn / --only / --all-tenants / --control are mutually exclusive".into());
        }
        Ok(())
    };
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--execute" => write = Write::Apply,
            "--dry-run" => write = Write::DryRun,
            "--graph-id" => {
                let v = args.next().ok_or("--graph-id requires a value")?;
                graph_id = Some(Uuid::parse_str(&v).map_err(|e| format!("--graph-id: invalid UUID: {e}"))?);
            }
            "--tenant-dsn" => set(Selector::TenantDsn(args.next().ok_or("--tenant-dsn requires a value")?))?,
            "--only" => set(Selector::Only(args.next().ok_or("--only requires a value")?))?,
            "--all-tenants" => set(Selector::AllTenants)?,
            "--control" => set(Selector::Control)?,
            "-h" | "--help" => {
                println!("{USAGE}");
                std::process::exit(0);
            }
            other => return Err(format!("unknown argument: {other}")),
        }
    }
    let selector = selector.ok_or("exactly one of --tenant-dsn / --only / --all-tenants / --control is required")?;
    Ok(Args { write, graph_id, selector })
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

/// DSN-swap guard: the tenant DB's `tenant_meta` fingerprint must match the
/// routing row before anything is read.
async fn connect_tenant(resolver: &OperatorDsnResolver, t: &TenantDatabase) -> Result<PgPool, String> {
    // Environment first, then the KEK-encrypted tenant_db_credentials row
    // (self-serve tenants have no Fly secret; issue #1514). Labelled by slug:
    // the error never carries a DSN.
    let dsn = resolver
        .resolve(&t.db_secret_ref)
        .await
        .map_err(|e| format!("resolving tenant DSN for {}: {e}", t.slug))?;
    let pool = connect(&dsn).await?;
    let fingerprint: Option<String> = sqlx::query_scalar("SELECT org_id FROM tenant_meta WHERE id = true")
        .fetch_optional(&pool)
        .await
        .map_err(|e| format!("tenant_meta fingerprint read: {e}"))?;
    if fingerprint.as_deref() == Some(t.clerk_org_id.as_str()) {
        return Ok(pool);
    }
    pool.close().await;
    Err(format!(
        "tenant_meta fingerprint {fingerprint:?} does not match routing row {}; refusing (DSN-swap guard)",
        t.clerk_org_id
    ))
}

async fn run_one(label: &str, pool: &PgPool, args: &Args) -> Result<(), (u8, String)> {
    node_atoms_bridge::assert_policy_bypass_on(pool, TABLES).await.map_err(|m| (3, m))?;
    let db: String = sqlx::query_scalar("SELECT current_database()").fetch_one(pool).await.map_err(|e| (1, e.to_string()))?;
    println!("[{label}] database={db}");
    let started = Instant::now();
    let out = co_occurrence::backfill(pool, args.graph_id, args.write).await.map_err(|e| (1, e.to_string()))?;
    let t = &out.total;
    for (graph, error) in &out.failed {
        eprintln!("  graph {graph} failed: {error}");
    }
    let verb = if args.write == Write::Apply { "written" } else { "would write (dry run: nothing written)" };
    eprintln!(
        "summary: [{label}] rule={} graphs={} desired={} {verb}: inserted={} updated={} deleted={} unchanged={} rejected_kept={} failed={} elapsed={:.1}s",
        co_occurrence::RULE, out.graphs, t.desired, t.inserted, t.updated, t.deleted, t.unchanged, t.rejected_kept,
        out.failed.len(), started.elapsed().as_secs_f64()
    );
    if out.failed.is_empty() {
        Ok(())
    } else {
        Err((1, format!("{} graphs failed", out.failed.len())))
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
    println!("backfill_co_occurrence (run={})", if args.write == Write::Apply { "EXECUTE" } else { "DRY RUN" });
    let control = match (&args.selector, migrator_database_url()) {
        (Selector::TenantDsn(_), _) => None,
        (_, Some(url)) => match connect(&url).await {
            Ok(p) => Some(p),
            Err(e) => {
                eprintln!("failed to connect control-plane database: {e}");
                return ExitCode::FAILURE;
            }
        },
        (_, None) => {
            eprintln!("MIGRATOR_DATABASE_URL (preferred) or DATABASE_URL must be set for --control/--only/--all-tenants");
            return ExitCode::from(2);
        }
    };
    let mut code = 0u8;
    let mut record = |label: &str, r: Result<(), (u8, String)>| {
        if let Err((c, msg)) = r {
            eprintln!("FAILED [{label}]: {msg}");
            code = code.max(c);
        }
    };
    match &args.selector {
        Selector::TenantDsn(dsn) => match connect(dsn).await {
            Ok(pool) => {
                record("tenant-dsn", run_one("tenant-dsn", &pool, &args).await);
                pool.close().await;
            }
            Err(e) => {
                eprintln!("failed to connect: {e}");
                return ExitCode::FAILURE;
            }
        },
        Selector::Control => {
            let c = control.as_ref().expect("control pool");
            record("control", run_one("control", c, &args).await);
        }
        Selector::Only(_) | Selector::AllTenants => {
            let c = control.as_ref().expect("control pool");
            let tenants: Vec<TenantDatabase> = match &args.selector {
                Selector::Only(name) => {
                    let row = match routing::lookup_by_slug(c, name).await {
                        Ok(Some(r)) => Ok(Some(r)),
                        Ok(None) => routing::lookup_by_org(c, name).await,
                        Err(e) => Err(e),
                    };
                    match row {
                        Ok(Some(r)) => vec![r],
                        Ok(None) => {
                            eprintln!("no tenant_databases routing row for '{name}'");
                            return ExitCode::from(2);
                        }
                        Err(e) => {
                            eprintln!("routing lookup failed: {e}");
                            return ExitCode::FAILURE;
                        }
                    }
                }
                _ => match routing::list_active(c).await {
                    Ok(rows) => rows,
                    Err(e) => {
                        eprintln!("failed to enumerate active tenants: {e}");
                        return ExitCode::FAILURE;
                    }
                },
            };
            let resolver = OperatorDsnResolver::from_env_or_env_only(c.clone(), "backfill_co_occurrence");
            for t in &tenants {
                let label = format!("tenant:{}", t.slug);
                match connect_tenant(&resolver, t).await {
                    Ok(pool) => {
                        record(&label, run_one(&label, &pool, &args).await);
                        pool.close().await;
                    }
                    Err(e) => record(&label, Err((1, e))),
                }
            }
        }
    }
    if let Some(c) = control {
        c.close().await;
    }
    ExitCode::from(code)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(v: &[&str]) -> Result<Args, String> {
        parse_args(v.iter().map(|s| s.to_string()))
    }

    #[test]
    fn dry_run_is_the_default_and_selectors_are_exclusive() {
        assert!(parse(&[]).is_err());
        assert!(parse(&["--only", "a", "--control"]).is_err());
        let a = parse(&["--only", "coralswarm"]).unwrap();
        assert_eq!((a.write, a.graph_id, a.selector), (Write::DryRun, None, Selector::Only("coralswarm".into())));
        assert_eq!(parse(&["--control", "--execute"]).unwrap().write, Write::Apply);
        assert!(parse(&["--control", "--graph-id", "nope"]).is_err());
    }
}
