//! `backfill_fact_supersession`: record temporal evidence and decide fact
//! supersession over the extraction v2 atoms and relations that already
//! exist, per database. No model call (the judge sweep owns ambiguous pairs).
//! Bounded and idempotent.
//!
//! **Thin wrapper.** The work lives in [`coralswarm_backend::fact_supersession`];
//! read its module doc for the state keys, the rules and the modes.
//!
//! ```text
//! backfill_fact_supersession [--execute] [--mode dry_run|live] [--graph-id <uuid>]
//!     [--repair-merges | --prune-ineligible] [--after <version_id|merge_id>] [--limit <N>]
//!     (--tenant-dsn <dsn> | --only <slug|clerk_org_id> | --all-tenants | --control)
//! ```
//!
//! Default is a dry run: every version runs inside a `READ ONLY` transaction
//! (no session SET) and the counts say what `--execute` would write.
//! `--execute --mode dry_run` records temporal evidence and `proposed`
//! decisions (readers ignore them). `--execute --mode live` also promotes
//! `proposed` rows to `active` and writes the predecessor transitions; run it
//! when `FACT_SUPERSESSION_MODE=live` is set on the app.
//!
//! Versions are processed oldest first, `--limit` per run. The last line names
//! the last version; pass it as `--after` to continue.
//!
//! `--repair-merges` walks standing reef merges (`node_merges`, not undone,
//! keeper present) oldest first instead of versions, and re-runs the merge
//! reconciliation for each: before the merge hook, a merge cascade-deleted
//! the duplicate's temporal records and decisions and nothing re-derived them
//! on the keeper. It also retires records whose relation named the
//! duplicate. `--after` is then a merge id. Same dry-run default, modes and
//! idempotency.
//!
//! `--prune-ineligible` deletes undecided decisions (`ambiguous` or
//! `proposed`, never judged) that the current rules would not make
//! (`fact_supersession::ineligible`): from captured code or test files, a
//! pair rule on a many-valued key, two names of one entity, or a "newer"
//! side picked by a tied observation or reversed by its own quote. It
//! prints the count per reason and every decision left by rule and status;
//! the dry run deletes nothing. Judged, blocked and curated rows are kept.
//! Run it after each deploy that tightens the rules, before supersession
//! goes live, so neither live promotion nor the judge acts on them.
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
//!   1  a database (or a version) failed
//!   2  usage error, missing control DSN, or unknown --only tenant
//!   3  a DSN does not bypass RLS

use std::process::ExitCode;
use std::time::{Duration, Instant};

use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use uuid::Uuid;

use coralswarm_backend::fact_supersession::{self, BackfillArgs, Mode};
use coralswarm_backend::migrator_database_url;
use coralswarm_backend::node_atoms_bridge;
use coralswarm_backend::tenancy::routing::{self, TenantDatabase};
use coralswarm_backend::tenancy::secrets::OperatorDsnResolver;

const USAGE: &str = "usage: backfill_fact_supersession [--execute] [--mode dry_run|live] [--graph-id <uuid>] \
[--repair-merges | --prune-ineligible] [--after <version_id|merge_id>] [--limit <N>] (--tenant-dsn <dsn> | --only <slug|clerk_org_id> | --all-tenants | --control)";

/// Tables this tool reads or writes.
const TABLES: &[&str] = &[
    "semantic_atoms",
    "semantic_atom_passages",
    "node_semantic_atoms",
    "relationship_edge",
    "source_versions",
    "source_items",
    "nodes",
    "non_supersession_pair",
    "source_version_observations",
    "temporal_evidence_records",
    "fact_supersession_edges",
    "node_merges",
];

#[derive(Debug)]
enum Selector {
    TenantDsn(String),
    Only(String),
    AllTenants,
    Control,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Task {
    Backfill,
    RepairMerges,
    PruneIneligible,
}

#[derive(Debug)]
struct Args {
    backfill: BackfillArgs,
    task: Task,
    selector: Selector,
}

fn parse_args(args: impl Iterator<Item = String>) -> Result<Args, String> {
    let mut backfill = BackfillArgs {
        execute: false,
        mode: Mode::DryRun,
        graph_id: None,
        after: None,
        limit: fact_supersession::BACKFILL_DEFAULT_LIMIT,
    };
    let mut selector: Option<Selector> = None;
    let mut task = Task::Backfill;
    let set = |s: Selector, selector: &mut Option<Selector>| -> Result<(), String> {
        if selector.is_some() {
            return Err("--tenant-dsn / --only / --all-tenants / --control are mutually exclusive".into());
        }
        *selector = Some(s);
        Ok(())
    };
    let uuid = |flag: &str, v: Option<String>| -> Result<Uuid, String> {
        let v = v.ok_or(format!("{flag} requires a value"))?;
        Uuid::parse_str(&v).map_err(|e| format!("{flag}: invalid UUID: {e}"))
    };
    let mut args = args;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--execute" => backfill.execute = true,
            "--repair-merges" | "--prune-ineligible" => {
                if task != Task::Backfill {
                    return Err("--repair-merges and --prune-ineligible are mutually exclusive".into());
                }
                task = if arg == "--repair-merges" { Task::RepairMerges } else { Task::PruneIneligible };
            }
            "--dry-run" => backfill.execute = false,
            "--mode" => {
                backfill.mode = match args.next().as_deref() {
                    Some("dry_run") => Mode::DryRun,
                    Some("live") => Mode::Live,
                    _ => return Err("--mode must be dry_run or live".into()),
                }
            }
            "--graph-id" => backfill.graph_id = Some(uuid("--graph-id", args.next())?),
            "--after" => backfill.after = Some(uuid("--after", args.next())?),
            "--limit" => {
                let v = args.next().ok_or("--limit requires a value")?;
                backfill.limit = v
                    .parse::<i64>()
                    .ok()
                    .filter(|n| (1..=50_000).contains(n))
                    .ok_or("--limit must be 1..=50000")?;
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
    Ok(Args { backfill, task, selector })
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

async fn run_one(label: &str, pool: &PgPool, args: &BackfillArgs, task: Task) -> Result<(), (u8, String)> {
    node_atoms_bridge::assert_policy_bypass_on(pool, TABLES).await.map_err(|m| (3, m))?;
    match task {
        Task::RepairMerges => return repair_one(label, pool, args).await,
        Task::PruneIneligible => return prune_one(label, pool, args).await,
        Task::Backfill => {}
    }
    let db: String = sqlx::query_scalar("SELECT current_database()").fetch_one(pool).await.map_err(|e| (1, e.to_string()))?;
    println!("[{label}] database={db}");
    let started = Instant::now();
    let out = fact_supersession::backfill(pool, args).await.map_err(|e| (1, e))?;
    let r = &out.report;
    println!("  v2 versions in database:            {}", out.candidates);
    println!("  versions processed this pass:       {} (failed: {})", r.versions, out.failed);
    println!("  facts:                              {} (without evidence link: {})", r.facts, r.facts_without_evidence);
    println!("  temporal asserts / withdraws:       {} / {}", r.asserts, r.withdraws);
    println!("  transitions (predecessor chains):   {}", r.transitions);
    println!("  decisions proposed / active / promoted: {} / {} / {}", r.proposed, r.active, r.promoted);
    println!("  ambiguous (judge) / blocked (non_supersession_pair): {} / {}", r.ambiguous, r.blocked);
    println!("  by rule: {}", serde_json::to_string(&r.by_rule).unwrap_or_default());
    println!("  elapsed: {:.1}s", started.elapsed().as_secs_f64());
    if let Some(last) = out.last_version {
        println!("  last version: {last} (pass --after {last} to continue)");
    }
    let verb = if args.execute { "written" } else { "would write (dry run: nothing written)" };
    eprintln!(
        "summary: [{label}] mode={} {verb} asserts={} transitions={} proposed={} active={} ambiguous={} blocked={} failed={}",
        args.mode.as_str(), r.asserts, r.transitions, r.proposed, r.active, r.ambiguous, r.blocked, out.failed
    );
    if out.failed > 0 {
        return Err((1, format!("{} versions failed", out.failed)));
    }
    Ok(())
}

async fn prune_one(label: &str, pool: &PgPool, args: &BackfillArgs) -> Result<(), (u8, String)> {
    let db: String = sqlx::query_scalar("SELECT current_database()").fetch_one(pool).await.map_err(|e| (1, e.to_string()))?;
    println!("[{label}] database={db} (pruning decisions the current rules would not make)");
    let r = fact_supersession::prune_ineligible(pool, args.graph_id, args.execute).await.map_err(|e| (1, e.to_string()))?;
    for (why, n) in &r.pruned {
        println!("  pruned {why}: {n}");
    }
    for ((rule, status), n) in &r.remaining {
        println!("  remaining {rule} / {status}: {n}");
    }
    let verb = if args.execute { "deleted" } else { "would delete (dry run: nothing written)" };
    eprintln!("summary: [{label}] prune-ineligible {verb} decisions={}", r.pruned.values().sum::<u64>());
    Ok(())
}

async fn repair_one(label: &str, pool: &PgPool, args: &BackfillArgs) -> Result<(), (u8, String)> {
    let db: String = sqlx::query_scalar("SELECT current_database()").fetch_one(pool).await.map_err(|e| (1, e.to_string()))?;
    println!("[{label}] database={db} (repairing merges)");
    let started = Instant::now();
    let out = fact_supersession::repair_merges(pool, args).await.map_err(|e| (1, e))?;
    let r = &out.report;
    println!("  standing merges in database:        {}", out.candidates);
    println!("  merges processed this pass:         {} (failed: {})", out.merges, out.failed);
    println!("  facts re-derived:                   {} (without evidence link: {})", r.facts, r.facts_without_evidence);
    println!("  temporal asserts / withdraws:       {} / {}", r.asserts, r.withdraws);
    println!("  transitions (predecessor chains):   {}", r.transitions);
    println!("  stale records retired:              {}", r.retracted);
    println!("  stale decisions removed / judged outcomes carried: {} / {}", r.decisions_removed, r.carried);
    println!("  decisions proposed / active / promoted: {} / {} / {}", r.proposed, r.active, r.promoted);
    println!("  ambiguous (judge) / blocked (non_supersession_pair): {} / {}", r.ambiguous, r.blocked);
    println!("  skipped (over {} facts): {}", fact_supersession::RECONCILE_FACT_LIMIT, r.skipped);
    println!("  elapsed: {:.1}s", started.elapsed().as_secs_f64());
    if let Some(last) = out.last_merge {
        println!("  last merge: {last} (pass --repair-merges --after {last} to continue)");
    }
    let verb = if args.execute { "written" } else { "would write (dry run: nothing written)" };
    eprintln!(
        "summary: [{label}] repair-merges mode={} {verb} asserts={} transitions={} retired={} decisions_removed={} proposed={} active={} ambiguous={} skipped={} failed={}",
        args.mode.as_str(), r.asserts, r.transitions, r.retracted, r.decisions_removed, r.proposed, r.active, r.ambiguous, r.skipped, out.failed
    );
    if out.failed > 0 || r.skipped > 0 {
        return Err((1, format!("{} merges failed, {} skipped", out.failed, r.skipped)));
    }
    Ok(())
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
        "backfill_fact_supersession (run={}, mode={}, limit={})",
        if args.backfill.execute { "EXECUTE" } else { "DRY RUN" },
        args.backfill.mode.as_str(),
        args.backfill.limit
    );
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
                record("tenant-dsn", run_one("tenant-dsn", &pool, &args.backfill, args.task).await);
                pool.close().await;
            }
            Err(e) => {
                eprintln!("failed to connect: {e}");
                return ExitCode::FAILURE;
            }
        },
        Selector::Control => {
            let c = control.as_ref().expect("control pool");
            record("control", run_one("control", c, &args.backfill, args.task).await);
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
            let resolver = OperatorDsnResolver::from_env_or_env_only(c.clone(), "backfill_fact_supersession");
            for t in &tenants {
                let label = format!("tenant:{}", t.slug);
                match connect_tenant(&resolver, t).await {
                    Ok(pool) => {
                        record(&label, run_one(&label, &pool, &args.backfill, args.task).await);
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
    fn dry_run_is_default_and_selector_is_exclusive() {
        assert!(parse(&[]).is_err());
        assert!(parse(&["--only", "a", "--control"]).is_err());
        let a = parse(&["--only", "coralswarm"]).unwrap();
        assert!(!a.backfill.execute);
        assert_eq!(a.backfill.mode, Mode::DryRun);
        assert_eq!(a.backfill.limit, fact_supersession::BACKFILL_DEFAULT_LIMIT);
    }

    #[test]
    fn bounds_and_modes_are_validated() {
        assert!(parse(&["--control", "--limit", "0"]).is_err());
        assert!(parse(&["--control", "--mode", "off"]).is_err());
        let a = parse(&["--control", "--execute", "--mode", "live", "--limit", "50"]).unwrap();
        assert_eq!((a.backfill.execute, a.backfill.mode, a.backfill.limit), (true, Mode::Live, 50));
        assert_eq!(a.task, Task::Backfill);
        assert_eq!(parse(&["--control", "--repair-merges"]).unwrap().task, Task::RepairMerges);
        assert_eq!(parse(&["--control", "--prune-ineligible"]).unwrap().task, Task::PruneIneligible);
        assert!(parse(&["--control", "--prune-ineligible", "--repair-merges"]).is_err());
    }
}
