//! `repair_sweep_duplicates`: merge the reefs `resolve_ambiguous_mentions`
//! minted that duplicate an existing reef under a longer or shorter name
//! (v3.4.3 gate: "c2980b CI" next to "c2980b", a full path next to a
//! shorter one). Policy in
//! [`coralswarm_backend::mention_sweep::repair_minted_duplicates`].
//!
//! ```text
//! repair_sweep_duplicates [--execute] [--graph-id <uuid>]
//!     [--created-after <rfc3339>] [--created-before <rfc3339>]
//!     (--tenant-dsn <dsn> | --control)
//! ```
//!
//! Default is a dry run: nothing is written and the pairs are listed (ids
//! only, no titles). `--execute` merges each pair in its own transaction
//! through `Store::merge_nodes_scoped`, which records `node_merges`, so
//! every merge can be undone (`Store::unmerge_nodes`). Re-running finds
//! nothing for merged pairs.
//!
//! The DSN must bypass RLS on the tables touched (the tenant owner DSN, or
//! the migrator DSN). Exit codes: 0 ok, 1 a merge or query failed, 2 usage,
//! 3 the DSN does not bypass RLS.

use std::process::ExitCode;
use std::time::Duration;

use chrono::{DateTime, Utc};
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

use coralswarm_backend::mention_sweep::{self, Write};
use coralswarm_backend::migrator_database_url;
use coralswarm_backend::node_atoms_bridge;

const USAGE: &str = "usage: repair_sweep_duplicates [--execute] [--graph-id <uuid>] [--created-after <rfc3339>] \
[--created-before <rfc3339>] (--tenant-dsn <dsn> | --control)";

const TABLES: &[&str] = &["graphs", "nodes", "node_merges", "reef_judgement", "node_semantic_atoms", "node_atoms", "relationship_edge"];

#[derive(Debug, PartialEq, Eq)]
enum Selector {
    TenantDsn(String),
    Control,
}

#[derive(Debug)]
struct Args {
    write: Write,
    graph_id: Option<Uuid>,
    after: Option<DateTime<Utc>>,
    before: Option<DateTime<Utc>>,
    selector: Selector,
}

fn parse_args(mut args: impl Iterator<Item = String>) -> Result<Args, String> {
    let mut write = Write::DryRun;
    let (mut graph_id, mut after, mut before, mut selector) = (None, None, None, None);
    let time = |flag: &str, v: Option<String>| -> Result<DateTime<Utc>, String> {
        let v = v.ok_or(format!("{flag} requires a value"))?;
        DateTime::parse_from_rfc3339(&v).map(|t| t.with_timezone(&Utc)).map_err(|e| format!("{flag}: {e}"))
    };
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--execute" => write = Write::Apply,
            "--dry-run" => write = Write::DryRun,
            "--graph-id" => {
                let v = args.next().ok_or("--graph-id requires a value")?;
                graph_id = Some(Uuid::parse_str(&v).map_err(|e| format!("--graph-id: invalid UUID: {e}"))?);
            }
            "--created-after" => after = Some(time("--created-after", args.next())?),
            "--created-before" => before = Some(time("--created-before", args.next())?),
            "--tenant-dsn" | "--control" if selector.is_some() => return Err("--tenant-dsn / --control are mutually exclusive".into()),
            "--tenant-dsn" => selector = Some(Selector::TenantDsn(args.next().ok_or("--tenant-dsn requires a value")?)),
            "--control" => selector = Some(Selector::Control),
            "-h" | "--help" => {
                println!("{USAGE}");
                std::process::exit(0);
            }
            other => return Err(format!("unknown argument: {other}")),
        }
    }
    let selector = selector.ok_or("one of --tenant-dsn / --control is required")?;
    Ok(Args { write, graph_id, after, before, selector })
}

#[tokio::main]
async fn main() -> ExitCode {
    let args = match parse_args(std::env::args().skip(1)) {
        Ok(a) => a,
        Err(msg) => {
            eprintln!("{msg}\n{USAGE}");
            return ExitCode::from(2);
        }
    };
    let dsn = match &args.selector {
        Selector::TenantDsn(d) => d.clone(),
        Selector::Control => match migrator_database_url() {
            Some(u) => u,
            None => {
                eprintln!("MIGRATOR_DATABASE_URL (preferred) or DATABASE_URL must be set for --control");
                return ExitCode::from(2);
            }
        },
    };
    let pool = match PgPoolOptions::new().max_connections(2).acquire_timeout(Duration::from_secs(15)).connect(&dsn).await {
        Ok(p) => p,
        Err(e) => {
            eprintln!("connect: {e}");
            return ExitCode::FAILURE;
        }
    };
    if let Err(m) = node_atoms_bridge::assert_policy_bypass_on(&pool, TABLES).await {
        eprintln!("{m}");
        return ExitCode::from(3);
    }
    let run = if args.write == Write::Apply { "EXECUTE" } else { "DRY RUN" };
    println!("repair_sweep_duplicates worker={} run={run}", mention_sweep::WORKER);
    let report = match mention_sweep::repair_minted_duplicates(&pool, args.graph_id, (args.after, args.before), args.write).await {
        Ok(r) => r,
        Err(e) => {
            eprintln!("FAILED: {e}");
            return ExitCode::FAILURE;
        }
    };
    pool.close().await;
    for pair in &report.pairs {
        println!("  graph={} dup={} -> keep={}", pair.graph_id, pair.dup, pair.keep);
    }
    for (dup, error) in &report.failed {
        eprintln!("  {dup} failed: {error}");
    }
    let verb = if args.write == Write::Apply { "merged" } else { "would merge (dry run: nothing written)" };
    eprintln!(
        "summary: examined={} {verb}={} no_match={} ambiguous={} judged_distinct={} failed={}",
        report.examined, report.pairs.len(), report.no_match, report.ambiguous, report.judged_distinct, report.failed.len()
    );
    if report.failed.is_empty() { ExitCode::SUCCESS } else { ExitCode::FAILURE }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(v: &[&str]) -> Result<Args, String> {
        parse_args(v.iter().map(|s| s.to_string()))
    }

    #[test]
    fn dry_run_is_the_default_and_a_selector_is_required() {
        assert!(parse(&[]).is_err());
        assert!(parse(&["--control", "--tenant-dsn", "x"]).is_err());
        let a = parse(&["--control"]).unwrap();
        assert_eq!((a.write, a.selector), (Write::DryRun, Selector::Control));
        assert_eq!(parse(&["--control", "--execute"]).unwrap().write, Write::Apply);
        let a = parse(&["--control", "--created-after", "2026-10-21T06:20:00Z", "--created-before", "2026-10-21T06:50:00Z"]).unwrap();
        assert!(a.after.unwrap() < a.before.unwrap());
        assert!(parse(&["--control", "--created-after", "yesterday"]).is_err());
    }
}
