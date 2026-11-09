//! `resolve_ambiguous_mentions`: re-resolve the entity mentions the binder
//! left `ambiguous`, against today's graph, with no model call. Policy in
//! [`coralswarm_backend::mention_sweep`].
//!
//! ```text
//! resolve_ambiguous_mentions [--execute] [--graph-id <uuid>] [--max-atoms <N>]
//!     [--decisions <path.jsonl>] (--tenant-dsn <dsn> | --control)
//! ```
//!
//! Default is a dry run: each page runs in a READ ONLY transaction that is
//! rolled back, and the counts say what `--execute` would write.
//! `--execute` commits one atom per transaction and is resumable: a re-run
//! finds only mentions that still compete, and writes nothing for them.
//! `--decisions` writes one JSON line per decided mention (ids, source type
//! and outcome; no text) for projecting the quality metrics.
//!
//! Swept mentions carry `"swept": "mention_sweep_v1"`; reefs it minted
//! carry `body.born.worker = "mention_sweep"`.
//!
//! ## Env
//!
//!   MIGRATOR_DATABASE_URL  control-plane DSN for --control (falls back to DATABASE_URL).
//!
//! The DSN must bypass RLS on the tables touched (the tenant owner DSN, or
//! the migrator DSN). The tool refuses otherwise.
//!
//! ## Exit codes
//!
//!   0  every graph processed
//!   1  a database, graph or atom failed
//!   2  usage error
//!   3  the DSN does not bypass RLS

use std::io::Write as _;
use std::process::ExitCode;
use std::time::{Duration, Instant};

use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

use coralswarm_backend::mention_sweep::{self, Write};
use coralswarm_backend::migrator_database_url;
use coralswarm_backend::node_atoms_bridge;

const USAGE: &str = "usage: resolve_ambiguous_mentions [--execute] [--graph-id <uuid>] [--max-atoms <N>] \
[--decisions <path>] (--tenant-dsn <dsn> | --control)";

const TABLES: &[&str] = &[
    "graphs",
    "nodes",
    "semantic_atoms",
    "node_semantic_atoms",
    "node_atoms",
    "source_versions",
    "source_items",
    "source_version_chunks",
    "reef_identifiers",
    "reef_judgement",
    "op_log",
];

#[derive(Debug, PartialEq, Eq)]
enum Selector {
    TenantDsn(String),
    Control,
}

#[derive(Debug)]
struct Args {
    write: Write,
    graph_id: Option<Uuid>,
    max_atoms: Option<usize>,
    decisions: Option<String>,
    selector: Selector,
}

fn parse_args(mut args: impl Iterator<Item = String>) -> Result<Args, String> {
    let mut write = Write::DryRun;
    let (mut graph_id, mut max_atoms, mut decisions, mut selector) = (None, None, None, None);
    let mut set = |s: Selector| -> Result<(), String> {
        if selector.replace(s).is_some() {
            return Err("--tenant-dsn / --control are mutually exclusive".into());
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
            "--max-atoms" => {
                let v = args.next().ok_or("--max-atoms requires a value")?;
                max_atoms = Some(v.parse::<usize>().map_err(|e| format!("--max-atoms: {e}"))?);
            }
            "--decisions" => decisions = Some(args.next().ok_or("--decisions requires a path")?),
            "--tenant-dsn" => set(Selector::TenantDsn(args.next().ok_or("--tenant-dsn requires a value")?))?,
            "--control" => set(Selector::Control)?,
            "-h" | "--help" => {
                println!("{USAGE}");
                std::process::exit(0);
            }
            other => return Err(format!("unknown argument: {other}")),
        }
    }
    let selector = selector.ok_or("one of --tenant-dsn / --control is required")?;
    Ok(Args { write, graph_id, max_atoms, decisions, selector })
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
    println!("resolve_ambiguous_mentions rule={} run={run}", mention_sweep::RULE);
    let started = Instant::now();
    let out = match mention_sweep::backfill(&pool, args.graph_id, args.write, args.max_atoms, args.decisions.is_some()).await {
        Ok(o) => o,
        Err(e) => {
            eprintln!("FAILED: {e}");
            return ExitCode::FAILURE;
        }
    };
    pool.close().await;
    if let Some(path) = &args.decisions {
        let written = std::fs::File::create(path).and_then(|mut f| {
            for d in &out.decisions {
                writeln!(f, "{}", serde_json::to_string(d).expect("decision serializes"))?;
            }
            Ok(())
        });
        if let Err(e) = written {
            eprintln!("--decisions {path}: {e}");
            return ExitCode::FAILURE;
        }
    }
    for (id, error) in &out.failed {
        eprintln!("  {id} failed: {error}");
    }
    let t = &out.total;
    let verb = if args.write == Write::Apply { "written" } else { "would write (dry run: nothing written)" };
    eprintln!(
        "summary: graphs={} atoms={} mentions={} {verb}: atoms_changed={} linked_ladder={} linked_exact_title={} minted={} \
         kept_competing={} kept_same_title_distinct={} kept_generic_title={} kept_rejected={} failed={} elapsed={:.1}s",
        out.graphs, t.atoms, t.mentions, t.atoms_changed, t.linked_ladder, t.linked_exact_title, t.minted,
        t.kept_competing, t.kept_same_title_distinct, t.kept_generic_title, t.kept_rejected, out.failed.len(),
        started.elapsed().as_secs_f64()
    );
    if out.failed.is_empty() { ExitCode::SUCCESS } else { ExitCode::FAILURE }
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
        assert_eq!(parse(&["--control", "--max-atoms", "5"]).unwrap().max_atoms, Some(5));
        assert!(parse(&["--control", "--graph-id", "nope"]).is_err());
    }
}
