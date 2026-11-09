//! `fold_subtopics` — G4 stock repair: merge unique-parent sub-topic reefs
//! into their entity-pass parent via `Store::merge_nodes`.
//!
//! **This binary is a thin wrapper.** Planning and writes live in
//! [`coralswarm_backend::fold_subtopics`]. Default is dry-run.
//! `--execute` writes. `--graph-id` optional. `--limit N` caps merges.
//!
//! `--execute` refuses `coralswarm_prodclone` and `coralswarm_kg_spike`
//! outright. It writes without ceremony to known scratch copies
//! (`coralswarm_kg_guard`, `coralswarm_prodclone_YYYYMMDD`). Any other
//! database name — the live Neon name included — additionally requires
//! `--i-am-sure`, and the target name is printed before the first merge so
//! a second person can read it back (plan §5.3 / §13.5).
//!
//! ## Usage
//!
//!   fold_subtopics [--execute [--i-am-sure]] [--graph-id <uuid>] [--limit N]
//!
//! ## Exit codes
//!
//!   0  dry-run or execute completed
//!   1  query/write error
//!   2  usage/argument error, or no DSN
//!   3  RLS preflight refused, or --execute refused for this database name

use std::process::ExitCode;

use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

use coralswarm_backend::fold_subtopics::{
    assert_execute_db, assert_policy_bypass, display_title, execute_plan, preview, ExecuteTarget,
};
use coralswarm_backend::migrator_database_url;

const USAGE: &str =
    "usage: fold_subtopics [--execute [--i-am-sure]] [--graph-id <uuid>] [--limit N]";

#[derive(Debug)]
struct Args {
    execute: bool,
    i_am_sure: bool,
    graph_id: Option<Uuid>,
    limit: Option<u64>,
}

fn parse_args(args: impl Iterator<Item = String>) -> Result<Args, String> {
    let mut execute = false;
    let mut i_am_sure = false;
    let mut graph_id = None;
    let mut limit = None;
    let mut args = args;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--execute" => execute = true,
            "--dry-run" => execute = false,
            "--i-am-sure" => i_am_sure = true,
            "--graph-id" => {
                let v = args.next().ok_or("--graph-id requires a value")?;
                graph_id = Some(
                    Uuid::parse_str(&v).map_err(|e| format!("--graph-id: invalid UUID: {e}"))?,
                );
            }
            "--limit" => {
                let v = args.next().ok_or("--limit requires a value")?;
                limit = Some(
                    v.parse::<u64>()
                        .map_err(|e| format!("--limit: invalid integer: {e}"))?,
                );
            }
            "-h" | "--help" => {
                println!("{USAGE}");
                println!();
                println!("Default is dry-run. --execute merges unique-parent sub-topic reefs.");
                println!("--execute refuses coralswarm_prodclone and coralswarm_kg_spike.");
                println!(
                    "--execute on any database other than coralswarm_kg_guard or a dated \
                     coralswarm_prodclone_YYYYMMDD copy also requires --i-am-sure."
                );
                std::process::exit(0);
            }
            other => return Err(format!("unknown argument: {other}")),
        }
    }
    if i_am_sure && !execute {
        return Err("--i-am-sure only makes sense together with --execute".into());
    }
    Ok(Args {
        execute,
        i_am_sure,
        graph_id,
        limit,
    })
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
    if let Err(msg) = assert_policy_bypass(&pool).await {
        eprintln!("{msg}");
        return ExitCode::from(3);
    }

    println!(
        "fold_subtopics — mode={}",
        if args.execute { "EXECUTE" } else { "DRY RUN" }
    );
    if let Some(id) = args.graph_id {
        println!("  narrowed to graph {id}");
    }
    if let Some(n) = args.limit {
        println!("  limit {n}");
    }

    let plan = match preview(&pool, args.graph_id).await {
        Ok(p) => p,
        Err(e) => {
            eprintln!("preview failed: {e}");
            return ExitCode::FAILURE;
        }
    };
    println!(
        "  children: {}  unique folds: {}  ambiguous: {}  none: {}",
        plan.child_count,
        plan.pairs.len(),
        plan.ambiguous,
        plan.none
    );

    let sample_n = 15.min(plan.pairs.len());
    if sample_n > 0 {
        println!("  sample folds:");
        for p in plan.pairs.iter().take(sample_n) {
            println!(
                "    {}  →  {}",
                display_title(&p.child_title),
                display_title(&p.parent_title)
            );
        }
    }

    if !args.execute {
        println!("dry-run: no reefs merged. pass --execute to merge unique-parent pairs.");
        return ExitCode::SUCCESS;
    }

    match assert_execute_db(&pool, args.i_am_sure).await {
        Ok(ExecuteTarget::Scratch(name)) => println!("  target database: {name} (scratch)"),
        Ok(ExecuteTarget::Unknown(name)) => {
            println!("  target database: {name} (NOT a known scratch copy — --i-am-sure given)")
        }
        Err(msg) => {
            eprintln!("{msg}");
            return ExitCode::from(3);
        }
    }

    // Merge the plan that was just printed, not a fresh scan.
    match execute_plan(&pool, plan, args.limit).await {
        Ok(r) => {
            println!(
                "merged {} reefs (moved {} coral links; {} already gone)",
                r.merged, r.moved_atoms, r.skipped_missing
            );
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("{e}");
            ExitCode::FAILURE
        }
    }
}
