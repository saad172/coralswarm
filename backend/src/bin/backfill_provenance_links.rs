//! `backfill_provenance_links` — G1 stock repair: insert missing `node_atoms`
//! rows implied by relationship provenance and `body.born.atom_id`.
//!
//! **This binary is a thin wrapper.** Counts and writes live in
//! [`coralswarm_backend::provenance_link_backfill`]. Default is dry-run.
//! `--execute` writes. `--graph-id` optional.
//!
//! The coral_count trigger stays on. Point `MIGRATOR_DATABASE_URL` (or
//! `DATABASE_URL`) at a role that bypasses RLS. `--execute` refuses
//! `coralswarm_prodclone` and `coralswarm_kg_spike`, and prints
//! `current_database()` before writing so a second person can confirm the
//! target for the live Neon run (plan §5.2).
//!
//! ## Usage
//!
//!   backfill_provenance_links [--execute] [--graph-id <uuid>]
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

use coralswarm_backend::migrator_database_url;
use coralswarm_backend::provenance_link_backfill::{
    assert_execute_db, assert_policy_bypass, execute, preview,
};

const USAGE: &str = "usage: backfill_provenance_links [--execute] [--graph-id <uuid>]";

#[derive(Debug)]
struct Args {
    execute: bool,
    graph_id: Option<Uuid>,
}

fn parse_args(args: impl Iterator<Item = String>) -> Result<Args, String> {
    let mut execute = false;
    let mut graph_id = None;
    let mut args = args;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--execute" => execute = true,
            "--dry-run" => execute = false,
            "--graph-id" => {
                let v = args.next().ok_or("--graph-id requires a value")?;
                graph_id = Some(
                    Uuid::parse_str(&v).map_err(|e| format!("--graph-id: invalid UUID: {e}"))?,
                );
            }
            "-h" | "--help" => {
                println!("{USAGE}");
                println!();
                println!("Default is dry-run. --execute inserts missing node_atoms rows.");
                println!("The coral_count trigger stays on. Operator must bypass RLS.");
                println!("--execute refuses coralswarm_prodclone and coralswarm_kg_spike.");
                std::process::exit(0);
            }
            other => return Err(format!("unknown argument: {other}")),
        }
    }
    Ok(Args { execute, graph_id })
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
        "backfill_provenance_links — mode={}",
        if args.execute { "EXECUTE" } else { "DRY RUN" }
    );
    if let Some(id) = args.graph_id {
        println!("  narrowed to graph {id}");
    }

    let preview = match preview(&pool, args.graph_id).await {
        Ok(p) => p,
        Err(e) => {
            eprintln!("preview failed: {e}");
            return ExitCode::FAILURE;
        }
    };
    println!(
        "  missing links: {}  distinct reefs: {}  distinct corals: {}  atom gone: {}",
        preview.missing_links,
        preview.distinct_nodes,
        preview.distinct_atoms,
        preview.implied_atom_gone
    );

    if !args.execute {
        println!("dry-run: no rows written. pass --execute to insert.");
        return ExitCode::SUCCESS;
    }

    match assert_execute_db(&pool).await {
        Ok(name) => println!("  target database: {name}"),
        Err(msg) => {
            eprintln!("{msg}");
            return ExitCode::from(3);
        }
    }

    match execute(&pool, args.graph_id).await {
        Ok(r) => {
            println!(
                "inserted {} node_atoms rows (ON CONFLICT DO NOTHING)",
                r.inserted
            );
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("execute failed: {e}");
            ExitCode::FAILURE
        }
    }
}
