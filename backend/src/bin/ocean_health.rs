//! `ocean_health` — G3: compute 7-day ocean health and write HealthViolation
//! job rows when paging alarms fire.
//!
//! Default is dry-run (prints metrics + alarms, writes nothing). `--execute`
//! writes `HealthViolation` rows. `--graph-id` optional. `--week YYYY-MM-DD`
//! (a Monday) replays that ISO week instead of the rolling window — read-only
//! on any DSN, including `coralswarm_prodclone`.
//!
//! `--execute` refuses `coralswarm_prodclone` and `coralswarm_kg_spike`.
//!
//! ## Usage
//!
//!   ocean_health [--execute] [--graph-id <uuid>] [--week YYYY-MM-DD]
//!
//! ## Exit codes
//!
//!   0  dry-run or execute completed
//!   1  query/write error
//!   2  usage/argument error, or no DSN
//!   3  RLS preflight refused

use std::process::ExitCode;

use chrono::NaiveDate;
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

use coralswarm_backend::migrator_database_url;
use coralswarm_backend::ocean_health::{
    assert_execute_db, assert_policy_bypass, compute_on_pool, evaluate, monday_week,
    record_violations, rolling_window, to_json,
};

const USAGE: &str = "usage: ocean_health [--execute] [--graph-id <uuid>] [--week YYYY-MM-DD]";

#[derive(Debug)]
struct Args {
    execute: bool,
    graph_id: Option<Uuid>,
    week: Option<NaiveDate>,
}

fn parse_args(args: impl Iterator<Item = String>) -> Result<Args, String> {
    let mut execute = false;
    let mut graph_id = None;
    let mut week = None;
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
            "--week" => {
                let v = args.next().ok_or("--week requires a value")?;
                week = Some(
                    NaiveDate::parse_from_str(&v, "%Y-%m-%d")
                        .map_err(|e| format!("--week: expected YYYY-MM-DD: {e}"))?,
                );
            }
            "-h" | "--help" => {
                println!("{USAGE}");
                println!();
                println!("Default is dry-run. --execute writes HealthViolation job rows.");
                println!("--week YYYY-MM-DD replays that Monday week (read-only).");
                println!("--execute refuses coralswarm_prodclone and coralswarm_kg_spike.");
                std::process::exit(0);
            }
            other => return Err(format!("unknown argument: {other}")),
        }
    }
    Ok(Args {
        execute,
        graph_id,
        week,
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

    let (start, end) = match args.week {
        Some(d) => monday_week(d),
        None => rolling_window(chrono::Utc::now()),
    };

    println!(
        "ocean_health — mode={}  window=[{} .. {})",
        if args.execute { "EXECUTE" } else { "DRY RUN" },
        start,
        end
    );

    if args.execute {
        if let Err(msg) = assert_execute_db(&pool).await {
            eprintln!("{msg}");
            return ExitCode::FAILURE;
        }
    }

    let graph_ids: Vec<Uuid> = match args.graph_id {
        Some(id) => vec![id],
        None => match sqlx::query_scalar("SELECT id FROM graphs ORDER BY name")
            .fetch_all(&pool)
            .await
        {
            Ok(ids) => ids,
            Err(e) => {
                eprintln!("list graphs failed: {e}");
                return ExitCode::FAILURE;
            }
        },
    };

    let mut paging_oceans = 0u64;
    let mut wrote = 0u64;
    for graph_id in graph_ids {
        let metrics = match compute_on_pool(&pool, graph_id, start, end).await {
            Ok(m) => m,
            Err(e) => {
                eprintln!("compute {graph_id} failed: {e}");
                return ExitCode::FAILURE;
            }
        };
        let paging = evaluate(&metrics);
        if !paging.is_empty() {
            paging_oceans += 1;
        }
        println!("{}", to_json(&metrics, &paging));

        if args.execute {
            match record_violations(&pool, None, &metrics, &paging).await {
                Ok(n) => wrote += n,
                Err(e) => {
                    eprintln!("{e}");
                    return ExitCode::FAILURE;
                }
            }
        }
    }

    println!(
        "oceans with paging alarms: {paging_oceans}  HealthViolation rows written: {}",
        if args.execute { wrote } else { 0 }
    );
    if !args.execute {
        println!("dry-run: no jobs written. pass --execute to write HealthViolation rows.");
    }
    ExitCode::SUCCESS
}
