use std::process::ExitCode;

use sqlx::postgres::PgConnection;
use sqlx::Connection;

use coralswarm_backend::workers::jobs_retention::{self, Limits, Policy, SweepReport, HANG_GRACE};

const USAGE: &str = "usage: jobs_retention (--dry-run | --execute) --database-url <postgres-url>";

struct Args {
    execute: bool,
    database_url: String,
}

fn parse_args() -> Result<Args, String> {
    let mut dry_run = false;
    let mut execute = false;
    let mut database_url = None;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--dry-run" => dry_run = true,
            "--execute" => execute = true,
            "--database-url" => database_url = Some(args.next().ok_or("--database-url requires a value")?),
            "-h" | "--help" => {
                println!("{USAGE}");
                std::process::exit(0);
            }
            other => return Err(format!("unknown argument: {other}")),
        }
    }
    if dry_run == execute {
        return Err("pass exactly one of --dry-run and --execute".to_string());
    }
    let database_url = database_url.ok_or("--database-url is required")?;
    Ok(Args { execute, database_url })
}

async fn describe(conn: &mut PgConnection) -> Result<String, sqlx::Error> {
    let (database, tenant): (String, bool) =
        sqlx::query_as("SELECT current_database()::text, to_regclass('public.tenant_meta') IS NOT NULL")
            .fetch_one(&mut *conn)
            .await?;
    if !tenant {
        return Ok(format!("database={database} plane=control"));
    }
    let org: Option<String> = sqlx::query_scalar("SELECT org_id FROM tenant_meta WHERE id")
        .fetch_optional(&mut *conn)
        .await?;
    Ok(format!("database={database} plane=tenant org={}", org.as_deref().unwrap_or("unset")))
}

fn print_counts(header: &str, counts: &std::collections::BTreeMap<jobs_retention::Target, i64>) {
    println!("kind\tstatus\t{header}");
    for ((kind, status), n) in counts {
        println!("{kind}\t{}\t{n}", status.as_str());
    }
    println!("total {header} {}", counts.values().sum::<i64>());
}

fn print_sweep(report: &SweepReport) {
    println!("kind\tstatus\tdeleted");
    for ((kind, status), n) in &report.deleted {
        println!("{kind}\t{}\t{n}", status.as_str());
    }
    println!(
        "total deleted {} in {} batches, slowest batch {} ms, budget exhausted {}",
        report.total_deleted(),
        report.batches,
        report.slowest_batch.as_millis(),
        report.budget_exhausted
    );
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn"));
    tracing_subscriber::fmt().with_env_filter(filter).with_writer(std::io::stderr).init();
    let args = match parse_args() {
        Ok(args) => args,
        Err(msg) => {
            eprintln!("{msg}\n{USAGE}");
            return ExitCode::from(2);
        }
    };
    let policy = match Policy::from_env() {
        Ok(policy) => policy,
        Err(msg) => {
            eprintln!("{msg}");
            return ExitCode::from(2);
        }
    };
    let mut conn = match PgConnection::connect(&args.database_url).await {
        Ok(conn) => conn,
        Err(e) => {
            eprintln!("connect failed: {e}");
            return ExitCode::FAILURE;
        }
    };
    let target = match describe(&mut conn).await {
        Ok(target) => target,
        Err(e) => {
            eprintln!("reading the database identity failed: {e}");
            return ExitCode::FAILURE;
        }
    };
    let mode = if args.execute { "execute" } else { "dry-run" };
    println!(
        "jobs_retention mode={mode} {target} done_days={} dead_days={}",
        policy.done_days, policy.dead_days
    );

    let mut failed = false;
    if args.execute {
        let report = jobs_retention::sweep_once(&mut conn, policy, Limits::TICK).await;
        print_sweep(&report);
        for ((kind, status), error) in &report.failed {
            eprintln!("batch failed for {kind} {}: {error}", status.as_str());
            failed = true;
        }
        if report.hung {
            eprintln!(
                "the database stopped answering mid-batch; gave up {} s after the budget without counting what is left",
                HANG_GRACE.as_secs()
            );
            return ExitCode::FAILURE;
        }
    }
    match jobs_retention::count_eligible(&mut conn, policy).await {
        Ok(counts) => print_counts(if args.execute { "left" } else { "eligible" }, &counts),
        Err(e) => {
            eprintln!("counting eligible rows failed: {e}");
            failed = true;
        }
    }
    conn.close().await.ok();
    if failed { ExitCode::FAILURE } else { ExitCode::SUCCESS }
}
