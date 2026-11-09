//! `pipeline_stage_health` — print the per-stage, per-source-type pipeline
//! table for one content plane, live and read-only.
//!
//! The quick check operators were hand-writing in SQL: captures, embeddings,
//! extraction, chunk rows, reef links, minted and duplicate reefs, titles,
//! edges, ER, journal, Jev and dead letters, per source type, for each
//! window, followed by the findings the alert rules would page on. See
//! `coralswarm_backend::pipeline_stage_health`.
//!
//! Every read runs in ONE transaction made `READ ONLY` with a transaction-
//! local statement timeout and `search_path` (`SET LOCAL` semantics only, so
//! a Neon pooler DSN is safe). Nothing is written. The DSN is read from the
//! env var named by `--dsn-env` and never printed.
//!
//! ## Usage
//!
//!   PIPELINE_HEALTH_DSN=postgres://… pipeline_stage_health \
//!     [--plane org|consumer] [--hours 24,168] [--json]
//!
//! `--plane org` (default) reads an org tenant database as the tenant system
//! actor; `--plane consumer` reads the control database one account at a
//! time. Thresholds come from the same `STAGE_HEALTH_*` env as the worker.
//! Control-plane facts (accounts that switched processing off, credit holds)
//! and the serving process's switches are not visible from here.
//!
//! ## Exit codes
//!
//!   0  printed (findings do not change the exit code)
//!   1  query error
//!   2  usage error, or the DSN env var is unset

use std::process::ExitCode;

use chrono::Utc;
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

use coralswarm_backend::pipeline_stage_health::{
    evaluate, harden, measure_dead_jobs, measure_scope, render_table, PlaneAccumulator, Thresholds,
};
use coralswarm_backend::tenancy::scope::{begin_account_scoped, begin_tenant_system_scoped_without_account};

const USAGE: &str =
    "usage: pipeline_stage_health [--dsn-env VAR] [--plane org|consumer] [--hours 24,168] [--json]";

struct Args {
    dsn_env: String,
    consumer: bool,
    hours: Vec<i64>,
    json: bool,
}

fn parse_args(mut args: impl Iterator<Item = String>) -> Result<Args, String> {
    let mut out = Args { dsn_env: "PIPELINE_HEALTH_DSN".into(), consumer: false, hours: vec![24, 168], json: false };
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--dsn-env" => out.dsn_env = args.next().ok_or("--dsn-env requires a value")?,
            "--plane" => match args.next().as_deref() {
                Some("org") => out.consumer = false,
                Some("consumer") => out.consumer = true,
                _ => return Err("--plane must be org or consumer".into()),
            },
            "--hours" => {
                let v = args.next().ok_or("--hours requires a value")?;
                out.hours = v
                    .split(',')
                    .map(|h| h.trim().parse::<i64>().map_err(|e| format!("--hours: {e}")))
                    .collect::<Result<_, _>>()?;
                if out.hours.iter().any(|h| *h <= 0) {
                    return Err("--hours: every window must be positive".into());
                }
            }
            "--json" => out.json = true,
            "-h" | "--help" => return Err(USAGE.into()),
            other => return Err(format!("unknown argument {other}\n{USAGE}")),
        }
    }
    Ok(out)
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    let args = match parse_args(std::env::args().skip(1)) {
        Ok(a) => a,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::from(2);
        }
    };
    let Ok(dsn) = std::env::var(&args.dsn_env) else {
        eprintln!("{} is not set\n{USAGE}", args.dsn_env);
        return ExitCode::from(2);
    };
    match run(&dsn, &args).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            // sqlx errors never echo the connection string.
            eprintln!("pipeline_stage_health: {e}");
            ExitCode::from(1)
        }
    }
}

async fn run(dsn: &str, args: &Args) -> Result<(), String> {
    let e = |e: sqlx::Error| e.to_string();
    let pool = PgPoolOptions::new().max_connections(1).connect(dsn).await.map_err(e)?;
    let th = Thresholds::from_env();
    let mut windows = th.windows();
    windows.extend(args.hours.iter().copied());
    windows.sort_unstable();
    windows.dedup();
    let now = Utc::now();
    let mut acc = PlaneAccumulator::default();
    let (tenant, dead) = if args.consumer {
        let accounts: Vec<Uuid> = {
            let mut tx = pool.begin().await.map_err(e)?;
            harden(&mut tx).await.map_err(e)?;
            let a = sqlx::query_scalar("SELECT id FROM accounts ORDER BY id").fetch_all(&mut *tx).await.map_err(e)?;
            tx.rollback().await.map_err(e)?;
            a
        };
        for account in accounts {
            let mut tx = begin_account_scoped(&pool, account).await.map_err(|e| e.to_string())?;
            harden(&mut tx).await.map_err(e)?;
            let graph_ids: Vec<Uuid> = sqlx::query_scalar("SELECT id FROM graphs WHERE account_id = $1 ORDER BY id")
                .bind(account)
                .fetch_all(&mut *tx)
                .await
                .map_err(e)?;
            if !graph_ids.is_empty() {
                acc.add(measure_scope(&mut tx, &graph_ids, &[account], now, &windows, &th).await.map_err(e)?);
            }
            tx.rollback().await.map_err(e)?;
        }
        let mut tx = pool.begin().await.map_err(e)?;
        harden(&mut tx).await.map_err(e)?;
        let dead = measure_dead_jobs(&mut tx, now, &windows).await.map_err(e)?;
        tx.rollback().await.map_err(e)?;
        ("__consumer__".to_string(), dead)
    } else {
        let mut tx = begin_tenant_system_scoped_without_account(&pool).await.map_err(|e| e.to_string())?;
        harden(&mut tx).await.map_err(e)?;
        let org: Option<String> = sqlx::query_scalar("SELECT org_id FROM tenant_meta LIMIT 1")
            .fetch_optional(&mut *tx)
            .await
            .map_err(e)?;
        let rows: Vec<(Uuid, Uuid)> =
            sqlx::query_as("SELECT id, account_id FROM graphs ORDER BY id").fetch_all(&mut *tx).await.map_err(e)?;
        let graph_ids: Vec<Uuid> = rows.iter().map(|(g, _)| *g).collect();
        let mut accounts: Vec<Uuid> = rows.iter().map(|(_, a)| *a).collect();
        accounts.sort_unstable();
        accounts.dedup();
        if !graph_ids.is_empty() {
            acc.add(measure_scope(&mut tx, &graph_ids, &accounts, now, &windows, &th).await.map_err(e)?);
        }
        let dead = measure_dead_jobs(&mut tx, now, &windows).await.map_err(e)?;
        tx.rollback().await.map_err(e)?;
        (org.unwrap_or_else(|| "(org)".into()), dead)
    };
    pool.close().await;
    let report = acc.finish(tenant, now, &th, &windows, dead, 0, None);
    let findings = evaluate(&report, &th);
    if args.json {
        let out = serde_json::json!({"report": report, "findings": findings, "thresholds": th});
        println!("{}", serde_json::to_string_pretty(&out).map_err(|e| e.to_string())?);
        return Ok(());
    }
    for (i, hours) in args.hours.iter().enumerate() {
        // Findings print once, under the last table.
        let shown: &[_] = if i + 1 == args.hours.len() { &findings } else { &[] };
        println!("{}", render_table(&report, *hours, shown));
    }
    Ok(())
}
