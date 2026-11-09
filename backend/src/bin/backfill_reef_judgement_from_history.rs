//! `backfill_reef_judgement_from_history` — the one-shot that reconciles
//! PRE-FLAG merge/dismissal history into `reef_judgement` (judgement-graph
//! resolver slice, PR5; issue #585's PR list).
//!
//! **This binary is a thin wrapper.** Every gate, counter and write lives in
//! [`coralswarm_backend::judgement_backfill`] — READ THAT MODULE'S DOC
//! first: it is where the gate order, the evidence shapes, the idempotency
//! keying, the expired-UNSURE supersession decision, and the reason the
//! POSITIVE half is expected to write nothing are all written down. This
//! file only parses arguments, resolves DSNs, loops the selected databases,
//! and reports. That split is deliberate: it is what lets
//! `backend/tests/judgement_backfill_pg.rs` drive the SAME code an operator
//! runs instead of a re-implementation of it.
//!
//! ## RUN ONCE PER DATABASE
//!
//! History lives per database: the control plane holds consumer-plane
//! oceans, and each org's `node_merges` / `nodes.body->'not_same'` rows live
//! only in that org's own tenant DB. A control-plane-only run therefore
//! reports a falsely-drained queue (see the `one-shot-backfills-run-per-
//! tenant` note). The DEFAULT selection is control plane + every ACTIVE
//! tenant, precisely so the common invocation cannot silently miss a plane.
//!
//! Re-running is SAFE. Gate 4 (no live judgement for the pair) is also this
//! backfill's idempotency key, so a second pass writes nothing and reports
//! its own previous rows as `already_backfilled`; a pass interrupted between
//! a POSITIVE write and its `node_merges` back-link repairs that link on the
//! next run.
//!
//! ## Usage
//!
//!   backfill_reef_judgement_from_history [--execute] [--dry-run]
//!       [--control-only | --only <slug|clerk_org_id>] [--only-graph <uuid>]
//!
//! `--dry-run` IS THE DEFAULT — the inverse of most tools here, and on
//! purpose: this writes durable verdicts into a table the ER worker acts on,
//! so the safe outcome of a mistyped invocation must be "printed a report".
//! `--dry-run` is still accepted explicitly (it makes an operator's intent
//! legible in shell history); passing it together with `--execute` is a
//! usage error rather than a silent precedence rule.
//!
//! A dry run performs EVERY gate against the real database and writes
//! nothing at all — no `reef_judgement` row, no `node_merges.judgement_id`.
//! Its counters predict an `--execute` run exactly (including same-run
//! duplicate candidates, which are simulated at gate 4); that fidelity is
//! the whole point, since the founder authorises the real run by reading
//! this output.
//!
//! `--only-graph` narrows to ONE ocean — a rehearsal/diagnosis aid, never a
//! complete run.
//!
//! ## Env
//!
//!   MIGRATOR_DATABASE_URL  elevated (owner-role) control-plane DSN.
//!                          Preferred.
//!   DATABASE_URL           fallback when MIGRATOR_DATABASE_URL is
//!                          unset/blank — the pre-OPS-4 config, where
//!                          DATABASE_URL still IS the owner role. Same
//!                          precedence as `migrate_control`/
//!                          `migrate_tenants` (`migrator_database_url()`).
//!                          The elevated role is REQUIRED, not merely
//!                          preferred: every write here is a PLAIN
//!                          (unscoped) `Store` call, sanctioned only against
//!                          a role that bypasses the consumer-plane
//!                          `account_isolation` policy. That is not left to
//!                          trust — `judgement_backfill::
//!                          assert_policy_bypass` proves it against the
//!                          catalog per database and REFUSES otherwise.
//!   TENANT_DB_<SLUG>       each tenant's own owner-role DSN, resolved from
//!                          the routing row's `db_secret_ref` via
//!                          `OperatorDsnResolver` — same convention as
//!                          `migrate_tenants`/`export_member_ocean`.
//!
//! ## Exit codes
//!
//!   0  every selected database was processed with zero per-candidate errors
//!   1  at least one database could not be processed (connection, DSN
//!      secret, preflight refusal, fingerprint mismatch), OR at least one
//!      per-candidate error occurred. The run does NOT stop at the first
//!      problem — a one-shot must not be stranded by one bad pair or one
//!      unreachable tenant — so a non-zero exit means "read the report, fix,
//!      and re-run"; re-running is idempotent.
//!   2  usage/argument error, or no control-plane DSN in the environment, or
//!      `--only <x>` named a tenant with no `tenant_databases` routing row.
//!      All caught before any work begins.
//!
//! Every run prints one machine-greppable `summary:` line per database to
//! stderr, plus one for the grand total — the `migrate_tenants` convention.

use std::process::ExitCode;
use std::time::Duration;

use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use uuid::Uuid;

use coralswarm_backend::judgement_backfill::{
    backfill_database, report_block, summary_line, BackfillCounts, BackfillOpts,
};
use coralswarm_backend::migrator_database_url;
use coralswarm_backend::tenancy::routing::{self, TenantDatabase};
use coralswarm_backend::tenancy::secrets::OperatorDsnResolver;

const USAGE: &str = "usage: backfill_reef_judgement_from_history [--execute] [--dry-run] \
[--control-only | --only <slug|clerk_org_id>] [--only-graph <uuid>]";

#[derive(Debug)]
struct Args {
    opts: BackfillOpts,
    control_only: bool,
    only: Option<String>,
}

fn parse_args() -> Result<Args, String> {
    let mut execute = false;
    let mut explicit_dry_run = false;
    let mut control_only = false;
    let mut only: Option<String> = None;
    let mut only_graph: Option<Uuid> = None;

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--execute" => execute = true,
            "--dry-run" => explicit_dry_run = true,
            "--control-only" => control_only = true,
            "--only" => only = Some(args.next().ok_or("--only requires a value")?),
            "--only-graph" => {
                let v = args.next().ok_or("--only-graph requires a value")?;
                only_graph =
                    Some(Uuid::parse_str(&v).map_err(|_| format!("--only-graph: not a UUID: {v}"))?);
            }
            "-h" | "--help" => {
                println!("{USAGE}");
                std::process::exit(0);
            }
            other => return Err(format!("unknown argument: {other}")),
        }
    }

    // Refused rather than resolved by precedence: an operator who typed both
    // does not have one intent this tool should guess at.
    if execute && explicit_dry_run {
        return Err("--execute and --dry-run are mutually exclusive".to_string());
    }
    if control_only && only.is_some() {
        return Err("--control-only and --only are mutually exclusive".to_string());
    }

    Ok(Args {
        opts: BackfillOpts { execute, only_graph },
        control_only,
        only,
    })
}

/// Open a pool for one database. `max_connections(2)` mirrors
/// `tenancy::migrate::process_one`/`export_member_ocean`: a one-shot needs
/// no concurrency, and a tenant DB must never have its connection budget
/// eaten by a maintenance tool.
async fn connect(dsn: &str) -> Result<PgPool, String> {
    PgPoolOptions::new()
        .max_connections(2)
        .acquire_timeout(Duration::from_secs(15))
        .connect(dsn)
        .await
        .map_err(|e| format!("connect: {e}"))
}

/// Resolve a tenant's DSN and verify the database it lands on is the one the
/// routing row names.
///
/// The fingerprint check is the same DSN-swap guard `export_member_ocean`
/// and `orgt_tenant_set_fingerprint` apply, and it matters MORE for a writer
/// than for an exporter: a swapped/stale `TENANT_DB_<SLUG>` would write one
/// org's legacy verdicts into another org's judgement graph, where they
/// would then drive that org's ER worker. A missing fingerprint fails closed
/// for the same reason it does there — an unfingerprinted DSN is no proof at
/// all that this is the right database.
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
    match fingerprint {
        Some(org) if org == t.clerk_org_id => Ok(pool),
        Some(org) => {
            pool.close().await;
            Err(format!(
                "tenant_meta fingerprint mismatch: routing row says {}, tenant DB says {org} — \
                 refusing (DSN-swap guard)",
                t.clerk_org_id
            ))
        }
        None => {
            pool.close().await;
            Err(format!(
                "tenant_meta has no fingerprint row for org {} — refusing (DSN-swap guard)",
                t.clerk_org_id
            ))
        }
    }
}

/// Run one database and report it. Returns `false` if this database could
/// not be completed (the caller keeps going and exits non-zero at the end).
async fn run_one(label: &str, pool: &PgPool, opts: &BackfillOpts, total: &mut BackfillCounts) -> bool {
    match backfill_database(pool, opts).await {
        Ok(counts) => {
            print!("{}", report_block(label, opts, &counts));
            eprintln!("{}", summary_line(label, opts, &counts));
            let clean = counts.errors == 0;
            total.add(&counts);
            clean
        }
        Err(e) => {
            eprintln!("FAILED [{label}]: {e}");
            false
        }
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    // A tracing subscriber IS wanted here (unlike `export_member_ocean`):
    // `judgement_backfill` emits real `tracing::warn!` events for per-
    // candidate anomalies (a write that failed, a back-link that matched no
    // row), and those are exactly the lines an operator needs beside the
    // tally when `errors` is non-zero.
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let args = match parse_args() {
        Ok(a) => a,
        Err(msg) => {
            eprintln!("{msg}");
            eprintln!("{USAGE}");
            return ExitCode::from(2);
        }
    };

    let control_url = match migrator_database_url() {
        Some(u) => u,
        None => {
            eprintln!(
                "MIGRATOR_DATABASE_URL (preferred) or DATABASE_URL must be set to an ELEVATED \
                 control-plane DSN — every write here is a plain, unscoped Store call and is \
                 refused outright under the least-privilege coralswarm_app role"
            );
            return ExitCode::from(2);
        }
    };

    println!(
        "backfill_reef_judgement_from_history — ONE-SHOT, RUN ONCE PER DATABASE (mode={})",
        args.opts.mode()
    );
    if let Some(g) = args.opts.only_graph {
        println!("  narrowed to ocean {g} — this is a rehearsal, NOT a complete run");
    }

    let control = match connect(&control_url).await {
        Ok(p) => p,
        Err(e) => {
            eprintln!("failed to connect control-plane database: {e}");
            return ExitCode::FAILURE;
        }
    };

    let mut total = BackfillCounts::default();
    let mut all_clean = true;

    // Selection. `--only <tenant>` deliberately EXCLUDES the control plane:
    // it names one tenant, and quietly folding the control plane into that
    // would make "I re-ran just testorg" untrue in the record.
    let tenants: Vec<TenantDatabase> = if let Some(name) = &args.only {
        let row = match routing::lookup_by_slug(&control, name).await {
            Ok(Some(r)) => Some(r),
            Ok(None) => match routing::lookup_by_org(&control, name).await {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("routing lookup failed: {e}");
                    return ExitCode::FAILURE;
                }
            },
            Err(e) => {
                eprintln!("routing lookup failed: {e}");
                return ExitCode::FAILURE;
            }
        };
        match row {
            Some(r) => vec![r],
            None => {
                // Loud, not a zero-tenant success: "--only typo" and "this
                // tenant has nothing to backfill" must never look alike.
                eprintln!("no tenant_databases routing row for '{name}'");
                return ExitCode::from(2);
            }
        }
    } else {
        if !run_one("control", &control, &args.opts, &mut total).await {
            all_clean = false;
        }
        if args.control_only {
            Vec::new()
        } else {
            match routing::list_active(&control).await {
                Ok(rows) => rows,
                Err(e) => {
                    eprintln!("failed to enumerate active tenants: {e}");
                    return ExitCode::FAILURE;
                }
            }
        }
    };

    let resolver = OperatorDsnResolver::from_env_or_env_only(control.clone(), "backfill_reef_judgement_from_history");
    for t in &tenants {
        let label = format!("tenant:{}", t.slug);
        match connect_tenant(&resolver, t).await {
            Ok(pool) => {
                if !run_one(&label, &pool, &args.opts, &mut total).await {
                    all_clean = false;
                }
                pool.close().await;
            }
            Err(e) => {
                eprintln!("FAILED [{label}]: {e}");
                all_clean = false;
            }
        }
    }
    control.close().await;

    print!("{}", report_block("TOTAL", &args.opts, &total));
    eprintln!("{}", summary_line("TOTAL", &args.opts, &total));
    if !args.opts.execute {
        println!(
            "Nothing was written. Read the tallies above, then re-run with --execute to apply."
        );
    }

    if all_clean {
        ExitCode::SUCCESS
    } else {
        eprintln!(
            "one or more databases failed or reported per-candidate errors — see above. Fixing \
             and re-running is safe: this backfill is idempotent."
        );
        ExitCode::FAILURE
    }
}
