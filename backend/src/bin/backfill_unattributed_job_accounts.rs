//! `backfill_unattributed_job_accounts` — the one-shot that attributes
//! historical `ExtractEntities` `jobs` rows (`status='done'`, `account_id
//! IS NULL`) so `workers::billing_derivation`'s unattributed-jobs alert
//! stops firing on pure history.
//!
//! **This binary is a thin wrapper.** Every gate, counter and write lives in
//! [`coralswarm_backend::jobs_account_backfill`] — READ THAT MODULE'S DOC
//! first: it explains what produced these rows, the resolution path
//! (`payload->>'atom_id'` → `atoms.graph_id` → `graphs.account_id`, via the
//! SAME `resolve_atoms_account_id` SECURITY DEFINER function `jobs::queue::
//! dequeue` uses for `pending` rows), why every statement here is
//! deliberately unscoped, and why the 20-ish rows a real database cannot
//! resolve (their atom was since deleted) are expected to remain — that
//! residual count is closed by bounding the ALERT itself
//! (`workers::billing_derivation::UNATTRIBUTED_ALERT_WINDOW`), not by this
//! backfill. This file only parses arguments, resolves DSNs, loops the
//! selected databases, and reports — same split as
//! `backfill_reef_judgement_from_history.rs`, so
//! `backend/tests/jobs_account_backfill_pg.rs` drives the SAME code an
//! operator runs instead of a re-implementation of it.
//!
//! ## RUN ONCE PER DATABASE
//!
//! `jobs`/`atoms`/`graphs` are shared tenant-tree tables: the control plane
//! (which doubles as the consumer CONTENT plane) and EVERY org's own tenant
//! database each carry their own rows. A control-plane-only run therefore
//! reports a falsely-drained queue for every org tenant — the exact failure
//! this repo has been bitten by before (`one-shot-backfills-run-per-tenant`).
//! The DEFAULT selection below — no flags at all — is control plane + every
//! ACTIVE tenant, precisely so the common invocation cannot silently miss a
//! plane.
//!
//! Re-running is SAFE, any number of times, in any scope: the candidate
//! query's own `account_id IS NULL` predicate is this backfill's entire
//! idempotency mechanism (see the core module's doc, "Idempotency" section)
//! — a second pass finds nothing left to touch for every row the first pass
//! resolved.
//!
//! ## Usage
//!
//!   backfill_unattributed_job_accounts [--execute] [--dry-run]
//!       [--control-only | --only <slug|clerk_org_id>]
//!
//! `--dry-run` IS THE DEFAULT — this writes a durable `jobs.account_id`
//! stamp, so the safe outcome of a mistyped invocation is "printed a
//! report", not "wrote something". `--dry-run` is still accepted explicitly
//! for shell-history legibility; passing it together with `--execute` is a
//! usage error, not a silently-resolved precedence rule.
//!
//! ## Env — READ THIS BEFORE RUNNING
//!
//!   MIGRATOR_DATABASE_URL  control-plane DSN. Preferred for consistency
//!                          with the other one-shot tools in this
//!                          directory, but NOT strictly required the way it
//!                          is for `backfill_reef_judgement_from_history`:
//!                          `jobs` carries no RLS on either plane (migration
//!                          `0083_consumer_rls`'s own header: "flagged, not
//!                          covered"), and `resolve_atoms_account_id` is
//!                          granted to `coralswarm_app` — a plain
//!                          least-privilege `DATABASE_URL` connection can
//!                          run this backfill correctly end to end. See the
//!                          core module's doc, "Why every write here is a
//!                          single unscoped statement".
//!   DATABASE_URL           fallback when `MIGRATOR_DATABASE_URL` is unset —
//!                          same precedence as every other tool in this
//!                          directory (`migrator_database_url()`).
//!   TENANT_DB_<SLUG>       **THIS IS THE PART THAT IS EASY TO MISS.** Each
//!                          tenant's own DSN, resolved from the routing
//!                          row's `db_secret_ref` via `OperatorDsnResolver`
//!                          (same convention as `migrate_tenants`/
//!                          `backfill_reef_judgement_from_history`). This
//!                          binary reads it automatically for every ACTIVE
//!                          tenant unless narrowed with `--control-only` /
//!                          `--only` — a run against the control plane
//!                          alone, with no tenant DSNs available in the
//!                          environment, WILL under-report: it does not skip
//!                          tenants silently (each unreachable tenant fails
//!                          loudly, see the exit-code table below), but an
//!                          operator who only ever runs this against a local
//!                          shell with `DATABASE_URL` set and no
//!                          `TENANT_DB_*` exports has NOT actually cleared
//!                          any org tenant's residue, no matter how clean
//!                          the control-plane report looks.
//!
//! ## Exit codes
//!
//!   0  every selected database was processed with zero per-candidate errors
//!   1  at least one database could not be processed (connection, DSN
//!      secret, fingerprint mismatch), OR at least one per-candidate error
//!      occurred. The run does NOT stop at the first problem — re-running is
//!      idempotent, so "read the report, fix, re-run" is always the right
//!      response.
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

use coralswarm_backend::jobs_account_backfill::{
    backfill_database, report_block, summary_line, BackfillCounts, BackfillOpts,
};
use coralswarm_backend::migrator_database_url;
use coralswarm_backend::tenancy::routing::{self, TenantDatabase};
use coralswarm_backend::tenancy::secrets::OperatorDsnResolver;

const USAGE: &str =
    "usage: backfill_unattributed_job_accounts [--execute] [--dry-run] \
[--control-only | --only <slug|clerk_org_id>]";

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

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--execute" => execute = true,
            "--dry-run" => explicit_dry_run = true,
            "--control-only" => control_only = true,
            "--only" => only = Some(args.next().ok_or("--only requires a value")?),
            "-h" | "--help" => {
                println!("{USAGE}");
                std::process::exit(0);
            }
            other => return Err(format!("unknown argument: {other}")),
        }
    }

    if execute && explicit_dry_run {
        return Err("--execute and --dry-run are mutually exclusive".to_string());
    }
    if control_only && only.is_some() {
        return Err("--control-only and --only are mutually exclusive".to_string());
    }

    Ok(Args { opts: BackfillOpts { execute }, control_only, only })
}

/// Open a pool for one database. `max_connections(2)` mirrors
/// `backfill_reef_judgement_from_history`/`export_member_ocean`: a one-shot
/// needs no concurrency, and a tenant DB must never have its connection
/// budget eaten by a maintenance tool.
async fn connect(dsn: &str) -> Result<PgPool, String> {
    PgPoolOptions::new()
        .max_connections(2)
        .acquire_timeout(Duration::from_secs(15))
        .connect(dsn)
        .await
        .map_err(|e| format!("connect: {e}"))
}

/// Resolve a tenant's DSN and verify the database it lands on is the one the
/// routing row names — the same DSN-swap guard
/// `backfill_reef_judgement_from_history` applies, and for the identical
/// reason: a swapped/stale `TENANT_DB_<SLUG>` would stamp one org's jobs
/// with another org's account_id, which is far worse than leaving them
/// unattributed.
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

// `BackfillCounts` has no library-provided field-wise accumulator (unlike
// `judgement_backfill::BackfillCounts::add`) — this binary is the only
// caller that ever needs to sum across databases, so it lives here rather
// than growing the core module's public surface for one call site.
trait AddCounts {
    fn add(&mut self, other: &BackfillCounts);
}
impl AddCounts for BackfillCounts {
    fn add(&mut self, other: &BackfillCounts) {
        self.scanned += other.scanned;
        self.malformed_payload += other.malformed_payload;
        self.dead_atom += other.dead_atom;
        self.ambiguous_atom += other.ambiguous_atom;
        self.updated += other.updated;
        self.raced += other.raced;
        self.errors += other.errors;
    }
}

#[tokio::main]
async fn main() -> ExitCode {
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
                "MIGRATOR_DATABASE_URL (preferred) or DATABASE_URL must be set to the \
                 control-plane DSN"
            );
            return ExitCode::from(2);
        }
    };

    println!(
        "backfill_unattributed_job_accounts — ONE-SHOT, RUN ONCE PER DATABASE (mode={})",
        args.opts.mode()
    );
    match (&args.only, args.control_only) {
        (Some(name), _) => {
            println!("  this run covers: tenant '{name}' ONLY (control plane NOT included)");
        }
        (None, true) => {
            println!(
                "  this run covers: control plane ONLY (--control-only — org tenants are NOT \
                 being touched this run)"
            );
        }
        (None, false) => {
            println!(
                "  this run covers: control plane + every ACTIVE tenant (TENANT_DB_<SLUG> per \
                 routing row)"
            );
        }
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

    // Selection. `--only <tenant>` deliberately EXCLUDES the control plane —
    // same reasoning as `backfill_reef_judgement_from_history`: silently
    // folding the control plane into "just this tenant" would make an
    // operator's own record of what they ran untrue.
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

    let resolver = OperatorDsnResolver::from_env_or_env_only(control.clone(), "backfill_unattributed_job_accounts");
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
        println!("Nothing was written. Read the tallies above, then re-run with --execute to apply.");
    }
    if total.unresolvable() > 0 {
        println!(
            "{} row(s) remain unresolvable (dead atom / malformed payload / ambiguous account) — \
             this is expected steady state, not a failed run; see workers::billing_derivation's \
             UNATTRIBUTED_ALERT_WINDOW for how the alert stays quiet about them.",
            total.unresolvable()
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
