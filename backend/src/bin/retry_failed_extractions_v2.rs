//! `retry_failed_extractions_v2`: hand FAILED `source-atoms-v2` and
//! `source-atoms-v3` versions back to standing extraction under their own
//! profile, per database. No model call. The v3 retries need standing
//! running with `SOURCE_EXTRACTION_PROFILE=source-atoms-v3` to be adopted.
//! Bounded and idempotent.
//!
//! **Thin wrapper.** The work lives in
//! [`coralswarm_backend::failed_extraction_retry`]; read its module doc for
//! the mechanism, the charging rule and the permission/floor policy.
//!
//! ```text
//! retry_failed_extractions_v2 [--execute] [--graph-id <uuid>] [--limit <N>]
//!     [--version-ids-from <file>] [--emit-version-ids <file>]
//!     [--before-floor --reason <text> [--operator <name>]]
//!     (--tenant-dsn <dsn> | --only <slug|clerk_org_id> | --all-tenants | --control)
//! ```
//!
//! Default is a dry run (READ ONLY transactions, no session SET) that prints,
//! per error class, failed / eligible / skipped-by-reason counts and an
//! estimated v2 cost from this database's measured windows and billed
//! receipts. `--execute` retries up to `--limit` (default 500, max 20000)
//! versions; repeat to continue. The v2 standing sweep then extracts them
//! under the managed gate and its caps.
//!
//! `--version-ids-from <file>` restricts the pass to the listed versions (one
//! UUID per line; blank lines and `#` comments ignored). `--emit-version-ids
//! <file>` (dry run only) writes the eligible versions, so the reviewed list
//! is exactly what a later `--execute --version-ids-from` retries.
//!
//! `--before-floor` is the contained one-off for failed captures from before
//! standing's 2026-10-13 floor: only those are selected, and each retried
//! version gets a `standing_floor_overrides` row recording
//! `--reason` and `--operator` (default `$USER`), which is what lets standing
//! process it. One database per run (not `--all-tenants`). The global floor is
//! unchanged for every other capture.
//!
//! ## Env
//!
//!   MIGRATOR_DATABASE_URL  elevated control-plane DSN (falls back to
//!                          DATABASE_URL). Required by --control / --only /
//!                          --all-tenants. With --tenant-dsn it is optional.
//!                          When it is set, it settles which logical calls
//!                          still hold credit. Without it, every version that
//!                          ever reserved a receipt is skipped.
//!   TENANT_DB_<SLUG>       each tenant's owner DSN, via OperatorDsnResolver.
//!
//! Every DSN must bypass RLS on the tables touched (the tenant owner, which
//! inherits `coralswarm_tenant_app`, does). The tool refuses an RLS-filtered
//! role: it would see, and retry, only part of the database.
//!
//! ## Exit codes
//!
//!   0  every selected database processed
//!   1  a database failed
//!   2  usage error, missing control DSN, or unknown --only tenant
//!   3  a DSN does not bypass RLS

use std::process::ExitCode;
use std::time::{Duration, Instant};

use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use uuid::Uuid;

use coralswarm_backend::migrator_database_url;
use coralswarm_backend::node_atoms_bridge;
use coralswarm_backend::failed_extraction_retry::{self as retry, ControlCheck};
use coralswarm_backend::tenancy::routing::{self, TenantDatabase};
use coralswarm_backend::tenancy::secrets::OperatorDsnResolver;

const USAGE: &str = "usage: retry_failed_extractions_v2 [--execute] [--graph-id <uuid>] \
[--limit <N>] [--version-ids-from <file>] [--emit-version-ids <file>] \
[--before-floor --reason <text> [--operator <name>]] \
(--tenant-dsn <dsn> | --only <slug|clerk_org_id> | --all-tenants | --control)";

/// Tenant tables this tool reads or writes.
const TABLES: &[&str] = &[
    "corpus_enrichment_units",
    "corpus_enrichment_runs",
    "source_versions",
    "source_items",
    "graphs",
    "jobs",
    "managed_attempt_receipts",
    "semantic_atom_windows",
    "corpus_capture_admissions",
    "selected_evidence_publications",
];
/// Written by `--before-floor --execute` (the paired standing_floor_overrides migration).
const OVERRIDE_TABLE: &str = "standing_floor_overrides";

#[derive(Debug)]
enum Selector {
    TenantDsn(String),
    Only(String),
    AllTenants,
    Control,
}

#[derive(Debug)]
struct Args {
    execute: bool,
    graph_id: Option<Uuid>,
    limit: i64,
    selector: Selector,
    selection: retry::Selection,
    emit_version_ids: Option<std::path::PathBuf>,
}

/// One UUID per line; blank lines and `#` comments ignored; duplicates kept once.
fn parse_version_ids(text: &str) -> Result<Vec<Uuid>, String> {
    let mut out = std::collections::BTreeSet::new();
    for (n, line) in text.lines().enumerate() {
        let line = line.split('#').next().unwrap_or_default().trim();
        if line.is_empty() {
            continue;
        }
        out.insert(Uuid::parse_str(line).map_err(|e| format!("--version-ids-from line {}: invalid UUID: {e}", n + 1))?);
    }
    if out.is_empty() {
        return Err("--version-ids-from lists no versions".into());
    }
    if out.len() > retry::MAX_LISTED_VERSIONS {
        return Err(format!("--version-ids-from lists more than {} versions", retry::MAX_LISTED_VERSIONS));
    }
    Ok(out.into_iter().collect())
}

fn parse_args(args: impl Iterator<Item = String>) -> Result<Args, String> {
    let mut execute = false;
    let mut graph_id = None;
    let mut limit = retry::DEFAULT_LIMIT;
    let mut selector: Option<Selector> = None;
    let mut before_floor = false;
    let mut reason: Option<String> = None;
    let mut operator: Option<String> = None;
    let mut versions: Option<Vec<Uuid>> = None;
    let mut emit_version_ids = None;
    let set = |s: Selector, selector: &mut Option<Selector>| -> Result<(), String> {
        if selector.is_some() {
            return Err("--tenant-dsn / --only / --all-tenants / --control are mutually exclusive".into());
        }
        *selector = Some(s);
        Ok(())
    };
    let mut args = args;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--execute" => execute = true,
            "--dry-run" => execute = false,
            "--graph-id" => {
                let v = args.next().ok_or("--graph-id requires a value")?;
                graph_id = Some(Uuid::parse_str(&v).map_err(|e| format!("--graph-id: invalid UUID: {e}"))?);
            }
            "--limit" => {
                let v = args.next().ok_or("--limit requires a value")?;
                limit = v
                    .parse::<i64>()
                    .ok()
                    .filter(|n| (1..=20_000).contains(n))
                    .ok_or("--limit must be 1..=20000")?;
            }
            "--before-floor" => before_floor = true,
            "--reason" => reason = Some(args.next().ok_or("--reason requires a value")?),
            "--operator" => operator = Some(args.next().ok_or("--operator requires a value")?),
            "--version-ids-from" => {
                let path = args.next().ok_or("--version-ids-from requires a file")?;
                let text = std::fs::read_to_string(&path).map_err(|e| format!("--version-ids-from {path}: {e}"))?;
                versions = Some(parse_version_ids(&text)?);
            }
            "--emit-version-ids" => {
                emit_version_ids = Some(std::path::PathBuf::from(args.next().ok_or("--emit-version-ids requires a file")?));
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
    if execute && emit_version_ids.is_some() {
        return Err("--emit-version-ids is a dry-run option".into());
    }
    let floor_override = if before_floor {
        if matches!(selector, Selector::AllTenants) {
            return Err("--before-floor runs against one database (--tenant-dsn / --only / --control)".into());
        }
        let reason = reason.ok_or("--before-floor requires --reason <text>")?;
        let operator = operator.or_else(|| std::env::var("USER").ok()).unwrap_or_default();
        Some(retry::FloorOverride::new(&reason, &operator)?)
    } else {
        if reason.is_some() || operator.is_some() {
            return Err("--reason / --operator only apply with --before-floor".into());
        }
        None
    };
    Ok(Args {
        execute,
        graph_id,
        limit,
        selector,
        selection: retry::Selection { versions, floor_override },
        emit_version_ids,
    })
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

async fn run_one(label: &str, pool: &PgPool, control: Option<&ControlCheck<'_>>, args: &Args) -> Result<(), (u8, String)> {
    node_atoms_bridge::assert_policy_bypass_on(pool, TABLES).await.map_err(|m| (3, m))?;
    // A dry run can size the one-off before the table is deployed; writing an
    // override cannot happen without its table.
    let overrides_ready: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
        .bind(OVERRIDE_TABLE)
        .fetch_one(pool)
        .await
        .map_err(|e| (1, e.to_string()))?;
    if overrides_ready {
        node_atoms_bridge::assert_policy_bypass_on(pool, &[OVERRIDE_TABLE]).await.map_err(|m| (3, m))?;
    } else if args.selection.floor_override.is_some() {
        if args.execute {
            return Err((1, format!("REFUSING --before-floor --execute: no `{OVERRIDE_TABLE}` table (its migration is not applied)")));
        }
        println!("  note: `{OVERRIDE_TABLE}` is not in this database yet (migration not applied); dry run only");
    }
    if let Some(c) = control {
        node_atoms_bridge::assert_policy_bypass_on(c.pool, &["managed_credit_reservations"])
            .await
            .map_err(|m| (3, format!("control plane: {m}")))?;
    }
    let db: String = sqlx::query_scalar("SELECT current_database()").fetch_one(pool).await.map_err(|e| (1, e.to_string()))?;
    println!("[{label}] database={db} control_settlement_check={}", control.map(|c| c.plane.as_str()).unwrap_or("UNAVAILABLE"));
    let candidates = retry::classify_selected(pool, args.graph_id, control, &args.selection)
        .await
        .map_err(|e| (1, e))?;
    let basis = retry::cost_basis(pool).await.map_err(|e| (1, format!("cost basis: {e}")))?;
    let p = retry::summarize(&candidates, &basis);
    if let Some(grant) = &args.selection.floor_override {
        println!("  BEFORE-FLOOR one-off: only failed captures before 2026-10-13; each retried version records a standing_floor_overrides row");
        println!("    reason={:?} operator={:?}", grant.reason, grant.granted_by);
    }
    if let Some(listed) = &args.selection.versions {
        let found = candidates.len();
        println!(
            "  listed versions: {} (failed and in scope: {found}, not failed / other graph / outside this selection: {})",
            listed.len(),
            listed.len().saturating_sub(found)
        );
        if overrides_ready {
            let recorded = retry::recorded_overrides(pool, listed).await.map_err(|e| (1, format!("override read: {e}")))?;
            println!("  floor overrides already recorded for listed versions: {recorded}");
        }
    }
    println!("  failed versions: {} (eligible: {})", p.total.failed, p.total.eligible);
    println!("  {:<34} {:>7} {:>9}  skipped by reason", "error class", "failed", "eligible");
    for (class, counts) in &p.by_error {
        let skipped: Vec<String> = counts.skipped.iter().map(|(k, v)| format!("{k}={v}")).collect();
        println!("  {:<34} {:>7} {:>9}  {}", truncate(class, 34), counts.failed, counts.eligible, skipped.join(" "));
    }
    let actions: Vec<String> = p.eligible_by_action.iter().map(|(k, v)| format!("{k}={v}")).collect();
    println!("  eligible by action: {}", if actions.is_empty() { "none".into() } else { actions.join(" ") });
    println!(
        "  estimated v2 cost: ~{:.0} windows x ${:.5} ({}) = ${:.2}, plus up to one Jev call per window with unbound mentions",
        p.estimated_windows,
        p.usd_per_window,
        if p.measured_cost { "measured mean billed extraction attempt" } else { "fallback; no billed receipts here" },
        p.estimated_usd()
    );
    if !args.execute {
        if let Some(path) = &args.emit_version_ids {
            let ids: Vec<String> = candidates.iter().filter(|c| c.bucket == "eligible").map(|c| c.version_id.to_string()).collect();
            let mut text = format!("# retry_failed_extractions_v2 eligible versions ({label}, {} ids)\n", ids.len());
            text.push_str(&ids.join("\n"));
            text.push('\n');
            std::fs::write(path, text).map_err(|e| (1, format!("--emit-version-ids {}: {e}", path.display())))?;
            println!("  wrote {} eligible version ids to {}", ids.len(), path.display());
        }
        println!("  dry run: nothing written. Pass --execute to retry (up to --limit {} versions).", args.limit);
        eprintln!("summary: [{label}] mode=dry-run failed={} eligible={}", p.total.failed, p.total.eligible);
        return Ok(());
    }
    let started = Instant::now();
    let done = retry::execute_selected(pool, args.graph_id, control, args.limit, &args.selection)
        .await
        .map_err(|e| (1, e))?;
    println!(
        "  done: requeued_v2={} readopted_v2={} requeued_v3={} readopted_v3={} skipped_after_recheck={} floor_overrides_recorded={} elapsed={:.1}s",
        done.requeued_v2, done.readopted_v2, done.requeued_v3, done.readopted_v3, done.skipped, done.overrides_recorded,
        started.elapsed().as_secs_f64()
    );
    eprintln!(
        "summary: [{label}] mode=execute retried={} skipped={}",
        done.requeued_v2 + done.readopted_v2 + done.requeued_v3 + done.readopted_v3,
        done.skipped
    );
    Ok(())
}

fn truncate(value: &str, max: usize) -> String {
    if value.chars().count() <= max { value.to_owned() } else { value.chars().take(max - 1).chain(['~']).collect() }
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
        "retry_failed_extractions_v2 (mode={}, limit={}, before_floor={}, listed={})",
        if args.execute { "EXECUTE" } else { "DRY RUN" },
        args.limit,
        args.selection.floor_override.is_some(),
        args.selection.versions.as_ref().map(|v| v.len().to_string()).unwrap_or_else(|| "all".into())
    );
    let control_url = migrator_database_url();
    let control = match (&args.selector, &control_url) {
        (Selector::TenantDsn(_), None) => None,
        (_, Some(url)) => match connect(url).await {
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
                let org: Option<String> = sqlx::query_scalar("SELECT org_id FROM tenant_meta WHERE id = true")
                    .fetch_optional(&pool)
                    .await
                    .ok()
                    .flatten();
                let check = control.as_ref().map(|c| ControlCheck {
                    pool: c,
                    plane: org.as_ref().map(|o| format!("org:{o}")).unwrap_or_else(|| "consumer".into()),
                });
                record("tenant-dsn", run_one("tenant-dsn", &pool, check.as_ref(), &args).await);
                pool.close().await;
            }
            Err(e) => {
                eprintln!("failed to connect: {e}");
                return ExitCode::FAILURE;
            }
        },
        Selector::Control => {
            let c = control.as_ref().expect("control pool");
            let check = ControlCheck { pool: c, plane: "consumer".into() };
            record("control", run_one("control", c, Some(&check), &args).await);
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
            let resolver = OperatorDsnResolver::from_env_or_env_only(c.clone(), "retry_failed_extractions_v2");
            for t in &tenants {
                let label = format!("tenant:{}", t.slug);
                match connect_tenant(&resolver, t).await {
                    Ok(pool) => {
                        let check = ControlCheck { pool: c, plane: format!("org:{}", t.clerk_org_id) };
                        record(&label, run_one(&label, &pool, Some(&check), &args).await);
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
        assert!(!a.execute);
        assert_eq!(a.limit, retry::DEFAULT_LIMIT);
    }

    #[test]
    fn before_floor_needs_a_reason_and_one_database() {
        assert!(parse(&["--only", "coralswarm", "--before-floor"]).is_err(), "a reason is required");
        assert!(parse(&["--all-tenants", "--before-floor", "--reason", "pre-floor one-off"]).is_err());
        assert!(parse(&["--only", "coralswarm", "--reason", "pre-floor one-off"]).is_err(), "--reason alone is meaningless");
        assert!(parse(&["--only", "coralswarm", "--before-floor", "--reason", "short"]).is_err());
        let a = parse(&["--only", "coralswarm", "--before-floor", "--reason", "COR-99 pre-floor one-off", "--operator", "Alex"]).unwrap();
        assert!(!a.execute, "dry run stays the default");
        let grant = a.selection.floor_override.unwrap();
        assert_eq!((grant.reason.as_str(), grant.granted_by.as_str()), ("COR-99 pre-floor one-off", "Alex"));
        assert!(parse(&["--only", "coralswarm", "--execute", "--emit-version-ids", "/dev/null"]).is_err());
    }

    #[test]
    fn version_id_files_parse_strictly() {
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();
        let ids = parse_version_ids(&format!("# header\n{a}\n\n{b}  # note\n{a}\n")).unwrap();
        assert_eq!(ids.len(), 2, "duplicates kept once");
        assert!(ids.contains(&a) && ids.contains(&b));
        assert!(parse_version_ids("# nothing\n").is_err());
        assert!(parse_version_ids("not-a-uuid\n").is_err());
    }

    #[test]
    fn bounds_are_validated() {
        assert!(parse(&["--control", "--limit", "0"]).is_err());
        let a = parse(&["--control", "--execute", "--limit", "50"]).unwrap();
        assert_eq!((a.execute, a.limit), (true, 50));
    }
}
