//! `backfill_chunk_links`: one-shot, per database. Link chunkless or partial
//! source versions to their positional chunk atoms, ONLY where the atom's
//! stored text equals the version's re-chunked text (step C of the
//! adopt-or-replace ingest fix). Counts and writes live in
//! [`coralswarm_backend::chunk_reconcile::backfill`]; read its module doc.
//!
//! ```text
//! backfill_chunk_links [--execute] [--replace-current] [--graph-id <uuid>]
//!     [--batch-size <N>] [--max-batches <N>]
//!     (--tenant-dsn <dsn> | --only <slug|clerk_org_id> | --all-tenants | --control)
//! backfill_chunk_links --rechunk-oversized [--execute] [--graph-id <uuid>]
//!     (--tenant-dsn <dsn> | --only <slug|clerk_org_id> | --all-tenants | --control)
//! ```
//!
//! Default is a dry run: every read happens in a `BEGIN READ ONLY`
//! transaction and nothing is written. `--execute` links verified positions
//! (one short transaction per version, under the capture source lock) and
//! stamps `atoms.chunk_sha256` for atoms it read. `--replace-current`
//! additionally replaces MISMATCHED positions of a source's CURRENT version
//! in place (content-revision CAS, re-embedded); without it they are only
//! counted. Mismatches on non-current versions are always left alone.
//! Re-running is safe: linked positions are skipped.
//!
//! `--rechunk-oversized` is a separate mode: it re-chunks, with today's
//! bounded chunker, the current version of every source holding an atom the
//! managed embedder refused as over the provider's input limit (stamped
//! `blocked:input_too_long`). The oversized position is replaced in place and
//! the rest of its text lands at new or replaced positions, each queued for
//! embedding; nothing is truncated. See
//! [`coralswarm_backend::chunk_reconcile::backfill::rechunk_oversized`].
//!
//! ## Order of operations
//!
//! 1. `backfill_chunk_links --only <tenant>` (dry run), review the counts.
//! 2. `backfill_chunk_links --only <tenant> --execute`.
//! 3. `backfill_node_atoms_bridge --only <tenant> --execute`, so reefs whose
//!    versions just gained chunk rows get their `node_atoms` links.
//!
//! ## Env
//!
//!   MIGRATOR_DATABASE_URL  elevated control-plane DSN (falls back to
//!                          DATABASE_URL). Required by --control / --only /
//!                          --all-tenants.
//!   TENANT_DB_<SLUG>       each tenant's owner DSN (OperatorDsnResolver).
//!   R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET_NAME,
//!   MASTER_KEK_HEX         to read blob-backed version and chunk text. When
//!                          absent, inline text is still verified and every
//!                          blob-backed version or chunk is counted as
//!                          unavailable/unverifiable, never linked.
//!
//! Every DSN must bypass RLS on the tables touched (owner role without FORCE,
//! or BYPASSRLS). The tool refuses otherwise.
//!
//! ## Exit codes
//!
//!   0  every selected database processed
//!   1  a database failed (connection, preflight, fingerprint, query)
//!   2  usage error, missing control DSN, or unknown --only tenant

use std::process::ExitCode;
use std::time::{Duration, Instant};

use coralswarm_core::r2::R2Client;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use uuid::Uuid;

use coralswarm_backend::chunk_reconcile::backfill::{self, Options, Report, DEFAULT_BATCH_SIZE};
use coralswarm_backend::migrator_database_url;
use coralswarm_backend::tenancy::routing::{self, TenantDatabase};
use coralswarm_backend::tenancy::secrets::OperatorDsnResolver;

const USAGE: &str = "usage: backfill_chunk_links [--execute] [--replace-current | --rechunk-oversized] \
[--graph-id <uuid>] [--batch-size <N>] [--max-batches <N>] \
(--tenant-dsn <dsn> | --only <slug|clerk_org_id> | --all-tenants | --control)";

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
    replace_current: bool,
    rechunk_oversized: bool,
    graph_id: Option<Uuid>,
    batch_size: i64,
    max_batches: Option<u64>,
    selector: Selector,
}

fn parse_args(args: impl Iterator<Item = String>) -> Result<Args, String> {
    let mut execute = false;
    let mut replace_current = false;
    let mut rechunk_oversized = false;
    let mut graph_id = None;
    let mut batch_size = DEFAULT_BATCH_SIZE;
    let mut max_batches = None;
    let mut selector: Option<Selector> = None;
    let set = |s: Selector, selector: &mut Option<Selector>| -> Result<(), String> {
        if selector.is_some() {
            return Err(
                "--tenant-dsn / --only / --all-tenants / --control are mutually exclusive".into(),
            );
        }
        *selector = Some(s);
        Ok(())
    };
    let mut args = args;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--execute" => execute = true,
            "--dry-run" => execute = false,
            "--replace-current" => replace_current = true,
            "--rechunk-oversized" => rechunk_oversized = true,
            "--graph-id" => {
                let v = args.next().ok_or("--graph-id requires a value")?;
                graph_id =
                    Some(Uuid::parse_str(&v).map_err(|e| format!("--graph-id: invalid UUID: {e}"))?);
            }
            "--batch-size" => {
                let v = args.next().ok_or("--batch-size requires a value")?;
                batch_size = v
                    .parse::<i64>()
                    .ok()
                    .filter(|n| (1..=5000).contains(n))
                    .ok_or("--batch-size must be 1..=5000")?;
            }
            "--max-batches" => {
                let v = args.next().ok_or("--max-batches requires a value")?;
                max_batches = Some(
                    v.parse::<u64>()
                        .ok()
                        .filter(|n| *n > 0)
                        .ok_or("--max-batches must be a positive integer")?,
                );
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
    let selector = selector.ok_or(
        "exactly one of --tenant-dsn / --only / --all-tenants / --control is required",
    )?;
    if rechunk_oversized && replace_current {
        return Err("--rechunk-oversized and --replace-current are separate modes".into());
    }
    Ok(Args {
        execute,
        replace_current,
        rechunk_oversized,
        graph_id,
        batch_size,
        max_batches,
        selector,
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

/// DSN-swap guard: the tenant DB's
/// `tenant_meta` fingerprint must match the routing row before any write.
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
    if fingerprint.as_deref() == Some(t.clerk_org_id.as_str()) {
        return Ok(pool);
    }
    pool.close().await;
    Err(format!(
        "tenant_meta fingerprint {fingerprint:?} does not match routing row {}; refusing \
         (DSN-swap guard)",
        t.clerk_org_id
    ))
}

fn print_report(r: &Report) {
    println!(
        "  versions={} (reef_linked={} current={}) text_unavailable={} complete_after={}\n  \
         positions={} already_linked={} linked={} replaced={} mismatched_current={} \
         mismatched_noncurrent={} unverifiable={} missing_atoms={} conflicts={} hashes_stamped={} \
         legacy_identity_supports={}",
        r.versions_examined, r.versions_reef_linked, r.versions_current, r.versions_text_unavailable,
        r.versions_complete_after, r.positions_examined, r.already_linked, r.linked, r.replaced,
        r.mismatched_current, r.mismatched_noncurrent, r.unverifiable, r.missing_atoms, r.conflicts,
        r.hashes_stamped, r.legacy_identity_supports
    );
}

async fn run_one(label: &str, pool: &PgPool, r2: Option<&R2Client>, args: &Args) -> bool {
    if let Err(msg) = coralswarm_backend::node_atoms_bridge::assert_policy_bypass_on(pool, backfill::TABLES).await {
        eprintln!("FAILED [{label}]: {msg}");
        return false;
    }
    let opts = Options {
        graph_id: args.graph_id,
        batch_size: args.batch_size,
        max_batches: args.max_batches,
        execute: args.execute,
        replace_current: args.replace_current,
    };
    println!("[{label}]");
    let started = Instant::now();
    if args.rechunk_oversized {
        return match backfill::rechunk_oversized(pool, r2, args.graph_id, args.execute).await {
            Ok(r) => {
                println!(
                    "  atoms_stamped={} atoms_unbound={} versions={} text_unavailable={} failed={}\n  \
                     positions_before={} positions_after={} inserted={} replaced={} adopted={} \
                     retired={} conflicts={} unverified={} still_stamped={}",
                    r.atoms_stamped, r.atoms_unbound, r.versions, r.versions_text_unavailable,
                    r.versions_failed, r.positions_before, r.positions_after, r.inserted, r.replaced,
                    r.adopted, r.retired, r.conflicts, r.unverified, r.still_stamped
                );
                println!(
                    "  {} elapsed={:.1}s",
                    if args.execute { "done:" } else { "dry run, nothing written:" },
                    started.elapsed().as_secs_f64()
                );
                eprintln!(
                    "summary: [{label}] mode=rechunk-oversized-{} versions={} failed={} still_stamped={}",
                    if args.execute { "execute" } else { "dry-run" },
                    r.versions, r.versions_failed, r.still_stamped
                );
                r.versions_failed == 0
            }
            Err(e) => {
                eprintln!("FAILED [{label}]: {e}");
                false
            }
        };
    }
    match backfill::run(pool, r2, &opts, |b| {
        println!("  batch versions={} linked={} replaced={}", b.versions_examined, b.linked, b.replaced)
    })
    .await
    {
        Ok(r) => {
            print_report(&r);
            println!(
                "  {} batches={} elapsed={:.1}s",
                if args.execute { "done:" } else { "dry run, nothing written:" },
                r.batches,
                started.elapsed().as_secs_f64()
            );
            eprintln!(
                "summary: [{label}] mode={} linked={} replaced={} mismatched_current={} mismatched_noncurrent={}",
                if args.execute { "execute" } else { "dry-run" },
                r.linked, r.replaced, r.mismatched_current, r.mismatched_noncurrent
            );
            true
        }
        Err(e) => {
            eprintln!("FAILED [{label}]: {e} (committed versions stay; re-run resumes)");
            false
        }
    }
}

async fn r2_from_env() -> Option<R2Client> {
    match (
        std::env::var("R2_ACCOUNT_ID"),
        std::env::var("R2_ACCESS_KEY_ID"),
        std::env::var("R2_SECRET_ACCESS_KEY"),
        std::env::var("R2_BUCKET_NAME"),
    ) {
        (Ok(a), Ok(k), Ok(s), Ok(b)) => Some(R2Client::new(&a, &k, &s, &b).await),
        _ => None,
    }
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
        "backfill_chunk_links (mode={}, replace_current={}, rechunk_oversized={}, batch_size={}, max_batches={:?})",
        if args.execute { "EXECUTE" } else { "DRY RUN" },
        args.replace_current,
        args.rechunk_oversized,
        args.batch_size,
        args.max_batches
    );
    let r2 = r2_from_env().await;
    if r2.is_none() {
        println!("R2_* not set: blob-backed text is counted as unavailable, never linked");
    }
    let r2 = r2.as_ref();
    let mut clean = true;
    match &args.selector {
        Selector::TenantDsn(dsn) => match connect(dsn).await {
            Ok(pool) => {
                clean = run_one("tenant-dsn", &pool, r2, &args).await;
                pool.close().await;
            }
            Err(e) => {
                eprintln!("failed to connect: {e}");
                return ExitCode::FAILURE;
            }
        },
        Selector::Control | Selector::Only(_) | Selector::AllTenants => {
            let Some(url) = migrator_database_url() else {
                eprintln!(
                    "MIGRATOR_DATABASE_URL (preferred) or DATABASE_URL must be set for \
                     --control/--only/--all-tenants"
                );
                return ExitCode::from(2);
            };
            let control = match connect(&url).await {
                Ok(p) => p,
                Err(e) => {
                    eprintln!("failed to connect control-plane database: {e}");
                    return ExitCode::FAILURE;
                }
            };
            let tenants: Vec<TenantDatabase> = match &args.selector {
                Selector::Control => {
                    clean = run_one("control", &control, r2, &args).await;
                    Vec::new()
                }
                Selector::Only(name) => {
                    let row = match routing::lookup_by_slug(&control, name).await {
                        Ok(Some(r)) => Ok(Some(r)),
                        Ok(None) => routing::lookup_by_org(&control, name).await,
                        Err(e) => Err(e),
                    };
                    match row {
                        Ok(Some(r)) => vec![r],
                        Ok(None) => {
                            eprintln!("no tenant_databases routing row for '{name}'");
                            control.close().await;
                            return ExitCode::from(2);
                        }
                        Err(e) => {
                            eprintln!("routing lookup failed: {e}");
                            control.close().await;
                            return ExitCode::FAILURE;
                        }
                    }
                }
                Selector::AllTenants => match routing::list_active(&control).await {
                    Ok(rows) => {
                        if rows.is_empty() {
                            println!("--all-tenants: no active tenants in the registry");
                        }
                        rows
                    }
                    Err(e) => {
                        eprintln!("failed to enumerate active tenants: {e}");
                        control.close().await;
                        return ExitCode::FAILURE;
                    }
                },
                Selector::TenantDsn(_) => unreachable!("handled above"),
            };
            let resolver = OperatorDsnResolver::from_env_or_env_only(control.clone(), "backfill_chunk_links");
            for t in &tenants {
                let label = format!("tenant:{}", t.slug);
                match connect_tenant(&resolver, t).await {
                    Ok(pool) => {
                        clean &= run_one(&label, &pool, r2, &args).await;
                        pool.close().await;
                    }
                    Err(e) => {
                        eprintln!("FAILED [{label}]: {e}");
                        clean = false;
                    }
                }
            }
            control.close().await;
        }
    }
    if clean {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(v: &[&str]) -> Result<Args, String> {
        parse_args(v.iter().map(|s| s.to_string()))
    }

    #[test]
    fn dry_run_is_default_and_replace_is_opt_in() {
        assert!(parse(&[]).is_err());
        assert!(parse(&["--control", "--all-tenants"]).is_err());
        let a = parse(&["--only", "coralswarm"]).unwrap();
        assert!(!a.execute && !a.replace_current);
        let a = parse(&["--control", "--execute", "--replace-current", "--batch-size", "5"]).unwrap();
        assert!(a.execute && a.replace_current);
        assert_eq!(a.batch_size, 5);
        assert!(parse(&["--control", "--batch-size", "0"]).is_err());
    }

    #[test]
    fn rechunk_oversized_is_its_own_dry_run_by_default_mode() {
        let a = parse(&["--only", "coralswarm", "--rechunk-oversized"]).unwrap();
        assert!(a.rechunk_oversized && !a.execute && !a.replace_current);
        assert!(parse(&["--control", "--rechunk-oversized", "--execute"]).unwrap().execute);
        assert!(parse(&["--control", "--rechunk-oversized", "--replace-current"]).is_err());
    }
}
