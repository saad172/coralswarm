//! `dedupe_reefs`: merge duplicate reefs (same graph, entity type, scope,
//! owner and normalized title) through the existing human-merge path,
//! per database. No model call. Dry run by default.
//!
//! **Thin wrapper.** The policy and the merge live in
//! [`coralswarm_backend::reef_dedupe`]; read its module doc.
//!
//! ```text
//! dedupe_reefs [--execute --created-by <user uuid>] [--graph-id <uuid>]
//!     [--rule named-entities|same-title]
//!     [--cutoff <rfc3339>] [--entity-types person,org,...] [--include-preexisting]
//!     [--max-groups <N>] [--pause-ms <N>] [--top <N>]
//!     (--tenant-dsn <dsn> | --only <slug|clerk_org_id> | --all-tenants | --control)
//! ```
//!
//! The dry run (READ ONLY, no session SET) prints the groups and reefs to
//! merge, the links and edges that move, every excluded title with its
//! reason and copy count, and the largest groups. `--execute` merges one
//! duplicate per transaction, every reef with a human POSITIVE `reef_judgement`
//! authored by `--created-by` and a `node_merges` undo row. It is
//! resumable: re-run to continue, and a finished run finds nothing.
//! `--max-groups` bounds a rehearsal. `--pause-ms` sleeps between duplicate merges
//! (default 0) to yield further to live traffic. Each duplicate merges in its own
//! short transaction with lock and statement timeouts (see `reef_dedupe`).
//!
//! `--rule same-title` is the #1516 one-off merge (see the module doc of
//! [`coralswarm_backend::reef_dedupe`]): every same-title group of one
//! title identity class (`org`; `topic` and `project` together) of any age,
//! no stoplist, only human negative judgements skip a pair, `person` never.
//! The keeper (self reef, else oldest) keeps its own kind. It refuses `--entity-types person`. The
//! runbook is `docs/runbooks/reef-same-title-merge.md`.
//!
//! Reverse a merge with `POST /v1/nodes/merges/{merge_id}/undo` (or
//! `Store::unmerge_nodes`). The merges are listed by
//! `SELECT m.merge_id FROM node_merges m JOIN reef_judgement j USING(judgement_id)
//!  WHERE j.evidence->>'route'='ops_dedupe_reefs_cli'`.
//!
//! ## Env
//!
//!   MIGRATOR_DATABASE_URL  control-plane DSN for --control / --only / --all-tenants.
//!   TENANT_DB_<SLUG>       each tenant's owner DSN, via OperatorDsnResolver.
//!
//! ## Exit codes
//!
//!   0  every selected database processed (merge failures are reported, and
//!      make the exit 1)
//!   1  a database or a group failed
//!   2  usage error
//!   3  the DSN does not bypass RLS

use std::process::ExitCode;
use std::time::{Duration, Instant};

use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use uuid::Uuid;

use coralswarm_backend::migrator_database_url;
use coralswarm_backend::node_atoms_bridge;
use coralswarm_backend::reef_dedupe::{self, Policy};
use coralswarm_backend::tenancy::routing::{self, TenantDatabase};
use coralswarm_backend::tenancy::secrets::OperatorDsnResolver;

const USAGE: &str = "usage: dedupe_reefs [--execute --created-by <uuid>] [--graph-id <uuid>] \
[--rule named-entities|same-title] [--cutoff <rfc3339>] [--entity-types a,b] [--include-preexisting] [--max-groups <N>] [--pause-ms <N>] [--top <N>] \
(--tenant-dsn <dsn> | --only <slug|clerk_org_id> | --all-tenants | --control)";

const TABLES: &[&str] = &[
    "nodes",
    "node_atoms",
    "node_semantic_atoms",
    "relationship_edge",
    "supersession_edge",
    "node_promotions",
    "node_merges",
    "reef_judgement",
    "reef_identifiers",
    "reef_identity_corrections",
    "graphs",
];

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
    created_by: Option<Uuid>,
    policy: Policy,
    max_groups: Option<usize>,
    pause_ms: u64,
    top: usize,
    selector: Selector,
}

fn next_val(args: &mut impl Iterator<Item = String>, flag: &str) -> Result<String, String> {
    args.next().ok_or_else(|| format!("{flag} requires a value"))
}

fn parse_args(args: impl Iterator<Item = String>) -> Result<Args, String> {
    let mut execute = false;
    let mut created_by = None;
    let mut rule = reef_dedupe::Rule::NamedEntities;
    let mut graph = None;
    let mut cutoff = None;
    let mut entity_types: Option<Vec<String>> = None;
    let mut include_preexisting = false;
    let mut max_groups = None;
    let mut pause_ms = 0u64;
    let mut top = 25;
    let mut selector: Option<Selector> = None;
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
            "--created-by" => created_by = Some(Uuid::parse_str(&next_val(&mut args, "--created-by")?).map_err(|e| format!("--created-by: {e}"))?),
            "--graph-id" => graph = Some(Uuid::parse_str(&next_val(&mut args, "--graph-id")?).map_err(|e| format!("--graph-id: {e}"))?),
            "--rule" => {
                rule = match next_val(&mut args, "--rule")?.as_str() {
                    "named-entities" => reef_dedupe::Rule::NamedEntities,
                    "same-title" => reef_dedupe::Rule::SameTitle,
                    other => return Err(format!("--rule: unknown rule {other:?} (named-entities | same-title)")),
                }
            }
            "--cutoff" => cutoff = Some(next_val(&mut args, "--cutoff")?.parse().map_err(|e| format!("--cutoff: {e}"))?),
            "--entity-types" => {
                let types: Vec<String> = next_val(&mut args, "--entity-types")?.split(',').map(|s| s.trim().to_lowercase()).filter(|s| !s.is_empty()).collect();
                if types.is_empty() {
                    return Err("--entity-types needs at least one type".into());
                }
                entity_types = Some(types);
            }
            "--include-preexisting" => include_preexisting = true,
            "--pause-ms" => pause_ms = next_val(&mut args, "--pause-ms")?.parse::<u64>().map_err(|e| format!("--pause-ms: {e}"))?,
            "--max-groups" => {
                max_groups = Some(next_val(&mut args, "--max-groups")?.parse::<usize>().ok().filter(|n| *n >= 1).ok_or("--max-groups must be >= 1")?)
            }
            "--top" => top = next_val(&mut args, "--top")?.parse().map_err(|_| "--top must be a number")?,
            "--tenant-dsn" => {
                let v = next_val(&mut args, "--tenant-dsn")?;
                set(Selector::TenantDsn(v), &mut selector)?
            }
            "--only" => {
                let v = next_val(&mut args, "--only")?;
                set(Selector::Only(v), &mut selector)?
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
    let mut policy = match rule {
        reef_dedupe::Rule::NamedEntities => Policy::default(),
        reef_dedupe::Rule::SameTitle => Policy::same_title(),
    };
    policy.graph = graph;
    if let Some(cutoff) = cutoff {
        policy.cutoff = cutoff;
    }
    if let Some(types) = entity_types {
        if rule == reef_dedupe::Rule::SameTitle && types.iter().any(|t| t == "person") {
            return Err("--rule same-title never merges person reefs; drop person from --entity-types".into());
        }
        policy.entity_types = types;
    }
    policy.include_preexisting |= include_preexisting;
    if execute && created_by.is_none() {
        return Err("--execute requires --created-by <uuid> (the operator the merge judgements are attributed to)".into());
    }
    let selector = selector.ok_or("exactly one of --tenant-dsn / --only / --all-tenants / --control is required")?;
    Ok(Args { execute, created_by, policy, max_groups, pause_ms, top, selector })
}

async fn connect(dsn: &str) -> Result<PgPool, String> {
    PgPoolOptions::new()
        .max_connections(2)
        .acquire_timeout(Duration::from_secs(15))
        .connect(dsn)
        .await
        .map_err(|e| format!("connect: {e}"))
}

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
    Err(format!("tenant_meta fingerprint {fingerprint:?} does not match routing row {}; refusing (DSN-swap guard)", t.clerk_org_id))
}

async fn run_one(label: &str, pool: &PgPool, args: &Args) -> Result<(), (u8, String)> {
    node_atoms_bridge::assert_policy_bypass_on(pool, TABLES).await.map_err(|m| (3, m))?;
    let db: String = sqlx::query_scalar("SELECT current_database()").fetch_one(pool).await.map_err(|e| (1, e.to_string()))?;
    let p = &args.policy;
    println!(
        "[{label}] database={db} rule={} cutoff={} entity_types={} include_preexisting={}",
        p.rule.as_str(),
        p.cutoff.to_rfc3339(),
        p.entity_types.join(","),
        p.include_preexisting
    );
    let groups = reef_dedupe::plan(pool, p).await.map_err(|e| (1, format!("plan: {e}")))?;
    let r = reef_dedupe::report(&groups, p);
    println!("  groups to merge: {}  reefs to merge away: {}", r.groups_to_merge, r.reefs_to_merge);
    println!(
        "  moved onto keepers (upper bounds): node_atoms={} node_semantic_atoms={} edges re-pointed or de-duplicated={}",
        r.node_atoms_moved, r.semantic_links_moved, r.edges_touched
    );
    if !p.include_preexisting {
        println!("  pre-cutoff duplicate copies left in merged groups (scope A): {}", r.preexisting_left);
    }
    let key = if p.rule == reef_dedupe::Rule::SameTitle { "identity class" } else { "entity type" };
    println!("  by {key} (groups, reefs merged away):");
    for (et, (g, n)) in &r.by_entity_type {
        println!("    {et:<14} groups={g:<6} reefs={n}");
    }
    println!("  by kinds in the group (groups, reefs merged away):");
    for (kinds, (g, n)) in &r.by_kinds {
        println!("    {kinds:<14} groups={g:<6} reefs={n}");
    }
    println!("  by keeper kind, which the merged reef keeps (groups, reefs merged away):");
    for (kind, (g, n)) in &r.by_keeper_kind {
        println!("    {kind:<14} groups={g:<6} reefs={n}");
    }
    println!("  by graph and {key} (groups, reefs merged away):");
    for ((graph, et), (g, n)) in &r.by_graph_type {
        println!("    graph={graph} {et:<14} groups={g:<6} reefs={n}");
    }
    println!("  by graph and kinds in the group (groups, reefs merged away):");
    for ((graph, kinds), (g, n)) in &r.by_graph_kinds {
        println!("    graph={graph} {kinds:<14} groups={g:<6} reefs={n}");
    }
    println!("  excluded (reason: groups, reefs that would otherwise merge):");
    for (why, (g, n)) in &r.excluded {
        println!("    {why:<36} groups={g:<6} reefs={n}");
    }
    println!("  excluded titles by reason (top {} per reason; entity type, title, copies):", args.top);
    let mut reasons: Vec<&str> = r.excluded_titles.iter().map(|t| t.2).collect();
    reasons.dedup();
    reasons.sort();
    reasons.dedup();
    for why in reasons {
        let rows: Vec<_> = r.excluded_titles.iter().filter(|t| t.2 == why).collect();
        println!("    [{why}] {} titles", rows.len());
        for (et, title, _, n) in rows.iter().take(args.top) {
            println!("      {et:<10} {title:<40} {n}");
        }
    }
    let mut top: Vec<_> = groups.iter().filter(|g| g.excluded.is_none() && !g.dups.is_empty()).collect();
    top.sort_by(|a, b| b.dups.len().cmp(&a.dups.len()).then(a.norm_title.cmp(&b.norm_title)));
    println!("  largest groups (top {}):", args.top);
    for g in top.iter().take(args.top) {
        println!(
            "    {:<14} {:<36} merge={:<5} kinds={} keep={} ({}, created {}, corals {}{}) graph={}",
            g.entity_type,
            g.norm_title,
            g.dups.len(),
            g.kinds(),
            g.keep.node_id,
            g.keep.kind,
            g.keep.created_at.format("%Y-%m-%d"),
            g.keep.coral_count,
            if g.keep.is_self { ", self reef" } else { "" },
            g.graph_id
        );
    }
    if !args.execute {
        println!("  dry run: nothing written. Pass --execute --created-by <uuid> to merge.");
        eprintln!("summary: [{label}] mode=dry-run groups={} reefs={}", r.groups_to_merge, r.reefs_to_merge);
        return Ok(());
    }
    let started = Instant::now();
    let created_by = args.created_by.expect("validated");
    let opts = reef_dedupe::ExecOptions { pause_ms: args.pause_ms, ..Default::default() };
    let done = reef_dedupe::execute_with(pool, p, created_by, args.max_groups, &opts).await.map_err(|e| (1, format!("execute: {e}")))?;
    println!(
        "  done: groups_merged={} groups_partial={} reefs_merged={} corals_moved={} groups_changed_since_plan={} reefs_changed_since_plan={} groups_failed={} reefs_failed={} elapsed={:.1}s",
        done.groups_merged,
        done.groups_partial,
        done.reefs_merged,
        done.corals_moved,
        done.groups_changed,
        done.reefs_changed,
        done.groups_failed,
        done.reefs_failed,
        started.elapsed().as_secs_f64()
    );
    for f in &done.failures {
        eprintln!("  duplicate failed (its transaction rolled back; re-run to retry): {f}");
    }
    eprintln!(
        "summary: [{label}] mode=execute groups={} partial={} reefs={} failed={}",
        done.groups_merged, done.groups_partial, done.reefs_merged, done.reefs_failed
    );
    if let Some(why) = &done.aborted {
        return Err((1, format!("aborted: {why}; {} group(s) not attempted; merges before the abort are committed, re-run to resume", done.groups_not_attempted)));
    }
    if done.reefs_failed > 0 {
        return Err((1, format!("{} duplicate(s) in {} group(s) failed and were rolled back; re-run to retry", done.reefs_failed, done.groups_failed)));
    }
    Ok(())
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
        "dedupe_reefs (mode={}, max_groups={:?})",
        if args.execute { "EXECUTE" } else { "DRY RUN" },
        args.max_groups
    );
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
                record("tenant-dsn", run_one("tenant-dsn", &pool, &args).await);
                pool.close().await;
            }
            Err(e) => {
                eprintln!("failed to connect: {e}");
                return ExitCode::FAILURE;
            }
        },
        _ => {
            let Some(url) = migrator_database_url() else {
                eprintln!("MIGRATOR_DATABASE_URL (preferred) or DATABASE_URL must be set for --control/--only/--all-tenants");
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
                    record("control", run_one("control", &control, &args).await);
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
                            return ExitCode::from(2);
                        }
                        Err(e) => {
                            eprintln!("routing lookup failed: {e}");
                            return ExitCode::FAILURE;
                        }
                    }
                }
                _ => match routing::list_active(&control).await {
                    Ok(rows) => rows,
                    Err(e) => {
                        eprintln!("failed to enumerate active tenants: {e}");
                        return ExitCode::FAILURE;
                    }
                },
            };
            let resolver = OperatorDsnResolver::from_env_or_env_only(control.clone(), "dedupe_reefs");
            for t in &tenants {
                let label = format!("tenant:{}", t.slug);
                match connect_tenant(&resolver, t).await {
                    Ok(pool) => {
                        record(&label, run_one(&label, &pool, &args).await);
                        pool.close().await;
                    }
                    Err(e) => record(&label, Err((1, e))),
                }
            }
            control.close().await;
        }
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
    fn dry_run_default_and_execute_needs_an_operator() {
        let a = parse(&["--only", "coralswarm"]).unwrap();
        assert!(!a.execute);
        assert_eq!(a.policy.entity_types, reef_dedupe::DEFAULT_ENTITY_TYPES.iter().map(|s| s.to_string()).collect::<Vec<_>>());
        assert!(parse(&["--only", "coralswarm", "--execute"]).is_err());
        let u = Uuid::new_v4().to_string();
        let a = parse(&["--only", "c", "--execute", "--created-by", &u, "--max-groups", "5", "--entity-types", "person,org"]).unwrap();
        assert_eq!((a.execute, a.max_groups), (true, Some(5)));
        assert_eq!(a.policy.entity_types, vec!["person".to_string(), "org".to_string()]);
    }

    #[test]
    fn same_title_rule_folds_every_non_person_kind_and_refuses_person() {
        let a = parse(&["--only", "coralswarm", "--rule", "same-title"]).unwrap();
        assert!(!a.execute, "dry run by default");
        assert_eq!(a.policy.rule, reef_dedupe::Rule::SameTitle);
        assert_eq!(a.policy.entity_types, vec!["org".to_string(), "project".to_string(), "topic".to_string()]);
        assert!(a.policy.include_preexisting);
        assert_eq!(a.policy.cutoff, reef_dedupe::SAME_TITLE_CUTOFF.parse::<chrono::DateTime<chrono::Utc>>().unwrap());
        let g = Uuid::new_v4().to_string();
        let a = parse(&["--graph-id", &g, "--rule", "same-title", "--control", "--entity-types", "topic"]).unwrap();
        assert_eq!((a.policy.graph, a.policy.entity_types.clone()), (Some(Uuid::parse_str(&g).unwrap()), vec!["topic".to_string()]));
        assert!(parse(&["--only", "c", "--rule", "same-title", "--entity-types", "org,person"]).is_err());
        assert!(parse(&["--only", "c", "--rule", "everything"]).is_err());
        assert_eq!(parse(&["--only", "c"]).unwrap().policy.rule, reef_dedupe::Rule::NamedEntities);
    }

    #[test]
    fn selector_is_required_and_exclusive() {
        assert!(parse(&[]).is_err());
        assert!(parse(&["--control", "--all-tenants"]).is_err());
        assert!(parse(&["--control", "--max-groups", "0"]).is_err());
    }
}
