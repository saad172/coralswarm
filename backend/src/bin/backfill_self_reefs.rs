//! `backfill_self_reefs` — the one-time backfill named in
//! `docs/plans/identity-resolution-at-the-door.md` §4.2/§10.3: give every
//! live ocean without one a self reef, bound to its owner's identifiers, so
//! the identity-resolution door's step 2 (`reef_identifiers`) is decisive
//! for the owner from that point on. Design doc evidence: as of 2026-09-06,
//! 8 live oceans had no self reef at all.
//!
//! **This binary is a thin wrapper.** Every gate, counter and write lives in
//! [`coralswarm_backend::self_reef_backfill`] — READ THAT MODULE'S DOC
//! first. This file only parses arguments, resolves the DSN, and reports.
//! That split is deliberate: it is what lets
//! `backend/tests/identity_door_pg.rs` drive the SAME code an operator runs
//! instead of a re-implementation of it.
//!
//! ## A SECOND target, added after the original 8-ocean sweep
//!
//! A graph that already HAS a self reef — e.g. the primary ocean `93cc446a`,
//! whose self reef "Alex Example" was created by Slack ingest before
//! identifiers existed — never got `clerk_user`/`email` rows in
//! `reef_identifiers`, because the original selection was `self_node_id IS
//! NULL` only. This binary now ALSO selects live graphs whose self reef is
//! missing a `clerk_user` identifier, and binds identifiers onto that
//! EXISTING self reef without creating or retitling anything. See
//! `self_reef_backfill`'s module doc for the email collision policy that
//! case needs (an owner's email can already be held by a different,
//! pre-door "owner fragment" reef).
//!
//! ## RUN ONCE PER DATABASE (there is no cross-database default here)
//!
//! Unlike some other backfills in this crate, this binary does NOT enumerate
//! tenant databases itself — it operates on whichever single DSN it is
//! pointed at. The consumer/control plane and each org's own tenant database
//! are separate Postgres instances (see
//! `docs/coralswarm-database-architecture`'s decisions); `graphs`/`nodes`/
//! `reef_identifiers` live in EVERY one of them, but `users` (needed to
//! resolve an owner's name/email/Clerk id) lives ONLY on the consumer/
//! control plane. Run this binary once against `DATABASE_URL`/
//! `MIGRATOR_DATABASE_URL` for the control plane, then once per org tenant
//! DSN — an invocation against a tenant-only database (no `users` table)
//! still runs, just with nothing to bind (see "No `users` table" below); it
//! does not error out or need a second connection.
//!
//! ## Usage
//!
//!   backfill_self_reefs [--execute] [--graph-id <uuid>]
//!
//! `--dry-run` (the default) performs every lookup and prints exactly what
//! WOULD be created/bound for every eligible graph, without writing
//! anything. `--execute` applies it. Idempotent and safe to re-run: a graph
//! is not selected at all once it has a self reef carrying `clerk_user`, and
//! every identifier write is `ON CONFLICT DO NOTHING`.
//!
//! `--graph-id <uuid>` narrows to one ocean — a rehearsal/diagnosis aid.
//!
//! ## Selection
//!
//! Live (`archived_at IS NULL`) graphs of `kind IN ('personal', 'member')`
//! (the two kinds design doc §10.3 calls "ocean provisioning" targets — a
//! single account/member owns each one; `kind = 'org'`, the shared commons
//! ocean, is deliberately excluded, same as `clerk_provision::
//! create_first_ocean_scoped`'s own live provisioning path) that are EITHER
//! self-reef-less OR already have a self reef with no `clerk_user`
//! identifier. See `self_reef_backfill::list_eligible_graphs`.
//!
//! ## Title resolution (design doc §10.3) — NoSelfReef case only
//!
//! `users.name` → the highest-mass same-graph person reef already carrying
//! an `email` identifier matching the owner → the email's local part
//! (flagged `body.needs_name = true`). Never the raw email. The
//! ExistingSelfReef case never resolves or changes a title at all.
//!
//! ## No `users` table (a pure tenant database)
//!
//! Detected once via `to_regclass('public.users')`. When absent, a
//! NoSelfReef graph falls back to the literal title `"Owner"` with
//! `body.needs_name = true` and no identifiers bound (there is nothing
//! local to bind from); an ExistingSelfReef graph gets nothing bound at
//! all. Both are reported as a distinct `no_users_table` count so an
//! operator can tell "ran against a tenant DB, as expected" apart from
//! "found nothing to do".
//!
//! ## Exit codes
//!
//!   0  every selected graph processed with zero per-graph errors
//!   1  at least one per-graph error (read the per-graph lines; re-run —
//!      idempotent)
//!   2  usage/argument error, or no DSN in the environment

use std::process::ExitCode;

use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

use coralswarm_backend::migrator_database_url;
use coralswarm_backend::self_reef_backfill::{
    list_eligible_graphs, process_graph, users_table_present, Totals,
};

const USAGE: &str = "usage: backfill_self_reefs [--execute] [--graph-id <uuid>]";

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
                graph_id =
                    Some(Uuid::parse_str(&v).map_err(|e| format!("--graph-id: invalid UUID: {e}"))?);
            }
            "-h" | "--help" => {
                println!("{USAGE}");
                println!();
                println!("Runs once per tenant DB — see this binary's module doc for why.");
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
    // Preflight: refuse a role that RLS would silently filter (see the fn doc).
    if let Err(msg) = coralswarm_backend::self_reef_backfill::assert_policy_bypass(&pool).await {
        eprintln!("{msg}");
        return ExitCode::from(3);
    }

    println!(
        "backfill_self_reefs — mode={} (runs once per tenant DB — see module doc)",
        if args.execute { "EXECUTE" } else { "DRY RUN" }
    );
    if let Some(id) = args.graph_id {
        println!("  narrowed to graph {id}");
    }

    let users_present = users_table_present(&pool).await;
    println!("  users table present on this connection: {users_present}");

    let graphs = match list_eligible_graphs(&pool, args.graph_id).await {
        Ok(g) => g,
        Err(e) => {
            eprintln!("failed to enumerate graphs: {e}");
            return ExitCode::FAILURE;
        }
    };
    if graphs.is_empty() {
        // Nothing to do is the expected steady state after one successful
        // `--execute`, but it must still be visible to whatever an operator
        // watches (scripts/check_silent_drop_is_alertable.sh): a run that
        // finds zero eligible graphs on a database that is KNOWN to hold
        // owner fragments means the selection query is wrong, not that the
        // work is done. `connector_health = true` is the field that guard
        // (and the ops dashboards) key on.
        tracing::warn!(
            connector_health = true,
            filter_graph = ?args.graph_id,
            "backfill_self_reefs: no eligible graphs found (every live personal/member \
             ocean already has a self reef with a bound clerk_user identifier)"
        );
        println!(
            "no eligible graphs found (every live personal/member ocean already has a self reef \
             with a bound clerk_user identifier)"
        );
    }

    let mut totals = Totals::default();
    for graph in &graphs {
        let report = process_graph(&pool, graph, users_present, args.execute, &mut totals).await;
        println!("{report}");
    }

    println!("{totals}");
    if !args.execute {
        println!("Nothing was written. Read the lines above, then re-run with --execute to apply.");
    }

    if totals.errors > 0 {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dry_run_is_the_default() {
        let args = parse_args(std::iter::empty()).expect("no args is valid");
        assert!(!args.execute);
        assert_eq!(args.graph_id, None);
    }

    #[test]
    fn execute_flag_flips_mode() {
        let args = parse_args(["--execute".to_string()].into_iter()).expect("valid");
        assert!(args.execute);
    }

    #[test]
    fn graph_id_must_be_a_uuid() {
        let err = parse_args(
            ["--graph-id".to_string(), "not-a-uuid".to_string()].into_iter(),
        )
        .unwrap_err();
        assert!(err.contains("invalid UUID"), "{err}");
    }

    #[test]
    fn unknown_flag_is_rejected() {
        let err = parse_args(["--bogus".to_string()].into_iter()).unwrap_err();
        assert!(err.contains("unknown argument"), "{err}");
    }
}
