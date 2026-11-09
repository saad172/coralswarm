//! `backfill_promoted_blobs` — one-shot backfill for the promote-content-
//! loss bug: `Store::copy_atoms_to_graph`/`_scoped` used to build a promoted
//! coral's `content_blob_key` inline in SQL and never wrote an R2 object at
//! that key. See [`coralswarm_backend::promoted_blob_backfill`]'s module doc
//! for the full fix this backfills, the exact candidate-selection SQL, and
//! why the org id comes from `tenant_meta.org_id`, never a CLI guess.
//!
//! **This binary is a thin wrapper.** Every gate, query and write lives in
//! [`coralswarm_backend::promoted_blob_backfill`] — READ THAT MODULE'S DOC
//! first. This file only parses arguments, resolves the DSN/R2 client, and
//! reports. That split is deliberate: it is what lets
//! `backend/tests/promoted_blob_backfill_pg.rs` drive the SAME code an
//! operator runs instead of a re-implementation of it.
//!
//! ## RUN ONCE PER DATABASE
//!
//! This binary does not enumerate tenant databases itself — it operates on
//! whichever single DSN it is pointed at. Run it once against the consumer/
//! control-plane DSN, then once per org's own tenant DSN.
//!
//! **Prod sizing — every count below is AS OF a date and moves each time
//! this runs. See `promoted_blob_backfill`'s own module doc, "Prod sizing"
//! section, for the current numbers and the per-graph breakdown; the
//! headline as of 2026-09-10 is that tenant `coralswarm`'s remaining work
//! is 1,191 BLANK-key rows (`CANDIDATES_SQL` arm 2 — the rows the
//! 2026-09-09 run could not see, because `workers::atom_title_backfill`
//! blanked the very key arm 1 selects on), all of them repairable.**
//!   - Tenant `testorg` (2026-09-09): 1 candidate, inline-only
//!     source-missing — expect 1 `skipped_inline_only`, 0 `put_ok`, exit 0.
//!   - Consumer plane (2026-09-09): 1,721 candidates — expect 405 `put_ok`
//!     + 1,316 `skipped_inline_only`, exit 0.
//!
//! ## Usage
//!
//!   backfill_promoted_blobs [--apply] [--graph-id <uuid>] [--org-id <clerk_org_id>]
//!
//! `--dry-run` (the default) performs every READ — the source atom's
//! `get_atom_blob`, so a dry run proves the content is actually fetchable,
//! AND a GET at the CORRECT target key, so a dry run also proves the
//! target genuinely needs fixing (not merely that the candidate query
//! matched a row — see `promoted_blob_backfill::process_candidate`'s doc
//! comment for why that check exists) — and prints exactly what WOULD be
//! re-uploaded/corrected/re-enqueued, without writing anything (no R2
//! `put`, no `UPDATE`, no job enqueue). `--apply` performs the writes.
//! Idempotent and safe to re-run (see the module doc's "Idempotent and safe
//! to re-run" section).
//!
//! `--graph-id <uuid>` narrows to one target ocean — a rehearsal/diagnosis
//! aid, same convention as `backfill_self_reefs`.
//!
//! `--org-id <clerk_org_id>` is a pure SANITY CHECK against this database's
//! own `tenant_meta.org_id` fingerprint (or its absence, on the consumer
//! plane) — never trusted over what the database itself says. A mismatch
//! refuses before any work starts.
//!
//! ## Concurrency
//!
//! Each batch's candidates run up to N at a time, where N defaults to
//! `promoted_blob_backfill::BLOB_IO_CONCURRENCY` (8) and can be overridden
//! per run with `BACKFILL_BLOB_IO_CONCURRENCY` (see "Env" below). The
//! connection pool is sized `N * 2 + 4` from the SAME resolved N — never
//! from the const — with a real acquire timeout, because the 2026-09-09 dry
//! run hit "pool timed out while waiting for an open connection" through a
//! real fly-proxy connection even running strictly serially (one
//! candidate's source lookup + DEK resolution + existence check + UPDATE
//! can each want a connection in quick succession). Every transport/DB call
//! in `process_candidate` is retried once before counting as a real error —
//! see `promoted_blob_backfill::RETRY_BACKOFF`.
//!
//! **Why the override exists (measured, prod, 2026-09-09 `--apply`):** at
//! concurrency 8 the run managed only ~5.3 s/row, with R2 GET 30-second
//! timeouts firing, and once exited 4 on a transient DNS failure; at
//! concurrency 1 the same work ran cleanly at ~1 s/row. Re-measuring DNS
//! and R2 latency found both healthy, so the suspicion is the 8 concurrent
//! `aws-sdk` connections themselves (the same shape as this repo's known
//! dead-pooled-h2-socket issue), not the network. Until that is understood,
//! an operator with thousands of rows to repair needs to be able to turn
//! this down without a code change and a redeploy. `R2Client` itself still
//! carries no per-operation timeout or retry policy of its own (this
//! module's `BLOB_IO_TIMEOUT`/`RETRY_BACKOFF` wrap it from outside) — a
//! real, separate follow-up, deliberately not changed here.
//!
//! ## Env
//!
//!   MIGRATOR_DATABASE_URL / DATABASE_URL   this database's DSN (elevated —
//!                                          see the preflight RLS-bypass
//!                                          check below)
//!   R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET_NAME
//!                                          R2 credentials (same names the
//!                                          app itself reads at boot)
//!   Master KEK env vars — same as every other DEK-resolving binary in this
//!   crate (`coralswarm_backend::crypto::load_master_keks`), needed to
//!   unwrap each account's DEK.
//!   BACKFILL_BLOB_IO_CONCURRENCY          optional; how many candidates'
//!                                         blob I/O run at once, and (times
//!                                         2, plus 4) how big the connection
//!                                         pool is. Defaults to
//!                                         `promoted_blob_backfill::
//!                                         BLOB_IO_CONCURRENCY` (8);
//!                                         surrounding whitespace is
//!                                         trimmed; `0` is clamped UP to 1
//!                                         and anything above
//!                                         `MAX_CONCURRENCY` is clamped
//!                                         DOWN to it, both with a printed
//!                                         note; anything unparseable is a
//!                                         configuration error (exit 2), never
//!                                         a silent fall back to the default.
//!                                         The resolved value is printed at
//!                                         startup, so a run's own output
//!                                         records what it actually used.
//!
//! ## Exit codes
//!
//!   0  `errors == 0 && put_failed == 0` — every selected candidate was
//!      either repaired, or correctly and permanently classified as not
//!      needing/not able to be repaired (`skipped*`,
//!      `unrepairable_source_missing`, `source_not_found` never affect
//!      this). See `promoted_blob_backfill`'s own module doc, "Exit code"
//!      section, for the full reasoning — a nonzero
//!      `unrepairable_source_missing` is worth reading the printed atom
//!      ids for, but is NOT a reason to expect a different exit code or a
//!      different result from re-running.
//!   1  `errors > 0` (transient transport/DB failures that survived a
//!      retry, or an ambiguous source — re-run to retry them) or
//!      `put_failed > 0` (a real write-path failure — read the per-row
//!      lines; re-run — idempotent)
//!   2  usage/argument/configuration error (including a `--org-id` mismatch)
//!   3  preflight refused: could not connect to the DSN at all, RLS would
//!      silently filter this role, or R2 is not configured. Nothing was
//!      read or written — safe to fix the config and re-run exactly as
//!      before.
//!   4  aborted traversal: the connection was live and at least one batch
//!      may already have been processed (see the totals/report lines
//!      printed before this point) but a later `list_candidates` query
//!      failed mid-run. Idempotent and safe to re-run — already-applied
//!      writes are not undone and will not be re-attempted.

use std::process::ExitCode;
use std::time::Duration;

use coralswarm_backend::migrator_database_url;
use coralswarm_backend::promoted_blob_backfill::{
    assert_policy_bypass, list_candidates, process_candidate, resolve_org_id, Totals,
    BLOB_IO_CONCURRENCY,
};
use coralswarm_backend::tenancy::TenantId;
use coralswarm_core::r2::R2Client;
use futures::StreamExt;
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

const USAGE: &str = "usage: backfill_promoted_blobs [--apply] [--graph-id <uuid>] [--org-id <clerk_org_id>]";

/// Env var that overrides [`BLOB_IO_CONCURRENCY`] for ONE run — see this
/// binary's "Concurrency" doc section for the prod measurement that made a
/// runtime knob necessary (concurrency 8: ~5.3 s/row with R2 GET timeouts
/// firing; concurrency 1: ~1 s/row clean).
const CONCURRENCY_ENV: &str = "BACKFILL_BLOB_IO_CONCURRENCY";

/// Ceiling on the resolved concurrency. N is really a request for `N * 2 + 4`
/// server-side Postgres connections (see the pool sizing in `main`), so past
/// this a run fails at `connect` — or starves everything else on that
/// database — long before it goes any faster. It also bounds the pool-size
/// arithmetic: an unbounded `usize` from the environment made
/// `concurrency as u32 * 2 + 4` a truncation-and-overflow hazard (CodeRabbit
/// review, PR #868).
///
/// Clamped DOWN loudly rather than refused, unlike an unparseable value:
/// "bigger than the ceiling" has exactly one sensible reading ("as parallel
/// as you can"), where `"eight"` has none — and the startup line prints what
/// was actually used either way.
const MAX_CONCURRENCY: usize = 64;

/// Resolve this run's blob-I/O concurrency from `raw` (the raw
/// [`CONCURRENCY_ENV`] value, `None` when unset), returning the value and a
/// one-line note describing how it was resolved — printed at startup so a
/// run's own output records what it actually used, rather than leaving a
/// reader to guess whether the env var was picked up.
///
/// Three deliberate behaviours:
/// * unset or empty -> the [`BLOB_IO_CONCURRENCY`] default, unchanged, so
///   nothing that does not set the var behaves differently than before;
/// * `0` -> clamped UP to 1, with a note (`buffer_unordered(0)` would make
///   no progress at all, and "the operator meant serial" is the only
///   sensible reading); anything above [`MAX_CONCURRENCY`] -> clamped DOWN
///   to it, also with a note (see that const for why a ceiling exists);
/// * anything unparseable (`"eight"`, `"-1"`, `"3.5"`… ) -> `Err`, which
///   `main` turns into exit 2. NEVER a silent fall back to the default: a
///   typo'd override that quietly ran at 8 anyway is exactly the failure
///   this knob exists to prevent, and it would be invisible in the output.
///   Surrounding whitespace is NOT a typo — `raw` is trimmed first, because
///   a stray space is a shell/env-file artifact, not an intent.
fn resolve_concurrency(raw: Option<&str>) -> Result<(usize, String), String> {
    let Some(raw) = raw.map(str::trim).filter(|s| !s.is_empty()) else {
        return Ok((
            BLOB_IO_CONCURRENCY,
            format!("{BLOB_IO_CONCURRENCY} (default; set {CONCURRENCY_ENV} to override)"),
        ));
    };
    let parsed: usize = raw.parse().map_err(|_| {
        format!(
            "{CONCURRENCY_ENV}={raw:?} is not a non-negative integer — refusing to guess \
             (unset it to use the default of {BLOB_IO_CONCURRENCY})"
        )
    })?;
    if parsed == 0 {
        return Ok((
            1,
            format!("1 (clamped up from {CONCURRENCY_ENV}=0 — serial; 0 would never progress)"),
        ));
    }
    if parsed > MAX_CONCURRENCY {
        return Ok((
            MAX_CONCURRENCY,
            format!(
                "{MAX_CONCURRENCY} (clamped DOWN from {CONCURRENCY_ENV}={parsed} — the ceiling; \
                 the pool would need {} connections, see MAX_CONCURRENCY)",
                MAX_CONCURRENCY * 2 + 4
            ),
        ));
    }
    Ok((parsed, format!("{parsed} (from {CONCURRENCY_ENV})")))
}

#[derive(Debug)]
struct Args {
    apply: bool,
    graph_id: Option<Uuid>,
    org_id: Option<String>,
}

fn parse_args(args: impl Iterator<Item = String>) -> Result<Args, String> {
    let mut apply = false;
    let mut graph_id = None;
    let mut org_id = None;
    let mut args = args;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--apply" => apply = true,
            "--dry-run" => apply = false,
            "--graph-id" => {
                let v = args.next().ok_or("--graph-id requires a value")?;
                graph_id =
                    Some(Uuid::parse_str(&v).map_err(|e| format!("--graph-id: invalid UUID: {e}"))?);
            }
            "--org-id" => {
                let v = args.next().ok_or("--org-id requires a value")?;
                org_id = Some(v);
            }
            "-h" | "--help" => {
                println!("{USAGE}");
                println!();
                println!("Runs once per database — see this binary's module doc for why.");
                std::process::exit(0);
            }
            other => return Err(format!("unknown argument: {other}")),
        }
    }
    Ok(Args { apply, graph_id, org_id })
}

/// How many atoms in `team`/`org` graphs have NO usable content AT ALL — a
/// blank `content_blob_key` *and* no `content_inline`. Cheap corroboration
/// for the zero-candidate path below, and deliberately independent of
/// `CANDIDATES_SQL` (no promotion-provenance join, no key-shape test): `0`
/// is consistent with a genuinely clean database, while anything else says
/// the candidate query did not reach rows that are demonstrably unreadable
/// right now — which is a bug report about this tool, not a clean bill of
/// health.
///
/// What it does NOT cover: a row whose `content_blob_key` is present and
/// well-formed but names an object that is missing from R2. Only the
/// per-candidate GETs can see those, so a `0` here still is not proof that
/// every promoted coral is readable — see the printed wording, which says so.
///
/// Returns `None` if the count itself fails. It runs after all the work is
/// done, purely to describe what was measured, and must never change the
/// exit code.
async fn count_contentless_promotable_atoms(pool: &sqlx::PgPool) -> Option<i64> {
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM atoms a \
         JOIN graphs g ON g.id = a.graph_id \
         WHERE g.kind IN ('team', 'org') \
           AND COALESCE(a.content_blob_key, '') = '' \
           AND COALESCE(a.content_inline, '') = ''",
    )
    .fetch_one(pool)
    .await
    .map_err(|e| {
        tracing::warn!(error = %e, "backfill_promoted_blobs: contentless-atom count failed");
        e
    })
    .ok()
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
    // Resolved ONCE, here, and threaded to BOTH places that need it (the
    // pool size immediately below and the `buffer_unordered` in the main
    // loop). Reading the const in one place and the override in the other
    // would mis-size the pool for every non-default run.
    let (concurrency, concurrency_note) =
        match resolve_concurrency(std::env::var(CONCURRENCY_ENV).ok().as_deref()) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("{e}");
                return ExitCode::from(2);
            }
        };
    // `concurrency * 2 + 4`: enough headroom for every candidate in a
    // concurrently-processed batch to want a connection for its source
    // lookup AND (once it gets that far) its existence-check/UPDATE at
    // once, without exhausting the pool — see this binary's own
    // "Concurrency" doc section for the real fly-proxy "pool timed out"
    // this sizing (plus a real acquire timeout, rather than the default's
    // effectively-unbounded wait) fixes.
    // Non-panicking by construction: `resolve_concurrency` clamps to
    // `MAX_CONCURRENCY`, so `N * 2 + 4` cannot overflow today — the
    // saturating form keeps that true if the ceiling is ever loosened, where
    // the previous `concurrency as u32 * 2 + 4` would silently truncate an
    // over-u32 value and could overflow (CodeRabbit review, PR #868).
    let max_connections =
        u32::try_from(concurrency).unwrap_or(u32::MAX).saturating_mul(2).saturating_add(4);
    let pool = match PgPoolOptions::new()
        .max_connections(max_connections)
        .acquire_timeout(Duration::from_secs(30))
        .connect(&dsn)
        .await
    {
        Ok(p) => p,
        Err(e) => {
            // Code 3 (preflight refused), not 1 (per-row errors) — nothing
            // was read or written yet. See this file's "Exit codes" doc
            // section.
            eprintln!("failed to connect: {e}");
            return ExitCode::from(3);
        }
    };

    // Preflight 1: refuse a role RLS would silently filter.
    if let Err(msg) = assert_policy_bypass(&pool).await {
        eprintln!("{msg}");
        return ExitCode::from(3);
    }

    // Preflight 2: resolve (and sanity-check) the org id this database
    // itself claims — never a CLI guess (see the module doc).
    let org_id = match resolve_org_id(&pool, args.org_id.as_deref()).await {
        Ok(o) => o,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::from(2);
        }
    };
    let tenant = match &org_id {
        Some(o) => TenantId::Org(o.clone()),
        None => TenantId::Consumer,
    };

    // Preflight 3: R2 credentials. Required even for a dry run — a dry run
    // still performs the `get_atom_blob` read (see the module doc's
    // `--dry-run` section), which is what proves the source content is
    // actually fetchable before anything is written.
    let r2 = match (
        std::env::var("R2_ACCOUNT_ID"),
        std::env::var("R2_ACCESS_KEY_ID"),
        std::env::var("R2_SECRET_ACCESS_KEY"),
        std::env::var("R2_BUCKET_NAME"),
    ) {
        (Ok(account_id), Ok(access_key_id), Ok(secret_access_key), Ok(bucket)) => {
            R2Client::new(&account_id, &access_key_id, &secret_access_key, &bucket).await
        }
        _ => {
            eprintln!(
                "R2_ACCOUNT_ID / R2_ACCESS_KEY_ID / R2_SECRET_ACCESS_KEY / R2_BUCKET_NAME must \
                 all be set"
            );
            return ExitCode::from(3);
        }
    };

    println!(
        "backfill_promoted_blobs — mode={} org_id={} (runs once per database — see module doc)",
        if args.apply { "APPLY" } else { "DRY RUN" },
        org_id.as_deref().unwrap_or("<consumer/control plane>")
    );
    println!("  blob I/O concurrency = {concurrency_note}; pool max_connections={max_connections}");
    if let Some(id) = args.graph_id {
        println!("  narrowed to graph {id}");
    }

    let mut totals = Totals::default();
    // Shared across every concurrently-processed candidate — see
    // `promoted_blob_backfill`'s "Concurrency" doc section for why this is
    // an async `Mutex` rather than a plain `&mut HashMap`.
    let dek_cache = tokio::sync::Mutex::new(std::collections::HashMap::new());
    // Every atom_id ATTEMPTED this run (success or failure) — see
    // `CANDIDATES_SQL`'s doc comment for why this, not OFFSET, is what
    // guarantees this loop terminates: a consumer-plane fix does not
    // change its key's TEXT, so a re-query with no exclusion would select
    // an already-fixed row forever. Extended only AFTER a whole batch's
    // concurrent futures have all completed (never while any are still in
    // flight), same as `totals`.
    let mut attempted: Vec<Uuid> = Vec::new();
    loop {
        let batch = match list_candidates(&pool, args.graph_id, &attempted).await {
            Ok(b) => b,
            Err(e) => {
                // Code 4 (aborted traversal), not 1 (per-row errors) — see
                // this file's "Exit codes" doc section. `totals`/the report
                // lines already printed above reflect every candidate
                // actually attempted before this failure.
                eprintln!("candidate query failed: {e}");
                eprintln!("{totals}");
                return ExitCode::from(4);
            }
        };
        if batch.is_empty() {
            break;
        }

        // Up to `concurrency` candidates in flight at once (the resolved
        // runtime value, NOT the const — see `resolve_concurrency`) — each
        // future borrows `pool`/`r2`/`org_id`/`tenant`/`dek_cache`, never
        // moves or spawns them, so this stays within the loop's own scope
        // (no `'static` requirement). One candidate's failure — transient
        // or not — never aborts any other's future in the batch; they run
        // to completion independently and are collected below.
        let outcomes = futures::stream::iter(batch.iter())
            .map(|cand| {
                process_candidate(&pool, &r2, org_id.as_deref(), &tenant, &dek_cache, cand, args.apply)
            })
            .buffer_unordered(concurrency)
            .collect::<Vec<_>>()
            .await;

        // Tally and print AFTER the whole batch's concurrent I/O has
        // finished — the single-threaded aggregation point that makes
        // mutating `totals`/`attempted` here safe with no locking of its
        // own.
        for outcome in outcomes {
            println!("{}", outcome.report);
            totals.apply(&outcome);
            attempted.push(outcome.atom_id);
        }
    }

    if totals.candidates == 0 {
        // Report ONLY what was measured. The previous wording here —
        // "every promoted coral in this database already has real R2
        // content, or none was ever promoted" — stated two things this tool
        // never establishes, and on 2026-09-10 it stated them about a
        // database holding 1,191 unreadable corals: the query's only arm at
        // the time keyed on a marker another worker had already erased (see
        // `promoted_blob_backfill::CANDIDATES_SQL`, arm 2). An operator
        // re-running the repair to check was told, in so many words, that
        // the database was clean. Exit 0 is still right for "nothing to
        // do"; claiming a clean bill of health is not.
        let contentless = count_contentless_promotable_atoms(&pool).await;
        tracing::warn!(
            connector_health = true,
            filter_graph = ?args.graph_id,
            contentless_team_org_atoms = ?contentless,
            "backfill_promoted_blobs: candidate query matched no rows"
        );
        println!(
            "the candidate query matched no rows. That is NOT evidence that every promoted coral \
             here is readable — only that neither arm of CANDIDATES_SQL selected anything (see \
             `promoted_blob_backfill::CANDIDATES_SQL`)."
        );
        match contentless {
            Some(0) => println!(
                "  corroboration: 0 atom(s) in team/org graphs have a blank content_blob_key and \
                 no content_inline — consistent with a genuinely clean database. Still not proof: \
                 a row whose key names a MISSING R2 object is invisible to that count, and only \
                 the per-candidate GETs can see those."
            ),
            Some(n) => println!(
                "  WARNING: {n} atom(s) in team/org graphs have a blank content_blob_key AND no \
                 content_inline — they are unreadable RIGHT NOW, and this run selected none of \
                 them. Do NOT read this as a clean database; this is a defect in candidate \
                 selection (or a narrowing --graph-id) and is worth reporting."
            ),
            None => println!(
                "  (the corroborating count of contentless team/org atoms could not be run — see \
                 the logged warning; treat this run as saying nothing about whether the database \
                 is clean)"
            ),
        }
    }

    println!("{totals}");
    println!("{}", totals.summary_line());
    if !args.apply {
        println!("Nothing was written. Read the lines above, then re-run with --apply to fix it.");
    }

    if totals.is_clean() {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dry_run_is_the_default() {
        let args = parse_args(std::iter::empty()).expect("no args is valid");
        assert!(!args.apply);
        assert_eq!(args.graph_id, None);
        assert_eq!(args.org_id, None);
    }

    #[test]
    fn apply_flag_flips_mode() {
        let args = parse_args(["--apply".to_string()].into_iter()).expect("valid");
        assert!(args.apply);
    }

    #[test]
    fn graph_id_must_be_a_uuid() {
        let err =
            parse_args(["--graph-id".to_string(), "not-a-uuid".to_string()].into_iter()).unwrap_err();
        assert!(err.contains("invalid UUID"), "{err}");
    }

    #[test]
    fn org_id_is_captured() {
        let args = parse_args(
            ["--org-id".to_string(), "org_abc123".to_string()].into_iter(),
        )
        .expect("valid");
        assert_eq!(args.org_id.as_deref(), Some("org_abc123"));
    }

    #[test]
    fn unknown_flag_is_rejected() {
        let err = parse_args(["--bogus".to_string()].into_iter()).unwrap_err();
        assert!(err.contains("unknown argument"), "{err}");
    }

    // ── BACKFILL_BLOB_IO_CONCURRENCY ────────────────────────────────────
    // `resolve_concurrency` takes the raw value as an argument rather than
    // reading the environment itself, so these tests never touch the
    // process environment (no `set_var`, no cross-test interference, no
    // `--test-threads=1` requirement).

    #[test]
    fn concurrency_defaults_to_the_const_when_unset_or_empty() {
        for raw in [None, Some(""), Some("   ")] {
            let (value, note) = resolve_concurrency(raw).expect("default is always resolvable");
            assert_eq!(value, BLOB_IO_CONCURRENCY, "raw = {raw:?}");
            assert!(note.contains("default"), "the note must say it is the default: {note}");
        }
    }

    #[test]
    fn concurrency_override_is_honoured_and_reported() {
        let (value, note) = resolve_concurrency(Some("1")).expect("1 is valid");
        assert_eq!(value, 1);
        assert!(note.contains(CONCURRENCY_ENV), "{note}");
        // Surrounding whitespace is a shell/`env`-file artifact, not a typo.
        let (value, _) = resolve_concurrency(Some(" 3 ")).expect("padded value is valid");
        assert_eq!(value, 3);
    }

    #[test]
    fn concurrency_above_the_ceiling_is_clamped_down_loudly() {
        let (value, note) =
            resolve_concurrency(Some("100000")).expect("too large is clamped, not rejected");
        assert_eq!(value, MAX_CONCURRENCY);
        assert!(note.contains("clamped DOWN"), "the clamp must be visible: {note}");
        // The point of the ceiling: `main`'s pool sizing (`N * 2 + 4`, as a
        // u32) stays well inside u32 for every value this can return.
        assert!(
            u32::try_from(value)
                .ok()
                .and_then(|v| v.checked_mul(2))
                .and_then(|v| v.checked_add(4))
                .is_some(),
            "a resolved concurrency must never be able to overflow the pool size"
        );
    }

    #[test]
    fn concurrency_zero_is_clamped_up_to_one_loudly() {
        let (value, note) = resolve_concurrency(Some("0")).expect("0 is clamped, not rejected");
        assert_eq!(value, 1, "buffer_unordered(0) would never make progress");
        assert!(note.contains("clamped"), "the clamp must be visible in the output: {note}");
    }

    #[test]
    fn concurrency_garbage_is_rejected_never_silently_defaulted() {
        for raw in ["eight", "-1", "3.5", "8x"] {
            let err = resolve_concurrency(Some(raw))
                .expect_err("an unparseable override must not fall back to the default");
            assert!(err.contains(CONCURRENCY_ENV), "{err}");
            assert!(
                err.contains("refusing to guess"),
                "the refusal must say why, so a typo is obvious: {err}"
            );
        }
    }
}
