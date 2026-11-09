//! `merge_reefs` — operator CLI for the one-time prod repair named in
//! `docs/plans/identity-resolution-at-the-door.md` §4.2 ("Prod repair") and
//! §7 decision 3: a human-source POSITIVE `reef_judgement` merging a
//! duplicate reef into its survivor, for the two fragments the design's own
//! evidence names (`Alex` -> `Alex Example` in `93cc446a`, and the five
//! owner fragments in `4849b58b` once that graph has a self reef).
//!
//! Today the human merge exists only behind the authenticated HTTP route
//! (`backend/src/routes/nodes.rs`'s `write_human_positive_and_merge` /
//! `merge_node`, roughly line 4745/4795) and the web UI's one-at-a-time
//! merge dropdown. Neither is reachable from the local proof harness
//! (`scripts/identity_door_local/`) or from an operator scripting a batch
//! repair against prod. This binary reproduces that route's exact write
//! sequence — a `reef_judgement` row, then `Store::merge_nodes_with_judgement`
//! — without going through Axum/Clerk/RLS, so it can be driven by a script.
//!
//! Per this repo's `10.1` file-ownership contract, `routes/nodes.rs` is not
//! edited or made callable from a bin: `write_human_positive_and_merge`'s
//! logic (route line ~4745) is copied here in miniature — see
//! `write_human_positive_and_merge_cli` below — rather than reused, because
//! it is a private, route-module fn that takes an Axum-side `ScopedTx`
//! wrapper this binary has no reason to construct.
//!
//! ## RUN ONCE PER TENANT DB, PER PAIR
//!
//! There is no cross-database default here (same discipline as
//! `backfill_self_reefs`): point `DATABASE_URL`/`MIGRATOR_DATABASE_URL` at
//! one tenant database and merge one pair at a time. A batch repair script
//! (`scripts/identity_door_local/25_ops_repair.sh`) invokes this once per
//! fragment.
//!
//! ## Usage
//!
//! ```text
//! merge_reefs [--execute] --graph <uuid> --keep <uuid> --dup <uuid> \
//!     --created-by <uuid> [--reason "<text>"]
//! ```
//!
//! `--dry-run` (the default) resolves both reefs and prints EXACTLY what
//! would happen — titles, coral counts, edge degree of both, the judgement
//! that would be written, and the alias list `--keep` would learn — without
//! writing anything. `--execute` applies it.
//!
//! `--keep` survives; `--dup` is merged away (its corals and edges
//! re-pointed onto `--keep`, then it is deleted — `Store::merge_nodes`'s own
//! long-standing contract, unchanged by this tool). `--created-by` is the
//! human the design assigns this judgement to (§4.2: "a human-source
//! POSITIVE judgement"; §10.6's owner-correction note: use the OWNING
//! human for `--graph`, which is not always the same account across every
//! graph — see `25_ops_repair.sh`). `--reason` is a free-text note folded
//! into `reef_judgement.evidence.reason`; defaults to a fixed string naming
//! this tool if omitted.
//!
//! ## Refusals
//!
//! This tool refuses (prints why, writes nothing, exits 1) rather than
//! merging when:
//!
//!   - `--keep` or `--dup` does not resolve to a live node at all;
//!   - `--keep == --dup`;
//!   - `--keep` or `--dup` does not belong to `--graph` (covers "in
//!     different graphs" — both must resolve to the SAME graph, and it must
//!     be the one named on the command line, not just each other);
//!   - `--dup` is `--graph`'s current self reef. **This is a CLI-only
//!     guardrail, stricter than the underlying store**: `merge_nodes_impl`
//!     (`store/mod.rs`, "Re-point the ocean's self reef if the dup IS it
//!     (#135)") would actually let this merge proceed and re-point
//!     `graphs.self_node_id` onto `--keep` instead of refusing — so the
//!     store treats "the self reef happened to be named as `dup`" as a
//!     recoverable no-op, not an error. This tool refuses anyway: the whole
//!     point of the ops repair is decoy fragments folding INTO the self
//!     reef, so a batch script naming the self reef as `--dup` by mistake
//!     (e.g. a fat-fingered `--keep`/`--dup` swap) is far more likely to be
//!     an operator error than an intended "the self reef was wrong" call —
//!     and an intended re-designation of the self reef has its own path
//!     (`ensure_self_node`), not this tool.
//!   - an ACTIVE `reef_judgement` with `source='human'` AND
//!     `verdict='negative'` already exists for this exact pair. **Also a
//!     CLI-only guardrail, stricter than the live route**: `write_judgement`
//!     (`store/mod.rs`'s `write_judgement_tx`) only ever refuses a write for
//!     `HumanJudgementExists` when the INCOMING write is non-human against
//!     an active human row; since this tool (like the route) always writes
//!     `source='human'`, the store itself would happily annul a prior human
//!     NEGATIVE and supersede it with this POSITIVE — the same "a human
//!     override supersedes" rule that lets one human correct another's
//!     mistake through the UI, one click at a time, with a person in the
//!     loop reading the card before confirming. This tool has no such human
//!     in the loop (it is meant to run unattended, from a script, over many
//!     pairs) so it refuses instead of silently overriding a prior "these
//!     are different" ruling — annul that judgement first
//!     (`Store::annul_judgement`) if the pair should now merge.
//!
//! ## What it does when it does not refuse (mirrors
//! `write_human_positive_and_merge` exactly)
//!
//!   1. Writes a `source='human'`, `verdict='positive'` `reef_judgement` row
//!      for (`--keep`, `--dup`) via `Store::write_judgement_scoped` — which
//!      supersedes (annuls, forward-pointers to the new row) any ACTIVE
//!      NON-human row for the same pair, the identical annul-then-insert
//!      flow `write_judgement_tx` always runs; this tool adds no special
//!      case for that.
//!   2. Calls `Store::merge_nodes_with_judgement_scoped` with that
//!      judgement id in the SAME transaction, so `node_merges.judgement_id`
//!      back-links the ruling that authorized the merge — identical to what
//!      the authenticated route does.
//!
//! ## Reversal
//!
//! A merge produced by this tool is reversible ONLY through the existing
//! `node_merges` undo path (`POST /v1/nodes/merges/{merge_id}/undo`, or
//! directly via `Store::unmerge_nodes(merge_id)`) — there is no separate
//! "unmerge_reefs" binary, and this tool does not touch `unmerge_nodes`'s
//! semantics in any way. On a successful `--execute` this tool prints the
//! resulting `merge_id` (looked up from `node_merges` by the judgement id it
//! just wrote) specifically so an operator has it in hand to undo.
//!
//! ## Exit codes
//!
//!   0  dry run completed (nothing written), or `--execute` merged
//!      successfully
//!   1  refused (see "Refusals" above) or a store/database error
//!   2  usage/argument error, or no DSN in the environment

use std::process::ExitCode;

use serde_json::{json, Value};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use uuid::Uuid;

use coralswarm_backend::migrator_database_url;
use coralswarm_backend::store::{JudgementSource, JudgementVerdict, NewJudgement, PostgresStore, Store};

const USAGE: &str = "usage: merge_reefs [--execute] --graph <uuid> --keep <uuid> --dup <uuid> \
--created-by <uuid> [--reason \"<text>\"]";

const DEFAULT_REASON: &str = "ops repair: docs/plans/identity-resolution-at-the-door.md §4.2 (merge_reefs CLI)";

#[derive(Debug)]
struct Args {
    execute: bool,
    graph: Uuid,
    keep: Uuid,
    dup: Uuid,
    created_by: Uuid,
    reason: String,
}

fn parse_args(args: impl Iterator<Item = String>) -> Result<Args, String> {
    let mut execute = false;
    let mut graph: Option<Uuid> = None;
    let mut keep: Option<Uuid> = None;
    let mut dup: Option<Uuid> = None;
    let mut created_by: Option<Uuid> = None;
    let mut reason: Option<String> = None;

    let mut args = args;
    fn parse_uuid(flag: &str, args: &mut impl Iterator<Item = String>) -> Result<Uuid, String> {
        let v = args.next().ok_or_else(|| format!("{flag} requires a value"))?;
        Uuid::parse_str(&v).map_err(|e| format!("{flag}: invalid UUID: {e}"))
    }

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--execute" => execute = true,
            "--dry-run" => execute = false,
            "--graph" => graph = Some(parse_uuid("--graph", &mut args)?),
            "--keep" => keep = Some(parse_uuid("--keep", &mut args)?),
            "--dup" => dup = Some(parse_uuid("--dup", &mut args)?),
            "--created-by" => created_by = Some(parse_uuid("--created-by", &mut args)?),
            "--reason" => {
                reason = Some(args.next().ok_or("--reason requires a value")?);
            }
            "-h" | "--help" => {
                println!("{USAGE}");
                println!();
                println!(
                    "Runs once per tenant DB, per pair — see this binary's module doc. \
                     A merge this tool produces is reversible ONLY through the existing \
                     `node_merges` undo path (Store::unmerge_nodes / POST \
                     /v1/nodes/merges/{{merge_id}}/undo) — there is no separate \
                     'unmerge_reefs' binary."
                );
                std::process::exit(0);
            }
            other => return Err(format!("unknown argument: {other}")),
        }
    }

    Ok(Args {
        execute,
        graph: graph.ok_or("--graph is required")?,
        keep: keep.ok_or("--keep is required")?,
        dup: dup.ok_or("--dup is required")?,
        created_by: created_by.ok_or("--created-by is required")?,
        reason: reason.unwrap_or_else(|| DEFAULT_REASON.to_string()),
    })
}

/// What we know about one side of the pair, read straight off `nodes` — no
/// `Store` method needed for a plain by-id lookup.
struct NodeSnapshot {
    node_id: Uuid,
    graph_id: Uuid,
    title: String,
    coral_count: i64,
    body: Value,
    degree: i64,
}

async fn load_node(pool: &PgPool, node_id: Uuid) -> Result<Option<NodeSnapshot>, sqlx::Error> {
    let row: Option<(Uuid, String, i64, Value)> = sqlx::query_as(
        "SELECT graph_id, title, coral_count::bigint, coalesce(body, '{}'::jsonb) \
         FROM nodes WHERE node_id = $1",
    )
    .bind(node_id)
    .fetch_optional(pool)
    .await?;
    let Some((graph_id, title, coral_count, body)) = row else {
        return Ok(None);
    };
    let degree: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM relationship_edge WHERE src_node_id = $1 OR dst_node_id = $1",
    )
    .bind(node_id)
    .fetch_one(pool)
    .await?;
    Ok(Some(NodeSnapshot { node_id, graph_id, title, coral_count, body, degree }))
}

/// Lowercased alias list `--keep` would learn from `--dup`, EXACTLY the set
/// `merge_nodes_impl` (`store/mod.rs`, "Teach the kept reef the merged
/// reef's name (+ its aliases) as aliases") computes: dup's own title, plus
/// dup's existing aliases, lowercased, deduped, minus anything already equal
/// to keep's own lowercased title (a reef is never aliased to itself).
fn aliases_keep_would_learn(dup: &NodeSnapshot, keep: &NodeSnapshot) -> Vec<String> {
    let keep_title_lower = keep.title.to_lowercase();
    let mut learn: Vec<String> = vec![dup.title.to_lowercase()];
    if let Some(arr) = dup.body.get("aliases").and_then(Value::as_array) {
        learn.extend(arr.iter().filter_map(|a| a.as_str().map(str::to_lowercase)));
    }
    let mut seen = std::collections::BTreeSet::new();
    learn
        .into_iter()
        .filter(|a| a != &keep_title_lower)
        .filter(|a| seen.insert(a.clone()))
        .collect()
}

fn print_node(label: &str, n: &NodeSnapshot, self_node_id: Option<Uuid>) {
    println!(
        "  {label}: {}  title={:?}  coral_count={}  edge_degree={}  is_self_reef={}",
        n.node_id,
        n.title,
        n.coral_count,
        n.degree,
        Some(n.node_id) == self_node_id,
    );
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
        "merge_reefs — mode={} (runs once per tenant DB, per pair — see module doc)",
        if args.execute { "EXECUTE" } else { "DRY RUN" }
    );
    println!("  graph:      {}", args.graph);
    println!("  keep:       {}", args.keep);
    println!("  dup:        {}", args.dup);
    println!("  created_by: {}", args.created_by);
    println!("  reason:     {}", args.reason);
    println!();

    // ── Refusal 1: keep == dup ───────────────────────────────────────────
    if args.keep == args.dup {
        eprintln!("REFUSED: --keep and --dup are the same reef ({})", args.keep);
        return ExitCode::FAILURE;
    }

    // ── Load both nodes; refusal 2: either missing ──────────────────────
    let keep_node = match load_node(&pool, args.keep).await {
        Ok(Some(n)) => n,
        Ok(None) => {
            eprintln!("REFUSED: --keep {} does not resolve to a live node", args.keep);
            return ExitCode::FAILURE;
        }
        Err(e) => {
            eprintln!("ERROR: looking up --keep {}: {e}", args.keep);
            return ExitCode::FAILURE;
        }
    };
    let dup_node = match load_node(&pool, args.dup).await {
        Ok(Some(n)) => n,
        Ok(None) => {
            eprintln!("REFUSED: --dup {} does not resolve to a live node", args.dup);
            return ExitCode::FAILURE;
        }
        Err(e) => {
            eprintln!("ERROR: looking up --dup {}: {e}", args.dup);
            return ExitCode::FAILURE;
        }
    };

    // ── Refusal 3: not both in --graph (covers "different graphs") ──────
    if keep_node.graph_id != args.graph || dup_node.graph_id != args.graph {
        eprintln!(
            "REFUSED: --keep is in graph {} and --dup is in graph {}, but --graph was {} — \
             both reefs must belong to exactly the graph named on the command line",
            keep_node.graph_id, dup_node.graph_id, args.graph
        );
        return ExitCode::FAILURE;
    }

    // ── Self-reef lookup (also refusal 4) ────────────────────────────────
    let self_node_id: Option<Uuid> =
        match sqlx::query_scalar("SELECT self_node_id FROM graphs WHERE id = $1")
            .bind(args.graph)
            .fetch_optional(&pool)
            .await
        {
            Ok(Some(id)) => id,
            Ok(None) => {
                eprintln!("REFUSED: --graph {} does not resolve to a live graph", args.graph);
                return ExitCode::FAILURE;
            }
            Err(e) => {
                eprintln!("ERROR: looking up graph {}: {e}", args.graph);
                return ExitCode::FAILURE;
            }
        };
    if self_node_id == Some(args.dup) {
        eprintln!(
            "REFUSED: --dup {} is graph {}'s self reef — the self reef must always be --keep. \
             (The store itself would actually re-point graphs.self_node_id onto --keep instead \
             of refusing — see merge_nodes_impl's \"#135\" comment — but this tool refuses so a \
             --keep/--dup swap in a batch script can never fold the owner's self reef into a \
             decoy. Swap --keep and --dup if that is what you intended.)",
            args.dup, args.graph
        );
        return ExitCode::FAILURE;
    }

    print_node("keep", &keep_node, self_node_id);
    print_node("dup ", &dup_node, self_node_id);
    println!();

    // ── Refusal 5: active human NEGATIVE judgement for this pair ────────
    let store = PostgresStore::new(pool.clone());
    let mut precheck_tx = match pool.begin().await {
        Ok(tx) => tx,
        Err(e) => {
            eprintln!("ERROR: opening precheck transaction: {e}");
            return ExitCode::FAILURE;
        }
    };
    let active = match store
        .get_active_judgement_scoped(&mut precheck_tx, args.graph, args.keep, args.dup)
        .await
    {
        Ok(j) => j,
        Err(e) => {
            eprintln!("ERROR: looking up active judgement for the pair: {e}");
            return ExitCode::FAILURE;
        }
    };
    precheck_tx.rollback().await.ok();

    if let Some(j) = &active {
        println!(
            "  existing active judgement: judgement_id={} verdict={} source={} created_at={}",
            j.judgement_id, j.verdict, j.source, j.created_at
        );
        if j.source == "human" && j.verdict == "negative" {
            eprintln!(
                "REFUSED: an ACTIVE judgement with source='human' verdict='negative' already \
                 exists for this pair (judgement_id={}). A human already ruled these reefs are \
                 DIFFERENT; this tool will not silently override that from an unattended batch \
                 run. Annul it first (Store::annul_judgement) if the pair should now merge.",
                j.judgement_id
            );
            return ExitCode::FAILURE;
        }
        println!(
            "  (this judgement will be superseded — annulled and forward-pointed — by the new \
             human POSITIVE write below, the same annul-then-insert flow write_judgement always \
             runs for any active NON-human row.)"
        );
    } else {
        println!("  existing active judgement: none");
    }
    println!();

    let learn = aliases_keep_would_learn(&dup_node, &keep_node);
    let evidence = json!({"route": "ops_merge_reefs_cli", "reason": args.reason});

    println!("Would write reef_judgement:");
    println!("  graph_id={}", args.graph);
    println!("  node_a_id/node_b_id={{{}, {}}} (canonical order applied internally)", args.keep, args.dup);
    println!("  verdict=positive source=human confidence=NULL model_id=NULL prompt_version=NULL");
    println!("  evidence={evidence}");
    println!("  created_by={}", args.created_by);
    println!();
    println!(
        "Would call merge_nodes_with_judgement(keep={}, dup={}, judgement_id=<new>): up to {} \
         coral(s) re-pointed, up to {} edge(s) touched (re-pointed or dropped on collision).",
        args.keep, args.dup, dup_node.coral_count, dup_node.degree
    );
    println!("Keep ({}) would learn aliases: {:?}", keep_node.title, learn);
    println!();

    if !args.execute {
        println!("Nothing was written. Re-run with --execute to apply.");
        return ExitCode::SUCCESS;
    }

    // ── Execute: write_human_positive_and_merge, copied in miniature ────
    let new_judgement = NewJudgement {
        graph_id: args.graph,
        node_a_id: args.keep,
        node_b_id: args.dup,
        verdict: JudgementVerdict::Positive,
        source: JudgementSource::Human,
        confidence: None,
        model_id: None,
        prompt_version: None,
        evidence,
        created_by: Some(args.created_by),
        expires_at: None,
    };

    let mut tx = match pool.begin().await {
        Ok(tx) => tx,
        Err(e) => {
            eprintln!("ERROR: opening write transaction: {e}");
            return ExitCode::FAILURE;
        }
    };

    let judgement_id = match store.write_judgement_scoped(&mut tx, new_judgement).await {
        Ok(id) => id,
        Err(e) => {
            tx.rollback().await.ok();
            eprintln!("REFUSED/ERROR writing judgement: {e}");
            return ExitCode::FAILURE;
        }
    };

    let moved = match store
        .merge_nodes_with_judgement_scoped(&mut tx, args.keep, args.dup, judgement_id)
        .await
    {
        Ok(m) => m,
        Err(e) => {
            tx.rollback().await.ok();
            eprintln!("REFUSED/ERROR merging: {e}");
            return ExitCode::FAILURE;
        }
    };

    if let Err(e) = tx.commit().await {
        eprintln!("ERROR: commit failed: {e}");
        return ExitCode::FAILURE;
    }

    let merge_id: Option<Uuid> =
        sqlx::query_scalar("SELECT merge_id FROM node_merges WHERE judgement_id = $1")
            .bind(judgement_id)
            .fetch_optional(&pool)
            .await
            .ok()
            .flatten();

    println!("EXECUTED.");
    println!("  judgement_id: {judgement_id}");
    println!("  merge_id:     {}", merge_id.map(|m| m.to_string()).unwrap_or_else(|| "<not found>".to_string()));
    println!("  corals moved: {moved}");
    println!(
        "  To reverse: POST /v1/nodes/merges/{}/undo, or Store::unmerge_nodes({}) directly.",
        merge_id.map(|m| m.to_string()).unwrap_or_else(|| "<merge_id>".to_string()),
        merge_id.map(|m| m.to_string()).unwrap_or_else(|| "<merge_id>".to_string()),
    );

    ExitCode::SUCCESS
}

#[cfg(test)]
mod tests {
    use super::*;

    fn uuid(byte: u8) -> Uuid {
        Uuid::from_bytes([byte; 16])
    }

    #[test]
    fn dry_run_is_the_default() {
        let args = parse_args(
            [
                "--graph".to_string(),
                uuid(1).to_string(),
                "--keep".to_string(),
                uuid(2).to_string(),
                "--dup".to_string(),
                uuid(3).to_string(),
                "--created-by".to_string(),
                uuid(4).to_string(),
            ]
            .into_iter(),
        )
        .expect("valid");
        assert!(!args.execute);
        assert_eq!(args.reason, DEFAULT_REASON);
    }

    #[test]
    fn execute_flag_flips_mode() {
        let args = parse_args(
            [
                "--execute".to_string(),
                "--graph".to_string(),
                uuid(1).to_string(),
                "--keep".to_string(),
                uuid(2).to_string(),
                "--dup".to_string(),
                uuid(3).to_string(),
                "--created-by".to_string(),
                uuid(4).to_string(),
            ]
            .into_iter(),
        )
        .expect("valid");
        assert!(args.execute);
    }

    #[test]
    fn missing_required_flag_is_rejected() {
        let err = parse_args(
            ["--graph".to_string(), uuid(1).to_string()].into_iter(),
        )
        .unwrap_err();
        assert!(err.contains("--keep is required"), "{err}");
    }

    #[test]
    fn invalid_uuid_is_rejected() {
        let err = parse_args(
            ["--graph".to_string(), "not-a-uuid".to_string()].into_iter(),
        )
        .unwrap_err();
        assert!(err.contains("invalid UUID"), "{err}");
    }

    #[test]
    fn unknown_flag_is_rejected() {
        let err = parse_args(["--bogus".to_string()].into_iter()).unwrap_err();
        assert!(err.contains("unknown argument"), "{err}");
    }

    #[test]
    fn reason_defaults_when_omitted_and_overrides_when_given() {
        let args = parse_args(
            [
                "--graph".to_string(),
                uuid(1).to_string(),
                "--keep".to_string(),
                uuid(2).to_string(),
                "--dup".to_string(),
                uuid(3).to_string(),
                "--created-by".to_string(),
                uuid(4).to_string(),
                "--reason".to_string(),
                "custom reason".to_string(),
            ]
            .into_iter(),
        )
        .expect("valid");
        assert_eq!(args.reason, "custom reason");
    }

    #[test]
    fn aliases_keep_would_learn_dedupes_and_excludes_keep_title() {
        let dup = NodeSnapshot {
            node_id: uuid(1),
            graph_id: uuid(9),
            title: "Alex".to_string(),
            coral_count: 3,
            body: json!({"aliases": ["Alex Example", "Alex Alex", "Alex"]}),
            degree: 4,
        };
        let keep = NodeSnapshot {
            node_id: uuid(2),
            graph_id: uuid(9),
            title: "Alex Example".to_string(),
            coral_count: 100,
            body: json!({}),
            degree: 200,
        };
        let learned = aliases_keep_would_learn(&dup, &keep);
        // "Alex" (dup's own title) is present; "Alex Example" is excluded
        // because it equals keep's own lowercased title; "Alex Alex" is
        // kept; duplicates collapse.
        assert_eq!(learned, vec!["Alex".to_string(), "Alex Alex".to_string()]);
    }
}
