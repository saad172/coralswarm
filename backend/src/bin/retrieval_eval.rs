//! Retrieval evaluation runner (Phase 1 quality gate).
//!
//! Ingests the committed fixture corpus into a real Postgres through the real
//! chunker + header + FTS + embedding path, runs every golden query through the
//! mirrored retrieval path, and reports Recall@20, nDCG@10 and abstention
//! accuracy (overall + per category: direct / temporal / abstention / staleness).
//!
//! The CI gate (`gate_against_baseline`) checks pooled Recall@20/nDCG@10, a
//! per-category Recall@20/nDCG@10 (scaled tolerance per category size), and an
//! abstention-category count floor — see `backend/eval/README.md` ("The gate")
//! for the full rules and reasoning.
//!
//! Usage:
//!   DATABASE_URL=postgres://…/coralswarm cargo run -p coralswarm-backend \
//!       --bin retrieval_eval
//!
//! Env:
//!   DATABASE_URL | RELINFER_TEST_DATABASE_URL   Postgres with migrations applied.
//!   EVAL_EMBEDDER = hash (default) | ort         Embedder: deterministic hashing
//!                                                (CI / no model) or real ONNX model.
//!   EVAL_MODEL_PATH / EVAL_TOKENIZER_PATH        Override ONNX paths for `ort`
//!                                                (else EMBED_MODEL_PATH / defaults).
//!   EVAL_TOLERANCE = 0.02 (default)              Slack below baseline before failing.
//!   EVAL_REPORT_OUT = backend/eval/last_report.json   Where the JSON report is written.
//!   EVAL_BASELINE_PATH = backend/eval/baseline.json   Baseline file to gate/update
//!                                                against (same as `--baseline-path`).
//!
//! Flags:
//!   --update-baseline           Overwrite the baseline file with the current run.
//!   --baseline-path <path>      Use a baseline file other than the default
//!                                backend/eval/baseline.json — e.g. the weekly
//!                                real-embedder workflow points this at
//!                                backend/eval/baseline.ort.json. Only the
//!                                DEFAULT path is restricted to the `hash`
//!                                embedder (see below); a non-default path may
//!                                be updated with any embedder, since it is not
//!                                what the blocking CI gate reads.
//!   --sweep alpha=START:END:STEP
//!       Diagnostic-only reranker tuning sweep: re-runs the eval across the
//!       given alpha range (beta = 1.0 - alpha) and prints a per-category
//!       metric grid. Does not touch any baseline file and does not change
//!       `RerankParams::default()`. See backend/eval/README.md.
//!   --dump-scores       Print `category,query_id,top1_score,abstained,expected_abstention`
//!                       for every golden query (one CSV line each, header first) and
//!                       exit 0 without ingesting/checking the gate against a baseline.
//!                       Diagnostic only — for the Phase 2 relevance-floor shadow-mode
//!                       threshold search (`ASK_RELEVANCE_FLOOR_SHADOW`); does not touch
//!                       the production gate or the committed baseline.
//!   --multi-ocean       Diagnostic-only, opt-in mode: ingests the fixture corpus split
//!                       across several synthetic oceans (graphs) instead of one, and
//!                       runs golden queries through `multi_graph_retrieve` (per-ocean
//!                       fan-out + cross-ocean merge/rerank + the per-ocean floor
//!                       guarantee) instead of the single-graph path every other mode
//!                       uses. Does not read or write any baseline, has no pass/fail
//!                       gate, and exits early (before the single-graph ingest below) —
//!                       see backend/eval/README.md ("Multi-ocean fan-out eval mode").
//!
//! Exit codes: 0 = pass (or baseline not comparable), 1 = regression / error.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{bail, Context};
use coralswarm_backend::eval::{
    self, Baseline, CategoryBaseline, EvalReport, FixtureDoc, GoldenQuery, HashEmbedder,
    ReefFixture,
};
use coralswarm_backend::retrieval::rerank::RerankParams;
use coralswarm_backend::store::{PostgresStore, Store};
use coralswarm_core::embed::EmbedProvider;
use coralswarm_core::embed_ort::OrtEmbedder;
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

fn eval_dir() -> PathBuf {
    // backend/eval, relative to this crate's manifest dir.
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("eval")
}

/// Look up a `--flag value` or `--flag=value` style argument.
fn arg_value(flag: &str) -> Option<String> {
    let args: Vec<String> = std::env::args().collect();
    let eq_prefix = format!("{flag}=");
    for i in 0..args.len() {
        if args[i] == flag {
            return args.get(i + 1).cloned();
        }
        if let Some(v) = args[i].strip_prefix(eq_prefix.as_str()) {
            return Some(v.to_string());
        }
    }
    None
}

/// Normalize a path for equality comparison, so that e.g.
/// `backend/eval/baseline.json`, `./backend/eval/baseline.json`, and the
/// absolute equivalent all compare equal.
///
/// Prefers `std::fs::canonicalize`, which resolves `.`/`..`/symlinks against
/// the real filesystem — but that requires the path to exist, and a
/// `--baseline-path`/`EVAL_BASELINE_PATH` may legitimately point at a file
/// that hasn't been created yet (e.g. the first `--update-baseline` run for a
/// brand-new non-default baseline). So when canonicalization fails, fall back
/// to a purely lexical normalization: make the path absolute against the
/// current directory, then collapse `.` and `..` components without touching
/// the filesystem. This is only used for the default-vs-configured baseline
/// comparison, never to open a file, so a non-existent path is expected and
/// fine.
fn normalize_for_compare(p: &std::path::Path) -> PathBuf {
    if let Ok(canonical) = p.canonicalize() {
        return canonical;
    }
    let absolute = if p.is_absolute() {
        p.to_path_buf()
    } else {
        std::env::current_dir()
            .map(|cwd| cwd.join(p))
            .unwrap_or_else(|_| p.to_path_buf())
    };
    let mut normalized = PathBuf::new();
    for component in absolute.components() {
        match component {
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                normalized.pop();
            }
            other => normalized.push(other.as_os_str()),
        }
    }
    normalized
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("retrieval_eval: {e:#}");
        std::process::exit(1);
    }
}

async fn run() -> anyhow::Result<()> {
    let update_baseline = std::env::args().any(|a| a == "--update-baseline");
    let sweep_arg = arg_value("--sweep");
    let dump_scores = std::env::args().any(|a| a == "--dump-scores");
    let multi_ocean = std::env::args().any(|a| a == "--multi-ocean");

    let db_url = std::env::var("DATABASE_URL")
        .or_else(|_| std::env::var("RELINFER_TEST_DATABASE_URL"))
        .context("set DATABASE_URL (or RELINFER_TEST_DATABASE_URL) to a Postgres with migrations applied")?;

    let dir = eval_dir();
    let corpus: Vec<FixtureDoc> =
        eval::load_jsonl(&dir.join("fixtures/corpus.jsonl")).context("load corpus")?;
    let golden: Vec<GoldenQuery> =
        eval::load_jsonl(&dir.join("golden/golden.jsonl")).context("load golden")?;
    if corpus.is_empty() || golden.is_empty() {
        bail!("empty corpus or golden set");
    }
    // Reef (node) fixtures are additive: the file is committed (backend/eval/
    // fixtures/reef.jsonl) so a normal run always exercises the `"reef"`
    // golden category, but `ingest_reef_fixtures` (below) is a no-op on an
    // empty slice, so nothing breaks if the file is ever emptied.
    let reef_fixtures: Vec<ReefFixture> =
        eval::load_jsonl(&dir.join("fixtures/reef.jsonl")).context("load reef fixtures")?;

    // ── Embedder ──────────────────────────────────────────────────────────────
    let embedder_name = std::env::var("EVAL_EMBEDDER").unwrap_or_else(|_| "hash".into());
    let embedder: Arc<dyn EmbedProvider> = match embedder_name.as_str() {
        "hash" => Arc::new(HashEmbedder),
        "ort" => {
            if let Ok(p) = std::env::var("EVAL_MODEL_PATH") {
                std::env::set_var("EMBED_MODEL_PATH", p);
            }
            if let Ok(p) = std::env::var("EVAL_TOKENIZER_PATH") {
                std::env::set_var("EMBED_TOKENIZER_PATH", p);
            }
            Arc::new(
                OrtEmbedder::load()
                    .context("load ONNX embedder (set EVAL_MODEL_PATH/EVAL_TOKENIZER_PATH)")?,
            )
        }
        other => bail!("unknown EVAL_EMBEDDER '{other}' (want hash|ort)"),
    };

    // ── Connect + seed tenancy ────────────────────────────────────────────────
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect(&db_url)
        .await
        .context("connect Postgres")?;

    let email = format!("eval-{}@test.local", Uuid::new_v4());
    let user_id: Uuid = sqlx::query_scalar("INSERT INTO users (email) VALUES ($1) RETURNING id")
        .bind(&email)
        .fetch_one(&pool)
        .await
        .context("seed user")?;
    let account_id: Uuid =
        sqlx::query_scalar("INSERT INTO accounts (owner_user_id) VALUES ($1) RETURNING id")
            .bind(user_id)
            .fetch_one(&pool)
            .await
            .context("seed account")?;

    sqlx::query("INSERT INTO account_members(account_id,user_id,role) VALUES($1,$2,'owner')")
        .bind(account_id).bind(user_id).execute(&pool).await.context("seed owner membership")?;
    let store = PostgresStore::new(pool.clone());

    // ── --multi-ocean: separate mode, own topology, exits before the
    // single-graph path below (it needs several graphs, not one) ─────────────
    if multi_ocean {
        return run_multi_ocean(
            &store,
            &pool,
            embedder.as_ref(),
            &embedder_name,
            account_id,
            user_id,
            &corpus,
            &reef_fixtures,
            &golden,
        )
        .await;
    }

    let graph = store
        .create_graph(account_id, &eval::unique_graph_name(), "personal")
        .await
        .context("create graph")?;
    let graph_id = graph.id;

    // ── Ingest (shared across sweep points and the single-run path) ───────────
    let doc_atoms = eval::ingest_corpus(
        &store,
        &pool,
        embedder.as_ref(),
        graph_id,
        user_id,
        &corpus,
    )
    .await?;
    let reef_nodes = eval::ingest_reef_fixtures(&store, graph_id, &doc_atoms, &reef_fixtures).await?;
    let supports = eval::single_member_fixture_supports(&doc_atoms, &reef_fixtures)?;
    eval::curate_reef_fixtures(&store, &pool, graph_id, account_id, user_id,
        &supports, &reef_fixtures, &reef_nodes).await?;

    // ── Sweep mode: diagnostic-only, no baseline read/write ────────────────────
    if let Some(spec) = sweep_arg {
        let sweep = SweepSpec::parse(&spec).context("parse --sweep (want e.g. alpha=0.5:0.9:0.05)")?;
        return run_sweep(&store, embedder.as_ref(), graph_id, &doc_atoms, &golden, &embedder_name, &sweep).await;
    }

    let report = eval::run_eval(
        &store,
        embedder.as_ref(),
        graph_id,
        &doc_atoms,
        &golden,
        &embedder_name,
        eval::DEFAULT_TOP_K,
        &RerankParams::default(),
    )
    .await?;

    print_summary(&report);

    // ── Write JSON artifact ───────────────────────────────────────────────────
    let out = std::env::var("EVAL_REPORT_OUT")
        .map(PathBuf::from)
        .unwrap_or_else(|_| dir.join("last_report.json"));
    let report_json =
        serde_json::to_string_pretty(&report).context("serialize eval report")?;
    tokio::fs::write(&out, report_json)
        .await
        .with_context(|| format!("write report {}", out.display()))?;
    println!("\nJSON report → {}", out.display());

    // ── --dump-scores: diagnostic top-1 score distribution, no gate ──────────
    //
    // Phase 2 relevance-floor shadow-mode threshold search: per golden query,
    // the TOP-1 (best) reranked candidate's blended score — the exact
    // `RankedAtom.score` value `any_cleared_floor` compares against the floor
    // in production (`routes/queries.rs`) — tagged with its category, so the
    // per-category score distribution (abstention vs. answerable) can be
    // computed from real output rather than eyeballed. Exits before touching
    // the baseline/gate: this is measurement only, not a CI check.
    if dump_scores {
        println!("category,query_id,top1_score,abstained,expected_abstention");
        for q in &report.per_query {
            println!(
                "{},{},{:.6},{},{}",
                q.kind, q.id, q.top_score, q.abstained, q.expected_abstention
            );
        }
        return Ok(());
    }

    // ── Baseline: update or gate ──────────────────────────────────────────────
    let default_baseline_path = dir.join("baseline.json");
    let baseline_path = arg_value("--baseline-path")
        .map(PathBuf::from)
        .or_else(|| std::env::var("EVAL_BASELINE_PATH").ok().map(PathBuf::from))
        .unwrap_or_else(|| default_baseline_path.clone());
    // Compare normalized forms, not raw strings: `backend/eval/baseline.json`,
    // `./backend/eval/baseline.json`, and the absolute equivalent all name the
    // same file but are lexically distinct `PathBuf`s. Without normalizing,
    // `is_default_baseline` could incorrectly come back `false` for a relative
    // `--baseline-path`/`EVAL_BASELINE_PATH` that actually points at the
    // default file, silently skipping the hash-embedder guard below.
    let is_default_baseline =
        normalize_for_compare(&baseline_path) == normalize_for_compare(&default_baseline_path);

    if update_baseline {
        // The DEFAULT baseline (backend/eval/baseline.json) is the blocking CI
        // gate's baseline, and that gate always runs the `hash` embedder.
        // Committing an `ort` (or any non-hash) baseline there would make the
        // embedder mismatch in `gate_against_baseline` skip the gate entirely,
        // so a regression could never fail CI — refuse to write a non-comparable
        // default baseline.
        //
        // A NON-default baseline path (e.g. backend/eval/baseline.ort.json,
        // used by the weekly real-embedder workflow — see
        // .github/workflows/retrieval-eval-ort.yml) is a separate, explicitly
        // non-blocking signal, so it may record any embedder's numbers.
        if report.embedder != "hash" && is_default_baseline {
            bail!(
                "--update-baseline only supports EVAL_EMBEDDER=hash for the default baseline path ({}); \
                 CI gates the hash baseline there, so a non-hash baseline would silently disable the gate. \
                 Pass --baseline-path <file> (e.g. backend/eval/baseline.ort.json) to record a non-hash baseline.",
                default_baseline_path.display()
            );
        }
        let regen_cmd = if is_default_baseline {
            "cargo run -p coralswarm-backend --bin retrieval_eval -- --update-baseline".to_string()
        } else {
            format!(
                "EVAL_EMBEDDER={} cargo run -p coralswarm-backend --bin retrieval_eval -- --update-baseline --baseline-path {}",
                report.embedder,
                baseline_path.display()
            )
        };
        let b = Baseline {
            embedder: report.embedder.clone(),
            recall_at_20: report.overall.recall_at_20,
            ndcg_at_10: report.overall.ndcg_at_10,
            abstention_accuracy: report.overall.abstention_accuracy,
            by_category: report
                .by_category
                .iter()
                .map(|(k, v)| (k.clone(), CategoryBaseline::from(v)))
                .collect(),
            reference_ts: eval::EVAL_REFERENCE_TS_DEFAULT.to_string(),
            note: format!("Regenerate with: {regen_cmd}"),
        };
        let baseline_json = serde_json::to_string_pretty(&b).context("serialize baseline")?;
        tokio::fs::write(&baseline_path, baseline_json)
            .await
            .context("write baseline")?;
        println!("\nBaseline updated → {}", baseline_path.display());
        return Ok(());
    }

    gate_against_baseline(&report, &baseline_path)
}

/// GitHub Actions (and most other CI providers) set `CI=true` in the job
/// environment by default — no repo-side workflow change is needed for this to
/// pick it up. Any other truthy value (`"1"`) is also accepted for callers who
/// set it manually. This is the only gate in this crate that branches on `CI`;
/// grepped for an existing `env::var("CI")` convention elsewhere in the crate
/// first and found none, so this establishes it.
fn is_ci() -> bool {
    matches!(
        std::env::var("CI").as_deref(),
        Ok("true") | Ok("1") | Ok("TRUE") | Ok("True")
    )
}

/// The abstention golden category is small (14 queries as of this writing) —
/// small enough that a *fractional* tolerance is noisy (one flipped query moves
/// the accuracy by ~7 percentage points). Gate it as a **count floor** instead:
/// at most this many of the category's queries may regress from correct (in
/// baseline) to incorrect (now) before the gate fails. `1` was chosen because
/// (a) it still gives a query a one-off pass for score-tie / ordering noise
/// (see `EVAL_TOLERANCE` / the recency-tie discussion in `backend/eval/README.md`),
/// while (b) two or more flips is very unlikely to be noise at n=14 and should
/// fail. If the abstention golden set grows substantially, revisit this as a
/// small fraction (e.g. `max(1, n / 15)`) rather than a hardcoded `1`.
const ABSTENTION_MAX_REGRESSION: usize = 1;

/// Per-category recall/nDCG tolerance. A full regression confined to one small
/// category (e.g. the 14-query `temporal` bucket) can hide inside the *pooled*
/// tolerance, since pooled recall/nDCG average over ~70+ queries. Scale the
/// tolerance per category so it can't: `max(pooled_tolerance, 1.5 / n)` is
/// "never looser than the pooled tolerance, and never so tight that plausible
/// single-query noise (worth `1/n` of the category) fails the gate" — the `1.5`
/// gives a little more than one query's worth of headroom (a category's
/// recall/nDCG for one query can move by more than `1/n` when nDCG's log-discount
/// is involved, not just a flat 0/1), while still failing anything that looks
/// like "more than about one query's worth of regression".
fn category_tolerance(pooled_tolerance: f64, n_category: usize) -> f64 {
    let n = n_category.max(1) as f64;
    pooled_tolerance.max(1.5 / n)
}

/// One recall/nDCG comparison against a floor (pooled or per-category).
struct MetricCheck {
    label: String,
    current: f64,
    baseline: f64,
    floor: f64,
    ok: bool,
}

impl MetricCheck {
    fn new(label: impl Into<String>, current: f64, baseline: f64, tol: f64) -> Self {
        let floor = baseline - tol;
        MetricCheck {
            label: label.into(),
            current,
            baseline,
            floor,
            ok: current >= floor,
        }
    }

    fn print(&self) {
        println!(
            "  {:<28} {:.4}  (baseline {:.4}, floor {:.4})  {}",
            self.label,
            self.current,
            self.baseline,
            self.floor,
            if self.ok { "OK" } else { "FAIL" }
        );
    }
}

/// The abstention-category count-floor check.
struct AbstentionFloorCheck {
    /// `None` when the baseline predates per-category snapshots (old baseline,
    /// `#[serde(default)]` empty `by_category`) — treated as "not comparable,
    /// skip" like the existing embedder-mismatch / missing-baseline cases.
    comparable: bool,
    baseline_correct: usize,
    baseline_total: usize,
    current_correct: usize,
    current_total: usize,
    ok: bool,
}

impl AbstentionFloorCheck {
    fn evaluate(baseline: &Baseline, report: &EvalReport) -> Self {
        let Some(base_cat) = baseline.by_category.get("abstention") else {
            return AbstentionFloorCheck {
                comparable: false,
                baseline_correct: 0,
                baseline_total: 0,
                current_correct: 0,
                current_total: 0,
                ok: true,
            };
        };
        let cur_cat = report.by_category.get("abstention");
        let current_correct = cur_cat.map(|c| c.abstention_correct).unwrap_or(0);
        let current_total = cur_cat.map(|c| c.queries).unwrap_or(0);
        // A regression is a *drop* in the correct count; more answerable/negative
        // queries in the current run than the baseline recorded (denominator grew)
        // is a golden-set change, not a quality regression, so only the raw
        // correct-count floor is enforced (not a ratio).
        let min_allowed = base_cat.abstention_correct.saturating_sub(ABSTENTION_MAX_REGRESSION);
        let ok = current_correct >= min_allowed;
        AbstentionFloorCheck {
            comparable: true,
            baseline_correct: base_cat.abstention_correct,
            baseline_total: base_cat.queries,
            current_correct,
            current_total,
            ok,
        }
    }

    fn print(&self) {
        if !self.comparable {
            println!(
                "  abstention count floor          baseline predates per-category snapshot — skipping"
            );
            return;
        }
        println!(
            "  abstention count floor          {}/{} correct  (baseline {}/{}, max regression {})  {}",
            self.current_correct,
            self.current_total,
            self.baseline_correct,
            self.baseline_total,
            ABSTENTION_MAX_REGRESSION,
            if self.ok { "OK" } else { "FAIL" }
        );
    }
}

/// Pure gate evaluation (no I/O) — kept separate from `gate_against_baseline` so
/// the gating math (count floor, per-category scaling) is unit-testable without
/// a filesystem or a live Postgres.
struct GateOutcome {
    tol: f64,
    pooled: Vec<MetricCheck>,
    per_category: Vec<MetricCheck>,
    abstention: AbstentionFloorCheck,
}

impl GateOutcome {
    fn passed(&self) -> bool {
        self.pooled.iter().all(|c| c.ok)
            && self.per_category.iter().all(|c| c.ok)
            && self.abstention.ok
    }

    fn print(&self) {
        println!("\n[gate] pooled tolerance={:.3}", self.tol);
        for c in &self.pooled {
            c.print();
        }
        if !self.per_category.is_empty() {
            println!("\n[gate] per-category (tolerance = max(pooled, 1.5/n)):");
            for c in &self.per_category {
                c.print();
            }
        }
        println!();
        self.abstention.print();
    }

    fn failure_message(&self) -> String {
        let mut failing: Vec<String> = Vec::new();
        for c in self.pooled.iter().chain(self.per_category.iter()) {
            if !c.ok {
                failing.push(format!(
                    "{} {:.4} (floor {:.4})",
                    c.label, c.current, c.floor
                ));
            }
        }
        if !self.abstention.ok {
            failing.push(format!(
                "abstention count floor: {}/{} correct (baseline {}/{}, max regression {})",
                self.abstention.current_correct,
                self.abstention.current_total,
                self.abstention.baseline_correct,
                self.abstention.baseline_total,
                ABSTENTION_MAX_REGRESSION
            ));
        }
        format!(
            "regression: {}. If intentional, re-baseline with --update-baseline.",
            failing.join("; ")
        )
    }
}

fn evaluate_gate(report: &EvalReport, baseline: &Baseline, tol: f64) -> GateOutcome {
    let pooled = vec![
        MetricCheck::new(
            "Recall@20 (pooled)",
            report.overall.recall_at_20,
            baseline.recall_at_20,
            tol,
        ),
        MetricCheck::new(
            "nDCG@10 (pooled)",
            report.overall.ndcg_at_10,
            baseline.ndcg_at_10,
            tol,
        ),
    ];

    // Per-category recall/nDCG. Skip "abstention" (its recall/nDCG are always
    // 0/0 — abstention queries carry no `relevant_doc_ids` — that category is
    // covered by the count floor instead), skip any category listed in
    // `eval::NON_GATED_CATEGORIES` (e.g. "distractor" — informational under
    // the hash embedder by design, see that constant's doc comment for why),
    // and skip any category the baseline doesn't have a snapshot for (old
    // baseline predating per-category data, or a brand-new category just
    // added to the golden set with no prior baseline to compare against —
    // both are "not comparable yet", not a failure).
    let mut per_category = Vec::new();
    let mut cats: Vec<&String> = baseline.by_category.keys().collect();
    cats.sort();
    for kind in cats {
        if kind == "abstention" || eval::NON_GATED_CATEGORIES.contains(&kind.as_str()) {
            continue;
        }
        let base_cat = &baseline.by_category[kind];
        let Some(cur_cat) = report.by_category.get(kind) else {
            continue;
        };
        let cat_tol = category_tolerance(tol, base_cat.queries);
        per_category.push(MetricCheck::new(
            format!("Recall@20 [{kind}]"),
            cur_cat.recall_at_20,
            base_cat.recall_at_20,
            cat_tol,
        ));
        per_category.push(MetricCheck::new(
            format!("nDCG@10 [{kind}]"),
            cur_cat.ndcg_at_10,
            base_cat.ndcg_at_10,
            cat_tol,
        ));
    }

    let abstention = AbstentionFloorCheck::evaluate(baseline, report);

    GateOutcome {
        tol,
        pooled,
        per_category,
        abstention,
    }
}

fn gate_against_baseline(report: &EvalReport, baseline_path: &std::path::Path) -> anyhow::Result<()> {
    let ci = is_ci();

    let text = match std::fs::read_to_string(baseline_path) {
        Ok(t) => t,
        Err(e) => {
            let msg = format!(
                "no baseline at {} ({e}) — record one with --update-baseline.",
                baseline_path.display()
            );
            if ci {
                bail!(
                    "[gate] {msg} Running under CI (CI env var set): a missing baseline is a \
                     hard failure, not a silent pass — commit a baseline before merging."
                );
            }
            println!("\n[gate] {msg} Skipping gate (not running under CI).");
            return Ok(());
        }
    };
    let baseline: Baseline = serde_json::from_str(&text).context("parse baseline")?;

    if baseline.embedder != report.embedder {
        let msg = format!(
            "baseline embedder '{}' != current '{}' — numbers are not comparable.",
            baseline.embedder, report.embedder
        );
        if ci {
            bail!(
                "[gate] {msg} Running under CI (CI env var set): an embedder mismatch is a hard \
                 failure, not a silent pass — CI must run the baseline's embedder ('hash')."
            );
        }
        println!("\n[gate] {msg} Skipping gate (not running under CI).");
        return Ok(());
    }

    let tol = parse_tolerance(&baseline)?;
    println!("\n[gate] baseline={}", baseline_path.display());
    let outcome = evaluate_gate(report, &baseline, tol);
    outcome.print();

    if outcome.passed() {
        println!("\n[gate] PASS");
        Ok(())
    } else {
        bail!("{}", outcome.failure_message())
    }
}

/// Default slack below baseline before the gate fails.
const EVAL_TOLERANCE_DEFAULT: f64 = 0.02;

/// Parse and bound `EVAL_TOLERANCE`. A tolerance must be finite and in
/// `[0.0, min(baseline.recall, baseline.ndcg))` — anything that would push a
/// floor to zero or below (e.g. `EVAL_TOLERANCE=1`) would let every result pass
/// and silently disable the gate, so those values are rejected rather than
/// honoured. Falls back to [`EVAL_TOLERANCE_DEFAULT`] when unset.
fn parse_tolerance(baseline: &Baseline) -> anyhow::Result<f64> {
    let raw = match std::env::var("EVAL_TOLERANCE") {
        Ok(v) => v,
        Err(_) => return Ok(EVAL_TOLERANCE_DEFAULT),
    };
    let tol: f64 = raw
        .trim()
        .parse()
        .with_context(|| format!("EVAL_TOLERANCE must be a number, got {raw:?}"))?;
    if !tol.is_finite() || tol < 0.0 {
        bail!("EVAL_TOLERANCE must be finite and non-negative, got {tol}");
    }
    // The floor is `baseline - tol`; keep it strictly positive so the gate can
    // still fail. `min` of the two baseline metrics is the binding constraint.
    let max_metric_floor = baseline.recall_at_20.min(baseline.ndcg_at_10);
    if tol >= max_metric_floor {
        bail!(
            "EVAL_TOLERANCE={tol} would drop a metric floor to <= 0 \
             (baseline min metric {max_metric_floor:.4}) and disable the gate"
        );
    }
    Ok(tol)
}

#[cfg(test)]
mod tests {
    use super::*;
    use coralswarm_backend::eval::CategoryMetrics;
    use std::collections::HashMap;

    fn cat(queries: usize, recall: f64, ndcg: f64, abstention_correct: usize) -> CategoryMetrics {
        CategoryMetrics {
            queries,
            recall_at_20: recall,
            ndcg_at_10: ndcg,
            abstention_accuracy: if queries > 0 {
                abstention_correct as f64 / queries as f64
            } else {
                0.0
            },
            abstention_correct,
        }
    }

    fn base_cat(queries: usize, recall: f64, ndcg: f64, abstention_correct: usize) -> CategoryBaseline {
        CategoryBaseline::from(&cat(queries, recall, ndcg, abstention_correct))
    }

    /// Mirrors `eval::run_eval`'s overall aggregation: recall/nDCG are a
    /// query-count-weighted average over *answerable* categories only (the
    /// "abstention" category contributes no recall/nDCG queries, matching
    /// production where abstention negatives are excluded from `cat_answerable`).
    fn report_with(by_category: HashMap<String, CategoryMetrics>) -> EvalReport {
        let answerable: Vec<&CategoryMetrics> = by_category
            .iter()
            .filter(|(k, _)| k.as_str() != "abstention")
            .map(|(_, v)| v)
            .collect();
        let ans_n: usize = answerable.iter().map(|c| c.queries).sum();
        let recall = if ans_n > 0 {
            answerable
                .iter()
                .map(|c| c.recall_at_20 * c.queries as f64)
                .sum::<f64>()
                / ans_n as f64
        } else {
            0.0
        };
        let ndcg = if ans_n > 0 {
            answerable
                .iter()
                .map(|c| c.ndcg_at_10 * c.queries as f64)
                .sum::<f64>()
                / ans_n as f64
        } else {
            0.0
        };
        let overall = cat(
            by_category.values().map(|c| c.queries).sum(),
            recall,
            ndcg,
            by_category.values().map(|c| c.abstention_correct).sum(),
        );
        EvalReport {
            embedder: "hash".into(),
            embed_model: "test-model".into(),
            top_k: 20,
            corpus_docs: 10,
            golden_queries: by_category.values().map(|c| c.queries).sum(),
            overall,
            by_category,
            per_query: Vec::new(),
        }
    }

    fn baseline_with(by_category: HashMap<String, CategoryBaseline>) -> Baseline {
        Baseline {
            embedder: "hash".into(),
            recall_at_20: 0.99,
            ndcg_at_10: 0.83,
            abstention_accuracy: 0.0,
            by_category,
            reference_ts: "2026-07-01T00:00:00Z".into(),
            note: "test".into(),
        }
    }

    // ── category_tolerance ──────────────────────────────────────────────────

    #[test]
    fn category_tolerance_never_looser_than_pooled() {
        assert_eq!(category_tolerance(0.02, 1000), 0.02);
    }

    #[test]
    fn category_tolerance_widens_for_small_categories() {
        // 14 queries (the abstention/temporal-sized categories): 1.5/14 ≈ 0.1071.
        let t = category_tolerance(0.02, 14);
        assert!((t - 1.5 / 14.0).abs() < 1e-9);
        assert!(t > 0.02);
    }

    #[test]
    fn category_tolerance_handles_zero_n_without_panicking() {
        let t = category_tolerance(0.02, 0);
        assert!(t.is_finite());
        assert_eq!(t, 1.5); // n clamped to 1
    }

    // ── AbstentionFloorCheck / count floor ──────────────────────────────────

    #[test]
    fn abstention_floor_passes_when_no_regression() {
        let mut base = HashMap::new();
        base.insert("abstention".to_string(), base_cat(14, 0.0, 0.0, 13));
        let baseline = baseline_with(base);

        let mut cur = HashMap::new();
        cur.insert("abstention".to_string(), cat(14, 0.0, 0.0, 13));
        let report = report_with(cur);

        let check = AbstentionFloorCheck::evaluate(&baseline, &report);
        assert!(check.comparable);
        assert!(check.ok, "13/14 -> 13/14 should not regress");
    }

    #[test]
    fn abstention_floor_allows_exactly_one_regression() {
        let mut base = HashMap::new();
        base.insert("abstention".to_string(), base_cat(14, 0.0, 0.0, 13));
        let baseline = baseline_with(base);

        let mut cur = HashMap::new();
        cur.insert("abstention".to_string(), cat(14, 0.0, 0.0, 12));
        let report = report_with(cur);

        let check = AbstentionFloorCheck::evaluate(&baseline, &report);
        assert!(check.ok, "one flip (13 -> 12) is within ABSTENTION_MAX_REGRESSION");
    }

    #[test]
    fn abstention_floor_fails_two_regressions() {
        let mut base = HashMap::new();
        base.insert("abstention".to_string(), base_cat(14, 0.0, 0.0, 13));
        let baseline = baseline_with(base);

        let mut cur = HashMap::new();
        cur.insert("abstention".to_string(), cat(14, 0.0, 0.0, 11));
        let report = report_with(cur);

        let check = AbstentionFloorCheck::evaluate(&baseline, &report);
        assert!(!check.ok, "two flips (13 -> 11) must fail the count floor");
    }

    #[test]
    fn abstention_floor_skips_when_baseline_has_no_category_snapshot() {
        // Old baseline recorded before per-category snapshots existed.
        let baseline = baseline_with(HashMap::new());
        let mut cur = HashMap::new();
        cur.insert("abstention".to_string(), cat(14, 0.0, 0.0, 0));
        let report = report_with(cur);

        let check = AbstentionFloorCheck::evaluate(&baseline, &report);
        assert!(!check.comparable);
        assert!(check.ok, "not-comparable must not fail the gate");
    }

    #[test]
    fn abstention_floor_improvement_always_passes() {
        let mut base = HashMap::new();
        base.insert("abstention".to_string(), base_cat(14, 0.0, 0.0, 10));
        let baseline = baseline_with(base);

        let mut cur = HashMap::new();
        cur.insert("abstention".to_string(), cat(14, 0.0, 0.0, 14));
        let report = report_with(cur);

        assert!(AbstentionFloorCheck::evaluate(&baseline, &report).ok);
    }

    // ── evaluate_gate: per-category + pooled + count floor, end to end ──────

    #[test]
    fn evaluate_gate_passes_when_nothing_regressed() {
        let mut base = HashMap::new();
        base.insert("direct".to_string(), base_cat(40, 0.99, 0.83, 40));
        base.insert("temporal".to_string(), base_cat(18, 0.95, 0.80, 18));
        base.insert("abstention".to_string(), base_cat(14, 0.0, 0.0, 12));
        let baseline = baseline_with(base);

        let mut cur = HashMap::new();
        cur.insert("direct".to_string(), cat(40, 0.99, 0.83, 40));
        cur.insert("temporal".to_string(), cat(18, 0.95, 0.80, 18));
        cur.insert("abstention".to_string(), cat(14, 0.0, 0.0, 12));
        let report = report_with(cur);

        let outcome = evaluate_gate(&report, &baseline, 0.02);
        assert!(outcome.passed(), "{}", outcome.failure_message());
    }

    #[test]
    fn evaluate_gate_catches_small_category_regression_hidden_by_pooled_average() {
        // `temporal` is a small slice (10 of 100 queries) that partially
        // regresses (recall 0.95 -> 0.78, a 0.17 drop). `direct` (90 queries,
        // 90% of the pool) is unchanged, so the *pooled* weighted average only
        // moves by 0.17 * 10/100 = 0.017 — under the 0.02 pooled tolerance, so
        // the pooled check alone would pass this. The category's own tolerance
        // (`max(0.02, 1.5/10) = 0.15`) is tighter than that, so per-category
        // gating catches what pooled gating would hide. This is exactly the
        // scenario per-category gating exists for.
        let mut base = HashMap::new();
        base.insert("direct".to_string(), base_cat(90, 0.99, 0.83, 90));
        base.insert("temporal".to_string(), base_cat(10, 0.95, 0.83, 10));
        let pooled_baseline = (0.99 * 90.0 + 0.95 * 10.0) / 100.0;
        let mut baseline = baseline_with(base);
        // `Baseline.recall_at_20` is the pooled snapshot recorded alongside the
        // per-category breakdown — mirror the real relationship (weighted
        // average of the categories) rather than `baseline_with`'s placeholder.
        baseline.recall_at_20 = pooled_baseline;
        baseline.ndcg_at_10 = 0.83;

        let mut cur = HashMap::new();
        cur.insert("direct".to_string(), cat(90, 0.99, 0.83, 90));
        cur.insert("temporal".to_string(), cat(10, 0.78, 0.83, 10)); // partial regression
        let report = report_with(cur);

        let pooled_current = (0.99 * 90.0 + 0.78 * 10.0) / 100.0;
        assert!(
            pooled_current >= pooled_baseline - 0.02,
            "sanity: pooled tolerance alone would NOT catch this regression \
             (baseline {pooled_baseline}, current {pooled_current})"
        );

        let outcome = evaluate_gate(&report, &baseline, 0.02);
        assert!(
            outcome.pooled.iter().all(|c| c.ok),
            "sanity: pooled checks should pass on their own"
        );
        assert!(
            !outcome.passed(),
            "per-category gating must fail a regressed small category the pooled check missed"
        );
        assert!(outcome.per_category.iter().any(|c| !c.ok && c.label.contains("temporal")));
    }

    #[test]
    fn evaluate_gate_skips_category_missing_from_baseline() {
        // A brand-new golden category (e.g. "staleness") with no prior baseline
        // entry is "not comparable yet", not a failure.
        let base = HashMap::new();
        let baseline = baseline_with(base);

        let mut cur = HashMap::new();
        cur.insert("staleness".to_string(), cat(9, 0.5, 0.4, 0));
        let report = report_with(cur);

        let outcome = evaluate_gate(&report, &baseline, 0.02);
        assert!(outcome.per_category.is_empty());
        assert!(!outcome.abstention.comparable);
    }

    #[test]
    fn evaluate_gate_skips_non_gated_category_even_with_a_baseline_snapshot() {
        // "distractor" (coralswarm_backend::eval::NON_GATED_CATEGORIES) is
        // deliberately hash-embedder-blind by construction — a real
        // regression on it under the hash embedder isn't meaningful, and a
        // spurious drop from hash-collision noise shouldn't fail the blocking
        // gate. Unlike the "missing from baseline" case above, this baseline
        // DOES have a "distractor" snapshot — the skip has to be by category
        // name, not just by baseline presence.
        //
        // `overall`/`baseline.recall_at_20` are set equal here (both 0.99) —
        // real `aggregate_eval_report` already excludes `NON_GATED_CATEGORIES`
        // from that pooled figure (see `eval.rs::
        // aggregate_eval_report_excludes_non_gated_categories_from_pooled_recall`),
        // so this test isolates exactly the thing under test here: the
        // per-category loop's skip, not the pooled computation.
        let mut base = HashMap::new();
        base.insert("direct".to_string(), base_cat(40, 0.99, 0.83, 40));
        base.insert("distractor".to_string(), base_cat(12, 0.90, 0.80, 12));
        let baseline = baseline_with(base);

        let mut by_category = HashMap::new();
        by_category.insert("direct".to_string(), cat(40, 0.99, 0.83, 40));
        // A catastrophic "regression" on distractor alone must not fail the gate.
        by_category.insert("distractor".to_string(), cat(12, 0.0, 0.0, 0));
        let report = EvalReport {
            embedder: "hash".into(),
            embed_model: "test-model".into(),
            top_k: 20,
            corpus_docs: 10,
            golden_queries: 52,
            overall: cat(52, 0.99, 0.83, 40),
            by_category,
            per_query: Vec::new(),
        };

        let outcome = evaluate_gate(&report, &baseline, 0.02);
        assert!(
            outcome
                .per_category
                .iter()
                .all(|c| !c.label.contains("distractor")),
            "distractor must never appear in the per-category gate checks"
        );
        assert!(
            outcome.passed(),
            "a regression confined to the non-gated distractor category must not fail the gate: {}",
            outcome.failure_message()
        );
    }

    #[test]
    fn evaluate_gate_pooled_metrics_still_gate() {
        let mut base = HashMap::new();
        base.insert("direct".to_string(), base_cat(40, 0.99, 0.83, 40));
        let baseline = baseline_with(base);

        let mut cur = HashMap::new();
        cur.insert("direct".to_string(), cat(40, 0.50, 0.40, 40));
        let report = report_with(cur);

        let outcome = evaluate_gate(&report, &baseline, 0.02);
        assert!(!outcome.passed());
        assert!(outcome.pooled.iter().any(|c| !c.ok));
    }

    // ── is_ci ─────────────────────────────────────────────────────────────

    #[test]
    fn is_ci_recognizes_github_actions_default() {
        // std::env is process-global; guard with a lock so this doesn't race
        // other tests in the same binary (this crate has few env-mutating
        // tests, but be defensive).
        static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
        let _guard = ENV_LOCK.lock().unwrap();

        let prior = std::env::var("CI").ok();

        std::env::set_var("CI", "true");
        assert!(is_ci());

        std::env::remove_var("CI");
        assert!(!is_ci());

        std::env::set_var("CI", "false");
        assert!(!is_ci());

        match prior {
            Some(v) => std::env::set_var("CI", v),
            None => std::env::remove_var("CI"),
        }
    }

    // ── normalize_for_compare / is_default_baseline ──────────────────────────

    #[test]
    fn normalize_for_compare_relative_matches_default() {
        let default = eval_dir().join("baseline.json");
        let relative = PathBuf::from("eval/baseline.json");
        assert_eq!(
            normalize_for_compare(&relative),
            normalize_for_compare(&default),
            "a bare relative path resolving to the default baseline must normalize equal"
        );
    }

    #[test]
    fn normalize_for_compare_dot_slash_relative_matches_default() {
        let default = eval_dir().join("baseline.json");
        let dot_relative = PathBuf::from("./eval/baseline.json");
        assert_eq!(
            normalize_for_compare(&dot_relative),
            normalize_for_compare(&default),
            "a `./`-prefixed relative path resolving to the default baseline must normalize equal"
        );
    }

    #[test]
    fn normalize_for_compare_absolute_matches_default() {
        let default = eval_dir().join("baseline.json");
        let absolute = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("eval/baseline.json");
        assert!(absolute.is_absolute());
        assert_eq!(
            normalize_for_compare(&absolute),
            normalize_for_compare(&default),
            "an absolute path resolving to the default baseline must normalize equal"
        );
    }

    #[test]
    fn normalize_for_compare_different_path_is_not_default() {
        let default = eval_dir().join("baseline.json");
        // A distinct, real file (the weekly ort-embedder baseline) — must not
        // be mistaken for the default hash-embedder baseline.
        let other = eval_dir().join("baseline.ort.json");
        assert_ne!(
            normalize_for_compare(&other),
            normalize_for_compare(&default),
            "a genuinely different baseline file must not normalize equal to the default"
        );
    }

    #[test]
    fn normalize_for_compare_lexical_fallback_for_missing_file() {
        // Neither side of this comparison exists on disk, so canonicalize()
        // fails for both and this exercises the lexical (non-filesystem)
        // fallback branch: `./eval/../eval/does-not-exist.json` should
        // lexically collapse to the same absolute path as
        // `eval/does-not-exist.json`.
        let a = PathBuf::from("./eval/../eval/does-not-exist.json");
        let b = PathBuf::from("eval/does-not-exist.json");
        assert_eq!(normalize_for_compare(&a), normalize_for_compare(&b));
    }
}

fn print_summary(report: &EvalReport) {
    println!("── Retrieval eval ──────────────────────────────────────────");
    println!(
        "embedder={} model={} top_k={} docs={} queries={}",
        report.embedder, report.embed_model, report.top_k, report.corpus_docs, report.golden_queries
    );
    println!(
        "\nOVERALL   Recall@20={:.4}  nDCG@10={:.4}  Abstention-acc={:.4}",
        report.overall.recall_at_20, report.overall.ndcg_at_10, report.overall.abstention_accuracy
    );
    println!("\n{:<12} {:>7} {:>10} {:>9} {:>12}", "category", "queries", "Recall@20", "nDCG@10", "Abstain-acc");
    let mut kinds: Vec<&String> = report.by_category.keys().collect();
    kinds.sort();
    for k in kinds {
        let m = &report.by_category[k];
        println!(
            "{:<12} {:>7} {:>10.4} {:>9.4} {:>12.4}",
            k, m.queries, m.recall_at_20, m.ndcg_at_10, m.abstention_accuracy
        );
    }
}

// ── Sweep mode (diagnostic reranker tuning grid) ────────────────────────────────
//
// NOT wired to any baseline file and does not change `RerankParams::default()`
// (backend/src/retrieval/rerank.rs) — purely a reporting tool for the manual
// policy documented in backend/eval/README.md: any future PR that wants to
// change the shipped alpha/beta must run this, paste the grid into the PR, and
// add an ADR (see docs/decisions/).

struct SweepSpec {
    param: SweepParam,
    start: f64,
    end: f64,
    step: f64,
}

enum SweepParam {
    Alpha,
}

impl SweepSpec {
    /// Parse `alpha=START:END:STEP`, e.g. `alpha=0.5:0.9:0.05`.
    fn parse(s: &str) -> anyhow::Result<Self> {
        let (param_name, range) = s
            .split_once('=')
            .with_context(|| format!("expected PARAM=START:END:STEP, got {s:?}"))?;
        let param = match param_name {
            "alpha" => SweepParam::Alpha,
            other => bail!("unsupported sweep param '{other}' (only 'alpha' is supported; beta = 1.0 - alpha)"),
        };
        let parts: Vec<&str> = range.split(':').collect();
        let [start, end, step]: [&str; 3] = parts
            .try_into()
            .map_err(|_| anyhow::anyhow!("expected START:END:STEP, got {range:?}"))?;
        let start: f64 = start.trim().parse().context("parse start")?;
        let end: f64 = end.trim().parse().context("parse end")?;
        let step: f64 = step.trim().parse().context("parse step")?;
        if !(start.is_finite() && end.is_finite() && step.is_finite()) {
            bail!("sweep bounds must be finite");
        }
        if step <= 0.0 {
            bail!("sweep step must be > 0, got {step}");
        }
        if end < start {
            bail!("sweep end ({end}) must be >= start ({start})");
        }
        Ok(Self { param, start, end, step })
    }

    /// Alpha values from `start` to `end` inclusive (within half a step of
    /// rounding slack, so `0.5:0.9:0.05` includes `0.9`).
    fn values(&self) -> Vec<f64> {
        let n_steps = ((self.end - self.start) / self.step).round().max(0.0) as i64;
        (0..=n_steps)
            .map(|i| {
                let v = self.start + (i as f64) * self.step;
                (v * 1e6).round() / 1e6 // clean up float accumulation
            })
            .collect()
    }
}

async fn run_sweep(
    store: &dyn Store,
    embedder: &dyn EmbedProvider,
    graph_id: Uuid,
    doc_atoms: &std::collections::HashMap<String, Vec<Uuid>>,
    golden: &[GoldenQuery],
    embedder_name: &str,
    sweep: &SweepSpec,
) -> anyhow::Result<()> {
    let alphas = sweep.values();
    let SweepParam::Alpha = sweep.param;

    println!(
        "── Retrieval eval — reranker sweep (diagnostic only; does not touch RerankParams::default()) ──"
    );
    println!(
        "embedder={} points={} alpha∈[{},{}] step={}\n",
        embedder_name, alphas.len(), sweep.start, sweep.end, sweep.step
    );

    let mut reports: Vec<(f64, EvalReport)> = Vec::with_capacity(alphas.len());
    for &alpha in &alphas {
        let beta = 1.0 - alpha;
        let params = RerankParams {
            alpha,
            beta,
            recency_half_life_days: RerankParams::default().recency_half_life_days,
        };
        let report = eval::run_eval(
            store,
            embedder,
            graph_id,
            doc_atoms,
            golden,
            embedder_name,
            eval::DEFAULT_TOP_K,
            &params,
        )
        .await
        .with_context(|| format!("run_eval failed at alpha={alpha}"))?;
        reports.push((alpha, report));
    }

    // ── Overall grid ────────────────────────────────────────────────────────
    println!("OVERALL");
    println!(
        "{:>8} {:>10} {:>9} {:>12}",
        "alpha", "Recall@20", "nDCG@10", "Abstain-acc"
    );
    for (alpha, r) in &reports {
        println!(
            "{:>8.3} {:>10.4} {:>9.4} {:>12.4}",
            alpha, r.overall.recall_at_20, r.overall.ndcg_at_10, r.overall.abstention_accuracy
        );
    }

    // ── Per-category grid ───────────────────────────────────────────────────
    let mut kinds: Vec<String> = reports
        .first()
        .map(|(_, r)| r.by_category.keys().cloned().collect())
        .unwrap_or_default();
    kinds.sort();

    for kind in &kinds {
        println!("\n{kind}");
        println!(
            "{:>8} {:>10} {:>9} {:>12}",
            "alpha", "Recall@20", "nDCG@10", "Abstain-acc"
        );
        for (alpha, r) in &reports {
            if let Some(m) = r.by_category.get(kind) {
                println!(
                    "{:>8.3} {:>10.4} {:>9.4} {:>12.4}",
                    alpha, m.recall_at_20, m.ndcg_at_10, m.abstention_accuracy
                );
            }
        }
    }

    println!(
        "\n[sweep] diagnostic only — RerankParams::default() (alpha=0.7/beta=0.3) unchanged. \
         To adopt a new default: run this sweep, paste the grid in the PR, and add an ADR \
         under docs/decisions/ (see backend/eval/README.md)."
    );

    Ok(())
}

// ── Multi-ocean fan-out mode (opt-in, diagnostic) ───────────────────────────────
//
// Exercises `crate::routes::queries::multi_graph_retrieve` — the per-ocean
// fan-out engine every other mode in this binary never reaches, since they all
// ingest into (and retrieve against) exactly one graph. NOT part of the
// default/blocking eval run: no baseline is read or written, and there is no
// pass/fail gate, mirroring `--sweep`'s stance (see backend/eval/README.md,
// "Multi-ocean fan-out eval mode").

/// Round-robin split count for the full-golden-set multi-ocean run (part 1,
/// `run_multi_ocean`). 3 (not 2) so `synthesis_context_cap` actually scales
/// past `top_k` (see that function in `routes/queries.rs`: for one ocean the
/// cap is exactly `top_k`; more oceans widens it) — giving the per-ocean floor
/// guarantee real room to matter rather than degenerating to the single-ocean
/// case.
const MULTI_OCEAN_COUNT: usize = 3;

/// `retrieval_eval --multi-ocean`: splits the fixture corpus across several
/// synthetic oceans (graphs) instead of one, then runs golden queries against
/// `multi_graph_retrieve` — the cross-ocean merge/rerank + per-ocean floor
/// guarantee path a single-graph eval run can never reach. Two parts:
///
/// 1. The full committed golden set, round-robin-split across
///    [`MULTI_OCEAN_COUNT`] oceans — a sanity check that fanning the SAME
///    corpus out across several graphs and globally re-merging it produces
///    comparable Recall@20/nDCG@10 to the single-graph run (proves the mode
///    actually works end to end, not just that it runs without erroring).
/// 2. [`run_minority_floor_scenario`]: a deliberately lopsided 2-ocean split
///    (the full corpus in one "majority" ocean, a single fact alone in a
///    "minority" ocean) with 2 queries whose only relevant doc lives in the
///    minority ocean — printing each query's survival + rank in the final
///    merged set as evidence for whether the per-ocean floor guarantee (ADR-
///    0011 A5) was load-bearing for it.
#[allow(clippy::too_many_arguments)]
async fn run_multi_ocean(
    store: &PostgresStore,
    pool: &sqlx::PgPool,
    embedder: &dyn EmbedProvider,
    embedder_name: &str,
    account_id: Uuid,
    user_id: Uuid,
    corpus: &[FixtureDoc],
    reef_fixtures: &[ReefFixture],
    golden: &[GoldenQuery],
) -> anyhow::Result<()> {
    println!("── Retrieval eval — multi-ocean fan-out (diagnostic only; no baseline) ──");
    println!(
        "embedder={embedder_name} oceans={MULTI_OCEAN_COUNT} corpus_docs={} golden_queries={}\n",
        corpus.len(),
        golden.len()
    );

    // ── Part 1: full golden set, round-robin split across MULTI_OCEAN_COUNT oceans ──
    let mut sorted_corpus: Vec<FixtureDoc> = corpus.to_vec();
    sorted_corpus.sort_by(|a, b| a.doc_id.cmp(&b.doc_id));
    let mut buckets: Vec<Vec<FixtureDoc>> = vec![Vec::new(); MULTI_OCEAN_COUNT];
    for (i, doc) in sorted_corpus.into_iter().enumerate() {
        buckets[i % MULTI_OCEAN_COUNT].push(doc);
    }

    let mut graph_ids: Vec<Uuid> = Vec::with_capacity(MULTI_OCEAN_COUNT);
    let mut doc_atoms: HashMap<String, Vec<Uuid>> = HashMap::new();
    for (i, bucket) in buckets.iter().enumerate() {
        let graph = store
            .create_graph(
                account_id,
                &format!("{}-mo{i}", eval::unique_graph_name()),
                "personal",
            )
            .await
            .with_context(|| format!("create ocean {i}"))?;
        let atoms = eval::ingest_corpus(store, pool, embedder, graph.id, user_id, bucket)
            .await
            .with_context(|| format!("ingest ocean {i} ({} docs)", bucket.len()))?;

        // A reef fixture may only be linked in the ocean that holds every one
        // of its `atom_doc_ids` (a node's `graph_id` must match its atoms' —
        // `keyword_search_node_atoms`/`expand` are both graph-scoped). Every
        // fixture in `reef.jsonl` links a single doc today, so this never
        // actually straddles a bucket boundary, but the filter keeps that
        // invariant explicit rather than assumed.
        let ocean_reefs: Vec<ReefFixture> = reef_fixtures
            .iter()
            .filter(|r| r.atom_doc_ids.iter().all(|d| atoms.contains_key(d)))
            .cloned()
            .collect();
        let reef_nodes = eval::ingest_reef_fixtures(store, graph.id, &atoms, &ocean_reefs)
            .await
            .with_context(|| format!("ingest reef fixtures ocean {i}"))?;
        let supports = eval::single_member_fixture_supports(&atoms, &ocean_reefs)?;
        eval::curate_reef_fixtures(store, pool, graph.id, account_id, user_id,
            &supports, &ocean_reefs, &reef_nodes).await?;

        println!(
            "ocean {i}: graph={} docs={} atoms={} reefs={}",
            graph.id,
            bucket.len(),
            atoms.values().map(|v| v.len()).sum::<usize>(),
            ocean_reefs.len(),
        );
        doc_atoms.extend(atoms);
        graph_ids.push(graph.id);
    }

    let report = eval::run_eval_multi_ocean(
        store,
        embedder,
        &graph_ids,
        &doc_atoms,
        golden,
        embedder_name,
        eval::DEFAULT_TOP_K,
    )
    .await?;
    println!();
    print_summary(&report);

    // ── Part 2: dedicated minority-ocean floor-guarantee scenario ───────────
    run_minority_floor_scenario(store, pool, embedder, account_id, user_id, corpus).await?;

    println!(
        "\n[multi-ocean] diagnostic only — no baseline read/write, no pass/fail gate, always \
         exits 0. See backend/eval/README.md (\"Multi-ocean fan-out eval mode\")."
    );
    Ok(())
}

/// Two hand-picked "minority ocean" fixture docs + queries for
/// [`run_minority_floor_scenario`]: each doc lives ALONE in its own tiny
/// minority ocean opposite the full (majority) fixture corpus. Each doc is
/// deliberately worded as a loose PARAPHRASE of its query (shares almost no
/// literal tokens with it — e.g. the query says "approved"/"review", the doc
/// says "sign-off"/"squared away") rather than restating it, so under the
/// hash embedder its own vector_sim/keyword_norm contribution to the blended
/// score is weak, while the much larger majority ocean (53 docs, ~100+ atoms,
/// many of which share SOME of the query's common words purely by volume)
/// fills up the global rerank pool ahead of it. Confirmed empirically (see
/// PR description / this function's printed output): the minority doc lands
/// in the LAST position of the merged set, past the natural 2-ocean cap
/// (`synthesis_context_cap(top_k, 2)` = 30 for `top_k=20`) — i.e. it would
/// NOT have made the cut on score alone. This is exactly the shape the
/// per-ocean floor guarantee (`multi_graph_retrieve`, ADR-0011 A5) exists
/// for: a minority ocean's single best floor-clearing source surviving into
/// the final synthesis set even when raw score alone wouldn't have made it.
const MINORITY_FLOOR_FIXTURES: &[(&str, &str, &str)] = &[
    // (doc_id, minority doc content, query)
    (
        "mo01",
        "a small logging update ships next sprint after a quick sign-off",
        "what got approved in the review for the new pipeline change",
    ),
    (
        "mo02",
        "the outside supplier agreement paperwork is finally squared away",
        "what contract renewal got approved this week",
    ),
];

/// Ingests [`MINORITY_FLOOR_FIXTURES`] one at a time (fresh majority + minority
/// oceans per fixture, so one scenario's ingestion can't influence another's
/// candidate pool), runs its query through `multi_graph_retrieve` across
/// `[majority, minority]`, and prints whether the minority doc's atom survived
/// into the final merged+reranked set and at what rank — the printed evidence
/// for whether the per-ocean floor guarantee actually mattered for it (a
/// low/last rank among a full set means it likely would NOT have made a
/// score-only cap; see [`MULTI_OCEAN_COUNT`]'s doc comment for the cap-scaling
/// mechanics this scenario is built to probe, at `ocean_count = 2` here).
async fn run_minority_floor_scenario(
    store: &PostgresStore,
    pool: &sqlx::PgPool,
    embedder: &dyn EmbedProvider,
    account_id: Uuid,
    user_id: Uuid,
    majority_corpus: &[FixtureDoc],
) -> anyhow::Result<()> {
    println!("\n── minority-ocean floor-guarantee scenario ──");
    println!(
        "(2 oceans per query: 1 majority ocean = the full {}-doc fixture corpus, \
         1 minority ocean holding a single fact alone)",
        majority_corpus.len()
    );

    for (doc_id, content, query) in MINORITY_FLOOR_FIXTURES {
        let majority_graph = store
            .create_graph(
                account_id,
                &format!("{}-majority-{doc_id}", eval::unique_graph_name()),
                "personal",
            )
            .await
            .with_context(|| format!("create majority ocean for {doc_id}"))?;
        let minority_graph = store
            .create_graph(
                account_id,
                &format!("{}-minority-{doc_id}", eval::unique_graph_name()),
                "personal",
            )
            .await
            .with_context(|| format!("create minority ocean for {doc_id}"))?;

        eval::ingest_corpus(
            store,
            pool,
            embedder,
            majority_graph.id,
            user_id,
            majority_corpus,
        )
        .await
        .with_context(|| format!("ingest majority ocean for {doc_id}"))?;

        let minority_doc = FixtureDoc {
            doc_id: doc_id.to_string(),
            source_type: "slack".to_string(),
            title: None,
            date: "2026-07-23".to_string(),
            participants: vec!["Devin".to_string()],
            content: content.to_string(),
        };
        let minority_atoms = eval::ingest_corpus(
            store,
            pool,
            embedder,
            minority_graph.id,
            user_id,
            std::slice::from_ref(&minority_doc),
        )
        .await
        .with_context(|| format!("ingest minority ocean for {doc_id}"))?;

        let graph_ids = [majority_graph.id, minority_graph.id];
        let outcome =
            eval::run_multi_ocean_retrieval(store, embedder, &graph_ids, query, eval::DEFAULT_TOP_K)
                .await
                .with_context(|| format!("multi-ocean retrieval for {doc_id}"))?;

        let target_atoms: HashSet<Uuid> = minority_atoms
            .get(*doc_id)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .collect();
        let rank = outcome
            .ranked_ids
            .iter()
            .position(|id| target_atoms.contains(id));
        let merged_len = outcome.ranked_ids.len();
        // `synthesis_context_cap(top_k, 2)` (`routes/queries.rs`) is
        // `top_k + (top_k / 2)` for exactly 2 oceans — mirrored here as a
        // read-only arithmetic check (not calling the private fn) so this
        // diagnostic can flag "this rank is AT the very edge of (or beyond)
        // where a plain global-score cap would have cut it off, so the
        // per-ocean floor guarantee is the most likely reason it's here at
        // all" without asserting that as fact (only `multi_graph_retrieve`
        // itself knows for certain).
        let naive_cap_two_oceans = eval::DEFAULT_TOP_K + eval::DEFAULT_TOP_K / 2;
        let likely_floor_rescued = rank.is_some_and(|r| r + 1 > naive_cap_two_oceans);

        println!(
            "  query={query:?}\n    minority_doc={doc_id} survived={} rank={} of {merged_len} \
             merged (naive 2-ocean cap={naive_cap_two_oceans}) likely_floor_rescued={} \
             top_score={:.4} abstained={}",
            rank.is_some(),
            rank.map(|r| (r + 1).to_string()).unwrap_or_else(|| "-".to_string()),
            likely_floor_rescued,
            outcome.top_score,
            outcome.abstained,
        );
    }

    Ok(())
}
