//! `judgement_backfill` — the reusable core of the one-shot
//! `backfill_reef_judgement_from_history` binary (judgement-graph resolver
//! slice, PR5; issue #585's PR list). `backend/src/bin/
//! backfill_reef_judgement_from_history.rs` is a thin argument-parse +
//! DSN-resolution wrapper around [`backfill_database`]; every gate, counter
//! and write lives HERE so `backend/tests/judgement_backfill_pg.rs` drives
//! the same code an operator runs, not a re-implementation of it.
//!
//! **RUN ONCE PER DATABASE** — the control plane AND each active tenant
//! database (see `one-shot-backfills-run-per-tenant`: a control-plane-only
//! run reports a falsely-drained queue, because a tenant's `node_merges` /
//! `nodes.body->'not_same'` history lives only in that tenant's own DB).
//! Re-running is nonetheless SAFE and idempotent — see "Idempotency" below.
//!
//! # What it reconciles
//!
//! Everything the judgement graph would have recorded had it existed before
//! `JUDGEMENT_GRAPH_WRITES` was turned on, in two halves:
//!
//! * **`node_merges` → POSITIVE** (`source='backfill'`). Every standing,
//!   not-yet-judged merge means "someone decided these two reefs were the
//!   same thing".
//! * **`nodes.body->'not_same'` → NEGATIVE** (`source='backfill'`). Every
//!   "keep separate" dismissal (CS-422) means the opposite.
//!
//! # THE POSITIVE HALF IS VACUOUS BY CONSTRUCTION — read this before running
//!
//! `store::merge_nodes` HARD-DELETES the duplicate reef when a merge
//! commits (`DELETE FROM nodes WHERE node_id = $dup_id`, `store/mod.rs`'s
//! `merge_nodes_impl`) — `node_merges` is an undo log, not a pair of live
//! endpoints. So on any real database:
//!
//! * a merge that STANDS fails gate 3 (endpoint liveness), because its
//!   `dup_id` no longer exists as a `nodes` row; and
//! * a merge that was UNDONE has both endpoints back (undo recreates the
//!   dup) but fails gate 2, because writing an ACTIVE POSITIVE for an
//!   undone merge would hand the ER worker's pending-positive stream a
//!   durable "merge these" instruction and silently re-merge the pair —
//!   exactly the PR3 F1 bug `unmerge_nodes`'s annul-linkage exists to
//!   prevent.
//!
//! Expected POSITIVE writes on every real DB are therefore **≈ 0**. The half
//! is built anyway, with per-gate counters, so that fact is AUDITABLE from a
//! dry run (`merges_skipped_dead_endpoint` / `merges_skipped_undone` /
//! `merges_skipped_already_judged` tally to `merges_scanned`) instead of
//! being a claim in a PR description. The real payload of this backfill is
//! the NEGATIVE half.
//!
//! # `not_same` is not a table
//!
//! Dismissals are a JSON array on the LOWER node's `nodes.body`
//! (`'not_same'` key), written by three different call sites (the human
//! dismiss route, the auto-dismiss path, and the ER worker's "different"
//! branch) with no per-pair date or attribution anywhere. Hence
//! `source='backfill'` and `created_by = NULL`: the history genuinely does
//! not record who decided, or when. Read back through
//! [`Store::list_merge_dismissals`], which already returns canonicalized
//! pairs.
//!
//! **Loss asymmetry, stated so nobody later reads a low count as a bug:** a
//! dismissal whose LOWER node has since been deleted is gone from the
//! database entirely (the array died with the row that carried it), while
//! one whose HIGHER node was deleted survives in the array and is skipped
//! here at gate 3. Only the second kind is visible in the counters.
//!
//! # Gates (each counted separately — that is what makes a dry run readable)
//!
//! Per candidate pair, in order:
//!
//! 1. **Canonicalize** to `(lo, hi)` with `lo < hi` — the
//!    `reef_judgement_ordered_pair` CHECK, the partial unique index, and
//!    `write_judgement`'s advisory-lock key all depend on one canonical
//!    ordering ([`ordered_pair`]).
//! 2. **Merges only — row state:** `undone_at IS NULL` (see the vacuousness
//!    note above for why an undone merge must never become an active
//!    POSITIVE) **and** `judgement_id IS NULL` (a post-flag merge was
//!    already judged by the ER worker/route that materialized it; there is
//!    nothing to reconcile).
//! 3. **Both endpoints live in `graph_id`** — this backfill runs its OWN
//!    `SELECT count(*) FROM nodes WHERE node_id = ANY($pair) AND graph_id =
//!    $1` rather than letting `write_judgement`'s identical step 1.5 raise
//!    [`StoreError::JudgementEndpointMissing`], because a DRY RUN must count
//!    this skip without writing anything, and because "this pair's endpoints
//!    are gone" is the single most common outcome here (see vacuousness) —
//!    it deserves a counter, not an error.
//! 4. **The pair's active judgement slot is EMPTY** — not merely "holds
//!    nothing live". `write_judgement` only REFUSES an existing
//!    `source='human'` row; a live `llm` / `identifier_auto` row would be
//!    annulled-and-superseded by this backfill's write. That is a clobber of
//!    a fresher, better-evidenced verdict by a legacy one, so this gate — not
//!    `write_judgement` — is what prevents it. An expired-but-unannulled
//!    UNSURE row also occupies the slot and is likewise skipped (see the
//!    expired-UNSURE section below for why that is a deliberate choice rather
//!    than a side effect). This gate is ALSO this backfill's idempotency key:
//!    a re-run finds its own previous row here and skips.
//! 5. **Write** via [`Store::write_judgement`] with `source=Backfill`,
//!    `created_by=None`, `expires_at=None`, `confidence=None`, and evidence
//!    EXACTLY as migration `0139_reef_judgement`'s header documents
//!    ([`merge_evidence`] / [`dismissal_evidence`]). Byte-identical evidence
//!    is load-bearing, not cosmetic: it participates in `write_judgement`'s
//!    true-replay equality check, so a drifted shape turns an idempotent
//!    re-run into a re-adjudication that annuls and supersedes its own
//!    previous row on every pass.
//! 6. **Merges only — back-link:** `UPDATE node_merges SET judgement_id = $1
//!    WHERE merge_id = $2 AND judgement_id IS NULL`, so `unmerge_nodes`'s
//!    annul-linkage (PR3 F1) keeps working for a backfilled merge.
//!
//! # Idempotency, and the one hole gate 4 alone does not close
//!
//! Gate 4 makes a re-run a no-op for both halves. The merge half has one
//! extra failure mode: `write_judgement` (step 5) and the back-link (step 6)
//! are separate transactions, so a process killed between them leaves a
//! judgement with no `node_merges.judgement_id`. A re-run would then find
//! its own active POSITIVE at gate 4 and skip — leaving the back-link
//! permanently missing. So gate 4's "this is my own row" branch REPAIRS the
//! link (counted as `backlinks_repaired`), matching on the row's own
//! `evidence->>'merge_id'` so a different merge over the same pair can never
//! adopt another merge's judgement.
//!
//! One consequence worth knowing before reading a second run's tally: a
//! merge this backfill completed SUCCESSFULLY is reported on the next run as
//! `merges_skipped_already_judged` (gate 2), not as
//! `merges_already_backfilled` — its back-link is set, so gate 2 catches it
//! first, and that counter is truthful (it WAS already judged, by us).
//! `merges_already_backfilled` therefore only ever counts the crash-window
//! case above, which is exactly what makes a non-zero value there
//! informative.
//!
//! # Expired UNSURE rows are SKIPPED, never superseded (founder decision,
//! # 2026-08-12)
//!
//! Gate 4 asks [`Store::get_active_judgement`], whose "live" filter treats an
//! expired-but-unannulled UNSURE row as NOT live. Left at that, such a pair
//! would pass gate 4 and `write_judgement` would annul the expired row
//! (`annulled_by_kind='system'`, `annul_reason='expired'`) and supersede it —
//! the one case where this backfill mutates a pre-existing row. Gate 4
//! therefore does NOT stop at "live": it skips any pair whose active slot is
//! occupied at all, and counts it as `skipped_expired_unsure`.
//!
//! Superseding was rejected for four reasons.
//!
//! 1. `not_same` has THREE indistinguishable writers — a human "keep
//!    separate", the automatic dismissal of low-impact uncertain pairs, and
//!    the resolver's own `"different"` dual-write — and the array records
//!    neither author nor timestamp. A NEGATIVE minted from it may therefore
//!    launder a machine's low-confidence auto-dismissal into a row that
//!    `gate_pair` honours until a prompt-version bump: STRONGER suppression
//!    than the expired UNSURE it replaced, which suppresses nothing and
//!    invites a fresh look.
//! 2. UNSURE is only ever written for pairs that passed the high-impact gate,
//!    so this would close precisely the questions judged consequential, using
//!    evidence whose author cannot be named.
//! 3. An expired UNSURE is itself a finding: the system asked a high-impact
//!    question, queued it for a human, and nobody answered inside the TTL.
//!    That is the review-starvation signal the `/review` merge section exists
//!    to surface. Superseding it tidies away the evidence.
//! 4. The live-vs-expired split is a clock accident. The same pair with the
//!    same evidence would be protected or not depending only on when this
//!    one-shot happens to run.
//!
//! Skipping costs nothing in either direction: `gate_pair` checks dismissals
//! FIRST and returns `Suppress` before it consults any judgement, so every
//! pair this backfill touches is already suppressed by the `not_same` entry
//! that nominated it. The judgement rows this tool writes are an audit
//! record, not a decision. If such a pair is later re-adjudicated, the
//! resolver writes a judgement with honest provenance (model, reason,
//! confidence), which beats a backfill's guess.
//!
//! The counter is retained so a dry run still REPORTS the population — the
//! decision is revisitable if it ever stops being zero, ideally restricted to
//! `not_same` entries attributable to a human.
//!
//! # Plane contract
//!
//! Every call here is the PLAIN (unscoped) `Store` method and raw SQL on a
//! bare pool — see `store/mod.rs`'s judgement-graph PLANE CONTRACT note.
//! That is sanctioned for exactly one reason: this tool connects as the
//! ELEVATED role (`MIGRATOR_DATABASE_URL` on the control plane, the tenant
//! owner credential on a tenant DB), which bypasses the consumer-plane
//! `account_isolation` policy. [`assert_policy_bypass`] proves that at
//! startup and REFUSES otherwise, so a run under the least-privilege
//! `coralswarm_app` role fails immediately with an actionable message
//! instead of erroring one candidate at a time with SQLSTATE 42704 halfway
//! through the reconciliation.

use std::collections::BTreeSet;

use serde_json::{json, Value};
use sqlx::PgPool;
use uuid::Uuid;

use crate::store::{
    JudgementSource, JudgementVerdict, NewJudgement, PostgresStore, ReefJudgement, Store,
    StoreError,
};

/// `evidence->>'backfilled_from'` for the `node_merges` half — migration
/// `0139_reef_judgement`'s documented shape.
pub const BACKFILLED_FROM_NODE_MERGES: &str = "node_merges";
/// `evidence->>'backfilled_from'` for the `not_same` half — migration
/// `0139_reef_judgement`'s documented shape.
pub const BACKFILLED_FROM_NOT_SAME: &str = "not_same";

/// Canonical `(node_a_id, node_b_id)` ordering (`node_a_id < node_b_id`).
///
/// Deliberately a local copy of `PostgresStore::ordered_pair`, which is
/// private to the `store` module: this backfill needs the canonical pair
/// BEFORE it calls anything on `Store` (its own endpoint-liveness query and
/// its dry-run dedupe set are both keyed on it), and widening a `store`
/// internal to `pub` for one caller would be the worse trade. The two must
/// agree — they are four identical lines, and
/// `judgement_backfill_pg.rs`'s `ordered_pair_matches_store_convention`
/// pins that agreement by driving a real `write_judgement` and reading back
/// which id landed in `node_a_id`.
pub fn ordered_pair(a: Uuid, b: Uuid) -> (Uuid, Uuid) {
    if a < b {
        (a, b)
    } else {
        (b, a)
    }
}

/// `reef_judgement.evidence` for a `node_merges`-derived POSITIVE. Shape is
/// fixed by migration `0139_reef_judgement`'s header — see gate 5 in this
/// module's doc for why byte-identity matters.
pub fn merge_evidence(merge_id: Uuid) -> Value {
    json!({ "backfilled_from": BACKFILLED_FROM_NODE_MERGES, "merge_id": merge_id })
}

/// `reef_judgement.evidence` for a `not_same`-derived NEGATIVE. Carries no
/// pair identity of its own: the pair IS the row's `(graph_id, node_a_id,
/// node_b_id)`, and the source array records nothing else (no date, no
/// actor — see this module's doc).
pub fn dismissal_evidence() -> Value {
    json!({ "backfilled_from": BACKFILLED_FROM_NOT_SAME })
}

/// How this run was invoked.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BackfillOpts {
    /// `false` (THE DEFAULT) = dry run: every gate runs for real against the
    /// database, nothing is ever written, and the counters report exactly
    /// what an `execute` run would do — including its skips. See
    /// [`backfill_database`]'s doc for how same-pair candidates are
    /// simulated so a dry run cannot over-count writes.
    pub execute: bool,
    /// Restrict to ONE ocean. Rehearsal/diagnosis aid only — a real run
    /// covers every graph in the database.
    pub only_graph: Option<Uuid>,
}

impl BackfillOpts {
    /// `"execute"` / `"dry-run"` — the `mode=` field of [`summary_line`].
    pub fn mode(&self) -> &'static str {
        if self.execute {
            "execute"
        } else {
            "dry-run"
        }
    }
}

/// Per-gate tallies for ONE database. Every gate in this module's doc has
/// its own field: a dry run is only readable if the founder can see WHY
/// candidates were skipped, not just how many.
///
/// In a dry run the `*_written` / `backlinks_*` fields mean "WOULD write" —
/// [`report_block`] says so in words, and [`summary_line`] carries
/// `mode=dry-run`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BackfillCounts {
    /// Oceans enumerated (after `--only-graph` narrowing, if any).
    pub graphs_scanned: u64,

    // ── node_merges half ────────────────────────────────────────────────
    /// `node_merges` rows examined.
    pub merges_scanned: u64,
    /// Gate 2: `undone_at IS NOT NULL` — an active backfilled POSITIVE would
    /// make the ER worker re-merge the pair.
    pub merges_skipped_undone: u64,
    /// Gate 2: `judgement_id IS NOT NULL` — post-flag, already judged.
    pub merges_skipped_already_judged: u64,
    /// Gate 3: an endpoint no longer resolves to a live node in the graph.
    /// THE EXPECTED OUTCOME for every standing merge (see vacuousness).
    pub merges_skipped_dead_endpoint: u64,
    /// Gate 4: a live judgement from another source (or another verdict)
    /// holds the pair — never clobbered.
    pub merges_skipped_live_judgement: u64,
    /// Gate 4: a live POSITIVE this backfill itself wrote for this exact
    /// `merge_id` (idempotent re-run).
    pub merges_already_backfilled: u64,
    /// Gate 5 writes.
    pub positives_written: u64,
    /// Gate 6 back-links written alongside a fresh POSITIVE.
    pub backlinks_written: u64,
    /// Gate 6 back-links repaired on a re-run (a previous run died between
    /// gate 5 and gate 6).
    pub backlinks_repaired: u64,

    // ── not_same half ───────────────────────────────────────────────────
    /// DISTINCT canonical dismissal pairs examined (deduped — see
    /// [`backfill_database`]).
    pub dismissals_scanned: u64,
    /// Gate 3: an endpoint no longer resolves (the HIGHER node was deleted;
    /// lower-node deletions are invisible — see the loss asymmetry note).
    pub dismissals_skipped_dead_endpoint: u64,
    /// Gate 4: a live judgement from another source holds the pair.
    pub dismissals_skipped_live_judgement: u64,
    /// Gate 4: a live NEGATIVE this backfill itself wrote (idempotent
    /// re-run).
    pub dismissals_already_backfilled: u64,
    /// Gate 5 writes.
    pub negatives_written: u64,

    // ── shared ──────────────────────────────────────────────────────────
    /// Subset of the writes above whose pair was held by an
    /// expired-but-unannulled UNSURE row occupying the pair's active slot.
    /// Skipped, never superseded — see this module's doc for why. Zero on
    /// every plane until an UNSURE row outlives its TTL, so a non-zero value
    /// is the signal to revisit that decision.
    pub skipped_expired_unsure: u64,
    /// `write_judgement` returned [`StoreError::HumanJudgementExists`]
    /// despite gate 4 — i.e. a human ruled on the pair between gate 4 and
    /// the write. Nothing written; not an error.
    pub raced_human_judgement: u64,
    /// `write_judgement` returned [`StoreError::JudgementEndpointMissing`]
    /// despite gate 3 — an endpoint was deleted between the two. Nothing
    /// written; not an error.
    pub raced_endpoint_missing: u64,
    /// Unexpected per-candidate failures. Non-zero makes the binary exit
    /// non-zero; the run still continues so one bad pair cannot strand the
    /// rest of the reconciliation.
    pub errors: u64,
}

impl BackfillCounts {
    /// Field-wise accumulate — the binary sums per-database counts into one
    /// grand total.
    pub fn add(&mut self, other: &BackfillCounts) {
        self.graphs_scanned += other.graphs_scanned;
        self.merges_scanned += other.merges_scanned;
        self.merges_skipped_undone += other.merges_skipped_undone;
        self.merges_skipped_already_judged += other.merges_skipped_already_judged;
        self.merges_skipped_dead_endpoint += other.merges_skipped_dead_endpoint;
        self.merges_skipped_live_judgement += other.merges_skipped_live_judgement;
        self.merges_already_backfilled += other.merges_already_backfilled;
        self.positives_written += other.positives_written;
        self.backlinks_written += other.backlinks_written;
        self.backlinks_repaired += other.backlinks_repaired;
        self.dismissals_scanned += other.dismissals_scanned;
        self.dismissals_skipped_dead_endpoint += other.dismissals_skipped_dead_endpoint;
        self.dismissals_skipped_live_judgement += other.dismissals_skipped_live_judgement;
        self.dismissals_already_backfilled += other.dismissals_already_backfilled;
        self.negatives_written += other.negatives_written;
        self.skipped_expired_unsure += other.skipped_expired_unsure;
        self.raced_human_judgement += other.raced_human_judgement;
        self.raced_endpoint_missing += other.raced_endpoint_missing;
        self.errors += other.errors;
    }

    /// Total `reef_judgement` rows written (or, in a dry run, that would be).
    pub fn writes(&self) -> u64 {
        self.positives_written + self.negatives_written
    }

    /// Every `node_merges` row is accounted for by exactly one gate. Used by
    /// the unit tests and worth stating: a tally that doesn't balance means
    /// a candidate fell through a gate without being counted.
    pub fn merges_accounted(&self) -> u64 {
        self.merges_skipped_undone
            + self.merges_skipped_already_judged
            + self.merges_skipped_dead_endpoint
            + self.merges_skipped_live_judgement
            + self.merges_already_backfilled
            + self.positives_written
            + self.raced_human_judgement
            + self.raced_endpoint_missing
            + self.errors
    }

    /// Companion to [`Self::merges_accounted`] for the dismissal half.
    /// `errors` is shared between the halves, so it is deliberately NOT
    /// counted here — the two balance checks are only exact when `errors ==
    /// 0`, which is the case every test asserts.
    pub fn dismissals_accounted(&self) -> u64 {
        self.dismissals_skipped_dead_endpoint
            + self.dismissals_skipped_live_judgement
            + self.dismissals_already_backfilled
            + self.negatives_written
            + self.raced_human_judgement
            + self.raced_endpoint_missing
    }
}

/// Anything that stops a whole database being processed. Per-CANDIDATE
/// problems never surface here — they increment [`BackfillCounts::errors`]
/// and the run continues, because one unreadable pair must not strand the
/// rest of a one-shot reconciliation.
#[derive(Debug, thiserror::Error)]
pub enum BackfillError {
    /// [`assert_policy_bypass`] refused: this connection's role does not
    /// bypass RLS on `reef_judgement`, or the table isn't there at all.
    #[error("{0}")]
    Preflight(String),
    #[error("store: {0}")]
    Store(#[from] StoreError),
    #[error("database: {0}")]
    Db(#[from] sqlx::Error),
}

/// Fail closed unless this connection can actually write `reef_judgement`
/// without an `app.account_id` scope.
///
/// Everything in this module is a PLAIN (unscoped) `Store` call or raw SQL
/// on a bare pool — sanctioned ONLY because a one-shot runs as the elevated
/// role. Under the least-privilege `coralswarm_app` role the consumer-plane
/// `account_isolation` policy's `current_setting('app.account_id')`
/// predicate raises SQLSTATE 42704 the first time it is evaluated, which
/// without this preflight would surface as a pile of per-candidate errors
/// partway through a run (and, in a DRY run against an empty-ish graph,
/// might not surface at all — a clean rehearsal followed by a failing real
/// run is the worst possible outcome for a tool whose whole contract is
/// "read the dry run first").
///
/// RLS does not apply when the role is a superuser, has `BYPASSRLS`, or owns
/// the table and the table is not `FORCE ROW LEVEL SECURITY` — checked here
/// in exactly that form against the catalog rather than inferred from the
/// DSN's username.
pub async fn assert_policy_bypass(pool: &PgPool) -> Result<(), BackfillError> {
    let table: Option<String> = sqlx::query_scalar("SELECT to_regclass('reef_judgement')::text")
        .fetch_one(pool)
        .await?;
    if table.is_none() {
        return Err(BackfillError::Preflight(
            "no `reef_judgement` table in this database — migration 0139 (control tree) / 0046 \
             (tenant tree) has not been applied here. Run migrate_control / migrate_tenants first."
                .to_string(),
        ));
    }

    let (bypasses, is_owner, forced): (bool, bool, bool) = sqlx::query_as(
        "SELECT \
            (SELECT rolsuper OR rolbypassrls FROM pg_roles WHERE rolname = current_user), \
            c.relowner = (SELECT oid FROM pg_roles WHERE rolname = current_user), \
            c.relforcerowsecurity \
         FROM pg_class c WHERE c.oid = 'reef_judgement'::regclass",
    )
    .fetch_one(pool)
    .await?;

    if bypasses || (is_owner && !forced) {
        return Ok(());
    }
    Err(BackfillError::Preflight(format!(
        "REFUSING: this connection's role does not bypass row-level security on `reef_judgement` \
         (superuser/BYPASSRLS = {bypasses}, owns the table = {is_owner}, FORCE ROW LEVEL SECURITY \
         = {forced}). Every write here is a PLAIN, unscoped `Store` call, which under the \
         least-privilege `coralswarm_app` role raises `unrecognized configuration parameter \
         \"app.account_id\"`. Point MIGRATOR_DATABASE_URL (control plane) or TENANT_DB_<SLUG> \
         (tenant plane) at the ELEVATED owner credential and re-run."
    )))
}

/// What occupies a pair's active `reef_judgement` slot, as gate 4 sees it.
#[derive(Debug)]
enum PairState {
    /// Nothing at all — a clean insert.
    Free,
    /// An un-annulled but EXPIRED UNSURE row holds the slot.
    /// [`Store::get_active_judgement`] does not consider it live, so without
    /// this variant the pair would look `Free` and `write_judgement` would
    /// annul (`'expired'`) and supersede the row. Gate 4 skips it instead;
    /// see this module's doc for the reasoning.
    ExpiredUnsure,
    /// A LIVE active row. Never clobbered.
    Live(Box<ReefJudgement>),
}

/// Gate 4's read. Two queries, and only on the `None` path: the "live"
/// definition stays owned by [`Store::get_active_judgement`] (so it can
/// never drift from what the rest of the slice means by live), and the
/// second query exists solely to tell [`PairState::Free`] apart from
/// [`PairState::ExpiredUnsure`] — a distinction `get_active_judgement`
/// cannot express, and the one that decides whether this tool writes or
/// steps back. Only `Free` is written; both occupied states are skipped.
async fn pair_state(
    store: &PostgresStore,
    pool: &PgPool,
    graph_id: Uuid,
    lo: Uuid,
    hi: Uuid,
) -> Result<PairState, BackfillError> {
    if let Some(live) = store.get_active_judgement(graph_id, lo, hi).await? {
        return Ok(PairState::Live(Box::new(live)));
    }
    // Not live. Is the active slot nonetheless OCCUPIED by an expired
    // (necessarily UNSURE — `reef_judgement_expiry_shape`) row? The partial
    // unique index keys on `annulled_at` alone, so such a row still holds
    // the slot until `write_judgement` annuls it.
    let occupied: Option<(bool,)> = sqlx::query_as(
        "SELECT true FROM reef_judgement \
         WHERE graph_id = $1 AND node_a_id = $2 AND node_b_id = $3 AND annulled_at IS NULL",
    )
    .bind(graph_id)
    .bind(lo)
    .bind(hi)
    .fetch_optional(pool)
    .await?;
    Ok(if occupied.is_some() {
        PairState::ExpiredUnsure
    } else {
        PairState::Free
    })
}

/// Gate 3 — the same `count(*) = 2` predicate `write_judgement_tx` applies
/// as its own step 1.5, run here first so a dry run can COUNT the skip and
/// a real run reports it as an outcome rather than an error. A degenerate
/// `lo == hi` pair (never observed; it would violate
/// `reef_judgement_ordered_pair` anyway) counts 1, so it lands in the same
/// skip bucket rather than reaching a write.
async fn endpoints_live(
    pool: &PgPool,
    graph_id: Uuid,
    lo: Uuid,
    hi: Uuid,
) -> Result<bool, BackfillError> {
    let pair: [Uuid; 2] = [lo, hi];
    let live: i64 =
        sqlx::query_scalar("SELECT count(*) FROM nodes WHERE node_id = ANY($1) AND graph_id = $2")
            .bind(pair.as_slice())
            .bind(graph_id)
            .fetch_one(pool)
            .await?;
    Ok(live == 2)
}

/// Is this live row one THIS backfill wrote for THIS merge? Matching on the
/// row's own `evidence->>'merge_id'` (not merely on `source='backfill'`) is
/// what keeps a second merge over the same pair from adopting the first
/// merge's judgement as its back-link.
fn is_our_merge_row(j: &ReefJudgement, merge_id: Uuid) -> bool {
    let want = merge_id.to_string();
    j.source == JudgementSource::Backfill.as_str()
        && j.verdict == JudgementVerdict::Positive.as_str()
        && j.evidence.get("backfilled_from").and_then(Value::as_str)
            == Some(BACKFILLED_FROM_NODE_MERGES)
        && j.evidence.get("merge_id").and_then(Value::as_str) == Some(want.as_str())
}

/// Is this live row one THIS backfill wrote for a `not_same` dismissal? The
/// pair is the row's own key, so `backfilled_from` + verdict + source is the
/// whole identity here (see [`dismissal_evidence`]).
fn is_our_dismissal_row(j: &ReefJudgement) -> bool {
    j.source == JudgementSource::Backfill.as_str()
        && j.verdict == JudgementVerdict::Negative.as_str()
        && j.evidence.get("backfilled_from").and_then(Value::as_str) == Some(BACKFILLED_FROM_NOT_SAME)
}

/// One `node_merges` row, reduced to what the gates need. Gate 2's two
/// predicates are read as booleans rather than filtered in SQL so each can
/// be COUNTED separately — a post-flag merge and an undone merge are very
/// different facts about a database, and a `WHERE` clause would render both
/// invisible.
#[derive(Debug, sqlx::FromRow)]
struct MergeRow {
    merge_id: Uuid,
    keep_id: Uuid,
    dup_id: Uuid,
    undone: bool,
    judged: bool,
}

/// The `node_merges` → POSITIVE half for one ocean. See this module's doc
/// for why this is expected to write nothing.
async fn backfill_merges_for_graph(
    pool: &PgPool,
    store: &PostgresStore,
    graph_id: Uuid,
    opts: &BackfillOpts,
    counts: &mut BackfillCounts,
    planned: &mut BTreeSet<(Uuid, Uuid)>,
) -> Result<(), BackfillError> {
    let rows: Vec<MergeRow> = sqlx::query_as(
        "SELECT merge_id, keep_id, dup_id, (undone_at IS NOT NULL) AS undone, \
                (judgement_id IS NOT NULL) AS judged \
         FROM node_merges WHERE graph_id = $1 ORDER BY created_at, merge_id",
    )
    .bind(graph_id)
    .fetch_all(pool)
    .await?;

    for row in rows {
        counts.merges_scanned += 1;

        // ── Gate 2: row state (no queries — cheapest gate, and the one that
        // attributes a post-flag merge correctly instead of burying it in
        // the dead-endpoint bucket it would ALSO land in).
        if row.undone {
            counts.merges_skipped_undone += 1;
            continue;
        }
        if row.judged {
            counts.merges_skipped_already_judged += 1;
            continue;
        }

        // ── Gate 1: canonicalize.
        let (lo, hi) = ordered_pair(row.keep_id, row.dup_id);

        // ── Gate 3: endpoint liveness.
        match endpoints_live(pool, graph_id, lo, hi).await {
            Ok(false) => {
                counts.merges_skipped_dead_endpoint += 1;
                continue;
            }
            Ok(true) => {}
            Err(e) => {
                counts.errors += 1;
                tracing::warn!(
                    merge_id = %row.merge_id, graph_id = %graph_id, error = %e,
                    "backfill: endpoint-liveness query failed for a node_merges candidate"
                );
                continue;
            }
        }

        // ── Gate 4: no live judgement (and this backfill's own idempotency
        // key).
        //
        // The dry-run branch simulates what THIS run has already decided to
        // write, so a rehearsal's tally equals the execute run's instead of
        // double-counting a duplicate candidate. It counts
        // `merges_skipped_live_judgement`, NOT `merges_already_backfilled`,
        // and the distinction is exact rather than cosmetic: a `planned` pair
        // was necessarily planned by a DIFFERENT `node_merges` row, so the
        // execute run's `is_our_merge_row` check (which matches on
        // `evidence->>'merge_id'`) rejects it and falls to the live-judgement
        // skip. Two standing, non-undone merges over one pair should not
        // exist — the first merge deletes the dup — but this half's whole job
        // is to be honest about shapes that "should not exist".
        if !opts.execute && planned.contains(&(lo, hi)) {
            counts.merges_skipped_live_judgement += 1;
            continue;
        }
        match pair_state(store, pool, graph_id, lo, hi).await {
            Ok(PairState::Free) => {}
            Ok(PairState::ExpiredUnsure) => {
                counts.skipped_expired_unsure += 1;
                continue;
            }
            Err(e) => {
                counts.errors += 1;
                tracing::warn!(
                    merge_id = %row.merge_id, graph_id = %graph_id, error = %e,
                    "backfill: active-judgement read failed for a node_merges candidate"
                );
                continue;
            }
            Ok(PairState::Live(live)) if is_our_merge_row(&live, row.merge_id) => {
                // A previous run wrote this row. It reached gate 2 with
                // `judgement_id IS NULL`, so that run died before its
                // back-link — repair it (see "Idempotency" in this module's
                // doc).
                counts.merges_already_backfilled += 1;
                if opts.execute {
                    match link_merge(pool, row.merge_id, live.judgement_id).await {
                        Ok(1) => counts.backlinks_repaired += 1,
                        Ok(_) => {
                            counts.errors += 1;
                            tracing::warn!(
                                merge_id = %row.merge_id,
                                judgement_id = %live.judgement_id,
                                "backfill: back-link repair matched no row (concurrent writer?)"
                            );
                        }
                        Err(e) => {
                            counts.errors += 1;
                            tracing::warn!(
                                merge_id = %row.merge_id,
                                judgement_id = %live.judgement_id, error = %e,
                                "backfill: back-link repair failed"
                            );
                        }
                    }
                } else {
                    counts.backlinks_repaired += 1;
                }
                continue;
            }
            Ok(PairState::Live(_)) => {
                counts.merges_skipped_live_judgement += 1;
                continue;
            }
        };

        // ── Gate 5: write.
        if !opts.execute {
            counts.positives_written += 1;
            counts.backlinks_written += 1;
            planned.insert((lo, hi));
            continue;
        }
        let judgement_id = match store
            .write_judgement(NewJudgement {
                graph_id,
                node_a_id: lo,
                node_b_id: hi,
                verdict: JudgementVerdict::Positive,
                source: JudgementSource::Backfill,
                confidence: None,
                model_id: None,
                prompt_version: None,
                evidence: merge_evidence(row.merge_id),
                created_by: None,
                expires_at: None,
            })
            .await
        {
            Ok(id) => id,
            Err(StoreError::HumanJudgementExists { .. }) => {
                counts.raced_human_judgement += 1;
                continue;
            }
            Err(StoreError::JudgementEndpointMissing { .. }) => {
                counts.raced_endpoint_missing += 1;
                continue;
            }
            Err(e) => {
                counts.errors += 1;
                tracing::warn!(
                    merge_id = %row.merge_id, graph_id = %graph_id, error = %e,
                    "backfill: write_judgement failed for a node_merges candidate"
                );
                continue;
            }
        };
        counts.positives_written += 1;

        // ── Gate 6: back-link.
        match link_merge(pool, row.merge_id, judgement_id).await {
            Ok(1) => counts.backlinks_written += 1,
            Ok(_) => {
                counts.errors += 1;
                tracing::warn!(
                    merge_id = %row.merge_id, judgement_id = %judgement_id,
                    "backfill: node_merges back-link matched no row (concurrent writer?) — the \
                     judgement stands, but unmerge_nodes will not annul it"
                );
            }
            Err(e) => {
                counts.errors += 1;
                tracing::warn!(
                    merge_id = %row.merge_id, judgement_id = %judgement_id, error = %e,
                    "backfill: node_merges back-link failed — the judgement stands, but \
                     unmerge_nodes will not annul it"
                );
            }
        }
    }
    Ok(())
}

/// Gate 6's statement. `AND judgement_id IS NULL` makes it safe to re-run
/// and impossible to overwrite a link some other writer established.
async fn link_merge(pool: &PgPool, merge_id: Uuid, judgement_id: Uuid) -> Result<u64, sqlx::Error> {
    Ok(sqlx::query(
        "UPDATE node_merges SET judgement_id = $1 WHERE merge_id = $2 AND judgement_id IS NULL",
    )
    .bind(judgement_id)
    .bind(merge_id)
    .execute(pool)
    .await?
    .rows_affected())
}

/// The `not_same` → NEGATIVE half for one ocean — the half that actually
/// carries payload.
async fn backfill_dismissals_for_graph(
    pool: &PgPool,
    store: &PostgresStore,
    graph_id: Uuid,
    opts: &BackfillOpts,
    counts: &mut BackfillCounts,
    planned: &mut BTreeSet<(Uuid, Uuid)>,
) -> Result<(), BackfillError> {
    // DEDUPED. `list_merge_dismissals` reads a raw JSON array per node and
    // canonicalizes; nothing in the schema stops the same partner appearing
    // twice in one array, or reciprocal entries on BOTH nodes (the writers
    // store it once, on the lower node, but a legacy/synced row is not
    // constrained to that). Without the dedupe an execute run would write
    // once and skip the duplicate at gate 4, while a dry run counted two
    // writes — i.e. the rehearsal would not match the real thing.
    let pairs: BTreeSet<(Uuid, Uuid)> = store
        .list_merge_dismissals(graph_id)
        .await?
        .into_iter()
        .map(|(a, b)| ordered_pair(a, b))
        .collect();

    for (lo, hi) in pairs {
        counts.dismissals_scanned += 1;

        // ── Gate 3: endpoint liveness.
        match endpoints_live(pool, graph_id, lo, hi).await {
            Ok(false) => {
                counts.dismissals_skipped_dead_endpoint += 1;
                continue;
            }
            Ok(true) => {}
            Err(e) => {
                counts.errors += 1;
                tracing::warn!(
                    graph_id = %graph_id, node_a_id = %lo, node_b_id = %hi, error = %e,
                    "backfill: endpoint-liveness query failed for a not_same candidate"
                );
                continue;
            }
        }

        // ── Gate 4 (+ the dry-run simulation of this same run's own
        // earlier writes — the merges half runs FIRST, so a pair that is
        // both merged and dismissed is correctly reported as skipped here
        // rather than written twice).
        if !opts.execute && planned.contains(&(lo, hi)) {
            counts.dismissals_skipped_live_judgement += 1;
            continue;
        }
        match pair_state(store, pool, graph_id, lo, hi).await {
            Ok(PairState::Free) => {}
            Ok(PairState::ExpiredUnsure) => {
                counts.skipped_expired_unsure += 1;
                continue;
            }
            Err(e) => {
                counts.errors += 1;
                tracing::warn!(
                    graph_id = %graph_id, node_a_id = %lo, node_b_id = %hi, error = %e,
                    "backfill: active-judgement read failed for a not_same candidate"
                );
                continue;
            }
            Ok(PairState::Live(live)) if is_our_dismissal_row(&live) => {
                counts.dismissals_already_backfilled += 1;
                continue;
            }
            Ok(PairState::Live(_)) => {
                counts.dismissals_skipped_live_judgement += 1;
                continue;
            }
        }

        // ── Gate 5: write.
        if !opts.execute {
            counts.negatives_written += 1;
            planned.insert((lo, hi));
            continue;
        }
        match store
            .write_judgement(NewJudgement {
                graph_id,
                node_a_id: lo,
                node_b_id: hi,
                verdict: JudgementVerdict::Negative,
                source: JudgementSource::Backfill,
                confidence: None,
                model_id: None,
                prompt_version: None,
                evidence: dismissal_evidence(),
                created_by: None,
                expires_at: None,
            })
            .await
        {
            Ok(_) => counts.negatives_written += 1,
            Err(StoreError::HumanJudgementExists { .. }) => counts.raced_human_judgement += 1,
            Err(StoreError::JudgementEndpointMissing { .. }) => counts.raced_endpoint_missing += 1,
            Err(e) => {
                counts.errors += 1;
                tracing::warn!(
                    graph_id = %graph_id, node_a_id = %lo, node_b_id = %hi, error = %e,
                    "backfill: write_judgement failed for a not_same candidate"
                );
            }
        }
    }
    Ok(())
}

/// Reconcile ONE already-connected database (control plane or one tenant).
///
/// Runs [`assert_policy_bypass`] first, then walks every ocean in the
/// database (or just `opts.only_graph`), doing the `node_merges` half before
/// the `not_same` half so a pair carrying both kinds of history resolves to
/// the merge's POSITIVE rather than racing itself.
///
/// **Dry run (`opts.execute == false`) writes NOTHING** — no
/// `reef_judgement` row, no `node_merges.judgement_id` — and its counters
/// predict an execute run exactly: candidates this same run already decided
/// to write are simulated at gate 4 via a per-graph `planned` set, so a
/// duplicate candidate is reported as a skip in the rehearsal exactly as it
/// would be skipped for real.
pub async fn backfill_database(
    pool: &PgPool,
    opts: &BackfillOpts,
) -> Result<BackfillCounts, BackfillError> {
    assert_policy_bypass(pool).await?;

    let graph_ids: Vec<Uuid> = match opts.only_graph {
        Some(g) => sqlx::query_scalar("SELECT id FROM graphs WHERE id = $1")
            .bind(g)
            .fetch_all(pool)
            .await?,
        None => sqlx::query_scalar("SELECT id FROM graphs ORDER BY created_at, id")
            .fetch_all(pool)
            .await?,
    };

    let store = PostgresStore::new(pool.clone());
    let mut counts = BackfillCounts::default();
    for graph_id in graph_ids {
        counts.graphs_scanned += 1;
        // Per-graph: pair ids are only unique within an ocean's own
        // judgement slot, and the set stays small.
        let mut planned: BTreeSet<(Uuid, Uuid)> = BTreeSet::new();
        backfill_merges_for_graph(pool, &store, graph_id, opts, &mut counts, &mut planned).await?;
        backfill_dismissals_for_graph(pool, &store, graph_id, opts, &mut counts, &mut planned)
            .await?;
    }
    Ok(counts)
}

/// ONE machine-greppable line, always printed to stderr per database (and
/// once more for the grand total) — the `migrate_tenants` convention, and
/// for the same reason: a run whose only record is an exit code nobody reads
/// is a run nobody can audit afterwards.
pub fn summary_line(label: &str, opts: &BackfillOpts, c: &BackfillCounts) -> String {
    format!(
        "summary: backfill_reef_judgement db={label} mode={} graphs={} \
         | merges: scanned={} undone={} already_judged={} dead_endpoint={} live_judgement={} \
         already_backfilled={} positives_written={} backlinks_written={} backlinks_repaired={} \
         | not_same: scanned={} dead_endpoint={} live_judgement={} already_backfilled={} \
         negatives_written={} \
         | skipped_expired_unsure={} raced_human={} raced_endpoint_missing={} errors={}",
        opts.mode(),
        c.graphs_scanned,
        c.merges_scanned,
        c.merges_skipped_undone,
        c.merges_skipped_already_judged,
        c.merges_skipped_dead_endpoint,
        c.merges_skipped_live_judgement,
        c.merges_already_backfilled,
        c.positives_written,
        c.backlinks_written,
        c.backlinks_repaired,
        c.dismissals_scanned,
        c.dismissals_skipped_dead_endpoint,
        c.dismissals_skipped_live_judgement,
        c.dismissals_already_backfilled,
        c.negatives_written,
        c.skipped_expired_unsure,
        c.raced_human_judgement,
        c.raced_endpoint_missing,
        c.errors,
    )
}

/// The human-readable block printed to stdout — what the founder actually
/// reads before authorising `--execute`. Says "would write" in a dry run so
/// no one mistakes a rehearsal's tally for work already done.
pub fn report_block(label: &str, opts: &BackfillOpts, c: &BackfillCounts) -> String {
    // "wrote" / "WOULD write" — the ONE word that says whether this tally is
    // a record or a forecast, so it can never be misread at a glance. The
    // two spellings are different lengths, which is why every label below is
    // padded to a fixed column ([`LABEL_W`]) rather than hand-spaced: the
    // numbers must line up in BOTH modes, since a founder reads the dry run
    // and the execute run side by side.
    const LABEL_W: usize = 42;
    fn line(s: &mut String, label: &str, value: String) {
        s.push_str(&format!("    {label:<LABEL_W$}{value}\n"));
    }
    let verb = if opts.execute {
        "wrote"
    } else {
        "WOULD write"
    };

    let mut s = format!(
        "-- {label} ({mode}) -- {graphs} ocean(s) scanned\n\
         \x20 node_merges -> POSITIVE (expected ~0: merge_nodes hard-deletes the duplicate reef)\n",
        mode = opts.mode(),
        graphs = c.graphs_scanned
    );
    line(&mut s, "scanned:", c.merges_scanned.to_string());
    line(
        &mut s,
        "skipped, undone merge:",
        c.merges_skipped_undone.to_string(),
    );
    line(
        &mut s,
        "skipped, already judged:",
        c.merges_skipped_already_judged.to_string(),
    );
    line(
        &mut s,
        "skipped, dead endpoint:",
        c.merges_skipped_dead_endpoint.to_string(),
    );
    line(
        &mut s,
        "skipped, live judgement:",
        c.merges_skipped_live_judgement.to_string(),
    );
    line(
        &mut s,
        "already backfilled (back-links repaired):",
        format!("{} ({})", c.merges_already_backfilled, c.backlinks_repaired),
    );
    line(
        &mut s,
        &format!("{verb} POSITIVE (back-links):"),
        format!("{} ({})", c.positives_written, c.backlinks_written),
    );

    s.push_str("  not_same -> NEGATIVE (the real payload of this backfill)\n");
    line(&mut s, "distinct pairs:", c.dismissals_scanned.to_string());
    line(
        &mut s,
        "skipped, dead endpoint:",
        c.dismissals_skipped_dead_endpoint.to_string(),
    );
    line(
        &mut s,
        "skipped, live judgement:",
        c.dismissals_skipped_live_judgement.to_string(),
    );
    line(
        &mut s,
        "already backfilled:",
        c.dismissals_already_backfilled.to_string(),
    );
    line(
        &mut s,
        &format!("{verb} NEGATIVE:"),
        c.negatives_written.to_string(),
    );

    s.push_str("  outcomes shared by both halves\n");
    line(
        &mut s,
        "expired-UNSURE pairs skipped:",
        c.skipped_expired_unsure.to_string(),
    );
    line(
        &mut s,
        "raced (human verdict / endpoint deleted):",
        format!("{} / {}", c.raced_human_judgement, c.raced_endpoint_missing),
    );
    line(&mut s, "errors:", c.errors.to_string());
    if !opts.execute {
        s.push_str("  DRY RUN -- nothing was written. Re-run with --execute to apply.\n");
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> BackfillCounts {
        BackfillCounts {
            graphs_scanned: 2,
            merges_scanned: 7,
            merges_skipped_undone: 1,
            merges_skipped_already_judged: 2,
            merges_skipped_dead_endpoint: 3,
            merges_skipped_live_judgement: 0,
            merges_already_backfilled: 0,
            positives_written: 1,
            backlinks_written: 1,
            backlinks_repaired: 0,
            dismissals_scanned: 5,
            dismissals_skipped_dead_endpoint: 1,
            dismissals_skipped_live_judgement: 1,
            dismissals_already_backfilled: 0,
            negatives_written: 3,
            skipped_expired_unsure: 1,
            raced_human_judgement: 0,
            raced_endpoint_missing: 0,
            errors: 0,
        }
    }

    /// The evidence shapes are a CONTRACT with migration 0139's header and
    /// with `write_judgement`'s true-replay equality — pinned literally so a
    /// drive-by rename shows up as a failing test rather than as a backfill
    /// that re-adjudicates its own rows on every re-run.
    #[test]
    fn evidence_shapes_match_migration_0139() {
        let merge_id = Uuid::parse_str("11111111-2222-3333-4444-555555555555").unwrap();
        assert_eq!(
            merge_evidence(merge_id),
            json!({"backfilled_from": "node_merges", "merge_id": "11111111-2222-3333-4444-555555555555"})
        );
        assert_eq!(dismissal_evidence(), json!({"backfilled_from": "not_same"}));
    }

    #[test]
    fn ordered_pair_is_canonical_either_way() {
        let lo = Uuid::parse_str("00000000-0000-0000-0000-00000000000a").unwrap();
        let hi = Uuid::parse_str("00000000-0000-0000-0000-00000000000b").unwrap();
        assert_eq!(ordered_pair(lo, hi), (lo, hi));
        assert_eq!(ordered_pair(hi, lo), (lo, hi));
    }

    fn judgement(source: &str, verdict: &str, evidence: Value) -> ReefJudgement {
        ReefJudgement {
            judgement_id: Uuid::new_v4(),
            graph_id: Uuid::new_v4(),
            node_a_id: Uuid::new_v4(),
            node_b_id: Uuid::new_v4(),
            verdict: verdict.to_string(),
            source: source.to_string(),
            confidence: None,
            model_id: None,
            prompt_version: None,
            evidence,
            created_at: chrono::Utc::now(),
            created_by: None,
            expires_at: None,
            annulled_at: None,
            annulled_by: None,
            annulled_by_kind: None,
            annul_reason: None,
            superseded_by: None,
        }
    }

    #[test]
    fn our_merge_row_requires_source_verdict_and_this_merge_id() {
        let merge_id = Uuid::new_v4();
        let other = Uuid::new_v4();
        assert!(is_our_merge_row(
            &judgement("backfill", "positive", merge_evidence(merge_id)),
            merge_id
        ));
        // A DIFFERENT merge over the same pair must not adopt this row.
        assert!(!is_our_merge_row(
            &judgement("backfill", "positive", merge_evidence(other)),
            merge_id
        ));
        // Not ours: an llm POSITIVE, or a backfilled NEGATIVE.
        assert!(!is_our_merge_row(
            &judgement("llm", "positive", merge_evidence(merge_id)),
            merge_id
        ));
        assert!(!is_our_merge_row(
            &judgement("backfill", "negative", merge_evidence(merge_id)),
            merge_id
        ));
        // Right source/verdict, wrong provenance key.
        assert!(!is_our_merge_row(
            &judgement("backfill", "positive", dismissal_evidence()),
            merge_id
        ));
    }

    #[test]
    fn our_dismissal_row_requires_source_verdict_and_provenance() {
        assert!(is_our_dismissal_row(&judgement(
            "backfill",
            "negative",
            dismissal_evidence()
        )));
        assert!(!is_our_dismissal_row(&judgement(
            "human",
            "negative",
            dismissal_evidence()
        )));
        assert!(!is_our_dismissal_row(&judgement(
            "backfill",
            "unsure",
            dismissal_evidence()
        )));
        assert!(!is_our_dismissal_row(&judgement(
            "backfill",
            "negative",
            merge_evidence(Uuid::new_v4())
        )));
    }

    #[test]
    fn counts_add_is_field_wise_and_totals_balance() {
        let mut total = BackfillCounts::default();
        total.add(&sample());
        total.add(&sample());
        assert_eq!(total.graphs_scanned, 4);
        assert_eq!(total.merges_scanned, 14);
        assert_eq!(total.negatives_written, 6);
        assert_eq!(total.writes(), 8);
        assert_eq!(total.errors, 0);
        // Every candidate is accounted for by exactly one gate.
        let one = sample();
        assert_eq!(one.merges_accounted(), one.merges_scanned);
        assert_eq!(one.dismissals_accounted(), one.dismissals_scanned);
    }

    #[test]
    fn summary_line_is_one_line_and_carries_every_counter() {
        let c = sample();
        for execute in [false, true] {
            let opts = BackfillOpts {
                execute,
                only_graph: None,
            };
            let line = summary_line("control", &opts, &c);
            assert!(line.starts_with("summary: backfill_reef_judgement db=control "));
            assert!(!line.contains('\n'), "summary must be greppable as ONE line");
            assert!(line.contains(if execute {
                "mode=execute"
            } else {
                "mode=dry-run"
            }));
            for field in [
                "graphs=2",
                "scanned=7",
                "undone=1",
                "already_judged=2",
                "dead_endpoint=3",
                "positives_written=1",
                "backlinks_written=1",
                "backlinks_repaired=0",
                "negatives_written=3",
                "skipped_expired_unsure=1",
                "errors=0",
            ] {
                assert!(line.contains(field), "summary missing {field}: {line}");
            }
        }
    }

    #[test]
    fn report_block_says_would_write_only_in_a_dry_run() {
        let c = sample();
        let dry = report_block(
            "control",
            &BackfillOpts {
                execute: false,
                only_graph: None,
            },
            &c,
        );
        assert!(dry.contains("WOULD write"));
        assert!(dry.contains("DRY RUN -- nothing was written"));

        let real = report_block(
            "control",
            &BackfillOpts {
                execute: true,
                only_graph: None,
            },
            &c,
        );
        assert!(real.contains("wrote"));
        assert!(!real.contains("WOULD write"));
        assert!(!real.contains("DRY RUN"));
    }

    #[test]
    fn mode_string_matches_the_flag() {
        assert_eq!(
            BackfillOpts {
                execute: false,
                only_graph: None
            }
            .mode(),
            "dry-run"
        );
        assert_eq!(
            BackfillOpts {
                execute: true,
                only_graph: None
            }
            .mode(),
            "execute"
        );
    }
}
