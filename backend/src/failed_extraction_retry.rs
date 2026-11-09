//! Operator core: re-extract FAILED `source-atoms-v2` and `source-atoms-v3`
//! versions under their own profile again. Thin binary:
//! `retry_failed_extractions_v2`. A failed `source-atoms-v1` version is
//! `unsupported_profile`: no worker extracts v1, and moving it to v3 is
//! `reextract_source_atoms_v2 --include-failed`.
//!
//! ## Why
//!
//! Before COR-99 (#1289, 2026-10-15) the atom validator failed a whole window
//! over one inexact quote or one unsupported entity mention. Those versions
//! are still `extraction_status='failed'` under `source-atoms-v1`. The
//! validator now drops only the offending atom, entity or relation, so the
//! same captures extract under v2. This tool hands them back to standing
//! adoption. It never calls a model.
//!
//! ## What one retry does (one transaction per version)
//!
//! Lock order is the one standing adoption uses: the per-version advisory
//! lock, the parent `source_items` row, the `source_versions` row, then the
//! version's units. The version is re-classified under those locks; if it is
//! no longer eligible, nothing is written.
//!
//! * `v2_requeue`: a failed v2 version whose standing owner unit failed. The
//!   unit goes back to `pending`/`infrastructure_unavailable` with
//!   `retry_after=now()`, which the standing sweep's existing requeue path
//!   (`requeue_units`) turns into a fresh job. The cursor is kept: committed
//!   v2 windows are never paid for again. The claim recovers the unit's old
//!   logical call and mints a new one.
//! * `v2_readopt`: a failed v2 version with no v2 owner. It is marked
//!   `blocked`/`reextraction_requested` (cursor kept) for standing adoption.
//! * `v3_requeue` / `v3_readopt`: the same two for a failed `source-atoms-v3`
//!   version, which stays on v3. Before this, v3 versions were
//!   `unsupported_profile` and a failed v3 version had no retry path at all
//!   (standing never re-adopts `failed`). Its earlier v1/v2 atoms, if any,
//!   are still there: they are retired only when the last v3 window commits.
//!
//! Standing then extracts each version through the normal gated path: the
//! managed gate, its per-attempt, per-version and daily caps, its receipts,
//! the account's egress policy and the requester's access.
//!
//! ## Charging
//!
//! A version is touched only when nothing of it is open: no pending or
//! running job, no `reserved` attempt receipt of the version or its units,
//! and, with a control pool, no `held` credit reservation for any logical
//! call of the version. Without a control pool, a version that ever reserved
//! a receipt is skipped (`unverified_control_settlement`). Settled receipts
//! (`billed`, `ambiguous`, `not_generated`) are final; the retry is new
//! logical calls charged once each by the gate.
//!
//! ## Permissions and availability
//!
//! Only permitted sources (`source_items.access_state='permitted'`) of live
//! graphs are touched; synthetic/demo captures never are. Captures that
//! standing does not process (before its 2026-10-13 backlog floor, without
//! v1 windows) are reported as `outside_standing_floor` and left failed:
//! retrying them would park them as blocked with nothing to pick them up.
//! The requester, egress policy and credits are checked again by the gate
//! before every model attempt.
//!
//! ## The before-floor one-off (`Selection::floor_override`)
//!
//! The founder chose a contained one-off for those pre-floor captures
//! (2026-10-19), not a lower floor. In this mode only failed versions
//! captured before the floor are selected (optionally only an explicit list),
//! and each one retried in the same transaction gets a
//! `standing_floor_overrides` row with the operator's reason.
//! `STANDING_ELIGIBLE` admits a version with a row for its current profile, so
//! standing adopts and extracts exactly those versions, through the same gate
//! and caps, whenever it is running. Every other pre-floor capture stays
//! outside standing.

use std::collections::BTreeMap;

use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

/// Default per-run bound.
pub const DEFAULT_LIMIT: i64 = 500;
/// The blocked reason a re-adoption carries.
pub const REQUESTED_REASON: &str = "reextraction_requested";
/// The hold a requeued v2 standing unit carries (see `requeue_units`).
pub const REQUEUE_REASON: &str = "infrastructure_unavailable";
/// Most versions one explicit `--version-ids-from` list may name.
pub const MAX_LISTED_VERSIONS: usize = 20_000;
/// Fallback when the database has no billed extraction receipts yet. Same
/// figure the re-extraction tool reports.
pub const FALLBACK_USD_PER_WINDOW: f64 = crate::source_reextract::ESTIMATED_USD_PER_WINDOW;

/// Per-version classification. `$1` graph (nullable), `$2` logical calls
/// with a held control reservation, `$3` true when the control plane was
/// checked, `$4` one version (nullable), `$5` the before-floor one-off (only
/// pre-floor captures, floor not a skip), `$6` an
/// explicit version list (nullable).
const CLASSIFY_SQL: &str = "
WITH f AS (
 SELECT v.graph_id, v.version_id, v.extraction_version, v.source_type, v.next_start_char, v.captured_at,
        COALESCE(v.extraction_error, '(none)') AS error_class, v.source_item_id,
        COALESCE(v.origin->>'transport'='synthetic',false) OR COALESCE(v.source_metadata->'synthetic_demo'='true'::jsonb,false) AS synthetic,
        (SELECT count(*) FROM semantic_atom_windows w WHERE w.graph_id=v.graph_id AND w.source_version_id=v.version_id AND w.extraction_version='source-atoms-v1') AS v1_windows,
        (SELECT count(*) FROM semantic_atom_windows w WHERE w.graph_id=v.graph_id AND w.source_version_id=v.version_id
          AND w.extraction_version=v.extraction_version AND v.extraction_version IN ('source-atoms-v2','source-atoms-v3')) AS v2_windows
   FROM source_versions v
  WHERE v.extraction_status='failed' AND ($1::uuid IS NULL OR v.graph_id=$1) AND ($4::uuid IS NULL OR v.version_id=$4)
    AND ($6::uuid[] IS NULL OR v.version_id=ANY($6)) AND (NOT $5 OR v.captured_at<'2026-10-13T00:00:00Z')
), x AS (
 SELECT f.*,
  EXISTS(SELECT 1 FROM source_items s WHERE s.graph_id=f.graph_id AND s.source_item_id=f.source_item_id AND s.access_state='permitted') AS permitted,
  EXISTS(SELECT 1 FROM graphs g WHERE g.id=f.graph_id AND g.archived_at IS NULL) AS graph_live,
  EXISTS(SELECT 1 FROM jobs j WHERE j.payload->>'source_version_id'=f.version_id::text AND j.status::text IN ('pending','running')) AS job_open,
  EXISTS(SELECT 1 FROM managed_attempt_receipts r WHERE r.state='reserved' AND (r.source_version_id=f.version_id
         OR r.unit_id IN (SELECT u.unit_id FROM corpus_enrichment_units u WHERE u.graph_id=f.graph_id AND COALESCE(u.promoted_version_id,u.holding_id)=f.version_id))) AS receipt_open,
  EXISTS(SELECT 1 FROM managed_attempt_receipts r WHERE r.source_version_id=f.version_id AND r.logical_call=ANY($2::uuid[]))
   OR EXISTS(SELECT 1 FROM corpus_enrichment_units u WHERE u.graph_id=f.graph_id AND COALESCE(u.promoted_version_id,u.holding_id)=f.version_id AND u.standing_logical_call=ANY($2::uuid[])) AS credit_held,
  EXISTS(SELECT 1 FROM managed_attempt_receipts r WHERE r.source_version_id=f.version_id)
   OR EXISTS(SELECT 1 FROM corpus_enrichment_units u WHERE u.graph_id=f.graph_id AND COALESCE(u.promoted_version_id,u.holding_id)=f.version_id AND u.standing_logical_call IS NOT NULL) AS ever_called,
  EXISTS(SELECT 1 FROM corpus_enrichment_units u WHERE u.graph_id=f.graph_id AND COALESCE(u.promoted_version_id,u.holding_id)=f.version_id
         AND u.extraction_version=f.extraction_version AND u.execution_owner AND u.state NOT IN ('processed','failed','excluded','unavailable')) AS owner_live,
  EXISTS(SELECT 1 FROM selected_evidence_publications p WHERE p.status='active' AND (p.source_version_id=f.version_id OR p.target_source_version_id=f.version_id)) AS publication,
  (SELECT u.unit_id FROM corpus_enrichment_units u JOIN corpus_enrichment_runs r ON r.run_id=u.run_id AND r.graph_id=u.graph_id
    WHERE u.graph_id=f.graph_id AND u.holding_id=f.version_id AND u.extraction_version=f.extraction_version
      AND f.extraction_version IN ('source-atoms-v2','source-atoms-v3') AND u.execution_owner
      AND u.state='failed' AND r.run_kind='standing' LIMIT 1) AS v2_failed_standing_unit,
  EXISTS(SELECT 1 FROM corpus_enrichment_units u WHERE u.graph_id=f.graph_id AND u.holding_id=f.version_id AND u.extraction_version=f.extraction_version
         AND (u.execution_owner OR u.state='failed')) AS v2_unit
 FROM f
)
SELECT graph_id, version_id, extraction_version, source_type, error_class, v1_windows, v2_windows,
 CASE
  WHEN extraction_version NOT IN ('source-atoms-v2','source-atoms-v3') THEN 'unsupported_profile'
  WHEN synthetic THEN 'synthetic'
  WHEN NOT permitted THEN 'source_unavailable'
  WHEN NOT graph_live THEN 'graph_unavailable'
  WHEN job_open OR receipt_open OR credit_held OR owner_live THEN 'in_flight'
  WHEN NOT $3 AND ever_called THEN 'unverified_control_settlement'
  WHEN publication THEN 'active_publication'
  WHEN captured_at<'2026-10-13T00:00:00Z' AND v1_windows=0 AND NOT $5 THEN 'outside_standing_floor'
  WHEN v2_failed_standing_unit IS NOT NULL THEN 'eligible'
  WHEN NOT v2_unit THEN 'eligible'
  ELSE 'unsupported_owner'
 END AS bucket,
 CASE
  WHEN v2_failed_standing_unit IS NOT NULL THEN replace(extraction_version,'source-atoms-','')||'_requeue'
  ELSE replace(extraction_version,'source-atoms-','')||'_readopt'
 END AS action,
 v2_failed_standing_unit
FROM x
ORDER BY captured_at, version_id";

/// One classified failed version.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct Candidate {
    pub graph_id: Uuid,
    pub version_id: Uuid,
    pub extraction_version: String,
    pub source_type: String,
    pub error_class: String,
    pub v1_windows: i64,
    /// Windows committed under the version's own v2 or v3 profile.
    pub v2_windows: i64,
    pub bucket: String,
    pub action: String,
    /// The version's failed standing owner unit under its own v2 or v3 profile.
    pub v2_failed_standing_unit: Option<Uuid>,
}

/// Where the control-plane credit check comes from.
pub struct ControlCheck<'a> {
    pub pool: &'a PgPool,
    /// `managed_credit_reservations.plane`: `consumer` or `org:<clerk_org_id>`.
    pub plane: String,
}

/// Which failed versions a pass considers.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Selection {
    /// Only these versions (`None`: every failed version).
    pub versions: Option<Vec<Uuid>>,
    /// The before-floor one-off: only captures before standing's floor, each
    /// admitted with a recorded `standing_floor_overrides` row.
    pub floor_override: Option<FloorOverride>,
}

/// The audit fields of a before-floor override row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FloorOverride {
    /// Why (8..=300 characters; checked by the table too).
    pub reason: String,
    /// Who (1..=100 characters).
    pub granted_by: String,
}

impl FloorOverride {
    pub fn new(reason: &str, granted_by: &str) -> Result<Self, String> {
        let (reason, granted_by) = (reason.trim(), granted_by.trim());
        if !(8..=300).contains(&reason.chars().count()) {
            return Err("override reason must be 8..=300 characters".into());
        }
        if !(1..=100).contains(&granted_by.chars().count()) {
            return Err("override operator must be 1..=100 characters".into());
        }
        Ok(Self { reason: reason.into(), granted_by: granted_by.into() })
    }
}

impl Selection {
    fn before_floor(&self) -> bool {
        self.floor_override.is_some()
    }
}

/// Counts for one error class (or the total).
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ClassCounts {
    pub failed: i64,
    pub eligible: i64,
    /// Skips by bucket.
    pub skipped: BTreeMap<String, i64>,
}

/// Dry-run report.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct Preview {
    pub total: ClassCounts,
    pub by_error: BTreeMap<String, ClassCounts>,
    pub eligible_by_action: BTreeMap<String, i64>,
    /// Windows the eligible versions are expected to take under v2.
    pub estimated_windows: f64,
    pub usd_per_window: f64,
    /// Whether `usd_per_window` was measured from this database's billed
    /// extraction receipts (else the fallback).
    pub measured_cost: bool,
}

impl Preview {
    pub fn estimated_usd(&self) -> f64 {
        self.estimated_windows * self.usd_per_window
    }
}

/// Logical calls with a HELD control-plane credit reservation.
async fn held_logical_calls(control: &ControlCheck<'_>, calls: &[Uuid]) -> Result<Vec<Uuid>, sqlx::Error> {
    if calls.is_empty() {
        return Ok(Vec::new());
    }
    let mut tx = control.pool.begin().await?;
    read_only(&mut tx).await?;
    let held = sqlx::query_scalar(
        "SELECT DISTINCT logical_call FROM managed_credit_reservations \
          WHERE plane=$1 AND state='held' AND logical_call=ANY($2)",
    )
    .bind(&control.plane)
    .bind(calls)
    .fetch_all(&mut *tx)
    .await?;
    tx.rollback().await?;
    Ok(held)
}

/// Every logical call any failed version (or its units) ever used.
const CALLS_SQL: &str = "
SELECT DISTINCT c FROM (
  SELECT r.logical_call AS c FROM managed_attempt_receipts r JOIN source_versions v ON v.version_id=r.source_version_id
   WHERE v.extraction_status='failed' AND ($1::uuid IS NULL OR v.graph_id=$1)
  UNION ALL
  SELECT u.standing_logical_call FROM corpus_enrichment_units u JOIN source_versions v
    ON v.graph_id=u.graph_id AND v.version_id=COALESCE(u.promoted_version_id,u.holding_id)
   WHERE v.extraction_status='failed' AND u.standing_logical_call IS NOT NULL AND ($1::uuid IS NULL OR v.graph_id=$1)
) calls";

/// READ ONLY with a statement bound, both transaction-local: a prod dry run
/// never leaves session state on a pooled connection.
async fn read_only(tx: &mut Transaction<'_, Postgres>) -> Result<(), sqlx::Error> {
    sqlx::query("SET TRANSACTION READ ONLY").execute(&mut **tx).await?;
    sqlx::query("SET LOCAL statement_timeout='30s'").execute(&mut **tx).await?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn classify_in(
    tx: &mut Transaction<'_, Postgres>,
    graph: Option<Uuid>,
    held: &[Uuid],
    control_checked: bool,
    version: Option<Uuid>,
    selection: &Selection,
) -> Result<Vec<Candidate>, sqlx::Error> {
    sqlx::query_as(CLASSIFY_SQL)
        .bind(graph)
        .bind(held)
        .bind(control_checked)
        .bind(version)
        .bind(selection.before_floor())
        .bind(selection.versions.as_deref())
        .fetch_all(&mut **tx)
        .await
}

async fn held_for(pool: &PgPool, graph: Option<Uuid>, control: Option<&ControlCheck<'_>>) -> Result<Vec<Uuid>, String> {
    let Some(control) = control else { return Ok(Vec::new()) };
    let mut tx = pool.begin().await.map_err(|e| e.to_string())?;
    read_only(&mut tx).await.map_err(|e| e.to_string())?;
    let calls: Vec<Uuid> = sqlx::query_scalar(CALLS_SQL).bind(graph).fetch_all(&mut *tx).await
        .map_err(|e| format!("logical calls: {e}"))?;
    tx.rollback().await.map_err(|e| e.to_string())?;
    held_logical_calls(control, &calls).await.map_err(|e| format!("control settlement check: {e}"))
}

/// Classify every failed version, in READ ONLY transactions.
pub async fn classify(
    pool: &PgPool,
    graph: Option<Uuid>,
    control: Option<&ControlCheck<'_>>,
) -> Result<Vec<Candidate>, String> {
    classify_selected(pool, graph, control, &Selection::default()).await
}

/// Classify the selected failed versions, in READ ONLY transactions.
pub async fn classify_selected(
    pool: &PgPool,
    graph: Option<Uuid>,
    control: Option<&ControlCheck<'_>>,
    selection: &Selection,
) -> Result<Vec<Candidate>, String> {
    let held = held_for(pool, graph, control).await?;
    let mut tx = pool.begin().await.map_err(|e| e.to_string())?;
    read_only(&mut tx).await.map_err(|e| e.to_string())?;
    let out = classify_in(&mut tx, graph, &held, control.is_some(), None, selection)
        .await
        .map_err(|e| format!("classify: {e}"))?;
    tx.rollback().await.map_err(|e| e.to_string())?;
    Ok(out)
}

/// Before-floor override rows already recorded for `versions`, READ ONLY.
pub async fn recorded_overrides(pool: &PgPool, versions: &[Uuid]) -> Result<i64, sqlx::Error> {
    let mut tx = pool.begin().await?;
    read_only(&mut tx).await?;
    let n = sqlx::query_scalar("SELECT count(*) FROM standing_floor_overrides WHERE source_version_id=ANY($1)")
        .bind(versions)
        .fetch_one(&mut *tx)
        .await?;
    tx.rollback().await?;
    Ok(n)
}

/// Measured inputs for the cost estimate, read READ ONLY: mean v2 windows per
/// completed version by source type, and the mean billed cost of one
/// extraction attempt.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct CostBasis {
    pub windows_by_source_type: BTreeMap<String, f64>,
    pub usd_per_window: Option<f64>,
}

pub async fn cost_basis(pool: &PgPool) -> Result<CostBasis, sqlx::Error> {
    let mut tx = pool.begin().await?;
    read_only(&mut tx).await?;
    let rows: Vec<(String, f64)> = sqlx::query_as(
        "SELECT v.source_type, avg(w.n)::float8 FROM source_versions v \
           JOIN LATERAL (SELECT count(*) n FROM semantic_atom_windows w WHERE w.graph_id=v.graph_id \
                           AND w.source_version_id=v.version_id AND w.extraction_version='source-atoms-v2') w ON w.n>0 \
          WHERE v.extraction_version='source-atoms-v2' AND v.extraction_status='done' GROUP BY v.source_type",
    )
    .fetch_all(&mut *tx)
    .await?;
    let micro: Option<f64> = sqlx::query_scalar(
        "SELECT avg(settled_micro_usd)::float8 FROM managed_attempt_receipts \
          WHERE purpose='source_extraction' AND state='billed' AND settled_micro_usd IS NOT NULL",
    )
    .fetch_one(&mut *tx)
    .await?;
    tx.rollback().await?;
    Ok(CostBasis {
        windows_by_source_type: rows.into_iter().collect(),
        usd_per_window: micro.map(|m| m / 1_000_000.0),
    })
}

/// Totals for a classification. Each eligible version is expected to take
/// its source type's mean v2 window count (at least one), less the windows a
/// requeued version already committed.
pub fn summarize(candidates: &[Candidate], basis: &CostBasis) -> Preview {
    let mut p = Preview {
        usd_per_window: basis.usd_per_window.unwrap_or(FALLBACK_USD_PER_WINDOW),
        measured_cost: basis.usd_per_window.is_some(),
        ..Preview::default()
    };
    for c in candidates {
        let class = p.by_error.entry(c.error_class.clone()).or_default();
        class.failed += 1;
        p.total.failed += 1;
        if c.bucket == "eligible" {
            class.eligible += 1;
            p.total.eligible += 1;
            *p.eligible_by_action.entry(c.action.clone()).or_default() += 1;
            let expected = basis.windows_by_source_type.get(&c.source_type).copied().unwrap_or(1.0).max(1.0);
            let remaining = (expected - c.v2_windows as f64).max(1.0);
            p.estimated_windows += remaining;
        } else {
            *class.skipped.entry(c.bucket.clone()).or_default() += 1;
            *p.total.skipped.entry(c.bucket.clone()).or_default() += 1;
        }
    }
    p
}

/// Outcome of one execute pass.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Executed {
    pub requeued_v2: u64,
    pub readopted_v2: u64,
    /// Failed v3 versions handed back to standing under v3.
    pub requeued_v3: u64,
    pub readopted_v3: u64,
    /// Re-checked under locks and no longer eligible. Left untouched.
    pub skipped: u64,
    /// Before-floor override rows written (one per retried version; zero when
    /// the row was already there).
    pub overrides_recorded: u64,
}

/// Retry up to `limit` eligible versions, one transaction per version.
pub async fn execute(
    pool: &PgPool,
    graph: Option<Uuid>,
    control: Option<&ControlCheck<'_>>,
    limit: i64,
) -> Result<Executed, String> {
    execute_selected(pool, graph, control, limit, &Selection::default()).await
}

/// Retry up to `limit` eligible selected versions, one transaction per
/// version. In the before-floor mode each retried version's override row is
/// written in its own transaction, so a version is either retried with its
/// recorded reason or untouched.
pub async fn execute_selected(
    pool: &PgPool,
    graph: Option<Uuid>,
    control: Option<&ControlCheck<'_>>,
    limit: i64,
    selection: &Selection,
) -> Result<Executed, String> {
    let held = held_for(pool, graph, control).await?;
    let candidates = {
        let mut tx = pool.begin().await.map_err(|e| e.to_string())?;
        read_only(&mut tx).await.map_err(|e| e.to_string())?;
        let out = classify_in(&mut tx, graph, &held, control.is_some(), None, selection)
            .await
            .map_err(|e| format!("classify: {e}"))?;
        tx.rollback().await.map_err(|e| e.to_string())?;
        out
    };
    let mut out = Executed::default();
    for c in candidates.iter().filter(|c| c.bucket == "eligible").take(limit.max(0) as usize) {
        let mut tx = pool.begin().await.map_err(|e| e.to_string())?;
        match retry_one_tx(&mut tx, c, &held, control.is_some(), selection).await {
            Ok(Some((action, recorded))) => {
                tx.commit().await.map_err(|e| format!("commit {}: {e}", c.version_id))?;
                out.overrides_recorded += u64::from(recorded);
                match action {
                    "v2_requeue" => out.requeued_v2 += 1,
                    "v3_requeue" => out.requeued_v3 += 1,
                    "v3_readopt" => out.readopted_v3 += 1,
                    _ => out.readopted_v2 += 1,
                }
            }
            Ok(None) => {
                tx.rollback().await.map_err(|e| e.to_string())?;
                out.skipped += 1;
            }
            Err(e) => {
                tx.rollback().await.ok();
                return Err(format!(
                    "version {}: {e} (versions committed before it stay retried; re-run continues)",
                    c.version_id
                ));
            }
        }
    }
    Ok(out)
}

/// One version, under the adoption lock order. `Ok(None)`: no longer
/// eligible, nothing written (the caller rolls back). `Some((action, true))`
/// when a before-floor override row was written.
async fn retry_one_tx(
    tx: &mut Transaction<'_, Postgres>,
    c: &Candidate,
    held: &[Uuid],
    control_checked: bool,
    selection: &Selection,
) -> Result<Option<(&'static str, bool)>, sqlx::Error> {
    let action = match retry_action_tx(tx, c, held, control_checked, selection).await? {
        Some(action) => action,
        None => return Ok(None),
    };
    let Some(grant) = &selection.floor_override else {
        return Ok(Some((action, false)));
    };
    // The version is now blocked (or its v2/v3 unit requeued); the override
    // is what lets standing take it. Same transaction: never a retried
    // pre-floor version without its recorded reason.
    let pinned = c.extraction_version.as_str();
    let recorded = sqlx::query(
        "INSERT INTO standing_floor_overrides(source_version_id,graph_id,extraction_version,reason,granted_by) \
         SELECT v.version_id,v.graph_id,v.extraction_version,$3,$4 FROM source_versions v \
          WHERE v.graph_id=$1 AND v.version_id=$2 AND v.extraction_version=$5 AND v.captured_at<'2026-10-13T00:00:00Z' \
         ON CONFLICT (source_version_id) DO NOTHING",
    )
    .bind(c.graph_id).bind(c.version_id).bind(&grant.reason).bind(&grant.granted_by).bind(pinned)
    .execute(&mut **tx).await?.rows_affected() == 1;
    let present: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM standing_floor_overrides WHERE source_version_id=$2 AND graph_id=$1 AND extraction_version=$3)",
    )
    .bind(c.graph_id).bind(c.version_id).bind(pinned)
    .fetch_one(&mut **tx).await?;
    if !present {
        return Ok(None);
    }
    Ok(Some((action, recorded)))
}

async fn retry_action_tx(
    tx: &mut Transaction<'_, Postgres>,
    c: &Candidate,
    held: &[Uuid],
    control_checked: bool,
    selection: &Selection,
) -> Result<Option<&'static str>, sqlx::Error> {
    sqlx::query("SELECT pg_advisory_xact_lock(hashtext('corpus-version:' || $1::text))")
        .bind(c.version_id.to_string()).execute(&mut **tx).await?;
    sqlx::query("SELECT source_item_id FROM source_items WHERE graph_id=$1 AND source_item_id=(SELECT source_item_id FROM source_versions WHERE graph_id=$1 AND version_id=$2) FOR UPDATE")
        .bind(c.graph_id).bind(c.version_id).fetch_optional(&mut **tx).await?;
    sqlx::query("SELECT version_id FROM source_versions WHERE graph_id=$1 AND version_id=$2 FOR UPDATE")
        .bind(c.graph_id).bind(c.version_id).fetch_optional(&mut **tx).await?;
    sqlx::query("SELECT unit_id FROM corpus_enrichment_units WHERE graph_id=$1 AND COALESCE(promoted_version_id,holding_id)=$2 ORDER BY unit_id FOR UPDATE")
        .bind(c.graph_id).bind(c.version_id).fetch_all(&mut **tx).await?;
    let fresh = classify_in(tx, Some(c.graph_id), held, control_checked, Some(c.version_id), selection).await?;
    let Some(fresh) = fresh.into_iter().next() else {
        return Ok(None);
    };
    if fresh.bucket != "eligible" || fresh.action != c.action || fresh.v2_failed_standing_unit != c.v2_failed_standing_unit {
        return Ok(None);
    }
    match fresh.action.as_str() {
        action @ ("v2_requeue" | "v3_requeue") => {
            let Some(unit) = fresh.v2_failed_standing_unit else { return Ok(None) };
            let moved = sqlx::query(
                "UPDATE corpus_enrichment_units SET state='pending',reason=$2,retry_after=now(),standing_claim_until=NULL,updated_at=now() \
                  WHERE unit_id=$1 AND execution_owner AND extraction_version=$3 AND state='failed'",
            )
            .bind(unit).bind(REQUEUE_REASON).bind(&c.extraction_version)
            .execute(&mut **tx).await?.rows_affected();
            if moved != 1 {
                return Ok(None);
            }
            mark_blocked(tx, c, REQUEUE_REASON).await
                .map(|ok| ok.then_some(if action == "v3_requeue" { "v3_requeue" } else { "v2_requeue" }))
        }
        "v2_readopt" => mark_blocked(tx, c, REQUESTED_REASON).await.map(|ok| ok.then_some("v2_readopt")),
        "v3_readopt" => mark_blocked(tx, c, REQUESTED_REASON).await.map(|ok| ok.then_some("v3_readopt")),
        _ => Ok(None),
    }
}

/// A failed v2 or v3 version back to `blocked` with `reason`, on its own
/// profile, cursor kept, and its capture receipt back to `blocked` for
/// adoption/requeue.
async fn mark_blocked(tx: &mut Transaction<'_, Postgres>, c: &Candidate, reason: &str) -> Result<bool, sqlx::Error> {
    let moved = sqlx::query(
        "UPDATE source_versions SET extraction_status='blocked',extraction_error=$3 \
          WHERE graph_id=$1 AND version_id=$2 AND extraction_version=$4 AND extraction_status='failed'",
    )
    .bind(c.graph_id).bind(c.version_id).bind(reason).bind(&c.extraction_version)
    .execute(&mut **tx).await?.rows_affected();
    if moved != 1 {
        return Ok(false);
    }
    sqlx::query(
        "UPDATE corpus_capture_admissions SET state='blocked',reason=$4,updated_at=now() \
          WHERE graph_id=$1 AND source_version_id=$2 AND extraction_version=$3 AND state<>'processed'",
    )
    .bind(c.graph_id).bind(c.version_id).bind(&c.extraction_version).bind(reason)
    .execute(&mut **tx).await?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workers::source_atoms::EXTRACTION_VERSION_V2;

    fn cand(error: &str, bucket: &str, action: &str, source_type: &str, v1_windows: i64, v2_windows: i64) -> Candidate {
        Candidate {
            graph_id: Uuid::nil(),
            version_id: Uuid::new_v4(),
            extraction_version: EXTRACTION_VERSION_V2.into(),
            source_type: source_type.into(),
            error_class: error.into(),
            v1_windows,
            v2_windows,
            bucket: bucket.into(),
            action: action.into(),
            v2_failed_standing_unit: None,
        }
    }

    #[test]
    fn floor_override_fields_are_bounded() {
        assert!(FloorOverride::new("short", "Alex").is_err());
        assert!(FloorOverride::new("pre-floor COR-99 one-off", "  ").is_err());
        assert!(FloorOverride::new(&"x".repeat(301), "Alex").is_err());
        let ok = FloorOverride::new("  pre-floor COR-99 one-off  ", " Alex ").unwrap();
        assert_eq!((ok.reason.as_str(), ok.granted_by.as_str()), ("pre-floor COR-99 one-off", "Alex"));
        assert!(!Selection::default().before_floor());
        assert!(Selection { versions: None, floor_override: Some(ok) }.before_floor());
    }

    #[test]
    fn summary_counts_by_error_class_and_estimates_cost() {
        let basis = CostBasis {
            windows_by_source_type: [("github_code".to_string(), 3.0)].into_iter().collect(),
            usd_per_window: Some(0.002),
        };
        let p = summarize(&[
            cand("evidence quote not found", "eligible", "v2_readopt", "github_code", 2, 0),
            cand("evidence quote not found", "outside_standing_floor", "v2_readopt", "slack", 0, 0),
            cand("model request failed", "eligible", "v2_requeue", "github_code", 0, 2),
            cand("model request failed", "in_flight", "v2_readopt", "slack", 0, 0),
            cand("too many atoms", "eligible", "v2_readopt", "slack", 0, 0),
        ], &basis);
        assert_eq!((p.total.failed, p.total.eligible), (5, 3));
        assert_eq!(p.by_error["evidence quote not found"].skipped["outside_standing_floor"], 1);
        assert_eq!(p.by_error["model request failed"].eligible, 1);
        assert_eq!(p.eligible_by_action["v2_readopt"], 2);
        // 3 (github_code readopt) + max(3-2,1) (requeue) + 1 (slack, no basis)
        assert_eq!(p.estimated_windows, 5.0);
        assert!(p.measured_cost);
        assert!((p.estimated_usd() - 0.01).abs() < 1e-9);
        let fallback = summarize(&[], &CostBasis::default());
        assert_eq!((fallback.usd_per_window, fallback.measured_cost), (FALLBACK_USD_PER_WINDOW, false));
    }
}
