//! Repair core of the `carry_forward_unchanged_versions` operator bin.
//!
//! A metadata-only re-capture used to mint a new current version that waits
//! for extraction with no facts, although an earlier version of the same item
//! holds the same bytes, extracted (#1382's anchor keys changed every
//! Slack/Linear fingerprint). Capture now records such a version as carried
//! (`source_evidence::carries_extraction`); this applies the same rule to
//! versions captured before it. Runs PLAIN under the elevated owner role and
//! walks every graph of one database, behind
//! `node_atoms_bridge::assert_policy_bypass` in the bin.
//!
//! The bin's `--receipts` repair ([`receipt_carried`]) writes the capture
//! receipt that carried versions captured before tenant 0193 / control 0362
//! never got.

use sqlx::PgPool;
use uuid::Uuid;

/// Current versions that a metadata-only re-capture minted before
/// `source_evidence::carries_extraction` existed: still waiting for extraction, with no facts
/// or windows of their own, while an earlier `done` version of the same item
/// holds the same bytes in the same representation. A version with a running
/// extraction job is left alone. `$1` is an optional graph, `$2` the bound.
const UNCARRIED_SQL: &str = "
SELECT c.graph_id,c.version_id,p.version_id prior_id,p.extraction_version,p.next_start_char
 FROM source_items s JOIN source_versions c ON c.graph_id=s.graph_id AND c.version_id=s.current_version_id
 CROSS JOIN LATERAL (SELECT p.version_id,p.extraction_version,p.next_start_char FROM source_versions p
   WHERE p.graph_id=c.graph_id AND p.source_item_id=c.source_item_id AND p.version_id<>c.version_id
    AND p.content_sha256=c.content_sha256 AND p.representation=c.representation
    AND p.extraction_status='done' AND NOT p.is_legacy_baseline
   ORDER BY EXISTS(SELECT 1 FROM semantic_atoms x WHERE x.graph_id=p.graph_id AND x.source_version_id=p.version_id) DESC,
    p.captured_at DESC,p.version_id DESC LIMIT 1) p
 WHERE ($1::uuid IS NULL OR s.graph_id=$1) AND s.access_state='permitted'
  AND c.content_sha256 IS NOT NULL AND NOT c.is_legacy_baseline AND c.extraction_status<>'done'
  AND NOT EXISTS(SELECT 1 FROM semantic_atoms x WHERE x.graph_id=c.graph_id AND x.source_version_id=c.version_id)
  AND NOT EXISTS(SELECT 1 FROM semantic_atom_windows w WHERE w.graph_id=c.graph_id AND w.source_version_id=c.version_id)
  AND NOT EXISTS(SELECT 1 FROM jobs j WHERE j.payload->>'source_version_id'=c.version_id::text AND j.status::text='running')
 ORDER BY c.captured_at,c.version_id LIMIT $2";

/// What [`carry_forward_uncarried`] would change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UncarriedPreview {
    pub versions: i64,
    /// Facts of the earlier versions, which those items' current versions
    /// serve once carried (and already serve through the read fallback).
    pub prior_atoms: i64,
    /// Extraction jobs still queued for them. Each finds its version `done`
    /// and completes without a model call.
    pub queued_jobs: i64,
}

pub async fn preview_uncarried(pool: &PgPool, graph: Option<Uuid>, limit: i64) -> Result<UncarriedPreview, sqlx::Error> {
    let mut tx = pool.begin().await?;
    sqlx::query("SET TRANSACTION READ ONLY").execute(&mut *tx).await?;
    sqlx::query("SET LOCAL statement_timeout='60s'").execute(&mut *tx).await?;
    let (versions, prior_atoms, queued_jobs): (i64, i64, i64) = sqlx::query_as(&format!(
        "WITH u AS ({UNCARRIED_SQL}) SELECT count(*),
          COALESCE(sum((SELECT count(*) FROM semantic_atoms x WHERE x.graph_id=u.graph_id AND x.source_version_id=u.prior_id)),0)::bigint,
          COALESCE(sum((SELECT count(*) FROM jobs j WHERE j.payload->>'source_version_id'=u.version_id::text AND j.status::text='pending')),0)::bigint
         FROM u"))
        .bind(graph).bind(limit).fetch_one(&mut *tx).await?;
    tx.rollback().await?;
    Ok(UncarriedPreview { versions, prior_atoms, queued_jobs })
}

/// Applies `source_evidence::carries_extraction` to versions captured before
/// it: each becomes `done` under its earlier version's profile and cursor.
/// Nothing is copied, deleted or queued. Idempotent: a carried version is
/// `done`, so a second run finds nothing for it. Returns the versions changed.
pub async fn carry_forward_uncarried(pool: &PgPool, graph: Option<Uuid>, limit: i64) -> Result<u64, sqlx::Error> {
    let mut tx = pool.begin().await?;
    sqlx::query("SET LOCAL statement_timeout='60s'").execute(&mut *tx).await?;
    let changed = sqlx::query(&format!(
        "WITH u AS ({UNCARRIED_SQL})
         UPDATE source_versions v SET extraction_status='done',extraction_error=NULL,
          extraction_version=u.extraction_version,next_start_char=u.next_start_char
         FROM u WHERE v.graph_id=u.graph_id AND v.version_id=u.version_id AND v.extraction_status<>'done'"))
        .bind(graph).bind(limit).execute(&mut *tx).await?.rows_affected();
    tx.commit().await?;
    Ok(changed)
}

/// Carried versions with no capture receipt: recorded `done` at capture by
/// `source_evidence::carries_extraction` before capture receipted carries
/// (tenant 0193 / control 0362), so `capture_admission_health` counted each
/// one an `unknown` clock forever (423 on the coralswarm org tenant,
/// 2026-10-20 to 2026-10-23). Same carry facts the observation trigger
/// checks, so every receipt written here records `carried_forward`. An item
/// that is not `permitted` cannot hold a `processed` receipt and is left
/// alone (and stays visibly unknown). `$1` is an optional graph, `$2` the
/// bound, `$3` the adoption floor.
const UNRECEIPTED_CARRIES_SQL: &str = "
SELECT v.version_id,v.graph_id,g.account_id,v.source_item_id,v.ingested_by,v.extraction_version
 FROM source_versions v JOIN graphs g ON g.id=v.graph_id
 JOIN source_items s ON s.source_item_id=v.source_item_id AND s.graph_id=v.graph_id
 WHERE ($1::uuid IS NULL OR v.graph_id=$1) AND v.captured_at>=$3 AND s.access_state='permitted'
  AND v.extraction_status='done' AND NOT v.is_legacy_baseline AND v.extraction_version<>'selected-passages-v1'
  AND NOT EXISTS(SELECT 1 FROM corpus_capture_admissions r WHERE r.source_version_id=v.version_id)
  AND NOT EXISTS(SELECT 1 FROM semantic_atom_windows w WHERE w.graph_id=v.graph_id AND w.source_version_id=v.version_id)
  AND EXISTS(SELECT 1 FROM source_versions p WHERE p.graph_id=v.graph_id AND p.source_item_id=v.source_item_id AND p.version_id<>v.version_id
    AND (p.captured_at,p.version_id)<(v.captured_at,v.version_id) AND p.content_sha256=v.content_sha256 AND p.representation=v.representation
    AND p.extraction_version=v.extraction_version AND p.extraction_status='done' AND NOT p.is_legacy_baseline)
 ORDER BY v.captured_at,v.version_id LIMIT $2";

/// What [`receipt_carried`] would change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnreceiptedPreview {
    pub versions: i64,
    /// Done versions since the floor with no receipt that this repair does
    /// not touch: not carries, or the item is not permitted. A non-zero
    /// count is a different defect to investigate, not something to receipt.
    pub other_unreceipted: i64,
}

pub async fn preview_unreceipted_carries(pool: &PgPool, graph: Option<Uuid>, limit: i64) -> Result<UnreceiptedPreview, sqlx::Error> {
    let floor = crate::capture_admission_health::adoption_floor();
    let mut tx = pool.begin().await?;
    sqlx::query("SET TRANSACTION READ ONLY").execute(&mut *tx).await?;
    sqlx::query("SET LOCAL statement_timeout='60s'").execute(&mut *tx).await?;
    let versions: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM ({UNRECEIPTED_CARRIES_SQL}) u"))
        .bind(graph).bind(limit).bind(floor).fetch_one(&mut *tx).await?;
    let all: i64 = sqlx::query_scalar("SELECT count(*) FROM source_versions v WHERE ($1::uuid IS NULL OR v.graph_id=$1) AND v.captured_at>=$2 AND v.extraction_status='done' AND NOT EXISTS(SELECT 1 FROM corpus_capture_admissions r WHERE r.source_version_id=v.version_id)")
        .bind(graph).bind(floor).fetch_one(&mut *tx).await?;
    tx.rollback().await?;
    Ok(UnreceiptedPreview { versions, other_unreceipted: (all - versions).max(0) })
}

/// Writes the missing `processed` receipt for each carried version. The
/// observation trigger stamps `observed_at`/`processed_at` now and decides
/// the basis from the source facts. Returns (receipts written, of which
/// recorded `carried_forward`). Idempotent: a receipted version is skipped.
pub async fn receipt_carried(pool: &PgPool, graph: Option<Uuid>, limit: i64) -> Result<(u64, i64), sqlx::Error> {
    let mut tx = pool.begin().await?;
    sqlx::query("SET LOCAL statement_timeout='60s'").execute(&mut *tx).await?;
    let written: Vec<Uuid> = sqlx::query_scalar(&format!(
        "INSERT INTO corpus_capture_admissions(source_version_id,graph_id,account_id,source_item_id,captured_by,extraction_version,state)
         SELECT version_id,graph_id,account_id,source_item_id,ingested_by,extraction_version,'processed' FROM ({UNRECEIPTED_CARRIES_SQL}) u
         ON CONFLICT(source_version_id) DO NOTHING RETURNING source_version_id"))
        .bind(graph).bind(limit).bind(crate::capture_admission_health::adoption_floor()).fetch_all(&mut *tx).await?;
    let carried: i64 = sqlx::query_scalar("SELECT count(*) FROM corpus_capture_admissions WHERE source_version_id=ANY($1) AND hold_basis='carried_forward'")
        .bind(&written).fetch_one(&mut *tx).await?;
    tx.commit().await?;
    Ok((written.len() as u64, carried))
}
