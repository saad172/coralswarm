//! Operator core for re-extracting done source versions under
//! `source-atoms-v3`. Thin binary: `reextract_source_atoms_v2 --to v3`.
//!
//! A request repins one `source-atoms-v1` or `source-atoms-v2` version to
//! v3, resets its cursor and marks it `blocked` / `reextraction_requested`
//! (`workers::source_atoms::v2::repin_tx`). Nothing calls a model here. The
//! standing sweep, with `SOURCE_EXTRACTION_PROFILE=source-atoms-v3`, adopts
//! the requested versions like any other blocked capture and extracts them
//! under the managed gate, its caps and its receipts. When a version's last
//! v3 window commits, its earlier atoms are retired unless an answer receipt
//! or a temporal record cites them (`v2::retire_superseded_tx`).
//!
//! No worker extracts v1 anymore, so this is the only way a v1-extracted
//! version moves forward. Until it does, readers serve its v1 atoms through
//! `served_semantic_atoms`.
//!
//! Bounded: `limit` versions per run. Idempotent: a repinned version is no
//! longer v1 or v2, so a second run finds nothing for it.
//!
//! Eligible: an extracted (done, with windows under its pin) v1 or v2
//! version, or, with `include_failed`, a failed one captured on or after the
//! standing floor. Versions with an in-flight owner, a pending/running
//! extraction job, or an active selected publication are skipped by the
//! repin itself.

use sqlx::PgPool;
use uuid::Uuid;

use crate::workers::source_atoms::{EXTRACTION_VERSION, EXTRACTION_VERSION_V2, EXTRACTION_VERSION_V3};

/// The profiles a request replaces.
const FROM: [&str; 2] = [EXTRACTION_VERSION, EXTRACTION_VERSION_V2];

const CANDIDATES_SQL: &str = "
SELECT v.graph_id, v.version_id, v.extraction_version
  FROM source_versions v JOIN source_items s ON s.graph_id=v.graph_id AND s.source_item_id=v.source_item_id
 WHERE v.extraction_version=ANY($4) AND s.access_state='permitted'
   AND ($1::uuid IS NULL OR v.graph_id=$1)
   AND NOT COALESCE(v.origin->>'transport'='synthetic',false)
   AND NOT COALESCE(v.source_metadata->'synthetic_demo'='true'::jsonb,false)
   AND ((v.extraction_status='done' AND EXISTS(SELECT 1 FROM semantic_atom_windows w
          WHERE w.source_version_id=v.version_id AND w.graph_id=v.graph_id AND w.extraction_version=v.extraction_version))
     OR ($2 AND v.extraction_status='failed' AND (v.captured_at>='2026-10-13T00:00:00Z' OR EXISTS(SELECT 1 FROM semantic_atom_windows w
          WHERE w.source_version_id=v.version_id AND w.graph_id=v.graph_id AND w.extraction_version=v.extraction_version))))
 ORDER BY v.captured_at, v.version_id
 LIMIT $3";

/// What a request would touch, read in one READ ONLY transaction.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Preview {
    /// Candidate versions (bounded by the limit).
    pub versions: i64,
    /// Their windows under the replaced profile: one model call each under
    /// the target, plus one Jev call per window with unbound mentions.
    pub windows: i64,
    /// Their semantic atoms under the replaced profile (retired as each
    /// version completes under the target, unless cited).
    pub prior_atoms: i64,
}

/// v2 extraction cost per window, measured v1 cost ($0.0007/call) times the
/// plan's 1.5-2x output estimate, rounded up. Reporting only.
pub const ESTIMATED_USD_PER_WINDOW: f64 = 0.0015;

/// v3 extraction cost per window: the v2 upper bound plus the measured
/// prompt and schema growth (+3,333 chars, ~835 input tokens, +$0.00007) and
/// the expected output growth (`"changes":[]` on every atom, a change on ~4%
/// of atoms, more dates and single-valued relations: +500..1,500 tokens,
/// +$0.0001..0.0003), at DeepSeek V4 Flash's $0.08/M in and $0.18/M out,
/// rounded up. Reporting only.
pub const ESTIMATED_USD_PER_WINDOW_V3: f64 = 0.0018;

pub async fn preview(
    pool: &PgPool,
    graph: Option<Uuid>,
    include_failed: bool,
    limit: i64,
) -> Result<Preview, sqlx::Error> {
    let mut tx = pool.begin().await?;
    sqlx::query("SET TRANSACTION READ ONLY").execute(&mut *tx).await?;
    let (versions, windows, prior_atoms): (i64, i64, i64) = sqlx::query_as(&format!(
        "WITH c AS ({CANDIDATES_SQL}) SELECT \
           (SELECT count(*) FROM c), \
           (SELECT count(*) FROM semantic_atom_windows w JOIN c ON c.version_id=w.source_version_id AND c.graph_id=w.graph_id WHERE w.extraction_version=c.extraction_version), \
           (SELECT count(*) FROM semantic_atoms a JOIN c ON c.version_id=a.source_version_id AND c.graph_id=a.graph_id WHERE a.extraction_version=c.extraction_version)"
    ))
    .bind(graph)
    .bind(include_failed)
    .bind(limit)
    .bind(FROM)
    .fetch_one(&mut *tx)
    .await?;
    tx.rollback().await?;
    Ok(Preview { versions, windows, prior_atoms })
}

/// Repin up to `limit` candidates to v3, one short transaction per version,
/// under the same advisory/parent/version lock order standing adoption uses.
/// Returns (repinned, skipped).
pub async fn execute(
    pool: &PgPool,
    graph: Option<Uuid>,
    include_failed: bool,
    limit: i64,
) -> Result<(u64, u64), sqlx::Error> {
    let candidates: Vec<(Uuid, Uuid, String)> = sqlx::query_as(CANDIDATES_SQL)
        .bind(graph)
        .bind(include_failed)
        .bind(limit)
        .bind(FROM)
        .fetch_all(pool)
        .await?;
    let (mut repinned, mut skipped) = (0, 0);
    for (graph_id, version_id, from) in candidates {
        let mut tx = pool.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtext('corpus-version:' || $1::text))")
            .bind(version_id.to_string())
            .execute(&mut *tx)
            .await?;
        sqlx::query("SELECT source_item_id FROM source_items WHERE graph_id=$1 AND source_item_id=(SELECT source_item_id FROM source_versions WHERE graph_id=$1 AND version_id=$2) FOR UPDATE")
            .bind(graph_id)
            .bind(version_id)
            .fetch_optional(&mut *tx)
            .await?;
        sqlx::query("SELECT version_id FROM source_versions WHERE graph_id=$1 AND version_id=$2 FOR UPDATE")
            .bind(graph_id)
            .bind(version_id)
            .fetch_optional(&mut *tx)
            .await?;
        if crate::workers::source_atoms::v2::repin_tx(&mut tx, graph_id, version_id, &from, EXTRACTION_VERSION_V3, "reextraction_requested").await? {
            repinned += 1;
        } else {
            skipped += 1;
        }
        tx.commit().await?;
    }
    Ok((repinned, skipped))
}
