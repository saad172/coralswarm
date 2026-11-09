//! Statement co-occurrence edges: `co_occurs_with` between two entity reefs
//! named in the same semantic atom (one extracted statement).
//!
//! A co-occurrence is evidence that two reefs were mentioned together. It is
//! not a relationship any source stated, so the predicate is
//! [`predicates::Domain::Derived`]: never extracted, never walked as a stated
//! current, and excluded by every store listing that presents edges as facts
//! (Ask synthesis, digests, traversal, review, inference closure, entity
//! resolution). The Library neighbourhood shows them with their own label.
//!
//! **Rule** ([`RULE`]). Inputs are `node_semantic_atoms` links of entity
//! reefs, excluding the ocean's self reef, whose atom comes from a source item
//! with `access_state = 'permitted'`. Statements linking more than
//! [`MAX_REEFS_PER_STATEMENT`] reefs are skipped. A pair is weighted by the
//! distinct source items whose statements name both, then by the statement
//! count. Each reef ranks its partners by that weight, and a pair is kept only
//! when it is in the top [`PER_REEF`] of BOTH reefs (mutual top-k). So no reef
//! carries more than [`PER_REEF`] co-occurrence edges and a reef named
//! everywhere cannot become a hub.
//!
//! **Shape.** One row per unordered pair, `src_node_id < dst_node_id`,
//! `origin = 'inferred'`, `status = 'confirmed'`. Provenance names the newest
//! supporting `semantic_atom_ids` and their versions' chunk `atom_ids`, which
//! `library_edge_permitted` checks at read time.
//!
//! **Refresh** ([`refresh_tx`]) recomputes the whole graph's desired set and
//! converges the stored rows to it: insert missing pairs, rewrite changed
//! provenance, delete pairs the rule no longer keeps. A pair a person rejected
//! is left alone and never re-created. Re-running with unchanged inputs
//! writes nothing. Every write appends its op-log entry. The caller owns the
//! transaction and its RLS scope; a per-graph advisory lock serialises
//! concurrent refreshes.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use crate::relationships::predicates::CO_OCCURS_WITH;
use crate::store::{append_ops_tx, sync, NewOp, StoreError, CLOUD_DEVICE_ID};

pub const RULE: &str = "statement_mutual_top5_v1";
pub const PER_REEF: i64 = 5;
pub const MAX_REEFS_PER_STATEMENT: i64 = 12;
const PROVENANCE_ATOMS: i32 = 8;
const PROVENANCE_CHUNKS: i64 = 16;
/// Below `TRAVERSABLE_MIN_CONFIDENCE`: a co-occurrence is never a current.
pub const CONFIDENCE: f32 = 0.5;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Write {
    DryRun,
    Apply,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub struct Refresh {
    pub desired: usize,
    pub inserted: usize,
    pub updated: usize,
    pub deleted: usize,
    pub unchanged: usize,
    /// Pairs the rule keeps that a person rejected; never re-created.
    pub rejected_kept: usize,
}

impl Refresh {
    pub fn changed(&self) -> usize {
        self.inserted + self.updated + self.deleted
    }

    pub fn add(&mut self, other: &Refresh) {
        self.desired += other.desired;
        self.inserted += other.inserted;
        self.updated += other.updated;
        self.deleted += other.deleted;
        self.unchanged += other.unchanged;
        self.rejected_kept += other.rejected_kept;
    }
}

const DESIRED_SQL: &str = r#"
WITH links AS MATERIALIZED (
  SELECT l.semantic_atom_id AS atom, l.node_id AS node, v.source_item_id AS item, a.created_at
    FROM node_semantic_atoms l
    JOIN nodes n ON n.node_id = l.node_id AND n.graph_id = l.graph_id AND n.type = 'entity'
    JOIN semantic_atoms a ON a.semantic_atom_id = l.semantic_atom_id AND a.graph_id = l.graph_id
    JOIN source_versions v ON v.version_id = a.source_version_id AND v.graph_id = a.graph_id
    JOIN source_items s ON s.source_item_id = v.source_item_id AND s.graph_id = v.graph_id
                       AND s.access_state = 'permitted'
   WHERE l.graph_id = $1
     AND l.node_id IS DISTINCT FROM (SELECT self_node_id FROM graphs WHERE id = $1)
), statements AS (
  SELECT atom FROM links GROUP BY atom HAVING count(*) BETWEEN 2 AND $2
), pairs AS (
  SELECT x.node AS src, y.node AS dst,
         count(DISTINCT x.item)::int AS items, count(*)::int AS statements,
         (array_agg(x.atom ORDER BY x.created_at DESC, x.atom))[1:$4] AS atoms
    FROM links x
    JOIN statements st ON st.atom = x.atom
    JOIN links y ON y.atom = x.atom AND x.node < y.node
   GROUP BY 1, 2
), ranked AS (
  SELECT src, dst, row_number() OVER (PARTITION BY node ORDER BY items DESC, statements DESC, other) AS rank
    FROM (SELECT src, dst, src AS node, dst AS other, items, statements FROM pairs
          UNION ALL
          SELECT src, dst, dst, src, items, statements FROM pairs) d
)
SELECT p.src, p.dst, p.items, p.statements, p.atoms,
       ARRAY(SELECT DISTINCT c.atom_id
               FROM semantic_atoms sa
               JOIN source_version_chunks c ON c.graph_id = sa.graph_id AND c.source_version_id = sa.source_version_id
              WHERE sa.graph_id = $1 AND sa.semantic_atom_id = ANY(p.atoms)
              ORDER BY c.atom_id LIMIT $5) AS chunks
  FROM pairs p
  JOIN (SELECT src, dst FROM ranked GROUP BY 1, 2 HAVING max(rank) <= $3) k USING (src, dst)
"#;

type DesiredRow = (Uuid, Uuid, i32, i32, Vec<Uuid>, Vec<Uuid>);

fn provenance(items: i32, statements: i32, atoms: &[Uuid], chunks: &[Uuid]) -> Value {
    json!({
        "derivation": "co_occurrence",
        "co_occurrence": {"rule": RULE, "items": items, "statements": statements},
        "semantic_atom_ids": atoms.iter().map(Uuid::to_string).collect::<Vec<_>>(),
        "atom_ids": chunks.iter().map(Uuid::to_string).collect::<Vec<_>>(),
    })
}

struct Stored {
    edge_id: Uuid,
    status: String,
    provenance: Value,
}

#[derive(Debug, Default, PartialEq)]
struct Plan {
    insert: Vec<(Uuid, Uuid, Value)>,
    update: Vec<(Uuid, Value)>,
    delete: Vec<Uuid>,
    unchanged: usize,
    rejected_kept: usize,
}

fn plan(desired: HashMap<(Uuid, Uuid), Value>, mut stored: HashMap<(Uuid, Uuid), Stored>) -> Plan {
    let mut out = Plan::default();
    for (pair, want) in desired {
        match stored.remove(&pair) {
            None => out.insert.push((pair.0, pair.1, want)),
            Some(s) if s.status == "rejected" => out.rejected_kept += 1,
            Some(s) if s.provenance == want => out.unchanged += 1,
            Some(s) => out.update.push((s.edge_id, want)),
        }
    }
    out.delete = stored.into_values().filter(|s| s.status != "rejected").map(|s| s.edge_id).collect();
    out.insert.sort_by_key(|(s, d, _)| (*s, *d));
    out.update.sort_by_key(|(id, _)| *id);
    out.delete.sort();
    out
}

type EdgeRow = (Uuid, Uuid, Uuid, DateTime<Utc>, DateTime<Utc>, i32, Value);

fn upsert_op(graph_id: Uuid, (edge_id, src, dst, created, updated, version, provenance): &EdgeRow) -> NewOp {
    sync::relationship_edge_op(
        Uuid::new_v4(),
        *edge_id,
        graph_id,
        *src,
        CO_OCCURS_WITH,
        *dst,
        "inferred",
        "confirmed",
        CONFIDENCE,
        provenance,
        *created,
        *updated,
        i64::from(*version),
        CLOUD_DEVICE_ID,
    )
}

/// Converge one graph's `co_occurs_with` edges to the rule. `DryRun` reads
/// and counts only (run it in a READ ONLY transaction to prove it).
pub async fn refresh_tx(tx: &mut Transaction<'_, Postgres>, graph_id: Uuid, write: Write) -> Result<Refresh, StoreError> {
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended('co_occurrence:' || $1::text, 0))")
        .bind(graph_id)
        .execute(&mut **tx)
        .await?;
    let rows: Vec<DesiredRow> = sqlx::query_as(DESIRED_SQL)
        .bind(graph_id)
        .bind(MAX_REEFS_PER_STATEMENT)
        .bind(PER_REEF)
        .bind(PROVENANCE_ATOMS)
        .bind(PROVENANCE_CHUNKS)
        .fetch_all(&mut **tx)
        .await?;
    let desired: HashMap<(Uuid, Uuid), Value> = rows
        .into_iter()
        .map(|(src, dst, items, statements, atoms, chunks)| ((src, dst), provenance(items, statements, &atoms, &chunks)))
        .collect();
    let lock = if write == Write::Apply { " FOR UPDATE" } else { "" };
    let stored: Vec<(Uuid, Uuid, Uuid, String, Value)> = sqlx::query_as(&format!(
        "SELECT edge_id, src_node_id, dst_node_id, status, provenance FROM relationship_edge \
          WHERE graph_id = $1 AND predicate = $2{lock}"
    ))
    .bind(graph_id)
    .bind(CO_OCCURS_WITH)
    .fetch_all(&mut **tx)
    .await?;
    let stored = stored
        .into_iter()
        .map(|(edge_id, src, dst, status, provenance)| ((src, dst), Stored { edge_id, status, provenance }))
        .collect();
    let desired_count = desired.len();
    let plan = plan(desired, stored);
    let mut out = Refresh {
        desired: desired_count,
        inserted: plan.insert.len(),
        updated: plan.update.len(),
        deleted: plan.delete.len(),
        unchanged: plan.unchanged,
        rejected_kept: plan.rejected_kept,
    };
    if write == Write::DryRun || out.changed() == 0 {
        return Ok(out);
    }

    let mut ops: Vec<NewOp> = Vec::with_capacity(out.changed());
    if !plan.delete.is_empty() {
        let deleted: Vec<(Uuid, DateTime<Utc>)> = sqlx::query_as(
            "DELETE FROM relationship_edge WHERE graph_id = $1 AND predicate = $2 AND edge_id = ANY($3) \
             RETURNING edge_id, now()",
        )
        .bind(graph_id)
        .bind(CO_OCCURS_WITH)
        .bind(&plan.delete)
        .fetch_all(&mut **tx)
        .await?;
        out.deleted = deleted.len();
        ops.extend(deleted.iter().map(|(id, at)| {
            sync::relationship_edge_delete_op(Uuid::new_v4(), *id, graph_id, *at, CLOUD_DEVICE_ID)
        }));
    }
    if !plan.update.is_empty() {
        let (ids, provenances): (Vec<Uuid>, Vec<Value>) = plan.update.into_iter().unzip();
        let updated: Vec<EdgeRow> = sqlx::query_as(
            "UPDATE relationship_edge e SET provenance = u.provenance, updated_at = now(), version = e.version + 1 \
               FROM unnest($3::uuid[], $4::jsonb[]) AS u(edge_id, provenance) \
              WHERE e.graph_id = $1 AND e.predicate = $2 AND e.edge_id = u.edge_id \
             RETURNING e.edge_id, e.src_node_id, e.dst_node_id, e.created_at, e.updated_at, e.version, e.provenance",
        )
        .bind(graph_id)
        .bind(CO_OCCURS_WITH)
        .bind(&ids)
        .bind(&provenances)
        .fetch_all(&mut **tx)
        .await?;
        out.updated = updated.len();
        ops.extend(updated.iter().map(|row| upsert_op(graph_id, row)));
    }
    if !plan.insert.is_empty() {
        let mut srcs = Vec::with_capacity(plan.insert.len());
        let mut dsts = Vec::with_capacity(plan.insert.len());
        let mut provenances = Vec::with_capacity(plan.insert.len());
        for (src, dst, p) in plan.insert {
            srcs.push(src);
            dsts.push(dst);
            provenances.push(p);
        }
        let inserted: Vec<EdgeRow> = sqlx::query_as(
            "INSERT INTO relationship_edge (graph_id, src_node_id, predicate, dst_node_id, origin, status, confidence, provenance) \
             SELECT $1, i.src, $2, i.dst, 'inferred', 'confirmed', $3, i.provenance \
               FROM unnest($4::uuid[], $5::uuid[], $6::jsonb[]) AS i(src, dst, provenance) \
             ON CONFLICT (graph_id, src_node_id, predicate, dst_node_id) DO NOTHING \
             RETURNING edge_id, src_node_id, dst_node_id, created_at, updated_at, version, provenance",
        )
        .bind(graph_id)
        .bind(CO_OCCURS_WITH)
        .bind(CONFIDENCE)
        .bind(&srcs)
        .bind(&dsts)
        .bind(&provenances)
        .fetch_all(&mut **tx)
        .await?;
        out.inserted = inserted.len();
        ops.extend(inserted.iter().map(|row| upsert_op(graph_id, row)));
    }
    append_ops_tx(tx, &ops).await?;
    Ok(out)
}

#[derive(Debug, Default)]
pub struct BackfillOutcome {
    pub total: Refresh,
    pub graphs: usize,
    pub failed: Vec<(Uuid, String)>,
}

/// Operator backfill over one database: refresh every graph (or one), each in
/// its own transaction, `READ ONLY` and rolled back under `DryRun`. Runs as
/// the caller's role; the bin requires the database owner.
pub async fn backfill(pool: &sqlx::PgPool, graph_id: Option<Uuid>, write: Write) -> Result<BackfillOutcome, sqlx::Error> {
    let graphs: Vec<Uuid> = sqlx::query_scalar("SELECT id FROM graphs WHERE $1::uuid IS NULL OR id = $1 ORDER BY id")
        .bind(graph_id)
        .fetch_all(pool)
        .await?;
    let mut out = BackfillOutcome { graphs: graphs.len(), ..BackfillOutcome::default() };
    for graph in graphs {
        let result = async {
            let mut tx = pool.begin().await?;
            if write == Write::DryRun {
                sqlx::query("SET TRANSACTION READ ONLY").execute(&mut *tx).await?;
            }
            sqlx::query("SET LOCAL statement_timeout = '120s'").execute(&mut *tx).await?;
            let refresh = refresh_tx(&mut tx, graph, write).await?;
            match write {
                Write::Apply => tx.commit().await?,
                Write::DryRun => tx.rollback().await?,
            }
            Ok::<_, StoreError>(refresh)
        }
        .await;
        match result {
            Ok(r) => out.total.add(&r),
            Err(e) => out.failed.push((graph, e.to_string())),
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(n: u128) -> Uuid {
        Uuid::from_u128(n)
    }

    fn stored(edge: u128, status: &str, p: Value) -> Stored {
        Stored { edge_id: id(edge), status: status.into(), provenance: p }
    }

    #[test]
    fn plan_converges_and_leaves_rejected_pairs_alone() {
        let p1 = provenance(1, 1, &[id(100)], &[]);
        let p2 = provenance(2, 3, &[id(101), id(102)], &[id(200)]);
        let desired = HashMap::from([
            ((id(1), id(2)), p1.clone()),
            ((id(1), id(3)), p2.clone()),
            ((id(2), id(3)), p1.clone()),
            ((id(3), id(4)), p1.clone()),
        ]);
        let existing = HashMap::from([
            ((id(1), id(2)), stored(10, "confirmed", p1.clone())),
            ((id(1), id(3)), stored(11, "confirmed", p1.clone())),
            ((id(3), id(4)), stored(12, "rejected", p1.clone())),
            ((id(5), id(6)), stored(13, "confirmed", p1.clone())),
            ((id(6), id(7)), stored(14, "rejected", p1.clone())),
        ]);
        let got = plan(desired, existing);
        assert_eq!(got.insert, vec![(id(2), id(3), p1)]);
        assert_eq!(got.update, vec![(id(11), p2)]);
        assert_eq!(got.delete, vec![id(13)]);
        assert_eq!((got.unchanged, got.rejected_kept), (1, 1));
    }

    #[test]
    fn co_occurrence_is_never_traversable() {
        assert!(CONFIDENCE < crate::relationships::predicates::TRAVERSABLE_MIN_CONFIDENCE);
        assert!(!crate::relationships::predicates::is_traversable_predicate(CO_OCCURS_WITH));
    }
}
