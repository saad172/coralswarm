//! Mirror managed-extraction reef links into `node_atoms`, the materialized
//! "corals of a reef" relation.
//!
//! Managed extraction (`workers::source_atoms`) links reefs to *semantic*
//! atoms through `node_semantic_atoms`. Reef coral lists, MCP `get_reef`, the
//! atlas, the digest trigger (through `nodes.coral_count`), ER facts, Ask's
//! node expansion and the Library all read corals from `node_atoms`. Every
//! semantic reef link is therefore mirrored here as `node_atoms(node, chunk)`
//! for the chunk atoms of the cited source version.
//!
//! # Why a table, not a `reef_corals` view
//!
//! Source-agnostic pipeline PR 11 planned to replace this mirror with a view
//! over `node_atoms ∪ (node_semantic_atoms → semantic_atoms →
//! source_version_chunks)`. Measured read-only on the coralswarm tenant
//! (2026-10-20, PostgreSQL 18, warm cache), that view costs the hot reads 2.5
//! to 40 times more, because every semantic link fans out to every chunk of
//! its version at read time (1,023,760 derived pairs from 76,424 links):
//! per-reef coral counts on a 50-reef Library page went from 0.4 s to 1.0-1.9
//! s, and a 2,000-chunk neighbourhood join from 0.05-0.28 s to 2.1-2.8 s. The
//! mirror is that fan-out computed once, at write time. On the same day the
//! two definitions agreed exactly: no derived pair was missing from
//! `node_atoms`, so no reef's coral list differs between them.
//!
//! # Passage → chunk mapping: all chunks of the version
//!
//! `source_version_chunks` maps a version to its retrieval chunk atoms but
//! carries no character offsets, and chunk text is stored with a header
//! prefix, optionally in a blob and optionally encrypted. A semantic atom's
//! passage (`semantic_atom_passages.start_char/end_char`) therefore cannot be
//! mapped to one chunk exactly, so a reef is linked to EVERY chunk atom of
//! the version its semantic atom cites. Refine this when chunks carry
//! offsets.
//!
//! A version with no `source_version_chunks` rows (its chunks were legacy
//! duplicates that ingest deliberately did not adopt, see
//! `source_evidence::link_verified_chunks_tx`) links nothing. Guessing a
//! chunk from `atoms.source_id` is exactly the positional adoption that
//! function forbids.
//!
//! # Write path
//!
//! Both the live worker and the backfill go through
//! [`Store::link_node_atoms_batch_scoped`]. That is the legacy writer's
//! insert (`INSERT INTO node_atoms (node_id, atom_id) ... ON CONFLICT DO
//! NOTHING`) plus one `node_atom` sync op per new row, in the caller's
//! transaction. The `(node_id, atom_id)` primary key makes repeats no-ops,
//! and the `node_atoms` triggers (`coral_count`, `last_happened_at`, and
//! through `coral_count` the digest enqueue) fire exactly as
//! they did for the legacy writer.
//!
//! # Kill switch
//!
//! `SOURCE_ATOMS_LINK_NODE_ATOMS=false` (or `0`/`off`/`no`) stops the live
//! worker's mirror, which empties new reefs on every coral reader above and
//! drops the chunk citations v2 relation edges carry. The backfill ignores
//! it, because it is an explicit operator action.

use std::collections::BTreeMap;

use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

use crate::store::{PostgresStore, Store, StoreError};

/// Kill-switch env var for the live dual write (default on).
pub const ENV_FLAG: &str = "SOURCE_ATOMS_LINK_NODE_ATOMS";

/// Default backfill page: (reef, source version) pairs per transaction.
pub const DEFAULT_BATCH_SIZE: i64 = 200;

/// Whether the live worker mirrors reef links into `node_atoms`. Read once
/// per persisted window (cheap), so tests can toggle it in-process.
pub fn enabled() -> bool {
    match std::env::var(ENV_FLAG) {
        Ok(v) => !matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "false" | "0" | "off" | "no"
        ),
        Err(_) => true,
    }
}

/// Chunk atoms of one source version, in the caller's (scoped) transaction.
pub(crate) async fn version_chunks_tx(
    tx: &mut Transaction<'_, Postgres>,
    graph_id: Uuid,
    version_id: Uuid,
) -> Result<Vec<Uuid>, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT atom_id FROM source_version_chunks \
         WHERE graph_id=$1 AND source_version_id=$2 ORDER BY atom_id",
    )
    .bind(graph_id)
    .bind(version_id)
    .fetch_all(&mut **tx)
    .await
}

/// Link each reef to its chunk atoms, in the caller's transaction. Callers
/// pass reefs in id order so two concurrent writers lock `nodes` rows
/// (through the `coral_count` trigger) in the same order. Returns rows
/// actually inserted.
pub(crate) async fn link_reefs_tx(
    store: &dyn Store,
    tx: &mut Transaction<'_, Postgres>,
    links: &[(Uuid, &[Uuid])],
) -> Result<u64, StoreError> {
    let mut inserted = 0;
    for (node_id, chunks) in links {
        inserted += store
            .link_node_atoms_batch_scoped(tx, *node_id, chunks)
            .await?;
    }
    Ok(inserted)
}

// ── Backfill ────────────────────────────────────────────────────────────────

/// (reef, source version) pairs whose semantic link is not yet fully
/// mirrored: the version has at least one chunk the reef is not linked to.
/// `$1` optional graph filter, `$2/$3` optional keyset cursor, `$4` limit.
const PENDING_PAIRS_SQL: &str = "
SELECT DISTINCT l.node_id, a.source_version_id, l.graph_id
  FROM node_semantic_atoms l
  JOIN semantic_atoms a ON a.graph_id = l.graph_id AND a.semantic_atom_id = l.semantic_atom_id
  JOIN nodes n ON n.node_id = l.node_id AND n.graph_id = l.graph_id
 WHERE ($1::uuid IS NULL OR l.graph_id = $1)
   AND ($2::uuid IS NULL OR (l.node_id, a.source_version_id) > ($2::uuid, $3::uuid))
   AND EXISTS (
         SELECT 1 FROM source_version_chunks c
          WHERE c.graph_id = l.graph_id AND c.source_version_id = a.source_version_id
            AND NOT EXISTS (SELECT 1 FROM node_atoms na
                             WHERE na.node_id = l.node_id AND na.atom_id = c.atom_id))
 ORDER BY l.node_id, a.source_version_id
 LIMIT $4
";

/// Dry-run counts for one database (optionally one graph).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Preview {
    /// Semantic atoms in scope.
    pub semantic_atoms: i64,
    /// Distinct (reef, version) semantic links in scope.
    pub reef_version_pairs: i64,
    /// Of those, pairs whose version has no `source_version_chunks` rows
    /// (nothing to bridge; see the module doc).
    pub pairs_without_chunks: i64,
    /// `node_atoms` rows the backfill would insert.
    pub missing_links: i64,
    /// Reefs that would gain at least one coral.
    pub reefs_touched: i64,
    /// Of those, reefs that have zero `node_atoms` rows today (invisible on
    /// every `node_atoms` reader).
    pub reefs_currently_empty: i64,
}

/// What `--execute` wrote.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Report {
    pub batches: u64,
    pub pairs: u64,
    pub inserted: u64,
}

/// Refuse to run under a role RLS would silently filter. The backfill walks
/// every graph in the database, so it needs the owner role (Neon
/// `neondb_owner` on a tenant, `MIGRATOR_DATABASE_URL` on the control plane)
/// on a table without FORCE ROW LEVEL SECURITY, or a BYPASSRLS role.
pub async fn assert_policy_bypass(pool: &PgPool) -> Result<(), String> {
    assert_policy_bypass_on(
        pool,
        &["nodes", "node_atoms", "node_semantic_atoms", "semantic_atoms", "source_version_chunks"],
    )
    .await
}

/// [`assert_policy_bypass`] for an explicit table list (shared with
/// `chunk_reconcile::backfill`).
pub async fn assert_policy_bypass_on(pool: &PgPool, tables: &[&str]) -> Result<(), String> {
    for &table in tables {
        let row: Option<(bool, bool, bool)> = sqlx::query_as(
            "SELECT \
                (SELECT rolsuper OR rolbypassrls FROM pg_roles WHERE rolname = current_user), \
                c.relowner = (SELECT oid FROM pg_roles WHERE rolname = current_user), \
                c.relforcerowsecurity \
             FROM pg_class c WHERE c.oid = to_regclass($1)",
        )
        .bind(table)
        .fetch_optional(pool)
        .await
        .map_err(|e| format!("preflight query failed: {e}"))?;
        let Some((bypasses, is_owner, forced)) = row else {
            return Err(format!(
                "no `{table}` table in this database. Apply migrate_control / migrate_tenants first."
            ));
        };
        if !(bypasses || (is_owner && !forced)) {
            return Err(format!(
                "REFUSING: this role does not bypass row-level security on `{table}` \
                 (superuser/BYPASSRLS = {bypasses}, owner = {is_owner}, FORCE = {forced}). \
                 RLS would hide rows and the backfill would under-report. Use the tenant owner \
                 DSN (tenant plane) or MIGRATOR_DATABASE_URL (control plane)."
            ));
        }
    }
    Ok(())
}

/// Counts only. Runs in a READ ONLY transaction, so it is safe against a
/// production DSN.
pub async fn preview(pool: &PgPool, graph_id: Option<Uuid>) -> Result<Preview, sqlx::Error> {
    let mut tx = pool.begin().await?;
    sqlx::query("SET TRANSACTION READ ONLY")
        .execute(&mut *tx)
        .await?;
    let row: (i64, i64, i64, i64, i64, i64) = sqlx::query_as(
        "
        WITH pairs AS (
            SELECT DISTINCT l.node_id, a.source_version_id, l.graph_id
              FROM node_semantic_atoms l
              JOIN semantic_atoms a ON a.graph_id = l.graph_id AND a.semantic_atom_id = l.semantic_atom_id
              JOIN nodes n ON n.node_id = l.node_id AND n.graph_id = l.graph_id
             WHERE ($1::uuid IS NULL OR l.graph_id = $1)
        ), missing AS (
            SELECT DISTINCT p.node_id, c.atom_id
              FROM pairs p
              JOIN source_version_chunks c
                ON c.graph_id = p.graph_id AND c.source_version_id = p.source_version_id
             WHERE NOT EXISTS (SELECT 1 FROM node_atoms na
                                WHERE na.node_id = p.node_id AND na.atom_id = c.atom_id)
        )
        SELECT
            (SELECT count(*) FROM semantic_atoms WHERE $1::uuid IS NULL OR graph_id = $1)::bigint,
            (SELECT count(*) FROM pairs)::bigint,
            (SELECT count(*) FROM pairs p WHERE NOT EXISTS (
                SELECT 1 FROM source_version_chunks c
                 WHERE c.graph_id = p.graph_id AND c.source_version_id = p.source_version_id))::bigint,
            (SELECT count(*) FROM missing)::bigint,
            (SELECT count(DISTINCT node_id) FROM missing)::bigint,
            (SELECT count(DISTINCT m.node_id) FROM missing m
              WHERE NOT EXISTS (SELECT 1 FROM node_atoms na WHERE na.node_id = m.node_id))::bigint
        ",
    )
    .bind(graph_id)
    .fetch_one(&mut *tx)
    .await?;
    tx.rollback().await?;
    Ok(Preview {
        semantic_atoms: row.0,
        reef_version_pairs: row.1,
        pairs_without_chunks: row.2,
        missing_links: row.3,
        reefs_touched: row.4,
        reefs_currently_empty: row.5,
    })
}

/// Insert the missing `node_atoms` rows, `batch_size` (reef, version) pairs
/// per committed transaction, walking a keyset cursor so every pair is
/// visited once per run. Idempotent: a re-run finds nothing pending, and a
/// row the live worker wrote concurrently is a primary-key no-op.
/// `max_batches` bounds a rehearsal. `on_batch` receives each batch's report
/// for progress output.
pub async fn execute(
    pool: &PgPool,
    graph_id: Option<Uuid>,
    batch_size: i64,
    max_batches: Option<u64>,
    mut on_batch: impl FnMut(&Report),
) -> Result<Report, StoreError> {
    let store = PostgresStore::new(pool.clone());
    let mut total = Report::default();
    let mut cursor: Option<(Uuid, Uuid)> = None;
    loop {
        if max_batches.is_some_and(|max| total.batches >= max) {
            break;
        }
        let mut tx = pool.begin().await?;
        let pairs: Vec<(Uuid, Uuid, Uuid)> = sqlx::query_as(PENDING_PAIRS_SQL)
            .bind(graph_id)
            .bind(cursor.map(|c| c.0))
            .bind(cursor.map(|c| c.1))
            .bind(batch_size.max(1))
            .fetch_all(&mut *tx)
            .await?;
        let Some(last) = pairs.last() else {
            tx.rollback().await?;
            break;
        };
        cursor = Some((last.0, last.1));
        let nodes: Vec<Uuid> = pairs.iter().map(|p| p.0).collect();
        let versions: Vec<Uuid> = pairs.iter().map(|p| p.1).collect();
        let graphs: Vec<Uuid> = pairs.iter().map(|p| p.2).collect();
        let grouped: Vec<(Uuid, Vec<Uuid>)> = sqlx::query_as(
            "SELECT p.node_id, array_agg(DISTINCT c.atom_id ORDER BY c.atom_id) \
               FROM unnest($1::uuid[], $2::uuid[], $3::uuid[]) AS p(node_id, version_id, graph_id) \
               JOIN source_version_chunks c \
                 ON c.graph_id = p.graph_id AND c.source_version_id = p.version_id \
              GROUP BY p.node_id ORDER BY p.node_id",
        )
        .bind(&nodes)
        .bind(&versions)
        .bind(&graphs)
        .fetch_all(&mut *tx)
        .await?;
        let grouped: BTreeMap<Uuid, Vec<Uuid>> = grouped.into_iter().collect();
        let links: Vec<(Uuid, &[Uuid])> = grouped
            .iter()
            .map(|(node, chunks)| (*node, chunks.as_slice()))
            .collect();
        let inserted = link_reefs_tx(&store, &mut tx, &links).await?;
        tx.commit().await?;
        let batch = Report {
            batches: 1,
            pairs: pairs.len() as u64,
            inserted,
        };
        total.batches += 1;
        total.pairs += batch.pairs;
        total.inserted += inserted;
        on_batch(&batch);
    }
    Ok(total)
}
