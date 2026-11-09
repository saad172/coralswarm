//! Reusable core of the `backfill_provenance_links` binary
//! (`docs/plans/knowledge-graph-connectivity.md` G1): insert the `node_atoms`
//! rows implied by `relationship_edge.provenance.atom_ids` (both endpoints)
//! and `nodes.body.born.atom_id`, so relationship-minted reefs carry their
//! evidence coral.
//!
//! `backend/src/bin/backfill_provenance_links.rs` is a thin argument-parse +
//! DSN wrapper around [`preview`]/[`execute`]. The coral_count trigger stays
//! on — do not set `session_replication_role = replica`.
//!
//! # Selection
//!
//! A (node, atom) pair is missing when:
//! - it is implied by an edge's `provenance.atom_ids` (src or dst) **or** by
//!   `nodes.body.born.atom_id`,
//! - the atom exists in the same graph as the node,
//! - no `node_atoms` row already holds the pair.
//!
//! Missing atoms are skipped (the analysis found zero). `ON CONFLICT DO
//! NOTHING` makes `--execute` idempotent.
//!
//! `--graph-id` narrows to one ocean. Default is every graph on the DSN.

use sqlx::PgPool;
use uuid::Uuid;

/// Refuse to run under a role that RLS would silently filter. Delegates to
/// the same preflight `backfill_self_reefs` uses (`nodes`).
pub async fn assert_policy_bypass(pool: &PgPool) -> Result<(), String> {
    crate::self_reef_backfill::assert_policy_bypass(pool).await
}

/// Local databases `--execute` must never touch: the live local backend DB
/// and the frozen post-backfill analysis copy. Dry-run is allowed anywhere.
const REFUSED_EXECUTE_DBS: &[&str] = &["coralswarm_prodclone", "coralswarm_kg_spike"];

/// `current_database()` on the DSN — printed before every `--execute` so the
/// operator (and the second person, for the live Neon run) can read the
/// target name back; see `docs/plans/graph-work-production-plan.md` §5.
pub async fn current_database(pool: &PgPool) -> Result<String, String> {
    sqlx::query_scalar("SELECT current_database()")
        .fetch_one(pool)
        .await
        .map_err(|e| format!("current_database failed: {e}"))
}

/// Refuse `--execute` against [`REFUSED_EXECUTE_DBS`]. Returns the database
/// name on success so the caller can print it.
pub async fn assert_execute_db(pool: &PgPool) -> Result<String, String> {
    let name = current_database(pool).await?;
    if REFUSED_EXECUTE_DBS.contains(&name.as_str()) {
        return Err(format!(
            "REFUSING --execute against `{name}`. Point MIGRATOR_DATABASE_URL at a fresh \
             prod dump copy (rehearsal) or the Neon migrator DSN (live)."
        ));
    }
    Ok(name)
}

/// UUID-shaped text. Rejects garbage in `provenance.atom_ids` / `born.atom_id`
/// before the `::uuid` cast would abort the whole statement.
const UUID_RE: &str =
    "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$";

/// Implied (node, atom, graph) pairs that are not yet in `node_atoms` and
/// whose atom exists in the same graph as the node.
const MISSING_LINKS_SQL: &str = "
WITH implied AS (
    SELECT e.src_node_id AS node_id, t.atom_id, e.graph_id
      FROM relationship_edge e
      CROSS JOIN LATERAL (
          SELECT (x)::uuid AS atom_id
            FROM jsonb_array_elements_text(COALESCE(e.provenance->'atom_ids', '[]'::jsonb)) AS x
           WHERE x ~ $2
      ) t
     WHERE ($1::uuid IS NULL OR e.graph_id = $1)
    UNION
    SELECT e.dst_node_id, t.atom_id, e.graph_id
      FROM relationship_edge e
      CROSS JOIN LATERAL (
          SELECT (x)::uuid AS atom_id
            FROM jsonb_array_elements_text(COALESCE(e.provenance->'atom_ids', '[]'::jsonb)) AS x
           WHERE x ~ $2
      ) t
     WHERE ($1::uuid IS NULL OR e.graph_id = $1)
    UNION
    SELECT n.node_id, (n.body #>> '{born,atom_id}')::uuid, n.graph_id
      FROM nodes n
     WHERE n.body #>> '{born,atom_id}' ~ $2
       AND ($1::uuid IS NULL OR n.graph_id = $1)
)
SELECT i.node_id, i.atom_id
  FROM implied i
  JOIN atoms a ON a.atom_id = i.atom_id AND a.graph_id = i.graph_id
  JOIN nodes n ON n.node_id = i.node_id AND n.graph_id = i.graph_id
 WHERE NOT EXISTS (
       SELECT 1 FROM node_atoms na
        WHERE na.node_id = i.node_id AND na.atom_id = i.atom_id
 )
";

/// Dry-run counts for one DSN (optionally one graph).
#[derive(Debug, Clone, Copy, Default)]
pub struct Preview {
    pub missing_links: i64,
    pub distinct_nodes: i64,
    pub distinct_atoms: i64,
    /// Implied pairs whose atom does not exist **in the node's graph** —
    /// deleted atoms and cross-graph references alike. `MISSING_LINKS_SQL`
    /// skips both, so this is the count of what the backfill will not
    /// repair; the plan's go/no-go expects 0.
    pub implied_atom_gone: i64,
}

/// What `--execute` wrote.
#[derive(Debug, Clone, Copy, Default)]
pub struct ExecuteReport {
    pub inserted: u64,
}

pub async fn preview(pool: &PgPool, graph_id: Option<Uuid>) -> Result<Preview, sqlx::Error> {
    let row: (i64, i64, i64) = sqlx::query_as(&format!(
        "SELECT count(*)::bigint, count(DISTINCT node_id)::bigint, count(DISTINCT atom_id)::bigint \
         FROM ({MISSING_LINKS_SQL}) s"
    ))
    .bind(graph_id)
    .bind(UUID_RE)
    .fetch_one(pool)
    .await?;

    let gone: i64 = sqlx::query_scalar(
        "
        WITH implied AS (
            SELECT e.src_node_id AS node_id, t.atom_id, e.graph_id
              FROM relationship_edge e
              CROSS JOIN LATERAL (
                  SELECT (x)::uuid AS atom_id
                    FROM jsonb_array_elements_text(COALESCE(e.provenance->'atom_ids', '[]'::jsonb)) AS x
                   WHERE x ~ $2
              ) t
             WHERE ($1::uuid IS NULL OR e.graph_id = $1)
            UNION
            SELECT e.dst_node_id, t.atom_id, e.graph_id
              FROM relationship_edge e
              CROSS JOIN LATERAL (
                  SELECT (x)::uuid AS atom_id
                    FROM jsonb_array_elements_text(COALESCE(e.provenance->'atom_ids', '[]'::jsonb)) AS x
                   WHERE x ~ $2
              ) t
             WHERE ($1::uuid IS NULL OR e.graph_id = $1)
            UNION
            SELECT n.node_id, (n.body #>> '{born,atom_id}')::uuid, n.graph_id
              FROM nodes n
             WHERE n.body #>> '{born,atom_id}' ~ $2
               AND ($1::uuid IS NULL OR n.graph_id = $1)
        )
        SELECT count(*)::bigint
          FROM implied i
         WHERE NOT EXISTS (
               SELECT 1 FROM atoms a
                WHERE a.atom_id = i.atom_id AND a.graph_id = i.graph_id
         )
        ",
    )
    .bind(graph_id)
    .bind(UUID_RE)
    .fetch_one(pool)
    .await?;

    Ok(Preview {
        missing_links: row.0,
        distinct_nodes: row.1,
        distinct_atoms: row.2,
        implied_atom_gone: gone,
    })
}

pub async fn execute(pool: &PgPool, graph_id: Option<Uuid>) -> Result<ExecuteReport, sqlx::Error> {
    let result = sqlx::query(&format!(
        "INSERT INTO node_atoms (node_id, atom_id) \
         SELECT node_id, atom_id FROM ({MISSING_LINKS_SQL}) s \
         ON CONFLICT DO NOTHING"
    ))
    .bind(graph_id)
    .bind(UUID_RE)
    .execute(pool)
    .await?;
    Ok(ExecuteReport {
        inserted: result.rows_affected(),
    })
}
