//! Ocean models. Workspace counts preserve the legacy admission and type folding.
use super::dirty::DirtyTrigger;
use super::*;
pub struct WorkspaceCounts;
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceCountsParams {
    pub graph: Uuid,
}
impl Params for WorkspaceCountsParams {
    fn parse(
        mut path: BTreeMap<String, String>,
        mut query: BTreeMap<String, String>,
    ) -> Result<Self, Error> {
        let graph = path.remove("graph_id").or_else(|| path.remove("id"));
        if !path.is_empty() {
            return Err("unknown path parameter".into());
        }
        let q = query.remove("graph");
        // Existing clients send window=7d; counts have always been full-history.
        query.remove("window");
        if !query.is_empty() {
            return Err("unknown query parameter".into());
        }
        if graph.is_some() && q.is_some() && graph != q {
            return Err("conflicting graph parameters".into());
        }
        Ok(Self {
            graph: graph.or(q).ok_or("graph required")?.parse()?,
        })
    }
}
impl ReadModel for WorkspaceCounts {
    const NAME: &'static str = "workspace_counts";
    const PATH: &'static str = "/graphs/{graph_id}/stats";
    const VERSION: i32 = 1;
    type Params = WorkspaceCountsParams;
    type Payload = crate::store::WorkspaceCounts;
    fn account_graph(p: &Self::Params) -> Option<Uuid> {
        Some(p.graph)
    }
    fn graphs(_viewer: &Viewer, p: &Self::Params) -> Vec<Uuid> {
        vec![p.graph]
    }
    fn compute<'a>(cx: ComputeCx<'a>, p: &'a Self::Params) -> ComputeFuture<'a, Self::Payload> {
        Box::pin(async move {
            #[cfg(feature = "e2e")]
            super::note_compute();
            if cx.graphs != [p.graph] {
                return Err("compute graph mismatch".into());
            }
            let (reefs, atoms, sources): (Value, i64, i64) = sqlx::query_as(COUNTS_SQL)
                .bind(p.graph)
                .fetch_one(&mut **cx.tx)
                .await?;
            Ok(crate::store::workspace_counts_from_row(
                reefs, atoms, sources,
            ))
        })
    }
    fn empty(_p: &Self::Params) -> Self::Payload {
        crate::store::workspace_counts_from_row(serde_json::json!({}), 0, 0)
    }
    fn dirty_triggers() -> &'static [DirtyTrigger] {
        &[
            DirtyTrigger::Table("atoms"),
            DirtyTrigger::Table("nodes"),
            DirtyTrigger::Table("node_atoms"),
            DirtyTrigger::Table("node_semantic_atoms"),
            DirtyTrigger::Table("source_items"),
            DirtyTrigger::Table("source_versions"),
            DirtyTrigger::Table("source_version_chunks"),
        ]
    }
}
// Pure SELECT extracted from 0184. Do not call a cache/materialization writer
// in compute: PostgreSQL enforces READ ONLY for the entire transaction.
// Keep the legacy EXISTS correlated to this graph's candidate reef. OFFSET 0
// preserves EXISTS semantics while preventing the alternative hashed subplan
// from reading every tenant link. Each reef still stops at its first admitted
// support, using the existing node_id index. See the real-PG work-bound test.
pub const COUNTS_SQL: &str = r#" WITH permitted_atoms AS MATERIALIZED (
  SELECT atom_id FROM library_permitted_atoms($1,NULL::uuid[])
 ), semantic_nodes AS MATERIALIZED (
  SELECT DISTINCT l.node_id FROM node_semantic_atoms l
  JOIN served_semantic_atoms a ON a.semantic_atom_id=l.semantic_atom_id AND a.graph_id=l.graph_id
  WHERE l.graph_id= $1
 ), reefs AS (
  SELECT n.type,count(*) reef_count FROM nodes n
  WHERE n.graph_id= $1 AND n.type IS NOT NULL
    AND (n.node_id IN (SELECT node_id FROM semantic_nodes)
      OR EXISTS (SELECT 1 FROM node_atoms l WHERE l.node_id=n.node_id
                 AND l.atom_id IN (SELECT atom_id FROM permitted_atoms) OFFSET 0))
  GROUP BY n.type
 ), atom_sources AS (
  SELECT split_part(a.source_id,':chunk:',1) source_key,count(*) atoms
  FROM permitted_atoms p JOIN atoms a ON a.atom_id=p.atom_id WHERE a.graph_id= $1
  GROUP BY 1
 )
 SELECT COALESCE((SELECT jsonb_object_agg(type,reef_count) FROM reefs),'{}'::jsonb),
        COALESCE((SELECT sum(atoms) FROM atom_sources),0)::bigint,
        (SELECT count(*) FROM atom_sources)
"#;
