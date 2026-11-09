//! G4 stock fold: merge relationship-minted sub-topic reefs into their
//! unique same-coral parent (`docs/plans/knowledge-graph-connectivity.md` §12.5).
//!
//! Same rule as G2 ([`crate::subtopic::unique_bind_parent`]). Unique parent
//! → `Store::merge_nodes` (aliases survive). Ambiguous or none → leave.
//! No LLM. Not `suggest_pair` (its hub guard refuses these folds).
//!
//! # Child set
//!
//! An entity reef is a fold child when it is not the self reef, has no
//! `body.entity_type` (relationship mint never set one), is an endpoint of
//! at least one *established* current — `origin = 'stated'` (any status:
//! the relationship pass wrote it) or `origin = 'inferred'` with
//! `status = 'confirmed'`; an inferred edge still `suggested` or already
//! `rejected` does not make a reef a child — and has `coral_count ≤ 8`. That
//! last bound is the measured ceiling of relationship-minted reefs after
//! the G1 provenance backfill (max 7 on the primary ocean); untyped
//! entity-pass hubs such as Apify sit well above it and stay parents.
//!
//! Parent candidates are other entity reefs on a shared coral that are
//! *not* themselves children. The self reef is never a parent.
//!
//! `backend/src/bin/fold_subtopics.rs` is a thin argument-parse + DSN
//! wrapper around [`preview`]/[`execute`]. Default is dry-run.

use std::collections::{HashMap, HashSet};

use sqlx::PgPool;
use uuid::Uuid;

use crate::store::{PostgresStore, Store};
use crate::subtopic::{unique_bind_parent, SameCoralReef};

/// Relationship-minted reefs in the measured ocean have coral_count ≤ 7
/// after G1. 8 is strictly above that set and below untyped entity-pass
/// hubs (Apify at 56, etc.) so those hubs stay parents.
pub const MAX_CHILD_CORALS: i32 = 8;

/// Local databases `--execute` never touches, flag or no flag: the live
/// local backend DB and the frozen post-backfill analysis copy.
const REFUSED_EXECUTE_DBS: &[&str] = &["coralswarm_prodclone", "coralswarm_kg_spike"];

/// Scratch copies `--execute` may write without `--i-am-sure`: the G1–G4
/// rehearsal DB, and dated prod-dump copies named per plan §5
/// (`coralswarm_prodclone_YYYYMMDD`).
const KNOWN_SCRATCH_DBS: &[&str] = &["coralswarm_kg_guard"];
const DATED_CLONE_PREFIX: &str = "coralswarm_prodclone_";

/// Refuse to run under a role that RLS would silently filter.
pub async fn assert_policy_bypass(pool: &PgPool) -> Result<(), String> {
    crate::self_reef_backfill::assert_policy_bypass(pool).await
}

/// How `--execute` classified the connected database.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecuteTarget {
    /// A known scratch copy — writes allowed.
    Scratch(String),
    /// Anything else (the live Neon name included) — writes allowed only
    /// with `--i-am-sure`.
    Unknown(String),
}

impl ExecuteTarget {
    pub fn name(&self) -> &str {
        match self {
            ExecuteTarget::Scratch(n) | ExecuteTarget::Unknown(n) => n,
        }
    }
}

/// Pure classification of a database name for `--execute`. `Err` is a hard
/// refusal; `Ok(Unknown)` needs `--i-am-sure` at the call site.
pub fn classify_execute_db(name: &str) -> Result<ExecuteTarget, String> {
    if REFUSED_EXECUTE_DBS.contains(&name) {
        return Err(format!(
            "REFUSING --execute against `{name}`. Point MIGRATOR_DATABASE_URL at \
             coralswarm_kg_guard (scratch), a dated coralswarm_prodclone_YYYYMMDD copy, \
             or the Neon migrator DSN with --i-am-sure."
        ));
    }
    let dated_clone = name
        .strip_prefix(DATED_CLONE_PREFIX)
        .is_some_and(|d| d.len() == 8 && d.bytes().all(|b| b.is_ascii_digit()));
    if KNOWN_SCRATCH_DBS.contains(&name) || dated_clone {
        Ok(ExecuteTarget::Scratch(name.to_string()))
    } else {
        Ok(ExecuteTarget::Unknown(name.to_string()))
    }
}

/// Refuse `--execute` against the live local backend DB and the frozen
/// post-backfill copy; require `i_am_sure` for any database name that is
/// not a known scratch copy (plan §13.5 — this is what stands between a
/// mistyped DSN and a merge on live Neon). Dry-run never calls this.
/// Returns the classified target so the caller can print the name.
pub async fn assert_execute_db(pool: &PgPool, i_am_sure: bool) -> Result<ExecuteTarget, String> {
    let name: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(pool)
        .await
        .map_err(|e| format!("current_database failed: {e}"))?;
    let target = classify_execute_db(&name)?;
    if let ExecuteTarget::Unknown(n) = &target {
        if !i_am_sure {
            return Err(format!(
                "REFUSING --execute against `{n}`: not a known scratch copy. If this is the \
                 intended live target, have a second person read the name back and re-run \
                 with --i-am-sure."
            ));
        }
    }
    Ok(target)
}

/// One child → parent fold.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FoldPair {
    pub graph_id: Uuid,
    pub child_id: Uuid,
    pub parent_id: Uuid,
    pub child_title: String,
    pub parent_title: String,
}

/// Dry-run / plan counts.
#[derive(Debug, Clone, Default)]
pub struct FoldPlan {
    pub pairs: Vec<FoldPair>,
    pub ambiguous: u64,
    pub none: u64,
    pub child_count: u64,
}

/// What `--execute` wrote.
#[derive(Debug, Clone, Default)]
pub struct ExecuteReport {
    pub merged: u64,
    pub moved_atoms: u64,
    pub skipped_missing: u64,
}

#[derive(Debug, Clone)]
struct ReefRow {
    node_id: Uuid,
    graph_id: Uuid,
    title: String,
    is_self: bool,
    entity_type: Option<String>,
    coral_count: i32,
    is_edge: bool,
}

pub fn is_fold_child(
    is_self: bool,
    entity_type: Option<&str>,
    coral_count: i32,
    is_edge: bool,
) -> bool {
    !is_self
        && entity_type
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .is_none()
        && coral_count <= MAX_CHILD_CORALS
        && is_edge
}

fn plan_folds(reefs: &[ReefRow], node_atoms: &[(Uuid, Uuid)]) -> FoldPlan {
    let mut atoms_of: HashMap<Uuid, Vec<Uuid>> = HashMap::new();
    let mut nodes_of: HashMap<Uuid, HashSet<Uuid>> = HashMap::new();
    for (node_id, atom_id) in node_atoms {
        atoms_of.entry(*node_id).or_default().push(*atom_id);
        nodes_of.entry(*atom_id).or_default().insert(*node_id);
    }

    let by_id: HashMap<Uuid, &ReefRow> = reefs.iter().map(|r| (r.node_id, r)).collect();
    let child_ids: HashSet<Uuid> = reefs
        .iter()
        .filter(|r| {
            is_fold_child(
                r.is_self,
                r.entity_type.as_deref(),
                r.coral_count,
                r.is_edge,
            )
        })
        .map(|r| r.node_id)
        .collect();

    let mut plan = FoldPlan {
        child_count: child_ids.len() as u64,
        ..FoldPlan::default()
    };

    for child in reefs.iter().filter(|r| child_ids.contains(&r.node_id)) {
        let mut same: HashSet<Uuid> = HashSet::new();
        if let Some(atoms) = atoms_of.get(&child.node_id) {
            for atom_id in atoms {
                if let Some(nodes) = nodes_of.get(atom_id) {
                    same.extend(nodes.iter().copied());
                }
            }
        }
        same.remove(&child.node_id);
        let cands: Vec<SameCoralReef> = same
            .iter()
            .filter_map(|id| by_id.get(id).copied())
            .filter(|r| !child_ids.contains(&r.node_id))
            .map(|r| SameCoralReef {
                node_id: r.node_id,
                title: r.title.clone(),
                is_self: r.is_self,
            })
            .collect();
        match unique_bind_parent(&child.title, &cands) {
            Some(parent_id) if parent_id != child.node_id => {
                let parent_title = by_id
                    .get(&parent_id)
                    .map(|p| p.title.clone())
                    .unwrap_or_default();
                plan.pairs.push(FoldPair {
                    graph_id: child.graph_id,
                    child_id: child.node_id,
                    parent_id,
                    child_title: child.title.clone(),
                    parent_title,
                });
            }
            _ => {
                let hits: Vec<&SameCoralReef> = cands
                    .iter()
                    .filter(|r| {
                        !r.is_self
                            && (r.title.trim().to_lowercase() == child.title.trim().to_lowercase()
                                || crate::subtopic::whole_word_contains(&child.title, &r.title))
                    })
                    .collect();
                if hits.len() > 1 {
                    plan.ambiguous += 1;
                } else {
                    plan.none += 1;
                }
            }
        }
    }

    plan.pairs.sort_by(|a, b| {
        a.graph_id
            .cmp(&b.graph_id)
            .then(a.child_title.cmp(&b.child_title))
            .then(a.child_id.cmp(&b.child_id))
    });
    plan
}

async fn load_reefs(pool: &PgPool, graph_id: Option<Uuid>) -> Result<Vec<ReefRow>, sqlx::Error> {
    let rows: Vec<(Uuid, Uuid, String, bool, Option<String>, i32, bool)> = sqlx::query_as(
        "SELECT n.node_id, n.graph_id, n.title, \
                COALESCE(n.node_id = g.self_node_id, false) AS is_self, \
                NULLIF(BTRIM(n.body->>'entity_type'), '') AS entity_type, \
                n.coral_count, \
                EXISTS ( \
                    SELECT 1 FROM relationship_edge e \
                     WHERE (e.origin = 'stated' \
                            OR (e.origin = 'inferred' AND e.status = 'confirmed')) \
                       AND (e.src_node_id = n.node_id OR e.dst_node_id = n.node_id) \
                       AND e.predicate <> 'co_occurs_with' \
                ) AS is_edge \
           FROM nodes n \
           JOIN graphs g ON g.id = n.graph_id \
          WHERE n.type = 'entity' \
            AND ($1::uuid IS NULL OR n.graph_id = $1)",
    )
    .bind(graph_id)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(
            |(node_id, graph_id, title, is_self, entity_type, coral_count, is_edge)| ReefRow {
                node_id,
                graph_id,
                title,
                is_self,
                entity_type,
                coral_count,
                is_edge,
            },
        )
        .collect())
}

async fn load_node_atoms(
    pool: &PgPool,
    graph_id: Option<Uuid>,
) -> Result<Vec<(Uuid, Uuid)>, sqlx::Error> {
    sqlx::query_as(
        "SELECT na.node_id, na.atom_id \
           FROM node_atoms na \
           JOIN nodes n ON n.node_id = na.node_id \
          WHERE n.type = 'entity' \
            AND ($1::uuid IS NULL OR n.graph_id = $1)",
    )
    .bind(graph_id)
    .fetch_all(pool)
    .await
}

pub async fn preview(pool: &PgPool, graph_id: Option<Uuid>) -> Result<FoldPlan, sqlx::Error> {
    let reefs = load_reefs(pool, graph_id).await?;
    let atoms = load_node_atoms(pool, graph_id).await?;
    Ok(plan_folds(&reefs, &atoms))
}

/// Preview once, then merge. Convenience for callers that have not already
/// displayed a plan; the bin uses [`execute_plan`] with the plan it printed
/// so the operator merges exactly what they read.
pub async fn execute(
    pool: &PgPool,
    graph_id: Option<Uuid>,
    limit: Option<u64>,
) -> Result<ExecuteReport, String> {
    let plan = preview(pool, graph_id)
        .await
        .map_err(|e| format!("preview failed: {e}"))?;
    execute_plan(pool, plan, limit).await
}

/// Merge every unique-parent pair in `plan`, in plan order, up to `limit`.
/// Takes the plan by value so the set of merges is the one the caller
/// already showed the operator — no second scan that could pick up
/// concurrent reef/coral/current changes between "display" and "write".
/// Per pair, the child and parent are re-checked for existence right
/// before the merge (a child folded away by an earlier pair in the same
/// run is skipped, a vanished parent stops the run).
///
/// The caller must have passed [`assert_execute_db`] first — this function
/// does not re-check the database name so the bin can print the target
/// before any write.
pub async fn execute_plan(
    pool: &PgPool,
    plan: FoldPlan,
    limit: Option<u64>,
) -> Result<ExecuteReport, String> {
    let store = PostgresStore::new(pool.clone());
    let take = limit.unwrap_or(u64::MAX) as usize;
    let mut report = ExecuteReport::default();
    for (i, pair) in plan.pairs.into_iter().take(take).enumerate() {
        let child_exists: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM nodes WHERE node_id = $1)")
                .bind(pair.child_id)
                .fetch_one(pool)
                .await
                .map_err(|e| format!("child exists check failed: {e}"))?;
        if !child_exists {
            report.skipped_missing += 1;
            continue;
        }
        let parent_exists: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM nodes WHERE node_id = $1)")
                .bind(pair.parent_id)
                .fetch_one(pool)
                .await
                .map_err(|e| format!("parent exists check failed: {e}"))?;
        if !parent_exists {
            return Err(format!(
                "parent {} for child {} is gone; stopping after {} merges",
                pair.parent_id, pair.child_id, report.merged
            ));
        }
        match store.merge_nodes(pair.parent_id, pair.child_id).await {
            Ok(moved) => {
                report.merged += 1;
                report.moved_atoms += moved;
            }
            Err(e) => {
                return Err(format!(
                    "merge {} → {} failed after {} merges: {e}",
                    pair.child_id, pair.parent_id, report.merged
                ));
            }
        }
        if (i + 1) % 50 == 0 {
            eprintln!(
                "fold_subtopics: merged {} so far (last {} → {})",
                report.merged,
                display_title(&pair.child_title),
                display_title(&pair.parent_title)
            );
        }
    }
    Ok(report)
}

/// Redact `@` titles so dry-run samples never print email addresses.
pub fn display_title(title: &str) -> String {
    if title.contains('@') {
        "<email>".to_string()
    } else {
        title.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reef(
        id: Uuid,
        graph: Uuid,
        title: &str,
        is_self: bool,
        et: Option<&str>,
        cc: i32,
        edge: bool,
    ) -> ReefRow {
        ReefRow {
            node_id: id,
            graph_id: graph,
            title: title.into(),
            is_self,
            entity_type: et.map(|s| s.to_string()),
            coral_count: cc,
            is_edge: edge,
        }
    }

    #[test]
    fn execute_db_classification() {
        assert!(classify_execute_db("coralswarm_prodclone").is_err());
        assert!(classify_execute_db("coralswarm_kg_spike").is_err());
        assert_eq!(
            classify_execute_db("coralswarm_kg_guard"),
            Ok(ExecuteTarget::Scratch("coralswarm_kg_guard".into()))
        );
        assert_eq!(
            classify_execute_db("coralswarm_prodclone_20260914"),
            Ok(ExecuteTarget::Scratch("coralswarm_prodclone_20260914".into()))
        );
        // Not eight digits → not a dated clone → needs --i-am-sure.
        assert_eq!(
            classify_execute_db("coralswarm_prodclone_v2"),
            Ok(ExecuteTarget::Unknown("coralswarm_prodclone_v2".into()))
        );
        assert_eq!(
            classify_execute_db("coralswarm"),
            Ok(ExecuteTarget::Unknown("coralswarm".into()))
        );
    }

    #[test]
    fn fat_untyped_hub_is_not_a_child() {
        assert!(!is_fold_child(false, None, 56, true));
        assert!(is_fold_child(false, None, 1, true));
        assert!(!is_fold_child(true, None, 1, true));
        assert!(!is_fold_child(false, Some("topic"), 1, true));
        assert!(!is_fold_child(false, None, 1, false));
    }

    #[test]
    fn unique_parent_folds_child_into_entity_pass() {
        let g = Uuid::new_v4();
        let parent = Uuid::new_v4();
        let child = Uuid::new_v4();
        let coral = Uuid::new_v4();
        let reefs = vec![
            reef(parent, g, "p2ptext", false, Some("project"), 973, true),
            reef(child, g, "p2ptext KB revamp", false, None, 1, true),
        ];
        let plan = plan_folds(&reefs, &[(parent, coral), (child, coral)]);
        assert_eq!(plan.pairs.len(), 1);
        assert_eq!(plan.pairs[0].parent_id, parent);
        assert_eq!(plan.pairs[0].child_id, child);
        assert_eq!(plan.ambiguous, 0);
        assert_eq!(plan.none, 0);
    }

    #[test]
    fn hyphen_does_not_fold_connect_into_coralswarm() {
        let g = Uuid::new_v4();
        let parent = Uuid::new_v4();
        let child = Uuid::new_v4();
        let coral = Uuid::new_v4();
        let reefs = vec![
            reef(parent, g, "CoralSwarm", false, Some("project"), 100, true),
            reef(
                child,
                g,
                "coralswarm-connect installer",
                false,
                None,
                1,
                true,
            ),
        ];
        let plan = plan_folds(&reefs, &[(parent, coral), (child, coral)]);
        assert!(plan.pairs.is_empty());
        assert_eq!(plan.none, 1);
    }

    #[test]
    fn two_parents_are_ambiguous_not_folded() {
        let g = Uuid::new_v4();
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();
        let child = Uuid::new_v4();
        let coral = Uuid::new_v4();
        let reefs = vec![
            reef(a, g, "Slack", false, Some("topic"), 10, true),
            reef(b, g, "Cursor", false, Some("project"), 10, true),
            reef(child, g, "Slack versus Cursor probe", false, None, 1, true),
        ];
        let plan = plan_folds(&reefs, &[(a, coral), (b, coral), (child, coral)]);
        assert!(plan.pairs.is_empty());
        assert_eq!(plan.ambiguous, 1);
    }

    #[test]
    fn self_reef_is_never_a_parent() {
        let g = Uuid::new_v4();
        let owner = Uuid::new_v4();
        let child = Uuid::new_v4();
        let coral = Uuid::new_v4();
        let reefs = vec![
            reef(owner, g, "Alex Example", true, Some("person"), 100, true),
            reef(
                child,
                g,
                "Alex Example coding session",
                false,
                None,
                1,
                true,
            ),
        ];
        let plan = plan_folds(&reefs, &[(owner, coral), (child, coral)]);
        assert!(plan.pairs.is_empty());
        assert_eq!(plan.none, 1);
    }
}
