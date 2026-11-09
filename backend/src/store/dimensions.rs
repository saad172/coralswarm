//! Aggregate dimensions registry — the single source of truth for the
//! "count / most / least / rank / trend / breakdown" answers.
//!
//! Background: the ask pipeline is `embed → retrieve top-K → synthesize`, which
//! can answer "what / who / where" from a relevance sample but never "how many /
//! which is most active". The data to answer those lives in storage (an atom
//! records its channel, author, source, scope, time; an edge its predicate and
//! review status) but was never grouped or surfaced to synthesis — so the model
//! silently mis-answered aggregate questions as "missing data".
//!
//! This module fixes that *structurally*. Each aggregatable dimension is one
//! [`Dimension`] entry in [`DIMENSIONS`]. `Store::ocean_stats` iterates the
//! registry, runs the generated `GROUP BY` per backend (Postgres), and
//! feeds the top-N breakdowns into the SYSTEM CONTEXT block (see
//! [`crate::self_context`]). Adding a new breakdown is a single registry entry —
//! not edits across three files — and a CI test ([`tests`]) fails the build if a
//! stored-and-indexed atom/edge dimension ever ships without a registry entry,
//! so this class of gap cannot silently recur.

/// SQL dialect of the backend running the aggregate. The only cross-dialect
/// differences in our grouped queries are the bind-placeholder token and the
/// month-bucket date function; everything else is plain `GROUP BY`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dialect {
    /// Cloud: Postgres via sqlx (`$1`, `to_char(date_trunc(...))`).
    Pg,
}

impl Dialect {
    /// The single positional bind placeholder for `graph_id` (every query binds it once).
    fn placeholder(self) -> &'static str {
        match self {
            Dialect::Pg => "$1",
        }
    }

    /// Expression that buckets a timestamp column into a `YYYY-MM` month label.
    fn month_bucket(self, col: &str) -> String {
        match self {
            Dialect::Pg => format!("to_char(date_trunc('month', {col}), 'YYYY-MM')"),
        }
    }
}

/// How a dimension is grouped. Most dimensions are a single-column `GROUP BY`;
/// time and reef-size are the two structural shapes that need bespoke SQL.
#[derive(Debug, Clone, Copy)]
pub enum DimKind {
    /// `GROUP BY` a column on `atoms`. When `name` is set, the column is nullable
    /// (rows with NULL are excluded) and the human label falls back to the id.
    AtomColumn {
        group: &'static str,
        name: Option<&'static str>,
    },
    /// `GROUP BY` a column on `relationship_edge`. `exclude_rejected` drops
    /// rejected edges (meaningful for "relationships by type"; kept for the
    /// review-status breakdown so "how many rejected" is answerable).
    EdgeColumn {
        group: &'static str,
        exclude_rejected: bool,
    },
    /// Month buckets over an `atoms` timestamp column (recent-first).
    AtomTimeBucket { ts: &'static str },
    /// Top reefs by number of attached corals (`nodes` ⋈ `node_atoms`).
    ReefSize,
}

/// One aggregatable dimension. `lead_in` is the human phrase used when rendering
/// the breakdown into SYSTEM CONTEXT; `index` names the DB index backing the
/// grouping (used for documentation and the registry↔migration CI check).
#[derive(Debug, Clone, Copy)]
pub struct Dimension {
    pub key: &'static str,
    pub lead_in: &'static str,
    pub kind: DimKind,
    pub index: Option<&'static str>,
    pub top_n: i64,
}

/// The registry. Adding a row here is the *single act* that surfaces a new
/// aggregate breakdown across both store backends and the synthesis context.
pub const DIMENSIONS: &[Dimension] = &[
    Dimension {
        key: "channel",
        lead_in: "Most active channels (by message volume)",
        kind: DimKind::AtomColumn {
            group: "channel_id",
            name: Some("channel_name"),
        },
        index: Some("idx_atoms_graph_channel"),
        top_n: 8,
    },
    Dimension {
        key: "author",
        lead_in: "Most active people (by message volume)",
        kind: DimKind::AtomColumn {
            group: "author_id",
            name: Some("author_name"),
        },
        index: Some("idx_atoms_graph_author"),
        top_n: 8,
    },
    Dimension {
        key: "source",
        lead_in: "Corals by source",
        kind: DimKind::AtomColumn {
            group: "source_type",
            name: None,
        },
        index: Some("idx_atoms_graph_source"),
        top_n: 8,
    },
    Dimension {
        key: "scope",
        lead_in: "Corals by scope (private vs shared)",
        kind: DimKind::AtomColumn {
            group: "scope",
            name: None,
        },
        index: None,
        top_n: 4,
    },
    Dimension {
        key: "time",
        lead_in: "Activity by month (most recent first)",
        kind: DimKind::AtomTimeBucket { ts: "timestamp" },
        index: Some("idx_atoms_graph_timestamp"),
        top_n: 6,
    },
    Dimension {
        key: "predicate",
        lead_in: "Relationships by type",
        kind: DimKind::EdgeColumn {
            group: "predicate",
            exclude_rejected: true,
        },
        index: None,
        top_n: 8,
    },
    Dimension {
        key: "status",
        lead_in: "Relationships by review status",
        kind: DimKind::EdgeColumn {
            group: "status",
            exclude_rejected: false,
        },
        index: Some("idx_relationship_edge_graph_status"),
        top_n: 4,
    },
    Dimension {
        key: "reef_size",
        lead_in: "Largest reefs (most corals)",
        kind: DimKind::ReefSize,
        index: Some("idx_node_atoms_node_id"),
        top_n: 6,
    },
];

/// One computed breakdown: the dimension's `key`/`lead_in` plus its top-N
/// `(display, count)` entries, already ordered for rendering.
#[derive(Debug, Clone)]
pub struct DimensionBreakdown {
    pub key: &'static str,
    pub lead_in: &'static str,
    pub entries: Vec<(String, i64)>,
}

/// Build the grouped, top-N SQL for one dimension in the given dialect. Every
/// query binds `graph_id` exactly once (the dialect's placeholder) and selects
/// `(display TEXT, count BIGINT)` so both backends decode it identically. The
/// `top_n` limit is a trusted constant from the registry (embedded, not bound).
pub fn build_sql(dim: &Dimension, dialect: Dialect) -> String {
    let ph = dialect.placeholder();
    let n = dim.top_n;
    let atom_guard = if dialect == Dialect::Pg {
        " AND library_atom_permitted(graph_id,atom_id)"
    } else {
        ""
    };
    let edge_guard = if dialect == Dialect::Pg {
        " AND library_edge_permitted(graph_id,edge_id)"
    } else {
        ""
    };
    let reef_guard = if dialect == Dialect::Pg {
        " AND library_atom_permitted(n.graph_id,na.atom_id) AND library_node_title(n.graph_id,n.node_id) IS NOT NULL"
    } else {
        ""
    };
    let reef_title = if dialect == Dialect::Pg {
        "library_node_title(n.graph_id,n.node_id)"
    } else {
        "n.title"
    };
    match dim.kind {
        DimKind::AtomColumn {
            group,
            name: Some(name),
        } => format!(
            "SELECT COALESCE(NULLIF({name}, ''), {group}) AS d, COUNT(*) AS c \
             FROM atoms WHERE graph_id = {ph}{atom_guard} AND {group} IS NOT NULL \
             GROUP BY {group}, {name} ORDER BY c DESC, d ASC LIMIT {n}"
        ),
        DimKind::AtomColumn { group, name: None } => format!(
            "SELECT {group} AS d, COUNT(*) AS c \
             FROM atoms WHERE graph_id = {ph}{atom_guard} \
             GROUP BY {group} ORDER BY c DESC, d ASC LIMIT {n}"
        ),
        DimKind::EdgeColumn {
            group,
            exclude_rejected,
        } => {
            let filter = if exclude_rejected {
                " AND status <> 'rejected'"
            } else {
                ""
            };
            format!(
                "SELECT {group} AS d, COUNT(*) AS c \
                 FROM relationship_edge WHERE graph_id = {ph}{edge_guard}{filter} \
                 GROUP BY {group} ORDER BY c DESC, d ASC LIMIT {n}"
            )
        }
        DimKind::AtomTimeBucket { ts } => {
            let bucket = dialect.month_bucket(ts);
            format!(
                "SELECT {bucket} AS d, COUNT(*) AS c \
                 FROM atoms WHERE graph_id = {ph}{atom_guard} AND {ts} IS NOT NULL \
                 GROUP BY 1 ORDER BY 1 DESC LIMIT {n}"
            )
        }
        DimKind::ReefSize => format!(
            "SELECT {reef_title} AS d, COUNT(na.atom_id) AS c \
             FROM nodes n JOIN node_atoms na ON na.node_id = n.node_id \
             WHERE n.graph_id = {ph}{reef_guard} \
             GROUP BY n.node_id, {reef_title} ORDER BY c DESC, d ASC LIMIT {n}"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn postgres_queries_bind_graph_once_and_cap_rows() {
        for dim in DIMENSIONS {
            let pg = build_sql(dim, Dialect::Pg);
            assert!(pg.contains("$1"), "pg `{}` must bind $1", dim.key);
            // Exactly one graph_id bind per query (we bind once in both stores).
            assert_eq!(pg.matches("$1").count(), 1, "{} pg one bind", dim.key);
            assert!(pg.contains("LIMIT"), "{} must cap rows", dim.key);
        }
    }

    #[test]
    fn time_bucket_uses_dialect_specific_date_fn() {
        let time = DIMENSIONS.iter().find(|d| d.key == "time").unwrap();
        assert!(build_sql(time, Dialect::Pg).contains("date_trunc"));
    }

    /// Registry → migration consistency: every `index` a dimension claims must
    /// actually be created by some migration. Catches a registry entry that
    /// references a phantom index.
    #[test]
    fn every_declared_index_exists_in_migrations() {
        let sql = read_all_migrations();
        for dim in DIMENSIONS {
            if let Some(idx) = dim.index {
                assert!(
                    sql.contains(idx),
                    "dimension `{}` claims index `{idx}` but no migration creates it",
                    dim.key
                );
            }
        }
    }

    /// The "never again" guard. Every composite index `(graph_id, <col>)` on
    /// `atoms` or `relationship_edge` is a stored, query-cheap dimension; assert
    /// each is either grouped by a registry dimension or on an explicit
    /// allowlist with a reason. Adding the next 0034-style indexed column without
    /// a registry entry fails CI here.
    #[test]
    fn every_indexed_atom_or_edge_dimension_is_registered() {
        let sql = read_all_migrations();

        // Columns the registry groups by, per table.
        let mut atom_cols: HashSet<&str> = HashSet::new();
        let mut edge_cols: HashSet<&str> = HashSet::new();
        for dim in DIMENSIONS {
            match dim.kind {
                DimKind::AtomColumn { group, .. } => {
                    atom_cols.insert(group);
                }
                DimKind::AtomTimeBucket { ts } => {
                    atom_cols.insert(ts);
                }
                DimKind::EdgeColumn { group, .. } => {
                    edge_cols.insert(group);
                }
                DimKind::ReefSize => {}
            }
        }

        // Indexed dimensions that are intentionally not aggregated, with reasons.
        let atom_allow: HashSet<&str> = [
            "created_at",   // ingest-time twin of `timestamp`, covered by the time dimension
            "workspace_id", // connector-internal tenancy provenance (migration 0058) for the
            // slack-name-backfill worker's credential scoping — not a
            // user-facing aggregate breakdown ("most active workspace" isn't
            // a meaningful ask; workspace is plumbing, not content)
            "source_id", // provider-native message/doc id (migration 0074,
                         // `idx_atoms_graph_source_id_pattern`, text_pattern_ops) — added
                         // for `list_meeting_merges`/`list_meeting_new_entities`'s
                         // `source_id LIKE 'prefix%'` lookups (dogfooding gap-sweep #2,
                         // item 4, 2026-07-18). Effectively per-atom-unique (one real
                         // value per message/chunk), so "most common source_id" is not a
                         // meaningful aggregate breakdown — this is a prefix-lookup
                         // optimization, not a dimension.
        ]
        .into_iter()
        .collect();
        let edge_allow: HashSet<&str> = [
            // Migration 0341 (tenant 0174) indexes edge creation time for
            // pipeline_stage_health's graph-scoped time-window counts
            // (GRAPH_STAGES_SQL), not a new ocean_stats breakdown. The
            // existing `time` dimension buckets atoms.timestamp, not edges.
            "created_at",
        ]
        .into_iter()
        .collect();

        for (table, col) in indexed_graph_dimensions(&sql) {
            let (known, allow) = match table.as_str() {
                "atoms" => (&atom_cols, &atom_allow),
                "relationship_edge" => (&edge_cols, &edge_allow),
                _ => continue,
            };
            assert!(
                known.contains(col.as_str()) || allow.contains(col.as_str()),
                "indexed dimension {table}.{col} has no registry entry in DIMENSIONS \
                 and is not allow-listed — add a Dimension for it (or allow-list it \
                 with a reason) so aggregate questions over it are answerable",
            );
        }
    }

    fn read_all_migrations() -> String {
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/migrations_control");
        let mut all = String::new();
        for entry in std::fs::read_dir(dir).expect("migrations dir") {
            let path = entry.unwrap().path();
            if path.extension().and_then(|e| e.to_str()) == Some("sql") {
                all.push_str(&std::fs::read_to_string(&path).unwrap());
                all.push('\n');
            }
        }
        all
    }

    /// Parse `CREATE INDEX ... ON <table> (graph_id, <col> ...)` and return the
    /// `(table, col)` pairs for `atoms` / `relationship_edge`. Single-column or
    /// non-`graph_id`-prefixed indexes (e.g. the edge src/dst traversal indexes)
    /// are intentionally ignored — they are not ocean-scoped breakdowns.
    fn indexed_graph_dimensions(sql: &str) -> Vec<(String, String)> {
        let mut out = Vec::new();
        for raw in sql.split("CREATE INDEX").skip(1) {
            let stmt = match raw.split_once(';') {
                Some((s, _)) => s,
                None => raw,
            };
            let on = match stmt.find(" ON ") {
                Some(i) => &stmt[i + 4..],
                None => continue,
            };
            let open = match on.find('(') {
                Some(i) => i,
                None => continue,
            };
            let table = on[..open].trim().trim_matches('"').to_string();
            if table != "atoms" && table != "relationship_edge" {
                continue;
            }
            let close = match on.find(')') {
                Some(i) => i,
                None => continue,
            };
            let cols: Vec<&str> = on[open + 1..close].split(',').map(|c| c.trim()).collect();
            // Only ocean-scoped composite indexes lead with graph_id.
            if cols.first().map(|c| first_ident(c)) != Some("graph_id") {
                continue;
            }
            if let Some(second) = cols.get(1) {
                out.push((table, first_ident(second).to_string()));
            }
        }
        out
    }

    /// First identifier in an index-column expression, dropping `DESC`/`ASC` and
    /// any trailing modifiers (e.g. `created_at DESC` → `created_at`).
    fn first_ident(col: &str) -> &str {
        col.trim()
            .trim_matches('"')
            .split_whitespace()
            .next()
            .unwrap_or("")
    }
}
