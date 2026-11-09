//! What changed, and does it matter to this read (#1524 blocker 2).
//!
//! The busy-graph change set, as one in-process unit with a narrow interface.
//! A Library read is served from maintained facts plus an exact overlay of the
//! keys still pending in the two change logs (`library_evidence_changes` for
//! the node grain, `library_listing_changes` for the listing grain). This
//! module answers the two questions that overlay needs, inside the read's
//! own REPEATABLE READ snapshot, as SQL fragments the read embeds in its one
//! statement (and as [`evaluate`], the same fragments run standalone):
//!
//! 1. **Distinct-key extraction** ([`keys`]). The pending items and nodes,
//!    counted as distinct keys, never as raw log rows: one captured item with
//!    32 semantic atoms writes ~32 listing rows but is ONE dirty item. Loose
//!    index scans visit one index probe per distinct key. A NULL-item listing
//!    row is an unresolved parent hint: the worker resolves those from the
//!    retained OLD identities, and a read never does (as today).
//! 2. **Relevance** ([`relevance`], design §3.3 B-ii steps 2-4). For each
//!    distinct dirty item, whether its OLD image (the published head, which
//!    is exactly its currently counted contribution while it is dirty) and
//!    its NEW image (the canonical header recomputed in this snapshot) are in
//!    the read's scope, by the read's own scope predicate
//!    (`listing_facts::scope_predicate`, the one definition). An item out of
//!    scope in both images cannot affect the read: the Model excludes its
//!    stored row by that predicate and Live excludes its fresh row by the
//!    same predicate.
//!
//! Budgets are distinct-key limits ([`Budgets`]); exceeding one is an explicit
//! fallback ([`Fallback`]), the one-change-touches-thousands case being
//! `dirty_hub`. Before any object key is resolved to nodes, its physical
//! inverse fan-out is checked against [`Budgets::object_edges`]
//! (`library_changes_raw_edges_safe`, tenant 0208 / control 0393): a moved or
//! deleted object, or a fan-out past the budget, is `raw_edge_overflow`.
//!
//! The Library reader embeds [`ctes`] in its dirty statement
//! (`library_read::read_model::listing`): admission is decided on these
//! distinct keys, and only the relevant items and nodes are overlaid (design
//! steps 5-8). This module reads only.
//!
//! It depends on `library_read` (the canonical item definition and the scope
//! predicate), and the reader depends on it. It does not depend on the
//! members' access-class module, nor that on this: a reader's access class
//! decides WHOSE cached facts it may read; this decides WHICH pending keys
//! matter to the read. The change logs are graph-policed by RLS, so a reader
//! sees exactly the changes of the graphs it can see.

pub mod keys;
pub mod relevance;

use serde_json::Value;
use sqlx::PgConnection;
use uuid::Uuid;

use crate::library_read::LibraryScope;

/// Distinct-key limits for one read. Each is a count of distinct keys, not of
/// log rows, except `object_rows`, the bounded index walk that finds the
/// node log's object keys (see [`keys`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Budgets {
    /// Distinct dirty listing items (design K_ITEMS).
    pub items: usize,
    /// Distinct dirty nodes, direct and resolved from object keys (design
    /// K_NODES). Exceeding it is `dirty_hub`.
    pub nodes: usize,
    /// Distinct node-log object keys (item, atom, semantic atom) resolved to
    /// nodes, one `library_evidence_change_nodes` probe each.
    pub object_keys: usize,
    /// Raw node-log object rows the extraction may walk to find those keys.
    pub object_rows: usize,
    /// Physical inverse links (legacy, semantic and stored item links, row
    /// security off) the object keys may fan out to before any is resolved.
    pub object_edges: usize,
}

impl Default for Budgets {
    /// Design §3.3 step 8: about 512 items and 256 nodes, to be tuned from
    /// the busy-graph measurement once the reader is wired.
    fn default() -> Self {
        Self { items: 512, nodes: 256, object_keys: 512, object_rows: 4096, object_edges: 16_384 }
    }
}

impl Budgets {
    pub fn checked(self) -> Result<Self, &'static str> {
        let ok = |n: usize, max: usize| (1..=max).contains(&n);
        if ok(self.items, 4096) && ok(self.nodes, 4096) && ok(self.object_keys, 4096) && ok(self.object_rows, 65_536)
            && ok(self.object_edges, 65_536)
        {
            Ok(self)
        } else {
            Err("Library change budgets must be positive and bounded")
        }
    }
}

/// An explicit, exact fallback: the read goes Live with this reason. The
/// strings are the read's existing fallback reasons, plus `dirty_hub`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fallback {
    /// A pending listing row with no item: an unresolved parent hint.
    ItemUnresolved,
    /// More distinct dirty items than [`Budgets::items`].
    ItemOverflow,
    /// More node-log object keys (or rows to find them) than budgeted.
    ChangeOverflow,
    /// The object keys' physical inverse fan-out is over
    /// [`Budgets::object_edges`], or a key names a moved or deleted object.
    RawEdgeOverflow,
    /// More distinct dirty nodes than [`Budgets::nodes`]: one change fanned
    /// out too far to overlay exactly (a hub item, a reparent, a merge).
    DirtyHub,
}

impl Fallback {
    pub const ALL: [Fallback; 5] = [Self::ItemUnresolved, Self::ItemOverflow, Self::ChangeOverflow, Self::RawEdgeOverflow, Self::DirtyHub];
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ItemUnresolved => "dirty_item_unresolved",
            Self::ItemOverflow => "dirty_item_overflow",
            Self::ChangeOverflow => "change_overflow",
            Self::RawEdgeOverflow => "raw_edge_overflow",
            Self::DirtyHub => "dirty_hub",
        }
    }
    pub fn parse(reason: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|f| f.as_str() == reason)
    }
}

/// The placeholders the fragments reference. Static strings only: SQL never
/// comes from a request. `graphs` is a `uuid[]`, `origin` the read's origin
/// filter (`jsonb`, `'{}'` for none), `collection` its collection key (`text`,
/// NULL for none).
#[derive(Clone, Copy, Debug)]
pub struct Binds {
    pub graphs: &'static str,
    pub origin: &'static str,
    pub collection: &'static str,
}

impl Binds {
    /// The Library read statement's own binds ($1 graphs, $2 origin filter,
    /// $6 collection), for embedding in it.
    pub const READ: Binds = Binds { graphs: "$1::uuid[]", origin: "$2", collection: "$6" };
    /// [`evaluate`]'s standalone statement.
    pub const STANDALONE: Binds = Binds { graphs: "$1::uuid[]", origin: "$2", collection: "$3" };
}

/// Every change-set CTE, comma-separated and without a leading `WITH`, for
/// embedding in a statement's WITH list. Defines, among others:
/// * `change_items(graph_id,source_item_id)`: distinct dirty listing items,
///   empty unless within budget;
/// * `change_relevance(graph_id,source_item_id,old_in_scope,new_in_scope)`;
/// * `change_relevant_items(graph_id,source_item_id)`: in scope in either image;
/// * `change_nodes(graph_id,node_id)`: distinct dirty nodes, empty unless the
///   object keys are within budget;
/// * `change_decision(reason)`: NULL when every budget holds, else a
///   [`Fallback`] string.
pub fn ctes(binds: Binds, budgets: Budgets) -> String {
    format!(
        "{}, {}, {}",
        keys::ctes(binds, budgets),
        relevance::ctes(binds, budgets),
        decision_cte(budgets)
    )
}

fn decision_cte(b: Budgets) -> String {
    format!(
        r#"change_decision AS MATERIALIZED (
 SELECT CASE
  WHEN (SELECT unresolved FROM change_listing_unresolved) THEN 'dirty_item_unresolved'
  WHEN (SELECT count(*) FROM change_listing_keys)>{items} THEN 'dirty_item_overflow'
  WHEN (SELECT count(*) FROM change_object_rows)>{rows} OR (SELECT count(*) FROM change_object_keys)>{objects} THEN 'change_overflow'
  WHEN NOT (SELECT safe FROM change_object_guard) THEN 'raw_edge_overflow'
  WHEN (SELECT count(*) FROM change_direct_nodes)>{nodes} OR (SELECT count(*) FROM change_nodes)>{nodes} THEN 'dirty_hub' END reason
)"#,
        items = b.items,
        rows = b.object_rows,
        objects = b.object_keys,
        nodes = b.nodes
    )
}

/// One dirty item's classification against one read's scope.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ItemRelevance {
    pub graph_id: Uuid,
    pub source_item_id: Uuid,
    /// The published head (the counted contribution) is in scope.
    pub old_in_scope: bool,
    /// The canonical header recomputed in this snapshot is in scope.
    pub new_in_scope: bool,
}

impl ItemRelevance {
    /// In scope in either image: the read must recompute it. Out of scope in
    /// both, it is left out of the overlay with no effect on the result.
    pub fn relevant(&self) -> bool {
        self.old_in_scope || self.new_in_scope
    }
}

/// The change set one read sees, as [`evaluate`] returns it. The counts are
/// the debug counts the relevance proofs assert on (capped lower bounds once
/// a budget is exceeded).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChangeSet {
    pub fallback: Option<Fallback>,
    /// Distinct dirty listing items found (at most `items + 1`).
    pub distinct_items: usize,
    /// Every classified item; empty when the item budget is exceeded.
    pub items: Vec<ItemRelevance>,
    /// Distinct dirty nodes; empty when the object keys exceed their budget.
    pub nodes: Vec<(Uuid, Uuid)>,
    pub object_keys: usize,
    pub object_rows: usize,
}

impl ChangeSet {
    pub fn relevant_items(&self) -> impl Iterator<Item = &ItemRelevance> {
        self.items.iter().filter(|i| i.relevant())
    }
    pub fn irrelevant_items(&self) -> impl Iterator<Item = &ItemRelevance> {
        self.items.iter().filter(|i| !i.relevant())
    }
}

/// The standalone change-set statement, with [`Binds::STANDALONE`].
pub fn evaluate_sql(budgets: Budgets) -> String {
    format!(
        r#"WITH {} SELECT (SELECT reason FROM change_decision) reason,
 (SELECT count(*) FROM change_listing_keys)::bigint distinct_items,
 (SELECT count(*) FROM change_object_keys)::bigint object_keys,
 (SELECT count(*) FROM change_object_rows)::bigint object_rows,
 COALESCE((SELECT jsonb_agg(jsonb_build_array(r.graph_id,r.source_item_id,r.old_in_scope,r.new_in_scope) ORDER BY r.graph_id,r.source_item_id) FROM change_relevance r),'[]'::jsonb) items,
 COALESCE((SELECT jsonb_agg(jsonb_build_array(n.graph_id,n.node_id) ORDER BY n.graph_id,n.node_id) FROM change_nodes n),'[]'::jsonb) nodes"#,
        ctes(Binds::STANDALONE, budgets)
    )
}

/// The change set for one Library read scope, in the caller's transaction.
/// Run it inside the read's own REPEATABLE READ snapshot, under the
/// reader's own role and settings: both logs and every input are RLS
/// policed, so the result is the reader's view.
pub async fn evaluate(conn: &mut PgConnection, scope: &LibraryScope, budgets: Budgets) -> Result<ChangeSet, sqlx::Error> {
    let budgets = budgets.checked().map_err(|e| sqlx::Error::Protocol(e.into()))?;
    let (reason, distinct_items, object_keys, object_rows, items, nodes): (Option<String>, i64, i64, i64, Value, Value) =
        sqlx::query_as(&evaluate_sql(budgets))
            .bind(&scope.graph_ids)
            .bind(scope.origin_filter())
            .bind(scope.collection_key())
            .fetch_one(conn)
            .await?;
    let fallback = match reason {
        None => None,
        Some(r) => Some(Fallback::parse(&r).ok_or_else(|| sqlx::Error::Protocol(format!("unknown change-set reason {r}")))?),
    };
    let decode = |e: serde_json::Error| sqlx::Error::Decode(Box::new(e));
    let items: Vec<(Uuid, Uuid, bool, bool)> = serde_json::from_value(items).map_err(decode)?;
    let nodes: Vec<(Uuid, Uuid)> = serde_json::from_value(nodes).map_err(decode)?;
    Ok(ChangeSet {
        fallback,
        distinct_items: distinct_items as usize,
        items: items
            .into_iter()
            .map(|(graph_id, source_item_id, old_in_scope, new_in_scope)| ItemRelevance { graph_id, source_item_id, old_in_scope, new_in_scope })
            .collect(),
        nodes,
        object_keys: object_keys as usize,
        object_rows: object_rows as usize,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fallback_reasons_round_trip_and_reuse_the_read_vocabulary() {
        for f in Fallback::ALL {
            assert_eq!(Fallback::parse(f.as_str()), Some(f));
        }
        assert_eq!(Fallback::DirtyHub.as_str(), "dirty_hub");
        assert_eq!(Fallback::parse("context_mismatch"), None);
    }

    #[test]
    fn budgets_are_bounded_distinct_key_limits() {
        assert!(Budgets::default().checked().is_ok());
        assert!(Budgets { items: 0, ..Budgets::default() }.checked().is_err());
        assert!(Budgets { nodes: 5000, ..Budgets::default() }.checked().is_err());
        assert!(Budgets { object_edges: 0, ..Budgets::default() }.checked().is_err());
    }

    #[test]
    fn the_fragments_use_only_the_given_binds_and_the_read_predicate() {
        let read = ctes(Binds::READ, Budgets::default());
        assert!(read.contains("unnest($1::uuid[])"));
        assert!(read.contains("CASE $6") && !read.contains("$3"));
        // The relevance predicate is the read's own, rendered twice: over the
        // stored head and over the fresh header.
        let old = crate::library_read::listing_facts::scope_predicate("h", "$2", "$6");
        let new = crate::library_read::listing_facts::scope_predicate("f", "$2", "$6");
        assert!(read.contains(&old) && read.contains(&new));
        let standalone = evaluate_sql(Budgets::default());
        assert!(standalone.contains("CASE $3") && !standalone.contains("$6"));
        // Budgets render as distinct-key limits (and one +1 lookahead each).
        let small = ctes(Binds::READ, Budgets { items: 7, nodes: 9, object_keys: 11, object_rows: 13, object_edges: 17 });
        for needle in ["LIMIT 8", "LIMIT 10", "LIMIT 12", "LIMIT 14", ">7 THEN 'dirty_item_overflow'", ">9 THEN 'dirty_hub'", "ARRAY[]::uuid[],17)", "THEN 'raw_edge_overflow'"] {
            assert!(small.contains(needle), "{needle}");
        }
    }
}
