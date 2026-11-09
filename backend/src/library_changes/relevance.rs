//! The relevance classifier (design §3.3 B-ii, steps 2-4).
//!
//! Step 2, old image: each distinct dirty item's published head
//! (`library_listing_source_heads`, a primary-key probe). While an item is
//! dirty its head is exactly the contribution the maintained facts count
//! (tenant 0199, "This header is also the old counted contribution"), so the
//! old image needs no capture of its own.
//!
//! Step 3, new image: the HEADER mode of the canonical item definition
//! (`listing_facts::canonical_ctes_in(Changed, Header)`), recomputed in the
//! read's snapshot: a few primary-key and index probes per item, no outcome
//! rows. A non-permitted item has no fresh header, so it is out of scope, as
//! it is for Live.
//!
//! Step 4: both images are tested with the read's OWN scope predicate
//! (`listing_facts::scope_predicate`, the one definition the Model's
//! `scope_sources` also renders). An item out of scope in both images is
//! irrelevant: the Model never reads its stored row (the predicate excludes
//! it), and Live never reads its fresh row (the same predicate excludes it),
//! so leaving it out of the overlay leaves the result unchanged. A missing
//! head or fresh header is "out of scope" for that image; a NULL predicate
//! is "out of scope", exactly as it is in the read's WHERE clause.
use super::{Binds, Budgets};
use crate::library_read::listing_facts::{canonical_ctes_in, scope_predicate, ItemInput, Mode};

pub(super) fn ctes(b: Binds, budgets: Budgets) -> String {
    let fresh = canonical_ctes_in(ItemInput::Changed, Mode::Header);
    let old = scope_predicate("h", b.origin, b.collection);
    let new = scope_predicate("f", b.origin, b.collection);
    format!(
        r#"change_items AS MATERIALIZED (
 SELECT k.graph_id,k.source_item_id FROM change_listing_keys k
 WHERE (SELECT count(*) FROM change_listing_keys)<={items} AND NOT (SELECT unresolved FROM change_listing_unresolved)
), change_fresh_headers AS MATERIALIZED (
 {fresh} SELECT s.graph_id,s.source_item_id,s.source_type,s.origin FROM listing_sources s
), change_relevance AS MATERIALIZED (
 SELECT i.graph_id,i.source_item_id,
  COALESCE(h.source_item_id IS NOT NULL AND ({old}),false) old_in_scope,
  COALESCE(f.source_item_id IS NOT NULL AND ({new}),false) new_in_scope
 FROM change_items i
 LEFT JOIN library_listing_source_heads h ON h.graph_id=i.graph_id AND h.source_item_id=i.source_item_id
 LEFT JOIN change_fresh_headers f ON f.graph_id=i.graph_id AND f.source_item_id=i.source_item_id
), change_relevant_items AS MATERIALIZED (
 SELECT r.graph_id,r.source_item_id FROM change_relevance r WHERE r.old_in_scope OR r.new_in_scope
)"#,
        items = budgets.items,
    )
}
