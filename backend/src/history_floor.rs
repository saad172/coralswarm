//! Team-seat history floors in application SQL (#1525).
//!
//! A member seated with `team_seat_grants.history_from` may read a team
//! ocean's legacy atoms and semantic atoms only from that instant on
//! (migration 0064). Production requests connect as the table owner, which
//! bypasses RLS, so the floor is applied by the read boundaries themselves,
//! through one predicate that tenant 0204 / control 0388 define:
//!
//! - `reader_history_floors()`: the reader's floors as a jsonb object of graph
//!   id to floor, or NULL. It delegates to `tenant_rls_visible_graph_windows()`
//!   on the tenant plane and is NULL on control.
//! - `history_floor_admits(graph, moment, floors)`: whether the floor admits a
//!   row of `graph` whose source-event time is `moment`.
//!
//! The SQL boundaries (`library_permitted_atoms`, `served_semantic_atoms`)
//! carry the predicate themselves. Rust SQL that reads `semantic_atoms`
//! directly uses [`admits`] with [`semantic_moment`], so every caller spells
//! it the same way.
//!
//! # The one boundary (#1545)
//!
//! Every member read of floored content goes through this module. There is
//! one predicate builder per kind of content, each naming the moment 0064's
//! floor is compared with:
//!
//! | Content | Builder | Moment |
//! |---|---|---|
//! | legacy atom (`atoms`) | [`admits_atom`] | `"timestamp"` |
//! | semantic atom / source version text | [`admits_version`] | the version's `COALESCE(occurred_at, captured_at)` |
//! | source item (title, metadata, inventory) | [`admits_source_item`], or [`admits_source_item_at`] when the shown version is joined | its current (or shown) version's moment, else `created_at` |
//! | meeting transcript (raw text, listing, metadata) | [`admits_transcript`] | the earliest of its version's moment, `created_at` and its chunk atoms' timestamps |
//! | precomputed per-period rows (journal rollups, Today gists) | [`admits_period`] | the period's start |
//!
//! Each builder is an SQL boolean over the caller's own row alias, applied in
//! the read's own statement: the floor is evaluated inside the read's
//! transaction and snapshot, never in a second query. The floors come from
//! `app.user_id`, so they bind only in a member-scoped transaction
//! (`Tenant::begin_member_scoped`); in system scope `reader_history_floors()`
//! is NULL and every builder admits, which is why workers that compute caches
//! are unchanged.
//!
//! `scripts/check_history_floor_boundary.sh` is the guard: it fails when a new
//! SQL literal reads a floored table without one of these builders (or a SQL
//! boundary that applies them), against a per-file ratchet.

/// SQL boolean: the calling reader's history floor admits a row of graph
/// `graph` whose source-event time is `moment` (both SQL expressions).
/// The floors are one uncorrelated InitPlan per statement.
pub fn admits(graph: &str, moment: &str) -> String {
    format!("public.history_floor_admits({graph},{moment},{FLOORS})")
}

/// The reader's floors, one uncorrelated InitPlan per statement.
const FLOORS: &str = "(SELECT public.reader_history_floors())";

/// [`admits`] for the legacy atom alias `atom` (`atoms`): its `"timestamp"`,
/// as 0064's policy and 0204's `library_permitted_atoms` compare it.
pub fn admits_atom(atom: &str) -> String {
    admits(&format!("{atom}.graph_id"), &format!("{atom}.\"timestamp\""))
}

/// The source item alias `item` (`source_items`): its current version's
/// moment, or its `created_at` when it has none (tenant 0205). For reads that
/// do not join the version.
pub fn admits_source_item(item: &str) -> String {
    format!("public.history_floor_admits_source_item({item}.graph_id,{item}.current_version_id,{item}.created_at,{FLOORS})")
}

/// The source item alias `item` shown at the joined version alias `version`
/// (current or selected; NULL when the item has none): that version's moment,
/// else the item's `created_at`. The inline form of [`admits_source_item`] for
/// set reads that already join the version, so a whole-ocean scope pays no
/// per-item lookup.
pub fn admits_source_item_at(item: &str, version: &str) -> String {
    admits(
        &format!("{item}.graph_id"),
        &format!("COALESCE({version}.occurred_at,{version}.captured_at,{item}.created_at)"),
    )
}

/// The meeting transcript alias `transcript` (`meeting_transcripts`): the
/// earliest of its bound version's moment, its `created_at` and its chunk
/// atoms' timestamps (tenant 0205). Its raw text holds every one of those
/// atoms, so it is shown only when each of them would be.
pub fn admits_transcript(transcript: &str) -> String {
    format!(
        "public.history_floor_admits_transcript({t}.graph_id,{t}.source_id,{t}.current_version_id,{t}.created_at,{FLOORS})",
        t = transcript
    )
}

/// A precomputed row of graph `graph` summarizing a period that starts at
/// `period_start` (journal rollups). A period that begins before the floor
/// may summarize pre-floor content, so it is hidden whole (fail closed). The
/// journal's live aggregate, floored per atom by [`admits_atom`], still shows
/// the post-floor atoms of a straddling hour.
pub fn admits_period(graph: &str, period_start: &str) -> String {
    admits(graph, period_start)
}

/// A semantic atom's source-event time: its source version's
/// `COALESCE(occurred_at, captured_at)`, for the version alias `version`.
pub fn semantic_moment(version: &str) -> String {
    format!("COALESCE({version}.occurred_at,{version}.captured_at)")
}

/// [`admits`] for the semantic atoms of source version alias `version`.
pub fn admits_version(version: &str) -> String {
    admits(&format!("{version}.graph_id"), &semantic_moment(version))
}

/// SQL boolean: the reader holds a history floor on any graph in the uuid[]
/// expression `graph_ids`. The accelerated Library reader refuses such
/// readers, who read the floor-filtered Live path instead.
pub fn floored_on_any(graph_ids: &str) -> String {
    format!("COALESCE((SELECT public.reader_history_floors()) ?| ARRAY(SELECT x::text FROM unnest({graph_ids}) x),false)")
}

#[cfg(test)]
mod tests {
    #[test]
    fn the_predicate_text_matches_the_migration_boundaries() {
        // The migrations spell the same call by hand; keep them in step.
        let sql = include_str!("../migrations_tenant/0204_history_floor_read_boundary.sql");
        assert!(sql.contains("public.history_floor_admits(a.graph_id,a.\"timestamp\",(SELECT public.reader_history_floors()))"));
        assert_eq!(super::admits("a.graph_id", "a.\"timestamp\""), "public.history_floor_admits(a.graph_id,a.\"timestamp\",(SELECT public.reader_history_floors()))");
        assert_eq!(super::admits_version("v"), "public.history_floor_admits(v.graph_id,COALESCE(v.occurred_at,v.captured_at),(SELECT public.reader_history_floors()))");
        assert_eq!(super::admits_atom("a"), super::admits("a.graph_id", "a.\"timestamp\""));
        let surfaces = include_str!("../migrations_tenant/0205_history_floor_source_surfaces.sql");
        assert!(surfaces.contains("FUNCTION public.history_floor_admits_source_item(g UUID, current_version UUID, created TIMESTAMPTZ, floors JSONB)"));
        assert!(surfaces.contains("FUNCTION public.history_floor_admits_transcript(g UUID, meeting TEXT, current_version UUID, created TIMESTAMPTZ, floors JSONB)"));
        assert_eq!(
            surfaces.lines().skip(1).collect::<Vec<_>>(),
            include_str!("../migrations_control/0390_history_floor_source_surfaces.sql").lines().skip(1).collect::<Vec<_>>(),
            "the paired 0205/0390 migrations must stay byte-identical below the header"
        );
        assert_eq!(
            include_str!("../migrations_tenant/0204_history_floor_read_boundary.sql").lines().skip(1).collect::<Vec<_>>(),
            include_str!("../migrations_control/0388_history_floor_read_boundary.sql").lines().skip(1).collect::<Vec<_>>(),
            "the paired migrations must stay byte-identical below the header"
        );
    }

    #[test]
    fn library_scope_reads_apply_the_floor_in_both_outcome_modes() {
        use crate::library_read::{LibraryScope, OutcomeVersions};
        for current in [false, true] {
            let mut scope = LibraryScope::exact_node(uuid::Uuid::nil(), uuid::Uuid::nil());
            scope.node_id = None;
            scope.outcome_versions = Some(if current { OutcomeVersions::Current } else { OutcomeVersions::AllRetained });
            let cte = crate::library_read::scope_cte(&scope);
            assert!(cte.contains(&super::admits_version("v")), "current={current}");
            assert!(cte.contains(&super::admits_version("mapped")));
        }
        assert!(crate::library_read::read_model::definition_sql(&LibraryScope::exact_node(uuid::Uuid::nil(), uuid::Uuid::nil()), "")
            .contains(&super::floored_on_any("$1::uuid[]")));
    }
}
