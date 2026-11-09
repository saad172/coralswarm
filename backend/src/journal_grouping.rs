//! The journal's atom grouping, as SQL text.
//!
//! The ONE definition behind `Store::journal_range_aggregate` (the Today hour
//! rows) and `services::journal_activities` (the Today activity cards), so the
//! two can never group the same atoms differently. Pure string builders: this
//! module has no pool and runs nothing; each caller embeds the text in its own
//! statement, inside its own member-scoped transaction. The Rust twin of the
//! key is `workers::journal_sweep::journal_group_key`.
//!
//! Both fragments assume the atom is aliased `a` in the caller's `FROM atoms a`.

use crate::ingest::atom::SESSION_SOURCE_TYPES_SQL_ARRAY;

/// SQL expression: the grouping key of atom `a`, using the session joined by
/// [`session_join_sql`] (alias `cs`).
///
/// A session atom groups by its resolved `coding_sessions.session_id` (its own
/// `source_id` when the session upsert never landed, rather than guessing a
/// boundary with nothing to check it against); every other source type by its
/// `source_id` with a trailing `:chunk:N` (the `meeting_transcripts` chunking
/// convention) removed. The session types come from `SessionKind::ALL`, so the
/// list cannot drift from `is_session_source_type`.
pub(crate) fn grouping_key_sql() -> String {
    format!(
        "CASE WHEN a.source_type = ANY({sess}) THEN COALESCE(cs.session_id, a.source_id) \
         ELSE regexp_replace(a.source_id, ':chunk:[0-9]+$', '') END",
        sess = *SESSION_SOURCE_TYPES_SQL_ARRAY
    )
}

/// SQL: `LEFT JOIN LATERAL ... cs ON true`, resolving the `coding_sessions`
/// row of the session atom `a`. Columns: `cs.session_id`, `cs.title`.
///
/// A session atom's `source_id` is `"cc:{session_id}:{checkpoint_id}[:chunk:N]"`
/// and neither id has a charset restriction beyond a length cap, so either may
/// contain colons: taking only the first piece would truncate
/// `"cc:team:alpha:c1"` to `"team"`. Enumerate its colon-delimited prefixes and
/// probe real session ids by equality instead. This preserves literal `%`, `_`
/// and backslashes while using the `(graph_id,session_id)` index, rather than
/// scanning every session once per atom. `UNIQUE (graph_id, session_id)` does not
/// stop one id being a prefix of another ("team" and "team:alpha" can
/// coexist), so more than one row can match; `ORDER BY length(session_id) DESC
/// LIMIT 1` picks the longest, which is always the real session boundary, and
/// the join can never fan an atom out (and double-count it).
pub(crate) fn session_join_sql() -> String {
    format!(
        r#"LEFT JOIN LATERAL (
                    SELECT cs_inner.session_id, cs_inner.title
                    FROM coding_sessions cs_inner
                    WHERE a.source_type = ANY({sess})
                      AND cs_inner.graph_id = a.graph_id
                      AND left(a.source_id,3) = 'cc:'
                      AND cs_inner.session_id = ANY(ARRAY(
                          SELECT array_to_string(parts[1:i], ':')
                          FROM (SELECT string_to_array(substr(a.source_id,4), ':') parts) p
                          CROSS JOIN LATERAL generate_series(1,cardinality(parts)-1) q(i)
                      ))
                    ORDER BY length(cs_inner.session_id) DESC
                    LIMIT 1
                ) cs ON true"#,
        sess = *SESSION_SOURCE_TYPES_SQL_ARRAY
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fragments_name_every_session_source_type_and_escape_like_metacharacters() {
        let key = grouping_key_sql();
        let join = session_join_sql();
        for t in ["coding_session", "meeting_session", "agent_session"] {
            assert!(
                key.contains(&format!("'{t}'")) && join.contains(&format!("'{t}'")),
                "{t}"
            );
        }
        assert!(key.contains("regexp_replace(a.source_id, ':chunk:[0-9]+$', '')"));
        assert!(join.contains("ORDER BY length(cs_inner.session_id) DESC"));
        assert!(join.contains("cs_inner.session_id = ANY"), "prefixes use exact indexed ids");
        assert!(join.contains("left(a.source_id,3) = 'cc:'"));
    }
}
