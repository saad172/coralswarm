//! Definition-3 listing facts. The listing worker owns every write, publication
//! and exact-ID acknowledgement; callers do not run a lifecycle. Node evidence
//! keeps its own `evidence::READ_MODEL_VERSION`; the reader requires both
//! grains certified, each at its own version.
//!
//! Staging has separate tables. The serving order contains one active row per
//! (graph,outcome), never inactive generations. During bounded reconciliation the
//! entire item remains guarded/dirty; its published header is the OLD counted
//! contribution. A final publication is metadata/deltas/selected-ID ACK only.
//! New events require replacement/restart, even if a generation already matches.

use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::Value;
use uuid::Uuid;

/// A schema contract reservation, NOT `evidence::READ_MODEL_VERSION`.
pub(crate) const RESERVED_DEFINITION_VERSION: i32 = 3;

#[derive(Clone, Debug, PartialEq, Serialize, sqlx::FromRow)]
pub(crate) struct SourceHeader {
    pub graph_id: Uuid,
    pub source_item_id: Uuid,
    pub source_type: String,
    pub provider_namespace: String,
    pub source_id: String,
    pub origin: Option<Value>,
    pub discovery_content_kind: Option<String>,
    pub current_version_id: Option<Uuid>,
    pub selected_version_id: Option<Uuid>,
    pub served_version_id: Option<Uuid>,
    pub captured_at: Option<DateTime<Utc>>,
    pub title256: Option<String>,
    pub unknown_independence: bool,
    pub raw_extraction_status: Option<String>,
    pub raw_next_start_char: Option<i64>,
    pub raw_extraction_reason: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, sqlx::FromRow)]
pub(crate) struct SourceFacts {
    #[serde(flatten)]
    #[sqlx(flatten)]
    pub header: SourceHeader,
    pub retained_count: i64,
    pub served_count: i64,
}

#[derive(Clone, Debug, PartialEq, Serialize, sqlx::FromRow)]
pub(crate) struct OutcomeFacts {
    pub graph_id: Uuid,
    pub semantic_atom_id: Uuid,
    pub source_version_id: Uuid,
    pub source_item_id: Uuid,
    pub occurred_at: Option<DateTime<Utc>>,
    pub captured_at: DateTime<Utc>,
    pub source_version_is_current: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, sqlx::FromRow)]
pub(crate) struct FacetContribution {
    pub graph_id: Uuid,
    pub source_item_id: Uuid,
    pub dimension: String,
    pub key: Value,
    pub label: Option<String>,
}

/// The only selectable input relations. SQL cannot come from a request string.
#[derive(Clone, Copy, Debug)]
pub(crate) enum ItemInput {
    /// $1 graph UUID, $2 item UUID array, supplied by the scoped worker.
    Worker,
    /// The already bounded dirty-item relation in the same Model statement.
    Dirty,
    /// The bounded distinct dirty items of a change set
    /// (`crate::library_changes`), in the same statement.
    Changed,
    /// The relevant dirty items whose fresh outcome rows the Model read
    /// materializes (`model_listing_row_items`), in the same statement.
    PageRows,
}

/// How much of the one canonical item definition a consumer renders. Every
/// mode is a prefix-compatible subset of the same CTE text, never a second
/// definition: `Header` is the metadata (`listing_sources`,
/// `listing_source_headers`), `Counts` adds the exact per-item counts
/// (`listing_source_facts`, over the unmaterialized `listing_outcomes`), and
/// `Rows` is the complete reconstruction including facets. `canonical_ctes`
/// is `Rows`, byte for byte as before the split.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Mode {
    Header,
    /// The Model's contribution swap (design §3.3 step 5) and its fresh
    /// outcome rows (`listing_outcomes`); its text is pinned below.
    Counts,
    Rows,
}

/// The listing read's own source-scope predicate over one header-shaped row
/// alias (`origin`, `source_type`): the origin filter and the collection.
/// It is the ONE definition. The Model's `scope_sources` renders it with the
/// read's binds (`$2`, `$6`); the change-set relevance classifier
/// (`crate::library_changes`) renders it over the stored head (the old image)
/// and over the fresh header (the new image). A NULL result is "not in
/// scope", exactly as in a WHERE clause.
pub(crate) fn scope_predicate(alias: &str, origin: &str, collection: &str) -> String {
    format!(r#"({origin}::jsonb='{{}}'::jsonb OR {alias}.origin @> {origin} OR ({alias}.origin IS NULL AND {origin}::jsonb=jsonb_build_object('content_kind',{alias}.source_type)))
 AND ({collection}::text IS NULL OR CASE {collection}
  WHEN 'agent_sessions' THEN {alias}.source_type IN('coding_session','agent_session') OR {alias}.origin->>'content_kind' IN('coding_session','agent_session')
  WHEN 'work_sessions' THEN {alias}.source_type='work_session' OR {alias}.origin->>'content_kind'='work_session'
  WHEN 'meetings' THEN {alias}.source_type IN('transcript','meeting_session','teams_transcript','zoom_transcript','google_meet_transcript','slack_huddle_summary') OR {alias}.origin->>'content_kind' IN('transcript','meeting','meeting_session') ELSE false END)"#)
}

/// One canonical reconstruction for both maintenance and dirty replacement.
/// Consumers choose metadata-only `listing_source_headers`, complete
/// `listing_source_facts`, `listing_outcomes` or `listing_facets`. A large-item
/// worker accumulates exact counts from bounded staged outcome batches; it must
/// not invoke the complete count consumer to bypass its input-work budget.
/// Jobs-derived public status and statement/passage matching
/// stay authoritative under the reader's RLS, not persisted here.
///
/// This is keyed by ITEM, never by node links: unlinked outcomes count, and
/// shared outcomes occur once. Check raw input budgets before invoking it on a
/// request. Complete counts are not counts of the cursor-limited payload page.
pub(crate) fn canonical_ctes(input: ItemInput) -> String {
    canonical_ctes_in(input, Mode::Rows)
}

/// [`canonical_ctes`] rendered in one [`Mode`]. The CTE order is fixed, so a
/// lighter mode is the full text with its unused tail CTEs left out.
pub(crate) fn canonical_ctes_in(input: ItemInput, mode: Mode) -> String {
    let keys = match input {
        ItemInput::Worker => "SELECT DISTINCT $1::uuid graph_id,unnest($2::uuid[]) source_item_id",
        ItemInput::Dirty => "SELECT DISTINCT graph_id,source_item_id FROM model_dirty_items",
        ItemInput::Changed => "SELECT DISTINCT graph_id,source_item_id FROM change_items",
        ItemInput::PageRows => "SELECT DISTINCT graph_id,source_item_id FROM model_listing_row_items",
    };
    let served =
        super::SERVED_VERSIONS_SQL.replace("FROM scope_sources s", "FROM listing_sources s");
    let outcomes = if mode == Mode::Header { "" } else { LISTING_OUTCOMES_CTE };
    let facts = if mode == Mode::Header { "" } else { LISTING_SOURCE_FACTS_CTE };
    let facets = if mode == Mode::Rows { LISTING_FACETS_CTE } else { "" };
    format!(
        r#"WITH listing_keys AS MATERIALIZED ({keys}),
listing_sources AS MATERIALIZED (
 SELECT s.graph_id,s.source_item_id,s.source_type,s.provider_namespace,s.source_id,s.origin,
  CASE WHEN s.origin IS NULL THEN s.source_type ELSE s.origin->>'content_kind' END discovery_content_kind,
  s.current_version_id,v.version_id selected_version_id,v.captured_at,
  substring(NULLIF(v.source_metadata->>'title','') FOR 256) title256,
  COALESCE(v.is_legacy_baseline,false) OR COALESCE(v.source_metadata->>'independence'='derived_copy',false) unknown_independence,
  v.extraction_status raw_extraction_status,v.next_start_char raw_next_start_char,
  CASE WHEN v.extraction_status='blocked' AND v.extraction_error IN
   ('managed_admission_required','capture_actor_unknown_managed_admission_required','managed_run_paused','execution_budget_exhausted','cloud_processing_disabled','requester_access_lost','source_removed_or_access_lost')
   THEN v.extraction_error END raw_extraction_reason
 FROM listing_keys k CROSS JOIN LATERAL (
  SELECT s.graph_id,s.source_item_id,s.source_type,s.provider_namespace,s.source_id,s.origin,s.current_version_id FROM source_items s WHERE s.graph_id=k.graph_id AND s.source_item_id=k.source_item_id
   AND s.access_state='permitted' OFFSET 0) s
 LEFT JOIN LATERAL (SELECT v.version_id,v.captured_at,v.source_metadata,v.is_legacy_baseline,v.extraction_status,v.next_start_char,v.extraction_error FROM source_versions v WHERE v.graph_id=s.graph_id
  AND v.source_item_id=s.source_item_id AND v.version_id=s.current_version_id OFFSET 0) v ON true
), listing_served AS MATERIALIZED ({served}){outcomes}, listing_source_headers AS MATERIALIZED (
 SELECT s.*,COALESCE(p.version_id,s.current_version_id) served_version_id
 FROM listing_sources s LEFT JOIN listing_served p ON p.graph_id=s.graph_id AND p.source_item_id=s.source_item_id
){facts}{facets}"#
    )
}

const LISTING_OUTCOMES_CTE: &str = r#",
listing_outcomes AS NOT MATERIALIZED (
 SELECT a.graph_id,a.semantic_atom_id,a.source_version_id,v.source_item_id,v.occurred_at,v.captured_at,
  COALESCE(a.source_version_id=COALESCE(p.version_id,s.current_version_id),false) source_version_is_current
 FROM listing_sources s CROSS JOIN LATERAL (SELECT v.graph_id,v.version_id,v.source_item_id,v.occurred_at,v.captured_at FROM source_versions v
  WHERE v.graph_id=s.graph_id AND v.source_item_id=s.source_item_id OFFSET 0) v
 LEFT JOIN listing_served p ON p.graph_id=s.graph_id AND p.source_item_id=s.source_item_id
 CROSS JOIN LATERAL (SELECT a.graph_id,a.semantic_atom_id,a.source_version_id FROM semantic_atoms a
  WHERE a.graph_id=v.graph_id AND a.source_version_id=v.version_id OFFSET 0) a
)"#;
const LISTING_SOURCE_FACTS_CTE: &str = r#", listing_source_facts AS MATERIALIZED (
 SELECT h.*,COALESCE(c.retained_count,0)::bigint retained_count,COALESCE(c.served_count,0)::bigint served_count
 FROM listing_source_headers h LEFT JOIN (SELECT graph_id,source_item_id,count(*) retained_count,
  count(*) FILTER(WHERE source_version_is_current) served_count FROM listing_outcomes GROUP BY 1,2) c
 ON c.graph_id=h.graph_id AND c.source_item_id=h.source_item_id
)"#;
const LISTING_FACETS_CTE: &str = r#", listing_facets AS (
 SELECT graph_id,source_item_id,'service'::text dimension,to_jsonb(origin->>'service_id') key,NULL::text label
 FROM listing_source_headers WHERE origin->>'service_id' IS NOT NULL
 UNION ALL
 SELECT graph_id,source_item_id,'content_kind',to_jsonb(discovery_content_kind),NULL::text
 FROM listing_source_headers WHERE discovery_content_kind IS NOT NULL
 UNION ALL
 SELECT graph_id,source_item_id,'connection',jsonb_build_object('service_id',origin->'service_id','connection',origin->'connection'),NULL::text
 FROM listing_source_headers WHERE jsonb_typeof(origin->'connection')='object'
 UNION ALL
 SELECT graph_id,source_item_id,'container',jsonb_build_object('service_id',origin->'service_id','container',(origin->'container')-'label'),origin->'container'->>'label'
 FROM listing_source_headers WHERE jsonb_typeof(origin->'container')='object'
)"#;

/// Exact captured set, not a numeric watermark. A smaller ID can commit later.
/// No maximum-ID acknowledgement operation is provided.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SelectedChangeIds(Vec<i64>);
impl SelectedChangeIds {
    pub fn new(mut ids: Vec<i64>) -> Result<Self, &'static str> {
        if ids.is_empty() || ids.iter().any(|id| *id <= 0) {
            return Err("a generation needs a positive durable guard ID");
        }
        ids.sort_unstable();
        ids.dedup();
        Ok(Self(ids))
    }
    pub fn as_slice(&self) -> &[i64] {
        &self.0
    }
    /// A later visible event always requires replacement/restart, not ACK.
    pub fn contains_unincorporated(&self, visible: &[i64]) -> bool {
        visible.iter().any(|id| self.0.binary_search(id).is_err())
    }
}

/// Internal scan unit: input keys visited, not rows changed or outputs emitted.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct ChunkLimit(u32);
impl ChunkLimit {
    pub fn new(rows: u32) -> Result<Self, &'static str> {
        if !(1..=2000).contains(&rows) {
            return Err("listing chunk rows must be in 1..=2000");
        }
        Ok(Self(rows))
    }
    pub fn rows(self) -> i64 {
        i64::from(self.0)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum ChunkResult {
    InProgress {
        scanned: u32,
        changed: u32,
        after: Uuid,
    },
    Exhausted {
        scanned: u32,
        changed: u32,
    },
}

/// Internal result of the deep worker. Matching a token alone is never success.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum GenerationResult {
    Building(ChunkResult),
    Reconciling(ChunkResult),
    ReadyToPublish,
    Published {
        generation: Uuid,
        acknowledged: SelectedChangeIds,
    },
    RestartRequired,
}

/// Bind graph,item,generation,after-key,validated ChunkLimit. Read/lookahead is
/// bounded before writes. The active trigger requires a durable dirty guard for
/// BOTH owners on a move. Never run this before input/restart checks and lock.
pub(crate) const RECONCILE_UPSERT_SQL: &str = r#"WITH probe AS MATERIALIZED (
 SELECT s.* FROM library_listing_staged_outcomes s WHERE s.graph_id=$1 AND s.source_item_id=$2 AND s.generation=$3
 AND ($4::uuid IS NULL OR s.semantic_atom_id>$4) ORDER BY s.semantic_atom_id LIMIT $5::bigint+1
), page AS MATERIALIZED (SELECT * FROM probe ORDER BY semantic_atom_id LIMIT $5), changed AS (
 INSERT INTO library_listing_outcomes(graph_id,semantic_atom_id,source_item_id,source_version_id,generation,occurred_at,captured_at,source_version_is_current)
 SELECT graph_id,semantic_atom_id,source_item_id,source_version_id,generation,occurred_at,captured_at,COALESCE(source_version_id=(SELECT served_version_id FROM library_listing_staged_sources WHERE graph_id=$1 AND source_item_id=$2 AND generation=$3),false) FROM page
 ON CONFLICT(graph_id,semantic_atom_id) DO UPDATE SET source_item_id=EXCLUDED.source_item_id,
 source_version_id=EXCLUDED.source_version_id,generation=EXCLUDED.generation,occurred_at=EXCLUDED.occurred_at,
 captured_at=EXCLUDED.captured_at,source_version_is_current=EXCLUDED.source_version_is_current RETURNING 1
) SELECT (SELECT count(*) FROM page) scanned,(SELECT count(*) FROM changed) changed,
 (SELECT semantic_atom_id FROM page ORDER BY semantic_atom_id DESC LIMIT 1) after_key,
 (SELECT count(*) FROM probe)>$5 more"#;

/// Prune the current owner+observed generation only. A moved destination row
/// cannot be deleted by stale source-item cleanup. Final publication never does
/// an all-item copy, UPDATE/tag flip or DELETE; exhausted cursors prove completion.
pub(crate) const RECONCILE_PRUNE_SQL: &str = r#"WITH probe AS MATERIALIZED (
 SELECT a.graph_id,a.source_item_id,a.semantic_atom_id,a.generation FROM library_listing_outcomes a
 WHERE a.graph_id=$1 AND a.source_item_id=$2 AND ($4::uuid IS NULL OR a.semantic_atom_id>$4)
 ORDER BY a.semantic_atom_id LIMIT $5::bigint+1
), page AS MATERIALIZED (SELECT * FROM probe ORDER BY semantic_atom_id LIMIT $5), changed AS (
 DELETE FROM library_listing_outcomes a USING page p WHERE a.graph_id=p.graph_id AND a.source_item_id=p.source_item_id
 AND a.semantic_atom_id=p.semantic_atom_id AND a.generation=p.generation
 AND NOT EXISTS(SELECT 1 FROM library_listing_staged_outcomes s WHERE s.graph_id=$1 AND s.source_item_id=$2
 AND s.generation=$3 AND s.semantic_atom_id=a.semantic_atom_id) RETURNING 1
) SELECT (SELECT count(*) FROM page) scanned,(SELECT count(*) FROM changed) changed,
 (SELECT semantic_atom_id FROM page ORDER BY semantic_atom_id DESC LIMIT 1) after_key,
 (SELECT count(*) FROM probe)>$5 more"#;

#[cfg(test)]
mod tests {
    use super::*;
    /// The pre-split body, kept only to prove `canonical_ctes` is byte-identical.
    fn legacy_canonical_ctes(input: ItemInput) -> String {
        let keys = match input {
            ItemInput::Worker => "SELECT DISTINCT $1::uuid graph_id,unnest($2::uuid[]) source_item_id",
            ItemInput::Dirty => "SELECT DISTINCT graph_id,source_item_id FROM model_dirty_items",
            ItemInput::Changed => "SELECT DISTINCT graph_id,source_item_id FROM change_items",
        ItemInput::PageRows => "SELECT DISTINCT graph_id,source_item_id FROM model_listing_row_items",
        };
        let served =
            super::super::SERVED_VERSIONS_SQL.replace("FROM scope_sources s", "FROM listing_sources s");
        format!(
            r#"WITH listing_keys AS MATERIALIZED ({keys}),
listing_sources AS MATERIALIZED (
 SELECT s.graph_id,s.source_item_id,s.source_type,s.provider_namespace,s.source_id,s.origin,
  CASE WHEN s.origin IS NULL THEN s.source_type ELSE s.origin->>'content_kind' END discovery_content_kind,
  s.current_version_id,v.version_id selected_version_id,v.captured_at,
  substring(NULLIF(v.source_metadata->>'title','') FOR 256) title256,
  COALESCE(v.is_legacy_baseline,false) OR COALESCE(v.source_metadata->>'independence'='derived_copy',false) unknown_independence,
  v.extraction_status raw_extraction_status,v.next_start_char raw_next_start_char,
  CASE WHEN v.extraction_status='blocked' AND v.extraction_error IN
   ('managed_admission_required','capture_actor_unknown_managed_admission_required','managed_run_paused','execution_budget_exhausted','cloud_processing_disabled','requester_access_lost','source_removed_or_access_lost')
   THEN v.extraction_error END raw_extraction_reason
 FROM listing_keys k CROSS JOIN LATERAL (
  SELECT s.graph_id,s.source_item_id,s.source_type,s.provider_namespace,s.source_id,s.origin,s.current_version_id FROM source_items s WHERE s.graph_id=k.graph_id AND s.source_item_id=k.source_item_id
   AND s.access_state='permitted' OFFSET 0) s
 LEFT JOIN LATERAL (SELECT v.version_id,v.captured_at,v.source_metadata,v.is_legacy_baseline,v.extraction_status,v.next_start_char,v.extraction_error FROM source_versions v WHERE v.graph_id=s.graph_id
  AND v.source_item_id=s.source_item_id AND v.version_id=s.current_version_id OFFSET 0) v ON true
), listing_served AS MATERIALIZED ({served}),
listing_outcomes AS NOT MATERIALIZED (
 SELECT a.graph_id,a.semantic_atom_id,a.source_version_id,v.source_item_id,v.occurred_at,v.captured_at,
  COALESCE(a.source_version_id=COALESCE(p.version_id,s.current_version_id),false) source_version_is_current
 FROM listing_sources s CROSS JOIN LATERAL (SELECT v.graph_id,v.version_id,v.source_item_id,v.occurred_at,v.captured_at FROM source_versions v
  WHERE v.graph_id=s.graph_id AND v.source_item_id=s.source_item_id OFFSET 0) v
 LEFT JOIN listing_served p ON p.graph_id=s.graph_id AND p.source_item_id=s.source_item_id
 CROSS JOIN LATERAL (SELECT a.graph_id,a.semantic_atom_id,a.source_version_id FROM semantic_atoms a
  WHERE a.graph_id=v.graph_id AND a.source_version_id=v.version_id OFFSET 0) a
), listing_source_headers AS MATERIALIZED (
 SELECT s.*,COALESCE(p.version_id,s.current_version_id) served_version_id
 FROM listing_sources s LEFT JOIN listing_served p ON p.graph_id=s.graph_id AND p.source_item_id=s.source_item_id
), listing_source_facts AS MATERIALIZED (
 SELECT h.*,COALESCE(c.retained_count,0)::bigint retained_count,COALESCE(c.served_count,0)::bigint served_count
 FROM listing_source_headers h LEFT JOIN (SELECT graph_id,source_item_id,count(*) retained_count,
  count(*) FILTER(WHERE source_version_is_current) served_count FROM listing_outcomes GROUP BY 1,2) c
 ON c.graph_id=h.graph_id AND c.source_item_id=h.source_item_id
), listing_facets AS (
 SELECT graph_id,source_item_id,'service'::text dimension,to_jsonb(origin->>'service_id') key,NULL::text label
 FROM listing_source_headers WHERE origin->>'service_id' IS NOT NULL
 UNION ALL
 SELECT graph_id,source_item_id,'content_kind',to_jsonb(discovery_content_kind),NULL::text
 FROM listing_source_headers WHERE discovery_content_kind IS NOT NULL
 UNION ALL
 SELECT graph_id,source_item_id,'connection',jsonb_build_object('service_id',origin->'service_id','connection',origin->'connection'),NULL::text
 FROM listing_source_headers WHERE jsonb_typeof(origin->'connection')='object'
 UNION ALL
 SELECT graph_id,source_item_id,'container',jsonb_build_object('service_id',origin->'service_id','container',(origin->'container')-'label'),origin->'container'->>'label'
 FROM listing_source_headers WHERE jsonb_typeof(origin->'container')='object'
)"#
        )
    }

    /// The header/counts/rows split renders the full definition byte for
    /// byte as before, so no reader, worker or sealed SQL hash changes.
    #[test]
    fn the_mode_split_keeps_the_full_definition_byte_identical() {
        for input in [ItemInput::Worker, ItemInput::Dirty, ItemInput::Changed, ItemInput::PageRows] {
            assert_eq!(canonical_ctes(input), legacy_canonical_ctes(input), "{input:?}");
            let header = canonical_ctes_in(input, Mode::Header);
            let counts = canonical_ctes_in(input, Mode::Counts);
            assert!(header.contains("listing_sources AS") && header.contains("listing_source_headers AS"));
            assert!(!header.contains("listing_outcomes AS") && !header.contains("listing_source_facts AS"));
            assert!(counts.contains("listing_outcomes AS") && counts.contains("listing_source_facts AS"));
            assert!(!counts.contains("listing_facets AS"));
            // A lighter mode is the full text with whole CTEs left out, never
            // a re-spelled definition: each piece is a substring of Rows.
            let full = canonical_ctes(input);
            for piece in [LISTING_OUTCOMES_CTE, LISTING_SOURCE_FACTS_CTE, LISTING_FACETS_CTE] {
                assert!(full.contains(piece));
            }
            assert_eq!(header.replace(LISTING_OUTCOMES_CTE, ""), header);
            assert_eq!(full.replace(LISTING_OUTCOMES_CTE, "").replace(LISTING_SOURCE_FACTS_CTE, "").replace(LISTING_FACETS_CTE, ""), header);
            assert_eq!(full.replace(LISTING_FACETS_CTE, ""), counts);
        }
    }
    /// The Model's scope predicate is rendered from the one definition with
    /// the read's binds, and equals the text it had when it was inline.
    #[test]
    fn the_scope_predicate_renders_the_read_text_exactly() {
        let inline = r#"($2::jsonb='{}'::jsonb OR h.origin @> $2 OR (h.origin IS NULL AND $2::jsonb=jsonb_build_object('content_kind',h.source_type)))
 AND ($6::text IS NULL OR CASE $6
  WHEN 'agent_sessions' THEN h.source_type IN('coding_session','agent_session') OR h.origin->>'content_kind' IN('coding_session','agent_session')
  WHEN 'work_sessions' THEN h.source_type='work_session' OR h.origin->>'content_kind'='work_session'
  WHEN 'meetings' THEN h.source_type IN('transcript','meeting_session','teams_transcript','zoom_transcript','google_meet_transcript','slack_huddle_summary') OR h.origin->>'content_kind' IN('transcript','meeting','meeting_session') ELSE false END)"#;
        assert_eq!(scope_predicate("h", "$2", "$6"), inline);
    }
    #[test]
    fn selected_ids_do_not_swallow_a_late_lower_commit() {
        let ids = SelectedChangeIds::new(vec![20, 10, 20]).unwrap();
        assert_eq!(ids.as_slice(), &[10, 20]);
        assert!(ids.contains_unincorporated(&[9, 10, 20]));
        assert!(!ids.contains_unincorporated(&[10, 20]));
        assert!(SelectedChangeIds::new(vec![]).is_err());
        assert!(SelectedChangeIds::new(vec![0]).is_err());
    }
    #[test]
    fn input_key_chunk_budget_rejects_zero_and_unbounded_work() {
        assert!(ChunkLimit::new(0).is_err());
        assert!(ChunkLimit::new(2001).is_err());
        assert_eq!(ChunkLimit::new(128).unwrap().rows(), 128);
    }
}
