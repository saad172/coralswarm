//! Sidebar data is materialized under its owner, including admitted titles and
//! ocean badges. No per-row title or ocean lookup occurs on a page request.
use super::{
    dirty::{DirtyEvent, DirtyTrigger},
    *,
};

pub struct ConversationList;
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConversationListParams {
    pub graph_id: Option<Uuid>,
    pub q: Option<String>,
}
impl Params for ConversationListParams {
    fn parse(
        path: BTreeMap<String, String>,
        query: BTreeMap<String, String>,
    ) -> Result<Self, Error> {
        if !path.is_empty()
            || query
                .keys()
                .any(|k| !matches!(k.as_str(), "graph_id" | "q"))
        {
            return Err("unknown conversation list parameter".into());
        }
        let graph_id = query.get("graph_id").map(|s| s.parse()).transpose()?;
        // Legacy cross-ocean listings ignore q. Normalize equivalent requests.
        let q = graph_id.and_then(|_| {
            query
                .get("q")
                .map(|s| s.trim().to_owned())
                .filter(|s| !s.is_empty())
        });
        Ok(Self { graph_id, q })
    }
}
impl ReadModel for ConversationList {
    const NAME: &'static str = "conversation_list";
    const PATH: &'static str = "/conversations";
    const VERSION: i32 = 1;
    const PER_USER: bool = true;
    type Params = ConversationListParams;
    type Payload = Value;
    fn account_graph(p: &Self::Params) -> Option<Uuid> {
        p.graph_id
    }
    fn graphs(v: &Viewer, p: &Self::Params) -> Vec<Uuid> {
        let mut graphs = v.visible_graphs();
        // Force verified-scope refusal rather than an empty computed answer for
        // an explicitly requested graph outside the actor's visibility.
        if let Some(g) = p.graph_id {
            graphs.push(g);
        }
        graphs
    }
    fn compute<'a>(cx: ComputeCx<'a>, p: &'a Self::Params) -> ComputeFuture<'a, Value> {
        Box::pin(async move {
            #[cfg(feature = "e2e")]
            super::note_compute();
            let pattern =
                p.q.as_ref()
                    .map(|q| format!("%{}%", q.replace('%', "\\%").replace('_', "\\_")));
            let payload: Value = sqlx::query_scalar(LIST_SQL)
                .bind(cx.graphs)
                .bind(cx.compute_as)
                .bind(p.graph_id)
                .bind(pattern)
                .fetch_one(&mut **cx.tx)
                .await?;
            Ok(payload)
        })
    }
    fn empty(_: &Self::Params) -> Value {
        serde_json::json!({"conversations":[],"shared":[]})
    }
    fn dirty_triggers() -> &'static [DirtyTrigger] {
        &[
            DirtyTrigger::Table("conversations"),
            DirtyTrigger::Table("conversation_messages"),
            DirtyTrigger::Table("conversation_oceans"),
            DirtyTrigger::Table("answer_support_receipts"),
            DirtyTrigger::Table("query_history"),
            DirtyTrigger::Table("activity_events"),
            DirtyTrigger::Table("graphs"),
            DirtyTrigger::Event(DirtyEvent::ConnectorGrantChanged),
        ]
    }
}

pub const LIST_SQL: &str = r#"
WITH candidates AS MATERIALIZED (
 SELECT c.*, c.user_id=$2 AS mine FROM conversations c
 WHERE c.graph_id=ANY($1) AND ($3::uuid IS NULL OR c.graph_id=$3)
   AND (c.user_id=$2 OR ($3::uuid IS NOT NULL AND $4::text IS NULL AND c.shared))
), history_ranked AS MATERIALIZED (
 SELECT c.*, row_number() OVER (PARTITION BY mine ORDER BY
   CASE WHEN mine AND $4::text IS NULL THEN pinned ELSE false END DESC,
   updated_at DESC,id) AS history_rank
 FROM candidates c
), title_candidates AS MATERIALIZED (
 -- Ordinary listings need only the bounded sidebar rows. Search must inspect
 -- admitted text throughout history before it can choose its matching rows.
 SELECT * FROM history_ranked WHERE $4::text IS NOT NULL OR history_rank<=200
), titled AS MATERIALIZED (
 SELECT c.*, library_conversation_title(c.id) AS admitted_title FROM title_candidates c
), ranked AS MATERIALIZED (
 SELECT c.*, row_number() OVER (PARTITION BY mine ORDER BY
   CASE WHEN mine AND $4::text IS NULL THEN pinned ELSE false END DESC,
   updated_at DESC,id) AS rank
 FROM titled c
 WHERE $4::text IS NULL OR (mine AND (admitted_title ILIKE $4 OR EXISTS (
   SELECT 1 FROM conversation_messages m WHERE m.conversation_id=c.id
    AND (m.role='user' OR library_answer_permitted(m.id)) AND m.text ILIKE $4)))
), selected AS MATERIALIZED (
 SELECT * FROM ranked WHERE rank<=CASE WHEN $4::text IS NULL THEN 200 ELSE 50 END
), badges AS MATERIALIZED (
 SELECT c.id, COALESCE(jsonb_agg(jsonb_build_object('ocean_id',g.id,'ocean_name',g.name)
   ORDER BY g.id) FILTER(WHERE g.id IS NOT NULL),'[]'::jsonb) oceans
 FROM selected c LEFT JOIN LATERAL (
   SELECT o.graph_id FROM conversation_oceans o WHERE o.conversation_id=c.id
   UNION ALL SELECT c.graph_id WHERE NOT EXISTS (
      SELECT 1 FROM conversation_oceans o WHERE o.conversation_id=c.id)
 ) o ON true LEFT JOIN graphs g ON g.id=o.graph_id AND g.id=ANY($1)
 GROUP BY c.id
), items AS (
 SELECT c.mine,c.rank,jsonb_build_object('id',c.id,'graph_id',c.graph_id,
   'title',c.admitted_title,'pinned',c.pinned,'shared',c.shared,'mode',c.mode,
   'oceans',b.oceans,'mine',c.mine,'created_at',c.created_at,'updated_at',c.updated_at) item
 FROM selected c JOIN badges b ON b.id=c.id
)
SELECT jsonb_build_object(
 'conversations',COALESCE(jsonb_agg(item ORDER BY rank) FILTER(WHERE mine),'[]'::jsonb),
 'shared',COALESCE(jsonb_agg(item ORDER BY rank) FILTER(WHERE NOT mine),'[]'::jsonb))
FROM items
"#;

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn params_reject_actor_and_normalize_legacy_search() {
        assert!(ConversationListParams::parse(
            BTreeMap::new(),
            [("user_id".into(), Uuid::nil().to_string())].into()
        )
        .is_err());
        assert!(ConversationListParams::parse(
            BTreeMap::new(),
            [("q".into(), " ignored ".into())].into()
        )
        .unwrap()
        .q
        .is_none());
    }
}
