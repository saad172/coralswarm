//! Topic load composes authoritative source reads in one certified snapshot.
//! Action/search and later-page Library endpoints remain interactive.
use super::{dirty::DirtyTrigger, *};
pub struct TopicPage;
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TopicParams {
    pub node: Uuid,
}
impl Params for TopicParams {}
#[derive(Debug, Serialize, Deserialize)]
pub struct TopicPayload {
    pub node: Option<Value>,
    pub edit_requires_admin: bool,
    pub digest: Option<Value>,
    pub sessions: Option<Value>,
    pub neighborhood: Option<Value>,
    pub search: Option<Value>,
}
impl ReadModel for TopicPage {
    const NAME: &'static str = "topic_page";
    const PATH: &'static str = "/load/topics/{node}";
    const VERSION: i32 = 1;
    const PER_USER: bool = true;
    const CONTROL_FACTS: bool = true;
    const ACCESS: access::AccessPolicy = access::AccessPolicy::OptionalAdmin;
    type Params = TopicParams;
    type Payload = TopicPayload;
    fn account_anchor(p: &Self::Params) -> Option<AccountAnchor> {
        Some(AccountAnchor::Node(p.node))
    }
    fn compute<'a>(cx: ComputeCx<'a>, p: &'a Self::Params) -> ComputeFuture<'a, Self::Payload> {
        Box::pin(async move {
            #[cfg(feature = "e2e")]
            super::note_compute();
            let graph: Option<Uuid> = sqlx::query_scalar("SELECT graph_id FROM nodes WHERE node_id=$1 AND graph_id=ANY($2) AND library_node_permitted(graph_id,node_id)")
                .bind(p.node).bind(cx.graphs).fetch_optional(&mut **cx.tx).await?;
            let Some(graph) = graph else {
                return Ok(Self::empty(p));
            };
            sqlx::query("SET LOCAL jit=off")
                .execute(&mut **cx.tx)
                .await?;
            let mut scope = crate::library_read::LibraryScope::exact_node(graph, p.node);
            scope.normalize()?;
            if !crate::library_read::validate_objects_tx(cx.tx, &scope).await? {
                return Ok(Self::empty(p));
            }
            let mut node: Value = sqlx::query_scalar("SELECT jsonb_build_object('node_id',n.node_id,'type',n.type,'title',library_node_title(n.graph_id,n.node_id),'scope',n.scope,'created_at',n.created_at,'updated_at',n.updated_at,'graph_id',n.graph_id) FROM nodes n WHERE n.node_id=$1 AND n.graph_id=ANY($2)")
                .bind(p.node).bind(cx.graphs).fetch_one(&mut **cx.tx).await?;
            let (_, count): (Option<String>, i64) =
                sqlx::query_as(crate::store::verdict::NODE_ADMISSION_SQL)
                    .bind(graph)
                    .bind(p.node)
                    .fetch_one(&mut **cx.tx)
                    .await?;
            node["atom_count"] = json!(count);
            let extras =
                crate::store::library::library_node_extras(cx.tx, p.node, cx.graphs).await?;
            node["entity_type"] = json!(extras.entity_type);
            node["entity_type_sticky"] = json!(extras.entity_type_sticky);
            node["last_happened_at"] = json!(extras.last_happened_at);
            node["lineage"] = json!(extras.lineage.iter().map(|l| json!({"node_id":l.node_id,"title":l.title,"graph_id":l.graph_id,"graph_name":l.graph_name})).collect::<Vec<_>>());
            node["decisions"] = json!(extras
                .decisions
                .iter()
                .map(|d| json!({"node_id":d.node_id,"title":d.title,"created_at":d.created_at}))
                .collect::<Vec<_>>());
            let counts = crate::source_evidence::node_evidence_counts_tx(cx.tx, graph, &[p.node])
                .await?
                .pop();
            node["semantic_atom_count"] = json!(counts.as_ref().map(|c| c.semantic_atom_count));
            node["source_support_count"] = json!(counts.as_ref().map(|c| c.source_support_count));
            node["unmapped_legacy_chunk_count"] =
                json!(counts.as_ref().map(|c| c.unmapped_legacy_chunk_count));
            let control = cx.control_tx.ok_or("topic requires control facts")?;
            let admin: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM account_members WHERE account_id=$1 AND user_id=$2 AND role IN ('admin','owner'))")
                .bind(cx.account_id).bind(cx.compute_as).fetch_one(&mut **control).await?;
            let editable: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM graphs WHERE id=$1 AND account_id=$2 AND ((kind='personal' OR (kind='member' AND owner_user_id=$3)) OR (kind IN ('org','team') AND $4)))")
                .bind(graph).bind(cx.account_id).bind(cx.compute_as).bind(admin).fetch_one(&mut **cx.tx).await?;
            node["can_edit"] = json!(editable);
            let edit_requires_admin: bool =
                sqlx::query_scalar("SELECT kind IN ('org','team') FROM graphs WHERE id=$1")
                    .bind(graph)
                    .fetch_one(&mut **cx.tx)
                    .await?;
            let safe_titles = crate::services::journal_activities::project_node_titles(
                cx.tx,
                &[graph],
                &[p.node],
            )
            .await?;
            let digest = if safe_titles
                .get(&(graph, p.node))
                .is_some_and(|title| node["title"].as_str() == Some(title.as_str()))
            {
                crate::reef_digest_read::read_for_reader(cx.tx, p.node)
                    .await?
                    .map(|d| d.http_body(false))
            } else {
                Some(json!({"summary":null,"excerpts":[],"unavailable":true,"regenerating":false}))
            };
            let payload = TopicPayload {
                node: Some(node),
                edit_requires_admin,
                digest,
                sessions: Some(
                    crate::library_read::node_sessions_first_tx(cx.tx, &scope, p.node).await?,
                ),
                neighborhood: Some(
                    crate::library_read::neighborhood_first_tx(cx.tx, &scope, p.node).await?,
                ),
                search: Some(crate::library_read::page_search_tx(cx.tx, &scope).await?),
            };
            let mut value = serde_json::to_value(payload)?;
            crate::services::page_titles::project(cx.tx, cx.graphs, graph, &mut value)
                .await?;
            Ok(serde_json::from_value(value)?)
        })
    }
    fn empty(_: &Self::Params) -> Self::Payload {
        TopicPayload {
            node: None,
            edit_requires_admin: false,
            digest: None,
            sessions: None,
            neighborhood: None,
            search: None,
        }
    }
    fn present(p: &mut Self::Payload, a: access::ReadAccess) {
        if p.edit_requires_admin && !a.account_authority {
            if let Some(node) = p.node.as_mut() {
                node["can_edit"] = json!(false);
            }
        }
    }
    fn dirty_triggers() -> &'static [DirtyTrigger] {
        &[
            DirtyTrigger::Table("nodes"),
        DirtyTrigger::Table("reef_identity_corrections"),
        DirtyTrigger::Table("managed_attempt_receipts"),
            DirtyTrigger::Table("atoms"),
            DirtyTrigger::Table("node_atoms"),
            DirtyTrigger::Table("node_semantic_atoms"),
            DirtyTrigger::Table("semantic_atoms"),
            DirtyTrigger::Table("source_items"),
            DirtyTrigger::Table("source_versions"),
            DirtyTrigger::Table("source_version_chunks"),
            DirtyTrigger::Table("coding_sessions"),
            DirtyTrigger::Table("relationship_edge"),
            DirtyTrigger::Table("managed_enrichment_inputs"),
            DirtyTrigger::Event(dirty::DirtyEvent::OrgAdminChanged),
        ]
    }
}
use serde_json::json;
