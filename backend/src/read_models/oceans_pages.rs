//! Ocean page models. Interactive `/nodes` pagination keeps its existing API.
#[path = "oceans_timing.rs"]
mod timing;
use super::{dirty::DirtyTrigger, *};
use crate::store::{page_sources, Node};

pub struct ReefPage;
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReefPageParams {
    #[serde(alias = "graph_id")]
    pub graph: Uuid,
    #[serde(default, rename = "type", alias = "node_type")]
    pub node_type: Option<String>,
    #[serde(default = "first_page")]
    pub page: i64,
    #[serde(default = "page_size")]
    pub page_size: i64,
}
fn first_page() -> i64 {
    1
}
fn page_size() -> i64 {
    20
}
impl Params for ReefPageParams {
    fn parse(
        path: BTreeMap<String, String>,
        query: BTreeMap<String, String>,
    ) -> Result<Self, Error> {
        let p: Self = super::parse_params(path, query)?;
        if p.page != 1 || !(1..=1000).contains(&p.page_size) {
            return Err("load model supports page 1 and page_size 1..1000".into());
        }
        if p.node_type
            .as_deref()
            .is_some_and(|t| !matches!(t, "entity" | "decision" | "thread" | "reference"))
        {
            return Err("invalid reef type".into());
        }
        Ok(p)
    }
}
#[derive(Debug, Serialize, Deserialize)]
pub struct ReefPagePayload {
    pub nodes: Vec<Node>,
    pub has_more: bool,
    pub page: i64,
    pub page_size: i64,
}
impl ReadModel for ReefPage {
    const NAME: &'static str = "reef_page";
    const PATH: &'static str = "/load/reefs";
    const VERSION: i32 = 1;
    const PER_USER: bool = true;
    type Params = ReefPageParams;
    type Payload = ReefPagePayload;
    fn account_graph(p: &Self::Params) -> Option<Uuid> {
        Some(p.graph)
    }
    fn graphs(_: &Viewer, p: &Self::Params) -> Vec<Uuid> {
        vec![p.graph]
    }
    fn compute<'a>(cx: ComputeCx<'a>, p: &'a Self::Params) -> ComputeFuture<'a, Self::Payload> {
        Box::pin(async move {
            if cx.graphs != [p.graph] || p.page != 1 || !(1..=1000).contains(&p.page_size) {
                return Err("invalid reef page scope".into());
            }
            #[cfg(feature = "e2e")]
            super::note_compute();
            let mut nodes = page_sources::list_nodes_scoped(
                cx.tx,
                p.graph,
                p.node_type.as_deref(),
                p.page_size + 1,
                0,
            )
            .await?;
            let has_more = nodes.len() > p.page_size as usize;
            nodes.truncate(p.page_size as usize);
            Ok(ReefPagePayload {
                nodes,
                has_more,
                page: 1,
                page_size: p.page_size,
            })
        })
    }
    fn empty(p: &Self::Params) -> Self::Payload {
        ReefPagePayload {
            nodes: vec![],
            has_more: false,
            page: 1,
            page_size: p.page_size,
        }
    }
    fn dirty_triggers() -> &'static [DirtyTrigger] {
        ocean_triggers()
    }
}
pub(crate) fn ocean_triggers() -> &'static [DirtyTrigger] {
    &[
        DirtyTrigger::Table("atoms"),
        DirtyTrigger::Table("nodes"),
        DirtyTrigger::Table("reef_identity_corrections"),
        DirtyTrigger::Table("managed_attempt_receipts"),
        DirtyTrigger::Table("node_atoms"),
        DirtyTrigger::Table("node_semantic_atoms"),
        DirtyTrigger::Table("semantic_atoms"),
        DirtyTrigger::Table("managed_enrichment_inputs"),
        DirtyTrigger::Table("source_items"),
        DirtyTrigger::Table("source_versions"),
        DirtyTrigger::Table("source_version_chunks"),
        DirtyTrigger::Table("relationship_edge"),
        DirtyTrigger::Table("graphs"),
    ]
}

pub struct Atlas;
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AtlasParams {
    #[serde(alias = "graph_id")]
    pub graph: Uuid,
    #[serde(default)]
    pub cooccur: Option<i64>,
    #[serde(default)]
    pub anchors: Anchors,
}
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Anchors {
    #[default]
    Top,
    All,
}
impl Params for AtlasParams {
    fn parse(
        mut path: BTreeMap<String, String>,
        query: BTreeMap<String, String>,
    ) -> Result<Self, Error> {
        if let Some(id) = path.remove("graph_id") {
            path.insert("graph".into(), id);
        }
        let p: Self = super::parse_params(path, query)?;
        if p.cooccur.is_some_and(|n| !(1..=5).contains(&n)) {
            return Err("cooccur must be 1..5".into());
        }
        Ok(p)
    }
}
// The large Atlas answer has concrete wire DTOs. Avoid rebuilding and
// serializing tens of thousands of generic JSON maps on every stored GET.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum AtlasNode {
    Ocean {
        id: String,
        label: String,
        kind: String,
        is_primary: bool,
    },
    Reef {
        id: Uuid,
        reef_kind: String,
        label: String,
        count: i64,
        promoted_from_graph_id: Option<Uuid>,
        is_self: bool,
    },
    Decision {
        id: Uuid,
        label: String,
        count: i64,
        date: DateTime<Utc>,
        promoted_from_graph_id: Option<Uuid>,
        is_self: bool,
    },
    Source {
        id: String,
        source_type: String,
        label: String,
    },
    Person {
        id: String,
        label: Option<String>,
        resolved: bool,
    },
    Coral {
        id: Uuid,
        label: String,
        source_type: String,
        src_id: String,
        reef_id: Option<Uuid>,
        person_id: Option<String>,
    },
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AtlasLink {
    pub a: String,
    pub b: String,
    pub kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub predicate: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub edge_id: Option<Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub origin: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub confidence: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub weight: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub anchor_rank: Option<usize>,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct AtlasPayload {
    pub ocean: Value,
    pub self_node_id: Option<Uuid>,
    pub cooccur: i64,
    pub nodes: Vec<AtlasNode>,
    pub links: Vec<AtlasLink>,
    pub truncated: AtlasTruncated,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct AtlasTruncated {
    pub atoms: bool,
}
impl ReadModel for Atlas {
    const NAME: &'static str = "atlas";
    const PATH: &'static str = "/graphs/{graph_id}/atlas";
    const VERSION: i32 = 1;
    // The people layer, primary flag and per-actor title admission can differ.
    const PER_USER: bool = true;
    type Params = AtlasParams;
    type Payload = AtlasPayload;
    fn account_graph(p: &Self::Params) -> Option<Uuid> {
        Some(p.graph)
    }
    fn graphs(_: &Viewer, p: &Self::Params) -> Vec<Uuid> {
        vec![p.graph]
    }
    fn compute<'a>(cx: ComputeCx<'a>, p: &'a Self::Params) -> ComputeFuture<'a, Self::Payload> {
        Box::pin(async move {
            if cx.graphs != [p.graph] {
                return Err("compute graph mismatch".into());
            }
            #[cfg(feature = "e2e")]
            super::note_compute();
            let q = crate::services::atlas_page::AtlasQuery {
                cooccur: p.cooccur,
                anchors: Some(
                    match p.anchors {
                        Anchors::All => "all",
                        Anchors::Top => "top",
                    }
                    .into(),
                ),
            };
            Ok(serde_json::from_value(
                crate::services::atlas_page::compute(cx.tx, p.graph, &q).await?,
            )?)
        })
    }
    fn empty(p: &Self::Params) -> Self::Payload {
        AtlasPayload {
            ocean: Value::Null,
            self_node_id: None,
            cooccur: p.cooccur.unwrap_or(2),
            nodes: vec![],
            links: vec![],
            truncated: AtlasTruncated { atoms: false },
        }
    }
    fn dirty_triggers() -> &'static [DirtyTrigger] {
        ocean_triggers()
    }
}

pub struct OceansIndex;
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OceansParams {
    pub graph: Option<Uuid>,
}
impl Params for OceansParams {
    fn normalize<'a>(
        mut self,
        tx: &'a mut sqlx::PgConnection,
        _user: Uuid,
    ) -> ComputeFuture<'a, Self> {
        Box::pin(async move {
            let tenant: bool =
                sqlx::query_scalar("SELECT to_regclass('public.tenant_meta') IS NOT NULL")
                    .fetch_one(&mut *tx)
                    .await?;
            if !tenant {
                let routed:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM tenant_databases WHERE account_id=NULLIF(current_setting('app.account_id',true),'')::uuid)").fetch_one(&mut *tx).await?;
                if routed {
                    return Err(Box::new(NotFound("account requires its tenant plane")) as Error);
                }
            }
            self.graph = None;
            Ok(self)
        })
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct OceanIndexRow {
    pub graph: Value,
    pub counts: crate::store::WorkspaceCounts,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct OceansPayload {
    pub oceans: Vec<OceanIndexRow>,
    pub members: Vec<Value>,
    pub team_members: BTreeMap<Uuid, Vec<Value>>,
    pub archived: Option<Vec<Value>>,
}
impl ReadModel for OceansIndex {
    const NAME: &'static str = "oceans_index";
    const PATH: &'static str = "/load/oceans/component";
    const VERSION: i32 = 1;
    const PER_USER: bool = true;
    const CONTROL_FACTS: bool = true;
    const ACCESS: access::AccessPolicy = access::AccessPolicy::OptionalAdmin;
    type Params = OceansParams;
    type Payload = OceansPayload;
    fn account_graph(p: &OceansParams) -> Option<Uuid> {
        p.graph
    }
    fn compute<'a>(cx: ComputeCx<'a>, _: &'a OceansParams) -> ComputeFuture<'a, Self::Payload> {
        Box::pin(async move {
            #[cfg(feature = "e2e")]
            super::note_compute();
            let mut phases = timing::Timing::new();
            let control = cx.control_tx.ok_or("ocean index requires control facts")?;
            phases.enter(timing::Phase::Roster);
            let members:Vec<Value>=sqlx::query_scalar("SELECT jsonb_build_object('user_id',u.id,'email',u.email,'role',m.role,'joined_at',m.joined_at) FROM account_members m JOIN users u ON u.id=m.user_id WHERE m.account_id=$1 ORDER BY m.joined_at")
            .bind(cx.account_id).fetch_all(&mut **control).await?;
            phases.finish();
            phases.members_n = members.len();
            phases.enter(timing::Phase::Metadata);
            let graphs:Vec<(Uuid,Value)>=sqlx::query_as("SELECT g.id,jsonb_build_object('id',g.id,'name',g.name,'kind',g.kind,'is_primary',g.is_primary,'created_at',g.created_at,'owner_user_id',g.owner_user_id,'archived_at',g.archived_at,'team_id',t.id,'my_team_role',m.role) FROM graphs g LEFT JOIN teams t ON t.graph_id=g.id LEFT JOIN team_members m ON m.team_id=t.id AND m.user_id=$2 AND m.departing_at IS NULL WHERE g.id=ANY($1) AND g.archived_at IS NULL ORDER BY g.created_at,g.id")
            .bind(cx.graphs).bind(cx.compute_as).fetch_all(&mut **cx.tx).await?;
            phases.finish();
            phases.graphs_n = graphs.len();
            let mut oceans = Vec::with_capacity(graphs.len());
            for (graph, metadata) in graphs {
                phases.enter(timing::Phase::Counts);
                let result = sqlx::query_as(super::oceans::COUNTS_SQL)
                    .bind(graph)
                    .fetch_one(&mut **cx.tx)
                    .await;
                phases.finish();
                let (reefs, atoms, sources): (Value, i64, i64) = result?;
                oceans.push(OceanIndexRow {
                    graph: metadata,
                    counts: crate::store::workspace_counts_from_row(reefs, atoms, sources),
                });
            }
            phases.enter(timing::Phase::RoleOrgFlag);
            let role: Option<String> = sqlx::query_scalar(
                "SELECT role FROM account_members WHERE account_id=$1 AND user_id=$2",
            )
            .bind(cx.account_id)
            .bind(cx.compute_as)
            .fetch_optional(&mut **control)
            .await?;
            let authority = matches!(role.as_deref(), Some("owner" | "admin"));
            let org: Option<String> =
                sqlx::query_scalar("SELECT clerk_org_id FROM accounts WHERE id=$1")
                    .bind(cx.account_id)
                    .fetch_one(&mut **control)
                    .await?;
            let teams = if let Some(org) = org.as_deref() {
                let flag:Option<bool>=sqlx::query_scalar("SELECT enabled FROM org_connector_flags WHERE clerk_org_id=$1 AND provider='teams'").bind(org).fetch_optional(&mut **control).await?;
                flag.unwrap_or_else(crate::workers::member_ocean_reconciler::org_teams_enabled)
                    && crate::workers::member_ocean_reconciler::member_oceans_enabled()
            } else {
                true
            };
            phases.finish();
            phases.enter(timing::Phase::Governance);
            // The sealed grant affects governance tables only. Never content graph
            // visibility; archived graphs still pass their ordinary graph RLS.
            if org.is_some() && teams && authority {
                if let Some(proof) = crate::tenancy::scope::verify_org_admin_tx(
                    &mut **control,
                    cx.account_id,
                    cx.compute_as,
                )
                .await?
                {
                    crate::tenancy::scope::set_tenant_org_admin(cx.tx, proof).await?;
                }
            }
            phases.finish();
            let mut team_members: BTreeMap<Uuid, Vec<Value>> = BTreeMap::new();
            if teams {
                phases.enter(timing::Phase::TeamPeople);
                let rows:Vec<(Uuid,Uuid,String,DateTime<Utc>)>=sqlx::query_as("SELECT t.id,m.user_id,m.role,m.joined_at FROM teams t JOIN team_members m ON m.team_id=t.id WHERE t.graph_id=ANY($1) AND m.departing_at IS NULL ORDER BY t.id,m.joined_at")
                .bind(cx.graphs).fetch_all(&mut **cx.tx).await?;
                phases.team_rows_n = rows.len();
                let ids: Vec<Uuid> = rows.iter().map(|r| r.1).collect();
                let people: Vec<(Uuid, Option<String>, String)> =
                    sqlx::query_as("SELECT id,name,email FROM users WHERE id=ANY($1)")
                        .bind(&ids)
                        .fetch_all(&mut **control)
                        .await?;
                phases.finish();
                let people: BTreeMap<_, _> = people
                    .into_iter()
                    .map(|(id, name, email)| (id, (name, email)))
                    .collect();
                for (team, user, role, joined) in rows {
                    let person = people.get(&user);
                    team_members.entry(team).or_default().push(serde_json::json!({"user_id":user,"role":role,"joined_at":joined,"name":person.and_then(|p|p.0.as_deref()),"email":person.map(|p|p.1.as_str())}));
                }
            }
            let archived = if teams && authority {
                phases.enter(timing::Phase::Archived);
                let rows = page_sources::list_archived_teams_scoped(cx.tx, cx.account_id).await?;
                phases.finish();
                Some(rows.into_iter().map(|t|{
                let (expires_at,restorable)=crate::routes::teams::team_restore_window(t.archived_at,cx.now);
                serde_json::json!({"team_id":t.team_id,"graph_id":t.graph_id,"name":t.name,"archived_at":t.archived_at,"expires_at":expires_at,"restorable":restorable})
            }).collect())
            } else {
                None
            };
            let payload = OceansPayload {
                oceans,
                members,
                team_members,
                archived,
            };
            phases.completed = 1;
            Ok(payload)
        })
    }
    fn empty(_: &OceansParams) -> Self::Payload {
        OceansPayload {
            oceans: vec![],
            members: vec![],
            team_members: BTreeMap::new(),
            archived: None,
        }
    }
    fn present(p: &mut Self::Payload, a: access::ReadAccess) {
        if !a.admin_fields {
            p.archived = None;
        }
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
            DirtyTrigger::Table("teams"),
            DirtyTrigger::Table("team_members"),
            DirtyTrigger::Table("graphs"),
            DirtyTrigger::Event(dirty::DirtyEvent::OrgAdminChanged),
        ]
    }
}

#[cfg(test)]
mod atlas_wire_contract {
    use super::*;
    use serde_json::json;
    #[test]
    fn concrete_dtos_preserve_legacy_nullable_and_variant_fields() {
        let id = Uuid::nil();
        for value in [
            json!({"type":"ocean","id":"ocean","label":"Fixture","kind":"org","is_primary":false}),
            json!({"type":"reef","id":id,"reef_kind":"entity","label":"Fixture","count":2,"promoted_from_graph_id":null,"is_self":false}),
            json!({"type":"decision","id":id,"label":"Fixture","count":2,"date":"2026-10-01T00:00:00Z","promoted_from_graph_id":id,"is_self":true}),
            json!({"type":"source","id":"src:note","source_type":"note","label":"note"}),
            json!({"type":"person","id":"person:opaque","label":null,"resolved":false}),
            json!({"type":"coral","id":id,"label":"Fixture","source_type":"note","src_id":"src:note","reef_id":null,"person_id":null}),
        ] {
            let typed: AtlasNode = serde_json::from_value(value.clone()).unwrap();
            assert_eq!(serde_json::to_value(typed).unwrap(), value);
        }
        for value in [
            json!({"a":"ocean","b":id,"kind":"ocean-reef"}),
            json!({"a":id,"b":id,"kind":"co-occurs","origin":"derived","weight":3,"owner":false}),
            json!({"a":id,"b":id,"kind":"decision-reef","origin":"derived","weight":2,"anchor_rank":1,"owner":true}),
            json!({"a":id,"b":id,"kind":"reef-reef","predicate":"works_on","edge_id":id,"status":"confirmed","origin":"stated","confidence":0.75,"owner":false}),
        ] {
            let typed: AtlasLink = serde_json::from_value(value.clone()).unwrap();
            assert_eq!(serde_json::to_value(typed).unwrap(), value);
        }
    }
}
