//! Coral/capture load models. Source inventory/evidence pagination and actions
//! keep their interactive routes; no expiring credential-bearing URL is stored.
use super::{dirty::DirtyTrigger, *};
use serde_json::json;
pub struct CoralDetail;
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoralParams {
    pub atom: Uuid,
}
impl Params for CoralParams {}
#[derive(Debug, Serialize, Deserialize)]
pub struct CoralPayload {
    pub atom: Option<Value>,
    pub source_inventory: Option<Value>,
}
impl ReadModel for CoralDetail {
    const NAME: &'static str = "coral_detail";
    const PATH: &'static str = "/load/corals/{atom}";
    const VERSION: i32 = 1;
    const PER_USER: bool = true;
    type Params = CoralParams;
    type Payload = CoralPayload;
    fn account_anchor(p: &Self::Params) -> Option<AccountAnchor> {
        Some(AccountAnchor::Atom(p.atom))
    }
    fn compute<'a>(cx: ComputeCx<'a>, p: &'a Self::Params) -> ComputeFuture<'a, Self::Payload> {
        Box::pin(async move {
            #[cfg(feature = "e2e")]
            super::note_compute();
            let graph:Option<Uuid>=sqlx::query_scalar("SELECT graph_id FROM atoms WHERE atom_id=$1 AND graph_id=ANY($2) AND library_atom_permitted(graph_id,atom_id)")
            .bind(p.atom).bind(cx.graphs).fetch_optional(&mut **cx.tx).await?;
            let Some(graph) = graph else {
                return Ok(Self::empty(p));
            };
            let d = crate::store::page_sources::atom_detail_scoped(cx.tx, p.atom, cx.account_id)
                .await?
                .ok_or("coral unavailable")?;
            let content = match cx.blobs {
                Some(reader) => {
                    reader
                        .text(
                            cx.tx,
                            cx.account_id,
                            cx.graphs,
                            graph,
                            d.content_inline.as_deref(),
                            Some(&d.content_blob_key),
                        )
                        .await?
                }
                None => d.content_inline.clone(),
            }
            .map(|raw| crate::store::strip_header_prefix(&raw, d.header_len).to_owned());
            let title = crate::services::atom_title::display_title(
                crate::services::atom_title::DisplayTitleInput {
                    title: d.title.as_deref(),
                    excerpt: d.excerpt.as_deref(),
                    stripped_body: content.as_deref(),
                    source_type: &d.source_type,
                    channel_name: d.channel_name.as_deref(),
                    author_name: d.author_name.as_deref(),
                    timestamp: d.timestamp.unwrap_or(d.created_at),
                },
            );
            let linked: Vec<Value> = d
                .linked_reefs
                .iter()
                .map(|r| json!({"node_id":r.node_id,"type":r.node_type,"title":r.title}))
                .collect();
            let mut atom = json!({"atom_id":d.atom_id,"graph_id":graph,"source_type":d.source_type,"source_id":d.source_id,"content":content,
            "participants":d.participants,"timestamp":d.timestamp,"created_at":d.created_at,"scope":d.scope,"fts_language":d.fts_language,
            "channel_id":d.channel_id,"channel_name":d.channel_name,"author_id":d.author_id,"author_name":d.author_name,
            "title":title,"excerpt":d.excerpt,"linked_reefs":linked});
            crate::services::page_titles::project(cx.tx, cx.graphs, graph, &mut atom)
                .await?;
            Ok(CoralPayload {
                atom: Some(atom),
                source_inventory: Some(
                    crate::source_evidence::inventory_first_tx(cx.tx, graph, p.atom).await?,
                ),
            })
        })
    }
    fn empty(_: &Self::Params) -> Self::Payload {
        CoralPayload {
            atom: None,
            source_inventory: None,
        }
    }
    fn dirty_triggers() -> &'static [DirtyTrigger] {
        library_triggers()
    }
}
pub(crate) fn library_triggers() -> &'static [DirtyTrigger] {
    &[
        DirtyTrigger::Table("atoms"),
        DirtyTrigger::Table("nodes"),
        DirtyTrigger::Table("reef_identity_corrections"),
        DirtyTrigger::Table("managed_attempt_receipts"),
        DirtyTrigger::Table("node_atoms"),
        DirtyTrigger::Table("source_items"),
        DirtyTrigger::Table("source_versions"),
        DirtyTrigger::Table("source_version_chunks"),
        DirtyTrigger::Table("semantic_atoms"),
        DirtyTrigger::Table("managed_enrichment_inputs"),
        DirtyTrigger::Table("semantic_atom_passages"),
        DirtyTrigger::Table("node_semantic_atoms"),
        DirtyTrigger::Table("meeting_transcripts"),
    ]
}

pub struct CaptureDetail;
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CaptureKind {
    #[default]
    Source,
    Meeting,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CaptureParams {
    #[serde(alias = "graph_id")]
    pub graph: Uuid,
    pub source: String,
    #[serde(default)]
    pub kind: CaptureKind,
    #[serde(default)]
    pub version_id: Option<Uuid>,
    #[serde(default)]
    pub atom_id: Option<Uuid>,
}
impl Params for CaptureParams {
    fn normalize<'a>(self, tx: &'a mut PgConnection, _user: Uuid) -> ComputeFuture<'a, Self> {
        Box::pin(async move {
            let item = self.item_on(tx).await?;
            if let Some(item) = item {
                crate::source_evidence::validate_first_selectors_on(tx, self.graph, item, self.version_id, self.atom_id).await?;
            } else if self.version_id.is_some() || self.atom_id.is_some() || matches!(self.kind, CaptureKind::Source) {
                return Err(NotFound("source not found").into());
            }
            Ok(self)
        })
    }
    fn parse(
        path: BTreeMap<String, String>,
        query: BTreeMap<String, String>,
    ) -> Result<Self, Error> {
        let mut p: Self = super::parse_params(path, query)?;
        if p.source.is_empty() || p.source.len() > 2048 || p.source.contains('\0') {
            return Err("invalid source".into());
        }
        if matches!(p.kind, CaptureKind::Source) {
            p.source = p.source.parse::<Uuid>()?.to_string();
        }
        Ok(p)
    }
}
impl CaptureParams {
    // Metadata only: shared by request selector validation and snapshot compute.
    async fn item_on(&self, tx: &mut PgConnection) -> Result<Option<Uuid>, Error> {
        match self.kind {
            CaptureKind::Source => Ok(Some(self.source.parse()?)),
            CaptureKind::Meeting => {
                let floor = crate::history_floor::admits_transcript("t");
                Ok(sqlx::query_scalar(&format!("SELECT s.source_item_id FROM meeting_transcripts t JOIN source_items s ON s.source_item_id=t.source_item_id AND s.graph_id=t.graph_id WHERE t.graph_id=$1 AND t.source_id=$2 AND s.access_state='permitted' AND {floor}"))
                    .bind(self.graph).bind(&self.source).fetch_optional(tx).await.map_err(crate::store::StoreError::from)?)
            }
        }
    }
}
#[derive(Debug, Serialize, Deserialize)]
pub struct CapturePayload {
    pub summary: Option<Value>,
    pub evidence: Option<Value>,
}
impl ReadModel for CaptureDetail {
    const NAME: &'static str = "capture_detail";
    const HIDE_DENIED_SCOPE: bool = true;
    const PATH: &'static str = "/load/captures";
    const VERSION: i32 = 2;
    const PER_USER: bool = true;
    type Params = CaptureParams;
    type Payload = CapturePayload;
    fn account_graph(p: &Self::Params) -> Option<Uuid> {
        Some(p.graph)
    }
    fn graphs(_: &Viewer, p: &Self::Params) -> Vec<Uuid> {
        vec![p.graph]
    }
    fn compute<'a>(cx: ComputeCx<'a>, p: &'a Self::Params) -> ComputeFuture<'a, Self::Payload> {
        Box::pin(async move {
            #[cfg(feature = "e2e")]
            super::note_compute();
            if cx.graphs != [p.graph] {
                return Err("capture graph mismatch".into());
            }
            let resolved_item = p.item_on(cx.tx).await?;
            let (item, summary) = match p.kind {
                CaptureKind::Source => (Some(p.source.parse::<Uuid>()?), None),
                CaptureKind::Meeting => {
                    let floor = crate::history_floor::admits_transcript("t");
                    let mapping:Option<(Uuid,Uuid,Option<DateTime<Utc>>)>=sqlx::query_as(&format!("SELECT s.source_item_id,v.version_id,v.occurred_at FROM meeting_transcripts t JOIN source_items s ON s.source_item_id=t.source_item_id AND s.graph_id=t.graph_id JOIN source_versions v ON v.version_id=t.current_version_id AND v.source_item_id=s.source_item_id AND v.graph_id=s.graph_id WHERE t.graph_id=$1 AND t.source_id=$2 AND s.access_state='permitted' AND {floor}"))
                    .bind(p.graph).bind(&p.source).fetch_optional(&mut **cx.tx).await?;
                    // Preserve the verified-lineage contract: raw projected summaries
                    // and corrections have no complete receipt and remain unavailable.
                    let refs:Vec<Value>=mapping.as_ref().map(|(item,version,_)|json!({"graph_id":p.graph,"source_item_id":item,"source_version_id":version})).into_iter().collect();
                    let item = resolved_item;
                    let summary = json!({"source_id":p.source,"title":null,"occurred_at":mapping.and_then(|m|m.2),"raw_text":null,"corrected_text":null,"corrections":[],"merges":[],"new_entities":[],"summary_status":"unavailable","summary":null,"summary_model":null,"summary_error":null,"segments":null,"source_items":refs,"projection_status":"unverified_lineage","evidence_status":if refs.is_empty(){"unknown"}else{"retained"}});
                    (item, Some(summary))
                }
            };
            let evidence = match item {
                Some(item) => {
                    crate::source_evidence::first_evidence_tx(
                        cx.tx,
                        cx.blobs,
                        cx.account_id,
                        cx.graphs,
                        p.graph,
                        item,
                        p.version_id,
                        p.atom_id,
                    )
                    .await?
                }
                None => None,
            };
            Ok(CapturePayload { summary, evidence })
        })
    }
    fn empty(_: &Self::Params) -> Self::Payload {
        CapturePayload {
            summary: None,
            evidence: None,
        }
    }
    fn dirty_triggers() -> &'static [DirtyTrigger] {
        library_triggers()
    }
}
