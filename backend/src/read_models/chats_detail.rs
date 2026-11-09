//! Conversation load pages; older-message pagination stays on the interactive API.
use super::{dirty::DirtyTrigger, *};
use serde_json::json;
pub struct ConversationDetail;
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DetailParams {
    pub id: Uuid,
}
impl Params for DetailParams {}
#[derive(Debug, Serialize, Deserialize)]
pub struct DetailPayload {
    pub conversation: Option<Value>,
    pub messages: Vec<Value>,
    pub messages_truncated: bool,
    pub older_before_seq: Option<i32>,
    /// Freshly normalized and object-validated in this exact snapshot. Never
    /// an authorization token: Ask revalidates all selectors for each question.
    pub resolved_scope: Option<crate::library_read::LibraryScope>,
}
impl ReadModel for ConversationDetail {
    const NAME: &'static str = "conversation_detail";
    const PATH: &'static str = "/load/conversations/{id}";
    const VERSION: i32 = 1;
    const PER_USER: bool = true;
    type Params = DetailParams;
    type Payload = DetailPayload;
    fn account_anchor(p: &Self::Params) -> Option<AccountAnchor> {
        Some(AccountAnchor::Conversation(p.id))
    }
    fn compute<'a>(cx: ComputeCx<'a>, p: &'a Self::Params) -> ComputeFuture<'a, Self::Payload> {
        Box::pin(async move {
            #[cfg(feature = "e2e")]
            super::note_compute();
            let allowed: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM conversations WHERE id=$1 AND account_id=$2 AND graph_id=ANY($3) AND (user_id=$4 OR shared))")
                .bind(p.id).bind(cx.account_id).bind(cx.graphs).bind(cx.compute_as).fetch_one(&mut **cx.tx).await?;
            if !allowed {
                return Ok(Self::empty(p));
            }
            let conv = crate::store::page_sources::get_conversation_scoped(cx.tx, p.id)
                .await?
                .ok_or("conversation unavailable")?;
            let raw: Option<Value> =
                sqlx::query_scalar("SELECT library_scope FROM conversations WHERE id=$1")
                    .bind(p.id)
                    .fetch_one(&mut **cx.tx)
                    .await?;
            let mut resolved_scope = None;
            let mut scope_status = if raw.is_some() {
                "unavailable"
            } else {
                "unscoped"
            };
            if let Some(mut scope) = raw
                .and_then(|v| serde_json::from_value::<crate::library_read::LibraryScope>(v).ok())
            {
                if scope.normalize().is_ok()
                    && scope.graph_ids.iter().all(|g| cx.graphs.contains(g))
                    && crate::library_read::validate_objects_tx(cx.tx, &scope).await?
                {
                    scope_status = "resolved";
                    resolved_scope = Some(scope);
                }
            }
            let temporal_selection: Option<Value> = if resolved_scope.is_some() {
                sqlx::query_scalar(
                    "SELECT temporal_selection FROM conversations WHERE id=$1 AND user_id=$2",
                )
                .bind(p.id)
                .bind(cx.compute_as)
                .fetch_optional(&mut **cx.tx)
                .await?
                .flatten()
                .or_else(|| Some(json!(crate::temporal::TemporalSelection::default())))
            } else {
                None
            };
            let mut graph_ids =
                crate::store::page_sources::list_conversation_oceans_scoped(cx.tx, p.id).await?;
            if graph_ids.is_empty() {
                graph_ids.push(conv.graph_id);
            }
            let find_enabled = crate::services::find_results::enabled();
            let name_ids = if find_enabled { cx.graphs } else { &graph_ids };
            let names: Vec<(Uuid, String)> =
                sqlx::query_as("SELECT id,name FROM graphs WHERE id=ANY($1) AND id=ANY($2) AND account_id=$3 AND archived_at IS NULL")
                    .bind(name_ids)
                    .bind(cx.graphs)
                    .bind(cx.account_id)
                    .fetch_all(&mut **cx.tx)
                    .await?;
            let names: BTreeMap<_, _> = names.into_iter().collect();
            let oceans: Vec<Value> = graph_ids.iter().filter(|id| cx.graphs.contains(id)).map(|g| json!({"ocean_id":g,"ocean_name":names.get(g).cloned().unwrap_or_default()})).collect();
            let (mut messages, messages_truncated, older_before_seq) =
                crate::services::conversation_page::messages(cx.tx, cx.compute_as, p.id, None)
                    .await?;
            if find_enabled {
                // Reuse names certified by this ComputeCx graph window/snapshot.
                let graphs: Vec<_> = names.into_iter().map(|(id,name)|
                    crate::services::find_results::GraphRef { id,name }).collect();
                crate::services::conversation_page::attach_find_results(cx.tx, p.id, &graphs, &mut messages).await?;
            }
            Ok(DetailPayload {
                conversation: Some(
                    json!({"id":conv.id,"graph_id":conv.graph_id,"title":conv.title,"pinned":conv.pinned,"shared":conv.shared,"mode":conv.mode,"oceans":oceans,"mine":conv.user_id==cx.compute_as,"created_at":conv.created_at,"updated_at":conv.updated_at,"library_scope":resolved_scope,"scope_status":scope_status,"temporal_selection":temporal_selection}),
                ),
                messages,
                messages_truncated,
                older_before_seq,
                resolved_scope,
            })
        })
    }
    fn empty(_: &Self::Params) -> Self::Payload {
        DetailPayload {
            conversation: None,
            messages: vec![],
            messages_truncated: false,
            older_before_seq: None,
            resolved_scope: None,
        }
    }
    fn overlay_current<'a>(
        tx: &'a mut PgConnection,
        scope: &'a Scope<Self::Params>,
        payload: &'a mut Self::Payload,
    ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'a>> {
        Box::pin(async move {
            use crate::services::message_feedback as feedback;
            if !feedback::enabled() {
                for message in &mut payload.messages {
                    if let Some(message) = message.as_object_mut() { message.remove("feedback"); }
                }
                return Ok(());
            }
            let ids = payload.messages.iter().filter(|m| m["role"] == "assistant")
                .map(|m| m["id"].as_str().ok_or("missing stored message id")?.parse::<Uuid>().map_err(|_| "invalid stored message id"))
                .collect::<Result<Vec<_>, _>>()?;
            for message in &mut payload.messages { message["feedback"] = Value::Null; }
            if ids.is_empty() { return Ok(()); }
            // The transcript remains the worker's certified snapshot. A saved
            // vote is a current per-actor control, including flips and clears.
            // Recheck the exact conversation/messages under live RLS; never
            // reuse the cached vote if this query fails or access was revoked.
            let current = feedback::for_visible_page(tx, scope.account_id, scope.compute_as,
                scope.params.id, scope.class.graphs(), &ids).await?;
            for message in &mut payload.messages {
                let id = message["id"].as_str().and_then(|id| id.parse::<Uuid>().ok());
                message["feedback"] = if message["role"] == "assistant" {
                    json!(id.and_then(|id| current.get(&id)))
                } else { Value::Null };
            }
            Ok(())
        })
    }
    fn present(p: &mut Self::Payload, _: access::ReadAccess) {
        if !crate::services::find_results::enabled() {
            for message in &mut p.messages {
                if let Some(message) = message.as_object_mut() { message.remove("source_results"); }
            }
        }
    }
    fn dirty_triggers() -> &'static [DirtyTrigger] {
        &[
            DirtyTrigger::Table("conversations"),
            DirtyTrigger::Table("conversation_messages"),
            DirtyTrigger::Table("conversation_oceans"),
            DirtyTrigger::Table("answer_support_receipts"),
            DirtyTrigger::Table("message_feedback"),
            DirtyTrigger::Table("query_history"),
            DirtyTrigger::Table("atoms"),
            DirtyTrigger::Table("nodes"),
            DirtyTrigger::Table("node_atoms"),
            DirtyTrigger::Table("source_items"),
            DirtyTrigger::Table("source_versions"),
            DirtyTrigger::Table("source_version_chunks"),
            DirtyTrigger::Table("graphs"),
            DirtyTrigger::Table("meeting_transcripts"),
            DirtyTrigger::Table("semantic_atoms"),
            DirtyTrigger::Table("node_semantic_atoms"),
            DirtyTrigger::Table("reef_identity_corrections"),
            DirtyTrigger::Table("semantic_atom_passages"),
        ]
    }
}

pub struct ChatEmpty;
#[derive(Debug, Serialize, Deserialize)]
pub struct ChatEmptyPayload {
    pub workspace_activity: Option<Value>,
    pub activity: Vec<Value>,
    pub connections: Option<Vec<Value>>,
    pub connections_status: String,
    pub teammate_count: Option<i64>,
    pub own_conversation_count: i64,
}
impl ReadModel for ChatEmpty {
    const NAME: &'static str = "chat_empty";
    const PATH: &'static str = "/load/chats/empty";
    const VERSION: i32 = 1;
    const PER_USER: bool = true;
    const CONTROL_FACTS: bool = true;
    const ACCESS: access::AccessPolicy = access::AccessPolicy::OptionalAdmin;
    type Params = super::home::WorkspaceActivityParams;
    type Payload = ChatEmptyPayload;
    fn compute<'a>(cx: ComputeCx<'a>, p: &'a Self::Params) -> ComputeFuture<'a, Self::Payload> {
        Box::pin(async move {
            #[cfg(feature = "e2e")]
            super::note_compute();
            let end = p
                .calendar_day
                .succ_opt()
                .ok_or("invalid calendar day")?
                .and_hms_opt(0, 0, 0)
                .ok_or("invalid calendar midnight")?
                .and_utc();
            let now = cx.now.min(end - chrono::Duration::nanoseconds(1));
            let workspace_activity = if crate::services::workspace_activity::enabled() {
                Some(serde_json::to_value(
                    crate::services::workspace_activity::compute(
                        &mut **cx.tx,
                        cx.account_id,
                        cx.graphs,
                        p.limit,
                        p.since_days,
                        now,
                    )
                    .await?,
                )?)
            } else {
                None
            };
            let activity:Vec<Value>=sqlx::query_scalar("SELECT jsonb_build_object('id',id,'user_id',user_id,'graph_id',graph_id,'event_type',event_type,'payload',payload,'created_at',created_at) FROM activity_events WHERE account_id=$1 AND (user_id=$2 OR user_id IS NULL) AND (graph_id IS NULL OR graph_id=ANY($3)) ORDER BY created_at DESC LIMIT 20")
            .bind(cx.account_id).bind(cx.compute_as).bind(cx.graphs).fetch_all(&mut **cx.tx).await?;
            let own_conversation_count:i64=sqlx::query_scalar("SELECT count(*) FROM conversations WHERE account_id=$1 AND user_id=$2 AND (graph_id IS NULL OR graph_id=ANY($3))")
            .bind(cx.account_id).bind(cx.compute_as).bind(cx.graphs).fetch_one(&mut **cx.tx).await?;
            let tx = cx
                .control_tx
                .ok_or("chat checklist requires control facts")?;
            let role: Option<String> = sqlx::query_scalar(
                "SELECT role FROM account_members WHERE account_id=$1 AND user_id=$2",
            )
            .bind(cx.account_id)
            .bind(cx.compute_as)
            .fetch_optional(&mut **tx)
            .await?;
            let org = cx.blobs.and_then(|b| b.org_id());
            let mut flags = BTreeMap::new();
            if let Some(org) = org {
                flags = sqlx::query_as::<_, (String, bool)>(
                    "SELECT provider,enabled FROM org_connector_flags WHERE clerk_org_id=$1",
                )
                .bind(org)
                .fetch_all(&mut **tx)
                .await?
                .into_iter()
                .collect();
            }
            let flag = |provider: &str, default: bool| {
                org.is_none() || flags.get(provider).copied().unwrap_or(default)
            };
            let slack = flag("slack", crate::routes::oauth::org_slack_oauth_enabled());
            let gmail = flag(
                "google_gmail",
                crate::connectors::google::surface_env_default(
                    crate::connectors::google::Surface::Gmail,
                ),
            );
            let chat = flag(
                "google_chat",
                crate::connectors::google::surface_env_default(
                    crate::connectors::google::Surface::Chat,
                ),
            );
            let meet = flag(
                "google_meet",
                crate::connectors::google::surface_env_default(
                    crate::connectors::google::Surface::Meet,
                ),
            );
            let calendar = flag(
                "google_calendar",
                crate::connectors::google::org_calendar_env_default(),
            );
            let github = flag(
                "github",
                matches!(std::env::var("ORG_GITHUB_ENABLED"),Ok(v) if v=="1"||v.eq_ignore_ascii_case("true")),
            );
            let microsoft = flag(
                "microsoft",
                crate::routes::teams_ingest::org_microsoft_teams_enabled(),
            );
            let allowed = slack || gmail || chat || meet || calendar || github;
            let connections = if allowed {
                use sqlx::Row;
                let rows=sqlx::query("SELECT provider,provider_workspace_id,scope,expires_at,created_at,credential_invalid_at,credential_invalid_reason,COALESCE(owner_user_id=$2,false) mine FROM oauth_tokens WHERE account_id=$1 AND (owner_user_id IS NULL OR owner_user_id=$2) ORDER BY provider,(owner_user_id IS NULL),updated_at DESC")
                .bind(cx.account_id).bind(cx.compute_as).fetch_all(&mut **tx).await?;
                Some(rows.into_iter().filter_map(|r|{
                let provider:String=r.get("provider");let visible=match provider.as_str(){"slack"|"slack_user"=>slack,"google_gmail"=>gmail,"google_chat"=>chat,"google_meet"=>meet,"google"=>calendar,"github"=>github,"microsoft"=>microsoft,_=>true};
                if !visible{return None}let scope:String=r.get("scope");let (missing_scopes,scopes_unknown)=crate::routes::oauth::scope_reconsent_status(&provider,&scope);
                Some(json!({"provider":provider,"provider_workspace_id":r.get::<Option<String>,_>("provider_workspace_id"),"scope":scope,"expires_at":r.get::<Option<DateTime<Utc>>,_>("expires_at"),"connected_at":r.get::<DateTime<Utc>,_>("created_at"),"missing_scopes":missing_scopes,"scopes_unknown":scopes_unknown,"credential_invalid_at":r.get::<Option<DateTime<Utc>>,_>("credential_invalid_at"),"credential_invalid_reason":r.get::<Option<String>,_>("credential_invalid_reason"),"mine":r.get::<bool,_>("mine")}))
            }).collect())
            } else {
                None
            };
            let teammate_count = if org.is_some()
                && matches!(role.as_deref(), Some("admin" | "owner"))
            {
                let teams = flag(
                    "teams",
                    crate::workers::member_ocean_reconciler::org_teams_enabled(),
                ) && crate::workers::member_ocean_reconciler::member_oceans_enabled();
                if teams {
                    Some(sqlx::query_scalar("SELECT count(*) FROM account_members WHERE account_id=$1 AND user_id<>$2").bind(cx.account_id).bind(cx.compute_as).fetch_one(&mut **tx).await?)
                } else {
                    None
                }
            } else {
                None
            };
            Ok(ChatEmptyPayload {
                workspace_activity,
                activity,
                connections,
                connections_status: if allowed { "available" } else { "unavailable" }.into(),
                teammate_count,
                own_conversation_count,
            })
        })
    }
    fn empty(_: &Self::Params) -> Self::Payload {
        ChatEmptyPayload {
            workspace_activity: None,
            activity: vec![],
            connections: None,
            connections_status: "pending".into(),
            teammate_count: None,
            own_conversation_count: 0,
        }
    }
    fn present(p: &mut Self::Payload, a: access::ReadAccess) {
        if !crate::services::workspace_activity::enabled() {p.workspace_activity=None;}
        if !a.admin_fields {
            p.teammate_count = None;
        }
    }
    fn dirty_triggers() -> &'static [DirtyTrigger] {
        &[
            DirtyTrigger::Table("atoms"),
            DirtyTrigger::Table("nodes"),
            DirtyTrigger::Table("graphs"),
            DirtyTrigger::Table("reef_identity_corrections"),
            DirtyTrigger::Table("node_semantic_atoms"),
            DirtyTrigger::Table("semantic_atoms"),
            DirtyTrigger::Table("source_items"),
            DirtyTrigger::Table("source_versions"),
            DirtyTrigger::Table("source_version_chunks"),
            DirtyTrigger::Table("node_atoms"),
            DirtyTrigger::Table("conversations"),
            DirtyTrigger::Table("conversation_messages"),
            DirtyTrigger::Table("activity_events"),
            DirtyTrigger::Table("coding_sessions"),
            DirtyTrigger::Table("query_history"),
            DirtyTrigger::Event(dirty::DirtyEvent::ConnectorGrantChanged),
            DirtyTrigger::Event(dirty::DirtyEvent::OrgAdminChanged),
        ]
    }
}
