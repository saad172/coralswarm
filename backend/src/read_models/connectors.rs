//! Connector page answers. Only workers run admission/control reads; HTTP reads
//! address stored rows using the verified actor and graph set.
use super::{
    dirty::{DirtyEvent, DirtyTrigger},
    *,
};
use crate::connectors::catalog::{self, activity, adapters, Facts, Importing, SourceRuntime};
use sqlx::Row;
use std::collections::HashMap;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NoParams {}
impl Params for NoParams {}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DetailParams {
    pub provider: String,
}
impl Params for DetailParams {
    fn parse(
        path: BTreeMap<String, String>,
        query: BTreeMap<String, String>,
    ) -> Result<Self, Error> {
        let mut values = query;
        for (k, v) in path {
            if values.get(&k).is_some_and(|q| q != &v) {
                return Err("conflicting path/query parameter".into());
            }
            values.insert(k, v);
        }
        if values.len() != 1 || !values.contains_key("provider") {
            return Err("unknown connector parameters".into());
        }
        let provider = values.remove("provider").unwrap();
        if catalog::registry::lookup(&provider).is_none() {
            return Err(Box::new(super::NotFound("unknown connector")));
        }
        Ok(Self { provider })
    }
}
const TRIGGERS: &[DirtyTrigger] = &[
    DirtyTrigger::Table("atoms"),
    DirtyTrigger::Table("nodes"),
    DirtyTrigger::Table("reef_identity_corrections"),
    DirtyTrigger::Table("node_atoms"),
    DirtyTrigger::Table("node_semantic_atoms"),
    DirtyTrigger::Table("composio_capture_scopes"),
    DirtyTrigger::Table("connector_sources"),
    DirtyTrigger::Table("github_watched_repos"),
    DirtyTrigger::Table("slack_watched_channels"),
    DirtyTrigger::Table("gmail_watched_threads"),
    DirtyTrigger::Table("outlook_watched_conversations"),
    DirtyTrigger::Table("teams_watched_resources"),
    DirtyTrigger::Table("teams_meeting_captures"),
    DirtyTrigger::Table("teams_meeting_installations"),
    DirtyTrigger::Table("google_chat_watched_spaces"),
    DirtyTrigger::Table("google_meet_captures"),
    DirtyTrigger::Event(DirtyEvent::ConnectorGrantChanged),
];

async fn facts(cx: &mut ComputeCx<'_>) -> Result<Facts, Error> {
    let control = cx
        .control_tx
        .as_mut()
        .ok_or("connector control facts unavailable")?;
    let mut facts = Facts::load_scoped(control, cx.account_id, cx.compute_as).await?;
    if !facts.installs.is_empty() {
        let rows=sqlx::query_as::<_,catalog::github_facts::RepoRow>("SELECT active,sync_error_code,sync_error_at,backfill_progress FROM github_watched_repos WHERE account_id=$1 AND owner_user_id=$2 AND graph_id=ANY($3) LIMIT 1000")
            .bind(cx.account_id).bind(cx.compute_as).bind(cx.graphs).fetch_all(&mut **cx.tx).await?;
        facts.github = catalog::github_facts::fold_repo_rows(&rows);
    }
    Ok(facts)
}
async fn runtimes(
    cx: &mut ComputeCx<'_>,
    facts: &Facts,
) -> Result<HashMap<(String, String), SourceRuntime>, Error> {
    let mut out = HashMap::new();
    for adapter in catalog::registry::all() {
        for spec in adapter.sources() {
            let token = match spec.key {
                "gmail" => "google_gmail",
                "chat" => "google_chat",
                "meet" => "google_meet",
                _ => "google",
            };
            let legacy = match adapter.key() {
                "google" => facts.token(token).is_some(),
                "github" => facts.github.active > 0,
                _ => true,
            };
            let mut rt = adapters::runtime_from_row(
                facts.source(adapter.key(), spec.key),
                spec,
                adapter.key(),
                legacy,
            );
            if adapter.key() == "google" && spec.key == "chat" {
                if let Some(t) = facts.token(token) {
                    if let Some(missing) = crate::connectors::google::chat_missing_scopes(&t.scope)
                        .filter(|m| !m.is_empty())
                    {
                        rt.problem = Some(
                            catalog::Problem::new("missing_scopes", t.created_at)
                                .for_source(spec.key)
                                .with_param(
                                    "count",
                                    catalog::ParamValue::Num(missing.len() as i64),
                                ),
                        );
                    }
                }
            }
            let query=match (adapter.key(),spec.key) {
                ("google","gmail")=>Some("SELECT count(*) FROM gmail_watched_threads WHERE account_id=$1 AND ingested_by=$2 AND graph_id=ANY($3) AND follow_replies"),
                ("google","meet")=>Some("SELECT count(*) FROM google_meet_captures WHERE account_id=$1 AND owner_user_id=$2 AND status<>'revoked'"),
                ("google","chat")=>Some("SELECT count(*) FROM google_chat_watched_spaces WHERE account_id=$1 AND owner_user_id=$2 AND graph_id=ANY($3) AND active"),
                ("microsoft","mail")=>Some("SELECT count(*) FROM outlook_watched_conversations WHERE account_id=$1 AND ingested_by=$2 AND graph_id=ANY($3) AND follow_replies"),
                ("microsoft","teams_chats")=>Some("SELECT count(*) FROM teams_watched_resources WHERE account_id=$1 AND user_id=$2 AND graph_id=ANY($3) AND kind='chat'"),
                ("microsoft","teams_channels")=>Some("SELECT count(*) FROM teams_watched_resources WHERE account_id=$1 AND user_id=$2 AND graph_id=ANY($3) AND kind='channel'"),
                ("microsoft","teams_meetings")=>Some("SELECT count(*) FROM teams_meeting_captures WHERE account_id=$1 AND user_id=$2 AND graph_id=ANY($3) AND revoked_at IS NULL"),
                _=>None,
            };
            if let Some(query) = query {
                let statement = sqlx::query_scalar(query)
                    .bind(cx.account_id)
                    .bind(cx.compute_as);
                let statement = if adapter.key() == "google" && spec.key == "meet" {
                    statement
                } else {
                    statement.bind(cx.graphs)
                };
                let n: i64 = statement.fetch_one(&mut **cx.tx).await?;
                rt = adapters::with_count(rt, n.min(u32::MAX as i64) as u32);
            }
            if adapter.key() == "slack" {
                let control = cx.control_tx.as_mut().ok_or("control facts unavailable")?;
                let n:i64=sqlx::query_scalar("SELECT count(*) FROM slack_member_channels WHERE account_id=$1 AND user_id=$2 AND selected")
                    .bind(cx.account_id).bind(cx.compute_as).fetch_one(&mut ***control).await?;
                rt = adapters::with_count(rt, n.min(u32::MAX as i64) as u32);
                if let Some(row) = facts.source("slack", spec.key) {
                    if let (Some(days), Some(requested)) = (
                        row.history_days.filter(|d| *d > 0),
                        row.history_requested_at
                            .filter(|at| cx.now - *at < chrono::Duration::days(1)),
                    ) {
                        let counts:(i64,i64)=sqlx::query_as("SELECT count(*) FILTER(WHERE status IN ('done','dead')),count(*) FROM jobs WHERE account_id=$1 AND kind='HistoryBackfill' AND starts_with(idempotency_key,$2)")
                            .bind(cx.account_id).bind(catalog::history::slack::key_prefix(cx.compute_as,requested)).fetch_one(&mut **cx.tx).await?;
                        rt.importing = catalog::history::slack::in_flight_percent(
                            counts.0, counts.1,
                        )
                        .map(|percent| Importing {
                            percent: Some(percent),
                            window_days: days as u32,
                        });
                    }
                }
            }
            if adapter.key() == "github" {
                rt = adapters::with_count(rt, facts.github.active.min(u32::MAX as i64) as u32);
                rt.importing = facts.github.importing_percent.map(|percent| Importing {
                    percent: Some(percent),
                    window_days: rt.history_days.unwrap_or(0),
                });
                rt.problem = facts
                    .github
                    .error
                    .as_ref()
                    .map(|(code, at)| catalog::Problem::new(code, *at).for_source(spec.key));
            }
            let progress_sql=match (adapter.key(),spec.key) {
                ("google","chat")=>Some("SELECT EXISTS(SELECT 1 FROM google_chat_watched_spaces WHERE account_id=$1 AND owner_user_id=$2 AND graph_id=ANY($3) AND active AND NOT backfill_complete)"),
                ("microsoft","teams_chats")=>Some("SELECT EXISTS(SELECT 1 FROM teams_watched_resources WHERE account_id=$1 AND user_id=$2 AND graph_id=ANY($3) AND kind='chat' AND NOT backfill_complete)"),
                ("microsoft","teams_channels")=>Some("SELECT EXISTS(SELECT 1 FROM teams_watched_resources WHERE account_id=$1 AND user_id=$2 AND graph_id=ANY($3) AND kind='channel' AND NOT backfill_complete)"),
                _=>None,
            };
            if let (Some(sql), Some(row)) = (progress_sql, facts.source(adapter.key(), spec.key)) {
                if let Some(days) = row.history_days.filter(|d| *d > 0) {
                    let pending: bool = sqlx::query_scalar(sql)
                        .bind(cx.account_id)
                        .bind(cx.compute_as)
                        .bind(cx.graphs)
                        .fetch_one(&mut **cx.tx)
                        .await?;
                    if pending {
                        rt.importing = Some(Importing {
                            percent: None,
                            window_days: days as u32,
                        });
                    }
                }
            }
            out.insert((adapter.key().into(), spec.key.into()), rt);
        }
    }
    Ok(out)
}
fn empty_list() -> Value {
    let facts = Facts::default();
    let activity = activity::ActivityByType::default();
    let rt = HashMap::new();
    serde_json::json!({"connectors":catalog::registry::all().iter().map(|a|catalog::summary::build_stored(a.as_ref(),&facts,&rt,&activity).map(|(s,_)|s)).collect::<Result<Vec<_>,_>>().expect("pure default facts")})
}
pub struct ConnectorsList;
impl ReadModel for ConnectorsList {
    const NAME: &'static str = "connectors_list";
    const PATH: &'static str = "/connectors";
    const VERSION: i32 = 1;
    const PER_USER: bool = true;
    const CONTROL_FACTS: bool = true;
    type Params = NoParams;
    type Payload = Value;
    fn compute<'a>(mut cx: ComputeCx<'a>, _: &'a NoParams) -> ComputeFuture<'a, Value> {
        Box::pin(async move {
            #[cfg(feature = "e2e")]
            super::note_compute();
            let facts = facts(&mut cx).await?;
            let runtimes = runtimes(&mut cx, &facts).await?;
            let activity = activity::ActivityByType::load_scoped(
                cx.tx,
                cx.account_id,
                cx.compute_as,
                cx.graphs,
            )
            .await?;
            let connectors = catalog::registry::all()
                .iter()
                .map(|a| {
                    catalog::summary::build_stored(a.as_ref(), &facts, &runtimes, &activity)
                        .map(|(s, _)| s)
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(serde_json::json!({"connectors":connectors}))
        })
    }
    fn empty(_: &NoParams) -> Value {
        empty_list()
    }
    fn dirty_triggers() -> &'static [DirtyTrigger] {
        TRIGGERS
    }
}

pub struct ConnectorDetail;
impl ReadModel for ConnectorDetail {
    const NAME: &'static str = "connector_detail";
    const PATH: &'static str = "/connectors/{provider}";
    const VERSION: i32 = 1;
    const PER_USER: bool = true;
    const CONTROL_FACTS: bool = true;
    type Params = DetailParams;
    type Payload = Value;
    fn compute<'a>(mut cx: ComputeCx<'a>, p: &'a DetailParams) -> ComputeFuture<'a, Value> {
        Box::pin(async move {
            #[cfg(feature = "e2e")]
            super::note_compute();
            let adapter = catalog::registry::lookup(&p.provider).ok_or("unknown connector")?;
            let facts = facts(&mut cx).await?;
            let runtimes = runtimes(&mut cx, &facts).await?;
            let activity = activity::ActivityByType::load_scoped(
                cx.tx,
                cx.account_id,
                cx.compute_as,
                cx.graphs,
            )
            .await?;
            let (summary, grant) =
                catalog::summary::build_stored(adapter, &facts, &runtimes, &activity)?;
            let mut payload = serde_json::to_value(catalog::summary::ConnectorDetail {
                summary,
                account_label: grant.as_ref().and_then(|g| g.account_label.clone()),
                connected_at: grant.as_ref().map(|g| g.connected_at),
                topics: activity::topics_scoped(
                    cx.tx,
                    cx.account_id,
                    cx.compute_as,
                    cx.graphs,
                    adapter,
                )
                .await?,
            })?;
            payload["captures"] = serde_json::to_value(
                activity::captures_scoped(
                    cx.tx,
                    cx.account_id,
                    cx.compute_as,
                    cx.graphs,
                    adapter,
                    activity::CaptureQuery::default(),
                )
                .await?,
            )?;
            payload["watched"] = watched(&mut cx, &p.provider).await?;
            payload["addin_tokens"] = addin_tokens(&mut cx, &p.provider).await?;
            Ok(payload)
        })
    }
    fn empty(p: &DetailParams) -> Value {
        let mut list = empty_list();
        let mut item = list["connectors"]
            .as_array_mut()
            .unwrap()
            .iter()
            .find(|v| v["key"] == p.provider)
            .cloned()
            .unwrap_or_else(|| serde_json::json!({}));
        item["account_label"] = Value::Null;
        item["connected_at"] = Value::Null;
        item["topics"] = serde_json::json!([]);
        item["captures"] = serde_json::json!({"items":[],"next_cursor":null});
        item["watched"] = serde_json::json!({"threads":[],"next_cursor":null});
        item["addin_tokens"] = serde_json::json!({"tokens":[]});
        item
    }
    fn dirty_triggers() -> &'static [DirtyTrigger] {
        TRIGGERS
    }
}

async fn watched(cx: &mut ComputeCx<'_>, provider: &str) -> Result<Value, Error> {
    let (table, key, source) = match provider {
        "google" => ("gmail_watched_threads", "thread_id", "gmail"),
        "microsoft" => (
            "outlook_watched_conversations",
            "conversation_id",
            "outlook_mail",
        ),
        _ => return Ok(serde_json::json!({"threads":[],"next_cursor":null})),
    };
    // Keys are static provider inventory. The title is projected only from the
    // admitted relation; a watcher row alone never authorizes atom text.
    let sql=format!("WITH permitted AS MATERIALIZED(SELECT g.id graph_id,p.atom_id FROM graphs g CROSS JOIN LATERAL library_permitted_atoms(g.id,NULL) p WHERE g.account_id=$1 AND g.id=ANY($3)) SELECT w.id,w.{key} thread_id,w.follow_replies,w.last_ingested_at,(SELECT a.title FROM atoms a JOIN permitted p ON p.graph_id=a.graph_id AND p.atom_id=a.atom_id WHERE a.graph_id=w.graph_id AND a.source_type=$4 AND a.source_id=w.{key} LIMIT 1) title FROM {table} w WHERE w.account_id=$1 AND w.ingested_by=$2 AND w.graph_id=ANY($3) ORDER BY w.last_ingested_at DESC,w.id DESC LIMIT 51");
    let rows = sqlx::query(&sql)
        .bind(cx.account_id)
        .bind(cx.compute_as)
        .bind(cx.graphs)
        .bind(source)
        .fetch_all(&mut **cx.tx)
        .await?;
    let next = if rows.len() > 50 {
        rows.get(49).map(|r| r.get::<Uuid, _>("id"))
    } else {
        None
    };
    let threads:Vec<Value>=rows.iter().take(50).map(|r|serde_json::json!({"thread_id":r.get::<String,_>("thread_id"),"following":r.get::<bool,_>("follow_replies"),"title":if provider=="microsoft"{None}else{r.get::<Option<String>,_>("title")},"saved_at":r.get::<DateTime<Utc>,_>("last_ingested_at").to_rfc3339()})).collect();
    Ok(serde_json::json!({"threads":threads,"next_cursor":next}))
}
async fn addin_tokens(cx: &mut ComputeCx<'_>, provider: &str) -> Result<Value, Error> {
    if provider != "google" {
        return Ok(serde_json::json!({"tokens":[]}));
    }
    let control = cx.control_tx.as_mut().ok_or("control facts unavailable")?;
    // Add-on token management belongs to the personal account only.
    let personal: bool =
        sqlx::query_scalar("SELECT a.clerk_org_id IS NULL OR EXISTS (SELECT 1 FROM personal_orgs po JOIN users u ON u.clerk_user_id=po.clerk_user_id WHERE po.clerk_org_id=a.clerk_org_id AND u.id=$2) FROM accounts a WHERE a.id=$1")
            .bind(cx.account_id)
            .bind(cx.compute_as)
            .fetch_one(&mut ***control)
            .await?;
    if !personal {
        return Ok(serde_json::json!({"tokens":[]}));
    }
    let rows:Vec<(Uuid,String,DateTime<Utc>,Option<DateTime<Utc>>)>=sqlx::query_as("SELECT id,name,created_at,last_used_at FROM addin_tokens WHERE owner_user_id=$1 AND account_id=$2 AND revoked_at IS NULL ORDER BY created_at DESC")
        .bind(cx.compute_as).bind(cx.account_id).fetch_all(&mut ***control).await?;
    let tokens:Vec<Value>=rows.into_iter().map(|(id,name,created_at,last_used_at)|serde_json::json!({"id":id,"name":name,"created_at":created_at,"last_used_at":last_used_at})).collect();
    Ok(serde_json::json!({"tokens":tokens}))
}

pub struct ConnectorStatus;
impl ReadModel for ConnectorStatus {
    const NAME: &'static str = "connector_status";
    const PATH: &'static str = "/connectors/status";
    const VERSION: i32 = 1;
    const PER_USER: bool = true;
    const CONTROL_FACTS: bool = true;
    type Params = NoParams;
    type Payload = Value;
    fn enabled() -> bool {
        crate::services::connector_status::home_dashboard_enabled()
    }
    fn compute<'a>(mut cx: ComputeCx<'a>, _: &'a NoParams) -> ComputeFuture<'a, Value> {
        Box::pin(async move {
            #[cfg(feature = "e2e")]
            super::note_compute();
            let control = cx.control_tx.as_mut().ok_or("control facts unavailable")?;
            let (inputs, hidden) = status_inputs(
                control,
                cx.tx,
                cx.account_id,
                cx.compute_as,
                cx.graphs,
                cx.now,
            )
            .await?;
            let atoms = crate::services::connector_status::fetch_atom_stats(
                cx.tx,
                cx.account_id,
                cx.compute_as,
                cx.graphs,
                cx.now,
            )
            .await?;
            let visible = |p: &str| !hidden.contains(p);
            Ok(serde_json::to_value(
                crate::services::connector_status::assemble(
                    &inputs,
                    &atoms,
                    Some(&visible),
                    cx.now,
                ),
            )?)
        })
    }
    fn empty(_: &NoParams) -> Value {
        serde_json::json!({"connectors":[],"summary":{"total":0,"healthy":0,"stale":0,"failing":0}})
    }
    fn dirty_triggers() -> &'static [DirtyTrigger] {
        TRIGGERS
    }
}
/// [`status_inputs`]' answer: the inputs and the org-hidden providers.
pub(crate) type StatusInputs = (
    crate::services::connector_status::ControlInputs,
    std::collections::HashSet<String>,
);
/// Every fact the connector state rule reads except the 30-day atom counts:
/// [`status_control_inputs`] then [`merge_content_inputs`]. The stored
/// `connector_status` model adds
/// [`crate::services::connector_status::fetch_atom_stats`] on top; Home's
/// attention read (`services::home_launchpad`) calls the two halves itself,
/// releasing the control transaction before it opens the content one (on the
/// personal plane both come from one pool), and skips the counts, because
/// they only order rows and decide `stale`, never `failing`.
pub(crate) async fn status_inputs(
    control: &mut sqlx::Transaction<'static, Postgres>,
    content: &mut sqlx::PgConnection,
    account_id: Uuid,
    user_id: Uuid,
    graphs: &[Uuid],
    now: DateTime<Utc>,
) -> Result<StatusInputs, Error> {
    let (mut inputs, hidden) = status_control_inputs(control, account_id, user_id).await?;
    merge_content_inputs(content, &mut inputs, account_id, user_id, graphs, now).await?;
    Ok((inputs, hidden))
}
/// The control-plane half of [`status_inputs`]: the caller's connections and
/// sync state, and the providers the org's connector flags hide. Reads only
/// `control`.
pub(crate) async fn status_control_inputs(
    control: &mut sqlx::Transaction<'static, Postgres>,
    account_id: Uuid,
    user_id: Uuid,
) -> Result<StatusInputs, Error> {
    let inputs = crate::services::connector_status::fetch_control_inputs_scoped(
        control, account_id, user_id,
    )
    .await?;
    let hidden = hidden_providers(control, account_id).await?;
    Ok((inputs, hidden))
}
/// The content-plane half of [`status_inputs`]: merges the caller's Composio
/// capture scopes and GitHub repositories over `graphs` into `inputs`. Reads
/// only `content`.
pub(crate) async fn merge_content_inputs(
    content: &mut sqlx::PgConnection,
    inputs: &mut crate::services::connector_status::ControlInputs,
    account_id: Uuid,
    user_id: Uuid,
    graphs: &[Uuid],
    now: DateTime<Utc>,
) -> Result<(), Error> {
    // Capture runtime lives on the content plane for orgs. Resolve the
    // latest caller-owned scope inside the same graph-filtered snapshot.
    let scopes:Vec<(Uuid,String,Option<String>,Option<DateTime<Utc>>)>=sqlx::query_as("SELECT DISTINCT ON(connection_id) connection_id,state,error_code,last_capture_at FROM composio_capture_scopes WHERE account_id=$1 AND owner_user_id=$2 AND graph_id=ANY($3) ORDER BY connection_id,updated_at DESC")
        .bind(account_id).bind(user_id).bind(graphs).fetch_all(&mut *content).await?;
    let scopes: HashMap<_, _> = scopes
        .into_iter()
        .map(|(id, state, error, last)| (id, (state, error, last)))
        .collect();
    for connection in &mut inputs.composio {
        if let Some((state, error, last)) = scopes.get(&connection.connection_id) {
            connection.scope_state = Some(state.clone());
            connection.error_code = error.clone();
            connection.last_capture_at = *last;
        }
    }

    let repos=sqlx::query("SELECT last_synced_at,sync_error_code,sync_error_at FROM github_watched_repos WHERE account_id=$1 AND owner_user_id=$2 AND graph_id=ANY($3) AND active")
    .bind(account_id).bind(user_id).bind(graphs).fetch_all(&mut *content).await?;
    for row in repos {
        let code: Option<String> = row.get("sync_error_code");
        inputs.obs.entry("github".into()).or_default().push(
            crate::services::connector_status::Obs {
                synced_at: row.get("last_synced_at"),
                error_at: code.as_ref().map(|_| {
                    row.get::<Option<DateTime<Utc>>, _>("sync_error_at")
                        .unwrap_or(now)
                }),
                error: code.map(|_| "A repository failed to sync".into()),
                polled: false,
            },
        );
    }
    Ok(())
}
async fn hidden_providers(
    control: &mut sqlx::Transaction<'static, Postgres>,
    account: Uuid,
) -> Result<std::collections::HashSet<String>, Error> {
    let org: Option<String> = sqlx::query_scalar("SELECT clerk_org_id FROM accounts WHERE id=$1")
        .bind(account)
        .fetch_one(&mut **control)
        .await?;
    let Some(org) = org else {
        return Ok(Default::default());
    };
    let overrides: HashMap<String, bool> = sqlx::query_as::<_, (String, bool)>(
        "SELECT provider,enabled FROM org_connector_flags WHERE clerk_org_id=$1",
    )
    .bind(org)
    .fetch_all(&mut **control)
    .await?
    .into_iter()
    .collect();
    let mut hidden = std::collections::HashSet::new();
    for (flag, provider, default) in [
        (
            "slack",
            "slack",
            crate::routes::oauth::org_slack_oauth_enabled(),
        ),
        (
            "google_gmail",
            "google_gmail",
            crate::connectors::google::surface_env_default(
                crate::connectors::google::Surface::Gmail,
            ),
        ),
        (
            "google_chat",
            "google_chat",
            crate::connectors::google::surface_env_default(
                crate::connectors::google::Surface::Chat,
            ),
        ),
        (
            "google_meet",
            "google_meet",
            crate::connectors::google::surface_env_default(
                crate::connectors::google::Surface::Meet,
            ),
        ),
        (
            "google_calendar",
            "google",
            crate::connectors::google::org_calendar_env_default(),
        ),
        (
            "microsoft",
            "microsoft",
            matches!(std::env::var("ORG_MICROSOFT_TEAMS_ENABLED"),Ok(v) if v=="1"||v.eq_ignore_ascii_case("true")),
        ),
        (
            "github",
            "github",
            matches!(std::env::var("ORG_GITHUB_ENABLED"),Ok(v) if v=="1"||v.eq_ignore_ascii_case("true")),
        ),
    ] {
        if !overrides.get(flag).copied().unwrap_or(default) {
            hidden.insert(provider.to_owned());
            if provider == "slack" {
                hidden.insert("slack_user".into());
            }
        }
    }
    Ok(hidden)
}

pub struct TeamsTranscriptCaptures;
impl ReadModel for TeamsTranscriptCaptures {
    const NAME: &'static str = "teams_transcript_captures";
    const PATH: &'static str = "/connectors/microsoft/teams/transcripts/captures";
    const VERSION: i32 = 1;
    const PER_USER: bool = true;
    const CONTROL_FACTS: bool = true;
    type Params = NoParams;
    type Payload = Value;
    fn compute<'a>(mut cx: ComputeCx<'a>, _: &'a NoParams) -> ComputeFuture<'a, Value> {
        Box::pin(async move {
            #[cfg(feature = "e2e")]
            super::note_compute();
            let control = cx.control_tx.as_mut().ok_or("control facts unavailable")?;
            let identity = transcript_identity(control, cx.account_id, cx.compute_as).await?;
            let Some((tid, aad)) = identity else {
                return Ok(Self::empty(&NoParams {}));
            };
            let rows:Vec<(Uuid,String,DateTime<Utc>)>=sqlx::query_as("SELECT c.id,c.meeting_id,c.created_at FROM teams_meeting_captures c JOIN graphs g ON g.id=c.graph_id WHERE c.account_id=$1 AND c.user_id=$2 AND c.tenant_id=$3 AND c.aad_id=$4 AND c.graph_id=ANY($5) AND c.revoked_at IS NULL AND g.account_id=$1 AND g.archived_at IS NULL AND ((g.kind='member' AND g.owner_user_id=$2) OR (g.kind='personal' AND g.owner_user_id IS NULL)) AND (c.source<>'meeting_bot' OR EXISTS(SELECT 1 FROM teams_meeting_installations i WHERE i.account_id=c.account_id AND i.tenant_id=c.tenant_id AND i.installation_id=c.installation_id AND i.active))")
            .bind(cx.account_id).bind(cx.compute_as).bind(tid).bind(aad).bind(cx.graphs).fetch_all(&mut **cx.tx).await?;
            let captures:Vec<Value>=rows.into_iter().map(|(capture_id,meeting_id,created_at)|serde_json::json!({"capture_id":capture_id,"meeting_id":meeting_id,"created_at":created_at,"status":"active"})).collect();
            Ok(serde_json::json!({"captures":captures}))
        })
    }
    fn empty(_: &NoParams) -> Value {
        serde_json::json!({"captures":[]})
    }
    fn dirty_triggers() -> &'static [DirtyTrigger] {
        TRIGGERS
    }
}
async fn transcript_identity(
    control: &mut sqlx::Transaction<'static, Postgres>,
    account: Uuid,
    user: Uuid,
) -> Result<Option<(String, String)>, Error> {
    let row:Option<(Option<String>,Uuid,bool)>=sqlx::query_as("SELECT a.clerk_org_id,a.owner_user_id,u.deleted_at IS NOT NULL FROM accounts a,users u WHERE a.id=$1 AND u.id=$2")
        .bind(account).bind(user).fetch_optional(&mut **control).await?;
    let Some((org, owner, false)) = row else {
        return Ok(None);
    };
    if org.is_none() && owner != user {
        return Ok(None);
    }
    let grants:Vec<(Option<String>,Option<String>,bool)>=sqlx::query_as("SELECT provider_workspace_id,provider_user_id,credential_invalid_at IS NOT NULL FROM oauth_tokens WHERE account_id=$1 AND provider='microsoft' AND (($3 AND owner_user_id=$2) OR (NOT $3 AND owner_user_id IS NULL))")
        .bind(account).bind(user).bind(org.is_some()).fetch_all(&mut **control).await?;
    match grants.as_slice() {
        [(Some(tid), Some(aad), false)]
            if org.is_some()
                || (Uuid::parse_str(tid).is_ok()
                    && Uuid::parse_str(aad).is_ok()
                    && tid != "9188040d-6c67-4c5b-b112-36a304b66dad") =>
        {
            Ok(Some((tid.clone(), aad.clone())))
        }
        _ => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn connector_params_reject_actor_and_path_conflicts() {
        assert!(NoParams::parse(
            BTreeMap::new(),
            [("user_id".into(), Uuid::nil().to_string())].into()
        )
        .is_err());
        assert!(DetailParams::parse(
            [("provider".into(), "slack".into())].into(),
            [("provider".into(), "google".into())].into()
        )
        .is_err());
        assert!(DetailParams::parse(
            [("provider".into(), "unknown".into())].into(),
            BTreeMap::new()
        )
        .is_err());
        assert_eq!(
            DetailParams::parse(
                [("provider".into(), "slack".into())].into(),
                BTreeMap::new()
            )
            .unwrap()
            .provider,
            "slack"
        );
    }
}
