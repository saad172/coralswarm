//! Per-huddle channel-visible consent. Contains routing/attendance metadata only;
//! canvas content is fetched only after a current, authorized confirmation.
use crate::routes::slack::AccountPlane;
use crate::tenancy::scope::begin_tenant_system_scoped;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row};
use std::collections::BTreeSet;
use uuid::Uuid;

pub(crate) const ACTION_ID: &str = "cs_huddle_confirm";
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct CaptureRequest {
    pub account_id: Uuid,
    pub workspace_id: String,
    pub channel_id: String,
    pub huddle_thread_ts: String,
    pub canvas_file_id: String,
    pub participant_history: Vec<String>,
    pub date_start: Option<i64>,
    pub date_end: Option<i64>,
}
impl CaptureRequest {
    fn job(&self) -> crate::jobs::JobKind {
        crate::jobs::JobKind::SlackHuddleCapture {
            account_id: self.account_id,
            workspace_id: self.workspace_id.clone(),
            channel_id: self.channel_id.clone(),
            huddle_thread_ts: self.huddle_thread_ts.clone(),
            canvas_file_id: self.canvas_file_id.clone(),
            participant_history: self.participant_history.clone(),
            date_start: self.date_start,
            date_end: self.date_end,
        }
    }
}
fn actor_id(s: &str) -> bool {
    let mut chars = s.chars();
    matches!(chars.next(), Some('U' | 'W' | 'B'))
        && (7..=21).contains(&s.len())
        && chars.all(|c| c.is_ascii_uppercase() || c.is_ascii_digit())
}
/// Missing, malformed or over-budget attendance is unknown, never zero.
pub(crate) fn authoritative_attendance(room: &Value) -> Result<Vec<String>, String> {
    let ids = room
        .get("participant_history")
        .and_then(Value::as_array)
        .ok_or("authoritative huddle attendance unavailable")?;
    if ids.is_empty() || ids.len() > 500 {
        return Err("authoritative huddle attendance unavailable or exceeds budget".into());
    }
    ids.iter()
        .map(|v| {
            v.as_str()
                .filter(|s| actor_id(s))
                .map(str::to_owned)
                .ok_or_else(|| "invalid authoritative huddle attendee".into())
        })
        .collect()
}
fn audience(roster: &[String], attendees: &[String]) -> Result<usize, String> {
    if roster.is_empty() || attendees.is_empty() || roster.iter().any(|s| !actor_id(s)) {
        return Err("complete channel roster and attendance required".into());
    }
    let roster: BTreeSet<_> = roster.iter().collect();
    let attendees: BTreeSet<_> = attendees.iter().collect();
    Ok(roster.difference(&attendees).count())
}
async fn slack_json(
    http: &reqwest::Client,
    token: &str,
    method: &'static str,
    query: &[(&str, &str)],
) -> Result<Value, String> {
    let response = http
        .get(format!("{}/{}", super::slack::slack_api_base(), method))
        .bearer_auth(token)
        .query(query)
        .timeout(std::time::Duration::from_secs(25))
        .send()
        .await
        .map_err(|_| "Slack request unavailable")?;
    if !response.status().is_success() {
        return Err("Slack request unavailable".into());
    }
    let json = super::slack::read_capped_json(response, method).await?;
    if json.get("ok").and_then(Value::as_bool) != Some(true) {
        return Err("Slack API refused request".into());
    }
    Ok(json)
}
/// OAuth proves the personal owner's Slack identity without comparing emails.
/// Return a fresh account+workspace-specific non-revoked account-wide grant.
async fn personal_owner_proof(
    control: &PgPool,
    request: &CaptureRequest,
) -> Result<Option<(Uuid, Uuid, String, chrono::DateTime<chrono::Utc>)>, String> {
    let mut tx = begin_tenant_system_scoped(control, request.account_id)
        .await
        .map_err(|e| e.to_string())?;
    let proof:Option<(Uuid,Uuid,String,chrono::DateTime<chrono::Utc>)>=sqlx::query_as("SELECT ot.id,a.owner_user_id,ot.provider_user_id,ot.updated_at \
        FROM oauth_tokens ot JOIN accounts a ON a.id=ot.account_id JOIN users u ON u.id=a.owner_user_id \
        JOIN account_members am ON am.account_id=a.id AND am.user_id=a.owner_user_id \
        WHERE ot.account_id=$1 AND ot.provider='slack' AND ot.provider_workspace_id=$2 \
        AND ot.owner_user_id IS NULL AND a.clerk_org_id IS NULL AND u.deleted_at IS NULL \
        AND ot.credential_invalid_at IS NULL AND (ot.expires_at IS NULL OR ot.expires_at>now()) \
        AND ot.provider_user_id IS NOT NULL AND octet_length(ot.access_token)>0")
        .bind(request.account_id).bind(&request.workspace_id).fetch_optional(&mut *tx).await.map_err(|e|e.to_string())?;
    tx.rollback().await.map_err(|e| e.to_string())?;
    Ok(proof.filter(|(_, _, actor, _)| actor_id(actor)))
}
#[derive(Debug)]
struct Snapshot {
    fingerprint: String,
    non_attendees: usize,
    exposed_non_attendees: usize,
    graphs: Vec<Uuid>,
    attendees: Vec<String>,
    destination: String,
    plane: AccountPlane,
}
/// Always re-read the authoritative room, entire roster and effective destination.
async fn snapshot(
    control: &PgPool,
    pool: &PgPool,
    http: &reqwest::Client,
    token: &str,
    request: &CaptureRequest,
) -> Result<Snapshot, String> {
    let roster = super::slack::fetch_conversation_members(http, token, &request.channel_id).await?;
    let root = slack_json(
        http,
        token,
        "conversations.history",
        &[
            ("channel", &request.channel_id),
            ("oldest", &request.huddle_thread_ts),
            ("latest", &request.huddle_thread_ts),
            ("inclusive", "true"),
            ("limit", "1"),
        ],
    )
    .await?;
    let message = root
        .get("messages")
        .and_then(Value::as_array)
        .and_then(|a| {
            a.iter()
                .find(|m| m.get("ts").and_then(Value::as_str) == Some(&request.huddle_thread_ts))
        })
        .ok_or("authoritative huddle root unavailable")?;
    let room = message
        .get("room")
        .ok_or("authoritative huddle room unavailable")?;
    if room.get("transcript_file_id").and_then(Value::as_str) != Some(&request.canvas_file_id) {
        return Err("huddle artifact changed; waiting for current Slack event".into());
    }
    let attendees = authoritative_attendance(room)?;
    let non_attendees = audience(&roster, &attendees)?;
    let plane = crate::routes::slack::account_plane(control, request.account_id)
        .await
        .map_err(|e| e.to_string())?;
    let destination = super::slack::resolve_channel_destinations(
        control,
        pool,
        request.account_id,
        plane,
        &request.workspace_id,
        &request.channel_id,
    )
    .await
    .map_err(|e| e.to_string())?;
    let mut graphs = match destination {
        super::slack::ChannelDestination::Single(graph) => vec![graph],
        super::slack::ChannelDestination::FanOut {
            graphs,
            unmapped_members,
            ..
        } => {
            if unmapped_members > 0 {
                return Err("destination audience is incomplete".into());
            }
            graphs
        }
    };
    graphs.sort();
    graphs.dedup();
    if graphs.is_empty() {
        // Fail closed: no prompt is posted and nothing is captured. Alertable so
        // an operator can see a channel whose huddles are never disclosed.
        tracing::warn!(
            account_id = %request.account_id,
            channel_id = %request.channel_id,
            connector_health = true,
            "huddle consent: resolved destination has no graphs — refusing, nothing captured"
        );
        return Err("huddle destination unavailable".into());
    }
    let mut tx = begin_tenant_system_scoped(pool, request.account_id)
        .await
        .map_err(|e| e.to_string())?;
    // Include current graph owners and team seats in the fingerprint: routing
    // can remain stable while destination access changes.
    let destinations: Vec<(Uuid, String, Option<Uuid>, String)> = sqlx::query_as(
        "SELECT id, name, owner_user_id, kind FROM graphs WHERE account_id=$1 AND id=ANY($2) ORDER BY id",
    )
    .bind(request.account_id)
    .bind(&graphs)
    .fetch_all(&mut *tx)
    .await
    .map_err(|e| e.to_string())?;
    if destinations.len() != graphs.len() {
        return Err("destination graph unavailable".into());
    }
    let seats: Vec<(Uuid, Uuid)> = sqlx::query_as(
        "SELECT t.graph_id, tm.user_id FROM teams t JOIN team_members tm ON tm.team_id=t.id \
         WHERE t.account_id=$1 AND t.graph_id=ANY($2) AND tm.departing_at IS NULL ORDER BY t.graph_id,tm.user_id")
        .bind(request.account_id).bind(&graphs).fetch_all(&mut *tx).await.map_err(|e| e.to_string())?;
    let personal_proof = if matches!(plane, AccountPlane::Personal) {
        Some(
            personal_owner_proof(control, request)
                .await?
                .ok_or("personal Slack installer proof unavailable")?,
        )
    } else {
        None
    };
    let org_members: Vec<Uuid> = if destinations.iter().any(|(_, _, _, kind)| {
        kind == "org" || (kind == "personal" && matches!(plane, AccountPlane::Org))
    }) {
        sqlx::query_scalar(
            "SELECT am.user_id FROM account_members am JOIN users u ON u.id=am.user_id \
            WHERE am.account_id=$1 AND u.deleted_at IS NULL ORDER BY am.user_id",
        )
        .bind(request.account_id)
        .fetch_all(control)
        .await
        .map_err(|e| e.to_string())?
    } else {
        vec![]
    };
    let mut readers = BTreeSet::new();
    for (graph, _, owner, kind) in &destinations {
        match kind.as_str() {
            "personal" if matches!(plane, AccountPlane::Personal) => {
                readers.insert(personal_proof.as_ref().unwrap().1);
            }
            "member" => {
                readers.insert(owner.ok_or("member destination owner unavailable")?);
            }
            "team" => {
                readers.extend(
                    seats
                        .iter()
                        .filter(|(g, _)| g == graph)
                        .map(|(_, user)| *user),
                );
            }
            "personal" | "org" => {
                readers.extend(org_members.iter().copied());
            }
            _ => return Err("destination audience cannot be determined".into()),
        }
    }
    let current_readers: Vec<Uuid> = sqlx::query_scalar(
        "SELECT am.user_id FROM account_members am JOIN users u ON u.id=am.user_id \
        WHERE am.account_id=$1 AND am.user_id=ANY($2) AND u.deleted_at IS NULL ORDER BY am.user_id",
    )
    .bind(request.account_id)
    .bind(readers.iter().copied().collect::<Vec<_>>())
    .fetch_all(control)
    .await
    .map_err(|e| e.to_string())?;
    if current_readers.len() != readers.len() {
        return Err("destination reader membership cannot be determined".into());
    }
    let mut identity_links:Vec<(Uuid,String,chrono::DateTime<chrono::Utc>)>=sqlx::query_as("SELECT user_id,external_user_id,link_verified_at FROM connector_identities \
        WHERE provider='slack' AND workspace_id=$1 AND user_id=ANY($2) AND link_method='verified_email' \
        AND link_verified_at IS NOT NULL ORDER BY user_id,external_user_id")
        .bind(&request.workspace_id).bind(readers.iter().copied().collect::<Vec<_>>())
        .fetch_all(&mut *tx).await.map_err(|e|e.to_string())?;
    tx.rollback().await.map_err(|e| e.to_string())?;
    // Personal owner identity comes only from OAuth, even when an email
    // directory contains a conflicting or missing personal-owner mapping.
    if let Some((_, owner, actor, _)) = &personal_proof {
        identity_links.retain(|(user, _, _)| user != owner);
        identity_links.push((*owner, actor.clone(), personal_proof.as_ref().unwrap().3));
        identity_links.sort();
    }
    let attendance: BTreeSet<_> = attendees.iter().collect();
    let mut exposed_non_attendees = 0;
    for reader in &readers {
        let aliases: Vec<_> = identity_links
            .iter()
            .filter(|(user, _, _)| user == reader)
            .map(|(_, actor, _)| actor)
            .collect();
        if aliases.is_empty() || aliases.iter().any(|id| !actor_id(id)) {
            return Err(
                "destination reader attendance identity unavailable; refusing exposure disclosure"
                    .into(),
            );
        }
        if !aliases.iter().any(|actor| attendance.contains(actor)) {
            exposed_non_attendees += 1;
        }
    }
    let roster: BTreeSet<_> = roster.iter().collect();
    let material = json!([
        request.account_id,
        request.workspace_id,
        request.channel_id,
        request.huddle_thread_ts,
        request.canvas_file_id,
        roster,
        attendance,
        destinations,
        seats,
        org_members,
        readers,
        identity_links,
        personal_proof
    ]);
    let fingerprint = hex::encode(Sha256::digest(material.to_string().as_bytes()));
    let destination = if destinations.len() > 1
        && destinations
            .iter()
            .all(|(_, _, _, kind)| matches!(kind.as_str(), "personal" | "member"))
    {
        format!("{} private member oceans", destinations.len())
    } else {
        destinations
            .iter()
            .map(|(graph, name, _, kind)| match kind.as_str() {
                "personal" if matches!(plane, AccountPlane::Personal) => {
                    format!("{name} (private ocean for its account owner)")
                }
                "member" => format!("{name} (private ocean for its owner)"),
                "personal" => format!(
                    "{name} (shared account ocean; {} account members)",
                    org_members.len()
                ),
                "team" => format!(
                    "{name} (team ocean; {} active members)",
                    seats.iter().filter(|(g, _)| g == graph).count()
                ),
                "org" => format!(
                    "{name} (organization ocean; {} account members)",
                    org_members.len()
                ),
                _ => format!("{name} ({kind} ocean)"),
            })
            .collect::<Vec<_>>()
            .join(", ")
    };
    Ok(Snapshot {
        fingerprint,
        non_attendees,
        exposed_non_attendees,
        graphs,
        attendees,
        destination,
        plane,
    })
}
async fn authorized(
    control: &PgPool,
    pool: &PgPool,
    http: &reqwest::Client,
    token: &str,
    request: &CaptureRequest,
    snapshot: &Snapshot,
    slack_actor: &str,
) -> Result<Option<Uuid>, String> {
    if matches!(snapshot.plane, AccountPlane::Personal) {
        return Ok(personal_owner_proof(control, request)
            .await?
            .filter(|(_, _, actor, _)| actor == slack_actor)
            .map(|(_, owner, _, _)| owner));
    }
    // Read the verified directory afresh. The generic matcher has an
    // in-process cache, which cannot establish that a confirmation's actor
    // is still linked after identity removal or reassignment.
    let _ = (http, token);
    let mut tx = begin_tenant_system_scoped(pool, request.account_id)
        .await
        .map_err(|e| e.to_string())?;
    let user: Option<Uuid> = sqlx::query_scalar(
        "SELECT user_id FROM connector_identities WHERE provider='slack' \
        AND workspace_id=$1 AND external_user_id=$2 AND user_id IS NOT NULL \
        AND link_method='verified_email' AND link_verified_at IS NOT NULL",
    )
    .bind(&request.workspace_id)
    .bind(slack_actor)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| e.to_string())?;
    tx.rollback().await.map_err(|e| e.to_string())?;
    let Some(user) = user else {
        return Ok(None);
    };
    let member: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM account_members am JOIN users u ON u.id=am.user_id \
        WHERE am.account_id=$1 AND am.user_id=$2 AND u.deleted_at IS NULL)",
    )
    .bind(request.account_id)
    .bind(user)
    .fetch_one(control)
    .await
    .map_err(|e| e.to_string())?;
    if !member {
        return Ok(None);
    }
    let mut tx = begin_tenant_system_scoped(pool, request.account_id)
        .await
        .map_err(|e| e.to_string())?;
    let teams: Vec<Uuid> =
        sqlx::query_scalar("SELECT id FROM teams WHERE account_id=$1 AND graph_id=ANY($2)")
            .bind(request.account_id)
            .bind(&snapshot.graphs)
            .fetch_all(&mut *tx)
            .await
            .map_err(|e| e.to_string())?;
    tx.rollback().await.map_err(|e| e.to_string())?;
    if teams.len() == snapshot.graphs.len() {
        for team in teams {
            if !matches!(
                crate::routes::slack_interactivity::check_team_ocean_admin(
                    pool,
                    request.account_id,
                    Some(user),
                    team
                )
                .await
                .map_err(|e| e.to_string())?,
                crate::routes::slack_interactivity::TeamOceanCheck::Admin { .. }
            ) {
                return Ok(None);
            }
        }
        Ok(Some(user))
    } else {
        let admin:bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM account_members WHERE account_id=$1 AND user_id=$2 AND role IN ('owner','admin'))")
            .bind(request.account_id).bind(user).fetch_one(control).await.map_err(|e| e.to_string())?;
        Ok(admin.then_some(user))
    }
}
/// Returns fresh authoritative attendees only after the current prompt is confirmed.
/// Posting failures and pending consent return errors; callers never write the terminal ledger.
pub(crate) async fn require_consent(
    control: &PgPool,
    pool: &PgPool,
    http: &reqwest::Client,
    token: &str,
    request: &CaptureRequest,
    expected_graphs: &[Uuid],
) -> Result<Vec<String>, String> {
    let current = snapshot(control, pool, http, token, request).await?;
    let mut expected = expected_graphs.to_vec();
    expected.sort();
    expected.dedup();
    if current.graphs != expected {
        return Err("destination changed during capture".into());
    }
    let mut tx = begin_tenant_system_scoped(pool, request.account_id)
        .await
        .map_err(|e| e.to_string())?;
    let lock_key = format!(
        "huddle:{}:{}:{}",
        request.account_id, request.channel_id, request.huddle_thread_ts
    );
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1,0))")
        .bind(&lock_key)
        .execute(&mut *tx)
        .await
        .map_err(|e| e.to_string())?;
    let row = sqlx::query("SELECT id, fingerprint, prompt_ts, confirmed_slack_user_id, confirmed_user_id FROM slack_huddle_consents \
        WHERE account_id=$1 AND workspace_id=$2 AND channel_id=$3 AND huddle_thread_ts=$4 FOR UPDATE")
        .bind(request.account_id).bind(&request.workspace_id).bind(&request.channel_id).bind(&request.huddle_thread_ts)
        .fetch_optional(&mut *tx).await.map_err(|e| e.to_string())?;
    if let Some(row) = &row {
        if row.get::<String, _>("fingerprint") == current.fingerprint
            && row.get::<Option<String>, _>("prompt_ts").is_some()
        {
            if let Some(actor) = row.get::<Option<String>, _>("confirmed_slack_user_id") {
                tx.rollback().await.map_err(|e| e.to_string())?;
                let current_actor =
                    authorized(control, pool, http, token, request, &current, &actor).await?;
                if current_actor.is_some()
                    && current_actor == row.get::<Option<Uuid>, _>("confirmed_user_id")
                {
                    return Ok(current.attendees);
                }
                // Actor lost authority: invalidate this version; the next attempt
                // posts a fresh prompt instead of trusting the old approval.
                let mut tx = begin_tenant_system_scoped(pool, request.account_id)
                    .await
                    .map_err(|e| e.to_string())?;
                sqlx::query("UPDATE slack_huddle_consents SET id=gen_random_uuid(), prompt_ts=NULL, confirmed_slack_user_id=NULL,confirmed_user_id=NULL \
                    WHERE account_id=$1 AND id=$2 AND fingerprint=$3 AND confirmed_slack_user_id=$4")
                    .bind(request.account_id).bind(row.get::<Uuid,_>("id")).bind(&current.fingerprint).bind(actor)
                    .execute(&mut *tx).await.map_err(|e| e.to_string())?;
                tx.commit().await.map_err(|e| e.to_string())?;
                return Err("huddle confirmation actor no longer authorized".into());
            }
            return Err("waiting for huddle audience confirmation".into());
        }
    }
    // New opaque ID for every changed audience/destination/actor; stale buttons
    // cannot confirm a later prompt even if a fingerprint returns to its old value.
    let id = row
        .as_ref()
        .filter(|row| row.get::<String, _>("fingerprint") == current.fingerprint)
        .map(|row| row.get::<Uuid, _>("id"))
        .unwrap_or_else(Uuid::new_v4);
    // Persist the opaque ID before posting, so uncertain transport/commit
    // failures retry with the same Slack client_msg_id.
    sqlx::query("INSERT INTO slack_huddle_consents (id,account_id,workspace_id,channel_id,huddle_thread_ts,fingerprint,non_attendee_count,capture_payload) \
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT (account_id,workspace_id,channel_id,huddle_thread_ts) DO UPDATE \
        SET id=EXCLUDED.id,fingerprint=EXCLUDED.fingerprint,non_attendee_count=EXCLUDED.non_attendee_count, \
        capture_payload=EXCLUDED.capture_payload,prompt_ts=NULL,confirmed_slack_user_id=NULL,confirmed_user_id=NULL,confirmed_at=NULL")
        .bind(id).bind(request.account_id).bind(&request.workspace_id).bind(&request.channel_id).bind(&request.huddle_thread_ts)
        .bind(&current.fingerprint).bind(current.non_attendees as i32).bind(json!(request))
        .execute(&mut *tx).await.map_err(|e| e.to_string())?;
    tx.commit().await.map_err(|e| e.to_string())?;
    let mut tx = begin_tenant_system_scoped(pool, request.account_id)
        .await
        .map_err(|e| e.to_string())?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1,0))")
        .bind(&lock_key)
        .execute(&mut *tx)
        .await
        .map_err(|e| e.to_string())?;
    let row = sqlx::query(
        "SELECT id,prompt_ts FROM slack_huddle_consents WHERE account_id=$1 \
        AND workspace_id=$2 AND channel_id=$3 AND huddle_thread_ts=$4 FOR UPDATE",
    )
    .bind(request.account_id)
    .bind(&request.workspace_id)
    .bind(&request.channel_id)
    .bind(&request.huddle_thread_ts)
    .fetch_one(&mut *tx)
    .await
    .map_err(|e| e.to_string())?;
    if row.get::<Uuid, _>("id") != id {
        return Err("huddle audience changed while posting".into());
    }
    if row.get::<Option<String>, _>("prompt_ts").is_some() {
        return Err("waiting for huddle audience confirmation".into());
    }
    let text = format!("{} channel members did not attend this huddle. Notes will be saved to {}. {} non-attendees will gain access in these destinations. Confirm this huddle before its notes are saved.", current.non_attendees, current.destination,current.exposed_non_attendees);
    let value = json!({"account_id": request.account_id, "consent_id": id}).to_string();
    let response = http.post(format!("{}/chat.postMessage", super::slack::slack_api_base()))
        .bearer_auth(token).timeout(std::time::Duration::from_secs(25)).json(&json!({
            "channel":request.channel_id,"thread_ts":request.huddle_thread_ts,"text":text,"client_msg_id":id,
            "blocks":[{"type":"section","text":{"type":"plain_text","text":text}},
                {"type":"actions","block_id":format!("cs_account:{}",request.account_id),"elements":[{
                    "type":"button","action_id":ACTION_ID,"text":{"type":"plain_text","text":"Confirm this huddle"},"value":value}]}]
        })).send().await.map_err(|_| "huddle disclosure post unavailable")?;
    if !response.status().is_success() {
        return Err("huddle disclosure post unavailable".into());
    }
    let body = super::slack::read_capped_json(response, "chat.postMessage").await?;
    if body.get("ok").and_then(Value::as_bool) != Some(true)
        || body.get("channel").and_then(Value::as_str) != Some(&request.channel_id)
    {
        return Err("huddle disclosure was not posted".into());
    }
    let ts = body
        .get("ts")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or("huddle disclosure has no timestamp")?;
    sqlx::query("UPDATE slack_huddle_consents SET prompt_ts=$3 WHERE account_id=$1 AND id=$2")
        .bind(request.account_id)
        .bind(id)
        .bind(ts)
        .execute(&mut *tx)
        .await
        .map_err(|e| e.to_string())?;
    tx.commit().await.map_err(|e| e.to_string())?;
    Err("waiting for huddle audience confirmation".into())
}
/// Signed-interaction dispatcher passes account/workspace/channel/message context;
/// no client-supplied destination, audience count, or attendee list is trusted.
pub(crate) async fn confirm(
    control: &PgPool,
    pool: &PgPool,
    http: &reqwest::Client,
    token: &str,
    account: Uuid,
    workspace: &str,
    channel: &str,
    message_ts: &str,
    actor: &str,
    id: Uuid,
) -> Result<(), String> {
    let mut tx = begin_tenant_system_scoped(pool, account)
        .await
        .map_err(|e| e.to_string())?;
    let row = sqlx::query("SELECT capture_payload,fingerprint,prompt_ts FROM slack_huddle_consents WHERE account_id=$1 AND id=$2 \
        AND workspace_id=$3 AND channel_id=$4")
        .bind(account).bind(id).bind(workspace).bind(channel).fetch_optional(&mut *tx).await.map_err(|e| e.to_string())?
        .ok_or("unknown huddle confirmation")?;
    tx.rollback().await.map_err(|e| e.to_string())?;
    if row.get::<Option<String>, _>("prompt_ts").as_deref() != Some(message_ts) {
        return Err("huddle confirmation message mismatch".into());
    }
    let request: CaptureRequest = serde_json::from_value(row.get("capture_payload"))
        .map_err(|_| "invalid huddle confirmation metadata")?;
    let current = snapshot(control, pool, http, token, &request).await?;
    if current.fingerprint != row.get::<String, _>("fingerprint") {
        // Post the replacement before allowing any capture. This invalidates
        // the old opaque ID; require_consent deliberately returns pending.
        let _ = require_consent(control, pool, http, token, &request, &current.graphs).await;
        return Err("huddle audience changed; confirm the new disclosure".into());
    }
    // Organization actors use verified email; personal actors use the
    // fresh first-party OAuth installer proof and never require email scopes.
    if matches!(current.plane, AccountPlane::Org) {
        let _ = super::slack::resolve_verified_slack_identity(
            control, pool, http, account, workspace, actor, token,
        )
        .await;
    }
    let current_actor = authorized(control, pool, http, token, &request, &current, actor)
        .await?
        .ok_or("huddle confirmation actor unauthorized")?;
    // Make the acknowledgement visible in the original huddle-thread
    // message before a worker can ingest. Keep its button for safe retries
    // if the later atomic database commit fails.
    let text = format!("Huddle confirmation received. {} channel members did not attend. Notes will be saved to {}. {} non-attendees will gain access in these destinations after checking the current audience and permissions again.",current.non_attendees,current.destination,current.exposed_non_attendees);
    let value = json!({"account_id":account,"consent_id":id}).to_string();
    let response=http.post(format!("{}/chat.update",super::slack::slack_api_base()))
        .bearer_auth(token).timeout(std::time::Duration::from_secs(25)).json(&json!({
            "channel":channel,"ts":message_ts,"text":text,
            "blocks":[{"type":"section","text":{"type":"plain_text","text":text}},
                {"type":"actions","block_id":format!("cs_account:{account}"),"elements":[{
                    "type":"button","action_id":ACTION_ID,"text":{"type":"plain_text","text":"Confirm this huddle"},"value":value}]}]
        })).send().await.map_err(|_|"huddle visible acknowledgement unavailable")?;
    if !response.status().is_success() {
        return Err("huddle visible acknowledgement unavailable".into());
    }
    let acknowledgement = super::slack::read_capped_json(response, "chat.update").await?;
    if acknowledgement.get("ok").and_then(Value::as_bool) != Some(true)
        || acknowledgement.get("channel").and_then(Value::as_str) != Some(channel)
        || acknowledgement.get("ts").and_then(Value::as_str) != Some(message_ts)
    {
        return Err("huddle visible acknowledgement was not posted".into());
    }
    let mut tx = begin_tenant_system_scoped(pool, account)
        .await
        .map_err(|e| e.to_string())?;
    let changed = sqlx::query(
        "UPDATE slack_huddle_consents SET confirmed_slack_user_id=$5,confirmed_user_id=$6,confirmed_at=now() \
        WHERE account_id=$1 AND id=$2 AND fingerprint=$3 AND prompt_ts=$4",
    )
    .bind(account)
    .bind(id)
    .bind(&current.fingerprint)
    .bind(message_ts)
    .bind(actor)
    .bind(current_actor)
    .execute(&mut *tx)
    .await
    .map_err(|e| e.to_string())?
    .rows_affected();
    if changed != 1 {
        return Err("stale huddle confirmation".into());
    }
    // Approval and durable continuation are atomic. Replays are deduplicated
    // while pending; the capture ledger covers a replay after completion.
    let key = format!("huddle-consent:{account}:{id}");
    crate::jobs::queue::enqueue_tx(&mut tx, &request.job(), Some(account), Some(&key), 0)
        .await
        .map_err(|e| e.to_string())?;
    tx.commit().await.map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn huddle_consent_counts_sets_and_outside_attendees() {
        let roster: Vec<String> = (0..10).map(|n| format!("U{n:010}")).collect();
        assert_eq!(audience(&roster, &roster[..3]).unwrap(), 7);
        let mut duplicate = roster.clone();
        duplicate.push(roster[0].clone());
        let attendees = vec![
            roster[0].clone(),
            roster[0].clone(),
            roster[1].clone(),
            roster[2].clone(),
            "U9999999999".into(),
        ];
        assert_eq!(audience(&duplicate, &attendees).unwrap(), 7);
    }
    #[test]
    fn huddle_consent_refuses_unknown_and_partial_attendance() {
        for room in [
            json!({}),
            json!({"participant_history":[]}),
            json!({"participant_history":["U0000000001",null]}),
            json!({"participant_history":vec!["U0000000001";501]}),
        ] {
            assert!(authoritative_attendance(&room).is_err());
        }
        assert!(audience(&[], &["U0000000001".into()]).is_err());
        assert!(audience(&["U0000000001".into()], &[]).is_err());
    }
}

#[cfg(test)]
mod pg_tests {
    use super::*;
    use axum::{
        extract::{Query, State},
        routing::{get, post},
        Json, Router,
    };
    use sqlx::{Connection, Executor, PgConnection};
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    };
    #[derive(Clone)]
    struct MockSlack {
        roster: Arc<Mutex<Value>>,
        room: Arc<Mutex<Value>>,
        posts: Arc<Mutex<Vec<Value>>>,
        post_ok: Arc<Mutex<bool>>,
        files: Arc<AtomicUsize>,
        users: Arc<AtomicUsize>,
    }
    async fn members(
        State(state): State<MockSlack>,
        Query(query): Query<std::collections::HashMap<String, String>>,
    ) -> Json<Value> {
        let roster = state.roster.lock().unwrap().clone();
        if !roster.is_array() {
            return Json(roster);
        }
        let ids = roster.as_array().unwrap();
        if query.get("cursor").map(String::as_str) == Some("second") {
            Json(json!({"ok":true,"members":ids[3..],"response_metadata":{"next_cursor":""}}))
        } else {
            Json(json!({"ok":true,"members":ids[..3],"response_metadata":{"next_cursor":"second"}}))
        }
    }
    async fn history(State(state): State<MockSlack>) -> Json<Value> {
        Json(json!({"ok":true,"messages":[{"ts":"1.1","room":state.room.lock().unwrap().clone()}]}))
    }
    async fn posting(State(state): State<MockSlack>, Json(body): Json<Value>) -> Json<Value> {
        state.posts.lock().unwrap().push(body);
        if !*state.post_ok.lock().unwrap() {
            return Json(json!({"ok":false,"error":"ratelimited"}));
        }
        Json(
            json!({"ok":true,"channel":"C000000001","ts":format!("2.{}",state.posts.lock().unwrap().len())}),
        )
    }
    async fn updating(State(state): State<MockSlack>, Json(body): Json<Value>) -> Json<Value> {
        if !*state.post_ok.lock().unwrap() {
            return Json(json!({"ok":false,"error":"ratelimited"}));
        }
        Json(json!({"ok":true,"channel":body["channel"],"ts":body["ts"]}))
    }
    async fn users(State(state): State<MockSlack>) -> Json<Value> {
        state.users.fetch_add(1, Ordering::SeqCst);
        Json(
            json!({"ok":true,"user":{"id":"U000000000","deleted":false,"is_bot":false,"profile":{"email":"owner@work.example"}}}),
        )
    }
    async fn files(State(state): State<MockSlack>) -> Json<Value> {
        state.files.fetch_add(1, Ordering::SeqCst);
        Json(json!({"ok":false,"error":"internal_error"}))
    }
    struct KekGuard(Option<String>);
    impl Drop for KekGuard {
        fn drop(&mut self) {
            match &self.0 {
                Some(value) => unsafe { std::env::set_var("MASTER_KEK_HEX", value) },
                None => unsafe { std::env::remove_var("MASTER_KEK_HEX") },
            }
        }
    }
    async fn prompt(pool: &PgPool, account: Uuid) -> (Uuid, String) {
        sqlx::query_as("SELECT id,prompt_ts FROM slack_huddle_consents WHERE account_id=$1")
            .bind(account)
            .fetch_one(pool)
            .await
            .unwrap()
    }
    async fn execute_capture(
        pool: &PgPool,
        request: &CaptureRequest,
        http: &reqwest::Client,
    ) -> Uuid {
        let job =
            crate::jobs::queue::enqueue(pool, &request.job(), Some(request.account_id), None, 0)
                .await
                .unwrap()
                .unwrap();
        let jobs = crate::jobs::queue::dequeue(pool, &["SlackHuddleCapture"], 20)
            .await
            .unwrap();
        for row in jobs {
            crate::workers::huddle::process_job(
                pool,
                &crate::tenancy::TenantId::Consumer,
                pool,
                http,
                None,
                row,
            )
            .await;
        }
        job
    }
    #[tokio::test]
    async fn huddle_consent_prompt_ack_capture_fail_closed_pg() {
        let Ok(base) = std::env::var("ORGT_TEST_BASE_URL") else {
            eprintln!("set ORGT_TEST_BASE_URL for huddle consent PG evidence");
            return;
        };
        let _slack_lock = super::super::slack::SLACK_API_BASE_ENV_LOCK.lock().unwrap();
        let _kek_lock = crate::workers::reembed::MASTER_KEK_HEX_ENV_LOCK
            .lock()
            .await;
        let _kek = KekGuard(std::env::var("MASTER_KEK_HEX").ok());
        unsafe {
            std::env::set_var("MASTER_KEK_HEX", hex::encode([0x57u8; 32]));
        }
        let db = "huddle_consent_boundaries";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin = PgConnection::connect(&base).await.unwrap();
        admin
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .unwrap();
        admin
            .execute(format!("CREATE DATABASE {db}").as_str())
            .await
            .unwrap();
        let url = format!("{}/{db}", base.rsplit_once('/').unwrap().0);
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(8)
            .connect(&url)
            .await
            .unwrap();
        sqlx::migrate!("./migrations_control")
            .run(&pool)
            .await
            .unwrap();
        let owner: Uuid = sqlx::query_scalar(
            "INSERT INTO users(email) VALUES('owner@huddle.local') RETURNING id",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        let account: Uuid =
            sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES($1) RETURNING id")
                .bind(owner)
                .fetch_one(&pool)
                .await
                .unwrap();
        sqlx::query("INSERT INTO account_members(account_id,user_id,role,seat_tier_id) VALUES($1,$2,'owner',NULL)")
            .bind(account).bind(owner).execute(&pool).await.unwrap();

        let outsider: Uuid = sqlx::query_scalar(
            "INSERT INTO users(email) VALUES('outsider@huddle.local') RETURNING id",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        let graph:Uuid=sqlx::query_scalar("INSERT INTO graphs(account_id,name,kind,owner_user_id,is_primary) VALUES($1,'Personal ocean','personal',NULL,true) RETURNING id")
            .bind(account).fetch_one(&pool).await.unwrap();
        let wrapped = coralswarm_core::kek::wrap_dek(&[0x57u8; 32], &[0x65u8; 32]).unwrap();
        sqlx::query("UPDATE accounts SET dek_wrapped=$1 WHERE id=$2")
            .bind(wrapped)
            .bind(account)
            .execute(&pool)
            .await
            .unwrap();
        let encrypted =
            coralswarm_core::oauth_token::encrypt_token(&[0x65u8; 32], "xoxb-huddle-mock").unwrap();
        sqlx::query("INSERT INTO oauth_tokens(account_id,provider,scope,access_token,provider_workspace_id,provider_user_id) VALUES($1,'slack','channels:history channels:read chat:write',$2,'T000000001','U000000000')")
            .bind(account).bind(encrypted).execute(&pool).await.unwrap();
        let ids: Vec<String> = (0..10).map(|n| format!("U{n:09}")).collect();
        let state = MockSlack {
            roster: Arc::new(Mutex::new(json!(ids))),
            room: Arc::new(Mutex::new(
                json!({"transcript_file_id":"F000000001","participant_history":ids[..3]}),
            )),
            posts: Arc::new(Mutex::new(vec![])),
            post_ok: Arc::new(Mutex::new(true)),
            files: Arc::new(AtomicUsize::new(0)),
            users: Arc::new(AtomicUsize::new(0)),
        };
        let router = Router::new()
            .route("/conversations.members", get(members))
            .route("/conversations.history", get(history))
            .route("/chat.postMessage", post(posting))
            .route("/chat.update", post(updating))
            .route("/files.info", get(files))
            .route("/users.info", get(users))
            .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        let _env = super::super::slack::SlackApiBaseTestGuard::set(&endpoint);
        let http = reqwest::Client::new();
        let request = CaptureRequest {
            account_id: account,
            workspace_id: "T000000001".into(),
            channel_id: "C000000001".into(),
            huddle_thread_ts: "1.1".into(),
            canvas_file_id: "F000000001".into(),
            participant_history: vec![],
            date_start: None,
            date_end: None,
        };
        let first = execute_capture(&pool, &request, &http).await;
        assert_eq!(
            state.files.load(Ordering::SeqCst),
            0,
            "no canvas fetch before confirmation"
        );
        assert_eq!(state.posts.lock().unwrap().len(), 1);
        assert!(
            state.posts.lock().unwrap()[0]["text"]
                .as_str()
                .unwrap()
                .contains("7 channel members"),
            "full paged roster minus three attendees"
        );
        assert!(state.posts.lock().unwrap()[0]["text"]
            .as_str()
            .unwrap()
            .contains("private ocean for its account owner"));
        assert!(state.posts.lock().unwrap()[0]["text"]
            .as_str()
            .unwrap()
            .contains("0 non-attendees will gain access"));
        let linked: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM connector_identities WHERE external_user_id='U000000000'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            linked, 0,
            "personal owner fixture has no email directory proof"
        );
        let status: String = sqlx::query_scalar("SELECT status::text FROM jobs WHERE id=$1")
            .bind(first)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(
            status, "pending",
            "unconfirmed consent must retry without a terminal outcome"
        );
        let ledger: i64 =
            sqlx::query_scalar("SELECT count(*) FROM slack_huddle_captures WHERE account_id=$1")
                .bind(account)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(ledger, 0);
        assert!(
            require_consent(&pool, &pool, &http, "mock", &request, &[graph])
                .await
                .is_err()
        );
        assert_eq!(
            state.posts.lock().unwrap().len(),
            1,
            "retries do not repeat a posted disclosure"
        );
        let (id, ts) = prompt(&pool, account).await;
        assert!(
            confirm(
                &pool,
                &pool,
                &http,
                "mock",
                account,
                &request.workspace_id,
                &request.channel_id,
                &ts,
                "U000000009",
                id
            )
            .await
            .is_err(),
            "personal non-owner refused"
        );
        assert!(
            confirm(
                &pool,
                &pool,
                &http,
                "mock",
                Uuid::new_v4(),
                &request.workspace_id,
                &request.channel_id,
                &ts,
                "U000000000",
                id
            )
            .await
            .is_err(),
            "cross-account prompt refused"
        );
        assert!(confirm(
            &pool,
            &pool,
            &http,
            "mock",
            account,
            "TOTHER",
            &request.channel_id,
            &ts,
            "U000000000",
            id
        )
        .await
        .is_err());
        assert!(confirm(
            &pool,
            &pool,
            &http,
            "mock",
            account,
            &request.workspace_id,
            "COTHER",
            &ts,
            "U000000000",
            id
        )
        .await
        .is_err());
        assert!(confirm(
            &pool,
            &pool,
            &http,
            "mock",
            account,
            &request.workspace_id,
            &request.channel_id,
            "forged-ts",
            "U000000000",
            id
        )
        .await
        .is_err());
        *state.roster.lock().unwrap() =
            json!([ids.clone(), vec!["U999999999".to_string()]].concat());
        assert!(
            confirm(
                &pool,
                &pool,
                &http,
                "mock",
                account,
                &request.workspace_id,
                &request.channel_id,
                &ts,
                "U000000000",
                id
            )
            .await
            .is_err(),
            "changed audience must prompt again"
        );
        let (new_id, new_ts) = prompt(&pool, account).await;
        assert_ne!(new_id, id);
        assert_eq!(state.posts.lock().unwrap().len(), 2);
        assert!(
            confirm(
                &pool,
                &pool,
                &http,
                "mock",
                account,
                &request.workspace_id,
                &request.channel_id,
                &ts,
                "U000000000",
                id
            )
            .await
            .is_err(),
            "old opaque ID refused"
        );
        *state.post_ok.lock().unwrap() = false;
        assert!(
            confirm(
                &pool,
                &pool,
                &http,
                "mock",
                account,
                &request.workspace_id,
                &request.channel_id,
                &new_ts,
                "U000000000",
                new_id
            )
            .await
            .is_err(),
            "visible acknowledgement failure must refuse capture"
        );
        assert!(
            require_consent(&pool, &pool, &http, "mock", &request, &[graph])
                .await
                .is_err()
        );
        *state.post_ok.lock().unwrap() = true;
        confirm(
            &pool,
            &pool,
            &http,
            "mock",
            account,
            &request.workspace_id,
            &request.channel_id,
            &new_ts,
            "U000000000",
            new_id,
        )
        .await
        .unwrap();
        confirm(
            &pool,
            &pool,
            &http,
            "mock",
            account,
            &request.workspace_id,
            &request.channel_id,
            &new_ts,
            "U000000000",
            new_id,
        )
        .await
        .unwrap();
        let pending: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM jobs WHERE idempotency_key=$1 AND status='pending'",
        )
        .bind(format!("huddle-consent:{account}:{new_id}"))
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            pending, 1,
            "replayed acknowledgements deduplicate continuation"
        );
        assert_eq!(
            require_consent(&pool, &pool, &http, "mock", &request, &[graph])
                .await
                .unwrap(),
            ids[..3]
        );
        execute_capture(&pool, &request, &http).await;
        assert!(
            state.files.load(Ordering::SeqCst) > 0,
            "confirmed capture reaches canvas fetch"
        );
        *state.room.lock().unwrap() =
            json!({"transcript_file_id":"F000000001","participant_history":ids[1..4]});
        assert!(
            require_consent(&pool, &pool, &http, "mock", &request, &[graph])
                .await
                .is_err()
        );
        assert!(
            state.posts.lock().unwrap().last().unwrap()["text"]
                .as_str()
                .unwrap()
                .contains("1 non-attendees will gain access"),
            "private owner who did not attend is the sole exposed non-attendee"
        );
        *state.room.lock().unwrap() =
            json!({"transcript_file_id":"F000000001","participant_history":ids[..3]});
        let files_before = state.files.load(Ordering::SeqCst);
        sqlx::query("UPDATE graphs SET name='Renamed destination' WHERE id=$1")
            .bind(graph)
            .execute(&pool)
            .await
            .unwrap();
        execute_capture(&pool, &request, &http).await;
        assert_eq!(
            state.files.load(Ordering::SeqCst),
            files_before,
            "destination changes refuse ingestion"
        );
        let (renamed_id, renamed_ts) = prompt(&pool, account).await;
        assert_ne!(renamed_id, new_id);
        confirm(
            &pool,
            &pool,
            &http,
            "mock",
            account,
            &request.workspace_id,
            &request.channel_id,
            &renamed_ts,
            "U000000000",
            renamed_id,
        )
        .await
        .unwrap();
        assert_eq!(
            state.users.load(Ordering::SeqCst),
            0,
            "personal OAuth-proven owner never needs email mapping or email scope"
        );
        // Re-read exact account/workspace installer proof, including grant
        // generation, at acknowledgement and worker execution.
        sqlx::query("UPDATE oauth_tokens SET provider_user_id='U000000009',updated_at=now() WHERE account_id=$1 AND provider='slack'")
            .bind(account).execute(&pool).await.unwrap();
        assert!(
            require_consent(&pool, &pool, &http, "mock", &request, &[graph])
                .await
                .is_err(),
            "reassigned installer grant invalidates approval"
        );
        let (reassigned, reassigned_ts) = prompt(&pool, account).await;
        assert!(
            confirm(
                &pool,
                &pool,
                &http,
                "mock",
                account,
                &request.workspace_id,
                &request.channel_id,
                &reassigned_ts,
                "U000000000",
                reassigned
            )
            .await
            .is_err(),
            "previous installer cannot approve reassigned grant"
        );
        sqlx::query("UPDATE oauth_tokens SET provider_user_id='U000000000',credential_invalid_at=now() WHERE account_id=$1 AND provider='slack'")
            .bind(account).execute(&pool).await.unwrap();
        assert!(
            require_consent(&pool, &pool, &http, "mock", &request, &[graph])
                .await
                .is_err(),
            "revoked installer proof refuses capture"
        );
        assert!(confirm(
            &pool,
            &pool,
            &http,
            "mock",
            account,
            &request.workspace_id,
            &request.channel_id,
            &reassigned_ts,
            "U000000000",
            reassigned
        )
        .await
        .is_err());
        sqlx::query("UPDATE oauth_tokens SET credential_invalid_at=NULL,updated_at=now() WHERE account_id=$1 AND provider='slack'")
            .bind(account).execute(&pool).await.unwrap();
        assert!(
            require_consent(&pool, &pool, &http, "mock", &request, &[graph])
                .await
                .is_err(),
            "reconnected grant requires fresh disclosure"
        );
        let foreign_workspace = CaptureRequest {
            workspace_id: "TOTHER".into(),
            ..request.clone()
        };
        assert!(
            require_consent(&pool, &pool, &http, "mock", &foreign_workspace, &[graph])
                .await
                .is_err(),
            "OAuth proof never falls back to another workspace"
        );
        *state.roster.lock().unwrap() = json!({"ok":false,"error":"missing_scope"});
        assert!(
            require_consent(&pool, &pool, &http, "mock", &request, &[graph])
                .await
                .is_err(),
            "unavailable roster cannot become zero"
        );
        *state.roster.lock().unwrap() =
            json!({"ok":true,"members":["U000000000"],"response_metadata":{"next_cursor":"loop"}});
        assert!(
            require_consent(&pool, &pool, &http, "mock", &request, &[graph])
                .await
                .is_err(),
            "unexhausted roster cursor must refuse, not return a partial roster"
        );
        *state.roster.lock().unwrap() =
            json!({"ok":true,"members":["U000000000",null],"response_metadata":{"next_cursor":""}});
        assert!(
            require_consent(&pool, &pool, &http, "mock", &request, &[graph])
                .await
                .is_err(),
            "malformed roster entries must refuse, not disappear from count"
        );
        *state.roster.lock().unwrap() = json!(ids);
        *state.room.lock().unwrap() = json!({"transcript_file_id":"F000000001"});
        assert!(
            require_consent(&pool, &pool, &http, "mock", &request, &[graph])
                .await
                .is_err(),
            "missing attendance cannot fall back to canvas prose"
        );
        *state.room.lock().unwrap() =
            json!({"transcript_file_id":"F000000001","participant_history":ids[..3]});
        sqlx::query("UPDATE graphs SET name='Transport retry destination' WHERE id=$1")
            .bind(graph)
            .execute(&pool)
            .await
            .unwrap();
        *state.post_ok.lock().unwrap() = false;
        assert!(
            require_consent(&pool, &pool, &http, "mock", &request, &[graph])
                .await
                .is_err()
        );
        let failed_id: Uuid =
            sqlx::query_scalar("SELECT id FROM slack_huddle_consents WHERE account_id=$1")
                .bind(account)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert!(
            require_consent(&pool, &pool, &http, "mock", &request, &[graph])
                .await
                .is_err()
        );
        let last_two = state.posts.lock().unwrap();
        assert_eq!(
            last_two[last_two.len() - 1]["client_msg_id"],
            last_two[last_two.len() - 2]["client_msg_id"],
            "uncertain/failed posts reuse persisted delivery identity"
        );
        assert_eq!(last_two.last().unwrap()["client_msg_id"], json!(failed_id));
        drop(last_two);
        assert_eq!(
            state.files.load(Ordering::SeqCst),
            files_before,
            "post/ack failure never fetches notes"
        );
        sqlx::query("DELETE FROM account_members WHERE account_id=$1 AND user_id=$2")
            .bind(account)
            .bind(outsider)
            .execute(&pool)
            .await
            .unwrap();
        let ledger: i64 =
            sqlx::query_scalar("SELECT count(*) FROM slack_huddle_captures WHERE account_id=$1")
                .bind(account)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(
            ledger, 0,
            "transient failures never create terminal ledger entries"
        );
        sqlx::query("DELETE FROM oauth_tokens WHERE account_id=$1 AND provider='slack'")
            .bind(account)
            .execute(&pool)
            .await
            .unwrap();
        assert!(
            personal_owner_proof(&pool, &request)
                .await
                .unwrap()
                .is_none(),
            "removed grant cannot authorize a prior installer"
        );
        assert!(
            require_consent(&pool, &pool, &http, "mock", &request, &[graph])
                .await
                .is_err()
        );
        // Organization content resides on a distinct tenant pool. A personal
        // account owner cannot approve an existing team unless they administer
        // that destination; the currently verified team admin can.
        *state.post_ok.lock().unwrap() = true;
        let tenant_db = "huddle_consent_tenant";
        let _cleanup_tenant_db = crate::test_db::TestDatabase::adopt_from_url(&base, tenant_db);
        admin
            .execute(format!("DROP DATABASE IF EXISTS {tenant_db} WITH (FORCE)").as_str())
            .await
            .unwrap();
        admin
            .execute(format!("CREATE DATABASE {tenant_db}").as_str())
            .await
            .unwrap();
        let tenant_url = format!("{}/{tenant_db}", base.rsplit_once('/').unwrap().0);
        let tenant_pool = sqlx::postgres::PgPoolOptions::new()
            .connect(&tenant_url)
            .await
            .unwrap();
        sqlx::migrate!("./migrations_tenant")
            .run(&tenant_pool)
            .await
            .unwrap();
        let org_account:Uuid = sqlx::query_scalar("INSERT INTO accounts(owner_user_id,clerk_org_id) VALUES($1,'org-huddle-consent') RETURNING id")
            .bind(owner).fetch_one(&pool).await.unwrap();
        for (user, role) in [(owner, "owner"), (outsider, "member")] {
            sqlx::query("INSERT INTO account_members(account_id,user_id,role,seat_tier_id) VALUES($1,$2,$3,(SELECT CASE WHEN a.clerk_org_id IS NOT NULL THEN st.id ELSE NULL END FROM accounts a JOIN seat_tiers st ON st.code='standard' AND st.is_active WHERE a.id=$1))")
                .bind(org_account).bind(user).bind(role).execute(&pool).await.unwrap();
        }
        for (actor, user) in [("U000000000", owner), ("U000000009", outsider)] {
            sqlx::query("INSERT INTO connector_identities(provider,workspace_id,external_user_id,canonical_name,user_id,link_method,link_verified_at) VALUES('slack','T000000001',$1,'Tenant actor',$2,'verified_email',now())")
                .bind(actor).bind(user).execute(&tenant_pool).await.unwrap();
        }
        let team_graph:Uuid = sqlx::query_scalar("INSERT INTO graphs(account_id,name,kind) VALUES($1,'Team destination','team') RETURNING id")
            .bind(org_account).fetch_one(&tenant_pool).await.unwrap();
        let team:Uuid = sqlx::query_scalar("INSERT INTO teams(account_id,graph_id,name,created_by) VALUES($1,$2,'Team destination',$3) RETURNING id")
            .bind(org_account).bind(team_graph).bind(outsider).fetch_one(&tenant_pool).await.unwrap();
        sqlx::query("INSERT INTO team_members(team_id,user_id,role) VALUES($1,$2,'admin')")
            .bind(team)
            .bind(outsider)
            .execute(&tenant_pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO slack_watched_channels(account_id,workspace_id,channel_id,mode,graph_id,user_id) VALUES($1,$2,$3,'single',$4,$5)")
            .bind(org_account).bind(&request.workspace_id).bind(&request.channel_id).bind(team_graph).bind(outsider)
            .execute(&tenant_pool).await.unwrap();
        let org_request = CaptureRequest {
            account_id: org_account,
            ..request.clone()
        };
        let posts_before_unknown = state.posts.lock().unwrap().len();
        sqlx::query("UPDATE connector_identities SET user_id=NULL,link_method=NULL,link_verified_at=NULL WHERE external_user_id='U000000009'")
            .execute(&tenant_pool).await.unwrap();
        assert!(
            require_consent(
                &pool,
                &tenant_pool,
                &http,
                "mock",
                &org_request,
                &[team_graph]
            )
            .await
            .is_err(),
            "unknown destination-reader attendance must refuse, never invent an exposure count"
        );
        assert_eq!(state.posts.lock().unwrap().len(), posts_before_unknown);
        sqlx::query("UPDATE connector_identities SET user_id=$1,link_method='verified_email',link_verified_at=now() WHERE external_user_id='U000000009'")
            .bind(outsider).execute(&tenant_pool).await.unwrap();
        assert!(require_consent(
            &pool,
            &tenant_pool,
            &http,
            "mock",
            &org_request,
            &[team_graph]
        )
        .await
        .is_err());
        let (team_id, team_ts) = prompt(&tenant_pool, org_account).await;
        let text = state.posts.lock().unwrap().last().unwrap()["text"]
            .as_str()
            .unwrap()
            .to_owned();
        assert!(text.contains("7 channel members did not attend"));
        assert!(text.contains("team ocean; 1 active members"));
        assert!(
            text.contains("1 non-attendees will gain access"),
            "truthful destination exposure is distinct from seven channel non-attendees"
        );
        assert!(confirm(
            &pool,
            &tenant_pool,
            &http,
            "mock",
            org_account,
            &request.workspace_id,
            &request.channel_id,
            &team_ts,
            "U000000000",
            team_id
        )
        .await
        .is_err());
        confirm(
            &pool,
            &tenant_pool,
            &http,
            "mock",
            org_account,
            &request.workspace_id,
            &request.channel_id,
            &team_ts,
            "U000000009",
            team_id,
        )
        .await
        .unwrap();
        assert!(require_consent(
            &pool,
            &tenant_pool,
            &http,
            "mock",
            &org_request,
            &[team_graph]
        )
        .await
        .is_ok());
        sqlx::query("UPDATE team_members SET departing_at=now() WHERE team_id=$1 AND user_id=$2")
            .bind(team)
            .bind(outsider)
            .execute(&tenant_pool)
            .await
            .unwrap();
        assert!(
            require_consent(
                &pool,
                &tenant_pool,
                &http,
                "mock",
                &org_request,
                &[team_graph]
            )
            .await
            .is_err(),
            "removed team audience/admin invalidates confirmation"
        );
        let (changed_team, changed_ts) = prompt(&tenant_pool, org_account).await;
        assert!(confirm(
            &pool,
            &tenant_pool,
            &http,
            "mock",
            org_account,
            &request.workspace_id,
            &request.channel_id,
            &changed_ts,
            "U000000009",
            changed_team
        )
        .await
        .is_err());
        // The organization default fan-out names its private member oceans,
        // requires an account admin/owner and binds the current membership.
        let mut member_graphs = Vec::new();
        for user in [owner, outsider] {
            let graph:Uuid=sqlx::query_scalar("INSERT INTO graphs(account_id,name,kind,owner_user_id,is_primary) VALUES($1,'Member destination','member',$2,true) RETURNING id")
                .bind(org_account).bind(user).fetch_one(&tenant_pool).await.unwrap();
            member_graphs.push(graph);
        }
        sqlx::query(
            "UPDATE slack_watched_channels SET mode='fanout',graph_id=NULL WHERE account_id=$1",
        )
        .bind(org_account)
        .execute(&tenant_pool)
        .await
        .unwrap();
        assert!(require_consent(
            &pool,
            &tenant_pool,
            &http,
            "mock",
            &org_request,
            &member_graphs
        )
        .await
        .is_err());
        let (fanout_id, fanout_ts) = prompt(&tenant_pool, org_account).await;
        assert!(state.posts.lock().unwrap().last().unwrap()["text"]
            .as_str()
            .unwrap()
            .contains("2 private member oceans"));
        assert!(
            confirm(
                &pool,
                &tenant_pool,
                &http,
                "mock",
                org_account,
                &request.workspace_id,
                &request.channel_id,
                &fanout_ts,
                "U000000009",
                fanout_id
            )
            .await
            .is_err(),
            "member cannot approve organization fan-out"
        );
        confirm(
            &pool,
            &tenant_pool,
            &http,
            "mock",
            org_account,
            &request.workspace_id,
            &request.channel_id,
            &fanout_ts,
            "U000000000",
            fanout_id,
        )
        .await
        .unwrap();
        assert!(require_consent(
            &pool,
            &tenant_pool,
            &http,
            "mock",
            &org_request,
            &member_graphs
        )
        .await
        .is_ok());
        sqlx::query("UPDATE account_members SET role='admin' WHERE account_id=$1 AND user_id=$2")
            .bind(org_account)
            .bind(outsider)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("UPDATE connector_identities SET user_id=$1,link_verified_at=now() WHERE provider='slack' AND workspace_id='T000000001' AND external_user_id='U000000000'")
            .bind(outsider).execute(&tenant_pool).await.unwrap();
        assert!(require_consent(&pool,&tenant_pool,&http,"mock",&org_request,&member_graphs).await.is_err(),"identity reassignment cannot reuse old approval even when new identity has admin permissions");
        sqlx::query("UPDATE connector_identities SET user_id=$1,link_verified_at=now() WHERE provider='slack' AND workspace_id='T000000001' AND external_user_id='U000000000'")
            .bind(owner).execute(&tenant_pool).await.unwrap();
        sqlx::query("DELETE FROM account_members WHERE account_id=$1 AND user_id=$2")
            .bind(org_account)
            .bind(outsider)
            .execute(&pool)
            .await
            .unwrap();
        assert!(
            require_consent(
                &pool,
                &tenant_pool,
                &http,
                "mock",
                &org_request,
                &member_graphs[..1]
            )
            .await
            .is_err(),
            "organization membership changes require new disclosure"
        );
        let control_rows: i64 =
            sqlx::query_scalar("SELECT count(*) FROM slack_huddle_consents WHERE account_id=$1")
                .bind(org_account)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(
            control_rows, 0,
            "org consent metadata never lands on control"
        );
        // Prove account policy using a non-owner role on the tenant plane.
        admin
            .execute("DROP ROLE IF EXISTS cs_huddle_consent_scope_test")
            .await
            .unwrap();
        admin
            .execute("CREATE ROLE cs_huddle_consent_scope_test NOLOGIN NOBYPASSRLS")
            .await
            .unwrap();
        sqlx::query("GRANT SELECT ON slack_huddle_consents TO cs_huddle_consent_scope_test")
            .execute(&tenant_pool)
            .await
            .unwrap();
        let mut scoped = begin_tenant_system_scoped(&tenant_pool, org_account)
            .await
            .unwrap();
        sqlx::query("SET LOCAL ROLE cs_huddle_consent_scope_test")
            .execute(&mut *scoped)
            .await
            .unwrap();
        let visible: i64 = sqlx::query_scalar("SELECT count(*) FROM slack_huddle_consents")
            .fetch_one(&mut *scoped)
            .await
            .unwrap();
        assert_eq!(visible, 1);
        sqlx::query("SELECT set_config('app.account_id',$1,true)")
            .bind(Uuid::new_v4().to_string())
            .execute(&mut *scoped)
            .await
            .unwrap();
        let foreign: i64 = sqlx::query_scalar("SELECT count(*) FROM slack_huddle_consents")
            .fetch_one(&mut *scoped)
            .await
            .unwrap();
        assert_eq!(foreign, 0);
        scoped.rollback().await.unwrap();
        tenant_pool.close().await;
        admin
            .execute(format!("DROP DATABASE {tenant_db} WITH (FORCE)").as_str())
            .await
            .unwrap();
        admin
            .execute("DROP ROLE cs_huddle_consent_scope_test")
            .await
            .unwrap();
        server.abort();
        pool.close().await;
        admin
            .execute(format!("DROP DATABASE {db} WITH (FORCE)").as_str())
            .await
            .unwrap();
    }
}
