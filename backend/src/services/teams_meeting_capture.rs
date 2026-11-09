//! An explicit meeting action is the only authority to read a transcript.
//! OAuth, calendar presence, recording and ordinary chat capture are not consent.
use crate::{
    services::connector_audit::{self, Actor, AuditEntry},
    tenancy::scope::begin_tenant_scoped,
};
use sqlx::PgPool;
use uuid::Uuid;

pub(crate) async fn active_meetings(
    pool: &PgPool,
    account: Uuid,
    member: Uuid,
    tenant: &str,
    aad: &str,
) -> sqlx::Result<Vec<(String, Uuid)>> {
    let mut tx = begin_tenant_scoped(pool, account, member)
        .await
        .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
    let rows=sqlx::query_as("SELECT c.meeting_id,c.graph_id FROM teams_meeting_captures c JOIN graphs g ON g.id=c.graph_id WHERE c.account_id=$1 AND c.user_id=$2 AND c.tenant_id=$3 AND c.aad_id=$4 AND c.revoked_at IS NULL AND g.account_id=$1 AND g.archived_at IS NULL AND ((g.kind='member' AND g.owner_user_id=$2) OR (g.kind='personal' AND g.owner_user_id IS NULL)) AND (c.source <> 'meeting_bot' OR EXISTS(SELECT 1 FROM teams_meeting_installations i WHERE i.account_id=c.account_id AND i.tenant_id=c.tenant_id AND i.installation_id=c.installation_id AND i.active))")
 .bind(account).bind(member).bind(tenant).bind(aad).fetch_all(&mut *tx).await?;
    tx.rollback().await?;
    Ok(rows)
}
pub(crate) async fn active_graph(
    pool: &PgPool,
    account: Uuid,
    member: Uuid,
    tenant: &str,
    aad: &str,
    meeting: &str,
) -> sqlx::Result<Option<Uuid>> {
    Ok(active_meetings(pool, account, member, tenant, aad)
        .await?
        .into_iter()
        .find(|(id, _)| id == meeting)
        .map(|(_, graph)| graph))
}
/// Status is scoped to one owned, verified occurrence; it never grants consent.
#[derive(Debug, sqlx::FromRow, serde::Serialize)]
pub(crate) struct CaptureStatus {
    pub capture_id: Uuid,
    pub meeting_id: String,
    pub status: String,
    pub occurrence_start: chrono::DateTime<chrono::Utc>,
    pub occurrence_end: chrono::DateTime<chrono::Utc>,
    pub last_checked_at: Option<chrono::DateTime<chrono::Utc>>,
    pub last_retrieved_at: Option<chrono::DateTime<chrono::Utc>>,
    pub retrieved_transcript_count: i32,
    pub last_error_code: Option<String>,
    #[serde(skip)]
    pub capture_generation: Uuid,
    #[serde(skip)]
    pub graph_id: Uuid,
    /// Recurring-series occurrences only; see `Meeting::transcript_end`.
    #[serde(skip)]
    pub transcript_end: Option<chrono::DateTime<chrono::Utc>>,
}

/// The opaque generation changes on explicit re-enable. A stopped/replaced generation
/// cannot receive an observation from an older in-flight provider request.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ObservationLease {
    pub capture_id: Uuid,
    pub capture_generation: Uuid,
}
impl CaptureStatus {
    pub fn lease(&self) -> Option<ObservationLease> {
        (self.status == "active").then_some(ObservationLease {
            capture_id: self.capture_id,
            capture_generation: self.capture_generation,
        })
    }
}

/// One capture row (by id or meeting) with its lifecycle status, as the member sees it.
pub(crate) async fn status(
    pool: &PgPool,
    account: Uuid,
    member: Uuid,
    tenant: &str,
    aad: &str,
    capture_id: Option<Uuid>,
    meeting: Option<&str>,
) -> sqlx::Result<Option<CaptureStatus>> {
    status_scoped(
        pool,
        account,
        member,
        Some(tenant),
        Some(aad),
        capture_id,
        meeting,
    )
    .await
}

/// Owner-only local ledger access remains available after Microsoft disconnect.
/// It never reads provider data or creates capture authority.
pub(crate) async fn owned_status(
    pool: &PgPool,
    account: Uuid,
    member: Uuid,
    id: Uuid,
) -> sqlx::Result<Option<CaptureStatus>> {
    status_scoped(pool, account, member, None, None, Some(id), None).await
}

async fn status_scoped(
    pool: &PgPool,
    account: Uuid,
    member: Uuid,
    tenant: Option<&str>,
    aad: Option<&str>,
    capture_id: Option<Uuid>,
    meeting: Option<&str>,
) -> sqlx::Result<Option<CaptureStatus>> {
    let mut tx = begin_tenant_scoped(pool, account, member)
        .await
        .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
    let row = sqlx::query_as::<_, CaptureStatus>(
        "SELECT c.id AS capture_id,c.meeting_id,c.graph_id,c.capture_generation,c.occurrence_start,c.occurrence_end,c.transcript_end,
        CASE WHEN c.revoked_at IS NULL AND (c.source<>'meeting_bot' OR EXISTS(
          SELECT 1 FROM teams_meeting_installations i WHERE i.account_id=c.account_id
          AND i.tenant_id=c.tenant_id AND i.installation_id=c.installation_id AND i.active))
          THEN 'active' ELSE 'stopped' END AS status,
        c.last_checked_at,c.last_retrieved_at,c.retrieved_transcript_count,c.last_error_code
        FROM teams_meeting_captures c JOIN graphs g ON g.id=c.graph_id
        WHERE c.account_id=$1 AND c.user_id=$2 AND ($3::text IS NULL OR c.tenant_id=$3) AND ($4::text IS NULL OR c.aad_id=$4)
        AND ($5::uuid IS NULL OR c.id=$5) AND ($6::text IS NULL OR c.meeting_id=$6)
        AND ($5::uuid IS NOT NULL OR $6::text IS NOT NULL)
        AND g.account_id=$1 AND g.archived_at IS NULL AND ((g.kind='member' AND g.owner_user_id=$2) OR (g.kind='personal' AND g.owner_user_id IS NULL))"
    ).bind(account).bind(member).bind(tenant).bind(aad).bind(capture_id).bind(meeting)
        .fetch_optional(&mut *tx).await?;
    tx.rollback().await?;
    Ok(row)
}

pub(crate) fn provider_error_code(
    error: &crate::connectors::teams_targeted_transcripts::Error,
) -> &'static str {
    use crate::connectors::teams_targeted_transcripts::Error;
    match error {
        Error::Graph {
            code: Some(code), ..
        } if code == "GraphAccessToTranscriptsDisabled" => "transcript_access_disabled",
        Error::Graph {
            status: 401 | 403, ..
        } => "permission_denied",
        Error::Graph { status: 404, .. } => "not_found",
        Error::Graph { status: 429, .. } => "rate_limited",
        Error::Graph { .. } | Error::Unavailable => "provider_unavailable",
        Error::IncompleteList => "incomplete_response",
        Error::InvalidMetadata | Error::InvalidTarget => "invalid_metadata",
    }
}

/// Record completed actual provider checks, including failed meeting-metadata
/// resolution before a transcript can be requested. A success count is the
/// most recent completed transcript fetch, including empty results and repeats;
/// it does not claim new atoms were saved by the asynchronous ingest queue.
pub(crate) async fn observe_fetch(
    pool: &PgPool,
    account: Uuid,
    member: Uuid,
    tenant: &str,
    aad: &str,
    meeting: &str,
    lease: ObservationLease,
    result: &Result<
        crate::connectors::teams_targeted_transcripts::Fetched,
        crate::connectors::teams_targeted_transcripts::Error,
    >,
) -> sqlx::Result<bool> {
    if result
        .as_ref()
        .is_ok_and(|fetched| fetched.meeting_id != meeting)
    {
        return Ok(false);
    }
    let (count, error) = match result {
        Ok(fetched) => (
            Some(i32::try_from(fetched.items.len()).map_err(|_| {
                sqlx::Error::Protocol("provider result count exceeded integer bound".into())
            })?),
            None,
        ),
        Err(e) => (None, Some(provider_error_code(e))),
    };
    let mut tx = begin_tenant_scoped(pool, account, member)
        .await
        .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
    let updated = sqlx::query(
        "UPDATE teams_meeting_captures c SET last_checked_at=now(),
        last_retrieved_at=CASE WHEN $8::integer IS NOT NULL THEN now() ELSE c.last_retrieved_at END,
        retrieved_transcript_count=COALESCE($8,c.retrieved_transcript_count),last_error_code=$9
        FROM graphs g WHERE c.id=$5 AND c.capture_generation=$6 AND c.meeting_id=$7
        AND c.account_id=$1 AND c.user_id=$2 AND c.tenant_id=$3 AND c.aad_id=$4 AND c.revoked_at IS NULL
        AND c.occurrence_start<=now() AND c.occurrence_end+interval '24 hours'>=now()
        AND g.id=c.graph_id AND g.account_id=$1 AND g.archived_at IS NULL AND ((g.kind='member' AND g.owner_user_id=$2) OR (g.kind='personal' AND g.owner_user_id IS NULL))
        AND (c.source<>'meeting_bot' OR EXISTS(SELECT 1 FROM teams_meeting_installations i
          WHERE i.account_id=c.account_id AND i.tenant_id=c.tenant_id AND i.installation_id=c.installation_id AND i.active))"
    ).bind(account).bind(member).bind(tenant).bind(aad).bind(lease.capture_id).bind(lease.capture_generation)
        .bind(meeting).bind(count).bind(error).execute(&mut *tx).await?.rows_affected();
    tx.commit().await?;
    Ok(updated == 1)
}

pub(crate) async fn lease_active(
    pool: &PgPool,
    account: Uuid,
    member: Uuid,
    tenant: &str,
    aad: &str,
    meeting: &str,
    lease: ObservationLease,
) -> sqlx::Result<bool> {
    Ok(status(
        pool,
        account,
        member,
        tenant,
        aad,
        Some(lease.capture_id),
        Some(meeting),
    )
    .await?
    .is_some_and(|row| {
        row.status == "active"
            && row.capture_generation == lease.capture_generation
            && row.occurrence_start <= chrono::Utc::now()
            && row.occurrence_end + chrono::Duration::hours(24) >= chrono::Utc::now()
    }))
}

/// `timestamptz` keeps microseconds. sqlx encodes a `DateTime` as the whole
/// microseconds since the Postgres epoch (2000-01-01, truncating toward it), so
/// this is exactly the value that would be stored and read back.
fn to_db_precision(at: chrono::DateTime<chrono::Utc>) -> chrono::DateTime<chrono::Utc> {
    let epoch = chrono::DateTime::from_timestamp(946_684_800, 0).expect("postgres epoch");
    (at - epoch)
        .num_microseconds()
        .map_or(at, |micros| epoch + chrono::Duration::microseconds(micros))
}

/// Caller must first verify the target through Microsoft using this member's grant.
pub(crate) async fn authorize(
    pool: &PgPool,
    account: Uuid,
    member: Uuid,
    tenant: &str,
    aad: &str,
    meeting: &str,
    graph: Uuid,
    start: chrono::DateTime<chrono::Utc>,
    end: chrono::DateTime<chrono::Utc>,
    source: &'static str,
    installation: Option<&str>,
    event: Option<&str>,
    event_at: Option<chrono::DateTime<chrono::Utc>>,
) -> sqlx::Result<Uuid> {
    authorize_with_window(
        pool, account, member, tenant, aad, meeting, graph, start, end, None, source,
        installation, event, event_at,
    )
    .await
}

/// As [`authorize`], also persisting the occurrence's transcript upper bound
/// (`Some` only for a recurring-series occurrence).
pub(crate) async fn authorize_with_window(
    pool: &PgPool,
    account: Uuid,
    member: Uuid,
    tenant: &str,
    aad: &str,
    meeting: &str,
    graph: Uuid,
    start: chrono::DateTime<chrono::Utc>,
    end: chrono::DateTime<chrono::Utc>,
    transcript_end: Option<chrono::DateTime<chrono::Utc>>,
    source: &'static str,
    installation: Option<&str>,
    event: Option<&str>,
    event_at: Option<chrono::DateTime<chrono::Utc>>,
) -> sqlx::Result<Uuid> {
    // The occurrence is compared with what `timestamptz` stored (microseconds), so
    // normalise the Graph-precision input the way the database will before any
    // comparison: a 100ns-tick repeat is the same occurrence, a >=1us change is not.
    let (start, end, transcript_end) = (
        to_db_precision(start),
        to_db_precision(end),
        transcript_end.map(to_db_precision),
    );
    if end <= start
        || source == "meeting_bot"
            && (installation.is_none_or(str::is_empty)
                || event.is_none_or(str::is_empty)
                || event_at.is_none())
    {
        return Err(sqlx::Error::Protocol(
            "unverified meeting consent source".into(),
        ));
    }
    let mut tx = begin_tenant_scoped(pool, account, member)
        .await
        .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1,0))")
        .bind(format!("{account}/{member}/{tenant}/{meeting}"))
        .execute(&mut *tx)
        .await?;
    let existing:Option<(Uuid,Option<chrono::DateTime<chrono::Utc>>,Option<String>,String,Option<String>,Uuid,chrono::DateTime<chrono::Utc>,chrono::DateTime<chrono::Utc>,String,Option<chrono::DateTime<chrono::Utc>>,i32)>=sqlx::query_as("SELECT id,revoked_at,source_event_id,source,installation_id,graph_id,occurrence_start,occurrence_end,aad_id,transcript_end,retrieved_transcript_count FROM teams_meeting_captures WHERE account_id=$1 AND user_id=$2 AND tenant_id=$3 AND meeting_id=$4 FOR UPDATE")
 .bind(account).bind(member).bind(tenant).bind(meeting).fetch_optional(&mut *tx).await?;
    if let Some((
        id,
        None,
        _,
        old_source,
        old_install,
        old_graph,
        old_start,
        old_end,
        old_aad,
        old_transcript_end,
        old_retrieved,
    )) = &existing
    {
        // A series shares one meeting ID; once the stored occurrence is over,
        // fresh EXPLICIT member consent (pane/explicit capture) for the next
        // occurrence replaces it. Bot events never retarget on their own.
        // Replacement must not drop a transcript not yet retrieved: only once the
        // old occurrence's own fetch window has closed (its series bound, else
        // end + 24h, the same bound the worker enforces) or it already retrieved
        // a transcript.
        let old_window_closed = old_transcript_end
            .unwrap_or(*old_end + chrono::Duration::hours(24))
            < chrono::Utc::now();
        // A year < 2000 occurrence is Graph's placeholder stored by an older
        // build: an impossible window that can never retrieve anything, so any
        // source may replace it.
        // A bot event may do so only for a row the bot owns: it must never turn a
        // member's explicit consent into installation-only authority (removing the
        // bot would then revoke it). Explicit consent may replace either.
        let impossible = chrono::Datelike::year(old_start) < 2000
            && (source != "meeting_bot" || old_source == "meeting_bot");
        let superseded = impossible
            || source != "meeting_bot"
                && *old_start != start
                && (old_window_closed || *old_retrieved > 0 && *old_end < chrono::Utc::now());
        if !superseded && (*old_graph != graph || *old_start != start || *old_end != end) {
            return Err(sqlx::Error::Protocol(
                "existing consent target differs; revoke before selecting a different occurrence"
                    .into(),
            ));
        }
        // Explicit add-in consent survives later bot removal, and a bot can never
        // downgrade an independent explicit user's authority to installation-only.
        if !superseded
            && old_aad == aad
            && (old_source != "meeting_bot"
                || (source == "meeting_bot" && old_install.as_deref() == installation))
        {
            tx.rollback().await?;
            return Ok(*id);
        }
    }
    if source == "meeting_bot"
        && matches!(&existing,Some((_,Some(revoked),old,_,_,_,_,_,_,_,_)) if old.as_deref()==event || event_at.is_none_or(|at|at <= *revoked))
    {
        return Err(sqlx::Error::Protocol(
            "provider event already revoked".into(),
        ));
    }
    let id:Uuid=sqlx::query_scalar("INSERT INTO teams_meeting_captures(account_id,user_id,tenant_id,meeting_id,graph_id,source,installation_id,occurrence_start,occurrence_end,source_event_id,source_event_at,aad_id,transcript_end) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13) ON CONFLICT(account_id,user_id,tenant_id,meeting_id) DO UPDATE SET revoked_at=NULL,next_poll_at=now(),aad_id=EXCLUDED.aad_id,source_event_at=EXCLUDED.source_event_at,source_event_id=EXCLUDED.source_event_id,graph_id=EXCLUDED.graph_id,source=EXCLUDED.source,installation_id=EXCLUDED.installation_id,occurrence_start=EXCLUDED.occurrence_start,occurrence_end=EXCLUDED.occurrence_end,transcript_end=EXCLUDED.transcript_end,created_at=now(),capture_generation=gen_random_uuid(),last_checked_at=NULL,last_retrieved_at=NULL,retrieved_transcript_count=0,last_error_code=NULL RETURNING id")
 .bind(account).bind(member).bind(tenant).bind(meeting).bind(graph).bind(source).bind(installation).bind(start).bind(end).bind(event).bind(event_at).bind(aad).bind(transcript_end).fetch_one(&mut *tx).await?;
    connector_audit::record_in_tx(&mut tx,AuditEntry{account_id:account,actor:Actor::User(member),action:"consent",provider:Some("microsoft".into()),subject:Some(id.to_string()),detail:Some(serde_json::json!({"operation":"meeting_transcript_capture","meeting_id":meeting,"tenant_id":tenant,"aad_id":aad,"graph_id":graph,"source":source,"occurrence_start":start,"occurrence_end":end,"availability_window_hours":24}))}).await?;
    tx.commit().await?;
    Ok(id)
}
pub(crate) async fn revoke(
    pool: &PgPool,
    account: Uuid,
    member: Uuid,
    tenant: &str,
    meeting: &str,
) -> sqlx::Result<()> {
    let mut tx = begin_tenant_scoped(pool, account, member)
        .await
        .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
    let id:Option<Uuid>=sqlx::query_scalar("UPDATE teams_meeting_captures SET revoked_at=now() WHERE account_id=$1 AND user_id=$2 AND tenant_id=$3 AND meeting_id=$4 AND revoked_at IS NULL RETURNING id")
 .bind(account).bind(member).bind(tenant).bind(meeting).fetch_optional(&mut *tx).await?;
    if let Some(id) = id {
        connector_audit::record_in_tx(&mut tx,AuditEntry{account_id:account,actor:Actor::User(member),action:"disconnect",provider:Some("microsoft".into()),subject:Some(id.to_string()),detail:Some(serde_json::json!({"operation":"meeting_transcript_capture","meeting_id":meeting,"tenant_id":tenant}))}).await?;
    }
    tx.commit().await?;
    Ok(())
}

/// Negative authority only: revoke an authenticated member's existing private selection.
pub(crate) async fn revoke_owned(
    pool: &PgPool,
    account: Uuid,
    member: Uuid,
    id: Uuid,
) -> sqlx::Result<bool> {
    let mut tx = begin_tenant_scoped(pool, account, member)
        .await
        .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
    let row: Option<(String,String)> = sqlx::query_as("SELECT c.meeting_id,c.tenant_id FROM teams_meeting_captures c JOIN graphs g ON g.id=c.graph_id WHERE c.id=$3 AND c.account_id=$1 AND c.user_id=$2 AND g.account_id=$1 AND g.archived_at IS NULL AND ((g.kind='member' AND g.owner_user_id=$2) OR (g.kind='personal' AND g.owner_user_id IS NULL)) FOR UPDATE OF c")
        .bind(account).bind(member).bind(id).fetch_optional(&mut *tx).await?;
    let Some((meeting, tenant)) = row else {
        tx.rollback().await?;
        return Ok(false);
    };
    let changed=sqlx::query("UPDATE teams_meeting_captures SET revoked_at=now() WHERE id=$3 AND account_id=$1 AND user_id=$2 AND revoked_at IS NULL")
        .bind(account).bind(member).bind(id).execute(&mut *tx).await?.rows_affected()>0;
    if changed {
        connector_audit::record_in_tx(&mut tx,AuditEntry{account_id:account,actor:Actor::User(member),action:"disconnect",provider:Some("microsoft".into()),subject:Some(id.to_string()),detail:Some(serde_json::json!({"operation":"meeting_transcript_capture","meeting_id":meeting,"tenant_id":tenant}))}).await?;
    }
    tx.commit().await?;
    Ok(true)
}

pub(crate) async fn active_graph_at(
    pool: &PgPool,
    account: Uuid,
    member: Uuid,
    tenant: &str,
    aad: &str,
    meeting: &str,
    created_at: chrono::DateTime<chrono::Utc>,
) -> sqlx::Result<Option<Uuid>> {
    let mut tx = begin_tenant_scoped(pool, account, member)
        .await
        .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
    let graph=sqlx::query_scalar("SELECT c.graph_id FROM teams_meeting_captures c JOIN graphs g ON g.id=c.graph_id WHERE c.account_id=$1 AND c.user_id=$2 AND c.tenant_id=$3 AND c.meeting_id=$4 AND c.aad_id=$6 AND c.revoked_at IS NULL AND c.occurrence_start <= $5 AND c.occurrence_end + interval '24 hours' >= $5 AND g.account_id=$1 AND g.archived_at IS NULL AND ((g.kind='member' AND g.owner_user_id=$2) OR (g.kind='personal' AND g.owner_user_id IS NULL)) AND (c.source <> 'meeting_bot' OR EXISTS(SELECT 1 FROM teams_meeting_installations i WHERE i.account_id=c.account_id AND i.tenant_id=c.tenant_id AND i.installation_id=c.installation_id AND i.active))")
 .bind(account).bind(member).bind(tenant).bind(meeting).bind(created_at).bind(aad).fetch_optional(&mut *tx).await?;
    tx.rollback().await?;
    Ok(graph)
}
pub(crate) async fn installation_status(
    pool: &PgPool,
    account: Uuid,
    tenant: &str,
    installation: &str,
    context_id: &str,
    active: bool,
) -> sqlx::Result<()> {
    let mut tx = crate::tenancy::scope::begin_account_scoped(pool, account)
        .await
        .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
    sqlx::query("INSERT INTO teams_meeting_installations(account_id,tenant_id,installation_id,context_id,active) VALUES($1,$2,$3,$4,$5) ON CONFLICT(account_id,tenant_id,installation_id) DO NOTHING")
 .bind(account).bind(tenant).bind(installation).bind(context_id).bind(active).execute(&mut *tx).await?;
    let current:bool=sqlx::query_scalar("SELECT active FROM teams_meeting_installations WHERE account_id=$1 AND tenant_id=$2 AND installation_id=$3 AND context_id=$4").bind(account).bind(tenant).bind(installation).bind(context_id).fetch_one(&mut *tx).await?;
    if !current {
        return Err(sqlx::Error::Protocol("meeting installation revoked".into()));
    }
    tx.commit().await?;
    Ok(())
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct MeetingIntent {
    pub tenant: String,
    pub aad: String,
    pub meeting_id: String,
    pub installation_id: String,
    pub event_id: String,
    pub event_at: chrono::DateTime<chrono::Utc>,
    aud: String,
    exp: i64,
}
/// HS256 key for meeting-capture intents: the HKDF `teams-meeting-capture`
/// derivation of the shared state secret (`crypto::kdf`), never the secret
/// itself and never a string-concatenated variant of it.
fn intent_key(secret: &str) -> [u8; crate::crypto::kdf::DERIVED_KEY_LEN] {
    crate::crypto::kdf::derive_key(
        secret.as_bytes(),
        crate::crypto::kdf::purpose::TEAMS_MEETING_CAPTURE,
    )
}
pub(crate) fn mint_intent(
    secret: &str,
    tenant: &str,
    aad: &str,
    meeting: &str,
    installation: &str,
    event: &str,
    event_at: chrono::DateTime<chrono::Utc>,
) -> Result<String, ()> {
    let c = MeetingIntent {
        tenant: tenant.into(),
        aad: aad.into(),
        meeting_id: meeting.into(),
        installation_id: installation.into(),
        event_id: event.into(),
        event_at,
        aud: "teams-meeting-capture".into(),
        exp: chrono::Utc::now().timestamp() + 600,
    };
    jsonwebtoken::encode(
        &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::HS256),
        &c,
        &jsonwebtoken::EncodingKey::from_secret(&intent_key(secret)),
    )
    .map_err(|_| ())
}
pub(crate) fn verify_intent(secret: &str, ticket: &str) -> Result<MeetingIntent, ()> {
    let mut v = jsonwebtoken::Validation::new(jsonwebtoken::Algorithm::HS256);
    v.set_audience(&["teams-meeting-capture"]);
    let c = jsonwebtoken::decode::<MeetingIntent>(
        ticket,
        &jsonwebtoken::DecodingKey::from_secret(&intent_key(secret)),
        &v,
    )
    .map_err(|_| ())?
    .claims;
    if Uuid::parse_str(&c.tenant).is_err()
        || Uuid::parse_str(&c.aad).is_err()
        || c.meeting_id.is_empty()
        || c.installation_id.is_empty()
    {
        return Err(());
    }
    Ok(c)
}

/// Narrow global signed-provider routing index; unlike consent/content this has no user payload.
pub(crate) async fn store_context(
    control: &PgPool,
    tenant: &str,
    meeting: &str,
    chat: &str,
    graph: &str,
    service: &str,
    aad: &str,
    event_at: chrono::DateTime<chrono::Utc>,
    event: &str,
) -> sqlx::Result<bool> {
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(control, Uuid::nil())
        .await
        .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
    let changed=sqlx::query("INSERT INTO teams_meeting_contexts(tenant_id,meeting_context_id,conversation_id,graph_meeting_id,service_url,installer_aad,active,event_at,event_id) VALUES($1,$2,$3,$4,$5,$6,true,$7,$8) ON CONFLICT(tenant_id,meeting_context_id) DO UPDATE SET conversation_id=EXCLUDED.conversation_id,graph_meeting_id=EXCLUDED.graph_meeting_id,service_url=EXCLUDED.service_url,installer_aad=EXCLUDED.installer_aad,active=true,authority_known=true,event_at=EXCLUDED.event_at,event_id=EXCLUDED.event_id,updated_at=now() WHERE NOT teams_meeting_contexts.authority_known OR EXCLUDED.event_at > teams_meeting_contexts.event_at OR (EXCLUDED.event_id=teams_meeting_contexts.event_id AND teams_meeting_contexts.active)")
 .bind(tenant).bind(meeting).bind(chat).bind(graph).bind(service).bind(aad).bind(event_at).bind(event).execute(&mut *tx).await?.rows_affected()>0;
    tx.commit().await?;
    Ok(changed)
}
pub(crate) async fn remove_context(
    control: &PgPool,
    tenant: &str,
    meeting: &str,
    event_at: chrono::DateTime<chrono::Utc>,
    event: &str,
) -> sqlx::Result<bool> {
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(control, Uuid::nil())
        .await
        .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
    let changed=sqlx::query("INSERT INTO teams_meeting_contexts(tenant_id,meeting_context_id,conversation_id,graph_meeting_id,service_url,installer_aad,active,event_at,event_id) VALUES($1,$2,'','','','',false,$3,$4) ON CONFLICT(tenant_id,meeting_context_id) DO UPDATE SET active=false,authority_known=true,event_at=EXCLUDED.event_at,event_id=EXCLUDED.event_id,updated_at=now() WHERE NOT teams_meeting_contexts.authority_known OR EXCLUDED.event_at >= teams_meeting_contexts.event_at")
 .bind(tenant).bind(meeting).bind(event_at).bind(event).execute(&mut *tx).await?.rows_affected()>0;
    tx.commit().await?;
    Ok(changed)
}
pub(crate) async fn remove_installations(
    pool: &PgPool,
    account: Uuid,
    tenant: &str,
    context: &str,
) -> sqlx::Result<()> {
    let mut tx = crate::tenancy::scope::begin_account_scoped(pool, account)
        .await
        .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
    sqlx::query("UPDATE teams_meeting_installations SET active=false,updated_at=now() WHERE account_id=$1 AND tenant_id=$2 AND context_id=$3").bind(account).bind(tenant).bind(context).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(())
}
pub(crate) async fn context(
    control: &PgPool,
    tenant: &str,
    meeting: &str,
    chat: Option<&str>,
) -> sqlx::Result<Option<(String, String, String)>> {
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(control, Uuid::nil())
        .await
        .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
    let row=sqlx::query_as("SELECT graph_meeting_id,installer_aad,event_id FROM teams_meeting_contexts WHERE tenant_id=$1 AND meeting_context_id=$2 AND active AND ($3::text IS NULL OR conversation_id=$3)").bind(tenant).bind(meeting).bind(chat).fetch_optional(&mut *tx).await?;
    tx.rollback().await?;
    Ok(row)
}

pub(crate) async fn matches_occurrence(
    pool: &PgPool,
    account: Uuid,
    member: Uuid,
    tenant: &str,
    aad: &str,
    meeting: &str,
    start: chrono::DateTime<chrono::Utc>,
    end: chrono::DateTime<chrono::Utc>,
) -> sqlx::Result<Option<Uuid>> {
    let mut tx = begin_tenant_scoped(pool, account, member)
        .await
        .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
    let graph=sqlx::query_scalar("SELECT c.graph_id FROM teams_meeting_captures c JOIN graphs g ON g.id=c.graph_id WHERE c.account_id=$1 AND c.user_id=$2 AND c.tenant_id=$3 AND c.meeting_id=$4 AND c.aad_id=$7 AND c.revoked_at IS NULL AND c.occurrence_start=$5 AND c.occurrence_end=$6 AND g.account_id=$1 AND g.archived_at IS NULL AND ((g.kind='member' AND g.owner_user_id=$2) OR (g.kind='personal' AND g.owner_user_id IS NULL)) AND (c.source <> 'meeting_bot' OR EXISTS(SELECT 1 FROM teams_meeting_installations i WHERE i.account_id=c.account_id AND i.tenant_id=c.tenant_id AND i.installation_id=c.installation_id AND i.active))")
 .bind(account).bind(member).bind(tenant).bind(meeting).bind(start).bind(end).bind(aad).fetch_optional(&mut *tx).await?;
    tx.rollback().await?;
    Ok(graph)
}
#[cfg(test)]
#[path = "teams_meeting_capture_tests.rs"]
mod tests;

/// A duplicate membership notification can fill missing context metadata but
/// must never replace the installation generation or invalidate its setup link.
pub(crate) async fn store_context_metadata(
    control: &PgPool,
    tenant: &str,
    meeting: &str,
    chat: &str,
    graph: &str,
    service: &str,
    aad: &str,
    event_at: chrono::DateTime<chrono::Utc>,
    event: &str,
) -> sqlx::Result<()> {
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(control, Uuid::nil())
        .await
        .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
    sqlx::query("INSERT INTO teams_meeting_contexts(tenant_id,meeting_context_id,conversation_id,graph_meeting_id,service_url,installer_aad,active,authority_known,event_at,event_id) VALUES($1,$2,$3,$4,$5,$6,false,false,$7,$8) ON CONFLICT(tenant_id,meeting_context_id) DO NOTHING")
 .bind(tenant).bind(meeting).bind(chat).bind(graph).bind(service).bind(aad).bind(event_at).bind(event).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(())
}
