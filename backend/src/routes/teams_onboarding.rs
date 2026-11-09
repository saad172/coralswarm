//! Teams-native enrollment. Targets are minted only from authenticated bot
//! activities; the browser chooses a CoralSwarm workspace, never a Teams actor.
use crate::{
    connectors::{microsoft, teams_doorbell, ConnectorProvider, MicrosoftConnector},
    middleware::auth::UserContext,
    oauth::state as oauth_state,
    tenancy::extractor::Tenant,
    AppState,
};
use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    Extension, Json,
};
use chrono::{DateTime, Utc};
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;
use uuid::Uuid;

const PURPOSE: &str = "teams-member-onboarding";
const TTL: i64 = oauth_state::STATE_TTL_SECS;
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Target {
    pub tenant: String,
    pub aad: String,
    pub kind: String,
    pub team: String,
    pub channel: String,
    pub chat: String,
    #[serde(skip)]
    pub intent_nonce: Option<String>,
    #[serde(skip)]
    pub activity_id: Option<String>,
}
#[derive(Serialize, Deserialize)]
struct Claims {
    aud: String,
    exp: i64,
    jti: String,
    target: Target,
}
/// HS256 key for onboarding tickets: the HKDF `teams-member-onboarding`
/// derivation of the shared state secret (`crypto::kdf`), never the secret
/// itself and never a string-concatenated variant of it.
fn key(secret: &str) -> [u8; crate::crypto::kdf::DERIVED_KEY_LEN] {
    crate::crypto::kdf::derive_key(
        secret.as_bytes(),
        crate::crypto::kdf::purpose::TEAMS_MEMBER_ONBOARDING,
    )
}
pub(crate) fn mint(secret: &str, target: Target, now: i64) -> Result<String, ()> {
    validate(&target)?;
    jsonwebtoken::encode(
        &Header::new(Algorithm::HS256),
        &Claims {
            aud: PURPOSE.into(),
            exp: now + TTL,
            jti: Uuid::new_v4().to_string(),
            target,
        },
        &EncodingKey::from_secret(&key(secret)),
    )
    .map_err(|_| ())
}
pub(crate) fn verify(secret: &str, ticket: &str) -> Result<Target, ()> {
    let mut validation = Validation::new(Algorithm::HS256);
    validation.set_audience(&[PURPOSE]);
    let claims = jsonwebtoken::decode::<Claims>(
        ticket,
        &DecodingKey::from_secret(&key(secret)),
        &validation,
    )
    .map_err(|_| ())?
    .claims;
    validate(&claims.target)?;
    Uuid::parse_str(&claims.jti).map_err(|_| ())?;
    let mut target = claims.target;
    target.intent_nonce = Some(claims.jti);
    Ok(target)
}
fn validate(target: &Target) -> Result<(), ()> {
    Uuid::parse_str(&target.tenant).map_err(|_| ())?;
    Uuid::parse_str(&target.aad).map_err(|_| ())?;
    match target.kind.as_str() {
        "channel"
            if !target.team.is_empty() && !target.channel.is_empty() && target.chat.is_empty() =>
        {
            Ok(())
        }
        "chat"
            if !target.chat.is_empty() && target.team.is_empty() && target.channel.is_empty() =>
        {
            Ok(())
        }
        _ => Err(()),
    }
}
#[derive(Debug)]
pub(crate) enum Error {
    Membership,
    Identity,
    Unavailable,
    InvalidIntent,
}
#[derive(Serialize)]
pub(crate) struct Activation {
    pub status: &'static str,
    pub watch_id: Uuid,
    pub created: bool,
    pub capture_since: Option<DateTime<Utc>>,
}
#[derive(Deserialize)]
pub struct Request {
    ticket: String,
}
fn error(status: StatusCode, code: &str, message: &str) -> Response {
    (status, Json(json!({"error":message,"code":code}))).into_response()
}

/// Read only the current member's live grant; an install index is not authority.
pub(crate) async fn identity(
    control: &sqlx::PgPool,
    account: Uuid,
    member: Uuid,
) -> sqlx::Result<Option<(String, String)>> {
    let mut tx = crate::tenancy::scope::begin_request_account_read(control, account)
        .await
        .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
    let row = sqlx::query_as(
        "SELECT provider_workspace_id, provider_user_id FROM oauth_tokens \
        WHERE account_id=$1 AND owner_user_id=$2 AND provider='microsoft' \
        AND credential_invalid_at IS NULL",
    )
    .bind(account)
    .bind(member)
    .fetch_optional(&mut *tx)
    .await?;
    tx.rollback().await?;
    Ok(row)
}
/// Microsoft's fixed directory ID for consumer (MSA) identities. Graph's transcript
/// APIs support delegated work/school identities only, so a grant issued by this
/// directory can never read a meeting transcript.
const MICROSOFT_CONSUMER_TENANT: &str = "9188040d-6c67-4c5b-b112-36a304b66dad";

/// Which authority a private meeting capture runs under. Derived from the
/// account's authoritative metadata, never from a selector the caller supplied.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CapturePlane {
    /// Organization account: the member's own grant, live membership and org flag.
    Org { member: Uuid },
    /// Personal account: its real owner and the account-wide work/school grant.
    /// `owner` is `accounts.owner_user_id`, never the ownerless credential row.
    Personal { owner: Uuid },
}

/// Which plane the caller's request context already resolved to. A mismatch with
/// the account's own metadata is refused rather than reconciled.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ExpectedPlane {
    Any,
    Org,
    Personal,
}

/// The verified Microsoft identity that may capture for `plane`'s user.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CaptureIdentity {
    pub plane: CapturePlane,
    pub tenant: String,
    pub aad: String,
}

#[derive(Debug)]
pub(crate) enum CaptureIdentityError {
    /// No current, owned grant for this caller (or the plane/owner does not match).
    NotConnected,
    /// The grant is a consumer Microsoft account; Graph exposes no transcripts for it.
    ConsumerUnsupported,
    Unavailable(sqlx::Error),
}

impl CaptureIdentity {
    pub(crate) fn user(&self) -> Uuid {
        match self.plane {
            CapturePlane::Org { member } => member,
            CapturePlane::Personal { owner } => owner,
        }
    }
    pub(crate) fn pair(&self) -> (String, String) {
        (self.tenant.clone(), self.aad.clone())
    }
}

/// Resolve the exact grant a private meeting capture may use for `user` in `account`.
///
/// * Org account: the unchanged member identity ([`identity`]); live membership and
///   the org flag remain the callers' existing gates.
/// * Personal account: only when `user` is the account's own live owner, and only
///   from the single ownerless (account-wide) Microsoft row. That row must be a
///   current work/school grant with a tenant/object pair. Another member's grant,
///   an org grant or a second ambiguous row never substitutes for it.
pub(crate) async fn capture_identity(
    control: &sqlx::PgPool,
    account: Uuid,
    user: Uuid,
    expected: ExpectedPlane,
) -> Result<CaptureIdentity, CaptureIdentityError> {
    let unavailable = CaptureIdentityError::Unavailable;
    let row: Option<(Option<String>, Uuid, bool)> = sqlx::query_as(
        "SELECT a.clerk_org_id, a.owner_user_id, u.deleted_at IS NOT NULL \
         FROM accounts a, users u WHERE a.id=$1 AND u.id=$2",
    )
    .bind(account)
    .bind(user)
    .fetch_optional(control)
    .await
    .map_err(unavailable)?;
    let Some((clerk_org_id, owner, user_deleted)) = row else {
        return Err(CaptureIdentityError::NotConnected);
    };
    if user_deleted {
        return Err(CaptureIdentityError::NotConnected);
    }
    match (clerk_org_id.is_some(), expected) {
        (true, ExpectedPlane::Personal) | (false, ExpectedPlane::Org) => {
            return Err(CaptureIdentityError::NotConnected)
        }
        _ => {}
    }
    if clerk_org_id.is_some() {
        return match identity(control, account, user)
            .await
            .map_err(CaptureIdentityError::Unavailable)?
        {
            Some((tenant, aad)) => Ok(CaptureIdentity {
                plane: CapturePlane::Org { member: user },
                tenant,
                aad,
            }),
            None => Err(CaptureIdentityError::NotConnected),
        };
    }
    if owner != user {
        return Err(CaptureIdentityError::NotConnected);
    }
    let mut tx = crate::tenancy::scope::begin_account_scoped(control, account)
        .await
        .map_err(|e| CaptureIdentityError::Unavailable(sqlx::Error::Protocol(e.to_string())))?;
    let grants: Vec<(Option<String>, Option<String>, bool)> = sqlx::query_as(
        "SELECT provider_workspace_id, provider_user_id, credential_invalid_at IS NOT NULL \
         FROM oauth_tokens WHERE account_id=$1 AND provider='microsoft' AND owner_user_id IS NULL",
    )
    .bind(account)
    .fetch_all(&mut *tx)
    .await
    .map_err(CaptureIdentityError::Unavailable)?;
    tx.rollback()
        .await
        .map_err(CaptureIdentityError::Unavailable)?;
    let [(Some(tenant), Some(aad), invalid)] = grants.as_slice() else {
        return Err(CaptureIdentityError::NotConnected);
    };
    if Uuid::parse_str(tenant).is_err() || Uuid::parse_str(aad).is_err() {
        return Err(CaptureIdentityError::NotConnected);
    }
    if tenant.eq_ignore_ascii_case(MICROSOFT_CONSUMER_TENANT) {
        return Err(CaptureIdentityError::ConsumerUnsupported);
    }
    if *invalid {
        return Err(CaptureIdentityError::NotConnected);
    }
    Ok(CaptureIdentity {
        plane: CapturePlane::Personal { owner },
        tenant: tenant.clone(),
        aad: aad.clone(),
    })
}

pub(crate) fn matches_identity(target: &Target, tenant: &str, aad: &str) -> bool {
    Uuid::parse_str(tenant).ok() == Uuid::parse_str(&target.tenant).ok()
        && Uuid::parse_str(aad).ok() == Uuid::parse_str(&target.aad).ok()
        && Uuid::parse_str(aad).is_ok()
        && Uuid::parse_str(tenant).is_ok()
}
async fn membership(state: &Arc<AppState>, account: Uuid, member: Uuid) -> Result<String, Error> {
    let control = state.control_db().ok_or(Error::Unavailable)?;
    let org: Option<String> = sqlx::query_scalar("SELECT clerk_org_id FROM accounts WHERE id=$1")
        .bind(account)
        .fetch_one(control)
        .await
        .map_err(|_| Error::Unavailable)?;
    let org = org.ok_or(Error::Membership)?;
    if !matches!(crate::middleware::auth::live_org_membership(control,&org,member).await,
        Ok(Some((id,_))) if id==account)
        || !crate::routes::teams_ingest::org_microsoft_teams_enabled_for(state, &org).await
    {
        return Err(Error::Membership);
    }
    Ok(org)
}

pub async fn onboard(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(ctx): Extension<UserContext>,
    Json(request): Json<Request>,
) -> Response {
    if tenant.org_context().is_none() {
        return error(
            StatusCode::CONFLICT,
            "organization_required",
            "Choose your organization workspace to finish Teams setup.",
        );
    }
    let Some(secret) = oauth_state::state_secret() else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "capture_unavailable",
            "Teams setup is temporarily unavailable.",
        );
    };
    let target = match verify(&secret, &request.ticket) {
        Ok(target) => target,
        Err(()) => {
            return error(
                StatusCode::BAD_REQUEST,
                "invalid_intent",
                "This Teams setup link is invalid or expired. Mention CoralSwarm again.",
            )
        }
    };
    if membership(&state, ctx.account_id, ctx.user_id)
        .await
        .is_err()
    {
        return error(
            StatusCode::FORBIDDEN,
            "membership_required",
            "Teams capture is not available in this workspace.",
        );
    }
    let Some(control) = state.control_db() else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "capture_unavailable",
            "Teams setup is temporarily unavailable.",
        );
    };
    match identity(control, ctx.account_id, ctx.user_id).await {
        Ok(Some((tid, aad))) if !matches_identity(&target, &tid, &aad) => error(
            StatusCode::CONFLICT,
            "microsoft_identity_mismatch",
            "Connect the Microsoft identity that mentioned CoralSwarm, then try this link again.",
        ),
        Ok(Some(_)) => match activate(&state, ctx.account_id, ctx.user_id, &target).await {
            Ok(result) => Json(result).into_response(),
            Err(e) => activation_error(e),
        },
        Ok(None) => {
            let signed = match oauth_state::sign_state_with_teams_onboarding(
                &secret,
                ctx.account_id,
                Utc::now().timestamp(),
                &Uuid::new_v4().to_string(),
                Some(ctx.user_id),
                None,
                Some(&request.ticket),
            ) {
                Ok(s) => s,
                Err(_) => {
                    return error(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "capture_unavailable",
                        "Teams setup is temporarily unavailable.",
                    )
                }
            };
            match MicrosoftConnector.authorize_url(&state.api_base_url(), &signed) {
                Ok(url) => {
                    let mut url = match reqwest::Url::parse(&url) {
                        Ok(url) => url,
                        Err(_) => {
                            return error(
                                StatusCode::SERVICE_UNAVAILABLE,
                                "capture_unavailable",
                                "Microsoft connection is temporarily unavailable.",
                            )
                        }
                    };
                    url.set_path(&format!("/{}/oauth2/v2.0/authorize", target.tenant));
                    Json(json!({"status":"connect_required","authorization_url":url.as_str()}))
                        .into_response()
                }
                Err(_) => error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "capture_unavailable",
                    "Microsoft connection is temporarily unavailable.",
                ),
            }
        }
        Err(_) => error(
            StatusCode::SERVICE_UNAVAILABLE,
            "capture_unavailable",
            "Teams setup is temporarily unavailable.",
        ),
    }
}
fn activation_error(e: Error) -> Response {
    match e {
        Error::InvalidIntent => error(
            StatusCode::BAD_REQUEST,
            "invalid_intent",
            "This setup link has already been used. Mention CoralSwarm again to activate capture.",
        ),
        Error::Membership => error(
            StatusCode::FORBIDDEN,
            "membership_required",
            "Your organization membership is required.",
        ),
        Error::Identity => error(
            StatusCode::CONFLICT,
            "microsoft_identity_mismatch",
            "The Microsoft identity does not match this Teams setup link.",
        ),
        Error::Unavailable => error(
            StatusCode::SERVICE_UNAVAILABLE,
            "capture_unavailable",
            "Capture could not be activated. Retry this setup link.",
        ),
    }
}

pub(crate) async fn activate(
    state: &Arc<AppState>,
    account: Uuid,
    member: Uuid,
    target: &Target,
) -> Result<Activation, Error> {
    if audit_source(target).is_none() {
        return Err(Error::InvalidIntent);
    }
    let org = membership(state, account, member).await?;
    let control = state.control_db().ok_or(Error::Unavailable)?;
    let Some((tid, aad)) = identity(control, account, member)
        .await
        .map_err(|_| Error::Unavailable)?
    else {
        return Err(Error::Identity);
    };
    if !matches_identity(target, &tid, &aad) {
        return Err(Error::Identity);
    }
    let (token, _) = crate::routes::teams_ingest::microsoft_token(state, account, member)
        .await
        .map_err(|_| Error::Unavailable)?;
    // Graph supplies the AAD team GUID. Teams activities sometimes carry its
    // opaque conversation id instead: discover metadata using ONLY this member's grant.
    let (team, name) = if target.kind == "channel" {
        let teams = microsoft::list_joined_teams(&state.http_client, &token)
            .await
            .map_err(|_| Error::Unavailable)?;
        let mut matches = Vec::new();
        for team in teams {
            if Uuid::parse_str(&target.team).is_ok() && team.id != target.team {
                continue;
            }
            let channels = microsoft::list_channels(&state.http_client, &token, &team.id)
                .await
                .map_err(|_| Error::Unavailable)?;
            for channel in channels {
                if channel.id == target.channel {
                    matches.push((team.id.clone(), channel.display_name));
                }
            }
        }
        match matches.as_slice() {
            [only] => only.clone(),
            _ => return Err(Error::Unavailable),
        }
    } else {
        // A successful exact-chat Graph read is required even when another member
        // already owns the shared subscription. It never authorizes that member's content.
        let response = state
            .http_client
            .get(format!(
                "https://graph.microsoft.com/v1.0/chats/{}?$select=id,topic",
                urlencoding::encode(&target.chat)
            ))
            .bearer_auth(&token)
            .send()
            .await
            .map_err(|_| Error::Unavailable)?;
        if !response.status().is_success() {
            return Err(Error::Unavailable);
        }
        (String::new(), "Teams chat".to_owned())
    };
    let resource = if target.kind == "channel" {
        format!("teams/{team}/channels/{}/messages", target.channel)
    } else {
        format!("chats/{}/messages", target.chat)
    };
    let content = crate::routes::graph_webhook::resolve_org_tenant_pool_for_graph_ingest(
        state, control, &org,
    )
    .await
    .ok_or(Error::Unavailable)?;
    let graph = match crate::routes::slack::resolve_primary_graph(
        &content,
        account,
        crate::routes::slack::AccountPlane::Org,
        crate::routes::slack::IngestCaller::Attributable(member),
    )
    .await
    {
        Ok(graph) => graph,
        Err(_) if crate::tenancy::member_ocean::enabled() => {
            let mut lookup = crate::tenancy::scope::begin_tenant_scoped(&content, account, member)
                .await
                .map_err(|_| Error::Unavailable)?;
            let exists:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM graphs WHERE account_id=$1 AND owner_user_id=$2 AND kind='member' AND is_primary AND archived_at IS NULL)")
                .bind(account).bind(member).fetch_one(&mut *lookup).await.map_err(|_|Error::Unavailable)?;
            lookup.rollback().await.map_err(|_| Error::Unavailable)?;
            if exists {
                return Err(Error::Unavailable);
            }
            tokio::time::timeout(
                std::time::Duration::from_secs(5),
                crate::tenancy::member_ocean::provision_member_scoped(
                    &content, control, account, member,
                ),
            )
            .await
            .map_err(|_| Error::Unavailable)?
            .map_err(|_| Error::Unavailable)?;
            crate::routes::slack::resolve_primary_graph(
                &content,
                account,
                crate::routes::slack::AccountPlane::Org,
                crate::routes::slack::IngestCaller::Attributable(member),
            )
            .await
            .map_err(|_| Error::Unavailable)?
        }
        Err(_) => return Err(Error::Unavailable),
    };
    let may_create = match target.intent_nonce.as_deref() {
        Some(nonce) => {
            crate::oauth::jti_store::consume(control, &format!("teams-onboarding:{nonce}"))
                .await
                .map_err(|_| Error::Unavailable)?
        }
        None => true, // A fresh authenticated mention is itself new consent.
    };
    let (watch_id, capture_since, created) = persist_watch(
        &content, account, member, graph, target, &team, &name, &tid, may_create,
    )
    .await?;
    let pending = target
        .intent_nonce
        .as_deref()
        .map(|nonce| format!("teams-onboarding:pending:{nonce}:{account}:{member}:{watch_id}"));
    let may_enroll = match pending.as_deref() {
        Some(key) => pending_enrollment(control, key, may_create).await?,
        None => true,
    };
    if may_enroll {
        // Retry of a partial activation preserves the original cutoff and history.
        let dek = crate::crypto::DekPlane::resolve_for_account(state, control, account)
            .await
            .map_err(|_| Error::Unavailable)?;
        teams_doorbell::ensure_delegated_container(
            &state.http_client,
            control,
            &dek.as_source(),
            &resource,
            account,
            member,
        )
        .await
        .map_err(|_| Error::Unavailable)?;
    }
    membership(state, account, member).await?;
    let still_valid = identity(control, account, member)
        .await
        .map_err(|_| Error::Unavailable)?;
    if !matches!(still_valid,Some((ref tid,ref aad)) if matches_identity(target,tid,aad)) {
        return Err(Error::Identity);
    }
    let enrolled:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM teams_container_watchers WHERE ms_tenant_id=$1 AND resource=$2 AND account_id=$3 AND user_id=$4)")
        .bind(&tid).bind(&resource).bind(account).bind(member).fetch_one(control).await.map_err(|_|Error::Unavailable)?;
    if !enrolled {
        return Err(Error::Unavailable);
    }
    // Never report active before the verified enrollment's durable audit exists.
    // Provider redelivery and setup-link retries share their source event ID.
    // A fresh signed mention records a distinct verification/re-enrollment.
    let mut audit_tx = crate::tenancy::scope::begin_tenant_scoped(&content, account, member)
        .await
        .map_err(|_| Error::Unavailable)?;
    audit_in_tx(
        &mut audit_tx,
        account,
        member,
        graph,
        watch_id,
        target,
        &team,
        "enrollment_verified",
        capture_since,
    )
    .await
    .map_err(|_| Error::Unavailable)?;
    audit_tx.commit().await.map_err(|_| Error::Unavailable)?;
    if may_enroll {
        if let Some(key) = pending.as_deref() {
            complete_enrollment(control, key).await?;
        }
    }
    Ok(Activation {
        status: "active",
        watch_id,
        created,
        capture_since,
    })
}

// Stable identifiers provide retry deduplication without persisting a setup
// ticket, OAuth code, token, message body, or any bearer material.
fn audit_source(target: &Target) -> Option<String> {
    if let Some(nonce) = target.intent_nonce.as_deref().filter(|s| !s.is_empty()) {
        Some(format!("verified_setup_link:{nonce}"))
    } else {
        target
            .activity_id
            .as_deref()
            .filter(|s| !s.is_empty())
            .map(|id| format!("signed_mention:{id}"))
    }
}
fn audit_event_id(account: Uuid, member: Uuid, watch: Uuid, phase: &str, source: &str) -> Uuid {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(format!(
        "teams-mention-audit:v1:{account}:{member}:{watch}:{phase}:{source}"
    ));
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&digest[..16]);
    Uuid::from_bytes(bytes)
}
async fn audit_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    account: Uuid,
    member: Uuid,
    graph: Uuid,
    watch: Uuid,
    target: &Target,
    team: &str,
    phase: &'static str,
    capture_since: Option<DateTime<Utc>>,
) -> sqlx::Result<()> {
    let source = if phase == "selected_pending_enrollment" {
        String::new()
    } else {
        audit_source(target).ok_or_else(|| {
            sqlx::Error::Protocol("verified onboarding audit source missing".into())
        })?
    };
    crate::services::connector_audit::record_once_in_tx(tx,
        audit_event_id(account, member, watch, phase, &source),
        crate::services::connector_audit::AuditEntry {
            account_id: account,
            actor: crate::services::connector_audit::Actor::User(member),
            action: if phase == "selected_pending_enrollment" { "route_container" } else { "consent" },
            provider: Some("microsoft".into()),
            subject: Some(watch.to_string()),
            detail: Some(json!({"operation":"teams_capture_activation", "source":if target.intent_nonce.is_some() { "verified_setup_link" } else { "signed_mention" }, "phase":phase,
                "ms_tenant_id":target.tenant,"aad_actor_id":target.aad,
                "kind":target.kind,"team_id":team,"channel_id":target.channel,
                "chat_id":target.chat,"watch_id":watch,"private_graph_id":graph,
                "auth_mode":"delegated", "capture_since":capture_since,
                "history_policy":if capture_since.is_some() { "from_now" } else { "existing_history_preserved" }})),
        }).await
}

async fn pending_enrollment(
    control: &sqlx::PgPool,
    key: &str,
    first_use: bool,
) -> Result<bool, Error> {
    if first_use {
        crate::oauth::jti_store::consume(control, key)
            .await
            .map_err(|_| Error::Unavailable)?;
        return Ok(true);
    }
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM oauth_state_jti WHERE jti=$1)")
        .bind(key)
        .fetch_one(control)
        .await
        .map_err(|_| Error::Unavailable)
}
async fn complete_enrollment(control: &sqlx::PgPool, key: &str) -> Result<(), Error> {
    sqlx::query("DELETE FROM oauth_state_jti WHERE jti=$1")
        .bind(key)
        .execute(control)
        .await
        .map_err(|_| Error::Unavailable)?;
    Ok(())
}

async fn persist_watch(
    content: &sqlx::PgPool,
    account: Uuid,
    member: Uuid,
    graph: Uuid,
    target: &Target,
    team: &str,
    name: &str,
    tid: &str,
    may_create: bool,
) -> Result<(Uuid, Option<DateTime<Utc>>, bool), Error> {
    let mut tx = crate::tenancy::scope::begin_tenant_scoped(content, account, member)
        .await
        .map_err(|_| Error::Unavailable)?;
    let inserted: Option<(Uuid, DateTime<Utc>)> = if may_create {
        sqlx::query_as("INSERT INTO teams_watched_resources \
        (account_id,user_id,graph_id,kind,team_id,channel_id,chat_id,display_name,auth_mode,tenant_id,history_choice,backfill_complete) \
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,'delegated',$9,'none',TRUE) \
        ON CONFLICT(account_id,kind,team_id,channel_id,chat_id,user_id) WHERE auth_mode='delegated' \
        DO NOTHING RETURNING id,created_at")
        .bind(account).bind(member).bind(graph).bind(&target.kind).bind(&team).bind(&target.channel)
        .bind(&target.chat).bind(name).bind(&tid).fetch_optional(&mut *tx).await.map_err(|_|Error::Unavailable)?
    } else {
        None
    };
    let (watch_id, capture_since) = if let Some((id, since)) = inserted {
        (id, Some(since))
    } else {
        sqlx::query_as("SELECT id,CASE WHEN history_choice='none' THEN created_at ELSE NULL END \
            FROM teams_watched_resources WHERE account_id=$1 AND user_id=$2 AND auth_mode='delegated' \
            AND kind=$3 AND team_id=$4 AND channel_id=$5 AND chat_id=$6 AND (tenant_id='' OR tenant_id=$7) AND graph_id=$8")
            .bind(account).bind(member).bind(&target.kind).bind(&team).bind(&target.channel).bind(&target.chat).bind(&tid).bind(graph)
            .fetch_optional(&mut *tx).await.map_err(|_|Error::Unavailable)?.ok_or(Error::InvalidIntent)?
    };
    if inserted.is_some() {
        // A failed audit write rolls back capture selection as well.
        audit_in_tx(
            &mut tx,
            account,
            member,
            graph,
            watch_id,
            target,
            team,
            "selected_pending_enrollment",
            capture_since,
        )
        .await
        .map_err(|_| Error::Unavailable)?;
    }
    tx.commit().await.map_err(|_| Error::Unavailable)?;
    Ok((watch_id, capture_since, inserted.is_some()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::Executor;
    fn target() -> Target {
        Target {
            tenant: Uuid::new_v4().to_string(),
            aad: Uuid::new_v4().to_string(),
            kind: "channel".into(),
            team: Uuid::new_v4().to_string(),
            channel: "channel".into(),
            chat: String::new(),
            intent_nonce: None,
            activity_id: Some("test-message-1".into()),
        }
    }
    #[test]
    fn teams_onboarding_ticket_is_purpose_bound_and_actor_exact() {
        let target = target();
        let a = Uuid::new_v4();
        let m = Uuid::new_v4();
        let w = Uuid::new_v4();
        let source = audit_source(&target).unwrap();
        assert_eq!(
            audit_event_id(a, m, w, "enrollment_verified", &source),
            audit_event_id(a, m, w, "enrollment_verified", &source)
        );
        assert_ne!(
            audit_event_id(a, m, w, "enrollment_verified", &source),
            audit_event_id(
                a,
                m,
                w,
                "enrollment_verified",
                "signed_mention:another-message"
            )
        );
        assert_ne!(
            audit_event_id(a, m, w, "enrollment_verified", &source),
            audit_event_id(
                a,
                m,
                w,
                "enrollment_verified",
                "verified_setup_link:test-message-1"
            )
        );
        let ticket = mint("test-secret", target.clone(), Utc::now().timestamp()).unwrap();
        let verified = verify("test-secret", &ticket).unwrap();
        assert!(verified.intent_nonce.is_some());
        assert!(matches_identity(&verified, &target.tenant, &target.aad));
        assert!(!matches_identity(
            &verified,
            &Uuid::new_v4().to_string(),
            &target.aad
        ));
        assert!(!matches_identity(
            &verified,
            &target.tenant,
            &Uuid::new_v4().to_string()
        ));
        assert!(!matches_identity(&verified, "", ""));
        assert!(verify("wrong-secret", &ticket).is_err());
        assert!(oauth_state::verify_state("test-secret", &ticket).is_err());
        let oauth = oauth_state::sign_state_with_teams_onboarding(
            "test-secret",
            Uuid::new_v4(),
            Utc::now().timestamp(),
            "nonce",
            Some(Uuid::new_v4()),
            None,
            Some(&ticket),
        )
        .unwrap();
        assert_eq!(
            oauth_state::verify_state("test-secret", &oauth)
                .unwrap()
                .teams_onboarding,
            Some(ticket)
        );
        assert!(verify("test-secret", &oauth).is_err());
        assert!(verify(
            "test-secret",
            &mint("test-secret", target, Utc::now().timestamp() - TTL - 120).unwrap()
        )
        .is_err());
    }
    #[tokio::test]
    async fn teams_onboarding_preserves_own_watch_and_consumed_target_cannot_recreate_pg() {
        let base = std::env::var("ORGT_TEST_BASE_URL")
            .expect("isolated PostgreSQL required for onboarding regression");
        let mut admin_url = reqwest::Url::parse(&base).unwrap();
        admin_url.set_path("/postgres");
        let admin = sqlx::PgPool::connect(admin_url.as_str()).await.unwrap();
        let database = crate::test_db::TestDatabase::from_url(admin_url.as_str(), "onboarding");
        admin
            .execute(format!("CREATE DATABASE {database}").as_str())
            .await
            .unwrap();
        let mut url = admin_url.clone();
        url.set_path(&format!("/{database}"));
        let fixture = sqlx::PgPool::connect(url.as_str()).await.unwrap();
        fixture.execute("CREATE TABLE teams_watched_resources(
            id uuid DEFAULT gen_random_uuid() PRIMARY KEY, account_id uuid,user_id uuid,graph_id uuid,
            kind text,team_id text,channel_id text,chat_id text,display_name text,auth_mode text,
            tenant_id text,history_choice text,backfill_complete boolean,
            backfill_cursor text,created_at timestamptz DEFAULT now());
            CREATE UNIQUE INDEX delegated_watch ON teams_watched_resources(account_id,kind,team_id,channel_id,chat_id,user_id) WHERE auth_mode='delegated';
            CREATE TABLE oauth_state_jti(jti text PRIMARY KEY,consumed_at timestamptz DEFAULT now());
            CREATE TABLE graphs(id uuid DEFAULT gen_random_uuid() PRIMARY KEY,account_id uuid,name text,kind text,owner_user_id uuid,is_primary boolean,archived_at timestamptz,created_at timestamptz DEFAULT now());
            CREATE UNIQUE INDEX own_primary ON graphs(account_id,owner_user_id) WHERE is_primary AND owner_user_id IS NOT NULL AND archived_at IS NULL;
            ALTER TABLE graphs ENABLE ROW LEVEL SECURITY;
            CREATE POLICY own_graph ON graphs USING(account_id::text=current_setting('app.account_id',true) AND owner_user_id::text=current_setting('app.user_id',true))
                WITH CHECK(account_id::text=current_setting('app.account_id',true) AND owner_user_id::text=current_setting('app.user_id',true));
            ALTER TABLE teams_watched_resources ENABLE ROW LEVEL SECURITY;
            CREATE POLICY member_boundary ON teams_watched_resources USING
                (account_id::text=current_setting('app.account_id',true) AND user_id::text=current_setting('app.user_id',true))
                WITH CHECK(account_id::text=current_setting('app.account_id',true) AND user_id::text=current_setting('app.user_id',true));").await.unwrap();
        // Exercise the deployed tenant audit schema and policies with an app role.
        fixture.execute("CREATE FUNCTION tenant_rls_is_system() RETURNS boolean LANGUAGE sql AS $$ SELECT COALESCE(current_setting('app.tenant_actor',true),'')='system' $$;
CREATE FUNCTION tenant_rls_actor_user_id() RETURNS uuid LANGUAGE sql AS $$ SELECT NULLIF(current_setting('app.user_id',true),'')::uuid $$;").await.unwrap();
        fixture
            .execute(include_str!(
                "../../migrations_tenant/0071_connector_audit_log.sql"
            ))
            .await
            .unwrap();
        let role = database.role("onboard");
        admin
            .execute(format!("CREATE ROLE {role} NOLOGIN NOBYPASSRLS").as_str())
            .await
            .unwrap();
        fixture
            .execute(
                format!(
                    "GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA public TO {role}"
                )
                .as_str(),
            )
            .await
            .unwrap();
        let app_role = role.clone();
        let app = sqlx::postgres::PgPoolOptions::new()
            .after_connect(move |conn, _| {
                let statement = format!("SET ROLE {app_role}");
                Box::pin(async move { conn.execute(statement.as_str()).await.map(|_| ()) })
            })
            .connect(url.as_str())
            .await
            .unwrap();
        let account = Uuid::new_v4();
        let member = Uuid::new_v4();
        let other = Uuid::new_v4();
        let graph = Uuid::new_v4();
        let target = target();
        let provisioned =
            crate::tenancy::member_ocean::provision_member_scoped(&app, &fixture, account, member)
                .await
                .unwrap()
                .expect("fresh member private ocean");
        assert!(crate::tenancy::member_ocean::provision_member_scoped(
            &app, &fixture, account, member
        )
        .await
        .unwrap()
        .is_none());
        assert_eq!(
            crate::routes::slack::resolve_primary_graph(
                &app,
                account,
                crate::routes::slack::AccountPlane::Org,
                crate::routes::slack::IngestCaller::Attributable(member)
            )
            .await
            .unwrap(),
            provisioned
        );
        assert!(crate::routes::slack::resolve_primary_graph(
            &app,
            account,
            crate::routes::slack::AccountPlane::Org,
            crate::routes::slack::IngestCaller::Attributable(other)
        )
        .await
        .is_err());
        let nonce = format!("teams-onboarding:{}", Uuid::new_v4());
        assert!(crate::oauth::jti_store::consume(&app, &nonce)
            .await
            .unwrap());
        let (id, cutoff, created) = persist_watch(
            &app,
            account,
            member,
            graph,
            &target,
            &target.team,
            "channel",
            &target.tenant,
            true,
        )
        .await
        .unwrap();
        assert!(created && cutoff.is_some());
        let selected_count: i64=sqlx::query_scalar("SELECT count(*) FROM connector_audit_log WHERE account_id=$1 AND actor_user_id=$2 AND action='route_container'")
            .bind(account).bind(member).fetch_one(&fixture).await.unwrap();
        assert_eq!(selected_count, 1);
        let mut scoped = crate::tenancy::scope::begin_tenant_scoped(&app, account, member)
            .await
            .unwrap();
        let visible: i64 = sqlx::query_scalar("SELECT count(*) FROM connector_audit_log")
            .fetch_one(&mut *scoped)
            .await
            .unwrap();
        assert_eq!(
            visible, 0,
            "ordinary member cannot read even their own compliance rows"
        );
        audit_in_tx(
            &mut scoped,
            account,
            member,
            graph,
            id,
            &target,
            &target.team,
            "enrollment_verified",
            cutoff,
        )
        .await
        .unwrap();
        audit_in_tx(
            &mut scoped,
            account,
            member,
            graph,
            id,
            &target,
            &target.team,
            "enrollment_verified",
            cutoff,
        )
        .await
        .unwrap();
        scoped.commit().await.unwrap();
        let completion_count:i64=sqlx::query_scalar("SELECT count(*) FROM connector_audit_log WHERE account_id=$1 AND actor_user_id=$2 AND action='consent'")
            .bind(account).bind(member).fetch_one(&fixture).await.unwrap();
        assert_eq!(
            completion_count, 1,
            "completion retry dedupes despite no member SELECT policy"
        );
        let detail: serde_json::Value = sqlx::query_scalar(
            "SELECT detail FROM connector_audit_log WHERE subject=$1 AND action='consent'",
        )
        .bind(id.to_string())
        .fetch_one(&fixture)
        .await
        .unwrap();
        let mut keys: Vec<_> = detail
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        keys.sort();
        assert_eq!(
            keys,
            vec![
                "aad_actor_id",
                "auth_mode",
                "capture_since",
                "channel_id",
                "chat_id",
                "history_policy",
                "kind",
                "ms_tenant_id",
                "operation",
                "phase",
                "private_graph_id",
                "source",
                "team_id",
                "watch_id"
            ]
        );
        assert_eq!(detail["source"], "signed_mention");
        assert_eq!(detail["phase"], "enrollment_verified");
        let ordered:bool=sqlx::query_scalar("SELECT (SELECT occurred_at FROM connector_audit_log WHERE subject=$1 AND action='route_container') <= (SELECT occurred_at FROM connector_audit_log WHERE subject=$1 AND action='consent')")
            .bind(id.to_string()).fetch_one(&fixture).await.unwrap();
        assert!(ordered);
        let mut fresh = target.clone();
        fresh.activity_id = Some("test-message-2".into());
        let mut verify_tx = crate::tenancy::scope::begin_tenant_scoped(&app, account, member)
            .await
            .unwrap();
        audit_in_tx(
            &mut verify_tx,
            account,
            member,
            graph,
            id,
            &fresh,
            &target.team,
            "enrollment_verified",
            cutoff,
        )
        .await
        .unwrap();
        let mut link = target.clone();
        link.activity_id = None;
        link.intent_nonce = Some(Uuid::new_v4().to_string());
        audit_in_tx(
            &mut verify_tx,
            account,
            member,
            graph,
            id,
            &link,
            &target.team,
            "enrollment_verified",
            cutoff,
        )
        .await
        .unwrap();
        audit_in_tx(
            &mut verify_tx,
            account,
            member,
            graph,
            id,
            &link,
            &target.team,
            "enrollment_verified",
            cutoff,
        )
        .await
        .unwrap();
        verify_tx.commit().await.unwrap();
        let verified_count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM connector_audit_log WHERE subject=$1 AND action='consent'",
        )
        .bind(id.to_string())
        .fetch_one(&fixture)
        .await
        .unwrap();
        assert_eq!(
            verified_count, 3,
            "fresh mention is distinct; same setup nonce retry dedupes"
        );
        let mut missing = target.clone();
        missing.activity_id = None;
        let mut missing_tx = crate::tenancy::scope::begin_tenant_scoped(&app, account, member)
            .await
            .unwrap();
        assert!(audit_in_tx(
            &mut missing_tx,
            account,
            member,
            graph,
            id,
            &missing,
            &target.team,
            "enrollment_verified",
            cutoff
        )
        .await
        .is_err());
        missing_tx.rollback().await.unwrap();
        let mut other_scope = crate::tenancy::scope::begin_tenant_scoped(&app, account, other)
            .await
            .unwrap();
        assert!(
            audit_in_tx(
                &mut other_scope,
                account,
                member,
                graph,
                id,
                &target,
                &target.team,
                "forged_actor",
                cutoff,
            )
            .await
            .is_err(),
            "cannot attribute another member"
        );
        other_scope.rollback().await.unwrap();
        let mut failed_target = target.clone();
        failed_target.channel = "audit-rollback-channel".into();
        fixture
            .execute(format!("REVOKE INSERT ON connector_audit_log FROM {role}").as_str())
            .await
            .unwrap();
        assert!(persist_watch(
            &app,
            account,
            member,
            graph,
            &failed_target,
            &target.team,
            "channel",
            &target.tenant,
            true
        )
        .await
        .is_err());
        let rolled_back:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM teams_watched_resources WHERE channel_id='audit-rollback-channel')")
            .fetch_one(&fixture).await.unwrap();
        assert!(
            !rolled_back,
            "failed compliance write rolls back watch creation"
        );
        fixture
            .execute(format!("GRANT INSERT ON connector_audit_log TO {role}").as_str())
            .await
            .unwrap();
        let pending = format!("{nonce}:pending:{account}:{member}:{id}");
        assert!(pending_enrollment(&app, &pending, true).await.unwrap());
        assert!(
            pending_enrollment(&app, &pending, false).await.unwrap(),
            "partial enrollment permits exact pending retry"
        );
        assert!(!pending_enrollment(
            &app,
            &format!("{nonce}:pending:{account}:{other}:{id}"),
            false
        )
        .await
        .unwrap());
        assert!(!pending_enrollment(
            &app,
            &format!("{nonce}:pending:{account}:{member}:{}", Uuid::new_v4()),
            false
        )
        .await
        .unwrap());
        complete_enrollment(&app, &pending).await.unwrap();
        assert!(
            !pending_enrollment(&app, &pending, false).await.unwrap(),
            "completed activation cannot re-enroll a revoked registry"
        );

        assert!(!crate::oauth::jti_store::consume(&app, &nonce)
            .await
            .unwrap());
        let repeated = persist_watch(
            &app,
            account,
            member,
            graph,
            &target,
            &target.team,
            "changed",
            &target.tenant,
            false,
        )
        .await
        .unwrap();
        assert_eq!(repeated, (id, cutoff, false));
        assert!(
            persist_watch(
                &app,
                account,
                member,
                Uuid::new_v4(),
                &target,
                &target.team,
                "changed",
                &target.tenant,
                false
            )
            .await
            .is_err(),
            "a replaced/private-ocean mismatch must not redirect capture"
        );

        // Fresh mentions are idempotent too: no existing history/graph/cursor mutation.
        fixture.execute(format!("UPDATE teams_watched_resources SET history_choice='7d',backfill_complete=FALSE,backfill_cursor='next' WHERE id='{id}'").as_str()).await.unwrap();
        assert_eq!(
            persist_watch(
                &app,
                account,
                member,
                graph,
                &target,
                &target.team,
                "changed",
                &target.tenant,
                true
            )
            .await
            .unwrap(),
            (id, None, false)
        );
        let row: (Uuid, String, String) = sqlx::query_as(
            "SELECT graph_id,display_name,backfill_cursor FROM teams_watched_resources WHERE id=$1",
        )
        .bind(id)
        .fetch_one(&fixture)
        .await
        .unwrap();
        assert_eq!(row, (graph, "channel".into(), "next".into()));
        assert!(persist_watch(
            &app,
            account,
            other,
            graph,
            &target,
            &target.team,
            "channel",
            &target.tenant,
            false
        )
        .await
        .is_err());
        assert!(persist_watch(
            &app,
            account,
            member,
            graph,
            &target,
            &target.team,
            "channel",
            &Uuid::new_v4().to_string(),
            false
        )
        .await
        .is_err());
        fixture
            .execute(format!("DELETE FROM teams_watched_resources WHERE id='{id}'").as_str())
            .await
            .unwrap();
        assert!(matches!(
            persist_watch(
                &app,
                account,
                member,
                graph,
                &target,
                &target.team,
                "channel",
                &target.tenant,
                false
            )
            .await,
            Err(Error::InvalidIntent)
        ));
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM teams_watched_resources")
            .fetch_one(&fixture)
            .await
            .unwrap();
        assert_eq!(count, 0);
        app.close().await;
        fixture.close().await;
        admin
            .execute(format!("DROP DATABASE {database} WITH (FORCE)").as_str())
            .await
            .unwrap();
        admin
            .execute(format!("DROP ROLE {role}").as_str())
            .await
            .unwrap();
        admin.close().await;
    }
}
