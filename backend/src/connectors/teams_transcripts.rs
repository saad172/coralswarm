//! Per-member Teams transcript capture (Phase G item 51).
//!
//! Body reads are bounded by explicit member-private meeting enrollment.
//! Calendar presence, recording, OAuth and ordinary Teams watches do not
//! authorize a transcript. Legacy unowned RSC notifications are ignored;
//! delegated notifications and sweeps check the exact meeting ledger.
//!
//! A tenant that has turned off "Graph API access to transcripts" answers
//! 403 `GraphAccessToTranscriptsDisabled`. That is a skip for the member who
//! hit it, recorded on `connector_health`, and the next pass does not call
//! Graph again for them. Another member's watch is left alone.

use async_trait::async_trait;
use chrono::{Duration, Utc};
use sqlx::PgPool;
use uuid::Uuid;

use super::microsoft::{self, OnlineMeetingCoverage};
use super::{ConnectorError, NormalizedItem, TokenOwner};
use crate::services::connector_health;

/// Which token a transcript read or organiser subscription uses.
pub(crate) fn transcript_token_owner(org_plane: bool, user_id: Uuid) -> TokenOwner {
    if org_plane {
        TokenOwner::Member(user_id)
    } else {
        TokenOwner::AccountWide
    }
}

/// Delegated organiser resource. Not the app-catalog RSC resource.
pub(crate) fn organiser_transcript_resource(graph_user_id: &str) -> String {
    format!("users/{graph_user_id}/onlineMeetings/getAllTranscripts")
}

pub(crate) fn is_delegated_organiser_transcript_resource(resource: &str) -> bool {
    let resource = resource.trim_start_matches('/');
    resource.starts_with("users/") && resource.contains("/onlineMeetings/getAllTranscripts")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MeetingSeat {
    Organiser,
    Attendee,
}

#[derive(Debug, Clone)]
pub(crate) struct TranscriptWatch {
    pub account_id: Uuid,
    pub user_id: Uuid,
    pub graph_user_id: String,
    pub org_plane: bool,
    pub seat: MeetingSeat,
    /// Member ocean the calendar sweep row is anchored to. Required to
    /// schedule an attendee pull.
    pub graph_id: Option<Uuid>,
    /// Attendee path fetches transcripts with this member's token. Organiser
    /// push does not.
    pub pull_attended: bool,
}

#[derive(Debug)]
pub(crate) enum CaptureOutcome {
    Subscribed {
        #[allow(dead_code)]
        resource: String,
    },
    ScheduledSweep {
        items: Vec<NormalizedItem>,
    },
    SkippedDisabled,
}

#[derive(Debug)]
pub(crate) struct CreatedTranscriptSub {
    pub subscription_id: String,
    pub expires_at: chrono::DateTime<Utc>,
}

#[derive(Debug)]
pub(crate) enum TranscriptOpError {
    AccessDisabled,
    /// Any other failure, with the HTTP status Graph answered when there
    /// was one, so the error rebuilt from it ([`TranscriptOpError::
    /// into_connector_error`]) still classifies (a 403 or 404 is permanent
    /// for the receive worker; a 5xx or a timeout retries).
    Other { message: String, status: Option<u16> },
}

impl TranscriptOpError {
    /// The connector error an [`TranscriptOpError::Other`] stands for.
    /// `AccessDisabled` is handled by the caller before this is reached.
    pub(crate) fn into_connector_error(self) -> ConnectorError {
        match self {
            TranscriptOpError::AccessDisabled => ConnectorError::Exchange("GraphAccessToTranscriptsDisabled".into()),
            TranscriptOpError::Other { message, status: Some(status) } => ConnectorError::upstream_code(status, message),
            TranscriptOpError::Other { message, status: None } => ConnectorError::Exchange(message),
        }
    }
}

#[async_trait]
pub(crate) trait TranscriptGraph: Send + Sync {
    async fn create_organiser_subscription(
        &self,
        access_token: &str,
        resource: &str,
        client_state: &str,
    ) -> Result<CreatedTranscriptSub, TranscriptOpError>;

    async fn pull_transcripts(
        &self,
        access_token: &str,
    ) -> Result<Vec<NormalizedItem>, TranscriptOpError>;
}

#[async_trait]
pub(crate) trait TranscriptCreds: Send + Sync {
    /// Access token for the owner [`transcript_token_owner`] selected.
    /// `None` when that exact row is absent.
    async fn access_token(&self, account_id: Uuid, owner: TokenOwner) -> Option<String>;
}

pub(crate) struct HttpTranscriptGraph<'a> {
    pub http: &'a reqwest::Client,
    /// When set, transcripts already stored for this account are not downloaded again.
    pub content: Option<&'a PgPool>,
    pub account_id: Uuid,
}

#[async_trait]
impl TranscriptGraph for HttpTranscriptGraph<'_> {
    async fn create_organiser_subscription(
        &self,
        access_token: &str,
        resource: &str,
        client_state: &str,
    ) -> Result<CreatedTranscriptSub, TranscriptOpError> {
        let expiration =
            Utc::now() + Duration::minutes(microsoft::teams_transcript_subscription_minutes());
        match microsoft::create_delegated_subscription(
            self.http,
            access_token,
            resource,
            client_state,
            expiration,
        )
        .await
        {
            Ok((subscription_id, expires_at)) => Ok(CreatedTranscriptSub {
                subscription_id,
                expires_at,
            }),
            Err(e) => Err(classify_op(&e)),
        }
    }

    async fn pull_transcripts(
        &self,
        access_token: &str,
    ) -> Result<Vec<NormalizedItem>, TranscriptOpError> {
        match microsoft::fetch_meeting_transcripts(
            self.http,
            access_token,
            self.content,
            self.account_id,
        )
        .await
        {
            Ok(items) => Ok(items),
            Err(e) => Err(classify_op(&e)),
        }
    }
}

fn classify_op(err: &ConnectorError) -> TranscriptOpError {
    if microsoft::graph_access_to_transcripts_disabled(err) {
        TranscriptOpError::AccessDisabled
    } else {
        TranscriptOpError::Other { message: err.to_string(), status: err.upstream_status() }
    }
}

struct HeldToken {
    token: String,
}

#[async_trait]
impl TranscriptCreds for HeldToken {
    async fn access_token(&self, _account_id: Uuid, _owner: TokenOwner) -> Option<String> {
        Some(self.token.clone())
    }
}

/// Ensure one member's transcript path. A kill-switch 403 records a skip for
/// this member and returns [`CaptureOutcome::SkippedDisabled`]. It does not
/// delete another member's subscription.
pub(crate) async fn ensure_member_transcript<G, C>(
    subs: &PgPool,
    sweep: &PgPool,
    health: &PgPool,
    graph: &G,
    creds: &C,
    watch: &TranscriptWatch,
) -> Result<CaptureOutcome, ConnectorError>
where
    G: TranscriptGraph,
    C: TranscriptCreds,
{
    let skip_user = if watch.org_plane {
        Some(watch.user_id)
    } else {
        None
    };
    if connector_health::transcript_access_disabled(health, watch.account_id, skip_user).await {
        return Ok(CaptureOutcome::SkippedDisabled);
    }

    match watch.seat {
        MeetingSeat::Organiser => {
            subscribe_organiser(subs, health, graph, creds, watch, skip_user).await
        }
        MeetingSeat::Attendee => {
            attend_on_sweep(sweep, health, graph, creds, watch, skip_user).await
        }
    }
}

/// One member's failure does not stop the next member.
#[cfg(test)]
pub(crate) async fn ensure_transcript_watches<G, C>(
    subs: &PgPool,
    sweep: &PgPool,
    health: &PgPool,
    graph: &G,
    creds: &C,
    watches: &[TranscriptWatch],
) -> Vec<Result<CaptureOutcome, ConnectorError>>
where
    G: TranscriptGraph,
    C: TranscriptCreds,
{
    let mut out = Vec::with_capacity(watches.len());
    for watch in watches {
        out.push(ensure_member_transcript(subs, sweep, health, graph, creds, watch).await);
    }
    out
}

async fn subscribe_organiser<G, C>(
    subs: &PgPool,
    health: &PgPool,
    graph: &G,
    creds: &C,
    watch: &TranscriptWatch,
    skip_user: Option<Uuid>,
) -> Result<CaptureOutcome, ConnectorError>
where
    G: TranscriptGraph,
    C: TranscriptCreds,
{
    if watch.graph_user_id.is_empty() {
        return Err(ConnectorError::Internal(
            "organiser transcript subscription needs a graph user id".into(),
        ));
    }
    let resource = organiser_transcript_resource(&watch.graph_user_id);
    let owner = transcript_token_owner(watch.org_plane, watch.user_id);
    if microsoft::live_subscription_is_current(subs, watch.account_id, &resource, owner)
        .await
        .unwrap_or(false)
    {
        return Ok(CaptureOutcome::Subscribed { resource });
    }
    let Some(token) = creds.access_token(watch.account_id, owner).await else {
        tracing::info!(
            account_id = %watch.account_id,
            user_id = %watch.user_id,
            "organiser transcript subscription skipped — no token for this owner"
        );
        return Ok(CaptureOutcome::SkippedDisabled);
    };
    let client_state = microsoft::new_subscription_client_state();
    match graph
        .create_organiser_subscription(&token, &resource, &client_state)
        .await
    {
        Ok(created) => {
            let _ = connector_health::note_transcript_access_recovered(
                health,
                watch.account_id,
                skip_user,
            )
            .await;
            microsoft::persist_subscription(
                subs,
                watch.account_id,
                &created.subscription_id,
                &resource,
                &client_state,
                created.expires_at,
                "",
                owner,
                microsoft::subscription_change_types_for(&resource),
            )
            .await?;
            Ok(CaptureOutcome::Subscribed { resource })
        }
        Err(TranscriptOpError::AccessDisabled) => {
            record_skip(health, watch.account_id, skip_user).await;
            Ok(CaptureOutcome::SkippedDisabled)
        }
        Err(err @ TranscriptOpError::Other { .. }) => {
            let err = err.into_connector_error();
            tracing::info!(
                error = %err,
                account_id = %watch.account_id,
                user_id = %watch.user_id,
                "organiser transcript subscription unavailable"
            );
            Err(err)
        }
    }
}

async fn attend_on_sweep<G, C>(
    sweep: &PgPool,
    health: &PgPool,
    graph: &G,
    creds: &C,
    watch: &TranscriptWatch,
    skip_user: Option<Uuid>,
) -> Result<CaptureOutcome, ConnectorError>
where
    G: TranscriptGraph,
    C: TranscriptCreds,
{
    if let Some(graph_id) = watch.graph_id {
        schedule_calendar_sweep(sweep, watch.account_id, watch.user_id, graph_id).await?;
    }
    if !watch.pull_attended {
        return Ok(CaptureOutcome::ScheduledSweep { items: Vec::new() });
    }
    let owner = transcript_token_owner(watch.org_plane, watch.user_id);
    let Some(token) = creds.access_token(watch.account_id, owner).await else {
        return Ok(CaptureOutcome::ScheduledSweep { items: Vec::new() });
    };
    match graph.pull_transcripts(&token).await {
        Ok(items) => {
            let _ = connector_health::note_transcript_access_recovered(
                health,
                watch.account_id,
                skip_user,
            )
            .await;
            Ok(CaptureOutcome::ScheduledSweep { items })
        }
        Err(TranscriptOpError::AccessDisabled) => {
            record_skip(health, watch.account_id, skip_user).await;
            Ok(CaptureOutcome::SkippedDisabled)
        }
        Err(TranscriptOpError::Other { message: msg, .. }) => {
            tracing::info!(
                error = %msg,
                account_id = %watch.account_id,
                user_id = %watch.user_id,
                "attendee transcript pull failed — sweep remains scheduled"
            );
            Ok(CaptureOutcome::ScheduledSweep { items: Vec::new() })
        }
    }
}

async fn record_skip(health: &PgPool, account_id: Uuid, user_id: Option<Uuid>) {
    if let Err(e) =
        connector_health::note_transcript_access_disabled(health, account_id, user_id).await
    {
        tracing::warn!(
            error = %e,
            %account_id,
            "failed to record transcript access skip"
        );
    }
}

/// Same upsert the org calendar worker uses the first time it sees a member.
/// An attendee-only member has no organiser subscription; the sweep is what
/// pulls their transcripts.
async fn schedule_calendar_sweep(
    pool: &PgPool,
    account_id: Uuid,
    owner_user_id: Uuid,
    graph_id: Uuid,
) -> Result<(), ConnectorError> {
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(pool, account_id)
        .await
        .map_err(|e| ConnectorError::Internal(format!("schedule transcript sweep: {e}")))?;
    sqlx::query(
        r#"
        INSERT INTO microsoft_calendar_sync_state (account_id, owner_user_id, graph_id)
        VALUES ($1, $2, $3)
        ON CONFLICT (account_id, owner_user_id) DO UPDATE
            SET graph_id = EXCLUDED.graph_id
        "#,
    )
    .bind(account_id)
    .bind(owner_user_id)
    .bind(graph_id)
    .execute(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("schedule transcript sweep: {e}")))?;
    tx.commit()
        .await
        .map_err(|e| ConnectorError::Internal(format!("schedule transcript sweep commit: {e}")))?;
    Ok(())
}

async fn graph_user_id(
    control: &PgPool,
    account_id: Uuid,
    owner: TokenOwner,
) -> Result<Option<String>, ConnectorError> {
    let mut tx = crate::tenancy::scope::begin_account_scoped(control, account_id)
        .await
        .map_err(|e| ConnectorError::Internal(format!("transcript graph user id: {e}")))?;
    let row: Option<(Option<String>,)> = match owner {
        TokenOwner::AccountWide => sqlx::query_as(
            "SELECT provider_user_id FROM oauth_tokens \
             WHERE account_id = $1 AND provider = 'microsoft' AND owner_user_id IS NULL",
        )
        .bind(account_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| ConnectorError::Internal(format!("transcript graph user id: {e}")))?,
        TokenOwner::Member(user_id) => sqlx::query_as(
            "SELECT provider_user_id FROM oauth_tokens \
             WHERE account_id = $1 AND provider = 'microsoft' AND owner_user_id = $2",
        )
        .bind(account_id)
        .bind(user_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| ConnectorError::Internal(format!("transcript graph user id: {e}")))?,
    };
    tx.rollback().await.ok();
    Ok(row.and_then(|(id,)| id).filter(|id| !id.is_empty()))
}

/// Calendar-tick entry. Classifies organiser vs attendee on the token the
/// caller already loaded for this member, then subscribes, schedules, or
/// both. A kill switch is a skip, not a failure of the calendar batch.
pub(crate) async fn plan_member_transcripts(
    state: &std::sync::Arc<crate::AppState>,
    http: &reqwest::Client,
    subs: &PgPool,
    sweep: &PgPool,
    health: &PgPool,
    account_id: Uuid,
    user_id: Uuid,
    org_plane: bool,
    graph_id: Option<Uuid>,
    access_token: &str,
    pull_attended: bool,
) -> Result<CaptureOutcome, ConnectorError> {
    let skip_user = if org_plane { Some(user_id) } else { None };
    if connector_health::transcript_access_disabled(health, account_id, skip_user).await {
        return Ok(CaptureOutcome::SkippedDisabled);
    }
    // This sweep is bounded by explicit meeting enrollment, never the calendar.
    let Some(graph) = graph_id else {
        return Ok(CaptureOutcome::ScheduledSweep { items: Vec::new() });
    };
    if !pull_attended {
        return Ok(CaptureOutcome::ScheduledSweep { items: Vec::new() });
    }
    match microsoft::fetch_consented_meeting_transcripts(
        state,
        http,
        access_token,
        subs,
        sweep,
        account_id,
        user_id,
        graph,
    )
    .await
    {
        Ok(items) => Ok(CaptureOutcome::ScheduledSweep { items }),
        Err(e) if microsoft::graph_access_to_transcripts_disabled(&e) => {
            record_skip(health, account_id, skip_user).await;
            Ok(CaptureOutcome::SkippedDisabled)
        }
        Err(e) => Err(e),
    }
}

async fn refresh_lapsed_organiser(
    http: &reqwest::Client,
    subs: &PgPool,
    health: &PgPool,
    account_id: Uuid,
    user_id: Uuid,
    org_plane: bool,
    access_token: &str,
) {
    let owner = transcript_token_owner(org_plane, user_id);
    let Ok(Some(graph_user_id)) = graph_user_id(subs, account_id, owner).await else {
        return;
    };
    let resource = organiser_transcript_resource(&graph_user_id);
    if !microsoft::subscription_row_present(subs, account_id, &resource, owner).await {
        return;
    }
    if microsoft::live_subscription_is_current(subs, account_id, &resource, owner)
        .await
        .unwrap_or(false)
    {
        return;
    }
    let watch = TranscriptWatch {
        account_id,
        user_id,
        graph_user_id,
        org_plane,
        seat: MeetingSeat::Organiser,
        graph_id: None,
        pull_attended: false,
    };
    let graph = HttpTranscriptGraph {
        http,
        content: None,
        account_id,
    };
    let creds = HeldToken {
        token: access_token.to_string(),
    };
    let skip_user = if org_plane { Some(user_id) } else { None };
    if let Err(e) = subscribe_organiser(subs, health, &graph, &creds, &watch, skip_user).await {
        tracing::info!(
            error = %e, %account_id, %user_id,
            "lapsed organiser transcript subscription was not recreated"
        );
    }
}

async fn apply_coverage(
    http: &reqwest::Client,
    subs: &PgPool,
    sweep: &PgPool,
    health: &PgPool,
    account_id: Uuid,
    user_id: Uuid,
    org_plane: bool,
    graph_id: Option<Uuid>,
    access_token: &str,
    pull_attended: bool,
    coverage: OnlineMeetingCoverage,
) -> Result<CaptureOutcome, ConnectorError> {
    if !coverage.organized_any && !coverage.attended_any {
        // An organiser subscription can outlive the calendar window that
        // created it. Recreate it when it has lapsed, even with no recent
        // meetings, or the next organised meeting has no push.
        refresh_lapsed_organiser(
            http,
            subs,
            health,
            account_id,
            user_id,
            org_plane,
            access_token,
        )
        .await;
        return Ok(CaptureOutcome::ScheduledSweep { items: Vec::new() });
    }
    let owner = transcript_token_owner(org_plane, user_id);
    let graph_user_id = graph_user_id(subs, account_id, owner)
        .await?
        .unwrap_or_default();
    let graph = HttpTranscriptGraph {
        http,
        content: Some(sweep),
        account_id,
    };
    let creds = HeldToken {
        token: access_token.to_string(),
    };
    let mut items = Vec::new();
    if coverage.organized_any && !graph_user_id.is_empty() {
        let watch = TranscriptWatch {
            account_id,
            user_id,
            graph_user_id: graph_user_id.clone(),
            org_plane,
            seat: MeetingSeat::Organiser,
            graph_id,
            pull_attended: false,
        };
        match ensure_member_transcript(subs, sweep, health, &graph, &creds, &watch).await {
            Ok(CaptureOutcome::SkippedDisabled) => return Ok(CaptureOutcome::SkippedDisabled),
            Ok(_) => {}
            Err(e) => {
                tracing::info!(
                    error = %e,
                    %account_id,
                    %user_id,
                    "organiser transcript subscription not created"
                );
            }
        }
    }
    if coverage.attended_any {
        let watch = TranscriptWatch {
            account_id,
            user_id,
            graph_user_id,
            org_plane,
            seat: MeetingSeat::Attendee,
            graph_id,
            pull_attended,
        };
        match ensure_member_transcript(subs, sweep, health, &graph, &creds, &watch).await? {
            CaptureOutcome::ScheduledSweep { items: pulled } => items = pulled,
            CaptureOutcome::SkippedDisabled => return Ok(CaptureOutcome::SkippedDisabled),
            CaptureOutcome::Subscribed { .. } => {}
        }
    }
    Ok(CaptureOutcome::ScheduledSweep { items })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    use sqlx::postgres::PgPoolOptions;
    use sqlx::{Connection, Executor, PgConnection};

    fn base_url() -> Option<String> {
        std::env::var("ORGT_TEST_BASE_URL").ok()
    }

    fn with_db(base: &str, db: &str) -> String {
        let (prefix, rest) = base.split_at(base.rfind('/').expect("url has a path /"));
        let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
        format!("{prefix}/{db}{params}")
    }

    async fn recreate(base: &str, db: &str) -> PgConnection {
        let mut conn = PgConnection::connect(base).await.expect("connect admin");
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .unwrap();
        conn.execute(format!("CREATE DATABASE {db}").as_str())
            .await
            .unwrap();
        conn
    }

    async fn control_pool(base: &str, db: &str) -> PgPool {
        let pool = PgPoolOptions::new()
            .connect(&with_db(base, db))
            .await
            .unwrap();
        sqlx::migrate!("./migrations_control")
            .run(&pool)
            .await
            .unwrap();
        pool
    }

    async fn seed_user(pool: &PgPool) -> Uuid {
        sqlx::query_scalar(
            "INSERT INTO users(id, email) VALUES (gen_random_uuid(), $1) RETURNING id",
        )
        .bind(format!("transcript-{}@test.local", Uuid::new_v4()))
        .fetch_one(pool)
        .await
        .unwrap()
    }

    async fn seed_org(pool: &PgPool, owner: Uuid) -> Uuid {
        sqlx::query_scalar(
            "INSERT INTO accounts(owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id",
        )
        .bind(owner)
        .bind(format!("org_{}", Uuid::new_v4().simple()))
        .fetch_one(pool)
        .await
        .unwrap()
    }

    async fn seed_personal(pool: &PgPool, owner: Uuid) -> Uuid {
        sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES ($1) RETURNING id")
            .bind(owner)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    async fn seed_graph(pool: &PgPool, account_id: Uuid, owner_user_id: Uuid) -> Uuid {
        sqlx::query_scalar(
            "INSERT INTO graphs(account_id, name, kind, is_primary, owner_user_id) \
             VALUES ($1, 'member ocean', 'member', true, $2) RETURNING id",
        )
        .bind(account_id)
        .bind(owner_user_id)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    async fn seed_token(
        pool: &PgPool,
        account: Uuid,
        owner: Option<Uuid>,
        scope: &str,
        graph_user_id: &str,
    ) {
        sqlx::query(
            "INSERT INTO oauth_tokens \
                (account_id, provider, scope, access_token, owner_user_id, provider_user_id) \
             VALUES ($1, 'microsoft', $2, $3, $4, $5)",
        )
        .bind(account)
        .bind(scope)
        .bind(vec![0u8])
        .bind(owner)
        .bind(graph_user_id)
        .execute(pool)
        .await
        .unwrap();
    }

    struct ScopeCreds {
        pool: PgPool,
    }

    #[async_trait]
    impl TranscriptCreds for ScopeCreds {
        async fn access_token(&self, account_id: Uuid, owner: TokenOwner) -> Option<String> {
            let mut tx = crate::tenancy::scope::begin_account_scoped(&self.pool, account_id)
                .await
                .ok()?;
            let scope: Option<String> = match owner {
                TokenOwner::AccountWide => sqlx::query_scalar(
                    "SELECT scope FROM oauth_tokens \
                     WHERE account_id = $1 AND provider = 'microsoft' AND owner_user_id IS NULL",
                )
                .bind(account_id)
                .fetch_optional(&mut *tx)
                .await
                .ok()
                .flatten(),
                TokenOwner::Member(user_id) => sqlx::query_scalar(
                    "SELECT scope FROM oauth_tokens \
                     WHERE account_id = $1 AND provider = 'microsoft' AND owner_user_id = $2",
                )
                .bind(account_id)
                .bind(user_id)
                .fetch_optional(&mut *tx)
                .await
                .ok()
                .flatten(),
            };
            tx.rollback().await.ok();
            scope
        }
    }

    struct RecordingGraph {
        creates: AtomicUsize,
        pulls: AtomicUsize,
        resources: Mutex<Vec<String>>,
        pull_tokens: Mutex<Vec<String>>,
        /// Create returns AccessDisabled when the token equals this scope.
        disabled_token: Option<String>,
    }

    #[async_trait]
    impl TranscriptGraph for RecordingGraph {
        async fn create_organiser_subscription(
            &self,
            access_token: &str,
            resource: &str,
            _client_state: &str,
        ) -> Result<CreatedTranscriptSub, TranscriptOpError> {
            self.creates.fetch_add(1, Ordering::SeqCst);
            self.resources.lock().unwrap().push(resource.to_string());
            if self.disabled_token.as_deref() == Some(access_token) {
                return Err(TranscriptOpError::AccessDisabled);
            }
            let n = self.creates.load(Ordering::SeqCst);
            Ok(CreatedTranscriptSub {
                subscription_id: format!("sub-{n}"),
                expires_at: Utc::now() + Duration::minutes(4230),
            })
        }

        async fn pull_transcripts(
            &self,
            access_token: &str,
        ) -> Result<Vec<NormalizedItem>, TranscriptOpError> {
            self.pulls.fetch_add(1, Ordering::SeqCst);
            self.pull_tokens
                .lock()
                .unwrap()
                .push(access_token.to_string());
            if self.disabled_token.as_deref() == Some(access_token) {
                return Err(TranscriptOpError::AccessDisabled);
            }
            Ok(Vec::new())
        }
    }

    fn watch(
        account: Uuid,
        user: Uuid,
        graph_user: &str,
        org_plane: bool,
        seat: MeetingSeat,
        graph_id: Option<Uuid>,
        pull: bool,
    ) -> TranscriptWatch {
        TranscriptWatch {
            account_id: account,
            user_id: user,
            graph_user_id: graph_user.to_string(),
            org_plane,
            seat,
            graph_id,
            pull_attended: pull,
        }
    }

    #[tokio::test]
    async fn org_plane_transcript_fetch_loads_member_personal_loads_account_wide_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping org_plane_transcript_fetch_loads_member_personal_loads_account_wide_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "cs461_tx_fetch";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool = control_pool(&base, db).await;

        let org_owner = seed_user(&pool).await;
        let org = seed_org(&pool, org_owner).await;
        let member = seed_user(&pool).await;
        let member_graph = seed_graph(&pool, org, member).await;
        seed_token(&pool, org, Some(member), "org-member", "aad-member").await;
        seed_token(&pool, org, None, "org-wide-decoy", "aad-decoy").await;

        let personal_user = seed_user(&pool).await;
        let personal = seed_personal(&pool, personal_user).await;
        let personal_graph = seed_graph(&pool, personal, personal_user).await;
        seed_token(&pool, personal, None, "personal-wide", "aad-personal").await;
        seed_token(
            &pool,
            personal,
            Some(personal_user),
            "personal-member-decoy",
            "aad-decoy2",
        )
        .await;

        let graph = RecordingGraph {
            creates: AtomicUsize::new(0),
            pulls: AtomicUsize::new(0),
            resources: Mutex::new(Vec::new()),
            pull_tokens: Mutex::new(Vec::new()),
            disabled_token: None,
        };
        let creds = ScopeCreds { pool: pool.clone() };

        ensure_member_transcript(
            &pool,
            &pool,
            &pool,
            &graph,
            &creds,
            &watch(
                org,
                member,
                "aad-member",
                true,
                MeetingSeat::Attendee,
                Some(member_graph),
                true,
            ),
        )
        .await
        .unwrap();
        ensure_member_transcript(
            &pool,
            &pool,
            &pool,
            &graph,
            &creds,
            &watch(
                personal,
                personal_user,
                "aad-personal",
                false,
                MeetingSeat::Attendee,
                Some(personal_graph),
                true,
            ),
        )
        .await
        .unwrap();

        let pulled = graph.pull_tokens.lock().unwrap().clone();
        assert_eq!(
            pulled,
            vec!["org-member".to_string(), "personal-wide".to_string()]
        );
        assert_eq!(graph.creates.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn organiser_subscription_is_per_user_and_skipped_for_attendee_only_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping organiser_subscription_is_per_user_and_skipped_for_attendee_only_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "cs461_tx_orgsub";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool = control_pool(&base, db).await;

        let owner = seed_user(&pool).await;
        let account = seed_org(&pool, owner).await;
        let organiser = seed_user(&pool).await;
        let attendee = seed_user(&pool).await;
        let attendee_graph = seed_graph(&pool, account, attendee).await;
        seed_token(&pool, account, Some(organiser), "org-organiser", "aad-org").await;
        seed_token(&pool, account, Some(attendee), "org-attendee", "aad-att").await;

        let graph = RecordingGraph {
            creates: AtomicUsize::new(0),
            pulls: AtomicUsize::new(0),
            resources: Mutex::new(Vec::new()),
            pull_tokens: Mutex::new(Vec::new()),
            disabled_token: None,
        };
        let creds = ScopeCreds { pool: pool.clone() };
        let results = ensure_transcript_watches(
            &pool,
            &pool,
            &pool,
            &graph,
            &creds,
            &[
                watch(
                    account,
                    organiser,
                    "aad-org",
                    true,
                    MeetingSeat::Organiser,
                    None,
                    false,
                ),
                watch(
                    account,
                    attendee,
                    "aad-att",
                    true,
                    MeetingSeat::Attendee,
                    Some(attendee_graph),
                    false,
                ),
            ],
        )
        .await;
        assert!(results.iter().all(|r| r.is_ok()));

        let resources = graph.resources.lock().unwrap().clone();
        assert_eq!(
            resources,
            vec!["users/aad-org/onlineMeetings/getAllTranscripts".to_string()]
        );

        let subs: Vec<(Option<Uuid>, String)> = sqlx::query_as(
            "SELECT owner_user_id, resource FROM graph_subscriptions WHERE account_id = $1",
        )
        .bind(account)
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(subs.len(), 1);
        assert_eq!(subs[0].0, Some(organiser));
        assert_eq!(subs[0].1, "users/aad-org/onlineMeetings/getAllTranscripts");
    }

    #[tokio::test]
    async fn attendee_path_schedules_the_calendar_sweep_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping attendee_path_schedules_the_calendar_sweep_pg: set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "cs461_tx_sweep";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool = control_pool(&base, db).await;

        let owner = seed_user(&pool).await;
        let account = seed_org(&pool, owner).await;
        let attendee = seed_user(&pool).await;
        let graph_id = seed_graph(&pool, account, attendee).await;
        seed_token(&pool, account, Some(attendee), "org-attendee", "aad-att").await;

        let graph = RecordingGraph {
            creates: AtomicUsize::new(0),
            pulls: AtomicUsize::new(0),
            resources: Mutex::new(Vec::new()),
            pull_tokens: Mutex::new(Vec::new()),
            disabled_token: None,
        };
        let creds = ScopeCreds { pool: pool.clone() };
        let outcome = ensure_member_transcript(
            &pool,
            &pool,
            &pool,
            &graph,
            &creds,
            &watch(
                account,
                attendee,
                "aad-att",
                true,
                MeetingSeat::Attendee,
                Some(graph_id),
                true,
            ),
        )
        .await
        .unwrap();
        assert!(matches!(outcome, CaptureOutcome::ScheduledSweep { .. }));
        assert_eq!(graph.creates.load(Ordering::SeqCst), 0);
        assert_eq!(graph.pulls.load(Ordering::SeqCst), 1);

        let row: Option<(Uuid, bool)> = sqlx::query_as(
            "SELECT graph_id, sync_enabled FROM microsoft_calendar_sync_state \
             WHERE account_id = $1 AND owner_user_id = $2",
        )
        .bind(account)
        .bind(attendee)
        .fetch_optional(&pool)
        .await
        .unwrap();
        let (scheduled_graph, enabled) = row.expect("attendee calendar sweep scheduled");
        assert_eq!(scheduled_graph, graph_id);
        assert!(enabled);
        let subs: i64 =
            sqlx::query_scalar("SELECT count(*) FROM graph_subscriptions WHERE account_id = $1")
                .bind(account)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(subs, 0);
    }

    #[tokio::test]
    async fn one_members_403_does_not_drop_another_members_transcript_watch_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping one_members_403_does_not_drop_another_members_transcript_watch_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "cs461_tx_403";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool = control_pool(&base, db).await;

        let owner = seed_user(&pool).await;
        let account = seed_org(&pool, owner).await;
        let member_a = seed_user(&pool).await;
        let member_b = seed_user(&pool).await;
        seed_token(&pool, account, Some(member_a), "member-a", "aad-a").await;
        seed_token(&pool, account, Some(member_b), "member-b", "aad-b").await;

        let graph = RecordingGraph {
            creates: AtomicUsize::new(0),
            pulls: AtomicUsize::new(0),
            resources: Mutex::new(Vec::new()),
            pull_tokens: Mutex::new(Vec::new()),
            disabled_token: Some("member-a".into()),
        };
        let creds = ScopeCreds { pool: pool.clone() };
        let watches = [
            watch(
                account,
                member_a,
                "aad-a",
                true,
                MeetingSeat::Organiser,
                None,
                false,
            ),
            watch(
                account,
                member_b,
                "aad-b",
                true,
                MeetingSeat::Organiser,
                None,
                false,
            ),
        ];
        let results =
            ensure_transcript_watches(&pool, &pool, &pool, &graph, &creds, &watches).await;
        assert!(matches!(results[0], Ok(CaptureOutcome::SkippedDisabled)));
        assert!(matches!(results[1], Ok(CaptureOutcome::Subscribed { .. })));

        let subs: Vec<(Uuid, String)> = sqlx::query_as(
            "SELECT owner_user_id, resource FROM graph_subscriptions \
             WHERE account_id = $1 AND owner_user_id IS NOT NULL",
        )
        .bind(account)
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(subs.len(), 1);
        assert_eq!(subs[0].0, member_b);
        assert_eq!(subs[0].1, "users/aad-b/onlineMeetings/getAllTranscripts");

        let skipped: Vec<Uuid> = sqlx::query_scalar(
            "SELECT backing_user_id FROM connector_health \
             WHERE account_id = $1 AND provider = 'microsoft' \
               AND capability = 'teams_transcripts' AND resolved_at IS NULL \
               AND backing_user_id IS NOT NULL",
        )
        .bind(account)
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(skipped, vec![member_a]);

        let creates_after_first = graph.creates.load(Ordering::SeqCst);
        ensure_transcript_watches(&pool, &pool, &pool, &graph, &creds, &watches).await;
        assert_eq!(
            graph.creates.load(Ordering::SeqCst),
            creates_after_first,
            "an open skip must not call Graph again"
        );
        let still: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM graph_subscriptions \
             WHERE account_id = $1 AND owner_user_id = $2",
        )
        .bind(account)
        .bind(member_b)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(still, 1);
    }
}
