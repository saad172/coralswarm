//! Teams channel/chat change notifications as one doorbell per
//! `(ms_tenant_id, resource)` (Phase G wave 3, item 56 / S5a).
//!
//! Graph allows a single `chatMessage` subscription per app-and-channel,
//! with no user or account dimension. Content is fetched on each watcher's
//! own token. Only resource ids cross accounts. Mail stays on
//! `graph_subscriptions`. App-mode (RSC) channel subscriptions stay there
//! too until item 48. This module is the delegated path.
//!
//! Owner election follows Chat's reader walk (`elect_space_reader` /
//! `MAX_READER_CANDIDATES_PER_PASS`): sticky current owner, then most recent
//! successful fetch, then `updated_at`, at most
//! [`MAX_CONTAINER_OWNER_CANDIDATES`] tries. The subscription row is
//! row-locked for the read; the Graph call happens after the lock is
//! released. Lifecycle failover, credential death, and departure
//! (S5b) re-elect through [`elect_container_owner`] and classify Graph
//! PATCH errors instead of dropping the row.

use std::future::Future;

use async_trait::async_trait;
use chrono::{Duration, Utc};
use sqlx::PgPool;
use uuid::Uuid;

use super::{microsoft, ConnectorError, MicrosoftConnector, TokenOwner};

/// Same bound as `workers::google_chat_sweep::MAX_READER_CANDIDATES_PER_PASS`.
pub(crate) const MAX_CONTAINER_OWNER_CANDIDATES: usize = 5;

/// One elected owner. `org_account` chooses the token row: `Member` on an
/// org, `AccountWide` on personal. `user_id` is always the watching user,
/// including on the personal plane, so the fetch knows who.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ElectedOwner {
    pub account_id: Uuid,
    pub user_id: Uuid,
    pub org_account: bool,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct DoorbellSubscription {
    pub ms_tenant_id: String,
    pub resource: String,
    pub client_state: String,
    pub subscription_id: String,
    pub owner_account_id: Option<Uuid>,
    pub owner_user_id: Option<Uuid>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ContainerWatcher {
    pub account_id: Uuid,
    pub user_id: Uuid,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WatcherOp {
    Keep,
    /// 403/404 on this watcher's own fetch. Drop only this routing row.
    Drop,
}

#[derive(Debug)]
pub(crate) struct CreatedDoor {
    pub subscription_id: String,
    pub expires_at: chrono::DateTime<Utc>,
}

#[derive(Debug)]
pub(crate) enum DoorCreateError {
    SlotTaken(String),
    Failed(String),
}

#[async_trait]
pub(crate) trait DoorbellGraph: Send + Sync {
    async fn create_subscription(
        &self,
        access_token: &str,
        resource: &str,
        client_state: &str,
    ) -> Result<CreatedDoor, DoorCreateError>;
    async fn delete_subscription(
        &self,
        access_token: &str,
        subscription_id: &str,
    ) -> Result<(), String>;
    /// `PATCH /subscriptions/{id}`. Errors are classified: revoked/401/403
    /// re-elect, 404 recreates, 429/5xx keeps the row and the owner.
    async fn renew_subscription(
        &self,
        access_token: &str,
        subscription_id: &str,
        resource: &str,
    ) -> Result<chrono::DateTime<Utc>, DoorPatchError>;
    /// A call that 401/403s when the token cannot act. Used to decide
    /// whether the current owner may be re-elected after `subscriptionRemoved`.
    async fn probe_token(&self, access_token: &str) -> Result<(), DoorPatchError>;
}

/// How a doorbell PATCH (or the cheap owner probe) failed. Never "drop the row".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DoorPatchError {
    /// Credential revoked, 401, or 403. Try another owner.
    Reelect,
    /// Graph no longer has this subscription id.
    Missing,
    /// 429 or 5xx. Keep the row and the current owner; try again later.
    RetryLater,
}

pub(crate) fn classify_patch_failure(err: &ConnectorError) -> DoorPatchError {
    if matches!(err, ConnectorError::CredentialRevoked { .. }) {
        return DoorPatchError::Reelect;
    }
    let Some(msg) = err.upstream_message() else {
        return DoorPatchError::RetryLater;
    };
    let lower = msg.to_ascii_lowercase();
    if lower.contains("revoked") || lower.contains("invalid_grant") {
        return DoorPatchError::Reelect;
    }
    // The status is a field of the error (`ConnectorError::UpstreamStatus`),
    // never read back out of the message. A failure with no status (network,
    // timeout, parse) waits, as a 429 or 5xx does.
    match err.upstream_status() {
        Some(401 | 403) => DoorPatchError::Reelect,
        Some(404) => DoorPatchError::Missing,
        _ => DoorPatchError::RetryLater,
    }
}

#[async_trait]
pub(crate) trait WatcherTokens: Send + Sync {
    /// Live access token for this watcher. `org_account` selects
    /// `TokenOwner::Member` vs `AccountWide`. `None` skips this candidate.
    async fn access_token(
        &self,
        account_id: Uuid,
        user_id: Uuid,
        org_account: bool,
    ) -> Option<String>;
}

pub(crate) struct HttpDoorbell<'a> {
    pub http: &'a reqwest::Client,
}

#[async_trait]
impl DoorbellGraph for HttpDoorbell<'_> {
    async fn create_subscription(
        &self,
        access_token: &str,
        resource: &str,
        client_state: &str,
    ) -> Result<CreatedDoor, DoorCreateError> {
        let expiration =
            Utc::now() + Duration::minutes(microsoft::teams_chat_message_subscription_minutes());
        match microsoft::create_delegated_subscription(
            self.http,
            access_token,
            resource,
            client_state,
            expiration,
        )
        .await
        {
            Ok((id, expires_at)) => Ok(CreatedDoor { subscription_id: id, expires_at }),
            Err(e) if microsoft::subscription_slot_taken(&e) => {
                Err(DoorCreateError::SlotTaken(e.to_string()))
            }
            Err(e) => Err(DoorCreateError::Failed(e.to_string())),
        }
    }

    async fn delete_subscription(
        &self,
        access_token: &str,
        subscription_id: &str,
    ) -> Result<(), String> {
        microsoft::delete_delegated_subscription(self.http, access_token, subscription_id)
            .await
            .map_err(|e| e.to_string())
    }

    async fn renew_subscription(
        &self,
        access_token: &str,
        subscription_id: &str,
        resource: &str,
    ) -> Result<chrono::DateTime<Utc>, DoorPatchError> {
        microsoft::renew_subscription(self.http, access_token, subscription_id, resource)
            .await
            .map_err(|e| classify_patch_failure(&e))
    }

    async fn probe_token(&self, access_token: &str) -> Result<(), DoorPatchError> {
        microsoft::probe_delegated_token(self.http, access_token)
            .await
            .map_err(|e| classify_patch_failure(&e))
    }
}

pub(crate) struct DekWatcherTokens<'a> {
    pub control: &'a PgPool,
    pub dek: &'a crate::crypto::DekPlaneSource,
    pub http: &'a reqwest::Client,
}

#[async_trait]
impl WatcherTokens for DekWatcherTokens<'_> {
    async fn access_token(
        &self,
        account_id: Uuid,
        user_id: Uuid,
        org_account: bool,
    ) -> Option<String> {
        let owner = if org_account {
            TokenOwner::Member(user_id)
        } else {
            TokenOwner::AccountWide
        };
        let plane = self.dek.plane_for(account_id);
        let mut creds = crate::routes::connectors::load_creds_bg(
            self.control,
            &plane,
            account_id,
            "microsoft",
            owner,
        )
        .await
        .ok()
        .flatten()?;
        let connector = MicrosoftConnector;
        if connector.refresh_if_needed(self.http, &mut creds).await.is_err() {
            return None;
        }
        if let Some(refreshed) = creds.refreshed.as_ref() {
            let _ = crate::routes::oauth::store_token_bundle(
                self.control,
                &plane,
                account_id,
                "microsoft",
                owner,
                refreshed,
            )
            .await;
        }
        Some(creds.access_token)
    }
}

/// Same token load as [`DekWatcherTokens`], but the DEK plane is resolved
/// per account. A doorbell lifecycle can fan across personal and org
/// watchers, and one [`crate::crypto::DekPlaneSource`] is a single tenant.
pub(crate) struct ResolvedWatcherTokens<'a> {
    pub state: &'a crate::AppState,
    pub control: &'a PgPool,
    pub http: &'a reqwest::Client,
}

#[async_trait]
impl WatcherTokens for ResolvedWatcherTokens<'_> {
    async fn access_token(
        &self,
        account_id: Uuid,
        user_id: Uuid,
        org_account: bool,
    ) -> Option<String> {
        let owner = if org_account {
            TokenOwner::Member(user_id)
        } else {
            TokenOwner::AccountWide
        };
        let dek_plane = crate::crypto::DekPlane::resolve_for_account(self.state, self.control, account_id)
            .await
            .ok()?;
        let mut creds = crate::routes::connectors::load_creds_bg(
            self.control,
            &dek_plane,
            account_id,
            "microsoft",
            owner,
        )
        .await
        .ok()
        .flatten()?;
        let connector = MicrosoftConnector;
        if connector.refresh_if_needed(self.http, &mut creds).await.is_err() {
            return None;
        }
        if let Some(refreshed) = creds.refreshed.as_ref() {
            let _ = crate::routes::oauth::store_token_bundle(
                self.control,
                &dek_plane,
                account_id,
                "microsoft",
                owner,
                refreshed,
            )
            .await;
        }
        Some(creds.access_token)
    }
}

/// Candidates for `resource`: watchers with a live Microsoft token
/// (`credential_invalid_at IS NULL`) who are current org members when the
/// account is an org. Personal accounts use their AccountWide token; the
/// stored `owner_user_id` is still the watching user.
///
/// Order: current owner first (sticky), then `last_fetch_ok_at`, then
/// `updated_at`. Row-locks the subscription row for the read and releases
/// it before returning, so the caller can try Graph without holding a
/// pooled connection.
pub(crate) async fn elect_container_owner(
    pool: &PgPool,
    ms_tenant_id: &str,
    resource: &str,
    exclude: &[(Uuid, Uuid)],
) -> Result<Option<ElectedOwner>, ConnectorError> {
    let (ex_accounts, ex_users): (Vec<Uuid>, Vec<Uuid>) = exclude.iter().copied().unzip();
    let mut tx = pool.begin().await.map_err(internal)?;
    sqlx::query(
        "SELECT state FROM teams_container_subscriptions \
         WHERE ms_tenant_id = $1 AND resource = $2 \
         FOR UPDATE",
    )
    .bind(ms_tenant_id)
    .bind(resource)
    .fetch_optional(&mut *tx)
    .await
    .map_err(internal)?;
    let candidates: Vec<(Uuid, Uuid, bool)> = sqlx::query_as(
        "SELECT w.account_id, w.user_id, (a.clerk_org_id IS NOT NULL) \
         FROM teams_container_watchers w \
         JOIN accounts a ON a.id = w.account_id \
         LEFT JOIN teams_container_subscriptions s \
           ON s.ms_tenant_id = w.ms_tenant_id AND s.resource = w.resource \
         WHERE w.ms_tenant_id = $1 AND w.resource = $2 \
           AND NOT EXISTS ( \
                 SELECT 1 FROM unnest($3::uuid[], $4::uuid[]) AS ex(account_id, user_id) \
                 WHERE ex.account_id = w.account_id AND ex.user_id = w.user_id \
               ) \
           AND ( \
                 a.clerk_org_id IS NULL \
                 OR EXISTS ( \
                      SELECT 1 FROM account_members am \
                      WHERE am.account_id = w.account_id AND am.user_id = w.user_id \
                    ) \
               ) \
         ORDER BY \
           (s.owner_account_id IS NOT DISTINCT FROM w.account_id \
            AND s.owner_user_id IS NOT DISTINCT FROM w.user_id) DESC, \
           w.last_fetch_ok_at DESC NULLS LAST, \
           w.updated_at DESC",
    )
    .bind(ms_tenant_id)
    .bind(resource)
    .bind(&ex_accounts)
    .bind(&ex_users)
    .fetch_all(&mut *tx)
    .await
    .map_err(internal)?;
    tx.commit().await.map_err(internal)?;
    // The registry spans accounts, but OAuth credentials are protected by
    // account RLS. Release the registry transaction before checking candidates
    // so a single-connection pool can also serve each scoped metadata read.
    for (account_id, user_id, org_account) in candidates {
        let owner = if org_account { Some(user_id) } else { None };
        let mut scoped = crate::tenancy::scope::begin_account_scoped(pool, account_id)
            .await
            .map_err(|e| ConnectorError::Internal(e.to_string()))?;
        let eligible: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM oauth_tokens \
             WHERE account_id = $1 AND provider = 'microsoft' \
               AND credential_invalid_at IS NULL \
               AND owner_user_id IS NOT DISTINCT FROM $2)",
        )
        .bind(account_id)
        .bind(owner)
        .fetch_one(&mut *scoped)
        .await
        .map_err(internal)?;
        scoped.rollback().await.map_err(internal)?;
        if eligible {
            return Ok(Some(ElectedOwner { account_id, user_id, org_account }));
        }
    }
    Ok(None)
}

/// Record a delegated watch and make sure one doorbell subscription exists
/// for the container. A second watcher of the same resource does not create
/// a second Graph subscription.
pub(crate) async fn ensure_delegated_container(
    http: &reqwest::Client,
    control: &PgPool,
    dek: &crate::crypto::DekPlaneSource,
    resource: &str,
    account_id: Uuid,
    user_id: Uuid,
) -> Result<(), ConnectorError> {
    let Some(ms_tenant_id) = microsoft_tid(control, account_id, user_id).await? else {
        tracing::info!(%account_id, %resource, "teams doorbell skipped — no Microsoft tenant id on the token");
        return Ok(());
    };
    let graph = HttpDoorbell { http };
    let tokens = DekWatcherTokens { control, dek, http };
    ensure_with(control, &graph, &tokens, &ms_tenant_id, resource, account_id, user_id).await
}

pub(crate) async fn ensure_with<G: DoorbellGraph, T: WatcherTokens>(
    pool: &PgPool,
    graph: &G,
    tokens: &T,
    ms_tenant_id: &str,
    resource: &str,
    account_id: Uuid,
    user_id: Uuid,
) -> Result<(), ConnectorError> {
    upsert_watcher(pool, ms_tenant_id, resource, account_id, user_id).await?;
    ensure_subscription_row(pool, ms_tenant_id, resource).await?;
    if !claim_election(pool, ms_tenant_id, resource).await? {
        return Ok(());
    }
    try_create(pool, graph, tokens, ms_tenant_id, resource).await
}

/// Stop watching. A non-owner removes only their watcher row. The last
/// watcher deletes the upstream subscription on their token BEFORE the
/// watcher row is removed. The owner leaving, with others still watching,
/// deletes the old subscription and elects a new owner.
pub(crate) async fn unwatch_container<G: DoorbellGraph, T: WatcherTokens>(
    pool: &PgPool,
    graph: &G,
    tokens: &T,
    resource: &str,
    account_id: Uuid,
    user_id: Uuid,
) -> Result<(), ConnectorError> {
    let tenants: Vec<(String,)> = sqlx::query_as(
        "SELECT ms_tenant_id FROM teams_container_watchers \
         WHERE account_id = $1 AND user_id = $2 AND resource = $3",
    )
    .bind(account_id)
    .bind(user_id)
    .bind(resource)
    .fetch_all(pool)
    .await
    .map_err(internal)?;
    for (ms_tenant_id,) in tenants {
        unwatch_one(pool, graph, tokens, &ms_tenant_id, resource, account_id, user_id).await?;
    }
    Ok(())
}

pub(crate) async fn unwatch_delegated(
    http: &reqwest::Client,
    control: &PgPool,
    dek: &crate::crypto::DekPlaneSource,
    resource: &str,
    account_id: Uuid,
    user_id: Uuid,
) -> Result<(), ConnectorError> {
    let graph = HttpDoorbell { http };
    let tokens = DekWatcherTokens { control, dek, http };
    unwatch_container(control, &graph, &tokens, resource, account_id, user_id).await
}

/// `(subscription_id, client_state)` for every doorbell among `ids`, in one
/// query, for the webhook's per-batch verification
/// (`routes::graph_webhook::route_secrets`).
pub(crate) async fn client_states_for(
    pool: &PgPool,
    ids: &[String],
) -> Result<Vec<(String, String)>, ConnectorError> {
    sqlx::query_as(
        "SELECT subscription_id, client_state FROM teams_container_subscriptions WHERE subscription_id = ANY($1)",
    )
    .bind(ids)
    .fetch_all(pool)
    .await
    .map_err(internal)
}

pub(crate) async fn lookup_subscription(
    pool: &PgPool,
    subscription_id: &str,
) -> Result<Option<DoorbellSubscription>, ConnectorError> {
    let row: Option<(String, String, String, String, Option<Uuid>, Option<Uuid>)> = sqlx::query_as(
        "SELECT ms_tenant_id, resource, client_state, subscription_id, \
                owner_account_id, owner_user_id \
         FROM teams_container_subscriptions \
         WHERE subscription_id = $1",
    )
    .bind(subscription_id)
    .fetch_optional(pool)
    .await
    .map_err(internal)?;
    Ok(row.map(|(ms_tenant_id, resource, client_state, subscription_id, owner_account_id, owner_user_id)| {
        DoorbellSubscription {
            ms_tenant_id,
            resource,
            client_state,
            subscription_id,
            owner_account_id,
            owner_user_id,
        }
    }))
}

/// Fan a notification out by calling `op` once per watcher. `Drop` removes
/// only that watcher. The closure owns the fetch, so one watcher's message
/// body is never handed to another watcher.
pub(crate) async fn for_each_watcher<F, Fut>(
    pool: &PgPool,
    ms_tenant_id: &str,
    resource: &str,
    mut op: F,
) -> Result<(), ConnectorError>
where
    F: FnMut(ContainerWatcher) -> Fut,
    Fut: Future<Output = WatcherOp>,
{
    let rows: Vec<(Uuid, Uuid)> = sqlx::query_as(
        "SELECT account_id, user_id FROM teams_container_watchers \
         WHERE ms_tenant_id = $1 AND resource = $2 \
         ORDER BY user_id",
    )
    .bind(ms_tenant_id)
    .bind(resource)
    .fetch_all(pool)
    .await
    .map_err(internal)?;
    for (account_id, user_id) in rows {
        if op(ContainerWatcher { account_id, user_id }).await == WatcherOp::Drop {
            sqlx::query(
                "DELETE FROM teams_container_watchers \
                 WHERE ms_tenant_id = $1 AND resource = $2 \
                   AND account_id = $3 AND user_id = $4",
            )
            .bind(ms_tenant_id)
            .bind(resource)
            .bind(account_id)
            .bind(user_id)
            .execute(pool)
            .await
            .map_err(internal)?;
        }
    }
    Ok(())
}

pub(crate) async fn note_successful_fetch(
    pool: &PgPool,
    ms_tenant_id: &str,
    resource: &str,
    account_id: Uuid,
    user_id: Uuid,
) -> Result<(), ConnectorError> {
    sqlx::query(
        "UPDATE teams_container_watchers \
         SET last_fetch_ok_at = now(), updated_at = now() \
         WHERE ms_tenant_id = $1 AND resource = $2 AND account_id = $3 AND user_id = $4",
    )
    .bind(ms_tenant_id)
    .bind(resource)
    .bind(account_id)
    .bind(user_id)
    .execute(pool)
    .await
    .map_err(internal)?;
    Ok(())
}

async fn unwatch_one<G: DoorbellGraph, T: WatcherTokens>(
    pool: &PgPool,
    graph: &G,
    tokens: &T,
    ms_tenant_id: &str,
    resource: &str,
    account_id: Uuid,
    user_id: Uuid,
) -> Result<(), ConnectorError> {
    let sub = load_subscription(pool, ms_tenant_id, resource).await?;
    let count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM teams_container_watchers \
         WHERE ms_tenant_id = $1 AND resource = $2",
    )
    .bind(ms_tenant_id)
    .bind(resource)
    .fetch_one(pool)
    .await
    .map_err(internal)?;
    let org = account_is_org(pool, account_id).await?;
    let token = tokens.access_token(account_id, user_id, org).await;
    let is_owner = sub.as_ref().is_some_and(|s| {
        s.owner_account_id == Some(account_id) && s.owner_user_id == Some(user_id)
    });

    if count <= 1 {
        if let (Some(sub), Some(token)) = (sub.as_ref(), token.as_deref()) {
            if let Some(id) = sub.subscription_id.as_deref() {
                if let Err(e) = graph.delete_subscription(token, id).await {
                    tracing::info!(
                        error = %e, %account_id, %resource,
                        "teams doorbell: last watcher upstream delete failed"
                    );
                }
            }
        }
        delete_watcher(pool, ms_tenant_id, resource, account_id, user_id).await?;
        sqlx::query(
            "DELETE FROM teams_container_subscriptions \
             WHERE ms_tenant_id = $1 AND resource = $2",
        )
        .bind(ms_tenant_id)
        .bind(resource)
        .execute(pool)
        .await
        .map_err(internal)?;
        return Ok(());
    }

    delete_watcher(pool, ms_tenant_id, resource, account_id, user_id).await?;
    if !is_owner {
        return Ok(());
    }

    if let (Some(sub), Some(token)) = (sub.as_ref(), token.as_deref()) {
        if let Some(id) = sub.subscription_id.as_deref() {
            if let Err(e) = graph.delete_subscription(token, id).await {
                tracing::info!(
                    error = %e, %account_id, %resource,
                    "teams doorbell: departing owner upstream delete failed"
                );
            }
        }
    }
    sqlx::query(
        "UPDATE teams_container_subscriptions \
         SET subscription_id = NULL, owner_account_id = NULL, owner_user_id = NULL, \
             state = 'electing', last_owner_change_at = now(), updated_at = now() \
         WHERE ms_tenant_id = $1 AND resource = $2",
    )
    .bind(ms_tenant_id)
    .bind(resource)
    .execute(pool)
    .await
    .map_err(internal)?;
    try_create(pool, graph, tokens, ms_tenant_id, resource).await
}

async fn try_create<G: DoorbellGraph, T: WatcherTokens>(
    pool: &PgPool,
    graph: &G,
    tokens: &T,
    ms_tenant_id: &str,
    resource: &str,
) -> Result<(), ConnectorError> {
    // A row that already has a Graph id is renewed in place. Creating a
    // second subscription hits Graph's one-slot rule.
    if renew_existing(pool, graph, tokens, ms_tenant_id, resource).await? {
        return Ok(());
    }
    create_under(pool, graph, tokens, ms_tenant_id, resource, &[]).await
}

async fn create_under<G: DoorbellGraph, T: WatcherTokens>(
    pool: &PgPool,
    graph: &G,
    tokens: &T,
    ms_tenant_id: &str,
    resource: &str,
    exclude_seed: &[(Uuid, Uuid)],
) -> Result<(), ConnectorError> {
    let client_state = client_state_of(pool, ms_tenant_id, resource).await?;
    let previous = load_subscription(pool, ms_tenant_id, resource).await?;
    let previous_owner = previous.as_ref().and_then(|s| s.owner_user_id);
    let mut tried: Vec<(Uuid, Uuid)> = exclude_seed.to_vec();
    let mut last_error: Option<String> = None;
    for _ in 0..MAX_CONTAINER_OWNER_CANDIDATES {
        let Some(owner) = elect_container_owner(pool, ms_tenant_id, resource, &tried).await? else {
            break;
        };
        tried.push((owner.account_id, owner.user_id));
        let Some(token) = tokens.access_token(owner.account_id, owner.user_id, owner.org_account).await else {
            last_error = Some(format!(
                "no usable token for {}/{}",
                owner.account_id, owner.user_id
            ));
            continue;
        };
        match graph.create_subscription(&token, resource, &client_state).await {
            Ok(created) => {
                mark_live(pool, ms_tenant_id, resource, &created, &client_state, owner).await?;
                if previous_owner.is_some_and(|prev| prev != owner.user_id) {
                    audit_owner_failover(
                        pool,
                        owner.account_id,
                        ms_tenant_id,
                        resource,
                        previous_owner,
                        owner.user_id,
                    )
                    .await;
                }
                let _ = crate::services::connector_health::note_teams_push_recovered(
                    pool,
                    ms_tenant_id,
                    resource,
                )
                .await;
                return Ok(());
            }
            Err(DoorCreateError::SlotTaken(msg)) => {
                mark_orphan_blocked(pool, ms_tenant_id, resource, &msg).await?;
                note_push_lost(pool, ms_tenant_id, resource, "orphan_blocked").await;
                return Ok(());
            }
            Err(DoorCreateError::Failed(msg)) => {
                if msg.to_ascii_lowercase().contains("quota") {
                    mark_polling_keep(pool, ms_tenant_id, resource, "quota", Some(&msg)).await?;
                    note_push_lost(pool, ms_tenant_id, resource, "quota").await;
                    return Ok(());
                }
                last_error = Some(msg);
            }
        }
    }
    mark_no_owner(pool, ms_tenant_id, resource, last_error.as_deref()).await?;
    note_push_lost(pool, ms_tenant_id, resource, "no_eligible_owner").await;
    Ok(())
}

#[derive(Debug)]
struct SubRow {
    subscription_id: Option<String>,
    owner_account_id: Option<Uuid>,
    owner_user_id: Option<Uuid>,
}

async fn load_subscription(
    pool: &PgPool,
    ms_tenant_id: &str,
    resource: &str,
) -> Result<Option<SubRow>, ConnectorError> {
    let row: Option<(Option<String>, Option<Uuid>, Option<Uuid>)> = sqlx::query_as(
        "SELECT subscription_id, owner_account_id, owner_user_id \
         FROM teams_container_subscriptions \
         WHERE ms_tenant_id = $1 AND resource = $2",
    )
    .bind(ms_tenant_id)
    .bind(resource)
    .fetch_optional(pool)
    .await
    .map_err(internal)?;
    Ok(row.map(|(subscription_id, owner_account_id, owner_user_id)| SubRow {
        subscription_id,
        owner_account_id,
        owner_user_id,
    }))
}

/// `false` when a live subscription is already healthy, or the slot is
/// blocked until `blocked_until`. Otherwise the row is in `electing` and
/// the caller should try Graph.
async fn claim_election(
    pool: &PgPool,
    ms_tenant_id: &str,
    resource: &str,
) -> Result<bool, ConnectorError> {
    let window = microsoft::teams_chat_message_refresh_window_minutes();
    let mut tx = pool.begin().await.map_err(internal)?;
    let row: Option<(String, Option<chrono::DateTime<Utc>>, Option<chrono::DateTime<Utc>>, Option<String>)> =
        sqlx::query_as(
            "SELECT state, expires_at, blocked_until, subscription_id \
             FROM teams_container_subscriptions \
             WHERE ms_tenant_id = $1 AND resource = $2 \
             FOR UPDATE",
        )
        .bind(ms_tenant_id)
        .bind(resource)
        .fetch_optional(&mut *tx)
        .await
        .map_err(internal)?;
    let Some((state, expires_at, blocked_until, subscription_id)) = row else {
        tx.rollback().await.ok();
        return Ok(false);
    };
    let fresh = expires_at.is_some_and(|exp| {
        exp > Utc::now() + Duration::minutes(window)
    });
    if state == "live" && fresh && subscription_id.is_some() {
        tx.commit().await.map_err(internal)?;
        return Ok(false);
    }
    if state == "polling" && blocked_until.is_some_and(|until| until > Utc::now()) {
        tx.commit().await.map_err(internal)?;
        return Ok(false);
    }
    sqlx::query(
        "UPDATE teams_container_subscriptions \
         SET state = 'electing', updated_at = now() \
         WHERE ms_tenant_id = $1 AND resource = $2",
    )
    .bind(ms_tenant_id)
    .bind(resource)
    .execute(&mut *tx)
    .await
    .map_err(internal)?;
    tx.commit().await.map_err(internal)?;
    Ok(true)
}

async fn ensure_subscription_row(
    pool: &PgPool,
    ms_tenant_id: &str,
    resource: &str,
) -> Result<(), ConnectorError> {
    let client_state = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
    sqlx::query(
        "INSERT INTO teams_container_subscriptions \
            (ms_tenant_id, resource, client_state, state) \
         VALUES ($1, $2, $3, 'electing') \
         ON CONFLICT (ms_tenant_id, resource) DO NOTHING",
    )
    .bind(ms_tenant_id)
    .bind(resource)
    .bind(&client_state)
    .execute(pool)
    .await
    .map_err(internal)?;
    Ok(())
}

async fn upsert_watcher(
    pool: &PgPool,
    ms_tenant_id: &str,
    resource: &str,
    account_id: Uuid,
    user_id: Uuid,
) -> Result<(), ConnectorError> {
    sqlx::query(
        "INSERT INTO teams_container_watchers \
            (ms_tenant_id, resource, account_id, user_id) \
         VALUES ($1, $2, $3, $4) \
         ON CONFLICT (ms_tenant_id, resource, account_id, user_id) \
         DO UPDATE SET updated_at = now()",
    )
    .bind(ms_tenant_id)
    .bind(resource)
    .bind(account_id)
    .bind(user_id)
    .execute(pool)
    .await
    .map_err(internal)?;
    Ok(())
}

async fn delete_watcher(
    pool: &PgPool,
    ms_tenant_id: &str,
    resource: &str,
    account_id: Uuid,
    user_id: Uuid,
) -> Result<(), ConnectorError> {
    sqlx::query(
        "DELETE FROM teams_container_watchers \
         WHERE ms_tenant_id = $1 AND resource = $2 AND account_id = $3 AND user_id = $4",
    )
    .bind(ms_tenant_id)
    .bind(resource)
    .bind(account_id)
    .bind(user_id)
    .execute(pool)
    .await
    .map_err(internal)?;
    Ok(())
}

async fn client_state_of(
    pool: &PgPool,
    ms_tenant_id: &str,
    resource: &str,
) -> Result<String, ConnectorError> {
    sqlx::query_scalar(
        "SELECT client_state FROM teams_container_subscriptions \
         WHERE ms_tenant_id = $1 AND resource = $2",
    )
    .bind(ms_tenant_id)
    .bind(resource)
    .fetch_one(pool)
    .await
    .map_err(internal)
}

async fn mark_live(
    pool: &PgPool,
    ms_tenant_id: &str,
    resource: &str,
    created: &CreatedDoor,
    client_state: &str,
    owner: ElectedOwner,
) -> Result<(), ConnectorError> {
    sqlx::query(
        "UPDATE teams_container_subscriptions \
         SET subscription_id = $3, client_state = $4, expires_at = $5, \
             owner_account_id = $6, owner_user_id = $7, state = 'live', \
             polling_reason = NULL, blocked_until = NULL, last_error = NULL, \
             last_owner_change_at = now(), updated_at = now() \
         WHERE ms_tenant_id = $1 AND resource = $2 \
           AND (subscription_id IS NULL OR subscription_id = $3)",
    )
    .bind(ms_tenant_id)
    .bind(resource)
    .bind(&created.subscription_id)
    .bind(client_state)
    .bind(created.expires_at)
    .bind(owner.account_id)
    .bind(owner.user_id)
    .execute(pool)
    .await
    .map_err(internal)?;
    Ok(())
}

async fn mark_orphan_blocked(
    pool: &PgPool,
    ms_tenant_id: &str,
    resource: &str,
    error: &str,
) -> Result<(), ConnectorError> {
    let minutes = microsoft::teams_chat_message_subscription_minutes();
    sqlx::query(
        "UPDATE teams_container_subscriptions \
         SET state = 'polling', polling_reason = 'orphan_blocked', \
             blocked_until = now() + make_interval(mins => $3), \
             last_error = $4, updated_at = now() \
         WHERE ms_tenant_id = $1 AND resource = $2 AND subscription_id IS NULL",
    )
    .bind(ms_tenant_id)
    .bind(resource)
    .bind(minutes as i32)
    .bind(error)
    .execute(pool)
    .await
    .map_err(internal)?;
    Ok(())
}

async fn mark_no_owner(
    pool: &PgPool,
    ms_tenant_id: &str,
    resource: &str,
    error: Option<&str>,
) -> Result<(), ConnectorError> {
    sqlx::query(
        "UPDATE teams_container_subscriptions \
         SET state = 'polling', polling_reason = 'no_eligible_owner', \
             last_error = $3, updated_at = now() \
         WHERE ms_tenant_id = $1 AND resource = $2 \
           AND subscription_id IS NULL AND state = 'electing'",
    )
    .bind(ms_tenant_id)
    .bind(resource)
    .bind(error)
    .execute(pool)
    .await
    .map_err(internal)?;
    Ok(())
}

async fn microsoft_tid(
    pool: &PgPool,
    account_id: Uuid,
    user_id: Uuid,
) -> Result<Option<String>, ConnectorError> {
    let org = account_is_org(pool, account_id).await?;
    let owner: Option<Uuid> = if org { Some(user_id) } else { None };
    let mut tx = crate::tenancy::scope::begin_account_scoped(pool, account_id)
        .await
        .map_err(|e| ConnectorError::Internal(e.to_string()))?;
    let tid: Option<String> = sqlx::query_scalar(
        "SELECT provider_workspace_id FROM oauth_tokens \
         WHERE account_id = $1 AND provider = 'microsoft' \
           AND credential_invalid_at IS NULL \
           AND owner_user_id IS NOT DISTINCT FROM $2 \
           AND provider_workspace_id <> '' \
         LIMIT 1",
    )
    .bind(account_id)
    .bind(owner)
    .fetch_optional(&mut *tx)
    .await
    .map_err(internal)?;
    tx.rollback().await.map_err(internal)?;
    Ok(tid.filter(|t| !t.is_empty()))
}

async fn account_is_org(pool: &PgPool, account_id: Uuid) -> Result<bool, ConnectorError> {
    let org: Option<bool> = sqlx::query_scalar(
        "SELECT clerk_org_id IS NOT NULL FROM accounts WHERE id = $1",
    )
    .bind(account_id)
    .fetch_optional(pool)
    .await
    .map_err(internal)?;
    Ok(org.unwrap_or(false))
}

fn internal(e: sqlx::Error) -> ConnectorError {
    ConnectorError::Internal(e.to_string())
}

/// What a lifecycle notification should do after the doorbell row is updated.
/// `resync_watchers` means each watcher re-lists on their own token (the
/// existing backstop). Message bodies are never copied across watchers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LifecycleOutcome {
    pub resync_watchers: bool,
}

/// Doorbell lifecycle. The subscription id is a `teams_container_subscriptions`
/// row, not `graph_subscriptions`. Mail and RSC never reach this.
pub(crate) async fn handle_doorbell_lifecycle<G: DoorbellGraph, T: WatcherTokens>(
    pool: &PgPool,
    graph: &G,
    tokens: &T,
    ms_tenant_id: &str,
    resource: &str,
    event: &str,
) -> Result<LifecycleOutcome, ConnectorError> {
    match event {
        "reauthorizationRequired" => {
            renew_existing(pool, graph, tokens, ms_tenant_id, resource).await?;
            Ok(LifecycleOutcome { resync_watchers: false })
        }
        "subscriptionRemoved" => {
            let created = recreate_after_removed(pool, graph, tokens, ms_tenant_id, resource).await?;
            Ok(LifecycleOutcome { resync_watchers: created })
        }
        "missed" => {
            // Keep the row. Each watcher resyncs on their own token.
            Ok(LifecycleOutcome { resync_watchers: true })
        }
        _ => Ok(LifecycleOutcome { resync_watchers: false }),
    }
}

/// Containers whose capture is polling. The 30–60 minute tick's work set.
/// Live doorbells are not included; mail is not this table.
pub(crate) async fn polling_containers(
    pool: &PgPool,
) -> Result<Vec<(String, String)>, ConnectorError> {
    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT ms_tenant_id, resource FROM teams_container_subscriptions \
         WHERE state = 'polling' \
         ORDER BY ms_tenant_id, resource",
    )
    .fetch_all(pool)
    .await
    .map_err(internal)?;
    Ok(rows)
}

/// Resources this account watches that are in `state = 'polling'`.
pub(crate) async fn polling_resources_for_account(
    pool: &PgPool,
    account_id: Uuid,
) -> Result<Vec<String>, ConnectorError> {
    let rows: Vec<(String,)> = sqlx::query_as(
        "SELECT DISTINCT w.resource \
         FROM teams_container_watchers w \
         JOIN teams_container_subscriptions s \
           ON s.ms_tenant_id = w.ms_tenant_id AND s.resource = w.resource \
         WHERE w.account_id = $1 AND s.state = 'polling' \
         ORDER BY w.resource",
    )
    .bind(account_id)
    .fetch_all(pool)
    .await
    .map_err(internal)?;
    Ok(rows.into_iter().map(|(r,)| r).collect())
}

/// Credential death, departure, or disconnect of `user_id`. Re-elect every
/// doorbell that member still owns. Does not call Graph — the caller that
/// still holds a token runs [`prepare_owner_departure`] first when an
/// upstream delete or PATCH is required. `drop_own_watch` removes only this
/// member's watcher rows (departure and disconnect). Credential death leaves
/// the watch so a reconnect can be elected again.
pub(crate) async fn reelect_after_owner_lost(
    pool: &PgPool,
    account_id: Uuid,
    user_id: Uuid,
    drop_own_watch: bool,
) -> Result<(), ConnectorError> {
    let owned: Vec<(String, String)> = sqlx::query_as(
        "SELECT ms_tenant_id, resource FROM teams_container_subscriptions \
         WHERE owner_account_id = $1 AND owner_user_id = $2",
    )
    .bind(account_id)
    .bind(user_id)
    .fetch_all(pool)
    .await
    .map_err(internal)?;
    for (ms_tenant_id, resource) in owned {
        match elect_container_owner(pool, &ms_tenant_id, &resource, &[(account_id, user_id)]).await?
        {
            Some(next) => {
                sqlx::query(
                    "UPDATE teams_container_subscriptions \
                     SET owner_account_id = $3, owner_user_id = $4, \
                         last_owner_change_at = now(), updated_at = now() \
                     WHERE ms_tenant_id = $1 AND resource = $2 \
                       AND owner_user_id = $5",
                )
                .bind(&ms_tenant_id)
                .bind(&resource)
                .bind(next.account_id)
                .bind(next.user_id)
                .bind(user_id)
                .execute(pool)
                .await
                .map_err(internal)?;
                audit_owner_failover(
                    pool,
                    next.account_id,
                    &ms_tenant_id,
                    &resource,
                    Some(user_id),
                    next.user_id,
                )
                .await;
                // Electing a successor is not the same as a live Graph
                // subscription. Close health only when the row is already
                // live with a subscription id; otherwise keep the push
                // condition open.
                let status: Option<(bool, Option<String>)> = sqlx::query_as(
                    "SELECT state = 'live' AND subscription_id IS NOT NULL, polling_reason \
                     FROM teams_container_subscriptions \
                     WHERE ms_tenant_id = $1 AND resource = $2",
                )
                .bind(&ms_tenant_id)
                .bind(&resource)
                .fetch_optional(pool)
                .await
                .map_err(internal)?;
                match status {
                    Some((true, _)) => {
                        let _ = crate::services::connector_health::note_teams_push_recovered(
                            pool,
                            &ms_tenant_id,
                            &resource,
                        )
                        .await;
                    }
                    Some((_, reason)) => {
                        note_push_lost(
                            pool,
                            &ms_tenant_id,
                            &resource,
                            reason.as_deref().unwrap_or("no_eligible_owner"),
                        )
                        .await;
                    }
                    None => {
                        note_push_lost(pool, &ms_tenant_id, &resource, "no_eligible_owner").await;
                    }
                }
            }
            None => {
                mark_polling_keep(pool, &ms_tenant_id, &resource, "no_eligible_owner", None).await?;
                note_push_lost(pool, &ms_tenant_id, &resource, "no_eligible_owner").await;
            }
        }
    }
    if drop_own_watch {
        let resources: Vec<(String, String)> = sqlx::query_as(
            "SELECT ms_tenant_id, resource FROM teams_container_watchers \
             WHERE account_id = $1 AND user_id = $2",
        )
        .bind(account_id)
        .bind(user_id)
        .fetch_all(pool)
        .await
        .map_err(internal)?;
        sqlx::query(
            "DELETE FROM teams_container_watchers \
             WHERE account_id = $1 AND user_id = $2",
        )
        .bind(account_id)
        .bind(user_id)
        .execute(pool)
        .await
        .map_err(internal)?;
        for (ms_tenant_id, resource) in resources {
            let left: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM teams_container_watchers \
                 WHERE ms_tenant_id = $1 AND resource = $2",
            )
            .bind(&ms_tenant_id)
            .bind(&resource)
            .fetch_one(pool)
            .await
            .map_err(internal)?;
            if left == 0 {
                sqlx::query(
                    "DELETE FROM teams_container_subscriptions \
                     WHERE ms_tenant_id = $1 AND resource = $2",
                )
                .bind(&ms_tenant_id)
                .bind(&resource)
                .execute(pool)
                .await
                .map_err(internal)?;
            }
        }
    }
    Ok(())
}

/// Align Graph with the elected owner while `departing_token` still works.
/// Called before the departing member's `oauth_tokens` row is deleted.
/// A successor PATCHes the existing id when they can; the old id is deleted
/// only when nobody else can take it, or when Graph's one-slot rule blocks
/// a create. Other members' watcher rows are not touched.
pub(crate) async fn prepare_owner_departure<G: DoorbellGraph, T: WatcherTokens>(
    pool: &PgPool,
    graph: &G,
    tokens: &T,
    account_id: Uuid,
    user_id: Uuid,
    departing_token: Option<&str>,
) -> Result<(), ConnectorError> {
    let owned: Vec<(String, String, Option<String>)> = sqlx::query_as(
        "SELECT ms_tenant_id, resource, subscription_id \
         FROM teams_container_subscriptions \
         WHERE owner_account_id = $1 AND owner_user_id = $2",
    )
    .bind(account_id)
    .bind(user_id)
    .fetch_all(pool)
    .await
    .map_err(internal)?;
    for (ms_tenant_id, resource, sub_id) in owned {
        let exclude = vec![(account_id, user_id)];
        if handoff_subscription(
            pool,
            graph,
            tokens,
            &ms_tenant_id,
            &resource,
            sub_id.as_deref(),
            &exclude,
            user_id,
            departing_token,
        )
        .await?
        {
            continue;
        }
        if let (Some(id), Some(token)) = (sub_id.as_deref(), departing_token) {
            if let Err(e) = graph.delete_subscription(token, id).await {
                tracing::info!(
                    error = %e, %account_id, %resource,
                    "teams doorbell: no successor, upstream delete failed"
                );
            }
        }
        sqlx::query(
            "UPDATE teams_container_subscriptions \
             SET subscription_id = NULL, state = 'polling', \
                 polling_reason = 'no_eligible_owner', updated_at = now() \
             WHERE ms_tenant_id = $1 AND resource = $2",
        )
        .bind(&ms_tenant_id)
        .bind(&resource)
        .execute(pool)
        .await
        .map_err(internal)?;
        note_push_lost(pool, &ms_tenant_id, &resource, "no_eligible_owner").await;
    }
    Ok(())
}

/// `true` when a successor now holds a live doorbell. `false` when nobody
/// eligible remains, or a SlotTaken retry did not become [`HandoffCreate::Live`].
async fn handoff_subscription<G: DoorbellGraph, T: WatcherTokens>(
    pool: &PgPool,
    graph: &G,
    tokens: &T,
    ms_tenant_id: &str,
    resource: &str,
    sub_id: Option<&str>,
    exclude: &[(Uuid, Uuid)],
    from_user: Uuid,
    departing_token: Option<&str>,
) -> Result<bool, ConnectorError> {
    let mut tried = exclude.to_vec();
    for _ in 0..MAX_CONTAINER_OWNER_CANDIDATES {
        let Some(owner) = elect_container_owner(pool, ms_tenant_id, resource, &tried).await? else {
            break;
        };
        let Some(token) = tokens
            .access_token(owner.account_id, owner.user_id, owner.org_account)
            .await
        else {
            tried.push((owner.account_id, owner.user_id));
            continue;
        };
        if let Some(id) = sub_id {
            match graph.renew_subscription(&token, id, resource).await {
                Ok(expires_at) => {
                    mark_renewed(pool, ms_tenant_id, resource, expires_at, owner).await?;
                    audit_owner_failover(
                        pool,
                        owner.account_id,
                        ms_tenant_id,
                        resource,
                        Some(from_user),
                        owner.user_id,
                    )
                    .await;
                    let _ = crate::services::connector_health::note_teams_push_recovered(
                        pool,
                        ms_tenant_id,
                        resource,
                    )
                    .await;
                    return Ok(true);
                }
                Err(DoorPatchError::RetryLater) => {
                    restore_live_keep_owner(pool, ms_tenant_id, resource, Some(id)).await?;
                    return Ok(true);
                }
                Err(DoorPatchError::Missing) => {}
                Err(DoorPatchError::Reelect) => {
                    tried.push((owner.account_id, owner.user_id));
                    continue;
                }
            }
        }
        match create_for_owner(pool, graph, ms_tenant_id, resource, owner, &token).await? {
            HandoffCreate::Live => {
                audit_owner_failover(
                    pool,
                    owner.account_id,
                    ms_tenant_id,
                    resource,
                    Some(from_user),
                    owner.user_id,
                )
                .await;
                return Ok(true);
            }
            HandoffCreate::SlotTaken => {
                if let (Some(id), Some(old)) = (sub_id, departing_token) {
                    let _ = graph.delete_subscription(old, id).await;
                    if matches!(
                        create_for_owner(pool, graph, ms_tenant_id, resource, owner, &token).await?,
                        HandoffCreate::Live
                    ) {
                        audit_owner_failover(
                            pool,
                            owner.account_id,
                            ms_tenant_id,
                            resource,
                            Some(from_user),
                            owner.user_id,
                        )
                        .await;
                        return Ok(true);
                    }
                }
                tried.push((owner.account_id, owner.user_id));
            }
            HandoffCreate::Retry => {
                tried.push((owner.account_id, owner.user_id));
            }
        }
    }
    Ok(false)
}

enum HandoffCreate {
    Live,
    SlotTaken,
    Retry,
}

async fn create_for_owner<G: DoorbellGraph>(
    pool: &PgPool,
    graph: &G,
    ms_tenant_id: &str,
    resource: &str,
    owner: ElectedOwner,
    token: &str,
) -> Result<HandoffCreate, ConnectorError> {
    let client_state = client_state_of(pool, ms_tenant_id, resource).await?;
    clear_subscription_id(pool, ms_tenant_id, resource).await?;
    match graph.create_subscription(token, resource, &client_state).await {
        Ok(created) => {
            mark_live(pool, ms_tenant_id, resource, &created, &client_state, owner).await?;
            let _ = crate::services::connector_health::note_teams_push_recovered(
                pool,
                ms_tenant_id,
                resource,
            )
            .await;
            Ok(HandoffCreate::Live)
        }
        Err(DoorCreateError::SlotTaken(msg)) => {
            mark_orphan_blocked(pool, ms_tenant_id, resource, &msg).await?;
            note_push_lost(pool, ms_tenant_id, resource, "orphan_blocked").await;
            Ok(HandoffCreate::SlotTaken)
        }
        Err(DoorCreateError::Failed(msg)) => {
            if msg.to_ascii_lowercase().contains("quota") {
                mark_polling_keep(pool, ms_tenant_id, resource, "quota", Some(&msg)).await?;
                note_push_lost(pool, ms_tenant_id, resource, "quota").await;
            }
            Ok(HandoffCreate::Retry)
        }
    }
}

/// PATCH the current Graph id with the elected owner. `true` when the row
/// was kept (renewed, throttled, or left polling because nobody can renew).
/// `false` when the id is gone and the caller should create a new one.
async fn renew_existing<G: DoorbellGraph, T: WatcherTokens>(
    pool: &PgPool,
    graph: &G,
    tokens: &T,
    ms_tenant_id: &str,
    resource: &str,
) -> Result<bool, ConnectorError> {
    let Some(sub) = load_subscription(pool, ms_tenant_id, resource).await? else {
        return Ok(false);
    };
    let Some(id) = sub.subscription_id.clone() else {
        return Ok(false);
    };
    let previous_owner = sub.owner_user_id;
    let mut tried: Vec<(Uuid, Uuid)> = Vec::new();
    for _ in 0..MAX_CONTAINER_OWNER_CANDIDATES {
        let Some(owner) = elect_container_owner(pool, ms_tenant_id, resource, &tried).await? else {
            break;
        };
        let Some(token) = tokens
            .access_token(owner.account_id, owner.user_id, owner.org_account)
            .await
        else {
            tried.push((owner.account_id, owner.user_id));
            continue;
        };
        match graph.renew_subscription(&token, &id, resource).await {
            Ok(expires_at) => {
                mark_renewed(pool, ms_tenant_id, resource, expires_at, owner).await?;
                if previous_owner.is_some_and(|prev| prev != owner.user_id) {
                    audit_owner_failover(
                        pool,
                        owner.account_id,
                        ms_tenant_id,
                        resource,
                        previous_owner,
                        owner.user_id,
                    )
                    .await;
                }
                let _ = crate::services::connector_health::note_teams_push_recovered(
                    pool,
                    ms_tenant_id,
                    resource,
                )
                .await;
                return Ok(true);
            }
            Err(DoorPatchError::RetryLater) => {
                restore_live_keep_owner(pool, ms_tenant_id, resource, Some(&id)).await?;
                return Ok(true);
            }
            Err(DoorPatchError::Missing) => {
                clear_subscription_id(pool, ms_tenant_id, resource).await?;
                return Ok(false);
            }
            Err(DoorPatchError::Reelect) => {
                tried.push((owner.account_id, owner.user_id));
            }
        }
    }
    mark_polling_keep(pool, ms_tenant_id, resource, "no_eligible_owner", None).await?;
    note_push_lost(pool, ms_tenant_id, resource, "no_eligible_owner").await;
    Ok(true)
}

async fn recreate_after_removed<G: DoorbellGraph, T: WatcherTokens>(
    pool: &PgPool,
    graph: &G,
    tokens: &T,
    ms_tenant_id: &str,
    resource: &str,
) -> Result<bool, ConnectorError> {
    let sub = load_subscription(pool, ms_tenant_id, resource).await?;
    let mut exclude: Vec<(Uuid, Uuid)> = Vec::new();
    if let Some(sub) = sub.as_ref() {
        if let (Some(account_id), Some(user_id)) = (sub.owner_account_id, sub.owner_user_id) {
            let org = account_is_org(pool, account_id).await?;
            // 429/5xx (`RetryLater`) keep the current owner. Exclude only
            // when the access token is missing or the probe is a hard
            // `Reelect` (revoked/401/403).
            match tokens.access_token(account_id, user_id, org).await {
                None => exclude.push((account_id, user_id)),
                Some(token) => {
                    if matches!(graph.probe_token(&token).await, Err(DoorPatchError::Reelect)) {
                        exclude.push((account_id, user_id));
                    }
                }
            }
        }
    }
    clear_subscription_id(pool, ms_tenant_id, resource).await?;
    create_under(pool, graph, tokens, ms_tenant_id, resource, &exclude).await?;
    let recreated: Option<bool> = sqlx::query_scalar(
        "SELECT state = 'live' AND subscription_id IS NOT NULL \
         FROM teams_container_subscriptions \
         WHERE ms_tenant_id = $1 AND resource = $2",
    )
    .bind(ms_tenant_id)
    .bind(resource)
    .fetch_optional(pool)
    .await
    .map_err(internal)?;
    Ok(recreated.unwrap_or(false))
}

async fn mark_renewed(
    pool: &PgPool,
    ms_tenant_id: &str,
    resource: &str,
    expires_at: chrono::DateTime<Utc>,
    owner: ElectedOwner,
) -> Result<(), ConnectorError> {
    sqlx::query(
        "UPDATE teams_container_subscriptions \
         SET expires_at = $3, owner_account_id = $4, owner_user_id = $5, \
             state = 'live', polling_reason = NULL, blocked_until = NULL, \
             last_error = NULL, \
             last_owner_change_at = CASE \
                 WHEN owner_user_id IS DISTINCT FROM $5 THEN now() \
                 ELSE last_owner_change_at \
             END, \
             updated_at = now() \
         WHERE ms_tenant_id = $1 AND resource = $2",
    )
    .bind(ms_tenant_id)
    .bind(resource)
    .bind(expires_at)
    .bind(owner.account_id)
    .bind(owner.user_id)
    .execute(pool)
    .await
    .map_err(internal)?;
    Ok(())
}

async fn restore_live_keep_owner(
    pool: &PgPool,
    ms_tenant_id: &str,
    resource: &str,
    subscription_id: Option<&str>,
) -> Result<(), ConnectorError> {
    sqlx::query(
        "UPDATE teams_container_subscriptions \
         SET state = 'live', updated_at = now() \
         WHERE ms_tenant_id = $1 AND resource = $2 \
           AND subscription_id IS NOT DISTINCT FROM $3",
    )
    .bind(ms_tenant_id)
    .bind(resource)
    .bind(subscription_id)
    .execute(pool)
    .await
    .map_err(internal)?;
    Ok(())
}

async fn clear_subscription_id(
    pool: &PgPool,
    ms_tenant_id: &str,
    resource: &str,
) -> Result<(), ConnectorError> {
    sqlx::query(
        "UPDATE teams_container_subscriptions \
         SET subscription_id = NULL, state = 'electing', updated_at = now() \
         WHERE ms_tenant_id = $1 AND resource = $2",
    )
    .bind(ms_tenant_id)
    .bind(resource)
    .execute(pool)
    .await
    .map_err(internal)?;
    Ok(())
}

async fn mark_polling_keep(
    pool: &PgPool,
    ms_tenant_id: &str,
    resource: &str,
    reason: &str,
    error: Option<&str>,
) -> Result<(), ConnectorError> {
    sqlx::query(
        "UPDATE teams_container_subscriptions \
         SET state = 'polling', polling_reason = $3, last_error = $4, updated_at = now() \
         WHERE ms_tenant_id = $1 AND resource = $2",
    )
    .bind(ms_tenant_id)
    .bind(resource)
    .bind(reason)
    .bind(error)
    .execute(pool)
    .await
    .map_err(internal)?;
    Ok(())
}

async fn note_push_lost(pool: &PgPool, ms_tenant_id: &str, resource: &str, reason: &str) {
    if let Err(e) = crate::services::connector_health::note_teams_push_lost(
        pool,
        ms_tenant_id,
        resource,
        reason,
    )
    .await
    {
        tracing::warn!(
            error = %e, ms_tenant_id, %resource,
            "teams doorbell: failed to open no_eligible_backer"
        );
    }
}

async fn audit_owner_failover(
    pool: &PgPool,
    account_id: Uuid,
    ms_tenant_id: &str,
    resource: &str,
    from_user: Option<Uuid>,
    to_user: Uuid,
) {
    crate::services::connector_audit::record_or_queue(
        pool,
        crate::services::connector_audit::AuditEntry {
            account_id,
            actor: crate::services::connector_audit::Actor::System,
            action: "route_container",
            provider: Some("microsoft".to_string()),
            subject: Some(resource.chars().take(256).collect()),
            detail: Some(serde_json::json!({
                "kind": "teams_container_owner_failover",
                "ms_tenant_id": ms_tenant_id,
                "from_user_id": from_user,
                "to_user_id": to_user,
            })),
        },
    )
    .await;
}

#[cfg(test)]
mod tests {
    /// The renewal PATCH's failure is classified on the status the error
    /// carries as a field (`ConnectorError::UpstreamStatus`): 401 and 403
    /// re-elect, 404 is missing, anything else (a 5xx, a 429, no status at
    /// all) waits. The text is read only for a revocation.
    #[test]
    fn classify_patch_failure_reads_the_status_field() {
        use super::*;
        let up = |status: u16, msg: &str| ConnectorError::upstream_code(status, msg);
        assert!(matches!(classify_patch_failure(&up(401, "subscription renew failed: 401 Unauthorized: ")), DoorPatchError::Reelect));
        assert!(matches!(classify_patch_failure(&up(403, "subscription renew failed: 403 Forbidden: ")), DoorPatchError::Reelect));
        assert!(matches!(classify_patch_failure(&up(404, "subscription renew failed: 404 Not Found: ")), DoorPatchError::Missing));
        assert!(matches!(classify_patch_failure(&up(404, "subscription delete: 404 Not Found")), DoorPatchError::Missing));
        assert!(matches!(classify_patch_failure(&up(429, "subscription renew failed: 429 Too Many Requests: ")), DoorPatchError::RetryLater));
        assert!(matches!(classify_patch_failure(&up(503, "subscription renew failed: 503 Service Unavailable: ")), DoorPatchError::RetryLater));
        assert!(matches!(classify_patch_failure(&ConnectorError::Exchange("graph request error: timed out".into())), DoorPatchError::RetryLater));
        assert!(
            matches!(classify_patch_failure(&ConnectorError::Exchange("subscription renew failed: 404 Not Found: ".into())), DoorPatchError::RetryLater),
            "a status written into the text is not a status"
        );
        assert!(matches!(classify_patch_failure(&ConnectorError::Exchange("token refresh failed: 400: invalid_grant".into())), DoorPatchError::Reelect));
        assert!(matches!(
            classify_patch_failure(&ConnectorError::CredentialRevoked {
                provider: "microsoft",
                reason: crate::connectors::CredentialDeathReason::MicrosoftAccountDisabled,
            }),
            DoorPatchError::Reelect
        ));
        assert!(matches!(classify_patch_failure(&ConnectorError::Internal("db".into())), DoorPatchError::RetryLater));
    }

    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;

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
        let pool = PgPoolOptions::new().connect(&with_db(base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();
        pool
    }

    async fn seed_user(pool: &PgPool) -> Uuid {
        sqlx::query_scalar(
            "INSERT INTO users(id, email) VALUES (gen_random_uuid(), $1) RETURNING id",
        )
        .bind(format!("doorbell-{}@test.local", Uuid::new_v4()))
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

    async fn seed_member(pool: &PgPool, account: Uuid, user: Uuid) {
        let tier: Uuid = sqlx::query_scalar(
            "SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true LIMIT 1",
        )
        .fetch_one(pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO account_members(account_id, user_id, role, seat_tier_id) \
             VALUES ($1, $2, 'member', $3)",
        )
        .bind(account)
        .bind(user)
        .bind(tier)
        .execute(pool)
        .await
        .unwrap();
    }

    async fn seed_ms_token(pool: &PgPool, account: Uuid, user: Uuid, tid: &str) {
        sqlx::query(
            "INSERT INTO oauth_tokens \
                (account_id, provider, scope, access_token, owner_user_id, provider_workspace_id) \
             VALUES ($1, 'microsoft', 'test', $2, $3, $4)",
        )
        .bind(account)
        .bind(vec![0u8])
        .bind(user)
        .bind(tid)
        .execute(pool)
        .await
        .unwrap();
    }

    struct MapTokens {
        a: Uuid,
        b: Uuid,
        token_a: String,
        token_b: String,
    }

    #[async_trait]
    impl WatcherTokens for MapTokens {
        async fn access_token(
            &self,
            _account_id: Uuid,
            user_id: Uuid,
            _org_account: bool,
        ) -> Option<String> {
            if user_id == self.a {
                Some(self.token_a.clone())
            } else if user_id == self.b {
                Some(self.token_b.clone())
            } else {
                None
            }
        }
    }

    struct RecordingGraph {
        creates: AtomicUsize,
        deletes: AtomicUsize,
        slot_taken: bool,
        saw_watcher_on_delete: AtomicBool,
        watch: Option<(PgPool, String, String)>,
        created_token: std::sync::Mutex<Option<String>>,
    }

    #[async_trait]
    impl DoorbellGraph for RecordingGraph {
        async fn create_subscription(
            &self,
            access_token: &str,
            _resource: &str,
            _client_state: &str,
        ) -> Result<CreatedDoor, DoorCreateError> {
            self.creates.fetch_add(1, Ordering::SeqCst);
            *self.created_token.lock().unwrap() = Some(access_token.to_string());
            if self.slot_taken {
                return Err(DoorCreateError::SlotTaken("already exists".into()));
            }
            Ok(CreatedDoor {
                subscription_id: format!("sub-{}", self.creates.load(Ordering::SeqCst)),
                expires_at: Utc::now() + Duration::minutes(4230),
            })
        }

        async fn delete_subscription(
            &self,
            _access_token: &str,
            _subscription_id: &str,
        ) -> Result<(), String> {
            self.deletes.fetch_add(1, Ordering::SeqCst);
            if let Some((pool, tenant, resource)) = &self.watch {
                let n: i64 = sqlx::query_scalar(
                    "SELECT count(*) FROM teams_container_watchers \
                     WHERE ms_tenant_id = $1 AND resource = $2",
                )
                .bind(tenant)
                .bind(resource)
                .fetch_one(pool)
                .await
                .unwrap();
                if n > 0 {
                    self.saw_watcher_on_delete.store(true, Ordering::SeqCst);
                }
            }
            Ok(())
        }

        async fn renew_subscription(
            &self,
            _access_token: &str,
            _subscription_id: &str,
            _resource: &str,
        ) -> Result<chrono::DateTime<Utc>, DoorPatchError> {
            Ok(Utc::now() + Duration::minutes(4230))
        }

        async fn probe_token(&self, _access_token: &str) -> Result<(), DoorPatchError> {
            Ok(())
        }
    }

    const TENANT: &str = "ms-tenant-1";
    const RESOURCE: &str = "teams/t1/channels/c1/messages";

    async fn app_pool_with_empty_scope(base: &str, db: &str) -> PgPool {
        PgPoolOptions::new()
            .max_connections(1)
            .after_connect(|conn, _| Box::pin(async move {
                sqlx::query("SET ROLE coralswarm_app").execute(&mut *conn).await?;
                sqlx::query("SELECT set_config('app.account_id', '', false)")
                    .execute(&mut *conn).await?;
                Ok(())
            }))
            .connect(&with_db(base, db)).await.unwrap()
    }

    #[tokio::test]
    async fn member_tid_uses_account_scope_with_strict_app_role_pg() {
        let Some(base) = base_url() else { return; };
        let db = "orgt_g3_doorbell_tid_rls";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        recreate(&base, db).await.close().await.unwrap();
        let admin = control_pool(&base, db).await;
        let user = seed_user(&admin).await;
        let account = seed_org(&admin, user).await;
        seed_member(&admin, account, user).await;
        seed_ms_token(&admin, account, user, TENANT).await;
        let other = seed_user(&admin).await;
        seed_member(&admin, account, other).await;
        let app = app_pool_with_empty_scope(&base, db).await;
        assert_eq!(microsoft_tid(&app, account, user).await.unwrap(), Some(TENANT.to_string()));
        assert_eq!(microsoft_tid(&app, account, other).await.unwrap(), None);
        app.close().await;
        admin.close().await;
    }

    #[tokio::test]
    async fn election_checks_each_account_scope_with_strict_app_role_pg() {
        let Some(base) = base_url() else { return; };
        let db = "orgt_g3_doorbell_election_rls";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        recreate(&base, db).await.close().await.unwrap();
        let admin = control_pool(&base, db).await;
        let user_a = seed_user(&admin).await;
        let user_b = seed_user(&admin).await;
        let account_a = seed_org(&admin, user_a).await;
        let account_b = seed_org(&admin, user_b).await;
        seed_member(&admin, account_a, user_a).await;
        seed_member(&admin, account_b, user_b).await;
        seed_ms_token(&admin, account_a, user_a, TENANT).await;
        seed_ms_token(&admin, account_b, user_b, TENANT).await;
        ensure_subscription_row(&admin, TENANT, RESOURCE).await.unwrap();
        upsert_watcher(&admin, TENANT, RESOURCE, account_a, user_a).await.unwrap();
        upsert_watcher(&admin, TENANT, RESOURCE, account_b, user_b).await.unwrap();
        sqlx::query("UPDATE teams_container_subscriptions SET owner_account_id=$1, owner_user_id=$2")
            .bind(account_a).bind(user_a).execute(&admin).await.unwrap();
        let app = app_pool_with_empty_scope(&base, db).await;
        let elected = elect_container_owner(&app, TENANT, RESOURCE, &[]).await.unwrap().unwrap();
        assert_eq!((elected.account_id, elected.user_id), (account_a, user_a));
        sqlx::query("UPDATE oauth_tokens SET credential_invalid_at=now() WHERE account_id=$1")
            .bind(account_a).execute(&admin).await.unwrap();
        let elected = elect_container_owner(&app, TENANT, RESOURCE, &[]).await.unwrap().unwrap();
        assert_eq!((elected.account_id, elected.user_id), (account_b, user_b));
        assert!(elect_container_owner(&app, TENANT, RESOURCE, &[(account_b,user_b)]).await.unwrap().is_none());
        app.close().await;
        admin.close().await;
    }

    #[tokio::test]
    async fn two_watchers_share_one_subscription_row_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping two_watchers_share_one_subscription_row_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "orgt_g3_doorbell_share";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool = control_pool(&base, db).await;
        let owner = seed_user(&pool).await;
        let account = seed_org(&pool, owner).await;
        let user_a = seed_user(&pool).await;
        let user_b = seed_user(&pool).await;
        seed_member(&pool, account, user_a).await;
        seed_member(&pool, account, user_b).await;
        seed_ms_token(&pool, account, user_a, TENANT).await;
        seed_ms_token(&pool, account, user_b, TENANT).await;

        let graph = RecordingGraph {
            creates: AtomicUsize::new(0),
            deletes: AtomicUsize::new(0),
            slot_taken: false,
            saw_watcher_on_delete: AtomicBool::new(false),
            watch: None,
            created_token: std::sync::Mutex::new(None),
        };
        let tokens = MapTokens {
            a: user_a,
            b: user_b,
            token_a: "token-a".into(),
            token_b: "token-b".into(),
        };
        ensure_with(&pool, &graph, &tokens, TENANT, RESOURCE, account, user_a)
            .await
            .unwrap();
        ensure_with(&pool, &graph, &tokens, TENANT, RESOURCE, account, user_b)
            .await
            .unwrap();

        let subs: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM teams_container_subscriptions WHERE ms_tenant_id = $1 AND resource = $2",
        )
        .bind(TENANT)
        .bind(RESOURCE)
        .fetch_one(&pool)
        .await
        .unwrap();
        let watchers: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM teams_container_watchers WHERE ms_tenant_id = $1 AND resource = $2",
        )
        .bind(TENANT)
        .bind(RESOURCE)
        .fetch_one(&pool)
        .await
        .unwrap();
        let state: String = sqlx::query_scalar(
            "SELECT state FROM teams_container_subscriptions WHERE ms_tenant_id = $1 AND resource = $2",
        )
        .bind(TENANT)
        .bind(RESOURCE)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(subs, 1, "one control-plane subscription row for the shared resource");
        assert_eq!(watchers, 2);
        assert_eq!(state, "live");
        assert_eq!(graph.creates.load(Ordering::SeqCst), 1, "Graph create runs once");
        pool.close().await;
    }

    #[tokio::test]
    async fn notification_fetches_once_per_watcher_and_403_drops_only_that_watcher_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping notification_fetches_once_per_watcher_and_403_drops_only_that_watcher_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_g3_doorbell_403";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool = control_pool(&base, db).await;
        let owner = seed_user(&pool).await;
        let account = seed_org(&pool, owner).await;
        let user_a = seed_user(&pool).await;
        let user_b = seed_user(&pool).await;
        sqlx::query(
            "INSERT INTO teams_container_subscriptions \
                (ms_tenant_id, resource, subscription_id, client_state, state, owner_account_id, owner_user_id, expires_at) \
             VALUES ($1, $2, 'sub-live', 'secret', 'live', $3, $4, now() + interval '2 days')",
        )
        .bind(TENANT)
        .bind(RESOURCE)
        .bind(account)
        .bind(user_a)
        .execute(&pool)
        .await
        .unwrap();
        for user in [user_a, user_b] {
            sqlx::query(
                "INSERT INTO teams_container_watchers (ms_tenant_id, resource, account_id, user_id) \
                 VALUES ($1, $2, $3, $4)",
            )
            .bind(TENANT)
            .bind(RESOURCE)
            .bind(account)
            .bind(user)
            .execute(&pool)
            .await
            .unwrap();
        }

        let fetches = Arc::new(AtomicUsize::new(0));
        let fetches_c = fetches.clone();
        for_each_watcher(&pool, TENANT, RESOURCE, move |w| {
            let fetches = fetches_c.clone();
            let user_b = user_b;
            async move {
                fetches.fetch_add(1, Ordering::SeqCst);
                if w.user_id == user_b {
                    WatcherOp::Drop
                } else {
                    WatcherOp::Keep
                }
            }
        })
        .await
        .unwrap();

        assert_eq!(fetches.load(Ordering::SeqCst), 2, "one fetch per watcher");
        let left: Vec<Uuid> = sqlx::query_scalar(
            "SELECT user_id FROM teams_container_watchers WHERE ms_tenant_id = $1 AND resource = $2",
        )
        .bind(TENANT)
        .bind(RESOURCE)
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(left, vec![user_a]);
        let still: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM teams_container_subscriptions WHERE subscription_id = 'sub-live'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(still, 1, "the other watcher's subscription stays");
        pool.close().await;
    }

    #[tokio::test]
    async fn non_owner_unwatch_leaves_subscription_last_watcher_deletes_upstream_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping non_owner_unwatch_leaves_subscription_last_watcher_deletes_upstream_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_g3_doorbell_unwatch";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool = control_pool(&base, db).await;
        let owner = seed_user(&pool).await;
        let account = seed_org(&pool, owner).await;
        let user_a = seed_user(&pool).await;
        let user_b = seed_user(&pool).await;
        seed_member(&pool, account, user_a).await;
        seed_member(&pool, account, user_b).await;
        seed_ms_token(&pool, account, user_a, TENANT).await;
        seed_ms_token(&pool, account, user_b, TENANT).await;
        sqlx::query(
            "INSERT INTO teams_container_subscriptions \
                (ms_tenant_id, resource, subscription_id, client_state, state, \
                 owner_account_id, owner_user_id, expires_at) \
             VALUES ($1, $2, 'sub-owned-by-a', 'secret', 'live', $3, $4, now() + interval '2 days')",
        )
        .bind(TENANT)
        .bind(RESOURCE)
        .bind(account)
        .bind(user_a)
        .execute(&pool)
        .await
        .unwrap();
        for user in [user_a, user_b] {
            sqlx::query(
                "INSERT INTO teams_container_watchers (ms_tenant_id, resource, account_id, user_id) \
                 VALUES ($1, $2, $3, $4)",
            )
            .bind(TENANT)
            .bind(RESOURCE)
            .bind(account)
            .bind(user)
            .execute(&pool)
            .await
            .unwrap();
        }

        let graph = RecordingGraph {
            creates: AtomicUsize::new(0),
            deletes: AtomicUsize::new(0),
            slot_taken: false,
            saw_watcher_on_delete: AtomicBool::new(false),
            watch: Some((pool.clone(), TENANT.into(), RESOURCE.into())),
            created_token: std::sync::Mutex::new(None),
        };
        let tokens = MapTokens {
            a: user_a,
            b: user_b,
            token_a: "token-a".into(),
            token_b: "token-b".into(),
        };

        unwatch_container(&pool, &graph, &tokens, RESOURCE, account, user_b)
            .await
            .unwrap();
        assert_eq!(graph.deletes.load(Ordering::SeqCst), 0, "non-owner does not delete upstream");
        let subs: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM teams_container_subscriptions WHERE subscription_id = 'sub-owned-by-a'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(subs, 1);
        let b_left: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM teams_container_watchers WHERE user_id = $1",
        )
        .bind(user_b)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(b_left, 0);

        unwatch_container(&pool, &graph, &tokens, RESOURCE, account, user_a)
            .await
            .unwrap();
        assert_eq!(graph.deletes.load(Ordering::SeqCst), 1);
        assert!(
            graph.saw_watcher_on_delete.load(Ordering::SeqCst),
            "upstream delete runs while the last watcher row still exists"
        );
        let watchers: i64 =
            sqlx::query_scalar("SELECT count(*) FROM teams_container_watchers").fetch_one(&pool).await.unwrap();
        let subs_after: i64 = sqlx::query_scalar("SELECT count(*) FROM teams_container_subscriptions")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(watchers, 0);
        assert_eq!(subs_after, 0);
        pool.close().await;
    }

    struct ScriptGraph {
        renew_err: std::sync::Mutex<std::collections::HashMap<String, DoorPatchError>>,
        probe_err: std::sync::Mutex<std::collections::HashMap<String, DoorPatchError>>,
        creates: AtomicUsize,
    }

    #[async_trait]
    impl DoorbellGraph for ScriptGraph {
        async fn create_subscription(
            &self,
            access_token: &str,
            _resource: &str,
            _client_state: &str,
        ) -> Result<CreatedDoor, DoorCreateError> {
            let n = self.creates.fetch_add(1, Ordering::SeqCst) + 1;
            Ok(CreatedDoor {
                subscription_id: format!("sub-new-{n}-{access_token}"),
                expires_at: Utc::now() + Duration::minutes(4230),
            })
        }

        async fn delete_subscription(
            &self,
            _access_token: &str,
            _subscription_id: &str,
        ) -> Result<(), String> {
            Ok(())
        }

        async fn renew_subscription(
            &self,
            access_token: &str,
            _subscription_id: &str,
            _resource: &str,
        ) -> Result<chrono::DateTime<Utc>, DoorPatchError> {
            if let Some(err) = self.renew_err.lock().unwrap().get(access_token).copied() {
                return Err(err);
            }
            Ok(Utc::now() + Duration::minutes(4230))
        }

        async fn probe_token(&self, access_token: &str) -> Result<(), DoorPatchError> {
            if let Some(err) = self.probe_err.lock().unwrap().get(access_token).copied() {
                return Err(err);
            }
            Ok(())
        }
    }

    fn script_graph(
        renew_err: std::collections::HashMap<String, DoorPatchError>,
        probe_err: std::collections::HashMap<String, DoorPatchError>,
    ) -> ScriptGraph {
        ScriptGraph {
            renew_err: std::sync::Mutex::new(renew_err),
            probe_err: std::sync::Mutex::new(probe_err),
            creates: AtomicUsize::new(0),
        }
    }

    async fn seed_live(
        pool: &PgPool,
        account: Uuid,
        owner: Uuid,
        watchers: &[Uuid],
        resource: &str,
        sub_id: &str,
        state: &str,
    ) {
        sqlx::query(
            "INSERT INTO teams_container_subscriptions \
                (ms_tenant_id, resource, subscription_id, client_state, state, \
                 owner_account_id, owner_user_id, expires_at) \
             VALUES ($1, $2, $3, 'secret', $4, $5, $6, now() + interval '1 day')",
        )
        .bind(TENANT)
        .bind(resource)
        .bind(sub_id)
        .bind(state)
        .bind(account)
        .bind(owner)
        .execute(pool)
        .await
        .unwrap();
        for user in watchers {
            sqlx::query(
                "INSERT INTO teams_container_watchers (ms_tenant_id, resource, account_id, user_id) \
                 VALUES ($1, $2, $3, $4)",
            )
            .bind(TENANT)
            .bind(resource)
            .bind(account)
            .bind(user)
            .execute(pool)
            .await
            .unwrap();
        }
    }

    async fn open_health_count(pool: &PgPool) -> i64 {
        sqlx::query_scalar(
            "SELECT count(*) FROM connector_health \
             WHERE capability = 'teams_container_push' AND condition = 'no_eligible_backer' \
               AND resolved_at IS NULL",
        )
        .fetch_one(pool)
        .await
        .unwrap()
    }

    async fn failover_audits(pool: &PgPool) -> i64 {
        sqlx::query_scalar(
            "SELECT count(*) FROM connector_audit_log \
             WHERE action = 'route_container' \
               AND detail->>'kind' = 'teams_container_owner_failover'",
        )
        .fetch_one(pool)
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn reauthorization_401_reelects_other_watcher_and_audits_without_health_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping reauthorization_401_reelects_other_watcher_and_audits_without_health_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_g4_reauth_401";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool = control_pool(&base, db).await;
        let org_owner = seed_user(&pool).await;
        let account = seed_org(&pool, org_owner).await;
        let user_a = seed_user(&pool).await;
        let user_b = seed_user(&pool).await;
        seed_member(&pool, account, user_a).await;
        seed_member(&pool, account, user_b).await;
        seed_ms_token(&pool, account, user_a, TENANT).await;
        seed_ms_token(&pool, account, user_b, TENANT).await;
        seed_live(&pool, account, user_a, &[user_a, user_b], RESOURCE, "sub-reauth", "live").await;
        let mut renew_err = std::collections::HashMap::new();
        renew_err.insert("token-a".into(), DoorPatchError::Reelect);
        let graph = script_graph(renew_err, std::collections::HashMap::new());
        let tokens = MapTokens {
            a: user_a,
            b: user_b,
            token_a: "token-a".into(),
            token_b: "token-b".into(),
        };
        handle_doorbell_lifecycle(&pool, &graph, &tokens, TENANT, RESOURCE, "reauthorizationRequired")
            .await
            .unwrap();
        let owner: Uuid = sqlx::query_scalar(
            "SELECT owner_user_id FROM teams_container_subscriptions WHERE resource = $1",
        )
        .bind(RESOURCE)
        .fetch_one(&pool)
        .await
        .unwrap();
        let sub_id: String = sqlx::query_scalar(
            "SELECT subscription_id FROM teams_container_subscriptions WHERE resource = $1",
        )
        .bind(RESOURCE)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(owner, user_b);
        assert_eq!(sub_id, "sub-reauth", "a successful PATCH keeps the subscription id");
        assert_eq!(failover_audits(&pool).await, 1);
        assert_eq!(open_health_count(&pool).await, 0);
        pool.close().await;
    }

    #[tokio::test]
    async fn reauthorization_401_with_no_other_watcher_opens_health_and_keeps_watches_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping reauthorization_401_with_no_other_watcher_opens_health_and_keeps_watches_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_g4_reauth_none";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool = control_pool(&base, db).await;
        let org_owner = seed_user(&pool).await;
        let account = seed_org(&pool, org_owner).await;
        let user_a = seed_user(&pool).await;
        let user_b = seed_user(&pool).await;
        seed_member(&pool, account, user_a).await;
        seed_member(&pool, account, user_b).await;
        seed_ms_token(&pool, account, user_a, TENANT).await;
        seed_ms_token(&pool, account, user_b, TENANT).await;
        seed_live(&pool, account, user_a, &[user_a], RESOURCE, "sub-solo", "live").await;
        const OTHER: &str = "teams/t1/channels/c-other/messages";
        seed_live(&pool, account, user_b, &[user_b], OTHER, "sub-other", "live").await;
        let mut renew_err = std::collections::HashMap::new();
        renew_err.insert("token-a".into(), DoorPatchError::Reelect);
        let graph = script_graph(renew_err, std::collections::HashMap::new());
        let tokens = MapTokens {
            a: user_a,
            b: user_b,
            token_a: "token-a".into(),
            token_b: "token-b".into(),
        };
        handle_doorbell_lifecycle(&pool, &graph, &tokens, TENANT, RESOURCE, "reauthorizationRequired")
            .await
            .unwrap();
        assert_eq!(open_health_count(&pool).await, 1);
        let remedy: String = sqlx::query_scalar(
            "SELECT detail->>'remedy' FROM connector_health \
             WHERE capability = 'teams_container_push' AND resolved_at IS NULL",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(
            remedy.contains("can reconnect") || remedy.contains("any member of this channel can connect"),
            "remedy was {remedy}"
        );
        let a_watch: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM teams_container_watchers WHERE user_id = $1 AND resource = $2",
        )
        .bind(user_a)
        .bind(RESOURCE)
        .fetch_one(&pool)
        .await
        .unwrap();
        let b_watch: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM teams_container_watchers WHERE user_id = $1 AND resource = $2",
        )
        .bind(user_b)
        .bind(OTHER)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(a_watch, 1, "the failed owner's watch stays");
        assert_eq!(b_watch, 1, "the other member's watch is not deleted");
        let still: String = sqlx::query_scalar(
            "SELECT subscription_id FROM teams_container_subscriptions WHERE resource = $1",
        )
        .bind(RESOURCE)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(still, "sub-solo", "the doorbell row is kept");
        pool.close().await;
    }

    #[tokio::test]
    async fn subscription_removed_recreates_under_a_watcher_whose_token_works_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping subscription_removed_recreates_under_a_watcher_whose_token_works_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_g4_removed";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool = control_pool(&base, db).await;
        let org_owner = seed_user(&pool).await;
        let account = seed_org(&pool, org_owner).await;
        let user_a = seed_user(&pool).await;
        let user_b = seed_user(&pool).await;
        seed_member(&pool, account, user_a).await;
        seed_member(&pool, account, user_b).await;
        seed_ms_token(&pool, account, user_a, TENANT).await;
        seed_ms_token(&pool, account, user_b, TENANT).await;
        seed_live(&pool, account, user_a, &[user_a, user_b], RESOURCE, "sub-gone", "live").await;
        let mut probe_err = std::collections::HashMap::new();
        probe_err.insert("token-a".into(), DoorPatchError::Reelect);
        let graph = script_graph(std::collections::HashMap::new(), probe_err);
        let tokens = MapTokens {
            a: user_a,
            b: user_b,
            token_a: "token-a".into(),
            token_b: "token-b".into(),
        };
        let outcome = handle_doorbell_lifecycle(
            &pool, &graph, &tokens, TENANT, RESOURCE, "subscriptionRemoved",
        )
        .await
        .unwrap();
        assert!(outcome.resync_watchers, "a recreated doorbell asks each watcher to resync");
        let owner: Uuid = sqlx::query_scalar(
            "SELECT owner_user_id FROM teams_container_subscriptions WHERE resource = $1",
        )
        .bind(RESOURCE)
        .fetch_one(&pool)
        .await
        .unwrap();
        let sub_id: String = sqlx::query_scalar(
            "SELECT subscription_id FROM teams_container_subscriptions WHERE resource = $1",
        )
        .bind(RESOURCE)
        .fetch_one(&pool)
        .await
        .unwrap();
        let state: String = sqlx::query_scalar(
            "SELECT state FROM teams_container_subscriptions WHERE resource = $1",
        )
        .bind(RESOURCE)
        .fetch_one(&pool)
        .await
        .unwrap();
        let watchers: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM teams_container_watchers WHERE resource = $1",
        )
        .bind(RESOURCE)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(owner, user_b);
        assert!(sub_id.starts_with("sub-new-"), "recreated under the working token, got {sub_id}");
        assert!(sub_id.contains("token-b"), "create used the other watcher's token");
        assert_eq!(state, "live");
        assert_eq!(watchers, 2);
        assert_eq!(open_health_count(&pool).await, 0);
        pool.close().await;
    }

    #[tokio::test]
    async fn credential_death_of_the_owner_reelects_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping credential_death_of_the_owner_reelects_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "orgt_g4_cred_death";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool = control_pool(&base, db).await;
        let org_owner = seed_user(&pool).await;
        let account = seed_org(&pool, org_owner).await;
        let user_a = seed_user(&pool).await;
        let user_b = seed_user(&pool).await;
        seed_member(&pool, account, user_a).await;
        seed_member(&pool, account, user_b).await;
        seed_ms_token(&pool, account, user_a, TENANT).await;
        seed_ms_token(&pool, account, user_b, TENANT).await;
        seed_live(&pool, account, user_a, &[user_a, user_b], RESOURCE, "sub-cred", "live").await;
        sqlx::query(
            "UPDATE oauth_tokens SET credential_invalid_at = now() \
             WHERE account_id = $1 AND owner_user_id = $2 AND provider = 'microsoft'",
        )
        .bind(account)
        .bind(user_a)
        .execute(&pool)
        .await
        .unwrap();
        crate::services::connector_health::on_backer_lost(
            &pool,
            None,
            account,
            Some(user_a),
            "microsoft",
            crate::services::connector_health::BackerLostCause::CredentialDeath,
            None,
            None,
        )
        .await
        .unwrap();
        let owner: Uuid = sqlx::query_scalar(
            "SELECT owner_user_id FROM teams_container_subscriptions WHERE resource = $1",
        )
        .bind(RESOURCE)
        .fetch_one(&pool)
        .await
        .unwrap();
        let watchers: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM teams_container_watchers WHERE resource = $1",
        )
        .bind(RESOURCE)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(owner, user_b);
        assert_eq!(watchers, 2, "credential death does not drop either watch");
        assert_eq!(failover_audits(&pool).await, 1);
        assert_eq!(open_health_count(&pool).await, 0);
        pool.close().await;
    }

    #[tokio::test]
    async fn renew_429_keeps_the_row_and_the_owner_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping renew_429_keeps_the_row_and_the_owner_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "orgt_g4_renew_429";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool = control_pool(&base, db).await;
        let org_owner = seed_user(&pool).await;
        let account = seed_org(&pool, org_owner).await;
        let user_a = seed_user(&pool).await;
        let user_b = seed_user(&pool).await;
        seed_member(&pool, account, user_a).await;
        seed_member(&pool, account, user_b).await;
        seed_ms_token(&pool, account, user_a, TENANT).await;
        seed_ms_token(&pool, account, user_b, TENANT).await;
        seed_live(&pool, account, user_a, &[user_a, user_b], RESOURCE, "sub-429", "live").await;
        let mut renew_err = std::collections::HashMap::new();
        renew_err.insert("token-a".into(), DoorPatchError::RetryLater);
        let graph = script_graph(renew_err, std::collections::HashMap::new());
        let tokens = MapTokens {
            a: user_a,
            b: user_b,
            token_a: "token-a".into(),
            token_b: "token-b".into(),
        };
        handle_doorbell_lifecycle(&pool, &graph, &tokens, TENANT, RESOURCE, "reauthorizationRequired")
            .await
            .unwrap();
        let owner: Uuid = sqlx::query_scalar(
            "SELECT owner_user_id FROM teams_container_subscriptions WHERE resource = $1",
        )
        .bind(RESOURCE)
        .fetch_one(&pool)
        .await
        .unwrap();
        let sub_id: String = sqlx::query_scalar(
            "SELECT subscription_id FROM teams_container_subscriptions WHERE resource = $1",
        )
        .bind(RESOURCE)
        .fetch_one(&pool)
        .await
        .unwrap();
        let state: String = sqlx::query_scalar(
            "SELECT state FROM teams_container_subscriptions WHERE resource = $1",
        )
        .bind(RESOURCE)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(owner, user_a);
        assert_eq!(sub_id, "sub-429");
        assert_eq!(state, "live");
        assert_eq!(failover_audits(&pool).await, 0);
        assert_eq!(open_health_count(&pool).await, 0);
        pool.close().await;
    }

    #[tokio::test]
    async fn polling_tick_selects_only_polling_containers_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping polling_tick_selects_only_polling_containers_pg: set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_g4_poll_select";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool = control_pool(&base, db).await;
        let org_owner = seed_user(&pool).await;
        let account = seed_org(&pool, org_owner).await;
        let user_a = seed_user(&pool).await;
        seed_member(&pool, account, user_a).await;
        seed_ms_token(&pool, account, user_a, TENANT).await;
        seed_live(&pool, account, user_a, &[user_a], RESOURCE, "sub-live-tick", "live").await;
        const POLLED: &str = "teams/t1/channels/c-poll/messages";
        seed_live(&pool, account, user_a, &[user_a], POLLED, "sub-poll", "polling").await;
        let selected = polling_containers(&pool).await.unwrap();
        assert_eq!(selected.len(), 1, "the tighter tick selects polling rows only");
        assert_eq!(selected[0].1, POLLED);
        let for_account = polling_resources_for_account(&pool, account).await.unwrap();
        assert_eq!(for_account, vec![POLLED.to_string()]);
        assert!(!for_account.iter().any(|r| r == RESOURCE));
        pool.close().await;
    }

    #[tokio::test]
    async fn reelect_of_polling_row_keeps_push_health_open_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping reelect_of_polling_row_keeps_push_health_open_pg: set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_g4_reelect_poll_health";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool = control_pool(&base, db).await;
        let org_owner = seed_user(&pool).await;
        let account = seed_org(&pool, org_owner).await;
        let user_a = seed_user(&pool).await;
        let user_b = seed_user(&pool).await;
        seed_member(&pool, account, user_a).await;
        seed_member(&pool, account, user_b).await;
        seed_ms_token(&pool, account, user_a, TENANT).await;
        seed_ms_token(&pool, account, user_b, TENANT).await;
        seed_live(&pool, account, user_a, &[user_a, user_b], RESOURCE, "sub-poll-dead", "polling")
            .await;
        sqlx::query(
            "UPDATE teams_container_subscriptions \
             SET subscription_id = NULL, polling_reason = 'no_eligible_owner' \
             WHERE resource = $1",
        )
        .bind(RESOURCE)
        .execute(&pool)
        .await
        .unwrap();
        crate::services::connector_health::note_teams_push_lost(
            &pool,
            TENANT,
            RESOURCE,
            "no_eligible_owner",
        )
        .await
        .unwrap();
        assert_eq!(open_health_count(&pool).await, 1);
        crate::services::connector_health::on_backer_lost(
            &pool,
            None,
            account,
            Some(user_a),
            "microsoft",
            crate::services::connector_health::BackerLostCause::CredentialDeath,
            None,
            None,
        )
        .await
        .unwrap();
        let owner: Uuid = sqlx::query_scalar(
            "SELECT owner_user_id FROM teams_container_subscriptions WHERE resource = $1",
        )
        .bind(RESOURCE)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(owner, user_b);
        assert_eq!(
            open_health_count(&pool).await,
            1,
            "electing a successor must not close health while the row stays polling with no subscription"
        );
        pool.close().await;
    }

    #[tokio::test]
    async fn subscription_removed_retry_later_probe_keeps_owner_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping subscription_removed_retry_later_probe_keeps_owner_pg: set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_g4_removed_429";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool = control_pool(&base, db).await;
        let org_owner = seed_user(&pool).await;
        let account = seed_org(&pool, org_owner).await;
        let user_a = seed_user(&pool).await;
        let user_b = seed_user(&pool).await;
        seed_member(&pool, account, user_a).await;
        seed_member(&pool, account, user_b).await;
        seed_ms_token(&pool, account, user_a, TENANT).await;
        seed_ms_token(&pool, account, user_b, TENANT).await;
        seed_live(&pool, account, user_a, &[user_a, user_b], RESOURCE, "sub-gone-429", "live").await;
        let mut probe_err = std::collections::HashMap::new();
        probe_err.insert("token-a".into(), DoorPatchError::RetryLater);
        let graph = script_graph(std::collections::HashMap::new(), probe_err);
        let tokens = MapTokens {
            a: user_a,
            b: user_b,
            token_a: "token-a".into(),
            token_b: "token-b".into(),
        };
        handle_doorbell_lifecycle(
            &pool, &graph, &tokens, TENANT, RESOURCE, "subscriptionRemoved",
        )
        .await
        .unwrap();
        let owner: Uuid = sqlx::query_scalar(
            "SELECT owner_user_id FROM teams_container_subscriptions WHERE resource = $1",
        )
        .bind(RESOURCE)
        .fetch_one(&pool)
        .await
        .unwrap();
        let sub_id: String = sqlx::query_scalar(
            "SELECT subscription_id FROM teams_container_subscriptions WHERE resource = $1",
        )
        .bind(RESOURCE)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(owner, user_a, "429/5xx probe must keep the current owner");
        assert!(
            sub_id.contains("token-a"),
            "recreate must use the current owner's token, got {sub_id}"
        );
        pool.close().await;
    }
}
