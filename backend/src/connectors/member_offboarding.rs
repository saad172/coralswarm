//! Member offboarding: stop capturing for someone who left an org
//! (settings launch PR 2.8).
//!
//! # The gap this closes
//!
//! `clerk_provision::remove_org_membership` (the Clerk
//! `organizationMembership.deleted` webhook, and PR #1465's admin "remove
//! member" route) deletes the departed member's `account_members` row and
//! nothing connector-side. Their per-member `oauth_tokens` rows for the ORG
//! account, their per-owner `connector_installs`, their Gmail/Calendar/Chat
//! watches, their Graph subscriptions and their Slack member captures all
//! survived. Capture paths that pick a destination by "does this member still
//! have an unarchived ocean" kept reading their mailbox until
//! `member_ocean_reconciler` archived it, and the org kept a live refresh
//! token to what may be a personal mailbox for as long as nobody noticed.
//!
//! [`revoke_departed_member_capture`] is the one routine that undoes all of
//! it for ONE `(org account, user)` pair. Two kinds of caller:
//!
//! * offboarding itself (`remove_org_membership`, after its own commit), and
//! * every capture path's live-membership gate
//!   ([`capture_owner_is_live_member`]): a push or a renewal that finds its
//!   owner is no longer a member refuses, then calls this to stop the watch,
//!   so a missed or failed offboarding still converges on the next event.
//!
//! # What is touched — this org account's rows for this user, nothing else
//!
//! Every statement is keyed on `account_id = <the org account>` AND the
//! departing user (`owner_user_id` / `user_id` / `ingested_by`). The same
//! user's personal account is a different `account_id`, so its connectors are
//! never matched. Account-wide rows (`owner_user_id IS NULL` — the org's Slack
//! bot, a personal-plane Calendar) belong to the org, not to the person who
//! happened to install them, and are left alone.
//!
//! Order, chosen so local revocation never waits on a provider:
//!
//! 1. **Credentials, in one control transaction:** `oauth_tokens`,
//!    `connector_installs`, `slack_member_channels`. Once this commits nothing
//!    in CoralSwarm can act as the member in this org again.
//! 2. **Upstream, best-effort, on the tokens step 1 returned:** Gmail
//!    `users.stop`, Calendar `channels.stop`, Workspace Events
//!    `subscriptions.delete`, Graph delegated subscription delete, then the
//!    credential itself (Google `oauth2/revoke`, Slack `auth.revoke`).
//! 3. **Watch state, local:** the existing disconnect purges
//!    (`google_disconnect::purge_google_connection` per surface,
//!    `microsoft_disconnect::purge_microsoft_connection`), GitHub watches
//!    paused, and any leftover owned watch rows on either plane.
//!
//! # Revoking upstream without breaking the member's own connections
//!
//! Google revokes the whole grant for (Google user, OAuth client) and Slack
//! user tokens are stable per (user, app), so revoking the org's copy can
//! revoke the copy their PERSONAL account still uses. Each revoke therefore
//! first asks `oauth_grant_held_elsewhere` (control migration 0368) whether any
//! other row still holds the same provider identity — or whether the same
//! person holds any other row for that provider, or the departing row has no
//! identifier to compare at all — and skips the revoke if so. Gmail's `users.stop` is mailbox-wide for the same reason and is skipped
//! while any other account still watches that mailbox
//! (`resolve_gmail_watch_owners`).
//!
//! # Failure and retry
//!
//! An upstream failure is logged and never fails offboarding. There is no
//! retry queue on purpose: retrying needs the credential, and keeping (or
//! re-encrypting into a job payload) a token we have just decided the org must
//! not hold would undo the point. What an upstream failure leaves is inert:
//! pushes for a mailbox or subscription with no watch row are dropped on
//! receipt, Gmail watches lapse within 7 days and Graph subscriptions within
//! 3, and the provider-side grant is one CoralSwarm can no longer use.
//!
//! # Idempotency
//!
//! A second run finds no rows in step 1, so step 2 has no token to call with
//! and step 3's deletes match nothing. Clerk may redeliver the webhook, and
//! every gate may call this on every refused event; both are safe.
//!
//! A user who has REJOINED is never touched: the routine re-checks live
//! membership first and returns [`Skipped::StillMember`].

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use coralswarm_core::oauth_token::decrypt_token;
use sqlx::PgPool;
use uuid::Uuid;

use crate::connectors::google::Surface;
use crate::connectors::{ConnectorCredentials, TokenOwner};
use crate::crypto::DekPlane;
use crate::tenancy::scope::{begin_account_scoped, begin_tenant_system_scoped};

/// Every per-member Google provider key. `"google"` is Calendar's
/// grandfathered key; an org member's Calendar row carries their owner id.
const GOOGLE_PROVIDERS: &[&str] = &["google", "google_gmail", "google_chat", "google_meet"];

/// Is `user_id` someone CoralSwarm may still capture for under `account_id`?
///
/// * Org account (`clerk_org_id` set): a live `account_members` row AND no
///   wholesale `org_revocations` row — the same predicate
///   `middleware::auth::live_org_membership` applies to a request.
/// * Personal account: always `true`. There is no membership to leave, and
///   which user owns a personal watch is already governed where the watch is
///   created; this gate exists for org departures and must not change
///   personal-plane behaviour.
/// * No such account: `false`.
///
/// `accounts`, `account_members` and `org_revocations` carry no RLS, so this
/// runs on the bare control pool. An `Err` means "could not check": callers
/// must fail CLOSED (do not capture) and must NOT offboard on it.
pub(crate) async fn capture_owner_is_live_member(
    control: &PgPool,
    account_id: Uuid,
    user_id: Uuid,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT EXISTS ( \
           SELECT 1 FROM accounts a \
           WHERE a.id = $1 AND ( \
             a.clerk_org_id IS NULL \
             OR (a.clerk_org_id IS NOT NULL \
                 AND EXISTS (SELECT 1 FROM account_members am \
                             WHERE am.account_id = a.id AND am.user_id = $2) \
                 AND NOT EXISTS (SELECT 1 FROM org_revocations r \
                                 WHERE r.clerk_org_id = a.clerk_org_id))))",
    )
    .bind(account_id)
    .bind(user_id)
    .fetch_one(control)
    .await
}

/// The provider calls [`revoke_departed_member_capture`] makes. A trait so the
/// pg tests can prove what is (and is not) revoked, and that a failing
/// provider never blocks local revocation, without a network.
#[async_trait]
pub(crate) trait OffboardUpstream: Send + Sync {
    /// A usable Google access token for these credentials, refreshing with
    /// the refresh token when the stored one is near expiry. Never persisted:
    /// the row it came from is already deleted.
    async fn google_access_token(&self, creds: ConnectorCredentials) -> Option<String>;
    /// As [`Self::google_access_token`], for Microsoft.
    async fn microsoft_access_token(&self, creds: ConnectorCredentials) -> Option<String>;
    /// Gmail `users.stop` for the token's own mailbox.
    async fn stop_gmail_watch(&self, access_token: &str) -> Result<(), String>;
    /// Calendar `channels.stop`.
    async fn stop_calendar_channel(&self, access_token: &str, channel_id: &str, resource_id: &str) -> Result<(), String>;
    /// Workspace Events `subscriptions.delete` for the member's Chat/Meet
    /// subscription (`google_disconnect::revoke_chat_subscription_best_effort`).
    #[allow(clippy::too_many_arguments)]
    async fn delete_workspace_subscription(
        &self,
        control: &PgPool,
        account_id: Uuid,
        user_id: Uuid,
        access_token: &str,
        surface: &str,
        before: DateTime<Utc>,
    ) -> bool;
    /// Google `oauth2/revoke` (refresh token preferred: it revokes the grant).
    async fn revoke_google_token(&self, token: &str) -> Result<(), String>;
    /// Slack `auth.revoke`.
    async fn revoke_slack_token(&self, token: &str) -> Result<(), String>;
    /// Graph `DELETE /subscriptions/{id}` for a DELEGATED subscription, with
    /// the member's own token.
    async fn delete_graph_subscription(&self, access_token: &str, subscription_id: &str) -> Result<(), String>;
}

/// The production [`OffboardUpstream`]. Every call goes through a bounded
/// request (`google::google_send`'s 30 s per-request timeout, or an explicit
/// one), so a stalled provider costs a bounded delay, never a hang.
pub(crate) struct HttpUpstream<'a> {
    pub(crate) http: &'a reqwest::Client,
}

const GOOGLE_REVOKE_ENDPOINT: &str = "https://oauth2.googleapis.com/revoke";
const GMAIL_STOP_ENDPOINT: &str = "https://gmail.googleapis.com/gmail/v1/users/me/stop";
const CALENDAR_STOP_ENDPOINT: &str = "https://www.googleapis.com/calendar/v3/channels/stop";

fn status_ok(resp: reqwest::Response) -> Result<(), String> {
    let status = resp.status();
    // 400/404 from a revoke or stop means "already gone" (expired token,
    // unknown channel) — the outcome offboarding wants either way.
    if status.is_success() || status == reqwest::StatusCode::NOT_FOUND || status == reqwest::StatusCode::BAD_REQUEST {
        Ok(())
    } else {
        Err(format!("upstream returned {status}"))
    }
}

#[async_trait]
impl OffboardUpstream for HttpUpstream<'_> {
    async fn google_access_token(&self, mut creds: ConnectorCredentials) -> Option<String> {
        let google = crate::connectors::google::GoogleConnector;
        match google.refresh_if_needed(self.http, &mut creds).await {
            Ok(()) => Some(creds.access_token),
            Err(e) => {
                tracing::info!(error = %e, "member offboarding: google token refresh failed — skipping its upstream stops");
                None
            }
        }
    }

    async fn microsoft_access_token(&self, mut creds: ConnectorCredentials) -> Option<String> {
        let microsoft = crate::connectors::microsoft::MicrosoftConnector;
        match microsoft.refresh_if_needed(self.http, &mut creds).await {
            Ok(()) => Some(creds.access_token),
            Err(e) => {
                tracing::info!(error = %e, "member offboarding: microsoft token refresh failed — skipping delegated subscription deletes");
                None
            }
        }
    }

    async fn stop_gmail_watch(&self, access_token: &str) -> Result<(), String> {
        let resp = crate::connectors::google::google_send(|| self.http.post(GMAIL_STOP_ENDPOINT).bearer_auth(access_token))
            .await
            .map_err(|e| e.to_string())?;
        status_ok(resp)
    }

    async fn stop_calendar_channel(&self, access_token: &str, channel_id: &str, resource_id: &str) -> Result<(), String> {
        let resp = crate::connectors::google::google_send(|| {
            self.http
                .post(CALENDAR_STOP_ENDPOINT)
                .bearer_auth(access_token)
                .json(&serde_json::json!({ "id": channel_id, "resourceId": resource_id }))
        })
        .await
        .map_err(|e| e.to_string())?;
        status_ok(resp)
    }

    async fn delete_workspace_subscription(
        &self,
        control: &PgPool,
        account_id: Uuid,
        user_id: Uuid,
        access_token: &str,
        surface: &str,
        before: DateTime<Utc>,
    ) -> bool {
        crate::connectors::google_disconnect::revoke_chat_subscription_best_effort(
            self.http,
            control,
            account_id,
            user_id,
            access_token,
            surface,
            Some(before),
        )
        .await
    }

    async fn revoke_google_token(&self, token: &str) -> Result<(), String> {
        let resp = crate::connectors::google::google_send(|| self.http.post(GOOGLE_REVOKE_ENDPOINT).form(&[("token", token)]))
            .await
            .map_err(|e| e.to_string())?;
        status_ok(resp)
    }

    async fn revoke_slack_token(&self, token: &str) -> Result<(), String> {
        crate::connectors::slack::revoke_member_token(self.http, token)
            .await
            .map_err(|e| e.to_string())
    }

    async fn delete_graph_subscription(&self, access_token: &str, subscription_id: &str) -> Result<(), String> {
        let resp = self
            .http
            .delete(format!("https://graph.microsoft.com/v1.0/subscriptions/{subscription_id}"))
            .bearer_auth(access_token)
            .timeout(std::time::Duration::from_secs(20))
            .send()
            .await
            .map_err(|e| e.to_string())?;
        status_ok(resp)
    }
}

/// Why [`revoke_departed_member_capture`] did nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Skipped {
    /// No `accounts` row.
    NoAccount,
    /// A personal account: membership is not something one leaves.
    PersonalAccount,
    /// The user is (again) a live member — a rejoin, or a gate that raced one.
    StillMember,
}

/// What [`revoke_departed_member_capture`] did. Logged by every caller and
/// asserted on by the pg tests.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct OffboardOutcome {
    pub(crate) skipped: Option<Skipped>,
    pub(crate) tokens_deleted: usize,
    pub(crate) installs_deleted: usize,
    pub(crate) slack_member_channels_deleted: usize,
    /// `google_watch_channels` rows removed, both planes.
    pub(crate) watch_channels_deleted: usize,
    /// `graph_subscriptions` rows owned by the member removed, both planes.
    pub(crate) graph_subscriptions_deleted: usize,
    pub(crate) github_repos_paused: usize,
    pub(crate) upstream_calls: usize,
    pub(crate) upstream_failures: usize,
    /// Revokes skipped because the same provider identity is still held by
    /// another row (typically the member's own personal account).
    pub(crate) revokes_skipped_shared: usize,
    /// Tokens whose ciphertext could not be decrypted (no DEK, wrong plane):
    /// deleted locally, no upstream call possible.
    pub(crate) undecryptable_tokens: usize,
    /// A LOCAL step after the credential delete failed (logged). The
    /// credential delete itself failing is an `Err`, not this flag.
    pub(crate) purge_failed: bool,
}

impl OffboardOutcome {
    fn skipped(why: Skipped) -> Self {
        OffboardOutcome { skipped: Some(why), ..Default::default() }
    }
}

/// One deleted `oauth_tokens` row, as `RETURNING` handed it back.
#[derive(sqlx::FromRow)]
struct DeletedToken {
    provider: String,
    provider_user_id: Option<String>,
    provider_workspace_id: Option<String>,
    access_token: Vec<u8>,
    refresh_token: Option<Vec<u8>>,
    expires_at: Option<DateTime<Utc>>,
}

/// One owned `google_watch_channels` row.
#[derive(sqlx::FromRow)]
struct OwnedWatch {
    surface: String,
    resource_id: String,
    channel_id: String,
    channel_resource_id: String,
}

/// Stop capturing for `user_id` under the org account `account_id`: delete
/// their org-scoped credentials and installs, stop their watches and
/// subscriptions upstream (best-effort), and purge the watch state. See this
/// module's doc for the exact scope, order, and failure posture.
///
/// `plane` is the account's resolved DEK plane (`DekPlane::resolve_for_account`
/// or a worker's `DekPlaneSource::plane_for`): it supplies the key the tokens
/// are encrypted under and the content pool tenant-side watch state lives on.
/// `None` (the plane could not be resolved — a suspended tenant, say) still
/// deletes every control-plane credential and watch row; only the decrypt
/// (so the upstream calls) and the tenant-side purges are skipped, and logged.
///
/// `Err` only when the eligibility read or the credential-delete transaction
/// fails — nothing was revoked, and the caller should log loudly (a gate's
/// next refusal, or Clerk's redelivery, retries it).
pub(crate) async fn revoke_departed_member_capture(
    upstream: &dyn OffboardUpstream,
    http: &reqwest::Client,
    control: &PgPool,
    plane: Option<&DekPlane>,
    account_id: Uuid,
    user_id: Uuid,
) -> Result<OffboardOutcome, sqlx::Error> {
    // ── 0. Eligibility ─────────────────────────────────────────────────────
    let org: Option<Option<String>> = sqlx::query_scalar("SELECT clerk_org_id FROM accounts WHERE id = $1")
        .bind(account_id)
        .fetch_optional(control)
        .await?;
    match org {
        None => return Ok(OffboardOutcome::skipped(Skipped::NoAccount)),
        Some(None) => return Ok(OffboardOutcome::skipped(Skipped::PersonalAccount)),
        Some(Some(_)) => {}
    }
    if capture_owner_is_live_member(control, account_id, user_id).await? {
        return Ok(OffboardOutcome::skipped(Skipped::StillMember));
    }

    let mut outcome = OffboardOutcome::default();
    let content: Option<PgPool> = plane.map(|p| p.content_pool().clone());
    // A tenant pool that is physically the control database (a personal-
    // shaped org with no tenant DB) needs no second pass.
    let content_is_control = content.as_ref().is_none_or(|c| same_database(c, control));

    // Cutoffs for the purges below, each from its own pool's clock (see
    // `google_disconnect`'s "purge_not_after must come from the SAME clock").
    // Captured BEFORE anything is deleted, so a row a rejoin re-creates after
    // this point survives the purge.
    let control_clock: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()").fetch_one(control).await?;
    let content_clock: Option<DateTime<Utc>> = match content.as_ref() {
        Some(c) => match sqlx::query_scalar("SELECT clock_timestamp()").fetch_one(c).await {
            Ok(t) => Some(t),
            Err(e) => {
                tracing::warn!(error = %e, %account_id, "member offboarding: tenant clock unreadable — tenant-side purges skipped");
                None
            }
        },
        None => None,
    };

    // Owned watches, read BEFORE the purge deletes them: step 2 needs their
    // channel ids to stop them upstream. Control holds every Gmail/Chat/Meet
    // row and an org member's Calendar row.
    let watches = owned_watches(control, account_id, user_id).await.unwrap_or_else(|e| {
        tracing::warn!(error = %e, %account_id, "member offboarding: could not read owned watch rows — upstream stops skipped");
        Vec::new()
    });

    // ── 1. Credentials — the authoritative revocation ─────────────────────
    let mut tx = begin_account_scoped(control, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(format!("member offboarding scoped tx: {e}")))?;
    let deleted: Vec<DeletedToken> = sqlx::query_as(
        "DELETE FROM oauth_tokens WHERE account_id = $1 AND owner_user_id = $2 \
         RETURNING provider, provider_user_id, provider_workspace_id, access_token, refresh_token, expires_at",
    )
    .bind(account_id)
    .bind(user_id)
    .fetch_all(&mut *tx)
    .await?;
    outcome.tokens_deleted = deleted.len();
    outcome.installs_deleted = sqlx::query("DELETE FROM connector_installs WHERE account_id = $1 AND owner_user_id = $2")
        .bind(account_id)
        .bind(user_id)
        .execute(&mut *tx)
        .await?
        .rows_affected() as usize;
    outcome.slack_member_channels_deleted =
        sqlx::query("DELETE FROM slack_member_channels WHERE account_id = $1 AND user_id = $2")
            .bind(account_id)
            .bind(user_id)
            .execute(&mut *tx)
            .await?
            .rows_affected() as usize;
    tx.commit().await?;

    if outcome.tokens_deleted > 0 || outcome.installs_deleted > 0 {
        tracing::info!(
            %account_id, %user_id,
            tokens = outcome.tokens_deleted, installs = outcome.installs_deleted,
            slack_member_channels = outcome.slack_member_channels_deleted,
            "member offboarding: deleted the departed member's org-scoped credentials"
        );
    }

    // ── 2. Upstream, best-effort ───────────────────────────────────────────
    let dek: Option<[u8; 32]> = if deleted.is_empty() {
        None
    } else {
        match plane {
            Some(p) => match p.resolve(account_id).await {
                Ok(dek) => dek,
                Err(e) => {
                    tracing::warn!(error = %e, %account_id, "member offboarding: DEK unavailable — upstream revokes skipped");
                    None
                }
            },
            None => None,
        }
    };
    let mut microsoft_token: Option<String> = None;
    for row in &deleted {
        let decrypted = dek.as_ref().and_then(|dek| {
            let access = decrypt_token(dek, &row.access_token).ok()?;
            let refresh = match row.refresh_token.as_deref() {
                Some(r) => Some(decrypt_token(dek, r).ok()?),
                None => None,
            };
            Some((access, refresh))
        });
        let Some((access, refresh)) = decrypted else {
            outcome.undecryptable_tokens += 1;
            continue;
        };
        let creds = ConnectorCredentials {
            access_token: access.clone(),
            refresh_token: refresh.clone(),
            expires_at: row.expires_at,
            provider_workspace_id: row.provider_workspace_id.clone().unwrap_or_default(),
            refreshed: None,
            row_updated_at: None,
        };
        match row.provider.as_str() {
            p if GOOGLE_PROVIDERS.contains(&p) => {
                if let Some(token) = upstream.google_access_token(creds).await {
                    stop_google_watches(upstream, control, &watches, row, &token, account_id, user_id, control_clock, &mut outcome)
                        .await;
                }
                let held = grant_held_elsewhere(control, GOOGLE_PROVIDERS, row, true, account_id, user_id).await;
                let revoke_with = refresh.as_deref().unwrap_or(access.as_str());
                record(&mut outcome, held, || upstream.revoke_google_token(revoke_with), "google oauth2/revoke").await;
            }
            "microsoft" => {
                if microsoft_token.is_none() {
                    microsoft_token = upstream.microsoft_access_token(creds).await;
                }
            }
            "slack_user" => {
                let held = grant_held_elsewhere(control, &["slack_user", "slack"], row, false, account_id, user_id).await;
                record(&mut outcome, held, || upstream.revoke_slack_token(&access), "slack auth.revoke").await;
            }
            // GitHub/Composio/anything else: the local delete is the
            // revocation (GitHub's grant revoke needs app credentials and is
            // per-user, not per-org; Composio's own worker already revokes a
            // departed member's connected account).
            _ => {}
        }
    }

    // ── 3. Watch state, local ──────────────────────────────────────────────
    let owner = TokenOwner::Member(user_id);
    match (content.as_ref(), content_clock) {
        (Some(content), Some(content_clock)) => {
            for surface in [Surface::Gmail, Surface::Calendar, Surface::Chat, Surface::Meet] {
                let purge = crate::connectors::google_disconnect::purge_google_connection(
                    control,
                    content,
                    account_id,
                    surface,
                    owner,
                    control_clock,
                    content_clock,
                )
                .await;
                outcome.watch_channels_deleted += purge.watch_channels_deleted;
                outcome.purge_failed |= purge.purge_had_a_failure();
            }
            // Delegated mail subscriptions are deleted upstream inside this
            // purge, with the member's own (refreshed) token.
            let ms = crate::connectors::microsoft_disconnect::purge_microsoft_connection(
                http,
                content,
                account_id,
                owner,
                microsoft_token.as_deref(),
            )
            .await;
            outcome.graph_subscriptions_deleted += ms.subscriptions_deleted;
            outcome.purge_failed |= ms.purge_had_a_failure();
            match crate::connectors::github_disconnect::pause_for_member(content, account_id, user_id).await {
                Ok(n) => outcome.github_repos_paused = n,
                Err(e) => {
                    tracing::warn!(error = %e, %account_id, "member offboarding: github watch pause failed");
                    outcome.purge_failed = true;
                }
            }
        }
        _ => tracing::warn!(
            %account_id, %user_id, connector_health = true,
            "member offboarding: no tenant plane — tenant-side watch state (threads, Teams/Outlook watches, \
             tenant graph subscriptions) left for the member-ocean reconciler; credentials are already gone"
        ),
    }
    // Backstop sweep of owned watch rows the surface purges above do not
    // reach (an org member's delegated transcript subscription lives on
    // CONTROL, for one): the control plane always, and the tenant plane when
    // it is a different database. Same owner scoping; same clock cutoffs.
    let mut sweep: Vec<(&PgPool, DateTime<Utc>, bool)> = vec![(control, control_clock, false)];
    if let (Some(content), Some(content_clock), false) = (content.as_ref(), content_clock, content_is_control) {
        sweep.push((content, content_clock, true));
    }
    for (pool, not_after, tenant) in sweep {
        match delete_owned_watch_rows(pool, account_id, user_id, not_after, tenant).await {
            Ok((watch_rows, subscriptions)) => {
                outcome.watch_channels_deleted += watch_rows;
                outcome.graph_subscriptions_deleted += subscriptions.len();
                for (subscription_id, tenant_id) in subscriptions {
                    // App-only (RSC) rows carry a tenant id and are the org's,
                    // renewed by app credentials; only a delegated row is the
                    // member's own to delete with their token.
                    let (Some(token), true) = (microsoft_token.as_deref(), tenant_id.is_empty()) else { continue };
                    outcome.upstream_calls += 1;
                    if let Err(e) = upstream.delete_graph_subscription(token, &subscription_id).await {
                        outcome.upstream_failures += 1;
                        tracing::warn!(error = %e, %account_id, "member offboarding: graph subscription delete failed — it lapses on its own");
                    }
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, %account_id, tenant, "member offboarding: owned watch sweep failed");
                outcome.purge_failed = true;
            }
        }
    }

    if outcome.tokens_deleted > 0 {
        crate::services::connector_audit::record_or_queue(
            content.as_ref().unwrap_or(control),
            crate::services::connector_audit::AuditEntry {
                account_id,
                actor: crate::services::connector_audit::Actor::System,
                action: "disconnect",
                provider: None,
                subject: Some(user_id.to_string()),
                detail: Some(serde_json::json!({
                    "reason": "member_offboarded",
                    "tokens_deleted": outcome.tokens_deleted,
                    "installs_deleted": outcome.installs_deleted,
                    "watch_channels_deleted": outcome.watch_channels_deleted,
                    "graph_subscriptions_deleted": outcome.graph_subscriptions_deleted,
                    "upstream_failures": outcome.upstream_failures,
                })),
            },
        )
        .await;
    }
    if outcome.purge_failed || outcome.upstream_failures > 0 {
        tracing::warn!(
            %account_id, %user_id, ?outcome, connector_health = true,
            "member offboarding: credentials revoked, but a best-effort step failed (see earlier lines)"
        );
    }
    Ok(outcome)
}

/// Run one upstream revoke unless the grant is shared (or unknowable).
async fn record<F, Fut>(outcome: &mut OffboardOutcome, held_elsewhere: Option<bool>, call: F, what: &str)
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<(), String>>,
{
    match held_elsewhere {
        Some(false) => {
            outcome.upstream_calls += 1;
            if let Err(e) = call().await {
                outcome.upstream_failures += 1;
                tracing::warn!(error = %e, what, "member offboarding: upstream revoke failed (non-fatal; the local credential is already gone)");
            }
        }
        // Shared, or the check itself failed: never risk revoking a grant
        // another connection still depends on.
        Some(true) | None => outcome.revokes_skipped_shared += 1,
    }
}

/// `oauth_grant_held_elsewhere` (control 0368). `None` when the check fails.
async fn grant_held_elsewhere(
    control: &PgPool,
    providers: &[&str],
    row: &DeletedToken,
    match_workspace: bool,
    account_id: Uuid,
    user_id: Uuid,
) -> Option<bool> {
    // No identifier at all (pre-2026-07-16 rows): sharing is unknowable, so
    // treat it as shared and never revoke. The SQL function answers the same;
    // checked here too so the decision never depends on one layer.
    if row.provider_user_id.as_deref().unwrap_or("").is_empty()
        && row.provider_workspace_id.as_deref().unwrap_or("").is_empty()
    {
        return Some(true);
    }
    let providers: Vec<String> = providers.iter().map(|p| p.to_string()).collect();
    match sqlx::query_scalar::<_, bool>("SELECT oauth_grant_held_elsewhere($1, $2, $3, $4, $5, $6)")
        .bind(&providers)
        .bind(row.provider_user_id.as_deref().unwrap_or(""))
        .bind(row.provider_workspace_id.as_deref().unwrap_or(""))
        .bind(match_workspace)
        .bind(account_id)
        .bind(user_id)
        .fetch_one(control)
        .await
    {
        Ok(held) => Some(held),
        Err(e) => {
            tracing::warn!(error = %e, %account_id, "member offboarding: grant-sharing check failed — skipping the revoke");
            None
        }
    }
}

/// Stop the member's watches for ONE deleted Google token, by its provider.
#[allow(clippy::too_many_arguments)]
async fn stop_google_watches(
    upstream: &dyn OffboardUpstream,
    control: &PgPool,
    watches: &[OwnedWatch],
    row: &DeletedToken,
    token: &str,
    account_id: Uuid,
    user_id: Uuid,
    control_clock: DateTime<Utc>,
    outcome: &mut OffboardOutcome,
) {
    match row.provider.as_str() {
        "google_gmail" => {
            let email = row.provider_workspace_id.as_deref().unwrap_or("");
            if !watches.iter().any(|w| w.surface == "gmail" && w.resource_id == email) {
                return;
            }
            // `users.stop` is mailbox-wide: another account still watching
            // this mailbox (the member's personal account) would lose its
            // pushes too.
            let others = match crate::connectors::google_gmail::resolve_watch_owners_for_push(control, email).await {
                Ok(owners) => owners.into_iter().any(|(a, u)| !(a == account_id && u == user_id)),
                Err(e) => {
                    tracing::warn!(error = %e, %account_id, "member offboarding: gmail watch-owner check failed — skipping users.stop");
                    true
                }
            };
            if others {
                outcome.revokes_skipped_shared += 1;
                return;
            }
            outcome.upstream_calls += 1;
            if let Err(e) = upstream.stop_gmail_watch(token).await {
                outcome.upstream_failures += 1;
                tracing::warn!(error = %e, %account_id, "member offboarding: gmail users.stop failed — the watch lapses within 7 days and its pushes are dropped");
            }
        }
        "google" => {
            for w in watches.iter().filter(|w| w.surface == "calendar") {
                if w.channel_id.is_empty() || w.channel_resource_id.is_empty() {
                    continue;
                }
                outcome.upstream_calls += 1;
                if let Err(e) = upstream.stop_calendar_channel(token, &w.channel_id, &w.channel_resource_id).await {
                    outcome.upstream_failures += 1;
                    tracing::warn!(error = %e, %account_id, "member offboarding: calendar channels.stop failed — it expires on its own");
                }
            }
        }
        surface @ ("google_chat" | "google_meet") => {
            let surface = if surface == "google_meet" { "meet" } else { "chat" };
            if watches.iter().any(|w| w.surface == surface) {
                outcome.upstream_calls += 1;
                upstream
                    .delete_workspace_subscription(control, account_id, user_id, token, surface, control_clock)
                    .await;
            }
        }
        _ => {}
    }
}

async fn owned_watches(control: &PgPool, account_id: Uuid, user_id: Uuid) -> Result<Vec<OwnedWatch>, sqlx::Error> {
    let mut tx = begin_account_scoped(control, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
    let rows = sqlx::query_as(
        "SELECT surface, resource_id, channel_id, channel_resource_id FROM google_watch_channels \
         WHERE account_id = $1 AND owner_user_id = $2",
    )
    .bind(account_id)
    .bind(user_id)
    .fetch_all(&mut *tx)
    .await?;
    tx.rollback().await.ok();
    Ok(rows)
}

/// Delete any `google_watch_channels`/`graph_subscriptions` row still owned by
/// the member on `pool` (watch rows only when last touched before
/// `not_after`). Returns the watch-row count and the removed subscriptions'
/// `(subscription_id, tenant_id)`.
async fn delete_owned_watch_rows(
    pool: &PgPool,
    account_id: Uuid,
    user_id: Uuid,
    not_after: DateTime<Utc>,
    tenant: bool,
) -> Result<(usize, Vec<(String, String)>), sqlx::Error> {
    let mut tx = if tenant {
        begin_tenant_system_scoped(pool, account_id).await
    } else {
        begin_account_scoped(pool, account_id).await
    }
    .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
    let watches = sqlx::query(
        "DELETE FROM google_watch_channels WHERE account_id = $1 AND owner_user_id = $2 AND updated_at <= $3",
    )
    .bind(account_id)
    .bind(user_id)
    .bind(not_after)
    .execute(&mut *tx)
    .await?
    .rows_affected() as usize;
    let subscriptions: Vec<(String, String)> = sqlx::query_as(
        "DELETE FROM graph_subscriptions WHERE account_id = $1 AND owner_user_id = $2 \
         RETURNING subscription_id, tenant_id",
    )
    .bind(account_id)
    .bind(user_id)
    .fetch_all(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok((watches, subscriptions))
}

fn same_database(a: &PgPool, b: &PgPool) -> bool {
    let (a, b) = (a.connect_options(), b.connect_options());
    (a.get_host(), a.get_port(), a.get_database()) == (b.get_host(), b.get_port(), b.get_database())
}

/// Gate helper for capture paths: `true` means "go ahead". On a definite
/// non-member it offboards (stops the watch) and returns `false`; on a lookup
/// error it returns `false` WITHOUT offboarding (fail closed, do not destroy
/// state on a guess). `plane` as for [`revoke_departed_member_capture`].
pub(crate) async fn admit_capture_or_offboard(
    http: &reqwest::Client,
    control: &PgPool,
    plane: Option<&DekPlane>,
    account_id: Uuid,
    user_id: Uuid,
    path: &'static str,
) -> bool {
    match capture_owner_is_live_member(control, account_id, user_id).await {
        Ok(true) => true,
        Ok(false) => {
            tracing::warn!(
                %account_id, %user_id, path, connector_health = true,
                "capture refused: the watch owner is no longer a member of this account — \
                 dropping the event and offboarding their capture"
            );
            let upstream = HttpUpstream { http };
            match revoke_departed_member_capture(&upstream, http, control, plane, account_id, user_id).await {
                Ok(outcome) => tracing::info!(%account_id, %user_id, path, ?outcome, "capture gate: offboarded a departed member"),
                Err(e) => tracing::error!(error = %e, %account_id, %user_id, path, "capture gate: offboarding a departed member failed"),
            }
            false
        }
        Err(e) => {
            tracing::warn!(error = %e, %account_id, %user_id, path, "capture refused: membership check failed (fail closed)");
            false
        }
    }
}


// ── Live-Postgres tests (settings launch PR 2.8) ────────────────────────────
//
// `ORGT_TEST_BASE_URL`-gated like every two-database (control + org tenant)
// suite: each test creates its own control and tenant databases, migrates
// both trees, and drops them. Without the variable each test prints a skip
// line and returns — CI's `cross-tenant-probe` step for this module fails the
// job on that line, so a green run here means the tests really ran.
#[cfg(test)]
mod member_offboarding_pg_tests {
    use super::*;
    use coralswarm_core::kek::{generate_dek, load_kek, wrap_dek};
    use coralswarm_core::oauth_token::encrypt_token;
    use sqlx::postgres::PgPoolOptions;
    use std::sync::{Arc, Mutex};

    fn base_url() -> Option<String> {
        std::env::var("ORGT_TEST_BASE_URL").ok()
    }

    fn with_db(base: &str, db: &str) -> String {
        let (prefix, rest) = base.split_at(base.rfind('/').expect("url has a path /"));
        let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
        format!("{prefix}/{db}{params}")
    }

    async fn recreate(base: &str, db: &str) {
        use sqlx::{Connection, Executor};
        let mut conn = sqlx::PgConnection::connect(base).await.expect("connect admin");
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.unwrap();
        conn.execute(format!("CREATE DATABASE {db}").as_str()).await.unwrap();
        conn.close().await.ok();
    }

    async fn drop_db(base: &str, db: &str) {
        use sqlx::{Connection, Executor};
        let mut conn = sqlx::PgConnection::connect(base).await.expect("connect admin");
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
        conn.close().await.ok();
    }

    fn set_test_kek() {
        // SAFETY: every test in the crate sets the same constant.
        unsafe { std::env::set_var("MASTER_KEK_HEX", "cc".repeat(32)) };
    }

    /// Records every provider call; answers `Err` to all of them when `fail`.
    struct FakeUpstream {
        fail: bool,
        calls: Mutex<Vec<String>>,
    }

    impl FakeUpstream {
        fn new(fail: bool) -> Self {
            FakeUpstream { fail, calls: Mutex::new(Vec::new()) }
        }
        fn push(&self, call: String) -> Result<(), String> {
            self.calls.lock().unwrap().push(call);
            if self.fail { Err("provider is down".into()) } else { Ok(()) }
        }
        fn calls(&self) -> Vec<String> {
            let mut c = self.calls.lock().unwrap().clone();
            c.sort();
            c
        }
    }

    #[async_trait]
    impl OffboardUpstream for FakeUpstream {
        async fn google_access_token(&self, creds: ConnectorCredentials) -> Option<String> {
            Some(creds.access_token)
        }
        async fn microsoft_access_token(&self, creds: ConnectorCredentials) -> Option<String> {
            Some(creds.access_token)
        }
        async fn stop_gmail_watch(&self, access_token: &str) -> Result<(), String> {
            self.push(format!("gmail_stop:{access_token}"))
        }
        async fn stop_calendar_channel(&self, access_token: &str, channel_id: &str, _r: &str) -> Result<(), String> {
            self.push(format!("calendar_stop:{access_token}:{channel_id}"))
        }
        async fn delete_workspace_subscription(
            &self,
            _c: &PgPool,
            _a: Uuid,
            _u: Uuid,
            access_token: &str,
            surface: &str,
            _b: DateTime<Utc>,
        ) -> bool {
            self.push(format!("workspace_subscription_delete:{surface}:{access_token}")).is_ok()
        }
        async fn revoke_google_token(&self, token: &str) -> Result<(), String> {
            self.push(format!("google_revoke:{token}"))
        }
        async fn revoke_slack_token(&self, token: &str) -> Result<(), String> {
            self.push(format!("slack_revoke:{token}"))
        }
        async fn delete_graph_subscription(&self, access_token: &str, subscription_id: &str) -> Result<(), String> {
            self.push(format!("graph_delete:{access_token}:{subscription_id}"))
        }
    }

    struct Fixture {
        control: PgPool,
        tenant: PgPool,
        org_id: String,
        org: Uuid,
        dek: [u8; 32],
        alice: Uuid,
        bob: Uuid,
        bob_clerk_id: String,
        bob_personal: Uuid,
    }

    impl Fixture {
        fn plane(&self) -> DekPlane {
            DekPlane::org(self.org_id.clone(), self.tenant.clone(), self.org)
        }
    }

    /// An org (owner alice, member bob) with its own tenant database and DEK,
    /// plus bob's own personal account.
    async fn fixture(base: &str, control_db: &str, tenant_db: &str, org_id: &str) -> Fixture {
        set_test_kek();
        recreate(base, control_db).await;
        recreate(base, tenant_db).await;
        let control = PgPoolOptions::new().connect(&with_db(base, control_db)).await.unwrap();
        sqlx::migrate!("../backend/migrations_control").run(&control).await.unwrap();
        let tenant = PgPoolOptions::new().connect(&with_db(base, tenant_db)).await.unwrap();
        sqlx::migrate!("../backend/migrations_tenant").run(&tenant).await.unwrap();
        sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, $1)")
            .bind(org_id)
            .execute(&tenant)
            .await
            .unwrap();

        let user = |label: &str| {
            let control = control.clone();
            let label = label.to_string();
            async move {
                let clerk = format!("user_{label}_{}", Uuid::new_v4().simple());
                let id: Uuid = sqlx::query_scalar(
                    "INSERT INTO users(id, email, clerk_user_id) VALUES (gen_random_uuid(), $1, $2) RETURNING id",
                )
                .bind(format!("{label}-{}@corp.test", Uuid::new_v4().simple()))
                .bind(&clerk)
                .fetch_one(&control)
                .await
                .unwrap();
                (id, clerk)
            }
        };
        let (alice, _) = user("alice").await;
        let (bob, bob_clerk_id) = user("bob").await;

        let dek = generate_dek();
        let wrapped = wrap_dek(&load_kek().unwrap(), &dek).unwrap();
        let org: Uuid = sqlx::query_scalar(
            "INSERT INTO accounts(owner_user_id, clerk_org_id, dek_wrapped) VALUES ($1, $2, $3) RETURNING id",
        )
        .bind(alice)
        .bind(org_id)
        .bind(&wrapped)
        .fetch_one(&control)
        .await
        .unwrap();
        for (who, role) in [(alice, "owner"), (bob, "member")] {
            let n = sqlx::query(
                "INSERT INTO account_members(account_id, user_id, role, seat_tier_id) \
                 SELECT $1, $2, $3, t.id FROM seat_tiers t WHERE t.code = 'standard' AND t.is_active",
            )
            .bind(org)
            .bind(who)
            .bind(role)
            .execute(&control)
            .await
            .unwrap()
            .rows_affected();
            assert_eq!(n, 1, "seed {role} membership");
        }
        sqlx::query("INSERT INTO account_keys (account_id, dek_wrapped) VALUES ($1, $2)")
            .bind(org)
            .bind(&wrapped)
            .execute(&tenant)
            .await
            .unwrap();
        let bob_personal: Uuid =
            sqlx::query_scalar("INSERT INTO accounts(owner_user_id, clerk_org_id) VALUES ($1, NULL) RETURNING id")
                .bind(bob)
                .fetch_one(&control)
                .await
                .unwrap();
        Fixture { control, tenant, org_id: org_id.to_string(), org, dek, alice, bob, bob_clerk_id, bob_personal }
    }

    #[allow(clippy::too_many_arguments)]
    async fn token(
        f: &Fixture,
        account: Uuid,
        owner: Uuid,
        provider: &str,
        workspace: &str,
        provider_user: &str,
        access: &str,
        refresh: Option<&str>,
    ) {
        let enc = |t: &str| encrypt_token(&f.dek, t).unwrap();
        sqlx::query(
            "INSERT INTO oauth_tokens (account_id, owner_user_id, provider, scope, access_token, refresh_token, \
                 expires_at, provider_user_id, provider_workspace_id) \
             VALUES ($1, $2, $3, 'test', $4, $5, now() + interval '1 hour', $6, $7)",
        )
        .bind(account)
        .bind(owner)
        .bind(provider)
        .bind(enc(access))
        .bind(refresh.map(enc))
        .bind(provider_user)
        .bind(workspace)
        .execute(&f.control)
        .await
        .unwrap();
    }

    async fn watch(pool: &PgPool, account: Uuid, owner: Uuid, surface: &str, resource: &str, channel: &str) {
        sqlx::query(
            "INSERT INTO google_watch_channels \
                (account_id, surface, resource_id, owner_user_id, channel_id, channel_resource_id, topic_name, expires_at, updated_at) \
             VALUES ($1, $2, $3, $4, $5, $5 || '-res', 'projects/p/topics/t', now() + interval '7 days', now() - interval '1 minute')",
        )
        .bind(account)
        .bind(surface)
        .bind(resource)
        .bind(owner)
        .bind(channel)
        .execute(pool)
        .await
        .unwrap();
    }

    async fn count(pool: &PgPool, sql: &str, account: Uuid, user: Uuid) -> i64 {
        sqlx::query_scalar(sql).bind(account).bind(user).fetch_one(pool).await.unwrap()
    }

    async fn leave(f: &Fixture, user: Uuid) {
        sqlx::query("DELETE FROM account_members WHERE account_id = $1 AND user_id = $2")
            .bind(f.org)
            .bind(user)
            .execute(&f.control)
            .await
            .unwrap();
    }

    const TOKENS: &str = "SELECT count(*) FROM oauth_tokens WHERE account_id = $1 AND owner_user_id = $2";
    const WATCHES: &str = "SELECT count(*) FROM google_watch_channels WHERE account_id = $1 AND owner_user_id = $2";

    /// The headline property. bob leaves the org: every org-scoped credential,
    /// install, Slack member capture, watch and Graph subscription of HIS is
    /// gone; his personal account's Gmail token and watch on the SAME Google
    /// identity survive, and so does alice's. Upstream: his Calendar channel
    /// and his Slack token are revoked and his delegated Graph subscription is
    /// deleted, but his Google grant is NOT revoked and his mailbox's Gmail
    /// watch is NOT stopped, because his personal account still uses both.
    /// Every provider call fails here, and none of that stops the local
    /// revocation. A second run is a no-op.
    #[tokio::test]
    async fn offboarding_revokes_the_org_copy_keeps_personal_and_is_idempotent_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping offboarding_revokes_the_org_copy_keeps_personal_and_is_idempotent_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let (cdb, tdb) = ("offboard_revoke_ctrl", "offboard_revoke_tnt");
        let f = fixture(&base, cdb, tdb, "org_offboard_revoke").await;
        let mailbox = "bob@corp.test";

        // bob's org connections.
        token(&f, f.org, f.bob, "google_gmail", mailbox, "sub-bob", "gmail-access", Some("gmail-refresh")).await;
        token(&f, f.org, f.bob, "google", mailbox, "sub-bob", "cal-access", Some("cal-refresh")).await;
        token(&f, f.org, f.bob, "slack_user", "T1", "U-bob", "xoxp-bob", None).await;
        token(&f, f.org, f.bob, "microsoft", "aad-1", "oid-bob", "ms-access", Some("ms-refresh")).await;
        watch(&f.control, f.org, f.bob, "gmail", mailbox, "").await;
        watch(&f.control, f.org, f.bob, "calendar", "primary", "cal-ch-1").await;
        sqlx::query(
            "INSERT INTO connector_installs (provider, external_workspace_id, account_id, owner_user_id, clerk_org_id) \
             VALUES ('slack', 'T1', $1, $2, $3)",
        )
        .bind(f.org).bind(f.bob).bind(&f.org_id).execute(&f.control).await.unwrap();
        sqlx::query("INSERT INTO slack_member_channels (account_id, user_id, workspace_id, channel_id) VALUES ($1, $2, 'T1', 'C1')")
            .bind(f.org).bind(f.bob).execute(&f.control).await.unwrap();
        sqlx::query(
            "INSERT INTO graph_subscriptions (account_id, subscription_id, resource, client_state, expires_at, tenant_id, owner_user_id) \
             VALUES ($1, 'sub-bob', 'me/mailFolders(''inbox'')/messages', 'cs', now() + interval '1 day', '', $2)",
        )
        .bind(f.org).bind(f.bob).execute(&f.control).await.unwrap();
        // bob's PERSONAL account: the same Google identity and mailbox.
        token(&f, f.bob_personal, f.bob, "google_gmail", mailbox, "sub-bob", "personal-access", Some("personal-refresh")).await;
        watch(&f.control, f.bob_personal, f.bob, "gmail", mailbox, "").await;
        // alice stays.
        token(&f, f.org, f.alice, "google_gmail", "alice@corp.test", "sub-alice", "alice-access", Some("alice-refresh")).await;
        watch(&f.control, f.org, f.alice, "gmail", "alice@corp.test", "").await;

        // Still a member: nothing is touched.
        let fake = FakeUpstream::new(true);
        let http = reqwest::Client::new();
        let plane = f.plane();
        let early = revoke_departed_member_capture(&fake, &http, &f.control, Some(&plane), f.org, f.bob).await.unwrap();
        assert_eq!(early.skipped, Some(Skipped::StillMember));
        assert_eq!(count(&f.control, TOKENS, f.org, f.bob).await, 4, "a current member keeps every token");

        leave(&f, f.bob).await;
        let out = revoke_departed_member_capture(&fake, &http, &f.control, Some(&plane), f.org, f.bob).await.unwrap();
        assert_eq!(out.skipped, None);
        assert_eq!(out.tokens_deleted, 4);
        assert_eq!(out.installs_deleted, 1);
        assert_eq!(out.slack_member_channels_deleted, 1);
        assert_eq!(out.undecryptable_tokens, 0, "the fixture's tokens are under the org DEK");

        assert_eq!(count(&f.control, TOKENS, f.org, f.bob).await, 0, "bob's org tokens are gone");
        assert_eq!(count(&f.control, WATCHES, f.org, f.bob).await, 0, "bob's org watches are gone");
        assert_eq!(
            count(&f.control, "SELECT count(*) FROM connector_installs WHERE account_id = $1 AND owner_user_id = $2", f.org, f.bob).await,
            0
        );
        assert_eq!(
            count(&f.control, "SELECT count(*) FROM slack_member_channels WHERE account_id = $1 AND user_id = $2", f.org, f.bob).await,
            0
        );
        assert_eq!(
            count(&f.control, "SELECT count(*) FROM graph_subscriptions WHERE account_id = $1 AND owner_user_id = $2", f.org, f.bob).await,
            0
        );
        assert_eq!(count(&f.control, TOKENS, f.bob_personal, f.bob).await, 1, "bob's PERSONAL token survives");
        assert_eq!(count(&f.control, WATCHES, f.bob_personal, f.bob).await, 1, "bob's PERSONAL watch survives");
        assert_eq!(count(&f.control, TOKENS, f.org, f.alice).await, 1, "alice's token survives");
        assert_eq!(count(&f.control, WATCHES, f.org, f.alice).await, 1, "alice's watch survives");

        assert_eq!(
            fake.calls(),
            vec![
                "calendar_stop:cal-access:cal-ch-1".to_string(),
                "graph_delete:ms-access:sub-bob".to_string(),
                "slack_revoke:xoxp-bob".to_string(),
            ],
            "no Google revoke and no users.stop: bob's personal account shares that grant and mailbox"
        );
        assert_eq!(out.upstream_failures, 3, "every provider call failed, and the revocation still held");
        assert_eq!(out.revokes_skipped_shared, 3, "users.stop + two Google revokes skipped as shared");

        // Clerk redelivers the webhook / a gate fires again: nothing left to do.
        let again = revoke_departed_member_capture(&fake, &http, &f.control, Some(&plane), f.org, f.bob).await.unwrap();
        assert_eq!(again, OffboardOutcome::default(), "a second run is a no-op");
        assert_eq!(fake.calls().len(), 3, "and makes no provider call");
        assert_eq!(count(&f.control, TOKENS, f.bob_personal, f.bob).await, 1);

        // A personal account is never offboarded.
        let personal = revoke_departed_member_capture(&fake, &http, &f.control, Some(&plane), f.bob_personal, f.bob).await.unwrap();
        assert_eq!(personal.skipped, Some(Skipped::PersonalAccount));

        f.control.close().await;
        f.tenant.close().await;
        drop_db(&base, cdb).await;
        drop_db(&base, tdb).await;
    }

    /// A grant nobody else holds IS revoked (with the refresh token, which
    /// revokes the grant), and the mailbox's only watch IS stopped.
    #[tokio::test]
    async fn an_unshared_google_grant_is_revoked_and_its_gmail_watch_stopped_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping an_unshared_google_grant_is_revoked_and_its_gmail_watch_stopped_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let (cdb, tdb) = ("offboard_unshared_ctrl", "offboard_unshared_tnt");
        let f = fixture(&base, cdb, tdb, "org_offboard_unshared").await;
        token(&f, f.org, f.bob, "google_gmail", "bob@corp.test", "sub-bob", "gmail-access", Some("gmail-refresh")).await;
        watch(&f.control, f.org, f.bob, "gmail", "bob@corp.test", "").await;
        leave(&f, f.bob).await;

        let fake = FakeUpstream::new(false);
        let plane = f.plane();
        let out = revoke_departed_member_capture(&fake, &reqwest::Client::new(), &f.control, Some(&plane), f.org, f.bob)
            .await
            .unwrap();
        assert_eq!(out.tokens_deleted, 1);
        assert_eq!(out.upstream_failures, 0);
        assert_eq!(
            fake.calls(),
            vec!["gmail_stop:gmail-access".to_string(), "google_revoke:gmail-refresh".to_string()]
        );
        assert_eq!(count(&f.control, WATCHES, f.org, f.bob).await, 0);

        f.control.close().await;
        f.tenant.close().await;
        drop_db(&base, cdb).await;
        drop_db(&base, tdb).await;
    }

    /// Review fix (settings PR 2.8): a grant is never revoked when it might be
    /// the one the same person still uses. (a) An org `google` row with NO
    /// identifiers (pre-2026-07-16 rows can lack both `sub` and email) next to
    /// bob's personal account-wide `google` Calendar row: nothing to compare,
    /// so no revoke. (b) An org `google_gmail` row whose identifiers differ
    /// from his personal row's: still bob's own grant family on his own
    /// account, so no revoke. Locally both org rows are still deleted.
    #[tokio::test]
    async fn a_grant_the_same_person_may_still_use_is_never_revoked_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping a_grant_the_same_person_may_still_use_is_never_revoked_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let (cdb, tdb) = ("offboard_same_person_ctrl", "offboard_same_person_tnt");
        let f = fixture(&base, cdb, tdb, "org_offboard_same_person").await;
        // (a) empty identifiers on the org row.
        token(&f, f.org, f.bob, "google", "", "", "cal-access", Some("cal-refresh")).await;
        // (b) different identifiers on the org row.
        token(&f, f.org, f.bob, "google_gmail", "bob-work@corp.test", "sub-work", "gmail-access", Some("gmail-refresh")).await;
        // bob's personal account-wide Calendar connection: owner_user_id NULL,
        // no identifiers either — matched only as "his own account".
        sqlx::query(
            "INSERT INTO oauth_tokens (account_id, owner_user_id, provider, scope, access_token, provider_user_id, provider_workspace_id) \
             VALUES ($1, NULL, 'google', 'test', $2, '', '')",
        )
        .bind(f.bob_personal)
        .bind(encrypt_token(&f.dek, "personal-cal").unwrap())
        .execute(&f.control)
        .await
        .unwrap();
        leave(&f, f.bob).await;

        let fake = FakeUpstream::new(false);
        let plane = f.plane();
        let out = revoke_departed_member_capture(&fake, &reqwest::Client::new(), &f.control, Some(&plane), f.org, f.bob)
            .await
            .unwrap();
        assert_eq!(out.tokens_deleted, 2, "both org rows are deleted locally");
        assert!(
            !fake.calls().iter().any(|c| c.starts_with("google_revoke")),
            "no Google grant revoke while bob may still use it: {:?}",
            fake.calls()
        );
        assert_eq!(out.revokes_skipped_shared, 2);
        let personal: i64 = sqlx::query_scalar("SELECT count(*) FROM oauth_tokens WHERE account_id = $1")
            .bind(f.bob_personal)
            .fetch_one(&f.control)
            .await
            .unwrap();
        assert_eq!(personal, 1, "bob's personal Calendar row survives");

        f.control.close().await;
        f.tenant.close().await;
        drop_db(&base, cdb).await;
        drop_db(&base, tdb).await;
    }

    /// The real offboarding entry point, end to end:
    /// `clerk_provision::remove_org_membership` (what the Clerk webhook and
    /// PR #1465's admin route both call) leaves none of bob's org tokens or
    /// watches behind, keeps his personal account's, and a redelivery is a
    /// clean no-op. The tokens here carry ciphertext no DEK opens, so the
    /// production upstream client is never reached (no network in a test).
    #[tokio::test]
    async fn remove_org_membership_revokes_org_connectors_and_a_replay_is_a_no_op_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping remove_org_membership_revokes_org_connectors_and_a_replay_is_a_no_op_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let (cdb, tdb) = ("offboard_e2e_ctrl", "offboard_e2e_tnt");
        let f = fixture(&base, cdb, tdb, "org_offboard_e2e").await;
        let secret = "SECRET_ORG_OFFBOARD_E2E";
        crate::billing::renewal::execute_financial_fixture(
            &f.control,
            f.org,
            sqlx::query(
                "INSERT INTO tenant_databases \
                    (clerk_org_id, account_id, slug, db_secret_ref, substrate, schema_version, status) \
                 VALUES ($1, $2, 'org-offboard-e2e', $3, 'neon', 1, 'active')",
            )
            .bind(&f.org_id)
            .bind(f.org)
            .bind(secret),
        )
        .await
        .unwrap();
        let resolver = crate::tenancy::secrets::StaticMapResolver::from_pairs([(secret, with_db(&base, tdb))]);
        let registry = Arc::new(crate::tenancy::registry::TenantRegistry::new(Arc::new(resolver)));
        let runtime = crate::tenancy::runtime::TenantRuntime::new(true, "coralswarm.test", registry);
        let state = crate::test_app_state_owned_with_db_and_store(f.control.clone(), Some(runtime));

        for (account, provider, ws) in [
            (f.org, "google_gmail", "bob@corp.test"),
            (f.org, "slack_user", "T1"),
            (f.bob_personal, "google_gmail", "bob@corp.test"),
        ] {
            sqlx::query(
                "INSERT INTO oauth_tokens (account_id, owner_user_id, provider, scope, access_token, provider_user_id, provider_workspace_id) \
                 VALUES ($1, $2, $3, 'test', '\\x00'::bytea, 'sub-bob', $4)",
            )
            .bind(account).bind(f.bob).bind(provider).bind(ws).execute(&f.control).await.unwrap();
        }
        watch(&f.control, f.org, f.bob, "gmail", "bob@corp.test", "").await;
        watch(&f.control, f.bob_personal, f.bob, "gmail", "bob@corp.test", "").await;

        for round in 0..2 {
            crate::clerk_provision::remove_org_membership(&state, &f.control, &f.org_id, &f.bob_clerk_id)
                .await
                .unwrap_or_else(|e| panic!("offboarding round {round} must succeed: {e:?}"));
            assert_eq!(count(&f.control, TOKENS, f.org, f.bob).await, 0, "round {round}: org tokens gone");
            assert_eq!(count(&f.control, WATCHES, f.org, f.bob).await, 0, "round {round}: org watch gone");
            assert_eq!(count(&f.control, TOKENS, f.bob_personal, f.bob).await, 1, "round {round}: personal token kept");
            assert_eq!(count(&f.control, WATCHES, f.bob_personal, f.bob).await, 1, "round {round}: personal watch kept");
        }

        f.control.close().await;
        f.tenant.close().await;
        drop_db(&base, cdb).await;
        drop_db(&base, tdb).await;
    }
}
