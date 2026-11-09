//! Microsoft connector — one OAuth connection over the Microsoft identity
//! platform (Azure AD v2.0) that unlocks Microsoft Graph: Outlook mail,
//! calendar, and Teams (channels/chats/meeting transcripts). This was the
//! proving ground for the connector framework: it exercises code exchange,
//! `id_token` claim parsing, token refresh, and the pull-based "sync now"
//! path end to end.
//!
//! # Scopes — FROZEN (m365 enterprise train PR 3.2)
//! Default [`scopes`]: `offline_access openid profile User.Read User.Read.All
//! Mail.Read Calendars.Read Team.ReadBasic.All Channel.ReadBasic.All
//! ChannelMessage.Read.All Chat.Read OnlineMeetings.ReadWrite
//! OnlineMeetingTranscript.Read.All`. `User.Read.All` is the delegated
//! directory read for `otherMails` when a non-guest roster row has no
//! email; `User.ReadBasic.All` does not expose that property. Adding it
//! is not retro-granted — a tenant that already consented must consent
//! again. `offline_access` yields a refresh
//! token; `openid`/`profile` yield an `id_token` we decode for the tenant
//! (`tid`) and user (`oid`). This set is now the scope-freeze artifact a
//! tenant admin consents to — see `docs/connectors/outlook.md`'s "Scope
//! freeze" section for the full per-scope rationale, and `.env.example`'s
//! `MICROSOFT_SCOPES` comment for why this file's default and that env var
//! must never drift apart (they did, once — see that comment). Once a
//! tenant admin has granted org-wide consent for this set
//! ([`ConnectorProvider::admin_consent_url`]), it is NOT retro-granted: an
//! added scope requires every already-consented tenant to re-consent.

use std::collections::HashSet;

use async_trait::async_trait;
use base64::Engine as _;
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Row};
use uuid::Uuid;

use super::{
    ConnectorCredentials, ConnectorError, ConnectorProvider, CredentialDeathReason, IngestScope,
    NormalizedItem, TokenBundle, TokenOwner,
};

/// The Microsoft Graph OAuth connector.
pub struct MicrosoftConnector;

// ── RLS scoping (org-tenancy conversion) ──────────────────────────────────────
//
// `graph_subscriptions`, `outlook_watched_conversations` and
// `teams_watched_resources` all carry `account_id`-keyed RLS policies on the
// consumer plane (migration `0083_consumer_rls`); `outlook_watched_
// conversations` and `teams_watched_resources` are ALSO on the tenant tree's
// `member_ocean_isolation` roster (`migrations_tenant/0009_tenant_rls`,
// anchored via their `graph_id`), while `graph_subscriptions` has no anchor
// there and is deliberately left uncovered (see `0009`'s own header, §6f) —
// so every query in this module that touches any of the three runs inside a
// `tenancy::scope::begin_tenant_system_scoped` transaction, not the plain
// `begin_account_scoped` this module used before the graph-subscriptions
// worker joined the tenant loop (m365 Train 2 PR 2.2). `begin_tenant_system_
// scoped` is a strict superset of `begin_account_scoped` — same
// `app.account_id` GUC, plus `app.tenant_actor='system'`, which `0083`'s
// policy never reads — so this is byte-identical on the consumer plane (the
// same equivalence `connectors::slack`'s own conversion documents) and now
// also correct against an org tenant pool, where these functions have no
// acting end user to present. Inert pre-OPS-4 (when the app still connected
// as a `BYPASSRLS` role); load-bearing now that OPS-4 has switched the app to
// the non-`BYPASSRLS` `coralswarm_app`.
//
// Three helpers here are *deliberately cross-account* — they are the
// once-per-tick WORK-SET DISCOVERY queries the CONSUMER-PLANE renewal/
// backstop sweeps in `workers/graph_subscriptions.rs` start from, and by
// definition cannot be scoped to an account they exist to enumerate. Rather
// than leave them as raw cross-account table reads (which post-OPS-4 — i.e.
// now — would silently return zero rows and quietly stop every subscription
// renewal), each goes through a narrow
// `SECURITY DEFINER` resolver from migration
// `0086_webhook_share_bootstrap_functions` that returns ONLY bare ids — never a
// content column, and never `graph_subscriptions.client_state` (the
// notification-validation secret). Every follow-up read of the actual row
// contents then happens under a proper per-account scope. See
// `expiring_subscriptions`, `accounts_with_watched_conversations` and
// `accounts_with_teams_watched` below. These three resolvers are
// CONSUMER-PLANE-ONLY — migration `0086` has no tenant-tree counterpart —
// which is fine: an org tenant database holds exactly one account (ADR-0007),
// so there is no cross-account set to discover in the first place. The
// `_for_account` siblings next to several of these (`expiring_subscriptions_
// for_account`, `set_subscription_expiry_for_account`, `delete_subscription_
// row_for_account`) are the org-tenant path: the caller already knows the
// one account, so they skip both the cross-account resolver and the
// `resolve_graph_subscription_account_id` re-derivation the plain
// `set_subscription_expiry`/`delete_subscription_row` do (also `0086`,
// also control-only).

/// Open a tenant-system-scoped transaction (see this module's RLS-scoping
/// note above), mapping the failure onto this module's
/// [`ConnectorError::Internal`] (whose text is logged, never returned to a
/// client — see `routes/connectors.rs::connector_error_response`).
async fn scoped_tx(
    pool: &PgPool,
    account_id: Uuid,
    what: &str,
) -> Result<sqlx::Transaction<'static, sqlx::Postgres>, ConnectorError> {
    crate::tenancy::scope::begin_tenant_system_scoped(pool, account_id)
        .await
        .map_err(|e| ConnectorError::Internal(format!("{what} scoped tx: {e}")))
}

// ── Env helpers (read at call time; connector itself is stateless) ─────────────

fn client_id() -> Result<String, ConnectorError> {
    std::env::var("MICROSOFT_CLIENT_ID")
        .map_err(|_| ConnectorError::NotConfigured("MICROSOFT_CLIENT_ID not configured".into()))
}

fn client_secret() -> Result<String, ConnectorError> {
    std::env::var("MICROSOFT_CLIENT_SECRET")
        .map_err(|_| ConnectorError::NotConfigured("MICROSOFT_CLIENT_SECRET not configured".into()))
}

/// Azure AD tenant. `common` allows both work/school and personal accounts.
fn tenant() -> String {
    std::env::var("MICROSOFT_TENANT").unwrap_or_else(|_| "common".into())
}

/// The `/v2.0/adminconsent` URL for tenant-wide admin consent — pure so the
/// URL shape is unit-testable without env-var races. `common` maps to
/// `organizations`: the adminconsent endpoint has no "any account type"
/// pseudo-tenant (a specific ORGANIZATION's admin consents); a configured
/// real tenant id/domain passes through as-is.
fn build_admin_consent_url(
    configured_tenant: &str,
    client_id: &str,
    redirect_uri: &str,
    state: &str,
) -> String {
    let consent_tenant = if configured_tenant == "common" {
        "organizations"
    } else {
        configured_tenant
    };
    format!(
        "https://login.microsoftonline.com/{}/v2.0/adminconsent?client_id={}&redirect_uri={}&scope={}&state={}",
        consent_tenant,
        urlencoding::encode(client_id),
        urlencoding::encode(redirect_uri),
        urlencoding::encode(&scopes()),
        urlencoding::encode(state),
    )
}

/// Delegated consent set used when `MICROSOFT_SCOPES` is unset. That env var,
/// when set, replaces this wholesale — `.env.example` must stay identical.
/// `User.Read.All` is admin-consent-required and is not retro-granted to a
/// tenant that already consented.
const DEFAULT_MICROSOFT_SCOPES: &str =
    "offline_access openid profile User.Read User.Read.All Mail.Read Calendars.Read \
Team.ReadBasic.All TeamMember.Read.All Channel.ReadBasic.All ChannelMessage.Read.All Chat.Read \
OnlineMeetings.ReadWrite OnlineMeetingTranscript.Read.All";

fn scopes() -> String {
    std::env::var("MICROSOFT_SCOPES").unwrap_or_else(|_| {
        // Outlook + calendar (CS-116/117) plus delegated Teams read (CS-119):
        // Team/Channel.ReadBasic for the picker, ChannelMessage.Read.All +
        // Chat.Read for backfill ingest. User.Read.All is the per-user
        // directory read for otherMails (plan item 23). The Azure app
        // registration must expose these for consent to succeed.
        DEFAULT_MICROSOFT_SCOPES.into()
    })
}

/// Exact `TeamMember.Read.All` grant. Microsoft reports either the short name
/// this app requests or the Graph resource URL; a longer name must not match.
pub(crate) fn scope_grants_team_member_read_all(scope: &str) -> bool {
    scope.split_whitespace().any(|token| {
        token == "TeamMember.Read.All" || token == "https://graph.microsoft.com/TeamMember.Read.All"
    })
}

fn authority_origin() -> String {
    #[cfg(feature = "e2e")]
    if let Ok(base) = std::env::var("E2E_MICROSOFT_AUTHORITY_BASE") {
        return base.trim_end_matches('/').to_owned();
    }
    "https://login.microsoftonline.com".to_owned()
}

fn authority(path: &str) -> String {
    format!(
        "{}/{}/oauth2/v2.0/{path}",
        authority_origin(),
        tenant()
    )
}

/// Authority for a specific tenant — app-only (client-credentials) tokens must
/// target the tenant where the Teams app was installed, not `common` (CS-121).
fn authority_for(tenant_id: &str, path: &str) -> String {
    format!("{}/{tenant_id}/oauth2/v2.0/{path}", authority_origin())
}

/// Test-only count of [`app_token`] calls. A chat roster read must stay at zero.
#[cfg(test)]
static APP_TOKEN_MINTS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// Acquire an **app-only** Graph token (client-credentials) for `tenant_id`. Used
/// by the Teams app's Resource-Specific Consent flow: the app reads only the
/// channels/chats/meetings an owner explicitly added it to — RSC scopes the app
/// token to those resources without org-wide admin consent (CS-121).
pub(crate) async fn app_token(
    http: &reqwest::Client,
    tenant_id: &str,
) -> Result<String, ConnectorError> {
    #[cfg(test)]
    APP_TOKEN_MINTS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let resp = http
        .post(authority_for(tenant_id, "token"))
        .form(&[
            ("grant_type", "client_credentials"),
            ("client_id", &client_id()?),
            ("client_secret", &client_secret()?),
            ("scope", "https://graph.microsoft.com/.default"),
        ])
        .timeout(std::time::Duration::from_secs(20))
        .send()
        .await
        .map_err(|e| ConnectorError::Exchange(e.to_string()))?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(ConnectorError::upstream(status, format!(
            "app token failed: {status}: {}",
            body.chars().take(300).collect::<String>()
        )));
    }
    let parsed: MsTokenResponse = resp
        .json()
        .await
        .map_err(|e| ConnectorError::Exchange(format!("app token parse: {e}")))?;
    Ok(parsed.access_token)
}

// ── Bot Connector (outbound bot messaging) — CS-122 ──────────────────────────────

/// A separately registered bot must use its own credentials and authentication
/// tenant. The customer's conversation tenant is not the bot's home tenant.
/// Graph/RSC and delegated OAuth continue using `MICROSOFT_CLIENT_*`.
struct BotConnectorConfig {
    client_id: String,
    client_secret: String,
    authority_tenant: String,
}

impl BotConnectorConfig {
    fn resolve(
        bot_id: Option<String>,
        bot_secret: Option<String>,
        bot_tenant: Option<String>,
        legacy_id: Option<String>,
        legacy_secret: Option<String>,
        conversation_tenant: &str,
    ) -> Result<Self, ConnectorError> {
        let configured = |value: Option<String>| value.filter(|v| !v.trim().is_empty());
        let missing = |name: &str| ConnectorError::NotConfigured(format!("{name} not configured"));
        if let Some(id) = configured(bot_id) {
            let secret =
                configured(bot_secret).ok_or_else(|| missing("MICROSOFT_BOT_APP_SECRET"))?;
            let tenant = configured(bot_tenant).ok_or_else(|| missing("MICROSOFT_BOT_TENANT"))?;
            // Only a home-tenant GUID or Microsoft's multitenant bot authority
            // is accepted; never interpret `common` as a client-credentials tenant.
            if tenant != "botframework.com" && Uuid::parse_str(&tenant).is_err() {
                return Err(ConnectorError::NotConfigured(
                    "MICROSOFT_BOT_TENANT must be the bot home-tenant GUID or botframework.com"
                        .into(),
                ));
            }
            return Ok(Self {
                client_id: id,
                client_secret: secret,
                authority_tenant: tenant,
            });
        }
        if configured(bot_secret).is_some() || configured(bot_tenant).is_some() {
            return Err(missing("MICROSOFT_BOT_APP_ID"));
        }
        // Preserve the existing shared-identity deployment when no separate
        // bot is configured. Never fall back to this identity for an explicit bot.
        Ok(Self {
            client_id: configured(legacy_id).ok_or_else(|| missing("MICROSOFT_CLIENT_ID"))?,
            client_secret: configured(legacy_secret)
                .ok_or_else(|| missing("MICROSOFT_CLIENT_SECRET"))?,
            authority_tenant: conversation_tenant.to_owned(),
        })
    }
}

/// Acquire a Bot Framework token using the registered bot's identity. A
/// single-tenant bot uses its home tenant; a multitenant bot uses botframework.com.
pub(crate) async fn bot_connector_token(
    http: &reqwest::Client,
    tenant_id: &str,
) -> Result<String, ConnectorError> {
    let config = BotConnectorConfig::resolve(
        std::env::var("MICROSOFT_BOT_APP_ID").ok(),
        std::env::var("MICROSOFT_BOT_APP_SECRET").ok(),
        std::env::var("MICROSOFT_BOT_TENANT").ok(),
        std::env::var("MICROSOFT_CLIENT_ID").ok(),
        std::env::var("MICROSOFT_CLIENT_SECRET").ok(),
        tenant_id,
    )?;
    exchange_bot_connector_token(
        http,
        &authority_for(&config.authority_tenant, "token"),
        &config,
    )
    .await
}

async fn exchange_bot_connector_token(
    http: &reqwest::Client,
    token_url: &str,
    config: &BotConnectorConfig,
) -> Result<String, ConnectorError> {
    let resp = http
        .post(token_url)
        .form(&[
            ("grant_type", "client_credentials"),
            ("client_id", config.client_id.as_str()),
            ("client_secret", config.client_secret.as_str()),
            ("scope", "https://api.botframework.com/.default"),
        ])
        .timeout(std::time::Duration::from_secs(20))
        .send()
        .await
        .map_err(|e| ConnectorError::Exchange(e.to_string()))?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(ConnectorError::upstream(status, format!(
            "bot connector token failed: {status}: {}",
            body.chars().take(300).collect::<String>()
        )));
    }
    let parsed: MsTokenResponse = resp
        .json()
        .await
        .map_err(|e| ConnectorError::Exchange(format!("bot token parse: {e}")))?;
    Ok(parsed.access_token)
}

#[cfg(test)]
mod bot_connector_identity_tests {
    use super::*;

    const HOME: &str = "84c4ead0-dfe0-4770-9d9f-b819c4cc0122";
    const CUSTOMER: &str = "42ca969d-908a-4dfe-bcc5-43d7c99e3ce9";

    fn separate(tenant: &str) -> Result<BotConnectorConfig, ConnectorError> {
        BotConnectorConfig::resolve(
            Some("production-bot-id".into()),
            Some("bot-only-secret".into()),
            Some(tenant.into()),
            Some("existing-oauth-id".into()),
            Some("oauth-only-secret".into()),
            CUSTOMER,
        )
    }

    #[test]
    fn explicit_bot_never_falls_back_to_oauth_secret_or_customer_tenant() {
        for (secret, tenant) in [(None, Some(HOME.into())), (Some("bot-secret".into()), None)] {
            assert!(BotConnectorConfig::resolve(
                Some("new-bot".into()),
                secret,
                tenant,
                Some("oauth-id".into()),
                Some("oauth-secret".into()),
                CUSTOMER,
            )
            .is_err());
        }
        assert!(separate("common").is_err());
        assert!(separate("organizations").is_err());
        assert!(separate("evil.example/path").is_err());
    }

    #[test]
    fn absent_separate_bot_preserves_existing_identity() {
        let config = BotConnectorConfig::resolve(
            None,
            None,
            None,
            Some("oauth-id".into()),
            Some("oauth-secret".into()),
            CUSTOMER,
        )
        .unwrap();
        assert_eq!(config.client_id, "oauth-id");
        assert_eq!(config.client_secret, "oauth-secret");
        assert_eq!(config.authority_tenant, CUSTOMER);
        assert!(BotConnectorConfig::resolve(
            None,
            Some("orphan-secret".into()),
            None,
            Some("oauth-id".into()),
            Some("oauth-secret".into()),
            CUSTOMER
        )
        .is_err());
    }

    async fn assert_exchange(tenant: &str) {
        use axum::{extract::Form, http::Uri, routing::post, Json, Router};
        use std::collections::HashMap;
        let (sent, received) = tokio::sync::oneshot::channel();
        let sent = std::sync::Arc::new(std::sync::Mutex::new(Some(sent)));
        let app = Router::new().fallback(post(
            move |uri: Uri, Form(form): Form<HashMap<String, String>>| {
                let sent = sent.clone();
                async move {
                    sent.lock()
                        .unwrap()
                        .take()
                        .unwrap()
                        .send((uri.path().to_owned(), form))
                        .unwrap();
                    Json(serde_json::json!({"access_token":"mock-bot-token"}))
                }
            },
        ));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let config = separate(tenant).unwrap();
        let authority = authority_for(&config.authority_tenant, "token");
        assert_eq!(
            authority,
            format!("https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token")
        );
        assert!(!authority.contains(CUSTOMER));
        let local_authority = authority.replacen("https://login.microsoftonline.com", &endpoint, 1);
        let token =
            exchange_bot_connector_token(&reqwest::Client::new(), &local_authority, &config)
                .await
                .unwrap();
        assert_eq!(token, "mock-bot-token");
        let (path, form) = received.await.unwrap();
        assert_eq!(path, format!("/{tenant}/oauth2/v2.0/token"));
        assert_eq!(form.get("client_id").unwrap(), "production-bot-id");
        assert_eq!(form.get("client_secret").unwrap(), "bot-only-secret");
        assert_eq!(form.get("grant_type").unwrap(), "client_credentials");
        assert_eq!(
            form.get("scope").unwrap(),
            "https://api.botframework.com/.default"
        );
        assert_eq!(form.len(), 4);
        server.abort();
    }

    #[tokio::test]
    async fn single_tenant_bot_exchange_uses_home_not_customer_authority() {
        assert_exchange(HOME).await;
    }

    #[tokio::test]
    async fn multitenant_bot_exchange_uses_bot_framework_authority() {
        assert_exchange("botframework.com").await;
    }
}

/// Post a Bot Framework activity into a conversation, returning the new
/// activity's id (used to later update the card in place). `service_url` comes
/// from the inbound activity that triggered this.
pub(crate) async fn send_activity(
    http: &reqwest::Client,
    bot_token: &str,
    service_url: &str,
    conversation_id: &str,
    activity: &serde_json::Value,
) -> Result<Option<String>, ConnectorError> {
    let url = format!(
        "{}/v3/conversations/{}/activities",
        service_url.trim_end_matches('/'),
        urlencoding::encode(conversation_id)
    );
    let resp = http
        .post(&url)
        .bearer_auth(bot_token)
        .json(activity)
        .timeout(std::time::Duration::from_secs(20))
        .send()
        .await
        .map_err(|e| ConnectorError::Internal(format!("send_activity: {e}")))?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(ConnectorError::Internal(format!(
            "send_activity {status}: {}",
            body.chars().take(300).collect::<String>()
        )));
    }
    let v: serde_json::Value = resp.json().await.unwrap_or_default();
    Ok(v.get("id").and_then(|i| i.as_str()).map(str::to_string))
}

/// Update (replace) a previously-sent activity — mutates the prompt card in
/// place ("⏳ Ingesting…" → "✅ …").
pub(crate) async fn update_activity(
    http: &reqwest::Client,
    bot_token: &str,
    service_url: &str,
    conversation_id: &str,
    activity_id: &str,
    activity: &serde_json::Value,
) -> Result<(), ConnectorError> {
    let url = format!(
        "{}/v3/conversations/{}/activities/{}",
        service_url.trim_end_matches('/'),
        urlencoding::encode(conversation_id),
        urlencoding::encode(activity_id)
    );
    let resp = http
        .put(&url)
        .bearer_auth(bot_token)
        .json(activity)
        .timeout(std::time::Duration::from_secs(20))
        .send()
        .await
        .map_err(|e| ConnectorError::Internal(format!("update_activity: {e}")))?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(ConnectorError::Internal(format!(
            "update_activity {status}: {}",
            body.chars().take(300).collect::<String>()
        )));
    }
    Ok(())
}

// ── Token endpoint response ────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct MsTokenResponse {
    access_token: String,
    refresh_token: Option<String>,
    #[serde(default)]
    id_token: Option<String>,
    #[serde(default)]
    scope: Option<String>,
    /// Seconds until `access_token` expires.
    #[serde(default)]
    expires_in: Option<i64>,
}

// ── Token endpoint ERROR response (CS-453 credential-death classification) ────

/// The Microsoft identity platform's OAuth error shape
/// (https://learn.microsoft.com/en-us/entra/identity-platform/reference-error-codes),
/// returned with a non-2xx status from the `/token` endpoint. `error` is the
/// OAuth2 error class (`"invalid_grant"`, `"invalid_client"`, …); `error_codes`
/// is Microsoft's own more specific AADSTS numeric codes — zero or more can
/// appear (`error_description` also embeds the first one as free text, e.g.
/// `"AADSTS50173: ..."`, but the structured array is what
/// [`classify_microsoft_token_error`] matches on).
#[derive(Debug, Deserialize, Default)]
struct MsTokenErrorResponse {
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    error_codes: Vec<i64>,
}

/// Classify a Microsoft `/token` endpoint failure body into a
/// [`CredentialDeathReason`] — but ONLY from a DEFINITE dead-credential
/// signal, never a guess. Returns `None` for everything else, which the
/// caller then reports as the pre-existing [`ConnectorError::Exchange`]
/// (network error, malformed body, throttling, an `invalid_grant` we don't
/// specifically recognize, or any other OAuth error class).
///
/// The gate is `error == "invalid_grant"` — Microsoft's OAuth2 error CLASS
/// for "this grant/token can no longer be exchanged" — AND one of a short,
/// deliberately narrow allowlist of `error_codes` entries that Microsoft's
/// own reference documents as meaning specifically consent-revoked,
/// account-disabled/deleted, a forced re-auth from a password/credential
/// change, or a refresh token expired from inactivity (70008/700082 below)
/// — never "needs MFA", "throttled", or any other interrupt that a retry /
/// an interactive prompt the user was going to see anyway can resolve:
///
///   * **50173** — "The provided grant has expired due to it being revoked,
///     a fresh auth token is needed. The user might have changed or reset
///     their password, or an admin might have reset it." → forced re-auth
///     from a password/credential change.
///   * **50057** — "The user account is disabled. The account has been
///     disabled by an administrator." → account disabled.
///   * **50034** — "The user account {identifier} does not exist in the
///     {tenant} directory." → the user was deleted (or never existed there).
///   * **65001** — "The user or administrator has not consented to use the
///     application." Ambiguous in general (an app's very FIRST token
///     request can also see this, meaning "never consented" rather than
///     "revoked") — but this function is only ever called from a token
///     REFRESH, where a working grant necessarily existed before, so seeing
///     it here specifically means consent was revoked since that grant was
///     issued.
///
///   * **70008** (`ExpiredOrRevokedGrant`) and **700082**
///     (`ExpiredOrRevokedGrantInactiveToken`) — the refresh token expired
///     from inactivity (Microsoft's sliding window, 90 days by default). Not
///     one of CS-453's three original categories and left as `Exchange` at
///     first, but the sweeps that now retry every connection on a schedule
///     (`workers::microsoft_calendar_sync`, `graph_subscriptions`) turned
///     that into a permanent `warn!` every tick with nothing to show the
///     user: an inactive-expired refresh token can never be exchanged again
///     — only a fresh interactive sign-in mints a new one — which is exactly
///     the "reconnect to resume" state `credential_invalid_at` records.
///     Two distinct reason codes so the stored reason says which one
///     Microsoft answered.
///
/// Deliberately left OUT, after checking Microsoft's own reference list —
/// listed here rather than silently omitted, per this story's own brief:
///   * **50055** (password expired), **50076**/**50079** (MFA required/
///     interrupt), **50131** (conditional access denied), **53003**
///     (blocked by conditional access) — all real interrupts a user must
///     act on, but none is "the credential is dead"; several are
///     resolvable without any admin/member action from OUR side (e.g. MFA
///     completes on the user's very next interactive sign-in), so treating
///     them as revoked would be the "could not ask ≠ does not work" mistake
///     D16 already names for `connector_health` (migration `0243`'s
///     header).
pub(crate) fn classify_microsoft_token_error(
    error: Option<&str>,
    error_codes: &[i64],
) -> Option<CredentialDeathReason> {
    if error != Some("invalid_grant") {
        return None;
    }
    error_codes.iter().find_map(|code| match code {
        50173 => Some(CredentialDeathReason::MicrosoftPasswordChangedOrAdminReset),
        50057 => Some(CredentialDeathReason::MicrosoftAccountDisabled),
        50034 => Some(CredentialDeathReason::MicrosoftAccountDeleted),
        65001 => Some(CredentialDeathReason::MicrosoftConsentRevoked),
        70008 => Some(CredentialDeathReason::MicrosoftGrantExpired),
        700082 => Some(CredentialDeathReason::MicrosoftRefreshTokenInactive),
        _ => None,
    })
}

/// Claims we read out of the (already-trusted, first-party) `id_token`. We do not
/// verify the signature — the token came straight from Microsoft's token endpoint
/// over TLS in direct response to our client-secret-authenticated request, so it
/// is only used to label the connection (tenant + user), never for authz.
#[derive(Debug, Deserialize)]
struct IdTokenClaims {
    /// Tenant id → `provider_workspace_id`.
    tid: Option<String>,
    /// Object (user) id → `provider_user_id`.
    oid: Option<String>,
    preferred_username: Option<String>,
    name: Option<String>,
}

/// Decode the middle (payload) segment of a JWT without signature verification.
fn decode_id_token_claims(id_token: &str) -> Option<IdTokenClaims> {
    let payload = id_token.split('.').nth(1)?;
    // JWT uses base64url without padding.
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload)
        .ok()?;
    serde_json::from_slice(&bytes).ok()
}

fn token_response_to_bundle(r: MsTokenResponse) -> TokenBundle {
    let claims = r
        .id_token
        .as_deref()
        .and_then(decode_id_token_claims)
        .unwrap_or(IdTokenClaims {
            tid: None,
            oid: None,
            preferred_username: None,
            name: None,
        });

    let expires_at = r
        .expires_in
        .map(|secs| Utc::now() + Duration::seconds(secs));

    TokenBundle {
        access_token: r.access_token,
        refresh_token: r.refresh_token,
        user_token: None,
        scope: r.scope.unwrap_or_default(),
        expires_at,
        provider_workspace_id: claims.tid.unwrap_or_default(),
        provider_workspace_name: claims.name.or(claims.preferred_username),
        provider_user_id: claims.oid.unwrap_or_default(),
    }
}

impl MicrosoftConnector {
    /// Exchange a refresh token for a fresh access token (and possibly a rotated
    /// refresh token). Used by [`sync`](Self::sync) when the stored token is
    /// expired.
    async fn refresh(
        &self,
        http: &reqwest::Client,
        refresh_token: &str,
    ) -> Result<TokenBundle, ConnectorError> {
        let resp = http
            .post(authority("token"))
            .form(&[
                ("grant_type", "refresh_token"),
                ("client_id", &client_id()?),
                ("client_secret", &client_secret()?),
                ("refresh_token", refresh_token),
                ("scope", &scopes()),
            ])
            .send()
            .await
            .map_err(|e| ConnectorError::Exchange(e.to_string()))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            // CS-453: a non-2xx `/token` refresh response is where Microsoft's
            // AADSTS error codes actually surface — classify BEFORE collapsing
            // into the generic `Exchange` (a malformed/unparsable body, or an
            // `error`/`error_codes` pair this function doesn't specifically
            // recognize, still falls through to `Exchange` below unchanged).
            let err_body: MsTokenErrorResponse = serde_json::from_str(&body).unwrap_or_default();
            if let Some(reason) =
                classify_microsoft_token_error(err_body.error.as_deref(), &err_body.error_codes)
            {
                tracing::info!(
                    error_codes = ?err_body.error_codes,
                    reason = reason.code(),
                    "microsoft token refresh: classified as a dead credential"
                );
                return Err(ConnectorError::CredentialRevoked {
                    provider: "microsoft",
                    reason,
                });
            }
            return Err(ConnectorError::upstream(status, format!(
                "token refresh failed: {status}: {}",
                body.chars().take(300).collect::<String>()
            )));
        }

        let parsed: MsTokenResponse = resp
            .json()
            .await
            .map_err(|e| ConnectorError::Exchange(format!("refresh parse: {e}")))?;
        Ok(token_response_to_bundle(parsed))
    }
}

#[async_trait]
impl ConnectorProvider for MicrosoftConnector {
    fn id(&self) -> &'static str {
        "microsoft"
    }

    fn display_name(&self) -> &'static str {
        "Microsoft"
    }

    fn source_types(&self) -> &'static [&'static str] {
        &[
            "outlook",
            CALENDAR_SOURCE_TYPE,
            "teams_channel",
            "teams_chat",
            "teams_transcript",
        ]
    }

    /// Override the default (atoms-only) container derivation to also surface
    /// resources that are *watched* but have produced no atoms yet — Teams
    /// channels/chats (`teams_watched_resources`) and Outlook threads
    /// (`outlook_watched_conversations`). So "which Teams channels are you
    /// watching?" is answerable the moment a channel is subscribed, before its
    /// first message lands. Best-effort: a failing side query degrades to the
    /// atom-derived set.
    async fn describe_containers(
        &self,
        pool: &PgPool,
        account_id: Uuid,
        graph_id: Uuid,
    ) -> Vec<super::state::Container> {
        use super::state::Container;

        let mut containers =
            super::state::containers_from_atoms(pool, account_id, graph_id, self.source_types())
                .await;
        // Dedup against what atoms already surfaced (case-insensitive on label).
        let mut seen: HashSet<String> = containers.iter().map(|c| c.label.to_lowercase()).collect();

        // Both watched-resource side tables carry 0083 RLS policies — read
        // them under one `begin_account_scoped` transaction (org-tenancy
        // conversion). A scope failure degrades to the atom-derived set,
        // exactly like a failed side query always has.
        let mut tx = match crate::tenancy::scope::begin_account_scoped(pool, account_id).await {
            Ok(tx) => tx,
            Err(e) => {
                tracing::warn!(error = %e, %graph_id, "operational-state: watched-resources scoped tx failed");
                return containers;
            }
        };

        // Watched Teams channels/chats (incl. watched-but-0-atom).
        if let Ok(rows) = sqlx::query(
            "SELECT kind, display_name, channel_id, chat_id, last_ingested_at \
             FROM teams_watched_resources WHERE graph_id = $1",
        )
        .bind(graph_id)
        .fetch_all(&mut *tx)
        .await
        {
            for r in rows {
                let kind: String = r.try_get("kind").unwrap_or_else(|_| "channel".into());
                let display_name: String = r.try_get("display_name").unwrap_or_default();
                let chat_id: String = r.try_get("chat_id").unwrap_or_default();
                let channel_id: String = r.try_get("channel_id").unwrap_or_default();
                let label = if !display_name.trim().is_empty() {
                    display_name.trim().to_string()
                } else if !chat_id.is_empty() {
                    chat_id
                } else {
                    channel_id
                };
                if label.is_empty() || !seen.insert(label.to_lowercase()) {
                    continue;
                }
                containers.push(Container {
                    kind,
                    label,
                    last_ingested: r.try_get("last_ingested_at").ok(),
                });
            }
        } else {
            tracing::warn!(%graph_id, "operational-state: teams watched query failed");
        }

        // Watched Outlook threads.
        if let Ok(rows) = sqlx::query(
            "SELECT conversation_id, last_ingested_at \
             FROM outlook_watched_conversations WHERE graph_id = $1",
        )
        .bind(graph_id)
        .fetch_all(&mut *tx)
        .await
        {
            for r in rows {
                let cid: String = r.try_get("conversation_id").unwrap_or_default();
                if cid.is_empty() {
                    continue;
                }
                let label = format!("thread {}", cid.chars().take(8).collect::<String>());
                if !seen.insert(label.to_lowercase()) {
                    continue;
                }
                containers.push(Container {
                    kind: "thread".into(),
                    label,
                    last_ingested: r.try_get("last_ingested_at").ok(),
                });
            }
        } else {
            tracing::warn!(%graph_id, "operational-state: outlook watched query failed");
        }

        // Read-only — release the scoped connection explicitly rather than
        // leaning on `Transaction`'s drop-rollback.
        tx.rollback().await.ok();

        containers
    }

    fn authorize_url(&self, base_url: &str, state: &str) -> Result<String, ConnectorError> {
        let client_id = client_id()?;
        let redirect_uri = format!("{base_url}/v1/oauth/microsoft/callback");
        let url = format!(
            "{}?client_id={}&response_type=code&redirect_uri={}&response_mode=query&scope={}&state={}",
            authority("authorize"),
            urlencoding::encode(&client_id),
            urlencoding::encode(&redirect_uri),
            urlencoding::encode(&scopes()),
            urlencoding::encode(state),
        );
        Ok(url)
    }

    fn admin_consent_url(
        &self,
        base_url: &str,
        state: &str,
    ) -> Option<Result<String, ConnectorError>> {
        // Microsoft's tenant-wide admin consent is its OWN endpoint —
        // `/oauth2/v2.0/adminconsent` — not the standard `/authorize` flow
        // with a prompt override. The admin signs in, consents on behalf of
        // the whole organization, and is redirected back with
        // `admin_consent=True&tenant=<aad-tenant-guid>` and NO auth code
        // (`routes::oauth::handle_microsoft_admin_consent` is that redirect's
        // dedicated callback branch). After this, employees in the tenant
        // connect via the normal authorize flow without per-user consent
        // prompts — Microsoft suppresses them natively for a consented
        // scope set. The `adminconsent` endpoint does not accept the
        // `common` pseudo-tenant (there is no "any org's admin" — a specific
        // organization consents), so `common` maps to `organizations`; a
        // configured real tenant id/domain is used as-is.
        let client_id = match client_id() {
            Ok(c) => c,
            Err(e) => return Some(Err(e)),
        };
        let redirect_uri = format!("{base_url}/v1/oauth/microsoft/callback");
        Some(Ok(build_admin_consent_url(
            &tenant(),
            &client_id,
            &redirect_uri,
            state,
        )))
    }

    async fn exchange_code(
        &self,
        http: &reqwest::Client,
        base_url: &str,
        code: &str,
    ) -> Result<TokenBundle, ConnectorError> {
        let client_id = client_id()?;
        let client_secret = client_secret()?;
        let redirect_uri = format!("{base_url}/v1/oauth/microsoft/callback");

        let resp = http
            .post(authority("token"))
            .form(&[
                ("grant_type", "authorization_code"),
                ("client_id", &client_id),
                ("client_secret", &client_secret),
                ("code", code),
                ("redirect_uri", &redirect_uri),
                ("scope", &scopes()),
            ])
            .send()
            .await
            .map_err(|e| ConnectorError::Exchange(e.to_string()))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(ConnectorError::upstream(status, format!(
                "code exchange failed: {status}: {}",
                body.chars().take(300).collect::<String>()
            )));
        }

        let parsed: MsTokenResponse = resp
            .json()
            .await
            .map_err(|e| ConnectorError::Exchange(format!("token parse: {e}")))?;
        Ok(token_response_to_bundle(parsed))
    }

    /// `true` for the route's gate only: the Microsoft calendar pull does
    /// not run through the trait `sync()` (its "fetch, commit the cursor,
    /// hand the items back" shape commits before the caller ingests).
    /// `routes::connectors::trigger_sync` dispatches Microsoft on both
    /// planes to `workers::microsoft_calendar_sync::sync_member` /
    /// `sync_personal_account` — the same locked, commit-after-ingest pass
    /// the 30-minute sweep runs.
    fn supports_sync(&self) -> bool {
        true
    }

    fn supports_fetch_items(&self) -> bool {
        true
    }

    async fn fetch_items(
        &self,
        http: &reqwest::Client,
        creds: &mut ConnectorCredentials,
        item_ids: &[String],
        scope: IngestScope,
    ) -> Result<Vec<NormalizedItem>, ConnectorError> {
        self.refresh_if_needed(http, creds).await?;
        // The user opts in on *one* message but means the whole thread (CS-117):
        // for each opened message we fetch it, then expand to every message in its
        // conversation so the snapshot is complete. Dedup by message id (the opened
        // message also appears in its own conversation) and by conversation id (two
        // opened messages may share a thread) so we never fetch or emit twice.
        //
        // `scope` (gmail-options backend train, PR 1 of 4): [`IngestScope::
        // Message`] skips the `fetch_conversation` expansion below entirely —
        // only the named item id(s), never the rest of the conversation, are
        // fetched. [`IngestScope::Thread`] (the default) is this loop's
        // original, only behavior.
        let mut seen_conversations: HashSet<String> = HashSet::new();
        let mut seen_messages: HashSet<String> = HashSet::new();
        let mut items = Vec::new();
        for id in item_ids {
            let msg = fetch_message(http, &creds.access_token, id).await?;
            let conversation_id = msg.conversation_id.clone();
            let msg_id = msg.id.clone();
            if seen_messages.insert(msg_id) {
                items.push(message_to_item(msg));
            }
            if scope != IngestScope::Thread {
                continue;
            }
            if let Some(cid) = conversation_id {
                if seen_conversations.insert(cid.clone()) {
                    // `resume_cursor: None` — this trait method has no
                    // account_id/pool to persist a cursor against (the
                    // `ConnectorProvider` trait is shared across every
                    // connector's opt-in-item path, not Microsoft-specific);
                    // `after_ingest` (which DOES get `pool`/`account_id`)
                    // corrects this conversation's `outlook_watched_
                    // conversations.backfill_complete` to the real value
                    // right after this call returns — see its own comment.
                    let page = fetch_conversation(http, &creds.access_token, &cid, None).await?;
                    if !page.complete {
                        // Known, documented gap (not silent — this IS the
                        // explicit signal): a >1000-message thread opted into
                        // via the generic item-ingest path is not currently
                        // resumed by the tenant-loop continuation tick, since
                        // `after_ingest` (the only place downstream of this
                        // call with an account_id/pool to persist a cursor
                        // against) has no way to learn `page.complete` back
                        // from this trait method — see `after_ingest`'s own
                        // comment. Every OTHER opt-in path in this connector
                        // (Teams channel/chat, workflow-matched mail, the
                        // live webhook's own thread expansion) IS fully
                        // resumable. Pathological in practice for email.
                        tracing::warn!(
                            conversation_id = %cid,
                            budget = MAX_CONVERSATION_MESSAGES,
                            "fetch_items: conversation exceeds per-call budget — ingesting \
                             capped prefix; NOT currently resumed by the continuation tick \
                             (known gap, see this call site's comment)"
                        );
                    }
                    for m in page.items {
                        if seen_messages.insert(m.id.clone()) {
                            items.push(message_to_item(m));
                        }
                    }
                }
            }
        }
        Ok(items)
    }

    async fn after_ingest(
        &self,
        http: &reqwest::Client,
        pool: &PgPool,
        account_id: Uuid,
        user_id: Uuid,
        graph_id: Uuid,
        owner: TokenOwner,
        creds: &ConnectorCredentials,
        items: &[NormalizedItem],
        control_pool: Option<&PgPool>,
        follow: bool,
        follow_since: Option<DateTime<Utc>>,
    ) -> Result<(), ConnectorError> {
        // `control_pool`: used only to publish the org-plane mail route
        // (`sync_mail_routes`) after the subscription is ensured.
        // Register every conversation we just ingested so future replies on it are
        // auto-captured, then make sure a live inbox subscription is feeding the
        // webhook. Both are idempotent (re-ingesting an already-watched thread is a
        // no-op upsert).
        //
        // `follow` (gmail-options backend train, PR 1 of 4): written onto
        // every upserted row's `follow_replies` column, unconditionally — a
        // watched row is written for EVERY opt-in save regardless of
        // `follow`'s value (locked decision, see `routes::connectors::
        // ingest_selected`'s own doc); only whether it goes on to drive
        // live-follow (`watched_conversation_graph`, `watched_conversations_
        // for_account`) depends on this value.
        let conversations: HashSet<&str> = items
            .iter()
            .filter_map(|i| i.thread_id.as_deref())
            .collect();
        if conversations.is_empty() {
            return Ok(());
        }
        for conversation_id in conversations {
            // Commit follow preferences, message-scope boundary and completed
            // initial snapshot in one transaction. A crash must never leave
            // message scope as a pending whole-conversation backfill.
            upsert_watched_conversation_for_save(
                pool,
                account_id,
                user_id,
                graph_id,
                conversation_id,
                follow,
                follow_since,
            )
            .await?;
        }
        // `owner` (m365 enterprise train PR 3.3) — the SAME `TokenOwner` the
        // credentials this `creds.access_token` came from were actually
        // loaded/persisted under (`routes::connectors::finish_ingest`'s own
        // `owner` parameter, itself `load_creds_owner_aware`'s result): on the
        // Personal plane, always `AccountWide` (byte-identical to before this
        // parameter existed — one mailbox per account, `owner_user_id IS
        // NULL`). On the org plane, once reachable, always `Member(uid)` —
        // `resolve_microsoft_token_target` never writes an org-plane row any
        // other way — so this is the mailbox `/me` actually resolves to for
        // THIS token, not a guess. See `graph_subscriptions.owner_user_id`'s
        // own migration header (`0190`/`0068`) for why a bare account-wide
        // subscription row can't represent two org members' mailboxes at
        // once.
        ensure_mail_subscriptions(http, pool, account_id, &creds.access_token, owner).await?;
        // ORG plane: the subscription row was just written to the org's tenant
        // DB, which the notification webhook cannot search. Publish the
        // control-plane pointer so replies on this thread are routable. A
        // failure must not fail the save; the renewal tick re-syncs routes.
        if let Some(control) = control_pool {
            if matches!(
                crate::routes::slack::account_plane(control, account_id).await,
                Ok(crate::routes::slack::AccountPlane::Org)
            ) {
                if let Err(e) = sync_mail_routes(pool, control, account_id).await {
                    tracing::warn!(error = %e, %account_id, "failed to publish graph mail routes");
                }
            }
        }
        Ok(())
    }
}

impl MicrosoftConnector {
    /// Refresh the access token in-place if it is expired (or within 60s of it),
    /// recording the new bundle on `creds.refreshed` so the caller can persist it.
    pub(crate) async fn refresh_if_needed(
        &self,
        http: &reqwest::Client,
        creds: &mut ConnectorCredentials,
    ) -> Result<(), ConnectorError> {
        let needs_refresh = creds
            .expires_at
            .map(|exp| exp <= Utc::now() + Duration::seconds(60))
            .unwrap_or(false);
        if needs_refresh {
            if let Some(refresh_token) = creds.refresh_token.clone() {
                let bundle = self.refresh(http, &refresh_token).await?;
                creds.access_token = bundle.access_token.clone();
                creds.expires_at = bundle.expires_at;
                creds.refreshed = Some(bundle);
            }
        }
        Ok(())
    }
}

// ── Graph fetchers ──────────────────────────────────────────────────────────────

pub(super) const GRAPH: &str = "https://graph.microsoft.com/v1.0";

/// Send one Graph HTTP request through the shared outbound rate limiter
/// (`crate::graph_rate_limit`), honoring a `Retry-After` response with a
/// single bounded retry — the behavior that module's doc comment names as
/// the thing to NOT do (Slack's paginators fail outright on 429). `build` is
/// invoked once per attempt (never more than twice) rather than the request
/// being cloned, sidestepping `reqwest::RequestBuilder`'s partial `Clone`
/// support. Every raw Graph call site in this module (GET/POST/PATCH/DELETE)
/// goes through this — see `graph_get` below for the common GET wrapper.
pub(super) async fn graph_send(
    build: impl Fn() -> reqwest::RequestBuilder,
) -> Result<reqwest::Response, ConnectorError> {
    let key = crate::graph_rate_limit::current_key()
        .unwrap_or_else(|| crate::graph_rate_limit::UNSCOPED_KEY.to_string());
    let limiter = crate::graph_rate_limit::shared();

    for attempt in 0..2u8 {
        let _permit = limiter
            .acquire(&key)
            .await
            .map_err(|e| ConnectorError::Exchange(format!("graph rate limiter: {e}")))?;
        let request = build().timeout(std::time::Duration::from_secs(30));
        #[cfg(feature = "e2e")]
        let request = e2e_graph_request(request)?;
        let resp = request.send()
            .await
            .map_err(|e| ConnectorError::Exchange(e.to_string()))?;
        let status = resp.status();
        if attempt == 0
            && (status == reqwest::StatusCode::TOO_MANY_REQUESTS
                || status == reqwest::StatusCode::SERVICE_UNAVAILABLE)
        {
            let retry_after = parse_retry_after(resp.headers());
            tracing::info!(
                %key,
                ?retry_after,
                %status,
                "graph throttled; honoring Retry-After and retrying once"
            );
            limiter.penalize(&key, retry_after).await;
            continue;
        }
        return Ok(resp);
    }
    unreachable!("loop always returns on its second iteration")
}

// Rewrite only authenticated Graph transport in the E2E binary. Pagination
// still carries real Graph URLs, so production next-link validation is exercised.
#[cfg(feature = "e2e")]
fn e2e_graph_request(
    request: reqwest::RequestBuilder,
) -> Result<reqwest::RequestBuilder, ConnectorError> {
    let Ok(base) = std::env::var("E2E_MICROSOFT_GRAPH_BASE") else {
        return Ok(request);
    };
    let (client, request) = request.build_split();
    let mut request = request.map_err(|e| ConnectorError::Exchange(e.to_string()))?;
    if request.url().scheme() == "https" && request.url().host_str() == Some("graph.microsoft.com") {
        let mut target =
            reqwest::Url::parse(&base).map_err(|e| ConnectorError::Exchange(e.to_string()))?;
        target.set_path(&format!(
            "{}{}",
            target.path().trim_end_matches('/'),
            request.url().path()
        ));
        target.set_query(request.url().query());
        *request.url_mut() = target;
    }
    Ok(reqwest::RequestBuilder::from_parts(client, request))
}

/// Parse a `Retry-After` header (seconds, per RFC 9110 — Graph does not use
/// the HTTP-date form) into a [`std::time::Duration`], falling back to a
/// fixed default when the header is absent or unparsable.
fn parse_retry_after(headers: &reqwest::header::HeaderMap) -> std::time::Duration {
    headers
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.trim().parse::<u64>().ok())
        .map(std::time::Duration::from_secs)
        .unwrap_or(crate::graph_rate_limit::DEFAULT_RETRY_AFTER)
}

async fn graph_get(
    http: &reqwest::Client,
    access_token: &str,
    url: &str,
) -> Result<serde_json::Value, ConnectorError> {
    let resp = graph_send(|| http.get(url).bearer_auth(access_token)).await?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(ConnectorError::upstream(status, format!(
            "graph GET {url} failed: {status}: {}",
            body.chars().take(300).collect::<String>()
        )));
    }
    resp.json()
        .await
        .map_err(|e| ConnectorError::Exchange(format!("graph parse: {e}")))
}

#[derive(Debug, Deserialize)]
struct EmailAddress {
    name: Option<String>,
    address: Option<String>,
}

#[derive(Debug, Deserialize)]
struct Recipient {
    #[serde(rename = "emailAddress")]
    email_address: Option<EmailAddress>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct GraphMessage {
    id: String,
    subject: Option<String>,
    from: Option<Recipient>,
    #[serde(rename = "toRecipients", default)]
    to_recipients: Vec<Recipient>,
    #[serde(rename = "ccRecipients", default)]
    cc_recipients: Vec<Recipient>,
    #[serde(rename = "receivedDateTime")]
    received_date_time: Option<DateTime<Utc>>,
    #[serde(rename = "bodyPreview")]
    body_preview: Option<String>,
    /// Full message body (HTML or text). Ingested in preference to the truncated
    /// `bodyPreview` so corals carry the whole email (CS-120).
    #[serde(default)]
    body: Option<GraphItemBody>,
    /// Stable thread id shared by every message in the conversation. Used to
    /// expand a single opened message into the whole thread and to match
    /// inbound change-notifications to watched threads (CS-117).
    #[serde(rename = "conversationId")]
    conversation_id: Option<String>,
    // Fields below feed workflow matching (CS-118).
    #[serde(default)]
    categories: Vec<String>,
    #[serde(rename = "hasAttachments")]
    has_attachments: Option<bool>,
    #[serde(rename = "parentFolderId")]
    parent_folder_id: Option<String>,
}

/// Message facts a workflow filter matches against, extracted from a
/// [`GraphMessage`]. Provider-neutral so the matcher (`routes::workflows`) needs
/// no Graph types. Addresses are raw; the matcher compares case-insensitively.
pub(crate) struct MatchInput {
    pub from: Option<String>,
    pub recipients: Vec<String>,
    pub subject: String,
    pub categories: Vec<String>,
    pub has_attachments: bool,
    /// Well-known folder name (e.g. `inbox`, `sentitems`) when known, else None.
    pub folder: Option<String>,
}

impl GraphMessage {
    /// Extract the fields a workflow filter matches on. `folder` is the message's
    /// well-known folder name when resolvable (from the firing subscription's
    /// resource, or a backfill folder-id map).
    pub(crate) fn match_input(&self, folder: Option<String>) -> MatchInput {
        let addr = |r: &Recipient| r.email_address.as_ref().and_then(|e| e.address.clone());
        MatchInput {
            from: self.from.as_ref().and_then(addr),
            recipients: self
                .to_recipients
                .iter()
                .chain(self.cc_recipients.iter())
                .filter_map(addr)
                .collect(),
            subject: self.subject.clone().unwrap_or_default(),
            categories: self.categories.clone(),
            has_attachments: self.has_attachments.unwrap_or(false),
            folder,
        }
    }

    /// The message's conversation id, if present.
    pub(crate) fn conversation_id(&self) -> Option<&str> {
        self.conversation_id.as_deref()
    }

    /// A message-only save captures future replies, never historical mail
    /// surfaced by a delayed notification or a recovery re-list. NULL is
    /// the legacy whole-thread watch; unknown dates on a bounded watch do
    /// not authorize importing potentially pre-save history.
    pub(crate) fn is_future_reply(&self, since: Option<DateTime<Utc>>) -> bool {
        since.is_none_or(|boundary| self.received_date_time.is_some_and(|date| date >= boundary))
    }

    /// The message's parent folder id (for backfill folder resolution).
    pub(crate) fn parent_folder_id(&self) -> Option<&str> {
        self.parent_folder_id.as_deref()
    }
}

#[derive(Debug, Deserialize)]
struct GraphList<T> {
    #[serde(default = "Vec::new")]
    value: Vec<T>,
    /// Graph's pagination cursor — present when more results remain. Followed
    /// to fetch the *whole* conversation, not just the first page.
    #[serde(rename = "@odata.nextLink")]
    next_link: Option<String>,
}

fn recipient_label(r: &Recipient) -> Option<String> {
    let ea = r.email_address.as_ref()?;
    ea.name.clone().or_else(|| ea.address.clone())
}

/// Map a single Outlook message to a coral. `source_type="outlook"` routes to the
/// email chunker. Used by [`MicrosoftConnector::fetch_items`] for the explicit,
/// per-thread opt-in ingest (CS-116) and by the change-notification webhook for
/// auto-captured replies (CS-117). `thread_id` carries the `conversationId` so the
/// ingest route can register the thread for auto-capture.
///
/// Uses [`body_to_text_boundaried`] (not [`body_to_text`]) to convert the
/// HTML body — m365 PR 2.0, CodeRabbit finding F2 on #691: the reply-chain
/// trim below is a line-by-line scan, and Outlook's own reply markers are
/// always their own `<div>`; without boundary preservation those markers
/// get flattened into the middle of a collapsed single line and can never
/// match. See `body_to_text_boundaried`'s and
/// `email_body::from_graph_body_boundaried`'s docs for the full reasoning.
/// This deliberately changes Outlook mail's atom text shape (adds
/// newlines at paragraph/div/br boundaries where the old conversion
/// collapsed them) — an accepted, deliberate change for this PR, not a
/// regression: the fixture pinning test for this function
/// (`fixture_outlook_message_shapes_subject_body_participants_and_thread`)
/// was updated to the new expected shape.
pub(crate) fn message_to_item(m: GraphMessage) -> NormalizedItem {
    let subject = m.subject.unwrap_or_else(|| "(no subject)".into());
    // Prefer the full body; fall back to the truncated preview if it's absent.
    let body = m
        .body
        .as_ref()
        .map(body_to_text_boundaried)
        .map(|t| t.trim().to_string())
        .filter(|t| !t.is_empty())
        .unwrap_or_else(|| m.body_preview.clone().unwrap_or_default());
    // Outlook-tuned quoted-reply-chain/signature trimming (m365 PR 2.0 —
    // closes the plan's original "quoted-reply-chain trimming for mail"
    // spec item; Teams messages are never trimmed, see
    // `email_body::trim_outlook_reply_chain`'s own doc). Conservative by
    // construction: never reduces a body to nothing.
    let body = super::email_body::trim_outlook_reply_chain(&body);
    let mut participants = Vec::new();
    if let Some(from) = m.from.as_ref().and_then(recipient_label) {
        participants.push(from);
    }
    participants.extend(m.to_recipients.iter().filter_map(recipient_label));

    NormalizedItem {
        source_type: "outlook".into(),
        source_id: m.id,
        content: format!("{subject}\n\n{body}"),
        participants,
        timestamp: m.received_date_time.unwrap_or_else(Utc::now),
        scope: None,
        thread_id: m.conversation_id,
        channel_id: None,
        author_id: None,
        author_name: None,
    }
}

#[derive(Debug, Deserialize)]
struct GraphDateTime {
    #[serde(rename = "dateTime")]
    date_time: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GraphAttendee {
    #[serde(rename = "emailAddress")]
    email_address: Option<EmailAddress>,
}

/// One Graph `event` resource, as returned by `GET /me/events/{id}` (with the
/// `$select` [`EVENT_SELECT`]) and by `calendarView/delta` (which returns
/// the same properties a `GET /calendarView` does — `body` included — and
/// does not support `$select`; unknown fields are ignored on decode, and
/// [`shape_graph_event`] renders only the selected ones, so the two sources
/// hash identically for an unchanged event).
#[derive(Debug, Deserialize)]
pub(crate) struct GraphEvent {
    pub(crate) id: String,
    subject: Option<String>,
    organizer: Option<Recipient>,
    #[serde(default)]
    attendees: Vec<GraphAttendee>,
    start: Option<GraphDateTime>,
    end: Option<GraphDateTime>,
    location: Option<GraphLocation>,
    #[serde(rename = "bodyPreview")]
    body_preview: Option<String>,
    /// The whole description. `bodyPreview` is Graph's ~255-character
    /// plain-text excerpt, so an edit to an agenda beyond that point left
    /// the shaped content — and its hash — unchanged and the coral stale.
    /// HTML by default from both endpoints (no `Prefer:
    /// outlook.body-content-type` is sent: it is not documented for the
    /// delta function, and the shaped text must not depend on which
    /// endpoint produced the event), converted locally by
    /// [`body_to_text_boundaried`].
    #[serde(default)]
    body: Option<GraphItemBody>,
    /// `true` once the organiser cancelled the meeting — the attendee's copy
    /// lingers as "Canceled: …" until they remove it, but it is no longer a
    /// meeting; treated exactly like a deleted event by
    /// `connectors::microsoft_calendar`.
    #[serde(rename = "isCancelled")]
    pub(crate) is_cancelled: Option<bool>,
    /// For an `occurrence`/`exception` (what `calendarView` expands a
    /// recurring series into): the id of the series master, which is the
    /// event this codebase stores ONE coral for (same model as Google's
    /// `singleEvents=false` listing — see `connectors::microsoft_calendar`'s
    /// module doc, "Recurring series"). Absent/null on a `singleInstance`
    /// and on the master itself.
    #[serde(rename = "seriesMasterId")]
    pub(crate) series_master_id: Option<String>,
}

/// The `$select` for a single-event fetch — the fields [`shape_graph_event`]
/// renders plus the two `connectors::microsoft_calendar` decides on
/// (`isCancelled`, `seriesMasterId`).
pub(super) const EVENT_SELECT: &str =
    "id,subject,organizer,attendees,start,end,location,body,bodyPreview,isCancelled,seriesMasterId";

/// Graph `location` resource — only `displayName` is rendered (the
/// structured `address`/`coordinates` fields are never shown to the user
/// in Outlook's own event card either).
#[derive(Debug, Deserialize)]
struct GraphLocation {
    #[serde(rename = "displayName")]
    display_name: Option<String>,
}

/// Parse a Graph event time, or `None` when absent/unparseable. Graph
/// returns event times in UTC by default (no offset suffix), so the naive
/// timestamp is interpreted as UTC.
fn parse_graph_dt_opt(dt: Option<&GraphDateTime>) -> Option<DateTime<Utc>> {
    dt.and_then(|d| d.date_time.as_deref()).and_then(|s| {
        chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f")
            .ok()
            .map(|naive| DateTime::from_naive_utc_and_offset(naive, Utc))
    })
}

/// [`parse_graph_dt_opt`] with the `start`-time fallback the atom
/// `timestamp` has always used: an event with no parseable start is stamped
/// "now" rather than dropped.
fn parse_graph_dt(dt: Option<&GraphDateTime>) -> DateTime<Utc> {
    parse_graph_dt_opt(dt).unwrap_or_else(Utc::now)
}

/// `atoms.source_type` for a Microsoft Graph calendar event — drives the
/// generic chunker (no `ChunkStrategy` special-case exists for it, same as
/// `google_calendar::SOURCE_TYPE`) and [`MicrosoftConnector::source_types`].
/// Also the gate `services::source_reingest::is_calendar_event` uses to
/// decide an item is an upstream-EDITABLE calendar event whose stale coral
/// must be retracted before re-ingest (a Graph event keeps its `id` across
/// edits, so `ingest_atom`'s `DO NOTHING` insert would otherwise skip the
/// new subject/time/attendees forever).
pub(crate) const CALENDAR_SOURCE_TYPE: &str = "microsoft_calendar";

/// Shape one Graph calendar event into a [`NormalizedItem`] — the Microsoft
/// twin of `google_calendar::shape_calendar_item`, same rendering:
///
/// ```text
/// {subject}
///
/// {body as text — bodyPreview only when body is absent}
/// Ends: {end, RFC 3339 UTC}
/// Location: {location.displayName}
/// ```
///
/// The first line is the subject (it becomes the coral's title via
/// `atom_title::fallback_title_for_source`); the body lines are each
/// omitted when absent/blank, and an event with no body at all is just its
/// subject. The start time is the atom's `timestamp`, not prose — exactly
/// as Google's — so a rescheduled start is detected by the timestamp
/// comparison and a moved end/room by the content hash.
///
/// The description is the FULL `body` (HTML → text through
/// [`body_to_text_boundaried`], so an agenda's paragraphs and bullets stay
/// on their own lines, as Outlook mail's do), with the ~255-character
/// `bodyPreview` only as the fallback [`message_to_item`] also keeps. An
/// event whose body text equals its preview (short, plain descriptions)
/// shapes exactly as it did when only the preview was rendered, so such a
/// coral is not replaced; a longer one is replaced once, on the first pass
/// that reports it, and is then stable.
///
/// Either text then loses the Teams join block Outlook injects into every
/// Teams meeting (`email_body::trim_teams_meeting_block` — join link,
/// meeting id, passcode, dial-in numbers, "Meeting options | Reset dial-in
/// PIN"); the agenda the organiser typed above it is what the coral
/// carries, and a meeting with nothing but the block has no description.
///
/// Renders ONLY the fields [`EVENT_SELECT`] names, whatever else the
/// decoded resource carried: a `calendarView/delta` item (full resource,
/// `$select` unsupported there) and a `GET /me/events/{id}` fetch of the
/// same unchanged event must produce byte-identical content, or every
/// delta pass would churn every coral (`services::source_reingest`).
pub(super) fn shape_graph_event(ev: GraphEvent) -> NormalizedItem {
    let subject = ev
        .subject
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or("(no subject)");

    let description_text = |t: &str| {
        let t = super::email_body::trim_teams_meeting_block(t);
        let t = t.trim();
        (!t.is_empty()).then(|| t.to_string())
    };
    let description = ev
        .body
        .as_ref()
        .map(body_to_text_boundaried)
        .filter(|t| !t.trim().is_empty())
        .map_or_else(|| ev.body_preview.as_deref().and_then(description_text), |t| description_text(&t));
    let mut body = String::new();
    if let Some(d) = description {
        body.push_str(&d);
        body.push('\n');
    }
    if let Some(end) = parse_graph_dt_opt(ev.end.as_ref()) {
        body.push_str(&format!("Ends: {}\n", end.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)));
    }
    if let Some(loc) = ev.location.as_ref().and_then(|l| l.display_name.as_deref()) {
        let l = loc.trim();
        if !l.is_empty() {
            body.push_str(&format!("Location: {l}\n"));
        }
    }
    let content = if body.trim().is_empty() {
        subject.to_string()
    } else {
        format!("{subject}\n\n{}", body.trim())
    };

    let mut participants = Vec::new();
    if let Some(org) = ev.organizer.as_ref().and_then(recipient_label) {
        participants.push(org);
    }
    participants.extend(ev.attendees.iter().filter_map(|a| {
        a.email_address
            .as_ref()
            .and_then(|ea| ea.name.clone().or_else(|| ea.address.clone()))
    }));

    NormalizedItem {
        source_type: CALENDAR_SOURCE_TYPE.into(),
        source_id: ev.id,
        content,
        participants,
        timestamp: parse_graph_dt(ev.start.as_ref()),
        scope: None,
        thread_id: None,
        channel_id: None,
        author_id: None,
        author_name: None,
    }
}

// ── Thread expansion + change-notification subscriptions (CS-117) ────────────────

/// Mail-folder resources we subscribe to for reply auto-capture. Graph cannot
/// subscribe to a single conversation, so we watch whole folders and match each
/// notification's `conversationId` against the watched set (see ADR-0008). We
/// watch **Inbox** (replies from others) and **Sent Items** (the user's own
/// replies, which never land in Inbox) so both sides of a thread are captured in
/// real time.
const INBOX_RESOURCE: &str = "me/mailFolders('inbox')/messages";
const SENT_RESOURCE: &str = "me/mailFolders('sentitems')/messages";
/// Every folder we keep a live subscription on, per account.
const WATCHED_RESOURCES: [&str; 2] = [INBOX_RESOURCE, SENT_RESOURCE];
/// Requested subscription lifetime. Graph caps mail-message subscriptions at
/// 4230 minutes; stay safely under it.
const SUBSCRIPTION_MINUTES: i64 = 4170;
/// Re-create the subscription at ingest time if it would expire within this
/// window. The renewal worker is the primary keep-alive; this guards the case
/// where the worker is behind or the sub lapsed.
const SUBSCRIPTION_REFRESH_WINDOW_MINUTES: i64 = 360;
/// Graph maximum for Teams `chatMessage` (channel + chat) subscriptions is
/// 4,320 minutes (three days). 60 minutes is only the threshold above which
/// `lifecycleNotificationUrl` is required — which [`graph_create_subscription`]
/// already sends — not a hard cap. Request 4,230, same margin style as mail's
/// 4,170 under its 4,230 cap. (CS-456 / Phase G item 57.)
const TEAMS_CHAT_MESSAGE_SUBSCRIPTION_MINUTES: i64 = 4230;

/// Lifetime requested for a Teams channel/chat doorbell subscription
/// (Phase G item 56). Same value [`subscription_minutes`] uses for those
/// resources — kept as a function so the control-plane registry can set
/// `blocked_until` to "now + the lifetime we asked Graph for" without
/// copying the number.
pub(crate) fn teams_chat_message_subscription_minutes() -> i64 {
    TEAMS_CHAT_MESSAGE_SUBSCRIPTION_MINUTES
}

/// Lifetime requested for a delegated organiser transcript subscription
/// (`users/{id}/onlineMeetings/getAllTranscripts`). Same margin as
/// [`subscription_minutes`] for transcript resources.
pub(crate) fn teams_transcript_subscription_minutes() -> i64 {
    TEAMS_TRANSCRIPT_SUBSCRIPTION_MINUTES
}
/// Graph maximum for Teams `callTranscript` subscriptions is also 4,320
/// minutes (Microsoft Graph change-notifications overview, verified 2026-09-24
/// against `transcript_subscription_resource` / `getAllTranscripts` +
/// `onlineMeetings` paths). Stay safely under it with the same margin.
const TEAMS_TRANSCRIPT_SUBSCRIPTION_MINUTES: i64 = 4230;
/// Refresh window for the long-lived Teams kinds above. Matches mail's
/// [`SUBSCRIPTION_REFRESH_WINDOW_MINUTES`] — a 25-minute window only made
/// sense when the lifetime was 50.
const TEAMS_LONG_REFRESH_WINDOW_MINUTES: i64 = 360;

/// How close to expiry the doorbell renews by re-create. Matches
/// [`subscription_refresh_window`] for channel/chat message resources.
pub(crate) fn teams_chat_message_refresh_window_minutes() -> i64 {
    TEAMS_LONG_REFRESH_WINDOW_MINUTES
}

/// Resources whose change notifications are Teams (not Outlook mail): channel
/// messages, chat messages, and meeting-transcript subscriptions (CS-121).
/// Lifetime and refresh window still branch per kind — see
/// [`subscription_minutes`] / [`subscription_refresh_window`].
pub(crate) fn is_teams_resource(resource: &str) -> bool {
    resource.contains("/channels/")
        || resource.starts_with("chats/")
        || resource.contains("getAllTranscripts")
        || resource.contains("onlineMeetings")
}

fn is_teams_transcript_resource(resource: &str) -> bool {
    resource.contains("getAllTranscripts") || resource.contains("onlineMeetings")
}

fn is_teams_chat_message_resource(resource: &str) -> bool {
    resource.contains("/channels/") || resource.starts_with("chats/")
}

/// Requested lifetime (minutes) for a subscription on `resource`.
/// Per-kind: mail 4,170; Teams channel/chat messages 4,230; Teams transcripts
/// 4,230. Not a single `is_teams_resource` boolean — transcripts and channel
/// messages share the Teams routing path but are different Graph resource
/// kinds (item 57).
fn subscription_minutes(resource: &str) -> i64 {
    if is_teams_transcript_resource(resource) {
        TEAMS_TRANSCRIPT_SUBSCRIPTION_MINUTES
    } else if is_teams_chat_message_resource(resource) {
        TEAMS_CHAT_MESSAGE_SUBSCRIPTION_MINUTES
    } else {
        SUBSCRIPTION_MINUTES
    }
}

/// How close to expiry we proactively re-create a subscription on `resource`.
fn subscription_refresh_window(resource: &str) -> i64 {
    if is_teams_transcript_resource(resource) || is_teams_chat_message_resource(resource) {
        TEAMS_LONG_REFRESH_WINDOW_MINUTES
    } else {
        SUBSCRIPTION_REFRESH_WINDOW_MINUTES
    }
}
/// Page size when expanding a conversation. We follow `@odata.nextLink` across
/// pages so the *whole* thread is captured, up to [`MAX_CONVERSATION_MESSAGES`].
const CONVERSATION_PAGE: usize = 50;
/// Hard cap on total messages pulled for one conversation — bounds a pathological
/// thread (and a crafted conversationId) without truncating any realistic thread.
const MAX_CONVERSATION_MESSAGES: usize = 1000;

/// `$select` for messages we map to corals + match workflows against (kept in
/// sync everywhere we fetch).
const MESSAGE_SELECT: &str = "id,conversationId,subject,from,toRecipients,ccRecipients,\
receivedDateTime,bodyPreview,body,categories,hasAttachments,parentFolderId";

fn notification_url() -> String {
    let base = std::env::var("BASE_URL").unwrap_or_else(|_| "http://localhost:8080".into());
    format!("{}/v1/webhooks/graph", base.trim_end_matches('/'))
}

/// 256 bits of opaque secret echoed back by Graph in every notification's
/// `clientState`, verified constant-time before we act on a notification.
pub(crate) fn new_subscription_client_state() -> String {
    format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple())
}

fn generate_client_state() -> String {
    new_subscription_client_state()
}

/// Fetch a single Outlook message (with its `conversationId`) by Graph id. Used
/// by [`MicrosoftConnector::fetch_items`] and the change-notification webhook.
pub(crate) async fn fetch_message(
    http: &reqwest::Client,
    access_token: &str,
    id: &str,
) -> Result<GraphMessage, ConnectorError> {
    let url = format!(
        "{GRAPH}/me/messages/{}?$select={MESSAGE_SELECT}",
        urlencoding::encode(id)
    );
    serde_json::from_value(graph_get(http, access_token, &url).await?)
        .map_err(|e| ConnectorError::Exchange(format!("message decode: {e}")))
}

/// Build the OData `$filter` selecting a conversation. String literals escape a
/// single quote by doubling it (defends against a crafted conversationId).
fn conversation_filter(conversation_id: &str) -> String {
    format!(
        "conversationId eq '{}'",
        conversation_id.replace('\'', "''")
    )
}

/// Result of one bounded, resumable Graph fetch — replaces the old "hit the
/// cap, log a warning, silently drop the rest" pattern (contract §6 test #6:
/// "no silent truncation"; see `.scratch/m365-enterprise/plan.md`'s "Corrections
/// to the decision record", PR 2.6). Every caller of a function returning this
/// must either persist `next_cursor` somewhere it will be resumed from, or have
/// proven `complete` is `true` — there is no third option that silently drops
/// `next_cursor` on the floor.
pub(crate) struct BackfillPage<T> {
    pub items: Vec<T>,
    /// Absolute Graph `@odata.nextLink` to resume from on a future call.
    /// `None` iff `complete` is `true` OR Graph itself returned no further
    /// pages within this call's own multi-page loop.
    pub next_cursor: Option<String>,
    /// `true` iff there is nothing left upstream — either because Graph's
    /// `@odata.nextLink` ran out, or the item count came in under the
    /// per-call page budget. `false` means the budget was hit with more
    /// upstream: the caller MUST persist `next_cursor` (an explicit, queryable
    /// "still fetching" signal) rather than discard it.
    pub complete: bool,
}

/// List messages in a conversation, paginating through Graph's `@odata.
/// nextLink` up to a per-call budget of [`MAX_CONVERSATION_MESSAGES`] (across
/// all folders — Inbox, Sent, Archive — since `/me/messages` spans the
/// mailbox). Used for the initial snapshot and the backstop resync (CS-117 /
/// ADR-0008). `resume_cursor` (an earlier call's `next_cursor`) picks up
/// exactly where a previous, budget-capped call left off — see
/// [`BackfillPage`]'s own doc. Passing `None` starts a fresh conversation
/// fetch from the top.
pub(crate) async fn fetch_conversation(
    http: &reqwest::Client,
    access_token: &str,
    conversation_id: &str,
    resume_cursor: Option<&str>,
) -> Result<BackfillPage<GraphMessage>, ConnectorError> {
    fetch_conversation_since(http, access_token, conversation_id, resume_cursor, None).await
}

/// Recovery fetch for a message-only watch (options train). Add the forward
/// boundary upstream so old history cannot exhaust the bounded page budget;
/// the caller also checks each message locally before ingest. NULL uses the
/// original whole-conversation query. Initial/resumable backfill callers go
/// through `fetch_conversation`, which preserves that original behavior.
pub(crate) async fn fetch_conversation_since(
    http: &reqwest::Client,
    access_token: &str,
    conversation_id: &str,
    resume_cursor: Option<&str>,
    follow_since: Option<DateTime<Utc>>,
) -> Result<BackfillPage<GraphMessage>, ConnectorError> {
    let mut filter = conversation_filter(conversation_id);
    if let Some(since) = follow_since {
        filter.push_str(&format!(" and receivedDateTime ge {}", since.to_rfc3339()));
    }
    // Graph returns an absolute `@odata.nextLink` we follow verbatim; the first
    // request carries the filter/$top/$select — UNLESS resuming, in which case
    // the stored cursor already carries everything (Graph's nextLink is a
    // complete, self-contained URL).
    let mut url = resume_cursor.map(str::to_string).unwrap_or_else(|| {
        format!(
            "{GRAPH}/me/messages?$filter={}&$top={CONVERSATION_PAGE}&$select={MESSAGE_SELECT}",
            urlencoding::encode(&filter)
        )
    });
    let mut messages = Vec::new();
    loop {
        let list: GraphList<GraphMessage> =
            serde_json::from_value(graph_get(http, access_token, &url).await?)
                .map_err(|e| ConnectorError::Exchange(format!("conversation decode: {e}")))?;
        messages.extend(list.value);
        match list.next_link {
            Some(next) if messages.len() < MAX_CONVERSATION_MESSAGES => url = next,
            Some(next) => {
                tracing::info!(
                    %conversation_id,
                    budget = MAX_CONVERSATION_MESSAGES,
                    fetched = messages.len(),
                    "conversation exceeds per-call budget — cursor persisted for resumption"
                );
                return Ok(BackfillPage {
                    items: messages,
                    next_cursor: Some(next),
                    complete: false,
                });
            }
            None => break,
        }
    }
    Ok(BackfillPage {
        items: messages,
        next_cursor: None,
        complete: true,
    })
}

#[derive(Debug, Deserialize)]
struct SubscriptionResponse {
    id: String,
    #[serde(rename = "expirationDateTime")]
    expiration_date_time: Option<DateTime<Utc>>,
}

/// The `changeType` string to request for `resource`'s subscription (m365
/// Train 4 PR 4.4 — upstream deletion propagation). Every OTHER resource this
/// module subscribes to is a `message`/`chatMessage` resource, and Graph
/// supports the full `created,updated,deleted` set for both: Outlook mail
/// (`me/mailFolders('…')/messages`) and Teams channel/chat messages (`teams/
/// {t}/channels/{c}/messages`, `chats/{c}/messages`) all report edits and
/// deletes, not just new arrivals.
///
/// The meeting-transcript stream (`appCatalogs/teamsApps/{id}/
/// installedToOnlineMeetings/getAllTranscripts`) is the one resource kind
/// left at `created`-only, deliberately: it fires exactly once per meeting,
/// when the VTT becomes available — there is no Graph-side concept of a
/// transcript being "updated" or "deleted" this notification stream could
/// ever report, so requesting anything past `created` for it would be
/// meaningless (`onlineMeetings` is included in the same carve-out for the
/// same reason, and as defense-in-depth against any future subscription
/// built directly on that resource root). `services::deletion::delete_
/// source`'s "same-account-only" cascade limit (see that module's own doc,
/// "Propagation") applies identically to whatever DOES get widened here —
/// this function only decides what Graph sends us, not how far a resulting
/// deletion propagates once it arrives.
fn subscription_change_types(resource: &str) -> &'static str {
    if resource.contains("getAllTranscripts") || resource.contains("onlineMeetings") {
        "created"
    } else {
        "created,updated,deleted"
    }
}

/// Create the inbox change-notification subscription. Graph validates the
/// `notificationUrl` *synchronously* (it POSTs a `validationToken` the webhook
/// must echo), so this only succeeds when the backend is publicly reachable.
async fn graph_create_subscription(
    http: &reqwest::Client,
    access_token: &str,
    resource: &str,
    client_state: &str,
    expiration: DateTime<Utc>,
) -> Result<SubscriptionResponse, ConnectorError> {
    let body = serde_json::json!({
        "changeType": subscription_change_types(resource),
        "notificationUrl": notification_url(),
        // Same endpoint receives lifecycle events; the handler branches on the
        // `lifecycleEvent` property (reauthorizationRequired/subscriptionRemoved/
        // missed) so we can renew, recreate, or resync — ADR-0008.
        "lifecycleNotificationUrl": notification_url(),
        "resource": resource,
        "expirationDateTime": expiration.to_rfc3339(),
        "clientState": client_state,
    });
    let resp = graph_send(|| {
        http.post(format!("{GRAPH}/subscriptions"))
            .bearer_auth(access_token)
            .json(&body)
    })
    .await?;
    if !resp.status().is_success() {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        return Err(ConnectorError::upstream(status, format!(
            "subscription create failed: {status}: {}",
            text.chars().take(300).collect::<String>()
        )));
    }
    resp.json()
        .await
        .map_err(|e| ConnectorError::Exchange(format!("subscription parse: {e}")))
}

/// Extend a subscription's lifetime (`PATCH /subscriptions/{id}`). Returns the
/// new expiry. Used by the renewal worker.
pub(crate) async fn renew_subscription(
    http: &reqwest::Client,
    access_token: &str,
    subscription_id: &str,
    resource: &str,
) -> Result<DateTime<Utc>, ConnectorError> {
    let expiration = Utc::now() + Duration::minutes(subscription_minutes(resource));
    let body = serde_json::json!({ "expirationDateTime": expiration.to_rfc3339() });
    let resp = graph_send(|| {
        http.patch(format!(
            "{GRAPH}/subscriptions/{}",
            urlencoding::encode(subscription_id)
        ))
        .bearer_auth(access_token)
        .json(&body)
    })
    .await?;
    if !resp.status().is_success() {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        return Err(ConnectorError::upstream(status, format!(
            "subscription renew failed: {status}: {}",
            text.chars().take(300).collect::<String>()
        )));
    }
    let parsed: SubscriptionResponse = resp
        .json()
        .await
        .map_err(|e| ConnectorError::Exchange(format!("renew parse: {e}")))?;
    Ok(parsed.expiration_date_time.unwrap_or(expiration))
}

/// Cheap liveness probe for a delegated token: `GET /me?$select=id`. A
/// 401/403 (or a classified credential death) means this token must not stay
/// the doorbell owner. Used by Teams container failover (`subscriptionRemoved`)
/// before re-election — not a content read.
pub(crate) async fn probe_delegated_token(
    http: &reqwest::Client,
    access_token: &str,
) -> Result<(), ConnectorError> {
    let url = format!("{GRAPH}/me?$select=id");
    graph_get(http, access_token, &url).await.map(|_| ())
}

/// Create one Graph change-notification subscription. Used by the Teams
/// doorbell (`connectors::teams_doorbell`) so channel/chat message
/// subscriptions are not one `graph_subscriptions` row per watcher.
pub(crate) async fn create_delegated_subscription(
    http: &reqwest::Client,
    access_token: &str,
    resource: &str,
    client_state: &str,
    expiration: DateTime<Utc>,
) -> Result<(String, DateTime<Utc>), ConnectorError> {
    let sub =
        graph_create_subscription(http, access_token, resource, client_state, expiration).await?;
    Ok((sub.id, sub.expiration_date_time.unwrap_or(expiration)))
}

/// `DELETE /subscriptions/{id}` for a delegated token. 404 is success.
pub(crate) async fn delete_delegated_subscription(
    http: &reqwest::Client,
    access_token: &str,
    subscription_id: &str,
) -> Result<(), ConnectorError> {
    graph_delete_subscription(http, access_token, subscription_id).await
}

/// Tenant admin kill switch (`Set-CsTeamsMeetingConfiguration` / "Graph API
/// access to transcripts"). Graph answers 403 with this code on every
/// transcript read and on subscription create/renew. It is not a dead
/// credential.
pub(crate) fn graph_access_to_transcripts_disabled(err: &ConnectorError) -> bool {
    err.upstream_message().is_some_and(|msg| msg.contains("GraphAccessToTranscriptsDisabled"))
}

/// Tenant turned off speaker attribution. `text/vtt` is refused; the
/// unattributed body is a different request, not a retry of the same one.
pub(crate) fn speaker_attribution_not_allowed(err: &ConnectorError) -> bool {
    err.upstream_message().is_some_and(|msg| msg.contains("SpeakerAttributionNotAllowed"))
}

pub(crate) fn subscription_change_types_for(resource: &str) -> &'static str {
    subscription_change_types(resource)
}

/// Graph refused to create because this app already holds the one slot for
/// the resource. Not a generic 403 (that is a permission miss and the next
/// candidate may succeed).
pub(crate) fn subscription_slot_taken(err: &ConnectorError) -> bool {
    if err.upstream_status() == Some(409) {
        return true;
    }
    let Some(msg) = err.upstream_message() else {
        return false;
    };
    let lower = msg.to_ascii_lowercase();
    lower.contains("already exists") || lower.contains("subscription already")
}

/// Ensure live subscriptions exist for every watched mail folder (Inbox + Sent
/// Items), creating any that are absent or nearing expiry. Idempotent, and
/// resilient: a failure on one folder is logged and does not block the other.
/// `pub(crate)` so the lifecycle handler can recreate a removed subscription.
/// `owner` (m365 enterprise train PR 3.3): which mailbox `access_token`
/// authenticates as — `TokenOwner::AccountWide` on the Personal plane
/// (byte-identical to every call before this parameter existed), or
/// `TokenOwner::Member(uid)` for a specific org member's own Outlook
/// connection. Threaded through to [`ensure_subscription_for`] so two org
/// members opting in concurrently each get their OWN live `inbox`/`sentitems`
/// subscription instead of the second silently no-oping against the first's.
pub(crate) async fn ensure_mail_subscriptions(
    http: &reqwest::Client,
    pool: &PgPool,
    account_id: Uuid,
    access_token: &str,
    owner: TokenOwner,
) -> Result<(), ConnectorError> {
    for resource in WATCHED_RESOURCES {
        if let Err(e) =
            ensure_subscription_for(http, pool, account_id, access_token, resource, "", owner).await
        {
            tracing::warn!(error = %e, %resource, "failed to ensure graph subscription");
        }
    }
    Ok(())
}

/// Ensure a live subscription for one resource (mail folder, Teams channel/chat
/// messages, or the meeting-transcript stream). Resource-type-aware expiry.
/// `tenant_id` is non-empty for app-only (RSC) subscriptions so renewal uses an
/// app token; empty for delegated. `pub(crate)` so the Teams install + lifecycle
/// handler can (re)create subscriptions.
///
/// `owner` (m365 enterprise train PR 3.3, migration `0190`/`0068`): every
/// Teams/transcript call site passes `TokenOwner::AccountWide` unconditionally
/// — those resource STRINGS already disambiguate by container (channel/chat/
/// tenant), so they never needed an owner dimension and keep using the
/// `owner_user_id IS NULL` partial index exactly as before this parameter
/// existed. Only the mail call sites ([`ensure_mail_subscriptions`]) pass a
/// real `Member` owner.
pub(crate) async fn ensure_subscription_for(
    http: &reqwest::Client,
    pool: &PgPool,
    account_id: Uuid,
    access_token: &str,
    resource: &str,
    tenant_id: &str,
    owner: TokenOwner,
) -> Result<(), ConnectorError> {
    // What this resource kind's subscription MUST be able to report today
    // (m365 Train 4 PR 4.4). Compared against what the stored subscription
    // was actually CREATED with, not just its expiry — see below.
    let wanted_change_types = subscription_change_types(resource);
    let mut stale_subscription_id: Option<String> = None;
    if let Some((expires_at, stored_change_types, subscription_id)) =
        subscription_expiry(pool, account_id, resource, owner).await?
    {
        // CAPABILITY CHECK BEFORE THE EXPIRY REUSE (CodeRabbit round 1 on PR
        // 4.4, finding 1): a subscription created before this PR carries
        // `changeType: "created"`, and `workers::graph_subscriptions`'
        // renewal tick keeps it alive with `PATCH /subscriptions/{id}`
        // (`renew_subscription`), which can only move `expirationDateTime` —
        // Graph offers NO way to widen an existing subscription's
        // `changeType`. Reusing on expiry alone therefore renews the legacy
        // fleet forever at `created`-only: deletion propagation would look
        // healthy (new installs fine, tests green) while silently never
        // firing for a single EXISTING install. So a capability mismatch
        // bypasses reuse entirely and forces a replace.
        if stored_change_types == wanted_change_types
            && expires_at > Utc::now() + Duration::minutes(subscription_refresh_window(resource))
        {
            return Ok(());
        }
        if stored_change_types != wanted_change_types {
            tracing::info!(
                %resource, %subscription_id,
                stored = %stored_change_types, wanted = %wanted_change_types,
                "graph subscription capability changed — replacing rather than renewing"
            );
            stale_subscription_id = Some(subscription_id);
        }
    }
    let client_state = generate_client_state();
    let expiration = Utc::now() + Duration::minutes(subscription_minutes(resource));
    // Delete the stale subscription upstream BEFORE creating its replacement:
    // Graph keeps both alive otherwise, and both point at the same
    // `notificationUrl`, so every change would arrive twice — once under a
    // `subscription_id` the upsert below is about to orphan (dropped as
    // "unknown subscription", noisy) and once under the new one. Best-effort:
    // a failed delete must not block the replacement, since the alternative
    // is staying on a subscription that cannot report deletions at all.
    if let Some(stale_id) = stale_subscription_id {
        if let Err(e) = graph_send(|| {
            http.delete(format!("{GRAPH}/subscriptions/{stale_id}"))
                .bearer_auth(access_token)
        })
        .await
        {
            tracing::warn!(
                error = %e, %resource, subscription_id = %stale_id,
                "failed to delete superseded graph subscription — replacing anyway \
                 (duplicate notifications possible until it expires)"
            );
        }
    }
    let sub =
        graph_create_subscription(http, access_token, resource, &client_state, expiration).await?;
    upsert_subscription(
        pool,
        account_id,
        &sub.id,
        resource,
        &client_state,
        sub.expiration_date_time.unwrap_or(expiration),
        tenant_id,
        owner,
        wanted_change_types,
    )
    .await
}

/// One `DELETE /subscriptions/{id}` attempt, `Err` on anything that is NOT a
/// success or a `404` (CodeRabbit review on #1052: `graph_send` itself never
/// turns a non-2xx status into an `Err` — only a transport-level failure
/// (timeout, DNS, rate-limiter) does — so a bare `if let Err(e) =
/// graph_send(...)` silently swallowed a genuine `400`/`401`/`403` Graph
/// rejection, contradicting [`delete_subscription_for`]'s own "logged for
/// findability" contract). `404` is treated as success — Graph already
/// forgot the subscription, same "already gone" case
/// [`delete_subscription_row`]'s own doc comment names.
async fn graph_delete_subscription(
    http: &reqwest::Client,
    token: &str,
    subscription_id: &str,
) -> Result<(), ConnectorError> {
    let resp = graph_send(|| {
        http.delete(format!(
            "https://graph.microsoft.com/v1.0/subscriptions/{subscription_id}"
        ))
        .bearer_auth(token)
    })
    .await?;
    let status = resp.status();
    if status.is_success() || status == reqwest::StatusCode::NOT_FOUND {
        Ok(())
    } else {
        Err(ConnectorError::upstream(status, format!(
            "subscription delete: {status}"
        )))
    }
}

/// Cancel a live Graph subscription for `(account_id, resource)` and drop its
/// row. Used when the CoralSwarm bot is removed from a Teams conversation
/// (CS-122), and (CS-452) when a member explicitly un-watches a Teams
/// container via `routes::teams_ingest::delete_watched`. App-only (RSC)
/// subscriptions are deleted on Graph with an app token; delegated ones are
/// deleted with `delegated_access_token` when the caller has one (CS-452 —
/// previously always skipped upstream, matching the exact gap
/// `connectors::microsoft_disconnect::purge_microsoft_connection`'s own
/// `delegated_access_token` doc comment names for its own upstream phase).
/// The row is removed regardless so renewal/re-notification stops either way
/// — an upstream failure (expired token, Graph already having forgotten the
/// subscription, no token supplied at all) is never fatal to the local
/// delete, only logged for findability.
///
/// `owner_user_id` (CS-452) scopes BOTH the lookup and the delete to an exact
/// `graph_subscriptions.owner_user_id` value (`IS NOT DISTINCT FROM`, never
/// the `IS NULL OR = $n` fallback `TokenOwner`'s own doc comment forbids).
/// Every call site today passes `None`: this function's resource strings are
/// exclusively Teams channel/chat containers
/// (`teams_subscription_resource`'s own doc), and every Teams/transcript
/// `graph_subscriptions` row is written `owner_user_id IS NULL`
/// unconditionally regardless of `auth_mode` (`ensure_subscription_for`'s
/// own doc comment) — so `None` is the only value that can ever match a row
/// this function is asked to delete today. The parameter exists so a future
/// caller that reuses this function for an owner-tagged (mail-shaped)
/// resource cannot silently inherit the no-owner-filter hazard a bare
/// `WHERE account_id = $1 AND resource = $2` would otherwise carry for that
/// case (two different owners' mail subscriptions share the identical
/// `"me/mailFolders('inbox')/messages"` resource literal — see
/// `connectors::microsoft_disconnect::delete_subscriptions`'s own doc
/// comment on why `owner_user_id` is the ONLY thing that disambiguates them).
pub(crate) async fn delete_subscription_for(
    http: &reqwest::Client,
    pool: &PgPool,
    account_id: Uuid,
    resource: &str,
    owner_user_id: Option<Uuid>,
    delegated_access_token: Option<&str>,
) -> Result<(), ConnectorError> {
    // Two scoped transactions, not one spanning the Graph call in between:
    // holding a pooled connection open across a network round-trip is the
    // anti-pattern PR #291's per-statement `JobScope` exists to avoid.
    let mut tx = scoped_tx(pool, account_id, "delete_subscription_for lookup").await?;
    let row: Option<(String, String)> = sqlx::query_as(
        "SELECT subscription_id, tenant_id FROM graph_subscriptions \
         WHERE account_id = $1 AND resource = $2 AND owner_user_id IS NOT DISTINCT FROM $3",
    )
    .bind(account_id)
    .bind(resource)
    .bind(owner_user_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("subscription lookup: {e}")))?;
    tx.rollback().await.ok();

    let Some((subscription_id, tenant_id)) = row else {
        return Ok(());
    };

    // Best-effort remote delete. App-only (RSC) subs carry a tenant id and
    // mint a fresh client-credentials token for it. Delegated subs (empty
    // `tenant_id`) use whatever `delegated_access_token` the caller supplied
    // — CS-452: previously this branch did not exist at all, so a delegated
    // Teams channel/chat subscription was NEVER deleted upstream on un-watch,
    // left to expire on Graph's own TTL (the exact orphan class item 3/4 of
    // CS-452's brief names — a longer subscription lifetime, e.g. item 57,
    // makes that orphan hold Graph's one-subscription-per-channel slot
    // correspondingly longer).
    if !tenant_id.is_empty() {
        if let Ok(token) = app_token(http, &tenant_id).await {
            if let Err(e) = graph_delete_subscription(http, &token, &subscription_id).await {
                tracing::info!(
                    error = %e,
                    %account_id,
                    %resource,
                    %subscription_id,
                    "delete_subscription_for: app-only upstream subscription delete failed — \
                     best-effort, local row is still removed below"
                );
            }
        }
    } else if let Some(token) = delegated_access_token {
        if let Err(e) = graph_delete_subscription(http, token, &subscription_id).await {
            tracing::info!(
                error = %e,
                %account_id,
                %resource,
                %subscription_id,
                "delete_subscription_for: delegated upstream subscription delete failed — \
                 best-effort, local row is still removed below"
            );
        }
    } else {
        tracing::info!(
            %account_id,
            %resource,
            %subscription_id,
            "delete_subscription_for: no delegated access token available — delegated \
             subscription upstream delete skipped; local row is still removed below, Graph \
             expires the orphan on its own short TTL"
        );
    }

    let mut tx = scoped_tx(pool, account_id, "delete_subscription_for").await?;
    sqlx::query(
        "DELETE FROM graph_subscriptions \
         WHERE account_id = $1 AND resource = $2 AND owner_user_id IS NOT DISTINCT FROM $3",
    )
    .bind(account_id)
    .bind(resource)
    .bind(owner_user_id)
    .execute(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("subscription delete: {e}")))?;
    tx.commit()
        .await
        .map_err(|e| ConnectorError::Internal(format!("subscription delete commit: {e}")))?;
    Ok(())
}

// ── Watch / subscription persistence ─────────────────────────────────────────────

/// Workflow/backfill watch registration. New rows follow the workflow;
/// existing rows retain their user's follow preference and capture boundary.
/// A matching rule must not silently undo an explicit unfollow or widen a
/// message-only save. Explicit opt-in saves use the separate atomic helper
/// below, which adopts that request's follow value and marks fetching complete.
pub(crate) async fn upsert_watched_conversation(
    pool: &PgPool,
    account_id: Uuid,
    user_id: Uuid,
    graph_id: Uuid,
    conversation_id: &str,
    follow_replies: bool,
) -> Result<(), ConnectorError> {
    // `backfill_complete = FALSE` on a fresh insert only (m365 Train 2 PR
    // 2.6) — same reasoning as `teams_ingest::install_watch`'s twin: a brand
    // new watch has not been backfilled yet, but re-watching an
    // already-watched conversation (the `ON CONFLICT` arm) must not reset an
    // in-progress or already-complete backfill.
    let mut tx = scoped_tx(pool, account_id, "upsert_watched_conversation").await?;
    sqlx::query(
        r#"
        INSERT INTO outlook_watched_conversations
            (account_id, graph_id, ingested_by, conversation_id, last_ingested_at, backfill_complete, follow_replies)
        VALUES ($1, $2, $3, $4, now(), FALSE, $5)
        ON CONFLICT (account_id, conversation_id)
        DO UPDATE SET last_ingested_at = now()
        "#,
    )
    .bind(account_id)
    .bind(graph_id)
    .bind(user_id)
    .bind(conversation_id)
    .bind(follow_replies)
    .execute(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("watch upsert: {e}")))?;
    tx.commit()
        .await
        .map_err(|e| ConnectorError::Internal(format!("watch upsert commit: {e}")))?;
    Ok(())
}

/// Explicit opt-in save: unlike workflow registration, adopts this request's
/// follow value. Initial fetching has completed synchronously; complete the
/// marker atomically with the row so message scope cannot schedule history.
async fn upsert_watched_conversation_for_save(
    pool: &PgPool,
    account_id: Uuid,
    user_id: Uuid,
    graph_id: Uuid,
    conversation_id: &str,
    follow: bool,
    follow_since: Option<DateTime<Utc>>,
) -> Result<(), ConnectorError> {
    let mut tx = scoped_tx(pool, account_id, "upsert_watched_conversation_for_save").await?;
    sqlx::query(r#"
        INSERT INTO outlook_watched_conversations
          (account_id,graph_id,ingested_by,conversation_id,last_ingested_at,backfill_complete,follow_replies,follow_since)
        VALUES ($1,$2,$3,$4,now(),TRUE,$5,$6)
        ON CONFLICT (account_id,conversation_id) DO UPDATE
        SET last_ingested_at=now(),follow_replies=EXCLUDED.follow_replies,
            follow_since=EXCLUDED.follow_since,backfill_complete=TRUE,backfill_cursor=NULL
    "#).bind(account_id).bind(graph_id).bind(user_id).bind(conversation_id).bind(follow).bind(follow_since)
        .execute(&mut *tx).await.map_err(|e| ConnectorError::Internal(format!("save watch upsert: {e}")))?;
    tx.commit()
        .await
        .map_err(|e| ConnectorError::Internal(format!("save watch upsert commit: {e}")))?;
    Ok(())
}
/// Resolve `(graph_id, ingested_by, follow_since)` for a followed conversation.
/// Bumps `last_ingested_at`. `None` means this account is not watching the thread,
/// OR is watching it with `follow_replies = FALSE` — either way the signal
/// the webhook uses to discard the reply rather than auto-capture it (an
/// unfollowed conversation is a real, intentional row; see
/// [`upsert_watched_conversation`]'s own doc — this is the live-follow gate,
/// gmail-options backend train PR 1 of 4, and deliberately does NOT touch
/// `last_ingested_at` when it declines to match for that reason: no ingest
/// happened, so nothing to record). The `ingested_by` user is the identity
/// auto-captured replies are ingested as.
pub(crate) async fn watched_conversation_graph(
    pool: &PgPool,
    account_id: Uuid,
    conversation_id: &str,
) -> Result<Option<(Uuid, Uuid, Option<DateTime<Utc>>)>, ConnectorError> {
    let mut tx = scoped_tx(pool, account_id, "watched_conversation_graph").await?;
    let row: Option<(Uuid, Uuid, Option<DateTime<Utc>>)> = sqlx::query_as(
        r#"
        UPDATE outlook_watched_conversations
        SET last_ingested_at = now()
        WHERE account_id = $1 AND conversation_id = $2 AND follow_replies = TRUE
        RETURNING graph_id, ingested_by, follow_since
        "#,
    )
    .bind(account_id)
    .bind(conversation_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("watch lookup: {e}")))?;
    tx.commit()
        .await
        .map_err(|e| ConnectorError::Internal(format!("watch lookup commit: {e}")))?;
    Ok(row)
}

/// All FOLLOWED watched conversations for an account: `(conversation_id,
/// graph_id, ingested_by, follow_since)`. Drives the backstop resync (re-list each thread
/// to recover replies missed during a subscription/delivery gap — ADR-0008)
/// and the has-anything-to-resync short-circuit that gates whether the
/// backstop tick bothers running at all — both are live-follow mechanisms
/// (gmail-options backend train, PR 1 of 4), so both filter on
/// `follow_replies = TRUE` here, at the one function they share.
pub(crate) async fn watched_conversations_for_account(
    pool: &PgPool,
    account_id: Uuid,
) -> Result<Vec<(String, Uuid, Uuid, Option<DateTime<Utc>>)>, ConnectorError> {
    let mut tx = scoped_tx(pool, account_id, "watched_conversations_for_account").await?;
    let rows = sqlx::query_as(
        r#"
        SELECT conversation_id, graph_id, ingested_by, follow_since
        FROM outlook_watched_conversations
        WHERE account_id = $1 AND follow_replies = TRUE
        "#,
    )
    .bind(account_id)
    .fetch_all(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("watched_conversations_for_account: {e}")));
    tx.rollback().await.ok();
    rows
}

// ---------------------------------------------------------------------------
// Provider-generic watched resource (gmail-options backend train, PR 1 of
// 4): status / toggle / list — backs `routes::connectors`'s
// `GET|PUT|DELETE /v1/connectors/{provider}/watched/{thread_id}` and
// `GET /v1/connectors/{provider}/watched`, reused (via that route module's
// own per-provider dispatch) by the Gmail add-on, the Outlook add-in and the
// web app. See `connectors::google_gmail`'s equivalent trio for the Gmail
// twin — deliberately NOT filtered on `follow_replies` (unlike
// [`watched_conversation_graph`]/[`watched_conversations_for_account`]
// above): these three back the "what have I saved, and is it still
// followed" read/write surface itself, so they must see and touch an
// unfollowed row exactly like a followed one — only the live-follow *ingest*
// paths gate on the flag.
// ---------------------------------------------------------------------------

/// `Some(following)` for an existing row, `None` if this account has never
/// opted this conversation in. Never writes anything.
pub(crate) async fn watched_status(
    pool: &PgPool,
    account_id: Uuid,
    conversation_id: &str,
) -> Result<Option<bool>, ConnectorError> {
    let mut tx = crate::tenancy::scope::begin_request_system_read(pool, account_id).await.map_err(|e| ConnectorError::Internal(format!("watched_status scoped tx: {e}")))?;
    let row: Option<(bool,)> = sqlx::query_as(
        "SELECT follow_replies FROM outlook_watched_conversations \
         WHERE account_id = $1 AND conversation_id = $2",
    )
    .bind(account_id)
    .bind(conversation_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("watched_status: {e}")))?;
    tx.rollback().await.ok();
    Ok(row.map(|(f,)| f))
}

/// Flip `follow_replies` for an already-watched conversation. `Some(new_value)`
/// on success, `None` if no row exists for this account + conversation — the
/// PUT/DELETE watched-resource routes turn that into a `404`. NEVER inserts a
/// row and NEVER deletes one — see [`upsert_watched_conversation`]'s own doc
/// for why a lingering row is load-bearing for deletion propagation.
pub(crate) async fn set_follow(
    pool: &PgPool,
    account_id: Uuid,
    conversation_id: &str,
    follow: bool,
) -> Result<Option<bool>, ConnectorError> {
    let mut tx = scoped_tx(pool, account_id, "set_follow").await?;
    let row: Option<(bool,)> = sqlx::query_as(
        r#"
        UPDATE outlook_watched_conversations
        SET follow_replies = $1
        WHERE account_id = $2 AND conversation_id = $3
        RETURNING follow_replies
        "#,
    )
    .bind(follow)
    .bind(account_id)
    .bind(conversation_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("set_follow: {e}")))?;
    tx.commit()
        .await
        .map_err(|e| ConnectorError::Internal(format!("set_follow commit: {e}")))?;
    Ok(row.map(|(f,)| f))
}

/// One row of the watched-conversation list (`GET /v1/connectors/{provider}/watched`).
pub(crate) struct WatchedRow {
    /// Kept for pg-test assertions and any future caller — the route
    /// handler itself only needs the cursor tuple's own copy (extracted
    /// before this conversion), not this field.
    #[allow(dead_code)]
    pub(crate) id: Uuid,
    pub(crate) conversation_id: String,
    #[allow(dead_code)]
    pub(crate) graph_id: Uuid,
    pub(crate) following: bool,
    pub(crate) saved_at: DateTime<Utc>,
}

/// Newest-first (by `saved_at` — see field doc) page of every watched
/// conversation for `account_id`, following or not (the web list
/// filters/toggles client-side — this function has no opinion). `cursor` is
/// the previous page's last row's own `id` — same keyset technique as
/// `connectors::google_gmail::list_watched` (see that function's own doc for
/// the full reasoning; byte-identical here, just against
/// `outlook_watched_conversations`). Returns up to `limit` rows plus the
/// next cursor (`None` once the account's last page has been reached).
///
/// `saved_at` = `last_ingested_at`, not `created_at` — same reasoning
/// `google_gmail::list_watched`'s own doc gives: this table has no
/// `updated_at` column either (`0035`'s own schema), and `last_ingested_at`
/// is the one that actually moves on a re-save.
pub(crate) async fn list_watched(
    pool: &PgPool,
    account_id: Uuid,
    limit: i64,
    cursor: Option<Uuid>,
) -> Result<(Vec<WatchedRow>, Option<Uuid>), ConnectorError> {
    let mut tx = scoped_tx(pool, account_id, "list_watched").await?;
    let mut rows: Vec<(Uuid, String, Uuid, bool, DateTime<Utc>)> = sqlx::query_as(
        r#"
        SELECT id, conversation_id, graph_id, follow_replies, last_ingested_at
        FROM outlook_watched_conversations
        WHERE account_id = $1
          AND (
            $3::uuid IS NULL
            OR (last_ingested_at, id) < (
                SELECT last_ingested_at, id FROM outlook_watched_conversations
                WHERE account_id = $1 AND id = $3
            )
          )
        ORDER BY last_ingested_at DESC, id DESC
        LIMIT $2
        "#,
    )
    .bind(account_id)
    .bind(limit + 1)
    .bind(cursor)
    .fetch_all(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("list_watched: {e}")))?;
    tx.rollback().await.ok();
    // One lookahead row distinguishes a full final page from a page
    // that actually has more content; clients need no empty sentinel page.
    let has_more = rows.len() as i64 > limit;
    rows.truncate(limit as usize);
    let next_cursor = rows.last().map(|(id, ..)| *id).filter(|_| has_more);
    let out = rows
        .into_iter()
        .map(
            |(id, conversation_id, graph_id, following, saved_at)| WatchedRow {
                id,
                conversation_id,
                graph_id,
                following,
                saved_at,
            },
        )
        .collect();
    Ok((out, next_cursor))
}

/// Resolve a captured Microsoft item within its owning account. Deleted Graph
/// notifications carry the bare message ID, while public ingress stores numeric
/// `:chunk:N` children. Match either form, including same-account promoted
/// copies, without accepting neighboring IDs or treating `%`/`_` as wildcards.
/// This lookup is used only by the Microsoft deletion notification path.
/// `None` means no captured copy remains; the caller skips deletion/audit noise.
pub(crate) async fn atom_graph_for_source(
    pool: &PgPool,
    account_id: Uuid,
    source_type: &str,
    source_id: &str,
) -> Result<Option<Uuid>, ConnectorError> {
    let mut tx = scoped_tx(pool, account_id, "atom_graph_for_source").await?;
    let row: Option<(Uuid,)> = sqlx::query_as(
        "SELECT a.graph_id FROM atoms a JOIN graphs g ON g.id = a.graph_id \
         WHERE g.account_id = $1 AND a.source_type = $2 AND (a.source_id = $3 OR (a.source_id LIKE $4 \
              AND substring(a.source_id FROM char_length($3) + 1) ~ '^:chunk:[0-9]+$')) LIMIT 1",
    )
    .bind(account_id)
    .bind(source_type)
    .bind(source_id)
    .bind(crate::services::deletion::escaped_chunk_pattern(source_id))
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("atom_graph_for_source: {e}")))?;
    tx.rollback().await.ok();
    Ok(row.map(|(g,)| g))
}

// ── Resumable backfill cursor state (m365 Train 2 PR 2.6) ────────────────────
//
// Persist the outcome of a bounded `fetch_conversation`/`fetch_channel_
// messages`/`fetch_chat_messages` call so a budget-capped fetch resumes on
// the next pass instead of the remainder being dropped with only a log line
// (contract §6 test #6). See [`BackfillPage`]'s own doc for the shape.

/// Persist the outcome of one `fetch_conversation` call for a watched
/// conversation. `next_cursor: None` with `complete: true` clears any earlier
/// stale cursor (the conversation caught up). No-op (zero rows affected) if
/// the conversation isn't watched — the caller has already upserted the
/// watch before ever fetching, so this is defensive, not the expected path.
pub(crate) async fn set_conversation_backfill_state(
    pool: &PgPool,
    account_id: Uuid,
    conversation_id: &str,
    next_cursor: Option<&str>,
    complete: bool,
) -> Result<(), ConnectorError> {
    let mut tx = scoped_tx(pool, account_id, "set_conversation_backfill_state").await?;
    sqlx::query(
        r#"
        UPDATE outlook_watched_conversations
        SET backfill_cursor = $1, backfill_complete = $2
        WHERE account_id = $3 AND conversation_id = $4
        "#,
    )
    .bind(next_cursor)
    .bind(complete)
    .bind(account_id)
    .bind(conversation_id)
    .execute(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("set_conversation_backfill_state: {e}")))?;
    tx.commit().await.map_err(|e| {
        ConnectorError::Internal(format!("set_conversation_backfill_state commit: {e}"))
    })?;
    Ok(())
}

/// Watched conversations for an account whose backfill has not completed —
/// `workers::graph_subscriptions::tick_backfill_continue`'s work set:
/// `(conversation_id, graph_id, ingested_by, backfill_cursor)`.
#[allow(clippy::type_complexity)]
pub(crate) async fn pending_conversation_backfills_for_account(
    pool: &PgPool,
    account_id: Uuid,
) -> Result<Vec<(String, Uuid, Uuid, Option<String>)>, ConnectorError> {
    let mut tx = scoped_tx(
        pool,
        account_id,
        "pending_conversation_backfills_for_account",
    )
    .await?;
    let rows = sqlx::query_as(
        r#"
        SELECT conversation_id, graph_id, ingested_by, backfill_cursor
        FROM outlook_watched_conversations
        WHERE account_id = $1 AND NOT backfill_complete
        "#,
    )
    .bind(account_id)
    .fetch_all(&mut *tx)
    .await
    .map_err(|e| {
        ConnectorError::Internal(format!("pending_conversation_backfills_for_account: {e}"))
    });
    tx.rollback().await.ok();
    rows
}

/// Persist the outcome of one `fetch_channel_messages`/`fetch_chat_messages`
/// call for a watched Teams resource, looked up the same way `teams_watched_
/// by_container` is (`channel_id = $2 OR chat_id = $2`), scoped to one
/// member + auth_mode so a delegated backfill cannot overwrite another
/// member's (or the app-mode) cursor.
pub(crate) async fn set_teams_backfill_state(
    pool: &PgPool,
    account_id: Uuid,
    container_id: &str,
    user_id: Uuid,
    auth_mode: &str,
    next_cursor: Option<&str>,
    complete: bool,
) -> Result<(), ConnectorError> {
    set_teams_backfill_state_fenced(pool, account_id, container_id, user_id, auth_mode, next_cursor, complete, None).await
}

/// A fetched page may only advance the generation whose bounds it read.
pub(crate) async fn set_teams_backfill_state_fenced(
    pool: &PgPool, account_id: Uuid, container_id: &str, user_id: Uuid,
    auth_mode: &str, next_cursor: Option<&str>, complete: bool,
    expected: Option<(Option<DateTime<Utc>>, Option<DateTime<Utc>>)>,
) -> Result<(), ConnectorError> {
    // A channel row stores `chat_id = ''` and a chat row stores `channel_id =
    // ''` (see `ingest_resource`/`install_watch`, which bind `String::new()`
    // for the unused field). An empty `container_id` would match `(channel_id
    // = $4 OR chat_id = $4)` against every row for this account, silently
    // clobbering every other watched resource's cursor/completion state —
    // worse on the org plane, where `account_id` is shared across members.
    if container_id.is_empty() {
        return Err(ConnectorError::Internal(
            "set_teams_backfill_state: empty container_id would match every watched \
             resource for this account"
                .into(),
        ));
    }
    let mut tx = scoped_tx(pool, account_id, "set_teams_backfill_state").await?;
    sqlx::query(
        r#"
        UPDATE teams_watched_resources
        SET backfill_cursor = $1, backfill_complete = $2
        WHERE account_id = $3 AND (channel_id = $4 OR chat_id = $4)
          AND user_id = $5 AND auth_mode = $6
          AND ($7 OR (backfill_since IS NOT DISTINCT FROM $8 AND backfill_until IS NOT DISTINCT FROM $9))
        "#,
    )
    .bind(next_cursor)
    .bind(complete)
    .bind(account_id)
    .bind(container_id)
    .bind(user_id)
    .bind(auth_mode)
    .bind(expected.is_none())
    .bind(expected.and_then(|v| v.0))
    .bind(expected.and_then(|v| v.1))
    .execute(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("set_teams_backfill_state: {e}")))?;
    tx.commit()
        .await
        .map_err(|e| ConnectorError::Internal(format!("set_teams_backfill_state commit: {e}")))?;
    Ok(())
}

/// Watched Teams resources for an account whose backfill has not completed —
/// `workers::graph_subscriptions::tick_backfill_continue`'s work set:
/// `(kind, team_id, channel_id, chat_id, graph_id, user_id, tenant_id,
/// auth_mode, backfill_cursor)`.
#[allow(clippy::type_complexity)]
pub(crate) async fn pending_teams_backfills_for_account(
    pool: &PgPool,
    account_id: Uuid,
) -> Result<
    Vec<(
        String,
        String,
        String,
        String,
        Uuid,
        Uuid,
        String,
        String,
        Option<String>,
        Option<DateTime<Utc>>,
        Option<DateTime<Utc>>,
    )>,
    ConnectorError,
> {
    let mut tx = scoped_tx(pool, account_id, "pending_teams_backfills_for_account").await?;
    let rows = sqlx::query_as(
        r#"
        SELECT kind, team_id, channel_id, chat_id, graph_id, user_id, tenant_id, auth_mode, backfill_cursor, backfill_since, backfill_until
        FROM teams_watched_resources
        WHERE account_id = $1 AND NOT backfill_complete
          AND NOT EXISTS (SELECT 1 FROM connector_sources cs
              WHERE cs.account_id = teams_watched_resources.account_id
                AND cs.owner_user_id = teams_watched_resources.user_id
                AND cs.provider = 'microsoft'
                AND cs.source = CASE WHEN kind = 'chat' THEN 'teams_chats' ELSE 'teams_channels' END
                AND (NOT cs.enabled OR cs.paused_at IS NOT NULL))
        "#,
    )
    .bind(account_id)
    .fetch_all(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("pending_teams_backfills_for_account: {e}")));
    tx.rollback().await.ok();
    rows
}

/// Distinct accounts that have at least one watched conversation — the work set
/// for the periodic backstop resync.
///
/// **Deliberately cross-account** (see this module's RLS-scoping note): this is
/// the sweep's entry point, so there is no account to scope it to yet. Reads
/// through `resolve_watched_conversation_accounts()`, the narrow `SECURITY
/// DEFINER` resolver from migration `0086_webhook_share_bootstrap_functions` —
/// bare `account_id`s only, no conversation ids, no content. Each returned
/// account is then processed under its own `begin_account_scoped` transaction
/// by `watched_conversations_for_account` above.
pub(crate) async fn accounts_with_watched_conversations(
    pool: &PgPool,
) -> Result<Vec<Uuid>, ConnectorError> {
    let rows: Vec<(Uuid,)> =
        sqlx::query_as("SELECT * FROM resolve_watched_conversation_accounts()")
            .fetch_all(pool)
            .await
            .map_err(|e| {
                ConnectorError::Internal(format!("accounts_with_watched_conversations: {e}"))
            })?;
    Ok(rows.into_iter().map(|(a,)| a).collect())
}

/// `owner` (m365 enterprise train PR 3.3): selects which of the two disjoint
/// partial unique indexes (migration `0190`/`0068`) to read against —
/// `owner_user_id IS NULL` for `AccountWide`, `owner_user_id = $n` for
/// `Member(uid)`. Never an `IS NULL OR = $n` fallback, same discipline
/// `routes::connectors::load_creds`'s own doc comment states for
/// `oauth_tokens`: a Member's own subscription must never be shadowed by (or
/// shadow) a different owner's row for the same resource.
async fn subscription_expiry(
    pool: &PgPool,
    account_id: Uuid,
    resource: &str,
    owner: TokenOwner,
) -> Result<Option<(DateTime<Utc>, String, String)>, ConnectorError> {
    let mut tx = scoped_tx(pool, account_id, "subscription_expiry").await?;
    let row: Option<(DateTime<Utc>, String, String)> = match owner {
        TokenOwner::AccountWide => {
            sqlx::query_as(
                "SELECT expires_at, change_types, subscription_id FROM graph_subscriptions \
             WHERE account_id = $1 AND resource = $2 AND owner_user_id IS NULL",
            )
            .bind(account_id)
            .bind(resource)
            .fetch_optional(&mut *tx)
            .await
        }
        TokenOwner::Member(owner_user_id) => {
            sqlx::query_as(
                "SELECT expires_at, change_types, subscription_id FROM graph_subscriptions \
             WHERE account_id = $1 AND resource = $2 AND owner_user_id = $3",
            )
            .bind(account_id)
            .bind(resource)
            .bind(owner_user_id)
            .fetch_optional(&mut *tx)
            .await
        }
    }
    .map_err(|e| ConnectorError::Internal(format!("subscription lookup: {e}")))?;
    tx.rollback().await.ok();
    Ok(row)
}

/// True when a live subscription for `(account_id, resource, owner)` already
/// exists and is comfortably clear of its refresh window — i.e. it needs
/// neither creation nor recreation right now. Mirrors the skip condition
/// inside [`ensure_subscription_for`], so callers can cheaply decide whether
/// there is any work to do *before* paying for a token mint. Every current
/// caller is a Teams/transcript resource and passes `TokenOwner::AccountWide`
/// — see [`ensure_subscription_for`]'s own doc comment.
pub(crate) async fn subscription_row_present(
    pool: &PgPool,
    account_id: Uuid,
    resource: &str,
    owner: TokenOwner,
) -> bool {
    subscription_expiry(pool, account_id, resource, owner)
        .await
        .ok()
        .flatten()
        .is_some()
}

pub(crate) async fn live_subscription_is_current(
    pool: &PgPool,
    account_id: Uuid,
    resource: &str,
    owner: TokenOwner,
) -> Result<bool, ConnectorError> {
    Ok(
        match subscription_expiry(pool, account_id, resource, owner).await? {
            // Capability check included deliberately (m365 Train 4 PR 4.4):
            // this helper exists so a caller can skip a token mint when
            // there is no subscription work to do, so it MUST stay an exact
            // mirror of `ensure_subscription_for`'s skip condition. If it
            // answered on expiry alone, a stale-capability subscription
            // would report "current", the caller would skip the mint, and
            // `ensure_subscription_for` — the only thing that can perform
            // the replacement — would never be reached. The `created`-only
            // legacy row would then renew forever, which is precisely the
            // bug the capability marker exists to close.
            Some((expires_at, stored_change_types, _)) => {
                stored_change_types == subscription_change_types(resource)
                    && expires_at
                        > Utc::now() + Duration::minutes(subscription_refresh_window(resource))
            }
            None => false,
        },
    )
}

/// Write one `graph_subscriptions` row. Delegated transcript subscriptions
/// pass [`TokenOwner::Member`] on the org plane and [`TokenOwner::AccountWide`]
/// on the personal plane. The RSC app-catalog stream stays account-wide.
pub(crate) async fn persist_subscription(
    pool: &PgPool,
    account_id: Uuid,
    subscription_id: &str,
    resource: &str,
    client_state: &str,
    expires_at: DateTime<Utc>,
    tenant_id: &str,
    owner: TokenOwner,
    change_types: &str,
) -> Result<(), ConnectorError> {
    upsert_subscription(
        pool,
        account_id,
        subscription_id,
        resource,
        client_state,
        expires_at,
        tenant_id,
        owner,
        change_types,
    )
    .await
}

/// `owner` (m365 enterprise train PR 3.3): the `ON CONFLICT` target must
/// restate the exact column list AND predicate of whichever partial unique
/// index (migration `0190`/`0068`) this write means to hit — same discipline
/// `routes::oauth::store_token_bundle_tx`'s own doc comment states for
/// `oauth_tokens`' identically-shaped pair of indexes — so these are two
/// genuinely different statements, not one query with an interpolated owner
/// value.
async fn upsert_subscription(
    pool: &PgPool,
    account_id: Uuid,
    subscription_id: &str,
    resource: &str,
    client_state: &str,
    expires_at: DateTime<Utc>,
    tenant_id: &str,
    owner: TokenOwner,
    change_types: &str,
) -> Result<(), ConnectorError> {
    let mut tx = scoped_tx(pool, account_id, "upsert_subscription").await?;
    match owner {
        TokenOwner::AccountWide => {
            sqlx::query(
                r#"
                INSERT INTO graph_subscriptions
                    (account_id, subscription_id, resource, client_state, expires_at, tenant_id, owner_user_id, change_types, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, NULL, $7, now())
                ON CONFLICT (account_id, resource) WHERE owner_user_id IS NULL
                DO UPDATE SET subscription_id = EXCLUDED.subscription_id,
                              client_state    = EXCLUDED.client_state,
                              expires_at      = EXCLUDED.expires_at,
                              tenant_id       = EXCLUDED.tenant_id,
                              change_types    = EXCLUDED.change_types,
                              updated_at      = now()
                "#,
            )
            .bind(account_id)
            .bind(subscription_id)
            .bind(resource)
            .bind(client_state)
            .bind(expires_at)
            .bind(tenant_id)
            .bind(change_types)
            .execute(&mut *tx)
            .await
        }
        TokenOwner::Member(owner_user_id) => {
            sqlx::query(
                r#"
                INSERT INTO graph_subscriptions
                    (account_id, subscription_id, resource, client_state, expires_at, tenant_id, owner_user_id, change_types, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, now())
                ON CONFLICT (account_id, resource, owner_user_id) WHERE owner_user_id IS NOT NULL
                DO UPDATE SET subscription_id = EXCLUDED.subscription_id,
                              client_state    = EXCLUDED.client_state,
                              expires_at      = EXCLUDED.expires_at,
                              tenant_id       = EXCLUDED.tenant_id,
                              change_types    = EXCLUDED.change_types,
                              updated_at      = now()
                "#,
            )
            .bind(account_id)
            .bind(subscription_id)
            .bind(resource)
            .bind(client_state)
            .bind(expires_at)
            .bind(tenant_id)
            .bind(owner_user_id)
            .bind(change_types)
            .execute(&mut *tx)
            .await
        }
    }
    .map_err(|e| ConnectorError::Internal(format!("subscription upsert: {e}")))?;
    tx.commit()
        .await
        .map_err(|e| ConnectorError::Internal(format!("subscription upsert commit: {e}")))?;
    Ok(())
}

/// Look up `(account_id, client_state, resource, tenant_id, owner_user_id)`
/// for an inbound notification's subscription id. `None` for an
/// unknown/stale subscription. The resource lets the webhook derive which
/// folder the message is in without an extra Graph call.
///
/// `owner_user_id` (m365 enterprise train PR 3.3, migration `0190`/`0068`):
/// `None` for an account-wide (Personal-plane mail, or any Teams/transcript)
/// subscription; `Some(uid)` for an org member's own mail subscription — the
/// mailbox `/me` actually resolves to for whichever token created it. Lets
/// the webhook pick the RIGHT member's token to fetch the changed message
/// with, before it has any other way to know who that is (`routes::
/// graph_webhook::process_notification`'s only other owner signal —
/// `microsoft::watched_conversation_graph`'s `user_id` — is not available
/// until AFTER the message is already fetched).
///
/// RLS bootstrap (org-tenancy conversion): the Graph change-notification
/// webhook authenticates by the provider's own signature/validation token, not
/// by a session — it arrives knowing ONLY a bare `subscription_id`, so the
/// owning account has to be DISCOVERED before any scope can be opened. That
/// first hop goes through `resolve_graph_subscription_account_id()`, the narrow
/// `SECURITY DEFINER` resolver from migration
/// `0086_webhook_share_bootstrap_functions` (bare `account_id` only — never
/// `client_state`, the notification-validation secret). The row's actual
/// contents are then read back under a proper `begin_account_scoped`
/// transaction for that account.
pub(crate) async fn subscription_by_id(
    pool: &PgPool,
    subscription_id: &str,
) -> Result<Option<(Uuid, String, String, String, Option<Uuid>)>, ConnectorError> {
    let account_id: Option<Uuid> =
        sqlx::query_scalar("SELECT resolve_graph_subscription_account_id($1)")
            .bind(subscription_id)
            .fetch_one(pool)
            .await
            .map_err(|e| ConnectorError::Internal(format!("subscription_by_id resolve: {e}")))?;
    let Some(account_id) = account_id else {
        return Ok(None);
    };
    subscription_by_id_for_account(pool, account_id, subscription_id).await
}

/// [`subscription_by_id`]'s second hop: read the subscription row under
/// `account_id`'s scope on `pool`. Used directly for an ORG account, whose
/// `graph_subscriptions` row lives on the org's tenant database — the caller
/// found the account through [`mail_route_account`] instead of the
/// control-plane `SECURITY DEFINER` resolver (which only sees control rows).
pub(crate) async fn subscription_by_id_for_account(
    pool: &PgPool,
    account_id: Uuid,
    subscription_id: &str,
) -> Result<Option<(Uuid, String, String, String, Option<Uuid>)>, ConnectorError> {
    let mut tx = scoped_tx(pool, account_id, "subscription_by_id").await?;
    let row: Option<(Uuid, String, String, String, Option<Uuid>)> = sqlx::query_as(
        "SELECT account_id, client_state, resource, tenant_id, owner_user_id \
         FROM graph_subscriptions WHERE subscription_id = $1 AND account_id = $2",
    )
    .bind(subscription_id)
    .bind(account_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("subscription_by_id: {e}")))?;
    tx.rollback().await.ok();
    Ok(row)
}

/// Control-plane pointer `subscription_id -> account_id` for an ORG
/// account's mail subscription (migration `0270_graph_mail_routes`). The
/// webhook only ever sees a bare subscription id and can only query the
/// control pool; without this an org member's mail notification resolves to
/// "unknown subscription" and is dropped. `None` for an unrouted id.
pub(crate) async fn mail_route_account(
    control_pool: &PgPool,
    subscription_id: &str,
) -> Result<Option<Uuid>, ConnectorError> {
    sqlx::query_scalar("SELECT account_id FROM graph_mail_routes WHERE subscription_id = $1")
        .bind(subscription_id)
        .fetch_optional(control_pool)
        .await
        .map_err(|e| ConnectorError::Internal(format!("mail_route_account: {e}")))
}

/// The hex SHA-256 of a subscription's `clientState`, as kept on its
/// control-plane mail route (`graph_mail_routes.client_state_sha256`,
/// migration `0403`) so the webhook can check an org mail notification's
/// `clientState` without the tenant hop. A `clientState` is 256 random bits
/// ([`new_subscription_client_state`]), so a preimage search on its hash is
/// infeasible and no key is needed; the secret itself stays on the tenant
/// row, as migration `0270` promised.
pub(crate) fn client_state_sha256(client_state: &str) -> String {
    use sha2::{Digest, Sha256};
    hex::encode(Sha256::digest(client_state.as_bytes()))
}

/// What the control plane knows about the org mail subscriptions among
/// `ids`: `(subscription_id, hash)` for every route among them, in one
/// query. An id with no route is absent; a route written before migration
/// `0401` has `None` for its hash, which the next [`sync_mail_routes`]
/// fills in. The webhook's own cheap authenticity check for a batch
/// (`routes::graph_webhook::route_secrets`).
pub(crate) async fn mail_route_client_state_hashes(
    control_pool: &PgPool,
    ids: &[String],
) -> Result<Vec<(String, Option<String>)>, ConnectorError> {
    sqlx::query_as("SELECT subscription_id, client_state_sha256 FROM graph_mail_routes WHERE subscription_id = ANY($1)")
        .bind(ids)
        .fetch_all(control_pool)
        .await
        .map_err(|e| ConnectorError::Internal(format!("mail_route_client_state_hashes: {e}")))
}

/// The account each personal subscription among `ids` belongs to, through
/// the control-plane `SECURITY DEFINER` resolver, in one query; ids no
/// subscription carries are absent.
pub(crate) async fn subscription_accounts(
    pool: &PgPool,
    ids: &[String],
) -> Result<Vec<(String, Uuid)>, ConnectorError> {
    let rows: Vec<(String, Option<Uuid>)> = sqlx::query_as(
        "SELECT s.id, resolve_graph_subscription_account_id(s.id) FROM unnest($1::text[]) AS s(id)",
    )
    .bind(ids)
    .fetch_all(pool)
    .await
    .map_err(|e| ConnectorError::Internal(format!("subscription_accounts resolve: {e}")))?;
    Ok(rows.into_iter().filter_map(|(id, account_id)| account_id.map(|a| (id, a))).collect())
}

/// `(subscription_id, client_state)` for every subscription among `ids`
/// under `account_id`'s scope, in one scoped read.
pub(crate) async fn subscription_client_states_for_account(
    pool: &PgPool,
    account_id: Uuid,
    ids: &[String],
) -> Result<Vec<(String, String)>, ConnectorError> {
    let mut tx = scoped_tx(pool, account_id, "subscription_client_states").await?;
    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT subscription_id, client_state FROM graph_subscriptions \
         WHERE account_id = $1 AND subscription_id = ANY($2)",
    )
    .bind(account_id)
    .bind(ids)
    .fetch_all(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("subscription_client_states: {e}")))?;
    tx.rollback().await.ok();
    Ok(rows)
}

/// Make `graph_mail_routes` (control) mirror the MAIL subscriptions currently
/// stored for `account_id` on `content_pool` (the org's tenant pool):
/// upsert a route per live mail subscription and drop routes for ids that no
/// longer exist (renewal-drop, `subscriptionRemoved` recreation). Idempotent;
/// called after every opt-in ingest and every renewal tick. Mail only — Teams
/// and transcript resources have their own routing and must never be routed
/// here. Returns the number of routes now registered.
pub(crate) async fn sync_mail_routes(
    content_pool: &PgPool,
    control_pool: &PgPool,
    account_id: Uuid,
) -> Result<usize, ConnectorError> {
    // Serialize syncs per account: without this a slow sync that read the old
    // subscription id could prune the route a concurrent sync just wrote for
    // the replacement. The control tx is held through upserts + prune, so a
    // later sync reads tenant ids only after the earlier one committed.
    let mut ctl = control_pool
        .begin()
        .await
        .map_err(|e| ConnectorError::Internal(format!("sync_mail_routes begin: {e}")))?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtext($1::text))")
        .bind(account_id)
        .execute(&mut *ctl)
        .await
        .map_err(|e| ConnectorError::Internal(format!("sync_mail_routes lock: {e}")))?;
    let mut tx = scoped_tx(content_pool, account_id, "sync_mail_routes").await?;
    let rows: Result<Vec<(String, String)>, _> = sqlx::query_as(
        "SELECT subscription_id, client_state FROM graph_subscriptions \
         WHERE account_id = $1 AND resource = ANY($2)",
    )
    .bind(account_id)
    .bind(WATCHED_RESOURCES.iter().map(|r| r.to_string()).collect::<Vec<_>>())
    .fetch_all(&mut *tx)
    .await;
    tx.rollback().await.ok();
    let rows = rows.map_err(|e| ConnectorError::Internal(format!("sync_mail_routes read: {e}")))?;
    let ids: Vec<String> = rows.iter().map(|(id, _)| id.clone()).collect();

    // The hash (never the secret) rides along so the webhook can verify an
    // org mail notification's `clientState` from the control plane alone
    // (`client_state_sha256`'s doc).
    for (id, client_state) in &rows {
        sqlx::query(
            "INSERT INTO graph_mail_routes (subscription_id, account_id, client_state_sha256) \
             VALUES ($1, $2, $3) \
             ON CONFLICT (subscription_id) DO UPDATE \
             SET account_id = EXCLUDED.account_id, client_state_sha256 = EXCLUDED.client_state_sha256",
        )
        .bind(id)
        .bind(account_id)
        .bind(client_state_sha256(client_state))
        .execute(&mut *ctl)
        .await
        .map_err(|e| ConnectorError::Internal(format!("sync_mail_routes upsert: {e}")))?;
    }
    sqlx::query(
        "DELETE FROM graph_mail_routes WHERE account_id = $1 AND NOT (subscription_id = ANY($2))",
    )
    .bind(account_id)
    .bind(&ids)
    .execute(&mut *ctl)
    .await
    .map_err(|e| ConnectorError::Internal(format!("sync_mail_routes prune: {e}")))?;
    ctl.commit()
        .await
        .map_err(|e| ConnectorError::Internal(format!("sync_mail_routes commit: {e}")))?;
    Ok(ids.len())
}

/// Derive the well-known folder name from a subscription resource string, e.g.
/// `me/mailFolders('inbox')/messages` → `inbox`. `None` if not a folder resource.
pub(crate) fn folder_name_from_resource(resource: &str) -> Option<String> {
    let start = resource.find("mailFolders('")? + "mailFolders('".len();
    let rest = &resource[start..];
    let end = rest.find("')")?;
    Some(rest[..end].to_string())
}

// ── Backfill helpers (CS-118) ─────────────────────────────────────────────────

/// Max messages scanned in ONE call during a workflow candidate scan — a
/// per-call page budget, not a hard ceiling on the mailbox: hitting it now
/// produces an explicit, persisted `outlook_workflows.backfill_complete =
/// false` (see [`BackfillPage`]) rather than the old silent-drop-the-rest
/// behavior, and `workers::graph_subscriptions::tick_backfill_continue`
/// re-runs the scan until it reports `complete`. Raised from the old
/// hard-cap value (1000) now that a resumed scan converges instead of losing
/// data permanently.
///
/// Memory note: `MESSAGE_SELECT` includes `body`, so one call accumulates up
/// to `BACKFILL_SCAN_CAP` complete message bodies in a single `Vec` before
/// returning, inside a `tokio::spawn`'d task
/// (`activate_workflow_inner`/`continue_workflow_backfills`), and the
/// continuation tick can run one such task per account per sweep. Peak
/// resident memory therefore scales with `BACKFILL_SCAN_CAP × body size ×
/// concurrent accounts` — accepted here because the resumability win
/// (fewer round trips to converge a large mailbox) is judged worth it, but
/// raise this further only with that multiplier in mind; consider streaming
/// pages into the ingest path directly if it ever needs to grow again.
const BACKFILL_SCAN_CAP: usize = 2000;

/// Well-known mail folders we map message `parentFolderId`s back to, for folder
/// matching during backfill.
const WELL_KNOWN_FOLDERS: [&str; 6] = [
    "inbox",
    "sentitems",
    "drafts",
    "archive",
    "deleteditems",
    "junkemail",
];

#[derive(Debug, Deserialize)]
struct GraphFolder {
    id: String,
    #[serde(rename = "totalItemCount")]
    total_item_count: Option<i64>,
}

/// Human display label for a well-known folder key (see [`WELL_KNOWN_FOLDERS`]).
/// `None` for anything else (e.g. a raw Graph folder id persisted by an older
/// workflow) — callers fall back to their own presentation rather than paying
/// for a Graph round-trip to resolve it here (this backs a hot list endpoint).
pub(crate) fn well_known_folder_label(key: &str) -> Option<&'static str> {
    match key.to_ascii_lowercase().as_str() {
        "inbox" => Some("Inbox"),
        "sentitems" => Some("Sent"),
        "drafts" => Some("Drafts"),
        "archive" => Some("Archive"),
        "deleteditems" => Some("Deleted Items"),
        "junkemail" => Some("Junk"),
        _ => None,
    }
}

/// Resolve well-known folder names → their folder ids, returning a reverse map
/// (`folderId` → `wellKnownName`) so backfill candidates' `parentFolderId` can be
/// named. Best-effort: folders that don't resolve are simply omitted.
pub(crate) async fn resolve_well_known_folders(
    http: &reqwest::Client,
    access_token: &str,
) -> std::collections::HashMap<String, String> {
    let mut map = std::collections::HashMap::new();
    for name in WELL_KNOWN_FOLDERS {
        if let Ok(Some(id)) = resolve_folder_id(http, access_token, name).await {
            map.insert(id, name.to_string());
        }
    }
    map
}

/// Resolve a single well-known folder name (e.g. `inbox`, `msgfolderroot`) to its
/// folder id. `Ok(None)` if it can't be resolved (best-effort).
pub(crate) async fn resolve_folder_id(
    http: &reqwest::Client,
    access_token: &str,
    well_known: &str,
) -> Result<Option<String>, ConnectorError> {
    let url = format!(
        "{GRAPH}/me/mailFolders/{}?$select=id",
        urlencoding::encode(well_known)
    );
    match graph_get(http, access_token, &url).await {
        Ok(v) => Ok(serde_json::from_value::<GraphFolder>(v).ok().map(|f| f.id)),
        Err(e) => {
            tracing::debug!(%well_known, error = %e, "could not resolve folder id");
            Ok(None)
        }
    }
}

/// Page messages from `initial_url` (or `resume_cursor`, if resuming a
/// previously budget-capped scan), following `@odata.nextLink` up to
/// [`BACKFILL_SCAN_CAP`] per call. Shared by the recent-scan fallback and
/// search-folder listing. Returns a [`BackfillPage`] rather than silently
/// dropping anything past the budget.
async fn paginate_messages(
    http: &reqwest::Client,
    access_token: &str,
    initial_url: String,
    resume_cursor: Option<&str>,
) -> Result<BackfillPage<GraphMessage>, ConnectorError> {
    let mut url = resume_cursor.map(str::to_string).unwrap_or(initial_url);
    let mut messages = Vec::new();
    loop {
        let list: GraphList<GraphMessage> =
            serde_json::from_value(graph_get(http, access_token, &url).await?)
                .map_err(|e| ConnectorError::Exchange(format!("messages decode: {e}")))?;
        messages.extend(list.value);
        match list.next_link {
            Some(next) if messages.len() < BACKFILL_SCAN_CAP => url = next,
            Some(next) => {
                return Ok(BackfillPage {
                    items: messages,
                    next_cursor: Some(next),
                    complete: false,
                })
            }
            None => break,
        }
    }
    Ok(BackfillPage {
        items: messages,
        next_cursor: None,
        complete: true,
    })
}

/// Fallback backfill candidate source: the mailbox's most-recent messages
/// (newest first). Used only when a `mailSearchFolder` can't be built/created
/// for the filter; the in-process matcher still decides the final matches.
pub(crate) async fn list_recent_messages(
    http: &reqwest::Client,
    access_token: &str,
    resume_cursor: Option<&str>,
) -> Result<BackfillPage<GraphMessage>, ConnectorError> {
    paginate_messages(
        http,
        access_token,
        format!(
            "{GRAPH}/me/messages?$top=50&$orderby=receivedDateTime%20desc&$select={MESSAGE_SELECT}"
        ),
        resume_cursor,
    )
    .await
}

/// Everything mode (m365 enterprise train PR 3.4): the whole-mailbox
/// `sync()`-shaped pull the contract's carve-out permits ONLY when the
/// mailbox owner has opted in themselves (`docs/connectors/
/// CONNECTOR_CONTRACT.md` §4/§7, `workers::outlook_everything_mode`). A thin
/// wrapper over [`list_recent_messages`] (the SAME `/me/messages` whole-
/// mailbox pagination CS-118 workflow backfill already uses — no new Graph
/// surface, no new normalisation) plus [`message_to_item`], so callers get
/// [`NormalizedItem`]s directly rather than reaching into `GraphMessage`
/// themselves. `message_to_item` already runs every body through the shared
/// HTML normaliser + Outlook reply-chain/signature trimming (m365 PR 2.0) —
/// nothing here duplicates that.
///
/// Deliberately NOT part of the [`ConnectorProvider`] trait: `sync()` is
/// shared with `workers::microsoft_calendar_sync`'s calendar-only tick, and
/// overloading it here would make every calendar tick also attempt a mail
/// pull for any owner who has everything mode on — a coupling this PR
/// avoids by keeping the two workers, and the two connector entry points
/// they call, structurally independent (see `workers::
/// outlook_everything_mode`'s own module doc).
pub(crate) async fn fetch_everything_mode_mail(
    http: &reqwest::Client,
    access_token: &str,
    resume_cursor: Option<&str>,
) -> Result<BackfillPage<NormalizedItem>, ConnectorError> {
    let page = list_recent_messages(http, access_token, resume_cursor).await?;
    Ok(BackfillPage {
        items: page.items.into_iter().map(message_to_item).collect(),
        next_cursor: page.next_cursor,
        complete: page.complete,
    })
}

/// List the messages currently in a (search or regular) folder, paginated +
/// capped. Search-folder members carry their *original* `parentFolderId`, so
/// folder matching still works on them.
pub(crate) async fn list_messages_in_folder(
    http: &reqwest::Client,
    access_token: &str,
    folder_id: &str,
    resume_cursor: Option<&str>,
) -> Result<BackfillPage<GraphMessage>, ConnectorError> {
    paginate_messages(
        http,
        access_token,
        format!(
            "{GRAPH}/me/mailFolders/{}/messages?$top=50&$select={MESSAGE_SELECT}",
            urlencoding::encode(folder_id)
        ),
        resume_cursor,
    )
    .await
}

/// Create a `mailSearchFolder` under the Search Folders root with the given
/// OData `filter_query` over `source_folder_ids` (nested folders included), and
/// return its id. Exchange evaluates the query natively across the whole
/// mailbox — the native, accurate way to backfill matches (ADR-0008 D6).
pub(crate) async fn create_search_folder(
    http: &reqwest::Client,
    access_token: &str,
    display_name: &str,
    filter_query: &str,
    source_folder_ids: &[String],
) -> Result<String, ConnectorError> {
    let body = serde_json::json!({
        "@odata.type": "microsoft.graph.mailSearchFolder",
        "displayName": display_name,
        "includeNestedFolders": true,
        "sourceFolderIds": source_folder_ids,
        "filterQuery": filter_query,
    });
    let resp = graph_send(|| {
        http.post(format!("{GRAPH}/me/mailFolders/searchfolders/childFolders"))
            .bearer_auth(access_token)
            .json(&body)
    })
    .await?;
    if !resp.status().is_success() {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        return Err(ConnectorError::upstream(status, format!(
            "search folder create failed: {status}: {}",
            text.chars().take(300).collect::<String>()
        )));
    }
    let folder: GraphFolder = resp
        .json()
        .await
        .map_err(|e| ConnectorError::Exchange(format!("search folder parse: {e}")))?;
    Ok(folder.id)
}

/// Search folders populate asynchronously. Poll the folder's item count until it
/// stops growing (stable across two reads) or a short budget elapses, so the
/// subsequent listing sees a populated folder rather than an empty one.
pub(crate) async fn await_folder_population(
    http: &reqwest::Client,
    access_token: &str,
    folder_id: &str,
) {
    let mut last: i64 = -1;
    for _ in 0..6 {
        let count = folder_total_count(http, access_token, folder_id)
            .await
            .unwrap_or(-1);
        if count >= 0 && count == last {
            return; // stabilised
        }
        last = count;
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
}

async fn folder_total_count(
    http: &reqwest::Client,
    access_token: &str,
    folder_id: &str,
) -> Option<i64> {
    // Include `id` in the projection: GraphFolder.id is required, so selecting
    // only totalItemCount would fail to deserialize.
    let url = format!(
        "{GRAPH}/me/mailFolders/{}?$select=id,totalItemCount",
        urlencoding::encode(folder_id)
    );
    graph_get(http, access_token, &url)
        .await
        .ok()
        .and_then(|v| serde_json::from_value::<GraphFolder>(v).ok())
        .and_then(|f| f.total_item_count)
}

/// Delete a mail folder (used to clean up a one-shot backfill search folder).
pub(crate) async fn delete_mail_folder(
    http: &reqwest::Client,
    access_token: &str,
    folder_id: &str,
) -> Result<(), ConnectorError> {
    let resp = graph_send(|| {
        http.delete(format!(
            "{GRAPH}/me/mailFolders/{}",
            urlencoding::encode(folder_id)
        ))
        .bearer_auth(access_token)
    })
    .await?;
    if !resp.status().is_success() {
        return Err(ConnectorError::upstream(resp.status(), format!("folder delete failed: {}", resp.status())));
    }
    Ok(())
}

/// Subscriptions expiring before `cutoff` — the renewal worker's work queue.
/// Returns `(subscription_id, account_id, resource, tenant_id, owner_user_id)`;
/// the resource drives the (mail vs Teams) renewal lifetime, and
/// `owner_user_id` (m365 enterprise train PR 3.3, migration `0190`/`0068`)
/// drives WHICH token a delegated (mail) renewal loads — see
/// [`subscription_by_id`]'s own doc comment for the identical field.
///
/// **Deliberately cross-account** (see this module's RLS-scoping note): a
/// renewal sweep is by definition an every-account query, so the *discovery*
/// hop goes through `resolve_expiring_subscriptions()`, the narrow `SECURITY
/// DEFINER` resolver from migration `0086_webhook_share_bootstrap_functions`,
/// which returns only `(subscription_id, account_id)` — no `resource`, no
/// `tenant_id`, no `owner_user_id`, and never `client_state`. The
/// per-subscription detail is then read back under each owner's own
/// `begin_account_scoped` transaction, so the only thing that ever crosses an
/// account boundary here is a pair of ids. Result ordering and tuple shape
/// (aside from the new trailing field) are unchanged for callers.
#[allow(clippy::type_complexity)]
pub(crate) async fn expiring_subscriptions(
    pool: &PgPool,
    cutoff: DateTime<Utc>,
) -> Result<Vec<(String, Uuid, String, String, Option<Uuid>)>, ConnectorError> {
    let due: Vec<(String, Uuid)> =
        sqlx::query_as("SELECT * FROM resolve_expiring_subscriptions($1)")
            .bind(cutoff)
            .fetch_all(pool)
            .await
            .map_err(|e| ConnectorError::Internal(format!("expiring_subscriptions: {e}")))?;

    // Per-account degradation, NOT `?`: this is a periodic sweep over every
    // account with a due subscription, so one account's transient DB failure
    // must not discard the work set for all the others. Propagating here would
    // mean a single hiccup silently skips renewal for every other due
    // subscription in the tick — and Teams/Outlook subscription TTLs are short
    // enough that missing a tick matters. A skipped account is simply re-derived
    // on the next tick, which is the same self-healing the deleted-row arm below
    // relies on.
    let mut out = Vec::with_capacity(due.len());
    for (subscription_id, account_id) in due {
        let mut tx = match scoped_tx(pool, account_id, "expiring_subscriptions").await {
            Ok(tx) => tx,
            Err(e) => {
                tracing::warn!(error = %e, %account_id, "expiring_subscriptions: scoped tx failed — skipping this account for this tick");
                continue;
            }
        };
        let row: Option<(String, String, Option<Uuid>)> = match sqlx::query_as(
            "SELECT resource, tenant_id, owner_user_id FROM graph_subscriptions WHERE subscription_id = $1",
        )
        .bind(&subscription_id)
        .fetch_optional(&mut *tx)
        .await
        {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(error = %e, %account_id, "expiring_subscriptions: detail lookup failed — skipping this subscription for this tick");
                tx.rollback().await.ok();
                continue;
            }
        };
        tx.rollback().await.ok();
        // A row deleted between the two hops simply drops out of the work set —
        // the next tick re-derives it if it still exists.
        if let Some((resource, tenant_id, owner_user_id)) = row {
            out.push((
                subscription_id,
                account_id,
                resource,
                tenant_id,
                owner_user_id,
            ));
        }
    }
    Ok(out)
}

/// The org-tenant analog of [`expiring_subscriptions`]: subscriptions
/// expiring within `cutoff` for ONE already-known account. An org tenant
/// database holds exactly one account (ADR-0007), so there is no cross-
/// account discovery problem to solve, and therefore no need for that
/// function's two-hop `SECURITY DEFINER` indirection (which exists purely to
/// survive `0083`'s cross-account boundary on the consumer plane — see this
/// module's RLS-scoping note). `owner_user_id` — see [`expiring_subscriptions`]'s
/// own doc comment for the identical field.
#[allow(clippy::type_complexity)]
pub(crate) async fn expiring_subscriptions_for_account(
    pool: &PgPool,
    account_id: Uuid,
    cutoff: DateTime<Utc>,
) -> Result<Vec<(String, String, String, Option<Uuid>)>, ConnectorError> {
    let mut tx = scoped_tx(pool, account_id, "expiring_subscriptions_for_account").await?;
    let rows = sqlx::query_as(
        "SELECT subscription_id, resource, tenant_id, owner_user_id FROM graph_subscriptions \
         WHERE account_id = $1 AND expires_at <= $2",
    )
    .bind(account_id)
    .bind(cutoff)
    .fetch_all(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("expiring_subscriptions_for_account: {e}")));
    tx.rollback().await.ok();
    rows
}

/// [`set_subscription_expiry`] for a caller that already knows the owning
/// account — the org-tenant renewal path (`workers::graph_subscriptions`'s
/// tenant-loop tick), which gets `account_id` for free from
/// [`expiring_subscriptions_for_account`] and so has no need for
/// `subscription_account_id`'s `resolve_graph_subscription_account_id` RPC —
/// a control-plane-only resolver (`0086`) with no tenant-tree counterpart.
pub(crate) async fn set_subscription_expiry_for_account(
    pool: &PgPool,
    account_id: Uuid,
    subscription_id: &str,
    expires_at: DateTime<Utc>,
) -> Result<(), ConnectorError> {
    let mut tx = scoped_tx(pool, account_id, "set_subscription_expiry_for_account").await?;
    sqlx::query(
        "UPDATE graph_subscriptions SET expires_at = $1, updated_at = now() \
         WHERE account_id = $2 AND subscription_id = $3",
    )
    .bind(expires_at)
    .bind(account_id)
    .bind(subscription_id)
    .execute(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("set_subscription_expiry_for_account: {e}")))?;
    tx.commit().await.map_err(|e| {
        ConnectorError::Internal(format!("set_subscription_expiry_for_account commit: {e}"))
    })?;
    Ok(())
}

/// [`delete_subscription_row`] for a caller that already knows the owning
/// account — see [`set_subscription_expiry_for_account`]'s doc comment for
/// why this avoids the control-only resolver.
pub(crate) async fn delete_subscription_row_for_account(
    pool: &PgPool,
    account_id: Uuid,
    subscription_id: &str,
) -> Result<(), ConnectorError> {
    let mut tx = scoped_tx(pool, account_id, "delete_subscription_row_for_account").await?;
    sqlx::query("DELETE FROM graph_subscriptions WHERE account_id = $1 AND subscription_id = $2")
        .bind(account_id)
        .bind(subscription_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| {
            ConnectorError::Internal(format!("delete_subscription_row_for_account: {e}"))
        })?;
    tx.commit().await.map_err(|e| {
        ConnectorError::Internal(format!("delete_subscription_row_for_account commit: {e}"))
    })?;
    Ok(())
}

// ── Microsoft Teams: list + backfill (CS-119) ─────────────────────────────────────

/// Top-level channel/chat messages pulled per backfill (first page only).
const TEAMS_MESSAGE_TOP: usize = 30;
/// Replies pulled per channel message.
const TEAMS_REPLY_TOP: usize = 50;

/// A team the signed-in user belongs to (for the ingest picker).
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct TeamSummary {
    pub id: String,
    #[serde(rename(deserialize = "displayName"), default)]
    pub display_name: String,
}

/// A channel within a team.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct ChannelSummary {
    pub id: String,
    #[serde(rename(deserialize = "displayName"), default)]
    pub display_name: String,
    #[serde(rename(deserialize = "membershipType"), default)]
    pub membership_type: Option<String>,
}

/// A chat (1:1, group, or meeting) the user participates in.
#[derive(Debug, Serialize)]
pub(crate) struct ChatSummary {
    pub id: String,
    pub display_name: String,
    pub chat_type: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ChatRaw {
    id: String,
    topic: Option<String>,
    #[serde(rename = "chatType")]
    chat_type: Option<String>,
}

// chatMessage shape (channels + chats share it).
#[derive(Debug, Deserialize)]
pub(crate) struct TeamsMessage {
    id: String,
    #[serde(rename = "messageType")]
    message_type: Option<String>,
    subject: Option<String>,
    #[serde(rename = "createdDateTime")]
    created_date_time: Option<DateTime<Utc>>,
    from: Option<TeamsFrom>,
    body: Option<GraphItemBody>,
}

#[derive(Debug, Deserialize)]
struct TeamsFrom {
    user: Option<TeamsUser>,
    /// Present when the message was authored by an application (a bot — incl.
    /// our own prompt/confirmation cards). Used to skip non-human messages.
    #[serde(default)]
    application: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct TeamsUser {
    /// AAD object id (GUID) of the author — a stable per-user handle that
    /// survives a missing/renamed `displayName`.
    id: Option<String>,
    #[serde(rename = "displayName")]
    display_name: Option<String>,
}

/// Graph `itemBody` — `{ contentType, content }`. Shared by Outlook messages
/// (full body) and Teams chatMessages.
#[derive(Debug, Deserialize)]
struct GraphItemBody {
    #[serde(rename = "contentType")]
    content_type: Option<String>,
    #[serde(default)]
    content: String,
}

/// Reduce an `itemBody` to plain text — HTML stripped, plain text passed
/// through. Delegates to the shared normaliser (gsuite Train G2 PR 2,
/// `connectors::email_body::from_graph_body`) — this is now a pure
/// pass-through wrapper kept so call sites don't need to know about
/// `GraphItemBody`'s field shape. No behavior change: same content-type
/// check, same `html_to_text` core, moved verbatim.
///
/// Used by Teams chat/channel messages ([`teams_message_to_item`]) and by
/// Teams transcripts — i.e. everywhere EXCEPT Outlook mail, which uses
/// [`body_to_text_boundaried`] instead (see its doc for why).
fn body_to_text(body: &GraphItemBody) -> String {
    super::email_body::from_graph_body(body.content_type.as_deref(), &body.content)
}

/// Outlook-**mail**-only sibling of [`body_to_text`]: delegates to
/// [`super::email_body::from_graph_body_boundaried`] (block-boundary-
/// preserving HTML conversion) instead of `from_graph_body`. Added in m365
/// PR 2.0 (CodeRabbit finding F2 on #691) so
/// [`super::email_body::trim_outlook_reply_chain`]'s line-by-line marker
/// scan — run on this function's output in [`message_to_item`] — can
/// actually see Outlook's `<div>`-per-line reply markers
/// (`"-----Original Message-----"`, mobile signatures) on a line by
/// themselves, instead of them being flattened into the middle of a
/// collapsed single-line body. See `from_graph_body_boundaried`'s own doc
/// for the full reasoning, including why this deliberately changes
/// Outlook mail's atom text shape (adds newlines) and why that's an
/// accepted change for this PR. Called by [`message_to_item`] and by
/// [`shape_graph_event`] (a Calendar event's description is the same
/// `<div>`-per-line Outlook HTML) — Teams keeps using [`body_to_text`].
fn body_to_text_boundaried(body: &GraphItemBody) -> String {
    super::email_body::from_graph_body_boundaried(body.content_type.as_deref(), &body.content)
}

/// Teams the signed-in user is a member of.
pub(crate) async fn list_joined_teams(
    http: &reqwest::Client,
    access_token: &str,
) -> Result<Vec<TeamSummary>, ConnectorError> {
    let url = format!("{GRAPH}/me/joinedTeams?$select=id,displayName");
    list_all_scope_items(http, access_token, url).await
}

/// A member of the account who is on this team and has no live Microsoft
/// grant. Uses the same connect-nudge claim as the hourly worker, so the
/// two cannot both email them inside one cooldown.
async fn email_member_missing_teams(
    state: &crate::AppState,
    control: &PgPool,
    account_id: Uuid,
    clerk_org_id: &str,
    user_id: Uuid,
    email: &str,
) -> bool {
    let connected: Option<bool> = sqlx::query_scalar(
        "SELECT TRUE FROM oauth_tokens \
         WHERE account_id = $1 AND provider = 'microsoft' \
           AND owner_user_id = $2 AND credential_invalid_at IS NULL",
    )
    .bind(account_id)
    .bind(user_id)
    .fetch_optional(control)
    .await
    .ok()
    .flatten();
    if connected.unwrap_or(false) {
        return false;
    }
    if crate::services::unmatched_recipient_email::is_suppressed_any_provider(control, email)
        .await
        .unwrap_or(true)
    {
        return false;
    }
    let mut tx = match crate::tenancy::scope::begin_tenant_system_scoped(control, account_id).await
    {
        Ok(tx) => tx,
        Err(_) => return false,
    };
    let claimed =
        match crate::services::unmatched_recipient_email::claim_member_connect_notice_slot(
            &mut *tx, account_id, email,
        )
        .await
        {
            Ok(claimed) => claimed,
            Err(_) => {
                tx.rollback().await.ok();
                return false;
            }
        };
    let Some(tokens) = claimed else {
        tx.rollback().await.ok();
        return false;
    };
    let web_base = state.org_web_base_url(control, clerk_org_id).await;
    if !crate::services::unmatched_recipient_email::send_connect_nudge_email(
        state,
        email,
        &web_base,
        &tokens.unsubscribe_token,
    )
    .await
    {
        tx.rollback().await.ok();
        return false;
    }
    tx.commit().await.is_ok()
}

/// A CoralSwarm user on this team who is not a member of the account.
/// The join token carries no Slack membership class, so auto-add does not
/// treat a Teams roster hit as a Slack full member. Personal accounts have
/// no org to join.
async fn email_known_teammate_to_join(
    state: &crate::AppState,
    control: &PgPool,
    account_id: Uuid,
    clerk_org_id: Option<&str>,
    team_id: &str,
    user_id: Uuid,
    email: &str,
) -> bool {
    let Some(clerk_org_id) = clerk_org_id else {
        return false;
    };
    let member: Option<bool> = sqlx::query_scalar(
        "SELECT TRUE FROM account_members WHERE account_id = $1 AND user_id = $2",
    )
    .bind(account_id)
    .bind(user_id)
    .fetch_optional(control)
    .await
    .ok()
    .flatten();
    if member.unwrap_or(false) {
        return email_member_missing_teams(
            state,
            control,
            account_id,
            clerk_org_id,
            user_id,
            email,
        )
        .await;
    }
    if crate::clerk_provision::invite_suppressed_by_tombstone(
        control,
        Some(clerk_org_id),
        Some(user_id),
        Some(email),
    )
    .await
    .unwrap_or(true)
    {
        return false;
    }
    let already = match crate::tenancy::scope::begin_tenant_system_scoped(control, account_id).await
    {
        Ok(mut tx) => {
            let found: Result<bool, sqlx::Error> = sqlx::query_scalar(
                "SELECT EXISTS ( \
                     SELECT 1 FROM org_join_requests \
                      WHERE account_id = $1 AND user_id = $2 \
                        AND (decision IS NULL OR decision = 'rejected') \
                 )",
            )
            .bind(account_id)
            .bind(user_id)
            .fetch_one(&mut *tx)
            .await;
            tx.rollback().await.ok();
            found.unwrap_or(true)
        }
        Err(_) => true,
    };
    if already {
        return false;
    }
    let Some(secret) = crate::services::unmatched_recipient_email::join_notice_secret() else {
        return false;
    };
    let mut tx = match crate::tenancy::scope::begin_tenant_system_scoped(control, account_id).await
    {
        Ok(tx) => tx,
        Err(_) => return false,
    };
    let claimed = match crate::services::unmatched_recipient_email::claim_join_notice_slot(
        &mut *tx,
        account_id,
        "microsoft_join",
        team_id,
        team_id,
        email,
    )
    .await
    {
        Ok(claimed) => claimed,
        Err(_) => {
            tx.rollback().await.ok();
            return false;
        }
    };
    let Some(tokens) = claimed else {
        tx.rollback().await.ok();
        return false;
    };
    let Ok(join_token) = crate::services::unmatched_recipient_email::mint_join_notice_token(
        &secret,
        account_id,
        user_id,
        clerk_org_id,
        email,
        Some(team_id),
        Some(team_id),
        None,
        crate::connectors::slack::MembershipClass::Unavailable,
    ) else {
        tx.rollback().await.ok();
        return false;
    };
    let join_web_base = state.personal_web_base_url();
    let join_url = format!(
        "{join_web_base}/join/{}?t={}",
        urlencoding::encode(clerk_org_id),
        urlencoding::encode(&join_token)
    );
    if !crate::services::unmatched_recipient_email::send_join_org_email(
        state,
        email,
        "your team",
        &join_url,
        &join_web_base,
        &tokens.unsubscribe_token,
    )
    .await
    {
        tx.rollback().await.ok();
        return false;
    }
    tx.commit().await.is_ok()
}

/// Unknown address on a team roster — "connect your own". The notice row is
/// claimed under `provider = 'microsoft'`, never a hardcoded `'slack'`.
/// Same claim-then-send order as the connect-time walk this was lifted from.
async fn email_unknown_teams_address(
    state: &crate::AppState,
    control: &PgPool,
    account_id: Uuid,
    clerk_org_id: Option<&str>,
    team_id: &str,
    email: &str,
    web_base: &str,
) -> bool {
    if crate::services::unmatched_recipient_email::is_suppressed_any_provider(control, email)
        .await
        .unwrap_or(true)
    {
        return false;
    }
    if crate::clerk_provision::invite_suppressed_by_tombstone(
        control,
        clerk_org_id,
        None,
        Some(email),
    )
    .await
    .unwrap_or(true)
    {
        return false;
    }
    let mut tx = match crate::tenancy::scope::begin_tenant_system_scoped(control, account_id).await
    {
        Ok(tx) => tx,
        Err(e) => {
            tracing::warn!(error = %e, "teams invite: claim transaction failed");
            return false;
        }
    };
    let claimed = match crate::services::unmatched_recipient_email::claim_notice_slot(
        &mut *tx,
        account_id,
        "microsoft",
        team_id,
        team_id,
        email,
    )
    .await
    {
        Ok(claimed) => claimed,
        Err(e) => {
            tx.rollback().await.ok();
            tracing::warn!(error = %e, "teams invite: cooldown claim failed");
            return false;
        }
    };
    let Some(tokens) = claimed else {
        tx.rollback().await.ok();
        return false;
    };
    if tx.commit().await.is_err() {
        return false;
    }
    crate::services::unmatched_recipient_email::send_nudge_email(
        state,
        email,
        web_base,
        &tokens.unsubscribe_token,
        &tokens.prefill_token,
    )
    .await
}

/// Email one resolved team roster: unknown addresses connect, known users
/// who are not members are invited to join, and members with no live
/// Microsoft token get the connect nudge. `known` is already identity-matched
/// by the resolver; [`email_known_teammate_to_join`] splits the last two.
pub(crate) async fn email_team_roster_resolution(
    state: &crate::AppState,
    control: &PgPool,
    account_id: Uuid,
    team_id: &str,
    resolution: &TeamRosterResolution,
) -> usize {
    let clerk_org_id: Option<String> =
        sqlx::query_scalar("SELECT clerk_org_id FROM accounts WHERE id = $1")
            .bind(account_id)
            .fetch_optional(control)
            .await
            .ok()
            .flatten();
    let web_base = match clerk_org_id.as_deref() {
        Some(org_id) => state.org_web_base_url(control, org_id).await,
        None => state.personal_web_base_url(),
    };
    let mut sent = 0usize;
    for email in &resolution.unmatched_emails {
        if sent >= 25 {
            break;
        }
        if email_unknown_teams_address(
            state,
            control,
            account_id,
            clerk_org_id.as_deref(),
            team_id,
            email,
            &web_base,
        )
        .await
        {
            sent += 1;
        }
    }
    for known in &resolution.known {
        if sent >= 25 {
            break;
        }
        if email_known_teammate_to_join(
            state,
            control,
            account_id,
            clerk_org_id.as_deref(),
            team_id,
            known.user_id,
            &known.email,
        )
        .await
        {
            sent += 1;
        }
    }
    sent
}

const TEAMS_INVITE_CONTAINER_CAP: usize = 10;
const TEAMS_INVITE_EMAIL_CAP: usize = 25;

/// Address [`invite_addresses_on_roster`] and [`resolve_roster_members_from`]
/// both consume. `None` means the row stays Unavailable: it is not an
/// unmatched address, and it is not a guest verdict.
fn roster_invite_address(member: &TeamRosterMember) -> Option<String> {
    member
        .email
        .as_deref()
        .map(str::trim)
        .filter(|email| !email.is_empty())
        .map(|email| email.to_lowercase())
}

/// First `otherMails` entry that can be invited. Blank strings and values
/// with no `@` are not addresses.
fn first_usable_other_mail<'a>(mails: impl IntoIterator<Item = &'a str>) -> Option<String> {
    mails.into_iter().find_map(|raw| {
        let trimmed = raw.trim();
        if trimmed.is_empty() || !trimmed.contains('@') {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
}

/// `GET /users/{id}?$select=otherMails`. No `userType`. Tests redirect the
/// host through the same graph root the chat-roster tests use.
fn user_other_mails_url(aad_user_id: &str) -> String {
    format!(
        "{}/users/{}?$select=otherMails",
        chat_roster_graph_root(),
        urlencoding::encode(aad_user_id)
    )
}

#[derive(Deserialize)]
struct UserOtherMails {
    #[serde(default, rename = "otherMails")]
    other_mails: Option<Vec<String>>,
}

async fn fetch_other_mail(
    http: &reqwest::Client,
    access_token: &str,
    aad_user_id: &str,
) -> Result<Option<String>, ConnectorError> {
    let url = user_other_mails_url(aad_user_id);
    let value = graph_get(http, access_token, &url).await?;
    let parsed: UserOtherMails = serde_json::from_value(value)
        .map_err(|e| ConnectorError::Exchange(format!("otherMails decode: {e}")))?;
    Ok(first_usable_other_mail(
        parsed
            .other_mails
            .unwrap_or_default()
            .iter()
            .map(String::as_str),
    ))
}

/// When a non-guest roster row has no email, read `otherMails` with the same
/// delegated token that read the roster. `User.Read.All` exposes that
/// property; this call does not request `userType` and does not mint an app
/// token. A guest is not looked up. A failed, forbidden, or empty read leaves
/// `email` unset, so the row stays Unavailable.
async fn fill_missing_emails_from_other_mails(
    http: &reqwest::Client,
    access_token: &str,
    home_tenant_id: &str,
    roster: &mut [TeamRosterMember],
) {
    for member in roster.iter_mut() {
        if roster_invite_address(member).is_some() {
            continue;
        }
        if teams_roster_member_is_guest(member, home_tenant_id) {
            continue;
        }
        match fetch_other_mail(http, access_token, &member.aad_user_id).await {
            Ok(Some(address)) => member.email = Some(address),
            Ok(None) => {}
            Err(error) => {
                tracing::debug!(
                    %error,
                    aad_user_id = %member.aad_user_id,
                    "teams roster: otherMails read failed; row stays unavailable"
                );
            }
        }
    }
}

/// Email one already-fetched roster. Guests are skipped by
/// [`roster_member_disposition`] — an in-tenant `guest` role, or a
/// `tenantId` that differs from the home tenant. A missing email is not a
/// guest and is not invited. A non-guest address recovered from `otherMails`
/// is invited like any other roster email.
async fn invite_addresses_on_roster(
    state: &crate::AppState,
    control: &PgPool,
    account_id: Uuid,
    clerk_org_id: Option<&str>,
    container_id: &str,
    home_tenant: &str,
    admit_domains: &[String],
    web_base: &str,
    roster: Vec<TeamRosterMember>,
    sent: &mut usize,
) {
    for member in roster {
        if *sent >= TEAMS_INVITE_EMAIL_CAP {
            break;
        }
        if roster_member_disposition(&member, home_tenant, admit_domains) == RosterDisposition::Skip
        {
            continue;
        }
        let Some(email) = roster_invite_address(&member) else {
            continue;
        };
        let known: Option<Uuid> =
            sqlx::query_scalar("SELECT id FROM users WHERE lower(email) = $1")
                .bind(&email)
                .fetch_optional(control)
                .await
                .ok()
                .flatten();
        if let Some(user_id) = known {
            if email_known_teammate_to_join(
                state,
                control,
                account_id,
                clerk_org_id,
                container_id,
                user_id,
                &email,
            )
            .await
            {
                *sent += 1;
            }
            continue;
        }
        if email_unknown_teams_address(
            state,
            control,
            account_id,
            clerk_org_id,
            container_id,
            &email,
            web_base,
        )
        .await
        {
            *sent += 1;
        }
    }
}

/// After a member connects, email addresses from the teams they joined and
/// the chats they are already in. Both lists use this member's delegated
/// token. Chat membership is [`list_chat_roster`]
/// (`GET /chats/{id}/members`); delegated `Chat.Read` already covers that
/// endpoint, so no app token is minted. A guest is skipped the same way a
/// team-roster guest is. A non-guest row with no email gets one delegated
/// `GET /users/{id}?$select=otherMails` on this same token before the invite
/// walk. A missing email after that read is not treated as a guest.
/// Best-effort: a Graph or mail failure skips that container. A team-list
/// failure still walks chats. Message bodies are not read.
pub(crate) async fn email_unmatched_joined_teams(
    state: &crate::AppState,
    control: &PgPool,
    account_id: Uuid,
    access_token: &str,
) {
    let clerk_org_id: Option<String> =
        sqlx::query_scalar("SELECT clerk_org_id FROM accounts WHERE id = $1")
            .bind(account_id)
            .fetch_optional(control)
            .await
            .ok()
            .flatten();
    let web_base = match clerk_org_id.as_deref() {
        Some(org_id) => state.org_web_base_url(control, org_id).await,
        None => state.personal_web_base_url(),
    };
    let home_tenant: String = sqlx::query_scalar(
        "SELECT provider_workspace_id FROM oauth_tokens \
         WHERE account_id = $1 AND provider = 'microsoft' AND credential_invalid_at IS NULL \
           AND provider_workspace_id IS NOT NULL \
         ORDER BY updated_at DESC LIMIT 1",
    )
    .bind(account_id)
    .fetch_optional(control)
    .await
    .ok()
    .flatten()
    .unwrap_or_default();
    let admit_domains = teams_guest_admit_domains(control, account_id).await;
    let mut sent = 0usize;

    match list_joined_teams(&state.http_client, access_token).await {
        Ok(teams) => {
            for team in teams.into_iter().take(TEAMS_INVITE_CONTAINER_CAP) {
                if sent >= TEAMS_INVITE_EMAIL_CAP {
                    break;
                }
                let mut roster = match list_team_roster(&state.http_client, access_token, &team.id)
                    .await
                {
                    Ok(roster) => roster,
                    Err(e) => {
                        tracing::info!(error = %e, team_id = %team.id, "teams invite: roster read failed");
                        continue;
                    }
                };
                fill_missing_emails_from_other_mails(
                    &state.http_client,
                    access_token,
                    &home_tenant,
                    &mut roster,
                )
                .await;
                invite_addresses_on_roster(
                    state,
                    control,
                    account_id,
                    clerk_org_id.as_deref(),
                    &team.id,
                    &home_tenant,
                    &admit_domains,
                    &web_base,
                    roster,
                    &mut sent,
                )
                .await;
            }
        }
        Err(e) => {
            tracing::info!(error = %e, %account_id, "teams invite: joined-teams lookup failed");
        }
    }

    if sent >= TEAMS_INVITE_EMAIL_CAP {
        return;
    }
    let chats = match list_chats_limited(
        &state.http_client,
        access_token,
        Some(TEAMS_INVITE_CONTAINER_CAP),
    )
    .await
    {
        Ok(chats) => chats,
        Err(e) => {
            tracing::info!(error = %e, %account_id, "teams invite: chat list failed");
            return;
        }
    };
    for chat in chats {
        if sent >= TEAMS_INVITE_EMAIL_CAP {
            break;
        }
        let mut roster = match list_chat_roster(&state.http_client, access_token, &chat.id).await {
            Ok(roster) => roster,
            Err(e) => {
                tracing::info!(error = %e, chat_id = %chat.id, "teams invite: chat roster read failed");
                continue;
            }
        };
        fill_missing_emails_from_other_mails(
            &state.http_client,
            access_token,
            &home_tenant,
            &mut roster,
        )
        .await;
        invite_addresses_on_roster(
            state,
            control,
            account_id,
            clerk_org_id.as_deref(),
            &chat.id,
            &home_tenant,
            &admit_domains,
            &web_base,
            roster,
            &mut sent,
        )
        .await;
    }
}

/// Queue one invite job per joined team. Uses the member's own token to
/// list teams — there is no app-token fallback. The worker re-reads each
/// roster on a delegated token and emails [`TeamRosterResolution`]'s
/// unmatched addresses. A pending job with the same key is a no-op.
pub(crate) async fn enqueue_joined_team_invite_jobs(
    http: &reqwest::Client,
    jobs_pool: &PgPool,
    account_id: Uuid,
    access_token: &str,
    tenant_id: &str,
) {
    let teams = match list_joined_teams(http, access_token).await {
        Ok(teams) => teams,
        Err(e) => {
            tracing::info!(error = %e, %account_id, "teams invite: could not list teams to enqueue");
            return;
        }
    };
    for team in teams.into_iter().take(10) {
        if let Err(e) = crate::jobs::queue::enqueue(
            jobs_pool,
            &crate::jobs::JobKind::EmailUnmatchedTeamsMembers {
                account_id,
                tenant_id: tenant_id.to_string(),
                team_id: team.id.clone(),
            },
            Some(account_id),
            Some(&format!("teams-unmatched:{account_id}:{}", team.id)),
            0,
        )
        .await
        {
            tracing::warn!(
                error = %e, %account_id, team_id = %team.id,
                "teams invite: enqueue failed"
            );
        }
    }
}

/// Channels within a team.
pub(crate) async fn list_channels(
    http: &reqwest::Client,
    access_token: &str,
    team_id: &str,
) -> Result<Vec<ChannelSummary>, ConnectorError> {
    let url = format!(
        "{GRAPH}/teams/{}/channels?$select=id,displayName,membershipType",
        urlencoding::encode(team_id)
    );
    list_all_scope_items(http, access_token, url).await
}

/// Graph documents `$expand`, `$top`, `$filter`, and `$orderby` for
/// `GET /me/chats`. `$select` is not one of them, and a request that sends
/// it fails before any chat is returned.
const TEAMS_CHAT_LIST_TOP: usize = 50;
/// Invite walks follow `@odata.nextLink` only until this many pages. An
/// empty page does not increase the chat count, so the chat cap alone does
/// not bound a chain of distinct continuation URLs.
const TEAMS_CHAT_LIST_PAGE_CAP: usize = 10;

#[cfg(test)]
thread_local! {
    static CHAT_LIST_GRAPH_ROOT: std::cell::RefCell<Option<String>> =
        const { std::cell::RefCell::new(None) };
}

fn chats_list_root() -> String {
    #[cfg(test)]
    {
        if let Some(root) = CHAT_LIST_GRAPH_ROOT.with(|cell| cell.borrow().clone()) {
            return root;
        }
    }
    GRAPH.to_string()
}

fn chats_list_url() -> String {
    format!("{}/me/chats?$top={TEAMS_CHAT_LIST_TOP}", chats_list_root())
}

/// A Graph `@odata.nextLink` is followed with the member's token. Only an
/// HTTPS URL on `graph.microsoft.com` is eligible. Anything else is refused
/// before that token is attached.
fn chat_list_next_link_allowed(next: &str) -> bool {
    let Ok(url) = reqwest::Url::parse(next) else {
        return false;
    };
    if !url.username().is_empty() || url.password().is_some() {
        return false;
    }
    #[cfg(test)]
    {
        if let Some(root) = CHAT_LIST_GRAPH_ROOT.with(|cell| cell.borrow().clone()) {
            let Ok(root_url) = reqwest::Url::parse(&root) else {
                return false;
            };
            return url.scheme() == root_url.scheme()
                && url.host_str() == root_url.host_str()
                && url.port() == root_url.port();
        }
    }
    url.scheme() == "https"
        && url
            .host_str()
            .is_some_and(|host| host.eq_ignore_ascii_case("graph.microsoft.com"))
        && url.port().is_none()
}

fn chat_from_raw(c: ChatRaw) -> ChatSummary {
    let display_name =
        c.topic
            .filter(|t| !t.is_empty())
            .unwrap_or_else(|| match c.chat_type.as_deref() {
                Some("oneOnOne") => "1:1 chat".into(),
                Some("group") => "Group chat".into(),
                Some("meeting") => "Meeting chat".into(),
                _ => "Chat".into(),
            });
    ChatSummary {
        id: c.id,
        display_name,
        chat_type: c.chat_type,
    }
}

/// Chats the user participates in. One page. The picker uses this; the
/// connect-time invite uses [`list_chats_limited`] so a short first page
/// still reaches the ten-chat cap.
pub(crate) async fn list_chats(
    http: &reqwest::Client,
    access_token: &str,
) -> Result<Vec<ChatSummary>, ConnectorError> {
    let rows: Vec<ChatRaw> = list_all_scope_items(http, access_token, chats_list_url()).await?;
    Ok(rows.into_iter().map(chat_from_raw).collect())
}

/// One client for chat-list reads. Redirects stay off so a 3xx cannot
/// carry the member token off Graph. Built once so a picker load does not
/// load TLS roots and open a new pool on every call.
fn chat_list_http() -> Result<&'static reqwest::Client, ConnectorError> {
    static CLIENT: std::sync::OnceLock<reqwest::Client> = std::sync::OnceLock::new();
    if let Some(client) = CLIENT.get() {
        return Ok(client);
    }
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|e| ConnectorError::Exchange(format!("chat list client: {e}")))?;
    Ok(CLIENT.get_or_init(|| client))
}

/// Follow `@odata.nextLink` until `limit` chats are collected, or the list
/// ends. `None` reads the first page only. The caller's client is not used:
/// that client follows redirects, and this path must not.
async fn list_chats_limited(
    _http: &reqwest::Client,
    access_token: &str,
    limit: Option<usize>,
) -> Result<Vec<ChatSummary>, ConnectorError> {
    let http = chat_list_http()?;
    let mut url = chats_list_url();
    let mut seen = std::collections::HashSet::new();
    let mut out = Vec::new();
    let mut pages = 0usize;
    loop {
        if limit.is_some() && pages >= TEAMS_CHAT_LIST_PAGE_CAP {
            tracing::info!(
                kept = out.len(),
                "teams invite: chat list page budget reached; keeping chats already listed"
            );
            return Ok(out);
        }
        pages += 1;
        if !seen.insert(url.clone()) {
            tracing::info!(
                kept = out.len(),
                "teams invite: chat list continuation repeated a page; keeping chats already listed"
            );
            return Ok(out);
        }
        let page = match graph_get(&http, access_token, &url).await {
            Ok(page) => page,
            Err(e) if out.is_empty() => return Err(e),
            Err(e) => {
                tracing::info!(
                    error = %e,
                    kept = out.len(),
                    "teams invite: chat list continuation failed; keeping chats already listed"
                );
                return Ok(out);
            }
        };
        let list: GraphList<ChatRaw> = match serde_json::from_value(page) {
            Ok(list) => list,
            Err(e) if out.is_empty() => {
                return Err(ConnectorError::Exchange(format!("chats decode: {e}")));
            }
            Err(e) => {
                tracing::info!(
                    error = %e,
                    kept = out.len(),
                    "teams invite: chat list continuation failed; keeping chats already listed"
                );
                return Ok(out);
            }
        };
        for c in list.value {
            out.push(chat_from_raw(c));
            if limit.is_some_and(|n| out.len() >= n) {
                return Ok(out);
            }
        }
        match list.next_link {
            Some(next) if limit.is_some() && chat_list_next_link_allowed(&next) => url = next,
            Some(_) if limit.is_some() => {
                tracing::info!(
                    kept = out.len(),
                    "teams invite: refusing a chat-list continuation outside Graph"
                );
                return Ok(out);
            }
            _ => break,
        }
    }
    Ok(out)
}

// ── Team roster + identity matching (m365 Train 2 PR 2.4, decision #3) ───────
//
// Roster auto-seating needs two things Slack's `resolve_channel_members` /
// `resolve_verified_slack_identity` (`connectors/slack.rs`) already do for
// Slack: (1) walk the container's membership, (2) map each member to a
// CoralSwarm user via a verified email. Graph makes (1)+(2) cheaper than
// Slack's: `TeamMember.Read.Group` (app-only, RSC-scoped) returns each
// member's `email` directly on the SAME roster call — no per-member
// `users.info`-equivalent round trip is needed, and there is no "bot" member
// to subtract (application identities are not listed in this collection).
//
// A standard Teams channel's roster IS the team's roster — Teams has no
// separate per-standard-channel membership; only private/shared channels
// differ, and `routes::teams_bot`'s fan-out already skips those (different
// RSC perms). So this walks the TEAM, not the channel.

/// One member of a Microsoft Team roster (`aadUserConversationMember`).
#[derive(Debug, Clone)]
pub(crate) struct TeamRosterMember {
    /// AAD object id — the external identity `connector_identities` keys on.
    pub aad_user_id: String,
    /// `aadUserConversationMember.email`, when Graph returns one — this IS
    /// the user's Entra `mail` attribute. [`list_team_roster`]'s `$select`
    /// is `id,userId,email,displayName,roles,tenantId` — no `userType`, no
    /// license/mailbox data — so a null/absent value here tells this
    /// resolver ONLY that Graph did not return a readable address; it
    /// cannot determine WHY (guest, no Exchange Online mailbox, or anything
    /// else) from this call alone. CS-429: live 2026-09-20/21 against the
    /// CoralSwarm test tenant is ONE confirmed example, not a general Graph
    /// rule — a same-tenant `userType = Member`, team-owner member arrived
    /// with `"email": null` beside two licensed members who carried an
    /// address, and a SEPARATE, out-of-band check in the tenant admin
    /// centers (not this roster call) confirmed that specific member had no
    /// Exchange Online mailbox
    /// (docs/plans/teams-withheld-email-live-test.md §9-10). A null/absent
    /// value here must be classified `Unavailable` ("we could not read an
    /// address, cause unknown") by [`resolve_team_roster_members`], never
    /// folded into "guest/unmatched" and never asserted to mean any specific
    /// cause either — mirroring `connectors::slack`'s
    /// `SlackIdentity::Unavailable` vs `::Unmatched` split. Before that
    /// classification, a non-guest row is given one delegated
    /// `GET /users/{id}?$select=otherMails`. An address found there becomes
    /// this field. A failed or empty read leaves it unset.
    pub email: Option<String>,
    pub display_name: Option<String>,
    /// `conversationMember.roles`. An in-tenant guest lists `guest` here.
    pub roles: Vec<String>,
    /// `aadUserConversationMember.tenantId`. An out-of-tenant member is
    /// assigned `owner`, so `roles` alone misses them; a tenant id that is
    /// present and different from this account's tenant is the guest check.
    pub tenant_id: Option<String>,
}

/// True when this roster row is a guest. In-tenant guests carry `guest` in
/// `roles`. Out-of-tenant members are assigned `owner`, so a `tenantId` that
/// differs from the home tenant is the other half of the check. A missing
/// `tenantId`, or a missing home tenant, is not treated as foreign.
///
/// Being a guest is not the invite decision. A guest is never seated.
/// [`roster_member_disposition`] admits one to the invite when their address
/// matches a verified org domain (or the connecting member's domain, until
/// a verified domain exists).
pub(crate) fn teams_roster_member_is_guest(
    member: &TeamRosterMember,
    home_tenant_id: &str,
) -> bool {
    if member
        .roles
        .iter()
        .any(|role| role.eq_ignore_ascii_case("guest"))
    {
        return true;
    }
    let home = home_tenant_id.trim();
    if home.is_empty() {
        return false;
    }
    member
        .tenant_id
        .as_deref()
        .map(str::trim)
        .filter(|tid| !tid.is_empty())
        .is_some_and(|tid| !tid.eq_ignore_ascii_case(home))
}

/// What a team-roster row may do under D2/D21 item 30.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RosterDisposition {
    /// Not a guest. Eligible to be identity-matched and seated.
    Member,
    /// A guest whose address sits on a verified org domain, or on the
    /// connecting member's domain while no verified domain exists.
    /// Invited. Never seated, and no connector identity is written.
    InviteOnly,
    /// A guest whose address matches no admit domain. Not invited, not seated.
    Skip,
}

/// D2/D21 item 30 for one Teams roster row. Guests are admitted to the invite
/// only by [`crate::services::org_domains::address_on_guest_anchor`].
pub(crate) fn roster_member_disposition(
    member: &TeamRosterMember,
    home_tenant_id: &str,
    admit_domains: &[String],
) -> RosterDisposition {
    if !teams_roster_member_is_guest(member, home_tenant_id) {
        return RosterDisposition::Member;
    }
    match member.email.as_deref() {
        Some(email)
            if crate::services::org_domains::address_on_guest_anchor(email, admit_domains) =>
        {
            RosterDisposition::InviteOnly
        }
        _ => RosterDisposition::Skip,
    }
}

/// Verified org domains, or the connecting member's domain while none exist.
/// A failed verified-domain lookup returns an empty list and does not
/// bootstrap — could not ask is not "nothing is verified."
async fn teams_guest_admit_domains(control: &PgPool, account_id: Uuid) -> Vec<String> {
    let verified = crate::services::org_domains::verified_domains(control, account_id)
        .await
        .map_err(|e| e.to_string());
    if let Err(reason) = &verified {
        tracing::warn!(
            reason = %reason, %account_id,
            "teams: verified org-domain lookup failed — admitting no guest by domain rather than \
             falling back to the connecting member's domain (fail closed)"
        );
    }
    let bootstrap = if matches!(&verified, Ok(domains) if domains.is_empty()) {
        teams_connecting_member_domain(control, account_id).await
    } else {
        None
    };
    crate::services::org_domains::guest_anchor_domains(verified, bootstrap)
}

/// Domain of the member whose Microsoft token is reading this roster.
/// An account-wide token (personal install, `owner_user_id` null) uses the
/// account owner. No Microsoft token means there is no connecting member to
/// bootstrap from.
async fn teams_connecting_member_domain(control: &PgPool, account_id: Uuid) -> Option<String> {
    let email: Option<String> = sqlx::query_scalar::<_, Option<String>>(
        "SELECT u.email FROM oauth_tokens t \
         JOIN users u ON u.id = COALESCE(t.owner_user_id, \
             (SELECT a.owner_user_id FROM accounts a WHERE a.id = t.account_id)) \
         WHERE t.account_id = $1 AND t.provider = 'microsoft' \
           AND t.credential_invalid_at IS NULL AND u.deleted_at IS NULL \
           AND u.email IS NOT NULL \
         ORDER BY t.updated_at DESC LIMIT 1",
    )
    .bind(account_id)
    .fetch_optional(control)
    .await
    .ok()
    .flatten()
    .flatten();
    email
        .as_deref()
        .and_then(crate::services::org_domains::email_domain_part)
}

#[cfg(test)]
mod teams_guest_filter_tests {
    use super::{
        roster_member_disposition, teams_roster_member_is_guest, RosterDisposition,
        TeamRosterMember,
    };
    use crate::services::org_domains::guest_anchor_domains;

    fn member(roles: &[&str], tenant_id: Option<&str>) -> TeamRosterMember {
        TeamRosterMember {
            aad_user_id: "aad".into(),
            email: Some("a@example.com".into()),
            display_name: None,
            roles: roles.iter().map(|r| (*r).to_string()).collect(),
            tenant_id: tenant_id.map(str::to_string),
        }
    }

    #[test]
    fn in_tenant_guest_role_is_a_guest() {
        assert!(teams_roster_member_is_guest(
            &member(&["guest"], Some("home")),
            "home"
        ));
    }

    #[test]
    fn foreign_tenant_owner_is_a_guest() {
        assert!(teams_roster_member_is_guest(
            &member(&["owner"], Some("other")),
            "home"
        ));
    }

    #[test]
    fn same_tenant_member_is_not_a_guest() {
        assert!(!teams_roster_member_is_guest(
            &member(&["owner"], Some("home")),
            "home"
        ));
        assert!(!teams_roster_member_is_guest(&member(&[], None), "home"));
    }

    #[test]
    fn missing_home_tenant_does_not_mark_the_roster_as_guests() {
        assert!(!teams_roster_member_is_guest(
            &member(&["owner"], Some("other")),
            ""
        ));
        assert!(teams_roster_member_is_guest(
            &member(&["guest"], Some("other")),
            ""
        ));
    }

    fn guest_at(email: &str) -> TeamRosterMember {
        let mut row = member(&["guest"], Some("home"));
        row.email = Some(email.to_string());
        row
    }

    #[test]
    fn a_guest_whose_address_matches_a_verified_org_domain_is_admitted() {
        let anchors = guest_anchor_domains(
            Ok(vec!["acme.com".into(), "acquired.com".into()]),
            Some("installer.test".into()),
        );
        assert_eq!(
            roster_member_disposition(&guest_at("pat@acquired.com"), "home", &anchors),
            RosterDisposition::InviteOnly,
            "a guest on a later verified domain is invited, and InviteOnly is never a seat"
        );
        assert_eq!(
            roster_member_disposition(&guest_at("pat@acme.com"), "home", &anchors),
            RosterDisposition::InviteOnly
        );
    }

    #[test]
    fn a_guest_whose_address_does_not_match_a_verified_org_domain_is_not_admitted() {
        let anchors =
            guest_anchor_domains(Ok(vec!["acme.com".into()]), Some("installer.test".into()));
        assert_eq!(
            roster_member_disposition(&guest_at("contractor@client.example"), "home", &anchors),
            RosterDisposition::Skip
        );
        assert_eq!(
            roster_member_disposition(&guest_at("pat@installer.test"), "home", &anchors),
            RosterDisposition::Skip,
            "the connecting member's domain does not admit once a verified domain exists"
        );
        assert_eq!(
            roster_member_disposition(&member(&["owner"], Some("home")), "home", &anchors),
            RosterDisposition::Member,
            "a same-tenant member is seated, not run through the guest admit rule"
        );
    }

    #[test]
    fn the_connecting_members_domain_bootstraps_only_until_a_verified_domain_exists() {
        let bootstrap = guest_anchor_domains(Ok(vec![]), Some("installer.test".into()));
        assert_eq!(
            roster_member_disposition(&guest_at("pat@installer.test"), "home", &bootstrap),
            RosterDisposition::InviteOnly
        );
        assert_eq!(
            roster_member_disposition(&guest_at("contractor@client.example"), "home", &bootstrap),
            RosterDisposition::Skip
        );

        let verified =
            guest_anchor_domains(Ok(vec!["acme.com".into()]), Some("installer.test".into()));
        assert_eq!(
            roster_member_disposition(&guest_at("pat@acme.com"), "home", &verified),
            RosterDisposition::InviteOnly
        );
        assert_eq!(
            roster_member_disposition(&guest_at("pat@installer.test"), "home", &verified),
            RosterDisposition::Skip
        );

        let failed = guest_anchor_domains(
            Err("connection reset".into()),
            Some("installer.test".into()),
        );
        assert_eq!(
            roster_member_disposition(&guest_at("pat@installer.test"), "home", &failed),
            RosterDisposition::Skip,
            "a failed verified-domain lookup must not bootstrap"
        );
    }
}

#[derive(Debug, Deserialize)]
struct RosterMemberRaw {
    #[serde(rename = "userId")]
    user_id: Option<String>,
    email: Option<String>,
    #[serde(rename = "displayName")]
    display_name: Option<String>,
    #[serde(default)]
    roles: Vec<String>,
    #[serde(default, rename = "tenantId")]
    tenant_id: Option<String>,
}

/// List every member of a Microsoft Team (`TeamMember.Read.Group`, app-only —
/// the same app token every other RSC-authorized Graph call in this module
/// uses). Paginated via `@odata.nextLink`, same idiom as
/// [`fetch_channel_messages`]'s reply pagination.
pub(crate) async fn list_team_roster(
    http: &reqwest::Client,
    app_token: &str,
    team_id: &str,
) -> Result<Vec<TeamRosterMember>, ConnectorError> {
    let mut url = format!(
        "{GRAPH}/teams/{}/members?$select=id,userId,email,displayName,roles,tenantId",
        urlencoding::encode(team_id)
    );
    let mut out = Vec::new();
    loop {
        let list: GraphList<RosterMemberRaw> =
            serde_json::from_value(graph_get(http, app_token, &url).await?)
                .map_err(|e| ConnectorError::Exchange(format!("team roster decode: {e}")))?;
        for m in list.value {
            if let Some(aad_user_id) = m.user_id {
                out.push(TeamRosterMember {
                    aad_user_id,
                    email: m.email,
                    display_name: m.display_name,
                    roles: m.roles,
                    tenant_id: m.tenant_id,
                });
            }
        }
        match list.next_link {
            Some(next) => url = next,
            None => break,
        }
    }
    Ok(out)
}

// Tests point this at a local server. Production stays on Graph. The members
// endpoint rejects OData, including `$select`.
#[cfg(test)]
thread_local! {
    static CHAT_ROSTER_GRAPH_ROOT: std::cell::RefCell<Option<String>> =
        const { std::cell::RefCell::new(None) };
}

fn chat_roster_graph_root() -> String {
    #[cfg(test)]
    {
        if let Some(root) = CHAT_ROSTER_GRAPH_ROOT.with(|cell| cell.borrow().clone()) {
            return root;
        }
    }
    GRAPH.to_string()
}

/// `GET /chats/{chat-id}/members`. Delegated `Chat.Read` is an accepted
/// permission. The endpoint does not support OData query parameters.
pub(crate) fn chat_members_url(chat_id: &str) -> String {
    format!(
        "{}/chats/{}/members",
        chat_roster_graph_root(),
        urlencoding::encode(chat_id)
    )
}

/// List every member of a chat the caller is in. `access_token` is that
/// member's delegated token. This does not mint an app token and does not
/// read chat messages. The resource is the same `aadUserConversationMember`
/// as a team roster (`roles`, `tenantId`, `email`), so
/// [`teams_roster_member_is_guest`] applies. A missing `email` stays `None`.
pub(crate) async fn list_chat_roster(
    http: &reqwest::Client,
    access_token: &str,
    chat_id: &str,
) -> Result<Vec<TeamRosterMember>, ConnectorError> {
    let mut url = chat_members_url(chat_id);
    let mut out = Vec::new();
    loop {
        let list: GraphList<RosterMemberRaw> =
            serde_json::from_value(graph_get(http, access_token, &url).await?)
                .map_err(|e| ConnectorError::Exchange(format!("chat roster decode: {e}")))?;
        for m in list.value {
            if let Some(aad_user_id) = m.user_id {
                out.push(TeamRosterMember {
                    aad_user_id,
                    email: m.email,
                    display_name: m.display_name,
                    roles: m.roles,
                    tenant_id: m.tenant_id,
                });
            }
        }
        match list.next_link {
            Some(next) => url = next,
            None => break,
        }
    }
    Ok(out)
}

#[cfg(test)]
mod chat_roster_request_tests {
    use super::{
        chat_list_next_link_allowed, chat_members_url, chats_list_url, list_chat_roster,
        list_chats_limited, teams_roster_member_is_guest, APP_TOKEN_MINTS, CHAT_LIST_GRAPH_ROOT,
        CHAT_ROSTER_GRAPH_ROOT,
    };
    use std::io::{Read, Write};
    use std::sync::{Arc, Mutex};

    #[test]
    fn chat_members_url_has_no_odata_select() {
        let url = chat_members_url("19:abc@thread.v2");
        assert!(!url.contains('?'), "{url}");
        assert!(!url.contains("$select"), "{url}");
        assert!(
            url.ends_with("/chats/19%3Aabc%40thread.v2/members"),
            "{url}"
        );
    }

    #[test]
    fn chats_list_url_keeps_top_and_drops_select() {
        let url = chats_list_url();
        assert!(!url.contains("$select"), "{url}");
        assert!(url.contains("$top=50"), "{url}");
        assert!(url.ends_with("/me/chats?$top=50"), "{url}");
    }

    #[test]
    fn chat_list_continuation_stays_on_graph_https() {
        assert!(chat_list_next_link_allowed(
            "https://graph.microsoft.com/v1.0/me/chats?$skiptoken=abc"
        ));
        assert!(!chat_list_next_link_allowed(
            "http://graph.microsoft.com/v1.0/me/chats?$skiptoken=abc"
        ));
        assert!(!chat_list_next_link_allowed("https://evil.example/steal"));
        assert!(!chat_list_next_link_allowed(
            "https://graph.microsoft.com.evil.example/v1.0/me/chats"
        ));
        assert!(!chat_list_next_link_allowed(
            "https://user:pass@graph.microsoft.com/v1.0/me/chats"
        ));
        assert!(!chat_list_next_link_allowed("not a url"));
    }

    /// A continuation on another host is not requested, and the chats from
    /// the first page are kept.
    #[tokio::test]
    async fn foreign_next_link_is_not_requested() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let hits = Arc::new(Mutex::new(0u8));
        let hits_srv = hits.clone();
        std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            *hits_srv.lock().unwrap() += 1;
            read_http_request(&mut stream);
            let body = r#"{"value":[{"id":"chat-1","topic":"Alpha","chatType":"group"}],"@odata.nextLink":"https://evil.example/steal"}"#;
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(resp.as_bytes()).unwrap();
            listener.set_nonblocking(true).unwrap();
            std::thread::sleep(std::time::Duration::from_millis(300));
            if listener.accept().is_ok() {
                *hits_srv.lock().unwrap() += 1;
            }
        });

        struct ClearRoot;
        impl Drop for ClearRoot {
            fn drop(&mut self) {
                CHAT_LIST_GRAPH_ROOT.with(|cell| *cell.borrow_mut() = None);
            }
        }
        let _clear = ClearRoot;
        CHAT_LIST_GRAPH_ROOT.with(|cell| {
            *cell.borrow_mut() = Some(format!("http://127.0.0.1:{port}/v1.0"));
        });

        let http = reqwest::Client::new();
        let chats = list_chats_limited(&http, "member-delegated-token", Some(10))
            .await
            .expect("a refused continuation keeps the first page");
        assert_eq!(chats.len(), 1);
        assert_eq!(chats[0].id, "chat-1");
        tokio::time::sleep(std::time::Duration::from_millis(400)).await;
        assert_eq!(
            *hits.lock().unwrap(),
            1,
            "the foreign continuation was requested"
        );
    }

    /// An empty page whose `@odata.nextLink` is the page just read must not
    /// be requested again.
    #[tokio::test]
    async fn repeated_continuation_stops_after_the_first_page() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let hits = Arc::new(Mutex::new(0u8));
        let hits_srv = hits.clone();
        let repeated_srv = format!("http://127.0.0.1:{port}/v1.0/me/chats?$top=50");
        std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            *hits_srv.lock().unwrap() += 1;
            read_http_request(&mut stream);
            let body = format!(r#"{{"value":[],"@odata.nextLink":"{repeated_srv}"}}"#);
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(resp.as_bytes()).unwrap();
            listener.set_nonblocking(true).unwrap();
            std::thread::sleep(std::time::Duration::from_millis(200));
            if listener.accept().is_ok() {
                *hits_srv.lock().unwrap() += 1;
            }
        });

        struct ClearRoot;
        impl Drop for ClearRoot {
            fn drop(&mut self) {
                CHAT_LIST_GRAPH_ROOT.with(|cell| *cell.borrow_mut() = None);
            }
        }
        let _clear = ClearRoot;
        CHAT_LIST_GRAPH_ROOT.with(|cell| {
            *cell.borrow_mut() = Some(format!("http://127.0.0.1:{port}/v1.0"));
        });

        let http = reqwest::Client::new();
        let chats = list_chats_limited(&http, "member-delegated-token", Some(10))
            .await
            .expect("a repeated continuation keeps the chats already listed");
        assert!(chats.is_empty());
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        assert_eq!(
            *hits.lock().unwrap(),
            1,
            "the repeated page was requested again"
        );
    }

    /// Distinct empty continuations stop at the page budget instead of
    /// walking until Graph runs out of links.
    #[tokio::test]
    async fn empty_continuations_stop_at_the_page_budget() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let hits = Arc::new(Mutex::new(0u8));
        let hits_srv = hits.clone();
        std::thread::spawn(move || {
            for n in 1..=12 {
                let Ok((mut stream, _)) = listener.accept() else {
                    break;
                };
                *hits_srv.lock().unwrap() += 1;
                read_http_request(&mut stream);
                let body = format!(
                    r#"{{"value":[],"@odata.nextLink":"http://127.0.0.1:{port}/v1.0/me/chats?$skiptoken={n}"}}"#
                );
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = stream.write_all(resp.as_bytes());
            }
        });

        struct ClearRoot;
        impl Drop for ClearRoot {
            fn drop(&mut self) {
                CHAT_LIST_GRAPH_ROOT.with(|cell| *cell.borrow_mut() = None);
            }
        }
        let _clear = ClearRoot;
        CHAT_LIST_GRAPH_ROOT.with(|cell| {
            *cell.borrow_mut() = Some(format!("http://127.0.0.1:{port}/v1.0"));
        });

        let http = reqwest::Client::new();
        let chats = list_chats_limited(&http, "member-delegated-token", Some(10))
            .await
            .expect("a long empty continuation chain keeps the chats already listed");
        assert!(chats.is_empty());
        assert_eq!(
            *hits.lock().unwrap(),
            10,
            "the page budget was not enforced"
        );
    }

    fn read_http_request(stream: &mut std::net::TcpStream) {
        stream
            .set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .unwrap();
        let mut buf = [0u8; 8192];
        let mut got = Vec::new();
        loop {
            match stream.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => {
                    got.extend_from_slice(&buf[..n]);
                    if got.windows(4).any(|w| w == b"\r\n\r\n") {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    }

    /// A short first page plus a failed `@odata.nextLink` still returns the
    /// chats already listed. A failed first page stays an error.
    #[tokio::test]
    async fn continuation_failure_keeps_chats_already_listed() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        std::thread::spawn(move || {
            for page in 0..2 {
                let (mut stream, _) = listener.accept().unwrap();
                read_http_request(&mut stream);
                let body = if page == 0 {
                    format!(
                        r#"{{"value":[{{"id":"chat-1","topic":"Alpha","chatType":"group"}}],"@odata.nextLink":"http://127.0.0.1:{port}/v1.0/me/chats?$skiptoken=2"}}"#
                    )
                } else {
                    r#"{"error":{"code":"UnknownError"}}"#.to_string()
                };
                let status = if page == 0 {
                    "200 OK"
                } else {
                    "500 Internal Server Error"
                };
                let resp = format!(
                    "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                stream.write_all(resp.as_bytes()).unwrap();
            }
        });

        struct ClearRoot;
        impl Drop for ClearRoot {
            fn drop(&mut self) {
                CHAT_LIST_GRAPH_ROOT.with(|cell| *cell.borrow_mut() = None);
            }
        }
        let _clear = ClearRoot;
        CHAT_LIST_GRAPH_ROOT.with(|cell| {
            *cell.borrow_mut() = Some(format!("http://127.0.0.1:{port}/v1.0"));
        });

        let http = reqwest::Client::new();
        let chats = list_chats_limited(&http, "member-delegated-token", Some(10))
            .await
            .expect("a failed continuation keeps the first page");
        assert_eq!(chats.len(), 1);
        assert_eq!(chats[0].id, "chat-1");
        assert_eq!(chats[0].display_name, "Alpha");
    }

    #[tokio::test]
    async fn first_page_failure_is_still_an_error() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            read_http_request(&mut stream);
            let body = r#"{"error":{"code":"UnknownError"}}"#;
            let resp = format!(
                "HTTP/1.1 500 Internal Server Error\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(resp.as_bytes()).unwrap();
        });

        struct ClearRoot;
        impl Drop for ClearRoot {
            fn drop(&mut self) {
                CHAT_LIST_GRAPH_ROOT.with(|cell| *cell.borrow_mut() = None);
            }
        }
        let _clear = ClearRoot;
        CHAT_LIST_GRAPH_ROOT.with(|cell| {
            *cell.borrow_mut() = Some(format!("http://127.0.0.1:{port}/v1.0"));
        });

        let http = reqwest::Client::new();
        let err = list_chats_limited(&http, "member-delegated-token", Some(10))
            .await
            .expect_err("a failed first page is an error");
        let message = err.to_string();
        assert!(message.contains("500"), "{message}");
    }

    /// The roster GET uses the token passed in, on `/chats/{id}/members`
    /// with no `$select`. An app-token mint is a different call, and this
    /// one does not make it.
    #[tokio::test]
    async fn list_chat_roster_uses_the_passed_member_token_and_not_an_app_token() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let seen = Arc::new(Mutex::new(String::new()));
        let seen_srv = seen.clone();
        std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(std::time::Duration::from_secs(5)))
                .unwrap();
            let mut buf = [0u8; 8192];
            let mut got = Vec::new();
            loop {
                match stream.read(&mut buf) {
                    Ok(0) => break,
                    Ok(n) => {
                        got.extend_from_slice(&buf[..n]);
                        if got.windows(4).any(|w| w == b"\r\n\r\n") {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
            *seen_srv.lock().unwrap() = String::from_utf8_lossy(&got).into_owned();
            let body = r#"{"value":[
                {"userId":"aad-no-mail","displayName":"No Mail","roles":[],"tenantId":"home-tenant"},
                {"userId":"aad-guest","email":"guest@client.example","displayName":"Guest","roles":["guest"],"tenantId":"home-tenant"},
                {"displayName":"No User Id","roles":["guest"]}
            ]}"#;
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(resp.as_bytes()).unwrap();
        });

        let root = format!("http://127.0.0.1:{port}/v1.0");
        struct ClearRoot;
        impl Drop for ClearRoot {
            fn drop(&mut self) {
                CHAT_ROSTER_GRAPH_ROOT.with(|cell| *cell.borrow_mut() = None);
            }
        }
        let _clear = ClearRoot;
        CHAT_ROSTER_GRAPH_ROOT.with(|cell| *cell.borrow_mut() = Some(root));
        let before = APP_TOKEN_MINTS.load(std::sync::atomic::Ordering::SeqCst);
        let http = reqwest::Client::new();
        let roster = list_chat_roster(&http, "member-delegated-token", "19:abc@thread.v2")
            .await
            .unwrap();
        let after = APP_TOKEN_MINTS.load(std::sync::atomic::Ordering::SeqCst);
        assert_eq!(after, before, "list_chat_roster must not mint an app token");

        let request = seen.lock().unwrap().clone();
        let first_line = request.lines().next().unwrap_or("");
        assert!(
            first_line.contains("/v1.0/chats/19%3Aabc%40thread.v2/members"),
            "{first_line}"
        );
        assert!(!first_line.contains('?'), "{first_line}");
        assert!(!request.contains("$select"), "{request}");
        assert!(
            request.lines().any(|line| {
                line.eq_ignore_ascii_case("authorization: Bearer member-delegated-token")
            }),
            "{request}"
        );
        assert!(!request.contains("client_credentials"), "{request}");

        assert_eq!(roster.len(), 2, "a row without userId is not a member");
        let missing_email = roster
            .iter()
            .find(|m| m.aad_user_id == "aad-no-mail")
            .unwrap();
        assert!(missing_email.email.is_none());
        assert!(
            !teams_roster_member_is_guest(missing_email, "home-tenant"),
            "a missing email is not a guest"
        );
        let guest = roster
            .iter()
            .find(|m| m.aad_user_id == "aad-guest")
            .unwrap();
        assert!(teams_roster_member_is_guest(guest, "home-tenant"));
    }
}

#[cfg(test)]
mod other_mails_tests {
    use super::{
        fill_missing_emails_from_other_mails, roster_invite_address, roster_member_disposition,
        teams_roster_member_is_guest, RosterDisposition, TeamRosterMember, APP_TOKEN_MINTS,
        CHAT_ROSTER_GRAPH_ROOT, DEFAULT_MICROSOFT_SCOPES,
    };
    use std::io::{Read, Write};
    use std::sync::{Arc, Mutex};

    const HOME: &str = "home-tenant";
    const TOKEN: &str = "member-delegated-token";

    fn row(
        id: &str,
        email: Option<&str>,
        roles: &[&str],
        tenant: Option<&str>,
    ) -> TeamRosterMember {
        TeamRosterMember {
            aad_user_id: id.into(),
            email: email.map(str::to_string),
            display_name: Some(id.into()),
            roles: roles.iter().map(|role| (*role).to_string()).collect(),
            tenant_id: tenant.map(str::to_string),
        }
    }

    struct ClearRoot;
    impl Drop for ClearRoot {
        fn drop(&mut self) {
            CHAT_ROSTER_GRAPH_ROOT.with(|cell| *cell.borrow_mut() = None);
        }
    }

    fn read_http_request(stream: &mut std::net::TcpStream) -> String {
        stream
            .set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .unwrap();
        let mut buf = [0u8; 8192];
        let mut got = Vec::new();
        loop {
            match stream.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => {
                    got.extend_from_slice(&buf[..n]);
                    if got.windows(4).any(|w| w == b"\r\n\r\n") {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
        String::from_utf8_lossy(&got).into_owned()
    }

    fn write_http(stream: &mut std::net::TcpStream, status: u16, body: &str) {
        let reason = match status {
            200 => "OK",
            403 => "Forbidden",
            _ => "Error",
        };
        let resp = format!(
            "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        stream.write_all(resp.as_bytes()).unwrap();
    }

    fn spawn_graph(
        respond: impl Fn(&str) -> (u16, String) + Send + 'static,
    ) -> (u16, Arc<Mutex<Vec<String>>>) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let seen_srv = seen.clone();
        std::thread::spawn(move || {
            for _ in 0..8 {
                let Ok((mut stream, _)) = listener.accept() else {
                    break;
                };
                let request = read_http_request(&mut stream);
                if request.is_empty() {
                    break;
                }
                seen_srv.lock().unwrap().push(request.clone());
                let (status, body) = respond(&request);
                write_http(&mut stream, status, &body);
            }
        });
        (port, seen)
    }

    fn point_graph_at(port: u16) -> ClearRoot {
        let clear = ClearRoot;
        CHAT_ROSTER_GRAPH_ROOT.with(|cell| {
            *cell.borrow_mut() = Some(format!("http://127.0.0.1:{port}/v1.0"));
        });
        clear
    }

    fn assert_stays_unavailable(member: &TeamRosterMember) {
        assert!(
            !teams_roster_member_is_guest(member, HOME),
            "a directory miss is not a guest"
        );
        assert_eq!(
            roster_invite_address(member),
            None,
            "no address means Unavailable, not an unmatched invite"
        );
        assert_eq!(
            roster_member_disposition(member, HOME, &[]),
            RosterDisposition::Member,
            "the row stays a member whose address we could not read"
        );
    }

    #[test]
    fn default_scopes_request_user_read_all_so_other_mails_can_be_read() {
        let tokens: Vec<&str> = DEFAULT_MICROSOFT_SCOPES.split_whitespace().collect();
        assert!(tokens.contains(&"User.Read"), "{tokens:?}");
        assert!(tokens.contains(&"User.Read.All"), "{tokens:?}");
        let example = include_str!("../../../.env.example");
        let marker = "MICROSOFT_SCOPES=\"";
        let start = example
            .find(marker)
            .expect("MICROSOFT_SCOPES in .env.example");
        let rest = &example[start + marker.len()..];
        let end = rest.find('"').expect("closing quote");
        let env_scopes = rest[..end].replace("\\\n", "");
        assert_eq!(
            env_scopes, DEFAULT_MICROSOFT_SCOPES,
            ".env.example MICROSOFT_SCOPES must match the code default exactly"
        );
    }

    /// A non-guest with a null roster email and `otherMails: ["a@corp.com"]`
    /// is invited and resolved with that address. Guests are not sent to the
    /// directory read.
    #[tokio::test]
    async fn non_guest_null_email_other_mails_is_the_invite_address_and_a_guest_is_not_read() {
        let (port, seen) = spawn_graph(|_| (200, r#"{"otherMails":["a@corp.com"]}"#.into()));
        let _clear = point_graph_at(port);
        let before = APP_TOKEN_MINTS.load(std::sync::atomic::Ordering::SeqCst);
        let mut roster = vec![
            row("aad-guest", None, &["guest"], Some(HOME)),
            row("aad-external", None, &["owner"], Some("other-tenant")),
            row("aad-kept", Some("kept@corp.com"), &[], Some(HOME)),
            row("aad-unlicensed", None, &[], Some(HOME)),
        ];
        let http = reqwest::Client::new();
        fill_missing_emails_from_other_mails(&http, TOKEN, HOME, &mut roster).await;
        let after = APP_TOKEN_MINTS.load(std::sync::atomic::Ordering::SeqCst);
        assert_eq!(
            after, before,
            "the directory read must not mint an app token"
        );

        let requests = seen.lock().unwrap().clone();
        assert_eq!(requests.len(), 1, "one directory read: {requests:?}");
        let request = &requests[0];
        assert!(request.contains("/users/aad-unlicensed"), "{request}");
        assert!(
            request.contains("$select=otherMails") || request.contains("%24select=otherMails"),
            "{request}"
        );
        assert!(
            !request.to_ascii_lowercase().contains("usertype"),
            "userType is not part of this read: {request}"
        );
        assert!(
            request.lines().any(|line| {
                line.eq_ignore_ascii_case(&format!("authorization: Bearer {TOKEN}"))
            }),
            "{request}"
        );
        assert!(!request.contains("client_credentials"), "{request}");
        for skipped in ["aad-guest", "aad-external", "aad-kept"] {
            assert!(
                !request.contains(skipped),
                "{skipped} must not be read: {request}"
            );
        }

        let unlicensed = roster
            .iter()
            .find(|m| m.aad_user_id == "aad-unlicensed")
            .unwrap();
        assert_eq!(unlicensed.email.as_deref(), Some("a@corp.com"));
        assert_eq!(
            roster_invite_address(unlicensed).as_deref(),
            Some("a@corp.com"),
            "the invite path and the resolver both consume this address"
        );
        assert!(!teams_roster_member_is_guest(unlicensed, HOME));
        assert_eq!(
            roster_member_disposition(unlicensed, HOME, &[]),
            RosterDisposition::Member
        );
        assert!(roster
            .iter()
            .find(|m| m.aad_user_id == "aad-guest")
            .unwrap()
            .email
            .is_none());
        assert!(roster
            .iter()
            .find(|m| m.aad_user_id == "aad-external")
            .unwrap()
            .email
            .is_none());
        assert_eq!(
            roster
                .iter()
                .find(|m| m.aad_user_id == "aad-kept")
                .unwrap()
                .email
                .as_deref(),
            Some("kept@corp.com")
        );
    }

    #[tokio::test]
    async fn failed_other_mails_read_stays_unavailable_and_is_not_a_guest() {
        let (port, seen) = spawn_graph(|_| {
            (
                403,
                r#"{"error":{"code":"Authorization_RequestDenied"}}"#.into(),
            )
        });
        let _clear = point_graph_at(port);
        let mut roster = vec![row("aad-unlicensed", None, &[], Some(HOME))];
        let http = reqwest::Client::new();
        fill_missing_emails_from_other_mails(&http, TOKEN, HOME, &mut roster).await;
        let requests = seen.lock().unwrap().clone();
        assert_eq!(requests.len(), 1, "the read was attempted: {requests:?}");
        assert!(!requests[0].to_ascii_lowercase().contains("usertype"));
        assert_stays_unavailable(&roster[0]);
    }

    #[tokio::test]
    async fn empty_other_mails_stays_unavailable_and_is_not_a_guest() {
        let (port, _) = spawn_graph(|_| (200, r#"{"otherMails":[]}"#.into()));
        let _clear = point_graph_at(port);
        let mut roster = vec![row("aad-unlicensed", None, &[], Some(HOME))];
        let http = reqwest::Client::new();
        fill_missing_emails_from_other_mails(&http, TOKEN, HOME, &mut roster).await;
        assert_stays_unavailable(&roster[0]);
    }
}

/// Lowercase + trim ONLY — same rule, and the same reason, as
/// `connectors::slack::normalize_email`: a Gmail dot/`+tag` alias must stay a
/// DIFFERENT address, never collapsed into the base one.
fn normalize_microsoft_email(raw: &str) -> String {
    raw.trim().to_lowercase()
}

/// Match one roster member's verified email to a CoralSwarm user and persist
/// the confirmed link into `connector_identities` (`provider = 'microsoft'`).
/// Mirrors `connectors::slack::match_and_persist_verified_email` exactly —
/// same two-pool split (`control_pool` for the CONTROL-only `users` lookup,
/// `content_pool` for the CONTENT-plane `connector_identities` upsert), same
/// "the persistence failure is logged and swallowed, the match itself is
/// still returned" posture.
async fn match_and_persist_verified_microsoft_email(
    control_pool: &PgPool,
    content_pool: &PgPool,
    tenant_id: &str,
    aad_user_id: &str,
    raw_email: &str,
    canonical_name_hint: &str,
) -> Result<Option<Uuid>, String> {
    let normalized = normalize_microsoft_email(raw_email);
    if normalized.is_empty() {
        return Ok(None);
    }

    let user_id: Option<Uuid> = sqlx::query_scalar(
        "SELECT id FROM users WHERE lower(trim(email)) = $1 AND deleted_at IS NULL",
    )
    .bind(&normalized)
    .fetch_optional(control_pool)
    .await
    .map_err(|e| format!("users email lookup: {e}"))?
    .flatten();
    let Some(user_id) = user_id else {
        return Ok(None);
    };

    if let Err(e) = sqlx::query(
        "INSERT INTO connector_identities \
             (provider, workspace_id, external_user_id, canonical_name, user_id, \
              link_method, link_verified_at) \
         VALUES ('microsoft', $1, $2, $3, $4, 'verified_email', now()) \
         ON CONFLICT (provider, workspace_id, external_user_id) DO UPDATE SET \
             user_id          = EXCLUDED.user_id, \
             link_method      = EXCLUDED.link_method, \
             link_verified_at = EXCLUDED.link_verified_at, \
             updated_at       = now()",
    )
    .bind(tenant_id)
    .bind(aad_user_id)
    .bind(canonical_name_hint)
    .bind(user_id)
    .execute(content_pool)
    .await
    {
        tracing::warn!(
            error = %e,
            tenant_id,
            aad_user_id,
            "match_and_persist_verified_microsoft_email: failed to persist confirmed link \
             (non-fatal)"
        );
    }

    Ok(Some(user_id))
}

/// Per-team-container cooldown for [`log_roster_unavailable_health`] — same
/// "one operator-facing line per shift, not per tick" reasoning as
/// `connectors::slack`'s `UNMATCHED_NOTICE_COOLDOWN`/
/// `should_send_unmatched_notice` (see that pair's own doc comments), just
/// gating a `tracing::warn!` instead of an outbound Slack message. Keyed on
/// `(tenant_id, team_id)` — the "container" this roster walk resolves — so
/// one noisy team never suppresses another's first warning.
fn roster_unavailable_health_cooldown(
) -> &'static std::sync::Mutex<std::collections::HashMap<(String, String), std::time::Instant>> {
    static C: std::sync::OnceLock<
        std::sync::Mutex<std::collections::HashMap<(String, String), std::time::Instant>>,
    > = std::sync::OnceLock::new();
    C.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()))
}

/// Same window as `connectors::slack::UNMATCHED_NOTICE_COOLDOWN`: long enough
/// that a tenant hears this once a shift, short enough that fixing whatever
/// is making the address unreadable and re-sweeping isn't left wondering
/// whether it took. Matches the "6-hour per-container cooldown" this warning
/// already had before CS-429 (docs/plans/teams-withheld-email-live-test.md
/// §5).
const ROSTER_UNAVAILABLE_HEALTH_COOLDOWN: std::time::Duration =
    std::time::Duration::from_secs(6 * 60 * 60);

/// `true` at most once per [`ROSTER_UNAVAILABLE_HEALTH_COOLDOWN`] per
/// `(tenant_id, team_id)`. Claims the slot as it reports, same idiom as
/// `connectors::slack::should_send_unmatched_notice`.
fn should_log_roster_unavailable_health(tenant_id: &str, team_id: &str) -> bool {
    let now = std::time::Instant::now();
    let mut guard = match roster_unavailable_health_cooldown().lock() {
        Ok(g) => g,
        Err(_) => return false,
    };
    guard.retain(|_, sent| now.duration_since(*sent) < ROSTER_UNAVAILABLE_HEALTH_COOLDOWN);
    let key = (tenant_id.to_string(), team_id.to_string());
    match guard.get(&key) {
        Some(sent) if now.duration_since(*sent) < ROSTER_UNAVAILABLE_HEALTH_COOLDOWN => false,
        _ => {
            guard.insert(key, now);
            true
        }
    }
}

/// CS-429: the specific, `connector_health = true` warning for a roster that
/// contains at least one member whose address could not be read. Reports
/// ONLY the observed fact — Graph returned no `email` — never a cause: this
/// resolver's own roster request (`list_team_roster`'s `$select`) has no
/// `userType`/license/mailbox data, so it cannot say WHY, and must not guess
/// (see [`TeamRosterMember::email`]'s own doc comment). Two DIFFERENT static
/// messages, not one templated string — the all-unavailable case must never
/// read as "found no members" (a `Vec::new()` from an ordinary empty team
/// would look identical on a count alone), and the partial case must never
/// suggest the readable members are also at risk. `unavailable`/`total` ride
/// as structured fields either way, so an operator can see the exact count
/// without string-parsing the message.
fn log_roster_unavailable_health(tenant_id: &str, team_id: &str, unavailable: usize, total: usize) {
    if unavailable == 0 || !should_log_roster_unavailable_health(tenant_id, team_id) {
        return;
    }
    if unavailable == total {
        tracing::warn!(
            tenant_id,
            team_id,
            unavailable,
            total,
            connector_health = true,
            "teams roster: could not read any member's address — every roster member's email \
             was returned null/absent by Graph (cause unknown; NOT necessarily a roster made \
             entirely of guests); this is an UNREADABLE roster, never to be reported or acted on \
             as though the team had zero members"
        );
    } else {
        tracing::warn!(
            tenant_id,
            team_id,
            unavailable,
            total,
            connector_health = true,
            "teams roster: could not read some roster members' addresses — Graph returned no \
             email for them (cause unknown), distinct from a guest/unmatched member who has an \
             address on file; members with a readable address are unaffected"
        );
    }
}

/// A roster member whose address matched a CoralSwarm user. Membership in
/// the account is a separate question — [`TeamRosterResolution::matched_user_ids`]
/// is the subset that sits on `account_members`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TeamRosterKnown {
    pub user_id: Uuid,
    pub email: String,
}

/// What one team-roster walk resolved. `unmatched_emails` is the invite
/// job's unknown-address audience. A missing email is `unavailable`, never
/// an unmatched address. A guest who matches no admit domain is removed
/// before any of these counts. A guest admitted by a verified org domain
/// (item 30) is invited and is not added to `matched_user_ids`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TeamRosterResolution {
    pub matched_user_ids: Vec<Uuid>,
    pub unmatched_emails: Vec<String>,
    pub known: Vec<TeamRosterKnown>,
    pub total: usize,
    pub unavailable: usize,
}

fn push_unique_email(out: &mut Vec<String>, email: String) {
    if !out.iter().any(|existing| existing == &email) {
        out.push(email);
    }
}

/// Network-free, DB-only core of [`resolve_team_roster_members`] — the exact
/// classification + identity-match + batched-membership-check logic, factored
/// out so it is unit-testable against a real Postgres database with a
/// SYNTHETIC roster, without a live Graph call (this crate has no HTTP-mock
/// harness — see `connectors::slack::extract_verified_slack_profile`'s own
/// doc comment for the identical reasoning).
///
/// CS-429: a roster member whose `email` is null/absent/empty increments
/// `unavailable`, NOT `unmatched_emails` — see [`TeamRosterMember::email`]'s
/// own doc comment. A readable address that matches no CoralSwarm account is
/// returned in `unmatched_emails` so the invite job can email it. A guest
/// whose address matches no admit domain is dropped before that split. A
/// guest admitted by a verified org domain — or by the connecting member's
/// domain, until one is verified — is invited and never seated. A blank
/// home tenant does not make the rest of the roster guests.
async fn resolve_roster_members_from(
    control_pool: &PgPool,
    content_pool: &PgPool,
    account_id: Uuid,
    tenant_id: &str,
    team_id: &str,
    roster: Vec<TeamRosterMember>,
) -> Result<TeamRosterResolution, String> {
    let admit_domains = teams_guest_admit_domains(control_pool, account_id).await;
    let roster: Vec<(TeamRosterMember, bool)> = roster
        .into_iter()
        .filter_map(
            |member| match roster_member_disposition(&member, tenant_id, &admit_domains) {
                RosterDisposition::Skip => None,
                RosterDisposition::Member => Some((member, false)),
                RosterDisposition::InviteOnly => Some((member, true)),
            },
        )
        .collect();
    let total = roster.len();
    let mut unavailable = 0usize;
    let mut unmatched_emails = Vec::new();
    let mut known: Vec<TeamRosterKnown> = Vec::new();
    // Identity-matched CANDIDATES, not yet filtered to this account's actual
    // membership — see the single batched check below (CodeRabbit review,
    // PR #667: a per-member `account_members` round trip inside this loop
    // was an N+1, doubling the DB round trips of a roster this walk can
    // already run to hundreds of members deep).
    let mut candidates: Vec<Uuid> = Vec::new();
    for (member, invite_only) in roster {
        let Some(normalized) = roster_invite_address(&member) else {
            // CS-429: still no address after the otherMails read — `Unavailable`,
            // never folded into "guest/unmatched". A directory failure does not
            // land in `unmatched_emails`.
            unavailable += 1;
            continue;
        };
        if invite_only {
            // Domain-admitted guest: invite, never seat, never write a
            // connector identity. A database failure still abandons the
            // roster, same as every other lookup in this walk.
            let user_id: Option<Uuid> = sqlx::query_scalar(
                "SELECT id FROM users WHERE lower(trim(email)) = $1 AND deleted_at IS NULL",
            )
            .bind(&normalized)
            .fetch_optional(control_pool)
            .await
            .map_err(|e| {
                format!(
                    "could not resolve microsoft roster member {} in team {team_id}: users email lookup: {e}",
                    member.aad_user_id
                )
            })?
            .flatten();
            if let Some(user_id) = user_id {
                if !known.iter().any(|row| row.user_id == user_id) {
                    known.push(TeamRosterKnown {
                        user_id,
                        email: normalized,
                    });
                }
            } else {
                push_unique_email(&mut unmatched_emails, normalized);
            }
            continue;
        }
        let name_hint = member
            .display_name
            .clone()
            .unwrap_or_else(|| normalized.clone());
        let user_id = match match_and_persist_verified_microsoft_email(
            control_pool,
            content_pool,
            tenant_id,
            &member.aad_user_id,
            &normalized,
            &name_hint,
        )
        .await
        {
            Ok(Some(id)) => id,
            // A readable address that matches no CoralSwarm account. Keep it
            // — the invite job emails this list. Distinct from `unavailable`.
            Ok(None) => {
                push_unique_email(&mut unmatched_emails, normalized);
                continue;
            }
            // A DATABASE failure mid-walk, not "no match" — the whole roster
            // is abandoned rather than reported short (see this function's
            // own doc comment).
            Err(e) => {
                return Err(format!(
                    "could not resolve microsoft roster member {} in team {team_id}: {e}",
                    member.aad_user_id
                ));
            }
        };
        if !known.iter().any(|row| row.user_id == user_id) {
            known.push(TeamRosterKnown {
                user_id,
                email: normalized,
            });
        }
        candidates.push(user_id);
    }

    log_roster_unavailable_health(tenant_id, team_id, unavailable, total);

    if candidates.is_empty() {
        return Ok(TeamRosterResolution {
            matched_user_ids: Vec::new(),
            unmatched_emails,
            known,
            total,
            unavailable,
        });
    }

    // ONE batched membership check for every identity-matched candidate,
    // replacing the old per-member `SELECT EXISTS(...)` round trip. Same
    // abort-wholesale semantics as every other DB touch in this function: a
    // failure here still returns `Err` and abandons the whole roster, never
    // a partial/short answer.
    let matched: Vec<Uuid> = sqlx::query_scalar(
        "SELECT user_id FROM account_members WHERE account_id = $1 AND user_id = ANY($2)",
    )
    .bind(account_id)
    .bind(&candidates)
    .fetch_all(control_pool)
    .await
    .map_err(|e| format!("account_members batch check: {e}"))?;

    Ok(TeamRosterResolution {
        matched_user_ids: matched,
        unmatched_emails,
        known,
        total,
        unavailable,
    })
}

/// A connected member's delegated token for `GET /teams/{id}/members`.
/// Org accounts use `TokenOwner::Member`. A personal account uses the
/// account-wide token, which is that one member's grant. There is no
/// app-token fallback: item 50 reads the roster on the member's own grant.
async fn delegated_roster_token(
    control: &PgPool,
    dek: &crate::crypto::DekPlane,
    http: &reqwest::Client,
    account_id: Uuid,
) -> Result<String, String> {
    let org: bool =
        sqlx::query_scalar("SELECT clerk_org_id IS NOT NULL FROM accounts WHERE id = $1")
            .bind(account_id)
            .fetch_optional(control)
            .await
            .map_err(|e| e.to_string())?
            .unwrap_or(false);
    let owner = if org {
        let uid: Option<Uuid> = sqlx::query_scalar(
            "SELECT owner_user_id FROM oauth_tokens \
             WHERE account_id = $1 AND provider = 'microsoft' \
               AND credential_invalid_at IS NULL AND owner_user_id IS NOT NULL \
             ORDER BY updated_at DESC LIMIT 1",
        )
        .bind(account_id)
        .fetch_optional(control)
        .await
        .map_err(|e| e.to_string())?;
        let Some(uid) = uid else {
            return Err("no connected member token for this tenant".into());
        };
        TokenOwner::Member(uid)
    } else {
        TokenOwner::AccountWide
    };
    let mut creds =
        crate::routes::connectors::load_creds_bg(control, dek, account_id, "microsoft", owner)
            .await
            .map_err(|e| e.to_string())?
            .ok_or_else(|| "no connected member token for this tenant".to_string())?;
    let connector = MicrosoftConnector;
    connector
        .refresh_if_needed(http, &mut creds)
        .await
        .map_err(|e| e.to_string())?;
    if let Some(refreshed) = creds.refreshed.as_ref() {
        let _ = crate::routes::oauth::store_token_bundle(
            control,
            dek,
            account_id,
            "microsoft",
            owner,
            refreshed,
        )
        .await;
    }
    Ok(creds.access_token)
}

/// Walk a Microsoft Team's roster and resolve it to CoralSwarm user ids —
/// the Teams analog of `connectors::slack::resolve_channel_members`. The
/// member token comes from [`delegated_roster_token`]; there is no app-token
/// fallback. See [`resolve_roster_members_from`] for `unavailable` versus
/// `unmatched_emails`.
///
/// `Err` means the roster could NOT be established with confidence (no
/// connected member token, the Graph roster call failed, or a database error hit
/// mid-walk). A readable address with no CoralSwarm account is returned, not
/// an error, and never a reason to abandon the whole roster. A roster we
/// could not fully enumerate is abandoned WHOLESALE rather than reported
/// short — [`workers::teams_seat_reconcile`] depends on this distinction the
/// exact same way `workers::slack_seat_reconcile` depends on
/// `resolve_channel_members`'s: "we do not know" must never be treated as
/// "the roster is empty, revoke everyone".
///
/// **Two pools** (mirrors `resolve_channel_members`'s split): `control_pool`
/// resolves the CONTROL-only `users` email lookup and the `account_members`
/// membership check; `content_pool` resolves `connector_identities` — the
/// plane this Microsoft tenant's content actually lands on. On the consumer
/// plane callers pass the SAME pool for both.
pub(crate) async fn resolve_team_roster_members(
    control_pool: &PgPool,
    content_pool: &PgPool,
    http: &reqwest::Client,
    dek: &crate::crypto::DekPlane,
    account_id: Uuid,
    tenant_id: &str,
    team_id: &str,
) -> Result<TeamRosterResolution, String> {
    let token = delegated_roster_token(control_pool, dek, http, account_id).await?;
    let mut roster = list_team_roster(http, &token, team_id)
        .await
        .map_err(|e| format!("team roster fetch: {e}"))?;
    fill_missing_emails_from_other_mails(http, &token, tenant_id, &mut roster).await;

    resolve_roster_members_from(
        control_pool,
        content_pool,
        account_id,
        tenant_id,
        team_id,
        roster,
    )
    .await
}

/// Per-call budget on top-level messages pulled per channel/chat backfill.
/// Was a hard, silently-truncating ceiling (200); now a per-call page budget
/// — hitting it produces an explicit [`BackfillPage`] with `complete: false`
/// and a `next_cursor`, which the caller persists onto the watched resource
/// row (`teams_watched_resources.backfill_cursor`/`backfill_complete`) for
/// `workers::graph_subscriptions::tick_backfill_continue` to resume. Raised
/// from 200 now that a capped call converges across resumptions instead of
/// losing the tail permanently.
const TEAMS_BACKFILL_CAP: usize = 500;

/// True if the message is older than the backfill window cutoff (if any).
fn older_than(m: &TeamsMessage, since: Option<DateTime<Utc>>) -> bool {
    match (since, m.created_date_time) {
        (Some(cut), Some(ts)) => ts < cut,
        _ => false,
    }
}

/// Backfill an opted-in channel: top-level messages (paginated up to a
/// [`TEAMS_BACKFILL_CAP`] per-call budget, filtered to the `since` window)
/// plus each one's reply thread, normalized into corals (`source_type =
/// "teams_channel"`). `resume_cursor` (an earlier call's `next_cursor`)
/// continues a previously budget-capped fetch; `None` starts fresh.
pub(crate) async fn fetch_channel_messages(
    http: &reqwest::Client,
    access_token: &str,
    team_id: &str,
    channel_id: &str,
    since: Option<DateTime<Utc>>,
    resume_cursor: Option<&str>,
) -> Result<BackfillPage<NormalizedItem>, ConnectorError> {
    let mut url = resume_cursor.map(str::to_string).unwrap_or_else(|| {
        format!(
            "{GRAPH}/teams/{}/channels/{}/messages?$top={TEAMS_MESSAGE_TOP}",
            urlencoding::encode(team_id),
            urlencoding::encode(channel_id)
        )
    });
    let mut items = Vec::new();
    let mut count = 0usize;
    loop {
        let list: GraphList<TeamsMessage> =
            serde_json::from_value(graph_get(http, access_token, &url).await?)
                .map_err(|e| ConnectorError::Exchange(format!("channel messages decode: {e}")))?;
        for msg in &list.value {
            if older_than(msg, since) {
                continue;
            }
            if let Some(item) = teams_message_to_item(msg, "teams_channel", channel_id) {
                items.push(item);
            }
            // Pull this message's reply thread (best-effort; a failure skips it).
            let replies_url = format!(
                "{GRAPH}/teams/{}/channels/{}/messages/{}/replies?$top={TEAMS_REPLY_TOP}",
                urlencoding::encode(team_id),
                urlencoding::encode(channel_id),
                urlencoding::encode(&msg.id)
            );
            if let Ok(v) = graph_get(http, access_token, &replies_url).await {
                if let Ok(rlist) = serde_json::from_value::<GraphList<TeamsMessage>>(v) {
                    for reply in &rlist.value {
                        if older_than(reply, since) {
                            continue;
                        }
                        if let Some(item) =
                            teams_message_to_item(reply, "teams_channel", channel_id)
                        {
                            items.push(item);
                        }
                    }
                }
            }
            count += 1;
        }
        // Budget is checked only on a PAGE boundary. Graph pagination is
        // page-granular, so a mid-page exit has no resumable cursor for the
        // unprocessed tail of the current page — on a final page (`next_link
        // == None`) that tail would be dropped while the row was marked
        // complete. Consuming the whole page first makes `next_link` an
        // exact, lossless resume point; re-processing an already-ingested
        // page is idempotent via `ingest_message`'s own dedup. This mirrors
        // `fetch_chat_messages`, which checks its cap after the page loop for
        // the same reason.
        if count >= TEAMS_BACKFILL_CAP {
            return Ok(BackfillPage {
                items,
                complete: list.next_link.is_none(),
                next_cursor: list.next_link,
            });
        }
        match list.next_link {
            Some(next) => url = next,
            None => break,
        }
    }
    Ok(BackfillPage {
        items,
        next_cursor: None,
        complete: true,
    })
}

/// Backfill an opted-in chat's messages (paginated up to a [`TEAMS_BACKFILL_
/// CAP`] per-call budget, filtered to the `since` window). `source_type =
/// "teams_chat"`. `resume_cursor` continues a previously budget-capped fetch.
pub(crate) async fn fetch_chat_messages(
    http: &reqwest::Client,
    access_token: &str,
    chat_id: &str,
    since: Option<DateTime<Utc>>,
    resume_cursor: Option<&str>,
) -> Result<BackfillPage<NormalizedItem>, ConnectorError> {
    let mut url = resume_cursor.map(str::to_string).unwrap_or_else(|| {
        format!(
            "{GRAPH}/chats/{}/messages?$top={TEAMS_MESSAGE_TOP}",
            urlencoding::encode(chat_id)
        )
    });
    let mut items = Vec::new();
    loop {
        let list: GraphList<TeamsMessage> =
            serde_json::from_value(graph_get(http, access_token, &url).await?)
                .map_err(|e| ConnectorError::Exchange(format!("chat messages decode: {e}")))?;
        for m in &list.value {
            if older_than(m, since) {
                continue;
            }
            if let Some(item) = teams_message_to_item(m, "teams_chat", chat_id) {
                items.push(item);
            }
        }
        if items.len() >= TEAMS_BACKFILL_CAP {
            return Ok(BackfillPage {
                items,
                complete: list.next_link.is_none(),
                next_cursor: list.next_link,
            });
        }
        match list.next_link {
            Some(next) => url = next,
            None => break,
        }
    }
    Ok(BackfillPage {
        items,
        next_cursor: None,
        complete: true,
    })
}

/// Normalize a Teams chatMessage into a coral. Returns `None` for non-user
/// messages (system events) and empty bodies. `container_id` (channel or chat
/// id) namespaces the `source_id` so ids stay unique within a graph and seeds
/// the `thread_id`. `pub(crate)` so the live webhook can reuse it.
pub(crate) fn teams_message_to_item(
    m: &TeamsMessage,
    source_type: &str,
    container_id: &str,
) -> Option<NormalizedItem> {
    if m.message_type.as_deref().unwrap_or("message") != "message" {
        return None;
    }
    // Skip messages authored by an application/bot — including CoralSwarm's own
    // prompt & confirmation cards — so the bot never ingests its own noise
    // (humans-only, mirroring Slack's bot_id filter). CS-122.
    if m.from
        .as_ref()
        .and_then(|f| f.application.as_ref())
        .is_some()
    {
        return None;
    }
    // Graph's original creation time is the privacy boundary for From-now
    // watches. An edit must not make an old message new, and an absent date
    // must never be replaced by the time we happened to fetch it.
    let timestamp = m.created_date_time?;
    let body = match m.body.as_ref() {
        Some(b) => body_to_text(b),
        None => return None,
    };
    let text = body.trim();
    if text.is_empty() {
        return None;
    }
    let user = m.from.as_ref().and_then(|f| f.user.as_ref());
    let author_name = user.and_then(|u| u.display_name.clone());
    let author_id = user.and_then(|u| u.id.clone());
    let content = match m.subject.as_deref().filter(|s| !s.is_empty()) {
        Some(subject) => format!("{subject}\n\n{text}"),
        None => text.to_string(),
    };
    Some(NormalizedItem {
        source_type: source_type.into(),
        source_id: format!("{container_id}:{}", m.id),
        content,
        participants: author_name.clone().into_iter().collect(),
        timestamp,
        scope: None,
        thread_id: Some(container_id.to_string()),
        // The container id is the channel/chat GUID; `ingest_message` resolves
        // its human display name from `teams_watched_resources`.
        channel_id: Some(container_id.to_string()),
        author_id,
        author_name,
    })
}

// ── Teams app (RSC) live + meeting transcripts (CS-121) ───────────────────────────

fn teams_app_id() -> Option<String> {
    std::env::var("TEAMS_APP_ID").ok().filter(|s| !s.is_empty())
}

/// The per-tenant subscription resource that streams a change notification for
/// every transcript of any meeting our Teams app is installed in. Created once
/// per account+tenant at install time (must exist before a meeting starts).
pub(crate) fn transcript_subscription_resource() -> Option<String> {
    teams_app_id()
        .map(|id| format!("appCatalogs/teamsApps/{id}/installedToOnlineMeetings/getAllTranscripts"))
}

/// Fetch a meeting transcript's content by its notification resource path
/// (`users('u')/onlineMeetings('m')/transcripts('t')`), returning plain text.
pub(crate) async fn fetch_transcript_text(
    http: &reqwest::Client,
    access_token: &str,
    transcript_resource: &str,
) -> Result<String, ConnectorError> {
    let meeting = meeting_id_from_resource(transcript_resource)
        .filter(|id| !id.is_empty())
        .ok_or_else(|| ConnectorError::Exchange("invalid transcript meeting".into()))?;
    let transcript = transcript_id_from_resource(transcript_resource)
        .filter(|id| !id.is_empty())
        .ok_or_else(|| ConnectorError::Exchange("invalid transcript identity".into()))?;
    // Never request the notification's arbitrary resource path. This member's
    // delegated /me namespace and exact identifiers are the only destination.
    let url = format!(
        "{}/me/onlineMeetings/{}/transcripts/{}/content",
        super::teams_targeted_transcripts::graph_origin(),
        urlencoding::encode(&meeting),
        urlencoding::encode(&transcript)
    );
    let body = fetch_transcript_content(http, access_token, &url).await?;
    Ok(vtt_to_text(&body))
}

/// Metadata only: establish the original transcript time before any body read.
pub(crate) async fn transcript_created_at(
    http: &reqwest::Client,
    token: &str,
    resource: &str,
) -> Result<Option<chrono::DateTime<chrono::Utc>>, ConnectorError> {
    let (Some(meeting), Some(transcript)) = (
        meeting_id_from_resource(resource),
        transcript_id_from_resource(resource),
    ) else {
        return Ok(None);
    };
    let url = format!(
        "{}/me/onlineMeetings/{}/transcripts/{}",
        super::teams_targeted_transcripts::graph_origin(),
        urlencoding::encode(&meeting),
        urlencoding::encode(&transcript)
    );
    let value = graph_get(http, token, &url).await?;
    Ok(transcript_metadata_created_at(&value, &transcript))
}

fn transcript_metadata_created_at(
    value: &serde_json::Value,
    transcript: &str,
) -> Option<chrono::DateTime<chrono::Utc>> {
    if value.get("id").and_then(serde_json::Value::as_str) != Some(transcript) {
        return None;
    }
    value
        .get("createdDateTime")
        .and_then(serde_json::Value::as_str)
        .and_then(|date| chrono::DateTime::parse_from_rfc3339(date).ok())
        .map(|date| date.with_timezone(&chrono::Utc))
}

/// Parse the `onlineMeetings('id')` segment from a transcript resource path, for
/// namespacing the coral and seeding `thread_id`.
pub(crate) fn meeting_id_from_resource(resource: &str) -> Option<String> {
    let idx = resource.find("onlineMeetings")?;
    let rest = &resource[idx..];
    let start = rest.find('\'')? + 1;
    let end = rest[start..].find('\'')? + start;
    Some(rest[start..end].to_string())
}

/// Parse the `transcripts('id')` segment. The sweep stores
/// `{meeting_id}:{transcript_id}`; the webhook must use the same key or the
/// same transcript is ingested twice.
pub(crate) fn transcript_id_from_resource(resource: &str) -> Option<String> {
    let idx = resource.find("/transcripts")?;
    let rest = &resource[idx..];
    let start = rest.find('\'')? + 1;
    let end = rest[start..].find('\'')? + start;
    Some(rest[start..end].to_string())
}

/// From a transcript resource path
/// (`users('u')/onlineMeetings('m')/transcripts('t')`), return the containing
/// `onlineMeeting` resource path (`users('u')/onlineMeetings('m')`) so its
/// metadata (including the meeting's real start time) can be fetched.
pub(crate) fn online_meeting_resource_from_transcript(resource: &str) -> Option<String> {
    let idx = resource.find("/transcripts")?;
    Some(resource[..idx].to_string())
}

#[derive(Debug, Deserialize)]
struct OnlineMeetingDetails {
    #[serde(rename = "startDateTime")]
    start_date_time: Option<String>,
}

/// Fetch an `onlineMeeting`'s real start time from its Graph metadata, given
/// the meeting's resource path (e.g. `users('u')/onlineMeetings('m')`, as
/// returned by `online_meeting_resource_from_transcript`). Used by the live
/// transcript-available webhook (CS-121) to stamp the atom's `timestamp` with
/// the meeting's actual start — mirroring how the backfill path
/// (`fetch_meeting_transcripts`) uses the calendar event's `start`.
///
/// `startDateTime` on this Graph resource is a plain ISO-8601 UTC string
/// (unlike calendar events' `{dateTime, timeZone}` object), so this is parsed
/// with `DateTime::parse_from_rfc3339` rather than `parse_graph_dt`.
pub(crate) async fn fetch_online_meeting_start(
    http: &reqwest::Client,
    access_token: &str,
    meeting_resource: &str,
) -> Option<DateTime<Utc>> {
    let url = format!("{GRAPH}/{}", meeting_resource.trim_start_matches('/'));
    let v = graph_get(http, access_token, &url).await.ok()?;
    let details: OnlineMeetingDetails = serde_json::from_value(v).ok()?;
    details
        .start_date_time
        .as_deref()
        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&Utc))
}

/// An account's owner user id, to attribute tenant-wide transcript ingests that
/// aren't tied to a specific delegated caller.
pub(crate) async fn account_owner_user(
    pool: &PgPool,
    account_id: Uuid,
) -> Result<Option<Uuid>, ConnectorError> {
    let row: Option<(Uuid,)> = sqlx::query_as("SELECT owner_user_id FROM accounts WHERE id = $1")
        .bind(account_id)
        .fetch_optional(pool)
        .await
        .map_err(|e| ConnectorError::Internal(format!("account_owner_user: {e}")))?;
    Ok(row.map(|(id,)| id))
}

// ── Teams meeting transcripts (CS-119 phase 3) ────────────────────────────────────

/// Recent calendar events scanned for meeting transcripts.
const TRANSCRIPT_EVENT_SCAN: usize = 15;

#[derive(Debug, Deserialize)]
struct TranscriptEvent {
    subject: Option<String>,
    start: Option<GraphDateTime>,
    #[serde(rename = "isOnlineMeeting")]
    is_online_meeting: Option<bool>,
    #[serde(rename = "onlineMeeting")]
    online_meeting: Option<EventOnlineMeeting>,
}

#[derive(Debug, Deserialize)]
struct EventOnlineMeeting {
    #[serde(rename = "joinUrl")]
    join_url: Option<String>,
}

#[derive(Debug, Deserialize)]
struct OnlineMeeting {
    id: String,
}

#[derive(Debug, Deserialize)]
struct MeetingTranscript {
    id: String,
}

/// Speaker-attributed transcript body. Selected with `$format` (equivalent
/// to `Accept: text/vtt`).
const TRANSCRIPT_ATTRIBUTED_FORMAT: &str = "text/vtt";
/// Speaker-unattributed transcript body. Graph only honors this on the
/// `Accept` header — `$format` cannot select it.
const TRANSCRIPT_UNATTRIBUTED_ACCEPT: &str = "application/vnd.microsoft.graph.transcript+text";

/// Raw-text Graph GET (transcript content is VTT, not JSON).
async fn graph_get_text(
    http: &reqwest::Client,
    access_token: &str,
    url: &str,
) -> Result<String, ConnectorError> {
    graph_get_text_accept(http, access_token, url, None).await
}

async fn graph_get_text_accept(
    http: &reqwest::Client,
    access_token: &str,
    url: &str,
    accept: Option<&str>,
) -> Result<String, ConnectorError> {
    let mut req = http.get(url).bearer_auth(access_token);
    if let Some(accept) = accept {
        req = req.header(reqwest::header::ACCEPT, accept);
    }
    #[cfg(feature = "e2e")]
    let req = e2e_graph_request(req)?;
    let resp = req
        .send()
        .await
        .map_err(|e| ConnectorError::Exchange(e.to_string()))?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(ConnectorError::upstream(status, graph_text_error_message(url, status.as_u16(), &body)));
    }
    resp.text()
        .await
        .map_err(|e| ConnectorError::Exchange(format!("graph text read: {e}")))
}

/// Download `/content` for a transcript. Tries speaker-attributed `text/vtt`
/// first. A tenant that disabled speaker attribution answers 403
/// `SpeakerAttributionNotAllowed`; the same URL is then requested again with
/// `Accept: application/vnd.microsoft.graph.transcript+text` and no `$format`.
/// `GraphAccessToTranscriptsDisabled` is not this case and is returned as-is.
async fn fetch_transcript_content(
    http: &reqwest::Client,
    access_token: &str,
    content_url: &str,
) -> Result<String, ConnectorError> {
    let attributed = format!("{content_url}?$format={TRANSCRIPT_ATTRIBUTED_FORMAT}");
    match graph_get_text(http, access_token, &attributed).await {
        Ok(body) => Ok(body),
        Err(e) if speaker_attribution_not_allowed(&e) => {
            tracing::debug!("transcript speaker attribution refused; fetching unattributed text");
            graph_get_text_accept(
                http,
                access_token,
                content_url,
                Some(TRANSCRIPT_UNATTRIBUTED_ACCEPT),
            )
            .await
        }
        Err(e) => Err(e),
    }
}

/// Graph puts `innerError.code` after `error.message`. Keep that code in the
/// error string even when the message is longer than the snippet we store.
fn graph_text_error_message(url: &str, status: u16, body: &str) -> String {
    let code = serde_json::from_str::<serde_json::Value>(body)
        .ok()
        .and_then(|v| {
            v.pointer("/error/innerError/code")
                .and_then(|c| c.as_str())
                .map(str::to_string)
        })
        .filter(|c| !c.is_empty());
    let snippet: String = body.chars().take(300).collect();
    match code {
        Some(code) => format!("graph GET (text) {url} failed: {status}: {code} {snippet}"),
        None => format!("graph GET (text) {url} failed: {status}: {snippet}"),
    }
}

/// Reduce a WebVTT transcript to plain text: drop the header, cue numbers, and
/// timestamp lines; collapse consecutive duplicate lines.
pub(super) fn vtt_to_text(vtt: &str) -> String {
    let mut lines: Vec<&str> = Vec::new();
    for raw in vtt.lines() {
        let line = raw.trim();
        if line.is_empty()
            || line == "WEBVTT"
            || line.contains("-->")
            || line.chars().all(|c| c.is_ascii_digit())
        {
            continue;
        }
        if lines.last() != Some(&line) {
            lines.push(line);
        }
    }
    lines.join("\n")
}

/// Backfill recent Teams meeting transcripts as corals (`teams_transcript`).
/// Delegated discovery: walk recent calendar events that are online meetings,
/// resolve each to its `onlineMeeting`, list its transcripts, and fetch VTT.
/// Legacy calendar discovery has no meeting-specific consent context. Fail closed.
pub(crate) async fn fetch_meeting_transcripts(
    _http: &reqwest::Client,
    _access_token: &str,
    _content_pool: Option<&PgPool>,
    _account_id: Uuid,
) -> Result<Vec<NormalizedItem>, ConnectorError> {
    Ok(Vec::new())
}

/// Only explicitly enrolled meetings belonging to this exact member are fetched.
/// Calendar events and ordinary Teams watches never expand this set.
pub(crate) async fn fetch_consented_meeting_transcripts(
    state: &std::sync::Arc<crate::AppState>,
    http: &reqwest::Client,
    access_token: &str,
    control: &PgPool,
    content: &PgPool,
    account: Uuid,
    member: Uuid,
    graph: Uuid,
) -> Result<Vec<NormalizedItem>, ConnectorError> {
    // Plane-aware identity: org keeps the member grant; a personal account only
    // resolves to its real owner's work/school account-wide grant (the single
    // ownerless Microsoft row, so the caller's account-wide token is that grant).
    let identity = match crate::routes::teams_onboarding::capture_identity(
        control,
        account,
        member,
        crate::routes::teams_onboarding::ExpectedPlane::Any,
    )
    .await
    {
        Ok(identity) => identity,
        Err(crate::routes::teams_onboarding::CaptureIdentityError::Unavailable(e)) => {
            return Err(ConnectorError::Internal(e.to_string()))
        }
        Err(_) => return Ok(Vec::new()),
    };
    let (tenant, aad) = identity.pair();
    if tenant.is_empty() {
        return Ok(Vec::new());
    }
    let org: Option<String> = sqlx::query_scalar("SELECT clerk_org_id FROM accounts WHERE id=$1")
        .bind(account)
        .fetch_optional(control)
        .await
        .map_err(|e| ConnectorError::Internal(e.to_string()))?
        .flatten();
    if let Some(org) = org {
        if !matches!(crate::middleware::auth::live_org_membership_for_capture(control, &org, member).await,
            Ok(Some((id, _))) if id == account)
            || !crate::routes::teams_ingest::org_microsoft_teams_enabled_for(state, &org).await
        {
            return Ok(Vec::new());
        }
    }
    let meetings = crate::services::teams_meeting_capture::active_meetings(
        content, account, member, &tenant, &aad,
    )
    .await
    .map_err(|e| ConnectorError::Internal(e.to_string()))?;
    let mut items = Vec::new();
    for (meeting, destination) in meetings {
        if destination != graph {
            continue;
        }
        // Recheck immediately before the content-producing provider request.
        if crate::services::teams_meeting_capture::active_graph(
            content, account, member, &tenant, &aad, &meeting,
        )
        .await
        .map_err(|e| ConnectorError::Internal(e.to_string()))?
            != Some(graph)
        {
            continue;
        }
        let verified = crate::connectors::teams_targeted_transcripts::resolve(
            http,
            access_token,
            crate::connectors::teams_targeted_transcripts::Target::MeetingId(meeting.clone()),
        )
        .await
        .map_err(targeted_transcript_error)?;
        let (Some(start), Some(end)) = (
            verified
                .start
                .as_deref()
                .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                .map(|d| d.with_timezone(&chrono::Utc)),
            verified
                .end
                .as_deref()
                .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                .map(|d| d.with_timezone(&chrono::Utc)),
        ) else {
            continue;
        };
        if crate::services::teams_meeting_capture::matches_occurrence(
            content, account, member, &tenant, &aad, &meeting, start, end,
        )
        .await
        .map_err(|e| ConnectorError::Internal(e.to_string()))?
            != Some(graph)
        {
            continue;
        }
        let fetched = crate::connectors::teams_targeted_transcripts::fetch_verified(
            http,
            access_token,
            verified,
        )
        .await
        .map_err(targeted_transcript_error)?;
        items.extend(fetched.items);
    }
    Ok(items)
}

/// A targeted-transcript failure as a connector error: a Graph refusal
/// keeps its status ([`ConnectorError::UpstreamStatus`]), so a receive job
/// that hits a 403 or 404 completes instead of retrying until dead.
pub(crate) fn targeted_transcript_error(
    error: crate::connectors::teams_targeted_transcripts::Error,
) -> ConnectorError {
    match error {
        crate::connectors::teams_targeted_transcripts::Error::Graph { status, code } => {
            ConnectorError::upstream_code(status, format!(
                "Microsoft transcript request {status}: {}",
                code.unwrap_or_default()
            ))
        }
        other => ConnectorError::Exchange(other.to_string()),
    }
}

/// Whether recent online meetings were organized by the signed-in user,
/// attended by them, or both. One flag each — a person can do both, and the
/// transcript paths are different (item 51).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct OnlineMeetingCoverage {
    pub organized_any: bool,
    pub attended_any: bool,
}

#[derive(Debug, Deserialize)]
struct SeatEvent {
    #[serde(rename = "isOnlineMeeting")]
    is_online_meeting: Option<bool>,
    #[serde(rename = "isOrganizer")]
    is_organizer: Option<bool>,
}

/// Classify recent calendar events. `Err` on the kill switch so the caller
/// stops instead of walking the rest of the window.
pub(crate) async fn recent_online_meeting_coverage(
    http: &reqwest::Client,
    access_token: &str,
) -> Result<OnlineMeetingCoverage, ConnectorError> {
    let url = format!(
        "{GRAPH}/me/events?$top={TRANSCRIPT_EVENT_SCAN}&$select=isOnlineMeeting,isOrganizer&$orderby=start/dateTime%20desc"
    );
    let events: GraphList<SeatEvent> =
        serde_json::from_value(graph_get(http, access_token, &url).await?)
            .map_err(|e| ConnectorError::Exchange(format!("meeting seat decode: {e}")))?;
    let mut coverage = OnlineMeetingCoverage::default();
    for ev in events.value {
        if !ev.is_online_meeting.unwrap_or(false) {
            continue;
        }
        if ev.is_organizer.unwrap_or(false) {
            coverage.organized_any = true;
        } else {
            coverage.attended_any = true;
        }
    }
    Ok(coverage)
}

// ── Teams live capture + backstop (CS-119 phase 2) ────────────────────────────────

/// Build the Graph subscription resource for a watched Teams target.
pub(crate) fn teams_subscription_resource(
    kind: &str,
    team_id: &str,
    channel_id: &str,
    chat_id: &str,
) -> Option<String> {
    match kind {
        "channel" if !team_id.is_empty() && !channel_id.is_empty() => {
            Some(format!("teams/{team_id}/channels/{channel_id}/messages"))
        }
        "chat" if !chat_id.is_empty() => Some(format!("chats/{chat_id}/messages")),
        _ => None,
    }
}

/// From a subscribed Teams resource, recover `(source_type, container_id)`:
/// `teams/{t}/channels/{c}/messages` → `("teams_channel", c)`;
/// `chats/{c}/messages` → `("teams_chat", c)`.
pub(crate) fn parse_subscribed_teams_resource(resource: &str) -> Option<(String, String)> {
    if let Some(idx) = resource.find("/channels/") {
        let after = &resource[idx + "/channels/".len()..];
        let channel_id = after.split('/').next().unwrap_or("");
        if !channel_id.is_empty() {
            return Some(("teams_channel".into(), channel_id.into()));
        }
    }
    if let Some(rest) = resource.strip_prefix("chats/") {
        let chat_id = rest.split('/').next().unwrap_or("");
        if !chat_id.is_empty() {
            return Some(("teams_chat".into(), chat_id.into()));
        }
    }
    None
}

/// Fetch a single Teams message by its OData resource path (from a change
/// notification's `resource` field, e.g. `teams('t')/channels('c')/messages('id')`).
pub(crate) async fn fetch_teams_message(
    http: &reqwest::Client,
    access_token: &str,
    odata_resource: &str,
) -> Result<TeamsMessage, ConnectorError> {
    let url = format!("{GRAPH}/{}", odata_resource.trim_start_matches('/'));
    serde_json::from_value(graph_get(http, access_token, &url).await?)
        .map_err(|e| ConnectorError::Exchange(format!("teams message decode: {e}")))
}

/// Resolve every watch row for a Teams channel/chat by container id, bumping
/// `last_ingested_at` on each. Empty when nobody is watching. S4: one row per
/// member on the delegated path — callers must fan out fetches onto each
/// watcher's own token (org Member / personal AccountWide).
pub(crate) async fn teams_watched_by_container(
    pool: &PgPool,
    account_id: Uuid,
    container_id: &str,
) -> Result<Vec<(Uuid, Uuid, String, String)>, ConnectorError> {
    let mut tx = scoped_tx(pool, account_id, "teams_watched_by_container").await?;
    let rows: Vec<(Uuid, Uuid, String, String)> = sqlx::query_as(
        r#"
        UPDATE teams_watched_resources
        SET last_ingested_at = now()
        WHERE account_id = $1 AND (channel_id = $2 OR chat_id = $2)
        RETURNING graph_id, user_id, tenant_id, auth_mode
        "#,
    )
    .bind(account_id)
    .bind(container_id)
    .fetch_all(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("teams_watched_by_container: {e}")))?;
    tx.commit()
        .await
        .map_err(|e| ConnectorError::Internal(format!("teams_watched_by_container commit: {e}")))?;
    Ok(rows)
}

/// Human display name for a watched Teams channel/chat (the label the user
/// picked at opt-in, stored in `teams_watched_resources`). Used to stamp an
/// atom's `channel_name` so its source surfaces as e.g. `#General` rather than
/// the raw container GUID. `None` when the container isn't watched or has no
/// stored label.
pub(crate) async fn teams_channel_name_by_container(
    pool: &PgPool,
    account_id: Uuid,
    container_id: &str,
) -> Option<String> {
    // Best-effort by contract (`Option`, errors swallowed) — a scope failure
    // degrades to "no stored label", exactly as a query error always has.
    //
    // `scoped_tx` (== `begin_tenant_system_scoped`), NOT plain
    // `begin_account_scoped` (m365 Train 2 PR 2.5 cross-plane sweep): this
    // used to be the one straggler in this file still calling
    // `begin_account_scoped` directly — its sibling `teams_watched_by_
    // container` right above already used `scoped_tx`. Harmless as long as
    // `pool` was provably always the consumer pool (`routes::teams_ingest.
    // rs`'s pre-2.5 unconditional org 403 kept it that way — see `routes::
    // graph_webhook`'s own `GraphIngestTarget::content_pool` doc comment for
    // the matching POOL-selection half of this same bug), but this function
    // reads `teams_watched_resources`, which carries migrations_tenant/
    // 0009's `member_ocean_isolation` policy — `begin_account_scoped` sets
    // neither `app.tenant_actor` nor `app.user_id`, so even after that pool
    // fix, an RLS-enforcing tenant pool would still silently deny this read.
    // No acting end user is available here (this runs off a background
    // ingest/backfill path, not a request), so the system-actor bypass is
    // the correct primitive — same as `teams_watched_by_container`'s.
    let mut tx = scoped_tx(pool, account_id, "teams_channel_name_by_container")
        .await
        .ok()?;
    let name = sqlx::query_scalar::<_, String>(
        r#"
        SELECT display_name FROM teams_watched_resources
        WHERE account_id = $1 AND (channel_id = $2 OR chat_id = $2)
              AND display_name <> ''
        LIMIT 1
        "#,
    )
    .bind(account_id)
    .bind(container_id)
    .fetch_optional(&mut *tx)
    .await
    .ok()
    .flatten();
    tx.rollback().await.ok();
    name
}

/// Every watched Teams resource for an account, for the periodic re-poll:
/// `(kind, team_id, channel_id, chat_id, graph_id, user_id, tenant_id, auth_mode)`.
#[allow(clippy::type_complexity)]
pub(crate) async fn teams_watched_for_account(
    pool: &PgPool,
    account_id: Uuid,
) -> Result<Vec<(String, String, String, String, Uuid, Uuid, String, String)>, ConnectorError> {
    let mut tx = scoped_tx(pool, account_id, "teams_watched_for_account").await?;
    let rows = sqlx::query_as(
        r#"
        SELECT kind, team_id, channel_id, chat_id, graph_id, user_id, tenant_id, auth_mode
        FROM teams_watched_resources
        WHERE account_id = $1
        "#,
    )
    .bind(account_id)
    .fetch_all(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("teams_watched_for_account: {e}")));
    tx.rollback().await.ok();
    rows
}

/// Accounts with at least one watched Teams resource (backstop work set).
///
/// **Deliberately cross-account** (see this module's RLS-scoping note): the
/// sweep's entry point. Reads through `resolve_teams_watched_accounts()`, the
/// narrow `SECURITY DEFINER` resolver from migration
/// `0086_webhook_share_bootstrap_functions` — bare `account_id`s only. Each
/// account's actual watch rows are then read under its own scope by
/// `teams_watched_for_account` above.
pub(crate) async fn accounts_with_teams_watched(
    pool: &PgPool,
) -> Result<Vec<Uuid>, ConnectorError> {
    let rows: Vec<(Uuid,)> = sqlx::query_as("SELECT * FROM resolve_teams_watched_accounts()")
        .fetch_all(pool)
        .await
        .map_err(|e| ConnectorError::Internal(format!("accounts_with_teams_watched: {e}")))?;
    Ok(rows.into_iter().map(|(a,)| a).collect())
}

// ── connector_installs routing (control-plane, migration 0121) ───────────────
//
// The Microsoft analog of `routes::slack::resolve_slack_account_ids` — see
// that function's own doc comment for the full self-healing design this
// mirrors (`0121_connector_installs`'s "OPERATOR-FACING" migration header has
// the rest of the rationale). `connector_installs` first (fast path), falling
// back on a miss to the authoritative `oauth_tokens` table. Both SQL
// resolvers this calls (`resolve_connector_install_account_ids`,
// `resolve_oauth_token_account_ids`) were added by 0121 already
// provider-generic — no new migration function is needed for Microsoft.
//
// Differs from Slack's version in exactly one way: Slack has a SECOND source
// table (`slack_integrations`, the legacy pre-`oauth_tokens` install record)
// to fall back through; Microsoft has never had an equivalent second table,
// so this fallback is one query, not two.
//
// Replaces `SELECT resolve_oauth_account_id($1, $2)` (0084, `LIMIT 1`, no
// `ORDER BY`) — the sole remaining Rust caller of that arbitrary-pick
// resolver, per 0121's own header — with a deterministic, multi-account
// result; `routes::teams_bot::process_activity` fans out over every match
// instead of silently picking one.

/// Every CoralSwarm account that has installed the Microsoft tenant
/// `tenant_id` (AAD `tid`) — deterministic (`created_at, account_id` order
/// via `resolve_connector_install_account_ids`), never an arbitrary pick.
pub(crate) async fn resolve_microsoft_account_ids(
    pool: &PgPool,
    tenant_id: &str,
) -> Result<Vec<Uuid>, ConnectorError> {
    let indexed: Vec<Uuid> =
        sqlx::query_scalar("SELECT resolve_connector_install_account_ids($1, $2)")
            .bind("microsoft")
            .bind(tenant_id)
            .fetch_all(pool)
            .await
            .map_err(|e| ConnectorError::Internal(format!("resolve_microsoft_account_ids: {e}")))?;
    if !indexed.is_empty() {
        return Ok(indexed);
    }

    if microsoft_negative_resolve_cache_hit(tenant_id) {
        // Recently confirmed nowhere — see `routes::slack::resolve_slack_
        // account_ids`'s own doc comment for why this suppression is safe and
        // what it bounds (a tenant whose local rows are all gone but is still
        // delivering signed Teams/Graph notifications would otherwise re-run
        // the fallback query on every single one).
        return Ok(Vec::new());
    }

    // ── Fallback: the AUTHORITATIVE table itself, not the index ──
    // Foundation PR 1.1 (migration 0175, amended): `resolve_oauth_token_
    // account_ids` is pinned to `owner_user_id IS NULL` rows — i.e.
    // `connectors::TokenOwner::AccountWide` — so it can never return one
    // account_id more than once for a workspace with several per-member
    // rows (which would silently duplicate this fan-out below once Train 3
    // mints per-member Microsoft tokens). See that migration's own comment
    // for the full self-heal-vs-fan-out reasoning.
    let found: Vec<Uuid> = sqlx::query_scalar("SELECT resolve_oauth_token_account_ids($1, $2)")
        .bind("microsoft")
        .bind(tenant_id)
        .fetch_all(pool)
        .await
        .map_err(|e| {
            ConnectorError::Internal(format!("resolve_microsoft_account_ids fallback: {e}"))
        })?;

    if found.is_empty() {
        cache_microsoft_negative_resolve(tenant_id);
        tracing::warn!(
            tenant_id = %tenant_id,
            connector_health = true,
            "no account found for Microsoft tenant anywhere — checked connector_installs AND \
             the oauth_tokens fallback; tenant_id did not resolve to any account (never \
             installed, fully uninstalled, or Teams/Graph is still delivering signed \
             notifications with no matching local row at all)"
        );
        return Ok(found);
    }

    // Distinct, differently-worded from the "found nothing" case above — see
    // `routes::slack::resolve_slack_account_ids`'s own doc comment for why a
    // healed miss and a genuine miss are different operator-facing events.
    tracing::warn!(
        tenant_id = %tenant_id,
        account_ids = ?found,
        connector_health = true,
        "connector_installs self-healed for Microsoft tenant — found via the oauth_tokens \
         fallback with no connector_installs row; a route bypassing record_connector_install \
         must have created this install"
    );
    for &account_id in &found {
        repair_microsoft_connector_install_best_effort(pool, account_id, tenant_id).await;
    }

    Ok(found)
}

/// Best-effort repair write for [`resolve_microsoft_account_ids`]'s
/// self-healing fallback — same discipline as `routes::slack::
/// repair_connector_install_best_effort` (correctness already comes from the
/// fallback query above; a failed repair here costs only one extra fallback
/// query on the next resolve for this tenant, nothing else). `external_user_id`
/// is always `None`: the fallback resolves accounts without tracking which
/// `oauth_tokens` row each came from.
async fn repair_microsoft_connector_install_best_effort(
    pool: &PgPool,
    account_id: Uuid,
    tenant_id: &str,
) {
    let mut tx = match scoped_tx(pool, account_id, "connector_installs repair").await {
        Ok(tx) => tx,
        Err(e) => {
            tracing::warn!(
                error = %e, %account_id, tenant_id = %tenant_id, connector_health = true,
                "self-heal: failed to open tx for connector_installs repair (non-fatal — will \
                 retry on the next resolve for this tenant)"
            );
            return;
        }
    };
    if let Err(e) = crate::routes::oauth::record_connector_install(
        &mut tx,
        account_id,
        "microsoft",
        tenant_id,
        None,
        // Microsoft self-heal repair — always account-wide, byte-identical
        // to before this parameter existed.
        None,
    )
    .await
    {
        tracing::warn!(
            error = %e, %account_id, tenant_id = %tenant_id, connector_health = true,
            "self-heal: failed to repair connector_installs (non-fatal — will retry on the next \
             resolve for this tenant)"
        );
        return;
    }
    if let Err(e) = tx.commit().await {
        tracing::warn!(
            error = %e, %account_id, tenant_id = %tenant_id, connector_health = true,
            "self-heal: failed to commit connector_installs repair (non-fatal — will retry on \
             the next resolve for this tenant)"
        );
    }
}

// ── Negative-result cache (bounds fallback query volume) ─────────────────────
// Mirrors `routes::slack::negative_resolve_cache` — see its own doc comment
// for the full reasoning. Microsoft-only, so keyed on `tenant_id` alone:
// Slack's cache carries a `(provider, workspace_id)` tuple because it is
// pre-emptively provider-generic; this one has exactly one caller and one
// provider, so the extra dimension would be dead weight.
fn microsoft_negative_resolve_cache(
) -> &'static std::sync::Mutex<std::collections::HashMap<String, std::time::Instant>> {
    static C: std::sync::OnceLock<
        std::sync::Mutex<std::collections::HashMap<String, std::time::Instant>>,
    > = std::sync::OnceLock::new();
    C.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()))
}

/// Same TTL as `routes::slack::NEGATIVE_RESOLVE_TTL` — see that constant's
/// own doc comment for the tradeoff.
const MICROSOFT_NEGATIVE_RESOLVE_TTL: std::time::Duration = std::time::Duration::from_secs(300);

/// Both accessors below deliberately do NOT `.unwrap()` the mutex lock: a
/// poisoned lock (some other thread panicked while holding it) must degrade
/// this cache to "nothing cached" / "write skipped", never propagate that
/// panic into an inbound Teams/Graph webhook request. The cost of losing the
/// cache is one extra fallback query per resolve until the process restarts
/// — cheap next to taking down request handling over a cache.
fn microsoft_negative_resolve_cache_hit(tenant_id: &str) -> bool {
    let Ok(cache) = microsoft_negative_resolve_cache().lock() else {
        return false;
    };
    matches!(cache.get(tenant_id), Some(t) if t.elapsed() < MICROSOFT_NEGATIVE_RESOLVE_TTL)
}

fn cache_microsoft_negative_resolve(tenant_id: &str) {
    let Ok(mut cache) = microsoft_negative_resolve_cache().lock() else {
        return;
    };
    // Sweep expired entries before inserting so this cache cannot grow
    // unboundedly — every distinct tenant_id that ever misses adds a key
    // nothing else would otherwise remove. Piggybacked on the (already rare
    // — see `resolve_microsoft_account_ids`'s own doc comment) negative-write
    // path rather than a separate timer/task, so a healthy tenant population
    // costs this function nothing extra.
    cache.retain(|_, inserted_at| inserted_at.elapsed() < MICROSOFT_NEGATIVE_RESOLVE_TTL);
    cache.insert(tenant_id.to_string(), std::time::Instant::now());
}

// ── Tests ───────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // A minimal unsigned JWT: header.payload.signature with the given payload.
    fn make_id_token(payload: &serde_json::Value) -> String {
        let b64 = |v: &[u8]| base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(v);
        let header = b64(br#"{"alg":"none","typ":"JWT"}"#);
        let body = b64(serde_json::to_string(payload).unwrap().as_bytes());
        format!("{header}.{body}.sig")
    }

    #[test]
    fn decodes_tid_and_oid_from_id_token() {
        let token = make_id_token(&serde_json::json!({
            "tid": "tenant-123",
            "oid": "user-456",
            "name": "Ada Lovelace",
            "preferred_username": "ada@contoso.com"
        }));
        let claims = decode_id_token_claims(&token).expect("should decode");
        assert_eq!(claims.tid.as_deref(), Some("tenant-123"));
        assert_eq!(claims.oid.as_deref(), Some("user-456"));
        assert_eq!(claims.name.as_deref(), Some("Ada Lovelace"));
    }

    #[test]
    fn malformed_id_token_returns_none() {
        assert!(decode_id_token_claims("not-a-jwt").is_none());
        assert!(decode_id_token_claims("only.two").is_none());
    }

    #[test]
    fn token_response_maps_claims_and_expiry() {
        let id_token = make_id_token(&serde_json::json!({"tid": "t1", "oid": "u1", "name": "Org"}));
        let r = MsTokenResponse {
            access_token: "at".into(),
            refresh_token: Some("rt".into()),
            id_token: Some(id_token),
            scope: Some("Mail.Read".into()),
            expires_in: Some(3600),
        };
        let b = token_response_to_bundle(r);
        assert_eq!(b.access_token, "at");
        assert_eq!(b.refresh_token.as_deref(), Some("rt"));
        assert_eq!(b.provider_workspace_id, "t1");
        assert_eq!(b.provider_user_id, "u1");
        assert_eq!(b.provider_workspace_name.as_deref(), Some("Org"));
        assert!(b.expires_at.is_some(), "expires_at derived from expires_in");
    }

    // ── classify_microsoft_token_error (CS-453) ────────────────────────────

    #[test]
    fn each_definite_aadsts_code_classifies_as_revoked() {
        assert_eq!(
            classify_microsoft_token_error(Some("invalid_grant"), &[50173]),
            Some(CredentialDeathReason::MicrosoftPasswordChangedOrAdminReset)
        );
        assert_eq!(
            classify_microsoft_token_error(Some("invalid_grant"), &[50057]),
            Some(CredentialDeathReason::MicrosoftAccountDisabled)
        );
        assert_eq!(
            classify_microsoft_token_error(Some("invalid_grant"), &[50034]),
            Some(CredentialDeathReason::MicrosoftAccountDeleted)
        );
        assert_eq!(
            classify_microsoft_token_error(Some("invalid_grant"), &[65001]),
            Some(CredentialDeathReason::MicrosoftConsentRevoked)
        );
    }

    /// A refresh token that expired from inactivity is dead until the user
    /// signs in again — the two AADSTS codes Microsoft uses for it classify
    /// as revoked, each under its own reason code, so the sweeps stop
    /// retrying the connection every tick and the connections list can say
    /// why it needs a reconnect.
    #[test]
    fn inactivity_expired_grant_codes_classify_as_revoked() {
        assert_eq!(
            classify_microsoft_token_error(Some("invalid_grant"), &[70008]),
            Some(CredentialDeathReason::MicrosoftGrantExpired)
        );
        assert_eq!(
            classify_microsoft_token_error(Some("invalid_grant"), &[700082]),
            Some(CredentialDeathReason::MicrosoftRefreshTokenInactive)
        );
        assert_ne!(
            CredentialDeathReason::MicrosoftGrantExpired.code(),
            CredentialDeathReason::MicrosoftRefreshTokenInactive.code()
        );
        // Still gated on the `invalid_grant` class like every other code.
        assert_eq!(classify_microsoft_token_error(Some("invalid_request"), &[70008]), None);
    }

    #[test]
    fn a_definite_code_among_several_error_codes_still_classifies() {
        // Microsoft's `error_codes` array can carry more than one entry —
        // the first DEFINITE one found still wins, regardless of position.
        assert_eq!(
            classify_microsoft_token_error(Some("invalid_grant"), &[9999999, 50057, 12345]),
            Some(CredentialDeathReason::MicrosoftAccountDisabled)
        );
    }

    #[test]
    fn wrong_error_class_never_classifies_even_with_a_matching_code() {
        // The numeric code alone is not enough — `error` must be exactly
        // `invalid_grant` (Microsoft's OAuth2 error CLASS for "this grant is
        // dead"). A different class carrying the same digits by coincidence
        // must not classify.
        assert_eq!(
            classify_microsoft_token_error(Some("invalid_client"), &[50173]),
            None
        );
        assert_eq!(classify_microsoft_token_error(None, &[50173]), None);
    }

    #[test]
    fn undocumented_or_deliberately_excluded_codes_never_classify() {
        // 50055 (password expired) is an interrupt the user's next
        // interactive sign-in resolves — not a dead credential. See
        // `classify_microsoft_token_error`'s own doc comment for the full
        // list of codes considered and left out.
        assert_eq!(
            classify_microsoft_token_error(Some("invalid_grant"), &[50055]),
            None
        );
        // MFA/conditional-access interrupts: real, but not "dead".
        assert_eq!(
            classify_microsoft_token_error(Some("invalid_grant"), &[50076]),
            None
        );
        assert_eq!(
            classify_microsoft_token_error(Some("invalid_grant"), &[50131]),
            None
        );
        // An empty/unparsed error_codes array (a malformed or unrecognized
        // body) never classifies either.
        assert_eq!(
            classify_microsoft_token_error(Some("invalid_grant"), &[]),
            None
        );
    }

    #[test]
    fn transient_failures_are_never_reachable_through_this_classifier() {
        // 5xx/429/timeout/network errors never produce an `error`/`error_codes`
        // body in the first place — `refresh()` only calls this classifier
        // inside the non-2xx branch, and a transport failure short-circuits
        // before that branch via `map_err` into `Exchange` directly (see
        // `refresh`'s own `.send().await.map_err(...)` line). This test pins
        // the classifier's OWN half of that contract: even a body that
        // merely LOOKS like a throttling/server-error response (no `error`
        // field Microsoft's OAuth error shape defines) never classifies.
        assert_eq!(classify_microsoft_token_error(None, &[]), None);
    }

    #[test]
    fn authorize_url_requires_client_id() {
        // Ensure absent for this assertion.
        unsafe {
            std::env::remove_var("MICROSOFT_CLIENT_ID");
        }
        let err = MicrosoftConnector
            .authorize_url("http://localhost:8080", "state")
            .unwrap_err();
        assert!(matches!(err, ConnectorError::NotConfigured(_)));
    }

    #[test]
    fn admin_consent_url_uses_the_dedicated_adminconsent_endpoint() {
        // CodeRabbit round 1 on PR 3.2: `/authorize?prompt=admin_consent` never
        // produces the code-less `admin_consent=True&tenant=<id>` redirect the
        // callback branch handles — only `/v2.0/adminconsent` does. Pin the
        // endpoint, the common→organizations mapping, and the absence of the
        // authorize-flow params.
        let url = build_admin_consent_url(
            "common",
            "cid",
            "http://localhost:8080/v1/oauth/microsoft/callback",
            "st",
        );
        assert!(
            url.starts_with("https://login.microsoftonline.com/organizations/v2.0/adminconsent?")
        );
        assert!(url.contains("client_id=cid"));
        assert!(url.contains("state=st"));
        assert!(!url.contains("response_type"), "not the authorize flow");
        assert!(
            !url.contains("prompt="),
            "prompt=admin_consent was the wrong mechanism"
        );

        let pinned = build_admin_consent_url("contoso.example", "cid", "http://x/cb", "st");
        assert!(
            pinned.starts_with(
                "https://login.microsoftonline.com/contoso.example/v2.0/adminconsent?"
            ),
            "a configured real tenant passes through unmapped"
        );
    }

    #[test]
    fn parse_graph_dt_interprets_naive_as_utc() {
        let dt = GraphDateTime {
            date_time: Some("2026-06-20T15:30:00.0000000".into()),
        };
        let parsed = parse_graph_dt(Some(&dt));
        assert_eq!(parsed.to_rfc3339(), "2026-06-20T15:30:00+00:00");
        assert_eq!(parse_graph_dt_opt(Some(&dt)), Some(parsed));
        assert_eq!(parse_graph_dt_opt(None), None);
        let junk = GraphDateTime { date_time: Some("not a time".into()) };
        assert_eq!(parse_graph_dt_opt(Some(&junk)), None);
    }

    fn graph_event(json: serde_json::Value) -> GraphEvent {
        serde_json::from_value(json).unwrap()
    }

    #[test]
    fn shape_graph_event_composes_subject_preview_end_location() {
        let item = shape_graph_event(graph_event(serde_json::json!({
            "id": "AAMkAG-1",
            "subject": "  Sprint review ",
            "bodyPreview": "Agenda: demo, retro",
            "start": { "dateTime": "2026-10-07T09:00:00.0000000" },
            "end": { "dateTime": "2026-10-07T09:30:00.0000000" },
            "location": { "displayName": "Room 4" },
            "organizer": { "emailAddress": { "name": "Alice Ng", "address": "alice@ex.com" } },
            "attendees": [
                { "emailAddress": { "name": "Bob", "address": "bob@ex.com" } },
                { "emailAddress": { "address": "carol@ex.com" } }
            ]
        })));
        assert_eq!(item.source_type, "microsoft_calendar");
        assert_eq!(item.source_id, "AAMkAG-1");
        assert_eq!(
            item.content,
            "Sprint review\n\nAgenda: demo, retro\nEnds: 2026-10-07T09:30:00Z\nLocation: Room 4"
        );
        assert_eq!(item.timestamp.to_rfc3339(), "2026-10-07T09:00:00+00:00");
        assert_eq!(item.participants, vec!["Alice Ng".to_string(), "Bob".to_string(), "carol@ex.com".to_string()]);
        assert!(item.thread_id.is_none());
    }

    #[test]
    fn shape_graph_event_with_nothing_but_a_subject_is_just_the_subject() {
        // No preview/end/location: no dangling "\n\n" after the title.
        let item = shape_graph_event(graph_event(serde_json::json!({
            "id": "AAMkAG-2",
            "subject": "Focus time",
            "start": { "dateTime": "2026-10-07T09:00:00.0000000" }
        })));
        assert_eq!(item.content, "Focus time");
        // Blank location/preview are dropped, not rendered as empty lines.
        let item = shape_graph_event(graph_event(serde_json::json!({
            "id": "AAMkAG-3",
            "bodyPreview": "   ",
            "location": { "displayName": "" },
            "start": { "dateTime": "2026-10-07T09:00:00.0000000" }
        })));
        assert_eq!(item.content, "(no subject)");
    }

    /// The reason `end` and `location` are selected at all: an edit that
    /// moves ONLY the end time, or ONLY the room, must reach
    /// `services::source_reingest::decide` as `Changed`. The stored chunk
    /// is what the previous fetch produced; the incoming item is the
    /// re-fetch after the edit.
    #[test]
    fn end_time_only_and_location_only_edits_are_detected_as_changed() {
        use crate::services::source_reingest::{content_hash_hex, decide, Incoming, ReingestDecision, StoredChunk};
        let base = serde_json::json!({
            "id": "AAMkAG-1",
            "subject": "Sprint review",
            "bodyPreview": "Agenda: demo, retro",
            "start": { "dateTime": "2026-10-07T09:00:00.0000000" },
            "end": { "dateTime": "2026-10-07T09:30:00.0000000" },
            "location": { "displayName": "Room 4" },
            "attendees": [ { "emailAddress": { "name": "Bob" } } ]
        });
        let before = shape_graph_event(graph_event(base.clone()));
        let stored = [StoredChunk {
            atom_id: uuid::Uuid::new_v4(),
            content_hash: Some(content_hash_hex(&before.content)),
            timestamp: before.timestamp,
            participants: before.participants.clone(),
        }];
        let decide_for = |after: &NormalizedItem| {
            decide(&stored, &Incoming { content: &after.content, timestamp: after.timestamp, participants: &after.participants })
        };

        // Control: the unchanged re-fetch every tick performs is a no-op.
        assert_eq!(decide_for(&shape_graph_event(graph_event(base.clone()))), ReingestDecision::Unchanged);

        // End time only (start, subject, body, room, attendees identical).
        let mut longer = base.clone();
        longer["end"] = serde_json::json!({ "dateTime": "2026-10-07T10:00:00.0000000" });
        assert!(matches!(decide_for(&shape_graph_event(graph_event(longer))), ReingestDecision::Changed { .. }));

        // Location only.
        let mut moved = base.clone();
        moved["location"] = serde_json::json!({ "displayName": "Room 7" });
        assert!(matches!(decide_for(&shape_graph_event(graph_event(moved))), ReingestDecision::Changed { .. }));

        // Location removed entirely.
        let mut nowhere = base.clone();
        nowhere.as_object_mut().unwrap().remove("location");
        assert!(matches!(decide_for(&shape_graph_event(graph_event(nowhere))), ReingestDecision::Changed { .. }));
    }

    /// A three-paragraph agenda whose plain text runs well past Graph's
    /// ~255-character `bodyPreview`: `(html, preview, tail)` where `tail`
    /// is a sentence that exists only beyond the preview.
    fn long_agenda(tail: &str) -> (String, String, String) {
        let p1 = "Agenda for the quarterly review: revenue, pipeline, hiring plan and the platform roadmap through Q2.";
        let p2 = "Please read the pre-read deck before the call; we will not walk through it slide by slide, only the open questions.";
        let p3 = format!("Decision needed: {tail}");
        let html = format!("<html><body><p>{p1}</p><p>{p2}</p><p>{p3}</p></body></html>");
        let plain = format!("{p1} {p2} {p3}");
        let preview: String = plain.chars().take(255).collect();
        assert!(!preview.contains(tail), "fixture: the tail must lie beyond the preview");
        (html, preview, p3)
    }

    /// The whole `body` is what the coral carries — not the ~255-character
    /// `bodyPreview` — converted with paragraph boundaries kept.
    #[test]
    fn shape_graph_event_ingests_the_full_body_over_the_preview() {
        let (html, preview, p3) = long_agenda("freeze the API surface on March 1st.");
        let item = shape_graph_event(graph_event(serde_json::json!({
            "id": "AAMkAG-body",
            "subject": "Quarterly review",
            "bodyPreview": preview,
            "body": { "contentType": "html", "content": html },
            "start": { "dateTime": "2026-10-07T09:00:00.0000000" },
            "end": { "dateTime": "2026-10-07T10:00:00.0000000" }
        })));
        assert!(item.content.contains(&p3), "the text beyond the preview is ingested: {}", item.content);
        assert!(item.content.contains("roadmap through Q2.\nPlease read"), "paragraphs stay on their own lines: {}", item.content);
        assert!(!item.content.contains('<'), "html stripped");
        assert!(item.content.ends_with("March 1st.\nEnds: 2026-10-07T10:00:00Z"), "{}", item.content);

        // A plain-text body passes through untouched.
        let item = shape_graph_event(graph_event(serde_json::json!({
            "id": "AAMkAG-text",
            "subject": "Standup",
            "bodyPreview": "Blockers only",
            "body": { "contentType": "text", "content": "Blockers only\nThen parking lot" },
            "start": { "dateTime": "2026-10-07T09:00:00.0000000" }
        })));
        assert_eq!(item.content, "Standup\n\nBlockers only\nThen parking lot");

        // No body at all (a legacy `$select` without it, or Graph omitting
        // it) → the preview, exactly as before.
        let item = shape_graph_event(graph_event(serde_json::json!({
            "id": "AAMkAG-prev",
            "subject": "Standup",
            "bodyPreview": "Blockers only",
            "start": { "dateTime": "2026-10-07T09:00:00.0000000" }
        })));
        assert_eq!(item.content, "Standup\n\nBlockers only");
        // An empty HTML body (what Graph returns for a description-less
        // event: `"body": {"contentType": "html", "content": ""}`) is
        // absent, not an empty description line.
        let item = shape_graph_event(graph_event(serde_json::json!({
            "id": "AAMkAG-empty",
            "subject": "Focus time",
            "bodyPreview": "",
            "body": { "contentType": "html", "content": "" },
            "start": { "dateTime": "2026-10-07T09:00:00.0000000" }
        })));
        assert_eq!(item.content, "Focus time");
    }

    /// The gap: an edit to the description beyond the first ~255
    /// characters leaves `bodyPreview` byte-identical, so a coral shaped
    /// from the preview alone hashed the same before and after and
    /// `services::source_reingest::decide` judged it `Unchanged`.
    #[test]
    fn body_edit_beyond_the_preview_is_detected_as_changed() {
        use crate::services::source_reingest::{content_hash_hex, decide, Incoming, ReingestDecision, StoredChunk};
        let (before_html, preview, _) = long_agenda("freeze the API surface on March 1st.");
        let (after_html, preview_after, _) = long_agenda("freeze the API surface on April 15th.");
        assert_eq!(preview, preview_after, "fixture: the edit is invisible in the preview");
        let shape = |html: &str| {
            shape_graph_event(graph_event(serde_json::json!({
                "id": "AAMkAG-body",
                "subject": "Quarterly review",
                "bodyPreview": preview,
                "body": { "contentType": "html", "content": html },
                "start": { "dateTime": "2026-10-07T09:00:00.0000000" },
                "end": { "dateTime": "2026-10-07T10:00:00.0000000" }
            })))
        };
        let before = shape(&before_html);
        let stored = [StoredChunk {
            atom_id: uuid::Uuid::new_v4(),
            content_hash: Some(content_hash_hex(&before.content)),
            timestamp: before.timestamp,
            participants: before.participants.clone(),
        }];
        let decide_for = |after: &NormalizedItem| {
            decide(&stored, &Incoming { content: &after.content, timestamp: after.timestamp, participants: &after.participants })
        };
        assert_eq!(decide_for(&shape(&before_html)), ReingestDecision::Unchanged, "same body → no churn");
        assert!(
            matches!(decide_for(&shape(&after_html)), ReingestDecision::Changed { .. }),
            "an edit past the preview must replace the coral"
        );
    }

    const CALENDAR_EVENT_TEAMS_INVITE: &str =
        include_str!("../../tests/fixtures/graph/calendar_event_teams_invite.json");
    const TEAMS_INVITE_AGENDA: &str =
        "Agenda for the pipeline review:\nQ3 pipeline & forecast\nHiring plan through Q2\nPre-read: the Q3 deck (link in the channel).";

    /// The full body of a Teams meeting is the organiser's agenda PLUS the
    /// join block Outlook injects (`backend/tests/fixtures/graph/
    /// calendar_event_teams_invite.json`, the current invite shape). The
    /// coral carries the agenda — bullets on their own lines — and none of
    /// the block, from the body and from the truncated preview alike.
    #[test]
    fn fixture_teams_invite_event_keeps_the_agenda_and_drops_the_join_block() {
        let item = shape_graph_event(serde_json::from_str(CALENDAR_EVENT_TEAMS_INVITE).unwrap());
        assert_eq!(
            item.content,
            format!("Pipeline review\n\n{TEAMS_INVITE_AGENDA}\nEnds: 2026-10-08T15:30:00Z\nLocation: Microsoft Teams Meeting")
        );
        assert_eq!(item.participants, vec!["Priya Nair", "Sam Rivera", "Eng Team"]);
        assert_eq!(item.timestamp.to_rfc3339(), "2026-10-08T15:00:00+00:00");
        for leaked in [
            "Microsoft Teams Need help", "Join the meeting now", "Meeting ID", "Passcode", "Dial in by phone",
            "323-555", "Find a local number", "conference ID", "Meeting options", "Reset dial-in PIN", "____",
        ] {
            assert!(!item.content.contains(leaked), "{leaked:?} leaked into {:?}", item.content);
        }

        // The preview-only shape (a `$select` without `body`): the 255-char
        // preview cuts the block mid-passcode, it is stripped to the end,
        // and the description is the same agenda.
        let mut v: serde_json::Value = serde_json::from_str(CALENDAR_EVENT_TEAMS_INVITE).unwrap();
        v.as_object_mut().unwrap().remove("body");
        assert!(v["bodyPreview"].as_str().unwrap().contains("Passcode: Hq7"), "fixture: the preview is cut inside the block");
        assert_eq!(shape_graph_event(graph_event(v.clone())).content, item.content);

        // Nothing but the block (no agenda typed): no description line, and
        // no fall-through to the preview, which is the same block.
        let html = v["bodyPreview"].as_str().unwrap().to_string();
        let full: serde_json::Value = serde_json::from_str(CALENDAR_EVENT_TEAMS_INVITE).unwrap();
        let body = full["body"]["content"].as_str().unwrap();
        let block_only = format!("<html><body>{}", &body[body.find("<div style=\"width:100%\">").unwrap()..]);
        v["body"] = serde_json::json!({ "contentType": "html", "content": block_only });
        v["bodyPreview"] = serde_json::json!(html[html.find("____").unwrap()..]);
        assert_eq!(
            shape_graph_event(graph_event(v)).content,
            "Pipeline review\n\nEnds: 2026-10-08T15:30:00Z\nLocation: Microsoft Teams Meeting"
        );
    }

    #[test]
    fn microsoft_supports_sync_and_fetch_items() {
        assert!(MicrosoftConnector.supports_sync());
        assert!(MicrosoftConnector.supports_fetch_items());
        assert_eq!(MicrosoftConnector.id(), "microsoft");
    }

    #[test]
    fn message_maps_to_outlook_item_with_participants() {
        let json = serde_json::json!({
            "id": "AAMkAD123",
            "subject": "Q3 planning",
            "from": { "emailAddress": { "name": "Ada", "address": "ada@contoso.com" } },
            "toRecipients": [
                { "emailAddress": { "name": "Bob", "address": "bob@contoso.com" } }
            ],
            "receivedDateTime": "2026-06-18T10:30:00Z",
            "bodyPreview": "Let's align on scope."
        });
        let msg: GraphMessage = serde_json::from_value(json).unwrap();
        let item = message_to_item(msg);
        assert_eq!(item.source_type, "outlook");
        assert_eq!(item.source_id, "AAMkAD123");
        assert!(item.content.contains("Q3 planning"));
        assert!(item.content.contains("align on scope"));
        assert_eq!(item.participants, vec!["Ada", "Bob"]);
    }

    #[test]
    fn message_ingests_full_body_over_preview() {
        let json = serde_json::json!({
            "id": "M1",
            "subject": "Contract",
            "bodyPreview": "Short preview…",
            "body": { "contentType": "html", "content": "<p>The <b>full</b> terms &amp; conditions follow.</p>" }
        });
        let m: GraphMessage = serde_json::from_value(json).unwrap();
        let item = message_to_item(m);
        assert!(
            item.content.contains("full terms & conditions"),
            "uses full body"
        );
        assert!(!item.content.contains("Short preview"), "not the preview");
        assert!(!item.content.contains('<'), "html stripped");
    }

    #[test]
    fn message_falls_back_to_preview_without_body() {
        let json =
            serde_json::json!({ "id": "M2", "subject": "Hi", "bodyPreview": "preview text" });
        let m: GraphMessage = serde_json::from_value(json).unwrap();
        assert!(message_to_item(m).content.contains("preview text"));
    }

    #[test]
    fn message_carries_conversation_id_as_thread_id() {
        let json = serde_json::json!({
            "id": "AAMkAD999",
            "subject": "Re: Q3 planning",
            "conversationId": "CONV-abc",
            "receivedDateTime": "2026-06-19T09:00:00Z",
            "bodyPreview": "Sounds good."
        });
        let msg: GraphMessage = serde_json::from_value(json).unwrap();
        let item = message_to_item(msg);
        assert_eq!(item.thread_id.as_deref(), Some("CONV-abc"));
        assert_eq!(item.source_type, "outlook");
    }

    #[test]
    fn message_without_conversation_id_has_no_thread() {
        // The CS-116 shape (no conversationId selected) still deserializes.
        let json = serde_json::json!({ "id": "X", "subject": "hi" });
        let msg: GraphMessage = serde_json::from_value(json).unwrap();
        assert!(message_to_item(msg).thread_id.is_none());
    }

    // m365 PR 2.0: message_to_item now wires
    // email_body::trim_outlook_reply_chain into the Outlook mail path.
    #[test]
    fn message_to_item_trims_original_message_delimiter_from_body() {
        let json = serde_json::json!({
            "id": "M-trim",
            "subject": "Deploy window",
            "body": {
                "contentType": "text",
                "content": "Approved, go ahead.\n\n-----Original Message-----\nFrom: John Smith <john@example.com>\nSent: Monday, January 1, 2024 9:00 AM\nSubject: Deploy window\n\nCan we deploy at 5pm?"
            }
        });
        let m: GraphMessage = serde_json::from_value(json).unwrap();
        let item = message_to_item(m);
        assert_eq!(item.content, "Deploy window\n\nApproved, go ahead.");
    }

    #[test]
    fn message_to_item_trims_mobile_signature_from_body() {
        let json = serde_json::json!({
            "id": "M-mobile",
            "subject": "Quick reply",
            "body": { "contentType": "text", "content": "Sounds good to me.\n\nSent from my iPhone" }
        });
        let m: GraphMessage = serde_json::from_value(json).unwrap();
        let item = message_to_item(m);
        assert_eq!(item.content, "Quick reply\n\nSounds good to me.");
    }

    // CodeRabbit finding F2 on #691: the two HTML-body regressions below are
    // the actual bug this PR fixes — a real Outlook HTML mail body puts each
    // paragraph/marker in its own `<div>`, and running `trim_outlook_reply_
    // chain`'s line-scan on a body converted WITHOUT block-boundary
    // preservation flattened `<div>Approved</div><div>-----Original
    // Message-----</div>` into one line
    // (`"Approved-----Original Message-----"`), so the delimiter could never
    // match and the whole quoted tail leaked into the atom. These prove it
    // now trims correctly through the HTML path, not just the already-
    // working plain-text path pinned above
    // (`message_to_item_trims_original_message_delimiter_from_body`/
    // `message_to_item_trims_mobile_signature_from_body`, `contentType:
    // "text"`, unaffected by this fix since there's no HTML to flatten).
    #[test]
    fn message_to_item_trims_original_message_delimiter_from_html_body() {
        let json = serde_json::json!({
            "id": "M-html-trim",
            "subject": "Deploy window",
            "body": {
                "contentType": "html",
                "content": "<div>Approved, go ahead.</div><div>-----Original Message-----</div><div>Can we deploy at 5pm?</div>"
            }
        });
        let m: GraphMessage = serde_json::from_value(json).unwrap();
        let item = message_to_item(m);
        assert_eq!(item.content, "Deploy window\n\nApproved, go ahead.");
    }

    #[test]
    fn message_to_item_trims_mobile_signature_from_html_body() {
        let json = serde_json::json!({
            "id": "M-html-mobile",
            "subject": "Quick reply",
            "body": {
                "contentType": "html",
                "content": "<div>Sounds good to me.</div><div>Sent from my iPhone</div>"
            }
        });
        let m: GraphMessage = serde_json::from_value(json).unwrap();
        let item = message_to_item(m);
        assert_eq!(item.content, "Quick reply\n\nSounds good to me.");
    }

    /// Negative case: marker text appearing mid-sentence, inside a SINGLE
    /// `<div>` (not its own line/div), must not trigger a trim — same
    /// false-positive discipline as `trim_outlook_reply_chain`'s own
    /// `outlook_does_not_trim_a_sentence_merely_mentioning_original_message`
    /// / `outlook_does_not_trim_sentence_merely_mentioning_sent_from_my_
    /// iphone`, now proven through the actual HTML→boundaried-text→trim
    /// pipeline `message_to_item` runs, not just the trim function in
    /// isolation.
    #[test]
    fn message_to_item_does_not_trim_marker_text_mid_sentence_in_a_single_div() {
        let json = serde_json::json!({
            "id": "M-html-negative",
            "subject": "FYI",
            "body": {
                "contentType": "html",
                "content": "<div>Please refer to the original message for context, and note I sent from my iPhone last time — both fine.</div>"
            }
        });
        let m: GraphMessage = serde_json::from_value(json).unwrap();
        let item = message_to_item(m);
        assert_eq!(
            item.content,
            "FYI\n\nPlease refer to the original message for context, and note I sent from my iPhone last time — both fine."
        );
    }

    // -- Fixture-driven (backend/tests/fixtures/graph/ — see its README for
    // provenance: hand-authored against Microsoft's documented Graph API
    // schemas, not a live capture) — Outlook -------------------------------

    const OUTLOOK_MESSAGE_PAYLOAD: &str =
        include_str!("../../tests/fixtures/graph/outlook_message_payload.json");
    const OUTLOOK_MESSAGE_HTML_EDGE: &str =
        include_str!("../../tests/fixtures/graph/outlook_message_html_script_style_entities.json");

    #[test]
    fn fixture_outlook_message_shapes_subject_body_participants_and_thread() {
        let m: GraphMessage = serde_json::from_str(OUTLOOK_MESSAGE_PAYLOAD).unwrap();
        let item = message_to_item(m);
        assert_eq!(item.source_type, "outlook");
        assert_eq!(item.source_id, "AAMkAGI2TG93AAA=");
        assert_eq!(item.thread_id.as_deref(), Some("AAQkAGI2_conv_001"));
        assert_eq!(item.timestamp.timestamp_millis(), 1770042600000);
        // Participants: from + toRecipients only — ccRecipients ("Eng Team")
        // must NOT appear (message_to_item never reads ccRecipients).
        assert_eq!(item.participants, vec!["Priya Nair", "Sam Rivera"]);
        assert!(!item.participants.iter().any(|p| p == "Eng Team"));
        // HTML stripped, entity decoded, and — as of m365 PR 2.0 (CodeRabbit
        // finding F2 on #691) — a newline IS now inserted at each </div>
        // and <br> boundary: `message_to_item` converts the Outlook mail
        // body via `body_to_text_boundaried` (block-boundary-preserving),
        // not the boundary-less `body_to_text`, so
        // `trim_outlook_reply_chain`'s line-scan can actually see Outlook's
        // own reply/signature markers on their own line. This is a
        // deliberate, accepted shape change for this PR — see
        // `message_to_item`'s and `email_body::from_graph_body_boundaried`'s
        // docs. (The "no separators" contract still holds for Teams — see
        // `html_to_text_still_has_no_block_boundary_separators` in
        // email_body.rs, and this file's own
        // `teams_message_html_still_has_no_block_boundary_separators` below.)
        assert_eq!(
            item.content,
            "Renewal terms for Q1\n\nHi Sam,\nAttaching the renewal terms for review.\nThanks,\nPriya"
        );
    }

    /// Outlook and Teams both funnel HTML bodies through the same
    /// script/style/comment-skipping + entity-decoding core (shared by
    /// `html_to_text` and `gmail_html_to_text` — see `email_body.rs`'s
    /// module doc); Outlook mail specifically goes through
    /// `body_to_text_boundaried` → `email_body::from_graph_body_boundaried`
    /// → `gmail_html_to_text` as of this PR (F2 on #691), Teams still
    /// through `body_to_text` → `from_graph_body` → `html_to_text` — only
    /// the boundary-insertion behavior differs between them, not the
    /// script/style/entity fix this test asserts. This fixture (the Outlook
    /// twin of `teams_message_html_script_style_entities.json`, same
    /// `body.content`) used to pin the OLD, broken behavior
    /// (`<style>`/`<script>` element text leaking through, `&mdash;` left
    /// un-decoded) — flagged in `docs/connectors/teams.md`'s known-gaps
    /// section as a CodeRabbit finding F1 deferral on #687. **m365 PR 2.0
    /// fixed the shared core** (script/style/head/title/comment skipping,
    /// full named+numeric entity decoding — see `email_body.rs`'s own
    /// tests for the core coverage); this test now asserts the FIXED
    /// behavior instead. (The single `<p>` in this fixture means the
    /// boundary-vs-not distinction doesn't show up in these particular
    /// `contains` assertions either way.)
    #[test]
    fn fixture_outlook_message_html_script_and_style_content_is_excluded_and_entities_decoded() {
        let m: GraphMessage = serde_json::from_str(OUTLOOK_MESSAGE_HTML_EDGE).unwrap();
        let item = message_to_item(m);
        assert!(
            !item.content.contains("color:red"),
            "style element text must now be excluded entirely, not just its tags"
        );
        assert!(
            !item.content.contains("alert('x')"),
            "script element text must now be excluded entirely, not just its tags"
        );
        assert!(
            item.content.contains("Deploy at 5 pm & watch dashboards"),
            "&nbsp;/&amp; decoded"
        );
        assert!(
            item.content.contains("dashboards \u{2014} don't"),
            "&mdash; must now decode to an em dash"
        );
        assert!(
            !item.content.contains("&mdash;"),
            "&mdash; must no longer survive undecoded"
        );
        assert!(
            item.content.contains("don't forget the <prod> flag"),
            "&#39;/&lt;/&gt; decoded"
        );
    }

    #[test]
    fn graph_list_parses_next_link_for_pagination() {
        // A page with more results carries @odata.nextLink…
        let page: GraphList<GraphMessage> = serde_json::from_value(serde_json::json!({
            "value": [{ "id": "m1", "conversationId": "c" }],
            "@odata.nextLink": "https://graph.microsoft.com/v1.0/me/messages?$skiptoken=ABC"
        }))
        .unwrap();
        assert_eq!(page.value.len(), 1);
        assert!(
            page.next_link.is_some(),
            "nextLink drives full-thread pagination"
        );

        // …the last page omits it, ending the loop.
        let last: GraphList<GraphMessage> =
            serde_json::from_value(serde_json::json!({ "value": [] })).unwrap();
        assert!(last.next_link.is_none());
    }

    #[test]
    fn conversation_filter_escapes_single_quotes() {
        assert_eq!(conversation_filter("ab'cd"), "conversationId eq 'ab''cd'");
        assert_eq!(conversation_filter("plain"), "conversationId eq 'plain'");
    }

    #[test]
    fn client_state_is_long_random_hex() {
        let a = generate_client_state();
        let b = generate_client_state();
        assert_eq!(a.len(), 64, "two simple uuids = 64 hex chars");
        assert!(a.chars().all(|c| c.is_ascii_hexdigit()));
        assert_ne!(a, b, "client states must differ");
    }

    #[test]
    fn notification_url_targets_graph_webhook() {
        assert!(notification_url().ends_with("/v1/webhooks/graph"));
    }

    #[test]
    fn teams_message_maps_html_body_and_namespaces_id() {
        let json = serde_json::json!({
            "id": "1700000000000",
            "messageType": "message",
            "createdDateTime": "2026-06-18T10:30:00Z",
            "from": { "user": { "id": "aad-guid-1", "displayName": "Ada" } },
            "body": { "contentType": "html", "content": "<p>Ship it &amp; <b>celebrate</b></p>" }
        });
        let m: TeamsMessage = serde_json::from_value(json).unwrap();
        let item = teams_message_to_item(&m, "teams_channel", "CHAN1").expect("user message");
        assert_eq!(item.source_type, "teams_channel");
        assert_eq!(item.source_id, "CHAN1:1700000000000");
        assert_eq!(item.thread_id.as_deref(), Some("CHAN1"));
        assert!(item.content.contains("Ship it & celebrate"));
        assert!(!item.content.contains('<'), "tags stripped");
        assert_eq!(item.participants, vec!["Ada"]);
        // Structured provenance: container id + author captured so the coral is
        // labelled with a human (not a GUID) downstream. channel_name is resolved
        // later from the watched resource, so it's left unset here.
        assert_eq!(item.channel_id.as_deref(), Some("CHAN1"));
        assert_eq!(item.author_id.as_deref(), Some("aad-guid-1"));
        assert_eq!(item.author_name.as_deref(), Some("Ada"));
    }

    // m365 PR 2.0: quoted-reply-chain trimming is Outlook-mail-only —
    // Teams messages must never be trimmed, even when the body happens to
    // contain a marker trim_outlook_reply_chain/trim_quoted_and_signature
    // would otherwise cut on (Teams has no reply-chain concept; the whole
    // message body is the message).
    #[test]
    fn teams_message_body_is_never_quoted_reply_trimmed() {
        let json = serde_json::json!({
            "id": "1700000000002",
            "messageType": "message",
            "createdDateTime": "2026-06-18T10:32:00Z",
            "from": { "user": { "id": "aad-guid-3", "displayName": "Sam" } },
            "body": {
                "contentType": "text",
                "content": "Approved.\n\n-----Original Message-----\nFrom: a@example.com\nSent: today"
            }
        });
        let m: TeamsMessage = serde_json::from_value(json).unwrap();
        let item = teams_message_to_item(&m, "teams_channel", "CHAN1").expect("user message");
        assert!(
            item.content.contains("-----Original Message-----"),
            "Teams bodies are never quoted-reply trimmed"
        );
    }

    #[test]
    fn teams_message_captures_author_id_without_display_name() {
        // Some Graph payloads omit displayName (guests, incomplete profiles).
        // The AAD id must still be captured so the author stays anchored.
        let json = serde_json::json!({
            "id": "1700000000001",
            "messageType": "message",
            "createdDateTime": "2026-06-18T10:31:00Z",
            "from": { "user": { "id": "aad-guid-2" } },
            "body": { "contentType": "text", "content": "hi" }
        });
        let m: TeamsMessage = serde_json::from_value(json).unwrap();
        let item = teams_message_to_item(&m, "teams_chat", "CHAT9").expect("user message");
        assert_eq!(item.channel_id.as_deref(), Some("CHAT9"));
        assert_eq!(item.author_id.as_deref(), Some("aad-guid-2"));
        assert!(item.author_name.is_none());
        assert!(
            item.participants.is_empty(),
            "no display name → no participant"
        );
    }

    #[test]
    fn vtt_to_text_strips_cues_and_dedups() {
        let vtt = "WEBVTT\n\n1\n00:00:01.000 --> 00:00:03.000\nHello team\n\n2\n00:00:03.000 --> 00:00:05.000\nHello team\n\n3\n00:00:05.000 --> 00:00:07.000\nLet's begin\n";
        let text = vtt_to_text(vtt);
        assert_eq!(text, "Hello team\nLet's begin");
        assert!(!text.contains("-->"));
        assert!(!text.contains("WEBVTT"));
    }

    #[test]
    fn vtt_to_text_keeps_unattributed_utterances() {
        // Graph's unattributed body is still timestamped, with a blank voice
        // line and no `<v>` speaker tag.
        let body = "\n00:00:01.500 --> 00:00:04.000 \n\nHello, thanks for joining. \n\n00:00:04.000 --> 00:00:07.200 \n\nGlad to be here. \n";
        assert_eq!(
            vtt_to_text(body),
            "Hello, thanks for joining.\nGlad to be here."
        );
    }

    #[test]
    fn speaker_attribution_refusal_is_distinct_from_the_kill_switch() {
        let refused = ConnectorError::Exchange(
            "graph GET (text) https://graph.example/content?$format=text/vtt failed: 403 Forbidden: {\"error\":{\"code\":\"Forbidden\",\"innerError\":{\"code\":\"SpeakerAttributionNotAllowed\"}}}".into(),
        );
        let killed = ConnectorError::Exchange(
            "graph GET (text) https://graph.example/content failed: 403 Forbidden: GraphAccessToTranscriptsDisabled".into(),
        );
        assert!(speaker_attribution_not_allowed(&refused));
        assert!(!graph_access_to_transcripts_disabled(&refused));
        assert!(graph_access_to_transcripts_disabled(&killed));
        assert!(!speaker_attribution_not_allowed(&killed));
    }

    /// The 409 is the error's status field, never read out of the text; a
    /// body naming the duplicate still counts, and a plain 403 does not.
    #[test]
    fn subscription_slot_taken_reads_the_status_field_or_the_duplicate_text() {
        assert!(subscription_slot_taken(&ConnectorError::upstream_code(409, "subscription create failed: 409 Conflict: ")));
        assert!(subscription_slot_taken(&ConnectorError::upstream_code(
            403,
            "subscription create failed: 403 Forbidden: a subscription already exists for this resource"
        )));
        assert!(subscription_slot_taken(&ConnectorError::Exchange("Subscription already exists".into())));
        assert!(!subscription_slot_taken(&ConnectorError::upstream_code(403, "subscription create failed: 403 Forbidden: ")));
        assert!(!subscription_slot_taken(&ConnectorError::Exchange("subscription create failed: 409 Conflict: ".into())), "text is not scraped for a status");
        assert!(!subscription_slot_taken(&ConnectorError::Internal("db".into())));
    }

    #[test]
    fn graph_text_error_keeps_inner_code_past_the_snippet() {
        let message = "x".repeat(400);
        let body = format!(
            "{{\"error\":{{\"code\":\"Forbidden\",\"message\":\"{message}\",\"innerError\":{{\"code\":\"SpeakerAttributionNotAllowed\"}}}}}}"
        );
        let err = ConnectorError::Exchange(graph_text_error_message(
            "https://graph.example/content",
            403,
            &body,
        ));
        assert!(speaker_attribution_not_allowed(&err));
        let ConnectorError::Exchange(msg) = err else {
            unreachable!()
        };
        assert!(msg.contains("SpeakerAttributionNotAllowed"));
        assert!(msg.len() < body.len());
    }

    /// m365 Train 4 PR 4.4: every subscribed resource widens to
    /// `created,updated,deleted` EXCEPT the meeting-transcript stream (and
    /// its `onlineMeetings` root), which stays `created`-only — see
    /// [`subscription_change_types`]'s own doc for why.
    #[test]
    fn subscription_change_types_widens_mail_and_teams_messages_but_not_transcripts() {
        assert_eq!(
            subscription_change_types("me/mailFolders('inbox')/messages"),
            "created,updated,deleted"
        );
        assert_eq!(
            subscription_change_types("me/mailFolders('sentitems')/messages"),
            "created,updated,deleted"
        );
        assert_eq!(
            subscription_change_types("teams/T1/channels/C1/messages"),
            "created,updated,deleted"
        );
        assert_eq!(
            subscription_change_types("chats/CHAT1/messages"),
            "created,updated,deleted"
        );
        assert_eq!(
            subscription_change_types(
                "appCatalogs/teamsApps/APP1/installedToOnlineMeetings/getAllTranscripts"
            ),
            "created",
            "transcripts never get an updated/deleted Graph notification — must stay created-only"
        );
    }

    #[test]
    fn teams_subscription_resource_shapes() {
        assert_eq!(
            teams_subscription_resource("channel", "T1", "C1", "").as_deref(),
            Some("teams/T1/channels/C1/messages")
        );
        assert_eq!(
            teams_subscription_resource("chat", "", "", "CHAT1").as_deref(),
            Some("chats/CHAT1/messages")
        );
        assert!(teams_subscription_resource("channel", "T1", "", "").is_none());
        assert!(teams_subscription_resource("chat", "", "", "").is_none());
    }

    #[test]
    fn meeting_id_parsed_from_transcript_resource() {
        assert_eq!(
            meeting_id_from_resource("users('u1')/onlineMeetings('MID-9')/transcripts('t1')")
                .as_deref(),
            Some("MID-9")
        );
        assert!(meeting_id_from_resource("chats('c')/messages('m')").is_none());
    }

    #[test]
    fn online_meeting_resource_stripped_from_transcript_path() {
        assert_eq!(
            online_meeting_resource_from_transcript(
                "users('u1')/onlineMeetings('MID-9')/transcripts('t1')"
            )
            .as_deref(),
            Some("users('u1')/onlineMeetings('MID-9')")
        );
        assert!(online_meeting_resource_from_transcript("chats('c')/messages('m')").is_none());
    }

    #[test]
    fn transcript_and_meeting_resources_use_teams_lifetime() {
        assert!(is_teams_resource(
            "appCatalogs/teamsApps/APP/installedToOnlineMeetings/getAllTranscripts"
        ));
        assert!(is_teams_resource(
            "users('u')/onlineMeetings('m')/transcripts('t')"
        ));
    }

    /// Item 57: lifetimes branch on resource kind, not a single Teams boolean.
    /// Callers of [`subscription_minutes`] / [`subscription_refresh_window`]
    /// (create + renew) must keep channel/chat long-lived and mail unchanged.
    #[test]
    fn subscription_lifetime_is_per_resource_kind() {
        assert_eq!(
            subscription_minutes("teams/T/channels/C/messages"),
            TEAMS_CHAT_MESSAGE_SUBSCRIPTION_MINUTES
        );
        assert_eq!(
            subscription_minutes("chats/CHAT1/messages"),
            TEAMS_CHAT_MESSAGE_SUBSCRIPTION_MINUTES
        );
        assert_eq!(
            subscription_minutes(
                "appCatalogs/teamsApps/APP/installedToOnlineMeetings/getAllTranscripts"
            ),
            TEAMS_TRANSCRIPT_SUBSCRIPTION_MINUTES
        );
        assert_eq!(
            subscription_minutes("me/mailFolders('inbox')/messages"),
            SUBSCRIPTION_MINUTES
        );
        assert_eq!(
            subscription_refresh_window("teams/T/channels/C/messages"),
            TEAMS_LONG_REFRESH_WINDOW_MINUTES
        );
        assert_eq!(
            subscription_refresh_window("me/mailFolders('inbox')/messages"),
            SUBSCRIPTION_REFRESH_WINDOW_MINUTES
        );
    }

    #[test]
    fn parse_subscribed_teams_resource_roundtrips() {
        assert_eq!(
            parse_subscribed_teams_resource("teams/T1/channels/C1/messages"),
            Some(("teams_channel".into(), "C1".into()))
        );
        assert_eq!(
            parse_subscribed_teams_resource("chats/CHAT1/messages"),
            Some(("teams_chat".into(), "CHAT1".into()))
        );
        assert!(parse_subscribed_teams_resource("me/mailFolders('inbox')/messages").is_none());
        assert!(is_teams_resource("teams/T/channels/C/messages"));
        assert!(is_teams_resource("chats/X/messages"));
        assert!(!is_teams_resource("me/mailFolders('inbox')/messages"));
    }

    #[test]
    fn teams_system_and_empty_messages_skipped() {
        let system = serde_json::json!({
            "id": "x", "messageType": "systemEventMessage",
            "body": { "contentType": "html", "content": "<systemEventMessage/>" }
        });
        let m: TeamsMessage = serde_json::from_value(system).unwrap();
        assert!(teams_message_to_item(&m, "teams_channel", "C").is_none());

        let empty = serde_json::json!({
            "id": "y", "messageType": "message",
            "body": { "contentType": "html", "content": "<div></div>" }
        });
        let m: TeamsMessage = serde_json::from_value(empty).unwrap();
        assert!(teams_message_to_item(&m, "teams_chat", "Z").is_none());
    }

    // -- Fixture-driven (backend/tests/fixtures/graph/ — see its README for
    // provenance) — Teams ---------------------------------------------------

    const TEAMS_CHANNEL_MESSAGE: &str =
        include_str!("../../tests/fixtures/graph/teams_channel_message.json");
    const TEAMS_CHAT_MESSAGE: &str =
        include_str!("../../tests/fixtures/graph/teams_chat_message.json");
    const TEAMS_MESSAGE_BOT_AUTHORED: &str =
        include_str!("../../tests/fixtures/graph/teams_message_bot_authored.json");
    const TEAMS_MESSAGE_SYSTEM_EVENT: &str =
        include_str!("../../tests/fixtures/graph/teams_message_system_event.json");
    const TEAMS_MESSAGE_HTML_EDGE: &str =
        include_str!("../../tests/fixtures/graph/teams_message_html_script_style_entities.json");
    const TEAMS_CHANNEL_NOTIFICATION: &str =
        include_str!("../../tests/fixtures/graph/teams_channel_notification.json");
    const TEAMS_TRANSCRIPT_NOTIFICATION: &str =
        include_str!("../../tests/fixtures/graph/teams_transcript_notification.json");

    #[test]
    fn fixture_teams_channel_message_shapes_subject_body_and_author() {
        let m: TeamsMessage = serde_json::from_str(TEAMS_CHANNEL_MESSAGE).unwrap();
        let item = teams_message_to_item(&m, "teams_channel", "19:channel-sprint@thread.tacv2")
            .expect("a real user channel message");
        assert_eq!(item.source_type, "teams_channel");
        assert_eq!(
            item.source_id,
            "19:channel-sprint@thread.tacv2:1738368000000"
        );
        assert_eq!(item.timestamp.timestamp_millis(), 1769936400000);
        assert_eq!(
            item.content,
            "Sprint planning kickoff\n\nLet's lock the sprint scope by Friday. Ping me with blockers."
        );
        assert_eq!(item.author_id.as_deref(), Some("aad-user-priya-0001"));
        assert_eq!(item.author_name.as_deref(), Some("Priya Nair"));
        assert_eq!(item.participants, vec!["Priya Nair"]);
        assert_eq!(
            item.channel_id.as_deref(),
            Some("19:channel-sprint@thread.tacv2")
        );
        assert_eq!(
            item.thread_id.as_deref(),
            Some("19:channel-sprint@thread.tacv2")
        );
    }

    #[test]
    fn fixture_teams_chat_message_has_no_subject_prefix() {
        let m: TeamsMessage = serde_json::from_str(TEAMS_CHAT_MESSAGE).unwrap();
        let item = teams_message_to_item(&m, "teams_chat", "19:chat-devon-priya@unq.gbl.spaces")
            .expect("a real user chat message");
        assert_eq!(item.source_type, "teams_chat");
        // No `subject` on a 1:1/group chat message — content is the body
        // text alone, no "Subject\n\n" prefix.
        assert_eq!(item.content, "Sounds good, I'll draft the doc today.");
        assert_eq!(item.author_name.as_deref(), Some("Devon Blake"));
    }

    #[test]
    fn fixture_teams_bot_authored_message_is_skipped() {
        let m: TeamsMessage = serde_json::from_str(TEAMS_MESSAGE_BOT_AUTHORED).unwrap();
        assert!(
            teams_message_to_item(&m, "teams_channel", "C").is_none(),
            "from.application present (no from.user) must be filtered out — humans only, CS-122"
        );
    }

    #[test]
    fn fixture_teams_system_event_message_is_skipped() {
        let m: TeamsMessage = serde_json::from_str(TEAMS_MESSAGE_SYSTEM_EVENT).unwrap();
        assert!(
            teams_message_to_item(&m, "teams_channel", "C").is_none(),
            "messageType != \"message\" must be filtered out"
        );
    }

    /// Same fix as `fixture_outlook_message_html_script_and_style_content_
    /// is_excluded_and_entities_decoded` — Teams and Outlook share the exact
    /// same `body_to_text` core, so m365 PR 2.0's `html_to_text` fix applies
    /// here too. See that test's own doc comment and
    /// `docs/connectors/teams.md`'s known-gaps section (now marked fixed).
    /// Teams messages are never quoted-reply-trimmed (no reply-chain
    /// concept) — only the `html_to_text` core changed, not that.
    #[test]
    fn fixture_teams_message_html_script_and_style_content_is_excluded_and_entities_decoded() {
        let m: TeamsMessage = serde_json::from_str(TEAMS_MESSAGE_HTML_EDGE).unwrap();
        let item = teams_message_to_item(&m, "teams_channel", "C").expect("non-empty body");
        assert!(
            !item.content.contains("color:red"),
            "style element text must be excluded entirely"
        );
        assert!(
            !item.content.contains("alert('x')"),
            "script element text must be excluded entirely"
        );
        assert!(item.content.contains("Deploy at 5 pm & watch dashboards"));
        assert!(
            item.content.contains("dashboards \u{2014} don't"),
            "&mdash; must decode to an em dash"
        );
        assert!(
            !item.content.contains("&mdash;"),
            "&mdash; must no longer survive undecoded"
        );
        assert!(item.content.contains("don't forget the <prod> flag"));
    }

    /// m365 PR 2.0 gave Outlook mail block-boundary preservation
    /// (`body_to_text_boundaried`, CodeRabbit finding F2 on #691) but
    /// deliberately left Teams alone — Teams has no reply-chain concept for
    /// boundary preservation to unblock (see `message_to_item`'s and
    /// `email_body::from_graph_body_boundaried`'s docs). Pins that Teams
    /// still collapses adjacent `<div>`s with no separator, the same
    /// contract `email_body.rs`'s own
    /// `html_to_text_still_has_no_block_boundary_separators` pins at the
    /// core-function level.
    #[test]
    fn teams_message_html_still_has_no_block_boundary_separators() {
        let json = serde_json::json!({
            "id": "T-nobound",
            "createdDateTime": "2026-06-18T10:30:00Z",
            "messageType": "message",
            "from": { "user": { "id": "u1", "displayName": "Ada" } },
            "body": { "contentType": "html", "content": "<div>Approve</div><div>today</div>" }
        });
        let m: TeamsMessage = serde_json::from_value(json).unwrap();
        let item = teams_message_to_item(&m, "teams_channel", "C").expect("non-empty body");
        assert_eq!(item.content, "Approvetoday");
    }

    #[test]
    fn teams_message_requires_original_creation_time_even_when_recently_edited() {
        let mut value: serde_json::Value = serde_json::from_str(TEAMS_CHANNEL_MESSAGE).unwrap();
        value["lastModifiedDateTime"] = serde_json::json!("2026-09-30T12:00:00Z");
        let original: TeamsMessage = serde_json::from_value(value.clone()).unwrap();
        let item = teams_message_to_item(&original, "teams_channel", "C").unwrap();
        assert_eq!(item.timestamp, original.created_date_time.unwrap());
        value.as_object_mut().unwrap().remove("createdDateTime");
        let missing: TeamsMessage = serde_json::from_value(value).unwrap();
        assert!(teams_message_to_item(&missing, "teams_channel", "C").is_none());
    }

    // The `NotificationBatch`/`Notification` envelope types live in
    // `routes::graph_webhook` (private there — the webhook route's own
    // concern). These two tests only need the notification's bare `resource`
    // string, so they read it straight out of the fixture's JSON rather than
    // taking on a cross-module type dependency for one field.
    fn first_notification_resource(batch_json: &str) -> String {
        let v: serde_json::Value = serde_json::from_str(batch_json).unwrap();
        v["value"][0]["resource"]
            .as_str()
            .expect("fixture has a resource field")
            .to_string()
    }

    #[test]
    fn fixture_teams_channel_notification_resource_routes_through_teams_path() {
        // This fixture's `resource` is shaped as the STORED/SUBSCRIBED
        // resource template (`teams_subscription_resource`'s own
        // `teams/{team}/channels/{channel}/messages` form, with the message
        // id appended) — the string `is_teams_resource`/`parse_subscribed_
        // teams_resource` actually key on in production (read back from
        // `graph_subscriptions.resource` via `subscription_by_id`), not
        // necessarily byte-identical to what Graph echoes in a live
        // notification's own `resource` field (which `process_teams_
        // notification` only ever uses as a message-fetch path, never for
        // this routing decision — see that function's own doc comment). See
        // this fixture's README entry for the honest caveat.
        let resource = first_notification_resource(TEAMS_CHANNEL_NOTIFICATION);
        assert!(
            is_teams_resource(&resource),
            "a /channels/ resource must route through the Teams path"
        );
        assert_eq!(
            parse_subscribed_teams_resource(&resource),
            Some((
                "teams_channel".into(),
                "19:channel-sprint@thread.tacv2".into()
            ))
        );
    }

    #[test]
    fn fixture_teams_transcript_notification_resolves_meeting_and_online_meeting_resource() {
        let resource = first_notification_resource(TEAMS_TRANSCRIPT_NOTIFICATION);
        assert!(
            is_teams_resource(&resource),
            "an onlineMeetings resource must route through the Teams path"
        );
        assert_eq!(
            meeting_id_from_resource(&resource).as_deref(),
            Some("MSoxMjM0NTY3ODkw")
        );
        assert_eq!(
            online_meeting_resource_from_transcript(&resource).as_deref(),
            Some(
                "users('48d31887-5fad-4d73-a9f5-3c356e68a038')/onlineMeetings('MSoxMjM0NTY3ODkw')"
            )
        );
    }

    #[test]
    fn admin_consent_capability_present() {
        // The org-wide admin-consent capability is exposed regardless of env
        // (building the URL itself may need a client id — asserted only if it
        // builds, so this stays parallel-test safe with the env-var tests above).
        let res = MicrosoftConnector.admin_consent_url("http://localhost:8080", "state-abc");
        assert!(res.is_some(), "microsoft exposes org-wide admin consent");
        if let Some(Ok(url)) = res {
            assert!(url.contains("prompt=admin_consent"), "got: {url}");
        }
    }
}

/// The Microsoft analog of `routes::slack::connector_install_self_heal_tests`
/// — see that module's own doc comment for the full design this mirrors
/// (migration `0121_connector_installs`, m365-enterprise Train 2 PR 2.1).
/// `resolve_microsoft_account_ids` is `pub(crate)`, so this is in-crate (not
/// `backend/tests/*_pg.rs`) — same reasoning as the Slack module.
///
/// Only ONE source table to fall back through here (`oauth_tokens` — Microsoft
/// has never had a `slack_integrations`-shaped legacy table), so this covers
/// three properties instead of Slack's four: the fast path via
/// `connector_installs`, the self-healing single-account fallback, the
/// multi-account fallback (two accounts, one tenant — the property the
/// replaced `resolve_oauth_account_id` `LIMIT 1` could not express), and the
/// genuinely-nowhere case.
///
/// Gated on `ORGT_TEST_BASE_URL`, same scratch-DB convention as every sibling
/// suite. Wired into CI in the `cross-tenant-probe` job
/// (`.github/workflows/ci.yml`), right after the Slack self-heal step.
///
///   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
///     cargo test -p coralswarm-backend --lib microsoft_connector_install_self_heal_tests
#[cfg(test)]
mod microsoft_connector_install_self_heal_tests {
    use sqlx::postgres::PgPoolOptions;
    use sqlx::{Connection, Executor, PgConnection, PgPool};
    use uuid::Uuid;

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

    async fn seed_account(pool: &PgPool, email: &str) -> Uuid {
        let user_id: Uuid = sqlx::query_scalar("INSERT INTO users(email) VALUES ($1) RETURNING id")
            .bind(email)
            .fetch_one(pool)
            .await
            .unwrap();
        sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES ($1) RETURNING id")
            .bind(user_id)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    async fn seed_oauth_tokens_only(pool: &PgPool, account_id: Uuid, tenant_id: &str) {
        sqlx::query(
            "INSERT INTO oauth_tokens (account_id, provider, scope, access_token, \
             provider_workspace_id) VALUES ($1, 'microsoft', 'test', $2, $3)",
        )
        .bind(account_id)
        .bind(b"stub-ciphertext".to_vec())
        .bind(tenant_id)
        .execute(pool)
        .await
        .unwrap();
    }

    async fn connector_installs_count(pool: &PgPool, account_id: Uuid, tenant_id: &str) -> i64 {
        sqlx::query_scalar(
            "SELECT count(*) FROM connector_installs \
             WHERE account_id = $1 AND provider = 'microsoft' AND external_workspace_id = $2",
        )
        .bind(account_id)
        .bind(tenant_id)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn fast_path_reads_connector_installs_directly_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping fast_path_reads_connector_installs_directly_pg: set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_ms_connector_install_fast_path";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new()
            .connect(&with_db(&base, db))
            .await
            .unwrap();
        sqlx::migrate!("./migrations_control")
            .run(&pool)
            .await
            .unwrap();

        let account = seed_account(&pool, "fast-path@test.local").await;
        sqlx::query(
            "INSERT INTO connector_installs (provider, external_workspace_id, account_id) \
             VALUES ('microsoft', 'T-FAST-PATH', $1)",
        )
        .bind(account)
        .execute(&pool)
        .await
        .unwrap();

        let resolved = super::resolve_microsoft_account_ids(&pool, "T-FAST-PATH")
            .await
            .expect("resolve_microsoft_account_ids must succeed");
        assert_eq!(
            resolved,
            vec![account],
            "the indexed row must resolve without touching oauth_tokens at all"
        );

        pool.close().await;
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }

    #[tokio::test]
    async fn fallback_resolves_and_repairs_an_oauth_tokens_only_install_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping fallback_resolves_and_repairs_an_oauth_tokens_only_install_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_ms_connector_install_self_heal_single";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new()
            .connect(&with_db(&base, db))
            .await
            .unwrap();
        sqlx::migrate!("./migrations_control")
            .run(&pool)
            .await
            .unwrap();

        let account = seed_account(&pool, "self-heal-oauth@test.local").await;
        seed_oauth_tokens_only(&pool, account, "T-SELF-HEAL-OAUTH").await;

        assert_eq!(
            connector_installs_count(&pool, account, "T-SELF-HEAL-OAUTH").await,
            0,
            "sanity: no connector_installs row exists before the first resolve"
        );

        let resolved = super::resolve_microsoft_account_ids(&pool, "T-SELF-HEAL-OAUTH")
            .await
            .expect("resolve_microsoft_account_ids must succeed");
        assert_eq!(
            resolved,
            vec![account],
            "the oauth_tokens-only install must still resolve via the fallback"
        );
        assert_eq!(
            connector_installs_count(&pool, account, "T-SELF-HEAL-OAUTH").await,
            1,
            "the fallback must have repaired connector_installs"
        );

        // Second resolve must hit the now-repaired fast path — same answer,
        // no further oauth_tokens dependency exercised (nothing here proves
        // that directly, but the row now existing is the fast-path precondition).
        let resolved_again = super::resolve_microsoft_account_ids(&pool, "T-SELF-HEAL-OAUTH")
            .await
            .expect("resolve_microsoft_account_ids must succeed on the repaired fast path");
        assert_eq!(resolved_again, vec![account]);

        pool.close().await;
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }

    /// The property the old `resolve_oauth_account_id` `LIMIT 1` could not
    /// express: two different CoralSwarm accounts installed under the SAME
    /// Microsoft tenant (two customers, or a customer plus someone's personal
    /// account) must BOTH come back, not an arbitrary one of the two.
    #[tokio::test]
    async fn fallback_returns_every_account_sharing_a_tenant_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping fallback_returns_every_account_sharing_a_tenant_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "orgt_ms_connector_install_self_heal_multi";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new()
            .connect(&with_db(&base, db))
            .await
            .unwrap();
        sqlx::migrate!("./migrations_control")
            .run(&pool)
            .await
            .unwrap();

        let account_a = seed_account(&pool, "self-heal-multi-a@test.local").await;
        let account_b = seed_account(&pool, "self-heal-multi-b@test.local").await;
        seed_oauth_tokens_only(&pool, account_a, "T-SELF-HEAL-SHARED").await;
        seed_oauth_tokens_only(&pool, account_b, "T-SELF-HEAL-SHARED").await;

        let mut resolved = super::resolve_microsoft_account_ids(&pool, "T-SELF-HEAL-SHARED")
            .await
            .expect("resolve_microsoft_account_ids must succeed");
        resolved.sort();
        let mut expected = vec![account_a, account_b];
        expected.sort();
        assert_eq!(
            resolved, expected,
            "BOTH accounts sharing this tenant must come back — an arbitrary LIMIT-1 pick here \
             would silently orphan whichever account it does not choose, exactly the bug this \
             migration exists to kill"
        );
        assert_eq!(
            connector_installs_count(&pool, account_a, "T-SELF-HEAL-SHARED").await,
            1
        );
        assert_eq!(
            connector_installs_count(&pool, account_b, "T-SELF-HEAL-SHARED").await,
            1
        );

        pool.close().await;
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }

    #[tokio::test]
    async fn fallback_finds_nothing_returns_empty_and_writes_nothing_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping fallback_finds_nothing_returns_empty_and_writes_nothing_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_ms_connector_install_self_heal_nothing";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new()
            .connect(&with_db(&base, db))
            .await
            .unwrap();
        sqlx::migrate!("./migrations_control")
            .run(&pool)
            .await
            .unwrap();

        let resolved = super::resolve_microsoft_account_ids(&pool, "T-NEVER-INSTALLED")
            .await
            .expect("resolve_microsoft_account_ids must succeed even when nothing resolves");
        assert!(
            resolved.is_empty(),
            "a tenant with no install anywhere must resolve to an empty set, not error"
        );

        let total: i64 = sqlx::query_scalar("SELECT count(*) FROM connector_installs")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(total, 0, "nothing found anywhere must write nothing");

        pool.close().await;
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }
}

#[cfg(test)]
mod backfill_cursor_pg_tests {
    //! m365 Train 2 PR 2.6: proves the resumable-backfill cursor state
    //! (`backfill_cursor`/`backfill_complete`, migration `0179`/`0061`) round-
    //! trips against a REAL tenant Postgres database — the explicit, queryable
    //! signal that replaces the old silent-truncation log line (contract §6
    //! test #6). Tenant-plane only (not `migrations_control`): the plan's own
    //! wording is "must work on org plane", and `watch_conflict_pg_tests`
    //! (this file's closest precedent, in `routes::teams_ingest`) already
    //! covers the identical `begin_tenant_system_scoped` mechanism this
    //! module's `scoped_tx` rides — see that module's doc for why a tenant DB
    //! alone is sufficient (no genuine control/tenant behavior divergence to
    //! prove for a bare column round-trip).
    //!
    //!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
    //!     cargo test -p coralswarm-backend --lib backfill_cursor_pg_tests

    use sqlx::postgres::PgPoolOptions;
    use sqlx::{Connection, Executor, PgConnection, PgPool};
    use std::sync::Arc;
    use uuid::Uuid;

    use super::*;
    use crate::routes::slack::{AccountPlane, IngestCaller};
    use crate::routes::teams_ingest::install_watch;
    use crate::AppState;

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

    /// A `kind='member'` primary ocean for `owner_user_id` — same precondition
    /// `watch_conflict_pg_tests::seed_member_graph` documents.
    async fn seed_member_graph(pool: &PgPool, account_id: Uuid, owner_user_id: Uuid) -> Uuid {
        sqlx::query_scalar(
            "INSERT INTO graphs (account_id, name, kind, owner_user_id, is_primary) \
             VALUES ($1, 'My Ocean', 'member', $2, true) \
             RETURNING id",
        )
        .bind(account_id)
        .bind(owner_user_id)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    fn state_no_control() -> Arc<AppState> {
        Arc::new(crate::test_app_state_owned(None))
    }

    #[tokio::test]
    async fn conversation_backfill_state_round_trips_on_the_org_plane_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping conversation_backfill_state_round_trips_on_the_org_plane_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_t26_conversation_backfill_cursor";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new()
            .connect(&with_db(&base, db))
            .await
            .unwrap();
        sqlx::migrate!("./migrations_tenant")
            .run(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, 'org_t26_conv_cursor')")
            .execute(&pool)
            .await
            .unwrap();

        let account_id = Uuid::new_v4();
        let user_id = Uuid::new_v4();
        let graph_id = seed_member_graph(&pool, account_id, user_id).await;
        let conversation_id = "AAkALgAAAA-conv-1";

        // Fresh insert must default to `backfill_complete = FALSE` (the
        // crash-safe default for a not-yet-backfilled watch — see
        // `upsert_watched_conversation`'s own comment) and show up in the
        // continuation tick's pending work set immediately.
        upsert_watched_conversation(&pool, account_id, user_id, graph_id, conversation_id, true)
            .await
            .expect("upsert must succeed");
        let pending = pending_conversation_backfills_for_account(&pool, account_id)
            .await
            .expect("pending list must succeed");
        assert_eq!(pending.len(), 1, "a fresh watch must be pending");
        assert_eq!(pending[0].0, conversation_id);
        assert_eq!(pending[0].3, None, "a fresh watch has no cursor yet");

        // A capped fetch persists a resumption cursor and stays pending —
        // the explicit "still fetching" signal, not a silent drop.
        set_conversation_backfill_state(
            &pool,
            account_id,
            conversation_id,
            Some("next-link-1"),
            false,
        )
        .await
        .expect("cursor persist must succeed");
        let pending = pending_conversation_backfills_for_account(&pool, account_id)
            .await
            .unwrap();
        assert_eq!(pending.len(), 1, "still pending after a capped fetch");
        assert_eq!(
            pending[0].3.as_deref(),
            Some("next-link-1"),
            "the resumption cursor must be exactly what was persisted"
        );

        // Continuation finishes it: cursor clears, and it drops out of the
        // pending work set entirely.
        set_conversation_backfill_state(&pool, account_id, conversation_id, None, true)
            .await
            .expect("completion persist must succeed");
        let pending = pending_conversation_backfills_for_account(&pool, account_id)
            .await
            .unwrap();
        assert!(
            pending.is_empty(),
            "a completed backfill must not be in the continuation tick's work set"
        );

        pool.close().await;
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }

    #[tokio::test]
    async fn teams_backfill_state_round_trips_on_the_org_plane_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping teams_backfill_state_round_trips_on_the_org_plane_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_t26_teams_backfill_cursor";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new()
            .connect(&with_db(&base, db))
            .await
            .unwrap();
        sqlx::migrate!("./migrations_tenant")
            .run(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, 'org_t26_teams_cursor')")
            .execute(&pool)
            .await
            .unwrap();

        let account_id = Uuid::new_v4();
        let user_id = Uuid::new_v4();
        let _graph_id = seed_member_graph(&pool, account_id, user_id).await;
        let state = state_no_control();

        // Seed a watched channel through the real install path (not raw SQL)
        // so this proves the actual production insert defaults correctly.
        // `Some(7)` (not `Some(0)`) — a fresh watch with a real, non-deferred
        // window is exactly the "pending backfill" case this test exercises;
        // `Some(0)` is the bot flow's "awaiting the user's choice" sentinel,
        // which now seeds `backfill_complete = TRUE` (CodeRabbit round on PR
        // #673) and would never appear in the pending list at all.
        let (_watch_id, inserted) = install_watch(
            &state,
            &pool,
            account_id,
            user_id,
            AccountPlane::Org,
            IngestCaller::Attributable(user_id),
            "channel",
            "team-t26",
            "chan-t26",
            "",
            "tenant-t26",
            "General",
            Some(7),
        )
        .await
        .expect("install_watch must succeed");
        assert!(inserted, "must be a fresh insert");

        // A second watched resource on the SAME account — a `chat` row, so
        // its `channel_id` is `''` (the unused-field sentinel `set_teams_
        // backfill_state`'s `(channel_id = $4 OR chat_id = $4)` predicate was
        // flagged against, CodeRabbit round on PR #673). Pins the blast
        // radius: completing "chan-t26" below must not touch this row.
        let (_sibling_watch_id, sibling_inserted) = install_watch(
            &state,
            &pool,
            account_id,
            user_id,
            AccountPlane::Org,
            IngestCaller::Attributable(user_id),
            "chat",
            "",
            "",
            "chat-t26-sibling",
            "tenant-t26",
            "Sibling Chat",
            Some(7),
        )
        .await
        .expect("sibling install_watch must succeed");
        assert!(sibling_inserted, "must be a fresh insert");

        let pending = pending_teams_backfills_for_account(&pool, account_id)
            .await
            .expect("pending list must succeed");
        assert_eq!(pending.len(), 2, "both fresh watches must be pending");
        assert_eq!(pending[0].8, None, "a fresh watch has no cursor yet");

        set_teams_backfill_state(
            &pool,
            account_id,
            "chan-t26",
            user_id,
            "app",
            Some("next-link-teams-1"),
            false,
        )
        .await
        .expect("cursor persist must succeed");
        let pending = pending_teams_backfills_for_account(&pool, account_id)
            .await
            .unwrap();
        assert_eq!(pending.len(), 2, "still pending after a capped fetch");
        let channel_row = pending
            .iter()
            .find(|p| p.2 == "chan-t26")
            .expect("channel row must still be present");
        assert_eq!(channel_row.8.as_deref(), Some("next-link-teams-1"));
        let since = channel_row.9.expect("requested floor survives continuation");
        let until = channel_row.10.expect("requested ceiling survives continuation");
        assert!(!in_teams_history_window(since - chrono::Duration::seconds(1), Some(since), Some(until)));
        assert!(in_teams_history_window(since, Some(since), Some(until)));
        // Another user cannot advance this watch, even within the same account.
        set_teams_backfill_state(&pool, account_id, "chan-t26", Uuid::new_v4(), "app", Some("foreign-cursor"), true).await.unwrap();
        let isolated = pending_teams_backfills_for_account(&pool, account_id).await.unwrap();
        let own = isolated.iter().find(|p| p.2 == "chan-t26").unwrap();
        assert_eq!(own.8.as_deref(), Some("next-link-teams-1"));
        assert_eq!((own.9, own.10), (Some(since), Some(until)));

        // Hold a fetched old response while a new request replaces the bounds.
        // Exercise the production completion writer from a separate connection.
        for complete in [false, true] {
            let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(2));
            let release = barrier.clone();
            let writer_pool = pool.clone();
            let old_page = tokio::spawn(async move {
                release.wait().await;
                set_teams_backfill_state_fenced(
                    &writer_pool, account_id, "chan-t26", user_id, "app",
                    Some("stale-page"), complete, Some((Some(since), Some(until))),
                ).await.unwrap();
            });
            let new_until = until + chrono::Duration::seconds(1);
            sqlx::query("UPDATE teams_watched_resources SET backfill_until = $1, backfill_cursor = NULL, backfill_complete = false WHERE account_id = $2 AND channel_id = 'chan-t26'")
                .bind(new_until).bind(account_id).execute(&pool).await.unwrap();
            barrier.wait().await;
            old_page.await.unwrap();
            let fresh = pending_teams_backfills_for_account(&pool, account_id).await.unwrap();
            let fresh = fresh.iter().find(|p| p.2 == "chan-t26").expect("old terminal response cannot complete new run");
            assert_eq!(fresh.8, None, "old page cannot replace new cursor");
            assert_eq!(fresh.10, Some(new_until));
        }

        let chat_row = pending
            .iter()
            .find(|p| p.3 == "chat-t26-sibling")
            .expect("sibling chat row must still be present, untouched");
        assert_eq!(
            chat_row.8, None,
            "sibling chat row's cursor must be untouched by the channel's update"
        );

        set_teams_backfill_state(&pool, account_id, "chan-t26", user_id, "app", None, true)
            .await
            .expect("completion persist must succeed");
        let pending = pending_teams_backfills_for_account(&pool, account_id)
            .await
            .unwrap();
        assert_eq!(
            pending.len(),
            1,
            "the completed channel must drop out, but the sibling chat must not"
        );
        assert_eq!(
            pending[0].3, "chat-t26-sibling",
            "the sibling chat is the one still pending — completing chan-t26 must not \
             have force-completed it via the empty-container_id blast radius"
        );

        pool.close().await;
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }
}

#[cfg(test)]
mod follow_replies_pg_tests {
    //! gmail-options backend train, PR 1 of 4: `outlook_watched_conversations.
    //! follow_replies` (migration `0222`/`0090`) round-trips against a REAL
    //! tenant Postgres database. Tenant-plane only — same "no genuine
    //! control/tenant behavior divergence to prove for a bare column
    //! round-trip" reasoning `backfill_cursor_pg_tests`'s own module doc
    //! gives (this module mirrors its scaffold exactly, including the
    //! `Uuid::new_v4()`-only account/user ids: the tenant DB alone has no
    //! `users`/`accounts` rows to seed).
    //!
    //!   ORGT_TEST_BASE_URL=postgres://postgres:postgres@localhost:5442/postgres \
    //!     cargo test -p coralswarm-backend --lib follow_replies_pg_tests

    use sqlx::postgres::PgPoolOptions;
    use sqlx::{Connection, Executor, PgConnection, PgPool};
    use uuid::Uuid;

    use super::*;

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
    async fn seed_member_graph(pool: &PgPool, account_id: Uuid, owner_user_id: Uuid) -> Uuid {
        sqlx::query_scalar(
            "INSERT INTO graphs (account_id, name, kind, owner_user_id, is_primary) \
             VALUES ($1, 'My Ocean', 'member', $2, true) \
             RETURNING id",
        )
        .bind(account_id)
        .bind(owner_user_id)
        .fetch_one(pool)
        .await
        .unwrap()
    }
    async fn migrated_pool(base: &str, db: &str, org_id: &str) -> (PgConnection, PgPool) {
        let admin_conn = recreate(base, db).await;
        let pool: PgPool = PgPoolOptions::new()
            .connect(&with_db(base, db))
            .await
            .unwrap();
        sqlx::migrate!("./migrations_tenant")
            .run(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, $1)")
            .bind(org_id)
            .execute(&pool)
            .await
            .unwrap();
        (admin_conn, pool)
    }

    /// A save with `follow=false` writes the row with `follow_replies=false`.
    #[tokio::test]
    async fn follow_false_save_writes_follow_replies_false_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping follow_false_save_writes_follow_replies_false_pg: set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_gopt_ms_follow_false_save";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let (mut admin_conn, pool) = migrated_pool(&base, db, "org_gopt_ms_follow_false").await;

        let account_id = Uuid::new_v4();
        let user_id = Uuid::new_v4();
        let graph_id = seed_member_graph(&pool, account_id, user_id).await;

        upsert_watched_conversation(
            &pool,
            account_id,
            user_id,
            graph_id,
            "conv-unfollowed",
            false,
        )
        .await
        .unwrap();
        let follow_replies: bool = sqlx::query_scalar(
            "SELECT follow_replies FROM outlook_watched_conversations \
             WHERE account_id = $1 AND conversation_id = $2",
        )
        .bind(account_id)
        .bind("conv-unfollowed")
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(
            !follow_replies,
            "follow=false must write follow_replies=false, never skip the row"
        );

        pool.close().await;
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }

    /// `watched_conversation_graph` (the webhook reply-auto-capture lookup)
    /// and `watched_conversations_for_account` (the backstop resync's work
    /// set) both exclude an unfollowed row; the deletion lookup
    /// (`atom_graph_for_source`, exercised elsewhere against `atoms`
    /// directly) is untouched by this table's flag entirely — its own doc
    /// establishes it never reads `outlook_watched_conversations` at all.
    #[tokio::test]
    async fn unfollowed_conversation_excluded_from_live_follow_paths_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping unfollowed_conversation_excluded_from_live_follow_paths_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_gopt_ms_unfollow_live";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let (mut admin_conn, pool) = migrated_pool(&base, db, "org_gopt_ms_unfollow_live").await;

        let account_id = Uuid::new_v4();
        let user_id = Uuid::new_v4();
        let graph_id = seed_member_graph(&pool, account_id, user_id).await;

        upsert_watched_conversation(&pool, account_id, user_id, graph_id, "conv-followed", true)
            .await
            .unwrap();
        upsert_watched_conversation(
            &pool,
            account_id,
            user_id,
            graph_id,
            "conv-unfollowed",
            false,
        )
        .await
        .unwrap();

        assert!(
            watched_conversation_graph(&pool, account_id, "conv-followed")
                .await
                .unwrap()
                .is_some(),
            "a followed conversation must still resolve for reply auto-capture"
        );
        assert!(
            watched_conversation_graph(&pool, account_id, "conv-unfollowed")
                .await
                .unwrap()
                .is_none(),
            "an unfollowed conversation must not be auto-captured, even though the row exists"
        );

        let resync_set = watched_conversations_for_account(&pool, account_id)
            .await
            .unwrap();
        assert_eq!(
            resync_set.len(),
            1,
            "only the followed conversation belongs in the resync work set"
        );
        assert_eq!(resync_set[0].0, "conv-followed");

        pool.close().await;
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }

    /// `watched_status`/`set_follow` — the watched-resource routes' store
    /// layer: GET on a missing row is `None` (route maps this to
    /// `following=false`, never `404`); PUT/DELETE (`set_follow`) flip the
    /// flag and NEVER delete the row.
    #[tokio::test]
    async fn watched_status_and_set_follow_never_delete_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping watched_status_and_set_follow_never_delete_pg: set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_gopt_ms_status_set_follow";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let (mut admin_conn, pool) =
            migrated_pool(&base, db, "org_gopt_ms_status_set_follow").await;

        let account_id = Uuid::new_v4();
        let user_id = Uuid::new_v4();
        let graph_id = seed_member_graph(&pool, account_id, user_id).await;

        assert_eq!(
            watched_status(&pool, account_id, "never-saved")
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            set_follow(&pool, account_id, "never-saved", false)
                .await
                .unwrap(),
            None
        );

        upsert_watched_conversation(&pool, account_id, user_id, graph_id, "conv-toggle", true)
            .await
            .unwrap();
        assert_eq!(
            watched_status(&pool, account_id, "conv-toggle")
                .await
                .unwrap(),
            Some(true)
        );

        assert_eq!(
            set_follow(&pool, account_id, "conv-toggle", false)
                .await
                .unwrap(),
            Some(false)
        );
        assert_eq!(
            watched_status(&pool, account_id, "conv-toggle")
                .await
                .unwrap(),
            Some(false)
        );
        assert_eq!(
            set_follow(&pool, account_id, "conv-toggle", true)
                .await
                .unwrap(),
            Some(true)
        );

        let count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM outlook_watched_conversations WHERE account_id = $1",
        )
        .bind(account_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            count, 1,
            "toggling follow must never insert or delete a row"
        );

        pool.close().await;
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }

    /// A later save of an already-watched conversation adopts THAT save's
    /// `follow` value — re-saving with `follow=true` re-follows.
    #[tokio::test]
    async fn resave_with_follow_true_re_follows_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping resave_with_follow_true_re_follows_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "orgt_gopt_ms_resave_refollow";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let (mut admin_conn, pool) = migrated_pool(&base, db, "org_gopt_ms_resave_refollow").await;

        let account_id = Uuid::new_v4();
        let user_id = Uuid::new_v4();
        let graph_id = seed_member_graph(&pool, account_id, user_id).await;

        upsert_watched_conversation_for_save(
            &pool,
            account_id,
            user_id,
            graph_id,
            "conv-resave",
            true,
            None,
        )
        .await
        .unwrap();
        set_follow(&pool, account_id, "conv-resave", false)
            .await
            .unwrap();
        assert_eq!(
            watched_status(&pool, account_id, "conv-resave")
                .await
                .unwrap(),
            Some(false)
        );

        upsert_watched_conversation_for_save(
            &pool,
            account_id,
            user_id,
            graph_id,
            "conv-resave",
            true,
            None,
        )
        .await
        .unwrap();
        assert_eq!(
            watched_status(&pool, account_id, "conv-resave")
                .await
                .unwrap(),
            Some(true),
            "re-saving with follow=true must re-follow"
        );

        pool.close().await;
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }
    #[tokio::test]
    async fn message_scope_boundary_survives_recovery_and_workflow_registration_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping message_scope_boundary_survives_recovery_and_workflow_registration_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "orgt_gopt_ms_message_boundary";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let (mut admin_conn, pool) = migrated_pool(&base, db, "org_gopt_ms_message_boundary").await;
        let account = Uuid::new_v4();
        let user = Uuid::new_v4();
        let graph = seed_member_graph(&pool, account, user).await;
        let since = Utc::now();
        for following in [false, true] {
            upsert_watched_conversation_for_save(
                &pool,
                account,
                user,
                graph,
                "message-only",
                following,
                Some(since),
            )
            .await
            .unwrap();
            let row: (bool, bool, Option<String>, Option<DateTime<Utc>>) = sqlx::query_as(
                "SELECT follow_replies,backfill_complete,backfill_cursor,follow_since FROM outlook_watched_conversations WHERE account_id=$1 AND conversation_id='message-only'",
            ).bind(account).fetch_one(&pool).await.unwrap();
            assert_eq!(row.0, following);
            assert!(
                row.1,
                "the same transaction must mark message scope complete"
            );
            assert!(row.2.is_none());
            assert!(row.3.is_some());
            assert!(pending_conversation_backfills_for_account(&pool, account)
                .await
                .unwrap()
                .is_empty());
            // A rule registering this already-saved conversation must retain
            // both the follow preference and its original forward-only boundary.
            upsert_watched_conversation(&pool, account, user, graph, "message-only", true)
                .await
                .unwrap();
            assert_eq!(
                watched_status(&pool, account, "message-only")
                    .await
                    .unwrap(),
                Some(following)
            );
            let recovery = watched_conversations_for_account(&pool, account)
                .await
                .unwrap();
            if following {
                assert_eq!(recovery.len(), 1);
                let boundary = recovery[0].3.unwrap();
                let message = |id: &str, timestamp: Option<DateTime<Utc>>| {
                    serde_json::from_value::<GraphMessage>(serde_json::json!({
                        "id": id,"conversationId":"message-only","receivedDateTime":timestamp
                    }))
                    .unwrap()
                };
                assert!(!message("historical", Some(since - Duration::days(1)))
                    .is_future_reply(Some(boundary)));
                assert!(message("future", Some(since + Duration::seconds(1)))
                    .is_future_reply(Some(boundary)));
                assert!(!message("unknown-date", None).is_future_reply(Some(boundary)));
                assert!(
                    message("historical", Some(since - Duration::days(1))).is_future_reply(None),
                    "legacy thread saves retain full history"
                );
            } else {
                assert!(recovery.is_empty());
            }
        }
        // A deliberate whole-thread re-save clears the projection and re-follows.
        upsert_watched_conversation_for_save(
            &pool,
            account,
            user,
            graph,
            "message-only",
            true,
            None,
        )
        .await
        .unwrap();
        assert!(watched_conversations_for_account(&pool, account)
            .await
            .unwrap()[0]
            .3
            .is_none());
        pool.close().await;
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }
}

#[cfg(test)]
mod org_thread_optin_pg_tests {
    //! m365 enterprise train PR 3.3 ("org thread opt-in + live follow").
    //! Proves the owner dimension `graph_subscriptions.owner_user_id`
    //! (migration `0190`/`0068`) added by this PR against a REAL tenant
    //! Postgres database:
    //!
    //! 1. An org member's opt-in (`upsert_watched_conversation` +
    //!    `upsert_subscription(..., TokenOwner::Member(uid))`, the exact
    //!    calls `after_ingest`/`ensure_mail_subscriptions` make) lands on the
    //!    TENANT pool with that member's own attribution
    //!    (`outlook_watched_conversations.ingested_by`,
    //!    `graph_subscriptions.owner_user_id`).
    //! 2. A SECOND member opting into the same mail resource
    //!    (`me/mailFolders('inbox')/messages`) is genuinely isolated — before
    //!    this PR's migration, the bare `UNIQUE (account_id, resource)`
    //!    constraint meant the second member's `ensure_subscription_for`
    //!    call would see the FIRST member's row as "already current" and
    //!    silently never create one of its own (the exact gap this PR's
    //!    migration header documents). The two disjoint partial unique
    //!    indexes fix that: both rows coexist, and `subscription_expiry`
    //!    reads back exactly the calling member's own row, never the other's.
    //! 3. `expiring_subscriptions_for_account` (the org-tenant renewal work
    //!    queue — `workers::graph_subscriptions::renew_due_for_account`'s
    //!    real, unmodified caller) returns each member's `owner_user_id`
    //!    correctly, proving renewal's data source can tell the two members'
    //!    subscriptions apart (the prerequisite for
    //!    `renew_one_inner` picking the right one's token).
    //!
    //! Tenant-plane only (`migrations_tenant`, not `migrations_control`) —
    //! same reasoning `backfill_cursor_pg_tests`' own doc comment gives:
    //! every function this module calls (`upsert_watched_conversation`/
    //! `upsert_subscription`/`subscription_expiry`/`expiring_subscriptions_
    //! for_account`) is a plain `begin_tenant_system_scoped` table operation
    //! with no `SECURITY DEFINER` control-only dependency, so a tenant DB
    //! alone is sufficient. (`subscription_by_id` is the one function in this
    //! file that DOES need a control-only function —
    //! `resolve_graph_subscription_account_id`, migration `0086` — and is
    //! proved separately in `subscription_by_id_owner_pg_tests` below,
    //! against `migrations_control`.)
    //!
    //!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
    //!     cargo test -p coralswarm-backend --lib org_thread_optin_pg_tests

    use sqlx::postgres::PgPoolOptions;
    use sqlx::{Connection, Executor, PgConnection, PgPool};
    use uuid::Uuid;

    use super::*;

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
    async fn seed_member_graph(pool: &PgPool, account_id: Uuid, owner_user_id: Uuid) -> Uuid {
        sqlx::query_scalar(
            "INSERT INTO graphs (account_id, name, kind, owner_user_id, is_primary) \
             VALUES ($1, 'My Ocean', 'member', $2, true) \
             RETURNING id",
        )
        .bind(account_id)
        .bind(owner_user_id)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    /// m365 Train 4 PR 4.4, CodeRabbit round 1 finding 1: a subscription
    /// stored with a NARROWER `change_types` than this resource kind needs
    /// today must NOT count as current, however far in the future its expiry
    /// sits. This is the whole legacy fleet's shape — every row that existed
    /// before 4.4 carries migration `0195`/`0072`'s `'created'` default —
    /// and if `live_subscription_is_current` said "yes" for it, the caller
    /// would skip the token mint, `ensure_subscription_for` would never run,
    /// and the row would renew at `created`-only forever: deletion
    /// notifications would never arrive for any pre-existing install, with
    /// nothing anywhere reporting a problem.
    #[tokio::test]
    async fn a_stale_capability_subscription_is_not_current_however_far_off_its_expiry_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping a_stale_capability_subscription_is_not_current_however_far_off_its_\
                 expiry_pg: set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_t44_stale_capability";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new()
            .connect(&with_db(&base, db))
            .await
            .unwrap();
        sqlx::migrate!("./migrations_tenant")
            .run(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, 'org_t44_stale_cap')")
            .execute(&pool)
            .await
            .unwrap();

        let account_id = Uuid::new_v4();
        let resource = "me/mailFolders('inbox')/messages";
        // Exactly what migration 0195/0072's DEFAULT leaves on every row that
        // predates PR 4.4 — written through the real upsert, not hand-rolled
        // SQL, so the column genuinely round-trips the legacy value.
        upsert_subscription(
            &pool,
            account_id,
            "sub-t44-legacy",
            resource,
            "cs-legacy",
            chrono::Utc::now() + chrono::Duration::days(2),
            "",
            TokenOwner::AccountWide,
            "created",
        )
        .await
        .expect("legacy-capability upsert must succeed");

        assert!(
            !live_subscription_is_current(&pool, account_id, resource, TokenOwner::AccountWide)
                .await
                .expect("currency check must succeed"),
            "a `created`-only row must NOT read as current for a resource that now needs \
             created,updated,deleted — otherwise the caller skips the mint and it renews \
             at the narrow capability forever"
        );

        // Same row, rewritten at today's capability: now it IS current, so
        // the check is genuinely discriminating on capability and not just
        // returning false for everything.
        upsert_subscription(
            &pool,
            account_id,
            "sub-t44-widened",
            resource,
            "cs-widened",
            chrono::Utc::now() + chrono::Duration::days(2),
            "",
            TokenOwner::AccountWide,
            subscription_change_types(resource),
        )
        .await
        .expect("widened upsert must succeed");
        assert!(
            live_subscription_is_current(&pool, account_id, resource, TokenOwner::AccountWide)
                .await
                .expect("currency check must succeed"),
            "a row at today's capability with a far-off expiry must read as current"
        );

        pool.close().await;
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }

    #[tokio::test]
    async fn member_opt_in_writes_watched_conversation_and_subscription_with_attribution_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping member_opt_in_writes_watched_conversation_and_subscription_with_\
                 attribution_pg: set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_t33_member_optin_attribution";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new()
            .connect(&with_db(&base, db))
            .await
            .unwrap();
        sqlx::migrate!("./migrations_tenant")
            .run(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, 'org_t33_optin_attr')")
            .execute(&pool)
            .await
            .unwrap();

        let account_id = Uuid::new_v4();
        let member = Uuid::new_v4();
        let graph_id = seed_member_graph(&pool, account_id, member).await;
        let conversation_id = "AAkALgAAAA-t33-conv-1";
        let resource = "me/mailFolders('inbox')/messages";

        // The exact calls `MicrosoftConnector::after_ingest` makes for this
        // member's opt-in.
        upsert_watched_conversation(&pool, account_id, member, graph_id, conversation_id, true)
            .await
            .expect("watch upsert must succeed on the tenant pool");
        upsert_subscription(
            &pool,
            account_id,
            "sub-t33-member-a",
            resource,
            "cs-member-a",
            chrono::Utc::now() + chrono::Duration::hours(1),
            "",
            TokenOwner::Member(member),
            "created,updated,deleted",
        )
        .await
        .expect("subscription upsert must succeed on the tenant pool");

        let watched = watched_conversation_graph(&pool, account_id, conversation_id)
            .await
            .expect("watch lookup must succeed")
            .expect("the just-watched conversation must be found");
        assert_eq!(
            watched,
            (graph_id, member, None),
            "attribution must name the connecting member"
        );

        let (expiry, change_types, _) =
            subscription_expiry(&pool, account_id, resource, TokenOwner::Member(member))
                .await
                .expect("subscription lookup must succeed")
                .expect("the just-created subscription must be found under its owner");
        assert!(
            expiry > chrono::Utc::now(),
            "expiry must be the future value just written"
        );
        assert_eq!(
            change_types, "created,updated,deleted",
            "m365 PR 4.4: the capability the row was created with must round-trip"
        );

        // The AccountWide index must NOT see this member's row — proves the
        // two partial indexes are genuinely disjoint, not a coincidental
        // pass.
        let account_wide =
            subscription_expiry(&pool, account_id, resource, TokenOwner::AccountWide)
                .await
                .expect("account-wide lookup must succeed (finding nothing is not an error)");
        assert_eq!(
            account_wide, None,
            "a member-owned subscription must be invisible to an AccountWide lookup"
        );

        pool.close().await;
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }

    #[tokio::test]
    async fn a_second_members_opt_in_on_the_same_mail_resource_is_isolated_not_shadowed_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping a_second_members_opt_in_on_the_same_mail_resource_is_isolated_not_\
                 shadowed_pg: set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_t33_second_member_isolation";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new()
            .connect(&with_db(&base, db))
            .await
            .unwrap();
        sqlx::migrate!("./migrations_tenant")
            .run(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, 'org_t33_second_member')")
            .execute(&pool)
            .await
            .unwrap();

        let account_id = Uuid::new_v4();
        let member_a = Uuid::new_v4();
        let member_b = Uuid::new_v4();
        let _graph_a = seed_member_graph(&pool, account_id, member_a).await;
        let _graph_b = seed_member_graph(&pool, account_id, member_b).await;
        let resource = "me/mailFolders('inbox')/messages";

        let expiry_a = chrono::Utc::now() + chrono::Duration::hours(1);
        let expiry_b = chrono::Utc::now() + chrono::Duration::hours(2);
        upsert_subscription(
            &pool,
            account_id,
            "sub-t33-a",
            resource,
            "cs-a",
            expiry_a,
            "",
            TokenOwner::Member(member_a),
            "created,updated,deleted",
        )
        .await
        .expect("member A's subscription upsert must succeed");
        // Before this PR's migration, this second upsert for the SAME
        // (account_id, resource) would have been indistinguishable from a
        // re-upsert of member A's own row under a bare 2-column unique
        // constraint. With the owner-scoped partial indexes it must succeed
        // as a genuinely SEPARATE row.
        upsert_subscription(
            &pool,
            account_id,
            "sub-t33-b",
            resource,
            "cs-b",
            expiry_b,
            "",
            TokenOwner::Member(member_b),
            "created,updated,deleted",
        )
        .await
        .expect("member B's subscription upsert must succeed — must NOT collide with A's row");

        let (read_a, _, _) =
            subscription_expiry(&pool, account_id, resource, TokenOwner::Member(member_a))
                .await
                .unwrap()
                .expect("member A's own subscription must still be found");
        let (read_b, _, _) =
            subscription_expiry(&pool, account_id, resource, TokenOwner::Member(member_b))
                .await
                .unwrap()
                .expect("member B's own subscription must be found");
        assert!(
            (read_a - expiry_a).num_seconds().abs() < 2,
            "member A's expiry must be exactly what A wrote, not overwritten by B"
        );
        assert!(
            (read_b - expiry_b).num_seconds().abs() < 2,
            "member B's expiry must be exactly what B wrote"
        );

        // Renewal's real data source (`workers::graph_subscriptions::
        // renew_due_for_account`'s unmodified caller) must be able to tell
        // the two members' rows apart.
        let due = expiring_subscriptions_for_account(
            &pool,
            account_id,
            chrono::Utc::now() + chrono::Duration::hours(3),
        )
        .await
        .expect("expiring_subscriptions_for_account must succeed");
        assert_eq!(
            due.len(),
            2,
            "both members' subscriptions must be in the renewal work set"
        );
        let owner_for = |sub_id: &str| {
            due.iter()
                .find(|(id, ..)| id == sub_id)
                .unwrap_or_else(|| panic!("{sub_id} missing from the renewal work set"))
                .3
        };
        assert_eq!(
            owner_for("sub-t33-a"),
            Some(member_a),
            "sub-t33-a must be owned by A"
        );
        assert_eq!(
            owner_for("sub-t33-b"),
            Some(member_b),
            "sub-t33-b must be owned by B"
        );

        pool.close().await;
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }
}

#[cfg(test)]
mod subscription_by_id_owner_pg_tests {
    //! m365 enterprise train PR 3.3: proves [`super::subscription_by_id`] —
    //! the live webhook's ONE way to discover which member a notified
    //! subscription belongs to before it fetches anything (`routes::
    //! graph_webhook::process_notification`'s `mail_owner` derivation) —
    //! reports the correct `owner_user_id` against a REAL database.
    //!
    //! `migrations_control`, not `migrations_tenant`: `subscription_by_id`
    //! calls `resolve_graph_subscription_account_id()`, a `SECURITY DEFINER`
    //! function that exists ONLY on the control tree (migration `0086`, see
    //! `scripts/check_schema_parity.sh`'s `CONTROL_FUNCTIONS` roster) — see
    //! this file's own RLS-scoping note and `subscription_by_id`'s own doc
    //! comment for why. This is also, today, the ONLY plane this function is
    //! actually reachable on for an org account that has not been migrated to
    //! a dedicated tenant database — see `docs/connectors/outlook.md` §6 for
    //! the known gap that leaves for a genuinely `Active`-migrated org.
    //!
    //!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
    //!     cargo test -p coralswarm-backend --lib subscription_by_id_owner_pg_tests

    use sqlx::postgres::PgPoolOptions;
    use sqlx::{Connection, Executor, PgConnection, PgPool};
    use uuid::Uuid;

    use super::*;

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
    async fn seed_account(pool: &PgPool, owner: Uuid) -> Uuid {
        sqlx::query("INSERT INTO users(id, email) VALUES ($1, $2)")
            .bind(owner)
            .bind(format!("user-{owner}@test.local"))
            .execute(pool)
            .await
            .unwrap();
        sqlx::query_scalar(
            "INSERT INTO accounts(owner_user_id, dek_wrapped) VALUES ($1, '\\x00') RETURNING id",
        )
        .bind(owner)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn subscription_by_id_reports_the_owning_members_uid_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping subscription_by_id_reports_the_owning_members_uid_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_t33_subscription_by_id_owner";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new()
            .connect(&with_db(&base, db))
            .await
            .unwrap();
        sqlx::migrate!("./migrations_control")
            .run(&pool)
            .await
            .unwrap();

        let owner = Uuid::new_v4();
        let account_id = seed_account(&pool, owner).await;
        let member = Uuid::new_v4();
        let resource = "me/mailFolders('inbox')/messages";

        upsert_subscription(
            &pool,
            account_id,
            "sub-t33-owner-lookup",
            resource,
            "cs-owner-lookup",
            chrono::Utc::now() + chrono::Duration::hours(1),
            "",
            TokenOwner::Member(member),
            "created,updated,deleted",
        )
        .await
        .expect("subscription upsert must succeed on the control pool");

        let (resolved_account, _client_state, resolved_resource, tenant_id, owner_user_id) =
            subscription_by_id(&pool, "sub-t33-owner-lookup")
                .await
                .expect("subscription_by_id must succeed")
                .expect("the subscription must be found");
        assert_eq!(resolved_account, account_id);
        assert_eq!(resolved_resource, resource);
        assert_eq!(
            tenant_id, "",
            "a delegated (mail) subscription carries no AAD tenant id"
        );
        assert_eq!(
            owner_user_id,
            Some(member),
            "the webhook's mail_owner derivation depends on this being the connecting member, \
             not None/AccountWide"
        );

        pool.close().await;
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }
}

/// CS-429: a roster member's null/absent `email` must classify as
/// `Unavailable` ("we could not read an address"), never folded into
/// "guest/unmatched" — see [`TeamRosterMember::email`]'s own doc comment and
/// docs/plans/teams-withheld-email-live-test.md §9-10 for the live-tenant
/// evidence this fixes (a same-tenant, unlicensed, team-owner Global
/// Administrator arrived with `"email": null`). Exercises
/// [`resolve_roster_members_from`] — the network-free core
/// `resolve_team_roster_members` wraps — directly with a SYNTHETIC roster
/// against a real control-plane Postgres database: this crate has no
/// HTTP-mock harness for a live Graph roster call (same reasoning as
/// `connectors::slack::extract_verified_slack_profile`'s own doc comment), so
/// the classification + identity-match + membership-check logic is what gets
/// exercised against a real database, not the HTTP leg.
///
///   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
///     cargo test -p coralswarm-backend --lib roster_unavailable_pg_tests -- --test-threads=1
#[cfg(test)]
mod roster_unavailable_pg_tests {
    use sqlx::postgres::PgPoolOptions;
    use sqlx::{Connection, Executor, PgConnection, PgPool};
    use uuid::Uuid;

    use super::*;

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

    /// One CoralSwarm user, a fresh account they own, and their own
    /// membership row — exactly what the `users` email lookup and the
    /// `account_members` batch check need. Returns `(account_id, user_id)`.
    async fn seed_account_with_member(pool: &PgPool, email: &str) -> (Uuid, Uuid) {
        let user_id: Uuid = sqlx::query_scalar("INSERT INTO users(email) VALUES ($1) RETURNING id")
            .bind(email)
            .fetch_one(pool)
            .await
            .unwrap();
        let account_id: Uuid =
            sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES ($1) RETURNING id")
                .bind(user_id)
                .fetch_one(pool)
                .await
                .unwrap();
        sqlx::query(
            "INSERT INTO account_members (account_id, user_id, role) VALUES ($1, $2, 'owner')",
        )
        .bind(account_id)
        .bind(user_id)
        .execute(pool)
        .await
        .unwrap();
        (account_id, user_id)
    }

    fn roster_member(
        aad_user_id: &str,
        email: Option<&str>,
        display_name: &str,
    ) -> TeamRosterMember {
        TeamRosterMember {
            aad_user_id: aad_user_id.to_string(),
            email: email.map(str::to_string),
            display_name: Some(display_name.to_string()),
            roles: Vec::new(),
            tenant_id: None,
        }
    }

    // ── tracing capture — same `MakeWriter` technique as `workers::
    // billing_derivation`'s `LogBuf` (see that module's own doc comment):
    // `resolve_roster_members_from`'s return value carries no signal at all
    // about whether the health warning fired, so capturing what it actually
    // logged is the only way to prove it.
    #[derive(Clone, Default)]
    struct LogBuf(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);
    impl std::io::Write for LogBuf {
        fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(b);
            Ok(b.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for LogBuf {
        type Writer = LogBuf;
        fn make_writer(&'a self) -> LogBuf {
            self.clone()
        }
    }

    /// Run `resolve_roster_members_from` under a captured subscriber and
    /// return `(the call's own result, everything logged at WARN or above)`.
    /// `#[tokio::test]` is single-threaded by default, so `set_default`'s
    /// thread-local guard stays active across the `.await` inside — same
    /// technique and rationale as `workers::billing_derivation::
    /// tick_capturing_logs`.
    async fn resolve_capturing_logs(
        control_pool: &PgPool,
        content_pool: &PgPool,
        account_id: Uuid,
        tenant_id: &str,
        team_id: &str,
        roster: Vec<TeamRosterMember>,
    ) -> (Result<TeamRosterResolution, String>, String) {
        let buf = LogBuf::default();
        let sub = tracing_subscriber::fmt()
            .with_writer(buf.clone())
            .with_max_level(tracing::Level::WARN)
            .with_ansi(false)
            .finish();
        let _guard = tracing::subscriber::set_default(sub);
        let result = resolve_roster_members_from(
            control_pool,
            content_pool,
            account_id,
            tenant_id,
            team_id,
            roster,
        )
        .await;
        drop(_guard);
        let logged =
            String::from_utf8(buf.0.lock().unwrap().clone()).expect("captured log is UTF-8");
        (result, logged)
    }

    /// CS-429 scenario 1 (the exact live-tenant shape, §9): a roster of
    /// three where ONE has `email: None`. The two with a readable, matching
    /// address still resolve — delivery to them is unchanged — the
    /// null-email member is `unavailable`, never `unmapped`; `unmapped`
    /// reads 0 because every OTHER member in this roster has a matching
    /// account (scenario 3 covers the has-an-address-but-matches-nobody
    /// case).
    #[tokio::test]
    async fn one_null_email_of_three_is_unavailable_not_unmapped_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping one_null_email_of_three_is_unavailable_not_unmapped_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_cs429_one_null_of_three";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new()
            .connect(&with_db(&base, db))
            .await
            .unwrap();
        sqlx::migrate!("./migrations_control")
            .run(&pool)
            .await
            .unwrap();

        let (account_id, user_a) = seed_account_with_member(&pool, "a-cs429@test.local").await;
        let user_b: Uuid = sqlx::query_scalar("INSERT INTO users(email) VALUES ($1) RETURNING id")
            .bind("b-cs429@test.local")
            .fetch_one(&pool)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO account_members (account_id, user_id, role) VALUES ($1, $2, 'member')",
        )
        .bind(account_id)
        .bind(user_b)
        .execute(&pool)
        .await
        .unwrap();

        let roster = vec![
            roster_member("aad-a", Some("a-cs429@test.local"), "A"),
            roster_member("aad-b", Some("b-cs429@test.local"), "B"),
            // The unlicensed-Member/team-owner shape from the live test
            // (docs/plans/teams-withheld-email-live-test.md §9): `email:
            // null`, not an omitted guest.
            roster_member("aad-c-unlicensed", None, "C (unlicensed)"),
        ];

        let (result, logged) = resolve_capturing_logs(
            &pool,
            &pool,
            account_id,
            "tenant-cs429-one-null",
            "team-cs429-one-null",
            roster,
        )
        .await;

        let resolved = result.expect("roster resolution must succeed");
        assert_eq!(resolved.total, 3, "the full roster is three humans");
        assert_eq!(
            resolved.matched_user_ids.len(),
            2,
            "A and B — both have a readable, matching address — must still resolve"
        );
        assert!(
            resolved.matched_user_ids.contains(&user_a)
                && resolved.matched_user_ids.contains(&user_b)
        );
        assert_eq!(
            resolved.unavailable, 1,
            "exactly the null-email member (C) is unavailable"
        );
        assert!(
            resolved.unmatched_emails.is_empty(),
            "a missing email is unavailable, not an unmatched address"
        );
        let unmapped = resolved.total - resolved.matched_user_ids.len() - resolved.unavailable;
        assert_eq!(
            unmapped, 0,
            "no genuinely unmatched-with-an-address member in this roster"
        );

        assert!(
            logged.contains("could not read some roster members' addresses"),
            "the specific partial-unavailable warning must fire; captured: {logged:?}"
        );
        assert!(
            !logged.contains("could not read any member's address"),
            "must not fire the ALL-unavailable message when only one of three is unavailable; \
             captured: {logged:?}"
        );

        pool.close().await;
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }

    /// CS-429 scenario 2: the whole-tenant variant
    /// (docs/plans/teams-withheld-email-live-test.md §5-6) — an all-null
    /// roster. Zero destinations, `unavailable == total`, and the warning
    /// must say it could not READ any address — never "found no members" /
    /// "found nobody", which would misdescribe an unreadable roster as an
    /// empty one.
    #[tokio::test]
    async fn all_null_roster_is_unavailable_total_with_the_specific_warning_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping all_null_roster_is_unavailable_total_with_the_specific_warning_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_cs429_all_null";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new()
            .connect(&with_db(&base, db))
            .await
            .unwrap();
        sqlx::migrate!("./migrations_control")
            .run(&pool)
            .await
            .unwrap();

        let (account_id, _owner) =
            seed_account_with_member(&pool, "owner-cs429-allnull@test.local").await;

        let roster = vec![
            roster_member("aad-x", None, "X"),
            roster_member("aad-y", None, "Y"),
            roster_member("aad-z", None, "Z"),
        ];

        let (result, logged) = resolve_capturing_logs(
            &pool,
            &pool,
            account_id,
            "tenant-cs429-all-null",
            "team-cs429-all-null",
            roster,
        )
        .await;

        let resolved = result.expect("roster resolution must succeed");
        assert_eq!(resolved.total, 3);
        assert!(
            resolved.matched_user_ids.is_empty(),
            "zero destinations — nobody in this roster has a readable address"
        );
        assert!(
            resolved.unmatched_emails.is_empty(),
            "a null email is unavailable, never an unmatched address"
        );
        assert_eq!(
            resolved.unavailable, resolved.total,
            "every roster member is unavailable, not unmapped"
        );

        assert!(
            logged.contains("could not read any member's address"),
            "the all-unavailable warning must fire; captured: {logged:?}"
        );
        assert!(
            !logged.contains("found no members") && !logged.contains("found nobody"),
            "must never describe an unreadable roster as an EMPTY one; captured: {logged:?}"
        );

        pool.close().await;
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }

    /// CS-429 scenario 3 (regression guard): a member WITH an address that
    /// matches no CoralSwarm account is still `unmapped`/Unmatched —
    /// `unavailable` must stay 0 and the health warning must not fire. This
    /// is the "genuine guest with an address on file" case the org-onboarding
    /// plan's D17 table requires to remain distinguishable from
    /// `Unavailable` (only `Unmatched` is ever a future-invite candidate).
    #[tokio::test]
    async fn an_address_matching_nobody_is_unmapped_not_unavailable_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping an_address_matching_nobody_is_unmapped_not_unavailable_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_cs429_unmapped_guest";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new()
            .connect(&with_db(&base, db))
            .await
            .unwrap();
        sqlx::migrate!("./migrations_control")
            .run(&pool)
            .await
            .unwrap();

        let (account_id, _owner) =
            seed_account_with_member(&pool, "owner-cs429-guest@test.local").await;

        // No `users` row anywhere has this address — a genuine guest.
        let roster = vec![roster_member(
            "aad-guest",
            Some("guest@outside.example.com"),
            "Guest",
        )];

        let (result, logged) = resolve_capturing_logs(
            &pool,
            &pool,
            account_id,
            "tenant-cs429-guest",
            "team-cs429-guest",
            roster,
        )
        .await;

        let resolved = result.expect("roster resolution must succeed");
        assert_eq!(resolved.total, 1);
        assert!(
            resolved.matched_user_ids.is_empty(),
            "a guest with no matching CoralSwarm account never resolves"
        );
        assert_eq!(
            resolved.unmatched_emails,
            vec!["guest@outside.example.com".to_string()],
            "a readable address with no CoralSwarm account is returned for the invite job"
        );
        assert!(resolved.known.is_empty());
        assert_eq!(
            resolved.unavailable, 0,
            "an address that simply matches nobody is unmapped, not unavailable"
        );
        assert!(
            !logged.contains("connector_health"),
            "an ordinary unmatched guest must never trip the unavailable health warning; \
             captured: {logged:?}"
        );

        pool.close().await;
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }

    /// A blank home tenant must not drop every row as a guest, a `guest` role
    /// must not land in `unmatched_emails`, and a missing email stays
    /// `unavailable`. A known user who is not an account member is `known`,
    /// not an unmatched address.
    #[tokio::test]
    async fn unmatched_addresses_keep_non_guests_when_home_tenant_is_blank_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping unmatched_addresses_keep_non_guests_when_home_tenant_is_blank_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_cs468_unmatched_blank_home";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new()
            .connect(&with_db(&base, db))
            .await
            .unwrap();
        sqlx::migrate!("./migrations_control")
            .run(&pool)
            .await
            .unwrap();

        let (account_id, _owner) = seed_account_with_member(&pool, "owner-cs468@test.local").await;
        let known_outsider: Uuid =
            sqlx::query_scalar("INSERT INTO users(email) VALUES ($1) RETURNING id")
                .bind("known-out@test.local")
                .fetch_one(&pool)
                .await
                .unwrap();

        let roster = vec![
            roster_member("aad-known", Some("known-out@test.local"), "Known"),
            roster_member("aad-unknown", Some("nobody@test.local"), "Nobody"),
            TeamRosterMember {
                aad_user_id: "aad-guest".into(),
                email: Some("guest-addr@test.local".into()),
                display_name: Some("Guest".into()),
                roles: vec!["guest".into()],
                tenant_id: Some("other".into()),
            },
            roster_member("aad-blank", None, "No email"),
            TeamRosterMember {
                aad_user_id: "aad-foreign".into(),
                email: Some("foreign@test.local".into()),
                display_name: Some("Foreign".into()),
                roles: vec!["owner".into()],
                tenant_id: Some("other-tenant".into()),
            },
        ];

        let (result, _) = resolve_capturing_logs(
            &pool,
            &pool,
            account_id,
            "",
            "team-cs468-blank-home",
            roster,
        )
        .await;
        let resolved = result.expect("roster resolution must succeed");
        assert_eq!(
            resolved.total, 4,
            "the guest role is filtered before the count"
        );
        assert_eq!(
            resolved.unavailable, 1,
            "the blank email is unavailable, not a guest"
        );
        assert!(resolved.matched_user_ids.is_empty());
        assert_eq!(
            resolved.unmatched_emails,
            vec![
                "nobody@test.local".to_string(),
                "foreign@test.local".to_string()
            ],
            "a blank home tenant must keep the foreign-tenant address; the guest role must not"
        );
        assert_eq!(resolved.known.len(), 1);
        assert_eq!(resolved.known[0].user_id, known_outsider);
        assert_eq!(resolved.known[0].email, "known-out@test.local");

        pool.close().await;
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }
}

#[cfg(test)]
mod meeting_consent_ingress_tests {
    #[test]
    fn transcript_metadata_requires_exact_identity_and_original_creation_time() {
        use serde_json::json;
        assert!(super::transcript_metadata_created_at(
            &json!({"id":"other", "createdDateTime":"2026-10-01T12:00:00Z"}),
            "requested"
        )
        .is_none());
        assert!(super::transcript_metadata_created_at(
            &json!({"id":"requested", "lastModifiedDateTime":"2026-10-01T12:00:00Z"}),
            "requested"
        )
        .is_none());
        assert!(super::transcript_metadata_created_at(
            &json!({"id":"requested", "createdDateTime":"invalid"}),
            "requested"
        )
        .is_none());
        assert_eq!(super::transcript_metadata_created_at(&json!({"id":"requested", "createdDateTime":"2026-10-01T12:00:00Z", "lastModifiedDateTime":"2026-10-02T12:00:00Z"}), "requested").unwrap().to_rfc3339(), "2026-10-01T12:00:00+00:00");
    }
    #[tokio::test]
    async fn legacy_calendar_discovery_never_reads_transcript_content() {
        // No database, grant, calendar or Graph server exists: the former
        // broad-discovery entry cannot issue a provider request at all.
        let items = super::fetch_meeting_transcripts(
            &reqwest::Client::new(),
            "invalid-unused-token",
            None,
            uuid::Uuid::new_v4(),
        )
        .await
        .unwrap();
        assert!(items.is_empty());
    }
}

/// Scope replacements require a complete discovery result; never truncate on a failed page.
async fn list_all_scope_items<T: serde::de::DeserializeOwned>(
    _http: &reqwest::Client, token: &str, mut url: String,
) -> Result<Vec<T>, ConnectorError> {
    let http = chat_list_http()?;
    let mut seen = std::collections::HashSet::new();
    let mut items = Vec::new();
    loop {
        if !seen.insert(url.clone()) { return Err(ConnectorError::Exchange("scope discovery repeated cursor".into())); }
        let page: GraphList<T> = serde_json::from_value(graph_get(http, token, &url).await?).map_err(|e| ConnectorError::Exchange(format!("scope discovery decode: {e}")))?;
        items.extend(page.value);
        match page.next_link {
            None => return Ok(items),
            Some(next) if chat_list_next_link_allowed(&next) => url = next,
            Some(_) => return Err(ConnectorError::Exchange("scope discovery continuation outside Graph".into())),
        }
    }
}
/// Inclusive durable history window; shared by first and continuation passes.
pub(crate) fn in_teams_history_window(timestamp: DateTime<Utc>, since: Option<DateTime<Utc>>, until: Option<DateTime<Utc>>) -> bool {
    since.is_none_or(|bound| timestamp >= bound) && until.is_none_or(|bound| timestamp <= bound)
}

#[cfg(test)]
mod teams_history_bounds_tests {
    use super::*;
    #[tokio::test]
    async fn resumed_chat_page_excludes_old_messages() {
        use axum::{routing::get, Json, Router};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/continued", listener.local_addr().unwrap());
        let app = Router::new().route("/continued", get(|| async {
            let mut old: serde_json::Value = serde_json::from_str(include_str!("../../tests/fixtures/graph/teams_chat_message.json")).unwrap();
            old["createdDateTime"] = serde_json::json!("2026-01-01T00:00:00Z");
            let mut recent = old.clone();
            recent["id"] = serde_json::json!("recent");
            recent["createdDateTime"] = serde_json::json!("2026-02-01T00:00:00Z");
            Json(serde_json::json!({"value": [old, recent]}))
        }));
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap(); });
        let since = "2026-01-25T00:00:00Z".parse().unwrap();
        let page = fetch_chat_messages(&reqwest::Client::new(), "test", "chat", Some(since), Some(&url)).await.unwrap();
        assert!(page.complete);
        assert_eq!(page.items.len(), 1);
        assert!(page.items[0].timestamp >= since);
        server.abort();
    }
    #[test]
    fn continuation_keeps_both_inclusive_bounds() {
        let until = Utc::now();
        let since = until - chrono::Duration::days(7);
        for _page in 0..3 {
            assert!(!in_teams_history_window(since - chrono::Duration::seconds(1), Some(since), Some(until)));
            assert!(in_teams_history_window(since, Some(since), Some(until)));
            assert!(in_teams_history_window(until, Some(since), Some(until)));
            assert!(!in_teams_history_window(until + chrono::Duration::seconds(1), Some(since), Some(until)));
        }
    }
}
