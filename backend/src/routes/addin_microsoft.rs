//! Microsoft SSO sign-in for the Teams meeting pane and the Outlook panes.
//!
//! # Why this exists
//!
//! The panes used to mount Clerk `<SignIn>` inside the host iframe. Clerk's
//! session cookie is third-party there and Teams on Mac blocks it, so every new
//! meeting asked the user to sign in again. The panes now obtain the user's
//! MICROSOFT identity from the host (Teams `getAuthToken()` / Office nested app
//! auth) and this module exchanges it for a short-lived CoralSwarm credential.
//! [`crate::microsoft_sso`] decides whether the Microsoft token is real; this
//! module decides what a real one is allowed to become.
//!
//! # The flow
//!
//! 1. **`POST /v1/addin/microsoft/session`** (unauthenticated; the Microsoft
//!    token IS the authentication). Body `{"ms_token": "..."}`. Behind
//!    `ADDIN_MICROSOFT_SSO_ENABLED` (default off → `404
//!    {"error":"microsoft_sso_disabled"}`, and the pane falls back to Clerk).
//!    The verified identity is `(tid, oid)` — never anything the client sent.
//!    * A **link** exists for `(tid, oid)` and its user is still a LIVE member
//!      of the linked workspace → `connected` + a `csms_` credential (~1h).
//!      Membership is re-checked on every exchange; a user who lost access is
//!      treated as unlinked (the dead link is removed).
//!    * No link, but **exactly one** CoralSwarm user has already done "Connect
//!      Microsoft" with that same Microsoft account (see
//!      `resolve_microsoft_connected_users`, migration 0264) → the link is
//!      created to that user + the workspace that connection belongs to, and
//!      the response is `connected` with `"auto_linked": true`. Zero or several
//!      matches → fall through. A link the user explicitly disconnected is NEVER
//!      auto-recreated.
//!    * Otherwise → `link_required` + a **pairing** (`code`, `poll_secret`,
//!      `approve_url`). The pairing row is BOUND, at creation, to the verified
//!      `(tid, oid)`.
//! 2. The pane opens `approve_url` in the user's browser. The web approve page
//!    calls **`GET /v1/addin/pair/info?code=`** (Clerk-authenticated) to render a
//!    workspace picker, then **`POST /v1/addin/pair/approve`** with
//!    `{code, account_id}` — see [`approve_pairing`]. Membership of the chosen
//!    workspace is verified server-side; the identity written to the link is the
//!    one the pairing row was bound to, never a request field.
//! 3. The pane polls **`POST /v1/addin/pair/poll`** (existing). For a Microsoft
//!    pairing `approved` carries NO token; the pane calls `/session` again and
//!    gets `connected`.
//! 4. With the credential: **`GET /v1/addin/microsoft/link`**, **`PUT
//!    /v1/addin/microsoft/link/workspace`** ("Change workspace") and **`DELETE
//!    /v1/addin/microsoft/link`** ("Disconnect") — each authenticated by the
//!    `csms_` credential and each acting on the ONE link Teams and Outlook
//!    share.
//!
//! # The credential (`csms_`)
//!
//! `csms_` + 32 CSPRNG bytes, SHA-256 hashed at rest in
//! `addin_microsoft_sessions` (migration 0264). It is NOT a PAT: it expires in
//! [`SESSION_TTL_MINUTES`] and the pane keeps it in memory only, re-exchanging
//! on expiry or 401. `middleware::auth::require_auth` recognizes the prefix,
//! resolves the session via [`resolve_session`], **re-verifies workspace access
//! on every request** ([`verify_workspace_access`]) and binds the request to the
//! LINKED workspace's tenancy:
//!
//! * personal workspace → `TokenKind::Personal` (the consumer plane);
//! * org workspace → `TokenKind::Org(clerk_org_id)`, i.e. the org's tenant
//!   plane through the binding gate exactly as an org Clerk session would be.
//!   An org credential can never be served the consumer plane: there is no code
//!   path that maps an org link to `Personal`, and a lost membership makes the
//!   credential 401 (so the pane re-exchanges and sees `link_required`).
//!
//! # Scope
//!
//! A session credential lives in an iframe next to host-controlled script, so it
//! is deliberately narrower than a Clerk session. [`session_scope`] (layered on
//! `/v1`) allows only what the panes
//! need: Outlook email save (`/connectors/microsoft/ingest`, `.../watched/...`),
//! the whole-mailbox switch (`GET`/`POST /connectors/microsoft/everything-mode`),
//! the meeting-capture routes, and the link routes above. Everything else —
//! including other providers' everything-mode, workflows, Teams channel ingest, token minting —
//! is refused with the `addin_session_scope` 403. Widening is a one-line change
//! to [`scope_allows`].
//!
//! Meeting capture works in an org workspace (member grant) and in a personal
//! workspace connected to a work/school Microsoft account (its owner's account-wide
//! grant; consumer Microsoft accounts get `consumer_identity_unsupported`). Email save
//! works in any workspace.

use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use axum::{
    extract::{Request, State},
    http::{HeaderMap, Method, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    Extension, Json,
};
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx::{PgConnection, PgPool};
use uuid::Uuid;

use crate::{
    middleware::auth::{live_org_membership, UserContext},
    microsoft_sso::{self, MicrosoftSsoConfig, MicrosoftTokenError, VerifiedMicrosoftIdentity},
    routes::{
        addin as gmail_addin,
        pat::{generate_pat, hash_token},
    },
    AppState,
};

// ── Constants ─────────────────────────────────────────────────────────────────

/// Prefix on every session credential — greppable, and the shape
/// `middleware::auth::require_auth` routes to [`resolve_session`] by.
pub(crate) const SESSION_TOKEN_PREFIX: &str = "csms_";

/// Credential lifetime. The pane re-exchanges on expiry or 401.
const SESSION_TTL_MINUTES: i64 = 60;

/// `/session` per-IP ceiling, applied BEFORE any signature work. The endpoint is
/// unauthenticated and does an RSA verify per call.
const SESSION_IP_RATE_MAX: u32 = 120;
const SESSION_IP_RATE_WINDOW_SECS: u64 = 60;
const SESSION_UNKNOWN_IP_KEY: &str = "addin_ms_session_ip:unknown";
/// `/session` per-verified-identity ceiling. A pane exchanges roughly once an
/// hour; 30/min is generous for retries and bounds session-row growth.
const SESSION_IDENTITY_RATE_MAX: u32 = 30;
const SESSION_IDENTITY_RATE_WINDOW_SECS: u64 = 60;
/// Pairing creation per verified identity. Each creation writes a row and
/// replaces the identity's previous one, so this bounds churn on the table.
const PAIRING_CREATE_RATE_MAX: u32 = 5;
const PAIRING_CREATE_RATE_WINDOW_SECS: u64 = 60;
/// Link mutations (`PUT workspace`) per user.
const LINK_MUTATION_RATE_MAX: u32 = 20;
const LINK_MUTATION_RATE_WINDOW_SECS: u64 = 60;

/// Cap on the org rows in a workspace list — same silent-truncation reasoning
/// as `mcp::tools::tenancy::LIST_TENANTS_ORG_CAP`.
const WORKSPACE_ORG_CAP: i64 = 200;
const ORG_NAME_LOOKUP_CONCURRENCY: usize = 8;
const ORG_NAME_CACHE_TTL: Duration = Duration::from_secs(600);

/// Largest `ms_token` we will even try to parse. Real AAD access tokens are
/// 1-3 KiB; this bounds what an unauthenticated caller can make us base64-decode.
const MS_TOKEN_MAX_LEN: usize = 16 * 1024;

// ── Workspaces ────────────────────────────────────────────────────────────────

/// What kind of workspace an account is, as the add-in sees it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum WorkspaceKind {
    /// The user's own account-of-one (consumer plane).
    Personal,
    /// An organization (`accounts.clerk_org_id`) — the tenant plane.
    Org(String),
}

impl WorkspaceKind {
    fn label(&self) -> &'static str {
        match self {
            Self::Personal => "personal",
            Self::Org(_) => "org",
        }
    }
}

/// A live, verified right to act in a workspace.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct WorkspaceAccess {
    pub(crate) kind: WorkspaceKind,
    /// The member's CURRENT role from `account_members` (org), or `owner`
    /// (personal). Never taken from a token.
    pub(crate) role: String,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub(crate) struct Workspace {
    pub(crate) account_id: Uuid,
    pub(crate) name: String,
    pub(crate) kind: &'static str,
}

/// Is `user_id` currently allowed to act in `account_id`?
///
/// * a personal account: only its owner, and only while the user is not deleted;
/// * an org account: a LIVE `account_members` row for that org
///   ([`live_org_membership`] — the same query `require_auth` and `/mcp` use,
///   including the `org_revocations` wholesale-revocation guard), and the org
///   must not be the caller's OWN personal organization (those are the
///   double-representation `personal_orgs` exists to translate away; they are
///   never offered, `mcp::tools::tenancy::list_tenants` excludes them the same
///   way).
///
/// `Ok(None)` means "no" — callers never distinguish "no such account" from "not
/// a member".
pub(crate) async fn verify_workspace_access(
    pool: &PgPool,
    user_id: Uuid,
    account_id: Uuid,
) -> Result<Option<WorkspaceAccess>, sqlx::Error> {
    let row: Option<(Option<String>, Uuid, bool)> = sqlx::query_as(
        "SELECT a.clerk_org_id, a.owner_user_id, u.deleted_at IS NOT NULL \
         FROM accounts a, users u WHERE a.id = $1 AND u.id = $2",
    )
    .bind(account_id)
    .bind(user_id)
    .fetch_optional(crate::tenancy::scope::request_read(pool))
    .await?;
    let Some((clerk_org_id, owner_user_id, user_deleted)) = row else {
        return Ok(None);
    };
    if user_deleted {
        return Ok(None);
    }
    match clerk_org_id {
        None => Ok((owner_user_id == user_id).then(|| WorkspaceAccess {
            kind: WorkspaceKind::Personal,
            role: "owner".to_string(),
        })),
        Some(org_id) => {
            let own_personal_org: bool = sqlx::query_scalar(
                "SELECT EXISTS (SELECT 1 FROM personal_orgs p \
                   JOIN users u ON u.clerk_user_id = p.clerk_user_id \
                  WHERE p.clerk_org_id = $1 AND u.id = $2)",
            )
            .bind(&org_id)
            .bind(user_id)
            .fetch_one(crate::tenancy::scope::request_read(pool))
            .await?;
            if own_personal_org {
                return Ok(None);
            }
            Ok(match live_org_membership(pool, &org_id, user_id).await? {
                Some((live_account, role)) if live_account == account_id => Some(WorkspaceAccess {
                    kind: WorkspaceKind::Org(org_id),
                    role,
                }),
                _ => None,
            })
        }
    }
}

fn org_name_cache() -> &'static DashMap<String, (String, Instant)> {
    static CACHE: OnceLock<DashMap<String, (String, Instant)>> = OnceLock::new();
    CACHE.get_or_init(DashMap::new)
}

/// Best-effort org display names: process-local TTL cache, then live Clerk
/// (bounded concurrency), then the org's `tenant_databases.slug`, then a generic
/// label. `accounts` has no name column. Never fails and never fabricates a
/// name for someone else's org — it only runs for orgs the caller is a member of.
async fn org_names(pool: &PgPool, org_ids: &[String]) -> Vec<String> {
    let mut names: Vec<Option<String>> = org_ids
        .iter()
        .map(|id| {
            org_name_cache()
                .get(id)
                .filter(|e| e.1.elapsed() < ORG_NAME_CACHE_TTL)
                .map(|e| e.0.clone())
        })
        .collect();

    let missing: Vec<(usize, String)> = org_ids
        .iter()
        .enumerate()
        .filter(|(i, _)| names[*i].is_none())
        .map(|(i, id)| (i, id.clone()))
        .collect();
    if !missing.is_empty() {
        if let Some(clerk) = crate::clerk_admin::ClerkAdminClient::from_env() {
            let futures: Vec<_> = missing
                .iter()
                .map(|(_, id)| {
                    let clerk = &clerk;
                    async move { clerk.get_organization(id).await.ok().map(|o| o.name) }
                })
                .collect();
            let fetched: Vec<Option<String>> = futures::stream::iter(futures)
                .buffered(ORG_NAME_LOOKUP_CONCURRENCY)
                .collect()
                .await;
            for ((i, id), name) in missing.iter().zip(fetched) {
                if let Some(name) = name {
                    org_name_cache().insert(id.clone(), (name.clone(), Instant::now()));
                    names[*i] = Some(name);
                }
            }
        }
    }

    let mut out = Vec::with_capacity(org_ids.len());
    for (i, id) in org_ids.iter().enumerate() {
        if let Some(n) = names[i].take() {
            out.push(n);
            continue;
        }
        let slug: Option<String> =
            sqlx::query_scalar("SELECT slug FROM tenant_databases WHERE clerk_org_id = $1")
                .bind(id)
                .fetch_optional(crate::tenancy::scope::request_read(pool))
                .await
                .ok()
                .flatten();
        out.push(slug.unwrap_or_else(|| "Organization".to_string()));
    }
    out
}

/// The [`Workspace`] for an access the caller has ALREADY verified.
async fn workspace_for(pool: &PgPool, account_id: Uuid, access: &WorkspaceAccess) -> Workspace {
    let name = match &access.kind {
        WorkspaceKind::Personal => "Personal".to_string(),
        WorkspaceKind::Org(org_id) => org_names(pool, std::slice::from_ref(org_id))
            .await
            .into_iter()
            .next()
            .unwrap_or_else(|| "Organization".to_string()),
    };
    Workspace {
        account_id,
        name,
        kind: access.kind.label(),
    }
}

/// Every workspace `user_id` may pick: their personal account first, then each
/// org they are a live member of. Capped at [`WORKSPACE_ORG_CAP`] orgs.
pub(crate) async fn list_workspaces(pool: &PgPool, user_id: Uuid) -> Result<Vec<Workspace>, sqlx::Error> {
    let mut out = Vec::new();
    let personal: Option<Uuid> = sqlx::query_scalar(
        "SELECT a.id FROM accounts a JOIN users u ON u.id = a.owner_user_id \
         WHERE a.owner_user_id = $1 AND a.clerk_org_id IS NULL AND u.deleted_at IS NULL \
         LIMIT 1",
    )
    .bind(user_id)
    .fetch_optional(crate::tenancy::scope::request_read(pool))
    .await?;
    if let Some(account_id) = personal {
        out.push(Workspace {
            account_id,
            name: "Personal".to_string(),
            kind: "personal",
        });
    }

    // The same org selection `mcp::tools::tenancy::list_tenants` makes: live
    // memberships, not wholesale-revoked, excluding the caller's OWN personal org.
    let orgs: Vec<(Uuid, String)> = sqlx::query_as(
        "SELECT a.id, a.clerk_org_id \
         FROM account_members am \
         JOIN accounts a ON a.id = am.account_id \
         WHERE am.user_id = $1 \
           AND a.clerk_org_id IS NOT NULL \
           AND NOT EXISTS (SELECT 1 FROM org_revocations r WHERE r.clerk_org_id = a.clerk_org_id) \
           AND COALESCE(org_deletion_state_for_org(a.clerk_org_id), '') \
               NOT IN ('requested', 'grace', 'awaiting_approval', 'destroying') \
           AND NOT EXISTS ( \
               SELECT 1 FROM personal_orgs p \
               JOIN users u ON u.clerk_user_id = p.clerk_user_id \
               WHERE p.clerk_org_id = a.clerk_org_id AND u.id = am.user_id \
           ) \
         ORDER BY a.clerk_org_id \
         LIMIT $2",
    )
    .bind(user_id)
    .bind(WORKSPACE_ORG_CAP)
    .fetch_all(crate::tenancy::scope::request_read(pool))
    .await?;
    let org_ids: Vec<String> = orgs.iter().map(|(_, org)| org.clone()).collect();
    let names = org_names(pool, &org_ids).await;
    for ((account_id, _), name) in orgs.into_iter().zip(names) {
        out.push(Workspace {
            account_id,
            name,
            kind: "org",
        });
    }
    Ok(out)
}

// ── Session credential: resolution (used by `middleware::auth::require_auth`) ──

/// The identity a verified `csms_` bearer resolves to.
pub(crate) struct SessionIdentity {
    pub(crate) session_id: Uuid,
    pub(crate) user_id: Uuid,
    pub(crate) account_id: Uuid,
    pub(crate) tenant_id: String,
    pub(crate) object_id: String,
}

/// Stamped onto request extensions by `require_auth` precisely when the bearer
/// was a `csms_` credential — the marker [`session_scope`] keys on and the link
/// routes require. Absent for every Clerk / `csat_` request.
#[derive(Clone, Debug)]
pub(crate) struct AddinMicrosoftAuth {
    pub(crate) session_id: Uuid,
    pub(crate) tenant_id: String,
    pub(crate) object_id: String,
}

/// Which add-in an OAuth token belongs to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AddinOAuthClient {
    /// Outlook / Teams (`ADDIN_CLERK_OAUTH_CLIENT_ID`): [`scope_allows`] + link.
    Outlook,
    /// Gmail add-on (`GMAIL_ADDIN_CLERK_OAUTH_CLIENT_ID`): [`gmail_scope_allows`].
    Gmail,
    /// Google Meet side panel (`MEET_ADDIN_CLERK_OAUTH_CLIENT_ID`): [`meet_scope_allows`].
    Meet,
}

/// Stamped onto request extensions by `require_auth` precisely when the bearer
/// was a Clerk OAuth access token minted for an add-in's own client
/// (`ADDIN_CLERK_OAUTH_CLIENT_ID` / `GMAIL_ADDIN_CLERK_OAUTH_CLIENT_ID` /
/// `MEET_ADDIN_CLERK_OAUTH_CLIENT_ID`, no `sid`). Like [`AddinMicrosoftAuth`] it
/// puts the request under [`session_scope`]; unlike it, it additionally
/// unlocks `POST /v1/addin/microsoft/link`.
#[derive(Clone, Debug)]
pub(crate) struct AddinOAuthAuth {
    /// Which add-in's OAuth client minted the token; decides the route scope.
    pub(crate) client: AddinOAuthClient,
    /// The token's org claim named the caller's OWN personal organization.
    pub(crate) personal_org: bool,
}

/// Look up a `csms_` bearer by its SHA-256 hash. `Ok(None)` covers unknown,
/// revoked, EXPIRED, and "the link it came from is gone / disconnected /
/// re-pointed" identically — `require_auth` maps every `Ok(None)` to the same
/// 401 a bogus token gets. The link join is what makes a re-bind effective even
/// against a session row that somehow escaped revocation.
pub(crate) async fn resolve_session(
    pool: &PgPool,
    raw_token: &str,
) -> Result<Option<SessionIdentity>, sqlx::Error> {
    let row: Option<(Uuid, Uuid, Uuid, String, String)> = sqlx::query_as(
        "SELECT s.id, s.user_id, s.account_id, s.tenant_id, s.object_id \
         FROM addin_microsoft_sessions s \
         JOIN addin_microsoft_links l \
           ON l.tenant_id = s.tenant_id AND l.object_id = s.object_id \
          AND l.user_id = s.user_id AND l.account_id = s.account_id \
          AND l.disconnected_at IS NULL \
         WHERE s.token_hash = $1 AND s.revoked_at IS NULL AND s.expires_at > now()",
    )
    .bind(hash_token(raw_token))
    .fetch_optional(crate::tenancy::scope::request_read(pool))
    .await?;
    Ok(row.map(|(session_id, user_id, account_id, tenant_id, object_id)| SessionIdentity {
        session_id,
        user_id,
        account_id,
        tenant_id,
        object_id,
    }))
}

const LAST_USED_TOUCH_TIMEOUT: Duration = Duration::from_secs(2);

/// Best-effort `last_used_at` bump — fire-and-forget, never fails the request.
pub(crate) fn touch_last_used(pool: PgPool, session_id: Uuid) {
    tokio::spawn(async move {
        let outcome = tokio::time::timeout(
            LAST_USED_TOUCH_TIMEOUT,
            sqlx::query("UPDATE addin_microsoft_sessions SET last_used_at = now() WHERE id = $1")
                .bind(session_id)
                .execute(&pool),
        )
        .await;
        match outcome {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => tracing::warn!(error = %e, "addin_microsoft_sessions last_used_at touch failed"),
            Err(_) => tracing::warn!("addin_microsoft_sessions last_used_at touch timed out"),
        }
    });
}

// ── Session credential: minting / revocation ──────────────────────────────────

/// Mint and persist a session credential for `(tid, oid)` bound to
/// `(user_id, account_id)`. Returns the plaintext (once) and its expiry.
async fn mint_session(
    conn: &mut PgConnection,
    tenant_id: &str,
    object_id: &str,
    user_id: Uuid,
    account_id: Uuid,
) -> Result<(String, DateTime<Utc>), sqlx::Error> {
    let plaintext = generate_pat(SESSION_TOKEN_PREFIX);
    let expires_at = Utc::now() + chrono::Duration::minutes(SESSION_TTL_MINUTES);
    // Opportunistic, bounded cleanup of THIS identity's long-dead rows.
    sqlx::query(
        "DELETE FROM addin_microsoft_sessions \
         WHERE tenant_id = $1 AND object_id = $2 \
           AND (expires_at < now() - INTERVAL '1 day' OR revoked_at < now() - INTERVAL '1 day')",
    )
    .bind(tenant_id)
    .bind(object_id)
    .execute(&mut *conn)
    .await?;
    sqlx::query(
        "INSERT INTO addin_microsoft_sessions \
           (tenant_id, object_id, user_id, account_id, token_hash, expires_at) \
         VALUES ($1, $2, $3, $4, $5, $6)",
    )
    .bind(tenant_id)
    .bind(object_id)
    .bind(user_id)
    .bind(account_id)
    .bind(hash_token(&plaintext))
    .bind(expires_at)
    .execute(&mut *conn)
    .await?;
    Ok((plaintext, expires_at))
}

/// Revoke every live credential for a link — Teams and Outlook alike.
async fn revoke_link_sessions(
    conn: &mut PgConnection,
    tenant_id: &str,
    object_id: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE addin_microsoft_sessions SET revoked_at = now() \
         WHERE tenant_id = $1 AND object_id = $2 AND revoked_at IS NULL",
    )
    .bind(tenant_id)
    .bind(object_id)
    .execute(&mut *conn)
    .await?;
    Ok(())
}

// ── Response helpers ──────────────────────────────────────────────────────────

fn service_unavailable() -> Response {
    (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"error": "service unavailable"}))).into_response()
}

fn rate_limited() -> Response {
    (StatusCode::TOO_MANY_REQUESTS, Json(json!({"error": "rate limit exceeded"}))).into_response()
}

fn no_store(mut response: Response) -> Response {
    response
        .headers_mut()
        .insert(axum::http::header::CACHE_CONTROL, "no-store".parse().unwrap());
    response
}

fn scope_forbidden() -> Response {
    (
        StatusCode::FORBIDDEN,
        Json(json!({
            "error": "this add-in session cannot call this endpoint",
            "code": "addin_session_scope",
        })),
    )
        .into_response()
}

async fn account_email(pool: &PgPool, user_id: Uuid) -> Option<String> {
    sqlx::query_scalar("SELECT email FROM users WHERE id = $1")
        .bind(user_id)
        .fetch_optional(crate::tenancy::scope::request_read(pool))
        .await
        .ok()
        .flatten()
}

/// The `connected` body shared by `/session` and `PUT .../link/workspace`.
async fn connected_response(
    pool: &PgPool,
    user_id: Uuid,
    account_id: Uuid,
    access: &WorkspaceAccess,
    token: &str,
    expires_at: DateTime<Utc>,
    auto_linked: bool,
) -> Response {
    let workspace = workspace_for(pool, account_id, access).await;
    let email = account_email(pool, user_id).await;
    let mut body = json!({
        "status": "connected",
        "token": token,
        "expires_at": expires_at,
        "account": {"email": email},
        "workspace": workspace,
    });
    if auto_linked {
        body["auto_linked"] = json!(true);
    }
    no_store((StatusCode::OK, Json(body)).into_response())
}

// ── POST /v1/addin/microsoft/session (UNAUTHENTICATED) ────────────────────────

#[derive(Debug, Deserialize)]
pub struct SessionRequest {
    pub ms_token: String,
}

fn client_rate_key(headers: &HeaderMap) -> String {
    match crate::middleware::access_log::extract_client_ip(headers) {
        Some(ip) => format!("addin_ms_session_ip:{ip}"),
        None => SESSION_UNKNOWN_IP_KEY.to_string(),
    }
}

/// Exchange a Microsoft access token for a CoralSwarm session credential, or a
/// pairing to link the identity. See the module doc for the full state machine.
pub async fn session(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<SessionRequest>,
) -> Response {
    if !microsoft_sso::sso_enabled() {
        return (StatusCode::NOT_FOUND, Json(json!({"error": "microsoft_sso_disabled"}))).into_response();
    }
    let Some(pool) = state.control_db() else {
        return service_unavailable();
    };
    if !state
        .rate_limiter
        .check_and_increment_key(&client_rate_key(&headers), SESSION_IP_RATE_MAX, SESSION_IP_RATE_WINDOW_SECS)
        .await
    {
        return rate_limited();
    }

    let invalid = || {
        (StatusCode::UNAUTHORIZED, Json(json!({"error": "invalid_microsoft_token"}))).into_response()
    };
    if body.ms_token.is_empty() || body.ms_token.len() > MS_TOKEN_MAX_LEN {
        return invalid();
    }
    let cfg = MicrosoftSsoConfig::from_env();
    let identity = match microsoft_sso::verify(&body.ms_token, &cfg, &state.http_client).await {
        Ok(id) => id,
        Err(MicrosoftTokenError::KeysUnavailable(e)) => {
            tracing::error!(error = %e, "microsoft SSO: signing keys unavailable");
            return service_unavailable();
        }
        Err(e) => {
            tracing::info!(reason = ?e, "microsoft SSO: token refused");
            return invalid();
        }
    };

    if !state
        .rate_limiter
        .check_and_increment_key(
            &format!("addin_ms_session:{}:{}", identity.tenant_id, identity.object_id),
            SESSION_IDENTITY_RATE_MAX,
            SESSION_IDENTITY_RATE_WINDOW_SECS,
        )
        .await
    {
        return rate_limited();
    }

    match exchange(&state, pool, &identity).await {
        Ok(response) => response,
        Err(e) => {
            tracing::error!(error = %e, "microsoft SSO: session exchange failed");
            service_unavailable()
        }
    }
}

/// The link row for an identity, if any.
struct LinkRow {
    user_id: Uuid,
    account_id: Uuid,
    disconnected: bool,
}

async fn load_link(pool: &PgPool, id: &VerifiedMicrosoftIdentity) -> Result<Option<LinkRow>, sqlx::Error> {
    let row: Option<(Uuid, Uuid, Option<DateTime<Utc>>)> = sqlx::query_as(
        "SELECT user_id, account_id, disconnected_at FROM addin_microsoft_links \
         WHERE tenant_id = $1 AND object_id = $2",
    )
    .bind(&id.tenant_id)
    .bind(&id.object_id)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(user_id, account_id, disconnected_at)| LinkRow {
        user_id,
        account_id,
        disconnected: disconnected_at.is_some(),
    }))
}

async fn exchange(
    state: &AppState,
    pool: &PgPool,
    id: &VerifiedMicrosoftIdentity,
) -> Result<Response, sqlx::Error> {
    let mut allow_auto_link = true;

    if let Some(link) = load_link(pool, id).await? {
        if link.disconnected {
            // The user said Disconnect: never silently re-link.
            allow_auto_link = false;
        } else if let Some(access) = verify_workspace_access(pool, link.user_id, link.account_id).await? {
            let mut conn = pool.acquire().await?;
            let (token, expires_at) =
                mint_session(&mut conn, &id.tenant_id, &id.object_id, link.user_id, link.account_id).await?;
            drop(conn);
            return Ok(connected_response(pool, link.user_id, link.account_id, &access, &token, expires_at, false).await);
        } else {
            // Membership lost (or the account/user is gone): the link is dead.
            // Remove it (sessions cascade) and fall through to a fresh link.
            tracing::info!("microsoft SSO: linked workspace no longer accessible; removing link");
            sqlx::query(
                "DELETE FROM addin_microsoft_links WHERE tenant_id = $1 AND object_id = $2 \
                 AND user_id = $3 AND account_id = $4 AND disconnected_at IS NULL",
            )
            .bind(&id.tenant_id)
            .bind(&id.object_id)
            .bind(link.user_id)
            .bind(link.account_id)
            .execute(pool)
            .await?;
        }
    }

    if allow_auto_link {
        if let Some(response) = try_auto_link(pool, id).await? {
            return Ok(response);
        }
    }

    create_pairing(state, pool, id).await
}

/// AUTO-LINK: exactly one CoralSwarm user has connected this same Microsoft
/// account (and still has access to the workspace that connection lives in).
async fn try_auto_link(pool: &PgPool, id: &VerifiedMicrosoftIdentity) -> Result<Option<Response>, sqlx::Error> {
    let candidates: Vec<(Uuid, Option<Uuid>)> =
        sqlx::query_as("SELECT account_id, owner_user_id FROM resolve_microsoft_connected_users($1, $2)")
            .bind(&id.tenant_id)
            .bind(&id.object_id)
            .fetch_all(pool)
            .await?;
    let [(account_id, Some(user_id))] = candidates.as_slice() else {
        return Ok(None);
    };
    let (account_id, user_id) = (*account_id, *user_id);
    let Some(access) = verify_workspace_access(pool, user_id, account_id).await? else {
        return Ok(None);
    };

    let mut tx = pool.begin().await?;
    let inserted = sqlx::query(
        "INSERT INTO addin_microsoft_links (tenant_id, object_id, user_id, account_id, link_method) \
         VALUES ($1, $2, $3, $4, 'auto') ON CONFLICT (tenant_id, object_id) DO NOTHING",
    )
    .bind(&id.tenant_id)
    .bind(&id.object_id)
    .bind(user_id)
    .bind(account_id)
    .execute(&mut *tx)
    .await?
    .rows_affected();
    if inserted == 0 {
        // Lost a race with a concurrent exchange or approval (Teams and Outlook
        // opening together). A link now exists: serve the WINNING row — if it is
        // active and still accessible — instead of sending this pane to a
        // pairing it does not need. Only fall through when there is none.
        tx.rollback().await?;
        if let Some(link) = load_link(pool, id).await?.filter(|l| !l.disconnected) {
            if let Some(access) = verify_workspace_access(pool, link.user_id, link.account_id).await? {
                let mut conn = pool.acquire().await?;
                let (token, expires_at) =
                    mint_session(&mut conn, &id.tenant_id, &id.object_id, link.user_id, link.account_id).await?;
                drop(conn);
                return Ok(Some(
                    connected_response(pool, link.user_id, link.account_id, &access, &token, expires_at, false).await,
                ));
            }
        }
        return Ok(None);
    }
    let (token, expires_at) = mint_session(&mut tx, &id.tenant_id, &id.object_id, user_id, account_id).await?;
    tx.commit().await?;
    tracing::info!(account_id = %account_id, "microsoft SSO: auto-linked a previously connected Microsoft account");
    Ok(Some(connected_response(pool, user_id, account_id, &access, &token, expires_at, true).await))
}

/// `link_required`: a pairing BOUND to the verified `(tid, oid)`.
async fn create_pairing(
    state: &AppState,
    pool: &PgPool,
    id: &VerifiedMicrosoftIdentity,
) -> Result<Response, sqlx::Error> {
    if !state
        .rate_limiter
        .check_and_increment_key(
            &format!("addin_ms_pair:{}:{}", id.tenant_id, id.object_id),
            PAIRING_CREATE_RATE_MAX,
            PAIRING_CREATE_RATE_WINDOW_SECS,
        )
        .await
    {
        return Ok(rate_limited());
    }

    // Opportunistic sweep, same as `pair/start` (best-effort).
    if let Err(e) = gmail_addin::sweep_stale_pairing_rows(pool).await {
        tracing::warn!(error = %e, "microsoft SSO: opportunistic pairing sweep failed (non-fatal)");
    }

    let poll_secret = gmail_addin::generate_poll_secret();
    let poll_secret_hash = hash_token(&poll_secret);
    let expires_at = Utc::now() + chrono::Duration::minutes(gmail_addin::PAIRING_TTL_MINUTES);

    for _ in 0..gmail_addin::PAIRING_CODE_MAX_ATTEMPTS {
        let code = gmail_addin::generate_pairing_code();
        let mut tx = pool.begin().await?;
        // One live pairing per identity: a pane that re-opens gets a fresh code
        // and the old one stops working.
        sqlx::query(
            "DELETE FROM addin_pairing_codes \
             WHERE kind = 'microsoft' AND ms_tenant_id = $1 AND ms_object_id = $2",
        )
        .bind(&id.tenant_id)
        .bind(&id.object_id)
        .execute(&mut *tx)
        .await?;
        let inserted = sqlx::query(
            "INSERT INTO addin_pairing_codes \
               (code, poll_secret_hash, expires_at, kind, ms_tenant_id, ms_object_id) \
             VALUES ($1, $2, $3, 'microsoft', $4, $5)",
        )
        .bind(&code)
        .bind(&poll_secret_hash)
        .bind(expires_at)
        .bind(&id.tenant_id)
        .bind(&id.object_id)
        .execute(&mut *tx)
        .await;
        match inserted {
            Ok(_) => {
                tx.commit().await?;
                let approve_url = format!(
                    "{}/addin/approve?code={code}&kind=microsoft",
                    state.personal_web_base_url()
                );
                return Ok(no_store(
                    (
                        StatusCode::OK,
                        Json(json!({
                            "status": "link_required",
                            "pairing": {
                                "code": code,
                                "poll_secret": poll_secret,
                                "approve_url": approve_url,
                                "expires_at": expires_at,
                            },
                        })),
                    )
                        .into_response(),
                ));
            }
            // Code collision on the primary key: retry with a fresh code.
            Err(sqlx::Error::Database(e)) if e.code().as_deref() == Some("23505") => {
                let _ = tx.rollback().await;
                continue;
            }
            Err(e) => return Err(e),
        }
    }
    tracing::error!("microsoft SSO: exhausted retries generating a unique pairing code");
    Ok(service_unavailable())
}

// ── Pairing: approve (called by `routes::addin::pair_approve`) ────────────────

/// Approve a MICROSOFT pairing for workspace `account_id`, as the authenticated
/// Clerk user `user_id`. The identity written to the link is the one the
/// pairing row was bound to when `/session` created it — never a request field.
/// Membership of the chosen workspace is verified here, server-side.
pub(crate) async fn approve_pairing(
    state: &AppState,
    user_id: Uuid,
    code: &str,
    account_id: Uuid,
) -> Response {
    let Some(pool) = state.control_db() else {
        return service_unavailable();
    };
    if code.is_empty() {
        return gmail_addin::bad_request("code is required");
    }
    if !state
        .rate_limiter
        .check_and_increment_key(
            &format!("addin_approve:{code}"),
            gmail_addin::PAIR_CODE_RATE_MAX_PER_WINDOW,
            gmail_addin::PAIR_CODE_RATE_WINDOW_SECS,
        )
        .await
    {
        return rate_limited();
    }

    match approve_pairing_inner(pool, user_id, code, account_id).await {
        Ok(Ok(access)) => {
            let workspace = workspace_for(pool, account_id, &access).await;
            (StatusCode::OK, Json(json!({"approved": true, "workspace": workspace}))).into_response()
        }
        Ok(Err(response)) => response,
        Err(e) => {
            tracing::error!(error = %e, "addin pair/approve (microsoft): failed");
            service_unavailable()
        }
    }
}

async fn approve_pairing_inner(
    pool: &PgPool,
    user_id: Uuid,
    code: &str,
    account_id: Uuid,
) -> Result<Result<WorkspaceAccess, Response>, sqlx::Error> {
    let mut tx = pool.begin().await?;
    // `FOR UPDATE`: a double click / retry blocks here instead of racing the
    // `approved_at IS NULL` check.
    let row: Option<(DateTime<Utc>, Option<DateTime<Utc>>, String, Option<String>, Option<String>)> =
        sqlx::query_as(
            "SELECT expires_at, approved_at, kind, ms_tenant_id, ms_object_id \
             FROM addin_pairing_codes WHERE code = $1 FOR UPDATE",
        )
        .bind(code)
        .fetch_optional(&mut *tx)
        .await?;
    let Some((expires_at, approved_at, kind, tenant_id, object_id)) = row else {
        return Ok(Err(gmail_addin::not_found("pairing code not found or already used")));
    };
    if approved_at.is_some() {
        return Ok(Err(gmail_addin::conflict("this pairing code has already been approved")));
    }
    if Utc::now() > expires_at {
        sqlx::query("DELETE FROM addin_pairing_codes WHERE code = $1")
            .bind(code)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        return Ok(Err(gmail_addin::bad_request(
            "this pairing code has expired — restart pairing from the add-in",
        )));
    }
    // A Gmail pairing is personal-plane only and takes no workspace choice.
    let (Some(tenant_id), Some(object_id), "microsoft") = (tenant_id, object_id, kind.as_str()) else {
        return Ok(Err(gmail_addin::bad_request(
            "account_id is only accepted for Microsoft add-in pairings",
        )));
    };

    let Some(access) = verify_workspace_access(pool, user_id, account_id).await? else {
        return Ok(Err((
            StatusCode::FORBIDDEN,
            Json(json!({"error": "you are not a member of that workspace"})),
        )
            .into_response()));
    };

    // Create or REPLACE the link. Clearing `disconnected_at` is deliberate: an
    // explicit human approval is exactly what lifts a Disconnect tombstone.
    sqlx::query(
        "INSERT INTO addin_microsoft_links (tenant_id, object_id, user_id, account_id, link_method) \
         VALUES ($1, $2, $3, $4, 'pairing') \
         ON CONFLICT (tenant_id, object_id) DO UPDATE SET \
           user_id = EXCLUDED.user_id, account_id = EXCLUDED.account_id, \
           link_method = 'pairing', disconnected_at = NULL, updated_at = now()",
    )
    .bind(&tenant_id)
    .bind(&object_id)
    .bind(user_id)
    .bind(account_id)
    .execute(&mut *tx)
    .await?;
    // A replaced link must not leave credentials for the previous binding alive.
    revoke_link_sessions(&mut tx, &tenant_id, &object_id).await?;
    sqlx::query(
        "UPDATE addin_pairing_codes SET account_id = $2, owner_user_id = $3, approved_at = now() \
         WHERE code = $1",
    )
    .bind(code)
    .bind(account_id)
    .bind(user_id)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(Ok(access))
}

// ── GET /v1/addin/pair/info (Clerk-authenticated) ─────────────────────────────

#[derive(Debug, Deserialize)]
pub struct PairInfoQuery {
    pub code: String,
}

/// What the web approve page needs to render: the pairing's `kind` and the
/// workspaces the caller may pick from. A Gmail pairing offers only the
/// personal workspace (Gmail is personal-plane only, unchanged). An unknown,
/// expired or already-approved code is one indistinguishable 404.
pub async fn pair_info(
    State(state): State<Arc<AppState>>,
    Extension(user_ctx): Extension<UserContext>,
    axum::extract::Query(q): axum::extract::Query<PairInfoQuery>,
) -> Response {
    let Some(pool) = state.control_db() else {
        return service_unavailable();
    };
    let code = q.code.trim();
    if code.is_empty() {
        return gmail_addin::bad_request("code is required");
    }
    if !state
        .rate_limiter
        .check_and_increment_key(
            &format!("addin_info:{code}"),
            gmail_addin::PAIR_CODE_RATE_MAX_PER_WINDOW,
            gmail_addin::PAIR_CODE_RATE_WINDOW_SECS,
        )
        .await
    {
        return rate_limited();
    }
    let row: Result<Option<String>, sqlx::Error> = sqlx::query_scalar(
        "SELECT kind FROM addin_pairing_codes \
         WHERE code = $1 AND approved_at IS NULL AND expires_at > now()",
    )
    .bind(code)
    .fetch_optional(crate::tenancy::scope::request_read(pool))
    .await;
    let kind = match row {
        Ok(Some(kind)) => kind,
        Ok(None) => return gmail_addin::not_found("pairing code not found or already used"),
        Err(e) => {
            tracing::error!(error = %e, "addin pair/info: lookup failed");
            return service_unavailable();
        }
    };
    let mut workspaces = match list_workspaces(pool, user_ctx.user_id).await {
        Ok(w) => w,
        Err(e) => {
            tracing::error!(error = %e, "addin pair/info: workspace list failed");
            return service_unavailable();
        }
    };
    if kind != "microsoft" {
        workspaces.retain(|w| w.kind == "personal");
    }
    (StatusCode::OK, Json(json!({"kind": kind, "workspaces": workspaces}))).into_response()
}

// ── Link routes (authenticated by the `csms_` credential) ─────────────────────

/// `GET /v1/addin/microsoft/link`.
pub async fn get_link(
    State(state): State<Arc<AppState>>,
    Extension(user_ctx): Extension<UserContext>,
    auth: Option<Extension<AddinMicrosoftAuth>>,
) -> Response {
    if auth.is_none() {
        return scope_forbidden();
    }
    let Some(pool) = state.control_db() else {
        return service_unavailable();
    };
    // `require_auth` just verified this access; resolved again only to learn the kind.
    let access = match verify_workspace_access(pool, user_ctx.user_id, user_ctx.account_id).await {
        Ok(Some(a)) => a,
        Ok(None) => return (StatusCode::UNAUTHORIZED, Json(json!({"error": "invalid or expired token"}))).into_response(),
        Err(e) => {
            tracing::error!(error = %e, "addin microsoft link: access check failed");
            return service_unavailable();
        }
    };
    let workspace = workspace_for(pool, user_ctx.account_id, &access).await;
    let workspaces = match list_workspaces(pool, user_ctx.user_id).await {
        Ok(w) => w,
        Err(e) => {
            tracing::error!(error = %e, "addin microsoft link: workspace list failed");
            return service_unavailable();
        }
    };
    let email = account_email(pool, user_ctx.user_id).await;
    no_store(
        (
            StatusCode::OK,
            Json(json!({
                "account": {"email": email},
                "workspace": workspace,
                "workspaces": workspaces,
            })),
        )
            .into_response(),
    )
}

// ── GET /v1/addin/workspace (any add-in credential, or a Clerk session) ───────

/// `GET /v1/addin/workspace` — which workspace this request acts in, with a
/// display name. The Clerk access token only carries `org_id`, so the add-ins
/// (and the web app) cannot label it themselves.
///
/// Resolved from the request's `UserContext.account_id` — i.e. AFTER
/// `require_auth` verified membership and translated a personal-org claim to the
/// caller's personal account — never from anything the client sends. A personal
/// account (`accounts.clerk_org_id IS NULL`, which is also what a personal-org
/// claim resolves to, or the caller's own personal-org account) is `{"kind":"personal","name":"Personal","org_id":null}`;
/// an org is named by the same [`org_names`] lookup the workspace picker uses.
pub async fn get_workspace(State(state): State<Arc<AppState>>, Extension(user_ctx): Extension<UserContext>) -> Response {
    let Some(pool) = state.control_db() else {
        return service_unavailable();
    };
    let org_id: Result<Option<Option<String>>, sqlx::Error> =
        sqlx::query_scalar("SELECT clerk_org_id FROM accounts WHERE id = $1")
            .bind(user_ctx.account_id)
            .fetch_optional(crate::tenancy::scope::request_read(pool))
            .await;
    let mut org_id = match org_id {
        Ok(Some(o)) => o,
        Ok(None) => {
            return (StatusCode::UNAUTHORIZED, Json(json!({"error": "invalid or expired token"}))).into_response()
        }
        Err(e) => {
            tracing::error!(error = %e, "addin workspace: account lookup failed");
            return service_unavailable();
        }
    };
    // The caller's OWN personal organization is the personal workspace under
    // another label: a token with no org claim can resolve to that
    // org-labelled account (`personal_account::personal_account_id` prefers it
    // when it exists), and `verify_workspace_access` never offers it as an org.
    // Report it as personal, exactly as the workspace picker does.
    if let Some(org) = &org_id {
        let own_personal_org: Result<bool, sqlx::Error> = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM personal_orgs p \
               JOIN users u ON u.clerk_user_id = p.clerk_user_id \
              WHERE p.clerk_org_id = $1 AND u.id = $2)",
        )
        .bind(org)
        .bind(user_ctx.user_id)
        .fetch_one(crate::tenancy::scope::request_read(pool))
        .await;
        match own_personal_org {
            Ok(true) => org_id = None,
            Ok(false) => {}
            Err(e) => {
                tracing::error!(error = %e, "addin workspace: personal-org check failed");
                return service_unavailable();
            }
        }
    }
    let (kind, name) = match &org_id {
        None => ("personal", "Personal".to_string()),
        Some(org) => (
            "org",
            org_names(pool, std::slice::from_ref(org))
                .await
                .into_iter()
                .next()
                .unwrap_or_else(|| "Organization".to_string()),
        ),
    };
    no_store(
        (
            StatusCode::OK,
            Json(json!({
                "kind": kind,
                "name": name,
                "account_id": user_ctx.account_id,
                "org_id": org_id,
            })),
        )
            .into_response(),
    )
}

// ── POST /v1/addin/microsoft/link (authenticated by an add-in Clerk OAuth token) ──

#[derive(Debug, Deserialize)]
pub struct LinkRequest {
    pub ms_token: String,
}

/// Bind the caller's Microsoft identity to the workspace their Clerk OAuth
/// token names, and mint a `csms_` session so later loads are silent through
/// `/session`. The Microsoft identity comes only from the VERIFIED `ms_token`;
/// the user and workspace come only from `UserContext` (verified token claims
/// plus a live membership check) - never from the request body.
pub async fn link(
    State(state): State<Arc<AppState>>,
    Extension(user_ctx): Extension<UserContext>,
    oauth: Option<Extension<AddinOAuthAuth>>,
    Json(body): Json<LinkRequest>,
) -> Response {
    // Outlook/Teams client only: a Gmail add-on token must never link Microsoft.
    let Some(Extension(oauth)) = oauth.filter(|o| o.0.client == AddinOAuthClient::Outlook) else {
        return scope_forbidden();
    };
    if !microsoft_sso::sso_enabled() {
        return (StatusCode::NOT_FOUND, Json(json!({"error": "microsoft_sso_disabled"}))).into_response();
    }
    let Some(pool) = state.control_db() else {
        return service_unavailable();
    };
    if !state
        .rate_limiter
        .check_and_increment_key(
            &format!("addin_ms_link:{}", user_ctx.user_id),
            LINK_MUTATION_RATE_MAX,
            LINK_MUTATION_RATE_WINDOW_SECS,
        )
        .await
    {
        return rate_limited();
    }

    let invalid = || {
        (StatusCode::UNAUTHORIZED, Json(json!({"error": "invalid_microsoft_token"}))).into_response()
    };
    if body.ms_token.is_empty() || body.ms_token.len() > MS_TOKEN_MAX_LEN {
        return invalid();
    }
    let cfg = MicrosoftSsoConfig::from_env();
    let identity = match microsoft_sso::verify(&body.ms_token, &cfg, &state.http_client).await {
        Ok(id) => id,
        Err(MicrosoftTokenError::KeysUnavailable(e)) => {
            tracing::error!(error = %e, "microsoft SSO link: signing keys unavailable");
            return service_unavailable();
        }
        Err(e) => {
            tracing::info!(reason = ?e, "microsoft SSO link: token refused");
            return invalid();
        }
    };

    // The workspace: the account `require_auth` resolved from the token's org
    // claim (live membership already enforced). A personal-ORGANIZATION claim
    // resolves to the org's own accounts row, which a link must never name
    // (`verify_workspace_access` refuses it, `list_workspaces` hides it): the
    // user's plain personal account stands for it, as it does on the csms_ path.
    let account_id = if oauth.personal_org {
        match sqlx::query_scalar::<_, Uuid>(
            "SELECT id FROM accounts WHERE owner_user_id = $1 AND clerk_org_id IS NULL \
             ORDER BY created_at LIMIT 1",
        )
        .bind(user_ctx.user_id)
        .fetch_optional(pool)
        .await
        {
            Ok(Some(id)) => id,
            Ok(None) => {
                return (
                    StatusCode::FORBIDDEN,
                    Json(json!({"error": "no personal workspace is available for this account"})),
                )
                    .into_response()
            }
            Err(e) => {
                tracing::error!(error = %e, "addin microsoft link: personal account lookup failed");
                return service_unavailable();
            }
        }
    } else {
        user_ctx.account_id
    };
    let access = match verify_workspace_access(pool, user_ctx.user_id, account_id).await {
        Ok(Some(a)) => a,
        Ok(None) => {
            return (
                StatusCode::FORBIDDEN,
                Json(json!({"error": "you are not a member of that workspace"})),
            )
                .into_response()
        }
        Err(e) => {
            tracing::error!(error = %e, "addin microsoft link: access check failed");
            return service_unavailable();
        }
    };

    let result: Result<(String, DateTime<Utc>), sqlx::Error> = async {
        let mut tx = pool.begin().await?;
        // Lock the existing row (if any) so "re-pointed" is decided against a
        // stable value.
        let previous: Option<(Uuid, Uuid)> = sqlx::query_as(
            "SELECT user_id, account_id FROM addin_microsoft_links \
             WHERE tenant_id = $1 AND object_id = $2 FOR UPDATE",
        )
        .bind(&identity.tenant_id)
        .bind(&identity.object_id)
        .fetch_optional(&mut *tx)
        .await?;
        // Upsert. Clearing `disconnected_at` is deliberate: the user just
        // signed in explicitly, which is what lifts a Disconnect tombstone.
        sqlx::query(
            "INSERT INTO addin_microsoft_links (tenant_id, object_id, user_id, account_id, link_method) \
             VALUES ($1, $2, $3, $4, 'clerk_oauth') \
             ON CONFLICT (tenant_id, object_id) DO UPDATE SET \
               user_id = EXCLUDED.user_id, account_id = EXCLUDED.account_id, \
               link_method = 'clerk_oauth', disconnected_at = NULL, updated_at = now()",
        )
        .bind(&identity.tenant_id)
        .bind(&identity.object_id)
        .bind(user_ctx.user_id)
        .bind(account_id)
        .execute(&mut *tx)
        .await?;
        if previous.is_some_and(|prev| prev != (user_ctx.user_id, account_id)) {
            // A re-pointed link must not leave credentials for the previous
            // binding alive (Teams and Outlook alike).
            revoke_link_sessions(&mut tx, &identity.tenant_id, &identity.object_id).await?;
        }
        let minted =
            mint_session(&mut tx, &identity.tenant_id, &identity.object_id, user_ctx.user_id, account_id).await?;
        tx.commit().await?;
        Ok(minted)
    }
    .await;
    match result {
        Ok((token, expires_at)) => {
            tracing::info!(account_id = %account_id, "microsoft add-in linked via Clerk OAuth");
            connected_response(pool, user_ctx.user_id, account_id, &access, &token, expires_at, false).await
        }
        Err(e) => {
            tracing::error!(error = %e, "addin microsoft link: failed");
            service_unavailable()
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct ChangeWorkspaceRequest {
    pub account_id: Uuid,
}

/// `PUT /v1/addin/microsoft/link/workspace` — "Change workspace". Re-binds the
/// link (Teams and Outlook together), revokes every old credential, and returns
/// the same `connected` body as `/session` with a NEW token.
pub async fn change_workspace(
    State(state): State<Arc<AppState>>,
    Extension(user_ctx): Extension<UserContext>,
    auth: Option<Extension<AddinMicrosoftAuth>>,
    Json(body): Json<ChangeWorkspaceRequest>,
) -> Response {
    let Some(Extension(auth)) = auth else {
        return scope_forbidden();
    };
    let Some(pool) = state.control_db() else {
        return service_unavailable();
    };
    if !state
        .rate_limiter
        .check_and_increment_key(
            &format!("addin_ms_link:{}", user_ctx.user_id),
            LINK_MUTATION_RATE_MAX,
            LINK_MUTATION_RATE_WINDOW_SECS,
        )
        .await
    {
        return rate_limited();
    }

    // The link's user is the session's user (the resolver joined on it); the
    // picked workspace must be one THAT user may act in.
    let access = match verify_workspace_access(pool, user_ctx.user_id, body.account_id).await {
        Ok(Some(a)) => a,
        Ok(None) => {
            return (
                StatusCode::FORBIDDEN,
                Json(json!({"error": "you are not a member of that workspace"})),
            )
                .into_response()
        }
        Err(e) => {
            tracing::error!(error = %e, "addin microsoft change workspace: access check failed");
            return service_unavailable();
        }
    };

    let result: Result<Option<(String, DateTime<Utc>)>, sqlx::Error> = async {
        let mut tx = pool.begin().await?;
        let updated = sqlx::query(
            "UPDATE addin_microsoft_links SET account_id = $3, updated_at = now() \
             WHERE tenant_id = $1 AND object_id = $2 AND user_id = $4 AND disconnected_at IS NULL",
        )
        .bind(&auth.tenant_id)
        .bind(&auth.object_id)
        .bind(body.account_id)
        .bind(user_ctx.user_id)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        if updated == 0 {
            tx.rollback().await?;
            return Ok(None);
        }
        revoke_link_sessions(&mut tx, &auth.tenant_id, &auth.object_id).await?;
        let minted =
            mint_session(&mut tx, &auth.tenant_id, &auth.object_id, user_ctx.user_id, body.account_id).await?;
        tx.commit().await?;
        Ok(Some(minted))
    }
    .await;
    match result {
        Ok(Some((token, expires_at))) => {
            connected_response(pool, user_ctx.user_id, body.account_id, &access, &token, expires_at, false).await
        }
        Ok(None) => (StatusCode::UNAUTHORIZED, Json(json!({"error": "invalid or expired token"}))).into_response(),
        Err(e) => {
            tracing::error!(error = %e, "addin microsoft change workspace: failed");
            service_unavailable()
        }
    }
}

/// `DELETE /v1/addin/microsoft/link` — "Disconnect". Marks the link
/// disconnected (so it is not silently auto-recreated) and revokes every
/// credential for it, for Teams and Outlook alike. `204`.
pub async fn disconnect_link(
    State(state): State<Arc<AppState>>,
    Extension(user_ctx): Extension<UserContext>,
    auth: Option<Extension<AddinMicrosoftAuth>>,
) -> Response {
    let Some(Extension(auth)) = auth else {
        return scope_forbidden();
    };
    let Some(pool) = state.control_db() else {
        return service_unavailable();
    };
    let result: Result<(), sqlx::Error> = async {
        let mut tx = pool.begin().await?;
        sqlx::query(
            "UPDATE addin_microsoft_links SET disconnected_at = now(), updated_at = now() \
             WHERE tenant_id = $1 AND object_id = $2 AND user_id = $3",
        )
        .bind(&auth.tenant_id)
        .bind(&auth.object_id)
        .bind(user_ctx.user_id)
        .execute(&mut *tx)
        .await?;
        revoke_link_sessions(&mut tx, &auth.tenant_id, &auth.object_id).await?;
        tx.commit().await
    }
    .await;
    match result {
        Ok(()) => {
            tracing::info!(session_id = %auth.session_id, "microsoft add-in link disconnected; all credentials revoked");
            StatusCode::NO_CONTENT.into_response()
        }
        Err(e) => {
            tracing::error!(error = %e, "addin microsoft disconnect: failed");
            service_unavailable()
        }
    }
}

// ── Scope allowlist, layered on `/v1` ─────────────────────────────────────────

/// The `/v1`-relative paths (`Router::nest` has already stripped `/v1`) a
/// `csms_` credential may call. See the module doc's "Scope" section.
pub(crate) fn scope_allows(method: &Method, path: &str) -> bool {
    if shared_scope_allows(method, path) {
        return true;
    }
    // Link management.
    match (method, path) {
        (&Method::GET, "/addin/microsoft/link") | (&Method::DELETE, "/addin/microsoft/link") => return true,
        (&Method::PUT, "/addin/microsoft/link/workspace") => return true,
        _ => {}
    }
    // Outlook email save: save one message, read/toggle its follow state.
    if method == Method::POST && path == "/connectors/microsoft/ingest" {
        return true;
    }
    // Whole-mailbox switch (`routes::outlook_everything_mode`). Safe for a
    // session credential in BOTH link contexts: the handlers take no target
    // user (acts on `UserContext.user_id` only, with an actor==target check),
    // write into that member's own private ocean, and refuse an org context
    // unless the org's Microsoft connector flag is on.
    if path == "/connectors/microsoft/everything-mode" && matches!(*method, Method::GET | Method::POST) {
        return true;
    }
    if path == "/connectors/microsoft/watched" && method == Method::GET {
        return true;
    }
    if let Some(rest) = path.strip_prefix("/connectors/microsoft/watched/") {
        return !rest.is_empty()
            && !rest.contains('/')
            && matches!(*method, Method::GET | Method::PUT | Method::DELETE);
    }
    // Meeting capture (Teams + Outlook panes). NOT `transcripts/sync`
    // (the whole-calendar sweep), NOT channel/chat ingest.
    const MEETING_BASE: &str = "/connectors/microsoft/teams/transcripts";
    match (method, path.strip_prefix(MEETING_BASE)) {
        (&Method::POST, Some("/connect" | "/meeting-status" | "/meeting-context" | "/sync-meeting")) => true,
        (&Method::POST | &Method::DELETE, Some("/capture")) => true,
        (&Method::GET, Some("/captures")) => true,
        _ => outlook_schedule_allows(method, path),
    }
}

/// Routes every add-in credential (`csms_`, Outlook OAuth, Gmail OAuth, Meet
/// OAuth) may call, in addition to its own surface. The ONLY routes
/// [`scope_allows`] shares with [`gmail_scope_allows`] or [`meet_scope_allows`];
/// the disjointness tests exempt exactly these.
pub(crate) fn shared_scope_allows(method: &Method, path: &str) -> bool {
    // Workspace label for the add-in header (`get_workspace`).
    method == Method::GET && path == "/addin/workspace"
}

/// Outlook compose capture v2 (`routes::outlook_schedule`): create / read / stop an
/// intent and look one up by nonce or event. Exact shapes only.
fn outlook_schedule_allows(method: &Method, path: &str) -> bool {
    const BASE: &str = "/outlook-capture-schedule";
    let Some(rest) = path.strip_prefix(BASE) else {
        return false;
    };
    let single = |tail: &str| !tail.is_empty() && !tail.contains('/');
    match (method, rest) {
        (&Method::POST, "/intents") => true,
        (&Method::GET, "/by-event") => true,
        (&Method::GET | &Method::DELETE, _) if rest.strip_prefix("/intents/").is_some_and(single) => {
            true
        }
        (&Method::GET, _) => rest.strip_prefix("/by-nonce/").is_some_and(single),
        _ => false,
    }
}

/// Middleware: for a request `require_auth` authenticated with a `csms_`
/// credential (marked by [`AddinMicrosoftAuth`]) or an add-in Clerk OAuth token
/// (marked by [`AddinOAuthAuth`]), allow only [`scope_allows`].
/// Every other request is untouched.
pub async fn session_scope(req: Request, next: Next) -> Response {
    let oauth = req.extensions().get::<AddinOAuthAuth>().map(|o| o.client);
    if oauth.is_none() && req.extensions().get::<AddinMicrosoftAuth>().is_none() {
        return next.run(req).await;
    }
    let (method, path) = (req.method(), req.uri().path());
    let allowed = match oauth {
        Some(AddinOAuthClient::Gmail) => gmail_scope_allows(method, path),
        Some(AddinOAuthClient::Meet) => meet_scope_allows(method, path),
        // The one extra route an Outlook/Teams OAuth token may call: linking.
        Some(AddinOAuthClient::Outlook) => {
            scope_allows(method, path) || (method == Method::POST && path == "/addin/microsoft/link")
        }
        None => scope_allows(method, path),
    };
    if allowed {
        return next.run(req).await;
    }
    scope_forbidden()
}

/// `/v1`-relative routes a Gmail add-on OAuth token may call.
///
/// The Gmail add-on's `csat_` PAT has NO route allowlist of its own: it is
/// confined to the personal plane by construction (`require_auth` gives it no
/// org claim) and otherwise reaches every personal REST route. There is
/// nothing to reuse, so this is the set of routes `extensions/gmail-addin`
/// actually calls (Ingest.gs, Card.gs, Settings.gs): save a thread, read/set
/// its follow state, read/set whole-mailbox mode, and approve / look up / undo a
/// Google Meet capture from the Calendar card. `DELETE /addin/tokens/{id}`
/// (Disconnect of a csat_) has no OAuth analogue. No overlap with
/// [`scope_allows`] apart from [`shared_scope_allows`]: that set is Microsoft-only,
/// this one Google-only.
pub(crate) fn gmail_scope_allows(method: &Method, path: &str) -> bool {
    if shared_scope_allows(method, path) {
        return true;
    }
    if method == Method::POST && path == "/connectors/google/ingest" {
        return true;
    }
    // Google Meet per-meeting pre-approval (`routes::google_meet`), called by the
    // add-on's Google Calendar card (`extensions/gmail-addin/Calendar.gs`):
    // approve (`POST`), look up (`GET`), undo (`DELETE .../{capture_id}`). Exact
    // shapes only. The by-conference-record routes (`.../meetings`,
    // `.../meetings/{id}/allow`) are for the Meet side panel and stay NOT allowed
    // here. Gmail-client-only: the Outlook/Teams client approves Teams meetings via
    // `/connectors/microsoft/teams/transcripts/*` and has no Google Meet surface.
    if path.starts_with(MEET_CAPTURES) {
        return meet_captures_allows(method, path);
    }
    if path == "/connectors/google/gmail/everything-mode" {
        return matches!(*method, Method::GET | Method::POST);
    }
    if let Some(rest) = path.strip_prefix("/connectors/google/watched/") {
        return !rest.is_empty() && !rest.contains('/') && matches!(*method, Method::GET | Method::PUT);
    }
    false
}

const MEET_CAPTURES: &str = "/connectors/google/meet/captures";

/// The Google Meet per-meeting pre-approval routes (`routes::google_meet`):
/// approve (`POST`), look up (`GET`), undo (`DELETE .../{capture_id}`). Exact
/// shapes only. Shared by the Gmail add-on's Calendar card and the Meet side
/// panel; the by-conference-record routes (`.../meetings`, `.../meetings/{id}/allow`)
/// are reachable by neither.
fn meet_captures_allows(method: &Method, path: &str) -> bool {
    let Some(rest) = path.strip_prefix(MEET_CAPTURES) else {
        return false;
    };
    match rest {
        "" => matches!(*method, Method::POST | Method::GET),
        _ => method == Method::DELETE && rest.strip_prefix('/').is_some_and(|id| !id.is_empty() && !id.contains('/')),
    }
}

/// `/v1`-relative routes a Google Meet side-panel OAuth token may call: the
/// shared workspace label plus the Meet pre-approval routes the panel
/// (`web/app/meet/sidepanel`) uses to add, look up and undo "save this meeting".
/// Nothing else: no Gmail ingest or mailbox switch, no Microsoft surface, no link.
/// Transcript on/off state is not read here; the panel shows a static hint.
pub(crate) fn meet_scope_allows(method: &Method, path: &str) -> bool {
    shared_scope_allows(method, path) || meet_captures_allows(method, path)
}

// ── Unit tests (no DB) ────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    #[test]
    fn session_prefix_is_distinct_from_the_other_bearer_shapes() {
        assert_ne!(SESSION_TOKEN_PREFIX, crate::routes::addin::ADDIN_TOKEN_PREFIX);
        // No prefix may be a prefix of another, or `starts_with` routing in
        // `require_auth` becomes order-dependent.
        let other = crate::routes::addin::ADDIN_TOKEN_PREFIX;
        assert!(!SESSION_TOKEN_PREFIX.starts_with(other) && !other.starts_with(SESSION_TOKEN_PREFIX));
    }

    #[test]
    fn scope_allows_the_documented_surface() {
        use Method as M;
        for (m, p) in [
            (M::GET, "/addin/workspace"),
            (M::GET, "/addin/microsoft/link"),
            (M::DELETE, "/addin/microsoft/link"),
            (M::PUT, "/addin/microsoft/link/workspace"),
            (M::POST, "/connectors/microsoft/ingest"),
            (M::GET, "/connectors/microsoft/everything-mode"),
            (M::POST, "/connectors/microsoft/everything-mode"),
            (M::GET, "/connectors/microsoft/watched"),
            (M::GET, "/connectors/microsoft/watched/AAMk123"),
            (M::PUT, "/connectors/microsoft/watched/AAMk123"),
            (M::DELETE, "/connectors/microsoft/watched/AAMk123"),
            (M::POST, "/connectors/microsoft/teams/transcripts/connect"),
            (M::POST, "/connectors/microsoft/teams/transcripts/meeting-status"),
            (M::POST, "/connectors/microsoft/teams/transcripts/meeting-context"),
            (M::POST, "/connectors/microsoft/teams/transcripts/sync-meeting"),
            (M::POST, "/connectors/microsoft/teams/transcripts/capture"),
            (M::DELETE, "/connectors/microsoft/teams/transcripts/capture"),
            (M::GET, "/connectors/microsoft/teams/transcripts/captures"),
            (M::POST, "/outlook-capture-schedule/intents"),
            (M::GET, "/outlook-capture-schedule/intents/11111111-1111-1111-1111-111111111111"),
            (M::DELETE, "/outlook-capture-schedule/intents/11111111-1111-1111-1111-111111111111"),
            (M::GET, "/outlook-capture-schedule/by-nonce/11111111-1111-1111-1111-111111111111"),
            (M::GET, "/outlook-capture-schedule/by-event"),
        ] {
            assert!(scope_allows(&m, p), "{m} {p} must be reachable by an add-in session");
        }
    }

    #[test]
    fn scope_refuses_everything_else() {
        use Method as M;
        for (m, p) in [
            // Privilege-escalating or out-of-pane Microsoft surface.
            (M::PUT, "/connectors/microsoft/everything-mode"),
            (M::DELETE, "/connectors/microsoft/everything-mode"),
            (M::POST, "/connectors/google/gmail/everything-mode"),
            (M::GET, "/connectors/google/gmail/everything-mode"),
            (M::POST, "/connectors/microsoft/workflows"),
            (M::POST, "/connectors/microsoft/teams/ingest"),
            (M::POST, "/connectors/microsoft/teams/transcripts/sync"),
            (M::POST, "/connectors/microsoft/teams/onboarding"),
            (M::DELETE, "/connectors/microsoft/teams/watched/1"),
            (M::GET, "/connectors/microsoft/watched/a/b"),
            // Wrong verb on an allowed path.
            (M::DELETE, "/connectors/microsoft/ingest"),
            (M::GET, "/connectors/microsoft/ingest"),
            (M::POST, "/addin/microsoft/link"),
            (M::PUT, "/addin/microsoft/link"),
            (M::POST, "/addin/workspace"),
            (M::PUT, "/addin/workspace"),
            (M::DELETE, "/addin/workspace"),
            (M::GET, "/addin/workspace/x"),
            (M::GET, "/addin/workspaces"),
            (M::GET, "/connectors/microsoft/teams/transcripts/capture"),
            // Credential minting / account surface.
            (M::POST, "/addin/pair/approve"),
            (M::GET, "/addin/tokens"),
            (M::GET, "/me"),
            (M::POST, "/ingest/atom"),
            (M::GET, "/graphs"),
            // Prefix tricks.
            (M::POST, "/connectors/microsoft/teams/transcripts/capture/../sync"),
            (M::POST, "/connectors/microsoft/ingest/x"),
            (M::GET, "/connectors/microsoft/watchedx"),
            (M::GET, "/connectors/microsoft/watched/"),
            // Outlook scheduling: exact shapes only.
            (M::GET, "/outlook-capture-schedule/intents"),
            (M::PUT, "/outlook-capture-schedule/intents/x"),
            (M::DELETE, "/outlook-capture-schedule/intents"),
            (M::GET, "/outlook-capture-schedule/intents/x/y"),
            (M::POST, "/outlook-capture-schedule/intents/x"),
            (M::DELETE, "/outlook-capture-schedule/by-nonce/x"),
            (M::GET, "/outlook-capture-schedule/by-nonce/"),
            (M::GET, "/outlook-capture-scheduler/intents/x"),
        ] {
            assert!(!scope_allows(&m, p), "{m} {p} must NOT be reachable by an add-in session");
        }
    }

    /// The scope layer leaves every non-session request alone and 403s a
    /// session-marked one off the allowlist — exercised through a real router so
    /// the marker-driven branch (not just the predicate) is covered.
    #[tokio::test]
    async fn session_scope_middleware_only_restricts_marked_requests() {
        use axum::{body::Body, routing::get, Router};
        use tower::ServiceExt;

        let router = || {
            Router::new()
                .route("/me", get(|| async { StatusCode::OK }))
                .route("/addin/microsoft/link", get(|| async { StatusCode::OK }))
                .layer(axum::middleware::from_fn(session_scope))
        };
        let marked = |path: &str| {
            let mut req = axum::http::Request::builder().uri(path).body(Body::empty()).unwrap();
            req.extensions_mut().insert(AddinMicrosoftAuth {
                session_id: Uuid::nil(),
                tenant_id: "t".into(),
                object_id: "o".into(),
            });
            req
        };
        let plain = |path: &str| axum::http::Request::builder().uri(path).body(Body::empty()).unwrap();

        // Unmarked (Clerk / PAT) request: untouched, even off the allowlist.
        assert_eq!(router().oneshot(plain("/me")).await.unwrap().status(), StatusCode::OK);
        // Marked, off the allowlist: the frozen scope body.
        let resp = router().oneshot(marked("/me")).await.unwrap();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
        let body: Value =
            serde_json::from_slice(&axum::body::to_bytes(resp.into_body(), 4096).await.unwrap()).unwrap();
        assert_eq!(body["code"], "addin_session_scope");
        // Marked, on the allowlist: through.
        assert_eq!(
            router().oneshot(marked("/addin/microsoft/link")).await.unwrap().status(),
            StatusCode::OK
        );
    }

    /// An add-in OAuth marker is scoped exactly like a `csms_` one, plus the one
    /// extra route (`POST /addin/microsoft/link`) that a `csms_` may NOT call.
    #[tokio::test]
    async fn session_scope_applies_to_addin_oauth_and_adds_only_the_link_post() {
        use axum::{body::Body, routing::{get, post}, Router};
        use tower::ServiceExt;

        let router = || {
            Router::new()
                .route("/me", get(|| async { StatusCode::OK }))
                .route("/addin/microsoft/link", get(|| async { StatusCode::OK }).post(|| async { StatusCode::OK }))
                .route("/connectors/microsoft/ingest", post(|| async { StatusCode::OK }))
                .layer(axum::middleware::from_fn(session_scope))
        };
        let oauth = |method: &str, path: &str| {
            let mut req = axum::http::Request::builder().method(method).uri(path).body(Body::empty()).unwrap();
            req.extensions_mut().insert(AddinOAuthAuth { client: AddinOAuthClient::Outlook, personal_org: false });
            req
        };
        let csms = |method: &str, path: &str| {
            let mut req = axum::http::Request::builder().method(method).uri(path).body(Body::empty()).unwrap();
            req.extensions_mut().insert(AddinMicrosoftAuth {
                session_id: Uuid::nil(),
                tenant_id: "t".into(),
                object_id: "o".into(),
            });
            req
        };
        let status = |req: axum::http::Request<Body>| async move { router().oneshot(req).await.unwrap().status() };

        assert_eq!(status(oauth("GET", "/me")).await, StatusCode::FORBIDDEN);
        assert_eq!(status(oauth("POST", "/connectors/microsoft/ingest")).await, StatusCode::OK);
        assert_eq!(status(oauth("POST", "/addin/microsoft/link")).await, StatusCode::OK);
        // The link POST is for OAuth tokens only; a csms_ is still refused it.
        assert_eq!(status(csms("POST", "/addin/microsoft/link")).await, StatusCode::FORBIDDEN);
        assert_eq!(status(csms("GET", "/addin/microsoft/link")).await, StatusCode::OK);
    }

    #[test]
    fn gmail_scope_is_exactly_the_gmail_addon_surface_and_disjoint_from_outlook() {
        use Method as M;
        for (m, p) in [
            (M::POST, "/connectors/google/ingest"),
            (M::GET, "/connectors/google/gmail/everything-mode"),
            (M::POST, "/connectors/google/gmail/everything-mode"),
            (M::GET, "/connectors/google/watched/18c3"),
            (M::PUT, "/connectors/google/watched/18c3"),
            (M::POST, "/connectors/google/meet/captures"),
            (M::GET, "/connectors/google/meet/captures"),
            (M::DELETE, "/connectors/google/meet/captures/7b0e5c1e-0000-4000-8000-000000000001"),
        ] {
            assert!(gmail_scope_allows(&m, p), "{m} {p}");
            assert!(!scope_allows(&m, p), "{m} {p} must not be an Outlook route");
        }
        // The one explicitly SHARED route: both scopes allow it, nothing else.
        assert!(gmail_scope_allows(&M::GET, "/addin/workspace"));
        assert!(scope_allows(&M::GET, "/addin/workspace"));
        assert!(shared_scope_allows(&M::GET, "/addin/workspace"));
        for (m, p) in [
            (M::POST, "/connectors/google/ingest"),
            (M::POST, "/connectors/google/meet/captures"),
            (M::GET, "/connectors/google/meet/captures"),
            (M::DELETE, "/connectors/google/meet/captures/abc"),
            (M::GET, "/connectors/google/gmail/everything-mode"),
            (M::GET, "/connectors/microsoft/everything-mode"),
            (M::GET, "/addin/microsoft/link"),
            (M::POST, "/addin/workspace"),
        ] {
            assert!(!shared_scope_allows(&m, p), "{m} {p} must not be shared");
        }
        for (m, p) in [
            (M::POST, "/addin/workspace"),
            (M::POST, "/addin/microsoft/link"),
            (M::GET, "/addin/microsoft/link"),
            (M::POST, "/connectors/microsoft/ingest"),
            (M::GET, "/me"),
            (M::DELETE, "/connectors/google/watched/18c3"),
            (M::GET, "/connectors/google/watched/"),
            (M::GET, "/connectors/google/watched/a/b"),
            (M::PUT, "/connectors/google/gmail/everything-mode"),
            (M::POST, "/connectors/google/ingest/x"),
            (M::DELETE, "/addin/tokens/1"),
            // Meet: only the exact pre-approval shapes.
            (M::DELETE, "/connectors/google/meet/captures"),
            (M::DELETE, "/connectors/google/meet/captures/"),
            (M::DELETE, "/connectors/google/meet/captures/a/b"),
            (M::GET, "/connectors/google/meet/captures/abc"),
            (M::POST, "/connectors/google/meet/captures/abc"),
            (M::PUT, "/connectors/google/meet/captures"),
            (M::GET, "/connectors/google/meet/capturesx"),
            (M::GET, "/connectors/google/meet/meetings"),
            (M::POST, "/connectors/google/meet/meetings/x/allow"),
            (M::DELETE, "/connectors/google/meet/meetings/x/allow"),
        ] {
            assert!(!gmail_scope_allows(&m, p), "{m} {p}");
        }
    }

    #[test]
    fn meet_scope_is_exactly_the_meet_panel_surface() {
        use Method as M;
        let meet_routes = [
            (M::POST, "/connectors/google/meet/captures"),
            (M::GET, "/connectors/google/meet/captures"),
            (M::DELETE, "/connectors/google/meet/captures/7b0e5c1e-0000-4000-8000-000000000001"),
        ];
        for (m, p) in meet_routes.clone() {
            assert!(meet_scope_allows(&m, p), "{m} {p}");
            // Never an Outlook/Teams route.
            assert!(!scope_allows(&m, p), "{m} {p} must not be an Outlook route");
        }
        assert!(meet_scope_allows(&M::GET, "/addin/workspace"));
        // Everything else is refused, including every Gmail-only and Outlook-only route.
        for (m, p) in [
            (M::POST, "/connectors/google/ingest"),
            (M::GET, "/connectors/google/gmail/everything-mode"),
            (M::POST, "/connectors/google/gmail/everything-mode"),
            (M::GET, "/connectors/google/watched/18c3"),
            (M::PUT, "/connectors/google/watched/18c3"),
            (M::POST, "/connectors/microsoft/ingest"),
            (M::GET, "/connectors/microsoft/everything-mode"),
            (M::POST, "/connectors/microsoft/teams/transcripts/capture"),
            (M::GET, "/addin/microsoft/link"),
            (M::POST, "/addin/microsoft/link"),
            (M::POST, "/addin/workspace"),
            (M::GET, "/me"),
            (M::DELETE, "/addin/tokens/1"),
            (M::DELETE, "/connectors/google/meet/captures"),
            (M::DELETE, "/connectors/google/meet/captures/"),
            (M::DELETE, "/connectors/google/meet/captures/a/b"),
            (M::GET, "/connectors/google/meet/captures/abc"),
            (M::PUT, "/connectors/google/meet/captures"),
            (M::GET, "/connectors/google/meet/capturesx"),
            (M::GET, "/connectors/google/meet/meetings"),
            (M::POST, "/connectors/google/meet/meetings/x/allow"),
            (M::DELETE, "/connectors/google/meet/meetings/x/allow"),
        ] {
            assert!(!meet_scope_allows(&m, p), "{m} {p}");
        }
        // Gmail-only routes stay Gmail-only.
        for (m, p) in [(M::POST, "/connectors/google/ingest"), (M::GET, "/connectors/google/gmail/everything-mode")] {
            assert!(gmail_scope_allows(&m, p) && !meet_scope_allows(&m, p), "{m} {p}");
        }
    }

    /// Each OAuth client marker is refused on the other clients' routes, through
    /// the real middleware.
    #[tokio::test]
    async fn session_scope_refuses_each_oauth_client_on_the_others_routes() {
        use axum::{body::Body, routing::{get, post}, Router};
        use tower::ServiceExt;

        let router = || {
            Router::new()
                .route("/addin/workspace", get(|| async { StatusCode::OK }))
                .route("/addin/microsoft/link", post(|| async { StatusCode::OK }))
                .route("/connectors/microsoft/ingest", post(|| async { StatusCode::OK }))
                .route("/connectors/google/ingest", post(|| async { StatusCode::OK }))
                .route(
                    "/connectors/google/meet/captures",
                    get(|| async { StatusCode::OK }).post(|| async { StatusCode::OK }),
                )
                .route("/connectors/google/meet/captures/{id}", axum::routing::delete(|| async { StatusCode::OK }))
                .route("/connectors/google/meet/meetings", get(|| async { StatusCode::OK }))
                .layer(axum::middleware::from_fn(session_scope))
        };
        let call = |client: AddinOAuthClient, method: &'static str, path: &'static str| async move {
            let mut req = axum::http::Request::builder().method(method).uri(path).body(Body::empty()).unwrap();
            req.extensions_mut().insert(AddinOAuthAuth { client, personal_org: false });
            router().oneshot(req).await.unwrap().status()
        };
        use AddinOAuthClient::{Gmail, Meet, Outlook};
        const OK: StatusCode = StatusCode::OK;
        const NO: StatusCode = StatusCode::FORBIDDEN;
        for (method, path, outlook, gmail, meet) in [
            ("GET", "/addin/workspace", OK, OK, OK),
            ("POST", "/addin/microsoft/link", OK, NO, NO),
            ("POST", "/connectors/microsoft/ingest", OK, NO, NO),
            ("POST", "/connectors/google/ingest", NO, OK, NO),
            ("POST", "/connectors/google/meet/captures", NO, OK, OK),
            ("GET", "/connectors/google/meet/captures", NO, OK, OK),
            ("DELETE", "/connectors/google/meet/captures/abc", NO, OK, OK),
            ("GET", "/connectors/google/meet/meetings", NO, NO, NO),
        ] {
            assert_eq!(call(Outlook, method, path).await, outlook, "outlook {method} {path}");
            assert_eq!(call(Gmail, method, path).await, gmail, "gmail {method} {path}");
            assert_eq!(call(Meet, method, path).await, meet, "meet {method} {path}");
        }
    }

    #[test]
    fn workspace_kind_labels() {
        assert_eq!(WorkspaceKind::Personal.label(), "personal");
        assert_eq!(WorkspaceKind::Org("org_x".into()).label(), "org");
    }

    #[test]
    fn workspace_serializes_to_the_contract_shape() {
        let id = Uuid::nil();
        let v = serde_json::to_value(Workspace {
            account_id: id,
            name: "Acme".into(),
            kind: "org",
        })
        .unwrap();
        assert_eq!(v, json!({"account_id": id, "name": "Acme", "kind": "org"}));
    }
}
