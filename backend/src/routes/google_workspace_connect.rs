//! "Connect Google Workspace": one Google consent screen, one connection per
//! surface the member actually granted (Calendar, Gmail, Chat, Meet).
//!
//! Pure logic (claim encoding, scope union, granted-scope mapping) is in
//! [`crate::connectors::google_workspace`], where its reasoning is written
//! down (why reading the granted `scope` is acceptable for this explicit
//! all-in-one flow even though the per-surface path refuses to). This module is
//! the route layer: gating by plane/flag, the `authorize` redirect, the
//! `callback` completion, and the one shared post-connect function both this
//! flow and the per-surface flow call.
//!
//! # What does NOT change
//! Capture semantics. A connect only stores credential rows plus the same
//! post-connect bootstrap an individual connect already ran (Calendar/Gmail
//! watch channel, Chat subscription and invite job, Meet sync-state row).
//! Gmail stays opt-in per thread, Meet per meeting, Chat per space.
//!
//! # Gating
//! Every surface stays gated by its own flag, in both planes, at three points
//! that all call [`enabled_surfaces`]: `mint_state` (the requested set is
//! signed into the state's `sfc` claim), `authorize` (claim ∩ flags now) and
//! `callback` (claim ∩ flags now). In an org a surface is on only if its own
//! `org_google_*_enabled_for` flag is on; if none is, mint/authorize/callback
//! refuse exactly like the per-surface gates do. In a personal workspace
//! Calendar is grandfathered on and the others follow their env default, the
//! same answer `authorize`'s per-surface gate gives there.
//!
//! # Partial failure
//! Each surface persists in its own transaction, so one failing surface never
//! blocks the others. Post-connect side effects are best-effort per surface
//! (as in the individual flow). The redirect reports connected / skipped /
//! failed per surface; see [`WorkspaceOutcome::to_query`].

use std::future::Future;
use std::sync::Arc;

use axum::{
    extract::{Extension, State},
    http::StatusCode,
    response::{IntoResponse, Redirect, Response},
    Json,
};
use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

use crate::connectors::google::Surface;
use crate::connectors::google_workspace::{
    encode_claim, parse_claim, plan_from_grant, union_scopes, WorkspaceOutcome,
};
use crate::connectors::{self, ConnectorProvider, TokenBundle};
use crate::crypto::DekPlane;
use crate::middleware::auth::UserContext;
use crate::oauth::state as oauth_state;
use crate::routes::oauth::{
    org_google_calendar_enabled_for, org_google_chat_enabled_for, org_google_gmail_enabled_for,
    org_google_meet_enabled_for, org_management_refused_response, record_connector_install,
    resolve_callback_dek_plane, resolve_google_token_target, store_token_bundle_tx,
};
use crate::tenancy::extractor::Tenant;
use crate::AppState;

// ---------------------------------------------------------------------------
// Gating
// ---------------------------------------------------------------------------

/// The surfaces the all-in-one flow may request for a workspace right now, in
/// `Surface::ALL` order. `clerk_org_id` is `Some` for an org account.
///
/// * Org: each surface on its own `org_google_<surface>_enabled_for` flag.
/// * Personal: Calendar is grandfathered on; Gmail/Chat/Meet follow
///   [`connectors::google::surface_env_default`], the same answer
///   `routes::oauth::google_surface_enabled_for` gives a personal caller.
pub(crate) async fn enabled_surfaces(state: &AppState, clerk_org_id: Option<&str>) -> Vec<Surface> {
    let mut out = Vec::new();
    for surface in Surface::ALL {
        let on = match clerk_org_id {
            Some(org) => match surface {
                Surface::Calendar => org_google_calendar_enabled_for(state, org).await,
                Surface::Gmail => org_google_gmail_enabled_for(state, org).await,
                Surface::Chat => org_google_chat_enabled_for(state, org).await,
                Surface::Meet => org_google_meet_enabled_for(state, org).await,
            },
            None => surface.is_grandfathered() || connectors::google::surface_env_default(surface),
        };
        if on && surface.is_implemented() {
            out.push(surface);
        }
    }
    out
}

/// `mint_state` arm for `?surface=workspace`: the signed `sfc` claim, or the
/// shared org refusal when this org has no Google surface enabled at all.
pub(crate) async fn mint_claim(state: &AppState, tenant: &Tenant) -> Result<String, Response> {
    let org = tenant.org_context().map(|c| c.org_id.as_str());
    let enabled = enabled_surfaces(state, org).await;
    if enabled.is_empty() {
        tracing::info!(
            org_id = ?org,
            "oauth google workspace mint refused: no Google surface is enabled for this workspace"
        );
        return Err(org_management_refused_response());
    }
    Ok(encode_claim(&enabled))
}

/// `google_org_callback_allowed`'s workspace arm: an org may complete the
/// all-in-one callback iff at least one surface in the state's claim is still
/// enabled for it.
pub(crate) async fn org_callback_allowed(
    state: &AppState,
    org_id: &str,
    state_surface: Option<&str>,
) -> bool {
    let requested = state_surface.and_then(parse_claim).unwrap_or_default();
    let enabled = enabled_surfaces(state, Some(org_id)).await;
    requested.iter().any(|s| enabled.contains(s))
}

/// `GET /v1/oauth/google/workspace`: which Google surfaces this workspace may
/// connect, so the web button reflects each flag independently. Authenticated
/// like `mint_state`; reveals only the caller's own workspace's flag state.
pub async fn workspace_surfaces(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(_user_ctx): Extension<UserContext>,
) -> Response {
    let org = tenant.org_context().map(|c| c.org_id.as_str());
    let enabled = enabled_surfaces(&state, org).await;
    let surfaces: Vec<serde_json::Value> = Surface::ALL
        .iter()
        .map(|s| json!({ "surface": s.as_str(), "enabled": enabled.contains(s) }))
        .collect();
    (StatusCode::OK, Json(json!({ "surfaces": surfaces }))).into_response()
}

// ---------------------------------------------------------------------------
// authorize
// ---------------------------------------------------------------------------

/// `authorize` for `?surface=workspace`. The state must verify, carry a
/// connecting user id and a workspace claim; the scope is the union of
/// (claim ∩ surfaces enabled right now).
pub(crate) async fn authorize(state: &AppState, state_val: &str, base_url: &str) -> Response {
    let mismatch = || {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "the workspace surface requires a freshly minted signed state for this \
                          surface — reconnect and try again"
            })),
        )
            .into_response()
    };
    let Some(secret) = oauth_state::state_secret() else {
        tracing::error!("OAUTH_STATE_SECRET missing or empty");
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "oauth state signing not configured"})),
        )
            .into_response();
    };
    let Ok(verified) = oauth_state::verify_state(&secret, state_val) else {
        return mismatch();
    };
    let requested = match verified.surface.as_deref().and_then(parse_claim) {
        Some(r) if verified.user_id.is_some() => r,
        _ => return mismatch(),
    };
    // Accounts and their Clerk organization mapping live only on the control plane.
    let Some(pool) = state.control_db() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "service unavailable"})),
        )
            .into_response();
    };
    let clerk_org_id: Option<String> =
        match sqlx::query_scalar("SELECT clerk_org_id FROM accounts WHERE id = $1")
            .bind(verified.account_id)
            .fetch_optional(pool)
            .await
        {
            Ok(Some(org)) => org,
            Ok(None) => return mismatch(),
            Err(e) => {
                tracing::error!(error = %e, "google workspace authorize: account lookup failed");
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(json!({"error": "service unavailable"})),
                )
                    .into_response();
            }
        };
    let effective = intersect(
        &requested,
        &enabled_surfaces(state, clerk_org_id.as_deref()).await,
    );
    if effective.is_empty() {
        tracing::info!("google workspace authorize refused: no requested surface is enabled");
        return (
            StatusCode::FORBIDDEN,
            Json(json!({"error": "no Google surface is enabled for this workspace yet"})),
        )
            .into_response();
    }
    match connectors::google::authorize_url_for_scopes(
        base_url,
        state_val,
        &union_scopes(&effective),
    ) {
        Ok(url) => Redirect::temporary(&url).into_response(),
        Err(e) => crate::routes::oauth::connector_error_response(e),
    }
}

fn intersect(a: &[Surface], b: &[Surface]) -> Vec<Surface> {
    Surface::ALL
        .iter()
        .copied()
        .filter(|s| a.contains(s) && b.contains(s))
        .collect()
}

// ---------------------------------------------------------------------------
// Shared per-surface post-connect (used by BOTH the individual and the
// all-in-one callback, so they cannot drift)
// ---------------------------------------------------------------------------

/// Everything an individual Google connect does AFTER the token row has
/// committed: the connector's `post_store` bootstrap (Calendar watch channel
/// on the personal or org path, Gmail `users.watch`, Chat Workspace Events
/// subscription, Meet sync-state row) and, for Chat, the invite job. All
/// best-effort; none can fail the redirect. `provider_key` is the surface's
/// stored literal (`Surface::token_provider`), never derived from the scope.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn google_surface_post_connect(
    state: &Arc<AppState>,
    pool: &PgPool,
    connector: &dyn ConnectorProvider,
    account_id: Uuid,
    bundle: &TokenBundle,
    provider_key: &str,
    connecting_user_id: Option<Uuid>,
    clerk_org_id: Option<&str>,
) {
    connector
        .post_store(&state.http_client, pool, account_id, bundle, provider_key)
        .await;

    if provider_key == Surface::Chat.token_provider() {
        if let Some(owner_user_id) = connecting_user_id {
            let app = state.clone();
            let control = pool.clone();
            let org = clerk_org_id.map(str::to_string);
            tokio::spawn(async move {
                crate::workers::google_chat_invite::enqueue_on_connect(
                    &app,
                    &control,
                    account_id,
                    owner_user_id,
                    org.as_deref(),
                )
                .await;
            });
        }
    }
}

// ---------------------------------------------------------------------------
// Persistence
// ---------------------------------------------------------------------------

/// Write ONE surface's connection exactly as the individual callback does:
/// owner and provider key from [`resolve_google_token_target`] (the same
/// per-surface owner rules, never re-implemented), then `oauth_tokens`,
/// `connector_installs` and the `install` audit row in one transaction.
/// Returns the stored provider key.
async fn persist_surface(
    pool: &PgPool,
    dek_plane: &DekPlane,
    account_id: Uuid,
    connecting_user_id: Uuid,
    is_org: bool,
    surface: Surface,
    bundle: &TokenBundle,
) -> Result<&'static str, ()> {
    let (owner, provider_key) =
        resolve_google_token_target(Some(surface.as_str()), Some(connecting_user_id), is_org)?;

    let mut tx = crate::tenancy::scope::begin_account_scoped(pool, account_id)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, %account_id, surface = surface.as_str(), "workspace connect: scoped tx failed");
        })?;
    store_token_bundle_tx(&mut tx, dek_plane, account_id, provider_key, owner, bundle)
        .await
        .map_err(|_| {
            tracing::error!(%account_id, surface = surface.as_str(), "workspace connect: token store failed");
        })?;
    record_connector_install(
        &mut tx,
        account_id,
        provider_key,
        &bundle.provider_workspace_id,
        Some(bundle.provider_user_id.as_str()),
        owner.owner_user_id(),
    )
    .await
    .map_err(|e| {
        tracing::error!(error = %e, %account_id, surface = surface.as_str(), "workspace connect: connector_installs failed");
    })?;
    crate::tenancy::scope::set_tenant_user(&mut tx, connecting_user_id)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, %account_id, "workspace connect: set app.user_id failed");
        })?;
    crate::services::connector_audit::record_in_tx(
        &mut tx,
        crate::services::connector_audit::AuditEntry {
            account_id,
            actor: crate::services::connector_audit::Actor::User(connecting_user_id),
            action: "install",
            provider: Some(provider_key.to_string()),
            subject: Some(bundle.provider_workspace_id.clone()),
            detail: None,
        },
    )
    .await
    .map_err(|e| {
        tracing::error!(error = %e, %account_id, surface = surface.as_str(), "workspace connect: audit row failed");
    })?;
    tx.commit().await.map_err(|e| {
        tracing::error!(error = %e, %account_id, surface = surface.as_str(), "workspace connect: commit failed");
    })?;
    Ok(provider_key)
}

/// Persist every requested-and-granted surface, then run `post_connect` for
/// each surface that persisted. `requested` is the verified claim already
/// intersected with the flags in force now; `bundle.scope` only subtracts from
/// it (see `connectors::google_workspace`'s module doc).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn connect_granted<F, Fut>(
    pool: &PgPool,
    dek_plane: &DekPlane,
    account_id: Uuid,
    connecting_user_id: Uuid,
    is_org: bool,
    requested: &[Surface],
    bundle: &TokenBundle,
    post_connect: F,
) -> WorkspaceOutcome
where
    F: Fn(Surface, &'static str) -> Fut,
    Fut: Future<Output = ()>,
{
    let plan = plan_from_grant(requested, &bundle.scope);
    let mut outcome = WorkspaceOutcome {
        skipped: plan.skipped,
        ..Default::default()
    };
    let mut stored: Vec<(Surface, &'static str)> = Vec::new();
    for surface in plan.connect {
        match persist_surface(
            pool,
            dek_plane,
            account_id,
            connecting_user_id,
            is_org,
            surface,
            bundle,
        )
        .await
        {
            Ok(key) => stored.push((surface, key)),
            Err(()) => outcome.failed.push(surface),
        }
    }
    // Post-connect AFTER every row has committed: the connector's owner
    // lookbacks read the just-written `oauth_tokens` rows. Concurrent, since
    // each is an independent best-effort network bootstrap.
    futures::future::join_all(stored.iter().map(|(s, key)| post_connect(*s, key))).await;
    outcome.connected = stored.into_iter().map(|(s, _)| s).collect();
    outcome
}

// ---------------------------------------------------------------------------
// callback
// ---------------------------------------------------------------------------

/// `callback` for a verified `sfc=workspace` state. Called from
/// `routes::oauth::callback` after the state/jti/org-gate checks, in place of
/// the per-surface token-target block.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn handle_callback(
    state: &Arc<AppState>,
    pool: &PgPool,
    connector: &'static dyn ConnectorProvider,
    account_id: Uuid,
    clerk_org_id: Option<&str>,
    connecting_user_id: Option<Uuid>,
    state_surface: &str,
    code: &str,
) -> Response {
    // No-fallback rule, same as every per-member surface: without a verified
    // connecting user id there is no owner to store the rows under.
    let Some(uid) = connecting_user_id else {
        tracing::warn!(
            %account_id,
            "google workspace callback refused: state carried no connecting user id"
        );
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "invalid state parameter"})),
        )
            .into_response();
    };
    let claimed = parse_claim(state_surface).unwrap_or_default();
    let requested = intersect(&claimed, &enabled_surfaces(state, clerk_org_id).await);
    if requested.is_empty() {
        // Checked before `exchange_code` so a doomed request never burns the
        // provider's one-time code.
        return (
            StatusCode::FORBIDDEN,
            Json(json!({"error": "no Google surface is enabled for this workspace yet"})),
        )
            .into_response();
    }

    let base_url = std::env::var("BASE_URL").unwrap_or_else(|_| "http://localhost:8080".into());
    let bundle = match connector
        .exchange_code(&state.http_client, &base_url, code)
        .await
    {
        Ok(b) => b,
        Err(e) => return crate::routes::oauth::connector_error_response(e),
    };
    let dek_plane = match resolve_callback_dek_plane(state, pool, account_id, clerk_org_id).await {
        Ok(p) => p,
        Err(resp) => return resp,
    };

    let outcome = connect_granted(
        pool,
        &dek_plane,
        account_id,
        uid,
        clerk_org_id.is_some(),
        &requested,
        &bundle,
        |surface, provider_key| {
            let bundle = &bundle;
            async move {
                // `Surface::token_provider` is the stored literal; the shared
                // function is the SAME one the individual callback calls.
                debug_assert_eq!(provider_key, surface.token_provider());
                google_surface_post_connect(
                    state,
                    pool,
                    connector,
                    account_id,
                    bundle,
                    provider_key,
                    Some(uid),
                    clerk_org_id,
                )
                .await;
            }
        },
    )
    .await;

    if let Err(e) =
        crate::read_models::dirty::connector_account_changed(state, account_id, uid).await
    {
        tracing::error!(error=%e,"Google Workspace callback read-model invalidation failed");
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error":"read model invalidation failed"})),
        )
            .into_response();
    }

    tracing::info!(
        %account_id,
        connected = ?outcome.connected,
        skipped = ?outcome.skipped,
        failed = ?outcome.failed,
        workspace = %bundle.provider_workspace_id,
        "Google Workspace connection established"
    );

    let frontend_url = state.web_base_url(pool, clerk_org_id).await;
    Redirect::temporary(&format!("{frontend_url}/connectors?{}", outcome.to_query()))
        .into_response()
}

#[cfg(test)]
mod google_workspace_pg_tests {
    //! Real-Postgres tests. Gated on `ORGT_TEST_BASE_URL`; the CI step that
    //! runs them fails on a skip line (see `.github/workflows/ci.yml`).

    use super::*;
    use chrono::Utc;
    use coralswarm_core::kek::{generate_dek, load_kek, wrap_dek};
    use sqlx::postgres::PgPoolOptions;
    use sqlx::{Connection, Executor, PgConnection};
    use std::sync::Mutex;

    use crate::connectors::google_workspace::SkipReason;
    use crate::connectors::TokenOwner;

    fn base_url() -> Option<String> {
        std::env::var("ORGT_TEST_BASE_URL").ok()
    }
    fn with_db(base: &str, db: &str) -> String {
        let (prefix, rest) = base.split_at(base.rfind('/').expect("url has a path /"));
        let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
        format!("{prefix}/{db}{params}")
    }
    async fn fresh_pool(base: &str, db: &str) -> PgPool {
        let mut conn = PgConnection::connect(base).await.expect("connect admin");
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .unwrap();
        conn.execute(format!("CREATE DATABASE {db}").as_str())
            .await
            .unwrap();
        conn.close().await.ok();
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
    async fn teardown(base: &str, db: &str, pool: PgPool) {
        pool.close().await;
        let mut admin = PgConnection::connect(base).await.unwrap();
        admin
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }
    fn set_test_kek() {
        // SAFETY: every test in this process sets the same constant.
        unsafe { std::env::set_var("MASTER_KEK_HEX", "cc".repeat(32)) };
    }
    async fn seed_user(pool: &PgPool) -> Uuid {
        sqlx::query_scalar(
            "INSERT INTO users(id, email) VALUES (gen_random_uuid(), $1) RETURNING id",
        )
        .bind(format!("user-{}@test.local", Uuid::new_v4()))
        .fetch_one(pool)
        .await
        .unwrap()
    }
    /// Account with a wrapped DEK; `clerk_org_id` makes it an org account.
    async fn seed_account(pool: &PgPool, owner: Uuid, clerk_org_id: Option<&str>) -> Uuid {
        let kek = load_kek().expect("test KEK loads");
        let wrapped = wrap_dek(&kek, &generate_dek()).expect("wrap test dek");
        sqlx::query_scalar(
            "INSERT INTO accounts(owner_user_id, dek_wrapped, clerk_org_id) VALUES ($1, $2, $3) RETURNING id",
        )
        .bind(owner)
        .bind(&wrapped)
        .bind(clerk_org_id)
        .fetch_one(pool)
        .await
        .unwrap()
    }
    async fn set_org_flag(pool: &PgPool, org: &str, provider: &str, enabled: bool) {
        sqlx::query(
            "INSERT INTO org_connector_flags (clerk_org_id, provider, enabled) VALUES ($1, $2, $3) \
             ON CONFLICT (clerk_org_id, provider) DO UPDATE SET enabled = EXCLUDED.enabled",
        )
        .bind(org)
        .bind(provider)
        .bind(enabled)
        .execute(pool)
        .await
        .unwrap();
    }
    fn granted_scope(surfaces: &[Surface]) -> String {
        union_scopes(surfaces)
    }
    fn bundle(scope: String) -> TokenBundle {
        TokenBundle {
            access_token: "gws-access".into(),
            refresh_token: Some("gws-refresh".into()),
            user_token: None,
            scope,
            expires_at: Some(Utc::now() + chrono::Duration::hours(1)),
            provider_workspace_id: "member@example.com".into(),
            provider_workspace_name: Some("member@example.com".into()),
            provider_user_id: "sub-member".into(),
        }
    }
    async fn token_rows(pool: &PgPool, account_id: Uuid) -> Vec<(String, Option<Uuid>)> {
        sqlx::query_as(
            "SELECT provider, owner_user_id FROM oauth_tokens WHERE account_id = $1 ORDER BY provider",
        )
        .bind(account_id)
        .fetch_all(pool)
        .await
        .unwrap()
    }
    async fn count(pool: &PgPool, sql: &str, account_id: Uuid) -> i64 {
        sqlx::query_scalar(sql)
            .bind(account_id)
            .fetch_one(pool)
            .await
            .unwrap()
    }
    fn recorder() -> (
        Arc<Mutex<Vec<(Surface, &'static str)>>>,
        impl Fn(Surface, &'static str) -> std::future::Ready<()>,
    ) {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let s2 = seen.clone();
        (seen, move |s, k| {
            s2.lock().unwrap().push((s, k));
            std::future::ready(())
        })
    }
    fn tenant(org_id: Option<&str>, pool: &PgPool) -> Tenant {
        Tenant {
            store: None,
            pool: Some(pool.clone()),
            decision: org_id.map(|o| {
                crate::tenancy::runtime::TenancyDecision::Org(
                    crate::tenancy::runtime::TenantContext {
                        org_id: o.to_string(),
                        account_id: Uuid::new_v4(),
                        user_id: Uuid::new_v4(),
                        pool: pool.clone(),
                        page_pool: None,
                    },
                )
            }),
            class: crate::route_class::RouteClass::Other,
        }
    }

    /// PERSONAL: Calendar is AccountWide (owner NULL), the others per-member;
    /// every granted surface gets a token row, a connector_installs row and an
    /// `install` audit row, and each one's post-connect runs exactly once.
    #[tokio::test]
    async fn personal_workspace_connect_writes_each_surface_with_its_own_owner_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping personal_workspace_connect_writes_each_surface_with_its_own_owner_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let _env = crate::routes::oauth::GOOGLE_SURFACE_ENV_LOCK.lock().await;
        set_test_kek();
        let db = "gws_one_connect_personal";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let pool = fresh_pool(&base, db).await;
        let user = seed_user(&pool).await;
        let account = seed_account(&pool, user, None).await;

        let requested = Surface::ALL.to_vec();
        let b = bundle(granted_scope(&requested));
        let (seen, post) = recorder();
        let outcome = connect_granted(
            &pool,
            &DekPlane::consumer(pool.clone()),
            account,
            user,
            false,
            &requested,
            &b,
            post,
        )
        .await;

        assert_eq!(outcome.connected, Surface::ALL.to_vec());
        assert!(
            outcome.skipped.is_empty() && outcome.failed.is_empty(),
            "{outcome:?}"
        );
        let rows = token_rows(&pool, account).await;
        assert_eq!(
            rows,
            vec![
                ("google".to_string(), None),
                ("google_chat".to_string(), Some(user)),
                ("google_gmail".to_string(), Some(user)),
                ("google_meet".to_string(), Some(user)),
            ],
            "Calendar AccountWide, the other three per-member"
        );
        assert_eq!(
            count(
                &pool,
                "SELECT count(*) FROM connector_installs WHERE account_id = $1",
                account
            )
            .await,
            4
        );
        assert_eq!(
            count(&pool, "SELECT count(*) FROM connector_audit_log WHERE account_id = $1 AND action = 'install'", account).await,
            4
        );
        let mut post_keys: Vec<&str> = seen.lock().unwrap().iter().map(|(_, k)| *k).collect();
        post_keys.sort();
        assert_eq!(
            post_keys,
            vec!["google", "google_chat", "google_gmail", "google_meet"]
        );

        teardown(&base, db, pool).await;
    }

    /// ORG: every surface is per-member (Calendar included, under the same
    /// `google` literal), and the member owns all four rows.
    #[tokio::test]
    async fn org_workspace_connect_stores_every_surface_per_member_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping org_workspace_connect_stores_every_surface_per_member_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let _env = crate::routes::oauth::GOOGLE_SURFACE_ENV_LOCK.lock().await;
        set_test_kek();
        let db = "gws_one_connect_org";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let pool = fresh_pool(&base, db).await;
        let user = seed_user(&pool).await;
        let account = seed_account(&pool, user, Some("org_gws_connect")).await;

        let requested = Surface::ALL.to_vec();
        let b = bundle(granted_scope(&requested));
        let (seen, post) = recorder();
        let outcome = connect_granted(
            &pool,
            &DekPlane::consumer(pool.clone()),
            account,
            user,
            true,
            &requested,
            &b,
            post,
        )
        .await;

        assert_eq!(outcome.connected, Surface::ALL.to_vec());
        let rows = token_rows(&pool, account).await;
        assert_eq!(rows.len(), 4);
        assert!(
            rows.iter().all(|(_, owner)| *owner == Some(user)),
            "in an org every surface, Calendar included, is owned by the connecting member: {rows:?}"
        );
        assert!(rows.iter().any(|(p, _)| p == "google"));
        assert_eq!(seen.lock().unwrap().len(), 4);

        teardown(&base, db, pool).await;
    }

    /// ORG flag-off surface: the requested set (claim ∩ flags) excludes it, so
    /// no row is written for it even though Google returned its scope (a
    /// previous grant surfaced by include_granted_scopes). Uses the REAL
    /// `enabled_surfaces` against `org_connector_flags` rows.
    #[tokio::test]
    async fn org_flag_off_surface_is_skipped_even_when_google_returns_its_scope_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping org_flag_off_surface_is_skipped_even_when_google_returns_its_scope_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let _env = crate::routes::oauth::GOOGLE_SURFACE_ENV_LOCK.lock().await;
        set_test_kek();
        let db = "gws_one_connect_flag_off";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let pool = fresh_pool(&base, db).await;
        let org = "org_gws_flag_off";
        let user = seed_user(&pool).await;
        let account = seed_account(&pool, user, Some(org)).await;
        set_org_flag(&pool, org, "google_calendar", true).await;
        set_org_flag(&pool, org, "google_gmail", true).await;
        set_org_flag(&pool, org, "google_chat", false).await;
        set_org_flag(&pool, org, "google_meet", false).await;

        let state = Arc::new(crate::test_app_state_owned_with_db(pool.clone(), None));

        // The mint-time claim carries only the enabled surfaces.
        let t = tenant(Some(org), &pool);
        let claim = mint_claim(&state, &t)
            .await
            .expect("two surfaces on, so mint succeeds");
        assert_eq!(claim, "workspace:calendar,gmail");

        // Callback-time: claim ∩ flags, then the grant. Google returned ALL
        // four scopes (the member granted Chat and Meet before).
        let requested = intersect(
            &parse_claim(&claim).unwrap(),
            &enabled_surfaces(&state, Some(org)).await,
        );
        let b = bundle(granted_scope(&Surface::ALL));
        let (seen, post) = recorder();
        let outcome = connect_granted(
            &pool,
            &DekPlane::consumer(pool.clone()),
            account,
            user,
            true,
            &requested,
            &b,
            post,
        )
        .await;

        assert_eq!(outcome.connected, vec![Surface::Calendar, Surface::Gmail]);
        assert_eq!(
            outcome.skipped,
            vec![
                (Surface::Chat, SkipReason::Disabled),
                (Surface::Meet, SkipReason::Disabled)
            ]
        );
        let providers: Vec<String> = token_rows(&pool, account)
            .await
            .into_iter()
            .map(|(p, _)| p)
            .collect();
        assert_eq!(
            providers,
            vec!["google".to_string(), "google_gmail".to_string()]
        );
        assert_eq!(
            seen.lock().unwrap().len(),
            2,
            "post-connect runs only for connected surfaces"
        );

        // And with every flag off the org is refused at mint, exactly like the
        // per-surface gates.
        for p in ["google_calendar", "google_gmail"] {
            set_org_flag(&pool, org, p, false).await;
        }
        state
            .connector_flag_cache
            .invalidate(org, "google_calendar");
        state.connector_flag_cache.invalidate(org, "google_gmail");
        let refused = mint_claim(&state, &t)
            .await
            .expect_err("no surface enabled");
        assert_eq!(refused.status(), StatusCode::FORBIDDEN);
        assert!(!org_callback_allowed(&state, org, Some("workspace:calendar,gmail")).await);

        teardown(&base, db, pool).await;
    }

    /// Granular consent: the member unticks Gmail and Meet. Only the ticked
    /// surfaces get rows; the unticked ones are reported, not created.
    #[tokio::test]
    async fn partial_grant_writes_only_the_ticked_surfaces_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping partial_grant_writes_only_the_ticked_surfaces_pg: set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let _env = crate::routes::oauth::GOOGLE_SURFACE_ENV_LOCK.lock().await;
        set_test_kek();
        let db = "gws_one_connect_partial";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let pool = fresh_pool(&base, db).await;
        let user = seed_user(&pool).await;
        let account = seed_account(&pool, user, None).await;

        let b = bundle(granted_scope(&[Surface::Calendar, Surface::Chat]));
        let (_seen, post) = recorder();
        let outcome = connect_granted(
            &pool,
            &DekPlane::consumer(pool.clone()),
            account,
            user,
            false,
            &Surface::ALL,
            &b,
            post,
        )
        .await;

        assert_eq!(outcome.connected, vec![Surface::Calendar, Surface::Chat]);
        assert_eq!(
            outcome.skipped,
            vec![
                (Surface::Gmail, SkipReason::Unticked),
                (Surface::Meet, SkipReason::Unticked)
            ]
        );
        let providers: Vec<String> = token_rows(&pool, account)
            .await
            .into_iter()
            .map(|(p, _)| p)
            .collect();
        assert_eq!(
            providers,
            vec!["google".to_string(), "google_chat".to_string()]
        );

        teardown(&base, db, pool).await;
    }

    /// DISCONNECT HAZARD. All four rows share one Google grant (same refresh
    /// token). Disconnecting ONE tile, through the real `delete_connection`
    /// handler, must remove only that surface's row and side effects.
    /// Today's disconnect makes no Google token-revoke call at all (asserted
    /// structurally by `no_google_token_revoke_call_exists_in_the_backend`
    /// below); this test pins the other half: the sibling rows, their
    /// installs, watch channels and sync state survive, so the shared grant is
    /// still usable by the three remaining surfaces.
    #[tokio::test]
    async fn disconnecting_one_tile_leaves_the_sibling_surfaces_and_their_grant_intact_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping disconnecting_one_tile_leaves_the_sibling_surfaces_and_their_grant_intact_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let _env = crate::routes::oauth::GOOGLE_SURFACE_ENV_LOCK.lock().await;
        set_test_kek();
        let db = "gws_one_connect_disconnect";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let pool = fresh_pool(&base, db).await;
        let user = seed_user(&pool).await;
        let account = seed_account(&pool, user, None).await;

        let b = bundle(granted_scope(&Surface::ALL));
        let (_seen, post) = recorder();
        let outcome = connect_granted(
            &pool,
            &DekPlane::consumer(pool.clone()),
            account,
            user,
            false,
            &Surface::ALL,
            &b,
            post,
        )
        .await;
        assert_eq!(outcome.connected.len(), 4);

        // Per-surface side-effect rows, one of each kind.
        for surface in ["calendar", "gmail"] {
            sqlx::query(
                "INSERT INTO google_watch_channels \
                   (account_id, surface, resource_id, owner_user_id, channel_id, channel_resource_id, channel_token, expires_at) \
                 VALUES ($1, $2, 'member@example.com', $3, $4, 'rid', 'tok', now() + interval '1 day')",
            )
            .bind(account)
            .bind(surface)
            .bind(if surface == "calendar" { None } else { Some(user) })
            .bind(format!("chan-{surface}"))
            .execute(&pool)
            .await
            .unwrap();
        }
        crate::connectors::google_meet::ensure_sync_state(&pool, account, user)
            .await
            .unwrap();

        // Remember every sibling's stored ciphertext: a disconnect of another
        // tile must not rewrite them.
        let before: Vec<(String, Vec<u8>, Option<Vec<u8>>)> = sqlx::query_as(
            "SELECT provider, access_token, refresh_token FROM oauth_tokens \
             WHERE account_id = $1 AND provider <> 'google_gmail' ORDER BY provider",
        )
        .bind(account)
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(before.len(), 3);

        let state = Arc::new(crate::test_app_state_owned_with_db(pool.clone(), None));
        let ctx = UserContext {
            user_id: user,
            account_id: account,
            role: "owner".to_string(),
            device_id: "test-device".to_string(),
        };
        let resp = crate::routes::oauth::delete_connection(
            State(state.clone()),
            tenant(None, &pool),
            Extension(ctx),
            axum::extract::Path("google_gmail".to_string()),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);

        let providers: Vec<String> = token_rows(&pool, account)
            .await
            .into_iter()
            .map(|(p, _)| p)
            .collect();
        assert_eq!(
            providers,
            vec![
                "google".to_string(),
                "google_chat".to_string(),
                "google_meet".to_string()
            ],
            "only the Gmail row is gone"
        );
        let after: Vec<(String, Vec<u8>, Option<Vec<u8>>)> = sqlx::query_as(
            "SELECT provider, access_token, refresh_token FROM oauth_tokens \
             WHERE account_id = $1 ORDER BY provider",
        )
        .bind(account)
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(
            after, before,
            "sibling credentials are byte-for-byte untouched"
        );

        let installs: Vec<String> = sqlx::query_scalar(
            "SELECT provider FROM connector_installs WHERE account_id = $1 ORDER BY provider",
        )
        .bind(account)
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(installs, vec!["google", "google_chat", "google_meet"]);
        assert_eq!(
            count(&pool, "SELECT count(*) FROM google_watch_channels WHERE account_id = $1 AND surface = 'calendar'", account).await,
            1,
            "Calendar's watch channel survives a Gmail disconnect"
        );
        assert_eq!(
            count(&pool, "SELECT count(*) FROM google_watch_channels WHERE account_id = $1 AND surface = 'gmail'", account).await,
            0,
            "Gmail's own watch channel is purged"
        );
        assert_eq!(
            count(
                &pool,
                "SELECT count(*) FROM google_meet_sync_state WHERE account_id = $1",
                account
            )
            .await,
            1,
            "Meet's sync state survives a Gmail disconnect"
        );

        teardown(&base, db, pool).await;
    }

    /// REFRESH. Four rows hold the same refresh token. Persisting a refreshed
    /// access token for ONE row (the exact write every worker does:
    /// `store_token_bundle` keyed by that row's provider and owner) must not
    /// touch any sibling row. Google does not rotate the refresh token on a
    /// refresh, and `refresh_if_needed` carries the stored one forward, so the
    /// shared grant stays valid for every row.
    #[tokio::test]
    async fn refreshing_one_rows_access_token_does_not_touch_its_siblings_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping refreshing_one_rows_access_token_does_not_touch_its_siblings_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let _env = crate::routes::oauth::GOOGLE_SURFACE_ENV_LOCK.lock().await;
        set_test_kek();
        let db = "gws_one_connect_refresh";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let pool = fresh_pool(&base, db).await;
        let user = seed_user(&pool).await;
        let account = seed_account(&pool, user, None).await;

        let b = bundle(granted_scope(&Surface::ALL));
        let (_seen, post) = recorder();
        connect_granted(
            &pool,
            &DekPlane::consumer(pool.clone()),
            account,
            user,
            false,
            &Surface::ALL,
            &b,
            post,
        )
        .await;

        let snapshot = |pool: PgPool| async move {
            sqlx::query_as::<_, (String, Vec<u8>, Option<Vec<u8>>)>(
                "SELECT provider, access_token, refresh_token FROM oauth_tokens \
                 WHERE account_id = $1 ORDER BY provider",
            )
            .bind(account)
            .fetch_all(&pool)
            .await
            .unwrap()
        };
        let before = snapshot(pool.clone()).await;

        // A refresh of the Gmail row: new access token, refresh token carried
        // forward exactly as `refresh_if_needed` does.
        let mut refreshed = b.clone();
        refreshed.access_token = "gws-access-REFRESHED".into();
        crate::routes::oauth::store_token_bundle(
            &pool,
            &DekPlane::consumer(pool.clone()),
            account,
            "google_gmail",
            TokenOwner::Member(user),
            &refreshed,
        )
        .await
        .map_err(|e| e.status())
        .expect("persist refreshed bundle");

        let after = snapshot(pool.clone()).await;
        assert_eq!(before.len(), 4);
        for (b_row, a_row) in before.iter().zip(after.iter()) {
            if b_row.0 == "google_gmail" {
                assert_ne!(b_row.1, a_row.1, "the refreshed row's access token changed");
            } else {
                assert_eq!(
                    b_row, a_row,
                    "sibling {} must be untouched by another row's refresh",
                    b_row.0
                );
            }
        }

        teardown(&base, db, pool).await;
    }

    /// The real `mint_state` -> `authorize` handlers, end to end: the state
    /// carries the connecting uid and the enabled-surface claim, and the
    /// Google redirect asks for exactly the union of THOSE surfaces' scopes.
    /// A surface whose flag is off (here Chat and Meet, in an org) is absent
    /// from the consent URL, and flipping the last flag off refuses at
    /// `authorize` before any redirect.
    #[tokio::test]
    async fn mint_then_authorize_requests_only_the_enabled_surfaces_scopes_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping mint_then_authorize_requests_only_the_enabled_surfaces_scopes_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let _env = crate::routes::oauth::GOOGLE_SURFACE_ENV_LOCK.lock().await;
        set_test_kek();
        let db = "gws_one_connect_authorize";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let pool = fresh_pool(&base, db).await;
        let org = "org_gws_authorize";
        let user = seed_user(&pool).await;
        let account = seed_account(&pool, user, Some(org)).await;
        set_org_flag(&pool, org, "google_calendar", true).await;
        set_org_flag(&pool, org, "google_gmail", true).await;
        set_org_flag(&pool, org, "google_chat", false).await;
        set_org_flag(&pool, org, "google_meet", false).await;

        let prior_secret = std::env::var("OAUTH_STATE_SECRET").ok();
        let prior_client = std::env::var("GOOGLE_CALENDAR_CLIENT_ID").ok();
        // SAFETY: GOOGLE_SURFACE_ENV_LOCK is held; the pg-gated suites run with
        // --test-threads=1 in CI, so nothing else reads these concurrently.
        unsafe {
            std::env::set_var(
                "OAUTH_STATE_SECRET",
                "gws-one-connect-test-secret-nostatezzz",
            );
            std::env::set_var("GOOGLE_CALENDAR_CLIENT_ID", "gws-test-client-id");
        }

        let state = Arc::new(crate::test_app_state_owned_with_db(pool.clone(), None));
        let ctx = || UserContext {
            user_id: user,
            account_id: account,
            role: "member".to_string(),
            device_id: "test-device".to_string(),
        };
        let mint = |t: Tenant| {
            let state = state.clone();
            let ctx = ctx();
            async move {
                crate::routes::oauth::mint_state(
                    State(state),
                    t,
                    Extension(ctx),
                    axum::extract::Query(crate::routes::oauth::MintStateParams {
                        surface: Some("workspace".to_string()),
                    }),
                )
                .await
            }
        };
        let resp = mint(tenant(Some(org), &pool)).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let minted: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let state_val = minted["state"].as_str().unwrap().to_string();

        let secret = oauth_state::state_secret().unwrap();
        let verified = oauth_state::verify_state(&secret, &state_val).unwrap();
        assert_eq!(
            verified.user_id,
            Some(user),
            "the state carries the connecting uid"
        );
        assert_eq!(
            verified.surface.as_deref(),
            Some("workspace:calendar,gmail")
        );

        let authorize_resp = |sv: String| {
            let state = state.clone();
            async move {
                crate::routes::oauth::authorize(
                    State(state),
                    axum::extract::Path("google".to_string()),
                    axum::extract::Query(crate::routes::oauth::AuthorizeParams {
                        state: Some(sv),
                        surface: Some("workspace".to_string()),
                    }),
                )
                .await
            }
        };
        let resp = authorize_resp(state_val.clone()).await;
        assert_eq!(resp.status(), StatusCode::TEMPORARY_REDIRECT);
        let loc = resp
            .headers()
            .get("location")
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        let scope_param = loc.split('&').find(|p| p.starts_with("scope=")).unwrap();
        let scope = urlencoding::decode(&scope_param["scope=".len()..])
            .unwrap()
            .into_owned();
        assert_eq!(scope, union_scopes(&[Surface::Calendar, Surface::Gmail]));
        assert!(
            !scope.contains("chat.") && !scope.contains("meetings."),
            "{scope}"
        );
        assert!(loc.contains("include_granted_scopes=true"));

        // A bare/forged `workspace` surface with a plain (claim-less) state is refused.
        let plain = oauth_state::sign_state(
            &secret,
            account,
            chrono::Utc::now().timestamp(),
            "n",
            Some(user),
            None,
        )
        .unwrap();
        assert_eq!(
            authorize_resp(plain).await.status(),
            StatusCode::BAD_REQUEST
        );

        // Last flag flipped off after mint: authorize refuses, no redirect.
        for p in ["google_calendar", "google_gmail"] {
            set_org_flag(&pool, org, p, false).await;
            state.connector_flag_cache.invalidate(org, p);
        }
        assert_eq!(
            authorize_resp(state_val).await.status(),
            StatusCode::FORBIDDEN
        );
        assert_eq!(
            mint(tenant(Some(org), &pool)).await.status(),
            StatusCode::FORBIDDEN
        );

        // SAFETY: as above.
        unsafe {
            match prior_secret {
                Some(v) => std::env::set_var("OAUTH_STATE_SECRET", v),
                None => std::env::remove_var("OAUTH_STATE_SECRET"),
            }
            match prior_client {
                Some(v) => std::env::set_var("GOOGLE_CALENDAR_CLIENT_ID", v),
                None => std::env::remove_var("GOOGLE_CALENDAR_CLIENT_ID"),
            }
        }
        teardown(&base, db, pool).await;
    }

    /// Files allowed to call the revoke endpoint, each with the reason. Keep
    /// this list minimal: every other file still trips the tripwire.
    ///
    /// - `connectors/member_offboarding.rs`: a departing org member's own
    ///   Google grant is revoked as part of offboarding (settings PR #1473).
    ///   It is best-effort and carries the sibling check this tripwire
    ///   demands: the revoke is skipped when the same grant is still held
    ///   elsewhere (another surface row or owner), so it cannot break a
    ///   sibling surface.
    const REVOKE_ALLOWLIST: &[&str] = &["connectors/member_offboarding.rs"];

    /// Tripwire for the revoke rule: no code path may call Google's token
    /// revoke endpoint without first checking that no sibling surface row
    /// still uses the grant. Today there is none (Google disconnect is local
    /// plus a Chat subscription delete), so this fails the build the day
    /// someone adds one, forcing them to read this module's doc and add the
    /// sibling check.
    #[test]
    fn no_google_token_revoke_call_exists_in_the_backend() {
        fn walk(dir: &std::path::Path, hits: &mut Vec<String>) {
            for entry in std::fs::read_dir(dir).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    walk(&path, hits);
                } else if path.extension().is_some_and(|e| e == "rs")
                    && !path.ends_with("google_workspace_connect.rs")
                    && !REVOKE_ALLOWLIST.iter().any(|f| path.ends_with(f))
                {
                    let text = std::fs::read_to_string(&path).unwrap();
                    if text.contains("oauth2.googleapis.com/revoke")
                        || text.contains("accounts.google.com/o/oauth2/revoke")
                    {
                        hits.push(path.display().to_string());
                    }
                }
            }
        }
        let mut hits = Vec::new();
        walk(
            &std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src"),
            &mut hits,
        );
        assert!(
            hits.is_empty(),
            "a Google token revoke call was added ({hits:?}). All four surface rows may share ONE \
             Google grant: revoking it from a single-tile disconnect silently breaks the other \
             surfaces. Call revoke only when no other google* oauth_tokens row for the same \
             account/owner remains."
        );
    }
}
