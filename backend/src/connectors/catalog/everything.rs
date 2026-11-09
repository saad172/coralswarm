//! Refresh member-owned conversation watches using the provider's discovery API.
use super::{AdapterCx, AdapterError, Facts};
use crate::{
    middleware::auth::UserContext,
    tenancy::{
        extractor::Tenant,
        runtime::{TenancyDecision, TenantContext},
        TenantId,
    },
    workers::tenant_harness::WorkerTenant,
    AppState,
};
use sqlx::PgPool;
use std::sync::Arc;
use uuid::Uuid;

pub async fn refresh(
    t: &WorkerTenant,
    control: &PgPool,
    state: &Arc<AppState>,
    provider: &str,
) -> Result<(), String> {
    let org = match &t.tenant {
        TenantId::Consumer => None,
        TenantId::Org(id) => Some(id.as_str()),
    };
    let accounts: Vec<Uuid> = sqlx::query_scalar("SELECT id FROM accounts WHERE ($1::text IS NULL AND NOT EXISTS (SELECT 1 FROM tenant_databases t WHERE t.account_id=accounts.id)) OR clerk_org_id=$1").bind(org).fetch_all(control).await.map_err(|e| e.to_string())?;
    let mut rows = Vec::<(Uuid, Uuid, String, String)>::new();
    for account in accounts {
        let mut tx = crate::tenancy::scope::begin_account_scoped(control, account)
            .await
            .map_err(|e| e.to_string())?;
        rows.extend(sqlx::query_as::<_, (Uuid, Uuid, String, String)>(
            "SELECT s.account_id, s.owner_user_id, s.source, m.role FROM connector_sources s JOIN account_members m ON m.account_id=s.account_id AND m.user_id=s.owner_user_id WHERE s.account_id=$1 AND s.provider=$2 AND s.scope_mode='everything' AND s.enabled AND s.paused_at IS NULL AND s.source IN ('chat','teams_chats','teams_channels')")
            .bind(account).bind(provider).fetch_all(&mut *tx).await.map_err(|e| e.to_string())?);
        tx.commit().await.map_err(|e| e.to_string())?;
    }
    for (account_id, user_id, source, role) in rows {
        let tenant = Tenant {
            pool: Some(t.pool.clone()),
            store: Some(t.store.clone()),
            decision: org.map(|org| {
                TenancyDecision::Org(TenantContext {
                    org_id: org.into(),
                    account_id,
                    user_id,
                    pool: t.pool.clone(),
                    page_pool: None,
                })
            }),
            class: crate::route_class::RouteClass::Other,
        };
        let user = UserContext {
            account_id,
            user_id,
            role,
            device_id: String::new(),
        };
        let facts = Facts::load(state, &tenant, account_id, user_id)
            .await
            .map_err(|e| e.to_string())?;
        let cx = AdapterCx::new(&facts, state, &tenant, &user);
        let _lock = lock(&cx, provider, &source)
            .await
            .map_err(|e| e.to_string())?;
        let mut tx = crate::tenancy::scope::begin_account_scoped(control, account_id)
            .await
            .map_err(|e| e.to_string())?;
        let active: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM connector_sources WHERE account_id=$1 AND owner_user_id=$2 AND provider=$3 AND source=$4 AND scope_mode='everything' AND enabled AND paused_at IS NULL AND EXISTS(SELECT 1 FROM account_members m WHERE m.account_id=$1 AND m.user_id=$2))")
            .bind(account_id).bind(user_id).bind(provider).bind(&source).fetch_one(&mut *tx).await.map_err(|e| e.to_string())?;
        tx.commit().await.map_err(|e| e.to_string())?;
        if !active {
            continue;
        }
        let result = match provider {
            "google" => super::adapters::google::refresh_everything(&cx, &source).await,
            _ => super::adapters::microsoft::refresh_everything(&cx, &source).await,
        };
        if let Err(error) = result {
            tracing::warn!(%account_id, %user_id, %source, %error, "conversation discovery failed; retry next sweep");
        }
    }
    Ok(())
}

/// Serialize discovery with picker replacements across API and worker processes.
pub(crate) async fn lock(
    cx: &AdapterCx<'_>,
    provider: &str,
    source: &str,
) -> Result<sqlx::Transaction<'static, sqlx::Postgres>, AdapterError> {
    let pool = cx
        .state
        .control_db()
        .ok_or_else(|| AdapterError::Database("control plane unavailable".into()))?;
    let mut tx = pool.begin().await?;
    let key = format!(
        "conversation-scope:{}:{}:{provider}:{source}",
        cx.account_id, cx.user_id
    );
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(key)
        .execute(&mut *tx)
        .await?;
    Ok(tx)
}
