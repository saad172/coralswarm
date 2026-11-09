use super::Error;
use sqlx::PgPool;
use uuid::Uuid;
#[derive(Clone, Copy, Debug)]
pub enum DirtyTrigger {
    Table(&'static str),
    Event(DirtyEvent),
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DirtyEvent {
    ConnectorGrantChanged,
    BillingChanged,
    PrefsChanged,
    OrgAdminChanged,
}
pub fn models_for(event: DirtyEvent) -> Vec<&'static str> {
    super::registry::all()
        .iter()
        .filter(|m| {
            m.dirty_triggers()
                .iter()
                .any(|t| matches!(t,DirtyTrigger::Event(e) if *e==event))
        })
        .map(|m| m.name())
        .collect()
}
/// Control fact callers pass their verified actor and named models. The caller
/// must call this AFTER committing the control fact and BEFORE acknowledging
/// a permission-reducing mutation, then note the org enqueue after this commit.
/// Events conservatively invalidate: the user fence marks every named model's
/// row behind, so the next GET serves it `stale: true` and owes an immediate
/// recompute that drops the removed grant. Models that must not show a removed
/// grant even for that window re-key on the control revision (CONTROL_FACTS).
/// The content-plane XID certifies ordering even when facts live in control.
pub async fn mark_user(
    pool: &PgPool,
    account_id: Uuid,
    user_id: Uuid,
    models: &[&str],
) -> Result<(), Error> {
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped_without_account(pool).await?;
    sqlx::query("SELECT read_models_mark_user_dirty($1,$2,$3)")
        .bind(account_id)
        .bind(user_id)
        .bind(models)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

pub async fn mark_account(
    pool: &PgPool,
    account_id: Uuid,
    actors: &[Uuid],
    models: &[&str],
) -> Result<(), Error> {
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped_without_account(pool).await?;
    sqlx::query("SELECT read_models_mark_account_dirty($1,$2,$3)")
        .bind(account_id)
        .bind(actors)
        .bind(models)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

/// A control mutation must commit before calling this helper and must propagate
/// failure before acknowledging a reduction. Use the original mutation lane.
pub async fn connector_changed(
    state: &crate::AppState,
    tenant: &crate::tenancy::extractor::Tenant,
    account_id: Uuid,
    user_id: Uuid,
) -> Result<(), Error> {
    let pool = tenant.pool.as_ref().ok_or("content pool unavailable")?;
    mark_user(
        pool,
        account_id,
        user_id,
        &models_for(DirtyEvent::ConnectorGrantChanged),
    )
    .await?;
    match tenant.org_context() {
        Some(cx) => state.worker_activity.note_enqueue(&cx.org_id),
        None => state.worker_activity.note_consumer_enqueue(),
    }
    Ok(())
}

/// OAuth callbacks and background credential writers already know the signed
/// account but have no request Tenant. Resolve its existing content plane using
/// the same registry fingerprint checks as the tenant gate.
pub async fn connector_account_changed(
    state: &crate::AppState,
    account_id: Uuid,
    user_id: Uuid,
) -> Result<(), Error> {
    let _ = user_id;
    account_changed(
        state,
        account_id,
        None,
        &models_for(DirtyEvent::ConnectorGrantChanged),
    )
    .await
}
pub async fn account_changed(
    state: &crate::AppState,
    account_id: Uuid,
    user_id: Option<Uuid>,
    models: &[&str],
) -> Result<(), Error> {
    let control = state.control_db().ok_or("control pool unavailable")?;
    let org: Option<String> = sqlx::query_scalar("SELECT clerk_org_id FROM accounts WHERE id=$1")
        .bind(account_id)
        .fetch_optional(control)
        .await?
        .flatten();
    let (pool, wake) = if let Some(org) = org {
        match crate::tenancy::routing::lookup_by_org(control, &org).await? {
            Some(row) if row.status() == Some(crate::tenancy::routing::TenantStatus::Active) => {
                let runtime = state
                    .tenant_runtime
                    .as_ref()
                    .ok_or("tenant runtime unavailable for invalidation")?;
                (
                    runtime
                        .registry
                        .get_or_build(&org, account_id, &row.db_secret_ref)
                        .await?,
                    Some(org),
                )
            }
            // Suspended tenants cannot serve content. Personal orgs have no routing row.
            Some(_) => return Ok(()),
            None => (control.clone(), None),
        }
    } else {
        (control.clone(), None)
    };
    if let Some(user) = user_id {
        mark_user(&pool, account_id, user, models).await?;
    } else {
        let actors:Vec<Uuid>=sqlx::query_scalar("SELECT user_id FROM account_members WHERE account_id=$1 UNION SELECT owner_user_id FROM accounts WHERE id=$1")
            .bind(account_id).fetch_all(control).await?;
        mark_account(&pool, account_id, &actors, models).await?;
    }
    match wake {
        Some(org) => state.worker_activity.note_enqueue(&org),
        None => state.worker_activity.note_consumer_enqueue(),
    }
    Ok(())
}

/// The handler has already authorized and committed its control mutation.
/// Successful responses wait for registry-derived invalidation before acknowledgement.
pub(crate) async fn after_control_response(
    state:&crate::AppState,account:Uuid,event:DirtyEvent,response:axum::response::Response,
)->axum::response::Response{
    if !response.status().is_success(){return response}
    if let Err(error)=account_changed(state,account,None,&models_for(event)).await{
        tracing::warn!(error=%error,"committed control mutation invalidation failed");
        use axum::response::IntoResponse;
        return (axum::http::StatusCode::SERVICE_UNAVAILABLE,axum::Json(serde_json::json!({"error":"read model invalidation unavailable"}))).into_response();
    }
    response
}
