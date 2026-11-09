//! Drive actual refresh workers in tests; never compute in the HTTP request.
use coralswarm_backend::{
    read_models as rm,
    store::PostgresStore,
    tenancy::{scope::begin_tenant_scoped, TenantId},
    workers::{read_model_refresh, tenant_harness::WorkerTenant},
};
use sqlx::PgPool;
use std::{collections::BTreeMap, sync::Arc};
use uuid::Uuid;

pub async fn warm<M: rm::ReadModel>(
    pool: &PgPool,
    account: Uuid,
    user: Uuid,
    query: BTreeMap<String, String>,
) -> Result<(), rm::Error> {
    use rm::Params;
    if !M::enabled() {
        return Ok(());
    }
    let mut tx = begin_tenant_scoped(pool, account, user).await?;
    let p = M::Params::parse(Default::default(), query)?
        .normalize(&mut tx, user)
        .await?;
    let mut viewer = rm::Viewer::load(&mut tx, account, user).await?;
    if M::CONTROL_FACTS {
        viewer.certify_control(&mut tx).await?;
    }
    let scope = viewer.scope::<M>(p)?;
    tx.rollback().await?;
    rm::store::request_refresh::<M>(pool, &scope).await?;
    let t = WorkerTenant {
        tenant: TenantId::Consumer,
        pool: pool.clone(),
        store: Arc::new(PostgresStore::new(pool.clone())),
    };
    for _ in 0..32 {
        read_model_refresh::tick(&t, pool)
            .await
            .map_err(|e| -> rm::Error { e.into() })?;
        let mut tx = begin_tenant_scoped(pool, account, user).await?;
        if rm::store::read::<M>(&mut tx, &scope)
            .await?
            .computed_at
            .is_some()
        {
            return Ok(());
        }
    }
    Err("real worker did not fill model".into())
}

pub fn payload(value: serde_json::Value) -> serde_json::Value {
    value.get("payload").cloned().unwrap_or(value)
}

pub async fn stored_call(
    app: &axum::Router,
    pool: &PgPool,
    uri: &str,
    token: &str,
) -> (axum::http::StatusCode, serde_json::Value) {
    use axum::{
        body::Body,
        http::{Method, Request, StatusCode},
    };
    use tower::ServiceExt;
    let get = || {
        Request::builder()
            .uri(uri)
            .header("Authorization", format!("Bearer {token}"))
            .body(Body::empty())
            .unwrap()
    };
    let response = app.clone().oneshot(get()).await.unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap_or_default();
    if status != StatusCode::OK || value.get("model").is_none() {
        return (status, value);
    }
    let (path, query) = uri.split_once('?').unwrap_or((uri, ""));
    let refresh = format!(
        "{path}/refresh{}",
        if query.is_empty() {
            String::new()
        } else {
            format!("?{query}")
        }
    );
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri(refresh)
                .header("Authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let t = WorkerTenant {
        tenant: TenantId::Consumer,
        pool: pool.clone(),
        store: Arc::new(PostgresStore::new(pool.clone())),
    };
    for _ in 0..32 {
        read_model_refresh::tick(&t, pool).await.unwrap();
        let response = app.clone().oneshot(get()).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        if !value["computed_at"].is_null() && value["stale"] == false {
            return (StatusCode::OK, payload(value));
        }
    }
    panic!("worker did not fill {uri}")
}
