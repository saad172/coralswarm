use sqlx::{
    postgres::{PgArguments, PgQueryResult},
    query::Query,
    PgPool, Postgres,
};
use uuid::Uuid;

pub async fn execute<'q>(
    pool: &PgPool,
    account_id: Uuid,
    query: Query<'q, Postgres, PgArguments>,
) -> Result<PgQueryResult, sqlx::Error> {
    let mut tx = coralswarm_backend::tenancy::scope::begin_account_scoped(pool, account_id)
        .await
        .map_err(|error| sqlx::Error::Protocol(error.to_string()))?;
    coralswarm_backend::billing::renewal::lock_period_rotation(&mut tx, account_id).await?;
    let result = query.execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(result)
}
