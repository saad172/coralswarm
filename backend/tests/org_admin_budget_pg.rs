//! The public control-plane admin wrapper keeps the caller's read budget.
use anyhow::{Context, Result};
use coralswarm_backend::{
    request_timing,
    route_class::RouteClass,
    store::StoreError,
    tenancy::scope::verify_org_admin,
    test_db::{migrated_database, Plane},
};
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    Executor,
};
use std::{
    str::FromStr,
    time::{Duration, Instant},
};
use uuid::Uuid;

#[tokio::test]
async fn org_admin_wrapper_preserves_page_budget_and_returns_connection_clean() -> Result<()> {
    let url = std::env::var("ORGT_TEST_BASE_URL")
        .context("ORGT_TEST_BASE_URL is required; no skipped wrapper proof")?;
    let base = PgConnectOptions::from_str(&url)?;
    anyhow::ensure!(matches!(base.get_host(), "localhost" | "127.0.0.1"));
    let db = migrated_database(&base, Plane::Control, "admin_budget").await;
    let opts = base.database(db.name());
    let owner = PgPoolOptions::new()
        .max_connections(2)
        .connect_with(opts.clone())
        .await?;
    let user: Uuid = sqlx::query_scalar("INSERT INTO users(email) VALUES($1) RETURNING id")
        .bind(format!("admin-budget-{}@test.local", Uuid::new_v4()))
        .fetch_one(&owner)
        .await?;
    let account: Uuid = sqlx::query_scalar(
        "INSERT INTO accounts(owner_user_id,clerk_org_id) VALUES($1,$2) RETURNING id",
    )
    .bind(user)
    .bind(format!("org_budget_{}", Uuid::new_v4()))
    .fetch_one(&owner)
    .await?;
    sqlx::query("INSERT INTO account_members(account_id,user_id,role,seat_tier_id) SELECT $1,$2,'owner',id FROM seat_tiers WHERE code='standard' AND is_active=true")
        .bind(account).bind(user).execute(&owner).await?;
    // Probe actual transaction settings through a restrictive SELECT policy
    // on the real authority table; no authority query/function is replaced.
    owner
        .execute("ALTER TABLE account_members ENABLE ROW LEVEL SECURITY")
        .await?;
    owner
        .execute("CREATE POLICY admin_budget_rows ON account_members FOR SELECT USING(true)")
        .await?;
    owner.execute("CREATE POLICY admin_budget_read_mode ON account_members AS RESTRICTIVE FOR SELECT USING(current_setting('transaction_read_only')='on' AND current_setting('statement_timeout')='1s' AND account_id=NULLIF(current_setting('app.account_id',true),'')::uuid AND user_id=NULLIF(current_setting('app.user_id',true),'')::uuid)").await?;
    let role = format!("admin_budget_{}", Uuid::new_v4().simple());
    let password = Uuid::new_v4().to_string();
    owner.execute(format!("CREATE ROLE {role} LOGIN INHERIT NOSUPERUSER NOBYPASSRLS NOCREATEDB NOCREATEROLE PASSWORD '{password}' IN ROLE coralswarm_app").as_str()).await?;
    let app = PgPoolOptions::new()
        .max_connections(1)
        .after_connect(|conn, _| {
            Box::pin(async move {
                sqlx::query("SET statement_timeout='9s'")
                    .execute(conn)
                    .await?;
                Ok(())
            })
        })
        .connect_with(opts.username(&role).password(&password))
        .await?;

    let outcome: Result<()> = async {
        let attrs: (bool, bool) = sqlx::query_as("SELECT rolsuper,rolbypassrls FROM pg_roles WHERE rolname=current_user")
            .fetch_one(&app).await?;
        anyhow::ensure!(attrs == (false, false));
        let mut blocker = owner.begin().await?;
        sqlx::query("LOCK TABLE account_members IN ACCESS EXCLUSIVE MODE").execute(&mut *blocker).await?;
        let start = Instant::now();
        let (result, timing) = request_timing::scope(Instant::now(), RouteClass::PageRead,
            tokio::time::timeout(Duration::from_millis(1800), verify_org_admin(&app, account, user))).await;
        blocker.rollback().await?;
        let result = result.context("admin wrapper exceeded the page statement budget")?;
        let error = match result {
            Err(error) => error,
            Ok(_) => anyhow::bail!("locked authority table unexpectedly returned an admin result"),
        };
        let code = match &error {
            StoreError::Db(inner) => inner.downcast_ref::<sqlx::Error>()
                .and_then(sqlx::Error::as_database_error).and_then(|e| e.code()).map(|c| c.to_string()),
            _ => None,
        };
        anyhow::ensure!(code.as_deref() == Some("57014"), "expected statement cancellation, got {error}");
        anyhow::ensure!(start.elapsed() >= Duration::from_millis(800));
        anyhow::ensure!(timing.page_read_signals().statement_timeouts == 1);
        anyhow::ensure!(timing.page_read_signals().acquire_timeouts == 0);
        let (proof, _) = request_timing::scope(Instant::now(), RouteClass::PageRead,
            verify_org_admin(&app, account, user)).await;
        anyhow::ensure!(proof?.is_some(), "real admin query must satisfy readonly/timeout/identity policy");
        let settings: (String, String, Option<String>, Option<String>) = sqlx::query_as("SELECT current_setting('transaction_read_only'),current_setting('statement_timeout'),NULLIF(current_setting('app.account_id',true),''),NULLIF(current_setting('app.user_id',true),'')")
            .fetch_one(&app).await?;
        anyhow::ensure!(settings == ("off".into(), "9s".into(), None, None), "transaction-local state leaked: {settings:?}");
        owner.execute("DROP POLICY admin_budget_read_mode ON account_members").await?;
        let (other, _) = request_timing::scope(Instant::now(), RouteClass::Other,
            verify_org_admin(&app, account, user)).await;
        anyhow::ensure!(other?.is_some(), "Other callers retain the same authority result");
        let (denied, _) = request_timing::scope(Instant::now(), RouteClass::PageRead,
            verify_org_admin(&app, account, Uuid::new_v4())).await;
        anyhow::ensure!(denied?.is_none(), "another actor cannot obtain the proof");
        Ok(())
    }.await;
    app.close().await;
    owner.execute(format!("DROP ROLE {role}").as_str()).await?;
    owner.close().await;
    outcome
}
