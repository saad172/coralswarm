use coralswarm_backend::{corpus_enrichment, tenancy::scope::begin_account_scoped};
use sqlx::{postgres::{PgConnectOptions, PgPoolOptions}, PgPool};
use coralswarm_backend::test_db::TestDatabase;
use uuid::Uuid;

/// The guard drops the database; keep it alive for the whole test.
pub async fn isolated_database(options: &PgConnectOptions) -> (TestDatabase, PgPool) {
    assert!(matches!(options.get_host(), "127.0.0.1" | "localhost" | "::1"));
    let base = PgPoolOptions::new().max_connections(1).connect_with(options.clone()).await.unwrap();
    let database = TestDatabase::new(options, "managed_atoms");
    sqlx::query(&format!("CREATE DATABASE {database}")).execute(&base).await.unwrap();
    base.close().await;
    let owner = PgPoolOptions::new().max_connections(6)
        .connect_with(options.clone().database(&database)).await.unwrap();
    sqlx::migrate!("./migrations_control").run(&owner).await.unwrap();
    (database, owner)
}

pub async fn drop_database(owner: &PgPool) {
    let options = owner.connect_options().as_ref().clone();
    let database = options.get_database().unwrap();
    assert!(database.starts_with("cstest_managed_atoms_"));
    assert!(database.bytes().all(|byte| byte.is_ascii_alphanumeric() || byte == b'_'));
    owner.close().await;
    let admin = PgPoolOptions::new().max_connections(1)
        .connect_with(options.clone().database("postgres")).await.unwrap();
    sqlx::query(&format!("DROP DATABASE {database} WITH (FORCE)"))
        .execute(&admin).await.unwrap();
    admin.close().await;
}

pub async fn own_version(owner: &PgPool, version: Uuid) -> (Uuid, Uuid, bool) {
    assert!(matches!(owner.connect_options().get_host(), "127.0.0.1" | "localhost" | "::1"));
    let mut connection = owner.acquire().await.unwrap();
    sqlx::query("SET app.identity_serving_contract='composite-v1'")
        .execute(&mut *connection).await.unwrap();
    sqlx::raw_sql(include_str!("../../schema/identity-contract.sql"))
        .execute(&mut *connection).await.unwrap();
    drop(connection);
    let (graph, account, requester, item, profile): (Uuid, Uuid, Uuid, Uuid, String) = sqlx::query_as(
        "SELECT v.graph_id,g.account_id,a.owner_user_id,v.source_item_id,v.extraction_version \
         FROM source_versions v JOIN graphs g ON g.id=v.graph_id JOIN accounts a ON a.id=g.account_id \
         WHERE v.version_id=$1",
    ).bind(version).fetch_one(owner).await.unwrap();
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM corpus_enrichment_units WHERE graph_id=$1 \
         AND holding_id=$2 AND extraction_version=$3 AND execution_owner)",
    ).bind(graph).bind(version).bind(&profile).fetch_one(owner).await.unwrap();
    if exists { return (graph, account, false); }
    let mut tx = begin_account_scoped(owner, account).await.unwrap();
    assert!(corpus_enrichment::lifecycle_ready_tx(&mut tx).await.unwrap());
    let run: Uuid = sqlx::query_scalar(
        "INSERT INTO corpus_enrichment_runs(graph_id,account_id,requested_by,mode,status,\
         extraction_version,model,max_items,max_characters,max_model_calls,admitted_items) \
         VALUES($1,$2,$3,'execute','running',$4,$5,1,8000000,100,1) RETURNING run_id",
    ).bind(graph).bind(account).bind(requester).bind(&profile)
        .bind(coralswarm_backend::llm::model_config::DEFAULT_EXTRACT_MODEL)
        .fetch_one(&mut *tx).await.unwrap();
    sqlx::query(
        "INSERT INTO corpus_enrichment_units(run_id,graph_id,kind,holding_id,source_item_id,\
         extraction_version,state,original_availability,execution_owner) \
         VALUES($1,$2,'retained_version',$3,$4,$5,'queued','available',true)",
    ).bind(run).bind(graph).bind(version).bind(item).bind(&profile)
        .execute(&mut *tx).await.unwrap();
    assert!(corpus_enrichment::enqueue_admission_tx(&mut tx, graph, version, account).await.unwrap());
    tx.commit().await.unwrap();
    (graph, account, true)
}
