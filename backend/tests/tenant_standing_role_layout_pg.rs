use sqlx::{postgres::{PgConnectOptions, PgPoolOptions}, PgPool};
use std::{borrow::Cow, str::FromStr};

async fn receipt_acl(pool: &PgPool, role: &str) {
    let forbidden: bool = sqlx::query_scalar(
        "SELECT has_table_privilege($1,'public.corpus_capture_admissions','DELETE')
         OR pg_has_role($1,(SELECT pg_get_userbyid(relowner) FROM pg_class
            WHERE oid='public.corpus_capture_admissions'::regclass),'MEMBER')
         OR EXISTS(SELECT 1 FROM unnest(ARRAY['processed_at','ever_held','hold_basis']) c
            WHERE has_column_privilege($1,'public.corpus_capture_admissions',c,'INSERT')
               OR has_column_privilege($1,'public.corpus_capture_admissions',c,'UPDATE'))",
    ).bind(role).fetch_one(pool).await.unwrap();
    assert!(!forbidden, "protected receipt authority for {role}");
}

#[tokio::test]
async fn tenant_standing_migration_accepts_tenant_only_role_catalog() {
    let url = match std::env::var("ORGT_TEST_BASE_URL") {
        Ok(url) => url,
        Err(_) if std::env::var_os("REQUIRE_PG_SUITE").is_some() => panic!("ORGT_TEST_BASE_URL required"),
        Err(_) => return,
    };
    let options = PgConnectOptions::from_str(&url).unwrap();
    let admin = PgPoolOptions::new().max_connections(2).connect_with(options.clone()).await.unwrap();
    let foreign_exists: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='coralswarm_app')")
        .fetch_one(&admin).await.unwrap();
    assert!(!foreign_exists, "requires an isolated tenant-only PostgreSQL cluster");

    for foreign_present in [false, true] {
        let database = coralswarm_backend::test_db::TestDatabase::new(&options, "tenant_standing_roles");
        sqlx::query(&format!("CREATE DATABASE {database}")).execute(&admin).await.unwrap();
        let pool = PgPoolOptions::new().max_connections(2)
            .connect_with(options.clone().database(&database)).await.unwrap();
        let mut baseline = sqlx::migrate!("./migrations_tenant");
        baseline.migrations = Cow::Owned(baseline.iter().filter(|m| m.version <= 150).cloned().collect());
        baseline.run(&pool).await.unwrap();
        let head: i64 = sqlx::query_scalar("SELECT max(version) FROM _sqlx_migrations WHERE success")
            .fetch_one(&pool).await.unwrap();
        assert_eq!(head, 150);
        if foreign_present {
            sqlx::query("CREATE ROLE coralswarm_app NOSUPERUSER NOBYPASSRLS")
                .execute(&admin).await.unwrap();
            sqlx::query("GRANT INSERT,UPDATE,DELETE ON corpus_capture_admissions TO coralswarm_app")
                .execute(&pool).await.unwrap();
        }
        let migrator = sqlx::migrate!("./migrations_tenant");
        if !foreign_present {
            sqlx::query("ALTER ROLE coralswarm_tenant_app RENAME TO missing_tenant_role_probe")
                .execute(&admin).await.unwrap();
            let missing_role = migrator.run(&pool).await;
            sqlx::query("ALTER ROLE missing_tenant_role_probe RENAME TO coralswarm_tenant_app")
                .execute(&admin).await.unwrap();
            match missing_role.unwrap_err() {
                sqlx::migrate::MigrateError::ExecuteMigration(error, 151) => {
                    assert_eq!(error.as_database_error().unwrap().code().as_deref(), Some("42704"));
                    assert!(error.to_string().contains("coralswarm_tenant_app"));
                }
                error => panic!("unexpected missing tenant role failure: {error}"),
            }
            let rolled_back: bool = sqlx::query_scalar(
                "SELECT NOT EXISTS(SELECT 1 FROM _sqlx_migrations WHERE version=151)
                    AND NOT EXISTS(SELECT 1 FROM information_schema.columns
                        WHERE table_schema='public' AND table_name='corpus_capture_admissions'
                        AND column_name='hold_basis')",
            ).fetch_one(&pool).await.unwrap();
            assert!(rolled_back, "missing tenant role must roll back migration 151");
            eprintln!("tenant151 missing mandatory role rejected; sqlstate=42704; rollback=true");
        }
        if let Err(error) = migrator.run(&pool).await {
            let recorded: i64 = sqlx::query_scalar("SELECT count(*) FROM _sqlx_migrations WHERE version=151")
                .fetch_one(&pool).await.unwrap();
            assert_eq!(recorded, 0, "failed migration must not enter the SQLx ledger");
            eprintln!("tenant151 failure {error:?}; ledger_rows={recorded}; foreign_present={foreign_present}");
            panic!("tenant migration must apply with its own app role: {error}");
        }
        receipt_acl(&pool, "coralswarm_tenant_app").await;
        if foreign_present {
            receipt_acl(&pool, "coralswarm_app").await;
        }
        let writable: bool = sqlx::query_scalar(
            "SELECT has_column_privilege('coralswarm_tenant_app','corpus_capture_admissions','source_version_id','INSERT')
                AND has_column_privilege('coralswarm_tenant_app','corpus_capture_admissions','state','UPDATE')",
        ).fetch_one(&pool).await.unwrap();
        assert!(writable, "tenant receipt writes remain available");
        let checksum: Vec<u8> = sqlx::query_scalar("SELECT checksum FROM _sqlx_migrations WHERE version=151 AND success")
            .fetch_one(&pool).await.unwrap();
        assert_eq!(checksum, migrator.iter().find(|m| m.version == 151).unwrap().checksum.as_ref());
        migrator.run(&pool).await.unwrap();
        let recorded: i64 = sqlx::query_scalar("SELECT count(*) FROM _sqlx_migrations WHERE version=151 AND success")
            .fetch_one(&pool).await.unwrap();
        assert_eq!(recorded, 1);
        eprintln!("tenant151 applied; foreign_present={foreign_present}; checksum_matches=true; rerun_rows={recorded}; protected_acl=false");
        pool.close().await;
        sqlx::query(&format!("DROP DATABASE {database}")).execute(&admin).await.unwrap();
    }
    admin.close().await;
}
