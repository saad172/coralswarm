//! SECURITY DEFINER routines resist pg_temp search_path hijacking
//! (control 0376 / tenant 0198, scripts/check_definer_search_path.sh).
//!
//! When pg_temp is not named in a definer's search_path, PostgreSQL searches
//! the caller's temporary schema first for relation and type names. For each
//! plane this suite, acting as that plane's app role:
//!
//! 1. Reproduces the escalation on the tree just before the hardening
//!    migration: a temporary DOMAIN `text` whose CHECK calls a pg_temp function
//!    runs that function as the definer's owner when
//!    `library_stage_unattached_upload(uuid)` initialises its `status TEXT`
//!    variable. Without this step the later assertions could pass vacuously.
//! 2. Applies the full tree and shows the same attack no longer reaches the
//!    owner.
//! 3. Shadows `graphs` with a temporary table holding an attacker row and shows
//!    the scope gate `resolve_graph_account_id(uuid)` still reads the real
//!    table, while the same unqualified query from the app session does see
//!    the shadow.
//! 4. Asserts no definer in the database lacks a trailing pg_temp, that the
//!    hardening SQL is a no-op when re-run, and that it preserves extra schemas
//!    and pins a definer that had no search_path at all.
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    Connection, Executor, PgConnection,
};
use std::{borrow::Cow, str::FromStr};
use uuid::Uuid;

const CONTROL_HARDENING: &str =
    include_str!("../migrations_control/0376_definer_search_path_pg_temp.sql");
const TENANT_HARDENING: &str =
    include_str!("../migrations_tenant/0198_definer_search_path_pg_temp.sql");

const UNPINNED: &str = "SELECT p.oid::regprocedure::text
    FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
    WHERE p.prosecdef AND n.nspname NOT IN ('pg_catalog','information_schema')
      AND NOT EXISTS (SELECT 1 FROM pg_depend d WHERE d.classid='pg_proc'::regclass AND d.objid=p.oid AND d.deptype='e')
      AND NOT EXISTS (SELECT 1 FROM unnest(p.proconfig) c WHERE c ~ '^search_path=(.*,)?\\s*\"?pg_temp\"?\\s*$')
    ORDER BY 1";

/// A session as `role` that has planted a temporary `text` domain whose CHECK
/// raises with the effective user. Returns the error text of calling the
/// definer, or None when the call succeeded without the probe running.
async fn type_shadow_attack(options: &PgConnectOptions, role: &str) -> Option<String> {
    let mut app = PgConnection::connect_with(options).await.unwrap();
    app.execute(format!("SET ROLE {role}").as_str()).await.unwrap();
    app.execute(
        "CREATE FUNCTION pg_temp.escalation_probe(v pg_catalog.text) RETURNS boolean
         LANGUAGE plpgsql AS $$BEGIN
           RAISE EXCEPTION 'definer escalation: current_user=%', current_user;
         END$$",
    )
    .await
    .unwrap();
    app.execute("CREATE DOMAIN pg_temp.text AS pg_catalog.text CHECK (pg_temp.escalation_probe(VALUE))")
        .await
        .unwrap();
    let outcome = sqlx::query_scalar::<_, bool>("SELECT library_stage_unattached_upload($1)")
        .bind(Uuid::new_v4())
        .fetch_one(&mut app)
        .await;
    app.close().await.unwrap();
    match outcome {
        Ok(staged) => {
            assert!(!staged, "no upload row exists, so nothing can be staged");
            None
        }
        Err(error) => Some(error.to_string()),
    }
}

async fn plane(
    admin: &mut PgConnection,
    options: &PgConnectOptions,
    tree: &str,
    app_role: &str,
    hardening_version: i64,
    hardening_sql: &str,
) {
    let database = coralswarm_backend::test_db::TestDatabase::new(options, &format!("definer_sp_{tree}"));
    admin.execute(format!("CREATE DATABASE {database}").as_str()).await.unwrap();
    let db_options = options.clone().database(&database);
    let owner = PgPoolOptions::new().max_connections(2).connect_with(db_options.clone()).await.unwrap();

    let full = if tree == "control" {
        sqlx::migrate!("./migrations_control")
    } else {
        sqlx::migrate!("./migrations_tenant")
    };
    assert!(
        full.iter().any(|m| m.version == hardening_version),
        "{tree}: hardening migration {hardening_version} is in the tree"
    );
    let mut before = if tree == "control" {
        sqlx::migrate!("./migrations_control")
    } else {
        sqlx::migrate!("./migrations_tenant")
    };
    before.migrations =
        Cow::Owned(before.iter().filter(|m| m.version < hardening_version).cloned().collect());
    before.run(&owner).await.unwrap();

    // 1. The vulnerability is real on the pre-hardening tree.
    let escalated = type_shadow_attack(&db_options, app_role)
        .await
        .unwrap_or_else(|| panic!("{tree}: pre-hardening attack should have reached the probe"));
    assert!(escalated.contains("definer escalation"), "{tree}: {escalated}");
    assert!(
        !escalated.contains(&format!("current_user={app_role}")),
        "{tree}: probe must have run as the definer owner, not the app role: {escalated}"
    );
    eprintln!("{tree}: before {hardening_version}, {app_role} reached the definer owner: {escalated}");

    // 2. The hardened tree refuses the same attack.
    full.run(&owner).await.unwrap();
    assert_eq!(
        type_shadow_attack(&db_options, app_role).await,
        None,
        "{tree}: hardened definer must resolve text to pg_catalog.text"
    );

    // 3. A temporary table shadowing a table the scope gate reads.
    let graph = Uuid::new_v4();
    let attacker_account = Uuid::new_v4();
    let mut app = PgConnection::connect_with(&db_options).await.unwrap();
    app.execute(format!("SET ROLE {app_role}").as_str()).await.unwrap();
    app.execute("CREATE TEMP TABLE graphs (id uuid, account_id uuid)").await.unwrap();
    sqlx::query("INSERT INTO pg_temp.graphs VALUES ($1, $2)")
        .bind(graph)
        .bind(attacker_account)
        .execute(&mut app)
        .await
        .unwrap();
    let session_sees: Option<Uuid> = sqlx::query_scalar("SELECT account_id FROM graphs WHERE id = $1")
        .bind(graph)
        .fetch_optional(&mut app)
        .await
        .unwrap();
    assert_eq!(session_sees, Some(attacker_account), "{tree}: the shadow is live in the app session");
    let resolved: Option<Uuid> = sqlx::query_scalar("SELECT resolve_graph_account_id($1)")
        .bind(graph)
        .fetch_one(&mut app)
        .await
        .unwrap();
    assert_eq!(resolved, None, "{tree}: the definer must read public.graphs, not the temp shadow");
    app.close().await.unwrap();

    // 4. Catalog-wide: nothing left unpinned.
    let unpinned: Vec<String> = sqlx::query_scalar(UNPINNED).fetch_all(&owner).await.unwrap();
    assert!(unpinned.is_empty(), "{tree}: definers without trailing pg_temp: {unpinned:?}");
    let pinned: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pg_proc WHERE prosecdef AND pronamespace = 'public'::regnamespace",
    )
    .fetch_one(&owner)
    .await
    .unwrap();

    // Re-running the hardening SQL changes nothing on a hardened database ...
    let snapshot = "SELECT string_agg(oid::regprocedure::text || '=' || coalesce(array_to_string(proconfig, ';'), ''), ' ' ORDER BY oid) FROM pg_proc WHERE prosecdef";
    let first: String = sqlx::query_scalar(snapshot).fetch_one(&owner).await.unwrap();
    sqlx::raw_sql(hardening_sql).execute(&owner).await.unwrap();
    let second: String = sqlx::query_scalar(snapshot).fetch_one(&owner).await.unwrap();
    assert_eq!(first, second, "{tree}: hardening must be idempotent");

    // ... keeps an extra schema in place with pg_temp appended, and pins a
    // definer that had no search_path.
    sqlx::raw_sql(
        "CREATE SCHEMA probe_ext;
         CREATE FUNCTION public.definer_probe_extra() RETURNS int LANGUAGE sql SECURITY DEFINER
           SET search_path = pg_catalog, probe_ext, public AS 'SELECT 1';
         CREATE FUNCTION public.definer_probe_temp_first() RETURNS int LANGUAGE sql SECURITY DEFINER
           SET search_path = pg_temp, \"$user\", public AS 'SELECT 1';
         CREATE FUNCTION public.definer_probe_unset() RETURNS int LANGUAGE sql SECURITY DEFINER AS 'SELECT 1';",
    )
    .execute(&owner)
    .await
    .unwrap();
    sqlx::raw_sql(hardening_sql).execute(&owner).await.unwrap();
    for (function, expected) in [
        ("definer_probe_extra", "search_path=pg_catalog, probe_ext, public, pg_temp"),
        ("definer_probe_temp_first", "search_path=\"$user\", public, pg_temp"),
        ("definer_probe_unset", "search_path=pg_catalog, public, pg_temp"),
    ] {
        let config: Vec<String> =
            sqlx::query_scalar("SELECT proconfig FROM pg_proc WHERE proname = $1")
                .bind(function)
                .fetch_one(&owner)
                .await
                .unwrap();
        assert_eq!(config, vec![expected.to_string()], "{tree}: {function}");
    }
    eprintln!("{tree}: {pinned} public definers pinned; attack refused; temp-table shadow ignored; rerun idempotent");

    owner.close().await;
    admin.execute(format!("DROP DATABASE {database} WITH (FORCE)").as_str()).await.unwrap();
}

#[tokio::test]
async fn definers_resist_pg_temp_search_path_hijack_on_both_planes() {
    let url = match std::env::var("ORGT_TEST_BASE_URL") {
        Ok(url) => url,
        Err(_) if std::env::var_os("REQUIRE_PG_SUITE").is_some() => panic!("ORGT_TEST_BASE_URL required"),
        Err(_) => {
            eprintln!("skip: ORGT_TEST_BASE_URL unset");
            return;
        }
    };
    // The two planes ship one byte-identical hardening body.
    let body = |sql: &str| sql.lines().skip(1).collect::<Vec<_>>().join("\n");
    assert_eq!(body(CONTROL_HARDENING), body(TENANT_HARDENING), "control 0376 and tenant 0198 must match");

    let options = PgConnectOptions::from_str(&url).unwrap();
    let mut admin = PgConnection::connect_with(&options).await.unwrap();
    plane(&mut admin, &options, "control", "coralswarm_app", 375, CONTROL_HARDENING).await;
    plane(&mut admin, &options, "tenant", "coralswarm_tenant_app", 197, TENANT_HARDENING).await;
    admin.close().await.unwrap();
}
