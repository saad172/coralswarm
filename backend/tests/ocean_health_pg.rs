//! COR-96: `compute_on_pool` reads the window inside a transaction, closes
//! it, and only then scans titles. This proves the split still produces the
//! window's metrics against the real schema.
//!
//! COR-75: the consumer idle tick schedules due oceans on the RLS-enforcing
//! app role, on a pooled connection an earlier account-scoped transaction
//! already used.
//!
//!   REQUIRE_PG_SUITE=1 RELINFER_TEST_DATABASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test ocean_health_pg -- --test-threads=1

mod support;

use std::str::FromStr;
use std::sync::Arc;

use chrono::{Duration, Utc};
use coralswarm_backend::ocean_health::compute_on_pool;
use coralswarm_backend::store::{PostgresStore, Store};
use coralswarm_backend::tenancy::scope::{begin_account_scoped, begin_tenant_scoped};
use coralswarm_backend::tenancy::TenantId;
use coralswarm_backend::workers::ocean_health::tick;
use coralswarm_backend::workers::tenant_harness::WorkerTenant;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::PgPool;
use uuid::Uuid;

async fn fresh_db(migrations: &sqlx::migrate::Migrator) -> Option<(PgPool, PgPool, coralswarm_backend::test_db::TestDatabase)> {
    let Ok(url) = std::env::var("RELINFER_TEST_DATABASE_URL") else {
        assert!(
            std::env::var_os("REQUIRE_PG_SUITE").is_none(),
            "RELINFER_TEST_DATABASE_URL required"
        );
        eprintln!("skipping ocean_health_pg: set RELINFER_TEST_DATABASE_URL");
        return None;
    };
    let opts = PgConnectOptions::from_str(&url).unwrap();
    assert!(
        matches!(opts.get_host(), "127.0.0.1" | "localhost" | "::1"),
        "test database must be local"
    );
    let base = PgPoolOptions::new()
        .max_connections(2)
        .connect_with(opts.clone())
        .await
        .unwrap();
    let db = coralswarm_backend::test_db::TestDatabase::new(&opts, "ocean_health");
    sqlx::query(&format!("CREATE DATABASE {db}"))
        .execute(&base)
        .await
        .unwrap();
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(opts.database(&db))
        .await
        .unwrap();
    migrations.run(&pool).await.unwrap();
    Some((base, pool, db))
}

#[tokio::test]
async fn compute_on_pool_counts_new_titles_containing_older_ones() {
    let Some((base, pool, db)) = fresh_db(&sqlx::migrate!("./migrations_control")).await else {
        return;
    };
    let suffix = Uuid::new_v4();
    let user: Uuid =
        sqlx::query_scalar("INSERT INTO users(email, clerk_user_id) VALUES($1, $2) RETURNING id")
            .bind(format!("ocean-health-{suffix}@test.local"))
            .bind(format!("ocean-health-{suffix}"))
            .fetch_one(&pool)
            .await
            .unwrap();
    let account: Uuid =
        sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES($1) RETURNING id")
            .bind(user)
            .fetch_one(&pool)
            .await
            .unwrap();
    let graph: Uuid =
        sqlx::query_scalar("INSERT INTO graphs(account_id, name) VALUES($1, 'health') RETURNING id")
            .bind(account)
            .fetch_one(&pool)
            .await
            .unwrap();

    let now = Utc::now();
    let old = now - Duration::days(30);
    let fresh = now - Duration::hours(1);
    let rows = [
        ("Alex Example", old, true),
        ("React", old, false),
        ("CoralSwarm", old, false),
        ("React 19 upgrade", fresh, false),
        ("Alex Example coding session", fresh, false),
        ("coralswarm-connect plugin", fresh, false),
        ("Postgres tuning", fresh, false),
    ];
    for (title, created_at, is_self) in rows {
        let id: Uuid = sqlx::query_scalar(
            "INSERT INTO nodes(node_id, graph_id, type, title, created_at) \
             VALUES (gen_random_uuid(), $1, 'entity', $2, $3) RETURNING node_id",
        )
        .bind(graph)
        .bind(title)
        .bind(created_at)
        .fetch_one(&pool)
        .await
        .unwrap();
        if is_self {
            sqlx::query("UPDATE graphs SET self_node_id = $1 WHERE id = $2")
                .bind(id)
                .bind(graph)
                .execute(&pool)
                .await
                .unwrap();
        }
    }

    let m = compute_on_pool(&pool, graph, now - Duration::days(7), now)
        .await
        .unwrap();
    assert_eq!(m.new_reefs, 4);
    assert_eq!(m.entity_reefs, 7);
    assert_eq!(m.contain_hits, 1, "only `React 19 upgrade` contains an older non-self title");

    pool.close().await;
    sqlx::query(&format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)"))
        .execute(&base)
        .await
        .unwrap();
}

async fn account_with_graph(pool: &PgPool) -> (Uuid, Uuid) {
    let tag = Uuid::new_v4();
    let user: Uuid =
        sqlx::query_scalar("INSERT INTO users(email, clerk_user_id) VALUES($1, $2) RETURNING id")
            .bind(format!("cor75-{tag}@test.local"))
            .bind(format!("cor75-{tag}"))
            .fetch_one(pool)
            .await
            .unwrap();
    let account: Uuid =
        sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES($1) RETURNING id")
            .bind(user)
            .fetch_one(pool)
            .await
            .unwrap();
    let graph = add_graph(pool, account).await;
    (account, graph)
}

async fn add_graph(pool: &PgPool, account: Uuid) -> Uuid {
    sqlx::query_scalar("INSERT INTO graphs(account_id, name) VALUES($1, 'health') RETURNING id")
        .bind(account)
        .fetch_one(pool)
        .await
        .unwrap()
}

#[tokio::test]
async fn consumer_idle_tick_schedules_every_accounts_due_oceans_on_the_app_role() {
    let Some((base, pool, db)) = fresh_db(&sqlx::migrate!("./migrations_control")).await else {
        return;
    };
    support::app_role::set_app_role_password_with_retry(&pool).await;
    let url = std::env::var("RELINFER_TEST_DATABASE_URL").unwrap();
    let app = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(
            PgConnectOptions::from_str(&url)
                .unwrap()
                .database(&db)
                .username("coralswarm_app")
                .password(support::app_role::APP_ROLE_TEST_PASSWORD),
        )
        .await
        .unwrap();

    let (a, graph_a) = account_with_graph(&pool).await;
    let (b, graph_b) = account_with_graph(&pool).await;
    let checked = add_graph(&pool, a).await;
    sqlx::query(
        "INSERT INTO jobs(kind, payload, status, account_id, finished_at) \
         VALUES('OceanHealth', jsonb_build_object('graph_id', $1::text), 'done', $2, now())",
    )
    .bind(checked)
    .bind(a)
    .execute(&pool)
    .await
    .unwrap();

    // Leaves `app.account_id` defined but empty on the only app connection,
    // the state a production pooled connection is in between requests.
    begin_account_scoped(&app, a).await.unwrap().rollback().await.unwrap();

    let worker = WorkerTenant {
        tenant: TenantId::Consumer,
        pool: app.clone(),
        store: Arc::new(PostgresStore::new(app.clone())) as Arc<dyn Store>,
    };
    let ticked = tick(&worker).await.expect("idle tick schedules due oceans");
    assert_eq!(ticked.processed, 2);

    let mut scheduled: Vec<(Uuid, Option<Uuid>)> = sqlx::query_as(
        "SELECT (payload->>'graph_id')::uuid, account_id FROM jobs \
          WHERE kind = 'OceanHealth' AND status = 'pending' ORDER BY 1",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    scheduled.sort();
    let mut expected = vec![(graph_a, Some(a)), (graph_b, Some(b))];
    expected.sort();
    assert_eq!(scheduled, expected, "each due ocean is queued once under its own account");

    app.close().await;
    pool.close().await;
    sqlx::query(&format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)"))
        .execute(&base)
        .await
        .unwrap();
}

#[tokio::test]
async fn tenant_idle_tick_schedules_all_ocean_kinds_without_leaking_system_scope() {
    let Some((base, pool, db)) = fresh_db(&sqlx::migrate!("./migrations_tenant")).await else {
        return;
    };
    support::tenant_role::set_app_role_password_with_retry(&pool).await;
    let url = std::env::var("RELINFER_TEST_DATABASE_URL").unwrap();
    let app = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(
            PgConnectOptions::from_str(&url)
                .unwrap()
                .database(&db)
                .username("coralswarm_tenant_app")
                .password(support::tenant_role::APP_ROLE_TEST_PASSWORD),
        )
        .await
        .unwrap();
    let identity: (String, String, bool, bool, i64) = sqlx::query_as(
        "SELECT current_user::text, session_user::text, rolsuper, rolbypassrls, \
                (SELECT count(*) FROM pg_tables WHERE schemaname='public' AND tableowner=current_user) \
           FROM pg_roles WHERE rolname=current_user",
    )
    .fetch_one(&app)
    .await
    .unwrap();
    assert_eq!(identity, ("coralswarm_tenant_app".into(), "coralswarm_tenant_app".into(), false, false, 0));
    assert!(sqlx::query_scalar::<_, bool>(
        "SELECT relrowsecurity FROM pg_class WHERE oid='public.graphs'::regclass",
    ).fetch_one(&app).await.unwrap());

    let account = Uuid::new_v4();
    let member = Uuid::new_v4();
    let mut due = Vec::new();
    let mut ambient = Vec::new();
    for kind in ["personal", "org", "member", "team"] {
        let graph: Uuid = sqlx::query_scalar(
            "INSERT INTO graphs(account_id, name, kind, owner_user_id) \
             VALUES($1, $2, $2, $3) RETURNING id",
        )
        .bind(account)
        .bind(kind)
        .bind((kind == "member").then_some(member))
        .fetch_one(&pool)
        .await
        .unwrap();
        due.push((graph, Some(account)));
        if matches!(kind, "personal" | "org") {
            ambient.push(graph);
        }
        if kind == "team" {
            let team: Uuid = sqlx::query_scalar(
                "INSERT INTO teams(account_id, graph_id, name, created_by) \
                 VALUES($1, $2, 'health', $3) RETURNING id",
            ).bind(account).bind(graph).bind(member).fetch_one(&pool).await.unwrap();
            sqlx::query("INSERT INTO team_members(team_id, user_id, role) VALUES($1, $2, 'admin')")
                .bind(team).bind(member).execute(&pool).await.unwrap();
        }
    }
    let mut already_pending = None;
    for (kind, status, fresh) in [
        ("member", "done", true),
        ("team", "running", false),
        ("personal", "pending", false),
        ("personal", "done", false),
    ] {
        let graph: Uuid = sqlx::query_scalar(
            "INSERT INTO graphs(account_id, name, kind, owner_user_id) \
             VALUES($1, 'checked health', $2, $3) RETURNING id",
        ).bind(account).bind(kind).bind((kind == "member").then_some(member))
            .fetch_one(&pool).await.unwrap();
        sqlx::query(
            "INSERT INTO jobs(kind, payload, status, account_id, idempotency_key, run_after, finished_at) \
             VALUES('OceanHealth', jsonb_build_object('graph_id', $1::text), $2::text::job_status, \
                    $3, $4, now() + interval '1 day', \
                    CASE WHEN $2::text='done' THEN \
                        CASE WHEN $5 THEN now() ELSE now() - interval '7 hours' END END)",
        ).bind(graph).bind(status).bind(account).bind(format!("ocean-health:{graph}"))
            .bind(fresh).execute(&pool).await.unwrap();
        if matches!(kind, "personal" | "org") {
            ambient.push(graph);
        }
        if status == "pending" {
            already_pending = Some((graph, Some(account)));
        } else if status == "done" && !fresh {
            due.push((graph, Some(account)));
        }
    }
    ambient.sort();
    begin_tenant_scoped(&app, account, member).await.unwrap().rollback().await.unwrap();
    let visible: Vec<Uuid> = sqlx::query_scalar("SELECT id FROM graphs ORDER BY id")
        .fetch_all(&app).await.unwrap();
    assert_eq!(visible, ambient, "the unscoped app connection hides member and team oceans");

    let worker = WorkerTenant {
        tenant: TenantId::Org("org_ocean_health_regression".into()),
        pool: app.clone(),
        store: Arc::new(PostgresStore::new(app.clone())) as Arc<dyn Store>,
    };
    let ticked = tick(&worker).await.expect("tenant idle tick schedules due oceans");
    let mut scheduled: Vec<(Uuid, Option<Uuid>)> = sqlx::query_as(
        "SELECT (payload->>'graph_id')::uuid, account_id FROM jobs \
          WHERE kind='OceanHealth' AND status='pending' ORDER BY 1",
    ).fetch_all(&pool).await.unwrap();
    let mut expected = due.clone();
    expected.push(already_pending.unwrap());
    expected.sort();
    scheduled.sort();
    assert_eq!(scheduled, expected, "all due ocean kinds are queued once under their account");
    assert_eq!(ticked.processed, due.len());

    sqlx::query("UPDATE jobs SET run_after=now()+interval '1 day' WHERE kind='OceanHealth' AND status='pending'")
        .execute(&pool).await.unwrap();
    let before: Vec<(Uuid, Uuid, Option<Uuid>, String)> = sqlx::query_as(
        "SELECT id, (payload->>'graph_id')::uuid, account_id, status::text FROM jobs ORDER BY id",
    ).fetch_all(&pool).await.unwrap();
    assert_eq!(tick(&worker).await.expect("repeated idle tick").processed, 0);
    let after: Vec<(Uuid, Uuid, Option<Uuid>, String)> = sqlx::query_as(
        "SELECT id, (payload->>'graph_id')::uuid, account_id, status::text FROM jobs ORDER BY id",
    ).fetch_all(&pool).await.unwrap();
    assert_eq!(after, before, "pending, running, and recent-done checks prevent duplicate jobs");
    assert!(!sqlx::query_scalar::<_, bool>("SELECT tenant_rls_is_system()")
        .fetch_one(&app).await.unwrap());
    let visible: Vec<Uuid> = sqlx::query_scalar("SELECT id FROM graphs ORDER BY id")
        .fetch_all(&app).await.unwrap();
    assert_eq!(visible, ambient, "the scheduler's system identity is transaction-local");

    app.close().await;
    pool.close().await;
    sqlx::query(&format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)"))
        .execute(&base).await.unwrap();
}
