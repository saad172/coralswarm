//! Real post-fetch ingestion and SQL lock barriers; no provider transport fake.
use super::*;
use sqlx::{postgres::PgPoolOptions, PgPool};
use uuid::Uuid;

fn item(kind: &str, id: &str) -> NormalizedItem {
    NormalizedItem {
        source_type: kind.into(),
        source_id: id.into(),
        content: "Teams admitted content".into(),
        participants: vec![],
        timestamp: chrono::Utc::now(),
        scope: None,
        thread_id: None,
        channel_id: Some("C".into()),
        author_id: None,
        author_name: None,
    }
}

async fn wait_for_lock(pool: &PgPool, relation: &str) {
    tokio::time::timeout(std::time::Duration::from_secs(20), async {
        loop {
            let blocked: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_locks WHERE relation=to_regclass($1) AND NOT granted)")
                .bind(relation).fetch_one(pool).await.unwrap();
            if blocked { break; }
            tokio::task::yield_now().await;
        }
    }).await.expect("production ingestion must reach the durable write barrier");
}

async fn pause(pool: &PgPool, account: Uuid, member: Uuid, source: &str) {
    sqlx::query("INSERT INTO connector_sources(account_id,owner_user_id,provider,source,enabled,paused_at) VALUES ($1,$2,'microsoft',$3,true,now()) ON CONFLICT (account_id,owner_user_id,provider,source) DO UPDATE SET paused_at=now()")
        .bind(account).bind(member).bind(source).execute(pool).await.unwrap();
}

#[tokio::test]
async fn teams_source_admission_orders_pause_with_real_capture_and_enqueue_pg() {
    let Ok(base) = std::env::var("ORGT_TEST_BASE_URL") else {
        eprintln!("SKIP: ORGT_TEST_BASE_URL required");
        return;
    };
    let options: sqlx::postgres::PgConnectOptions = base.parse().unwrap();
    let db = crate::test_db::migrated_database(
        &options,
        crate::test_db::Plane::Control,
        "teams_admission",
    )
    .await;
    let pool = PgPoolOptions::new()
        .max_connections(12)
        .connect_with(options.database(db.name()))
        .await
        .unwrap();
    let (account, member) = crate::billing::test_support::seed_personal_account(&pool).await;
    let graph: Uuid = sqlx::query_scalar("INSERT INTO graphs(account_id,name,kind,is_primary) VALUES ($1,'My Ocean','personal',true) RETURNING id")
        .bind(account).fetch_one(&pool).await.unwrap();
    let _env_guard = crate::workers::reembed::MASTER_KEK_HEX_ENV_LOCK
        .lock()
        .await;
    let old_kek = std::env::var_os("MASTER_KEK_HEX");
    unsafe {
        std::env::set_var("MASTER_KEK_HEX", "1".repeat(64));
    }
    let kek = coralswarm_core::kek::load_kek().unwrap();
    let wrapped =
        coralswarm_core::kek::wrap_dek(&kek, &coralswarm_core::kek::generate_dek()).unwrap();
    sqlx::query("UPDATE accounts SET dek_wrapped=$1 WHERE id=$2")
        .bind(wrapped)
        .bind(account)
        .execute(&pool)
        .await
        .unwrap();
    let state = Arc::new(AppState {
        store: Some(Arc::new(crate::store::PostgresStore::new(pool.clone()))),
        ..crate::test_app_state_owned_with_db(pool.clone(), None)
    });

    for (source, kind, watch_kind) in [
        ("teams_channels", "teams_channel", "channel"),
        ("teams_chats", "teams_chat", "chat"),
    ] {
        sqlx::query("INSERT INTO teams_watched_resources(account_id,user_id,graph_id,kind,channel_id,chat_id,auth_mode) VALUES ($1,$2,$3,$4,$5,$6,'delegated')")
            .bind(account).bind(member).bind(graph).bind(watch_kind)
            .bind(if watch_kind=="channel" {"C"} else {""}).bind(if watch_kind=="chat" {"C"} else {""})
            .execute(&pool).await.unwrap();

        // Legacy absence means enabled, but watch authorization remains required.
        assert!(
            ingest_message_with(
                &state,
                account,
                member,
                graph,
                item(kind, &format!("{kind}-baseline")),
                None,
                &GraphIngestTarget::Sync
            )
            .await.unwrap()
        );
        let baseline: i64 =
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM source_versions WHERE graph_id=$1")
                .bind(graph)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert!(baseline > 0, "fixture must really capture durable content");

        // Hold a fetched response, acknowledge pause, then release it into the
        // same production boundary used by continuation and webhook paths.
        let release = Arc::new(tokio::sync::Barrier::new(2));
        let response_state = state.clone();
        let response_release = release.clone();
        let response = item(kind, &format!("{kind}-late"));
        let held = tokio::spawn(async move {
            response_release.wait().await;
            ingest_message_with(
                &response_state,
                account,
                member,
                graph,
                response,
                None,
                &GraphIngestTarget::Sync,
            )
            .await.unwrap()
        });
        pause(&pool, account, member, source).await;
        release.wait().await;
        assert!(!held.await.unwrap());
        let after: i64 =
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM source_versions WHERE graph_id=$1")
                .bind(graph)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(
            after, baseline,
            "no durable content after pause acknowledgment"
        );
        assert!(
            !ingest_message_with(
                &state,
                account,
                member,
                graph,
                item(kind, "late-enqueue"),
                None,
                &GraphIngestTarget::Enqueue {
                    pool: pool.clone(),
                    org_id: "test".into()
                }
            )
            .await.unwrap()
        );

        // An admitted real capture must finish BEFORE pause acknowledges.
        sqlx::query(
            "UPDATE connector_sources SET paused_at=NULL WHERE account_id=$1 AND source=$2",
        )
        .bind(account)
        .bind(source)
        .execute(&pool)
        .await
        .unwrap();
        for enqueue in [false, true] {
            let mut barrier = pool.begin().await.unwrap();
            let relation = if enqueue { "jobs" } else { "source_versions" };
            sqlx::query(&format!("LOCK TABLE {relation} IN SHARE MODE"))
                .execute(&mut *barrier)
                .await
                .unwrap();
            let capture_state = state.clone();
            let capture_pool = pool.clone();
            let response = item(kind, &format!("{kind}-admitted-{enqueue}"));
            let capture = tokio::spawn(async move {
                let target = if enqueue {
                    GraphIngestTarget::Enqueue {
                        pool: capture_pool,
                        org_id: "test".into(),
                    }
                } else {
                    GraphIngestTarget::Sync
                };
                ingest_message_with(
                    &capture_state,
                    account,
                    member,
                    graph,
                    response,
                    None,
                    &target,
                )
                .await.unwrap()
            });
            wait_for_lock(&pool, relation).await;
            let pause_pool = pool.clone();
            let action = tokio::spawn(async move {
                pause(&pause_pool, account, member, source).await;
            });
            // Observe the actual blocked control mutation, without timing sleeps.
            tokio::time::timeout(std::time::Duration::from_secs(20), async {
                loop {
                    let blocked: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE 'INSERT INTO connector_sources%')")
                        .fetch_one(&pool).await.unwrap();
                    if blocked { break; }
                    tokio::task::yield_now().await;
                }
            }).await.expect("pause must wait behind admitted capture/enqueue");
            assert!(!action.is_finished());
            barrier.rollback().await.unwrap();
            assert!(
                tokio::time::timeout(std::time::Duration::from_secs(30), capture)
                    .await
                    .unwrap()
                    .unwrap(),
                "admitted write must succeed without deadlock"
            );
            tokio::time::timeout(std::time::Duration::from_secs(10), action)
                .await
                .unwrap()
                .unwrap();
            let durable: i64 = if enqueue {
                sqlx::query_scalar(
                    "SELECT count(*) FROM jobs WHERE account_id=$1 AND kind='GraphMessageIngest'",
                )
                .bind(account)
                .fetch_one(&pool)
                .await
                .unwrap()
            } else {
                sqlx::query_scalar::<_, i64>("SELECT count(*) FROM source_versions WHERE graph_id=$1")
                    .bind(graph)
                    .fetch_one(&pool)
                    .await
                    .unwrap()
                    - baseline
            };
            assert!(
                durable > 0,
                "admitted production operation must persist before acknowledgment"
            );

            sqlx::query(
                "UPDATE connector_sources SET paused_at=NULL WHERE account_id=$1 AND source=$2",
            )
            .bind(account)
            .bind(source)
            .execute(&pool)
            .await
            .unwrap();
        }
        sqlx::query("UPDATE connector_sources SET enabled=false WHERE account_id=$1 AND source=$2")
            .bind(account)
            .bind(source)
            .execute(&pool)
            .await
            .unwrap();
        assert!(
            !ingest_message_with(
                &state,
                account,
                member,
                graph,
                item(kind, "disabled"),
                None,
                &GraphIngestTarget::Sync
            )
            .await.unwrap()
        );
        sqlx::query("UPDATE connector_sources SET enabled=true WHERE account_id=$1 AND source=$2")
            .bind(account)
            .bind(source)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM teams_watched_resources WHERE account_id=$1 AND kind=$2")
            .bind(account)
            .bind(watch_kind)
            .execute(&pool)
            .await
            .unwrap();
        assert!(
            !ingest_message_with(
                &state,
                account,
                member,
                graph,
                item(kind, "no-watch"),
                None,
                &GraphIngestTarget::Sync
            )
            .await.unwrap()
        );
    }
    unsafe {
        if let Some(v) = old_kek {
            std::env::set_var("MASTER_KEK_HEX", v);
        } else {
            std::env::remove_var("MASTER_KEK_HEX");
        }
    }
}
