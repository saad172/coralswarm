//! Regression for a claimed fairness refresh delaying current-authority first fills.
use super::*;
#[derive(Clone, Default)]
pub(super) struct Receipts(pub(super) Arc<std::sync::Mutex<Vec<u8>>>);
impl std::io::Write for Receipts {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn first_fill_dispatch_precedes_stored_fairness_without_losing_its_slot() {
    use rm::connectors::ConnectorsList as C;
    for org in [false, true] {
        let f = Fixture::new(org).await;
        let old = f.domain_scope::<C>(Default::default()).await;
        rm::store::enqueue::<C>(&f.app, &old).await.unwrap();
        read_model_refresh::tick(&f.worker(), &f.control)
            .await
            .unwrap();
        let mut control = begin_tenant_scoped(&f.control, f.account, f.user)
            .await
            .unwrap();
        sqlx::query("UPDATE account_members SET role='admin' WHERE account_id=$1 AND user_id=$2")
            .bind(f.account)
            .bind(f.user)
            .execute(&mut *control)
            .await
            .unwrap();
        control.commit().await.unwrap();
        let current = f.domain_scope::<C>(Default::default()).await;
        assert_ne!(current.key(), old.key());
        let mut tx = begin_tenant_scoped(&f.app, f.account, f.user)
            .await
            .unwrap();
        assert!(
            rm::store::read::<C>(&mut tx, &current)
                .await
                .unwrap()
                .computed_at
                .is_none(),
            "never borrow revision N for N+1"
        );
        tx.rollback().await.unwrap();
        // A physically earlier, very old stored row wins the fairness slot.
        // The returned vector defines bounded admission order, so this row
        // must be admitted AFTER all selected first fills, regardless of
        // UPDATE RETURNING's physical row order.
        sqlx::query("INSERT INTO read_models(model,scope_key,version,account_id,graph_ids,compute_as,params,payload,computed_at,computed_snapshot,dirty_at,dirty_bumped_at) VALUES('workspace_counts','stored-fair',1,$1,$2,$3,'{}','{}',now(),pg_current_snapshot(),now()-interval '1 day',now()-interval '1 day')")
            .bind(f.account).bind(vec![f.graph]).bind(f.user).execute(&f.owner).await.unwrap();
        rm::store::enqueue::<C>(&f.app, &current).await.unwrap();
        let due:bool=sqlx::query_scalar("SELECT LEAST(dirty_bumped_at+make_interval(secs=>debounce_seconds),dirty_at+make_interval(secs=>max_delay_seconds))<=now() FROM read_models WHERE scope_key=$1")
            .bind(current.key()).fetch_one(&f.owner).await.unwrap();
        assert!(due, "new revision is due immediately");
        let claims = rm::store::claim(&f.app).await.unwrap();
        assert_eq!(claims.len(), 2);
        assert_eq!(
            claims[0].scope_key,
            current.key(),
            "batch admission must prefer first fill before stored fairness compute"
        );
        assert_eq!(
            claims[1].scope_key, "stored-fair",
            "fairness still owns one of four slots"
        );
        let (payload, snapshot) = f.compute(&claims[0]).await;
        assert!(rm::store::publish(&f.app, &claims[0], payload, &snapshot)
            .await
            .unwrap());
        let mut tx = begin_tenant_scoped(&f.app, f.account, f.user)
            .await
            .unwrap();
        assert!(rm::store::read::<C>(&mut tx, &current)
            .await
            .unwrap()
            .computed_at
            .is_some());
        tx.rollback().await.unwrap();
        f.close().await;
    }
}

#[tokio::test]
async fn real_worker_publishes_three_current_first_fills_before_blocked_fair_refresh() {
    use axum::{
        body::{to_bytes, Body},
        http::Request,
    };
    use coralswarm_backend::{
        tenancy::runtime::{TenancyDecision, TenantContext},
        UserContext,
    };
    use rm::{
        chats::ConversationList as Fair,
        connectors::{ConnectorDetail as D, ConnectorsList as C},
    };
    use tower::ServiceExt;
    for org in [false, true] {
        let f = Fixture::new(org).await;
        let old = f.domain_scope::<C>(Default::default()).await;
        rm::store::enqueue::<C>(&f.app, &old).await.unwrap();
        read_model_refresh::tick(&f.worker(), &f.control)
            .await
            .unwrap();
        f.domain::<Fair>(Default::default()).await;
        let mut control = begin_tenant_scoped(&f.control, f.account, f.user)
            .await
            .unwrap();
        sqlx::query("UPDATE account_members SET role='admin' WHERE account_id=$1 AND user_id=$2")
            .bind(f.account)
            .bind(f.user)
            .execute(&mut *control)
            .await
            .unwrap();
        control.commit().await.unwrap();
        // Actual canonical backend GET, with already verified actor/tenant.
        // Clerk's actual JWT verification is covered by the E2E companion.
        let state =
            coralswarm_backend::test_app_state_owned_with_db_and_store(f.control.clone(), None);
        let decision = if org {
            TenancyDecision::Org(TenantContext {
                org_id: "org_read_models".into(),
                account_id: f.account,
                user_id: f.user,
                pool: f.app.clone(),
                page_pool: None,
            })
        } else {
            TenancyDecision::Personal
        };
        let router = coralswarm_backend::test_support_v1_app(
            Arc::new(state),
            UserContext {
                account_id: f.account,
                user_id: f.user,
                role: "admin".into(),
                device_id: "first-fill-regression".into(),
            },
        )
        .layer(axum::Extension(decision));
        async fn get(router: &axum::Router) -> Value {
            let response = router
                .clone()
                .oneshot(
                    Request::builder()
                        .uri("/v1/connectors")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), 200);
            serde_json::from_slice(&to_bytes(response.into_body(), 1_000_000).await.unwrap())
                .unwrap()
        }
        assert!(
            get(&router).await["computed_at"].is_null(),
            "revision N payload cannot satisfy N+1 GET"
        );
        let current = f.domain_scope::<C>(Default::default()).await;
        // Barrier on completion of the GET's detached enqueue, not a timed guess.
        tokio::time::timeout(Duration::from_secs(5),async {
            loop {
                let exists:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM read_models WHERE scope_key=$1 AND immediate_due_at IS NOT NULL)").bind(current.key()).fetch_one(&f.owner).await.unwrap();
                if exists {break;} tokio::task::yield_now().await;
            }
        }).await.unwrap();
        let details = [
            f.domain_scope::<D>([("provider".into(), "google".into())].into())
                .await,
            f.domain_scope::<D>([("provider".into(), "microsoft".into())].into())
                .await,
        ];
        for scope in &details {
            rm::store::enqueue::<D>(&f.app, scope).await.unwrap();
        }
        sqlx::query("UPDATE read_models SET dirty_at=now()-interval '1 day',dirty_bumped_at=now()-interval '1 day' WHERE model='conversation_list'").execute(&f.owner).await.unwrap();
        let mut lock = f.owner.begin().await.unwrap();
        sqlx::query("LOCK TABLE conversations IN ACCESS EXCLUSIVE MODE")
            .execute(&mut *lock)
            .await
            .unwrap();
        let worker = f.worker();
        let control = f.control.clone();
        use tracing::instrument::WithSubscriber;
        let receipts = Receipts::default();
        let writer = receipts.clone();
        let subscriber = tracing_subscriber::fmt()
            .json()
            .with_writer(move || writer.clone())
            .finish();
        let task = tokio::spawn(
            async move { read_model_refresh::tick(&worker, &control).await }
                .with_subscriber(subscriber),
        );
        // Wait until the real fairness computation reaches its blocked table.
        tokio::time::timeout(Duration::from_secs(8),async {
            loop {
                let waiting:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_locks WHERE database=(SELECT oid FROM pg_database WHERE datname=current_database()) AND relation='conversations'::regclass AND NOT granted)").fetch_one(&f.owner).await.unwrap();
                if waiting {break;} assert!(!task.is_finished(),"worker must reach fair compute barrier");tokio::task::yield_now().await;
            }
        }).await.unwrap();
        // Concurrent first fills can still be finishing when the fairness
        // refresh enters its barrier. Require all three to publish while held.
        let first_fills = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let n:i64=sqlx::query_scalar("SELECT count(*) FROM read_models WHERE model IN ('connectors_list','connector_detail') AND computed_at IS NOT NULL").fetch_one(&f.owner).await.unwrap();
                if n == 3 {break;}
                tokio::task::yield_now().await;
            }
        }).await;
        let answer = get(&router).await;
        let published:i64=sqlx::query_scalar("SELECT count(*) FROM read_models WHERE model IN ('connectors_list','connector_detail') AND computed_at IS NOT NULL").fetch_one(&f.owner).await.unwrap();
        // Always release before assertions, so even a red regression drains.
        lock.rollback().await.unwrap();
        let tick = task.await.unwrap().unwrap();
        assert!(
            !answer["computed_at"].is_null(),
            "first-fill GET succeeds while fair refresh is blocked, org={org}"
        );
        assert!(first_fills.is_ok());
        assert_eq!(
            published, 3,
            "all three first fills publish while fair barrier is held"
        );
        assert_eq!(
            tick.processed, 4,
            "oldest fairness still progresses in same tick"
        );
        let raw = String::from_utf8(receipts.0.lock().unwrap().clone()).unwrap();
        let events: Vec<Value> = raw
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap()["fields"].clone())
            .filter(|f| f["message"] == "read-model refresh completed")
            .collect();
        assert_eq!(events.len(), 4);
        for event in &events {
            if event["first_fill"] == true {
                assert!(event["due_to_claim_ms"].as_f64().unwrap() >= 0.);
            } else {
                assert!(event.get("due_to_claim_ms").is_none());
            }
            assert!(
                event["model_compute_ms"].as_f64().unwrap()
                    <= event["compute_ms"].as_f64().unwrap()
            );
            assert_eq!(event["published"], true);
            assert!(
                event.get("params").is_none()
                    && event.get("account_id").is_none()
                    && event.get("claim_token").is_none()
            );
        }
        assert!(events[..3].iter().all(|e| e["first_fill"] == true));
        assert_eq!(events[3]["first_fill"], false);
        println!(
            "{}",
            json!({"scenario":"first_fill_batch_barrier","org":org,"completed_before_fair_release":3,"timings":events.iter().map(|e|json!({"model":e["model"],"queue_ms":e["due_to_claim_ms"],"batch_wait_ms":e["batch_wait_ms"],"compute_ms":e["compute_ms"],"model_compute_ms":e["model_compute_ms"],"publish_ms":e["publish_ms"]})).collect::<Vec<_>>()})
        );

        let outstanding: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM read_models WHERE immediate_due_at IS NOT NULL",
        )
        .fetch_one(&f.owner)
        .await
        .unwrap();
        assert_eq!(
            outstanding, 0,
            "successful clean publication closes immediate episode"
        );
        f.close().await;
    }
}

#[tokio::test]
async fn immediate_stage_clock_survives_reenqueue_failure_and_lost_publication() {
    let f = Fixture::new(false).await;
    f.enqueue().await;
    let first = f.claim().await;
    assert!(first.first_fill);
    assert!(first.immediate_due_at.unwrap() <= first.claimed_at);
    assert!(first.due_to_claim_ms.unwrap() >= 0.);
    let (payload, snapshot) = f.compute(&first).await;
    f.enqueue().await;
    assert!(
        rm::store::publish(&f.app, &first, payload.clone(), &snapshot)
            .await
            .unwrap()
    );
    let still_due: Option<chrono::DateTime<chrono::Utc>> =
        sqlx::query_scalar("SELECT immediate_due_at FROM read_models")
            .fetch_one(&f.owner)
            .await
            .unwrap();
    assert_eq!(
        still_due, first.immediate_due_at,
        "publication over newer generation cannot close the immediate episode"
    );
    let second = f.claim().await;
    assert!(second.dirty_generation > first.dirty_generation);
    assert!(!second.first_fill);
    assert_eq!(
        second.immediate_due_at, first.immediate_due_at,
        "poll/enqueue cannot hide original waiting time"
    );
    assert!(
        !rm::store::publish(&f.app, &first, payload, &snapshot)
            .await
            .unwrap(),
        "old claim cannot release newer owner"
    );
    assert_eq!(
        rm::store::fail(&f.app, &second, FailureKind::Other)
            .await
            .unwrap(),
        Some(1)
    );
    assert!(
        rm::store::claim(&f.app).await.unwrap().is_empty(),
        "backoff remains eligible fence"
    );
    let scope = f.scope().await;
    rm::store::enqueue::<Model>(&f.app, &scope).await.unwrap();
    assert!(
        rm::store::claim(&f.app).await.unwrap().is_empty(),
        "automatic enqueue cannot defeat backoff"
    );
    sqlx::query("UPDATE read_models SET retry_after=now()-interval '1 second'")
        .execute(&f.owner)
        .await
        .unwrap();
    let retry = f.claim().await;
    assert_eq!(retry.immediate_due_at, first.immediate_due_at);
    let (payload, snapshot) = f.compute(&retry).await;
    assert!(rm::store::publish(&f.app, &retry, payload, &snapshot)
        .await
        .unwrap());
    let closed: bool = sqlx::query_scalar("SELECT immediate_due_at IS NULL FROM read_models")
        .fetch_one(&f.owner)
        .await
        .unwrap();
    assert!(closed);

    f.enqueue().await;
    // Exact rolling-old-binary shape: a certified clean publication knows
    // dirty_at but has no immediate_due_at assignment. The additive trigger
    // must close its metadata episode as well.
    sqlx::query("UPDATE read_models SET dirty_at=NULL,dirty_bumped_at=NULL")
        .execute(&f.owner)
        .await
        .unwrap();
    let legacy_closed: bool =
        sqlx::query_scalar("SELECT immediate_due_at IS NULL FROM read_models")
            .fetch_one(&f.owner)
            .await
            .unwrap();
    assert!(
        legacy_closed,
        "rolling older publication cannot leak a prior episode into a later measurement"
    );
    f.close().await;
}

/// A SQL lock, not a scheduling delay, keeps the oldest first fill in compute.
#[tokio::test]
async fn first_fill_peer_progress_while_older_first_fill_is_sql_blocked() {
    use rm::{chats::ConversationList as Slow, connectors::ConnectorsList as Cheap};
    for org in [false, true] {
        let f = Fixture::new(org).await;
        // A real stored refresh must still win the reserved oldest slot.
        f.domain::<Model>([("graph".into(), f.graph.to_string())].into())
            .await;
        let slow = f.domain_scope::<Slow>(Default::default()).await;
        let cheap = f.domain_scope::<Cheap>(Default::default()).await;
        rm::store::enqueue::<Slow>(&f.app, &slow).await.unwrap();
        rm::store::enqueue::<Cheap>(&f.app, &cheap).await.unwrap();
        sqlx::query("UPDATE read_models SET dirty_at=now()-CASE model WHEN 'workspace_counts' THEN interval '1 day' WHEN 'conversation_list' THEN interval '2 hours' ELSE interval '1 hour' END,dirty_bumped_at=now()-interval '1 hour'")
            .execute(&f.owner).await.unwrap();
        let mut lock = f.owner.begin().await.unwrap();
        sqlx::query("LOCK TABLE conversations IN ACCESS EXCLUSIVE MODE")
            .execute(&mut *lock)
            .await
            .unwrap();
        let worker = f.worker();
        let control = f.control.clone();
        let task = tokio::spawn(async move { read_model_refresh::tick(&worker, &control).await });
        let blocked = tokio::time::timeout(Duration::from_secs(8), async {
            loop {
                let n:i64=sqlx::query_scalar("SELECT count(*) FROM pg_locks WHERE database=(SELECT oid FROM pg_database WHERE datname=current_database()) AND relation='conversations'::regclass AND NOT granted").fetch_one(&f.owner).await.unwrap();
                if n == 1 { break; }
                assert!(!task.is_finished(), "worker must enter real SQL barrier");
                tokio::task::yield_now().await;
            }
        }).await;
        // A generous liveness deadline only; no millisecond performance claim.
        let peer_progress = if blocked.is_ok() {
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let done: bool = sqlx::query_scalar(
                        "SELECT computed_at IS NOT NULL FROM read_models WHERE scope_key=$1",
                    )
                    .bind(cheap.key())
                    .fetch_one(&f.owner)
                    .await
                    .unwrap();
                    if done {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .is_ok()
        } else {
            false
        };
        let selected: i64 =
            sqlx::query_scalar("SELECT count(*) FROM read_models WHERE claim_token IS NOT NULL")
                .fetch_one(&f.owner)
                .await
                .unwrap();
        // Cleanup precedes EVERY verdict, including the controlled red.
        lock.rollback().await.unwrap();
        let tick = tokio::time::timeout(Duration::from_secs(45), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let settled:i64=sqlx::query_scalar("SELECT count(*) FROM read_models WHERE computed_at IS NOT NULL AND claim_token IS NULL AND refresh_claimed_at IS NULL AND dirty_at IS NULL AND failed_attempts=0").fetch_one(&f.owner).await.unwrap();
        let fair_done:bool=sqlx::query_scalar("SELECT computed_at IS NOT NULL AND dirty_at IS NULL FROM read_models WHERE model='workspace_counts'").fetch_one(&f.owner).await.unwrap();
        println!(
            "{}",
            json!({"scenario":"first_fill_peer_progress","org":org,"sql_barrier_entered":blocked.is_ok(),"peer_published_while_blocked":peer_progress,"selected_unsettled_while_blocked":selected,"processed_after_release":tick.processed,"settled_after_release":settled,"oldest_stored_settled":fair_done})
        );
        f.close().await;
        assert!(blocked.is_ok(), "slow first fill must reach SQL barrier");
        assert_eq!(
            tick.processed, 3,
            "same batch includes oldest stored fairness claim"
        );
        assert_eq!(settled, 3, "all claims settle after barrier release");
        assert!(fair_done);
        assert!(
            peer_progress,
            "cheap FIRST FILL must publish while older FIRST FILL holds real SQL lock, org={org}"
        );
    }
}

#[tokio::test]
async fn first_fill_dispatch_bound_and_failed_claim_do_not_cancel_peers() {
    use rm::{chats::ConversationList as Slow, connectors::ConnectorsList as Cheap};
    use tracing::instrument::WithSubscriber;
    for org in [false, true] {
        let f = Fixture::new(org).await;
        for q in ["first", "second", "third"] {
            let scope = f
                .domain_scope::<Slow>(
                    [
                        ("graph_id".into(), f.graph.to_string()),
                        ("q".into(), q.into()),
                    ]
                    .into(),
                )
                .await;
            rm::store::enqueue::<Slow>(&f.app, &scope).await.unwrap();
        }
        let cheap = f.domain_scope::<Cheap>(Default::default()).await;
        rm::store::enqueue::<Cheap>(&f.app, &cheap).await.unwrap();
        sqlx::query("UPDATE read_models SET dirty_at=now()-CASE model WHEN 'conversation_list' THEN interval '2 hours' ELSE interval '1 hour' END,dirty_bumped_at=now()-interval '1 hour'").execute(&f.owner).await.unwrap();
        let receipts = Receipts::default();
        let writer = receipts.clone();
        let subscriber = tracing_subscriber::fmt()
            .json()
            .with_writer(move || writer.clone())
            .finish();
        let mut lock = f.owner.begin().await.unwrap();
        sqlx::query("LOCK TABLE conversations IN ACCESS EXCLUSIVE MODE")
            .execute(&mut *lock)
            .await
            .unwrap();
        let worker = f.worker();
        let control = f.control.clone();
        let task = tokio::spawn(
            async move { read_model_refresh::tick(&worker, &control).await }
                .with_subscriber(subscriber),
        );
        let barrier=tokio::time::timeout(Duration::from_secs(8),async {
            loop {
                let n:i64=sqlx::query_scalar("SELECT count(*) FROM pg_locks WHERE database=(SELECT oid FROM pg_database WHERE datname=current_database()) AND relation='conversations'::regclass AND NOT granted").fetch_one(&f.owner).await.unwrap();
                if n>=2 {break n;}
                tokio::task::yield_now().await;
            }
        }).await;
        lock.rollback().await.unwrap();
        let result = task.await.unwrap().unwrap();
        let raw = String::from_utf8(receipts.0.lock().unwrap().clone()).unwrap();
        let mut active = 0;
        let mut maximum = 0;
        let mut completed = 0;
        for line in raw.lines() {
            let v: Value = serde_json::from_str(line).unwrap();
            match v["fields"]["message"].as_str() {
                Some("read-model refresh started") => {
                    active += 1;
                    maximum = maximum.max(active);
                }
                Some("read-model refresh completed") => {
                    active -= 1;
                    completed += 1;
                }
                _ => {}
            }
        }
        let settled:i64=sqlx::query_scalar("SELECT count(*) FROM read_models WHERE computed_at IS NOT NULL AND claim_token IS NULL AND dirty_at IS NULL").fetch_one(&f.owner).await.unwrap();
        assert_eq!(barrier.unwrap(), 2, "exactly two actual SQL waits admitted");
        assert_eq!(
            (active, maximum, completed, settled, result.processed),
            (0, 2, 4, 4, 4)
        );

        // Real publication failure: only conversation-list publishes raise. Other
        // selected claims still settle, and the unchanged lease permits recovery.
        f.owner.execute("CREATE FUNCTION test_reject_publish() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.model='conversation_list' AND NEW.computed_at IS DISTINCT FROM OLD.computed_at THEN RAISE EXCEPTION 'private publish sentinel'; END IF; RETURN NEW; END $$; CREATE TRIGGER test_reject_publish BEFORE UPDATE ON read_models FOR EACH ROW EXECUTE FUNCTION test_reject_publish()")
            .await.unwrap();
        sqlx::query("UPDATE read_models SET dirty_at=now()-interval '1 hour',dirty_bumped_at=now()-interval '1 hour'").execute(&f.owner).await.unwrap();
        assert!(read_model_refresh::tick(&f.worker(), &f.control)
            .await
            .is_err());
        let peers:i64=sqlx::query_scalar("SELECT count(*) FROM read_models WHERE model='connectors_list' AND dirty_at IS NULL AND claim_token IS NULL").fetch_one(&f.owner).await.unwrap();
        let leased:i64=sqlx::query_scalar("SELECT count(*) FROM read_models WHERE model='conversation_list' AND claim_token IS NOT NULL").fetch_one(&f.owner).await.unwrap();
        assert_eq!(
            (peers, leased),
            (1, 3),
            "publication errors must drain peers; failed publishes keep fenced recovery leases"
        );
        f.owner.execute("DROP TRIGGER test_reject_publish ON read_models; DROP FUNCTION test_reject_publish(); UPDATE read_models SET refresh_claimed_at=now()-interval '3 minutes' WHERE claim_token IS NOT NULL").await.unwrap();
        assert_eq!(
            read_model_refresh::tick(&f.worker(), &f.control)
                .await
                .unwrap()
                .processed,
            3
        );
        let remaining:i64=sqlx::query_scalar("SELECT count(*) FROM read_models WHERE dirty_at IS NOT NULL OR claim_token IS NOT NULL").fetch_one(&f.owner).await.unwrap();
        assert_eq!(
            remaining, 0,
            "all failed publications reclaimed and settled"
        );
        println!(
            "{}",
            json!({"scenario":"bounded_dispatch_and_publish_failure","org":org,"maximum_active":maximum,"settled":settled,"reclaimed":leased})
        );
        f.close().await;
    }
}

#[tokio::test]
async fn cancelled_bounded_batch_rolls_back_and_reclaims_every_selected_claim() {
    use rm::chats::ConversationList as Slow;
    for org in [false, true] {
        let f = Fixture::new(org).await;
        for q in ["a", "b", "c", "d"] {
            let scope = f
                .domain_scope::<Slow>(
                    [
                        ("graph_id".into(), f.graph.to_string()),
                        ("q".into(), q.into()),
                    ]
                    .into(),
                )
                .await;
            rm::store::enqueue::<Slow>(&f.app, &scope).await.unwrap();
        }
        let mut lock = f.owner.begin().await.unwrap();
        sqlx::query("LOCK TABLE conversations IN ACCESS EXCLUSIVE MODE")
            .execute(&mut *lock)
            .await
            .unwrap();
        let worker = f.worker();
        let control = f.control.clone();
        let task = tokio::spawn(async move { read_model_refresh::tick(&worker, &control).await });
        let blocked=tokio::time::timeout(Duration::from_secs(8),async {
            loop {
                let n:i64=sqlx::query_scalar("SELECT count(*) FROM pg_locks WHERE database=(SELECT oid FROM pg_database WHERE datname=current_database()) AND relation='conversations'::regclass AND NOT granted").fetch_one(&f.owner).await.unwrap();
                if n>=2 {break n;}
                tokio::task::yield_now().await;
            }
        }).await;
        task.abort();
        let cancelled = task.await.unwrap_err().is_cancelled();
        // Save real old tokens for a stale-worker fence assertion after reclaim.
        let claims:Vec<rm::store::Claim>=sqlx::query_as("SELECT model,scope_key,version,account_id,graph_ids,compute_as,params,dirty_generation,claim_token,control_revision,control_freshness,immediate_due_at,refresh_claimed_at AS claimed_at,true AS first_fill,NULL::float8 AS due_to_claim_ms FROM read_models").fetch_all(&f.owner).await.unwrap();
        lock.rollback().await.unwrap();
        // Dropping a sqlx transaction queues rollback. Reacquire every slot and
        // observe idle transactions, so this verifies DB cleanup, not just Rust Drop.
        let mut connections = Vec::new();
        for _ in 0..f.app.options().get_max_connections() {
            connections.push(f.app.acquire().await.unwrap());
        }
        for connection in &mut connections {
            let clean:bool=sqlx::query_scalar("SELECT current_setting('transaction_read_only')='off' AND NULLIF(current_setting('app.user_id',true),'') IS NULL").fetch_one(&mut **connection).await.unwrap();
            assert!(
                clean,
                "cancelled snapshots must roll back actor-local settings"
            );
        }
        drop(connections);
        assert!(
            rm::store::claim(&f.app).await.unwrap().is_empty(),
            "live leases prevent duplicate claims after cancellation"
        );
        sqlx::query("UPDATE read_models SET refresh_claimed_at=now()-interval '3 minutes'")
            .execute(&f.owner)
            .await
            .unwrap();
        let reclaimed = rm::store::claim(&f.app).await.unwrap();
        let mut publishes = 0;
        for claim in &reclaimed {
            let old = claims
                .iter()
                .find(|c| c.scope_key == claim.scope_key)
                .unwrap();
            assert_ne!(old.claim_token, claim.claim_token);
            assert_eq!(
                rm::store::fail(&f.app, old, FailureKind::Other)
                    .await
                    .unwrap(),
                None,
                "cancelled claim cannot fail newer owner"
            );
            let (payload, snapshot) = f.compute(claim).await;
            assert!(
                !rm::store::publish(&f.app, old, payload.clone(), &snapshot)
                    .await
                    .unwrap(),
                "cancelled worker cannot publish over new token"
            );
            if rm::store::publish(&f.app, claim, payload, &snapshot)
                .await
                .unwrap()
            {
                publishes += 1;
            }
        }
        let settled:i64=sqlx::query_scalar("SELECT count(*) FROM read_models WHERE computed_at IS NOT NULL AND dirty_at IS NULL AND claim_token IS NULL").fetch_one(&f.owner).await.unwrap();
        f.close().await;
        assert_eq!(blocked.unwrap(), 2);
        assert!(cancelled);
        assert_eq!(
            (claims.len(), reclaimed.len(), publishes, settled),
            (4, 4, 4, 4)
        );
        println!(
            "{}",
            json!({"scenario":"bounded_batch_cancellation","org":org,"rolled_back":2,"reclaimed_and_settled":settled})
        );
    }
}

#[tokio::test]
async fn small_pool_dispatch_serializes_control_fact_snapshots_without_deadlock() {
    use rm::connectors::{ConnectorDetail as D, ConnectorsList as C};
    use tracing::instrument::WithSubscriber;
    for org in [false, true] {
        let mut f = Fixture::new(org).await;
        if org {
            let pool = PgPoolOptions::new()
                .max_connections(1)
                .acquire_timeout(Duration::from_secs(2))
                .connect_with((*f.control.connect_options()).clone())
                .await
                .unwrap();
            f.control.close().await;
            f.control = pool;
        } else {
            let pool = PgPoolOptions::new()
                .max_connections(2)
                .acquire_timeout(Duration::from_secs(2))
                .connect_with((*f.app.connect_options()).clone())
                .await
                .unwrap();
            f.app.close().await;
            f.app = pool.clone();
            f.control = pool;
        }
        let scope = f.domain_scope::<C>(Default::default()).await;
        rm::store::enqueue::<C>(&f.app, &scope).await.unwrap();
        for provider in ["google", "microsoft"] {
            let scope = f
                .domain_scope::<D>([("provider".into(), provider.into())].into())
                .await;
            rm::store::enqueue::<D>(&f.app, &scope).await.unwrap();
        }
        let receipts = Receipts::default();
        let writer = receipts.clone();
        let subscriber = tracing_subscriber::fmt()
            .json()
            .with_writer(move || writer.clone())
            .finish();
        let result = read_model_refresh::tick(&f.worker(), &f.control)
            .with_subscriber(subscriber)
            .await
            .unwrap();
        let raw = String::from_utf8(receipts.0.lock().unwrap().clone()).unwrap();
        let mut active = 0;
        let mut maximum = 0;
        for line in raw.lines() {
            let event: Value = serde_json::from_str(line).unwrap();
            match event["fields"]["message"].as_str() {
                Some("read-model refresh started") => {
                    active += 1;
                    maximum = maximum.max(active);
                }
                Some("read-model refresh completed") => {
                    active -= 1;
                }
                _ => {}
            }
        }
        let settled:i64=sqlx::query_scalar("SELECT count(*) FROM read_models WHERE computed_at IS NOT NULL AND claim_token IS NULL AND failed_attempts=0").fetch_one(&f.owner).await.unwrap();
        assert_eq!((active, maximum, result.processed, settled), (0, 1, 3, 3));
        f.close().await;
    }
}
