//! Actual OceansIndex body, actor-scoped SQL and fixed-field receipt contract.
use super::read_model_first_fill::Receipts;
use super::*;
use rm::oceans_pages::OceansIndex as Oceans;
use tracing::instrument::WithSubscriber;

fn phase_receipt(raw: &str, completed: u64, graphs: usize) -> Value {
    let receipts: Vec<Value> = raw
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap()["fields"].clone())
        .filter(|event| event["message"] == "oceans index phases completed")
        .collect();
    assert_eq!(receipts.len(), 1, "one receipt per body, including failure");
    let event = receipts.into_iter().next().unwrap();
    let expected: std::collections::BTreeSet<_> = [
        "message",
        "completed",
        "body_ms",
        "roster_ms",
        "metadata_ms",
        "counts_n",
        "counts_sum_ms",
        "counts_max_ms",
        "role_org_flag_ms",
        "governance_ms",
        "team_people_ms",
        "archived_ms",
        "residual_ms",
        "graphs_n",
        "members_n",
        "team_rows_n",
    ]
    .into_iter()
    .collect();
    assert_eq!(
        event
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect::<std::collections::BTreeSet<_>>(),
        expected,
        "privacy allowlist must be exact"
    );
    for (name, value) in event.as_object().unwrap() {
        if name != "message" {
            assert!(
                value.as_f64().is_some_and(|v| v.is_finite() && v >= 0.),
                "numeric-only field {name}"
            );
        }
    }
    assert_eq!(event["completed"], completed);
    assert_eq!(event["graphs_n"], graphs);
    let sum: f64 = [
        "roster_ms",
        "metadata_ms",
        "counts_sum_ms",
        "role_org_flag_ms",
        "governance_ms",
        "team_people_ms",
        "archived_ms",
        "residual_ms",
    ]
    .iter()
    .map(|name| event[*name].as_f64().unwrap())
    .sum();
    let body = event["body_ms"].as_f64().unwrap();
    assert!(
        (sum - body).abs() <= 1e-8 * body.max(1.),
        "nonoverlapping phases reconcile to body"
    );
    assert!(event["counts_max_ms"].as_f64().unwrap() <= event["counts_sum_ms"].as_f64().unwrap());
    event
}

async fn tick_receipts(
    f: &Fixture,
) -> (
    Result<coralswarm_backend::workers::tenant_harness::Tick, String>,
    String,
) {
    let receipts = Receipts::default();
    let writer = receipts.clone();
    let subscriber = tracing_subscriber::fmt()
        .json()
        .with_writer(move || writer.clone())
        .finish();
    let result = read_model_refresh::tick(&f.worker(), &f.control)
        .with_subscriber(subscriber)
        .await;
    let raw = String::from_utf8(receipts.0.lock().unwrap().clone()).unwrap();
    (result, raw)
}

#[tokio::test]
async fn oceans_numeric_phases_reconcile_for_zero_one_many_graphs_and_sql_failure() {
    for org in [false, true] {
        let f = Fixture::new(org).await;
        // Set real account authority. A separate flags-on invocation also
        // exercises OptionalAdmin governance proof/grant on the org plane.
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
        sqlx::query("UPDATE graphs SET archived_at=now() WHERE id=$1")
            .bind(f.graph)
            .execute(&f.owner)
            .await
            .unwrap();
        for graphs in [0, 1, 3] {
            if graphs == 1 {
                sqlx::query("UPDATE graphs SET archived_at=NULL WHERE id=$1")
                    .bind(f.graph)
                    .execute(&f.owner)
                    .await
                    .unwrap();
                f.atom("phase-fixture").await;
            }
            if graphs == 3 {
                for _ in 0..2 {
                    sqlx::query("INSERT INTO graphs(account_id,name,kind,owner_user_id) VALUES($1,'private graph sentinel','member',$2)").bind(f.account).bind(f.user).execute(&f.owner).await.unwrap();
                }
            }
            let scope = f.domain_scope::<Oceans>(Default::default()).await;
            rm::store::request_refresh::<Oceans>(&f.app, &scope)
                .await
                .unwrap();
            let (tick, raw) = tick_receipts(&f).await;
            assert!(tick.is_ok());
            let event = phase_receipt(&raw, 1, graphs);
            assert_eq!(
                event["counts_n"], graphs,
                "one real sequential counts await per metadata graph"
            );
            assert_eq!(event["members_n"], if org { 2 } else { 1 });
            if graphs == 0 {
                assert_eq!(event["counts_sum_ms"], 0.);
                assert_eq!(event["counts_max_ms"], 0.);
            }
            let mut tx = begin_tenant_scoped(&f.app, f.account, f.user)
                .await
                .unwrap();
            let stored = rm::store::read::<Oceans>(&mut tx, &scope).await.unwrap();
            assert!(stored.computed_at.is_some());
            let payload = serde_json::to_value(stored.payload).unwrap();
            assert_eq!(payload["oceans"].as_array().unwrap().len(), graphs);
            assert_eq!(
                payload["members"].as_array().unwrap().len(),
                if org { 2 } else { 1 }
            );
            if graphs > 0 {
                let atom_sum: i64 = payload["oceans"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|row| row["counts"]["atom_count"].as_i64().unwrap())
                    .sum();
                assert_eq!(
                    atom_sum, 1,
                    "phase receipt must preserve actual admitted counts"
                );
            }
            tx.rollback().await.unwrap();
        }
        // Real counts SQL wait/error; configure existing pool sessions only.
        let mut connections = Vec::new();
        for _ in 0..f.app.options().get_max_connections() {
            connections.push(f.app.acquire().await.unwrap());
        }
        for connection in &mut connections {
            sqlx::query("SET lock_timeout='150ms'")
                .execute(&mut **connection)
                .await
                .unwrap();
        }
        drop(connections);
        let scope = f.domain_scope::<Oceans>(Default::default()).await;
        rm::store::request_refresh::<Oceans>(&f.app, &scope)
            .await
            .unwrap();
        let peer = f
            .domain_scope::<rm::chats::ConversationList>(Default::default())
            .await;
        rm::store::enqueue::<rm::chats::ConversationList>(&f.app, &peer)
            .await
            .unwrap();
        let mut lock = f.owner.begin().await.unwrap();
        sqlx::query("LOCK TABLE atoms IN ACCESS EXCLUSIVE MODE")
            .execute(&mut *lock)
            .await
            .unwrap();
        let (result, raw) = tick_receipts(&f).await;
        lock.rollback().await.unwrap();
        assert!(
            result.is_ok(),
            "compute failure should record backoff normally"
        );
        let event = phase_receipt(&raw, 0, 3);
        assert_eq!(event["counts_n"], 1);
        let failed: (i32, bool) = sqlx::query_as(
            "SELECT failed_attempts,claim_token IS NULL FROM read_models WHERE scope_key=$1",
        )
        .bind(scope.key())
        .fetch_one(&f.owner)
        .await
        .unwrap();
        assert_eq!(failed, (1, true));
        let peer_settled:bool=sqlx::query_scalar("SELECT computed_at IS NOT NULL AND claim_token IS NULL FROM read_models WHERE scope_key=$1").bind(peer.key()).fetch_one(&f.owner).await.unwrap();
        assert!(peer_settled, "SQL failure cannot cancel a selected peer");
        assert!(!raw.contains("private graph sentinel"));
        println!(
            "{}",
            json!({"scenario":"oceans_phases","org":org,"graph_cases":[0,1,3],"failure_receipt":true,"fixed_numeric_allowlist":true})
        );
        f.close().await;
    }
}
