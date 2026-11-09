#[path = "../../../tests/support/bootstrap_trace.rs"]
mod bootstrap_trace;
use super::*;
use std::time::Duration;
use tokio::{
    sync::{mpsc, oneshot},
    time::advance,
};

type Gate = oneshot::Receiver<()>;
fn gate(
    index: usize,
    started: mpsc::UnboundedSender<usize>,
) -> (oneshot::Sender<()>, impl Future<Output = ()>) {
    let (tx, rx): (_, Gate) = oneshot::channel();
    (tx, async move {
        started.send(index).unwrap();
        rx.await.unwrap();
    })
}

fn required_value(status: StatusCode, value: Value) -> RequiredPart {
    (status, Json(value))
}
fn response(status: StatusCode, value: Value) -> Response {
    (status, Json(value)).into_response()
}

async fn run_join(
    gates: [Gate; 7],
    started: mpsc::UnboundedSender<usize>,
    me_status: StatusCode,
) -> (Response, measurement::Record) {
    let [me, graphs, count, pending, selfserve, mine, billing] = gates;
    async fn wait(index: usize, started: &mpsc::UnboundedSender<usize>, gate: Gate) {
        started.send(index).unwrap();
        gate.await.unwrap();
    }
    measurement::scope("arbitrary-private-role", async {
        let parts = join_parts(
            async {
                wait(0, &started, me).await;
                required_value(me_status, json!({"private": "me"}))
            },
            async {
                wait(1, &started, graphs).await;
                required_value(StatusCode::OK, json!(["private graph"]))
            },
            async {
                wait(2, &started, count).await;
                response(StatusCode::FORBIDDEN, json!({"private_error": "redact"}))
            },
            async {
                wait(3, &started, pending).await;
                PartValue::ok(json!([]))
            },
            async {
                wait(4, &started, selfserve).await;
                PartValue::null(Outcome::Error)
            },
            async {
                wait(5, &started, mine).await;
                PartValue::null(Outcome::Refused)
            },
            async {
                wait(6, &started, billing).await;
                PartValue::null(Outcome::Skipped)
            },
        )
        .await;
        let response = assemble(parts).await;
        measurement::assembly_end(response.status());
        response
    })
    .await
}

fn gates() -> ([oneshot::Sender<()>; 7], [Gate; 7]) {
    let channels: Vec<_> = (0..7).map(|_| oneshot::channel()).collect();
    let (tx, rx): (Vec<_>, Vec<_>) = channels.into_iter().unzip();
    (tx.try_into().unwrap(), rx.try_into().unwrap())
}

/// Real production join + response assembly, seven independently controlled
/// futures. Paused time verifies readiness, rather than elapsed CPU budgets.
#[tokio::test(start_paused = true)]
async fn actual_join_records_seven_branches_on_one_clock_and_keeps_optional_gating() {
    let (tx, rx) = gates();
    let (started, mut starts) = mpsc::unbounded_channel();
    let task = tokio::spawn(run_join(rx, started, StatusCode::OK));
    let mut seen = Vec::new();
    for _ in 0..7 {
        seen.push(starts.recv().await.unwrap());
    }
    seen.sort();
    assert_eq!(seen, [0, 1, 2, 3, 4, 5, 6]);
    let [me, graphs, count, pending, selfserve, mine, billing] = tx;
    advance(Duration::from_millis(30)).await;
    me.send(()).unwrap();
    tokio::task::yield_now().await;
    advance(Duration::from_millis(40)).await;
    graphs.send(()).unwrap();
    count.send(()).unwrap();
    selfserve.send(()).unwrap();
    mine.send(()).unwrap();
    billing.send(()).unwrap();
    tokio::task::yield_now().await;
    assert!(
        !task.is_finished(),
        "optional pending read still gates the real join"
    );
    advance(Duration::from_millis(50)).await;
    pending.send(()).unwrap();
    let (response, record) = task.await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(record.branches.me.end_us, Some(30_000));
    assert_eq!(record.branches.graphs.end_us, Some(70_000));
    assert_eq!(record.required_pair_ready_us, 70_000);
    assert!(record.required_pair_ok);
    assert_eq!(record.branches.pending_orgs.end_us, Some(120_000));
    assert_eq!(record.branches.pending_orgs.elapsed_us, Some(120_000));
    assert_eq!(record.assembly_start_us, 120_000);
    assert_eq!(record.assembly_end_us, 120_000);
    assert_eq!(record.assembly_elapsed_us, 0);
    assert_eq!(record.complete_elapsed_us, 120_000);
    assert_eq!(record.branches.admin_join_count.outcome, Outcome::Refused);
    assert_eq!(record.branches.selfserve_org.outcome, Outcome::Error);
    assert_eq!(record.branches.my_join_requests.outcome, Outcome::Refused);
    assert_eq!(record.branches.billing_banner.outcome, Outcome::Skipped);
    assert!(!record.branches.billing_banner.executed);
    assert_privacy_schema(&record);
    let capture = bootstrap_trace::Capture::default();
    let _subscriber = tracing::subscriber::set_default(capture.subscriber());
    measurement::emit(&record);
    let events = capture.take();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0], serde_json::to_value(&record).unwrap());
    let body: Value = serde_json::from_slice(
        &axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(body["pending_orgs"], json!([]));
    for part in [
        "selfserve_org",
        "my_join_requests",
        "billing_banner",
        "join_requests_count",
    ] {
        assert!(body[part].is_null());
    }
    assert_eq!(body["me"], json!({"private": "me"}));
    assert_eq!(body["graphs"], json!(["private graph"]));
    assert_eq!(body.as_object().unwrap().len(), 10);
}

#[tokio::test(start_paused = true)]
async fn required_refusal_still_awaits_all_parts_and_preserves_exact_body() {
    let (tx, rx) = gates();
    let (started, mut starts) = mpsc::unbounded_channel();
    let task = tokio::spawn(run_join(rx, started, StatusCode::FORBIDDEN));
    for _ in 0..7 {
        starts.recv().await.unwrap();
    }
    let [me, graphs, count, pending, selfserve, mine, billing] = tx;
    me.send(()).unwrap();
    graphs.send(()).unwrap();
    count.send(()).unwrap();
    selfserve.send(()).unwrap();
    mine.send(()).unwrap();
    billing.send(()).unwrap();
    tokio::task::yield_now().await;
    assert!(!task.is_finished());
    advance(Duration::from_millis(100)).await;
    pending.send(()).unwrap();
    let (response, record) = task.await.unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert_eq!(
        axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap(),
        serde_json::to_vec(&json!({"private": "me"})).unwrap()
    );
    assert!(!record.required_pair_ok);
    assert_eq!(record.outcome, Outcome::Refused);
    assert_eq!(record.branches.me.outcome, Outcome::Refused);
    assert_eq!(record.complete_elapsed_us, 100_000);
    assert_privacy_schema(&record);
}

#[tokio::test(start_paused = true)]
async fn existing_optional_timeout_cancels_provider_and_keeps_count_unbounded() {
    let (started, mut starts) = mpsc::unbounded_channel();
    let (count, count_gate) = gate(2, started.clone());
    let (provider_started, provider_signal) = oneshot::channel();
    let task = tokio::spawn(async move {
        measurement::scope("owner", async {
            let parts = join_parts(
                async { required_value(StatusCode::OK, json!({})) },
                async { required_value(StatusCode::OK, json!([])) },
                async {
                    count_gate.await;
                    response(StatusCode::OK, json!({"open": 8}))
                },
                async { PartValue::ok(json!([])) },
                async { PartValue::ok(json!(false)) },
                async { PartValue::ok(json!([])) },
                async {
                    let mut db = Guard::start(Phase::BillingDb);
                    db.finish(Outcome::Ok);
                    let _provider = Guard::start(Phase::BillingProvider);
                    provider_started.send(()).unwrap();
                    std::future::pending::<PartValue>().await
                },
            )
            .await;
            let response = assemble(parts).await;
            measurement::assembly_end(response.status());
            response
        })
        .await
    });
    provider_signal.await.unwrap();
    starts.recv().await.unwrap();
    advance(OPTIONAL_PART_DEADLINE).await;
    tokio::task::yield_now().await;
    assert!(
        !task.is_finished(),
        "count never gained an optional timeout"
    );
    advance(Duration::from_millis(200)).await;
    count.send(()).unwrap();
    let (response, record) = task.await.unwrap();
    assert_eq!(record.branches.billing_banner.outcome, Outcome::Timeout);
    assert_eq!(record.branches.billing_banner.end_us, Some(500_000));
    assert_eq!(record.billing.provider.outcome, Outcome::Cancelled);
    assert!(record.billing.provider.executed);
    assert_eq!(record.billing.provider.elapsed_us, Some(500_000));
    assert_eq!(record.billing.db.outcome, Outcome::Ok);
    assert_eq!(record.branches.admin_join_count.end_us, Some(700_000));
    assert_eq!(record.complete_elapsed_us, 700_000);
    assert_privacy_schema(&record);
    let body: Value = serde_json::from_slice(
        &axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert!(body["billing_banner"].is_null());
    assert_eq!(body["join_requests_count"], json!({"open": 8}));
}

fn assert_privacy_schema(record: &measurement::Record) {
    fn keys(value: &Value, expected: &[&str]) {
        let mut actual: Vec<_> = value
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        let mut expected = expected.to_vec();
        actual.sort();
        expected.sort();
        assert_eq!(actual, expected);
    }
    let record = serde_json::to_value(record).unwrap();
    keys(
        &record,
        &[
            "schema_version",
            "role",
            "branches",
            "billing",
            "required_pair_ready_us",
            "required_pair_ok",
            "assembly_start_us",
            "assembly_end_us",
            "assembly_elapsed_us",
            "complete_elapsed_us",
            "outcome",
        ],
    );
    keys(
        &record["branches"],
        &[
            "me",
            "graphs",
            "admin_join_count",
            "pending_orgs",
            "selfserve_org",
            "my_join_requests",
            "billing_banner",
        ],
    );
    keys(&record["billing"], &["db", "provider"]);
    for timing in record["branches"]
        .as_object()
        .unwrap()
        .values()
        .chain(record["billing"].as_object().unwrap().values())
    {
        keys(
            timing,
            &["start_us", "end_us", "elapsed_us", "executed", "outcome"],
        );
        for field in ["start_us", "end_us", "elapsed_us"] {
            assert!(timing[field].is_null() || timing[field].as_u64().is_some());
        }
        assert!(timing["executed"].is_boolean());
        assert!([
            "not_reached",
            "ok",
            "skipped",
            "timeout",
            "refused",
            "error",
            "cancelled"
        ]
        .contains(&timing["outcome"].as_str().unwrap()));
    }
    assert!(["owner", "admin", "member", "other"].contains(&record["role"].as_str().unwrap()));
    assert!(serde_json::to_string(&record).unwrap().len() < 1800);
    // Prove a size bound at maximum numeric width as well as real samples.
    let mut widest = record.clone();
    for group in ["branches", "billing"] {
        for timing in widest[group].as_object_mut().unwrap().values_mut() {
            for field in ["start_us", "end_us", "elapsed_us"] {
                timing[field] = json!(u64::MAX);
            }
            timing["executed"] = json!(false);
            timing["outcome"] = json!("not_reached");
        }
    }
    for field in [
        "required_pair_ready_us",
        "assembly_start_us",
        "assembly_end_us",
        "assembly_elapsed_us",
        "complete_elapsed_us",
    ] {
        widest[field] = json!(u64::MAX);
    }
    widest["schema_version"] = json!(u8::MAX);
    widest["required_pair_ok"] = json!(false);
    widest["role"] = json!("member");
    widest["outcome"] = json!("not_reached");
    assert!(serde_json::to_string(&widest).unwrap().len() < 1800);
}

#[tokio::test]
async fn optional_decode_failures_are_redacted_and_unknown_does_not_become_empty() {
    let capture = bootstrap_trace::Capture::default();
    let _subscriber = tracing::subscriber::set_default(capture.subscriber());
    for response in [
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "secret raw SQL/account/token",
        )
            .into_response(),
        (StatusCode::OK, "not JSON; secret payment data").into_response(),
        Response::new(axum::body::Body::from_stream(futures_util::stream::once(
            async { Err::<Vec<u8>, _>(std::io::Error::other("private body error")) },
        ))),
    ] {
        let result = optional_named_part(response).await;
        assert!(result.value.is_null());
        assert_eq!(result.outcome, Outcome::Error);
    }
    assert!(
        capture.take().is_empty(),
        "raw body/SQL errors must never become telemetry"
    );
    let state = crate::test_app_state();
    let user = UserContext {
        user_id: uuid::Uuid::nil(),
        account_id: uuid::Uuid::nil(),
        role: "owner".into(),
        device_id: "private-device".into(),
    };
    assert!(pending_orgs_part(&state, &user).await.value.is_null());
    assert!(selfserve_org_part(&state, &user).await.value.is_null());
}

#[tokio::test(start_paused = true)]
async fn count_body_decode_stays_in_assembly_and_graphs_refusal_keeps_its_body() {
    let (body_read, body_started) = oneshot::channel();
    let (release, body_gate) = oneshot::channel();
    let task = tokio::spawn(async move {
        measurement::scope("member", async {
            let parts = join_parts(
                async { required_value(StatusCode::OK, json!({})) },
                async { required_value(StatusCode::OK, json!([])) },
                async {
                    Response::new(axum::body::Body::from_stream(futures_util::stream::once(
                        async {
                            body_read.send(()).unwrap();
                            body_gate.await.unwrap();
                            Ok::<_, std::io::Error>("private non-JSON body/error/token")
                        },
                    )))
                },
                async { PartValue::null(Outcome::Skipped) },
                async { PartValue::null(Outcome::Skipped) },
                async { PartValue::null(Outcome::Skipped) },
                async { PartValue::null(Outcome::Skipped) },
            )
            .await;
            let response = assemble(parts).await;
            measurement::assembly_end(response.status());
            response
        })
        .await
    });
    body_started.await.unwrap();
    advance(Duration::from_millis(100)).await;
    release.send(()).unwrap();
    let (response, record) = task.await.unwrap();
    assert_eq!(record.required_pair_ready_us, 0);
    assert_eq!(record.branches.admin_join_count.end_us, Some(0));
    assert_eq!(record.branches.admin_join_count.outcome, Outcome::Error);
    assert_eq!(record.assembly_start_us, 0);
    assert_eq!(record.assembly_end_us, 100_000);
    assert_eq!(record.assembly_elapsed_us, 100_000);
    assert_eq!(record.complete_elapsed_us, 100_000);
    let body: Value = serde_json::from_slice(
        &axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert!(body["join_requests_count"].is_null());
    assert_privacy_schema(&record);
    let (response, record) = measurement::scope("member", async {
        let parts = join_parts(
            async { required_value(StatusCode::OK, json!({})) },
            async {
                required_value(
                    StatusCode::SERVICE_UNAVAILABLE,
                    json!({"error": "same required error"}),
                )
            },
            // Refusal must not try to decode this unreadable optional body.
            async { (StatusCode::OK, "bad body").into_response() },
            async { PartValue::null(Outcome::Skipped) },
            async { PartValue::null(Outcome::Skipped) },
            async { PartValue::null(Outcome::Skipped) },
            async { PartValue::null(Outcome::Skipped) },
        )
        .await;
        let response = assemble(parts).await;
        measurement::assembly_end(response.status());
        response
    })
    .await;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(record.branches.graphs.outcome, Outcome::Error);
    assert_eq!(record.branches.admin_join_count.outcome, Outcome::Ok);
    assert!(!record.required_pair_ok);
    assert_eq!(
        axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap(),
        serde_json::to_vec(&json!({"error": "same required error"})).unwrap()
    );
}
