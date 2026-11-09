//! Every tenant is admitted to the Model program; only the program itself can
//! still choose Live for a request. A Live serve logs `library_live_fallback`
//! once, naming the program's reason; a Model serve logs nothing.
mod support;
use axum::http::StatusCode;
use serde_json::json;
use support::library_fixture::{search, Harness, LogBuf};

#[tokio::test]
async fn a_program_fallback_logs_once_and_a_model_serve_logs_nothing() {
    for name in ["LIBRARY_READ_MODEL", "LIBRARY_PROOF_RUN_ID", "LIBRARY_PROOF_OPERATOR_ACCOUNT_ID"] {
        std::env::remove_var(name);
    }
    let harness = Harness::new("library_live_fallback").await;
    let (member, node) = harness.ready_member().await;
    let newcomer = support::library_fixture::member(&harness.owner).await;

    let buf = LogBuf::default();
    let subscriber = tracing_subscriber::fmt().json().with_writer(buf.clone()).with_ansi(false).finish();
    let _guard = tracing::subscriber::set_default(subscriber);

    let (status, body) = search(&harness.tenant, &member.user, json!({"graph_ids": [member.graph]})).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["nodes"]["total"], 1, "{body}");
    let (events, metrics) = buf.drain();
    assert!(events.is_empty() && metrics.is_empty(), "a Model serve logs no fallback: {events:?} {metrics:?}");

    // No cohort: a graph the worker has not certified yet is admitted and
    // falls back inside the program, as `not_ready`.
    let (status, _) = search(&harness.tenant, &newcomer.user, json!({"graph_ids": [newcomer.graph]})).await;
    assert_eq!(status, StatusCode::OK);
    let (events, metrics) = buf.drain();
    assert_eq!(events.len(), 1, "{events:?}");
    let fields = &events[0]["fields"];
    assert_eq!(events[0]["level"], "WARN");
    assert_eq!((&fields["reason"], &fields["mode"], &fields["plane"]), (&json!("not_ready"), &json!("on"), &json!("consumer")));
    assert_eq!(fields["account_id"], newcomer.user.account_id.to_string());
    assert!(fields["graph_ids"].as_str().unwrap().contains(&newcomer.graph.to_string()), "{fields}");
    assert_eq!(metrics.len(), 1, "{metrics:?}");
    assert_eq!(metrics[0]["fields"]["reason"], "not_ready");

    let (status, _) = search(&harness.tenant, &member.user, json!({"graph_ids": [member.graph], "node_id": node})).await;
    assert_eq!(status, StatusCode::OK);
    let (events, metrics) = buf.drain();
    assert_eq!(events.len(), 1, "{events:?}");
    let fields = &events[0]["fields"];
    assert_eq!((&fields["reason"], &fields["node_scoped"]), (&json!("narrow_scope"), &json!(true)));
    assert_eq!(fields["account_id"], member.user.account_id.to_string());
    assert_eq!(metrics.len(), 1, "{metrics:?}");
    assert_eq!(metrics[0]["fields"]["reason"], "scoped_request");

    println!("PostgreSQL {}: fallback evidence surface proven", harness.version);
    harness.close().await;
}
