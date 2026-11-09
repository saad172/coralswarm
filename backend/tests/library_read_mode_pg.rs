//! The reader's mode is the process's kill switch and nothing else. One child
//! process per environment, because `read_model::mode()` is a process
//! OnceLock: the parent builds one Model-ready database and re-execs itself
//! per case; each child reads its mode the way boot does, serves one search
//! through the real handler and proves which path answered.
//!
//! Case `prod_secrets` is prod's current `fly secrets`: the two
//! `LIBRARY_PROOF_*` values that used to pin a sealed run now decide nothing.
mod support;
use coralswarm_backend::library_read::read_model::{self, Mode};
use serde_json::{json, Value};
use support::library_fixture::{search, Harness, LogBuf};
use uuid::Uuid;

const CHILD: &str = "CS_LIBRARY_READ_MODE_CHILD";
const DB: &str = "CS_LIBRARY_READ_MODE_DB";
const GRAPH: &str = "CS_LIBRARY_READ_MODE_GRAPH";
const USER: &str = "CS_LIBRARY_READ_MODE_USER";
const ACCOUNT: &str = "CS_LIBRARY_READ_MODE_ACCOUNT";
const EXPECT: &str = "CS_LIBRARY_READ_MODE_EXPECT";

struct Case {
    name: &'static str,
    env: &'static [(&'static str, &'static str)],
    expect: &'static str,
}
const CASES: &[Case] = &[
    Case { name: "prod_secrets", env: &[("LIBRARY_READ_MODEL", "on"), ("LIBRARY_PROOF_RUN_ID", "d34b6315-3688-509b-a25d-bf6894703ef8"), ("LIBRARY_PROOF_OPERATOR_ACCOUNT_ID", "8e16ded0-c9a0-47cd-9952-b552ad66b86f")], expect: "model" },
    Case { name: "unset", env: &[], expect: "model" },
    Case { name: "off", env: &[("LIBRARY_READ_MODEL", "off")], expect: "live" },
    Case { name: "shadow", env: &[("LIBRARY_READ_MODEL", "shadow")], expect: "model_warned" },
    Case { name: "garbage", env: &[("LIBRARY_READ_MODEL", "true")], expect: "model_warned" },
];

#[tokio::test]
async fn every_environment_boots_and_only_off_serves_live() {
    if std::env::var_os(CHILD).is_some() {
        return child().await;
    }
    let harness = Harness::new("library_read_mode").await;
    let (member, _node) = harness.ready_member().await;
    for case in CASES {
        let mut command = std::process::Command::new(std::env::current_exe().unwrap());
        command.args(["--exact", "every_environment_boots_and_only_off_serves_live", "--nocapture"]);
        for name in ["LIBRARY_READ_MODEL", "LIBRARY_PROOF_RUN_ID", "LIBRARY_PROOF_OPERATOR_ACCOUNT_ID", "FLY_MACHINE_ID", "LIBRARY_PROOF_MACHINE_ID"] {
            command.env_remove(name);
        }
        for (name, value) in case.env {
            command.env(name, value);
        }
        let output = command
            .env(CHILD, case.name)
            .env(DB, harness.db.name())
            .env(GRAPH, member.graph.to_string())
            .env(USER, member.user.user_id.to_string())
            .env(ACCOUNT, member.user.account_id.to_string())
            .env(EXPECT, case.expect)
            .output()
            .unwrap();
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(output.status.success(), "case {} failed:\n{stdout}\n{stderr}", case.name);
        assert!(stdout.contains("1 passed; 0 failed;"), "case {} ran no test:\n{stdout}", case.name);
        assert!(stderr.contains(&format!("LIBRARY_READ_MODE_PROVEN {}", case.name)), "case {} did not reach its proof:\n{stderr}", case.name);
        println!("case {}: {}", case.name, case.expect);
    }
    println!("PostgreSQL {}: {} environments proven", harness.version, CASES.len());
    harness.close().await;
}

async fn child() {
    let name = std::env::var(CHILD).unwrap();
    let expect = std::env::var(EXPECT).unwrap();
    let buf = LogBuf::default();
    let subscriber = tracing_subscriber::fmt().json().with_writer(buf.clone()).with_ansi(false).finish();
    let _guard = tracing::subscriber::set_default(subscriber);

    // The call boot makes, under this child's environment.
    let mode = read_model::mode();
    let boot_lines = buf.drain_lines();
    let warnings: Vec<&Value> = boot_lines.iter().filter(|l| l["level"] == "WARN").collect();
    match expect.as_str() {
        "model" => {
            assert_eq!(mode, Mode::On);
            assert!(warnings.is_empty(), "{boot_lines:?}");
        }
        "model_warned" => {
            assert_eq!(mode, Mode::On);
            assert_eq!(warnings.len(), 1, "{boot_lines:?}");
            assert!(warnings[0]["fields"]["message"].as_str().unwrap().contains("Model path"), "{boot_lines:?}");
        }
        "live" => {
            assert_eq!(mode, Mode::Off);
            assert!(warnings.is_empty(), "{boot_lines:?}");
        }
        other => panic!("unknown expectation {other}"),
    }

    let (options, _) = support::library_fixture::base_options();
    let app = Harness::app_pool(&options, &std::env::var(DB).unwrap()).await;
    let tenant = coralswarm_backend::tenancy::extractor::Tenant {
        store: Some(std::sync::Arc::new(coralswarm_backend::store::PostgresStore::new(app.clone()))),
        pool: Some(app.clone()),
        decision: None,
        class: coralswarm_backend::route_class::RouteClass::Other,
    };
    let graph: Uuid = std::env::var(GRAPH).unwrap().parse().unwrap();
    let user = coralswarm_backend::UserContext {
        user_id: std::env::var(USER).unwrap().parse().unwrap(),
        account_id: std::env::var(ACCOUNT).unwrap().parse().unwrap(),
        role: "owner".into(),
        device_id: "library-read-mode".into(),
    };
    let (status, body) = search(&tenant, &user, json!({"graph_ids": [graph]})).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["nodes"]["total"], 1, "the fixture graph's one node is served: {body}");
    let (events, metrics) = buf.drain();
    if expect == "live" {
        assert_eq!(events.len(), 1, "one Live serve logs one fallback event: {events:?}");
        assert_eq!((&events[0]["fields"]["mode"], &events[0]["fields"]["reason"]), (&json!("off"), &json!("disabled")));
        assert_eq!(events[0]["level"], "INFO");
        assert_eq!(metrics.len(), 1, "{metrics:?}");
        assert_eq!(metrics[0]["fields"]["reason"], "disabled");
    } else {
        assert!(events.is_empty() && metrics.is_empty(), "a Model serve logs no fallback: {events:?} {metrics:?}");
    }
    app.close().await;
    eprintln!("LIBRARY_READ_MODE_PROVEN {name}");
}
