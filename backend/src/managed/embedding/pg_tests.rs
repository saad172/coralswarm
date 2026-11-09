use super::*;
use axum::{extract::State, routing::post, Json, Router};
use coralswarm_core::embed_openrouter::OpenRouterEmbedder;
use serde_json::{json, Value};
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    PgPool,
};
use std::{
    str::FromStr,
    sync::{Arc, Mutex},
};

struct Fixture {
    admin: PgPool,
    owner: PgPool,
    app: PgPool,
    db: crate::test_db::TestDatabase,
    tenant: bool,
    account: Uuid,
    user: Uuid,
    graph: Uuid,
    atoms: Vec<Uuid>,
}

impl Fixture {
    async fn new(tenant: bool) -> Self {
        let base = std::env::var("ORGT_TEST_BASE_URL")
            .expect("disposable local ORGT_TEST_BASE_URL required; accounting tests never skip");
        let opts = PgConnectOptions::from_str(&base).unwrap();
        assert!(matches!(opts.get_host(), "127.0.0.1" | "localhost" | "::1"));
        let admin = PgPoolOptions::new()
            .max_connections(2)
            .connect_with(opts.clone())
            .await
            .unwrap();
        let db = crate::test_db::TestDatabase::new(&opts, "embedding_accounting");
        sqlx::query(&format!("CREATE DATABASE {db}"))
            .execute(&admin)
            .await
            .unwrap();
        let owner = PgPoolOptions::new()
            .max_connections(8)
            .connect_with(opts.clone().database(&db))
            .await
            .unwrap();
        if tenant {
            sqlx::migrate!("./migrations_tenant")
                .run(&owner)
                .await
                .unwrap();
        } else {
            sqlx::migrate!("./migrations_control")
                .run(&owner)
                .await
                .unwrap();
        }
        let app = PgPoolOptions::new()
            .max_connections(8)
            .after_connect(move |conn, _| {
                Box::pin(async move {
                    sqlx::query(if tenant {
                        "SET ROLE coralswarm_tenant_app"
                    } else {
                        "SET ROLE coralswarm_app"
                    })
                    .execute(conn)
                    .await?;
                    Ok(())
                })
            })
            .connect_with(opts.database(&db))
            .await
            .unwrap();
        let restricted: bool = sqlx::query_scalar(
            "SELECT NOT rolsuper AND NOT rolbypassrls FROM pg_roles WHERE rolname=current_user",
        )
        .fetch_one(&app)
        .await
        .unwrap();
        assert!(
            restricted,
            "fixture must exercise actual restricted app role"
        );
        let account = Uuid::new_v4();
        let user = Uuid::new_v4();
        if !tenant {
            sqlx::query("INSERT INTO users(id,email) VALUES($1,$2)")
                .bind(user)
                .bind(format!("{user}@embedding.test"))
                .execute(&owner)
                .await
                .unwrap();
            sqlx::query("INSERT INTO accounts(id,owner_user_id) VALUES($1,$2)")
                .bind(account)
                .bind(user)
                .execute(&owner)
                .await
                .unwrap();
        }
        let graph: Uuid = sqlx::query_scalar(
            "INSERT INTO graphs(account_id,name) VALUES($1,'embedding accounting') RETURNING id",
        )
        .bind(account)
        .fetch_one(&owner)
        .await
        .unwrap();
        let mut atoms = Vec::new();
        for index in 0..3 {
            atoms.push(sqlx::query_scalar("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,timestamp) VALUES($1,'note',$2,'','source fixture',now()) RETURNING atom_id").bind(graph).bind(format!("fixture-{index}")).fetch_one(&owner).await.unwrap());
        }
        Self {
            admin,
            owner,
            app,
            db,
            tenant,
            account,
            user,
            graph,
            atoms,
        }
    }

    fn plane(&self) -> JobScope<'_> {
        if self.tenant {
            JobScope::Org(&self.app)
        } else {
            JobScope::Consumer {
                pool: &self.app,
                account_id: self.account,
            }
        }
    }

    fn batch(&self) -> ReceiptBatch {
        ReceiptBatch::checked(
            self.account,
            self.graph,
            Uuid::new_v4(),
            self.user,
            self.atoms
                .iter()
                .zip(["reef α", "ocean\nβ", "coral 🪸"])
                .map(|(id, text)| ReceiptInput {
                    atom_id: *id,
                    content_revision: 1,
                    text: text.into(),
                })
                .collect(),
        )
        .unwrap()
    }

    async fn counts(&self) -> (i64, i64, i64) {
        sqlx::query_as("SELECT (SELECT count(*) FROM managed_attempt_receipts),(SELECT count(*) FROM managed_embedding_inputs),COALESCE((SELECT sum(reserved_micro_usd)::bigint FROM managed_budget_periods),0)").fetch_one(&self.owner).await.unwrap()
    }

    async fn finish(self) {
        self.app.close().await;
        self.owner.close().await;
        sqlx::query(&format!("DROP DATABASE {} WITH (FORCE)", self.db))
            .execute(&self.admin)
            .await
            .unwrap();
        self.admin.close().await;
    }
}

fn sheet(rate: f64) -> PriceSheet {
    sheet_for("text-embedding-3-small", rate)
}

fn sheet_for(pattern: &str, rate: f64) -> PriceSheet {
    let model = crate::billing::rates::RateRow {
        provider: "openrouter".into(),
        model_pattern: pattern.into(),
        priority: 0,
        input_per_million: rate,
        output_per_million: 9999.0,
        cache_write_multiplier: 1.0,
        cache_read_multiplier: 1.0,
        effective_from: DateTime::<Utc>::from_timestamp(0, 0).unwrap(),
        effective_to: None,
        is_blend_fallback: false,
    };
    let blend = crate::billing::rates::RateRow {
        model_pattern: "blend".into(),
        is_blend_fallback: true,
        ..model.clone()
    };
    PriceSheet::from_rows(vec![model, blend])
}

fn rate_history() -> (PriceSheet, DateTime<Utc>) {
    let boundary = Utc::now() - chrono::Duration::days(1);
    let historical = crate::billing::rates::RateRow {
        provider: "openrouter".into(),
        model_pattern: "text-embedding-3-small".into(),
        priority: 0,
        input_per_million: 0.02,
        output_per_million: 9999.0,
        cache_write_multiplier: 1.0,
        cache_read_multiplier: 1.0,
        effective_from: DateTime::<Utc>::from_timestamp(0, 0).unwrap(),
        effective_to: Some(boundary),
        is_blend_fallback: false,
    };
    let current = crate::billing::rates::RateRow {
        input_per_million: 0.50,
        effective_from: boundary,
        effective_to: None,
        ..historical.clone()
    };
    let blend = crate::billing::rates::RateRow {
        model_pattern: "blend".into(),
        is_blend_fallback: true,
        ..current.clone()
    };
    (
        PriceSheet::from_rows(vec![historical, current, blend]),
        boundary - chrono::Duration::seconds(1),
    )
}

fn caps() -> Caps {
    Caps {
        disabled: false,
        credit_enforcement: true,
        daily_extraction: MicroUsd(0),
        daily_embedding: MicroUsd(1_000_000),
        daily_enrichment: MicroUsd(0),
        daily_interactive_ask: MicroUsd(0),
        per_attempt: MicroUsd(50_000),
        per_version: MicroUsd(0),
        headroom: 99.0,
        reap_after_secs: 600,
    }
}

fn gate<'a>(plane: &'a JobScope<'a>, batch: ReceiptBatch, caps: Caps) -> EmbeddingGate<'a> {
    let sheet = sheet(0.02);
    let worst = embedding_worst_case(&batch, &sheet, Utc::now()).unwrap();
    EmbeddingGate {
        plane,
        batch,
        logical_call: Uuid::new_v4(),
        worst,
        caps,
        sheet,
        reserved_once: AtomicBool::new(false),
    }
}

#[derive(Clone)]
struct HttpState {
    requests: Arc<Mutex<Vec<Value>>>,
    response: String,
    status: axum::http::StatusCode,
    delay_ms: u64,
}

async fn loopback(
    response: Value,
    status: axum::http::StatusCode,
    delay_ms: u64,
) -> (OpenRouterEmbedder, HttpState, tokio::task::JoinHandle<()>) {
    loopback_raw(response.to_string(), status, delay_ms).await
}

async fn loopback_raw(
    response: String,
    status: axum::http::StatusCode,
    delay_ms: u64,
) -> (OpenRouterEmbedder, HttpState, tokio::task::JoinHandle<()>) {
    let state = HttpState {
        requests: Arc::new(Mutex::new(Vec::new())),
        response,
        status,
        delay_ms,
    };
    let router = Router::new()
        .route(
            "/v1/embeddings",
            post(
                |State(state): State<HttpState>, Json(body): Json<Value>| async move {
                    state.requests.lock().unwrap().push(body);
                    tokio::time::sleep(std::time::Duration::from_millis(state.delay_ms)).await;
                    (state.status, state.response)
                },
            ),
        )
        .with_state(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_millis(200))
        .build()
        .unwrap();
    (
        OpenRouterEmbedder::new(client, "local-accounting-only")
            .with_base_url(format!("http://{address}")),
        state,
        server,
    )
}

fn response(usage: Value) -> Value {
    json!({"model": MODEL,"usage":usage,"data": (0..3).map(|index| json!({"index":index,"embedding":vec![0.5;384]})).collect::<Vec<_>>()})
}

async fn send(client: &OpenRouterEmbedder, gate: &EmbeddingGate<'_>) -> bool {
    let texts: Vec<_> = gate
        .batch
        .inputs
        .iter()
        .map(|input| input.text.as_str())
        .collect();
    crate::embedding::submit(client, crate::embedding::Purpose::Managed(gate), &texts).await.is_ok()
}

async fn accounting(tenant: bool) {
    let f = Fixture::new(tenant).await;
    let plane = f.plane();
    sqlx::query("INSERT INTO managed_budget_periods(account_id,purpose,period_start,limit_micro_usd) VALUES($1,'cloud_embedding',(now() AT TIME ZONE 'UTC')::date-1,77)")
        .bind(f.account).execute(&f.owner).await.unwrap();
    let mut first_caps = caps();
    first_caps.daily_embedding = MicroUsd(1);
    let first = gate(&plane, f.batch(), first_caps);
    let bound = first.worst.per_attempt.get();
    assert_eq!(bound, 1);
    assert_eq!(first.worst.max_tokens, 0);
    let (client, http, server) = loopback(
        response(json!({"cost":0.000001})),
        axum::http::StatusCode::OK,
        0,
    )
    .await;
    assert!(send(&client, &first).await);
    assert!(
        !send(&client, &first).await,
        "one gate cannot authorize a second POST"
    );
    let requests = http.requests.lock().unwrap().clone();
    assert_eq!(requests.len(), 1);
    assert_eq!(
        requests[0]["input"],
        json!(["reef α", "ocean\nβ", "coral 🪸"])
    );
    assert_eq!(requests[0]["model"], MODEL);
    assert_eq!(requests[0]["dimensions"], 384);
    assert_eq!(requests[0]["provider"]["zdr"], true);
    assert_eq!(requests[0]["provider"]["data_collection"], "deny");
    assert_eq!(requests[0]["provider"]["allow_fallbacks"], false);
    assert_eq!(requests[0]["provider"]["max_price"]["prompt"], 0.02);
    assert_eq!(
        requests[0]["provider"]["max_price"]["request"].as_f64(),
        Some(0.0)
    );
    assert!(requests[0].get("models").is_none());
    assert_eq!(f.counts().await, (1, 3, 0));
    let rows: Vec<(Uuid,i16,i64,Vec<u8>,i64)> = sqlx::query_as("SELECT atom_id,ordinal,content_revision,content_sha256,input_bytes FROM managed_embedding_inputs ORDER BY ordinal").fetch_all(&f.owner).await.unwrap();
    for (index, (id, ordinal, revision, hash, bytes)) in rows.iter().enumerate() {
        let text = ["reef α", "ocean\nβ", "coral 🪸"][index];
        assert_eq!(
            (*id, *ordinal, *revision, *bytes),
            (f.atoms[index], index as i16, 1, text.len() as i64)
        );
        let expected = [
            "eccc900d96b64163be349151b501ee65fbe4b6af3dda2384e90520b4244eba17",
            "dcb558e819cf95f605ff37f96fa669f77a99fc4b7997b0a427b30a344b0d3bb8",
            "189ad3d304df19d5ee9df434af89b97b13ed23863d63e2147c1cbb22815c8d54",
        ][index];
        assert_eq!(
            hash.iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>(),
            expected
        );
    }
    let shape: (String,Option<Uuid>,Option<Uuid>,i16,i32,i64) = sqlx::query_as("SELECT purpose,unit_id,source_version_id,attempt_no,max_tokens,settled_micro_usd FROM managed_attempt_receipts").fetch_one(&f.owner).await.unwrap();
    assert_eq!(shape, ("cloud_embedding".into(), None, None, 1, 0, 1));

    for (account, graph, purpose, ordinal, revision, hash_bytes, input_bytes, code) in [
        (f.account, f.graph, "cloud_embedding", 7, 1, 31, 1, "23514"),
        (f.account, f.graph, "cloud_embedding", 8, 1, 32, 1, "23514"),
        (f.account, f.graph, "cloud_embedding", 7, 0, 32, 1, "23514"),
        (f.account, f.graph, "cloud_embedding", 7, 1, 32, 0, "23514"),
        (
            f.account,
            f.graph,
            "source_extraction",
            7,
            1,
            32,
            1,
            "23514",
        ),
        (
            f.account,
            Uuid::new_v4(),
            "cloud_embedding",
            7,
            1,
            32,
            1,
            "23503",
        ),
    ] {
        let mut tx = first.begin().await.unwrap();
        let error=sqlx::query("INSERT INTO managed_embedding_inputs(receipt_id,account_id,graph_id,purpose,atom_id,ordinal,content_revision,content_sha256,input_bytes) SELECT receipt_id,$1,$2,$3,$4,$5,$6,$7,$8 FROM managed_attempt_receipts WHERE logical_call=$9")
            .bind(account).bind(graph).bind(purpose).bind(Uuid::new_v4()).bind(ordinal as i16).bind(revision as i64).bind(vec![0u8;hash_bytes]).bind(input_bytes as i64).bind(first.logical_call).execute(&mut *tx).await.unwrap_err();
        assert_eq!(
            error.as_database_error().unwrap().code().as_deref(),
            Some(code)
        );
        tx.rollback().await.unwrap();
    }
    period_cap_snapshot(&f).await;
    let other_graph: Uuid = sqlx::query_scalar(
        "INSERT INTO graphs(account_id,name) VALUES($1,'second graph') RETURNING id",
    )
    .bind(f.account)
    .fetch_one(&f.owner)
    .await
    .unwrap();
    let other_atom: Uuid = sqlx::query_scalar("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,timestamp) VALUES($1,'note','second-graph','',now()) RETURNING atom_id").bind(other_graph).fetch_one(&f.owner).await.unwrap();
    let before = f.counts().await;
    for mutation in 0..5 {
        let mut batch = f.batch();
        match mutation {
            0 => batch.graph_id = Uuid::new_v4(),
            1 => batch.account_id = Uuid::new_v4(),
            2 => batch.inputs[1].content_revision = 2,
            3 => batch.inputs[1].atom_id = Uuid::new_v4(),
            _ => batch.inputs[1].atom_id = other_atom,
        }
        assert!(!send(&client, &gate(&plane, batch, caps())).await);
    }
    let mut limited = caps();
    limited.per_attempt = MicroUsd(0);
    assert!(!send(&client, &gate(&plane, f.batch(), limited)).await);
    sqlx::query("UPDATE managed_budget_periods SET limit_micro_usd=charged_micro_usd WHERE account_id=$1 AND purpose='cloud_embedding' AND period_start=(now() AT TIME ZONE 'UTC')::date")
        .bind(f.account).execute(&f.owner).await.unwrap();
    limited = caps();
    limited.daily_embedding = MicroUsd(1);
    assert!(!send(&client, &gate(&plane, f.batch(), limited)).await);
    assert_eq!(f.counts().await, before);
    assert_eq!(http.requests.lock().unwrap().len(), 1);
    sqlx::query("UPDATE managed_budget_periods SET limit_micro_usd=$2 WHERE account_id=$1 AND purpose='cloud_embedding' AND period_start=(now() AT TIME ZONE 'UTC')::date")
        .bind(f.account).bind(caps().daily_embedding.get()).execute(&f.owner).await.unwrap();
    server.abort();

    let rollback = gate(&plane, f.batch(), caps());
    let mut tx = rollback.begin().await.unwrap();
    reserve_inputs_tx(
        &mut tx,
        &rollback.batch,
        rollback.logical_call,
        &rollback.worst,
        &rollback.caps,
    )
    .await
    .unwrap();
    tx.rollback().await.unwrap();
    let mut partial = f.batch();
    partial.inputs[1].atom_id = partial.inputs[0].atom_id;
    let mut tx = rollback.begin().await.unwrap();
    assert!(reserve_inputs_tx(
        &mut tx,
        &partial,
        Uuid::new_v4(),
        &rollback.worst,
        &rollback.caps
    )
    .await
    .is_err());
    tx.rollback().await.unwrap();
    assert_eq!(
        f.counts().await,
        before,
        "rollback removes complete input set and financial reserve"
    );

    for (usage, status, delay, expected, ok) in [
        (
            json!({"prompt_tokens": 101}),
            axum::http::StatusCode::OK,
            0,
            3,
            true,
        ),
        (json!({}), axum::http::StatusCode::OK, 0, bound, true),
        (Value::Null, axum::http::StatusCode::OK, 0, bound, true),
        (
            json!({"cost":-1}),
            axum::http::StatusCode::OK,
            0,
            bound,
            true,
        ),
        (
            json!({}),
            axum::http::StatusCode::TOO_MANY_REQUESTS,
            0,
            0,
            false,
        ),
        (json!({}), axum::http::StatusCode::OK, 400, bound, false),
    ] {
        let current = gate(&plane, f.batch(), caps());
        let (client, http, server) = loopback(response(usage), status, delay).await;
        assert_eq!(send(&client, &current).await, ok);
        assert_eq!(http.requests.lock().unwrap().len(), 1);
        let charged: i64 = sqlx::query_scalar(
            "SELECT settled_micro_usd FROM managed_attempt_receipts WHERE logical_call=$1",
        )
        .bind(current.logical_call)
        .fetch_one(&f.owner)
        .await
        .unwrap();
        assert_eq!(charged, expected);
        server.abort();
    }

    for (body, expected) in [
        (
            json!({"model":MODEL,"data":(0..3).map(|index|json!({"index":index,"embedding":vec![0.5;384]})).collect::<Vec<_>>()}),
            bound,
        ),
        (
            json!({"model":MODEL,"usage":{"cost":0.000004},"data":[]}),
            4,
        ),
        (
            json!({"model":"unmatched/model","usage":{"prompt_tokens":101},"data":[]}),
            bound,
        ),
    ] {
        let current = gate(&plane, f.batch(), caps());
        let (client, http, server) = loopback(body, axum::http::StatusCode::OK, 0).await;
        let _ = send(&client, &current).await;
        assert_eq!(http.requests.lock().unwrap().len(), 1);
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT settled_micro_usd FROM managed_attempt_receipts WHERE logical_call=$1"
            )
            .bind(current.logical_call)
            .fetch_one(&f.owner)
            .await
            .unwrap(),
            expected
        );
        server.abort();
    }
    for raw in [
        "{",
        r#"{"model":"openai/text-embedding-3-small","usage":{"cost":1e400}}"#,
    ] {
        let current = gate(&plane, f.batch(), caps());
        let (client, http, server) = loopback_raw(raw.into(), axum::http::StatusCode::OK, 0).await;
        assert!(!send(&client, &current).await);
        assert_eq!(http.requests.lock().unwrap().len(), 1);
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT settled_micro_usd FROM managed_attempt_receipts WHERE logical_call=$1"
            )
            .bind(current.logical_call)
            .fetch_one(&f.owner)
            .await
            .unwrap(),
            bound
        );
        server.abort();
    }

    let reset = gate(&plane, f.batch(), caps());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let reset_server = tokio::spawn(async move {
        use tokio::io::AsyncReadExt;
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut bytes = [0u8; 8192];
        let received = socket.read(&mut bytes).await.unwrap();
        assert!(bytes[..received].starts_with(b"POST /v1/embeddings "));
    });
    let reset_client = OpenRouterEmbedder::new(reqwest::Client::new(), "local-accounting-only")
        .with_base_url(format!("http://{address}"));
    assert!(!send(&reset_client, &reset).await);
    reset_server.await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT settled_micro_usd FROM managed_attempt_receipts WHERE logical_call=$1"
        )
        .bind(reset.logical_call)
        .fetch_one(&f.owner)
        .await
        .unwrap(),
        bound
    );

    for model in [None, Some("wrong/embedding-model")] {
        let current = gate(&plane, f.batch(), caps());
        let mut body = response(json!({"cost":0.000002}));
        if let Some(model) = model {
            body["model"] = json!(model);
        } else {
            body.as_object_mut().unwrap().remove("model");
        }
        let (client, http, server) = loopback(body, axum::http::StatusCode::OK, 0).await;
        assert!(
            !send(&client, &current).await,
            "managed response must explicitly identify the priced model"
        );
        assert_eq!(http.requests.lock().unwrap().len(), 1);
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT settled_micro_usd FROM managed_attempt_receipts WHERE logical_call=$1"
            )
            .bind(current.logical_call)
            .fetch_one(&f.owner)
            .await
            .unwrap(),
            2
        );
        server.abort();
    }
    rate_boundary(&f).await;
    lost_commit_ack(&f).await;

    let canceled = gate(&plane, f.batch(), caps());
    let permit = coralswarm_core::send_gate::acquire(&canceled, 1)
        .await
        .unwrap();
    let receipt = permit.into_sent();
    sqlx::query("UPDATE managed_attempt_receipts SET created_at=now()-interval '1 hour' WHERE logical_call=$1").bind(canceled.logical_call).execute(&f.owner).await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i32>("SELECT managed_reap_stale(interval '10 minutes')")
            .fetch_one(&f.app)
            .await
            .unwrap(),
        1
    );
    canceled.settle(receipt, &SendOutcome::NotGenerated).await;
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT settled_micro_usd FROM managed_attempt_receipts WHERE logical_call=$1"
        )
        .bind(canceled.logical_call)
        .fetch_one(&f.owner)
        .await
        .unwrap(),
        bound
    );
    assert!(canceled.reserve(1).await.is_err());

    let charged: i64 = sqlx::query_scalar(
        "SELECT charged_micro_usd FROM managed_budget_periods WHERE account_id=$1 AND purpose='cloud_embedding' AND period_start=(now() AT TIME ZONE 'UTC')::date",
    )
    .bind(f.account)
    .fetch_one(&f.owner)
    .await
    .unwrap();
    let mut shared_cap = caps();
    shared_cap.daily_embedding = MicroUsd(charged + 1);
    sqlx::query("UPDATE managed_budget_periods SET limit_micro_usd=$2 WHERE account_id=$1 AND purpose='cloud_embedding' AND period_start=(now() AT TIME ZONE 'UTC')::date")
        .bind(f.account).bind(charged+1).execute(&f.owner).await.unwrap();
    let a = gate(&plane, f.batch(), shared_cap);
    let b = gate(
        &plane,
        ReceiptBatch::checked(
            f.account,
            other_graph,
            Uuid::new_v4(),
            f.user,
            vec![ReceiptInput {
                atom_id: other_atom,
                content_revision: 1,
                text: "other".into(),
            }],
        )
        .unwrap(),
        shared_cap,
    );
    let (a, b) = tokio::join!(a.reserve(1), b.reserve(1));
    assert_eq!(usize::from(a.is_ok()) + usize::from(b.is_ok()), 1);
    assert!(sqlx::query_scalar::<_,bool>("SELECT bool_and(reserved_micro_usd+charged_micro_usd<=limit_micro_usd) FROM managed_budget_periods").fetch_one(&f.owner).await.unwrap());
    let evidence_before: i64 =
        sqlx::query_scalar("SELECT count(*) FROM managed_embedding_inputs WHERE atom_id=ANY($1)")
            .bind(&f.atoms)
            .fetch_one(&f.owner)
            .await
            .unwrap();
    billing_once(&f).await;
    sqlx::query("DELETE FROM atoms WHERE atom_id=ANY($1)")
        .bind(&f.atoms)
        .execute(&f.owner)
        .await
        .unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM managed_embedding_inputs WHERE atom_id=ANY($1)"
        )
        .bind(&f.atoms)
        .fetch_one(&f.owner)
        .await
        .unwrap(),
        evidence_before
    );
    drop(plane);
    f.finish().await;
}

async fn period_cap_snapshot(f: &Fixture) {
    let plane = f.plane();
    let before = f.counts().await;
    let periods: i64 = sqlx::query_scalar("SELECT count(*) FROM managed_budget_periods WHERE account_id=$1 AND purpose='cloud_embedding'")
        .bind(f.account).fetch_one(&f.owner).await.unwrap();
    assert_eq!(
        periods, 2,
        "first use creates a fresh UTC period independently of the prior day's cap"
    );
    let initial: (i64,i64,i64) = sqlx::query_as("SELECT limit_micro_usd,reserved_micro_usd,charged_micro_usd FROM managed_budget_periods WHERE account_id=$1 AND purpose='cloud_embedding' AND period_start=(now() AT TIME ZONE 'UTC')::date")
        .bind(f.account).fetch_one(&f.owner).await.unwrap();
    assert_eq!(initial, (1, 0, 1));
    let (client, http, server) = loopback(
        response(json!({"cost":0.000001})),
        axum::http::StatusCode::OK,
        0,
    )
    .await;
    let mut higher = caps();
    higher.daily_embedding = MicroUsd(100);
    let admitted = send(&client, &gate(&plane, f.batch(), higher)).await;
    let limit: i64 = sqlx::query_scalar("SELECT limit_micro_usd FROM managed_budget_periods WHERE account_id=$1 AND purpose='cloud_embedding' AND period_start=(now() AT TIME ZONE 'UTC')::date")
        .bind(f.account).fetch_one(&f.owner).await.unwrap();
    let posts = http.requests.lock().unwrap().len();
    let after = f.counts().await;
    assert_eq!(
        (posts, after, limit),
        (0, before, 1),
        "a higher rolling process cap cannot reopen the exhausted period snapshot"
    );
    assert!(!admitted);

    sqlx::query("UPDATE managed_budget_periods SET limit_micro_usd=3 WHERE account_id=$1 AND purpose='cloud_embedding' AND period_start=(now() AT TIME ZONE 'UTC')::date")
        .bind(f.account).execute(&f.owner).await.unwrap();
    let mut lower = caps();
    lower.daily_embedding = MicroUsd(0);
    assert!(
        send(&client, &gate(&plane, f.batch(), lower)).await,
        "an explicit operator limit applies despite a lower process cap"
    );
    assert!(send(&client, &gate(&plane, f.batch(), higher)).await);
    let operator: (i64,i64,i64) = sqlx::query_as("SELECT limit_micro_usd,reserved_micro_usd,charged_micro_usd FROM managed_budget_periods WHERE account_id=$1 AND purpose='cloud_embedding' AND period_start=(now() AT TIME ZONE 'UTC')::date")
        .bind(f.account).fetch_one(&f.owner).await.unwrap();
    assert_eq!(
        operator,
        (3, 0, 3),
        "later process caps preserve the operator's open-period limit"
    );
    let exhausted = f.counts().await;
    assert!(!send(&client, &gate(&plane, f.batch(), higher)).await);
    assert_eq!(f.counts().await, exhausted);
    assert_eq!(http.requests.lock().unwrap().len(), 2);
    let previous: i64 = sqlx::query_scalar("SELECT limit_micro_usd FROM managed_budget_periods WHERE account_id=$1 AND purpose='cloud_embedding' AND period_start=(now() AT TIME ZONE 'UTC')::date-1")
        .bind(f.account).fetch_one(&f.owner).await.unwrap();
    assert_eq!(previous, 77);
    sqlx::query("UPDATE managed_budget_periods SET limit_micro_usd=$2 WHERE account_id=$1 AND purpose='cloud_embedding' AND period_start=(now() AT TIME ZONE 'UTC')::date")
        .bind(f.account).bind(caps().daily_embedding.get()).execute(&f.owner).await.unwrap();
    server.abort();
}

async fn rate_boundary(f: &Fixture) {
    let plane = f.plane();
    let mut batch = f.batch();
    let bytes = batch
        .inputs
        .iter()
        .map(|input| input.text.len())
        .sum::<usize>();
    batch.inputs[0].text.push_str(&"x".repeat(100 - bytes));
    let mut current = gate(&plane, batch, caps());
    let (sheet, as_of) = rate_history();
    current.sheet = sheet;
    current.worst = embedding_worst_case(&current.batch, &current.sheet, as_of).unwrap();
    assert_eq!(
        current
            .sheet
            .rate("openrouter", MODEL, Utc::now())
            .input_per_million,
        0.50
    );
    assert_eq!(current.worst.per_attempt.get(), 2);
    let (client, http, server) = loopback(
        response(json!({"prompt_tokens":100})),
        axum::http::StatusCode::OK,
        0,
    )
    .await;
    assert!(send(&client, &current).await);
    let requests = http.requests.lock().unwrap().clone();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0]["provider"]["max_price"]["prompt"], 0.02);
    assert_eq!(
        requests[0]["input"]
            .as_array()
            .unwrap()
            .iter()
            .map(|text| text.as_str().unwrap().len())
            .sum::<usize>(),
        100
    );
    let ledger:(String,i64,i64)=sqlx::query_as("SELECT state,reserved_micro_usd,settled_micro_usd FROM managed_attempt_receipts WHERE logical_call=$1").bind(current.logical_call).fetch_one(&f.owner).await.unwrap();
    assert_eq!(
        ledger,
        ("billed".into(), 2, 2),
        "token-only usage must keep the rate sent before the effective-date boundary"
    );
    server.abort();
}

async fn lost_commit_ack(f: &Fixture) {
    use sqlx::postgres::PgSslMode;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
    };
    let options = f.app.connect_options().as_ref().clone();
    let remote = (options.get_host().to_string(), options.get_port());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let commit_seen = Arc::new(AtomicBool::new(false));
    let observed = commit_seen.clone();
    let relay = tokio::spawn(async move {
        let (client, _) = listener.accept().await.unwrap();
        let server = TcpStream::connect(remote).await.unwrap();
        let (mut client_read, mut client_write) = client.into_split();
        let (mut server_read, mut server_write) = server.into_split();
        let outgoing = async {
            let mut buffer = [0u8; 16384];
            let mut tail = Vec::new();
            loop {
                let size = client_read.read(&mut buffer).await.unwrap();
                if size == 0 {
                    return;
                }
                tail.extend_from_slice(&buffer[..size]);
                if tail.windows(6).any(|window| window == b"COMMIT") {
                    observed.store(true, Ordering::SeqCst);
                }
                tail = tail[tail.len().saturating_sub(5)..].to_vec();
                if server_write.write_all(&buffer[..size]).await.is_err() {
                    return;
                }
            }
        };
        let incoming = async {
            let mut buffer = [0u8; 16384];
            loop {
                let size = server_read.read(&mut buffer).await.unwrap();
                if size == 0 || observed.load(Ordering::SeqCst) {
                    return;
                }
                if client_write.write_all(&buffer[..size]).await.is_err() {
                    return;
                }
            }
        };
        tokio::select! {_=outgoing=>{},_=incoming=>{}}
    });
    let tenant = f.tenant;
    let proxy = PgPoolOptions::new()
        .max_connections(1)
        .after_connect(move |connection, _| {
            Box::pin(async move {
                sqlx::query(if tenant {
                    "SET ROLE coralswarm_tenant_app"
                } else {
                    "SET ROLE coralswarm_app"
                })
                .execute(connection)
                .await?;
                Ok(())
            })
        })
        .connect_with(
            options
                .host("127.0.0.1")
                .port(port)
                .ssl_mode(PgSslMode::Disable),
        )
        .await
        .unwrap();
    let plane = if f.tenant {
        JobScope::Org(&proxy)
    } else {
        JobScope::Consumer {
            pool: &proxy,
            account_id: f.account,
        }
    };
    let current = gate(&plane, f.batch(), caps());
    let before = f.counts().await;
    let (client, http, server) = loopback(
        response(json!({"cost":0.000001})),
        axum::http::StatusCode::OK,
        0,
    )
    .await;
    assert!(!send(&client, &current).await);
    assert!(commit_seen.load(Ordering::SeqCst));
    assert_eq!(
        f.counts().await,
        (
            before.0 + 1,
            before.1 + 3,
            before.2 + current.worst.per_attempt.get()
        )
    );
    assert_eq!(current.reserve(1).await, Err(Deny::Unavailable));
    assert_eq!(http.requests.lock().unwrap().len(), 0);
    sqlx::query("UPDATE managed_attempt_receipts SET created_at=now()-interval '1 hour' WHERE logical_call=$1").bind(current.logical_call).execute(&f.owner).await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i32>("SELECT managed_reap_stale(interval '10 minutes')")
            .fetch_one(&f.app)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT settled_micro_usd FROM managed_attempt_receipts WHERE logical_call=$1"
        )
        .bind(current.logical_call)
        .fetch_one(&f.owner)
        .await
        .unwrap(),
        current.worst.per_attempt.get()
    );
    server.abort();
    proxy.close().await;
    relay.await.unwrap();
}

async fn billing_once(f: &Fixture) {
    let control_fixture = if f.tenant {
        Some(Fixture::new(false).await)
    } else {
        None
    };
    let control = control_fixture
        .as_ref()
        .map(|fixture| &fixture.owner)
        .unwrap_or(&f.owner);
    let org = format!("org_embedding_{}", Uuid::new_v4().simple());
    if f.tenant {
        sqlx::query("INSERT INTO users(id,email) VALUES($1,$2)")
            .bind(f.user)
            .bind(format!("{}@billing.test", f.user))
            .execute(control)
            .await
            .unwrap();
        sqlx::query("INSERT INTO accounts(id,owner_user_id,clerk_org_id) VALUES($1,$2,$3)")
            .bind(f.account)
            .bind(f.user)
            .bind(&org)
            .execute(control)
            .await
            .unwrap();
        let mut financial=crate::tenancy::scope::begin_account_scoped(control,f.account).await.unwrap();
        crate::billing::renewal::lock_period_rotation(&mut financial,f.account).await.unwrap();
        sqlx::query("INSERT INTO tenant_databases(clerk_org_id,account_id,slug,db_secret_ref,status) VALUES($1,$2,'embedding-accounting','LOCAL_ACCOUNTING_ONLY','active')").bind(&org).bind(f.account).execute(&mut *financial).await.unwrap();
        financial.commit().await.unwrap();
        sqlx::query("INSERT INTO tenant_meta(org_id) VALUES($1)").bind(&org).execute(&f.owner).await.unwrap();
    }
    for kind in ["EmbedAtoms", "EmbedAtomsRetry", "ReembedOutdatedChunks"] {
        assert!(crate::managed::MANAGED_JOB_KINDS.contains(&kind));
        assert!(!crate::workers::billing_derivation::JOB_AXIS_KINDS.contains(&kind));
        let job: Uuid=sqlx::query_scalar("INSERT INTO jobs(kind,payload,status,account_id,finished_at,started_at) VALUES($1,'{}','done',$2,now()-interval '10 minutes',now()-interval '11 minutes') RETURNING id").bind(kind).bind(f.account).fetch_one(&f.owner).await.unwrap();
        sqlx::query("INSERT INTO llm_usage(account_id,graph_id,task,provider,model,input_tokens,output_tokens,source_job_id,cost_usd,created_at) VALUES($1,$2,'embed','openrouter',$3,100,0,$4,50,now()-interval '10 minutes')").bind(f.account).bind(f.graph).bind(MODEL).bind(job).execute(&f.owner).await.unwrap();
    }
    sqlx::query("UPDATE managed_attempt_receipts SET settled_at=now()-interval '10 minutes' WHERE settled_at IS NOT NULL").execute(&f.owner).await.unwrap();
    let (expected_count,expected_micro):(i64,i64)=sqlx::query_as("SELECT count(*),sum(settled_micro_usd)::bigint FROM managed_attempt_receipts WHERE settled_micro_usd>0").fetch_one(&f.owner).await.unwrap();
    let worker = crate::workers::tenant_harness::WorkerTenant {
        tenant: if f.tenant {
            crate::tenancy::TenantId::Org(org)
        } else {
            crate::tenancy::TenantId::Consumer
        },
        store: Arc::new(crate::store::PostgresStore::new(f.app.clone())),
        pool: f.app.clone(),
    };
    for _ in 0..2 {
        crate::workers::billing_derivation::tick(&worker, control)
            .await
            .unwrap();
    }
    let (actual_count,actual_micro,other_axes):(i64,i64,i64)=sqlx::query_as("SELECT count(*),(sum(cost_usd)*1000000)::bigint,count(*) FILTER(WHERE source_receipt_id IS NULL OR source_job_id IS NOT NULL OR source_query_id IS NOT NULL) FROM credit_ledger WHERE account_id=$1").bind(f.account).fetch_one(control).await.unwrap();
    assert_eq!(
        (actual_count, actual_micro, other_axes),
        (expected_count, expected_micro, 0)
    );
    let distinct: i64 = sqlx::query_scalar(
        "SELECT count(DISTINCT source_receipt_id) FROM credit_ledger WHERE account_id=$1",
    )
    .bind(f.account)
    .fetch_one(control)
    .await
    .unwrap();
    assert_eq!(distinct, expected_count);
    if let Some(fixture) = control_fixture {
        fixture.finish().await;
    }
}

#[tokio::test]
async fn consumer_native_accounting_receipts_and_loopback() {
    accounting(false).await;
}

#[tokio::test]
async fn tenant_native_accounting_receipts_and_loopback() {
    accounting(true).await;
}

#[test]
fn checked_inputs_and_exact_input_only_prices() {
    let id = Uuid::new_v4();
    let make = |texts: Vec<&str>, revision| {
        ReceiptBatch::checked(
            id,
            id,
            id,
            id,
            texts
                .into_iter()
                .map(|text| ReceiptInput {
                    atom_id: id,
                    content_revision: revision,
                    text: text.into(),
                })
                .collect(),
        )
    };
    assert!(make(vec![], 1).is_err());
    assert!(make(vec![""], 1).is_err());
    assert!(make(vec!["a"], 0).is_err());
    assert!(make(vec!["a", "b"], 1).is_err());
    assert!(make(vec!["a"; 9], 1).is_err());
    let batch = make(vec!["🪸"], 1).unwrap();
    assert_eq!(
        embedding_worst_case(&batch, &sheet(0.26), Utc::now())
            .unwrap()
            .per_attempt
            .get(),
        2
    );
    assert_eq!(
        embedding_worst_case(&batch, &sheet(0.26), Utc::now())
            .unwrap()
            .input_bound_tokens,
        4
    );
    assert_eq!(
        embedding_worst_case(&batch, &sheet_for("unmatched-model", 0.02), Utc::now()).unwrap_err(),
        RefuseCode::UnpricedModel
    );
    for rate in [f64::NAN, f64::INFINITY, -0.1, f64::MAX] {
        assert!(embedding_worst_case(&batch, &sheet(rate), Utc::now()).is_err());
    }
    assert_eq!(input_micro(100, 0.02), Some(2));
    assert_eq!(input_micro(u64::MAX, f64::MAX), None);
}

#[tokio::test]
async fn token_usage_keeps_reserved_rate_across_effective_boundary() {
    let pool = PgPoolOptions::new()
        .connect_lazy("postgres://localhost:9/unused_pricing_only")
        .unwrap();
    let id = Uuid::new_v4();
    let plane = JobScope::Consumer {
        pool: &pool,
        account_id: id,
    };
    let batch = ReceiptBatch::checked(
        id,
        id,
        id,
        id,
        vec![ReceiptInput {
            atom_id: id,
            content_revision: 1,
            text: "x".repeat(100),
        }],
    )
    .unwrap();
    let mut current = gate(&plane, batch, caps());
    let (sheet, as_of) = rate_history();
    current.sheet = sheet;
    current.worst = embedding_worst_case(&current.batch, &current.sheet, as_of).unwrap();
    assert_eq!(
        current
            .sheet
            .rate("openrouter", MODEL, Utc::now())
            .input_per_million,
        0.50
    );
    assert_eq!(current.worst.ceiling().prompt, 0.02);
    assert_eq!(current.worst.per_attempt.get(), 2);
    let mut observed = SendOutcome::Billed {
        reported_cost_usd: None,
        input_tokens: 100,
        output_tokens: u64::MAX,
        served_model: MODEL.into(),
        upstream: None,
        generation_id: None,
    };
    assert_eq!(current.billed_micro(&observed), Some(2));
    if let SendOutcome::Billed {
        reported_cost_usd, ..
    } = &mut observed
    {
        *reported_cost_usd = Some(0.000007);
    }
    assert_eq!(current.billed_micro(&observed), Some(7));
    pool.close().await;
}
