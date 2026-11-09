use super::*;
use axum::{extract::State, routing::post, Json, Router};
use coralswarm_core::embed_openrouter::OpenRouterEmbedder;
use serde_json::{json, Value};
use sha2::Digest;
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;

struct EmbeddingFixture {
    finance: Fixture,
    ids: Vec<Uuid>,
    texts: Vec<String>,
}

impl EmbeddingFixture {
    async fn new(org: bool) -> Self {
        Self::with(Fixture::new(org).await).await
    }

    /// Corals and keys on `f`'s plane: the account's DEK on an org's tenant
    /// database, else on its control `accounts` row (a personal org too).
    async fn with(f: Fixture) -> Self {
        let org = matches!(f.tenant, TenantId::Org(_));
        f.balance(Decimal::from(100)).await;
        let mut content = begin_plane(&f.plane()).await.unwrap();
        sqlx::query("DELETE FROM corpus_enrichment_units WHERE graph_id=$1")
            .bind(f.graph)
            .execute(&mut *content)
            .await
            .unwrap();
        sqlx::query("DELETE FROM corpus_enrichment_runs WHERE graph_id=$1")
            .bind(f.graph)
            .execute(&mut *content)
            .await
            .unwrap();
        let texts: Vec<String> = ["reef α", "ocean\nβ", "coral 🪸"]
            .into_iter()
            .map(str::to_owned)
            .collect();
        let mut ids = Vec::new();
        for text in &texts {
            ids.push(sqlx::query_scalar("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,timestamp) VALUES($1,'note',$2,$4,$3,now()) RETURNING atom_id")
                .bind(f.graph).bind(Uuid::new_v4().to_string()).bind(text).bind(format!("{}/{}",f.graph,Uuid::new_v4())).fetch_one(&mut *content).await.unwrap());
        }
        content.commit().await.unwrap();
        sqlx::query("INSERT INTO model_rates(provider,model_pattern,priority,input_per_million,output_per_million,cache_write_multiplier,cache_read_multiplier,effective_from,is_blend_fallback) VALUES('openrouter','text-embedding-3-small',-100,0.02,0,1,1,'2000-01-01',false)")
            .execute(&f.control.owner).await.unwrap();
        let wrapped = coralswarm_core::kek::wrap_dek(
            &coralswarm_core::kek::load_kek().expect("local fixture MASTER_KEK_HEX required"),
            &[43; 32],
        )
        .unwrap();
        if org {
            sqlx::query("INSERT INTO account_keys(account_id,dek_wrapped) VALUES($1,$2)")
                .bind(f.account)
                .bind(wrapped)
                .execute(f.owner())
                .await
                .unwrap();
        } else {
            let mut tx = f.financial().await;
            sqlx::query("UPDATE accounts SET dek_wrapped=$2 WHERE id=$1")
                .bind(f.account)
                .bind(wrapped)
                .execute(&mut *tx)
                .await
                .unwrap();
            tx.commit().await.unwrap();
        }
        Self {
            finance: f,
            ids,
            texts,
        }
    }

    async fn job(&self, retry: bool) -> crate::jobs::queue::JobRow {
        let kind = if retry {
            crate::jobs::JobKind::EmbedAtomsRetry {
                atom_ids: self.ids.clone(),
            }
        } else {
            crate::jobs::JobKind::EmbedAtoms {
                atom_ids: self.ids.clone(),
            }
        };
        let id = crate::jobs::queue::enqueue(
            self.finance.app(),
            &kind,
            Some(self.finance.account),
            None,
            0,
        )
        .await
        .unwrap()
        .unwrap();
        crate::jobs::queue::dequeue(self.finance.app(), &["EmbedAtoms", "EmbedAtomsRetry"], 1)
            .await
            .unwrap()
            .into_iter()
            .find(|job| job.id == id)
            .unwrap()
    }

    async fn drive(&self, job: &crate::jobs::queue::JobRow, cloud: &OpenRouterEmbedder) {
        let f = &self.finance;
        let store = crate::store::PostgresStore::new(f.app().clone());
        crate::workers::embed::managed_job_for_test(
            &f.tenant,
            f.app(),
            &store,
            cloud,
            &f.control.app,
            job,
            &self.ids,
        )
        .await;
    }

    async fn finish(self) {
        self.finance.finish().await;
    }
}

#[derive(Clone)]
struct Wire {
    bodies: Arc<Mutex<Vec<Value>>>,
    started: Arc<Notify>,
    release: Arc<Notify>,
    wait: bool,
    usage: Option<Value>,
    reply: Arc<Mutex<Option<Value>>>,
    /// Refuse, as OpenAI does, a request holding an input over this many
    /// bytes (a stand-in for its 8192-token limit).
    over_limit: Arc<Mutex<Option<usize>>>,
    /// Answer every request with this status and no vectors.
    fail: Arc<Mutex<Option<axum::http::StatusCode>>>,
    /// Refuse with a plain 400 any request holding an input that contains
    /// this text: a per-input refusal that names no input.
    reject: Arc<Mutex<Option<String>>>,
}

async fn serve(
    State(wire): State<Wire>,
    Json(body): Json<Value>,
) -> (axum::http::StatusCode, Json<Value>) {
    wire.bodies.lock().unwrap().push(body.clone());
    wire.started.notify_one();
    if wire.wait {
        wire.release.notified().await;
    }
    if let Some(status) = *wire.fail.lock().unwrap() {
        return (status, Json(json!({"error":{"message":"upstream unavailable"}})));
    }
    let reject = wire.reject.lock().unwrap().clone();
    if let Some(reject) = reject {
        if body["input"].as_array().unwrap().iter().any(|i| i.as_str().unwrap().contains(&reject)) {
            return (axum::http::StatusCode::BAD_REQUEST, Json(json!({"error":{"message":"Invalid request","code":400}})));
        }
    }
    let limit = *wire.over_limit.lock().unwrap();
    if let Some(index) = limit.and_then(|limit| {
        body["input"].as_array().unwrap().iter().position(|i| i.as_str().unwrap().len() > limit)
    }) {
        return (
            axum::http::StatusCode::BAD_REQUEST,
            Json(json!({"error":{"message":format!("Invalid 'input[{index}]': maximum input length is 8192 tokens."),"code":400}})),
        );
    }
    let n = body["input"].as_array().unwrap().len();
    let mut response = json!({"model":"openai/text-embedding-3-small","data":(0..n).rev().map(|index|json!({"index":index,"embedding":vec![(index+1) as f64;384]})).collect::<Vec<_>>()});
    if let Some(usage) = &wire.usage {
        response["usage"] = usage.clone();
    }
    let reply = wire.reply.lock().unwrap().clone();
    (axum::http::StatusCode::OK, Json(reply.unwrap_or(response)))
}

async fn loopback(
    wait: bool,
    usage: Option<Value>,
) -> (OpenRouterEmbedder, Wire, tokio::task::JoinHandle<()>) {
    let wire = Wire {
        bodies: Arc::new(Mutex::new(Vec::new())),
        started: Arc::new(Notify::new()),
        release: Arc::new(Notify::new()),
        wait,
        usage,
        reply: Arc::new(Mutex::new(None)),
        over_limit: Arc::new(Mutex::new(None)),
        fail: Arc::new(Mutex::new(None)),
        reject: Arc::new(Mutex::new(None)),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let app = Router::new()
        .route("/v1/embeddings", post(serve))
        .with_state(wire.clone());
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let cloud = OpenRouterEmbedder::new(
        reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .unwrap(),
        "local-fixture-only",
    )
    .with_base_url(url);
    (cloud, wire, server)
}

async fn successful_worker(org: bool, retry: bool, usage: Option<Value>, expected_state: &str) {
    successful_worker_on(EmbeddingFixture::new(org).await, retry, usage, expected_state).await;
}

async fn successful_worker_on(f: EmbeddingFixture, retry: bool, usage: Option<Value>, expected_state: &str) {
    let job = f.job(retry).await;
    let (cloud, wire, server) = loopback(false, usage).await;
    f.drive(&job, &cloud).await;
    let bodies = wire.bodies.lock().unwrap().clone();
    assert_eq!(bodies.len(), 1);
    assert_eq!(bodies[0]["model"], "openai/text-embedding-3-small");
    assert_eq!(bodies[0]["dimensions"], 384);
    assert_eq!(bodies[0]["provider"]["allow_fallbacks"], false);
    assert_eq!(bodies[0]["provider"]["data_collection"], "deny");
    assert_eq!(bodies[0]["provider"]["zdr"], true);
    assert_eq!(bodies[0]["provider"]["max_price"]["prompt"], 0.02);
    let sent: Vec<String> = serde_json::from_value(bodies[0]["input"].clone()).unwrap();
    let mut content = begin_plane(&f.finance.plane()).await.unwrap();
    let receipts:Vec<(Uuid,String,Option<Uuid>,Option<Uuid>,Uuid)>=sqlx::query_as("SELECT receipt_id,state,unit_id,source_version_id,attributed_user_id FROM managed_attempt_receipts WHERE job_id=$1 AND protocol_version=1 AND purpose='cloud_embedding' AND dispatch_phase='armed'")
        .bind(job.id).fetch_all(&mut *content).await.unwrap();
    assert_eq!(receipts.len(), 1);
    assert_eq!(receipts[0].1, expected_state);
    assert_eq!(receipts[0].2, None);
    assert_eq!(receipts[0].3, None);
    assert_eq!(receipts[0].4, f.finance.payer);
    let inputs:Vec<(Uuid,i16,Vec<u8>,i64,String)>=sqlx::query_as("SELECT atom_id,ordinal,content_sha256,input_bytes,outcome FROM managed_embedding_inputs WHERE receipt_id=$1 ORDER BY ordinal")
        .bind(receipts[0].0).fetch_all(&mut *content).await.unwrap();
    assert_eq!(inputs.len(), 3);
    for (ordinal, input) in inputs.iter().enumerate() {
        assert_eq!(input.1, ordinal as i16);
        assert_eq!(
            input.2,
            sha2::Sha256::digest(sent[ordinal].as_bytes()).to_vec()
        );
        assert_eq!(input.3, sent[ordinal].len() as i64);
        assert_eq!(input.4, "written");
        let row:(String,bool,bool)=sqlx::query_as("SELECT embed_model_next,embedding_next IS NOT NULL,content_tsv IS NOT NULL FROM atoms WHERE atom_id=$1")
            .bind(input.0).fetch_one(&mut *content).await.unwrap();
        assert_eq!(
            row,
            (coralswarm_core::embed::EMBED_MODEL_CLOUD.into(), true, true)
        );
    }
    content.commit().await.unwrap();
    let financial: (String, Uuid) = sqlx::query_as(
        "SELECT state,payer_id FROM managed_credit_reservations WHERE receipt_id=$1",
    )
    .bind(receipts[0].0)
    .fetch_one(&f.finance.control.owner)
    .await
    .unwrap();
    assert_eq!(financial, ("settled".into(), f.finance.payer));
    let debit: i64 =
        sqlx::query_scalar("SELECT count(*) FROM credit_ledger WHERE source_receipt_id=$1")
            .bind(receipts[0].0)
            .fetch_one(&f.finance.control.owner)
            .await
            .unwrap();
    assert_eq!(debit, 1);
    let row: (String, bool) = sqlx::query_as(
        "SELECT status::text,payload ? '_managed_embedding_claim' FROM jobs WHERE id=$1",
    )
    .bind(job.id)
    .fetch_one(f.finance.owner())
    .await
    .unwrap();
    assert_eq!(row, ("done".into(), false));
    let replay = f.job(true).await;
    f.drive(&replay, &cloud).await;
    assert_eq!(
        wire.bodies.lock().unwrap().len(),
        1,
        "fresh retry over already-current vectors must not resend or debit"
    );
    let receipts: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM managed_attempt_receipts WHERE purpose='cloud_embedding'",
    )
    .fetch_one(f.finance.owner())
    .await
    .unwrap();
    assert_eq!(receipts, 1);
    server.abort();
    assert!(server.await.unwrap_err().is_cancelled());
    f.finish().await;
}

#[tokio::test]
async fn c23_consumer_real_embed_worker_funded_exact_input_and_single_debit() {
    successful_worker(false, false, Some(json!({"cost":0.000001})), "billed").await;
}
#[tokio::test]
async fn c23_org_real_retry_worker_funded_exact_input_and_single_debit() {
    successful_worker(true, true, Some(json!({"cost":0.000001})), "billed").await;
}
/// COR-114: a personal org (a consumer-plane account with a `clerk_org_id`)
/// embeds through the real worker, funded and debited once. Every batch was
/// held `requester_access_lost` before the plane resolver.
#[tokio::test]
async fn c23_personal_org_real_embed_worker_funded_exact_input_and_single_debit() {
    let f = EmbeddingFixture::with(Fixture::personal_org().await).await;
    successful_worker_on(f, false, Some(json!({"cost":0.000001})), "billed").await;
}
#[tokio::test]
async fn c23_consumer_usable_vectors_with_ambiguous_usage_keep_conservative_charge() {
    successful_worker(false, true, None, "ambiguous").await;
}
#[tokio::test]
async fn c23_org_usable_vectors_with_ambiguous_usage_keep_conservative_charge() {
    successful_worker(true, false, None, "ambiguous").await;
}

/// Prod, 2026-10-23: one atom over the provider's input limit failed its
/// whole batch with a 400, was held as `provider_unavailable` with
/// `embed_attempts` untouched, and the sweep re-sent the batch every ten
/// minutes, forever. Through the gateway the long text is split into inputs
/// within the limit, sent alone so it can only fail itself, and its pieces'
/// vectors pooled into one: every atom gets a vector, nothing is cut, and
/// the provider's limit is never reached.
async fn oversized_input(org: bool) {
    let f = EmbeddingFixture::new(org).await;
    let long = "the reef holds ".repeat(700);
    assert!(long.len() > crate::embedding::EMBED_INPUT_MAX_BYTES);
    let mut tx = begin_plane(&f.finance.plane()).await.unwrap();
    sqlx::query("UPDATE atoms SET content_inline=$2 WHERE atom_id=$1")
        .bind(f.ids[1]).bind(&long).execute(&mut *tx).await.unwrap();
    tx.commit().await.unwrap();
    let job = f.job(false).await;
    let (cloud, wire, server) = loopback(false, Some(json!({"cost":0.000001}))).await;
    *wire.over_limit.lock().unwrap() = Some(crate::embedding::EMBED_INPUT_MAX_BYTES);
    f.drive(&job, &cloud).await;
    let bodies = wire.bodies.lock().unwrap().clone();
    let sent: Vec<Vec<String>> = bodies
        .iter()
        .map(|b| serde_json::from_value(b["input"].clone()).unwrap())
        .collect();
    let mut bounded: Vec<String> = vec![f.texts[0].clone(), f.texts[2].clone()];
    let mut first = sent[0].clone();
    bounded.sort();
    first.sort();
    assert_eq!(sent.len(), 2, "bounded inputs together, the long one alone");
    assert_eq!(first, bounded);
    assert_eq!(sent[1].len(), 2, "the long text is split, not refused");
    assert!(sent[1].iter().all(|piece| piece.len() <= crate::embedding::EMBED_INPUT_MAX_BYTES));
    assert_eq!(
        sent[1].join(" ").split_whitespace().collect::<Vec<_>>(),
        long.split_whitespace().collect::<Vec<_>>(),
        "every word is sent: split, never truncated"
    );
    let mut tx = begin_plane(&f.finance.plane()).await.unwrap();
    let rows: Vec<(Uuid, String, bool, i32)> = sqlx::query_as("SELECT atom_id,COALESCE(embed_model_next,''),embedding_next IS NOT NULL,embed_attempts FROM atoms WHERE atom_id=ANY($1) ORDER BY atom_id")
        .bind(&f.ids).fetch_all(&mut *tx).await.unwrap();
    for (_, stamp, vector, attempts) in rows {
        assert_eq!((stamp.as_str(), vector, attempts), (coralswarm_core::embed::EMBED_MODEL_CLOUD, true, 0));
    }
    // One receipt per provider request; the long atom's input is receipted
    // at its full size.
    let receipts: Vec<(String, i64, i64)> = sqlx::query_as("SELECT r.state,(SELECT count(*) FROM managed_embedding_inputs i WHERE i.receipt_id=r.receipt_id),(SELECT sum(input_bytes)::bigint FROM managed_embedding_inputs i WHERE i.receipt_id=r.receipt_id) FROM managed_attempt_receipts r WHERE r.job_id=$1 AND r.dispatch_phase='armed' ORDER BY 2 DESC")
        .bind(job.id).fetch_all(&mut *tx).await.unwrap();
    let bounded_bytes = (f.texts[0].len() + f.texts[2].len()) as i64;
    assert_eq!(receipts, vec![("billed".into(), 2, bounded_bytes), ("billed".into(), 1, long.len() as i64)]);
    tx.commit().await.unwrap();
    server.abort();
    assert!(server.await.unwrap_err().is_cancelled());
    f.finish().await;
}

#[tokio::test]
async fn consumer_oversized_input_is_split_and_embedded() {
    oversized_input(false).await;
}
#[tokio::test]
async fn org_oversized_input_is_split_and_embedded() {
    oversized_input(true).await;
}

/// A 4xx other than 429 is permanent, and pinned on the input at fault: the
/// refused batch is re-sent one input per request (each on its own
/// receipt), the good inputs embed, and only the refused one is held, with
/// `embed_attempts` at the cap so the sweep never sends it again. Before
/// the gateway, an unknown 4xx was a counted transient hold for the whole
/// batch.
async fn rejected_input(org: bool) {
    let f = EmbeddingFixture::new(org).await;
    let job = f.job(false).await;
    let (cloud, wire, server) = loopback(false, Some(json!({"cost":0.000001}))).await;
    *wire.reject.lock().unwrap() = Some(f.texts[1].clone());
    f.drive(&job, &cloud).await;
    let bodies = wire.bodies.lock().unwrap().clone();
    let sizes: Vec<usize> = bodies.iter().map(|b| b["input"].as_array().unwrap().len()).collect();
    assert_eq!(sizes, vec![3, 1, 1, 1], "the refused batch, then each input alone");
    let mut tx = begin_plane(&f.finance.plane()).await.unwrap();
    let rows: Vec<(Uuid, String, bool, bool, i32)> = sqlx::query_as("SELECT atom_id,COALESCE(embed_model_next,''),embedding_next IS NOT NULL,content_tsv IS NOT NULL,embed_attempts FROM atoms WHERE atom_id=ANY($1)")
        .bind(&f.ids).fetch_all(&mut *tx).await.unwrap();
    for (id, stamp, vector, keyword, attempts) in rows {
        if id == f.ids[1] {
            assert_eq!(stamp, "blocked:provider_rejected");
            assert!(!vector);
            assert!(keyword, "a refused atom stays keyword-searchable");
            assert_eq!(attempts, crate::embedding::MAX_EMBED_ATTEMPTS, "permanent: straight to the cap");
        } else {
            assert_eq!((stamp.as_str(), vector, attempts), (coralswarm_core::embed::EMBED_MODEL_CLOUD, true, 0));
        }
    }
    let states: Vec<String> = sqlx::query_scalar("SELECT state FROM managed_attempt_receipts WHERE job_id=$1 AND dispatch_phase='armed' ORDER BY state")
        .bind(job.id).fetch_all(&mut *tx).await.unwrap();
    assert_eq!(states, vec!["billed", "billed", "not_generated", "not_generated"], "refusals are not charged");
    sqlx::query("UPDATE atoms SET embed_failed_at=clock_timestamp()-interval '30 days' WHERE atom_id=ANY($1)")
        .bind(&f.ids).execute(&mut *tx).await.unwrap();
    tx.commit().await.unwrap();
    let worker = WorkerTenant {
        tenant: f.finance.tenant.clone(),
        pool: f.finance.app().clone(),
        store: Arc::new(crate::store::PostgresStore::new(f.finance.app().clone())),
    };
    crate::workers::embed_retry::tick(&worker, &crate::shutdown::Shutdown::new())
        .await
        .unwrap();
    let retry = crate::jobs::queue::dequeue(f.finance.app(), &["EmbedAtomsRetry"], 1)
        .await
        .unwrap();
    assert!(retry.is_empty(), "the sweep must never re-send a permanently refused input");
    server.abort();
    assert!(server.await.unwrap_err().is_cancelled());
    f.finish().await;
}

#[tokio::test]
async fn consumer_rejected_input_is_pinned_and_permanent() {
    rejected_input(false).await;
}
#[tokio::test]
async fn org_rejected_input_is_pinned_and_permanent() {
    rejected_input(true).await;
}

/// The database backstop (`0360`/`0191_embed_input_bounds`): inline atom text
/// longer than any provider request may carry is refused where it is
/// stored. Text at the ceiling is accepted.
async fn inline_text_ceiling(org: bool) {
    let f = EmbeddingFixture::new(org).await;
    let ceiling = crate::embedding::REQUEST_MAX_BYTES;
    let mut tx = begin_plane(&f.finance.plane()).await.unwrap();
    sqlx::query("UPDATE atoms SET content_inline=$2 WHERE atom_id=$1")
        .bind(f.ids[0]).bind("x".repeat(ceiling)).execute(&mut *tx).await.unwrap();
    let over = sqlx::query("UPDATE atoms SET content_inline=$2 WHERE atom_id=$1")
        .bind(f.ids[0]).bind("x".repeat(ceiling + 1)).execute(&mut *tx).await.unwrap_err();
    let code = over.as_database_error().and_then(|e| e.code()).map(|c| c.to_string());
    assert_eq!(code.as_deref(), Some("23514"), "{over}");
    assert!(over.to_string().contains("atoms_content_inline_embeddable"), "{over}");
    drop(tx);
    f.finish().await;
}

#[tokio::test]
async fn consumer_inline_atom_text_over_the_gateway_ceiling_is_refused() {
    inline_text_ceiling(false).await;
}
#[tokio::test]
async fn org_inline_atom_text_over_the_gateway_ceiling_is_refused() {
    inline_text_ceiling(true).await;
}

/// A transport failure is held `provider_unavailable` and now counts as an
/// attempt, so the sweep's attempts cap ends a failure that never clears.
async fn provider_failure_counts(org: bool) {
    let f = EmbeddingFixture::new(org).await;
    let (cloud, wire, server) = loopback(false, Some(json!({"cost":0.000001}))).await;
    *wire.fail.lock().unwrap() = Some(axum::http::StatusCode::SERVICE_UNAVAILABLE);
    for expected in 1..=2 {
        let job = f.job(true).await;
        f.drive(&job, &cloud).await;
        let mut tx = begin_plane(&f.finance.plane()).await.unwrap();
        let rows: Vec<(String, i32)> = sqlx::query_as("SELECT embed_model_next,embed_attempts FROM atoms WHERE atom_id=ANY($1)")
            .bind(&f.ids).fetch_all(&mut *tx).await.unwrap();
        tx.commit().await.unwrap();
        assert_eq!(rows.len(), 3);
        for row in rows {
            assert_eq!(row, ("blocked:provider_unavailable".into(), expected));
        }
    }
    assert_eq!(wire.bodies.lock().unwrap().len(), 2);
    server.abort();
    assert!(server.await.unwrap_err().is_cancelled());
    f.finish().await;
}

#[tokio::test]
async fn consumer_provider_failure_hold_counts_an_attempt() {
    provider_failure_counts(false).await;
}
#[tokio::test]
async fn org_provider_failure_hold_counts_an_attempt() {
    provider_failure_counts(true).await;
}

async fn refused_worker(org: bool, reason: &str) {
    let f = EmbeddingFixture::new(org).await;
    let marker = match reason {
        "credits" => {
            f.finance.balance(Decimal::ZERO).await;
            "blocked:credits_exhausted"
        }
        "daily" => {
            let mut tx = begin_plane(&f.finance.plane()).await.unwrap();
            sqlx::query("INSERT INTO managed_budget_periods(account_id,purpose,period_start,limit_micro_usd,charged_micro_usd) VALUES($1,'cloud_embedding',(clock_timestamp() AT TIME ZONE 'UTC')::date,1,1)")
                .bind(f.finance.account).execute(&mut *tx).await.unwrap();
            tx.commit().await.unwrap();
            "blocked:daily_budget_exhausted"
        }
        _ => panic!("unknown refusal fixture"),
    };
    let job = f.job(false).await;
    let (cloud, wire, server) = loopback(false, Some(json!({"cost":0.000001}))).await;
    f.drive(&job, &cloud).await;
    assert!(wire.bodies.lock().unwrap().is_empty());
    let mut tx = begin_plane(&f.finance.plane()).await.unwrap();
    let rows:Vec<(String,bool,bool,i32,bool)>=sqlx::query_as("SELECT embed_model_next,content_tsv IS NOT NULL,embedding_next IS NULL,embed_attempts,embed_failed_at IS NOT NULL FROM atoms WHERE atom_id=ANY($1)")
        .bind(&f.ids).fetch_all(&mut *tx).await.unwrap();
    assert_eq!(rows.len(), 3);
    for row in rows {
        assert_eq!(row, (marker.into(), true, true, 0, true));
    }
    let prepared_then_refused = org && reason == "credits";
    let receipts: Vec<(Uuid, bool)> = sqlx::query_as(
        "SELECT receipt_id,protocol_version=1 AND purpose='cloud_embedding' \
         AND dispatch_phase='cancelled_unsent' AND state='not_generated' \
         AND settled_micro_usd=0 AND reserved_micro_usd=1 AND attempt_no=1 \
         AND unit_id IS NULL AND source_version_id IS NULL \
         AND settled_at IS NOT NULL AND generation_id IS NULL \
         FROM managed_attempt_receipts WHERE job_id=$1",
    )
    .bind(job.id)
    .fetch_all(&mut *tx)
    .await
    .unwrap();
    assert_eq!(receipts.len(), usize::from(prepared_then_refused));
    assert!(receipts
        .iter()
        .all(|(_, cancelled_unsent)| *cancelled_unsent));
    let inputs: Vec<String> = sqlx::query_scalar(
        "SELECT outcome FROM managed_embedding_inputs WHERE receipt_id IN \
         (SELECT receipt_id FROM managed_attempt_receipts WHERE job_id=$1)",
    )
    .bind(job.id)
    .fetch_all(&mut *tx)
    .await
    .unwrap();
    assert_eq!(
        inputs,
        vec!["sent"; if prepared_then_refused { 3 } else { 0 }]
    );
    let budget: (i64, i64) = sqlx::query_as(
        "SELECT COALESCE(sum(reserved_micro_usd),0)::bigint, \
         COALESCE(sum(charged_micro_usd),0)::bigint FROM managed_budget_periods \
         WHERE account_id=$1 AND purpose='cloud_embedding'",
    )
    .bind(f.finance.account)
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    assert_eq!(budget, (0, if reason == "daily" { 1 } else { 0 }));
    let terminal: (String, String, Option<String>, bool, bool) = sqlx::query_as(
        "SELECT status::text,outcome,error_message,finished_at IS NOT NULL, \
         payload ? '_managed_embedding_claim' FROM jobs WHERE id=$1",
    )
    .bind(job.id)
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    assert_eq!(
        terminal,
        (
            "done".into(),
            "nothing_to_do".into(),
            Some("managed_embedding_held".into()),
            true,
            false
        )
    );
    if reason == "daily" {
        let next_day:bool=sqlx::query_scalar("SELECT bool_and(embed_failed_at+interval '30 seconds'=(date_trunc('day',clock_timestamp() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC')+interval '1 day') FROM atoms WHERE atom_id=ANY($1)")
            .bind(&f.ids).fetch_one(&mut *tx).await.unwrap();
        assert!(next_day);
    }
    tx.commit().await.unwrap();
    let held: i64 =
        sqlx::query_scalar("SELECT count(*) FROM managed_credit_reservations WHERE state='held'")
            .fetch_one(&f.finance.control.owner)
            .await
            .unwrap();
    assert_eq!(held, 0);
    let reservations: Vec<(Uuid, String, i64)> = sqlx::query_as(
        "SELECT receipt_id,state,settled_micro_usd FROM managed_credit_reservations \
         WHERE account_id=$1",
    )
    .bind(f.finance.account)
    .fetch_all(&f.finance.control.owner)
    .await
    .unwrap();
    assert_eq!(
        reservations,
        receipts
            .iter()
            .map(|(id, _)| (*id, "released".into(), 0))
            .collect::<Vec<_>>()
    );
    let financial: (Decimal, Decimal, i64) = sqlx::query_as(
        "SELECT balance,managed_debited_credits, \
         (SELECT count(*) FROM credit_ledger WHERE account_id=$1 AND entry_kind='usage_managed') \
         FROM credit_balances WHERE account_id=$1",
    )
    .bind(f.finance.account)
    .fetch_one(&f.finance.control.owner)
    .await
    .unwrap();
    assert_eq!(
        financial,
        (
            if reason == "credits" {
                Decimal::ZERO
            } else {
                Decimal::from(100)
            },
            Decimal::ZERO,
            0
        )
    );
    server.abort();
    assert!(server.await.unwrap_err().is_cancelled());
    f.finish().await;
}

#[tokio::test]
async fn c23_consumer_credit_and_daily_refusals_keep_keyword_and_visible_stamps() {
    for reason in ["credits", "daily"] {
        refused_worker(false, reason).await;
    }
}
#[tokio::test]
async fn c23_org_credit_and_daily_refusals_keep_keyword_and_visible_stamps() {
    for reason in ["credits", "daily"] {
        refused_worker(true, reason).await;
    }
}

async fn stale_response(org: bool, kind: &str) {
    let f = EmbeddingFixture::new(org).await;
    if kind == "source" {
        let mut tx = begin_plane(&f.finance.plane()).await.unwrap();
        crate::source_evidence::link_verified_chunks_tx(
            &mut tx,
            f.finance.graph,
            f.finance.version,
            &f.ids,
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
    }
    let job = f.job(false).await;
    let (cloud, wire, server) = loopback(true, Some(json!({"cost":0.000001}))).await;
    let operation = f.drive(&job, &cloud);
    let mut operation = Box::pin(operation);
    tokio::select! { _=wire.started.notified()=>{},_= &mut operation=>panic!("worker ended before actual HTTP request") }
    if kind == "member" {
        let mut tx = f.finance.financial().await;
        sqlx::query("UPDATE users SET deleted_at=clock_timestamp() WHERE id=$1")
            .bind(f.finance.payer)
            .execute(&mut *tx)
            .await
            .unwrap();
        tx.commit().await.unwrap();
    } else {
        let mut tx = begin_plane(&f.finance.plane()).await.unwrap();
        if kind == "source" {
            sqlx::query("UPDATE source_items SET access_state='restricted' WHERE graph_id=$1")
                .bind(f.finance.graph)
                .execute(&mut *tx)
                .await
                .unwrap();
        } else {
            sqlx::query(
                "UPDATE atoms SET content_inline=content_inline||' changed' WHERE atom_id=ANY($1)",
            )
            .bind(&f.ids)
            .execute(&mut *tx)
            .await
            .unwrap();
        }
        tx.commit().await.unwrap();
    }
    wire.release.notify_one();
    operation.await;
    assert_eq!(wire.bodies.lock().unwrap().len(), 1);
    let mut tx = begin_plane(&f.finance.plane()).await.unwrap();
    let states: Vec<String> =
        sqlx::query_scalar("SELECT outcome FROM managed_embedding_inputs ORDER BY ordinal")
            .fetch_all(&mut *tx)
            .await
            .unwrap();
    assert_eq!(states, vec!["discarded_stale"; 3]);
    let vectors: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM atoms WHERE atom_id=ANY($1) AND embedding_next IS NOT NULL",
    )
    .bind(&f.ids)
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    assert_eq!(vectors, 0);
    if kind != "member" {
        let untouched: (i64, i64, i64) = sqlx::query_as("SELECT count(*) FILTER(WHERE content_tsv IS NULL),count(*) FILTER(WHERE embed_failed_at IS NULL),count(*) FILTER(WHERE embed_model_next IS NULL) FROM atoms WHERE atom_id=ANY($1)")
            .bind(&f.ids).fetch_one(&mut *tx).await.unwrap();
        assert_eq!(
            untouched,
            (3, 3, 3),
            "changed or source-revoked input must not be indexed or queued from stale text"
        );
    }
    let receipt: (String, i64) = sqlx::query_as(
        "SELECT state,settled_micro_usd FROM managed_attempt_receipts WHERE job_id=$1",
    )
    .bind(job.id)
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    assert_eq!(receipt, ("billed".into(), 1));
    tx.commit().await.unwrap();
    let debit: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM credit_ledger WHERE account_id=$1 AND source_receipt_id IS NOT NULL",
    )
    .bind(f.finance.account)
    .fetch_one(&f.finance.control.owner)
    .await
    .unwrap();
    assert_eq!(debit, 1);
    server.abort();
    assert!(server.await.unwrap_err().is_cancelled());
    f.finish().await;
}
#[tokio::test]
async fn c23_consumer_current_revision_source_and_executor_fence_provider_response() {
    for kind in ["content", "source", "member"] {
        stale_response(false, kind).await;
    }
}
#[tokio::test]
async fn c23_org_current_revision_source_and_executor_fence_provider_response() {
    for kind in ["content", "source", "member"] {
        stale_response(true, kind).await;
    }
}

async fn publication_hold_resumes(org: bool, pause: bool) {
    let f = EmbeddingFixture::new(org).await;
    let run = if pause {
        let mut tx = begin_plane(&f.finance.plane()).await.unwrap();
        crate::source_evidence::link_verified_chunks_tx(
            &mut tx,
            f.finance.graph,
            f.finance.version,
            &f.ids,
        )
        .await
        .unwrap();
        let run: Uuid = sqlx::query_scalar("INSERT INTO corpus_enrichment_runs(graph_id,account_id,requested_by,mode,corpus_kind,status,extraction_version,model,max_items,max_characters,max_model_calls) VALUES($1,$2,$3,'execute','real','running','source-atoms-v1','deepseek/deepseek-v4-flash-0731',1000,8000000,10000) RETURNING run_id")
            .bind(f.finance.graph).bind(f.finance.account).bind(f.finance.payer)
            .fetch_one(&mut *tx).await.unwrap();
        sqlx::query("INSERT INTO corpus_enrichment_units(run_id,graph_id,kind,holding_id,source_item_id,extraction_version,state,execution_owner) SELECT $1,graph_id,'retained_version',version_id,source_item_id,extraction_version,'processed',true FROM source_versions WHERE version_id=$2")
            .bind(run).bind(f.finance.version).execute(&mut *tx).await.unwrap();
        tx.commit().await.unwrap();
        Some(run)
    } else {
        None
    };
    let job = f.job(false).await;
    let (cloud, wire, server) = loopback(true, Some(json!({"cost":0.000001}))).await;
    let mut operation = Box::pin(f.drive(&job, &cloud));
    tokio::select! {
        _ = wire.started.notified() => {},
        _ = &mut operation => panic!("worker ended before publication authority changed during HTTP"),
    }
    let marker = if let Some(run) = run {
        let mut tx = begin_plane(&f.finance.plane()).await.unwrap();
        sqlx::query("UPDATE corpus_enrichment_runs SET status='paused',pause_reason='operator_paused' WHERE run_id=$1")
            .bind(run).execute(&mut *tx).await.unwrap();
        tx.commit().await.unwrap();
        "blocked:managed_run_paused"
    } else {
        let mut tx = f.finance.financial().await;
        sqlx::query("UPDATE users SET deleted_at=clock_timestamp() WHERE id=$1")
            .bind(f.finance.payer)
            .execute(&mut *tx)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        "blocked:requester_access_lost"
    };
    wire.release.notify_one();
    operation.await;
    assert_eq!(wire.bodies.lock().unwrap().len(), 1);
    let mut tx = begin_plane(&f.finance.plane()).await.unwrap();
    let atoms: Vec<(String, bool, bool, bool, i32)> = sqlx::query_as("SELECT COALESCE(embed_model_next,''),COALESCE(content_tsv=to_tsvector('simple',content_inline),false),embedding_next IS NULL,embed_failed_at IS NOT NULL,embed_attempts FROM atoms WHERE atom_id=ANY($1)")
        .bind(&f.ids).fetch_all(&mut *tx).await.unwrap();
    assert_eq!(atoms.len(), 3);
    // A requester hold is an attempt; an operator pause waits uncounted.
    let attempts = if pause { 0 } else { 1 };
    for atom in atoms {
        assert_eq!(atom, (marker.into(), true, true, true, attempts));
    }
    let terminal: (String, String, bool) = sqlx::query_as("SELECT status::text,error_message,payload ? '_managed_embedding_claim' FROM jobs WHERE id=$1")
        .bind(job.id).fetch_one(&mut *tx).await.unwrap();
    assert_eq!(
        terminal,
        ("done".into(), "managed_embedding_held".into(), false)
    );
    let inputs: Vec<String> = sqlx::query_scalar("SELECT outcome FROM managed_embedding_inputs WHERE receipt_id IN (SELECT receipt_id FROM managed_attempt_receipts WHERE job_id=$1) ORDER BY ordinal")
        .bind(job.id).fetch_all(&mut *tx).await.unwrap();
    assert_eq!(inputs, vec!["discarded_stale"; 3]);
    let receipt: (Uuid, String, i64, Value) = sqlx::query_as("SELECT receipt_id,state,settled_micro_usd,to_jsonb(r) FROM managed_attempt_receipts r WHERE job_id=$1")
        .bind(job.id).fetch_one(&mut *tx).await.unwrap();
    assert_eq!((receipt.1.as_str(), receipt.2), ("billed", 1));
    let original_debits: i64 =
        sqlx::query_scalar("SELECT count(*) FROM credit_ledger WHERE source_receipt_id=$1")
            .bind(receipt.0)
            .fetch_one(&f.finance.control.owner)
            .await
            .unwrap();
    assert_eq!(original_debits, 1);
    if let Some(run) = run {
        sqlx::query(
            "UPDATE corpus_enrichment_runs SET status='complete',pause_reason=NULL WHERE run_id=$1",
        )
        .bind(run)
        .execute(&mut *tx)
        .await
        .unwrap();
    }
    sqlx::query("UPDATE atoms SET embed_failed_at=clock_timestamp()-interval '1 hour' WHERE atom_id=ANY($1)")
        .bind(&f.ids).execute(&mut *tx).await.unwrap();
    tx.commit().await.unwrap();
    if !pause {
        let mut tx = f.finance.financial().await;
        sqlx::query("UPDATE users SET deleted_at=NULL WHERE id=$1")
            .bind(f.finance.payer)
            .execute(&mut *tx)
            .await
            .unwrap();
        tx.commit().await.unwrap();
    }
    let worker = WorkerTenant {
        tenant: f.finance.tenant.clone(),
        pool: f.finance.app().clone(),
        store: Arc::new(crate::store::PostgresStore::new(f.finance.app().clone())),
    };
    crate::workers::embed_retry::tick(&worker, &crate::shutdown::Shutdown::new())
        .await
        .unwrap();
    let retry = crate::jobs::queue::dequeue(f.finance.app(), &["EmbedAtomsRetry"], 1)
        .await
        .unwrap()
        .pop()
        .expect("publication hold must remain reachable through the actual sweep");
    assert_ne!(retry.id, job.id);
    let (retry_ids, bypass) = crate::workers::embed::parse_atom_ids(&retry).unwrap();
    assert!(bypass);
    assert_eq!(
        retry_ids
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>(),
        f.ids.iter().copied().collect()
    );
    let mut retry_operation = Box::pin(f.drive(&retry, &cloud));
    tokio::select! {
        _ = wire.started.notified() => {},
        _ = &mut retry_operation => panic!("restored authority did not reach the real embedding transport"),
    }
    wire.release.notify_one();
    retry_operation.await;
    assert_eq!(wire.bodies.lock().unwrap().len(), 2);
    let mut tx = begin_plane(&f.finance.plane()).await.unwrap();
    let restored: (i64, i64, i64) = sqlx::query_as("SELECT count(*) FILTER(WHERE embedding_next IS NOT NULL AND embed_model_next=$2),count(*) FILTER(WHERE embed_failed_at IS NULL),count(*) FILTER(WHERE embed_attempts=$3) FROM atoms WHERE atom_id=ANY($1)")
        .bind(&f.ids).bind(coralswarm_core::embed::EMBED_MODEL_CLOUD).bind(attempts).fetch_one(&mut *tx).await.unwrap();
    assert_eq!(restored, (3, 3, 3));
    let unchanged: Value = sqlx::query_scalar(
        "SELECT to_jsonb(r) FROM managed_attempt_receipts r WHERE receipt_id=$1",
    )
    .bind(receipt.0)
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    assert_eq!(
        unchanged, receipt.3,
        "resumption cannot rewrite the charged refused publication"
    );
    let written: i64 = sqlx::query_scalar("SELECT count(*) FROM managed_embedding_inputs i JOIN managed_attempt_receipts r USING(receipt_id) WHERE r.job_id=$1 AND i.outcome='written' AND r.state='billed'")
        .bind(retry.id).fetch_one(&mut *tx).await.unwrap();
    assert_eq!(written, 3);
    tx.commit().await.unwrap();
    let debits: (i64, i64) = sqlx::query_as("SELECT count(*) FILTER(WHERE source_receipt_id=$2),count(*) FILTER(WHERE source_receipt_id IS NOT NULL) FROM credit_ledger WHERE account_id=$1")
        .bind(f.finance.account).bind(receipt.0).fetch_one(&f.finance.control.owner).await.unwrap();
    assert_eq!(debits, (1, 2));
    server.abort();
    assert!(server.await.unwrap_err().is_cancelled());
    f.finish().await;
}

#[tokio::test]
async fn c23_consumer_publication_requester_hold_resumes_through_actual_sweep() {
    publication_hold_resumes(false, false).await;
}
#[tokio::test]
async fn c23_org_publication_requester_hold_resumes_through_actual_sweep() {
    publication_hold_resumes(true, false).await;
}
#[tokio::test]
async fn c23_consumer_publication_operator_pause_resumes_through_actual_sweep() {
    publication_hold_resumes(false, true).await;
}
#[tokio::test]
async fn c23_org_publication_operator_pause_resumes_through_actual_sweep() {
    publication_hold_resumes(true, true).await;
}

async fn concurrent_owner(org: bool) {
    let f = EmbeddingFixture::new(org).await;
    let job = f.job(false).await;
    let (cloud, wire, server) = loopback(true, Some(json!({"cost":0.000001}))).await;
    let first = f.drive(&job, &cloud);
    let mut first = Box::pin(first);
    tokio::select! {_=wire.started.notified()=>{},_=&mut first=>panic!("first owner ended before POST")}
    let actual = crate::jobs::queue::JobRow {
        id: job.id,
        kind: job.kind.clone(),
        payload: sqlx::query_scalar("SELECT payload FROM jobs WHERE id=$1")
            .bind(job.id)
            .fetch_one(f.finance.owner())
            .await
            .unwrap(),
        attempts: job.attempts,
        account_id: job.account_id,
    };
    f.drive(&actual, &cloud).await;
    assert_eq!(wire.bodies.lock().unwrap().len(), 1);
    let state: String =
        sqlx::query_scalar("SELECT state FROM managed_attempt_receipts WHERE job_id=$1")
            .bind(job.id)
            .fetch_one(f.finance.owner())
            .await
            .unwrap();
    assert_eq!(
        state, "reserved",
        "same queue attempt cannot classify live owner ambiguous"
    );
    wire.release.notify_one();
    first.await;
    let state: String = sqlx::query_scalar("SELECT status::text FROM jobs WHERE id=$1")
        .bind(job.id)
        .fetch_one(f.finance.owner())
        .await
        .unwrap();
    assert_eq!(state, "done");
    server.abort();
    assert!(server.await.unwrap_err().is_cancelled());
    f.finish().await;
}
#[tokio::test]
async fn c23_consumer_same_attempt_owner_refuses_overlap_without_recovering_live_receipt() {
    concurrent_owner(false).await;
}
#[tokio::test]
async fn c23_org_same_attempt_owner_refuses_overlap_without_recovering_live_receipt() {
    concurrent_owner(true).await;
}

async fn reembed_worker(org: bool, funded: bool) {
    let f = EmbeddingFixture::new(org).await;
    if !funded {
        f.finance.balance(Decimal::ZERO).await;
    }
    let mut tx = begin_plane(&f.finance.plane()).await.unwrap();
    sqlx::query("UPDATE atoms SET embedding=$2 WHERE atom_id=ANY($1)")
        .bind(&f.ids)
        .bind(pgvector::Vector::from(vec![0.5; 384]))
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let id = crate::jobs::queue::enqueue(
        f.finance.app(),
        &crate::jobs::JobKind::ReembedOutdatedChunks {
            after_atom_id: None,
        },
        Some(f.finance.account),
        None,
        0,
    )
    .await
    .unwrap()
    .unwrap();
    let job = crate::jobs::queue::dequeue(f.finance.app(), &["ReembedOutdatedChunks"], 1)
        .await
        .unwrap()
        .pop()
        .expect("real numbered migration must admit Reembed job through actual queue trigger");
    assert_eq!(job.id, id);
    let (cloud, wire, server) = loopback(false, Some(json!({"cost":0.000001}))).await;
    let store = crate::store::PostgresStore::new(f.finance.app().clone());
    crate::workers::reembed::managed_job_for_test(
        &f.finance.tenant,
        f.finance.app(),
        &store,
        &cloud,
        &f.finance.control.app,
        job,
    )
    .await;
    assert_eq!(wire.bodies.lock().unwrap().len(), usize::from(funded));
    let mut tx = begin_plane(&f.finance.plane()).await.unwrap();
    let budget:Option<(String,i64)>=sqlx::query_as("SELECT purpose,limit_micro_usd FROM managed_budget_periods WHERE account_id=$1 AND purpose='cloud_embedding'").bind(f.finance.account).fetch_optional(&mut *tx).await.unwrap();
    if funded {
        assert_eq!(budget, Some(("cloud_embedding".into(), 1_000_000)));
    }
    let rows: Vec<(String, bool)> = sqlx::query_as(
        "SELECT embed_model_next,content_tsv IS NOT NULL FROM atoms WHERE atom_id=ANY($1)",
    )
    .bind(&f.ids)
    .fetch_all(&mut *tx)
    .await
    .unwrap();
    for row in rows {
        assert_eq!(
            row,
            (
                if funded {
                    coralswarm_core::embed::EMBED_MODEL_CLOUD
                } else {
                    "blocked:credits_exhausted"
                }
                .into(),
                true
            )
        );
    }
    tx.commit().await.unwrap();
    server.abort();
    assert!(server.await.unwrap_err().is_cancelled());
    f.finish().await;
}
#[tokio::test]
async fn c23_consumer_real_reembed_queue_admission_uses_same_funding_and_visible_hold() {
    for funded in [true, false] {
        reembed_worker(false, funded).await;
    }
}
#[tokio::test]
async fn c23_org_real_reembed_queue_admission_uses_same_funding_and_visible_hold() {
    for funded in [true, false] {
        reembed_worker(true, funded).await;
    }
}

async fn reaper_recovery(org: bool) {
    let f = EmbeddingFixture::new(org).await;
    let job = f.job(false).await;
    let (first_cloud, first_wire, first_server) =
        loopback(true, Some(json!({"cost":0.000001}))).await;
    let old = f.drive(&job, &first_cloud);
    let mut old = Box::pin(old);
    tokio::select! {_=first_wire.started.notified()=>{},_=&mut old=>panic!("old owner ended before POST")}
    let prior_owned = crate::jobs::queue::JobRow {
        id: job.id,
        kind: job.kind.clone(),
        payload: sqlx::query_scalar("SELECT payload FROM jobs WHERE id=$1")
            .bind(job.id)
            .fetch_one(f.finance.owner())
            .await
            .unwrap(),
        attempts: job.attempts,
        account_id: job.account_id,
    };
    let mut tx = begin_plane(&f.finance.plane()).await.unwrap();
    sqlx::query(
        "UPDATE atoms SET content_inline=content_inline||' new revision' WHERE atom_id=ANY($1)",
    )
    .bind(&f.ids)
    .execute(&mut *tx)
    .await
    .unwrap();
    sqlx::query("UPDATE jobs SET started_at=clock_timestamp()-interval '1 hour' WHERE id=$1")
        .bind(job.id)
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let reaped = crate::jobs::queue::reap(f.finance.app(), 1, 1, None)
        .await
        .unwrap();
    assert_eq!(reaped.len(), 1);
    sqlx::query("UPDATE jobs SET run_after=clock_timestamp() WHERE id=$1")
        .bind(job.id)
        .execute(f.finance.owner())
        .await
        .unwrap();
    let current = crate::jobs::queue::dequeue(f.finance.app(), &["EmbedAtoms"], 1)
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(current.attempts, job.attempts + 1);
    let (next_cloud, next_wire, next_server) = loopback(true, Some(json!({"cost":0.000001}))).await;
    let successor = f.drive(&current, &next_cloud);
    let mut successor = Box::pin(successor);
    tokio::select! {_=next_wire.started.notified()=>{},_=&mut successor=>panic!("successor ended before POST")}
    let stale_recovery = crate::managed::embedding::recover_owned_job_for_test(
        &f.finance.tenant,
        f.finance.app(),
        &f.finance.control.app,
        &prior_owned,
    )
    .await;
    assert_eq!(stale_recovery, Err(Deny::Hold(HoldCode::Fenced)));
    let live:String=sqlx::query_scalar("SELECT state FROM managed_attempt_receipts WHERE job_id=$1 ORDER BY created_at DESC,receipt_id DESC LIMIT 1")
        .bind(job.id).fetch_one(f.finance.owner()).await.unwrap();
    assert_eq!(
        live, "reserved",
        "a suspended prior owner must not discover or settle its successor's active attempt"
    );
    next_wire.release.notify_one();
    successor.await;
    assert_eq!(next_wire.bodies.lock().unwrap().len(), 1);
    let charged_before: Value = sqlx::query_scalar("SELECT jsonb_agg(to_jsonb(r) ORDER BY created_at,receipt_id) FROM managed_attempt_receipts r WHERE job_id=$1")
        .bind(job.id).fetch_one(f.finance.owner()).await.unwrap();
    let published_before: Value = sqlx::query_scalar(
        "SELECT jsonb_agg(to_jsonb(a) ORDER BY atom_id) FROM atoms a WHERE atom_id=ANY($1)",
    )
    .bind(&f.ids)
    .fetch_one(f.finance.owner())
    .await
    .unwrap();
    first_wire.release.notify_one();
    old.await;
    let mut tx = begin_plane(&f.finance.plane()).await.unwrap();
    let charged_after: Value = sqlx::query_scalar("SELECT jsonb_agg(to_jsonb(r) ORDER BY created_at,receipt_id) FROM managed_attempt_receipts r WHERE job_id=$1")
        .bind(job.id).fetch_one(&mut *tx).await.unwrap();
    let published_after: Value = sqlx::query_scalar(
        "SELECT jsonb_agg(to_jsonb(a) ORDER BY atom_id) FROM atoms a WHERE atom_id=ANY($1)",
    )
    .bind(&f.ids)
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    assert_eq!(charged_after, charged_before);
    assert_eq!(published_after, published_before);
    let receipts:Vec<(String,String)>=sqlx::query_as("SELECT state,request_hash FROM managed_attempt_receipts WHERE job_id=$1 ORDER BY created_at,receipt_id")
        .bind(job.id).fetch_all(&mut *tx).await.unwrap();
    assert_eq!(receipts.len(), 2);
    assert_eq!(receipts[0].0, "ambiguous");
    assert_eq!(receipts[1].0, "billed");
    assert_ne!(receipts[0].1, receipts[1].1);
    let old_outcomes:i64=sqlx::query_scalar("SELECT count(*) FROM managed_embedding_inputs i JOIN managed_attempt_receipts r USING(receipt_id) WHERE r.job_id=$1 AND r.state='ambiguous' AND i.outcome='sent'")
        .bind(job.id).fetch_one(&mut *tx).await.unwrap();
    assert_eq!(old_outcomes, 3);
    let old_inputs: Vec<(Uuid,i64,Vec<u8>,i64,String)> = sqlx::query_as("SELECT i.atom_id,i.content_revision,i.content_sha256,i.input_bytes,i.outcome FROM managed_embedding_inputs i JOIN managed_attempt_receipts r USING(receipt_id) WHERE r.job_id=$1 AND r.state='ambiguous' ORDER BY i.ordinal")
        .bind(job.id).fetch_all(&mut *tx).await.unwrap();
    let sent: Vec<String> =
        serde_json::from_value(first_wire.bodies.lock().unwrap()[0]["input"].clone()).unwrap();
    assert_eq!(old_inputs.len(), 3);
    for (input, text) in old_inputs.iter().zip(sent) {
        assert!(f.ids.contains(&input.0));
        assert_eq!(input.2, sha2::Sha256::digest(text.as_bytes()).to_vec());
        assert_eq!(input.3, text.len() as i64);
        assert_eq!(input.4, "sent");
        let successor: (i64,String) = sqlx::query_as("SELECT i.content_revision,i.outcome FROM managed_embedding_inputs i JOIN managed_attempt_receipts r USING(receipt_id) WHERE r.job_id=$1 AND r.state='billed' AND i.atom_id=$2")
            .bind(job.id).bind(input.0).fetch_one(&mut *tx).await.unwrap();
        assert_eq!(successor, (input.1 + 1, "written".into()));
    }
    let vectors: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM atoms WHERE atom_id=ANY($1) AND embedding_next IS NOT NULL",
    )
    .bind(&f.ids)
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    assert_eq!(vectors, 3);
    let status: String = sqlx::query_scalar("SELECT status::text FROM jobs WHERE id=$1")
        .bind(job.id)
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    assert_eq!(status, "done");
    tx.commit().await.unwrap();
    let debits: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM credit_ledger WHERE account_id=$1 AND source_receipt_id IS NOT NULL",
    )
    .bind(f.finance.account)
    .fetch_one(&f.finance.control.owner)
    .await
    .unwrap();
    assert_eq!(debits, 2);
    first_server.abort();
    next_server.abort();
    assert!(first_server.await.unwrap_err().is_cancelled());
    assert!(next_server.await.unwrap_err().is_cancelled());
    f.finish().await;
}
#[tokio::test]
async fn c23_consumer_actual_reaper_recovers_prior_hash_and_rejects_old_response() {
    reaper_recovery(false).await;
}
#[tokio::test]
async fn c23_org_actual_reaper_recovers_prior_hash_and_rejects_old_response() {
    reaper_recovery(true).await;
}

async fn owner_before_source(org: bool) {
    let f = EmbeddingFixture::new(org).await;
    let mut setup = begin_plane(&f.finance.plane()).await.unwrap();
    crate::source_evidence::link_verified_chunks_tx(
        &mut setup,
        f.finance.graph,
        f.finance.version,
        &f.ids,
    )
    .await
    .unwrap();
    let run: Uuid = sqlx::query_scalar("INSERT INTO corpus_enrichment_runs(graph_id,account_id,requested_by,mode,corpus_kind,status,extraction_version,model,max_items,max_characters,max_model_calls) VALUES($1,$2,$3,'execute','real','running','source-atoms-v1','deepseek/deepseek-v4-flash-0731',1000,8000000,10000) RETURNING run_id")
        .bind(f.finance.graph).bind(f.finance.account).bind(f.finance.payer).fetch_one(&mut *setup).await.unwrap();
    sqlx::query("INSERT INTO corpus_enrichment_units(run_id,graph_id,kind,holding_id,source_item_id,extraction_version,state,execution_owner) SELECT $1,graph_id,'retained_version',version_id,source_item_id,extraction_version,'processed',true FROM source_versions WHERE version_id=$2")
        .bind(run).bind(f.finance.version).execute(&mut *setup).await.unwrap();
    setup.commit().await.unwrap();
    let job = f.job(false).await;
    let worker_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(f.finance.app())
        .await
        .unwrap();
    let (cloud, wire, server) = loopback(true, Some(json!({"cost":0.000001}))).await;
    let worker = f.drive(&job, &cloud);
    let mut worker = Box::pin(worker);
    tokio::select! {_=wire.started.notified()=>{},_=&mut worker=>panic!("worker ended before POST")}
    let mut extraction = if org {
        crate::tenancy::scope::begin_tenant_system_scoped(f.finance.owner(), f.finance.account)
            .await
            .unwrap()
    } else {
        begin_account_scoped(f.finance.owner(), f.finance.account)
            .await
            .unwrap()
    };
    let extraction_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *extraction)
        .await
        .unwrap();
    sqlx::query("SELECT run_id FROM corpus_enrichment_runs WHERE run_id=$1 FOR UPDATE")
        .bind(run)
        .fetch_one(&mut *extraction)
        .await
        .unwrap();
    wire.release.notify_one();
    let blocked = async {
        loop {
            let waiting: bool = sqlx::query_scalar("SELECT $2=ANY(pg_blocking_pids($1))")
                .bind(worker_pid)
                .bind(extraction_pid)
                .fetch_one(f.finance.owner())
                .await
                .unwrap();
            if waiting {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    };
    tokio::select! {
        result=tokio::time::timeout(Duration::from_millis(1500),blocked)=>result.expect("publication must wait on the held execution owner"),
        _=&mut worker=>panic!("worker ended before the owner lock was released"),
    }
    let _source: Uuid = sqlx::query_scalar("SELECT s.source_item_id FROM source_items s JOIN source_versions v ON v.graph_id=s.graph_id AND v.source_item_id=s.source_item_id WHERE v.version_id=$1 FOR UPDATE OF s NOWAIT")
        .bind(f.finance.version).fetch_one(&mut *extraction).await.expect("embedding must not own the source while waiting on its execution owner");
    extraction.commit().await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), worker)
        .await
        .unwrap();
    let mut read = begin_plane(&f.finance.plane()).await.unwrap();
    let published: i64 = sqlx::query_scalar("SELECT count(*) FROM atoms WHERE atom_id=ANY($1) AND embedding_next IS NOT NULL AND embed_model_next=$2 AND content_tsv IS NOT NULL AND embed_failed_at IS NULL")
        .bind(&f.ids).bind(coralswarm_core::embed::EMBED_MODEL_CLOUD).fetch_one(&mut *read).await.unwrap();
    assert_eq!(published, 3);
    let written: i64 = sqlx::query_scalar("SELECT count(*) FROM managed_embedding_inputs i JOIN managed_attempt_receipts r USING(receipt_id) WHERE r.job_id=$1 AND r.state='billed' AND i.outcome='written'")
        .bind(job.id).fetch_one(&mut *read).await.unwrap();
    assert_eq!(written, 3);
    let done: bool = sqlx::query_scalar("SELECT status::text='done' FROM jobs WHERE id=$1")
        .bind(job.id)
        .fetch_one(&mut *read)
        .await
        .unwrap();
    assert!(done);
    read.commit().await.unwrap();
    let debits: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM credit_ledger WHERE account_id=$1 AND source_receipt_id IS NOT NULL",
    )
    .bind(f.finance.account)
    .fetch_one(&f.finance.control.owner)
    .await
    .unwrap();
    assert_eq!(debits, 1);
    assert_eq!(wire.bodies.lock().unwrap().len(), 1);
    server.abort();
    assert!(server.await.unwrap_err().is_cancelled());
    f.finish().await;
}

#[tokio::test]
async fn c23_consumer_publication_waits_on_owner_before_locking_source() {
    owner_before_source(false).await;
}
#[tokio::test]
async fn c23_org_publication_waits_on_owner_before_locking_source() {
    owner_before_source(true).await;
}

async fn current_pause(org: bool, retire: bool) {
    let f = EmbeddingFixture::new(org).await;
    let mut tx = begin_plane(&f.finance.plane()).await.unwrap();
    crate::source_evidence::link_verified_chunks_tx(
        &mut tx,
        f.finance.graph,
        f.finance.version,
        &f.ids,
    )
    .await
    .unwrap();
    let mut current_run = None;
    for current in [false, true] {
        let run:Uuid=sqlx::query_scalar("INSERT INTO corpus_enrichment_runs(graph_id,account_id,requested_by,mode,corpus_kind,status,pause_reason,extraction_version,model,max_items,max_characters,max_model_calls) VALUES($1,$2,$3,'execute','real','paused','operator_paused','source-atoms-v1','deepseek/deepseek-v4-flash-0731',1000,8000000,10000) RETURNING run_id")
            .bind(f.finance.graph).bind(f.finance.account).bind(f.finance.payer).fetch_one(&mut *tx).await.unwrap();
        sqlx::query("INSERT INTO corpus_enrichment_units(run_id,graph_id,kind,holding_id,source_item_id,extraction_version,state,execution_owner) SELECT $1,graph_id,'retained_version',version_id,source_item_id,extraction_version,'processed',$3 FROM source_versions WHERE version_id=$2")
            .bind(run).bind(f.finance.version).bind(current).execute(&mut *tx).await.unwrap();
        if current {
            current_run = Some(run);
        }
    }
    tx.commit().await.unwrap();
    let job = f.job(false).await;
    let (cloud, wire, server) = loopback(false, Some(json!({"cost":0.000001}))).await;
    f.drive(&job, &cloud).await;
    assert!(wire.bodies.lock().unwrap().is_empty());
    let mut tx = begin_plane(&f.finance.plane()).await.unwrap();
    let stamps: Vec<String> =
        sqlx::query_scalar("SELECT embed_model_next FROM atoms WHERE atom_id=ANY($1)")
            .bind(&f.ids)
            .fetch_all(&mut *tx)
            .await
            .unwrap();
    assert_eq!(stamps, vec!["blocked:managed_run_paused"; 3]);
    sqlx::query(if retire {
        "UPDATE corpus_enrichment_runs SET retired_at=clock_timestamp() WHERE run_id=$1"
    } else {
        "UPDATE corpus_enrichment_runs SET status='complete',pause_reason=NULL WHERE run_id=$1"
    })
    .bind(current_run.unwrap())
    .execute(&mut *tx)
    .await
    .unwrap();
    let owner_state: (bool, String, bool) = sqlx::query_as("SELECT r.retired_at IS NOT NULL,r.status,u.execution_owner FROM corpus_enrichment_runs r JOIN corpus_enrichment_units u USING(run_id) WHERE r.run_id=$1")
        .bind(current_run.unwrap()).fetch_one(&mut *tx).await.unwrap();
    assert_eq!(
        owner_state,
        (
            retire,
            if retire { "paused" } else { "complete" }.into(),
            true
        )
    );
    sqlx::query("UPDATE atoms SET embed_failed_at=clock_timestamp()-interval '1 hour' WHERE atom_id=ANY($1)").bind(&f.ids).execute(&mut *tx).await.unwrap();
    tx.commit().await.unwrap();
    let worker = WorkerTenant {
        tenant: f.finance.tenant.clone(),
        pool: f.finance.app().clone(),
        store: Arc::new(crate::store::PostgresStore::new(f.finance.app().clone())),
    };
    crate::workers::embed_retry::tick(&worker, &crate::shutdown::Shutdown::new())
        .await
        .unwrap();
    let retry = crate::jobs::queue::dequeue(f.finance.app(), &["EmbedAtomsRetry"], 1)
        .await
        .unwrap()
        .pop()
        .expect("actual retry sweep must preserve resumable managed hold");
    f.drive(&retry, &cloud).await;
    assert_eq!(
        wire.bodies.lock().unwrap().len(),
        1,
        "completed or retired execution owner and historical non-owner must not block new embedding"
    );
    let unit_cost:i64=sqlx::query_scalar("SELECT COALESCE(sum(charged_micro_usd),0)::bigint FROM corpus_enrichment_units WHERE graph_id=$1").bind(f.finance.graph).fetch_one(f.finance.owner()).await.unwrap();
    assert_eq!(
        unit_cost, 0,
        "unitless embedding must not debit any extraction unit"
    );
    server.abort();
    assert!(server.await.unwrap_err().is_cancelled());
    f.finish().await;
}
#[tokio::test]
async fn c23_consumer_current_operator_pause_holds_and_completed_retry_resumes() {
    for retire in [false, true] {
        current_pause(false, retire).await;
    }
}
#[tokio::test]
async fn c23_org_current_operator_pause_holds_and_completed_retry_resumes() {
    for retire in [false, true] {
        current_pause(true, retire).await;
    }
}

async fn bounded_rollover(org: bool) {
    let f = EmbeddingFixture::new(org).await;
    let mut tx = begin_plane(&f.finance.plane()).await.unwrap();
    sqlx::query("INSERT INTO managed_budget_periods(account_id,purpose,period_start,limit_micro_usd,charged_micro_usd) VALUES($1,'cloud_embedding',(clock_timestamp() AT TIME ZONE 'UTC')::date-1,1000000,1000000)")
        .bind(f.finance.account).execute(&mut *tx).await.unwrap();
    sqlx::query("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,timestamp,embed_model_next,embed_failed_at) SELECT $1,'note','bounded-'||n::text,'','bounded retained source',clock_timestamp(),'blocked:daily_budget_exhausted',clock_timestamp()-interval '1 hour' FROM generate_series(1,257) n")
        .bind(f.finance.graph).execute(&mut *tx).await.unwrap();
    tx.commit().await.unwrap();
    let worker = WorkerTenant {
        tenant: f.finance.tenant.clone(),
        pool: f.finance.app().clone(),
        store: Arc::new(crate::store::PostgresStore::new(f.finance.app().clone())),
    };
    crate::workers::embed_retry::tick(&worker, &crate::shutdown::Shutdown::new())
        .await
        .unwrap();
    // One sweep resumes exactly 256 of the 257 prior-day-exhausted atoms,
    // split into retry jobs of at most 64 (`embed_retry::RETRY_JOB_MAX_ATOMS`,
    // #1351: 500-atom retry jobs outlived the 20-minute lease).
    let jobs = crate::jobs::queue::dequeue(f.finance.app(), &["EmbedAtomsRetry"], 16)
        .await
        .unwrap();
    assert_eq!(jobs.len(), 4, "256 resumed atoms split into ceil(256/64) retry jobs");
    let mut all_ids = std::collections::HashSet::new();
    let mut parsed = Vec::new();
    for job in &jobs {
        let (ids, bypass) = crate::workers::embed::parse_atom_ids(job).unwrap();
        assert!(bypass);
        assert_eq!(ids.len(), 64, "every retry job is capped at 64 atoms");
        let mut tx = begin_plane(&f.finance.plane()).await.unwrap();
        let loaded = crate::workers::embed::load_pending_atoms(&mut *tx, &ids, bypass)
            .await
            .unwrap();
        assert_eq!(loaded.len(), 64);
        tx.commit().await.unwrap();
        all_ids.extend(ids.iter().copied());
        parsed.push(ids);
    }
    assert_eq!(all_ids.len(), 256, "the jobs name 256 distinct atoms, none twice");
    let (cloud, wire, server) = loopback(false, Some(json!({"cost":0.000001}))).await;
    let store = crate::store::PostgresStore::new(f.finance.app().clone());
    for (job, ids) in jobs.iter().zip(&parsed) {
        crate::workers::embed::managed_job_for_test(
            &f.finance.tenant,
            f.finance.app(),
            &store,
            &cloud,
            &f.finance.control.app,
            job,
            ids,
        )
        .await;
    }
    assert_eq!(wire.bodies.lock().unwrap().len(), 32, "256 atoms in batches of 8 across the four jobs");
    let mut tx = begin_plane(&f.finance.plane()).await.unwrap();
    let counts:(i64,i64)=sqlx::query_as("SELECT count(*) FILTER(WHERE embedding_next IS NOT NULL),count(*) FILTER(WHERE embed_model_next='blocked:daily_budget_exhausted') FROM atoms WHERE graph_id=$1 AND source_id LIKE 'bounded-%'")
        .bind(f.finance.graph).fetch_one(&mut *tx).await.unwrap();
    assert_eq!(counts, (256, 1));
    let budgets:Vec<(chrono::NaiveDate,i64,i64)>=sqlx::query_as("SELECT period_start,limit_micro_usd,charged_micro_usd FROM managed_budget_periods WHERE account_id=$1 AND purpose='cloud_embedding' ORDER BY period_start")
        .bind(f.finance.account).fetch_all(&mut *tx).await.unwrap();
    assert_eq!(budgets.len(), 2);
    assert_eq!(budgets[0].1, 1_000_000);
    assert_eq!(budgets[0].2, 1_000_000);
    assert_eq!(budgets[1].1, 1_000_000);
    assert_eq!(budgets[1].2, 32);
    assert_eq!(budgets[1].0, budgets[0].0.succ_opt().unwrap());
    tx.commit().await.unwrap();
    server.abort();
    assert!(server.await.unwrap_err().is_cancelled());
    f.finish().await;
}
#[tokio::test]
async fn c23_consumer_prior_utc_day_exhaustion_resumes_only_256_in_64_atom_retry_jobs_through_actual_sweep_and_worker() {
    bounded_rollover(false).await;
}
#[tokio::test]
async fn c23_org_prior_utc_day_exhaustion_resumes_only_256_in_64_atom_retry_jobs_through_actual_sweep_and_worker() {
    bounded_rollover(true).await;
}

async fn charged_response_refusal(org: bool) {
    for ambiguous in [false, true] {
        let f = EmbeddingFixture::new(org).await;
        let job = f.job(false).await;
        let (cloud, wire, server) = loopback(false, None).await;
        let mut reply =
            json!({"model":"openai/text-embedding-3-small","data":[{"index":0,"embedding":[1.0]}]});
        reply["usage"] = if ambiguous {
            json!({})
        } else {
            json!({"cost":0.000001})
        };
        *wire.reply.lock().unwrap() = Some(reply);
        f.drive(&job, &cloud).await;
        // An unusable answer is a permanent failure, but it was charged, so
        // it is never re-sent input by input.
        assert_eq!(wire.bodies.lock().unwrap().len(), 1);
        let mut tx = begin_plane(&f.finance.plane()).await.unwrap();
        let receipt:(String,i64,i64)=sqlx::query_as("SELECT state,settled_micro_usd,reserved_micro_usd FROM managed_attempt_receipts WHERE job_id=$1")
            .bind(job.id).fetch_one(&mut *tx).await.unwrap();
        assert_eq!(receipt.0, if ambiguous { "ambiguous" } else { "billed" });
        assert_eq!(receipt.1, if ambiguous { receipt.2 } else { 1 });
        assert!(receipt.1 > 0);
        let rows:Vec<(bool,bool,String)>=sqlx::query_as("SELECT embedding_next IS NULL,content_tsv IS NOT NULL,embed_model_next FROM atoms WHERE atom_id=ANY($1)")
            .bind(&f.ids).fetch_all(&mut *tx).await.unwrap();
        assert_eq!(rows.len(), 3);
        for row in rows {
            assert_eq!(row, (true, true, "blocked:provider_rejected".into()));
        }
        tx.commit().await.unwrap();
        let debits:i64=sqlx::query_scalar("SELECT count(*) FROM credit_ledger WHERE account_id=$1 AND source_receipt_id IS NOT NULL")
            .bind(f.finance.account).fetch_one(&f.finance.control.owner).await.unwrap();
        assert_eq!(debits, 1);
        server.abort();
        assert!(server.await.unwrap_err().is_cancelled());
        f.finish().await;
    }
}
#[tokio::test]
async fn c23_consumer_invalid_vectors_keep_actual_or_ambiguous_charge_and_keyword_hold() {
    charged_response_refusal(false).await;
}
#[tokio::test]
async fn c23_org_invalid_vectors_keep_actual_or_ambiguous_charge_and_keyword_hold() {
    charged_response_refusal(true).await;
}

async fn frozen_worker_rate(org: bool) {
    let f = EmbeddingFixture::new(org).await;
    let job = f.job(false).await;
    let (cloud, wire, server) = loopback(true, Some(json!({"prompt_tokens":50}))).await;
    let operation = f.drive(&job, &cloud);
    let mut operation = Box::pin(operation);
    tokio::select! {_=wire.started.notified()=>{},_=&mut operation=>panic!("worker ended before actual wire request")}
    sqlx::query("UPDATE model_rates SET input_per_million=200 WHERE provider='openrouter' AND model_pattern='text-embedding-3-small'")
        .execute(&f.finance.control.owner).await.unwrap();
    wire.release.notify_one();
    operation.await;
    let receipt: (String, i64) = sqlx::query_as(
        "SELECT state,settled_micro_usd FROM managed_attempt_receipts WHERE job_id=$1",
    )
    .bind(job.id)
    .fetch_one(f.finance.owner())
    .await
    .unwrap();
    assert_eq!(receipt, ("billed".into(), 1));
    assert_eq!(
        wire.bodies.lock().unwrap()[0]["provider"]["max_price"]["prompt"],
        0.02
    );
    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM managed_embedding_inputs WHERE outcome='written'")
            .fetch_one(f.finance.owner())
            .await
            .unwrap();
    assert_eq!(count, 3);
    server.abort();
    assert!(server.await.unwrap_err().is_cancelled());
    f.finish().await;
}
#[tokio::test]
async fn c23_consumer_token_usage_settles_frozen_actual_worker_rate() {
    frozen_worker_rate(false).await;
}
#[tokio::test]
async fn c23_org_token_usage_settles_frozen_actual_worker_rate() {
    frozen_worker_rate(true).await;
}

struct ReadBlobs {
    mode: u8,
    reads: std::sync::atomic::AtomicUsize,
}
#[async_trait::async_trait]
impl crate::services::blob_store::PromotionBlobStore for ReadBlobs {
    async fn get_blob(
        &self,
        _key: &str,
        _dek: &[u8; 32],
    ) -> Result<Vec<u8>, coralswarm_core::r2::R2Error> {
        self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        match self.mode {
            0 => Ok(b"authoritative blob bytes".to_vec()),
            1 => Err(coralswarm_core::r2::R2Error::NotFound(
                "local-fixture".into(),
            )),
            2 => Err(coralswarm_core::r2::R2Error::S3(
                "local-fixture transport failure".into(),
            )),
            _ => Ok(vec![0xff]),
        }
    }
    async fn put_blob(
        &self,
        _key: &str,
        _body: &[u8],
        _dek: &[u8; 32],
    ) -> Result<(), coralswarm_core::r2::R2Error> {
        panic!("embedding must not upload content")
    }
    async fn delete_blob(&self, _key: &str) -> Result<(), coralswarm_core::r2::R2Error> {
        panic!("embedding must not delete content")
    }
}

async fn authoritative_blob(org: bool) {
    for mode in 0..4 {
        let f = EmbeddingFixture::new(org).await;
        let job = f.job(false).await;
        let (cloud, wire, server) = loopback(false, Some(json!({"cost":0.000001}))).await;
        let blobs = ReadBlobs {
            mode,
            reads: std::sync::atomic::AtomicUsize::new(0),
        };
        let store = crate::store::PostgresStore::new(f.finance.app().clone());
        let result = crate::managed::embedding::process(
            &f.finance.tenant,
            f.finance.app(),
            &f.finance.control.app,
            &job,
            &f.ids,
            &cloud,
            Some(&blobs),
            &store,
        )
        .await;
        if mode < 2 {
            result
                .unwrap()
                .finish(&f.finance.tenant, f.finance.app(), None)
                .await
                .unwrap();
            let bodies = wire.bodies.lock().unwrap().clone();
            assert_eq!(bodies.len(), 1);
            let sent: Vec<String> = serde_json::from_value(bodies[0]["input"].clone()).unwrap();
            if mode == 0 {
                assert_eq!(sent, vec!["authoritative blob bytes"; 3]);
            } else {
                let mut expected = f.texts.clone();
                let mut actual = sent.clone();
                expected.sort();
                actual.sort();
                assert_eq!(actual, expected);
            }
            let rows: Vec<(i16, Vec<u8>)> = sqlx::query_as(
                "SELECT ordinal,content_sha256 FROM managed_embedding_inputs ORDER BY ordinal",
            )
            .fetch_all(f.finance.owner())
            .await
            .unwrap();
            for (ordinal, hash) in rows {
                assert_eq!(
                    hash,
                    sha2::Sha256::digest(sent[ordinal as usize].as_bytes()).to_vec()
                );
            }
        } else {
            assert!(matches!(result, Err(Deny::Unavailable)));
            assert!(wire.bodies.lock().unwrap().is_empty());
            let count: i64 =
                sqlx::query_scalar("SELECT count(*) FROM managed_attempt_receipts WHERE job_id=$1")
                    .bind(job.id)
                    .fetch_one(f.finance.owner())
                    .await
                    .unwrap();
            assert_eq!(count, 0);
        }
        assert!(blobs.reads.load(std::sync::atomic::Ordering::SeqCst) > 0);
        server.abort();
        assert!(server.await.unwrap_err().is_cancelled());
        f.finish().await;
    }
}
#[tokio::test]
async fn c23_consumer_blob_first_notfound_only_and_utf8_failure_use_exact_input_owner() {
    authoritative_blob(false).await;
}
#[tokio::test]
async fn c23_org_blob_first_notfound_only_and_utf8_failure_use_exact_input_owner() {
    authoritative_blob(true).await;
}

struct WaitingBlobs {
    started: Notify,
    release: Notify,
    reads: std::sync::atomic::AtomicUsize,
}
#[async_trait::async_trait]
impl crate::services::blob_store::PromotionBlobStore for WaitingBlobs {
    async fn get_blob(
        &self,
        _key: &str,
        _dek: &[u8; 32],
    ) -> Result<Vec<u8>, coralswarm_core::r2::R2Error> {
        if self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
            self.started.notify_one();
            self.release.notified().await;
        }
        Ok(b"current source bytes".to_vec())
    }
    async fn put_blob(
        &self,
        _key: &str,
        _body: &[u8],
        _dek: &[u8; 32],
    ) -> Result<(), coralswarm_core::r2::R2Error> {
        panic!("embedding must not upload content")
    }
    async fn delete_blob(&self, _key: &str) -> Result<(), coralswarm_core::r2::R2Error> {
        panic!("embedding must not delete content")
    }
}

async fn revoked_during_blob(org: bool) {
    let f = EmbeddingFixture::new(org).await;
    let mut tx = begin_plane(&f.finance.plane()).await.unwrap();
    crate::source_evidence::link_verified_chunks_tx(
        &mut tx,
        f.finance.graph,
        f.finance.version,
        &f.ids,
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    let job = f.job(false).await;
    let (cloud, wire, server) = loopback(false, Some(json!({"cost":0.000001}))).await;
    let blobs = WaitingBlobs {
        started: Notify::new(),
        release: Notify::new(),
        reads: std::sync::atomic::AtomicUsize::new(0),
    };
    let store = crate::store::PostgresStore::new(f.finance.app().clone());
    let operation = crate::managed::embedding::process(
        &f.finance.tenant,
        f.finance.app(),
        &f.finance.control.app,
        &job,
        &f.ids,
        &cloud,
        Some(&blobs),
        &store,
    );
    let mut operation = Box::pin(operation);
    tokio::select! {_=blobs.started.notified()=>{},_=&mut operation=>panic!("operation ended before real owner blob read")}
    let mut tx = begin_plane(&f.finance.plane()).await.unwrap();
    sqlx::query("UPDATE source_items SET access_state='restricted' WHERE graph_id=$1")
        .bind(f.finance.graph)
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    blobs.release.notify_one();
    let report = operation.await.unwrap();
    assert_eq!(report.blocked, 3);
    report
        .finish(&f.finance.tenant, f.finance.app(), None)
        .await
        .unwrap();
    assert!(wire.bodies.lock().unwrap().is_empty());
    let receipts: i64 =
        sqlx::query_scalar("SELECT count(*) FROM managed_attempt_receipts WHERE job_id=$1")
            .bind(job.id)
            .fetch_one(f.finance.owner())
            .await
            .unwrap();
    assert_eq!(receipts, 0);
    let leaked_index: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM atoms WHERE atom_id=ANY($1) AND content_tsv IS NOT NULL",
    )
    .bind(&f.ids)
    .fetch_one(f.finance.owner())
    .await
    .unwrap();
    assert_eq!(
        leaked_index, 0,
        "revoked bytes cannot be reindexed by hold fallback"
    );
    server.abort();
    assert!(server.await.unwrap_err().is_cancelled());
    f.finish().await;
}
#[tokio::test]
async fn c23_consumer_source_revocation_during_blob_io_blocks_post_and_keyword_fallback() {
    revoked_during_blob(false).await;
}
#[tokio::test]
async fn c23_org_source_revocation_during_blob_io_blocks_post_and_keyword_fallback() {
    revoked_during_blob(true).await;
}

/// The 2026-10-13 constant fence left atoms keyword-indexed, stamped
/// `blocked:managed_purpose_unsupported`, with no vector and `embed_failed_at`
/// NULL. They must be pending again, claimed by the actual retry sweep with no
/// manual SQL, and embedded by the actual worker entry point (`process_job`,
/// not the test hook), which proves the runtime gate is open.
async fn legacy_fence_backlog(org: bool) {
    let f = EmbeddingFixture::new(org).await;
    let mut tx = begin_plane(&f.finance.plane()).await.unwrap();
    sqlx::query("UPDATE atoms SET embed_model_next=$2,embedding_next=NULL,embed_failed_at=NULL,content_tsv=to_tsvector('simple',content_inline) WHERE atom_id=ANY($1)")
        .bind(&f.ids).bind(crate::workers::embed::BLOCKED_MANAGED_PURPOSE).execute(&mut *tx).await.unwrap();
    assert_eq!(
        crate::workers::embed::load_pending_atoms(&mut *tx, &f.ids, false)
            .await
            .unwrap()
            .len(),
        3,
        "fence backlog is pending for an ordinary EmbedAtoms job"
    );
    tx.commit().await.unwrap();

    let worker = WorkerTenant {
        tenant: f.finance.tenant.clone(),
        pool: f.finance.app().clone(),
        store: Arc::new(crate::store::PostgresStore::new(f.finance.app().clone())),
    };
    crate::workers::embed_retry::tick(&worker, &crate::shutdown::Shutdown::new())
        .await
        .unwrap();
    let job = crate::jobs::queue::dequeue(f.finance.app(), &["EmbedAtomsRetry"], 1)
        .await
        .unwrap()
        .pop()
        .expect("the retry sweep claims the fence backlog");
    let (mut ids, bypass) = crate::workers::embed::parse_atom_ids(&job).unwrap();
    assert!(bypass);
    ids.sort();
    let mut expected = f.ids.clone();
    expected.sort();
    assert_eq!(ids, expected);
    let mut tx = begin_plane(&f.finance.plane()).await.unwrap();
    let markers: Vec<Option<String>> = sqlx::query_scalar(
        "SELECT embed_model_next FROM atoms WHERE atom_id=ANY($1) AND embed_failed_at IS NOT NULL",
    )
    .bind(&f.ids)
    .fetch_all(&mut *tx)
    .await
    .unwrap();
    assert_eq!(markers, vec![None; 3], "the claim clears the fence stamp");
    tx.commit().await.unwrap();

    let (cloud, wire, server) = loopback(false, Some(json!({"cost":0.000001}))).await;
    let store = crate::store::PostgresStore::new(f.finance.app().clone());
    let job_id = job.id;
    crate::workers::embed::process_job(
        &f.finance.tenant,
        f.finance.app(),
        &store,
        &cloud,
        Some(&cloud),
        Some(&f.finance.control.app),
        None,
        job,
    )
    .await;
    assert_eq!(wire.bodies.lock().unwrap().len(), 1, "one gated POST for the 3-atom batch");
    let mut tx = begin_plane(&f.finance.plane()).await.unwrap();
    let rows: Vec<(Option<String>, bool, bool)> = sqlx::query_as(
        "SELECT embed_model_next,embedding_next IS NOT NULL,content_tsv IS NOT NULL FROM atoms WHERE atom_id=ANY($1)",
    )
    .bind(&f.ids)
    .fetch_all(&mut *tx)
    .await
    .unwrap();
    assert_eq!(
        rows,
        vec![(Some(coralswarm_core::embed::EMBED_MODEL_CLOUD.into()), true, true); 3]
    );
    let receipts: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM managed_attempt_receipts WHERE job_id=$1 AND purpose='cloud_embedding'",
    )
    .bind(job_id)
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    assert_eq!(receipts, 1);
    tx.commit().await.unwrap();
    let status: String = sqlx::query_scalar("SELECT status::text FROM jobs WHERE id=$1")
        .bind(job_id)
        .fetch_one(f.finance.owner())
        .await
        .unwrap();
    assert_eq!(status, "done");
    server.abort();
    assert!(server.await.unwrap_err().is_cancelled());
    f.finish().await;
}

#[tokio::test]
async fn c23_consumer_legacy_fence_backlog_backfills_through_actual_sweep_and_worker() {
    legacy_fence_backlog(false).await;
}

#[tokio::test]
async fn c23_org_legacy_fence_backlog_backfills_through_actual_sweep_and_worker() {
    legacy_fence_backlog(true).await;
}

// ── NUL in atom text (prod, 2026-10-25) ───────────────────────────────────────
//
// A GitHub file with a literal NUL in a template string was captured with the
// NUL in its chunk body (the object store never passed through Postgres'
// check). Every embed of that chunk paid for the provider call, then failed
// the publish's `content_tsv` write with `invalid byte sequence for encoding
// "UTF8": 0x00`, and the job stayed `running` for the reaper: 309 paid sends
// for one atom, about six dead retry jobs an hour.

/// The prod body's shape: a cache key joined with a NUL.
const NUL_BODY: &[u8] = b"const cacheKey = `${url}\0${scopeKey(authHeader)}`;";

struct NulBlobs;
#[async_trait::async_trait]
impl crate::services::blob_store::PromotionBlobStore for NulBlobs {
    async fn get_blob(&self, _key: &str, _dek: &[u8; 32]) -> Result<Vec<u8>, coralswarm_core::r2::R2Error> {
        Ok(NUL_BODY.to_vec())
    }
    async fn put_blob(&self, _key: &str, _body: &[u8], _dek: &[u8; 32]) -> Result<(), coralswarm_core::r2::R2Error> {
        panic!("embedding must not upload content")
    }
    async fn delete_blob(&self, _key: &str) -> Result<(), coralswarm_core::r2::R2Error> {
        panic!("embedding must not delete content")
    }
}

async fn job_row(f: &EmbeddingFixture, id: Uuid) -> (String, i32, Option<String>) {
    sqlx::query_as("SELECT status::text,attempts,error_message FROM jobs WHERE id=$1")
        .bind(id)
        .fetch_one(f.finance.owner())
        .await
        .unwrap()
}

/// An existing atom whose sealed body holds a NUL (the prod atoms): the read
/// cleans it, so it embeds, is keyword-indexed and the job completes.
async fn nul_body_in_the_object_store_embeds(org: bool) {
    let f = EmbeddingFixture::new(org).await;
    let job = f.job(true).await;
    let (cloud, wire, server) = loopback(false, Some(json!({"cost":0.000001}))).await;
    let store = crate::store::PostgresStore::new(f.finance.app().clone());
    crate::workers::embed::managed_job_with_blobs_for_test(
        &f.finance.tenant,
        f.finance.app(),
        &store,
        &cloud,
        &f.finance.control.app,
        Some(&NulBlobs),
        &job,
        &f.ids,
    )
    .await;
    let (status, _, error) = job_row(&f, job.id).await;
    assert_eq!(status, "done", "{error:?}");
    let bodies = wire.bodies.lock().unwrap().clone();
    assert_eq!(bodies.len(), 1, "one paid request, never a re-send");
    let mut tx = begin_plane(&f.finance.plane()).await.unwrap();
    let rows: Vec<(Option<String>, bool, bool, i32)> = sqlx::query_as(
        "SELECT embed_model_next,embedding_next IS NOT NULL,content_tsv @@ to_tsquery('simple','scopekey'),embed_attempts FROM atoms WHERE atom_id=ANY($1)",
    )
    .bind(&f.ids)
    .fetch_all(&mut *tx)
    .await
    .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(rows, vec![(Some(coralswarm_core::embed::EMBED_MODEL_CLOUD.into()), true, true, 0); 3]);
    let sent: Vec<String> = serde_json::from_value(bodies[0]["input"].clone()).unwrap();
    assert_eq!(sent, vec!["const cacheKey = `${url}\u{FFFD}${scopeKey(authHeader)}`;"; 3]);
    server.abort();
    assert!(server.await.unwrap_err().is_cancelled());
    f.finish().await;
}
#[tokio::test]
async fn consumer_nul_body_in_the_object_store_embeds() {
    nul_body_in_the_object_store_embeds(false).await;
}
#[tokio::test]
async fn org_nul_body_in_the_object_store_embeds() {
    nul_body_in_the_object_store_embeds(true).await;
}

/// A capture whose text, metadata and display names hold NUL lands through
/// the funnel with every NUL replaced, then embeds. Dev mode stores chunk
/// bodies inline, in Postgres, so before the funnel cleaned capture text this
/// capture failed outright at "inline chunk content".
async fn nul_capture_lands_clean_and_embeds(org: bool) {
    use crate::ingest::capture::*;
    let mut f = EmbeddingFixture::new(org).await;
    let body = String::from_utf8(NUL_BODY.to_vec()).unwrap();
    let participants = vec!["Ali\0ce".to_string()];
    let metadata = json!({"title": "transport\0.ts", "path": ["web", "lib\0"]});
    let chunks = coralswarm_core::chunker::chunk(&body, coralswarm_core::chunker::ChunkStrategy::Generic);
    let prepared = Prepared::new("nul-src", "note", &body, &chunks, &Header::Recorded("Repo: web\0".into()), None);
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(f.finance.app(), f.finance.account).await.unwrap();
    let (version_id, written) = store_for_test(
        &mut tx,
        f.finance.graph,
        f.finance.account,
        crate::source_evidence::CaptureSource {
            source_type: "note",
            provider_namespace: "declared:user:nul",
            source_id: "nul-src",
            normalized_text: &body,
            representation: "normalized_text",
            metadata: &metadata,
            occurred_at: None,
            participants: &participants,
            ingested_by: None,
            scope: "private",
            duration_ms: None,
            segments: None,
        },
        &prepared,
        AtomFields {
            author_name: Some("Ali\0ce"),
            channel_name: Some("#e\0ng"),
            ..AtomFields::new("note", &participants, Utc::now(), None)
        },
    )
    .await
    .expect("a NUL-bearing capture lands");
    tx.commit().await.unwrap();
    assert!(!written.is_empty());

    let mut tx = begin_plane(&f.finance.plane()).await.unwrap();
    let rows: Vec<(String, Vec<String>, Option<String>, Option<String>)> = sqlx::query_as(
        "SELECT content_inline,participants,author_name,channel_name FROM atoms WHERE atom_id=ANY($1)",
    )
    .bind(&written)
    .fetch_all(&mut *tx)
    .await
    .unwrap();
    for (text, people, author, channel) in &rows {
        assert!(text.contains("${url}\u{FFFD}${scopeKey"), "{text}");
        assert!(text.starts_with("Repo: web\u{FFFD}\n\n"), "{text}");
        assert_eq!(people, &vec!["Ali\u{FFFD}ce".to_string()]);
        assert_eq!(author.as_deref(), Some("Ali\u{FFFD}ce"));
        assert_eq!(channel.as_deref(), Some("#e\u{FFFD}ng"));
    }
    let snapshot: Value = sqlx::query_scalar("SELECT source_metadata FROM source_versions WHERE version_id=$1")
        .bind(version_id)
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    assert_eq!(snapshot["title"], "transport\u{FFFD}.ts");
    assert_eq!(snapshot["path"][1], "lib\u{FFFD}");
    tx.commit().await.unwrap();

    f.ids = written.clone();
    // The capture's own embed job, enqueued in its transaction.
    let job = crate::jobs::queue::dequeue(f.finance.app(), &["EmbedAtoms"], 1)
        .await
        .unwrap()
        .pop()
        .expect("the capture enqueued its embed job with its rows");
    let (cloud, wire, server) = loopback(false, Some(json!({"cost":0.000001}))).await;
    f.drive(&job, &cloud).await;
    assert_eq!(wire.bodies.lock().unwrap().len(), 1);
    let mut tx = begin_plane(&f.finance.plane()).await.unwrap();
    let embedded: Vec<(Option<String>, bool, bool)> = sqlx::query_as(
        "SELECT embed_model_next,embedding_next IS NOT NULL,content_tsv IS NOT NULL FROM atoms WHERE atom_id=ANY($1)",
    )
    .bind(&written)
    .fetch_all(&mut *tx)
    .await
    .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(embedded, vec![(Some(coralswarm_core::embed::EMBED_MODEL_CLOUD.into()), true, true); written.len()]);
    assert_eq!(job_row(&f, job.id).await.0, "done");
    server.abort();
    assert!(server.await.unwrap_err().is_cancelled());
    f.finish().await;
}
#[tokio::test]
async fn consumer_nul_capture_lands_clean_and_embeds() {
    nul_capture_lands_clean_and_embeds(false).await;
}
#[tokio::test]
async fn org_nul_capture_lands_clean_and_embeds() {
    nul_capture_lands_clean_and_embeds(true).await;
}

/// The object store fails every read, with a NUL in the error text.
struct FailingBlobs;
#[async_trait::async_trait]
impl crate::services::blob_store::PromotionBlobStore for FailingBlobs {
    async fn get_blob(&self, _key: &str, _dek: &[u8; 32]) -> Result<Vec<u8>, coralswarm_core::r2::R2Error> {
        Err(coralswarm_core::r2::R2Error::S3("local-fixture reset\0mid-body".into()))
    }
    async fn put_blob(&self, _key: &str, _body: &[u8], _dek: &[u8; 32]) -> Result<(), coralswarm_core::r2::R2Error> {
        panic!("embedding must not upload content")
    }
    async fn delete_blob(&self, _key: &str) -> Result<(), coralswarm_core::r2::R2Error> {
        panic!("embedding must not delete content")
    }
}

async fn attempts(f: &EmbeddingFixture) -> Vec<i32> {
    let mut tx = begin_plane(&f.finance.plane()).await.unwrap();
    let rows: Vec<i32> = sqlx::query_scalar("SELECT embed_attempts FROM atoms WHERE atom_id=ANY($1)")
        .bind(&f.ids)
        .fetch_all(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    rows
}

async fn run_failing(
    f: &EmbeddingFixture,
    store: &crate::store::PostgresStore,
    cloud: &OpenRouterEmbedder,
    job: &crate::jobs::queue::JobRow,
) {
    crate::workers::embed::managed_job_with_blobs_for_test(
        &f.finance.tenant,
        f.finance.app(),
        store,
        cloud,
        &f.finance.control.app,
        Some(&FailingBlobs),
        job,
        &f.ids,
    )
    .await;
}

/// A run that ends `Deny::Unavailable` is settled by the worker, not left
/// `running` for the reaper: it fails on the job's ladder with the cause
/// recorded (NUL cleaned), and its last attempt charges the atoms one embed
/// attempt, so the retry sweep's cap ends a failure that repeats.
async fn unavailable_run_is_settled_and_counted(org: bool) {
    let f = EmbeddingFixture::new(org).await;
    let job = f.job(true).await;
    let (cloud, wire, server) = loopback(false, Some(json!({"cost":0.000001}))).await;
    let store = crate::store::PostgresStore::new(f.finance.app().clone());
    run_failing(&f, &store, &cloud, &job).await;
    let (status, tries, error) = job_row(&f, job.id).await;
    assert_eq!((status.as_str(), tries), ("pending", job.attempts + 1), "back on the ladder, not left running");
    let error = error.unwrap();
    assert!(error.starts_with("managed embedding unavailable: "), "{error}");
    assert!(error.contains("local-fixture reset\u{FFFD}mid-body"), "{error}");
    assert_eq!(attempts(&f).await, vec![0; 3], "an attempt with more to come charges no atom");

    sqlx::query("UPDATE jobs SET run_after=now(),max_attempts=attempts+1 WHERE id=$1")
        .bind(job.id)
        .execute(f.finance.owner())
        .await
        .unwrap();
    let last = crate::jobs::queue::dequeue(f.finance.app(), &["EmbedAtoms", "EmbedAtomsRetry"], 4)
        .await
        .unwrap()
        .into_iter()
        .find(|row| row.id == job.id)
        .expect("the failed job runs again");
    run_failing(&f, &store, &cloud, &last).await;
    assert_eq!(job_row(&f, job.id).await.0, "dead");
    assert_eq!(attempts(&f).await, vec![1; 3], "the last attempt charges each atom one embed attempt");
    assert!(wire.bodies.lock().unwrap().is_empty(), "nothing was sent");
    server.abort();
    assert!(server.await.unwrap_err().is_cancelled());
    f.finish().await;
}
#[tokio::test]
async fn consumer_unavailable_run_is_settled_and_counted() {
    unavailable_run_is_settled_and_counted(false).await;
}
#[tokio::test]
async fn org_unavailable_run_is_settled_and_counted() {
    unavailable_run_is_settled_and_counted(true).await;
}
