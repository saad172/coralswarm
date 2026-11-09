//! `quality_eval_ask`: run a question file through the real scoped Ask path
//! (`scoped_ask::execute`, the function `POST /v1/queries` calls) in-process,
//! as a named org member, and write one JSON record per question. COR-66.
//!
//! ```text
//! quality_eval_ask --questions <file.json> --out <file.json> --org-slug <slug>
//!     --user-id <users.id> [--sleep-ms 1500] [--hold-secs 0]
//! ```
//!
//! Why a bin and not HTTP: the HTTP path needs a Clerk session for a real
//! user, and minting org-context sessions mutates Clerk user state (it once
//! broke the founder's prod login). This bin never talks to Clerk. It builds
//! the production `AppState` with [`coralswarm_backend::build_cloud_state`]
//! (no workers, no listeners), resolves the org's tenant pool through the
//! production `TenantRuntime`, and calls Ask with a `UserContext` for
//! `--user-id`, which must be a current member of that org's account.
//!
//! **What it writes.** Only what any Ask writes: the conversation, the two
//! messages, the answer-support receipts, `query_history`, and the metered
//! `llm_usage` / `interactive_ask` receipts (billed to the org account, under
//! its $1/day `interactive_ask` cap). It reads nothing else and changes no
//! source, reef or edge. Use a test identity, not anyone's primary login: the
//! conversations appear in that user's Ask history.
//!
//! **Input** (`--questions`): `{"questions":[{"id","graph_ids" (or "graph_id"),"text",
//! "style"?,"candidate_limit"?}]}`. **Output** (`--out`): `{"meta",
//! "results":[…]}` with the answer text, the evidence ids in client `[n]`
//! order with `jev_verdict`, Jev phases, timings and the `query_id` that
//! `llm_usage.source_query_id` joins on. Evidence bodies and quotes are NOT
//! written, so the output holds the answer and ids only. stderr carries one
//! line per question with the id, status and milliseconds, never content, so
//! Fly logs stay free of captured text.
//!
//! `--hold-secs N` keeps the process (and so a `--rm` one-shot machine)
//! alive up to N seconds after writing `--out`, until `<out>.fetched` exists,
//! so the operator can `fly ssh console -C "cat <out>"` and then touch it.
//! See `backend/eval/QUALITY_EVAL.md` for the full one-command flow.
//!
//! Exit codes: 0 every question ran (an Ask refusal is a recorded result,
//! not a failure); 1 setup failed; 2 usage error.

use std::sync::Arc;
use std::time::{Duration, Instant};

use coralswarm_backend::scoped_ask::{self, AskRequest};
use coralswarm_backend::store::{PostgresStore, Store};
use coralswarm_backend::tenancy::extractor::Tenant;
use coralswarm_backend::tenancy::runtime::{TenancyDecision, TenantContext};
use coralswarm_backend::UserContext;
use serde_json::{json, Value};
use uuid::Uuid;

struct Args {
    questions: String,
    out: String,
    org_slug: String,
    user_id: Uuid,
    sleep_ms: u64,
    hold_secs: u64,
}

fn usage(msg: &str) -> ! {
    eprintln!("quality_eval_ask: {msg}");
    eprintln!("usage: quality_eval_ask --questions <file> --out <file> --org-slug <slug> --user-id <uuid> [--sleep-ms N] [--hold-secs N]");
    std::process::exit(2);
}

fn parse_args() -> Args {
    let mut it = std::env::args().skip(1);
    let (mut questions, mut out, mut org_slug, mut user_id) = (None, None, None, None);
    let (mut sleep_ms, mut hold_secs) = (1500u64, 0u64);
    while let Some(flag) = it.next() {
        let mut value = || it.next().unwrap_or_else(|| usage(&format!("{flag} needs a value")));
        match flag.as_str() {
            "--questions" => questions = Some(value()),
            "--out" => out = Some(value()),
            "--org-slug" => org_slug = Some(value()),
            "--user-id" => user_id = Some(value().parse().unwrap_or_else(|_| usage("--user-id must be a uuid"))),
            "--sleep-ms" => sleep_ms = value().parse().unwrap_or_else(|_| usage("--sleep-ms must be an integer")),
            "--hold-secs" => hold_secs = value().parse().unwrap_or_else(|_| usage("--hold-secs must be an integer")),
            other => usage(&format!("unknown argument {other}")),
        }
    }
    Args {
        questions: questions.unwrap_or_else(|| usage("--questions is required")),
        out: out.unwrap_or_else(|| usage("--out is required")),
        org_slug: org_slug.unwrap_or_else(|| usage("--org-slug is required")),
        user_id: user_id.unwrap_or_else(|| usage("--user-id is required")),
        sleep_ms,
        hold_secs,
    }
}

/// The evidence ids the client resolves `[n]` against, in the same order:
/// retained citations (one per outcome) then legacy chunks. Quotes and chunk
/// bodies are dropped on purpose.
fn evidence(payload: &Value) -> Vec<Value> {
    let mut out = Vec::new();
    for outcome in payload["outcomes"].as_array().into_iter().flatten() {
        for c in outcome["citations"].as_array().into_iter().flatten() {
            out.push(json!({
                "kind": "source_passage",
                "semantic_atom_id": c["semantic_atom_id"], "passage_id": c["passage_id"],
                "source_item_id": c["source_item_id"], "source_version_id": c["source_version_id"],
                "source_type": c["source_type"], "occurred_at": c["occurred_at"],
                "outcome_kind": outcome["kind"],
                "source_version_is_current": outcome["source_version_is_current"],
                "superseded": outcome.get("supersession").is_some_and(|s| !s.is_null()),
                "jev_verdict": c.get("jev_verdict").or_else(|| outcome.get("jev_verdict")),
            }));
        }
    }
    for chunk in payload["legacy_evidence"].as_array().into_iter().flatten() {
        out.push(json!({
            "kind": "legacy_chunk", "atom_id": chunk["atom_id"], "occurred_at": chunk["occurred_at"],
            "jev_verdict": chunk.get("jev_verdict"),
        }));
    }
    out
}

async fn body_json(response: axum::response::Response) -> (u16, Value) {
    let status = response.status().as_u16();
    let bytes = axum::body::to_bytes(response.into_body(), 64 * 1024).await.unwrap_or_default();
    (status, serde_json::from_slice(&bytes).unwrap_or_else(|_| json!({"error": "non-JSON refusal"})))
}

async fn setup(args: &Args) -> Result<(Arc<coralswarm_backend::AppState>, Tenant, UserContext), String> {
    let boot = coralswarm_backend::build_cloud_state().await?;
    let state = boot.state;
    let control = state.db.clone().ok_or("DATABASE_URL is unset: the control plane is required")?;
    let runtime = state.tenant_runtime.clone().ok_or("tenancy runtime disabled (MULTITENANT_ENABLED)")?;
    let routing = runtime
        .resolve_routing(&control, &args.org_slug)
        .await
        .map_err(|e| format!("routing lookup: {e}"))?
        .ok_or_else(|| format!("no tenant named {}", args.org_slug))?;
    if routing.status != "active" {
        return Err(format!("tenant {} is {}", args.org_slug, routing.status));
    }
    let role: Option<String> = sqlx::query_scalar(
        "SELECT m.role FROM account_members m JOIN users u ON u.id=m.user_id \
         WHERE m.account_id=$1 AND m.user_id=$2 AND u.deleted_at IS NULL",
    )
    .bind(routing.account_id)
    .bind(args.user_id)
    .fetch_optional(&control)
    .await
    .map_err(|e| format!("membership lookup: {e}"))?;
    let role = role.ok_or("--user-id is not a current member of this org's account")?;
    let pool = runtime
        .registry
        .get_or_build(&routing.clerk_org_id, routing.account_id, &routing.db_secret_ref)
        .await
        .map_err(|e| format!("tenant pool: {e:?}"))?;
    let user = UserContext { user_id: args.user_id, account_id: routing.account_id, role, device_id: String::new() };
    let store: Arc<dyn Store> = Arc::new(PostgresStore::new(pool.clone()));
    let tenant = Tenant {
        store: Some(store),
        pool: Some(pool.clone()),
        decision: Some(TenancyDecision::Org(TenantContext {
            org_id: routing.clerk_org_id.clone(),
            account_id: routing.account_id,
            user_id: args.user_id,
            pool,
            page_pool: None,
        })),
        class: coralswarm_backend::route_class::RouteClass::Other,
    };
    Ok((state, tenant, user))
}

#[tokio::main]
async fn main() {
    let args = parse_args();
    let input: Value = std::fs::read_to_string(&args.questions)
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or_else(|| usage("--questions is not readable JSON"));
    let questions = input["questions"].as_array().cloned().unwrap_or_else(|| usage("no questions[]"));
    let (state, tenant, user) = match setup(&args).await {
        Ok(v) => v,
        Err(e) => {
            eprintln!("quality_eval_ask: setup failed: {e}");
            std::process::exit(1);
        }
    };
    // Same credit gate `POST /v1/queries` checks before Ask: an account that is
    // out of interactive credit gets no answers here either.
    if let Err(refusal) = coralswarm_backend::billing::enforcement::interactive_admission(
        state.control_db(), user.account_id, user.user_id,
    ).await {
        eprintln!("quality_eval_ask: interactive credit gate refused ({refusal:?}); refusing to run");
        std::process::exit(1);
    }
    let started_at = chrono::Utc::now();
    let mut results = Vec::with_capacity(questions.len());
    for (i, q) in questions.iter().enumerate() {
        let id = q["id"].as_str().unwrap_or("?").to_owned();
        // `graph_ids` (one Library scope over several oceans of one account)
        // or the single `graph_id`.
        let scope_ids: Vec<Uuid> = match q["graph_ids"].as_array() {
            Some(ids) => ids.iter().filter_map(|v| v.as_str()?.parse().ok()).collect(),
            None => q["graph_id"].as_str().and_then(|s| s.parse().ok()).into_iter().collect(),
        };
        let text = q["text"].as_str().unwrap_or_default().to_owned();
        if scope_ids.is_empty() {
            results.push(json!({"id": id, "status": 0, "error": "graph_ids missing"}));
            continue;
        }
        // Deserialized, not a struct literal: `AskRequest` has crate-private
        // fields that default when skipped, exactly as on the HTTP path.
        let request: AskRequest = match serde_json::from_value(json!({
            "text": text, "library_scope": {"graph_ids": scope_ids},
            "style": q["style"].as_str(), "candidate_limit": q["candidate_limit"].as_i64(),
        })) {
            Ok(r) => r,
            Err(e) => {
                results.push(json!({"id": id, "status": 0, "error": format!("request: {e}")}));
                continue;
            }
        };
        let t0 = Instant::now();
        let asked_at = chrono::Utc::now();
        let record = match scoped_ask::execute(&state, &tenant, &user, request).await {
            Ok(answer) => {
                let p = &answer.payload;
                json!({
                    "id": id, "status": 200, "asked_at": asked_at, "wall_ms": t0.elapsed().as_millis() as u64,
                    "query_id": p["query_id"], "conversation_id": p["conversation_id"],
                    "mode": p["mode"], "answer_status": p["answer_status"], "answer": p["text"],
                    "evidence": evidence(p), "jev": p["jev"], "timing": p["timing"],
                    "truncated": p["truncated"], "retrieval_reason": p["retrieval_reason"],
                    "n_outcomes": p["outcomes"].as_array().map_or(0, Vec::len),
                    "n_legacy": p["legacy_evidence"].as_array().map_or(0, Vec::len),
                    "egress_blocked": p["egress_blocked"],
                })
            }
            Err(response) => {
                let (status, body) = body_json(response).await;
                json!({"id": id, "status": status, "asked_at": asked_at, "wall_ms": t0.elapsed().as_millis() as u64, "error": body["error"]})
            }
        };
        eprintln!("quality_eval_ask: [{}/{}] {id} status={} ms={}", i + 1, questions.len(), record["status"], record["wall_ms"]);
        results.push(record);
        tokio::time::sleep(Duration::from_millis(args.sleep_ms)).await;
    }
    let out = json!({
        "meta": {"tool": "quality_eval_ask", "org_slug": args.org_slug, "user_id": args.user_id,
                 "started_at": started_at, "finished_at": chrono::Utc::now(), "n": results.len(),
                 "jev_ask_path_mode": std::env::var("JEV_ASK_PATH_MODE").ok(),
                 "fact_supersession_mode": std::env::var("FACT_SUPERSESSION_MODE").ok(),
                 "digests_enabled": std::env::var("DIGESTS_ENABLED").ok()},
        "results": results,
    });
    if let Err(e) = std::fs::write(&args.out, serde_json::to_vec(&out).expect("json")) {
        eprintln!("quality_eval_ask: write {}: {e}", args.out);
        std::process::exit(1);
    }
    eprintln!("quality_eval_ask: wrote {} results to {}", out["meta"]["n"], args.out);
    let fetched = format!("{}.fetched", args.out);
    let deadline = Instant::now() + Duration::from_secs(args.hold_secs);
    while Instant::now() < deadline && !std::path::Path::new(&fetched).exists() {
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::evidence;
    use serde_json::json;

    /// `[n]` in an answer resolves to retained citations in outcome order, then
    /// legacy chunks (the client's numbering and `jev::numbered`'s). The scorer
    /// maps `[n]` to `evidence()[n-1]`, so the order is load-bearing.
    #[test]
    fn evidence_is_in_client_citation_order_with_verdicts_and_no_text() {
        let payload = json!({
            "outcomes": [
                {"kind": "fact", "source_version_is_current": true,
                 "citations": [{"semantic_atom_id": "a1", "source_item_id": "i1", "quote": "PRIVATE QUOTE", "jev_verdict": "verified"}]},
                {"kind": "decision", "source_version_is_current": true, "supersession": {"status": "superseded"},
                 "citations": [{"semantic_atom_id": "a2", "source_item_id": "i2", "quote": "PRIVATE QUOTE 2"}]},
            ],
            "legacy_evidence": [{"atom_id": "l1", "content": "PRIVATE CHUNK", "jev_verdict": "fabricated"}],
        });
        let ev = evidence(&payload);
        let ids: Vec<_> = ev.iter().map(|e| e["semantic_atom_id"].as_str().or(e["atom_id"].as_str()).unwrap()).collect();
        assert_eq!(ids, ["a1", "a2", "l1"]);
        assert_eq!(ev[0]["jev_verdict"], "verified");
        assert_eq!(ev[1]["superseded"], true);
        assert_eq!(ev[0]["superseded"], false);
        assert_eq!(ev[2]["jev_verdict"], "fabricated");
        let text = serde_json::to_string(&ev).unwrap();
        assert!(!text.contains("PRIVATE"), "evidence() must not carry quotes or chunk bodies: {text}");
    }
}
