//! RLS probes for the org-tenancy OPS-4 MCP-surface conversion
//! (`mcp/tools/{read,write}.rs`).
//!
//! Two layers of proof, both against a REAL `coralswarm_app` login (the
//! non-BYPASSRLS role OPS-4 cuts the app over to), via the shared
//! `tests/support/rls_probe.rs` harness — same two-layer shape
//! `routes_b2_rls_pg.rs` established for the route conversion:
//!
//! 1. **Query-shape probes** — every RLS-covered TABLE the converted tools now
//!    reach through a `_scoped` Store twin (`nodes`, `node_atoms`, `atoms`,
//!    `graphs`, `activity_events`, `meeting_transcripts`,
//!    `transcript_corrections`, `coding_sessions`) gets the harness's three
//!    standard proofs: scoped-to-A returns exactly A's rows; scoped-to-A aimed
//!    at B's ids returns NOTHING; and the same query with `app.account_id`
//!    never set HARD-ERRORS (0083's strict `current_setting` predicate).
//!
//! 2. **Tool-level probes** — the REAL `/mcp` JSON-RPC surface
//!    (`test_support_app` + rmcp's streamable-HTTP transport + genuine RS256
//!    token verification against a local JWKS listener, exactly as
//!    `mcp_pg_e2e.rs` drives it) served ENTIRELY off the `coralswarm_app`
//!    pool. This proves the converted tool bodies (a) still WORK once every
//!    RLS-table query must run inside `begin_account_scoped` — an unconverted
//!    query path hard-errors here rather than serving — and (b) fail closed
//!    when caller A's ARGUMENTS reference account B's ids, which is the actual
//!    security property this conversion is about: a `get_reef`/`get_atom`/
//!    `update_reef`/`confirm_reef` call authenticated as A must never read or
//!    write B's rows, no matter what ids it is handed.
//!
//! Env-gated on `ORGT_TEST_BASE_URL`, like every `*_pg.rs` here (and wired
//! into CI's `rls-probe-harness` job so these asserts actually execute):
//!
//!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test mcp_rls_pg
//!
//! ## Not covered here, by design
//! `ask_ocean` / `search_atoms` / `get_transcript` are thin pass-throughs to
//! the SERVICES layer (`services::ask`, `services::transcripts`), which is its
//! own conversion unit — see `mcp/tools/read.rs`'s module doc. `add_context`
//! delegates to `ingest::submit`, converted and proven in
//! phase B.1 (`route_rls_b1_pg.rs`). Probing them here would be asserting on
//! another lane's (currently unconverted) code.

mod support;

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use chrono::Utc;
use coralswarm_backend::clerk::test_support::{mint_oauth, test_jwks, ISSUER};
use coralswarm_backend::store::PostgresStore;
use serde_json::{json, Value};
use sqlx::PgPool;
use std::sync::Arc;
use support::rls_probe::RlsProbe;
use tower::ServiceExt;
use uuid::Uuid;

// ── Seeding helpers (owner/BYPASSRLS pool — seeding must bypass the very
//    policies these tests then probe) ────────────────────────────────────────

/// A pending decision reef, the shape `confirm_reef`/`reject_reef` operate on:
/// `type = 'decision'`, `body.review_status = 'pending'`, and `body.decided_at`
/// set to satisfy the `decisions_require_decided_at` CHECK.
async fn seed_pending_decision(owner: &PgPool, graph_id: Uuid, atom_id: Uuid, title: &str) -> Uuid {
    let body = json!({ "review_status": "pending", "decided_at": Utc::now().to_rfc3339() });
    let node_id: Uuid = sqlx::query_scalar(
        "INSERT INTO nodes(graph_id, type, title, body) VALUES ($1, 'decision', $2, $3) \
         RETURNING node_id",
    )
    .bind(graph_id)
    .bind(title)
    .bind(body)
    .fetch_one(owner)
    .await
    .expect("insert pending decision");
    // Keep the fixture's one real coral, but give it literal synthetic context
    // for both the existing Probe Node and this pending decision before linking.
    let updated = sqlx::query("UPDATE atoms SET content_inline=concat_ws(chr(10),content_inline,$3) WHERE atom_id=$1 AND graph_id=$2")
        .bind(atom_id).bind(graph_id)
        .bind(format!("Probe Node. Synthetic pending decision: {title}."))
        .execute(owner).await.expect("record same-graph decision support").rows_affected();
    assert_eq!(updated, 1, "decision support must be the existing coral in this graph");
    link_reef_to_coral(owner, node_id, atom_id).await;
    let admitted: (bool, Option<String>) = sqlx::query_as("SELECT library_node_permitted($1,$2),library_node_title($1,$2)")
        .bind(graph_id).bind(node_id).fetch_one(owner).await.unwrap();
    assert_eq!(admitted, (true, Some(title.to_owned())), "the pending decision needs genuinely permitted literal support");
    node_id
}

/// Link the fixture reef to the fixture coral, so `get_reef`'s
/// `reef_atoms_scoped` (a `node_atoms` → `atoms` join) has a row to find.
async fn link_reef_to_coral(owner: &PgPool, node_id: Uuid, atom_id: Uuid) {
    sqlx::query("INSERT INTO node_atoms(node_id, atom_id) VALUES ($1, $2)")
        .bind(node_id)
        .bind(atom_id)
        .execute(owner)
        .await
        .expect("link reef to coral");
}

/// One meeting transcript + one correction row for it — `list_meetings` /
/// `get_meeting`'s tables.
async fn seed_meeting(owner: &PgPool, graph_id: Uuid, atom_id: Uuid, source_id: &str) -> Uuid {
    let id: Uuid = sqlx::query_scalar(
        "INSERT INTO meeting_transcripts(graph_id, source_id, raw_inline, duration_ms) \
         VALUES ($1, $2, 'probe transcript text', 1000) RETURNING id",
    )
    .bind(graph_id)
    .bind(source_id)
    .fetch_one(owner)
    .await
    .expect("insert meeting transcript");
    sqlx::query(
        "INSERT INTO transcript_corrections(graph_id, atom_id, source_id, surface_form, canonical) \
         VALUES ($1, $2, $3, 'Jordan', 'Morgan')",
    )
    .bind(graph_id)
    .bind(atom_id)
    .bind(source_id)
    .execute(owner)
    .await
    .expect("insert transcript correction");
    id
}

/// One coding session — `list_sessions`' table.
async fn seed_coding_session(owner: &PgPool, graph_id: Uuid, session_id: &str) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO coding_sessions(graph_id, session_id, project_key, title) \
         VALUES ($1, $2, 'probe-project', 'Probe Session') RETURNING id",
    )
    .bind(graph_id)
    .bind(session_id)
    .fetch_one(owner)
    .await
    .expect("insert coding session")
}

// ── 1. Query-shape probes ────────────────────────────────────────────────────

#[tokio::test]
async fn mcp_tool_query_shapes_are_account_scoped() {
    let Some(probe) = RlsProbe::setup("orgt_rls_mcp_shapes").await else {
        eprintln!("skipping mcp_tool_query_shapes_are_account_scoped: set ORGT_TEST_BASE_URL");
        return;
    };

    link_reef_to_coral(&probe.owner, probe.a.node_id, probe.a.atom_id).await;
    link_reef_to_coral(&probe.owner, probe.b.node_id, probe.b.atom_id).await;
    let a_meeting = seed_meeting(&probe.owner, probe.a.graph_id, probe.a.atom_id, "probe-a").await;
    let b_meeting = seed_meeting(&probe.owner, probe.b.graph_id, probe.b.atom_id, "probe-b").await;
    let a_session = seed_coding_session(&probe.owner, probe.a.graph_id, "sess-a").await;
    let b_session = seed_coding_session(&probe.owner, probe.b.graph_id, "sess-b").await;
    let _ = (b_meeting, b_session);

    // Each entry is (label, query, A's binds, A's expected rows, B's binds).
    // `query` selects a single uuid column, the harness's contract.
    struct Shape<'a> {
        label: &'a str,
        query: &'a str,
        a_binds: Vec<Uuid>,
        a_expected: Vec<Uuid>,
        b_binds: Vec<Uuid>,
    }

    let shapes = vec![
        // `list_nodes_scoped` (list_reefs) — graph-filtered `nodes`.
        Shape {
            label: "list_reefs / list_nodes_scoped",
            query: "SELECT n.node_id FROM nodes n WHERE n.graph_id = $1",
            a_binds: vec![probe.a.graph_id],
            a_expected: vec![probe.a.node_id],
            b_binds: vec![probe.b.graph_id],
        },
        // `node_graph_id_scoped` / `get_node_scoped` / `node_version_scoped`
        // (get_reef, update_reef) — id-filtered `nodes`.
        Shape {
            label: "get_reef / node_graph_id_scoped",
            query: "SELECT graph_id FROM nodes WHERE node_id = $1",
            a_binds: vec![probe.a.node_id],
            a_expected: vec![probe.a.graph_id],
            b_binds: vec![probe.b.node_id],
        },
        // `node_owned_by_account_scoped` (update_reef) — the `nodes` → `graphs`
        // ownership join. Both tables are RLS-covered.
        Shape {
            label: "update_reef / node_owned_by_account_scoped",
            query: "SELECT n.node_id FROM nodes n JOIN graphs g ON g.id = n.graph_id \
                    WHERE n.node_id = $1 AND g.account_id = $2",
            a_binds: vec![probe.a.node_id, probe.a.account_id],
            a_expected: vec![probe.a.node_id],
            b_binds: vec![probe.b.node_id, probe.b.account_id],
        },
        // `reef_atoms_scoped` (get_reef) — the `node_atoms` → `atoms` join.
        Shape {
            label: "get_reef / reef_atoms_scoped",
            query: "SELECT a.atom_id FROM node_atoms na JOIN atoms a ON a.atom_id = na.atom_id \
                    WHERE na.node_id = $1",
            a_binds: vec![probe.a.node_id],
            a_expected: vec![probe.a.atom_id],
            b_binds: vec![probe.b.node_id],
        },
        // `atom_graph_id_scoped` / `atom_detail_scoped` (get_atom).
        Shape {
            label: "get_atom / atom_graph_id_scoped",
            query: "SELECT graph_id FROM atoms WHERE atom_id = $1",
            a_binds: vec![probe.a.atom_id],
            a_expected: vec![probe.a.graph_id],
            b_binds: vec![probe.b.atom_id],
        },
        // `list_activity_scoped` (recent_activity).
        Shape {
            label: "recent_activity / list_activity_scoped",
            query: "SELECT id FROM activity_events WHERE account_id = $1",
            a_binds: vec![probe.a.account_id],
            // `RlsProbe` seeds exactly one activity event per account, and its
            // id isn't returned by the fixture — assert via the cross-account
            // and unscoped arms plus a non-empty check below instead.
            a_expected: Vec::new(),
            b_binds: vec![probe.b.account_id],
        },
        // `list_meeting_transcripts_scoped` / `get_meeting_transcript_scoped`.
        Shape {
            label: "list_meetings / list_meeting_transcripts_scoped",
            query: "SELECT id FROM meeting_transcripts WHERE graph_id = $1",
            a_binds: vec![probe.a.graph_id],
            a_expected: vec![a_meeting],
            b_binds: vec![probe.b.graph_id],
        },
        // `list_transcript_corrections_scoped` (get_meeting).
        Shape {
            label: "get_meeting / list_transcript_corrections_scoped",
            query: "SELECT correction_id FROM transcript_corrections \
                    WHERE graph_id = $1 AND source_id = 'probe-a'",
            a_binds: vec![probe.a.graph_id],
            a_expected: Vec::new(), // id not captured; see the non-empty check below
            b_binds: vec![probe.b.graph_id],
        },
        // `list_coding_sessions_scoped` (list_sessions).
        Shape {
            label: "list_sessions / list_coding_sessions_scoped",
            query: "SELECT id FROM coding_sessions WHERE graph_id = $1",
            a_binds: vec![probe.a.graph_id],
            a_expected: vec![a_session],
            b_binds: vec![probe.b.graph_id],
        },
        // `ocean_stats_scoped` (get_ocean_stats) — its per-sub-query scoped
        // transactions run this `atoms` count shape.
        Shape {
            label: "get_ocean_stats / ocean_stats_scoped",
            query: "SELECT atom_id FROM atoms WHERE graph_id = $1",
            a_binds: vec![probe.a.graph_id],
            a_expected: vec![probe.a.atom_id],
            b_binds: vec![probe.b.graph_id],
        },
    ];

    for shape in &shapes {
        if !shape.a_expected.is_empty() {
            probe
                .assert_scoped_returns_own_rows(
                    probe.a.account_id,
                    shape.query,
                    &shape.a_binds,
                    &shape.a_expected,
                )
                .await;
        }
        probe
            .assert_cross_account_returns_nothing(probe.a.account_id, shape.query, &shape.b_binds)
            .await;
        probe.assert_unscoped_hard_errors(shape.query, &shape.a_binds).await;
        eprintln!("shape probe ok: {}", shape.label);
    }

    // The two shapes whose fixture ids aren't returned by the seeder still get
    // a positive proof: scoped to A they must return SOMETHING (not merely
    // "nothing, same as the cross-account case").
    for (label, query, binds) in [
        (
            "recent_activity / list_activity_scoped",
            "SELECT id FROM activity_events WHERE account_id = $1",
            vec![probe.a.account_id],
        ),
        (
            "get_meeting / list_transcript_corrections_scoped",
            "SELECT correction_id FROM transcript_corrections WHERE graph_id = $1 \
             AND source_id = 'probe-a'",
            vec![probe.a.graph_id],
        ),
    ] {
        let mut tx = probe.scoped_tx(probe.a.account_id).await;
        let mut q = sqlx::query_scalar::<sqlx::Postgres, Uuid>(query);
        for b in &binds {
            q = q.bind(*b);
        }
        let rows = q
            .fetch_all(&mut *tx)
            .await
            .unwrap_or_else(|e| panic!("scoped query for {label} failed: {e}"));
        assert!(!rows.is_empty(), "scoped to A, {label} must return A's own row(s)");
        tx.rollback().await.ok();
    }

    probe.teardown().await;
}

// ── 2. Tool-level probes over the real `/mcp` transport ──────────────────────

fn future_exp() -> usize {
    (Utc::now().timestamp() + 3600) as usize
}

/// Local JWKS listener serving the shared test keypair (same fixture as
/// `mcp_pg_e2e.rs` / `routes_b2_rls_pg.rs` — duplicated for the same
/// separate-compilation-unit reason).
async fn spawn_local_jwks() -> String {
    let jwks_body = test_jwks();
    let app = axum::Router::new().route(
        "/.well-known/jwks.json",
        axum::routing::get(move || {
            let body = jwks_body.clone();
            async move { ([("content-type", "application/json")], body) }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind local JWKS listener");
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.ok();
    });
    format!("http://{addr}/.well-known/jwks.json")
}

/// Parse the SSE body a stateless-mode `/mcp` POST returns (`data: <json>`)
/// back into the JSON-RPC message it carries.
fn parse_sse_json(bytes: &[u8]) -> Value {
    let text = String::from_utf8_lossy(bytes);
    let mut data_lines = Vec::new();
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("data:") {
            data_lines.push(rest.strip_prefix(' ').unwrap_or(rest));
        }
    }
    serde_json::from_str(&data_lines.join("\n"))
        .unwrap_or_else(|e| panic!("failed to parse SSE JSON-RPC payload: {e}\nraw: {text}"))
}

/// Call one MCP tool. Returns `(is_error, body)` where `body` is the tool's own
/// `{"error":{"code","message"}}` or success JSON.
async fn call_tool(app: &axum::Router, token: &str, name: &str, arguments: Value) -> (bool, Value) {
    let body = json!({
        "jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": { "name": name, "arguments": arguments },
    });
    let req = Request::builder()
        .method("POST")
        .uri("/mcp")
        // `tower::oneshot` synthesizes no `Host`; rmcp's DNS-rebinding guard
        // requires one (see `mcp_pg_e2e.rs`'s own note).
        .header("host", "localhost")
        .header("content-type", "application/json")
        .header("accept", "application/json, text/event-stream")
        .header("Authorization", format!("Bearer {token}"))
        .body(Body::from(body.to_string()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let content_type = resp
        .headers()
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let bytes = to_bytes(resp.into_body(), 16 * 1024 * 1024).await.unwrap();
    let rpc = if content_type.contains("text/event-stream") {
        parse_sse_json(&bytes)
    } else {
        serde_json::from_slice(&bytes).unwrap_or(Value::Null)
    };
    assert_eq!(status, StatusCode::OK, "tools/call transport failure for {name}: {rpc:?}");
    let result = rpc
        .get("result")
        .unwrap_or_else(|| panic!("no result for {name}: {rpc:?}"));
    let is_error = result.get("isError").and_then(|v| v.as_bool()).unwrap_or(false);
    let content = result["content"][0]["text"]
        .as_str()
        .unwrap_or_else(|| panic!("no text content for {name}: {result:?}"));
    (is_error, serde_json::from_str(content).expect("tool payload is JSON"))
}

fn error_code(body: &Value) -> &str {
    body["error"]["code"].as_str().unwrap_or_default()
}

struct McpEnv;

impl Drop for McpEnv {
    fn drop(&mut self) {
        // SAFETY: this binary's only env-mutating test (see its comment).
        unsafe {
            std::env::remove_var("MCP_ENABLED");
            std::env::remove_var("CLERK_ISSUER");
            std::env::remove_var("CLERK_JWKS_URL");
            std::env::remove_var("MCP_RESOURCE_URL");
        }
    }
}

#[tokio::test]
async fn mcp_tools_work_and_isolate_as_coralswarm_app() {
    let Some(probe) = RlsProbe::setup("orgt_rls_mcp_tools").await else {
        eprintln!("skipping mcp_tools_work_and_isolate_as_coralswarm_app: set ORGT_TEST_BASE_URL");
        return;
    };

    // ── Env. This binary's only env-mutating test, so no cross-test lock is
    // needed (each test binary is its own process, and the shape-probe test
    // above touches no env). ────────────────────────────────────────────────
    let jwks_url = spawn_local_jwks().await;
    // SAFETY: see the comment above.
    unsafe {
        std::env::set_var("MCP_ENABLED", "1");
        std::env::set_var("CLERK_ISSUER", ISSUER);
        std::env::set_var("CLERK_JWKS_URL", &jwks_url);
        std::env::set_var("MCP_RESOURCE_URL", "https://mcp-rls-probe.test/mcp");
    }
    let _env = McpEnv;

    // ── Wire the harness fixtures to Clerk-shaped identities so `mcp_auth`'s
    // provisioning resolves to EXACTLY the probe's seeded users/accounts (no
    // new rows). The `account_members` rows are what the SECURITY DEFINER
    // reachability resolver consults. Same setup as `routes_b2_rls_pg.rs`. ──
    let sub_a = "mcp_rls_probe_user_a";
    let sub_b = "mcp_rls_probe_user_b";
    for (sub, fx) in [(sub_a, &probe.a), (sub_b, &probe.b)] {
        sqlx::query("UPDATE users SET clerk_user_id = $1 WHERE id = $2")
            .bind(sub)
            .bind(fx.user_id)
            .execute(&probe.owner)
            .await
            .expect("stamp clerk_user_id");
        sqlx::query(
            "INSERT INTO account_members(account_id, user_id, role) VALUES ($1, $2, 'owner')",
        )
        .bind(fx.account_id)
        .bind(fx.user_id)
        .execute(&probe.owner)
        .await
        .expect("seed account_members");
    }
    // The fixture graphs are non-primary by default; `list_reefs` et al. only
    // default to a primary ocean when `ocean_id` is omitted, and every call
    // below passes one explicitly, so no `is_primary` flip is needed.

    link_reef_to_coral(&probe.owner, probe.a.node_id, probe.a.atom_id).await;
    link_reef_to_coral(&probe.owner, probe.b.node_id, probe.b.atom_id).await;
    seed_meeting(&probe.owner, probe.a.graph_id, probe.a.atom_id, "probe-a").await;
    seed_meeting(&probe.owner, probe.b.graph_id, probe.b.atom_id, "probe-b").await;
    seed_coding_session(&probe.owner, probe.a.graph_id, "sess-a").await;
    seed_coding_session(&probe.owner, probe.b.graph_id, "sess-b").await;
    let a_decision = seed_pending_decision(&probe.owner, probe.a.graph_id, probe.a.atom_id, "A decides").await;
    let b_decision = seed_pending_decision(&probe.owner, probe.b.graph_id, probe.b.atom_id, "B decides").await;

    // ── AppState served ENTIRELY off the coralswarm_app pool: every content
    // query a converted tool runs is now subject to 0083's policies, so an
    // unconverted (unscoped) path hard-errors instead of serving. ───────────
    let mut state = (*coralswarm_backend::test_app_state()).clone();
    state.db = Some(probe.app_pool.clone());
    state.store = Some(Arc::new(PostgresStore::new(probe.app_pool.clone())));
    let app = coralswarm_backend::test_support_app(Arc::new(state));

    // OAuth-shaped claims (`scope`/`client_id`, no `sid`) — `/mcp` rejects a
    // plain Clerk session JWT outright.
    let token_a = mint_oauth(json!({
        "aud": std::env::var("MCP_RESOURCE_URL").expect("configured MCP test resource"),
        "iss": ISSUER, "sub": sub_a, "exp": future_exp(),
        "scope": "coralswarm:read coralswarm:write",
        "client_id": "mcp_rls_probe_client",
    }));

    // ── list_oceans: A sees A's ocean and never B's ──────────────────────────
    let (is_err, body) = call_tool(&app, &token_a, "list_oceans", json!({})).await;
    assert!(!is_err, "list_oceans must succeed under RLS: {body:?}");
    let ids: Vec<&str> = body["oceans"]
        .as_array()
        .expect("oceans array")
        .iter()
        .filter_map(|o| o["id"].as_str())
        .collect();
    assert!(
        ids.contains(&probe.a.graph_id.to_string().as_str()),
        "A must see its own ocean: {ids:?}"
    );
    assert!(
        !ids.contains(&probe.b.graph_id.to_string().as_str()),
        "A must never see B's ocean: {ids:?}"
    );

    // ── Ocean-anchored read tools: own ocean serves, B's ocean 404s ──────────
    let (is_err, body) =
        call_tool(&app, &token_a, "list_reefs", json!({"ocean_id": probe.a.graph_id})).await;
    assert!(!is_err, "list_reefs on A's own ocean must succeed: {body:?}");
    let reef_ids: Vec<&str> = body["reefs"]
        .as_array()
        .expect("reefs array")
        .iter()
        .filter_map(|r| r["reef_id"].as_str())
        .collect();
    assert!(
        reef_ids.contains(&probe.a.node_id.to_string().as_str()),
        "A's list_reefs must contain A's reef: {reef_ids:?}"
    );
    assert!(
        !reef_ids.contains(&probe.b.node_id.to_string().as_str()),
        "A's list_reefs must never contain B's reef: {reef_ids:?}"
    );

    for tool in ["list_reefs", "list_meetings", "get_ocean_stats", "list_sessions"] {
        let (is_err, body) =
            call_tool(&app, &token_a, tool, json!({"ocean_id": probe.b.graph_id})).await;
        assert!(is_err, "{tool} aimed at B's ocean must be denied: {body:?}");
        assert_eq!(error_code(&body), "not_found", "{tool} denial must not leak: {body:?}");
    }

    for tool in ["list_meetings", "get_ocean_stats", "list_sessions"] {
        let (is_err, body) =
            call_tool(&app, &token_a, tool, json!({"ocean_id": probe.a.graph_id})).await;
        assert!(!is_err, "{tool} on A's own ocean must succeed under RLS: {body:?}");
    }

    // `get_ocean_stats` (the `ocean_stats_scoped` fan-out) must see A's real
    // counts, not a silently-empty aggregate.
    let (_, stats) =
        call_tool(&app, &token_a, "get_ocean_stats", json!({"ocean_id": probe.a.graph_id})).await;
    assert_eq!(stats["atom_count"], 1, "A's ocean stats must count A's coral: {stats:?}");
    assert_eq!(stats["reef_total"], 2, "A's ocean stats must count A's reefs: {stats:?}");

    // ── get_meeting: A's meeting serves; B's meeting id under A's ocean 404s ─
    let (is_err, body) = call_tool(
        &app,
        &token_a,
        "get_meeting",
        json!({"ocean_id": probe.a.graph_id, "meeting_id": "probe-a"}),
    )
    .await;
    assert!(!is_err, "get_meeting on A's own meeting must succeed: {body:?}");
    assert_eq!(body["meeting_id"], "probe-a");
    let (is_err, body) = call_tool(
        &app,
        &token_a,
        "get_meeting",
        json!({"ocean_id": probe.b.graph_id, "meeting_id": "probe-b"}),
    )
    .await;
    assert!(is_err, "get_meeting aimed at B's meeting must be denied: {body:?}");
    assert_eq!(error_code(&body), "not_found");

    // ── get_reef / get_atom: the id-anchored cross-account property ──────────
    // Two stated edges from A's reef: one carrying extraction v2's validity
    // (#1339), one stating none.
    let dated: Uuid = sqlx::query_scalar(
        "INSERT INTO relationship_edge(graph_id,src_node_id,predicate,dst_node_id,origin,status,confidence,provenance,valid_from,valid_until) \
         VALUES($1,$2,'responsible_for',$3,'stated','confirmed',0.9,'{}','2026-03-01T00:00:00Z','2026-12-31T00:00:00Z') RETURNING edge_id",
    )
    .bind(probe.a.graph_id).bind(probe.a.node_id).bind(a_decision)
    .fetch_one(&probe.owner).await.expect("seed dated edge");
    let undated: Uuid = sqlx::query_scalar(
        "INSERT INTO relationship_edge(graph_id,src_node_id,predicate,dst_node_id,origin,status,confidence,provenance) \
         VALUES($1,$2,'works_on',$3,'stated','confirmed',0.9,'{}') RETURNING edge_id",
    )
    .bind(probe.a.graph_id).bind(probe.a.node_id).bind(a_decision)
    .fetch_one(&probe.owner).await.expect("seed undated edge");
    let (is_err, body) =
        call_tool(&app, &token_a, "get_reef", json!({"reef_id": probe.a.node_id})).await;
    assert!(!is_err, "get_reef on A's own reef must succeed under RLS: {body:?}");
    assert_eq!(body["reef_id"], probe.a.node_id.to_string());
    let relationships = body["relationships"].as_array().expect("relationships array");
    let by_id = |id: Uuid| relationships.iter().find(|r| r["edge_id"] == id.to_string()).unwrap_or_else(|| panic!("edge {id} missing: {body:?}"));
    let d = by_id(dated);
    assert_eq!((&d["valid_from"], &d["valid_until"]), (&json!("2026-03-01T00:00:00Z"), &json!("2026-12-31T00:00:00Z")), "{d:?}");
    let u = by_id(undated);
    assert!(u.get("valid_from").is_none() && u.get("valid_until").is_none(), "an undated edge carries no validity keys: {u:?}");
    let corals = body["corals"].as_array().expect("corals array");
    assert_eq!(corals.len(), 1, "get_reef must resolve A's linked coral: {body:?}");
    assert_eq!(corals[0]["atom_id"], probe.a.atom_id.to_string());

    let (is_err, body) =
        call_tool(&app, &token_a, "get_reef", json!({"reef_id": probe.b.node_id})).await;
    assert!(is_err, "get_reef aimed at B's reef must be denied: {body:?}");
    assert_eq!(error_code(&body), "not_found");

    let (is_err, body) =
        call_tool(&app, &token_a, "get_atom", json!({"atom_id": probe.a.atom_id})).await;
    assert!(!is_err, "get_atom on A's own coral must succeed under RLS: {body:?}");
    assert_eq!(body["atom_id"], probe.a.atom_id.to_string());

    let (is_err, body) =
        call_tool(&app, &token_a, "get_atom", json!({"atom_id": probe.b.atom_id})).await;
    assert!(is_err, "get_atom aimed at B's coral must be denied: {body:?}");
    assert_eq!(error_code(&body), "not_found");

    // ── recent_activity: A's own events only ────────────────────────────────
    let (is_err, body) = call_tool(&app, &token_a, "recent_activity", json!({})).await;
    assert!(!is_err, "recent_activity must succeed under RLS: {body:?}");
    let events = body["events"].as_array().expect("events array");
    assert_eq!(events.len(), 1, "A must see exactly its own seeded event: {body:?}");
    let (is_err, body) =
        call_tool(&app, &token_a, "recent_activity", json!({"ocean_id": probe.b.graph_id})).await;
    assert!(is_err, "recent_activity aimed at B's ocean must be denied: {body:?}");
    assert_eq!(error_code(&body), "not_found");

    // ── Write tools: A's own reef commits; B's reef is refused AND untouched ─
    let (is_err, body) = call_tool(
        &app,
        &token_a,
        "update_reef",
        json!({"reef_id": probe.a.node_id, "title": "Renamed Under RLS"}),
    )
    .await;
    assert!(!is_err, "update_reef on A's own reef must succeed under RLS: {body:?}");
    assert_eq!(body["after"]["title"], "Renamed Under RLS");
    let persisted: String = sqlx::query_scalar("SELECT title FROM nodes WHERE node_id = $1")
        .bind(probe.a.node_id)
        .fetch_one(&probe.owner)
        .await
        .expect("read A's reef title back");
    assert_eq!(
        persisted, "Renamed Under RLS",
        "the scoped write transaction must actually COMMIT"
    );

    let (is_err, body) = call_tool(
        &app,
        &token_a,
        "update_reef",
        json!({"reef_id": probe.b.node_id, "title": "Hijacked"}),
    )
    .await;
    assert!(is_err, "update_reef aimed at B's reef must be denied: {body:?}");
    assert_eq!(error_code(&body), "not_found");
    let b_title: String = sqlx::query_scalar("SELECT title FROM nodes WHERE node_id = $1")
        .bind(probe.b.node_id)
        .fetch_one(&probe.owner)
        .await
        .expect("read B's reef title back");
    // `RlsProbe::seed_account` titles both accounts' fixture reefs "Probe Node".
    assert_eq!(b_title, "Probe Node", "B's reef must be byte-for-byte untouched");

    // confirm_reef: A's own pending decision flips and commits.
    let (is_err, body) = call_tool(
        &app,
        &token_a,
        "confirm_reef",
        json!({"reef_id": a_decision, "rationale": "scoped commit"}),
    )
    .await;
    assert!(!is_err, "confirm_reef on A's own decision must succeed under RLS: {body:?}");
    assert_eq!(body["review_status"], "confirmed");
    let a_status: Option<String> =
        sqlx::query_scalar("SELECT body->>'review_status' FROM nodes WHERE node_id = $1")
            .bind(a_decision)
            .fetch_one(&probe.owner)
            .await
            .expect("read A's decision status back");
    assert_eq!(a_status.as_deref(), Some("confirmed"), "the confirm must have COMMITTED");

    // confirm_reef / reject_reef aimed at B's decision: refused, and B's row
    // stays `pending` — the write-side cross-account property.
    for tool in ["confirm_reef", "reject_reef"] {
        let (is_err, body) = call_tool(&app, &token_a, tool, json!({"reef_id": b_decision})).await;
        assert!(is_err, "{tool} aimed at B's decision must be denied: {body:?}");
        assert_eq!(error_code(&body), "not_found", "{tool} denial must not leak: {body:?}");
    }
    let b_status: Option<String> =
        sqlx::query_scalar("SELECT body->>'review_status' FROM nodes WHERE node_id = $1")
            .bind(b_decision)
            .fetch_one(&probe.owner)
            .await
            .expect("read B's decision status back");
    assert_eq!(
        b_status.as_deref(),
        Some("pending"),
        "B's decision must be left exactly as seeded"
    );

    probe.teardown().await;
}
