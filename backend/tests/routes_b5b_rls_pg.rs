//! RLS probes for the org-tenancy OPS-4 phase-B.5b conversion:
//! `routes/{sessions,session_shares,teams}.rs` plus migration
//! `0087_session_share_bootstrap_functions`.
//!
//! Three layers of proof, all against a REAL `coralswarm_app` login (the
//! non-BYPASSRLS role OPS-4 cuts the app over to), via the shared
//! `tests/support/rls_probe.rs` harness (PR #286):
//!
//! 1. **Bootstrap-function proofs** — 0087's two SECURITY DEFINER resolvers
//!    answer from an UNSCOPED `coralswarm_app` connection (that is their whole
//!    purpose: breaking the "you cannot scope to an account you have not
//!    discovered yet" cycle for a token-authenticated ANONYMOUS route), return
//!    bare ids only, and return NULL for the cases each call site relies on
//!    (revoked token, unknown token) so their generic 404 stays
//!    indistinguishable.
//!
//! 2. **Query-shape probes** — the exact table/filter shapes the converted
//!    `_scoped` Store calls execute get the harness's three standard proofs:
//!    scoped-to-A returns exactly A's rows; scoped-to-A aimed at B's ids
//!    returns NOTHING; the same query with `app.account_id` never set
//!    HARD-ERRORS. Plus scoped-WRITE probes: a forged cross-account INSERT is
//!    refused with SQLSTATE **42501** (the RLS `WITH CHECK` refusal — asserted
//!    specifically so a `23503` foreign-key error cannot pass for one).
//!
//! 3. **Route-level probes** — the real public router (`test_support_app`,
//!    real Clerk-shaped JWTs against a local JWKS listener) served entirely
//!    off the `coralswarm_app` pool, including anonymous whole-original
//!    refusal and historical-share revocation. An unconverted query path inside these handlers
//!    hard-errors here instead of serving.
//!
//! Env-gated, like every `*_pg.rs` here:
//!
//!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test routes_b5b_rls_pg

mod support;

use axum::body::Body;
use axum::http::{Request as HttpRequest, StatusCode};
use coralswarm_backend::clerk::test_support::{mint, test_jwks, ISSUER};
use coralswarm_backend::store::PostgresStore;
use serde_json::{json, Value};
use sqlx::PgPool;
use std::sync::Arc;
use support::rls_probe::RlsProbe;
use tower::ServiceExt;
use uuid::Uuid;

// ── Shared seeding helpers (owner/BYPASSRLS pool — seeding must bypass the
//    policies the tests then probe) ─────────────────────────────────────────

async fn seed_coding_session(owner: &PgPool, graph_id: Uuid, session_id: &str, project: &str) {
    // `projects` carries the FK target for `coding_sessions.project_key`'s
    // (graph_id, project_key) pair on some schema revisions; insert it first
    // and let a duplicate be a no-op.
    sqlx::query(
        "INSERT INTO projects(graph_id, project_key) VALUES ($1, $2) ON CONFLICT DO NOTHING",
    )
    .bind(graph_id)
    .bind(project)
    .execute(owner)
    .await
    .expect("insert project");
    sqlx::query(
        "INSERT INTO coding_sessions(graph_id, session_id, project_key, title) \
         VALUES ($1, $2, $3, 'probe session')",
    )
    .bind(graph_id)
    .bind(session_id)
    .bind(project)
    .execute(owner)
    .await
    .expect("insert coding session");
}

async fn seed_session_share(
    owner: &PgPool,
    graph_id: Uuid,
    session_id: &str,
    token: &str,
    created_by: Uuid,
) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO coding_session_shares(graph_id, session_id, token, visibility, created_by) \
         VALUES ($1, $2, $3, 'link', $4) RETURNING id",
    )
    .bind(graph_id)
    .bind(session_id)
    .bind(token)
    .bind(created_by)
    .fetch_one(owner)
    .await
    .expect("insert coding session share")
}

/// One coral belonging to `session_id`, following the `"cc:{session}:"`
/// `source_id` convention `Store::list_shared_session_corals` matches on.
async fn seed_session_coral(owner: &PgPool, graph_id: Uuid, session_id: &str, body: &str) -> Uuid {
    let atom_id: Uuid = sqlx::query_scalar(
        "INSERT INTO atoms(graph_id, source_type, source_id, content_blob_key, content_inline, \
                           timestamp) \
         VALUES ($1, 'coding_session', $2, 'blob-key', $3, now()) RETURNING atom_id",
    )
    .bind(graph_id)
    .bind(format!("cc:{session_id}:1"))
    .bind(body)
    .fetch_one(owner)
    .await
    .expect("insert session coral");
    // The list projects immutable permitted captures, not mutable session rows
    // or an unverified cc: prefix alone. Retain this exact synthetic body and
    // bind only the newly inserted, content-verified coral to its version.
    let (account, actor, kind, project, title): (Uuid, Uuid, String, Option<String>, Option<String>) =
        sqlx::query_as("SELECT g.account_id,a.owner_user_id,s.session_kind,s.project_key,s.title FROM graphs g JOIN accounts a ON a.id=g.account_id JOIN coding_sessions s ON s.graph_id=g.id WHERE g.id=$1 AND s.session_id=$2")
            .bind(graph_id).bind(session_id).fetch_one(owner).await.unwrap();
    let metadata = json!({"session_id":session_id,"capture_context":{
        "revision":1,"session_id":session_id,"session_kind":kind,
        "session_title":title,"project_key":project,"root_session_id":session_id
    }});
    let mut tx = coralswarm_backend::tenancy::scope::begin_account_scoped(owner, account).await.unwrap();
    coralswarm_backend::capture_context::lock_session_tx(&mut tx, graph_id, session_id).await.unwrap();
    let captured = coralswarm_backend::source_evidence::capture_source_version_tx(
        &mut tx, None, graph_id, account,
        &coralswarm_backend::source_evidence::CaptureSource {
            source_type: "coding_session", provider_namespace: "fixture:b5b",
            source_id: &format!("cc:{session_id}:1"), normalized_text: body,
            representation: "original_text", metadata: &metadata, occurred_at: None,
            participants: &[], ingested_by: Some(actor), scope: "private",
            duration_ms: None, segments: None,
        }, None, &[7; 32],
    ).await.expect("retain genuine session source and immutable context");
    coralswarm_backend::source_evidence::link_verified_chunks_tx(&mut tx, graph_id, captured.version_id, &[atom_id])
        .await.expect("map the exact newly inserted coral");
    let admitted: (i64, i64) = sqlx::query_as("SELECT (SELECT count(*) FROM source_version_chunks c JOIN source_versions v ON v.graph_id=c.graph_id AND v.version_id=c.source_version_id JOIN source_items s ON s.graph_id=v.graph_id AND s.source_item_id=v.source_item_id WHERE c.graph_id=$1 AND c.atom_id=$2 AND v.version_id=$3 AND s.current_version_id=v.version_id AND s.access_state='permitted' AND v.raw_inline=$4 AND library_atom_permitted(c.graph_id,c.atom_id)),(SELECT count(*) FROM library_sessions($1) WHERE session_id=$5)")
        .bind(graph_id).bind(atom_id).bind(captured.version_id).bind(body).bind(session_id)
        .fetch_one(&mut *tx).await.unwrap();
    assert_eq!(admitted, (1, 1), "the session must have exact retained support before the route is exercised");
    tx.commit().await.unwrap();
    atom_id
}

async fn seed_team(owner: &PgPool, account_id: Uuid, graph_id: Uuid, created_by: Uuid) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO teams(account_id, graph_id, name, created_by) \
         VALUES ($1, $2, 'probe team', $3) RETURNING id",
    )
    .bind(account_id)
    .bind(graph_id)
    .bind(created_by)
    .fetch_one(owner)
    .await
    .expect("insert team")
}

/// True when `e` is the RLS `WITH CHECK` refusal (`42501`), i.e. the write was
/// rejected by policy rather than by a foreign key / unique constraint. Mirrors
/// `store::is_pg_rls_denied`'s discrimination (PR #285) — a `23503` here would
/// mean the test proved the wrong thing.
fn is_rls_denied(e: &sqlx::Error) -> bool {
    matches!(e, sqlx::Error::Database(db) if db.code().as_deref() == Some("42501"))
}

// ── 1. Migration 0087's bootstrap resolvers ─────────────────────────────────

#[tokio::test]
async fn b5b_session_share_bootstraps_answer_unscoped_and_return_bare_ids() {
    let Some(probe) = RlsProbe::setup("orgt_rls_b5b_bootstrap").await else {
        eprintln!(
            "skipping b5b_session_share_bootstraps_answer_unscoped_and_return_bare_ids: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };

    seed_coding_session(&probe.owner, probe.a.graph_id, "sess-a", "proj-a").await;
    let active_id = seed_session_share(
        &probe.owner,
        probe.a.graph_id,
        "sess-a",
        "tok-active",
        probe.a.user_id,
    )
    .await;
    let revoked_id = seed_session_share(
        &probe.owner,
        probe.a.graph_id,
        "sess-a",
        "tok-revoked",
        probe.a.user_id,
    )
    .await;
    sqlx::query("UPDATE coding_session_shares SET revoked_at = now() WHERE id = $1")
        .bind(revoked_id)
        .execute(&probe.owner)
        .await
        .expect("revoke the second share");

    // The whole point: these answer WITHOUT `app.account_id` ever being set.
    // A plain `SELECT ... FROM coding_session_shares` on the same connection
    // would hard-error (proven at the bottom of this test).
    let owner: Option<Uuid> =
        sqlx::query_scalar("SELECT resolve_session_share_account_id($1)")
            .bind("tok-active")
            .fetch_one(&probe.app_pool)
            .await
            .expect("resolve_session_share_account_id must answer from an unscoped connection");
    assert_eq!(
        owner,
        Some(probe.a.account_id),
        "an ACTIVE token must resolve to its ocean's owning account"
    );

    // ACTIVE shares only — a revoked token must be indistinguishable from an
    // unknown one, which is what keeps the viewer's generic 404 honest.
    for token in ["tok-revoked", "tok-does-not-exist"] {
        let owner: Option<Uuid> =
            sqlx::query_scalar("SELECT resolve_session_share_account_id($1)")
                .bind(token)
                .fetch_one(&probe.app_pool)
                .await
                .expect("resolve_session_share_account_id must not error on a miss");
        assert_eq!(owner, None, "token {token:?} must resolve to NULL");
    }

    // The graph resolver matches ANY share, active or revoked — that is what
    // keeps `revoke_session_share` a no-op on a double revoke rather than a 404.
    for (id, label) in [(active_id, "active"), (revoked_id, "revoked")] {
        let graph: Option<Uuid> = sqlx::query_scalar("SELECT resolve_session_share_graph_id($1)")
            .bind(id)
            .fetch_one(&probe.app_pool)
            .await
            .expect("resolve_session_share_graph_id must answer from an unscoped connection");
        assert_eq!(
            graph,
            Some(probe.a.graph_id),
            "the {label} share's graph must resolve"
        );
    }
    let graph: Option<Uuid> = sqlx::query_scalar("SELECT resolve_session_share_graph_id($1)")
        .bind(Uuid::new_v4())
        .fetch_one(&probe.app_pool)
        .await
        .expect("unknown share id must not error");
    assert_eq!(graph, None);

    // Control: the raw table read the resolvers replace DOES hard-error
    // unscoped. Without this the test above proves nothing about why the
    // SECURITY DEFINER hop is needed.
    probe
        .assert_unscoped_hard_errors(
            "SELECT id FROM coding_session_shares WHERE graph_id = $1",
            &[probe.a.graph_id],
        )
        .await;

    probe.teardown().await;
}

// ── 2. Query-shape + scoped-write probes ────────────────────────────────────

#[tokio::test]
async fn b5b_query_shapes_are_account_scoped() {
    let Some(probe) = RlsProbe::setup("orgt_rls_b5b_shapes").await else {
        eprintln!("skipping b5b_query_shapes_are_account_scoped: set ORGT_TEST_BASE_URL");
        return;
    };

    seed_coding_session(&probe.owner, probe.a.graph_id, "sess-a", "proj-a").await;
    seed_coding_session(&probe.owner, probe.b.graph_id, "sess-b", "proj-b").await;
    let a_share = seed_session_share(
        &probe.owner,
        probe.a.graph_id,
        "sess-a",
        "shape-tok-a",
        probe.a.user_id,
    )
    .await;
    let b_share = seed_session_share(
        &probe.owner,
        probe.b.graph_id,
        "sess-b",
        "shape-tok-b",
        probe.b.user_id,
    )
    .await;
    let a_team = seed_team(
        &probe.owner,
        probe.a.account_id,
        probe.a.graph_id,
        probe.a.user_id,
    )
    .await;
    let b_team = seed_team(
        &probe.owner,
        probe.b.account_id,
        probe.b.graph_id,
        probe.b.user_id,
    )
    .await;

    // ── routes/sessions.rs: list_coding_sessions_scoped / get_coding_session_scoped
    probe
        .assert_scoped_returns_own_rows(
            probe.a.account_id,
            "SELECT id FROM coding_sessions WHERE graph_id = $1",
            &[probe.a.graph_id],
            &[sqlx::query_scalar::<_, Uuid>(
                "SELECT id FROM coding_sessions WHERE graph_id = $1",
            )
            .bind(probe.a.graph_id)
            .fetch_one(&probe.owner)
            .await
            .expect("read A's session id via the owner pool")],
        )
        .await;
    probe
        .assert_cross_account_returns_nothing(
            probe.a.account_id,
            "SELECT id FROM coding_sessions WHERE graph_id = $1",
            &[probe.b.graph_id],
        )
        .await;
    probe
        .assert_unscoped_hard_errors(
            "SELECT id FROM coding_sessions WHERE graph_id = $1",
            &[probe.a.graph_id],
        )
        .await;

    // ── routes/session_shares.rs: the token/id-anchored share reads ─────────
    // Holding a correctly resolved id from the OTHER account must buy nothing.
    probe
        .assert_scoped_returns_own_rows(
            probe.a.account_id,
            "SELECT id FROM coding_session_shares WHERE id = $1",
            &[a_share],
            &[a_share],
        )
        .await;
    probe
        .assert_cross_account_returns_nothing(
            probe.a.account_id,
            "SELECT id FROM coding_session_shares WHERE id = $1",
            &[b_share],
        )
        .await;

    // ── routes/teams.rs: the four named-team tables ────────────────────────
    probe
        .assert_scoped_returns_own_rows(
            probe.a.account_id,
            "SELECT id FROM teams WHERE account_id = $1",
            &[probe.a.account_id],
            &[a_team],
        )
        .await;
    probe
        .assert_cross_account_returns_nothing(
            probe.a.account_id,
            "SELECT id FROM teams WHERE id = $1",
            &[b_team],
        )
        .await;
    probe
        .assert_unscoped_hard_errors("SELECT id FROM teams WHERE id = $1", &[a_team])
        .await;
    probe
        .assert_cross_account_returns_nothing(
            probe.a.account_id,
            "SELECT tm.id FROM team_members tm JOIN teams t ON t.id = tm.team_id WHERE t.id = $1",
            &[b_team],
        )
        .await;
    // Run the team_keys shape scoped FIRST: a successful (empty) scoped run is
    // what proves the SQL is well-formed, so the hard-error assertion below
    // can only be passing because of the policy — not because of a typo.
    probe
        .assert_scoped_returns_own_rows(
            probe.a.account_id,
            "SELECT tk.id FROM team_keys tk JOIN teams t ON t.graph_id = tk.graph_id \
             WHERE t.id = $1",
            &[a_team],
            &[],
        )
        .await;
    probe
        .assert_unscoped_hard_errors(
            "SELECT tk.id FROM team_keys tk JOIN teams t ON t.graph_id = tk.graph_id \
             WHERE t.id = $1",
            &[a_team],
        )
        .await;

    probe.teardown().await;
}

#[tokio::test]
async fn b5b_scoped_writes_commit_and_cross_account_writes_are_refused() {
    let Some(probe) = RlsProbe::setup("orgt_rls_b5b_writes").await else {
        eprintln!(
            "skipping b5b_scoped_writes_commit_and_cross_account_writes_are_refused: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };

    seed_coding_session(&probe.owner, probe.a.graph_id, "sess-a", "proj-a").await;
    seed_coding_session(&probe.owner, probe.b.graph_id, "sess-b", "proj-b").await;

    // ── create_session_share_scoped: own-account commits ───────────────────
    let mut tx = probe.scoped_tx(probe.a.account_id).await;
    sqlx::query(
        "INSERT INTO coding_session_shares(graph_id, session_id, token, visibility, created_by) \
         VALUES ($1, 'sess-a', 'write-tok-a', 'link', $2)",
    )
    .bind(probe.a.graph_id)
    .bind(probe.a.user_id)
    .execute(&mut *tx)
    .await
    .expect("A's own share must insert under A's scope");
    tx.commit().await.expect("commit A's share");

    // ── ... and a forged cross-account insert is refused by POLICY ─────────
    let mut tx = probe.scoped_tx(probe.a.account_id).await;
    let forged = sqlx::query(
        "INSERT INTO coding_session_shares(graph_id, session_id, token, visibility, created_by) \
         VALUES ($1, 'sess-b', 'write-tok-forged', 'link', $2)",
    )
    .bind(probe.b.graph_id)
    .bind(probe.a.user_id)
    .execute(&mut *tx)
    .await;
    let err = forged.expect_err("A must not be able to mint a share in B's ocean");
    assert!(
        is_rls_denied(&err),
        "cross-account coding_session_shares INSERT must be refused by RLS (42501), got {err:?}"
    );
    tx.rollback().await.ok();
    let leaked: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM coding_session_shares WHERE graph_id = $1")
            .bind(probe.b.graph_id)
            .fetch_one(&probe.owner)
            .await
            .expect("count B's shares via the owner pool");
    assert_eq!(leaked, 0, "the refused cross-account share must not have landed");

    // ── revoke_session_share_scoped: A cannot revoke B's share ────────────
    let b_share = seed_session_share(
        &probe.owner,
        probe.b.graph_id,
        "sess-b",
        "write-tok-b",
        probe.b.user_id,
    )
    .await;
    let mut tx = probe.scoped_tx(probe.a.account_id).await;
    let affected = sqlx::query("UPDATE coding_session_shares SET revoked_at = now() WHERE id = $1")
        .bind(b_share)
        .execute(&mut *tx)
        .await
        .expect("a policy-filtered UPDATE is not an error, it just matches nothing")
        .rows_affected();
    assert_eq!(
        affected, 0,
        "A's scope must not see B's share row, so the revoke must affect zero rows"
    );
    tx.commit().await.ok();
    let still_active: Option<Option<chrono::DateTime<chrono::Utc>>> =
        sqlx::query_scalar("SELECT revoked_at FROM coding_session_shares WHERE id = $1")
            .bind(b_share)
            .fetch_optional(&probe.owner)
            .await
            .expect("read B's share via the owner pool");
    assert_eq!(
        still_active,
        Some(None),
        "B's share must still be active after A's attempted revoke"
    );

    // ── create_team_tx_scoped: a forged cross-account team is refused ──────
    let mut tx = probe.scoped_tx(probe.a.account_id).await;
    let forged = sqlx::query(
        "INSERT INTO teams(account_id, graph_id, name, created_by) \
         VALUES ($1, $2, 'forged team', $3)",
    )
    .bind(probe.b.account_id)
    .bind(probe.b.graph_id)
    .bind(probe.a.user_id)
    .execute(&mut *tx)
    .await;
    let err = forged.expect_err("A must not be able to create a team in B's account");
    assert!(
        is_rls_denied(&err),
        "cross-account teams INSERT must be refused by RLS (42501), got {err:?}"
    );
    tx.rollback().await.ok();

    probe.teardown().await;
}

// ── 3. Route-level probes through the real public router, as coralswarm_app ─

fn future_exp() -> usize {
    (chrono::Utc::now().timestamp() + 3600) as usize
}

/// Local JWKS listener serving the shared test keypair — same fixture
/// `routes_b2_rls_pg.rs` uses, duplicated for the same separate-compilation-
/// unit reason.
async fn spawn_local_jwks() -> String {
    let jwks_body = test_jwks();
    let jwks_app = axum::Router::new().route(
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
        axum::serve(listener, jwks_app).await.ok();
    });
    format!("http://{addr}/.well-known/jwks.json")
}

async fn body_json(resp: axum::response::Response) -> Value {
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice(&bytes).unwrap_or(Value::Null)
}

#[tokio::test]
async fn b5b_converted_handlers_work_and_isolate_as_coralswarm_app() {
    let Some(probe) = RlsProbe::setup("orgt_rls_b5b_routes").await else {
        eprintln!(
            "skipping b5b_converted_handlers_work_and_isolate_as_coralswarm_app: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };

    // ── Env: local JWKS so `require_auth` verifies real Clerk-shaped tokens.
    // This binary's only env-mutating test, so no cross-test lock is needed
    // (each test binary is its own process). ─────────────────────────────────
    let jwks_url = spawn_local_jwks().await;
    // SAFETY: single-threaded with respect to env mutation — this is the only
    // test in this binary that touches CLERK_*/MASTER_KEK_HEX (see above).
    unsafe {
        std::env::set_var("CLERK_ISSUER", ISSUER);
        std::env::set_var("CLERK_JWKS_URL", &jwks_url);
        // `create_team`'s envelope-encryption flow unwraps the caller's
        // account DEK under this KEK; without it the handler 500s on crypto
        // configuration before it ever reaches a scoped query.
        std::env::set_var("MASTER_KEK_HEX", "ab".repeat(32));
    }

    let sub_a = "b5b_rls_probe_user_a";
    let sub_b = "b5b_rls_probe_user_b";
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

    // Provision A's account DEK, KEK-wrapped under the test master key above,
    // so `create_team`'s `load_account_dek` succeeds and the handler reaches
    // its scoped write.
    {
        let kek = coralswarm_core::kek::load_kek().expect("load test KEK");
        let wrapped = coralswarm_core::kek::wrap_dek(&kek, &[7u8; 32]).expect("wrap test DEK");
        sqlx::query("UPDATE accounts SET dek_wrapped = $1 WHERE id = $2")
            .bind(&wrapped)
            .bind(probe.a.account_id)
            .execute(&probe.owner)
            .await
            .expect("provision A's account DEK");
    }

    seed_coding_session(&probe.owner, probe.a.graph_id, "sess-a", "proj-a").await;
    seed_coding_session(&probe.owner, probe.b.graph_id, "sess-b", "proj-b").await;
    seed_session_coral(&probe.owner, probe.a.graph_id, "sess-a", "A's coral body").await;
    let b_share_id = seed_session_share(
        &probe.owner,
        probe.b.graph_id,
        "sess-b",
        "route-tok-b",
        probe.b.user_id,
    )
    .await;

    // ── AppState served ENTIRELY off the coralswarm_app pool ────────────────
    let mut state = (*coralswarm_backend::test_app_state()).clone();
    state.db = Some(probe.app_pool.clone());
    state.store = Some(Arc::new(PostgresStore::new(probe.app_pool.clone())));
    let app = coralswarm_backend::test_support_app(Arc::new(state));

    let token_a = mint(json!({ "iss": ISSUER, "sub": sub_a, "exp": future_exp() }));

    macro_rules! req_a {
        ($method:expr, $uri:expr) => {
            HttpRequest::builder()
                .method($method)
                .uri($uri)
                .header("authorization", format!("Bearer {token_a}"))
                .body(Body::empty())
                .unwrap()
        };
        ($method:expr, $uri:expr, $json:expr) => {
            HttpRequest::builder()
                .method($method)
                .uri($uri)
                .header("authorization", format!("Bearer {token_a}"))
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&$json).unwrap()))
                .unwrap()
        };
    }

    // ── routes/sessions.rs: GET /v1/sessions ───────────────────────────────
    let resp = app
        .clone()
        .oneshot(req_a!(
            "GET",
            format!("/v1/sessions?graph_id={}", probe.a.graph_id)
        ))
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "A's own session list must serve under the coralswarm_app role"
    );
    let body = body_json(resp).await;
    assert_eq!(
        body["sessions"].as_array().map(|a| a.len()),
        Some(1),
        "the scoped list must return exactly A's own session: {body}"
    );

    // Cross-account: A aiming at B's ocean 404s (reachability) and leaks nothing.
    let resp = app
        .clone()
        .oneshot(req_a!(
            "GET",
            format!("/v1/sessions?graph_id={}", probe.b.graph_id)
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    let body = body_json(resp).await;
    assert!(
        !body.to_string().contains("sess-b"),
        "the cross-account 404 must not leak B's session: {body}"
    );

    // ── routes/session_shares.rs: POST /v1/sessions/shares ─────────────────
    let resp = app
        .clone()
        .oneshot(req_a!(
            "POST",
            "/v1/sessions/shares",
            json!({ "graph_id": probe.a.graph_id, "session_id": "sess-a" })
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CONFLICT, "cloud whole-original sharing requires selected evidence consent");
    let consent_refusal = json!({
        "error": "Whole-original links require selected evidence consent",
        "code": "selected_evidence_consent_required"
    });
    assert_eq!(body_json(resp).await, consent_refusal);
    let a_shares: i64 = sqlx::query_scalar("SELECT count(*) FROM coding_session_shares WHERE graph_id=$1")
        .bind(probe.a.graph_id).fetch_one(&probe.owner).await.unwrap();
    assert_eq!(a_shares, 0, "refused creation must not write a share");

    // The pre-lookup consent gate also refuses B without exposing existence.
    let resp = app
        .clone()
        .oneshot(req_a!(
            "POST",
            "/v1/sessions/shares",
            json!({ "graph_id": probe.b.graph_id, "session_id": "sess-b" })
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CONFLICT);
    assert_eq!(body_json(resp).await, consent_refusal);
    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM coding_session_shares WHERE graph_id = $1")
            .bind(probe.b.graph_id)
            .fetch_one(&probe.owner)
            .await
            .expect("count B's shares via the owner pool");
    assert_eq!(count, 1, "only the pre-seeded share may exist in B's ocean");
    let a_shares_after_foreign: i64 = sqlx::query_scalar("SELECT count(*) FROM coding_session_shares WHERE graph_id=$1")
        .bind(probe.a.graph_id).fetch_one(&probe.owner).await.unwrap();
    assert_eq!(a_shares_after_foreign, a_shares, "neither refused request may write a share in A's ocean");

    // A historical share still exercises scoped revoke below. It is seeded
    // explicitly, never obtained through the now-refused creation endpoint.
    let a_token = "route-tok-a";
    let a_share_id = seed_session_share(&probe.owner, probe.a.graph_id, "sess-a", a_token, probe.a.user_id).await;

    // ── Anonymous cloud whole-original reads never reveal private content. ─
    let resp = app
        .clone()
        .oneshot(
            HttpRequest::builder()
                .method("GET")
                .uri(format!("/v1/shared/sessions/{a_token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CONFLICT);
    assert_eq!(body_json(resp).await, consent_refusal, "an active historical token must expose no session metadata or original coral body");

    // Unknown, active and revoked tokens receive the identical refusal.
    let resp = app
        .clone()
        .oneshot(
            HttpRequest::builder()
                .method("GET")
                .uri("/v1/shared/sessions/definitely-not-a-real-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CONFLICT);
    assert_eq!(body_json(resp).await, consent_refusal);

    // ── POST /v1/sessions/shares/{id}/revoke ───────────────────────────────
    let resp = app
        .clone()
        .oneshot(req_a!(
            "POST",
            format!("/v1/sessions/shares/{a_share_id}/revoke")
        ))
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "A must be able to revoke A's own share under the app role"
    );
    let revoked: Option<chrono::DateTime<chrono::Utc>> =
        sqlx::query_scalar("SELECT revoked_at FROM coding_session_shares WHERE id = $1")
            .bind(a_share_id)
            .fetch_one(&probe.owner)
            .await
            .expect("read A's share via the owner pool");
    assert!(revoked.is_some(), "A's own share must actually be revoked");

    // Revocation persists, and the token still cannot reveal a whole original.
    let resp = app
        .clone()
        .oneshot(
            HttpRequest::builder()
                .method("GET")
                .uri(format!("/v1/shared/sessions/{a_token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CONFLICT);
    assert_eq!(body_json(resp).await, consent_refusal);

    // A holding B's share id: 404 (reachability), and B's share stays active.
    let resp = app
        .clone()
        .oneshot(req_a!(
            "POST",
            format!("/v1/sessions/shares/{b_share_id}/revoke")
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    let revoked: Option<chrono::DateTime<chrono::Utc>> =
        sqlx::query_scalar("SELECT revoked_at FROM coding_session_shares WHERE id = $1")
            .bind(b_share_id)
            .fetch_one(&probe.owner)
            .await
            .expect("read B's share via the owner pool");
    assert!(
        revoked.is_none(),
        "A must not be able to revoke B's session share"
    );

    // ── routes/teams.rs: POST /v1/teams (named team, envelope-encrypted) ───
    // `create_team_tx_scoped` writes graphs + teams + team_keys +
    // team_member_key_envelopes + team_members in ONE transaction, so this is
    // the densest RLS write in the file. It needs a real KEK-wrapped account
    // DEK, which the harness fixtures don't provision — so mint one here.
    let resp = app
        .clone()
        .oneshot(req_a!("POST", "/v1/teams", json!({ "name": "probe team" })))
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::CREATED,
        "A's own named team must commit under the coralswarm_app role"
    );
    let created = body_json(resp).await;
    let team_id: Uuid = created["team_id"].as_str().unwrap().parse().unwrap();
    // Every one of the five rows must have landed, in A's account.
    let owning_account: Uuid = sqlx::query_scalar("SELECT account_id FROM teams WHERE id = $1")
        .bind(team_id)
        .fetch_one(&probe.owner)
        .await
        .expect("read the new team via the owner pool");
    assert_eq!(owning_account, probe.a.account_id);
    let members: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM team_members WHERE team_id = $1")
        .bind(team_id)
        .fetch_one(&probe.owner)
        .await
        .expect("count team members via the owner pool");
    assert_eq!(members, 1, "the creator's membership row must have committed");
    let envelopes: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM team_member_key_envelopes e \
         JOIN team_keys k ON k.id = e.team_key_id \
         JOIN teams t ON t.graph_id = k.graph_id WHERE t.id = $1",
    )
    .bind(team_id)
    .fetch_one(&probe.owner)
    .await
    .expect("count key envelopes via the owner pool");
    assert_eq!(envelopes, 1, "the creator's key envelope must have committed");

    // A cannot remove a member of B's team. This now fails closed at 404
    // rather than the previous 403, and the change is deliberate: the
    // last-admin race fix added a `SELECT 1 FROM teams WHERE id = $1 FOR
    // UPDATE` as the FIRST statement of the handler's scoped transaction, and
    // under RLS that row is invisible to A, so the handler returns "team not
    // found" before it ever reaches the admin check that used to answer 403.
    //
    // 404 is the better answer for a cross-account caller: 403 confirms that
    // B's team EXISTS, which is an existence oracle across a tenant boundary.
    // Within an account nothing changed — a non-admin member of a team they
    // CAN see still passes the lock and gets 403 from the admin check.
    let b_team = seed_team(
        &probe.owner,
        probe.b.account_id,
        probe.b.graph_id,
        probe.b.user_id,
    )
    .await;
    let resp = app
        .clone()
        .oneshot(req_a!(
            "DELETE",
            format!("/v1/teams/{b_team}/members/{}", probe.b.user_id)
        ))
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::NOT_FOUND,
        "A must not be able to act as an admin of B's team — and must not even \
         learn that it exists"
    );

    // A non-admin member of a team in THEIR OWN account: 403, not 404, not
    // success. This is the second half of the comment two blocks up, and
    // CodeRabbit correctly flagged that nothing tested it: the cross-account
    // case above 404s because the `SELECT 1 FROM teams WHERE id = $1 FOR
    // UPDATE` lock (the last-admin-race fix) never even finds B's team under
    // RLS — the handler answers "not found" before it ever reaches the admin
    // check.
    //
    // Note this is NOT constructed by adding a second real user to A's
    // account: `account_members` plays no role in how a NON-org caller's
    // `account_id` is resolved (`resolve_clerk_context`'s personal-account
    // branch is `ensure_personal_account`, keyed purely on
    // `accounts.owner_user_id = <this user>` — it never consults
    // `account_members` at all), so a second Clerk-authenticated user always
    // resolves to THEIR OWN personal account, never someone else's, no
    // matter what `account_members` row exists. The right fixture is
    // therefore a THIRD user acting entirely within their OWN account: mint
    // user C, let a real request materialize C's personal account (the exact
    // `ensure_personal_account` path every other user in this test went
    // through), then seed a team INSIDE that same account with C as a plain
    // 'member' (not 'admin'). That team IS visible to C's account under RLS,
    // so the `FOR UPDATE` lock succeeds and the handler reaches the admin
    // check — which is where a non-admin member must be stopped. Proves they
    // get exactly 403 — never 404 (that would mean the lock itself failed,
    // the cross-account case above) and never 2xx (that would mean the admin
    // check was bypassed entirely).
    let sub_c = "b5b_rls_probe_user_c";
    let token_c = mint(json!({ "iss": ISSUER, "sub": sub_c, "exp": future_exp() }));

    // A trivial authenticated GET materializes C's personal account via the
    // same `require_auth` -> `ensure_personal_account` path every other user
    // in this test already went through — no raw fixture insert stands in
    // for account resolution itself.
    let resp = app
        .clone()
        .oneshot(
            HttpRequest::builder()
                .method("GET")
                .uri("/v1/teams")
                .header("authorization", format!("Bearer {token_c}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "a trivial authenticated GET for user C must materialize C's own personal account"
    );
    let user_c: Uuid = sqlx::query_scalar("SELECT id FROM users WHERE clerk_user_id = $1")
        .bind(sub_c)
        .fetch_one(&probe.owner)
        .await
        .expect("read C's user id via the owner pool");
    let account_c: Uuid = sqlx::query_scalar(
        "SELECT id FROM accounts WHERE owner_user_id = $1 AND clerk_org_id IS NULL",
    )
    .bind(user_c)
    .fetch_one(&probe.owner)
    .await
    .expect("C's personal account must exist after the GET above materialized it");

    let team_c = seed_team(&probe.owner, account_c, {
        sqlx::query_scalar("INSERT INTO graphs(account_id, name, kind) VALUES ($1, 'C team ocean', 'team') RETURNING id")
            .bind(account_c)
            .fetch_one(&probe.owner)
            .await
            .expect("insert graph for C's own team")
    }, user_c)
    .await;
    sqlx::query("INSERT INTO team_members(team_id, user_id, role) VALUES ($1, $2, 'member')")
        .bind(team_c)
        .bind(user_c)
        .execute(&probe.owner)
        .await
        .expect("seed team_members for C as a non-admin member of C's own team");

    let resp = app
        .clone()
        .oneshot(
            HttpRequest::builder()
                .method("DELETE")
                .uri(format!("/v1/teams/{team_c}/members/{}", Uuid::new_v4()))
                .header("authorization", format!("Bearer {token_c}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::FORBIDDEN,
        "a non-admin member of a team in THEIR OWN account must get 403 from the admin check \
         (the team IS visible under RLS, so the FOR UPDATE lock succeeds and the handler reaches \
         the admin check) — not 404 (that would mean the lock failed, the cross-account case \
         above) and not success (that would mean the admin check was bypassed)"
    );

    // The control-plane membership routes must be entirely unaffected — they
    // read `accounts`/`account_members`/`users`, none of which carry a 0083
    // policy, so they serve identically under the app role.
    let resp = app.clone().oneshot(req_a!("GET", "/v1/teams")).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    assert_eq!(
        body.as_array().map(|a| a.len()),
        Some(1),
        "A must see exactly its own account membership: {body}"
    );
    let resp = app
        .clone()
        .oneshot(req_a!("GET", "/v1/teams/members"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    probe.teardown().await;
}
