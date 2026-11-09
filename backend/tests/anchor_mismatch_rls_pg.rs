//! The anchor mismatch: a legitimate reader gets 404 on data the list route shows.
//!
//! # What this proves, in BOTH directions
//!
//! The id-anchored conversation handlers used to scope their RLS transaction to
//! the CALLER'S ACTIVE account. `conversations` is covered by migration 0083, so
//! whenever the caller's active account differs from the ocean's OWNER the row is
//! invisible, `load_readable` sees `Ok(None)`, and the handler returns 404 — for a
//! conversation the graph-anchored sidebar had just rendered. They now resolve the
//! owning account through `resolve_conversation_account_id` (migration 0105).
//!
//! The precondition is ordinary: `require_auth` derives `account_id` from the JWT's
//! `org_id` claim — absent, it is the caller's PERSONAL account
//! (`ensure_personal_account`: `owner_user_id = $1 AND clerk_org_id IS NULL`). So
//! any user reading a conversation in an ocean they can reach but do not own hits
//! this, which is every shared/team ocean and every Personal↔Org switch.
//!
//! **Widening RLS visibility is only safe if authorization was never coming from
//! it.** It wasn't — `load_readable` requires owner, or `shared` AND
//! `graph_reachable_by_user`, else 403. This suite asserts both halves, because
//! only the pair is meaningful:
//!
//!   * SHARED conversation in a REACHABLE ocean the caller does not own → **200**
//!     (the fix; 404 before)
//!   * PRIVATE conversation in that same ocean → **403** (the safety; must NOT
//!     become 200 now that the row is visible)
//!   * SHARED conversation in an UNREACHABLE ocean → **404**, never 403 (the
//!     non-leaking refusal)
//!
//! The first alone would pass just as well against a change that handed every
//! conversation to every caller. The third exists because anchoring on the owning
//! account makes other accounts' rows visible to RLS for the first time, so an
//! unconditional 403 in `load_readable` would newly confirm a conversation EXISTS
//! to someone with no access to its ocean. That regression is not hypothetical —
//! the first cut of this change shipped it, and `routes_b2_rls_pg` caught it.
//!
//! Env-gated on `ORGT_TEST_BASE_URL`, like every other `*_pg.rs` here.

mod support;

use axum::body::Body;
use axum::http::{Request as HttpRequest, StatusCode};
use coralswarm_backend::clerk::test_support::{mint, test_jwks, ISSUER};
use coralswarm_backend::store::PostgresStore;
use serde_json::json;
use std::sync::Arc;
use support::rls_probe::RlsProbe;
use tower::ServiceExt;
use uuid::Uuid;

fn future_exp() -> usize {
    (chrono::Utc::now().timestamp() + 3600) as usize
}

/// Local JWKS listener serving the shared test keypair. `test_jwks()` returns the
/// document as a STRING, so it must be served raw with an explicit content-type —
/// wrapping it in `axum::Json` would JSON-encode the string and every token would
/// fail verification with a 401.
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

async fn get_conversation(app: &axum::Router, token: &str, id: Uuid) -> (StatusCode, String) {
    let req = HttpRequest::builder()
        .method("GET")
        .uri(format!("/v1/conversations/{id}"))
        .header("authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, String::from_utf8_lossy(&bytes).into_owned())
}

#[tokio::test]
async fn conversation_in_another_accounts_ocean_is_readable_when_shared_and_forbidden_when_not() {
    let Some(probe) = RlsProbe::setup("orgt_anchor_mismatch_rls").await else {
        eprintln!("skipping conversation anchor-mismatch suite: set ORGT_TEST_BASE_URL");
        return;
    };

    // ── Fixture, seeded through the OWNER pool ────────────────────────────────
    // Seeding must bypass the very policies under test.
    //
    // User B is a MEMBER of account A but owns account B. With no `org_id` claim,
    // `require_auth` resolves B's active account to B's own — so every read of a
    // conversation living in A's ocean is exactly the mismatch under test.
    let sub_b = "anchor_mismatch_probe_user_b";
    sqlx::query("UPDATE users SET clerk_user_id = $1 WHERE id = $2")
        .bind(sub_b)
        .bind(probe.b.user_id)
        .execute(&probe.owner)
        .await
        .expect("stamp clerk_user_id");
    for (account, user) in [
        (probe.b.account_id, probe.b.user_id), // B's own account (their personal one)
        (probe.a.account_id, probe.b.user_id), // …and membership in A, so A's ocean is reachable
    ] {
        sqlx::query(
            "INSERT INTO account_members(account_id, user_id, role) VALUES ($1, $2, 'member') \
             ON CONFLICT DO NOTHING",
        )
        .bind(account)
        .bind(user)
        .execute(&probe.owner)
        .await
        .expect("seed account_members");
    }

    // Two conversations, both owned by user A in A's ocean. They differ ONLY in
    // `shared`, which is the single bit `load_readable` authorizes on.
    let shared_conv = Uuid::new_v4();
    let private_conv = Uuid::new_v4();
    for (id, shared, title) in [
        (shared_conv, true, "shared thread"),
        (private_conv, false, "private thread"),
    ] {
        sqlx::query(
            "INSERT INTO conversations(id, account_id, graph_id, user_id, title, shared) \
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(id)
        .bind(probe.a.account_id)
        .bind(probe.a.graph_id)
        .bind(probe.a.user_id)
        .bind(title)
        .bind(shared)
        .execute(&probe.owner)
        .await
        .expect("seed conversation");
    }

    // A THIRD account that user B is NOT a member of, holding a SHARED conversation.
    // `shared` is deliberately true so the only thing refusing the read is
    // reachability — this isolates the 404-vs-403 distinction from the shared bit.
    // An ORG account, not a second personal one: `uq_accounts_one_personal_per_owner`
    // permits exactly one account per owner with `clerk_org_id IS NULL`. An org
    // ocean the caller is not a member of is also the more faithful shape.
    let unreachable_account: Uuid = sqlx::query_scalar(
        "INSERT INTO accounts(owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id",
    )
    .bind(probe.a.user_id)
    .bind(format!("org_unreachable_{}", Uuid::new_v4()))
    .fetch_one(&probe.owner)
    .await
    .expect("seed unreachable account");
    let unreachable_graph: Uuid = sqlx::query_scalar(
        "INSERT INTO graphs(account_id, name) VALUES ($1, 'unreachable') RETURNING id",
    )
    .bind(unreachable_account)
    .fetch_one(&probe.owner)
    .await
    .expect("seed unreachable graph");
    let unreachable_conv = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO conversations(id, account_id, graph_id, user_id, title, shared) \
         VALUES ($1, $2, $3, $4, 'unreachable thread', true)",
    )
    .bind(unreachable_conv)
    .bind(unreachable_account)
    .bind(unreachable_graph)
    .bind(probe.a.user_id)
    .execute(&probe.owner)
    .await
    .expect("seed unreachable conversation");

    let jwks_url = spawn_local_jwks().await;
    // SAFETY: single-threaded with respect to env mutation — this is the only test
    // in this binary and the only thing here that touches CLERK_*.
    unsafe {
        std::env::set_var("CLERK_ISSUER", ISSUER);
        std::env::set_var("CLERK_JWKS_URL", &jwks_url);
    }

    // Served ENTIRELY off the coralswarm_app pool — the non-BYPASSRLS role.
    let mut state = (*coralswarm_backend::test_app_state()).clone();
    state.db = Some(probe.app_pool.clone());
    state.store = Some(Arc::new(PostgresStore::new(probe.app_pool.clone())));
    let app = coralswarm_backend::test_support_app(Arc::new(state));

    // No `org_id` claim ⇒ active account is B's personal account, NOT A's.
    let token_b = mint(json!({ "iss": ISSUER, "sub": sub_b, "exp": future_exp() }));

    // ── 1. THE FIX ────────────────────────────────────────────────────────────
    // Shared conversation in A's ocean, read by a member whose active account is B.
    // Against the pre-fix handler the scoped transaction is opened on B, RLS hides
    // the row, `load_readable` gets `Ok(None)` and this is a 404 — the sidebar
    // renders it, opening it fails.
    let (status, body) = get_conversation(&app, &token_b, shared_conv).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "a SHARED conversation in a reachable ocean must be readable by a member whose \
         active account is not the ocean's owner. 404 here is the anchor mismatch: the row \
         was hidden by RLS before `load_readable` could authorize it. body: {body}"
    );

    // ── 2. THE SAFETY ─────────────────────────────────────────────────────────
    // Same ocean, same caller, same visibility — the ONLY difference is `shared`.
    // This is what proves the fix widened VISIBILITY without widening ACCESS: the
    // row is now visible to RLS, so if authorization were coming from the scope
    // this would have turned into a 200.
    let (status, body) = get_conversation(&app, &token_b, private_conv).await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "a PRIVATE conversation must stay forbidden even though the owning-account scope \
         now makes its row visible. A 200 here would mean the fix replaced a 404 with a \
         cross-account read; a 403 is correct here because the caller CAN see this ocean, \
         so the thread's existence is not a secret from them. body: {body}"
    );

    // ── 3. THE NON-LEAKING REFUSAL ────────────────────────────────────────────
    // A SHARED conversation in an ocean the caller cannot reach at all. Shared, so
    // the only thing refusing it is reachability.
    //
    // This must be 404, not 403. Anchoring on the OWNING account makes rows in
    // other accounts visible to RLS for the first time, so an unconditional 403 in
    // `load_readable` would newly confirm that a conversation EXISTS to someone
    // with no access to its ocean — an existence oracle where there was none.
    // `routes_b2_rls_pg` pins the same property ("filtered to absent, not
    // 403-leaked"); it is asserted here too because this suite is what would be
    // read by anyone changing this refusal path.
    let (status, body) = get_conversation(&app, &token_b, unreachable_conv).await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "a conversation in an UNREACHABLE ocean must be indistinguishable from one that \
         does not exist. 403 here leaks its existence. body: {body}"
    );
}
