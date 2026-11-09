//! #1496: a fresh-auth conversation read opens ONE transaction, and the
//! control-plane admission re-check still runs wherever the door did not
//! prove the answer.
//!
//! Drives `GET /v1/conversations/{id}` through the REAL router
//! (`require_auth` → handler) on a scratch control database, connected AS
//! `coralswarm_app`, and reads the `Server-Timing` `txopen` count, which
//! counts every `tenancy::scope::begin_*` opened on the request task
//! (`request_timing::record_tx_open`):
//!
//! 1. Clerk session, personal member: `desc="1 tx"` (the tenant read only;
//!    the door's prefetch vouched for membership).
//!    The same user's Clerk OAuth access token (no `sid`): `desc="2 tx"`.
//! 2. Gmail add-on PAT for the SAME conversation: no door fact is minted for
//!    that credential, so the control re-check runs: `desc="2 tx"`.
//! 3. Membership row removed, Clerk session again: the caller still OWNS the
//!    account, so auth succeeds, but ownership is not membership — no door
//!    fact is minted and the read is refused (404). On the consumer plane
//!    graph reachability (`resolve_graph_reachable_by_user`) also requires
//!    the member row and refuses first, so this step asserts the outcome,
//!    not which fence fired; the minting rule itself is unit-tested in
//!    `middleware::auth::tests::door_admission_is_minted_only_for_the_context_the_prefetch_names`.
//!
//! One test function on purpose: it sets the process-global `CLERK_*` env.
//!
//! Gated on `ORGT_TEST_BASE_URL`; skips (with a line) when unset.
//!   ORGT_TEST_BASE_URL="postgres://alex@localhost:5432/postgres" \
//!     cargo test -p coralswarm-backend --test conversation_door_admission_pg

mod support;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use coralswarm_backend::clerk::test_support::{mint, test_jwks, ISSUER};
use coralswarm_backend::store::{PostgresStore, Store};
use serde_json::json;
use sha2::{Digest, Sha256};
use support::rls_probe::RlsProbe;
use tower::ServiceExt;
use uuid::Uuid;

fn get(uri: &str, token: &str) -> Request<Body> {
    Request::builder()
        .uri(uri)
        .header("authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap()
}

fn tx_desc(response: &axum::response::Response) -> String {
    let timing = response
        .headers()
        .get("server-timing")
        .expect("every response carries Server-Timing")
        .to_str()
        .unwrap()
        .to_string();
    let start = timing.find("desc=\"").unwrap_or_else(|| panic!("no txopen desc in {timing}"));
    let rest = &timing[start + 6..];
    rest[..rest.find('"').unwrap()].to_string()
}

#[tokio::test]
async fn fresh_auth_conversation_read_skips_only_the_control_recheck_the_door_proved() {
    let Some(probe) = RlsProbe::setup("conversation_door_admission_pg").await else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };
    let (user, account) = (probe.a.user_id, probe.a.account_id);
    let sub = format!("door-fixture-{user}");
    sqlx::query("UPDATE users SET clerk_user_id=$2 WHERE id=$1")
        .bind(user)
        .bind(&sub)
        .execute(&probe.owner)
        .await
        .unwrap();
    // The RlsProbe fixture owns its account but has no member row; a real
    // personal account always has one (`ensure_personal_account`).
    sqlx::query("INSERT INTO account_members(account_id,user_id,role) VALUES($1,$2,'owner')")
        .bind(account)
        .bind(user)
        .execute(&probe.owner)
        .await
        .unwrap();
    let graph: Uuid = sqlx::query_scalar(
        "INSERT INTO graphs(account_id,name,kind,owner_user_id) VALUES($1,'door fixture','member',$2) RETURNING id",
    )
    .bind(account)
    .bind(user)
    .fetch_one(&probe.owner)
    .await
    .unwrap();
    let conversation = PostgresStore::new(probe.owner.clone())
        .create_conversation(account, graph, user, "door fixture")
        .await
        .expect("seed conversation")
        .id;
    let pat = "csat_door_admission_fixture_token_do_not_reuse";
    sqlx::query("INSERT INTO addin_tokens (account_id, owner_user_id, token_hash) VALUES ($1, $2, $3)")
        .bind(account)
        .bind(user)
        .bind(hex::encode(Sha256::digest(pat.as_bytes())))
        .execute(&probe.owner)
        .await
        .unwrap();

    let jwks = test_jwks();
    let server = axum::Router::new().route(
        "/jwks",
        axum::routing::get(move || {
            let body = jwks.clone();
            async move { ([("content-type", "application/json")], body) }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/jwks", listener.local_addr().unwrap());
    let previous = (std::env::var_os("CLERK_ISSUER"), std::env::var_os("CLERK_JWKS_URL"));
    unsafe {
        std::env::set_var("CLERK_ISSUER", ISSUER);
        std::env::set_var("CLERK_JWKS_URL", url);
    }
    let jwks_task = tokio::spawn(async move { axum::serve(listener, server).await.unwrap() });

    let state = std::sync::Arc::new(coralswarm_backend::test_app_state_owned_with_db_and_store(
        probe.app_pool.clone(),
        None,
    ));
    let router = coralswarm_backend::test_support_app(state);
    let clerk = mint(json!({
        "iss": ISSUER,
        "sub": sub,
        // An interactive session token (has `sid`): only these mint the door
        // fact. Without `sid` this is a Clerk OAuth access token.
        "sid": "sess_door_fixture",
        "exp": (chrono::Utc::now() + chrono::Duration::hours(1)).timestamp(),
    }));
    let uri = format!("/v1/conversations/{conversation}");

    // 1. Fresh Clerk auth, live member: the tenant read is the only tx.
    let response = router.clone().oneshot(get(&uri, &clerk)).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        tx_desc(&response),
        "1 tx",
        "the door's prefetch proved membership; the control re-check must not open a second tx"
    );

    // 1b. The same user's Clerk OAuth access token (no `sid`; classified
    //     `Foreign`, allowed under the default REST_FOREIGN_OAUTH=warn): only
    //     interactive sessions mint the fact, so the re-check runs.
    let oauth = mint(json!({
        "iss": ISSUER,
        "sub": format!("door-fixture-{user}"),
        "exp": (chrono::Utc::now() + chrono::Duration::hours(1)).timestamp(),
    }));
    let response = router.clone().oneshot(get(&uri, &oauth)).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(tx_desc(&response), "2 tx", "a Clerk OAuth access token keeps the re-check");

    // 2. Same conversation via a PAT: no door fact, so the control re-check
    //    still runs. Proves (1) measured a skipped tx, not a route that never
    //    had one.
    let response = router.clone().oneshot(get(&uri, pat)).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(tx_desc(&response), "2 tx", "a credential without a door fact keeps the re-check");

    // 3. Membership gone, ownership kept: auth still succeeds (personal
    //    accounts resolve by owner) but the read must still be refused — the
    //    door fact must not have turned ownership into admission.
    sqlx::query("DELETE FROM account_members WHERE account_id=$1 AND user_id=$2")
        .bind(account)
        .bind(user)
        .execute(&probe.owner)
        .await
        .unwrap();
    let response = router.clone().oneshot(get(&uri, &clerk)).await.unwrap();
    assert_eq!(
        response.status(),
        StatusCode::NOT_FOUND,
        "a caller with no membership row must still be refused"
    );

    jwks_task.abort();
    unsafe {
        for (key, value) in [("CLERK_ISSUER", previous.0), ("CLERK_JWKS_URL", previous.1)] {
            match value {
                Some(value) => std::env::set_var(key, value),
                None => std::env::remove_var(key),
            }
        }
    }
    probe.teardown().await;
}
