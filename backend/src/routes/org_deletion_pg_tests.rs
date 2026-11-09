//! Route-level authorization for `/v1/orgs/{org_id}/deletion*`: an
//! interactive Clerk session only (#1473), a fresh re-verification for the
//! request, the in-app owner only. `RELINFER_TEST_DATABASE_URL`-gated; the
//! handlers run on a `coralswarm_app` pool. The job itself is tested in
//! `services::org_deletion::pg_tests` and `backend/tests/org_destroy_pg.rs`.

use std::sync::Arc;

use axum::extract::{Extension, Path, State};
use axum::http::StatusCode;
use axum::Json;
use uuid::Uuid;

use super::*;
use crate::billing::test_support::*;
use crate::middleware::auth::{with_request_authority_for_test, InteractiveClerkSession};

async fn app_pool(owner: &sqlx::PgPool) -> sqlx::PgPool {
    use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
    use std::str::FromStr;
    // Cluster-wide role: concurrent tests race on it ("tuple concurrently
    // updated"), so retry.
    for attempt in 0..10 {
        match sqlx::query("ALTER ROLE coralswarm_app WITH LOGIN PASSWORD 'app_test_pw'").execute(owner).await {
            Ok(_) => break,
            Err(e) if attempt < 9 => {
                let _ = e;
                tokio::time::sleep(std::time::Duration::from_millis(50 * (attempt + 1))).await;
            }
            Err(e) => panic!("ALTER ROLE coralswarm_app: {e}"),
        }
    }
    let url = std::env::var("RELINFER_TEST_DATABASE_URL").unwrap();
    let opts = PgConnectOptions::from_str(&url).unwrap().username("coralswarm_app").password("app_test_pw");
    PgPoolOptions::new().max_connections(4).connect_with(opts).await.unwrap()
}

fn fresh() -> Option<Extension<InteractiveClerkSession>> {
    Some(Extension(InteractiveClerkSession { factor_verification_age: Some([1, -1]) }))
}

fn stale() -> Option<Extension<InteractiveClerkSession>> {
    Some(Extension(InteractiveClerkSession { factor_verification_age: Some([60, -1]) }))
}

struct Seed {
    account: Uuid,
    org: String,
    creator: Uuid,
    member: Uuid,
}

async fn seed(owner: &sqlx::PgPool) -> Seed {
    let (account, creator) = seed_org_account(owner, "admin").await;
    let member = add_org_member(owner, account, "member").await;
    let org: String = sqlx::query_scalar("SELECT clerk_org_id FROM accounts WHERE id = $1")
        .bind(account)
        .fetch_one(owner)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO tenant_provisioning_jobs (clerk_org_id, account_id, slug, db_secret_ref, step, status, ready_at) \
         VALUES ($1, $2, 'o-' || left(replace($2::text, '-', ''), 16), \
                 'TENANT_DB_O_' || upper(left(replace($2::text, '-', ''), 16)), 'done', 'ready', now())",
    )
    .bind(&org)
    .bind(account)
    .execute(owner)
    .await
    .unwrap();
    Seed { account, org, creator, member }
}

fn on() {
    std::env::set_var(od::FLAG, "1");
}

#[tokio::test]
async fn every_route_needs_an_interactive_session() {
    let Some(owner) = pg_test_pool("org_deletion_routes::interactive").await else { return };
    on();
    let app = app_pool(&owner).await;
    let s = seed(&owner).await;
    let state = Arc::new(crate::test_app_state_owned_with_db_and_store(app, None));
    let ctx = || Extension(user_context(s.account, s.creator));

    let get = get_org_deletion(State(state.clone()), ctx(), None, Path(s.org.clone())).await;
    let post = post_org_deletion(
        State(state.clone()),
        ctx(),
        None,
        Path(s.org.clone()),
        Json(RequestBody { confirm_name: "x".into() }),
    )
    .await;
    let cancel = post_cancel_org_deletion(State(state.clone()), ctx(), None, Path(s.org.clone())).await;
    let export = get_org_deletion_export(State(state.clone()), ctx(), None, Path(s.org.clone())).await;
    // A session marker but a non-interactive request authority (#1473).
    let refused = with_request_authority_for_test(
        false,
        post_org_deletion(
            State(state.clone()),
            ctx(),
            fresh(),
            Path(s.org.clone()),
            Json(RequestBody { confirm_name: "x".into() }),
        ),
    )
    .await;

    for r in [get, post, cancel, export] {
        assert_eq!(r.status(), StatusCode::FORBIDDEN);
        assert_eq!(body_json(r).await["code"], "session_required");
    }
    assert_eq!(refused.status(), StatusCode::FORBIDDEN);
    assert_eq!(body_json(refused).await["code"], "interactive_session_required");
}

#[tokio::test]
async fn only_the_owner_acts_and_the_request_needs_a_fresh_verification() {
    let Some(owner) = pg_test_pool("org_deletion_routes::owner").await else { return };
    on();
    let app = app_pool(&owner).await;
    let s = seed(&owner).await;
    let (_, stranger) = seed_personal_account(&owner).await;
    let state = Arc::new(crate::test_app_state_owned_with_db_and_store(app, None));
    let body = || Json(RequestBody { confirm_name: "Acme".into() });

    let stale_owner = post_org_deletion(
        State(state.clone()),
        Extension(user_context(s.account, s.creator)),
        stale(),
        Path(s.org.clone()),
        body(),
    )
    .await;
    let member_post = post_org_deletion(
        State(state.clone()),
        Extension(user_context(s.account, s.member)),
        fresh(),
        Path(s.org.clone()),
        body(),
    )
    .await;
    let member_cancel =
        post_cancel_org_deletion(State(state.clone()), Extension(user_context(s.account, s.member)), fresh(), Path(s.org.clone()))
            .await;
    let member_export =
        get_org_deletion_export(State(state.clone()), Extension(user_context(s.account, s.member)), fresh(), Path(s.org.clone()))
            .await;
    let member_get =
        get_org_deletion(State(state.clone()), Extension(user_context(s.account, s.member)), fresh(), Path(s.org.clone())).await;
    let stranger_get =
        get_org_deletion(State(state.clone()), Extension(user_context(s.account, stranger)), fresh(), Path(s.org.clone())).await;
    let owner_get =
        get_org_deletion(State(state.clone()), Extension(user_context(s.account, s.creator)), fresh(), Path(s.org.clone())).await;
    // The owner, fresh and interactive, gets past every authorization check
    // to the org-name lookup (Clerk is not configured in this suite).
    let owner_post = post_org_deletion(
        State(state.clone()),
        Extension(user_context(s.account, s.creator)),
        fresh(),
        Path(s.org.clone()),
        body(),
    )
    .await;

    assert_eq!(stale_owner.status(), StatusCode::FORBIDDEN);
    assert_eq!(body_json(stale_owner).await["code"], "reverification_required");
    for r in [member_post, member_cancel, member_export] {
        assert_eq!(r.status(), StatusCode::FORBIDDEN);
        assert_eq!(body_json(r).await["code"], "not_owner");
    }
    assert_eq!(member_get.status(), StatusCode::OK);
    let mg = body_json(member_get).await;
    assert_eq!(mg["is_owner"], false);
    assert_eq!(mg["blocked_reason"], "not_owner");
    assert_eq!(stranger_get.status(), StatusCode::NOT_FOUND);
    let og = body_json(owner_get).await;
    assert_eq!(og["is_owner"], true);
    assert_eq!(og["eligible"], true);
    assert_eq!(og["deletion"], serde_json::Value::Null);
    assert!(
        !matches!(owner_post.status(), StatusCode::FORBIDDEN | StatusCode::NOT_FOUND),
        "the owner passes authorization: {}",
        owner_post.status()
    );
    let none: Option<od::OrgDeletion> = od::live_for_account(&owner, s.account).await.unwrap();
    assert!(none.is_none(), "nothing was requested");
}

// ── security-review fixes ────────────────────────────────────────────────

/// BLOCKER 1 (route half): the owner export needs the same fresh
/// re-verification as the request; only one export runs per org at a time;
/// and a member never gets one. (That the export's CONTENT is the owner's
/// own, on a pool that bypasses RLS, is `tests/org_destroy_pg.rs`.)
#[tokio::test]
async fn the_owner_export_needs_a_fresh_verification_and_runs_one_at_a_time() {
    let Some(owner) = pg_test_pool("org_deletion_routes::export_guard").await else { return };
    on();
    let app = app_pool(&owner).await;
    let s = seed(&owner).await;
    let state = Arc::new(crate::test_app_state_owned_with_db_and_store(app.clone(), None));
    od::insert(&app, s.account, &s.org, od::Initiator::Owner, Some(s.creator)).await.unwrap();
    let export = |who: Uuid, session| {
        let state = state.clone();
        let org = s.org.clone();
        let account = s.account;
        async move { get_org_deletion_export(State(state), Extension(user_context(account, who)), session, Path(org)).await }
    };

    let stale_owner = export(s.creator, stale()).await;
    let stale_member = export(s.member, stale()).await;
    // Another export of this org is in flight (the lock a running export holds).
    let held = od::ExportGuard::try_acquire(&app, &s.org).await.unwrap().expect("first export takes the lock");
    let second = od::ExportGuard::try_acquire(&app, &s.org).await.unwrap();
    let busy = export(s.creator, fresh()).await;
    let mut held = held;
    held.release().await;
    let again = od::ExportGuard::try_acquire(&app, &s.org).await.unwrap();

    assert_eq!(stale_owner.status(), StatusCode::FORBIDDEN);
    assert_eq!(body_json(stale_owner).await["code"], "reverification_required");
    assert_eq!(stale_member.status(), StatusCode::FORBIDDEN);
    assert_eq!(body_json(stale_member).await["code"], "reverification_required", "re-verification is checked before anything about the org is revealed");
    assert!(second.is_none(), "the lock is exclusive per org");
    assert_eq!(busy.status(), StatusCode::CONFLICT);
    assert_eq!(body_json(busy).await["code"], "export_in_progress");
    assert!(again.is_some(), "released on close");
    let mut again = again.unwrap();
    again.release().await;
}

/// LOW 14: who asked and what happened to the subscription are the owner's
/// to see, not a locked-out member's.
#[tokio::test]
async fn a_locked_out_member_does_not_see_who_asked_or_the_subscription_outcome() {
    let Some(owner) = pg_test_pool("org_deletion_routes::status_fields").await else { return };
    on();
    let app = app_pool(&owner).await;
    let s = seed(&owner).await;
    let state = Arc::new(crate::test_app_state_owned_with_db_and_store(app.clone(), None));
    let d = od::insert(&app, s.account, &s.org, od::Initiator::Unpaid, None).await.unwrap().deletion;
    sqlx::query("UPDATE org_deletions SET stripe_outcome = 'ended_now' WHERE id = $1").bind(d.id).execute(&owner).await.unwrap();
    let get = |who: Uuid| {
        let state = state.clone();
        let org = s.org.clone();
        let account = s.account;
        async move {
            body_json(get_org_deletion(State(state), Extension(user_context(account, who)), fresh(), Path(org)).await).await
        }
    };

    let as_member = get(s.member).await;
    let as_owner = get(s.creator).await;

    let m = &as_member["deletion"];
    assert_eq!(m["state"], "requested");
    assert!(m.get("initiated_by").is_none() && m.get("subscription").is_none(), "{m}");
    assert_eq!(m["can_cancel"], false);
    assert_eq!(m["export_available"], false);
    let o = &as_owner["deletion"];
    assert_eq!(o["initiated_by"], "unpaid");
    assert_eq!(o["subscription"], "ended_now");
    assert_eq!(o["export_available"], true);
}
