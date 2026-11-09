//! Real member-disconnect handler on the production coralswarm_app RLS role.
//! A BYPASSRLS fixture cannot catch an unscoped oauth_tokens lookup.

#[path = "support/financial_fixture.rs"]
mod financial_fixture;

mod support;

use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    routing::post,
    Extension, Json, Router,
};
use coralswarm_backend::tenancy::runtime::{TenancyDecision, TenantContext};
use coralswarm_backend::{delete_connection, tenancy::extractor::Tenant, UserContext};
use serde_json::json;
use sqlx::{postgres::PgPoolOptions, Connection, Executor, PgConnection};
use std::sync::{Arc, Mutex};
use support::rls_probe::RlsProbe;
use uuid::Uuid;

const WORKSPACE: &str = "T-own-disconnect";
const TEST_KEK_HEX: &str = "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef";

async fn seed_grant(
    probe: &RlsProbe,
    account: Uuid,
    owner: Option<Uuid>,
    token: &str,
    dek: &[u8; 32],
) {
    let encrypted = coralswarm_core::oauth_token::encrypt_token(dek, token).unwrap();
    let provider = if owner.is_some() {
        "slack_user"
    } else {
        "slack"
    };
    sqlx::query("INSERT INTO oauth_tokens(account_id, owner_user_id, provider, access_token, provider_workspace_id, scope) VALUES ($1,$2,$3,$4,$5,'channels:history,channels:read')")
        .bind(account).bind(owner).bind(provider).bind(encrypted).bind(WORKSPACE)
        .execute(&probe.owner).await.unwrap();
    sqlx::query("INSERT INTO connector_installs(account_id, provider, external_workspace_id, owner_user_id) VALUES ($1,'slack',$2,$3)")
        .bind(account).bind(WORKSPACE).bind(owner).execute(&probe.owner).await.unwrap();
    if let Some(user) = owner {
        sqlx::query("INSERT INTO slack_member_channels(account_id,user_id,workspace_id,channel_id) VALUES($1,$2,$3,'C-shared')")
            .bind(account).bind(user).bind(WORKSPACE).execute(&probe.owner).await.unwrap();
    }
}

#[tokio::test]
async fn own_slack_disconnect_succeeds_as_app_role_and_preserves_other_grants() {
    if std::env::var("ORGT_TEST_BASE_URL").is_err() {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    }
    run_disconnect_case(false).await;
    run_disconnect_case(true).await;
}

async fn run_disconnect_case(org: bool) {
    let db = if org {
        "orgt_slack_disconnect_org_rls"
    } else {
        "orgt_slack_disconnect_personal_rls"
    };
    let Some(probe) = RlsProbe::setup(db).await else {
        eprintln!("skipping own_slack_disconnect_succeeds_as_app_role_and_preserves_other_grants: set ORGT_TEST_BASE_URL");
        return;
    };
    // One connection makes the reset-empty session case deterministic.
    let app_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(probe.app_pool.connect_options().as_ref().clone())
        .await
        .unwrap();
    if org {
        sqlx::query("SELECT set_config('app.account_id', '', false)")
            .execute(&app_pool)
            .await
            .unwrap();
    }
    let role: String = sqlx::query_scalar("SELECT current_user")
        .fetch_one(&app_pool)
        .await
        .unwrap();
    assert_eq!(role, "coralswarm_app");
    let bypass: bool =
        sqlx::query_scalar("SELECT rolbypassrls FROM pg_roles WHERE rolname=current_user")
            .fetch_one(&app_pool)
            .await
            .unwrap();
    assert!(!bypass, "the endpoint must run without BYPASSRLS");

    // This binary has one test, so its mock provider and fixture KEK cannot
    // race another test's environment. No real Slack credential is used.
    unsafe {
        std::env::set_var("MASTER_KEK_HEX", TEST_KEK_HEX);
    }
    let kek: [u8; 32] = hex::decode(TEST_KEK_HEX).unwrap().try_into().unwrap();
    let dek = coralswarm_core::kek::generate_dek();
    let wrapped = coralswarm_core::kek::wrap_dek(&kek, &dek).unwrap();
    sqlx::query("UPDATE accounts SET dek_wrapped=$1 WHERE id=$2")
        .bind(wrapped)
        .bind(probe.a.account_id)
        .execute(&probe.owner)
        .await
        .unwrap();
    let tenant_fixture = if org {
        // Real tenant schema and a distinct DEK ensure org revocation cannot
        // accidentally decrypt through the consumer control-plane key.
        let base = std::env::var("ORGT_TEST_BASE_URL").unwrap();
        let mut admin = PgConnection::connect(&base).await.unwrap();
        let tenant_db = "orgt_slack_disconnect_tenant_rls";
        admin
            .execute(format!("DROP DATABASE IF EXISTS {tenant_db} WITH (FORCE)").as_str())
            .await
            .unwrap();
        admin
            .execute(format!("CREATE DATABASE {tenant_db}").as_str())
            .await
            .unwrap();
        let tenant_pool = PgPoolOptions::new()
            .connect_with(
                probe
                    .owner
                    .connect_options()
                    .as_ref()
                    .clone()
                    .database(tenant_db),
            )
            .await
            .unwrap();
        sqlx::migrate!("./migrations_tenant")
            .run(&tenant_pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO account_keys(account_id,dek_wrapped) VALUES($1,$2)")
            .bind(probe.a.account_id)
            .bind(coralswarm_core::kek::wrap_dek(&kek, &dek).unwrap())
            .execute(&tenant_pool)
            .await
            .unwrap();
        financial_fixture::execute(
            &probe.owner,
            probe.a.account_id,
            sqlx::query("UPDATE accounts SET owner_user_id=$1, clerk_org_id='org_disconnect_rls', dek_wrapped=$2 WHERE id=$3")
                .bind(probe.b.user_id).bind(coralswarm_core::kek::wrap_dek(&kek, &coralswarm_core::kek::generate_dek()).unwrap())
                .bind(probe.a.account_id),
        ).await.unwrap();
        Some((admin, tenant_pool, tenant_db))
    } else {
        None
    };
    let tier: Option<Uuid> = if org {
        Some(
            sqlx::query_scalar(
                "SELECT id FROM seat_tiers WHERE code='standard' AND is_active=true",
            )
            .fetch_one(&probe.owner)
            .await
            .unwrap(),
        )
    } else {
        None
    };
    sqlx::query("INSERT INTO account_members(account_id,user_id,role,seat_tier_id) VALUES($1,$2,$3,$4) ON CONFLICT(account_id,user_id) DO UPDATE SET role=excluded.role")
        .bind(probe.a.account_id).bind(probe.a.user_id).bind(if org { "member" } else { "owner" })
        .bind(tier)
        .execute(&probe.owner).await.unwrap();
    seed_grant(
        &probe,
        probe.a.account_id,
        Some(probe.a.user_id),
        "xoxp-own-fixture",
        &dek,
    )
    .await;
    seed_grant(
        &probe,
        probe.a.account_id,
        Some(probe.b.user_id),
        "xoxp-colleague-fixture",
        &dek,
    )
    .await;
    seed_grant(
        &probe,
        probe.a.account_id,
        None,
        "xoxb-workspace-fixture",
        &dek,
    )
    .await;
    seed_grant(
        &probe,
        probe.b.account_id,
        Some(probe.b.user_id),
        "xoxp-other-account-fixture",
        &dek,
    )
    .await;

    // Prove the historical bare read fails with the real strict policy,
    // both on a fresh session and after transaction-local scope reset.
    let bare_error = sqlx::query("SELECT id FROM oauth_tokens WHERE account_id=$1")
        .bind(probe.a.account_id)
        .fetch_all(&app_pool)
        .await
        .unwrap_err();
    assert_eq!(
        bare_error.as_database_error().unwrap().code().as_deref(),
        Some(if org { "22P02" } else { "42704" })
    );

    let revoked = Arc::new(Mutex::new(Vec::new()));
    let seen = revoked.clone();
    let mock = Router::new().route(
        "/auth.revoke",
        post(move |headers: HeaderMap| {
            let seen = seen.clone();
            async move {
                seen.lock().unwrap().push(
                    headers
                        .get("authorization")
                        .unwrap()
                        .to_str()
                        .unwrap()
                        .to_string(),
                );
                Json(json!({"ok":true,"revoked":true}))
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, mock).await.unwrap();
    });
    unsafe {
        std::env::set_var("SLACK_API_BASE", format!("http://{address}"));
    }

    let state = Arc::new(coralswarm_backend::test_app_state_owned_with_db(
        app_pool.clone(),
        None,
    ));
    let caller = UserContext {
        user_id: probe.a.user_id,
        account_id: probe.a.account_id,
        role: if org { "member" } else { "owner" }.to_string(),
        device_id: "fixture".to_string(),
    };
    let tenant = if let Some((_, pool, _)) = &tenant_fixture {
        Tenant {
            store: None,
            pool: Some(pool.clone()),
            decision: Some(TenancyDecision::Org(TenantContext {
                org_id: "org_disconnect_rls".to_string(),
                account_id: probe.a.account_id,
                user_id: probe.a.user_id,
                pool: pool.clone(),
                page_pool: None,
            })),
            class: coralswarm_backend::route_class::RouteClass::Other,
        }
    } else {
        Tenant::legacy(&state)
    };
    let response = delete_connection(
        State(state.clone()),
        tenant.clone(),
        Extension(caller.clone()),
        Path("slack_user".to_string()),
    )
    .await;
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "Confirm disconnect must succeed under production RLS instead of returning internal error"
    );
    assert_eq!(
        *revoked.lock().unwrap(),
        vec!["Bearer xoxp-own-fixture".to_string()],
        "only the caller's own credential is revoked"
    );

    let tokens: Vec<(Uuid, Option<Uuid>)> = sqlx::query_as("SELECT account_id, owner_user_id FROM oauth_tokens ORDER BY account_id, owner_user_id NULLS FIRST")
        .fetch_all(&probe.owner).await.unwrap();
    assert_eq!(tokens.len(), 3);
    assert!(!tokens.contains(&(probe.a.account_id, Some(probe.a.user_id))));
    assert!(tokens.contains(&(probe.a.account_id, Some(probe.b.user_id))));
    assert!(tokens.contains(&(probe.a.account_id, None)));
    assert!(tokens.contains(&(probe.b.account_id, Some(probe.b.user_id))));
    let installs: i64 = sqlx::query_scalar("SELECT count(*) FROM connector_installs")
        .fetch_one(&probe.owner)
        .await
        .unwrap();
    assert_eq!(installs, 3);
    let indexed: Vec<(Uuid, Uuid)> =
        sqlx::query_as("SELECT account_id,user_id FROM slack_member_channels")
            .fetch_all(&probe.owner)
            .await
            .unwrap();
    assert_eq!(indexed.len(), 2);
    assert!(!indexed.contains(&(probe.a.account_id, probe.a.user_id)));
    let audit: i64 = sqlx::query_scalar("SELECT count(*) FROM connector_audit_log WHERE account_id=$1 AND actor_user_id=$2 AND action='disconnect' AND provider='slack_user'")
        .bind(probe.a.account_id).bind(probe.a.user_id).fetch_one(&probe.owner).await.unwrap();
    assert_eq!(audit, 1, "the successful disconnect is durably audited");

    let response = delete_connection(
        State(state.clone()),
        tenant,
        Extension(caller),
        Path("slack_user".to_string()),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!(revoked.lock().unwrap().len(), 1);
    server.abort();
    app_pool.close().await;
    if let Some((mut admin, pool, db)) = tenant_fixture {
        pool.close().await;
        admin
            .execute(format!("DROP DATABASE {db} WITH (FORCE)").as_str())
            .await
            .unwrap();
    }
    probe.teardown().await;
}
