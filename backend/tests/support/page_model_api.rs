//! Real Clerk JWT/auth/gate seam for L4 privilege regression coverage.
use super::fixture::Fixture;
use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
    Router,
};
use coralswarm_backend::{
    clerk::test_support::{mint, mint_oauth, test_jwks, ISSUER},
    tenancy::{registry::TenantRegistry, runtime::TenantRuntime, secrets::StaticMapResolver},
};
use serde_json::{json, Value};
use sqlx::ConnectOptions;
use std::sync::Arc;
use tower::ServiceExt;
pub struct Api {
    pub app: Router,
    pub admin: String,
    pub member: String,
    pub oauth: String,
}
impl Api {
    pub async fn new(f: &Fixture) -> Self { Self::build(f, true).await }
    /// Identical router/auth, without mutating the retained benchmark identity.
    pub async fn reopen_benchmark(f: &Fixture) -> Self { Self::build(f, false).await }
    async fn build(f: &Fixture, seed_identity: bool) -> Self {
        let body = test_jwks();
        let router = Router::new().route(
            "/jwks",
            axum::routing::get(move || {
                let body = body.clone();
                async move { axum::Json(serde_json::from_str::<Value>(&body).unwrap()) }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        std::env::set_var("CLERK_ISSUER", ISSUER);
        std::env::set_var("CLERK_JWKS_URL", format!("http://{addr}/jwks"));
        std::env::set_var(
            "MASTER_KEK_HEX",
            "3333333333333333333333333333333333333333333333333333333333333333",
        );
        std::env::set_var("REST_FOREIGN_OAUTH", "warn");
        std::env::set_var("MEMBER_OCEANS_ENABLED", "true");
        let sub = format!("l4_admin_{}", f.user.simple());
        let other = format!("l4_member_{}", f.other.simple());
        if seed_identity {
        for (id, sub) in [(f.user, &sub), (f.other, &other)] {
            sqlx::query("UPDATE users SET clerk_user_id=$2 WHERE id=$1")
                .bind(id)
                .bind(sub)
                .execute(&f.control)
                .await
                .unwrap();
        }
        sqlx::query("UPDATE account_members SET role='admin' WHERE account_id=$1 AND user_id=$2")
            .bind(f.account)
            .bind(f.user)
            .execute(&f.control)
            .await
            .unwrap();
        sqlx::query("INSERT INTO org_connector_flags(clerk_org_id,provider,enabled) VALUES('org_read_models','teams',true) ON CONFLICT(clerk_org_id,provider) DO UPDATE SET enabled=true").execute(&f.control).await.unwrap();
        if f.org {
            let mut tx = coralswarm_backend::tenancy::scope::begin_account_scoped(
                &f.control_owner,
                f.account,
            )
            .await
            .unwrap();
            coralswarm_backend::billing::renewal::lock_period_rotation(&mut tx, f.account)
                .await
                .unwrap();
            sqlx::query("INSERT INTO tenant_databases(clerk_org_id,account_id,slug,db_secret_ref,status) VALUES('org_read_models',$1,'l4fixture','L4_TEST_DSN','active')")
            .bind(f.account).execute(&mut *tx).await.unwrap();
            tx.commit().await.unwrap();
        }
        }
        let dsn = f.app.connect_options().to_url_lossy().to_string();
        let resolver = StaticMapResolver::from_pairs([("L4_TEST_DSN", dsn)]);
        let runtime = TenantRuntime::new(
            true,
            "coralswarm.example",
            Arc::new(TenantRegistry::new(Arc::new(resolver))),
        );
        let state = coralswarm_backend::test_app_state_owned_with_db_and_store(
            f.control.clone(),
            f.org.then_some(runtime),
        );
        let token = |subject: &str, sid: bool| {
            let mut v = json!({"iss":ISSUER,"sub":subject,"exp":(chrono::Utc::now().timestamp()+3600) as usize,"org_id":"org_read_models","org_role":"org:admin"});
            if !f.org {
                v.as_object_mut().unwrap().remove("org_id");
                v.as_object_mut().unwrap().remove("org_role");
            }
            if sid {
                v["sid"] = json!(format!("sess_{subject}"));
                mint(v)
            } else {
                mint_oauth(v)
            }
        };
        Self {
            app: coralswarm_backend::test_support_app(Arc::new(state)),
            admin: token(&sub, true),
            member: token(&other, true),
            oauth: token(&sub, false),
        }
    }
    pub async fn call(&self, method: &str, path: &str, token: &str) -> (StatusCode, Value) {
        self.call_json(method, path, token, None).await
    }
    pub async fn call_json(
        &self,
        method: &str,
        path: &str,
        token: &str,
        body: Option<Value>,
    ) -> (StatusCode, Value) {
        let (status, bytes) = self.call_body(method, path, token, body).await;
        (
            status,
            serde_json::from_slice(&bytes).unwrap_or_else(|_| panic!("non-json status={status}")),
        )
    }
    /// Complete authenticated handler/body cost, before test-client JSON decode.
    pub async fn call_body(
        &self,
        method: &str,
        path: &str,
        token: &str,
        body: Option<Value>,
    ) -> (StatusCode, Vec<u8>) {
        let mut builder = Request::builder()
            .method(method)
            .uri(path)
            .header("host", "coralswarm.example")
            .header("authorization", format!("Bearer {token}"));
        let body = match body {
            Some(value) => {
                builder = builder.header("content-type", "application/json");
                Body::from(serde_json::to_vec(&value).unwrap())
            }
            None => Body::empty(),
        };
        let req = builder.body(body).unwrap();
        let resp = self.app.clone().oneshot(req).await.unwrap();
        let status = resp.status();
        let bytes = to_bytes(resp.into_body(), 16 * 1024 * 1024).await.unwrap();
        (status, bytes.to_vec())
    }
}
impl Drop for Api {
    fn drop(&mut self) {
        for key in [
            "CLERK_ISSUER",
            "CLERK_JWKS_URL",
            "MASTER_KEK_HEX",
            "REST_FOREIGN_OAUTH",
            "MEMBER_OCEANS_ENABLED",
        ] {
            std::env::remove_var(key);
        }
    }
}
