//! Production implementations of `account_deletion`'s two outward-facing
//! dependencies, kept apart so the job logic stays testable with fakes.

use std::sync::Arc;

use async_trait::async_trait;
use sqlx::PgPool;
use uuid::Uuid;

use crate::services::account_deletion::{
    blocking_team_orgs, ConnectorTeardown, Mail, Mailer, TenantPlane, UpstreamReport,
};
use crate::AppState;

/// Google's token revocation endpoint (public, takes the access or refresh
/// token). `GOOGLE_OAUTH_REVOKE_URL` exists only so a test can point at a mock.
fn google_revoke_url() -> String {
    std::env::var("GOOGLE_OAUTH_REVOKE_URL")
        .unwrap_or_else(|_| "https://oauth2.googleapis.com/revoke".to_string())
}

/// Best-effort upstream revoke for the grants this codebase has a revoke
/// primitive for:
///
/// * Slack (bot and member tokens): `auth.revoke`.
/// * Google (Gmail, Calendar, Chat, Meet): the OAuth revoke endpoint, for the
///   access and refresh token.
/// * Composio connected accounts: Composio's delete-with-revoke.
///
/// Microsoft delegated tokens and GitHub installations have no revoke call in
/// this codebase (the Microsoft/Google disconnect modules document the same
/// precedent): they are deleted locally and lapse on the provider's side. Every
/// failure is counted and logged, never raised.
pub struct LiveConnectors {
    pub http: reqwest::Client,
}

#[async_trait]
impl ConnectorTeardown for LiveConnectors {
    async fn revoke_upstream(&self, control: &PgPool, account_id: Uuid) -> UpstreamReport {
        let mut report = UpstreamReport::default();
        let dek = match crate::crypto::DekPlane::consumer(control.clone()).resolve(account_id).await {
            Ok(Some(dek)) => Some(dek),
            Ok(None) => None,
            Err(e) => {
                tracing::warn!(%account_id, error = %e, "account deletion: no DEK; upstream token revoke skipped");
                None
            }
        };
        let rows: Vec<(String, Vec<u8>, Option<Vec<u8>>)> = match async {
            let mut tx = crate::tenancy::scope::begin_account_scoped(control, account_id)
                .await
                .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
            let rows = sqlx::query_as(
                "SELECT provider, access_token, refresh_token FROM oauth_tokens WHERE account_id = $1",
            )
            .bind(account_id)
            .fetch_all(&mut *tx)
            .await?;
            tx.commit().await?;
            Ok::<_, sqlx::Error>(rows)
        }
        .await
        {
            Ok(rows) => rows,
            Err(e) => {
                tracing::warn!(%account_id, error = %e, "account deletion: could not read oauth tokens for upstream revoke");
                Vec::new()
            }
        };
        for (provider, access, refresh) in rows {
            let Some(dek) = dek.as_ref() else {
                report.skipped += 1;
                continue;
            };
            let decrypt = |bytes: &[u8]| coralswarm_core::oauth_token::decrypt_token(dek, bytes).ok();
            let Some(access_token) = decrypt(&access) else {
                report.skipped += 1;
                continue;
            };
            let outcome: Option<bool> = if provider == "slack" || provider == "slack_user" {
                Some(crate::oauth::slack::revoke_token(&self.http, &access_token).await.is_ok())
            } else if provider.starts_with("google") {
                let mut ok = self.revoke_google(&access_token).await;
                if let Some(refresh_token) = refresh.as_deref().and_then(decrypt) {
                    ok &= self.revoke_google(&refresh_token).await;
                }
                Some(ok)
            } else {
                None
            };
            match outcome {
                Some(true) => report.attempted += 1,
                Some(false) => {
                    report.attempted += 1;
                    report.failed += 1;
                }
                None => report.skipped += 1,
            }
        }
        // Composio connected accounts.
        let composio: Vec<String> = match async {
            let mut tx = crate::tenancy::scope::begin_account_scoped(control, account_id)
                .await
                .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
            let rows = sqlx::query_scalar(
                "SELECT connected_account_id FROM composio_connections WHERE account_id = $1 AND revoked_at IS NULL",
            )
            .bind(account_id)
            .fetch_all(&mut *tx)
            .await?;
            tx.commit().await?;
            Ok::<_, sqlx::Error>(rows)
        }
        .await
        {
            Ok(rows) => rows,
            Err(_) => Vec::new(),
        };
        if !composio.is_empty() {
            match crate::connectors::composio::Client::configured() {
                Ok(client) => {
                    for remote in composio {
                        report.attempted += 1;
                        if client.revoke(&remote).await.is_err() {
                            report.failed += 1;
                        }
                    }
                }
                Err(_) => report.skipped += composio.len(),
            }
        }
        report
    }
}

impl LiveConnectors {
    async fn revoke_google(&self, token: &str) -> bool {
        match self
            .http
            .post(google_revoke_url())
            .form(&[("token", token)])
            .timeout(std::time::Duration::from_secs(10))
            .send()
            .await
        {
            // 200 revoked; 400 invalid_token means it was already revoked or expired.
            Ok(r) => r.status().is_success() || r.status().as_u16() == 400,
            Err(_) => false,
        }
    }
}

/// Tenant-plane work, through the same runtime the request path uses.
pub struct LiveTenantPlane {
    pub state: Arc<AppState>,
}

#[async_trait]
impl TenantPlane for LiveTenantPlane {
    async fn detach_org_memberships(&self, user_id: Uuid, clerk_user_id: Option<&str>) -> Result<(), String> {
        let Some(pool) = self.state.control_db() else {
            return Err("control database unavailable".into());
        };
        let orgs = blocking_team_orgs(pool, user_id).await.map_err(|e| e.to_string())?;
        if orgs.is_empty() {
            return Ok(());
        }
        let Some(clerk_user_id) = clerk_user_id else {
            return Err("cannot detach org memberships without the Clerk user id".into());
        };
        for org in orgs {
            crate::clerk_provision::remove_org_membership(&self.state, pool, &org.clerk_org_id, clerk_user_id)
                .await
                .map_err(|e| format!("detach {}: {e}", org.clerk_org_id))?;
        }
        Ok(())
    }

    fn reconciler_flags(&self) -> (bool, bool) {
        (
            crate::workers::member_ocean_reconciler::member_oceans_enabled(),
            crate::workers::member_ocean_reconciler::member_ocean_purge_enabled(),
        )
    }

    async fn member_workspaces_remaining(&self, user_id: Uuid) -> Result<usize, String> {
        let Some(control) = self.state.control_db() else {
            return Err("control database unavailable".into());
        };
        let orgs: Vec<String> = sqlx::query_scalar(
            "SELECT clerk_org_id FROM member_ocean_desired_state WHERE user_id = $1 AND state <> 'purged'",
        )
        .bind(user_id)
        .fetch_all(control)
        .await
        .map_err(|e| e.to_string())?;
        if orgs.is_empty() {
            return Ok(0);
        }
        let Some(runtime) = self.state.tenant_runtime.as_ref() else {
            return Err("tenant runtime unavailable; member workspaces cannot be located".into());
        };
        let mut remaining = 0usize;
        for org in orgs {
            let revoked: bool =
                sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM org_revocations WHERE clerk_org_id = $1)")
                    .bind(&org)
                    .fetch_one(control)
                    .await
                    .map_err(|e| e.to_string())?;
            if revoked {
                // The organization was torn down wholesale; its databases are
                // handled by that teardown, not by one member's deletion.
                continue;
            }
            let Some(route) = runtime
                .resolve_routing_by_org(control, &org)
                .await
                .map_err(|e| e.to_string())?
            else {
                continue;
            };
            if !route.is_servable() {
                return Err(format!("org {org} database is not servable ({})", route.status));
            }
            let pool = runtime
                .registry
                .get_or_build(&org, route.account_id, &route.db_secret_ref)
                .await
                .map_err(|e| format!("org {org} pool: {e}"))?;
            let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(&pool, route.account_id)
                .await
                .map_err(|e| e.to_string())?;
            // Hand the workspace to the reconciler's audited purge: back-date
            // the archive so it is past the restore window.
            sqlx::query(
                "UPDATE graphs SET archived_at = LEAST(COALESCE(archived_at, now()), now() - interval '61 days') \
                  WHERE kind = 'member' AND owner_user_id = $1",
            )
            .bind(user_id)
            .execute(&mut *tx)
            .await
            .map_err(|e| e.to_string())?;
            let left: i64 =
                sqlx::query_scalar("SELECT count(*) FROM graphs WHERE kind = 'member' AND owner_user_id = $1")
                    .bind(user_id)
                    .fetch_one(&mut *tx)
                    .await
                    .map_err(|e| e.to_string())?;
            tx.commit().await.map_err(|e| e.to_string())?;
            remaining += left as usize;
        }
        Ok(remaining)
    }
}

/// Resend, the same provider and key the invite emails use. `RESEND_API_BASE_URL`
/// overrides the host for tests.
pub struct LiveMailer {
    pub http: reqwest::Client,
    pub resend_api_key: Option<String>,
}

#[async_trait]
impl Mailer for LiveMailer {
    async fn send(&self, mail: Mail) -> Result<(), String> {
        let Some(key) = self.resend_api_key.as_deref() else {
            return Err("RESEND_API_KEY is not configured".into());
        };
        let base = std::env::var("RESEND_API_BASE_URL").unwrap_or_else(|_| "https://api.resend.com".into());
        let payload = serde_json::json!({
            "from": "coralswarm <person@example.com>",
            "to": [mail.to],
            "subject": mail.subject,
            "text": mail.text,
        });
        let mut req = self.http.post(format!("{base}/emails")).bearer_auth(key).json(&payload);
        if let Some(k) = mail.idempotency_key.as_deref() {
            req = req.header("Idempotency-Key", k);
        }
        let send = req.send();
        match tokio::time::timeout(std::time::Duration::from_secs(10), send).await {
            Ok(Ok(r)) if r.status().is_success() => Ok(()),
            Ok(Ok(r)) => Err(format!("Resend answered {}", r.status())),
            Ok(Err(e)) => Err(format!("Resend transport error: {e}")),
            Err(_) => Err("Resend timed out".into()),
        }
    }
}
