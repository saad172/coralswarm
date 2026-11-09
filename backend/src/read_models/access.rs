//! Eligibility is checked before stored GET/refresh and again before compute.
//! Cache keys separate data visibility; they do not confer admin authority.
use crate::{middleware::auth::UserContext, tenancy::extractor::Tenant};
use axum::response::Response;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum AccessPolicy {
    #[default]
    Member,
    /// A member page with optional administration facts, projected at read time.
    OptionalAdmin,
    /// Org plane, live account admin/owner, interactive Clerk credential.
    OrgAdmin,
    /// Personal usage is self-service; org usage has the OrgAdmin gate.
    Usage,
}
#[derive(Clone, Copy, Default)]
pub struct ReadAccess {
    pub admin_fields: bool,
    pub account_authority: bool,
}
impl AccessPolicy {
    pub(crate) async fn request(
        self,
        state: &crate::AppState,
        tenant: &Tenant,
        user: &UserContext,
    ) -> Result<ReadAccess, Response> {
        self.request_for_account(state, tenant, user, user.account_id)
            .await
    }
    /// The account is resolved from a verified graph/object anchor. Never use
    /// the active account's admin role to present another account's facts.
    pub(crate) async fn request_for_account(
        self,
        state: &crate::AppState,
        tenant: &Tenant,
        user: &UserContext,
        account: uuid::Uuid,
    ) -> Result<ReadAccess, Response> {
        if tenant
            .org_context()
            .is_some_and(|cx| cx.account_id != account)
        {
            use axum::response::IntoResponse;
            return Err((
                axum::http::StatusCode::FORBIDDEN,
                axum::Json(serde_json::json!({"error":"account mismatch"})),
            )
                .into_response());
        }
        match self {
            Self::Member => Ok(ReadAccess::default()),
            Self::OptionalAdmin => {
                let authority =
                    crate::routes::teams::has_account_authority(state, account, user.user_id)
                        .await
                        .map_err(|r| {
                            use axum::response::IntoResponse;
                            r.into_response()
                        })?;
                let teams = match tenant.org_context() {
                    Some(ctx) => {
                        crate::routes::teams::org_teams_active_for(state, &ctx.org_id).await
                    }
                    None => true,
                };
                Ok(ReadAccess {
                    admin_fields: authority && teams,
                    account_authority: authority,
                })
            }
            Self::Usage if tenant.org_context().is_none() && account == user.account_id => {
                Ok(ReadAccess::default())
            }
            Self::OrgAdmin | Self::Usage => {
                let ctx = crate::routes::org_invites::require_org_admin(tenant, user)?;
                if self == Self::OrgAdmin {
                    let allowed = match state.control_store() {
                        Some(store) => store
                            .account_is_real_org(ctx.account_id, user.user_id)
                            .await
                            .unwrap_or(false),
                        None => false,
                    };
                    if !allowed
                        || !crate::routes::teams::org_teams_active_for(state, &ctx.org_id).await
                    {
                        use axum::response::IntoResponse;
                        return Err((axum::http::StatusCode::FORBIDDEN,axum::Json(serde_json::json!({"error":"organization member administration unavailable"}))).into_response());
                    }
                }
                Ok(ReadAccess {
                    admin_fields: true,
                    account_authority: true,
                })
            }
        }
    }
    /// Workers have no HTTP credential. The enqueuer had to pass `request`;
    /// their current DB role is independently revalidated here. A worker never
    /// fabricates a UserContext or an interactive authority proof.
    pub(crate) async fn compute(
        self,
        tx: &mut sqlx::PgConnection,
        account: uuid::Uuid,
        actor: uuid::Uuid,
        org: Option<&str>,
    ) -> Result<(), super::Error> {
        if matches!(self, Self::Member | Self::OptionalAdmin)
            || (self == Self::Usage && org.is_none())
        {
            return Ok(());
        }
        let org = org.ok_or("model requires an organization workspace")?;
        let allowed: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM accounts a JOIN account_members m ON m.account_id=a.id \
             WHERE a.id=$1 AND m.user_id=$2 AND m.role IN ('admin','owner') AND a.clerk_org_id=$3 \
             AND NOT EXISTS(SELECT 1 FROM org_revocations r WHERE r.clerk_org_id=$3) \
             AND COALESCE(org_deletion_state_for_org($3),'') NOT IN ('requested','grace','awaiting_approval','destroying'))")
            .bind(account).bind(actor).bind(org).fetch_one(&mut *tx).await?;
        if self == Self::OrgAdmin {
            let real: bool = sqlx::query_scalar("SELECT NOT EXISTS(SELECT 1 FROM personal_orgs p JOIN users u ON u.clerk_user_id=p.clerk_user_id WHERE p.clerk_org_id=$1 AND u.id=$2)")
                .bind(org).bind(actor).fetch_one(&mut *tx).await?;
            let teams: Option<bool> = sqlx::query_scalar("SELECT enabled FROM org_connector_flags WHERE clerk_org_id=$1 AND provider='teams'")
                .bind(org).fetch_optional(&mut *tx).await?;
            if !real
                || !teams.unwrap_or_else(crate::workers::member_ocean_reconciler::org_teams_enabled)
                || !crate::workers::member_ocean_reconciler::member_oceans_enabled()
            {
                return Err("organization member administration unavailable".into());
            }
        }
        if !allowed {
            return Err("compute actor is no longer an eligible org admin".into());
        }
        Ok(())
    }
}
