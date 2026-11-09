//! Snapshot roster query with the existing bounded Clerk enrichment.
use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use sqlx::{Postgres, Transaction};
use uuid::Uuid;
pub async fn roster(
    tx: &mut Transaction<'_, Postgres>,
    account: Uuid,
    clerk_org_id: &str,
) -> Result<Vec<Value>, crate::read_models::Error> {
    let rows: Result<
        Vec<(
            Uuid,
            Option<String>,
            String,
            String,
            Option<String>,
            Vec<String>,
            Option<DateTime<Utc>>,
            bool,
        )>,
        sqlx::Error,
    > = sqlx::query_as(
        "SELECT u.id, u.name, u.email, am.role, u.clerk_user_id, \
                COALESCE(( \
                    SELECT array_agg(DISTINCT t.provider ORDER BY t.provider) \
                    FROM oauth_tokens t \
                    WHERE t.account_id = am.account_id AND t.owner_user_id = am.user_id \
                ), ARRAY[]::text[]) AS connected_providers, \
                ( \
                    SELECT n.last_notified_at FROM slack_unmatched_email_notices n \
                    WHERE n.provider = 'member_connect' AND n.email = u.email \
                ) AS connect_nudged_at, \
                COALESCE(a.owner_user_id = am.user_id, false) AS is_owner \
         FROM account_members am \
         JOIN users u ON u.id = am.user_id \
         JOIN accounts a ON a.id = am.account_id \
         WHERE am.account_id = $1 \
         ORDER BY am.joined_at ASC",
    )
    .bind(account)
    .fetch_all(&mut **tx)
    .await;

    let rows = rows?;
    let caller_is_admin = true;

    // Best-effort Clerk enrichment — see this function's own doc comment.
    // `None` for either map means "Clerk lookup unavailable/failed/skipped
    // (non-admin caller)", which every member below reads as pending=null /
    // invite fields=null, not an error for the whole endpoint. Concurrent
    // (`tokio::join!`) over a dedicated short-timeout client (finding 8).
    let (last_sign_in_by_clerk_id, invite_meta_by_clerk_id): (
        Option<std::collections::HashMap<String, Option<i64>>>,
        Option<std::collections::HashMap<String, (Value, String)>>,
    ) = if !caller_is_admin {
        (None, None)
    } else {
        match crate::clerk_admin::ClerkAdminClient::from_env() {
            Some(clerk) => {
                let (users, memberships) = tokio::join!(
                    clerk.list_org_users(&clerk_org_id),
                    clerk.list_org_memberships(&clerk_org_id),
                );
                match (users, memberships) {
                    (Ok(users), Ok(memberships)) => {
                        let last_sign_in = users
                            .into_iter()
                            .map(|u| (u.id, u.last_sign_in_at))
                            .collect();
                        let meta = memberships
                            .into_iter()
                            .map(|m| (m.public_user_data.user_id, (m.public_metadata, m.role)))
                            .collect();
                        (Some(last_sign_in), Some(meta))
                    }
                    (users, memberships) => {
                        tracing::warn!(
                            clerk_org_id = %clerk_org_id,
                            users_ok = users.is_ok(), memberships_ok = memberships.is_ok(),
                            "list_org_members: Clerk enrichment failed — serving the roster with \
                             pending/invited_by/invited_at all null"
                        );
                        (None, None)
                    }
                }
            }
            None => (None, None),
        }
    };

    let members: Vec<Value> = rows
        .into_iter()
        .map(
            |(
                user_id,
                name,
                email,
                role,
                clerk_user_id,
                connected_providers,
                connect_nudged_at,
                is_owner,
            )| {
                if !caller_is_admin {
                    // Exact pre-org-invites-train shape — see this function's
                    // own doc comment, "Admin-only enrichment".
                    return json!({
                        "user_id": user_id,
                        "name":    name,
                        "email":   email,
                        "role":    role,
                    });
                }
                let pending = clerk_user_id.as_deref().and_then(|cid| {
                    last_sign_in_by_clerk_id
                        .as_ref()
                        .and_then(|m| m.get(cid))
                        .map(|last_sign_in| last_sign_in.is_none())
                });
                let (invited_by, invited_at) = clerk_user_id
                    .as_deref()
                    .and_then(|cid| invite_meta_by_clerk_id.as_ref().and_then(|m| m.get(cid)))
                    .map(|(metadata, _role)| {
                        let invited_by = match (
                            metadata.get("invited_by_user_id").and_then(|v| v.as_str()),
                            metadata.get("invited_by_name").and_then(|v| v.as_str()),
                        ) {
                            (Some(id), name) => Some(json!({"user_id": id, "name": name})),
                            (None, _) => None,
                        };
                        let invited_at = metadata
                            .get("invited_at")
                            .and_then(|v| v.as_str())
                            .map(str::to_string);
                        (invited_by, invited_at)
                    })
                    .unwrap_or((None, None));
                json!({
                    "user_id": user_id,
                    "name":    name,
                    "email":   email,
                    "role":    role,
                    "pending": pending,
                    "invited_by": invited_by,
                    "invited_at": invited_at,
                    "connected_providers": connected_providers,
                    "connect_nudged_at": connect_nudged_at,
                    // True for `accounts.owner_user_id`. The owner's `role` reads
                    // `admin`, so this is the only way for a client to tell that
                    // `DELETE`/`PATCH /v1/orgs/members/{user_id}` will refuse the row.
                    "is_owner": is_owner,
                })
            },
        )
        .collect();

    Ok(members)
}
