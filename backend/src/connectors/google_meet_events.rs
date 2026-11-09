//! Meet transcript-ready triggers. Watch metadata is control-plane only;
//! transcript contents and each attendee's approval stay on their own plane.
use super::{google::google_send, ConnectorError};
use crate::tenancy::scope::begin_tenant_system_scoped;
use sqlx::PgPool;
use uuid::Uuid;

pub(crate) const EVENT_TYPE: &str = "google.workspace.meet.transcript.v2.fileGenerated";

fn subscription_body(topic: &str, sub: &str) -> serde_json::Value {
    serde_json::json!({
        "targetResource": format!("//cloudidentity.googleapis.com/users/{sub}"),
        "eventTypes": [EVENT_TYPE],
        "notificationEndpoint": {"pubsubTopic": topic},
        "ttl": "0s"
    })
}

pub(crate) async fn ensure_subscription(
    http: &reqwest::Client,
    pool: &PgPool,
    account: Uuid,
    owner: Uuid,
    sub: &str,
    token: &str,
) -> Result<(), ConnectorError> {
    if sub.is_empty() || sub.contains('/') || sub.chars().any(char::is_whitespace) {
        return Err(ConnectorError::Internal(
            "invalid Meet Google subject".into(),
        ));
    }
    if !super::google_chat::is_current_account_member(pool, account, owner).await? { return Ok(()); }
    let mut tx = begin_tenant_system_scoped(pool, account)
        .await
        .map_err(|e| ConnectorError::Internal(e.to_string()))?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1,0))")
        .bind(format!("meet_watch:{account}:{owner}"))
        .execute(&mut *tx)
        .await
        .map_err(|e| ConnectorError::Internal(e.to_string()))?;
    let grant_before: chrono::DateTime<chrono::Utc> = sqlx::query_scalar("SELECT now()")
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| ConnectorError::Internal(e.to_string()))?;
    let live: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM google_watch_channels WHERE account_id=$1 AND surface='meet' AND resource_id=$2 AND owner_user_id=$3 AND expires_at > now() + interval '1 hour')")
        .bind(account).bind(sub).bind(owner).fetch_one(&mut *tx).await.map_err(|e| ConnectorError::Internal(e.to_string()))?;
    if live {
        return Ok(());
    }
    let topic = std::env::var("GOOGLE_MEET_PUBSUB_TOPIC").map_err(|_| {
        ConnectorError::NotConfigured("GOOGLE_MEET_PUBSUB_TOPIC not configured".into())
    })?;
    let response = google_send(|| {
        http.post(format!(
            "{}/subscriptions",
            super::google_chat::workspace_events_base()
        ))
        .bearer_auth(token)
        .json(&subscription_body(&topic, sub))
    })
    .await?;
    let (name, expiry) = if response.status() == reqwest::StatusCode::CONFLICT {
        // Google keeps one subscription per (authorizing user, target
        // resource) per app: this Google user is already subscribed from
        // another account, so this account shares that subscription.
        find_existing_subscription(http, token, sub, &topic).await?
    } else if !response.status().is_success() {
        return Err(ConnectorError::upstream(
            response.status(),
            format!("Meet subscription create: {}", response.status()),
        ));
    } else {
        let operation: crate::workers::google_watch_renewal::Operation = response
            .json()
            .await
            .map_err(|e| ConnectorError::Exchange(e.to_string()))?;
        let operation =
            crate::workers::google_watch_renewal::poll_operation(http, token, operation).await?;
        super::google_chat::subscription_from_operation(&operation)?
    };
    // Fence connect/disconnect: never restore a watch after this exact grant
    // disappeared, and never attach a subscription to somebody else's token.
    let result = sqlx::query("INSERT INTO google_watch_channels(account_id,surface,resource_id,channel_id,owner_user_id,topic_name,expires_at) SELECT $1,'meet',$2,$3,$4,$5,$6 WHERE EXISTS(SELECT 1 FROM oauth_tokens WHERE account_id=$1 AND provider='google_meet' AND owner_user_id=$4 AND provider_user_id=$2 AND updated_at <= $7) AND EXISTS(SELECT 1 FROM account_members m WHERE m.account_id=$1 AND m.user_id=$4) ON CONFLICT(account_id,surface,resource_id) DO UPDATE SET channel_id=EXCLUDED.channel_id, owner_user_id=EXCLUDED.owner_user_id, topic_name=EXCLUDED.topic_name, expires_at=EXCLUDED.expires_at,last_error=NULL,last_error_at=NULL,updated_at=now() WHERE google_watch_channels.owner_user_id=EXCLUDED.owner_user_id")
        .bind(account).bind(sub).bind(&name).bind(owner).bind(topic).bind(expiry).bind(grant_before).execute(&mut *tx).await.map_err(|e| ConnectorError::Internal(e.to_string()))?;
    tx.commit()
        .await
        .map_err(|e| ConnectorError::Internal(e.to_string()))?;
    if result.rows_affected() == 0 && !referenced_by_another_watch(pool, &name, account, owner).await? {
        super::google_chat::delete_subscription_best_effort(http, token, &name).await;
    }
    Ok(())
}

fn target_resource(sub: &str) -> String {
    format!("//cloudidentity.googleapis.com/users/{sub}")
}

/// The subscription Google refused to duplicate: the user's own, for the
/// Meet transcript event type, delivering to our topic.
async fn find_existing_subscription(
    http: &reqwest::Client,
    token: &str,
    sub: &str,
    topic: &str,
) -> Result<(String, chrono::DateTime<chrono::Utc>), ConnectorError> {
    let filter = format!(
        "event_types:\"{EVENT_TYPE}\" AND target_resource=\"{}\"",
        target_resource(sub)
    );
    let response = google_send(|| {
        http.get(format!(
            "{}/subscriptions",
            super::google_chat::workspace_events_base()
        ))
        .bearer_auth(token)
        .query(&[("filter", filter.as_str()), ("pageSize", "100")])
    })
    .await?;
    if !response.status().is_success() {
        return Err(ConnectorError::upstream(
            response.status(),
            format!("Meet subscription list: {}", response.status()),
        ));
    }
    let body: serde_json::Value = response
        .json()
        .await
        .map_err(|e| ConnectorError::Exchange(e.to_string()))?;
    pick_shared_subscription(&body, sub, topic).ok_or_else(|| {
        ConnectorError::Exchange(
            "Meet subscription exists for this user but none delivers to our topic".into(),
        )
    })
}

fn pick_shared_subscription(
    list: &serde_json::Value,
    sub: &str,
    topic: &str,
) -> Option<(String, chrono::DateTime<chrono::Utc>)> {
    let target = target_resource(sub);
    list.get("subscriptions")?.as_array()?.iter().find_map(|s| {
        if s.get("targetResource")?.as_str()? != target
            || s.pointer("/notificationEndpoint/pubsubTopic")?.as_str()? != topic
            || s.get("state").and_then(|v| v.as_str()) == Some("DELETED")
        {
            return None;
        }
        let name = s.get("name")?.as_str()?.trim();
        if name.is_empty() {
            return None;
        }
        let expiry = chrono::DateTime::parse_from_rfc3339(s.get("expireTime")?.as_str()?)
            .ok()?
            .with_timezone(&chrono::Utc);
        Some((name.to_string(), expiry))
    })
}

/// Whether a watch row of another account or owner points at `name`. A
/// subscription one Google user shares across accounts must outlive any
/// single account's row.
pub(crate) async fn referenced_by_another_watch(
    pool: &PgPool,
    name: &str,
    account: Uuid,
    owner: Uuid,
) -> Result<bool, ConnectorError> {
    Ok(resolve_subscription(pool, name)
        .await?
        .into_iter()
        .any(|(_, a, o)| (a, o) != (account, owner)))
}

/// Only IDs leave the bootstrap resolver; re-read the watch scoped to its
/// account before loading credentials or processing a provider record.
pub(crate) async fn resolve_subscription(
    pool: &PgPool,
    name: &str,
) -> Result<Vec<(Uuid, Uuid, Uuid)>, ConnectorError> {
    sqlx::query_as("SELECT id,account_id,owner_user_id FROM resolve_google_meet_subscription($1)")
        .bind(name)
        .fetch_all(pool)
        .await
        .map_err(|e| ConnectorError::Internal(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn subscriptions_use_owned_user_and_transcript_readiness() {
        let body = subscription_body("projects/p/topics/meet-events", "123");
        assert_eq!(
            body["targetResource"],
            "//cloudidentity.googleapis.com/users/123"
        );
        assert_eq!(body["eventTypes"], serde_json::json!([EVENT_TYPE]));
        assert_eq!(body["ttl"], "0s");
        assert!(body.get("payloadOptions").is_none());
    }

    #[test]
    fn shared_subscription_is_the_users_live_one_on_our_topic() {
        let topic = "projects/p/topics/meet-events";
        let list = serde_json::json!({"subscriptions": [
            {"name": "subscriptions/other-user", "targetResource": "//cloudidentity.googleapis.com/users/999",
             "notificationEndpoint": {"pubsubTopic": topic}, "state": "ACTIVE", "expireTime": "2026-10-16T00:00:00Z"},
            {"name": "subscriptions/stale-topic", "targetResource": "//cloudidentity.googleapis.com/users/123",
             "notificationEndpoint": {"pubsubTopic": "projects/p/topics/old"}, "state": "ACTIVE", "expireTime": "2026-10-16T00:00:00Z"},
            {"name": "subscriptions/gone", "targetResource": "//cloudidentity.googleapis.com/users/123",
             "notificationEndpoint": {"pubsubTopic": topic}, "state": "DELETED", "expireTime": "2026-10-16T00:00:00Z"},
            {"name": "subscriptions/ours", "targetResource": "//cloudidentity.googleapis.com/users/123",
             "notificationEndpoint": {"pubsubTopic": topic}, "state": "ACTIVE", "expireTime": "2026-10-16T00:00:00Z"}
        ]});
        let (name, expiry) = pick_shared_subscription(&list, "123", topic).unwrap();
        assert_eq!(name, "subscriptions/ours");
        assert_eq!(expiry.to_rfc3339(), "2026-10-16T00:00:00+00:00");
        assert!(pick_shared_subscription(&list, "123", "projects/p/topics/none").is_none());
        assert!(pick_shared_subscription(&serde_json::json!({}), "123", topic).is_none());
    }
}
