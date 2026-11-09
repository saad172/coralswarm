//! Slack App Home review section (high-signal review, Stage 2).
//!
//! Review comes to where the user already is: the App Home shows the top
//! clarifications (closest to their auto-resolve deadline first — urgency is
//! the point) with one-tap Confirm / Not-right buttons, wired through the
//! existing interactivity endpoint (`routes::slack_interactivity`, action-id
//! namespace `cs_review_*`). The /review page stays the archive; this is the
//! venue.
//!
//! Reads follow `slack_onboarding::derive_status`'s convention: raw scoped
//! SQL against the resolved pools, one `begin_account_scoped` transaction
//! (RLS is enforcing on the consumer plane; inert-by-design on org tenants).
//!
//! The section is per VIEWER, not per account (settings 2.12). In an org
//! every ocean shares the account, so an account-wide read showed whoever
//! opened the tab the titles of every other member's private pending
//! decisions and suggested edges. [`review_items_for_viewer`] narrows both
//! reads to the oceans the viewer can read, the same
//! `ReachabilityResolver::readable_graph_ids` scope `GET /v1/candidates`
//! uses.

use chrono::{DateTime, Duration, Utc};
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::review_policy::REVIEW_DEADLINE_DAYS;
use crate::routes::slack::AccountPlane;
use crate::store::PostgresStore;
use crate::tenancy::reachability::ReachabilityResolver;
use crate::tenancy::runtime::{TenancyDecision, TenantContext};

/// How many clarifications the Home surfaces. Two: enough to act on, few
/// enough that acting feels finishable — the whole point of high-signal
/// review is that this list is tiny.
const TOP_N: i64 = 2;

/// One clarification the Home can render and resolve.
#[derive(Debug, Clone)]
pub enum Clarification {
    Edge {
        edge_id: Uuid,
        src_title: String,
        predicate: String,
        dst_title: String,
        confidence: f32,
        deadline_at: DateTime<Utc>,
    },
    Decision {
        node_id: Uuid,
        title: String,
        deadline_at: DateTime<Utc>,
    },
}

impl Clarification {
    fn deadline(&self) -> DateTime<Utc> {
        match self {
            Clarification::Edge { deadline_at, .. } => *deadline_at,
            Clarification::Decision { deadline_at, .. } => *deadline_at,
        }
    }
}

/// The App Home review section for one identified viewer: the top pending
/// clarifications in the oceans `viewer_user_id` can read.
///
/// `decision` is the viewer's tenancy decision ([`viewer_decision`]) and
/// `tenant_pool` the account's OWN plane (the control pool for a personal
/// account, the org's tenant database for an org). The resolver gets a store
/// over that same pool — what `tenant.store` is on an HTTP route — so this
/// never holds a handle to a plane the account does not live on. (The org
/// arm of `ReachabilityResolver` reads only the decision's tenant pool.)
pub async fn review_items_for_viewer(
    decision: Option<&TenancyDecision>,
    tenant_pool: &PgPool,
    account_id: Uuid,
    viewer_user_id: Uuid,
) -> Result<Vec<Clarification>, String> {
    let plane_store = PostgresStore::new(tenant_pool.clone());
    let readable =
        ReachabilityResolver::readable_graph_ids(decision, &plane_store, viewer_user_id)
            .await
            .map_err(|e| format!("slack review readable oceans: {e}"))?;
    top_clarifications(tenant_pool, account_id, &readable).await
}

/// The tenancy decision for a Slack-identified `user_id` acting on
/// `account_id`: `Org` (scoped to the account's tenant pool) for an org
/// account, `Personal` otherwise. A Slack event has no Clerk request, so this
/// is built from the resolved account plane rather than by the tenancy gate.
pub(crate) fn viewer_decision(
    plane: AccountPlane,
    clerk_org_id: Option<&str>,
    account_id: Uuid,
    user_id: Uuid,
    tenant_pool: &PgPool,
) -> TenancyDecision {
    match (plane, clerk_org_id) {
        (AccountPlane::Org, Some(org_id)) => TenancyDecision::Org(TenantContext {
            org_id: org_id.to_string(),
            account_id,
            user_id,
            pool: tenant_pool.clone(),
            page_pool: None,
        }),
        _ => TenancyDecision::Personal,
    }
}

/// Top pending clarifications in `readable_graph_ids` (which must already be
/// the viewer's read scope) within one account, deadline-ascending. An empty
/// allowlist renders nothing.
async fn top_clarifications(
    tenant_pool: &PgPool,
    account_id: Uuid,
    readable_graph_ids: &[Uuid],
) -> Result<Vec<Clarification>, String> {
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(tenant_pool, account_id)
        .await
        .map_err(|e| format!("slack review scope: {e}"))?;

    let edge_rows = sqlx::query(
        "SELECT e.edge_id, e.predicate, e.confidence, \
                COALESCE(e.queued_at, e.created_at) AS queued, \
                sn.title AS src_title, dn.title AS dst_title \
         FROM relationship_edge e \
         JOIN graphs g ON g.id = e.graph_id \
         JOIN nodes sn ON sn.node_id = e.src_node_id \
         JOIN nodes dn ON dn.node_id = e.dst_node_id \
         WHERE g.account_id = $1 AND g.id = ANY($3::uuid[]) AND e.status = 'suggested' \
         ORDER BY queued ASC LIMIT $2",
    )
    .bind(account_id)
    .bind(TOP_N)
    .bind(readable_graph_ids)
    .fetch_all(&mut *tx)
    .await
    .map_err(|e| format!("slack review edges: {e}"))?;

    let decision_rows = sqlx::query(
        "SELECT n.node_id, n.title, \
                COALESCE((n.body->>'pending_since')::timestamptz, n.updated_at) AS queued \
         FROM nodes n \
         JOIN graphs g ON g.id = n.graph_id \
         WHERE g.account_id = $1 AND g.id = ANY($3::uuid[]) AND n.type = 'decision' \
           AND n.body->>'review_status' = 'pending' \
         ORDER BY queued ASC LIMIT $2",
    )
    .bind(account_id)
    .bind(TOP_N)
    .bind(readable_graph_ids)
    .fetch_all(&mut *tx)
    .await
    .map_err(|e| format!("slack review decisions: {e}"))?;
    tx.rollback().await.ok();

    let deadline = |queued: DateTime<Utc>| queued + Duration::days(REVIEW_DEADLINE_DAYS);
    let mut out: Vec<Clarification> = Vec::new();
    for r in edge_rows {
        out.push(Clarification::Edge {
            edge_id: r.get("edge_id"),
            src_title: r.get("src_title"),
            predicate: r.get("predicate"),
            dst_title: r.get("dst_title"),
            confidence: r.get("confidence"),
            deadline_at: deadline(r.get("queued")),
        });
    }
    for r in decision_rows {
        out.push(Clarification::Decision {
            node_id: r.get("node_id"),
            title: r.get("title"),
            deadline_at: deadline(r.get("queued")),
        });
    }
    out.sort_by_key(Clarification::deadline);
    out.truncate(TOP_N as usize);
    Ok(out)
}

/// "in 3d" / "today" — mirrors the web card's deadline chip copy
/// (`reviewShared.relDeadline`), including its CEILING division: 4d23h reads
/// "in 5d", and anything not strictly in the future reads "today".
fn rel_deadline(deadline_at: DateTime<Utc>) -> String {
    let secs = (deadline_at - Utc::now()).num_seconds();
    if secs <= 0 {
        return "today".to_string();
    }
    let days = (secs + 86_399) / 86_400;
    format!("in {days}d")
}

/// Render the Home section blocks for `items` — empty input renders nothing
/// (no "inbox zero" placeholder; absence of asks IS the product working).
/// Button `value`s carry `{account_id, id}` as JSON — context only, never
/// authorization (the handler re-derives the account and membership
/// server-side; see `routes::slack_interactivity`'s `cs_review_*` arm).
pub fn render_review_section(account_id: Uuid, items: &[Clarification]) -> Vec<serde_json::Value> {
    if items.is_empty() {
        return Vec::new();
    }
    let mut blocks = vec![serde_json::json!({
        "type": "header",
        "text": { "type": "plain_text", "text": "Worth your judgment", "emoji": false }
    })];
    for item in items {
        let (text, confirm_action, reject_action, id) = match item {
            Clarification::Edge { edge_id, src_title, predicate, dst_title, confidence, deadline_at } => (
                format!(
                    "*{}* {} *{}*  ·  {}% confident  ·  auto-resolves {}",
                    src_title,
                    predicate.replace('_', " "),
                    dst_title,
                    (confidence * 100.0).round() as i64,
                    rel_deadline(*deadline_at),
                ),
                "cs_review_edge_confirm",
                "cs_review_edge_reject",
                edge_id.to_string(),
            ),
            Clarification::Decision { node_id, title, deadline_at } => (
                format!(
                    "Decision changed: *{}*  ·  auto-resolves {}",
                    title,
                    rel_deadline(*deadline_at),
                ),
                "cs_review_dec_confirm",
                "cs_review_dec_reject",
                node_id.to_string(),
            ),
        };
        let value = serde_json::json!({"account_id": account_id, "id": id}).to_string();
        blocks.push(serde_json::json!({
            "type": "section",
            "text": { "type": "mrkdwn", "text": text }
        }));
        blocks.push(serde_json::json!({
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "style": "primary",
                    "text": { "type": "plain_text", "text": "Confirm", "emoji": false },
                    "action_id": confirm_action,
                    "value": value
                },
                {
                    "type": "button",
                    "text": { "type": "plain_text", "text": "Not right", "emoji": false },
                    "action_id": reject_action,
                    "value": value
                }
            ]
        }));
    }
    blocks
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_empty_is_no_blocks() {
        assert!(render_review_section(Uuid::new_v4(), &[]).is_empty());
    }

    #[test]
    fn render_carries_action_ids_and_context_value() {
        let account = Uuid::new_v4();
        let edge = Uuid::new_v4();
        let items = vec![Clarification::Edge {
            edge_id: edge,
            src_title: "Taylor".into(),
            predicate: "expert_in".into(),
            dst_title: "financial projections".into(),
            confidence: 0.72,
            deadline_at: Utc::now() + Duration::days(3),
        }];
        let blocks = render_review_section(account, &items);
        // header + section + actions
        assert_eq!(blocks.len(), 3);
        let actions = &blocks[2]["elements"];
        assert_eq!(actions[0]["action_id"], "cs_review_edge_confirm");
        assert_eq!(actions[1]["action_id"], "cs_review_edge_reject");
        let value: serde_json::Value =
            serde_json::from_str(actions[0]["value"].as_str().unwrap()).unwrap();
        assert_eq!(value["account_id"], account.to_string());
        assert_eq!(value["id"], edge.to_string());
        // The section text leads with the claim and carries the deadline.
        let text = blocks[1]["text"]["text"].as_str().unwrap();
        assert!(text.contains("Taylor"), "{text}");
        assert!(text.contains("auto-resolves in 3d"), "{text}");
    }

    #[test]
    fn deadline_copy_floors_at_today() {
        assert_eq!(rel_deadline(Utc::now() - Duration::hours(1)), "today");
        assert_eq!(rel_deadline(Utc::now() + Duration::days(5)), "in 5d");
    }
}
