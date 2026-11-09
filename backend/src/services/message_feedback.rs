use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use crate::{
    middleware::auth::UserContext,
    routes::conversations::{conversation_owner_account, readable_conversation, Unreadable},
    tenancy::{extractor::Tenant, scope::set_tenant_user},
};

pub const FLAG: &str = "MESSAGE_FEEDBACK_ENABLED";

pub fn enabled() -> bool {
    std::env::var(FLAG)
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

pub const NOTE_MAX_CHARS: usize = 2000;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Vote {
    Up,
    Down,
}

impl Vote {
    fn parse(s: &str) -> Option<Self> {
        match s {
            "up" => Some(Self::Up),
            "down" => Some(Self::Down),
            _ => None,
        }
    }

    fn to_db(self) -> i16 {
        match self {
            Self::Up => 1,
            Self::Down => -1,
        }
    }

    fn from_db(v: i16) -> Self {
        if v > 0 {
            Self::Up
        } else {
            Self::Down
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Reason {
    Wrong,
    MissingSource,
    Outdated,
    Other,
}

impl Reason {
    fn parse(s: &str) -> Option<Self> {
        match s {
            "wrong" => Some(Self::Wrong),
            "missing_source" => Some(Self::MissingSource),
            "outdated" => Some(Self::Outdated),
            "other" => Some(Self::Other),
            _ => None,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Wrong => "wrong",
            Self::MissingSource => "missing_source",
            Self::Outdated => "outdated",
            Self::Other => "other",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Feedback {
    vote: Vote,
    reasons: Vec<Reason>,
    note: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct FeedbackBody {
    pub vote: String,
    #[serde(default)]
    pub reasons: Vec<String>,
    #[serde(default)]
    pub note: Option<String>,
}

impl TryFrom<FeedbackBody> for Feedback {
    type Error = FeedbackError;

    fn try_from(body: FeedbackBody) -> Result<Self, FeedbackError> {
        let invalid = |m: String| Err(FeedbackError::Invalid(m));
        let Some(vote) = Vote::parse(&body.vote) else {
            return invalid(format!("vote must be \"up\" or \"down\", got {:?}", body.vote));
        };
        let mut reasons = Vec::with_capacity(body.reasons.len());
        for r in &body.reasons {
            match Reason::parse(r) {
                Some(reason) => reasons.push(reason),
                None => return invalid(format!(
                    "unknown reason {r:?}: expected wrong, missing_source, outdated or other"
                )),
            }
        }
        reasons.sort();
        reasons.dedup();
        if vote == Vote::Up && !reasons.is_empty() {
            return invalid("reasons apply only to a down vote".into());
        }
        let note = body.note.filter(|n| !n.trim().is_empty());
        if note.as_ref().is_some_and(|n| n.chars().count() > NOTE_MAX_CHARS) {
            return invalid(format!("note is longer than {NOTE_MAX_CHARS} characters"));
        }
        Ok(Self { vote, reasons, note })
    }
}

#[derive(Debug)]
pub enum FeedbackError {
    Invalid(String),
    NotFound,
    NotAnswer,
    Unavailable,
    Internal(String),
}

impl From<sqlx::Error> for FeedbackError {
    fn from(e: sqlx::Error) -> Self {
        Self::Internal(e.to_string())
    }
}

pub async fn upsert(
    tenant: &Tenant,
    user: &UserContext,
    conversation_id: Uuid,
    message_id: Uuid,
    feedback: &Feedback,
) -> Result<Feedback, FeedbackError> {
    let (mut tx, account_id) = open_answer(tenant, user, conversation_id, message_id).await?;
    let reasons: Vec<&str> = feedback.reasons.iter().map(|r| r.as_str()).collect();
    sqlx::query(
        "INSERT INTO message_feedback (message_id, user_id, account_id, vote, reasons, note)
         VALUES ($1, $2, $3, $4, $5, $6)
         ON CONFLICT (message_id, user_id) DO UPDATE
            SET vote = EXCLUDED.vote, reasons = EXCLUDED.reasons, note = EXCLUDED.note, updated_at = now()
          WHERE (message_feedback.vote, message_feedback.reasons, message_feedback.note)
                IS DISTINCT FROM (EXCLUDED.vote, EXCLUDED.reasons, EXCLUDED.note)",
    )
    .bind(message_id)
    .bind(user.user_id)
    .bind(account_id)
    .bind(feedback.vote.to_db())
    .bind(&reasons)
    .bind(&feedback.note)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(feedback.clone())
}

pub async fn clear(
    tenant: &Tenant,
    user: &UserContext,
    conversation_id: Uuid,
    message_id: Uuid,
) -> Result<(), FeedbackError> {
    let (mut tx, _) = open_answer(tenant, user, conversation_id, message_id).await?;
    sqlx::query("DELETE FROM message_feedback WHERE message_id = $1 AND user_id = $2")
        .bind(message_id)
        .bind(user.user_id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

pub async fn for_conversation(
    tx: &mut Transaction<'static, Postgres>,
    user_id: Uuid,
    message_ids: &[Uuid],
) -> Result<HashMap<Uuid, Feedback>, sqlx::Error> {
    let rows: Vec<(Uuid, i16, Vec<String>, Option<String>)> = sqlx::query_as(
        "SELECT f.message_id, f.vote, f.reasons, f.note
           FROM message_feedback f
          WHERE f.message_id = ANY($1) AND f.user_id = $2",
    )
    .bind(message_ids)
    .bind(user_id)
    .fetch_all(&mut **tx)
    .await?;
    Ok(rows
        .into_iter()
        .map(|(id, vote, reasons, note)| {
            let reasons = reasons.iter().filter_map(|r| Reason::parse(r)).collect();
            (id, Feedback { vote: Vote::from_db(vote), reasons, note })
        })
        .collect())
}

/// Only the requested, currently readable answer page and this verified actor.
/// One primary-key-bounded read in the caller's existing read-only transaction.
pub async fn for_visible_page(
    tx: &mut sqlx::PgConnection,
    account: Uuid,
    actor: Uuid,
    conversation: Uuid,
    graphs: &[Uuid],
    messages: &[Uuid],
) -> Result<HashMap<Uuid, Feedback>, sqlx::Error> {
    let rows: Vec<(Uuid, i16, Vec<String>, Option<String>)> = sqlx::query_as(
        "SELECT f.message_id, f.vote, f.reasons, f.note
         FROM message_feedback f
         JOIN conversation_messages m ON m.id=f.message_id
         JOIN conversations c ON c.id=m.conversation_id
         WHERE f.message_id=ANY($1) AND f.user_id=$2 AND f.account_id=$3
           AND c.id=$4 AND c.account_id=$3 AND c.graph_id=ANY($5)
           AND (c.user_id=$2 OR c.shared) AND m.role='assistant'",
    ).bind(messages).bind(actor).bind(account).bind(conversation).bind(graphs)
        .fetch_all(tx).await?;
    Ok(rows.into_iter().map(|(id, vote, reasons, note)| (id, Feedback {
        vote: Vote::from_db(vote),
        reasons: reasons.iter().filter_map(|r| Reason::parse(r)).collect(),
        note,
    })).collect())
}

async fn open_answer(
    tenant: &Tenant,
    user: &UserContext,
    conversation_id: Uuid,
    message_id: Uuid,
) -> Result<(Transaction<'static, Postgres>, Uuid), FeedbackError> {
    if tenant.pool.is_none() {
        return Err(FeedbackError::Unavailable);
    }
    let account_id = conversation_owner_account(tenant, conversation_id)
        .await
        .unwrap_or(user.account_id);
    let mut tx = tenant
        .begin_member_scoped(account_id)
        .await
        .map_err(|e| FeedbackError::Internal(e.to_string()))?;
    set_tenant_user(&mut tx, user.user_id)
        .await
        .map_err(|e| FeedbackError::Internal(e.to_string()))?;
    let mut scoped = Some(tx);
    let readable = readable_conversation(tenant, &mut scoped, conversation_id, user.user_id).await;
    let mut tx = scoped.expect("readable_conversation keeps the transaction");
    match readable {
        Ok(_) => {}
        Err(Unreadable::NotVisible | Unreadable::Forbidden) => return Err(FeedbackError::NotFound),
        Err(Unreadable::NoDatabase) => return Err(FeedbackError::Unavailable),
        Err(Unreadable::Store) => return Err(FeedbackError::Internal("conversation read".into())),
    }
    let role: Option<String> = sqlx::query_scalar(
        "SELECT role FROM conversation_messages WHERE id = $1 AND conversation_id = $2",
    )
    .bind(message_id)
    .bind(conversation_id)
    .fetch_optional(&mut *tx)
    .await?;
    match role.as_deref() {
        Some("assistant") => Ok((tx, account_id)),
        Some(_) => Err(FeedbackError::NotAnswer),
        None => Err(FeedbackError::NotFound),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn body(vote: &str, reasons: &[&str], note: Option<&str>) -> FeedbackBody {
        FeedbackBody {
            vote: vote.into(),
            reasons: reasons.iter().map(|r| r.to_string()).collect(),
            note: note.map(Into::into),
        }
    }

    #[test]
    fn message_feedback_parses_and_canonicalizes_reasons() {
        let fb = Feedback::try_from(body("down", &["outdated", "wrong", "outdated"], Some("stale"))).unwrap();
        assert_eq!(fb.vote, Vote::Down);
        assert_eq!(fb.reasons, vec![Reason::Wrong, Reason::Outdated]);
        assert_eq!(fb.note.as_deref(), Some("stale"));
        assert_eq!(
            serde_json::to_value(&fb).unwrap(),
            serde_json::json!({"vote": "down", "reasons": ["wrong", "outdated"], "note": "stale"})
        );
    }

    #[test]
    fn message_feedback_rejects_bad_bodies() {
        for b in [
            body("sideways", &[], None),
            body("down", &["bogus"], None),
            body("up", &["wrong"], None),
            body("down", &[], Some(&"x".repeat(NOTE_MAX_CHARS + 1))),
        ] {
            assert!(matches!(Feedback::try_from(b), Err(FeedbackError::Invalid(_))));
        }
    }

    #[test]
    fn message_feedback_note_limit_counts_characters_and_drops_blank() {
        let at_limit = "é".repeat(NOTE_MAX_CHARS);
        assert_eq!(Feedback::try_from(body("down", &[], Some(&at_limit))).unwrap().note, Some(at_limit));
        assert_eq!(Feedback::try_from(body("up", &[], Some("  "))).unwrap().note, None);
    }

    #[test]
    fn message_feedback_vote_round_trips_through_the_stored_smallint() {
        for v in [Vote::Up, Vote::Down] {
            assert_eq!(Vote::from_db(v.to_db()), v);
        }
    }
}
