//! Durable state for Outlook compose capture v2 (`outlook_schedule_intents`).
//!
//! An intent and the `teams_meeting_captures` row it governs live in the SAME tenant
//! database, so every state change here is ONE transaction that first locks the
//! intent row. That is what makes "a move never resurrects a user Stop" structural:
//! a follower can only touch a capture while its intent is still `pending_save` /
//! `active`, only updates a capture row in place while it is NOT revoked, and
//! treats a revoked capture it owns as the user's Stop.
use chrono::{DateTime, Duration, Utc};
use serde_json::{json, Value};
use sqlx::PgPool;
use uuid::Uuid;

use crate::{
    services::connector_audit::{self, Actor, AuditEntry},
    tenancy::scope::begin_tenant_scoped,
};

pub(crate) const MAX_PENDING: i64 = 10;
pub(crate) const MAX_OPEN: i64 = 50;
pub(crate) const MAX_CREATED_PER_HOUR: i64 = 30;
/// Maximum intents one member's worker pass handles.
pub(crate) const CLAIM_BATCH: i64 = 10;
/// Lease applied when an intent is claimed (the worker then sets the real time).
const CLAIM_LEASE_SECS: i64 = 120;

const COLS: &str = "id,account_id,user_id,nonce,tenant_id,aad_id,graph_id,status,error_code,created_at,expires_at,\
event_id,recurring,join_url,meeting_id,occurrence_start,capture_id,auto_transcription,manual_reason,gone_checks";

fn db(e: impl std::fmt::Display) -> sqlx::Error {
    sqlx::Error::Protocol(e.to_string())
}

#[derive(Clone, Debug, sqlx::FromRow)]
pub(crate) struct Intent {
    pub id: Uuid,
    pub account_id: Uuid,
    pub user_id: Uuid,
    pub nonce: Uuid,
    pub tenant_id: String,
    pub aad_id: String,
    pub graph_id: Uuid,
    pub status: String,
    pub error_code: Option<String>,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub event_id: Option<String>,
    pub recurring: bool,
    pub join_url: Option<String>,
    pub meeting_id: Option<String>,
    pub occurrence_start: Option<DateTime<Utc>>,
    pub capture_id: Option<Uuid>,
    pub auto_transcription: Option<String>,
    pub manual_reason: Option<String>,
    /// Consecutive "not found" reads of the tracked event (see `note_gone`).
    pub gone_checks: i32,
}

impl Intent {
    /// The public status document (see docs/plans/outlook-compose-capture-v2.md).
    pub(crate) fn view(&self) -> Value {
        let next = (self.status == "active")
            .then_some(self.occurrence_start)
            .flatten();
        json!({
            "status": self.status,
            "error_code": self.error_code,
            "recurring": self.recurring,
            "next_occurrence_start": next,
            "auto_transcription": self.auto_transcription,
            "manual_reason": self.manual_reason,
            "expires_at": self.expires_at,
        })
    }
}

pub(crate) enum CreateError {
    TooManyPending,
    TooManyOpen,
    RateLimited,
    Db(sqlx::Error),
}
impl From<sqlx::Error> for CreateError {
    fn from(e: sqlx::Error) -> Self {
        Self::Db(e)
    }
}

pub(crate) struct Created {
    pub id: Uuid,
    pub nonce: Uuid,
    pub expires_at: DateTime<Utc>,
}

pub(crate) async fn create(
    pool: &PgPool,
    account: Uuid,
    user: Uuid,
    tenant: &str,
    aad: &str,
    graph: Uuid,
) -> Result<Created, CreateError> {
    let mut tx = begin_tenant_scoped(pool, account, user).await.map_err(db)?;
    // Serialize the cap checks for this member.
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1,0))")
        .bind(format!("outlook-schedule/{account}/{user}"))
        .execute(&mut *tx)
        .await?;
    let (pending, open, recent): (i64, i64, i64) = sqlx::query_as(
        "SELECT count(*) FILTER (WHERE status='pending_save' AND expires_at>now()),
                count(*) FILTER (WHERE status IN ('pending_save','active')),
                count(*) FILTER (WHERE created_at>now()-interval '1 hour')
         FROM outlook_schedule_intents WHERE account_id=$1 AND user_id=$2",
    )
    .bind(account)
    .bind(user)
    .fetch_one(&mut *tx)
    .await?;
    if pending >= MAX_PENDING {
        return Err(CreateError::TooManyPending);
    }
    if open >= MAX_OPEN {
        return Err(CreateError::TooManyOpen);
    }
    if recent >= MAX_CREATED_PER_HOUR {
        return Err(CreateError::RateLimited);
    }
    let (id, nonce, expires_at): (Uuid, Uuid, DateTime<Utc>) = sqlx::query_as(
        "INSERT INTO outlook_schedule_intents(account_id,user_id,tenant_id,aad_id,graph_id)
         VALUES($1,$2,$3,$4,$5) RETURNING id,nonce,expires_at",
    )
    .bind(account)
    .bind(user)
    .bind(tenant)
    .bind(aad)
    .bind(graph)
    .fetch_one(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(Created { id, nonce, expires_at })
}

pub(crate) async fn get(
    pool: &PgPool,
    account: Uuid,
    user: Uuid,
    id: Uuid,
) -> sqlx::Result<Option<Intent>> {
    one(pool, account, user, "id=$3", id).await
}

pub(crate) async fn by_nonce(
    pool: &PgPool,
    account: Uuid,
    user: Uuid,
    nonce: Uuid,
) -> sqlx::Result<Option<Intent>> {
    one(pool, account, user, "nonce=$3", nonce).await
}

async fn one(
    pool: &PgPool,
    account: Uuid,
    user: Uuid,
    predicate: &str,
    key: Uuid,
) -> sqlx::Result<Option<Intent>> {
    let mut tx = crate::tenancy::scope::begin_request_tenant_read(pool, account, user).await.map_err(db)?;
    let row = sqlx::query_as::<_, Intent>(&format!(
        "SELECT {COLS} FROM outlook_schedule_intents WHERE account_id=$1 AND user_id=$2 AND {predicate}"
    ))
    .bind(account)
    .bind(user)
    .bind(key)
    .fetch_optional(&mut *tx)
    .await?;
    tx.rollback().await?;
    Ok(row)
}

/// Most recent intent that tracked this (immutable) event id.
pub(crate) async fn by_event(
    pool: &PgPool,
    account: Uuid,
    user: Uuid,
    event_id: &str,
) -> sqlx::Result<Option<Intent>> {
    let mut tx = crate::tenancy::scope::begin_request_tenant_read(pool, account, user).await.map_err(db)?;
    let row = sqlx::query_as::<_, Intent>(&format!(
        "SELECT {COLS} FROM outlook_schedule_intents WHERE account_id=$1 AND user_id=$2 AND event_id=$3
         ORDER BY (status='active') DESC, created_at DESC LIMIT 1"
    ))
    .bind(account)
    .bind(user)
    .bind(event_id)
    .fetch_optional(&mut *tx)
    .await?;
    tx.rollback().await?;
    Ok(row)
}

async fn audit(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    account: Uuid,
    user: Uuid,
    action: &'static str,
    capture: Uuid,
    detail: Value,
) -> sqlx::Result<()> {
    connector_audit::record_in_tx(
        tx,
        AuditEntry {
            account_id: account,
            actor: Actor::User(user),
            action: action.into(),
            provider: Some("microsoft".into()),
            subject: Some(capture.to_string()),
            detail: Some(detail),
        },
    )
    .await
}

/// Revoke a capture (sets the Stop barrier through the table trigger).
async fn revoke_capture(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    intent: &Intent,
    capture: Uuid,
    reason: &str,
) -> sqlx::Result<()> {
    let revoked: Option<String> = sqlx::query_scalar(
        "UPDATE teams_meeting_captures SET revoked_at=now() WHERE id=$1 AND account_id=$2 AND user_id=$3 AND revoked_at IS NULL RETURNING meeting_id",
    )
    .bind(capture)
    .bind(intent.account_id)
    .bind(intent.user_id)
    .fetch_optional(&mut **tx)
    .await?;
    if let Some(meeting) = revoked {
        audit(
            tx,
            intent.account_id,
            intent.user_id,
            "disconnect",
            capture,
            json!({"operation":"meeting_transcript_capture","source":"outlook_schedule","reason":reason,"meeting_id":meeting,"intent_id":intent.id}),
        )
        .await?;
    }
    Ok(())
}

async fn lock_open(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    account: Uuid,
    user: Uuid,
    id: Uuid,
) -> sqlx::Result<Option<Intent>> {
    let row = sqlx::query_as::<_, Intent>(&format!(
        "SELECT {COLS} FROM outlook_schedule_intents WHERE id=$1 AND account_id=$2 AND user_id=$3 FOR UPDATE"
    ))
    .bind(id)
    .bind(account)
    .bind(user)
    .fetch_optional(&mut **tx)
    .await?;
    Ok(row)
}

fn is_open(i: &Intent) -> bool {
    matches!(i.status.as_str(), "pending_save" | "active")
}

/// The user's Stop. Idempotent; stopping an unknown intent returns `false`.
pub(crate) async fn stop_by_user(
    pool: &PgPool,
    account: Uuid,
    user: Uuid,
    id: Uuid,
) -> sqlx::Result<bool> {
    let mut tx = begin_tenant_scoped(pool, account, user).await.map_err(db)?;
    let Some(intent) = lock_open(&mut tx, account, user, id).await? else {
        tx.rollback().await?;
        return Ok(false);
    };
    if is_open(&intent) {
        if let Some(capture) = intent.capture_id {
            revoke_capture(&mut tx, &intent, capture, "user_stop").await?;
        }
        sqlx::query(
            "UPDATE outlook_schedule_intents SET status='stopped',error_code=NULL,stopped_at=now(),updated_at=now() WHERE id=$1",
        )
        .bind(id)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    Ok(true)
}

/// Why an intent is being closed by the worker.
pub(crate) struct Close<'a> {
    pub status: &'static str,
    pub code: Option<&'a str>,
    /// Revoke the capture it owns (cancel / delete / series end). `false` for natural completion.
    pub revoke: bool,
}

/// Close an open intent (worker side). `false` when it was no longer open.
pub(crate) async fn close(
    pool: &PgPool,
    account: Uuid,
    user: Uuid,
    id: Uuid,
    how: Close<'_>,
) -> sqlx::Result<bool> {
    let mut tx = begin_tenant_scoped(pool, account, user).await.map_err(db)?;
    let Some(intent) = lock_open(&mut tx, account, user, id).await?.filter(is_open) else {
        tx.rollback().await?;
        return Ok(false);
    };
    if how.revoke {
        if let Some(capture) = intent.capture_id {
            revoke_capture(&mut tx, &intent, capture, how.code.unwrap_or("closed")).await?;
        }
    }
    sqlx::query(
        "UPDATE outlook_schedule_intents SET status=$2,error_code=$3,stopped_at=now(),updated_at=now() WHERE id=$1",
    )
    .bind(id)
    .bind(how.status)
    .bind(how.code)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(true)
}

/// How many consecutive 404s, spanning at least how long, confirm a deleted event.
pub(crate) const GONE_CONFIRM_CHECKS: i32 = 3;
pub(crate) const GONE_CONFIRM_SPAN_MINUTES: i32 = 5;
pub(crate) const GONE_RETRY_SECS: i32 = 120;

/// Record one 404 on the tracked event and look again soon. Returns `Some(true)` once the
/// 404 has persisted across enough checks and time (DB clock), `Some(false)` while still
/// unconfirmed, `None` if the intent is no longer active (a Stop wins).
pub(crate) async fn note_gone(
    pool: &PgPool,
    account: Uuid,
    user: Uuid,
    id: Uuid,
) -> sqlx::Result<Option<bool>> {
    let mut tx = begin_tenant_scoped(pool, account, user).await.map_err(db)?;
    let row: Option<(i32, bool)> = sqlx::query_as(
        "UPDATE outlook_schedule_intents SET gone_checks=gone_checks+1,
           gone_since=COALESCE(gone_since,now()),
           next_check_at=now()+make_interval(secs=>$4),updated_at=now()
         WHERE id=$1 AND account_id=$2 AND user_id=$3 AND status='active'
         RETURNING gone_checks, (now()-gone_since) >= make_interval(mins=>$5)",
    )
    .bind(id)
    .bind(account)
    .bind(user)
    .bind(GONE_RETRY_SECS as f64)
    .bind(GONE_CONFIRM_SPAN_MINUTES)
    .fetch_optional(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(row.map(|(checks, spanned)| checks >= GONE_CONFIRM_CHECKS && spanned))
}

/// Any successful read of the event: forget earlier 404s.
pub(crate) async fn clear_gone(pool: &PgPool, account: Uuid, user: Uuid, id: Uuid) -> sqlx::Result<()> {
    let mut tx = begin_tenant_scoped(pool, account, user).await.map_err(db)?;
    sqlx::query(
        "UPDATE outlook_schedule_intents SET gone_checks=0,gone_since=NULL
         WHERE id=$1 AND account_id=$2 AND user_id=$3 AND status IN ('pending_save','active') AND gone_checks>0",
    )
    .bind(id)
    .bind(account)
    .bind(user)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(())
}

/// Reschedule an open intent's next check and set/clear its advisory error code.
pub(crate) async fn set_check(
    pool: &PgPool,
    account: Uuid,
    user: Uuid,
    id: Uuid,
    next: DateTime<Utc>,
    code: Option<&str>,
) -> sqlx::Result<()> {
    let mut tx = begin_tenant_scoped(pool, account, user).await.map_err(db)?;
    sqlx::query(
        "UPDATE outlook_schedule_intents SET next_check_at=$2,error_code=$3,updated_at=now()
         WHERE id=$1 AND status IN ('pending_save','active')",
    )
    .bind(id)
    .bind(next)
    .bind(code)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(())
}

pub(crate) async fn record_transcription(
    pool: &PgPool,
    account: Uuid,
    user: Uuid,
    id: Uuid,
    auto: &str,
    reason: Option<&str>,
) -> sqlx::Result<()> {
    let mut tx = begin_tenant_scoped(pool, account, user).await.map_err(db)?;
    sqlx::query(
        "UPDATE outlook_schedule_intents SET auto_transcription=$2,manual_reason=$3,updated_at=now()
         WHERE id=$1 AND status='active'",
    )
    .bind(id)
    .bind(auto)
    .bind(reason)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(())
}

/// What the follower wants the capture to look like right now.
pub(crate) struct Plan {
    pub event_id: String,
    pub recurring: bool,
    pub join_url: String,
    /// Graph onlineMeeting id of the Teams meeting.
    pub meeting_id: String,
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    /// Only for an occurrence of a series (see `Meeting::transcript_end`).
    pub transcript_end: Option<DateTime<Utc>>,
    pub next_check_at: DateTime<Utc>,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Commit {
    /// Tracking is in place. `meeting_changed` = a different onlineMeeting than before
    /// (or first activation): transcription must be (re)attempted.
    Tracking {
        capture_id: Uuid,
        meeting_changed: bool,
    },
    /// The user stopped the capture (from the intent or from the Teams pane).
    UserStopped,
    /// The intent was closed or expired meanwhile; nothing was changed.
    NotOpen,
    /// A capture for that meeting already exists under another ocean.
    Conflict,
}

/// Apply a [`Plan`] atomically. See the module docs for the Stop rules.
pub(crate) async fn commit_tracking(
    pool: &PgPool,
    account: Uuid,
    user: Uuid,
    id: Uuid,
    plan: &Plan,
) -> sqlx::Result<Commit> {
    let mut tx = begin_tenant_scoped(pool, account, user).await.map_err(db)?;
    let Some(intent) = lock_open(&mut tx, account, user, id).await?.filter(is_open) else {
        tx.rollback().await?;
        return Ok(Commit::NotOpen);
    };
    if intent.status == "pending_save" {
        // Lifetime is judged on the tenant clock.
        let expired: bool = sqlx::query_scalar("SELECT $1::timestamptz <= now()")
            .bind(intent.expires_at)
            .fetch_one(&mut *tx)
            .await?;
        if expired {
            sqlx::query("UPDATE outlook_schedule_intents SET status='expired',updated_at=now() WHERE id=$1")
                .bind(id)
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
            return Ok(Commit::NotOpen);
        }
    }

    // 1. Following a changed Teams link: leave the old meeting behind.
    let mut meeting_changed = intent.meeting_id.as_deref() != Some(plan.meeting_id.as_str());
    if let Some(cid) = intent.capture_id {
        let row: Option<(String, bool)> = sqlx::query_as(
            "SELECT meeting_id, revoked_at IS NOT NULL FROM teams_meeting_captures
             WHERE id=$1 AND account_id=$2 AND user_id=$3 FOR UPDATE",
        )
        .bind(cid)
        .bind(account)
        .bind(user)
        .fetch_optional(&mut *tx)
        .await?;
        match row {
            // Revoked or gone while we still owned it: the user's Stop wins, always.
            None | Some((_, true)) => {
                sqlx::query("UPDATE outlook_schedule_intents SET status='stopped',error_code='capture_stopped',stopped_at=now(),updated_at=now() WHERE id=$1")
                    .bind(id)
                    .execute(&mut *tx)
                    .await?;
                tx.commit().await?;
                return Ok(Commit::UserStopped);
            }
            Some((old_meeting, false)) if old_meeting != plan.meeting_id => {
                revoke_capture(&mut tx, &intent, cid, "followed_new_teams_link").await?;
                meeting_changed = true;
            }
            Some(_) => {}
        }
    }

    // 2. The same event tagged again: the newest intent supersedes older live ones.
    let older: Vec<(Uuid, Option<Uuid>)> = sqlx::query_as(
        "UPDATE outlook_schedule_intents SET status='stopped',error_code='superseded',stopped_at=now(),updated_at=now()
         WHERE account_id=$1 AND user_id=$2 AND event_id=$3 AND id<>$4 AND status='active'
         RETURNING id, capture_id",
    )
    .bind(account)
    .bind(user)
    .bind(&plan.event_id)
    .bind(id)
    .fetch_all(&mut *tx)
    .await?;
    for (_, capture) in &older {
        if let Some(cid) = capture {
            // Hand a same-meeting capture over; revoke one for a different meeting.
            let other_meeting: Option<String> = sqlx::query_scalar(
                "SELECT meeting_id FROM teams_meeting_captures WHERE id=$1 AND account_id=$2 AND user_id=$3",
            )
            .bind(cid)
            .bind(account)
            .bind(user)
            .fetch_optional(&mut *tx)
            .await?;
            if other_meeting.is_some_and(|m| m != plan.meeting_id) {
                revoke_capture(&mut tx, &intent, *cid, "superseded").await?;
            }
        }
    }

    // 3. Upsert the capture for the followed meeting.
    struct Row {
        id: Uuid,
        revoked: bool,
        after_stop: bool,
        graph: Uuid,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        transcript_end: Option<DateTime<Utc>>,
    }
    let existing: Option<(Uuid, bool, bool, Uuid, DateTime<Utc>, DateTime<Utc>, Option<DateTime<Utc>>)> =
        sqlx::query_as(
            "SELECT id, revoked_at IS NOT NULL,
                    $5::timestamptz > COALESCE(last_stopped_at, revoked_at, '-infinity'::timestamptz),
                    graph_id, occurrence_start, occurrence_end, transcript_end
             FROM teams_meeting_captures
             WHERE account_id=$1 AND user_id=$2 AND tenant_id=$3 AND meeting_id=$4 FOR UPDATE",
        )
        .bind(account)
        .bind(user)
        .bind(&intent.tenant_id)
        .bind(&plan.meeting_id)
        .bind(intent.created_at)
        .fetch_optional(&mut *tx)
        .await?;
    let existing = existing.map(|(id, revoked, after_stop, graph, start, end, transcript_end)| Row {
        id,
        revoked,
        after_stop,
        graph,
        start,
        end,
        transcript_end,
    });
    let capture_id = match existing {
        None => {
            let cid: Uuid = sqlx::query_scalar(
                "INSERT INTO teams_meeting_captures(account_id,user_id,tenant_id,meeting_id,graph_id,source,occurrence_start,occurrence_end,aad_id,transcript_end)
                 VALUES($1,$2,$3,$4,$5,'meeting_addin',$6,$7,$8,$9) RETURNING id",
            )
            .bind(account)
            .bind(user)
            .bind(&intent.tenant_id)
            .bind(&plan.meeting_id)
            .bind(intent.graph_id)
            .bind(plan.start)
            .bind(plan.end)
            .bind(&intent.aad_id)
            .bind(plan.transcript_end)
            .fetch_one(&mut *tx)
            .await?;
            audit(&mut tx, account, user, "consent", cid, consent_detail(&intent, plan)).await?;
            cid
        }
        Some(row) if row.graph != intent.graph_id => {
            tx.rollback().await?;
            return Ok(Commit::Conflict);
        }
        Some(row) if row.revoked => {
            // Someone's earlier Stop. Only a deliberate intent created AFTER it may
            // re-enable; our own stopped capture was handled in step 1.
            if !row.after_stop {
                sqlx::query("UPDATE outlook_schedule_intents SET status='stopped',error_code='capture_stopped',stopped_at=now(),updated_at=now() WHERE id=$1")
                    .bind(id)
                    .execute(&mut *tx)
                    .await?;
                tx.commit().await?;
                return Ok(Commit::UserStopped);
            }
            reset_capture(&mut tx, &intent, row.id, plan, true).await?;
            audit(&mut tx, account, user, "consent", row.id, consent_detail(&intent, plan)).await?;
            row.id
        }
        Some(row) => {
            // Compare the values SQLx actually stores, not provider nanoseconds
            // that are discarded by PostgreSQL's microsecond representation.
            let same = postgres_timestamp_micros(row.start) == postgres_timestamp_micros(plan.start)
                && postgres_timestamp_micros(row.end) == postgres_timestamp_micros(plan.end)
                && row.transcript_end.map(postgres_timestamp_micros)
                    == plan.transcript_end.map(postgres_timestamp_micros);
            if !same {
                reset_capture(&mut tx, &intent, row.id, plan, false).await?;
                audit(&mut tx, account, user, "consent", row.id, consent_detail(&intent, plan)).await?;
            }
            row.id
        }
    };

    // 4. The intent now tracks it.
    sqlx::query(
        "UPDATE outlook_schedule_intents SET status='active',error_code=NULL,event_id=$2,recurring=$3,join_url=$4,
         meeting_id=$5,occurrence_start=$6,occurrence_end=$7,capture_id=$8,next_check_at=$9,updated_at=now(),gone_checks=0,gone_since=NULL,
         auto_transcription=CASE WHEN $10 THEN NULL ELSE auto_transcription END,
         manual_reason=CASE WHEN $10 THEN NULL ELSE manual_reason END
         WHERE id=$1",
    )
    .bind(id)
    .bind(&plan.event_id)
    .bind(plan.recurring)
    .bind(&plan.join_url)
    .bind(&plan.meeting_id)
    .bind(plan.start)
    .bind(plan.end)
    .bind(capture_id)
    .bind(plan.next_check_at)
    .bind(meeting_changed)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(Commit::Tracking {
        capture_id,
        meeting_changed,
    })
}

/// Mirrors SQLx's chrono timestamp encoding: signed whole microseconds since
/// PostgreSQL's 2000-01-01 epoch. Using that epoch also matches truncation for
/// dates before 2000; Unix-epoch floor rounding would differ there. Bind/write
/// behavior is unchanged — SQLx already applies this precision on every write.
fn postgres_timestamp_micros(value: DateTime<Utc>) -> Option<i64> {
    let epoch = DateTime::from_timestamp(946_684_800, 0).expect("valid PostgreSQL epoch");
    value.signed_duration_since(epoch).num_microseconds()
}

#[cfg(test)]
mod timestamp_precision_tests {
    use super::*;

    #[test]
    fn comparisons_preserve_changes_at_storage_precision() {
        let raw = DateTime::parse_from_rfc3339("2026-10-09T12:00:00.123456789Z").unwrap().with_timezone(&Utc);
        let stored = DateTime::parse_from_rfc3339("2026-10-09T12:00:00.123456Z").unwrap().with_timezone(&Utc);
        assert_eq!(postgres_timestamp_micros(raw), postgres_timestamp_micros(stored));
        assert_ne!(postgres_timestamp_micros(raw + chrono::Duration::microseconds(1)),
            postgres_timestamp_micros(stored));
        assert_eq!(Some(raw).map(postgres_timestamp_micros), Some(stored).map(postgres_timestamp_micros));
        assert_ne!(None::<DateTime<Utc>>.map(postgres_timestamp_micros), Some(stored).map(postgres_timestamp_micros));
    }

    #[test]
    fn comparisons_match_signed_truncation_before_the_postgres_epoch() {
        let raw = DateTime::parse_from_rfc3339("1999-12-31T23:59:59.999999789Z").unwrap().with_timezone(&Utc);
        let stored = DateTime::from_timestamp(946_684_800, 0).unwrap();
        assert_eq!(postgres_timestamp_micros(raw), Some(0));
        assert_eq!(postgres_timestamp_micros(raw), postgres_timestamp_micros(stored));
    }
}

fn consent_detail(intent: &Intent, plan: &Plan) -> Value {
    json!({
        "operation":"meeting_transcript_capture","source":"outlook_schedule",
        "meeting_id":plan.meeting_id,"tenant_id":intent.tenant_id,"aad_id":intent.aad_id,
        "graph_id":intent.graph_id,"intent_id":intent.id,"recurring":plan.recurring,
        "occurrence_start":plan.start,"occurrence_end":plan.end,"availability_window_hours":24
    })
}

/// New window on an existing capture row; fresh generation so an in-flight fetch of the
/// previous window cannot write an observation onto it. `revive` additionally clears a
/// revoked_at that predates this intent (the Stop barrier column is kept by the trigger).
async fn reset_capture(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    intent: &Intent,
    capture: Uuid,
    plan: &Plan,
    revive: bool,
) -> sqlx::Result<()> {
    let sql = if revive {
        "UPDATE teams_meeting_captures SET revoked_at=NULL,"
    } else {
        // Never touch a revoked row: the WHERE below refuses it.
        "UPDATE teams_meeting_captures SET"
    };
    let done = sqlx::query(&format!(
        "{sql} aad_id=$6,graph_id=$7,occurrence_start=$3,occurrence_end=$4,transcript_end=$5,next_poll_at=now(),
         capture_generation=gen_random_uuid(),last_checked_at=NULL,last_retrieved_at=NULL,
         retrieved_transcript_count=0,last_error_code=NULL
         WHERE id=$1 AND account_id=$2 AND ($8 OR revoked_at IS NULL)"
    ))
    .bind(capture)
    .bind(intent.account_id)
    .bind(plan.start)
    .bind(plan.end)
    .bind(plan.transcript_end)
    .bind(&intent.aad_id)
    .bind(intent.graph_id)
    .bind(revive)
    .execute(&mut **tx)
    .await?
    .rows_affected();
    if done != 1 {
        return Err(db("capture row changed under lock"));
    }
    Ok(())
}

/// Claim this member's due intents (lease), expiring overdue unsaved ones first and
/// purging old terminal rows.
pub(crate) async fn claim_due(
    pool: &PgPool,
    account: Uuid,
    user: Uuid,
    tenant: &str,
    aad: &str,
) -> sqlx::Result<Vec<Intent>> {
    let mut tx = begin_tenant_scoped(pool, account, user).await.map_err(db)?;
    sqlx::query(
        "UPDATE outlook_schedule_intents SET status='expired',updated_at=now()
         WHERE account_id=$1 AND user_id=$2 AND status='pending_save' AND expires_at<=now()",
    )
    .bind(account)
    .bind(user)
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "DELETE FROM outlook_schedule_intents WHERE account_id=$1 AND user_id=$2
         AND status IN ('stopped','expired','error') AND updated_at<now()-interval '30 days'",
    )
    .bind(account)
    .bind(user)
    .execute(&mut *tx)
    .await?;
    let rows = sqlx::query_as::<_, Intent>(&format!(
        "UPDATE outlook_schedule_intents SET next_check_at=now()+make_interval(secs=>$5)
         WHERE id IN (SELECT id FROM outlook_schedule_intents
            WHERE account_id=$1 AND user_id=$2 AND tenant_id=$3 AND aad_id=$4
              AND status IN ('pending_save','active') AND next_check_at<=now()
            ORDER BY next_check_at LIMIT {CLAIM_BATCH} FOR UPDATE SKIP LOCKED)
         RETURNING {COLS}"
    ))
    .bind(account)
    .bind(user)
    .bind(tenant)
    .bind(aad)
    .bind(CLAIM_LEASE_SECS as f64)
    .fetch_all(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(rows)
}

/// Time until this member's earliest open intent is due again.
pub(crate) async fn next_delay(
    pool: &PgPool,
    account: Uuid,
    user: Uuid,
) -> sqlx::Result<Option<std::time::Duration>> {
    let mut tx = begin_tenant_scoped(pool, account, user).await.map_err(db)?;
    // DB clock on both sides, like the claim.
    let ms: Option<i64> = sqlx::query_scalar(
        "SELECT GREATEST(extract(epoch FROM (min(next_check_at) - now())) * 1000, 1)::bigint
         FROM outlook_schedule_intents
         WHERE account_id=$1 AND user_id=$2 AND status IN ('pending_save','active')",
    )
    .bind(account)
    .bind(user)
    .fetch_one(&mut *tx)
    .await?;
    tx.rollback().await?;
    Ok(ms.map(|ms| std::time::Duration::from_millis(ms as u64)))
}

/// Poll cadence for an open, tracked intent: near events every 5 min, far ones every 30.
pub(crate) fn recheck_after(next_start: Option<DateTime<Utc>>, now: DateTime<Utc>) -> DateTime<Utc> {
    match next_start {
        Some(start) if start - now > Duration::hours(36) => now + Duration::minutes(30),
        _ => now + Duration::minutes(5),
    }
}
