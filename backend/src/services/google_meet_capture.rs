//! Google Meet per-meeting opt-in (`google_meet_captures`, control `0269` /
//! tenant `0111`). Mirrors `services::teams_meeting_capture`: an explicit,
//! per-member, per-meeting action is the ONLY authority to ingest a Meet
//! transcript. OAuth connection, calendar presence and being an attendee are
//! not consent.
//!
//! # Consent is per member, for that member's OWN copy
//!
//! A row belongs to `(account_id, owner_user_id)` and RLS (owner policy, same
//! shape as `teams_meeting_captures`) only lets that member see or write it.
//! The sweep reads another attendee's row only inside a transaction scoped to
//! THAT attendee (`begin_tenant_scoped(pool, attendee_account, attendee_user)`),
//! and only ever to decide whether to write THAT attendee's own copy. One
//! member's approval never produces a copy for anyone else; each attendee
//! approves their own.
//!
//! # Matching
//!
//! * A row with `conference_record_id` matches exactly that record. An exact
//!   row ALWAYS wins over a pre-approval: `ingested` / `revoked` rows are
//!   final for that record.
//! * A pre-approval (no `conference_record_id`; `space_name` and/or
//!   `meeting_code`) covers the FIRST conference record of that space that
//!   ends at or after the moment the approval was given (so a meeting that is
//!   running, or has not started, when the member approves is covered; one that
//!   already ended is not). The sweep binds the record id onto the row, after
//!   which it is an exact row. A recurring meeting needs a fresh approval per
//!   occurrence.
//! * `allowed` rows stop matching at `expires_at` (30 days; Google keeps
//!   transcripts about that long).
//!
//! The pool passed to every function is the plane's CONTENT pool: the control
//! pool for a personal account, the org's tenant pool for an org account. The
//! table and its RLS policy are identical on both.

use chrono::{DateTime, Utc};
use sqlx::PgPool;
use uuid::Uuid;

use crate::tenancy::scope::begin_tenant_scoped;

pub(crate) const STATUS_ALLOWED: &str = "allowed";
pub(crate) const STATUS_INGESTED: &str = "ingested";
pub(crate) const STATUS_REVOKED: &str = "revoked";

/// Per-member cap on rows read in one pass (a member approves meetings by
/// hand; this is a safety valve, not a product limit).
const MAX_ROWS: i64 = 500;

#[derive(Debug, Clone, PartialEq, sqlx::FromRow, serde::Serialize)]
pub(crate) struct Capture {
    pub id: Uuid,
    #[serde(skip)]
    pub account_id: Uuid,
    #[serde(skip)]
    pub owner_user_id: Uuid,
    pub conference_record_id: Option<String>,
    pub space_name: Option<String>,
    pub meeting_code: Option<String>,
    pub status: String,
    pub terminal_reason: Option<String>,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub ingested_at: Option<DateTime<Utc>>,
    pub revoked_at: Option<DateTime<Utc>>,
}

const COLS: &str = "id, account_id, owner_user_id, conference_record_id, space_name, meeting_code, \
                    status, terminal_reason, created_at, expires_at, ingested_at, revoked_at";

fn scope_err(e: crate::store::StoreError) -> sqlx::Error {
    sqlx::Error::Protocol(e.to_string())
}

// ---------------------------------------------------------------------------
// Pure helpers (unit-tested)
// ---------------------------------------------------------------------------

/// `"abc-defg-hij"`, a `meet.google.com/...` URL, or the bare 10 letters ->
/// the canonical lowercase dashed code. `None` for anything else.
pub(crate) fn normalize_meeting_code(raw: &str) -> Option<String> {
    let mut s = raw.trim();
    for prefix in ["https://", "http://"] {
        if let Some(rest) = s.strip_prefix(prefix) {
            s = rest;
        }
    }
    if let Some(rest) = s.strip_prefix("meet.google.com/") {
        s = rest;
    }
    let s = s.split(['?', '#', '/']).next().unwrap_or("").to_ascii_lowercase();
    let letters: String = s.chars().filter(|c| *c != '-').collect();
    if letters.len() != 10 || !letters.chars().all(|c| c.is_ascii_lowercase()) {
        return None;
    }
    // Dashes, if present, must be in the canonical places.
    if s.contains('-') && s.len() != 12 {
        return None;
    }
    if s.contains('-') && (s.as_bytes()[3] != b'-' || s.as_bytes()[8] != b'-') {
        return None;
    }
    Some(format!("{}-{}-{}", &letters[0..3], &letters[3..7], &letters[7..10]))
}

fn valid_id_chars(s: &str) -> bool {
    !s.is_empty() && s.len() <= 128 && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

/// `"spaces/{id}"` (or a bare id) -> `"spaces/{id}"`; `None` if malformed.
pub(crate) fn normalize_space(raw: &str) -> Option<String> {
    let id = raw.trim().strip_prefix("spaces/").unwrap_or_else(|| raw.trim());
    valid_id_chars(id).then(|| format!("spaces/{id}"))
}

/// `"conferenceRecords/{id}"` (or a bare id) -> canonical name; `None` if malformed.
pub(crate) fn normalize_record(raw: &str) -> Option<String> {
    let id = raw.trim().strip_prefix("conferenceRecords/").unwrap_or_else(|| raw.trim());
    valid_id_chars(id).then(|| format!("conferenceRecords/{id}"))
}

/// What a member's rows say about one conference record.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum CaptureMatch {
    /// No row covers this record: do not ingest.
    None,
    /// An open (`allowed`, unexpired) row covers it. `bound == false` means it
    /// is a pre-approval the caller should bind to the record before use.
    Allowed(Capture),
    Ingested(Capture),
    Revoked(Capture),
}

impl CaptureMatch {
    /// `none` / `allowed` / `ingested`, the vocabulary of the meetings API.
    /// A revoked row reads as `none` (the member may allow it again).
    pub(crate) fn state(&self) -> &'static str {
        match self {
            CaptureMatch::None | CaptureMatch::Revoked(_) => "none",
            CaptureMatch::Allowed(_) => "allowed",
            CaptureMatch::Ingested(_) => "ingested",
        }
    }
    pub(crate) fn capture(&self) -> Option<&Capture> {
        match self {
            CaptureMatch::None => None,
            CaptureMatch::Allowed(c) | CaptureMatch::Ingested(c) | CaptureMatch::Revoked(c) => Some(c),
        }
    }
}

/// True when some open pre-approval can only be matched by the space's
/// `meetingCode` (it has no space name), i.e. the caller must look the code up.
pub(crate) fn needs_meeting_code(captures: &[Capture]) -> bool {
    captures.iter().any(|c| {
        c.status == STATUS_ALLOWED && c.conference_record_id.is_none() && c.space_name.is_none() && c.meeting_code.is_some()
    })
}

/// Decide what `captures` (ONE member's rows) say about a conference record.
/// `record_end` is the record's `endTime`; `now` bounds `allowed` rows.
pub(crate) fn resolve(
    captures: &[Capture],
    record: &str,
    space: Option<&str>,
    meeting_code: Option<&str>,
    record_end: DateTime<Utc>,
    now: DateTime<Utc>,
) -> CaptureMatch {
    if let Some(c) = captures.iter().find(|c| c.conference_record_id.as_deref() == Some(record)) {
        return match c.status.as_str() {
            STATUS_INGESTED => CaptureMatch::Ingested(c.clone()),
            STATUS_REVOKED => CaptureMatch::Revoked(c.clone()),
            _ if c.expires_at > now => CaptureMatch::Allowed(c.clone()),
            _ => CaptureMatch::None,
        };
    }
    captures
        .iter()
        .filter(|c| {
            c.status == STATUS_ALLOWED
                && c.conference_record_id.is_none()
                && c.expires_at > now
                // Covers a record that ends after the approval, never one that
                // had already ended when the member approved.
                && record_end >= c.created_at
                && match (&c.space_name, &c.meeting_code) {
                    (Some(s), _) => space == Some(s.as_str()),
                    (None, Some(code)) => meeting_code == Some(code.as_str()),
                    (None, None) => false,
                }
        })
        .min_by_key(|c| c.created_at)
        .map_or(CaptureMatch::None, |c| CaptureMatch::Allowed(c.clone()))
}

// ---------------------------------------------------------------------------
// Persistence (every call runs inside a transaction scoped to the member)
// ---------------------------------------------------------------------------

/// Every row this member can act on: all `ingested`/`revoked` rows plus
/// unexpired `allowed` rows.
pub(crate) async fn load(pool: &PgPool, account: Uuid, member: Uuid) -> sqlx::Result<Vec<Capture>> {
    let mut tx = crate::tenancy::scope::begin_request_tenant_read(pool, account, member).await.map_err(scope_err)?;
    let rows = sqlx::query_as::<_, Capture>(&format!(
        "SELECT {COLS} FROM google_meet_captures \
         WHERE account_id = $1 AND owner_user_id = $2 AND (status <> 'allowed' OR expires_at > now()) \
         ORDER BY created_at DESC LIMIT {MAX_ROWS}"
    ))
    .bind(account)
    .bind(member)
    .fetch_all(&mut *tx)
    .await?;
    tx.rollback().await?;
    Ok(rows)
}

/// The member allows one concrete conference record. Idempotent; re-allowing a
/// revoked row reopens it; an already-ingested row is left `ingested`.
pub(crate) async fn allow_record(
    pool: &PgPool,
    account: Uuid,
    member: Uuid,
    record: &str,
    space: Option<&str>,
) -> sqlx::Result<Capture> {
    let mut tx = begin_tenant_scoped(pool, account, member).await.map_err(scope_err)?;
    let row = sqlx::query_as::<_, Capture>(&format!(
        "INSERT INTO google_meet_captures (account_id, owner_user_id, conference_record_id, space_name) \
         VALUES ($1, $2, $3, $4) \
         ON CONFLICT (account_id, owner_user_id, conference_record_id) WHERE conference_record_id IS NOT NULL \
         DO UPDATE SET \
           status = CASE WHEN google_meet_captures.status = 'ingested' THEN 'ingested' ELSE 'allowed' END, \
           revoked_at = CASE WHEN google_meet_captures.status = 'ingested' THEN google_meet_captures.revoked_at ELSE NULL END, \
           terminal_reason = CASE WHEN google_meet_captures.status = 'ingested' THEN google_meet_captures.terminal_reason ELSE NULL END, \
           expires_at = CASE WHEN google_meet_captures.status = 'ingested' THEN google_meet_captures.expires_at ELSE now() + interval '30 days' END, \
           space_name = COALESCE(google_meet_captures.space_name, EXCLUDED.space_name), \
           approved_at = CASE WHEN google_meet_captures.status = 'ingested' THEN google_meet_captures.approved_at ELSE now() END, \
           updated_at = now() \
         RETURNING {COLS}"
    ))
    .bind(account)
    .bind(member)
    .bind(record)
    .bind(space)
    .fetch_one(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(row)
}

/// Pre-approve a meeting by space and/or meeting code before or while it runs.
/// Returns `(row, created)`; an existing open pre-approval for the same space
/// or code is returned unchanged (`created == false`).
pub(crate) async fn pre_approve(
    pool: &PgPool,
    account: Uuid,
    member: Uuid,
    space: Option<&str>,
    meeting_code: Option<&str>,
) -> sqlx::Result<(Capture, bool)> {
    let mut tx = begin_tenant_scoped(pool, account, member).await.map_err(scope_err)?;
    let inserted = sqlx::query_as::<_, Capture>(&format!(
        "INSERT INTO google_meet_captures (account_id, owner_user_id, space_name, meeting_code) \
         VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING RETURNING {COLS}"
    ))
    .bind(account)
    .bind(member)
    .bind(space)
    .bind(meeting_code)
    .fetch_optional(&mut *tx)
    .await?;
    if let Some(row) = inserted {
        tx.commit().await?;
        return Ok((row, true));
    }
    let existing = sqlx::query_as::<_, Capture>(&format!(
        "SELECT {COLS} FROM google_meet_captures \
         WHERE account_id = $1 AND owner_user_id = $2 AND status = 'allowed' AND conference_record_id IS NULL \
           AND (($3::text IS NOT NULL AND space_name = $3) OR ($4::text IS NOT NULL AND meeting_code = $4)) \
         ORDER BY created_at LIMIT 1"
    ))
    .bind(account)
    .bind(member)
    .bind(space)
    .bind(meeting_code)
    .fetch_one(&mut *tx)
    .await?;
    tx.rollback().await?;
    Ok((existing, false))
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum RevokeOutcome {
    Revoked(Uuid),
    /// Already ingested: the copy exists; revoking cannot undo it.
    AlreadyIngested,
    /// Nothing to revoke (never allowed, already revoked, or expired).
    NotFound,
}

/// Revoke by conference record (the `DELETE meetings/{id}/allow` route).
pub(crate) async fn revoke_by_record(pool: &PgPool, account: Uuid, member: Uuid, record: &str) -> sqlx::Result<RevokeOutcome> {
    let mut tx = begin_tenant_scoped(pool, account, member).await.map_err(scope_err)?;
    let revoked: Option<(Uuid,)> = sqlx::query_as(
        "UPDATE google_meet_captures SET status = 'revoked', revoked_at = now(), updated_at = now() \
         WHERE account_id = $1 AND owner_user_id = $2 AND conference_record_id = $3 AND status = 'allowed' \
         RETURNING id",
    )
    .bind(account)
    .bind(member)
    .bind(record)
    .fetch_optional(&mut *tx)
    .await?;
    let outcome = match revoked {
        Some((id,)) => RevokeOutcome::Revoked(id),
        None => {
            let ingested: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM google_meet_captures WHERE account_id = $1 AND owner_user_id = $2 \
                 AND conference_record_id = $3 AND status = 'ingested')",
            )
            .bind(account)
            .bind(member)
            .bind(record)
            .fetch_one(&mut *tx)
            .await?;
            if ingested {
                RevokeOutcome::AlreadyIngested
            } else {
                RevokeOutcome::NotFound
            }
        }
    };
    tx.commit().await?;
    Ok(outcome)
}

/// Revoke by capture id (cancels a pre-approval that has not matched yet).
pub(crate) async fn revoke_by_id(pool: &PgPool, account: Uuid, member: Uuid, id: Uuid) -> sqlx::Result<RevokeOutcome> {
    let mut tx = begin_tenant_scoped(pool, account, member).await.map_err(scope_err)?;
    let revoked: Option<(Uuid,)> = sqlx::query_as(
        "UPDATE google_meet_captures SET status = 'revoked', revoked_at = now(), updated_at = now() \
         WHERE id = $3 AND account_id = $1 AND owner_user_id = $2 AND status = 'allowed' RETURNING id",
    )
    .bind(account)
    .bind(member)
    .bind(id)
    .fetch_optional(&mut *tx)
    .await?;
    let outcome = match revoked {
        Some((id,)) => RevokeOutcome::Revoked(id),
        None => {
            let ingested: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM google_meet_captures WHERE id = $3 AND account_id = $1 \
                 AND owner_user_id = $2 AND status = 'ingested')",
            )
            .bind(account)
            .bind(member)
            .bind(id)
            .fetch_one(&mut *tx)
            .await?;
            if ingested {
                RevokeOutcome::AlreadyIngested
            } else {
                RevokeOutcome::NotFound
            }
        }
    };
    tx.commit().await?;
    Ok(outcome)
}

/// This member's open pre-approvals and recent captures, newest first.
pub(crate) async fn list_recent(pool: &PgPool, account: Uuid, member: Uuid) -> sqlx::Result<Vec<Capture>> {
    let mut rows = load(pool, account, member).await?;
    rows.retain(|c| c.status != STATUS_REVOKED);
    rows.truncate(100);
    Ok(rows)
}

/// Bind a pre-approval to the conference record that matched it (stays
/// `allowed`). `false` when the row is no longer an open pre-approval, or an
/// exact row for that record already exists (the member's explicit choice for
/// that record wins).
pub(crate) async fn bind_record(
    pool: &PgPool,
    account: Uuid,
    member: Uuid,
    id: Uuid,
    record: &str,
    space: Option<&str>,
) -> sqlx::Result<bool> {
    let mut tx = begin_tenant_scoped(pool, account, member).await.map_err(scope_err)?;
    let n = sqlx::query(
        "UPDATE google_meet_captures SET conference_record_id = $4, space_name = COALESCE(space_name, $5), updated_at = now() \
         WHERE id = $3 AND account_id = $1 AND owner_user_id = $2 AND status = 'allowed' AND conference_record_id IS NULL \
           AND NOT EXISTS (SELECT 1 FROM google_meet_captures o WHERE o.account_id = $1 AND o.owner_user_id = $2 \
                           AND o.conference_record_id = $4)",
    )
    .bind(account)
    .bind(member)
    .bind(id)
    .bind(record)
    .bind(space)
    .execute(&mut *tx)
    .await?
    .rows_affected();
    tx.commit().await?;
    Ok(n == 1)
}

/// The member's own copy landed. Only an `allowed` row moves to `ingested`;
/// `false` means it was revoked (or already ingested) in the meantime.
pub(crate) async fn mark_ingested(pool: &PgPool, account: Uuid, member: Uuid, record: &str) -> sqlx::Result<bool> {
    let mut tx = begin_tenant_scoped(pool, account, member).await.map_err(scope_err)?;
    let n = sqlx::query(
        "UPDATE google_meet_captures SET status = 'ingested', ingested_at = now(), updated_at = now() \
         WHERE account_id = $1 AND owner_user_id = $2 AND conference_record_id = $3 AND status = 'allowed'",
    )
    .bind(account)
    .bind(member)
    .bind(record)
    .execute(&mut *tx)
    .await?
    .rows_affected();
    tx.commit().await?;
    Ok(n == 1)
}

/// The capture can never produce a copy; stop re-checking it (row stays `allowed`).
pub(crate) async fn mark_terminal(
    pool: &PgPool,
    account: Uuid,
    member: Uuid,
    record: &str,
    reason: &str,
) -> sqlx::Result<()> {
    let mut tx = begin_tenant_scoped(pool, account, member).await.map_err(scope_err)?;
    sqlx::query(
        "UPDATE google_meet_captures SET terminal_reason = $4, updated_at = now() \
         WHERE account_id = $1 AND owner_user_id = $2 AND conference_record_id = $3 AND status = 'allowed'",
    )
    .bind(account)
    .bind(member)
    .bind(record)
    .bind(reason)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(())
}

/// Bump `updated_at` so `open_bound` (ordered by it) rotates this row to the back.
pub(crate) async fn touch(pool: &PgPool, account: Uuid, member: Uuid, record: &str) -> sqlx::Result<()> {
    let mut tx = begin_tenant_scoped(pool, account, member).await.map_err(scope_err)?;
    sqlx::query(
        "UPDATE google_meet_captures SET updated_at = now() \
         WHERE account_id = $1 AND owner_user_id = $2 AND conference_record_id = $3 AND status = 'allowed'",
    )
    .bind(account)
    .bind(member)
    .bind(record)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(())
}

/// The member's open, record-bound captures still waiting for a copy: what the
/// sweep re-checks every tick, independent of its listing watermark.
pub(crate) async fn open_bound(pool: &PgPool, account: Uuid, member: Uuid, limit: i64) -> sqlx::Result<Vec<Capture>> {
    let mut tx = begin_tenant_scoped(pool, account, member).await.map_err(scope_err)?;
    let rows = sqlx::query_as::<_, Capture>(&format!(
        "SELECT {COLS} FROM google_meet_captures \
         WHERE account_id = $1 AND owner_user_id = $2 AND status = 'allowed' AND terminal_reason IS NULL \
           AND conference_record_id IS NOT NULL AND expires_at > now() \
         ORDER BY updated_at, created_at LIMIT $3"
    ))
    .bind(account)
    .bind(member)
    .bind(limit)
    .fetch_all(&mut *tx)
    .await?;
    tx.rollback().await?;
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    fn cap(record: Option<&str>, space: Option<&str>, code: Option<&str>, status: &str, age_h: i64) -> Capture {
        let now = Utc::now();
        Capture {
            id: Uuid::new_v4(),
            account_id: Uuid::nil(),
            owner_user_id: Uuid::nil(),
            conference_record_id: record.map(str::to_string),
            space_name: space.map(str::to_string),
            meeting_code: code.map(str::to_string),
            status: status.to_string(),
            terminal_reason: None,
            created_at: now - Duration::hours(age_h),
            expires_at: now + Duration::days(30) - Duration::hours(age_h),
            ingested_at: (status == STATUS_INGESTED).then_some(now),
            revoked_at: (status == STATUS_REVOKED).then_some(now),
        }
    }

    #[test]
    fn meeting_codes_normalize_from_urls_and_bare_letters() {
        assert_eq!(normalize_meeting_code("abc-defg-hij").as_deref(), Some("abc-defg-hij"));
        assert_eq!(normalize_meeting_code("  ABC-DEFG-HIJ ").as_deref(), Some("abc-defg-hij"));
        assert_eq!(normalize_meeting_code("abcdefghij").as_deref(), Some("abc-defg-hij"));
        assert_eq!(
            normalize_meeting_code("https://meet.google.com/abc-defg-hij?authuser=1").as_deref(),
            Some("abc-defg-hij")
        );
        assert_eq!(normalize_meeting_code("meet.google.com/abc-defg-hij").as_deref(), Some("abc-defg-hij"));
    }

    #[test]
    fn malformed_meeting_codes_are_rejected() {
        for bad in ["", "abc-defg", "abc-defg-hi1", "ab-cdef-ghij", "abcdefghijk", "abc--defghij", "../etc/passwd", "lookup/x"] {
            assert_eq!(normalize_meeting_code(bad), None, "{bad}");
        }
    }

    #[test]
    fn space_and_record_names_normalize_and_reject_garbage() {
        assert_eq!(normalize_space("spaces/AbC_1-2").as_deref(), Some("spaces/AbC_1-2"));
        assert_eq!(normalize_space("AbC").as_deref(), Some("spaces/AbC"));
        assert_eq!(normalize_space("spaces/a/b"), None);
        assert_eq!(normalize_space("spaces/"), None);
        assert_eq!(normalize_record("conferenceRecords/xyz").as_deref(), Some("conferenceRecords/xyz"));
        assert_eq!(normalize_record("xyz").as_deref(), Some("conferenceRecords/xyz"));
        assert_eq!(normalize_record("xyz/transcripts/1"), None);
        assert_eq!(normalize_record(&"a".repeat(129)), None);
    }

    #[test]
    fn no_rows_means_no_match() {
        let now = Utc::now();
        assert_eq!(resolve(&[], "conferenceRecords/a", Some("spaces/s"), None, now, now), CaptureMatch::None);
    }

    #[test]
    fn exact_row_matches_by_status() {
        let now = Utc::now();
        for (status, want) in [(STATUS_ALLOWED, "allowed"), (STATUS_INGESTED, "ingested"), (STATUS_REVOKED, "none")] {
            let rows = [cap(Some("conferenceRecords/a"), None, None, status, 1)];
            assert_eq!(resolve(&rows, "conferenceRecords/a", None, None, now, now).state(), want, "{status}");
        }
        let rows = [cap(Some("conferenceRecords/a"), None, None, STATUS_ALLOWED, 1)];
        assert_eq!(resolve(&rows, "conferenceRecords/b", None, None, now, now), CaptureMatch::None);
    }

    #[test]
    fn expired_allowed_row_never_matches() {
        let now = Utc::now();
        let mut c = cap(Some("conferenceRecords/a"), None, None, STATUS_ALLOWED, 1);
        c.expires_at = now - Duration::minutes(1);
        assert_eq!(resolve(&[c], "conferenceRecords/a", None, None, now, now), CaptureMatch::None);
        let mut p = cap(None, Some("spaces/s"), None, STATUS_ALLOWED, 1);
        p.expires_at = now - Duration::minutes(1);
        assert_eq!(resolve(&[p], "conferenceRecords/a", Some("spaces/s"), None, now, now), CaptureMatch::None);
    }

    #[test]
    fn pre_approval_by_space_covers_a_record_ending_after_approval() {
        let now = Utc::now();
        let rows = [cap(None, Some("spaces/s"), None, STATUS_ALLOWED, 2)];
        let ends_after = now - Duration::hours(1);
        let m = resolve(&rows, "conferenceRecords/a", Some("spaces/s"), None, ends_after, now);
        assert_eq!(m.state(), "allowed");
        assert_eq!(m.capture().unwrap().id, rows[0].id);
        // Different space: no cover.
        assert_eq!(
            resolve(&rows, "conferenceRecords/a", Some("spaces/other"), None, ends_after, now),
            CaptureMatch::None
        );
        // No space known for the record: no cover.
        assert_eq!(resolve(&rows, "conferenceRecords/a", None, None, ends_after, now), CaptureMatch::None);
    }

    #[test]
    fn pre_approval_never_covers_a_record_that_ended_before_it_was_given() {
        let now = Utc::now();
        let rows = [cap(None, Some("spaces/s"), None, STATUS_ALLOWED, 1)];
        let ended_before = now - Duration::hours(3);
        assert_eq!(resolve(&rows, "conferenceRecords/a", Some("spaces/s"), None, ended_before, now), CaptureMatch::None);
    }

    #[test]
    fn pre_approval_by_code_needs_the_spaces_code_and_matches_it() {
        let now = Utc::now();
        let rows = [cap(None, None, Some("abc-defg-hij"), STATUS_ALLOWED, 2)];
        assert!(needs_meeting_code(&rows));
        let end = now - Duration::hours(1);
        assert_eq!(resolve(&rows, "conferenceRecords/a", Some("spaces/s"), None, end, now), CaptureMatch::None);
        assert_eq!(
            resolve(&rows, "conferenceRecords/a", Some("spaces/s"), Some("abc-defg-hij"), end, now).state(),
            "allowed"
        );
        assert_eq!(
            resolve(&rows, "conferenceRecords/a", Some("spaces/s"), Some("zzz-zzzz-zzz"), end, now),
            CaptureMatch::None
        );
    }

    #[test]
    fn a_row_with_a_space_never_asks_for_the_code() {
        let rows = [cap(None, Some("spaces/s"), Some("abc-defg-hij"), STATUS_ALLOWED, 1)];
        assert!(!needs_meeting_code(&rows));
        let bound = [cap(Some("conferenceRecords/a"), None, Some("abc-defg-hij"), STATUS_ALLOWED, 1)];
        assert!(!needs_meeting_code(&bound));
    }

    #[test]
    fn exact_row_beats_a_pre_approval_and_a_revoked_exact_row_blocks_it() {
        let now = Utc::now();
        let end = now - Duration::hours(1);
        let revoked = cap(Some("conferenceRecords/a"), Some("spaces/s"), None, STATUS_REVOKED, 1);
        let pre = cap(None, Some("spaces/s"), None, STATUS_ALLOWED, 2);
        let m = resolve(&[pre.clone(), revoked], "conferenceRecords/a", Some("spaces/s"), None, end, now);
        assert_eq!(m.state(), "none", "an explicit revoke for this record wins over a pre-approval");
        let ingested = cap(Some("conferenceRecords/a"), Some("spaces/s"), None, STATUS_INGESTED, 1);
        let m = resolve(&[pre, ingested], "conferenceRecords/a", Some("spaces/s"), None, end, now);
        assert_eq!(m.state(), "ingested");
    }

    #[test]
    fn earliest_pre_approval_wins() {
        let now = Utc::now();
        let a = cap(None, Some("spaces/s"), None, STATUS_ALLOWED, 5);
        let b = cap(None, None, Some("abc-defg-hij"), STATUS_ALLOWED, 2);
        let m = resolve(
            &[b, a.clone()],
            "conferenceRecords/a",
            Some("spaces/s"),
            Some("abc-defg-hij"),
            now,
            now,
        );
        assert_eq!(m.capture().unwrap().id, a.id);
    }

    #[test]
    fn already_ingested_or_revoked_pre_approvals_are_not_open() {
        let now = Utc::now();
        let rows = [
            cap(None, Some("spaces/s"), None, STATUS_REVOKED, 2),
        ];
        assert_eq!(resolve(&rows, "conferenceRecords/a", Some("spaces/s"), None, now, now), CaptureMatch::None);
    }
}

// ---------------------------------------------------------------------------
// pg tests: the table's RLS and constraints, exercised AS THE REAL
// non-owner roles (`coralswarm_app` on the control plane,
// `coralswarm_tenant_app` on a tenant database) - the owner role bypasses RLS
// and grants, so it cannot prove any of this.
//
//   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//     cargo test -p coralswarm-backend --lib \
//     services::google_meet_capture::google_meet_capture_pg_tests -- --test-threads=1
// ---------------------------------------------------------------------------
#[cfg(test)]
mod google_meet_capture_pg_tests {
    use super::*;
    use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
    use sqlx::{Connection, Executor, PgConnection};

    const TENANT_ROLE_PW: &str = "meet_capture_tenant_rls_test_only_pw";

    fn base_url() -> Option<String> {
        std::env::var("ORGT_TEST_BASE_URL").ok()
    }
    fn with_db(base: &str, db: &str) -> String {
        let (prefix, rest) = base.split_at(base.rfind('/').expect("url has a path /"));
        let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
        format!("{prefix}/{db}{params}")
    }
    async fn fresh(base: &str, db: &str) -> PgPool {
        let mut conn = PgConnection::connect(base).await.expect("connect admin");
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.unwrap();
        conn.execute(format!("CREATE DATABASE {db}").as_str()).await.unwrap();
        conn.close().await.ok();
        PgPoolOptions::new().connect(&with_db(base, db)).await.unwrap()
    }
    async fn drop_db(base: &str, db: &str) {
        let mut conn = PgConnection::connect(base).await.unwrap();
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    /// The behaviour every plane must have, run against `pool` (a NOBYPASSRLS
    /// role's pool): per-member isolation, WITH CHECK, uniqueness, lifecycle.
    async fn exercise(pool: &PgPool) {
        let acct = Uuid::new_v4();
        let (a, b) = (Uuid::new_v4(), Uuid::new_v4());
        let rec = "conferenceRecords/abc123";

        // Each member allows the same record independently.
        let ra = allow_record(pool, acct, a, rec, Some("spaces/s1")).await.unwrap();
        let rb = allow_record(pool, acct, b, rec, None).await.unwrap();
        assert_ne!(ra.id, rb.id);
        // Idempotent: allowing twice is one row.
        let again = allow_record(pool, acct, a, rec, None).await.unwrap();
        assert_eq!(again.id, ra.id);
        assert_eq!(load(pool, acct, a).await.unwrap().len(), 1);

        // Isolation: B can neither see, revoke, bind nor mark A's row.
        assert_eq!(load(pool, acct, b).await.unwrap().iter().map(|c| c.id).collect::<Vec<_>>(), vec![rb.id]);
        assert_eq!(revoke_by_id(pool, acct, b, ra.id).await.unwrap(), RevokeOutcome::NotFound);
        assert!(!mark_ingested(pool, acct, b, "conferenceRecords/other").await.unwrap());
        // A scope with the wrong account sees nothing even for the right user.
        assert!(load(pool, Uuid::new_v4(), a).await.unwrap().is_empty());
        // No scope at all -> no rows (FORCE RLS), never a leak.
        let unscoped: i64 = sqlx::query_scalar("SELECT count(*) FROM google_meet_captures").fetch_one(pool).await.unwrap();
        assert_eq!(unscoped, 0, "an unscoped read must see no rows");

        // WITH CHECK: A's scope cannot write a row owned by B.
        let mut tx = begin_tenant_scoped(pool, acct, a).await.unwrap();
        let forged = sqlx::query(
            "INSERT INTO google_meet_captures (account_id, owner_user_id, conference_record_id) VALUES ($1, $2, 'conferenceRecords/forged')",
        )
        .bind(acct)
        .bind(b)
        .execute(&mut *tx)
        .await;
        assert!(forged.is_err(), "RLS WITH CHECK must reject a row for another member");
        drop(tx);
        // CHECK constraints: an identity is required and malformed ids are refused.
        for bad in [
            "INSERT INTO google_meet_captures (account_id, owner_user_id) VALUES ($1, $2)",
            "INSERT INTO google_meet_captures (account_id, owner_user_id, conference_record_id) VALUES ($1, $2, 'not-a-record')",
            "INSERT INTO google_meet_captures (account_id, owner_user_id, meeting_code) VALUES ($1, $2, 'ABC-defg-hij')",
        ] {
            let mut tx = begin_tenant_scoped(pool, acct, a).await.unwrap();
            assert!(sqlx::query(bad).bind(acct).bind(a).execute(&mut *tx).await.is_err(), "{bad}");
        }

        // Lifecycle: allowed -> ingested; revoke then reports it.
        assert!(mark_ingested(pool, acct, a, rec).await.unwrap());
        assert!(!mark_ingested(pool, acct, a, rec).await.unwrap(), "only an allowed row can become ingested");
        assert_eq!(revoke_by_record(pool, acct, a, rec).await.unwrap(), RevokeOutcome::AlreadyIngested);
        // B's is untouched by A's lifecycle and can still be revoked then re-allowed.
        assert!(matches!(revoke_by_record(pool, acct, b, rec).await.unwrap(), RevokeOutcome::Revoked(_)));
        assert_eq!(allow_record(pool, acct, b, rec, None).await.unwrap().status, STATUS_ALLOWED);

        // Pre-approvals: one open per space/code per member; binding makes it exact.
        let (p1, created) = pre_approve(pool, acct, a, Some("spaces/s9"), None).await.unwrap();
        assert!(created);
        let (p2, created) = pre_approve(pool, acct, a, Some("spaces/s9"), None).await.unwrap();
        assert!(!created);
        assert_eq!(p1.id, p2.id);
        let (c1, created) = pre_approve(pool, acct, a, None, Some("abc-defg-hij")).await.unwrap();
        assert!(created);
        assert!(bind_record(pool, acct, a, p1.id, "conferenceRecords/zzz", Some("spaces/s9")).await.unwrap());
        assert!(!bind_record(pool, acct, a, p1.id, "conferenceRecords/yyy", None).await.unwrap(), "already bound");
        assert!(!bind_record(pool, acct, b, c1.id, "conferenceRecords/yyy", None).await.unwrap(), "B cannot bind A's row");
        // A bound pre-approval frees its space for the next occurrence.
        assert!(pre_approve(pool, acct, a, Some("spaces/s9"), None).await.unwrap().1);
        // A pre-approval can be cancelled by id.
        assert!(matches!(revoke_by_id(pool, acct, a, c1.id).await.unwrap(), RevokeOutcome::Revoked(_)));
        // open_bound returns only record-bound, unexpired, non-terminal allowed rows.
        let open: Vec<_> = open_bound(pool, acct, a, 10).await.unwrap().into_iter().filter_map(|c| c.conference_record_id).collect();
        assert_eq!(open, vec!["conferenceRecords/zzz".to_string()]);
        mark_terminal(pool, acct, a, "conferenceRecords/zzz", "no_transcript").await.unwrap();
        assert!(open_bound(pool, acct, a, 10).await.unwrap().is_empty());
        // Rotation: touching a row moves it behind a newer-touched one.
        allow_record(pool, acct, a, "conferenceRecords/r1", None).await.unwrap();
        allow_record(pool, acct, a, "conferenceRecords/r2", None).await.unwrap();
        touch(pool, acct, a, "conferenceRecords/r1").await.unwrap();
        let order: Vec<_> = open_bound(pool, acct, a, 10).await.unwrap().into_iter().filter_map(|c| c.conference_record_id).collect();
        assert_eq!(order, vec!["conferenceRecords/r2".to_string(), "conferenceRecords/r1".to_string()]);
        // A deterministic non-ingest closes the capture and takes it off the queue.
        mark_terminal(pool, acct, a, "conferenceRecords/r2", "not_ingestable").await.unwrap();
        let order: Vec<_> = open_bound(pool, acct, a, 10).await.unwrap().into_iter().filter_map(|c| c.conference_record_id).collect();
        assert_eq!(order, vec!["conferenceRecords/r1".to_string()]);
    }

    #[tokio::test]
    async fn control_plane_rls_and_lifecycle_as_the_real_app_role_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping control_plane_rls_and_lifecycle_as_the_real_app_role_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "meet_capture_ctl_rls";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let owner = fresh(&base, db).await;
        sqlx::migrate!("./migrations_control").run(&owner).await.unwrap();
        let app = crate::app_role_test_support::app_role_pool(&owner, &with_db(&base, db)).await;
        exercise(&app).await;
        app.close().await;
        owner.close().await;
        drop_db(&base, db).await;
    }

    #[tokio::test]
    async fn tenant_plane_rls_and_lifecycle_as_the_real_tenant_app_role_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping tenant_plane_rls_and_lifecycle_as_the_real_tenant_app_role_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "meet_capture_ten_rls";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let owner = fresh(&base, db).await;
        sqlx::migrate!("./migrations_tenant").run(&owner).await.unwrap();
        sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, 'org_meet_capture')").execute(&owner).await.unwrap();
        let stmt = format!("ALTER ROLE coralswarm_tenant_app WITH PASSWORD '{TENANT_ROLE_PW}'");
        let mut ok = false;
        for attempt in 0..10u32 {
            if sqlx::query(&stmt).execute(&owner).await.is_ok() {
                ok = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50 * u64::from(attempt + 1))).await;
        }
        assert!(ok, "set test password on coralswarm_tenant_app");
        let opts = <PgConnectOptions as std::str::FromStr>::from_str(&with_db(&base, db))
            .unwrap()
            .username("coralswarm_tenant_app")
            .password(TENANT_ROLE_PW);
        let app = PgPoolOptions::new().max_connections(2).connect_with(opts).await.expect("connect AS coralswarm_tenant_app");
        exercise(&app).await;
        app.close().await;
        owner.close().await;
        drop_db(&base, db).await;
    }
}
