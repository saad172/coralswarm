//! Google Meet per-meeting opt-in routes (founder decision 2026-10-06: Meet
//! transcripts are captured like Microsoft Teams - the member allows
//! INDIVIDUAL meetings; nothing is ingested automatically).
//!
//! Clerk-session authed, account + user scoped, same gate shape as
//! `routes::gmail_everything_mode` / `routes::google_chat`:
//!
//! * `GET    /v1/connectors/google/meet/meetings?days=30`
//! * `POST   /v1/connectors/google/meet/meetings/{conference_record_id}/allow`
//! * `DELETE /v1/connectors/google/meet/meetings/{conference_record_id}/allow`
//! * `POST   /v1/connectors/google/meet/captures`  `{"meeting_code": ..}` | `{"space": ..}`
//! * `GET    /v1/connectors/google/meet/captures`
//! * `DELETE /v1/connectors/google/meet/captures/{capture_id}`
//!
//! Personal and org workspaces behave the same; an org caller is additionally
//! gated on `routes::oauth::org_google_meet_enabled_for` (default OFF), the same
//! flag that gates the sweep. Capture rows live on the caller's own plane
//! (control for a personal account, the org's tenant database for an org).
//!
//! # Why the meetings list is LIVE from Meet, not a cached candidate table
//!
//! The list is the member's own `conferenceRecords.list` read with the member's
//! own token. Reasons: (1) Google already keeps every candidate for ~30 days,
//! exactly the window a member can approve in, so a second copy adds nothing but
//! staleness; (2) a cache would store metadata (who met, when, which space)
//! about meetings the member never agreed to bring in, which is the opposite of
//! the opt-in posture; (3) there is no new table, no new retention rule and no
//! stale-row cleanup; (4) the cost is bounded (<= 200 records listed, <= 50
//! enriched, small concurrency) and only paid when the member opens the picker.
//! The sweep therefore never needs to "remember" an un-approved meeting: see
//! `connectors::google_meet::sync_owner_once` for why advancing its watermark
//! past one is safe.
//!
//! # Who can approve what
//!
//! Google scopes conference records to spaces the caller organized, so only an
//! organizer sees a record in `meetings` and can allow it by id. Anyone else
//! pre-approves by `meeting_code` / `space` (`POST captures`); the organizer's
//! sweep then writes that attendee's OWN copy if, and only if, that attendee's
//! row covers the record. Every approval is for the approver's own copy only.
//!
//! Add-in reach: the three `captures` routes are on the add-in allowlist
//! (`routes::addin_microsoft::meet_scope_allows` for the Meet side panel,
//! `gmail_scope_allows` for the Calendar card). The `meetings` routes are
//! Clerk-session-only.

use std::collections::HashMap;
use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Extension, Json,
};
use chrono::{DateTime, Duration, Utc};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

use crate::connectors::google_meet::{
    self as meet, MeetPlane, OrgMeetPlane, RecordOutcome, RecordScope, ResolvedAttendee, TranscriptState,
};
use crate::connectors::{ConnectorError, GoogleConnector, TokenOwner};
use crate::middleware::auth::UserContext;
use crate::routes::connectors::load_creds_bg;
use crate::routes::oauth::store_token_bundle;
use crate::services::google_meet_capture::{self as capture, Capture, RevokeOutcome};
use crate::tenancy::extractor::Tenant;
use crate::AppState;

/// Newest records enriched with transcript state / meeting code per listing.
const MAX_ENRICHED: usize = 50;
/// Concurrent Meet calls while enriching.
const ENRICH_CONCURRENCY: usize = 5;
const DEFAULT_DAYS: i64 = 30;
const MAX_DAYS: i64 = 30;

fn json_err(status: StatusCode, code: &str, message: &str) -> Response {
    (status, Json(json!({"error": code, "message": message}))).into_response()
}

fn unavailable() -> Response {
    json_err(StatusCode::SERVICE_UNAVAILABLE, "service_unavailable", "service unavailable")
}

fn internal(e: impl std::fmt::Display, what: &str) -> Response {
    tracing::error!(error = %e, "google_meet routes: {what}");
    json_err(StatusCode::INTERNAL_SERVER_ERROR, "internal", "internal error")
}

/// Everything a handler needs, resolved once: the plane, its content pool, the
/// control pool (tokens), and the caller's own identity.
struct Ctx {
    plane: MeetPlane,
    pool: PgPool,
    control: PgPool,
    reader: ResolvedAttendee,
}

/// Org gate + plane resolution. Personal: always allowed. Org: only when the
/// org's `google_meet` flag is on (the same flag as the sweep), else 403.
async fn context(state: &Arc<AppState>, tenant: &Tenant, user: &UserContext) -> Result<Ctx, Response> {
    let org = tenant.org_context();
    if let Some(ctx) = org {
        if !crate::routes::oauth::org_google_meet_enabled_for(state, &ctx.org_id).await {
            tracing::info!(org_id = %ctx.org_id, "google meet capture route refused under org context (flag off)");
            return Err(json_err(
                StatusCode::FORBIDDEN,
                "org_google_meet_disabled",
                "Google Meet capture is available in your personal workspace only, or once your \
                 organization admin enables the Google Meet connector",
            ));
        }
    }
    let Some(pool) = tenant.pool.clone() else { return Err(unavailable()) };
    let Some(control) = state.control_db().cloned() else { return Err(unavailable()) };
    let (plane, account_id) = match org {
        Some(ctx) => (
            MeetPlane::Org(OrgMeetPlane { org_id: ctx.org_id.clone(), account_id: ctx.account_id, pool: pool.clone() }),
            ctx.account_id,
        ),
        None => (MeetPlane::Personal, user.account_id),
    };
    Ok(Ctx { plane, pool, control, reader: ResolvedAttendee { account_id, owner_user_id: user.user_id } })
}

/// The caller's own refreshed `google_meet` access token (never another
/// member's). 409 `google_meet_not_connected` when they have not connected Meet.
async fn access_token(state: &Arc<AppState>, tenant: &Tenant, ctx: &Ctx) -> Result<String, Response> {
    let dek_plane = crate::crypto::DekPlane::from_decision(tenant.decision.as_ref(), &ctx.control);
    let owner = TokenOwner::Member(ctx.reader.owner_user_id);
    let account_id = ctx.reader.account_id;
    let creds = match load_creds_bg(&ctx.control, &dek_plane, account_id, "google_meet", owner).await {
        Ok(c) => c,
        Err(e) => return Err(internal(e, "load google_meet creds")),
    };
    let Some(mut creds) = creds else {
        return Err(json_err(
            StatusCode::CONFLICT,
            "google_meet_not_connected",
            "connect Google Meet first",
        ));
    };
    if let Err(e) = GoogleConnector.refresh_if_needed(&state.http_client, &mut creds).await {
        tracing::warn!(error = %e, %account_id, "google_meet token refresh failed");
        return Err(json_err(
            StatusCode::BAD_GATEWAY,
            "google_refresh_failed",
            "could not refresh your Google Meet connection",
        ));
    }
    if let Some(refreshed) = creds.refreshed.as_ref() {
        if let Err(e) = store_token_bundle(&ctx.control, &dek_plane, account_id, "google_meet", owner, refreshed).await {
            tracing::warn!(status = ?e.status(), %account_id, "failed to persist refreshed google_meet token (non-fatal)");
        }
    }
    Ok(creds.access_token)
}

fn scoped_key(ctx: &Ctx) -> String {
    format!("google_meet:{}", ctx.reader.account_id)
}

// ---------------------------------------------------------------------------
// GET meetings
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct MeetingsQuery {
    pub days: Option<i64>,
}

#[derive(Debug, Serialize, PartialEq)]
pub(crate) struct MeetingItem {
    conference_record_id: String,
    start_time: Option<DateTime<Utc>>,
    end_time: Option<DateTime<Utc>>,
    space: Option<String>,
    meeting_code: Option<String>,
    in_progress: bool,
    has_transcript: bool,
    /// `ready` | `processing` | `none` | `in_progress` | `unknown`
    transcript_state: &'static str,
    /// `none` | `allowed` | `ingested`
    capture_state: &'static str,
    capture_id: Option<Uuid>,
    /// Why an `allowed` capture can never produce a copy (`no_transcript`,
    /// `empty_transcript`), if so.
    capture_note: Option<String>,
}

/// `days` query param -> `1..=30` (Google keeps conference records ~30 days).
fn clamp_days(days: Option<i64>) -> i64 {
    days.unwrap_or(DEFAULT_DAYS).clamp(1, MAX_DAYS)
}

fn transcript_label(state: Option<TranscriptState>, in_progress: bool) -> &'static str {
    if in_progress {
        return "in_progress";
    }
    match state {
        Some(TranscriptState::Ready) => "ready",
        Some(TranscriptState::Processing) => "processing",
        Some(TranscriptState::None) => "none",
        None => "unknown",
    }
}

/// Pure assembly of one list item from what Meet and the member's rows say.
pub(crate) fn build_item(
    record: &meet::ConferenceRecord,
    transcript: Option<TranscriptState>,
    meeting_code: Option<&str>,
    captures: &[Capture],
    now: DateTime<Utc>,
) -> MeetingItem {
    let parse = |s: &Option<String>| s.as_deref().and_then(|s| DateTime::parse_from_rfc3339(s).ok()).map(|d| d.with_timezone(&Utc));
    let start_time = parse(&record.start_time);
    let end_time = parse(&record.end_time);
    let in_progress = end_time.is_none();
    let m = capture::resolve(captures, &record.name, record.space.as_deref(), meeting_code, end_time.unwrap_or(now), now);
    // A revoked row reads as `none`: expose no id/note for it.
    let open = m.capture().filter(|_| m.state() != "none");
    MeetingItem {
        conference_record_id: record.name.clone(),
        start_time,
        end_time,
        space: record.space.clone(),
        meeting_code: meeting_code.map(str::to_string),
        in_progress,
        has_transcript: transcript == Some(TranscriptState::Ready),
        transcript_state: transcript_label(transcript, in_progress),
        capture_state: m.state(),
        capture_id: open.map(|c| c.id),
        capture_note: open.and_then(|c| c.terminal_reason.clone()),
    }
}

async fn space_code(http: reqwest::Client, token: String, space: String) -> (String, Option<String>) {
    let code = match meet::get_space(&http, &token, &space).await {
        Ok(sp) => sp.and_then(|sp| sp.meeting_code).and_then(|c| capture::normalize_meeting_code(&c)),
        Err(e) => {
            tracing::warn!(error = %e, %space, "meet spaces.get failed while listing");
            None
        }
    };
    (space, code)
}

async fn record_transcript(http: reqwest::Client, token: String, name: String, ended: bool) -> Option<TranscriptState> {
    if !ended {
        return None;
    }
    match meet::transcript_state(&http, &token, &name).await {
        Ok(s) => Some(s),
        Err(e) => {
            tracing::warn!(error = %e, record = %name, "meet transcripts.list failed while listing");
            None
        }
    }
}

/// `GET /v1/connectors/google/meet/meetings?days=30`
///
/// 200 `{"days": 30, "truncated": false, "meetings": [MeetingItem, ...]}`,
/// newest first. 409 `google_meet_not_connected`, 502 when Meet fails.
pub async fn list_meetings(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Query(q): Query<MeetingsQuery>,
) -> Response {
    let ctx = match context(&state, &tenant, &user).await {
        Ok(c) => c,
        Err(r) => return r,
    };
    let token = match access_token(&state, &tenant, &ctx).await {
        Ok(t) => t,
        Err(r) => return r,
    };
    let days = clamp_days(q.days);
    let now = Utc::now();
    let filter = format!("start_time>=\"{}\"", (now - Duration::days(days)).to_rfc3339());
    let http = &state.http_client;

    let result = crate::graph_rate_limit::scoped(scoped_key(&ctx), async {
        let records = meet::list_conference_records_filtered(http, &token, &filter).await?;
        let enriched: Vec<&meet::ConferenceRecord> = records.iter().take(MAX_ENRICHED).collect();

        // Meeting codes: one spaces.get per distinct space. Owned values only
        // (a stream of borrows trips the higher-ranked-lifetime limit once the
        // handler future must be `Send`).
        let mut spaces: Vec<String> = enriched.iter().filter_map(|r| r.space.clone()).collect();
        spaces.sort_unstable();
        spaces.dedup();
        let codes: HashMap<String, Option<String>> = futures::stream::iter(spaces)
            .map(|s| space_code(http.clone(), token.clone(), s))
            .buffered(ENRICH_CONCURRENCY)
            .collect()
            .await;

        // Transcript state for every ended record.
        let transcripts: Vec<Option<TranscriptState>> = futures::stream::iter(
            enriched.iter().map(|r| (r.name.clone(), r.end_time.is_some())).collect::<Vec<_>>(),
        )
        .map(|(name, ended)| record_transcript(http.clone(), token.clone(), name, ended))
        .buffered(ENRICH_CONCURRENCY)
        .collect()
        .await;
        Ok::<_, ConnectorError>((records, codes, transcripts))
    })
    .await;
    let (records, codes, transcripts) = match result {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(error = %e, account_id = %ctx.reader.account_id, "meet conferenceRecords.list failed");
            return json_err(StatusCode::BAD_GATEWAY, "meet_unavailable", "could not read your Google Meet conferences");
        }
    };

    let captures = match capture::load(&ctx.pool, ctx.reader.account_id, ctx.reader.owner_user_id).await {
        Ok(c) => c,
        Err(e) => return internal(e, "load captures"),
    };
    let truncated = records.len() > MAX_ENRICHED;
    let meetings: Vec<MeetingItem> = records
        .iter()
        .enumerate()
        .map(|(i, r)| {
            let code = r.space.as_deref().and_then(|s| codes.get(s)).and_then(|c| c.as_deref());
            let transcript = transcripts.get(i).copied().flatten();
            build_item(r, transcript, code, &captures, now)
        })
        .collect();
    (StatusCode::OK, Json(json!({"days": days, "truncated": truncated, "meetings": meetings}))).into_response()
}

// ---------------------------------------------------------------------------
// POST / DELETE meetings/{id}/allow
// ---------------------------------------------------------------------------

/// `POST /v1/connectors/google/meet/meetings/{conferenceRecordId}/allow`
///
/// Allows this meeting for the CALLER'S OWN copy and ingests it now when it can.
/// 200 `{"conference_record_id", "capture_id", "capture_state": "allowed"|"ingested",
/// "ingest": "done"|"already_ingested"|"no_transcript"|"empty_transcript"}`;
/// 202 with `"ingest": "in_progress"|"pending_transcript"|"retrying"` when the
/// capture is recorded but the copy cannot land yet (the sweep finishes it,
/// bounded by the capture's 30-day expiry). 404 when the meeting does not exist
/// or is not visible to the caller (pre-approve by meeting code instead).
pub async fn allow_meeting(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(conference_record_id): Path<String>,
) -> Response {
    let Some(name) = capture::normalize_record(&conference_record_id) else {
        return json_err(StatusCode::BAD_REQUEST, "invalid_conference_record_id", "malformed conference record id");
    };
    let ctx = match context(&state, &tenant, &user).await {
        Ok(c) => c,
        Err(r) => return r,
    };
    let token = match access_token(&state, &tenant, &ctx).await {
        Ok(t) => t,
        Err(r) => return r,
    };
    let record = match crate::graph_rate_limit::scoped(
        scoped_key(&ctx),
        meet::get_conference_record(&state.http_client, &token, &name),
    )
    .await
    {
        Ok(Some(r)) => r,
        Ok(None) => {
            return json_err(
                StatusCode::NOT_FOUND,
                "meeting_not_found",
                "this meeting does not exist, has expired, or is not visible to you; if you were an attendee \
                 rather than the organizer, approve it by meeting code instead",
            )
        }
        Err(e) => {
            tracing::warn!(error = %e, "meet conferenceRecords.get failed");
            return json_err(StatusCode::BAD_GATEWAY, "meet_unavailable", "could not read this meeting from Google Meet");
        }
    };
    let row = match capture::allow_record(&ctx.pool, ctx.reader.account_id, ctx.reader.owner_user_id, &name, record.space.as_deref()).await {
        Ok(r) => r,
        Err(e) => return internal(e, "allow_record"),
    };
    if row.status == capture::STATUS_INGESTED {
        return (StatusCode::OK, Json(allow_body(&name, &row, "already_ingested"))).into_response();
    }

    let outcome = crate::graph_rate_limit::scoped(
        scoped_key(&ctx),
        meet::process_record_opt_in(&ctx.plane, &state.http_client, &ctx.control, &state, ctx.reader, &token, &record, RecordScope::OwnOnly),
    )
    .await;
    let ingest = match &outcome {
        Ok(RecordOutcome::Done { copies, .. }) if *copies > 0 => "done",
        Ok(RecordOutcome::Done { .. }) => "retrying",
        Ok(RecordOutcome::NoTranscript) => "no_transcript",
        Ok(RecordOutcome::TranscriptPending) => "pending_transcript",
        Ok(RecordOutcome::NotEnded) => "in_progress",
        Ok(RecordOutcome::NothingApproved) => "retrying",
        Err(e) => {
            tracing::warn!(error = %e, conference_record_id = %name, "immediate ingest after allow failed; sweep will retry");
            "retrying"
        }
    };
    // Re-read the row: the single source of truth for the response.
    let row = match capture::load(&ctx.pool, ctx.reader.account_id, ctx.reader.owner_user_id).await {
        Ok(rows) => rows.into_iter().find(|c| c.conference_record_id.as_deref() == Some(name.as_str())).unwrap_or(row),
        Err(_) => row,
    };
    let ingest = match (row.status.as_str(), row.terminal_reason.as_deref()) {
        (capture::STATUS_INGESTED, _) => "done",
        (_, Some("empty_transcript")) => "empty_transcript",
        (_, Some("no_transcript")) => "no_transcript",
        _ => ingest,
    };
    let status = match ingest {
        "in_progress" | "pending_transcript" | "retrying" => StatusCode::ACCEPTED,
        _ => StatusCode::OK,
    };
    (status, Json(allow_body(&name, &row, ingest))).into_response()
}

fn allow_body(name: &str, row: &Capture, ingest: &str) -> serde_json::Value {
    json!({
        "conference_record_id": name,
        "capture_id": row.id,
        "capture_state": if row.status == capture::STATUS_INGESTED { "ingested" } else { "allowed" },
        "ingest": ingest,
    })
}

/// `DELETE /v1/connectors/google/meet/meetings/{conferenceRecordId}/allow`
///
/// 200 `{"conference_record_id", "revoked": true, "capture_state": "none"}` when an
/// open capture was revoked. 409 `{"error": "already_ingested", "revoked": false,
/// "capture_state": "ingested", "message": ...}` when the copy already landed
/// (removing ingested atoms is out of scope). 404 when there is nothing to revoke.
pub async fn revoke_meeting(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(conference_record_id): Path<String>,
) -> Response {
    let Some(name) = capture::normalize_record(&conference_record_id) else {
        return json_err(StatusCode::BAD_REQUEST, "invalid_conference_record_id", "malformed conference record id");
    };
    let ctx = match context(&state, &tenant, &user).await {
        Ok(c) => c,
        Err(r) => return r,
    };
    match capture::revoke_by_record(&ctx.pool, ctx.reader.account_id, ctx.reader.owner_user_id, &name).await {
        Ok(out) => revoke_response(out, Some(&name)),
        Err(e) => internal(e, "revoke_by_record"),
    }
}

fn revoke_response(out: RevokeOutcome, name: Option<&str>) -> Response {
    match out {
        RevokeOutcome::Revoked(id) => (
            StatusCode::OK,
            Json(json!({"conference_record_id": name, "capture_id": id, "revoked": true, "capture_state": "none"})),
        )
            .into_response(),
        RevokeOutcome::AlreadyIngested => (
            StatusCode::CONFLICT,
            Json(json!({
                "error": "already_ingested",
                "message": "this meeting was already ingested; revoking now does not remove the copy",
                "conference_record_id": name,
                "revoked": false,
                "capture_state": "ingested",
            })),
        )
            .into_response(),
        RevokeOutcome::NotFound => (
            StatusCode::NOT_FOUND,
            Json(json!({
                "error": "no_capture",
                "message": "there is no open capture for this meeting",
                "conference_record_id": name,
                "revoked": false,
                "capture_state": "none",
            })),
        )
            .into_response(),
    }
}

// ---------------------------------------------------------------------------
// POST / GET / DELETE captures (pre-approval)
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct PreApproveBody {
    pub meeting_code: Option<String>,
    pub space: Option<String>,
}

/// Validate a pre-approval body: exactly ONE of `meeting_code` / `space`.
pub(crate) fn parse_pre_approval(body: &PreApproveBody) -> Result<(Option<String>, Option<String>), &'static str> {
    let code = body.meeting_code.as_deref().map(str::trim).filter(|s| !s.is_empty());
    let space = body.space.as_deref().map(str::trim).filter(|s| !s.is_empty());
    match (code, space) {
        (Some(_), Some(_)) | (None, None) => Err("provide exactly one of meeting_code or space"),
        (Some(c), None) => capture::normalize_meeting_code(c)
            .map(|c| (Some(c), None))
            .ok_or("meeting_code must look like abc-defg-hij (or a meet.google.com link)"),
        (None, Some(s)) => capture::normalize_space(s)
            .map(|s| (None, Some(s)))
            .ok_or("space must look like spaces/{id}"),
    }
}

/// `POST /v1/connectors/google/meet/captures` with `{"meeting_code": "abc-defg-hij"}`
/// or `{"space": "spaces/{id}"}`: pre-approve an upcoming or running meeting for
/// the caller's OWN copy. It covers the first conference record of that meeting
/// that ends after now (a recurring meeting needs a fresh approval each time) and
/// expires after 30 days if unused.
///
/// 201 `{"capture": Capture, "capture_state": "allowed", "created": true}`; 200
/// with `"created": false` when an identical open pre-approval already exists.
/// 400 on a malformed body, 409 `google_meet_not_connected`.
pub async fn pre_approve(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Json(body): Json<PreApproveBody>,
) -> Response {
    let (code, space) = match parse_pre_approval(&body) {
        Ok(v) => v,
        Err(m) => return json_err(StatusCode::BAD_REQUEST, "invalid_request", m),
    };
    let ctx = match context(&state, &tenant, &user).await {
        Ok(c) => c,
        Err(r) => return r,
    };
    let token = match access_token(&state, &tenant, &ctx).await {
        Ok(t) => t,
        Err(r) => return r,
    };
    // Best effort: learn the other identifier so the sweep can match on either.
    // Failure (private space, transient error) is fine; the given one still works.
    let lookup = code.as_deref().map(|c| format!("spaces/{c}")).or_else(|| space.clone());
    let (mut code, mut space) = (code, space);
    if let Some(name) = lookup {
        match crate::graph_rate_limit::scoped(scoped_key(&ctx), meet::get_space(&state.http_client, &token, &name)).await {
            Ok(Some(sp)) => {
                if space.is_none() {
                    space = capture::normalize_space(&sp.name);
                }
                if code.is_none() {
                    code = sp.meeting_code.as_deref().and_then(capture::normalize_meeting_code);
                }
            }
            Ok(None) => {}
            Err(e) => tracing::warn!(error = %e, "meet spaces.get failed during pre-approval (non-fatal)"),
        }
    }
    match capture::pre_approve(&ctx.pool, ctx.reader.account_id, ctx.reader.owner_user_id, space.as_deref(), code.as_deref()).await {
        Ok((row, created)) => (
            if created { StatusCode::CREATED } else { StatusCode::OK },
            Json(json!({"capture": row, "capture_state": "allowed", "created": created})),
        )
            .into_response(),
        Err(e) => internal(e, "pre_approve"),
    }
}

/// `GET /v1/connectors/google/meet/captures`: the caller's own open and ingested
/// captures (revoked rows are omitted), newest first: `{"captures": [Capture, ...]}`.
pub async fn list_captures(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
) -> Response {
    let ctx = match context(&state, &tenant, &user).await {
        Ok(c) => c,
        Err(r) => return r,
    };
    match capture::list_recent(&ctx.pool, ctx.reader.account_id, ctx.reader.owner_user_id).await {
        Ok(rows) => (StatusCode::OK, Json(json!({"captures": rows}))).into_response(),
        Err(e) => internal(e, "list_recent"),
    }
}

/// `DELETE /v1/connectors/google/meet/captures/{capture_id}`: cancel a capture
/// (typically a pre-approval that has not matched yet). Same responses as
/// [`revoke_meeting`].
pub async fn revoke_capture(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(capture_id): Path<Uuid>,
) -> Response {
    let ctx = match context(&state, &tenant, &user).await {
        Ok(c) => c,
        Err(r) => return r,
    };
    match capture::revoke_by_id(&ctx.pool, ctx.reader.account_id, ctx.reader.owner_user_id, capture_id).await {
        Ok(out) => revoke_response(out, None),
        Err(e) => internal(e, "revoke_by_id"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(name: &str, space: Option<&str>, end: Option<&str>) -> meet::ConferenceRecord {
        meet::ConferenceRecord {
            name: name.to_string(),
            start_time: Some("2026-10-05T10:00:00Z".to_string()),
            end_time: end.map(str::to_string),
            space: space.map(str::to_string),
        }
    }

    fn row(record: Option<&str>, space: Option<&str>, status: &str) -> Capture {
        let now = Utc::now();
        Capture {
            id: Uuid::new_v4(),
            account_id: Uuid::nil(),
            owner_user_id: Uuid::nil(),
            conference_record_id: record.map(str::to_string),
            space_name: space.map(str::to_string),
            meeting_code: None,
            status: status.to_string(),
            terminal_reason: None,
            created_at: now - Duration::days(10),
            expires_at: now + Duration::days(20),
            ingested_at: (status == "ingested").then_some(now),
            revoked_at: (status == "revoked").then_some(now),
        }
    }

    #[test]
    fn item_without_a_capture_is_state_none() {
        let now = Utc::now();
        let r = rec("conferenceRecords/a", Some("spaces/s"), Some("2026-10-05T11:00:00Z"));
        let it = build_item(&r, Some(TranscriptState::Ready), Some("abc-defg-hij"), &[], now);
        assert_eq!(it.capture_state, "none");
        assert!(it.has_transcript);
        assert_eq!(it.transcript_state, "ready");
        assert_eq!(it.meeting_code.as_deref(), Some("abc-defg-hij"));
        assert!(it.capture_id.is_none());
    }

    #[test]
    fn item_reflects_allowed_and_ingested_and_hides_revoked() {
        let now = Utc::now();
        let r = rec("conferenceRecords/a", None, Some("2026-10-05T11:00:00Z"));
        for (status, want) in [("allowed", "allowed"), ("ingested", "ingested"), ("revoked", "none")] {
            let rows = [row(Some("conferenceRecords/a"), None, status)];
            let it = build_item(&r, Some(TranscriptState::None), None, &rows, now);
            assert_eq!(it.capture_state, want, "{status}");
            assert!(!it.has_transcript);
            assert_eq!(it.transcript_state, "none");
        }
    }

    #[test]
    fn ongoing_meeting_is_in_progress_and_a_preapproval_covers_it() {
        let now = Utc::now();
        let r = rec("conferenceRecords/live", Some("spaces/s"), None);
        let pre = row(None, Some("spaces/s"), "allowed");
        let it = build_item(&r, None, None, &[pre], now);
        assert!(it.in_progress);
        assert_eq!(it.transcript_state, "in_progress");
        assert_eq!(it.capture_state, "allowed");
    }

    #[test]
    fn pre_approval_body_requires_exactly_one_valid_identifier() {
        let b = |c: Option<&str>, s: Option<&str>| PreApproveBody { meeting_code: c.map(str::to_string), space: s.map(str::to_string) };
        assert_eq!(
            parse_pre_approval(&b(Some("https://meet.google.com/abc-defg-hij"), None)).unwrap(),
            (Some("abc-defg-hij".to_string()), None)
        );
        assert_eq!(parse_pre_approval(&b(None, Some("spaces/Xy1"))).unwrap(), (None, Some("spaces/Xy1".to_string())));
        assert!(parse_pre_approval(&b(None, None)).is_err());
        assert!(parse_pre_approval(&b(Some("abc-defg-hij"), Some("spaces/x"))).is_err());
        assert!(parse_pre_approval(&b(Some("nope"), None)).is_err());
        assert!(parse_pre_approval(&b(None, Some("spaces/a/b"))).is_err());
        assert!(parse_pre_approval(&b(Some("  "), None)).is_err());
    }

    #[test]
    fn days_are_clamped_to_googles_retention_window() {
        assert_eq!(clamp_days(Some(99)), 30);
        assert_eq!(clamp_days(Some(0)), 1);
        assert_eq!(clamp_days(Some(-5)), 1);
        assert_eq!(clamp_days(Some(7)), 7);
        assert_eq!(clamp_days(None), 30);
    }

    #[test]
    fn revoke_response_statuses() {
        assert_eq!(revoke_response(RevokeOutcome::Revoked(Uuid::nil()), Some("x")).status(), StatusCode::OK);
        assert_eq!(revoke_response(RevokeOutcome::AlreadyIngested, Some("x")).status(), StatusCode::CONFLICT);
        assert_eq!(revoke_response(RevokeOutcome::NotFound, None).status(), StatusCode::NOT_FOUND);
    }

    fn user(account_id: Uuid) -> UserContext {
        UserContext { user_id: Uuid::new_v4(), account_id, role: "member".into(), device_id: String::new() }
    }

    /// The org gate is the SAME flag as the sweep's (`org_google_meet_enabled_for`,
    /// default OFF): an org caller is refused with 403 before any pool is touched,
    /// while a personal caller is never refused by it (here it reaches the pool
    /// check and gets 503 only because this bare test state has no database).
    #[tokio::test]
    async fn org_callers_are_refused_while_the_org_meet_flag_is_off_and_personal_is_not() {
        if std::env::var("ORG_GOOGLE_MEET_ENABLED").is_ok() {
            eprintln!("skipping: ORG_GOOGLE_MEET_ENABLED is set in this environment");
            return;
        }
        let state = crate::test_app_state();
        let account = Uuid::new_v4();
        let org = Tenant {
            store: None,
            pool: None,
            decision: Some(crate::tenancy::runtime::TenancyDecision::Org(
                crate::tenancy::runtime::TenantContext {
                    org_id: "org_meet_route_gate".to_string(),
                    account_id: account,
                    user_id: Uuid::new_v4(),
                    pool: sqlx::postgres::PgPoolOptions::new()
                        .connect_lazy("postgres://localhost/unused")
                        .expect("lazy"),
                    page_pool: None,
                },
            )),
            class: crate::route_class::RouteClass::Other,
        };
        let Err(refused) = context(&state, &org, &user(account)).await else { panic!("org flag off must refuse") };
        assert_eq!(refused.status(), StatusCode::FORBIDDEN);
        let personal = Tenant {
            store: None,
            pool: None,
            decision: None,
            class: crate::route_class::RouteClass::Other,
        };
        let Err(r) = context(&state, &personal, &user(account)).await else {
            panic!("no pool in a bare test state")
        };
        assert_eq!(
            r.status(),
            StatusCode::SERVICE_UNAVAILABLE,
            "personal is never refused by the org gate"
        );
    }
}
