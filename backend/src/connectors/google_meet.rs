//! Google Meet ingest (gsuite Train G3 PR 2 — `docs/connectors/
//! google-workspace.md` §11, the Meetings archetype, contract §8).
//!
//! # The scope question this PR resolves first
//!
//! `google-workspace.md`'s scope table (§4) froze Meet's data scope at
//! `https://www.googleapis.com/auth/meetings.space.readonly` and flagged
//! whether transcript BODIES additionally need a Drive scope as an open item.
//! Verified against Google's live REST API reference
//! (`developers.google.com/workspace/meet/api/reference/rest/v2/
//! conferenceRecords.transcripts.entries` and its sibling `.../list` method
//! page, fetched 2026-09-07): `TranscriptEntry.text` — "The transcribed text
//! of the participant's voice" — is returned DIRECTLY by
//! `conferenceRecords.transcripts.entries.list`, and that method's own
//! "Authorization scopes" section requires only `meetings.space.created` OR
//! `meetings.space.readonly` — the SAME frozen scope already declared, no
//! widening. The `Transcript` resource's own `docsDestination` field (a
//! Google-Docs EXPORT location) is a separate, optional artifact this
//! connector never reads — the entries API is the source of truth here, not
//! the Docs export. **No scope change.** `conferenceRecords.list`,
//! `conferenceRecords.participants.list`, and `conferenceRecords.
//! transcripts.list` were checked the same way and require the identical two
//! scopes — every call this module makes is covered.
//!
//! # Per-meeting opt-in (founder decision 2026-10-06 - supersedes auto-ingest)
//!
//! **Nothing is ingested unless the member allowed that specific meeting**, like
//! Microsoft Teams. The earlier "auto-ingest every transcribed meeting" sweep is
//! gone. A member's consent is a `google_meet_captures` row (control `0269` /
//! tenant `0111`; `services::google_meet_capture`): by conference record
//! (`POST .../meetings/{id}/allow`) or a pre-approval by meeting code / space
//! (`POST .../captures`). [`process_record_opt_in`] is the single decision point:
//! a copy lands for a member only if THAT member has an open row covering the
//! record, and the row is then marked `ingested`.
//!
//! * **Fan-out is per attendee, never "one approval for everyone".** The
//!   organizer's token is the only credential Google honours for a record, so the
//!   organizer's sweep also serves attendees - but it consults each resolved
//!   attendee's OWN rows and writes only that attendee's own copy, into their
//!   own ocean, only if they approved. An approval is consent for the approver's
//!   own copy; each attendee approves their own (by meeting code if they cannot
//!   see the record id, since Meet shows records only to the organizer).
//! * **Late approval still lands.** Records nobody approved are ledgered and the
//!   watermark moves on (the meeting list is read live from Meet for ~30 days, so
//!   nothing is stored about them). Approving later ingests immediately (route)
//!   and, if that cannot finish, [`sync_owner_once`]'s phase 2 re-fetches the
//!   member's approved-but-not-ingested captures BY NAME every tick until their
//!   30-day expiry, independent of the watermark. A pre-approval covers only a
//!   record that ends after it was given.
//! * The permanent-vs-transient rules below are unchanged and still govern every
//!   copy that is written.
//!
//! # Fan-out, not container routing (founder decision, not re-litigated here)
//!
//! (Superseded for WHEN a copy is written - see "Per-meeting opt-in" above - but
//! unchanged for WHERE: each attendee's own member-private ocean.)
//!
//! Contract §8.3's audience problem is resolved for Meet as **option 1**:
//! fan out to attendees individually, each into their OWN member-private
//! ocean — the Mailbox archetype's own-ocean-only shape (contract §7),
//! mirroring Outlook/Gmail's per-member ingest (`TokenOwner::Member`), NOT
//! Chat's shared-container-with-disclosure shape (option 2). An attendee with
//! no resolvable, connected Google identity gets NOTHING for that meeting —
//! this is not held, retried, or queued pending a future identity link, and
//! one unresolved attendee never fails the rest of the meeting's fan-out.
//!
//! **"No linked identity" is a PERMANENT classification and must never be
//! confused with a TRANSIENT failure** (fixed post-review, before merge — an
//! earlier version of [`resolve_attendees`] conflated the two, silently
//! dropping a hard control-DB error the identical way it dropped a genuine
//! `Ok(None)`; for a declared-immutable, no-repair surface that meant a
//! transient hiccup could permanently cost an attendee their transcript with
//! no error ever surfaced). [`resolve_attendees`] and
//! [`ingest_transcript_for_attendee`]'s caller ([`fan_out_and_count`]) both
//! keep these separate: a hard failure anywhere in a pass means
//! [`process_conference_record`] returns `Err` and the WHOLE record retries
//! next tick — see each function's own doc.
//!
//! # Two planes, never mixed (Meet org-plane lift)
//!
//! A sweep pass runs on exactly ONE plane ([`MeetPlane`]), decided by which
//! account OWNS the reader's `google_meet` connection:
//!
//! * **[`MeetPlane::Personal`]** — the original, unchanged path. The reader is
//!   a personal-account member; attendees are resolved with the cross-account
//!   `sub` resolver ([`resolve_attendee`]) and each lands in their own
//!   personal primary ocean on the shared consumer database. A RESOLVED
//!   attendee whose own account is an ORG account is still refused
//!   ([`resolve_attendee`]'s own doc): org content is only ever written by an
//!   org-plane pass.
//! * **[`MeetPlane::Org`]** — the org lift, behind the per-org
//!   `google_meet` flag (`routes::oauth::org_google_meet_enabled_for`,
//!   default OFF, mirroring Gmail's/Chat's lifts). The reader is a member of
//!   an org; the sync cursor and the ingested-conference ledger live on that
//!   org's TENANT database (the tenant twins `0077`/`0078` of control
//!   `0206`/`0207`), the token stays on the control plane (`oauth_tokens` is
//!   control-only; the account DEK is read from the tenant plane), and
//!   attendees are resolved ONLY within the reader's own org account
//!   ([`resolve_attendee_in_org`], the account-scoped resolver) — each
//!   landing in their own `kind='member'` primary ocean in that org's tenant
//!   database.
//!
//! **The two planes never cross.** An org pass can never write a personal
//! ocean (a personal attendee simply does not resolve inside the org), and a
//! personal pass can never write an org ocean (`resolve_attendee` refuses org
//! accounts). **A person who is in BOTH** (the same Google `sub` connected
//! under a personal account AND under an org account) gets, from any single
//! pass, a copy only on that pass's own plane: an org-connected reader's
//! meeting lands in the attendee's ORG member ocean and nowhere else; a
//! personal-connected reader's meeting lands in their personal ocean only if
//! the cross-account resolver finds exactly one account for the `sub` — and
//! it fails closed (resolves nothing) once the `sub` is connected under two
//! accounts, which is exactly the "in both" case, so that attendee gets no
//! personal copy rather than a guessed one. Documented, deliberate: never
//! guess a plane, never double-write.
//!
//! An org attendee with no live member-private ocean (not yet provisioned,
//! archived on offboarding) is treated like an unresolved attendee: no copy,
//! not held, not retried, and never a hard failure that would wedge the
//! reader's watermark (see [`ingest_transcript_for_attendee`]).
//!
//! # Identity resolution: `sub`, never email (contract C5)
//!
//! Confirmed against the same REST reference fetched for the scope question:
//! `Participant.signedinUser` carries `user` (`"users/{sub}"`, the Google
//! `sub`) and `displayName` — never an email address, on any Participant
//! variant (`anonymousUser`/`phoneUser` carry a display name only, and are
//! structurally NEVER resolvable to a CoralSwarm identity — the same
//! `sub`-not-email discipline Chat's own C5 row documents). Every attendee is
//! resolved via [`super::google_chat::resolve_google_identity_by_sub`] — the
//! CROSS-ACCOUNT bootstrap resolver, not the account-scoped
//! `resolve_google_identity_in_account` — because a meeting's participants
//! are, in the general case, NOT members of the reader's own account (see
//! this module's own "Why the cross-account resolver" note below). This
//! reuses one of the two EXISTING resolver shapes `google-workspace.md`
//! (lines 676-693) documents; no third shape is introduced. Fails closed on
//! ambiguity (a `sub` connected under more than one CoralSwarm account
//! resolves to nothing), the same posture that resolver already has for
//! Chat's `ADDED_TO_SPACE` bootstrap.
//!
//! ## Why the cross-account resolver, not the account-scoped one
//!
//! `google-workspace.md`'s own account-scoped resolver
//! (`resolve_google_identity_in_account`) is for a caller that ALREADY knows
//! which account it is acting for (Chat's roster-seating/disclosure paths,
//! both scoped to the space's own routed account). A Meet sweep discovers a
//! meeting through ONE connected member's own `conferenceRecords.list`
//! visibility (Google's Meet API scopes conference records to spaces the
//! caller ORGANIZED, not merely attended), so every OTHER attendee is, in the
//! general case, a person this sweep has no `account_id` for yet — the exact
//! "genuine account-discovery case" `resolve_google_identity_by_sub`'s own
//! doc comment describes for Chat's `ADDED_TO_SPACE` bootstrap. This is a
//! second legitimate call site for that SAME resolver, not a new shape — see
//! the PR description for this call-out in full, in case a reviewer expected
//! the account-scoped resolver here instead.
//!
//! # Immutable, deliberately (C9/C15 — a discrepancy worth flagging)
//!
//! `google-workspace.md`'s own capability matrix (§3, C9/C15 rows) declares
//! Meet transcripts immutable — "Meet transcripts immutable per §8.4
//! carve-out" — and this module implements exactly that: no edit/delete
//! propagation, no subscription to any update signal (there is no push
//! subscription for Meet in this PR at all — see "Delivery" below). **Flagged
//! for the record:** `CONNECTOR_CONTRACT.md` §8.6 ("Declared: Google Meet")
//! reads the opposite way — "Per §8.4, the transcript is correctable, not
//! immutable... C15 applies here, not C9's immutability declaration." The
//! task that produced this PR named `google-workspace.md` as the single
//! authoritative source and explicitly directed the immutable/no-propagation
//! behavior, so that is what is built and pinned by a test
//! (`google_meet_reingest_never_changes_existing_content_pg`) — but the two
//! docs disagree and a founder call on which one is stale would be worth
//! having; this is surfaced in the PR description too, not just here.
//!
//! # Delivery: pull-only backstop, push deferred (contract C7)
//!
//! `google-workspace.md` §3 C7 row: "Push planned — Workspace Events API,
//! same family. Not built in this PR." This module is the pull half:
//! [`sync_owner_once`] is driven by `workers::google_meet_sweep`'s tenant-loop
//! tick (30-minute cadence, matching every other Google pull sweep in this
//! codebase) rather than any push subscription. Deferred, not silently
//! skipped: a Workspace Events subscription for Meet would need the same
//! GCP/Pub/Sub provisioning spine Chat's own `chat-events` topic uses
//! (`scripts/provision_google_workspace.sh`), a new webhook arm in
//! `routes::google_push`, and `workers::google_watch_renewal` support for a
//! `'meet'` surface — meaningful scope on its own, out of this PR.
//!
//! # Storage
//!
//! `google_meet_sync_state` (control `0206`/tenant `0077`) — one row per
//! connected member, the pull watermark (`last_synced_end_time`) plus the
//! same atomic-claim/fencing-token shape `gmail_everything_mode` (`0197`)
//! already established. `google_meet_ingested_conferences` (control
//! `0205`/tenant `0077`) — a per-(reader account, conference record) ledger:
//! cheap re-processing dedup plus attendee/resolved counts for
//! observability. Neither table stores a `graph_id` — the fan-out
//! destination is resolved FRESH, per attendee, on every ingest via
//! [`crate::routes::slack::resolve_primary_graph`], never persisted; see
//! each migration's own header for the full column-by-column rationale.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::Deserialize;
use sqlx::PgPool;
use uuid::Uuid;

use super::google::google_send;
use super::ConnectorError;
use crate::middleware::auth::UserContext;
use crate::ingest::atom::IngestAtomRequest;
use crate::routes::slack::{account_plane, resolve_primary_graph, AccountPlane, IngestCaller};
use crate::store::{PostgresStore, Store};
use crate::tenancy::extractor::Tenant;
use crate::tenancy::runtime::{TenancyDecision, TenantContext};
use crate::AppState;

/// `atoms.source_type` for every Meet transcript atom. Deliberately the bare
/// `"meet"` literal — `coralswarm_core::chunker::ChunkStrategy::
/// for_source_type` already maps it to `ChunkStrategy::Transcript` (alongside
/// `"transcript"`/`"zoom"`/`"teams_transcript"`), so speaker-turn-aware
/// chunking works for free as long as [`build_transcript_content`] emits the
/// `"{speaker}: {text}"` shape that strategy expects — no new chunker case
/// needed for this connector.
pub(crate) const SOURCE_TYPE: &str = "meet";

const MEET_API_BASE: &str = "https://meet.googleapis.com/v2";

#[cfg(test)]
tokio::task_local! {
    /// Test-only seam: points every Meet REST call made inside the scope at a
    /// local mock server. Task-local, so it is parallel-test-safe and can never
    /// leak into production code (it does not exist outside `cfg(test)`).
    pub(crate) static TEST_API_BASE: String;
}

fn api_base() -> String {
    #[cfg(test)]
    if let Ok(base) = TEST_API_BASE.try_with(|b| b.clone()) {
        return base;
    }
    super::google_transport::endpoint(MEET_API_BASE)
}

/// `conferenceRecords.list`'s own documented cap (values above 100 are
/// coerced) — kept well under it since a personal Meet history rarely
/// approaches this in one page.
const LIST_PAGE_SIZE: u32 = 50;

/// `conferenceRecords.transcripts.entries.list`'s own documented maximum.
const ENTRIES_PAGE_SIZE: u32 = 100;

/// Per-tick, per-owner cap on how many conference records this sweep will
/// fully process (transcript + participants + fan-out) — same "a tick must
/// have a bounded worst case" reasoning `gmail_everything_mode::
/// OWNERS_PER_TICK` documents, applied to conference records instead of
/// owners. A personal Meet history rarely produces more than a handful of
/// completed, transcribed meetings between 30-minute sweeps.
const MAX_CONFERENCE_RECORDS_PER_TICK: usize = 10;

/// Safety valve on total conference records fetched from a single `list`
/// call's pagination before this sweep gives up and processes whatever it
/// already has — never an unbounded page walk.
const MAX_CONFERENCE_RECORDS_FETCHED: usize = 200;

/// Safety valve on transcript-entry and participant pagination — a real
/// meeting transcript is bounded by meeting length, not by anything this
/// constant is likely to hit; it exists so a malformed/adversarial response
/// (an endless `nextPageToken` loop) cannot hang a sweep tick forever.
const MAX_PAGES: usize = 25;

/// First-run lookback when `google_meet_sync_state.last_synced_end_time` is
/// `NULL` (a member who just connected `google_meet`). Bounded rather than
/// "since the beginning of time": `ConferenceRecord.expireTime` is 30 days
/// after the meeting ends per Google's own API reference, so nothing older
/// than 30 days is reachable regardless — 7 days is a conservative, useful
/// default rather than racing that expiry on a member's very first sweep.
pub(crate) fn default_lookback() -> chrono::Duration {
    chrono::Duration::days(7)
}

// ---------------------------------------------------------------------------
// Meet REST API v2 shapes — see this module's own doc for the scope citation.
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct ConferenceRecordsListResponse {
    #[serde(default, rename = "conferenceRecords")]
    conference_records: Vec<ConferenceRecord>,
    #[serde(default, rename = "nextPageToken")]
    next_page_token: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct ConferenceRecord {
    /// `"conferenceRecords/{id}"` — also this transcript's `source_id`
    /// anchor (see [`ingest_transcript_for_attendee`]).
    pub(crate) name: String,
    #[serde(default, rename = "startTime")]
    pub(crate) start_time: Option<String>,
    /// Unset for an ongoing conference — [`sync_owner_once`] skips any
    /// record with no `end_time` (nothing to transcript-check yet).
    #[serde(default, rename = "endTime")]
    pub(crate) end_time: Option<String>,
    /// `"spaces/{id}"` - the meeting space this record is an occurrence of.
    /// What a space / meeting-code pre-approval is matched against.
    #[serde(default)]
    pub(crate) space: Option<String>,
}

/// `spaces.get` - only the fields the opt-in matching and listing need.
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct Space {
    #[serde(default)]
    pub(crate) name: String,
    /// `"abc-defg-hij"`.
    #[serde(default, rename = "meetingCode")]
    pub(crate) meeting_code: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TranscriptsListResponse {
    #[serde(default)]
    transcripts: Vec<Transcript>,
}

#[derive(Debug, Clone, Deserialize)]
struct Transcript {
    /// `"conferenceRecords/{cr}/transcripts/{t}"`.
    name: String,
    #[serde(default)]
    state: String,
}

const TRANSCRIPT_STATE_FILE_GENERATED: &str = "FILE_GENERATED";

#[derive(Debug, Deserialize)]
struct TranscriptEntriesListResponse {
    #[serde(default, rename = "transcriptEntries")]
    transcript_entries: Vec<TranscriptEntry>,
    #[serde(default, rename = "nextPageToken")]
    next_page_token: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct TranscriptEntry {
    /// `"conferenceRecords/{cr}/participants/{p}"` — joins against
    /// [`Participant::name`] to label a speaker turn.
    #[serde(default)]
    pub(crate) participant: String,
    #[serde(default)]
    pub(crate) text: String,
}

#[derive(Debug, Deserialize)]
struct ParticipantsListResponse {
    #[serde(default)]
    participants: Vec<Participant>,
    #[serde(default, rename = "nextPageToken")]
    next_page_token: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct Participant {
    /// `"conferenceRecords/{cr}/participants/{p}"`.
    pub(crate) name: String,
    /// `None` for `anonymousUser`/`phoneUser` — those two variants are
    /// STRUCTURALLY never resolvable to a CoralSwarm identity (no Google
    /// `sub` exists for them at all), so they are simply excluded from
    /// fan-out, same treatment as any other unresolved attendee.
    #[serde(default, rename = "signedinUser")]
    pub(crate) signedin_user: Option<SignedinUser>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct SignedinUser {
    /// `"users/{sub}"` — never an email (contract C5; see this module's own
    /// doc).
    #[serde(default)]
    pub(crate) user: String,
    #[serde(default, rename = "displayName")]
    pub(crate) display_name: String,
}

fn parse_google_timestamp(s: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(s).ok().map(|d| d.with_timezone(&Utc))
}

// ---------------------------------------------------------------------------
// HTTP calls
// ---------------------------------------------------------------------------

async fn list_conference_records_page(
    http: &reqwest::Client,
    access_token: &str,
    filter: &str,
    page_token: Option<&str>,
) -> Result<ConferenceRecordsListResponse, ConnectorError> {
    let base = api_base();
    let mut url = format!(
        "{base}/conferenceRecords?pageSize={LIST_PAGE_SIZE}&filter={}",
        urlencoding::encode(filter)
    );
    if let Some(pt) = page_token {
        url.push_str("&pageToken=");
        url.push_str(&urlencoding::encode(pt));
    }
    let resp = google_send(|| http.get(super::google_transport::endpoint(&url)).bearer_auth(access_token)).await?;
    if !resp.status().is_success() {
        let status = resp.status();
        return Err(ConnectorError::upstream(status, format!("meet conferenceRecords.list failed: {status}")));
    }
    resp.json()
        .await
        .map_err(|e| ConnectorError::Exchange(format!("meet conferenceRecords.list parse: {e}")))
}

/// Every completed conference record (`endTime` set) whose `endTime` is at
/// or after `since`, newest concerns aside — [`sync_owner_once`] sorts and
/// bounds the result itself. Bounded total fetch: [`MAX_CONFERENCE_RECORDS_FETCHED`]
/// across at most [`MAX_PAGES`] pages.
pub(crate) async fn list_conference_records_since(
    http: &reqwest::Client,
    access_token: &str,
    since: DateTime<Utc>,
) -> Result<Vec<ConferenceRecord>, ConnectorError> {
    let filter = format!("end_time>=\"{}\"", since.to_rfc3339());
    let mut out = Vec::new();
    let mut page_token: Option<String> = None;
    for _ in 0..MAX_PAGES {
        let page = list_conference_records_page(http, access_token, &filter, page_token.as_deref()).await?;
        out.extend(page.conference_records);
        if out.len() >= MAX_CONFERENCE_RECORDS_FETCHED {
            break;
        }
        match page.next_page_token {
            Some(t) if !t.is_empty() => page_token = Some(t),
            _ => break,
        }
    }
    Ok(out)
}

/// Conference records matching an arbitrary Meet `filter` (e.g. `start_time>="..."`),
/// newest first as Google returns them, same bounded walk as
/// [`list_conference_records_since`].
pub(crate) async fn list_conference_records_filtered(
    http: &reqwest::Client,
    access_token: &str,
    filter: &str,
) -> Result<Vec<ConferenceRecord>, ConnectorError> {
    let mut out = Vec::new();
    let mut page_token: Option<String> = None;
    for _ in 0..MAX_PAGES {
        let page = list_conference_records_page(http, access_token, filter, page_token.as_deref()).await?;
        out.extend(page.conference_records);
        if out.len() >= MAX_CONFERENCE_RECORDS_FETCHED {
            break;
        }
        match page.next_page_token {
            Some(t) if !t.is_empty() => page_token = Some(t),
            _ => break,
        }
    }
    Ok(out)
}

/// `conferenceRecords.get`. `Ok(None)` for 403/404: the record does not exist,
/// has expired (~30 days after it ended) or is not visible to this token.
pub(crate) async fn get_conference_record(
    http: &reqwest::Client,
    access_token: &str,
    conference_record_name: &str,
) -> Result<Option<ConferenceRecord>, ConnectorError> {
    let base = api_base();
    let url = format!("{base}/{conference_record_name}");
    let resp = google_send(|| http.get(super::google_transport::endpoint(&url)).bearer_auth(access_token)).await?;
    let status = resp.status();
    if status == reqwest::StatusCode::NOT_FOUND || status == reqwest::StatusCode::FORBIDDEN {
        return Ok(None);
    }
    if !status.is_success() {
        return Err(ConnectorError::upstream(status, format!("meet conferenceRecords.get failed: {status}")));
    }
    resp.json()
        .await
        .map(Some)
        .map_err(|e| ConnectorError::Exchange(format!("meet conferenceRecords.get parse: {e}")))
}

/// `spaces.get` (accepts `spaces/{id}` or `spaces/{meetingCode}`). `Ok(None)` for
/// 403/404 - a space this token cannot read is simply "unknown", never an error.
pub(crate) async fn get_space(
    http: &reqwest::Client,
    access_token: &str,
    space_name: &str,
) -> Result<Option<Space>, ConnectorError> {
    let base = api_base();
    let url = format!("{base}/{space_name}");
    let resp = google_send(|| http.get(super::google_transport::endpoint(&url)).bearer_auth(access_token)).await?;
    let status = resp.status();
    if status == reqwest::StatusCode::NOT_FOUND || status == reqwest::StatusCode::FORBIDDEN {
        return Ok(None);
    }
    if !status.is_success() {
        return Err(ConnectorError::upstream(status, format!("meet spaces.get failed: {status}")));
    }
    resp.json()
        .await
        .map(Some)
        .map_err(|e| ConnectorError::Exchange(format!("meet spaces.get parse: {e}")))
}

/// What `transcripts.list` says about a record: used by the meetings listing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TranscriptState {
    None,
    Processing,
    Ready,
}

pub(crate) async fn transcript_state(
    http: &reqwest::Client,
    access_token: &str,
    conference_record_name: &str,
) -> Result<TranscriptState, ConnectorError> {
    let transcripts = list_transcripts(http, access_token, conference_record_name).await?;
    Ok(if transcripts.iter().any(|t| t.state == TRANSCRIPT_STATE_FILE_GENERATED) {
        TranscriptState::Ready
    } else if transcripts.is_empty() {
        TranscriptState::None
    } else {
        TranscriptState::Processing
    })
}

async fn list_transcripts(
    http: &reqwest::Client,
    access_token: &str,
    conference_record_name: &str,
) -> Result<Vec<Transcript>, ConnectorError> {
    let base = api_base();
    let url = format!("{base}/{conference_record_name}/transcripts");
    let resp = google_send(|| http.get(super::google_transport::endpoint(&url)).bearer_auth(access_token)).await?;
    if !resp.status().is_success() {
        let status = resp.status();
        return Err(ConnectorError::upstream(status, format!("meet transcripts.list failed: {status}")));
    }
    let parsed: TranscriptsListResponse = resp
        .json()
        .await
        .map_err(|e| ConnectorError::Exchange(format!("meet transcripts.list parse: {e}")))?;
    Ok(parsed.transcripts)
}

async fn list_transcript_entries_all(
    http: &reqwest::Client,
    access_token: &str,
    transcript_name: &str,
) -> Result<Vec<TranscriptEntry>, ConnectorError> {
    let mut out = Vec::new();
    let mut page_token: Option<String> = None;
    for _ in 0..MAX_PAGES {
        let base = api_base();
        let mut url = format!("{base}/{transcript_name}/entries?pageSize={ENTRIES_PAGE_SIZE}");
        if let Some(pt) = &page_token {
            url.push_str("&pageToken=");
            url.push_str(&urlencoding::encode(pt));
        }
        let resp = google_send(|| http.get(super::google_transport::endpoint(&url)).bearer_auth(access_token)).await?;
        if !resp.status().is_success() {
            let status = resp.status();
            return Err(ConnectorError::upstream(status, format!("meet transcripts.entries.list failed: {status}")));
        }
        let page: TranscriptEntriesListResponse = resp
            .json()
            .await
            .map_err(|e| ConnectorError::Exchange(format!("meet transcripts.entries.list parse: {e}")))?;
        out.extend(page.transcript_entries);
        match page.next_page_token {
            Some(t) if !t.is_empty() => page_token = Some(t),
            _ => break,
        }
    }
    Ok(out)
}

async fn list_participants_all(
    http: &reqwest::Client,
    access_token: &str,
    conference_record_name: &str,
) -> Result<Vec<Participant>, ConnectorError> {
    let mut out = Vec::new();
    let mut page_token: Option<String> = None;
    for _ in 0..MAX_PAGES {
        let base = api_base();
        let mut url = format!("{base}/{conference_record_name}/participants?pageSize={LIST_PAGE_SIZE}");
        if let Some(pt) = &page_token {
            url.push_str("&pageToken=");
            url.push_str(&urlencoding::encode(pt));
        }
        let resp = google_send(|| http.get(super::google_transport::endpoint(&url)).bearer_auth(access_token)).await?;
        if !resp.status().is_success() {
            let status = resp.status();
            return Err(ConnectorError::upstream(status, format!("meet participants.list failed: {status}")));
        }
        let page: ParticipantsListResponse = resp
            .json()
            .await
            .map_err(|e| ConnectorError::Exchange(format!("meet participants.list parse: {e}")))?;
        out.extend(page.participants);
        match page.next_page_token {
            Some(t) if !t.is_empty() => page_token = Some(t),
            _ => break,
        }
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Pure content shaping (unit-tested, no HTTP/DB)
// ---------------------------------------------------------------------------

/// `participant resource name -> display label`, `"Participant"` for a
/// signed-in user with no display name on record. Never keyed on
/// `anonymousUser`/`phoneUser` display names here — [`build_transcript_content`]
/// falls back to a generic label for any entry whose participant does not
/// appear in this map at all (an entry can reference a participant this
/// caller never fetched a signed-in identity for).
fn participant_labels(participants: &[Participant]) -> HashMap<String, String> {
    participants
        .iter()
        .filter_map(|p| {
            let su = p.signedin_user.as_ref()?;
            let label = if su.display_name.trim().is_empty() {
                "Participant".to_string()
            } else {
                su.display_name.clone()
            };
            Some((p.name.clone(), label))
        })
        .collect()
}

/// Join transcript entries into ONE document, `"{speaker}: {text}"` per line
/// — the exact shape `ChunkStrategy::Transcript` (`packages/core/src/
/// chunker.rs`) expects for speaker-turn-aware chunking. Entries are used in
/// the order the API returned them (`transcripts.entries.list`'s own doc:
/// "By default, ordered by start time and in ascending order"), never
/// re-sorted here. Blank/whitespace-only entry text is dropped rather than
/// emitted as an empty speaker turn.
pub(crate) fn build_transcript_content(entries: &[TranscriptEntry], labels: &HashMap<String, String>) -> String {
    entries
        .iter()
        .filter_map(|e| {
            let text = e.text.trim();
            if text.is_empty() {
                return None;
            }
            let label = labels.get(&e.participant).cloned().unwrap_or_else(|| "Participant".to_string());
            Some(format!("{label}: {text}"))
        })
        .collect::<Vec<_>>()
        .join("\n")
}

// ---------------------------------------------------------------------------
// Identity resolution — see this module's own doc, "Why the cross-account
// resolver, not the account-scoped one".
// ---------------------------------------------------------------------------

/// A resolved attendee: a CoralSwarm account + member this meeting's
/// transcript will land into (their own private ocean, resolved fresh at
/// ingest time — never persisted).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ResolvedAttendee {
    pub(crate) account_id: Uuid,
    pub(crate) owner_user_id: Uuid,
}

/// Resolve one participant's bare Google `sub` to a connected CoralSwarm
/// member — `Ok(None)` for every "no resolvable identity" case the founder's
/// fan-out decision requires to be silent, permanent, and non-blocking for
/// this meeting: no CoralSwarm account has ever connected that `sub`, the
/// `sub` is connected under more than one account (fails closed —
/// `resolve_google_identity_by_sub`'s own ambiguity rule), or the resolved
/// account is on the ORG plane (this PR's own Personal-plane-only fan-out
/// boundary — see this module's own doc). Never held, retried, or queued.
pub(crate) async fn resolve_attendee(control_pool: &PgPool, sub: &str) -> Result<Option<ResolvedAttendee>, ConnectorError> {
    let Some((account_id, owner_user_id)) = super::google_chat::resolve_google_identity_by_sub(control_pool, sub).await? else {
        return Ok(None);
    };
    match account_plane(control_pool, account_id).await {
        Ok(AccountPlane::Personal) => Ok(Some(ResolvedAttendee { account_id, owner_user_id })),
        Ok(AccountPlane::Org) => Ok(None),
        Err(e) => Err(ConnectorError::Internal(format!("resolve_attendee: account_plane lookup failed: {e}"))),
    }
}

/// Which plane one sweep pass runs on — see this module's own doc, "Two
/// planes, never mixed". Chosen ONCE by the caller (`workers::
/// google_meet_sweep`'s `tick_consumer` vs `tick_org`) and threaded through
/// every step of the pass, so no step can silently pick the other plane.
#[derive(Clone)]
pub(crate) enum MeetPlane {
    /// Personal account on the shared consumer/control database. Byte-for-byte
    /// the pre-lift behavior.
    Personal,
    /// An ordinary org with its own tenant database.
    Org(OrgMeetPlane),
}

/// The org half of [`MeetPlane::Org`]: the one org account this pass reads
/// for, and the org's own TENANT pool (sync-state, ledger, member oceans).
#[derive(Clone)]
pub(crate) struct OrgMeetPlane {
    pub(crate) org_id: String,
    pub(crate) account_id: Uuid,
    pub(crate) pool: PgPool,
}

/// Org-plane attendee resolution: a Google `sub` -> a member of THIS org
/// account, via the ACCOUNT-SCOPED resolver
/// ([`super::google_chat::resolve_google_identity_in_account`]) — never the
/// cross-account one. A `sub` connected only under some other account
/// (including a personal account) resolves to nothing here: an org pass can
/// therefore never route a copy to anyone outside the org. `Ok(None)` is the
/// permanent "not an org member / no connected identity" classification;
/// `Err` is a genuine lookup failure (transient — the whole record retries).
pub(crate) async fn resolve_attendee_in_org(
    control_pool: &PgPool,
    org_account_id: Uuid,
    sub: &str,
) -> Result<Option<ResolvedAttendee>, ConnectorError> {
    let owner = super::google_chat::resolve_google_identity_in_account(control_pool, org_account_id, sub).await?;
    if let Some(owner_user_id) = owner {
        if !super::google_chat::is_current_account_member(control_pool, org_account_id, owner_user_id).await? { return Ok(None); }
    }
    Ok(owner.map(|owner_user_id| ResolvedAttendee {
        account_id: org_account_id,
        owner_user_id,
    }))
}

/// Resolve every signed-in participant, deduplicating a person who
/// reconnected mid-meeting (multiple `Participant` rows resolving to the
/// same `(account_id, owner_user_id)`) into ONE fan-out target.
///
/// **A permanent `Ok(None)` (no linked identity, ambiguous `sub`, or an
/// ORG-plane account) is NOT the same claim as a transient resolution
/// error, and this function does not conflate them** (CodeRabbit review,
/// fixed before merge — an earlier version of this function logged and
/// silently dropped a HARD error — e.g. a control-DB hiccup mid-lookup —
/// identically to "this person has no linked identity", which for an
/// immutable, no-repair surface meant a transient failure could
/// PERMANENTLY cost an attendee their transcript with no error ever
/// surfaced). Returns `(resolved, hard_failures)`: `resolved` is exactly the
/// founder-decision set (every attendee this tick found a linked, Personal-
/// plane identity for); `hard_failures` is non-empty ONLY when a resolution
/// attempt itself errored, and [`process_conference_record`]'s own contract
/// is to treat a non-empty `hard_failures` as "do not ledger this meeting as
/// done — retry the whole record next tick" (see that function's own doc),
/// never as "these attendees are unresolved". Continues past a hard failure
/// rather than aborting immediately, so every OTHER participant still gets a
/// real resolution attempt this tick even though the caller will retry the
/// whole record regardless.
async fn resolve_attendees(
    plane: &MeetPlane,
    control_pool: &PgPool,
    participants: &[Participant],
) -> (Vec<ResolvedAttendee>, Vec<ConnectorError>) {
    let mut seen: HashSet<(Uuid, Uuid)> = HashSet::new();
    let mut out = Vec::new();
    let mut hard_failures = Vec::new();
    for p in participants {
        let Some(su) = &p.signedin_user else { continue };
        let sub = super::google_chat::bare_sub(&su.user);
        if sub.is_empty() {
            continue;
        }
        let resolution = match plane {
            MeetPlane::Personal => resolve_attendee(control_pool, sub).await,
            MeetPlane::Org(org) => resolve_attendee_in_org(control_pool, org.account_id, sub).await,
        };
        match resolution {
            Ok(Some(attendee)) => {
                if seen.insert((attendee.account_id, attendee.owner_user_id)) {
                    out.push(attendee);
                }
            }
            Ok(None) => {}
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "google meet: attendee identity resolution failed (transient, NOT permanent) — \
                     this meeting will not be ledgered as done this tick, so it retries"
                );
                hard_failures.push(e);
            }
        }
    }
    (out, hard_failures)
}

// ---------------------------------------------------------------------------
// Fan-out ingest — one attendee, one call to the generic ingest route.
// ---------------------------------------------------------------------------

/// Land `content` into `attendee`'s OWN primary private ocean —
/// `resolve_primary_graph`'s `Personal` arm (this PR's own plane boundary),
/// `IngestCaller::Attributable(attendee.owner_user_id)`. Reuses the generic
/// `routes::ingest::ingest_atom` route directly (the same "construct a
/// synthetic `Tenant`/`UserContext` and call the route from a worker" shape
/// `workers::gmail_everything_mode::sync_owner` already uses), so DEK
/// resolution, R2 blob storage, chunking (`ChunkStrategy::Transcript` via
/// [`SOURCE_TYPE`]), and the `EmbedAtoms` enqueue are handled identically to
/// every other ingest path — nothing Meet-specific is reimplemented here.
///
/// `source_id = "meet:{conference_record_id}"` — ONE atom family per
/// (attendee's own graph, meeting), never per transcript entry: a re-run
/// (the sweep's own re-processing dedup failing open, or a future manual
/// backfill) reuses the committed capture in `ingest::capture` under the
/// source identity's transaction lock. The trusted native Meet origin below
/// selects that first-write-wins contract before versioning or chunk
/// reconciliation can replace, append or retire any transcript content.
///
/// `Ok(true)` — ingested (or already present, idempotently). `Ok(false)` —
/// a deterministic validation rejection (`422`, e.g. empty content after
/// trimming) — consumed, never retried, mirroring `routes::google_push::
/// run_gmail_sync_for_email`'s identical 422-as-consumed posture. `Err` —
/// anything else; the caller logs and moves on to the next attendee (this
/// module's own doc: one attendee's failure never blocks another's).
#[allow(clippy::too_many_arguments)] // `plane` joined an already-long positional list; a params struct would churn every pg-test call site
pub(crate) async fn ingest_transcript_for_attendee(
    plane: &MeetPlane,
    state: &Arc<AppState>,
    control_pool: &PgPool,
    attendee: ResolvedAttendee,
    conference_record_id: &str,
    content: &str,
    participants: &[String],
    event_timestamp: DateTime<Utc>,
) -> Result<bool, ConnectorError> {
    // The pause gate at the write itself, for any caller that did not check
    // first. An error, not `Ok(false)`: `false` closes the capture for good, and
    // a paused source only waits.
    if crate::connectors::catalog::pause::is_paused(
        control_pool, attendee.account_id, attendee.owner_user_id, crate::connectors::catalog::pause::GOOGLE_MEET,
    ).await {
        return Err(ConnectorError::Internal("google meet source is paused or off".into()));
    }
    // `tenant` is the request-shaped content-plane handle `ingest_atom`
    // expects: `Tenant::legacy` for Personal (byte-identical to before), the
    // org's own tenant pool + `TenancyDecision::Org` for Org (the same
    // construction `workers::gmail_everything_mode::sync_owner` uses).
    let (graph_id, tenant) = match plane {
        MeetPlane::Personal => {
            let graph_id = resolve_primary_graph(
                control_pool,
                attendee.account_id,
                AccountPlane::Personal,
                IngestCaller::Attributable(attendee.owner_user_id),
            )
            .await
            .map_err(|e| ConnectorError::Internal(format!("resolve_primary_graph: {e}")))?;
            (graph_id, Tenant::legacy(state))
        }
        MeetPlane::Org(org) => {
            // Structural guard, independent of the resolver: an org pass
            // writes ONLY for attendees of its own org account.
            if attendee.account_id != org.account_id {
                return Err(ConnectorError::Internal(format!(
                    "google meet org pass for account {} refused an attendee of account {} — \
                     org and personal/other-org planes never mix",
                    org.account_id, attendee.account_id
                )));
            }
            // An org member with no live member-private ocean (provisioning
            // not caught up, archived on offboarding) has no destination.
            // That is "no resolvable destination" — the same silent,
            // permanent, non-blocking class as an unresolved attendee — NOT
            // a hard failure: `resolve_primary_graph` would return an `Err`
            // here, which the fan-out would count as transient and retry the
            // whole record forever, wedging this reader's watermark behind
            // one member's missing ocean.
            if !org_member_ocean_exists(&org.pool, org.account_id, attendee.owner_user_id).await? {
                tracing::warn!(
                    %conference_record_id,
                    account_id = %org.account_id,
                    owner_user_id = %attendee.owner_user_id,
                    connector_health = true,
                    "google meet: org attendee has no live member-private ocean — no copy for \
                     them (not held, not retried)"
                );
                return Ok(false);
            }
            let graph_id = resolve_primary_graph(
                &org.pool,
                org.account_id,
                AccountPlane::Org,
                IngestCaller::Attributable(attendee.owner_user_id),
            )
            .await
            .map_err(|e| ConnectorError::Internal(format!("resolve_primary_graph (org): {e}")))?;
            let store: Arc<dyn Store> = Arc::new(PostgresStore::new(org.pool.clone()));
            let tenant = Tenant {
                store: Some(store),
                pool: Some(org.pool.clone()),
                decision: Some(TenancyDecision::Org(TenantContext {
                    org_id: org.org_id.clone(),
                    account_id: org.account_id,
                    user_id: attendee.owner_user_id,
                    pool: org.pool.clone(),
                    page_pool: None,
                })),
                class: crate::route_class::RouteClass::Other,
            };
            (graph_id, tenant)
        }
    };

    let req = IngestAtomRequest {
        graph_id,
        source_type: SOURCE_TYPE.to_string(),
        source_id: format!("meet:{conference_record_id}"),
        content: content.to_string(),
        participants: participants.to_vec(),
        timestamp: event_timestamp,
        // Private — each attendee's own ocean, never shared (Mailbox
        // archetype, own-ocean-only; see this module's own doc).
        scope: None,
        fts_language: None,
        channel_id: None,
        channel_name: None,
        author_id: None,
        author_name: None,
        project: None,
        mcp_client_name: None,
        raw_content: None,
        duration_ms: None,
        segments: None,
        session_id: None,
        session_kind: None,
        session_title: None,
        repo_path: None,
        remote: None,
        hostname: None,
        harness_version: None,
        model: None,
        branch: None,
        agent_name: None,
        platform: None,
        task: None,
        parent_session_id: None,
        trusted_user_agent: None,
        client_info: None,
        code_path: None,
    };

    let user_ctx = UserContext {
        user_id: attendee.owner_user_id,
        account_id: attendee.account_id,
        role: "member".into(),
        device_id: String::new(),
    };
    let origin = crate::source_origin::SourceOrigin::native("google_meet","transcript","google_meet_attendee_capture")
        .and_then(|o|o.with_container("conference_record",conference_record_id,"google_meet:conference_records",None))
        .map_err(ConnectorError::Internal)?;
    let refused = crate::ingest::submit(crate::ingest::IngestRequest {
        origin: Some(&origin),
        ..crate::ingest::IngestRequest::private(crate::ingest::Member::new(state, &tenant, &user_ctx, req))
    }).await.err();

    match refused {
        None => Ok(true),
        Some(refused) if refused.is_invalid() => {
            tracing::warn!(
                %conference_record_id,
                account_id = %attendee.account_id,
                owner_user_id = %attendee.owner_user_id,
                error = %refused,
                "google meet: the ingest door rejected this transcript as invalid — \
                 skipping permanently for this attendee, not retried"
            );
            Ok(false)
        }
        Some(refused) => Err(ConnectorError::Exchange(format!("ingest_atom failed: {refused}"))),
    }
}

/// Does `user_id` have a live, primary, member-private ocean in this org's
/// tenant database? Mirrors `resolve_org_member_primary_graph`'s own
/// predicate (`kind = 'member'`, `owner_user_id`, `archived_at IS NULL`,
/// `is_primary`) so the destination decision stays with that resolver — this
/// is only the "is there one at all" pre-check that lets the caller tell a
/// missing ocean (permanent skip) apart from a real lookup failure
/// (transient).
async fn org_member_ocean_exists(pool: &PgPool, account_id: Uuid, user_id: Uuid) -> Result<bool, ConnectorError> {
    let mut tx = crate::tenancy::scope::begin_tenant_scoped(pool, account_id, user_id)
        .await
        .map_err(|e| ConnectorError::Internal(format!("org_member_ocean_exists scoped tx: {e}")))?;
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM graphs \
         WHERE account_id = $1 AND owner_user_id = $2 AND kind = 'member' \
           AND archived_at IS NULL AND is_primary = true)",
    )
    .bind(account_id)
    .bind(user_id)
    .fetch_one(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("org_member_ocean_exists query: {e}")))?;
    tx.rollback().await.ok();
    Ok(exists)
}

// ---------------------------------------------------------------------------
// Ledger — `google_meet_ingested_conferences` (dedup + observability only;
// see that migration's own header for why fan-out routing itself lives in
// `atoms`, not here).
// ---------------------------------------------------------------------------

/// The pool + scoped transaction a plane's ledger/cursor rows live behind:
/// the control pool under `begin_account_scoped` for Personal (unchanged),
/// the org's tenant pool under `begin_tenant_system_scoped` for Org.
async fn begin_ledger_tx(
    plane: &MeetPlane,
    control_pool: &PgPool,
    account_id: Uuid,
) -> Result<sqlx::Transaction<'static, sqlx::Postgres>, crate::store::StoreError> {
    match plane {
        MeetPlane::Personal => crate::tenancy::scope::begin_account_scoped(control_pool, account_id).await,
        MeetPlane::Org(org) => crate::tenancy::scope::begin_tenant_system_scoped(&org.pool, account_id).await,
    }
}

async fn already_ingested(
    plane: &MeetPlane,
    control_pool: &PgPool,
    account_id: Uuid,
    conference_record_id: &str,
) -> Result<bool, ConnectorError> {
    let mut tx = begin_ledger_tx(plane, control_pool, account_id)
        .await
        .map_err(|e| ConnectorError::Internal(format!("already_ingested scoped tx: {e}")))?;
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM google_meet_ingested_conferences WHERE account_id = $1 AND conference_record_id = $2)",
    )
    .bind(account_id)
    .bind(conference_record_id)
    .fetch_one(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("already_ingested query: {e}")))?;
    tx.rollback().await.ok();
    Ok(exists)
}

#[allow(clippy::too_many_arguments)] // `plane` joined an already-long positional list; a params struct would churn every pg-test call site
async fn record_ingested_conference(
    plane: &MeetPlane,
    control_pool: &PgPool,
    account_id: Uuid,
    owner_user_id: Uuid,
    conference_record_id: &str,
    transcript_id: &str,
    attendee_count: usize,
    resolved_count: usize,
) -> Result<(), ConnectorError> {
    let mut tx = begin_ledger_tx(plane, control_pool, account_id)
        .await
        .map_err(|e| ConnectorError::Internal(format!("record_ingested_conference scoped tx: {e}")))?;
    sqlx::query(
        "INSERT INTO google_meet_ingested_conferences \
            (account_id, owner_user_id, conference_record_id, transcript_id, attendee_count, resolved_count) \
         VALUES ($1, $2, $3, $4, $5, $6) \
         ON CONFLICT (account_id, conference_record_id) DO NOTHING",
    )
    .bind(account_id)
    .bind(owner_user_id)
    .bind(conference_record_id)
    .bind(transcript_id)
    .bind(attendee_count as i32)
    .bind(resolved_count as i32)
    .execute(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("record_ingested_conference insert: {e}")))?;
    tx.commit()
        .await
        .map_err(|e| ConnectorError::Internal(format!("record_ingested_conference commit: {e}")))?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Sync-state bootstrap — `GoogleConnector::post_store`'s Meet arm.
// ---------------------------------------------------------------------------

/// Idempotent bootstrap of this member's `google_meet_sync_state` row on
/// connect, mirroring `google_gmail::ensure_watch`/`google_chat::
/// ensure_subscription`'s own "safe to call on every connect, including a
/// reconnect that already has a row" posture. `last_synced_end_time` starts
/// `NULL` on a genuinely fresh row, so the sweep's own first-run fallback
/// ([`default_lookback`]) applies rather than this function guessing a
/// window. Never fails the OAuth redirect — the caller logs and moves on
/// (`ConnectorProvider::post_store`'s own contract).
///
/// **Two races fixed here (CodeRabbit review, before merge) — both because
/// `callback`'s `oauth_tokens` commit and this call run in SEPARATE
/// transactions** (`post_store` is a best-effort side effect run AFTER the
/// generic upsert commits, same shape every other Google surface's
/// `post_store` arm uses — see `ConnectorProvider::post_store`'s own doc):
///
/// 1. **Disconnect-races-connect.** A disconnect that runs entirely between
///    `callback`'s `oauth_tokens` commit and this call finds nothing to
///    purge (no row exists yet), then this call inserts one AFTER — an
///    orphaned row for a member who is no longer connected. The `WHERE
///    EXISTS (...)` guard below narrows this: the insert only proceeds if
///    an `oauth_tokens` row for this exact `(account_id, owner_user_id,
///    'google_meet')` still exists AT THIS STATEMENT'S OWN SNAPSHOT — this
///    does not fully eliminate the race (two independent, unlocked
///    transactions never can, short of merging them into one), but it
///    closes the common case (disconnect completes well before this call
///    even runs) outright. The residual narrow window is additionally
///    self-healed by `workers::google_meet_sweep::sweep_owner`: a claimed
///    row whose owner has no `google_meet` credentials is treated as
///    "disconnected" and the row is deleted on the spot, not retried
///    forever — see that function's own doc.
/// 2. **`ON CONFLICT DO NOTHING` never refreshed `updated_at`** — the exact
///    "purge_not_after — the reconnect race" class this module's sibling
///    tables (`google_watch_channels` via `google_gmail::ensure_watch`/
///    `google_calendar::ensure_channel`) already close by unconditionally
///    stamping `updated_at = now()` on every insert AND every `ON CONFLICT
///    DO UPDATE`. A stale row (last touched before some disconnect
///    request's `purge_not_after` cutoff was captured) that a RECONNECT
///    refreshes after that cutoff but before the purge's own DELETE runs
///    used to keep its OLD timestamp — so `delete_meet_sync_state`'s
///    `updated_at <= purge_not_after` guard still matched it, deleting a
///    row a reconnect had just re-established interest in. Fixed by
///    switching to `ON CONFLICT DO UPDATE SET updated_at = now()`, matching
///    the sibling tables' own discipline exactly.
pub(crate) async fn ensure_sync_state(control_pool: &PgPool, account_id: Uuid, owner_user_id: Uuid) -> Result<(), ConnectorError> {
    // Org accounts never get a CONTROL-plane cursor row: the control table is
    // swept by the consumer-plane tick, which runs the PERSONAL fan-out — an
    // org member's row there would be processed on the wrong plane. An org
    // member's cursor lives on the org's tenant database and is seeded by
    // `workers::google_meet_sweep::tick_org` from the control-plane
    // `oauth_tokens` rows, so there is nothing to do here for them.
    match account_plane(control_pool, account_id).await {
        Ok(AccountPlane::Personal) => {}
        Ok(AccountPlane::Org) => return Ok(()),
        Err(e) => return Err(ConnectorError::Internal(format!("ensure_sync_state: account_plane lookup failed: {e}"))),
    }
    let mut tx = crate::tenancy::scope::begin_account_scoped(control_pool, account_id)
        .await
        .map_err(|e| ConnectorError::Internal(format!("ensure_sync_state scoped tx: {e}")))?;
    sqlx::query(
        "INSERT INTO google_meet_sync_state (account_id, owner_user_id) \
         SELECT $1, $2 \
         WHERE EXISTS ( \
             SELECT 1 FROM oauth_tokens \
             WHERE account_id = $1 AND provider = 'google_meet' AND owner_user_id = $2 \
         ) \
         ON CONFLICT (account_id, owner_user_id) DO UPDATE SET updated_at = now()",
    )
    .bind(account_id)
    .bind(owner_user_id)
    .execute(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("ensure_sync_state insert: {e}")))?;
    tx.commit()
        .await
        .map_err(|e| ConnectorError::Internal(format!("ensure_sync_state commit: {e}")))?;
    Ok(())
}

// ---------------------------------------------------------------------------
// One conference record, opt-in only.
// ---------------------------------------------------------------------------

use crate::services::google_meet_capture::{self as capture, Capture, CaptureMatch};

/// The pool a plane's `google_meet_captures` / ledger / cursor rows live on.
pub(crate) fn plane_pool<'a>(plane: &'a MeetPlane, control_pool: &'a PgPool) -> &'a PgPool {
    match plane {
        MeetPlane::Personal => control_pool,
        MeetPlane::Org(org) => &org.pool,
    }
}

/// Who a [`process_record_opt_in`] pass may write a copy for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RecordScope {
    /// The sweep: the reader (whose token this is) plus every attendee that
    /// resolves to a CoralSwarm member on this plane. Each is checked against
    /// THEIR OWN capture rows; nobody without an `allowed` row gets a copy.
    Sweep,
    /// An explicit allow by the reader (route or recheck): the reader's own
    /// copy only. Other attendees' approvals are picked up by the sweep.
    OwnOnly,
}

/// What a pass did with one conference record.
#[derive(Debug)]
pub(crate) enum RecordOutcome {
    /// The meeting has not ended; nothing can be ingested yet. Retried later.
    NotEnded,
    /// No attendee has an open capture covering this record: nothing ingested
    /// (the default for every meeting nobody approved).
    NothingApproved,
    /// Approved, but Meet has no transcript for this record. Terminal.
    NoTranscript,
    /// Approved, a transcript exists but is not generated yet. Retried.
    TranscriptPending,
    /// Approved captures were processed. `copies` = copies that landed now.
    Done { copies: usize, transcript_id: String, attendees: usize },
}

/// Surface hard failures: a record with ANY transient failure must never be
/// reported as done (the caller would ledger it and move its watermark on),
/// exactly the rule `fan_out_and_count` documents.
fn fail_or(hard_failures: Vec<String>, record: &str, outcome: RecordOutcome) -> Result<RecordOutcome, ConnectorError> {
    if hard_failures.is_empty() {
        return Ok(outcome);
    }
    Err(ConnectorError::Exchange(format!(
        "google meet: {} hard failure(s) for {record} - not treating it as done; the whole record \
         retries next tick: {}",
        hard_failures.len(),
        hard_failures.join("; ")
    )))
}

/// Process one conference record under the opt-in rule: **ingest a copy for a
/// member only if that member has an open `google_meet_captures` row covering
/// this record**, then mark that row ingested. No row, no copy.
///
/// # Consent and fan-out
///
/// A capture row is the approving member's consent for THEIR OWN copy only.
/// The reader's token is the only credential Google will honour for this
/// record (Meet scopes conference records to spaces the caller organized), so
/// in [`RecordScope::Sweep`] the reader's pass also serves attendees: each
/// resolved attendee's OWN rows are consulted (in a transaction scoped to that
/// attendee) and a copy lands in that attendee's own private ocean only if
/// they approved. An attendee who did not approve gets nothing, whoever else
/// approved. Fan-out is therefore opt-in per attendee, never "one person
/// approved, everyone gets it".
///
/// # Classification (unchanged from the auto-ingest sweep)
///
/// "No resolvable identity" / "no destination ocean" are PERMANENT and silent
/// (the member simply gets no copy); any transient failure (identity lookup,
/// capture read, ingest, marking) makes the pass return `Err` so the caller
/// retries the whole record - safe because ingest is idempotent
/// (`atoms (graph_id, source_type, source_id) ON CONFLICT DO NOTHING`) and rows
/// already marked `ingested` are skipped on the retry.
#[allow(clippy::too_many_arguments)] // mirrors the pre-opt-in call shape; a params struct would churn every call site
pub(crate) async fn process_record_opt_in(
    plane: &MeetPlane,
    http: &reqwest::Client,
    control_pool: &PgPool,
    state: &Arc<AppState>,
    reader: ResolvedAttendee,
    access_token: &str,
    record: &ConferenceRecord,
    scope: RecordScope,
) -> Result<RecordOutcome, ConnectorError> {
    // Serialize push and pull approval binding and attendee writes for this record.
    let mut record_lock = begin_ledger_tx(plane, control_pool, reader.account_id).await.map_err(|e| ConnectorError::Internal(e.to_string()))?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(format!("google_meet_record:{}", record.name))
        .execute(&mut *record_lock).await.map_err(|e| ConnectorError::Internal(e.to_string()))?;
    let Some(end_time) = record.end_time.as_deref().and_then(parse_google_timestamp) else {
        return Ok(RecordOutcome::NotEnded);
    };
    let start_time = record.start_time.as_deref().and_then(parse_google_timestamp);
    let pool = plane_pool(plane, control_pool);

    let mut hard_failures: Vec<String> = Vec::new();
    let mut participants: Option<Vec<Participant>> = None;
    let mut candidates = vec![reader];
    if scope == RecordScope::Sweep {
        let listed = list_participants_all(http, access_token, &record.name).await?;
        let (resolved, failures) = resolve_attendees(plane, control_pool, &listed).await;
        hard_failures.extend(failures.iter().map(|e| e.to_string()));
        for attendee in resolved {
            if !candidates.contains(&attendee) {
                candidates.push(attendee);
            }
        }
        participants = Some(listed);
    }

    // Each candidate's own rows decide whether THEY get a copy.
    let mut meeting_code: Option<Option<String>> = None;
    let mut approved: Vec<(ResolvedAttendee, Capture)> = Vec::new();
    for candidate in candidates {
        let rows = match capture::load(pool, candidate.account_id, candidate.owner_user_id).await {
            Ok(rows) => rows,
            Err(e) => {
                hard_failures.push(format!("capture lookup failed (account {}): {e}", candidate.account_id));
                continue;
            }
        };
        if rows.is_empty() {
            continue;
        }
        if meeting_code.is_none() && capture::needs_meeting_code(&rows) {
            if let Some(space) = record.space.as_deref() {
                let code = get_space(http, access_token, space)
                    .await?
                    .and_then(|s| s.meeting_code)
                    .and_then(|c| capture::normalize_meeting_code(&c));
                meeting_code = Some(code);
            }
        }
        let code = meeting_code.as_ref().and_then(|c| c.as_deref());
        if let CaptureMatch::Allowed(row) =
            capture::resolve(&rows, &record.name, record.space.as_deref(), code, end_time, Utc::now())
        {
            if row.terminal_reason.is_none() {
                approved.push((candidate, row));
            }
        }
    }

    // Bind pre-approvals to this record first: from here on the row is exact,
    // so a transient failure below can never lose the approval.
    let mut targets: Vec<ResolvedAttendee> = Vec::new();
    for (candidate, row) in approved {
        if row.conference_record_id.is_none() {
            match capture::bind_record(
                pool,
                candidate.account_id,
                candidate.owner_user_id,
                row.id,
                &record.name,
                record.space.as_deref(),
            )
            .await
            {
                Ok(true) => {}
                Ok(false) => {
                    tracing::warn!(conference_record_id = %record.name, "google meet: pre-approval no longer open, skipping");
                    continue;
                }
                Err(e) => {
                    hard_failures.push(format!("binding a pre-approval failed (account {}): {e}", candidate.account_id));
                    continue;
                }
            }
        }
        targets.push(candidate);
    }
    if targets.is_empty() {
        return fail_or(hard_failures, &record.name, RecordOutcome::NothingApproved);
    }

    let transcripts = list_transcripts(http, access_token, &record.name).await?;
    let Some(transcript) = transcripts.iter().find(|t| t.state == TRANSCRIPT_STATE_FILE_GENERATED) else {
        if transcripts.is_empty() {
            for t in &targets {
                if let Err(e) = capture::mark_terminal(pool, t.account_id, t.owner_user_id, &record.name, "no_transcript").await {
                    hard_failures.push(format!("marking no_transcript failed: {e}"));
                }
            }
            return fail_or(hard_failures, &record.name, RecordOutcome::NoTranscript);
        }
        return Ok(RecordOutcome::TranscriptPending);
    };

    let entries = list_transcript_entries_all(http, access_token, &transcript.name).await?;
    let participants = match participants {
        Some(p) => p,
        None => list_participants_all(http, access_token, &record.name).await?,
    };
    let attendees = participants.len();
    let labels = participant_labels(&participants);
    let content = build_transcript_content(&entries, &labels);

    if content.trim().is_empty() {
        for t in &targets {
            if let Err(e) = capture::mark_terminal(pool, t.account_id, t.owner_user_id, &record.name, "empty_transcript").await {
                hard_failures.push(format!("marking empty_transcript failed: {e}"));
            }
        }
        return fail_or(
            hard_failures,
            &record.name,
            RecordOutcome::Done { copies: 0, transcript_id: transcript.name.clone(), attendees },
        );
    }

    let display_names: Vec<String> = participants
        .iter()
        .filter_map(|p| p.signedin_user.as_ref())
        .map(|s| s.display_name.clone())
        .filter(|s| !s.is_empty())
        .collect();
    let event_timestamp = start_time.unwrap_or_else(Utc::now);

    let mut copies = 0usize;
    for target in targets {
        // A member who paused or turned off Meet gets no copy: skipped, not
        // closed. Their approved capture stays open and lands after a resume
        // (the sweep's re-check reads it by name until it expires).
        if crate::connectors::catalog::pause::is_paused(
            control_pool, target.account_id, target.owner_user_id, crate::connectors::catalog::pause::GOOGLE_MEET,
        ).await {
            continue;
        }
        match ingest_transcript_for_attendee(
            plane,
            state,
            control_pool,
            target,
            &record.name,
            &content,
            &display_names,
            event_timestamp,
        )
        .await
        {
            Ok(true) => {
                copies += 1;
                match capture::mark_ingested(pool, target.account_id, target.owner_user_id, &record.name).await {
                    Ok(true) => {}
                    Ok(false) => tracing::warn!(
                        conference_record_id = %record.name,
                        account_id = %target.account_id,
                        "google meet: capture was revoked while its copy was being written (narrow race); \
                         the copy has landed, the row stays revoked"
                    ),
                    Err(e) => hard_failures.push(format!("marking ingested failed (account {}): {e}", target.account_id)),
                }
            }
            // Deterministic non-ingest (no destination ocean / 422): permanent
            // by this connector's rules (not held, not retried), so close the
            // capture instead of letting it occupy the recheck queue until expiry.
            Ok(false) => {
                tracing::warn!(
                    conference_record_id = %record.name,
                    account_id = %target.account_id,
                    owner_user_id = %target.owner_user_id,
                    "google meet: approved capture produced no copy (no destination / rejected) - closing it"
                );
                if let Err(e) = capture::mark_terminal(pool, target.account_id, target.owner_user_id, &record.name, "not_ingestable").await {
                    hard_failures.push(format!("marking not_ingestable failed: {e}"));
                }
            }
            Err(e) => hard_failures.push(format!(
                "{e} (account {}, owner {})",
                target.account_id, target.owner_user_id
            )),
        }
    }
    fail_or(
        hard_failures,
        &record.name,
        RecordOutcome::Done { copies, transcript_id: transcript.name.clone(), attendees },
    )
}

/// Per-tick cap on approved-but-not-yet-ingested captures a sweep re-checks by
/// record name (independent of the listing watermark).
const MAX_RECHECK_PER_TICK: i64 = 10;

/// Phase 2 of a sweep tick: the reader's own `allowed`, record-bound,
/// not-terminal, unexpired captures, fetched BY NAME. This is what makes a late
/// approval land even after the listing watermark moved past the record
/// (approval of an old record, a transcript that was still generating, a
/// transient failure in the route's immediate ingest). Bounded by each row's
/// `expires_at` (30 days). Never fails the tick: problems are logged and the
/// row is simply tried again next tick.
async fn recheck_open_captures(
    plane: &MeetPlane,
    http: &reqwest::Client,
    control_pool: &PgPool,
    state: &Arc<AppState>,
    reader: ResolvedAttendee,
    access_token: &str,
) -> usize {
    let pool = plane_pool(plane, control_pool);
    let open = match capture::open_bound(pool, reader.account_id, reader.owner_user_id, MAX_RECHECK_PER_TICK).await {
        Ok(rows) => rows,
        Err(e) => {
            tracing::warn!(error = %e, account_id = %reader.account_id, "google meet: could not read open captures");
            return 0;
        }
    };
    let mut copies = 0usize;
    for row in open {
        let Some(name) = row.conference_record_id.as_deref() else { continue };
        // Rotate (before any fetch, so a 403/404 row cannot starve the queue): bump updated_at so a row that keeps not finishing moves behind newer ones.
        if let Err(e) = capture::touch(pool, reader.account_id, reader.owner_user_id, name).await {
            tracing::warn!(error = %e, conference_record_id = %name, "google meet: could not rotate recheck row");
        }
        // A 403/404 here is NOT marked terminal: a pre-approval row can be
        // bound by the organizer's sweep for a record this member's own token
        // cannot see, and the organizer's pass still needs it.
        let record = match get_conference_record(http, access_token, name).await {
            Ok(Some(r)) => r,
            Ok(None) => continue,
            Err(e) => {
                tracing::warn!(error = %e, conference_record_id = %name, "google meet: recheck fetch failed");
                continue;
            }
        };
        match process_record_opt_in(plane, http, control_pool, state, reader, access_token, &record, RecordScope::OwnOnly).await {
            Ok(RecordOutcome::Done { copies: n, .. }) => copies += n,
            Ok(_) => {}
            Err(e) => tracing::warn!(error = %e, conference_record_id = %name, "google meet: recheck of an approved capture failed"),
        }
    }
    copies
}

/// What [`sync_owner_once`] accomplished this tick.
pub(crate) struct SweepOutcome {
    /// Copies that landed this tick (0 for a tick where nobody approved anything).
    pub(crate) conferences_processed: usize,
    /// The new `last_synced_end_time` to persist, when it advanced at all -
    /// `None` means nothing changed (nothing due, or the very first record
    /// this tick already needed a retry - see this function's own doc).
    pub(crate) new_watermark: Option<DateTime<Utc>>,
}

/// One connected member's sweep pass, **opt-in only**.
///
/// Phase 1 lists conference records that ended since `since` and, oldest first,
/// runs [`process_record_opt_in`] on up to [`MAX_CONFERENCE_RECORDS_PER_TICK`]
/// of them. Records nobody approved are NOT ingested; they are ledgered as seen
/// (so the boundary record is not re-fetched every tick) and the watermark moves
/// past them. That is safe because a late approval never depends on the watermark:
///
/// * a pre-approval (space / meeting code) only covers records that end AFTER it
///   was given, so it is always in place before the record shows up here;
/// * an explicit allow by record id ingests immediately (route) and, if that
///   could not finish (meeting still running, transcript still generating, a
///   transient error), phase 2 re-fetches the record BY NAME every tick until
///   the capture's 30-day expiry.
///
/// The un-approved candidates the member can still approve are listed live from
/// Meet by `GET /v1/connectors/google/meet/meetings` (Google itself keeps them
/// ~30 days), so nothing is stored about a meeting nobody approved.
///
/// **Never silently drops an approved record** (contract section 6): the first
/// record that fails, or whose approved transcript is still generating, stops the
/// loop without advancing the watermark past it, so it is reconsidered next tick.
#[allow(clippy::too_many_arguments)] // `plane` joined an already-long positional list; a params struct would churn every pg-test call site
pub(crate) async fn sync_owner_once(
    plane: &MeetPlane,
    http: &reqwest::Client,
    control_pool: &PgPool,
    state: &Arc<AppState>,
    account_id: Uuid,
    owner_user_id: Uuid,
    access_token: &str,
    since: DateTime<Utc>,
) -> Result<SweepOutcome, ConnectorError> {
    let reader = ResolvedAttendee { account_id, owner_user_id };
    let mut records = list_conference_records_since(http, access_token, since).await?;
    records.retain(|r| r.end_time.is_some());
    records.sort_by(|a, b| a.end_time.cmp(&b.end_time));

    let mut copies = 0usize;
    let mut watermark: Option<DateTime<Utc>> = None;

    for record in records.into_iter().take(MAX_CONFERENCE_RECORDS_PER_TICK) {
        let Some(end_time) = record.end_time.as_deref().and_then(parse_google_timestamp) else {
            continue;
        };

        if already_ingested(plane, control_pool, account_id, &record.name).await? {
            watermark = Some(end_time);
            continue;
        }

        match process_record_opt_in(plane, http, control_pool, state, reader, access_token, &record, RecordScope::Sweep).await {
            Ok(RecordOutcome::TranscriptPending | RecordOutcome::NotEnded) => break,
            Ok(outcome) => {
                let (transcript_id, attendees, landed) = match &outcome {
                    // Dedupe only: store nothing about a meeting nobody approved.
                    RecordOutcome::NothingApproved => (String::new(), 0, 0),
                    RecordOutcome::NoTranscript => (String::new(), 0, 0),
                    RecordOutcome::Done { copies, transcript_id, attendees } => (transcript_id.clone(), *attendees, *copies),
                    RecordOutcome::TranscriptPending | RecordOutcome::NotEnded => unreachable!("handled above"),
                };
                if let Err(e) = record_ingested_conference(
                    plane,
                    control_pool,
                    account_id,
                    owner_user_id,
                    &record.name,
                    &transcript_id,
                    attendees,
                    landed,
                )
                .await
                {
                    tracing::warn!(error = %e, conference_record_id = %record.name, "google meet: could not ledger a processed record; retrying next tick");
                    break;
                }
                copies += landed;
                watermark = Some(end_time);
            }
            Err(e) => {
                tracing::warn!(
                    error = %e, conference_record_id = %record.name, %account_id, %owner_user_id,
                    "google meet: sweep failed on a conference record - stopping this tick's pass \
                     here so it is retried, never skipped"
                );
                break;
            }
        }
    }

    copies += recheck_open_captures(plane, http, control_pool, state, reader, access_token).await;

    Ok(SweepOutcome {
        conferences_processed: copies,
        new_watermark: watermark,
    })
}

// ---------------------------------------------------------------------------
// Tests — pure logic only (HTTP/DB-backed behavior is pg-tested in
// `backend/tests/google_meet_pg.rs`).
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_transcript_content_labels_each_speaker_turn() {
        let entries = vec![
            TranscriptEntry {
                participant: "conferenceRecords/c/participants/1".into(),
                text: "hello there".into(),
            },
            TranscriptEntry {
                participant: "conferenceRecords/c/participants/2".into(),
                text: "hi back".into(),
            },
        ];
        let mut labels = HashMap::new();
        labels.insert("conferenceRecords/c/participants/1".to_string(), "Alice".to_string());
        labels.insert("conferenceRecords/c/participants/2".to_string(), "Bob".to_string());
        let content = build_transcript_content(&entries, &labels);
        assert_eq!(content, "Alice: hello there\nBob: hi back");
    }

    #[test]
    fn build_transcript_content_falls_back_to_generic_label_for_an_unmapped_participant() {
        let entries = vec![TranscriptEntry {
            participant: "conferenceRecords/c/participants/9".into(),
            text: "unmapped speaker".into(),
        }];
        let content = build_transcript_content(&entries, &HashMap::new());
        assert_eq!(content, "Participant: unmapped speaker");
    }

    #[test]
    fn build_transcript_content_drops_blank_entries() {
        let entries = vec![
            TranscriptEntry {
                participant: "p1".into(),
                text: "   ".into(),
            },
            TranscriptEntry {
                participant: "p1".into(),
                text: "real text".into(),
            },
        ];
        let content = build_transcript_content(&entries, &HashMap::new());
        assert_eq!(content, "Participant: real text");
    }

    #[test]
    fn participant_labels_skips_anonymous_and_phone_participants() {
        let participants = vec![
            Participant {
                name: "conferenceRecords/c/participants/1".into(),
                signedin_user: Some(SignedinUser {
                    user: "users/sub-1".into(),
                    display_name: "Alice".into(),
                }),
            },
            Participant {
                name: "conferenceRecords/c/participants/2".into(),
                signedin_user: None,
            },
        ];
        let labels = participant_labels(&participants);
        assert_eq!(labels.len(), 1);
        assert_eq!(labels.get("conferenceRecords/c/participants/1").unwrap(), "Alice");
        assert!(!labels.contains_key("conferenceRecords/c/participants/2"));
    }

    #[test]
    fn participant_labels_falls_back_to_generic_for_a_blank_display_name() {
        let participants = vec![Participant {
            name: "p1".into(),
            signedin_user: Some(SignedinUser {
                user: "users/sub-1".into(),
                display_name: "".into(),
            }),
        }];
        let labels = participant_labels(&participants);
        assert_eq!(labels.get("p1").unwrap(), "Participant");
    }

    #[test]
    fn parse_google_timestamp_parses_rfc3339_and_rejects_garbage() {
        assert!(parse_google_timestamp("2026-09-01T12:00:00Z").is_some());
        assert!(parse_google_timestamp("not a timestamp").is_none());
    }

    #[test]
    fn source_type_maps_to_the_transcript_chunk_strategy() {
        // Pins the whole reason SOURCE_TYPE is the bare "meet" literal, not
        // something more descriptive like "google_meet_transcript" — see
        // this module's own doc comment on SOURCE_TYPE.
        assert!(matches!(
            coralswarm_core::chunker::ChunkStrategy::for_source_type(SOURCE_TYPE),
            coralswarm_core::chunker::ChunkStrategy::Transcript
        ));
    }

    #[test]
    fn default_lookback_is_bounded_well_under_googles_30_day_retention() {
        assert!(default_lookback() < chrono::Duration::days(30));
        assert!(default_lookback() >= chrono::Duration::days(1));
    }

    /// A structural pin for C9/C15 (declared immutable — see this module's
    /// own doc, "Immutable, deliberately") that does not need Postgres: this
    /// connector's own source file must never mention the deletion/
    /// correction-propagation machinery every OTHER Google surface with real
    /// C9 support calls — the deletion cascade `google_gmail`'s and
    /// `google_chat`'s own per-connector deletion-propagation functions hand
    /// items to, and the tree-wide correction-propagation mechanism
    /// `docs/connectors/CONNECTOR_CONTRACT.md` §8.5 describes. A future edit
    /// that wired one of those in would flip this test red before it ever
    /// reached code review,
    /// the same "pinned by a test, not only a comment" discipline
    /// `google_chat`'s own `subscribed_event_types_exclude_updates_and_
    /// reactions` test uses for its own non-subscription declaration.
    ///
    /// The forbidden identifiers are assembled from fragments, deliberately —
    /// spelling any of them whole, even in THIS test's own source, would
    /// make the file self-match and this test permanently, silently
    /// vacuous (the same failure mode a naive version of this test hit
    /// during development: it matched its own doc comment, above).
    #[test]
    fn source_never_references_deletion_or_correction_propagation() {
        let source = include_str!("google_meet.rs");
        let forbidden = [
            ["delete", "_source"].concat(),
            ["propagate", "_deletions"].concat(),
            ["corrections", "::", "propagate"].concat(),
        ];
        for needle in &forbidden {
            assert!(
                !source.contains(needle.as_str()),
                "google_meet.rs must never reference {needle} — Meet transcripts are declared \
                 immutable (C9/C15), not correctable; see this module's own doc comment"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// pg-gated tests — real Postgres, `coralswarm_app`-equivalent read paths via
// the actual `resolve_google_identity_by_sub`/`resolve_primary_graph`/
// `ingest_atom` production code paths (no shortcuts, no mocked DB layer).
// Declared directly at file scope (not nested inside `mod tests` above) so
// `scripts/check_pg_suite_coverage.sh`'s in-module parser can enumerate and
// verify this suite is wired into `ci.yml` — same convention `routes::
// connectors::microsoft_owner_aware_pg_tests`'s own doc comment documents.
//
//   ORGT_TEST_BASE_URL=postgres://postgres:postgres@localhost:15544/postgres \
//     cargo test -p coralswarm-backend --lib connectors::google_meet::google_meet_pg_tests \
//     -- --test-threads=1
// ---------------------------------------------------------------------------

#[cfg(test)]
mod google_meet_pg_tests {
    use std::sync::Arc;

    use chrono::Utc;
    use coralswarm_core::kek::{generate_dek, load_kek, wrap_dek};
    use sqlx::postgres::PgPoolOptions;
    use sqlx::{Connection, Executor, PgConnection, PgPool};
    use uuid::Uuid;

    use super::*;
    use crate::connectors::TokenBundle;
    use crate::routes::oauth::store_token_bundle_tx;
    use crate::tenancy::scope::begin_account_scoped;
    use crate::AppState;

    fn base_url() -> Option<String> {
        std::env::var("ORGT_TEST_BASE_URL").ok()
    }
    fn with_db(base: &str, db: &str) -> String {
        let (prefix, rest) = base.split_at(base.rfind('/').expect("url has a path /"));
        let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
        format!("{prefix}/{db}{params}")
    }
    async fn recreate(base: &str, db: &str) -> PgConnection {
        let mut conn = PgConnection::connect(base).await.expect("connect admin");
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .unwrap();
        conn.execute(format!("CREATE DATABASE {db}").as_str()).await.unwrap();
        conn
    }
    async fn teardown(base: &str, db: &str, pool: PgPool) {
        pool.close().await;
        let mut admin_conn = PgConnection::connect(base).await.unwrap();
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }
    /// Same fixed-KEK convention as `routes::oauth`/`routes::connectors`' own
    /// pg test modules.
    fn set_test_kek() {
        // SAFETY: every test in this process sets `MASTER_KEK_HEX` to the
        // SAME constant, so a concurrent write from another test is benign
        // (identical bytes).
        unsafe { std::env::set_var("MASTER_KEK_HEX", "cc".repeat(32)) };
    }
    async fn seed_user(pool: &PgPool) -> Uuid {
        sqlx::query_scalar("INSERT INTO users(id, email) VALUES (gen_random_uuid(), $1) RETURNING id")
            .bind(format!("user-{}@test.local", Uuid::new_v4()))
            .fetch_one(pool)
            .await
            .unwrap()
    }
    /// Also seeds the owner's `account_members` row — `resolve_graph_
    /// reachable_by_user`/`resolve_graph_account_id` (both `SECURITY
    /// DEFINER`, `graphs.account_id IN (SELECT account_id FROM
    /// account_members WHERE user_id = ...)`) require it for EVERY plane,
    /// personal included; production provisioning
    /// (`clerk_provision.rs`) always inserts one alongside `accounts`.
    async fn seed_account_with_dek(pool: &PgPool, owner: Uuid) -> Uuid {
        let kek = load_kek().expect("test KEK loads");
        let wrapped = wrap_dek(&kek, &generate_dek()).expect("wrap test dek");
        let account_id: Uuid =
            sqlx::query_scalar("INSERT INTO accounts(owner_user_id, dek_wrapped) VALUES ($1, $2) RETURNING id")
                .bind(owner)
                .bind(&wrapped)
                .fetch_one(pool)
                .await
                .unwrap();
        sqlx::query("INSERT INTO account_members(account_id, user_id, role) VALUES ($1, $2, 'owner')")
            .bind(account_id)
            .bind(owner)
            .execute(pool)
            .await
            .unwrap();
        account_id
    }
    /// A connected Google identity for `owner` under `account_id`, carrying
    /// `sub` as its `provider_user_id` — the exact shape
    /// `resolve_google_identity_by_sub`/`resolve_google_identity_in_account`
    /// both read. `provider` is deliberately varied across tests (`"google_gmail"`,
    /// `"google_chat"`, `"google_meet"`) to prove the resolver is genuinely
    /// provider-family-blind (`provider LIKE 'google%'`), not `google_meet`-only.
    async fn connect_google_identity(pool: &PgPool, account_id: Uuid, owner: Uuid, provider: &str, sub: &str) {
        sqlx::query("INSERT INTO account_members(account_id,user_id,role,seat_tier_id) SELECT $1,$2,'member',CASE WHEN clerk_org_id IS NOT NULL THEN (SELECT id FROM seat_tiers WHERE code='standard' AND is_active=true) END FROM accounts WHERE id=$1 ON CONFLICT DO NOTHING")
            .bind(account_id).bind(owner).execute(pool).await.unwrap();
        let bundle = TokenBundle {
            access_token: format!("access-{sub}"),
            refresh_token: Some(format!("refresh-{sub}")),
            user_token: None,
            scope: "openid email".into(),
            expires_at: Some(Utc::now() + chrono::Duration::hours(1)),
            provider_workspace_id: format!("{sub}@example.com"),
            provider_workspace_name: Some(sub.to_string()),
            provider_user_id: sub.to_string(),
        };
        let mut tx = begin_account_scoped(pool, account_id).await.expect("begin scoped tx");
        store_token_bundle_tx(
            &mut tx,
            &crate::crypto::DekPlane::consumer(pool.clone()),
            account_id,
            provider,
            crate::connectors::TokenOwner::Member(owner),
            &bundle,
        )
        .await
        .expect("store_token_bundle_tx");
        tx.commit().await.expect("commit");
    }

    /// One row per atom chunk this connector wrote for `source_id_prefix`
    /// (`"meet:{conference_record_id}"`), in chunk order — reads
    /// `content_inline` directly (no R2 configured in `state_for` below, so
    /// this is where `ingest::submit`'s dev-mode branch stores it).
    async fn atom_contents(pool: &PgPool, graph_id: Uuid, source_id_prefix: &str) -> Vec<String> {
        sqlx::query_scalar(
            "SELECT content_inline FROM atoms \
             WHERE graph_id = $1 AND source_type = $2 AND source_id LIKE $3 \
             ORDER BY source_id",
        )
        .bind(graph_id)
        .bind(SOURCE_TYPE)
        .bind(format!("{source_id_prefix}:chunk:%"))
        .fetch_all(pool)
        .await
        .unwrap()
    }

    /// The pre-opt-in fan-out loop, kept here as a test helper: the hard-failure
    /// accounting (`resolution_failures` + per-attendee ingest errors => `Err`, never
    /// "done") is the rule `process_record_opt_in` still follows, and these tests pin
    /// the per-attendee ingest mechanics (own-ocean landing, org no-ocean skip).
    /// Fan `content` out to every attendee in `resolved`, then decide whether
    /// this pass is safe to ledger. `resolution_failures` (from [`resolve_attendees`])
    /// and every per-attendee ingest error are BOTH hard failures — `Ok(n)` (n =
    /// how many attendees actually landed a copy) is returned ONLY when there
    /// were none of either kind; otherwise `Err` is returned and the caller
    /// ([`process_conference_record`]) must NOT call `record_ingested_conference`
    /// — see that function's own doc for why (CodeRabbit review, fixed before
    /// merge: a hard failure must never be silently equated with "this record is
    /// done").
    ///
    /// Deliberately its own function (not inlined into `process_conference_record`)
    /// so it is directly pg-testable without needing a live HTTP mock for the
    /// transcript/participant LISTING calls this module has none for (same
    /// "no HTTP mock in this crate, no injection seam" limit `workers::
    /// google_chat_sweep`'s own test module doc already documents) — a hard
    /// failure is reproduced here by pointing `ingest_transcript_for_attendee` at
    /// a `ResolvedAttendee` whose `account_id` genuinely does not exist, which
    /// fails for real against a real Postgres database, no mocking required.
    #[allow(clippy::too_many_arguments)] // `plane` joined an already-long positional list; a params struct would churn every pg-test call site
    async fn fan_out_and_count(
        plane: &MeetPlane,
        state: &Arc<AppState>,
        control_pool: &PgPool,
        resolved: Vec<ResolvedAttendee>,
        resolution_failures: Vec<ConnectorError>,
        conference_record_id: &str,
        content: &str,
        display_names: &[String],
        event_timestamp: DateTime<Utc>,
    ) -> Result<usize, ConnectorError> {
        let mut hard_failures: Vec<String> = resolution_failures.iter().map(|e| e.to_string()).collect();
        let mut resolved_count = 0usize;
        for attendee in resolved {
            match ingest_transcript_for_attendee(
                plane,
                state,
                control_pool,
                attendee,
                conference_record_id,
                content,
                display_names,
                event_timestamp,
            )
            .await
            {
                Ok(true) => resolved_count += 1,
                Ok(false) => {}
                Err(e) => {
                    tracing::warn!(
                        error = %e, %conference_record_id,
                        account_id = %attendee.account_id, owner_user_id = %attendee.owner_user_id,
                        "google meet: fan-out ingest failed for one attendee (transient, NOT \
                         permanent) — this meeting will not be ledgered as done this tick, so it \
                         retries; continuing with this pass's other attendees in the meantime"
                    );
                    hard_failures.push(format!(
                        "{e} (account {}, owner {})",
                        attendee.account_id, attendee.owner_user_id
                    ));
                }
            }
        }
        if !hard_failures.is_empty() {
            return Err(ConnectorError::Exchange(format!(
                "google meet: {} of {} attendee(s) hit a hard failure for {conference_record_id} — \
                 not ledgering; the whole record retries next tick: {}",
                hard_failures.len(),
                hard_failures.len() + resolved_count,
                hard_failures.join("; ")
            )));
        }
        Ok(resolved_count)
    }

    fn state_for(pool: PgPool) -> Arc<AppState> {
        Arc::new(crate::test_app_state_owned_with_db_and_store(pool, None))
    }

    /// The founder's fan-out decision (this module's own doc), proven
    /// end-to-end against real Postgres: a signed-in participant with NO
    /// connected Google identity, and an anonymous/phone participant (which
    /// structurally can never HAVE one), are both silently excluded from
    /// [`resolve_attendees`]'s result — never held, retried, or queued — while
    /// EVERY resolved participant lands the SAME transcript content in THEIR
    /// OWN primary ocean, verified to be genuinely distinct oceans on
    /// genuinely distinct accounts (not merely distinct rows on one shared
    /// graph).
    #[tokio::test]
    async fn fan_out_reaches_only_resolved_attendees_each_into_their_own_ocean_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping fan_out_reaches_only_resolved_attendees_each_into_their_own_ocean_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        set_test_kek();
        let db = "gsuite_g3_2_meet_fanout";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let owner_alice = seed_user(&pool).await;
        let account_alice = seed_account_with_dek(&pool, owner_alice).await;
        connect_google_identity(&pool, account_alice, owner_alice, "google_gmail", "sub-alice").await;

        let owner_bob = seed_user(&pool).await;
        let account_bob = seed_account_with_dek(&pool, owner_bob).await;
        connect_google_identity(&pool, account_bob, owner_bob, "google_chat", "sub-bob").await;

        // "sub-nobody" never connects anything, anywhere — the negative
        // control. An anonymous participant (no `signedin_user` at all) is
        // included to prove that shape is excluded too, not merely an
        // unresolved sub.
        let participants = vec![
            Participant {
                name: "conferenceRecords/x/participants/1".into(),
                signedin_user: Some(SignedinUser {
                    user: "users/sub-alice".into(),
                    display_name: "Alice".into(),
                }),
            },
            Participant {
                name: "conferenceRecords/x/participants/2".into(),
                signedin_user: Some(SignedinUser {
                    user: "users/sub-bob".into(),
                    display_name: "Bob".into(),
                }),
            },
            Participant {
                name: "conferenceRecords/x/participants/3".into(),
                signedin_user: Some(SignedinUser {
                    user: "users/sub-nobody".into(),
                    display_name: "Nobody".into(),
                }),
            },
            Participant {
                name: "conferenceRecords/x/participants/4".into(),
                signedin_user: None,
            },
        ];

        // Direct negative control on the resolver itself.
        assert_eq!(
            resolve_attendee(&pool, "sub-nobody").await.unwrap(),
            None,
            "an attendee with no connected Google identity anywhere must resolve to nothing"
        );

        let (resolved, resolution_failures) = resolve_attendees(&MeetPlane::Personal, &pool, &participants).await;
        assert!(
            resolution_failures.is_empty(),
            "every one of these lookups is a clean hit-or-miss against a real DB, never a hard \
             failure: {resolution_failures:?}"
        );
        assert_eq!(
            resolved.len(),
            2,
            "exactly the two connected participants resolve — the unresolved sub and the \
             anonymous participant must be silently excluded, got {resolved:?}"
        );
        assert!(resolved.contains(&ResolvedAttendee {
            account_id: account_alice,
            owner_user_id: owner_alice
        }));
        assert!(resolved.contains(&ResolvedAttendee {
            account_id: account_bob,
            owner_user_id: owner_bob
        }));

        let state = state_for(pool.clone());
        let content = "Alice: hello there\nBob: hi back";
        let conference_record_id = "conferenceRecords/x";
        for attendee in &resolved {
            let landed = ingest_transcript_for_attendee(
                &MeetPlane::Personal,
                &state,
                &pool,
                *attendee,
                conference_record_id,
                content,
                &["Alice".to_string(), "Bob".to_string()],
                Utc::now(),
            )
            .await
            .expect("ingest_transcript_for_attendee must succeed for a resolved attendee");
            assert!(landed);
        }

        let graph_alice = resolve_primary_graph(
            &pool,
            account_alice,
            AccountPlane::Personal,
            IngestCaller::Attributable(owner_alice),
        )
        .await
        .unwrap();
        let graph_bob = resolve_primary_graph(&pool, account_bob, AccountPlane::Personal, IngestCaller::Attributable(owner_bob))
            .await
            .unwrap();
        assert_ne!(graph_alice, graph_bob, "each attendee's own primary ocean must be genuinely distinct");

        // `ingest::submit` (a) prepends a contextual chunk header (date +
        // participants, `cs-phase0-chunk-headers`) and (b) chunks per speaker
        // turn (`ChunkStrategy::Transcript`, since `SOURCE_TYPE = "meet"`
        // maps to it — see this module's own `source_type_maps_to_the_
        // transcript_chunk_strategy` test), so the two-line transcript lands
        // as TWO atom rows, each `"{header}\n\n{one speaker's line}"` — never
        // byte-identical to `content` as a whole. Assert every original
        // speaker line survives, unmodified, somewhere in the chunk set.
        let source_id_prefix = format!("meet:{conference_record_id}");
        let alice_contents = atom_contents(&pool, graph_alice, &source_id_prefix).await;
        let bob_contents = atom_contents(&pool, graph_bob, &source_id_prefix).await;
        for (label, contents) in [("Alice", &alice_contents), ("Bob", &bob_contents)] {
            assert_eq!(contents.len(), 2, "{label}'s ocean must hold both speaker-turn chunks: {contents:?}");
            for line in content.split('\n') {
                assert!(
                    contents.iter().any(|c| c.ends_with(line)),
                    "{label}'s copy is missing the unmodified line {line:?}: {contents:?}"
                );
            }
        }

        // A Meet transcript is meeting-kind: each attendee's copy queues its
        // own summary (ADR-0016: `submit` enqueues it on every path).
        for graph_id in [graph_alice, graph_bob] {
            let queued: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM jobs WHERE kind = 'GenerateMeetingSummary' \
                 AND payload->>'graph_id' = $1 AND payload->>'source_id' = $2",
            )
            .bind(graph_id.to_string())
            .bind(&source_id_prefix)
            .fetch_one(&pool)
            .await
            .unwrap();
            assert_eq!(queued, 1, "graph {graph_id} must queue one meeting summary");
        }

        teardown(&base, db, pool).await;
    }

    /// C9/C15 (this module's own doc, "Immutable, deliberately"): a re-run
    /// against the SAME `(graph, conference_record_id)` with DIFFERENT
    /// content — modelling a hypothetical speaker-relabel correction arriving
    /// upstream — must never change the atom this connector already wrote.
    /// The capture funnel's locked native-Meet replay guard protects both
    /// the atom family and its immutable source snapshot end to end.
    #[tokio::test]
    async fn reingest_never_changes_the_already_landed_transcript_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping reingest_never_changes_the_already_landed_transcript_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        set_test_kek();
        let db = "gsuite_g3_2_meet_immutable";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let owner = seed_user(&pool).await;
        let account_id = seed_account_with_dek(&pool, owner).await;
        let attendee = ResolvedAttendee {
            account_id,
            owner_user_id: owner,
        };
        let state = state_for(pool.clone());
        let conference_record_id = "conferenceRecords/y";

        let landed = ingest_transcript_for_attendee(
            &MeetPlane::Personal,
            &state,
            &pool,
            attendee,
            conference_record_id,
            "Alice: the original transcript",
            &["Alice".to_string()],
            Utc::now(),
        )
        .await
        .expect("first ingest must succeed");
        assert!(landed, "first ingest must actually land (true)");

        let graph_id = resolve_primary_graph(&pool, account_id, AccountPlane::Personal, IngestCaller::Attributable(owner))
            .await
            .unwrap();
        let snapshot = |pool: PgPool| async move {
            sqlx::query_scalar::<_, serde_json::Value>(
                "SELECT jsonb_build_object(
                  'items',(SELECT jsonb_agg(to_jsonb(s) ORDER BY s.source_item_id) FROM source_items s WHERE s.graph_id=$1),
                  'versions',(SELECT jsonb_agg(to_jsonb(v) ORDER BY v.version_id) FROM source_versions v WHERE v.graph_id=$1),
                  'projection',(SELECT to_jsonb(m) FROM meeting_transcripts m WHERE m.graph_id=$1),
                  'jobs',(SELECT jsonb_agg(to_jsonb(j) ORDER BY j.id) FROM jobs j WHERE j.account_id=$2))",
            ).bind(graph_id).bind(account_id).fetch_one(&pool).await.unwrap()
        };
        let first_snapshot = snapshot(pool.clone()).await;

        // Same conference record, DIFFERENT content — the scenario a real
        // speaker-relabel correction would produce upstream.
        ingest_transcript_for_attendee(
            &MeetPlane::Personal,
            &state,
            &pool,
            attendee,
            conference_record_id,
            "Bob: a totally different, unrelated transcript",
            &["Bob".to_string()],
            Utc::now(),
        )
        .await
        .expect("a re-run must not error");

        let contents = atom_contents(&pool, graph_id, &format!("meet:{conference_record_id}")).await;
        assert_eq!(contents.len(), 1, "a re-run must not create a second atom for the same meeting");
        assert!(
            contents[0].ends_with("Alice: the original transcript"),
            "the FIRST call's content must survive unchanged: {:?}",
            contents[0]
        );
        assert!(
            !contents[0].contains("a totally different, unrelated transcript"),
            "the SECOND call's content must never overwrite the first — Meet transcripts are \
             declared immutable, not correctable (C9/C15): {:?}",
            contents[0]
        );
        assert_eq!(snapshot(pool.clone()).await, first_snapshot, "a changed repeat must preserve the raw version, current pointer, projection and jobs");

        teardown(&base, db, pool).await;
    }

    /// ADR-0016 §4 from a connector's side: an object-store outage aborts
    /// the capture and comes back as a retryable `Err`, never the `Ok(false)`
    /// that skips this attendee for good, so the record is not ledgered and
    /// the next tick lands it. Nothing is stored in between.
    #[tokio::test]
    async fn an_object_store_outage_is_retried_not_skipped_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping an_object_store_outage_is_retried_not_skipped_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        set_test_kek();
        let db = "gsuite_g3_2_meet_r2_outage";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let owner = seed_user(&pool).await;
        let account_id = seed_account_with_dek(&pool, owner).await;
        let attendee = ResolvedAttendee { account_id, owner_user_id: owner };
        let mut outage = crate::test_app_state_owned_with_db_and_store(pool.clone(), None);
        // A local S3 stand-in that refuses every upload, as R2 does during an
        // outage. No network.
        let (r2, store) = crate::test_object_store::failing_after(0).await;
        outage.r2 = Some(Arc::new(r2));
        let conference_record_id = "conferenceRecords/outage";
        let control = &pool;
        let ingest = |state: Arc<AppState>| async move {
            ingest_transcript_for_attendee(
                &MeetPlane::Personal,
                &state,
                control,
                attendee,
                conference_record_id,
                "Alice: the store is down for this one",
                &["Alice".to_string()],
                Utc::now(),
            )
            .await
        };

        let refused = ingest(Arc::new(outage)).await;
        assert!(
            matches!(&refused, Err(e) if e.to_string().contains("storage unavailable")),
            "an outage is a retryable failure, not a permanent skip: {refused:?}"
        );
        assert_eq!(store.puts(), 1, "the source's body was refused, and nothing followed it");
        let graph_id = resolve_primary_graph(&pool, account_id, AccountPlane::Personal, IngestCaller::Attributable(owner))
            .await
            .unwrap();
        let source_id = format!("meet:{conference_record_id}");
        assert!(atom_contents(&pool, graph_id, &source_id).await.is_empty(), "no coral without its body");
        let versions: i64 = sqlx::query_scalar("SELECT count(*) FROM source_versions WHERE graph_id = $1 AND source_id = $2")
            .bind(graph_id)
            .bind(&source_id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(versions, 0, "and no version either");

        assert!(ingest(state_for(pool.clone())).await.expect("the retry lands"), "the next tick stores it");
        assert_eq!(atom_contents(&pool, graph_id, &source_id).await.len(), 1);

        teardown(&base, db, pool).await;
    }

    /// **Negative control for CodeRabbit Major 1** (fixed before merge): a
    /// hard failure for ONE resolved attendee must never be silently
    /// absorbed while another attendee's success gets the whole meeting
    /// ledgered as done. Verified by temporarily reverting
    /// `fan_out_and_count`'s `if !hard_failures.is_empty() { return Err(...)
    /// }` guard (restoring this PR's original, buggy unconditional
    /// `Ok(resolved_count)`) and re-running this test locally: it fails,
    /// asserting `Ok(1)` where it expects `Err`. Restored before this commit.
    #[tokio::test]
    async fn a_hard_failure_for_one_attendee_never_lets_the_record_be_ledgered_as_done_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping a_hard_failure_for_one_attendee_never_lets_the_record_be_ledgered_as_done_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        set_test_kek();
        let db = "gsuite_g3_2_meet_hardfail";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let owner_good = seed_user(&pool).await;
        let account_good = seed_account_with_dek(&pool, owner_good).await;

        // A `ResolvedAttendee` naming an account that genuinely does not
        // exist — `ingest_transcript_for_attendee`'s own
        // `resolve_primary_graph` call fails for real against this real
        // database. No HTTP mocking needed: this is a DB-level hard failure,
        // exactly the class Major 1 is about (a DEK read failure, pool
        // exhaustion, or an R2 outage would surface the identical
        // `Err(ConnectorError)` shape from deeper inside `ingest_atom`).
        let bogus_attendee = ResolvedAttendee {
            account_id: Uuid::new_v4(),
            owner_user_id: Uuid::new_v4(),
        };
        let good_attendee = ResolvedAttendee {
            account_id: account_good,
            owner_user_id: owner_good,
        };

        let state = state_for(pool.clone());
        let conference_record_id = "conferenceRecords/hardfail";
        let content = "Alice: hello\nGhost: this must not silently vanish";

        let result = fan_out_and_count(
            &MeetPlane::Personal,
            &state,
            &pool,
            vec![good_attendee, bogus_attendee],
            Vec::new(),
            conference_record_id,
            content,
            &["Alice".to_string(), "Ghost".to_string()],
            Utc::now(),
        )
        .await;

        assert!(
            result.is_err(),
            "a hard failure for the bogus attendee must surface as Err, not be silently absorbed \
             into a successful count — got {result:?}"
        );

        // Partial progress is preserved and safe: re-processing this record
        // on retry is idempotent (atoms' own ON CONFLICT DO NOTHING), so the
        // good attendee's copy landing now, even though the overall pass
        // reports failure, costs nothing.
        let graph_good = resolve_primary_graph(
            &pool,
            account_good,
            AccountPlane::Personal,
            IngestCaller::Attributable(owner_good),
        )
        .await
        .unwrap();
        let good_contents = atom_contents(&pool, graph_good, &format!("meet:{conference_record_id}")).await;
        assert!(
            !good_contents.is_empty(),
            "the attendee who resolved successfully must still receive their own copy"
        );

        teardown(&base, db, pool).await;
    }

    /// **Negative control for CodeRabbit Major 4** (fixed before merge): a
    /// reconnect must refresh `updated_at`, or an in-flight disconnect's
    /// `purge_not_after` cutoff (captured BEFORE the reconnect) would delete
    /// a row the reconnect just re-established interest in — the exact
    /// "purge_not_after — the reconnect race" class `connectors::
    /// google_disconnect`'s own module doc documents for the sibling watch
    /// tables. Verified by temporarily reverting `ensure_sync_state`'s `ON
    /// CONFLICT DO UPDATE SET updated_at = now()` back to `DO NOTHING` and
    /// re-running this test locally: it fails (`deleted == 1`, `survives ==
    /// false`). Restored before this commit. The purge query mirrors
    /// `connectors::google_disconnect::delete_meet_sync_state`'s own
    /// `updated_at <= purge_not_after` guard exactly (that function is
    /// module-private, so this proves the CONTRACT it relies on directly
    /// against the table rather than reaching into it).
    #[tokio::test]
    async fn reconnect_refreshes_updated_at_so_a_racing_disconnect_purge_does_not_delete_it_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping reconnect_refreshes_updated_at_so_a_racing_disconnect_purge_does_not_delete_it_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        set_test_kek();
        let db = "gsuite_g3_2_meet_reconnect_race";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let owner = seed_user(&pool).await;
        let account_id = seed_account_with_dek(&pool, owner).await;
        connect_google_identity(&pool, account_id, owner, "google_meet", "sub-reconnect").await;

        // Initial connect.
        ensure_sync_state(&pool, account_id, owner).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        // A disconnect request captures its cutoff HERE — strictly after the
        // original connect, but (in the race this test models) before the
        // member's reconnect that follows.
        let purge_not_after: DateTime<Utc> = sqlx::query_scalar("SELECT now()").fetch_one(&pool).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        // The member reconnects — same (account_id, owner_user_id), so this
        // hits the ON CONFLICT arm, not a fresh INSERT.
        ensure_sync_state(&pool, account_id, owner).await.unwrap();

        // The disconnect's own purge now runs, using the cutoff it captured
        // BEFORE the reconnect above — byte-identical predicate to
        // `delete_meet_sync_state`'s real query.
        let deleted: i64 = sqlx::query_scalar(
            "WITH d AS ( \
                DELETE FROM google_meet_sync_state \
                WHERE account_id = $1 AND owner_user_id = $2 AND updated_at <= $3 \
                RETURNING 1 \
             ) SELECT COUNT(*) FROM d",
        )
        .bind(account_id)
        .bind(owner)
        .bind(purge_not_after)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            deleted, 0,
            "the reconnect's own refresh must move updated_at past the disconnect's already-\
             captured cutoff, so the racing purge must find nothing to delete"
        );

        let survives: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM google_meet_sync_state WHERE account_id = $1 AND owner_user_id = $2)",
        )
        .bind(account_id)
        .bind(owner)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(survives, "the row must still exist after the racing purge ran");

        teardown(&base, db, pool).await;
    }

    // ── Meet org-plane lift: org fan-out, ledger, and cross-plane isolation ──
    //
    // Two REAL databases per test — a control DB (`migrations_control`) and an
    // org TENANT DB (`migrations_tenant`) — exactly the plane split production
    // has for an ordinary org. `ORGT_TEST_BASE_URL` unset skips (and, like
    // every pg test here, passes vacuously) — the PR description states
    // whether these really ran.

    async fn tenant_db_pool(base: &str, db: &str, org_id: &str) -> PgPool {
        let conn = recreate(base, db).await;
        conn.close().await.ok();
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(base, db)).await.unwrap();
        sqlx::migrate!("./migrations_tenant").run(&pool).await.unwrap();
        sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, $1)")
            .bind(org_id)
            .execute(&pool)
            .await
            .unwrap();
        pool
    }

    async fn control_db_pool(base: &str, db: &str) -> PgPool {
        let conn = recreate(base, db).await;
        conn.close().await.ok();
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();
        pool
    }

    /// An ORG account on the control plane (`clerk_org_id` set), plus the
    /// org's own DEK on the TENANT plane (`account_keys`) where org
    /// ingest reads it from. (The control row also carries a test DEK only so
    /// `connect_google_identity`'s consumer-plane `store_token_bundle_tx` can
    /// encrypt a fixture token — production never has one there.)
    async fn seed_org_account(control: &PgPool, tenant: &PgPool, owner: Uuid, clerk_org_id: &str) -> Uuid {
        let kek = load_kek().expect("test KEK loads");
        let wrapped = wrap_dek(&kek, &generate_dek()).expect("wrap org dek");
        // No `account_members` rows: nothing in the Meet path reads membership
        // (an org attendee is identified by its connected-identity row, and
        // the destination by its member ocean), and the org-member seat-tier
        // trigger would force fixture noise for no coverage.
        let account_id: Uuid = sqlx::query_scalar(
            "INSERT INTO accounts(owner_user_id, clerk_org_id, dek_wrapped) VALUES ($1, $2, $3) RETURNING id",
        )
        .bind(owner)
        .bind(clerk_org_id)
        .bind(&wrapped)
        .fetch_one(control)
        .await
        .unwrap();
        sqlx::query("INSERT INTO account_keys (account_id, dek_wrapped) VALUES ($1, $2)")
            .bind(account_id)
            .bind(&wrapped)
            .execute(tenant)
            .await
            .unwrap();
        account_id
    }

    async fn seed_member_ocean(tenant: &PgPool, account_id: Uuid, user: Uuid) -> Uuid {
        sqlx::query_scalar(
            "INSERT INTO graphs(account_id, name, kind, is_primary, owner_user_id) \
             VALUES ($1, 'member ocean', 'member', true, $2) RETURNING id",
        )
        .bind(account_id)
        .bind(user)
        .fetch_one(tenant)
        .await
        .unwrap()
    }

    fn signed_in(n: u8, sub: &str, name: &str) -> Participant {
        Participant {
            name: format!("conferenceRecords/x/participants/{n}"),
            signedin_user: Some(SignedinUser {
                user: format!("users/{sub}"),
                display_name: name.into(),
            }),
        }
    }

    async fn meet_atom_graphs(pool: &PgPool) -> Vec<Uuid> {
        sqlx::query_scalar("SELECT DISTINCT graph_id FROM atoms WHERE source_type = $1 ORDER BY graph_id")
            .bind(SOURCE_TYPE)
            .fetch_all(pool)
            .await
            .unwrap()
    }

    /// The headline org test: an org-plane pass resolves attendees ONLY inside
    /// its own org, lands each copy in that member's own member-private ocean
    /// on the TENANT database, skips an org member who has no live ocean
    /// without failing the record, and never touches the control/personal
    /// plane. The same participants run through the PERSONAL pass resolve to
    /// the one personal-only attendee and nobody else — org members are
    /// refused, and a person whose `sub` is connected under BOTH planes is
    /// ambiguous and gets nothing (never guessed).
    #[tokio::test]
    async fn org_fan_out_stays_inside_the_org_and_never_crosses_planes_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping org_fan_out_stays_inside_the_org_and_never_crosses_planes_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        set_test_kek();
        let control = control_db_pool(&base, "meet_org_fanout_ctl").await;
        let tenant = tenant_db_pool(&base, "meet_org_fanout_ten", "org_meet_fanout").await;

        let org_owner = seed_user(&control).await;
        let org_account = seed_org_account(&control, &tenant, org_owner, "org_meet_fanout").await;
        let alice = seed_user(&control).await;
        let bob = seed_user(&control).await;
        let dave = seed_user(&control).await;
        let eve = seed_user(&control).await;
        for (u, sub) in [(alice, "sub-alice"), (bob, "sub-bob"), (dave, "sub-dave"), (eve, "sub-eve")] {
            connect_google_identity(&control, org_account, u, "google_gmail", sub).await;
        }
        let g_alice = seed_member_ocean(&tenant, org_account, alice).await;
        let g_bob = seed_member_ocean(&tenant, org_account, bob).await;
        let g_dave = seed_member_ocean(&tenant, org_account, dave).await;
        // Eve: org member with an identity but NO member ocean (not
        // provisioned / archived).

        // Carol: personal-only. Dave ALSO has a personal account carrying the
        // SAME Google sub (the "in both planes" case).
        let carol = seed_user(&control).await;
        let carol_account = seed_account_with_dek(&control, carol).await;
        connect_google_identity(&control, carol_account, carol, "google_gmail", "sub-carol").await;
        let dave_personal_account = seed_account_with_dek(&control, dave).await;
        connect_google_identity(&control, dave_personal_account, dave, "google_gmail", "sub-dave").await;

        let participants = vec![
            signed_in(1, "sub-alice", "Alice"),
            signed_in(2, "sub-bob", "Bob"),
            signed_in(3, "sub-carol", "Carol"),
            signed_in(4, "sub-dave", "Dave"),
            signed_in(5, "sub-eve", "Eve"),
            Participant {
                name: "conferenceRecords/x/participants/6".into(),
                signedin_user: None,
            },
        ];

        // ── Org pass ──
        let plane = MeetPlane::Org(OrgMeetPlane {
            org_id: "org_meet_fanout".into(),
            account_id: org_account,
            pool: tenant.clone(),
        });
        let (resolved, failures) = resolve_attendees(&plane, &control, &participants).await;
        assert!(failures.is_empty());
        let mut owners: Vec<Uuid> = resolved.iter().map(|a| a.owner_user_id).collect();
        owners.sort();
        let mut expected = vec![alice, bob, dave, eve];
        expected.sort();
        assert_eq!(owners, expected, "org pass resolves exactly the org's own members (carol is not one)");
        assert!(
            resolved.iter().all(|a| a.account_id == org_account),
            "every org-pass attendee must be an attendee of the ORG account, never a personal one (dave's personal account included)"
        );

        let state = state_for(control.clone());
        let content = "Alice: org hello\nBob: org reply";
        let n = fan_out_and_count(
            &plane,
            &state,
            &control,
            resolved,
            Vec::new(),
            "conferenceRecords/org1",
            content,
            &["Alice".to_string(), "Bob".to_string()],
            Utc::now(),
        )
        .await
        .expect("an org member with no ocean must be skipped, not fail the record");
        assert_eq!(n, 3, "alice, bob and dave land; eve (no member ocean) is skipped");

        for g in [g_alice, g_bob, g_dave] {
            let landed_text = atom_contents(&tenant, g, "meet:conferenceRecords/org1").await.join("\n");
            assert!(
                landed_text.contains("Alice: org hello") && landed_text.contains("Bob: org reply"),
                "each org attendee's own member ocean on the tenant DB holds the transcript: {landed_text}"
            );
        }
        let mut landed = meet_atom_graphs(&tenant).await;
        landed.sort();
        let mut want = vec![g_alice, g_bob, g_dave];
        want.sort();
        assert_eq!(landed, want, "the tenant DB holds copies in exactly those three member oceans");
        assert!(
            meet_atom_graphs(&control).await.is_empty(),
            "an org pass must never write a Meet atom to the control/personal plane — not for carol, not for dave's personal account"
        );

        // ── Personal pass over the SAME participants ──
        let (personal, pfail) = resolve_attendees(&MeetPlane::Personal, &control, &participants).await;
        assert!(pfail.is_empty());
        assert_eq!(
            personal,
            vec![ResolvedAttendee { account_id: carol_account, owner_user_id: carol }],
            "the personal pass resolves only the personal-only attendee: org members are refused, and dave \
             (sub connected under BOTH planes) is ambiguous and fails closed to nothing"
        );

        control.close().await;
        teardown(&base, "meet_org_fanout_ten", tenant).await;
        let mut admin = PgConnection::connect(&base).await.unwrap();
        admin.execute("DROP DATABASE IF EXISTS meet_org_fanout_ctl WITH (FORCE)").await.ok();
    }

    /// An org pass refuses a foreign-account attendee even if one is handed to
    /// it (the structural guard behind the resolver) — nothing is written.
    #[tokio::test]
    async fn org_pass_refuses_an_attendee_of_another_account_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping org_pass_refuses_an_attendee_of_another_account_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        set_test_kek();
        let control = control_db_pool(&base, "meet_org_guard_ctl").await;
        let tenant = tenant_db_pool(&base, "meet_org_guard_ten", "org_meet_guard").await;
        let org_owner = seed_user(&control).await;
        let org_account = seed_org_account(&control, &tenant, org_owner, "org_meet_guard").await;
        let carol = seed_user(&control).await;
        let carol_account = seed_account_with_dek(&control, carol).await;

        let plane = MeetPlane::Org(OrgMeetPlane {
            org_id: "org_meet_guard".into(),
            account_id: org_account,
            pool: tenant.clone(),
        });
        let state = state_for(control.clone());
        let res = ingest_transcript_for_attendee(
            &plane,
            &state,
            &control,
            ResolvedAttendee { account_id: carol_account, owner_user_id: carol },
            "conferenceRecords/guard",
            "Carol: should never land",
            &["Carol".to_string()],
            Utc::now(),
        )
        .await;
        assert!(res.is_err(), "a personal attendee handed to an org pass must be refused outright");
        assert!(meet_atom_graphs(&tenant).await.is_empty());
        assert!(meet_atom_graphs(&control).await.is_empty());

        control.close().await;
        teardown(&base, "meet_org_guard_ten", tenant).await;
        let mut admin = PgConnection::connect(&base).await.unwrap();
        admin.execute("DROP DATABASE IF EXISTS meet_org_guard_ctl WITH (FORCE)").await.ok();
    }

    /// The org ledger lives on the TENANT database; the personal ledger on
    /// control. A row recorded on one plane is invisible to the other.
    #[tokio::test]
    async fn org_ledger_lives_on_the_tenant_plane_not_control_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping org_ledger_lives_on_the_tenant_plane_not_control_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        set_test_kek();
        let control = control_db_pool(&base, "meet_org_ledger_ctl").await;
        let tenant = tenant_db_pool(&base, "meet_org_ledger_ten", "org_meet_ledger").await;
        let org_owner = seed_user(&control).await;
        let org_account = seed_org_account(&control, &tenant, org_owner, "org_meet_ledger").await;
        let plane = MeetPlane::Org(OrgMeetPlane {
            org_id: "org_meet_ledger".into(),
            account_id: org_account,
            pool: tenant.clone(),
        });

        assert!(!already_ingested(&plane, &control, org_account, "conferenceRecords/l1").await.unwrap());
        record_ingested_conference(&plane, &control, org_account, org_owner, "conferenceRecords/l1", "t", 3, 2)
            .await
            .unwrap();
        assert!(already_ingested(&plane, &control, org_account, "conferenceRecords/l1").await.unwrap());
        assert!(
            !already_ingested(&MeetPlane::Personal, &control, org_account, "conferenceRecords/l1")
                .await
                .unwrap(),
            "the personal (control) ledger must not see the org's row"
        );
        let on_tenant: i64 = sqlx::query_scalar("SELECT count(*) FROM google_meet_ingested_conferences")
            .fetch_one(&tenant)
            .await
            .unwrap();
        let on_control: i64 = sqlx::query_scalar("SELECT count(*) FROM google_meet_ingested_conferences")
            .fetch_one(&control)
            .await
            .unwrap();
        assert_eq!((on_tenant, on_control), (1, 0));

        control.close().await;
        teardown(&base, "meet_org_ledger_ten", tenant).await;
        let mut admin = PgConnection::connect(&base).await.unwrap();
        admin.execute("DROP DATABASE IF EXISTS meet_org_ledger_ctl WITH (FORCE)").await.ok();
    }

    /// Connecting `google_meet` under an ORG account never writes a
    /// control-plane cursor row (the consumer tick would sweep it on the wrong
    /// plane); a personal account still does.
    #[tokio::test]
    async fn ensure_sync_state_never_writes_a_control_row_for_an_org_account_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping ensure_sync_state_never_writes_a_control_row_for_an_org_account_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        set_test_kek();
        let control = control_db_pool(&base, "meet_org_ensure_ctl").await;
        let tenant = tenant_db_pool(&base, "meet_org_ensure_ten", "org_meet_ensure").await;
        let org_owner = seed_user(&control).await;
        let org_account = seed_org_account(&control, &tenant, org_owner, "org_meet_ensure").await;
        let member = seed_user(&control).await;
        connect_google_identity(&control, org_account, member, "google_meet", "sub-member").await;
        ensure_sync_state(&control, org_account, member).await.unwrap();

        let personal = seed_user(&control).await;
        let personal_account = seed_account_with_dek(&control, personal).await;
        connect_google_identity(&control, personal_account, personal, "google_meet", "sub-personal").await;
        ensure_sync_state(&control, personal_account, personal).await.unwrap();

        let org_rows: i64 = sqlx::query_scalar("SELECT count(*) FROM google_meet_sync_state WHERE account_id = $1")
            .bind(org_account)
            .fetch_one(&control)
            .await
            .unwrap();
        let personal_rows: i64 = sqlx::query_scalar("SELECT count(*) FROM google_meet_sync_state WHERE account_id = $1")
            .bind(personal_account)
            .fetch_one(&control)
            .await
            .unwrap();
        assert_eq!((org_rows, personal_rows), (0, 1));

        control.close().await;
        teardown(&base, "meet_org_ensure_ten", tenant).await;
        let mut admin = PgConnection::connect(&base).await.unwrap();
        admin.execute("DROP DATABASE IF EXISTS meet_org_ensure_ctl WITH (FORCE)").await.ok();
    }

    // ─────────────────────────────────────────────────────────────────────
    // Per-meeting opt-in (founder decision 2026-10-06). End-to-end against a
    // real Postgres AND a local mock of the Meet REST API (the task-local
    // `TEST_API_BASE` seam): the sweep/route code path runs unmodified, only
    // the HTTP base URL differs.
    // ─────────────────────────────────────────────────────────────────────

    use crate::services::google_meet_capture as cap;
    use axum::extract::{Path as AxPath, State as AxState};
    use axum::routing::get as ax_get;
    use axum::Json as AxJson;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    struct MockRecord {
        id: &'static str,
        space_id: &'static str,
        code: &'static str,
        start: DateTime<Utc>,
        end: Option<DateTime<Utc>>,
        /// (sub, display name) for each signed-in participant.
        participants: Vec<(&'static str, &'static str)>,
        /// `""` = no transcript at all; else the transcript `state`.
        transcript: &'static str,
    }

    #[derive(Default)]
    struct MockMeet {
        records: Mutex<Vec<MockRecord>>,
        participants_calls: AtomicUsize,
        transcripts_calls: AtomicUsize,
        entries_calls: AtomicUsize,
        space_calls: AtomicUsize,
    }

    fn rec_json(r: &MockRecord) -> serde_json::Value {
        let mut v = serde_json::json!({
            "name": format!("conferenceRecords/{}", r.id),
            "startTime": r.start.to_rfc3339(),
            "space": format!("spaces/{}", r.space_id),
        });
        if let Some(e) = r.end {
            v["endTime"] = serde_json::json!(e.to_rfc3339());
        }
        v
    }

    async fn start_mock(mock: Arc<MockMeet>) -> String {
        async fn list(AxState(m): AxState<Arc<MockMeet>>) -> AxJson<serde_json::Value> {
            let recs: Vec<_> = m.records.lock().unwrap().iter().map(rec_json).collect();
            AxJson(serde_json::json!({ "conferenceRecords": recs }))
        }
        async fn get_one(
            AxState(m): AxState<Arc<MockMeet>>,
            AxPath(id): AxPath<String>,
        ) -> Result<AxJson<serde_json::Value>, axum::http::StatusCode> {
            let recs = m.records.lock().unwrap();
            recs.iter().find(|r| r.id == id).map(|r| AxJson(rec_json(r))).ok_or(axum::http::StatusCode::NOT_FOUND)
        }
        async fn transcripts(AxState(m): AxState<Arc<MockMeet>>, AxPath(id): AxPath<String>) -> AxJson<serde_json::Value> {
            m.transcripts_calls.fetch_add(1, Ordering::SeqCst);
            let recs = m.records.lock().unwrap();
            let state = recs.iter().find(|r| r.id == id).map(|r| r.transcript).unwrap_or("");
            if state.is_empty() {
                return AxJson(serde_json::json!({}));
            }
            AxJson(serde_json::json!({"transcripts": [{"name": format!("conferenceRecords/{id}/transcripts/t1"), "state": state}]}))
        }
        async fn entries(AxState(m): AxState<Arc<MockMeet>>, AxPath((id, _t)): AxPath<(String, String)>) -> AxJson<serde_json::Value> {
            m.entries_calls.fetch_add(1, Ordering::SeqCst);
            AxJson(serde_json::json!({"transcriptEntries": [
                {"participant": format!("conferenceRecords/{id}/participants/p0"), "text": "first speaker line"},
                {"participant": format!("conferenceRecords/{id}/participants/p1"), "text": "second speaker line"},
            ]}))
        }
        async fn participants(AxState(m): AxState<Arc<MockMeet>>, AxPath(id): AxPath<String>) -> AxJson<serde_json::Value> {
            m.participants_calls.fetch_add(1, Ordering::SeqCst);
            let recs = m.records.lock().unwrap();
            let ps: Vec<_> = recs
                .iter()
                .find(|r| r.id == id)
                .map(|r| {
                    r.participants
                        .iter()
                        .enumerate()
                        .map(|(i, (sub, name))| {
                            serde_json::json!({
                                "name": format!("conferenceRecords/{id}/participants/p{i}"),
                                "signedinUser": {"user": format!("users/{sub}"), "displayName": name},
                            })
                        })
                        .collect()
                })
                .unwrap_or_default();
            AxJson(serde_json::json!({ "participants": ps }))
        }
        async fn space(
            AxState(m): AxState<Arc<MockMeet>>,
            AxPath(id): AxPath<String>,
        ) -> Result<AxJson<serde_json::Value>, axum::http::StatusCode> {
            m.space_calls.fetch_add(1, Ordering::SeqCst);
            let recs = m.records.lock().unwrap();
            recs.iter()
                .find(|r| r.space_id == id || r.code == id)
                .map(|r| AxJson(serde_json::json!({"name": format!("spaces/{}", r.space_id), "meetingCode": r.code})))
                .ok_or(axum::http::StatusCode::NOT_FOUND)
        }
        let app = axum::Router::new()
            .route("/conferenceRecords", ax_get(list))
            .route("/conferenceRecords/{id}", ax_get(get_one))
            .route("/conferenceRecords/{id}/transcripts", ax_get(transcripts))
            .route("/conferenceRecords/{id}/transcripts/{t}/entries", ax_get(entries))
            .route("/conferenceRecords/{id}/participants", ax_get(participants))
            .route("/spaces/{id}", ax_get(space))
            .with_state(mock);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });
        format!("http://{addr}")
    }

    /// One record ended an hour ago in space `sp1` / code `abc-defg-hij`.
    fn one_record(participants: Vec<(&'static str, &'static str)>, transcript: &'static str) -> Arc<MockMeet> {
        let now = Utc::now();
        let m = MockMeet::default();
        m.records.lock().unwrap().push(MockRecord {
            id: "r1",
            space_id: "sp1",
            code: "abc-defg-hij",
            start: now - chrono::Duration::hours(2),
            end: Some(now - chrono::Duration::hours(1)),
            participants,
            transcript,
        });
        Arc::new(m)
    }

    const R1: &str = "conferenceRecords/r1";

    async fn run_sweep(
        url: &str,
        plane: &MeetPlane,
        control: &PgPool,
        state: &Arc<AppState>,
        reader: ResolvedAttendee,
        since: DateTime<Utc>,
    ) -> SweepOutcome {
        TEST_API_BASE
            .scope(
                url.to_string(),
                sync_owner_once(plane, &reqwest::Client::new(), control, state, reader.account_id, reader.owner_user_id, "tok", since),
            )
            .await
            .expect("sweep must not fail")
    }

    async fn cap_status(pool: &PgPool, account: Uuid, user: Uuid) -> Vec<(String, Option<String>)> {
        cap::load(pool, account, user)
            .await
            .unwrap()
            .into_iter()
            .map(|c| (c.status, c.conference_record_id))
            .collect()
    }

    struct Personal2 {
        control: PgPool,
        a: ResolvedAttendee,
        b: ResolvedAttendee,
    }

    /// Two PERSONAL members, each with their own account + connected Google
    /// identity (`sub-a` organizer/reader, `sub-b` attendee).
    async fn personal_two(base: &str, db: &str) -> Personal2 {
        set_test_kek();
        let control = control_db_pool(base, db).await;
        let ua = seed_user(&control).await;
        let aa = seed_account_with_dek(&control, ua).await;
        connect_google_identity(&control, aa, ua, "google_meet", "sub-a").await;
        let ub = seed_user(&control).await;
        let ab = seed_account_with_dek(&control, ub).await;
        connect_google_identity(&control, ab, ub, "google_meet", "sub-b").await;
        Personal2 {
            control,
            a: ResolvedAttendee { account_id: aa, owner_user_id: ua },
            b: ResolvedAttendee { account_id: ab, owner_user_id: ub },
        }
    }

    async fn personal_graph(pool: &PgPool, who: ResolvedAttendee) -> Uuid {
        resolve_primary_graph(pool, who.account_id, AccountPlane::Personal, IngestCaller::Attributable(who.owner_user_id))
            .await
            .unwrap()
    }

    async fn finish(base: &str, db: &str, control: PgPool) {
        teardown(base, db, control).await;
    }

    /// No capture row => NOTHING is ingested (the new default), the transcript is
    /// never even fetched, the record is ledgered so it is not re-listed every
    /// tick, and the watermark advances past it.
    #[tokio::test]
    async fn no_capture_means_not_ingested_and_transcript_never_fetched_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping no_capture_means_not_ingested_and_transcript_never_fetched_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "meet_optin_none";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let p = personal_two(&base, db).await;
        let mock = one_record(vec![("sub-a", "Alice"), ("sub-b", "Bob")], "FILE_GENERATED");
        let url = start_mock(mock.clone()).await;
        let state = state_for(p.control.clone());
        let since = Utc::now() - chrono::Duration::days(7);

        let out = run_sweep(&url, &MeetPlane::Personal, &p.control, &state, p.a, since).await;
        assert_eq!(out.conferences_processed, 0);
        assert!(out.new_watermark.is_some(), "the watermark moves past an un-approved record");
        assert!(meet_atom_graphs(&p.control).await.is_empty(), "nothing ingested without a capture");
        assert_eq!(mock.transcripts_calls.load(Ordering::SeqCst), 0, "an un-approved transcript is never read");
        assert_eq!(mock.entries_calls.load(Ordering::SeqCst), 0);
        assert!(already_ingested(&MeetPlane::Personal, &p.control, p.a.account_id, R1).await.unwrap());
        let (att, res): (i32, i32) = sqlx::query_as("SELECT attendee_count, resolved_count FROM google_meet_ingested_conferences")
            .fetch_one(&p.control)
            .await
            .unwrap();
        assert_eq!((att, res), (0, 0), "nothing about an un-approved meeting's size is stored");

        // Second tick: ledgered, so participants are not even re-fetched.
        let calls = mock.participants_calls.load(Ordering::SeqCst);
        run_sweep(&url, &MeetPlane::Personal, &p.control, &state, p.a, since).await;
        assert_eq!(mock.participants_calls.load(Ordering::SeqCst), calls);
        assert!(meet_atom_graphs(&p.control).await.is_empty());
        finish(&base, db, p.control).await;
    }

    /// Allow => the member's OWN copy lands exactly once and the row becomes
    /// `ingested`; the other attendee (who did not approve) gets nothing.
    #[tokio::test]
    async fn allow_ingests_the_approvers_own_copy_once_and_marks_it_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping allow_ingests_the_approvers_own_copy_once_and_marks_it_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "meet_optin_allow";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let p = personal_two(&base, db).await;
        let mock = one_record(vec![("sub-a", "Alice"), ("sub-b", "Bob")], "FILE_GENERATED");
        let url = start_mock(mock.clone()).await;
        let state = state_for(p.control.clone());

        let row = cap::allow_record(&p.control, p.a.account_id, p.a.owner_user_id, R1, Some("spaces/sp1")).await.unwrap();
        assert_eq!(row.status, "allowed");

        // The route's immediate path: OwnOnly, reader's own copy.
        let record = ConferenceRecord {
            name: R1.into(),
            start_time: Some((Utc::now() - chrono::Duration::hours(2)).to_rfc3339()),
            end_time: Some((Utc::now() - chrono::Duration::hours(1)).to_rfc3339()),
            space: Some("spaces/sp1".into()),
        };
        let out = TEST_API_BASE
            .scope(
                url.clone(),
                process_record_opt_in(&MeetPlane::Personal, &reqwest::Client::new(), &p.control, &state, p.a, "tok", &record, RecordScope::OwnOnly),
            )
            .await
            .unwrap();
        assert!(matches!(out, RecordOutcome::Done { copies: 1, .. }), "got {out:?}");

        let ga = personal_graph(&p.control, p.a).await;
        let text = atom_contents(&p.control, ga, &format!("meet:{R1}")).await.join("\n");
        assert!(text.contains("Alice: first speaker line") && text.contains("Bob: second speaker line"), "{text}");
        assert_eq!(meet_atom_graphs(&p.control).await, vec![ga], "only the approver's ocean holds a copy");
        assert_eq!(cap_status(&p.control, p.a.account_id, p.a.owner_user_id).await, vec![("ingested".into(), Some(R1.into()))]);
        assert!(cap_status(&p.control, p.b.account_id, p.b.owner_user_id).await.is_empty(), "no row was ever created for Bob");

        // Once: a second pass (sweep scope, which also looks at Bob) does nothing.
        let (t, e) = (mock.transcripts_calls.load(Ordering::SeqCst), mock.entries_calls.load(Ordering::SeqCst));
        let again = TEST_API_BASE
            .scope(
                url,
                process_record_opt_in(&MeetPlane::Personal, &reqwest::Client::new(), &p.control, &state, p.a, "tok", &record, RecordScope::Sweep),
            )
            .await
            .unwrap();
        assert!(matches!(again, RecordOutcome::NothingApproved), "got {again:?}");
        assert_eq!((mock.transcripts_calls.load(Ordering::SeqCst), mock.entries_calls.load(Ordering::SeqCst)), (t, e));
        assert_eq!(atom_contents(&p.control, ga, &format!("meet:{R1}")).await.join("\n"), text);
        finish(&base, db, p.control).await;
    }

    /// A record approved AFTER the sweep already advanced past it (ledgered,
    /// watermark moved) is still ingested: phase 2 re-fetches approved captures
    /// by name every tick, independent of the watermark.
    #[tokio::test]
    async fn late_approval_after_the_watermark_moved_is_picked_up_by_the_recheck_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping late_approval_after_the_watermark_moved_is_picked_up_by_the_recheck_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "meet_optin_late";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let p = personal_two(&base, db).await;
        let mock = one_record(vec![("sub-a", "Alice"), ("sub-b", "Bob")], "FILE_GENERATED");
        let url = start_mock(mock.clone()).await;
        let state = state_for(p.control.clone());
        let since = Utc::now() - chrono::Duration::days(7);

        let first = run_sweep(&url, &MeetPlane::Personal, &p.control, &state, p.a, since).await;
        let watermark = first.new_watermark.expect("advanced");
        assert!(meet_atom_graphs(&p.control).await.is_empty());

        // The member approves it later (the route's immediate ingest not having
        // happened/finished is exactly the case being modelled).
        cap::allow_record(&p.control, p.a.account_id, p.a.owner_user_id, R1, Some("spaces/sp1")).await.unwrap();
        let second = run_sweep(&url, &MeetPlane::Personal, &p.control, &state, p.a, watermark).await;
        assert_eq!(second.conferences_processed, 1);
        let ga = personal_graph(&p.control, p.a).await;
        assert_eq!(meet_atom_graphs(&p.control).await, vec![ga]);
        assert_eq!(cap_status(&p.control, p.a.account_id, p.a.owner_user_id).await[0].0, "ingested");
        finish(&base, db, p.control).await;
    }

    /// An approved record whose transcript is still generating holds the
    /// watermark (never silently dropped) and lands once it is generated.
    #[tokio::test]
    async fn approved_record_with_a_pending_transcript_is_retried_not_dropped_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping approved_record_with_a_pending_transcript_is_retried_not_dropped_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "meet_optin_pending";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let p = personal_two(&base, db).await;
        let mock = one_record(vec![("sub-a", "Alice")], "STARTED");
        let url = start_mock(mock.clone()).await;
        let state = state_for(p.control.clone());
        let since = Utc::now() - chrono::Duration::days(7);
        cap::allow_record(&p.control, p.a.account_id, p.a.owner_user_id, R1, None).await.unwrap();

        let out = run_sweep(&url, &MeetPlane::Personal, &p.control, &state, p.a, since).await;
        assert!(out.new_watermark.is_none(), "an approved, still-generating record must not advance the watermark");
        assert!(meet_atom_graphs(&p.control).await.is_empty());
        assert_eq!(cap_status(&p.control, p.a.account_id, p.a.owner_user_id).await[0].0, "allowed");

        mock.records.lock().unwrap()[0].transcript = "FILE_GENERATED";
        let out = run_sweep(&url, &MeetPlane::Personal, &p.control, &state, p.a, since).await;
        assert_eq!(out.conferences_processed, 1);
        assert_eq!(cap_status(&p.control, p.a.account_id, p.a.owner_user_id).await[0].0, "ingested");
        finish(&base, db, p.control).await;
    }

    /// Pre-approval by MEETING CODE (and by space) covers the record when it
    /// ends after the approval; it is bound to the record and ingested for the
    /// pre-approver ONLY (here: the attendee, not the organizer/reader).
    #[tokio::test]
    async fn pre_approval_by_meeting_code_and_by_space_covers_the_record_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping pre_approval_by_meeting_code_and_by_space_covers_the_record_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        for (db, by_code) in [("meet_optin_code", true), ("meet_optin_space", false)] {
            let p = personal_two(&base, db).await;
            let mock = one_record(vec![("sub-a", "Alice"), ("sub-b", "Bob")], "FILE_GENERATED");
            let url = start_mock(mock.clone()).await;
            let state = state_for(p.control.clone());
            let since = Utc::now() - chrono::Duration::days(7);

            // Bob pre-approves BEFORE the meeting ended (row created 3h ago).
            let (row, created) = if by_code {
                cap::pre_approve(&p.control, p.b.account_id, p.b.owner_user_id, None, Some("abc-defg-hij")).await.unwrap()
            } else {
                cap::pre_approve(&p.control, p.b.account_id, p.b.owner_user_id, Some("spaces/sp1"), None).await.unwrap()
            };
            assert!(created);
            sqlx::query("UPDATE google_meet_captures SET created_at = now() - interval '3 hours' WHERE id = $1")
                .bind(row.id)
                .execute(&p.control)
                .await
                .unwrap();

            let out = run_sweep(&url, &MeetPlane::Personal, &p.control, &state, p.a, since).await;
            assert_eq!(out.conferences_processed, 1, "{db}");
            if by_code {
                assert!(mock.space_calls.load(Ordering::SeqCst) >= 1, "a code-only pre-approval looks the space's code up");
            }
            let gb = personal_graph(&p.control, p.b).await;
            assert_eq!(meet_atom_graphs(&p.control).await, vec![gb], "{db}: only Bob's own ocean holds the copy");
            assert_eq!(cap_status(&p.control, p.b.account_id, p.b.owner_user_id).await, vec![("ingested".into(), Some(R1.into()))]);
            assert!(cap_status(&p.control, p.a.account_id, p.a.owner_user_id).await.is_empty(), "the organizer never approved");
            finish(&base, db, p.control).await;
        }
    }

    /// A pre-approval given AFTER the meeting ended does not cover it; neither
    /// does one for a different meeting code.
    #[tokio::test]
    async fn pre_approval_after_the_meeting_ended_or_for_another_code_covers_nothing_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping pre_approval_after_the_meeting_ended_or_for_another_code_covers_nothing_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "meet_optin_late_pre";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let p = personal_two(&base, db).await;
        let mock = one_record(vec![("sub-a", "Alice"), ("sub-b", "Bob")], "FILE_GENERATED");
        let url = start_mock(mock.clone()).await;
        let state = state_for(p.control.clone());
        let since = Utc::now() - chrono::Duration::days(7);
        // Alice: right code but approved just now (after the meeting ended).
        cap::pre_approve(&p.control, p.a.account_id, p.a.owner_user_id, None, Some("abc-defg-hij")).await.unwrap();
        // Bob: approved long ago, but for a different meeting.
        let (other, _) = cap::pre_approve(&p.control, p.b.account_id, p.b.owner_user_id, None, Some("zzz-zzzz-zzz")).await.unwrap();
        sqlx::query("UPDATE google_meet_captures SET created_at = now() - interval '3 hours' WHERE id = $1")
            .bind(other.id)
            .execute(&p.control)
            .await
            .unwrap();
        run_sweep(&url, &MeetPlane::Personal, &p.control, &state, p.a, since).await;
        assert!(meet_atom_graphs(&p.control).await.is_empty());
        assert_eq!(mock.transcripts_calls.load(Ordering::SeqCst), 0);
        finish(&base, db, p.control).await;
    }

    /// Revoke before ingest => never ingested, even by the sweep. Revoke after
    /// ingest reports that it was already ingested and removes nothing.
    #[tokio::test]
    async fn revoke_before_ingest_blocks_it_and_revoke_after_reports_ingested_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping revoke_before_ingest_blocks_it_and_revoke_after_reports_ingested_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "meet_optin_revoke";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let p = personal_two(&base, db).await;
        let mock = one_record(vec![("sub-a", "Alice"), ("sub-b", "Bob")], "FILE_GENERATED");
        let url = start_mock(mock.clone()).await;
        let state = state_for(p.control.clone());
        let since = Utc::now() - chrono::Duration::days(7);

        cap::allow_record(&p.control, p.a.account_id, p.a.owner_user_id, R1, None).await.unwrap();
        let out = cap::revoke_by_record(&p.control, p.a.account_id, p.a.owner_user_id, R1).await.unwrap();
        assert!(matches!(out, cap::RevokeOutcome::Revoked(_)));
        run_sweep(&url, &MeetPlane::Personal, &p.control, &state, p.a, since).await;
        assert!(meet_atom_graphs(&p.control).await.is_empty(), "a revoked capture is never ingested");
        assert_eq!(mock.transcripts_calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            cap::revoke_by_record(&p.control, p.a.account_id, p.a.owner_user_id, R1).await.unwrap(),
            cap::RevokeOutcome::NotFound
        );

        // Re-allow reopens it; once ingested, revoke says so and the copy stays.
        cap::allow_record(&p.control, p.a.account_id, p.a.owner_user_id, R1, None).await.unwrap();
        run_sweep(&url, &MeetPlane::Personal, &p.control, &state, p.a, since).await;
        let ga = personal_graph(&p.control, p.a).await;
        assert_eq!(meet_atom_graphs(&p.control).await, vec![ga]);
        assert_eq!(
            cap::revoke_by_record(&p.control, p.a.account_id, p.a.owner_user_id, R1).await.unwrap(),
            cap::RevokeOutcome::AlreadyIngested
        );
        assert_eq!(meet_atom_graphs(&p.control).await, vec![ga], "revoking after ingest removes nothing");
        // Re-allowing an ingested row leaves it ingested.
        let row = cap::allow_record(&p.control, p.a.account_id, p.a.owner_user_id, R1, None).await.unwrap();
        assert_eq!(row.status, "ingested");
        finish(&base, db, p.control).await;
    }

    /// Cross-member isolation: Alice's approval never ingests for Bob, and Bob's
    /// own approval later gives HIM his own copy without touching Alice's.
    #[tokio::test]
    async fn one_attendees_approval_never_ingests_for_another_attendee_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping one_attendees_approval_never_ingests_for_another_attendee_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "meet_optin_isolation";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let p = personal_two(&base, db).await;
        let mock = one_record(vec![("sub-a", "Alice"), ("sub-b", "Bob")], "FILE_GENERATED");
        let url = start_mock(mock.clone()).await;
        let state = state_for(p.control.clone());
        let since = Utc::now() - chrono::Duration::days(7);

        cap::allow_record(&p.control, p.a.account_id, p.a.owner_user_id, R1, None).await.unwrap();
        run_sweep(&url, &MeetPlane::Personal, &p.control, &state, p.a, since).await;
        let ga = personal_graph(&p.control, p.a).await;
        assert_eq!(meet_atom_graphs(&p.control).await, vec![ga], "only Alice's ocean has a copy");
        assert!(cap_status(&p.control, p.b.account_id, p.b.owner_user_id).await.is_empty());

        // Bob is invisible to Alice's rows and vice versa (explicit scope check).
        assert!(cap::load(&p.control, p.b.account_id, p.a.owner_user_id).await.unwrap().is_empty());
        assert!(cap::load(&p.control, p.a.account_id, p.b.owner_user_id).await.unwrap().is_empty());

        // Bob pre-approves in time (organizer is Alice; her token reads it).
        let (row, _) = cap::pre_approve(&p.control, p.b.account_id, p.b.owner_user_id, None, Some("abc-defg-hij")).await.unwrap();
        sqlx::query("UPDATE google_meet_captures SET created_at = now() - interval '3 hours' WHERE id = $1")
            .bind(row.id)
            .execute(&p.control)
            .await
            .unwrap();
        let record = ConferenceRecord {
            name: R1.into(),
            start_time: Some((Utc::now() - chrono::Duration::hours(2)).to_rfc3339()),
            end_time: Some((Utc::now() - chrono::Duration::hours(1)).to_rfc3339()),
            space: Some("spaces/sp1".into()),
        };
        let out = TEST_API_BASE
            .scope(
                url,
                process_record_opt_in(&MeetPlane::Personal, &reqwest::Client::new(), &p.control, &state, p.a, "tok", &record, RecordScope::Sweep),
            )
            .await
            .unwrap();
        assert!(matches!(out, RecordOutcome::Done { copies: 1, .. }), "only Bob's NEW copy lands, got {out:?}");
        let gb = personal_graph(&p.control, p.b).await;
        let mut want = vec![ga, gb];
        want.sort();
        assert_eq!(meet_atom_graphs(&p.control).await, want);
        finish(&base, db, p.control).await;
    }

    /// An approved meeting that has no transcript at all is terminal (no
    /// endless re-checking) and the row is never marked ingested.
    #[tokio::test]
    async fn approved_meeting_without_a_transcript_is_terminal_not_ingested_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping approved_meeting_without_a_transcript_is_terminal_not_ingested_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "meet_optin_notx";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let p = personal_two(&base, db).await;
        let mock = one_record(vec![("sub-a", "Alice")], "");
        let url = start_mock(mock.clone()).await;
        let state = state_for(p.control.clone());
        let since = Utc::now() - chrono::Duration::days(7);
        cap::allow_record(&p.control, p.a.account_id, p.a.owner_user_id, R1, None).await.unwrap();
        let out = run_sweep(&url, &MeetPlane::Personal, &p.control, &state, p.a, since).await;
        assert!(out.new_watermark.is_some());
        assert!(meet_atom_graphs(&p.control).await.is_empty());
        let reason: Option<String> = sqlx::query_scalar("SELECT terminal_reason FROM google_meet_captures")
            .fetch_one(&p.control)
            .await
            .unwrap();
        assert_eq!(reason.as_deref(), Some("no_transcript"));
        assert!(cap::open_bound(&p.control, p.a.account_id, p.a.owner_user_id, 10).await.unwrap().is_empty());
        finish(&base, db, p.control).await;
    }

    /// Org plane: capture rows, the ledger and the copies all live on the org's
    /// TENANT database; only the approving org member gets a copy; the control
    /// plane holds no capture rows at all.
    #[tokio::test]
    async fn org_plane_opt_in_writes_only_the_approvers_copy_on_the_tenant_plane_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping org_plane_opt_in_writes_only_the_approvers_copy_on_the_tenant_plane_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        set_test_kek();
        let control = control_db_pool(&base, "meet_optin_org_ctl").await;
        let tenant = tenant_db_pool(&base, "meet_optin_org_ten", "org_optin").await;
        let owner = seed_user(&control).await;
        let org = seed_org_account(&control, &tenant, owner, "org_optin").await;
        let alice = seed_user(&control).await;
        let bob = seed_user(&control).await;
        connect_google_identity(&control, org, alice, "google_meet", "sub-a").await;
        connect_google_identity(&control, org, bob, "google_meet", "sub-b").await;
        let g_alice = seed_member_ocean(&tenant, org, alice).await;
        let g_bob = seed_member_ocean(&tenant, org, bob).await;
        let plane = MeetPlane::Org(OrgMeetPlane { org_id: "org_optin".into(), account_id: org, pool: tenant.clone() });
        let reader = ResolvedAttendee { account_id: org, owner_user_id: alice };

        let mock = one_record(vec![("sub-a", "Alice"), ("sub-b", "Bob")], "FILE_GENERATED");
        let url = start_mock(mock.clone()).await;
        let state = state_for(control.clone());
        let since = Utc::now() - chrono::Duration::days(7);

        // Nobody approved: nothing, and the ledger is on the tenant plane.
        run_sweep(&url, &plane, &control, &state, reader, since).await;
        assert!(meet_atom_graphs(&tenant).await.is_empty());
        let tenant_ledger: i64 = sqlx::query_scalar("SELECT count(*) FROM google_meet_ingested_conferences").fetch_one(&tenant).await.unwrap();
        assert_eq!(tenant_ledger, 1);

        // Bob approves by id (late, via the recheck-by-name path of HIS sweep
        // would need his token; here the organizer pass honours his pre-approval).
        let (row, _) = cap::pre_approve(&tenant, org, bob, Some("spaces/sp1"), None).await.unwrap();
        sqlx::query("UPDATE google_meet_captures SET created_at = now() - interval '3 hours' WHERE id = $1")
            .bind(row.id)
            .execute(&tenant)
            .await
            .unwrap();
        let record = ConferenceRecord {
            name: R1.into(),
            start_time: Some((Utc::now() - chrono::Duration::hours(2)).to_rfc3339()),
            end_time: Some((Utc::now() - chrono::Duration::hours(1)).to_rfc3339()),
            space: Some("spaces/sp1".into()),
        };
        let out = TEST_API_BASE
            .scope(
                url,
                process_record_opt_in(&plane, &reqwest::Client::new(), &control, &state, reader, "tok", &record, RecordScope::Sweep),
            )
            .await
            .unwrap();
        assert!(matches!(out, RecordOutcome::Done { copies: 1, .. }), "got {out:?}");
        assert_eq!(meet_atom_graphs(&tenant).await, vec![g_bob], "only Bob's member ocean (he approved); Alice did not");
        assert_ne!(g_bob, g_alice);
        assert_eq!(cap_status(&tenant, org, bob).await, vec![("ingested".into(), Some(R1.into()))]);
        let control_rows: i64 = sqlx::query_scalar("SELECT count(*) FROM google_meet_captures").fetch_one(&control).await.unwrap();
        assert_eq!(control_rows, 0, "org captures never touch the control plane");
        assert!(meet_atom_graphs(&control).await.is_empty());

        control.close().await;
        teardown(&base, "meet_optin_org_ten", tenant).await;
        let mut admin = PgConnection::connect(&base).await.unwrap();
        admin.execute("DROP DATABASE IF EXISTS meet_optin_org_ctl WITH (FORCE)").await.ok();
    }
    #[tokio::test]
    async fn meet_push_refetches_approved_record_behind_watermark_and_deduplicates_pg() {
        let Some(base) = base_url() else { return; };
        let db = "meet_push_optin";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let p = personal_two(&base, db).await;
        for reader in [p.a, p.b] {
            sqlx::query("INSERT INTO account_members(account_id,user_id,role) VALUES($1,$2,'owner') ON CONFLICT DO NOTHING")
                .bind(reader.account_id).bind(reader.owner_user_id).execute(&p.control).await.unwrap();
        }
        sqlx::query("INSERT INTO google_watch_channels(account_id,surface,resource_id,owner_user_id,channel_id,expires_at) VALUES($1,'meet','sub-a',$2,'subscriptions/push',now()+interval '1 day')")
            .bind(p.a.account_id).bind(p.a.owner_user_id).execute(&p.control).await.unwrap();
        let mock = one_record(vec![("sub-a", "Alice"), ("sub-b", "Bob")], "FILE_GENERATED");
        let url = start_mock(mock.clone()).await;
        let state = state_for(p.control.clone());
        let event = crate::routes::google_push::MeetEvent { subscription: "subscriptions/push".into(), record: R1.into() };
        cap::allow_record(&p.control, p.a.account_id, p.a.owner_user_id, R1, None).await.unwrap();
        ensure_sync_state(&p.control, p.a.account_id, p.a.owner_user_id).await.unwrap();
        sqlx::query("UPDATE google_meet_sync_state SET last_synced_end_time=now() WHERE account_id=$1").bind(p.a.account_id).execute(&p.control).await.unwrap();
        TEST_API_BASE.scope(url.clone(), async {
            let (a,b) = tokio::join!(crate::routes::google_push::run_meet_event(&state,&event), crate::routes::google_push::run_meet_event(&state,&event));
            a.unwrap();b.unwrap();
        }).await;
        let graph = personal_graph(&p.control,p.a).await;
        assert_eq!(meet_atom_graphs(&p.control).await, vec![graph]);
        assert!(cap_status(&p.control,p.b.account_id,p.b.owner_user_id).await.is_empty());
        assert_eq!(mock.entries_calls.load(Ordering::SeqCst),1);
        let unknown = crate::routes::google_push::MeetEvent { subscription:"subscriptions/unknown".into(), record:R1.into() };
        TEST_API_BASE.scope(url, crate::routes::google_push::run_meet_event(&state,&unknown)).await.unwrap();
        assert_eq!(mock.entries_calls.load(Ordering::SeqCst),1);
        finish(&base,db,p.control).await;
    }

    #[tokio::test]
    async fn org_meet_push_uses_tenant_approval_and_key_flag_and_current_reader_pg() {
        let Some(base)=base_url() else {return;};set_test_kek();
        let ctl_db="meet_push_org_ctl";
        let _cleanup_ctl_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", ctl_db);let ten_db="meet_push_org_ten";let org_id="org_meet_push";
        let control=control_db_pool(&base,ctl_db).await;let tenant=tenant_db_pool(&base,ten_db,org_id).await;
        let owner=seed_user(&control).await;let account=seed_org_account(&control,&tenant,owner,org_id).await;
        let alice=seed_user(&control).await;let bob=seed_user(&control).await;
        connect_google_identity(&control,account,alice,"google_meet","sub-a").await;
        connect_google_identity(&control,account,bob,"google_meet","sub-b").await;
        let ga=seed_member_ocean(&tenant,account,alice).await;let gb=seed_member_ocean(&tenant,account,bob).await;
        sqlx::query("UPDATE accounts SET dek_wrapped=NULL WHERE id=$1").bind(account).execute(&control).await.unwrap();
        // Enter the real account/period financial transaction for this guarded fixture write.
        let mut fixture_financial_tx = crate::tenancy::scope::begin_account_scoped(&control, account)
            .await.unwrap();
        crate::billing::renewal::lock_period_rotation(&mut fixture_financial_tx, account)
            .await.unwrap();
        sqlx::query("INSERT INTO tenant_databases(clerk_org_id,account_id,slug,db_secret_ref,substrate,schema_version,status) VALUES($1,$2,'meet-push-org','MEET_PUSH_ORG_TEST','neon',1,'active')").bind(org_id).bind(account).execute(&mut *fixture_financial_tx).await.unwrap();
        fixture_financial_tx.commit().await.unwrap();
        sqlx::query("INSERT INTO google_watch_channels(account_id,surface,resource_id,owner_user_id,channel_id,expires_at) VALUES($1,'meet','sub-a',$2,'subscriptions/org-push',now()+interval '1 day')").bind(account).bind(alice).execute(&control).await.unwrap();
        let resolver=crate::tenancy::secrets::StaticMapResolver::from_pairs([("MEET_PUSH_ORG_TEST",with_db(&base,ten_db))]);
        let registry=Arc::new(crate::tenancy::registry::TenantRegistry::new(Arc::new(resolver)));
        let runtime=crate::tenancy::runtime::TenantRuntime::new(true,"coralswarm.test",registry);
        let state=Arc::new(crate::test_app_state_owned_with_db(control.clone(),Some(runtime)));
        let mock=one_record(vec![("sub-a","Alice"),("sub-b","Bob")],"FILE_GENERATED");let url=start_mock(mock.clone()).await;
        let event=crate::routes::google_push::MeetEvent {subscription:"subscriptions/org-push".into(),record:R1.into()};
        cap::allow_record(&tenant,account,alice,R1,None).await.unwrap();
        crate::connector_flags::set_override(&control,&state.connector_flag_cache,org_id,"google_meet",false).await.unwrap();
        TEST_API_BASE.scope(url.clone(),crate::routes::google_push::run_meet_event(&state,&event)).await.unwrap();assert!(meet_atom_graphs(&tenant).await.is_empty());
        crate::connector_flags::set_override(&control,&state.connector_flag_cache,org_id,"google_meet",true).await.unwrap();
        sqlx::query("DELETE FROM account_members WHERE account_id=$1 AND user_id=$2").bind(account).bind(alice).execute(&control).await.unwrap();
        TEST_API_BASE.scope(url.clone(),crate::routes::google_push::run_meet_event(&state,&event)).await.unwrap();assert!(meet_atom_graphs(&tenant).await.is_empty());
        sqlx::query("INSERT INTO account_members(account_id,user_id,role,seat_tier_id) SELECT $1,$2,'member',id FROM seat_tiers WHERE code='standard' AND is_active=true").bind(account).bind(alice).execute(&control).await.unwrap();
        TEST_API_BASE.scope(url,crate::routes::google_push::run_meet_event(&state,&event)).await.unwrap();
        assert_eq!(meet_atom_graphs(&tenant).await,vec![ga]);assert_ne!(ga,gb);assert!(meet_atom_graphs(&control).await.is_empty());
        assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM google_meet_captures").fetch_one(&control).await.unwrap(),0);
        assert!(cap_status(&tenant,account,bob).await.is_empty());
        drop(state);control.close().await;teardown(&base,ten_db,tenant).await;let mut admin=PgConnection::connect(&base).await.unwrap();admin.execute(format!("DROP DATABASE {ctl_db} WITH (FORCE)").as_str()).await.unwrap();
    }

    #[tokio::test]
    async fn meet_subscription_create_expiry_heal_resolver_and_disconnect_owner_fence_pg() {
        let Some(base) = base_url() else { return; };
        let db="meet_subscription_lifecycle";
        let p=personal_two(&base,db).await;
        std::env::set_var("GOOGLE_MEET_PUBSUB_TOPIC","projects/test/topics/meet-events");
        let calls=Arc::new(AtomicUsize::new(0));
        let deletes=Arc::new(AtomicUsize::new(0));
        async fn create(AxState(c):AxState<Arc<AtomicUsize>>, axum::Json(body):axum::Json<serde_json::Value>) -> AxJson<serde_json::Value> {
            assert_eq!(body["targetResource"],"//cloudidentity.googleapis.com/users/sub-a");
            assert!(body.get("payloadOptions").is_none());
            let n=c.fetch_add(1,Ordering::SeqCst);
            let _ = n;
            AxJson(serde_json::json!({"name":"operations/create","done":false}))
        }
        let delete_count=deletes.clone();
        async fn complete(AxState(c):AxState<Arc<AtomicUsize>>) -> AxJson<serde_json::Value> {
            AxJson(serde_json::json!({"name":"operations/create","done":true,"response":{"name":format!("subscriptions/{}",c.load(Ordering::SeqCst)-1),"expireTime":(Utc::now()+chrono::Duration::days(7)).to_rfc3339()}}))
        }
        let app=axum::Router::new().route("/operations/create",axum::routing::get(complete)).route("/subscriptions",axum::routing::post(create)).route("/subscriptions/{id}",axum::routing::delete(move || {
            let count=delete_count.clone();async move {count.fetch_add(1,Ordering::SeqCst);AxJson(serde_json::json!({"name":"operations/delete","done":true}))}
        })).with_state(calls.clone());
        let listener=tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url=format!("http://{}",listener.local_addr().unwrap());
        tokio::spawn(async move {axum::serve(listener,app).await.unwrap();});
        let http=reqwest::Client::new();
        super::super::google_chat::TEST_WORKSPACE_EVENTS_BASE.scope(url,async {
            let (first,second)=tokio::join!(super::super::google_meet_events::ensure_subscription(&http,&p.control,p.a.account_id,p.a.owner_user_id,"sub-a","tok"),super::super::google_meet_events::ensure_subscription(&http,&p.control,p.a.account_id,p.a.owner_user_id,"sub-a","tok"));first.unwrap();second.unwrap();
            assert_eq!(calls.load(Ordering::SeqCst),1,"live subscriptions are idempotent");
            let rows=super::super::google_meet_events::resolve_subscription(&p.control,"subscriptions/0").await.unwrap();
            assert_eq!(rows.len(),1);assert_eq!((rows[0].1,rows[0].2),(p.a.account_id,p.a.owner_user_id));
            assert!(super::super::google_meet_events::resolve_subscription(&p.control,"subscriptions/unknown").await.unwrap().is_empty());
            sqlx::query("UPDATE google_watch_channels SET expires_at=now()-interval '1 hour' WHERE account_id=$1").bind(p.a.account_id).execute(&p.control).await.unwrap();
            super::super::google_meet_events::ensure_subscription(&http,&p.control,p.a.account_id,p.a.owner_user_id,"sub-a","tok").await.unwrap();
            assert_eq!(calls.load(Ordering::SeqCst),2);
            assert!(super::super::google_meet_events::resolve_subscription(&p.control,"subscriptions/0").await.unwrap().is_empty());
            let revoke_cutoff:DateTime<Utc>=sqlx::query_scalar("SELECT now()").fetch_one(&p.control).await.unwrap();
            assert!(super::super::google_disconnect::revoke_chat_subscription_best_effort(&http,&p.control,p.a.account_id,p.a.owner_user_id,"tok","meet",Some(revoke_cutoff)).await);
            assert_eq!(deletes.load(Ordering::SeqCst),1);
            cap::allow_record(&p.control,p.a.account_id,p.a.owner_user_id,"conferenceRecords/old",None).await.unwrap();
            cap::allow_record(&p.control,p.a.account_id,p.b.owner_user_id,"conferenceRecords/other",None).await.unwrap();
            cap::allow_record(&p.control,p.a.account_id,p.a.owner_user_id,"conferenceRecords/new",None).await.unwrap();
            let cutoff:DateTime<Utc>=sqlx::query_scalar("SELECT clock_timestamp()").fetch_one(&p.control).await.unwrap();
            // A sweep binding/rotation is not a new explicit approval.
            sqlx::query("UPDATE google_meet_captures SET updated_at=now()+interval '1 hour' WHERE conference_record_id='conferenceRecords/old'").execute(&p.control).await.unwrap();
            sqlx::query("UPDATE google_meet_captures SET approved_at=now()+interval '1 hour',updated_at=now()+interval '1 hour' WHERE conference_record_id='conferenceRecords/new'").execute(&p.control).await.unwrap();
            let outcome=super::super::google_disconnect::purge_google_connection(&p.control,&p.control,p.a.account_id,super::super::google::Surface::Meet,crate::connectors::TokenOwner::Member(p.a.owner_user_id),cutoff,cutoff).await;
            assert_eq!(outcome.watch_channels_deleted,1);
            assert_eq!(outcome.meet_captures_revoked,1,"old consent is revoked, concurrent new consent and other owner survive");
            assert!(super::super::google_meet_events::resolve_subscription(&p.control,"subscriptions/1").await.unwrap().is_empty());
            let statuses:Vec<(String,String)>=sqlx::query_as("SELECT conference_record_id,status FROM google_meet_captures ORDER BY conference_record_id").fetch_all(&p.control).await.unwrap();
            assert_eq!(statuses,vec![("conferenceRecords/new".into(),"allowed".into()),("conferenceRecords/old".into(),"revoked".into()),("conferenceRecords/other".into(),"allowed".into())]);
            super::super::google_meet_events::ensure_subscription(&http,&p.control,p.a.account_id,p.a.owner_user_id,"sub-a","tok").await.unwrap();
            assert!(!super::super::google_disconnect::revoke_chat_subscription_best_effort(&http,&p.control,p.a.account_id,p.a.owner_user_id,"tok","meet",Some(cutoff)).await,"old disconnect must not delete reconnected subscription");
            let fenced=super::super::google_disconnect::purge_google_connection(&p.control,&p.control,p.a.account_id,super::super::google::Surface::Meet,crate::connectors::TokenOwner::Member(p.a.owner_user_id),cutoff,cutoff).await;
            assert_eq!(fenced.watch_channels_deleted,0);
            assert_eq!(deletes.load(Ordering::SeqCst),1);
            // A conflicting row attributed to another owner cannot be taken
            // over even when the provider subject matches our own grant.
            sqlx::query("UPDATE google_watch_channels SET owner_user_id=$1 WHERE account_id=$2").bind(p.b.owner_user_id).bind(p.a.account_id).execute(&p.control).await.unwrap();
            super::super::google_meet_events::ensure_subscription(&http,&p.control,p.a.account_id,p.a.owner_user_id,"sub-a","tok").await.unwrap();
            let foreign=super::super::google_meet_events::resolve_subscription(&p.control,"subscriptions/2").await.unwrap();
            assert_eq!(foreign[0].2,p.b.owner_user_id);
            assert!(super::super::google_meet_events::resolve_subscription(&p.control,"subscriptions/3").await.unwrap().is_empty());
            assert_eq!(deletes.load(Ordering::SeqCst),2,"unpersisted orphan is stopped, foreign owner is untouched");
        }).await;
        finish(&base,db,p.control).await;
    }

    /// Google allows one Workspace Events subscription per (authorizing user,
    /// target resource) per app, so the same Google user connected in two
    /// CoralSwarm accounts must share one subscription name: the second
    /// create returns 409 ALREADY_EXISTS. The second account adopts the
    /// existing subscription, a push fans out to every account through its
    /// own opt-in fence, and one account's disconnect never deletes a
    /// subscription the other still references.
    #[tokio::test]
    async fn meet_one_google_user_in_two_accounts_shares_the_provider_subscription_pg() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let Some(base) = base_url() else { return; };
        let db = "meet_subscription_shared";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let p = personal_two(&base, db).await;
        let user_c = seed_user(&p.control).await;
        let account_c = seed_account_with_dek(&p.control, user_c).await;
        connect_google_identity(&p.control, account_c, user_c, "google_meet", "sub-a").await;
        let c = ResolvedAttendee { account_id: account_c, owner_user_id: user_c };
        const TOPIC: &str = "projects/test/topics/meet-events";
        std::env::set_var("GOOGLE_MEET_PUBSUB_TOPIC", TOPIC);

        #[derive(Default)]
        struct Events { posts: AtomicUsize, deletes: AtomicUsize, filters: std::sync::Mutex<Vec<String>> }
        fn existing(expire: &str) -> serde_json::Value {
            serde_json::json!({
                "name": "subscriptions/shared",
                "targetResource": "//cloudidentity.googleapis.com/users/sub-a",
                "eventTypes": [super::super::google_meet_events::EVENT_TYPE],
                "notificationEndpoint": {"pubsubTopic": TOPIC},
                "state": "ACTIVE",
                "expireTime": expire
            })
        }
        let expire = (Utc::now() + chrono::Duration::days(7)).to_rfc3339();
        async fn create(AxState(e): AxState<Arc<Events>>, axum::Json(body): axum::Json<serde_json::Value>) -> (axum::http::StatusCode, AxJson<serde_json::Value>) {
            assert_eq!(body["targetResource"], "//cloudidentity.googleapis.com/users/sub-a");
            if e.posts.fetch_add(1, Ordering::SeqCst) == 0 {
                let expire = (Utc::now() + chrono::Duration::days(7)).to_rfc3339();
                (axum::http::StatusCode::OK, AxJson(serde_json::json!({"name": "operations/create", "done": true, "response": existing(&expire)})))
            } else {
                (axum::http::StatusCode::CONFLICT, AxJson(serde_json::json!({"error": {"code": 409, "status": "ALREADY_EXISTS", "message": "subscription already exists"}})))
            }
        }
        let events = Arc::new(Events::default());
        let list_expire = expire.clone();
        let app = axum::Router::new()
            .route("/subscriptions", axum::routing::post(create).get(move |AxState(e): AxState<Arc<Events>>, axum::extract::Query(q): axum::extract::Query<std::collections::HashMap<String, String>>| {
                let expire = list_expire.clone();
                async move {
                    e.filters.lock().unwrap().push(q.get("filter").cloned().unwrap_or_default());
                    AxJson(serde_json::json!({"subscriptions": [existing(&expire)]}))
                }
            }))
            .route("/subscriptions/{id}", axum::routing::delete(|AxState(e): AxState<Arc<Events>>| async move {
                e.deletes.fetch_add(1, Ordering::SeqCst);
                AxJson(serde_json::json!({"name": "operations/delete", "done": true}))
            }))
            .with_state(events.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap(); });
        let http = reqwest::Client::new();
        let meet = one_record(vec![("sub-a", "Alice")], "FILE_GENERATED");
        let meet_url = start_mock(meet.clone()).await;
        let state = state_for(p.control.clone());
        let event = crate::routes::google_push::MeetEvent { subscription: "subscriptions/shared".into(), record: R1.into() };
        use crate::connectors::google_meet_events::{ensure_subscription, resolve_subscription, EVENT_TYPE};
        crate::connectors::google_chat::TEST_WORKSPACE_EVENTS_BASE.scope(url, TEST_API_BASE.scope(meet_url, async {
            ensure_subscription(&http, &p.control, p.a.account_id, p.a.owner_user_id, "sub-a", "tok").await.unwrap();
            assert_eq!(events.posts.load(Ordering::SeqCst), 1);
            ensure_subscription(&http, &p.control, c.account_id, c.owner_user_id, "sub-a", "tok").await.unwrap();
            assert_eq!(events.posts.load(Ordering::SeqCst), 2, "the second account's create is refused with 409");
            let filters = events.filters.lock().unwrap().clone();
            assert_eq!(filters.len(), 1, "one subscriptions.list after the 409");
            assert!(filters[0].contains(EVENT_TYPE) && filters[0].contains("//cloudidentity.googleapis.com/users/sub-a"), "{filters:?}");
            let mut rows: Vec<(Uuid, Uuid)> = resolve_subscription(&p.control, "subscriptions/shared").await.unwrap().into_iter().map(|(_, a, o)| (a, o)).collect();
            rows.sort();
            let mut want = vec![(p.a.account_id, p.a.owner_user_id), (c.account_id, c.owner_user_id)];
            want.sort();
            assert_eq!(rows, want, "both accounts reference the one provider subscription");
            assert_eq!(events.deletes.load(Ordering::SeqCst), 0, "adopting never stops the shared subscription");
            ensure_subscription(&http, &p.control, c.account_id, c.owner_user_id, "sub-a", "tok").await.unwrap();
            assert_eq!(events.posts.load(Ordering::SeqCst), 2, "an adopted live row is idempotent");

            for who in [p.a, c] {
                ensure_sync_state(&p.control, who.account_id, who.owner_user_id).await.unwrap();
            }
            cap::allow_record(&p.control, c.account_id, c.owner_user_id, R1, None).await.unwrap();
            crate::routes::google_push::run_meet_event(&state, &event).await.unwrap();
            let g_c = personal_graph(&p.control, c).await;
            assert_eq!(meet_atom_graphs(&p.control).await, vec![g_c], "only the account whose owner approved the record ingests it");
            assert!(cap_status(&p.control, p.a.account_id, p.a.owner_user_id).await.is_empty());
            cap::allow_record(&p.control, p.a.account_id, p.a.owner_user_id, R1, None).await.unwrap();
            crate::routes::google_push::run_meet_event(&state, &event).await.unwrap();
            let g_a = personal_graph(&p.control, p.a).await;
            let mut want = vec![g_a, g_c];
            want.sort();
            assert_eq!(meet_atom_graphs(&p.control).await, want, "each account runs its own opt-in fence");
            assert_eq!(meet.entries_calls.load(Ordering::SeqCst), 2, "one transcript fetch per approving account, no re-ingest");

            let cutoff: chrono::DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()").fetch_one(&p.control).await.unwrap();
            assert!(!crate::connectors::google_disconnect::revoke_chat_subscription_best_effort(&http, &p.control, p.a.account_id, p.a.owner_user_id, "tok", "meet", Some(cutoff)).await, "a disconnect leaves a subscription another account still references");
            assert_eq!(events.deletes.load(Ordering::SeqCst), 0);
            let purged = crate::connectors::google_disconnect::purge_google_connection(&p.control, &p.control, p.a.account_id, crate::connectors::google::Surface::Meet, crate::connectors::TokenOwner::Member(p.a.owner_user_id), cutoff, cutoff).await;
            assert_eq!(purged.watch_channels_deleted, 1);
            assert_eq!(resolve_subscription(&p.control, "subscriptions/shared").await.unwrap().len(), 1);
            assert!(crate::connectors::google_disconnect::revoke_chat_subscription_best_effort(&http, &p.control, c.account_id, c.owner_user_id, "tok", "meet", Some(cutoff)).await, "the last reference stops the subscription");
            assert_eq!(events.deletes.load(Ordering::SeqCst), 1);
        })).await;
        finish(&base, db, p.control).await;
    }

}
