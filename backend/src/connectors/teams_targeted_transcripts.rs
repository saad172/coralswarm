//! Explicit one-meeting delegated transcript pull. Never discovers calendar
//! events, enrolls a subscription, or falls back to another member/app token.
use chrono::{DateTime, Utc};
use reqwest::{Client, Response, Url};
use serde::Deserialize;
use serde_json::Value;

use super::{microsoft, NormalizedItem};

pub(crate) const GRAPH: &str = "https://graph.microsoft.com/v1.0";

/// Integration tests (feature `test-support`, never a production build) may point the
/// public routes at a local provider mock; the origin is never caller-selectable.
#[cfg(feature = "test-support")]
static TEST_GRAPH_ORIGIN: std::sync::RwLock<Option<String>> = std::sync::RwLock::new(None);

#[cfg(feature = "test-support")]
pub fn test_support_set_graph_origin(origin: Option<String>) {
    *TEST_GRAPH_ORIGIN.write().unwrap() = origin;
}

pub(crate) fn graph_origin() -> String {
    #[cfg(feature = "e2e")]
    if let Ok(base) = std::env::var("E2E_MICROSOFT_GRAPH_BASE") {
        return format!("{}/v1.0", base.trim_end_matches('/'));
    }
    #[cfg(feature = "test-support")]
    if let Some(origin) = TEST_GRAPH_ORIGIN.read().unwrap().clone() {
        return origin;
    }
    GRAPH.to_string()
}
/// Transcripts of a single or ad-hoc meeting are accepted up to this long after
/// its end (meetings overrun; Teams creates the transcript when it stops).
const TRANSCRIPT_AVAILABILITY: chrono::Duration = chrono::Duration::hours(24);
/// Overrun tolerated for one occurrence of a recurring series.
pub(crate) const SERIES_OVERRUN: chrono::Duration = chrono::Duration::hours(2);
/// How far past now the calendar is read when authorizing a recurring occurrence.
const SERIES_HORIZON: chrono::Duration = chrono::Duration::hours(4);
/// A recurring occurrence is accepted from this long before it starts.
const JOIN_EARLY: chrono::Duration = chrono::Duration::minutes(15);
/// Default length of an ad-hoc meeting whose own bounds are missing or too short.
const AD_HOC_MIN_DURATION: chrono::Duration = chrono::Duration::hours(3);
const MAX_OCCURRENCE: chrono::Duration = chrono::Duration::hours(24);

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Request {
    pub meeting_id: Option<String>,
    pub join_url: Option<String>,
    #[serde(default)]
    pub meeting_ticket: Option<String>,
    /// Teams pane only: the meeting chat, used to gate capture on recording.
    #[serde(default)]
    pub chat_id: Option<String>,
}

#[derive(Debug)]
pub(crate) enum Target {
    MeetingId(String),
    JoinUrl(String),
    JoinMeetingId(String),
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    #[error("provide exactly one Graph meeting ID or Teams meeting join URL")]
    InvalidTarget,
    #[error("Microsoft could not verify this meeting's identity and start time")]
    InvalidMetadata,
    #[error("Microsoft returned an incomplete transcript list")]
    IncompleteList,
    #[error("Microsoft rejected this meeting request ({status})")]
    Graph { status: u16, code: Option<String> },
    // Do not retain upstream URLs: a join URL may contain private meeting keys.
    #[error("Microsoft could not complete this meeting request")]
    Unavailable,
}

impl Request {
    pub(crate) fn target(self) -> Result<Target, Error> {
        if self.meeting_ticket.is_some() {
            return Err(Error::InvalidTarget);
        }
        match (self.meeting_id, self.join_url) {
            (Some(id), None) if valid_id(&id) => Ok(Target::MeetingId(id)),
            (None, Some(link)) => join_target(&link),
            _ => Err(Error::InvalidTarget),
        }
    }
}

pub(crate) fn join_target(link: &str) -> Result<Target, Error> {
    let url = Url::parse(link).map_err(|_| Error::InvalidTarget)?;
    if link.len() > 4096
        || url.scheme() != "https"
        || !matches!(
            url.host_str(),
            Some("teams.microsoft.com" | "teams.cloud.microsoft")
        )
        || !url.username().is_empty()
        || url.password().is_some()
        || url.port().is_some()
        || url.fragment().is_some()
    {
        return Err(Error::InvalidTarget);
    }
    let raw_path = link
        .split_once("://")
        .and_then(|(_, rest)| rest.find('/').map(|start| &rest[start..]))
        .map(|path| path.split('?').next().unwrap_or(path));
    if raw_path.is_some_and(|path| path.starts_with("/meet/")) && raw_path != Some(url.path()) {
        return Err(Error::InvalidTarget);
    }
    if let Some(id) = url.path().strip_prefix("/meet/") {
        // Check the supplied path too: URL parsing normalizes dot segments.
        // Only the numeric join ID is retained; passcodes never enter Graph.
        if id.is_empty()
            || id.len() > 64
            || !id.bytes().all(|b| b.is_ascii_digit())
            || raw_path != Some(url.path())
        {
            return Err(Error::InvalidTarget);
        }
        return Ok(Target::JoinMeetingId(id.to_owned()));
    }
    if url
        .path()
        .strip_prefix("/l/meetup-join/")
        .is_some_and(|tail| !tail.is_empty())
    {
        return Ok(Target::JoinUrl(link.to_owned()));
    }
    Err(Error::InvalidTarget)
}

fn valid_id(id: &str) -> bool {
    !id.is_empty()
        && !matches!(id, "." | "..")
        && id.len() <= 2048
        && !id.chars().any(|c| c.is_whitespace() || c.is_control())
}

#[derive(Clone, Deserialize)]
pub(crate) struct Meeting {
    pub(crate) id: String,
    pub(crate) subject: Option<String>,
    #[serde(rename = "startDateTime")]
    pub(crate) start: Option<String>,
    #[serde(rename = "endDateTime")]
    pub(crate) end: Option<String>,
    #[serde(rename = "joinWebUrl")]
    pub(crate) join_url: Option<String>,
    #[serde(rename = "creationDateTime", default)]
    creation: Option<String>,
    #[serde(rename = "joinMeetingIdSettings")]
    join_settings: Option<JoinMeetingIdSettings>,
    /// Set by `verify_occurrence` for an occurrence of a recurring series: the
    /// latest transcript creation time that can belong to it. `None` keeps the
    /// original window (start .. end + 24h).
    #[serde(skip)]
    pub(crate) transcript_end: Option<DateTime<Utc>>,
    /// Set by `verify_occurrence` when the occurrence is series-linked (any
    /// calendar row that is not a plain single instance). Auto-transcription is
    /// never changed for these: `recordAutomatically` is series-wide.
    #[serde(skip)]
    pub(crate) recurring: bool,
}

#[derive(Clone, Deserialize)]
struct JoinMeetingIdSettings {
    #[serde(rename = "joinMeetingId")]
    join_id: Option<String>,
}

pub(crate) struct Fetched {
    pub meeting_id: String,
    pub items: Vec<NormalizedItem>,
}

async fn accepted(response: Response) -> Result<Response, Error> {
    if response.status().is_success() {
        return Ok(response);
    }
    let status = response.status().as_u16();
    let body: Value = response.json().await.unwrap_or(Value::Null);
    let code = body
        .pointer("/error/innerError/code")
        .or_else(|| body.pointer("/error/code"))
        .and_then(Value::as_str)
        .filter(|code| code.len() <= 128)
        .map(str::to_owned);
    Err(Error::Graph { status, code })
}

async fn get_json(http: &Client, token: &str, url: &str) -> Result<Value, Error> {
    let response = microsoft::graph_send(|| http.get(url).bearer_auth(token))
        .await
        .map_err(|_| Error::Unavailable)?;
    accepted(response)
        .await?
        .json()
        .await
        .map_err(|_| Error::InvalidMetadata)
}

async fn content(http: &Client, token: &str, url: &str) -> Result<String, Error> {
    let response = microsoft::graph_send(|| {
        http.get(url)
            .query(&[("$format", "text/vtt")])
            .bearer_auth(token)
    })
    .await
    .map_err(|_| Error::Unavailable)?;
    let response = match accepted(response).await {
        Ok(response) => response,
        Err(Error::Graph {
            status: 403,
            code: Some(code),
        }) if code == "SpeakerAttributionNotAllowed" => {
            let response = microsoft::graph_send(|| {
                http.get(url)
                    .header(
                        reqwest::header::ACCEPT,
                        "application/vnd.microsoft.graph.transcript+text",
                    )
                    .bearer_auth(token)
            })
            .await
            .map_err(|_| Error::Unavailable)?;
            accepted(response).await?
        }
        Err(error) => return Err(error),
    };
    response.text().await.map_err(|_| Error::Unavailable)
}

pub(crate) async fn fetch(http: &Client, token: &str, target: Target) -> Result<Fetched, Error> {
    fetch_from(http, token, target, &graph_origin()).await
}

// The only caller-selected destination is a Graph resource identifier, never
// an HTTP origin. This base parameter exists solely for the local HTTP tests.
async fn fetch_from(
    http: &Client,
    token: &str,
    target: Target,
    graph: &str,
) -> Result<Fetched, Error> {
    let meeting = resolve_from(http, token, target, graph).await?;
    fetch_verified_from(http, token, meeting, graph).await
}
pub(crate) async fn resolve(http: &Client, token: &str, target: Target) -> Result<Meeting, Error> {
    resolve_from(http, token, target, &graph_origin()).await
}
/// [`resolve`] against an explicit Graph base (Outlook scheduling tests).
pub(crate) async fn resolve_at(
    http: &Client,
    token: &str,
    target: Target,
    graph: &str,
) -> Result<Meeting, Error> {
    resolve_from(http, token, target, graph).await
}
/// Resolve a target to its onlineMeeting through the caller's own delegated token.
async fn resolve_from(
    http: &Client,
    token: &str,
    target: Target,
    graph: &str,
) -> Result<Meeting, Error> {
    let meeting: Meeting = match target {
        Target::MeetingId(id) => {
            let value = get_json(
                http,
                token,
                &format!("{graph}/me/onlineMeetings/{}", urlencoding::encode(&id)),
            )
            .await?;
            let meeting: Meeting =
                serde_json::from_value(value).map_err(|_| Error::InvalidMetadata)?;
            if meeting.id != id {
                return Err(Error::InvalidMetadata);
            }
            meeting
        }
        Target::JoinUrl(link) => {
            let meeting = lookup_meeting(
                http,
                token,
                graph,
                &format!("joinWebUrl eq '{}'", link.replace('\'', "''")),
            )
            .await?;
            if meeting.join_url.as_deref() != Some(link.as_str()) {
                return Err(Error::InvalidMetadata);
            }
            meeting
        }
        Target::JoinMeetingId(id) => {
            let meeting = lookup_meeting(
                http,
                token,
                graph,
                &format!("joinMeetingIdSettings/joinMeetingId eq '{id}'"),
            )
            .await?;
            if meeting
                .join_settings
                .as_ref()
                .and_then(|settings| settings.join_id.as_deref())
                != Some(id.as_str())
            {
                return Err(Error::InvalidMetadata);
            }
            // Microsoft may return its canonical legacy URL for a modern link.
            // Bind through the verified numeric ID, never through a redirect.
            match meeting
                .join_url
                .as_deref()
                .and_then(|link| join_target(link).ok())
            {
                Some(Target::JoinUrl(_)) => (),
                Some(Target::JoinMeetingId(canonical_id)) if canonical_id == id => (),
                _ => return Err(Error::InvalidMetadata),
            }
            meeting
        }
    };
    if !valid_id(&meeting.id) {
        return Err(Error::InvalidMetadata);
    }
    // Verify time metadata as well as identity before a consent can be persisted.
    // Instant ("Meet now") meetings may omit start/end; their creation time then
    // anchors the occurrence in `verify_occurrence`. With neither, refuse.
    if parse_utc(&meeting.start).is_none() && parse_utc(&meeting.creation).is_none() {
        return Err(Error::InvalidMetadata);
    }
    Ok(meeting)
}
pub(crate) async fn fetch_verified(
    http: &Client,
    token: &str,
    meeting: Meeting,
) -> Result<Fetched, Error> {
    fetch_verified_from(http, token, meeting, &graph_origin()).await
}
/// Fetch the transcripts of a verified meeting, filtered to its authorized window.
async fn fetch_verified_from(
    http: &Client,
    token: &str,
    meeting: Meeting,
    graph: &str,
) -> Result<Fetched, Error> {
    let end = parse_utc(&meeting.end).ok_or(Error::InvalidMetadata)?;
    let timestamp: DateTime<Utc> = parse_utc(&meeting.start).ok_or(Error::InvalidMetadata)?;
    if end <= timestamp {
        return Err(Error::InvalidMetadata);
    }
    let meeting_url = format!(
        "{graph}/me/onlineMeetings/{}",
        urlencoding::encode(&meeting.id)
    );
    let value = get_json(http, token, &format!("{meeting_url}/transcripts")).await?;
    if value.get("@odata.nextLink").is_some() {
        return Err(Error::IncompleteList);
    }
    let transcripts = value
        .get("value")
        .and_then(Value::as_array)
        .ok_or(Error::InvalidMetadata)?;
    let mut items = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for transcript in transcripts {
        let id = transcript
            .get("id")
            .and_then(Value::as_str)
            .filter(|id| valid_id(id))
            .ok_or(Error::InvalidMetadata)?;
        if !seen.insert(id.to_owned()) {
            continue;
        }
        let created = transcript
            .get("createdDateTime")
            .and_then(Value::as_str)
            .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
            .map(|d| d.with_timezone(&Utc))
            .ok_or(Error::InvalidMetadata)?;
        // A recurring series shares one meeting ID across occurrences, so its
        // occurrence carries an explicit upper bound that cannot reach the next
        // one. Everything else keeps the original end + 24h window.
        let upper = meeting
            .transcript_end
            .unwrap_or(end + TRANSCRIPT_AVAILABILITY);
        if created < timestamp || created > upper {
            continue;
        }
        let body = content(
            http,
            token,
            &format!(
                "{meeting_url}/transcripts/{}/content",
                urlencoding::encode(id)
            ),
        )
        .await?;
        let text = microsoft::vtt_to_text(&body);
        if text.trim().is_empty() {
            continue;
        }
        items.push(NormalizedItem {
            source_type: "teams_transcript".into(),
            source_id: format!("{}:{id}", meeting.id),
            content: format!(
                "{}\n\n{text}",
                meeting.subject.as_deref().unwrap_or("Teams meeting")
            ),
            participants: Vec::new(),
            timestamp,
            scope: None,
            thread_id: Some(meeting.id.clone()),
            channel_id: None,
            author_id: None,
            author_name: None,
        });
    }
    // Return only after the complete target has succeeded, so callers cannot
    // ingest partial results when a later transcript is forbidden/unavailable.
    Ok(Fetched {
        meeting_id: meeting.id,
        items,
    })
}

async fn lookup_meeting(
    http: &Client,
    token: &str,
    graph: &str,
    filter: &str,
) -> Result<Meeting, Error> {
    let mut url =
        Url::parse(&format!("{graph}/me/onlineMeetings")).map_err(|_| Error::Unavailable)?;
    url.query_pairs_mut().append_pair("$filter", filter);
    let value = get_json(http, token, url.as_str()).await?;
    let values = value
        .get("value")
        .and_then(Value::as_array)
        .ok_or(Error::InvalidMetadata)?;
    if values.is_empty() {
        return Err(Error::Graph {
            status: 404,
            code: None,
        });
    }
    if values.len() != 1 || value.get("@odata.nextLink").is_some() {
        return Err(Error::InvalidMetadata);
    }
    serde_json::from_value(values[0].clone()).map_err(|_| Error::InvalidMetadata)
}

/// Teams "recording/transcription is running" gate, read from the meeting
/// chat's system messages (`GET /me/chats/{chatId}/messages`, delegated
/// `Chat.Read`). Teams posts `eventDetail` system messages carrying the
/// session `callId`; see [`transcription_active`] for the rule.
const CHAT_EVENT_TOP: u32 = 50;
/// Page cap (250 messages) bounding latency on a very chatty meeting chat.
const CHAT_EVENT_MAX_PAGES: usize = 5;

fn event_detail(message: &Value) -> Option<(&str, Option<&str>, &Value)> {
    let detail = message.get("eventDetail")?;
    let kind = detail.get("@odata.type")?.as_str()?;
    let call = detail.get("callId").and_then(Value::as_str);
    Some((kind.trim_start_matches("#microsoft.graph."), call, detail))
}

/// Pure rule over the `value` array of a chat-messages response (newest first).
/// The current session is the newest `callStarted` with no later `callEnded`.
/// Recording/transcription is active iff, for that session's `callId`, the
/// newest of {recording `initial`, transcript started} is newer than the newest
/// of {recording `chunkFinished`/`success`}. No current session -> false.
pub(crate) fn transcription_active(messages: &[Value]) -> bool {
    // Oldest-first; a stable sort on createdDateTime keeps list order as the
    // tie-break (the API lists newest first, so reverse first).
    let mut ordered: Vec<(Option<DateTime<Utc>>, &Value)> = messages
        .iter()
        .rev()
        .map(|m| {
            let at = m
                .get("createdDateTime")
                .and_then(Value::as_str)
                .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
                .map(|d| d.with_timezone(&Utc));
            (at, m)
        })
        .collect();
    if ordered.iter().all(|(at, _)| at.is_some()) {
        ordered.sort_by_key(|(at, _)| *at);
    }
    let mut call: Option<&str> = None;
    let mut active = false;
    for (_, message) in ordered {
        let Some((kind, id, detail)) = event_detail(message) else {
            continue;
        };
        let Some(id) = id else { continue };
        match kind {
            "callStartedEventMessageDetail" => {
                call = Some(id);
                active = false;
            }
            "callEndedEventMessageDetail" if call == Some(id) => {
                call = None;
                active = false;
            }
            "callTranscriptEventMessageDetail" if call == Some(id) => active = true,
            "callRecordingEventMessageDetail" if call == Some(id) => {
                match detail.get("callRecordingStatus").and_then(Value::as_str) {
                    Some("initial") => active = true,
                    Some("chunkFinished" | "success") => active = false,
                    _ => {}
                }
            }
            _ => {}
        }
    }
    call.is_some() && active
}

/// Read the meeting chat's newest system messages with the caller's own token.
pub(crate) async fn recording_active(
    http: &Client,
    token: &str,
    chat_id: &str,
) -> Result<bool, Error> {
    recording_active_from(http, token, chat_id, &graph_origin()).await
}

async fn recording_active_from(
    http: &Client,
    token: &str,
    chat_id: &str,
    graph: &str,
) -> Result<bool, Error> {
    let mut url = format!(
        "{graph}/me/chats/{}/messages?$top={CHAT_EVENT_TOP}",
        urlencoding::encode(chat_id)
    );
    let mut messages: Vec<Value> = Vec::new();
    for _ in 0..CHAT_EVENT_MAX_PAGES {
        let page = get_json(http, token, &url).await?;
        messages.extend(
            page.get("value")
                .and_then(Value::as_array)
                .ok_or(Error::InvalidMetadata)?
                .iter()
                .cloned(),
        );
        // Newest first: the first start/end event decides the session, so stop there.
        if session_boundary_seen(&messages) {
            return Ok(transcription_active(&messages));
        }
        match page.get("@odata.nextLink").and_then(Value::as_str) {
            // Never send the bearer token anywhere but Graph.
            Some(next) if next.starts_with(graph) => url = next.to_string(),
            // Start of chat without any call event: no session.
            _ => return Ok(false),
        }
    }
    // Page cap hit without finding callStarted/callEnded: fail closed.
    Ok(false)
}

/// True once the (newest-first) list contains a callStarted or callEnded event.
fn session_boundary_seen(messages: &[Value]) -> bool {
    messages.iter().any(|m| {
        matches!(
            event_detail(m).map(|(k, _, _)| k),
            Some("callStartedEventMessageDetail" | "callEndedEventMessageDetail")
        )
    })
}

#[cfg(test)]
mod targeted_transcript_tests {
    use super::*;
    use axum::{
        body::Body,
        extract::State,
        http::{Request as HttpRequest, StatusCode},
        response::IntoResponse,
        Json, Router,
    };
    use serde_json::json;
    use std::sync::{Arc, Mutex};

    const LINK: &str = "https://teams.microsoft.com/l/meetup-join/fixture?context=test";
    const JOIN_ID: &str = "220909967122015";
    const SHORT_LINK: &str = "https://teams.microsoft.com/meet/220909967122015?p=fixture";
    #[derive(Clone, Copy)]
    enum Mode {
        Normal,
        Denied(u16),
        MissingStart,
        MissingTranscriptCreation,
        DelayedTranscript,
        ExpiredTranscript,
        OverrunTranscript,
        NextOccurrenceTranscript,
        BeforeStartTranscript,
        LateDenied,
        Unattributed,
        MismatchedJoin,
        MultipleMeetings,
        PaginatedMeetings,
        PaginatedTranscripts,
        DisabledTranscripts,
        MismatchedJoinId,
        MissingJoinId,
        MissingJoinIdField,
        UntrustedCanonicalJoin,
        MismatchedCanonicalShortId,
    }
    struct Graph {
        mode: Mode,
        requests: Mutex<Vec<(String, String, String)>>,
    }
    struct Server {
        graph: Arc<Graph>,
        base: String,
        task: tokio::task::JoinHandle<()>,
    }
    impl Drop for Server {
        fn drop(&mut self) {
            self.task.abort();
        }
    }
    impl Server {
        async fn new(mode: Mode) -> Self {
            let graph = Arc::new(Graph {
                mode,
                requests: Mutex::new(Vec::new()),
            });
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let base = format!("http://{}/v1.0", listener.local_addr().unwrap());
            let app = Router::new().fallback(reply).with_state(graph.clone());
            let task = tokio::spawn(async move {
                axum::serve(listener, app).await.unwrap();
            });
            Self { graph, base, task }
        }
        fn requests(&self) -> Vec<(String, String, String)> {
            self.graph.requests.lock().unwrap().clone()
        }
    }
    fn metadata(mode: Mode) -> Value {
        let mut value = json!({"id":"M/one", "subject":"Isolated fixture", "joinWebUrl":LINK,
            "joinMeetingIdSettings":{"joinMeetingId":JOIN_ID},
            "endDateTime":"2026-09-30T21:00:00Z",
            "startDateTime":if matches!(mode, Mode::MissingStart) { Value::Null } else { json!("2026-09-30T20:00:00Z") }});
        match mode {
            Mode::MismatchedJoinId => {
                value["joinMeetingIdSettings"]["joinMeetingId"] = json!("999999999999999")
            }
            Mode::MissingJoinId => value["joinMeetingIdSettings"] = Value::Null,
            Mode::MissingJoinIdField => value["joinMeetingIdSettings"] = json!({}),
            Mode::UntrustedCanonicalJoin => {
                value["joinWebUrl"] = json!("https://evil.example/l/meetup-join/fixture")
            }
            Mode::MismatchedCanonicalShortId => {
                value["joinWebUrl"] = json!("https://teams.microsoft.com/meet/999999999999999")
            }
            _ => (),
        }
        value
    }
    async fn reply(
        State(graph): State<Arc<Graph>>,
        request: HttpRequest<Body>,
    ) -> axum::response::Response {
        let path = request.uri().path();
        graph.requests.lock().unwrap().push((
            request.uri().to_string(),
            request
                .headers()
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .to_owned(),
            request
                .headers()
                .get("accept")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .to_owned(),
        ));
        if let Mode::Denied(status) = graph.mode {
            return (
                StatusCode::from_u16(status).unwrap(),
                Json(json!({"error":{"code":"Denied"}})),
            )
                .into_response();
        }
        if path == "/v1.0/me/onlineMeetings" {
            let value = match graph.mode {
                Mode::MismatchedJoin => {
                    let mut m = metadata(graph.mode);
                    m["joinWebUrl"] = json!("https://teams.microsoft.com/meet/other");
                    json!({"value":[m]})
                }
                Mode::MultipleMeetings => {
                    json!({"value":[metadata(graph.mode),metadata(graph.mode)]})
                }
                Mode::PaginatedMeetings => {
                    json!({"value":[metadata(graph.mode)],"@odata.nextLink":"https://other.example/not-followed"})
                }
                _ => json!({"value":[metadata(graph.mode)]}),
            };
            return Json(value).into_response();
        }
        if path == "/v1.0/me/onlineMeetings/M%2Fone" {
            return Json(metadata(graph.mode)).into_response();
        }
        if path.ends_with("/transcripts") {
            let created = match graph.mode {
                Mode::MissingTranscriptCreation => Some(Value::Null),
                // Meeting is 20:00-21:00 on 09-30. Teams creates the transcript
                // when transcription stops, so overrunning meetings are common.
                Mode::DelayedTranscript => Some(json!("2026-10-01T20:59:00Z")),
                Mode::ExpiredTranscript => Some(json!("2026-10-01T21:00:01Z")),
                Mode::OverrunTranscript => Some(json!("2026-09-30T22:30:00Z")),
                // 20:00 the next day: another occurrence IF the meeting is a series.
                Mode::NextOccurrenceTranscript => Some(json!("2026-10-01T20:00:00Z")),
                Mode::BeforeStartTranscript => Some(json!("2026-09-30T19:49:59Z")),
                _ => None,
            };
            if let Some(created) = created {
                return Json(json!({"value":[{"id":"t1","createdDateTime":created}]}))
                    .into_response();
            }
            if matches!(graph.mode, Mode::PaginatedTranscripts) {
                return Json(json!({"value":[{"id":"t1","createdDateTime":"2026-09-30T20:30:00Z"}],"@odata.nextLink":"https://other.example/not-followed"})).into_response();
            }
            return Json(if matches!(graph.mode, Mode::LateDenied) {
                json!({"value":[{"id":"t1","createdDateTime":"2026-09-30T20:30:00Z"},{"id":"t2","createdDateTime":"2026-09-30T20:31:00Z"}]})
            } else {
                json!({"value":[{"id":"t1","createdDateTime":"2026-09-30T20:30:00Z"},{"id":"t1","createdDateTime":"2026-09-30T20:30:00Z"}]})
            })
            .into_response();
        }
        if path.ends_with("/transcripts/t2/content") {
            return (
                StatusCode::FORBIDDEN,
                Json(json!({"error":{"code":"Denied"}})),
            )
                .into_response();
        }
        if path.ends_with("/transcripts/t1/content") {
            if matches!(graph.mode, Mode::DisabledTranscripts) {
                return (
                    StatusCode::FORBIDDEN,
                    Json(json!({"error":{"code":"GraphAccessToTranscriptsDisabled"}})),
                )
                    .into_response();
            }
            if matches!(graph.mode, Mode::Unattributed) && request.uri().query().is_some() {
                return (
                    StatusCode::FORBIDDEN,
                    Json(json!({"error":{"innerError":{"code":"SpeakerAttributionNotAllowed"}}})),
                )
                    .into_response();
            }
            return "WEBVTT\n\n1\n00:00:00.000 --> 00:00:02.000\nHarmless transcript fixture marker.\n".into_response();
        }
        (StatusCode::NOT_FOUND, "unexpected Graph resource").into_response()
    }
    async fn run(server: &Server, target: Target) -> Result<Fetched, Error> {
        fetch_from(
            &Client::new(),
            "own-member-fixture-token",
            target,
            &server.base,
        )
        .await
    }
    fn assert_own_target_only(server: &Server) {
        for (url, auth, _) in server.requests() {
            assert!(
                url.starts_with("/v1.0/me/onlineMeetings"),
                "unexpected discovery: {url}"
            );
            assert!(!url.contains("/events") && !url.contains("/subscriptions"));
            assert_eq!(auth, "Bearer own-member-fixture-token");
        }
    }

    // ---- recording gate ----
    fn ev(at: &str, kind: &str, call: &str, status: Option<&str>) -> Value {
        let mut detail = json!({"@odata.type": format!("#microsoft.graph.{kind}"), "callId": call});
        if let Some(st) = status {
            detail["callRecordingStatus"] = json!(st);
        }
        json!({"id": at, "messageType": "unknownFutureValue", "createdDateTime": at, "eventDetail": detail})
    }
    const START: &str = "callStartedEventMessageDetail";
    const END: &str = "callEndedEventMessageDetail";
    const REC: &str = "callRecordingEventMessageDetail";
    const TRANS: &str = "callTranscriptEventMessageDetail";
    /// Graph lists newest first; fixtures are written oldest first for readability.
    fn newest_first(mut v: Vec<Value>) -> Vec<Value> {
        v.reverse();
        v
    }

    #[test]
    fn gate_no_session_is_inactive() {
        assert!(!transcription_active(&[]));
        let chatter = json!({"createdDateTime":"2026-10-03T10:00:00Z","messageType":"message","eventDetail":null});
        assert!(!transcription_active(&[chatter]));
        // recording events with no callStarted in the window
        assert!(!transcription_active(&newest_first(vec![ev("2026-10-03T10:00:00Z", REC, "c1", Some("initial"))])));
    }

    #[test]
    fn gate_started_without_recording_is_inactive() {
        assert!(!transcription_active(&newest_first(vec![ev("2026-10-03T10:00:00Z", START, "c1", None)])));
    }

    #[test]
    fn gate_transcript_started_is_active() {
        let m = newest_first(vec![
            ev("2026-10-03T10:00:00Z", START, "c1", None),
            ev("2026-10-03T10:01:00Z", REC, "c1", Some("initial")),
            ev("2026-10-03T10:01:01Z", TRANS, "c1", None),
        ]);
        assert!(transcription_active(&m));
        let transcript_only = newest_first(vec![
            ev("2026-10-03T10:00:00Z", START, "c1", None),
            ev("2026-10-03T10:01:01Z", TRANS, "c1", None),
        ]);
        assert!(transcription_active(&transcript_only));
    }

    #[test]
    fn gate_recording_initial_is_active() {
        let m = newest_first(vec![
            ev("2026-10-03T10:00:00Z", START, "c1", None),
            ev("2026-10-03T10:01:00Z", REC, "c1", Some("initial")),
        ]);
        assert!(transcription_active(&m));
    }

    #[test]
    fn gate_chunk_finished_after_initial_is_inactive_and_restart_reactivates() {
        let mut v = vec![
            ev("2026-10-03T10:00:00Z", START, "c1", None),
            ev("2026-10-03T10:01:00Z", REC, "c1", Some("initial")),
            ev("2026-10-03T10:01:01Z", TRANS, "c1", None),
            ev("2026-10-03T10:05:00Z", REC, "c1", Some("chunkFinished")),
        ];
        assert!(!transcription_active(&newest_first(v.clone())));
        v.push(ev("2026-10-03T10:06:00Z", REC, "c1", Some("initial")));
        assert!(transcription_active(&newest_first(v)));
    }

    #[test]
    fn gate_previous_session_recording_does_not_leak_into_new_session() {
        let m = newest_first(vec![
            ev("2026-10-03T09:00:00Z", START, "old", None),
            ev("2026-10-03T09:01:00Z", REC, "old", Some("initial")),
            ev("2026-10-03T09:30:00Z", END, "old", None),
            ev("2026-10-03T10:00:00Z", START, "new", None),
        ]);
        assert!(!transcription_active(&m));
        // a late "success" for the finished session must not affect the new one either
        let mut m2 = m.clone();
        m2.insert(0, ev("2026-10-03T10:00:30Z", REC, "old", Some("success")));
        assert!(!transcription_active(&m2));
        // and a recording for the new session turns it on
        m2.insert(0, ev("2026-10-03T10:02:00Z", REC, "new", Some("initial")));
        assert!(transcription_active(&m2));
    }

    #[test]
    fn gate_call_ended_is_inactive() {
        let m = newest_first(vec![
            ev("2026-10-03T10:00:00Z", START, "c1", None),
            ev("2026-10-03T10:01:00Z", REC, "c1", Some("initial")),
            ev("2026-10-03T10:30:00Z", END, "c1", None),
        ]);
        assert!(!transcription_active(&m));
    }

    #[tokio::test]
    async fn gate_reads_chat_messages_with_the_callers_token() {
        let seen: Arc<Mutex<Vec<(String, String)>>> = Arc::default();
        let body = json!({"value": newest_first(vec![
            ev("2026-10-03T10:00:00Z", START, "c1", None),
            ev("2026-10-03T10:01:00Z", REC, "c1", Some("initial")),
        ])});
        async fn reply(
            State((seen, body, fail)): State<(Arc<Mutex<Vec<(String, String)>>>, Value, bool)>,
            request: HttpRequest<Body>,
        ) -> axum::response::Response {
            let auth = request.headers().get("authorization").and_then(|v| v.to_str().ok()).unwrap_or("").to_string();
            seen.lock().unwrap().push((request.uri().to_string(), auth));
            if fail {
                return (StatusCode::FORBIDDEN, Json(json!({"error":{"code":"Forbidden"}}))).into_response();
            }
            Json(body).into_response()
        }
        for fail in [false, true] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let base = format!("http://{}/v1.0", listener.local_addr().unwrap());
            let app = Router::new().fallback(reply).with_state((seen.clone(), body.clone(), fail));
            let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
            let out = recording_active_from(&Client::new(), "tok", "19:abc@thread.v2", &base).await;
            task.abort();
            if fail {
                assert!(out.is_err());
            } else {
                assert!(out.unwrap());
            }
        }
        let seen = seen.lock().unwrap();
        assert!(seen[0].0.starts_with("/v1.0/me/chats/19%3Aabc%40thread.v2/messages?%24top=50")
            || seen[0].0.starts_with("/v1.0/me/chats/19%3Aabc%40thread.v2/messages?$top=50"), "{}", seen[0].0);
        assert_eq!(seen[0].1, "Bearer tok");
    }

    #[tokio::test]
    async fn gate_follows_next_link_until_call_started_and_fails_closed_at_the_cap() {
        async fn serve(pages: Vec<Value>) -> (String, tokio::task::JoinHandle<()>, Arc<Mutex<usize>>) {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let base = format!("http://{}/v1.0", listener.local_addr().unwrap());
            let hits = Arc::new(Mutex::new(0usize));
            let app = Router::new()
                .fallback(
                    |State((pages, hits, base)): State<(Vec<Value>, Arc<Mutex<usize>>, String)>,
                     request: HttpRequest<Body>| async move {
                        *hits.lock().unwrap() += 1;
                        let n: usize = request
                            .uri()
                            .query()
                            .and_then(|q| q.split('&').find_map(|p| p.strip_prefix("page=")))
                            .and_then(|v| v.parse().ok())
                            .unwrap_or(0);
                        let mut page = pages[n.min(pages.len() - 1)].clone();
                        if n + 1 < pages.len() {
                            page["@odata.nextLink"] = json!(format!("{base}/me/chats/c/messages?page={}", n + 1));
                        }
                        Json(page)
                    },
                )
                .with_state((pages, hits.clone(), base.clone()));
            (base, tokio::spawn(async move { axum::serve(listener, app).await.unwrap() }), hits)
        }
        let chatter = |i: usize| json!({"createdDateTime": format!("2026-10-03T10:{:02}:00Z", 30 + i), "messageType":"message"});
        // page 1 (newest): chatter + recording initial; page 2: callStarted.
        let p1 = json!({"value": [chatter(2), ev("2026-10-03T10:02:00Z", REC, "c1", Some("initial")), chatter(1)]});
        let p2 = json!({"value": [chatter(0), ev("2026-10-03T10:00:00Z", START, "c1", None)]});
        let (base, task, hits) = serve(vec![p1, p2]).await;
        assert!(recording_active_from(&Client::new(), "tok", "c", &base).await.unwrap());
        assert_eq!(*hits.lock().unwrap(), 2);
        task.abort();
        // start of chat with no call event at all: inactive after one page.
        let (base, task, _) = serve(vec![json!({"value":[chatter(0)]})]).await;
        assert!(!recording_active_from(&Client::new(), "tok", "c", &base).await.unwrap());
        task.abort();
        // endless chatter: capped at 5 pages, fails closed.
        let pages: Vec<Value> = (0..9).map(|_| json!({"value":[chatter(0)]})).collect();
        let (base, task, hits) = serve(pages).await;
        assert!(!recording_active_from(&Client::new(), "tok", "c", &base).await.unwrap());
        assert_eq!(*hits.lock().unwrap(), CHAT_EVENT_MAX_PAGES);
        task.abort();
    }

    #[test]
    fn malformed_or_conflicting_targets_are_rejected_before_graph() {
        for body in [
            json!({}),
            json!({"meeting_id":"", "join_url":LINK}),
            json!({"meeting_id":"M/one","join_url":LINK}),
            json!({"meeting_id":"bad\nid"}),
            json!({"meeting_id":"."}),
            json!({"meeting_id":".."}),
            json!({"join_url":"https://teams.microsoft.com.evil.example/l/meetup-join/a"}),
            json!({"join_url":"https://teams.microsoft.com@evil.example/l/meetup-join/a"}),
            json!({"join_url":"http://teams.microsoft.com/l/meetup-join/a"}),
            json!({"join_url":"https://teams.microsoft.com:444/l/meetup-join/a"}),
            json!({"join_url":"https://teams.microsoft.com/anything"}),
        ] {
            assert!(serde_json::from_value::<Request>(body)
                .unwrap()
                .target()
                .is_err());
        }
        assert!(serde_json::from_value::<Request>(
            json!({"meeting_id":"M/one","graph_id":"other-member"})
        )
        .is_err());
    }

    #[tokio::test]
    async fn metadata_resolution_cannot_read_a_transcript_or_body() {
        let server = Server::new(Mode::Normal).await;
        let meeting = resolve_from(
            &Client::new(),
            "own-member-fixture-token",
            Target::MeetingId("M/one".into()),
            &server.base,
        )
        .await
        .unwrap();
        assert_eq!(meeting.id, "M/one");
        assert_eq!(server.requests().len(), 1);
        assert!(!server.requests()[0].0.contains("/transcripts"));
    }

    #[tokio::test]
    async fn artifact_availability_window_is_finite_and_original_time_is_required() {
        let delayed = Server::new(Mode::DelayedTranscript).await;
        assert_eq!(
            run(&delayed, Target::MeetingId("M/one".into()))
                .await
                .unwrap()
                .items
                .len(),
            1
        );
        assert!(delayed.requests().iter().any(|r| r.0.contains("/content")));
        for mode in [
            Mode::MissingTranscriptCreation,
            Mode::ExpiredTranscript,
            Mode::BeforeStartTranscript,
        ] {
            let server = Server::new(mode).await;
            let result = run(&server, Target::MeetingId("M/one".into())).await;
            assert!(result.is_err() || result.unwrap().items.is_empty());
            assert!(!server.requests().iter().any(|r| r.0.contains("/content")));
        }
    }

    async fn fetch_with(server: &Server, transcript_end: Option<&str>) -> Fetched {
        let meeting = resolve_from(
            &Client::new(),
            "own-member-fixture-token",
            Target::MeetingId("M/one".into()),
            &server.base,
        )
        .await
        .unwrap();
        let mut meeting = meeting;
        meeting.transcript_end = transcript_end.map(|t| {
            DateTime::parse_from_rfc3339(t)
                .unwrap()
                .with_timezone(&Utc)
        });
        fetch_verified_from(&Client::new(), "own-member-fixture-token", meeting, &server.base)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn overrunning_single_or_ad_hoc_meeting_keeps_its_transcript() {
        // Scheduled 20:00-21:00, transcript created 22:30 (end + 90 min): a
        // single-instance or ad-hoc occurrence (no series bound) captures it.
        let server = Server::new(Mode::OverrunTranscript).await;
        assert_eq!(fetch_with(&server, None).await.items.len(), 1);
        // Even a day later, as before this change (end + 24h).
        let late = Server::new(Mode::DelayedTranscript).await;
        assert_eq!(fetch_with(&late, None).await.items.len(), 1);
    }

    #[tokio::test]
    async fn recurring_occurrence_never_captures_another_occurrences_transcript() {
        // Same overrun transcript, but this occurrence of a series is bounded by
        // its next sibling (21:30): the overrun up to there is kept...
        let overrun = Server::new(Mode::OverrunTranscript).await;
        assert_eq!(
            fetch_with(&overrun, Some("2026-09-30T23:00:00Z")).await.items.len(),
            1
        );
        // ...a transcript at/after the next occurrence is not captured, nor one
        // from the previous occurrence (before this one started).
        let next = Server::new(Mode::NextOccurrenceTranscript).await;
        let fetched = fetch_with(&next, Some("2026-09-30T23:00:00Z")).await;
        assert!(fetched.items.is_empty());
        assert!(!next.requests().iter().any(|r| r.0.contains("/content")));
        let early = Server::new(Mode::BeforeStartTranscript).await;
        assert!(fetch_with(&early, Some("2026-09-30T23:00:00Z")).await.items.is_empty());
        let over = Server::new(Mode::OverrunTranscript).await;
        assert!(fetch_with(&over, Some("2026-09-30T22:00:00Z")).await.items.is_empty());
    }

    #[tokio::test]
    async fn one_meeting_uses_member_token_actual_time_and_stable_source_ids() {
        let server = Server::new(Mode::Normal).await;
        let fetched = run(&server, Target::MeetingId("M/one".into()))
            .await
            .unwrap();
        assert_eq!(fetched.meeting_id, "M/one");
        assert_eq!(
            fetched.items.len(),
            1,
            "duplicate transcript ID must not produce duplicate content requests"
        );
        assert_eq!(fetched.items[0].source_id, "M/one:t1");
        assert_eq!(fetched.items[0].thread_id.as_deref(), Some("M/one"));
        assert_eq!(
            fetched.items[0].timestamp.to_rfc3339(),
            "2026-09-30T20:00:00+00:00"
        );
        assert!(fetched.items[0]
            .content
            .contains("Harmless transcript fixture marker."));
        assert_eq!(server.requests().len(), 3);
        assert_own_target_only(&server);
        let repeated = run(&server, Target::MeetingId("M/one".into()))
            .await
            .unwrap();
        assert_eq!(repeated.items[0].source_id, fetched.items[0].source_id);
    }

    #[tokio::test]
    async fn join_link_resolves_only_its_exact_meeting_without_calendar_sweep() {
        let server = Server::new(Mode::Normal).await;
        let fetched = run(
            &server,
            Request {
                meeting_ticket: None,
                chat_id: None,
                meeting_id: None,
                join_url: Some(LINK.into()),
            }
            .target()
            .unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(fetched.meeting_id, "M/one");
        let uri = Url::parse(&format!("http://localhost{}", server.requests()[0].0)).unwrap();
        assert_eq!(
            uri.query_pairs()
                .find(|(key, _)| key == "$filter")
                .unwrap()
                .1,
            format!("joinWebUrl eq '{LINK}'")
        );
        assert_own_target_only(&server);
    }

    #[tokio::test]
    async fn modern_short_link_binds_numeric_join_id_to_canonical_meeting() {
        let server = Server::new(Mode::Normal).await;
        let target = Request {
            meeting_ticket: None,
            chat_id: None,
            meeting_id: None,
            join_url: Some(SHORT_LINK.into()),
        }
        .target()
        .unwrap();
        let fetched = run(&server, target).await.unwrap();
        assert_eq!(fetched.meeting_id, "M/one");
        assert_eq!(fetched.items[0].source_id, "M/one:t1");
        let uri = Url::parse(&format!("http://localhost{}", server.requests()[0].0)).unwrap();
        assert_eq!(
            uri.query_pairs()
                .find(|(key, _)| key == "$filter")
                .unwrap()
                .1,
            format!("joinMeetingIdSettings/joinMeetingId eq '{JOIN_ID}'")
        );
        assert!(
            !server.requests()[0].0.contains("fixture"),
            "private short-link passcode must not enter Graph lookup"
        );
        assert_own_target_only(&server);
        let canonical = run(&server, Target::JoinUrl(LINK.into())).await.unwrap();
        assert_eq!(canonical.items[0].source_id, fetched.items[0].source_id);
    }

    #[test]
    fn malformed_modern_links_cannot_become_graph_filters() {
        for link in [
            "https://teams.microsoft.com/meet/",
            "https://teams.microsoft.com/meet/not-a-number",
            "https://teams.microsoft.com/meet/123/extra",
            "https://teams.microsoft.com/meet/123/../220909967122015",
            "https://teams.microsoft.com/meet/../l/meetup-join/fixture",
            "https://teams.microsoft.com/meet/%2e%2e/meet/220909967122015",
            "https://teams.microsoft.com/meet/%3220909967122015",
            "https://teams.microsoft.com/meet/123'or'1'eq'1",
        ] {
            assert!(
                matches!(
                    Request {
                        meeting_ticket: None,
                        chat_id: None,
                        meeting_id: None,
                        join_url: Some(link.into())
                    }
                    .target(),
                    Err(Error::InvalidTarget)
                ),
                "accepted malformed modern link: {link}"
            );
        }
    }

    #[tokio::test]
    async fn modern_join_id_and_canonical_binding_fail_closed_before_transcripts() {
        for mode in [
            Mode::MismatchedJoinId,
            Mode::MissingJoinId,
            Mode::MissingJoinIdField,
            Mode::UntrustedCanonicalJoin,
            Mode::MismatchedCanonicalShortId,
            Mode::MissingStart,
        ] {
            let server = Server::new(mode).await;
            let target = Request {
                meeting_ticket: None,
                chat_id: None,
                meeting_id: None,
                join_url: Some(SHORT_LINK.into()),
            }
            .target()
            .unwrap();
            assert!(matches!(
                run(&server, target).await,
                Err(Error::InvalidMetadata)
            ));
            assert_eq!(
                server.requests().len(),
                1,
                "invalid identity must not fetch transcripts"
            );
            assert_own_target_only(&server);
        }
    }

    #[tokio::test]
    async fn modern_lookup_denied_never_uses_join_url_or_calendar_fallback() {
        for status in [403, 404] {
            let server = Server::new(Mode::Denied(status)).await;
            let target = Request {
                meeting_ticket: None,
                chat_id: None,
                meeting_id: None,
                join_url: Some(SHORT_LINK.into()),
            }
            .target()
            .unwrap();
            assert!(matches!(run(&server, target).await,
                Err(Error::Graph { status: actual, .. }) if actual == status));
            assert_eq!(server.requests().len(), 1);
            assert_own_target_only(&server);
        }
    }

    #[tokio::test]
    async fn ambiguous_or_different_join_resolution_never_fetches_transcripts() {
        for mode in [
            Mode::MismatchedJoin,
            Mode::MultipleMeetings,
            Mode::PaginatedMeetings,
        ] {
            let server = Server::new(mode).await;
            assert!(matches!(
                run(&server, Target::JoinUrl(LINK.into())).await,
                Err(Error::InvalidMetadata)
            ));
            assert_eq!(server.requests().len(), 1);
        }
    }

    #[tokio::test]
    async fn incomplete_lists_and_disabled_access_never_follow_or_fall_back() {
        let server = Server::new(Mode::PaginatedTranscripts).await;
        assert!(matches!(
            run(&server, Target::MeetingId("M/one".into())).await,
            Err(Error::IncompleteList)
        ));
        assert_eq!(server.requests().len(), 2);
        let server = Server::new(Mode::DisabledTranscripts).await;
        assert!(
            matches!(run(&server, Target::MeetingId("M/one".into())).await,
            Err(Error::Graph { status:403, code:Some(code) }) if code=="GraphAccessToTranscriptsDisabled")
        );
        assert_eq!(
            server.requests().len(),
            3,
            "disabled access must not use speaker fallback"
        );
    }

    #[tokio::test]
    async fn forbidden_and_missing_meetings_never_fall_back_or_return_items() {
        for status in [403, 404] {
            let server = Server::new(Mode::Denied(status)).await;
            assert!(
                matches!(run(&server, Target::MeetingId("M/one".into())).await,
                Err(Error::Graph { status: actual, .. }) if actual==status)
            );
            assert_eq!(server.requests().len(), 1);
            assert_own_target_only(&server);
        }
    }

    #[tokio::test]
    async fn missing_event_time_refuses_content_fetch_and_ingest_time_fallback() {
        let server = Server::new(Mode::MissingStart).await;
        assert!(matches!(
            run(&server, Target::MeetingId("M/one".into())).await,
            Err(Error::InvalidMetadata)
        ));
        assert_eq!(server.requests().len(), 1);
    }

    #[tokio::test]
    async fn later_forbidden_transcript_discards_entire_target_batch() {
        let server = Server::new(Mode::LateDenied).await;
        assert!(matches!(
            run(&server, Target::MeetingId("M/one".into())).await,
            Err(Error::Graph { status: 403, .. })
        ));
        assert_eq!(server.requests().len(), 4);
        assert_own_target_only(&server);
    }

    #[tokio::test]
    async fn speaker_refusal_retries_unattributed_content_for_same_target_only() {
        let server = Server::new(Mode::Unattributed).await;
        let fetched = run(&server, Target::MeetingId("M/one".into()))
            .await
            .unwrap();
        assert_eq!(fetched.items.len(), 1);
        let requests = server.requests();
        assert_eq!(requests.len(), 4);
        assert!(requests[2].0.contains('?'));
        assert!(!requests[3].0.contains('?'));
        assert_eq!(
            requests[3].2,
            "application/vnd.microsoft.graph.transcript+text"
        );
        assert_own_target_only(&server);
    }
}

/// Metadata-only proof of one calendar occurrence; never authorizes a recurring series.
const MAX_CALENDAR_EVENTS: usize = 100;
const MAX_HYDRATED_EVENTS: usize = 8;
// Operational diagnostics deliberately contain no selector, identity, time,
// meeting metadata, response body, or bearer credentials.
#[derive(Debug)]
struct OccurrenceDiagnostic {
    stage: &'static str,
    reason: &'static str,
    status: Option<u16>,
    code: Option<String>,
    eligible: usize,
    matches: usize,
}
impl OccurrenceDiagnostic {
    fn new(
        stage: &'static str,
        reason: &'static str,
        error: &Error,
        eligible: usize,
        matches: usize,
    ) -> Self {
        let (status, code) = match error {
            Error::Graph { status, code } => (
                Some(*status),
                code.as_deref()
                    .filter(|v| {
                        matches!(
                            *v,
                            "ErrorItemNotFound"
                                | "ResourceNotFound"
                                | "Request_ResourceNotFound"
                                | "ErrorAccessDenied"
                                | "Authorization_RequestDenied"
                                | "Forbidden"
                                | "BadRequest"
                                | "InvalidRequest"
                                | "InvalidAuthenticationToken"
                                | "TooManyRequests"
                                | "GraphAccessToTranscriptsDisabled"
                        )
                    })
                    .map(str::to_owned),
            ),
            _ => (None, None),
        };
        Self {
            stage,
            reason,
            status,
            code,
            eligible,
            matches,
        }
    }
    fn report(self, error: Error) -> Error {
        tracing::warn!(stage=self.stage,reason=self.reason,status=?self.status,code=?self.code,eligible=self.eligible,matches=self.matches,"Teams meeting occurrence rejected");
        error
    }
}
fn reject_occurrence(
    stage: &'static str,
    reason: &'static str,
    error: Error,
    eligible: usize,
    matches: usize,
) -> Error {
    OccurrenceDiagnostic::new(stage, reason, &error, eligible, matches).report(error)
}

// Failure-only shape counts; never retain provider fields. First exclusion wins,
// in the same order as the eligibility checks. Bound observations to $top=100.
#[derive(Debug, Default, PartialEq, Eq)]
struct CalendarExclusions {
    total: usize,
    missing_join: usize,
    unsupported_type: usize,
    non_utc: usize,
    unparseable_bounds: usize,
    start_mismatch: usize,
    end_mismatch: usize,
}
impl CalendarExclusions {
    fn from_rows(
        rows: &[Value],
        start: DateTime<chrono::FixedOffset>,
        end: DateTime<chrono::FixedOffset>,
    ) -> Self {
        let mut d = Self {
            total: rows.len().min(100),
            ..Self::default()
        };
        for row in rows.iter().take(100) {
            if row
                .pointer("/onlineMeeting/joinUrl")
                .and_then(Value::as_str)
                .is_none()
            {
                d.missing_join += 1;
            } else if row.get("type").and_then(Value::as_str) != Some("singleInstance") {
                d.unsupported_type += 1;
            } else if !["start", "end"].iter().all(|key| {
                row.pointer(&format!("/{key}/timeZone"))
                    .and_then(Value::as_str)
                    == Some("UTC")
            }) {
                d.non_utc += 1;
            } else {
                let parse = |key: &str| {
                    row.pointer(&format!("/{key}/dateTime"))
                        .and_then(Value::as_str)
                        .and_then(|s| {
                            DateTime::parse_from_rfc3339(s)
                                .ok()
                                .or_else(|| DateTime::parse_from_rfc3339(&format!("{s}Z")).ok())
                        })
                };
                match (parse("start"), parse("end")) {
                    (Some(s), Some(e)) => {
                        if s != start {
                            d.start_mismatch += 1;
                        } else if e != end {
                            d.end_mismatch += 1;
                        }
                    }
                    _ => d.unparseable_bounds += 1,
                }
            }
        }
        d
    }
    fn report(&self) {
        tracing::warn!(
            total = self.total,
            missing_join = self.missing_join,
            unsupported_type = self.unsupported_type,
            non_utc = self.non_utc,
            unparseable_bounds = self.unparseable_bounds,
            start_mismatch = self.start_mismatch,
            end_mismatch = self.end_mismatch,
            "Teams meeting calendar eligibility excluded"
        );
    }
}

/// Authorize ONE occurrence window for this meeting and return the meeting with
/// `start`/`end` set to that window. Callers must persist and fetch with the
/// returned bounds, never the onlineMeeting's own. Rules, in order:
/// 1. Exactly one UTC single-instance calendar event with the onlineMeeting's
///    exact bounds links to the meeting: accepted unchanged.
/// 2. Otherwise the calendar is searched around now and around the declared
///    bounds for events linked to the meeting (same join URL, or an alias the
///    provider binds to the same opaque meeting ID):
///    - none linked (ad-hoc "Meet now"): the onlineMeeting's own bounds, or
///      its creation time plus a bounded default, are the occurrence;
///    - one single (possibly edited) event: that event's bounds;
///    - recurring rows: the one occurrence containing now (or starting within
///      `JOIN_EARLY`); none or several current ones is refused.
///      Several single events are ambiguous and refused.
/// The meeting must already have resolved through the caller's own delegated
/// token, which is the visibility proof.
pub(crate) async fn verify_occurrence(
    http: &Client,
    token: &str,
    meeting: &Meeting,
) -> Result<Meeting, Error> {
    verify_occurrence_from(http, token, meeting, &graph_origin()).await
}

/// [`verify_occurrence`] against an explicit Graph base (tests).
async fn verify_occurrence_from(
    http: &Client,
    token: &str,
    meeting: &Meeting,
    graph: &str,
) -> Result<Meeting, Error> {
    verify_occurrence_at(http, token, meeting, graph, Utc::now()).await
}

/// Strict exact-match proof first, then the flexible search, at a given clock.
async fn verify_occurrence_at(
    http: &Client,
    token: &str,
    meeting: &Meeting,
    graph: &str,
    now: DateTime<Utc>,
) -> Result<Meeting, Error> {
    let fallback = std::sync::atomic::AtomicBool::new(false);
    match verify_exact_from(http, token, meeting, graph, &fallback).await {
        Ok(()) => return Ok(meeting.clone()),
        Err(error) if !fallback.load(std::sync::atomic::Ordering::Relaxed) => return Err(error),
        Err(_) => {}
    }
    verify_flexible(http, token, meeting, graph, now).await
}

/// Graph reports `0001-01-01T00:00:00Z` (DateTime.MinValue) for instant
/// "Meet now" meetings. A time is only believable if it is after 1999 and not
/// more than a year ahead; anything else is treated as MISSING.
fn plausible_time<Tz: chrono::TimeZone>(d: &DateTime<Tz>) -> bool {
    use chrono::Datelike;
    d.year() >= 2000 && d.with_timezone(&Utc) < Utc::now() + chrono::Duration::days(366)
}

/// Parse an onlineMeeting time; implausible values (see [`plausible_time`]) are `None`.
fn parse_utc(v: &Option<String>) -> Option<DateTime<Utc>> {
    v.as_deref()
        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        .filter(plausible_time)
        .map(|d| d.with_timezone(&Utc))
}

/// Copy of `meeting` carrying the authorized occurrence window (and no series bound yet).
fn with_bounds(meeting: &Meeting, start: DateTime<Utc>, end: DateTime<Utc>) -> Meeting {
    let mut out = meeting.clone();
    out.transcript_end = None;
    out.recurring = false;
    out.start = Some(start.to_rfc3339());
    out.end = Some(end.to_rfc3339());
    out
}

/// Occurrence for a meeting no calendar event links to. Deterministic for a
/// given onlineMeeting (no clock input), so status and capture calls agree.
/// - Believable declared start: declared bounds (min 3h, max 24h).
/// - Placeholder/missing start (instant "Meet now"): anchored on creation time,
///   window = creation .. creation + 24h. A link reused later the same day is
///   still covered, and the window can never move: it ends 24h after creation.
/// - Neither believable: None (refused; no window is invented).
fn ad_hoc_bounds(meeting: &Meeting) -> Option<(DateTime<Utc>, DateTime<Utc>)> {
    let Some(start) = parse_utc(&meeting.start) else {
        let created = parse_utc(&meeting.creation)?;
        return Some((created, created + MAX_OCCURRENCE));
    };
    let end = match parse_utc(&meeting.end) {
        Some(e) if e > start && e - start <= MAX_OCCURRENCE => e,
        _ => start + AD_HOC_MIN_DURATION,
    };
    Some((start, end.max(start + AD_HOC_MIN_DURATION)))
}

/// Start/end of a UTC calendar row; `None` for non-UTC, unparseable or implausible bounds.
fn utc_event_bounds(event: &Value) -> Option<(DateTime<Utc>, DateTime<Utc>)> {
    let parse = |key: &str| {
        if event
            .pointer(&format!("/{key}/timeZone"))
            .and_then(Value::as_str)
            != Some("UTC")
        {
            return None;
        }
        event
            .pointer(&format!("/{key}/dateTime"))
            .and_then(Value::as_str)
            .and_then(|s| {
                DateTime::parse_from_rfc3339(s)
                    .ok()
                    .or_else(|| DateTime::parse_from_rfc3339(&format!("{s}Z")).ok())
            })
            .map(|d| d.with_timezone(&Utc))
    };
    match (parse("start"), parse("end")) {
        (Some(s), Some(e)) if e > s && plausible_time(&s) && plausible_time(&e) => Some((s, e)),
        _ => None,
    }
}

const MAX_FLEX_HYDRATIONS: usize = 12;
const MAX_FLEX_ALIAS_LOOKUPS: usize = 12;

/// One bounded `calendarView` read; incomplete, malformed or oversized lists are refused.
async fn calendar_rows(
    http: &Client,
    token: &str,
    graph: &str,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> Result<Vec<Value>, Error> {
    let mut url = Url::parse(&format!("{graph}/me/calendarView"))
        .map_err(|_| reject_occurrence("calendar", "invalid_endpoint", Error::Unavailable, 0, 0))?;
    url.query_pairs_mut()
        .append_pair("startDateTime", &from.to_rfc3339())
        .append_pair("endDateTime", &to.to_rfc3339())
        .append_pair(
            "$select",
            "id,type,start,end,onlineMeeting,isOnlineMeeting,onlineMeetingProvider",
        )
        .append_pair("$top", "100");
    let value = get_json(http, token, url.as_str())
        .await
        .map_err(|e| reject_occurrence("calendar", "provider_lookup_failed", e, 0, 0))?;
    if value.get("@odata.nextLink").is_some() {
        return Err(reject_occurrence(
            "calendar",
            "incomplete_list",
            Error::InvalidMetadata,
            0,
            0,
        ));
    }
    let rows = value
        .get("value")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            reject_occurrence("calendar", "malformed_list", Error::InvalidMetadata, 0, 0)
        })?;
    if rows.len() > MAX_CALENDAR_EVENTS {
        return Err(reject_occurrence(
            "calendar",
            "oversized_list",
            Error::InvalidMetadata,
            0,
            0,
        ));
    }
    Ok(rows.clone())
}

/// Token-level failures abort the proof; anything else on one calendar row
/// only means that row cannot be shown to link the meeting.
fn row_error_is_fatal(error: &Error) -> bool {
    matches!(error, Error::Graph { status: 401 | 429, .. })
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Linked {
    Single,
    Instance,
    Series,
}

/// Search the calendar for rows linked to `meeting` and pick its occurrence; every
/// unverifiable candidate row makes the proof fail closed instead of ad-hoc.
async fn verify_flexible(
    http: &Client,
    token: &str,
    meeting: &Meeting,
    graph: &str,
    now: DateTime<Utc>,
) -> Result<Meeting, Error> {
    let join = meeting.join_url.as_deref().ok_or_else(|| {
        reject_occurrence("metadata", "missing_join", Error::InvalidMetadata, 0, 0)
    })?;
    let declared = match (parse_utc(&meeting.start), parse_utc(&meeting.end)) {
        (Some(s), Some(e)) if e > s && e - s <= MAX_OCCURRENCE => Some((s, e)),
        _ => None,
    };
    let mut windows = vec![(now, now + SERIES_HORIZON)];
    if let Some((s, e)) = declared {
        let pad = chrono::Duration::hours(12);
        windows.push((s - pad, e + pad));
    }
    let mut rows: Vec<Value> = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for (from, to) in windows {
        for row in calendar_rows(http, token, graph, from, to).await? {
            let key = match row.get("id").and_then(Value::as_str) {
                Some(id) => id.to_owned(),
                None => row.to_string(),
            };
            let fresh = seen.insert(key);
            if fresh {
                rows.push(row);
            }
        }
    }
    // Spend the hydration/alias budgets on the rows nearest to now first.
    let distance = |row: &Value| match utc_event_bounds(row) {
        Some((s, e)) if s <= now && now < e => chrono::Duration::zero(),
        Some((s, e)) => (s - now).abs().min((e - now).abs()),
        None => chrono::Duration::days(3650),
    };
    rows.sort_by_key(|row| distance(row));
    let (mut hydrations, mut aliases) = (0, 0);
    // Set whenever a row that MIGHT link this meeting could not be checked
    // (lookup failed or budget spent). Absence of proof is not proof of absence:
    // such a meeting must not be treated as ad-hoc.
    let mut unresolved = false;
    let mut linked: Vec<(Linked, DateTime<Utc>, DateTime<Utc>)> = Vec::new();
    for row in &rows {
        let kind_bounds = utc_event_bounds(row);
        let hydrated;
        let mut row = row;
        if row
            .pointer("/onlineMeeting/joinUrl")
            .and_then(Value::as_str)
            .is_none()
        {
            let teams_capable = row.get("isOnlineMeeting").and_then(Value::as_bool) != Some(false)
                && row
                    .get("onlineMeetingProvider")
                    .and_then(Value::as_str)
                    .is_none_or(|p| p == "teamsForBusiness");
            // Implausible bounds (placeholder rows) do NOT exempt a row from
            // identity resolution: it is hydrated like any other, and if it links
            // this meeting it counts as linked-but-unusable (refusal).
            if !teams_capable {
                continue;
            }
            if hydrations >= MAX_FLEX_HYDRATIONS {
                unresolved = true;
                continue;
            }
            hydrations += 1;
            match hydrate_event_detail(http, token, row, graph).await {
                Ok(Some(detail)) => {
                    hydrated = detail;
                    row = &hydrated;
                }
                Ok(None) => continue,
                Err(e) if row_error_is_fatal(&e) => return Err(e),
                Err(_) => {
                    unresolved = true;
                    continue;
                }
            }
        }
        let Some(candidate) = row
            .pointer("/onlineMeeting/joinUrl")
            .and_then(Value::as_str)
        else {
            continue;
        };
        if candidate != join {
            if join_target(candidate).is_err() {
                continue;
            }
            if aliases >= MAX_FLEX_ALIAS_LOOKUPS {
                unresolved = true;
                continue;
            }
            aliases += 1;
            match resolve_calendar_alias(http, token, candidate, graph).await {
                Ok(alias) if alias.id == meeting.id => {}
                Ok(_) => continue,
                Err(e) if row_error_is_fatal(&e) => return Err(e),
                Err(_) => {
                    unresolved = true;
                    continue;
                }
            }
        }
        let kind = match row.get("type").and_then(Value::as_str) {
            Some("singleInstance") => Linked::Single,
            Some("occurrence" | "exception") => Linked::Instance,
            _ => Linked::Series,
        };
        match (kind, kind_bounds) {
            (Linked::Series, _) => linked.push((kind, now, now)),
            (_, Some((s, e))) if e - s <= MAX_OCCURRENCE => linked.push((kind, s, e)),
            // A linked row with unusable bounds still proves the meeting is
            // calendar-managed; it can never supply the occurrence.
            _ => linked.push((Linked::Series, now, now)),
        }
    }
    let count = linked.len();
    let refuse = |reason| {
        Err(reject_occurrence(
            "calendar",
            reason,
            Error::InvalidMetadata,
            count,
            0,
        ))
    };
    if linked.is_empty() && unresolved {
        return refuse("no_eligible_occurrence");
    }
    if linked.is_empty() {
        // Ad-hoc: visible through the caller's own /me/onlineMeetings, no event.
        return match ad_hoc_bounds(meeting) {
            Some((s, e)) => Ok(with_bounds(meeting, s, e)),
            None => refuse("no_eligible_occurrence"),
        };
    }
    let recurring = linked.iter().any(|(k, _, _)| *k != Linked::Single);
    if !recurring {
        return match linked.as_slice() {
            [(_, s, e)] => Ok(with_bounds(meeting, *s, *e)),
            _ => refuse("ambiguous_occurrence"),
        };
    }
    let current: Vec<_> = linked
        .iter()
        .filter(|(k, s, e)| *k != Linked::Series && *s - JOIN_EARLY <= now && now < *e)
        .collect();
    match current.as_slice() {
        [(_, s, e)] => {
            let mut out = with_bounds(meeting, *s, *e);
            out.recurring = true;
            // The transcript may land after a scheduled end, but never at or
            // after the next linked occurrence's start. Beyond what the calendar
            // read covered (now + horizon) nothing is assumed.
            let next = linked
                .iter()
                .filter(|(k, ns, _)| *k != Linked::Series && ns > s)
                .map(|(_, ns, _)| *ns)
                .min();
            let seen_until = now + SERIES_HORIZON;
            let mut upper = *e + SERIES_OVERRUN;
            match next {
                Some(n) => upper = upper.min(n),
                None if upper > seen_until => upper = seen_until.max(*e),
                None => {}
            }
            out.transcript_end = Some(upper);
            Ok(out)
        }
        [] => refuse("no_current_occurrence"),
        _ => refuse("ambiguous_occurrence"),
    }
}

/// Own-event detail read for a calendar row whose collection projection lacks
/// the native join URL. `Ok(None)` means the row is not a Teams meeting.
async fn hydrate_event_detail(
    http: &Client,
    token: &str,
    row: &Value,
    graph: &str,
) -> Result<Option<Value>, Error> {
    let id = row
        .get("id")
        .and_then(Value::as_str)
        .filter(|id| valid_id(id))
        .ok_or(Error::InvalidMetadata)?;
    let mut url = Url::parse(&format!("{graph}/me/events/{}", urlencoding::encode(id)))
        .map_err(|_| Error::Unavailable)?;
    url.query_pairs_mut().append_pair(
        "$select",
        "id,type,start,end,onlineMeeting,isOnlineMeeting,onlineMeetingProvider",
    );
    let detail = get_json(http, token, url.as_str()).await?;
    if detail.get("@odata.nextLink").is_some()
        || detail.get("id").and_then(Value::as_str) != Some(id)
        || detail.get("isOnlineMeeting").and_then(Value::as_bool) != Some(true)
        || detail.get("onlineMeetingProvider").and_then(Value::as_str) != Some("teamsForBusiness")
    {
        return Ok(None);
    }
    Ok(Some(detail))
}

/// Strict legacy proof: one UTC single-instance event with exactly the
/// onlineMeeting's bounds. Sets `fallback` only for refusals where the
/// flexible search may still find the meeting's occurrence.
async fn verify_exact_from(
    http: &Client,
    token: &str,
    meeting: &Meeting,
    graph: &str,
    fallback: &std::sync::atomic::AtomicBool,
) -> Result<(), Error> {
    let start = meeting
        .start
        .as_deref()
        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        .filter(plausible_time)
        .ok_or_else(|| {
            fallback.store(true, std::sync::atomic::Ordering::Relaxed);
            reject_occurrence("metadata", "invalid_start", Error::InvalidMetadata, 0, 0)
        })?;
    let end = meeting
        .end
        .as_deref()
        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        .filter(plausible_time)
        .ok_or_else(|| {
            fallback.store(true, std::sync::atomic::Ordering::Relaxed);
            reject_occurrence("metadata", "invalid_end", Error::InvalidMetadata, 0, 0)
        })?;
    if end <= start || end - start > chrono::Duration::hours(24) {
        fallback.store(true, std::sync::atomic::Ordering::Relaxed);
        return Err(reject_occurrence(
            "metadata",
            "unsupported_bounds",
            Error::InvalidMetadata,
            0,
            0,
        ));
    }
    let join = meeting.join_url.as_deref().ok_or_else(|| {
        reject_occurrence("metadata", "missing_join", Error::InvalidMetadata, 0, 0)
    })?;
    let mut url = Url::parse(&format!("{graph}/me/calendarView"))
        .map_err(|_| reject_occurrence("calendar", "invalid_endpoint", Error::Unavailable, 0, 0))?;
    url.query_pairs_mut()
        .append_pair("startDateTime", &start.to_rfc3339())
        .append_pair("endDateTime", &end.to_rfc3339())
        .append_pair("$select", "id,type,start,end,onlineMeeting")
        .append_pair("$top", "100");
    let value = get_json(http, token, url.as_str())
        .await
        .map_err(|e| reject_occurrence("calendar", "provider_lookup_failed", e, 0, 0))?;
    if value.get("@odata.nextLink").is_some() {
        return Err(reject_occurrence(
            "calendar",
            "incomplete_list",
            Error::InvalidMetadata,
            0,
            0,
        ));
    }
    let rows = value
        .get("value")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            reject_occurrence("calendar", "malformed_list", Error::InvalidMetadata, 0, 0)
        })?;
    if rows.len() > MAX_CALENDAR_EVENTS {
        return Err(reject_occurrence(
            "calendar",
            "oversized_list",
            Error::InvalidMetadata,
            0,
            0,
        ));
    }
    // When a collection projection lacks native online-meeting information, only
    // rows already proving the exact non-recurring bounds may be hydrated. An
    // unchecked row could duplicate a match, so exceeding the budget refuses
    // the entire proof before any extra request rather than skipping rows.
    let hydration_count = rows
        .iter()
        .filter(|row| {
            row.pointer("/onlineMeeting/joinUrl")
                .and_then(Value::as_str)
                .is_none()
                && exact_event_bounds(row, start, end)
        })
        .count();
    if hydration_count > MAX_HYDRATED_EVENTS {
        return Err(reject_occurrence(
            "hydration",
            "hydration_budget_exceeded",
            Error::InvalidMetadata,
            0,
            0,
        ));
    }
    let mut matches = 0;
    let mut eligible = 0;
    let mut wrong_identity = 0;
    for row in rows {
        let hydrated;
        let row = if row
            .pointer("/onlineMeeting/joinUrl")
            .and_then(Value::as_str)
            .is_none()
            && exact_event_bounds(row, start, end)
        {
            hydrated = hydrate_calendar_event(http, token, row, graph, start, end).await?;
            &hydrated
        } else {
            row
        };
        let Some(candidate) = row
            .pointer("/onlineMeeting/joinUrl")
            .and_then(Value::as_str)
        else {
            continue;
        };
        // Keep singleInstance, UTC and exact provider bounds mandatory. A
        // different link is a selector, never evidence of another occurrence.
        if !exact_single_occurrence(std::slice::from_ref(row), candidate, start, end) {
            continue;
        }
        eligible += 1;
        if candidate != join {
            let alias = resolve_calendar_alias(http, token, candidate, graph)
                .await
                .map_err(|e| {
                    reject_occurrence("alias", "provider_binding_failed", e, eligible, matches)
                })?;
            if alias.id != meeting.id {
                wrong_identity += 1;
                continue;
            }
            if alias
                .start
                .as_deref()
                .and_then(|v| DateTime::parse_from_rfc3339(v).ok())
                != Some(start)
                || alias
                    .end
                    .as_deref()
                    .and_then(|v| DateTime::parse_from_rfc3339(v).ok())
                    != Some(end)
            {
                return Err(reject_occurrence(
                    "alias",
                    "returned_bounds_mismatch",
                    Error::InvalidMetadata,
                    eligible,
                    matches,
                ));
            }
        }
        matches += 1;
        // Check aliases as well as canonical rows: two calendar entries for
        // the same meeting must not pass merely because only one URL is equal.
        if matches > 1 {
            return Err(reject_occurrence(
                "calendar",
                "ambiguous_occurrence",
                Error::InvalidMetadata,
                eligible,
                matches,
            ));
        }
    }
    if matches == 1 {
        Ok(())
    } else {
        if eligible == 0 {
            CalendarExclusions::from_rows(rows, start, end).report();
        }
        if eligible == 0 {
            fallback.store(true, std::sync::atomic::Ordering::Relaxed);
        }
        let reason = if wrong_identity > 0 {
            "different_provider_identity"
        } else if eligible == 0 {
            "no_eligible_occurrence"
        } else {
            "no_verified_occurrence"
        };
        Err(reject_occurrence(
            "calendar",
            reason,
            Error::InvalidMetadata,
            eligible,
            matches,
        ))
    }
}

async fn hydrate_calendar_event(
    http: &Client,
    token: &str,
    row: &Value,
    graph: &str,
    start: DateTime<chrono::FixedOffset>,
    end: DateTime<chrono::FixedOffset>,
) -> Result<Value, Error> {
    let reject = |reason, error| reject_occurrence("hydration", reason, error, 0, 0);
    let id = row
        .get("id")
        .and_then(Value::as_str)
        .filter(|id| valid_id(id))
        .ok_or_else(|| reject("invalid_event_id", Error::InvalidMetadata))?;
    let mut url = Url::parse(&format!("{graph}/me/events/{}", urlencoding::encode(id)))
        .map_err(|_| reject("invalid_event_id", Error::Unavailable))?;
    url.query_pairs_mut().append_pair(
        "$select",
        "id,type,start,end,onlineMeeting,isOnlineMeeting,onlineMeetingProvider",
    );
    let detail = get_json(http, token, url.as_str())
        .await
        .map_err(|e| reject("provider_lookup_failed", e))?;
    if detail.get("@odata.nextLink").is_some() {
        return Err(reject("incomplete_list", Error::InvalidMetadata));
    }
    if detail.get("id").and_then(Value::as_str) != Some(id) {
        return Err(reject(
            "returned_event_identity_mismatch",
            Error::InvalidMetadata,
        ));
    }
    if !exact_event_bounds(&detail, start, end) {
        return Err(reject("returned_bounds_mismatch", Error::InvalidMetadata));
    }
    if detail.get("isOnlineMeeting").and_then(Value::as_bool) != Some(true)
        || detail.get("onlineMeetingProvider").and_then(Value::as_str) != Some("teamsForBusiness")
    {
        return Err(reject("native_meeting_unavailable", Error::InvalidMetadata));
    }
    let join = detail
        .pointer("/onlineMeeting/joinUrl")
        .and_then(Value::as_str)
        .ok_or_else(|| reject("missing_native_join", Error::InvalidMetadata))?;
    // The normal alias path subsequently binds this selector to the same own
    // opaque onlineMeeting ID. A body link or deprecated URL is never proof.
    join_target(join).map_err(|_| reject("missing_native_join", Error::InvalidMetadata))?;
    Ok(detail)
}

async fn resolve_calendar_alias(
    http: &Client,
    token: &str,
    candidate: &str,
    graph: &str,
) -> Result<Meeting, Error> {
    match join_target(candidate)? {
        Target::JoinUrl(link) => {
            // The calendar provider supplied this selector. Its unique /me
            // result may canonicalize the URL; the caller above still requires
            // the exact previously verified opaque meeting ID and bounds.
            // Public caller-selected JoinUrl resolution remains stricter.
            let alias = lookup_meeting(
                http,
                token,
                graph,
                &format!("joinWebUrl eq '{}'", link.replace('\'', "''")),
            )
            .await?;
            if !valid_id(&alias.id)
                || alias
                    .join_url
                    .as_deref()
                    .and_then(|v| join_target(v).ok())
                    .is_none()
            {
                return Err(Error::InvalidMetadata);
            }
            Ok(alias)
        }
        target => resolve_from(http, token, target, graph).await,
    }
}

fn exact_single_occurrence(
    rows: &[Value],
    join: &str,
    start: DateTime<chrono::FixedOffset>,
    end: DateTime<chrono::FixedOffset>,
) -> bool {
    let count = rows
        .iter()
        .filter(|event| {
            exact_event_bounds(event, start, end)
                && event
                    .pointer("/onlineMeeting/joinUrl")
                    .and_then(Value::as_str)
                    == Some(join)
        })
        .count();
    count == 1
}

fn exact_event_bounds(
    event: &Value,
    start: DateTime<chrono::FixedOffset>,
    end: DateTime<chrono::FixedOffset>,
) -> bool {
    if event.get("type").and_then(Value::as_str) != Some("singleInstance") {
        return false;
    }
    let parse = |key: &str| {
        event
            .pointer(&format!("/{key}/dateTime"))
            .and_then(Value::as_str)
            .and_then(|s| {
                DateTime::parse_from_rfc3339(s)
                    .ok()
                    .or_else(|| DateTime::parse_from_rfc3339(&format!("{s}Z")).ok())
            })
    };
    // Graph default calendarView response is UTC; reject another zone rather than guessing.
    if !["start", "end"].iter().all(|key| {
        event
            .pointer(&format!("/{key}/timeZone"))
            .and_then(Value::as_str)
            == Some("UTC")
    }) {
        return false;
    }
    parse("start") == Some(start) && parse("end") == Some(end)
}

#[cfg(test)]
mod meeting_occurrence_tests {
    use super::*;
    #[test]
    fn recording_or_series_calendar_presence_cannot_authorize_an_occurrence() {
        let start = DateTime::parse_from_rfc3339("2026-10-01T02:00:00Z").unwrap();
        let end = start + chrono::Duration::hours(1);
        let event = serde_json::json!({"type":"singleInstance","onlineMeeting":{"joinUrl":"https://teams.microsoft.com/l/meetup-join/fixture"},"start":{"dateTime":start.to_rfc3339(),"timeZone":"UTC"},"end":{"dateTime":end.to_rfc3339(),"timeZone":"UTC"}});
        assert!(exact_single_occurrence(
            &[event.clone()],
            "https://teams.microsoft.com/l/meetup-join/fixture",
            start,
            end
        ));
        for kind in ["seriesMaster", "occurrence", "exception"] {
            let mut series = event.clone();
            series["type"] = serde_json::json!(kind);
            assert!(!exact_single_occurrence(
                &[series],
                "https://teams.microsoft.com/l/meetup-join/fixture",
                start,
                end
            ));
        }
        assert!(!exact_single_occurrence(
            &[event.clone(), event.clone()],
            "https://teams.microsoft.com/l/meetup-join/fixture",
            start,
            end
        ));
        assert!(!exact_single_occurrence(
            &[event],
            "https://teams.microsoft.com/l/meetup-join/other",
            start,
            end
        ));
    }
}

#[cfg(test)]
mod calendar_alias_tests {
    use super::*;
    use axum::{extract::State, http::StatusCode, response::IntoResponse, Json, Router};
    use serde_json::json;
    use std::sync::{Arc, Mutex};
    const CANONICAL: &str = "https://teams.microsoft.com/l/meetup-join/canonical?context=fixture";
    const LEGACY_ALIAS: &str =
        "https://teams.microsoft.com/l/meetup-join/calendar-alias?context=fixture";
    const MODERN_ALIAS: &str = "https://teams.microsoft.com/meet/123456?p=synthetic-passcode";
    fn metadata() -> Value {
        json!({"id":"opaque/selected", "joinWebUrl":CANONICAL,
            "joinMeetingIdSettings":{"joinMeetingId":"123456"},
            "startDateTime":"2026-10-01T02:20:00Z", "endDateTime":"2026-10-01T02:30:00Z"})
    }
    fn event(join: &str) -> Value {
        json!({"type":"singleInstance","onlineMeeting":{"joinUrl":join},
            "start":{"dateTime":"2026-10-01T02:20:00.0000000","timeZone":"UTC"},
            "end":{"dateTime":"2026-10-01T02:30:00.0000000","timeZone":"UTC"}})
    }
    fn projected_event() -> Value {
        let mut row = event(CANONICAL);
        row["id"] = json!("event/fixture");
        row["onlineMeeting"] = Value::Null;
        row
    }
    fn native_event(join: &str) -> Value {
        let mut row = event(join);
        row["id"] = json!("event/fixture");
        row["isOnlineMeeting"] = json!(true);
        row["onlineMeetingProvider"] = json!("teamsForBusiness");
        row
    }
    struct Fixture {
        calendar: Value,
        lookup: Value,
        denied: bool,
        event_detail: Option<Value>,
        event_denied: bool,
        requests: Mutex<Vec<(String, String)>>,
    }
    struct Server {
        fixture: Arc<Fixture>,
        base: String,
        task: tokio::task::JoinHandle<()>,
    }
    impl Drop for Server {
        fn drop(&mut self) {
            self.task.abort();
        }
    }
    impl Server {
        async fn new(calendar: Value, lookup: Value, denied: bool) -> Self {
            Self::with_event(calendar, lookup, denied, None, false).await
        }
        async fn with_event(
            calendar: Value,
            lookup: Value,
            denied: bool,
            event_detail: Option<Value>,
            event_denied: bool,
        ) -> Self {
            let fixture = Arc::new(Fixture {
                calendar,
                lookup,
                denied,
                event_detail,
                event_denied,
                requests: Mutex::new(vec![]),
            });
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let base = format!("http://{}/v1.0", listener.local_addr().unwrap());
            let app = Router::new().fallback(reply).with_state(fixture.clone());
            let task = tokio::spawn(async move {
                axum::serve(listener, app).await.unwrap();
            });
            Self {
                fixture,
                base,
                task,
            }
        }
        async fn verify(&self) -> Result<Meeting, Error> {
            self.verify_meeting(metadata()).await
        }
        async fn verify_meeting(&self, meeting: Value) -> Result<Meeting, Error> {
            let meeting: Meeting = serde_json::from_value(meeting).unwrap();
            let now = DateTime::parse_from_rfc3339("2026-10-01T02:25:00Z")
                .unwrap()
                .with_timezone(&Utc);
            verify_occurrence_at(&Client::new(), "own-member-fixture", &meeting, &self.base, now)
                .await
        }
        fn count(&self) -> usize {
            let requests = self.fixture.requests.lock().unwrap();
            for (uri, auth) in requests.iter() {
                assert_eq!(auth, "Bearer own-member-fixture");
                assert!(
                    uri.starts_with("/v1.0/me/calendarView?")
                        || uri.starts_with("/v1.0/me/onlineMeetings?")
                        || uri.starts_with("/v1.0/me/events/")
                );
                if uri.starts_with("/v1.0/me/events/") {
                    let url = Url::parse(&format!("http://fixture{uri}")).unwrap();
                    assert_eq!(
                        url.query_pairs().collect::<Vec<_>>(),
                        vec![(
                            "$select".into(),
                            "id,type,start,end,onlineMeeting,isOnlineMeeting,onlineMeetingProvider"
                                .into()
                        )]
                    );
                }
                assert!(
                    !uri.contains("/content")
                        && !uri.contains("/transcripts")
                        && !uri.contains("/users/")
                );
            }
            requests.len()
        }
    }
    async fn reply(
        State(f): State<Arc<Fixture>>,
        request: axum::http::Request<axum::body::Body>,
    ) -> axum::response::Response {
        f.requests.lock().unwrap().push((
            request.uri().to_string(),
            request
                .headers()
                .get("authorization")
                .unwrap()
                .to_str()
                .unwrap()
                .into(),
        ));
        match request.uri().path() {
            "/v1.0/me/calendarView" => Json(f.calendar.clone()).into_response(),
            "/v1.0/me/onlineMeetings" if f.denied => (
                StatusCode::FORBIDDEN,
                Json(json!({"error":{"code":"Denied"}})),
            )
                .into_response(),
            "/v1.0/me/onlineMeetings" => Json(f.lookup.clone()).into_response(),
            path if path.starts_with("/v1.0/me/events/") && f.event_denied => (
                StatusCode::FORBIDDEN,
                Json(json!({"error":{"code":"ErrorAccessDenied"}})),
            )
                .into_response(),
            path if path.starts_with("/v1.0/me/events/") => {
                Json(f.event_detail.clone().unwrap_or(Value::Null)).into_response()
            }
            _ => StatusCode::NOT_FOUND.into_response(),
        }
    }
    #[tokio::test]
    async fn missing_calendar_projection_hydrates_only_own_exact_event_before_binding() {
        let mut projected = event(CANONICAL);
        projected["id"] = json!("event/fixture");
        projected["onlineMeeting"] = Value::Null;
        let mut detail = event(CANONICAL);
        detail["id"] = json!("event/fixture");
        detail["isOnlineMeeting"] = json!(true);
        detail["onlineMeetingProvider"] = json!("teamsForBusiness");
        for join in [CANONICAL, MODERN_ALIAS] {
            detail["onlineMeeting"]["joinUrl"] = json!(join);
            let server = Server::with_event(
                json!({"value":[projected.clone()]}),
                json!({"value":[metadata()]}),
                false,
                Some(detail.clone()),
                false,
            )
            .await;
            assert!(
                server.verify().await.is_ok(),
                "complete own-event metadata must satisfy the unchanged occurrence proof"
            );
            assert_eq!(server.count(), if join == CANONICAL { 2 } else { 3 });
        }
    }

    #[tokio::test]
    async fn hydrated_event_missing_native_metadata_or_conflicting_proof_never_authorizes() {
        let mut negatives = vec![];
        for (pointer, value) in [
            ("/id", json!("event/other")),
            ("/type", json!("occurrence")),
            ("/start/timeZone", json!("Pacific Standard Time")),
            ("/start/dateTime", json!("2026-10-01T02:19:00Z")),
            ("/end/dateTime", json!("2026-10-01T02:31:00Z")),
            ("/end/dateTime", json!("private-invalid-time")),
            ("/isOnlineMeeting", json!(false)),
            ("/isOnlineMeeting", Value::Null),
            ("/onlineMeetingProvider", json!("skypeForBusiness")),
            ("/onlineMeeting/joinUrl", Value::Null),
            ("/onlineMeeting/joinUrl", json!(17)),
            (
                "/onlineMeeting/joinUrl",
                json!("https://evil.example/meeting"),
            ),
        ] {
            let mut detail = native_event(CANONICAL);
            *detail.pointer_mut(pointer).unwrap() = value;
            negatives.push(detail);
        }
        let mut incomplete = native_event(CANONICAL);
        incomplete["@odata.nextLink"] = json!("https://evil.example/page");
        negatives.push(incomplete);
        let mut body_only = native_event(CANONICAL);
        body_only["onlineMeeting"] = Value::Null;
        body_only["onlineMeetingUrl"] = json!(CANONICAL);
        body_only["body"] = json!({"content":CANONICAL});
        negatives.push(body_only);
        negatives.push(Value::Null);
        for detail in negatives {
            let server = Server::with_event(
                json!({"value":[projected_event()]}),
                json!({"value":[metadata()]}),
                false,
                Some(detail),
                false,
            )
            .await;
            assert!(server.verify().await.is_err());
            assert_eq!(
                server.count(),
                2,
                "detail refusal cannot reach aliases or content"
            );
        }
        let denied = Server::with_event(
            json!({"value":[projected_event()]}),
            json!({}),
            false,
            None,
            true,
        )
        .await;
        assert!(matches!(
            denied.verify().await,
            Err(Error::Graph { status: 403, .. })
        ));
        assert_eq!(denied.count(), 2);
    }

    #[tokio::test]
    async fn hydration_requires_a_valid_event_id_before_any_detail_request() {
        for id in ["..", "", "private invalid id", &"x".repeat(2049)] {
            let mut row = projected_event();
            row["id"] = json!(id);
            let server = Server::with_event(
                json!({"value":[row]}),
                json!({"value":[metadata()]}),
                false,
                Some(native_event(CANONICAL)),
                false,
            )
            .await;
            // An exact-bounds row with an unusable ID is refused outright (the
            // strict proof hydrates it), and no detail read is ever made for it.
            assert!(server.verify().await.is_err());
            server.count();
            assert!(!server
                .fixture
                .requests
                .lock()
                .unwrap()
                .iter()
                .any(|(uri, _)| uri.starts_with("/v1.0/me/events/")));
        }
    }

    #[tokio::test]
    async fn hydration_keeps_alias_identity_and_calendar_uniqueness_mandatory() {
        let mut other = metadata();
        other["id"] = json!("opaque/other");
        let server = Server::with_event(
            json!({"value":[projected_event()]}),
            json!({"value":[other]}),
            false,
            Some(native_event(MODERN_ALIAS)),
            false,
        )
        .await;
        assert!(server.verify().await.is_err());
        assert_eq!(server.count(), 3);
        for rows in [
            vec![event(CANONICAL), projected_event()],
            vec![projected_event(), projected_event()],
        ] {
            let server = Server::with_event(
                json!({"value":rows}),
                json!({"value":[metadata()]}),
                false,
                Some(native_event(MODERN_ALIAS)),
                false,
            )
            .await;
            assert!(
                server.verify().await.is_err(),
                "hydration must not hide duplicate bound occurrences"
            );
            server.count();
        }
    }

    #[tokio::test]
    async fn hydration_budget_refuses_entire_proof_before_reads_including_existing_match() {
        let mut other = metadata();
        other["id"] = json!("opaque/other");
        for count in [MAX_HYDRATED_EVENTS, MAX_HYDRATED_EVENTS + 1] {
            let mut rows = vec![event(CANONICAL)];
            rows.extend(vec![projected_event(); count]);
            let server = Server::with_event(
                json!({"value":rows}),
                json!({"value":[other.clone()]}),
                false,
                Some(native_event(MODERN_ALIAS)),
                false,
            )
            .await;
            assert_eq!(server.verify().await.is_ok(), count == MAX_HYDRATED_EVENTS);
            assert_eq!(
                server.count(),
                if count == MAX_HYDRATED_EVENTS {
                    1 + 2 * MAX_HYDRATED_EVENTS
                } else {
                    1
                },
                "budget overflow cannot accept the first match or read details"
            );
        }
        let server = Server::with_event(
            json!({"value":vec![projected_event();MAX_CALENDAR_EVENTS+1]}),
            json!({}),
            false,
            Some(native_event(CANONICAL)),
            false,
        )
        .await;
        assert!(server.verify().await.is_err());
        assert_eq!(server.count(), 1);
    }

    #[tokio::test]
    async fn event_id_is_encoded_as_one_own_resource_segment() {
        for id in ["event/fixture", "opaque+id/==", "event/../?#%fixture"] {
            let mut row = projected_event();
            row["id"] = json!(id);
            let mut detail = native_event(CANONICAL);
            detail["id"] = json!(id);
            let server =
                Server::with_event(json!({"value":[row]}), json!({}), true, Some(detail), false)
                    .await;
            assert!(server.verify().await.is_ok());
            assert_eq!(server.count(), 2);
            let requests = server.fixture.requests.lock().unwrap();
            assert_eq!(
                requests[1].0.split('?').next().unwrap(),
                format!("/v1.0/me/events/{}", urlencoding::encode(id))
            );
        }
    }
    #[tokio::test]
    async fn modern_and_legacy_calendar_aliases_require_same_provider_meeting() {
        for alias in [
            MODERN_ALIAS,
            LEGACY_ALIAS,
            "https://teams.cloud.microsoft/meet/123456?p=synthetic",
        ] {
            let s = Server::new(
                json!({"value":[event(alias)]}),
                json!({"value":[metadata()]}),
                false,
            )
            .await;
            assert!(
                s.verify().await.is_ok(),
                "provider-verified alias must bind"
            );
            assert_eq!(s.count(), 2);
        }
    }
    #[tokio::test]
    async fn canonical_calendar_match_needs_no_additional_lookup() {
        let s = Server::new(json!({"value":[event(CANONICAL)]}), json!({}), true).await;
        assert!(s.verify().await.is_ok());
        assert_eq!(s.count(), 1);
    }
    #[tokio::test]
    async fn canonical_and_alias_duplicates_do_not_bypass_unique_occurrence() {
        for rows in [
            vec![event(CANONICAL), event(MODERN_ALIAS)],
            vec![event(CANONICAL), event(LEGACY_ALIAS)],
            vec![event(MODERN_ALIAS), event(LEGACY_ALIAS)],
        ] {
            let s = Server::new(json!({"value":rows}), json!({"value":[metadata()]}), false).await;
            assert!(s.verify().await.is_err());
            s.count();
        }
    }
    #[tokio::test]
    async fn alias_lookup_fails_closed_on_identity_metadata_permission_or_ambiguity() {
        let mut wrong = metadata();
        wrong["id"] = json!("opaque/other");
        let mut bounds = metadata();
        bounds["endDateTime"] = json!("2026-10-01T02:31:00Z");
        let mut untrusted = metadata();
        untrusted["joinWebUrl"] = json!("https://evil.example/meeting");
        let mut join_id = metadata();
        join_id["joinMeetingIdSettings"]["joinMeetingId"] = json!("999999");
        let negatives = [
            json!({"value":[wrong]}),
            json!({"value":[bounds]}),
            json!({"value":[untrusted]}),
            json!({"value":[metadata(),metadata()]}),
            json!({"value":[metadata()],"@odata.nextLink":"https://evil.example/page"}),
            json!({"value":[{}]}),
        ];
        for alias in [MODERN_ALIAS, LEGACY_ALIAS] {
            for lookup in &negatives {
                let s = Server::new(json!({"value":[event(alias)]}), lookup.clone(), false).await;
                assert!(s.verify().await.is_err());
                assert_eq!(s.count(), 2);
            }
            let s = Server::new(json!({"value":[event(alias)]}), json!({}), true).await;
            assert!(s.verify().await.is_err());
            assert_eq!(s.count(), 2);
        }
        // Numeric join-ID verification belongs only to the modern resolver;
        // a legacy alias is bound by provider filter plus opaque identity.
        let s = Server::new(
            json!({"value":[event(MODERN_ALIAS)]}),
            json!({"value":[join_id]}),
            false,
        )
        .await;
        assert!(s.verify().await.is_err());
        assert_eq!(s.count(), 2);
    }

    #[tokio::test]
    async fn calendar_exclusions_distinguish_missing_join_and_each_guard_without_provider_work() {
        let start = DateTime::parse_from_rfc3339("2026-10-01T02:20:00Z").unwrap();
        let end = DateTime::parse_from_rfc3339("2026-10-01T02:30:00Z").unwrap();
        let mut excluded = vec![];
        for online in [Value::Null, json!({}), json!({"joinUrl":17})] {
            let mut row = event(CANONICAL);
            row["onlineMeeting"] = online;
            excluded.push(row);
        }
        for (field, value) in [
            ("type", json!("occurrence")),
            ("start/timeZone", json!("Pacific Standard Time")),
            ("end/dateTime", json!("private-unparseable-time")),
            ("start/dateTime", json!("2026-10-01T02:19:00Z")),
            ("end/dateTime", json!("2026-10-01T02:31:00Z")),
        ] {
            let mut row = event(CANONICAL);
            *row.pointer_mut(&format!("/{field}")).unwrap() = value;
            excluded.push(row);
        }
        let d = CalendarExclusions::from_rows(&excluded, start, end);
        assert_eq!(
            d,
            CalendarExclusions {
                total: 8,
                missing_join: 3,
                unsupported_type: 1,
                non_utc: 1,
                unparseable_bounds: 1,
                start_mismatch: 1,
                end_mismatch: 1,
            }
        );
        assert_eq!(
            CalendarExclusions::from_rows(&[], start, end),
            CalendarExclusions::default()
        );
        // No exclusion can reach a transcript/content request (count() checks
        // every URL), and none of these rows can mint an occurrence of its own:
        // whatever is accepted is the meeting's own declared occurrence or a
        // linked single event's own bounds.
        for row in excluded {
            let server =
                Server::new(json!({"value":[row]}), json!({"value":[metadata()]}), false).await;
            if let Ok(accepted) = server.verify().await {
                // Own declared start, or an edited single event's own start.
                let start = accepted.start.unwrap();
                assert!(
                    start.starts_with("2026-10-01T02:20:00")
                        || start.starts_with("2026-10-01T02:19:00"),
                    "{start}"
                );
            }
            server.count();
        }
        let canonical = CalendarExclusions::from_rows(&[event(CANONICAL)], start, end);
        assert_eq!(
            canonical,
            CalendarExclusions {
                total: 1,
                ..CalendarExclusions::default()
            }
        );
    }

    #[test]
    fn exclusion_counts_are_bounded_and_do_not_retain_private_metadata() {
        let start = DateTime::parse_from_rfc3339("2026-10-01T02:20:00Z").unwrap();
        let end = DateTime::parse_from_rfc3339("2026-10-01T02:30:00Z").unwrap();
        let mut row = event("https://teams.microsoft.com/meet/private?token=secret");
        row["id"] = json!("private-event-identifier");
        row["subject"] = json!("PRIVATE SUBJECT");
        row["body"] = json!("PRIVATE CONTENT actor@example.com");
        row["start"]["dateTime"] = json!("private-unparseable-time");
        let d = CalendarExclusions::from_rows(&vec![row; 101], start, end);
        assert_eq!(d.total, 100);
        assert_eq!(d.unparseable_bounds, 100);
        struct Writer(Arc<Mutex<Vec<u8>>>);
        impl std::io::Write for Writer {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(bytes);
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let output = Arc::new(Mutex::new(Vec::new()));
        let sink = output.clone();
        let subscriber = tracing_subscriber::fmt()
            .json()
            .without_time()
            .with_writer(move || Writer(sink.clone()))
            .finish();
        tracing::subscriber::with_default(subscriber, || d.report());
        let rendered = String::from_utf8(output.lock().unwrap().clone()).unwrap();
        let log: Value = serde_json::from_str(&rendered).unwrap();
        let fields = log["fields"].as_object().unwrap();
        assert_eq!(fields.len(), 8);
        assert_eq!(
            fields["message"],
            "Teams meeting calendar eligibility excluded"
        );
        assert_eq!(fields["total"], 100);
        assert_eq!(fields["unparseable_bounds"], 100);
        for (key, value) in fields {
            if key != "message" {
                assert!(value.is_u64(), "counts only");
            }
        }
        for private in [
            "https://", "secret", "private", "SUBJECT", "CONTENT", "actor@", "2026-",
        ] {
            assert!(!rendered.contains(private));
        }
    }

    // --- any-meeting acceptance (the clock is fixed at 2026-10-01T02:25:00Z) ---
    fn row(kind: &str, id: &str, join: &str, start: &str, end: &str) -> Value {
        json!({"id":id,"type":kind,"onlineMeeting":{"joinUrl":join},
            "start":{"dateTime":start,"timeZone":"UTC"},
            "end":{"dateTime":end,"timeZone":"UTC"}})
    }
    async fn verify_rows(rows: Vec<Value>) -> Result<Meeting, Error> {
        let s = Server::new(json!({"value":rows}), json!({"value":[metadata()]}), false).await;
        let result = s.verify().await;
        s.count();
        result
    }
    fn bounds(m: &Meeting) -> (String, String) {
        (m.start.clone().unwrap(), m.end.clone().unwrap())
    }

    #[tokio::test]
    async fn recurring_series_accepts_only_the_occurrence_containing_now() {
        // Yesterday's, today's (current) and tomorrow's occurrences of the series.
        let rows = vec![
            row("occurrence", "o1", CANONICAL, "2026-09-30T02:00:00", "2026-09-30T03:00:00"),
            row("occurrence", "o2", CANONICAL, "2026-10-01T02:00:00", "2026-10-01T03:00:00"),
            row("occurrence", "o3", CANONICAL, "2026-10-02T02:00:00", "2026-10-02T03:00:00"),
        ];
        let accepted = verify_rows(rows).await.unwrap();
        // The occurrence's own bounds, NOT the onlineMeeting's 02:20-02:30.
        assert_eq!(
            bounds(&accepted),
            ("2026-10-01T02:00:00+00:00".into(), "2026-10-01T03:00:00+00:00".into())
        );
        // Bounded by the next sibling (10-02 02:00) or end + 2h (05:00) if sooner.
        assert_eq!(
            accepted.transcript_end.unwrap().to_rfc3339(),
            "2026-10-01T05:00:00+00:00"
        );
        // An exception (moved instance) of the series is an occurrence as well.
        let moved = row("exception", "o4", CANONICAL, "2026-10-01T02:15:00", "2026-10-01T02:45:00");
        assert_eq!(
            bounds(&verify_rows(vec![moved]).await.unwrap()).1,
            "2026-10-01T02:45:00+00:00"
        );
    }

    #[tokio::test]
    async fn recurring_transcript_bound_stops_at_the_next_back_to_back_occurrence() {
        let now_occ = row("occurrence", "a", CANONICAL, "2026-10-01T02:00:00", "2026-10-01T03:00:00");
        let next = row("occurrence", "b", CANONICAL, "2026-10-01T03:00:00", "2026-10-01T04:00:00");
        let accepted = verify_rows(vec![now_occ, next]).await.unwrap();
        assert_eq!(
            accepted.transcript_end.unwrap().to_rfc3339(),
            "2026-10-01T03:00:00+00:00"
        );
    }

    #[tokio::test]
    async fn verify_marks_recurring_and_not_single() {
        let now_occ = row("occurrence", "a", CANONICAL, "2026-10-01T02:00:00", "2026-10-01T03:00:00");
        assert!(verify_rows(vec![now_occ]).await.unwrap().recurring);
        let single = row("singleInstance", "e", CANONICAL, "2026-10-01T02:40:00", "2026-10-01T03:40:00");
        assert!(!verify_rows(vec![single]).await.unwrap().recurring);
    }

    #[tokio::test]
    async fn only_recurring_occurrences_get_a_transcript_bound() {
        let edited = row("singleInstance", "e", CANONICAL, "2026-10-01T02:40:00", "2026-10-01T03:40:00");
        assert!(verify_rows(vec![edited]).await.unwrap().transcript_end.is_none());
        assert!(verify_rows(vec![]).await.unwrap().transcript_end.is_none());
        let exact = Server::new(json!({"value":[event(CANONICAL)]}), json!({}), true).await;
        assert!(exact.verify().await.unwrap().transcript_end.is_none());
    }

    #[tokio::test]
    async fn recurring_series_joins_up_to_fifteen_minutes_early_but_no_earlier() {
        // now = 02:25; an occurrence starting 02:40 is exactly at the join window.
        let soon = row("occurrence", "s", CANONICAL, "2026-10-01T02:40:00", "2026-10-01T03:10:00");
        assert!(verify_rows(vec![soon]).await.is_ok());
        let later = row("occurrence", "s", CANONICAL, "2026-10-01T02:41:00", "2026-10-01T03:10:00");
        assert!(verify_rows(vec![later]).await.is_err());
    }

    #[tokio::test]
    async fn recurring_series_without_a_current_occurrence_or_with_two_is_refused() {
        let past = row("occurrence", "p", CANONICAL, "2026-09-30T02:00:00", "2026-09-30T03:00:00");
        let next = row("occurrence", "n", CANONICAL, "2026-10-02T02:00:00", "2026-10-02T03:00:00");
        assert!(verify_rows(vec![past, next]).await.is_err());
        let a = row("occurrence", "a", CANONICAL, "2026-10-01T02:00:00", "2026-10-01T03:00:00");
        let b = row("exception", "b", CANONICAL, "2026-10-01T02:10:00", "2026-10-01T02:50:00");
        assert!(verify_rows(vec![a, b]).await.is_err(), "ambiguous stays ambiguous");
        // A series master carries no occurrence of its own; non-UTC rows are unusable.
        let master = row("seriesMaster", "m", CANONICAL, "2026-10-01T02:00:00", "2026-10-01T03:00:00");
        assert!(verify_rows(vec![master]).await.is_err());
        let mut zone = row("occurrence", "z", CANONICAL, "2026-10-01T02:00:00", "2026-10-01T03:00:00");
        zone["start"]["timeZone"] = json!("Pacific Standard Time");
        assert!(verify_rows(vec![zone]).await.is_err());
    }

    #[tokio::test]
    async fn recurring_alias_row_binds_to_the_same_meeting_before_it_counts() {
        let current = row("occurrence", "o", MODERN_ALIAS, "2026-10-01T02:00:00", "2026-10-01T03:00:00");
        let ok = verify_rows(vec![current.clone()]).await.unwrap();
        assert_eq!(bounds(&ok).1, "2026-10-01T03:00:00+00:00");
        // The provider says that alias is a DIFFERENT meeting: it links nothing,
        // so the meeting is ad-hoc and takes its own declared bounds.
        let mut other = metadata();
        other["id"] = json!("opaque/other");
        let s = Server::new(json!({"value":[current]}), json!({"value":[other]}), false).await;
        let accepted = s.verify().await.unwrap();
        assert_eq!(bounds(&accepted).0, "2026-10-01T02:20:00+00:00");
    }

    #[tokio::test]
    async fn unresolved_calendar_rows_never_make_a_meeting_ad_hoc() {
        // Alias lookup denied: the row may be this meeting's current occurrence.
        let current = row("occurrence", "o", MODERN_ALIAS, "2026-10-01T02:00:00", "2026-10-01T03:00:00");
        let s = Server::new(json!({"value":[current]}), json!({}), true).await;
        assert!(s.verify().await.is_err());
        // Detail read of a row without a join URL denied.
        let mut projected = row("occurrence", "p", CANONICAL, "2026-10-01T02:40:00", "2026-10-01T03:00:00");
        projected["onlineMeeting"] = Value::Null;
        let s = Server::with_event(json!({"value":[projected]}), json!({}), false, None, true).await;
        assert!(s.verify().await.is_err());
        // Alias budget exhausted: more unchecked candidates than lookups allowed.
        let rows: Vec<Value> = (0..=MAX_FLEX_ALIAS_LOOKUPS)
            .map(|i| {
                row("occurrence", &format!("r{i}"), &format!("https://teams.microsoft.com/l/meetup-join/other{i}"),
                    "2026-10-01T02:00:00", "2026-10-01T03:00:00")
            })
            .collect();
        let mut other = metadata();
        other["id"] = json!("opaque/other");
        let s = Server::new(json!({"value":rows}), json!({"value":[other]}), false).await;
        assert!(s.verify().await.is_err());
    }

    #[tokio::test]
    async fn edited_single_meeting_uses_the_calendar_events_bounds() {
        let edited = row("singleInstance", "e", CANONICAL, "2026-10-01T02:40:00", "2026-10-01T03:40:00");
        let accepted = verify_rows(vec![edited]).await.unwrap();
        assert_eq!(
            bounds(&accepted),
            ("2026-10-01T02:40:00+00:00".into(), "2026-10-01T03:40:00+00:00".into())
        );
        let a = row("singleInstance", "a", CANONICAL, "2026-10-01T02:40:00", "2026-10-01T03:40:00");
        let b = row("singleInstance", "b", CANONICAL, "2026-10-01T04:00:00", "2026-10-01T05:00:00");
        assert!(verify_rows(vec![a, b]).await.is_err(), "two edited events are ambiguous");
    }

    #[tokio::test]
    async fn ad_hoc_meeting_without_a_calendar_event_takes_its_own_bounds() {
        let accepted = verify_rows(vec![]).await.unwrap();
        // Declared 02:20-02:30 is shorter than the ad-hoc minimum: end is extended.
        assert_eq!(
            bounds(&accepted),
            ("2026-10-01T02:20:00+00:00".into(), "2026-10-01T05:20:00+00:00".into())
        );
        // Unrelated calendar entries (other meetings, in-person events) do not matter.
        let unrelated = row("singleInstance", "u", "https://evil.example/meet/1", "2026-10-01T02:00:00", "2026-10-01T03:00:00");
        let mut in_person = unrelated.clone();
        in_person["id"] = json!("v");
        in_person["onlineMeeting"] = Value::Null;
        in_person["isOnlineMeeting"] = json!(false);
        assert!(verify_rows(vec![unrelated, in_person]).await.is_ok());
        // Deterministic: status and capture compute the same occurrence.
        assert_eq!(
            bounds(&verify_rows(vec![]).await.unwrap()),
            bounds(&accepted)
        );
    }

    #[tokio::test]
    async fn ad_hoc_meeting_with_missing_or_odd_bounds_uses_creation_time() {
        let mut m = metadata();
        m["startDateTime"] = Value::Null;
        m["endDateTime"] = Value::Null;
        m["creationDateTime"] = json!("2026-10-01T02:10:00Z");
        let s = Server::new(json!({"value":[]}), json!({}), false).await;
        let accepted = s.verify_meeting(m.clone()).await.unwrap();
        assert_eq!(
            bounds(&accepted),
            ("2026-10-01T02:10:00+00:00".into(), "2026-10-02T02:10:00+00:00".into())
        );
        // A series-wide span (> 24h) is not an occurrence length: bounded default.
        m["startDateTime"] = json!("2026-10-01T02:00:00Z");
        m["endDateTime"] = json!("2026-12-01T02:00:00Z");
        let accepted = s.verify_meeting(m.clone()).await.unwrap();
        assert_eq!(bounds(&accepted).1, "2026-10-01T05:00:00+00:00");
        // Nothing to anchor on: refused.
        m["startDateTime"] = Value::Null;
        m["creationDateTime"] = Value::Null;
        assert!(s.verify_meeting(m).await.is_err());
    }

    const PLACEHOLDER: &str = "0001-01-01T00:00:00Z";

    #[tokio::test]
    async fn graph_meet_now_placeholder_bounds_anchor_on_creation_time() {
        let s = Server::new(json!({"value":[]}), json!({}), false).await;
        let mut m = metadata();
        m["startDateTime"] = json!(PLACEHOLDER);
        m["endDateTime"] = json!(PLACEHOLDER);
        m["creationDateTime"] = json!("2026-10-01T02:10:00Z");
        let accepted = s.verify_meeting(m.clone()).await.unwrap();
        assert_eq!(
            bounds(&accepted),
            ("2026-10-01T02:10:00+00:00".into(), "2026-10-02T02:10:00+00:00".into())
        );
        // Deterministic: a later call (the clock is not an input) agrees.
        assert_eq!(bounds(&s.verify_meeting(m.clone()).await.unwrap()), bounds(&accepted));
        assert!(accepted.transcript_end.is_none());
        // Placeholder start with a believable end still anchors on creation.
        m["endDateTime"] = json!("2026-10-01T03:00:00Z");
        assert_eq!(bounds(&s.verify_meeting(m.clone()).await.unwrap()).0, "2026-10-01T02:10:00+00:00");
        // A placeholder creation time is no anchor: everything implausible refuses.
        m["endDateTime"] = json!(PLACEHOLDER);
        m["creationDateTime"] = json!(PLACEHOLDER);
        assert!(s.verify_meeting(m.clone()).await.is_err());
        m["creationDateTime"] = Value::Null;
        assert!(s.verify_meeting(m).await.is_err());
    }

    #[tokio::test]
    async fn strict_proof_never_accepts_placeholder_or_implausible_bounds() {
        // A calendar event carrying the same placeholder bounds must not match.
        let mut ev = event(CANONICAL);
        ev["start"]["dateTime"] = json!("0001-01-01T00:00:00.0000000");
        ev["end"]["dateTime"] = json!("0001-01-01T03:00:00.0000000");
        let s = Server::new(json!({"value":[ev]}), json!({}), false).await;
        let mut m = metadata();
        m["startDateTime"] = json!(PLACEHOLDER);
        m["endDateTime"] = json!("0001-01-01T03:00:00Z");
        m["creationDateTime"] = json!("2026-10-01T02:10:00Z");
        // A linked event whose own bounds are the placeholder cannot supply a
        // window, and the meeting is calendar-managed, so it is refused.
        assert!(s.verify_meeting(m.clone()).await.is_err());
        // With no calendar event, the same meeting anchors on creation.
        let s = Server::new(json!({"value":[]}), json!({}), false).await;
        let accepted = s.verify_meeting(m).await.unwrap();
        assert!(accepted.start.unwrap().starts_with("2026-10-01"), "must not be year 1");
        // Implausible far-future and pre-2000 bounds are likewise ignored.
        for bad in ["1999-12-31T00:00:00Z", "2999-01-01T00:00:00Z"] {
            let mut m = metadata();
            m["startDateTime"] = json!(bad);
            m["creationDateTime"] = json!("2026-10-01T02:10:00Z");
            let accepted = s.verify_meeting(m).await.unwrap();
            assert!(accepted.start.unwrap().starts_with("2026-10-01"));
        }
    }

    #[tokio::test]
    async fn placeholder_calendar_row_without_projected_join_fails_closed() {
        let mut m = metadata();
        m["startDateTime"] = json!(PLACEHOLDER);
        m["endDateTime"] = json!(PLACEHOLDER);
        m["creationDateTime"] = json!("2026-10-01T02:10:00Z");
        let mut projected = projected_event();
        projected["start"]["dateTime"] = json!("0001-01-01T00:00:00.0000000");
        projected["end"]["dateTime"] = json!("0001-01-01T03:00:00.0000000");
        // Hydrates to THIS meeting: linked but unusable -> refused, not ad-hoc.
        let s = Server::with_event(
            json!({"value":[projected.clone()]}),
            json!({"value":[metadata()]}),
            false,
            Some(native_event(CANONICAL)),
            false,
        )
        .await;
        assert!(s.verify_meeting(m.clone()).await.is_err());
        // Detail read denied: cannot be checked -> refused.
        let s = Server::with_event(json!({"value":[projected.clone()]}), json!({}), false, None, true).await;
        assert!(s.verify_meeting(m.clone()).await.is_err());
        // Row without an id cannot be hydrated either -> refused.
        let mut no_id = projected.clone();
        no_id.as_object_mut().unwrap().remove("id");
        let s = Server::with_event(json!({"value":[no_id]}), json!({}), false, Some(native_event(CANONICAL)), false).await;
        assert!(s.verify_meeting(m.clone()).await.is_err());
        // Proven to be a DIFFERENT meeting's row: genuinely ad-hoc.
        let mut other_detail = native_event("https://teams.microsoft.com/l/meetup-join/other");
        other_detail["id"] = json!("event/fixture");
        let mut other = metadata();
        other["id"] = json!("opaque/other");
        other["joinWebUrl"] = json!("https://teams.microsoft.com/l/meetup-join/other");
        let s = Server::with_event(json!({"value":[projected]}), json!({"value":[other]}), false, Some(other_detail), false).await;
        assert!(s.verify_meeting(m).await.is_ok());
    }

    #[tokio::test]
    async fn fetch_refuses_placeholder_bounds() {
        let server = Server::new(json!({"value":[]}), json!({}), false).await;
        let mut meeting: Meeting = serde_json::from_value(metadata()).unwrap();
        meeting.start = Some("0001-01-01T00:00:00Z".into());
        meeting.end = Some("0001-01-01T03:00:00Z".into());
        assert!(matches!(
            fetch_verified_from(&Client::new(), "t", meeting, &server.base).await,
            Err(Error::InvalidMetadata)
        ));
    }

    #[tokio::test]
    async fn incomplete_or_malformed_calendar_never_authorizes() {
        let s = Server::new(
            json!({"value":[event(CANONICAL)],"@odata.nextLink":"https://evil.example/page"}),
            json!({}),
            false,
        )
        .await;
        assert!(s.verify().await.is_err());
        assert_eq!(s.count(), 1);
        let s = Server::new(json!({"nope":[]}), json!({}), false).await;
        assert!(s.verify().await.is_err());
    }

    #[tokio::test]
    async fn legacy_exact_match_is_returned_unchanged_without_a_search() {
        let s = Server::new(json!({"value":[event(CANONICAL)]}), json!({}), true).await;
        let accepted = s.verify().await.unwrap();
        assert_eq!(accepted.start.as_deref(), Some("2026-10-01T02:20:00Z"));
        assert_eq!(s.count(), 1);
    }
}

#[cfg(test)]
mod occurrence_diagnostic_tests {
    use super::*;
    #[test]
    fn diagnostics_exclude_private_provider_strings_and_preserve_static_classification() {
        let private =
            "https://teams.microsoft.com/meet/private?token=secret SUBJECT body actor@example.com";
        let d = OccurrenceDiagnostic::new(
            "alias",
            "provider_binding_failed",
            &Error::Graph {
                status: 403,
                code: Some(private.into()),
            },
            1,
            0,
        );
        let rendered = format!("{d:?}");
        assert_eq!(d.status, Some(403));
        assert_eq!(d.code, None);
        assert!(rendered.contains("provider_binding_failed"));
        for canary in ["https://", "private?", "secret", "SUBJECT", "actor@"] {
            assert!(!rendered.contains(canary));
        }
        let safe = OccurrenceDiagnostic::new(
            "alias",
            "provider_binding_failed",
            &Error::Graph {
                status: 404,
                code: Some("ErrorItemNotFound".into()),
            },
            1,
            0,
        );
        assert_eq!(safe.code.as_deref(), Some("ErrorItemNotFound"));
        assert_eq!(safe.eligible, 1);
        assert_eq!(safe.matches, 0);
        for code in [
            "",
            "../../secret",
            "code\nsecret",
            "PrivateAlphanumericSecret",
            "48e39fb9-114e-4ded-93f3-90ed9f28e731",
        ] {
            assert!(OccurrenceDiagnostic::new(
                "alias",
                "provider_binding_failed",
                &Error::Graph {
                    status: 403,
                    code: Some(code.into())
                },
                0,
                0
            )
            .code
            .is_none());
        }
    }
}
