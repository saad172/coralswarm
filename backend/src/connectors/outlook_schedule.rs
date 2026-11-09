//! Graph reads for Outlook compose capture v2.
//!
//! Finds the event a draft became (by the exact nonce in the fixed legacy named
//! property, never by subject/time), re-reads it by its IMMUTABLE id, and expands
//! a series into occurrences. Uses only the member's own delegated `Calendars.Read`
//! token. Nothing here writes to the calendar.
use chrono::{DateTime, NaiveDateTime, Utc};
use reqwest::{Client, Url};
use serde_json::Value;
use uuid::Uuid;

use super::microsoft;
use super::teams_targeted_transcripts::{join_target, SERIES_OVERRUN};

/// Fixed legacy named property the Outlook add-in CustomProperties bag lives in.
pub(crate) const PROPERTY: &str =
    "String {00020329-0000-0000-C000-000000000046} Name cecp-00000000-0000-4000-8000-5bfdfb1b6d07";
#[cfg(test)]
const KEY: &str = "coralswarmCaptureIntent";
const PREFER: &str = "outlook.timezone=\"UTC\", IdType=\"ImmutableId\"";
const EVENT_SELECT: &str = "id,type,isOrganizer,isCancelled,isOnlineMeeting,onlineMeetingProvider,start,end,onlineMeeting,recurrence,isAllDay";
const MAX_BODY: usize = 262_144;
const MAX_EVENT_LEN: chrono::Duration = chrono::Duration::hours(24);

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub(crate) enum Error {
    /// More than one event, malformed/duplicate marker, or an oversized answer.
    #[error("ambiguous")]
    Ambiguous,
    /// An event shape we refuse to capture (all-day, non-UTC, absurd length, ...).
    #[error("unsupported")]
    Unsupported,
    /// 404 on a read by id: the event no longer exists.
    #[error("gone")]
    Gone,
    /// Anything else: retry later, change nothing.
    #[error("unavailable")]
    Unavailable,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Kind {
    Single,
    SeriesMaster,
}

#[derive(Clone, Debug)]
pub(crate) struct EventView {
    /// Immutable Graph id (the series master's id for a series).
    pub id: String,
    pub kind: Kind,
    pub is_organizer: bool,
    pub is_cancelled: bool,
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    pub join_url: Option<String>,
    /// `recurrence.range.type` for a series (`endDate`, `numbered`, `noEnd`).
    pub range_type: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Instance {
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    pub cancelled: bool,
    /// A row we could not read in full (e.g. longer than a day). It is never tracked, but its
    /// start still bounds the previous occurrence's transcript window.
    pub boundary_only: bool,
}

/// One occurrence chosen to be tracked.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Occurrence {
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    /// Latest transcript creation time that can belong to this occurrence.
    pub transcript_end: DateTime<Utc>,
}

async fn read_json(response: reqwest::Response) -> Result<Value, Error> {
    // Limit actual received bytes, including chunked responses.
    let mut response = response;
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|_| Error::Unavailable)? {
        if bytes.len() + chunk.len() > MAX_BODY {
            return Err(Error::Unavailable);
        }
        bytes.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&bytes).map_err(|_| Error::Unavailable)
}

async fn get(http: &Client, token: &str, url: &Url) -> Result<Value, Error> {
    let response = microsoft::graph_send(|| {
        http.get(url.clone())
            .bearer_auth(token)
            .header("Prefer", PREFER)
    })
    .await
    .map_err(|_| Error::Unavailable)?;
    let status = response.status();
    if status == reqwest::StatusCode::NOT_FOUND {
        return Err(Error::Gone);
    }
    if !status.is_success() {
        return Err(Error::Unavailable);
    }
    read_json(response).await
}

fn datetime(row: &Value, key: &str) -> Result<DateTime<Utc>, Error> {
    if row
        .pointer(&format!("/{key}/timeZone"))
        .and_then(Value::as_str)
        != Some("UTC")
    {
        return Err(Error::Unsupported);
    }
    let s = row
        .pointer(&format!("/{key}/dateTime"))
        .and_then(Value::as_str)
        .ok_or(Error::Unsupported)?;
    DateTime::parse_from_rfc3339(s)
        .map(|d| d.with_timezone(&Utc))
        .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f").map(|d| d.and_utc()))
        .map_err(|_| Error::Unsupported)
}

/// Parse one event row. `Err(Unsupported)` for shapes we never capture.
fn parse_event(row: &Value) -> Result<EventView, Error> {
    let kind = match row.get("type").and_then(Value::as_str) {
        Some("singleInstance") => Kind::Single,
        Some("seriesMaster") => Kind::SeriesMaster,
        _ => return Err(Error::Unsupported),
    };
    if row.get("isAllDay").and_then(Value::as_bool) == Some(true) {
        return Err(Error::Unsupported);
    }
    let id = row
        .get("id")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty() && id.len() <= 2048 && !id.chars().any(|c| c.is_whitespace() || c.is_control()))
        .ok_or(Error::Unsupported)?
        .to_owned();
    let start = datetime(row, "start")?;
    let end = datetime(row, "end")?;
    if end <= start || end - start > MAX_EVENT_LEN {
        return Err(Error::Unsupported);
    }
    let join_url = row
        .pointer("/onlineMeeting/joinUrl")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_owned);
    if let Some(link) = &join_url {
        // Teams links only (host + path shape); anything else is not capturable.
        join_target(link).map_err(|_| Error::Unsupported)?;
    }
    Ok(EventView {
        id,
        kind,
        is_organizer: row.get("isOrganizer").and_then(Value::as_bool) == Some(true),
        is_cancelled: row.get("isCancelled").and_then(Value::as_bool) == Some(true),
        start,
        end,
        join_url,
        range_type: row
            .pointer("/recurrence/range/type")
            .and_then(Value::as_str)
            .map(str::to_owned),
    })
}

/// Pages of event rows read while looking for a nonce (Graph's `contains` filter is not
/// relied upon to narrow the list, so other tagged events may come back).
const MAX_DISCOVERY_PAGES: usize = 5;

/// Counts only: what one discovery attempt saw. Logged without any content or id.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DiscoveryStats {
    pub rows: usize,
    pub has_next_link: bool,
    pub property_matched: usize,
    pub bag_parsed: usize,
    pub nonce_matched: usize,
}

/// Property ids compare case-insensitively and ignore whitespace runs: Graph does not
/// promise to echo the id exactly as it was queried.
fn same_property(id: &str) -> bool {
    let norm = |s: &str| s.split_whitespace().collect::<Vec<_>>().join(" ").to_ascii_lowercase();
    norm(id) == norm(PROPERTY)
}

/// Find THE event carrying this nonce. `Ok(None)` = not saved yet / not visible yet.
/// Events carrying a different nonce (or an unreadable bag) are ignored, never ambiguity.
pub(crate) async fn discover(
    http: &Client,
    token: &str,
    graph: &str,
    nonce: Uuid,
) -> Result<Option<EventView>, Error> {
    let mut url = Url::parse(&format!("{graph}/me/events")).map_err(|_| Error::Unavailable)?;
    url.query_pairs_mut()
        .append_pair(
            "$filter",
            &format!(
                "singleValueExtendedProperties/Any(ep: ep/id eq '{PROPERTY}' and contains(ep/value, '{nonce}'))"
            ),
        )
        .append_pair(
            "$expand",
            &format!("singleValueExtendedProperties($filter=id eq '{PROPERTY}')"),
        )
        .append_pair("$select", EVENT_SELECT)
        .append_pair("$top", "50");
    let mut stats = DiscoveryStats::default();
    let mut found: Vec<Value> = Vec::new();
    let mut next = Some(url);
    let mut pages = 0;
    let mut failure = None;
    while let Some(page_url) = next.take() {
        pages += 1;
        let data = match get(http, token, &page_url).await {
            Ok(d) => d,
            // A 404 on the collection is a malformed deployment, not "event gone".
            Err(Error::Gone) => {
                failure = Some(Error::Unavailable);
                break;
            }
            Err(e) => {
                failure = Some(e);
                break;
            }
        };
        scan_page(&data, nonce, &mut stats, &mut found);
        stats.has_next_link = data.get("@odata.nextLink").is_some();
        // Keep going even after a match: a duplicate of our nonce on a later page must be seen.
        if let Some(link) = data.get("@odata.nextLink").and_then(Value::as_str) {
            // Only ever follow a link back to the Graph host we were already talking to.
            match Url::parse(link) {
                Ok(u) if link.starts_with(graph) && pages < MAX_DISCOVERY_PAGES => next = Some(u),
                _ => {
                    failure = Some(Error::Unavailable);
                    break;
                }
            }
        }
    }
    let outcome = match failure {
        Some(e) => Err(e),
        None => resolve_matches(&found, nonce),
    };
    tracing::info!(
        pages,
        rows = stats.rows,
        has_next_link = stats.has_next_link,
        property_matched = stats.property_matched,
        bag_parsed = stats.bag_parsed,
        nonce_matched = stats.nonce_matched,
        outcome = match &outcome {
            Ok(Some(_)) => "found",
            Ok(None) => "none",
            Err(Error::Ambiguous) => "ambiguous",
            Err(Error::Unsupported) => "unsupported",
            Err(_) => "unavailable",
        },
        "outlook schedule: discovery attempt"
    );
    outcome
}

/// Collect the rows of one page that carry OUR nonce, counting what else was seen.
fn scan_page(data: &Value, nonce: Uuid, stats: &mut DiscoveryStats, found: &mut Vec<Value>) {
    #[derive(serde::Deserialize)]
    struct Bag {
        #[serde(rename = "coralswarmCaptureIntent")]
        nonce: String,
    }
    let Some(rows) = data.get("value").and_then(Value::as_array) else {
        return;
    };
    for row in rows {
        stats.rows += 1;
        let Some(props) = row.get("singleValueExtendedProperties").and_then(Value::as_array) else {
            continue;
        };
        for prop in props {
            if !prop.get("id").and_then(Value::as_str).is_some_and(same_property) {
                continue;
            }
            stats.property_matched += 1;
            let Some(raw) = prop.get("value").and_then(Value::as_str).filter(|r| r.len() <= 8192) else {
                continue;
            };
            // A duplicate key makes serde fail; extra keys (the intent id) are tolerated.
            let Ok(bag) = serde_json::from_str::<Bag>(raw) else {
                continue;
            };
            stats.bag_parsed += 1;
            if bag.nonce == nonce.to_string() {
                stats.nonce_matched += 1;
                found.push(row.clone());
            }
        }
    }
}

/// Zero matches: keep waiting. One event: it. Two or more DISTINCT events with our nonce: ambiguous.
fn resolve_matches(found: &[Value], _nonce: Uuid) -> Result<Option<EventView>, Error> {
    let mut ids: Vec<&str> = found.iter().filter_map(|r| r.get("id").and_then(Value::as_str)).collect();
    ids.sort_unstable();
    ids.dedup();
    if found.iter().any(|r| r.get("id").and_then(Value::as_str).is_none()) {
        return Err(Error::Unsupported);
    }
    match ids.len() {
        0 => Ok(None),
        1 => parse_event(&found[0]).map(Some),
        _ => Err(Error::Ambiguous),
    }
}

/// Re-read a tracked event by its immutable id.
pub(crate) async fn get_event(
    http: &Client,
    token: &str,
    graph: &str,
    id: &str,
) -> Result<EventView, Error> {
    let mut url = Url::parse(&format!("{graph}/me/events/{}", urlencoding::encode(id)))
        .map_err(|_| Error::Unavailable)?;
    url.query_pairs_mut().append_pair("$select", EVENT_SELECT);
    let data = get(http, token, &url).await?;
    if data.get("id").and_then(Value::as_str) != Some(id) {
        // Graph answered with a different object than the one asked for.
        return Err(Error::Ambiguous);
    }
    parse_event(&data)
}

/// Convert any event id the pane has (REST id) into the immutable id.
pub(crate) async fn immutable_id(
    http: &Client,
    token: &str,
    graph: &str,
    id: &str,
) -> Result<Option<String>, Error> {
    if id.is_empty() || id.len() > 2048 || id.chars().any(|c| c.is_whitespace() || c.is_control()) {
        return Ok(None);
    }
    let mut url = Url::parse(&format!("{graph}/me/events/{}", urlencoding::encode(id)))
        .map_err(|_| Error::Unavailable)?;
    url.query_pairs_mut().append_pair("$select", "id");
    match get(http, token, &url).await {
        Ok(v) => Ok(v
            .get("id")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(str::to_owned)),
        Err(Error::Gone) => Ok(None),
        Err(e) => Err(e),
    }
}

/// Occurrences/exceptions of a series overlapping `[from, to]`, cancelled ones flagged.
pub(crate) async fn instances(
    http: &Client,
    token: &str,
    graph: &str,
    master_id: &str,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> Result<Vec<Instance>, Error> {
    let mut url = Url::parse(&format!(
        "{graph}/me/events/{}/instances",
        urlencoding::encode(master_id)
    ))
    .map_err(|_| Error::Unavailable)?;
    url.query_pairs_mut()
        .append_pair("startDateTime", &from.to_rfc3339())
        .append_pair("endDateTime", &to.to_rfc3339())
        .append_pair("$select", "id,type,start,end,isCancelled")
        .append_pair("$orderby", "start/dateTime")
        .append_pair("$top", "100");
    let data = get(http, token, &url).await?;
    // A truncated list can hide the occurrence that bounds a transcript window: fail closed.
    if data.get("@odata.nextLink").is_some() {
        return Err(Error::Ambiguous);
    }
    let rows = data
        .get("value")
        .and_then(Value::as_array)
        .ok_or(Error::Ambiguous)?;
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let cancelled = row.get("isCancelled").and_then(Value::as_bool) == Some(true);
        // Without a readable start the row cannot even serve as a boundary: refuse the read.
        let start = datetime(row, "start")?;
        match datetime(row, "end") {
            Ok(end) if end > start && end - start <= MAX_EVENT_LEN => {
                out.push(Instance { start, end, cancelled, boundary_only: false })
            }
            _ => out.push(Instance { start, end: start, cancelled: false, boundary_only: true }),
        }
    }
    out.sort_by_key(|i| i.start);
    Ok(out)
}

/// The occurrence to track now: the earliest non-cancelled one whose transcript
/// window has not closed. Same bound as `verify_occurrence`: the transcript may land
/// after the scheduled end, but never at or after the next occurrence's start.
pub(crate) fn select_occurrence(instances: &[Instance], now: DateTime<Utc>) -> Option<Occurrence> {
    // Cancelled rows are neither tracked nor boundaries (no meeting happens); unreadable rows
    // are boundaries only.
    let bounds: Vec<&Instance> = instances.iter().filter(|i| !i.cancelled).collect();
    for (idx, inst) in bounds.iter().enumerate() {
        if inst.boundary_only {
            continue;
        }
        let mut upper = inst.end + SERIES_OVERRUN;
        if let Some(next) = bounds.get(idx + 1) {
            upper = upper.min(next.start).max(inst.end);
        }
        if upper > now {
            return Some(Occurrence {
                start: inst.start,
                end: inst.end,
                transcript_end: upper,
            });
        }
    }
    None
}

/// Look-ahead windows used to find the next occurrence (days after now).
pub(crate) const LOOKAHEAD_DAYS: [i64; 3] = [7, 45, 400];

/// Result of looking for a series' current/next occurrence.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum SeriesNext {
    Found(Occurrence),
    /// No occurrence in the whole look-ahead and the series has a bounded range.
    Ended,
    /// No occurrence found but the series is open-ended: look again later.
    NoneYet,
}

pub(crate) async fn next_occurrence(
    http: &Client,
    token: &str,
    graph: &str,
    master: &EventView,
    now: DateTime<Utc>,
) -> Result<SeriesNext, Error> {
    let from = now - chrono::Duration::hours(3);
    for days in LOOKAHEAD_DAYS {
        let list = instances(http, token, graph, &master.id, from, now + chrono::Duration::days(days)).await?;
        if let Some(found) = select_occurrence(&list, now) {
            return Ok(SeriesNext::Found(found));
        }
    }
    Ok(if master.range_type.as_deref() == Some("noEnd") {
        SeriesNext::NoneYet
    } else {
        SeriesNext::Ended
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn row(nonce: Uuid) -> Value {
        let start = Utc::now() + chrono::Duration::days(1);
        json!({"id":"E1","type":"singleInstance","isOrganizer":true,"isCancelled":false,
          "start":{"dateTime":start.to_rfc3339(),"timeZone":"UTC"},
          "end":{"dateTime":(start+chrono::Duration::hours(1)).to_rfc3339(),"timeZone":"UTC"},
          "onlineMeeting":{"joinUrl":"https://teams.microsoft.com/l/meetup-join/x"},
          "singleValueExtendedProperties":[{"id":PROPERTY,"value":json!({KEY:nonce.to_string()}).to_string()}]})
    }

    /// One page through the same scan + resolution `discover` uses.
    fn parse_discovery(data: &Value, nonce: Uuid) -> Result<Option<EventView>, Error> {
        let mut stats = DiscoveryStats::default();
        let mut found = Vec::new();
        scan_page(data, nonce, &mut stats, &mut found);
        resolve_matches(&found, nonce)
    }
    fn tagged(id: &str, nonce: Uuid) -> Value {
        let mut r = row(nonce);
        r["id"] = json!(id);
        r
    }

    #[test]
    fn exact_nonce_single_event() {
        let n = Uuid::new_v4();
        let ev = parse_discovery(&json!({"value":[row(n)]}), n).unwrap().unwrap();
        assert_eq!(ev.id, "E1");
        assert!(ev.is_organizer && !ev.is_cancelled && ev.kind == Kind::Single);
        assert!(ev.join_url.is_some());
        assert!(parse_discovery(&json!({"value":[]}), n).unwrap().is_none());
        // The same event twice is still one event.
        assert!(parse_discovery(&json!({"value":[row(n),row(n)]}), n).unwrap().is_some());
    }

    #[test]
    fn other_events_are_never_ambiguity() {
        let n = Uuid::new_v4();
        // Another event's nonce only: keep waiting.
        assert!(parse_discovery(&json!({"value":[tagged("OTHER", Uuid::new_v4())]}), n).unwrap().is_none());
        // Ours alongside others, in any order: ours.
        let mixed = json!({"value":[tagged("OTHER", Uuid::new_v4()), tagged("MINE", n), tagged("OTHER2", Uuid::new_v4())]});
        assert_eq!(parse_discovery(&mixed, n).unwrap().unwrap().id, "MINE");
        // A row without the property, or with an unreadable bag, is ignored.
        let mut junk = tagged("JUNK", n);
        junk["singleValueExtendedProperties"][0]["value"] = json!("not json");
        let mut bare = tagged("BARE", n);
        bare.as_object_mut().unwrap().remove("singleValueExtendedProperties");
        assert!(parse_discovery(&json!({"value":[junk, bare]}), n).unwrap().is_none());
        // Our nonce on two DIFFERENT events is the only ambiguity.
        let two = json!({"value":[tagged("A", n), tagged("B", n)]});
        assert_eq!(parse_discovery(&two, n).unwrap_err(), Error::Ambiguous);
    }

    #[test]
    fn property_id_compares_case_insensitively() {
        let n = Uuid::new_v4();
        let mut r = row(n);
        r["singleValueExtendedProperties"][0]["id"] =
            json!("string {00020329-0000-0000-c000-000000000046} name cecp-00000000-0000-4000-8000-5bfdfb1b6d07");
        assert!(parse_discovery(&json!({"value":[r]}), n).unwrap().is_some());
        let mut r = row(n);
        r["singleValueExtendedProperties"][0]["id"] = json!("String {00020329-0000-0000-C000-000000000046} Name other");
        assert!(parse_discovery(&json!({"value":[r]}), n).unwrap().is_none());
    }

    #[test]
    fn the_panes_intent_id_key_next_to_the_nonce_is_tolerated() {
        // The pane stores `coralswarmCaptureIntentId` in the same CustomProperties bag.
        let n = Uuid::new_v4();
        let mut r = row(n);
        r["singleValueExtendedProperties"][0]["value"] = json!(json!({
            KEY: n.to_string(),
            "coralswarmCaptureIntentId": Uuid::new_v4().to_string()
        })
        .to_string());
        assert!(parse_discovery(&json!({"value":[r]}), n).unwrap().is_some());
    }

    #[test]
    fn a_duplicate_marker_key_never_matches() {
        let n = Uuid::new_v4();
        let mut r = row(n);
        r["singleValueExtendedProperties"][0]["value"] = json!(format!(
            "{{\"{KEY}\":\"{}\",\"{KEY}\":\"{n}\"}}",
            Uuid::new_v4()
        ));
        assert!(parse_discovery(&json!({"value":[r]}), n).unwrap().is_none());
    }

    /// Serve `/me/events` pages in order (later calls repeat the last page). `{BASE}` in a
    /// body is replaced with the server's own address so `@odata.nextLink` can point back.
    async fn pages(bodies: Vec<Value>) -> String {
        use axum::{routing::get, Json, Router};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let texts: Vec<String> = bodies.iter().map(|b| b.to_string().replace("{BASE}", &base)).collect();
        let state = std::sync::Arc::new(std::sync::Mutex::new((texts, 0usize)));
        let app = Router::new().route(
            "/me/events",
            get(move || {
                let state = state.clone();
                async move {
                    let mut g = state.lock().unwrap();
                    let i = g.1.min(g.0.len() - 1);
                    g.1 += 1;
                    Json(serde_json::from_str::<Value>(&g.0[i]).unwrap())
                }
            }),
        );
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        base
    }

    #[tokio::test]
    async fn discover_pages_past_foreign_rows_and_treats_an_unfinished_scan_as_retry() {
        let n = Uuid::new_v4();
        let next = || json!("{BASE}/me/events?page=2");
        let run = |base: String| async move { discover(&Client::new(), "t", &base, n).await };
        // Another event's nonce first, ours on page two: found, not ambiguous.
        let base = pages(vec![
            json!({"value":[tagged("OTHER", Uuid::new_v4())],"@odata.nextLink":next()}),
            json!({"value":[tagged("MINE", n)]}),
        ])
        .await;
        assert_eq!(run(base).await.unwrap().unwrap().id, "MINE");
        // Foreign rows only, no more pages: not saved yet.
        let base = pages(vec![json!({"value":[tagged("OTHER", Uuid::new_v4())]})]).await;
        assert!(run(base).await.unwrap().is_none());
        // Endless foreign pages: stop at the cap with a RETRY error, never a terminal one.
        let base = pages(vec![json!({"value":[tagged("OTHER", Uuid::new_v4())],"@odata.nextLink":next()})]).await;
        assert_eq!(run(base).await.unwrap_err(), Error::Unavailable);
        // A next link to another host is never followed.
        let base = pages(vec![json!({"value":[],"@odata.nextLink":"https://evil.example/me/events"})]).await;
        assert_eq!(run(base).await.unwrap_err(), Error::Unavailable);
        // Ours on page 1 and a copy of it on a different event on page 2: ambiguous.
        let base = pages(vec![
            json!({"value":[tagged("A", n)],"@odata.nextLink":next()}),
            json!({"value":[tagged("B", n)]}),
        ])
        .await;
        assert_eq!(run(base).await.unwrap_err(), Error::Ambiguous);
        // A match, then pages that never end: cannot rule out a duplicate, so retry.
        let base = pages(vec![
            json!({"value":[tagged("A", n)],"@odata.nextLink":next()}),
            json!({"value":[],"@odata.nextLink":next()}),
        ])
        .await;
        assert_eq!(run(base).await.unwrap_err(), Error::Unavailable);
        // Ours on two events: the only ambiguity.
        let base = pages(vec![json!({"value":[tagged("A", n), tagged("B", n)]})]).await;
        assert_eq!(run(base).await.unwrap_err(), Error::Ambiguous);
        // Ours next to a foreign event, lowercase property id: tracked.
        let mut mine = tagged("MINE", n);
        mine["singleValueExtendedProperties"][0]["id"] = json!(PROPERTY.to_ascii_lowercase());
        let base = pages(vec![json!({"value":[tagged("OTHER", Uuid::new_v4()), mine]})]).await;
        assert_eq!(run(base).await.unwrap().unwrap().id, "MINE");
    }

    #[test]
    fn unsupported_shapes_are_refused_but_series_master_is_allowed() {
        let n = Uuid::new_v4();
        let mut r = row(n);
        r["type"] = json!("occurrence");
        assert_eq!(parse_event(&r).unwrap_err(), Error::Unsupported);
        let mut r = row(n);
        r["type"] = json!("seriesMaster");
        r["recurrence"] = json!({"range":{"type":"noEnd"}});
        let ev = parse_event(&r).unwrap();
        assert_eq!(ev.kind, Kind::SeriesMaster);
        assert_eq!(ev.range_type.as_deref(), Some("noEnd"));
        let mut r = row(n);
        r["start"]["timeZone"] = json!("Pacific Standard Time");
        assert_eq!(parse_event(&r).unwrap_err(), Error::Unsupported);
        let mut r = row(n);
        r["onlineMeeting"]["joinUrl"] = json!("https://evil.example/l/meetup-join/x");
        assert_eq!(parse_event(&r).unwrap_err(), Error::Unsupported);
        let mut r = row(n);
        r["isAllDay"] = json!(true);
        assert_eq!(parse_event(&r).unwrap_err(), Error::Unsupported);
    }

    fn inst(start_h: i64, len_h: i64, cancelled: bool, now: DateTime<Utc>) -> Instance {
        Instance {
            start: now + chrono::Duration::hours(start_h),
            end: now + chrono::Duration::hours(start_h + len_h),
            cancelled,
            boundary_only: false,
        }
    }

    #[test]
    fn select_skips_cancelled_and_bounds_by_next_occurrence() {
        let now = Utc::now();
        // Yesterday's finished, today's cancelled, tomorrow's live, then the one after.
        let list = vec![
            inst(-24, 1, false, now),
            inst(0, 1, true, now),
            inst(24, 1, false, now),
            inst(48, 1, false, now),
        ];
        let o = select_occurrence(&list, now).unwrap();
        assert_eq!(o.start, now + chrono::Duration::hours(24));
        assert_eq!(o.transcript_end, o.end + SERIES_OVERRUN);
        // Back-to-back occurrences: window cannot reach the next one's start.
        let list = vec![inst(1, 1, false, now), inst(2, 1, false, now)];
        let o = select_occurrence(&list, now).unwrap();
        assert_eq!(o.transcript_end, now + chrono::Duration::hours(2));
    }

    #[test]
    fn select_rotates_once_the_transcript_window_has_closed() {
        let now = Utc::now();
        // Ended 5h ago (window closed 3h ago) -> next one.
        let list = vec![inst(-6, 1, false, now), inst(18, 1, false, now)];
        assert_eq!(select_occurrence(&list, now).unwrap().start, now + chrono::Duration::hours(18));
        // Ended 30 min ago: still inside the 2h overrun window -> stay on it.
        let list = vec![inst(-2, 1, false, now) , inst(18, 1, false, now)];
        let o = select_occurrence(&[Instance{start: now-chrono::Duration::minutes(90), end: now-chrono::Duration::minutes(30), cancelled:false, boundary_only:false}, list[1].clone()], now).unwrap();
        assert_eq!(o.end, now - chrono::Duration::minutes(30));
        assert!(select_occurrence(&[inst(-6, 1, false, now)], now).is_none());
        assert!(select_occurrence(&[inst(1, 1, true, now)], now).is_none());
    }

    #[test]
    fn an_unreadable_row_still_bounds_the_previous_occurrence() {
        let now = Utc::now();
        let mut boundary = inst(2, 1, false, now);
        boundary.end = boundary.start;
        boundary.boundary_only = true;
        // Occurrence ends in 1h; its 2h overrun would reach 3h, but an unreadable row starts at 2h.
        let list = vec![inst(0, 1, false, now), boundary.clone()];
        let o = select_occurrence(&list, now - chrono::Duration::hours(0)).unwrap();
        assert_eq!(o.transcript_end, now + chrono::Duration::hours(2));
        // The boundary row itself is never selected.
        assert!(select_occurrence(&[boundary], now).is_none());
    }

    async fn serve(body: serde_json::Value, seen: std::sync::Arc<std::sync::Mutex<Option<String>>>) -> String {
        use axum::{extract::OriginalUri, routing::get, Json, Router};
        let app = Router::new().route(
            "/me/events/{id}/instances",
            get(move |OriginalUri(uri): OriginalUri| {
                let body = body.clone();
                let seen = seen.clone();
                async move {
                    *seen.lock().unwrap() = Some(uri.to_string());
                    Json(body)
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("http://{addr}")
    }

    #[tokio::test]
    async fn instances_orders_fails_closed_on_paging_and_keeps_unreadable_rows_as_boundaries() {
        let now = Utc::now();
        let row = |h: i64, len: i64| json!({"start":{"dateTime":(now+chrono::Duration::hours(h)).to_rfc3339(),"timeZone":"UTC"},
            "end":{"dateTime":(now+chrono::Duration::hours(h+len)).to_rfc3339(),"timeZone":"UTC"},"isCancelled":false});
        let call = |base: String| async move {
            instances(&Client::new(), "t", &base, "M", now, now + chrono::Duration::days(7)).await
        };
        // Ordered request, readable rows, a >24h row becomes a boundary.
        let seen = Default::default();
        let base = serve(json!({"value":[row(5, 1), row(1, 1), row(3, 30)]}), std::sync::Arc::clone(&seen)).await;
        let list = call(base).await.unwrap();
        let q = seen.lock().unwrap().clone().unwrap();
        assert!(q.contains("%24orderby=start%2FdateTime") || q.contains("$orderby=start/dateTime"), "{q}");
        assert_eq!(list.len(), 3);
        assert!(list[1].boundary_only && !list[0].boundary_only && !list[2].boundary_only);
        assert!(list.windows(2).all(|w| w[0].start <= w[1].start));
        // A second page fails closed.
        let base = serve(json!({"value":[row(1, 1)],"@odata.nextLink":"https://x/next"}), Default::default()).await;
        assert_eq!(call(base).await.unwrap_err(), Error::Ambiguous);
        // A row with no readable start fails closed instead of being dropped.
        let base = serve(json!({"value":[{"isCancelled":false}]}), Default::default()).await;
        assert_eq!(call(base).await.unwrap_err(), Error::Unsupported);
    }
}
