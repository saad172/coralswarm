//! Generic read-only capture through Composio REST v3.1. Every toolkit-specific
//! tool, path and paging style comes from a checked-in recipe (`recipe.rs`,
//! `recipes/*.json`); this engine owns execution, paging, bounds, provenance and
//! identity. There is no user-provided tool slug, GraphQL operation, endpoint or
//! write-back path, and no capture of tool output without a recipe.
//!
//! Capture is "connect and done": a connection is polled for everything the
//! account can read workspace-wide that changed since a watermark set at
//! connect time. Nothing older is fetched for ingestion.
pub mod budget;
pub mod recipe;
use chrono::{DateTime, Duration, Utc};
pub use recipe::Recipe;
use recipe::{CaptureHints, ChunkHint, TitleFacts, 
    Bindings, BodyFormat, Call, ChangeStrategy, ChildList, ChildPaging, CursorPaging, Fields, MatchCompare, More,
    Pointer, Position, Render, Stream, StreamPaging,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::borrow::Cow;

pub const BASE: &str = "https://backend.composio.dev/api/v3.1";
/// The broker base a client talks to: `override_base` (trimmed, no trailing
/// slash) when it is non-blank, else [`BASE`].
#[cfg(any(test, feature = "e2e"))]
fn resolve_base(override_base: Option<&str>) -> String {
    match override_base.map(|b| b.trim().trim_end_matches('/')) {
        Some(b) if !b.is_empty() => b.into(),
        _ => BASE.into(),
    }
}
/// e2e only: `COMPOSIO_API_BASE` points the client at the in-runner fake
/// (`e2e/stack/fakes.mjs`). Any other build ignores it, so the broker key can
/// only ever reach Composio.
fn configured_base() -> String {
    #[cfg(feature = "e2e")]
    {
        resolve_base(std::env::var("COMPOSIO_API_BASE").ok().as_deref())
    }
    #[cfg(not(feature = "e2e"))]
    {
        BASE.into()
    }
}
pub const PAGE_SIZE: usize = 25;
/// Each poll re-reads this far before the watermark; content-hash dedupe makes
/// the re-read items no-ops.
pub const OVERLAP_MINUTES: i64 = 5;
/// Provider clocks and Jira's minute-granular relative JQL may place `since` a
/// little earlier than ours; only an item older than this is a filter failure.
const FILTER_SLACK_SECONDS: i64 = 120;
/// The self-check reads (never ingests) one page per stream from this window,
/// so a quiet workspace still exercises the recipe's paths.
pub const PROBE_WINDOW_DAYS: i64 = 30;
const MAX_CHILD_PAGES: usize = 100;
const MAX_CAPTURE_ITEMS: usize = 1000;
const MAX_CAPTURE_BYTES: usize = 8 * 1024 * 1024;
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Composio is not configured")]
    NotConfigured,
    #[error("Reconnect this account")]
    Reconnect,
    #[error("Provider permission denied")]
    PermissionDenied,
    #[error("The connected account no longer exists")]
    Gone,
    #[error("Retry after {0} seconds")]
    RateLimited(u64),
    #[error("Provider is temporarily unavailable")]
    Upstream,
    #[error("Provider response did not match the pinned read contract: {0}")]
    Contract(&'static str),
}
#[derive(Clone)]
pub struct Client {
    http: reqwest::Client,
    key: String,
    base: String,
    budget: Option<std::sync::Arc<budget::CallBudget>>,
}
impl Client {
    fn transport() -> Result<reqwest::Client, Error> {
        // Broker authentication must never follow a Location to another host.
        reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| Error::Upstream)
    }
    pub fn configured() -> Result<Self, Error> {
        let key = std::env::var("COMPOSIO_API_KEY")
            .ok()
            .filter(|k| !k.trim().is_empty())
            .ok_or(Error::NotConfigured)?;
        Ok(Self {
            http: Self::transport()?,
            key,
            base: configured_base(),
            budget: None,
        })
    }
    /// Every request from this client first waits for room in `budget`.
    pub fn with_budget(mut self, budget: std::sync::Arc<budget::CallBudget>) -> Self {
        self.budget = Some(budget);
        self
    }
    /// A client for a local mock broker. Only tests construct this; the
    /// deployment base and key come from `configured`.
    #[cfg(any(test, feature = "test-support"))]
    #[doc(hidden)]
    pub fn for_mock_broker(base: &str) -> Result<Self, Error> {
        Ok(Self {
            http: Self::transport()?,
            key: "mock-broker-key".into(),
            base: base.trim_end_matches('/').into(),
            budget: None,
        })
    }
    pub async fn request(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<Value>,
    ) -> Result<Value, Error> {
        if let Some(budget) = &self.budget {
            budget.acquire().await;
        }
        let mut req = self
            .http
            .request(method, format!("{}{path}", self.base))
            .header("x-api-key", &self.key)
            .timeout(std::time::Duration::from_secs(30));
        if let Some(body) = body {
            req = req.json(&body);
        }
        let res = req.send().await.map_err(|_| Error::Upstream)?;
        match res.status().as_u16() {
            429 => {
                let delay = res
                    .headers()
                    .get("retry-after")
                    .and_then(|h| h.to_str().ok())
                    .and_then(|h| h.parse().ok())
                    .unwrap_or(60);
                return Err(Error::RateLimited(delay));
            }
            401 => return Err(Error::Reconnect),
            403 => return Err(Error::PermissionDenied),
            404 => return Err(Error::Gone),
            s if !(200..300).contains(&s) => return Err(Error::Upstream),
            _ => {}
        }
        // Raw upstream error/credential fields are never logged or sent to clients.
        let mut res = res;
        let mut bytes = Vec::new();
        while let Some(chunk) = res.chunk().await.map_err(|_| Error::Upstream)? {
            if bytes.len() + chunk.len() > 4 * 1024 * 1024 {
                return Err(Error::Contract("response size bound"));
            }
            bytes.extend_from_slice(&chunk);
        }
        serde_json::from_slice(&bytes).map_err(|_| Error::Contract("JSON"))
    }
    /// Executes one recipe-owned call. Arguments are the recipe's fixed template
    /// with typed slots filled; nothing else can reach the broker.
    async fn execute(
        &self,
        recipe: &Recipe,
        account: &str,
        user: &str,
        call: &Call,
        bindings: &Bindings<'_>,
    ) -> Result<Value, Error> {
        if !recipe.read_tools.contains(&call.tool) {
            return Err(Error::Contract("read tool allowlist"));
        }
        let mut args = call.args.render(bindings).map_err(Error::Contract)?;
        if let Some(arguments) = args.as_object_mut() {
            arguments.retain(|_, value| !value.is_null());
        }
        let response = self
            .request(
                reqwest::Method::POST,
                &format!("/tools/execute/{}", call.tool),
                Some(json!({"connected_account_id":account,"user_id":user,"version":recipe.version,"arguments":args})),
            )
            .await?;
        if response.get("successful").and_then(Value::as_bool) != Some(true) {
            let status = response
                .pointer("/error/status_code")
                .or_else(|| response.pointer("/error/status"))
                .or_else(|| response.pointer("/data/status_code"))
                .and_then(Value::as_u64);
            return Err(match status {
                Some(401) => Error::Reconnect,
                Some(403) => Error::PermissionDenied,
                Some(404) => Error::Gone,
                Some(429) => Error::RateLimited(60),
                _ => Error::Upstream,
            });
        }
        let data = response
            .get("data")
            .cloned()
            .ok_or(Error::Contract("data"))?;
        let data = match data {
            Value::String(s) => {
                serde_json::from_str(&s).map_err(|_| Error::Contract("data JSON"))?
            }
            v => v,
        };
        if has_provider_errors(&data) {
            return Err(Error::Contract("provider errors"));
        }
        Ok(data)
    }
    /// Derive immutable workspace identity from the authenticated provider, never
    /// a browser-supplied namespace or a mutable key.
    pub async fn namespace(&self, recipe: &Recipe, account: &str, user: &str) -> Result<String, Error> {
        let none = Bindings::default();
        let id = match &recipe.namespace.source {
            recipe::NamespaceSource::Lookup { call, id } => {
                let data = self.execute(recipe, account, user, call, &none).await?;
                id.first(&data)
                    .and_then(id_text)
                    .ok_or(Error::Contract("provider namespace"))?
                    .into_owned()
            }
            recipe::NamespaceSource::Match {
                anchor,
                candidates,
                key,
                id,
                compare,
            } => {
                let bound = self.execute(recipe, account, user, &anchor.call, &none).await?;
                let listed = self
                    .execute(recipe, account, user, &candidates.call, &none)
                    .await?;
                let bound = anchor
                    .value
                    .get(&bound)
                    .and_then(id_text)
                    .ok_or(Error::Contract("bound workspace"))?;
                let listed = candidates
                    .list
                    .first(&listed)
                    .and_then(Value::as_array)
                    .ok_or(Error::Contract("accessible workspaces"))?;
                let found = listed
                    .iter()
                    .find(|c| key.get(c).and_then(id_text).is_some_and(|k| same_workspace(*compare, &bound, &k)))
                    .ok_or(Error::Contract("immutable workspace id"))?;
                id.get(found)
                    .and_then(id_text)
                    .ok_or(Error::Contract("immutable workspace id"))?
                    .into_owned()
            }
            recipe::NamespaceSource::Connection { id, host } => {
                if !account_id_ok(account) {
                    return Err(Error::Contract("connected account id"));
                }
                let connected = self
                    .request(reqwest::Method::GET, &format!("/connected_accounts/{account}"), None)
                    .await?;
                if connected.pointer("/toolkit/slug").and_then(Value::as_str) != Some(recipe.toolkit.as_str()) {
                    return Err(Error::Contract("connected account toolkit"));
                }
                id.first(&connected)
                    .and_then(Value::as_str)
                    .and_then(|raw| connection_namespace(raw, *host))
                    .ok_or(Error::Contract("connection namespace"))?
            }
        };
        if id.is_empty() || id.len() > 400 {
            return Err(Error::Contract("provider namespace"));
        }
        let namespace = format!("{}:{}:{id}", recipe.toolkit, recipe.namespace.kind);
        crate::source_origin::ProviderNamespace::new(&namespace)
            .map_err(|_| Error::Contract("provider namespace"))?;
        Ok(namespace)
    }
    /// One page of one change stream. `probe` limits child lists to the first
    /// item; the self-check uses it to exercise paths without fanning out.
    #[allow(clippy::too_many_arguments)]
    pub async fn page(
        &self,
        recipe: &Recipe,
        account: &str,
        user: &str,
        namespace: &str,
        window: &Window,
        at: &Resume,
        probe: bool,
    ) -> Result<Page, Error> {
        let stream = recipe
            .streams
            .get(at.stream)
            .ok_or(Error::Contract("resume stream"))?;
        let resumed = |first: u64| {
            at.cursor
                .as_deref()
                .map_or(Ok(first), |c| c.parse::<u64>().map_err(|_| Error::Contract("resume position")))
        };
        let (cursor, offset, page) = match &stream.paging {
            StreamPaging::Cursor(_) => (at.cursor.as_deref(), None, None),
            StreamPaging::Numbered { by: Position::Offset, .. } => (None, Some(resumed(0)?), None),
            StreamPaging::Numbered { by: Position::Page, .. } => (None, None, Some(resumed(1)?.max(1))),
        };
        let data = self
            .execute(
                recipe,
                account,
                user,
                &stream.call,
                &Bindings {
                    since: Some(window.since),
                    since_minutes: Some(window.minutes()),
                    cursor,
                    offset,
                    page,
                    page_size: Some(PAGE_SIZE),
                    ..Default::default()
                },
            )
            .await?;
        let values = stream
            .items
            .first(&data)
            .and_then(Value::as_array)
            .ok_or(Error::Contract("items"))?;
        if values.len() > PAGE_SIZE {
            return Err(Error::Contract("item page size bound"));
        }
        let mut items = Vec::new();
        let mut reached_watermark = false;
        let mut previous: Option<DateTime<Utc>> = None;
        for (index, value) in values.iter().enumerate() {
            let item = normalize(recipe, namespace, stream_shape(stream), value, None, None, None)?;
            match stream.change {
                ChangeStrategy::Filter => {
                    if item.timestamp < window.filter_floor() {
                        return Err(Error::Contract("change filter not honored"));
                    }
                }
                ChangeStrategy::SortStop => {
                    if previous.is_some_and(|p| item.timestamp > p) {
                        return Err(Error::Contract("change sort order not honored"));
                    }
                    previous = Some(item.timestamp);
                    if item.timestamp < window.since {
                        reached_watermark = true;
                        break;
                    }
                }
            }
            let container = item.container.clone();
            let (parent_title, parent_reference) = (item.source_title.clone(), item.reference.clone());
            push_item(&mut items, item)?;
            if let Some(children) = &stream.children {
                if !probe || index == 0 {
                    let parent = native_id(stream.fields.id.get(value))?;
                    let facts = ParentFacts { title: parent_title.as_deref(), reference: parent_reference.as_deref() };
                    self.children(recipe, account, user, namespace, children, &parent, container.as_ref(), facts, &mut items)
                        .await?;
                }
            }
        }
        let next = if reached_watermark {
            None
        } else {
            match &stream.paging {
                StreamPaging::Cursor(paging) => cursor_next(paging, &data)?,
                StreamPaging::Numbered { total, .. } => {
                    let read = values.len() as u64;
                    match (offset, page) {
                        (Some(offset), _) => numbered_more(total.as_ref(), &data, offset, read)?
                            .then(|| (offset + read).to_string()),
                        (_, Some(page)) => numbered_more(total.as_ref(), &data, (page - 1) * PAGE_SIZE as u64, read)?
                            .then(|| (page + 1).to_string()),
                        _ => return Err(Error::Contract("resume position")),
                    }
                }
            }
        };
        if next.is_some() && next == at.cursor {
            return Err(Error::Contract("non-advancing item cursor"));
        }
        let next = match next {
            Some(cursor) => Some(Resume {
                stream: at.stream,
                cursor: Some(cursor),
            }),
            None if at.stream + 1 < recipe.streams.len() => Some(Resume {
                stream: at.stream + 1,
                cursor: None,
            }),
            None => None,
        };
        Ok(Page { items, next })
    }
    /// Every permitted child page of one item, bounded; a partial child list
    /// fails the page so its checkpoint is retained.
    #[allow(clippy::too_many_arguments)]
    async fn children(
        &self,
        recipe: &Recipe,
        account: &str,
        user: &str,
        namespace: &str,
        children: &ChildList,
        parent: &str,
        container: Option<&ItemContainer>,
        parent_facts: ParentFacts<'_>,
        items: &mut Vec<Item>,
    ) -> Result<(), Error> {
        let mut cursor: Option<String> = None;
        let mut seen = std::collections::BTreeSet::new();
        let mut offset = 0u64;
        let shape = Shape {
            kind: &children.kind,
            fields: &children.fields,
            render: Render::Item,
            capture: &children.capture,
        };
        for _ in 0..MAX_CHILD_PAGES {
            let data = self
                .execute(
                    recipe,
                    account,
                    user,
                    &children.call,
                    &Bindings {
                        parent: Some(parent),
                        cursor: cursor.as_deref(),
                        offset: Some(offset),
                        ..Default::default()
                    },
                )
                .await?;
            let (values, next) = child_page(children, &data, offset)?;
            for value in values {
                push_item(items, normalize(recipe, namespace, shape, value, Some(parent), container, Some(parent_facts))?)?;
            }
            match next {
                None => return Ok(()),
                Some(Next::Cursor(next)) => {
                    if !seen.insert(next.clone()) {
                        return Err(Error::Contract("non-advancing child cursor"));
                    }
                    cursor = Some(next)
                }
                Some(Next::Offset(next)) => offset = next,
            }
        }
        Err(Error::Contract(
            "child page safety bound reached; checkpoint retained",
        ))
    }
    /// Reads, never ingests, the first page of every stream over the probe
    /// window and checks paths and the change strategy. A recipe or provider
    /// contract failure is a verdict; auth, rate-limit and transport errors are
    /// returned so the caller can retry or ask for reconnect instead.
    pub async fn self_check(
        &self,
        recipe: &Recipe,
        account: &str,
        user: &str,
        namespace: &str,
        now: DateTime<Utc>,
    ) -> Result<SelfCheck, Error> {
        let window = Window {
            since: now - Duration::days(PROBE_WINDOW_DAYS),
            now,
        };
        let mut seen = 0;
        for stream in 0..recipe.streams.len() {
            let at = Resume { stream, cursor: None };
            match self.page(recipe, account, user, namespace, &window, &at, true).await {
                Ok(page) => seen += page.items.len(),
                Err(Error::Contract(why)) => return Ok(SelfCheck::Failed(why)),
                // The namespace call just succeeded, so a refused recipe call is
                // the recipe's problem (bad JQL, renamed argument), not an outage.
                Err(Error::Upstream) => return Ok(SelfCheck::Failed("provider refused a recipe call")),
                Err(e) => return Err(e),
            }
        }
        Ok(if seen > 0 { SelfCheck::Passed } else { SelfCheck::Inconclusive })
    }
    /// Deletes the connected account at Composio and revokes its provider
    /// credentials. An account Composio no longer has is already disconnected.
    pub async fn revoke(&self, account: &str) -> Result<(), Error> {
        if !account_id_ok(account) {
            return Err(Error::Contract("connected account id"));
        }
        match self
            .request(
                reqwest::Method::DELETE,
                &format!("/connected_accounts/{account}?revoke_on_delete=true"),
                None,
            )
            .await
        {
            Ok(_) | Err(Error::Gone) => Ok(()),
            Err(e) => Err(e),
        }
    }
}
fn same_workspace(compare: MatchCompare, bound: &str, candidate: &str) -> bool {
    match compare {
        MatchCompare::Exact => bound == candidate,
        MatchCompare::Origin => {
            let origin = |raw: &str| {
                reqwest::Url::parse(raw)
                    .ok()
                    .filter(|u| u.scheme() == "https" && u.domain().is_some())
                    .map(|u| u.origin())
            };
            origin(bound).is_some_and(|b| Some(b) == origin(candidate))
        }
    }
}
/// A host name taken from a connected-account field: the lowercase host of an
/// `https` URL, or the value itself (a subdomain). IP hosts are refused.
fn connection_namespace(raw: &str, host: bool) -> Option<String> {
    let name = if host {
        let url = reqwest::Url::parse(raw).ok().filter(|u| u.scheme() == "https")?;
        url.domain()?.to_owned()
    } else {
        raw.to_owned()
    }
    .to_ascii_lowercase();
    let label_ok = |l: &str| !l.is_empty() && !l.starts_with('-') && !l.ends_with('-');
    (name.len() <= 253
        && name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'.')
        && name.split('.').all(label_ok))
    .then_some(name)
}
/// Connected account ids are Composio nanoids (`ca_...`); anything else never
/// reaches a URL path.
pub fn account_id_ok(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 256
        && id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelfCheck {
    /// At least one real item parsed and honored the change strategy.
    Passed,
    /// Every call parsed but the probe window was empty; check again later.
    Inconclusive,
    Failed(&'static str),
}
/// The change window of one poll pass.
#[derive(Debug, Clone, Copy)]
pub struct Window {
    pub since: DateTime<Utc>,
    pub now: DateTime<Utc>,
}
impl Window {
    /// Re-reads `OVERLAP_MINUTES` before the watermark.
    pub fn from_watermark(watermark: DateTime<Utc>, now: DateTime<Utc>) -> Self {
        Self {
            since: watermark - Duration::minutes(OVERLAP_MINUTES),
            now,
        }
    }
    /// Whole minutes back to `since`, rounded up, never zero.
    pub fn minutes(&self) -> u64 {
        let seconds = (self.now - self.since).num_seconds().max(1) as u64;
        seconds.div_ceil(60)
    }
    /// The oldest update time a filtering provider may legitimately return.
    fn filter_floor(&self) -> DateTime<Utc> {
        let relative = self.now - Duration::minutes(self.minutes() as i64);
        relative.min(self.since) - Duration::seconds(FILTER_SLACK_SECONDS)
    }
}
/// Where an interrupted pass resumes: a stream index and its provider cursor.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Resume {
    pub stream: usize,
    pub cursor: Option<String>,
}
/// How long to wait before polling a connection again: 5 minutes after a change
/// in the last hour, 60 minutes after a week without one, else 15.
pub fn next_poll_in(
    now: DateTime<Utc>,
    last_change_at: Option<DateTime<Utc>>,
    connected_at: DateTime<Utc>,
) -> Duration {
    let quiet_since = last_change_at.unwrap_or(connected_at);
    if last_change_at.is_some_and(|t| now - t < Duration::hours(1)) {
        Duration::minutes(5)
    } else if now - quiet_since >= Duration::days(7) {
        Duration::minutes(60)
    } else {
        Duration::minutes(15)
    }
}
fn push_item(items: &mut Vec<Item>, item: Item) -> Result<(), Error> {
    if items.len() >= MAX_CAPTURE_ITEMS {
        return Err(Error::Contract(
            "capture page item safety bound; checkpoint retained",
        ));
    }
    if items.iter().map(|i| i.content.len()).sum::<usize>() + item.content.len() > MAX_CAPTURE_BYTES
    {
        return Err(Error::Contract(
            "capture page content byte bound; checkpoint retained",
        ));
    }
    items.push(item);
    Ok(())
}
#[derive(Debug)]
pub struct Page {
    pub items: Vec<Item>,
    /// `None` once every stream has been read to the end of the window.
    pub next: Option<Resume>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ItemContainer {
    pub kind: String,
    pub id: String,
    pub name: Option<String>,
}
#[derive(Debug, Clone)]
pub struct Item {
    pub source_type: String,
    pub source_id: String,
    pub content: String,
    pub timestamp: DateTime<Utc>,
    pub author_id: Option<String>,
    pub author_name: Option<String>,
    pub container: Option<ItemContainer>,
    pub kind: String,
    /// The recipe's title for this item (its `capture.title` templates), or
    /// `None` to derive one from the content.
    pub title: Option<String>,
    /// The recipe's chunking for this item.
    pub chunk: ChunkHint,
    /// The provider's own title and display reference, kept so a child list
    /// can name its parent in its title.
    pub source_title: Option<String>,
    pub reference: Option<String>,
    /// The display key that names this item for extraction: its own
    /// reference, else its parent's (a Linear comment names `COR-65`).
    /// Metadata only; the content keeps the retained render.
    pub identifier: Option<String>,
}
impl Item {
    /// The capture funnel's hints for this item.
    pub(crate) fn capture_hints(&self) -> crate::ingest::capture::CaptureHints {
        crate::ingest::capture::CaptureHints {
            chunk: Some(self.chunk.strategy()),
            title: self.title.clone(),
            identifier: self.identifier.clone(),
            url: None,
        }
    }
}
/// The parent item a child list was fetched for.
#[derive(Debug, Clone, Copy, Default)]
struct ParentFacts<'a> {
    title: Option<&'a str>,
    reference: Option<&'a str>,
}
#[derive(Clone, Copy)]
struct Shape<'a> {
    kind: &'a str,
    fields: &'a Fields,
    render: Render,
    capture: &'a CaptureHints,
}
fn stream_shape(stream: &Stream) -> Shape<'_> {
    Shape {
        kind: &stream.kind,
        fields: &stream.fields,
        render: stream.render,
        capture: &stream.capture,
    }
}
/// A provider id as text: strings byte for byte, JSON integers (never floats
/// or booleans) as their decimal string.
fn id_text(value: &Value) -> Option<Cow<'_, str>> {
    match value {
        Value::String(s) => Some(Cow::Borrowed(s)),
        Value::Number(n) if n.is_u64() || n.is_i64() => Some(Cow::Owned(n.to_string())),
        _ => None,
    }
}
fn native_id(value: Option<&Value>) -> Result<Cow<'_, str>, Error> {
    value
        .and_then(id_text)
        .filter(|id| {
            !id.is_empty()
                && id.len() <= 256
                && id.trim() == *id
                && !id.contains('/')
                && !id.chars().any(char::is_control)
        })
        .ok_or(Error::Contract("immutable native id"))
}
fn text(value: &Value) -> String {
    if let Some(s) = value.as_str() {
        return s.into();
    }
    let mut output = String::new();
    if let Some(s) = value.get("text").and_then(Value::as_str) {
        output.push_str(s);
    }
    if let Some(nodes) = value.get("content").and_then(Value::as_array) {
        for node in nodes {
            output.push_str(&text(node));
            if matches!(
                node.get("type").and_then(Value::as_str),
                Some("paragraph" | "heading" | "listItem" | "codeBlock")
            ) {
                output.push('\n');
            }
        }
    }
    output
}
/// `project` -> `Project`: the container label used in retained content.
fn title_case(kind: &str) -> String {
    let mut chars = kind.chars();
    chars
        .next()
        .map(|c| c.to_uppercase().chain(chars).collect())
        .unwrap_or_default()
}
/// Epoch values below this are seconds, at or above it milliseconds. 1e11
/// seconds is the year 5138 and 1e11 milliseconds is 1973-03-03, so no real
/// update time is ambiguous.
const EPOCH_MILLIS_FROM: u64 = 100_000_000_000;
/// RFC 3339 (or Jira's `+0000` offset) text, or Unix seconds or milliseconds
/// as a JSON integer or a digit string.
fn parse_timestamp(value: &Value) -> Result<DateTime<Utc>, Error> {
    let epoch = match value {
        Value::Number(n) => n.as_u64(),
        Value::String(s) if !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()) => s.parse().ok(),
        Value::String(date) => {
            return Ok(DateTime::parse_from_rfc3339(date)
                .or_else(|_| DateTime::parse_from_str(date, "%Y-%m-%dT%H:%M:%S%.f%z"))
                .map_err(|_| Error::Contract("provider timestamp"))?
                .with_timezone(&Utc))
        }
        _ => return Err(Error::Contract("timestamp")),
    };
    let epoch = epoch
        .and_then(|n| i64::try_from(n).ok().map(|i| (n, i)))
        .ok_or(Error::Contract("provider timestamp"))?;
    match epoch {
        (n, i) if n < EPOCH_MILLIS_FROM => DateTime::from_timestamp(i, 0),
        (_, i) => DateTime::from_timestamp_millis(i),
    }
    .ok_or(Error::Contract("provider timestamp"))
}
/// The item's own container, else the one its parent sits in. An absent or
/// null container is fine (a Linear issue outside any project).
fn container_of(fields: &Fields, value: &Value, inherited: Option<&ItemContainer>) -> Option<ItemContainer> {
    let Some(container) = &fields.container else {
        return inherited.cloned();
    };
    let id = container
        .id
        .first(value)
        .and_then(id_text)
        .filter(|id| crate::source_origin::ContainerId::new(&**id).is_ok())?;
    Some(ItemContainer {
        kind: container.kind.clone(),
        id: id.into_owned(),
        name: container.name.first(value).and_then(Value::as_str).map(str::to_owned),
    })
}
/// Content and identity for one provider item. Source identities are
/// `<namespace>/<kind>/[<parent>/]<native id>`, exactly as PR #1239 produced, so
/// re-captured items version the atoms that capture retained.
fn normalize(
    recipe: &Recipe,
    namespace: &str,
    shape: Shape,
    value: &Value,
    parent: Option<&str>,
    inherited: Option<&ItemContainer>,
    parent_facts: Option<ParentFacts<'_>>,
) -> Result<Item, Error> {
    let fields = shape.fields;
    let native = native_id(fields.id.get(value))?;
    let parent = match &fields.parent {
        Some(path) => Some(native_id(path.first(value))?),
        None => parent.map(Cow::Borrowed),
    };
    let timestamp = parse_timestamp(fields.updated_at.first(value).ok_or(Error::Contract("timestamp"))?)?;
    let author = fields
        .author
        .as_ref()
        .and_then(|a| Some((a, a.object.first(value)?)));
    let author_id = author
        .and_then(|(a, v)| a.id.first(v))
        .and_then(id_text)
        .map(Cow::into_owned);
    let author_name = author
        .and_then(|(a, v)| a.name.first(v))
        .and_then(Value::as_str)
        .map(str::to_owned);
    let title = fields.title.first(value).and_then(Value::as_str);
    let body = match (fields.body.format, fields.body.path.first(value)) {
        (BodyFormat::Html, Some(Value::String(html))) => {
            crate::connectors::email_body::from_graph_body_boundaried(Some("html"), html).trim().to_owned()
        }
        (_, found) => text(found.unwrap_or(&Value::Null)),
    };
    let container = container_of(fields, value, inherited);
    let content = match shape.render {
        Render::Item => {
            let state = fields.state.first(value).and_then(Value::as_str).unwrap_or("");
            let reference = fields.reference.first(value).and_then(Value::as_str).unwrap_or("");
            let container_line = container
                .as_ref()
                .map(|c| format!("{}: {}\n", title_case(&c.kind), c.id))
                .unwrap_or_default();
            format!(
                "{}\nService: {}\n{container_line}Reference: {reference}\nState: {state}\n{body}",
                title.unwrap_or(shape.kind),
                recipe.toolkit,
            )
        }
        Render::Snapshot => {
            let c = container.as_ref().ok_or(Error::Contract("snapshot container id"))?;
            let label = title_case(&c.kind);
            format!(
                "{label} snapshot observed by CoralSwarm\n{}\nService: {}\n{label}: {}\n{body}",
                title.unwrap_or(&label),
                recipe.toolkit,
                c.id
            )
        }
    };
    let reference = fields.reference.first(value).and_then(Value::as_str).filter(|r| !r.trim().is_empty());
    let excerpt = crate::services::atom_title::fallback_title(&body);
    let parent_facts = parent_facts.unwrap_or(ParentFacts {
        title: shape.capture.parent_title.first(value).and_then(Value::as_str),
        reference: shape.capture.parent_reference.first(value).and_then(Value::as_str),
    });
    let title_hint = shape
        .capture
        .title(&TitleFacts {
            title,
            reference,
            author: author_name.as_deref(),
            container: container.as_ref().and_then(|c| c.name.as_deref()),
            excerpt: excerpt.as_deref(),
            parent_title: parent_facts.title,
            parent_reference: parent_facts.reference,
        })
        .map(|t| crate::services::atom_title::truncate_title(&t));
    let identifier = reference
        .or(parent_facts.reference)
        .map(|r| r.split_whitespace().collect::<Vec<_>>().join(" "))
        .filter(|r| !r.is_empty());
    let suffix = parent
        .map(|p| format!("{p}/{native}"))
        .unwrap_or_else(|| native.into_owned());
    Ok(Item {
        title: title_hint,
        chunk: shape.capture.chunk,
        source_title: title.filter(|t| !t.trim().is_empty()).map(str::to_owned),
        reference: reference.map(str::to_owned),
        identifier,
        source_type: format!("{}_{}", recipe.toolkit, shape.kind),
        source_id: format!("{namespace}/{}/{suffix}", shape.kind),
        content,
        timestamp,
        author_id,
        author_name,
        container,
        kind: shape.kind.into(),
    })
}
fn cursor_next(paging: &CursorPaging, data: &Value) -> Result<Option<String>, Error> {
    let more = match &paging.more {
        More::HasMore(p) => p
            .get(data)
            .and_then(Value::as_bool)
            .ok_or(Error::Contract("has-more flag"))?,
        More::IsLast(p) => !p
            .get(data)
            .and_then(Value::as_bool)
            .ok_or(Error::Contract("last-page flag"))?,
        More::Token => return token(paging.next.get(data)),
    };
    if !more {
        return Ok(None);
    }
    token(paging.next.get(data))?
        .map(Some)
        .ok_or(Error::Contract("continuation token"))
}
/// A continuation token: a non-empty string, or an integer as its decimal
/// string. Absent, null and empty mean there is none.
fn token(value: Option<&Value>) -> Result<Option<String>, Error> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) if s.is_empty() => Ok(None),
        Some(v) => id_text(v)
            .map(|t| Some(t.into_owned()))
            .ok_or(Error::Contract("continuation token")),
    }
}
/// Whether a numbered stream has another page after reading `read` items
/// past `before`: never after an empty page; up to `total` when the recipe
/// names it; else while pages come back full.
fn numbered_more(total: Option<&Pointer>, data: &Value, before: u64, read: u64) -> Result<bool, Error> {
    if read == 0 {
        return Ok(false);
    }
    Ok(match total {
        Some(total) => {
            before + read
                < total
                    .get(data)
                    .and_then(Value::as_u64)
                    .ok_or(Error::Contract("item total"))?
        }
        None => read >= PAGE_SIZE as u64,
    })
}
enum Next {
    Cursor(String),
    Offset(u64),
}
fn child_page<'a>(
    children: &ChildList,
    data: &'a Value,
    offset: u64,
) -> Result<(&'a Vec<Value>, Option<Next>), Error> {
    let page = children
        .page
        .first(data)
        .ok_or(Error::Contract("child page"))?;
    let values = children
        .items
        .get(page)
        .and_then(Value::as_array)
        .ok_or(Error::Contract("child items"))?;
    let next = match &children.paging {
        ChildPaging::Cursor { cursor } => cursor_next(cursor, page)?.map(Next::Cursor),
        ChildPaging::Offset { total } => {
            let total = total
                .get(page)
                .and_then(Value::as_u64)
                .ok_or(Error::Contract("child total"))?;
            let next = offset + values.len() as u64;
            if next < total && values.is_empty() {
                return Err(Error::Contract("non-advancing child page"));
            }
            (next < total).then_some(Next::Offset(next))
        }
        ChildPaging::Single {} => None,
    };
    Ok((values, next))
}
/// Composio wraps successful GraphQL results as `"errors": null`; only a
/// non-null, non-empty `errors` value is a provider failure.
fn has_provider_errors(data: &Value) -> bool {
    match data.get("errors") {
        None | Some(Value::Null) => false,
        Some(Value::Array(a)) => !a.is_empty(),
        Some(_) => true,
    }
}
#[cfg(test)]
mod tests;
