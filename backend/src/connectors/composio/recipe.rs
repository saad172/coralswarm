//! Declarative per-toolkit capture recipes. A recipe is checked-in data that
//! names the read-only tools, JSON paths and paging style for one Composio
//! toolkit; the engine in `mod.rs` owns every behaviour. Recipes are parsed and
//! validated once at load, so the engine only ever sees well-formed shapes.
//!
//! Adding a toolkit means adding `recipes/<toolkit>.json` and one line to
//! `SOURCES`. There is no fallback that ingests tool output without a recipe.
use chrono::{DateTime, SecondsFormat, Utc};
use serde::Deserialize;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::LazyLock;

const SOURCES: &[(&str, &str)] = &[
    ("linear", include_str!("recipes/linear.json")),
    ("jira", include_str!("recipes/jira.json")),
    ("zendesk", include_str!("recipes/zendesk.json")),
    ("dart", include_str!("recipes/dart.json")),
    ("confluence", include_str!("recipes/confluence.json")),
    ("hubspot", include_str!("recipes/hubspot.json")),
];

/// Composio toolkits CoralSwarm already captures with its own connectors. A
/// recipe for one would capture the same content twice under a second
/// identity, so loading it fails.
pub const NATIVE_TOOLKITS: &[(&str, &str)] = &[
    ("slack", "Slack"),
    ("slackbot", "Slack"),
    ("gmail", "Gmail"),
    ("googlecalendar", "Google Calendar"),
    ("google_chat", "Google Chat"),
    ("googlechat", "Google Chat"),
    ("googlemeet", "Google Meet"),
    ("googledrive", "Google Drive"),
    ("googledocs", "Google Docs"),
    ("googlesuper", "Google Workspace"),
    ("github", "GitHub"),
    ("github_mcp", "GitHub"),
    ("microsoft_teams", "Microsoft Teams"),
    ("outlook", "Outlook mail and calendar"),
];

static REGISTRY: LazyLock<BTreeMap<String, Recipe>> =
    LazyLock::new(|| load(SOURCES).unwrap_or_else(|e| panic!("invalid Composio recipe: {e}")));

/// The recipe registered for a toolkit slug; unknown slugs have no capture path.
pub fn get(toolkit: &str) -> Option<&'static Recipe> {
    REGISTRY.get(toolkit)
}

/// The chunk hint a Composio capture of `source_type` (`{toolkit}_{kind}`)
/// under `provider_namespace` (`{toolkit}:...`) was written with, so a later
/// reader can reproduce its chunks. `None` for anything not captured by a
/// registered recipe.
pub fn chunk_hint_for(source_type: &str, provider_namespace: &str) -> Option<ChunkHint> {
    let toolkit = provider_namespace.split(':').next()?;
    let kind = source_type.strip_prefix(toolkit)?.strip_prefix('_')?;
    let recipe = get(toolkit)?;
    recipe.streams.iter().find_map(|s| {
        if s.kind == kind {
            return Some(s.capture.chunk);
        }
        s.children.as_ref().filter(|c| c.kind == kind).map(|c| c.capture.chunk)
    })
}

pub fn all() -> impl Iterator<Item = &'static Recipe> {
    REGISTRY.values()
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Recipe {
    /// Composio toolkit slug; also the CoralSwarm service id and source_type prefix.
    pub toolkit: String,
    pub label: String,
    /// Pinned Composio toolkit version sent with every execution.
    pub version: String,
    pub scopes: Scopes,
    /// Read-only tool allowlist. Every call below must name one of these.
    pub read_tools: BTreeSet<String>,
    pub namespace: Namespace,
    /// Workspace-wide change lists, polled in order on every pass.
    pub streams: Vec<Stream>,
    /// Coverage statements shown on the setup page, keyed by topic.
    #[serde(default)]
    pub limits: BTreeMap<String, String>,
    #[serde(default)]
    pub references: Vec<String>,
}

/// OAuth scopes the deployment's auth config may grant. A grant must include
/// every required scope and nothing outside required + optional.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Scopes {
    pub required: Vec<String>,
    #[serde(default)]
    pub optional: Vec<String>,
}

/// The immutable provider workspace identity. Revocation, dedupe and source
/// identities are all qualified by `<toolkit>:<kind>:<id>`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Namespace {
    pub kind: String,
    pub source: NamespaceSource,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum NamespaceSource {
    /// One call; the id is the first present path.
    Lookup { call: Call, id: Paths },
    /// Two calls: pick the candidate whose `key` equals the anchor value. Used
    /// when the authenticated grant can see several workspaces (Jira sites).
    Match {
        anchor: Probe,
        candidates: Candidates,
        key: Pointer,
        id: Pointer,
        #[serde(default)]
        compare: MatchCompare,
    },
    /// No tool call: a field of the Composio connected account the engine
    /// reads at verify (`GET /connected_accounts/{id}`), for providers whose
    /// tools expose no account id. Only `CONNECTION_FIELDS` may be read; with
    /// `host` the value is a URL and its lowercase host is the id.
    Connection {
        id: Paths,
        #[serde(default)]
        host: bool,
    },
}

/// How a `match` namespace compares the anchor value with each candidate key.
#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MatchCompare {
    /// The two values are equal byte for byte.
    #[default]
    Exact,
    /// Both are `https` URLs with the same origin (scheme, host, port); paths
    /// are ignored. Confluence's base URL carries `/wiki` and the site URL
    /// does not.
    Origin,
}

/// Connected-account fields a namespace may read, under `/params` or `/data`.
/// Those objects also hold tokens and headers, which no recipe can name.
pub const CONNECTION_FIELDS: &[&str] = &["subdomain", "domain", "base_url", "instance_url"];
const CONNECTION_URL_FIELDS: &[&str] = &["base_url", "instance_url"];

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Probe {
    pub call: Call,
    pub value: Pointer,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Candidates {
    pub call: Call,
    /// First path that holds an array.
    pub list: Paths,
}

/// How a stream finds what changed since the watermark. There is deliberately
/// no full-scan variant: a toolkit that cannot do one of these cheaply has no
/// recipe.
#[derive(Debug, Clone, Copy, Deserialize, serde::Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ChangeStrategy {
    /// The list call takes the watermark (`{{since}}` or `{{since_minutes}}`)
    /// and returns only items updated at or after it.
    Filter,
    /// The list call is sorted by update time, newest first; paging stops at
    /// the first item older than the watermark.
    SortStop,
}

/// One workspace-wide change list (issues, comments, projects).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Stream {
    pub kind: String,
    pub change: ChangeStrategy,
    pub call: Call,
    pub items: Paths,
    pub paging: StreamPaging,
    pub fields: Fields,
    #[serde(default)]
    pub render: Render,
    /// Per-item lists fetched completely for every changed item (Jira comments,
    /// which only move their issue's `updated`).
    pub children: Option<ChildList>,
    /// How this stream's items are chunked and titled.
    #[serde(default)]
    pub capture: CaptureHints,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Render {
    /// Title, service, container, reference, state and body.
    #[default]
    Item,
    /// The container's own description (a project), in the snapshot format
    /// PR #1239 retained, so existing project atoms keep their content.
    Snapshot,
}

/// Per-item lists fetched completely for every item (comments).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChildList {
    pub kind: String,
    pub call: Call,
    /// Page root within the response; `items` and paging paths are relative to it.
    pub page: Paths,
    pub items: Pointer,
    pub paging: ChildPaging,
    pub fields: Fields,
    /// How these children are chunked and titled. A child's `parent_title`
    /// and `parent_reference` slots read the parent item it was fetched for.
    #[serde(default)]
    pub capture: CaptureHints,
}

/// Opaque continuation tokens. `has_more` or `is_last` says whether `next`
/// must be present; with neither, a present, non-empty `next` is the flag.
#[derive(Debug, Clone, Deserialize)]
#[serde(try_from = "RawCursorPaging")]
pub struct CursorPaging {
    pub more: More,
    pub next: Pointer,
}

#[derive(Debug, Clone)]
pub enum More {
    HasMore(Pointer),
    IsLast(Pointer),
    /// No flag: a present, non-null, non-empty token means another page.
    Token,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawCursorPaging {
    has_more: Option<Pointer>,
    is_last: Option<Pointer>,
    next: Pointer,
}

impl TryFrom<RawCursorPaging> for CursorPaging {
    type Error = String;
    fn try_from(raw: RawCursorPaging) -> Result<Self, String> {
        let more = match (raw.has_more, raw.is_last) {
            (Some(p), None) => More::HasMore(p),
            (None, Some(p)) => More::IsLast(p),
            (None, None) => More::Token,
            (Some(_), Some(_)) => {
                return Err("cursor paging takes exactly one of has_more / is_last, or neither when the next token alone says more".into())
            }
        };
        Ok(Self {
            more,
            next: raw.next,
        })
    }
}

/// How a change stream pages: provider tokens (no `style`, or `cursor`), or
/// numeric positions (`offset`, `page`).
#[derive(Debug, Clone, Deserialize)]
#[serde(try_from = "RawStreamPaging")]
pub enum StreamPaging {
    Cursor(CursorPaging),
    /// `{{offset}}` (items skipped, from 0) or `{{page}}` (from 1), with
    /// `{{page_size}}`. The stream ends at an empty page; else at `total`
    /// items when the recipe names it; else at a page shorter than the page size.
    Numbered { by: Position, total: Option<Pointer> },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Position {
    Offset,
    Page,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawStreamPaging {
    style: Option<String>,
    has_more: Option<Pointer>,
    is_last: Option<Pointer>,
    next: Option<Pointer>,
    total: Option<Pointer>,
}

impl TryFrom<RawStreamPaging> for StreamPaging {
    type Error = String;
    fn try_from(raw: RawStreamPaging) -> Result<Self, String> {
        let by = match raw.style.as_deref() {
            None | Some("cursor") => {
                if raw.total.is_some() {
                    return Err("cursor paging has no total".into());
                }
                let next = raw.next.ok_or("cursor paging needs a next token path")?;
                return CursorPaging::try_from(RawCursorPaging {
                    has_more: raw.has_more,
                    is_last: raw.is_last,
                    next,
                })
                .map(Self::Cursor);
            }
            Some("offset") => Position::Offset,
            Some("page") => Position::Page,
            Some(other) => return Err(format!("unknown stream paging style {other:?}")),
        };
        if raw.has_more.is_some() || raw.is_last.is_some() || raw.next.is_some() {
            return Err("offset and page paging end by count; has_more, is_last and next are cursor paging".into());
        }
        Ok(Self::Numbered { by, total: raw.total })
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "style", rename_all = "snake_case", deny_unknown_fields)]
pub enum ChildPaging {
    Cursor {
        #[serde(flatten)]
        cursor: CursorPaging,
    },
    /// Numeric offsets; the list ends once offset reaches `total`.
    Offset { total: Pointer },
    /// The whole list arrives in one response (Intercom conversation parts).
    Single {},
}

/// Where an item's identity and display fields live. Every list is ordered and
/// uses the first path present in the item, even when its value is null.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Fields {
    /// Immutable native id; never a mutable key such as a Jira issue key.
    pub id: Pointer,
    /// Also the time the change strategy is checked against.
    pub updated_at: Paths,
    /// Native id of the item this one belongs to (a comment's issue). It is
    /// part of the source identity, so it must be immutable too.
    pub parent: Option<Paths>,
    /// The project an item sits in, read from the item itself.
    pub container: Option<ContainerRef>,
    #[serde(default)]
    pub title: Paths,
    /// Plain string or Atlassian document (ADF) text; `{"path", "format": "html"}`
    /// renders an HTML string to plain text.
    #[serde(default)]
    pub body: Body,
    #[serde(default)]
    pub state: Paths,
    /// Human display reference, e.g. `ENG-12`; content only, never identity.
    #[serde(default)]
    pub reference: Paths,
    pub author: Option<Author>,
}

/// Where an item's text lives and how it is encoded. A bare path (or list) is
/// `text`.
#[derive(Debug, Clone, Default)]
pub struct Body {
    pub path: Paths,
    pub format: BodyFormat,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BodyFormat {
    /// A plain string, or an Atlassian document (ADF) object.
    #[default]
    Text,
    /// An HTML or XHTML string (Confluence storage format, Zendesk comments),
    /// rendered to plain text with block boundaries kept as line breaks.
    Html,
}

impl<'de> Deserialize<'de> for Body {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Typed {
            path: Paths,
            format: BodyFormat,
        }
        let value = Value::deserialize(d)?;
        if value.is_object() {
            let Typed { path, format } = Typed::deserialize(value).map_err(serde::de::Error::custom)?;
            return Ok(Self { path, format });
        }
        Ok(Self {
            path: Paths::deserialize(value).map_err(serde::de::Error::custom)?,
            format: BodyFormat::Text,
        })
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContainerRef {
    pub kind: String,
    pub id: Paths,
    #[serde(default)]
    pub name: Paths,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Author {
    pub object: Paths,
    pub id: Paths,
    pub name: Paths,
}

/// How the capture funnel treats one stream's items: chunking and titles.
/// Data, not code, so a new toolkit gets sensible chunks and real titles from
/// its recipe alone. Absent hints are the safe defaults: `document` chunking
/// and a title derived from the content. Validated at load; a malformed hint
/// fails the recipe (and with it the registry), never a capture.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CaptureHints {
    #[serde(default)]
    pub chunk: ChunkHint,
    /// Title templates, tried in order; the first whose every slot has a
    /// value titles every chunk of the item. None resolving falls back to a
    /// title derived from the content.
    #[serde(default)]
    pub title: Vec<TitleTemplate>,
    /// Where a stream item (one with `fields.parent`) carries its parent's
    /// title / display reference, for the `parent_title` and
    /// `parent_reference` slots. Child lists take them from the parent item.
    #[serde(default)]
    pub parent_title: Paths,
    #[serde(default)]
    pub parent_reference: Paths,
}

/// How rendered item content is chunked. Rendered items are a short field
/// header (`Service:`, `Reference:`, `State:` lines) and a body, so only
/// strategies that keep those lines together are offered: the chat strategy
/// splits per line and the transcript strategy reads `Name: text` lines as
/// speaker turns.
#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ChunkHint {
    /// Paragraph- and sentence-aware windows sized for the embedder.
    #[default]
    Document,
    /// Split at quoted-reply boundaries (`> `). For short reply-shaped
    /// bodies only: a single unquoted block is one chunk, however long.
    Email,
}

impl ChunkHint {
    pub fn strategy(self) -> coralswarm_core::chunker::ChunkStrategy {
        match self {
            ChunkHint::Document => coralswarm_core::chunker::ChunkStrategy::Generic,
            ChunkHint::Email => coralswarm_core::chunker::ChunkStrategy::Email,
        }
    }
}

/// A value a title template can name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum TitleSlot {
    /// `fields.title`.
    Title,
    /// `fields.reference` (an issue key such as `ENG-12`).
    Reference,
    /// The author's display name.
    Author,
    /// The container's display name (never its id).
    Container,
    /// The opening sentence of the body.
    Excerpt,
    ParentTitle,
    ParentReference,
}

impl TitleSlot {
    fn parse(name: &str) -> Option<Self> {
        Some(match name {
            "title" => Self::Title,
            "reference" => Self::Reference,
            "author" => Self::Author,
            "container" => Self::Container,
            "excerpt" => Self::Excerpt,
            "parent_title" => Self::ParentTitle,
            "parent_reference" => Self::ParentReference,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum TitlePart {
    Text(String),
    Slot(TitleSlot),
}

/// `"Comment on {parent_reference}: {parent_title}"`: literal text and
/// `{slot}`s. No other braces.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TitleTemplate(Vec<TitlePart>);

/// Slot values for one item; `None` or blank means the slot has no value.
#[derive(Debug, Clone, Copy, Default)]
pub struct TitleFacts<'a> {
    pub title: Option<&'a str>,
    pub reference: Option<&'a str>,
    pub author: Option<&'a str>,
    pub container: Option<&'a str>,
    pub excerpt: Option<&'a str>,
    pub parent_title: Option<&'a str>,
    pub parent_reference: Option<&'a str>,
}

impl TitleTemplate {
    fn parse(text: &str) -> Result<Self, String> {
        let mut parts = Vec::new();
        let mut rest = text;
        while !rest.is_empty() {
            match rest.find(['{', '}']) {
                None => {
                    parts.push(TitlePart::Text(rest.into()));
                    break;
                }
                Some(i) if rest.as_bytes()[i] == b'}' => {
                    return Err(format!("title template {text:?} has an unmatched }}"))
                }
                Some(i) => {
                    if i > 0 {
                        parts.push(TitlePart::Text(rest[..i].into()));
                    }
                    let close = rest[i..]
                        .find('}')
                        .ok_or_else(|| format!("title template {text:?} has an unclosed {{"))?;
                    let name = &rest[i + 1..i + close];
                    let slot = TitleSlot::parse(name)
                        .ok_or_else(|| format!("title template {text:?} names unknown slot {{{name}}}"))?;
                    parts.push(TitlePart::Slot(slot));
                    rest = &rest[i + close + 1..];
                }
            }
        }
        if !parts.iter().any(|p| matches!(p, TitlePart::Slot(_))) {
            return Err(format!("title template {text:?} names no slot; a constant title says nothing about the item"));
        }
        Ok(Self(parts))
    }

    pub fn slots(&self) -> impl Iterator<Item = TitleSlot> + '_ {
        self.0.iter().filter_map(|p| match p {
            TitlePart::Slot(s) => Some(*s),
            TitlePart::Text(_) => None,
        })
    }

    /// The title, or `None` when any slot has no value. Slot values are
    /// whitespace-collapsed; the result is trimmed.
    pub fn render(&self, facts: &TitleFacts) -> Option<String> {
        let mut out = String::new();
        for part in &self.0 {
            match part {
                TitlePart::Text(t) => out.push_str(t),
                TitlePart::Slot(slot) => {
                    let value = match slot {
                        TitleSlot::Title => facts.title,
                        TitleSlot::Reference => facts.reference,
                        TitleSlot::Author => facts.author,
                        TitleSlot::Container => facts.container,
                        TitleSlot::Excerpt => facts.excerpt,
                        TitleSlot::ParentTitle => facts.parent_title,
                        TitleSlot::ParentReference => facts.parent_reference,
                    }?;
                    let flat = value.split_whitespace().collect::<Vec<_>>().join(" ");
                    if flat.is_empty() {
                        return None;
                    }
                    out.push_str(&flat);
                }
            }
        }
        let out = out.trim();
        (!out.is_empty()).then(|| out.to_owned())
    }
}

impl<'de> Deserialize<'de> for TitleTemplate {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        Self::parse(&String::deserialize(d)?).map_err(serde::de::Error::custom)
    }
}

impl CaptureHints {
    /// The first title template that resolves for `facts`.
    pub fn title(&self, facts: &TitleFacts) -> Option<String> {
        self.title.iter().find_map(|t| t.render(facts))
    }

    /// Every slot must have a source in `fields` (or, for a child list, in
    /// its parent stream's fields). `parent` is the parent stream's fields
    /// for a child list, `None` for a stream.
    fn validate(&self, t: &str, kind: &str, fields: &Fields, parent: Option<&Fields>) -> Result<(), String> {
        let is_child = parent.is_some();
        if is_child && !(self.parent_title.is_empty() && self.parent_reference.is_empty()) {
            return Err(format!("{t}: {kind} is a child list; parent_title and parent_reference come from its parent item"));
        }
        if !is_child && fields.parent.is_none() && !(self.parent_title.is_empty() && self.parent_reference.is_empty()) {
            return Err(format!("{t}: {kind} has no parent, so it cannot name parent_title or parent_reference paths"));
        }
        for slot in self.title.iter().flat_map(TitleTemplate::slots) {
            let sourced = match slot {
                TitleSlot::Title => !fields.title.is_empty(),
                TitleSlot::Reference => !fields.reference.is_empty(),
                TitleSlot::Author => fields.author.as_ref().is_some_and(|a| !a.name.is_empty()),
                TitleSlot::Container => {
                    fields.container.as_ref().is_some_and(|c| !c.name.is_empty())
                        || parent.and_then(|p| p.container.as_ref()).is_some_and(|c| !c.name.is_empty())
                }
                TitleSlot::Excerpt => !fields.body.path.is_empty(),
                TitleSlot::ParentTitle => match parent {
                    Some(p) => !p.title.is_empty(),
                    None => !self.parent_title.is_empty(),
                },
                TitleSlot::ParentReference => match parent {
                    Some(p) => !p.reference.is_empty(),
                    None => !self.parent_reference.is_empty(),
                },
            };
            if !sourced {
                return Err(format!("{t}: {kind} title slot {slot:?} has no source field"));
            }
        }
        Ok(())
    }
}

/// A JSON pointer (RFC 6901); `""` is the whole document.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pointer(String);

impl Pointer {
    pub fn get<'a>(&self, value: &'a Value) -> Option<&'a Value> {
        value.pointer(&self.0)
    }
}

impl<'de> Deserialize<'de> for Pointer {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        if !(s.is_empty() || s.starts_with('/')) {
            return Err(serde::de::Error::custom(format!("JSON pointer {s:?} must start with /")));
        }
        Ok(Self(s))
    }
}

/// Ordered fallback pointers; a single string is a one-element list.
#[derive(Debug, Clone, Default)]
pub struct Paths(Vec<Pointer>);

impl Paths {
    fn pointers(&self) -> &[Pointer] {
        &self.0
    }
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
    /// First path present in `value`, matching `Value::get(..).or_else(..)`.
    pub fn first<'a>(&self, value: &'a Value) -> Option<&'a Value> {
        self.0.iter().find_map(|p| p.get(value))
    }
}

impl<'de> Deserialize<'de> for Paths {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum OneOrMany {
            One(Pointer),
            Many(Vec<Pointer>),
        }
        Ok(Self(match OneOrMany::deserialize(d)? {
            OneOrMany::One(p) => vec![p],
            OneOrMany::Many(v) => v,
        }))
    }
}

/// A recipe-owned tool call. Arguments are fixed at load; only typed slots are
/// filled at run time, so no request can carry a browser-supplied argument.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Call {
    pub tool: String,
    pub args: Template,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Slot {
    /// Watermark minus overlap, RFC 3339 UTC with milliseconds.
    Since,
    /// Whole minutes between now and the watermark minus overlap, rounded up.
    /// Relative windows avoid Jira JQL's per-user time zone.
    SinceMinutes,
    /// Watermark minus overlap as Unix seconds / milliseconds: JSON integers,
    /// or the same digits as a string (`_text`) for providers that type the
    /// value as text (HubSpot search filters).
    SinceEpochS,
    SinceEpochMs,
    SinceEpochSText,
    SinceEpochMsText,
    /// Native id of the item whose children are listed.
    Parent,
    /// Provider continuation token, or null on the first page.
    Cursor,
    /// Numeric offset: items already read (children and offset streams).
    Offset,
    /// Page number from 1 (page streams).
    Page,
    /// Engine page size.
    PageSize,
}

impl Slot {
    fn parse(name: &str) -> Option<Self> {
        Some(match name {
            "since" => Self::Since,
            "since_minutes" => Self::SinceMinutes,
            "since_epoch_s" => Self::SinceEpochS,
            "since_epoch_ms" => Self::SinceEpochMs,
            "since_epoch_s_text" => Self::SinceEpochSText,
            "since_epoch_ms_text" => Self::SinceEpochMsText,
            "parent" => Self::Parent,
            "cursor" => Self::Cursor,
            "offset" => Self::Offset,
            "page" => Self::Page,
            "page_size" => Self::PageSize,
            _ => return None,
        })
    }
}

/// Every slot that carries the watermark; a filter stream uses exactly one.
const WATERMARK: &[Slot] = &[
    Slot::Since,
    Slot::SinceMinutes,
    Slot::SinceEpochS,
    Slot::SinceEpochMs,
    Slot::SinceEpochSText,
    Slot::SinceEpochMsText,
];

#[derive(Debug, Clone)]
pub enum Segment {
    Text(String),
    SinceMinutes,
}

/// Parsed argument template. A string that is exactly `{{slot}}` becomes that
/// slot's typed value; only `{{since_minutes}}`, an engine-computed integer,
/// may be embedded in text (JQL).
#[derive(Debug, Clone)]
pub enum Template {
    Literal(Value),
    Slot(Slot),
    Text(Vec<Segment>),
    Array(Vec<Template>),
    Object(Vec<(String, Template)>),
}

#[derive(Default)]
pub struct Bindings<'a> {
    pub since: Option<DateTime<Utc>>,
    pub since_minutes: Option<u64>,
    pub parent: Option<&'a str>,
    pub cursor: Option<&'a str>,
    pub offset: Option<u64>,
    pub page: Option<u64>,
    pub page_size: Option<usize>,
}

impl Template {
    fn parse(value: &Value) -> Result<Self, String> {
        Ok(match value {
            Value::String(s) => Self::parse_text(s)?,
            Value::Array(a) => Self::Array(a.iter().map(Self::parse).collect::<Result<_, _>>()?),
            Value::Object(o) => Self::Object(
                o.iter()
                    .map(|(k, v)| Ok((k.clone(), Self::parse(v)?)))
                    .collect::<Result<_, String>>()?,
            ),
            v => Self::Literal(v.clone()),
        })
    }
    fn parse_text(s: &str) -> Result<Self, String> {
        if !s.contains("{{") {
            if writes_graphql(s) {
                return Err("GraphQL mutation/subscription literals are not read-only".into());
            }
            return Ok(Self::Literal(Value::String(s.into())));
        }
        if let Some(name) = s.strip_prefix("{{").and_then(|r| r.strip_suffix("}}")) {
            if let Some(slot) = Slot::parse(name) {
                return Ok(Self::Slot(slot));
            }
        }
        let mut segments = Vec::new();
        let mut rest = s;
        while let Some(start) = rest.find("{{") {
            let end = rest[start..]
                .find("}}")
                .ok_or_else(|| format!("unterminated slot in {s:?}"))?;
            if start > 0 {
                segments.push(Segment::Text(rest[..start].into()));
            }
            match &rest[start + 2..start + end] {
                // Only an engine-computed integer may be spliced into text;
                // provider cursors, parent ids and timestamps stay whole values.
                "since_minutes" => segments.push(Segment::SinceMinutes),
                other => return Err(format!("slot {other:?} cannot be embedded in text")),
            }
            rest = &rest[start + end + 2..];
        }
        if !rest.is_empty() {
            segments.push(Segment::Text(rest.into()));
        }
        if segments
            .iter()
            .any(|s| matches!(s, Segment::Text(t) if writes_graphql(t)))
        {
            return Err("GraphQL mutation/subscription literals are not read-only".into());
        }
        Ok(Self::Text(segments))
    }
    fn slots(&self, out: &mut BTreeSet<Slot>) {
        match self {
            Self::Slot(s) => {
                out.insert(*s);
            }
            Self::Text(segments) => {
                if segments.iter().any(|s| matches!(s, Segment::SinceMinutes)) {
                    out.insert(Slot::SinceMinutes);
                }
            }
            Self::Array(a) => a.iter().for_each(|t| t.slots(out)),
            Self::Object(o) => o.iter().for_each(|(_, t)| t.slots(out)),
            Self::Literal(_) => {}
        }
    }
    pub fn render(&self, b: &Bindings) -> Result<Value, &'static str> {
        Ok(match self {
            Self::Literal(v) => v.clone(),
            Self::Slot(Slot::Since) => Value::from(
                b.since
                    .ok_or("since binding")?
                    .to_rfc3339_opts(SecondsFormat::Millis, true),
            ),
            Self::Slot(Slot::SinceMinutes) => Value::from(b.since_minutes.ok_or("since binding")?),
            Self::Slot(Slot::SinceEpochS) => Value::from(b.since.ok_or("since binding")?.timestamp()),
            Self::Slot(Slot::SinceEpochMs) => Value::from(b.since.ok_or("since binding")?.timestamp_millis()),
            Self::Slot(Slot::SinceEpochSText) => {
                Value::from(b.since.ok_or("since binding")?.timestamp().to_string())
            }
            Self::Slot(Slot::SinceEpochMsText) => {
                Value::from(b.since.ok_or("since binding")?.timestamp_millis().to_string())
            }
            Self::Slot(Slot::Parent) => Value::from(b.parent.ok_or("parent binding")?),
            Self::Slot(Slot::Cursor) => b.cursor.map_or(Value::Null, Value::from),
            Self::Slot(Slot::Offset) => Value::from(b.offset.ok_or("offset binding")?),
            Self::Slot(Slot::Page) => Value::from(b.page.ok_or("page binding")?),
            Self::Slot(Slot::PageSize) => Value::from(b.page_size.ok_or("page size binding")?),
            Self::Text(segments) => {
                let mut s = String::new();
                for segment in segments {
                    match segment {
                        Segment::Text(t) => s.push_str(t),
                        Segment::SinceMinutes => {
                            s.push_str(&b.since_minutes.ok_or("since binding")?.to_string())
                        }
                    }
                }
                Value::String(s)
            }
            Self::Array(a) => Value::Array(a.iter().map(|t| t.render(b)).collect::<Result<_, _>>()?),
            Self::Object(o) => Value::Object(
                o.iter()
                    .map(|(k, t)| Ok((k.clone(), t.render(b)?)))
                    .collect::<Result<_, &'static str>>()?,
            ),
        })
    }
    /// True when the call carries the watermark, i.e. the provider filters.
    pub fn uses_since(&self) -> bool {
        let mut used = BTreeSet::new();
        self.slots(&mut used);
        WATERMARK.iter().any(|s| used.contains(s))
    }
}

impl<'de> Deserialize<'de> for Template {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let value = Value::deserialize(d)?;
        if !value.is_object() {
            return Err(serde::de::Error::custom("call args must be a JSON object"));
        }
        Self::parse(&value).map_err(serde::de::Error::custom)
    }
}

fn writes_graphql(s: &str) -> bool {
    s.split(|c: char| !c.is_ascii_alphanumeric() && c != '_')
        .any(|w| w.eq_ignore_ascii_case("mutation") || w.eq_ignore_ascii_case("subscription"))
}

/// Composio GraphQL passthrough tools (LINEAR_RUN_QUERY_OR_MUTATION) take the
/// document in this argument and will run a mutation if given one. The read-only
/// OAuth scope blocks writes at the provider; this keeps the recipe itself from
/// ever asking.
const GRAPHQL_DOCUMENT_ARG: &str = "query_or_mutation";

/// A fixed document whose single operation is an explicit `query`.
fn graphql_query_only(arg: &Template) -> bool {
    let Template::Literal(Value::String(doc)) = arg else {
        return false;
    };
    let doc = doc.trim_start();
    let explicit_query = doc
        .strip_prefix("query")
        .and_then(|rest| rest.chars().next())
        .is_some_and(|c| c.is_whitespace() || c == '(' || c == '{');
    explicit_query && !writes_graphql(doc)
}

/// Verbs that name a provider write. A tool slug containing one is never a
/// read tool, whatever its Composio hints say (LINEAR_CREATE_LINEAR_ISSUE_DETAILS
/// is tagged readOnlyHint).
const WRITE_VERBS: &[&str] = &[
    "ADD", "ARCHIVE", "ASSIGN", "CLOSE", "CREATE", "DELETE", "EDIT", "INVITE", "MERGE", "MOVE",
    "PATCH", "POST", "PUT", "REMOVE", "SEND", "SET", "TRANSITION", "UNARCHIVE", "UPDATE",
    "UPLOAD", "UPSERT", "WRITE",
];

fn slug_ok(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 64
        && s.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
}

enum Site<'a> {
    Namespace,
    Stream(ChangeStrategy, &'a StreamPaging),
    Children(&'a ChildPaging),
}

impl Recipe {
    /// Deployment secret holding this toolkit's read-only auth config id.
    /// `linear` reads `COMPOSIO_LINEAR_AUTH_CONFIG_ID`, as before recipes.
    pub fn auth_config_env(&self) -> String {
        format!("COMPOSIO_{}_AUTH_CONFIG_ID", self.toolkit.to_ascii_uppercase())
    }

    /// True when a granted scope list is non-empty, includes every required
    /// scope and contains nothing outside the recipe's read scopes.
    pub fn grants_read_only(&self, scopes: Option<&Value>) -> bool {
        let values: Vec<&str> = match scopes {
            Some(Value::Array(a)) => match a.iter().map(Value::as_str).collect::<Option<Vec<_>>>() {
                Some(values) => values,
                None => return false,
            },
            Some(Value::String(s)) => s.split([' ', ',']).filter(|s| !s.is_empty()).collect(),
            _ => return false,
        };
        let allowed = |v: &&str| self.scopes.required.iter().chain(&self.scopes.optional).any(|s| s == v);
        !values.is_empty()
            && values.iter().all(|v| allowed(v))
            && self.scopes.required.iter().all(|r| values.contains(&r.as_str()))
    }

    /// Content kinds in capture order, for setup copy.
    pub fn kinds(&self) -> Vec<&str> {
        let mut kinds = Vec::new();
        for stream in &self.streams {
            kinds.push(stream.kind.as_str());
            if let Some(children) = &stream.children {
                kinds.push(children.kind.as_str());
            }
        }
        kinds
    }

    fn calls(&self) -> Vec<(Site<'_>, &Call)> {
        let mut calls = match &self.namespace.source {
            NamespaceSource::Lookup { call, .. } => vec![(Site::Namespace, call)],
            NamespaceSource::Match { anchor, candidates, .. } => {
                vec![(Site::Namespace, &anchor.call), (Site::Namespace, &candidates.call)]
            }
            NamespaceSource::Connection { .. } => vec![],
        };
        for stream in &self.streams {
            calls.push((Site::Stream(stream.change, &stream.paging), &stream.call));
            if let Some(children) = &stream.children {
                calls.push((Site::Children(&children.paging), &children.call));
            }
        }
        calls
    }

    fn validate(&self) -> Result<(), String> {
        let t = &self.toolkit;
        if !slug_ok(t) {
            return Err(format!("toolkit slug {t:?} must be [a-z0-9_]"));
        }
        if let Some((_, native)) = NATIVE_TOOLKITS.iter().find(|(slug, _)| slug == t) {
            return Err(format!(
                "{t}: CoralSwarm captures {native} natively; a Composio recipe would capture it twice"
            ));
        }
        if self.streams.is_empty() {
            return Err(format!("{t}: at least one change stream"));
        }
        let mut kinds = BTreeSet::new();
        for kind in self.kinds() {
            if !slug_ok(kind) {
                return Err(format!("{t}: kind {kind:?} must be [a-z0-9_]"));
            }
            if !kinds.insert(kind) {
                return Err(format!("{t}: content kinds must be distinct"));
            }
        }
        for fields in self
            .streams
            .iter()
            .flat_map(|s| std::iter::once(&s.fields).chain(s.children.as_ref().map(|c| &c.fields)))
        {
            if let Some(container) = &fields.container {
                if !slug_ok(&container.kind) {
                    return Err(format!("{t}: container kind {:?} must be [a-z0-9_]", container.kind));
                }
            }
        }
        for stream in &self.streams {
            stream.capture.validate(t, &stream.kind, &stream.fields, None)?;
            if let Some(children) = &stream.children {
                children.capture.validate(t, &children.kind, &children.fields, Some(&stream.fields))?;
            }
        }
        for stream in &self.streams {
            if stream.render == Render::Snapshot
                && (stream.children.is_some() || stream.fields.container.is_none())
            {
                return Err(format!(
                    "{t}: snapshot stream {} needs its own container id and no children",
                    stream.kind
                ));
            }
            if stream.children.is_some() && stream.fields.parent.is_some() {
                return Err(format!("{t}: stream {} cannot both have and be a child", stream.kind));
            }
        }
        if let NamespaceSource::Connection { id, host } = &self.namespace.source {
            let mut leaves = BTreeSet::new();
            for pointer in id.pointers() {
                match pointer.0.split('/').collect::<Vec<_>>()[..] {
                    ["", "params" | "data", leaf] if CONNECTION_FIELDS.contains(&leaf) => leaves.insert(leaf),
                    _ => {
                        return Err(format!(
                            "{t}: a connection namespace reads only /params or /data {}",
                            CONNECTION_FIELDS.join(", ")
                        ))
                    }
                };
            }
            let [leaf] = leaves.into_iter().collect::<Vec<_>>()[..] else {
                return Err(format!("{t}: a connection namespace names exactly one field"));
            };
            if *host != CONNECTION_URL_FIELDS.contains(&leaf) {
                return Err(format!("{t}: a connection namespace takes the host of a URL field (\"host\": true) and the value of any other"));
            }
        }
        if self.version.trim().is_empty() || self.label.trim().is_empty() {
            return Err(format!("{t}: version and label are required"));
        }
        if self.scopes.required.is_empty() {
            return Err(format!("{t}: at least one required read scope"));
        }
        let prefix = format!("{}_", t.to_ascii_uppercase());
        for tool in &self.read_tools {
            let Some(rest) = tool.strip_prefix(&prefix) else {
                return Err(format!("{t}: tool {tool} is outside the toolkit"));
            };
            if let Some(verb) = rest.split('_').find(|w| WRITE_VERBS.contains(w)) {
                return Err(format!("{t}: tool {tool} names write verb {verb}"));
            }
        }
        let calls = self.calls();
        let used_tools: BTreeSet<&String> = calls.iter().map(|(_, c)| &c.tool).collect();
        if let Some(unused) = self.read_tools.iter().find(|tool| !used_tools.contains(tool)) {
            return Err(format!("{t}: read tool {unused} is never called"));
        }
        for (site, call) in calls {
            let name = match site {
                Site::Namespace => "namespace",
                Site::Stream(..) => "stream",
                Site::Children(_) => "children",
            };
            if !self.read_tools.contains(&call.tool) {
                return Err(format!("{t}: {name} tool {} is not in read_tools", call.tool));
            }
            if let Template::Object(args) = &call.args {
                if let Some((_, doc)) = args.iter().find(|(k, _)| k == GRAPHQL_DOCUMENT_ARG) {
                    if !graphql_query_only(doc) {
                        return Err(format!(
                            "{t}: {name} {GRAPHQL_DOCUMENT_ARG} must be a fixed GraphQL query operation"
                        ));
                    }
                }
            }
            let mut used = BTreeSet::new();
            call.args.slots(&mut used);
            let (allowed, required): (Vec<Slot>, Vec<Slot>) = match site {
                Site::Namespace => (vec![], vec![]),
                Site::Stream(change, paging) => {
                    let mut allowed = vec![Slot::PageSize];
                    // Sort-and-stop finds the watermark itself; a since
                    // argument would make it a filter.
                    if change == ChangeStrategy::Filter {
                        allowed.extend(WATERMARK);
                    }
                    // Numbered paging stops on counts, so the provider must
                    // page at the engine's size.
                    let required = match paging {
                        StreamPaging::Cursor(_) => vec![Slot::Cursor],
                        StreamPaging::Numbered { by: Position::Offset, .. } => vec![Slot::Offset, Slot::PageSize],
                        StreamPaging::Numbered { by: Position::Page, .. } => vec![Slot::Page, Slot::PageSize],
                    };
                    allowed.extend(&required);
                    (allowed, required)
                }
                Site::Children(ChildPaging::Offset { .. }) => {
                    (vec![Slot::Parent, Slot::Offset], vec![Slot::Parent, Slot::Offset])
                }
                Site::Children(ChildPaging::Cursor { .. }) => {
                    (vec![Slot::Parent, Slot::Cursor], vec![Slot::Parent, Slot::Cursor])
                }
                Site::Children(ChildPaging::Single {}) => (vec![Slot::Parent], vec![Slot::Parent]),
            };
            if let Some(slot) = used.iter().find(|s| !allowed.contains(s)) {
                return Err(format!("{t}: {name} cannot use slot {slot:?}"));
            }
            if let Some(slot) = required.iter().find(|s| !used.contains(s)) {
                return Err(format!("{t}: {name} must use slot {slot:?}"));
            }
            if matches!(site, Site::Stream(ChangeStrategy::Filter, _))
                && WATERMARK.iter().filter(|s| used.contains(s)).count() != 1
            {
                return Err(format!(
                    "{t}: a filter stream passes the watermark exactly once, as one of {{{{since}}}}, {{{{since_minutes}}}}, {{{{since_epoch_s}}}}, {{{{since_epoch_ms}}}}, {{{{since_epoch_s_text}}}} or {{{{since_epoch_ms_text}}}}"
                ));
            }
        }
        Ok(())
    }
}

fn load(sources: &[(&str, &str)]) -> Result<BTreeMap<String, Recipe>, String> {
    let mut registry = BTreeMap::new();
    for (name, text) in sources {
        let recipe: Recipe = serde_json::from_str(text).map_err(|e| format!("{name}: {e}"))?;
        if recipe.toolkit != *name {
            return Err(format!("{name}: file name must equal toolkit slug"));
        }
        recipe.validate()?;
        if registry.insert(recipe.toolkit.clone(), recipe).is_some() {
            return Err(format!("{name}: duplicate toolkit"));
        }
    }
    Ok(registry)
}

#[cfg(test)]
pub(super) fn load_for_test(name: &str, recipe: &Value) -> Result<Recipe, String> {
    let text = recipe.to_string();
    load(&[(name, &text)]).map(|mut r| r.remove(name).unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn linear() -> Value {
        serde_json::from_str(SOURCES[0].1).unwrap()
    }
    fn jira() -> Value {
        serde_json::from_str(SOURCES[1].1).unwrap()
    }
    fn parse(v: Value) -> Result<(), String> {
        let name = v["toolkit"].as_str().unwrap_or("linear").to_owned();
        load_for_test(&name, &v).map(|_| ())
    }

    #[test]
    fn checked_in_recipes_load_and_cover_every_recipe_file() {
        assert_eq!(all().count(), SOURCES.len());
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/connectors/composio/recipes");
        let mut files: Vec<String> = std::fs::read_dir(dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().trim_end_matches(".json").to_owned())
            .collect();
        files.sort();
        let mut registered: Vec<String> = SOURCES.iter().map(|(n, _)| n.to_string()).collect();
        registered.sort();
        assert_eq!(files, registered, "every recipes/*.json must be registered in SOURCES");
    }

    #[test]
    fn auth_config_env_keeps_existing_secret_names() {
        assert_eq!(get("linear").unwrap().auth_config_env(), "COMPOSIO_LINEAR_AUTH_CONFIG_ID");
        assert_eq!(get("jira").unwrap().auth_config_env(), "COMPOSIO_JIRA_AUTH_CONFIG_ID");
    }

    #[test]
    fn scope_check_reads_the_recipe() {
        let linear = get("linear").unwrap();
        let jira = get("jira").unwrap();
        assert!(linear.grants_read_only(Some(&json!(["read"]))));
        assert!(!linear.grants_read_only(Some(&json!(["read", "write"]))));
        assert!(!linear.grants_read_only(Some(&json!([]))));
        assert!(jira.grants_read_only(Some(&json!("read:jira-work offline_access"))));
        assert!(jira.grants_read_only(Some(&json!("read:jira-work"))));
        assert!(!jira.grants_read_only(Some(&json!(["offline_access"]))));
        assert!(!jira.grants_read_only(Some(&json!(["write:jira-work"]))));
        assert!(!jira.grants_read_only(None));
        assert!(!linear.grants_read_only(Some(&json!(["read", {"write": true}]))));
    }

    #[test]
    fn native_toolkits_are_refused_at_load() {
        for (slug, _) in NATIVE_TOOLKITS {
            let mut v = linear();
            v["toolkit"] = json!(slug);
            let tool = json!(format!("{}_RUN_QUERY_OR_MUTATION", slug.to_ascii_uppercase()));
            v["read_tools"] = json!([tool]);
            v["namespace"]["source"]["lookup"]["call"]["tool"] = tool.clone();
            for stream in v["streams"].as_array_mut().unwrap() {
                stream["call"]["tool"] = tool.clone();
            }
            let err = load_for_test(slug, &v).unwrap_err();
            assert!(err.contains("natively"), "{slug}: {err}");
        }
        for slug in ["slack", "gmail", "googlecalendar", "googledrive", "googledocs", "google_chat", "github", "microsoft_teams", "outlook"] {
            assert!(NATIVE_TOOLKITS.iter().any(|(s, _)| *s == slug), "{slug}");
        }
    }

    #[test]
    fn change_strategy_has_no_full_scan() {
        for (strategy, ok) in [("filter", true), ("sort_stop", true), ("full_scan", false), ("none", false)] {
            let parsed = serde_json::from_value::<ChangeStrategy>(json!(strategy));
            assert_eq!(parsed.is_ok(), ok, "{strategy}");
        }
        let mut v = linear();
        v["streams"][0].as_object_mut().unwrap().remove("change");
        assert!(parse(v).is_err(), "a stream without a change strategy does not load");
        // A filter stream that never passes the watermark would be a full scan.
        let mut v = linear();
        v["streams"][0]["call"]["args"]["variables"]
            .as_object_mut()
            .unwrap()
            .remove("since");
        assert!(parse(v).unwrap_err().contains("exactly once"));
        // Sort-and-stop finds the watermark by order, never by argument.
        let mut v = linear();
        v["streams"][0]["change"] = json!("sort_stop");
        assert!(parse(v).unwrap_err().contains("cannot use slot Since"));
    }

    #[test]
    fn rejects_write_tools_foreign_toolkits_and_unused_tools() {
        let mut v = linear();
        v["read_tools"].as_array_mut().unwrap().push(json!("LINEAR_CREATE_LINEAR_ISSUE_DETAILS"));
        assert!(parse(v).unwrap_err().contains("write verb"));
        let mut v = linear();
        v["read_tools"].as_array_mut().unwrap().push(json!("JIRA_SEARCH_ISSUES"));
        assert!(parse(v).unwrap_err().contains("outside the toolkit"));
        let mut v = linear();
        v["streams"][0]["call"]["tool"] = json!("LINEAR_LIST_LINEAR_TEAMS");
        assert!(parse(v).unwrap_err().contains("not in read_tools"));
        let mut v = linear();
        v["read_tools"].as_array_mut().unwrap().push(json!("LINEAR_LIST_LINEAR_ISSUES"));
        assert!(parse(v).unwrap_err().contains("never called"));
    }

    #[test]
    fn rejects_graphql_writes_and_unsafe_slots() {
        let mut v = linear();
        v["namespace"]["source"]["lookup"]["call"]["args"]["query_or_mutation"] =
            json!("mutation { issueDelete(id: \"x\") { success } }");
        assert!(parse(v).unwrap_err().contains("not read-only"));
        let mut v = linear();
        v["streams"][0]["call"]["args"]["variables"]["after"] = json!("prefix {{cursor}}");
        assert!(parse(v).unwrap_err().contains("cannot be embedded"));
        let mut v = linear();
        v["streams"][0]["call"]["args"]["variables"]["since"] = json!("{{since}} ago");
        assert!(parse(v).unwrap_err().contains("cannot be embedded"));
        let mut v = linear();
        v["streams"][0]["call"]["args"]["variables"]["x"] = json!("{{unknown}}");
        assert!(parse(v).is_err());
        let mut v = linear();
        v["namespace"]["source"]["lookup"]["call"]["args"]["after"] = json!("{{cursor}}");
        assert!(parse(v).unwrap_err().contains("cannot use slot"));
        let mut v = linear();
        v["streams"][0]["call"]["args"]["variables"]
            .as_object_mut()
            .unwrap()
            .remove("after");
        assert!(parse(v).unwrap_err().contains("must use slot"));
        let mut v = jira();
        v["streams"][0]["call"]["args"]["jql"] = json!("updated >= -{{since_minutes}}m OR mutation");
        assert!(parse(v).unwrap_err().contains("not read-only"));
    }

    #[test]
    fn graphql_document_arg_must_be_a_fixed_query_operation() {
        let set = |doc: Value| {
            let mut v = linear();
            v["streams"][1]["call"]["args"]["query_or_mutation"] = doc;
            parse(v)
        };
        for doc in [
            json!("mutation CoralSwarm { issueDelete(id: \"x\") { success } }"),
            json!("{ organization { id } }"),
            json!("subscription S { issueUpdates { id } }"),
            json!("query Q { a } mutation M { issueDelete(id: \"x\") { success } }"),
            json!("queryX { a }"),
            json!("{{cursor}}"),
            json!(["query Q { a }"]),
            json!(7),
        ] {
            let err = set(doc.clone()).unwrap_err();
            assert!(
                err.contains("fixed GraphQL query") || err.contains("not read-only") || err.contains("cannot use slot"),
                "{doc}: {err}"
            );
        }
        let mut v = linear();
        v["namespace"]["source"]["lookup"]["call"]["args"]["query_or_mutation"] =
            json!("  query($x: String) { organization { id } }");
        assert!(parse(v).is_ok());
    }

    fn zendesk() -> Value {
        serde_json::from_str(SOURCES[2].1).unwrap()
    }

    #[test]
    fn every_checked_in_stream_and_child_has_capture_hints() {
        for r in all() {
            for stream in &r.streams {
                assert!(!stream.capture.title.is_empty(), "{} {}: title hints", r.toolkit, stream.kind);
                if let Some(children) = &stream.children {
                    assert!(!children.capture.title.is_empty(), "{} {}: title hints", r.toolkit, children.kind);
                }
            }
        }
    }

    #[test]
    fn chunk_hints_are_found_by_source_type_and_namespace() {
        assert_eq!(chunk_hint_for("linear_comment", "linear:workspace:w"), Some(ChunkHint::Document));
        assert_eq!(chunk_hint_for("zendesk_comment", "zendesk:host:acme.zendesk.com"), Some(ChunkHint::Document));
        assert_eq!(chunk_hint_for("linear_comment", "slack:workspace:T1"), None, "another provider's item");
        assert_eq!(chunk_hint_for("linear_unknown", "linear:workspace:w"), None);
        assert_eq!(chunk_hint_for("slack", "slack:workspace:T1"), None);
    }

    #[test]
    fn capture_hints_default_to_document_chunks_and_content_titles() {
        let mut v = linear();
        for stream in v["streams"].as_array_mut().unwrap() {
            stream.as_object_mut().unwrap().remove("capture");
        }
        let r = load_for_test("linear", &v).unwrap();
        assert!(r.streams.iter().all(|s| s.capture.chunk == ChunkHint::Document && s.capture.title.is_empty()));
        assert!(matches!(ChunkHint::Document.strategy(), coralswarm_core::chunker::ChunkStrategy::Generic));
        assert!(matches!(ChunkHint::Email.strategy(), coralswarm_core::chunker::ChunkStrategy::Email));
    }

    /// A malformed hint fails the recipe at load, so the registry never
    /// serves a toolkit whose titles or chunking would be guessed.
    #[test]
    fn malformed_capture_hints_fail_closed() {
        let cases: Vec<(Value, &str)> = vec![
            ({ let mut v = linear(); v["streams"][0]["capture"]["chunk"] = json!("chat"); v }, "unknown variant"),
            ({ let mut v = linear(); v["streams"][0]["capture"]["chunk"] = json!("transcript"); v }, "unknown variant"),
            ({ let mut v = linear(); v["streams"][0]["capture"]["surprise"] = json!(1); v }, "unknown field"),
            ({ let mut v = linear(); v["streams"][0]["capture"]["title"] = json!(["{titel}"]); v }, "unknown slot"),
            ({ let mut v = linear(); v["streams"][0]["capture"]["title"] = json!(["{title"]); v }, "unclosed"),
            ({ let mut v = linear(); v["streams"][0]["capture"]["title"] = json!(["title}"]); v }, "unmatched"),
            ({ let mut v = linear(); v["streams"][0]["capture"]["title"] = json!(["Issue"]); v }, "names no slot"),
            ({ let mut v = linear(); v["streams"][0]["capture"]["title"] = json!("{title}"); v }, "invalid type"),
            // The project stream has no reference field to fill the slot.
            ({ let mut v = linear(); v["streams"][2]["capture"]["title"] = json!(["{reference}"]); v }, "no source field"),
            // An issue has no parent: no parent paths, no parent slots.
            ({ let mut v = linear(); v["streams"][0]["capture"]["parent_title"] = json!("/x"); v }, "has no parent"),
            ({ let mut v = linear(); v["streams"][0]["capture"]["title"] = json!(["{parent_title}"]); v }, "no source field"),
            // A comment names its parent only through a declared path.
            ({ let mut v = linear(); v["streams"][1]["capture"].as_object_mut().unwrap().remove("parent_title"); v }, "no source field"),
            // A child list takes parent facts from its parent item, never paths.
            ({ let mut v = zendesk(); v["streams"][0]["children"]["capture"]["parent_title"] = json!("/x"); v }, "child list"),
            // A Zendesk ticket has no reference, so its comments cannot name one.
            ({ let mut v = zendesk(); v["streams"][0]["children"]["capture"]["title"] = json!(["{parent_reference}"]); v }, "no source field"),
        ];
        for (recipe, want) in cases {
            let err = parse(recipe).expect_err(want);
            assert!(err.contains(want), "{want}: {err}");
        }
    }

    #[test]
    fn title_templates_take_the_first_one_that_fully_resolves() {
        let r = get("linear").unwrap();
        let comment = &r.streams[1].capture;
        let facts = TitleFacts { parent_title: Some(" Fresh\n decision "), parent_reference: Some("COR-1"), excerpt: Some("Agreed"), ..Default::default() };
        assert_eq!(comment.title(&facts).as_deref(), Some("Comment on COR-1: Fresh decision"));
        let facts = TitleFacts { parent_title: Some("Fresh decision"), parent_reference: Some("  "), ..facts };
        assert_eq!(comment.title(&facts).as_deref(), Some("Comment on Fresh decision"));
        let facts = TitleFacts { parent_title: None, ..facts };
        assert_eq!(comment.title(&facts).as_deref(), Some("Comment: Agreed"));
        assert_eq!(comment.title(&TitleFacts::default()), None, "nothing resolves: derive from content");
    }

    #[test]
    fn rejects_malformed_shapes() {
        let mut v = linear();
        v["streams"][0]["paging"]["is_last"] = json!("/isLast");
        assert!(parse(v).unwrap_err().contains("exactly one"));
        let mut v = linear();
        v["streams"][0]["fields"]["id"] = json!("id");
        assert!(parse(v).is_err());
        let mut v = linear();
        v["surprise"] = json!(true);
        assert!(parse(v).is_err());
        let mut v = linear();
        v["toolkit"] = json!("jira");
        assert!(load_for_test("linear", &v).unwrap_err().contains("file name"));
        let mut v = linear();
        v["scopes"]["required"] = json!([]);
        assert!(parse(v).is_err());
        let mut v = linear();
        v["streams"] = json!([]);
        assert!(parse(v).unwrap_err().contains("at least one"));
        let mut v = linear();
        v["streams"][1]["kind"] = json!("issue");
        assert!(parse(v).unwrap_err().contains("distinct"));
    }

    fn acme_stream(paging: Value, args: Value) -> Value {
        json!({"toolkit":"acme","label":"Acme","version":"v1","scopes":{"required":["read"]},
            "read_tools":["ACME_GET_ACCOUNT","ACME_SEARCH","ACME_LIST_PARTS"],
            "namespace":{"kind":"workspace","source":{"lookup":{"call":{"tool":"ACME_GET_ACCOUNT","args":{}},"id":"/id"}}},
            "streams":[{"kind":"item","change":"filter","call":{"tool":"ACME_SEARCH","args":args},"items":"/results","paging":paging,
                "fields":{"id":"/id","updated_at":"/updated_at"},
                "children":{"kind":"part","call":{"tool":"ACME_LIST_PARTS","args":{"item":"{{parent}}","after":"{{cursor}}"}},
                    "page":"","items":"/parts","paging":{"style":"cursor","next":"/next"},"fields":{"id":"/id","updated_at":"/updated_at"}}}]})
    }

    #[test]
    fn numbered_stream_paging_rules() {
        let offset = json!({"style":"offset","total":"/total"});
        let ok = json!({"since":"{{since}}","limit":"{{page_size}}","start":"{{offset}}"});
        assert!(parse(acme_stream(offset.clone(), ok.clone())).is_ok());
        assert!(parse(acme_stream(json!({"style":"page"}), json!({"since":"{{since}}","limit":"{{page_size}}","page":"{{page}}"}))).is_ok());
        let cases = [
            (offset.clone(), json!({"since":"{{since}}","limit":"{{page_size}}"}), "must use slot Offset"),
            (offset.clone(), json!({"since":"{{since}}","start":"{{offset}}"}), "must use slot PageSize"),
            (offset.clone(), json!({"since":"{{since}}","limit":"{{page_size}}","start":"{{offset}}","after":"{{cursor}}"}), "cannot use slot Cursor"),
            (offset.clone(), json!({"since":"{{since}}","limit":"{{page_size}}","start":"{{page}}"}), "cannot use slot Page"),
            (json!({"style":"page"}), ok.clone(), "cannot use slot Offset"),
            (json!({"next":"/next"}), json!({"since":"{{since}}","after":"{{cursor}}","start":"{{offset}}"}), "cannot use slot Offset"),
            (json!({"style":"offset","has_more":"/more"}), ok.clone(), "end by count"),
            (json!({"style":"offset","next":"/next"}), ok.clone(), "end by count"),
            (json!({"style":"scroll"}), ok.clone(), "unknown stream paging style"),
            (json!({"next":"/next","total":"/total"}), ok.clone(), "no total"),
            (json!({"style":"offset","total":"total"}), ok.clone(), "must start with /"),
            (offset.clone(), json!({"since":"{{since}}","limit":"{{page_size}}","start":"from {{offset}}"}), "cannot be embedded"),
        ];
        for (paging, args, want) in cases {
            let err = parse(acme_stream(paging.clone(), args.clone())).unwrap_err();
            assert!(err.contains(want), "{paging} {args}: {err}");
        }
        let mut v = acme_stream(offset.clone(), ok.clone());
        v["streams"][0]["children"]["call"]["args"]["page"] = json!("{{page}}");
        assert!(parse(v).unwrap_err().contains("children cannot use slot Page"));
        let mut v = acme_stream(offset, ok);
        v["namespace"]["source"]["lookup"]["call"]["args"]["start"] = json!("{{offset}}");
        assert!(parse(v).unwrap_err().contains("namespace cannot use slot Offset"));
    }

    #[test]
    fn epoch_watermark_slots_are_whole_engine_computed_values() {
        let since = DateTime::parse_from_rfc3339("2026-10-12T00:00:00.250Z").unwrap().with_timezone(&Utc);
        let args = json!({"s":"{{since_epoch_s}}","ms":"{{since_epoch_ms}}","s_text":"{{since_epoch_s_text}}","ms_text":"{{since_epoch_ms_text}}"});
        let rendered = serde_json::from_value::<Template>(args)
            .unwrap()
            .render(&Bindings { since: Some(since), ..Default::default() })
            .unwrap();
        assert_eq!(rendered, json!({"s":1791763200,"ms":1791763200250u64,"s_text":"1791763200","ms_text":"1791763200250"}));
        let filter = |args: Value| {
            let mut v = acme_stream(json!({"next":"/next"}), args);
            v["streams"][0].as_object_mut().unwrap().remove("children");
            v["read_tools"] = json!(["ACME_GET_ACCOUNT", "ACME_SEARCH"]);
            v
        };
        for slot in ["since_epoch_s", "since_epoch_ms", "since_epoch_s_text", "since_epoch_ms_text"] {
            let v = filter(json!({"value":format!("{{{{{slot}}}}}"),"after":"{{cursor}}"}));
            assert!(get_uses_since(&v), "{slot}");
            let err = parse(filter(json!({"value":format!("updated>{{{{{slot}}}}}"),"after":"{{cursor}}"}))).unwrap_err();
            assert!(err.contains("cannot be embedded"), "{slot}: {err}");
            let mut v = filter(json!({"value":format!("{{{{{slot}}}}}"),"after":"{{cursor}}"}));
            v["streams"][0]["change"] = json!("sort_stop");
            assert!(parse(v).unwrap_err().contains("cannot use slot SinceEpoch"), "{slot}");
        }
        let err = parse(filter(json!({"a":"{{since}}","b":"{{since_epoch_ms}}","after":"{{cursor}}"}))).unwrap_err();
        assert!(err.contains("exactly once"), "{err}");
        let err = parse(filter(json!({"after":"{{cursor}}"}))).unwrap_err();
        assert!(err.contains("exactly once"), "{err}");
    }

    fn get_uses_since(v: &Value) -> bool {
        load_for_test("acme", v).unwrap().streams[0].call.args.uses_since()
    }

    #[test]
    fn connection_namespace_reads_only_allowlisted_fields() {
        let with = |source: Value| {
            let mut v = linear();
            v["namespace"]["source"] = json!({"connection":source});
            v
        };
        let mut ok = linear();
        ok["namespace"]["source"] = json!({"connection":{"id":["/params/base_url","/data/base_url"],"host":true}});
        // The lookup tool is still called by the streams.
        assert!(parse(ok).is_ok());
        assert!(parse(with(json!({"id":"/params/subdomain"}))).is_ok());
        for (source, want) in [
            (json!({"id":"/data/access_token"}), "reads only"),
            (json!({"id":"/params/refresh_token"}), "reads only"),
            (json!({"id":"/params/headers/Authorization"}), "reads only"),
            (json!({"id":"/state/val/subdomain"}), "reads only"),
            (json!({"id":"/params/subdomain/0"}), "reads only"),
            (json!({"id":"/subdomain"}), "reads only"),
            (json!({"id":""}), "reads only"),
            (json!({"id":["/params/subdomain","/data/base_url"],"host":true}), "exactly one field"),
            (json!({"id":[]}), "exactly one field"),
            (json!({"id":"/params/subdomain","host":true}), "takes the host"),
            (json!({"id":"/params/base_url"}), "takes the host"),
        ] {
            let err = parse(with(source.clone())).unwrap_err();
            assert!(err.contains(want), "{source}: {err}");
        }
        assert!(parse(with(json!({"id":"/params/subdomain","call":{"tool":"LINEAR_RUN_QUERY_OR_MUTATION","args":{}}}))).is_err());
    }

    #[test]
    fn single_page_children_take_only_the_parent() {
        let with = |paging: Value, args: Value| {
            let mut v = acme_stream(json!({"next":"/next"}), json!({"since":"{{since}}","after":"{{cursor}}"}));
            v["streams"][0]["children"]["paging"] = paging;
            v["streams"][0]["children"]["call"]["args"] = args;
            parse(v)
        };
        let single = json!({"style":"single"});
        assert!(with(single.clone(), json!({"conversation_id":"{{parent}}","display_as":"plaintext"})).is_ok());
        for (args, want) in [
            (json!({"conversation_id":"{{parent}}","after":"{{cursor}}"}), "children cannot use slot Cursor"),
            (json!({"conversation_id":"{{parent}}","start":"{{offset}}"}), "children cannot use slot Offset"),
            (json!({"conversation_id":"{{parent}}","limit":"{{page_size}}"}), "children cannot use slot PageSize"),
            (json!({"display_as":"plaintext"}), "children must use slot Parent"),
        ] {
            let err = with(single.clone(), args.clone()).unwrap_err();
            assert!(err.contains(want), "{args}: {err}");
        }
        assert!(with(json!({"style":"single","total":"/total_count"}), json!({"id":"{{parent}}"})).is_err());
        assert!(with(json!({"style":"none"}), json!({"id":"{{parent}}"})).is_err());
    }

    #[test]
    fn jql_renders_only_an_engine_computed_minute_count() {
        let jira = get("jira").unwrap();
        let args = jira.streams[0]
            .call
            .args
            .render(&Bindings {
                since_minutes: Some(20),
                cursor: None,
                page_size: Some(10),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(args["jql"], "updated >= -20m ORDER BY updated ASC");
        assert_eq!(args["next_page_token"], Value::Null);
        assert_eq!(args["max_results"], 10);
    }

    /// Validation lever for `scripts/composio/draft_recipe.mjs` drafts:
    /// `COMPOSIO_DRAFT_RECIPES=<file-or-dir>[:...] cargo test --lib validate_draft_recipes -- --ignored --nocapture`.
    #[test]
    #[ignore = "set COMPOSIO_DRAFT_RECIPES to draft files or directories"]
    fn validate_draft_recipes() {
        let paths = std::env::var_os("COMPOSIO_DRAFT_RECIPES").expect("COMPOSIO_DRAFT_RECIPES");
        let mut files = Vec::new();
        for p in std::env::split_paths(&paths) {
            if p.is_dir() {
                let mut dir: Vec<_> = std::fs::read_dir(&p)
                    .unwrap()
                    .map(|e| e.unwrap().path())
                    .filter(|f| f.extension().is_some_and(|x| x == "json"))
                    .collect();
                dir.sort();
                files.extend(dir);
            } else {
                files.push(p);
            }
        }
        let mut failed = 0;
        for file in &files {
            let name = file.file_stem().unwrap().to_string_lossy().into_owned();
            let text = std::fs::read_to_string(file).unwrap();
            match serde_json::from_str::<Value>(&text).map_err(|e| e.to_string()).and_then(|v| load_for_test(&name, &v)) {
                Ok(_) => println!("DRAFT_OK {name}"),
                Err(e) => {
                    failed += 1;
                    println!("DRAFT_FAIL {name}: {e}");
                }
            }
        }
        println!("DRAFT_SUMMARY {} ok, {failed} failed", files.len() - failed);
        assert!(!files.is_empty(), "no draft files found");
        assert_eq!(failed, 0, "{failed} draft(s) failed recipe.rs validation");
    }

    #[test]
    fn since_renders_as_utc_milliseconds() {
        let linear = get("linear").unwrap();
        let since = DateTime::parse_from_rfc3339("2026-10-13T20:05:00+02:00").unwrap().with_timezone(&Utc);
        let args = linear.streams[0]
            .call
            .args
            .render(&Bindings {
                since: Some(since),
                page_size: Some(25),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(args["variables"]["since"], "2026-10-13T18:05:00.000Z");
        assert_eq!(args["variables"]["after"], Value::Null);
        assert!(linear.streams.iter().all(|s| s.call.args.uses_since()));
    }
}
