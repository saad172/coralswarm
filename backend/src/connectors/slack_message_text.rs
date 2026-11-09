//! Flatten a Slack message object into the one plain-text string the ingest
//! pipeline stores as coral content.
//!
//! # Why this exists
//!
//! Until this module landed, both Slack ingest funnels
//! (`routes::slack_events`' live webhook and `routes::slack_interactivity`'s
//! `backfill_channel`) read `event["text"]` and dropped the message when it
//! was empty. That is fine for a human typing into a channel — Slack always
//! populates `text` there — and useless for an **app**: measured in workspace
//! the measured workspace over 2026-08-01 → 2026-09-09, **235 of 253** bot-authored
//! messages carried an EMPTY `text`, with the entire human-readable payload
//! living in `attachments[]` (a GitHub-shaped app) or `blocks[]` (two others). Lifting
//! the `bot_id` filter alone would therefore have changed nothing.
//!
//! # The rules (each one is a measurement, not a preference)
//!
//! 1. **`attachments[].fallback` is not a substitute for the real fields.**
//!    On the captured `PR opened` payload `fallback` is 54 characters against
//!    8232 in `pretext` + `title` + `text` — 0.7% — and it contains neither
//!    the PR number nor its title. So this module flattens `pretext`,
//!    `title` (+ `title_link`), `text` and `fields[]`, and reaches for
//!    `fallback` ONLY when every one of those is empty.
//! 2. **Link-preview unfurls are not content.** Slack attaches an unfurl
//!    (`is_msg_unfurl: true`, or any attachment carrying `from_url`) to a
//!    HUMAN message that merely contained a link. Flattening those would
//!    paste a stranger's web page into the ocean under the human's name, so
//!    they are skipped.
//! 3. **`root` is never read.** A `thread_broadcast` message carries the FULL
//!    parent message under `root`; flattening it would ingest every merged
//!    PR twice (once as the threaded reply, once as the broadcast).
//! 4. **`text` and `blocks` are usually the same content twice.** Slack's
//!    `text` is the notification fallback for a Block Kit message, so a
//!    `rich_text` block is normally a mirror of it — but not always: the
//!    Claude app's `text` is the first SENTENCE of a message whose `blocks`
//!    carry ~15x more. [`pick_body`] therefore keeps the richer of the two
//!    when they are mirrors, and concatenates when they are genuinely
//!    different.
//!
//! # What this module deliberately does NOT do
//!
//! No Slack mrkdwn normalisation. `<@U123>` mentions, `<url|label>` links and
//! backtick code spans are stored exactly as Slack sent them — because that
//! is already true of every HUMAN message ingested today, and introducing
//! cleanup here alone would make bot atoms and human atoms disagree about
//! what a Slack link looks like. Normalising both is a separate change.

use serde_json::Value;

/// Hard cap on the flattened result. A single GitHub release note or a long
/// CI log pasted by a bot can be far larger than any human message; the cap
/// keeps one pathological payload from dominating a job payload (which is
/// stored in `jobs.payload`) and the chunker's input.
pub(crate) const MAX_FLATTENED_BYTES: usize = 32 * 1024;

/// Appended when [`MAX_FLATTENED_BYTES`] truncates the result, so a reader of
/// the resulting coral can tell content was cut rather than that the bot
/// stopped mid-sentence.
pub(crate) const TRUNCATION_MARKER: &str = "\n\n[truncated]";

/// Flatten one Slack message object (a `conversations.history` message OR an
/// `event_callback` event — the two share the `text`/`blocks`/`attachments`
/// shape) into the text to ingest.
///
/// Returns `None` when nothing readable exists, which is the caller's signal
/// to drop the message exactly as the old empty-`text` guard did.
pub(crate) fn flatten_slack_message(event: &Value) -> Option<String> {
    // Rule 2 applies to HUMAN messages only — see [`flatten_attachment`].
    let author_is_app = is_bot_authored_event(event);
    let text = event.get("text").and_then(Value::as_str).unwrap_or("");
    let blocks_text = event
        .get("blocks")
        .and_then(Value::as_array)
        .map(|b| flatten_blocks(b))
        .unwrap_or_default();

    let mut sections: Vec<String> = Vec::new();

    let body = pick_body(text, &blocks_text);
    if !body.trim().is_empty() {
        sections.push(body);
    }

    if let Some(attachments) = event.get("attachments").and_then(Value::as_array) {
        for att in attachments {
            if let Some(flat) = flatten_attachment(att, author_is_app) {
                sections.push(flat);
            }
        }
    }

    let joined = sections.join("\n\n");
    let collapsed = collapse_blank_runs(&joined);
    let trimmed = collapsed.trim();
    if trimmed.is_empty() {
        return None;
    }
    Some(cap(trimmed))
}

/// Choose between the message's `text` and its flattened `blocks`.
///
/// Slack populates `text` as the *notification fallback* for a Block Kit
/// message, so the two are usually the same words — but each rendering loses
/// something the other keeps: `text` carries the mrkdwn markers (`**bold**`,
/// backticks, `- ` bullets) that `rich_text` encodes as structure, while a
/// `rich_text` link node keeps only the label where `text` keeps the whole
/// `<url|label>`. A literal string comparison therefore never says "mirror"
/// even when the two are the same message, and neither string ever contains
/// the other.
///
/// So the mirror test is on the WORD SET, not the characters: when ≥90% of
/// the shorter side's words also appear in the longer side, the two are the
/// same content and only the LONGER one is kept (ties go to `text`, so a
/// plain human message round-trips byte-for-byte). Otherwise they are
/// genuinely different and both are kept.
///
/// Measured against the fixtures: `human_plain` and `app_rich_text_blocks`
/// keep `text`; `claude_app_message` keeps `blocks` (44 vs 657 alphanumeric
/// characters — `text` is only its first sentence).
fn pick_body(text: &str, blocks_text: &str) -> String {
    if blocks_text.trim().is_empty() {
        return text.to_string();
    }
    if text.trim().is_empty() {
        return blocks_text.to_string();
    }

    let text_words = word_set(text);
    let block_words = word_set(blocks_text);
    let (small, large) = if text_words.len() <= block_words.len() {
        (&text_words, &block_words)
    } else {
        (&block_words, &text_words)
    };
    // An EMPTY word set on either side means the mirror test has nothing to
    // measure — it must not therefore answer "mirror", which is what a
    // `coverage = 1.0` default did: for a message with no word-shaped tokens
    // at all (an emoji-only post, say) that silently discarded one of two
    // genuinely different renderings. Fall back to an exact, whitespace-blind
    // comparison — the strictest available answer, so this arm can only ever
    // say "mirror" about two renderings that really are the same characters.
    // (An alphanumeric skeleton is NOT strict enough here: `🎉` and `🚨` both
    // reduce to the empty string.)
    let is_mirror = if small.is_empty() {
        normalized_ws(text) == normalized_ws(blocks_text)
    } else {
        let overlap = small.iter().filter(|w| large.contains(*w)).count();
        overlap as f64 / small.len() as f64 >= 0.9
    };

    if is_mirror {
        // Mirrors — keep whichever rendering carries more content, measured
        // on alphanumerics so the mrkdwn markers `text` adds do not by
        // themselves make it look richer than the blocks.
        if alnum_len(text) >= alnum_len(blocks_text) {
            text.to_string()
        } else {
            blocks_text.to_string()
        }
    } else {
        format!("{}\n\n{}", text.trim_end(), blocks_text)
    }
}

/// Lower-cased alphanumeric words of length ≥ 2, as an unordered set — the
/// comparison currency for [`pick_body`]'s mirror test.
///
/// Splits on **Unicode** non-alphanumerics, not ASCII ones. The ASCII form
/// produced an EMPTY set for any message written in a non-Latin script
/// (Japanese, Chinese, Cyrillic, Arabic, Hebrew), which forced every such
/// message down the "mirror" branch and made [`pick_body`]'s keep-both arm
/// unreachable for it. A scriptio-continua language (CJK) still yields
/// long run-tokens rather than words, which is fine for this test: identical
/// runs compare equal, different runs do not overlap, and the empty-set
/// fallback in `pick_body` covers what is left.
fn word_set(s: &str) -> std::collections::HashSet<String> {
    s.split(|c: char| !c.is_alphanumeric())
        .filter(|w| w.chars().count() >= 2)
        .map(str::to_lowercase)
        .collect()
}

fn alnum_len(s: &str) -> usize {
    s.chars().filter(|c| c.is_alphanumeric()).count()
}

/// `s` with every run of whitespace collapsed to one space and the ends
/// trimmed — the exact-but-whitespace-blind comparison [`pick_body`] falls
/// back to when the word sets cannot decide.
fn normalized_ws(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Flatten one `attachments[]` entry, or `None` for a link-preview unfurl on a
/// human message and for an entry with nothing readable in it.
///
/// `author_is_app` narrows rule 2 (module doc) to the only case its
/// justification covers. The rule exists because an unfurl on a HUMAN message
/// is Slack's rendering of a page that human merely linked — flattening it
/// attributes a stranger's article to them. An APP's attachment is the app's
/// own content even when it carries `from_url`: Jira, Trello, Drive and every
/// app-unfurl integration set that field on the card they themselves post.
/// Applying the rule to them dropped those messages **entirely and silently**
/// in exactly the empty-`text` case this module exists to fix.
fn flatten_attachment(att: &Value, author_is_app: bool) -> Option<String> {
    if !author_is_app
        && (att.get("is_msg_unfurl").and_then(Value::as_bool) == Some(true)
            // `.is_some()` would be true for an explicit JSON `null`, which is
            // what any `Option<String>` serde round-trip emits — that would
            // drop a real attachment. Every other string read in this module
            // goes through `non_empty`; so does this one.
            || non_empty(att, "from_url").is_some())
    {
        return None;
    }

    let mut lines: Vec<String> = Vec::new();

    if let Some(v) = non_empty(att, "pretext") {
        lines.push(v.to_string());
    }
    if let Some(title) = non_empty(att, "title") {
        match non_empty(att, "title_link") {
            Some(link) => lines.push(format!("{title} ({link})")),
            None => lines.push(title.to_string()),
        }
    }
    if let Some(v) = non_empty(att, "text") {
        lines.push(v.to_string());
    }
    if let Some(fields) = att.get("fields").and_then(Value::as_array) {
        for f in fields {
            let title = f.get("title").and_then(Value::as_str).unwrap_or("").trim();
            let value = f.get("value").and_then(Value::as_str).unwrap_or("").trim();
            match (title.is_empty(), value.is_empty()) {
                (true, true) => {}
                (true, false) => lines.push(value.to_string()),
                (false, true) => lines.push(title.to_string()),
                (false, false) => lines.push(format!("{title}: {value}")),
            }
        }
    }

    if lines.is_empty() {
        // Rule 1 (module doc): last resort only.
        return non_empty(att, "fallback").map(str::to_string);
    }
    Some(lines.join("\n"))
}

fn non_empty<'a>(v: &'a Value, key: &str) -> Option<&'a str> {
    v.get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
}

/// Flatten `blocks[]`. `divider`/`actions` carry no content and are skipped;
/// an `image` contributes its `title` and `alt_text` (Slack requires the
/// latter), so an image-only message is not silently dropped; everything else
/// contributes whatever readable text it holds.
fn flatten_blocks(blocks: &[Value]) -> String {
    let mut out: Vec<String> = Vec::new();
    for b in blocks {
        let kind = b.get("type").and_then(Value::as_str).unwrap_or("");
        // An `image` block is not prose, but it is not empty either: Slack
        // REQUIRES `alt_text` on one and allows an optional `title`. Skipping
        // it outright meant an image-only message flattened to nothing and was
        // dropped silently — the same class of silent loss this module exists
        // to remove (review finding: image-only messages). `divider` and
        // `actions` really do carry no content and stay skipped.
        if kind == "image" {
            let mut parts: Vec<String> = Vec::new();
            if let Some(t) = b.pointer("/title/text").and_then(Value::as_str) {
                if !t.trim().is_empty() {
                    parts.push(t.to_string());
                }
            }
            if let Some(alt) = non_empty(b, "alt_text") {
                parts.push(format!("[image: {alt}]"));
            }
            if parts.is_empty() {
                // No alt_text at all (malformed — Slack rejects it on post)
                // still gets a marker, so "there was an image here" survives
                // rather than the message vanishing.
                parts.push("[image]".to_string());
            }
            out.push(parts.join("\n"));
            continue;
        }
        if matches!(kind, "divider" | "actions") {
            continue;
        }
        if kind == "rich_text" {
            let flat = flatten_rich_text_children(b);
            if !flat.trim().is_empty() {
                out.push(flat);
            }
            continue;
        }

        let mut parts: Vec<String> = Vec::new();
        // `header` / `section`: the content is a single text object.
        if let Some(t) = b.pointer("/text/text").and_then(Value::as_str) {
            if !t.trim().is_empty() {
                parts.push(t.to_string());
            }
        }
        // `section.fields[]`: a two-column layout, one text object per cell.
        if let Some(fields) = b.get("fields").and_then(Value::as_array) {
            for f in fields {
                if let Some(t) = f.get("text").and_then(Value::as_str) {
                    if !t.trim().is_empty() {
                        parts.push(t.to_string());
                    }
                }
            }
        }
        // `context`: several small elements, each its own text object. (The
        // Claude app's install notice puts its "connect your account" line
        // here and nowhere else.)
        if let Some(elements) = b.get("elements").and_then(Value::as_array) {
            for e in elements {
                if let Some(t) = e.get("text").and_then(Value::as_str) {
                    if !t.trim().is_empty() {
                        parts.push(t.to_string());
                    }
                }
            }
        }
        if !parts.is_empty() {
            out.push(parts.join("\n"));
        }
    }
    out.join("\n\n")
}

/// Walk a `rich_text` block's children. Containers (`rich_text_section`,
/// `rich_text_list`, `rich_text_quote`, `rich_text_preformatted`) nest one
/// more level of `elements`; leaves carry the actual characters.
fn flatten_rich_text_children(node: &Value) -> String {
    let Some(children) = node.get("elements").and_then(Value::as_array) else {
        return String::new();
    };
    let parts: Vec<String> = children.iter().map(flatten_rich_text_node).collect();
    // Every top-level child of a `rich_text` block is a container (one
    // paragraph, list or quote) — they stack vertically.
    parts
        .into_iter()
        .filter(|p| !p.is_empty())
        .collect::<Vec<_>>()
        .join("\n")
}

fn flatten_rich_text_node(node: &Value) -> String {
    let kind = node.get("type").and_then(Value::as_str).unwrap_or("");
    if node.get("elements").and_then(Value::as_array).is_some() {
        return match kind {
            // A list's children are themselves containers — one line each.
            "rich_text_list" => flatten_rich_text_children(node),
            // A section / quote / preformatted is one run of INLINE leaves:
            // `"…connected to it, "` + `<@U…>` is one sentence, not two
            // lines, so these join with no separator.
            _ => join_inline(node),
        };
    }
    match kind {
        "text" => node
            .get("text")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string(),
        // The label is what a reader sees in Slack; the bare URL is the only
        // thing left when there is no label.
        "link" => node
            .get("text")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .or_else(|| node.get("url").and_then(Value::as_str))
            .unwrap_or("")
            .to_string(),
        // Rendered exactly as a human message stores a mention today — this
        // module adds no mrkdwn normalisation of its own (module doc).
        "user" => match node.get("user_id").and_then(Value::as_str) {
            Some(id) => format!("<@{id}>"),
            None => String::new(),
        },
        "channel" => match node.get("channel_id").and_then(Value::as_str) {
            Some(id) => format!("<#{id}>"),
            None => String::new(),
        },
        "emoji" => match node.get("name").and_then(Value::as_str) {
            Some(name) => format!(":{name}:"),
            None => String::new(),
        },
        // `@here` / `@channel` / `@everyone`. Dropping these lost the part of
        // an alert that says who it is for — often its whole point.
        "broadcast" => match node.get("range").and_then(Value::as_str) {
            Some(range) => format!("<!{range}>"),
            None => String::new(),
        },
        // `@some-group`. Slack's own wire form for a usergroup mention is
        // `<!subteam^ID>`; the handle is not in the block, so the id is all
        // there is — same "exactly as Slack sent it" posture as `<@U…>`.
        "usergroup" => match node.get("usergroup_id").and_then(Value::as_str) {
            Some(id) => format!("<!subteam^{id}>"),
            None => String::new(),
        },
        // A rendered date. `fallback` is the human-readable rendering Slack
        // itself supplies for clients that cannot format it; prefer that over
        // reconstructing `<!date^…>` from the parts.
        "date" => node
            .get("fallback")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .or_else(|| {
                node.get("timestamp")
                    .and_then(Value::as_i64)
                    .map(|ts| format!("<!date^{ts}^{{date_short_pretty}} {{time}}|{ts}>"))
            })
            .unwrap_or_default(),
        // A colour swatch renders as its hex value; that is the whole content.
        "color" => node
            .get("value")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string(),
        // Anything Slack adds later contributes nothing rather than raw JSON.
        // If a future kind carries prose, it belongs above, with a test.
        _ => String::new(),
    }
}

/// Whether this message was posted by an APP rather than a human — decided
/// from the message object alone, so the flattener needs no out-of-band flag.
pub(crate) fn is_bot_authored_event(event: &Value) -> bool {
    bot_id_of(event).is_some()
        || event.get("subtype").and_then(Value::as_str) == Some("bot_message")
}

/// Join a container's leaves with no separator — they are inline runs of one
/// paragraph, so `"Our Organization now has slack connected to it, "` +
/// `<@U…>` must come back out as one sentence.
fn join_inline(node: &Value) -> String {
    let Some(children) = node.get("elements").and_then(Value::as_array) else {
        return String::new();
    };
    children
        .iter()
        .map(flatten_rich_text_node)
        .collect::<Vec<_>>()
        .concat()
}

/// Collapse any run of more than one BLANK line down to one, so a stack of
/// sparsely-populated attachments does not become a column of blank space in
/// the coral body.
///
/// Blank means "nothing but whitespace", not "a bare `\n`". The character-
/// counting version this replaced saw `"a\n \n \n \nb"` — which is exactly
/// what attachments with empty-but-present fields produce — as no run at all,
/// because a space reset its counter. `\r\n` is normalised to `\n` first for
/// the same reason: a run of them was likewise invisible. Slack's own `text`
/// uses bare `\n`, so that normalisation is inert on every payload measured.
fn collapse_blank_runs(s: &str) -> String {
    let normalised = s.replace("\r\n", "\n");
    let mut out: Vec<&str> = Vec::new();
    let mut blank_run = 0usize;
    for line in normalised.split('\n') {
        if line.trim().is_empty() {
            blank_run += 1;
            if blank_run <= 1 {
                out.push("");
            }
        } else {
            blank_run = 0;
            out.push(line);
        }
    }
    out.join("\n")
}

/// Truncate to [`MAX_FLATTENED_BYTES`] on a char boundary, appending
/// [`TRUNCATION_MARKER`].
///
/// The marker's own length is RESERVED before slicing, so the result is
/// `<= MAX_FLATTENED_BYTES` — the bound this constant's doc comment and
/// `docs/connectors/slack.md` both assert. The earlier form appended the
/// marker after a full-length slice and so returned
/// `MAX_FLATTENED_BYTES + marker.len()`, overshooting the documented cap.
fn cap(s: &str) -> String {
    if s.len() <= MAX_FLATTENED_BYTES {
        return s.to_string();
    }
    let mut end = MAX_FLATTENED_BYTES - TRUNCATION_MARKER.len();
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}{}", &s[..end], TRUNCATION_MARKER)
}

// ── Admission: the ONE rule all three ingest funnels apply ───────────────────

/// A Slack message that passed admission, reduced to exactly what the ingest
/// pipeline needs — the raw event does not survive into the job payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AdmittedMessage {
    /// The flattened body to ingest. Never empty.
    pub(crate) text: String,
    /// The app's `bot_id` (`B…`), or `None` for a human message.
    pub(crate) bot_id: Option<String>,
    /// The app's display name, or `None` for a human message. `Some` while
    /// `bot_id` is `None` is the legacy `subtype: "bot_message"` shape.
    pub(crate) bot_name: Option<String>,
}

impl AdmittedMessage {
    /// Whether an APP posted this. Both funnels' self-bot checks and
    /// `ingest_slack_message`'s author branch key on this ONE predicate —
    /// the review found the three call sites had already drifted into three
    /// different spellings of it.
    pub(crate) fn is_bot_message(&self) -> bool {
        self.bot_id.is_some() || self.bot_name.is_some()
    }
}

/// Whether bot-authored Slack messages are ingested at all.
///
/// **Default ON** — this is the shipped behaviour, and the flag exists so a
/// workspace drowning in CI notifications can be returned to the pre-change
/// behaviour with an env var instead of a revert. Note the polarity is the
/// OPPOSITE of `MEMBER_OCEANS_ENABLED`'s default-off convention (which gates
/// an unfinished capability); the parsing is otherwise the same one-line
/// contract, inverted: `"0"` or a case-insensitive `"false"` turns it off,
/// unset or anything else leaves it on.
///
/// Read per event, never cached, so flipping it takes effect on the next
/// message with no redeploy.
pub(crate) fn bot_message_ingest_enabled() -> bool {
    !matches!(
        std::env::var("SLACK_INGEST_BOT_MESSAGES").ok().as_deref(),
        Some(v) if v == "0" || v.eq_ignore_ascii_case("false")
    )
}

/// The message subtypes this connector ingests. Everything else — edits
/// (`message_changed`), deletes, `channel_join`/`channel_leave`,
/// `file_share`, … — stays dropped.
///
/// * `None` — an ordinary message, human or (modern) app.
/// * `"bot_message"` — the LEGACY app-post shape: no `user`, no
///   `bot_profile`, identity is `username` only.
/// * `"thread_broadcast"` — a threaded reply the author also broadcast to the
///   channel. Ingested as an ordinary threaded reply (its `thread_ts` is
///   already set); its `root` — a full copy of the parent message — is never
///   read, see rule 3 in this module's header.
pub(crate) fn ingestible_subtype(subtype: Option<&str>) -> bool {
    matches!(subtype, None | Some("bot_message") | Some("thread_broadcast"))
}

/// Decide whether one Slack message object is ingestible, and reduce it to
/// [`AdmittedMessage`]. `None` ⇒ drop it and ack.
///
/// **This is the single copy of the admission rule.** It used to be
/// copy-pasted into three funnels (`routes::slack_events`' live webhook,
/// `routes::slack_interactivity::backfill_channel`, and the since-deleted
/// legacy `/v1/webhooks/slack`) and the review found them already
/// drifted: `bot_name` was derived three different ways, and the legacy route
/// guarded its self-bot exclusion on `bot_id.is_some()` while the live one
/// used `bot_id.is_some() || bot_name.is_some()`, so a `subtype:
/// "bot_message"` post with no `bot_id` skipped the self-bot check entirely
/// on that path. One function, two callers, tests here.
///
/// # The kill switch restores the OLD behaviour for EVERY message
///
/// `SLACK_INGEST_BOT_MESSAGES=0` is documented — in `.env.example` and in
/// `docs/connectors/slack.md` — as "byte-for-byte as before". Gating only the
/// bot branch would not have delivered that, because [`flatten_slack_message`]
/// replaced the plain `text` read for HUMAN messages too: with the switch off,
/// a human message with empty `text` but populated `blocks` would still have
/// been ingested (it was dropped before), and one whose `blocks` are richer
/// than its `text` would still have been stored with different bytes. So the
/// off arm below reproduces the pre-2026-09 predicate exactly — drop every
/// bot message AND every sub-typed message, and read `text` verbatim — rather
/// than the docs being quietly weakened to match a narrower switch.
pub(crate) fn admit_slack_message(event: &Value) -> Option<AdmittedMessage> {
    let subtype = event.get("subtype").and_then(Value::as_str);
    let bot_id = bot_id_of(event);
    let is_bot_message = bot_id.is_some() || subtype == Some("bot_message");

    if !bot_message_ingest_enabled() {
        // Byte-for-byte the pre-2026-09 admission, human messages included.
        if is_bot_message || subtype.is_some() {
            return None;
        }
        let text = event
            .get("text")
            .and_then(Value::as_str)
            .filter(|t| !t.is_empty())?;
        return Some(AdmittedMessage {
            text: text.to_owned(),
            bot_id: None,
            bot_name: None,
        });
    }

    if !ingestible_subtype(subtype) {
        return None;
    }
    // Flatten BEFORE any emptiness check: 235 of the 253 dropped bot messages
    // have an EMPTY `text`, so a guard reading `text` alone would still drop
    // every one of them even with the bot filter lifted.
    let text = flatten_slack_message(event)?;
    let bot_name = is_bot_message.then(|| bot_author_name(event));
    Some(AdmittedMessage {
        text,
        bot_id,
        bot_name,
    })
}

/// The atom `author_id` for an admitted message: the app's `bot_id` (`B…`)
/// for a bot post, the Slack user id (`U…`) for a human one.
///
/// The `B…`/`U…` split is what makes a bot author structurally unable to
/// collide with a human one — or with `owner_slack_id`, which is what keeps
/// the #801 owner-authored self-reef branch unreachable for an app. A legacy
/// `bot_message` with no `bot_id` has neither: its `user` field is absent, so
/// the caller's default is the literal string `"unknown"`, and every such app
/// in every workspace would have collapsed onto that one id. This synthesises
/// a third, explicitly namespaced space for exactly that case, keyed on the
/// only identity such a payload carries.
pub(crate) fn author_id_for(admitted: &AdmittedMessage, slack_user_id: &str) -> String {
    if let Some(bot_id) = admitted.bot_id.as_deref() {
        return bot_id.to_string();
    }
    match admitted.bot_name.as_deref() {
        Some(name) => format!("slack_app:{}", slugify_app_name(name)),
        None => slack_user_id.to_string(),
    }
}

/// Lower-cased, `-`-joined alphanumeric runs of an app's display name, so
/// "Ledger Bot" and "ledger  bot" resolve to one author id. Never
/// empty: a name with no alphanumerics at all degrades to `unnamed`.
fn slugify_app_name(name: &str) -> String {
    let slug: String = name
        .split(|c: char| !c.is_alphanumeric())
        .filter(|part| !part.is_empty())
        .map(str::to_lowercase)
        .collect::<Vec<_>>()
        .join("-");
    if slug.is_empty() {
        "unnamed".to_string()
    } else {
        slug
    }
}

/// The display name to attribute a bot-authored message to: the app's own
/// profile name, else the legacy `bot_message` `username`, else a constant.
///
/// `bot_message_subtype.json` is why the middle arm exists — a legacy
/// `subtype: "bot_message"` payload carries NO `user` and NO `bot_profile`,
/// so `username` ("Ledger Bot" in the fixture) is the only identity in the object.
pub(crate) fn bot_author_name(event: &Value) -> String {
    event
        .pointer("/bot_profile/name")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .or_else(|| {
            event
                .get("username")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
        })
        .unwrap_or("bot")
        .to_string()
}

/// The `bot_id` (`B…`) of a bot-authored message, if it is one.
pub(crate) fn bot_id_of(event: &Value) -> Option<String> {
    event
        .get("bot_id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn fixture(name: &str) -> Value {
        let path = format!(
            "{}/tests/fixtures/slack_bot_messages/{name}.json",
            env!("CARGO_MANIFEST_DIR")
        );
        let raw = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path}: {e}"));
        serde_json::from_str(&raw).unwrap_or_else(|e| panic!("parse {path}: {e}"))
    }

    // ── The measurement this whole module exists for ─────────────────────────
    //
    // `fallback` is 54 characters and contains neither the PR number nor the
    // title; `pretext` + `title` + `text` is 8232. A regression that reached
    // for `fallback` first would still produce a non-empty string and still
    // ingest an atom — the ONLY thing that catches it is asserting the size
    // and the specific content.
    #[test]
    fn github_pr_opened_flattens_attachments_not_the_one_line_fallback() {
        let out = flatten_slack_message(&fixture("github_pr_opened_attachments"))
            .expect("a message whose entire content is in attachments must still be ingestible");
        assert!(
            out.contains("#101"),
            "the PR number lives in attachments[0].title and is absent from `fallback` — got: {out}"
        );
        assert!(
            out.contains("the workspace guard read a stale key"),
            "the PR title must survive flattening — got: {out}"
        );
        assert!(
            out.contains("Pull request opened by"),
            "attachments[0].pretext must survive flattening — got: {out}"
        );
        assert!(
            out.contains("Comments: 1"),
            "attachments[].fields[] must be rendered as `title: value` — got: {out}"
        );
        assert!(
            out.len() > 1024,
            "flattening the real fields yields >1 KiB; {} bytes means the code fell back to \
             `fallback` (58 bytes, 0.7% of the content)",
            out.len()
        );
    }

    #[test]
    fn github_commit_push_keeps_pretext_and_text() {
        let out = flatten_slack_message(&fixture("github_commit_attachments")).expect("ingestible");
        assert!(out.contains("1 new commit"), "pretext missing — got: {out}");
        assert!(
            out.contains("the workspace guard read a stale key"),
            "attachment text missing — got: {out}"
        );
    }

    // ── `root` is a whole second copy of the parent message ──────────────────
    #[test]
    fn thread_broadcast_never_flattens_its_root() {
        let event = fixture("github_thread_broadcast");
        // The parent is the SAME pull request's "opened" notification, so it
        // shares the broadcast's `title` — the two are told apart by the
        // `pretext` verb and by the 8 KiB PR body that only the parent has.
        let root_body = event
            .pointer("/root/attachments/0/text")
            .and_then(Value::as_str)
            .expect("fixture's root carries the full PR description");
        assert!(root_body.len() > 8000, "fixture premise: root's body is large");

        let out = flatten_slack_message(&event).expect("ingestible");
        assert!(
            out.contains("Pull request merged by"),
            "the broadcast's OWN attachment must be flattened — got: {out}"
        );
        assert!(
            !out.contains("Pull request opened by"),
            "`root` is the FULL parent message (here, the same PR's `opened` notification); \
             flattening it ingests every merged PR twice — got: {out}"
        );
        assert!(
            !out.contains(&root_body[..200]),
            "the parent's 8 KiB body must not be dragged in — got {} bytes",
            out.len()
        );
        assert!(
            out.len() < 500,
            "a broadcast is pretext + title and nothing else; {} bytes means `root` leaked in",
            out.len()
        );
    }

    // ── `text` and `blocks` are the same words twice ─────────────────────────
    #[test]
    fn an_apps_rich_text_blocks_do_not_duplicate_their_own_text() {
        let event = fixture("app_rich_text_blocks");
        let text = event["text"].as_str().unwrap();
        let out = flatten_slack_message(&event).expect("ingestible");
        assert_eq!(
            out, text,
            "the rich_text block is a mirror of `text` and `text` is the richer rendering \
             (it keeps the mrkdwn markers), so the output must be `text` alone"
        );
        assert!(
            out.matches("Scheduled jobs are blocked on billing").count() == 1,
            "the same paragraph must not appear twice"
        );
    }

    #[test]
    fn human_plain_round_trips_byte_for_byte() {
        let event = fixture("human_plain");
        let out = flatten_slack_message(&event).expect("ingestible");
        assert_eq!(
            out,
            event["text"].as_str().unwrap(),
            "a plain human message must ingest EXACTLY the text it ingests today — this \
             change must not alter one byte of the human path"
        );
    }

    // ── the opposite case: `text` is only the first sentence ─────────────────
    #[test]
    fn claude_app_message_prefers_the_richer_blocks() {
        let event = fixture("claude_app_message");
        let out = flatten_slack_message(&event).expect("ingestible");
        assert!(
            out.contains("How I collect and process context"),
            "blocks[1] carries ~80% of this message and `text` carries none of it — got: {out}"
        );
        assert!(
            out.contains("Open Claude's Home tab"),
            "the `context` block's element text must survive — got: {out}"
        );
        let first_sentence = "Hello! I'm Claude, an AI assistant created by Anthropic.";
        assert_eq!(
            out.matches(first_sentence).count(),
            1,
            "`text` is a truncated mirror of blocks[0]; keeping both duplicates the opening \
             sentence — got: {out}"
        );
    }

    #[test]
    fn legacy_bot_message_subtype_has_only_username_as_identity() {
        let event = fixture("bot_message_subtype");
        assert!(
            event.get("user").is_none() && event.get("bot_profile").is_none(),
            "fixture premise: a legacy `bot_message` carries neither `user` nor `bot_profile`"
        );
        assert_eq!(
            bot_author_name(&event),
            "Ledger Bot",
            "with no bot_profile, `username` is the ONLY identity in the payload"
        );
        assert_eq!(bot_id_of(&event).as_deref(), Some("B0TESTSEN0"));
        let out = flatten_slack_message(&event).expect("ingestible");
        assert!(out.contains("Hosting"), "got: {out}");
    }

    #[test]
    fn bot_author_name_prefers_the_bot_profile() {
        assert_eq!(bot_author_name(&fixture("github_commit_attachments")), "GitHub");
        assert_eq!(bot_author_name(&fixture("claude_app_message")), "Claude");
        assert_eq!(
            bot_author_name(&json!({"bot_id": "B1"})),
            "bot",
            "neither bot_profile nor username: a constant, never an empty author"
        );
    }

    // ── unfurls are someone else's web page, not this author's words ─────────
    #[test]
    fn link_preview_unfurls_are_skipped_on_a_human_message() {
        let event = json!({
            "text": "look at this",
            "attachments": [
                {"is_msg_unfurl": true, "title": "Some Blog Post", "text": "a whole article"},
                {"from_url": "https://example.com", "title": "Another Preview"},
            ]
        });
        let out = flatten_slack_message(&event).expect("ingestible");
        assert_eq!(
            out, "look at this",
            "an unfurl is Slack rendering a link the author merely pasted — flattening it \
             would attribute a stranger's page to the author"
        );
    }

    /// Review finding 7. Jira/Trello/Drive and every app-unfurl integration
    /// set `from_url` on the card THEY post. With the empty `text` that 92% of
    /// bot messages have, the human-only rule dropped the whole message —
    /// silently, and precisely in the case this module exists to fix.
    #[test]
    fn an_apps_own_attachment_is_kept_even_when_it_carries_from_url() {
        let event = json!({
            "bot_id": "B0JIRA",
            "text": "",
            "attachments": [{
                "from_url": "https://acme.atlassian.net/browse/ENG-42",
                "title": "ENG-42 Login times out on slow networks",
                "text": "Status changed from In Progress to Done by Rahel",
            }]
        });
        let out = flatten_slack_message(&event)
            .expect("an app's own card must not be mistaken for a link preview");
        assert!(out.contains("ENG-42"), "got: {out}");
        assert!(out.contains("In Progress to Done"), "got: {out}");
    }

    /// Review finding 9. `{"from_url": null}` is what any `Option<String>`
    /// serde round-trip emits; `.is_some()` on the raw `Value` is true for it.
    #[test]
    fn an_explicit_null_from_url_is_not_an_unfurl() {
        let event = json!({
            "text": "",
            "attachments": [{"from_url": null, "title": "A real attachment"}]
        });
        assert_eq!(
            flatten_slack_message(&event).unwrap(),
            "A real attachment",
            "a JSON null `from_url` must not be read as a link preview"
        );
    }

    #[test]
    fn fallback_is_used_only_when_every_real_field_is_empty() {
        let event = json!({
            "text": "",
            "attachments": [{"fallback": "the only thing here", "footer": "ignored"}]
        });
        assert_eq!(
            flatten_slack_message(&event).unwrap(),
            "the only thing here"
        );
    }

    #[test]
    fn title_link_is_appended_when_present() {
        let event = json!({
            "attachments": [{"title": "Build #12 failed", "title_link": "https://ci.example/12"}]
        });
        assert_eq!(
            flatten_slack_message(&event).unwrap(),
            "Build #12 failed (https://ci.example/12)"
        );
    }

    /// Review finding: an image-only message flattened to nothing and was
    /// dropped SILENTLY — the same class of loss this module exists to
    /// remove. Slack requires `alt_text` on an image block and allows an
    /// optional `title`, so there is always something to keep.
    #[test]
    fn an_image_only_message_is_not_silently_dropped() {
        let event = json!({
            "bot_id": "B0TESTGIT0",
            "text": "",
            "blocks": [{
                "type": "image",
                "block_id": "img1",
                "title": {"type": "plain_text", "text": "Error rate, last 24h"},
                "image_url": "https://example.com/chart.png",
                "alt_text": "a line chart climbing sharply after 14:00",
            }]
        });
        let out = flatten_slack_message(&event)
            .expect("a message whose only content is an image must still be ingestible");
        assert!(out.contains("Error rate, last 24h"), "the title must survive — got: {out}");
        assert!(
            out.contains("[image: a line chart climbing sharply after 14:00]"),
            "alt_text is REQUIRED by Slack and is the only description there is — got: {out}"
        );
    }

    #[test]
    fn an_image_block_with_no_alt_text_still_leaves_a_marker() {
        let event = json!({
            "text": "",
            "blocks": [{"type": "image", "image_url": "https://example.com/x.png"}]
        });
        assert_eq!(
            flatten_slack_message(&event).unwrap(),
            "[image]",
            "even a malformed image block must leave evidence the message existed"
        );
    }

    #[test]
    fn an_image_alongside_prose_does_not_crowd_it_out() {
        let event = json!({
            "text": "",
            "blocks": [
                {"type": "section", "text": {"type": "mrkdwn", "text": "Deploy finished."}},
                {"type": "image", "image_url": "https://example.com/x.png", "alt_text": "graph"},
                {"type": "divider"},
                {"type": "actions", "elements": [{"type": "button"}]},
            ]
        });
        assert_eq!(
            flatten_slack_message(&event).unwrap(),
            "Deploy finished.\n\n[image: graph]",
            "divider/actions still contribute nothing; the image contributes its alt text"
        );
    }

    #[test]
    fn nothing_readable_returns_none() {
        assert!(flatten_slack_message(&json!({"text": ""})).is_none());
        assert!(flatten_slack_message(&json!({"text": "   \n\n  "})).is_none());
        assert!(flatten_slack_message(&json!({"bot_id": "B1", "attachments": []})).is_none());
        assert!(
            flatten_slack_message(&json!({"blocks": [{"type": "divider"}]})).is_none(),
            "a divider carries nothing at all — unlike an image, which carries alt_text"
        );
    }

    #[test]
    fn blank_runs_collapse_to_at_most_one_empty_line() {
        let event = json!({"text": "a\n\n\n\n\nb"});
        assert_eq!(flatten_slack_message(&event).unwrap(), "a\n\nb");
    }

    /// Review finding 10. A blank line produced by a sparsely-populated
    /// attachment is `" "`, not `""` — the character-counting collapse saw a
    /// space and reset its run counter, so these passed through untouched.
    #[test]
    fn whitespace_only_lines_and_crlf_also_collapse() {
        assert_eq!(
            flatten_slack_message(&json!({"text": "a\n \n \n \nb"})).unwrap(),
            "a\n\nb",
            "lines that are blank-but-for-whitespace must collapse like empty ones"
        );
        assert_eq!(
            flatten_slack_message(&json!({"text": "a\r\n\r\n\r\n\r\nb"})).unwrap(),
            "a\n\nb",
            "a CRLF run must collapse too"
        );
        assert_eq!(
            flatten_slack_message(&json!({"text": "a\n\tb"})).unwrap(),
            "a\n\tb",
            "a single newline and an indented line are content, not a blank run"
        );
    }

    /// Review finding 12. The cap is a BOUND, and the module constant plus
    /// `docs/connectors/slack.md` both state it as one; appending the marker
    /// after a full-length slice overshot it by the marker's length.
    #[test]
    fn oversized_content_is_capped_at_the_bound_not_past_it() {
        let huge = "x".repeat(MAX_FLATTENED_BYTES + 5000);
        let out = flatten_slack_message(&json!({"text": huge})).unwrap();
        assert!(
            out.len() <= MAX_FLATTENED_BYTES,
            "{} bytes exceeds the documented {MAX_FLATTENED_BYTES}-byte cap",
            out.len()
        );
        assert_eq!(out.len(), MAX_FLATTENED_BYTES);
        assert!(out.ends_with(TRUNCATION_MARKER));
    }

    /// A multi-byte char straddling the reserved boundary must not panic and
    /// must still land under the bound.
    #[test]
    fn the_cap_never_splits_a_multibyte_char() {
        let huge = "é".repeat(MAX_FLATTENED_BYTES);
        let out = flatten_slack_message(&json!({"text": huge})).unwrap();
        assert!(out.len() <= MAX_FLATTENED_BYTES);
        assert!(out.ends_with(TRUNCATION_MARKER));
    }

    #[test]
    fn rich_text_leaf_kinds_render_the_way_a_human_message_stores_them() {
        let event = json!({
            "blocks": [{
                "type": "rich_text",
                "elements": [{
                    "type": "rich_text_section",
                    "elements": [
                        {"type": "emoji", "name": "wave"},
                        {"type": "text", "text": " hi "},
                        {"type": "user", "user_id": "U123"},
                        {"type": "text", "text": " in "},
                        {"type": "channel", "channel_id": "C456"},
                        {"type": "text", "text": " see "},
                        {"type": "link", "url": "https://e.co", "text": "here"},
                        {"type": "text", "text": " and "},
                        {"type": "link", "url": "https://bare.co"},
                    ]
                }]
            }]
        });
        assert_eq!(
            flatten_slack_message(&event).unwrap(),
            ":wave: hi <@U123> in <#C456> see here and https://bare.co"
        );
    }

    /// Review finding 11. The catch-all silently returned `""` for these, so
    /// `@here` / `@channel` / an on-call usergroup ping / a rendered deadline
    /// vanished from the ingested text — usually the load-bearing part of the
    /// alert that carried them.
    #[test]
    fn rich_text_kinds_beyond_the_first_five_are_not_silently_dropped() {
        let event = json!({
            "blocks": [{
                "type": "rich_text",
                "elements": [{
                    "type": "rich_text_section",
                    "elements": [
                        {"type": "broadcast", "range": "here"},
                        {"type": "text", "text": " and "},
                        {"type": "broadcast", "range": "channel"},
                        {"type": "text", "text": " — "},
                        {"type": "usergroup", "usergroup_id": "S01ONCALL"},
                        {"type": "text", "text": " deploy freeze until "},
                        {"type": "date", "timestamp": 1788996204, "fallback": "Sep 9 at 3:23 PM"},
                        {"type": "text", "text": " "},
                        {"type": "color", "value": "#ff0000"},
                    ]
                }]
            }]
        });
        assert_eq!(
            flatten_slack_message(&event).unwrap(),
            "<!here> and <!channel> — <!subteam^S01ONCALL> deploy freeze until \
             Sep 9 at 3:23 PM #ff0000"
        );
    }

    /// A `date` with no `fallback` still renders something dereferenceable
    /// rather than disappearing.
    #[test]
    fn a_date_leaf_without_a_fallback_still_renders() {
        let event = json!({
            "blocks": [{"type": "rich_text", "elements": [{
                "type": "rich_text_section",
                "elements": [{"type": "date", "timestamp": 1788996204}]
            }]}]
        });
        let out = flatten_slack_message(&event).unwrap();
        assert!(out.contains("1788996204"), "got: {out}");
    }

    /// Review finding 8. An ASCII-only word split produced EMPTY sets on both
    /// sides for any non-Latin script, which forced `coverage = 1.0` and made
    /// the keep-both branch unreachable — silently discarding one of two
    /// genuinely different renderings.
    #[test]
    fn a_non_latin_message_can_still_reach_the_keep_both_branch() {
        let event = json!({
            "text": "デプロイ完了",
            "blocks": [{"type": "section", "text": {"type": "mrkdwn",
                "text": "ステージング環境のロールバック手順はこちらを参照してください"}}]
        });
        let out = flatten_slack_message(&event).unwrap();
        assert!(out.contains("デプロイ完了"), "the `text` half must survive — got: {out}");
        assert!(
            out.contains("ロールバック手順"),
            "the blocks half is genuinely different content and must survive too — got: {out}"
        );
    }

    /// …and a non-Latin message whose blocks ARE a mirror of its text must
    /// still be de-duplicated, so the fix above did not simply disable the
    /// mirror test for those scripts.
    #[test]
    fn a_non_latin_mirror_is_still_de_duplicated() {
        let event = json!({
            "text": "Привет, деплой завершён",
            "blocks": [{"type": "rich_text", "elements": [{
                "type": "rich_text_section",
                "elements": [{"type": "text", "text": "Привет, деплой завершён"}]
            }]}]
        });
        assert_eq!(
            flatten_slack_message(&event).unwrap(),
            "Привет, деплой завершён",
            "Cyrillic words split on whitespace fine; this must still read as a mirror"
        );
    }

    /// The empty-word-set fallback: no word-shaped token anywhere, so the
    /// mirror test has nothing to measure and must not default to "mirror".
    #[test]
    fn an_empty_word_set_does_not_force_a_mirror() {
        let event = json!({
            "text": "🎉",
            "blocks": [{"type": "section", "text": {"type": "mrkdwn", "text": "🚨"}}]
        });
        let out = flatten_slack_message(&event).unwrap();
        assert!(out.contains('🎉') && out.contains('🚨'), "got: {out}");
    }

    #[test]
    fn rich_text_lists_and_quotes_each_get_their_own_line() {
        let event = json!({
            "blocks": [{
                "type": "rich_text",
                "elements": [
                    {"type": "rich_text_quote", "elements": [{"type": "text", "text": "quoted"}]},
                    {"type": "rich_text_list", "style": "bullet", "elements": [
                        {"type": "rich_text_section", "elements": [{"type": "text", "text": "one"}]},
                        {"type": "rich_text_section", "elements": [{"type": "text", "text": "two"}]},
                    ]},
                    {"type": "rich_text_preformatted", "elements": [{"type": "text", "text": "code"}]},
                ]
            }]
        });
        assert_eq!(
            flatten_slack_message(&event).unwrap(),
            "quoted\none\ntwo\ncode"
        );
    }

    #[test]
    fn text_and_blocks_that_are_genuinely_different_are_both_kept() {
        let event = json!({
            "text": "alpha bravo charlie delta",
            "blocks": [{"type": "section", "text": {"type": "mrkdwn",
                "text": "wholly unrelated echo foxtrot golf hotel"}}]
        });
        let out = flatten_slack_message(&event).unwrap();
        assert!(out.contains("alpha bravo"), "got: {out}");
        assert!(out.contains("echo foxtrot"), "got: {out}");
    }

    // ── Admission: the one rule, and what the kill switch really restores ───
    //
    // These mutate a process-wide env var, so they share one lock rather than
    // racing each other under the default parallel test runner — same
    // convention as `routes::slack_events`' `slack_signing_secret_lock`.
    #[cfg(test)]
    fn bot_ingest_env_lock() -> &'static std::sync::Mutex<()> {
        static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
        LOCK.get_or_init(|| std::sync::Mutex::new(()))
    }

    #[cfg(test)]
    fn with_switch<T>(value: Option<&str>, f: impl FnOnce() -> T) -> T {
        let _guard = bot_ingest_env_lock().lock().unwrap_or_else(|e| e.into_inner());
        let old = std::env::var("SLACK_INGEST_BOT_MESSAGES").ok();
        unsafe {
            match value {
                Some(v) => std::env::set_var("SLACK_INGEST_BOT_MESSAGES", v),
                None => std::env::remove_var("SLACK_INGEST_BOT_MESSAGES"),
            }
        }
        let out = f();
        unsafe {
            match old {
                Some(v) => std::env::set_var("SLACK_INGEST_BOT_MESSAGES", v),
                None => std::env::remove_var("SLACK_INGEST_BOT_MESSAGES"),
            }
        }
        out
    }

    #[test]
    fn admission_flattens_a_bot_message_and_names_its_app() {
        let admitted = with_switch(None, || {
            admit_slack_message(&fixture("github_pr_opened_attachments"))
        })
        .expect("a bot message with empty text and attachments is ingestible");
        assert!(admitted.is_bot_message());
        assert_eq!(admitted.bot_id.as_deref(), Some("B0TESTGIT0"));
        assert_eq!(admitted.bot_name.as_deref(), Some("GitHub"));
        assert!(admitted.text.contains("#101"));
    }

    #[test]
    fn admission_leaves_a_human_message_alone() {
        let human = fixture("human_plain");
        let admitted = with_switch(None, || admit_slack_message(&human)).expect("ingestible");
        assert!(!admitted.is_bot_message());
        assert!(admitted.bot_id.is_none() && admitted.bot_name.is_none());
        assert_eq!(admitted.text, human["text"].as_str().unwrap());
    }

    #[test]
    fn admission_drops_every_subtype_outside_the_allowlist() {
        for refused in ["message_changed", "message_deleted", "channel_join", "file_share"] {
            let event = json!({"subtype": refused, "text": "still text", "user": "U1"});
            assert!(
                with_switch(None, || admit_slack_message(&event)).is_none(),
                "{refused} must stay dropped"
            );
        }
        for admitted in ["bot_message", "thread_broadcast"] {
            let event = json!({"subtype": admitted, "text": "content", "user": "U1"});
            assert!(
                with_switch(None, || admit_slack_message(&event)).is_some(),
                "{admitted} must be admitted"
            );
        }
    }

    /// Review finding 5. A legacy `bot_message` with no `bot_id` is still an
    /// app post; the legacy webhook used to guard its self-bot exclusion on
    /// `bot_id.is_some()` alone and so skipped both the comparison and the
    /// fail-closed arm for it. One predicate now, on the admitted message.
    #[test]
    fn a_bot_message_subtype_without_a_bot_id_is_still_a_bot_message() {
        let event = json!({"subtype": "bot_message", "username": "Some App", "text": "hi"});
        let admitted = with_switch(None, || admit_slack_message(&event)).expect("ingestible");
        assert!(admitted.bot_id.is_none());
        assert_eq!(admitted.bot_name.as_deref(), Some("Some App"));
        assert!(
            admitted.is_bot_message(),
            "no bot_id, but `username` and the subtype both say app — the self-bot check \
             and the author branch must both see this as one"
        );
    }

    /// Review finding 3. `.env.example` and `docs/connectors/slack.md` both
    /// promise the switch restores prior behaviour "byte-for-byte". Gating
    /// only the bot branch would not have: `flatten_slack_message` replaced
    /// the plain `text` read for HUMAN messages too.
    #[test]
    fn the_kill_switch_restores_prior_behaviour_for_human_messages_too() {
        // A human message whose content is only in `blocks`: dropped before
        // this PR, and must be dropped again with the switch off.
        let blocks_only = json!({
            "user": "U1",
            "text": "",
            "blocks": [{"type": "section", "text": {"type": "mrkdwn", "text": "only in blocks"}}]
        });
        assert!(
            with_switch(Some("0"), || admit_slack_message(&blocks_only)).is_none(),
            "with the switch off a human blocks-only message must be dropped, as it was \
             before this change"
        );
        assert!(
            with_switch(None, || admit_slack_message(&blocks_only)).is_some(),
            "…and admitted with the switch on, or the test proves nothing"
        );

        // A human message whose `blocks` are richer than its `text`: stored
        // with DIFFERENT bytes by the flattener, so the switch must bypass it.
        let claude_shaped = json!({
            "user": "U1",
            "text": "First sentence only.",
            "blocks": [
                {"type": "section", "text": {"type": "mrkdwn", "text": "First sentence only."}},
                {"type": "section", "text": {"type": "mrkdwn", "text": "A whole second paragraph the text field never had."}},
            ]
        });
        assert_eq!(
            with_switch(Some("0"), || admit_slack_message(&claude_shaped))
                .expect("a human message with non-empty text still ingests")
                .text,
            "First sentence only.",
            "with the switch off a human message must store EXACTLY its `text`"
        );
        assert!(
            with_switch(None, || admit_slack_message(&claude_shaped))
                .unwrap()
                .text
                .contains("second paragraph"),
            "…and the flattener must be doing something when the switch is on"
        );

        // And the bot half of the promise.
        assert!(
            with_switch(Some("0"), || admit_slack_message(&fixture(
                "github_pr_opened_attachments"
            )))
            .is_none(),
            "switch off ⇒ bot messages dropped"
        );
        assert!(
            with_switch(Some("FALSE"), || admit_slack_message(&fixture("app_rich_text_blocks"))).is_none(),
            "the off value is case-insensitive"
        );
        // A `thread_broadcast` carries a subtype, so the pre-change predicate
        // dropped it — and "byte-for-byte" means it must be dropped again.
        assert!(
            with_switch(Some("0"), || admit_slack_message(&json!({
                "subtype": "thread_broadcast", "user": "U1", "text": "hi"
            })))
            .is_none(),
            "switch off ⇒ every sub-typed message is dropped, exactly as before"
        );
    }

    /// Review finding 6. `"unknown"` is `ingest_slack_message`'s default for
    /// an absent `user`, so every legacy-`bot_message` app in every workspace
    /// used to collapse onto that single author id.
    #[test]
    fn author_id_never_falls_back_to_the_unknown_sentinel_for_an_app() {
        let legacy = AdmittedMessage {
            text: "x".into(),
            bot_id: None,
            bot_name: Some("Ledger Bot".into()),
        };
        assert_eq!(author_id_for(&legacy, "unknown"), "slack_app:ledger-bot");
        assert_eq!(
            author_id_for(&legacy, "unknown"),
            author_id_for(
                &AdmittedMessage { bot_name: Some("ledger  BOT".into()), ..legacy.clone() },
                "unknown"
            ),
            "the same app must resolve to the same id regardless of spacing/case"
        );

        let modern = AdmittedMessage {
            text: "x".into(),
            bot_id: Some("B0TESTGIT0".into()),
            bot_name: Some("GitHub".into()),
        };
        assert_eq!(author_id_for(&modern, "U0TESTGIT0"), "B0TESTGIT0");

        let human = AdmittedMessage { text: "x".into(), bot_id: None, bot_name: None };
        assert_eq!(author_id_for(&human, "U0TESTUSR0"), "U0TESTUSR0");

        let nameless = AdmittedMessage {
            text: "x".into(),
            bot_id: None,
            bot_name: Some("···".into()),
        };
        assert_eq!(
            author_id_for(&nameless, "unknown"),
            "slack_app:unnamed",
            "a name with no alphanumerics must still not degrade to the bare sentinel"
        );
    }

    #[test]
    fn own_bot_confirmation_is_still_flattenable_the_drop_is_a_separate_gate() {
        // This module has no opinion about WHOSE message it is — the self-bot
        // exclusion lives at enqueue time, where the install's own bot id is
        // known. Proving that here keeps the two concerns from being
        // conflated by a future reader.
        let out = flatten_slack_message(&fixture("own_bot_confirmation")).expect("flattens fine");
        assert!(out.contains("Ingested 1 past messages"));
        assert_eq!(bot_id_of(&fixture("own_bot_confirmation")).as_deref(), Some("B0TESTBOT0"));
    }
}
