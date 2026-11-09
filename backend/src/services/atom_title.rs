//! Per-coral headlines (`atoms.title`, migration 0097/0010).
//!
//! # Why titles exist
//!
//! Date-range recall ("what did we work on today and yesterday?") force-merges
//! every atom in the window into the synthesis set, and that set drives both a
//! per-atom content fetch and the synthesis prompt. Bodies do not scale there:
//! a single measured day on the reference ocean held 139 atoms, so any workable
//! bound had to drop most of the range — and the bound it used dropped whole
//! DAYS, because it kept the most recent N and a dense newest day evicted every
//! older one. Titles change the unit of recall from a body to ~10 words, so a
//! two-day range costs a few thousand tokens and the answer can cover EVERY
//! coral in the range instead of a sampled subset.
//!
//! # Why per-coral and not per-session
//!
//! `coding_sessions` already has a `title`, and reusing it was the obvious
//! shortcut. It only covers corals captured under a coding session, though —
//! notes, meeting transcripts, email, Slack and Teams have no session, so
//! session-level recall silently omits them from a date range. Titling the
//! coral makes every source type behave identically.
//!
//! # The provenance-header trap
//!
//! The coding-session capture path prefixes `content_inline` with a provenance
//! header — `"Session: <session title>. Repo: … Branch: …"` — whose length is
//! recorded in `atoms.header_len`. Titling the raw `content_inline` would
//! therefore produce the SESSION title on every coral in that session, which is
//! precisely the session-granularity failure titles exist to avoid. Always
//! strip via [`strip_provenance_header`] first. Only some atoms carry a header
//! (1235 of 3818 on the reference ocean), so stripping is conditional.

use chrono::{DateTime, Utc};

use crate::llm::{CompletionRequest, Message, Role};

/// Cost-accounting label this call records under (usage-metering PR4;
/// `llm::usage::metered_complete`'s `task` argument).
const ATOM_TITLE_USAGE_TASK: &str = "atom_title";

/// Hard ceiling on a stored title, in characters. Generous next to the prompt's
/// 12-word guidance — this is a storage sanity bound against a runaway model
/// or a pathological first sentence, not the stylistic limit.
pub(crate) const TITLE_MAX_CHARS: usize = 160;

/// Body characters handed to the model. A headline is determined by the opening
/// of a coral in practice, and every extra character is cost paid across a
/// whole-ocean backfill.
const TITLE_INPUT_CHARS: usize = 1200;

/// Max tokens for the completion — a 12-word headline needs very few.
const TITLE_MAX_TOKENS: u32 = 40;

/// The model's sentinel for "this text has no content worth titling".
const NONE_SENTINEL: &str = "none";

/// Drop the coding-session provenance prefix from an atom's inline content.
///
/// `header_len` is a BYTE length recorded at capture time, and it is validated
/// against the actual string rather than trusted. Every degradation path here
/// errs toward "no body" rather than "use the whole string", because the whole
/// string INCLUDES THE HEADER — and a title generated from the header is the
/// enclosing session's title stamped onto this coral, which is precisely the
/// failure per-coral titles exist to prevent. Losing one title is recoverable;
/// silently mislabelling every coral in a session is not.
///
/// * `header_len >= content.len()` — the header accounts for the entire string,
///   so there is genuinely nothing left to title. Returns `""`. (An earlier
///   version returned the whole string here "and let the caller's emptiness
///   check decide", which was wrong: the header is not empty, so the check
///   passed and the header went to the model. Caught by
///   `tests/atom_title_backfill_pg.rs` against a real header-only coral.)
/// * `header_len` not on a char boundary — a slightly-off byte length, so walk
///   FORWARD to the next boundary and slice there. That keeps the body while
///   still dropping the header, which beats both alternatives (panicking, or
///   falling back to the header-inclusive whole string).
/// * `header_len <= 0` or absent — no header was recorded; the whole string is
///   the body.
pub(crate) fn strip_provenance_header(content: &str, header_len: Option<i32>) -> &str {
    let Some(len) = header_len else {
        return content;
    };
    if len <= 0 {
        return content;
    }
    let len = len as usize;
    if len >= content.len() {
        return "";
    }
    // Walk forward to the next char boundary so a byte length that lands mid-UTF-8
    // still slices (and still drops the header) instead of falling back.
    let mut cut = len;
    while cut < content.len() && !content.is_char_boundary(cut) {
        cut += 1;
    }
    content.get(cut..).unwrap_or("").trim_start()
}

/// Deterministic fallback headline: the opening sentence (or line) of `body`,
/// normalized and truncated.
///
/// Used when there is no LLM configured or the owning account forbids cloud
/// egress (local-only accounts), so a coral is never left untitled for
/// want of a model. Deliberately dumb and allocation-cheap — it must never fail.
pub(crate) fn fallback_title(body: &str) -> Option<String> {
    let flat = body.split_whitespace().collect::<Vec<_>>().join(" ");
    let flat = flat.trim();
    if flat.is_empty() {
        return None;
    }
    // First sentence end that leaves something substantial; otherwise the whole
    // (truncated) opening. `.` is checked with a following space so version
    // numbers, file names and SHAs don't split the headline.
    let cut = flat
        .char_indices()
        .find(|&(i, c)| {
            matches!(c, '.' | '!' | '?')
                && i >= 24
                && flat[i + c.len_utf8()..]
                    .chars()
                    .next()
                    .is_none_or(|n| n.is_whitespace())
        })
        .map(|(i, _)| i);
    let first = match cut {
        Some(i) => &flat[..i],
        None => flat,
    };
    Some(truncate_title(first))
}

/// Source-aware variant of [`fallback_title`]. Calendar atoms are shaped
/// `"{summary}\n\n{description / Location / Join: url}"`
/// (`connectors::google_calendar::shape_calendar_item`, and the Microsoft
/// calendar shape), so the generic whitespace-flattening opening-sentence
/// rule produced titles like `Sync Join: https://meet.google.com/...`. For
/// those sources the title is the first line only: the event summary.
pub(crate) fn fallback_title_for_source(source_type: &str, body: &str) -> Option<String> {
    if matches!(source_type, "google_calendar" | "microsoft_calendar") {
        let first = body.lines().map(str::trim).find(|l| !l.is_empty())?;
        return Some(truncate_title(first));
    }
    fallback_title(body)
}

/// Clamp to [`TITLE_MAX_CHARS`] on a char boundary, appending an ellipsis when
/// the text was actually cut.
pub(crate) fn truncate_title(s: &str) -> String {
    let s = s.trim().trim_end_matches(['.', ',', ';', ':']).trim();
    if s.chars().count() <= TITLE_MAX_CHARS {
        return s.to_string();
    }
    let cut: String = s.chars().take(TITLE_MAX_CHARS - 1).collect();
    // Prefer breaking at the last space so the tail isn't a severed word.
    let cut = match cut.rfind(' ') {
        Some(i) if i >= TITLE_MAX_CHARS / 2 => cut[..i].to_string(),
        _ => cut,
    };
    format!("{}…", cut.trim_end())
}

/// Normalize a model-produced headline, rejecting the ones that are worse than
/// the deterministic fallback.
///
/// Returns `None` for the `none` sentinel, for empty output, and for the
/// "This note describes…" preamble shapes the prompt forbids — a model that
/// ignores the instruction should fall back rather than store a useless title.
fn clean_model_title(raw: &str) -> Option<String> {
    let mut t = raw.trim();
    // Models like to wrap a one-line answer in quotes or a label despite being
    // told not to.
    if let Some(rest) = t.strip_prefix("Title:").or_else(|| t.strip_prefix("title:")) {
        t = rest.trim();
    }
    t = t.trim_matches(|c| c == '"' || c == '\'' || c == '`').trim();
    // Only ever take the first line — a chatty model may add commentary below.
    t = t.lines().next().unwrap_or("").trim();
    if t.is_empty() || t.eq_ignore_ascii_case(NONE_SENTINEL) {
        return None;
    }
    let lower = t.to_ascii_lowercase();
    const BANNED_PREFIXES: &[&str] = &[
        "this note",
        "this session",
        "this coral",
        "this transcript",
        "this message",
        "summary of",
        "summary:",
        "update on",
        "discussion about",
        "a summary",
        "here is",
        "here's",
    ];
    if BANNED_PREFIXES.iter().any(|p| lower.starts_with(p)) {
        return None;
    }
    Some(truncate_title(t))
}

/// Provenance of a stored/generated title — persisted as `atoms.title_kind`
/// (migrations `0212_atoms_title_kind`/`0080_atoms_title_kind`, title-always-
/// present PR). Lets the sweep's candidate predicate distinguish "never
/// titled" from "titled by the deterministic fallback, and worth upgrading
/// once an LLM is actually available" (an egress-blocked account, a `None`
/// LLM instance, or the ingest-time synchronous write in `routes::ingest`)
/// from "already titled by the model — leave alone". Without this the sweep
/// would have to choose between re-examining every fallback-titled atom on
/// EVERY pass forever, or never upgrading them at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TitleKind {
    /// The model produced a usable headline.
    Llm,
    /// [`fallback_title`] produced it — no LLM configured, the account's
    /// egress policy forbids the call, or ingest's synchronous write. Stays a
    /// sweep candidate.
    Fallback,
    /// The model was asked (and paid) and its answer for this coral was the
    /// `none` sentinel or a rejected shape, so the deterministic fallback is
    /// stored. Not a sweep candidate again: re-asking would re-pay for the
    /// same answer every day.
    Declined,
}

impl TitleKind {
    /// The exact string persisted in `atoms.title_kind` — never derived via
    /// `Debug`/`Display` so the column's two allowed values stay decoupled
    /// from this enum's Rust-side naming.
    pub(crate) fn as_db_str(self) -> &'static str {
        match self {
            TitleKind::Llm => "llm",
            TitleKind::Fallback => "fallback",
            TitleKind::Declined => "declined",
        }
    }
}

/// The model input for one coral: the body with its provenance header
/// stripped (see the module docs on why that matters), cut to
/// [`TITLE_INPUT_CHARS`]. `None` when nothing titleable is left.
pub(crate) fn title_input(content: &str, header_len: Option<i32>) -> Option<String> {
    let body = strip_provenance_header(content, header_len);
    if body.trim().is_empty() {
        return None;
    }
    Some(body.chars().take(TITLE_INPUT_CHARS).collect())
}

/// The deterministic headline for one coral, header stripped: what is stored
/// when no model may be asked (no LLM, egress forbidden) or the model's answer
/// for it was unusable.
pub(crate) fn fallback_for(content: &str, header_len: Option<i32>) -> Option<String> {
    fallback_title(strip_provenance_header(content, header_len))
}

/// Cost-accounting label of the batched titling call.
pub(crate) const fn usage_task() -> &'static str {
    ATOM_TITLE_USAGE_TASK
}

/// System prompt for the batched call (one call per batch of corals).
const ATOM_TITLE_BATCH_SYSTEM_PROMPT: &str = include_str!("../../prompts/atom_title_batch_v1.txt");

/// Output budget per coral in a batch (a 12-word headline plus JSON framing).
const TITLE_BATCH_TOKENS_PER_CORAL: u32 = 48;

/// The one model request for a batch of coral bodies (from [`title_input`]):
/// numbered `[1]..[n]`, answered through the `emit_titles` tool with one
/// entry per index.
pub(crate) fn batch_request(model: &str, bodies: &[String]) -> CompletionRequest {
    let mut prompt = String::with_capacity(bodies.iter().map(|b| b.len() + 16).sum());
    for (i, body) in bodies.iter().enumerate() {
        prompt.push_str(&format!("[{}]\n{}\n\n", i + 1, body.trim()));
    }
    CompletionRequest {
        messages: vec![Message { role: Role::User, content: prompt }],
        model: model.to_string(),
        max_tokens: TITLE_MAX_TOKENS + TITLE_BATCH_TOKENS_PER_CORAL * bodies.len() as u32,
        system: Some(ATOM_TITLE_BATCH_SYSTEM_PROMPT.to_string()),
        tool_schema: Some(crate::llm::ToolSchema {
            strict: false,
            name: "emit_titles".to_string(),
            schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "titles": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "index": { "type": "integer" },
                                "title": { "type": "string" }
                            },
                            "required": ["index", "title"]
                        }
                    }
                },
                "required": ["titles"]
            }),
        }),
        reasoning: crate::llm::ReasoningSetting::Off,
        ..Default::default()
    }
}

/// The batch's answers by position: `Some(title)` for a usable headline
/// ([`clean_model_title`]), `None` for `none`, a rejected shape, a missing or
/// duplicate index, or an unparseable response. Never longer than `n`.
pub(crate) fn parse_batch_titles(raw: &str, n: usize) -> Vec<Option<String>> {
    #[derive(serde::Deserialize)]
    struct Entry {
        index: usize,
        title: String,
    }
    #[derive(serde::Deserialize)]
    struct Batch {
        titles: Vec<Entry>,
    }
    let mut out = vec![None; n];
    let cleaned = raw.trim().trim_start_matches("```json").trim_start_matches("```").trim();
    let (Some(start), Some(end)) = (cleaned.find('{'), cleaned.rfind('}')) else {
        return out;
    };
    if end < start {
        return out;
    }
    let Ok(batch) = serde_json::from_str::<Batch>(&cleaned[start..=end]) else {
        return out;
    };
    let mut seen = vec![false; n];
    for entry in batch.titles {
        let Some(slot) = entry.index.checked_sub(1).filter(|i| *i < n) else {
            continue;
        };
        if std::mem::replace(&mut seen[slot], true) {
            // A repeated index is ambiguous: keep neither answer.
            out[slot] = None;
            continue;
        }
        out[slot] = clean_model_title(&entry.title);
    }
    out
}

// ── Display-time guarantee ("titles always present") ───────────────────────
//
// The founder's requirement: every coral and every coding session shown
// anywhere in the product must carry a non-empty title, IMMEDIATELY, with no
// dependence on the async LLM sweep having run. [`display_title`] and
// [`display_session_title`] are the one seam every list/citation/detail
// endpoint routes through to guarantee that — see this module's own doc
// comment for why a `NULL atoms.title` is common in production (R2-only
// storage, a sweep that never reached a tenant DB, a bulk copy that dropped
// the column) and therefore not something any single write path can be
// trusted to have already fixed by the time a read happens.

/// Everything [`display_title`] needs to guarantee a non-empty headline for
/// one coral, gathered from whatever the caller already has on hand — no
/// caller is required to widen its own query just to populate this. Only
/// `source_type` and `timestamp` are required (the `atoms` row never allows
/// either to be NULL); every other field degrades gracefully when absent.
pub struct DisplayTitleInput<'a> {
    /// `atoms.title` — an LLM- or fallback-generated headline, when present.
    pub title: Option<&'a str>,
    /// `atoms.excerpt` — populated at ingest regardless of storage backend
    /// (`store::derive_journal_excerpt`), so it is the strongest fallback
    /// short of a real title: unlike `content_inline`, it survives the R2
    /// cutover (2026-08-04) untouched.
    pub excerpt: Option<&'a str>,
    /// The atom's inline body with any provenance header ALREADY STRIPPED
    /// (see [`strip_provenance_header`]), when the caller happens to have it
    /// on hand (e.g. a coral-detail route that already fetched/decrypted the
    /// body for display). Most list endpoints do not fetch this and should
    /// pass `None` — `excerpt` covers the same ground far more cheaply.
    pub stripped_body: Option<&'a str>,
    /// `atoms.source_type` (`"slack"`, `"coding_session"`, `"meeting_transcript"`, …).
    pub source_type: &'a str,
    pub channel_name: Option<&'a str>,
    pub author_name: Option<&'a str>,
    /// `atoms.timestamp` — the source event time, used for the final
    /// "{Source label} · {date}" fallback rung.
    pub timestamp: DateTime<Utc>,
}

/// Guarantee a non-empty, human-readable headline for one coral.
///
/// Precedence: a real `atoms.title` → a deterministic opening-sentence title
/// derived from `excerpt` → the same, derived from `stripped_body` → a source
/// label built from `source_type` + channel/author → that label with the
/// event date appended. The LAST rung never returns `None` (there is always a
/// `source_type` and a `timestamp`), so this function is infallible — no
/// caller anywhere needs an "untitled" placeholder ever again.
pub fn display_title(input: DisplayTitleInput<'_>) -> String {
    if let Some(t) = non_empty(input.title) {
        return t.to_string();
    }
    if let Some(t) = input.excerpt.and_then(fallback_title) {
        return t;
    }
    if let Some(t) = input.stripped_body.and_then(fallback_title) {
        return t;
    }
    let label = source_label(input.source_type, input.channel_name, input.author_name);
    format!("{label} · {}", input.timestamp.format("%Y-%m-%d"))
}

/// Everything [`display_session_title`] needs to guarantee a non-empty
/// headline for one `coding_sessions` row.
pub struct DisplaySessionTitleInput<'a> {
    /// `coding_sessions.title`, when present.
    pub session_title: Option<&'a str>,
    /// The already-computed [`display_title`] of the session's first (or
    /// latest) checkpoint coral, when the caller has one on hand. Deliberately
    /// NOT re-derived here — this function has no atom row to work from, and
    /// forcing a fetch would give every session-listing caller a second query
    /// it may not need (many already have this from the same batch that built
    /// `corals[]`).
    pub checkpoint_title: Option<&'a str>,
    /// `coding_sessions.project_key`, when present.
    pub project_key: Option<&'a str>,
    /// `coding_sessions.started_at` (or an equivalent event time) — used for
    /// both fallback rungs below the checkpoint title.
    pub timestamp: DateTime<Utc>,
    /// `coding_sessions.session_kind` (migration `0185_coding_sessions_
    /// session_kind`; `"coding"`/`"meeting"`/`"agent"`), when the caller has
    /// it on hand. Feeds ONLY the last fallback rung's label — `None` (a
    /// caller that hasn't fetched it, or a row from before the column
    /// existed) degrades to the historical `"Coding session"` label, the
    /// same text every caller got before this field existed.
    pub session_kind: Option<&'a str>,
    /// `coding_sessions.agent_name` (agent sessions, migration 0219), when
    /// the caller has it. Feeds the agent rung: an agent session with no
    /// explicit title reads `"{agent_name}: {task}"` (or just the name).
    pub agent_name: Option<&'a str>,
    /// `coding_sessions.task`, paired with `agent_name` above.
    pub task: Option<&'a str>,
}

/// Guarantee a non-empty, human-readable headline for one coding session.
///
/// Precedence: `coding_sessions.title` → `"{agent_name}: {task}"` /
/// `"{agent_name}"` (agent sessions) → the display title of the session's
/// first/latest checkpoint → `"{project_key} · {date}"` → `"{kind label} ·
/// {date}"`. The last rung never returns `None`, so this function is
/// infallible, same guarantee as [`display_title`].
pub fn display_session_title(input: DisplaySessionTitleInput<'_>) -> String {
    if let Some(t) = non_empty(input.session_title) {
        return t.to_string();
    }
    if let Some(name) = non_empty(input.agent_name) {
        return match non_empty(input.task) {
            Some(task) => format!("{name}: {task}"),
            None => name.to_string(),
        };
    }
    if let Some(t) = non_empty(input.checkpoint_title) {
        return t.to_string();
    }
    if let Some(p) = non_empty(input.project_key) {
        return format!("{p} · {}", input.timestamp.format("%Y-%m-%d"));
    }
    format!(
        "{} · {}",
        session_kind_label(input.session_kind),
        input.timestamp.format("%Y-%m-%d")
    )
}

/// Human-readable label for the LAST rung of [`display_session_title`] —
/// mirrors `store::journal_block_title_fallback`'s `"meeting_session"`/
/// `"agent_session"` arms (the SAME wording, so a session and its own
/// corals never disagree on what to call it), keyed on the WIRE
/// `session_kind` (`"coding"`/`"meeting"`/`"agent"`) rather than
/// `atoms.source_type`, since this function has no atom row to read one
/// from. `None` or anything outside the closed set defaults to
/// `"Coding session"` — the backward-compatible label every caller got
/// before `session_kind` existed ([`crate::ingest::atom::SessionKind::
/// DEFAULT`] is `Coding` for the identical reason).
fn session_kind_label(session_kind: Option<&str>) -> &'static str {
    match session_kind {
        Some("meeting") => "Meeting session",
        Some("agent") => "Agent session",
        // A persisted session of the retired desktop `work` kind. Ingest
        // rejects the kind; removed with the desktop data purge.
        Some("work") => "Work session",
        _ => "Coding session",
    }
}

/// Trim and reject empty/whitespace-only strings — the shared "is this field
/// actually usable" check every rung of both display-title functions applies
/// before accepting a caller-supplied value. Deliberately does NOT special-
/// case the literal text `"none"` the way [`clean_model_title`] does: that
/// sentinel is a contract between the LLM and [`generate_title`] alone, and a
/// human- or import-supplied `title`/`session_title` of literally "none"
/// (unlikely, but not this function's call to second-guess) should display as
/// written.
fn non_empty(s: Option<&str>) -> Option<&str> {
    s.map(str::trim).filter(|s| !s.is_empty())
}

/// Build a `"{Source} · #{channel} · {author}"`-shaped label from whatever
/// provenance an atom carries, omitting either half when absent. The source
/// half is always present (`source_type` is never NULL on `atoms`), so this
/// never returns an empty string.
fn source_label(source_type: &str, channel_name: Option<&str>, author_name: Option<&str>) -> String {
    let pretty = pretty_source_type(source_type);
    match (non_empty(channel_name), non_empty(author_name)) {
        (Some(c), Some(a)) => format!("{pretty} · #{c} · {a}"),
        (Some(c), None) => format!("{pretty} · #{c}"),
        (None, Some(a)) => format!("{pretty} · {a}"),
        (None, None) => pretty,
    }
}

/// Human-readable label for an `atoms.source_type` value. Falls back to a
/// space-for-underscore, capitalized rendering of the raw value for anything
/// not special-cased, so a new/unrecognized source type still reads as a
/// label rather than a raw snake_case string.
fn pretty_source_type(source_type: &str) -> String {
    match source_type {
        "slack" => "Slack".to_string(),
        "coding_session" => "Coding session".to_string(),
        "agent_session" => "Agent session".to_string(),
        "email" | "gmail" | "outlook" => "Email".to_string(),
        "meeting_transcript" | "zoom" | "meet" | "teams_transcript" | "google_meet" => {
            "Meeting".to_string()
        }
        "teams" | "teams_chat" => "Teams".to_string(),
        "note" => "Note".to_string(),
        // GitHub connector PR A (docs/connectors/github.md §3.5) — the four
        // narrative types plus the two recognised-but-not-yet-produced code
        // types (source_types() doc comment has the full reasoning for why
        // those two are registered now anyway).
        "github_pr" => "GitHub PR".to_string(),
        "github_issue" => "GitHub issue".to_string(),
        "github_release" => "GitHub release".to_string(),
        "github_commit" => "GitHub commit".to_string(),
        "github_code" => "GitHub code".to_string(),
        "github_code_summary" => "GitHub code summary".to_string(),
        other => {
            let spaced = other.replace('_', " ");
            let mut chars = spaced.chars();
            match chars.next() {
                Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
                None => "Coral".to_string(),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strips_the_session_provenance_header() {
        // The exact shape the capture path writes, and the exact trap: without
        // stripping, every coral in a session gets titled with the SESSION
        // title, which is the whole reason titles are per-coral.
        let header = "Session: RCA + fix: ocean not noticing coding sessions. \
                      Repo: /home/dev/dev/coralswarm. Branch: main.\n\n";
        let body = "Merge-train complete: all 15 PRs merged, main at cb1a479.";
        let content = format!("{header}{body}");

        let stripped = strip_provenance_header(&content, Some(header.len() as i32));
        assert_eq!(stripped, body);
        assert!(
            !stripped.contains("Session:"),
            "the provenance header must not survive into the title input"
        );
    }

    #[test]
    fn header_len_is_validated_and_degrades_toward_no_body() {
        let content = "short body";

        // No header recorded ⇒ the whole string is the body.
        assert_eq!(strip_provenance_header(content, Some(-5)), content);
        assert_eq!(strip_provenance_header(content, None), content);

        // Header covers (or overruns) the whole string ⇒ NO body. Returning the
        // whole string here is the bug `tests/atom_title_backfill_pg.rs` caught:
        // the header is non-empty, so an emptiness check downstream passes and
        // the session title gets titled onto the coral.
        assert_eq!(
            strip_provenance_header(content, Some(content.len() as i32)),
            "",
            "a header-only coral must yield NO body, never the header itself"
        );
        assert_eq!(strip_provenance_header(content, Some(9999)), "");

        // "é" is two bytes, so offset 1 is mid-char: walk forward to the next
        // boundary rather than panicking or returning the header-inclusive whole.
        let multibyte = "é rest of body";
        assert_eq!(strip_provenance_header(multibyte, Some(1)), "rest of body");
    }

    #[test]
    fn a_header_only_coral_is_never_titled_from_its_header() {
        // The exact production shape: `content_inline` is nothing but the
        // provenance prefix. Without this, every such coral inherits the
        // enclosing SESSION title — the failure mode per-coral titles exist to
        // prevent, and one that looks perfectly healthy in the data.
        let header = "Session: Ship member-private oceans. Repo: /x. Branch: main.\n\n";
        assert_eq!(
            strip_provenance_header(header, Some(header.len() as i32)),
            ""
        );
    }

    #[test]
    fn fallback_takes_the_first_sentence_without_splitting_identifiers() {
        let t = fallback_title(
            "Merge-train 2026-07-26 COMPLETE: all 15 PRs merged, main at cb1a479. \
             Next up is the deploy.",
        )
        .unwrap();
        assert!(
            t.starts_with("Merge-train 2026-07-26 COMPLETE"),
            "got {t:?}"
        );
        assert!(
            !t.contains("Next up"),
            "the fallback must stop at the first sentence: {t:?}"
        );

        // A version-like "1.2.3" must not be treated as a sentence end.
        let v = fallback_title("Bumped the desktop app to 1.2.3 after the icon fix").unwrap();
        assert!(v.contains("1.2.3"), "got {v:?}");
    }

    #[test]
    fn calendar_fallback_title_is_the_summary_only() {
        let body = "Sync\n\nJoin: https://meet.google.com/eeo-xbbb-jjf";
        // The generic rule is what produced the bad titles.
        assert_eq!(
            fallback_title(body).unwrap(),
            "Sync Join: https://meet.google.com/eeo-xbbb-jjf"
        );
        for st in ["google_calendar", "microsoft_calendar"] {
            assert_eq!(fallback_title_for_source(st, body).unwrap(), "Sync", "{st}");
        }
        assert_eq!(
            fallback_title_for_source("google_calendar", "Alex <> Stephen\n\nLocation: R4\nJoin: https://x").unwrap(),
            "Alex <> Stephen"
        );
        assert_eq!(fallback_title_for_source("google_calendar", "  \n "), None);
        // Other sources keep the generic behaviour.
        assert_eq!(fallback_title_for_source("outlook", body), fallback_title(body));
    }

    #[test]
    fn fallback_is_none_only_for_genuinely_empty_bodies() {
        assert_eq!(fallback_title("   \n\t  "), None);
        assert_eq!(fallback_title(""), None);
        assert!(fallback_title("x").is_some());
    }

    #[test]
    fn titles_are_truncated_on_a_char_boundary() {
        let long = "é".repeat(TITLE_MAX_CHARS * 2);
        let t = truncate_title(&long);
        assert!(t.chars().count() <= TITLE_MAX_CHARS, "len {}", t.chars().count());
        assert!(t.ends_with('…'));
    }

    #[test]
    fn model_output_is_cleaned_and_bad_shapes_rejected() {
        assert_eq!(
            clean_model_title("  \"15-PR merge train completed\"  "),
            Some("15-PR merge train completed".to_string())
        );
        assert_eq!(
            clean_model_title("Title: Auth bypass fixed in the share route"),
            Some("Auth bypass fixed in the share route".to_string())
        );
        // Only the first line survives a chatty model.
        assert_eq!(
            clean_model_title("Deploy pipeline hardened\n\nLet me know if…"),
            Some("Deploy pipeline hardened".to_string())
        );
        // The sentinel and the forbidden preamble shapes fall back instead.
        assert_eq!(clean_model_title("none"), None);
        assert_eq!(clean_model_title("NONE"), None);
        assert_eq!(clean_model_title(""), None);
        assert_eq!(clean_model_title("This note describes the merge train"), None);
        assert_eq!(clean_model_title("Summary of today's work"), None);
    }

    #[test]
    fn title_input_strips_the_header_and_skips_empty_bodies() {
        let header = "Session: Some session title. Repo: /x.\n\n";
        let content = format!("{header}Real body about the deploy");
        assert_eq!(title_input(&content, Some(header.len() as i32)).as_deref(), Some("Real body about the deploy"));
        assert_eq!(title_input("    ", None), None);
        assert_eq!(title_input("   ", Some(3)), None, "a header-only coral has nothing to title");
        assert_eq!(title_input(&"x".repeat(5000), None).unwrap().chars().count(), TITLE_INPUT_CHARS);
        assert!(fallback_for("Scoped the retrieval core under consumer RLS. Then deployed.", None)
            .unwrap()
            .starts_with("Scoped the retrieval core"));
    }

    #[test]
    fn batch_request_numbers_every_body_and_forces_the_tool() {
        let req = batch_request("m", &["first body".into(), "second body".into()]);
        let prompt = &req.messages[0].content;
        assert!(prompt.contains("[1]\nfirst body") && prompt.contains("[2]\nsecond body"), "{prompt}");
        assert_eq!(req.tool_schema.as_ref().unwrap().name, "emit_titles");
        assert!(req.max_tokens > TITLE_MAX_TOKENS);
    }

    #[test]
    fn batch_titles_are_cleaned_and_positional() {
        let raw = r#"```json
{"titles":[{"index":2,"title":"\"Deploy verified in prod v193\""},{"index":1,"title":"none"},
 {"index":3,"title":"This note describes RLS"},{"index":9,"title":"out of range"},
 {"index":4,"title":"Merge train done"},{"index":4,"title":"Merge train done twice"}]}
```"#;
        assert_eq!(
            parse_batch_titles(raw, 5),
            vec![None, Some("Deploy verified in prod v193".to_string()), None, None, None],
            "none, banned shapes, out-of-range and duplicated indexes yield no title"
        );
        assert_eq!(parse_batch_titles("not json", 2), vec![None, None]);
        assert_eq!(parse_batch_titles(r#"{"titles":[{"index":1,"title":"Only one"}]}"#, 2), vec![Some("Only one".into()), None]);
    }

    // ── display_title / display_session_title ──────────────────────────────

    fn ts() -> DateTime<Utc> {
        "2026-09-08T12:00:00Z".parse().unwrap()
    }

    #[test]
    fn display_title_prefers_a_real_title_over_everything_else() {
        let got = display_title(DisplayTitleInput {
            title: Some("Real headline"),
            excerpt: Some("An excerpt that would otherwise win"),
            stripped_body: Some("A body that would otherwise win"),
            source_type: "slack",
            channel_name: Some("eng"),
            author_name: Some("Alex"),
            timestamp: ts(),
        });
        assert_eq!(got, "Real headline");
    }

    #[test]
    fn display_title_falls_back_to_excerpt_then_body_then_source_label() {
        // No title ⇒ excerpt wins.
        let got = display_title(DisplayTitleInput {
            title: None,
            excerpt: Some("Shipped the merge train and deployed to prod."),
            stripped_body: Some("A body that would otherwise win"),
            source_type: "slack",
            channel_name: None,
            author_name: None,
            timestamp: ts(),
        });
        assert!(got.starts_with("Shipped the merge train"), "got {got:?}");

        // No title, no excerpt ⇒ stripped body wins.
        let got = display_title(DisplayTitleInput {
            title: None,
            excerpt: None,
            stripped_body: Some("Fixed the auth bypass in the share route."),
            source_type: "slack",
            channel_name: None,
            author_name: None,
            timestamp: ts(),
        });
        assert!(got.starts_with("Fixed the auth bypass"), "got {got:?}");

        // Nothing at all ⇒ source label + date.
        let got = display_title(DisplayTitleInput {
            title: None,
            excerpt: None,
            stripped_body: None,
            source_type: "slack",
            channel_name: Some("eng"),
            author_name: Some("Alex"),
            timestamp: ts(),
        });
        assert_eq!(got, format!("Slack · #eng · Alex · {}", ts().format("%Y-%m-%d")));
    }

    #[test]
    fn display_title_treats_empty_and_whitespace_fields_as_absent() {
        // A whitespace-only title/excerpt/body must not win over a lower rung
        // just because it's `Some(_)` — every rung is a genuine "usable
        // content" check, not a bare `Option::is_some`.
        let got = display_title(DisplayTitleInput {
            title: Some("   "),
            excerpt: Some("\n\t"),
            stripped_body: Some(""),
            source_type: "meeting_transcript",
            channel_name: None,
            author_name: None,
            timestamp: ts(),
        });
        assert_eq!(got, format!("Meeting · {}", ts().format("%Y-%m-%d")));
    }

    #[test]
    fn display_title_never_returns_empty_even_with_no_provenance_at_all() {
        let got = display_title(DisplayTitleInput {
            title: None,
            excerpt: None,
            stripped_body: None,
            source_type: "unknown_connector",
            channel_name: None,
            author_name: None,
            timestamp: ts(),
        });
        assert!(!got.trim().is_empty(), "display_title must never be empty");
        assert!(got.starts_with("Unknown connector"), "got {got:?}");
    }

    #[test]
    fn display_title_literal_none_title_is_shown_as_written() {
        // Unlike the LLM `none` sentinel `clean_model_title` special-cases,
        // a caller-supplied `title` of literally "none" is a real (if odd)
        // human value and must be displayed, not treated as absent.
        let got = display_title(DisplayTitleInput {
            title: Some("none"),
            excerpt: None,
            stripped_body: None,
            source_type: "slack",
            channel_name: None,
            author_name: None,
            timestamp: ts(),
        });
        assert_eq!(got, "none");
    }

    #[test]
    fn display_session_title_precedence() {
        // Real session title wins outright.
        assert_eq!(
            display_session_title(DisplaySessionTitleInput {
                session_title: Some("RCA + fix: ocean not noticing coding sessions"),
                checkpoint_title: Some("would lose"),
                project_key: Some("would lose"),
                timestamp: ts(),
                session_kind: None,
                agent_name: None,
                task: None,
            }),
            "RCA + fix: ocean not noticing coding sessions"
        );

        // No session title ⇒ checkpoint title.
        assert_eq!(
            display_session_title(DisplaySessionTitleInput {
                session_title: None,
                checkpoint_title: Some("Merge-train complete"),
                project_key: Some("would lose"),
                timestamp: ts(),
                session_kind: None,
                agent_name: None,
                task: None,
            }),
            "Merge-train complete"
        );

        // Neither ⇒ project key + date.
        assert_eq!(
            display_session_title(DisplaySessionTitleInput {
                session_title: None,
                checkpoint_title: None,
                project_key: Some("coralswarm"),
                timestamp: ts(),
                session_kind: None,
                agent_name: None,
                task: None,
            }),
            format!("coralswarm · {}", ts().format("%Y-%m-%d"))
        );

        // Nothing at all ⇒ generic label + date, never empty. `session_kind:
        // None` degrades to the historical "Coding session" label.
        let got = display_session_title(DisplaySessionTitleInput {
            session_title: None,
            checkpoint_title: None,
            project_key: None,
            timestamp: ts(),
            session_kind: None,
            agent_name: None,
            task: None,
        });
        assert_eq!(got, format!("Coding session · {}", ts().format("%Y-%m-%d")));
    }

    #[test]
    fn display_session_title_treats_whitespace_only_fields_as_absent() {
        let got = display_session_title(DisplaySessionTitleInput {
            session_title: Some("   "),
            checkpoint_title: Some(""),
            project_key: Some("\n"),
            timestamp: ts(),
            session_kind: None,
            agent_name: None,
            task: None,
        });
        assert_eq!(got, format!("Coding session · {}", ts().format("%Y-%m-%d")));
    }

    #[test]
    fn display_session_title_labels_the_generic_rung_by_session_kind() {
        // Independent review finding m2: the generic last rung must not
        // hardcode "Coding session" for a meeting/agent session that has
        // neither a real title, a checkpoint title, nor a project_key.
        for (kind, want_label) in [
            (Some("coding"), "Coding session"),
            (Some("meeting"), "Meeting session"),
            (Some("agent"), "Agent session"),
            (Some("work"), "Work session"),
            (None, "Coding session"),
            (Some("bogus-future-kind"), "Coding session"),
        ] {
            let got = display_session_title(DisplaySessionTitleInput {
                session_title: None,
                checkpoint_title: None,
                project_key: None,
                timestamp: ts(),
                session_kind: kind,
                agent_name: None,
                task: None,
            });
            assert_eq!(
                got,
                format!("{want_label} · {}", ts().format("%Y-%m-%d")),
                "session_kind={kind:?}"
            );
        }
    }

    #[test]
    fn display_session_title_prefers_agent_name_and_task_after_an_explicit_title() {
        let base = |session_title, agent_name, task| DisplaySessionTitleInput {
            session_title,
            checkpoint_title: Some("checkpoint headline"),
            project_key: Some("coralswarm"),
            timestamp: ts(),
            session_kind: Some("agent"),
            agent_name,
            task,
        };
        assert_eq!(
            display_session_title(base(None, Some("grokbot"), Some("CS-042"))),
            "grokbot: CS-042"
        );
        assert_eq!(display_session_title(base(None, Some("grokbot"), None)), "grokbot");
        assert_eq!(
            display_session_title(base(None, Some("  "), Some("CS-042"))),
            "checkpoint headline",
            "a blank agent name skips the agent rung"
        );
        assert_eq!(
            display_session_title(base(Some("Explicit"), Some("grokbot"), Some("CS-042"))),
            "Explicit"
        );
        assert_eq!(pretty_source_type("agent_session"), "Agent session");
    }

    // GitHub connector PR A — all six §3.5 source types are accepted
    // (registered) at this switch site, each with its own readable label.
    #[test]
    fn pretty_source_type_github_all_six_types_are_accepted() {
        assert_eq!(pretty_source_type("github_pr"), "GitHub PR");
        assert_eq!(pretty_source_type("github_issue"), "GitHub issue");
        assert_eq!(pretty_source_type("github_release"), "GitHub release");
        assert_eq!(pretty_source_type("github_commit"), "GitHub commit");
        assert_eq!(pretty_source_type("github_code"), "GitHub code");
        assert_eq!(pretty_source_type("github_code_summary"), "GitHub code summary");
    }
}
